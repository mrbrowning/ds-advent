use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::ops::AddAssign;

use log::{error, info};
use maelstrom_csp2::send;
use maelstrom_csp2::{
    message::{
        ErrorMessagePayload, InitMessagePayload, Message, MessageId, MessagePayload, ParsedInput,
        ParsedMessage, ParsedRpc,
    },
    node::{Command, NodeDelegate, NodeInput, ReplyRecord},
    rpc_error::MaelstromError,
};
use serde::{ser::SerializeTuple, Deserialize, Serialize};
use tokio::{
    io::{stdin, stdout, BufReader},
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
};

use message_macro::maelstrom_message;

const READ_OPERATION: &str = "r";
const WRITE_OPERATION: &str = "w";

const NEIGHBOR_TIMEOUT: u64 = 200;

#[derive(Clone, Debug)]
enum ChallengeCommand {
    Write(TxnId, KvKey, i64),
    Read {
        txn_id: TxnId,
        read: KvKey,
    },
    InitTxn(TxnId, Vec<KvKey>),
    Commit {
        txn_id: TxnId,
        sink: UnboundedSender<NodeInput<ChallengePayload, ChallengeCommand>>,
    },
    CommitOk(Vec<Operation>),
    CommitFailed,
}

#[maelstrom_message]
#[derive(Serialize, Deserialize, Clone, Debug)]
enum ChallengePayload {
    #[serde(rename = "txn")]
    Txn(TxnPayload),

    #[serde(rename = "txn_ok")]
    TxnOk(TxnOkPayload),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TxnPayload {
    txn: Vec<Operation>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TxnOkPayload {
    txn: Vec<Operation>,
}

#[derive(Clone, Copy, Debug)]
enum Operation {
    // I'd like to distinguish between a read request and response here, but the txn schema doesn't distinguish between
    // a read request and a read response for a non-existent key.
    Read(KvKey, Option<i64>),
    Write(KvKey, i64),
}

impl Serialize for Operation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut tuple = serializer.serialize_tuple(3)?;
        match self {
            Operation::Read(key, value) => {
                tuple.serialize_element(READ_OPERATION)?;
                tuple.serialize_element(key)?;
                tuple.serialize_element(value)?;
            }
            Operation::Write(key, value) => {
                tuple.serialize_element(WRITE_OPERATION)?;
                tuple.serialize_element(key)?;
                tuple.serialize_element(&Some(*value))?;
            }
        }

        tuple.end()
    }
}

impl<'de> Deserialize<'de> for Operation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let (operation, key, value): (String, KvKey, Option<i64>) =
            Deserialize::deserialize(deserializer)?;

        match operation.as_str() {
            READ_OPERATION => Ok(Operation::Read(key, value)),
            WRITE_OPERATION => Ok(Operation::Write(key, value.unwrap())),
            _ => Err(serde::de::Error::custom("Unknown operation")),
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Copy, Hash, Debug)]
struct KvKey(i64);

#[derive(PartialEq, Eq, Clone, Copy, Hash, Debug)]
struct TxnId(i64);

impl From<i64> for TxnId {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl AddAssign<i64> for TxnId {
    fn add_assign(&mut self, rhs: i64) {
        self.0 += rhs;
    }
}

#[derive(PartialEq, Eq, Clone, Copy, Hash, Debug)]
struct VersionedValue {
    value: i64,
    version: i64,
}

#[derive(PartialEq, Eq, Clone, Copy, Hash, Debug)]
enum SnapshottedValue {
    Missing,
    Present(VersionedValue),
}

#[derive(PartialEq, Eq, Clone, Copy, Hash, Debug)]
enum VersionedOperation {
    Read(KvKey, SnapshottedValue),
    Write(KvKey, VersionedValue),
}

#[derive(Debug)]
struct KvStore {
    data: HashMap<KvKey, VersionedValue>,
    snapshots: HashMap<TxnId, (HashMap<KvKey, SnapshottedValue>, Vec<VersionedOperation>)>,
}

impl KvStore {
    fn new() -> Self {
        Self {
            data: HashMap::new(),
            snapshots: HashMap::new(),
        }
    }

    fn init_txn(&mut self, txn_id: TxnId, keys: Vec<KvKey>) {
        let snapshot: HashMap<_, _> = keys
            .iter()
            .map(|k| {
                (
                    *k,
                    self.data
                        .get(k)
                        .map(|v| SnapshottedValue::Present(*v))
                        .unwrap_or(SnapshottedValue::Missing),
                )
            })
            .collect();
        self.snapshots.insert(txn_id, (snapshot, Vec::new()));
    }

    fn read(&mut self, txn_id: TxnId, key: KvKey) -> Option<i64> {
        let (data, ref mut operations) = self
            .snapshots
            .get_mut(&txn_id)
            .expect("Expected existing snapshot for transaction");

        let value = data.get(&key).expect("Expected existing value");
        operations.push(VersionedOperation::Read(key, *value));

        match value {
            SnapshottedValue::Missing => None,
            SnapshottedValue::Present(v) => Some(v.value),
        }
    }

    fn write(&mut self, txn_id: TxnId, key: KvKey, value: i64) {
        let (ref mut data, ref mut operations) = self
            .snapshots
            .get_mut(&txn_id)
            .expect("Expected existing snapshot for transaction");

        let new_value = match data.get(&key).expect("Expected existing value") {
            SnapshottedValue::Missing => VersionedValue { value, version: 0 },
            SnapshottedValue::Present(v) => VersionedValue {
                value,
                version: v.version + 1,
            },
        };

        data.insert(key, SnapshottedValue::Present(new_value));
        operations.push(VersionedOperation::Write(key, new_value));
    }

    fn commit(&mut self, txn_id: TxnId) -> Result<Vec<Operation>, MaelstromError> {
        let (_, operations) = self
            .snapshots
            .remove(&txn_id)
            .expect("Expected existing snapshot for transaction");

        for op in operations.iter() {
            if let VersionedOperation::Write(k, v) = op {
                if let Some(existing_value) = self.data.get_mut(k) {
                    if existing_value.version >= v.version {
                        return Err(MaelstromError::Other(
                            "Transaction commit failed due to intervening write".to_string(),
                        ));
                    }
                }
            }
        }

        // No intervening transactions with writes completed, or this one had no writes (in which case it's fine to just
        // logically order it before whichever ones completed in the meantime, we're not looking for anything close to
        // linearizability).
        self.data.extend(operations.iter().filter_map(|op| {
            if let VersionedOperation::Write(k, v) = op {
                Some((k, v))
            } else {
                None
            }
        }));

        let result: Vec<_> = operations
            .into_iter()
            .map(|op| match op {
                VersionedOperation::Read(k, v) => match v {
                    SnapshottedValue::Missing => Operation::Read(k, None),
                    SnapshottedValue::Present(v) => Operation::Read(k, Some(v.value)),
                },
                VersionedOperation::Write(k, v) => Operation::Write(k, v.value),
            })
            .collect();

        Ok(result)
    }
}

struct TxnDelegate {
    msg_rx: Option<UnboundedReceiver<ParsedInput<ChallengePayload>>>,
    msg_tx: UnboundedSender<Message<ChallengePayload>>,

    cmd_rx: Option<UnboundedReceiver<Command<ChallengePayload, ChallengeCommand>>>,
    cmd_tx: UnboundedSender<Command<ChallengePayload, ChallengeCommand>>,

    node_id: String,
    node_ids: Box<[String]>,

    msg_id: MessageId,
    reply_records: HashMap<MessageId, ReplyRecord<ChallengePayload, ChallengeCommand>>,

    kv_store: KvStore,
    txn_id: TxnId,
}

impl TxnDelegate {
    #[allow(unused_variables)]
    async fn handle_txn(
        &mut self,
        src: String,
        msg_id: MessageId,
        txn: Vec<Operation>,
    ) -> Result<(), MaelstromError> {
        if self.is_cluster_member(&src) {
            let msg_tx = self.get_msg_tx();
            let reply = self.reply(src, msg_id, ChallengePayload::TxnOk(TxnOkPayload { txn }));

            send!(msg_tx, reply, "Delegate egress hung up: {}");
            return Ok(());
        }

        let mut cmd_tx = self.get_command_tx();
        let (mut task_tx, mut task_rx) =
            unbounded_channel::<NodeInput<ChallengePayload, ChallengeCommand>>();
        // Not a fan of this clone in particular, but the alternatives seem to be either sharing self with an Arc/Mutex
        // or plumbing through an index for node_ids everywhere and then translating at the edge from the node id
        // strings. Keeping the node id list itself in an Arc is a possibility too, but it means every implementor of
        // NodeDelegate needs to pay for that.
        let node_ids_clone = self.node_ids.clone();
        let neighbors: Box<[String]> = self
            .node_ids
            .iter()
            .filter(|n| **n != self.node_id)
            .cloned()
            .collect();
        let txn_id = self.next_txn_id();

        tokio::spawn(async move {
            let txn_results: Vec<Operation>;
            let keys: Vec<_> = txn
                .iter()
                .map(|op| match op {
                    Operation::Write(k, _) => *k,
                    Operation::Read(k, _) => *k,
                })
                .collect();

            loop {
                let keys_clone = keys.clone();
                (cmd_tx, task_tx) = (async move {
                    cmd_tx
                        .send(Command::Custom(ChallengeCommand::InitTxn(
                            txn_id, keys_clone,
                        )))
                        .expect("Node hung up on itself");

                    (cmd_tx, task_tx)
                })
                .await;

                for op in txn.iter() {
                    let command = Command::Custom(match op {
                        Operation::Write(k, v) => ChallengeCommand::Write(txn_id, *k, *v),
                        Operation::Read(k, _) => ChallengeCommand::Read { txn_id, read: *k },
                    });

                    (cmd_tx, task_tx) = (async move {
                        cmd_tx.send(command).expect("Node hung up on itself");

                        (cmd_tx, task_tx)
                    })
                    .await;
                }

                cmd_tx
                    .send(Command::Custom(ChallengeCommand::Commit {
                        txn_id,
                        sink: task_tx.clone(),
                    }))
                    .expect("Node hung up on itself");

                let result = task_rx.recv().await.expect("Node hung up on itself");
                if let NodeInput::Command(Command::Custom(ChallengeCommand::CommitOk(results))) =
                    result
                {
                    txn_results = results;
                    break;
                }
            }

            cmd_tx
                .send(Command::Reply {
                    dest: src,
                    msg_id,
                    msg: ChallengePayload::TxnOk(TxnOkPayload { txn: txn_results }),
                })
                .expect("Node hung up on itself");

            let payload: Vec<_> = txn
                .iter()
                .filter(|op| matches!(op, Operation::Write(_, _)))
                .copied()
                .collect();
            if !payload.is_empty() {
                for n in neighbors.iter() {
                    cmd_tx
                        .send(Command::Rpc {
                            dest: n.clone(),
                            msg: ChallengePayload::Txn(TxnPayload {
                                txn: payload.clone(),
                            }),
                            sink: task_tx.clone(),
                            timeout_ms: Some(NEIGHBOR_TIMEOUT),
                            msg_id: None,
                        })
                        .expect("Node hung up on itself");
                }

                let mut responses: HashSet<String> = HashSet::new();
                while responses.len() < neighbors.len() {
                    let result = task_rx.recv().await.expect("Node hung up on itself");
                    match result {
                        NodeInput::Message(ParsedInput::Reply(r)) => {
                            if let ChallengePayload::TxnOk(_) = r.body {
                                responses.insert(r.src);
                            } else {
                                panic!("Got unexpected reply");
                            }
                        }
                        NodeInput::Command(Command::Timeout(msg_id, node)) => {
                            // Retry until we hear back.
                            cmd_tx
                                .send(Command::Cancel(msg_id))
                                .expect("Node hung up on itself");

                            cmd_tx
                                .send(Command::Rpc {
                                    dest: node,
                                    msg: ChallengePayload::Txn(TxnPayload {
                                        txn: payload.clone(),
                                    }),
                                    sink: task_tx.clone(),
                                    timeout_ms: Some(NEIGHBOR_TIMEOUT),
                                    msg_id: None,
                                })
                                .expect("Node hung up on itself");
                        }
                        _ => panic!("Got unexpected command"),
                    }
                }
            }
        });

        Ok(())
    }

    fn next_txn_id(&mut self) -> TxnId {
        self.txn_id += 1;
        self.txn_id
    }
}

impl NodeDelegate for TxnDelegate {
    type MessageType = ChallengePayload;
    type CommandType = ChallengeCommand;

    fn init(
        node_id: String,
        node_ids: impl IntoIterator<Item = String>,
        msg_tx: UnboundedSender<Message<Self::MessageType>>,
        msg_rx: UnboundedReceiver<ParsedInput<Self::MessageType>>,
    ) -> Self {
        let (cmd_tx, cmd_rx) = unbounded_channel::<Command<Self::MessageType, Self::CommandType>>();

        Self {
            msg_tx,
            msg_rx: Some(msg_rx),
            cmd_tx,
            cmd_rx: Some(cmd_rx),
            node_id,
            node_ids: node_ids.into_iter().collect(),
            msg_id: 1.into(),
            reply_records: HashMap::new(),
            kv_store: KvStore::new(),
            txn_id: 0.into(),
        }
    }

    #[allow(clippy::manual_async_fn)]
    fn handle_rpc(
        &mut self,
        message: ParsedRpc<Self::MessageType>,
    ) -> impl Future<Output = Result<(), MaelstromError>> + Send {
        async move {
            match message.body {
                ChallengePayload::Txn(t) => {
                    self.handle_txn(message.src, message.msg_id, t.txn).await?;
                }

                _ => panic!("Unexpected message type: {:?}", message),
            }

            Ok(())
        }
    }

    #[allow(clippy::manual_async_fn)]
    fn handle_message(
        &mut self,
        _: ParsedMessage<Self::MessageType>,
    ) -> impl Future<Output = Result<(), MaelstromError>> + Send {
        async move { todo!() }
    }

    fn get_msg_id(&mut self) -> &mut MessageId {
        &mut self.msg_id
    }

    fn get_msg_rx(&mut self) -> UnboundedReceiver<ParsedInput<Self::MessageType>> {
        self.msg_rx.take().unwrap()
    }

    fn get_msg_tx(&self) -> UnboundedSender<Message<Self::MessageType>> {
        self.msg_tx.clone()
    }

    fn get_reply_records_mut(
        &mut self,
    ) -> &mut HashMap<
        MessageId,
        maelstrom_csp2::node::ReplyRecord<Self::MessageType, Self::CommandType>,
    > {
        &mut self.reply_records
    }

    #[allow(clippy::manual_async_fn)]
    fn handle_custom_command(
        &mut self,
        command: Self::CommandType,
    ) -> impl futures::prelude::Future<Output = Result<(), MaelstromError>> + Send {
        async {
            info!("Got command: {:?}", command);
            match command {
                ChallengeCommand::Write(txn_id, k, v) => {
                    self.kv_store.write(txn_id, k, v);
                }
                ChallengeCommand::Read { txn_id, read } => {
                    let _ = self.kv_store.read(txn_id, read);
                }
                ChallengeCommand::InitTxn(txn_id, keys) => {
                    self.kv_store.init_txn(txn_id, keys);
                }
                ChallengeCommand::Commit { txn_id, sink } => {
                    let response: NodeInput<ChallengePayload, _> =
                        NodeInput::Command(Command::Custom(match self.kv_store.commit(txn_id) {
                            Ok(ops) => ChallengeCommand::CommitOk(ops),
                            Err(_) => ChallengeCommand::CommitFailed,
                        }));
                    sink.send(response).expect("Node hung up on itself");
                }
                _ => panic!("Unexpected command type: {:?}", command),
            };

            Ok(())
        }
    }

    fn get_node_id(&self) -> &str {
        &self.node_id
    }

    fn get_command_rx(
        &mut self,
    ) -> UnboundedReceiver<maelstrom_csp2::node::Command<Self::MessageType, Self::CommandType>>
    {
        self.cmd_rx.take().unwrap()
    }

    fn get_command_tx(
        &self,
    ) -> UnboundedSender<maelstrom_csp2::node::Command<Self::MessageType, Self::CommandType>> {
        self.cmd_tx.clone()
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let (node, mut ingress, mut egress) = maelstrom_csp2::get_node_and_io::<
        ChallengePayload,
        TxnDelegate,
    >(BufReader::new(stdin()), stdout());

    tokio::spawn(async move {
        if let Err(e) = ingress.run().await {
            panic!("Ingress died: {}", e);
        }
    });
    tokio::spawn(async move {
        if let Err(e) = egress.run().await {
            panic!("Ingress died: {}", e);
        }
    });

    match node.run().await {
        Err(e) => {
            error!("Node init loop failed: {}", e);
        }
        Ok(initialized) => {
            if let Err(e) = initialized.run().await {
                error!("Node failed: {}", e);
            }
        }
    }
}

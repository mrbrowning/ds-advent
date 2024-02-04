use std::collections::HashMap;

use log::error;
use maelstrom_csp2::{
    get_node_and_io,
    kv::{KvCasPayload, KvReadPayload, KvType, KvWritePayload},
    message::{
        ErrorMessagePayload, InitMessagePayload, Message, MessageId, MessagePayload, ParsedInput,
        ParsedMessage, ParsedRpc,
    },
    node::{Command, NodeDelegate, NodeInput, ReplyRecord},
    rpc_error::{ErrorType, MaelstromError},
    send,
};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{stdin, stdout, BufReader},
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
};

use message_macro::maelstrom_message;

const KV_TIMEOUT_MS: u64 = 200;
const COUNTER_KEY: &str = "counter";

#[derive(Serialize, Deserialize, Clone, Debug)]
struct AddPayload {
    delta: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct ReadOkPayload {
    value: i64,
}

#[maelstrom_message]
#[derive(Serialize, Deserialize, Clone, Debug)]
enum ChallengePayload {
    #[serde(rename = "add")]
    Add(AddPayload),

    #[serde(rename = "add_ok")]
    AddOk,

    #[serde(rename = "read")]
    Read(KvReadPayload),

    #[serde(rename = "read_ok")]
    ReadOk(ReadOkPayload),

    #[serde(rename = "write")]
    KvWrite(KvWritePayload<i64>),

    #[serde(rename = "write_ok")]
    KvWriteOk,

    #[serde(rename = "cas")]
    KvCas(KvCasPayload<i64>),

    #[serde(rename = "cas_ok")]
    KvCasOk,
}

#[derive(Clone, Debug)]
struct ChallengeCommand {
    add: i64,
}

#[derive(Debug)]
struct CounterDelegate {
    node_id: String,
    node_ids: Box<[String]>,

    msg_tx: UnboundedSender<Message<ChallengePayload>>,
    msg_rx: Option<UnboundedReceiver<ParsedInput<ChallengePayload>>>,

    cmd_tx: UnboundedSender<Command<ChallengePayload, ChallengeCommand>>,
    cmd_rx: Option<UnboundedReceiver<Command<ChallengePayload, ChallengeCommand>>>,

    cached_counter: i64,

    msg_id: MessageId,
    reply_records: HashMap<MessageId, ReplyRecord<ChallengePayload, ChallengeCommand>>,
}

impl CounterDelegate {
    async fn handle_read_request(
        &mut self,
        request: ParsedRpc<ChallengePayload>,
    ) -> Result<(), MaelstromError> {
        if self.is_cluster_member(&request.src) {
            let msg_tx = self.get_msg_tx();
            let reply = self.reply(
                request.src,
                request.msg_id,
                ChallengePayload::ReadOk(ReadOkPayload {
                    value: self.cached_counter,
                }),
            );

            send!(msg_tx, reply, "Delegate egress hung up: {}");
            return Ok(());
        }

        let node_ids: Vec<String> = self
            .node_ids
            .iter()
            .filter_map(|id| {
                if id != &self.node_id {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect();
        let (task_tx, mut task_rx) =
            unbounded_channel::<NodeInput<ChallengePayload, ChallengeCommand>>();
        let cmd_tx = self.get_command_tx();
        let counter_value = self.cached_counter;

        tokio::spawn(async move {
            cmd_tx
                .send(Command::Rpc {
                    dest: String::from(KvType::SeqKv),
                    msg: ChallengePayload::Read(KvReadPayload {
                        key: Some(COUNTER_KEY.into()),
                    }),
                    sink: task_tx.clone(),
                    timeout_ms: Some(KV_TIMEOUT_MS),
                    msg_id: None,
                })
                .expect("Node hung up on itself");

            for id in node_ids.iter() {
                cmd_tx
                    .send(Command::Rpc {
                        dest: id.clone(),
                        msg: ChallengePayload::Read(KvReadPayload { key: None }),
                        sink: task_tx.clone(),
                        timeout_ms: Some(KV_TIMEOUT_MS),
                        msg_id: None,
                    })
                    .expect("Node hung up on itself");
            }

            let mut votes: Vec<i64> = Vec::new();
            while votes.len() < node_ids.len() + 1 {
                let result = task_rx.recv().await.expect("Node hung up on itself");
                match &result {
                    NodeInput::Message(m) => {
                        if let ParsedInput::Reply(r) = m {
                            match &r.body {
                                ChallengePayload::ReadOk(r) => {
                                    votes.push(r.value);
                                }
                                ChallengePayload::Error(e) => {
                                    let err = ErrorType::try_from(e.code).unwrap();
                                    match err {
                                        // We haven't written the counter yet. Just send them what
                                        // we have.
                                        ErrorType::KeyDoesNotExist => {
                                            votes.push(counter_value);
                                        }
                                        _ => panic!("Unexpected error from peer: {:?}", result),
                                    }
                                }
                                _ => panic!("Unexpected reply: {:?}", r.body),
                            }
                        }
                    }
                    NodeInput::Command(c) => {
                        if let Command::Timeout(msg_id, _) = c {
                            // Peer timed out, use our cached value in its place.
                            votes.push(counter_value);

                            cmd_tx
                                .send(Command::Cancel(*msg_id))
                                .expect("Node hung up on itself");
                        }
                    }
                }
            }

            cmd_tx
                .send(Command::Reply {
                    dest: request.src,
                    msg_id: request.msg_id,
                    msg: ChallengePayload::ReadOk(ReadOkPayload {
                        value: *votes.iter().max().unwrap(),
                    }),
                })
                .unwrap();
        });

        Ok(())
    }

    async fn handle_add_request(
        &mut self,
        delta: i64,
        request: ParsedRpc<ChallengePayload>,
    ) -> Result<(), MaelstromError> {
        let counter_value = self.cached_counter;
        let (task_tx, mut task_rx) =
            unbounded_channel::<NodeInput<ChallengePayload, ChallengeCommand>>();
        let cmd_tx = self.get_command_tx();

        tokio::spawn(async move {
            let mut cas_rpc = Command::Rpc {
                dest: String::from(KvType::SeqKv),
                msg: ChallengePayload::KvCas(KvCasPayload {
                    key: COUNTER_KEY.into(),
                    from: counter_value,
                    to: counter_value + delta,
                    create_if_not_exists: Some(true),
                }),
                sink: task_tx.clone(),
                timeout_ms: Some(KV_TIMEOUT_MS),
                msg_id: None,
            };
            cmd_tx
                .send(cas_rpc.clone())
                .expect("Node hung up on itself");

            // If the CAS fails on compare, we issue a read to get the newer value and flip this
            // boolean so that we can loop on retrying the read if it times out. Once we get it
            // back, we switch back into sending a CAS and retrying until it doesn't time out
            // anymore.
            let mut waiting_on_cas = true;
            while let Some(result) = task_rx.recv().await {
                if waiting_on_cas {
                    match result {
                        NodeInput::Message(m) => {
                            if let ParsedInput::Reply(r) = m {
                                match r.body {
                                    ChallengePayload::KvCasOk => {
                                        // Send a command back to the node delegate to add the
                                        // delta to our counter. That means our cached value could
                                        // be out of sync with the value in the KV store for long
                                        // parts of this node's run, but the CRDT-like semantics of
                                        // a grow-only counter make that not a problem.
                                        cmd_tx
                                            .send(Command::Custom(ChallengeCommand { add: delta }))
                                            .expect("Node hung up on itself");

                                        cmd_tx
                                            .send(Command::Reply {
                                                dest: request.src,
                                                msg_id: request.msg_id,
                                                msg: ChallengePayload::AddOk,
                                            })
                                            .expect("Node hung up on itself");

                                        break;
                                    }
                                    ChallengePayload::Error(e) => {
                                        let err = ErrorType::try_from(e.code).unwrap();
                                        match err {
                                            ErrorType::PreconditionFailed => {
                                                // The compare failed, so move to trying to read
                                                // the current value from the KV store.
                                                waiting_on_cas = false;
                                                cmd_tx
                                                    .send(Command::Rpc {
                                                        dest: String::from(KvType::SeqKv),
                                                        msg: ChallengePayload::Read(
                                                            KvReadPayload {
                                                                key: Some(COUNTER_KEY.into()),
                                                            },
                                                        ),
                                                        sink: task_tx.clone(),
                                                        timeout_ms: Some(KV_TIMEOUT_MS),
                                                        msg_id: None,
                                                    })
                                                    .expect("Node hung up on itself");
                                            }
                                            _ => panic!("Got err from kv store: {:?}", err),
                                        }
                                    }
                                    _ => panic!("Unexpected reply: {:?}", r.body),
                                }
                            }
                        }
                        NodeInput::Command(c) => {
                            if let Command::Timeout(msg_id, _) = c {
                                cmd_tx
                                    .send(Command::Cancel(msg_id))
                                    .expect("Node hung up on itself");

                                // Retry the CAS.
                                cmd_tx
                                    .send(cas_rpc.clone())
                                    .expect("Node hung up on itself");
                            }
                        }
                    }
                } else {
                    // Issue a read to the KV store and retry until we get it.
                    match result {
                        NodeInput::Message(m) => {
                            if let ParsedInput::Reply(r) = m {
                                match r.body {
                                    ChallengePayload::ReadOk(r) => {
                                        cas_rpc = Command::Rpc {
                                            dest: String::from(KvType::SeqKv),
                                            msg: ChallengePayload::KvCas(KvCasPayload {
                                                key: COUNTER_KEY.into(),
                                                from: r.value,
                                                to: r.value + delta,
                                                create_if_not_exists: Some(true),
                                            }),
                                            sink: task_tx.clone(),
                                            timeout_ms: Some(KV_TIMEOUT_MS),
                                            msg_id: None,
                                        };
                                        cmd_tx
                                            .send(cas_rpc.clone())
                                            .expect("Node hung up on itself");

                                        waiting_on_cas = true;
                                    }
                                    _ => panic!("Unexpected reply: {:?}", r.body),
                                }
                            }
                        }
                        NodeInput::Command(c) => {
                            if let Command::Timeout(msg_id, _) = c {
                                cmd_tx
                                    .send(Command::Cancel(msg_id))
                                    .expect("Node hung up on itself");

                                // Retry timed out read.
                                cmd_tx
                                    .send(Command::Rpc {
                                        dest: String::from(KvType::SeqKv),
                                        msg: ChallengePayload::Read(KvReadPayload {
                                            key: Some(COUNTER_KEY.into()),
                                        }),
                                        sink: task_tx.clone(),
                                        timeout_ms: Some(KV_TIMEOUT_MS),
                                        msg_id: None,
                                    })
                                    .expect("Node hung up on itself");
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }
}

impl NodeDelegate for CounterDelegate {
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
            node_id,
            node_ids: node_ids.into_iter().collect(),
            msg_tx,
            msg_rx: Some(msg_rx),
            cmd_tx,
            cmd_rx: Some(cmd_rx),
            cached_counter: 0,
            msg_id: 0.into(),
            reply_records: HashMap::new(),
        }
    }

    #[allow(clippy::manual_async_fn)]
    fn handle_message(
        &mut self,
        _: ParsedMessage<Self::MessageType>,
    ) -> impl futures::prelude::Future<Output = Result<(), MaelstromError>> + Send {
        async move { todo!() }
    }

    #[allow(clippy::manual_async_fn)]
    fn handle_rpc(
        &mut self,
        message: ParsedRpc<Self::MessageType>,
    ) -> impl futures::prelude::Future<Output = Result<(), MaelstromError>> + Send {
        async move {
            match &message.body {
                ChallengePayload::Read(_) => self.handle_read_request(message).await,
                ChallengePayload::Add(a) => self.handle_add_request(a.delta, message).await,
                _ => Err(MaelstromError::Other(format!(
                    "Got unexpected message in handle_message: {:?}",
                    message
                ))),
            }
        }
    }

    #[allow(clippy::manual_async_fn)]
    fn handle_custom_command(
        &mut self,
        command: Self::CommandType,
    ) -> impl futures::prelude::Future<Output = Result<(), MaelstromError>> + Send {
        async move {
            self.cached_counter += command.add;

            Ok(())
        }
    }

    fn get_msg_id(&mut self) -> &mut MessageId {
        &mut self.msg_id
    }

    fn get_msg_rx(
        &mut self,
    ) -> tokio::sync::mpsc::UnboundedReceiver<ParsedInput<ChallengePayload>> {
        self.msg_rx.take().unwrap()
    }

    fn get_msg_tx(&self) -> UnboundedSender<Message<Self::MessageType>> {
        self.msg_tx.clone()
    }

    fn get_reply_records_mut(
        &mut self,
    ) -> &mut HashMap<MessageId, ReplyRecord<Self::MessageType, Self::CommandType>> {
        &mut self.reply_records
    }

    fn get_node_id(&self) -> &str {
        &self.node_id
    }

    fn get_command_rx(
        &mut self,
    ) -> UnboundedReceiver<Command<Self::MessageType, Self::CommandType>> {
        self.cmd_rx.take().unwrap()
    }

    fn get_command_tx(&self) -> UnboundedSender<Command<Self::MessageType, Self::CommandType>> {
        self.cmd_tx.clone()
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let (node, mut ingress, mut egress) =
        get_node_and_io::<ChallengePayload, CounterDelegate>(BufReader::new(stdin()), stdout());

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

#![allow(unused)]

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    hash::Hash,
};

use log::error;
use maelstrom_csp::{
    get_node_and_io,
    message::{
        ErrorMessagePayload, InitMessagePayload, Message, MessageBody, MessageId, MessagePayload,
    },
    node::NodeDelegate,
    rpc_error::MaelstromError,
    send,
};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{stdin, stdout, BufReader},
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};

const BROADCAST_TIMEOUT_MS: u64 = 100;

#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
struct BroadcastMessagePayload {
    message: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct ReadOkMessagePayload {
    messages: Vec<i64>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TopologyMessagePayload {
    topology: HashMap<String, Vec<String>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type")]
enum BroadcastPayload {
    #[serde(rename = "broadcast")]
    Broadcast(BroadcastMessagePayload),

    #[serde(rename = "broadcast_ok")]
    BroadcastOk,

    #[serde(rename = "read")]
    Read,

    #[serde(rename = "read_ok")]
    ReadOk(ReadOkMessagePayload),

    #[serde(rename = "topology")]
    Topology(TopologyMessagePayload),

    #[serde(rename = "topology_ok")]
    TopologyOk,

    #[serde(rename = "init")]
    Init(InitMessagePayload),

    #[serde(rename = "init_ok")]
    InitOk,

    #[serde(rename = "error")]
    Error(ErrorMessagePayload),

    Empty,
}

impl Default for BroadcastPayload {
    fn default() -> Self {
        Self::Empty
    }
}

impl MessagePayload for BroadcastPayload {
    fn as_init_msg(&self) -> Option<InitMessagePayload> {
        match self {
            BroadcastPayload::Init(m) => Some(m.clone()),
            _ => None,
        }
    }

    fn to_init_ok_msg() -> Self {
        Self::InitOk
    }

    fn to_err_msg(err: ErrorMessagePayload) -> Self {
        Self::Error(err)
    }
}

type BroadcastMessage = Message<BroadcastPayload>;

struct MessageStore {
    messages: HashSet<i64>,
}

impl MessageStore {
    fn new() -> Self {
        Self {
            messages: HashSet::new(),
        }
    }

    fn insert(&mut self, item: i64) -> bool {
        self.messages.insert(item)
    }

    fn contains(&self, item: i64) -> bool {
        self.messages.contains(&item)
    }

    fn messages(&self) -> Vec<i64> {
        self.messages.iter().map(|x| *x).collect()
    }
}

struct Topology {
    neighbors: Vec<String>,
}

impl Topology {
    fn new(neighbors: Vec<String>) -> Self {
        Self { neighbors }
    }

    fn neighbors(&self) -> &[String] {
        &self.neighbors
    }

    fn set_neighbors(&mut self, neighbors: &[String]) {
        self.neighbors = neighbors.iter().map(|s| s.clone()).collect();
    }

    fn node_mut(&mut self, name: &str) -> Option<Neighbor> {
        todo!()
    }
}

#[derive(Clone, Copy, Debug)]
enum NeighborState {
    Healthy,
    Unhealthy,
}

struct Neighbor {
    node_id: String,
    state: NeighborState,
    last_sent: HashMap<MessageId, i64>,
    backlog: HashSet<i64>,
    acked: HashSet<i64>,
}

impl Neighbor {
    fn node_id(&self) -> &str {
        &self.node_id
    }

    // TODO: change name
    async fn transition(
        &mut self,
        src: impl AsRef<str>,
        msg: &BroadcastMessagePayload,
        delegate: &mut BroadcastDelegate,
    ) -> Result<(), MaelstromError> {
        match &self.state {
            NeighborState::Healthy => {
                let msg_id = delegate
                    .rpc_with_timeout(src, BroadcastPayload::Broadcast(*msg), BROADCAST_TIMEOUT_MS)
                    .await?;
                self.last_sent.insert(msg_id, msg.message);
            }
            NeighborState::Unhealthy => {
                self.backlog.insert(msg.message);
            }
        }

        Ok(())
    }

    fn acked(&self, message: i64) -> bool {
        self.acked.contains(&message)
    }

    async fn die(
        &mut self,
        src: impl AsRef<str>,
        msg_id: MessageId,
        delegate: &mut BroadcastDelegate,
    ) -> Result<(), MaelstromError> {
        // TODO: I'm here
        match self.state {
            NeighborState::Healthy => {
                // mark as unhealthy, store whatever
                self.state = NeighborState::Unhealthy;
                let msg_id = delegate
                    .rpc_with_timeout(
                        src,
                        BroadcastPayload::Broadcast(todo!()),
                        BROADCAST_TIMEOUT_MS,
                    )
                    .await?;
            }
            NeighborState::Unhealthy => {
                // dnothing
                todo!()
            }
        }
    }
}

struct BroadcastDelegate {
    msg_tx: UnboundedSender<BroadcastMessage>,
    msg_rx: Option<UnboundedReceiver<BroadcastMessage>>,
    self_tx: UnboundedSender<BroadcastMessage>,
    outstanding_replies: HashMap<MessageId, String>,

    node_id: String,
    msg_id: MessageId,

    msg_store: MessageStore,
    topology: Topology,
}

impl BroadcastDelegate {
    async fn handle_broadcast(
        &mut self,
        msg: &BroadcastMessagePayload,
        src: &str,
    ) -> Result<(), MaelstromError> {
        let value = msg.message;
        let neighbors: Vec<String> = self
            .topology
            .neighbors()
            .iter()
            .map(|s| s.to_string())
            .collect();
        for n in neighbors.iter().filter(|n| n.as_str() != src) {
            // TODO: change names
            let mut node = self
                .topology
                .node_mut(n)
                .ok_or(MaelstromError::Other(format!(
                    "Couldn't find node in neighbors: {}",
                    n
                )))?;
            if node.acked(value) {
                continue;
            }

            node.transition(src, msg, self).await?;
        }

        Ok(())
        // if self.msg_store.contains(msg.message) {
        //     return Ok(BroadcastPayload::BroadcastOk);
        // }

        // self.msg_store.insert(msg.message);

        // for n in self.topology.neighbors().iter().filter(|n| *n != src) {
        //     let body = MessageBody {
        //         msg_id: Some(self.next_msg_id()),
        //         in_reply_to: None,
        //         local_msg: None,
        //         contents: BroadcastPayload::Broadcast(BroadcastMessagePayload {
        //             message: msg.message,
        //         }),
        //     };
        //     let msg = Self::format_outgoing(Some(n), body);

        //     send!(self.get_msg_tx(), msg, "Delegate egress hung up: {}");
        // }

        // Ok(BroadcastPayload::BroadcastOk)
    }

    async fn handle_read(&self) -> Result<BroadcastPayload, MaelstromError> {
        let messages = self.msg_store.messages();

        Ok(BroadcastPayload::ReadOk(ReadOkMessagePayload { messages }))
    }

    async fn handle_read_ok(
        &mut self,
        msg: &ReadOkMessagePayload,
    ) -> Result<BroadcastPayload, MaelstromError> {
        todo!()
    }

    async fn handle_topology(
        &mut self,
        msg: &TopologyMessagePayload,
    ) -> Result<BroadcastPayload, MaelstromError> {
        let neighbors = msg
            .topology
            .get(&self.node_id)
            .ok_or(MaelstromError::Other(format!(
                "Didn't find self ({}) in topology",
                self.node_id
            )))?;
        self.topology.set_neighbors(neighbors.as_slice());

        Ok(BroadcastPayload::TopologyOk)
    }
}

impl NodeDelegate for BroadcastDelegate {
    type MessageType = BroadcastPayload;
    fn init(
        node_id: impl AsRef<str>,
        node_ids: impl AsRef<Vec<String>>,
        msg_tx: UnboundedSender<BroadcastMessage>,
        msg_rx: UnboundedReceiver<BroadcastMessage>,
        self_tx: UnboundedSender<BroadcastMessage>,
    ) -> Self {
        Self {
            msg_tx,
            msg_rx: Some(msg_rx),
            self_tx,
            outstanding_replies: HashMap::new(),
            node_id: node_id.as_ref().into(),
            msg_id: 0.into(),
            msg_store: MessageStore::new(),
            topology: Topology::new(
                node_ids
                    .as_ref()
                    .iter()
                    .filter(|n| n.as_str() != node_id.as_ref())
                    .map(|s| s.clone())
                    .collect(),
            ),
        }
    }

    // TODO: ways to test this -- add cfg(test) code, maybe to wrap get_msg_tx and whatnot, and interpose a snoop channel tehre that you can read
    fn get_outstanding_replies(&self) -> &HashMap<MessageId, String> {
        &self.outstanding_replies
    }

    fn get_outstanding_replies_mut(&mut self) -> &mut HashMap<MessageId, String> {
        &mut self.outstanding_replies
    }

    fn handle_reply(
        &mut self,
        reply: BroadcastMessage,
    ) -> impl Future<Output = Result<(), MaelstromError>> + Send {
        async { Ok(()) }
    }

    fn handle_message(
        &mut self,
        message: BroadcastMessage,
    ) -> impl Future<Output = Result<(), MaelstromError>> + Send {
        async move {
            let response: BroadcastPayload;
            match &message.body.contents {
                BroadcastPayload::Broadcast(b) => {
                    let src = message.src.as_ref().ok_or(MaelstromError::Other(format!(
                        "No source for message: {:?}",
                        message
                    )))?;
                    self.handle_broadcast(b, src).await?;
                }
                BroadcastPayload::Read => {
                    // TODO: handle sends
                    response = self.handle_read().await?;
                }
                BroadcastPayload::ReadOk(r) => {
                    response = self.handle_read_ok(r).await?;
                }
                BroadcastPayload::Topology(t) => {
                    response = self.handle_topology(t).await?;
                }
                _ => {
                    return Err(MaelstromError::Other(format!(
                        "Unexpected message type: {:?}",
                        message
                    )));
                }
            };

            Ok(())
        }
    }

    fn handle_local_message(
        &mut self,
        msg: maelstrom_csp::message::LocalMessage,
    ) -> impl Future<Output = Result<(), MaelstromError>> + Send {
        async move {
            let (node_id, msg_id) = match msg {
                // TODO: change reply_id to msg_id everywhere
                maelstrom_csp::message::LocalMessage::Cancel(msg_id) => (
                    self.outstanding_replies
                        .remove(&msg_id)
                        .ok_or(MaelstromError::Other(format!(
                            "Couldn't find reply id for message: {:?}",
                            msg
                        )))?,
                    msg_id,
                ),
            };

            let mut node = self
                .topology
                .node_mut(&node_id)
                .ok_or(MaelstromError::Other(format!(
                    "Couldn't find node in neighbors: {}",
                    node_id
                )))?;
            node.die(&node_id, msg_id, self).await?;

            Ok(())
        }
    }

    fn get_msg_id(&mut self) -> &mut MessageId {
        &mut self.msg_id
    }

    fn get_msg_rx(&mut self) -> UnboundedReceiver<BroadcastMessage> {
        self.msg_rx.take().unwrap()
    }

    fn get_msg_tx(&self) -> UnboundedSender<BroadcastMessage> {
        self.msg_tx.clone()
    }

    fn get_self_tx(&self) -> UnboundedSender<Message<Self::MessageType>> {
        self.self_tx.clone()
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let (node, mut ingress, mut egress) =
        get_node_and_io::<BroadcastPayload, BroadcastDelegate>(BufReader::new(stdin()), stdout());

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

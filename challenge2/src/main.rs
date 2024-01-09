#![allow(unused)]

use std::{
    collections::{HashMap, HashSet},
    future::Future,
};

use log::error;
use maelstrom_csp::{
    get_node_and_io,
    message::{ErrorMessagePayload, InitMessagePayload, Message, MessageBody, MessagePayload},
    node::NodeDelegate,
    rpc_error::MaelstromError,
    send,
};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{stdin, stdout, BufReader},
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};

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

    fn neighbors(&self) -> Vec<String> {
        self.neighbors.clone()
    }

    fn set_neighbors(&mut self, neighbors: &[String]) {
        self.neighbors = neighbors.iter().map(|s| s.clone()).collect();
    }
}

struct BroadcastDelegate {
    msg_tx: UnboundedSender<BroadcastMessage>,
    msg_rx: Option<UnboundedReceiver<BroadcastMessage>>,

    node_id: String,
    msg_id: i64,
    msg_store: MessageStore,
    topology: Topology,
}

impl BroadcastDelegate {
    async fn handle_broadcast(
        &mut self,
        msg: &BroadcastMessagePayload,
        src: &str,
    ) -> Result<BroadcastPayload, MaelstromError> {
        if self.msg_store.contains(msg.message) {
            return Ok(BroadcastPayload::BroadcastOk);
        }

        self.msg_store.insert(msg.message);

        for n in self.topology.neighbors().iter().filter(|n| *n != src) {
            let body = MessageBody {
                msg_id: Some(self.next_msg_id()),
                in_reply_to: None,
                local_msg: None,
                contents: BroadcastPayload::Broadcast(BroadcastMessagePayload {
                    message: msg.message,
                }),
            };
            let msg = Self::format_outgoing(Some(n), body);

            send!(self.get_msg_tx(), msg, "Delegate egress hung up: {}");
        }

        Ok(BroadcastPayload::BroadcastOk)
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
        self.topology.set_neighbors(&neighbors);

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
    ) -> Self {
        Self {
            msg_tx,
            msg_rx: Some(msg_rx),
            node_id: node_id.as_ref().into(),
            msg_id: 0,
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
                    response = self.handle_broadcast(b, src).await?;
                }
                BroadcastPayload::Read => {
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

            send!(
                self.get_msg_tx(),
                self.reply(message, response)?,
                "Delegate egress hung up: {}"
            );

            Ok(())
        }
    }

    fn get_msg_id(&mut self) -> &mut i64 {
        &mut self.msg_id
    }

    fn get_msg_rx(&mut self) -> UnboundedReceiver<BroadcastMessage> {
        self.msg_rx.take().unwrap()
    }

    fn get_msg_tx(&self) -> UnboundedSender<BroadcastMessage> {
        self.msg_tx.clone()
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

#![allow(unused)]

use std::collections::{HashMap, HashSet};

use log::{error, info};
use maelstrom_csp::{
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

    fn messages(&self) -> Vec<i64> {
        self.messages.iter().map(|x| *x).collect()
    }
}

struct BroadcastDelegate {
    msg_tx: UnboundedSender<Message<BroadcastPayload>>,
    msg_rx: Option<UnboundedReceiver<Message<BroadcastPayload>>>,

    msg_id: i64,
    msg_store: MessageStore,
}

impl BroadcastDelegate {
    async fn handle_broadcast(
        &mut self,
        msg: BroadcastMessagePayload,
    ) -> Result<BroadcastPayload, MaelstromError> {
        self.msg_store.insert(msg.message);

        Ok(BroadcastPayload::BroadcastOk)
    }

    async fn handle_read(&self) -> Result<BroadcastPayload, MaelstromError> {
        let messages = self.msg_store.messages();

        Ok(BroadcastPayload::ReadOk(ReadOkMessagePayload { messages }))
    }

    async fn handle_read_ok(
        &mut self,
        msg: ReadOkMessagePayload,
    ) -> Result<BroadcastPayload, MaelstromError> {
        todo!()
    }

    async fn handle_topology(
        &mut self,
        msg: TopologyMessagePayload,
    ) -> Result<BroadcastPayload, MaelstromError> {
        Ok(BroadcastPayload::TopologyOk)
    }
}

impl NodeDelegate<BroadcastPayload> for BroadcastDelegate {
    fn init(
        node_id: impl AsRef<str>,
        node_ids: impl AsRef<Vec<String>>,
        msg_tx: tokio::sync::mpsc::UnboundedSender<
            maelstrom_csp::message::Message<BroadcastPayload>,
        >,
        msg_rx: tokio::sync::mpsc::UnboundedReceiver<
            maelstrom_csp::message::Message<BroadcastPayload>,
        >,
    ) -> Self {
        Self {
            msg_tx,
            msg_rx: Some(msg_rx),
            msg_id: 0,
            msg_store: MessageStore::new(),
        }
    }

    fn handle_reply(
        &mut self,
        reply: maelstrom_csp::message::Message<BroadcastPayload>,
    ) -> impl std::future::Future<Output = Result<(), maelstrom_csp::rpc_error::MaelstromError>> + Send
    {
        async { Ok(()) }
    }

    fn handle_message(
        &mut self,
        message: maelstrom_csp::message::Message<BroadcastPayload>,
    ) -> impl std::future::Future<Output = Result<(), maelstrom_csp::rpc_error::MaelstromError>> + Send
    {
        async move {
            let response: BroadcastPayload;
            match message.body.contents.clone() {
                BroadcastPayload::Broadcast(b) => {
                    response = self.handle_broadcast(b).await?;
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

    fn next_msg_id(&mut self) -> i64 {
        self.msg_id += 1;
        self.msg_id
    }

    fn get_msg_rx(
        &mut self,
    ) -> tokio::sync::mpsc::UnboundedReceiver<maelstrom_csp::message::Message<BroadcastPayload>>
    {
        self.msg_rx.take().unwrap()
    }

    fn get_msg_tx(
        &self,
    ) -> tokio::sync::mpsc::UnboundedSender<maelstrom_csp::message::Message<BroadcastPayload>> {
        self.msg_tx.clone()
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let (node, mut ingress, mut egress) = maelstrom_csp::get_node_and_io::<
        BroadcastPayload,
        BroadcastDelegate,
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

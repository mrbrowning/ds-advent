use std::collections::HashSet;

use log::error;
use maelstrom_csp::{
    message::{ErrorMessagePayload, InitMessagePayload, Message, MessageId, MessagePayload},
    node::NodeDelegate,
    rpc_error::MaelstromError,
    send,
};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{stdin, stdout, BufReader},
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};

use message_macro::maelstrom_message;

#[derive(Serialize, Deserialize, Clone, Debug)]
struct EchoRequestPayload {
    echo: String,
}

#[maelstrom_message]
#[derive(Serialize, Deserialize, Clone, Debug)]
enum EchoPayload {
    #[serde(rename = "echo")]
    Echo(EchoRequestPayload),

    #[serde(rename = "echo_ok")]
    EchoOk(EchoRequestPayload),
}

struct EchoDelegate {
    msg_rx: Option<UnboundedReceiver<Message<EchoPayload>>>,
    msg_tx: UnboundedSender<Message<EchoPayload>>,
    self_tx: UnboundedSender<Message<EchoPayload>>,

    msg_id: MessageId,
    outstanding_replies: HashSet<(MessageId, String)>,
}

impl NodeDelegate for EchoDelegate {
    type MessageType = EchoPayload;

    fn init(
        _: impl AsRef<str>,
        _: impl AsRef<Vec<String>>,
        msg_tx: UnboundedSender<Message<Self::MessageType>>,
        msg_rx: UnboundedReceiver<Message<Self::MessageType>>,
        self_tx: UnboundedSender<Message<Self::MessageType>>,
    ) -> Self {
        Self {
            msg_rx: Some(msg_rx),
            msg_tx,
            self_tx,
            msg_id: 1.into(),
            outstanding_replies: HashSet::new(),
        }
    }

    fn get_outstanding_replies(&self) -> &std::collections::HashSet<(MessageId, String)> {
        &self.outstanding_replies
    }

    fn get_outstanding_replies_mut(
        &mut self,
    ) -> &mut std::collections::HashSet<(MessageId, String)> {
        &mut self.outstanding_replies
    }

    #[allow(clippy::manual_async_fn)]
    fn handle_reply(
        &mut self,
        _: Message<Self::MessageType>,
    ) -> impl std::future::Future<Output = Result<(), MaelstromError>> + Send {
        async { Ok(()) }
    }

    #[allow(clippy::manual_async_fn)]
    fn handle_message(
        &mut self,
        message: Message<Self::MessageType>,
    ) -> impl std::future::Future<Output = Result<(), MaelstromError>> + Send {
        async {
            let msg_tx = self.get_msg_tx();
            if let EchoPayload::Echo(e) = message.clone().body.contents {
                let contents = EchoPayload::EchoOk(e);
                send!(msg_tx, self.reply(message, contents)?, "Egress hung up: {}");
            }

            Ok(())
        }
    }

    fn get_msg_id(&mut self) -> &mut MessageId {
        &mut self.msg_id
    }

    fn get_msg_rx(&mut self) -> UnboundedReceiver<Message<Self::MessageType>> {
        self.msg_rx.take().unwrap()
    }

    fn get_msg_tx(&self) -> UnboundedSender<Message<Self::MessageType>> {
        self.msg_tx.clone()
    }

    fn get_self_tx(&self) -> UnboundedSender<Message<Self::MessageType>> {
        self.self_tx.clone()
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let (node, mut ingress, mut egress) = maelstrom_csp::get_node_and_io::<EchoPayload, EchoDelegate>(
        BufReader::new(stdin()),
        stdout(),
    );

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

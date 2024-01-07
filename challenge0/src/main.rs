use log::error;
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

#[derive(Serialize, Deserialize, Clone, Debug)]
struct EchoRequestPayload {
    echo: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type")]
enum EchoPayload {
    #[serde(rename = "echo")]
    Echo(EchoRequestPayload),

    #[serde(rename = "echo_ok")]
    EchoOk(EchoRequestPayload),

    #[serde(rename = "init")]
    Init(InitMessagePayload),

    #[serde(rename = "init_ok")]
    InitOk,

    #[serde(rename = "error")]
    Error(ErrorMessagePayload),
}

impl MessagePayload for EchoPayload {
    fn as_init_msg(&self) -> Option<InitMessagePayload> {
        match self {
            EchoPayload::Init(m) => Some(m.clone()),
            _ => None,
        }
    }

    fn to_init_ok_msg() -> Self {
        Self::InitOk
    }
}

struct EchoDelegate {
    msg_rx: Option<UnboundedReceiver<Message<EchoPayload>>>,
    msg_tx: UnboundedSender<Message<EchoPayload>>,

    msg_id: i64,
}

impl NodeDelegate for EchoDelegate {
    type MessageType = EchoPayload;

    fn init(
        _: impl AsRef<str>,
        _: impl AsRef<Vec<String>>,
        msg_tx: UnboundedSender<Message<Self::MessageType>>,
        msg_rx: UnboundedReceiver<Message<Self::MessageType>>,
    ) -> Self {
        Self {
            msg_rx: Some(msg_rx),
            msg_tx,
            msg_id: 1,
        }
    }

    fn handle_reply(
        &mut self,
        _: Message<Self::MessageType>,
    ) -> impl std::future::Future<Output = Result<(), MaelstromError>> + Send {
        async { Ok(()) }
    }

    fn handle_message(
        &mut self,
        message: Message<Self::MessageType>,
    ) -> impl std::future::Future<Output = Result<(), MaelstromError>> + Send {
        async {
            let msg_tx = self.get_msg_tx();
            match message.clone().body.contents {
                EchoPayload::Echo(e) => {
                    let body = MessageBody {
                        msg_id: Some(self.next_msg_id()),
                        in_reply_to: None,
                        local_msg: None,
                        contents: EchoPayload::EchoOk(e),
                    };

                    send!(msg_tx, Self::reply(message, body)?, "Egress hung up: {}");
                }
                _ => (),
            }

            Ok(())
        }
    }

    fn next_msg_id(&mut self) -> i64 {
        self.msg_id
    }

    fn get_msg_rx(&mut self) -> UnboundedReceiver<Message<Self::MessageType>> {
        self.msg_rx.take().unwrap()
    }

    fn get_msg_tx(&self) -> UnboundedSender<Message<Self::MessageType>> {
        self.msg_tx.clone()
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

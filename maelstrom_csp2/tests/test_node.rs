use std::{collections::HashMap, option::Option};

use common::EchoPayload;
use maelstrom_csp2::{
    message::{InitMessagePayload, Message, MessageBody, MessageId, ParsedInput, ParsedMessage},
    node::{Command, Node, NodeDelegate, ReplyRecord, UninitializedNode},
    rpc_error::MaelstromError,
    send,
};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

mod common;

#[tokio::test]
async fn test_node_runs_init() {
    let (ingress_tx, ingress_rx) = unbounded_channel::<Message<EchoPayload>>();
    let (egress_tx, mut egress_rx) = unbounded_channel::<Message<EchoPayload>>();
    let node: UninitializedNode<EchoPayload, EchoDelegate> =
        UninitializedNode::new(ingress_rx, egress_tx);

    tokio::spawn(async move {
        let initialized = match node.run().await {
            Err(e) => panic!("Uninitialized node died: {}", e),
            Ok(n) => n,
        };
        if let Err(e) = initialized.run().await {
            panic!("Node died: {}", e);
        }
    });

    let nodes = vec!["n1".into(), "n2".into()];
    let init_msg = Message {
        src: None,
        dest: None,
        body: MessageBody {
            msg_id: None,
            in_reply_to: None,
            contents: EchoPayload::Init(InitMessagePayload {
                node_id: "n1".into(),
                node_ids: nodes.clone(),
            }),
        },
    };
    if let Err(e) = ingress_tx.send(init_msg) {
        panic!("Got error: {}", e);
    }

    if let Some(init_ok) = egress_rx.recv().await {
        assert_eq!(init_ok.src, Some("n1".into()));
        match init_ok.body.contents {
            EchoPayload::InitOk => (),
            _ => panic!("Got unexpected message type: {:?}", init_ok),
        }
    } else {
        panic!("Didn't receive init_ok message on channel");
    }

    if let Some(msg) = egress_rx.recv().await {
        match msg.body.contents {
            EchoPayload::Init(init) => {
                assert_eq!(init.node_id, "n1");
                assert_eq!(init.node_ids, nodes);
            }
            _ => panic!("Got unexpected message type: {:?}", msg),
        }
    } else {
        panic!("Failed to receive init info on channel");
    }
}

#[tokio::test]
async fn test_node_runs_echo() {
    let (node, ingress_tx, mut egress_rx) = get_node_and_channels::<EchoDelegate>();

    tokio::spawn(async move {
        if let Err(e) = node.run().await {
            panic!("Node died: {}", e);
        }
    });

    let msg = Message {
        src: Some("n2".into()),
        dest: Some("n1".into()),
        body: MessageBody {
            msg_id: Some(1.into()),
            in_reply_to: None,
            contents: EchoPayload::Echo,
        },
    };
    if let Err(e) = ingress_tx.send(msg) {
        panic!("Got error from ingress_rx: {}", e);
    }
    if egress_rx.recv().await.is_none() {
        panic!("Failed to receive on_start init info on channel");
    }

    if let Some(response) = egress_rx.recv().await {
        assert_eq!(response.src, Some("n1".into()));
        assert_eq!(response.dest, Some("n2".into()));
        assert_eq!(response.body.msg_id, None);
        assert_eq!(response.body.in_reply_to, Some(1.into()));
        match response.body.contents {
            EchoPayload::EchoOk => (),
            _ => panic!("Got unexpected message type: {:?}", response),
        }
    } else {
        panic!("Failed to receive echo response on channel");
    }
}

#[allow(clippy::type_complexity)]
fn get_node_and_channels<D: NodeDelegate<MessageType = EchoPayload> + Send>() -> (
    Node<EchoPayload, D>,
    UnboundedSender<Message<EchoPayload>>,
    UnboundedReceiver<Message<EchoPayload>>,
) {
    let (ingress_tx, ingress_rx) = unbounded_channel::<Message<EchoPayload>>();
    let (egress_tx, egress_rx) = unbounded_channel::<Message<EchoPayload>>();
    let node: Node<EchoPayload, D> = Node::new(
        "n1".into(),
        vec!["n1".into(), "n2".into()],
        ingress_rx,
        egress_tx,
    );

    (node, ingress_tx, egress_rx)
}

#[derive(Clone, Debug)]
struct EchoCommand;

struct EchoDelegate {
    node_id: String,
    node_ids: Vec<String>,

    msg_tx: UnboundedSender<Message<EchoPayload>>,
    msg_rx: Option<UnboundedReceiver<ParsedInput<EchoPayload>>>,

    cmd_tx: UnboundedSender<Command<EchoPayload, EchoCommand>>,
    cmd_rx: Option<UnboundedReceiver<Command<EchoPayload, EchoCommand>>>,

    msg_id: MessageId,
}

impl NodeDelegate for EchoDelegate {
    type MessageType = EchoPayload;
    type CommandType = EchoCommand;

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
            msg_id: 1.into(),
        }
    }

    fn get_reply_records_mut(
        &mut self,
    ) -> &mut HashMap<MessageId, ReplyRecord<Self::MessageType, Self::CommandType>> {
        todo!()
    }

    #[allow(clippy::manual_async_fn)]
    fn on_start(
        &mut self,
    ) -> impl futures::prelude::Future<Output = Result<(), MaelstromError>> + Send {
        async move {
            send!(
                self.get_msg_tx(),
                self.send(
                    "out".into(),
                    EchoPayload::Init(InitMessagePayload {
                        node_id: self.node_id.clone(),
                        node_ids: self.node_ids.clone(),
                    })
                ),
                "Egress hung up: {}"
            );

            Ok(())
        }
    }

    #[allow(clippy::manual_async_fn)]
    fn handle_message(
        &mut self,
        _: ParsedMessage<Self::MessageType>,
    ) -> impl std::future::Future<Output = Result<(), MaelstromError>> + Send {
        async move { todo!() }
    }

    #[allow(clippy::manual_async_fn)]
    fn handle_rpc(
        &mut self,
        message: maelstrom_csp2::message::ParsedRpc<Self::MessageType>,
    ) -> impl futures::prelude::Future<Output = Result<(), MaelstromError>> + Send {
        async move {
            let msg_tx = self.get_msg_tx();
            if let EchoPayload::Echo = message.body {
                send!(
                    msg_tx,
                    self.reply(message.src, message.msg_id, EchoPayload::EchoOk),
                    "Egress hung up: {}"
                );
            }

            Ok(())
        }
    }

    #[allow(clippy::manual_async_fn)]
    fn handle_custom_command(
        &mut self,
        _: Self::CommandType,
    ) -> impl futures::prelude::Future<Output = Result<(), MaelstromError>> + Send {
        async { todo!() }
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

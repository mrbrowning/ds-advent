use std::{collections::HashSet, time::Duration};

use common::EchoPayload;
use maelstrom_csp::{
    message::{InitMessagePayload, Message, MessageBody},
    node::{Node, NodeDelegate, UninitializedNode},
    rpc_error::MaelstromError,
    send,
};
use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    time::timeout,
};

mod common;

#[tokio::test]
async fn test_node_runs_init() {
    let (ingress_tx, ingress_rx) = unbounded_channel::<Message<EchoPayload>>();
    let (egress_tx, mut egress_rx) = unbounded_channel::<Message<EchoPayload>>();
    let node: UninitializedNode<EchoPayload, EchoDelegate> =
        UninitializedNode::new(ingress_tx.clone(), ingress_rx, egress_tx);

    let (node_id_tx, mut node_id_rx) = unbounded_channel::<(String, Vec<String>)>();
    tokio::spawn(async move {
        let initialized = match node.run().await {
            Err(e) => panic!("Uninitialized node died: {}", e),
            Ok(n) => n,
        };

        let _ = node_id_tx.send((initialized.node_id().into(), initialized.node_ids().into()));
    });

    let nodes = vec!["n1".into(), "n2".into()];
    let init_msg = Message {
        src: None,
        dest: None,
        body: MessageBody {
            msg_id: None,
            in_reply_to: None,
            local_msg: None,
            contents: EchoPayload::Init(InitMessagePayload {
                node_id: "n1".into(),
                node_ids: nodes.clone(),
            }),
        },
    };
    if let Err(e) = ingress_tx.send(init_msg) {
        panic!("Got error: {}", e);
    }

    if let Some((node_id, node_ids)) = node_id_rx.recv().await {
        assert_eq!(node_id, "n1");
        assert_eq!(node_ids, nodes);
    } else {
        panic!("Failed to receive init info on channel");
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
            msg_id: Some(1),
            in_reply_to: None,
            local_msg: None,
            contents: EchoPayload::Echo,
        },
    };
    if let Err(e) = ingress_tx.send(msg) {
        panic!("Got error from ingress_rx: {}", e);
    }

    if let Some(response) = egress_rx.recv().await {
        assert_eq!(response.src, Some("n1".into()));
        assert_eq!(response.dest, Some("n2".into()));
        assert_eq!(response.body.msg_id, None);
        assert_eq!(response.body.in_reply_to, Some(1));
        match response.body.contents {
            EchoPayload::EchoOk => (),
            _ => panic!("Got unexpected message type: {:?}", response),
        }
    } else {
        panic!("Failed to receive echo response on channel");
    }
}

#[tokio::test]
async fn test_node_handles_rpc() {
    let (node, ingress_tx, mut egress_rx) = get_node_and_channels::<RpcDelegate>();
    let this_node = node.node_id().to_string();
    let other_node = "n2".to_string();

    tokio::spawn(async move {
        if let Err(e) = node.run().await {
            panic!("Node died: {}", e);
        }
    });

    let msg = Message {
        src: Some(other_node),
        dest: Some(this_node),
        body: MessageBody {
            msg_id: Some(1),
            in_reply_to: None,
            local_msg: None,
            contents: EchoPayload::Echo,
        },
    };
    if let Err(e) = ingress_tx.send(msg) {
        panic!("Got error from ingress_rx: {}", e);
    }

    match timeout(Duration::from_millis(100), egress_rx.recv()).await {
        Ok(Some(response)) => {
            assert_eq!(response.dest, Some("n2".into()));
        }
        Ok(None) => {
            panic!("Egress hung up");
        }
        Err(_) => {
            panic!("Egress recv timed out for EchoOk");
        }
    }

    let msg = Message {
        src: Some("n2".into()),
        dest: Some("n1".into()),
        body: MessageBody {
            msg_id: Some(1),
            in_reply_to: Some(1),
            local_msg: None,
            contents: EchoPayload::EchoOk,
        },
    };
    if let Err(e) = ingress_tx.send(msg) {
        panic!("Got error from ingress_rx: {}", e);
    }

    match timeout(Duration::from_millis(1000), egress_rx.recv()).await {
        Ok(Some(response)) => {
            assert_eq!(response.src, Some("n1".into()));
            assert_eq!(response.dest, Some("n2".into()));
            assert_eq!(response.body.msg_id, Some(2));
            match response.body.contents {
                EchoPayload::ReplyOk => (),
                _ => panic!("Got unexpected message type: {:?}", response),
            }
        }
        Ok(None) => {
            panic!("Failed to receive echo response on channel");
        }
        Err(_) => {
            panic!("Egress recv timed out for ReplyOk");
        }
    }
}

#[tokio::test]
async fn test_node_ignores_unexpected_reply() {
    let (node, ingress_tx, mut egress_rx) = get_node_and_channels::<RpcDelegate>();
    let this_node = node.node_id().to_string();
    let other_node = "n2".to_string();

    tokio::spawn(async move {
        if let Err(e) = node.run().await {
            panic!("Node died: {}", e);
        }
    });

    let msg = Message {
        src: Some(other_node),
        dest: Some(this_node),
        body: MessageBody {
            msg_id: Some(1),
            in_reply_to: None,
            local_msg: None,
            contents: EchoPayload::Echo,
        },
    };
    if let Err(e) = ingress_tx.send(msg) {
        panic!("Got error from ingress_rx: {}", e);
    }

    match timeout(Duration::from_millis(100), egress_rx.recv()).await {
        Ok(Some(response)) => {
            assert_eq!(response.dest, Some("n2".into()));
        }
        Ok(None) => {
            panic!("Egress hung up");
        }
        Err(_) => {
            panic!("Egress recv timed out for EchoOk");
        }
    }

    let msg = Message {
        src: Some("n2".into()),
        dest: Some("n1".into()),
        body: MessageBody {
            msg_id: Some(1),
            in_reply_to: Some(2),
            local_msg: None,
            contents: EchoPayload::EchoOk,
        },
    };
    if let Err(e) = ingress_tx.send(msg) {
        panic!("Got error from ingress_rx: {}", e);
    }

    // You caught me, this is a bad way to test this. I got ahead of myself and didn't design for testability here.
    match timeout(Duration::from_millis(100), egress_rx.recv()).await {
        Ok(Some(response)) => {
            panic!(
                "Got unexpected reply to non-existent reply id {:?}: {:?}",
                response.body.in_reply_to, response
            );
        }
        Ok(None) => {
            panic!("Failed to receive echo response on channel");
        }
        _ => (),
    }
}

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
        ingress_tx.clone(),
        ingress_rx,
        egress_tx,
    );

    (node, ingress_tx, egress_rx)
}

struct EchoDelegate {
    msg_rx: Option<UnboundedReceiver<Message<EchoPayload>>>,
    msg_tx: UnboundedSender<Message<EchoPayload>>,
    self_tx: UnboundedSender<Message<EchoPayload>>,

    msg_id: i64,
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
            msg_id: 1,
        }
    }

    fn get_outstanding_replies(&self) -> &std::collections::HashSet<i64> {
        todo!()
    }

    fn get_outstanding_replies_mut(&mut self) -> &mut std::collections::HashSet<i64> {
        todo!()
    }

    fn handle_reply(
        &mut self,
        _: Message<Self::MessageType>,
    ) -> impl std::future::Future<Output = Result<(), MaelstromError>> + Send {
        async move { Ok(()) }
    }

    fn handle_message(
        &mut self,
        message: Message<Self::MessageType>,
    ) -> impl std::future::Future<Output = Result<(), MaelstromError>> + Send {
        async move {
            let msg_tx = self.get_msg_tx();
            match message.body.contents {
                EchoPayload::Echo => {
                    send!(
                        msg_tx,
                        self.reply(message, EchoPayload::EchoOk)?,
                        "Egress hung up: {}"
                    );
                }
                _ => (),
            }

            Ok(())
        }
    }

    fn get_msg_id(&mut self) -> &mut i64 {
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

struct RpcDelegate {
    msg_rx: Option<UnboundedReceiver<Message<EchoPayload>>>,
    msg_tx: UnboundedSender<Message<EchoPayload>>,
    self_tx: UnboundedSender<Message<EchoPayload>>,

    msg_id: i64,
    outstanding_replies: HashSet<i64>,
}

impl NodeDelegate for RpcDelegate {
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
            msg_id: 0,
            outstanding_replies: HashSet::new(),
        }
    }

    fn get_outstanding_replies(&self) -> &std::collections::HashSet<i64> {
        &self.outstanding_replies
    }

    fn get_outstanding_replies_mut(&mut self) -> &mut std::collections::HashSet<i64> {
        &mut self.outstanding_replies
    }

    fn handle_reply(
        &mut self,
        _: Message<Self::MessageType>,
    ) -> impl std::future::Future<Output = Result<(), MaelstromError>> + Send {
        async move {
            let msg_tx = self.get_msg_tx();
            let body = MessageBody {
                msg_id: Some(2),
                in_reply_to: None,
                local_msg: None,
                contents: EchoPayload::ReplyOk,
            };

            if let Err(e) = msg_tx.send(Self::format_outgoing(Some("n2"), body)) {
                return Err(MaelstromError::ChannelError(format!(
                    "Egress hung up: {}",
                    e
                )));
            }

            Ok(())
        }
    }

    fn handle_message(
        &mut self,
        message: Message<Self::MessageType>,
    ) -> impl std::future::Future<Output = Result<(), MaelstromError>> + Send {
        async move {
            let msg_tx = self.get_msg_tx();
            match message.body.contents {
                EchoPayload::Echo => {
                    send!(
                        msg_tx,
                        self.rpc(message.src, EchoPayload::EchoOk),
                        "Egress hung up: {}"
                    );
                }
                _ => (),
            }

            Ok(())
        }
    }

    fn get_msg_id(&mut self) -> &mut i64 {
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

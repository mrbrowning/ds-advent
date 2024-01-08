use std::{collections::HashSet, fmt::Display, marker::PhantomData};

use log::info;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::{
    message::{
        ErrorMessagePayload, InitMessagePayload, LocalMessage, Message, MessageBody, MessagePayload,
    },
    rpc_error::{ErrorType, MaelstromError, RPCError},
    send,
};

pub trait NodeDelegate {
    type MessageType: MessagePayload + Send;

    fn init(
        node_id: impl AsRef<str>,
        node_ids: impl AsRef<Vec<String>>,
        msg_tx: UnboundedSender<Message<Self::MessageType>>,
        msg_rx: UnboundedReceiver<Message<Self::MessageType>>,
    ) -> Self;

    fn on_start(&mut self) -> impl std::future::Future<Output = Result<(), MaelstromError>> + Send {
        async { Ok(()) }
    }

    fn handle_reply(
        &mut self,
        reply: Message<Self::MessageType>,
    ) -> impl std::future::Future<Output = Result<(), MaelstromError>> + Send;

    fn handle_message(
        &mut self,
        message: Message<Self::MessageType>,
    ) -> impl std::future::Future<Output = Result<(), MaelstromError>> + Send;

    fn next_msg_id(&mut self) -> i64;

    fn error_body(err: MaelstromError) -> ErrorMessagePayload {
        fn wrap_err(err: impl Display) -> ErrorMessagePayload {
            ErrorMessagePayload {
                code: ErrorType::Crash.into(),
                text: format!("{}", err),
            }
        }

        match err {
            MaelstromError::RPCError(e) => ErrorMessagePayload {
                code: e.error_code(),
                text: e.message().into(),
            },
            MaelstromError::SerializationError(e) => wrap_err(e),
            MaelstromError::IOError(e) => wrap_err(e),
            MaelstromError::Other(e) => wrap_err(e),
            MaelstromError::ChannelError(_) => unreachable!(),
        }
    }

    fn format_outgoing(
        dest: Option<impl AsRef<str>>,
        body: MessageBody<Self::MessageType>,
    ) -> Message<Self::MessageType> {
        Message {
            src: None,
            dest: dest.map(|d| d.as_ref().into()),
            body: body,
        }
    }

    fn send(&self, dest: Option<impl AsRef<str>>, contents: Self::MessageType) -> Message<Self::MessageType> {
        let body = MessageBody {
            msg_id: None,
            in_reply_to: None,
            local_msg: None,
            contents,
        };

        Self::format_outgoing(dest, body)
    }

    fn reply(&self, request: Message<Self::MessageType>, contents: Self::MessageType) -> Result<Message<Self::MessageType>, MaelstromError> {
        let in_reply_to = Some(request.body.msg_id.ok_or(MaelstromError::RPCError(
            RPCError::new(
                ErrorType::MalformedRequest.into(),
                "Message body missing msg_id".to_string(),
            ),
        ))?);
        let body = MessageBody {
            msg_id: None,
            in_reply_to,
            local_msg: None,
            contents,
        };

        Ok(Self::format_outgoing(request.src, body))
    }

    fn rpc(&mut self, dest: Option<impl AsRef<str>>, contents: Self::MessageType) -> Message<Self::MessageType> {
        let body = MessageBody {
            msg_id: Some(self.next_msg_id()),
            in_reply_to: None,
            local_msg: None,
            contents,
        };

        Self::format_outgoing(dest, body)
    }

    fn get_msg_rx(&mut self) -> UnboundedReceiver<Message<Self::MessageType>>;

    fn get_msg_tx(&self) -> UnboundedSender<Message<Self::MessageType>>;

    fn run(&mut self) -> impl std::future::Future<Output = Result<(), MaelstromError>> + Send
    where
        Self: std::marker::Send,
    {
        async {
            if let Err(e) = self.on_start().await {
                match e {
                    MaelstromError::ChannelError(_) => return Err(e),
                    _ => (),
                }
            }

            let mut msg_rx = self.get_msg_rx();
            loop {
                let msg = {
                    let msg = msg_rx.recv().await;
                    if msg.is_none() {
                        return Err(MaelstromError::ChannelError("node hung up".into()));
                    }

                    msg.unwrap()
                };

                if msg.body.in_reply_to.is_some() {
                    let res = self.handle_reply(msg).await;
                    match res {
                        Err(MaelstromError::ChannelError(_)) => return Err(res.err().unwrap()),
                        _ => (),
                    }
                } else {
                    let res = self.handle_message(msg).await;
                    match res {
                        Err(MaelstromError::ChannelError(_)) => return Err(res.err().unwrap()),
                        _ => (),
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct UninitializedNode<M: MessagePayload + Send, D: NodeDelegate<MessageType = M> + 'static> {
    ingress_tx: UnboundedSender<Message<M>>,
    ingress_rx: UnboundedReceiver<Message<M>>,
    egress_tx: UnboundedSender<Message<M>>,

    _msg_marker: PhantomData<M>,
    _delegate_marker: PhantomData<D>,
}

impl<M: MessagePayload + Send, D: NodeDelegate<MessageType = M> + Send> UninitializedNode<M, D> {
    pub fn new(
        ingress_tx: UnboundedSender<Message<M>>,
        ingress_rx: UnboundedReceiver<Message<M>>,
        egress_tx: UnboundedSender<Message<M>>,
    ) -> Self {
        Self {
            ingress_tx,
            ingress_rx,
            egress_tx,
            _msg_marker: PhantomData,
            _delegate_marker: PhantomData,
        }
    }

    pub async fn run(mut self) -> Result<Node<M, D>, MaelstromError> {
        let init_msg: InitMessagePayload;
        let in_reply_to: Option<i64>;
        let dest: Option<String>;

        loop {
            let msg = {
                let msg = self.ingress_rx.recv().await;
                if msg.is_none() {
                    return Err(MaelstromError::ChannelError(
                        "message ingress hung up".into(),
                    ));
                }

                msg.unwrap()
            };

            init_msg = {
                if msg.body.contents.as_init_msg().is_none() {
                    info!("Not initialized, ignoring message: {:?}", msg);
                    continue;
                }

                msg.body.contents.as_init_msg().unwrap()
            };
            dest = msg.src;
            in_reply_to = msg.body.msg_id;

            break;
        }

        let response: Message<M> = Message {
            src: Some(init_msg.node_id.clone()),
            dest,
            body: MessageBody {
                msg_id: None,
                in_reply_to,
                local_msg: None,
                contents: M::to_init_ok_msg(),
            },
        };
        send!(self.egress_tx, response, "Egress hung up: {}");

        Ok(Node::new(
            init_msg.node_id,
            init_msg.node_ids,
            self.ingress_tx,
            self.ingress_rx,
            self.egress_tx,
        ))
    }
}

#[derive(Debug)]
pub struct Node<M: MessagePayload + Send, D: NodeDelegate<MessageType = M> + 'static> {
    delegate: Option<D>,
    delegate_tx: UnboundedSender<Message<M>>,

    node_id: String,
    node_ids: Vec<String>,

    ingress_rx: UnboundedReceiver<Message<M>>,
    egress_tx: UnboundedSender<Message<M>>,

    outstanding_replies: HashSet<i64>,
}

impl<M: MessagePayload + Send, D: NodeDelegate<MessageType = M> + Send> Node<M, D> {
    pub fn new(
        node_id: String,
        node_ids: Vec<String>,
        ingress_tx: UnboundedSender<Message<M>>,
        ingress_rx: UnboundedReceiver<Message<M>>,
        egress_tx: UnboundedSender<Message<M>>,
    ) -> Self {
        let (delegate_tx, delegate_rx) = unbounded_channel::<Message<M>>();
        let delegate = D::init(
            node_id.clone(),
            node_ids.clone(),
            ingress_tx.clone(),
            delegate_rx,
        );

        Self {
            delegate: Some(delegate),
            delegate_tx,
            node_id: node_id,
            node_ids: node_ids.into(),
            ingress_rx,
            egress_tx,
            outstanding_replies: HashSet::new(),
        }
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn node_ids(&self) -> &[String] {
        &self.node_ids
    }

    #[cfg(test)]
    pub fn outstanding_replies(&mut self) -> &mut HashSet<i64> {
        &mut self.outstanding_replies
    }

    fn handle_local_message(&mut self, message: LocalMessage) {
        match message {
            LocalMessage::Cancel(reply_id) => {
                self.outstanding_replies.remove(&reply_id);
            }
        }
    }

    pub async fn run(mut self) -> Result<(), MaelstromError> {
        let mut delegate = self.delegate.take().unwrap();

        tokio::spawn(async move {
            if let Err(e) = delegate.run().await {
                panic!("Delegate failed: {}", e);
            }
        });

        loop {
            let mut msg = {
                let msg = self.ingress_rx.recv().await;
                if msg.is_none() {
                    return Err(MaelstromError::ChannelError(
                        "message ingress hung up".into(),
                    ));
                }

                msg.unwrap()
            };

            if msg.body.local_msg.is_some() {
                self.handle_local_message(msg.body.local_msg.take().unwrap());
                continue;
            }

            if msg.dest.is_none() {
                info!("Ignoring message with no destination: {:?}", msg);
                continue;
            }

            let dest = msg.dest.as_ref().unwrap().as_str();
            if dest != self.node_id {
                msg.src = Some(self.node_id.clone());

                if msg.body.msg_id.is_some() {
                    // We're expecting a reply to this and will let it through when it arrives.
                    self.outstanding_replies
                        .insert(*msg.body.msg_id.as_ref().unwrap());
                }
                send!(self.egress_tx, msg, "Message egress hung up: {}");

                continue;
            }

            info!("Received {:?}", msg);
            if msg.body.in_reply_to.is_some()
                && !self
                    .outstanding_replies
                    .contains(msg.body.in_reply_to.as_ref().unwrap())
            {
                // This either timed out according to the delegate or we registered it spuriously. Either way, ignore.
                info!("Ignoring unexpected reply: {:?}", msg);
                continue;
            } else if msg.body.contents.as_init_msg().is_some() {
                info!("Ignoring init message to initialized node");
                continue;
            }

            send!(self.delegate_tx, msg, "Node delegate hung up: {}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use serde::{Deserialize, Serialize};
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_node_cancels_expected_reply() {
        let (mut node, _, _) = get_node_and_channels();
        node.outstanding_replies().insert(1);

        node.handle_local_message(LocalMessage::Cancel(1));
        assert!(node.outstanding_replies().is_empty());
    }

    #[tokio::test]
    async fn test_node_ignores_unexpected_reply() {
        let (node, ingress_tx, _egress_rx) = get_node_and_channels();

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
                in_reply_to: Some(1),
                local_msg: None,
                contents: TestPayload::Empty,
            },
        };
        if let Err(e) = ingress_tx.send(msg) {
            panic!("Got error from ingress_rx: {}", e);
        }

        // You caught me, this is a bad way to test this. I got ahead of myself and didn't design for testability here.
        sleep(Duration::from_millis(500)).await;
    }

    #[tokio::test]
    async fn test_delegate_runs_on_start() {
        let (mut delegate, _ingress_tx, mut egress_rx) = get_delegate_and_channels();

        tokio::spawn(async move {
            if let Err(e) = delegate.run().await {
                panic!("Delegate died: {}", e);
            }
        });

        if let Some(m) = egress_rx.recv().await {
            assert_eq!(m.dest, Some("n1".into()));
            assert_eq!(m.body.msg_id, Some(1));
        } else {
            panic!("Failed to receive on_start message from delegate");
        }
    }

    #[test]
    fn test_delegate_formats_send_msg() {
        let (delegate, _, _) = get_delegate_and_channels();
        let dest = "n1".to_string();

        let contents = TestPayload::Empty;
        let sent = delegate.send(Some(dest.clone()), contents);

        assert!(sent.src.is_none());
        assert_eq!(sent.dest, Some(dest));
        assert!(sent.body.msg_id.is_none());
        assert!(sent.body.in_reply_to.is_none());
        assert!(sent.body.local_msg.is_none());
    }

    #[test]
    fn test_delegate_formats_reply_msg() {
        let (delegate, _, _) = get_delegate_and_channels();
        let dest = "n1".to_string();
        let request = Message {
            src: Some(dest.clone()),
            dest: None,
            body: MessageBody {
                msg_id: Some(1),
                in_reply_to: None,
                local_msg: None,
                contents: TestPayload::Empty,
            },
        };

        let contents = TestPayload::Empty;
        let sent = delegate.reply(request, contents).unwrap();

        assert!(sent.src.is_none());
        assert_eq!(sent.dest, Some(dest));
        assert!(sent.body.msg_id.is_none());
        assert_eq!(sent.body.in_reply_to, Some(1));
        assert!(sent.body.local_msg.is_none());
    }

    #[test]
    fn test_delegate_formats_rpc_msg() {
        let (mut delegate, _, _) = get_delegate_and_channels();
        let dest = "n1".to_string();

        let contents = TestPayload::Empty;
        let sent = delegate.rpc(Some(&dest), contents);

        assert!(sent.src.is_none());
        assert_eq!(sent.dest, Some(dest));
        assert_eq!(sent.body.msg_id, Some(1));
        assert!(sent.body.in_reply_to.is_none());
        assert!(sent.body.local_msg.is_none());
    }

    type TestMessage = Message<TestPayload>;

    fn get_node_and_channels() -> (
        Node<TestPayload, TestDelegate>,
        UnboundedSender<Message<TestPayload>>,
        UnboundedReceiver<Message<TestPayload>>,
    ) {
        let (ingress_tx, ingress_rx) = unbounded_channel::<Message<TestPayload>>();
        let (egress_tx, egress_rx) = unbounded_channel::<Message<TestPayload>>();
        let (delegate_tx, delegate_rx) = unbounded_channel::<Message<TestPayload>>();

        let node: Node<TestPayload, TestDelegate> = Node {
            delegate: Some(TestDelegate {
                msg_tx: ingress_tx.clone(),
                msg_rx: Some(delegate_rx),
                msg_id: 0,
            }),
            delegate_tx,
            node_id: "n1".into(),
            node_ids: vec!["n1".into(), "n2".into()],

            ingress_rx,
            egress_tx,

            outstanding_replies: HashSet::new(),
        };

        (node, ingress_tx, egress_rx)
    }

    fn get_delegate_and_channels() -> (
        TestDelegate,
        UnboundedSender<TestMessage>,
        UnboundedReceiver<TestMessage>,
    ) {
        let (ingress_tx, ingress_rx) = unbounded_channel::<Message<TestPayload>>();
        let (egress_tx, egress_rx) = unbounded_channel::<Message<TestPayload>>();
        let delegate = TestDelegate::init("", vec![], egress_tx, ingress_rx);

        (delegate, ingress_tx, egress_rx)
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    #[serde(tag = "type")]
    enum TestPayload {
        #[serde(rename = "empty")]
        Empty,
    }

    impl MessagePayload for TestPayload {
        fn as_init_msg(&self) -> Option<InitMessagePayload> {
            None
        }

        fn to_init_ok_msg() -> Self {
            todo!()
        }
    }

    struct TestDelegate {
        msg_tx: UnboundedSender<Message<TestPayload>>,
        msg_rx: Option<UnboundedReceiver<Message<TestPayload>>>,

        msg_id: i64,
    }

    impl NodeDelegate for TestDelegate {
        type MessageType = TestPayload;

        fn init(
            _: impl AsRef<str>,
            _: impl AsRef<Vec<String>>,
            msg_tx: UnboundedSender<Message<Self::MessageType>>,
            msg_rx: UnboundedReceiver<Message<Self::MessageType>>,
        ) -> Self {
            Self {
                msg_tx,
                msg_rx: Some(msg_rx),
                msg_id: 0,
            }
        }

        fn on_start(
            &mut self,
        ) -> impl std::future::Future<Output = Result<(), MaelstromError>> + Send {
            async move {
                let msg_tx = self.get_msg_tx();

                let msg = Message {
                    src: None,
                    dest: Some("n1".into()),
                    body: MessageBody {
                        msg_id: Some(1),
                        in_reply_to: None,
                        local_msg: None,
                        contents: TestPayload::Empty,
                    },
                };
                send!(msg_tx, msg, "Delegate egress hung up: {}");

                Ok(())
            }
        }

        fn handle_reply(
            &mut self,
            _: Message<Self::MessageType>,
        ) -> impl std::future::Future<Output = Result<(), MaelstromError>> + Send {
            async { panic!("Reply should have been ignored") }
        }

        fn handle_message(
            &mut self,
            _: Message<Self::MessageType>,
        ) -> impl std::future::Future<Output = Result<(), MaelstromError>> + Send {
            async { Ok(()) }
        }

        fn next_msg_id(&mut self) -> i64 {
            self.msg_id += 1;
            self.msg_id
        }

        fn get_msg_rx(&mut self) -> UnboundedReceiver<Message<Self::MessageType>> {
            self.msg_rx.take().unwrap()
        }

        fn get_msg_tx(&self) -> UnboundedSender<Message<Self::MessageType>> {
            self.msg_tx.clone()
        }
    }
}

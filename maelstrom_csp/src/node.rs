use std::{
    collections::HashSet,
    future::Future,
    time::Duration,
    {fmt::Display, marker::PhantomData},
};

use futures::FutureExt;
use log::info;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::{
    message::{
        ErrorMessagePayload, InitMessagePayload, LocalMessage, Message, MessageBody, MessageId,
        MessagePayload,
    },
    rpc_error::{ErrorType, MaelstromError, RPCError},
    send,
};

pub trait NodeDelegate {
    type MessageType: MessagePayload + Clone + Send + 'static;

    fn init(
        node_id: impl AsRef<str>,
        node_ids: impl AsRef<Vec<String>>,
        msg_tx: UnboundedSender<Message<Self::MessageType>>,
        msg_rx: UnboundedReceiver<Message<Self::MessageType>>,
        self_tx: UnboundedSender<Message<Self::MessageType>>,
    ) -> Self;

    fn on_start(&mut self) -> impl Future<Output = Result<(), MaelstromError>> + Send {
        async { Ok(()) }
    }

    fn get_outstanding_replies(&self) -> &HashSet<(MessageId, String)>;

    fn get_outstanding_replies_mut(&mut self) -> &mut HashSet<(MessageId, String)>;

    fn is_expecting_reply(&self, msg_id: MessageId, node: impl Into<String>) -> bool {
        self.get_outstanding_replies()
            .contains(&(msg_id, node.into()))
    }

    fn expect_reply(&mut self, msg_id: MessageId, node_id: impl Into<String>) {
        self.get_outstanding_replies_mut()
            .insert((msg_id, node_id.into()));
    }

    fn cancel_reply(&mut self, msg_id: MessageId, node: impl Into<String>) {
        self.get_outstanding_replies_mut()
            .remove(&(msg_id, node.into()));
    }

    fn receive_reply(
        &mut self,
        reply: Message<Self::MessageType>,
    ) -> impl Future<Output = Result<(), MaelstromError>> + Send
    where
        Self: Send,
    {
        async {
            let in_reply_to = reply.body.in_reply_to.ok_or(MaelstromError::Other(format!(
                "Reply missing in_reply_to: {:?}",
                reply
            )))?;
            if self.is_expecting_reply(in_reply_to, reply.src.clone().unwrap()) {
                return self.handle_reply(reply).await;
            }
            info!("Got unexpected reply: {:?}", reply);

            Ok(())
        }
    }

    fn handle_local_message(
        &mut self,
        msg: LocalMessage,
    ) -> impl Future<Output = Result<(), MaelstromError>> + Send {
        match msg {
            LocalMessage::Cancel(reply_id, node) => {
                self.cancel_reply(reply_id, node);
            }
        }
        async { Ok(()) }
    }

    fn handle_reply(
        &mut self,
        reply: Message<Self::MessageType>,
    ) -> impl Future<Output = Result<(), MaelstromError>> + Send;

    fn handle_message(
        &mut self,
        message: Message<Self::MessageType>,
    ) -> impl Future<Output = Result<(), MaelstromError>> + Send;

    fn get_msg_id(&mut self) -> &mut MessageId;

    fn next_msg_id(&mut self) -> MessageId {
        let msg_id = self.get_msg_id();
        *msg_id += 1;

        *msg_id
    }

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

    fn handle_err(
        &self,
        request: Message<Self::MessageType>,
        err: MaelstromError,
    ) -> Result<(), MaelstromError> {
        if let MaelstromError::ChannelError(_) = err {
            return Err(err);
        }

        match self.reply(
            request,
            Self::MessageType::to_err_msg(Self::error_body(err)),
        ) {
            Ok(r) => self
                .get_msg_tx()
                .send(r)
                .map_err(|e| MaelstromError::ChannelError(format!("Egress hung up: {}", e))),
            Err(e) => {
                info!("Couldn't send error reply: {}", e);
                Ok(())
            }
        }
    }

    fn format_outgoing(
        dest: Option<impl AsRef<str>>,
        body: MessageBody<Self::MessageType>,
    ) -> Message<Self::MessageType> {
        Message {
            src: None,
            dest: dest.map(|d| d.as_ref().into()),
            body,
        }
    }

    fn send(
        &self,
        dest: Option<impl AsRef<str>>,
        contents: Self::MessageType,
    ) -> Message<Self::MessageType> {
        let body = MessageBody {
            msg_id: None,
            in_reply_to: None,
            local_msg: None,
            contents,
        };

        Self::format_outgoing(dest, body)
    }

    fn reply(
        &self,
        request: Message<Self::MessageType>,
        contents: Self::MessageType,
    ) -> Result<Message<Self::MessageType>, MaelstromError> {
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

    fn rpc(
        &mut self,
        dest: impl AsRef<str>,
        contents: Self::MessageType,
    ) -> Message<Self::MessageType> {
        let msg_id = self.next_msg_id();

        self.rpc_with_msg_id(dest, contents, msg_id)
    }

    fn rpc_with_msg_id(
        &mut self,
        dest: impl AsRef<str>,
        contents: Self::MessageType,
        msg_id: MessageId,
    ) -> Message<Self::MessageType> {
        let body = MessageBody {
            msg_id: Some(msg_id),
            in_reply_to: None,
            local_msg: None,
            contents,
        };
        self.expect_reply(msg_id, dest.as_ref());

        Self::format_outgoing(Some(dest), body)
    }

    fn sync_rpc(
        &mut self,
        dest: impl AsRef<str>,
        contents: Self::MessageType,
        on_send: Box<dyn FnOnce(MessageId) -> Box<dyn Future<Output = ()> + Send + 'static>>,
    ) -> impl Future<Output = Result<MessageId, MaelstromError>> + Send {
        let msg_id = self.next_msg_id();

        self.sync_rpc_with_msg_id(dest, contents, on_send, msg_id)
            .map(move |r| r.map(move |_| msg_id))
    }

    fn sync_rpc_with_msg_id(
        &mut self,
        dest: impl AsRef<str>,
        contents: Self::MessageType,
        on_send: Box<dyn FnOnce(MessageId) -> Box<dyn Future<Output = ()> + Send + 'static>>,
        msg_id: MessageId,
    ) -> impl Future<Output = Result<(), MaelstromError>> + Send {
        let outgoing = self.rpc_with_msg_id(dest, contents, msg_id);
        let msg_tx = self.get_msg_tx();
        let fut = Box::into_pin(on_send(msg_id));

        async move {
            send!(msg_tx, outgoing, "Delegate egress hung up: {}");
            tokio::spawn(fut);

            Ok(())
        }
    }

    fn rpc_with_timeout(
        &mut self,
        dest: impl Into<String>,
        contents: Self::MessageType,
        duration: u64,
    ) -> impl Future<Output = Result<MessageId, MaelstromError>> + Send {
        let msg_id = self.next_msg_id();

        self.rpc_with_timeout_with_msg_id(dest.into(), contents, duration, msg_id)
            .map(move |r| r.map(move |_| msg_id))
    }

    fn rpc_with_timeout_with_msg_id(
        &mut self,
        dest: String,
        contents: Self::MessageType,
        duration: u64,
        msg_id: MessageId,
    ) -> impl Future<Output = Result<(), MaelstromError>> + Send {
        let msg_tx = self.get_self_tx();
        self.sync_rpc_with_msg_id(
            dest.clone(),
            contents,
            Box::new(move |reply_id| {
                Box::new(async move {
                    tokio::time::sleep(Duration::from_millis(duration)).await;

                    let r = msg_tx.send(Message {
                        src: None,
                        dest: None,
                        body: MessageBody {
                            msg_id: None,
                            in_reply_to: None,
                            local_msg: Some(LocalMessage::Cancel(reply_id, dest)),
                            contents: Self::MessageType::default(),
                        },
                    });

                    if let Err(e) = r {
                        panic!("Message egress hung up: {}", e);
                    }
                })
            }),
            msg_id,
        )
    }

    fn get_msg_rx(&mut self) -> UnboundedReceiver<Message<Self::MessageType>>;

    fn get_msg_tx(&self) -> UnboundedSender<Message<Self::MessageType>>;

    fn get_self_tx(&self) -> UnboundedSender<Message<Self::MessageType>>;

    fn run(&mut self) -> impl Future<Output = Result<(), MaelstromError>> + Send
    where
        Self: std::marker::Send,
    {
        async {
            if let Err(e) = self.on_start().await {
                if let MaelstromError::ChannelError(_) = e {
                    return Err(e);
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

                if msg.body.local_msg.is_some() {
                    self.handle_local_message(msg.body.local_msg.unwrap())
                        .await?;
                } else if msg.body.in_reply_to.is_some() {
                    if let Err(e) = self.receive_reply(msg.clone()).await {
                        self.handle_err(msg, e)?;
                    }
                } else if let Err(e) = self.handle_message(msg.clone()).await {
                    self.handle_err(msg, e)?;
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
        let in_reply_to: Option<MessageId>;
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
            delegate_tx.clone(),
        );

        Self {
            delegate: Some(delegate),
            delegate_tx,
            node_id,
            node_ids,
            ingress_rx,
            egress_tx,
        }
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn node_ids(&self) -> &[String] {
        &self.node_ids
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

            if msg.dest.is_none() {
                info!("Ignoring message with no destination: {:?}", msg);
                continue;
            }

            let dest = msg.dest.as_ref().unwrap().as_str();
            if dest != self.node_id {
                // It's outgoing from the delegate, rewrite the source with our ID and send it on the wire.
                msg.src = Some(self.node_id.clone());
                info!("Sent: {:?}", msg);
                send!(self.egress_tx, msg, "Message egress hung up: {}");

                continue;
            }

            info!("Received {:?}", msg);
            if msg.body.contents.as_init_msg().is_some() {
                info!("Ignoring init message to initialized node");
                continue;
            }

            send!(self.delegate_tx, msg, "Node delegate hung up: {}");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    use serde::{Deserialize, Serialize};

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
            assert_eq!(m.body.msg_id, Some(1.into()));
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
                msg_id: Some(1.into()),
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
        assert_eq!(sent.body.in_reply_to, Some(1.into()));
        assert!(sent.body.local_msg.is_none());
    }

    #[test]
    fn test_delegate_formats_rpc_msg() {
        let (mut delegate, _, _) = get_delegate_and_channels();
        let dest = "n1".to_string();

        let contents = TestPayload::Empty;
        let sent = delegate.rpc(&dest, contents);

        assert!(sent.src.is_none());
        assert_eq!(sent.dest, Some(dest));
        assert_eq!(sent.body.msg_id, Some(1.into()));
        assert!(sent.body.in_reply_to.is_none());
        assert!(sent.body.local_msg.is_none());
    }

    #[tokio::test]
    async fn test_delegate_runs_sync_rpc() {
        let (mut delegate, ingress_tx, _egress_rx) = get_delegate_and_channels();

        // Swap out the delegate's ingress channel receiver so we can snoop on it.
        let (_null_tx, null_rx) = unbounded_channel::<TestMessage>();
        let mut ingress_rx = delegate.msg_rx.replace(null_rx).unwrap();
        let ingress_tx_clone = ingress_tx.clone();

        let node = "n3";
        if let Err(e) = delegate
            .sync_rpc(
                "n1",
                TestPayload::Empty,
                Box::new(move |_| {
                    Box::new(async move {
                        let message = Message {
                            src: Some(node.into()),
                            dest: None,
                            body: MessageBody {
                                msg_id: None,
                                in_reply_to: None,
                                local_msg: None,
                                contents: TestPayload::Empty,
                            },
                        };
                        if let Err(e) = ingress_tx_clone.send(message) {
                            panic!("Got error from ingress_tx: {}", e);
                        }
                    })
                }),
            )
            .await
        {
            panic!("Got error from sync_rpc: {}", e);
        }

        if let Some(m) = ingress_rx.recv().await {
            assert_eq!(m.src, Some(node.into()));
        } else {
            panic!("Never received sync_rpc message");
        }
    }

    #[tokio::test]
    async fn test_delegate_handles_local_msg() {
        let (mut delegate, _, _) = get_delegate_and_channels();
        delegate
            .get_outstanding_replies_mut()
            .insert((1.into(), "".into()));

        if let Err(e) = delegate
            .handle_local_message(LocalMessage::Cancel(1.into(), "".into()))
            .await
        {
            panic!("Got error: {}", e);
        }

        assert!(!delegate.is_expecting_reply(1.into(), ""));
    }

    type TestMessage = Message<TestPayload>;

    fn get_delegate_and_channels() -> (
        TestDelegate,
        UnboundedSender<TestMessage>,
        UnboundedReceiver<TestMessage>,
    ) {
        let (ingress_tx, ingress_rx) = unbounded_channel::<Message<TestPayload>>();
        let (egress_tx, egress_rx) = unbounded_channel::<Message<TestPayload>>();
        let delegate = TestDelegate::init("", vec![], egress_tx, ingress_rx, ingress_tx.clone());

        (delegate, ingress_tx, egress_rx)
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    #[serde(tag = "type")]
    enum TestPayload {
        #[serde(rename = "empty")]
        Empty,
    }

    impl Default for TestPayload {
        fn default() -> Self {
            Self::Empty
        }
    }

    impl MessagePayload for TestPayload {
        fn as_init_msg(&self) -> Option<InitMessagePayload> {
            None
        }

        fn to_init_ok_msg() -> Self {
            todo!()
        }

        fn to_err_msg(_: ErrorMessagePayload) -> Self {
            todo!()
        }
    }

    struct TestDelegate {
        msg_tx: UnboundedSender<Message<TestPayload>>,
        msg_rx: Option<UnboundedReceiver<Message<TestPayload>>>,

        msg_id: MessageId,
        outstanding_replies: HashSet<(MessageId, String)>,
    }

    impl NodeDelegate for TestDelegate {
        type MessageType = TestPayload;

        fn init(
            _: impl AsRef<str>,
            _: impl AsRef<Vec<String>>,
            msg_tx: UnboundedSender<Message<Self::MessageType>>,
            msg_rx: UnboundedReceiver<Message<Self::MessageType>>,
            _: UnboundedSender<Message<Self::MessageType>>,
        ) -> Self {
            Self {
                msg_tx,
                msg_rx: Some(msg_rx),
                msg_id: 0.into(),
                outstanding_replies: HashSet::new(),
            }
        }

        #[allow(clippy::manual_async_fn)]
        fn on_start(&mut self) -> impl Future<Output = Result<(), MaelstromError>> + Send {
            async move {
                let msg_tx = self.get_msg_tx();

                let msg = Message {
                    src: None,
                    dest: Some("n1".into()),
                    body: MessageBody {
                        msg_id: Some(1.into()),
                        in_reply_to: None,
                        local_msg: None,
                        contents: TestPayload::Empty,
                    },
                };
                send!(msg_tx, msg, "Delegate egress hung up: {}");

                Ok(())
            }
        }

        fn get_outstanding_replies(&self) -> &HashSet<(MessageId, String)> {
            &self.outstanding_replies
        }

        fn get_outstanding_replies_mut(&mut self) -> &mut HashSet<(MessageId, String)> {
            &mut self.outstanding_replies
        }

        #[allow(clippy::manual_async_fn)]
        fn handle_reply(
            &mut self,
            _: Message<Self::MessageType>,
        ) -> impl Future<Output = Result<(), MaelstromError>> + Send {
            async { panic!("Reply should have been ignored") }
        }

        #[allow(clippy::manual_async_fn)]
        fn handle_message(
            &mut self,
            _: Message<Self::MessageType>,
        ) -> impl Future<Output = Result<(), MaelstromError>> + Send {
            async { Ok(()) }
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
            todo!()
        }
    }
}

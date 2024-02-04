use std::{
    collections::HashMap,
    future::Future,
    marker::Send,
    time::Duration,
    {fmt::Display, marker::PhantomData},
};

use log::info;
use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    task::{JoinHandle, JoinSet},
};

use crate::{
    message::{
        ErrorMessagePayload, InitMessagePayload, Message, MessageBody, MessageId, MessagePayload,
        ParsedInput, ParsedMessage, ParsedReply, ParsedRpc,
    },
    rpc_error::{ErrorType, MaelstromError},
    send,
};

pub type ReplyRecord<M, C> = (UnboundedSender<NodeInput<M, C>>, Option<JoinHandle<()>>);

#[derive(Clone, Debug)]
pub enum Command<M: MessagePayload, C> {
    Timeout(MessageId, String),
    Cancel(MessageId),
    Send {
        dest: String,
        msg: M,
    },
    Reply {
        dest: String,
        msg_id: MessageId,
        msg: M,
    },
    Rpc {
        dest: String,
        msg: M,
        sink: UnboundedSender<NodeInput<M, C>>,
        timeout_ms: Option<u64>,
        msg_id: Option<MessageId>,
    },
    Custom(C),
}

#[derive(Clone, Debug)]
pub enum NodeInput<M: MessagePayload, C> {
    Message(ParsedInput<M>),
    Command(Command<M, C>),
}

impl<M: MessagePayload, C> From<ParsedInput<M>> for NodeInput<M, C> {
    fn from(value: ParsedInput<M>) -> Self {
        Self::Message(value)
    }
}

impl<M: MessagePayload, C> From<Command<M, C>> for NodeInput<M, C> {
    fn from(value: Command<M, C>) -> Self {
        Self::Command(value)
    }
}

pub trait NodeDelegate {
    type MessageType: MessagePayload + Clone + Send + 'static;
    type CommandType: std::fmt::Debug + Send + 'static;

    fn init(
        node_id: String,
        node_ids: impl IntoIterator<Item = String>,
        msg_tx: UnboundedSender<Message<Self::MessageType>>,
        msg_rx: UnboundedReceiver<ParsedInput<Self::MessageType>>,
    ) -> Self;

    fn on_start(&mut self) -> impl Future<Output = Result<(), MaelstromError>> + Send {
        async { Ok(()) }
    }

    fn get_reply_records_mut(
        &mut self,
    ) -> &mut HashMap<MessageId, ReplyRecord<Self::MessageType, Self::CommandType>>;

    fn handle_message(
        &mut self,
        message: ParsedMessage<Self::MessageType>,
    ) -> impl Future<Output = Result<(), MaelstromError>> + Send;

    fn handle_rpc(
        &mut self,
        message: ParsedRpc<Self::MessageType>,
    ) -> impl Future<Output = Result<(), MaelstromError>> + Send;

    fn handle_reply(
        &mut self,
        reply: ParsedReply<Self::MessageType>,
    ) -> impl Future<Output = Result<(), MaelstromError>> + Send
    where
        Self: Send,
    {
        async move {
            if let Some((msg_tx, handle)) = self.get_reply_records_mut().get_mut(&reply.in_reply_to)
            {
                // Cancel the timeout task if it exists, we got the reply in time.
                if let Some(t) = handle.as_ref() {
                    t.abort()
                }

                msg_tx
                    .send(ParsedInput::from(reply).into())
                    .expect("NodeDelegate hung up on itself");

                Ok(())
            } else {
                info!("Ignoring unexpected reply {:?}", reply);

                Ok(())
            }
        }
    }

    fn handle_command(
        &mut self,
        command: Command<Self::MessageType, Self::CommandType>,
    ) -> impl Future<Output = Result<(), MaelstromError>> + Send
    where
        Self: std::marker::Send,
    {
        async move {
            info!("Received command: {:?}", command);
            match command {
                Command::Cancel(msg_id) => {
                    self.get_reply_records_mut().remove(&msg_id);
                }
                Command::Send { dest, msg } => {
                    send!(
                        self.get_msg_tx(),
                        self.send(dest, msg),
                        "Egress hung up: {}"
                    );
                }
                Command::Reply { dest, msg_id, msg } => {
                    send!(
                        self.get_msg_tx(),
                        self.reply(dest, msg_id, msg),
                        "Egress hung up: {}"
                    );
                }
                Command::Rpc {
                    dest,
                    msg,
                    sink,
                    timeout_ms,
                    msg_id,
                } => {
                    let msg = if let Some(msg_id) = msg_id {
                        self.rpc_with_msg_id(dest, msg, sink, timeout_ms, msg_id)
                    } else {
                        self.rpc(dest, msg, sink, timeout_ms)
                    };

                    send!(self.get_msg_tx(), msg, "Egress hung up: {}");
                }
                Command::Custom(c) => {
                    self.handle_custom_command(c).await?;
                }
                Command::Timeout(_, _) => (),
            };

            Ok(())
        }
    }

    #[allow(unused_variables)]
    fn handle_custom_command(
        &mut self,
        command: Self::CommandType,
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
        dest: String,
        err: MaelstromError,
        msg_id: Option<MessageId>,
    ) -> Result<(), MaelstromError> {
        if let MaelstromError::ChannelError(_) = err {
            return Err(err);
        }

        let err_payload = Self::MessageType::to_err_msg(Self::error_body(err));
        let reply = if let Some(msg_id) = msg_id {
            self.reply(dest, msg_id, err_payload)
        } else {
            self.send(dest, err_payload)
        };
        send!(self.get_msg_tx(), reply, "Egress hung up: {}");

        Ok(())
    }

    fn format_outgoing(
        &self,
        dest: String,
        body: MessageBody<Self::MessageType>,
    ) -> Message<Self::MessageType> {
        Message {
            src: Some(self.get_node_id().into()),
            dest: Some(dest),
            body,
        }
    }

    fn send(&self, dest: String, contents: Self::MessageType) -> Message<Self::MessageType> {
        let body = MessageBody {
            msg_id: None,
            in_reply_to: None,
            contents,
        };

        self.format_outgoing(dest, body)
    }

    fn reply(
        &self,
        dest: String,
        msg_id: MessageId,
        contents: Self::MessageType,
    ) -> Message<Self::MessageType> {
        let in_reply_to = Some(msg_id);
        let body = MessageBody {
            msg_id: None,
            in_reply_to,
            contents,
        };

        self.format_outgoing(dest, body)
    }

    fn rpc(
        &mut self,
        dest: String,
        contents: Self::MessageType,
        reply_sink: UnboundedSender<NodeInput<Self::MessageType, Self::CommandType>>,
        timeout_ms: Option<u64>,
    ) -> Message<Self::MessageType> {
        let msg_id = self.next_msg_id();

        self.rpc_with_msg_id(dest, contents, reply_sink, timeout_ms, msg_id)
    }

    fn rpc_with_msg_id(
        &mut self,
        dest: String,
        contents: Self::MessageType,
        reply_sink: UnboundedSender<NodeInput<Self::MessageType, Self::CommandType>>,
        timeout_ms: Option<u64>,
        msg_id: MessageId,
    ) -> Message<Self::MessageType> {
        let body = MessageBody {
            msg_id: Some(msg_id),
            in_reply_to: None,
            contents,
        };
        let dest_clone = dest.clone();
        let reply_record = (
            reply_sink.clone(),
            timeout_ms.map(move |t| {
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(t)).await;
                    info!("Request with msg_id {} timed out", msg_id);

                    // If this failed, it's because this task didn't get aborted in time but the reply to this msg_id
                    // was successfully handled. Ignore the Err.
                    let _ =
                        reply_sink.send(NodeInput::Command(Command::Timeout(msg_id, dest_clone)));
                })
            }),
        );
        self.get_reply_records_mut().insert(msg_id, reply_record);

        self.format_outgoing(dest, body)
    }

    fn get_node_id(&self) -> &str;

    fn is_cluster_member(&self, node_id: &str) -> bool {
        node_id.chars().next().is_some_and(|c| c == 'n')
    }

    fn get_msg_rx(&mut self) -> UnboundedReceiver<ParsedInput<Self::MessageType>>;

    fn get_msg_tx(&self) -> UnboundedSender<Message<Self::MessageType>>;

    fn get_command_rx(
        &mut self,
    ) -> UnboundedReceiver<Command<Self::MessageType, Self::CommandType>>;

    fn get_command_tx(&self) -> UnboundedSender<Command<Self::MessageType, Self::CommandType>>;

    fn run(&mut self) -> impl Future<Output = Result<(), MaelstromError>> + Send
    where
        Self: Send,
    {
        async {
            if let Err(e) = self.on_start().await {
                if let MaelstromError::ChannelError(_) = e {
                    return Err(e);
                }
            }

            let mut join_set = JoinSet::new();

            // I'd have much rather preferred to use select_all over these as streams, using StreamExt::map to wrap
            // them in the NodeInput enum, but that results in heterogeneous concrete types that can't be unified.
            let mut msg_rx = self.get_msg_rx();
            let mut command_rx = self.get_command_rx();
            let (input_tx, mut input_rx) =
                unbounded_channel::<NodeInput<Self::MessageType, Self::CommandType>>();
            let input_tx_clone = input_tx.clone();
            join_set.spawn(async move {
                while let Some(command) = command_rx.recv().await {
                    let _ = input_tx_clone.send(NodeInput::Command(command));
                }
            });
            join_set.spawn(async move {
                while let Some(msg) = msg_rx.recv().await {
                    let _ = input_tx.send(NodeInput::Message(msg));
                }
            });

            loop {
                let input = {
                    let input = input_rx.recv().await;
                    if let Some(msg) = input {
                        msg
                    } else {
                        return Err(MaelstromError::ChannelError("node hung up".into()));
                    }
                };

                match input {
                    NodeInput::Message(msg) => {
                        let (src, msg_id, result) = match msg {
                            ParsedInput::Message(m) => {
                                (m.src.clone(), None, self.handle_message(m).await)
                            }
                            ParsedInput::Reply(r) => {
                                (r.src.clone(), None, self.handle_reply(r).await)
                            }
                            ParsedInput::Rpc(r) => {
                                (r.src.clone(), Some(r.msg_id), self.handle_rpc(r).await)
                            }
                        };

                        if let Err(e) = result {
                            self.handle_err(src, e, msg_id)?;
                        }
                    }
                    NodeInput::Command(c) => {
                        self.handle_command(c).await?;
                    }
                };
            }
        }
    }
}

#[derive(Debug)]
pub struct UninitializedNode<M: MessagePayload + Send, D: NodeDelegate<MessageType = M> + 'static> {
    ingress_rx: UnboundedReceiver<Message<M>>,
    egress_tx: UnboundedSender<Message<M>>,

    _msg_marker: PhantomData<M>,
    _delegate_marker: PhantomData<D>,
}

impl<M: MessagePayload + Send, D: NodeDelegate<MessageType = M> + Send> UninitializedNode<M, D> {
    pub fn new(
        ingress_rx: UnboundedReceiver<Message<M>>,
        egress_tx: UnboundedSender<Message<M>>,
    ) -> Self {
        Self {
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
                contents: M::to_init_ok_msg(),
            },
        };
        send!(self.egress_tx, response, "Egress hung up: {}");

        Ok(Node::new(
            init_msg.node_id,
            init_msg.node_ids,
            self.ingress_rx,
            self.egress_tx,
        ))
    }
}

#[derive(Debug)]
pub struct Node<M: MessagePayload + Send, D: NodeDelegate<MessageType = M> + 'static> {
    pub delegate: Option<D>,
    delegate_tx: UnboundedSender<ParsedInput<M>>,

    ingress_rx: UnboundedReceiver<Message<M>>,
}

impl<M: MessagePayload + Send, D: NodeDelegate<MessageType = M> + Send> Node<M, D> {
    pub fn new(
        node_id: String,
        node_ids: Vec<String>,
        ingress_rx: UnboundedReceiver<Message<M>>,
        egress_tx: UnboundedSender<Message<M>>,
    ) -> Self {
        let (delegate_tx, delegate_rx) = unbounded_channel::<ParsedInput<M>>();
        let delegate = D::init(node_id.clone(), node_ids.clone(), egress_tx, delegate_rx);

        Self {
            delegate: Some(delegate),
            delegate_tx,
            ingress_rx,
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
            let msg = if let Some(msg) = self.ingress_rx.recv().await {
                msg
            } else {
                return Err(MaelstromError::ChannelError(
                    "message ingress hung up".into(),
                ));
            };

            if msg.src.is_none() {
                info!("Ignoring message with no destination: {:?}", msg);
                continue;
            }

            info!("Received {:?}", msg);
            if msg.body.contents.as_init_msg().is_some() {
                info!("Ignoring init message to initialized node");
                continue;
            }

            let parsed = if let Some(msg_id) = msg.body.msg_id {
                ParsedInput::Rpc(ParsedRpc {
                    src: msg.src.unwrap(),
                    msg_id,
                    body: msg.body.contents,
                })
            } else if let Some(in_reply_to) = msg.body.in_reply_to {
                ParsedInput::Reply(ParsedReply {
                    src: msg.src.unwrap(),
                    in_reply_to,
                    body: msg.body.contents,
                })
            } else {
                ParsedInput::Message(ParsedMessage {
                    src: msg.src.unwrap(),
                    body: msg.body.contents,
                })
            };

            send!(self.delegate_tx, parsed, "Node delegate hung up: {}");
        }
    }
}

#[cfg(test)]
mod tests {
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
        let sent = delegate.send(dest.clone(), contents);

        assert_eq!(sent.src, Some("".into()));
        assert_eq!(sent.dest, Some(dest));
        assert!(sent.body.msg_id.is_none());
        assert!(sent.body.in_reply_to.is_none());
    }

    #[test]
    fn test_delegate_formats_reply_msg() {
        let (delegate, _, _) = get_delegate_and_channels();

        let dest = "n1".to_string();
        let msg_id: MessageId = 1.into();
        let contents = TestPayload::Empty;
        let sent = delegate.reply(dest.clone(), msg_id, contents);

        assert_eq!(sent.src, Some("".into()));
        assert_eq!(sent.dest, Some(dest));
        assert!(sent.body.msg_id.is_none());
        assert_eq!(sent.body.in_reply_to, Some(msg_id));
    }

    #[test]
    fn test_delegate_formats_rpc_msg() {
        let (mut delegate, _, _) = get_delegate_and_channels();
        let dest = "n1".to_string();

        let contents = TestPayload::Empty;
        let (msg_tx, _) = unbounded_channel::<NodeInput<TestPayload, TestCommand>>();
        let sent = delegate.rpc(dest.clone(), contents, msg_tx, None);

        assert!(sent.src == Some("".into()));
        assert_eq!(sent.dest, Some(dest));
        assert_eq!(sent.body.msg_id, Some(1.into()));
        assert!(sent.body.in_reply_to.is_none());
    }

    type TestInput = ParsedInput<TestPayload>;
    type TestMessage = Message<TestPayload>;

    fn get_delegate_and_channels() -> (
        TestDelegate,
        UnboundedSender<TestInput>,
        UnboundedReceiver<TestMessage>,
    ) {
        let (ingress_tx, ingress_rx) = unbounded_channel::<ParsedInput<TestPayload>>();
        let (egress_tx, egress_rx) = unbounded_channel::<Message<TestPayload>>();
        let delegate = TestDelegate::init("".into(), vec![], egress_tx, ingress_rx);

        (delegate, ingress_tx, egress_rx)
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    #[serde(tag = "type")]
    enum TestPayload {
        #[serde(rename = "empty")]
        Empty,
    }

    #[derive(Clone, Debug)]
    struct TestCommand;

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
        msg_rx: Option<UnboundedReceiver<ParsedInput<TestPayload>>>,

        cmd_tx: UnboundedSender<Command<TestPayload, TestCommand>>,
        cmd_rx: Option<UnboundedReceiver<Command<TestPayload, TestCommand>>>,

        msg_id: MessageId,
        reply_records: HashMap<MessageId, ReplyRecord<TestPayload, TestCommand>>,
    }

    impl NodeDelegate for TestDelegate {
        type MessageType = TestPayload;
        type CommandType = TestCommand;

        fn init(
            _: String,
            _: impl IntoIterator<Item = String>,
            msg_tx: UnboundedSender<Message<Self::MessageType>>,
            msg_rx: UnboundedReceiver<ParsedInput<Self::MessageType>>,
        ) -> Self {
            let (cmd_tx, cmd_rx) =
                unbounded_channel::<Command<Self::MessageType, Self::CommandType>>();
            Self {
                msg_tx,
                msg_rx: Some(msg_rx),
                cmd_tx,
                cmd_rx: Some(cmd_rx),
                msg_id: 0.into(),
                reply_records: HashMap::new(),
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
                        contents: TestPayload::Empty,
                    },
                };
                send!(msg_tx, msg, "Delegate egress hung up: {}");

                Ok(())
            }
        }

        fn get_reply_records_mut(
            &mut self,
        ) -> &mut std::collections::HashMap<
            MessageId,
            ReplyRecord<Self::MessageType, Self::CommandType>,
        > {
            &mut self.reply_records
        }

        #[allow(clippy::manual_async_fn)]
        fn handle_message(
            &mut self,
            _: ParsedMessage<Self::MessageType>,
        ) -> impl Future<Output = Result<(), MaelstromError>> + Send {
            async { Ok(()) }
        }

        #[allow(clippy::manual_async_fn)]
        fn handle_rpc(
            &mut self,
            _: ParsedRpc<Self::MessageType>,
        ) -> impl Future<Output = Result<(), MaelstromError>> + Send {
            async { todo!() }
        }

        #[allow(clippy::manual_async_fn)]
        fn handle_custom_command(
            &mut self,
            _: Self::CommandType,
        ) -> impl Future<Output = Result<(), MaelstromError>> + Send {
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
            ""
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
}

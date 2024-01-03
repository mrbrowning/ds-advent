use std::{collections::HashMap, sync::{Mutex, Arc}, ops::Deref};

use log::{error, info};
use serde::{Serialize, Deserialize, de};
use tokio::{io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader, Stdin, self}, sync::mpsc::channel, task::JoinSet};

use crate::rpc_error::{MaelstromError, RPCError, ErrorType};

type JSONMap = serde_json::Map<String, serde_json::Value>;

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Message {
    #[serde(skip_serializing_if = "Option::is_none")]
    src: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dest: Option<String>,
    #[serde(skip_serializing_if = "JSONMap::is_empty")]
    body: JSONMap,
}

impl Message {
    fn msg_type(&self) -> String {
        let body: Result<MessageBody, _> = MessageBody::try_from(self.body.clone());
        match body {
            Ok(b) => b.msg_type,
            Err(_) => "".to_string(),
        }
    }
    
    fn rpc_error(&self) -> Option<RPCError> {
        let body_res: Result<MessageBody, _> = MessageBody::try_from(self.body.clone());
        if let Err(e) = body_res {
            return Some(RPCError::new(ErrorType::Crash as i64, e.to_string()));
        } 
        let body = body_res.unwrap();
        if body.error.is_none() {
            return None;
        }
        let (code, text) = body.error.unwrap();

        Some(RPCError::new(code, text))
    }
}

#[derive(Debug)]
struct MessageBody {
    msg_type: String,
    msg_id: Option<i64>,
    in_reply_to: Option<i64>,
    error: Option<(i64, String)>,
}

impl TryFrom<JSONMap> for MessageBody {
    type Error = MaelstromError;

    fn try_from(value: JSONMap) -> Result<Self, Self::Error> {
        // This is a little silly, but I don't feel like doing this manually right now.
        let serialized = serde_json::to_string(&value)?;

        Ok(serde_json::from_str(&serialized)?)
    }
}

impl From<MessageBody> for JSONMap {
    fn from(value: MessageBody) -> Self {
        let mut map = JSONMap::new();

        map.insert("msg_type".to_string(), serde_json::Value::from(value.msg_type));
        if let Some(msg_id) = value.msg_id {
            map.insert("msg_id".to_string(), serde_json::Value::from(msg_id));
        }
        if let Some(in_reply_to) = value.in_reply_to {
            map.insert("in_reply_to".to_string(), serde_json::Value::from(in_reply_to));
        }
        
        if let Some((code, text)) = value.error {
            map.insert("code".to_string(), serde_json::Value::from(code));
            map.insert("text".to_string(), serde_json::Value::from(text));
        }
        
        map
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct MessageBodyJSON {
    #[serde(rename = "type")]
    msg_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    msg_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
}

impl Serialize for MessageBody {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer {
        let mut code: Option<i64> = None;
        let mut text: Option<String> = None;
        if let Some((code_orig, text_orig)) = &self.error {
            code = Some(*code_orig);
            text = Some(text_orig.to_string());
        }
        
        let msg_body_json = MessageBodyJSON {
            msg_type: self.msg_type.to_string(),
            msg_id: self.msg_id,
            in_reply_to: self.in_reply_to,
            code,
            text,
        };
        
        msg_body_json.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for MessageBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de> {
        let msg_body_json = MessageBodyJSON::deserialize(deserializer)?;
        let mut error: Option<(i64, String)> = None;

        if msg_body_json.code.is_some() && msg_body_json.text.is_some() {
            error = Some((msg_body_json.code.unwrap(), msg_body_json.text.unwrap()));
        } else if msg_body_json.code.is_some() || msg_body_json.text.is_some() {
            return Err("Both error code and text must be defined or none").map_err(de::Error::custom);
        }
        
        Ok(MessageBody {
            msg_type: msg_body_json.msg_type,
            msg_id: msg_body_json.msg_id,
            in_reply_to: msg_body_json.in_reply_to,
            error,
        })
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct InitMessageBody {
    #[serde(flatten)]
    message: MessageBody,
    node_id: String,
    node_ids: Vec<String>,
}

impl TryFrom<JSONMap> for InitMessageBody {
    type Error = MaelstromError;

    fn try_from(value: JSONMap) -> Result<Self, Self::Error> {
        // This is a little silly, but I don't feel like doing this manually right now.
        let serialized = serde_json::to_string(&value)?;

        Ok(serde_json::from_str(&serialized)?)
    }
}

impl From<InitMessageBody> for JSONMap {
    fn from(value: InitMessageBody) -> Self {
        let mut map = JSONMap::from(value.message);

        map.insert("node_id".to_string(), serde_json::Value::from(value.node_id));
        map.insert("node_ids".to_string(), serde_json::Value::from(value.node_ids));
        
        map
    }
}
enum HandlerCommand {
    Nop,
    Reply(Message, Vec<u8>),
}

impl From<()> for HandlerCommand {
    fn from(value: ()) -> Self {
        Self::Nop
    }
}

type Handler = dyn Fn(Message) -> Result<HandlerCommand, MaelstromError> + Send + Sync;
type NodeHandler<R> = dyn Fn(Node<Initialized, R>, Message) -> Result<HandlerCommand, MaelstromError> + Send + Sync;

#[derive(Debug)]
struct Uninitialized {}

#[derive(Debug)]
struct Initialized {
    id: String,
    node_ids: Vec<String>,
}

trait NodeState {}
impl NodeState for Uninitialized {}
impl NodeState for Initialized {}

struct Node<S: NodeState, R: AsyncBufRead + Send + Unpin + 'static> {
    state: S,
    next_msg_id: Arc<Mutex<i64>>,
    
    handlers: Arc<Mutex<HashMap<String, Arc<Handler>>>>,
    callbacks: Arc<Mutex<HashMap<i64, Box<Handler>>>>,
    
    // These are the only cases where we need to hold a mutex across an await, because i/o. It'd be
    // better to just have a dedicated task holding a channel for this, but the goal here was to
    // mimic the Go version's semantics in the small to the greatest extent possible.
    stdin: Arc<tokio::sync::Mutex<R>>,
    stdout: Arc<tokio::sync::Mutex<dyn AsyncWrite + Send + Unpin>>,
}

impl<S: NodeState + std::fmt::Debug, R: AsyncBufRead + Send + Unpin> std::fmt::Debug for Node<S, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node").field("state", &self.state).field("next_msg_id", &self.next_msg_id).finish()
    }
}

impl<S: NodeState, R: AsyncBufRead + Send + Unpin> Node<S, R> {
    fn handle(&self, handler_type: impl Into<String>, handler_fn: Arc<Handler>) {
        let mut handlers = self.handlers.lock().unwrap();
        let handler_type_str = handler_type.into();
        if handlers.get(&handler_type_str).is_some() {
            panic!("duplicate message handler for {} message type", handler_type_str);
        }

        handlers.insert(handler_type_str, handler_fn);
    }
    
    fn handle_callback(&self, handler: impl Deref<Target = Handler>, msg: Message) {
        if let Err(e) = handler(msg) {
            error!("callback error: {}", e);
        }
    }
}

impl<R: AsyncBufRead + Send + Unpin> Node<Uninitialized, R> {
    pub fn new(stdin: R, stdout: impl AsyncWrite + Send + Unpin + 'static) -> Self {
        Node {
            state: Uninitialized {},
            next_msg_id: Arc::new(Mutex::new(0)),
            
            handlers: Arc::new(Mutex::new(HashMap::new())),
            callbacks: Arc::new(Mutex::new(HashMap::new())),
            
            stdin: Arc::new(tokio::sync::Mutex::new(stdin)),
            stdout: Arc::new(tokio::sync::Mutex::new(stdout)),
        } 
    }

    fn init(self, id: String, node_ids: Vec<String>) -> Node<Initialized, R> {
        Node {
            state: Initialized {
                id,
                node_ids,
            },
            next_msg_id: self.next_msg_id,

            handlers: self.handlers,
            callbacks: self.callbacks,
            
            stdin: self.stdin,
            stdout: self.stdout,
        }
    }
    
    async fn handle_init_message(self, msg: Message) -> Result<Node<Initialized, R>, MaelstromError> {
        let body: InitMessageBody = InitMessageBody::try_from(msg.body.clone())?;
        let node = self.init(body.node_id, body.node_ids);

        {
            let handlers = node.handlers.lock().unwrap();
            if let Some(handler) = handlers.get("init") {
                handler(msg.clone())?;
            }
        }
        info!("Node {} initialized", node.state.id);

        let msg_body = MessageBody {
            msg_type: "init_ok".to_string(),
            msg_id: None,
            in_reply_to: None,
            error: None,
        };
        let body_bytes = serde_json::to_vec(&msg_body)?;
        node.reply(msg, body_bytes).await?;
        
        Ok(node)
    }

    pub async fn run(self) -> Result<Node<Initialized, R>, MaelstromError> {
        let this = Arc::new(self);
        let mut stdin = this.stdin.lock().await;

        let mut join_set: JoinSet<()> = JoinSet::new();
        let mut line = String::new();
        loop {
            let read_result = stdin.read_line(&mut line).await;
            if let Err(e) = read_result {
                return Err(e.into());
            } else if read_result.unwrap() == 0 {
                break;
            }

            let msg: Message = serde_json::from_str(&line)?;
            let body: MessageBody = MessageBody::try_from(msg.body.clone())?;
            
            info!("Received {:?}", msg);
            
            if let Some(in_reply_to) = body.in_reply_to {
                let mut callbacks = this.callbacks.lock().unwrap();
                let handler = callbacks.remove(&in_reply_to);
                drop(callbacks);

                if handler.is_none() {
                    info!("Ignoring reply to {} with no callback", in_reply_to);
                    continue;
                }
                
                let this_clone = this.clone();
                let msg_clone = msg.clone();
                join_set.spawn(async move {
                    this_clone.handle_callback(handler.unwrap(), msg_clone);
                });
            }
            
            if body.msg_type == "init" {
                drop(stdin);
                while let Some(_) = join_set.join_next().await { }

                // We've awaited everyone who has a reference to this, we can move out of it.
                let uninitialized_node = Arc::into_inner(this).unwrap();

                return Ok(uninitialized_node.handle_init_message(msg).await?);
            }
        }
        
        Err(MaelstromError::Other("Unexpected end of event loop".to_string()))
    }
}

impl Node<Uninitialized, BufReader<Stdin>> {
    fn default() -> Self {
        Self::new(BufReader::new(io::stdin()), io::stdout())
    }
}

impl<R: AsyncBufRead + Send + Unpin> Node<Initialized, R> {
    pub fn id(&self) -> &str {
        &self.state.id
    }
    
    pub fn node_ids(&self) -> &[String] {
        &self.state.node_ids
    }
    
    async fn log_err_and_reply(&self, msg: Message, err: impl Serialize) {
        let err_bytes = serde_json::to_vec(&err);
        if let Err(e) = err_bytes {
            error!("serialization error: {}", e);
            return;
        }

        if let Err(e) = self.reply(msg, err_bytes.unwrap()).await {
            error!("reply error: {}", e);
        }
    }
    
    async fn wrap_err_log_and_reply(&self, msg: Message, err: impl std::fmt::Display) {
        error!("Exception handling {:?}:\n{}", msg, err);
        let wrapped = RPCError::new(ErrorType::Crash as i64, format!("{}", err));
        self.log_err_and_reply(msg, wrapped).await;
    }
    
    async fn handle_message(&self, handler: impl Deref<Target = Handler>, msg: Message) {
        let result = handler(msg.clone());
        if let Err(e) = result {
            match e {
                MaelstromError::RPCError(rpc_error) => {
                    self.log_err_and_reply(msg, rpc_error).await;
                },
                MaelstromError::SerializationError(e) => {
                    self.wrap_err_log_and_reply(msg, e).await;
                },
                MaelstromError::IOError(e) => {
                    self.wrap_err_log_and_reply(msg, e).await;
                },
                MaelstromError::Other(e) => {
                    self.wrap_err_log_and_reply(msg, e).await;
                },
            }
            
            return;
        }

        match result.unwrap() {
            HandlerCommand::Nop => (),
            HandlerCommand::Reply(msg, body) => {
                if let Err(e) = self.reply(msg, body).await {
                    error!("Reply error: {}", e);
                }
            },
        }
    }
    
    async fn send(&self, dest: Option<impl AsRef<str>>, body: impl Serialize) -> Result<(), MaelstromError> {
        let body_json = serde_json::to_vec(&body)?;
        let msg = Message {
            src: Some(self.state.id.to_string()),
            dest: dest.map(|d| d.as_ref().to_string()),
            body: serde_json::from_slice(&body_json)?,
        };
        let buf = serde_json::to_vec(&msg)?;

        info!("Sent {}", "buf");

        let mut writer = self.stdout.lock().await;
        writer.write_all(&buf).await?;
        writer.write_all("\n".as_bytes()).await?;

        Ok(())
    }
    
    async fn reply(&self, request: Message, body: Vec<u8>) -> Result<(), MaelstromError> {
        let mut body_map: JSONMap = serde_json::from_slice(&body)?;
        let msg_id = request.body.get("msg_id").ok_or(MaelstromError::RPCError(RPCError::new(ErrorType::MalformedRequest as i64, "Message body missing msg_id".to_string())))?;
        body_map.insert("in_reply_to".to_string(), msg_id.clone());

        self.send(request.src, body_map).await
    }
    
    async fn rpc(&self, dest: impl AsRef<str>, body: impl Serialize, handler: Box<Handler>) -> Result<(), MaelstromError> {
        let mut next_msg_id = self.next_msg_id.lock().unwrap();

        *next_msg_id += 1;
        let msg_id = *next_msg_id;
        drop(next_msg_id);

        let mut callbacks = self.callbacks.lock().unwrap();
        callbacks.insert(msg_id, handler);
        drop(callbacks);
        
        let buf = serde_json::to_vec(&body)?;
        let mut body_map: JSONMap = serde_json::from_slice(&buf)?;
        body_map.insert("msg_id".to_string(), serde_json::Value::from(msg_id));

        self.send(Some(dest), body_map).await
    }
    
    async fn sync_rpc(&self, dest: impl AsRef<str>, body: impl Serialize) -> Result<Message, MaelstromError> {
        let (tx, mut rx) = channel::<Message>(1);
        self.rpc(dest, body, Box::new(move |msg| {
            let _ = tx.send(msg);
            Ok(().into())
        })).await?;
        
        let msg_result = rx.recv().await;
        if msg_result.is_none() {
            return Err(MaelstromError::Other("Sender dropped".to_string()));
        }
        let msg = msg_result.unwrap();
        let err = msg.rpc_error();
        if let Some(e) = err {
            return Err(e.into());
        }

        Ok(msg)
    }
    
    pub async fn run(self) -> Result<(), MaelstromError> {
        let this = Arc::new(self);
        let mut stdin = this.stdin.lock().await;

        let mut line = String::new();
        let mut join_set: JoinSet<()> = JoinSet::new();
        loop {
            let read_result = stdin.read_line(&mut line).await;
            if let Err(e) = read_result {
                return Err(e.into());
            } else if read_result.unwrap() == 0 {
                break;
            }

            let msg: Message = serde_json::from_str(&line)?;
            let body: MessageBody = MessageBody::try_from(msg.body.clone())?;
            
            info!("Received {:?}", msg);
            
            if let Some(in_reply_to) = body.in_reply_to {
                let mut callbacks = this.callbacks.lock().unwrap();
                let handler = callbacks.remove(&in_reply_to);
                drop(callbacks);

                if handler.is_none() {
                    info!("Ignoring reply to {} with no callback", in_reply_to);
                    continue;
                }
                
                let this_clone = this.clone();
                let msg_clone = msg.clone();
                join_set.spawn(async move {
                    this_clone.handle_callback(handler.unwrap(), msg_clone);
                });
            }
            
            if body.msg_type == "init" {
                info!("Ignoring init message to initialized node");
                continue;
            }
            
            let handlers = this.handlers.lock().unwrap();
            if let Some(handler) = handlers.get(&body.msg_type) {
                let this_clone = this.clone();
                let msg_clone = msg.clone();
                let handler_clone = handler.clone();
                drop(handlers);

                join_set.spawn(async move {
                    this_clone.handle_message(handler_clone, msg_clone).await;
                });
            } else {
                return Err(MaelstromError::Other(format!("No handler for {}", line.trim())));
            }
        }
        while let Some(_) = join_set.join_next().await { }
        drop(stdin);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    use futures::{FutureExt, pin_mut};
    use tokio::io::AsyncRead;
    use std::future::Future;
    use std::task::Poll;
    use tokio::sync::mpsc as tokio_mpsc;
    use std::io::{Error, ErrorKind};
    
    struct AsyncChannelReader {
        receiver: tokio_mpsc::Receiver<Vec<u8>>,
    }
    
    impl AsyncRead for AsyncChannelReader {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let fut = self.get_mut().receiver.poll_recv(cx);
            match fut {
                Poll::Ready(None) => {
                    // The channel is closed, so let's behave the way we would if we got EOF.
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Some(msg)) => {
                    buf.put_slice(&msg);
                    Poll::Ready(Ok(()))
                }
                Poll::Pending => {
                    Poll::Pending
                }
            }
        }
    }
    
    struct AsyncChannelWriter {
        sender: tokio_mpsc::Sender<Vec<u8>>,
    }
    
    impl AsyncWrite for AsyncChannelWriter {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, std::io::Error>> {
            let buf_len = buf.len();
            let fut = self.sender.send(buf.to_vec()).map(|x| x.map(|_| buf_len).map_err(|e| Error::new(ErrorKind::Other, e)));
            pin_mut!(fut);
            
            fut.poll(cx)
        }

        fn poll_flush(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> Poll<Result<(), std::io::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> Poll<Result<(), std::io::Error>> {
            Poll::Ready(Ok(()))
        }
    }
    
    fn new_node() -> (Node<Uninitialized, BufReader<AsyncChannelReader>>, tokio_mpsc::Sender<Vec<u8>>, tokio_mpsc::Receiver<Vec<u8>>) {
        let (input_tx, input_rx) = tokio_mpsc::channel::<Vec<u8>>(10);
        let (output_tx, output_rx) = tokio_mpsc::channel::<Vec<u8>>(10);
        let reader = BufReader::new(AsyncChannelReader { receiver: input_rx });
        let writer = AsyncChannelWriter { sender: output_tx };
        
        (Node::new(reader, writer), input_tx, output_rx)
    }

    #[tokio::test]
    async fn test_node_rejects_malformed_json() {
        let input = "\n".as_bytes();
        let output: Vec<u8> = vec![];
        let node: Node<Uninitialized, _> = Node::new(input, output);
        
        let result = node.run().await;
        if let Err(e) = result {
            match e {
                MaelstromError::SerializationError(_) => (),
                _ => {
                    assert!(false, "Wanted serialization error, got {}", e);
                }
            }
        } else {
            assert!(false, "JSON deserialization didn't fail");
        }
    }
    
    #[tokio::test]
    async fn test_node_fails_on_missing_handler() {
        let msg = r#"{"dest":"n1", "body":{"type":"echo", "msg_id":1}}"#;
        let input = format!("{}\n", msg).leak();
        let output: Vec<u8> = vec![];
        let node = Node::new(input.as_bytes(), output).init("0".to_string(), vec![]);

        let result = node.run().await;
        if let Err(e) = result {
            match e {
                MaelstromError::Other(s) => {
                    assert_eq!(s, format!("No handler for {}", msg));                        
                }
                _ => {
                    assert!(false, "Wanted missing handler error, got {}", e);
                }
            }
        } else {
            assert!(false, "JSON deserialization didn't fail");
        }
    }
    
    #[tokio::test]
    async fn test_node_returns_rpc_error() {
        let msg = r#"{"dest":"n1", "body":{"type":"foo", "msg_id":1000}}"#;
        let input = format!("{}\n", msg).leak();
        let (sender, mut receiver) = tokio_mpsc::channel::<Vec<u8>>(2);
        let output = AsyncChannelWriter { sender };
        let mut node = Node::new(input.as_bytes(), output).init("0".to_string(), vec![]);
        
        node.handle("foo", Arc::new(|_| Err(MaelstromError::RPCError(RPCError::new(ErrorType::NotSupported as i64, "bad call".to_string())))));
        
        let result = node.run().await;
        if let Err(e) = result {
            assert!(false, "Got error: {}", e);
            return;
        }
        
        let result = receiver.recv().await;
        match result {
            Some(out) => {
                let line = String::from_utf8(out).unwrap();
                assert_eq!(line, r#"{"src":"0","body":{"code":10,"in_reply_to":1000,"text":"bad call","type":"error"}}"#);
            },
            None => {
                assert!(false, "Got nothing from recv");
            }
        }
    }
    
    #[tokio::test]
    async fn test_node_returns_non_rpc_error() {
        let msg = r#"{"dest":"n1", "body":{"type":"foo", "msg_id":1000}}"#;
        let input = format!("{}\n", msg).leak();
        let (sender, mut receiver) = tokio_mpsc::channel::<Vec<u8>>(2);
        let output = AsyncChannelWriter { sender };
        let mut node = Node::new(input.as_bytes(), output).init("0".to_string(), vec![]);
        
        node.handle("foo", Arc::new(|_| Err(MaelstromError::Other("bad call".to_string()))));
        
        let result = node.run().await;
        if let Err(e) = result {
            assert!(false, "Got error: {}", e);
            return;
        }
        
        let result = receiver.recv().await;
        match result {
            Some(out) => {
                let line = String::from_utf8(out).unwrap();
                assert_eq!(line, r#"{"src":"0","body":{"code":13,"in_reply_to":1000,"text":"bad call","type":"error"}}"#);
            },
            None => {
                assert!(false, "Got nothing from recv");
            }
        }
    }
    
    #[tokio::test]
    async fn test_node_runs_init() {
        let (node, node_sender, mut node_receiver) = new_node();
        let msg = r#"{"body":{"type":"init", "msg_id":1, "node_id":"n3", "node_ids":["n1", "n2", "n3"]}}"#;
        let input = format!("{}\n", msg);
        
        let (node_id_sender, mut node_id_receiver) = tokio_mpsc::channel::<(String, Vec<String>)>(1);
        tokio::task::spawn(async move {
            let n = node.run().await.unwrap();
            let _ = node_id_sender.send((n.id().to_string(), n.node_ids().to_owned())).await;
        });
        
        if let Err(e) = node_sender.send(input.into()).await {
            assert!(false, "Got error: {}", e);
        }
        
        if let Some(bytes) = node_receiver.recv().await {
            assert_eq!(bytes, r#"{"src":"n3","body":{"in_reply_to":1,"type":"init_ok"}}"#.as_bytes());
        } else {
            assert!(false, "Node failed to print info on stdout");
        }
        
        if let Some((node_id, node_ids)) = node_id_receiver.recv().await {
            assert_eq!(node_id, "n3");
            assert_eq!(node_ids, vec!["n1", "n2", "n3"]);
        } else {
            assert!(false, "Couldn't recv node id info");
        }
    }
    
    #[tokio::test]
    async fn test_node_runs_echo() {
        let (uninitialized_node, node_sender, mut node_receiver) = new_node();
        let node = uninitialized_node.init("n1".to_string(), vec!["n1".to_string()]);

        node.handle("echo", Arc::new(move |msg| {
            let mut body = MessageBody::try_from(msg.body.clone())?;
            body.msg_type = "echo_ok".to_string();
            let body_bytes = serde_json::to_vec(&body)?;

            Ok(HandlerCommand::Reply(msg, body_bytes))
        }));
        
        tokio::task::spawn(async move {
            node.run().await;
        });
        
        let msg = r#"{"dest":"n1", "body":{"type":"echo", "msg_id":2}}"#;
        let input = format!("{}\n", msg);
        if let Err(e) = node_sender.send(input.into()).await {
            assert!(false, "Got error: {}", e);
        }
        
        if let Some(bytes) = node_receiver.recv().await {
            assert_eq!(bytes, r#"{"src":"n1","body":{"in_reply_to":2,"msg_id":2,"type":"echo_ok"}}"#.as_bytes());
        } else {
            assert!(false, "Couldn't recv echo reply");
        }
    }
    
    #[tokio::test]
    #[should_panic(expected = "duplicate message handler for foo message type")]
    async fn test_duplicate_handler_panics() {
        let (node, _, _) = new_node();
        
        node.handle("foo", Arc::new(|_| Ok(().into())));
        node.handle("foo", Arc::new(|_| Ok(().into())));
    }
}
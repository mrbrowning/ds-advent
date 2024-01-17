use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt},
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};

use crate::rpc_error::MaelstromError;

pub struct Ingress<M: for<'de> Deserialize<'de>> {
    sender: UnboundedSender<M>,
    reader: Box<dyn AsyncBufRead + Send + Unpin>,
}

impl<M: for<'de> Deserialize<'de>> Ingress<M> {
    pub fn new(sender: UnboundedSender<M>, reader: Box<dyn AsyncBufRead + Send + Unpin>) -> Self {
        Self { sender, reader }
    }

    pub async fn run(&mut self) -> Result<(), MaelstromError> {
        loop {
            let mut line = String::new();
            let _ = self.reader.read_line(&mut line).await?;

            let msg: M = serde_json::from_str(&line)?;
            self.sender
                .send(msg)
                .map_err(|e| MaelstromError::Other(format!("Ingress receiver error: {}", e)))?;
        }
    }

    pub fn to_sender_type<SendType: for<'de> Deserialize<'de>>(
        self,
        sender: UnboundedSender<SendType>,
    ) -> Ingress<SendType> {
        Ingress {
            sender,
            reader: self.reader,
        }
    }
}

pub struct Egress<M: Serialize> {
    receiver: UnboundedReceiver<M>,
    writer: Box<dyn AsyncWrite + Send + Unpin>,
}

impl<M: Serialize> Egress<M> {
    pub fn new(receiver: UnboundedReceiver<M>, writer: Box<dyn AsyncWrite + Send + Unpin>) -> Self {
        Self { receiver, writer }
    }

    pub async fn run(&mut self) -> Result<(), MaelstromError> {
        loop {
            let out = self
                .receiver
                .recv()
                .await
                .ok_or(MaelstromError::Other("Sender to Egress hung up".into()))?;
            let mut buf = serde_json::to_string(&out)?;

            buf.push('\n');
            self.writer.write_all(buf.as_bytes()).await?;
        }
    }

    pub fn to_receiver_type<RecvType: Serialize>(
        self,
        receiver: UnboundedReceiver<RecvType>,
    ) -> Egress<RecvType> {
        Egress {
            receiver,
            writer: self.writer,
        }
    }
}

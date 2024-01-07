#![allow(unused)]

use std::{
    io::{Error, ErrorKind},
    task::Poll,
};

use futures::{pin_mut, Future, FutureExt};
use maelstrom_csp::message::{ErrorMessagePayload, InitMessagePayload, MessagePayload};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{self, AsyncRead, AsyncWrite, BufReader},
    sync::mpsc::{self, channel},
};

pub struct AsyncChannelReader {
    /// An AsyncRead-conforming wrapper around a channel, so tests can write to node's stdin.
    receiver: mpsc::Receiver<String>,
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
                buf.put_slice(&msg.as_bytes());
                Poll::Ready(Ok(()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncChannelReader {
    pub fn new(receiver: mpsc::Receiver<String>) -> Self {
        Self { receiver }
    }
}

pub struct AsyncChannelWriter {
    /// An AsyncWrite-conforming wrapper around a channel, so tests can read node's stdout.
    sender: mpsc::Sender<String>,
}

impl AsyncWrite for AsyncChannelWriter {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let buf_len = buf.len();
        let as_str = String::from_utf8(buf.to_vec());
        if let Err(e) = as_str {
            // This kind of violates LSP in spirit if not in letter, but we only write strings.
            return Poll::Ready(Err(Error::new(ErrorKind::Other, e)));
        }

        let fut = self.sender.send(as_str.unwrap()).map(|x| {
            x.map(|_| buf_len)
                .map_err(|e| Error::new(ErrorKind::Other, e))
        });
        pin_mut!(fut);

        fut.poll(cx)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncChannelWriter {
    pub fn new(sender: mpsc::Sender<String>) -> Self {
        Self { sender }
    }
}

pub fn get_reader_pair(size: usize) -> (BufReader<AsyncChannelReader>, mpsc::Sender<String>) {
    let (tx, rx) = channel::<String>(size);
    let reader = BufReader::new(AsyncChannelReader::new(rx));

    (reader, tx)
}

pub fn get_writer_pair(size: usize) -> (AsyncChannelWriter, mpsc::Receiver<String>) {
    let (tx, rx) = channel::<String>(size);
    let writer = AsyncChannelWriter::new(tx);

    (writer, rx)
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type")]
pub enum EchoPayload {
    #[serde(rename = "echo")]
    Echo,

    #[serde(rename = "echo_ok")]
    EchoOk,

    #[serde(rename = "init")]
    Init(InitMessagePayload),

    #[serde(rename = "init_ok")]
    InitOk,

    #[serde(rename = "error")]
    Error(ErrorMessagePayload),

    #[serde(rename = "reply_ok")]
    ReplyOk,
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

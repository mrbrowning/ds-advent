use std::{
    fmt::Display,
    ops::{Add, AddAssign},
};

use serde::{Deserialize, Serialize};

pub const INIT_MESSAGE_TYPE: &str = "init";
pub const INIT_MESSAGE_REPLY_TYPE: &str = "init_ok";
pub const ERROR_MESSAGE_TYPE: &str = "error";

#[derive(Serialize, Deserialize, PartialEq, Eq, Ord, PartialOrd, Clone, Copy, Hash, Debug)]
pub struct MessageId(i64);

impl Display for MessageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Add<i64> for MessageId {
    type Output = MessageId;

    fn add(self, rhs: i64) -> Self::Output {
        MessageId(self.0 + rhs)
    }
}

impl AddAssign<i64> for MessageId {
    fn add_assign(&mut self, rhs: i64) {
        *self = MessageId(self.0 + rhs);
    }
}

impl From<i64> for MessageId {
    fn from(value: i64) -> Self {
        MessageId(value)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Copy, Hash, Debug)]
pub struct ErrorCode(i64);

impl Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<i64> for ErrorCode {
    fn from(value: i64) -> Self {
        ErrorCode(value)
    }
}

impl From<ErrorCode> for i64 {
    fn from(value: ErrorCode) -> Self {
        value.0
    }
}

pub trait MessagePayload: std::fmt::Debug + Default {
    fn as_init_msg(&self) -> Option<InitMessagePayload>;

    fn to_init_ok_msg() -> Self;

    fn to_err_msg(err: ErrorMessagePayload) -> Self;
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum LocalMessageType {
    Cancel,
    Other(String),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LocalMessage {
    pub msg_id: MessageId,
    pub node_id: String,
    pub msg_type: LocalMessageType,
}

// "Parsed" in the sense used by Alexis King: https://lexi-lambda.github.io/blog/2019/11/05/parse-don-t-validate/
#[derive(Clone, Debug)]
pub struct ParsedMessage<M> {
    pub src: String,
    pub body: M,
}

#[derive(Clone, Debug)]
pub struct ParsedReply<M> {
    pub src: String,
    pub in_reply_to: MessageId,
    pub body: M,
}

#[derive(Clone, Debug)]
pub struct ParsedRpc<M> {
    pub src: String,
    pub msg_id: MessageId,
    pub body: M,
}

#[derive(Clone, Debug)]
pub enum ParsedInput<M> {
    Message(ParsedMessage<M>),
    Reply(ParsedReply<M>),
    Rpc(ParsedRpc<M>),
}

impl<M> From<ParsedMessage<M>> for ParsedInput<M> {
    fn from(msg: ParsedMessage<M>) -> Self {
        ParsedInput::Message(msg)
    }
}

impl<M> From<ParsedReply<M>> for ParsedInput<M> {
    fn from(msg: ParsedReply<M>) -> Self {
        ParsedInput::Reply(msg)
    }
}

impl<M> From<ParsedRpc<M>> for ParsedInput<M> {
    fn from(msg: ParsedRpc<M>) -> Self {
        ParsedInput::Rpc(msg)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Message<B: MessagePayload> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub src: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub dest: Option<String>,

    pub body: MessageBody<B>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MessageBody<P: MessagePayload> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<MessageId>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<MessageId>,

    #[serde(flatten)]
    pub contents: P,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct InitMessagePayload {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct InitMessageReplyPayload {
    #[serde(rename = "type")]
    pub msg_type: String,
}

impl InitMessageReplyPayload {
    #[allow(unused)]
    fn new() -> Self {
        InitMessageReplyPayload {
            msg_type: INIT_MESSAGE_REPLY_TYPE.into(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ErrorMessagePayload {
    pub code: ErrorCode,
    pub text: String,
}

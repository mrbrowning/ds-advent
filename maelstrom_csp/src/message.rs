use serde::{Deserialize, Serialize};

pub const INIT_MESSAGE_TYPE: &str = "init";
pub const INIT_MESSAGE_REPLY_TYPE: &str = "init_ok";
pub const ERROR_MESSAGE_TYPE: &str = "error";

pub trait MessagePayload: std::fmt::Debug {
    fn as_init_msg(&self) -> Option<InitMessagePayload>;

    fn to_init_ok_msg() -> Self;
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum LocalMessage {
    Cancel(i64),
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
    pub msg_id: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<i64>,

    #[serde(skip)]
    pub local_msg: Option<LocalMessage>,

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
    pub code: i64,
    pub text: String,
}

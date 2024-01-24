#![allow(unused)]
use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

use crate::message::{
    ErrorMessagePayload, InitMessagePayload, Message, MessageBody, MessageId, MessagePayload,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum KvType {
    LinKv,
    SeqKv,
    LwwKv,
}

impl std::fmt::Display for KvType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            KvType::LinKv => "lin-kv",
            KvType::SeqKv => "seq-kv",
            KvType::LwwKv => "lww-kv",
        };
        write!(f, "{}", s)
    }
}

impl From<KvType> for String {
    fn from(kv_type: KvType) -> Self {
        format!("{}", kv_type)
    }
}

pub trait KvMessageExt: MessagePayload {
    type Value;
    fn to_read_message(key: &str, msg_id: MessageId, kv_type: KvType) -> Message<Self>;

    fn to_write_message(
        key: &str,
        value: Self::Value,
        msg_id: MessageId,
        kv_type: KvType,
    ) -> Message<Self>;

    fn to_cas_message(
        key: &str,
        from: Self::Value,
        to: Self::Value,
        create_if_not_exists: bool,
        msg_id: MessageId,
        kv_type: KvType,
    ) -> Message<Self>;
}

#[derive(Debug)]
pub struct KvClient<M, V> {
    kv_type: KvType,

    _marker: PhantomData<(M, V)>,
}

impl<M: KvMessageExt<Value = V>, V: Serialize + std::fmt::Debug> KvClient<M, V> {
    pub fn new(kv_type: KvType) -> Self {
        KvClient {
            kv_type,
            _marker: PhantomData,
        }
    }

    pub fn read_message(&self, key: &str, msg_id: MessageId) -> Message<M> {
        M::to_read_message(key, msg_id, self.kv_type)
    }

    pub fn write_message(&self, key: &str, value: V, msg_id: MessageId) -> Message<M> {
        M::to_write_message(key, value, msg_id, self.kv_type)
    }

    pub fn cas_message(
        &self,
        key: &str,
        from: V,
        to: V,
        create_if_not_exists: bool,
        msg_id: MessageId,
    ) -> Message<M> {
        M::to_cas_message(key, from, to, create_if_not_exists, msg_id, self.kv_type)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct KvReadPayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct KvReadOkPayload<V: Serialize> {
    pub value: V,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct KvWritePayload<V: Serialize> {
    pub key: String,
    pub value: V,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct KvCasPayload<V: Serialize> {
    pub key: String,
    pub from: V,
    pub to: V,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub create_if_not_exists: Option<bool>,
}

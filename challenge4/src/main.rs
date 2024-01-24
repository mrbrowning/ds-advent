use std::{
    cmp::max,
    collections::{HashMap, HashSet},
};

use log::error;
use maelstrom_csp::{
    get_node_and_io,
    kv::{KvCasPayload, KvMessageExt, KvReadPayload, KvType, KvWritePayload},
    message::{
        ErrorCode, ErrorMessagePayload, InitMessagePayload, Message, MessageBody, MessageId,
        MessagePayload,
    },
    node::NodeDelegate,
    rpc_error::{ErrorType, MaelstromError},
    send,
};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{stdin, stdout, BufReader},
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};

use message_macro::maelstrom_message;

const KV_TIMEOUT_MS: u64 = 200;
const COUNTER_KEY: &str = "counter";

#[derive(Serialize, Deserialize, Clone, Debug)]
struct AddPayload {
    delta: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct ReadOkPayload {
    value: i64,
}

#[maelstrom_message]
#[derive(Serialize, Deserialize, Clone, Debug)]
enum ChallengePayload {
    #[serde(rename = "add")]
    Add(AddPayload),

    #[serde(rename = "add_ok")]
    AddOk,

    #[serde(rename = "read")]
    Read(KvReadPayload),

    #[serde(rename = "read_ok")]
    ReadOk(ReadOkPayload),

    #[serde(rename = "write")]
    KvWrite(KvWritePayload<i64>),

    #[serde(rename = "write_ok")]
    KvWriteOk,

    #[serde(rename = "cas")]
    KvCas(KvCasPayload<i64>),

    #[serde(rename = "cas_ok")]
    KvCasOk,
}

impl KvMessageExt for ChallengePayload {
    type Value = i64;

    fn to_read_message(
        key: &str,
        msg_id: MessageId,
        kv_type: maelstrom_csp::kv::KvType,
    ) -> Message<Self> {
        Message {
            src: None,
            dest: Some(format!("{}", kv_type)),
            body: MessageBody {
                msg_id: Some(msg_id),
                in_reply_to: None,
                local_msg: None,
                contents: ChallengePayload::Read(KvReadPayload {
                    key: Some(key.to_string()),
                }),
            },
        }
    }

    fn to_write_message(
        key: &str,
        value: i64,
        msg_id: MessageId,
        kv_type: maelstrom_csp::kv::KvType,
    ) -> Message<Self> {
        Message {
            src: None,
            dest: Some(format!("{}", kv_type)),
            body: MessageBody {
                msg_id: Some(msg_id),
                in_reply_to: None,
                local_msg: None,
                contents: ChallengePayload::KvWrite(KvWritePayload {
                    key: key.to_string(),
                    value,
                }),
            },
        }
    }

    fn to_cas_message(
        key: &str,
        from: i64,
        to: i64,
        create_if_not_exists: bool,
        msg_id: MessageId,
        kv_type: maelstrom_csp::kv::KvType,
    ) -> Message<Self> {
        Message {
            src: None,
            dest: Some(format!("{}", kv_type)),
            body: MessageBody {
                msg_id: Some(msg_id),
                in_reply_to: None,
                local_msg: None,
                contents: ChallengePayload::KvCas(KvCasPayload {
                    key: key.to_string(),
                    from,
                    to,
                    create_if_not_exists: Some(create_if_not_exists),
                }),
            },
        }
    }
}

type ChallengeMessage = Message<ChallengePayload>;

#[derive(Debug)]
enum RequestKind {
    Read(Vec<i64>),
    Cas(i64),
    ReadForCas,
}

#[derive(Debug)]
struct ReplyRecord {
    request: ChallengeMessage,
    request_kind: RequestKind,
}

#[derive(Debug)]
struct CounterDelegate {
    node_id: String,
    node_ids: HashSet<String>,

    msg_tx: UnboundedSender<Message<ChallengePayload>>,
    msg_rx: Option<UnboundedReceiver<Message<ChallengePayload>>>,
    self_tx: UnboundedSender<Message<ChallengePayload>>,

    cached_counter: i64,

    msg_id: MessageId,
    outstanding_replies: HashSet<(MessageId, String)>,
    reply_records: HashMap<MessageId, ReplyRecord>,
}

impl CounterDelegate {
    fn quorum_value(&self, neighbor_values: &Vec<i64>) -> Option<i64> {
        if neighbor_values.len() == self.node_ids.len() + 1 {
            // We've heard from everyone including the KV store, or they've timed out and gotten a default value, so
            // we can name a quorum value.
            return neighbor_values.iter().max().copied();
        }

        None
    }

    fn handle_read_timeout(
        &mut self,
        msg_id: MessageId,
        mut quorum: Vec<i64>,
        request: ChallengeMessage,
    ) -> Result<(), MaelstromError> {
        let msg_tx = self.get_msg_tx();

        quorum.push(self.cached_counter);
        if let Some(value) = self.quorum_value(&quorum) {
            // We got a timeout or an actual read from everyone, respond to the reader.
            // except distinguish between neighbors and clients
            self.cached_counter = value;
            let reply = Self::gen_reply(
                request,
                ChallengePayload::ReadOk(ReadOkPayload {
                    value: self.cached_counter,
                }),
            )?;

            send!(msg_tx, reply, "Delegate egress hung up: {}");
        } else {
            // Whoever this is timed out, so substitute our maybe-stale value for its quorum read.
            self.reply_records.insert(
                msg_id,
                ReplyRecord {
                    request,
                    request_kind: RequestKind::Read(quorum),
                },
            );
        }

        Ok(())
    }

    async fn handle_cas_timeout(
        &mut self,
        delta: i64,
        request: ChallengeMessage,
    ) -> Result<(), MaelstromError> {
        let msg_id = self.next_msg_id();
        let proposed_value = self.cached_counter + delta;

        self.reply_records.insert(
            msg_id,
            ReplyRecord {
                request,
                request_kind: RequestKind::Cas(delta),
            },
        );
        self.rpc_with_timeout_with_msg_id(
            String::from(KvType::SeqKv),
            ChallengePayload::KvCas(KvCasPayload {
                key: COUNTER_KEY.into(),
                from: self.cached_counter,
                to: proposed_value,
                create_if_not_exists: Some(true),
            }),
            KV_TIMEOUT_MS,
            msg_id,
        )
        .await
    }

    async fn handle_cas_read_timeout(
        &mut self,
        request: ChallengeMessage,
    ) -> Result<(), MaelstromError> {
        let msg_id = self.next_msg_id();
        self.reply_records.insert(
            msg_id,
            ReplyRecord {
                request,
                request_kind: RequestKind::ReadForCas,
            },
        );

        self.rpc_with_timeout_with_msg_id(
            String::from(KvType::SeqKv),
            ChallengePayload::Read(KvReadPayload {
                key: Some(COUNTER_KEY.into()),
            }),
            KV_TIMEOUT_MS,
            msg_id,
        )
        .await
    }

    fn handle_quorum_read_ok(
        &mut self,
        msg_id: MessageId,
        value: i64,
        request: ChallengeMessage,
        mut quorum: Vec<i64>,
    ) -> Result<(), MaelstromError> {
        let msg_tx = self.get_msg_tx();

        quorum.push(value);
        if let Some(value) = self.quorum_value(&quorum) {
            // We got replies or timeouts from everyone in our quorum group, respond to the reader.
            self.cached_counter = max(value, self.cached_counter);
            let reply = Self::gen_reply(
                request,
                ChallengePayload::ReadOk(ReadOkPayload {
                    value: self.cached_counter,
                }),
            )?;

            send!(msg_tx, reply, "Delegate egress hung up: {}");
        } else {
            // We haven't heard from everybody yet, so wait on that.
            self.reply_records.insert(
                msg_id,
                ReplyRecord {
                    request,
                    request_kind: RequestKind::Read(quorum),
                },
            );
        }

        Ok(())
    }

    async fn handle_cas_read_ok(
        &mut self,
        value: i64,
        request: ChallengeMessage,
    ) -> Result<(), MaelstromError> {
        self.cached_counter = max(self.cached_counter, value);
        let msg_id = self.next_msg_id();
        let delta = match &request.body.contents {
            ChallengePayload::Add(a) => a.delta,
            _ => panic!("Unexpected message type in reply record: {:?}", request),
        };
        self.reply_records.insert(
            msg_id,
            ReplyRecord {
                request,
                request_kind: RequestKind::Cas(delta),
            },
        );

        self.rpc_with_timeout_with_msg_id(
            String::from(KvType::SeqKv),
            ChallengePayload::KvCas(KvCasPayload {
                key: COUNTER_KEY.into(),
                from: self.cached_counter,
                to: self.cached_counter + delta,
                create_if_not_exists: None,
            }),
            KV_TIMEOUT_MS,
            msg_id,
        )
        .await
    }

    fn handle_cas_ok(&mut self, request: ChallengeMessage) -> Result<(), MaelstromError> {
        let msg_tx = self.get_msg_tx();
        let delta = match &request.body.contents {
            ChallengePayload::Add(payload) => payload.delta,
            _ => panic!("Unexpected message type in reply record: {:?}", request),
        };
        self.cached_counter += delta;

        let reply = Self::gen_reply(request, ChallengePayload::AddOk)?;
        send!(msg_tx, reply, "Delegate egress hung up: {}");

        Ok(())
    }

    async fn handle_cas_error(
        &mut self,
        error_code: ErrorCode,
        request: ChallengeMessage,
    ) -> Result<(), MaelstromError> {
        let error_type = ErrorType::try_from(error_code)?;
        match error_type {
            ErrorType::KeyDoesNotExist => {
                let reply = Self::gen_reply(
                    request,
                    ChallengePayload::ReadOk(ReadOkPayload {
                        value: self.cached_counter,
                    }),
                )?;

                let msg_tx = self.get_msg_tx();
                send!(msg_tx, reply, "Delegate egress hung up: {}");

                Ok(())
            }
            ErrorType::PreconditionFailed => {
                let msg_id = self.next_msg_id();
                self.reply_records.insert(
                    msg_id,
                    ReplyRecord {
                        request,
                        request_kind: RequestKind::ReadForCas,
                    },
                );

                // Just avoid parsing the string and send out a read again.
                self.rpc_with_timeout_with_msg_id(
                    String::from(KvType::SeqKv),
                    ChallengePayload::Read(KvReadPayload {
                        key: Some(COUNTER_KEY.into()),
                    }),
                    KV_TIMEOUT_MS,
                    msg_id,
                )
                .await
            }
            _ => panic!("Got error from kv store: {:?}", error_type),
        }
    }

    async fn handle_read_request(
        &mut self,
        request: ChallengeMessage,
    ) -> Result<(), MaelstromError> {
        if self.node_ids.contains(request.src.as_ref().unwrap()) {
            let msg_tx = self.get_msg_tx();
            let reply = Self::gen_reply(
                request,
                ChallengePayload::ReadOk(ReadOkPayload {
                    value: self.cached_counter,
                }),
            )?;

            send!(msg_tx, reply, "Delegate egress hung up: {}");
            return Ok(());
        }

        let msg_id = self.next_msg_id();
        self.reply_records.insert(
            msg_id,
            ReplyRecord {
                request,
                request_kind: RequestKind::Read(vec![]),
            },
        );

        self.rpc_with_timeout_with_msg_id(
            String::from(KvType::SeqKv),
            ChallengePayload::Read(KvReadPayload {
                key: Some(COUNTER_KEY.into()),
            }),
            KV_TIMEOUT_MS,
            msg_id,
        )
        .await?;

        // Probably not possible in general but it'd be wonderful if the borrow checker understood the split borrow
        // happening here, with node_ids being able to be borrowed immutably for the iteration and not being referenced
        // at all in the call chain emanating from the loop body.
        let node_ids: Vec<String> = self
            .node_ids
            .iter()
            .filter_map(|id| {
                if id != &self.node_id {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect();
        for id in node_ids {
            self.rpc_with_timeout_with_msg_id(
                id,
                ChallengePayload::Read(KvReadPayload { key: None }),
                KV_TIMEOUT_MS,
                msg_id,
            )
            .await?;
        }

        Ok(())
    }

    async fn handle_add_request(
        &mut self,
        delta: i64,
        request: ChallengeMessage,
    ) -> Result<(), MaelstromError> {
        let msg_id = self.next_msg_id();
        self.reply_records.insert(
            msg_id,
            ReplyRecord {
                request,
                request_kind: RequestKind::Cas(delta),
            },
        );

        self.rpc_with_timeout_with_msg_id(
            String::from(KvType::SeqKv),
            ChallengePayload::KvCas(KvCasPayload {
                key: COUNTER_KEY.into(),
                from: self.cached_counter,
                to: self.cached_counter + delta,
                create_if_not_exists: Some(true),
            }),
            KV_TIMEOUT_MS,
            msg_id,
        )
        .await
    }
}

// TODO: the final request needs to be up to date, and we can't just rely on the kv store. Reads should fan out:
// since the counter only ever grows, we know that the highest value is the most recent. During a partition, we should
// still wait until the timeout and then return our best knowledge of what the current is. But so our read should
// depend on everyone we're in contact with, and it should return the highest value seen. Whether from kv or neighbors.

impl NodeDelegate for CounterDelegate {
    type MessageType = ChallengePayload;

    fn init(
        node_id: impl AsRef<str>,
        node_ids: impl AsRef<Vec<String>>,
        msg_tx: UnboundedSender<Message<Self::MessageType>>,
        msg_rx: UnboundedReceiver<Message<Self::MessageType>>,
        self_tx: UnboundedSender<Message<Self::MessageType>>,
    ) -> Self {
        Self {
            node_id: node_id.as_ref().into(),
            node_ids: node_ids
                .as_ref()
                .clone()
                .into_iter()
                .filter(|id| id != node_id.as_ref())
                .collect(),
            msg_tx,
            msg_rx: Some(msg_rx),
            self_tx,
            cached_counter: 0,
            msg_id: 0.into(),
            outstanding_replies: HashSet::new(),
            reply_records: HashMap::new(),
        }
    }

    fn get_outstanding_replies(&self) -> &HashSet<(MessageId, String)> {
        &self.outstanding_replies
    }

    fn get_outstanding_replies_mut(&mut self) -> &mut HashSet<(MessageId, String)> {
        &mut self.outstanding_replies
    }

    #[allow(clippy::manual_async_fn)]
    fn handle_local_message(
        &mut self,
        msg: maelstrom_csp::message::LocalMessage,
    ) -> impl futures::prelude::Future<Output = Result<(), MaelstromError>> + Send {
        async move {
            match msg.msg_type {
                maelstrom_csp::message::LocalMessageType::Cancel => {
                    let reply_record = if let Some(record) = self.reply_records.remove(&msg.msg_id)
                    {
                        record
                    } else {
                        return Ok(());
                    };

                    match reply_record.request_kind {
                        RequestKind::Read(v) => {
                            // This was a quorum read to either a node or the kv store that timed out.
                            self.handle_read_timeout(msg.msg_id, v, reply_record.request)
                        }

                        RequestKind::Cas(delta) => {
                            // This was a CAS operation to the kv store that timed out.
                            self.handle_cas_timeout(delta, reply_record.request).await
                        }
                        RequestKind::ReadForCas => {
                            // This was a read, initiated by a failed CAS operation to the kv store, that timed out.
                            self.handle_cas_read_timeout(reply_record.request).await
                        }
                    }
                }
                maelstrom_csp::message::LocalMessageType::Other(_) => unreachable!(),
            }
        }
    }

    #[allow(clippy::manual_async_fn)]
    fn handle_reply(
        &mut self,
        reply: Message<Self::MessageType>,
    ) -> impl futures::prelude::Future<Output = Result<(), MaelstromError>> + Send {
        async move {
            let reply_record =
                if let Some(record) = self.reply_records.remove(&reply.body.in_reply_to.unwrap()) {
                    record
                } else {
                    return Ok(());
                };

            match &reply.body.contents {
                ChallengePayload::ReadOk(payload) => {
                    match reply_record.request_kind {
                        RequestKind::Read(quorum_reads) => {
                            // This is a reply to a quorum read request we sent to either another node or our kv store.
                            self.handle_quorum_read_ok(
                                reply.body.in_reply_to.unwrap(),
                                payload.value,
                                reply_record.request,
                                quorum_reads,
                            )
                        }
                        RequestKind::ReadForCas => {
                            // This is a reply to a read request initiated due to a failed CAS operation.
                            self.handle_cas_read_ok(payload.value, reply_record.request)
                                .await
                        }
                        RequestKind::Cas(_) => Err(MaelstromError::Other(format!(
                            "Expected reply to cas from kv store in response to request {:?}",
                            reply_record.request
                        ))),
                    }
                }
                ChallengePayload::KvCasOk => self.handle_cas_ok(reply_record.request),
                ChallengePayload::Error(e) => {
                    self.handle_cas_error(e.code, reply_record.request).await
                }
                _ => Err(MaelstromError::Other(format!(
                    "Unexpected reply: {:?}",
                    reply
                ))),
            }
        }
    }

    #[allow(clippy::manual_async_fn)]
    fn handle_message(
        &mut self,
        message: Message<Self::MessageType>,
    ) -> impl futures::prelude::Future<Output = Result<(), MaelstromError>> + Send {
        async move {
            match &message.body.contents {
                ChallengePayload::Read(_) => self.handle_read_request(message).await,
                ChallengePayload::Add(a) => self.handle_add_request(a.delta, message).await,
                _ => Err(MaelstromError::Other(format!(
                    "Got unexpected message in handle_message: {:?}",
                    message
                ))),
            }
        }
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
        self.self_tx.clone()
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let (node, mut ingress, mut egress) =
        get_node_and_io::<ChallengePayload, CounterDelegate>(BufReader::new(stdin()), stdout());

    tokio::spawn(async move {
        if let Err(e) = ingress.run().await {
            panic!("Ingress died: {}", e);
        }
    });
    tokio::spawn(async move {
        if let Err(e) = egress.run().await {
            panic!("Ingress died: {}", e);
        }
    });

    match node.run().await {
        Err(e) => {
            error!("Node init loop failed: {}", e);
        }
        Ok(initialized) => {
            if let Err(e) = initialized.run().await {
                error!("Node failed: {}", e);
            }
        }
    }
}

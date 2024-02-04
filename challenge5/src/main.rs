use core::hash::{Hash, Hasher};
use std::{
    cmp::min,
    collections::{hash_map::DefaultHasher, HashMap},
    future::Future,
};

use log::error;
use maelstrom_csp2::{
    message::{
        ErrorMessagePayload, InitMessagePayload, Message, MessageId, MessagePayload, ParsedInput,
        ParsedMessage, ParsedRpc,
    },
    node::{Command, NodeDelegate, NodeInput, ReplyRecord},
    rpc_error::{ErrorType, MaelstromError, RPCError},
    send,
};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{stdin, stdout, BufReader},
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
};

use message_macro::maelstrom_message;

const PEER_TIMEOUT_MS: u64 = 200;

#[maelstrom_message]
#[derive(Serialize, Deserialize, Clone, Debug)]
enum ChallengePayload {
    #[serde(rename = "send")]
    Send(SendPayload),

    #[serde(rename = "send_ok")]
    SendOk(SendOkPayload),

    #[serde(rename = "poll")]
    Poll(PollPayload),

    #[serde(rename = "poll_ok")]
    PollOk(PollOkPayload),

    #[serde(rename = "commit_offsets")]
    CommitOffsets(CommitOffsetsPayload),

    #[serde(rename = "commit_offsets_ok")]
    CommitOffsetsOk,

    #[serde(rename = "list_committed_offsets")]
    ListCommittedOffsets(ListCommittedOffsetsPayload),

    #[serde(rename = "list_committed_offsets_ok")]
    ListCommittedOffsetsOk(ListCommittedOffsetsOkPayload),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct SendPayload {
    key: String,
    msg: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct SendOkPayload {
    offset: usize,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct PollPayload {
    offsets: HashMap<String, usize>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct PollOkPayload {
    msgs: HashMap<String, Vec<(usize, i64)>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct CommitOffsetsPayload {
    offsets: HashMap<String, usize>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct ListCommittedOffsetsPayload {
    keys: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct ListCommittedOffsetsOkPayload {
    offsets: HashMap<String, usize>,
}

#[derive(Clone, Debug)]
struct ChallengeCommand;

fn emplace<'a, V: Hash, S: Hash>(item: &V, sites: impl Iterator<Item = &'a S>) -> Option<&'a S> {
    // Use rendezvous hashing to determine which node id from `sites` `item` lives on.
    let sites: Vec<_> = sites.collect();
    let mut scores: Vec<_> = sites
        .iter()
        .enumerate()
        .map(|(i, s)| {
            let mut hasher = DefaultHasher::new();
            (&item, s).hash(&mut hasher);

            (hasher.finish(), i)
        })
        .collect();
    scores.sort();

    // Realistically, first would work just as well here, since all that matters is that there's a
    // total order over the per-node hashes and not what it is.
    scores.last().and_then(|(_, i)| sites.get(*i)).copied()
}

#[derive(Debug)]
struct Topics {
    // Obviously this is just a toy implementation, but in a real scenario we'd want to have non-trivial storage logic
    // behind the veil of this struct's interface, so we'll play pretend a little here.
    topics: HashMap<String, Topic>,
}

impl Topics {
    fn new() -> Self {
        Self {
            topics: HashMap::new(),
        }
    }

    fn append(&mut self, key: String, msg: i64) -> usize {
        self.topics.entry(key).or_insert(Topic::new()).append(msg)
    }

    fn msgs_from_offset(&self, key: impl AsRef<str>, offset: usize) -> Option<Vec<(usize, i64)>> {
        self.topics
            .get(key.as_ref())
            .map(|t| t.msgs_from_offset(offset))
    }

    fn commit_offset(&mut self, key: impl AsRef<str>, offset: usize) -> Result<(), MaelstromError> {
        self.topics
            .get_mut(key.as_ref())
            .map(|t| t.commit_offset(offset))
            .ok_or(MaelstromError::RPCError(RPCError::new(
                ErrorType::KeyDoesNotExist.into(),
                format!("Key does not exist: {}", key.as_ref()),
            )))?
    }
}

#[derive(Debug)]
struct Topic {
    msgs: Vec<i64>,
    offset: usize,
}

impl Topic {
    fn new() -> Self {
        Self {
            msgs: Vec::new(),
            offset: 0,
        }
    }

    fn append(&mut self, msg: i64) -> usize {
        self.msgs.push(msg);

        self.msgs.len() - 1
    }

    fn msgs_from_offset(&self, offset: usize) -> Vec<(usize, i64)> {
        self.msgs[min(offset, self.msgs.len())..]
            .iter()
            .enumerate()
            .map(|(i, v)| (i + offset, *v))
            .collect()
    }

    fn commit_offset(&mut self, offset: usize) -> Result<(), MaelstromError> {
        if offset < self.msgs.len() {
            self.offset = offset;
            Ok(())
        } else {
            Err(MaelstromError::RPCError(RPCError::new(
                ErrorType::KeyDoesNotExist.into(),
                format!("Key does not exist: {}", offset),
            )))
        }
    }
}

struct LogDelegate {
    msg_rx: Option<UnboundedReceiver<ParsedInput<ChallengePayload>>>,
    msg_tx: UnboundedSender<Message<ChallengePayload>>,

    cmd_rx: Option<UnboundedReceiver<Command<ChallengePayload, ChallengeCommand>>>,
    cmd_tx: UnboundedSender<Command<ChallengePayload, ChallengeCommand>>,

    node_id: String,
    node_ids: Box<[String]>,

    topics: Topics,

    msg_id: MessageId,
    reply_records: HashMap<MessageId, ReplyRecord<ChallengePayload, ChallengeCommand>>,
}

impl LogDelegate {
    fn get_msgs<'a>(
        &self,
        offsets: impl Iterator<Item = (&'a str, usize)>,
    ) -> HashMap<String, Vec<(usize, i64)>> {
        // Get a map of topics to messages from those topics, starting from the relevant offsets.
        offsets
            .filter_map(|(topic, offset)| {
                self.topics
                    .msgs_from_offset(topic, offset)
                    .map(|messages| (topic.to_owned(), messages))
            })
            .collect()
    }

    fn placements<'a>(
        &self,
        topics: impl Iterator<Item = &'a str>,
    ) -> HashMap<String, Vec<String>> {
        // Get a map of nodes to the topics they own.
        let mut placements: HashMap<String, Vec<String>> = HashMap::new();
        for t in topics {
            if let Some(node) = emplace(&t, self.node_ids.iter()) {
                placements
                    .entry(node.clone())
                    .or_default()
                    .push(t.to_string());
            }
        }

        placements
    }

    async fn handle_send(
        &mut self,
        src: String,
        msg_id: MessageId,
        key: String,
        msg: i64,
    ) -> Result<(), MaelstromError> {
        let msg_tx = self.get_msg_tx();
        let owner = emplace(&key, self.node_ids.iter()).unwrap();
        if owner == &self.node_id || self.is_cluster_member(&src) {
            // This is a forwarded request, don't contact other nodes.
            let offset = self.topics.append(key, msg);
            let reply = self.reply(
                src,
                msg_id,
                ChallengePayload::SendOk(SendOkPayload { offset }),
            );
            send!(msg_tx, reply, "Egress hung up: {}");

            return Ok(());
        }

        let cmd_tx = self.get_command_tx();
        let (task_tx, mut task_rx) =
            unbounded_channel::<NodeInput<ChallengePayload, ChallengeCommand>>();
        let owner = owner.clone();

        tokio::spawn(async move {
            // Forward this send request to the owner and wait on a response.
            cmd_tx
                .send(Command::Rpc {
                    dest: owner.clone(),
                    msg: ChallengePayload::Send(SendPayload { key, msg }),
                    sink: task_tx,
                    timeout_ms: Some(PEER_TIMEOUT_MS),
                    msg_id: None,
                })
                .expect("Node hung up on itself");

            let result = task_rx.recv().await.expect("Node hung up on itself");
            if let NodeInput::Message(ParsedInput::Reply(r)) = result {
                match r.body {
                    ChallengePayload::SendOk(s) => {
                        cmd_tx
                            .send(Command::Reply {
                                dest: src,
                                msg_id,
                                msg: ChallengePayload::SendOk(s),
                            })
                            .expect("Node hung up on itself");
                    }
                    _ => panic!("Unexpected reply: {:?}", r.body),
                }
            }
        });

        Ok(())
    }

    async fn handle_poll(
        &mut self,
        src: String,
        msg_id: MessageId,
        mut offsets: HashMap<String, usize>,
    ) -> Result<(), MaelstromError> {
        let msg_tx = self.get_msg_tx();
        if self.is_cluster_member(&src) {
            let msgs = self.get_msgs(
                offsets
                    .iter()
                    .map(|(topic, offset)| (topic.as_str(), *offset)),
            );
            let reply = self.reply(
                src,
                msg_id,
                ChallengePayload::PollOk(PollOkPayload { msgs }),
            );
            send!(msg_tx, reply, "Egress hung up: {}");

            return Ok(());
        }

        let mut placements = self.placements(offsets.keys().map(|k| k.as_str()));
        let mut msgs: HashMap<String, Vec<(usize, i64)>> = self.get_msgs(
            placements
                .remove(&self.node_id)
                .unwrap_or_default()
                .iter()
                .filter_map(|topic| offsets.get(topic).map(|o| (topic.as_str(), *o))),
        );
        let cmd_tx = self.get_command_tx();
        let (task_tx, mut task_rx) =
            unbounded_channel::<NodeInput<ChallengePayload, ChallengeCommand>>();

        tokio::spawn(async move {
            // Forward a poll to the owners of topics from `offsets`, merge the responses, and reply to the requester.
            for (node, topics) in placements.iter() {
                cmd_tx
                    .send(Command::Rpc {
                        dest: node.clone(),
                        msg: ChallengePayload::Poll(PollPayload {
                            offsets: topics
                                .iter()
                                .filter_map(|t| offsets.remove(t).map(|o| (t.to_string(), o)))
                                .collect(),
                        }),
                        sink: task_tx.clone(),
                        timeout_ms: Some(PEER_TIMEOUT_MS),
                        msg_id: None,
                    })
                    .expect("Node hung up on itself");
            }

            let mut responses: Vec<HashMap<String, Vec<(usize, i64)>>> = Vec::new();
            while responses.len() < placements.len() {
                let result = task_rx.recv().await.expect("Node hung up on itself");
                let mut got_poll_ok = false;

                if let NodeInput::Message(ParsedInput::Reply(r)) = result {
                    if let ChallengePayload::PollOk(p) = r.body {
                        responses.push(p.msgs);
                        got_poll_ok = true;
                    }
                }

                if !got_poll_ok {
                    panic!("Got unexpected reply");
                }
            }

            msgs.extend(responses.into_iter().flat_map(|r| r.into_iter()));
            cmd_tx
                .send(Command::Reply {
                    dest: src,
                    msg_id,
                    msg: ChallengePayload::PollOk(PollOkPayload { msgs }),
                })
                .expect("Node hung up on itself");
        });

        Ok(())
    }

    async fn handle_commit_offsets(
        &mut self,
        src: String,
        msg_id: MessageId,
        mut offsets: HashMap<String, usize>,
    ) -> Result<(), MaelstromError> {
        let msg_tx = self.get_msg_tx();
        if self.is_cluster_member(&src) {
            for (key, offset) in offsets {
                self.topics.commit_offset(key, offset)?;
            }

            let reply = self.reply(src, msg_id, ChallengePayload::CommitOffsetsOk);
            send!(msg_tx, reply, "Egress hung up: {}");

            return Ok(());
        }

        let mut placements = self.placements(offsets.keys().map(|k| k.as_str()));
        if let Some(topics) = placements.remove(&self.node_id) {
            for (key, offset) in topics
                .iter()
                .filter_map(|t| offsets.get(t).map(|o| (t, *o)))
            {
                self.topics.commit_offset(key, offset)?;
            }
        }
        let mut responses = 0;
        let cmd_tx = self.get_command_tx();
        let (task_tx, mut task_rx) =
            unbounded_channel::<NodeInput<ChallengePayload, ChallengeCommand>>();

        tokio::spawn(async move {
            // Forward a commit to the owners of topics from `offsets`, wait on ok responses and reply to the requester.
            for (node, topics) in placements.iter() {
                cmd_tx
                    .send(Command::Rpc {
                        dest: node.clone(),
                        msg: ChallengePayload::CommitOffsets(CommitOffsetsPayload {
                            offsets: topics
                                .iter()
                                .filter_map(|t| offsets.remove(t).map(|o| (t.to_string(), o)))
                                .collect(),
                        }),
                        sink: task_tx.clone(),
                        timeout_ms: Some(PEER_TIMEOUT_MS),
                        msg_id: None,
                    })
                    .expect("Node hung up on itself");
            }

            while responses < placements.len() {
                let result = task_rx.recv().await.expect("Node hung up on itself");
                let mut got_poll_ok = false;

                if let NodeInput::Message(ParsedInput::Reply(r)) = &result {
                    if let ChallengePayload::CommitOffsetsOk = &r.body {
                        responses += 1;
                        got_poll_ok = true;
                    }
                }

                if !got_poll_ok {
                    panic!("Got unexpected reply: {:?}", result);
                }
            }

            cmd_tx
                .send(Command::Reply {
                    dest: src,
                    msg_id,
                    msg: ChallengePayload::CommitOffsetsOk,
                })
                .expect("Node hung up on itself");
        });

        Ok(())
    }

    async fn handle_list_committed_offsets(
        &mut self,
        src: String,
        msg_id: MessageId,
        keys: Box<[String]>,
    ) -> Result<(), MaelstromError> {
        let msg_tx = self.get_msg_tx();
        if self.node_ids.contains(&src) {
            let offsets: HashMap<String, usize> = keys
                .iter()
                .filter_map(|topic_name| {
                    self.topics
                        .topics
                        .get(topic_name)
                        .map(|topic| (topic_name.to_string(), topic.offset))
                })
                .collect();

            let reply = self.reply(
                src,
                msg_id,
                ChallengePayload::ListCommittedOffsetsOk(ListCommittedOffsetsOkPayload { offsets }),
            );
            send!(msg_tx, reply, "Egress hung up: {}");

            return Ok(());
        }

        let mut placements = self.placements(keys.iter().map(|k| k.as_str()));
        let mut offsets: HashMap<String, usize> = placements
            .remove(&self.node_id)
            .unwrap_or_default()
            .iter()
            .filter_map(|topic_name| {
                self.topics
                    .topics
                    .get(topic_name)
                    .map(|topic| (topic_name.to_string(), topic.offset))
            })
            .collect();
        let cmd_tx = self.get_command_tx();
        let (task_tx, mut task_rx) =
            unbounded_channel::<NodeInput<ChallengePayload, ChallengeCommand>>();

        tokio::spawn(async move {
            for node in placements.keys() {
                cmd_tx
                    .send(Command::Rpc {
                        dest: node.clone(),
                        msg: ChallengePayload::ListCommittedOffsets(ListCommittedOffsetsPayload {
                            keys: placements.get(node).unwrap().clone(),
                        }),
                        sink: task_tx.clone(),
                        timeout_ms: Some(PEER_TIMEOUT_MS),
                        msg_id: None,
                    })
                    .expect("Node hung up on itself");
            }

            let mut responses: Vec<HashMap<String, usize>> = Vec::new();
            while responses.len() < placements.len() {
                let result = task_rx.recv().await.expect("Node hung up on itself");
                let mut got_list_ok = false;

                if let NodeInput::Message(ParsedInput::Reply(r)) = result {
                    if let ChallengePayload::ListCommittedOffsetsOk(co) = r.body {
                        responses.push(co.offsets);
                        got_list_ok = true;
                    }
                }

                if !got_list_ok {
                    panic!("Got unexpected reply");
                }
            }
            offsets.extend(responses.into_iter().flat_map(|r| r.into_iter()));

            cmd_tx
                .send(Command::Reply {
                    dest: src,
                    msg_id,
                    msg: ChallengePayload::ListCommittedOffsetsOk(ListCommittedOffsetsOkPayload {
                        offsets,
                    }),
                })
                .expect("Node hung up on itself");
        });

        Ok(())
    }
}

impl NodeDelegate for LogDelegate {
    type MessageType = ChallengePayload;
    type CommandType = ChallengeCommand;

    fn init(
        node_id: String,
        node_ids: impl IntoIterator<Item = String>,
        msg_tx: UnboundedSender<Message<Self::MessageType>>,
        msg_rx: UnboundedReceiver<ParsedInput<Self::MessageType>>,
    ) -> Self {
        let (cmd_tx, cmd_rx) = unbounded_channel::<Command<Self::MessageType, Self::CommandType>>();

        Self {
            msg_tx,
            msg_rx: Some(msg_rx),
            cmd_tx,
            cmd_rx: Some(cmd_rx),
            node_id,
            node_ids: node_ids.into_iter().collect(),
            topics: Topics::new(),
            msg_id: 1.into(),
            reply_records: HashMap::new(),
        }
    }

    #[allow(clippy::manual_async_fn)]
    fn handle_rpc(
        &mut self,
        message: ParsedRpc<Self::MessageType>,
    ) -> impl Future<Output = Result<(), MaelstromError>> + Send {
        async move {
            match message.body {
                ChallengePayload::Send(s) => {
                    self.handle_send(message.src, message.msg_id, s.key, s.msg)
                        .await?;
                }
                ChallengePayload::Poll(p) => {
                    self.handle_poll(message.src, message.msg_id, p.offsets)
                        .await?;
                }
                ChallengePayload::CommitOffsets(o) => {
                    self.handle_commit_offsets(message.src, message.msg_id, o.offsets)
                        .await?;
                }
                ChallengePayload::ListCommittedOffsets(o) => {
                    self.handle_list_committed_offsets(
                        message.src,
                        message.msg_id,
                        o.keys.into_boxed_slice(),
                    )
                    .await?;
                }

                _ => panic!("Unexpected message type: {:?}", message),
            };

            Ok(())
        }
    }

    #[allow(clippy::manual_async_fn)]
    fn handle_message(
        &mut self,
        _: ParsedMessage<Self::MessageType>,
    ) -> impl Future<Output = Result<(), MaelstromError>> + Send {
        async move { todo!() }
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

    fn get_reply_records_mut(
        &mut self,
    ) -> &mut HashMap<
        MessageId,
        maelstrom_csp2::node::ReplyRecord<Self::MessageType, Self::CommandType>,
    > {
        &mut self.reply_records
    }

    #[allow(clippy::manual_async_fn)]
    fn handle_custom_command(
        &mut self,
        _: Self::CommandType,
    ) -> impl futures::prelude::Future<Output = Result<(), MaelstromError>> + Send {
        async { Ok(()) }
    }

    fn get_node_id(&self) -> &str {
        &self.node_id
    }

    fn get_command_rx(
        &mut self,
    ) -> UnboundedReceiver<maelstrom_csp2::node::Command<Self::MessageType, Self::CommandType>>
    {
        self.cmd_rx.take().unwrap()
    }

    fn get_command_tx(
        &self,
    ) -> UnboundedSender<maelstrom_csp2::node::Command<Self::MessageType, Self::CommandType>> {
        self.cmd_tx.clone()
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let (node, mut ingress, mut egress) = maelstrom_csp2::get_node_and_io::<
        ChallengePayload,
        LogDelegate,
    >(BufReader::new(stdin()), stdout());

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

#![allow(unused)]

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    future::Future,
    hash::Hash,
    marker::PhantomData,
};

use log::{error, info, warn};
use maelstrom_csp::{
    get_node_and_io,
    message::{
        self, ErrorMessagePayload, InitMessagePayload, LocalMessage, Message, MessageBody,
        MessageId, MessagePayload,
    },
    node::NodeDelegate,
    rpc_error::MaelstromError,
    send,
};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{stdin, stdout, BufReader},
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};

use message_macro::maelstrom_message;

const BROADCAST_TIMEOUT_MS: u64 = 5000;

#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
struct BroadcastPayload {
    message: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct AllMessagePayload {
    messages: Vec<i64>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct ReadOkPayload {
    messages: Vec<i64>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TopologyPayload {
    topology: HashMap<String, Vec<String>>,
}

#[maelstrom_message]
#[derive(Serialize, Deserialize, Clone, Debug)]
enum ChallengePayload {
    #[serde(rename = "broadcast")]
    Broadcast(BroadcastPayload),

    #[serde(rename = "broadcast_ok")]
    BroadcastOk,

    #[serde(rename = "read")]
    Read,

    #[serde(rename = "read_ok")]
    ReadOk(ReadOkPayload),

    #[serde(rename = "topology")]
    Topology(TopologyPayload),

    #[serde(rename = "topology_ok")]
    TopologyOk,

    #[serde(rename = "state")]
    AllMessages(AllMessagePayload),

    #[serde(rename = "state_ok")]
    AllMessagesOk,
}

type ChallengeMessage = Message<ChallengePayload>;

trait NeighborHealth {}
struct Healthy {}
struct Unhealthy {}

impl NeighborHealth for Healthy {}
impl NeighborHealth for Unhealthy {}

trait StateManager {
    fn accept_broadcast(
        self: Box<Self>,
        msg: &BroadcastPayload,
    ) -> (Box<dyn StateManager + Send + 'static>, Command);

    fn accept_timeout(
        self: Box<Self>,
        msg_id: MessageId,
    ) -> (Box<dyn StateManager + Send + 'static>, Command);

    fn accept_broadcast_timeout(
        self: Box<Self>,
        msg_id: MessageId,
    ) -> (Box<dyn StateManager + Send + 'static>, Command);

    fn accept_state_timeout(
        self: Box<Self>,
        msg_id: MessageId,
    ) -> (Box<dyn StateManager + Send + 'static>, Command);

    fn accept_broadcast_ok(
        self: Box<Self>,
        msg_id: MessageId,
    ) -> (Box<dyn StateManager + Send + 'static>, Command);

    fn accept_state(
        self: Box<Self>,
        msg: &AllMessagePayload,
    ) -> (Box<dyn StateManager + Send + 'static>, Command);

    fn accept_state_ok(
        self: Box<Self>,
        msg_id: MessageId,
    ) -> (Box<dyn StateManager + Send + 'static>, Command);

    fn acked(&self, message: i64) -> bool;
}

struct NeighborState<S: NeighborHealth> {
    pending: BTreeMap<MessageId, i64>,
    pending_all_msgs: Option<Vec<i64>>,
    retry_contents: Option<(MessageId, i64)>,
    backlog: HashSet<i64>,
    acked: HashSet<i64>,
    // Message IDs are now unique per destination, not globally for this node, and they're reused for retries. The
    // Maelstrom spec says both that they "should" be unique per sending node, but also that there are no constraints
    // on message body structure, so I interpret that to mean that this relaxation is admissible. It could also be
    // interpreted as requiring the message id field to be unique if it exists, but it doesn't seem to cause any
    // problems.
    msg_id: MessageId,

    _marker: PhantomData<S>,
}

#[derive(Debug)]
enum Command {
    Broadcast(MessageId, i64),
    AllMessages(MessageId, Vec<i64>),
    RestEasy,
}

impl<S: NeighborHealth + Send + 'static> NeighborState<S>
where
    NeighborState<S>: StateManager,
{
    fn new() -> Self {
        Self {
            pending: BTreeMap::new(),
            pending_all_msgs: None,
            retry_contents: None,
            backlog: HashSet::new(),
            acked: HashSet::new(),
            msg_id: 0.into(),
            _marker: PhantomData,
        }
    }

    fn next_msg_id(&mut self) -> MessageId {
        self.msg_id += 1;
        self.msg_id
    }

    fn accept_timeout_impl(
        mut self: Box<Self>,
        msg_id: MessageId,
    ) -> (Box<dyn StateManager + Send + 'static>, Command) {
        if self.pending.contains_key(&msg_id)
            || (self.retry_contents.is_some() && self.retry_contents.as_ref().unwrap().0 == msg_id)
        {
            self.accept_broadcast_timeout(msg_id)
        } else if self.pending_all_msgs.is_some() {
            self.accept_state_timeout(msg_id)
        } else {
            // We're not cancelling timeouts, so we'll get a spurious one after successful ack from the other node.
            info!("Ignoring stale timeout for msg_id: {:?}", msg_id);
            (self, Command::RestEasy)
        }
    }
}

impl StateManager for NeighborState<Healthy> {
    fn accept_broadcast(
        mut self: Box<Self>,
        msg: &BroadcastPayload,
    ) -> (Box<dyn StateManager + Send + 'static>, Command) {
        // Got a broadcast from someone else, gossip about it with this node if it hasn't seen it before.
        if self.acked(msg.message) {
            return (self, Command::RestEasy);
        }

        let msg_id = self.next_msg_id();
        self.pending.insert(msg_id, msg.message);

        (self, Command::Broadcast(msg_id, msg.message))
    }

    fn accept_timeout(
        self: Box<Self>,
        msg_id: MessageId,
    ) -> (Box<dyn StateManager + Send + 'static>, Command) {
        self.accept_timeout_impl(msg_id)
    }

    fn accept_broadcast_timeout(
        mut self: Box<Self>,
        msg_id: MessageId,
    ) -> (Box<dyn StateManager + Send + 'static>, Command) {
        // Our last braodcast timed out. Transition to unhealthy, put everything not acked in the backlog, resend as a
        // heartbeat.
        let message = self.pending.remove(&msg_id).unwrap();
        let mut backlog: HashSet<i64> = HashSet::new();
        backlog.extend(self.pending.values().into_iter());

        // A possible ordering of incoming messages is that we get this timeout after having sent a state message,
        // so make sure to save this too.
        backlog.extend(self.pending_all_msgs.unwrap_or_default().into_iter());

        let next_state: NeighborState<Unhealthy> = NeighborState {
            pending: BTreeMap::new(),
            pending_all_msgs: None,
            retry_contents: Some((msg_id, message)),
            backlog,
            acked: self.acked,
            msg_id: self.msg_id,
            _marker: PhantomData,
        };

        (Box::new(next_state), Command::Broadcast(msg_id, message))
    }

    fn accept_state_timeout(
        mut self: Box<Self>,
        msg: MessageId,
    ) -> (Box<dyn StateManager + Send + 'static>, Command) {
        // This node was briefly healthy, but our state message timed out. Transition to unhealthy and pick out a value
        // to send as a heartbeat.
        let mut all_msg_contents = self.pending_all_msgs.take().unwrap();
        let mut backlog: HashSet<i64> = HashSet::new();

        let msg_id: MessageId;
        let message: i64;
        if !self.pending.is_empty() {
            // Nominate one of these pending messages to be the heartbeat while this node is unhealthy.
            (msg_id, message) = self.pending.pop_first().unwrap();

            // In case broadcasts were sent while this node was healthy but waiting to get a AllMessagesOk response.
            backlog.extend(self.pending.values().into_iter());
        } else {
            // Nominate one of the values from the state we tried to send out as the heartbeat.
            msg_id = self.next_msg_id();
            message = all_msg_contents
                .pop()
                .expect("State we sent out was empty, so it shouldn't have been sent");
        }
        backlog.extend(all_msg_contents.into_iter());

        let next_state: NeighborState<Unhealthy> = NeighborState {
            pending: BTreeMap::new(),
            pending_all_msgs: None,
            retry_contents: Some((msg_id, message)),
            backlog,
            acked: self.acked,
            msg_id: self.msg_id,
            _marker: PhantomData,
        };

        (Box::new(next_state), Command::Broadcast(msg_id, message))
    }

    fn accept_broadcast_ok(
        mut self: Box<Self>,
        msg_id: MessageId,
    ) -> (Box<dyn StateManager + Send + 'static>, Command) {
        // Our broadcast got acked, make note of that and move on.
        let message = self.pending.remove(&msg_id).unwrap();
        self.acked.insert(message);

        (self, Command::RestEasy)
    }

    fn accept_state(
        mut self: Box<Self>,
        msg: &AllMessagePayload,
    ) -> (Box<dyn StateManager + Send + 'static>, Command) {
        // We got a state message from someone else, gossip about it to this node if there's anything it hasn't seen.
        if msg
            .messages
            .iter()
            .filter(|m| !self.acked(**m))
            .collect::<Vec<_>>()
            .len()
            == 0
        {
            return (self, Command::RestEasy);
        }

        let msg_id = self.next_msg_id();
        self.pending_all_msgs.insert(msg.messages.clone());

        (self, Command::AllMessages(msg_id, msg.messages.clone()))
    }

    fn accept_state_ok(
        mut self: Box<Self>,
        msg_id: MessageId,
    ) -> (Box<dyn StateManager + Send + 'static>, Command) {
        // Our state message got acked, make note of that and move on.
        let messages = self.pending_all_msgs.take().unwrap();
        self.acked.extend(messages.into_iter());

        (self, Command::RestEasy)
    }

    fn acked(&self, message: i64) -> bool {
        self.acked.contains(&message)
    }
}

impl StateManager for NeighborState<Unhealthy> {
    fn accept_broadcast(
        mut self: Box<Self>,
        msg: &BroadcastPayload,
    ) -> (Box<dyn StateManager + Send + 'static>, Command) {
        // Got a broadcast from someone else while this node was unhealthy. Add it to the backlog and move on.
        if self.acked(msg.message) {
            return (self, Command::RestEasy);
        }

        self.backlog.insert(msg.message);

        (self, Command::RestEasy)
    }

    fn accept_timeout(
        self: Box<Self>,
        msg_id: MessageId,
    ) -> (Box<dyn StateManager + Send + 'static>, Command) {
        self.accept_timeout_impl(msg_id)
    }

    fn accept_broadcast_timeout(
        mut self: Box<Self>,
        msg_id: MessageId,
    ) -> (Box<dyn StateManager + Send + 'static>, Command) {
        // Our heartbeat to this node timed out. Resend.
        let (retry_msg_id, retry_message) = self.retry_contents.as_ref().unwrap();
        let command = if msg_id == *retry_msg_id {
            Command::Broadcast(msg_id, *retry_message)
        } else {
            Command::RestEasy
        };

        (self, command)
    }

    fn accept_state_timeout(
        self: Box<Self>,
        msg_id: MessageId,
    ) -> (Box<dyn StateManager + Send + 'static>, Command) {
        // An unlikely possibility, but if a node has transitioned from unhealthy to healthy, sent a state message,
        // then sent a broadcast very soon after while it was healthy but the state response was pending, those timeout
        // tasks could conceivably be interleaved on different executor threads in such a way that the broadcast timeout
        // gets acked first and transitions this node back to unhealthy before we get notified that the state message
        // timed out. In any case, we saved the pending state to the backlog during that transition, so do nothing here.
        (self, Command::RestEasy)
    }

    fn accept_broadcast_ok(
        mut self: Box<Self>,
        msg_id: MessageId,
    ) -> (Box<dyn StateManager + Send + 'static>, Command) {
        // Our heartbeat was acked! Transition back to healthy and send a state catchup message out.
        let all_msg_contents: Vec<i64> = self.backlog.into_iter().collect();
        let command = if all_msg_contents.len() > 0 {
            self.pending_all_msgs.insert(all_msg_contents.clone());
            Command::AllMessages(msg_id, all_msg_contents)
        } else {
            Command::RestEasy
        };

        let new: NeighborState<Healthy> = NeighborState {
            pending: BTreeMap::new(),
            pending_all_msgs: self.pending_all_msgs,
            retry_contents: None,
            backlog: HashSet::new(),
            acked: self.acked,
            msg_id: self.msg_id,
            _marker: PhantomData,
        };

        (Box::new(new), command)
    }

    fn accept_state(
        mut self: Box<Self>,
        msg: &AllMessagePayload,
    ) -> (Box<dyn StateManager + Send + 'static>, Command) {
        // We got a state update from someone else while this node was unhealthy. Add it to the backlog and move on, the
        // heartbeat we're sending will take care of things when this node comes back online.
        if msg
            .messages
            .iter()
            .filter(|m| !self.acked(**m))
            .collect::<Vec<_>>()
            .len()
            == 0
        {
            return (self, Command::RestEasy);
        }
        self.backlog.extend(msg.messages.clone().into_iter());

        (self, Command::RestEasy)
    }

    fn accept_state_ok(
        self: Box<Self>,
        msg_id: MessageId,
    ) -> (Box<dyn StateManager + Send + 'static>, Command) {
        // Basically the same situation as accept_state_timeout: if we send out a state message and then a broadcast in
        // quick succession, and the state response just barely squeaks through before the timeout but the broadcast
        // times out right after, an interleaving where the broadcast timeout notification appears on our queue first is
        // conceivable. We'll take advantage of the idempotency of broadcast/state RPCs here and the unlikelihood of
        // this event to simplify our bookkeeping and just stay unhealthy and keep all those messages in the backlog, to
        // be resent when this node becomes healthy again.
        (self, Command::RestEasy)
    }

    fn acked(&self, message: i64) -> bool {
        self.acked.contains(&message)
    }
}

struct Neighbor {
    neighbor_state: Option<Box<dyn StateManager + Send>>,
    node_id: String,
}

impl Neighbor {
    fn new(node_id: impl AsRef<str>) -> Self {
        let state_machine: NeighborState<Healthy> = NeighborState::new();

        Self {
            node_id: node_id.as_ref().into(),
            neighbor_state: Some(Box::new(state_machine)),
        }
    }

    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn accept_message(&mut self, msg: &ChallengeMessage) -> Command {
        let mut neighbor_state = self.neighbor_state.take().unwrap();

        let (state, command) = match &msg.body.contents {
            ChallengePayload::Broadcast(b) => neighbor_state.accept_broadcast(b),
            ChallengePayload::BroadcastOk => {
                neighbor_state.accept_broadcast_ok(msg.body.in_reply_to.unwrap())
            }
            ChallengePayload::AllMessages(s) => neighbor_state.accept_state(s),
            ChallengePayload::AllMessagesOk => {
                neighbor_state.accept_state_ok(msg.body.in_reply_to.unwrap())
            }

            // We shouldn't be getting any other variant here.
            _ => {
                panic!("Unexpected message in handle_reply: {:?}", msg);
            }
        };
        self.neighbor_state.insert(state);

        command
    }

    fn accept_timeout(&mut self, msg_id: MessageId) -> Command {
        let mut neighbor_state = self.neighbor_state.take().unwrap();
        let (state, command) = neighbor_state.accept_timeout(msg_id);
        self.neighbor_state.insert(state);

        command
    }

    fn acked(&self, message: i64) -> bool {
        self.neighbor_state.as_ref().unwrap().acked(message)
    }
}

struct Neighbors {
    neighbors: HashMap<String, Neighbor>,
}

impl Neighbors {
    fn new(neighbors: impl Iterator<Item = String>) -> Self {
        Self {
            neighbors: neighbors.map(|n| (n.clone(), Neighbor::new(n))).collect(),
        }
    }

    fn iter_mut(&mut self) -> impl Iterator<Item = &mut Neighbor> {
        self.neighbors.values_mut()
    }

    fn set_neighbors(&mut self, neighbors: &[String]) {
        let new_neighbor_ids: HashSet<String> = neighbors.iter().map(|s| s.clone()).collect();
        let current_neighbor_ids: HashSet<String> =
            self.neighbors.keys().map(|s| s.into()).collect();
        for n in new_neighbor_ids.iter() {
            if !current_neighbor_ids.contains(n) {
                self.neighbors.insert(n.clone(), Neighbor::new(n));
            }
        }

        let to_remove: Vec<String> = self
            .neighbors
            .keys()
            .filter(|n| !new_neighbor_ids.contains(*n))
            .map(|s| s.into())
            .collect();
        to_remove.iter().map(|n| self.neighbors.remove(n));
    }

    fn node_mut(&mut self, name: &str) -> Option<&mut Neighbor> {
        self.neighbors.get_mut(name)
    }
}

struct BroadcastDelegate {
    msg_tx: UnboundedSender<ChallengeMessage>,
    msg_rx: Option<UnboundedReceiver<ChallengeMessage>>,
    self_tx: UnboundedSender<ChallengeMessage>,
    outstanding_replies: HashSet<(MessageId, String)>,

    node_id: String,
    msg_id: MessageId,

    msg_store: HashSet<i64>,
    neighbors: Neighbors,
}

impl BroadcastDelegate {
    fn handle_topology(
        &mut self,
        msg: &TopologyPayload,
    ) -> Result<ChallengePayload, MaelstromError> {
        let neighbors = msg
            .topology
            .get(&self.node_id)
            .ok_or(MaelstromError::Other(format!(
                "Didn't find self ({}) in neighbors: {:?}",
                self.node_id, msg
            )))?;
        self.neighbors.set_neighbors(neighbors.as_slice());

        Ok(ChallengePayload::TopologyOk)
    }

    fn run_state_machines(&mut self, msg: &ChallengeMessage) -> Vec<(String, Command)> {
        self.neighbors
            .iter_mut()
            .map(|n| (n.node_id().into(), n.accept_message(msg)))
            .collect()
    }

    async fn run_command(&mut self, dest: String, command: Command) -> Result<(), MaelstromError> {
        match command {
            Command::Broadcast(msg_id, message) => {
                self.rpc_with_timeout_with_msg_id(
                    dest,
                    ChallengePayload::Broadcast(BroadcastPayload { message }),
                    BROADCAST_TIMEOUT_MS,
                    msg_id,
                )
                .await?;
            }
            Command::AllMessages(msg_id, messages) => {
                self.rpc_with_timeout_with_msg_id(
                    dest,
                    ChallengePayload::AllMessages(AllMessagePayload { messages }),
                    BROADCAST_TIMEOUT_MS,
                    msg_id,
                )
                .await?;
            }
            Command::RestEasy => (),
        }

        Ok(())
    }
}

impl NodeDelegate for BroadcastDelegate {
    type MessageType = ChallengePayload;
    fn init(
        node_id: impl AsRef<str>,
        node_ids: impl AsRef<Vec<String>>,
        msg_tx: UnboundedSender<ChallengeMessage>,
        msg_rx: UnboundedReceiver<ChallengeMessage>,
        self_tx: UnboundedSender<ChallengeMessage>,
    ) -> Self {
        Self {
            msg_tx,
            msg_rx: Some(msg_rx),
            self_tx,
            outstanding_replies: HashSet::new(),
            node_id: node_id.as_ref().into(),
            msg_id: 0.into(),
            msg_store: HashSet::new(),
            neighbors: Neighbors::new(
                node_ids
                    .as_ref()
                    .iter()
                    .filter(|n| n.as_str() != node_id.as_ref())
                    .map(|s| s.clone()),
            ),
        }
    }

    fn get_outstanding_replies(&self) -> &HashSet<(MessageId, String)> {
        &self.outstanding_replies
    }

    fn get_outstanding_replies_mut(&mut self) -> &mut HashSet<(MessageId, String)> {
        &mut self.outstanding_replies
    }

    fn handle_reply(
        &mut self,
        reply: ChallengeMessage,
    ) -> impl Future<Output = Result<(), MaelstromError>> + Send {
        async move {
            let mut defer_to_state_machine = false;
            match &reply.body.contents {
                // Let the state machines for remote nodes handle this.
                ChallengePayload::BroadcastOk => {
                    defer_to_state_machine = true;
                }
                ChallengePayload::AllMessagesOk => {
                    defer_to_state_machine = true;
                }

                ChallengePayload::ReadOk(r) => {
                    // We're using a push model on failure recovery, so we shouldn't receive this message from a peer.
                    self.msg_store.extend(r.messages.iter());
                }
                ChallengePayload::Error(e) => {
                    // Bummer. Log it and move on.
                    warn!("Exception handling {:?}: {}", reply, e.text);
                }

                // Nops.
                ChallengePayload::TopologyOk => (),
                ChallengePayload::Empty => (),

                // We shouldn't be getting any other variant here.
                _ => {
                    return Err(MaelstromError::Other(format!(
                        "Unexpected message in handle_reply: {:?}",
                        reply
                    )));
                }
            }

            if defer_to_state_machine {
                let msg_id = reply.body.in_reply_to.unwrap();
                let node_id = reply.src.as_ref().unwrap().to_string();
                let command = {
                    let mut node =
                        self.neighbors
                            .node_mut(&node_id)
                            .ok_or(MaelstromError::Other(format!(
                                "Couldn't find node in neighbors: {}",
                                node_id
                            )))?;

                    node.accept_message(&reply)
                };

                self.run_command(node_id.clone(), command).await?;
            }

            Ok(())
        }
    }

    fn handle_message(
        &mut self,
        message: ChallengeMessage,
    ) -> impl Future<Output = Result<(), MaelstromError>> + Send {
        async move {
            let response: ChallengePayload;
            let mut defer_to_state_machine = false;
            match &message.body.contents {
                ChallengePayload::Broadcast(b) => {
                    if !self.msg_store.contains(&b.message) {
                        self.msg_store.insert(b.message);
                        defer_to_state_machine = true;
                    }

                    send!(
                        self.get_msg_tx(),
                        self.reply(message.clone(), ChallengePayload::BroadcastOk)?,
                        "Egress hung up: {}"
                    );
                }
                ChallengePayload::AllMessages(s) => {
                    if s.messages
                        .iter()
                        .filter(|m| !self.msg_store.contains(*m))
                        .collect::<Vec<_>>()
                        .len()
                        > 0
                    {
                        // Rely on idempotence to be lazy here and not filter out the messages we've already seen.
                        defer_to_state_machine = true;
                    }

                    send!(
                        self.get_msg_tx(),
                        self.reply(message.clone(), ChallengePayload::AllMessagesOk)?,
                        "Egress hung up: {}"
                    );
                }

                ChallengePayload::Topology(t) => {
                    let response = self.handle_topology(&t)?;
                    send!(
                        self.get_msg_tx(),
                        self.reply(message.clone(), response)?,
                        "Egress hung up: {}"
                    );
                }
                ChallengePayload::Read => {
                    let reply = self.reply(
                        message.clone(),
                        ChallengePayload::ReadOk(ReadOkPayload {
                            messages: self.msg_store.iter().map(|m| *m).collect(),
                        }),
                    )?;
                    send!(self.get_msg_tx(), reply, "Egress hung up: {}");
                }

                ChallengePayload::Empty => (),

                _ => {
                    return Err(MaelstromError::Other(format!(
                        "Got unexpected message in handle_message: {:?}",
                        message
                    )));
                }
            };

            if defer_to_state_machine {
                let commands = self.run_state_machines(&message);
                for (node_id, command) in commands.into_iter() {
                    if &node_id != message.src.as_ref().unwrap() {
                        // Don't talk back to the node we just heard from.
                        self.run_command(node_id, command).await?;
                    }
                }
            }

            Ok(())
        }
    }

    fn handle_local_message(
        &mut self,
        msg: LocalMessage,
    ) -> impl Future<Output = Result<(), MaelstromError>> + Send {
        async move {
            let (msg_id, node_id) = match &msg {
                LocalMessage::Cancel(msg_id, node_id) => {
                    if !self.outstanding_replies.remove(&(*msg_id, node_id.clone())) {
                        info!(
                            "Ignoring stale timeout for (msg_id, node_id) ({}, {})",
                            msg_id, node_id
                        );
                    }
                    (msg_id, node_id)
                }
            };

            let command = {
                let mut node = self
                    .neighbors
                    .node_mut(&node_id)
                    .ok_or(MaelstromError::Other(format!(
                        "Couldn't find node in neighbors: {}",
                        node_id
                    )))?;

                node.accept_timeout(*msg_id)
            };
            self.run_command(node_id.clone(), command).await?;

            Ok(())
        }
    }

    fn get_msg_id(&mut self) -> &mut MessageId {
        &mut self.msg_id
    }

    fn get_msg_rx(&mut self) -> UnboundedReceiver<ChallengeMessage> {
        self.msg_rx.take().unwrap()
    }

    fn get_msg_tx(&self) -> UnboundedSender<ChallengeMessage> {
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
        get_node_and_io::<ChallengePayload, BroadcastDelegate>(BufReader::new(stdin()), stdout());

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

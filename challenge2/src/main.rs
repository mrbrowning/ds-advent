use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    future::Future,
    marker::PhantomData,
    ops::Deref,
    time::Duration,
};

use log::{error, info, warn};
use maelstrom_csp::{
    get_node_and_io,
    message::{
        ErrorMessagePayload, InitMessagePayload, LocalMessage, LocalMessageType, Message,
        MessageBody, MessageId, MessagePayload,
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

const BROADCAST_TIMEOUT_MS: u64 = 1000;
const BRANCHING_FACTOR: u64 = 4;
const BUFFER_TIMEOUT_MS: u64 = 500;

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

    #[serde(rename = "peer_broadcast")]
    PeerBroadcast(AllMessagePayload),

    #[serde(rename = "peer_broadcast_ok")]
    PeerBroadcastOk,

    #[serde(rename = "read")]
    Read,

    #[serde(rename = "read_ok")]
    ReadOk(ReadOkPayload),

    #[serde(rename = "topology")]
    Topology(TopologyPayload),

    #[serde(rename = "topology_ok")]
    TopologyOk,

    #[serde(rename = "all_messages")]
    AllMessages(AllMessagePayload),

    #[serde(rename = "all_messages_ok")]
    AllMessagesOk,
}

type ChallengeMessage = Message<ChallengePayload>;

// Implement neighbors as state machines with two states, healthy and unhealthy. While it would simplify the logic of
// the accept methods and would move more of the burden of guaranteeing correctness into the type system if we used more
// granular states, e.g. a PendingBroadcastResponse state, this would either force us to serialize every pair of
// requests and responses (so no multiple outstanding messages) or require a combinatorial explosion of states modeling
// all possible combinations of outstanding messages up to some arbitrary bound, neither of which is practical.
trait NeighborHealth {}

#[derive(Debug)]
struct Healthy {}

#[derive(Debug)]
struct Unhealthy {}

impl NeighborHealth for Healthy {}
impl NeighborHealth for Unhealthy {}

trait StateManager: std::fmt::Debug {
    fn accept_broadcast(
        self: Box<Self>,
        msg: &BroadcastPayload,
    ) -> (Box<dyn StateManager + Send + 'static>, Command);

    fn accept_peer_broadcast(
        self: Box<Self>,
        msg: &AllMessagePayload,
        is_from_me: bool,
    ) -> (Box<dyn StateManager + Send + 'static>, Command);

    fn accept_timeout(
        self: Box<Self>,
        msg_id: MessageId,
    ) -> (Box<dyn StateManager + Send + 'static>, Command);

    fn accept_peer_broadcast_timeout(
        self: Box<Self>,
        msg_id: MessageId,
    ) -> (Box<dyn StateManager + Send + 'static>, Command);

    fn accept_all_messages_timeout(
        self: Box<Self>,
        msg_id: MessageId,
    ) -> (Box<dyn StateManager + Send + 'static>, Command);

    fn accept_broadcast_ok(
        self: Box<Self>,
        msg_id: MessageId,
    ) -> (Box<dyn StateManager + Send + 'static>, Command);

    fn accept_all_messages(
        self: Box<Self>,
        msg: &AllMessagePayload,
        is_from_me: bool,
    ) -> (Box<dyn StateManager + Send + 'static>, Command);

    fn accept_all_messages_ok(
        self: Box<Self>,
        msg_id: MessageId,
    ) -> (Box<dyn StateManager + Send + 'static>, Command);

    fn send_anyway(&mut self, msg_id: MessageId) -> Command;

    fn acked(&self, message: i64) -> bool;
}

#[derive(Debug)]
enum Command {
    Broadcast(MessageId, Vec<i64>),
    AwaitNext(MessageId),
    AllMessages(MessageId, Vec<i64>),
    RestEasy,
}

#[derive(Debug)]
struct NeighborState<S: NeighborHealth> {
    buffered: Option<(MessageId, i64)>,
    pending: BTreeMap<MessageId, Vec<i64>>,
    pending_all_msgs: Option<Vec<i64>>,
    retry_contents: Option<(MessageId, Vec<i64>)>,
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

impl<S: NeighborHealth + Send + std::fmt::Debug + 'static> NeighborState<S>
where
    NeighborState<S>: StateManager,
{
    fn new() -> Self {
        Self {
            buffered: None,
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
        self: Box<Self>,
        msg_id: MessageId,
    ) -> (Box<dyn StateManager + Send + 'static>, Command) {
        if self.pending.contains_key(&msg_id)
            || (self.retry_contents.is_some() && self.retry_contents.as_ref().unwrap().0 == msg_id)
        {
            self.accept_peer_broadcast_timeout(msg_id)
        } else if self.pending_all_msgs.is_some() {
            self.accept_all_messages_timeout(msg_id)
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

        if let Some((msg_id, buffered)) = self.buffered {
            let messages = vec![buffered, msg.message];
            self.pending.insert(msg_id, messages.clone());
            self.buffered.take();

            return (self, Command::Broadcast(msg_id, messages));
        }
        let msg_id = self.next_msg_id();
        let _ = self.buffered.insert((msg_id, msg.message));
        info!(
            "Stashing away buffered message with id {}: {:?}",
            msg_id, msg.message
        );

        (self, Command::AwaitNext(msg_id))
    }

    fn accept_peer_broadcast(
        mut self: Box<Self>,
        msg: &AllMessagePayload,
        is_from_me: bool,
    ) -> (Box<dyn StateManager + Send + 'static>, Command) {
        if is_from_me {
            self.acked.extend(msg.messages.iter());
            return (self, Command::RestEasy);
        }

        // Got a broadcast from someone else, gossip about it with this node if it hasn't seen it before.
        if msg.messages.iter().all(|m| self.acked(*m)) {
            return (self, Command::RestEasy);
        }

        let msg_id = self.next_msg_id();
        self.pending.insert(msg_id, msg.messages.clone());

        (self, Command::Broadcast(msg_id, msg.messages.clone()))
    }

    fn accept_timeout(
        self: Box<Self>,
        msg_id: MessageId,
    ) -> (Box<dyn StateManager + Send + 'static>, Command) {
        self.accept_timeout_impl(msg_id)
    }

    fn accept_peer_broadcast_timeout(
        mut self: Box<Self>,
        msg_id: MessageId,
    ) -> (Box<dyn StateManager + Send + 'static>, Command) {
        // Our last broadcast timed out. Transition to unhealthy, put everything not acked in the backlog, resend as a
        // heartbeat.
        let messages = self.pending.remove(&msg_id).unwrap();
        let mut backlog: HashSet<i64> = HashSet::new();
        backlog.extend(self.pending.values().flatten());
        if let Some((_, buffered)) = self.buffered.take() {
            backlog.insert(buffered);
        }

        // A possible ordering of incoming messages is that we get this timeout after having sent a state message,
        // so make sure to save this too.
        backlog.extend(self.pending_all_msgs.unwrap_or_default());

        let next_state: NeighborState<Unhealthy> = NeighborState {
            buffered: None,
            pending: BTreeMap::new(),
            pending_all_msgs: None,
            retry_contents: Some((msg_id, messages.clone())),
            backlog,
            acked: self.acked,
            msg_id: self.msg_id,
            _marker: PhantomData,
        };

        (Box::new(next_state), Command::Broadcast(msg_id, messages))
    }

    fn accept_all_messages_timeout(
        mut self: Box<Self>,
        _: MessageId,
    ) -> (Box<dyn StateManager + Send + 'static>, Command) {
        // This node was briefly healthy, but our all-messages message timed out. Transition to unhealthy and pick out a
        // value to send as a heartbeat.
        let mut all_msg_contents = self.pending_all_msgs.take().unwrap();
        let mut backlog: HashSet<i64> = HashSet::new();

        let msg_id: MessageId;
        let messages: Vec<i64>;
        if !self.pending.is_empty() {
            // Nominate one of these pending messages to be the heartbeat while this node is unhealthy.
            (msg_id, messages) = self.pending.pop_first().unwrap();

            // In case broadcasts were sent while this node was healthy but waiting to get a AllMessagesOk response.
            backlog.extend(self.pending.values().flatten());
        } else {
            // Nominate one of the values from the all-messages message we tried to send out as the heartbeat.
            msg_id = self.next_msg_id();
            messages = vec![all_msg_contents
                .pop()
                .expect("State we sent out was empty, so it shouldn't have been sent")];
        }

        if let Some((_, buffered)) = self.buffered.take() {
            backlog.insert(buffered);
        }
        backlog.extend(all_msg_contents);

        let next_state: NeighborState<Unhealthy> = NeighborState {
            buffered: None,
            pending: BTreeMap::new(),
            pending_all_msgs: None,
            retry_contents: Some((msg_id, messages.clone())),
            backlog,
            acked: self.acked,
            msg_id: self.msg_id,
            _marker: PhantomData,
        };

        (Box::new(next_state), Command::Broadcast(msg_id, messages))
    }

    fn accept_broadcast_ok(
        mut self: Box<Self>,
        msg_id: MessageId,
    ) -> (Box<dyn StateManager + Send + 'static>, Command) {
        // Our broadcast got acked, make note of that and move on.
        let messages = self.pending.remove(&msg_id).unwrap_or_default();
        self.acked.extend(messages);

        (self, Command::RestEasy)
    }

    fn accept_all_messages(
        mut self: Box<Self>,
        msg: &AllMessagePayload,
        is_from_me: bool,
    ) -> (Box<dyn StateManager + Send + 'static>, Command) {
        if is_from_me {
            self.acked.extend(msg.messages.iter());
            return (self, Command::RestEasy);
        }

        // We got an all-messages from someone else, gossip about it to this node if there's anything it hasn't seen.
        if msg
            .messages
            .iter()
            .filter(|m| !self.acked(**m))
            .collect::<Vec<_>>()
            .is_empty()
        {
            return (self, Command::RestEasy);
        }

        let msg_id = self.next_msg_id();
        let _ = self.pending_all_msgs.insert(msg.messages.clone());

        (self, Command::AllMessages(msg_id, msg.messages.clone()))
    }

    fn accept_all_messages_ok(
        mut self: Box<Self>,
        _: MessageId,
    ) -> (Box<dyn StateManager + Send + 'static>, Command) {
        // Our all-messages message got acked, make note of that and move on.
        let messages = self.pending_all_msgs.take().unwrap_or_default();
        self.acked.extend(messages);

        (self, Command::RestEasy)
    }

    fn send_anyway(&mut self, msg_id: MessageId) -> Command {
        if let Some((buffered_msg_id, buffered)) = self.buffered {
            if msg_id == buffered_msg_id {
                let messages = vec![buffered];
                self.pending.insert(msg_id, messages.clone());
                self.buffered.take();

                return Command::Broadcast(buffered_msg_id, messages);
            }
        }

        Command::RestEasy
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

    fn accept_peer_broadcast(
        mut self: Box<Self>,
        msg: &AllMessagePayload,
        is_from_me: bool,
    ) -> (Box<dyn StateManager + Send + 'static>, Command) {
        if is_from_me {
            self.acked.extend(msg.messages.iter());
            return self.accept_broadcast_ok(0.into());
        }

        // Got a peer broadcast from someone else while this node was unhealthy. Add it to the backlog and move on.
        if msg.messages.iter().all(|m| self.acked(*m)) {
            return (self, Command::RestEasy);
        }

        self.backlog.extend(msg.messages.clone());

        (self, Command::RestEasy)
    }

    fn accept_timeout(
        self: Box<Self>,
        msg_id: MessageId,
    ) -> (Box<dyn StateManager + Send + 'static>, Command) {
        self.accept_timeout_impl(msg_id)
    }

    fn accept_peer_broadcast_timeout(
        self: Box<Self>,
        msg_id: MessageId,
    ) -> (Box<dyn StateManager + Send + 'static>, Command) {
        // Our heartbeat to this node timed out. Resend.
        let (retry_msg_id, retry_messages) = self.retry_contents.as_ref().unwrap();
        let command = if msg_id == *retry_msg_id {
            Command::Broadcast(msg_id, retry_messages.clone())
        } else {
            Command::RestEasy
        };

        (self, command)
    }

    fn accept_all_messages_timeout(
        self: Box<Self>,
        _: MessageId,
    ) -> (Box<dyn StateManager + Send + 'static>, Command) {
        // An unlikely possibility, but if a node has transitioned from unhealthy to healthy, sent an all-messages
        // message, then sent a broadcast very soon after while it was healthy but the state response was pending, those
        // timeout tasks could conceivably be interleaved on different executor threads in such a way that the broadcast
        // timeout gets acked first and transitions this node back to unhealthy before we get notified that the state
        // message timed out. In any case, we saved the pending state to the backlog during that transition, so do
        // nothing here.
        (self, Command::RestEasy)
    }

    fn accept_broadcast_ok(
        mut self: Box<Self>,
        _: MessageId,
    ) -> (Box<dyn StateManager + Send + 'static>, Command) {
        // Our heartbeat was acked! Transition back to healthy and send a all-messages catchup message out.
        let msg_id = self.next_msg_id();
        let mut all_msg_contents: Vec<i64> = self.backlog.into_iter().collect();
        all_msg_contents.extend(self.retry_contents.unwrap_or((0.into(), vec![])).1);

        let command = if !all_msg_contents.is_empty() {
            let _ = self.pending_all_msgs.insert(all_msg_contents.clone());
            Command::AllMessages(msg_id, all_msg_contents)
        } else {
            Command::RestEasy
        };

        let new: NeighborState<Healthy> = NeighborState {
            buffered: None,
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

    fn accept_all_messages(
        mut self: Box<Self>,
        msg: &AllMessagePayload,
        is_from_me: bool,
    ) -> (Box<dyn StateManager + Send + 'static>, Command) {
        if is_from_me {
            self.acked.extend(msg.messages.iter());
            return self.accept_broadcast_ok(0.into());
        }

        // We got an all-messages update from someone else while this node was unhealthy. Add it to the backlog and move
        // on, the heartbeat we're sending will take care of things when this node comes back online.
        if msg
            .messages
            .iter()
            .filter(|m| !self.acked(**m))
            .collect::<Vec<_>>()
            .is_empty()
        {
            return (self, Command::RestEasy);
        }
        self.backlog.extend(msg.messages.clone());

        (self, Command::RestEasy)
    }

    fn accept_all_messages_ok(
        self: Box<Self>,
        _: MessageId,
    ) -> (Box<dyn StateManager + Send + 'static>, Command) {
        // Basically the same situation as accept_all_messages_timeout: if we send out an all-messages message, then a
        // broadcast in quick succession, and the all-messages response just barely squeaks through before the timeout
        // but the broadcast times out right after, an interleaving where the broadcast timeout notification appears on
        // our queue first is conceivable. We'll take advantage of the idempotency of broadcast/all-message RPCs here
        // and the unlikelihood of this event to simplify our bookkeeping and just stay unhealthy and keep all those
        // messages in the backlog, to be resent when this node becomes healthy again.
        (self, Command::RestEasy)
    }

    fn send_anyway(&mut self, _: MessageId) -> Command {
        Command::RestEasy
    }

    fn acked(&self, message: i64) -> bool {
        self.acked.contains(&message)
    }
}

#[derive(Debug)]
struct Neighbor {
    neighbor_state: Option<Box<dyn StateManager + Send>>,
    node_id: String,
}

trait Topology {
    fn neighbors(&self, node_ids: &[String], node_id: &str) -> Vec<String>;
}

impl<T: Deref<Target = dyn Topology + Send>> Topology for T {
    fn neighbors(&self, node_ids: &[String], node_id: &str) -> Vec<String> {
        self.deref().neighbors(node_ids, node_id)
    }
}

struct RingTopology {}

impl Topology for RingTopology {
    fn neighbors(&self, node_ids: &[String], node_id: &str) -> Vec<String> {
        for (i, n) in node_ids.iter().enumerate() {
            if (n.as_str() == node_id) && (i < node_ids.len() - 1) {
                return vec![node_ids[i + 1].clone()];
            } else if (n.as_str() == node_id) && (i == node_ids.len() - 1) {
                return vec![node_ids[0].clone()];
            }
        }

        panic!("{} not in node_ids: {:?}", node_id, node_ids);
    }
}

struct TreeTopology {
    branching_factor: u64,
}

impl TreeTopology {
    fn new(branching_factor: u64) -> Self {
        Self { branching_factor }
    }

    fn spanning_tree(
        num_nodes: usize,
        this_node: usize,
        branching_factor: u64,
    ) -> (Option<usize>, Vec<usize>) {
        let mut queue: VecDeque<usize> = VecDeque::new();
        queue.push_back(0);

        let mut remaining: VecDeque<usize> = VecDeque::new();
        remaining.extend(1..num_nodes);
        let mut parent: Option<usize> = None;
        while !queue.is_empty() {
            let current = queue.pop_front().unwrap();
            let mut children: Vec<usize> = Vec::new();

            for _ in 0..branching_factor as usize {
                if !remaining.is_empty() {
                    let child = remaining.pop_front().unwrap();
                    if child == this_node {
                        parent = Some(current);
                    }

                    children.push(child);
                } else {
                    break;
                }
            }

            if current == this_node {
                return (parent, children);
            } else {
                queue.extend(children);
            }
        }

        panic!("{} not traversed", this_node);
    }
}

impl Topology for TreeTopology {
    fn neighbors(&self, node_ids: &[String], node_id: &str) -> Vec<String> {
        let node_index = node_ids
            .iter()
            .enumerate()
            .find(|n| n.1 == node_id)
            .unwrap()
            .0;
        let (parent, neighbor_indexes) =
            Self::spanning_tree(node_ids.len(), node_index, self.branching_factor);

        let mut neighbors: Vec<String> = neighbor_indexes
            .iter()
            .map(|i| node_ids[*i].clone())
            .collect();
        if let Some(parent) = parent {
            neighbors.push(node_ids[parent].clone());
        }

        neighbors
    }
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
        let neighbor_state = self.neighbor_state.take().unwrap();
        let is_from_me = msg.src.as_ref().unwrap() == self.node_id();

        let (state, command) = match &msg.body.contents {
            ChallengePayload::Broadcast(b) => neighbor_state.accept_broadcast(b),
            ChallengePayload::PeerBroadcast(b) => {
                neighbor_state.accept_peer_broadcast(b, is_from_me)
            }
            ChallengePayload::PeerBroadcastOk => {
                neighbor_state.accept_broadcast_ok(msg.body.in_reply_to.unwrap())
            }
            ChallengePayload::AllMessages(s) => neighbor_state.accept_all_messages(s, is_from_me),
            ChallengePayload::AllMessagesOk => {
                neighbor_state.accept_all_messages_ok(msg.body.in_reply_to.unwrap())
            }

            // We shouldn't be getting any other variant here.
            _ => {
                panic!("Unexpected message in handle_reply: {:?}", msg);
            }
        };
        let _ = self.neighbor_state.insert(state);

        command
    }

    fn accept_timeout(&mut self, msg_id: MessageId) -> Command {
        let neighbor_state = self.neighbor_state.take().unwrap();
        let (state, command) = neighbor_state.accept_timeout(msg_id);
        let _ = self.neighbor_state.insert(state);

        command
    }

    fn send_anyway(&mut self, msg_id: MessageId) -> Command {
        let command = self.neighbor_state.as_mut().unwrap().send_anyway(msg_id);

        command
    }

    #[allow(dead_code)]
    fn acked(&self, message: i64) -> bool {
        self.neighbor_state.as_ref().unwrap().acked(message)
    }
}

struct Neighbors<T: Topology + Send> {
    #[allow(dead_code)]
    topology: T,
    neighbors: HashMap<String, Neighbor>,
}

impl<T: Topology + Send> Neighbors<T> {
    fn new(all_nodes: &[String], node_id: &str, topology: T) -> Self {
        let neighbors: HashMap<String, Neighbor> = topology
            .neighbors(all_nodes, node_id)
            .iter()
            .map(|n| (n.clone(), Neighbor::new(n)))
            .collect();
        info!("My neighbors are: {:?}", neighbors);

        Self {
            topology,
            neighbors,
        }
    }

    fn iter_mut(&mut self) -> impl Iterator<Item = &mut Neighbor> {
        self.neighbors.values_mut()
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

    #[allow(dead_code)]
    node_id: String,
    msg_id: MessageId,

    msg_store: HashSet<i64>,
    neighbors: Neighbors<Box<dyn Topology + Send>>,
}

impl BroadcastDelegate {
    fn handle_topology(&mut self, _: &TopologyPayload) -> Result<ChallengePayload, MaelstromError> {
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
            Command::Broadcast(msg_id, messages) => {
                self.rpc_with_timeout_with_msg_id(
                    dest,
                    ChallengePayload::PeerBroadcast(AllMessagePayload { messages }),
                    BROADCAST_TIMEOUT_MS,
                    msg_id,
                )
                .await?;
            }
            Command::AwaitNext(msg_id) => {
                let msg_tx = self.get_self_tx();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(BUFFER_TIMEOUT_MS)).await;

                    let r = msg_tx.send(Message {
                        src: None,
                        dest: None,
                        body: MessageBody {
                            msg_id: None,
                            in_reply_to: None,
                            local_msg: Some(LocalMessage {
                                msg_id,
                                node_id: dest.clone(),
                                msg_type: LocalMessageType::Other("send_anyway".to_string()),
                            }),
                            contents: ChallengePayload::Empty,
                        },
                    });

                    if let Err(e) = r {
                        panic!("Message egress hung up: {}", e);
                    }
                });
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
                node_ids.as_ref(),
                node_id.as_ref(),
                Box::new(TreeTopology::new(BRANCHING_FACTOR)),
            ),
        }
    }

    fn get_outstanding_replies(&self) -> &HashSet<(MessageId, String)> {
        &self.outstanding_replies
    }

    fn get_outstanding_replies_mut(&mut self) -> &mut HashSet<(MessageId, String)> {
        &mut self.outstanding_replies
    }

    #[allow(clippy::manual_async_fn)]
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
                ChallengePayload::PeerBroadcastOk => {
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
                let node_id = reply.src.as_ref().unwrap().to_string();
                let command = {
                    let node = self
                        .neighbors
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

    #[allow(clippy::manual_async_fn)]
    fn handle_message(
        &mut self,
        message: ChallengeMessage,
    ) -> impl Future<Output = Result<(), MaelstromError>> + Send {
        async move {
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
                ChallengePayload::PeerBroadcast(b) => {
                    if !b.messages.iter().all(|m| self.msg_store.contains(m)) {
                        self.msg_store.extend(b.messages.iter());
                        defer_to_state_machine = true;
                    }

                    send!(
                        self.get_msg_tx(),
                        self.reply(message.clone(), ChallengePayload::PeerBroadcastOk)?,
                        "Egress hung up: {}"
                    );
                }
                ChallengePayload::AllMessages(s) => {
                    if !s
                        .messages
                        .iter()
                        .filter(|m| !self.msg_store.contains(*m))
                        .collect::<Vec<_>>()
                        .is_empty()
                    {
                        // Rely on idempotence to be lazy here and not filter out the messages we've already seen.
                        self.msg_store.extend(s.messages.iter());
                        defer_to_state_machine = true;
                    }

                    send!(
                        self.get_msg_tx(),
                        self.reply(message.clone(), ChallengePayload::AllMessagesOk)?,
                        "Egress hung up: {}"
                    );
                }

                ChallengePayload::Topology(t) => {
                    let response = self.handle_topology(t)?;
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
                            messages: self.msg_store.iter().copied().collect(),
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
                    self.run_command(node_id, command).await?;
                }
            }

            Ok(())
        }
    }

    #[allow(clippy::manual_async_fn)]
    fn handle_local_message(
        &mut self,
        msg: LocalMessage,
    ) -> impl Future<Output = Result<(), MaelstromError>> + Send {
        async move {
            let node = self
                .neighbors
                .node_mut(&msg.node_id)
                .ok_or(MaelstromError::Other(format!(
                    "Couldn't find node in neighbors: {}",
                    msg.node_id
                )))?;
            let command = match &msg.msg_type {
                LocalMessageType::Cancel => {
                    if !self
                        .outstanding_replies
                        .remove(&(msg.msg_id, msg.node_id.clone()))
                    {
                        info!(
                            "Ignoring stale timeout for (msg_id, node_id) ({}, {})",
                            msg.msg_id, msg.node_id
                        );
                    }

                    node.accept_timeout(msg.msg_id)
                }
                LocalMessageType::Other(_) => {
                    info!("Attempting send anyway to {}: {:?}", msg.node_id, msg);
                    node.send_anyway(msg.msg_id)
                }
            };

            self.run_command(msg.node_id, command).await?;

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

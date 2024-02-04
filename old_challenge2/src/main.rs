#![allow(clippy::all)]

use std::{
    sync::{Arc, Mutex},
    time::SystemTime,
};

use futures::FutureExt;
use log::error;
use maelstrom::{
    immediate,
    node::{InitMessageBody, Message, Node},
};

const NODE_ID_BITS: i64 = 10;
const MAX_NODE_ID: u16 = (1 << NODE_ID_BITS) - 1;

const SEQUENCE_ID_BITS: i64 = 12;
const SEQUENCE_ID_CEILING: u64 = 1 << SEQUENCE_ID_BITS;

const HIGH_ORDER_MASK: u64 = !(1 << 63);

macro_rules! unwrap {
    ($value:expr, $msg:expr) => {{
        let val = $value;
        if val.is_err() {
            error!($msg, val.err().unwrap());
            return;
        }

        val.unwrap()
    }};
}

#[derive(Debug)]
struct Generator {
    /// A generator for Snowflake-style 64 bit IDs.
    ///
    /// Bits are allocated as follows:
    ///  0 - 9  (10 bits): sequence index starting at 0 at launch, incrementing for each new id, and rolling over at 4096.
    /// 10 - 21 (12 bits): node ID, equal to this node's index in the node list in the init message sent by maelstrom.
    /// 22 - 62 (41 bits): Number of milliseconds since the Unix epoch, truncated. Not necessarily monotonic, so not
    ///                    suitable for any but demo purposes.
    /// 63 -      (1 bit): reserved, set to zero here.
    node_id: u64,
    sequence_id: u64,
    epoch: SystemTime,
}

impl Generator {
    fn new(node_id: u16, epoch: SystemTime) -> anyhow::Result<Self> {
        if node_id > MAX_NODE_ID {
            return Err(anyhow::Error::msg(format!(
                "Node ID {} exceeds maximum of {}",
                node_id, MAX_NODE_ID
            )));
        }

        Ok(Self {
            node_id: (node_id as u64) << SEQUENCE_ID_BITS,
            sequence_id: 0,
            epoch,
        })
    }

    fn get_next_id(&mut self) -> u64 {
        self.get_shifted_timestamp() | self.node_id | self.next_sequence_id()
    }

    fn shift_timestamp(timestamp: u64) -> u64 {
        (timestamp << (NODE_ID_BITS + SEQUENCE_ID_BITS)) & HIGH_ORDER_MASK
    }

    fn get_shifted_timestamp(&self) -> u64 {
        let since_epoch = self
            .epoch
            .elapsed()
            .expect("1970 should always be in the past, except in our hearts");

        Self::shift_timestamp(since_epoch.as_millis() as u64)
    }

    fn next_sequence_id(&mut self) -> u64 {
        let sequence_id = self.sequence_id;
        self.sequence_id = (self.sequence_id + 1) % SEQUENCE_ID_CEILING;

        sequence_id
    }

    #[cfg(test)]
    fn peek_sequence_id(&self) -> u64 {
        self.sequence_id
    }

    #[cfg(test)]
    fn node_id(&self) -> u64 {
        self.node_id
    }
}

fn get_node_id(node_name: impl AsRef<str>, node_list: &Vec<String>) -> Result<u16, anyhow::Error> {
    if node_list.len() > MAX_NODE_ID as usize {
        // Fail eagerly on this, we don't want some nodes successfully initializing and others not for no good reason.
        return Err(anyhow::Error::msg(format!(
            "List size {} exceeds maximum node index",
            node_list.len()
        )));
    }

    let index = node_list
        .iter()
        .position(|item| item == node_name.as_ref())
        .ok_or(anyhow::Error::msg(format!(
            "Node {} not found in node list {:?}",
            node_name.as_ref(),
            node_list
        )))?;
    let truncated = index.try_into()?;

    Ok(truncated)
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let n = Node::default();
    let msg_slot: Arc<Mutex<Option<Message>>> = Arc::new(Mutex::new(None));
    let msg_slot_clone = msg_slot.clone();
    n.handle(
        "init",
        Arc::new(move |_, msg| {
            let mut msg_slot = msg_slot_clone.lock().expect("msg_slot lock poisoned");
            let _ = msg_slot.insert(msg);

            immediate!(Ok(()))
        }),
    )
    .await;

    let initialized = n.run_init().await;
    if initialized.is_err() {
        error!("Node init loop failed: {}", initialized.err().unwrap());
        return;
    }
    let node = initialized.unwrap();

    let init_message: Message = {
        let mut lock = msg_slot.lock().expect("msg_slot lock poisoned");
        if lock.is_none() {
            error!("Failed to record init message");
            return;
        }

        lock.take().unwrap()
    };
    let body: InitMessageBody = unwrap!(
        InitMessageBody::try_from(init_message.body),
        "Failed to load init message from body: {}"
    );
    let node_id: u16 = unwrap!(
        get_node_id(body.node_id, &body.node_ids),
        "Failed to generate node id: {}"
    );
    let generator: Arc<Mutex<Generator>> = Arc::new(Mutex::new(unwrap!(
        Generator::new(node_id, SystemTime::UNIX_EPOCH),
        "Failed to initialize generator: {}"
    )));

    let generator_clone = generator.clone();
    node.handle(
        "generate",
        Arc::new(move |node, msg| {
            let mut body = msg.body.clone();
            let mut generator = generator_clone.lock().expect("Generator lock was poisoned");
            // The safest interpretation of JSON numbers is JavaScript semantics, with the biggest integer representable
            // without loss of precision being 2^53, so unfortunately cast this to string.
            let node_id = format!("{:016x}", generator.get_next_id());

            body.insert("type".into(), serde_json::Value::from("generate_ok"));
            body.insert("id".into(), serde_json::Value::from(node_id));

            node.reply(msg, body).boxed()
        }),
    )
    .await;

    if let Err(e) = Node::run(Arc::new(node)).await {
        error!("Node failed: {}", e);
    }
}

#[cfg(test)]
mod tests {
    // Use mostly literals instead of constants from above in these tests as a sanity check.

    use super::*;

    use std::{
        thread,
        time::{Duration, SystemTime},
    };

    #[test]
    fn test_generator_limits_node_id_range() {
        let epoch = SystemTime::UNIX_EPOCH;

        let good_node_ids: Vec<u16> = vec![0, 1023];
        for node_id in good_node_ids {
            let generator = Generator::new(node_id, epoch);
            assert!(
                generator.is_ok(),
                "Generator creation failed for acceptable node id {}: {:?}",
                node_id,
                generator
            );
        }

        let bad_node_ids: Vec<u16> = vec![1024, 65535];
        for node_id in bad_node_ids {
            let generator = Generator::new(node_id, epoch);
            assert!(
                generator.is_err(),
                "Generator creation succeeded for too-large node id {}: {:?}",
                node_id,
                generator
            );
        }
    }

    #[test]
    fn test_generator_rolls_over_sequence_id() {
        let mut generator = Generator::new(0, SystemTime::UNIX_EPOCH)
            .expect("Generator creation for node 0 and Unix epoch failed");

        for i in 0..4096 {
            assert_eq!(i as u64, generator.next_sequence_id());
        }
        assert_eq!(0, generator.peek_sequence_id());
    }

    #[test]
    fn test_generator_shifts_node_id() {
        let generator = Generator::new(1, SystemTime::UNIX_EPOCH)
            .expect("Generator creation for node 1 and Unix epoch failed");

        assert_eq!(generator.node_id(), 4096);
    }

    #[test]
    fn test_shift_timestamp_masks_correct_bits() {
        let masked = Generator::shift_timestamp(u64::MAX);
        let high_order_mask: u64 = 1 << 63;
        let low_order_mask: u64 = (1 << 22) - 1;
        let time_mask = !(high_order_mask | low_order_mask);

        assert_eq!(masked & time_mask, masked);
    }

    #[test]
    fn test_node_id_finds_index() {
        let nodes: Vec<String> = (0..MAX_NODE_ID).map(|i| format!("n{}", i)).collect();

        assert_eq!(get_node_id("n1", &nodes).expect("node_id() failed"), 1);
    }

    #[test]
    fn test_node_id_rejects_long_list() {
        let nodes: Vec<String> = (0..MAX_NODE_ID + 1).map(|i| format!("n{}", i)).collect();

        assert!(get_node_id("n1", &nodes).is_err());
    }

    #[test]
    fn test_generate_id_has_node_id() {
        let mut generator = Generator::new(MAX_NODE_ID, SystemTime::UNIX_EPOCH)
            .expect("Generator creation with MAX_NODE_ID and Unix epoch failed");

        let time_mask = !(u64::MAX << 22);
        let sequence_mask: u64 = !((1 << 12) - 1);
        let mask = time_mask & sequence_mask;

        assert_eq!(generator.get_next_id() & mask, (1023 as u64) << 12);
    }

    #[test]
    fn test_generate_id_increments() {
        let mut generator = Generator::new(1, SystemTime::UNIX_EPOCH)
            .expect("Generator creation with 1 and Unix epoch failed");

        let time_mask = !(u64::MAX << 22);
        let first_id = generator.get_next_id();
        let second_id = generator.get_next_id();

        assert_eq!((second_id - first_id) & time_mask, 1);
    }

    #[test]
    fn test_time_increases() {
        // This could fail if someone or something mucks with the system clock while the test happens.
        let mut generator = Generator::new(1, SystemTime::UNIX_EPOCH)
            .expect("Generator creation with 1 and Unix epoch failed");

        let node_sequence_mask: u64 = !((1 << 22) - 1);

        let first_id = generator.get_next_id();
        thread::sleep(Duration::from_millis(10));
        let second_id = generator.get_next_id();

        assert!((second_id & node_sequence_mask) - (first_id & node_sequence_mask) > 0);
    }
}

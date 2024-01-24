use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufRead, AsyncWrite},
    sync::mpsc::unbounded_channel,
};

use crate::node::UninitializedNode;

pub mod io;
pub mod kv;
pub mod message;
pub mod node;
pub mod rpc_error;
pub mod types;
pub mod util;

#[allow(clippy::type_complexity)]
pub fn get_node_and_io<
    M: message::MessagePayload + Send + Serialize + for<'de> Deserialize<'de>,
    D: node::NodeDelegate<MessageType = M> + Send,
>(
    reader: impl AsyncBufRead + Send + Unpin + 'static,
    writer: impl AsyncWrite + Send + Unpin + 'static,
) -> (
    node::UninitializedNode<M, D>,
    io::Ingress<message::Message<M>>,
    io::Egress<message::Message<M>>,
) {
    let (ingress_tx, ingress_rx) = unbounded_channel::<message::Message<M>>();
    let (egress_tx, egress_rx) = unbounded_channel::<message::Message<M>>();

    let ingress = io::Ingress::new(ingress_tx.clone(), Box::new(reader));
    let egress = io::Egress::new(egress_rx, Box::new(writer));

    let node: UninitializedNode<M, D> = UninitializedNode::new(ingress_rx, egress_tx);

    (node, ingress, egress)
}

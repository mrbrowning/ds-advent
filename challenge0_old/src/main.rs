use std::sync::Arc;

use futures::FutureExt;
use log::error;
use maelstrom::node::Node;

#[tokio::main]
async fn main() {
    env_logger::init();

    let n = Node::default();
    n.handle(
        "echo",
        Arc::new(move |node, msg| {
            let mut body = msg.body.clone();
            body.insert("type".to_string(), serde_json::Value::from("echo_ok"));

            node.reply(msg, body).boxed()
        }),
    )
    .await;

    match n.run_init().await {
        Err(e) => {
            error!("Node init loop failed: {}", e);
        }
        Ok(node) => {
            if let Err(e) = Node::run(Arc::new(node)).await {
                error!("Node failed: {}", e);
            }
        }
    }
}

use maelstrom_csp2::{
    io::{Egress, Ingress},
    message::{Message, MessageBody},
    rpc_error::MaelstromError,
};
use tokio::sync::{mpsc::unbounded_channel, oneshot};

use crate::common::EchoPayload;

mod common;

#[tokio::test]
async fn test_ingress_fails_on_malformed_msg() {
    let (tx, _) = unbounded_channel::<Message<EchoPayload>>();
    let (reader, stdin) = common::get_reader_pair(1);

    let mut ingress = Ingress::new(tx, Box::new(reader));
    let (err_tx, err_rx) = oneshot::channel::<MaelstromError>();
    tokio::spawn(async move {
        if let Err(e) = ingress.run().await {
            let _ = err_tx.send(e);
        }
    });

    let msg_text: String = "\n".into();
    tokio::spawn(async move {
        let _ = stdin.send(msg_text).await;
    });

    if let Ok(e) = err_rx.await {
        match e {
            MaelstromError::SerializationError(_) => (),
            _ => {
                panic!("Wanted serialization error, got: {}", e);
            }
        }
    } else {
        panic!("Ingress failed to exit on error");
    }
}

#[tokio::test]
async fn test_ingress_ingests_msg() {
    let (tx, mut rx) = unbounded_channel::<Message<EchoPayload>>();
    let (reader, stdin) = common::get_reader_pair(1);

    let mut ingress = Ingress::new(tx, Box::new(reader));
    tokio::spawn(async move {
        let _ = ingress.run().await;
    });

    let msg_text = format!(
        "{}\n",
        r#"{"dest":"n1", "body":{"type":"echo", "msg_id":1}}"#
    );
    tokio::spawn(async move {
        let _ = stdin.send(msg_text).await;
    });

    let msg = rx.recv().await;
    if let Some(m) = msg {
        assert_eq!(m.dest, Some("n1".into()));
        assert_eq!(m.body.msg_id, Some(1.into()));
        match m.body.contents {
            EchoPayload::Echo => (),
            _ => panic!("Got unexpected message type: {:?}", m.body),
        }
    } else {
        panic!("Ingress hung up");
    }
}

#[tokio::test]
async fn test_egress_writes_msg() {
    let (tx, rx) = unbounded_channel::<Message<EchoPayload>>();
    let (writer, mut stdout) = common::get_writer_pair(1);

    let mut egress = Egress::new(rx, Box::new(writer));
    tokio::spawn(async move {
        let _ = egress.run().await;
    });

    let msg: Message<EchoPayload> = Message {
        src: None,
        dest: Some("n2".into()),
        body: MessageBody {
            msg_id: Some(1.into()),
            in_reply_to: None,
            contents: EchoPayload::Echo,
        },
    };
    tokio::spawn(async move {
        let _ = tx.send(msg);
    });

    let out = stdout.recv().await;
    if let Some(written) = out {
        assert_eq!(
            written.trim(),
            r#"{"dest":"n2","body":{"msg_id":1,"type":"echo"}}"#
        );
    } else {
        panic!("Egress never wrote message");
    }
}

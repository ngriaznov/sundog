//! The `fwdskew` command: the frames it builds and the connections it sends them on.

use sundog::wire::{self, Msg};
use tokio::io::AsyncReadExt as _;
use tokio::net::TcpListener;

use super::*;
use crate::forward::{FORWARD_WRITER, forward_stream, parse_fwdskew, send_forwards};

/// Splits `stream` into its length-prefixed frames and decodes each.
fn decode_stream(mut stream: &[u8]) -> Vec<Msg> {
    let mut messages = Vec::new();
    while !stream.is_empty() {
        let (head, rest) = stream.split_at(4);
        let length = u32::from_be_bytes(head.try_into().expect("four bytes")) as usize;
        let (frame, rest) = rest.split_at(length);
        messages.push(wire::decode(&Bytes::copy_from_slice(frame)).expect("a frame decodes"));
        stream = rest;
    }
    messages
}

#[test]
fn a_forward_stream_greets_as_the_forward_writer_then_forwards_one_record() {
    let stream = forward_stream("it", "k", "v", 1_700_000_000_000).expect("encodes");
    let messages = decode_stream(&stream);
    assert_eq!(messages.len(), 2, "{messages:?}");
    assert!(matches!(
        &messages[0],
        Msg::Hello { node, protocol, cluster, .. }
            if node.as_u64() == FORWARD_WRITER
                && *protocol == wire::PROTOCOL_VERSION
                && *cluster == wire::UNNAMED_CLUSTER
    ));
    let Msg::ForwardBatch {
        cache,
        view_hash,
        recs,
        ..
    } = &messages[1]
    else {
        panic!("a forward follows the hello: {:?}", messages[1]);
    };
    assert_eq!((cache.as_str(), *view_hash), ("it", 0));
    let [rec] = recs.as_slice() else {
        panic!("one record: {recs:?}");
    };
    assert_eq!(
        rec.ver.wall_ms, 1_700_000_000_000,
        "the stamp is the one asked"
    );
    assert_eq!(rec.ver.node.as_u64(), FORWARD_WRITER);
    assert_eq!(
        postcard::from_bytes::<String>(&rec.key).expect("key decodes"),
        "k"
    );
    assert_eq!(
        postcard::from_bytes::<String>(rec.value.as_ref().expect("a put")).expect("value decodes"),
        "v"
    );
}

#[tokio::test]
async fn send_forwards_delivers_the_stream_to_each_listening_target_and_counts_it() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let live = listener.local_addr().expect("a local address");
    // Bound and dropped: nothing listens there.
    let dead = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback")
        .local_addr()
        .expect("a local address");
    let reader = tokio::spawn(async move {
        let (mut connection, _) = listener.accept().await.expect("accepted");
        let mut received = Vec::new();
        connection.read_to_end(&mut received).await.expect("read");
        received
    });

    let sent = send_forwards(&[dead, live], "it", "k", "v", 90_000)
        .await
        .expect("encodes");

    assert_eq!(sent, 1, "the dead target is not counted");
    let messages = decode_stream(&reader.await.expect("reader finished"));
    let Msg::ForwardBatch { recs, .. } = &messages[1] else {
        panic!("a forward follows the hello: {messages:?}");
    };
    let ahead = recs[0].ver.wall_ms.saturating_sub(
        u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("past the epoch")
                .as_millis(),
        )
        .expect("fits"),
    );
    assert!(
        (80_000..=90_000).contains(&ahead),
        "stamped about 90 seconds ahead of the clock: {ahead}"
    );
}

#[test]
fn fwdskew_arguments_are_a_lead_a_key_and_a_value_that_may_hold_spaces() {
    assert_eq!(
        parse_fwdskew(Some("90000"), Some("k a value with spaces")),
        Some((90_000, "k", "a value with spaces"))
    );
    assert_eq!(parse_fwdskew(None, None), None);
    assert_eq!(parse_fwdskew(Some("soon"), Some("k v")), None);
    assert_eq!(parse_fwdskew(Some("1"), Some("key-without-value")), None);
    assert_eq!(parse_fwdskew(Some("1"), None), None);
}

//! A mismatched bucket small enough for a listing, whose keys are large
//! enough that the listing outgrows a frame, answers a digest exchange in
//! several `AeBucket` messages, which the requester joins.

use super::anti_entropy::{raw_request_conn, replies_until_req_done};
use super::*;

/// `count` entries with `key_len`-byte keys, the big-endian index leading,
/// at one version.
fn long_key_entries(count: u64, key_len: usize) -> Vec<(Bytes, Hlc)> {
    (0..count)
        .map(|i| {
            let mut key = vec![0u8; key_len];
            key[..8].copy_from_slice(&i.to_be_bytes());
            (
                Bytes::from(key),
                Hlc {
                    wall_ms: 5,
                    logical: 0,
                    node: NodeId::from(1),
                },
            )
        })
        .collect()
}

/// 300 entries of 16 KiB keys: under the 384 entries that make a sketch, and
/// 4.9 MB of keys, more than one frame holds.
fn large_key_entries() -> Vec<(Bytes, Hlc)> {
    long_key_entries(300, 16 * 1_024)
}

/// A raw `AeDigest` for bucket 3 that mismatches, from a peer that said hello
/// with `protocol`, and the replies up to `ReqDone`.
async fn digest_replies_for_protocol(server: &Mesh, protocol: u16) -> Vec<Msg> {
    let hello = wire::encode(&Msg::Hello {
        node: NodeId::from(2),
        incarnation: 1,
        protocol,
        cluster: wire::UNNAMED_CLUSTER,
    })
    .expect("encodes");
    let mut peer = raw_request_conn(server.local_addr(), hello).await;
    let request = wire::encode(&Msg::AeDigest {
        cache: SmolStr::new("users"),
        buckets: vec![(3, 999)],
    })
    .expect("encodes");
    peer.send(request).await.expect("request sends");
    replies_until_req_done(&mut peer).await
}

fn listing_server(entries: Vec<(Bytes, Hlc)>) -> Arc<FixtureHandler> {
    Arc::new(FixtureHandler {
        digests: vec![(3, 111)],
        bucket_entries: entries,
        ..Default::default()
    })
}

#[tokio::test]
async fn a_digest_listing_past_a_frame_arrives_in_several_messages_none_over_a_frame() {
    let entries = large_key_entries();
    let (server, _inbound) = spawn_mesh(NodeId::from(1), listing_server(entries.clone())).await;

    let replies = digest_replies_for_protocol(&server, wire::PROTOCOL_AE_LISTING_CHUNKS).await;

    assert!(replies.len() > 1, "{} messages", replies.len());
    let mut joined = Vec::new();
    for msg in &replies {
        let Msg::AeBucket {
            bucket, entries, ..
        } = msg
        else {
            panic!("only AeBucket replies, got {msg:?}");
        };
        assert_eq!(*bucket, 3);
        assert!(
            wire::encode(msg).expect("each message encodes").len() <= wire::MAX_FRAME,
            "a slice fits a frame"
        );
        joined.extend(entries.iter().cloned());
    }
    assert_eq!(joined, entries, "the slices hold every entry, in order");
}

#[tokio::test]
async fn a_peer_on_protocol_eight_gets_the_digest_listing_as_one_message() {
    // 2.4 MB: past the half frame a slice holds, within the frame a whole
    // listing needs, which is all a peer on protocol 8 can read.
    let entries = long_key_entries(300, 8 * 1_024);
    let (server, _inbound) = spawn_mesh(NodeId::from(1), listing_server(entries.clone())).await;

    let replies = digest_replies_for_protocol(&server, wire::PROTOCOL_INVALIDATION_FOLD).await;

    let [Msg::AeBucket { entries: got, .. }] = &replies[..] else {
        panic!("one AeBucket for a peer that reads each as the whole bucket: {replies:?}");
    };
    assert_eq!(*got, entries);
}

#[tokio::test]
async fn a_peer_on_the_chunking_protocol_gets_a_digest_listing_that_fits_a_frame_as_one_message() {
    // 2.4 MB, as above. The previous release speaks this protocol and reads
    // each message of a digest reply as the whole bucket, so a listing it
    // received whole before is still sent whole.
    let entries = long_key_entries(300, 8 * 1_024);
    let (server, _inbound) = spawn_mesh(NodeId::from(1), listing_server(entries.clone())).await;

    let replies = digest_replies_for_protocol(&server, wire::PROTOCOL_AE_LISTING_CHUNKS).await;

    let [Msg::AeBucket { entries: got, .. }] = &replies[..] else {
        panic!("one AeBucket for a listing a frame holds: {replies:?}");
    };
    assert_eq!(*got, entries);
}

#[tokio::test]
async fn a_digest_listing_that_fits_a_frame_is_one_message() {
    let entries = long_key_entries(100, 32);
    let (server, _inbound) = spawn_mesh(NodeId::from(1), listing_server(entries.clone())).await;

    let replies = digest_replies_for_protocol(&server, wire::PROTOCOL_AE_LISTING_CHUNKS).await;

    let [Msg::AeBucket { entries: got, .. }] = &replies[..] else {
        panic!("a listing that fits is not cut: {replies:?}");
    };
    assert_eq!(*got, entries);
}

#[tokio::test]
async fn the_requester_joins_the_slices_of_a_digest_listing_into_one_mismatch() {
    let entries = large_key_entries();
    let (server, _server_inbound) =
        spawn_mesh(NodeId::from(1), listing_server(entries.clone())).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let got = client
        .ae_round(NodeId::from(1), SmolStr::new("users"), vec![(3, 999)])
        .await
        .expect("the exchange completes");

    assert_eq!(
        got,
        vec![AeMismatch::Bucket(3, kv(entries))],
        "one mismatch holding the whole bucket, so the diff sees every entry the peer holds"
    );
}

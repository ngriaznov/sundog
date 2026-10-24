//! Anti-entropy request tests.

use super::*;
use crate::net::request::{REQUEST_TIMEOUT, with_request_timeout};

#[test]
fn ae_part_reply_names_its_bucket_and_part_in_either_shape() {
    let listing = AePartReply::Listing {
        bucket: 7,
        part: 3,
        entries: Vec::new(),
    };
    let sketch = AePartReply::Sketch {
        bucket: 9,
        part: 63,
        cells: Vec::new(),
    };
    assert_eq!((listing.bucket(), listing.part()), (7, 3));
    assert_eq!((sketch.bucket(), sketch.part()), (9, 63));
}

#[tokio::test]
async fn ae_round_returns_only_mismatched_buckets() {
    let entries = vec![(
        Bytes::from_static(b"k1"),
        Hlc {
            wall_ms: 5,
            logical: 0,
            node: NodeId::from(1),
        },
    )];
    let handler = Arc::new(FixtureHandler {
        records: Vec::new(),
        digests: vec![(0, 111), (1, 222)],
        bucket_entries: entries.clone(),
        pulled: Mutex::new(Vec::new()),
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    // bucket 0 matches, bucket 1 mismatches, bucket 2 is requester-only.
    let local_buckets = vec![(0, 111), (1, 999)];
    let result = client
        .ae_round(NodeId::from(1), SmolStr::new("users"), local_buckets)
        .await
        .expect("ae round succeeds");

    assert_eq!(result, vec![AeMismatch::Bucket(1, kv(entries))]);
}

#[tokio::test]
async fn ae_round_returns_part_digests_for_a_bucket_past_the_part_threshold() {
    let part_digests: Vec<u64> = (0..64u64).collect();
    let handler = Arc::new(FixtureHandler {
        digests: vec![(0, 111), (1, 222)],
        bucket_lens: vec![(1, 500)],
        part_digests: vec![(1, part_digests.clone())],
        ae_part_min_bucket: 100,
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    // bucket 0 matches (under threshold anyway); bucket 1 mismatches and
    // its 500-entry fixture length passes ae_part_min_bucket, so the
    // responder answers with its part digests instead of a listing.
    let local_buckets = vec![(0, 111), (1, 999)];
    let result = client
        .ae_round(NodeId::from(1), SmolStr::new("users"), local_buckets)
        .await
        .expect("ae round succeeds");

    assert_eq!(result, vec![AeMismatch::PartDigests(1, part_digests)]);
}

/// Accepts connections off `listener` forever, one task per connection:
/// reads `Hello`, then serves at most one `AeDigest` with an empty
/// `ReqDone`-only reply and closes right after, standing in for the
/// server's `REQ_CONN_IDLE_TIMEOUT` without waiting out the real 60s
/// bound. `Mesh::update_peers` also opens its own persistent writer
/// connection to the same address, which sends `Hello` and then
/// nothing else; that connection's task blocks forever on its
/// second read and is otherwise harmless, since every connection is
/// served independently. `served` fires once per request answered,
/// after its connection is already closed.
fn spawn_serve_one_digest_per_connection(
    listener: TcpListener,
    served: Arc<tokio::sync::Notify>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.expect("accept");
            let served = Arc::clone(&served);
            tokio::spawn(async move {
                let mut framed = LengthDelimitedCodec::builder()
                    .max_frame_length(MAX_FRAME)
                    .new_framed(stream);
                let Some(Ok(_hello)) = framed.next().await else {
                    return;
                };
                let Some(Ok(_digest)) = framed.next().await else {
                    return;
                };
                let reply = wire::encode(&Msg::ReqDone).expect("encodes");
                framed.send(reply).await.expect("reply sends");
                drop(framed); // closes the connection, as the idle timeout would
                served.notify_one();
            });
        }
    })
}

/// A connection `Mesh::acquire_conn` reuses from the pool after the
/// server already closed it (its idle timeout, forced here to fire
/// after exactly one request rather than after the real
/// `REQ_CONN_IDLE_TIMEOUT`) must not fail the request it carries: the
/// stale reply is caught as an immediate `EOF` and the request retries
/// on a fresh dial instead.
#[tokio::test]
async fn ae_round_retries_a_pooled_connection_the_server_already_closed() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("listener has a local addr");
    let served = Arc::new(tokio::sync::Notify::new());
    let _server = spawn_serve_one_digest_per_connection(listener, Arc::clone(&served));

    let (client, _client_inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(2), addr)]);

    let first = client
        .ae_round(NodeId::from(2), SmolStr::new("users"), Vec::new())
        .await
        .expect("first round succeeds and checks its connection into the pool");
    assert!(first.is_empty(), "{first:?}");

    served.notified().await;

    let second = client
        .ae_round(NodeId::from(2), SmolStr::new("users"), Vec::new())
        .await
        .expect(
            "a pooled connection the server already closed must not fail the request: \
             acquire_conn retries it on a fresh dial",
        );
    assert!(second.is_empty(), "{second:?}");
}

/// A raw request connection to `addr` that has already sent `hello`, for
/// speaking as a peer of any protocol version, including the ones this
/// build's own `Mesh` doesn't send.
async fn raw_request_conn(
    addr: SocketAddr,
    hello: Bytes,
) -> tokio_util::codec::Framed<tokio::net::TcpStream, tokio_util::codec::LengthDelimitedCodec> {
    let stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect to the mesh listener");
    let mut framed = tokio_util::codec::LengthDelimitedCodec::builder()
        .max_frame_length(wire::MAX_FRAME)
        .new_framed(stream);
    framed.send(hello).await.expect("hello sends");
    framed
}

/// A protocol-1 hello: an unnamed protocol-2 encoding minus its
/// trailing `protocol` and `cluster` bytes, one each, checked to decode
/// back as protocol 1.
fn legacy_hello(node: NodeId) -> Bytes {
    let full = wire::encode(&Msg::Hello {
        node,
        incarnation: 1,
        protocol: 2,
        cluster: wire::UNNAMED_CLUSTER,
    })
    .expect("encodes");
    let legacy = full.slice(..full.len() - 2);
    assert!(matches!(
        wire::decode(&legacy),
        Ok(Msg::Hello { protocol: 1, .. })
    ));
    legacy
}

async fn replies_until_req_done(
    framed: &mut tokio_util::codec::Framed<
        tokio::net::TcpStream,
        tokio_util::codec::LengthDelimitedCodec,
    >,
) -> Vec<Msg> {
    let mut replies = Vec::new();
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(5), framed.next())
            .await
            .expect("a reply arrives")
            .expect("connection stays open")
            .expect("frame reads");
        let msg = wire::decode(&frame.freeze()).expect("reply decodes");
        if matches!(msg, Msg::ReqDone) {
            return replies;
        }
        replies.push(msg);
    }
}

#[tokio::test]
async fn a_protocol_one_peer_never_receives_part_digests() {
    let handler = Arc::new(FixtureHandler {
        digests: vec![(1, 222)],
        bucket_lens: vec![(1, 500)],
        part_digests: vec![(1, (0..64u64).collect())],
        bucket_entries: vec![(Bytes::from_static(b"k1"), sample_record(1).ver)],
        ae_part_min_bucket: 100,
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let digest = wire::encode(&Msg::AeDigest {
        cache: SmolStr::new("users"),
        buckets: vec![(1, 999)],
    })
    .expect("encodes");

    let mut old_peer = raw_request_conn(server.local_addr(), legacy_hello(NodeId::from(2))).await;
    old_peer.send(digest.clone()).await.expect("digest sends");
    let replies = replies_until_req_done(&mut old_peer).await;
    assert!(
        replies
            .iter()
            .all(|msg| matches!(msg, Msg::AeBucket { .. } | Msg::AeSketch { .. })),
        "a protocol-1 peer gets only the shapes it understands: {replies:?}"
    );
    assert!(!replies.is_empty(), "{replies:?}");

    let hello = wire::encode(&Msg::Hello {
        node: NodeId::from(3),
        incarnation: 1,
        protocol: wire::PROTOCOL_VERSION,
        cluster: wire::UNNAMED_CLUSTER,
    })
    .expect("encodes");
    let mut new_peer = raw_request_conn(server.local_addr(), hello).await;
    new_peer.send(digest).await.expect("digest sends");
    let replies = replies_until_req_done(&mut new_peer).await;
    assert!(
        replies
            .iter()
            .any(|msg| matches!(msg, Msg::AePartDigests { .. })),
        "the same bucket answers a current peer with part digests: {replies:?}"
    );
}

#[tokio::test]
async fn a_cold_donor_serves_a_protocol_one_peer_and_declines_a_current_one() {
    let handler = Arc::new(FixtureHandler {
        records: vec![sample_record(1)],
        snapshot_unavailable: true,
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let request = wire::encode(&Msg::StRequest {
        cache: SmolStr::new("users"),
    })
    .expect("encodes");

    let mut old_peer = raw_request_conn(server.local_addr(), legacy_hello(NodeId::from(2))).await;
    old_peer.send(request.clone()).await.expect("request sends");
    let frame = tokio::time::timeout(Duration::from_secs(5), old_peer.next())
        .await
        .expect("a reply arrives")
        .expect("connection stays open")
        .expect("frame reads");
    assert!(
        matches!(wire::decode(&frame.freeze()), Ok(Msg::StChunk { .. })),
        "a peer that cannot be declined is served"
    );

    let hello = wire::encode(&Msg::Hello {
        node: NodeId::from(3),
        incarnation: 1,
        protocol: wire::PROTOCOL_VERSION,
        cluster: wire::UNNAMED_CLUSTER,
    })
    .expect("encodes");
    let mut new_peer = raw_request_conn(server.local_addr(), hello).await;
    new_peer.send(request).await.expect("request sends");
    let frame = tokio::time::timeout(Duration::from_secs(5), new_peer.next())
        .await
        .expect("a reply arrives")
        .expect("connection stays open")
        .expect("frame reads");
    assert!(matches!(
        wire::decode(&frame.freeze()),
        Ok(Msg::StUnavailable { .. })
    ));
}

#[tokio::test]
async fn ae_parts_returns_listings_and_sketches_per_the_threshold() {
    let small_entries = vec![(
        Bytes::from_static(b"k1"),
        Hlc {
            wall_ms: 5,
            logical: 0,
            node: NodeId::from(1),
        },
    )];
    // Past ClusterConfig::default's ae_sketch_min_bucket (384), so this
    // part answers with a sketch instead of a listing.
    let big_entries: Vec<(Bytes, Hlc)> = (0..400u32)
        .map(|i| {
            (
                Bytes::from(i.to_le_bytes().to_vec()),
                Hlc {
                    wall_ms: u64::from(i) + 1,
                    logical: 0,
                    node: NodeId::from(1),
                },
            )
        })
        .collect();
    let handler = Arc::new(FixtureHandler {
        part_entries: vec![
            (BucketPart { bucket: 1, part: 2 }, kv(small_entries.clone())),
            (BucketPart { bucket: 1, part: 3 }, kv(big_entries)),
        ],
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let got = client
        .ae_parts(NodeId::from(1), SmolStr::new("users"), vec![(1, 2), (1, 3)])
        .await
        .expect("ae_parts succeeds");

    assert_eq!(got.len(), 2, "one reply per requested part");
    assert!(
        got.iter().any(|reply| matches!(
            reply,
            AePartReply::Listing { bucket: 1, part: 2, entries } if *entries == kv(small_entries.clone())
        )),
        "the small part answers with a listing: {got:?}"
    );
    assert!(
        got.iter().any(|reply| matches!(
            reply,
            AePartReply::Sketch {
                bucket: 1,
                part: 3,
                ..
            }
        )),
        "the large part answers with a sketch: {got:?}"
    );
}

/// `count` entries whose keys are `key_len` bytes each, the big-endian index
/// leading, at one version.
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

/// The sketch a responder with the default cell count builds over `entries`.
fn sketch_of(entries: &[(Bytes, Hlc)]) -> Vec<wire::Cell> {
    let mut iblt = crate::cluster::sketch::Iblt::new(ClusterConfig::default().ae_sketch_cells);
    for (key, version) in entries {
        iblt.insert(xxh3_64(key), *version);
    }
    iblt.into_cells()
}

/// One digest exchange for bucket 1 of `users`, which the requester holds
/// under a different digest, against a responder whose bucket holds
/// `entries`.
async fn ae_round_over_bucket(entries: Vec<(Bytes, Hlc)>) -> Vec<AeMismatch> {
    let handler = Arc::new(FixtureHandler {
        digests: vec![(1, 222)],
        bucket_entries: entries,
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);
    client
        .ae_round(NodeId::from(1), SmolStr::new("users"), vec![(1, 999)])
        .await
        .expect("ae round succeeds")
}

#[tokio::test]
async fn a_digest_exchange_answers_a_small_bucket_of_long_keys_with_a_sketch() {
    // Three entries, far under ae_sketch_min_bucket, whose keys of a sixth of
    // a frame each make a listing past the half frame a reply may hold.
    let long = long_key_entries(3, MAX_FRAME / 6);
    assert_eq!(
        ae_round_over_bucket(long.clone()).await,
        vec![AeMismatch::Sketch(1, sketch_of(&long))]
    );

    let short = long_key_entries(3, 32);
    assert_eq!(
        ae_round_over_bucket(short.clone()).await,
        vec![AeMismatch::Bucket(1, kv(short))],
        "the same count of short keys stays a listing"
    );
}

#[tokio::test]
async fn an_ae_parts_request_answers_a_small_part_of_long_keys_with_a_sketch() {
    let long = long_key_entries(3, MAX_FRAME / 6);
    let short = long_key_entries(3, 32);
    let handler = Arc::new(FixtureHandler {
        part_entries: vec![
            (BucketPart { bucket: 1, part: 2 }, kv(long.clone())),
            (BucketPart { bucket: 1, part: 3 }, kv(short.clone())),
        ],
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let got = client
        .ae_parts(NodeId::from(1), SmolStr::new("users"), vec![(1, 2), (1, 3)])
        .await
        .expect("ae_parts succeeds");

    assert_eq!(got.len(), 2, "one reply per requested part: {got:?}");
    assert!(
        got.contains(&AePartReply::Sketch {
            bucket: 1,
            part: 2,
            cells: sketch_of(&long),
        }),
        "the part of long keys answers with a sketch"
    );
    assert!(
        got.contains(&AePartReply::Listing {
            bucket: 1,
            part: 3,
            entries: kv(short),
        }),
        "the part of short keys stays a listing"
    );
}

#[tokio::test]
async fn ae_digest_from_a_peer_with_replicate_frames_queued_is_answered_empty() {
    let entries = vec![(
        Bytes::from_static(b"k1"),
        Hlc {
            wall_ms: 5,
            logical: 0,
            node: NodeId::from(1),
        },
    )];
    let handler = Arc::new(FixtureHandler {
        records: Vec::new(),
        digests: vec![(0, 111), (1, 222)],
        bucket_entries: entries.clone(),
        pulled: Mutex::new(Vec::new()),
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);
    // The server knows the client at an address nobody listens on, so a
    // replicate frame toward it stays queued: a stream in motion, from
    // the server's side, for as long as this test runs.
    let unreachable = SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 1));
    server.update_peers(vec![peer_at(NodeId::from(2), unreachable)]);
    let frame = OutFrame::new(Msg::Replicate {
        cache: SmolStr::new("users"),
        rec: sample_record(1),
    })
    .expect("encodes");
    server.send_frames(NodeId::from(2), MsgClass::Replicate, [frame]);

    let round = || async {
        client
            .ae_round(
                NodeId::from(1),
                SmolStr::new("users"),
                vec![(0, 111), (1, 999)],
            )
            .await
            .expect("ae round succeeds")
    };
    for attempt in 0..MAX_AE_DEFERRALS {
        let result = round().await;
        assert!(
            result.is_empty(),
            "round {attempt}: bucket 1 mismatches, but the stream in motion covers it: {result:?}"
        );
    }
    assert_eq!(
        round().await,
        vec![AeMismatch::Bucket(1, kv(entries.clone()))],
        "the bound serves the next round, frames queued or not"
    );
    assert!(round().await.is_empty(), "a served round resets the bound");
    assert!(
        !server.inner.defers_ae_digest_from(NodeId::from(3)),
        "a peer nothing streams to is served as usual"
    );
}

#[tokio::test]
async fn ae_round_times_out_against_a_peer_that_accepts_but_never_responds() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("listener has a local addr");
    let _stalled_peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut framed = LengthDelimitedCodec::builder()
            .max_frame_length(MAX_FRAME)
            .new_framed(stream);
        let _hello = framed.next().await.expect("hello arrives");
        let _digest = framed.next().await.expect("ae digest arrives");
        // Accepts and reads, then goes silent: the failure mode
        // `REQUEST_TIMEOUT` bounds.
        std::future::pending::<()>().await;
    });

    let (mesh, _inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    mesh.update_peers(vec![peer_at(NodeId::from(2), addr)]);

    let result = tokio::time::timeout(
        REQUEST_TIMEOUT + Duration::from_secs(5),
        mesh.ae_round(NodeId::from(2), SmolStr::new("users"), Vec::new()),
    )
    .await
    .expect(
        "ae_round must give up on its own internal REQUEST_TIMEOUT, well inside this \
         generous outer bound, not hang forever",
    );
    assert!(
        result.is_err(),
        "a peer that accepts but never responds must surface as an error, not hang"
    );
}

#[tokio::test]
async fn ae_pull_returns_requested_records_as_replicate_messages() {
    let records = vec![sample_record(9)];
    let handler = Arc::new(FixtureHandler {
        records: records.clone(),
        digests: Vec::new(),
        bucket_entries: Vec::new(),
        pulled: Mutex::new(Vec::new()),
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let keys = vec![Bytes::from_static(b"k9")];
    let got = client
        .ae_pull(NodeId::from(1), SmolStr::new("users"), keys)
        .await
        .expect("pull succeeds");
    assert_eq!(got, records);
}

#[tokio::test]
async fn ae_entries_returns_full_listings_for_the_requested_buckets() {
    let entries = vec![(
        Bytes::from_static(b"k1"),
        Hlc {
            wall_ms: 5,
            logical: 0,
            node: NodeId::from(1),
        },
    )];
    let handler = Arc::new(FixtureHandler {
        records: Vec::new(),
        digests: Vec::new(),
        bucket_entries: entries.clone(),
        pulled: Mutex::new(Vec::new()),
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let got = client
        .ae_entries(NodeId::from(1), SmolStr::new("users"), vec![3, 7])
        .await
        .expect("ae_entries succeeds");

    assert_eq!(got, vec![(3, kv(entries.clone())), (7, kv(entries))]);
}

/// `count` entries with 32-byte keys, the big-endian index leading, at one
/// version: about 50 bytes each on the wire.
fn large_bucket_entries(count: u64) -> Vec<(Bytes, Hlc)> {
    (0..count)
        .map(|i| {
            let mut key = [0u8; 32];
            key[..8].copy_from_slice(&i.to_be_bytes());
            (
                Bytes::copy_from_slice(&key),
                Hlc {
                    wall_ms: 5,
                    logical: 0,
                    node: NodeId::from(1),
                },
            )
        })
        .collect()
}

#[tokio::test]
async fn ae_entries_returns_a_bucket_listing_larger_than_one_frame() {
    let entries = large_bucket_entries(300_000);
    let handler = Arc::new(FixtureHandler {
        bucket_entries: entries.clone(),
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let got = client
        .ae_entries(NodeId::from(1), SmolStr::new("users"), vec![3])
        .await
        .expect("a large bucket listing is served under MAX_FRAME");

    assert_eq!(
        got.len(),
        1,
        "the chunks of one bucket join into one element"
    );
    let (bucket, listing) = &got[0];
    assert_eq!(*bucket, 3);
    assert_eq!(listing.len(), 300_000);
    assert!(
        listing
            .iter()
            .zip(&entries)
            .all(|(got, (key, version))| got.key == *key && got.version == *version),
        "the joined listing keeps the responder's entries in order"
    );
}

/// A raw `AeEntries` request for buckets `[3, 7]` of cache `users` from a
/// peer that said hello with `protocol`, and the replies up to `ReqDone`.
async fn ae_entries_replies_for_protocol(server: &Mesh, protocol: u16) -> Vec<Msg> {
    let hello = wire::encode(&Msg::Hello {
        node: NodeId::from(2),
        incarnation: 1,
        protocol,
        cluster: wire::UNNAMED_CLUSTER,
    })
    .expect("encodes");
    let mut peer = raw_request_conn(server.local_addr(), hello).await;
    let request = wire::encode(&Msg::AeEntries {
        cache: SmolStr::new("users"),
        buckets: vec![3, 7],
    })
    .expect("encodes");
    peer.send(request).await.expect("request sends");
    replies_until_req_done(&mut peer).await
}

#[tokio::test]
async fn a_protocol_eight_peer_gets_one_ae_bucket_per_bucket() {
    let handler = Arc::new(FixtureHandler {
        bucket_entries: large_bucket_entries(1_000),
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let replies = ae_entries_replies_for_protocol(&server, wire::PROTOCOL_INVALIDATION_FOLD).await;
    let sizes: Vec<(u16, usize)> = replies
        .iter()
        .map(|msg| match msg {
            Msg::AeBucket {
                bucket, entries, ..
            } => (*bucket, entries.len()),
            other => panic!("only AeBucket replies, got {other:?}"),
        })
        .collect();
    assert_eq!(sizes, vec![(3, 1_000), (7, 1_000)]);
}

#[tokio::test]
async fn a_protocol_nine_peer_gets_a_large_bucket_in_several_ae_bucket_messages() {
    let handler = Arc::new(FixtureHandler {
        bucket_entries: large_bucket_entries(300_000),
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let hello = wire::encode(&Msg::Hello {
        node: NodeId::from(2),
        incarnation: 1,
        protocol: wire::PROTOCOL_AE_LISTING_CHUNKS,
        cluster: wire::UNNAMED_CLUSTER,
    })
    .expect("encodes");
    let mut peer = raw_request_conn(server.local_addr(), hello).await;
    let request = wire::encode(&Msg::AeEntries {
        cache: SmolStr::new("users"),
        buckets: vec![3],
    })
    .expect("encodes");
    peer.send(request).await.expect("request sends");

    let mut frames = Vec::new();
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(10), peer.next())
            .await
            .expect("a reply arrives")
            .expect("connection stays open")
            .expect("frame reads");
        assert!(frame.len() <= wire::MAX_FRAME, "{} bytes", frame.len());
        let msg = wire::decode(&frame.freeze()).expect("reply decodes");
        if matches!(msg, Msg::ReqDone) {
            break;
        }
        frames.push(msg);
    }
    assert!(frames.len() > 1, "{} AeBucket messages", frames.len());
    let mut total = 0;
    for msg in frames {
        let Msg::AeBucket {
            bucket, entries, ..
        } = msg
        else {
            panic!("only AeBucket replies");
        };
        assert_eq!(bucket, 3);
        total += entries.len();
    }
    assert_eq!(total, 300_000);
}

/// Twenty records with 1 MiB values under one-byte keys: a pull reply of
/// 20 MiB from a few small keys.
fn large_value_records() -> Vec<WireRecord> {
    (0..20u8)
        .map(|n| WireRecord {
            key: Bytes::copy_from_slice(&[n]),
            value: Some(Bytes::from(vec![n; 1024 * 1024])),
            ver: Hlc {
                wall_ms: 5,
                logical: 0,
                node: NodeId::from(1),
            },
            expires_at_ms: None,
        })
        .collect()
}

/// A pull reply is whole. The requester reads every message up to `ReqDone`
/// as the complete answer, so a reply cut short would read as keys the
/// responder does not hold.
#[tokio::test]
async fn an_ae_pull_for_small_keys_with_large_values_gets_every_record() {
    let records = large_value_records();
    let handler = Arc::new(FixtureHandler {
        records: records.clone(),
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let keys: Vec<Bytes> = records.iter().map(|rec| rec.key.clone()).collect();
    let got = client
        .ae_pull(NodeId::from(1), SmolStr::new("users"), keys)
        .await
        .expect("the pull completes");

    assert_eq!(got, records, "every record arrives, in order");
}

#[tokio::test]
async fn an_ae_pull_by_hash_for_large_values_gets_every_record() {
    let records = large_value_records();
    let ver = records[0].ver;
    let handler = Arc::new(FixtureHandler {
        records: records.clone(),
        bucket_entries: records.iter().map(|rec| (rec.key.clone(), ver)).collect(),
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let hashes: Vec<u64> = records.iter().map(|rec| xxh3_64(&rec.key)).collect();
    let got = client
        .ae_pull_hashes(NodeId::from(1), SmolStr::new("users"), 3, hashes)
        .await
        .expect("the pull completes");

    assert_eq!(got, records, "every record arrives, in order");
}

#[tokio::test]
async fn ae_pull_hashes_resolves_only_the_requested_hash_to_records() {
    // Two entries, one hash requested: the default `records_for_hashes`
    // must filter to the matching key, not hand every key to `records_for`.
    let key_a = Bytes::from_static(b"a");
    let key_b = Bytes::from_static(b"b");
    let ver = Hlc {
        wall_ms: 1,
        logical: 0,
        node: NodeId::from(1),
    };
    let handler = Arc::new(FixtureHandler {
        records: vec![sample_record(9)],
        digests: Vec::new(),
        bucket_entries: vec![(key_a.clone(), ver), (key_b, ver)],
        pulled: Mutex::new(Vec::new()),
        ..Default::default()
    });
    let handler_for_asserts = Arc::clone(&handler);
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let hash_a = xxh3_64(&key_a);
    let got = client
        .ae_pull_hashes(NodeId::from(1), SmolStr::new("users"), 3, vec![hash_a])
        .await
        .expect("ae_pull_hashes succeeds");
    assert_eq!(got, vec![sample_record(9)]);

    let pulled = handler_for_asserts
        .pulled
        .lock()
        .expect("invariant: fixture mutex is never poisoned");
    assert_eq!(
        pulled.as_slice(),
        [(SmolStr::new("users"), vec![key_a])],
        "the resolved hash must pull exactly key_a's bytes, never key_b's"
    );
}

/// A handler whose `entries_for_buckets` never resolves, for
/// [`ae_entries_times_out_promptly_under_a_short_override`].
struct HangingHandler;

impl RequestHandler for HangingHandler {
    fn snapshot_chunks(&self, _cache: SmolStr) -> BoxStream<'static, Vec<WireRecord>> {
        Box::pin(futures::stream::empty())
    }
    fn digests(&self, _cache: SmolStr) -> BoxFuture<'_, Vec<BucketDigest>> {
        Box::pin(async { Vec::new() })
    }
    fn bucket_entries(&self, _cache: SmolStr, _bucket: u16) -> BoxFuture<'_, Vec<KeyVersion>> {
        Box::pin(async { Vec::new() })
    }
    fn entries_for_buckets(
        &self,
        _cache: SmolStr,
        _buckets: Vec<u16>,
    ) -> BoxFuture<'_, crate::store::BucketEntries> {
        Box::pin(std::future::pending())
    }
    fn records_for(&self, _cache: SmolStr, _keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>> {
        Box::pin(async { Vec::new() })
    }
    fn bucket_lens(&self, _cache: SmolStr, _buckets: Vec<u16>) -> BoxFuture<'_, Vec<BucketLen>> {
        Box::pin(async { Vec::new() })
    }
    fn part_digests(
        &self,
        _cache: SmolStr,
        _buckets: Vec<u16>,
    ) -> BoxFuture<'_, Vec<BucketPartDigests>> {
        Box::pin(async { Vec::new() })
    }
    fn entries_for_parts(
        &self,
        _cache: SmolStr,
        _parts: Vec<BucketPart>,
    ) -> BoxFuture<'_, crate::store::PartEntries> {
        Box::pin(async { Vec::new() })
    }
}

#[tokio::test]
async fn ae_entries_times_out_promptly_under_a_short_override() {
    // `AeEntries` is served by `entries_for_buckets`, which never
    // resolves here: `ae_entries` must give up on its own internal
    // timeout rather than hang forever waiting on a stuck peer.
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), Arc::new(HangingHandler)).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let short = Duration::from_millis(100);
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        with_request_timeout(
            short,
            client.ae_entries(NodeId::from(1), SmolStr::new("users"), vec![3]),
        ),
    )
    .await
    .expect(
        "ae_entries must give up on its own short REQUEST_TIMEOUT override, well inside \
         this generous outer bound, not hang forever",
    );
    assert!(
        result.is_err(),
        "a handler whose entries never resolve must time out, not hang"
    );
}

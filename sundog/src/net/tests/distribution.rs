//! Distribution-mode request tests: fetch, load, and scoped or masked anti-entropy rounds.

use super::*;
use crate::net::request::with_request_timeout;

/// The kind of a [`CodecError::Io`]; any other error shape fails the test.
fn io_kind(err: &CodecError) -> io::ErrorKind {
    let CodecError::Io(source) = err else {
        panic!("expected a CodecError::Io, got {err:?}");
    };
    source.kind()
}

#[tokio::test]
async fn fetch_returns_the_responders_record_when_found() {
    let rec = sample_record(1);
    let handler = Arc::new(FixtureHandler {
        fetch_fixture: FetchFixture::Found(Some(rec.clone())),
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let outcome = client
        .fetch(
            NodeId::from(1),
            SmolStr::new("users"),
            Bytes::from_static(b"k1"),
            42,
        )
        .await
        .expect("fetch succeeds");
    assert_eq!(outcome, FetchOutcome::Found(Some(rec)));
}

#[tokio::test]
async fn load_returns_the_responders_records_and_uncached_values() {
    let rec = sample_record(1);
    let handler = Arc::new(FixtureHandler {
        load_fixture: LoadFixture::Loaded(
            vec![rec.clone()],
            vec![(Bytes::from_static(b"k2"), Bytes::from_static(b"v2"))],
        ),
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let outcome = client
        .load(
            NodeId::from(1),
            SmolStr::new("users"),
            vec![
                rec.key.clone(),
                Bytes::from_static(b"k2"),
                Bytes::from_static(b"k3"),
            ],
        )
        .await
        .expect("load succeeds");
    assert_eq!(
        outcome,
        LoadOutcome::Loaded {
            found: vec![rec],
            uncached: vec![(Bytes::from_static(b"k2"), Bytes::from_static(b"v2"))],
        }
    );
}

#[tokio::test]
async fn load_reports_the_responders_loader_failure() {
    let handler = Arc::new(FixtureHandler {
        load_fixture: LoadFixture::Failed("source unavailable"),
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let outcome = client
        .load(
            NodeId::from(1),
            SmolStr::new("users"),
            vec![Bytes::from_static(b"k1")],
        )
        .await
        .expect("the exchange succeeds");
    assert_eq!(
        outcome,
        LoadOutcome::Failed("source unavailable".to_string())
    );
}

#[tokio::test]
async fn load_is_declined_against_the_default_handler_body() {
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let outcome = client
        .load(
            NodeId::from(1),
            SmolStr::new("users"),
            vec![Bytes::from_static(b"k1")],
        )
        .await
        .expect("the exchange succeeds");
    assert_eq!(outcome, LoadOutcome::Declined);
}

#[tokio::test]
async fn load_refuses_to_dial_a_peer_older_than_protocol_load() {
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at_protocol(
        NodeId::from(1),
        server.local_addr(),
        wire::PROTOCOL_LOAD - 1,
    )]);

    let result = client
        .load(
            NodeId::from(1),
            SmolStr::new("users"),
            vec![Bytes::from_static(b"k1")],
        )
        .await;
    assert!(
        result.is_err(),
        "a peer older than PROTOCOL_LOAD is never sent a load request"
    );
}

#[tokio::test]
async fn fetch_returns_a_definitive_miss() {
    let handler = Arc::new(FixtureHandler {
        fetch_fixture: FetchFixture::Found(None),
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let outcome = client
        .fetch(
            NodeId::from(1),
            SmolStr::new("users"),
            Bytes::from_static(b"k1"),
            42,
        )
        .await
        .expect("fetch succeeds");
    assert_eq!(outcome, FetchOutcome::Found(None));
}

#[tokio::test]
async fn fetch_reports_stale_when_the_responders_view_hash_differs() {
    let handler = Arc::new(FixtureHandler {
        fetch_fixture: FetchFixture::Stale(999),
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let outcome = client
        .fetch(
            NodeId::from(1),
            SmolStr::new("users"),
            Bytes::from_static(b"k1"),
            1,
        )
        .await
        .expect("fetch succeeds");
    assert_eq!(
        outcome,
        FetchOutcome::Stale {
            responder_view_hash: 999
        }
    );
}

#[tokio::test]
async fn fetch_is_declined_against_the_default_handler_body() {
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let outcome = client
        .fetch(
            NodeId::from(1),
            SmolStr::new("users"),
            Bytes::from_static(b"k1"),
            1,
        )
        .await
        .expect("fetch succeeds even against a handler that never overrode it");
    assert_eq!(
        outcome,
        FetchOutcome::Declined,
        "the default RequestHandler::fetch body declines, so the requester tries the next owner"
    );
}

#[tokio::test]
async fn fetch_refuses_to_dial_a_peer_that_does_not_speak_distribution_mode() {
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at_protocol(
        NodeId::from(1),
        server.local_addr(),
        wire::PROTOCOL_DISTRIBUTED - 1,
    )]);

    let result = client
        .fetch(
            NodeId::from(1),
            SmolStr::new("users"),
            Bytes::from_static(b"k1"),
            1,
        )
        .await;
    let err = result.expect_err(
        "a peer that does not speak PROTOCOL_DISTRIBUTED must never be dialed for a fetch",
    );
    assert_eq!(
        io_kind(&err),
        io::ErrorKind::Unsupported,
        "the refusal is the kind that marks an owner too old for a distribution-mode fetch"
    );
}

/// A fetch to a node the peer table does not hold fails before any dial
/// with a `CodecError::Io` of kind `NotFound`, the kind `Cache::explain`
/// reports as an owner that is not a member.
#[tokio::test]
async fn fetch_to_an_unknown_peer_fails_with_not_found() {
    let (mesh, _inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;

    let err = mesh
        .fetch(
            NodeId::from(42),
            SmolStr::new("users"),
            Bytes::from_static(b"k1"),
            1,
        )
        .await
        .expect_err("a node outside the peer table is never dialed for a fetch");
    assert_eq!(
        io_kind(&err),
        io::ErrorKind::NotFound,
        "an unknown owner is told apart from an unreachable one"
    );
    mesh.shutdown().await;
}

/// A fetch to a peer that accepts and never answers gives up on the mesh's
/// own request timeout with a `CodecError::Io` of kind `TimedOut`, the
/// kind `Cache::explain` reports as an owner that timed out.
#[tokio::test]
async fn fetch_times_out_with_the_mesh_backstop_kind() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("listener has a local addr");
    let stalled_peer = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.expect("accept");
            // Every connection reads and never answers, whether it is the
            // persistent writer's or the fetch's: the failure mode
            // `REQUEST_TIMEOUT` bounds.
            tokio::spawn(async move {
                let mut framed = LengthDelimitedCodec::builder()
                    .max_frame_length(MAX_FRAME)
                    .new_framed(stream);
                while framed.next().await.is_some() {}
            });
        }
    });

    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), addr)]);

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        with_request_timeout(
            Duration::from_millis(100),
            client.fetch(
                NodeId::from(1),
                SmolStr::new("users"),
                Bytes::from_static(b"k1"),
                1,
            ),
        ),
    )
    .await
    .expect(
        "fetch must give up on its own short request timeout, well inside this generous \
         outer bound, not hang forever",
    );
    let err = result.expect_err("a peer that never answers yields no reply");
    assert_eq!(
        io_kind(&err),
        io::ErrorKind::TimedOut,
        "the mesh's request timeout surfaces as the TimedOut kind"
    );
    client.shutdown().await;
    stalled_peer.abort();
}

#[tokio::test]
async fn ae_round_scoped_returns_only_mismatched_buckets() {
    let entries = vec![(
        Bytes::from_static(b"k1"),
        Hlc {
            wall_ms: 5,
            logical: 0,
            node: NodeId::from(1),
        },
    )];
    let handler = Arc::new(FixtureHandler {
        ae_scoped_fixture: AeScopedFixture::Digests(vec![
            BucketDigest {
                bucket: 0,
                digest: 111,
            },
            BucketDigest {
                bucket: 1,
                digest: 222,
            },
        ]),
        bucket_entries: entries.clone(),
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let result = client
        .ae_round_scoped(
            NodeId::from(1),
            SmolStr::new("users"),
            1,
            vec![(0, 111), (1, 999)],
        )
        .await
        .expect("scoped ae round succeeds");
    assert_eq!(
        result,
        AeRoundOutcome::Mismatches(vec![AeMismatch::Bucket(1, kv(entries))])
    );
}

#[tokio::test]
async fn ae_round_scoped_reports_stale_when_the_responders_view_hash_differs() {
    let handler = Arc::new(FixtureHandler {
        ae_scoped_fixture: AeScopedFixture::Stale(777),
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let result = client
        .ae_round_scoped(NodeId::from(1), SmolStr::new("users"), 1, Vec::new())
        .await
        .expect("scoped ae round succeeds");
    assert_eq!(
        result,
        AeRoundOutcome::Stale {
            responder_view_hash: 777
        }
    );
}

#[tokio::test]
async fn ae_round_scoped_reports_no_mismatches_against_the_default_handler_body() {
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let result = client
        .ae_round_scoped(NodeId::from(1), SmolStr::new("users"), 1, vec![(0, 111)])
        .await
        .expect("scoped ae round succeeds even against a handler that never overrode it");
    assert_eq!(
        result,
        AeRoundOutcome::Mismatches(Vec::new()),
        "the default RequestHandler::ae_digest_scoped body degrades to no mismatches"
    );
}

#[tokio::test]
async fn ae_round_masked_answers_each_bucket_whose_masked_fold_differs_with_its_part_digests() {
    let digests: Vec<u64> = (1..=64).collect();
    let handler = Arc::new(FixtureHandler {
        ae_scoped_fixture: AeScopedFixture::Digests(Vec::new()),
        part_digests: vec![(0, digests.clone()), (1, digests.clone())],
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let result = client
        .ae_round_masked(
            NodeId::from(1),
            SmolStr::new("users"),
            1,
            vec![(0, 0b11, 1 ^ 2), (1, 0b1, 999)],
        )
        .await
        .expect("masked ae round succeeds");
    assert_eq!(
        result,
        AeRoundOutcome::Mismatches(vec![AeMismatch::PartDigests(1, digests)]),
        "bucket 0's fold over parts 0 and 1 matches; bucket 1's does not"
    );
}

#[tokio::test]
async fn ae_round_masked_reports_stale_and_degrades_against_the_default_handler_body() {
    let handler = Arc::new(FixtureHandler {
        ae_scoped_fixture: AeScopedFixture::Stale(777),
        ..Default::default()
    });
    let (stale, _stale_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (plain, _plain_inbound) = spawn_mesh(NodeId::from(3), empty_handler()).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![
        peer_at(NodeId::from(1), stale.local_addr()),
        peer_at(NodeId::from(3), plain.local_addr()),
    ]);

    let round = |peer| {
        client.ae_round_masked(
            NodeId::from(peer),
            SmolStr::new("users"),
            1,
            vec![(0, 1, 5)],
        )
    };
    assert_eq!(
        round(1).await.expect("masked ae round succeeds"),
        AeRoundOutcome::Stale {
            responder_view_hash: 777
        }
    );
    assert_eq!(
        round(3).await.expect("masked ae round succeeds"),
        AeRoundOutcome::Mismatches(Vec::new()),
        "the default RequestHandler::ae_digest_masked body degrades to no mismatches"
    );
}

#[tokio::test]
async fn ae_round_masked_refuses_to_dial_a_peer_that_does_not_speak_part_ownership() {
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at_protocol(
        NodeId::from(1),
        server.local_addr(),
        wire::PROTOCOL_PART_OWNERSHIP - 1,
    )]);

    let result = client
        .ae_round_masked(NodeId::from(1), SmolStr::new("users"), 1, Vec::new())
        .await;
    assert!(
        result.is_err(),
        "a peer that does not speak PROTOCOL_PART_OWNERSHIP is never sent a masked round"
    );
}

#[tokio::test]
async fn ae_round_scoped_refuses_to_dial_a_peer_that_does_not_speak_distribution_mode() {
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at_protocol(
        NodeId::from(1),
        server.local_addr(),
        wire::PROTOCOL_DISTRIBUTED - 1,
    )]);

    let result = client
        .ae_round_scoped(NodeId::from(1), SmolStr::new("users"), 1, Vec::new())
        .await;
    assert!(
        result.is_err(),
        "a peer that does not speak PROTOCOL_DISTRIBUTED must never be dialed for a scoped ae round"
    );
}

//! Peer table, outbox, listener and send-side tests.

use super::*;

#[tokio::test]
async fn a_bound_listener_holds_its_port_and_spawn_on_accepts_on_it() {
    let bound = BoundListener::bind("127.0.0.1:0".parse().expect("valid loopback addr"))
        .await
        .expect("bind loopback");
    let addr = bound.local_addr();
    assert_ne!(addr.port(), 0, "port 0 resolves to the assigned port");
    let err = std::net::TcpListener::bind(addr)
        .expect_err("the bound listener keeps its port from any other socket");
    assert_eq!(err.kind(), io::ErrorKind::AddrInUse);

    let greeting = Greeting {
        node: NodeId::from(1),
        incarnation: 1,
        cluster: wire::UNNAMED_CLUSTER,
    };
    let (mesh, _inbound) =
        Mesh::spawn_on(bound, greeting, &ClusterConfig::default(), empty_handler())
            .expect("spawn on the bound listener");
    assert_eq!(mesh.local_addr(), addr);
    tokio::net::TcpStream::connect(addr)
        .await
        .expect("the mesh accepts on the port the listener held");
}

#[tokio::test]
async fn hello_is_sent_first_on_a_new_persistent_connection() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let fake_peer_addr = listener.local_addr().expect("listener has a local addr");

    let (mesh, _inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    mesh.update_peers(vec![peer_at(NodeId::from(2), fake_peer_addr)]);

    let (stream, _) = listener.accept().await.expect("accept dial-in");
    let mut framed = LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME)
        .new_framed(stream);
    let frame = framed
        .next()
        .await
        .expect("frame arrives")
        .expect("no io error");
    let msg = wire::decode(&frame.freeze()).expect("decodes");
    assert_eq!(
        msg,
        Msg::Hello {
            node: NodeId::from(1),
            incarnation: 1,
            protocol: wire::PROTOCOL_VERSION,
            cluster: wire::UNNAMED_CLUSTER,
        }
    );
}

#[tokio::test]
async fn invalidate_overflow_drops_oldest_not_newest() {
    let handler = empty_handler();
    let config = ClusterConfig {
        outbox_capacity: 2,
        ..ClusterConfig::default()
    };
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("valid loopback addr");
    let (mesh, _inbound) = Mesh::spawn(addr, NodeId::from(1), 1, &config, handler)
        .await
        .expect("bind loopback");

    // A peer with no listener: the writer spins on connection failures, so
    // its outbox never drains.
    let dead_peer: SocketAddr = "127.0.0.1:1".parse().expect("valid unroutable addr");
    mesh.update_peers(vec![peer_at(NodeId::from(2), dead_peer)]);

    let key_of = |n: u8| Bytes::from(vec![n]);
    let msg = |n: u8| Msg::Invalidate {
        cache: SmolStr::new("users"),
        key: key_of(n),
        ver: Hlc {
            wall_ms: u64::from(n),
            logical: 0,
            node: NodeId::from(1),
        },
    };
    mesh.send(NodeId::from(2), MsgClass::Invalidate, msg(1));
    mesh.send(NodeId::from(2), MsgClass::Invalidate, msg(2));
    mesh.send(NodeId::from(2), MsgClass::Invalidate, msg(3)); // 1 must be dropped

    let invalidate = {
        let table = mesh.inner.peers.read().expect("lock");
        Arc::clone(
            &table
                .get(&NodeId::from(2))
                .expect("peer registered")
                .invalidate,
        )
    };
    let first = invalidate.pop().await;
    let second = invalidate.pop().await;
    assert_eq!(first.msg, msg(2));
    assert_eq!(second.msg, msg(3));
}

#[tokio::test]
async fn replicate_overflow_drops_newest_and_marks_peer_dirty() {
    let handler = empty_handler();
    let config = ClusterConfig {
        outbox_capacity: 1,
        ..ClusterConfig::default()
    };
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("valid loopback addr");
    let (mesh, _inbound) = Mesh::spawn(addr, NodeId::from(1), 1, &config, handler)
        .await
        .expect("bind loopback");

    let dead_peer: SocketAddr = "127.0.0.1:1".parse().expect("valid unroutable addr");
    mesh.update_peers(vec![peer_at(NodeId::from(2), dead_peer)]);

    let rec = |n: u8| Msg::Replicate {
        cache: SmolStr::new("users"),
        rec: sample_record(n),
    };
    mesh.send(NodeId::from(2), MsgClass::Replicate, rec(1));
    assert!(mesh.take_dirty_peers().is_empty(), "no overflow yet");

    mesh.send(NodeId::from(2), MsgClass::Replicate, rec(2)); // dropped: outbox full

    let dirty = mesh.take_dirty_peers();
    assert_eq!(dirty, vec![NodeId::from(2)]);
    assert!(
        mesh.take_dirty_peers().is_empty(),
        "dirty mark is cleared once taken"
    );
}

#[tokio::test]
async fn send_to_an_unknown_peer_is_a_silent_no_op() {
    let (mesh, _inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    mesh.send(
        NodeId::from(99),
        MsgClass::Invalidate,
        Msg::Invalidate {
            cache: SmolStr::new("users"),
            key: Bytes::new(),
            ver: Hlc {
                wall_ms: 0,
                logical: 0,
                node: NodeId::from(1),
            },
        },
    );
}

#[test]
fn defer_ae_digest_only_with_frames_queued_and_a_bounded_number_of_times() {
    assert!(!defer_ae_digest(0, 0), "an empty outbox is served");
    assert!(defer_ae_digest(1, 0));
    assert!(defer_ae_digest(5, MAX_AE_DEFERRALS - 1));
    assert!(
        !defer_ae_digest(5, MAX_AE_DEFERRALS),
        "the bound serves a round even with frames still queued"
    );
}

#[cfg(feature = "tls")]
fn no_tls() -> crate::net::TlsCtx {
    None
}
#[cfg(not(feature = "tls"))]
fn no_tls() -> crate::net::TlsCtx {
    TlsCtx
}

#[test]
fn record_bucket_ack_is_readable_by_cache_bucket_and_requester() {
    let mesh = MeshInner::for_tests(no_tls(), CancellationToken::new());
    mesh.record_bucket_ack(SmolStr::new("prices"), 7, NodeId::from(3), 42);
    assert!(
        mesh.acked_buckets
            .read()
            .expect("lock is never poisoned")
            .contains_key(&(SmolStr::new("prices"), 7, NodeId::from(3))),
        "the ack lands under its (cache, bucket, requester) key"
    );
    assert!(
        !mesh
            .acked_buckets
            .read()
            .expect("lock is never poisoned")
            .contains_key(&(SmolStr::new("prices"), 8, NodeId::from(3))),
        "a different bucket is not recorded"
    );
}

#[test]
fn acked_owners_only_counts_a_due_bucket_within_the_window() {
    let mesh = MeshInner::for_tests(no_tls(), CancellationToken::new());
    mesh.record_bucket_ack(SmolStr::new("prices"), 7, NodeId::from(3), 42);
    mesh.record_bucket_ack(SmolStr::new("prices"), 9, NodeId::from(4), 42);
    mesh.record_bucket_ack(SmolStr::new("other"), 7, NodeId::from(5), 42);
    let owners = mesh.acked_owners(&SmolStr::new("prices"), &[7], Duration::from_secs(60), 42);
    assert_eq!(
        owners,
        HashMap::from([(7u16, HashSet::from([NodeId::from(3)]))]),
        "only the acked owner of a bucket actually in `due`, for the right cache, counts, \
         keyed by the bucket it acked"
    );
    // Sleeps briefly so the ack is reliably older than a zero window.
    std::thread::sleep(Duration::from_millis(5));
    let none = mesh.acked_owners(&SmolStr::new("prices"), &[7], Duration::ZERO, 42);
    assert!(
        none.is_empty(),
        "an ack older than the window does not count as reconciled"
    );
}

#[test]
fn an_ack_carrying_a_stale_view_hash_does_not_count_as_reconciled() {
    let mesh = MeshInner::for_tests(no_tls(), CancellationToken::new());
    // Acked under view hash 1; this node's current view has moved to 2.
    mesh.record_bucket_ack(SmolStr::new("prices"), 7, NodeId::from(3), 1);
    let stale = mesh.acked_owners(&SmolStr::new("prices"), &[7], Duration::from_secs(60), 2);
    assert!(
        stale.is_empty(),
        "an ack from before a view change does not count as reconciled"
    );
    let current = mesh.acked_owners(&SmolStr::new("prices"), &[7], Duration::from_secs(60), 1);
    assert_eq!(
        current,
        HashMap::from([(7u16, HashSet::from([NodeId::from(3)]))]),
        "the same ack counts once the requested view hash matches the ack's own"
    );
}

#[tokio::test]
async fn peer_protocol_is_zero_for_an_unknown_peer_and_otherwise_the_gossiped_value() {
    let mesh_handle = Mesh {
        local_addr: "127.0.0.1:0".parse().expect("valid loopback addr"),
        inner: MeshInner::for_tests(no_tls(), CancellationToken::new()),
    };
    assert_eq!(mesh_handle.inner.peer_protocol(NodeId::from(9)), 0);
    mesh_handle.update_peers(vec![peer_at_protocol(
        NodeId::from(9),
        "127.0.0.1:1".parse().expect("valid loopback addr"),
        3,
    )]);
    assert_eq!(mesh_handle.inner.peer_protocol(NodeId::from(9)), 3);
}

#[tokio::test]
async fn mesh_acked_owners_delegates_to_the_inner_table() {
    let mesh_handle = Mesh {
        local_addr: "127.0.0.1:0".parse().expect("valid loopback addr"),
        inner: MeshInner::for_tests(no_tls(), CancellationToken::new()),
    };
    mesh_handle
        .inner
        .record_bucket_ack(SmolStr::new("prices"), 7, NodeId::from(3), 42);
    assert_eq!(
        mesh_handle.acked_owners(&SmolStr::new("prices"), &[7], Duration::from_secs(60), 42),
        HashMap::from([(7u16, HashSet::from([NodeId::from(3)]))])
    );
}

#[tokio::test]
async fn replicate_in_flight_reflects_queued_and_recent_frames() {
    let (mesh, _inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    // A peer nobody listens on: an enqueued frame stays queued forever.
    let unreachable = SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 1));
    mesh.update_peers(vec![peer_at(NodeId::from(2), unreachable)]);
    let window = Duration::from_millis(500);
    assert!(
        !mesh.replicate_in_flight(NodeId::from(2), window),
        "nothing sent yet"
    );
    assert!(
        !mesh.replicate_in_flight(NodeId::from(9), window),
        "an unknown peer is never in flight"
    );
    let frame = OutFrame::new(Msg::Replicate {
        cache: SmolStr::new("users"),
        rec: sample_record(1),
    })
    .expect("encodes");
    mesh.send_frames(NodeId::from(2), MsgClass::Replicate, [frame]);
    assert!(
        mesh.replicate_in_flight(NodeId::from(2), window),
        "a queued frame counts as in flight"
    );
    assert!(
        mesh.replicate_in_flight(NodeId::from(2), Duration::ZERO),
        "queued frames count regardless of the window"
    );
}

#[tokio::test]
async fn send_many_enqueued_answers_whether_every_message_reached_the_outbox() {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("valid loopback addr");
    let config = ClusterConfig {
        outbox_capacity: 2,
        ..ClusterConfig::default()
    };
    let (mesh, _inbound) = Mesh::spawn(addr, NodeId::from(1), 1, &config, empty_handler())
        .await
        .expect("bind loopback");
    let replicate = |key: u8| Msg::Replicate {
        cache: SmolStr::new("users"),
        rec: sample_record(key),
    };
    assert!(
        !mesh.send_many_enqueued(NodeId::from(2), MsgClass::Replicate, [replicate(1)]),
        "a peer missing from the table receives nothing"
    );

    // A peer nobody listens on: its writer never drains the outbox.
    let unreachable = SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 1));
    mesh.update_peers(vec![peer_at(NodeId::from(2), unreachable)]);
    assert!(
        mesh.send_many_enqueued(NodeId::from(2), MsgClass::Replicate, [replicate(1)]),
        "a known peer's outbox takes the message"
    );
    assert!(
        !mesh.send_many_enqueued(NodeId::from(2), MsgClass::Replicate, (2..=4).map(replicate)),
        "a message that meets a full outbox is dropped"
    );
}

#[tokio::test]
async fn mark_dirty_shows_up_in_the_next_take() {
    let (mesh, _inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    mesh.update_peers(vec![peer_at(
        NodeId::from(2),
        SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 1)),
    )]);
    let dirty = mesh.take_dirty_peers();
    assert!(dirty.is_empty(), "{dirty:?}");
    mesh.mark_dirty(NodeId::from(2));
    assert_eq!(mesh.take_dirty_peers(), vec![NodeId::from(2)]);
    assert!(mesh.take_dirty_peers().is_empty(), "taking clears the mark");
    mesh.mark_dirty(NodeId::from(42));
    assert!(
        mesh.take_dirty_peers().is_empty(),
        "an unknown peer is ignored"
    );
}

#[tokio::test]
async fn request_to_an_unknown_peer_errors_instead_of_hanging() {
    let (mesh, _inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    let err = mesh
        .ae_pull(NodeId::from(42), SmolStr::new("users"), Vec::new())
        .await;
    assert!(err.is_err());
}

#[tokio::test]
async fn inbound_broadcast_traffic_reaches_the_mpsc_receiver() {
    let (server, mut inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    let addr = server.local_addr();

    let sender = TcpStream::connect(addr).await.expect("connect");
    let mut framed = LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME)
        .new_framed(sender);
    let hello = wire::encode(&Msg::Hello {
        node: NodeId::from(2),
        incarnation: 1,
        protocol: wire::PROTOCOL_VERSION,
        cluster: wire::UNNAMED_CLUSTER,
    })
    .expect("encodes");
    futures::SinkExt::send(&mut framed, hello)
        .await
        .expect("send hello");

    let invalidate = Msg::Invalidate {
        cache: SmolStr::new("users"),
        key: Bytes::from_static(b"k1"),
        ver: Hlc {
            wall_ms: 1,
            logical: 0,
            node: NodeId::from(2),
        },
    };
    let encoded = wire::encode(&invalidate).expect("encodes");
    futures::SinkExt::send(&mut framed, encoded)
        .await
        .expect("send invalidate");

    let got = inbound.recv().await.expect("message forwarded");
    assert_eq!(
        got,
        InboundMsg {
            from: NodeId::from(2),
            msg: invalidate
        }
    );
}

#[tokio::test]
async fn update_peers_cancels_the_writer_of_a_dropped_peer() {
    let (mesh, _inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    let unreachable = SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 1));
    mesh.update_peers(vec![peer_at(NodeId::from(2), unreachable)]);

    let cancel = {
        let table = mesh.inner.peers.read().expect("lock");
        table
            .get(&NodeId::from(2))
            .expect("peer registered")
            .cancel
            .clone()
    };
    assert!(!cancel.is_cancelled(), "not cancelled while still a peer");

    // Dropping peer 2 from the incoming set removes its handle and
    // must cancel the writer task spawned for it.
    mesh.update_peers(Vec::new());
    assert!(
        cancel.is_cancelled(),
        "removing a peer must cancel its writer's token"
    );
    assert!(
        mesh.inner
            .peers
            .read()
            .expect("lock")
            .get(&NodeId::from(2))
            .is_none(),
        "the dropped peer's handle must leave the table"
    );
}

/// The value of `sundog_backlog_dropped_total{peer}` a recorder has seen.
fn backlog_dropped(snapshotter: &metrics_util::debugging::Snapshotter, peer: NodeId) -> u64 {
    snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, ..)| {
            key.key().name() == "sundog_backlog_dropped_total"
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "peer" && label.value() == peer.to_string())
        })
        .map(|(_, _, _, value)| match value {
            metrics_util::debugging::DebugValue::Counter(n) => n,
            _ => 0,
        })
        .sum()
}

/// Accepts one connection on `listener` and reads its `Hello`.
async fn accept_persistent(
    listener: &TcpListener,
) -> tokio_util::codec::Framed<TcpStream, LengthDelimitedCodec> {
    let (stream, _) = listener.accept().await.expect("accept dial-in");
    let mut framed = LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME)
        .new_framed(stream);
    let hello = framed.next().await.expect("hello").expect("no io error");
    assert!(matches!(
        wire::decode(&hello.freeze()).expect("decodes"),
        Msg::Hello { .. }
    ));
    framed
}

/// Polls `mesh.take_dirty_peers()` for up to `within`, answering what the
/// first non-empty take held.
async fn dirty_within(mesh: &Mesh, within: Duration) -> Vec<NodeId> {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let dirty = mesh.take_dirty_peers();
        if !dirty.is_empty() || tokio::time::Instant::now() >= deadline {
            return dirty;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn a_broken_connection_that_carried_frames_marks_the_peer_dirty() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let fake_peer_addr = listener.local_addr().expect("listener has a local addr");
    let (mesh, _inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    mesh.update_peers(vec![peer_at(NodeId::from(2), fake_peer_addr)]);

    let mut conn = accept_persistent(&listener).await;
    mesh.send(
        NodeId::from(2),
        MsgClass::Replicate,
        Msg::Replicate {
            cache: SmolStr::new("users"),
            rec: sample_record(1),
        },
    );
    conn.next().await.expect("the frame").expect("no io error");
    // The link outlives the redial backoff, then breaks.
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert!(
        mesh.take_dirty_peers().is_empty(),
        "a connection that is still up owes nothing"
    );
    drop(conn);

    assert_eq!(
        dirty_within(&mesh, Duration::from_secs(3)).await,
        vec![NodeId::from(2)],
        "the peer is marked once the writer has redialed"
    );
    assert!(mesh.take_dirty_peers().is_empty(), "one break, one mark");
}

#[tokio::test]
async fn an_idle_connection_close_leaves_the_peer_clean() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let fake_peer_addr = listener.local_addr().expect("listener has a local addr");
    let (mesh, _inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    mesh.update_peers(vec![peer_at(NodeId::from(2), fake_peer_addr)]);

    drop(accept_persistent(&listener).await);
    let _redialed = accept_persistent(&listener).await;

    assert!(
        dirty_within(&mesh, Duration::from_millis(500))
            .await
            .is_empty(),
        "a connection that carried nothing loses nothing"
    );
}

#[tokio::test]
async fn a_replicate_sent_into_a_closed_connection_is_redelivered_on_the_redial() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let fake_peer_addr = listener.local_addr().expect("listener has a local addr");
    let (mesh, _inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    mesh.update_peers(vec![peer_at(NodeId::from(2), fake_peer_addr)]);

    drop(accept_persistent(&listener).await);
    let replicate = |n: u8| Msg::Replicate {
        cache: SmolStr::new("users"),
        rec: sample_record(n),
    };
    mesh.send(NodeId::from(2), MsgClass::Replicate, replicate(1));
    tokio::time::sleep(Duration::from_millis(200)).await;
    mesh.send(NodeId::from(2), MsgClass::Replicate, replicate(2));

    let mut conn2 = accept_persistent(&listener).await;
    let delivered = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let frame = conn2.next().await.expect("frame").expect("no io error");
            if wire::decode(&frame.freeze()).expect("decodes") == replicate(2) {
                return;
            }
        }
    })
    .await;
    assert!(
        delivered.is_ok(),
        "the frame sent through Mesh::send reaches the peer's next connection"
    );
}

#[tokio::test]
async fn invalidate_overflow_marks_the_peer_dirty_and_counts_the_dropped_frame() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let config = ClusterConfig {
        outbox_capacity: 2,
        ..ClusterConfig::default()
    };
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("valid loopback addr");
    let (mesh, _inbound) = Mesh::spawn(addr, NodeId::from(1), 1, &config, empty_handler())
        .await
        .expect("bind loopback");
    // A peer nobody listens on: its outbox never drains.
    let dead_peer: SocketAddr = "127.0.0.1:1".parse().expect("valid unroutable addr");
    mesh.update_peers(vec![peer_at(NodeId::from(2), dead_peer)]);
    let invalidate = |n: u8| Msg::Invalidate {
        cache: SmolStr::new("users"),
        key: Bytes::from(vec![n]),
        ver: sample_record(n).ver,
    };

    mesh.send_many(
        NodeId::from(2),
        MsgClass::Invalidate,
        [invalidate(1), invalidate(2)],
    );
    assert!(
        mesh.take_dirty_peers().is_empty(),
        "room in the outbox: nothing dropped"
    );
    assert_eq!(backlog_dropped(&snapshotter, NodeId::from(2)), 0);

    mesh.send(NodeId::from(2), MsgClass::Invalidate, invalidate(3));

    assert_eq!(
        mesh.take_dirty_peers(),
        vec![NodeId::from(2)],
        "an invalidation lost to overflow steers the next anti-entropy round"
    );
    assert_eq!(backlog_dropped(&snapshotter, NodeId::from(2)), 1);
}

#[tokio::test]
async fn a_message_that_cannot_be_encoded_marks_the_peer_dirty_and_is_counted() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let (mesh, _inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    let dead_peer: SocketAddr = "127.0.0.1:1".parse().expect("valid unroutable addr");
    mesh.update_peers(vec![peer_at(NodeId::from(2), dead_peer)]);
    let too_large = Msg::Replicate {
        cache: SmolStr::new("users"),
        rec: WireRecord {
            value: Some(Bytes::from(vec![0u8; MAX_FRAME + 1])),
            ..sample_record(1)
        },
    };

    assert!(
        !mesh.send_many_enqueued(NodeId::from(2), MsgClass::Replicate, [too_large]),
        "a message that does not encode is not enqueued"
    );

    assert_eq!(mesh.take_dirty_peers(), vec![NodeId::from(2)]);
    assert_eq!(backlog_dropped(&snapshotter, NodeId::from(2)), 1);
}

#[tokio::test]
async fn note_unsent_marks_a_known_peer_and_ignores_zero_frames_and_unknown_peers() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let (mesh, _inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    let dead_peer: SocketAddr = "127.0.0.1:1".parse().expect("valid unroutable addr");
    mesh.update_peers(vec![peer_at(NodeId::from(2), dead_peer)]);

    mesh.note_unsent(NodeId::from(2), 0);
    mesh.note_unsent(NodeId::from(9), 4);
    assert_eq!(mesh.take_dirty_peers(), Vec::<NodeId>::new());
    assert_eq!(backlog_dropped(&snapshotter, NodeId::from(9)), 0);

    mesh.note_unsent(NodeId::from(2), 3);
    assert_eq!(mesh.take_dirty_peers(), vec![NodeId::from(2)]);
    assert_eq!(backlog_dropped(&snapshotter, NodeId::from(2)), 3);
}

#[tokio::test]
async fn a_forwarded_write_sent_awaiting_into_a_closed_connection_reaches_the_redial() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let owner_addr = listener.local_addr().expect("listener has a local addr");
    let (mesh, _inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    mesh.update_peers(vec![peer_at(NodeId::from(2), owner_addr)]);

    // The owner's link goes stale: closed, with the writer not yet aware.
    drop(accept_persistent(&listener).await);
    let forward = |n: u8| {
        batch_forward(&SmolStr::new("users"), 7, 0, vec![sample_record(n)])
            .into_iter()
            .map(|msg| OutFrame::new(msg).expect("encodes"))
            .collect::<Vec<_>>()
    };
    mesh.send_frames_awaiting(NodeId::from(2), forward(1)).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    mesh.send_frames_awaiting(NodeId::from(2), forward(2)).await;

    let mut conn2 = accept_persistent(&listener).await;
    let wanted = forward(2).remove(0).msg;
    let delivered = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let frame = conn2.next().await.expect("frame").expect("no io error");
            if wire::decode(&frame.freeze()).expect("decodes") == wanted {
                return;
            }
        }
    })
    .await;
    assert!(
        delivered.is_ok(),
        "the only copy of a non-owner's write survives a stale owner link"
    );
}

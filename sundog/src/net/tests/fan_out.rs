//! Replicate batching and fan-out send tests.

use super::*;

#[test]
fn batch_replicate_splits_by_count_and_budget_and_keeps_a_singleton_plain() {
    let cache = SmolStr::new("users");
    let tiny = |i: u32| WireRecord {
        key: Bytes::from(i.to_le_bytes().to_vec()),
        value: Some(Bytes::from_static(b"v")),
        ver: Hlc {
            wall_ms: 1,
            logical: 0,
            node: NodeId::from(1),
        },
        expires_at_ms: None,
    };
    assert!(matches!(
        batch_replicate(&cache, vec![tiny(0)]).as_slice(),
        [Msg::Replicate { .. }]
    ));
    let msgs = batch_replicate(
        &cache,
        (0..u32::try_from(REPLICATE_BATCH_COUNT + 1).expect("fits"))
            .map(tiny)
            .collect(),
    );
    assert_eq!(
        msgs.len(),
        2,
        "one record past the count cap starts a second batch"
    );
    assert!(
        matches!(&msgs[0], Msg::ReplicateBatch { recs, .. } if recs.len() == REPLICATE_BATCH_COUNT)
    );
    assert!(matches!(&msgs[1], Msg::Replicate { .. }));
    let big = |i: u32| WireRecord {
        value: Some(Bytes::from(vec![0u8; REPLICATE_BATCH_BUDGET / 3])),
        ..tiny(i)
    };
    let msgs = batch_replicate(&cache, (0..4).map(big).collect());
    assert_eq!(
        msgs.len(),
        2,
        "the byte budget splits four third-budget records two and two"
    );
}

#[test]
fn batch_forward_stamps_every_chunk_and_keeps_a_singleton_a_forward_batch() {
    let cache = SmolStr::new("users");
    let tiny = |i: u32| WireRecord {
        key: Bytes::from(i.to_le_bytes().to_vec()),
        value: Some(Bytes::from_static(b"v")),
        ver: Hlc {
            wall_ms: 1,
            logical: 0,
            node: NodeId::from(1),
        },
        expires_at_ms: None,
    };
    assert!(
        matches!(
            batch_forward(&cache, 9, 1, vec![tiny(0)]).as_slice(),
            [Msg::ForwardBatch { view_hash: 9, hops: 1, recs, .. }] if recs.len() == 1
        ),
        "a lone record still travels as a ForwardBatch, never a plain Replicate"
    );
    let msgs = batch_forward(
        &cache,
        9,
        0,
        (0..u32::try_from(REPLICATE_BATCH_COUNT + 1).expect("fits"))
            .map(tiny)
            .collect(),
    );
    assert_eq!(msgs.len(), 2, "the count cap splits like batch_replicate");
    assert!(msgs.iter().all(|msg| matches!(
        msg,
        Msg::ForwardBatch {
            view_hash: 9,
            hops: 0,
            ..
        }
    )));
    assert!(
        matches!(&msgs[0], Msg::ForwardBatch { recs, .. } if recs.len() == REPLICATE_BATCH_COUNT)
    );
}

#[tokio::test]
async fn ae_pull_of_thousands_of_records_returns_every_record_through_batched_frames() {
    let records: Vec<WireRecord> = (0..3000u32)
        .map(|i| WireRecord {
            key: Bytes::from(i.to_le_bytes().to_vec()),
            value: Some(Bytes::from(vec![7u8; 16])),
            ver: Hlc {
                wall_ms: u64::from(i) + 1,
                logical: 0,
                node: NodeId::from(1),
            },
            expires_at_ms: None,
        })
        .collect();
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

    let bytes_before = bytes_sent_total();
    let got = client
        .ae_pull(
            NodeId::from(1),
            SmolStr::new("users"),
            vec![Bytes::from_static(b"any")],
        )
        .await
        .expect("ae_pull succeeds");
    let bytes = bytes_sent_total() - bytes_before;
    assert_eq!(got, records, "every pulled record arrives, in order");
    // The reply is `batch_replicate`'s frames plus `ReqDone`, counted
    // from the batching itself: the process-wide frame counter also
    // takes every concurrently running test's frames.
    let frames = batch_replicate(&SmolStr::new("users"), records).len() + 1;
    assert!(
        frames < 100,
        "a 3000-record pull reply travels as a few batch frames, not one per record: {frames}"
    );
    assert!(
        bytes > 0,
        "sending 3000 records' worth of frames must grow the byte counter"
    );
}

/// Records carried by every `Replicate`/`ReplicateBatch` message read
/// off `inbound` within `wait`.
async fn count_replicated(inbound: &mut mpsc::Receiver<InboundMsg>, wait: Duration) -> usize {
    let deadline = tokio::time::Instant::now() + wait;
    let mut count = 0usize;
    while let Ok(Some(msg)) = tokio::time::timeout_at(deadline, inbound.recv()).await {
        match msg.msg {
            Msg::Replicate { .. } => count += 1,
            Msg::ReplicateBatch { recs, .. } => count += recs.len(),
            _ => {}
        }
    }
    count
}

fn replicate_msgs(count: usize) -> Vec<Msg> {
    (0..count)
        .map(|i| Msg::Replicate {
            cache: SmolStr::new("users"),
            rec: WireRecord {
                key: Bytes::from(format!("k{i}")),
                value: Some(Bytes::from_static(b"v")),
                ver: Hlc {
                    wall_ms: 1,
                    logical: u32::try_from(i).expect("small"),
                    node: NodeId::from(1),
                },
                expires_at_ms: None,
            },
        })
        .collect()
}

#[tokio::test]
async fn shutdown_flushes_replicate_frames_queued_just_before_it() {
    let (receiver, mut inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    let (sender, _sender_inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    sender.update_peers(vec![peer_at(NodeId::from(2), receiver.local_addr())]);

    sender.send_many(NodeId::from(2), MsgClass::Replicate, replicate_msgs(2_000));
    sender.shutdown().await;

    let got = count_replicated(&mut inbound, Duration::from_secs(3)).await;
    assert_eq!(
        got, 2_000,
        "every frame queued before shutdown reaches the peer"
    );
    receiver.shutdown().await;
}

/// A mesh that names `cluster` in its `Hello` and checks every accepted
/// one against it, as `Cluster::build` spawns it.
async fn spawn_named_mesh(node: NodeId, cluster: &str) -> (Mesh, mpsc::Receiver<InboundMsg>) {
    let bound = BoundListener::bind("127.0.0.1:0".parse().expect("valid loopback addr"))
        .await
        .expect("bind loopback");
    let greeting = Greeting {
        node,
        incarnation: 1,
        cluster: wire::cluster_id(cluster),
    };
    Mesh::spawn_on(bound, greeting, &ClusterConfig::default(), empty_handler())
        .expect("spawn on the bound listener")
}

/// A node of another cluster that dials this node's address, left over
/// from a node of its own that held it before, gets nothing through:
/// its replication never reaches this cluster's inbound. A node of the
/// same cluster, and an unnamed peer standing in for one older than
/// protocol 6, still do.
#[tokio::test]
async fn an_accepted_connection_from_another_cluster_delivers_nothing() {
    let (receiver, mut inbound) = spawn_named_mesh(NodeId::from(2), "prices").await;
    let receiver_peer = peer_at(NodeId::from(2), receiver.local_addr());

    let (stranger, _) = spawn_named_mesh(NodeId::from(3), "sessions").await;
    stranger.update_peers(vec![receiver_peer.clone()]);
    stranger.send_many(NodeId::from(2), MsgClass::Replicate, replicate_msgs(10));
    assert_eq!(
        count_replicated(&mut inbound, Duration::from_secs(1)).await,
        0,
        "another cluster's replication is dropped at the handshake"
    );

    let (sibling, _) = spawn_named_mesh(NodeId::from(4), "prices").await;
    sibling.update_peers(vec![receiver_peer.clone()]);
    sibling.send_many(NodeId::from(2), MsgClass::Replicate, replicate_msgs(10));
    assert_eq!(
        count_replicated(&mut inbound, Duration::from_secs(3)).await,
        10
    );

    let (unnamed, _) = spawn_mesh(NodeId::from(5), empty_handler()).await;
    unnamed.update_peers(vec![receiver_peer]);
    unnamed.send_many(NodeId::from(2), MsgClass::Replicate, replicate_msgs(10));
    assert_eq!(
        count_replicated(&mut inbound, Duration::from_secs(3)).await,
        10,
        "a peer that names no cluster is accepted, as an older release is"
    );

    for mesh in [stranger, sibling, unnamed, receiver] {
        mesh.shutdown().await;
    }
}

#[tokio::test]
async fn send_frames_awaiting_waits_for_outbox_space_instead_of_dropping() {
    let (receiver, mut inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("valid loopback addr");
    let config = ClusterConfig {
        outbox_capacity: 4,
        ..ClusterConfig::default()
    };
    let (sender, _sender_inbound) = Mesh::spawn(addr, NodeId::from(1), 1, &config, empty_handler())
        .await
        .expect("bind loopback");
    sender.update_peers(vec![peer_at(NodeId::from(2), receiver.local_addr())]);

    let frames: Vec<OutFrame> = replicate_msgs(200)
        .into_iter()
        .map(|msg| OutFrame::new(msg).expect("encodes"))
        .collect();
    sender.send_frames_awaiting(NodeId::from(2), frames).await;
    sender.shutdown().await;

    let got = count_replicated(&mut inbound, Duration::from_secs(3)).await;
    assert_eq!(
        got, 200,
        "a four-slot outbox carries two hundred frames when the sender waits for space"
    );
    receiver.shutdown().await;
}

#[test]
fn fan_out_keep_waiting_only_for_a_live_peer() {
    assert!(
        fan_out_keep_waiting(true),
        "a live peer's frame is never dropped: keep waiting"
    );
    assert!(
        !fan_out_keep_waiting(false),
        "a peer gone from the table falls back to today's drop-and-count path"
    );
}

/// Pins that a live peer's stalled outbox keeps `send_frames_awaiting`
/// waiting past one `FAN_OUT_SEND_DEADLINE` instead of dropping frames.
#[tokio::test]
async fn send_frames_awaiting_waits_past_one_deadline_for_a_still_live_peer() {
    let config = ClusterConfig {
        outbox_capacity: 1,
        ..ClusterConfig::default()
    };
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("valid loopback addr");
    let (sender, _sender_inbound) = Mesh::spawn(addr, NodeId::from(1), 1, &config, empty_handler())
        .await
        .expect("bind loopback");

    // No listener behind this peer, so its outbox never drains.
    let dead_peer: SocketAddr = "127.0.0.1:1".parse().expect("valid unroutable addr");
    sender.update_peers(vec![peer_at(NodeId::from(2), dead_peer)]);

    // Fills the one-slot outbox so the call below has to wait.
    sender.send(
        NodeId::from(2),
        MsgClass::Replicate,
        Msg::Replicate {
            cache: SmolStr::new("users"),
            rec: sample_record(0),
        },
    );

    let frames: Vec<OutFrame> = replicate_msgs(3)
        .into_iter()
        .map(|msg| OutFrame::new(msg).expect("encodes"))
        .collect();
    let started = tokio::time::Instant::now();
    // Bounded past one deadline slice; the call should still be waiting.
    let outcome = tokio::time::timeout(
        FAN_OUT_SEND_DEADLINE * 2,
        sender.send_frames_awaiting(NodeId::from(2), frames),
    )
    .await;
    assert!(
        outcome.is_err(),
        "a live peer's frames are never dropped, so the wait outlives one deadline slice"
    );
    assert!(
        started.elapsed() >= FAN_OUT_SEND_DEADLINE,
        "the call keeps waiting past at least one full slice"
    );
    assert!(
        sender.take_dirty_peers().is_empty(),
        "a live peer's stalled outbox is never marked dirty, so nothing was dropped"
    );
}

/// Pins that removing the peer mid-wait drops its remaining frames
/// instead of waiting forever.
#[tokio::test]
async fn send_frames_awaiting_drops_once_the_peer_leaves_the_table_mid_wait() {
    let config = ClusterConfig {
        outbox_capacity: 1,
        ..ClusterConfig::default()
    };
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("valid loopback addr");
    let (sender, _sender_inbound) = Mesh::spawn(addr, NodeId::from(1), 1, &config, empty_handler())
        .await
        .expect("bind loopback");

    let dead_peer: SocketAddr = "127.0.0.1:1".parse().expect("valid unroutable addr");
    sender.update_peers(vec![peer_at(NodeId::from(2), dead_peer)]);

    // take_dirty_peers only reports peers still in the table, so this
    // reads the dirty flag directly off the handle instead.
    let dirty_flag = {
        let table = sender.inner.peers.read().expect("lock");
        Arc::clone(&table.get(&NodeId::from(2)).expect("peer registered").dirty)
    };

    sender.send(
        NodeId::from(2),
        MsgClass::Replicate,
        Msg::Replicate {
            cache: SmolStr::new("users"),
            rec: sample_record(0),
        },
    );

    let frames: Vec<OutFrame> = replicate_msgs(3)
        .into_iter()
        .map(|msg| OutFrame::new(msg).expect("encodes"))
        .collect();
    let wait_fut = sender.send_frames_awaiting(NodeId::from(2), frames);
    tokio::pin!(wait_fut);

    // Confirms the call is still pending before the peer is removed.
    let not_yet = tokio::time::timeout(Duration::from_millis(200), &mut wait_fut).await;
    assert!(
        not_yet.is_err(),
        "the one-slot outbox is still full; the call must still be waiting"
    );
    assert!(
        !dirty_flag.load(Ordering::Relaxed),
        "nothing dropped yet while the peer is still live"
    );

    sender.update_peers(vec![]);

    tokio::time::timeout(Duration::from_secs(3), &mut wait_fut)
        .await
        .expect("send_frames_awaiting returns once the peer is gone");

    // `dirty` is set only alongside the drop counter, so this proves
    // the remaining frames were dropped rather than waited on.
    assert!(
        dirty_flag.load(Ordering::Relaxed),
        "a peer that leaves mid-wait drops its remaining frames and is marked dirty"
    );
}

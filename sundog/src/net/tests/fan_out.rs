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

/// `count` invalidations of `key_len`-byte keys, versions `first..first + count`.
fn invalidate_msgs(first: usize, count: usize, key_len: usize) -> Vec<Msg> {
    (first..first + count)
        .map(|i| Msg::Invalidate {
            cache: SmolStr::new("users"),
            key: Bytes::from(vec![0u8; key_len]),
            ver: Hlc {
                wall_ms: 1,
                logical: u32::try_from(i).expect("small"),
                node: NodeId::from(1),
            },
        })
        .collect()
}

#[tokio::test]
async fn shutdown_flushes_invalidate_frames_queued_behind_a_slow_receiver() {
    const FRAMES: usize = 64;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("listener has a local addr");
    // A slow receiver: accepts, then reads nothing for 100 ms, so the
    // sender's socket buffers fill and its writer blocks mid-send.
    let reader = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut framed = LengthDelimitedCodec::builder()
            .max_frame_length(MAX_FRAME)
            .new_framed(stream);
        let _hello = framed.next().await.expect("hello arrives");
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut got = 0usize;
        while let Ok(Some(Ok(_))) =
            tokio::time::timeout(Duration::from_secs(2), framed.next()).await
        {
            got += 1;
        }
        got
    });

    let (sender, _sender_inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    sender.update_peers(vec![peer_at(NodeId::from(2), addr)]);
    // The first batch is popped whole by the writer and blocks mid-send
    // against the slow receiver; the second waits in the queue.
    sender.send_many(
        NodeId::from(2),
        MsgClass::Invalidate,
        invalidate_msgs(0, FRAMES / 2, 512 * 1024),
    );
    tokio::time::sleep(Duration::from_millis(10)).await;
    sender.send_many(
        NodeId::from(2),
        MsgClass::Invalidate,
        invalidate_msgs(FRAMES / 2, FRAMES / 2, 512 * 1024),
    );
    sender.shutdown().await;

    let got = reader.await.expect("reader finishes");
    assert_eq!(
        got, FRAMES,
        "every invalidate frame queued before shutdown reaches the peer, as a replicate frame does"
    );
}

/// One `WARN` event: its message and fields, rendered as text.
struct Warned {
    message: String,
    fields: HashMap<&'static str, String>,
}

/// Collects one event's fields.
struct EventFields(HashMap<&'static str, String>);

impl tracing::field::Visit for EventFields {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name(), format!("{value:?}"));
    }
}

/// A tracing layer that keeps every `WARN` event.
#[derive(Clone, Default)]
struct WarnLog(Arc<Mutex<Vec<Warned>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for WarnLog {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if *event.metadata().level() != tracing::Level::WARN {
            return;
        }
        let mut fields = EventFields(HashMap::new());
        event.record(&mut fields);
        let message = fields.0.remove("message").unwrap_or_default();
        self.0.lock().expect("lock").push(Warned {
            message,
            fields: fields.0,
        });
    }
}

impl WarnLog {
    /// Installs the layer as this thread's subscriber while the guard lives.
    fn capture() -> (Self, crate::cluster::test_support::ScopedSubscriber) {
        use tracing_subscriber::layer::SubscriberExt as _;
        let log = Self::default();
        let guard = crate::cluster::test_support::scoped_subscriber(
            tracing_subscriber::registry().with(log.clone()),
        );
        (log, guard)
    }

    /// The warnings of shutdown discarding queued frames, since the last call.
    fn taken_discards(&self) -> Vec<Warned> {
        std::mem::take(&mut *self.0.lock().expect("lock"))
            .into_iter()
            .filter(|warned| warned.message.contains("discards"))
            .collect()
    }
}

/// Shuts down a mesh whose only peer never connects, a writer in its
/// reconnect backoff, after `invalidate` invalidation frames and `replicate`
/// replicate frames were queued for it. Answers how long shutdown took and
/// the discard warnings it logged.
async fn shutdown_toward_a_peer_that_never_connects(
    invalidate: usize,
    replicate: usize,
) -> (Duration, Vec<Warned>) {
    let (warned, _log) = WarnLog::capture();
    let (sender, _sender_inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    // Nothing listens here, so the writer redials every `RECONNECT_BACKOFF`
    // and takes nothing from either outbox.
    let nobody: SocketAddr = "127.0.0.1:1".parse().expect("valid unroutable addr");
    sender.update_peers(vec![peer_at(NodeId::from(2), nobody)]);
    sender.send_many(
        NodeId::from(2),
        MsgClass::Invalidate,
        invalidate_msgs(0, invalidate, 16),
    );
    sender.send_many(
        NodeId::from(2),
        MsgClass::Replicate,
        replicate_msgs(replicate),
    );

    let started = tokio::time::Instant::now();
    tokio::time::timeout(OUTBOX_FLUSH_DEADLINE * 4, sender.shutdown())
        .await
        .expect("shutdown returns within its flush deadline plus slack");
    (started.elapsed(), warned.taken_discards())
}

/// A peer whose link is down for the whole wait holds shutdown to its
/// deadline and no longer, whichever outbox its frames wait in. The frames it
/// never took are discarded, and shutdown logs their count, once, for that
/// peer.
#[tokio::test]
async fn shutdown_gives_up_at_its_deadline_on_a_peer_that_never_connects_and_logs_the_discards() {
    for (invalidate, replicate) in [(5, 0), (0, 3), (5, 3)] {
        let (waited, discards) =
            shutdown_toward_a_peer_that_never_connects(invalidate, replicate).await;
        assert!(
            waited >= OUTBOX_FLUSH_DEADLINE,
            "{invalidate} invalidate and {replicate} replicate frames hold shutdown for its \
             whole deadline: {waited:?}"
        );
        assert_eq!(
            discards.len(),
            1,
            "one warning for the one peer: {invalidate} and {replicate}"
        );
        assert_eq!(
            discards[0].fields.get("peer"),
            Some(&NodeId::from(2).to_string())
        );
        assert_eq!(
            discards[0].fields.get("frames"),
            Some(&(invalidate + replicate).to_string()),
            "the warning counts frames, not bytes: {invalidate} and {replicate}"
        );
    }
}

/// Shutdown warns about discarded frames only for a peer that left some:
/// a peer that took every frame draws no warning.
#[tokio::test]
async fn shutdown_logs_no_discards_when_the_peer_took_every_frame() {
    let (warned, _log) = WarnLog::capture();
    let (receiver, mut inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    let (sender, _sender_inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    sender.update_peers(vec![peer_at(NodeId::from(2), receiver.local_addr())]);
    sender.send_many(NodeId::from(2), MsgClass::Replicate, replicate_msgs(20));
    sender.send_many(
        NodeId::from(2),
        MsgClass::Invalidate,
        invalidate_msgs(0, 20, 16),
    );
    sender.shutdown().await;

    assert_eq!(
        count_replicated(&mut inbound, Duration::from_secs(3)).await,
        20
    );
    assert!(
        warned.taken_discards().is_empty(),
        "nothing was left in either outbox"
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
fn idle_deadline_is_the_idle_time_from_now_or_none() {
    let now = tokio::time::Instant::now();
    assert_eq!(
        idle_deadline(SendBudget::Idle(Duration::from_secs(10)), now),
        Some(now + Duration::from_secs(10))
    );
    assert_eq!(idle_deadline(SendBudget::Unbounded, now), None);
    assert_eq!(
        idle_deadline(SendBudget::Idle(Duration::MAX), now),
        None,
        "an idle time past the clock's range waits for as long as the peer is live"
    );
}

#[test]
fn fan_out_room_wait_takes_one_slice_cut_at_the_deadline_and_none_once_passed_or_latched() {
    let now = tokio::time::Instant::now();
    let slice_end = now + FAN_OUT_SEND_DEADLINE;
    let near = Some(now + Duration::from_millis(300));
    let far = Some(now + FAN_OUT_SEND_DEADLINE * 5);
    let passed = Some(now);
    // (deadline, latched stalled, wait)
    let table = [
        (None, false, RoomWait::Until(slice_end)),
        (None, true, RoomWait::Until(slice_end)),
        (far, false, RoomWait::Until(slice_end)),
        (
            near,
            false,
            RoomWait::Until(now + Duration::from_millis(300)),
        ),
        (passed, false, RoomWait::TryOnly),
        (far, true, RoomWait::TryOnly),
        (near, true, RoomWait::TryOnly),
    ];
    for (deadline, stalled, wait) in table {
        assert_eq!(
            fan_out_room_wait(deadline, stalled, now),
            wait,
            "deadline {deadline:?}, stalled {stalled}"
        );
    }
}

#[test]
fn fan_out_no_room_waits_again_only_for_a_live_peer_before_its_deadline() {
    let now = tokio::time::Instant::now();
    let slice = RoomWait::Until(now);
    let left = Some(now + Duration::from_secs(1));
    let passed = Some(now);
    // (peer live, latched stalled, wait, deadline, outcome)
    let table = [
        (true, false, slice, None, NoRoom::WaitAgain),
        (true, true, slice, None, NoRoom::WaitAgain),
        (false, false, slice, None, NoRoom::Departed),
        (true, false, slice, left, NoRoom::WaitAgain),
        (true, false, slice, passed, NoRoom::Stalled),
        (true, true, slice, left, NoRoom::Stalled),
        (true, false, RoomWait::TryOnly, left, NoRoom::Stalled),
        (false, true, RoomWait::TryOnly, left, NoRoom::Departed),
        (false, false, slice, passed, NoRoom::Departed),
    ];
    for (live, stalled, wait, deadline, outcome) in table {
        assert_eq!(
            fan_out_no_room(live, stalled, wait, deadline, now),
            outcome,
            "live {live}, stalled {stalled}, wait {wait:?}, deadline {deadline:?}"
        );
    }
}

#[test]
fn stall_latch_sets_on_a_stall_clears_on_a_whole_batch_and_leaves_a_departure() {
    // (how the send ended, the writer took a frame since the latch, latch)
    let table = [
        (SendEnd::Stalled, false, Some(true)),
        (SendEnd::Stalled, true, Some(true)),
        (SendEnd::Placed, true, Some(false)),
        (SendEnd::Placed, false, None),
        (SendEnd::Departed, true, None),
        (SendEnd::Departed, false, None),
    ];
    for (end, taken_since, latch) in table {
        assert_eq!(
            stall_latch(end, taken_since),
            latch,
            "{end:?}, taken since {taken_since}"
        );
    }
}

/// Each counter's total over its series since the last checkpoint: a
/// snapshot reads the counters out, so each checkpoint takes one.
fn counter_totals(
    snapshotter: &metrics_util::debugging::Snapshotter,
) -> std::collections::HashMap<String, u64> {
    let mut totals = std::collections::HashMap::new();
    for (key, _, _, value) in snapshotter.snapshot().into_vec() {
        if let metrics_util::debugging::DebugValue::Counter(n) = value {
            *totals.entry(key.key().name().to_string()).or_default() += n;
        }
    }
    totals
}

/// `name`'s total in a [`counter_totals`] checkpoint.
fn total(totals: &std::collections::HashMap<String, u64>, name: &str) -> u64 {
    totals.get(name).copied().unwrap_or(0)
}

/// `count` one-record `ForwardBatch` frames, keys `0..count`.
fn forward_frames(count: u8) -> Vec<OutFrame> {
    (0..count)
        .flat_map(|n| batch_forward(&SmolStr::new("users"), 7, 0, vec![sample_record(n)]))
        .map(|msg| OutFrame::new(msg).expect("encodes"))
        .collect()
}

/// Records in the `ForwardBatch` frames `inbound` delivers within `wait`.
async fn count_forwarded(inbound: &mut mpsc::Receiver<InboundMsg>, wait: Duration) -> usize {
    let deadline = tokio::time::Instant::now() + wait;
    let mut count = 0usize;
    while let Ok(Some(msg)) = tokio::time::timeout_at(deadline, inbound.recv()).await {
        if let Msg::ForwardBatch { recs, .. } = msg.msg {
            count += recs.len();
        }
    }
    count
}

/// The `dirty` flag and the stall latch of `peer`'s handle in `mesh`.
fn peer_flags(mesh: &Mesh, peer: NodeId) -> (Arc<AtomicBool>, Arc<AtomicU64>) {
    let table = mesh.inner.peers.read().expect("lock");
    let handle = table.get(&peer).expect("peer registered");
    (Arc::clone(&handle.dirty), Arc::clone(&handle.stalled))
}

/// A mesh whose peer 2 is live in its table but never takes a frame: no
/// listener answers its address, so its writer never connects. Its outbox
/// has `capacity` slots, the first `filled` of them taken.
async fn mesh_with_a_stalled_peer(capacity: usize, filled: u8) -> Mesh {
    let config = ClusterConfig {
        outbox_capacity: capacity,
        ..ClusterConfig::default()
    };
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("valid loopback addr");
    let (mesh, _inbound) = Mesh::spawn(addr, NodeId::from(1), 1, &config, empty_handler())
        .await
        .expect("bind loopback");
    let dead_peer: SocketAddr = "127.0.0.1:1".parse().expect("valid unroutable addr");
    mesh.update_peers(vec![peer_at(NodeId::from(2), dead_peer)]);
    for n in 0..filled {
        mesh.send(
            NodeId::from(2),
            MsgClass::Replicate,
            Msg::Replicate {
                cache: SmolStr::new("users"),
                rec: sample_record(n),
            },
        );
    }
    mesh
}

/// Pins the bound on a live peer that stopped reading: a bounded send
/// places what fits, drops the rest once its budget runs out, marks the peer
/// dirty and latches it stalled, counted once; the next bounded send to it
/// waits for nothing.
#[tokio::test]
async fn a_bounded_send_drops_past_its_budget_and_latches_a_live_peer_that_stopped_reading() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let sender = mesh_with_a_stalled_peer(2, 1).await;
    let (dirty, stalled) = peer_flags(&sender, NodeId::from(2));
    let budget = Duration::from_millis(1_200);

    let started = tokio::time::Instant::now();
    let placed = tokio::time::timeout(
        Duration::from_secs(5),
        sender.send_frames_within(NodeId::from(2), forward_frames(3), SendBudget::Idle(budget)),
    )
    .await
    .expect("a bounded send ends at its budget, not never");
    let took = started.elapsed();
    assert_eq!(
        placed, 1,
        "the one free slot takes the first frame; the other two find no room"
    );
    assert!(
        took >= budget,
        "the send waits out its idle budget first: {took:?}"
    );
    assert!(dirty.load(Ordering::Relaxed), "a drop marks the peer dirty");
    assert!(
        stalled.load(Ordering::Relaxed) != 0,
        "the peer is latched stalled"
    );
    let totals = counter_totals(&snapshotter);
    assert_eq!(total(&totals, "sundog_fan_out_stalls_total"), 1);
    assert_eq!(total(&totals, "sundog_backlog_dropped_total"), 2);
    assert!(
        total(&totals, "sundog_fan_out_wait_seconds_total") >= 1,
        "the wait cut short at the budget counts its whole seconds too"
    );

    dirty.store(false, Ordering::Relaxed);
    let started = tokio::time::Instant::now();
    let placed = sender
        .send_frames_within(
            NodeId::from(2),
            forward_frames(2),
            SendBudget::Idle(Duration::from_secs(10)),
        )
        .await;
    assert_eq!(placed, 0, "the outbox is full and the peer latched");
    assert!(
        started.elapsed() < FAN_OUT_SEND_DEADLINE / 2,
        "a send to a latched peer takes free slots only, so it waits for nothing: {:?}",
        started.elapsed()
    );
    assert!(
        dirty.load(Ordering::Relaxed),
        "every drop marks the peer dirty"
    );
    assert!(stalled.load(Ordering::Relaxed) != 0, "the latch holds");
    assert_eq!(
        sender
            .send_frames_within(
                NodeId::from(2),
                Vec::new(),
                SendBudget::Idle(Duration::from_secs(10)),
            )
            .await,
        0
    );
    assert!(
        stalled.load(Ordering::Relaxed) != 0,
        "a send with no frames places none, so it does not clear the latch"
    );
    let totals = counter_totals(&snapshotter);
    assert_eq!(
        total(&totals, "sundog_fan_out_stalls_total"),
        0,
        "a stall is counted when the latch sets, not per batch it drops"
    );
    assert_eq!(total(&totals, "sundog_backlog_dropped_total"), 2);
}

#[test]
fn a_frame_s_resident_bytes_count_its_encoding_its_message_and_each_record() {
    let rec = sample_record(1);
    let per_record = std::mem::size_of::<WireRecord>()
        + rec.key.len()
        + rec.value.as_ref().map_or(0, Bytes::len);
    let base = std::mem::size_of::<Msg>();
    for n in [1u8, 10, 100] {
        let frame = OutFrame::new(
            batch_forward(
                &SmolStr::new("users"),
                7,
                0,
                (0..n).map(|_| sample_record(1)).collect(),
            )
            .remove(0),
        )
        .expect("encodes");
        assert_eq!(
            frame.resident_bytes(),
            base + frame.frame.len() + usize::from(n) * per_record,
            "{n} records"
        );
    }
    let single = OutFrame::new(Msg::Replicate {
        cache: SmolStr::new("users"),
        rec,
    })
    .expect("encodes");
    assert_eq!(
        single.resident_bytes(),
        base + single.frame.len() + per_record
    );
    let invalidation = OutFrame::new(Msg::Invalidate {
        cache: SmolStr::new("users"),
        key: Bytes::from_static(b"abc"),
        ver: sample_record(1).ver,
    })
    .expect("encodes");
    assert_eq!(
        invalidation.resident_bytes(),
        base + invalidation.frame.len() + 3
    );
    let small = OutFrame::new(
        batch_forward(
            &SmolStr::new("users"),
            7,
            0,
            (0..100).map(|_| sample_record(1)).collect(),
        )
        .remove(0),
    )
    .expect("encodes");
    assert!(
        small.resident_bytes() > 3 * small.frame.len(),
        "small records' decoded copies outweigh their encoding several times over"
    );
}

/// A mesh whose peer 2 never takes a frame, under an outbox of 100 frame
/// slots and room in bytes for `frames` of [`forward_frames`]' frames.
async fn mesh_with_a_byte_bounded_stalled_peer(frames: usize) -> Mesh {
    let frame_len = forward_frames(1)[0].resident_bytes();
    let config = ClusterConfig {
        outbox_capacity: 100,
        outbox_bytes: frame_len * frames,
        ..ClusterConfig::default()
    };
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("valid loopback addr");
    let (mesh, _inbound) = Mesh::spawn(addr, NodeId::from(1), 1, &config, empty_handler())
        .await
        .expect("bind loopback");
    let dead_peer: SocketAddr = "127.0.0.1:1".parse().expect("valid unroutable addr");
    mesh.update_peers(vec![peer_at(NodeId::from(2), dead_peer)]);
    mesh
}

/// Pins the byte bound on the drop-newest path: once a peer's queued frames
/// fill `outbox_bytes`, the next is dropped and counted, though frame slots
/// are left.
#[tokio::test]
async fn a_replicate_send_drops_past_the_outbox_byte_budget_with_frame_slots_free() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let sender = mesh_with_a_byte_bounded_stalled_peer(3).await;
    let (dirty, _stalled) = peer_flags(&sender, NodeId::from(2));

    assert!(
        !sender.send_frames(NodeId::from(2), MsgClass::Replicate, forward_frames(5)),
        "two of the five frames find no room"
    );
    assert!(dirty.load(Ordering::Relaxed), "a drop marks the peer dirty");
    assert_eq!(
        total(
            &counter_totals(&snapshotter),
            "sundog_backlog_dropped_total"
        ),
        2,
        "three frames fill the byte budget and the other two are dropped"
    );
    let table = sender.inner.peers.read().expect("lock");
    let handle = table.get(&NodeId::from(2)).expect("peer registered");
    assert_eq!(
        handle.replicate_tx.max_capacity() - handle.replicate_tx.capacity(),
        3,
        "97 of 100 frame slots are still free"
    );
}

/// Pins the byte bound on the waiting path: a fan-out send places what the
/// byte budget holds and then waits for bytes as it waits for slots, so a
/// peer that takes nothing stalls it.
#[tokio::test]
async fn a_bounded_send_waits_for_outbox_bytes_and_stalls_on_a_peer_that_takes_nothing() {
    let sender = mesh_with_a_byte_bounded_stalled_peer(3).await;
    let (_dirty, stalled) = peer_flags(&sender, NodeId::from(2));

    let placed = tokio::time::timeout(
        Duration::from_secs(5),
        sender.send_frames_within(
            NodeId::from(2),
            forward_frames(5),
            SendBudget::Idle(Duration::from_millis(300)),
        ),
    )
    .await
    .expect("the send ends at its idle budget");
    assert_eq!(placed, 3, "the byte budget holds three frames");
    assert!(
        stalled.load(Ordering::Relaxed) != 0,
        "a peer that takes nothing latches"
    );
}

/// Pins that leftover byte room is not a peer reading: a latched peer that
/// takes nothing keeps its latch when a batch small enough for the room
/// left fits whole, so the next batch waits for nothing either.
#[tokio::test]
async fn a_small_batch_fitting_a_latched_peer_s_leftover_room_keeps_the_latch() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let frame_len = forward_frames(1)[0].resident_bytes();
    let small = OutFrame::new(Msg::Invalidate {
        cache: SmolStr::new("users"),
        key: Bytes::from_static(b"k"),
        ver: sample_record(1).ver,
    })
    .expect("encodes");
    assert!(small.resident_bytes() < frame_len / 2);
    let config = ClusterConfig {
        outbox_capacity: 100,
        outbox_bytes: frame_len * 3 + frame_len / 2,
        ..ClusterConfig::default()
    };
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("valid loopback addr");
    let (sender, _inbound) = Mesh::spawn(addr, NodeId::from(1), 1, &config, empty_handler())
        .await
        .expect("bind loopback");
    let dead_peer: SocketAddr = "127.0.0.1:1".parse().expect("valid unroutable addr");
    sender.update_peers(vec![peer_at(NodeId::from(2), dead_peer)]);
    let (dirty, stalled) = peer_flags(&sender, NodeId::from(2));
    let idle = || SendBudget::Idle(Duration::from_millis(300));

    let placed = sender
        .send_frames_within(NodeId::from(2), forward_frames(5), idle())
        .await;
    assert_eq!(placed, 3, "half a frame's room is left");
    assert!(stalled.load(Ordering::Relaxed) != 0, "the peer latches");
    dirty.store(false, Ordering::Relaxed);

    let placed = sender
        .send_frames_within(NodeId::from(2), vec![small], idle())
        .await;
    assert_eq!(placed, 1, "the small batch fits the room left whole");
    assert!(
        stalled.load(Ordering::Relaxed) != 0,
        "the writer took nothing, so the latch holds"
    );
    assert!(
        !dirty.load(Ordering::Relaxed),
        "a latch that holds marks nothing again"
    );
    let started = tokio::time::Instant::now();
    sender
        .send_frames_within(NodeId::from(2), forward_frames(1), idle())
        .await;
    assert!(
        started.elapsed() < Duration::from_millis(250),
        "the next batch waits for nothing: {:?}",
        started.elapsed()
    );
    assert_eq!(
        total(&counter_totals(&snapshotter), "sundog_fan_out_stalls_total"),
        1,
        "one stall, never cleared and set again"
    );
}

/// Pins how a latch clears: once the peer's writer has taken a frame since
/// the latch was set, a bounded send that places every frame clears it and
/// marks the peer dirty again, since the anti-entropy round the stall's mark
/// steered may have run while the peer could not answer; a whole batch to a
/// peer not latched marks nothing.
#[tokio::test]
async fn a_bounded_send_that_places_every_frame_clears_the_latch_and_marks_the_peer_again() {
    let (receiver, mut inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    let (sender, _sender_inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    sender.update_peers(vec![peer_at(NodeId::from(2), receiver.local_addr())]);
    let (_dirty, stalled) = peer_flags(&sender, NodeId::from(2));
    let budget = || SendBudget::Idle(Duration::from_secs(10));
    // Latched before the writer took its first frame, which it then takes.
    stalled.store(1, Ordering::Relaxed);
    sender
        .send_frames_within(NodeId::from(2), forward_frames(1), budget())
        .await;
    assert_eq!(
        count_forwarded(&mut inbound, Duration::from_secs(3)).await,
        1
    );
    stalled.store(1, Ordering::Relaxed);
    let _ = sender.take_dirty_peers();

    let placed = sender
        .send_frames_within(NodeId::from(2), forward_frames(3), budget())
        .await;
    assert_eq!(placed, 3, "an outbox with room takes every frame");
    assert_eq!(stalled.load(Ordering::Relaxed), 0, "the latch clears");
    assert_eq!(sender.take_dirty_peers(), vec![NodeId::from(2)]);
    assert_eq!(
        count_forwarded(&mut inbound, Duration::from_secs(3)).await,
        3
    );

    let placed = sender
        .send_frames_within(NodeId::from(2), forward_frames(2), budget())
        .await;
    assert_eq!(placed, 2);
    assert!(
        sender.take_dirty_peers().is_empty(),
        "a whole batch to a peer not latched drops nothing and marks nothing"
    );
    assert_eq!(
        sender
            .send_frames_within(NodeId::from(9), forward_frames(1), budget())
            .await,
        0,
        "a peer not in the table takes none"
    );
    for mesh in [sender, receiver] {
        mesh.shutdown().await;
    }
}

#[test]
fn outboxes_pending_while_either_outbox_holds_a_frame() {
    // (replicate free slots, replicate capacity, invalidate length, pending)
    let table = [
        (8192, 8192, 0, false),
        (8191, 8192, 0, true),
        (0, 8192, 0, true),
        (8192, 8192, 1, true),
        (8191, 8192, 5, true),
        (0, 8192, 8192, true),
        (1, 1, 0, false),
        (0, 1, 0, true),
    ];
    for (free, cap, invalidate, pending) in table {
        assert_eq!(
            outboxes_pending(free, cap, invalidate),
            pending,
            "free {free} of {cap}, {invalidate} invalidations queued"
        );
    }
}

#[test]
fn queued_frames_counts_the_replicate_slots_in_use_and_the_invalidations() {
    // (replicate free slots, replicate capacity, invalidate length, frames)
    let table = [
        (8192, 8192, 0, 0),
        (8190, 8192, 0, 2),
        (8192, 8192, 7, 7),
        (8190, 8192, 7, 9),
        (0, 8192, 0, 8192),
        (0, 8192, 8192, 16384),
        // More free slots than capacity cannot happen; it counts as none in use.
        (10, 4, 3, 3),
    ];
    for (free, cap, invalidate, frames) in table {
        assert_eq!(
            queued_frames(free, cap, invalidate),
            frames,
            "free {free} of {cap}, {invalidate} invalidations queued"
        );
        assert_eq!(
            outboxes_pending(free, cap, invalidate),
            frames > 0,
            "pending exactly when a frame is queued: free {free} of {cap}, {invalidate} queued"
        );
    }
}

/// Pins that the bound is on silence, not on the whole send: a peer whose
/// outbox keeps taking frames, however slowly, is waited on past the idle
/// time for as long as the send takes, and nothing is dropped or latched.
#[tokio::test]
async fn a_bounded_send_keeps_waiting_while_a_slow_peer_keeps_taking_frames() {
    // A peer that reads its connection at 2 MiB/s, however its reads split.
    const READ_RATE: u64 = 2 * 1024 * 1024;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let peer_addr = listener.local_addr().expect("listener has a local addr");
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("the writer dials the peer");
        let mut buf = vec![0u8; 64 * 1024];
        let started = tokio::time::Instant::now();
        let mut total = 0u64;
        while let Ok(read) = tokio::io::AsyncReadExt::read(&mut stream, &mut buf).await
            && read > 0
        {
            total += u64::try_from(read).expect("a read fits in u64");
            let due = Duration::from_micros(total * 1_000_000 / READ_RATE);
            tokio::time::sleep_until(started + due).await;
        }
    });
    let config = ClusterConfig {
        outbox_capacity: 1,
        ..ClusterConfig::default()
    };
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("valid loopback addr");
    let (sender, _inbound) = Mesh::spawn(addr, NodeId::from(1), 1, &config, empty_handler())
        .await
        .expect("bind loopback");
    sender.update_peers(vec![peer_at(NodeId::from(2), peer_addr)]);
    let (dirty, stalled) = peer_flags(&sender, NodeId::from(2));
    let _ = sender.take_dirty_peers();
    // 24 MiB: well past what the loopback socket buffers hold, so the send
    // runs for seconds at the peer's pace, each frame half a second.
    let frames: Vec<OutFrame> = (0..24u8)
        .flat_map(|n| {
            let rec = WireRecord {
                value: Some(Bytes::from(vec![n; 1024 * 1024])),
                ..sample_record(n)
            };
            batch_forward(&SmolStr::new("users"), 7, 0, vec![rec])
        })
        .map(|msg| OutFrame::new(msg).expect("encodes"))
        .collect();
    let idle = Duration::from_secs(3);

    let started = tokio::time::Instant::now();
    let placed = tokio::time::timeout(
        Duration::from_secs(120),
        sender.send_frames_within(NodeId::from(2), frames, SendBudget::Idle(idle)),
    )
    .await
    .expect("a peer that keeps reading takes the whole send");
    let took = started.elapsed();
    assert_eq!(placed, 24, "every frame goes in");
    assert!(
        took > idle,
        "the send outlasts its idle time, so what it bounds is silence: {took:?}"
    );
    assert!(
        stalled.load(Ordering::Relaxed) == 0,
        "a reading peer is not latched"
    );
    assert!(!dirty.load(Ordering::Relaxed), "nothing was dropped");
}

/// Pins that a live peer's stalled outbox keeps `send_frames_awaiting`
/// waiting past one `FAN_OUT_SEND_DEADLINE` instead of dropping frames, a
/// stall latch set by a bounded send notwithstanding, and counts the slice
/// that timed out in `sundog_fan_out_wait_seconds_total`.
#[tokio::test]
async fn send_frames_awaiting_waits_past_one_deadline_for_a_still_live_peer() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
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

    let (_dirty, stalled) = peer_flags(&sender, NodeId::from(2));
    stalled.store(1, Ordering::Relaxed);

    let frames: Vec<OutFrame> = replicate_msgs(3)
        .into_iter()
        .map(|msg| OutFrame::new(msg).expect("encodes"))
        .collect();
    let started = tokio::time::Instant::now();
    // Bounded past one deadline slice and short of a second; the call
    // should still be waiting.
    let outcome = tokio::time::timeout(
        FAN_OUT_SEND_DEADLINE + FAN_OUT_SEND_DEADLINE / 2,
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
    assert_eq!(
        total(
            &counter_totals(&snapshotter),
            "sundog_fan_out_wait_seconds_total"
        ),
        FAN_OUT_SEND_DEADLINE.as_secs(),
        "the one slice that timed out is counted by the seconds it waited"
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

/// The latch holds one past the frames the writer had taken when it set, so
/// zero stays the clear latch, and a send skips waiting only while the
/// writer has taken nothing since.
#[test]
fn the_latch_marks_the_frames_taken_and_is_waited_out_only_while_the_writer_takes_none() {
    assert_eq!(latch_mark(0), 1);
    assert_eq!(latch_mark(5), 6);
    assert_eq!(latch_mark(u64::MAX), u64::MAX, "the mark saturates");
    // (latch, frames taken, a frame taken since the latch, waited out)
    let table = [
        (0, 0, false, false),
        (0, 9, false, false),
        (1, 0, false, true),
        (6, 5, false, true),
        (6, 6, true, false),
        (6, 9, true, false),
    ];
    for (latched_at, taken, since, waited_out) in table {
        assert_eq!(
            taken_since_latch(latched_at, taken),
            since,
            "latch {latched_at}, taken {taken}"
        );
        assert_eq!(
            waits_out_latch(latched_at, taken),
            waited_out,
            "latch {latched_at}, taken {taken}"
        );
    }
}

/// Pins the latch's baseline for a writer that took frames before its peer
/// stopped reading: the latch sets one past them, a whole batch with nothing
/// taken since keeps it, a stall again moves it to the frames taken by then
/// without counting a second stall, and it clears, marking the peer again,
/// only once the writer has taken a frame past the baseline.
#[test]
fn settle_stall_latch_counts_from_the_frames_the_writer_had_taken() {
    use crate::net::mesh::settle_stall_latch;

    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let peer = NodeId::from(2);
    let (stalled, dirty) = (AtomicU64::new(0), AtomicBool::new(false));

    settle_stall_latch(peer, SendEnd::Stalled, 3, 5, &stalled, &dirty);
    assert_eq!(stalled.load(Ordering::Relaxed), 6, "one past five taken");
    settle_stall_latch(peer, SendEnd::Placed, 0, 5, &stalled, &dirty);
    assert_eq!(
        stalled.load(Ordering::Relaxed),
        6,
        "nothing taken since, so a whole batch keeps it"
    );
    settle_stall_latch(peer, SendEnd::Stalled, 1, 8, &stalled, &dirty);
    assert_eq!(
        stalled.load(Ordering::Relaxed),
        9,
        "a stall again moves the baseline"
    );
    settle_stall_latch(peer, SendEnd::Placed, 0, 8, &stalled, &dirty);
    assert_eq!(stalled.load(Ordering::Relaxed), 9);
    assert!(
        !dirty.load(Ordering::Relaxed),
        "a latch that holds marks nothing"
    );
    settle_stall_latch(peer, SendEnd::Placed, 0, 9, &stalled, &dirty);
    assert_eq!(stalled.load(Ordering::Relaxed), 0, "the latch clears");
    assert!(
        dirty.load(Ordering::Relaxed),
        "clearing marks the peer again"
    );
    assert_eq!(
        total(&counter_totals(&snapshotter), "sundog_fan_out_stalls_total"),
        1,
        "counted once, when it first set"
    );

    settle_stall_latch(peer, SendEnd::Stalled, 2, 9, &stalled, &dirty);
    settle_stall_latch(peer, SendEnd::Departed, 4, 12, &stalled, &dirty);
    assert_eq!(
        stalled.load(Ordering::Relaxed),
        10,
        "a departure leaves the latch alone"
    );
}

/// A one-record `ForwardBatch` frame for key `n` whose value is `len` bytes.
fn forward_frame_of(n: u8, len: usize) -> OutFrame {
    let rec = WireRecord {
        value: Some(Bytes::from(vec![n; len])),
        ..sample_record(n)
    };
    let msg = batch_forward(&SmolStr::new("users"), 7, 0, vec![rec])
        .pop()
        .expect("one record, one frame");
    OutFrame::new(msg).expect("encodes")
}

/// A sender with `config` whose peer 2 reads its connection at `rate` bytes
/// a second, however its reads split.
async fn mesh_with_a_slow_reader(config: &ClusterConfig, rate: u64) -> Mesh {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let peer_addr = listener.local_addr().expect("listener has a local addr");
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("the writer dials the peer");
        let mut buf = vec![0u8; 64 * 1024];
        let started = tokio::time::Instant::now();
        let mut total = 0u64;
        while let Ok(read) = tokio::io::AsyncReadExt::read(&mut stream, &mut buf).await
            && read > 0
        {
            total += u64::try_from(read).expect("a read fits in u64");
            let due = Duration::from_micros(total * 1_000_000 / rate);
            tokio::time::sleep_until(started + due).await;
        }
    });
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("valid loopback addr");
    let (sender, _inbound) = Mesh::spawn(addr, NodeId::from(1), 1, config, empty_handler())
        .await
        .expect("bind loopback");
    sender.update_peers(vec![peer_at(NodeId::from(2), peer_addr)]);
    sender
}

/// Pins that the idle wait measures the writer's silence: a frame that
/// weighs four of the frames queued ahead of it waits for the writer to take
/// four of them, two seconds at the peer's pace, past the idle time, and
/// goes in because each frame the writer takes renews the wait.
#[tokio::test]
async fn a_heavy_frame_waits_while_the_writer_keeps_taking_the_frames_ahead_of_it() {
    let light = forward_frame_of(0, 1024 * 1024).resident_bytes();
    let config = ClusterConfig {
        outbox_capacity: 100,
        outbox_bytes: light * 6,
        ..ClusterConfig::default()
    };
    // 2 MiB/s: a 1 MiB frame half a second.
    let sender = mesh_with_a_slow_reader(&config, 2 * 1024 * 1024).await;
    let (dirty, stalled) = peer_flags(&sender, NodeId::from(2));
    let _ = sender.take_dirty_peers();
    // 20 MiB ahead fills the loopback socket buffers and then the outbox.
    let mut frames: Vec<OutFrame> = (0..20u8)
        .map(|n| forward_frame_of(n, 1024 * 1024))
        .collect();
    let heavy = forward_frame_of(20, 3_900 * 1024);
    assert!(heavy.resident_bytes() > light * 7 / 2);
    frames.push(heavy);
    frames.push(forward_frame_of(21, 1024 * 1024));

    let placed = tokio::time::timeout(
        Duration::from_secs(120),
        sender.send_frames_within(
            NodeId::from(2),
            frames,
            SendBudget::Idle(Duration::from_millis(1_250)),
        ),
    )
    .await
    .expect("a peer that keeps reading takes the whole send");
    assert_eq!(placed, 22, "every frame goes in, the heavy one too");
    assert_eq!(
        stalled.load(Ordering::Relaxed),
        0,
        "a reading peer is not latched"
    );
    assert!(!dirty.load(Ordering::Relaxed), "nothing was dropped");
}

/// The frames `peer`'s writer has taken from its replicate outbox.
fn writer_taken(mesh: &Mesh, peer: NodeId) -> u64 {
    let table = mesh.inner.peers.read().expect("lock");
    table
        .get(&peer)
        .expect("peer registered")
        .replicate_tx
        .taken()
}

/// Pins that a latched peer whose writer reads again is waited on: a batch
/// heavier than the whole byte budget places whole as the writer drains the
/// outbox, which clears the latch and marks the peer again, where a send
/// that took only the room free at once would drop the rest every time.
#[tokio::test]
async fn a_latched_peer_that_reads_again_takes_a_batch_heavier_than_its_outbox() {
    let (receiver, mut inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    let frame_len = forward_frames(1)[0].resident_bytes();
    let config = ClusterConfig {
        outbox_capacity: 100,
        outbox_bytes: frame_len * 2,
        ..ClusterConfig::default()
    };
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("valid loopback addr");
    let (sender, _sender_inbound) = Mesh::spawn(addr, NodeId::from(1), 1, &config, empty_handler())
        .await
        .expect("bind loopback");
    sender.update_peers(vec![peer_at(NodeId::from(2), receiver.local_addr())]);
    let (_dirty, stalled) = peer_flags(&sender, NodeId::from(2));
    let budget = || SendBudget::Idle(Duration::from_secs(10));
    // Latched as a stall leaves it, then the writer takes a frame.
    stalled.store(
        latch_mark(writer_taken(&sender, NodeId::from(2))),
        Ordering::Relaxed,
    );
    sender
        .send_frames_within(NodeId::from(2), forward_frames(1), budget())
        .await;
    assert_eq!(
        count_forwarded(&mut inbound, Duration::from_secs(3)).await,
        1
    );
    let _ = sender.take_dirty_peers();

    let placed = sender
        .send_frames_within(NodeId::from(2), forward_frames(6), budget())
        .await;
    assert_eq!(placed, 6, "the batch places whole as the writer drains");
    assert_eq!(stalled.load(Ordering::Relaxed), 0, "the latch clears");
    assert_eq!(sender.take_dirty_peers(), vec![NodeId::from(2)]);
    assert_eq!(
        count_forwarded(&mut inbound, Duration::from_secs(3)).await,
        6
    );
    for mesh in [sender, receiver] {
        mesh.shutdown().await;
    }
}

/// Pins the invalidate queue's byte bound: past `outbox_bytes`, a push
/// drops the oldest queued frames until the new one fits, counts each in
/// `sundog_backlog_dropped_total` and marks the peer dirty.
#[tokio::test]
async fn an_invalidate_send_past_the_byte_budget_drops_the_oldest_and_counts_each() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let small = forward_frame_of(0, 100);
    let config = ClusterConfig {
        outbox_capacity: 100,
        outbox_bytes: small.resident_bytes() * 3,
        ..ClusterConfig::default()
    };
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("valid loopback addr");
    let (sender, _inbound) = Mesh::spawn(addr, NodeId::from(1), 1, &config, empty_handler())
        .await
        .expect("bind loopback");
    let dead_peer: SocketAddr = "127.0.0.1:1".parse().expect("valid unroutable addr");
    sender.update_peers(vec![peer_at(NodeId::from(2), dead_peer)]);
    let (dirty, _stalled) = peer_flags(&sender, NodeId::from(2));

    sender.send_frames(
        NodeId::from(2),
        MsgClass::Invalidate,
        (0..3).map(|_| small.clone()),
    );
    assert!(!dirty.load(Ordering::Relaxed), "three fit");
    let heavy = forward_frame_of(1, 1000);
    assert!(heavy.resident_bytes() > small.resident_bytes() * 2);
    sender.send_frames(NodeId::from(2), MsgClass::Invalidate, [heavy]);

    assert!(dirty.load(Ordering::Relaxed));
    assert_eq!(
        total(
            &counter_totals(&snapshotter),
            "sundog_backlog_dropped_total"
        ),
        3,
        "a frame over twice the size of each queued one drops all three"
    );
    let table = sender.inner.peers.read().expect("lock");
    let queue = &table.get(&NodeId::from(2)).expect("peer").invalidate;
    assert_eq!(queue.len(), 1);
}

/// A detached record holds bytes of its own: neither its key nor its value
/// points into the buffer the record was sliced from, which a frame it
/// travels on would otherwise keep alive whole.
#[test]
fn a_detached_record_owns_its_bytes() {
    let buffer = Bytes::from(vec![7u8; 4096]);
    let rec = WireRecord {
        key: buffer.slice(0..8),
        value: Some(buffer.slice(8..64)),
        ..sample_record(1)
    };
    let within = |bytes: &Bytes| buffer.as_ptr_range().contains(&bytes.as_ptr());
    assert!(within(&rec.key));

    let detached = rec.detached();
    assert_eq!(detached, rec);
    assert!(!within(&detached.key));
    assert!(!within(detached.value.as_ref().expect("a value")));
}

/// Pins that a frame keeps its place in the queue for the outbox's bytes
/// from one wait slice to the next: a heavy frame that has waited across a
/// slice boundary since a light frame started waiting still takes the bytes
/// the writer frees first, so a stream of light frames cannot starve it.
#[tokio::test(start_paused = true)]
async fn a_waiting_frame_keeps_its_place_for_the_outbox_bytes_across_slices() {
    let light = forward_frame_of(0, 100);
    let weight = light.resident_bytes();
    let heavy = forward_frame_of(9, weight * 2);
    assert!(
        heavy.resident_bytes() > weight * 3,
        "it needs the outbox empty"
    );
    let mesh = mesh_with_a_stalled_peer(100, 0).await;
    // An outbox whose writer this test plays.
    let (tx, mut rx) = crate::net::replicate_outbox(100, weight * 4);
    mesh.inner
        .peers
        .write()
        .expect("lock")
        .get_mut(&NodeId::from(2))
        .expect("peer registered")
        .replicate_tx = tx;
    for _ in 0..4 {
        assert!(mesh.send_frames(NodeId::from(2), MsgClass::Replicate, [light.clone()]));
    }

    let start = tokio::time::Instant::now();
    let heavy_send = tokio::spawn({
        let mesh = mesh.clone();
        async move {
            mesh.send_frames_within(NodeId::from(2), vec![heavy], SendBudget::Unbounded)
                .await
        }
    });
    // The heavy frame's first slice ends before the light frames start
    // waiting, and its second ends after they started and before their
    // first does, when the writer frees the bytes.
    tokio::time::sleep_until(start + FAN_OUT_SEND_DEADLINE + Duration::from_millis(100)).await;
    let light_send = tokio::spawn({
        let mesh = mesh.clone();
        let lights: Vec<OutFrame> = (1..4u8).map(|n| forward_frame_of(n, 100)).collect();
        async move {
            mesh.send_frames_within(NodeId::from(2), lights, SendBudget::Unbounded)
                .await
        }
    });
    tokio::time::sleep_until(start + FAN_OUT_SEND_DEADLINE * 2 + Duration::from_millis(50)).await;

    let mut order = Vec::new();
    for _ in 0..8 {
        let frame = tokio::time::timeout(Duration::from_secs(30), rx.recv())
            .await
            .expect("a frame within the timeout")
            .expect("the sends keep the outbox open");
        order.push(frame.resident_bytes() > weight * 3);
    }
    assert_eq!(
        order,
        [false, false, false, false, true, false, false, false],
        "the heavy frame goes in as soon as the four ahead of it leave"
    );
    assert_eq!(heavy_send.await.expect("no panic"), 1);
    assert_eq!(light_send.await.expect("no panic"), 3);
}

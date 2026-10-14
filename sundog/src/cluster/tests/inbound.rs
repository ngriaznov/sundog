//! The inbound loop: a wedged peer stalls neither it nor shutdown, and the re-forward lanes behind it.

use std::sync::Mutex;

use futures::SinkExt as _;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use super::background::wire_record_for_u32;
use super::*;
use crate::cluster::inbound::inbound_loop;
use crate::hlc::Hlc;
use crate::net::test_support::{BucketPullHandler, peer_at};
use crate::wire::{Msg, WireRecord};
use bytes::Bytes;

/// One apply call a [`ViewShard`] received, with its record count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Applied {
    /// `apply_remote_batch`: replication, anti-entropy, state transfer.
    Replicated(usize),
    /// `apply_forwarded_batch`: the records of `ForwardBatch` messages.
    Forwarded(usize),
}

/// A [`ShardOps`] double with an ownership view that records every
/// invalidation and apply call it receives. Everything else panics if called.
struct ViewShard {
    view: Arc<OwnershipView>,
    invalidated: Arc<Mutex<Vec<Bytes>>>,
    applied: Arc<Mutex<Vec<Applied>>>,
}

impl ShardOps for ViewShard {
    fn apply_remote(&self, _rec: WireRecord) -> futures::future::BoxFuture<'_, ()> {
        Box::pin(async {})
    }
    fn apply_remote_batch(&self, recs: Vec<WireRecord>) -> futures::future::BoxFuture<'_, ()> {
        self.applied
            .lock()
            .expect("fixture mutex is never poisoned")
            .push(Applied::Replicated(recs.len()));
        Box::pin(async {})
    }
    fn apply_forwarded_batch(&self, recs: Vec<WireRecord>) -> futures::future::BoxFuture<'_, ()> {
        self.applied
            .lock()
            .expect("fixture mutex is never poisoned")
            .push(Applied::Forwarded(recs.len()));
        Box::pin(async {})
    }
    fn invalidate(&self, key: Bytes, _ver: Hlc) -> futures::future::BoxFuture<'_, ()> {
        self.invalidated
            .lock()
            .expect("fixture mutex is never poisoned")
            .push(key);
        Box::pin(async {})
    }
    fn ownership_view(&self) -> Option<Arc<OwnershipView>> {
        Some(Arc::clone(&self.view))
    }
    fn ownership_view_hash(&self) -> Option<u64> {
        Some(self.view.view_hash())
    }
    fn digests(&self) -> futures::future::BoxFuture<'_, Vec<crate::store::BucketDigest>> {
        unimplemented!()
    }
    fn bucket_entries(
        &self,
        _bucket: u16,
    ) -> futures::future::BoxFuture<'_, Vec<crate::store::KeyVersion>> {
        unimplemented!()
    }
    fn entries_for_buckets(
        &self,
        _buckets: Vec<u16>,
    ) -> futures::future::BoxFuture<'_, crate::store::BucketEntries> {
        unimplemented!()
    }
    fn bucket_lens(
        &self,
        _buckets: Vec<u16>,
    ) -> futures::future::BoxFuture<'_, Vec<crate::store::BucketLen>> {
        unimplemented!()
    }
    fn part_digests(
        &self,
        _buckets: Vec<u16>,
    ) -> futures::future::BoxFuture<'_, Vec<crate::store::BucketPartDigests>> {
        unimplemented!()
    }
    fn entries_for_parts(
        &self,
        _parts: Vec<crate::store::BucketPart>,
    ) -> futures::future::BoxFuture<'_, crate::store::PartEntries> {
        unimplemented!()
    }
    fn records_for(&self, _keys: Vec<Bytes>) -> futures::future::BoxFuture<'_, Vec<WireRecord>> {
        unimplemented!()
    }
    fn snapshot_chunks(&self) -> futures::stream::BoxStream<'static, Vec<WireRecord>> {
        unimplemented!()
    }
    fn gc_tombstones(&self, _any_member_absent: bool) -> futures::future::BoxFuture<'_, ()> {
        unimplemented!()
    }
    fn run_pending_tasks(&self) -> futures::future::BoxFuture<'_, ()> {
        unimplemented!()
    }
    fn close_spill(&self) {}
    fn held_buckets(&self) -> futures::future::BoxFuture<'_, Vec<u16>> {
        unimplemented!()
    }
}

/// An unreachable address: a peer there is live in the table while its
/// writer never connects, so its outbox never drains.
fn wedged_addr() -> SocketAddr {
    "127.0.0.1:1".parse().expect("valid addr")
}

fn shard_registry(view: &Arc<OwnershipView>) -> (ShardRegistry, Arc<Mutex<Vec<Bytes>>>) {
    let (shards, invalidated, _applied) = recording_registry(view);
    (shards, invalidated)
}

/// A registry holding a [`ViewShard`], with the keys it was invalidated
/// for and the apply calls it received.
type RecordingRegistry = (
    ShardRegistry,
    Arc<Mutex<Vec<Bytes>>>,
    Arc<Mutex<Vec<Applied>>>,
);

/// [`shard_registry`] plus the log of apply calls the shard receives.
fn recording_registry(view: &Arc<OwnershipView>) -> RecordingRegistry {
    let invalidated = Arc::new(Mutex::new(Vec::new()));
    let applied = Arc::new(Mutex::new(Vec::new()));
    let shard: Arc<dyn ShardOps> = Arc::new(ViewShard {
        view: Arc::clone(view),
        invalidated: Arc::clone(&invalidated),
        applied: Arc::clone(&applied),
    });
    let shards: ShardRegistry = Arc::new(RwLock::new(HashMap::new()));
    shards
        .write()
        .expect("lock")
        .insert(SmolStr::new("users"), shard);
    (shards, invalidated, applied)
}

fn small_outbox() -> ClusterConfig {
    ClusterConfig {
        outbox_capacity: 2,
        ..ClusterConfig::default()
    }
}

/// A `ForwardBatch` of `records` stamped with a view hash other than `view`'s.
fn stale_batch(view: &OwnershipView, from: NodeId, records: Vec<WireRecord>) -> InboundMsg {
    InboundMsg {
        from,
        msg: Msg::ForwardBatch {
            cache: SmolStr::new("users"),
            view_hash: view.view_hash() ^ 1,
            hops: 0,
            recs: records,
        },
    }
}

fn invalidate_of(from: NodeId, key: &'static [u8]) -> InboundMsg {
    InboundMsg {
        from,
        msg: Msg::Invalidate {
            cache: SmolStr::new("users"),
            key: Bytes::from_static(key),
            ver: Hlc {
                wall_ms: 1,
                logical: 0,
                node: from,
            },
        },
    }
}

/// Whether `key` shows up in `invalidated` within `within`.
async fn applied_within(
    invalidated: &Arc<Mutex<Vec<Bytes>>>,
    key: &'static [u8],
    within: Duration,
) -> bool {
    let key = Bytes::from_static(key);
    tokio::time::timeout(within, async {
        loop {
            if invalidated.lock().expect("lock").contains(&key) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .is_ok()
}

/// An inbound loop on node 1 of a three-node view whose peer 3 is live and
/// wedged, fed through `tx`.
struct WedgedLoop {
    tx: mpsc::Sender<InboundMsg>,
    invalidated: Arc<Mutex<Vec<Bytes>>>,
    view: Arc<OwnershipView>,
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

impl WedgedLoop {
    const SELF: u64 = 1;
    const SENDER: u64 = 2;
    const WEDGED: u64 = 3;

    async fn start() -> Self {
        let view = Arc::new(OwnershipView::compute(
            NodeId::from(Self::SELF),
            vec![
                NodeId::from(Self::SELF),
                NodeId::from(Self::SENDER),
                NodeId::from(Self::WEDGED),
            ],
            std::num::NonZeroU8::new(2).expect("nonzero"),
        ));
        let (shards, invalidated) = shard_registry(&view);
        let (mesh, _unused_inbound) = Mesh::spawn(
            "127.0.0.1:0".parse().expect("valid loopback addr"),
            NodeId::from(Self::SELF),
            1,
            &small_outbox(),
            Arc::new(BucketPullHandler::default()),
        )
        .await
        .expect("bind loopback");
        mesh.update_peers(vec![peer_at(NodeId::from(Self::WEDGED), wedged_addr())]);
        let (tx, rx) = mpsc::channel(64);
        let cancel = CancellationToken::new();
        let task = tokio::spawn(inbound_loop(
            shards,
            Arc::new(InboundActivity::default()),
            mesh,
            NodeId::from(Self::SELF),
            rx,
            cancel.clone(),
        ));
        Self {
            tx,
            invalidated,
            view,
            cancel,
            task,
        }
    }

    /// Sends `count` stale batches of 200 records from the sender.
    async fn flood(&self, count: usize) {
        let records: Vec<WireRecord> = (0u32..200).map(wire_record_for_u32).collect();
        for _ in 0..count {
            self.tx
                .send(stale_batch(
                    &self.view,
                    NodeId::from(Self::SENDER),
                    records.clone(),
                ))
                .await
                .expect("inbound open");
        }
    }
}

/// One live peer whose outbox is full must not stop the single inbound
/// consumer from applying other traffic: an unrelated `Invalidate` queued
/// behind stale-view `ForwardBatch`es is applied promptly.
#[tokio::test]
async fn a_wedged_live_peer_does_not_stall_the_inbound_loop_on_a_stale_view_reforward() {
    let rig = WedgedLoop::start().await;
    rig.flood(8).await;
    rig.tx
        .send(invalidate_of(
            NodeId::from(WedgedLoop::SENDER),
            b"unrelated",
        ))
        .await
        .expect("inbound open");

    let applied = applied_within(&rig.invalidated, b"unrelated", Duration::from_secs(1)).await;

    rig.cancel.cancel();
    rig.task.abort();
    assert!(
        applied,
        "an Invalidate for another key is applied within 1s while one live peer's outbox is full"
    );
}

/// A cancel ends the loop at once while a live peer is wedged, so
/// `Cluster::shutdown` does not wait for the failure detector to remove the
/// peer.
#[tokio::test]
async fn the_inbound_loop_returns_promptly_on_cancel_with_a_wedged_live_peer() {
    let rig = WedgedLoop::start().await;
    rig.flood(8).await;
    rig.tx
        .send(invalidate_of(
            NodeId::from(WedgedLoop::SENDER),
            b"unrelated",
        ))
        .await
        .expect("inbound open");
    assert!(
        applied_within(&rig.invalidated, b"unrelated", Duration::from_secs(1)).await,
        "the loop has taken the whole burst"
    );

    rig.cancel.cancel();

    tokio::time::timeout(Duration::from_secs(2), rig.task)
        .await
        .expect("the loop exits within the lane drain deadline, not at the failure detector")
        .expect("the loop did not panic");
}

/// A raw peer connection into `mesh`'s data port, past `Hello`.
async fn raw_peer(addr: SocketAddr, node: NodeId) -> Framed<TcpStream, LengthDelimitedCodec> {
    let stream = TcpStream::connect(addr).await.expect("connect");
    let mut conn = LengthDelimitedCodec::builder()
        .max_frame_length(crate::wire::MAX_FRAME)
        .new_framed(stream);
    let hello = Msg::Hello {
        node,
        incarnation: 1,
        protocol: crate::wire::PROTOCOL_VERSION,
        cluster: crate::wire::UNNAMED_CLUSTER,
    };
    conn.send(crate::wire::encode(&hello).expect("encodes"))
        .await
        .expect("hello sent");
    conn
}

/// A record with a `len`-byte value, so a few batches fill the sockets.
fn fat_record(key: u32, len: usize) -> WireRecord {
    WireRecord {
        value: Some(Bytes::from(vec![7u8; len])),
        ..wire_record_for_u32(key)
    }
}

/// Two nodes that each re-forward to the other, over real meshes with tiny
/// outboxes and inbound channels: each keeps applying what reaches it.
/// Waiting on the other's outbox from the inbound loop made the two a
/// circular wait.
#[tokio::test]
async fn two_nodes_re_forwarding_at_each_other_keep_applying_inbound_traffic() {
    let (node_a, node_b, writer) = (NodeId::from(1), NodeId::from(2), NodeId::from(3));
    let eligible = vec![node_a, node_b, writer];
    let k = std::num::NonZeroU8::new(2).expect("nonzero");
    let mut nodes = Vec::new();
    for node in [node_a, node_b] {
        let view = Arc::new(OwnershipView::compute(node, eligible.clone(), k));
        let (shards, invalidated) = shard_registry(&view);
        let (mesh, inbound) = Mesh::spawn(
            "127.0.0.1:0".parse().expect("valid loopback addr"),
            node,
            1,
            &small_outbox(),
            Arc::new(BucketPullHandler::default()),
        )
        .await
        .expect("bind loopback");
        nodes.push((node, view, shards, invalidated, mesh, inbound));
    }
    let addr_of = |index: usize| nodes[index].4.local_addr();
    let (addr_a, addr_b) = (addr_of(0), addr_of(1));
    nodes[0].4.update_peers(vec![peer_at(node_b, addr_b)]);
    nodes[1].4.update_peers(vec![peer_at(node_a, addr_a)]);

    let cancel = CancellationToken::new();
    let mut loops = Vec::new();
    let mut seen = Vec::new();
    let mut views = Vec::new();
    for (node, view, shards, invalidated, mesh, inbound) in nodes {
        loops.push(tokio::spawn(inbound_loop(
            shards,
            Arc::new(InboundActivity::default()),
            mesh,
            node,
            inbound,
            cancel.clone(),
        )));
        seen.push(invalidated);
        views.push(view);
    }

    // The writer floods each node with batches routed under a stale view.
    let mut floods = Vec::new();
    for (addr, view) in [
        (addr_a, Arc::clone(&views[0])),
        (addr_b, Arc::clone(&views[1])),
    ] {
        floods.push(tokio::spawn(async move {
            let mut conn = raw_peer(addr, writer).await;
            let records: Vec<WireRecord> =
                (0u32..40).map(|key| fat_record(key, 32 * 1024)).collect();
            for _ in 0..12 {
                let batch = stale_batch(&view, writer, records.clone());
                conn.send(crate::wire::encode(&batch.msg).expect("encodes"))
                    .await
                    .expect("batch sent");
            }
            let marker = invalidate_of(writer, b"marker");
            conn.send(crate::wire::encode(&marker.msg).expect("encodes"))
                .await
                .expect("marker sent");
            conn
        }));
    }

    let mut conns = Vec::new();
    for flood in floods {
        conns.push(flood.await.expect("flood task ran"));
    }
    for invalidated in &seen {
        assert!(
            applied_within(invalidated, b"marker", Duration::from_secs(20)).await,
            "a node whose inbound loop waited on the other's outbox never applies the marker"
        );
    }
    drop(conns);
    cancel.cancel();
    for task in loops {
        tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .expect("the loop exits on cancel")
            .expect("the loop did not panic");
    }
}

mod reforwarder {
    //! The per-peer lanes the loop re-forwards through.

    use tokio::time::Instant;

    use super::*;
    use crate::cluster::inbound::{
        REFORWARD_LANE_BYTES, Reforwarder, lane_admits, lane_retired, warn_due,
    };
    use crate::net::{MsgClass, OutFrame};

    const SELF: u64 = 1;
    const WEDGED: u64 = 3;
    const HEALTHY: u64 = 4;

    /// A one-record `ForwardBatch` frame for `key`, as a re-forward sends.
    fn forward_frame(key: u32) -> OutFrame {
        OutFrame::new(Msg::ForwardBatch {
            cache: SmolStr::new("users"),
            view_hash: 7,
            hops: 1,
            recs: vec![wire_record_for_u32(key)],
        })
        .expect("encodes")
    }

    /// A mesh on node 1 with outboxes of two frames and the wedged peer 3 in
    /// its table, that peer's outbox already full.
    async fn mesh_with_a_full_wedged_peer() -> Mesh {
        let (mesh, _inbound) = Mesh::spawn(
            "127.0.0.1:0".parse().expect("valid loopback addr"),
            NodeId::from(SELF),
            1,
            &small_outbox(),
            Arc::new(BucketPullHandler::default()),
        )
        .await
        .expect("bind loopback");
        mesh.update_peers(vec![peer_at(NodeId::from(WEDGED), wedged_addr())]);
        for key in 0..2u8 {
            mesh.send(
                NodeId::from(WEDGED),
                MsgClass::Replicate,
                Msg::Replicate {
                    cache: SmolStr::new("users"),
                    rec: wire_record_for_u32(u32::from(key)),
                },
            );
        }
        mesh
    }

    /// The value of `sundog_backlog_dropped_total{peer}`.
    fn backlog_dropped(snapshotter: &metrics_util::debugging::Snapshotter, peer: u64) -> u64 {
        snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(key, ..)| {
                key.key().name() == "sundog_backlog_dropped_total"
                    && key.key().labels().any(|label| {
                        label.key() == "peer" && label.value() == NodeId::from(peer).to_string()
                    })
            })
            .map(|(_, _, _, value)| match value {
                metrics_util::debugging::DebugValue::Counter(n) => n,
                _ => 0,
            })
            .sum()
    }

    #[test]
    fn a_lane_admits_a_batch_that_fits_or_any_batch_when_empty() {
        assert!(
            lane_admits(0, 10, 4),
            "an empty lane takes a batch past the bound"
        );
        assert!(lane_admits(6, 4, 10), "up to the bound");
        assert!(!lane_admits(7, 4, 10), "past the bound");
        assert!(!lane_admits(usize::MAX, 4, 10), "no overflow wrap");
        const { assert!(REFORWARD_LANE_BYTES >= 64 * 1024 * 1024) };
    }

    #[test]
    fn a_lane_is_retired_when_the_peer_left_or_the_worker_ended() {
        assert!(!lane_retired(true, false));
        assert!(lane_retired(false, false));
        assert!(lane_retired(true, true));
        assert!(lane_retired(false, true));
    }

    #[test]
    fn a_lane_warns_at_most_once_per_spacing() {
        let start = Instant::now();
        assert!(warn_due(None, start));
        assert!(!warn_due(Some(start), start + Duration::from_secs(4)));
        assert!(warn_due(Some(start), start + Duration::from_secs(5)));
    }

    #[tokio::test]
    async fn dispatch_to_a_peer_outside_the_table_opens_no_lane_and_counts_no_drop() {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let mesh = mesh_with_a_full_wedged_peer().await;
        let mut reforwarder = Reforwarder::with_lane_bytes(mesh.clone(), 1);

        reforwarder.dispatch(NodeId::from(99), vec![forward_frame(1)]);
        reforwarder.dispatch(NodeId::from(WEDGED), Vec::new());

        assert_eq!(
            reforwarder.lane_count(),
            0,
            "nothing could be sent to either"
        );
        assert_eq!(reforwarder.worker_count(), 0);
        assert_eq!(backlog_dropped(&snapshotter, 99), 0);
        assert_eq!(mesh.take_dirty_peers(), Vec::<NodeId>::new());
    }

    #[tokio::test]
    async fn a_lane_over_its_byte_bound_drops_counts_and_marks_the_peer_dirty() {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let mesh = mesh_with_a_full_wedged_peer().await;
        let wedged = NodeId::from(WEDGED);
        let one = vec![forward_frame(1)];
        let batch_bytes: usize = one.iter().map(|frame| frame.frame.len()).sum();
        let mut reforwarder = Reforwarder::with_lane_bytes(mesh.clone(), 3 * batch_bytes);

        for _ in 0..3 {
            reforwarder.dispatch(wedged, one.clone());
        }
        assert_eq!(reforwarder.queued_bytes(wedged), 3 * batch_bytes);
        assert_eq!(
            mesh.take_dirty_peers(),
            Vec::<NodeId>::new(),
            "a lane with room drops nothing"
        );
        assert_eq!(backlog_dropped(&snapshotter, WEDGED), 0);

        reforwarder.dispatch(wedged, one.clone());

        assert_eq!(
            reforwarder.queued_bytes(wedged),
            3 * batch_bytes,
            "the batch past the bound is not queued"
        );
        assert_eq!(mesh.take_dirty_peers(), vec![wedged]);
        assert_eq!(
            backlog_dropped(&snapshotter, WEDGED),
            1,
            "one frame dropped"
        );
    }

    #[tokio::test]
    async fn a_wedged_peer_does_not_hold_back_re_forwards_to_a_healthy_peer_and_each_lane_keeps_order()
     {
        let mesh = mesh_with_a_full_wedged_peer().await;
        let (healthy_mesh, mut arrivals) = crate::net::test_support::spawn_mesh(
            NodeId::from(HEALTHY),
            Arc::new(BucketPullHandler::default()),
        )
        .await;
        mesh.update_peers(vec![
            peer_at(NodeId::from(WEDGED), wedged_addr()),
            peer_at(NodeId::from(HEALTHY), healthy_mesh.local_addr()),
        ]);
        let mut reforwarder = Reforwarder::new(mesh.clone());

        for key in 0..5 {
            reforwarder.dispatch(NodeId::from(WEDGED), vec![forward_frame(key)]);
            reforwarder.dispatch(NodeId::from(HEALTHY), vec![forward_frame(key)]);
        }

        let mut keys = Vec::new();
        let got = tokio::time::timeout(Duration::from_secs(5), async {
            while keys.len() < 5 {
                let InboundMsg { from, msg } = arrivals.recv().await.expect("receiver open");
                assert_eq!(from, NodeId::from(SELF));
                let Msg::ForwardBatch { recs, hops, .. } = msg else {
                    panic!("only ForwardBatch frames are queued: {msg:?}");
                };
                assert_eq!(hops, 1);
                keys.extend(recs.into_iter().map(|rec| rec.key));
            }
        })
        .await;
        assert!(
            got.is_ok(),
            "the healthy peer receives while the wedged lane is full"
        );
        let expected: Vec<Bytes> = (0..5).map(|key| wire_record_for_u32(key).key).collect();
        assert_eq!(
            keys, expected,
            "frames reach a peer in the order dispatched"
        );
    }

    #[tokio::test]
    async fn retire_departed_drops_the_lane_of_a_peer_that_left_and_a_rejoin_opens_a_new_one() {
        let mesh = mesh_with_a_full_wedged_peer().await;
        let wedged = NodeId::from(WEDGED);
        let mut reforwarder = Reforwarder::new(mesh.clone());
        reforwarder.dispatch(wedged, vec![forward_frame(1)]);
        assert_eq!(reforwarder.lane_count(), 1);
        reforwarder.retire_departed();
        assert_eq!(reforwarder.lane_count(), 1, "a live peer keeps its lane");

        mesh.update_peers(Vec::new());
        reforwarder.retire_departed();
        assert_eq!(
            reforwarder.lane_count(),
            0,
            "the departed peer's lane is dropped"
        );
        let reaped = tokio::time::timeout(Duration::from_secs(5), async {
            while reforwarder.worker_count() > 0 {
                reforwarder.retire_departed();
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        assert!(reaped.is_ok(), "the worker ends once its peer is gone");

        mesh.update_peers(vec![peer_at(wedged, wedged_addr())]);
        reforwarder.dispatch(wedged, vec![forward_frame(2)]);
        assert_eq!(
            reforwarder.lane_count(),
            1,
            "the rejoined peer gets a fresh lane"
        );
    }

    #[tokio::test]
    async fn shutdown_hands_a_healthy_lane_to_the_mesh_and_leaves_a_wedged_one_behind() {
        let mesh = mesh_with_a_full_wedged_peer().await;
        let (healthy_mesh, mut arrivals) = crate::net::test_support::spawn_mesh(
            NodeId::from(HEALTHY),
            Arc::new(BucketPullHandler::default()),
        )
        .await;
        mesh.update_peers(vec![
            peer_at(NodeId::from(WEDGED), wedged_addr()),
            peer_at(NodeId::from(HEALTHY), healthy_mesh.local_addr()),
        ]);
        let mut reforwarder = Reforwarder::new(mesh.clone());
        reforwarder.dispatch(NodeId::from(WEDGED), vec![forward_frame(9)]);
        for key in 0..3 {
            reforwarder.dispatch(NodeId::from(HEALTHY), vec![forward_frame(key)]);
        }
        let started = Instant::now();

        reforwarder.shutdown().await;

        assert!(
            started.elapsed() < Duration::from_secs(2),
            "a wedged lane costs the drain deadline, not the failure detector: {:?}",
            started.elapsed()
        );
        let mut count = 0;
        let got = tokio::time::timeout(Duration::from_secs(5), async {
            while count < 3 {
                arrivals.recv().await.expect("receiver open");
                count += 1;
            }
        })
        .await;
        assert!(
            got.is_ok(),
            "the healthy lane's batches were handed to the mesh before shutdown returned"
        );
    }
}

/// An inbound loop on node 1 that is handed `messages` in one burst, then
/// drained and cancelled: the apply calls its shard received, in order.
async fn applied_from_one_burst(messages: Vec<InboundMsg>) -> Vec<Applied> {
    let view = Arc::new(OwnershipView::compute(
        NodeId::from(1),
        vec![NodeId::from(1), NodeId::from(2)],
        std::num::NonZeroU8::new(2).expect("nonzero"),
    ));
    let (shards, _invalidated, applied) = recording_registry(&view);
    let (mesh, _unused_inbound) = Mesh::spawn(
        "127.0.0.1:0".parse().expect("valid loopback addr"),
        NodeId::from(1),
        1,
        &ClusterConfig::default(),
        Arc::new(BucketPullHandler::default()),
    )
    .await
    .expect("bind loopback");
    let (tx, rx) = mpsc::channel(64);
    for message in messages {
        tx.send(message).await.expect("inbound open");
    }
    drop(tx);
    // The channel closes once the burst is drained, which ends the loop.
    inbound_loop(
        shards,
        Arc::new(InboundActivity::default()),
        mesh,
        NodeId::from(1),
        rx,
        CancellationToken::new(),
    )
    .await;
    applied.lock().expect("lock").clone()
}

/// A message of `count` records for the `users` cache: a `ForwardBatch`
/// under the receiver's own view hash (so nothing re-forwards) at `hops`, or
/// a `ReplicateBatch`.
fn burst_message(forwarded_at_hops: Option<u8>, count: usize) -> InboundMsg {
    let recs: Vec<WireRecord> = (0..count)
        .map(|n| wire_record_for_u32(u32::try_from(n).expect("small")))
        .collect();
    let cache = SmolStr::new("users");
    let msg = match forwarded_at_hops {
        Some(hops) => Msg::ForwardBatch {
            cache,
            view_hash: OwnershipView::compute(
                NodeId::from(1),
                vec![NodeId::from(1), NodeId::from(2)],
                std::num::NonZeroU8::new(2).expect("nonzero"),
            )
            .view_hash(),
            hops,
            recs,
        },
        None => Msg::ReplicateBatch { cache, recs },
    };
    InboundMsg {
        from: NodeId::from(2),
        msg,
    }
}

/// Consecutive `ForwardBatch`es share one `apply_forwarded_batch` call, a
/// `Replicate` between two of them splits the run, and the replicate takes
/// `apply_remote_batch`: a shard holds a forward it refuses for clock skew
/// and drops a replicate it refuses.
#[tokio::test]
async fn forward_batches_apply_as_forwards_and_never_share_a_call_with_replicates() {
    let applied = applied_from_one_burst(vec![
        burst_message(Some(0), 1),
        burst_message(Some(0), 2),
        burst_message(None, 1),
        burst_message(None, 3),
        burst_message(Some(0), 1),
    ])
    .await;
    assert_eq!(
        applied,
        [
            Applied::Forwarded(3),
            Applied::Replicated(4),
            Applied::Forwarded(1)
        ]
    );
}

/// A forward already re-forwarded once (`hops` at the cap) is the last copy
/// the final owner receives, and applies as a forward too.
#[tokio::test]
async fn a_re_forwarded_batch_at_the_hop_cap_applies_as_a_forward() {
    let applied = applied_from_one_burst(vec![burst_message(
        Some(crate::cluster::inbound::MAX_FORWARD_HOPS),
        2,
    )])
    .await;
    assert_eq!(applied, [Applied::Forwarded(2)]);
}

#[test]
fn same_run_joins_only_the_same_cache_and_the_same_kind() {
    use crate::cluster::inbound::same_run;

    let users = SmolStr::new("users");
    let orders = SmolStr::new("orders");
    assert!(same_run(Some((&users, true)), &users, true));
    assert!(same_run(Some((&users, false)), &users, false));
    assert!(
        !same_run(Some((&users, true)), &users, false),
        "a replicate does not join a forward run"
    );
    assert!(
        !same_run(Some((&users, false)), &users, true),
        "a forward does not join a replicate run"
    );
    assert!(!same_run(Some((&users, true)), &orders, true));
    assert!(
        !same_run(None, &users, true),
        "nothing pending, nothing to join"
    );
}

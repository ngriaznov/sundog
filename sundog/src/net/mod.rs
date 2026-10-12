//! Data plane: a lazily dialed TCP mesh between live peers carrying
//! invalidations, replications, state transfer, and anti-entropy. A lost
//! message is acceptable; anti-entropy repairs it.
//!
//! Each live peer gets one persistent writer connection, `Hello` then a
//! stream of `Invalidate` and `Replicate` frames drained from bounded
//! per-class outboxes, plus a small pool of reused connections for
//! request/response exchanges. The pool stays off the broadcast path, so a
//! slow snapshot never delays live traffic. Both kinds share one listener:
//! the message after `Hello` decides whether the connection serves requests
//! until idle or loops as the persistent link.
//!
//! With feature `tls` and `ClusterConfig::tls` set, every connection is
//! wrapped in mutual TLS. The `MeshStream` and `TlsCtx` aliases collapse to
//! plain types when the feature is off or `sim` is on, so the framing code
//! never branches on the feature.

mod conn;

pub(crate) use conn::{REPLICATE_BATCH_BUDGET, REPLICATE_BATCH_COUNT};
mod frames;
mod handler;
mod outbox;
mod request;
mod tcp;
#[cfg(all(test, not(feature = "sim")))]
pub(crate) mod test_support;
#[cfg(all(feature = "tls", not(feature = "sim")))]
mod tls;
mod types;

use frames::record_frame_sent; // consumed by conn::framing via crate::net::record_frame_sent
pub(crate) use frames::{OutFrame, batch_forward, batch_replicate};
pub use frames::{bytes_sent_total, frames_sent_total};
pub use handler::RequestHandler;
pub use types::{AeMismatch, AePartReply, InboundMsg, MsgClass};
pub(crate) use types::{
    AeRoundOutcome, AeServeOutcome, BucketPull, BucketStreamItem, FetchOutcome, FetchServe,
    LoadOutcome, LoadServe, StServe,
};

use std::collections::{HashMap, HashSet, VecDeque};
#[cfg(all(feature = "tls", not(feature = "sim")))]
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use smol_str::SmolStr;
use tcp::TcpListener;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::ClusterConfig;
use crate::error::JoinError;
use crate::membership::Peer;
use crate::node::NodeId;
use crate::wire::{self, Msg};
use outbox::DropOldestQueue;

#[cfg(all(feature = "tls", not(feature = "sim")))]
pub use tls::MESH_SERVER_NAME;

/// The concrete stream type every dialed or accepted connection is framed
/// over: `tcp::TcpStream` unmodified, unless `tls` is active on the
/// real-tokio transport, in which case it's [`tls::MeshStream`].
#[cfg(all(feature = "tls", not(feature = "sim")))]
pub(crate) type MeshStream = tls::MeshStream;
#[cfg(not(all(feature = "tls", not(feature = "sim"))))]
pub(crate) type MeshStream = tcp::TcpStream;

/// This node's TLS context: `Some` wraps every connection in mutual TLS, `None`
/// stays plaintext.
#[cfg(all(feature = "tls", not(feature = "sim")))]
pub(crate) type TlsCtx = Option<Arc<tls::MeshTls>>;
/// TLS isn't compiled in for this transport: a zero-sized stand-in so
/// `conn`'s dial/accept plumbing carries a `tls` parameter unconditionally.
/// A dedicated unit struct, since a bare `()` trips clippy's unit-argument
/// lint.
#[cfg(not(all(feature = "tls", not(feature = "sim"))))]
#[derive(Clone)]
pub(crate) struct TlsCtx;

#[cfg(all(feature = "tls", not(feature = "sim")))]
fn build_tls_ctx(config: &ClusterConfig, bind_addr: SocketAddr) -> Result<TlsCtx, JoinError> {
    config
        .tls
        .as_ref()
        .map(|tls_config| tls::MeshTls::new(tls_config).map(Arc::new))
        .transpose()
        .map_err(|source| JoinError::Bind {
            addr: bind_addr,
            source: io::Error::other(source),
        })
}

// Returns `TlsCtx` bare rather than `Result<TlsCtx, JoinError>`;
// `Mesh::spawn` picks the matching call form per the same `cfg`.
#[cfg(all(feature = "tls", feature = "sim"))]
fn build_tls_ctx(config: &ClusterConfig) -> TlsCtx {
    if config.tls.is_some() {
        tracing::warn!(
            "ClusterConfig::tls is set but the `sim` feature is active; TLS is not applied \
             over the turmoil transport; see net::tls's module docs"
        );
    }
    TlsCtx
}

#[cfg(not(feature = "tls"))]
fn build_tls_ctx(_config: &ClusterConfig) -> TlsCtx {
    TlsCtx
}

/// Capacity of the inbound-message channel and of each per-peer, per-class
/// outbox absent a caller override, mirroring
/// [`ClusterConfig::outbox_capacity`]'s default.
const DEFAULT_CHANNEL_CAPACITY: usize = 8_192;

/// How long [`Mesh::shutdown`] waits for queued replicate frames to reach
/// their writers before cancelling them, and the moment it then leaves for
/// the writers' last frames to go out.
const OUTBOX_FLUSH_DEADLINE: Duration = Duration::from_millis(500);
const OUTBOX_FLUSH_TAIL: Duration = Duration::from_millis(20);

/// How long [`Mesh::send_frames_awaiting`] waits for outbox space before
/// rechecking peer liveness. Bounds the recheck interval, not the total
/// time a live peer's send may take.
pub(crate) const FAN_OUT_SEND_DEADLINE: Duration = Duration::from_secs(2);

/// Milliseconds since the first call in this process, a monotonic stamp
/// for "how recently" bookkeeping that never needs wall-clock meaning.
pub(crate) fn mono_ms() -> u64 {
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    duration_to_ms(START.get_or_init(Instant::now).elapsed())
}

fn duration_to_ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

struct PeerHandle {
    data_addr: SocketAddr,
    invalidate: Arc<DropOldestQueue<OutFrame>>,
    replicate_tx: mpsc::Sender<OutFrame>,
    dirty: Arc<AtomicBool>,
    /// [`mono_ms`] + 1 of the last `Replicate` frame enqueued, `0` if none.
    /// Read by [`Mesh::replicate_in_flight`].
    last_replicate_enqueued: Arc<AtomicU64>,
    /// Anti-entropy digests from this peer answered empty in a row, for
    /// [`MeshInner::defers_ae_digest_from`]'s bound.
    ae_deferrals: AtomicU32,
    cancel: CancellationToken,
    /// Pooled request/response connections for this peer, dialed on
    /// demand, dropped with this handle when the peer departs.
    req_pool: Arc<conn::ReqPool>,
    /// This peer's last-known [`crate::wire::Msg::Hello`] protocol, from its
    /// [`Peer::protocol`] gossip field. Refreshed on every
    /// [`Mesh::update_peers`] call, read by
    /// [`MeshInner::require_peer_protocol`] to gate a distribution-mode
    /// send before ever dialing.
    protocol: AtomicU16,
}

/// [`MeshInner::acked_buckets`]'s key: the bucket a `Msg::StBucketAck`
/// named and who sent it.
type AckedBucketKey = (SmolStr, u16, NodeId);
/// [`MeshInner::acked_buckets`]'s value: when the ack was recorded and the
/// `view_hash` it carried.
type AckedBucketEntry = (Instant, u64);

struct MeshInner {
    node: NodeId,
    incarnation: u64,
    /// This node's [`wire::cluster_id`], or [`wire::UNNAMED_CLUSTER`] for a
    /// bare mesh: sent in every `Hello`, and checked against every
    /// accepted one.
    cluster: u64,
    outbox_capacity: usize,
    peers: RwLock<HashMap<NodeId, PeerHandle>>,
    accept_cancel: CancellationToken,
    tls: TlsCtx,
    /// Bucket acks from `Msg::StBucketAck`, written by
    /// `conn::serve_st_buckets` when a requester releases a bucket.
    /// `rebalance_task` uses a fresh, current-view entry to skip a
    /// redundant confirming AE round for that bucket.
    acked_buckets: RwLock<HashMap<AckedBucketKey, AckedBucketEntry>>,
}

/// Digests from one peer answered empty in a row before the responder serves
/// one regardless, so a steady write trickle toward that peer cannot starve
/// its repair. Mirrors the initiator's own bound on skipped rounds.
const MAX_AE_DEFERRALS: u32 = 3;

/// Whether the responder answers a digest empty: only while its outbox
/// toward the requesting peer still holds `queued` replicate frames, and
/// never more than [`MAX_AE_DEFERRALS`] times running.
fn defer_ae_digest(queued: usize, deferred_so_far: u32) -> bool {
    queued > 0 && deferred_so_far < MAX_AE_DEFERRALS
}

/// Whether [`Mesh::send_frames_awaiting`] should wait again after a
/// timed-out [`FAN_OUT_SEND_DEADLINE`] slice: yes while `peer_live`, so a
/// live peer's frame is never dropped.
const fn fan_out_keep_waiting(peer_live: bool) -> bool {
    peer_live
}

impl MeshInner {
    /// Whether replicate traffic toward `peer` is still in motion: frames
    /// queued in its outbox, or a frame enqueued within the last `window`.
    fn replicate_in_flight(&self, peer: NodeId, window: Duration) -> bool {
        let table = self
            .peers
            .read()
            .expect("invariant: peers lock is never poisoned");
        let Some(handle) = table.get(&peer) else {
            return false;
        };
        let queued = handle.replicate_tx.max_capacity() - handle.replicate_tx.capacity();
        if queued > 0 {
            return true;
        }
        let last = handle.last_replicate_enqueued.load(Ordering::Relaxed);
        last != 0 && (mono_ms() + 1).saturating_sub(last) <= duration_to_ms(window)
    }

    /// Whether an anti-entropy digest from `peer` is answered empty: this
    /// node's outbox toward it still holds replicate frames, so a listing of
    /// every bucket the stream has not yet delivered would only ship the
    /// same records twice. Bounded by [`MAX_AE_DEFERRALS`] in a row; the
    /// peer's next round repairs whatever the stream left.
    fn defers_ae_digest_from(&self, peer: NodeId) -> bool {
        let table = self
            .peers
            .read()
            .expect("invariant: peers lock is never poisoned");
        let Some(handle) = table.get(&peer) else {
            return false;
        };
        let queued = handle.replicate_tx.max_capacity() - handle.replicate_tx.capacity();
        let deferred = handle.ae_deferrals.load(Ordering::Relaxed);
        if defer_ae_digest(queued, deferred) {
            handle.ae_deferrals.store(deferred + 1, Ordering::Relaxed);
            true
        } else {
            handle.ae_deferrals.store(0, Ordering::Relaxed);
            false
        }
    }

    /// Records that `from` acked `bucket` of `cache` via `Msg::StBucketAck`.
    /// `view_hash` is the acker's own view hash at request time;
    /// [`MeshInner::acked_owners`] checks it against this node's current one.
    fn record_bucket_ack(&self, cache: SmolStr, bucket: u16, from: NodeId, view_hash: u64) {
        self.acked_buckets
            .write()
            .expect("invariant: acked_buckets lock is never poisoned")
            .insert((cache, bucket, from), (Instant::now(), view_hash));
    }

    /// Every acked owner of each of `due`'s buckets of `cache`, keyed by
    /// bucket. An ack for one bucket says nothing about any other bucket the
    /// same owner co-owns. An ack older than `window`, for a bucket not in
    /// `due`, or carrying a `view_hash` other than this node's current one,
    /// does not count.
    fn acked_owners(
        &self,
        cache: &SmolStr,
        due: &[u16],
        window: Duration,
        view_hash: u64,
    ) -> HashMap<u16, HashSet<NodeId>> {
        let now = Instant::now();
        let due: HashSet<u16> = due.iter().copied().collect();
        let mut acked: HashMap<u16, HashSet<NodeId>> = HashMap::new();
        for ((c, bucket, owner), (acked_at, acked_view_hash)) in self
            .acked_buckets
            .read()
            .expect("invariant: acked_buckets lock is never poisoned")
            .iter()
        {
            if c == cache
                && due.contains(bucket)
                && now.saturating_duration_since(*acked_at) <= window
                && *acked_view_hash == view_hash
            {
                acked.entry(*bucket).or_default().insert(*owner);
            }
        }
        acked
    }

    /// `peer`'s last-known gossiped protocol, or `0` if unseen. Lets
    /// `conn::bucket_stream` gate its `Msg::StBucketAck` send on what the
    /// donor advertised.
    fn peer_protocol(&self, peer: NodeId) -> u16 {
        self.peers
            .read()
            .expect("invariant: peers lock is never poisoned")
            .get(&peer)
            .map_or(0, |handle| handle.protocol.load(Ordering::Relaxed))
    }

    /// What this mesh says in every `Hello` it sends.
    const fn greeting(&self) -> Greeting {
        Greeting {
            node: self.node,
            incarnation: self.incarnation,
            cluster: self.cluster,
        }
    }

    /// A mesh with no peers, for `conn`'s tests that drive
    /// `handle_accepted` over a raw accepted stream.
    #[cfg(all(test, not(feature = "sim")))]
    pub(super) fn for_tests(tls: TlsCtx, accept_cancel: CancellationToken) -> Arc<Self> {
        Arc::new(Self {
            node: NodeId::from(1),
            incarnation: 1,
            cluster: wire::UNNAMED_CLUSTER,
            outbox_capacity: DEFAULT_CHANNEL_CAPACITY,
            peers: RwLock::new(HashMap::new()),
            accept_cancel,
            tls,
            acked_buckets: RwLock::new(HashMap::new()),
        })
    }
}

/// Who a mesh says it is in the `Hello` that opens each connection it dials.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Greeting {
    pub(crate) node: NodeId,
    pub(crate) incarnation: u64,
    /// The node's [`wire::cluster_id`], or [`wire::UNNAMED_CLUSTER`].
    pub(crate) cluster: u64,
}

impl Greeting {
    /// The `Hello` this greeting sends.
    pub(crate) const fn hello(self) -> Msg {
        Msg::Hello {
            node: self.node,
            incarnation: self.incarnation,
            protocol: wire::PROTOCOL_VERSION,
            cluster: self.cluster,
        }
    }
}

/// The data-plane TCP listener, bound and holding its port until
/// [`Mesh::spawn_on`] takes it over. Binding port `0` here and handing the
/// listener on keeps the port from being released in between, where
/// another socket could take it.
pub(crate) struct BoundListener {
    listener: TcpListener,
    local_addr: SocketAddr,
}

impl BoundListener {
    /// Binds `bind_addr`, resolving port `0` to the port the OS assigns.
    ///
    /// # Errors
    ///
    /// Returns [`JoinError::Bind`] if `bind_addr` cannot be bound.
    pub(crate) async fn bind(bind_addr: SocketAddr) -> Result<Self, JoinError> {
        let listener = TcpListener::bind(bind_addr)
            .await
            .map_err(|source| JoinError::Bind {
                addr: bind_addr,
                source,
            })?;
        let local_addr = listener.local_addr().map_err(|source| JoinError::Bind {
            addr: bind_addr,
            source,
        })?;
        Ok(Self {
            listener,
            local_addr,
        })
    }

    /// The bound address, with port `0` resolved.
    pub(crate) const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

/// A cheap-to-clone handle onto the running data-plane mesh.
#[derive(Clone)]
pub struct Mesh {
    local_addr: SocketAddr,
    inner: Arc<MeshInner>,
}

impl Mesh {
    /// Binds the data-plane TCP listener and starts the mesh's background
    /// accept/dial tasks, returning the handle with the receiver of
    /// inbound invalidation/replication traffic; request/response traffic
    /// returns inline from [`Mesh::request_state`], [`Mesh::ae_round`],
    /// and [`Mesh::ae_pull`] instead. `incarnation` is embedded in every
    /// `Hello` this node sends; `handler` answers requests from peers
    /// dialing in; `config.outbox_capacity` sizes the inbound channel and
    /// outboxes.
    ///
    /// # Errors
    ///
    /// Returns [`JoinError::Bind`] if `bind_addr` cannot be bound.
    pub async fn spawn(
        bind_addr: SocketAddr,
        node: NodeId,
        incarnation: u64,
        config: &ClusterConfig,
        handler: Arc<dyn RequestHandler>,
    ) -> Result<(Self, mpsc::Receiver<InboundMsg>), JoinError> {
        let listener = BoundListener::bind(bind_addr).await?;
        let greeting = Greeting {
            node,
            incarnation,
            cluster: wire::UNNAMED_CLUSTER,
        };
        Self::spawn_on(listener, greeting, config, handler)
    }

    /// [`Mesh::spawn`] over a listener already bound by
    /// [`BoundListener::bind`], so the port a caller advertised before
    /// spawning is the one the mesh accepts on, and naming the cluster in
    /// `greeting`: the mesh drops an accepted connection whose `Hello`
    /// names another.
    #[cfg_attr(
        not(all(feature = "tls", not(feature = "sim"))),
        expect(
            clippy::unnecessary_wraps,
            reason = "only building the TLS context can fail"
        )
    )]
    pub(crate) fn spawn_on(
        listener: BoundListener,
        greeting: Greeting,
        config: &ClusterConfig,
        handler: Arc<dyn RequestHandler>,
    ) -> Result<(Self, mpsc::Receiver<InboundMsg>), JoinError> {
        let BoundListener {
            listener,
            local_addr,
        } = listener;
        #[cfg(all(feature = "tls", not(feature = "sim")))]
        let tls = build_tls_ctx(config, local_addr)?;
        #[cfg(not(all(feature = "tls", not(feature = "sim"))))]
        let tls = build_tls_ctx(config);

        let outbox_capacity = if config.outbox_capacity == 0 {
            DEFAULT_CHANNEL_CAPACITY
        } else {
            config.outbox_capacity
        };
        let (inbound_tx, inbound_rx) = mpsc::channel(outbox_capacity);
        let inner = Arc::new(MeshInner {
            node: greeting.node,
            incarnation: greeting.incarnation,
            cluster: greeting.cluster,
            outbox_capacity,
            peers: RwLock::new(HashMap::new()),
            accept_cancel: CancellationToken::new(),
            tls,
            acked_buckets: RwLock::new(HashMap::new()),
        });
        tokio::spawn(conn::accept_loop(
            listener,
            inbound_tx,
            handler,
            Arc::clone(&inner),
        ));
        Ok((Self { local_addr, inner }, inbound_rx))
    }

    /// The address the data-plane listener is bound to, relevant when
    /// `bind_addr` used port `0`.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// The greeting this mesh opens every connection with, for the tests
    /// that check what `Cluster::build` named in it.
    #[cfg(all(test, not(feature = "sim")))]
    pub(crate) fn greeting(&self) -> Greeting {
        self.inner.greeting()
    }

    /// Refreshes the set of peers the mesh dials and fans traffic out to:
    /// spawns a writer for each newly seen peer, cancels it for any that
    /// departed or changed `data_addr`. Called on every
    /// [`crate::membership::Membership::peers`] change.
    ///
    /// # Panics
    ///
    /// Panics if the peer-table lock is poisoned.
    pub fn update_peers(&self, peers: Vec<Peer>) {
        // `incoming_peers` keeps `peers`' own order (minus self) so the
        // spawn loop below creates each newly seen peer's writer in that
        // deterministic order; `incoming` exists only for the O(1) lookups
        // `retain` below needs, and is never itself iterated -- a `HashMap`
        // iterated directly would order those spawns by this process's own
        // per-instance hash-key randomization, not by anything about the
        // peers themselves, letting two runs seeded identically schedule
        // this node's writer tasks in a different relative order.
        let incoming_peers: Vec<Peer> = peers
            .into_iter()
            .filter(|peer| peer.node != self.inner.node)
            .collect();
        let incoming: HashMap<NodeId, (SocketAddr, u16)> = incoming_peers
            .iter()
            .map(|peer| (peer.node, (peer.data_addr, peer.protocol)))
            .collect();

        let mut table = self
            .inner
            .peers
            .write()
            .expect("invariant: peers lock is never poisoned");
        table.retain(|node, handle| {
            let Some(&(data_addr, protocol)) = incoming.get(node) else {
                handle.cancel.cancel();
                return false;
            };
            if data_addr != handle.data_addr {
                handle.cancel.cancel();
                return false;
            }
            // The peer is unchanged; its gossiped protocol may still have
            // moved (a rolling upgrade), so it is refreshed regardless.
            handle.protocol.store(protocol, Ordering::Relaxed);
            true
        });
        for peer in incoming_peers {
            table
                .entry(peer.node)
                .or_insert_with(|| self.spawn_peer_handle(peer.data_addr, peer.protocol));
        }
    }

    fn spawn_peer_handle(&self, data_addr: SocketAddr, protocol: u16) -> PeerHandle {
        let invalidate = Arc::new(DropOldestQueue::new(self.inner.outbox_capacity));
        let (replicate_tx, replicate_rx) = mpsc::channel(self.inner.outbox_capacity);
        let cancel = CancellationToken::new();
        tokio::spawn(conn::run_peer_writer(
            self.inner.greeting(),
            data_addr,
            Arc::clone(&invalidate),
            replicate_rx,
            cancel.clone(),
            self.inner.tls.clone(),
        ));
        PeerHandle {
            data_addr,
            invalidate,
            replicate_tx,
            dirty: Arc::new(AtomicBool::new(false)),
            last_replicate_enqueued: Arc::new(AtomicU64::new(0)),
            ae_deferrals: AtomicU32::new(0),
            cancel,
            req_pool: Arc::new(conn::ReqPool::new()),
            protocol: AtomicU16::new(protocol),
        }
    }

    /// Best-effort, non-blocking fan-out of `msg` to `peer` on the outbox
    /// selected by `class`. Overflow is handled per `class`'s drop policy.
    /// A `peer` the mesh doesn't know about is a silent no-op.
    ///
    /// # Panics
    ///
    /// Panics if the peer-table lock is poisoned.
    pub fn send(&self, peer: NodeId, class: MsgClass, msg: Msg) {
        self.send_many(peer, class, std::iter::once(msg));
    }

    /// [`Mesh::send`] for many messages, resolving the peer-table lock
    /// once for the batch. Each message still gets its own overflow
    /// decision. For a multi-peer fan-out sharing content, use
    /// `Mesh::send_frames` instead.
    ///
    /// # Panics
    ///
    /// Panics if the peer-table lock is poisoned.
    pub fn send_many(&self, peer: NodeId, class: MsgClass, msgs: impl IntoIterator<Item = Msg>) {
        self.send_many_enqueued(peer, class, msgs);
    }

    /// [`Mesh::send_many`], answering whether every message reached
    /// `peer`'s outbox: `false` when `peer` is missing from the peer table,
    /// which a failure-detector flap causes as well as a departure, or when
    /// a message failed to encode or met a full outbox. Enqueued is not
    /// delivered: the outbox can still lose a frame to a broken connection.
    ///
    /// # Panics
    ///
    /// Panics if the peer-table lock is poisoned.
    pub(crate) fn send_many_enqueued(
        &self,
        peer: NodeId,
        class: MsgClass,
        msgs: impl IntoIterator<Item = Msg>,
    ) -> bool {
        let mut encoded = true;
        let frames = msgs.into_iter().filter_map(|msg| match OutFrame::new(msg) {
            Ok(frame) => Some(frame),
            Err(error) => {
                tracing::warn!(%error, "failed to encode outbound message; dropped");
                encoded = false;
                None
            }
        });
        let enqueued = self.send_frames(peer, class, frames);
        encoded && enqueued
    }

    /// [`Mesh::send_many`], but for already-encoded [`OutFrame`]s, so
    /// broadcast content is encoded once regardless of peer count.
    /// Otherwise identical to [`Mesh::send_many`]. Answers whether every
    /// frame reached `peer`'s outbox, as [`Mesh::send_many_enqueued`] does.
    ///
    /// # Panics
    ///
    /// Panics if the peer-table lock is poisoned.
    pub(crate) fn send_frames(
        &self,
        peer: NodeId,
        class: MsgClass,
        frames: impl IntoIterator<Item = OutFrame>,
    ) -> bool {
        let table = self
            .inner
            .peers
            .read()
            .expect("invariant: peers lock is never poisoned");
        let Some(handle) = table.get(&peer) else {
            return false;
        };
        let mut enqueued_replicate = false;
        let mut enqueued_all = true;
        for frame in frames {
            match class {
                MsgClass::Invalidate => handle.invalidate.push(frame),
                MsgClass::Replicate => {
                    enqueued_replicate = true;
                    if let Err(mpsc::error::TrySendError::Full(_)) =
                        handle.replicate_tx.try_send(frame)
                    {
                        enqueued_all = false;
                        handle.dirty.store(true, Ordering::Relaxed);
                        metrics::counter!(
                            "sundog_backlog_dropped_total",
                            "peer" => peer.to_string()
                        )
                        .increment(1);
                    }
                }
            }
        }
        if enqueued_replicate {
            handle
                .last_replicate_enqueued
                .store(mono_ms() + 1, Ordering::Relaxed);
        }
        enqueued_all
    }

    /// [`Mesh::send_frames`] for records the sender keeps no copy of: a
    /// `Mode::Distributed` fan-out, where a forwarded write's only copy is
    /// the frame itself, so a frame for a peer still live in the peer table
    /// is never dropped. Waits for outbox space in
    /// [`FAN_OUT_SEND_DEADLINE`] slices, logging and counting each timed-out
    /// one in `sundog_fan_out_wait_seconds_total{peer}`. Once `peer` leaves
    /// the table or its outbox closes, remaining frames are dropped,
    /// counted in `sundog_backlog_dropped_total`, and the peer marked
    /// dirty. An unknown `peer` is a silent no-op.
    ///
    /// # Panics
    ///
    /// Panics if the peer-table lock is poisoned.
    pub(crate) async fn send_frames_awaiting(&self, peer: NodeId, frames: Vec<OutFrame>) {
        let (tx, dirty, enqueued_stamp) = {
            let table = self
                .inner
                .peers
                .read()
                .expect("invariant: peers lock is never poisoned");
            let Some(handle) = table.get(&peer) else {
                return;
            };
            (
                handle.replicate_tx.clone(),
                Arc::clone(&handle.dirty),
                Arc::clone(&handle.last_replicate_enqueued),
            )
        };
        let mut queue: VecDeque<OutFrame> = frames.into();
        let mut dropped = 0u64;
        while let Some(frame) = queue.pop_front() {
            let slice_deadline = tokio::time::Instant::now() + FAN_OUT_SEND_DEADLINE;
            match tokio::time::timeout_at(slice_deadline, tx.reserve()).await {
                Ok(Ok(permit)) => permit.send(frame),
                Ok(Err(_)) => {
                    // The channel closed: this peer's writer already ended.
                    dropped += 1 + u64::try_from(queue.len()).unwrap_or(u64::MAX);
                    queue.clear();
                }
                Err(_) => {
                    if fan_out_keep_waiting(self.peer_is_live(peer)) {
                        metrics::counter!(
                            "sundog_fan_out_wait_seconds_total",
                            "peer" => peer.to_string()
                        )
                        .increment(FAN_OUT_SEND_DEADLINE.as_secs());
                        tracing::debug!(
                            %peer,
                            "outbox still full past one fan-out wait slice; peer is still \
                             live, waiting again"
                        );
                        queue.push_front(frame);
                    } else {
                        dropped += 1 + u64::try_from(queue.len()).unwrap_or(u64::MAX);
                        queue.clear();
                    }
                }
            }
        }
        enqueued_stamp.store(mono_ms() + 1, Ordering::Relaxed);
        if dropped > 0 {
            dirty.store(true, Ordering::Relaxed);
            metrics::counter!("sundog_backlog_dropped_total", "peer" => peer.to_string())
                .increment(dropped);
            tracing::warn!(%peer, dropped, "outbox stayed full past the fan-out deadline; frames dropped");
        }
    }

    /// Whether `peer` is still in the peer table:
    /// [`Mesh::send_frames_awaiting`]'s liveness check between wait slices.
    ///
    /// # Panics
    ///
    /// Panics if the peer-table lock is poisoned.
    fn peer_is_live(&self, peer: NodeId) -> bool {
        self.inner
            .peers
            .read()
            .expect("invariant: peers lock is never poisoned")
            .contains_key(&peer)
    }

    /// Waits, up to `deadline`, until every peer's replicate outbox has been
    /// handed to its writer, then a moment more for the writer's last
    /// frames to leave: what [`Mesh::shutdown`] runs before cancelling the
    /// writers, so a frame accepted right before shutdown still goes out.
    async fn flush_outboxes(&self, deadline: tokio::time::Instant) {
        loop {
            let pending = {
                let table = self
                    .inner
                    .peers
                    .read()
                    .expect("invariant: peers lock is never poisoned");
                table
                    .values()
                    .any(|h| h.replicate_tx.capacity() < h.replicate_tx.max_capacity())
            };
            if !pending || tokio::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        tokio::time::sleep(OUTBOX_FLUSH_TAIL).await;
    }

    /// Whether replicate traffic toward `peer` is still in motion: frames
    /// queued in its outbox, or a frame enqueued within the last `window`.
    /// Anti-entropy skips a round against such a peer.
    ///
    /// # Panics
    ///
    /// Panics if the peer-table lock is poisoned.
    pub(crate) fn replicate_in_flight(&self, peer: NodeId, window: Duration) -> bool {
        self.inner.replicate_in_flight(peer, window)
    }

    /// Marks `peer` dirty for the next [`Mesh::take_dirty_peers`], for
    /// when the scheduler skips a round it already took the mark for.
    ///
    /// # Panics
    ///
    /// Panics if the peer-table lock is poisoned.
    pub(crate) fn mark_dirty(&self, peer: NodeId) {
        let table = self
            .inner
            .peers
            .read()
            .expect("invariant: peers lock is never poisoned");
        if let Some(handle) = table.get(&peer) {
            handle.dirty.store(true, Ordering::Relaxed);
        }
    }

    /// Returns every peer whose `Replicate` outbox has dropped a message
    /// since the last call, clearing their dirty mark.
    ///
    /// # Panics
    ///
    /// Panics if the peer-table lock is poisoned.
    #[must_use]
    pub fn take_dirty_peers(&self) -> Vec<NodeId> {
        let table = self
            .inner
            .peers
            .read()
            .expect("invariant: peers lock is never poisoned");
        table
            .iter()
            .filter(|(_, handle)| handle.dirty.swap(false, Ordering::Relaxed))
            .map(|(node, _)| *node)
            .collect()
    }

    /// Shuts down the mesh: stops accepting, cancels every per-peer writer.
    /// Signals the spawned background work to stop without waiting on sockets.
    ///
    /// # Panics
    ///
    /// Panics if the peer-table lock is poisoned.
    pub async fn shutdown(self) {
        self.inner.accept_cancel.cancel();
        self.flush_outboxes(tokio::time::Instant::now() + OUTBOX_FLUSH_DEADLINE)
            .await;
        let table = std::mem::take(
            &mut *self
                .inner
                .peers
                .write()
                .expect("invariant: peers lock is never poisoned"),
        );
        for handle in table.into_values() {
            handle.cancel.cancel();
        }
        // Yield once so cancelled tasks observe the token before this handle
        // drops.
        tokio::task::yield_now().await;
    }
}

// These tests dial real `tokio::net` sockets against a live `Mesh`. Turmoil
// sockets only work inside a driven `turmoil::Sim`, so this module is
// real-transport-only, like the `tls` submodule.
#[cfg(all(test, not(feature = "sim")))]
mod tests;

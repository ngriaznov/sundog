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
mod mesh;
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

use std::collections::{HashMap, HashSet};
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

// These tests dial real `tokio::net` sockets against a live `Mesh`. Turmoil
// sockets only work inside a driven `turmoil::Sim`, so this module is
// real-transport-only, like the `tls` submodule.
#[cfg(all(test, not(feature = "sim")))]
mod tests;

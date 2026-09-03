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
mod outbox;
mod tcp;
#[cfg(all(feature = "tls", not(feature = "sim")))]
mod tls;

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use smol_str::SmolStr;
use tcp::TcpListener;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use xxhash_rust::xxh3::xxh3_64;

use crate::config::ClusterConfig;
use crate::error::{CodecError, JoinError};
use crate::hlc::Hlc;
use crate::membership::Peer;
use crate::node::NodeId;
use crate::wire::{self, Cell, Msg, WireRecord};
use outbox::DropOldestQueue;

/// A queued outbound message paired with its already-encoded wire frame,
/// cheaply cloned into each peer's outbox so the writer never re-encodes
/// byte-identical content. `msg` stays alongside `frame` because
/// `Replicate`-class traffic coalesces consecutive queued entries by it.
#[derive(Debug, Clone)]
pub(crate) struct OutFrame {
    pub(crate) msg: Msg,
    pub(crate) frame: Bytes,
}

impl OutFrame {
    pub(crate) fn new(msg: Msg) -> Result<Self, CodecError> {
        let frame = wire::encode(&msg)?;
        Ok(Self { msg, frame })
    }
}

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

/// Process-wide wire-frame counters, incremented at [`conn::send_msg`], the
/// single choke point every outbound frame passes through:
/// [`frames_sent_total`] and [`bytes_sent_total`] sum every `Mesh` in this
/// process.
static FRAMES_SENT: AtomicU64 = AtomicU64::new(0);
static BYTES_SENT: AtomicU64 = AtomicU64::new(0);

/// Records one frame of `len` bytes written to the wire: bumps the
/// process-wide [`frames_sent_total`]/[`bytes_sent_total`] counters and
/// mirrors them to `metrics` for a live Prometheus scrape.
pub(super) fn record_frame_sent(len: usize) {
    FRAMES_SENT.fetch_add(1, Ordering::Relaxed);
    BYTES_SENT.fetch_add(len as u64, Ordering::Relaxed);
    metrics::counter!("sundog_frames_sent_total").increment(1);
    metrics::counter!("sundog_bytes_sent_total").increment(len as u64);
}

/// Splits a run of records into `Msg::ReplicateBatch` chunks by the same
/// budget and count cap `net::conn`'s coalescer enforces
/// ([`REPLICATE_BATCH_BUDGET`]/[`REPLICATE_BATCH_COUNT`]). A chunk of
/// exactly one record stays a plain [`Msg::Replicate`].
pub(crate) fn batch_replicate(cache_name: &SmolStr, records: Vec<WireRecord>) -> Vec<Msg> {
    let mut chunks: Vec<Vec<WireRecord>> = Vec::new();
    let mut current: Vec<WireRecord> = Vec::new();
    let mut current_bytes = 0usize;
    for rec in records {
        let rec_bytes = wire::replicate_frame_len(
            cache_name.len(),
            rec.key.len(),
            rec.value.as_ref().map_or(0, Bytes::len),
        );
        if !current.is_empty()
            && (current.len() >= REPLICATE_BATCH_COUNT
                || current_bytes + rec_bytes > REPLICATE_BATCH_BUDGET)
        {
            chunks.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current_bytes += rec_bytes;
        current.push(rec);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
        .into_iter()
        .map(|mut recs| {
            if recs.len() == 1 {
                Msg::Replicate {
                    cache: cache_name.clone(),
                    rec: recs.pop().expect("invariant: length checked above"),
                }
            } else {
                Msg::ReplicateBatch {
                    cache: cache_name.clone(),
                    recs,
                }
            }
        })
        .collect()
}

/// Milliseconds since the first call in this process, a monotonic stamp
/// for "how recently" bookkeeping that never needs wall-clock meaning.
pub(crate) fn mono_ms() -> u64 {
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    duration_to_ms(START.get_or_init(Instant::now).elapsed())
}

fn duration_to_ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// Total wire frames sent by this process since start, across every
/// [`Mesh`] it has spawned. A cheap diagnostic for benchmarking replication
/// cost.
#[must_use]
pub fn frames_sent_total() -> u64 {
    FRAMES_SENT.load(Ordering::Relaxed)
}

/// Total wire-frame bytes sent by this process since start, across every
/// [`Mesh`] it has spawned. A cheap diagnostic for benchmarking replication
/// cost.
#[must_use]
pub fn bytes_sent_total() -> u64 {
    BYTES_SENT.load(Ordering::Relaxed)
}

/// Bound on one request/response exchange over a fresh connection, end to
/// end. Without this, a peer that accepts and then stalls blocks the
/// caller forever; AP semantics mean a stuck peer must never hang this node.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

// The override seam below backs only the `not(sim)` test module's
// `ae_entries_times_out_promptly_under_a_short_override`; gated the same
// way that module is, so it isn't dead code under `sim`.
#[cfg(all(test, not(feature = "sim")))]
thread_local! {
    /// Per-thread override of [`REQUEST_TIMEOUT`], set only by
    /// [`with_request_timeout`] in tests. Production code never touches
    /// this; [`request_timeout`] always returns the real constant outside
    /// `#[cfg(test)]`.
    static REQUEST_TIMEOUT_OVERRIDE: std::cell::Cell<Option<Duration>> =
        const { std::cell::Cell::new(None) };
}

/// The request timeout every `ae_*`/`request_state` call races against:
/// [`REQUEST_TIMEOUT`], unless a test installed a shorter override on this
/// thread via [`with_request_timeout`].
fn request_timeout() -> Duration {
    #[cfg(all(test, not(feature = "sim")))]
    {
        if let Some(over) = REQUEST_TIMEOUT_OVERRIDE.with(std::cell::Cell::get) {
            return over;
        }
    }
    REQUEST_TIMEOUT
}

/// Test-only seam for [`request_timeout`]: polls `fut` with `duration` in
/// effect on this thread for [`request_timeout`] to read, restoring the
/// real [`REQUEST_TIMEOUT`] once `fut` resolves. `duration` is set before
/// `fut` is ever polled and cleared only after, so it's in effect for
/// `fut`'s whole lifetime, not just the moment this function is called.
/// Doesn't change production behavior, which never reads the override.
#[cfg(all(test, not(feature = "sim")))]
async fn with_request_timeout<T>(
    duration: Duration,
    fut: impl std::future::Future<Output = T>,
) -> T {
    REQUEST_TIMEOUT_OVERRIDE.with(|cell| cell.set(Some(duration)));
    let result = fut.await;
    REQUEST_TIMEOUT_OVERRIDE.with(|cell| cell.set(None));
    result
}

/// Wraps a timed-out request/response exchange as a [`CodecError::Io`], the
/// same shape a genuine connection failure produces.
fn request_timeout_error(what: &str, timeout: Duration) -> CodecError {
    CodecError::Io(io::Error::new(
        io::ErrorKind::TimedOut,
        format!("{what} did not complete within {timeout:?}"),
    ))
}

/// Backpressure class for a fan-out message, selecting the per-class drop
/// policy on outbox overflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MsgClass {
    /// Overflow drops the oldest queued invalidation: a storm on a dead peer
    /// must never stall writers.
    Invalidate,
    /// Overflow drops the new message and marks the peer dirty so the next
    /// anti-entropy round targets it first.
    Replicate,
}

/// One inbound message, tagged with the peer it arrived from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundMsg {
    /// The peer that sent `msg`.
    pub from: NodeId,
    pub msg: Msg,
}

/// One anti-entropy digest-exchange reply, as [`Mesh::ae_round`] collects
/// them: a mismatched bucket's full listing, or, once too large for that,
/// an IBLT sketch for the initiator to peel against its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AeMismatch {
    /// `(bucket, entries)`: the bucket's full listing.
    Bucket(u16, Vec<(Bytes, Hlc)>),
    /// `(bucket, cells)`: an IBLT sketch over the bucket.
    Sketch(u16, Vec<Cell>),
    /// `(bucket, digests)`: the bucket's 64 part digests, sent instead of a
    /// listing or sketch once the bucket's entry count passed
    /// `ClusterConfig::ae_part_min_bucket`. The initiator compares these
    /// against its own part digests and requests only the mismatched parts
    /// via [`Mesh::ae_parts`].
    PartDigests(u16, Vec<u64>),
}

impl AeMismatch {
    /// The bucket this reply covers, regardless of which shape it took.
    #[must_use]
    pub const fn bucket(&self) -> u16 {
        match self {
            Self::Bucket(bucket, _) | Self::Sketch(bucket, _) | Self::PartDigests(bucket, _) => {
                *bucket
            }
        }
    }
}

/// One reply to [`Mesh::ae_parts`]: a mismatched part's full listing, or,
/// once too large for that, an IBLT sketch, the part-grained counterpart of
/// [`AeMismatch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AePartReply {
    /// The part's full `(key, version)` listing.
    Listing {
        bucket: u16,
        part: u8,
        entries: Vec<(Bytes, Hlc)>,
    },
    /// An IBLT sketch over the part.
    Sketch {
        bucket: u16,
        part: u8,
        cells: Vec<Cell>,
    },
}

/// What the net layer needs from the local shard registry to answer
/// another node's state-transfer or anti-entropy request. An unknown cache
/// degrades to an empty result rather than an error: a normal race, not a
/// fault.
pub trait RequestHandler: Send + Sync + 'static {
    /// Streams a full snapshot of `cache` in write-sized chunks, for state
    /// transfer on join.
    fn snapshot_chunks(&self, cache: SmolStr) -> BoxStream<'static, Vec<WireRecord>>;
    /// Returns `cache`'s current per-bucket digest array.
    fn digests(&self, cache: SmolStr) -> BoxFuture<'_, Vec<(u16, u64)>>;
    /// Returns the live key/version listing for one bucket of `cache`.
    fn bucket_entries(&self, cache: SmolStr, bucket: u16) -> BoxFuture<'_, Vec<(Bytes, Hlc)>>;
    /// [`RequestHandler::bucket_entries`] for many buckets in one shard pass.
    fn entries_for_buckets(
        &self,
        cache: SmolStr,
        buckets: Vec<u16>,
    ) -> BoxFuture<'_, crate::store::BucketEntries>;
    /// Returns full records for `keys` in `cache` that this node holds.
    fn records_for(&self, cache: SmolStr, keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>>;

    /// Returns the entry count of each of `buckets` in `cache`, without
    /// materializing their contents: the cheap check `serve_ae_digest` makes
    /// before deciding whether a mismatched bucket is answered with part
    /// digests instead of a listing or sketch.
    fn bucket_lens(&self, cache: SmolStr, buckets: Vec<u16>) -> BoxFuture<'_, Vec<(u16, usize)>>;

    /// Returns `cache`'s part digests for each of `buckets`: `(bucket, 64
    /// part-digests)` per bucket, the responder's step-2 reply for a bucket
    /// past [`RequestHandler::ae_part_min_bucket`] entries.
    fn part_digests(
        &self,
        cache: SmolStr,
        buckets: Vec<u16>,
    ) -> BoxFuture<'_, Vec<(u16, Vec<u64>)>>;

    /// [`RequestHandler::entries_for_buckets`] at part granularity: `(key,
    /// version)` for every live entry and un-GC'd tombstone in each
    /// requested `(bucket, part)` pair of `cache`.
    fn entries_for_parts(
        &self,
        cache: SmolStr,
        parts: Vec<(u16, u8)>,
    ) -> BoxFuture<'_, crate::store::PartEntries>;

    /// Returns full records for the entries of `bucket` in `cache` whose
    /// key hash is in `hashes`: the sketch-decoded counterpart to
    /// [`RequestHandler::records_for`]'s direct-key form.
    ///
    /// The default composes [`RequestHandler::bucket_entries`] and
    /// [`RequestHandler::records_for`], paying for two lookups; override
    /// it when one lookup serves both.
    fn records_for_hashes(
        &self,
        cache: SmolStr,
        bucket: u16,
        hashes: Vec<u64>,
    ) -> BoxFuture<'_, Vec<WireRecord>> {
        Box::pin(async move {
            let wanted: std::collections::HashSet<u64> = hashes.into_iter().collect();
            let entries = self.bucket_entries(cache.clone(), bucket).await;
            let keys: Vec<Bytes> = entries
                .into_iter()
                .filter(|(key, _)| wanted.contains(&xxh3_64(key)))
                .map(|(key, _)| key)
                .collect();
            self.records_for(cache, keys).await
        })
    }

    /// Bucket size above which the responder answers a mismatch with its 64
    /// part digests instead of a listing or sketch. Mirrors
    /// [`crate::config::ClusterConfig::ae_part_min_bucket`].
    fn ae_part_min_bucket(&self) -> usize {
        crate::config::ClusterConfig::default().ae_part_min_bucket
    }

    /// Bucket size above which the responder answers with an IBLT sketch
    /// instead of its full listing. Mirrors
    /// [`crate::config::ClusterConfig::ae_sketch_min_bucket`].
    fn ae_sketch_min_bucket(&self) -> usize {
        crate::config::ClusterConfig::default().ae_sketch_min_bucket
    }

    /// Sketch size, in cells, the responder builds an `Msg::AeSketch` reply
    /// with. Mirrors [`crate::config::ClusterConfig::ae_sketch_cells`]'s
    /// default.
    fn ae_sketch_cells(&self) -> usize {
        crate::config::ClusterConfig::default().ae_sketch_cells
    }
}

struct PeerHandle {
    data_addr: SocketAddr,
    invalidate: Arc<DropOldestQueue<OutFrame>>,
    replicate_tx: mpsc::Sender<OutFrame>,
    dirty: Arc<AtomicBool>,
    /// [`mono_ms`] + 1 of the last `Replicate` frame enqueued, `0` if none.
    /// Read by [`Mesh::replicate_in_flight`].
    last_replicate_enqueued: AtomicU64,
    /// Anti-entropy digests from this peer answered empty in a row, for
    /// [`MeshInner::defers_ae_digest_from`]'s bound.
    ae_deferrals: AtomicU32,
    cancel: CancellationToken,
    /// Pooled request/response connections for this peer, dialed on
    /// demand, dropped with this handle when the peer departs.
    req_pool: Arc<conn::ReqPool>,
}

struct MeshInner {
    node: NodeId,
    incarnation: u64,
    outbox_capacity: usize,
    peers: RwLock<HashMap<NodeId, PeerHandle>>,
    accept_cancel: CancellationToken,
    tls: TlsCtx,
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

    /// A mesh with no peers, for `conn`'s tests that drive
    /// `handle_accepted` over a raw accepted stream.
    #[cfg(all(test, not(feature = "sim")))]
    pub(super) fn for_tests(tls: TlsCtx, accept_cancel: CancellationToken) -> Arc<Self> {
        Arc::new(Self {
            node: NodeId::from(1),
            incarnation: 1,
            outbox_capacity: DEFAULT_CHANNEL_CAPACITY,
            peers: RwLock::new(HashMap::new()),
            accept_cancel,
            tls,
        })
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
        #[cfg(all(feature = "tls", not(feature = "sim")))]
        let tls = build_tls_ctx(config, bind_addr)?;
        #[cfg(not(all(feature = "tls", not(feature = "sim"))))]
        let tls = build_tls_ctx(config);

        let outbox_capacity = if config.outbox_capacity == 0 {
            DEFAULT_CHANNEL_CAPACITY
        } else {
            config.outbox_capacity
        };
        let (inbound_tx, inbound_rx) = mpsc::channel(outbox_capacity);
        let inner = Arc::new(MeshInner {
            node,
            incarnation,
            outbox_capacity,
            peers: RwLock::new(HashMap::new()),
            accept_cancel: CancellationToken::new(),
            tls,
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

    /// Refreshes the set of peers the mesh dials and fans traffic out to:
    /// spawns a writer for each newly seen peer, cancels it for any that
    /// departed or changed `data_addr`. Called on every
    /// [`crate::membership::Membership::peers`] change.
    ///
    /// # Panics
    ///
    /// Panics if the peer-table lock is poisoned.
    pub fn update_peers(&self, peers: Vec<Peer>) {
        let incoming: HashMap<NodeId, SocketAddr> = peers
            .into_iter()
            .filter(|peer| peer.node != self.inner.node)
            .map(|peer| (peer.node, peer.data_addr))
            .collect();

        let mut table = self
            .inner
            .peers
            .write()
            .expect("invariant: peers lock is never poisoned");
        table.retain(|node, handle| {
            let keep = incoming.get(node) == Some(&handle.data_addr);
            if !keep {
                handle.cancel.cancel();
            }
            keep
        });
        for (node, data_addr) in incoming {
            table
                .entry(node)
                .or_insert_with(|| self.spawn_peer_handle(data_addr));
        }
    }

    fn spawn_peer_handle(&self, data_addr: SocketAddr) -> PeerHandle {
        let invalidate = Arc::new(DropOldestQueue::new(self.inner.outbox_capacity));
        let (replicate_tx, replicate_rx) = mpsc::channel(self.inner.outbox_capacity);
        let cancel = CancellationToken::new();
        tokio::spawn(conn::run_peer_writer(
            self.inner.node,
            self.inner.incarnation,
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
            last_replicate_enqueued: AtomicU64::new(0),
            ae_deferrals: AtomicU32::new(0),
            cancel,
            req_pool: Arc::new(conn::ReqPool::new()),
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
        let frames = msgs.into_iter().filter_map(|msg| match OutFrame::new(msg) {
            Ok(frame) => Some(frame),
            Err(error) => {
                tracing::warn!(%error, "failed to encode outbound message; dropped");
                None
            }
        });
        self.send_frames(peer, class, frames);
    }

    /// [`Mesh::send_many`], but for already-encoded [`OutFrame`]s, so
    /// broadcast content is encoded once regardless of peer count.
    /// Otherwise identical to [`Mesh::send_many`].
    ///
    /// # Panics
    ///
    /// Panics if the peer-table lock is poisoned.
    pub(crate) fn send_frames(
        &self,
        peer: NodeId,
        class: MsgClass,
        frames: impl IntoIterator<Item = OutFrame>,
    ) {
        let table = self
            .inner
            .peers
            .read()
            .expect("invariant: peers lock is never poisoned");
        let Some(handle) = table.get(&peer) else {
            return;
        };
        let mut enqueued_replicate = false;
        for frame in frames {
            match class {
                MsgClass::Invalidate => handle.invalidate.push(frame),
                MsgClass::Replicate => {
                    enqueued_replicate = true;
                    if let Err(mpsc::error::TrySendError::Full(_)) =
                        handle.replicate_tx.try_send(frame)
                    {
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

    fn peer_req_pool(&self, peer: NodeId) -> Result<(SocketAddr, Arc<conn::ReqPool>), CodecError> {
        self.inner
            .peers
            .read()
            .expect("invariant: peers lock is never poisoned")
            .get(&peer)
            .map(|handle| (handle.data_addr, Arc::clone(&handle.req_pool)))
            .ok_or_else(|| {
                CodecError::Io(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("peer {peer} is not a known mesh member"),
                ))
            })
    }

    /// Checks a pooled, already-`Hello`'d connection out of `peer`'s pool
    /// and sends `first` on it, or dials a fresh one, coalescing `Hello`
    /// and `first` into one flush. Returns the connection with its pool.
    async fn acquire_conn(
        &self,
        peer: NodeId,
        first: Msg,
    ) -> Result<(conn::PeerFramed, Arc<conn::ReqPool>), CodecError> {
        let (addr, pool) = self.peer_req_pool(peer)?;
        while let Some(mut framed) = pool.checkout() {
            // A fresh dial has a real yield point; a reused connection has
            // none. Without this yield, an anti-entropy loop that skips
            // dialing can hog a single-threaded runtime turn after turn.
            tokio::task::yield_now().await;
            if conn::send_msg(&mut framed, &first).await.is_ok() {
                return Ok((framed, pool));
            }
            // A stale or broken pooled connection: try the next one, or
            // fall through to a fresh dial if none are left.
        }
        let (node, incarnation, tls) = (self.inner.node, self.inner.incarnation, &self.inner.tls);
        let framed = conn::dial_with_hello_and(addr, node, incarnation, tls, first).await?;
        Ok((framed, pool))
    }

    /// Requests a full snapshot of `cache` from `donor`, for state transfer
    /// on join, and returns a stream of its `StChunk`s. Reads lazily off a
    /// fresh connection as the caller polls.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] if `donor` is unknown or the request cannot be
    /// sent.
    pub async fn request_state(
        &self,
        donor: NodeId,
        cache: SmolStr,
    ) -> Result<BoxStream<'static, Result<Vec<WireRecord>, CodecError>>, CodecError> {
        // Only the checkout-or-dial step is bounded here; `try_donor`'s own
        // `PER_DONOR_BUDGET` governs the full snapshot stream instead.
        let timeout = request_timeout();
        let (framed, pool) =
            tokio::time::timeout(timeout, self.acquire_conn(donor, Msg::StRequest { cache }))
                .await
                .unwrap_or_else(|_| {
                    Err(request_timeout_error("state transfer request", timeout))
                })?;
        Ok(conn::state_stream(framed, pool))
    }

    /// Runs one anti-entropy digest exchange against `peer`: sends
    /// `local_buckets` and returns the reply for every mismatched bucket.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] if `peer` is unknown or the exchange fails.
    pub async fn ae_round(
        &self,
        peer: NodeId,
        cache: SmolStr,
        local_buckets: Vec<(u16, u64)>,
    ) -> Result<Vec<AeMismatch>, CodecError> {
        let timeout = request_timeout();
        tokio::time::timeout(timeout, async {
            let (framed, pool) = self
                .acquire_conn(
                    peer,
                    Msg::AeDigest {
                        cache,
                        buckets: local_buckets,
                    },
                )
                .await?;
            conn::collect_ae_mismatches(framed, &pool).await
        })
        .await
        .unwrap_or_else(|_| {
            Err(request_timeout_error(
                "anti-entropy digest exchange",
                timeout,
            ))
        })
    }

    /// The `AeSketch` fallback: full `(key, version)` listings for
    /// `buckets` whose `AeSketch` reply failed to decode, answered like
    /// [`Mesh::ae_round`].
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] if `peer` is unknown or the exchange fails.
    pub async fn ae_entries(
        &self,
        peer: NodeId,
        cache: SmolStr,
        buckets: Vec<u16>,
    ) -> Result<Vec<(u16, Vec<(Bytes, Hlc)>)>, CodecError> {
        let timeout = request_timeout();
        tokio::time::timeout(timeout, async {
            let (framed, pool) = self
                .acquire_conn(peer, Msg::AeEntries { cache, buckets })
                .await?;
            conn::collect_ae_buckets(framed, &pool).await
        })
        .await
        .unwrap_or_else(|_| {
            Err(request_timeout_error(
                "anti-entropy sketch-fallback listing",
                timeout,
            ))
        })
    }

    /// Requests the mismatched `(bucket, part)` pairs found by comparing a
    /// peer's [`AeMismatch::PartDigests`] reply against this node's own part
    /// digests: the third step of the part path, answered one
    /// [`AePartReply`] per part.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] if `peer` is unknown or the exchange fails.
    pub async fn ae_parts(
        &self,
        peer: NodeId,
        cache: SmolStr,
        parts: Vec<(u16, u8)>,
    ) -> Result<Vec<AePartReply>, CodecError> {
        let timeout = request_timeout();
        tokio::time::timeout(timeout, async {
            let (framed, pool) = self
                .acquire_conn(peer, Msg::AeParts { cache, parts })
                .await?;
            conn::collect_ae_part_replies(framed, &pool).await
        })
        .await
        .unwrap_or_else(|_| Err(request_timeout_error("anti-entropy part exchange", timeout)))
    }

    /// Pulls full records for `keys` from `peer`, the `AePull` step.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] if `peer` is unknown or the exchange fails.
    pub async fn ae_pull(
        &self,
        peer: NodeId,
        cache: SmolStr,
        keys: Vec<Bytes>,
    ) -> Result<Vec<WireRecord>, CodecError> {
        let timeout = request_timeout();
        tokio::time::timeout(timeout, async {
            let (framed, pool) = self.acquire_conn(peer, Msg::AePull { cache, keys }).await?;
            conn::collect_pulled_records(framed, &pool).await
        })
        .await
        .unwrap_or_else(|_| Err(request_timeout_error("anti-entropy pull", timeout)))
    }

    /// Pulls records from `peer` for `bucket`'s entries whose key hash is
    /// in `hashes`, the sketch-decoded counterpart to [`Mesh::ae_pull`].
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] if `peer` is unknown or the exchange fails.
    pub async fn ae_pull_hashes(
        &self,
        peer: NodeId,
        cache: SmolStr,
        bucket: u16,
        hashes: Vec<u64>,
    ) -> Result<Vec<WireRecord>, CodecError> {
        let timeout = request_timeout();
        tokio::time::timeout(timeout, async {
            let (framed, pool) = self
                .acquire_conn(
                    peer,
                    Msg::AePullHashes {
                        cache,
                        bucket,
                        hashes,
                    },
                )
                .await?;
            conn::collect_pulled_records(framed, &pool).await
        })
        .await
        .unwrap_or_else(|_| Err(request_timeout_error("anti-entropy pull-by-hash", timeout)))
    }

    /// Shuts down the mesh: stops accepting, cancels every per-peer writer.
    /// Signals the spawned background work to stop without waiting on sockets.
    ///
    /// # Panics
    ///
    /// Panics if the peer-table lock is poisoned.
    pub async fn shutdown(self) {
        self.inner.accept_cancel.cancel();
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
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use futures::StreamExt as _;
    use tokio::net::{TcpListener, TcpStream};
    use tokio_util::codec::LengthDelimitedCodec;

    use super::*;
    use crate::node::NodeName;
    use crate::wire::{self, MAX_FRAME};

    struct FixtureHandler {
        records: Vec<WireRecord>,
        digests: Vec<(u16, u64)>,
        bucket_entries: Vec<(Bytes, Hlc)>,
        pulled: Mutex<Vec<(SmolStr, Vec<Bytes>)>>,
        bucket_lens: Vec<(u16, usize)>,
        part_digests: Vec<(u16, Vec<u64>)>,
        part_entries: crate::store::PartEntries,
        ae_part_min_bucket: usize,
    }

    impl Default for FixtureHandler {
        fn default() -> Self {
            Self {
                records: Vec::new(),
                digests: Vec::new(),
                bucket_entries: Vec::new(),
                pulled: Mutex::new(Vec::new()),
                bucket_lens: Vec::new(),
                part_digests: Vec::new(),
                part_entries: Vec::new(),
                ae_part_min_bucket: ClusterConfig::default().ae_part_min_bucket,
            }
        }
    }

    impl RequestHandler for FixtureHandler {
        fn snapshot_chunks(&self, _cache: SmolStr) -> BoxStream<'static, Vec<WireRecord>> {
            Box::pin(futures::stream::iter(vec![self.records.clone()]))
        }

        fn digests(&self, _cache: SmolStr) -> BoxFuture<'_, Vec<(u16, u64)>> {
            Box::pin(async { self.digests.clone() })
        }

        fn bucket_entries(
            &self,
            _cache: SmolStr,
            _bucket: u16,
        ) -> BoxFuture<'_, Vec<(Bytes, Hlc)>> {
            Box::pin(async { self.bucket_entries.clone() })
        }

        fn entries_for_buckets(
            &self,
            _cache: SmolStr,
            buckets: Vec<u16>,
        ) -> BoxFuture<'_, crate::store::BucketEntries> {
            Box::pin(async move {
                buckets
                    .into_iter()
                    .map(|bucket| (bucket, self.bucket_entries.clone()))
                    .collect()
            })
        }

        fn records_for(&self, cache: SmolStr, keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>> {
            self.pulled
                .lock()
                .expect("invariant: fixture mutex is never poisoned")
                .push((cache, keys));
            Box::pin(async { self.records.clone() })
        }

        fn bucket_lens(
            &self,
            _cache: SmolStr,
            buckets: Vec<u16>,
        ) -> BoxFuture<'_, Vec<(u16, usize)>> {
            Box::pin(async move {
                let fixture = &self.bucket_lens;
                buckets
                    .into_iter()
                    .map(|bucket| {
                        let len = fixture
                            .iter()
                            .find(|(b, _)| *b == bucket)
                            .map_or(0, |(_, len)| *len);
                        (bucket, len)
                    })
                    .collect()
            })
        }

        fn part_digests(
            &self,
            _cache: SmolStr,
            buckets: Vec<u16>,
        ) -> BoxFuture<'_, Vec<(u16, Vec<u64>)>> {
            Box::pin(async move {
                let fixture = &self.part_digests;
                buckets
                    .into_iter()
                    .map(|bucket| {
                        let digests = fixture
                            .iter()
                            .find(|(b, _)| *b == bucket)
                            .map_or_else(Vec::new, |(_, d)| d.clone());
                        (bucket, digests)
                    })
                    .collect()
            })
        }

        fn entries_for_parts(
            &self,
            _cache: SmolStr,
            parts: Vec<(u16, u8)>,
        ) -> BoxFuture<'_, crate::store::PartEntries> {
            Box::pin(async move {
                let fixture = &self.part_entries;
                parts
                    .into_iter()
                    .map(|key| {
                        let entries = fixture
                            .iter()
                            .find(|(k, _)| *k == key)
                            .map_or_else(Vec::new, |(_, e)| e.clone());
                        (key, entries)
                    })
                    .collect()
            })
        }

        fn ae_part_min_bucket(&self) -> usize {
            self.ae_part_min_bucket
        }
    }

    fn sample_record(n: u8) -> WireRecord {
        WireRecord {
            key: Bytes::from(vec![n]),
            value: Some(Bytes::from(vec![n, n])),
            ver: Hlc {
                wall_ms: u64::from(n),
                logical: 0,
                node: NodeId::from(u64::from(n)),
            },
            expires_at_ms: None,
        }
    }

    async fn spawn_mesh(
        node: NodeId,
        handler: Arc<dyn RequestHandler>,
    ) -> (Mesh, mpsc::Receiver<InboundMsg>) {
        let addr: SocketAddr = "127.0.0.1:0".parse().expect("valid loopback addr");
        Mesh::spawn(addr, node, 1, &ClusterConfig::default(), handler)
            .await
            .expect("bind loopback")
    }

    fn empty_handler() -> Arc<dyn RequestHandler> {
        Arc::new(FixtureHandler {
            records: Vec::new(),
            digests: Vec::new(),
            bucket_entries: Vec::new(),
            pulled: Mutex::new(Vec::new()),
            ..Default::default()
        })
    }

    fn peer_at(node: NodeId, addr: SocketAddr) -> Peer {
        Peer {
            node,
            name: NodeName::new("test", node),
            gossip_addr: addr,
            data_addr: addr,
            incarnation: 1,
        }
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
                incarnation: 1
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

    #[tokio::test]
    async fn state_transfer_roundtrip() {
        let records = vec![sample_record(1), sample_record(2)];
        let handler = Arc::new(FixtureHandler {
            records: records.clone(),
            digests: Vec::new(),
            bucket_entries: Vec::new(),
            pulled: Mutex::new(Vec::new()),
            ..Default::default()
        });
        let (donor, _donor_inbound) = spawn_mesh(NodeId::from(1), handler).await;
        let (requester, _req_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
        requester.update_peers(vec![peer_at(NodeId::from(1), donor.local_addr())]);

        let mut stream = requester
            .request_state(NodeId::from(1), SmolStr::new("users"))
            .await
            .expect("request accepted");
        let mut got = Vec::new();
        while let Some(chunk) = stream.next().await {
            got.extend(chunk.expect("chunk decodes"));
        }
        assert_eq!(got, records);
    }

    #[tokio::test]
    async fn state_transfer_stream_reports_an_error_on_truncated_connection() {
        // A hand-rolled "donor" that drops the connection mid-stream,
        // without a `done: true` chunk: the requester must see an error.
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("listener has a local addr");

        let donor = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut framed = LengthDelimitedCodec::builder()
                .max_frame_length(MAX_FRAME)
                .new_framed(stream);
            let _hello = framed.next().await.expect("hello arrives");
            let _request = framed.next().await.expect("st request arrives");
            let chunk = Msg::StChunk {
                cache: SmolStr::new("users"),
                recs: vec![sample_record(1)],
                done: false,
            };
            let encoded = wire::encode(&chunk).expect("encodes");
            futures::SinkExt::send(&mut framed, encoded)
                .await
                .expect("send partial chunk");
            drop(framed); // connection closes: no `done: true` chunk ever sent
        });

        let (requester, _req_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
        requester.update_peers(vec![peer_at(NodeId::from(1), addr)]);

        let mut stream = requester
            .request_state(NodeId::from(1), SmolStr::new("users"))
            .await
            .expect("request accepted");
        let first = stream.next().await.expect("first chunk arrives");
        assert_eq!(first.expect("decodes"), vec![sample_record(1)]);
        let second = stream
            .next()
            .await
            .expect("stream ends with an error, not silently");
        assert!(second.is_err(), "truncated stream must surface as an error");

        donor.await.expect("donor did not panic");
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

        assert_eq!(result, vec![AeMismatch::Bucket(1, entries)]);
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
            part_entries: vec![((1, 2), small_entries.clone()), ((1, 3), big_entries)],
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
                AePartReply::Listing { bucket: 1, part: 2, entries } if *entries == small_entries
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
            vec![AeMismatch::Bucket(1, entries.clone())],
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
            super::REQUEST_TIMEOUT + Duration::from_secs(5),
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

        assert_eq!(got, vec![(3, entries.clone()), (7, entries)]);
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

        let frames_before = frames_sent_total();
        let bytes_before = bytes_sent_total();
        let got = client
            .ae_pull(
                NodeId::from(1),
                SmolStr::new("users"),
                vec![Bytes::from_static(b"any")],
            )
            .await
            .expect("ae_pull succeeds");
        let frames = frames_sent_total() - frames_before;
        let bytes = bytes_sent_total() - bytes_before;
        assert_eq!(got, records, "every pulled record arrives, in order");
        assert!(
            frames < 100,
            "a 3000-record pull reply travels as a few batch frames, not one per record: {frames}"
        );
        assert!(
            bytes > 0,
            "sending 3000 records' worth of frames must grow the byte counter"
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
    async fn mark_dirty_shows_up_in_the_next_take() {
        let (mesh, _inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
        mesh.update_peers(vec![peer_at(
            NodeId::from(2),
            SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 1)),
        )]);
        assert!(mesh.take_dirty_peers().is_empty());
        mesh.mark_dirty(NodeId::from(2));
        assert_eq!(mesh.take_dirty_peers(), vec![NodeId::from(2)]);
        assert!(mesh.take_dirty_peers().is_empty(), "taking clears the mark");
        mesh.mark_dirty(NodeId::from(42));
        assert!(
            mesh.take_dirty_peers().is_empty(),
            "an unknown peer is ignored"
        );
    }

    /// A handler whose `entries_for_buckets` never resolves, for
    /// [`ae_entries_times_out_promptly_under_a_short_override`].
    struct HangingHandler;
    impl RequestHandler for HangingHandler {
        fn snapshot_chunks(&self, _cache: SmolStr) -> BoxStream<'static, Vec<WireRecord>> {
            Box::pin(futures::stream::empty())
        }
        fn digests(&self, _cache: SmolStr) -> BoxFuture<'_, Vec<(u16, u64)>> {
            Box::pin(async { Vec::new() })
        }
        fn bucket_entries(
            &self,
            _cache: SmolStr,
            _bucket: u16,
        ) -> BoxFuture<'_, Vec<(Bytes, Hlc)>> {
            Box::pin(async { Vec::new() })
        }
        fn entries_for_buckets(
            &self,
            _cache: SmolStr,
            _buckets: Vec<u16>,
        ) -> BoxFuture<'_, crate::store::BucketEntries> {
            Box::pin(std::future::pending())
        }
        fn records_for(
            &self,
            _cache: SmolStr,
            _keys: Vec<Bytes>,
        ) -> BoxFuture<'_, Vec<WireRecord>> {
            Box::pin(async { Vec::new() })
        }
        fn bucket_lens(
            &self,
            _cache: SmolStr,
            _buckets: Vec<u16>,
        ) -> BoxFuture<'_, Vec<(u16, usize)>> {
            Box::pin(async { Vec::new() })
        }
        fn part_digests(
            &self,
            _cache: SmolStr,
            _buckets: Vec<u16>,
        ) -> BoxFuture<'_, Vec<(u16, Vec<u64>)>> {
            Box::pin(async { Vec::new() })
        }
        fn entries_for_parts(
            &self,
            _cache: SmolStr,
            _parts: Vec<(u16, u8)>,
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
        // must cancel the writer task that was spawned for it.
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
}

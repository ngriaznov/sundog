//! Typed shards over the store engine: versioned apply, conflict resolution,
//! tombstones, and the per-bucket digests anti-entropy compares.
//!
//! A `Shard` holds no network handle. Every local write pushes its key onto the
//! shard's fan-out queue and publishes an `Origin::Local` [`Event`]; the
//! cluster layer turns those into wire traffic.

use std::collections::HashSet;
use std::hash::Hash;
use std::sync::{Arc, Mutex as StdMutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::{self, BoxStream, StreamExt};
use serde::Serialize;
use serde::de::DeserializeOwned;
use smol_str::SmolStr;
use tokio::sync::broadcast;
use xxhash_rust::xxh3::xxh3_64;

use crate::config::ClusterConfig;
use crate::error::{CacheError, CodecError};
use crate::hlc::{Hlc, HlcClock};
use crate::node::NodeId;
use crate::wire::{self, MAX_FRAME, WireRecord};

mod engine;
use engine::{ApplyOutcome, Engine, JoinOutcome};

/// A sequential, single-threaded reference model of everything downstream of a
/// successful wire decode. Shared by `sundog-fuzz`'s fuzz targets and
/// `store::prop_tests`, so the semantics are written once. `#[doc(hidden)]`: a
/// testing/fuzzing seam, not API.
#[cfg(any(test, feature = "fuzzing"))]
#[doc(hidden)]
pub mod model;

/// Number of anti-entropy buckets per shard: `bucket(k) = xxh3(key_bytes) &
/// (BUCKET_COUNT - 1)`.
pub const BUCKET_COUNT: usize = 1024;

/// A custom per-entry weigher for size-bounded eviction: `(key, value) ->
/// weight`. Boxed so [`crate::cache::CacheBuilder::weigher`] and
/// [`Shard::with_weigher`] can store one before its closure type is nameable.
pub(crate) type Weigher<K, V> = Box<dyn Fn(&K, &V) -> u32 + Send + Sync>;

/// Upper bound on records per [`WireRecord`] batch yielded by
/// [`ShardOps::snapshot_chunks`], caps chunk size only for small-value caches:
/// a chunk breaks earlier once its encoded size approaches [`MAX_FRAME`].
const SNAPSHOT_CHUNK_SIZE: usize = 500;

/// Headroom reserved below [`MAX_FRAME`] for the `Msg::StChunk` envelope around
/// a snapshot chunk's records.
const SNAPSHOT_CHUNK_ENVELOPE_HEADROOM: usize = 4 * 1024;

/// Groups `records` into chunks that stay under [`MAX_FRAME`] once wrapped in a
/// `Msg::StChunk`. Splits on cumulative wire-encoded size as well as
/// [`SNAPSHOT_CHUNK_SIZE`], since a fixed record count alone undercounts caches
/// whose average value exceeds a few KiB.
fn chunk_records_for_snapshot(records: Vec<WireRecord>) -> Vec<Vec<WireRecord>> {
    let budget = MAX_FRAME.saturating_sub(SNAPSHOT_CHUNK_ENVELOPE_HEADROOM);
    let mut chunks = Vec::new();
    let mut current = Vec::new();
    let mut current_size = 0usize;
    for rec in records {
        let rec_size =
            wire::RECORD_HEADER_LEN + rec.key.len() + rec.value.as_ref().map_or(0, Bytes::len);
        let over_budget = current_size + rec_size > budget || current.len() >= SNAPSHOT_CHUNK_SIZE;
        if over_budget && !current.is_empty() {
            chunks.push(std::mem::take(&mut current));
            current_size = 0;
        }
        current_size += rec_size;
        current.push(rec);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

/// Capacity of each shard's [`Event`] broadcast channel. A subscriber that
/// falls this far behind misses events (`broadcast::error::RecvError::Lagged`)
/// instead of applying backpressure to writers.
const EVENTS_CAPACITY: usize = 1024;

/// Keys this node wrote locally and has not yet fanned out to
/// `cluster::fan_out_task`. A write appends its key; a drain takes the whole
/// backlog at once, so no channel ever drops a write. Holds keys, not values:
/// `records_for_typed` re-fetches fresh wire bytes.
pub(crate) struct FanOutQueue<K> {
    pending: StdMutex<Vec<K>>,
    notify: tokio::sync::Notify,
}

impl<K> FanOutQueue<K> {
    fn new() -> Self {
        Self {
            pending: StdMutex::new(Vec::new()),
            notify: tokio::sync::Notify::new(),
        }
    }

    fn push(&self, key: K) {
        self.pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(key);
        self.notify.notify_one();
    }

    fn extend(&self, keys: impl IntoIterator<Item = K>) {
        self.pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend(keys);
        self.notify.notify_one();
    }

    /// Takes every pending key, leaving the queue empty.
    pub(crate) fn drain(&self) -> Vec<K> {
        std::mem::take(&mut *self.pending.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// Resolves once the queue holds at least one key. A push that lands before
    /// the wait stores a permit, so nothing is ever missed.
    pub(crate) async fn wait_nonempty(&self) {
        loop {
            if !self
                .pending
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_empty()
            {
                return;
            }
            self.notify.notified().await;
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}

/// A named cache's clustering behavior: how writes fan out to other nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// No cluster traffic; entries live only on this node.
    Local,
    /// Every node caches independently; writes broadcast an
    /// [`crate::wire::Msg::Invalidate`].
    Invalidation,
    /// Every node holds every entry; writes broadcast the full
    /// [`crate::wire::Msg::Replicate`].
    Replicated,
}

impl Mode {
    /// The wire token gossiped for this mode under a `cache:<name>` chitchat
    /// key. A stable string, not a `Debug`/`Display` impl, so renaming a
    /// variant never changes the wire.
    pub(crate) const fn as_token(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Invalidation => "invalidation",
            Self::Replicated => "replicated",
        }
    }

    /// Parses [`Mode::as_token`]'s output back into a `Mode`, or `None` for
    /// anything else, so an unrecognized token skips that peer rather than
    /// failing the whole gossip round.
    pub(crate) fn from_token(token: &str) -> Option<Self> {
        match token {
            "local" => Some(Self::Local),
            "invalidation" => Some(Self::Invalidation),
            "replicated" => Some(Self::Replicated),
            _ => None,
        }
    }
}

/// Who caused a cache [`Event`]: this node's own API call, or a message
/// received from a remote peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Origin {
    /// Caused by a local `insert`/`remove`/`get_or_load` call.
    Local,
    /// Caused by an inbound wire message from the given peer.
    Remote(NodeId),
}

/// A change notification published on [`Shard::events`] / `Cache::events`.
#[derive(Debug, Clone)]
pub enum Event<K, V> {
    /// A key was inserted where none existed before.
    Created { key: K, value: V, origin: Origin },
    /// An existing key's value changed.
    Updated { key: K, value: V, origin: Origin },
    /// A key was removed; a tombstone was applied.
    Removed { key: K, origin: Origin },
}

/// Entries per bucket, as an anti-entropy exchange reports them: every
/// requested bucket present, empty lists included.
pub type BucketEntries = Vec<(u16, Vec<(Bytes, Hlc)>)>;

/// The type-erased surface the network layer drives a shard through, wire bytes
/// in and out. This is the boundary where postcard (de)serialization happens;
/// local reads never deserialize. Implemented by `Shard<K, V>` for any `K`, `V`
/// meeting its bounds, and held as `Arc<dyn ShardOps>` in the cluster's cache
/// registry.
///
/// Async methods return `BoxFuture` rather than `async fn` so `dyn ShardOps`
/// stays usable from a `HashMap<SmolStr, Arc<dyn ShardOps>>`.
pub trait ShardOps: Send + Sync {
    /// Applies an inbound replicated record iff its version is newer than
    /// what's stored, the versioned-apply rule that makes replication
    /// commutative.
    fn apply_remote(&self, rec: WireRecord) -> BoxFuture<'_, ()>;

    /// [`ShardOps::apply_remote`] for a whole batch, one lock acquisition per
    /// touched stripe rather than one per record.
    fn apply_remote_batch(&self, recs: Vec<WireRecord>) -> BoxFuture<'_, ()>;

    /// Drops the local copy of `key` iff `ver` is newer than the locally stored
    /// version. Not routed through [`ConflictResolver`]: an invalidation
    /// carries no value, so `Hlc` order is the only signal.
    fn invalidate(&self, key: Bytes, ver: Hlc) -> BoxFuture<'_, ()>;

    /// This shard's current per-bucket XOR digests, `(bucket, digest)` for all
    /// [`BUCKET_COUNT`] buckets. The first step of an anti-entropy round.
    fn digests(&self) -> BoxFuture<'_, Vec<(u16, u64)>>;

    /// `(key, version)` for every live entry and un-GC'd tombstone in `bucket`,
    /// for a peer that reported a digest mismatch there.
    fn bucket_entries(&self, bucket: u16) -> BoxFuture<'_, Vec<(Bytes, Hlc)>>;

    /// [`ShardOps::bucket_entries`] for many buckets in one pass, so an
    /// anti-entropy round stays linear in shard size instead of quadratic.
    fn entries_for_buckets(&self, buckets: Vec<u16>) -> BoxFuture<'_, BucketEntries>;

    /// The full [`WireRecord`] for each of `keys` this shard holds, present
    /// entries and tombstones alike, answering an `AePull`.
    fn records_for(&self, keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>>;

    /// Streams the shard's full contents in ~500-record chunks for state
    /// transfer to a joining node, applied on arrival through the same
    /// versioned [`ShardOps::apply_remote`] path as live traffic.
    fn snapshot_chunks(&self) -> BoxStream<'static, Vec<WireRecord>>;

    /// Garbage-collects tombstones older than `tombstone_ttl`. While
    /// `any_member_absent`, a tombstone past `tombstone_ttl` but not
    /// `tombstone_max_ttl` is left in place instead.
    fn gc_tombstones(&self, any_member_absent: bool) -> BoxFuture<'_, ()>;

    /// Runs `engine::Engine::sweep`, since the engine has no free-running sweep
    /// of its own. Called periodically by `tombstone_gc_task`, independent
    /// of read/write traffic.
    fn run_pending_tasks(&self) -> BoxFuture<'_, ()>;
}

/// One side of a [`ConflictResolver::winner`] comparison: everything a resolver
/// needs to pick a winner, at the wire level. Carries postcard-encoded value
/// bytes rather than the typed value, matching this module's "local reads never
/// deserialize" boundary.
#[derive(Debug, Clone, Copy)]
pub struct RecordView<'a> {
    /// The record's postcard-encoded value bytes, or `None` for a tombstone.
    pub value: Option<&'a [u8]>,
    /// The record's version.
    pub ver: Hlc,
    /// Absolute expiry in epoch milliseconds, or `None` for no TTL/no value.
    pub expires_at_ms: Option<u64>,
}

/// The outcome of a [`ConflictResolver::winner`] call: which argument record
/// wins, by position rather than role.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Winner {
    /// The first record (`a`) wins; `b` is discarded.
    A,
    /// The second record (`b`) wins; `a` is discarded.
    B,
}

/// Picks a winner between two differently-versioned records for the same key. A
/// resolver picks; it never merges, since a synthesized value would make
/// `Shard::apply`'s outcome depend on which two versions happened to collide
/// locally.
///
/// # Correctness contract
///
/// `Shard::apply`'s convergence guarantee transfers to a custom resolver only
/// if `winner` is deterministic (a pure function of `key`, `a`, `b`),
/// antisymmetric (`winner(key, a, b) == A` iff `winner(key, b, a) == B`, never
/// favoring argument position), and total and transitive (the "beats" relation
/// over any set of distinct-version records for one key is a strict total
/// order, with no cycle). `Shard::apply` calls `winner` only when `a.ver !=
/// b.ver`.
///
/// The default [`LwwResolver`] satisfies all three by comparing [`Hlc`] alone.
/// A resolver that violates antisymmetry or transitivity is not safe on more
/// than one replica: nothing in this crate detects the violation, and
/// convergence stops holding.
pub trait ConflictResolver: Send + Sync + 'static {
    /// Decides which of `a`, `b`, two different versions of the record stored
    /// at `key`'s wire-encoded bytes, wins. See the trait docs for the
    /// correctness contract.
    fn winner(&self, key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner;

    /// Whether `winner` reads `RecordView::value`. Defaults to `true`. Override
    /// to `false` for a resolver that, like [`LwwResolver`], only compares
    /// `ver`/`expires_at_ms`, so the versioned apply skips encoding both
    /// records' values on every apply.
    fn needs_value_bytes(&self) -> bool {
        true
    }
}

/// The default resolver: last-write-wins by [`Hlc`], ignoring value bytes and
/// `expires_at_ms` entirely.
#[derive(Debug, Clone, Copy, Default)]
pub struct LwwResolver;

impl ConflictResolver for LwwResolver {
    fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        if a.ver >= b.ver { Winner::A } else { Winner::B }
    }

    fn needs_value_bytes(&self) -> bool {
        false
    }
}

/// A stored value paired with the version it was last written at, folded into
/// the per-key version table, and its absolute expiry, so every replica
/// converts the same origin-stamped deadline into a local remaining duration.
#[derive(Debug, Clone)]
pub struct Stored<V> {
    /// The current value.
    pub value: V,
    /// `value`'s postcard-encoded bytes: the first encode's bytes on the
    /// local-origin path (`insert`/`insert_many`/`get_or_load`'s fill), the
    /// verbatim wire bytes on the replica-apply path
    /// (`apply_remote_batch`). Always equal to `postcard::to_stdvec(&
    /// value)`, or to wire bytes decoding to a structurally equal `value`.
    pub encoded: Bytes,
    /// The version this value was written at.
    pub ver: Hlc,
    /// Absolute expiry in epoch milliseconds, or `None` for no TTL.
    pub expires_at_ms: Option<u64>,
}

/// A tombstone: the version of the delete that created it, and its two GC
/// deadlines. `ttl_deadline_ms` is when it becomes eligible for ordinary
/// collection; `max_deadline_ms` is the hard cap past which it collects
/// regardless of member absence. See [`ShardOps::gc_tombstones`].
#[derive(Debug, Clone, Copy)]
struct Tombstone {
    ver: Hlc,
    ttl_deadline_ms: u64,
    max_deadline_ms: u64,
}

/// What a versioned write carries into `Shard::apply`: a live value, or a
/// deletion marker.
enum Incoming<V> {
    Put {
        value: V,
        expires_at_ms: Option<u64>,
        /// `value`'s postcard-encoded bytes. See [`Stored::encoded`] for which
        /// bytes this is on each apply path.
        encoded: Bytes,
    },
    Tombstone,
}

/// Wraps a stampede-collapsed loader failure, the type-erased `Arc<dyn Error +
/// Send + Sync>` `engine::Inflight` stores so every joined waiter returns the
/// same failure the owner saw, as a boxable [`std::error::Error`] for
/// [`CacheError::Loader`].
#[derive(Debug)]
struct SharedLoaderFailure(Arc<dyn std::error::Error + Send + Sync>);

impl std::fmt::Display for SharedLoaderFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for SharedLoaderFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn duration_ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// Worst-case postcard-encoded size of an [`Hlc`]: 10 LEB128 bytes for
/// `wall_ms: u64`, 5 for `logical: u32`, 10 for `node: NodeId` (a `u64`),
/// rounded up from 25 for headroom.
const HLC_ENCODED_MAX: usize = 32;

/// `xxh3(key_bytes ‖ postcard(ver))`, the digest contribution of one live entry
/// or tombstone. Encodes `ver` into a stack buffer, not a heap `Vec`, since
/// this runs on every apply.
fn entry_fingerprint(key_bytes: &[u8], ver: Hlc) -> u64 {
    let mut ver_buf = [0u8; HLC_ENCODED_MAX];
    let ver_bytes = postcard::to_slice(&ver, &mut ver_buf)
        .expect("invariant: Hlc always postcard-encodes within HLC_ENCODED_MAX bytes");
    let mut buf = Vec::with_capacity(key_bytes.len() + ver_bytes.len());
    buf.extend_from_slice(key_bytes);
    buf.extend_from_slice(ver_bytes);
    xxh3_64(&buf)
}

/// Postcard-encodes `key`, its wire form and digest-hash input alike.
///
/// Debug builds assert the encoding round-trips to itself. A key type whose
/// `Serialize` impl is not canonical, such as an iteration-order-dependent
/// `HashMap`-typed key, would silently corrupt digests and break wire identity.
fn encode_key<K>(key: &K) -> Result<Bytes, CodecError>
where
    K: Serialize + DeserializeOwned,
{
    let bytes = postcard::to_stdvec(key)?;
    debug_assert!(
        postcard::from_bytes::<K>(&bytes)
            .ok()
            .and_then(|decoded| postcard::to_stdvec(&decoded).ok())
            .is_some_and(|re_encoded| re_encoded == bytes),
        "a key's postcard encoding is canonical and deterministic; no map-typed keys"
    );
    Ok(Bytes::from(bytes))
}

/// A typed named cache: an `engine::Engine` of `K -> V` plus the
/// version-and-conflict machinery that backs [`ShardOps`]. The typed `Cache<K,
/// V>` handle users hold (`crate::cache`) wraps `Arc<Shard<K, V>>`.
///
/// # Bounds
///
/// `K`'s postcard encoding doubles as its wire form and its digest-hash input,
/// so it encodes deterministically. No map-typed keys.
pub struct Shard<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    name: SmolStr,
    mode: Mode,
    engine: Arc<Engine<K, V>>,
    events: broadcast::Sender<Event<K, V>>,
    /// Keys written locally and not yet fanned out; see [`FanOutQueue`]. Kept
    /// separate from `events` so its `receiver_count()` reflects only real
    /// external subscribers.
    fan_out: Arc<FanOutQueue<K>>,
    /// Guards only the synchronous HLC bump itself, never held across `.await`.
    clock: StdMutex<HlcClock>,
    /// The deterministic-clock hook every timestamp this shard stamps reads, in
    /// place of the system clock. Defaults to it in [`Shard::new`];
    /// overridden by [`Shard::with_clock`].
    clock_fn: Arc<dyn Fn() -> u64 + Send + Sync>,
    ttl: Option<Duration>,
    tombstone_ttl_ms: u64,
    tombstone_max_ttl_ms: u64,
    resolver: Arc<dyn ConflictResolver>,
    max_frame: usize,
    /// Remembered, with `tti` below, so [`Shard::with_weigher`] can rebuild
    /// `engine::Engine` from scratch: a weigher installs only at
    /// construction.
    max_capacity: u64,
    tti: Option<Duration>,
    /// Handle for `sundog_cache_hits_total{cache}`, created once here since
    /// label resolution costs more than the read path can afford per call.
    hits: metrics::Counter,
    /// Handle for `sundog_cache_misses_total{cache}`, same reason as `hits`.
    misses: metrics::Counter,
}

// Every method here is fully synchronous, with no `.await`, but stays
// `async fn` so callers and a later backend swap need no signature change.
#[allow(
    clippy::unused_async,
    clippy::unused_async_trait_impl,
    reason = "public API keeps async fn even though this backend never awaits"
)]
impl<K, V> Shard<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// Builds a new shard. `node` stamps this shard's local writes.
    ///
    /// Tombstone GC uses [`ClusterConfig::default`]'s `tombstone_ttl` until
    /// overridden via [`Shard::with_tombstone_ttl`]; `Shard::new` takes no
    /// `ClusterConfig` itself.
    #[must_use]
    pub fn new(
        name: SmolStr,
        mode: Mode,
        node: NodeId,
        max_capacity: u64,
        ttl: Option<Duration>,
        tti: Option<Duration>,
    ) -> Self {
        let engine = Arc::new(Engine::new(max_capacity, tti, None));
        let hits = metrics::counter!("sundog_cache_hits_total", "cache" => name.to_string());
        let misses = metrics::counter!("sundog_cache_misses_total", "cache" => name.to_string());

        Self {
            name,
            mode,
            engine,
            events: broadcast::channel(EVENTS_CAPACITY).0,
            fan_out: Arc::new(FanOutQueue::new()),
            clock: StdMutex::new(HlcClock::new(node)),
            clock_fn: Arc::new(now_ms),
            ttl,
            tombstone_ttl_ms: duration_ms(ClusterConfig::default().tombstone_ttl),
            tombstone_max_ttl_ms: duration_ms(ClusterConfig::default().tombstone_max_ttl),
            resolver: Arc::new(LwwResolver),
            max_frame: MAX_FRAME,
            max_capacity,
            tti,
            hits,
            misses,
        }
    }

    /// Overrides the tombstone retention period used by
    /// [`ShardOps::gc_tombstones`], defaulting
    /// to [`ClusterConfig::default`]'s value. Own-and-return, for the
    /// composition layer to thread a live cluster's configured
    /// `tombstone_ttl` through right after construction.
    #[must_use]
    pub fn with_tombstone_ttl(mut self, tombstone_ttl: Duration) -> Self {
        self.tombstone_ttl_ms = duration_ms(tombstone_ttl);
        self
    }

    /// Overrides the hard cap on tombstone retention used by
    /// [`ShardOps::gc_tombstones`].
    #[must_use]
    pub fn with_tombstone_max_ttl(mut self, tombstone_max_ttl: Duration) -> Self {
        self.tombstone_max_ttl_ms = duration_ms(tombstone_max_ttl);
        self
    }

    /// Overrides the [`ConflictResolver`] `Shard::apply` consults whenever an
    /// incoming record's version differs from what's stored. Defaults to
    /// [`LwwResolver`].
    #[must_use]
    pub fn with_resolver(mut self, resolver: Arc<dyn ConflictResolver>) -> Self {
        self.resolver = resolver;
        self
    }

    /// Overrides the hard cap [`Shard::insert`] enforces before writing a
    /// value, threaded from a live cluster's configured
    /// `ClusterConfig::max_frame`. Defaults to [`MAX_FRAME`].
    #[must_use]
    pub fn with_max_frame(mut self, max_frame: usize) -> Self {
        self.max_frame = max_frame;
        self
    }

    /// Installs a custom per-entry weigher for size-bounded eviction, in place
    /// of the default of one weight unit per entry. Rebuilds
    /// `engine::Engine` from scratch, so call this immediately after
    /// [`Shard::new`], before any reads or writes reach this shard.
    #[must_use]
    pub fn with_weigher<W>(mut self, weigher: W) -> Self
    where
        W: Fn(&K, &V) -> u32 + Send + Sync + 'static,
    {
        self.engine = Arc::new(Engine::new(
            self.max_capacity,
            self.tti,
            Some(Box::new(weigher)),
        ));
        self
    }

    /// Overrides the clock every timestamp this shard stamps reads from, in
    /// place of the system clock. Reserved for a deterministic-clock fuzz
    /// harness driving this shard's notion of time.
    #[doc(hidden)]
    #[must_use]
    pub fn with_clock(mut self, now_ms: Arc<dyn Fn() -> u64 + Send + Sync>) -> Self {
        self.clock_fn = now_ms;
        self
    }

    /// This shard's cache name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// This shard's clustering mode.
    #[must_use]
    pub const fn mode(&self) -> Mode {
        self.mode
    }

    /// The timestamp, in epoch milliseconds, this shard stamps its writes,
    /// deadlines, and sweeps with. The system clock, unless overridden by
    /// [`Shard::with_clock`].
    fn now_ms(&self) -> u64 {
        (self.clock_fn)()
    }

    fn stamp_local(&self) -> Hlc {
        self.clock
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .now(self.now_ms())
    }

    fn observe_remote(&self, remote: Hlc) {
        self.clock
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .observe(self.now_ms(), remote);
    }

    /// The absolute expiry a write stamped now carries: the per-write `ttl` if
    /// given, else the shard's configured default, else none. Only this
    /// stamp is per-cache; everything downstream reads each record's own
    /// `expires_at_ms`.
    fn expiry_for(&self, ttl: Option<Duration>) -> Option<u64> {
        ttl.or(self.ttl)
            .map(|d| self.now_ms().saturating_add(duration_ms(d)))
    }

    /// Handles the outcome of one versioned apply: a local write's fan-out and
    /// event. A `Rejected` outcome, where the incoming record lost, is a
    /// silent no-op.
    ///
    /// `notify_fan_out: false` is how [`Shard::insert_many`] and
    /// [`Shard::remove_many`] opt out of the per-write queue push in favor
    /// of one hand-off per stripe group.
    fn handle_apply_outcome(
        &self,
        outcome: ApplyOutcome<K, V>,
        origin: Origin,
        notify_fan_out: bool,
    ) {
        match outcome {
            ApplyOutcome::Rejected => {}
            ApplyOutcome::Put {
                key,
                value,
                created,
            } => {
                if notify_fan_out && matches!(origin, Origin::Local) {
                    self.fan_out.push(key.clone());
                }
                if self.events.receiver_count() > 0 {
                    let event = if created {
                        Event::Created { key, value, origin }
                    } else {
                        Event::Updated { key, value, origin }
                    };
                    let _ = self.events.send(event);
                }
            }
            ApplyOutcome::Tombstoned { key } => {
                if notify_fan_out && matches!(origin, Origin::Local) {
                    self.fan_out.push(key.clone());
                }
                if self.events.receiver_count() > 0 {
                    let _ = self.events.send(Event::Removed { key, origin });
                }
            }
        }
    }

    /// The versioned-apply core: applies `incoming` at `ver` for `key` iff the
    /// configured [`ConflictResolver`] picks it over whatever this shard
    /// currently holds, publishing the resulting [`Event`] on success.
    /// Idempotent and commutative: the single path shared by single writes,
    /// replication, state transfer, and anti-entropy repair. Bulk writes and
    /// replicated batches go through `engine::Engine::apply_many` directly
    /// instead, holding one stripe lock across a whole group.
    ///
    /// Equal versions are always a no-op: a given `(wall_ms, logical, node)`
    /// triple comes from at most one write ever, so an equal-version
    /// incoming record is already the one stored.
    async fn apply(
        &self,
        key: K,
        key_bytes: Bytes,
        ver: Hlc,
        incoming: Incoming<V>,
        origin: Origin,
    ) {
        let hash = engine::hash_key_bytes(key_bytes.as_ref());
        let bucket = engine::stripe_index_from_hash(hash);
        let outcome = self
            .engine
            .apply_many(
                bucket,
                vec![(hash, key, key_bytes, ver, incoming)],
                self.resolver.as_ref(),
                self.tombstone_ttl_ms,
                self.tombstone_max_ttl_ms,
                self.now_ms(),
            )
            .into_iter()
            .next()
            .expect("invariant: apply_many returns exactly one outcome per entry given");
        self.handle_apply_outcome(outcome, origin, true);
    }

    /// Reads `key`, without triggering read-through. A deleted key is never
    /// present, since a tombstone leaves no live entry to find.
    ///
    /// Counts `sundog_cache_hits_total{cache}` on `Some`,
    /// `sundog_cache_misses_total{cache}` on `None`.
    pub async fn get(&self, key: &K) -> Option<V> {
        if let Some(value) = self.engine.get(key, self.now_ms()) {
            self.hits.increment(1);
            Some(value)
        } else {
            self.misses.increment(1);
            None
        }
    }

    /// Reads whether `key` has a live entry, honoring expiry, without cloning
    /// the stored value. This asks about the entry as written, not against
    /// some other deadline; no read method here takes a TTL argument. An
    /// existence check, not a read: it moves neither
    /// `sundog_cache_hits_total` nor `sundog_cache_misses_total`.
    pub async fn contains_key(&self, key: &K) -> bool {
        self.engine.contains_key(key, self.now_ms())
    }

    /// The number of live entries this node currently holds. Runs the engine's
    /// sweep first, so completed TTL/TTI expiries are reflected rather than
    /// estimated. Sampled periodically by
    /// `cluster::cache_entries_gauge_task` to publish
    /// `sundog_cache_entries{cache}`.
    pub async fn entry_count(&self) -> u64 {
        let now = self.now_ms();
        self.engine.sweep(now);
        self.engine.live_entry_count()
    }

    /// A weakly consistent, point-in-time snapshot of this node's local live
    /// keys. Not a cluster view, and gives no guarantee about a key
    /// inserted concurrently with the scan. Cost is O(entries).
    #[must_use]
    pub fn keys(&self) -> Vec<K> {
        self.engine.keys(self.now_ms())
    }

    /// Reads `key`, invoking `loader` on a miss. Concurrent callers racing on
    /// the same missing key collapse into one `loader` call.
    ///
    /// Counts `sundog_cache_misses_total{cache}` exactly once per `loader`
    /// execution, from the fill's owner. Every other call, a cache hit or a
    /// collapsed caller, counts `sundog_cache_hits_total{cache}` instead.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::Loader`] if `loader` fails.
    ///
    /// # Panics
    ///
    /// Panics if a value the loader returned fails to postcard-encode, since
    /// `Shard::insert` relies on the same bound to encode any `V`.
    pub async fn get_or_load<F, E>(&self, key: &K, loader: F) -> Result<V, CacheError>
    where
        F: AsyncFnOnce(&K) -> Result<V, E>,
        E: std::error::Error + Send + Sync + 'static,
    {
        let key_bytes = encode_key(key)?;
        let hash = engine::hash_key_bytes(key_bytes.as_ref());
        loop {
            if let Some(value) = self
                .engine
                .get_by_bytes(key_bytes.as_ref(), hash, self.now_ms())
            {
                self.hits.increment(1);
                return Ok(value);
            }
            match self.engine.miss_or_join(&key_bytes, hash, self.now_ms()) {
                JoinOutcome::Hit(value) => {
                    self.hits.increment(1);
                    return Ok(value);
                }
                JoinOutcome::Join(inflight, mut done) => {
                    // `Err` means the owner's `Inflight` dropped without
                    // finishing; the next iteration takes
                    // over the load.
                    let _ = done.changed().await;
                    if let Some(err) = inflight.error.get() {
                        return Err(CacheError::Loader(Box::new(SharedLoaderFailure(
                            Arc::clone(err),
                        ))));
                    }
                    // The fast-path read above picks up the fresh value, or its
                    // absence.
                }
                JoinOutcome::Owner(inflight) => {
                    let guard =
                        self.engine
                            .guard_inflight(key_bytes.clone(), hash, Arc::clone(&inflight));
                    return match loader(key).await {
                        Ok(value) => {
                            let ver = self.stamp_local();
                            let encoded = Bytes::from(postcard::to_stdvec(&value).expect(
                                "invariant: a value returned by the loader postcard-encodes",
                            ));
                            let expires_at_ms = self.expiry_for(None);
                            self.engine.complete_fresh_load(
                                key,
                                &key_bytes,
                                hash,
                                ver,
                                value.clone(),
                                encoded,
                                expires_at_ms,
                                self.now_ms(),
                                &inflight,
                            );
                            guard.complete();
                            self.fan_out.push(key.clone());
                            if self.events.receiver_count() > 0 {
                                let _ = self.events.send(Event::Created {
                                    key: key.clone(),
                                    value: value.clone(),
                                    origin: Origin::Local,
                                });
                            }
                            self.misses.increment(1);
                            Ok(value)
                        }
                        Err(err) => {
                            let err: Arc<dyn std::error::Error + Send + Sync> = Arc::new(err);
                            self.engine.fail_inflight(
                                &key_bytes,
                                hash,
                                &inflight,
                                Arc::clone(&err),
                            );
                            guard.complete();
                            Err(CacheError::Loader(Box::new(SharedLoaderFailure(err))))
                        }
                    };
                }
            }
        }
    }

    /// [`Shard::get_or_load`] for a loader that never fails: same stampede
    /// collapse on concurrent misses, same fan-out of the fill. The
    /// `Result` remains only for [`CacheError::Codec`]; `make` itself
    /// cannot fail.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::Codec`] if `key` fails to postcard-encode.
    pub async fn get_or_insert_with<F>(&self, key: &K, make: F) -> Result<V, CacheError>
    where
        F: AsyncFnOnce(&K) -> V,
    {
        self.get_or_load(key, async move |k| {
            Ok::<V, std::convert::Infallible>(make(k).await)
        })
        .await
    }

    /// Stamps and applies a local write, then fans it out per [`Mode`]:
    /// `Invalidate` for `Mode::Invalidation`, `Replicate` for
    /// `Mode::Replicated`, nothing for `Mode::Local`, through the
    /// composition layer's subscription to [`Shard::events`].
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::ValueTooLarge`] if the wire frame this write would
    /// replicate as exceeds the configured frame cap. See
    /// [`Shard::with_max_frame`], default [`MAX_FRAME`].
    pub async fn insert(&self, key: K, value: V) -> Result<(), CacheError> {
        self.insert_expiring(key, value, None).await
    }

    /// [`Shard::insert`] with a lifespan for this entry alone, overriding the
    /// shard's default TTL. Replicates exactly as a default-TTL stamp does.
    ///
    /// # Errors
    ///
    /// As [`Shard::insert`].
    pub async fn insert_with_ttl(&self, key: K, value: V, ttl: Duration) -> Result<(), CacheError> {
        self.insert_expiring(key, value, Some(ttl)).await
    }

    async fn insert_expiring(
        &self,
        key: K,
        value: V,
        ttl: Option<Duration>,
    ) -> Result<(), CacheError> {
        let key_bytes = encode_key(&key)?;
        let encoded = Bytes::from(postcard::to_stdvec(&value).map_err(CodecError::from)?);
        let ver = self.stamp_local();
        let expires_at_ms = self.expiry_for(ttl);
        let wire_size = wire::replicate_frame_len(self.name.len(), key_bytes.len(), encoded.len());
        if wire_size > self.max_frame {
            return Err(CacheError::ValueTooLarge {
                cache: self.name.clone(),
                size: encoded.len(),
                limit: self.max_frame,
            });
        }
        self.apply(
            key,
            key_bytes,
            ver,
            Incoming::Put {
                value,
                expires_at_ms,
                encoded,
            },
            Origin::Local,
        )
        .await;
        Ok(())
    }

    /// [`Shard::insert`] for many entries, grouped by key stripe and applied
    /// under one lock acquisition per touched stripe rather than one per
    /// entry, so unrelated writers to other stripes are never blocked for
    /// the whole batch. Not a transaction: entries validated
    /// before an oversized one apply regardless of the error this returns.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::ValueTooLarge`] if any entry's wire frame exceeds
    /// the configured frame cap (see [`Shard::insert`]).
    pub async fn insert_many(
        &self,
        entries: impl IntoIterator<Item = (K, V)>,
    ) -> Result<(), CacheError> {
        self.insert_many_expiring(entries, None).await
    }

    /// [`Shard::insert_many`] with one lifespan applied to every entry in the
    /// batch, overriding the shard's default TTL. See
    /// [`Shard::insert_with_ttl`].
    ///
    /// # Errors
    ///
    /// As [`Shard::insert_many`].
    pub async fn insert_many_with_ttl(
        &self,
        entries: impl IntoIterator<Item = (K, V)>,
        ttl: Duration,
    ) -> Result<(), CacheError> {
        self.insert_many_expiring(entries, Some(ttl)).await
    }

    async fn insert_many_expiring(
        &self,
        entries: impl IntoIterator<Item = (K, V)>,
        ttl: Option<Duration>,
    ) -> Result<(), CacheError> {
        // Stops preparing entries at the first one that fails to encode or
        // exceeds `max_frame`, but keeps everything prepared before it so
        // the apply loop below still applies those, per this method's
        // not-a-transaction contract.
        let mut prepared = Vec::new();
        let mut failure: Option<CacheError> = None;
        for (key, value) in entries {
            let key_bytes = match encode_key(&key) {
                Ok(key_bytes) => key_bytes,
                Err(err) => {
                    failure = Some(err.into());
                    break;
                }
            };
            let encoded = match postcard::to_stdvec(&value).map_err(CodecError::from) {
                Ok(bytes) => Bytes::from(bytes),
                Err(err) => {
                    failure = Some(err.into());
                    break;
                }
            };
            let ver = self.stamp_local();
            let expires_at_ms = self.expiry_for(ttl);
            let wire_size =
                wire::replicate_frame_len(self.name.len(), key_bytes.len(), encoded.len());
            if wire_size > self.max_frame {
                failure = Some(CacheError::ValueTooLarge {
                    cache: self.name.clone(),
                    size: encoded.len(),
                    limit: self.max_frame,
                });
                break;
            }
            let hash = engine::hash_key_bytes(key_bytes.as_ref());
            prepared.push((hash, key, key_bytes, ver, value, expires_at_ms, encoded));
        }

        let mut by_stripe: Vec<Vec<_>> = (0..BUCKET_COUNT).map(|_| Vec::new()).collect();
        for entry in prepared {
            by_stripe[engine::stripe_index_from_hash(entry.0)].push(entry);
        }
        let now = self.now_ms();
        let mut applied_keys: Vec<K> = Vec::new();
        for (bucket, group) in by_stripe.into_iter().enumerate() {
            if group.is_empty() {
                continue;
            }
            let entries: Vec<_> = group
                .into_iter()
                .map(
                    |(hash, key, key_bytes, ver, value, expires_at_ms, encoded)| {
                        (
                            hash,
                            key,
                            key_bytes,
                            ver,
                            Incoming::Put {
                                value,
                                expires_at_ms,
                                encoded,
                            },
                        )
                    },
                )
                .collect();
            let outcomes = self.engine.apply_many(
                bucket,
                entries,
                self.resolver.as_ref(),
                self.tombstone_ttl_ms,
                self.tombstone_max_ttl_ms,
                now,
            );
            for outcome in outcomes {
                applied_keys.extend(outcome.key().cloned());
                self.handle_apply_outcome(outcome, Origin::Local, false);
            }
            // Only the writes that landed, handed off per stripe as they
            // land, so replication streams while the rest of the fill is
            // still applying.
            self.fan_out.extend(applied_keys.drain(..));
        }
        match failure {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// Stamps and applies a local tombstone, then fans it out per [`Mode`], as
    /// [`Shard::insert`] does.
    ///
    /// # Errors
    ///
    /// Returns a [`CacheError`] if the key cannot be encoded for the wire.
    pub async fn remove(&self, key: &K) -> Result<(), CacheError> {
        let key_bytes = encode_key(key)?;
        let ver = self.stamp_local();
        self.apply(
            key.clone(),
            key_bytes,
            ver,
            Incoming::Tombstone,
            Origin::Local,
        )
        .await;
        Ok(())
    }

    /// [`Shard::remove`] for many keys at once, the tombstone counterpart of
    /// [`Shard::insert_many`]. **Not a transaction**: if a key partway through
    /// fails to encode, the keys before it are still tombstoned.
    ///
    /// # Errors
    ///
    /// Returns a [`CacheError`] if any key fails to encode for the wire.
    pub async fn remove_many(&self, keys: impl IntoIterator<Item = K>) -> Result<(), CacheError> {
        let mut prepared = Vec::new();
        for key in keys {
            let key_bytes = encode_key(&key)?;
            let ver = self.stamp_local();
            let hash = engine::hash_key_bytes(key_bytes.as_ref());
            prepared.push((hash, key, key_bytes, ver));
        }

        let mut by_stripe: Vec<Vec<_>> = (0..BUCKET_COUNT).map(|_| Vec::new()).collect();
        for entry in prepared {
            by_stripe[engine::stripe_index_from_hash(entry.0)].push(entry);
        }
        let now = self.now_ms();
        let mut applied_keys: Vec<K> = Vec::new();
        for (bucket, group) in by_stripe.into_iter().enumerate() {
            if group.is_empty() {
                continue;
            }
            let entries: Vec<_> = group
                .into_iter()
                .map(|(hash, key, key_bytes, ver)| (hash, key, key_bytes, ver, Incoming::Tombstone))
                .collect();
            let outcomes = self.engine.apply_many(
                bucket,
                entries,
                self.resolver.as_ref(),
                self.tombstone_ttl_ms,
                self.tombstone_max_ttl_ms,
                now,
            );
            for outcome in outcomes {
                applied_keys.extend(outcome.key().cloned());
                self.handle_apply_outcome(outcome, Origin::Local, false);
            }
            self.fan_out.extend(applied_keys.drain(..));
        }
        Ok(())
    }

    /// Tombstones every key this node currently holds, via
    /// [`Shard::remove_many`] over a snapshot of [`Shard::keys`], cost
    /// O(entries). An entry a peer holds that never reached
    /// this node survives untouched.
    ///
    /// # Errors
    ///
    /// As [`Shard::remove_many`].
    pub async fn clear(&self) -> Result<(), CacheError> {
        self.remove_many(self.keys()).await
    }

    /// Drops the local copy of `key` without writing a tombstone or fanning
    /// out. An escape hatch for tests and manual cache-busting; the entry
    /// may reappear on the next replicated write or anti-entropy round.
    pub async fn invalidate_local(&self, key: &K) {
        let Ok(key_bytes) = postcard::to_stdvec(key) else {
            return;
        };
        let hash = engine::hash_key_bytes(&key_bytes);
        self.engine.invalidate_local(&key_bytes, hash);
    }

    /// Subscribes to this shard's change events.
    #[must_use]
    pub fn events(&self) -> broadcast::Receiver<Event<K, V>> {
        self.events.subscribe()
    }

    /// The queue of locally written keys awaiting fan-out. `fan_out_task`'s
    /// cheaper alternative to subscribing on [`Shard::events`], since it
    /// never reads `Event`'s `value`. Carries local writes only; see
    /// [`FanOutQueue`].
    pub(crate) fn fan_out_queue(&self) -> Arc<FanOutQueue<K>> {
        Arc::clone(&self.fan_out)
    }

    /// [`ShardOps::records_for`] for callers that already hold typed `K`s, such
    /// as `cluster::fan_out_batch` re-fetching for keys read off its own
    /// `Event<K, V>`s. Encodes each key straight to the bytes
    /// `engine::Engine::record_for` looks entries up by, with no
    /// decode step on either end.
    pub(crate) async fn records_for_typed(&self, keys: &[K]) -> Vec<WireRecord> {
        let now = self.now_ms();
        keys.iter()
            .filter_map(|key| {
                let key_bytes = encode_key(key).ok()?;
                self.engine.record_for(key_bytes.as_ref(), now)
            })
            .collect()
    }
}

impl<K, V> ShardOps for Shard<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    fn apply_remote(&self, rec: WireRecord) -> BoxFuture<'_, ()> {
        Box::pin(async move { self.apply_remote_batch(vec![rec]).await })
    }

    fn apply_remote_batch(&self, recs: Vec<WireRecord>) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            // Grouping by raw key bytes' stripe needs no decode. Each group
            // keeps `recs`' relative order, so the same key always
            // lands in the same stripe in arrival order.
            type RemoteEntry<K, V> = (u64, K, Bytes, Hlc, Incoming<V>, Origin);
            let mut by_stripe: Vec<Vec<RemoteEntry<K, V>>> =
                (0..BUCKET_COUNT).map(|_| Vec::new()).collect();
            for rec in recs {
                self.observe_remote(rec.ver);
                let hash = engine::hash_key_bytes(rec.key.as_ref());
                let Ok(key) = postcard::from_bytes::<K>(&rec.key) else {
                    tracing::warn!(cache = %self.name, "apply_remote_batch: undecodable key bytes");
                    continue;
                };
                let origin = Origin::Remote(rec.ver.node);
                let incoming = match rec.value {
                    Some(value_bytes) => {
                        let Ok(value) = postcard::from_bytes::<V>(&value_bytes) else {
                            tracing::warn!(
                                cache = %self.name,
                                "apply_remote_batch: undecodable value bytes"
                            );
                            continue;
                        };
                        Incoming::Put {
                            value,
                            expires_at_ms: rec.expires_at_ms,
                            // The verbatim wire bytes, decoded above only for the resolver.
                            encoded: value_bytes,
                        }
                    }
                    None => Incoming::Tombstone,
                };
                by_stripe[engine::stripe_index_from_hash(hash)]
                    .push((hash, key, rec.key, rec.ver, incoming, origin));
            }
            let now = self.now_ms();
            for (bucket, group) in by_stripe.into_iter().enumerate() {
                if group.is_empty() {
                    continue;
                }
                let mut origins = Vec::with_capacity(group.len());
                let entries: Vec<_> = group
                    .into_iter()
                    .map(|(hash, key, key_bytes, ver, incoming, origin)| {
                        origins.push(origin);
                        (hash, key, key_bytes, ver, incoming)
                    })
                    .collect();
                let outcomes = self.engine.apply_many(
                    bucket,
                    entries,
                    self.resolver.as_ref(),
                    self.tombstone_ttl_ms,
                    self.tombstone_max_ttl_ms,
                    now,
                );
                for (outcome, origin) in outcomes.into_iter().zip(origins) {
                    self.handle_apply_outcome(outcome, origin, true);
                }
            }
        })
    }

    fn invalidate(&self, key: Bytes, ver: Hlc) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.observe_remote(ver);
            let Ok(decoded_key) = postcard::from_bytes::<K>(&key) else {
                tracing::warn!(cache = %self.name, "invalidate: undecodable key bytes");
                return;
            };
            let hash = engine::hash_key_bytes(key.as_ref());
            if self.engine.invalidate(key.as_ref(), hash, ver).is_some() {
                let _ = self.events.send(Event::Removed {
                    key: decoded_key,
                    origin: Origin::Remote(ver.node),
                });
            }
        })
    }

    fn digests(&self) -> BoxFuture<'_, Vec<(u16, u64)>> {
        let digests = self.engine.digests();
        Box::pin(async move { digests })
    }

    fn bucket_entries(&self, bucket: u16) -> BoxFuture<'_, Vec<(Bytes, Hlc)>> {
        let now = self.now_ms();
        let entries = self
            .engine
            .collect_buckets(&[bucket], now)
            .pop()
            .map(|(_, entries)| entries)
            .unwrap_or_default();
        Box::pin(async move { entries })
    }

    fn entries_for_buckets(&self, buckets: Vec<u16>) -> BoxFuture<'_, BucketEntries> {
        let now = self.now_ms();
        // Every requested bucket exactly once, ascending, so the initiator
        // learns to push even for buckets this peer holds nothing in.
        let mut wanted: Vec<u16> = buckets
            .into_iter()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        wanted.sort_unstable();
        let entries = self.engine.collect_buckets(&wanted, now);
        Box::pin(async move { entries })
    }

    fn records_for(&self, keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>> {
        let now = self.now_ms();
        let recs = keys
            .into_iter()
            .filter_map(|key_bytes| self.engine.record_for(key_bytes.as_ref(), now))
            .collect();
        Box::pin(async move { recs })
    }

    fn snapshot_chunks(&self) -> BoxStream<'static, Vec<WireRecord>> {
        let engine = Arc::clone(&self.engine);
        let now = self.now_ms();
        let fut = async move { chunk_records_for_snapshot(engine.snapshot_records(now)) };
        Box::pin(stream::once(fut).flat_map(stream::iter))
    }

    fn gc_tombstones(&self, any_member_absent: bool) -> BoxFuture<'_, ()> {
        let now = self.now_ms();
        self.engine.gc_tombstones(any_member_absent, now);
        Box::pin(async move {})
    }

    fn run_pending_tasks(&self) -> BoxFuture<'_, ()> {
        let now = self.now_ms();
        self.engine.sweep(now);
        Box::pin(async move {})
    }
}

#[cfg(test)]
mod tests {
    use rand::{RngExt as _, SeedableRng as _, rngs::StdRng};

    use super::*;

    fn shard<K, V>(node: u64) -> Shard<K, V>
    where
        K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
        V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    {
        Shard::new(
            SmolStr::new("test"),
            Mode::Replicated,
            NodeId::from(node),
            10_000,
            None,
            None,
        )
    }

    fn hlc(wall_ms: u64, node: u64) -> Hlc {
        Hlc {
            wall_ms,
            logical: 0,
            node: NodeId::from(node),
        }
    }

    fn key_bytes<K: Serialize>(key: &K) -> Bytes {
        Bytes::from(postcard::to_stdvec(key).expect("test key encodes"))
    }

    /// The anti-entropy bucket `key_bytes` hashes into.
    fn bucket_of(key_bytes: &[u8]) -> u16 {
        u16::try_from(engine::stripe_index_from_hash(engine::hash_key_bytes(
            key_bytes,
        )))
        .expect("invariant: masked to BUCKET_COUNT - 1, always fits in u16")
    }

    #[test]
    fn mode_is_copy_and_comparable() {
        assert_eq!(Mode::Local, Mode::Local);
        assert_ne!(Mode::Local, Mode::Replicated);
    }

    #[test]
    fn origin_distinguishes_local_from_remote() {
        assert_ne!(Origin::Local, Origin::Remote(NodeId::from(1)));
    }

    #[tokio::test]
    async fn newer_remote_tombstone_beats_older_local_put() {
        let s = shard::<u32, String>(1);
        s.insert(1, "a".into()).await.expect("insert");
        assert_eq!(s.get(&1).await, Some("a".into()));

        let rec = WireRecord {
            key: key_bytes(&1u32),
            value: None,
            ver: hlc(u64::MAX / 2, 2),
            expires_at_ms: None,
        };
        ShardOps::apply_remote(&s, rec).await;
        assert_eq!(s.get(&1).await, None);
    }

    #[tokio::test]
    async fn newer_remote_put_beats_older_local_tombstone() {
        let s = shard::<u32, String>(1);
        s.insert(1, "a".into()).await.expect("insert");
        s.remove(&1).await.expect("remove");
        assert_eq!(s.get(&1).await, None);

        let rec = WireRecord {
            key: key_bytes(&1u32),
            value: Some(Bytes::from_static(b"\x01b")),
            ver: hlc(u64::MAX / 2, 2),
            expires_at_ms: None,
        };
        ShardOps::apply_remote(&s, rec).await;
        assert_eq!(s.get(&1).await, Some("b".into()));
    }

    #[tokio::test]
    async fn stale_remote_writes_are_rejected_idempotently() {
        let s = shard::<u32, String>(1);
        let winner = WireRecord {
            key: key_bytes(&1u32),
            value: Some(Bytes::from_static(b"\x01x")),
            ver: hlc(1_000_000, 2),
            expires_at_ms: None,
        };
        ShardOps::apply_remote(&s, winner.clone()).await;
        assert_eq!(s.get(&1).await, Some("x".into()));

        let stale = WireRecord {
            key: key_bytes(&1u32),
            value: Some(Bytes::from_static(b"\x01y")),
            ver: hlc(1, 3),
            expires_at_ms: None,
        };
        ShardOps::apply_remote(&s, stale).await;
        assert_eq!(
            s.get(&1).await,
            Some("x".into()),
            "an older write does not overwrite a newer one"
        );

        // Re-applying the exact same winning record is also a no-op.
        ShardOps::apply_remote(&s, winner).await;
        assert_eq!(s.get(&1).await, Some("x".into()));
    }

    #[tokio::test]
    async fn invalidate_respects_newer_local_write() {
        let s = shard::<u32, String>(1);
        s.insert(1, "fresh".into()).await.expect("insert");

        // An invalidation for an old version does not evict a newer local
        // write.
        ShardOps::invalidate(&s, key_bytes(&1u32), hlc(1, 9)).await;
        assert_eq!(s.get(&1).await, Some("fresh".into()));

        // A newer invalidation does evict it, and writes no tombstone.
        ShardOps::invalidate(&s, key_bytes(&1u32), hlc(u64::MAX / 2, 9)).await;
        assert_eq!(s.get(&1).await, None);
        assert!(
            ShardOps::records_for(&s, vec![key_bytes(&1u32)])
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn insert_update_remove_emit_matching_events() {
        let s = shard::<u32, String>(1);
        let mut events = s.events();

        s.insert(1, "a".into()).await.expect("insert");
        match events.recv().await.expect("created") {
            Event::Created {
                key: 1,
                value,
                origin: Origin::Local,
            } => assert_eq!(value, "a"),
            other => panic!("unexpected event: {other:?}"),
        }

        s.insert(1, "b".into()).await.expect("insert");
        match events.recv().await.expect("updated") {
            Event::Updated {
                key: 1,
                value,
                origin: Origin::Local,
            } => assert_eq!(value, "b"),
            other => panic!("unexpected event: {other:?}"),
        }

        s.remove(&1).await.expect("remove");
        match events.recv().await.expect("removed") {
            Event::Removed {
                key: 1,
                origin: Origin::Local,
            } => {}
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn get_or_load_fills_once_and_emits_created() {
        let s = shard::<u32, String>(1);
        let mut events = s.events();
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let c = std::sync::Arc::clone(&calls);
        let loaded = s
            .get_or_load(
                &7,
                async move |_key: &u32| -> Result<String, std::convert::Infallible> {
                    c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Ok("loaded".to_string())
                },
            )
            .await
            .expect("load succeeds");
        assert_eq!(loaded, "loaded");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        match events.recv().await.expect("created") {
            Event::Created {
                key: 7,
                value,
                origin: Origin::Local,
            } => assert_eq!(value, "loaded"),
            other => panic!("unexpected event: {other:?}"),
        }

        // A hit does not call the loader again.
        let c2 = std::sync::Arc::clone(&calls);
        let hit = s
            .get_or_load(
                &7,
                async move |_key: &u32| -> Result<String, std::convert::Infallible> {
                    c2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Ok("should-not-run".to_string())
                },
            )
            .await
            .expect("hit succeeds");
        assert_eq!(hit, "loaded");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[derive(Debug, thiserror::Error)]
    #[error("loader boom")]
    struct BoomError;

    #[tokio::test]
    async fn get_or_load_propagates_loader_error() {
        let s = shard::<u32, String>(1);
        let err = s
            .get_or_load(&1, async move |_key: &u32| -> Result<String, BoomError> {
                Err(BoomError)
            })
            .await
            .expect_err("loader failed");
        assert!(matches!(err, CacheError::Loader(_)));
    }

    #[tokio::test]
    async fn get_or_load_stampede_collapses_to_one_loader_call() {
        const CONCURRENCY: usize = 64;
        let s = Arc::new(shard::<u32, String>(1));
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..CONCURRENCY {
            let s = Arc::clone(&s);
            let calls = std::sync::Arc::clone(&calls);
            tasks.spawn(async move {
                s.get_or_load(&42, async move |_key: &u32| -> Result<String, BoomError> {
                    calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    // Widens the race window so every caller waits on this one
                    // load.
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    Ok("loaded-once".to_string())
                })
                .await
                .expect("loader succeeds")
            });
        }

        let mut results = Vec::with_capacity(CONCURRENCY);
        while let Some(result) = tasks.join_next().await {
            results.push(result.expect("stampede caller does not panic"));
        }

        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the loader runs exactly once under a stampede of {CONCURRENCY} concurrent misses"
        );
        assert!(results.iter().all(|value| value == "loaded-once"));
        assert_eq!(results.len(), CONCURRENCY);
    }

    #[tokio::test]
    async fn get_or_load_stampede_with_a_failing_loader_shares_one_error_with_every_waiter() {
        const CONCURRENCY: usize = 8;
        let s = Arc::new(shard::<u32, String>(1));
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..CONCURRENCY {
            let s = Arc::clone(&s);
            let calls = std::sync::Arc::clone(&calls);
            tasks.spawn(async move {
                s.get_or_load(&99, async move |_key: &u32| -> Result<String, BoomError> {
                    calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    // Widens the race window so every caller joins this one
                    // load before it fails.
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    Err(BoomError)
                })
                .await
            });
        }

        let mut results = Vec::with_capacity(CONCURRENCY);
        while let Some(result) = tasks.join_next().await {
            results.push(result.expect("stampede caller does not panic"));
        }

        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the loader runs exactly once under a stampede of {CONCURRENCY} concurrent misses"
        );
        assert_eq!(results.len(), CONCURRENCY);
        assert!(
            results
                .iter()
                .all(|r| matches!(r, Err(CacheError::Loader(_)))),
            "every joined waiter returns the same Loader failure"
        );
    }

    /// Deterministically lands a write in the narrow gap between
    /// `get_or_load`'s fast-path miss and `Engine::miss_or_join`'s locked
    /// re-check, by giving the shard a clock whose second call (the one
    /// feeding `miss_or_join`) performs the write itself before returning a
    /// timestamp. No real concurrency, and no flakiness: the write always
    /// lands on the second clock read, deterministically.
    #[tokio::test]
    async fn get_or_load_hits_when_a_write_lands_between_the_fast_path_miss_and_the_locked_recheck()
    {
        let s0 = shard::<u32, String>(1);
        let engine_for_clock = Arc::clone(&s0.engine);
        let resolver_for_clock = Arc::clone(&s0.resolver);
        let tombstone_ttl_ms = s0.tombstone_ttl_ms;
        let tombstone_max_ttl_ms = s0.tombstone_max_ttl_ms;

        let call_idx = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let call_idx_for_clock = Arc::clone(&call_idx);
        let clock: Arc<dyn Fn() -> u64 + Send + Sync> = Arc::new(move || {
            let n = call_idx_for_clock.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n == 1 {
                let kb = key_bytes(&7u32);
                let hash = engine::hash_key_bytes(kb.as_ref());
                let bucket = engine::stripe_index_from_hash(hash);
                let value = "landed".to_string();
                let encoded = Bytes::from(postcard::to_stdvec(&value).expect("encode"));
                let _ = engine_for_clock.apply_many(
                    bucket,
                    vec![(
                        hash,
                        7u32,
                        kb,
                        hlc(1, 9),
                        Incoming::Put {
                            value,
                            expires_at_ms: None,
                            encoded,
                        },
                    )],
                    resolver_for_clock.as_ref(),
                    tombstone_ttl_ms,
                    tombstone_max_ttl_ms,
                    0,
                );
            }
            0
        });
        let s = s0.with_clock(clock);

        let loaded = s
            .get_or_load(&7, async move |_key: &u32| -> Result<String, BoomError> {
                panic!(
                    "the value already landed before this call reaches the loader; \
                     get_or_load must resolve via the locked re-check's Hit instead"
                );
            })
            .await
            .expect("resolves via the Hit path, never reaching the loader");
        assert_eq!(loaded, "landed");
        assert_eq!(
            call_idx.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "exactly two clock reads: the fast-path miss, then the locked re-check that hits"
        );
    }

    #[tokio::test]
    async fn value_too_large_is_rejected() {
        let s = shard::<u32, Vec<u8>>(1);
        let big = vec![0u8; MAX_FRAME + 1];
        let err = s
            .insert(1, big)
            .await
            .expect_err("oversized value rejected");
        assert!(matches!(err, CacheError::ValueTooLarge { .. }));
    }

    #[tokio::test]
    async fn insert_many_applies_entries_before_an_oversized_one_then_fails() {
        let s = shard::<u32, Vec<u8>>(1);
        let ok_value = vec![0u8; 4];
        let big = vec![0u8; MAX_FRAME + 1];
        let err = s
            .insert_many([(1u32, ok_value.clone()), (2u32, big)])
            .await
            .expect_err("the oversized second entry is rejected");
        assert!(matches!(err, CacheError::ValueTooLarge { .. }));
        assert_eq!(
            s.get(&1).await,
            Some(ok_value),
            "insert_many is not a transaction: the key before the oversized one already applied"
        );
    }

    #[tokio::test]
    async fn long_key_round_trips_through_the_heap_encoding_path() {
        // Longer than engine::KEY_STACK_BUF (128 bytes) once postcard-encoded,
        // so both the write and the read fall back to a heap allocation.
        let s = shard::<String, String>(1);
        let key = "k".repeat(200);
        s.insert(key.clone(), "v".into()).await.expect("insert");
        assert_eq!(s.get(&key).await, Some("v".to_string()));
        assert!(s.contains_key(&key).await);
    }

    #[tokio::test]
    async fn snapshot_chunks_covers_all_live_entries() {
        let s = shard::<u32, String>(1);
        for i in 0..5u32 {
            s.insert(i, i.to_string()).await.expect("insert");
        }
        let mut stream = ShardOps::snapshot_chunks(&s);
        let mut total = 0usize;
        while let Some(chunk) = stream.next().await {
            total += chunk.len();
        }
        assert_eq!(total, 5);
    }

    #[tokio::test]
    async fn snapshot_chunks_splits_by_byte_size_not_just_record_count() {
        let s = shard::<u32, Vec<u8>>(1);
        let value_size = 100_000;
        let record_count = 50u32;
        for i in 0..record_count {
            s.insert(i, vec![0u8; value_size]).await.expect("insert");
        }

        let mut stream = ShardOps::snapshot_chunks(&s);
        let mut chunk_count = 0usize;
        let mut total_records = 0usize;
        while let Some(chunk) = stream.next().await {
            chunk_count += 1;
            total_records += chunk.len();
            let encoded_size: usize = chunk
                .iter()
                .map(|rec| postcard::to_stdvec(rec).expect("test record encodes").len())
                .sum();
            assert!(
                encoded_size < MAX_FRAME,
                "one snapshot chunk, wrapped in a Msg::StChunk, stays under the wire frame cap"
            );
        }
        assert_eq!(total_records, record_count as usize);
        assert!(
            chunk_count > 1,
            "50 * 100KB records (5MB) does not fit in one wire-frame-bounded chunk"
        );
    }

    #[tokio::test]
    async fn roundtrip_through_shard_ops_converges_two_shards() {
        let a = shard::<u32, String>(1);
        let b = shard::<u32, String>(2);
        a.insert(42, "hello".into()).await.expect("insert");

        let recs = ShardOps::records_for(&a, vec![key_bytes(&42u32)]).await;
        assert_eq!(recs.len(), 1);
        ShardOps::apply_remote(&b, recs[0].clone()).await;
        assert_eq!(b.get(&42).await, Some("hello".into()));

        a.remove(&42).await.expect("remove");
        let recs = ShardOps::records_for(&a, vec![key_bytes(&42u32)]).await;
        assert_eq!(recs.len(), 1);
        assert!(recs[0].is_tombstone());
        ShardOps::apply_remote(&b, recs[0].clone()).await;
        assert_eq!(b.get(&42).await, None);
    }

    #[tokio::test]
    async fn apply_remote_batch_skips_undecodable_records_and_applies_the_rest() {
        let s = shard::<u32, String>(1);
        // Postcard decoding of any non-unit type fails on an empty slice: no
        // length-prefix or varint byte to even start on.
        let undecodable_key = WireRecord {
            key: Bytes::new(),
            value: Some(Bytes::from(postcard::to_stdvec("ignored").expect("encode"))),
            ver: hlc(10, 2),
            expires_at_ms: None,
        };
        let undecodable_value = WireRecord {
            key: key_bytes(&2u32),
            value: Some(Bytes::new()),
            ver: hlc(10, 2),
            expires_at_ms: None,
        };
        let valid = WireRecord {
            key: key_bytes(&3u32),
            value: Some(Bytes::from(postcard::to_stdvec("ok").expect("encode"))),
            ver: hlc(10, 2),
            expires_at_ms: None,
        };
        ShardOps::apply_remote_batch(&s, vec![undecodable_key, undecodable_value, valid]).await;

        assert_eq!(
            s.get(&3).await,
            Some("ok".to_string()),
            "the one decodable record in the batch still applies"
        );
        assert_eq!(
            s.get(&2).await,
            None,
            "a record with undecodable value bytes is skipped, not applied"
        );
        assert_digest_matches_full_recompute(&s).await;
    }

    #[tokio::test]
    async fn invalidate_with_undecodable_key_bytes_is_a_silent_no_op() {
        let s = shard::<u32, String>(1);
        s.insert(1, "a".into()).await.expect("insert");

        ShardOps::invalidate(&s, Bytes::new(), hlc(u64::MAX / 2, 2)).await;

        assert_eq!(
            s.get(&1).await,
            Some("a".into()),
            "an invalidate call for undecodable key bytes touches nothing"
        );
        assert_digest_matches_full_recompute(&s).await;
    }

    /// Two keys guaranteed to land in different stripes, for the striping tests
    /// below.
    fn two_keys_in_different_stripes() -> (u32, u32) {
        let a = 0u32;
        let a_stripe = bucket_of(&key_bytes(&a));
        let b = (1u32..10_000)
            .find(|b| bucket_of(&key_bytes(b)) != a_stripe)
            .expect("some key among the first 10,000 lands in a different stripe from key 0");
        (a, b)
    }

    #[tokio::test]
    async fn insert_stripe_locks_are_independent_per_bucket() {
        let s = Arc::new(shard::<u32, String>(1));
        let (key_a, key_b) = two_keys_in_different_stripes();
        let bucket_a = usize::from(bucket_of(&key_bytes(&key_a)));
        let bucket_b = usize::from(bucket_of(&key_bytes(&key_b)));

        let held = Arc::clone(&s);
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let handle = std::thread::spawn(move || {
            let _guard = held.engine.stripe_lock(bucket_a).write();
            tx.send(()).expect("send");
            std::thread::sleep(Duration::from_millis(150));
        });
        rx.recv().expect("lock holder signals it has the lock");

        assert!(
            s.engine.stripe_lock(bucket_b).try_write().is_some(),
            "a different stripe's lock is not blocked"
        );
        assert!(
            s.engine.stripe_lock(bucket_a).try_write().is_none(),
            "the held stripe's lock is still contended"
        );
        handle.join().expect("lock holder thread does not panic");
        assert!(
            s.engine.stripe_lock(bucket_a).try_write().is_some(),
            "released after the holder finishes"
        );

        // The released stripe still accepts writes end to end.
        s.insert(key_a, "a".into()).await.expect("insert");
        assert_eq!(s.get(&key_a).await, Some("a".into()));
    }

    #[tokio::test]
    async fn concurrent_inserts_across_many_stripes_all_land() {
        const DISTINCT_STRIPES: usize = 16;
        let s = Arc::new(shard::<u32, String>(1));
        let mut keys = vec![0u32];
        let mut seen_stripes = HashSet::from([bucket_of(&key_bytes(&0u32))]);
        let mut candidate = 1u32;
        while seen_stripes.len() < DISTINCT_STRIPES {
            if seen_stripes.insert(bucket_of(&key_bytes(&candidate))) {
                keys.push(candidate);
            }
            candidate += 1;
        }

        let handles: Vec<_> = keys
            .iter()
            .copied()
            .map(|k| {
                let s = Arc::clone(&s);
                tokio::spawn(async move { s.insert(k, format!("v{k}")).await })
            })
            .collect();
        for handle in handles {
            handle
                .await
                .expect("spawned insert does not panic")
                .expect("insert");
        }
        for k in keys {
            assert_eq!(s.get(&k).await, Some(format!("v{k}")));
        }
    }

    #[tokio::test]
    async fn absolute_ttl_on_the_wire_expires_a_shard_configured_with_no_ttl_of_its_own() {
        let a = Shard::<u32, String>::new(
            SmolStr::new("test"),
            Mode::Replicated,
            NodeId::from(1),
            10_000,
            Some(Duration::from_millis(50)),
            None,
        );
        let b = shard::<u32, String>(2); // no ttl configured here

        a.insert(1, "short-lived".into()).await.expect("insert");
        let recs = ShardOps::records_for(&a, vec![key_bytes(&1u32)]).await;
        assert_eq!(recs.len(), 1);
        assert!(
            recs[0].expires_at_ms.is_some(),
            "the wire record carries the absolute deadline `a` computed"
        );
        ShardOps::apply_remote(&b, recs[0].clone()).await;
        assert_eq!(b.get(&1).await, Some("short-lived".into()));

        // Waits well past the 50ms TTL, matching the digest-flushing test
        // below.
        tokio::time::sleep(Duration::from_millis(1300)).await;
        assert_eq!(a.get(&1).await, None, "the origin's own copy expires");
        assert_eq!(
            b.get(&1).await,
            None,
            "b expires the entry from the wire-carried deadline alone"
        );
    }

    #[tokio::test]
    async fn per_entry_ttl_overrides_the_shard_default_and_replicates() {
        let a = Shard::<u32, String>::new(
            SmolStr::new("test"),
            Mode::Replicated,
            NodeId::from(1),
            10_000,
            Some(Duration::from_secs(30)),
            None,
        );
        let b = shard::<u32, String>(2);
        let short = Duration::from_millis(50);

        a.insert_with_ttl(1, "short".into(), short)
            .await
            .expect("insert with ttl");
        a.insert(2, "default".into()).await.expect("insert");
        a.insert_many_with_ttl([(3, "short-batch".to_string())], short)
            .await
            .expect("insert_many with ttl");
        a.get_or_load(&4, async |_| Ok::<_, std::io::Error>("default-fill".into()))
            .await
            .expect("fill");
        // A hit does not re-stamp: key 1 keeps its short override.
        let hit = a
            .get_or_load(&1, async |_| Ok::<_, std::io::Error>("never-called".into()))
            .await
            .expect("hit");
        assert_eq!(hit, "short");

        let recs = ShardOps::records_for(&a, (1..=4u32).map(|k| key_bytes(&k)).collect()).await;
        assert_eq!(recs.len(), 4);
        for rec in &recs {
            assert!(
                rec.expires_at_ms.is_some(),
                "every record carries a deadline"
            );
        }
        ShardOps::apply_remote_batch(&b, recs).await;
        for key in 1..=4u32 {
            assert!(b.get(&key).await.is_some(), "b holds key {key} on arrival");
        }

        // Past the short TTL, short of the 30s default.
        tokio::time::sleep(Duration::from_millis(1300)).await;
        for (shard, name) in [(&a, "a"), (&b, "b")] {
            assert_eq!(shard.get(&1).await, None, "{name}: short insert expires");
            assert_eq!(shard.get(&3).await, None, "{name}: short batch expires");
            assert_eq!(
                shard.get(&2).await.as_deref(),
                Some("default"),
                "{name}: the default-TTL entry outlives the overrides"
            );
            assert_eq!(
                shard.get(&4).await.as_deref(),
                Some("default-fill"),
                "{name}: a read-through fill takes the default, never an override"
            );
        }
    }

    /// Covers both [`ShardOps::apply_remote`] and
    /// [`ShardOps::apply_remote_batch`], and both a key that expired for
    /// real and one that never existed.
    #[tokio::test]
    async fn dead_on_arrival_ttl_record_never_resurrects_a_locally_expired_entry() {
        let s = Shard::<u32, String>::new(
            SmolStr::new("test"),
            Mode::Replicated,
            NodeId::from(1),
            10_000,
            Some(Duration::from_millis(50)),
            None,
        );
        s.insert(1, "original".into()).await.expect("insert");
        assert_eq!(s.get(&1).await, Some("original".into()));

        // Let it expire locally for real, well past the 50ms TTL.
        tokio::time::sleep(Duration::from_millis(1300)).await;
        assert_eq!(
            s.get(&1).await,
            None,
            "sanity: the original entry is gone before the dead-on-arrival record arrives"
        );

        let doa = WireRecord {
            key: key_bytes(&1u32),
            // Clearly newer than anything this shard's clock has stamped, so
            // only the absolute deadline can cause the rejection.
            ver: hlc(now_ms() + 10_000, 2),
            value: Some(Bytes::from(
                postcard::to_stdvec(&"resurrected".to_string()).expect("test value encodes"),
            )),
            expires_at_ms: Some(now_ms().saturating_sub(60_000)),
        };

        ShardOps::apply_remote(&s, doa.clone()).await;
        assert_eq!(
            s.get(&1).await,
            None,
            "a dead-on-arrival record does not resurrect a locally-expired entry, via apply_remote"
        );

        let s2 = Shard::<u32, String>::new(
            SmolStr::new("test"),
            Mode::Replicated,
            NodeId::from(1),
            10_000,
            Some(Duration::from_millis(50)),
            None,
        );
        s2.insert(1, "original".into()).await.expect("insert");
        tokio::time::sleep(Duration::from_millis(1300)).await;
        assert_eq!(s2.get(&1).await, None, "sanity: gone here too");

        ShardOps::apply_remote_batch(&s2, vec![doa]).await;
        assert_eq!(
            s2.get(&1).await,
            None,
            "same guarantee via apply_remote_batch"
        );
    }

    #[tokio::test]
    async fn gc_tombstones_drops_expired_entries_and_updates_digest() {
        let mut s = shard::<u32, String>(1);
        s.remove(&1).await.expect("remove creates a tombstone");
        assert!(
            !ShardOps::records_for(&s, vec![key_bytes(&1u32)])
                .await
                .is_empty()
        );

        // Force the tombstone already recorded to read as expired.
        s.tombstone_ttl_ms = 0;
        s.engine
            .debug_force_tombstone_ttl_past(key_bytes(&1u32).as_ref(), false);

        ShardOps::gc_tombstones(&s, false).await;
        assert!(
            ShardOps::records_for(&s, vec![key_bytes(&1u32)])
                .await
                .is_empty()
        );
        assert_digest_matches_full_recompute(&s).await;
    }

    /// Forces one tombstone's `ttl_deadline_ms`, and `max_deadline_ms` too when
    /// `past_max`.
    fn force_tombstone_past_ttl<K, V>(s: &Shard<K, V>, key_bytes: &Bytes, past_max: bool)
    where
        K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
        V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    {
        s.engine
            .debug_force_tombstone_ttl_past(key_bytes.as_ref(), past_max);
    }

    #[tokio::test]
    async fn gc_tombstones_defers_collection_while_a_member_is_absent() {
        let mut s = shard::<u32, String>(1);
        s.remove(&1).await.expect("remove creates a tombstone");
        s.tombstone_ttl_ms = 0;
        force_tombstone_past_ttl(&s, &key_bytes(&1u32), false);

        ShardOps::gc_tombstones(&s, true).await;
        assert!(
            !ShardOps::records_for(&s, vec![key_bytes(&1u32)])
                .await
                .is_empty(),
            "a tombstone past tombstone_ttl but not tombstone_max_ttl survives \
             collection while a member is absent"
        );
        assert_digest_matches_full_recompute(&s).await;
    }

    #[tokio::test]
    async fn gc_tombstones_proceeds_once_no_member_is_absent() {
        let mut s = shard::<u32, String>(1);
        s.remove(&1).await.expect("remove creates a tombstone");
        s.tombstone_ttl_ms = 0;
        force_tombstone_past_ttl(&s, &key_bytes(&1u32), false);

        ShardOps::gc_tombstones(&s, true).await;
        assert!(
            !ShardOps::records_for(&s, vec![key_bytes(&1u32)])
                .await
                .is_empty(),
            "deferred while the member was absent"
        );
        assert_digest_matches_full_recompute(&s).await;

        ShardOps::gc_tombstones(&s, false).await;
        assert!(
            ShardOps::records_for(&s, vec![key_bytes(&1u32)])
                .await
                .is_empty(),
            "collected once the member returned (no member absent any more)"
        );
        assert_digest_matches_full_recompute(&s).await;
    }

    #[tokio::test]
    async fn gc_tombstones_hard_cap_overrides_deferral() {
        let mut s = shard::<u32, String>(1);
        s.remove(&1).await.expect("remove creates a tombstone");
        s.tombstone_ttl_ms = 0;
        s.tombstone_max_ttl_ms = 0;
        force_tombstone_past_ttl(&s, &key_bytes(&1u32), true);

        ShardOps::gc_tombstones(&s, true).await;
        assert!(
            ShardOps::records_for(&s, vec![key_bytes(&1u32)])
                .await
                .is_empty(),
            "a tombstone past tombstone_max_ttl is collected even while a member is absent"
        );
        assert_digest_matches_full_recompute(&s).await;
    }

    #[tokio::test]
    async fn run_pending_tasks_flushes_a_quiet_shards_stale_ttl_eviction_digest() {
        let s = Shard::<u32, String>::new(
            SmolStr::new("test"),
            Mode::Replicated,
            NodeId::from(1),
            10_000,
            Some(Duration::from_millis(50)),
            None,
        );
        s.insert(1, "value".into()).await.expect("insert");
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Logical expiry is visible before the engine's sweep corrects the
        // digest.
        assert!(
            ShardOps::bucket_entries(&s, bucket_of(&key_bytes(&1u32)))
                .await
                .is_empty()
        );

        // Without this, the sweep never runs for a quiet shard on its own.
        ShardOps::run_pending_tasks(&s).await;
        assert_digest_matches_full_recompute(&s).await;
    }

    /// Compares the incrementally-maintained digest against a full recompute.
    async fn assert_digest_matches_full_recompute<K, V>(s: &Shard<K, V>)
    where
        K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
        V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    {
        let expected = s.engine.recompute_digests();
        for (bucket, digest) in ShardOps::digests(s).await {
            assert_eq!(
                digest,
                expected[usize::from(bucket)],
                "bucket {bucket} incremental digest diverged from full recompute"
            );
        }
    }

    #[tokio::test]
    async fn digest_incremental_matches_full_recompute_after_random_ops() {
        let s = shard::<u32, u64>(1);
        let mut rng = StdRng::seed_from_u64(0xC0FF_EE42);

        for i in 0..300 {
            let key = rng.random_range(0..16u32);
            match rng.random_range(0..3u32) {
                0 => {
                    let _ = s.insert(key, u64::from(key) * 31).await;
                }
                1 => {
                    let _ = s.remove(&key).await;
                }
                _ => {
                    let rec = WireRecord {
                        key: key_bytes(&key),
                        value: if rng.random_bool(0.5) {
                            Some(Bytes::from(
                                postcard::to_stdvec(&(u64::from(key) * 7)).expect("encode"),
                            ))
                        } else {
                            None
                        },
                        ver: hlc(rng.random_range(1..u64::MAX / 4), rng.random_range(2..5)),
                        expires_at_ms: None,
                    };
                    ShardOps::apply_remote(&s, rec).await;
                }
            }
            if i % 10 == 0 {
                assert_digest_matches_full_recompute(&s).await;
            }
        }
        assert_digest_matches_full_recompute(&s).await;
    }

    #[tokio::test]
    async fn with_max_frame_overrides_the_default_cap() {
        let s = shard::<u32, Vec<u8>>(1).with_max_frame(64);
        let err = s
            .insert(1, vec![0u8; 100])
            .await
            .expect_err("a value over the overridden cap is rejected");
        assert!(matches!(err, CacheError::ValueTooLarge { .. }));

        s.insert(2, Vec::new())
            .await
            .expect("a small value still fits under the overridden cap");
    }

    #[tokio::test]
    async fn insert_rejects_when_the_value_alone_fits_but_the_wire_frame_does_not() {
        // The value's own encoding fits a 25-byte cap; the record's full wire
        // frame does not.
        let s = shard::<u32, Vec<u8>>(1).with_max_frame(25);
        let err = s
            .insert(1, vec![0u8; 20])
            .await
            .expect_err("the full wire frame, not only the value, counts toward the cap");
        assert!(matches!(err, CacheError::ValueTooLarge { .. }));
    }

    #[tokio::test]
    async fn with_weigher_drives_the_engines_total_weight() {
        let s = shard::<u32, Vec<u8>>(1).with_weigher(|_key: &u32, value: &Vec<u8>| {
            u32::try_from(value.len()).unwrap_or(u32::MAX)
        });
        s.insert(1, vec![0u8; 7]).await.expect("insert");
        let (_, weight) = s.engine.debug_totals();
        assert_eq!(
            weight, 7,
            "a custom weigher drives the engine's total weight, not the default of 1 per entry"
        );
    }

    /// A resolver where the longer value wins, `Hlc` breaking ties on equal
    /// length. `(len, ver)` compared lexicographically satisfies
    /// `ConflictResolver::winner`'s contract.
    #[derive(Debug, Clone, Copy)]
    struct LongestValueWins;

    impl ConflictResolver for LongestValueWins {
        fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
            let len_a = a.value.map_or(0, <[u8]>::len);
            let len_b = b.value.map_or(0, <[u8]>::len);
            match len_a.cmp(&len_b) {
                std::cmp::Ordering::Greater => Winner::A,
                std::cmp::Ordering::Less => Winner::B,
                std::cmp::Ordering::Equal => {
                    if a.ver >= b.ver {
                        Winner::A
                    } else {
                        Winner::B
                    }
                }
            }
        }
    }

    /// Always prefers whichever record is passed as `a`, violating
    /// `ConflictResolver::winner`'s antisymmetry requirement.
    #[derive(Debug, Clone, Copy)]
    struct AlwaysA;

    impl ConflictResolver for AlwaysA {
        fn winner(&self, _key: &[u8], _a: RecordView<'_>, _b: RecordView<'_>) -> Winner {
            Winner::A
        }
    }

    #[test]
    fn longest_value_wins_is_antisymmetric_over_sample_pairs() {
        let resolver = LongestValueWins;
        let samples = [
            RecordView {
                value: Some(b"a"),
                ver: hlc(1, 1),
                expires_at_ms: None,
            },
            RecordView {
                value: Some(b"abc"),
                ver: hlc(2, 2),
                expires_at_ms: None,
            },
            RecordView {
                value: None,
                ver: hlc(3, 1),
                expires_at_ms: None,
            },
            RecordView {
                value: Some(b"ab"),
                ver: hlc(1, 9),
                expires_at_ms: None,
            },
            // Same length as "ab" above, different version: the only pair
            // here that exercises the equal-length tie-break by `Hlc`.
            RecordView {
                value: Some(b"zz"),
                ver: hlc(5, 3),
                expires_at_ms: None,
            },
        ];
        for &a in &samples {
            for &b in &samples {
                if a.ver == b.ver {
                    continue; // Shard::apply never asks about equal versions.
                }
                let ab = resolver.winner(b"k", a, b);
                let ba = resolver.winner(b"k", b, a);
                assert_ne!(ab, ba, "swapping the arguments swaps the winner");
            }
        }

        // `AlwaysA` is exactly the antisymmetry violation the trait docs
        // warn about: swapping the arguments does not swap the winner.
        let broken = AlwaysA;
        let (x, y) = (samples[0], samples[1]);
        assert_eq!(broken.winner(b"k", x, y), Winner::A);
        assert_eq!(broken.winner(b"k", y, x), Winner::A);
    }

    #[tokio::test]
    async fn custom_resolver_overrides_plain_hlc_order() {
        let s = shard::<u32, Vec<u8>>(1).with_resolver(Arc::new(LongestValueWins));

        let long_but_old = WireRecord {
            key: key_bytes(&1u32),
            value: Some(Bytes::from(
                postcard::to_stdvec(&vec![0u8; 10]).expect("encode"),
            )),
            ver: hlc(100, 2),
            expires_at_ms: None,
        };
        ShardOps::apply_remote(&s, long_but_old).await;
        assert_eq!(s.get(&1).await, Some(vec![0u8; 10]));

        // Strictly newer by Hlc, but shorter: loses under LongestValueWins.
        let short_but_new = WireRecord {
            key: key_bytes(&1u32),
            value: Some(Bytes::from(
                postcard::to_stdvec(&vec![0u8; 3]).expect("encode"),
            )),
            ver: hlc(200, 3),
            expires_at_ms: None,
        };
        ShardOps::apply_remote(&s, short_but_new).await;
        assert_eq!(
            s.get(&1).await,
            Some(vec![0u8; 10]),
            "the default Hlc-only rule does not apply once a custom resolver is installed"
        );

        // Longer *and* newer: wins outright.
        let long_and_new = WireRecord {
            key: key_bytes(&1u32),
            value: Some(Bytes::from(
                postcard::to_stdvec(&vec![0u8; 20]).expect("encode"),
            )),
            ver: hlc(300, 3),
            expires_at_ms: None,
        };
        ShardOps::apply_remote(&s, long_and_new).await;
        assert_eq!(s.get(&1).await, Some(vec![0u8; 20]));
    }

    /// The permutation-convergence property holds for a custom resolver too,
    /// not only the default one.
    #[tokio::test]
    async fn custom_resolver_converges_across_permutations_and_duplicates() {
        let a = shard::<u32, Vec<u8>>(10).with_resolver(Arc::new(LongestValueWins));
        let b = shard::<u32, Vec<u8>>(20).with_resolver(Arc::new(LongestValueWins));

        let mut records = Vec::new();
        for i in 0..6u64 {
            let key = u32::try_from(i % 3).expect("small");
            let len = usize::try_from((i * 7) % 11).expect("small");
            let value = vec![u8::try_from(i).expect("small"); len];
            records.push(WireRecord {
                key: key_bytes(&key),
                value: Some(Bytes::from(postcard::to_stdvec(&value).expect("encode"))),
                ver: hlc(1_000 + i * 3, i % 4 + 1),
                expires_at_ms: None,
            });
        }
        records.push(WireRecord {
            key: key_bytes(&1u32),
            value: None,
            ver: hlc(1_100, 9),
            expires_at_ms: None,
        });

        for rec in &records {
            ShardOps::apply_remote(&a, rec.clone()).await;
        }
        // Reverse order, each record applied twice: reordering and
        // duplication together, the two hazards anti-entropy tolerates.
        for rec in records.iter().rev() {
            ShardOps::apply_remote(&b, rec.clone()).await;
            ShardOps::apply_remote(&b, rec.clone()).await;
        }

        for key in 0..3u32 {
            assert_eq!(
                a.get(&key).await,
                b.get(&key).await,
                "key {key} diverged across permutations under a shared custom resolver"
            );
        }
        assert_eq!(
            ShardOps::digests(&a).await,
            ShardOps::digests(&b).await,
            "digests diverged between replicas under the same custom resolver"
        );
    }

    #[tokio::test]
    async fn contains_key_reflects_insert_remove_and_ttl_expiry() {
        let s = shard::<u32, String>(1);
        assert!(!s.contains_key(&1).await, "missing key");

        s.insert(1, "a".into()).await.expect("insert");
        assert!(s.contains_key(&1).await, "present after insert");

        s.remove(&1).await.expect("remove");
        assert!(!s.contains_key(&1).await, "gone after remove");

        let ttl_shard = Shard::<u32, String>::new(
            SmolStr::new("test"),
            Mode::Replicated,
            NodeId::from(1),
            10_000,
            Some(Duration::from_millis(50)),
            None,
        );
        ttl_shard
            .insert(2, "short-lived".into())
            .await
            .expect("insert with default ttl");
        assert!(ttl_shard.contains_key(&2).await, "present before expiry");
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!ttl_shard.contains_key(&2).await, "gone after ttl expiry");
    }

    #[tokio::test]
    async fn bulk_writes_queue_every_key_for_fan_out_and_a_drain_takes_them_all() {
        let s = shard::<u32, String>(1);
        let queue = s.fan_out_queue();
        s.insert_many((0..10_000u32).map(|k| (k, k.to_string())))
            .await
            .expect("bulk insert");
        assert_eq!(queue.len(), 10_000, "nothing is lost to a lagged channel");
        let mut keys = queue.drain();
        keys.sort_unstable();
        assert_eq!(keys, (0..10_000u32).collect::<Vec<_>>());
        assert_eq!(queue.len(), 0);

        s.remove_many(0..10_000u32).await.expect("bulk remove");
        assert_eq!(queue.drain().len(), 10_000);
        s.insert(1, "one".into()).await.expect("insert");
        s.insert(1, "one again".into()).await.expect("insert");
        assert_eq!(
            queue.drain(),
            vec![1, 1],
            "single writes queue their key each time"
        );
    }

    #[tokio::test]
    async fn fan_out_queue_wakes_a_waiter_for_a_push_before_or_after_the_wait() {
        let queue = Arc::new(FanOutQueue::<u32>::new());
        queue.push(7);
        tokio::time::timeout(Duration::from_secs(1), queue.wait_nonempty())
            .await
            .expect("a push before the wait is not missed");
        assert_eq!(queue.drain(), vec![7]);

        let waiter = {
            let queue = Arc::clone(&queue);
            tokio::spawn(async move {
                queue.wait_nonempty().await;
                queue.drain()
            })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        queue.extend([1, 2]);
        let drained = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("a push after the wait wakes it")
            .expect("waiter completes");
        assert_eq!(drained, vec![1, 2]);
    }

    #[tokio::test]
    async fn insert_many_queues_only_the_keys_that_landed() {
        // Under `AlwaysA` whatever is stored wins, so a bulk write over an
        // existing key is rejected and must not reach the fan-out queue.
        let s = shard::<u32, String>(1).with_resolver(Arc::new(AlwaysA));
        s.insert(1, "first".into()).await.expect("insert");
        let _ = s.fan_out.drain();

        s.insert_many([(1, "again".to_string()), (2, "two".to_string())])
            .await
            .expect("insert_many");

        assert_eq!(
            s.fan_out.drain(),
            vec![2],
            "only the write that landed is queued for replication"
        );
        assert_eq!(s.get(&1).await, Some("first".to_string()));
        assert_eq!(s.get(&2).await, Some("two".to_string()));
    }

    #[tokio::test]
    async fn remove_many_queues_only_the_keys_that_landed() {
        let s = shard::<u32, String>(1).with_resolver(Arc::new(AlwaysA));
        s.insert(1, "first".into()).await.expect("insert");
        let _ = s.fan_out.drain();

        s.remove_many([1, 3]).await.expect("remove_many");

        assert_eq!(
            s.fan_out.drain(),
            vec![3],
            "the rejected tombstone never reaches the fan-out queue"
        );
        assert_eq!(s.get(&1).await, Some("first".to_string()));
    }

    #[tokio::test]
    async fn remove_many_tombstones_every_key_and_leaves_others_untouched() {
        let s = shard::<u32, String>(1);
        for k in 0..5u32 {
            s.insert(k, k.to_string()).await.expect("insert");
        }

        let mut events = s.events();
        s.remove_many([0u32, 1, 2]).await.expect("remove_many");

        for k in 0..3u32 {
            assert_eq!(s.get(&k).await, None, "key {k} is tombstoned");
            let recs = ShardOps::records_for(&s, vec![key_bytes(&k)]).await;
            assert_eq!(recs.len(), 1);
            assert!(recs[0].is_tombstone(), "key {k} reads back as a tombstone");
        }
        for k in 3..5u32 {
            assert_eq!(
                s.get(&k).await,
                Some(k.to_string()),
                "key {k} was not in the batch and survives"
            );
        }

        let mut removed = HashSet::new();
        for _ in 0..3 {
            match tokio::time::timeout(Duration::from_secs(1), events.recv())
                .await
                .expect("event arrives")
                .expect("channel open")
            {
                Event::Removed { key, .. } => {
                    removed.insert(key);
                }
                other => panic!("unexpected event: {other:?}"),
            }
        }
        assert_eq!(removed, HashSet::from([0u32, 1, 2]));
    }

    #[tokio::test]
    async fn clear_empties_a_populated_shard_and_leaves_tombstones() {
        let s = shard::<u32, String>(1);
        for k in 0..10u32 {
            s.insert(k, k.to_string()).await.expect("insert");
        }
        assert_eq!(s.entry_count().await, 10);

        s.clear().await.expect("clear");
        assert_eq!(s.entry_count().await, 0);

        for k in 0..10u32 {
            let recs = ShardOps::records_for(&s, vec![key_bytes(&k)]).await;
            assert_eq!(recs.len(), 1, "key {k} leaves a tombstone");
            assert!(recs[0].is_tombstone());
        }
    }

    #[tokio::test]
    async fn get_or_insert_with_fills_on_miss_and_skips_make_on_hit() {
        let s = shard::<u32, String>(1);
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let c1 = std::sync::Arc::clone(&calls);
        let filled = s
            .get_or_insert_with(&1, async move |_key: &u32| {
                c1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                "loaded".to_string()
            })
            .await
            .expect("fill succeeds");
        assert_eq!(filled, "loaded");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);

        let c2 = std::sync::Arc::clone(&calls);
        let hit = s
            .get_or_insert_with(&1, async move |_key: &u32| {
                c2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                "should-not-run".to_string()
            })
            .await
            .expect("hit succeeds");
        assert_eq!(hit, "loaded");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn keys_returns_exactly_the_live_keys() {
        let s = shard::<u32, String>(1);
        for k in 0..5u32 {
            s.insert(k, k.to_string()).await.expect("insert");
        }
        s.remove(&2).await.expect("remove");

        let mut keys = s.keys();
        keys.sort_unstable();
        assert_eq!(keys, vec![0, 1, 3, 4]);
    }

    #[tokio::test]
    async fn with_clock_drives_every_shard_timestamp() {
        use std::sync::atomic::{AtomicU64, Ordering};

        let fake_now = Arc::new(AtomicU64::new(1_000));
        let reader = Arc::clone(&fake_now);
        let clock_fn: Arc<dyn Fn() -> u64 + Send + Sync> =
            Arc::new(move || reader.load(Ordering::SeqCst));
        let s = Shard::<u32, String>::new(
            SmolStr::new("test"),
            Mode::Replicated,
            NodeId::from(1),
            10_000,
            Some(Duration::from_millis(100)),
            None,
        )
        .with_clock(Arc::clone(&clock_fn));

        s.insert(1, "a".into()).await.expect("insert");
        let recs = ShardOps::records_for(&s, vec![key_bytes(&1u32)]).await;
        assert_eq!(
            recs[0].ver.wall_ms, 1_000,
            "the HLC stamp reads the injected clock, not the system clock"
        );
        assert_eq!(
            recs[0].expires_at_ms,
            Some(1_100),
            "the expiry deadline is computed from the injected clock"
        );
        assert_eq!(
            s.get(&1).await,
            Some("a".into()),
            "not yet past the injected clock's TTL"
        );

        // Advance the fake clock, not real time, past the TTL.
        fake_now.store(1_300, Ordering::SeqCst);
        assert_eq!(
            s.get(&1).await,
            None,
            "reads are TTL-blind but still honor the injected clock for expiry"
        );
        ShardOps::run_pending_tasks(&s).await;
        assert_eq!(
            s.entry_count().await,
            0,
            "the sweep ran against the injected clock, not real time"
        );
    }
}

#[cfg(test)]
mod prop_tests;

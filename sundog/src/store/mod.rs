//! Store: bespoke-engine-backed shards, versioned apply, and the digest
//! machinery that makes anti-entropy cheap. The engine itself
//! (`engine::Engine`) is [`BUCKET_COUNT`] independently locked stripes —
//! one stripe per anti-entropy bucket, holding both live entries and
//! tombstones — so a read costs one stripe read lock plus a lookup by the
//! key's postcard bytes, a versioned write runs fully synchronously under
//! one stripe write lock, and enumerating one anti-entropy bucket touches
//! only that bucket's stripe. See `engine`'s module docs for the engine
//! itself; this module is the typed `Shard`/`ShardOps` surface built on it.
//!
//! `Shard` intentionally holds no handle to `net::Mesh` — its constructor
//! takes none, by design. Every local
//! mutation (`insert`, `remove`, and `get_or_load`'s fill) publishes an
//! `Origin::Local` [`Event`] on [`Shard::events`]; correlating that stream to
//! wire fan-out (`Mesh::send` per [`Mode`]) is the composition layer's job.

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

/// A sequential, single-threaded reference model of everything downstream of
/// a successful wire decode — versioned apply, tombstone retention, expiry,
/// and digest bookkeeping — shared by `sundog-fuzz`'s stateful fuzz targets
/// and this crate's own
/// `shard_matches_the_reference_model_under_arbitrary_op_sequences` property
/// test (`store::prop_tests`). `#[doc(hidden)]` because it's a
/// testing/fuzzing seam, not part of the crate's API: it exists so the
/// semantics get written once, not twice.
#[cfg(any(test, feature = "fuzzing"))]
#[doc(hidden)]
pub mod model;

/// Number of anti-entropy buckets per shard: `bucket(k) = xxh3(key_bytes) & (BUCKET_COUNT - 1)`.
pub const BUCKET_COUNT: usize = 1024;

/// A custom per-entry weigher for size-bounded eviction: `(key, value) ->
/// weight`, consulted by `engine::Engine` on every write and by its
/// sampled-LRU capacity eviction. Boxed since
/// [`crate::cache::CacheBuilder::weigher`] and [`Shard::with_weigher`] need
/// to store one before its concrete closure type is nameable.
pub(crate) type Weigher<K, V> = Box<dyn Fn(&K, &V) -> u32 + Send + Sync>;

/// Upper bound on records per [`WireRecord`] batch yielded by
/// [`ShardOps::snapshot_chunks`] — a chunk breaks earlier than this
/// if its cumulative encoded size approaches [`MAX_FRAME`] first (see
/// [`chunk_records_for_snapshot`]), so this only caps chunk size for
/// small-value caches.
const SNAPSHOT_CHUNK_SIZE: usize = 500;

/// Headroom reserved below [`MAX_FRAME`] when sizing a snapshot chunk, for
/// the `Msg::StChunk` envelope (the raw-record frame's discriminant byte,
/// fixed header, and cache name — `crate::wire`'s module docs) around the
/// records themselves.
const SNAPSHOT_CHUNK_ENVELOPE_HEADROOM: usize = 4 * 1024;

/// Groups `records` into chunks that stay under [`MAX_FRAME`] once wrapped in
/// a `Msg::StChunk`, splitting on cumulative wire-encoded size as
/// well as [`SNAPSHOT_CHUNK_SIZE`] — a fixed record count alone undercounts
/// for caches whose average value is more than a few KiB, well within the
/// supported value size (up to a few MiB). Each record's
/// contribution (`wire::RECORD_HEADER_LEN` plus its key/value lengths) is
/// exact, not approximated: the raw-record wire layout (`crate::wire`'s
/// module docs) is a fixed-size header, unlike postcard's variable-length
/// framing this used to re-derive per record just to measure it.
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

/// Capacity of each shard's [`Event`] broadcast channel. Slow subscribers that
/// fall this far behind miss events (`broadcast::error::RecvError::Lagged`)
/// rather than applying backpressure to writers.
const EVENTS_CAPACITY: usize = 1024;

/// How many keys one `FanOutNotice::Many` carries at most — aligned with
/// `net::conn`'s `REPLICATE_BATCH_COUNT` so one notice's worth of records
/// coalesces into at most one full wire batch. A bulk burst of `n` writes
/// occupies `n / 4096` fan-out channel slots instead of `n`, which is what
/// keeps `Cache::insert_many` from lagging the channel and degrading its
/// whole burst to anti-entropy repair.
const FAN_OUT_MANY_CHUNK: usize = 4096;

/// One fan-out notification: "these locally-written keys need replicating".
/// Only local writes ever notify — remote applies would just re-broadcast
/// what a peer already sent — so no origin travels here.
#[derive(Clone)]
pub(crate) enum FanOutNotice<K> {
    /// A single write ([`Shard::insert`], [`Shard::remove`], a read-through
    /// fill).
    One(K),
    /// A bulk burst ([`Shard::insert_many`]), chunked to
    /// [`FAN_OUT_MANY_CHUNK`].
    Many(Vec<K>),
}

/// A named cache's clustering behavior: how writes fan out to other nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// No cluster traffic at all; entries live only on this node.
    Local,
    /// Every node caches independently; writes broadcast an [`crate::wire::Msg::Invalidate`].
    Invalidation,
    /// Every node holds every entry; writes broadcast the full [`crate::wire::Msg::Replicate`].
    Replicated,
}

impl Mode {
    /// The wire token gossiped for this mode under a `cache:<name>` chitchat
    /// key (`membership`'s cache-mode fingerprint) — a stable string rather
    /// than a `Debug`/`Display` impl, so renaming a variant doesn't silently
    /// change what's on the wire.
    pub(crate) const fn as_token(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Invalidation => "invalidation",
            Self::Replicated => "replicated",
        }
    }

    /// Parses [`Mode::as_token`]'s output back into a `Mode`, or `None` for
    /// anything else — a peer running a newer/older version that gossips an
    /// unrecognized token is skipped by the caller, not treated as a parse
    /// failure of the whole peer.
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
    Created {
        /// The key that was created.
        key: K,
        /// Its new value.
        value: V,
        /// What caused the write.
        origin: Origin,
    },
    /// An existing key's value changed.
    Updated {
        /// The key that was updated.
        key: K,
        /// Its new value.
        value: V,
        /// What caused the write.
        origin: Origin,
    },
    /// A key was removed (a tombstone was applied).
    Removed {
        /// The key that was removed.
        key: K,
        /// What caused the removal.
        origin: Origin,
    },
}

/// The type-erased surface the network layer drives a shard through, wire
/// bytes in and out — the boundary where postcard (de)serialization happens.
/// Local reads never deserialize; only wire traffic crossing this boundary
/// does. Implemented by `Shard<K, V>` for any `K`, `V` meeting its bounds;
/// held as `Arc<dyn ShardOps>` in the cluster's cache registry.
///
/// Async methods return `BoxFuture` rather than using `async fn` in the
/// trait, matching `Discovery`'s object-safety pattern (`dyn ShardOps` must
/// be usable from a `HashMap<SmolStr, Arc<dyn ShardOps>>`).
/// Entries per bucket, as an anti-entropy exchange reports them: every
/// requested bucket present, empty lists included.
pub type BucketEntries = Vec<(u16, Vec<(Bytes, Hlc)>)>;

pub trait ShardOps: Send + Sync {
    /// Applies an inbound replicated record iff its version is newer than
    /// what's stored — the versioned-apply rule that makes replication
    /// commutative. The single path shared by local writes, live
    /// replication, state transfer, and anti-entropy repair.
    fn apply_remote(&self, rec: WireRecord) -> BoxFuture<'_, ()>;

    /// [`ShardOps::apply_remote`] for a whole batch, grouped by key stripe
    /// (an anti-entropy bucket) and applied under one acquisition per
    /// touched stripe rather than one per record — per-record version checks
    /// and digest bookkeeping are otherwise identical, and an
    /// [`crate::store::Event`] is still emitted per record. The path
    /// `net::conn`'s coalesced [`crate::wire::Msg::ReplicateBatch`] frames,
    /// state-transfer chunks, and anti-entropy pull batches all apply
    /// through.
    fn apply_remote_batch(&self, recs: Vec<WireRecord>) -> BoxFuture<'_, ()>;

    /// Applies an inbound invalidation: drops the local copy of `key` iff
    /// `ver` is newer than the locally stored version. Deliberately not
    /// routed through [`ConflictResolver`]: an invalidation carries no value
    /// (it is a "the entry changed, drop your copy" signal, not a record),
    /// so there is nothing for a resolver to compare — `Hlc` order is the
    /// only signal available here, in [`Mode::Invalidation`] as much as it
    /// ever was.
    fn invalidate(&self, key: Bytes, ver: Hlc) -> BoxFuture<'_, ()>;

    /// Returns this shard's current per-bucket XOR digests, `(bucket, digest)`
    /// for all [`BUCKET_COUNT`] buckets — the first step of an anti-entropy
    /// round.
    fn digests(&self) -> BoxFuture<'_, Vec<(u16, u64)>>;

    /// Returns `(key, version)` for every live entry and un-GC'd tombstone in
    /// `bucket`, for a peer that reported a digest mismatch there.
    fn bucket_entries(&self, bucket: u16) -> BoxFuture<'_, Vec<(Bytes, Hlc)>>;

    /// [`ShardOps::bucket_entries`] for many buckets in ONE pass over the
    /// shard. An anti-entropy round against a mostly-divergent peer touches
    /// up to all 1,024 buckets; per-bucket scans would make that quadratic
    /// in shard size.
    fn entries_for_buckets(&self, buckets: Vec<u16>) -> BoxFuture<'_, BucketEntries>;

    /// Returns the full [`WireRecord`] for each of `keys` that this shard
    /// holds (present entries and tombstones alike), answering an `AePull`.
    fn records_for(&self, keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>>;

    /// Streams the shard's full contents in ~500-record chunks for state
    /// transfer to a joining node. Iteration is weakly consistent —
    /// safe because every chunk is applied through the same versioned
    /// [`ShardOps::apply_remote`] path as live traffic.
    fn snapshot_chunks(&self) -> BoxStream<'static, Vec<WireRecord>>;

    /// Garbage-collects tombstones older than the configured
    /// `tombstone_ttl`, keeping the digest and entry set consistent.
    ///
    /// `any_member_absent` defers collecting a tombstone once it is past
    /// `tombstone_ttl` but not yet past `tombstone_max_ttl`: while `true`, a
    /// tombstone in that window is left in place — collected only once
    /// `false` again, or once it ages past `tombstone_max_ttl` regardless.
    /// The composition layer (`cluster::tombstone_gc_task`) is the only
    /// caller and the only place that decides this — see its docs for how
    /// it's derived from cluster membership.
    fn gc_tombstones(&self, any_member_absent: bool) -> BoxFuture<'_, ()>;

    /// Runs `engine::Engine::sweep`: the engine has no free-running
    /// background sweep of its own — a stripe's expired/idle entries are
    /// only ever corrected on the read path (by treating them as absent) or
    /// here — so a shard that goes quiet right after a TTL/TTI-driven
    /// absence would otherwise keep a stale digest forever. TTL/TTI eviction
    /// timing is inherently advisory, not exact, which is why this flush has
    /// to be driven explicitly. Called periodically by `tombstone_gc_task`
    /// independent of read/write traffic.
    fn run_pending_tasks(&self) -> BoxFuture<'_, ()>;
}

/// One side of a [`ConflictResolver::winner`] comparison: everything a
/// resolver needs to pick a winner, at the wire level — postcard-encoded
/// value bytes rather than the typed value, matching the boundary described
/// in this module's docs ("local reads never deserialize").
#[derive(Debug, Clone, Copy)]
pub struct RecordView<'a> {
    /// The record's postcard-encoded value bytes, or `None` for a tombstone.
    pub value: Option<&'a [u8]>,
    /// The record's version.
    pub ver: Hlc,
    /// Absolute expiry in epoch milliseconds, or `None` for no TTL/no value.
    pub expires_at_ms: Option<u64>,
}

/// The outcome of a [`ConflictResolver::winner`] call: which of the two
/// argument records — by position, not by role — should be kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Winner {
    /// The first record (`a`) wins; `b` is discarded.
    A,
    /// The second record (`b`) wins; `a` is discarded.
    B,
}

/// Picks a winner between two differently-versioned records for the same
/// key. A resolver **picks**, it never **merges**: synthesizing a combined
/// value would make `Shard::apply`'s outcome depend on which two versions
/// happened to collide locally, breaking digest equality between replicas
/// that saw the same writes in a different order.
///
/// # Correctness contract — required for the permutation-convergence
/// property
///
/// `Shard::apply`'s core guarantee is that applying any set of writes, in
/// any order, any number of times, converges to the same state on every
/// replica. That guarantee transfers to a custom resolver only if `winner`
/// is:
///
/// - **Deterministic.** A pure function of `key`, `a`, and `b` alone — no
///   clock reads, no randomness, no external state. The same three inputs
///   must always produce the same [`Winner`].
/// - **Antisymmetric (order-independent).** Swapping the two records must
///   swap the answer: `winner(key, a, b) == A` if and only if
///   `winner(key, b, a) == B`. A resolver that instead favors "whichever
///   argument arrived as `b`" (or any other property of argument position
///   rather than of the records themselves) breaks convergence outright —
///   two replicas that apply the same pair of writes with `a`/`b` swapped
///   (as happens constantly: which side is "stored" and which is
///   "incoming" depends purely on arrival order) would disagree on the
///   winner forever.
/// - **Total and transitive.** Across any set of distinct-version records
///   for one key, the induced "beats" relation must be a strict total
///   order: never A beats B, B beats C, and C beats A. A cycle means there
///   is no stable winner, and replicas that received the records in
///   different orders can flap indefinitely instead of converging.
///
/// `Shard::apply` only ever calls `winner` when `a.ver != b.ver`; equal
/// versions are always a no-op before `winner` is consulted — a given
/// `(wall_ms, logical, node)` triple is produced by at most one write ever,
/// so equal versions imply identical records, and a resolver never has to,
/// and never gets asked to, break that tie.
///
/// The default [`LwwResolver`] satisfies all three properties by comparing
/// [`Hlc`] alone, which is already a deterministic, antisymmetric, total,
/// transitive order — this is the behavior every `Shard` had before
/// `ConflictResolver` existed, bit-for-bit.
///
/// A resolver that violates antisymmetry or transitivity is not safe to run
/// on more than one replica: nothing in this crate detects the violation,
/// convergence stops holding. Such a resolver is deliberately not
/// covered by the permutation-convergence test in this module — the
/// property cannot hold for it, only document the hazard.
pub trait ConflictResolver: Send + Sync + 'static {
    /// Decides which of `a`, `b` — two different versions of the record
    /// stored at `key` (`key`'s wire-encoded bytes) — wins. See the trait
    /// docs for the correctness contract this must satisfy.
    fn winner(&self, key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner;

    /// Whether `winner` reads `RecordView::value`. Defaults to `true`,
    /// preserving behavior for any resolver written before this method
    /// existed. Override to `false` for a resolver — like [`LwwResolver`] —
    /// that only ever compares `ver`/`expires_at_ms`: the versioned apply
    /// then skips postcard-encoding both records' values on every apply,
    /// work such a resolver never uses.
    fn needs_value_bytes(&self) -> bool {
        true
    }
}

/// The default resolver: last-write-wins by [`Hlc`], ignoring value bytes
/// and `expires_at_ms` entirely — the only comparison `Shard::apply` ever
/// made before [`ConflictResolver`] became pluggable, preserved bit-for-bit.
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

/// A stored value paired with the version it was last written at — the
/// per-key version table, folded into the cached value itself — and its
/// absolute expiry, so every replica converts the same origin-stamped
/// deadline into a local remaining duration.
#[derive(Debug, Clone)]
pub struct Stored<V> {
    /// The current value.
    pub value: V,
    /// `value`'s postcard-encoded bytes, cached once at construction rather
    /// than re-derived on every wire send: on the local-origin path (`insert`/
    /// `insert_many`/`get_or_load`'s fill), the bytes produced by that first
    /// encode; on the replica-apply path (`apply_remote_batch`), the
    /// verbatim bytes received off the wire — never re-encoded from `value`.
    /// Invariant: always equal to `postcard::to_stdvec(&value)`, or wire
    /// bytes that decode to a `value` structurally equal to it.
    pub encoded: Bytes,
    /// The version this value was written at.
    pub ver: Hlc,
    /// Absolute expiry in epoch milliseconds, or `None` for no TTL.
    pub expires_at_ms: Option<u64>,
}

/// A tombstone: the version of the delete that created it, and its two GC
/// deadlines. `ttl_deadline_ms` is when it becomes eligible for ordinary
/// collection; `max_deadline_ms` is the hard cap (`tombstone_max_ttl`) past
/// which it is collected regardless of member absence — see
/// [`ShardOps::gc_tombstones`]'s docs.
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
        /// `value`'s postcard-encoded bytes — see [`Stored::encoded`] for
        /// which bytes this is on each apply path.
        encoded: Bytes,
    },
    Tombstone,
}

/// Wraps a stampede-collapsed loader failure — the type-erased
/// `Arc<dyn Error + Send + Sync>` `engine::Inflight` stores so every joined
/// waiter can return the same failure the owner saw — as a boxable
/// [`std::error::Error`] for [`CacheError::Loader`].
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

/// Worst-case postcard-encoded size of an [`Hlc`]: `wall_ms: u64` (up to 10
/// LEB128 bytes) + `logical: u32` (up to 5) + `node: NodeId` (a `u64`, up to
/// 10) = 25, rounded up for headroom.
const HLC_ENCODED_MAX: usize = 32;

/// `xxh3(key_bytes ‖ postcard(ver))` — the digest contribution of one live
/// entry or tombstone. Encodes `ver` into a stack buffer rather
/// than a heap `Vec`: a few bytes, computed on every apply.
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
/// Debug builds assert the encoding round-trips to itself: a key type whose
/// `Serialize` impl isn't canonical (e.g. iteration-order-dependent, as a
/// `HashMap`-typed key would be) would silently corrupt digests and break
/// wire identity.
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
        "key's postcard encoding must be canonical/deterministic — no map-typed keys"
    );
    Ok(Bytes::from(bytes))
}

/// A typed named cache: an `engine::Engine` of `K -> V` — [`BUCKET_COUNT`]
/// independently locked stripes, each one both an anti-entropy bucket and
/// the lock a versioned write to a key in it holds for that write's whole
/// duration — plus the version-and-conflict machinery that backs
/// [`ShardOps`]. The typed `Cache<K, V>` handle users hold (`crate::cache`)
/// is a thin wrapper over `Arc<Shard<K, V>>`.
///
/// # Bounds
///
/// `K`'s postcard encoding doubles as its wire form and its digest-hash
/// input, so it must encode deterministically — no map-typed keys.
pub struct Shard<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    name: SmolStr,
    mode: Mode,
    engine: Arc<Engine<K, V>>,
    events: broadcast::Sender<Event<K, V>>,
    /// Internal-only notification channel: keys only, no value clone, fed by
    /// every *local* versioned write — remote applies never notify, they'd
    /// only re-broadcast what a peer already sent (`fan_out_task`'s sole
    /// source of truth for "what changed" — it re-fetches fresh wire bytes
    /// through `records_for_typed` rather than reading a value off this
    /// channel, so there is nothing here for it to be stale about). A bulk
    /// burst notifies as `FanOutNotice::Many` chunks so it can never lag
    /// the channel. Kept separate from `events` so the app-facing
    /// broadcast's `receiver_count()` reflects only real external
    /// subscribers, making the "skip the value clone when nobody's
    /// listening" guard on `events` meaningful.
    fan_out: broadcast::Sender<FanOutNotice<K>>,
    /// Guards only the synchronous HLC bump itself, never held across `.await`.
    clock: StdMutex<HlcClock>,
    /// The deterministic-clock hook: every timestamp this shard stamps (HLC,
    /// expiry deadlines, tombstone deadlines, TTI comparisons, sweeps) reads
    /// this instead of the system clock. Defaults to it in [`Shard::new`];
    /// overridden by [`Shard::with_clock`].
    clock_fn: Arc<dyn Fn() -> u64 + Send + Sync>,
    ttl: Option<Duration>,
    tombstone_ttl_ms: u64,
    tombstone_max_ttl_ms: u64,
    resolver: Arc<dyn ConflictResolver>,
    max_frame: usize,
    /// Remembered (alongside `tti` below) so [`Shard::with_weigher`] can
    /// rebuild `engine::Engine` from scratch — a weigher can only be
    /// installed at construction, unlike `resolver`/`max_frame` above.
    max_capacity: u64,
    tti: Option<Duration>,
    /// Handle for `sundog_cache_hits_total{cache}`, created once here rather
    /// than resolved by `metrics::counter!` on every call — label resolution
    /// has a per-call cost the read path can't afford. See [`Shard::get`] and
    /// [`Shard::get_or_load`] for exactly what counts as a hit.
    hits: metrics::Counter,
    /// Handle for `sundog_cache_misses_total{cache}`, created once for the
    /// same reason as the `hits` counter above. See [`Shard::get`] and
    /// [`Shard::get_or_load`] for exactly what counts as a miss.
    misses: metrics::Counter,
}

// The engine backing every method here is fully synchronous — no `.await`
// anywhere in this impl block — but the public API constraint is that these
// stay `async fn`: existing callers (`Cache`'s own async wrappers, and every
// downstream `.await` site) must not have to change, and a later backend
// swap should not have to change them back. `clippy::unused_async` and
// `clippy::unused_async_trait_impl` would otherwise ask to turn every one of
// them into a plain `fn` returning `impl Future`.
#[allow(clippy::unused_async, clippy::unused_async_trait_impl)]
impl<K, V> Shard<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// Builds a new shard. `node` stamps this shard's local writes.
    ///
    /// Tombstone GC uses [`ClusterConfig::default`]'s `tombstone_ttl` until
    /// overridden via [`Shard::with_tombstone_ttl`]: `Shard::new` takes no
    /// `ClusterConfig` (the store layer stays independent of cluster
    /// wiring), so a live cluster's configured value reaches this shard
    /// through that follow-up call instead.
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
            fan_out: broadcast::channel(EVENTS_CAPACITY).0,
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
    /// [`ShardOps::gc_tombstones`] (defaults to [`ClusterConfig::default`]'s
    /// value, since [`Shard::new`]'s signature is fixed and takes no
    /// `ClusterConfig`). Own-and-return, so the composition layer can thread
    /// a live cluster's configured `tombstone_ttl` through right after
    /// construction.
    #[must_use]
    pub fn with_tombstone_ttl(mut self, tombstone_ttl: Duration) -> Self {
        self.tombstone_ttl_ms = duration_ms(tombstone_ttl);
        self
    }

    /// Overrides the hard cap on tombstone retention used by
    /// [`ShardOps::gc_tombstones`] (defaults to [`ClusterConfig::default`]'s
    /// value). Same fixed-`Shard::new`-signature, follow-up-call pattern as
    /// [`Shard::with_tombstone_ttl`].
    #[must_use]
    pub fn with_tombstone_max_ttl(mut self, tombstone_max_ttl: Duration) -> Self {
        self.tombstone_max_ttl_ms = duration_ms(tombstone_max_ttl);
        self
    }

    /// Overrides the [`ConflictResolver`] consulted by `Shard::apply`
    /// whenever an incoming record's version differs from what's stored
    /// (defaults to [`LwwResolver`]). Same fixed-`Shard::new`-signature,
    /// follow-up-call pattern as [`Shard::with_tombstone_ttl`].
    #[must_use]
    pub fn with_resolver(mut self, resolver: Arc<dyn ConflictResolver>) -> Self {
        self.resolver = resolver;
        self
    }

    /// Overrides the hard cap [`Shard::insert`] enforces before writing a
    /// value (defaults to [`MAX_FRAME`]) — threaded from a live cluster's
    /// configured `ClusterConfig::max_frame` the same way
    /// [`Shard::with_tombstone_ttl`] threads `tombstone_ttl`.
    #[must_use]
    pub fn with_max_frame(mut self, max_frame: usize) -> Self {
        self.max_frame = max_frame;
        self
    }

    /// Installs a custom per-entry weigher for size-bounded eviction, in
    /// place of the default of one weight unit per entry — threaded from
    /// [`crate::cache::CacheBuilder::weigher`]. Rebuilds `engine::Engine`
    /// from scratch (a weigher can only be installed at construction), so
    /// this must be called immediately after [`Shard::new`], before any
    /// reads or writes reach this shard — harmless at that call site, since
    /// the engine is still empty there.
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

    /// Overrides the clock every timestamp this shard stamps reads from —
    /// HLC stamping, expiry deadlines, tombstone deadlines, TTI comparisons,
    /// and sweeps — in place of the system clock. Reserved for a
    /// deterministic-clock fuzz harness driving this shard's notion of time
    /// explicitly.
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

    /// The timestamp (epoch milliseconds) this shard stamps its writes,
    /// deadlines, and sweeps with — the system clock, unless overridden by
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

    /// The absolute expiry a write stamped now should carry: the per-write
    /// `ttl` if the caller gave one, else the shard's configured default,
    /// else none. Only this stamp is ever per-cache — everything downstream
    /// (the wire, the engine's expiry check, anti-entropy, state transfer)
    /// reads each record's own `expires_at_ms`.
    fn expiry_for(&self, ttl: Option<Duration>) -> Option<u64> {
        ttl.or(self.ttl)
            .map(|d| self.now_ms().saturating_add(duration_ms(d)))
    }

    /// Handles the outcome of one versioned apply: a local write's
    /// fan-out and event, unchanged from before `Engine` existed. A
    /// `Rejected` outcome (the incoming record lost) is silently a no-op, as
    /// it always was.
    ///
    /// `notify_fan_out: false` is [`Shard::insert_many`]/[`Shard::remove_many`]'s
    /// bulk path opting out of the per-write [`FanOutNotice::One`] here in
    /// favor of its own `FanOutNotice::Many` chunks after the batch — same
    /// notifications, thousands of times fewer channel slots. (A local
    /// origin is what makes a notice happen at all; remote applies never
    /// notify.)
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
                    let _ = self.fan_out.send(FanOutNotice::One(key.clone()));
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
                    let _ = self.fan_out.send(FanOutNotice::One(key.clone()));
                }
                if self.events.receiver_count() > 0 {
                    let _ = self.events.send(Event::Removed { key, origin });
                }
            }
        }
    }

    /// The versioned-apply core: applies `incoming` at `ver` for `key` iff
    /// the configured [`ConflictResolver`] picks it over whatever this shard
    /// currently holds for `key` (a live entry or a tombstone), publishing
    /// the resulting [`Event`] on success. Idempotent and commutative — the
    /// single path shared by local single writes, replicated writes, state
    /// transfer, and anti-entropy repair (bulk local writes and replicated
    /// batches go through `engine::Engine::apply_many` directly, to hold
    /// one stripe lock across a whole group). With the default
    /// [`LwwResolver`] this is exactly "apply iff `ver` is newer than the
    /// stored version", unchanged from before [`ConflictResolver`] existed.
    ///
    /// Equal versions are always a no-op, regardless of resolver: a given
    /// `(wall_ms, logical, node)` triple is produced by at most one write
    /// ever, so an equal-version incoming record is definitionally the
    /// record already stored, and the resolver's correctness contract (see
    /// [`ConflictResolver::winner`]) doesn't have to, and isn't asked to,
    /// break that tie.
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

    /// Reads `key`, without triggering read-through. Tombstones never leave
    /// a live entry to find, so a deleted key isn't present here.
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

    /// Reads whether `key` has a live entry, honoring expiry, without
    /// cloning the stored value. Reads never take a TTL argument at this API
    /// surface: this asks about the entry as it was written, not against
    /// some other deadline. An existence check, not a read: it moves neither
    /// `sundog_cache_hits_total` nor `sundog_cache_misses_total`.
    pub async fn contains_key(&self, key: &K) -> bool {
        self.engine.contains_key(key, self.now_ms())
    }

    /// The number of live entries this node currently holds, with the
    /// engine's sweep run first so completed TTL/TTI expiries are reflected
    /// rather than estimated. Sampled periodically by
    /// `cluster::cache_entries_gauge_task` to publish
    /// `sundog_cache_entries{cache}`.
    pub async fn entry_count(&self) -> u64 {
        let now = self.now_ms();
        self.engine.sweep(now);
        self.engine.live_entry_count()
    }

    /// A weakly consistent, point-in-time snapshot of this node's local live
    /// keys — not a cluster view, and no guarantee about a key inserted
    /// concurrently with the scan. Cost is O(entries).
    #[must_use]
    pub fn keys(&self) -> Vec<K> {
        self.engine.keys(self.now_ms())
    }

    /// Reads `key`, invoking `loader` on a miss. Concurrent callers racing on
    /// the same missing key are collapsed into one `loader` call.
    ///
    /// Counts `sundog_cache_misses_total{cache}` exactly once per `loader`
    /// execution, from the caller that becomes the fill's owner — every
    /// other call to this method, whether the key was already cached or it
    /// was one of the collapsed callers waiting on the same fill, counts
    /// `sundog_cache_hits_total{cache}` instead.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::Loader`] if `loader` fails.
    ///
    /// # Panics
    ///
    /// Panics if a value the loader just returned fails to postcard-encode
    /// (unexpected — the same bound `Shard::insert` already relies on to
    /// build a wire frame from any `V`).
    pub async fn get_or_load<F, E>(&self, key: &K, loader: F) -> Result<V, CacheError>
    where
        F: AsyncFnOnce(&K) -> Result<V, E>,
        E: std::error::Error + Send + Sync + 'static,
    {
        let key_bytes = encode_key(key)?;
        let hash = engine::hash_key_bytes(key_bytes.as_ref());
        loop {
            if let Some(value) = self.engine.get(key, self.now_ms()) {
                self.hits.increment(1);
                return Ok(value);
            }
            match self.engine.miss_or_join(&key_bytes, hash, self.now_ms()) {
                JoinOutcome::Hit(value) => {
                    self.hits.increment(1);
                    return Ok(value);
                }
                JoinOutcome::Join(inflight) => {
                    let notified = inflight.notify.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    notified.await;
                    if let Some(err) = inflight.error.get() {
                        return Err(CacheError::Loader(Box::new(SharedLoaderFailure(
                            Arc::clone(err),
                        ))));
                    }
                    // The fresh value (or its absence, if the fill raced with
                    // an expiry) is picked up by the fast-path read at the
                    // top of the next iteration — that re-check is what
                    // "counts as a hit" for a joined caller means.
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
                            let _ = self.fan_out.send(FanOutNotice::One(key.clone()));
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
    /// `Result` remains only for [`CacheError::Codec`] — `make` itself has
    /// no way to fail.
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

    /// Stamps and applies a local write, then fans it out per [`Mode`]
    /// (`Invalidate` for `Mode::Invalidation`, `Replicate` for
    /// `Mode::Replicated`, nothing for `Mode::Local`) — via the composition
    /// layer's subscription to [`Shard::events`], since `Shard` holds no
    /// `Mesh` handle itself.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::ValueTooLarge`] if the wire frame this write
    /// would replicate as (key, value, version, and expiry together — not
    /// just the value's own bytes) exceeds the configured frame cap (see
    /// [`Shard::with_max_frame`], default [`MAX_FRAME`]).
    pub async fn insert(&self, key: K, value: V) -> Result<(), CacheError> {
        self.insert_expiring(key, value, None).await
    }

    /// [`Shard::insert`] with a lifespan for this entry alone, overriding
    /// the shard's default TTL (or giving an entry one on a shard configured
    /// with none). The absolute deadline replicates with the record exactly
    /// as a default-TTL stamp does.
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

    /// [`Shard::insert`] for many entries, grouped by key stripe (anti-entropy
    /// bucket, since `engine::Engine` uses one stripe per bucket) and
    /// applied under one lock acquisition per touched stripe rather than one
    /// per entry — the "amortized lock path" a bulk local fill wants,
    /// bounded to a single stripe at a time so unrelated local writers and
    /// inbound applies to other stripes aren't blocked for the whole batch.
    /// Each entry still gets its own [`Hlc`] stamp and its own [`Event`];
    /// this is not a transaction, just a cheaper way to apply many
    /// independent writes: entries validated before an oversized one are
    /// applied regardless of the error this returns, and fan-out to the wire
    /// happens exactly as for individual inserts (per-event, coalesced into
    /// [`crate::wire::Msg::ReplicateBatch`] frames by `net::conn`'s writer,
    /// never as one wire message here).
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::ValueTooLarge`] if any entry's wire frame
    /// exceeds the configured frame cap (see [`Shard::insert`]).
    pub async fn insert_many(
        &self,
        entries: impl IntoIterator<Item = (K, V)>,
    ) -> Result<(), CacheError> {
        self.insert_many_expiring(entries, None).await
    }

    /// [`Shard::insert_many`] with one lifespan applied to every entry in the
    /// batch, overriding the shard's default TTL — see
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
        let mut prepared = Vec::new();
        for (key, value) in entries {
            let key_bytes = encode_key(&key)?;
            let encoded = Bytes::from(postcard::to_stdvec(&value).map_err(CodecError::from)?);
            let ver = self.stamp_local();
            let expires_at_ms = self.expiry_for(ttl);
            let wire_size =
                wire::replicate_frame_len(self.name.len(), key_bytes.len(), encoded.len());
            if wire_size > self.max_frame {
                return Err(CacheError::ValueTooLarge {
                    cache: self.name.clone(),
                    size: encoded.len(),
                    limit: self.max_frame,
                });
            }
            let hash = engine::hash_key_bytes(key_bytes.as_ref());
            prepared.push((hash, key, key_bytes, ver, value, expires_at_ms, encoded));
        }

        let mut by_stripe: Vec<Vec<_>> = (0..BUCKET_COUNT).map(|_| Vec::new()).collect();
        for entry in prepared {
            by_stripe[engine::stripe_index_from_hash(entry.0)].push(entry);
        }
        let now = self.now_ms();
        for (bucket, group) in by_stripe.into_iter().enumerate() {
            if group.is_empty() {
                continue;
            }
            let mut applied_keys: Vec<K> = Vec::with_capacity(group.len());
            let entries: Vec<_> = group
                .into_iter()
                .map(
                    |(hash, key, key_bytes, ver, value, expires_at_ms, encoded)| {
                        applied_keys.push(key.clone());
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
                self.handle_apply_outcome(outcome, Origin::Local, false);
            }
            // One `Many` notice per chunk instead of one `One` per entry
            // (see `handle_apply_outcome`'s `notify_fan_out` docs), sent per
            // stripe as it lands rather than after the whole batch:
            // replication streams concurrently with the remaining stripes,
            // so peers start catching up mid-fill instead of anti-entropy
            // racing a silent bulk and re-shipping it record by record.
            for chunk in applied_keys.chunks(FAN_OUT_MANY_CHUNK) {
                let _ = self.fan_out.send(FanOutNotice::Many(chunk.to_vec()));
            }
        }
        Ok(())
    }

    /// Stamps and applies a local tombstone, then fans it out per [`Mode`]
    /// (see [`Shard::insert`]'s note on how fan-out actually happens).
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

    /// [`Shard::remove`] for many keys at once: each is stamped with its own
    /// tombstone version, grouped by key stripe (anti-entropy bucket), and
    /// applied under one lock acquisition per touched stripe — the tombstone
    /// counterpart of [`Shard::insert_many`]. **Not a transaction** — same
    /// caveat as [`Shard::insert_many`], read "written" as "tombstoned": if
    /// a key partway through fails to encode, the keys before it are still
    /// tombstoned. Emits one [`Event::Removed`] per key and fans out in
    /// `FanOutNotice::Many` chunks exactly as [`Shard::insert_many`]
    /// describes.
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
        for (bucket, group) in by_stripe.into_iter().enumerate() {
            if group.is_empty() {
                continue;
            }
            let mut applied_keys: Vec<K> = Vec::with_capacity(group.len());
            let entries: Vec<_> = group
                .into_iter()
                .map(|(hash, key, key_bytes, ver)| {
                    applied_keys.push(key.clone());
                    (hash, key, key_bytes, ver, Incoming::Tombstone)
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
            for outcome in outcomes {
                self.handle_apply_outcome(outcome, Origin::Local, false);
            }
            for chunk in applied_keys.chunks(FAN_OUT_MANY_CHUNK) {
                let _ = self.fan_out.send(FanOutNotice::Many(chunk.to_vec()));
            }
        }
        Ok(())
    }

    /// Tombstones every key this node currently holds, via
    /// [`Shard::remove_many`] over a snapshot of [`Shard::keys`]. This is
    /// "remove every key I currently hold" — an entry a peer holds that
    /// never reached this node, or a concurrent write that outraces the
    /// snapshot's tombstone on the [`Hlc`], is untouched and survives. In
    /// [`Mode::Replicated`], where every node holds every entry, that makes
    /// it a cluster-wide clear once the fanned-out tombstones converge; in
    /// [`Mode::Invalidation`] or [`Mode::Local`] it only ever empties this
    /// node's own working set. Cost is O(entries) — a full scan of the local
    /// cache.
    ///
    /// # Errors
    ///
    /// As [`Shard::remove_many`].
    pub async fn clear(&self) -> Result<(), CacheError> {
        self.remove_many(self.keys()).await
    }

    /// Drops the local copy of `key` without writing a tombstone or fanning
    /// out — an escape hatch for tests and manual cache-busting; the entry
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

    /// Subscribes to this shard's lightweight keys-only fan-out
    /// notifications — `fan_out_task`'s cheaper alternative to
    /// subscribing on [`Shard::events`] directly, since it never reads
    /// `Event`'s `value` at all. Carries local writes only; see
    /// [`FanOutNotice`].
    pub(crate) fn fan_out_events(&self) -> broadcast::Receiver<FanOutNotice<K>> {
        self.fan_out.subscribe()
    }

    /// [`ShardOps::records_for`], but for callers that already hold typed
    /// `K`s (`cluster::fan_out_batch`, re-fetching for keys it just read off
    /// its own `Event<K, V>`s) — encodes each key straight to the bytes
    /// `engine::Engine::record_for` looks entries up by, with no decode
    /// step on either end.
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
            // Grouping by raw key bytes' stripe needs no decode: the hash
            // that picks a stripe only ever looks at the wire bytes.
            // Preserves each group's relative order (same key always lands
            // in the same stripe, in the order it appeared in `recs`), which
            // is all per-key serialization needs — order between different
            // keys was never a requirement.
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
                            // The verbatim bytes just received off the wire,
                            // not a fresh re-encode of `value` — decoded
                            // once above only to satisfy the
                            // resolver/typed-cache boundary.
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
        // Every requested bucket exactly once, ascending — an anti-entropy
        // responder must report even the buckets where it holds nothing, so
        // the initiator learns to push.
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

    /// The anti-entropy bucket `key_bytes` hashes into — the engine's own
    /// `hash_key_bytes`/`stripe_index_from_hash`, which double as the
    /// per-key lock stripe now that a stripe *is* a bucket.
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
            "older write must not overwrite a newer one"
        );

        // Re-applying the exact same winning record must also be a no-op.
        ShardOps::apply_remote(&s, winner).await;
        assert_eq!(s.get(&1).await, Some("x".into()));
    }

    #[tokio::test]
    async fn invalidate_respects_newer_local_write() {
        let s = shard::<u32, String>(1);
        s.insert(1, "fresh".into()).await.expect("insert");

        // An invalidation for an old version must not evict a newer local write.
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

        // A hit must not call the loader again.
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
                    // Widens the race window: every concurrent caller must
                    // still be waiting on this single in-flight load.
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    Ok("loaded-once".to_string())
                })
                .await
                .expect("loader succeeds")
            });
        }

        let mut results = Vec::with_capacity(CONCURRENCY);
        while let Some(result) = tasks.join_next().await {
            results.push(result.expect("stampede task does not panic"));
        }

        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "loader must run exactly once under a stampede of {CONCURRENCY} concurrent misses"
        );
        assert!(results.iter().all(|value| value == "loaded-once"));
        assert_eq!(results.len(), CONCURRENCY);
    }

    #[tokio::test]
    async fn value_too_large_is_rejected() {
        let s = shard::<u32, Vec<u8>>(1);
        let big = vec![0u8; MAX_FRAME + 1];
        let err = s
            .insert(1, big)
            .await
            .expect_err("must reject oversized value");
        assert!(matches!(err, CacheError::ValueTooLarge { .. }));
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
                "a single snapshot chunk (as sent inside one Msg::StChunk) must stay under the wire frame cap"
            );
        }
        assert_eq!(total_records, record_count as usize);
        assert!(
            chunk_count > 1,
            "50 * 100KB records (5MB) must not fit in one wire-frame-bounded chunk"
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

    /// The first `u32` from `1` on whose stripe (= anti-entropy bucket)
    /// differs from key `0`'s — used by the striping tests below to get two
    /// real keys guaranteed to serialize independently, without depending on
    /// any hash's exact output.
    fn two_keys_in_different_stripes() -> (u32, u32) {
        let a = 0u32;
        let a_stripe = bucket_of(&key_bytes(&a));
        let b = (1u32..10_000)
            .find(|b| bucket_of(&key_bytes(b)) != a_stripe)
            .expect("some key among the first 10,000 lands in a different stripe from key 0");
        (a, b)
    }

    /// Direct proof of the striping property, probed at the engine's own
    /// lock level: a raw `parking_lot::RwLock` blocks the OS thread that
    /// waits on it rather than yielding to an async runtime the way the old
    /// per-stripe `tokio::sync::Mutex` did, so this holds one stripe's write
    /// lock from a real OS thread and checks with `try_write` rather than
    /// racing an async `tokio::time::timeout` around `Shard::insert` (which
    /// would just freeze a current-thread runtime instead of timing out). A
    /// different stripe's lock must stay free throughout; the held stripe's
    /// own lock must stay contended until the holder releases it — the
    /// latter proving the former is really exercising cross-stripe
    /// concurrency, not just a no-op lock.
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
            "a different stripe's lock must not be blocked"
        );
        assert!(
            s.engine.stripe_lock(bucket_a).try_write().is_none(),
            "the held stripe's lock must still be contended"
        );
        handle.join().expect("lock holder thread does not panic");
        assert!(
            s.engine.stripe_lock(bucket_a).try_write().is_some(),
            "released after the holder finishes"
        );

        // And the striping is real end to end: an insert into the
        // (now-released) held stripe still lands.
        s.insert(key_a, "a".into()).await.expect("insert");
        assert_eq!(s.get(&key_a).await, Some("a".into()));
    }

    /// Spawns concurrent `insert` tasks against keys chosen to land in
    /// distinct stripes and confirms every one lands — real concurrent
    /// scheduling (not just a manually held guard) across many independent
    /// stripes at once.
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
            handle.await.expect("task did not panic").expect("insert");
        }
        for k in keys {
            assert_eq!(s.get(&k).await, Some(format!("v{k}")));
        }
    }

    /// Lifespan travels as an *absolute* `expires_at_ms`, computed
    /// once at the origin. `a` is built with a TTL; `b` is not — proving the
    /// deadline that ultimately expires `b`'s copy is the one baked into the
    /// wire record `a` sent, not something `b` would derive from its own
    /// (absent) local TTL config.
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
        let b = shard::<u32, String>(2); // no ttl configured here at all

        a.insert(1, "short-lived".into()).await.expect("insert");
        let recs = ShardOps::records_for(&a, vec![key_bytes(&1u32)]).await;
        assert_eq!(recs.len(), 1);
        assert!(
            recs[0].expires_at_ms.is_some(),
            "the wire record must carry the absolute deadline `a` computed"
        );
        ShardOps::apply_remote(&b, recs[0].clone()).await;
        assert_eq!(b.get(&1).await, Some("short-lived".into()));

        // A read treats a past deadline as absent immediately — no sweep
        // needed for that — but this waits generously past the 50ms TTL
        // regardless, matching the margin the digest-flushing test below
        // needs.
        tokio::time::sleep(Duration::from_millis(1300)).await;
        assert_eq!(a.get(&1).await, None, "the origin's own copy must expire");
        assert_eq!(
            b.get(&1).await,
            None,
            "b must expire the entry from the wire-carried deadline alone"
        );
    }

    /// A per-write TTL overrides the shard default in both directions and
    /// travels as the record's own absolute deadline: `a` defaults to a long
    /// TTL, writes entries with a short override (single and batch) and one
    /// with the default, and `b` (no TTL configured at all) expires exactly
    /// the short ones from the wire-carried deadline. Reads stay out of it:
    /// a `get_or_load` fill takes the default, and a hit changes nothing.
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
        // A hit must not re-stamp: key 1 keeps its short override.
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

    /// The absolute-expiry guarantee, stated directly: a record whose
    /// `expires_at_ms` is already in the past when it arrives never becomes
    /// readable, whether it lands through [`ShardOps::apply_remote`] or
    /// [`ShardOps::apply_remote_batch`] — regardless of whether the key
    /// already held a value that expired here for real, or never existed at
    /// all. A read treats a past `expires_at_ms` as absent unconditionally,
    /// so the deadline alone decides readability the instant this apply
    /// returns, before any later sweep runs.
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
            "sanity: the original entry must actually be gone before this test means anything"
        );

        let doa = WireRecord {
            key: key_bytes(&1u32),
            // Clearly newer than anything this shard's clock has stamped, so
            // nothing about version comparison could be why this is
            // rejected — only the absolute deadline can be.
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
            "a dead-on-arrival record must never resurrect a locally-expired entry, via apply_remote"
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

    /// Forces one tombstone's `ttl_deadline_ms` (and, for
    /// [`gc_tombstones_hard_cap_overrides_deferral`], `max_deadline_ms`) into
    /// the past, exactly as [`gc_tombstones_drops_expired_entries_and_updates_digest`]
    /// does for the plain (no-absence) case.
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
            "a tombstone past tombstone_ttl but not tombstone_max_ttl must survive \
             collection while a member is absent"
        );
        assert_digest_matches_full_recompute(&s).await;
    }

    /// Also the digest/tombstone-set consistency check across a
    /// deferred-then-collected cycle: the digest must agree with a full
    /// recompute both right after the deferred pass (tombstone still
    /// present) and right after the pass that finally collects it.
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
            "a tombstone past tombstone_max_ttl must be collected even while a member is absent"
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

        // Logical expiry is visible to reads immediately once it's past —
        // before the engine's sweep has run to correct the digest.
        assert!(
            ShardOps::bucket_entries(&s, bucket_of(&key_bytes(&1u32)))
                .await
                .is_empty()
        );

        // Without this periodic flush, the engine's sweep would never run
        // for this now-quiet shard on its own, and the digest would
        // disagree with `bucket_entries` forever.
        ShardOps::run_pending_tasks(&s).await;
        assert_digest_matches_full_recompute(&s).await;
    }

    /// Compares the engine's incrementally-maintained digest against
    /// `engine::Engine::recompute_digests`'s full pass over every stripe's
    /// live entries and tombstones.
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
            .expect_err("must reject a value over the overridden cap");
        assert!(matches!(err, CacheError::ValueTooLarge { .. }));

        s.insert(2, Vec::new())
            .await
            .expect("a small value still fits under the overridden cap");
    }

    #[tokio::test]
    async fn insert_rejects_when_the_value_alone_fits_but_the_wire_frame_does_not() {
        // The value's own postcard encoding (~21 bytes for a 20-byte Vec<u8>)
        // fits under a 25-byte cap; the key, HLC version, expiry, cache name,
        // and Msg::Replicate envelope around it do not. A check against the
        // value's bytes alone (the pre-fix behavior) would wrongly accept
        // this, then fail later when the wire codec actually encodes it.
        let s = shard::<u32, Vec<u8>>(1).with_max_frame(25);
        let err = s
            .insert(1, vec![0u8; 20])
            .await
            .expect_err("the full wire frame, not just the value, must count toward the cap");
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
            "a custom weigher must drive the engine's total weight, not the default of 1 per entry"
        );
    }

    /// A non-default resolver used to prove `ConflictResolver` actually
    /// changes `Shard::apply`'s outcome (default LWW is exercised by every
    /// test above, unchanged): whichever record has the longer value wins,
    /// `Hlc` breaking ties on equal length. This is a proper total order —
    /// `(len, ver)` compared lexicographically — so it satisfies the
    /// determinism/antisymmetry/totality contract `ConflictResolver::winner`
    /// documents.
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

    /// A resolver that always prefers whichever record is passed as `a` —
    /// deliberately violates `ConflictResolver::winner`'s antisymmetry
    /// requirement. Not exercised by a convergence test (the property
    /// cannot hold for it, per the trait docs); this only demonstrates that
    /// such a resolver is trivially distinguishable from `LongestValueWins`
    /// above, so the doc's warning has a concrete referent.
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
        ];
        for &a in &samples {
            for &b in &samples {
                if a.ver == b.ver {
                    continue; // Shard::apply never asks about equal versions.
                }
                let ab = resolver.winner(b"k", a, b);
                let ba = resolver.winner(b"k", b, a);
                assert_ne!(ab, ba, "swapping the arguments must swap the winner");
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

        // Strictly newer by Hlc, but shorter: must lose under LongestValueWins.
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
            "the default Hlc-only rule must not apply once a custom resolver is installed"
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

    /// The permutation-convergence property, specialized to a
    /// custom resolver: two independent shards, sharing the same
    /// [`LongestValueWins`] resolver, apply the same set of writes/removes in
    /// different orders with duplicates — and must land on identical state
    /// and identical digests. This is the license for pluggability: the
    /// property that makes replaying records in any order safe does not rely
    /// on any specific resolver, only on the resolver's own contract.
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
        // Reverse order, each record applied twice — reordering and
        // duplication together, the two hazards anti-entropy must tolerate.
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
    async fn remove_many_tombstones_every_key_and_leaves_others_untouched() {
        let s = shard::<u32, String>(1);
        for k in 0..5u32 {
            s.insert(k, k.to_string()).await.expect("insert");
        }

        let mut events = s.events();
        s.remove_many([0u32, 1, 2]).await.expect("remove_many");

        for k in 0..3u32 {
            assert_eq!(s.get(&k).await, None, "key {k} must be tombstoned");
            let recs = ShardOps::records_for(&s, vec![key_bytes(&k)]).await;
            assert_eq!(recs.len(), 1);
            assert!(
                recs[0].is_tombstone(),
                "key {k} must read back as a tombstone"
            );
        }
        for k in 3..5u32 {
            assert_eq!(
                s.get(&k).await,
                Some(k.to_string()),
                "key {k} was not in the batch and must survive"
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
            assert_eq!(recs.len(), 1, "key {k} must leave a tombstone");
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

    /// [`Shard::with_clock`] must drive every timestamp this shard stamps —
    /// HLC stamping, expiry deadlines, and the sweep — not just some of
    /// them: a write's HLC and its TTL deadline are pinned to the injected
    /// clock's initial value, then advancing the injected clock (not real
    /// time) past the TTL makes the entry unreadable and lets a subsequent
    /// sweep collect it.
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
            "the HLC stamp must read the injected clock, not the system clock"
        );
        assert_eq!(
            recs[0].expires_at_ms,
            Some(1_100),
            "the expiry deadline must be computed from the injected clock"
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
            "the sweep must have run against the injected clock, not real time"
        );
    }
}

#[cfg(test)]
mod prop_tests;

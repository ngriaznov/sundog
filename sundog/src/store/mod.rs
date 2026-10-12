//! Typed shards over the store engine: versioned apply, conflict resolution,
//! tombstones, and the per-bucket digests anti-entropy compares.
//!
//! A `Shard` holds no network handle. Every local write pushes its key onto the
//! shard's fan-out queue and publishes an `Origin::Local` [`Event`]; the
//! cluster layer turns those into wire traffic.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::Hash;
#[cfg(feature = "spill")]
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex as StdMutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::{self, BoxStream, StreamExt};
use serde::Serialize;
use serde::de::DeserializeOwned;
use smol_str::SmolStr;
use tokio::sync::{Notify, broadcast};
use xxhash_rust::xxh3::xxh3_64;

#[cfg(doc)]
use crate::config::ClusterConfig;
#[cfg(doc)]
use crate::error::CacheError;
use crate::error::CodecError;
use crate::hlc::{Hlc, HlcClock};
use crate::node::NodeId;
use crate::ownership::{Granularity, OwnershipTracker, OwnershipView, ResidencySet};
use crate::verdict;
use crate::wire::{self, MAX_FRAME, WireRecord};

mod engine;
mod fan_out;
mod load;
pub(crate) mod part;
mod read_timing;
mod refresh;
mod resolver;
mod shard_build;
mod shard_merge;
mod shard_ops;
mod shard_read;
mod shard_rearm;
mod shard_spill;
mod shard_write;
mod types;
use engine::Engine;

/// Reference CRDT value types (a PN-Counter today) and the
/// [`ConflictResolver`]s that merge them through [`ConflictResolver::merge`].
pub mod crdt;

/// The optional local SSD/NVMe spill tier. Off by default; see
/// [`spill::SpillConfig`] and [`crate::cache::CacheBuilder::spill`].
#[cfg(feature = "spill")]
pub mod spill;

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

/// Number of anti-entropy parts per bucket: `part(k) = (xxh3(key_bytes) >> 10)
/// & (PART_COUNT - 1)`, the six hash bits above the ten a key's bucket
/// consumes. A mismatched bucket with more entries than
/// [`crate::config::ClusterConfig::ae_part_min_bucket`] is compared at this
/// finer grain before either side sends a listing or a sketch.
pub const PART_COUNT: usize = 64;

pub(crate) use engine::hash_key_bytes;
#[cfg(feature = "sim")]
#[doc(hidden)]
pub use fan_out::SimFanOut;
pub(crate) use fan_out::{FanOutItem, FanOutQueue};
pub(crate) use load::Sourced;
pub use load::{DEFAULT_MAX_BATCH_KEYS, LoadError, Loader};
pub use part::PartId;
pub(crate) use part::PartMask;
pub(crate) use refresh::{RefreshRequest, refresh_permille};
pub use resolver::{ConflictResolver, LwwResolver, Merged, RecordView, Winner};
#[cfg(doc)]
use shard_build::SettlingResolver;
#[cfg(doc)]
use shard_merge::drain_due_deadlines;
use shard_ops::MaskedDigest;
pub use shard_ops::ShardOps;
pub(crate) use shard_ops::{Scope, ScopedDigests};
pub use types::{
    BucketDigest, BucketEntries, BucketLen, BucketPart, BucketPartDigests, CompactPassOutcome,
    CompactionBounds, Event, KeyVersion, Mode, Origin, PartEntries, Quiescence, Ttl,
};

/// A custom per-entry weigher for size-bounded eviction: `(key, value) ->
/// weight`. Boxed so [`crate::cache::CacheBuilder::weigher`] and
/// [`Shard::with_weigher`] can store one before its closure type is nameable.
pub(crate) type Weigher<K, V> = Box<dyn Fn(&K, &V) -> u32 + Send + Sync>;

/// [`Shard::with_weigher`]'s installed weigher, kept as an `Arc` outside the
/// engine so a [`Shard::with_capacity_hint`] rebuild can carry it forward.
pub(crate) type SharedWeigher<K, V> = Arc<dyn Fn(&K, &V) -> u32 + Send + Sync>;

/// Upper bound on records per [`WireRecord`] batch yielded by
/// [`ShardOps::snapshot_chunks`], caps chunk size only for small-value caches:
/// a chunk breaks sooner once its encoded size approaches [`MAX_FRAME`].
const SNAPSHOT_CHUNK_SIZE: usize = 500;

/// Headroom reserved below [`MAX_FRAME`] for the `Msg::StChunk` envelope around
/// a snapshot chunk's records. `pub(crate)` so
/// [`crate::config::ClusterConfig::rebalance_chunk_bytes_value`] shares this
/// headroom with `Msg::StBucketChunk`.
pub(crate) const SNAPSHOT_CHUNK_ENVELOPE_HEADROOM: usize = 4 * 1024;

/// Groups `records` into chunks that stay under [`MAX_FRAME`] once wrapped in a
/// `Msg::StChunk`. Splits on cumulative wire-encoded size as well as
/// [`SNAPSHOT_CHUNK_SIZE`], since a fixed record count alone undercounts caches
/// whose average value exceeds a few KiB.
pub(crate) fn chunk_records_for_snapshot(records: Vec<WireRecord>) -> Vec<Vec<WireRecord>> {
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

/// The anti-entropy bucket `key_bytes` hashes into, `u16`-sized to match
/// every bucket-carrying wire and `ShardOps` shape. Ownership reads the
/// finer [`PartId::of_key`].
#[cfg(test)]
pub(crate) fn bucket_of(key_bytes: &[u8]) -> u16 {
    PartId::of_key(key_bytes).bucket()
}

/// One inbound [`WireRecord`], decoded down to what
/// [`ShardOps::apply_remote_batch`]'s per-stripe grouping needs: its
/// precomputed hash, typed key, raw key bytes, version, and the value or
/// tombstone it carries, plus which peer (or local call) it came from.
type RemoteEntry<K, V> = (u64, K, Bytes, Hlc, Incoming<V>, Origin);

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
        /// `value`'s postcard-encoded bytes. On the local-origin path,
        /// `insert`/`insert_many`/`get_or_load`'s fill, these are the
        /// first encode's bytes; on the replica-apply path,
        /// `apply_remote_batch`, they are the verbatim wire bytes. Always
        /// equal to `postcard::to_stdvec(&value)`, or to wire bytes
        /// decoding to a structurally equal `value`.
        encoded: Bytes,
    },
    Tombstone,
}

/// The lifespan a re-arm gives an entry, from now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Lifetime {
    /// Expires after this long.
    For(Duration),
    /// The shard's default TTL, or no expiry on a shard without one.
    Default,
    /// Never expires.
    Unbounded,
}

/// What [`Shard::rearm_local`] found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LocalRearm {
    /// A live entry was here and took the new lifetime, or a write stamped
    /// after it superseded the re-arm.
    Rearmed,
    /// No live entry, and this shard holds the key's part warm: the key is
    /// absent.
    Absent,
    /// A `Mode::Distributed` shard that does not own the key's part, or
    /// owns it but has not yet pulled or verified it: only an owner's
    /// copy answers.
    NotHeld,
}

/// One key's in-flight [`Shard::merge`] fold: accumulated in memory, across
/// however many `merge` calls land inside the current coalescing window,
/// until [`Shard::flush_due_pending_merges`] (or a close/drop flush that
/// ignores the deadline) hands it to the ordinary write path as a single
/// [`Shard::apply`], the same versioned apply every other write goes
/// through, so the fold gets its own fresh version and folds again, this
/// time against whatever is stored.
struct PendingMerge<V> {
    /// `K`'s encoded bytes, kept so a flush never re-encodes the key.
    key_bytes: Bytes,
    value: V,
    /// `value`'s postcard-encoded bytes, kept alongside `value` for the same
    /// reason [`Incoming::Put::encoded`] is: a fold to fold again can reuse
    /// them without a re-encode.
    encoded: Bytes,
    expires_at_ms: Option<u64>,
    /// The version stamped by whichever `merge` call last folded into this
    /// entry, used only as the "incoming" side of the next fold's
    /// [`RecordView`], discarded at flush time in favor of a fresh stamp,
    /// since the flush itself, not any one call that built up to it, is the
    /// write that lands.
    ver: Hlc,
}

/// One [`Shard::merge`] coalescing stripe: the pending folds whose key
/// hashes into it, plus a deadline-ordered index into them.
/// [`engine::stripe_index_from_hash`] picks the stripe, the same way it
/// picks an anti-entropy bucket for the engine's own stripes, so
/// [`Shard::flush_due_pending_merges`] locks and scans one stripe at a time
/// rather than the whole shard's pending state under one mutex.
struct PendingMergeStripe<K, V> {
    entries: HashMap<K, PendingMerge<V>>,
    /// `(deadline_ms, seq) -> key`, ordered by deadline so
    /// [`drain_due_deadlines`] pops exactly the entries due instead of
    /// scanning every entry in this stripe on every sweep.
    /// `seq` (this stripe's own [`Self::next_seq`] at insertion) breaks a
    /// tie between two entries that share a `deadline_ms` deterministically,
    /// without requiring `K: Ord`.
    by_deadline: BTreeMap<(u64, u64), K>,
    next_seq: u64,
}

impl<K, V> PendingMergeStripe<K, V> {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            by_deadline: BTreeMap::new(),
            next_seq: 0,
        }
    }
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

pub(crate) fn now_ms() -> u64 {
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

/// Bytes of [`entry_fingerprint`]'s stack buffer, which holds a key and its
/// encoded version up to this length together.
const FINGERPRINT_STACK_BYTES: usize = 128;

/// `xxh3(key_bytes ‖ postcard(ver))`, the digest contribution of one live entry
/// or tombstone. Runs on every apply, so the key and the encoded `ver` join in
/// a stack buffer; only a key too long for it joins in a heap `Vec`. Both
/// paths hash the same bytes.
fn entry_fingerprint(key_bytes: &[u8], ver: Hlc) -> u64 {
    let mut buf = [0u8; FINGERPRINT_STACK_BYTES];
    if let Some(key_slot) = buf.get_mut(..key_bytes.len()) {
        key_slot.copy_from_slice(key_bytes);
        if let Ok(ver_bytes) = postcard::to_slice(&ver, &mut buf[key_bytes.len()..]) {
            let len = key_bytes.len() + ver_bytes.len();
            return xxh3_64(&buf[..len]);
        }
    }
    let mut ver_buf = [0u8; HLC_ENCODED_MAX];
    let ver_bytes = postcard::to_slice(&ver, &mut ver_buf)
        .expect("invariant: Hlc always postcard-encodes within HLC_ENCODED_MAX bytes");
    let mut joined = Vec::with_capacity(key_bytes.len() + ver_bytes.len());
    joined.extend_from_slice(key_bytes);
    joined.extend_from_slice(ver_bytes);
    xxh3_64(&joined)
}

/// The version a compacted record takes: the stale version's own successor
/// in `wall_ms`/`logical`, with the node tiebreaker derived from the
/// compacted bytes the same way `merge_version` derives it from merged
/// bytes. Two replicas compacting byte-identical records at the same
/// version therefore mint byte-identical versions and their digests keep
/// agreeing, while two that reached different bytes from the same version
/// (one retired a writer the other has not yet judged dead) mint different
/// versions, so anti-entropy still sees the difference and merges it. A
/// version minted from the local clock instead differs per replica for
/// identical bytes, and anti-entropy then re-exchanges every compacted
/// record in both directions after every pass. The shard observes the
/// minted version into its own clock right after, so a local write in the
/// same millisecond still stamps strictly later.
fn compacted_version(stale: Hlc, compacted: &[u8]) -> Hlc {
    derived_successor(stale, xxh3_64(compacted))
}

/// The version a re-armed record carries: `base`'s successor, keyed by
/// `base` and the new deadline the way [`compacted_version`] keys by
/// content. It outranks the record it re-arms and every older one, and
/// loses to any write stamped after `base` in a later millisecond, so a
/// re-arm never overrides a write or delete made after the value it read.
/// Two re-arms of one record to different deadlines mint different
/// versions, since two contents never share one; the same re-arm made
/// twice mints the same version.
fn rearmed_version(base: Hlc, expires_at_ms: Option<u64>) -> Hlc {
    let mut buf = [0u8; 30];
    buf[0] = b'r';
    buf[1..9].copy_from_slice(&base.wall_ms.to_le_bytes());
    buf[9..13].copy_from_slice(&base.logical.to_le_bytes());
    buf[13..21].copy_from_slice(&base.node.as_u64().to_le_bytes());
    if let Some(deadline) = expires_at_ms {
        buf[21] = 1;
        buf[22..30].copy_from_slice(&deadline.to_le_bytes());
    }
    derived_successor(base, xxh3_64(&buf))
}

/// `stale`'s successor in `wall_ms`/`logical`, carrying into the next
/// millisecond on a logical overflow, stamped with a node id derived from
/// `content_hash`.
fn derived_successor(stale: Hlc, content_hash: u64) -> Hlc {
    let (wall_ms, logical) = match stale.logical.checked_add(1) {
        Some(logical) => (stale.wall_ms, logical),
        None => (stale.wall_ms.saturating_add(1), 0),
    };
    Hlc {
        wall_ms,
        logical,
        node: NodeId::merge_derived(content_hash),
    }
}

/// Postcard-encodes `key`, its wire form and digest-hash input alike.
///
/// Debug builds assert the encoding round-trips to itself. A key type whose
/// `Serialize` impl is not canonical, such as an iteration-order-dependent
/// `HashMap`-typed key, would silently corrupt digests and break wire identity.
pub(crate) fn encode_key<K>(key: &K) -> Result<Bytes, CodecError>
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

/// A capped shard's `sundog_ceiling_refusals_total{cache, kind}` handles,
/// created once like `hits`.
struct CeilingRefusals {
    /// `kind="write"`: a local write refused at the ceiling.
    writes: metrics::Counter,
    /// `kind="fill"`: a read-through fill returned without being stored.
    fills: metrics::Counter,
}

impl CeilingRefusals {
    fn new(cache: &str) -> Self {
        let counter = |kind: &'static str| {
            metrics::counter!(
                "sundog_ceiling_refusals_total",
                "cache" => cache.to_string(),
                "kind" => kind
            )
        };
        Self {
            writes: counter("write"),
            fills: counter("fill"),
        }
    }
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
    fan_out: Arc<FanOutQueue<FanOutItem<K>>>,
    /// Guards only the synchronous HLC bump itself, never held across `.await`.
    clock: StdMutex<HlcClock>,
    /// The deterministic-clock hook every timestamp this shard stamps reads, in
    /// place of the system clock. Defaults to it in [`Shard::new`];
    /// overridden by [`Shard::with_clock`].
    clock_fn: Arc<dyn Fn() -> u64 + Send + Sync>,
    ttl: Option<Duration>,
    tombstone_ttl_ms: u64,
    tombstone_max_ttl_ms: u64,
    /// [`ClusterConfig::max_clock_skew`] in milliseconds: how far ahead of
    /// this node's clock a remote stamp may be before
    /// [`Shard::observe_remote`] refuses it. `None` accepts any stamp.
    max_clock_skew_ms: Option<u64>,
    /// Handle for `sundog_clock_skew_rejected_total{cache}`, created once
    /// here like `hits`.
    skew_rejected: metrics::Counter,
    /// Set once the first refused stamp has been logged at `warn`; later
    /// refusals log at `debug` and only count.
    skew_warned: AtomicBool,
    /// Set once this node's clock has been logged as further behind its
    /// own last stamp than `max_clock_skew_ms`.
    clock_jump_warned: AtomicBool,
    /// The resolver every decision runs through: `raw_resolver` wrapped in
    /// a [`SettlingResolver`] carrying `crdt_retire_after_ms` and this
    /// shard's clock, rebuilt whenever any of the three changes.
    resolver: Arc<dyn ConflictResolver>,
    /// The resolver as handed to [`Shard::with_resolver`].
    raw_resolver: Arc<dyn ConflictResolver>,
    /// The bounds [`ConflictResolver::settle`] prunes fold receipts against
    /// on every merge apply; the compaction sweep passes the same bounds
    /// per pass.
    crdt_bounds: CompactionBounds,
    max_frame: usize,
    /// [`Shard::with_max_resident_bytes`]'s ceiling: a local write that
    /// finds [`Shard::resident_bytes`] at or over it fails with
    /// [`CacheError::OverMemoryCeiling`]. `u64::MAX` means no ceiling.
    max_resident_bytes: u64,
    /// `sundog_ceiling_refusals_total` handles, registered by
    /// [`Shard::with_max_resident_bytes`] only, so an uncapped cache
    /// exports no refusal series.
    ceiling_refusals: Option<CeilingRefusals>,
    /// [`Shard::with_fan_out_backlog_capacity`]'s cap: the threshold
    /// [`Shard::wait_for_fan_out_room`] waits against, default from [`ClusterConfig::default`].
    fan_out_backlog_capacity: usize,
    /// [`Shard::with_fan_out_wait_timeout`]'s bound on
    /// [`Shard::wait_for_fan_out_room`]'s wait, default from [`ClusterConfig::default`].
    fan_out_wait_timeout: Duration,
    /// [`Shard::with_merge_coalesce_window`]'s configured window. Zero (the
    /// default) means every [`Shard::merge`] call applies at once;
    /// `crate::cache::CacheBuilder::merge_coalesce_window` is the validated
    /// entry point that sets this through a merging resolver only.
    merge_window: Duration,
    /// Keys with an in-flight [`Shard::merge`] fold not yet applied, split
    /// into [`BUCKET_COUNT`] independently locked stripes by the same
    /// `xxh3(key_bytes)`-derived index [`engine::stripe_index_from_hash`]
    /// gives the engine's own stripes, so many keys coalescing at once
    /// spread their pending state, and the background sweep's own lock
    /// contention, across many mutexes instead of one. Only ever non-empty
    /// while `merge_window` is non-zero.
    pending_merges: Box<[StdMutex<PendingMergeStripe<K, V>>]>,
    /// Notified whenever [`Shard::merge`] opens a fresh coalescing window
    /// (a key with nothing already pending), so
    /// `crate::cache::merge_coalesce_task`'s sweep can recompute its
    /// sleep-until instead of parking past a deadline that did not exist
    /// yet when it last checked.
    merge_wake: Notify,
    /// Remembered alongside `tti`/`capacity_hint`/`weigher` so
    /// [`Shard::with_weigher`] and [`Shard::with_capacity_hint`] can each
    /// rebuild the engine without losing what the other already set.
    /// construction.
    max_capacity: u64,
    tti: Option<Duration>,
    /// [`Shard::with_capacity_hint`]'s hint, `None` until set. Threaded into
    /// every rebuild so it survives a later [`Shard::with_weigher`] call.
    capacity_hint: Option<u64>,
    /// [`Shard::with_weigher`]'s installed weigher, `None` until set.
    /// Threaded into every rebuild so it survives a later capacity-hint call.
    weigher: Option<SharedWeigher<K, V>>,
    /// Handle for `sundog_cache_hits_total{cache}`, created once here since
    /// label resolution costs more than the read path can afford per call.
    hits: metrics::Counter,
    /// Handle for `sundog_cache_misses_total{cache}`, same reason as `hits`.
    misses: metrics::Counter,
    /// `sundog_read_duration_seconds{cache,outcome}`, sampled.
    read_durations: read_timing::ReadDurations,
    /// [`Shard::with_loader`]'s loader, which [`Shard::load`] and
    /// [`Shard::load_many`] answer a miss through.
    loader: Option<Loader<K, V>>,
    /// `sundog_loader_calls_total{cache}`, registered by
    /// [`Shard::with_loader`] only, so a cache with no loader exports no
    /// series.
    loader_calls: Option<metrics::Counter>,
    /// The loads waiting for the loader's batch window to close.
    load_queue: load::LoadQueue<K, V>,
    /// [`Shard::with_refresh_ahead`]'s configuration and state, `None`
    /// without refresh-ahead.
    refresh: Option<refresh::Refresh<K>>,
    /// `Some` only for `Mode::Distributed`, set once at construction via
    /// [`Shard::with_ownership`], before this shard is ever shared. Backs
    /// [`ShardOps::ownership_view`].
    ownership: Option<OwnershipTracker>,
    /// `Some` only for `Mode::Distributed`. Drives anti-entropy's cohort
    /// widening and the donor-serving exception; never consulted by the
    /// inbound-apply guard, which stays strict current-view ownership.
    residency: Option<Arc<ResidencySet>>,
    /// Set by `Shard::attach_spill`: the semaphore bounding concurrent
    /// disk reads and the metric handles the spilled-key read path counts
    /// against. Unset until then, and always unset in a non-`spill` build.
    /// A `OnceLock`, not a plain `Option` behind `&mut self`, so
    /// `attach_spill` can run through `&self`. See its own doc for why
    /// that matters.
    #[cfg(feature = "spill")]
    spill_read: OnceLock<SpillRead>,
}

/// [`Shard::with_spill`]'s read-side counterpart to the tier itself: the
/// semaphore [`Shard::get`]/[`Shard::get_or_load`]'s disk reads acquire a
/// permit from before `spawn_blocking`-ing a positional read, bounded by
/// the configured `read_concurrency`, plus the four `Counter` handles and
/// the `Histogram` handle those reads and the batched read path count and
/// time against, precomputed for the same reason `Shard::hits`/`Shard::misses`
/// are.
#[cfg(feature = "spill")]
#[derive(Clone)]
struct SpillRead {
    semaphore: Arc<tokio::sync::Semaphore>,
    /// `sundog_spill_reads_total{cache,outcome="hit"}`.
    reads_hit: metrics::Counter,
    /// `sundog_spill_reads_total{cache,outcome="stale"}`: a generation
    /// mismatch, meaning the region rotated out from under the pointer, or
    /// a checksum failure.
    reads_stale: metrics::Counter,
    /// `sundog_spill_reads_total{cache,outcome="io_error"}`: the positional
    /// read itself failed, or the bytes it returned failed to postcard-decode
    /// as `V`.
    reads_io_error: metrics::Counter,
    /// `sundog_spill_promotions_total{cache}`.
    promotions: metrics::Counter,
    /// `sundog_spill_read_duration_seconds{cache}`: every disk read
    /// `sundog_spill_reads_total` counts. A read of its own (a `get`, a
    /// loader's spilled-key read, a re-arm by `expire`, `touch` or
    /// `persist`) is timed from its wait for a permit to its outcome; a read of [`read_spilled_batch`]'s (anti-entropy, snapshots,
    /// answering a peer's fetch) from its positional read to its outcome. A
    /// conflict resolution's disk read is neither counted nor timed.
    read_duration: metrics::Histogram,
}

#[cfg(feature = "spill")]
impl SpillRead {
    /// Counts one disk read's `outcome` and records its duration since
    /// `started`.
    fn finish(&self, outcome: &metrics::Counter, started: std::time::Instant) {
        outcome.increment(1);
        self.read_duration.record(started.elapsed());
    }
}

#[allow(
    clippy::unused_async,
    clippy::unused_async_trait_impl,
    reason = "most methods here keep async fn even though this backend never awaits"
)]
impl<K, V> Shard<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// The inbound-apply guard, applied to a batch before decoding: keeps
    /// the records this node's current view owns, forwards the ones for a
    /// bucket mid disown-grace on to its current owners, and drops the rest.
    fn guard_inbound(&self, recs: Vec<WireRecord>) -> Vec<WireRecord> {
        match self.ownership.as_ref().map(OwnershipTracker::current) {
            Some(view) => {
                let (owned, unowned): (Vec<_>, Vec<_>) = recs
                    .into_iter()
                    .partition(|rec| view.owns(PartId::of_key(rec.key.as_ref())));
                let (redirected, dropped): (Vec<_>, Vec<_>) =
                    unowned.into_iter().partition(|rec| {
                        self.residency
                            .as_ref()
                            .is_some_and(|r| r.is_releasing(PartId::of_key(rec.key.as_ref())))
                    });
                if !redirected.is_empty() {
                    metrics::counter!(
                        "sundog_forwarded_writes_total",
                        "cache" => self.name.to_string()
                    )
                    .increment(u64::try_from(redirected.len()).unwrap_or(u64::MAX));
                    tracing::debug!(
                        cache = %self.name,
                        redirected = redirected.len(),
                        "apply_remote_batch: forwarded records for a releasing bucket on \
                         to its current owners"
                    );
                    // A refusal means the cache is closing: nothing
                    // forwards from a node on its way out.
                    let _ = self
                        .fan_out
                        .extend(redirected.into_iter().map(FanOutItem::Forward));
                }
                if !dropped.is_empty() {
                    metrics::counter!(
                        "sundog_unowned_inbound_dropped_total",
                        "cache" => self.name.to_string()
                    )
                    .increment(u64::try_from(dropped.len()).unwrap_or(u64::MAX));
                    tracing::debug!(
                        cache = %self.name,
                        dropped = dropped.len(),
                        "apply_remote_batch: dropped records for a bucket this node does \
                         not own"
                    );
                }
                owned
            }
            None => recs,
        }
    }

    /// This shard's current ownership view and residency set together, for
    /// the outbound-serving guard and `ae_peer_filter`: `None` for every
    /// mode but `Mode::Distributed`. Both are cheap `Arc` clones, so a
    /// caller in an async block that must own its captures (`snapshot_chunks`)
    /// and one that only borrows for the call's duration use the same
    /// accessor.
    fn residency_check(&self) -> Option<(Arc<OwnershipView>, Arc<ResidencySet>)> {
        match (&self.ownership, &self.residency) {
            (Some(tracker), Some(residency)) => Some((tracker.current(), Arc::clone(residency))),
            _ => None,
        }
    }

    /// Whether `part` is currently resident for outbound anti-entropy or
    /// snapshot serving: owned, or mid disown-grace. `is_resident = owns ||
    /// residency.is_releasing`, used only by the outbound-serving guard and
    /// the donor-serving exception, never by the inbound-apply guard,
    /// which stays strict current-view ownership.
    fn is_resident_part(residency: &(Arc<OwnershipView>, Arc<ResidencySet>), part: PartId) -> bool {
        let (view, res) = residency;
        verdict::resident(view.owns(part), || res.is_releasing(part))
    }

    /// Whether any part of `bucket` is resident; see
    /// [`Shard::is_resident_part`].
    fn is_resident_bucket(
        residency: &(Arc<OwnershipView>, Arc<ResidencySet>),
        bucket: u16,
    ) -> bool {
        let (view, res) = residency;
        view.owns_any_in_bucket(bucket) || res.releasing_in_bucket(bucket)
    }

    /// [`Shard::is_resident_bucket`] for every bucket at once, indexed by
    /// bucket: one pass over the owned and releasing parts instead of one
    /// lookup per part.
    fn resident_bucket_mask(residency: &(Arc<OwnershipView>, Arc<ResidencySet>)) -> Vec<bool> {
        let (view, res) = residency;
        let mut mask = view.owned_bucket_mask().to_vec();
        for part in res.releasing_parts() {
            mask[usize::from(part.bucket())] = true;
        }
        mask
    }

    /// The parts one anti-entropy round with `peer` covers: every part
    /// both self and `peer` own under the current view, plus every part
    /// mid disown-grace here that `peer` owns, the backstop for a lost
    /// rebalance pull. Ascending, each part once.
    fn peer_scope(
        residency: &(Arc<OwnershipView>, Arc<ResidencySet>),
        peer: NodeId,
    ) -> Vec<PartId> {
        let (view, releasing) = residency;
        let mut scope = crate::ownership::shared_owned_parts(view, peer);
        scope.extend(
            releasing
                .releasing_parts()
                .into_iter()
                .filter(|&part| view.owners_of(part).contains(&peer)),
        );
        scope.sort_unstable();
        scope.dedup();
        scope
    }

    /// The bucket digest of every bucket [`Shard::peer_scope`] touches.
    fn bucket_digests_for(
        &self,
        residency: &(Arc<OwnershipView>, Arc<ResidencySet>),
        peer: NodeId,
    ) -> Vec<BucketDigest> {
        let scope: HashSet<u16> = Self::peer_scope(residency, peer)
            .into_iter()
            .map(PartId::bucket)
            .collect();
        self.engine
            .digests()
            .into_iter()
            .filter(|(bucket, _)| scope.contains(bucket))
            .map(|(bucket, digest)| BucketDigest { bucket, digest })
            .collect()
    }

    /// `parts` gathered by bucket, ascending, each bucket's parts as a mask
    /// and their digests folded: one engine read per bucket.
    fn masked_digests(&self, parts: &[PartId]) -> Vec<MaskedDigest> {
        let mut masks: BTreeMap<u16, PartMask> = BTreeMap::new();
        for part in parts {
            masks.entry(part.bucket()).or_default().insert(part.part());
        }
        masks
            .into_iter()
            .map(|(bucket, mask)| MaskedDigest {
                bucket,
                mask,
                digest: mask.fold(&self.engine.part_digests(bucket)),
            })
            .collect()
    }

    /// Dedups and sorts a bulk request list, ascending.
    fn dedup_sorted<T: Ord>(mut items: Vec<T>) -> Vec<T> {
        items.sort_unstable();
        items.dedup();
        items
    }

    /// Runs `compute` for a resident `bucket`, else substitutes `absent`
    /// unrun. `mask` is [`Shard::resident_bucket_mask`]'s, `None` for a
    /// shard with no residency guard.
    fn gated<R>(
        mask: Option<&[bool]>,
        bucket: u16,
        compute: impl FnOnce() -> R,
        absent: R,
    ) -> (u16, R) {
        let resident =
            mask.is_none_or(|mask| mask.get(usize::from(bucket)).copied().unwrap_or(false));
        let value = if resident { compute() } else { absent };
        (bucket, value)
    }

    /// Dedups and sorts `buckets`, then [`Self::gated`]-maps each one; the shared shape of `bucket_lens` and `part_digests`.
    fn gated_map<R: Clone>(
        &self,
        buckets: Vec<u16>,
        compute: impl Fn(u16) -> R,
        absent: R,
    ) -> Vec<(u16, R)> {
        let mask = self
            .residency_check()
            .map(|residency| Self::resident_bucket_mask(&residency));
        Self::dedup_sorted(buckets)
            .into_iter()
            .map(|b| Self::gated(mask.as_deref(), b, || compute(b), absent.clone()))
            .collect()
    }

    /// The residency-gated backfill shared by `entries_for_buckets` and `entries_for_parts`.
    fn resident_backfill<T: Ord + Copy>(
        &self,
        wanted: Vec<T>,
        is_resident: impl Fn(&(Arc<OwnershipView>, Arc<ResidencySet>), T) -> bool,
        query: impl FnOnce(Vec<T>) -> Vec<(T, Vec<KeyVersion>)>,
    ) -> Vec<(T, Vec<KeyVersion>)> {
        let wanted = Self::dedup_sorted(wanted);
        match self.residency_check() {
            Some(residency) => {
                let (resident, unresident): (Vec<T>, Vec<T>) = wanted
                    .into_iter()
                    .partition(|&t| is_resident(&residency, t));
                let mut entries = query(resident);
                entries.extend(unresident.into_iter().map(|t| (t, Vec::new())));
                entries.sort_unstable_by_key(|&(t, _)| t);
                entries
            }
            None => query(wanted),
        }
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

/// Reads a batch of currently-spilled pointers off-lock, behind a single
/// semaphore permit for the whole batch rather than one per key.
/// [`ShardOps::records_for`], the AE-pull-reply path, and
/// [`ShardOps::snapshot_chunks`] both fold spilled entries in this way. A
/// pointer read that fails, whether from a stale generation, a checksum
/// mismatch, or a genuine I/O error, is dropped like an absent key;
/// nothing here promotes. Returns `Vec::new()` with no read attempted if
/// this shard never attached a tier.
#[cfg(feature = "spill")]
async fn read_spilled_batch<K, V>(
    engine: &engine::Engine<K, V>,
    spill_read: Option<&SpillRead>,
    cache_name: &str,
    spilled: Vec<engine::SpilledPointer>,
) -> Vec<WireRecord>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let Some(spill_read) = spill_read else {
        return Vec::new();
    };
    let Some(tier) = engine.spill() else {
        return Vec::new();
    };
    let tier = Arc::clone(tier);
    let Ok(permit) = Arc::clone(&spill_read.semaphore).acquire_owned().await else {
        return Vec::new();
    };
    let spill_read = spill_read.clone();
    let cache_name = cache_name.to_string();
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let mut out = Vec::with_capacity(spilled.len());
        for (key_bytes, ver, expires_at_ms, loc) in spilled {
            let started = std::time::Instant::now();
            match tier.read_at(loc) {
                Ok(Some(bytes)) => {
                    spill_read.finish(&spill_read.reads_hit, started);
                    out.push(WireRecord {
                        key: key_bytes,
                        value: Some(bytes.encoded),
                        ver,
                        expires_at_ms,
                    });
                }
                Ok(None) => {
                    spill_read.finish(&spill_read.reads_stale, started);
                }
                Err(err) => {
                    tracing::warn!(
                        cache = %cache_name,
                        error = %err,
                        "sundog spill: anti-entropy/snapshot read failed"
                    );
                    spill_read.finish(&spill_read.reads_io_error, started);
                }
            }
        }
        out
    })
    .await
    .unwrap_or_default()
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
            // The inbound-apply guard: a `Mode::Distributed` shard never
            // applies a record for a bucket its own *current* view does not
            // own: strict current-view ownership, before decoding, is the
            // single choke point every inbound record path (live
            // replication, an anti-entropy pull reply, a rebalance transfer
            // chunk) funnels through. A record for a bucket this node is
            // mid disown-grace on came from a sender whose view still lists
            // this node as an owner; this node's view is the newer one, so
            // the record is forwarded on to the bucket's current owners,
            // never applied here and never dropped. A record for a bucket
            // this node never held is dropped and counted.
            let recs = self.guard_inbound(recs);
            let mut decoded: Vec<RemoteEntry<K, V>> = Vec::with_capacity(recs.len());
            for rec in recs {
                if !self.observe_remote(rec.ver) {
                    continue;
                }
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
                decoded.push((hash, key, rec.key, rec.ver, incoming, origin));
            }

            // One reservation for the whole decoded batch (see
            // `Shard::reserve_for_batch`) throttles replication, anti-entropy
            // repair, and state-transfer apply alike through this one method.
            let (reservation, deadline) = self
                .reserve_for_batch(
                    decoded
                        .iter()
                        .map(|(_, _, key_bytes, _, incoming, _)| (key_bytes, incoming)),
                )
                .await;

            // Groups by raw key-byte stripe, no decode needed; order within
            // a stripe is preserved.
            let mut by_stripe: Vec<Vec<RemoteEntry<K, V>>> =
                (0..BUCKET_COUNT).map(|_| Vec::new()).collect();
            for entry in decoded {
                by_stripe[engine::stripe_index_from_hash(entry.0)].push(entry);
            }
            let now = self.now_ms();
            let mut written = engine::Written::default();
            // Scoped as in `Shard::apply_grouped`: the reservation is back
            // with the tier before the retry asks for a fresh one.
            let deficit = {
                let mut reservation = reservation;
                let mut deficit = 0u64;
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
                    let (outcomes, bucket_deficit) = self.engine.apply_many_with_reservation(
                        bucket,
                        entries,
                        self.resolver.as_ref(),
                        self.tombstone_ttl_ms,
                        self.tombstone_max_ttl_ms,
                        now,
                        reservation.as_mut(),
                        &mut written,
                    );
                    deficit += bucket_deficit;
                    for (outcome, origin) in outcomes.into_iter().zip(origins) {
                        self.handle_apply_outcome(outcome, origin, true);
                    }
                }
                deficit
            };
            self.retry_reservation_deficit(0, deficit, deadline, &mut written)
                .await;
        })
    }

    fn invalidate(&self, key: Bytes, ver: Hlc) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            if !self.observe_remote(ver) {
                return;
            }
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

    #[allow(private_interfaces, reason = "see ShardOps::serve_load")]
    fn serve_load(&self, keys: Vec<Bytes>) -> BoxFuture<'_, crate::net::LoadServe> {
        Box::pin(self.serve_load_encoded(keys))
    }

    fn refresh_hint(&self, key: Bytes, ver: Hlc) -> BoxFuture<'_, ()> {
        self.take_refresh_hint(&key, ver);
        Box::pin(async {})
    }

    fn digests(&self) -> BoxFuture<'_, Vec<BucketDigest>> {
        let digests = self.engine.digests();
        // The outbound-serving guard: a `Mode::Distributed` shard never
        // reports a bucket it neither owns nor is releasing.
        let digests = match self.residency_check() {
            Some(residency) => {
                let mask = Self::resident_bucket_mask(&residency);
                digests
                    .into_iter()
                    .filter(|&(bucket, _)| mask[usize::from(bucket) % BUCKET_COUNT])
                    .collect()
            }
            None => digests,
        };
        let digests = digests
            .into_iter()
            .map(|(bucket, digest)| BucketDigest { bucket, digest })
            .collect();
        Box::pin(async move { digests })
    }

    fn ae_digests_for(&self, peer: NodeId) -> BoxFuture<'_, Vec<BucketDigest>> {
        let Some(residency) = self.residency_check() else {
            return self.digests();
        };
        let digests = self.bucket_digests_for(&residency, peer);
        Box::pin(async move { digests })
    }

    #[allow(private_interfaces, reason = "see the trait method's own allow")]
    fn ae_scoped_digests_for(&self, peer: NodeId) -> BoxFuture<'_, Option<ScopedDigests>> {
        let scoped = self.residency_check().map(|residency| {
            let scope = match residency.0.granularity() {
                Granularity::Bucket => Scope::Buckets(
                    self.bucket_digests_for(&residency, peer)
                        .into_iter()
                        .map(|bd| (bd.bucket, bd.digest))
                        .collect(),
                ),
                Granularity::Part => {
                    Scope::Parts(self.masked_digests(&Self::peer_scope(&residency, peer)))
                }
            };
            ScopedDigests {
                view_hash: residency.0.view_hash(),
                scope,
            }
        });
        Box::pin(async move { scoped })
    }

    fn bucket_entries(&self, bucket: u16) -> BoxFuture<'_, Vec<KeyVersion>> {
        if let Some(residency) = self.residency_check()
            && !Self::is_resident_bucket(&residency, bucket)
        {
            return Box::pin(async { Vec::new() });
        }
        let entries = self
            .engine
            .collect_buckets(&[bucket], self.now_ms())
            .pop()
            .map(|(_, entries)| entries)
            .unwrap_or_default();
        Box::pin(async move { entries })
    }

    fn entries_for_buckets(&self, buckets: Vec<u16>) -> BoxFuture<'_, BucketEntries> {
        // Every requested bucket exactly once, ascending, so the initiator
        // learns to push even for buckets this peer holds nothing in.
        let entries = self.resident_backfill(
            buckets,
            |residency, bucket| Self::is_resident_bucket(residency, bucket),
            |resident| self.engine.collect_buckets(&resident, self.now_ms()),
        );
        Box::pin(async move { entries })
    }

    fn bucket_lens(&self, buckets: Vec<u16>) -> BoxFuture<'_, Vec<BucketLen>> {
        let out = self.gated_map(buckets, |b| self.engine.bucket_len(b), 0);
        let out = out
            .into_iter()
            .map(|(bucket, len)| BucketLen { bucket, len })
            .collect();
        Box::pin(async move { out })
    }

    fn held_buckets(&self) -> BoxFuture<'_, Vec<u16>> {
        let held: Vec<u16> = (0..BUCKET_COUNT)
            .map(|b| u16::try_from(b).expect("invariant: BUCKET_COUNT fits u16"))
            .filter(|&b| self.engine.bucket_len(b) > 0)
            .collect();
        Box::pin(async move { held })
    }

    fn held_parts(&self) -> BoxFuture<'_, Vec<PartId>> {
        let held = self.engine.held_parts();
        Box::pin(async move { held })
    }

    fn part_digests(&self, buckets: Vec<u16>) -> BoxFuture<'_, Vec<BucketPartDigests>> {
        let out = self.gated_map(buckets, |b| self.engine.part_digests(b), Vec::new());
        let out = out
            .into_iter()
            .map(|(bucket, digests)| BucketPartDigests { bucket, digests })
            .collect();
        Box::pin(async move { out })
    }

    fn entries_for_parts(&self, parts: Vec<BucketPart>) -> BoxFuture<'_, PartEntries> {
        let entries = self.resident_backfill(
            parts,
            |residency, part: BucketPart| Self::is_resident_part(residency, PartId::from(part)),
            |resident| self.engine.collect_parts(&resident, self.now_ms()),
        );
        Box::pin(async move { entries })
    }

    /// A spilled entry's value is read off-lock behind the tier's read
    /// semaphore, batched into one `spawn_blocking` call, and dropped if the
    /// read fails. It is never promoted: an anti-entropy pull reply covers
    /// many keys, and reinstalling every one of them on every round would
    /// repopulate RAM as fast as eviction drains it.
    fn records_for(&self, keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>> {
        let now = self.now_ms();
        // The outbound-serving guard, at the key level: a key whose bucket
        // this shard neither owns nor is releasing is dropped from the
        // request before it ever reaches `engine`.
        let keys = match self.residency_check() {
            Some(residency) => keys
                .into_iter()
                .filter(|k| Self::is_resident_part(&residency, PartId::of_key(k.as_ref())))
                .collect(),
            None => keys,
        };
        #[cfg(feature = "spill")]
        {
            let (records, spilled) = self.engine.records_for_or_spilled(&keys, now);
            Box::pin(async move {
                let mut records = records;
                if !spilled.is_empty() {
                    records.extend(
                        read_spilled_batch(
                            self.engine.as_ref(),
                            self.spill_read.get(),
                            &self.name,
                            spilled,
                        )
                        .await,
                    );
                }
                records
            })
        }
        #[cfg(not(feature = "spill"))]
        {
            let recs = keys
                .into_iter()
                .filter_map(|key_bytes| self.engine.record_for(key_bytes.as_ref(), now))
                .collect();
            Box::pin(async move { recs })
        }
    }

    /// [`ShardOps::records_for`]'s note applies here too: a spilled entry is
    /// read off-lock and folded in, never promoted.
    fn snapshot_chunks(&self) -> BoxStream<'static, Vec<WireRecord>> {
        let engine = Arc::clone(&self.engine);
        let now = self.now_ms();
        let residency = self.residency_check();
        #[cfg(feature = "spill")]
        let spill_read = self.spill_read.get().cloned();
        #[cfg(feature = "spill")]
        let name = self.name.to_string();
        let fut = async move {
            let records = engine.snapshot_records(now);
            #[cfg(feature = "spill")]
            let records = {
                let spilled = engine.snapshot_spilled(now);
                let mut records = records;
                if !spilled.is_empty() {
                    records.extend(
                        read_spilled_batch(engine.as_ref(), spill_read.as_ref(), &name, spilled)
                            .await,
                    );
                }
                records
            };
            // The outbound-serving guard, over a full snapshot's records.
            let records = match residency {
                Some(residency) => records
                    .into_iter()
                    .filter(|rec| {
                        Self::is_resident_part(&residency, PartId::of_key(rec.key.as_ref()))
                    })
                    .collect(),
                None => records,
            };
            chunk_records_for_snapshot(records)
        };
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

    /// All the work here is synchronous: `Engine::compact` never awaits,
    /// nor does re-applying its candidates, so, like
    /// [`ShardOps::gc_tombstones`]/[`ShardOps::run_pending_tasks`] above,
    /// it all runs before the trivial future is built rather than inside
    /// an `async move` block: `retire`'s borrow only needs to outlive this
    /// call, not whatever lifetime the returned `BoxFuture<'_, _>` (tied
    /// to `&self`, per this trait method's elided signature) would force
    /// on anything captured into it.
    fn compact_pass(
        &self,
        now_ms: u64,
        retire: &dyn Fn(crdt::WriterId) -> bool,
        quiet: Quiescence,
        bounds: CompactionBounds,
        max_entries: usize,
    ) -> BoxFuture<'_, CompactPassOutcome> {
        if !self.resolver.merges() {
            // `ConflictResolver::compact`'s own contract: "only ever
            // called while `merges()` is `true`". Enforced here rather
            // than left to `Engine::compact`'s caller to remember, so a
            // non-merging shard (`LwwResolver` and every other
            // pick-a-side resolver, the common case) never pays for a
            // full-engine scan whose every call would return `None`
            // anyway, and the contract holds regardless of what a future
            // caller of this method does or doesn't check first.
            return Box::pin(async {
                CompactPassOutcome {
                    retired: Vec::new(),
                    compacted: 0,
                    stripes_visited: BUCKET_COUNT,
                }
            });
        }
        // `Engine::compact` takes `retire` as `&dyn Fn`, not `FnMut`, so
        // recording which writers it accepted (as opposed to every writer
        // merely passed to it) goes through interior mutability
        // rather than a captured `&mut`. A resolver's own `compact` only
        // ever calls `retire` on a writer identity the record still holds
        // a *live* slot for (never one already folded away by a prior
        // pass), and a `true` answer always turns that record into a
        // candidate (`newly` becomes non-empty), so every writer captured
        // here belongs to some candidate this scan found, deduplicated by
        // the `HashSet`, since the same eligible writer can surface across
        // more than one candidate record in a single pass.
        //
        // This is scoped to the whole scan, not to the handful of
        // candidates that go on to apply below: a candidate for a bucket
        // this shard's *current* view does not own anymore is skipped,
        // never applied, yet the writer(s) it would have retired are
        // still reported here. This is deliberate, not an oversight: the
        // alternative (only ever reporting a writer once every one of its
        // live records anywhere in the cluster is compacted) is
        // unobtainable at this layer, since nothing this generic-over-`V`
        // method sees distinguishes "a writer's slot" from arbitrary
        // bytes. Reporting it anyway is safe in the direction that
        // matters: the caller (`crdt_compact_tick`) only counts it in
        // `sundog_crdt_retired_writers_total`, never deletes data on the
        // strength of it, and the member stays tracked gone until it
        // returns, so a skipped record is retried on the next pass: the
        // skipped bucket's own stale copy is either physically dropped by
        // `ShardOps::release_buckets` once its disown-grace period ends
        // (the common case: ownership rarely moves back), or, if this
        // shard reacquires the bucket first, recompacted on this shard's
        // own next pass. The metric therefore counts writers found
        // eligible in the scan, which can exceed the records rewritten.
        let newly_retired: StdMutex<HashSet<crdt::WriterId>> = StdMutex::new(HashSet::new());
        let tracking_retire = |writer: crdt::WriterId| {
            let accepted = retire(writer);
            if accepted {
                newly_retired
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(writer);
            }
            accepted
        };
        let engine::CompactScan {
            candidates,
            stripes_visited,
        } = self.engine.compact(
            self.resolver.as_ref(),
            now_ms,
            &tracking_retire,
            quiet,
            bounds,
            max_entries,
        );
        let mut compacted = 0usize;
        for engine::CompactCandidate {
            key,
            key_bytes,
            stale_ver,
            new_encoded,
        } in candidates
        {
            // Mirrors `guard_inbound`'s strict current-view rule: a
            // `Mode::Distributed` shard mid disown-grace never re-applies
            // a compacted record for a bucket its *current* view does not
            // own anymore. Skipped, never forwarded: unlike an ordinary
            // write, a compaction candidate has no caller waiting on it,
            // and the bucket's new owner will compact its own copy on its
            // own schedule once it, too, decides the same writers are
            // dead.
            if !self.owns_key(key_bytes.as_ref()) {
                continue;
            }
            let Ok(value) = postcard::from_bytes::<V>(&new_encoded) else {
                tracing::warn!(
                    cache = %self.name,
                    "compact_pass: a resolver's own compacted bytes failed to decode as V"
                );
                continue;
            };
            // A version-gated direct replace, never the ordinary
            // merge-based apply path: see `Engine::compact_replace_if_current`'s
            // own doc for why a merge-based apply here would silently
            // undo stage three's own pruning on every single pass. `false`
            // means something else touched this entry since the scan
            // above read it (or it is not live anymore, or not
            // resident anymore): skipped, exactly like an unowned bucket
            // above, and recomputed fresh on the next pass rather than
            // counted here.
            let hash = engine::hash_key_bytes(key_bytes.as_ref());
            let new_ver = compacted_version(stale_ver, new_encoded.as_ref());
            self.observe_derived(new_ver);
            if self
                .engine
                .compact_replace_if_current(engine::CompactReplace {
                    key: &key,
                    key_bytes: key_bytes.as_ref(),
                    hash,
                    expected_ver: stale_ver,
                    new_ver,
                    value,
                    encoded: new_encoded,
                })
            {
                compacted += 1;
            }
        }
        let retired: Vec<crdt::WriterId> = newly_retired
            .into_inner()
            .unwrap_or_else(PoisonError::into_inner)
            .into_iter()
            .collect();
        Box::pin(async move {
            CompactPassOutcome {
                retired,
                compacted,
                stripes_visited,
            }
        })
    }

    fn close_spill(&self) {
        Shard::close_spill(self);
    }

    fn close_spill_checkpointed(&self) -> BoxFuture<'_, ()> {
        Box::pin(Shard::close_spill_checkpointed(self))
    }

    fn seal_fan_out(&self) {
        self.fan_out.seal();
    }

    #[allow(
        private_interfaces,
        reason = "OwnershipView has no public fields or methods; only this crate can act on the returned value regardless of this trait's own visibility"
    )]
    fn ownership_view(&self) -> Option<Arc<OwnershipView>> {
        self.ownership.as_ref().map(OwnershipTracker::current)
    }

    fn ownership_view_hash(&self) -> Option<u64> {
        self.ownership
            .as_ref()
            .map(|tracker| tracker.current().view_hash())
    }

    fn release_buckets(&self, buckets: &[u16]) -> BoxFuture<'_, u64> {
        let removed = self.engine.release_buckets(buckets);
        Box::pin(async move { removed })
    }

    fn release_parts(&self, parts: &[PartId]) -> BoxFuture<'_, u64> {
        let removed = self.engine.release_parts(parts);
        Box::pin(async move { removed })
    }

    // `Residency::cold` is the same rule over one read of every mark; this
    // reads only the marks it needs, for the per-part loops that call it.
    fn is_cold_part(&self, part: PartId) -> bool {
        self.residency.as_ref().is_some_and(|residency| {
            residency.is_cold(part)
                || (residency.is_unsettled(part)
                    && self
                        .ownership
                        .as_ref()
                        .is_some_and(|tracker| tracker.current().owns(part)))
        })
    }

    // `Residency::distrusted` is the same rule over one read of every mark.
    fn is_unverified_part(&self, part: PartId) -> bool {
        self.residency
            .as_ref()
            .is_some_and(|residency| residency.is_unverified(part))
    }

    fn is_cold_bucket(&self, bucket: u16) -> bool {
        PartId::of_bucket(bucket).any(|part| self.is_cold_part(part))
    }

    fn is_unverified_bucket(&self, bucket: u16) -> bool {
        PartId::of_bucket(bucket).any(|part| self.is_unverified_part(part))
    }

    fn ae_peer_filter(&self, dirty: Vec<NodeId>, live: Vec<NodeId>) -> (Vec<NodeId>, Vec<NodeId>) {
        let (Some(tracker), Some(residency)) = (self.ownership.as_ref(), self.residency.as_ref())
        else {
            return (dirty, live);
        };
        let view = tracker.current();
        let mut cohort: HashSet<NodeId> = view.co_owners().iter().copied().collect();
        for part in residency.releasing_parts() {
            cohort.extend(view.owners_of(part).iter().copied());
        }
        let filter = |nodes: Vec<NodeId>| -> Vec<NodeId> {
            nodes.into_iter().filter(|n| cohort.contains(n)).collect()
        };
        (filter(dirty), filter(live))
    }

    fn merges(&self) -> bool {
        self.resolver.merges()
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) mod read_cases;
#[cfg(test)]
pub(crate) mod test_support;

#[cfg(test)]
mod prop_tests;

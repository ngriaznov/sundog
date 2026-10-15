//! Typed shards over the store engine: versioned apply, conflict resolution,
//! tombstones, and the per-bucket digests anti-entropy compares.
//!
//! A `Shard` holds no network handle. Every local write pushes its key onto the
//! shard's fan-out queue and publishes an `Origin::Local` [`Event`]; the
//! cluster layer turns those into wire traffic.

use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
#[cfg(feature = "spill")]
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
use std::sync::{Arc, Mutex as StdMutex, RwLock as StdRwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
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
use crate::ownership::{OwnershipTracker, ResidencySet};
use crate::wire::{self, MAX_FRAME, WireRecord};

mod engine;
mod fan_out;
mod load;
pub(crate) mod part;
mod read_timing;
mod refresh;
mod resolver;
mod shard_build;
mod shard_ignored;
mod shard_merge;
mod shard_ops;
mod shard_read;
mod shard_rearm;
mod shard_serve;
mod shard_skew;
mod shard_spill;
mod shard_write;
mod skew_hold;
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
#[cfg(feature = "spill")]
use fan_out::spilled_needs_read;
pub(crate) use fan_out::{
    FanOutItem, FanOutQueue, FanOutRecords, ReadThroughRoute, newest_per_key,
};
use fan_out::{fan_out_ships_values, partition_writes};
pub(crate) use load::Sourced;
pub use load::{DEFAULT_MAX_BATCH_KEYS, LoadError, Loader};
pub use part::PartId;
pub(crate) use part::PartMask;
pub(crate) use refresh::{RefreshRequest, refresh_permille};
pub use resolver::{ConflictResolver, LwwResolver, Merged, RecordView, Winner, later_deadline};
#[cfg(doc)]
use shard_build::SettlingResolver;
#[cfg(doc)]
use shard_merge::drain_due_deadlines;
pub use shard_ops::ShardOps;
pub(crate) use shard_ops::{Scope, ScopedDigests};
#[cfg(all(doc, feature = "spill"))]
use shard_serve::read_spilled_batch;
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

/// A tombstone: the version it stands for, the two GC deadlines it is kept
/// under, and, for one an expired entry left, the expiry it was anchored on.
/// `ttl_deadline_ms` is when it becomes eligible for ordinary collection;
/// `max_deadline_ms` is the hard cap past which it collects regardless of
/// member absence. See [`ShardOps::gc_tombstones`].
///
/// Every tombstone counts its retention from an instant every replica
/// agrees on, capped at the receiving node's clock: the delete's own version
/// stamp (`ver.wall_ms`) for a delete, the expiry for the tombstone of an
/// entry that expired (`expired_at_ms` is `Some`; the sweep makes one in a
/// replicated mode, at that entry's version). Every replica that learns of
/// a tombstone derives the same deadlines, whenever it learns, so they
/// collect it on the same gc tick. A tombstone that arrives from a peer
/// already past its retention is ignored. See [`ShardOps::run_pending_tasks`].
#[derive(Debug, Clone, Copy)]
struct Tombstone {
    ver: Hlc,
    ttl_deadline_ms: u64,
    max_deadline_ms: u64,
    /// The expiry an expired entry's tombstone is anchored on, carried on
    /// the wire as the record's `expires_at_ms`; `None` for a delete.
    expired_at_ms: Option<u64>,
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
    /// A deletion marker. `expired_at_ms` is `Some` for the tombstone of an
    /// entry that expired, whose retention counts from that expiry; a
    /// delete's counts from its version stamp.
    Tombstone { expired_at_ms: Option<u64> },
}

impl<V> Incoming<V> {
    /// A delete's marker: kept for a retention from the delete's version stamp.
    const DELETE: Self = Self::Tombstone {
        expired_at_ms: None,
    };
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
/// through, which folds it again against whatever is stored. The flush
/// applies it at [`Self::ver`], so the fold orders against every other write
/// as its last call did; a tombstone newer than that version discards it
/// instead.
struct PendingMerge<V> {
    /// `K`'s encoded bytes, kept so a flush never re-encodes the key.
    key_bytes: Bytes,
    value: V,
    /// `value`'s postcard-encoded bytes, kept alongside `value` for the same
    /// reason [`Incoming::Put::encoded`] is: a fold to fold again can reuse
    /// them without a re-encode.
    encoded: Bytes,
    expires_at_ms: Option<u64>,
    /// The newest version stamped by a `merge` call folded into this entry:
    /// the "incoming" side of the next fold's [`RecordView`], and the
    /// version the flush applies the fold at. The folded content is applied
    /// as of its last call, so a remove, clear or tombstone stamped between
    /// the call and the flush outvotes it exactly as it outvotes a write
    /// with no window.
    ver: Hlc,
    /// The `by_deadline` row this entry owns, so a discard removes its row
    /// and a later fold for the same key never flushes early on a stale one.
    deadline_key: (u64, u64),
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

impl<K: Hash + Eq, V> PendingMergeStripe<K, V> {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            by_deadline: BTreeMap::new(),
            next_seq: 0,
        }
    }

    /// Takes `key`'s pending fold out, with its deadline row.
    fn remove(&mut self, key: &K) -> Option<PendingMerge<V>> {
        let entry = self.entries.remove(key)?;
        self.by_deadline.remove(&entry.deadline_key);
        Some(entry)
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
    /// Forwarded records [`Shard::observe_remote`] refused as stamped past
    /// `max_clock_skew_ms`, parked until this node's clock is within the
    /// bound of their stamps; see `shard_skew.rs`. Never held across an
    /// `.await`.
    skew_hold: StdMutex<skew_hold::SkewHold>,
    /// Notified whenever a record is held, so the release task recomputes
    /// its sleep (mirrors `merge_wake`).
    skew_wake: Notify,
    /// Set once the first forwarded record dropped instead of held has been
    /// logged at `warn`; later drops log at `debug` and only count.
    skew_drop_warned: AtomicBool,
    /// The clock reading of the last warning about forwarded tombstones
    /// ignored as lapsed, `u64::MAX` before the first; see `shard_ignored.rs`.
    ignored_forward_warned_ms: AtomicU64,
    /// Runs the spill tier's checkpointed close once. The flag is true once
    /// a close finished; the lock is held for the whole checkpoint by the
    /// task running it, so later and concurrent closers, and one that
    /// follows a dropped first caller, wait for that run and find it done.
    spill_close: Arc<tokio::sync::Mutex<bool>>,
    /// Test-only: makes the next checkpointed close's task panic before it
    /// starts, so a test sees the close left undone and the next one run it.
    #[cfg(all(test, feature = "spill"))]
    spill_checkpoint_panic: AtomicBool,
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
    /// The number of entries across [`Self::pending_merges`], kept exact
    /// under the stripe locks. A tombstone checks it before it encodes or
    /// locks anything, so a shard with nothing pending pays one load.
    pending_merge_count: AtomicUsize,
    /// Held shared by every path that takes a pending fold out of its stripe,
    /// from the take through the last apply of what it took
    /// ([`Shard::begin_flush`]), and exclusively by [`Shard::clear`] around its
    /// split of the pending keys. A key a flush has taken is then in neither
    /// the stripes nor the engine for no longer than the flush, and the split
    /// waits it out. Only held in synchronous sections, never across an await.
    merge_flush_gate: StdRwLock<()>,
    /// Test-only: runs between a flush's take and its apply, so a test can
    /// park the flush there.
    #[cfg(test)]
    after_take_hook: StdMutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    /// Notified whenever [`Shard::merge`] opens a fresh coalescing window
    /// (a key with nothing already pending), so
    /// `crate::cache::tasks::merge_coalesce_task`'s sweep can recompute its
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

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) mod read_cases;
#[cfg(test)]
pub(crate) mod test_support;

#[cfg(test)]
mod prop_regressions;
#[cfg(test)]
mod prop_tests;

//! Typed shards over the store engine: versioned apply, conflict resolution,
//! tombstones, and the per-bucket digests anti-entropy compares.
//!
//! A `Shard` holds no network handle. Every local write pushes its key onto the
//! shard's fan-out queue and publishes an `Origin::Local` [`Event`]; the
//! cluster layer turns those into wire traffic.

use std::collections::hash_map::{Entry, OccupiedEntry};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::hash::Hash;
#[cfg(feature = "spill")]
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
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

use crate::config::ClusterConfig;
use crate::error::{CacheError, CodecError};
use crate::explain::{LocalRead, LocalRecord, Residency};
use crate::hlc::{Hlc, HlcClock};
use crate::net::REPLICATE_BATCH_COUNT;
use crate::node::NodeId;
use crate::ownership::{Granularity, OwnershipTracker, OwnershipView, ResidencySet};
use crate::verdict;
use crate::wire::{self, MAX_FRAME, WireRecord};

mod engine;
mod load;
pub(crate) mod part;
mod read_timing;
mod refresh;
mod resolver;
mod types;
use engine::{ApplyOutcome, Engine, JoinOutcome, Reservation};

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
pub(crate) use load::Sourced;
pub use load::{DEFAULT_MAX_BATCH_KEYS, LoadError, Loader};
pub use part::PartId;
pub(crate) use part::PartMask;
pub(crate) use refresh::{RefreshRequest, refresh_permille};
pub use resolver::{ConflictResolver, LwwResolver, Merged, RecordView, Winner};
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

/// Boxes a remembered [`SharedWeigher`] into a fresh [`Weigher`] for
/// `engine::Engine::new`, keeping the `Arc` alive for the next rebuild.
fn boxed_weigher<K, V>(weigher: SharedWeigher<K, V>) -> Weigher<K, V>
where
    K: 'static,
    V: 'static,
{
    Box::new(move |k: &K, v: &V| weigher(k, v))
}

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

/// Whether a clock whose last stamp runs `lead_ms` ahead of the system
/// clock has fallen behind by more than `max_clock_skew_ms` allows. `None`
/// never flags.
fn clock_jumped(lead_ms: u64, max_clock_skew_ms: Option<u64>) -> bool {
    max_clock_skew_ms.is_some_and(|max| lead_ms > max)
}

/// Capacity of each shard's [`Event`] broadcast channel. A subscriber that
/// falls this far behind misses events (`broadcast::error::RecvError::Lagged`)
/// instead of applying backpressure to writers.
const EVENTS_CAPACITY: usize = 1024;

/// Keys this node wrote locally and has not yet fanned out to
/// `cluster::fan_out_task`. A write appends its key; a drain takes the whole
/// backlog at once, so no channel ever drops a write. Holds keys, not values:
/// `records_for_typed` re-fetches fresh wire bytes. A queue nothing drains, a
/// `Mode::Local` shard's or a closed cache's, accepts nothing.
pub(crate) struct FanOutQueue<K> {
    /// This cache's name, used as the `cache` label on the fan-out metrics.
    name: SmolStr,
    pending: StdMutex<Vec<K>>,
    notify: tokio::sync::Notify,
    /// Notified when [`FanOutQueue::drain`] empties a non-empty backlog, so
    /// [`FanOutQueue::wait_for_room`] waiters recheck instead of polling.
    drained: tokio::sync::Notify,
    /// Whether a push lands in `pending` at all: `false` for a
    /// `Mode::Local` shard, whose writes never fan out and are not errors.
    accepting: AtomicBool,
    /// Set by [`FanOutQueue::seal`] and [`FanOutQueue::close`]: the cache is
    /// closing, and a write from here on is refused with
    /// [`CacheError::Closed`] rather than accepted and never sent. Read and
    /// written under `pending`'s lock, so a push and a seal never
    /// interleave: every push that returns `true` lands before the seal's
    /// final drain.
    closed: StdMutex<bool>,
}

impl<K> FanOutQueue<K> {
    fn new(name: SmolStr, accepting: bool) -> Self {
        Self {
            name,
            pending: StdMutex::new(Vec::new()),
            notify: tokio::sync::Notify::new(),
            drained: tokio::sync::Notify::new(),
            accepting: AtomicBool::new(accepting),
            closed: StdMutex::new(false),
        }
    }

    /// Publishes `sundog_fan_out_backlog{cache}` as `len`, the one place
    /// every mutation of `pending` reports its length through.
    fn publish_backlog(&self, len: usize) {
        metrics::gauge!("sundog_fan_out_backlog", "cache" => self.name.to_string())
            .set(fan_out_backlog_gauge_value(len));
    }

    /// Stops accepting writes but keeps the backlog for the fan-out task's
    /// final drain, and wakes that task: the first step of a close or a
    /// cluster shutdown, before the task is cancelled.
    pub(crate) fn seal(&self) {
        let _pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
        *self.closed.lock().unwrap_or_else(PoisonError::into_inner) = true;
        self.notify.notify_one();
    }

    /// Stops accepting keys and drops the backlog: nothing drains this queue
    /// any more.
    pub(crate) fn close(&self) {
        self.accepting.store(false, Ordering::Release);
        let mut pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
        *self.closed.lock().unwrap_or_else(PoisonError::into_inner) = true;
        pending.clear();
        self.publish_backlog(0);
        self.drained.notify_waiters();
    }

    /// Whether a write may still be accepted: `false` once sealed or
    /// closed.
    fn is_open(&self) -> bool {
        !*self.closed.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Queues `key`, or returns `false` once the queue is sealed or closed.
    /// A `Mode::Local` shard's queue accepts nothing and returns `true`.
    #[must_use]
    fn push(&self, key: K) -> bool {
        let mut pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
        if !self.is_open() {
            return false;
        }
        if !self.accepting.load(Ordering::Acquire) {
            return true;
        }
        pending.push(key);
        self.publish_backlog(pending.len());
        self.notify.notify_one();
        true
    }

    /// [`FanOutQueue::push`] for many keys at once.
    #[must_use]
    fn extend(&self, keys: impl IntoIterator<Item = K>) -> bool {
        let mut pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
        if !self.is_open() {
            return false;
        }
        if !self.accepting.load(Ordering::Acquire) {
            return true;
        }
        pending.extend(keys);
        self.publish_backlog(pending.len());
        self.notify.notify_one();
        true
    }

    /// Takes every pending key and wakes [`FanOutQueue::wait_for_room`]'s waiters.
    pub(crate) fn drain(&self) -> Vec<K> {
        let items =
            std::mem::take(&mut *self.pending.lock().unwrap_or_else(PoisonError::into_inner));
        self.publish_backlog(0);
        if !items.is_empty() {
            self.drained.notify_waiters();
        }
        items
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

    /// Waits up to `timeout` for the backlog to fall under `capacity`, giving
    /// an async write path a way to apply its own backpressure. The write
    /// proceeds either way; on timeout,
    /// `sundog_fan_out_wait_timeouts_total{cache}` counts it. The synchronous
    /// write chain has no runtime to await on and never calls this.
    pub(crate) async fn wait_for_room(&self, capacity: usize, timeout: Duration) {
        if !fan_out_over_capacity(self.len(), capacity) {
            return;
        }
        let waited = tokio::time::timeout(timeout, async {
            loop {
                // Armed before the recheck so a `drain` landing between the
                // check and the `.await` is never missed.
                let notified = self.drained.notified();
                if !fan_out_over_capacity(self.len(), capacity) {
                    return;
                }
                notified.await;
            }
        })
        .await;
        if waited.is_err() {
            metrics::counter!(
                "sundog_fan_out_wait_timeouts_total",
                "cache" => self.name.to_string()
            )
            .increment(1);
        }
    }

    fn len(&self) -> usize {
        self.pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}

/// Whether a [`Shard::retry_reservation_deficit`] round proves further
/// rounds futile: it was `granted` the whole flush queue (`whole_queue`
/// permits), spent none of it (`unspent` is still `granted`), and still
/// owes a `deficit`. The victim it stopped at then needs more than the
/// whole queue, which no reservation can ever cover. A round granted less
/// than the whole queue proves nothing: its victim may only need more than
/// that round asked for, and the next round asks for its whole deficit.
/// Pure; unit tested directly.
#[cfg(feature = "spill")]
const fn retry_round_is_futile(granted: u32, whole_queue: u32, unspent: u32, deficit: u64) -> bool {
    deficit > 0 && granted == whole_queue && unspent == granted
}

/// Whether [`FanOutQueue::wait_for_room`] keeps waiting: `true` while
/// `pending_len` is at or above `capacity`.
fn fan_out_over_capacity(pending_len: usize, capacity: usize) -> bool {
    pending_len >= capacity
}

/// `len` as `sundog_fan_out_backlog`'s gauge value.
#[expect(
    clippy::cast_precision_loss,
    reason = "a gauge only needs f64's exact-integer range, up to 2^53, which comfortably \
              covers any realistic fan-out backlog"
)]
fn fan_out_backlog_gauge_value(len: usize) -> f64 {
    len as f64
}

/// One entry in a shard's fan-out queue. `Applied` is every write this
/// shard holds: every mode's own local write, and an owner's write under
/// `Mode::Distributed`. `cluster::fan_out_batch` re-fetches its current
/// record via `Shard::records_for_typed`, unchanged from every other mode.
/// `Forward` is a `Mode::Distributed` non-owner write: the record is never
/// applied, so there is nothing to re-fetch, and the
/// fully built [`WireRecord`] travels with the queue entry itself. One
/// queue, one drain, for every mode: this generalizes [`FanOutQueue`]'s
/// item type rather than adding a second queue and a second task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FanOutItem<K> {
    Applied(K),
    Forward(WireRecord),
}

/// Sim-only mirror of [`FanOutItem`], reachable from `tests/sim.rs` under
/// the `sim` feature since `FanOutItem` itself is not; see
/// [`Shard::drain_fan_out_for_sim`].
#[cfg(feature = "sim")]
#[doc(hidden)]
pub enum SimFanOut<K> {
    /// Mirrors [`FanOutItem::Applied`].
    Applied(K),
    /// Mirrors [`FanOutItem::Forward`].
    Forward(WireRecord),
}

/// Whether a [`Shard::get_or_load`] fill goes out to peers. A fill copies
/// data from its source and changes nothing there. `Replicated` and
/// `Distributed` send it so a peer holding the key skips its own loader
/// call. `Invalidation` sends nothing: an invalidation would drop every
/// peer's copy of the same data, and two nodes reading one key would evict
/// each other on every fill. Pure; unit tested directly.
const fn fill_fans_out(mode: Mode) -> bool {
    !matches!(mode, Mode::Invalidation)
}

/// One inbound [`WireRecord`], decoded down to what
/// [`ShardOps::apply_remote_batch`]'s per-stripe grouping needs: its
/// precomputed hash, typed key, raw key bytes, version, and the value or
/// tombstone it carries, plus which peer (or local call) it came from.
type RemoteEntry<K, V> = (u64, K, Bytes, Hlc, Incoming<V>, Origin);

/// [`ShardOps::ae_scoped_digests_for`]'s answer: one view's hash and what
/// a round under it compares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScopedDigests {
    pub(crate) view_hash: u64,
    pub(crate) scope: Scope,
}

/// What one scoped anti-entropy round compares, at its view's granularity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Scope {
    /// Under a bucket view: each bucket in scope and its digest.
    Buckets(Vec<(u16, u64)>),
    /// Under a part view: each bucket holding a part in scope, those parts,
    /// and their folded digest.
    Parts(Vec<MaskedDigest>),
}

/// One bucket of a [`Scope::Parts`]: the parts in scope and the XOR of
/// their part digests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MaskedDigest {
    pub(crate) bucket: u16,
    pub(crate) mask: PartMask,
    pub(crate) digest: u64,
}

impl Scope {
    /// Each bucket the round compares and the parts of it that count.
    pub(crate) fn coverage(&self) -> Vec<(u16, PartMask)> {
        match self {
            Self::Buckets(ids) => ids
                .iter()
                .map(|&(bucket, _)| (bucket, PartMask::ALL))
                .collect(),
            Self::Parts(masked) => masked.iter().map(|m| (m.bucket, m.mask)).collect(),
        }
    }

    /// Keeps the buckets whose parts in scope `keep` accepts.
    pub(crate) fn retain(&mut self, mut keep: impl FnMut(u16, PartMask) -> bool) {
        match self {
            Self::Buckets(ids) => ids.retain(|&(bucket, _)| keep(bucket, PartMask::ALL)),
            Self::Parts(masked) => masked.retain(|m| keep(m.bucket, m.mask)),
        }
    }
}

/// The type-erased surface the network layer drives a shard through, wire bytes
/// in and out: the boundary where postcard (de)serialization happens for the
/// wire. A local read decodes its own stored record under the stripe's read
/// lock. Implemented by `Shard<K, V>` for any `K`, `V` meeting its bounds, and
/// held as `Arc<dyn ShardOps>` in the cluster's cache registry.
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

    /// Serves a peer's [`crate::wire::Msg::Load`] for `keys`, encoded:
    /// loads them through this shard's loader, joining loads already in
    /// flight, and answers with the record stored for each key the cache
    /// or its source holds. Default: unavailable, for a shard with no
    /// loader.
    #[allow(
        private_interfaces,
        reason = "LoadServe is crate-private like the rest of the load path; \
                  see crate::net::RequestHandler::fetch"
    )]
    fn serve_load(&self, keys: Vec<Bytes>) -> BoxFuture<'_, crate::net::LoadServe> {
        let _ = keys;
        Box::pin(async { crate::net::LoadServe::Unavailable })
    }

    /// Takes a peer's [`crate::wire::Msg::RefreshHint`]: the peer read
    /// `key` at version `ver` past its refresh point. Default: ignores it.
    fn refresh_hint(&self, key: Bytes, ver: Hlc) -> BoxFuture<'_, ()> {
        let _ = (key, ver);
        Box::pin(async {})
    }

    /// This shard's current per-bucket XOR digests, `(bucket, digest)` for all
    /// [`BUCKET_COUNT`] buckets. The first step of an anti-entropy round.
    fn digests(&self) -> BoxFuture<'_, Vec<BucketDigest>>;

    /// [`ShardOps::digests`] narrowed to what one anti-entropy round with
    /// `peer` has to reconcile. The full list for every mode but
    /// `Mode::Distributed`, whose override keeps only the buckets `peer`
    /// owns in this shard's current view and this shard holds (owned or
    /// mid disown-grace): a bucket the peer does not own would be reported
    /// as a mismatch every round and its entries pushed only to be dropped
    /// by the peer's inbound guard.
    fn ae_digests_for(&self, peer: NodeId) -> BoxFuture<'_, Vec<BucketDigest>> {
        let _ = peer;
        self.digests()
    }

    /// A `Mode::Distributed` shard's scoped anti-entropy digests for `peer`,
    /// read under one ownership view: its hash and a `Scope` at its
    /// granularity. `None` for every other mode. The default reads
    /// [`ShardOps::ownership_view_hash`] and [`ShardOps::ae_digests_for`]
    /// as a bucket scope.
    #[allow(
        private_interfaces,
        reason = "ScopedDigests is only built and read inside this crate; see ShardOps::ownership_view"
    )]
    fn ae_scoped_digests_for(&self, peer: NodeId) -> BoxFuture<'_, Option<ScopedDigests>> {
        Box::pin(async move {
            let view_hash = self.ownership_view_hash()?;
            let ids = self
                .ae_digests_for(peer)
                .await
                .into_iter()
                .map(|bd| (bd.bucket, bd.digest))
                .collect();
            Some(ScopedDigests {
                view_hash,
                scope: Scope::Buckets(ids),
            })
        })
    }

    /// A [`KeyVersion`] for every live entry and un-GC'd tombstone in
    /// `bucket`, for a peer that reported a digest mismatch there.
    fn bucket_entries(&self, bucket: u16) -> BoxFuture<'_, Vec<KeyVersion>>;

    /// [`ShardOps::bucket_entries`] for many buckets in one pass, so an
    /// anti-entropy round stays linear in shard size instead of quadratic.
    fn entries_for_buckets(&self, buckets: Vec<u16>) -> BoxFuture<'_, BucketEntries>;

    /// The number of live entries plus un-GC'd tombstones in each of
    /// `buckets`, without materializing any of them: an anti-entropy
    /// responder's cheap check for whether a mismatched bucket passes
    /// [`crate::config::ClusterConfig::ae_part_min_bucket`] before it pays to
    /// build a listing or sketch. A bucket at or past [`BUCKET_COUNT`]
    /// answers `0`.
    fn bucket_lens(&self, buckets: Vec<u16>) -> BoxFuture<'_, Vec<BucketLen>>;

    /// Every bucket this shard holds at least one live entry or un-GC'd
    /// tombstone in, whether or not its current view owns the bucket:
    /// unlike [`ShardOps::bucket_lens`], residency never gates this list.
    /// `rebalance_task` folds the held buckets its new view does not own
    /// into the ones it releases, so a bucket owned only under a view it
    /// never observed still hands its data off. The default answers
    /// nothing, for a stub with no store behind it.
    fn held_buckets(&self) -> BoxFuture<'_, Vec<u16>> {
        Box::pin(async { Vec::new() })
    }

    /// [`ShardOps::held_buckets`] at part granularity: every part holding at
    /// least one live entry or un-GC'd tombstone, whether or not the current
    /// view owns it. The default names every part of each held bucket.
    fn held_parts(&self) -> BoxFuture<'_, Vec<PartId>> {
        Box::pin(async move {
            self.held_buckets()
                .await
                .into_iter()
                .flat_map(PartId::of_bucket)
                .collect()
        })
    }

    /// This shard's part digests for each of `buckets`, the second-level
    /// reply for a bucket whose digest mismatched and whose entry count
    /// passed [`crate::config::ClusterConfig::ae_part_min_bucket`].
    fn part_digests(&self, buckets: Vec<u16>) -> BoxFuture<'_, Vec<BucketPartDigests>>;

    /// [`ShardOps::entries_for_buckets`] at part granularity: a
    /// [`KeyVersion`] for every live entry and un-GC'd tombstone in each
    /// requested [`BucketPart`].
    fn entries_for_parts(&self, parts: Vec<BucketPart>) -> BoxFuture<'_, PartEntries>;

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

    /// Runs one rate-limited pass of the CRDT writer-retirement sweep over
    /// this shard's merging caches: reads up to `max_entries` live,
    /// resident candidates, asks the resolver to fold writers out of each
    /// via [`ConflictResolver::compact`], then applies every changed
    /// record with a version-gated direct replace, never the ordinary
    /// merge-based apply path (see
    /// `engine::Engine::compact_replace_if_current`'s doc for why).
    /// `now_ms`, `retire`, `quiet`, and `bounds` are exactly
    /// [`ConflictResolver::compact`]'s own parameters, threaded straight
    /// through; this method's job is the shard-level plumbing, never the
    /// retirement decision, which stays the resolver's call.
    ///
    /// Returns every writer the scan found eligible for retirement,
    /// deduplicated across the batch, and how many records were
    /// compacted; the writer count can exceed the record count, since a
    /// bucket this shard doesn't own anymore is skipped without applying
    /// but its writer(s) are still reported. A no-op for a shard whose
    /// resolver never compacts (the common case for [`LwwResolver`]).
    /// Called periodically by `crdt_compact_task`.
    fn compact_pass(
        &self,
        now_ms: u64,
        retire: &dyn Fn(crdt::WriterId) -> bool,
        quiet: Quiescence,
        bounds: CompactionBounds,
        max_entries: usize,
    ) -> BoxFuture<'_, CompactPassOutcome> {
        let _ = (now_ms, retire, quiet, bounds, max_entries);
        Box::pin(async {
            CompactPassOutcome {
                retired: Vec::new(),
                compacted: 0,
                stripes_visited: BUCKET_COUNT,
            }
        })
    }

    /// Closes this shard's spill tier, if the `spill` feature is compiled in
    /// and one was attached via `CacheBuilder::spill`: stops accepting
    /// new spills and drops the flusher thread's channel sender. A no-op
    /// otherwise. This close writes no checkpoint, so the next open is cold
    /// regardless of `SpillConfig::warm_reopen`; use
    /// [`ShardOps::close_spill_checkpointed`] for a warm reopen.
    fn close_spill(&self) {}

    /// [`ShardOps::close_spill`], plus, with `SpillConfig::warm_reopen` on, a
    /// checkpoint of every live entry and a snapshot a later warm reopen
    /// replays. The default just calls `close_spill`, so an implementor with
    /// no spill tier keeps working, cold on the next open. Called by
    /// `crate::cache::Cache::close` and cluster shutdown.
    fn close_spill_checkpointed(&self) -> BoxFuture<'_, ()> {
        self.close_spill();
        Box::pin(async {})
    }

    /// Stops accepting writes while keeping the fan-out backlog for the
    /// fan-out task's final drain: the first step of a cluster shutdown,
    /// before its tasks are cancelled. A write from here on fails with
    /// [`CacheError::Closed`] instead of being accepted and never sent.
    fn seal_fan_out(&self) {}

    /// This shard's current bucket-ownership view, for a `Mode::Distributed`
    /// cache that has one attached. `None` for every other mode, and for a
    /// `Distributed` cache before its tracker is attached.
    #[allow(
        private_interfaces,
        reason = "OwnershipView has no public fields or methods; only this crate can act on the returned value regardless of this trait's own visibility"
    )]
    fn ownership_view(&self) -> Option<Arc<OwnershipView>> {
        None
    }

    /// This shard's current bucket-ownership `view_hash`, for a
    /// `Mode::Distributed` cache that has one attached. `None` for every
    /// other mode, and for a `Distributed` cache before its tracker is
    /// attached. A thin projection of [`ShardOps::ownership_view`] for a
    /// caller that only needs the epoch check, not the whole view.
    fn ownership_view_hash(&self) -> Option<u64> {
        None
    }

    /// Removes every locally held entry, live or tombstone, in each of
    /// `buckets`, for a `Mode::Distributed` cache whose disown-grace has
    /// elapsed for them. A no-op returning `0` for every other mode, which
    /// never disowns a bucket in the first place. See
    /// `engine::Engine::release_buckets` for the accounting this resets.
    fn release_buckets(&self, buckets: &[u16]) -> BoxFuture<'_, u64> {
        let _ = buckets;
        Box::pin(async { 0 })
    }

    /// [`ShardOps::release_buckets`] for single parts: removes every locally
    /// held entry, live or tombstone, in each of `parts`, leaving the rest
    /// of each bucket untouched. A no-op returning `0` for a stub.
    fn release_parts(&self, parts: &[PartId]) -> BoxFuture<'_, u64> {
        let _ = parts;
        Box::pin(async { 0 })
    }

    /// Whether `part` is owned here but not yet pulled from a co-owner; see
    /// [`ShardOps::is_cold_bucket`]. `false` for a stub.
    fn is_cold_part(&self, part: PartId) -> bool {
        let _ = part;
        false
    }

    /// Whether `part` was warm-reloaded and not yet verified; see
    /// [`ShardOps::is_unverified_bucket`]. `false` for a stub.
    fn is_unverified_part(&self, part: PartId) -> bool {
        let _ = part;
        false
    }

    /// Whether `bucket` is owned by this `Mode::Distributed` shard but not
    /// yet pulled from a co-owner, so a local miss there is not an answer.
    /// `false` for every other mode.
    fn is_cold_bucket(&self, bucket: u16) -> bool {
        let _ = bucket;
        false
    }

    /// Whether `bucket` was warm-reloaded from spill and not yet verified
    /// against a co-owner: a local hit there is not trustworthy, since a
    /// co-owner may have deleted the record during this node's downtime.
    /// `false` for every other mode and for a bucket never warm-reloaded.
    fn is_unverified_bucket(&self, bucket: u16) -> bool {
        let _ = bucket;
        false
    }

    /// Narrows anti-entropy's `dirty`/`live` peer candidates to this
    /// shard's current cohort before `pick_peer`'s random choice runs.
    /// Identity for every mode but `Mode::Distributed`, whose override
    /// narrows both lists to peers sharing at least one bucket this shard
    /// currently owns or is mid disown-grace on: a peer sharing nothing
    /// with this shard has nothing to reconcile.
    fn ae_peer_filter(&self, dirty: Vec<NodeId>, live: Vec<NodeId>) -> (Vec<NodeId>, Vec<NodeId>) {
        (dirty, live)
    }

    /// Whether this shard's [`ConflictResolver`] merges, forwarded straight
    /// from [`ConflictResolver::merges`]. `cluster::anti_entropy` reads this once
    /// per round and, when `true`, has `diff_bucket`/`diff_decoded` queue a
    /// key present on both sides under different versions for both push and
    /// pull instead of only pushing the greater side to the lesser one, so
    /// two replicas each holding half of a merge exchange records in the
    /// same round rather than needing a second round to carry the minted
    /// result back. The pulls run first, and a key that lands at exactly the
    /// peer's version is not pushed back. `false` for every shard without a
    /// resolver set, since [`LwwResolver::merges`] is `false`.
    fn merges(&self) -> bool {
        false
    }
}

/// The resolver a merging shard runs: `inner` for every decision,
/// plus [`ConflictResolver::settle`] applied to each merge result with the
/// shard's clock and its [`CompactionBounds`], so aged fold receipts are
/// dropped on the apply path and not only on the compaction sweep.
struct SettlingResolver {
    inner: Arc<dyn ConflictResolver>,
    bounds: CompactionBounds,
    clock_fn: Arc<dyn Fn() -> u64 + Send + Sync>,
}

impl ConflictResolver for SettlingResolver {
    fn winner(&self, key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        self.inner.winner(key, a, b)
    }

    fn needs_value_bytes(&self) -> bool {
        self.inner.needs_value_bytes()
    }

    fn merges(&self) -> bool {
        self.inner.merges()
    }

    fn merge(&self, key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Option<Merged> {
        let merged = self.inner.merge(key, a, b)?;
        let now_ms = (self.clock_fn)();
        match self.inner.settle(
            key,
            merged.value.as_ref(),
            now_ms,
            self.bounds.receipt_ttl_ms,
        ) {
            Some(value) => Some(Merged {
                value,
                expires_at_ms: merged.expires_at_ms,
            }),
            None => Some(merged),
        }
    }

    fn compact(
        &self,
        key: &[u8],
        value: &[u8],
        now_ms: u64,
        retire: &dyn Fn(crdt::WriterId) -> bool,
        quiet: Quiescence,
        bounds: CompactionBounds,
    ) -> Option<Bytes> {
        self.inner
            .compact(key, value, now_ms, retire, quiet, bounds)
    }

    fn settle(&self, key: &[u8], value: &[u8], now_ms: u64, receipt_ttl_ms: u64) -> Option<Bytes> {
        self.inner.settle(key, value, now_ms, receipt_ttl_ms)
    }
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

/// Builds an [`Incoming::Put`], for the bulk-insert closures that reassemble one from a prepared tuple.
fn put_incoming<V>(value: V, expires_at_ms: Option<u64>, encoded: Bytes) -> Incoming<V> {
    Incoming::Put {
        value,
        expires_at_ms,
        encoded,
    }
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

/// The epoch millisecond at which a [`Shard::merge`] coalescing window
/// opened at `started_at_ms` must flush. Pure so the scheduling math behind
/// `Shard::merge`'s pending map has a unit test independent of a live clock.
fn merge_deadline_ms(started_at_ms: u64, window: Duration) -> u64 {
    started_at_ms.saturating_add(duration_ms(window))
}

/// Whether a pending coalesced merge whose window closes at `deadline_ms`
/// must flush by `now_ms`. The one real decision
/// [`Shard::flush_due_pending_merges`]'s background sweep makes, split out
/// so it has its own unit test that needs neither a live `Shard` nor
/// `tokio::time`.
fn merge_window_elapsed(now_ms: u64, deadline_ms: u64) -> bool {
    now_ms >= deadline_ms
}

/// Pops every `(deadline_ms, seq) -> key` entry from `by_deadline` whose
/// deadline has passed by `now_ms`, in ascending deadline order, stopping at
/// the first entry not yet due. `BTreeMap` keeps `by_deadline` ordered by
/// its `(deadline_ms, seq)` key, so this touches only the entries due,
/// never scanning the whole stripe, keeping
/// [`Shard::flush_due_pending_merges`]'s sweep cheap at high key counts.
/// Pure over the ordering structure alone (no `V`, no live `Shard`), split
/// out for its own unit test the same way [`merge_window_elapsed`] is.
fn drain_due_deadlines<K>(by_deadline: &mut BTreeMap<(u64, u64), K>, now_ms: u64) -> Vec<K> {
    let mut due = Vec::new();
    while let Some(&(deadline_ms, _)) = by_deadline.keys().next() {
        if !merge_window_elapsed(now_ms, deadline_ms) {
            break;
        }
        let (_, key) = by_deadline
            .pop_first()
            .expect("just peeked the smallest key");
        due.push(key);
    }
    due
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

/// [`Shard::attach_spill`]'s result: the parts the reopen replayed at
/// least one record for. Empty covers both a cold fallback and
/// a clean reopen with nothing to replay.
#[cfg(feature = "spill")]
pub(crate) struct AttachSpillOutcome {
    pub(crate) warm_parts: HashSet<PartId>,
}

/// Increments `sundog_spill_checkpoint_entries_total{cache,stage}` by
/// `count` for one checkpoint phase, matching
/// `Shard::close_spill_checkpointed`'s `info` logs.
#[cfg(feature = "spill")]
fn record_checkpoint_stage(cache: &str, stage: &'static str, count: u64) {
    metrics::counter!(
        "sundog_spill_checkpoint_entries_total",
        "cache" => cache.to_string(),
        "stage" => stage,
    )
    .increment(count);
}

/// Increments `sundog_spill_reopen_entries_total{cache,stage}` by `count`
/// for one reopen phase, matching [`Shard::attach_spill`]'s `info` logs.
#[cfg(feature = "spill")]
fn record_reopen_stage(cache: &str, stage: &'static str, count: u64) {
    metrics::counter!(
        "sundog_spill_reopen_entries_total",
        "cache" => cache.to_string(),
        "stage" => stage,
    )
    .increment(count);
}

// Every method here is fully synchronous, with no `.await`, except
// `Shard::get`/`Shard::get_or_load`'s spilled-key path under `feature =
// "spill"`: a disk read runs behind `spawn_blocking` and a semaphore
// permit there, the one place this backend awaits. `async fn` stays the
// signature everywhere in this block regardless, so callers and a later
// backend swap need no signature change.
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
        let engine = Arc::new(Engine::new(max_capacity, tti, None, None));
        let hits = metrics::counter!("sundog_cache_hits_total", "cache" => name.to_string());
        let misses = metrics::counter!("sundog_cache_misses_total", "cache" => name.to_string());
        let read_durations = read_timing::ReadDurations::new(&name);
        let skew_rejected =
            metrics::counter!("sundog_clock_skew_rejected_total", "cache" => name.to_string());
        let fan_out = Arc::new(FanOutQueue::new(name.clone(), !matches!(mode, Mode::Local)));

        Self {
            name,
            mode,
            engine,
            events: broadcast::channel(EVENTS_CAPACITY).0,
            fan_out,
            clock: StdMutex::new(HlcClock::new(node)),
            clock_fn: Arc::new(now_ms),
            ttl,
            tombstone_ttl_ms: duration_ms(ClusterConfig::default().tombstone_ttl),
            tombstone_max_ttl_ms: duration_ms(ClusterConfig::default().tombstone_max_ttl),
            max_clock_skew_ms: ClusterConfig::default().max_clock_skew.map(duration_ms),
            skew_rejected,
            skew_warned: AtomicBool::new(false),
            clock_jump_warned: AtomicBool::new(false),
            resolver: Arc::new(LwwResolver),
            raw_resolver: Arc::new(LwwResolver),
            crdt_bounds: ClusterConfig::default().crdt_compaction_bounds(),
            max_frame: MAX_FRAME,
            max_resident_bytes: u64::MAX,
            ceiling_refusals: None,
            fan_out_backlog_capacity: ClusterConfig::default().fan_out_backlog_capacity,
            fan_out_wait_timeout: ClusterConfig::default().fan_out_wait_timeout,
            merge_window: Duration::ZERO,
            pending_merges: (0..BUCKET_COUNT)
                .map(|_| StdMutex::new(PendingMergeStripe::new()))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            merge_wake: Notify::new(),
            max_capacity,
            tti,
            capacity_hint: None,
            weigher: None,
            hits,
            misses,
            read_durations,
            loader: None,
            loader_calls: None,
            load_queue: load::LoadQueue::new(),
            refresh: None,
            ownership: None,
            residency: None,
            #[cfg(feature = "spill")]
            spill_read: OnceLock::new(),
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

    /// Overrides how far ahead of this node's clock a remote stamp may be
    /// ([`ClusterConfig::max_clock_skew`]); `None` accepts any stamp.
    #[must_use]
    pub fn with_max_clock_skew(mut self, max_clock_skew: Option<Duration>) -> Self {
        self.max_clock_skew_ms = max_clock_skew.map(duration_ms);
        self
    }

    /// Overrides the [`ConflictResolver`] `Shard::apply` consults whenever an
    /// incoming record's version differs from what's stored. Defaults to
    /// [`LwwResolver`].
    #[must_use]
    pub fn with_resolver(mut self, resolver: Arc<dyn ConflictResolver>) -> Self {
        self.raw_resolver = resolver;
        self.rebuild_resolver();
        self
    }

    /// Overrides the bounds the resolver's [`ConflictResolver::settle`]
    /// step prunes fold receipts against on every merge apply; the
    /// compaction sweep passes the same bounds per pass. Defaults to
    /// [`ClusterConfig::default`]'s.
    #[must_use]
    pub fn with_crdt_bounds(mut self, bounds: CompactionBounds) -> Self {
        self.crdt_bounds = bounds;
        self.rebuild_resolver();
        self
    }

    /// Applies everything a live cluster's configuration decides for a
    /// shard: tombstone bounds, the clock-skew bound, CRDT bounds, frame
    /// cap, fan-out capacity, and its wait timeout.
    #[must_use]
    pub fn with_cluster_config(self, config: &ClusterConfig) -> Self {
        self.with_tombstone_ttl(config.tombstone_ttl)
            .with_tombstone_max_ttl(config.tombstone_max_ttl)
            .with_max_clock_skew(config.max_clock_skew)
            .with_crdt_bounds(config.crdt_compaction_bounds())
            .with_max_frame(config.max_frame)
            .with_fan_out_backlog_capacity(config.fan_out_backlog_capacity)
            .with_fan_out_wait_timeout(config.fan_out_wait_timeout)
    }

    /// Rewraps `raw_resolver` with the current bound and clock. A
    /// non-merging resolver is used bare: it never merges, so it never
    /// settles either, and the wrapper would only add a hop.
    fn rebuild_resolver(&mut self) {
        self.resolver = if self.raw_resolver.merges() {
            Arc::new(SettlingResolver {
                inner: Arc::clone(&self.raw_resolver),
                bounds: self.crdt_bounds,
                clock_fn: Arc::clone(&self.clock_fn),
            })
        } else {
            Arc::clone(&self.raw_resolver)
        };
    }

    /// Overrides the hard cap [`Shard::insert`] enforces before writing a
    /// value, threaded from a live cluster's configured
    /// `ClusterConfig::max_frame`. Defaults to [`MAX_FRAME`].
    #[must_use]
    pub fn with_max_frame(mut self, max_frame: usize) -> Self {
        self.max_frame = max_frame;
        self
    }

    /// Sets a soft ceiling on [`Shard::resident_bytes`]. A local write
    /// ([`Shard::insert`], [`Shard::insert_with_ttl`], [`Shard::merge`],
    /// [`Shard::insert_many`]) that finds the ceiling reached fails with
    /// [`CacheError::OverMemoryCeiling`], and a read-through fill returns its
    /// value without storing it. Removals and writes arriving from peers are
    /// always accepted, so every replica keeps the same entries and resident
    /// bytes can pass the ceiling by what peers send. Defaults to `u64::MAX`,
    /// no ceiling.
    ///
    /// A shard with no ceiling counts no bytes: [`Shard::resident_bytes`]
    /// is `None` and the write paths skip the check. Setting one rebuilds
    /// the engine with counting on, carrying forward any
    /// [`Shard::with_weigher`] and [`Shard::with_capacity_hint`], so call it
    /// right after [`Shard::new`], before any reads or writes reach this
    /// shard. `u64::MAX` removes the ceiling.
    #[must_use]
    pub fn with_max_resident_bytes(mut self, ceiling: u64) -> Self {
        self.max_resident_bytes = ceiling;
        self.ceiling_refusals = (ceiling != u64::MAX).then(|| CeilingRefusals::new(&self.name));
        if self.ceiling_refusals.is_some() != self.engine.resident_bytes().is_some() {
            self.engine = self.build_engine();
        }
        self
    }

    /// Bytes this shard's live entries hold in memory, counted only under
    /// a [`Shard::with_max_resident_bytes`] ceiling and `None` without one:
    /// a fixed per-entry overhead for the entry slot and its index row,
    /// plus each record's heap-held key and value bytes. A spilled entry
    /// counts only its key. A lower bound on the process memory the entries
    /// occupy, since allocator rounding and spare arena capacity are not
    /// counted.
    #[must_use]
    pub fn resident_bytes(&self) -> Option<u64> {
        self.engine.resident_bytes()
    }

    /// [`Shard::resident_bytes`] once it has reached the ceiling set by
    /// [`Shard::with_max_resident_bytes`], `None` below it or with no
    /// ceiling.
    fn at_ceiling(&self) -> Option<u64> {
        let resident_bytes = self.engine.resident_bytes()?;
        (resident_bytes >= self.max_resident_bytes).then_some(resident_bytes)
    }

    /// Ends a loader run that returned `value` for `key`, the load stamped
    /// `ver` before it started, through [`Shard::store_loaded`], and counts
    /// the run as a miss.
    fn finish_fill(
        &self,
        key: &K,
        key_bytes: &Bytes,
        hash: u64,
        ver: Hlc,
        inflight: &engine::Inflight<V>,
        value: V,
    ) {
        self.store_loaded(key, key_bytes, hash, ver, inflight, value);
        self.misses.increment(1);
    }

    /// Stores a loaded `value` for `key` through
    /// [`engine::Engine::complete_fresh_load`], the load stamped `ver`
    /// before it started, or hands it to joined waiters uncached at the
    /// ceiling. A stored value fans out per [`fill_fans_out`] and emits
    /// [`Event::Created`], or [`Event::Updated`] over a live entry; one a
    /// write, removal or invalidation overtook stores and sends nothing.
    /// Returns whether the value was stored.
    fn store_loaded(
        &self,
        key: &K,
        key_bytes: &Bytes,
        hash: u64,
        ver: Hlc,
        inflight: &engine::Inflight<V>,
        value: V,
    ) -> bool {
        if self.at_ceiling().is_some() {
            self.engine
                .finish_uncached(key_bytes, hash, inflight, value);
            if let Some(refusals) = &self.ceiling_refusals {
                refusals.fills.increment(1);
            }
            return false;
        }
        let encoded = Bytes::from(
            postcard::to_stdvec(&value)
                .expect("invariant: a value returned by the loader postcard-encodes"),
        );
        let expires_at_ms = self.expiry_for(None);
        let outcome = self.engine.complete_fresh_load(
            engine::FreshLoad {
                key,
                key_bytes,
                hash,
                ver,
                value: value.clone(),
                encoded,
                expires_at_ms,
            },
            self.now_ms(),
            inflight,
        );
        let engine::FillOutcome::Installed { had_live } = outcome else {
            return false;
        };
        // A refusal means the cache is closing; the loaded value still
        // answers the read.
        if fill_fans_out(self.mode) {
            let _ = self.fan_out.push(FanOutItem::Applied(key.clone()));
        }
        if self.events.receiver_count() > 0 {
            let key = key.clone();
            let origin = Origin::Local;
            let _ = self.events.send(if had_live {
                Event::Updated { key, value, origin }
            } else {
                Event::Created { key, value, origin }
            });
        }
        true
    }

    /// `Err(CacheError::OverMemoryCeiling)`, counted, when this shard is at
    /// its ceiling: the gate every local write passes first.
    fn check_ceiling(&self) -> Result<(), CacheError> {
        match self.at_ceiling() {
            None => Ok(()),
            Some(resident_bytes) => {
                if let Some(refusals) = &self.ceiling_refusals {
                    refusals.writes.increment(1);
                }
                Err(CacheError::OverMemoryCeiling {
                    cache: self.name.clone(),
                    resident_bytes,
                    ceiling: self.max_resident_bytes,
                })
            }
        }
    }

    /// Overrides the fan-out backlog capacity an async write waits against
    /// before queuing. Defaults to [`ClusterConfig::default`]'s.
    #[must_use]
    pub fn with_fan_out_backlog_capacity(mut self, capacity: usize) -> Self {
        self.fan_out_backlog_capacity = capacity;
        self
    }

    /// Overrides the bound on that fan-out backlog wait. Defaults to
    /// [`ClusterConfig::default`]'s.
    #[must_use]
    pub fn with_fan_out_wait_timeout(mut self, timeout: Duration) -> Self {
        self.fan_out_wait_timeout = timeout;
        self
    }

    /// Sets the window [`Shard::merge`] coalesces consecutive calls to one
    /// key within: they fold in memory instead of each applying on its own,
    /// and the fold applies once, when the window elapses. Zero, the
    /// default, applies every `merge` call at once. A raw setter that
    /// trusts the caller to have already checked the resolver merges;
    /// [`crate::cache::CacheBuilder::merge_coalesce_window`] is the
    /// validated entry point.
    #[must_use]
    pub fn with_merge_coalesce_window(mut self, window: Duration) -> Self {
        self.merge_window = window;
        self
    }

    /// This shard's configured [`Shard::with_merge_coalesce_window`], for
    /// `crate::cache`'s background sweep to decide whether to spawn at all.
    pub(crate) fn merge_window(&self) -> Duration {
        self.merge_window
    }

    /// Flips [`Engine::apply_many`]'s pre-fold on or off for this shard's
    /// engine, `true` (pre-folding on) by default. `#[doc(hidden)]`:
    /// reachable from `crate::cache::CacheBuilder::prefold_enabled`, in turn
    /// reachable from an integration-test binary outside this crate; a
    /// benchmark measuring pre-fold's own effect is the only caller that
    /// ever needs it off, to compare against the unfolded per-record path
    /// [`Engine::apply_many`] otherwise always takes. Never call this
    /// outside a benchmark or test.
    #[doc(hidden)]
    #[must_use]
    pub fn with_prefold_enabled(self, enabled: bool) -> Self {
        self.engine.set_prefold_enabled(enabled);
        self
    }

    /// This shard's engine's current [`Shard::with_prefold_enabled`] flag,
    /// for a test to confirm the toggle reached the engine.
    #[cfg(test)]
    pub(crate) fn prefold_enabled(&self) -> bool {
        self.engine.prefold_enabled()
    }

    /// Attaches this shard's `Mode::Distributed` bucket-ownership tracker
    /// and its disown-grace residency set. Call this once, right after
    /// construction and before the shard is ever shared, so
    /// [`ShardOps::ownership_view`] never observes anything but a real,
    /// already-computed view. Every other mode leaves both unset.
    #[must_use]
    pub(crate) fn with_ownership(
        mut self,
        tracker: OwnershipTracker,
        residency: Arc<ResidencySet>,
    ) -> Self {
        self.ownership = Some(tracker);
        self.residency = Some(residency);
        self
    }

    /// The residency set [`Shard::with_ownership`] attached, for a test
    /// that marks a bucket cold or releasing by hand.
    #[cfg(all(test, not(feature = "sim")))]
    pub(crate) fn residency(&self) -> Option<&Arc<ResidencySet>> {
        self.residency.as_ref()
    }

    /// Test-only mirror of [`Shard::with_ownership`], reachable from
    /// `tests/sim.rs`, which lives outside this crate and cannot otherwise
    /// reach a `pub(crate)` builder method. Never call this outside the sim
    /// harness.
    #[cfg(feature = "sim")]
    #[doc(hidden)]
    #[must_use]
    pub fn with_ownership_for_sim(
        self,
        tracker: OwnershipTracker,
        residency: Arc<ResidencySet>,
    ) -> Self {
        self.with_ownership(tracker, residency)
    }

    /// Test-only mirror of [`Shard::fan_out_queue`]'s drain, translating
    /// each [`FanOutItem`] into the sim-visible [`SimFanOut`] shape so the
    /// harness never needs `FanOutItem`/`FanOutQueue` themselves made
    /// reachable. Reachable from `tests/sim.rs` for the same reason as
    /// [`Shard::with_ownership_for_sim`].
    #[cfg(feature = "sim")]
    #[doc(hidden)]
    pub fn drain_fan_out_for_sim(&self) -> Vec<SimFanOut<K>> {
        self.fan_out_queue()
            .drain()
            .into_iter()
            .map(|item| match item {
                FanOutItem::Applied(key) => SimFanOut::Applied(key),
                FanOutItem::Forward(rec) => SimFanOut::Forward(rec),
            })
            .collect()
    }

    /// Installs a custom per-entry weigher for size-bounded eviction, in place
    /// of the default of one weight unit per entry. Rebuilds
    /// `engine::Engine` from scratch, carrying forward any
    /// [`Shard::with_capacity_hint`] already set. Call this right after
    /// [`Shard::new`], before any reads or writes reach this shard; the two
    /// builders can run in either order.
    #[must_use]
    pub fn with_weigher<W>(mut self, weigher: W) -> Self
    where
        W: Fn(&K, &V) -> u32 + Send + Sync + 'static,
    {
        self.weigher = Some(Arc::new(weigher));
        self.engine = self.build_engine();
        self
    }

    /// Hints this shard's expected local entry count, presizing every
    /// stripe up front; see [`crate::cache::CacheBuilder::capacity_hint`]
    /// for the contract. Rebuilds the engine, carrying forward any
    /// [`Shard::with_weigher`] already set, in either order. Call right
    /// after [`Shard::new`], before any reads or writes reach this shard.
    #[must_use]
    pub fn with_capacity_hint(mut self, hint: u64) -> Self {
        self.capacity_hint = Some(hint);
        self.engine = self.build_engine();
        self
    }

    /// A fresh engine from this shard's capacity, TTI, weigher, capacity
    /// hint, ceiling and the current engine's pre-fold flag: the rebuild
    /// every engine-shaping builder shares, so each carries the others'
    /// settings forward. Counts resident bytes only under a ceiling.
    fn build_engine(&self) -> Arc<Engine<K, V>> {
        let engine = Engine::new(
            self.max_capacity,
            self.tti,
            self.weigher.clone().map(boxed_weigher),
            self.capacity_hint,
        );
        engine.set_prefold_enabled(self.engine.prefold_enabled());
        Arc::new(if self.max_resident_bytes == u64::MAX {
            engine
        } else {
            engine.count_resident_bytes()
        })
    }

    /// Per-stripe `live` arena capacities, in stripe order. `#[doc(hidden)]`
    /// test/benchmark accessor for an integration-test binary outside this crate.
    #[doc(hidden)]
    pub fn stripe_capacities(&self) -> Vec<usize> {
        self.engine.stripe_capacities()
    }

    /// Opens a local SSD/NVMe spill tier at `cfg` and attaches it to this
    /// shard's engine: once `max_capacity` is exceeded, eviction demotes the
    /// coldest entries onto disk instead of discarding them. Call this last
    /// in the builder chain, right after [`Shard::with_weigher`] if both are
    /// used. [`Shard::with_weigher`] rebuilds the engine from scratch, and
    /// the tier attaches to whichever engine is current when this runs. A
    /// write's eviction that the tier refuses leaves the victim resident, to
    /// be retried by a later eviction pass, for a `Mode::Replicated` or
    /// `Mode::Distributed` shard, and deletes it for
    /// `Mode::Local`/`Mode::Invalidation`; a promotion's eviction leaves a
    /// refused victim resident in every mode.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`std::io::Error`] if the tier's directory or
    /// a region file cannot be created; see `spill::SpillTier::open`.
    ///
    /// # Panics
    ///
    /// Panics if `Shard::attach_spill` has already run on this shard; this
    /// method calls it.
    #[cfg(feature = "spill")]
    pub fn with_spill(self, cfg: &spill::SpillConfig) -> Result<Self, std::io::Error> {
        self.attach_spill(cfg)?;
        Ok(self)
    }

    /// The core of [`Shard::with_spill`], through `&self` rather than
    /// consuming `self`: opens the tier, attaches it to this shard's engine,
    /// and creates the spilled-key read-path metric handles.
    /// `Engine::spill`/`Engine::set_spill` and this shard's own `spill_read`
    /// are `OnceLock`s rather than plain fields behind `&mut self`, so this
    /// can run once the shard is already `Arc`-shared.
    /// [`crate::cache::CacheBuilder::open`] relies on that. It reserves
    /// this shard's name in the cluster's shard registry first, then calls
    /// this only for the `open()` that wins that reservation, so a losing,
    /// already-open, `open()` never wipes or preallocates another cache's
    /// region files. [`Shard::with_spill`] stays the ordinary
    /// builder-chain entry point for a caller that already owns an
    /// unshared `Shard`.
    ///
    ///
    /// Prefers `spill::SpillTier::reopen` over a cold open when
    /// `SpillConfig::warm_reopen` is on and a checkpoint is still inside
    /// `ClusterConfig::tombstone_ttl`, replaying it instead of wiping the
    /// region files, filtered to [`Shard::with_ownership`]'s owned parts.
    /// Returns [`AttachSpillOutcome::warm_parts`], the subset it replayed.
    /// A warm-reloaded part still counts as cold per
    /// [`ShardOps::is_cold_part`] until the caller clears it.
    /// # Errors
    ///
    /// Returns the underlying [`std::io::Error`] if the tier's directory or
    /// a region file cannot be created; see `spill::SpillTier::open`.
    ///
    /// # Panics
    ///
    /// Panics if called more than once on the same shard.
    #[cfg(feature = "spill")]
    pub(crate) fn attach_spill(
        &self,
        cfg: &spill::SpillConfig,
    ) -> Result<AttachSpillOutcome, std::io::Error> {
        let sink = Arc::clone(&self.engine) as Arc<dyn spill::SpillSink>;
        let view = self.ownership.as_ref().map(OwnershipTracker::current);
        let owned = move |part: PartId| view.as_ref().is_none_or(|v| v.owns(part));
        let outcome = spill::SpillTier::reopen(
            cfg,
            &self.name,
            sink.as_ref(),
            now_ms(),
            self.tombstone_ttl_ms,
            owned,
        )?;
        if outcome.warm {
            tracing::info!(
                cache = %self.name,
                records = outcome.records_installed,
                "sundog spill: warm reopen replayed the tier from disk",
            );
            metrics::counter!(
                "sundog_spill_reopen_total",
                "cache" => self.name.to_string(),
                "outcome" => "warm",
                "reason" => "",
            )
            .increment(1);
        } else {
            let reason = outcome.reason.unwrap_or("unknown");
            tracing::info!(
                cache = %self.name,
                reason,
                "sundog spill: cold fallback",
            );
            metrics::counter!(
                "sundog_spill_reopen_total",
                "cache" => self.name.to_string(),
                "outcome" => "cold_fallback",
                "reason" => reason,
            )
            .increment(1);
        }
        metrics::counter!(
            "sundog_spill_reopen_records_total",
            "cache" => self.name.to_string(),
        )
        .increment(outcome.records_installed);
        tracing::info!(
            cache = %self.name,
            read = outcome.entries_read,
            dropped_unowned = outcome.dropped_unowned,
            dropped_expired = outcome.dropped_expired,
            dropped_bad = outcome.dropped_bad,
            installed = outcome.records_installed,
            refused_present = outcome.refused_present,
            "sundog spill: reopen entry accounting",
        );
        record_reopen_stage(&self.name, "read", outcome.entries_read);
        record_reopen_stage(&self.name, "dropped_unowned", outcome.dropped_unowned);
        record_reopen_stage(&self.name, "dropped_expired", outcome.dropped_expired);
        record_reopen_stage(&self.name, "dropped_bad", outcome.dropped_bad);
        record_reopen_stage(&self.name, "installed", outcome.records_installed);
        record_reopen_stage(&self.name, "refused_present", outcome.refused_present);
        let tier = Arc::new(outcome.tier);
        tier.set_keep_resident_when_refused(matches!(
            self.mode,
            Mode::Replicated | Mode::Distributed { .. }
        ));
        self.engine.set_spill(Arc::clone(&tier));
        tier.attach(Arc::downgrade(&sink));
        self.spill_read
            .set(SpillRead {
                semaphore: Arc::new(tokio::sync::Semaphore::new(cfg.read_concurrency_value())),
                reads_hit: metrics::counter!(
                    "sundog_spill_reads_total",
                    "cache" => self.name.to_string(), "outcome" => "hit",
                ),
                reads_stale: metrics::counter!(
                    "sundog_spill_reads_total",
                    "cache" => self.name.to_string(), "outcome" => "stale",
                ),
                reads_io_error: metrics::counter!(
                    "sundog_spill_reads_total",
                    "cache" => self.name.to_string(), "outcome" => "io_error",
                ),
                promotions: metrics::counter!(
                    "sundog_spill_promotions_total",
                    "cache" => self.name.to_string(),
                ),
                read_duration: metrics::histogram!(
                    "sundog_spill_read_duration_seconds",
                    "cache" => self.name.to_string(),
                ),
            })
            .unwrap_or_else(|_| panic!("invariant: attach_spill runs at most once per shard"));
        Ok(AttachSpillOutcome {
            warm_parts: outcome.warm_parts,
        })
    }

    /// Overrides the clock every timestamp this shard stamps reads from, in
    /// place of the system clock. Reserved for a deterministic-clock fuzz
    /// harness driving this shard's notion of time.
    #[doc(hidden)]
    #[must_use]
    pub fn with_clock(mut self, now_ms: Arc<dyn Fn() -> u64 + Send + Sync>) -> Self {
        self.clock_fn = now_ms;
        self.rebuild_resolver();
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

    /// Whether [`Shard::with_ownership`] attached a residency set to this
    /// shard. Test-only: production code reads `residency` directly once
    /// anti-entropy's cohort widening consumes it.
    #[cfg(test)]
    fn has_residency(&self) -> bool {
        self.residency.is_some()
    }

    /// The timestamp, in epoch milliseconds, this shard stamps its writes,
    /// deadlines, and sweeps with. The system clock, unless overridden by
    /// [`Shard::with_clock`].
    fn now_ms(&self) -> u64 {
        (self.clock_fn)()
    }

    fn stamp_local(&self) -> Hlc {
        let now = self.now_ms();
        let (stamp, lead_ms) = {
            let mut clock = self.clock.lock().unwrap_or_else(PoisonError::into_inner);
            let lead_ms = clock.lead_ms(now);
            (clock.now(now), lead_ms)
        };
        if clock_jumped(lead_ms, self.max_clock_skew_ms)
            && !self.clock_jump_warned.swap(true, Ordering::Relaxed)
        {
            tracing::warn!(
                cache = %self.name,
                behind_ms = lead_ms,
                "this node's clock is further behind its own last write stamp than max_clock_skew: \
                 the system clock stepped back, or it was set ahead and corrected; writes stay \
                 ordered at the last stamp until the clock catches up"
            );
        }
        stamp
    }

    /// Merges `remote` into this shard's clock and returns `true`, or
    /// refuses it and returns `false` when it is stamped more than
    /// `max_clock_skew_ms` ahead of this node's clock: a refused record is
    /// not applied, and counts in `sundog_clock_skew_rejected_total{cache}`.
    fn observe_remote(&self, remote: Hlc) -> bool {
        let now = self.now_ms();
        let accepted = {
            let mut clock = self.clock.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(max_skew_ms) = self.max_clock_skew_ms {
                clock.observe_bounded(now, remote, max_skew_ms).is_some()
            } else {
                clock.observe(now, remote);
                true
            }
        };
        if !accepted {
            self.skew_rejected.increment(1);
            let ahead_ms = remote.wall_ms.saturating_sub(now);
            if self.skew_warned.swap(true, Ordering::Relaxed) {
                tracing::debug!(cache = %self.name, writer = %remote.node, ahead_ms, "refused a record stamped past max_clock_skew");
            } else {
                tracing::warn!(
                    cache = %self.name,
                    writer = %remote.node,
                    ahead_ms,
                    "refused a record stamped further ahead of this node's clock than \
                     max_clock_skew; the writer's clock runs fast, or this one runs slow. \
                     sundog_clock_skew_rejected_total counts every refusal"
                );
            }
        }
        accepted
    }

    /// Merges a stamp this shard derived from one it already holds, such
    /// as a compacted record's, with no skew bound: the stamp it came from
    /// passed [`Shard::observe_remote`] or was stamped here.
    fn observe_derived(&self, derived: Hlc) {
        self.clock
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .observe(self.now_ms(), derived);
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
    /// of [`Shard::hand_off_bulk`].
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
                // A refusal means the cache is closing: the write landed
                // here, as a detached local write, and fans out nowhere.
                if notify_fan_out && matches!(origin, Origin::Local) {
                    let _ = self.fan_out.push(FanOutItem::Applied(key.clone()));
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
                // A refusal means the cache is closing: the write landed
                // here, as a detached local write, and fans out nowhere.
                if notify_fan_out && matches!(origin, Origin::Local) {
                    let _ = self.fan_out.push(FanOutItem::Applied(key.clone()));
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
    fn apply(&self, key: K, key_bytes: Bytes, ver: Hlc, incoming: Incoming<V>, origin: Origin) {
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

    /// Whether this shard's own current ownership view owns `key_bytes`'
    /// part. `true` unconditionally for every mode but `Mode::Distributed`,
    /// which has no [`OwnershipTracker`] attached at all: every other mode
    /// owns everything it holds.
    fn owns_key(&self, key_bytes: &[u8]) -> bool {
        self.ownership
            .as_ref()
            .is_none_or(|tracker| tracker.current().owns(PartId::of_key(key_bytes)))
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

    /// This shard's residency marks for `part`, with ownership taken from
    /// `view`, the view a read decides under. A shard with no residency
    /// set, one not in `Mode::Distributed`, reports ownership alone.
    pub(crate) fn residency_of(&self, view: &OwnershipView, part: PartId) -> Residency {
        let owns = view.owns(part);
        self.residency
            .as_ref()
            .map_or(Residency::new(owns), |residency| {
                residency.residency_of(part, owns, std::time::Instant::now())
            })
    }

    /// What this shard stores for `key_bytes`, judged at this shard's clock,
    /// with that clock reading: no recency touch, no disk read, no
    /// promotion, no load and no metric. Not gated on ownership.
    pub(crate) fn local_record(&self, key_bytes: &[u8]) -> (u64, LocalRecord) {
        let at_ms = self.now_ms();
        (at_ms, self.engine.inspect(key_bytes, at_ms))
    }

    /// [`Shard::residency_of`] under this shard's current view. A shard not
    /// in `Mode::Distributed` owns every part.
    pub(crate) fn current_residency(&self, part: PartId) -> Residency {
        self.ownership
            .as_ref()
            .map_or(Residency::new(true), |tracker| {
                self.residency_of(&tracker.current(), part)
            })
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

    /// Non-owner write path: builds the [`WireRecord`] a
    /// `Mode::Distributed` write for a bucket this node does not own fans
    /// out, without ever touching `engine`; see the store module's
    /// write-path docs for the no-window proof this relies on. Emits
    /// [`Event::Created`]/[`Event::Removed`] directly from the
    /// caller-supplied value: a non-owner holds no prior copy to tell a
    /// create from an update, so a forwarded write never emits
    /// [`Event::Updated`], a documented, deliberate difference from every
    /// other write path here. The caller pushes the returned record onto
    /// [`Shard::fan_out`] as [`FanOutItem::Forward`] and counts
    /// `sundog_forwarded_writes_total{cache}`; this method only builds the
    /// record and emits the event.
    fn forward_write(
        &self,
        key: K,
        key_bytes: Bytes,
        incoming: Incoming<V>,
        ver: Hlc,
    ) -> WireRecord {
        match incoming {
            Incoming::Put {
                value,
                expires_at_ms,
                encoded,
            } => {
                let rec = WireRecord {
                    key: key_bytes,
                    value: Some(encoded),
                    ver,
                    expires_at_ms,
                };
                if self.events.receiver_count() > 0 {
                    let _ = self.events.send(Event::Created {
                        key,
                        value,
                        origin: Origin::Local,
                    });
                }
                rec
            }
            Incoming::Tombstone => {
                let rec = WireRecord {
                    key: key_bytes,
                    value: None,
                    ver,
                    expires_at_ms: None,
                };
                if self.events.receiver_count() > 0 {
                    let _ = self.events.send(Event::Removed {
                        key,
                        origin: Origin::Local,
                    });
                }
                rec
            }
        }
    }

    /// Splits prepared entries into owned and forwarded halves by current part ownership.
    fn partition_owned<T>(&self, prepared: Vec<T>, hash: impl Fn(&T) -> u64) -> (Vec<T>, Vec<T>) {
        match &self.ownership {
            Some(tracker) => {
                let view = tracker.current();
                prepared
                    .into_iter()
                    .partition(|entry| view.owns(PartId::from_hash(hash(entry))))
            }
            None => (prepared, Vec::new()),
        }
    }

    /// [`Shard::insert_many`]'s and [`Shard::remove_many`]'s shared fan-out step, chunked into [`REPLICATE_BATCH_COUNT`] batches.
    fn forward_prepared<T>(
        &self,
        prepared: Vec<T>,
        to_forward: impl Fn(T) -> (K, Bytes, Hlc, Incoming<V>),
    ) -> bool {
        if prepared.is_empty() {
            return true;
        }
        let total = u64::try_from(prepared.len()).unwrap_or(u64::MAX);
        let mut landed = Vec::new();
        let flush = |landed: &mut Vec<_>| {
            self.fan_out
                .extend(landed.drain(..).map(FanOutItem::Forward))
        };
        for entry in prepared {
            let (key, key_bytes, ver, incoming) = to_forward(entry);
            landed.push(self.forward_write(key, key_bytes, incoming, ver));
            if landed.len() >= REPLICATE_BATCH_COUNT && !flush(&mut landed) {
                return false;
            }
        }
        if !flush(&mut landed) {
            return false;
        }
        metrics::counter!("sundog_forwarded_writes_total", "cache" => self.name.to_string())
            .increment(total);
        true
    }

    /// Reads `key`, without triggering read-through. A deleted key is never
    /// present, since a tombstone leaves no live entry to find.
    ///
    /// Counts `sundog_cache_hits_total{cache}` on `Some`,
    /// `sundog_cache_misses_total{cache}` on `None`. On a `feature = "spill"`
    /// build, a RAM miss that turns out to be a currently-spilled entry is
    /// read back off disk behind the tier's read semaphore, via
    /// `spawn_blocking`, and promoted back to residency under a fresh
    /// stripe write lock when room within `max_capacity` can be made;
    /// either way it still counts as a hit. See
    /// the store module's docs and [`Shard::get_sync`], which never does
    /// this. About one read in 256 on each thread is timed into
    /// `sundog_read_duration_seconds{cache,outcome}`, at a stride drawn
    /// uniformly from 128 to 384 reads.
    pub async fn get(&self, key: &K) -> Option<V> {
        let started = read_timing::start_read();
        if let Some(value) = self.read_resident_key(key, self.now_ms()) {
            self.hits.increment(1);
            self.read_durations.record(started, true);
            return Some(value);
        }
        #[cfg(feature = "spill")]
        if let Some(value) = self.get_spilled(key).await {
            self.hits.increment(1);
            self.read_durations.record(started, true);
            return Some(value);
        }
        self.misses.increment(1);
        self.read_durations.record(started, false);
        None
    }

    /// [`Shard::get`] without an async runtime: same hit and miss counting,
    /// for every entry except a currently-spilled one. On `feature =
    /// "spill"`, a spilled entry reads as a miss here. This is the
    /// documented behavioral difference between the sync and async twins:
    /// the RAM-only synchronous path never touches disk, so it cannot read a
    /// value that has moved there. Use [`Shard::get`] to read a spilled
    /// value; it also promotes the entry back to residency when there is
    /// room. Timed
    /// into `sundog_read_duration_seconds` on the same per-thread sample.
    #[must_use]
    pub fn get_sync(&self, key: &K) -> Option<V> {
        let started = read_timing::start_read();
        if let Some(value) = self.read_resident_key(key, self.now_ms()) {
            self.hits.increment(1);
            self.read_durations.record(started, true);
            Some(value)
        } else {
            self.misses.increment(1);
            self.read_durations.record(started, false);
            None
        }
    }

    /// Reads whether `key` has a live entry, honoring expiry, without cloning
    /// the stored value. This asks about the entry as written, not against
    /// some other deadline; no read method here takes a TTL argument. An
    /// existence check, not a read: it moves neither
    /// `sundog_cache_hits_total` nor `sundog_cache_misses_total`, though it
    /// restarts the entry's `tti` idle clock as a read does. A
    /// currently-spilled entry counts as present with zero disk reads:
    /// existence doesn't need the value bytes.
    pub async fn contains_key(&self, key: &K) -> bool {
        self.contains_key_sync(key)
    }

    /// [`Shard::contains_key`] without an async runtime. Answers `true` for
    /// a currently-spilled entry as [`Shard::contains_key`] does, with zero
    /// disk reads: existence doesn't need the value bytes.
    #[must_use]
    pub fn contains_key_sync(&self, key: &K) -> bool {
        self.engine.contains_key(key, self.now_ms())
    }

    /// After a RAM miss, checks whether `key` is currently spilled and, if
    /// so, reads its bytes back off disk and promotes it to residency.
    /// Returns `None` for a resident or absent entry, a stale generation
    /// where the region rotated out from under the pointer, a checksum
    /// mismatch, or a postcard decode failure. Each of the latter three
    /// counts `sundog_spill_reads_total{cache,outcome}` accordingly. A hit
    /// counts `outcome = "hit"` and, iff it flips the entry back to
    /// residency, `sundog_spill_promotions_total{cache}`. The promotion
    /// evicts colder entries to make room within `max_capacity` first, and
    /// never deletes one the spill tier refuses, in any mode: a refused
    /// victim stays resident, so a full flush queue leaves the promotion
    /// without room. The promotion does not happen without room: a
    /// tombstone or a newer write racing the disk read, a tier that refuses
    /// every victim, or an entry heavier than the whole cap leaves the
    /// entry on disk, and the read still answers its value (see
    /// [`engine::Engine::promote_locked`]). No lock or permit is ever held
    /// across more than one `.await` point at a time, and no stripe lock is
    /// ever held across the disk read.
    #[cfg(feature = "spill")]
    async fn get_spilled(&self, key: &K) -> Option<V> {
        let key_bytes = encode_key(key).ok()?;
        let hash = engine::hash_key_bytes(key_bytes.as_ref());
        self.get_spilled_by_bytes(key_bytes.as_ref(), hash)
            .await
            .map(|(_, value)| value)
    }

    /// [`Shard::get_spilled`] for a caller that already has `key_bytes` and
    /// its hash, such as [`Shard::get_or_load`]'s owner arm, with the
    /// version the value was spilled at.
    #[cfg(feature = "spill")]
    async fn get_spilled_by_bytes(&self, key_bytes: &[u8], hash: u64) -> Option<(Hlc, V)> {
        let (ver, value, encoded) = self.read_spilled_by_bytes(key_bytes, hash).await?;
        if self
            .engine
            .promote_locked(key_bytes, hash, ver, &value, &encoded, self.now_ms())
            && let Some(spill_read) = self.spill_read.get()
        {
            spill_read.promotions.increment(1);
        }
        Some((ver, value))
    }

    /// Reads the key's currently spilled value off disk, without promoting
    /// it: the version it was spilled at, the value, and its encoded bytes.
    /// `None` for a key not spilled here, and for a read that fails or finds
    /// the record rotated out, each counted in the spill read metrics.
    #[cfg(feature = "spill")]
    async fn read_spilled_by_bytes(&self, key_bytes: &[u8], hash: u64) -> Option<(Hlc, V, Bytes)> {
        let spill_read = self.spill_read.get()?;
        let tier = Arc::clone(self.engine.spill()?);
        let (ver, loc) = self.engine.spilled_loc(key_bytes, hash, self.now_ms())?;
        let started = std::time::Instant::now();
        let permit = Arc::clone(&spill_read.semaphore)
            .acquire_owned()
            .await
            .ok()?;
        let read = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            tier.read_at(loc)
        })
        .await;
        let bytes = match read {
            Ok(Ok(Some(bytes))) => bytes,
            Ok(Ok(None)) => {
                spill_read.finish(&spill_read.reads_stale, started);
                return None;
            }
            Ok(Err(err)) => {
                tracing::warn!(
                    cache = %self.name,
                    error = %err,
                    "sundog spill: positional read failed"
                );
                spill_read.finish(&spill_read.reads_io_error, started);
                return None;
            }
            Err(_join_err) => {
                spill_read.finish(&spill_read.reads_io_error, started);
                return None;
            }
        };
        let Ok(value) = postcard::from_bytes::<V>(&bytes.encoded) else {
            spill_read.finish(&spill_read.reads_io_error, started);
            return None;
        };
        spill_read.finish(&spill_read.reads_hit, started);
        Some((ver, value, bytes.encoded))
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

    /// [`Shard::keys`] as a visitor: `f` runs once per local live key, never
    /// under a stripe lock, and no `Vec` of every key is built.
    pub fn for_each_key(&self, f: impl FnMut(K)) {
        self.engine.for_each_key(self.now_ms(), f);
    }

    /// Reads `key`, invoking `loader` on a miss. Concurrent callers racing on
    /// the same missing key collapse into one `loader` call.
    ///
    /// Counts `sundog_cache_misses_total{cache}` exactly once per `loader`
    /// execution, from the fill's owner. Every other call, a cache hit or a
    /// collapsed caller, counts `sundog_cache_hits_total{cache}` instead.
    ///
    /// On a `feature = "spill"` build, the collapse extends to a spilled
    /// key's disk read too: only the call that wins ownership of the
    /// missing key checks whether it is currently spilled and, if so, reads
    /// it back before ever calling `loader`. Every other concurrent caller
    /// joins and waits as it already does for an in-flight `loader` run,
    /// so a burst of concurrent misses on one spilled key reads it off
    /// disk once.
    ///
    /// A fill that finishes with the shard at the ceiling set by
    /// [`Shard::with_max_resident_bytes`] returns its value to the caller
    /// and every joined waiter without storing it, and counts
    /// `sundog_ceiling_refusals_total{cache, kind="fill"}`.
    ///
    /// An invalidation for `key` that lands while `loader` runs, from a peer
    /// or [`Shard::invalidate_local`], means the loader may have read the
    /// source before the change it announces: the fill returns its value to
    /// the caller and every joined waiter without storing it, and the next
    /// read loads afresh.
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
                    if let Some(value) = inflight.value.get() {
                        self.hits.increment(1);
                        return Ok(value.clone());
                    }
                    // The fast-path read above picks up the fresh value, or its
                    // absence.
                }
                JoinOutcome::Owner(inflight) => {
                    let guard =
                        self.engine
                            .guard_inflight(key_bytes.clone(), hash, Arc::clone(&inflight));
                    // The sole owner of this key's fill checks the disk
                    // before ever calling `loader`: a burst of concurrent
                    // misses collapses to at most one spilled-key read, the
                    // same way it already collapses to at most one `loader`
                    // call. Ending the in-flight entry hands the value to
                    // every joined waiter, promoted back to RAM or not, iff
                    // the key still holds it; a removal, expiry or newer
                    // write since the read leaves them to read again.
                    #[cfg(feature = "spill")]
                    if let Some((ver, value)) =
                        self.get_spilled_by_bytes(key_bytes.as_ref(), hash).await
                    {
                        self.hits.increment(1);
                        self.engine.finish_spilled_read(
                            &key_bytes,
                            hash,
                            &inflight,
                            ver,
                            value.clone(),
                            self.now_ms(),
                        );
                        guard.complete();
                        return Ok(value);
                    }
                    // Stamped before the loader runs, so a write or removal
                    // that lands during the load is newer than the fill and
                    // keeps its place.
                    let ver = self.stamp_local();
                    return match loader(key).await {
                        Ok(value) => {
                            self.finish_fill(key, &key_bytes, hash, ver, &inflight, value.clone());
                            guard.complete();
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
    /// composition layer's subscription to [`Shard::events`]. Under
    /// `Mode::Distributed` a write for a bucket this node does not own is
    /// never applied here: it is built as a record and forwarded to the
    /// bucket's owners through the same queue.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::ValueTooLarge`] if the wire frame this write would
    /// replicate as exceeds the configured frame cap. See
    /// [`Shard::with_max_frame`], default [`MAX_FRAME`]. Returns
    /// [`CacheError::OverMemoryCeiling`] if this shard is at the ceiling set
    /// by [`Shard::with_max_resident_bytes`].
    pub async fn insert(&self, key: K, value: V) -> Result<(), CacheError> {
        self.wait_for_fan_out_room().await;
        self.insert_sync(key, value)
    }

    /// [`Shard::insert`] without an async runtime: same fan-out and events.
    ///
    /// # Errors
    ///
    /// As [`Shard::insert`].
    pub fn insert_sync(&self, key: K, value: V) -> Result<(), CacheError> {
        self.insert_expiring(key, value, None)
    }

    /// [`Shard::insert`] with a lifespan for this entry alone, overriding the
    /// shard's default TTL. Replicates exactly as a default-TTL stamp does.
    ///
    /// # Errors
    ///
    /// As [`Shard::insert`].
    pub async fn insert_with_ttl(&self, key: K, value: V, ttl: Duration) -> Result<(), CacheError> {
        self.wait_for_fan_out_room().await;
        self.insert_expiring(key, value, Some(ttl))
    }

    fn insert_expiring(&self, key: K, value: V, ttl: Option<Duration>) -> Result<(), CacheError> {
        self.check_ceiling()?;
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
        let incoming = Incoming::Put {
            value,
            expires_at_ms,
            encoded,
        };
        self.apply_or_forward(key, key_bytes, ver, incoming)
    }

    /// Gives `key`'s live entry a lifespan of `ttl` from now and keeps its
    /// value. The write re-puts the value this shard holds, stamped as its
    /// successor: a write or remove stamped in a later millisecond than that
    /// value supersedes it. A spilled entry's value is read back from disk
    /// first. Returns whether a live entry was here. A `Mode::Distributed`
    /// shard answers `false` for a key in a part it does not hold warm, as
    /// [`Shard::get`] misses there; `Cache::expire` asks the owners instead.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::Codec`] if `key` fails to encode or the stored
    /// value fails to decode.
    pub async fn expire(&self, key: &K, ttl: Duration) -> Result<bool, CacheError> {
        let rearmed = self.rearm_local(key, Lifetime::For(ttl)).await?;
        Ok(rearmed == LocalRearm::Rearmed)
    }

    /// [`Shard::expire`] with the shard's default TTL: the entry's lifespan
    /// restarts from now, and on a shard with no default it stops expiring.
    ///
    /// # Errors
    ///
    /// As [`Shard::expire`].
    pub async fn touch(&self, key: &K) -> Result<bool, CacheError> {
        let rearmed = self.rearm_local(key, Lifetime::Default).await?;
        Ok(rearmed == LocalRearm::Rearmed)
    }

    /// [`Shard::expire`] with no lifespan: the entry stops expiring.
    ///
    /// # Errors
    ///
    /// As [`Shard::expire`].
    pub async fn persist(&self, key: &K) -> Result<bool, CacheError> {
        let rearmed = self.rearm_local(key, Lifetime::Unbounded).await?;
        Ok(rearmed == LocalRearm::Rearmed)
    }

    /// The remaining lifetime of `key`'s live entry in this shard, resident
    /// or spilled, with no disk read and no recency touch. `None` when no
    /// live entry is here, an entry idle past `tti` included. The lifetime
    /// is the TTL's alone: a `tti` idle timeout that ends the entry sooner
    /// is not reflected.
    #[must_use]
    pub fn ttl_of(&self, key: &K) -> Option<Ttl> {
        let key_bytes = encode_key(key).ok()?;
        let hash = engine::hash_key_bytes(key_bytes.as_ref());
        self.engine.ttl_of(key_bytes.as_ref(), hash, self.now_ms())
    }

    /// The absolute expiry `lifetime` stamps now.
    fn deadline_for(&self, lifetime: Lifetime) -> Option<u64> {
        match lifetime {
            Lifetime::For(ttl) => self.expiry_for(Some(ttl)),
            Lifetime::Default => self.expiry_for(None),
            Lifetime::Unbounded => None,
        }
    }

    /// Re-arms `key`'s entry from this shard's own copy: reads the live
    /// value, resident or spilled, and applies it back with `lifetime`'s
    /// deadline at [`rearmed_version`], only while the entry is still at
    /// the version read. An entry that moves on in between has taken a
    /// write stamped after the read, which supersedes the re-arm anyway, so
    /// that still counts as [`LocalRearm::Rearmed`].
    pub(crate) async fn rearm_local(
        &self,
        key: &K,
        lifetime: Lifetime,
    ) -> Result<LocalRearm, CacheError> {
        let key_bytes = encode_key(key)?;
        let hash = engine::hash_key_bytes(key_bytes.as_ref());
        let part = PartId::of_key(key_bytes.as_ref());
        if !verdict::reads_local(&self.current_residency(part)) {
            return Ok(LocalRearm::NotHeld);
        }
        self.wait_for_fan_out_room().await;
        // Read again past the wait and before the record, so the decision
        // sees ownership as it is now, and a pull landing during the read
        // cannot clear a cold mark the decision then trusts.
        let residency = self.current_residency(part);
        if !verdict::reads_local(&residency) {
            return Ok(LocalRearm::NotHeld);
        }
        let resident = self
            .engine
            .record_for(key_bytes.as_ref(), self.now_ms())
            .and_then(|rec| rec.value.map(|encoded| (rec.ver, encoded)));
        let base = match resident {
            Some((ver, encoded)) => {
                let value: V = postcard::from_bytes(&encoded).map_err(CodecError::from)?;
                Some((ver, value, encoded))
            }
            None => self.read_spilled_for_rearm(key_bytes.as_ref(), hash).await,
        };
        let Some((base_ver, value, encoded)) = base else {
            return Ok(match verdict::local_verdict(&residency, false) {
                LocalRead::Miss => LocalRearm::Absent,
                _ => LocalRearm::NotHeld,
            });
        };
        let expires_at_ms = self.deadline_for(lifetime);
        let ver = rearmed_version(base_ver, expires_at_ms);
        let applied = self.engine.apply_if_current(
            (
                hash,
                key.clone(),
                key_bytes,
                ver,
                Incoming::Put {
                    value,
                    expires_at_ms,
                    encoded,
                },
            ),
            base_ver,
            self.resolver.as_ref(),
            self.tombstone_ttl_ms,
            self.tombstone_max_ttl_ms,
            self.now_ms(),
        );
        if let Some(outcome) = applied {
            self.observe_derived(ver);
            self.handle_apply_outcome(outcome, Origin::Local, true);
        }
        Ok(LocalRearm::Rearmed)
    }

    /// The key's spilled value, read back for a re-arm. Always `None`
    /// without the `spill` feature.
    #[cfg_attr(
        not(feature = "spill"),
        allow(
            clippy::unused_async,
            clippy::unused_self,
            reason = "the spill build reads the value off disk here"
        )
    )]
    async fn read_spilled_for_rearm(&self, key_bytes: &[u8], hash: u64) -> Option<(Hlc, V, Bytes)> {
        #[cfg(feature = "spill")]
        {
            self.read_spilled_by_bytes(key_bytes, hash).await
        }
        #[cfg(not(feature = "spill"))]
        {
            let _ = (key_bytes, hash);
            None
        }
    }

    /// Re-arms `key` from `base`, a record an owner answered with, when
    /// this shard holds no warm copy of its own: the same write
    /// [`Shard::rearm_local`] makes, applied here if this shard owns the
    /// key's part and forwarded to the owners otherwise. Returns `false`
    /// for a tombstone or an expired record, which has nothing to re-arm.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::Codec`] if `key` fails to encode or `base`'s
    /// value fails to decode, and [`CacheError::Closed`] if the shard can no
    /// longer forward.
    pub(crate) async fn forward_rearm(
        &self,
        key: K,
        base: WireRecord,
        lifetime: Lifetime,
    ) -> Result<bool, CacheError> {
        let Some(encoded) = base.value else {
            return Ok(false);
        };
        if base
            .expires_at_ms
            .is_some_and(|deadline| deadline <= self.now_ms())
        {
            return Ok(false);
        }
        let key_bytes = encode_key(&key)?;
        let value: V = postcard::from_bytes(&encoded).map_err(CodecError::from)?;
        let expires_at_ms = self.deadline_for(lifetime);
        let ver = rearmed_version(base.ver, expires_at_ms);
        self.wait_for_fan_out_room().await;
        self.apply_or_forward(
            key,
            key_bytes,
            ver,
            Incoming::Put {
                value,
                expires_at_ms,
                encoded,
            },
        )?;
        Ok(true)
    }

    /// Waits for the fan-out backlog to fall under capacity via
    /// [`FanOutQueue::wait_for_room`]. Every async write path calls this; the
    /// synchronous chain never does.
    async fn wait_for_fan_out_room(&self) {
        self.fan_out
            .wait_for_room(self.fan_out_backlog_capacity, self.fan_out_wait_timeout)
            .await;
    }

    /// Applies a versioned write locally if this shard owns `key_bytes`'
    /// bucket, or builds and forwards it to the bucket's current owners
    /// otherwise: the tail every local-write entry point shares,
    /// [`Shard::insert_expiring`] and a coalesced [`Shard::merge`] fold's
    /// flush alike.
    fn apply_or_forward(
        &self,
        key: K,
        key_bytes: Bytes,
        ver: Hlc,
        incoming: Incoming<V>,
    ) -> Result<(), CacheError> {
        if !self.owns_key(key_bytes.as_ref()) {
            let rec = self.forward_write(key, key_bytes, incoming, ver);
            if !self.fan_out.push(FanOutItem::Forward(rec)) {
                return Err(self.closed());
            }
            metrics::counter!("sundog_forwarded_writes_total", "cache" => self.name.to_string())
                .increment(1);
            return Ok(());
        }
        self.apply(key, key_bytes, ver, incoming, Origin::Local);
        Ok(())
    }

    /// Folds `value` into `key` through the configured [`ConflictResolver`]
    /// without a read, applying through the same versioned-apply path as
    /// [`Shard::insert`]. With [`Shard::with_merge_coalesce_window`] left
    /// at its default zero, every call applies at once. With a non-zero
    /// window, consecutive calls to the same key fold into an in-memory
    /// pending value instead, applying once when the window that opened at
    /// the first call elapses (`crate::cache::merge_coalesce_task`,
    /// `Cache::close`, and this shard's `Drop` all guarantee a flush);
    /// replication and every [`Event`] then see exactly one record, the
    /// fold, not each call.
    ///
    /// [`Shard::get`]/[`Shard::get_sync`] never consult a pending fold: it
    /// stays invisible to reads for up to one whole window.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::ValueTooLarge`] if `value`'s wire frame exceeds
    /// the configured frame cap. See [`Shard::with_max_frame`]. Returns
    /// [`CacheError::OverMemoryCeiling`] as [`Shard::insert`] does; a call
    /// folded into a pending value within the window is checked when it is
    /// made, not when the fold flushes.
    pub async fn merge(&self, key: K, value: V) -> Result<(), CacheError> {
        self.check_ceiling()?;
        let key_bytes = encode_key(&key)?;
        let encoded = Bytes::from(postcard::to_stdvec(&value).map_err(CodecError::from)?);
        let wire_size = wire::replicate_frame_len(self.name.len(), key_bytes.len(), encoded.len());
        if wire_size > self.max_frame {
            return Err(CacheError::ValueTooLarge {
                cache: self.name.clone(),
                size: encoded.len(),
                limit: self.max_frame,
            });
        }
        let ver = self.stamp_local();
        if self.merge_window.is_zero() {
            self.wait_for_fan_out_room().await;
            let expires_at_ms = self.expiry_for(None);
            return self.apply_or_forward(
                key,
                key_bytes,
                ver,
                Incoming::Put {
                    value,
                    expires_at_ms,
                    encoded,
                },
            );
        }
        let hash = engine::hash_key_bytes(key_bytes.as_ref());
        let mut stripe = self.pending_merges[engine::stripe_index_from_hash(hash)]
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let PendingMergeStripe {
            entries,
            by_deadline,
            next_seq,
        } = &mut *stripe;
        match entries.entry(key) {
            Entry::Occupied(slot) => {
                self.fold_into_occupied_merge(slot, &key_bytes, encoded, value, ver)?;
            }
            Entry::Vacant(slot) => {
                let deadline_ms = merge_deadline_ms(self.now_ms(), self.merge_window);
                let seq = *next_seq;
                *next_seq += 1;
                by_deadline.insert((deadline_ms, seq), slot.key().clone());
                slot.insert(PendingMerge {
                    key_bytes,
                    value,
                    encoded,
                    expires_at_ms: self.expiry_for(None),
                    ver,
                });
                drop(stripe);
                self.merge_wake.notify_one();
            }
        }
        Ok(())
    }

    /// [`Shard::merge`]'s occupied-slot branch: folds `value` into whatever
    /// this stripe already holds pending, via the resolver's `merge` or `winner` call.
    fn fold_into_occupied_merge(
        &self,
        slot: OccupiedEntry<'_, K, PendingMerge<V>>,
        key_bytes: &Bytes,
        encoded: Bytes,
        value: V,
        ver: Hlc,
    ) -> Result<(), CacheError> {
        let (a, b) = {
            let existing = slot.get();
            (
                RecordView {
                    value: Some(existing.encoded.as_ref()),
                    ver: existing.ver,
                    expires_at_ms: existing.expires_at_ms,
                },
                RecordView {
                    value: Some(encoded.as_ref()),
                    ver,
                    expires_at_ms: self.expiry_for(None),
                },
            )
        };
        let merged = self
            .resolver
            .merges()
            .then(|| self.resolver.merge(key_bytes.as_ref(), a, b))
            .flatten();
        match merged {
            Some(Merged {
                value: merged_bytes,
                expires_at_ms,
            }) => {
                let merged_value: V =
                    postcard::from_bytes(&merged_bytes).map_err(CodecError::from)?;
                let entry = slot.into_mut();
                entry.value = merged_value;
                entry.encoded = merged_bytes;
                entry.expires_at_ms = expires_at_ms;
                entry.ver = ver;
            }
            None => match self.resolver.winner(key_bytes.as_ref(), a, b) {
                Winner::B => {
                    let entry = slot.into_mut();
                    entry.value = value;
                    entry.encoded = encoded;
                    entry.expires_at_ms = self.expiry_for(None);
                    entry.ver = ver;
                }
                Winner::A => {
                    // The incoming call lost outright: nothing about the pending entry
                    // changes, mirroring `resolve_and_rebind`'s `IncomingLoses => None`.
                }
            },
        }
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
    /// the configured frame cap (see [`Shard::insert`]). Returns
    /// [`CacheError::OverMemoryCeiling`], applying nothing, if this shard is
    /// at its ceiling when the call begins; a batch that begins under the
    /// ceiling applies whole.
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
        self.check_ceiling()?;
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

        // A `Mode::Distributed` shard splits off every entry whose bucket
        // this node does not own before the owned group's ordinary
        // by-stripe apply below: a forwarded entry never reaches `engine`,
        // matching `Shard::insert`'s single-write guard.
        let (owned_prepared, forwarded_prepared) = self.partition_owned(prepared, |e| e.0);
        self.wait_for_fan_out_room().await;
        if !self.forward_prepared(forwarded_prepared, |(_, key, key_bytes, ver, v, e, enc)| {
            (key, key_bytes, ver, put_incoming(v, e, enc))
        }) {
            return Err(self.closed());
        }

        self.apply_grouped(owned_prepared, |(hash, key, key_bytes, ver, v, e, enc)| {
            (hash, key, key_bytes, ver, put_incoming(v, e, enc))
        })
        .await;
        match failure {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// Reserves flush-queue admission for one whole
    /// [`Shard::apply_grouped`]/[`ShardOps::apply_remote_batch`] call, before
    /// any stripe lock: sums `spill::spill_record_len` over every
    /// `Incoming::Put`, then makes one `SpillTier::reserve` call bounded by
    /// the tier's `spill_wait_timeout`. Also returns that wait's deadline so
    /// [`Shard::retry_reservation_deficit`] shares the same budget instead of
    /// starting a fresh one. `None` for both when no spill tier is attached
    /// or the reserve times out; the caller then falls back to
    /// [`engine::Engine::apply_many_with_reservation`]'s non-blocking path.
    #[cfg_attr(
        not(feature = "spill"),
        allow(
            clippy::unused_async,
            clippy::unused_self,
            reason = "the tier accessor and `reserve` only exist under feature = \"spill\""
        )
    )]
    async fn reserve_for_batch<'e>(
        &self,
        entries: impl Iterator<Item = (&'e Bytes, &'e Incoming<V>)>,
    ) -> (Option<Reservation<'_>>, Option<std::time::Instant>)
    where
        V: 'e,
    {
        #[cfg(feature = "spill")]
        {
            let Some(tier) = self.engine.spill() else {
                return (None, None);
            };
            let reserved: u64 = entries
                .map(|(key_bytes, incoming)| match incoming {
                    Incoming::Put { encoded, .. } => {
                        u64::from(spill::spill_record_len(key_bytes.len(), encoded.len()))
                    }
                    Incoming::Tombstone => 0,
                })
                .sum();
            let reserved = u32::try_from(reserved).unwrap_or(u32::MAX);
            let budget = tier.spill_wait_timeout_value();
            let deadline = std::time::Instant::now() + budget;
            let reservation = tier.reserve(reserved, budget).await.ok();
            (reservation, Some(deadline))
        }
        #[cfg(not(feature = "spill"))]
        {
            let _ = entries;
            (None, None)
        }
    }

    /// Pays down `deficit` bytes
    /// [`engine::Engine::apply_many_with_reservation`]'s per-bucket loop left
    /// resident (never dropped) when the call's one `Reservation` ran out.
    /// Retries with a fresh [`SpillTier::reserve`] for the remainder, then
    /// another `enforce_capacity_with_reservation` pass, until the deficit
    /// clears or `deadline` passes. `deadline` is the same one
    /// [`Shard::reserve_for_batch`] measured, not a fresh budget, so the
    /// whole call stays bounded by one `spill_wait_timeout` rather than `2x`
    /// it. A no-op when `deficit` is `0`. Once the budget runs out with a
    /// deficit still outstanding, this makes one closing
    /// [`engine::Engine::enforce_capacity`] call (`reservation: None`) so the
    /// deficit is resolved before this call returns, not deferred to
    /// whatever write happens to touch this shard next. A round that held
    /// the whole flush queue and spent none of it ends the retries at once
    /// (see [`retry_round_is_futile`]): the victim it stopped at needs more
    /// than the whole queue, so no later reservation could cover it either.
    /// Every pass leaves the entries `written` holds, the batch's own, for
    /// last.
    #[cfg_attr(
        not(feature = "spill"),
        allow(
            clippy::unused_async,
            clippy::unused_self,
            reason = "the tier accessor and `reserve` only exist under feature = \"spill\""
        )
    )]
    async fn retry_reservation_deficit(
        &self,
        start_bucket: usize,
        deficit: u64,
        deadline: Option<std::time::Instant>,
        written: &mut engine::Written,
    ) {
        #[cfg(feature = "spill")]
        {
            if deficit == 0 {
                return;
            }
            let Some(tier) = self.engine.spill() else {
                return;
            };
            let Some(deadline) = deadline else {
                self.engine
                    .enforce_capacity(start_bucket, written, self.now_ms());
                return;
            };
            let mut deficit = deficit;
            while deficit > 0 {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    // Budget already spent (initial wait or an earlier
                    // iteration): skip straight to the closing fallback.
                    break;
                }
                let bytes = u32::try_from(deficit).unwrap_or(u32::MAX);
                match tier.reserve(bytes, remaining).await {
                    Ok(mut reservation) => {
                        let granted = reservation.remaining();
                        deficit = self.engine.enforce_capacity_with_reservation(
                            start_bucket,
                            Some(&mut reservation),
                            written,
                            self.now_ms(),
                        );
                        if retry_round_is_futile(
                            granted,
                            tier.whole_queue_permits(),
                            reservation.remaining(),
                            deficit,
                        ) {
                            break;
                        }
                    }
                    // `reserve` already recorded
                    // `sundog_spill_wait_timeouts_total`; the fallback below resolves the rest.
                    Err(_) => break,
                }
            }
            if deficit > 0 {
                self.engine
                    .enforce_capacity(start_bucket, written, self.now_ms());
            }
        }
        #[cfg(not(feature = "spill"))]
        {
            let _ = (start_bucket, deficit, deadline, written);
        }
    }

    /// [`Shard::insert_many_expiring`]'s and [`Shard::remove_many`]'s shared apply step: groups by stripe, applies each under one lock.
    ///
    /// Reserves flush-queue admission once for the whole call via
    /// [`Shard::reserve_for_batch`], threading the `Reservation` through
    /// every bucket's [`engine::Engine::apply_many_with_reservation`] call.
    /// [`Shard::retry_reservation_deficit`] then pays down any deficit,
    /// sharing the same deadline rather than a fresh budget.
    async fn apply_grouped<T>(
        &self,
        prepared: Vec<T>,
        to_batch_entry: impl Fn(T) -> (u64, K, Bytes, Hlc, Incoming<V>),
    ) {
        let batch_entries: Vec<(u64, K, Bytes, Hlc, Incoming<V>)> =
            prepared.into_iter().map(to_batch_entry).collect();

        let (reservation, deadline) = self
            .reserve_for_batch(
                batch_entries
                    .iter()
                    .map(|(_, _, key_bytes, _, incoming)| (key_bytes, incoming)),
            )
            .await;

        let mut by_stripe: Vec<Vec<_>> = (0..BUCKET_COUNT).map(|_| Vec::new()).collect();
        for batch_entry in batch_entries {
            by_stripe[engine::stripe_index_from_hash(batch_entry.0)].push(batch_entry);
        }
        let now = self.now_ms();
        let mut applied_keys: Vec<K> = Vec::new();
        let mut written = engine::Written::default();
        // The reservation lives in this block only, so its unspent budget
        // is back with the tier before `retry_reservation_deficit` asks for
        // a fresh one: a retry clamped to the whole flush queue can never be
        // granted while this call still holds part of it.
        let deficit = {
            let mut reservation = reservation;
            let mut deficit = 0u64;
            for (bucket, group) in by_stripe.into_iter().enumerate() {
                if group.is_empty() {
                    continue;
                }
                let (outcomes, bucket_deficit) = self.engine.apply_many_with_reservation(
                    bucket,
                    group,
                    self.resolver.as_ref(),
                    self.tombstone_ttl_ms,
                    self.tombstone_max_ttl_ms,
                    now,
                    reservation.as_mut(),
                    &mut written,
                );
                deficit += bucket_deficit;
                for outcome in outcomes {
                    applied_keys.extend(outcome.key().cloned());
                    self.handle_apply_outcome(outcome, Origin::Local, false);
                }
                self.hand_off_bulk(&mut applied_keys, false);
            }
            deficit
        };
        self.hand_off_bulk(&mut applied_keys, true);
        self.retry_reservation_deficit(0, deficit, deadline, &mut written)
            .await;
    }

    /// Stamps and applies a local tombstone, then fans it out per [`Mode`], as
    /// [`Shard::insert`] does.
    ///
    /// # Errors
    ///
    /// Returns a [`CacheError`] if the key cannot be encoded for the wire.
    pub async fn remove(&self, key: &K) -> Result<(), CacheError> {
        self.wait_for_fan_out_room().await;
        self.remove_sync(key)
    }

    /// [`Shard::remove`] without an async runtime: same fan-out and events.
    ///
    /// # Errors
    ///
    /// As [`Shard::remove`].
    pub fn remove_sync(&self, key: &K) -> Result<(), CacheError> {
        let key_bytes = encode_key(key)?;
        let ver = self.stamp_local();
        if !self.owns_key(key_bytes.as_ref()) {
            let rec = self.forward_write(key.clone(), key_bytes, Incoming::Tombstone, ver);
            if !self.fan_out.push(FanOutItem::Forward(rec)) {
                return Err(self.closed());
            }
            metrics::counter!("sundog_forwarded_writes_total", "cache" => self.name.to_string())
                .increment(1);
            return Ok(());
        }
        self.apply(
            key.clone(),
            key_bytes,
            ver,
            Incoming::Tombstone,
            Origin::Local,
        );
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

        // Same owner-vs-forward split as `Shard::insert_many_expiring`: a
        // forwarded tombstone never reaches `engine`.
        let (owned_prepared, forwarded_prepared) = self.partition_owned(prepared, |e| e.0);
        self.wait_for_fan_out_room().await;
        if !self.forward_prepared(forwarded_prepared, |(_, key, key_bytes, ver)| {
            (key, key_bytes, ver, Incoming::Tombstone)
        }) {
            return Err(self.closed());
        }

        self.apply_grouped(owned_prepared, |(hash, key, key_bytes, ver)| {
            (hash, key, key_bytes, ver, Incoming::Tombstone)
        })
        .await;
        Ok(())
    }

    /// Hands a bulk write's landed keys to the fan-out queue one full
    /// replicate batch ([`REPLICATE_BATCH_COUNT`] keys) at a time, and the
    /// remainder when `flush` is set, so replication streams full frames
    /// while the rest of the write is still applying and a fill costs a
    /// bounded number of frames per peer whatever the machine's speed.
    fn hand_off_bulk(&self, landed: &mut Vec<K>, flush: bool) {
        if landed.is_empty() || (!flush && landed.len() < REPLICATE_BATCH_COUNT) {
            return;
        }
        // A refusal means the cache is closing: these writes landed here,
        // as detached local writes, and fan out nowhere.
        let _ = self
            .fan_out
            .extend(landed.drain(..).map(FanOutItem::Applied));
    }

    /// The error a forwarded write gets once the cache is closing: it
    /// lands nowhere locally, so refusing it is the only honest answer. A
    /// write this node applies itself still succeeds, as a detached local
    /// write.
    fn closed(&self) -> CacheError {
        CacheError::Closed {
            cache: self.name.clone(),
        }
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
    /// never reads `Event`'s `value`. Carries local writes and forwarded
    /// non-owner writes; see [`FanOutQueue`] and [`FanOutItem`].
    pub(crate) fn fan_out_queue(&self) -> Arc<FanOutQueue<FanOutItem<K>>> {
        Arc::clone(&self.fan_out)
    }

    /// Closes this shard's attached spill tier, if `Shard::attach_spill`
    /// ever ran: stops accepting new spills and drops the flusher thread's
    /// channel sender, so its loop drains whatever is queued and exits on
    /// its own, writing no checkpoint. The next `attach_spill` here opens
    /// cold even with `SpillConfig::warm_reopen` on; see
    /// [`Shard::close_spill_checkpointed`] for a warm reopen. A no-op
    /// otherwise, and always a no-op in a non-`spill` build.
    #[cfg_attr(
        not(feature = "spill"),
        allow(
            clippy::unused_self,
            reason = "the tier accessor only exists under feature = \"spill\""
        )
    )]
    pub(crate) fn close_spill(&self) {
        #[cfg(feature = "spill")]
        if let Some(tier) = self.engine.spill() {
            tracing::debug!(
                cache = %self.name,
                bytes_used = tier.bytes_used(),
                "sundog spill: closing tier"
            );
            tier.close();
        }
    }

    /// [`Shard::close_spill`], plus, with `SpillConfig::warm_reopen` on, a
    /// checkpoint of every resident entry to disk before closing, each step a
    /// separate blocking task. The disk write is unconditional, racy against
    /// a concurrent `Cache` clone's remove/overwrite;
    /// `Engine::finalize_checkpoint_writes` re-validates under the stripe
    /// lock afterward and keeps only entries still live at the captured
    /// version, so a losing key is dropped, never resurrected. Without
    /// `warm_reopen`, this is [`Shard::close_spill`] alone: no checkpoint.
    ///
    /// A no-op otherwise, and always a no-op in a non-`spill` build. Called
    /// by `Cache::close` and, for a cache still registered at cluster
    /// shutdown, by `Cluster::shutdown`'s sweep.
    #[cfg_attr(
        not(feature = "spill"),
        allow(
            clippy::unused_self,
            clippy::unused_async,
            reason = "the tier accessor, and everything this awaits, only exist under \
                      feature = \"spill\""
        )
    )]
    #[allow(
        clippy::too_many_lines,
        reason = "one scripted checkpoint sequence, logging and metering each stage in order"
    )]
    pub(crate) async fn close_spill_checkpointed(&self) {
        #[cfg(feature = "spill")]
        if let Some(tier) = self.engine.spill().cloned() {
            tracing::debug!(
                cache = %self.name,
                bytes_used = tier.bytes_used(),
                "sundog spill: closing tier"
            );
            if tier.warm_reopen_value() {
                let now = self.now_ms();
                let engine = Arc::clone(&self.engine);
                let resident =
                    tokio::task::spawn_blocking(move || engine.checkpoint_resident_entries(now))
                        .await
                        .unwrap_or_else(|err| {
                            tracing::warn!(
                                cache = %self.name,
                                error = %err,
                                "sundog spill: resident-entry scan task panicked; the \
                                 checkpoint below covers only already-spilled entries",
                            );
                            Vec::new()
                        });
                tracing::info!(
                    cache = %self.name,
                    count = resident.len(),
                    "sundog spill: checkpoint captured resident entries",
                );
                record_checkpoint_stage(&self.name, "captured", resident.len() as u64);

                let sink = Arc::clone(&self.engine) as Arc<dyn spill::SpillSink>;
                let checkpoint_tier = Arc::clone(&tier);
                let flush_outcome = tokio::task::spawn_blocking(move || {
                    checkpoint_tier.checkpoint_flush(sink.as_ref(), resident)
                })
                .await
                .unwrap_or_else(|err| {
                    tracing::warn!(
                        cache = %self.name,
                        error = %err,
                        "sundog spill: checkpoint flush task panicked; the snapshot below \
                         still covers every already-spilled entry",
                    );
                    spill::CheckpointFlushOutcome::default()
                });
                let newly_written = flush_outcome.written;
                tracing::info!(
                    cache = %self.name,
                    count = newly_written.len(),
                    "sundog spill: checkpoint wrote records",
                );
                record_checkpoint_stage(&self.name, "written", newly_written.len() as u64);
                tracing::info!(
                    cache = %self.name,
                    jobs_drained = flush_outcome.jobs_drained,
                    jobs_abandoned = flush_outcome.jobs_abandoned,
                    "sundog spill: checkpoint drained the flush queue",
                );
                record_checkpoint_stage(&self.name, "drained", flush_outcome.jobs_drained);
                record_checkpoint_stage(&self.name, "abandoned", flush_outcome.jobs_abandoned);

                // Re-validates under the stripe lock: a concurrent write may have
                // changed the key since capture, so a losing entry is dropped here.
                let engine = Arc::clone(&self.engine);
                let (_survivors, finalize_counts) = tokio::task::spawn_blocking(move || {
                    engine.finalize_checkpoint_writes(newly_written, now)
                })
                .await
                .unwrap_or_else(|err| {
                    tracing::warn!(
                        cache = %self.name,
                        error = %err,
                        "sundog spill: checkpoint finalize task panicked; the snapshot below \
                         drops every entry this checkpoint just wrote",
                    );
                    (Vec::new(), engine::CheckpointFinalizeCounts::default())
                });
                tracing::info!(
                    cache = %self.name,
                    kept = finalize_counts.kept,
                    dropped_version_changed = finalize_counts.dropped_version_changed,
                    dropped_tombstoned = finalize_counts.dropped_tombstoned,
                    dropped_gone = finalize_counts.dropped_gone,
                    dropped_expired = finalize_counts.dropped_expired,
                    "sundog spill: checkpoint finalized writes",
                );
                record_checkpoint_stage(&self.name, "kept", finalize_counts.kept);
                record_checkpoint_stage(
                    &self.name,
                    "dropped_version_changed",
                    finalize_counts.dropped_version_changed,
                );
                record_checkpoint_stage(
                    &self.name,
                    "dropped_tombstoned",
                    finalize_counts.dropped_tombstoned,
                );
                record_checkpoint_stage(&self.name, "dropped_gone", finalize_counts.dropped_gone);
                record_checkpoint_stage(
                    &self.name,
                    "dropped_expired",
                    finalize_counts.dropped_expired,
                );

                let engine = Arc::clone(&self.engine);
                let entries = tokio::task::spawn_blocking(move || engine.snapshot_spilled(now))
                    .await
                    .unwrap_or_else(|err| {
                        tracing::warn!(
                            cache = %self.name,
                            error = %err,
                            "sundog spill: spilled-pointer scan task panicked; the \
                             checkpoint below covers only entries this checkpoint just wrote",
                        );
                        Vec::new()
                    });
                tracing::info!(
                    cache = %self.name,
                    count = entries.len(),
                    "sundog spill: checkpoint listed already-spilled entries",
                );
                record_checkpoint_stage(&self.name, "listed", entries.len() as u64);

                // `entries` already includes every survivor:
                // `finalize_checkpoint_writes` flipped each to `Spilled` before this
                // scan ran, so extending again would double-count.
                tracing::info!(
                    cache = %self.name,
                    count = entries.len(),
                    "sundog spill: checkpoint snapshot total",
                );
                record_checkpoint_stage(&self.name, "snapshot", entries.len() as u64);

                let snapshot_tier = Arc::clone(&tier);
                let closed = tokio::task::spawn_blocking(move || {
                    snapshot_tier.write_checkpoint_snapshot(&entries, now);
                    snapshot_tier.close();
                })
                .await;
                if let Err(err) = closed {
                    tracing::warn!(
                        cache = %self.name,
                        error = %err,
                        "sundog spill: checkpoint snapshot task panicked",
                    );
                }
            } else {
                tier.close();
            }
        }
    }

    /// Test-only: takes every byte of the spill tier's flush-queue
    /// admission, waiting up to five seconds for the flusher to hand back
    /// what in-flight jobs hold, so every eviction hand-off is refused for a
    /// full queue until the returned [`spill::Reservation`] drops. Nothing
    /// is queued and nothing is written, so no entry is affected and no drop
    /// is counted.
    ///
    /// # Panics
    ///
    /// Panics without a spill tier, or if the flusher does not drain in
    /// time.
    #[cfg(all(feature = "spill", test, not(feature = "sim")))]
    pub(crate) async fn debug_hold_flush_queue(&self) -> spill::Reservation<'_> {
        let tier = self.engine.spill().expect("a spill tier is attached");
        tier.reserve(tier.whole_queue_permits(), Duration::from_secs(5))
            .await
            .expect("the flusher drains its queue")
    }

    /// Whether this shard's attached spill tier is closed by
    /// Whether this shard's spill tier is closed (by either close path) or
    /// none is attached. Test-only: lets a test check closure directly.
    #[cfg(all(feature = "spill", test, not(feature = "sim")))]
    pub(crate) fn spill_tier_closed(&self) -> bool {
        self.engine.spill().is_none_or(|tier| tier.is_closed())
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

    /// Applies one flushed [`PendingMerge`] through
    /// [`Shard::apply_or_forward`], exactly as if its folded value were
    /// freshly [`Shard::insert`]ed: a fresh version stamped now, so
    /// replication and events see the fold as one write happening at flush
    /// time, not backdated to whichever `merge` call opened the window.
    /// Pushes onto the fan-out queue without awaiting room first, unlike
    /// `Shard::merge`'s immediate-apply branch: this runs from the
    /// merge-coalesce sweep and from `Drop`, where no async runtime may be
    /// left to await on.
    fn flush_one_pending_merge(&self, key: K, pending: PendingMerge<V>) {
        let ver = self.stamp_local();
        let incoming = Incoming::Put {
            value: pending.value,
            expires_at_ms: pending.expires_at_ms,
            encoded: pending.encoded,
        };
        // Best-effort: a forward failure here means the cache is closing,
        // and nothing calling this (the background sweep, `close`, or
        // `Drop`) has anywhere to hand an error to, the same fate an
        // ordinary local write racing a close already gets, documented on
        // `Shard::insert`.
        let _ = self.apply_or_forward(key, pending.key_bytes, ver, incoming);
    }

    /// Flushes every key whose [`Shard::merge`] coalescing window has
    /// elapsed by `now_ms`. Visits [`Shard::pending_merges`]'s
    /// [`BUCKET_COUNT`] stripes one at a time, locking each only long
    /// enough for [`drain_due_deadlines`] to pop that stripe's due entries
    /// out of its deadline-ordered index, never one lock (or one linear
    /// scan) for the whole shard's pending state, so many keys coalescing
    /// at once don't serialize this sweep on each other. The background
    /// sweep (`crate::cache::merge_coalesce_task`) is the only production
    /// caller; `pub(crate)` so `crate::cache` can drive it without owning
    /// the pending map itself.
    pub(crate) fn flush_due_pending_merges(&self, now_ms: u64) {
        for stripe_lock in &*self.pending_merges {
            let due: Vec<(K, PendingMerge<V>)> = {
                let mut stripe = stripe_lock.lock().unwrap_or_else(PoisonError::into_inner);
                drain_due_deadlines(&mut stripe.by_deadline, now_ms)
                    .into_iter()
                    .filter_map(|key| stripe.entries.remove(&key).map(|entry| (key, entry)))
                    .collect()
            };
            for (key, entry) in due {
                self.flush_one_pending_merge(key, entry);
            }
        }
    }

    /// Flushes every currently pending coalesced merge, regardless of
    /// whether its window has elapsed: `crate::cache::Cache::close` and
    /// this shard's own `Drop` call this so nothing folded in stays
    /// invisible past the cache's own lifetime, closing the one gap
    /// [`Shard::flush_due_pending_merges`]'s window-bound leaves. Drains
    /// each stripe in turn, the same one-stripe-at-a-time locking
    /// [`Shard::flush_due_pending_merges`] uses.
    pub(crate) fn flush_all_pending_merges(&self) {
        for stripe_lock in &*self.pending_merges {
            let due: Vec<(K, PendingMerge<V>)> = {
                let mut stripe = stripe_lock.lock().unwrap_or_else(PoisonError::into_inner);
                stripe.by_deadline.clear();
                std::mem::take(&mut stripe.entries).into_iter().collect()
            };
            for (key, entry) in due {
                self.flush_one_pending_merge(key, entry);
            }
        }
    }

    /// The earliest deadline among every currently pending coalesced merge
    /// across every stripe, or `None` when nothing is pending anywhere.
    /// `crate::cache::merge_coalesce_task` sleeps until this to flush right
    /// at the window's edge instead of polling. Each stripe's own earliest
    /// deadline is its `by_deadline` index's first key, no scan of that
    /// stripe's entries needed, so this costs one brief lock per stripe,
    /// not one lock over the whole shard's pending state.
    pub(crate) fn next_pending_merge_deadline_ms(&self) -> Option<u64> {
        self.pending_merges
            .iter()
            .filter_map(|stripe_lock| {
                stripe_lock
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .by_deadline
                    .keys()
                    .next()
                    .map(|&(deadline_ms, _)| deadline_ms)
            })
            .min()
    }

    /// Resolves the next time a [`Shard::merge`] call opens a fresh
    /// coalescing window (a key with nothing already pending), so
    /// `crate::cache::merge_coalesce_task` can recompute its sleep-until
    /// instead of parking past a deadline that did not exist yet when it
    /// last checked.
    pub(crate) fn merge_wake_notified(&self) -> impl Future<Output = ()> + '_ {
        self.merge_wake.notified()
    }

    /// Test-only: `(version, value)` of `key`'s currently pending
    /// [`Shard::merge`] fold, if any, without the caller needing to know
    /// which of [`Self::pending_merges`]'s stripes it hashes into.
    #[cfg(test)]
    fn pending_merge_snapshot(&self, key: &K) -> Option<(Hlc, V)> {
        let key_bytes = encode_key(key).ok()?;
        let hash = engine::hash_key_bytes(key_bytes.as_ref());
        self.pending_merges[engine::stripe_index_from_hash(hash)]
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entries
            .get(key)
            .map(|entry| (entry.ver, entry.value.clone()))
    }
}

impl<K, V> Drop for Shard<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// Flushes every pending coalesced [`Shard::merge`] fold before this
    /// shard's last reference goes away, so a fold nothing ever explicitly
    /// closed still lands rather than vanishing silently. A no-op whenever
    /// nothing is pending, which is every shard
    /// [`Shard::with_merge_coalesce_window`] never touched.
    fn drop(&mut self) {
        self.flush_all_pending_merges();
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

//! Store: moka-backed shards, versioned apply, and the digest machinery that
//! makes anti-entropy cheap. Plan §3, §7, §8.
//!
//! `Shard` intentionally holds no handle to `net::Mesh` — its constructor
//! signature is fixed by `docs/INTERFACES.md` and takes none. Every local
//! mutation (`insert`, `remove`, and `get_or_load`'s fill) publishes an
//! `Origin::Local` [`Event`] on [`Shard::events`]; correlating that stream to
//! wire fan-out (`Mesh::send` per [`Mode`]) is the composition layer's job.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::{self, BoxStream, StreamExt};
use moka::Expiry;
use moka::notification::RemovalCause;
use serde::Serialize;
use serde::de::DeserializeOwned;
use smol_str::SmolStr;
use tokio::sync::{Mutex as AsyncMutex, broadcast};
use xxhash_rust::xxh3::xxh3_64;

use crate::config::ClusterConfig;
use crate::error::{CacheError, CodecError};
use crate::hlc::{Hlc, HlcClock};
use crate::node::NodeId;
use crate::wire::{self, MAX_FRAME, WireRecord};

/// Number of anti-entropy buckets per shard: `bucket(k) = xxh3(key_bytes) & (BUCKET_COUNT - 1)`.
pub const BUCKET_COUNT: usize = 1024;

/// A custom per-entry weigher for size-bounded eviction: `(key, value) ->
/// weight`, passed through to `moka`'s own weigher hook. Boxed since
/// [`crate::cache::CacheBuilder::weigher`] and [`Shard::with_weigher`] need
/// to store one before its concrete closure type is nameable.
pub(crate) type Weigher<K, V> = Box<dyn Fn(&K, &V) -> u32 + Send + Sync>;

/// Upper bound on records per [`WireRecord`] batch yielded by
/// [`ShardOps::snapshot_chunks`] (plan §9) — a chunk breaks earlier than this
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
/// a `Msg::StChunk` (plan §9), splitting on cumulative wire-encoded size as
/// well as [`SNAPSHOT_CHUNK_SIZE`] — a fixed record count alone undercounts
/// for caches whose average value is more than a few KiB, which the plan's
/// own target scale ("values ≤ a few MiB") allows. Each record's
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

/// A named cache's clustering behavior, mirroring Infinispan's cache modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// No cluster traffic at all; a plain local `moka` cache.
    Local,
    /// Every node caches independently; writes broadcast an [`crate::wire::Msg::Invalidate`].
    Invalidation,
    /// Every node holds every entry; writes broadcast the full [`crate::wire::Msg::Replicate`].
    Replicated,
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
/// bytes in and out — the boundary where postcard (de)serialization actually
/// happens (plan §7: "local reads never deserialize"). Implemented by
/// `Shard<K, V>` for any `K`, `V` meeting its bounds; held as
/// `Arc<dyn ShardOps>` in the cluster's cache registry.
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
    /// commutative (plan §4). The single path shared by local writes, live
    /// replication, state transfer, and anti-entropy repair.
    fn apply_remote(&self, rec: WireRecord) -> BoxFuture<'_, ()>;

    /// [`ShardOps::apply_remote`] for a whole batch, grouped by key stripe
    /// (see [`stripe_of`]) and applied under one acquisition per touched
    /// stripe rather than one per record — per-record version checks and digest
    /// bookkeeping are otherwise identical, and an [`crate::store::Event`] is
    /// still emitted per record. The path `net::conn`'s coalesced
    /// [`crate::wire::Msg::ReplicateBatch`] frames, state-transfer chunks,
    /// and anti-entropy pull batches all apply through.
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
    /// round (plan §8).
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
    /// transfer to a joining node (plan §9). Iteration is weakly consistent —
    /// safe because every chunk is applied through the same versioned
    /// [`ShardOps::apply_remote`] path as live traffic.
    fn snapshot_chunks(&self) -> BoxStream<'static, Vec<WireRecord>>;

    /// Garbage-collects tombstones older than the configured
    /// `tombstone_ttl`, keeping the digest and entry set consistent.
    fn gc_tombstones(&self) -> BoxFuture<'_, ()>;

    /// Flushes `moka`'s internal housekeeping. `moka` has no free-running
    /// background sweep — the eviction listener that corrects the digest for
    /// a TTL/size removal (plan §8) only fires as a side effect of a later
    /// cache op, so a shard that goes quiet right after such a removal would
    /// otherwise keep a stale digest forever (plan §13: "eviction is
    /// advisory-timed"). Called periodically by `tombstone_gc_task`
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
/// that saw the same writes in a different order (plan §4, §14 pulled into
/// v1 — see `docs/HOUSE_RULES.md`).
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
/// versions are always a no-op before `winner` is consulted (plan §4: a
/// given `(wall_ms, logical, node)` triple is produced by at most one write
/// ever, so equal versions imply identical records) — a resolver never has
/// to, and never gets asked to, break that tie.
///
/// The default [`LwwResolver`] satisfies all three properties by comparing
/// [`Hlc`] alone, which is already a deterministic, antisymmetric, total,
/// transitive order — this is the behavior every `Shard` had before
/// `ConflictResolver` existed, bit-for-bit.
///
/// A resolver that violates antisymmetry or transitivity is not safe to run
/// on more than one replica: nothing in this crate detects the violation,
/// convergence simply stops holding. Such a resolver is deliberately not
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
    /// that only ever compares `ver`/`expires_at_ms`: `Shard::apply_locked`
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
/// per-key version table, folded into the cached value itself (plan §7) —
/// and its absolute expiry, so every replica converts the same origin-stamped
/// deadline into a local remaining duration (plan §7).
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

/// A tombstone: the version of the delete that created it, and when it
/// becomes eligible for garbage collection.
#[derive(Debug, Clone, Copy)]
struct Tombstone {
    ver: Hlc,
    gc_deadline_ms: u64,
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

/// Converts an absolute epoch-millisecond expiry into the remaining duration
/// from now, for moka's [`Expiry`] hook. A deadline already in the past
/// yields `Duration::ZERO`: expired-on-arrival records still went through
/// version comparison in `Shard::apply` before reaching here (plan §7).
fn remaining_from_absolute(expires_at_ms: Option<u64>) -> Option<Duration> {
    let expires_at_ms = expires_at_ms?;
    Some(Duration::from_millis(
        expires_at_ms.saturating_sub(now_ms()),
    ))
}

/// Converts absolute per-entry expiry into moka's relative-duration `Expiry`
/// hook (plan §7); TTI, by contrast, is configured directly on the
/// `CacheBuilder` since it is local-only (plan §7, §13).
struct AbsoluteExpiry;

impl<K, V> Expiry<K, Arc<Stored<V>>> for AbsoluteExpiry {
    fn expire_after_create(
        &self,
        _key: &K,
        value: &Arc<Stored<V>>,
        _created_at: std::time::Instant,
    ) -> Option<Duration> {
        remaining_from_absolute(value.expires_at_ms)
    }

    fn expire_after_update(
        &self,
        _key: &K,
        value: &Arc<Stored<V>>,
        _updated_at: std::time::Instant,
        _duration_until_expiry: Option<Duration>,
    ) -> Option<Duration> {
        remaining_from_absolute(value.expires_at_ms)
    }
}

/// Wraps a stampede-collapsed loader failure (`Arc<E>`, from moka's
/// `try_get_with`) as a boxable [`std::error::Error`] for [`CacheError::Loader`].
#[derive(Debug)]
struct LoaderFailure<E>(Arc<E>);

impl<E: std::fmt::Display> std::fmt::Display for LoaderFailure<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl<E: std::error::Error + 'static> std::error::Error for LoaderFailure<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.0)
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

/// The bucket a key's postcard bytes hash into: `xxh3(key_bytes) & (BUCKET_COUNT - 1)`.
fn bucket_of(key_bytes: &[u8]) -> u16 {
    let bucket = xxh3_64(key_bytes) & (BUCKET_COUNT as u64 - 1);
    u16::try_from(bucket).expect("invariant: masked to BUCKET_COUNT - 1, always fits in u16")
}

/// Number of independent tombstone-map/apply-serialization lock stripes per
/// shard: a fixed power-of-two smaller than [`BUCKET_COUNT`], big enough to
/// let unrelated keys' writes proceed fully concurrently, small enough to
/// avoid one-mutex-per-bucket overkill. Divides `BUCKET_COUNT` evenly, which
/// [`stripe_of`] relies on.
const TOMBSTONE_STRIPES: usize = 64;

/// A key's tombstone-map/apply-serialization stripe. Reuses [`bucket_of`]'s
/// hash rather than rehashing independently — the entire correctness
/// argument for striping is that a given key always lands in the same
/// stripe, exactly as it always lands in the same digest bucket, so two
/// writes to the same key stay serialized against each other while writes to
/// different keys may now interleave (never a correctness requirement — only
/// same-key mutual exclusion is, see [`Shard::apply_locked`]'s docs).
fn stripe_of(key_bytes: &[u8]) -> usize {
    usize::from(bucket_of(key_bytes)) % TOMBSTONE_STRIPES
}

/// Worst-case postcard-encoded size of an [`Hlc`]: `wall_ms: u64` (up to 10
/// LEB128 bytes) + `logical: u32` (up to 5) + `node: NodeId` (a `u64`, up to
/// 10) = 25, rounded up for headroom.
const HLC_ENCODED_MAX: usize = 32;

/// `xxh3(key_bytes ‖ postcard(ver))` — the digest contribution of one live
/// entry or tombstone (plan §8). Encodes `ver` into a stack buffer rather
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

/// Postcard-encodes `key`, its wire form and digest-hash input alike (plan §7).
///
/// Debug builds assert the encoding round-trips to itself: a key type whose
/// `Serialize` impl isn't canonical (e.g. iteration-order-dependent, as a
/// `HashMap`-typed key would be) would silently corrupt digests and break
/// wire identity (plan §13).
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
        "key's postcard encoding must be canonical/deterministic — no map-typed keys (plan §13)"
    );
    Ok(Bytes::from(bytes))
}

/// A typed named cache: a `moka` cache of `K -> Arc<Stored<V>>` (values
/// `Arc`-wrapped so remote fan-out clones are pointer clones), plus the
/// tombstone map and digest array that back [`ShardOps`]. The typed
/// `Cache<K, V>` handle users hold (`crate::cache`) is a thin wrapper over
/// `Arc<Shard<K, V>>`.
///
/// # Bounds
///
/// `K`'s postcard encoding doubles as its wire form and its digest-hash
/// input, so it must encode deterministically — no map-typed keys (plan §13).
pub struct Shard<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    name: SmolStr,
    mode: Mode,
    cache: moka::future::Cache<K, Arc<Stored<V>>>,
    events: broadcast::Sender<Event<K, V>>,
    /// Internal-only notification channel: just `(key, origin)`, no value
    /// clone, fed by every versioned write regardless of whether anyone is
    /// subscribed to `events` (`fan_out_task`'s sole source of truth for
    /// "what changed" — it re-fetches fresh wire bytes through
    /// `records_for_typed` rather than reading a value off this channel, so
    /// there is nothing here for it to be stale about). Kept separate from
    /// `events` so the app-facing broadcast's `receiver_count()` reflects
    /// only real external subscribers, making the "skip the value clone
    /// when nobody's listening" guard on `events` meaningful.
    fan_out: broadcast::Sender<(K, Origin)>,
    /// Guards only the synchronous HLC bump itself, never held across `.await`.
    clock: StdMutex<HlcClock>,
    /// Also the apply-serialization lock, striped by key ([`stripe_of`]):
    /// every versioned write holds only its key's stripe for that write's
    /// whole read-decide-mutate sequence, moka calls included, so concurrent
    /// applies to the *same* key can't interleave a stale decision — applies
    /// to different keys can interleave freely, which is not a correctness
    /// requirement (see [`Shard::apply_locked`]'s docs). Safe to hold a
    /// stripe across those `.await`s because the eviction listener
    /// (which may fire synchronously from inside them) never touches this
    /// lock — it only does lock-free atomic digest updates. A boxed slice of
    /// [`TOMBSTONE_STRIPES`] independent maps rather than one
    /// `Arc<AsyncMutex<_>>`, so no single acquisition ever spans more than
    /// one stripe.
    tombstones: Arc<[AsyncMutex<HashMap<Bytes, Tombstone>>]>,
    digest: Arc<[AtomicU64]>,
    ttl: Option<Duration>,
    tombstone_ttl_ms: u64,
    resolver: Arc<dyn ConflictResolver>,
    max_frame: usize,
    /// Remembered (alongside `tti` below) so [`Shard::with_weigher`] can
    /// rebuild the `moka` cache from scratch — a weigher can only be
    /// installed at `moka` build time, unlike `resolver`/`max_frame` above.
    max_capacity: u64,
    tti: Option<Duration>,
}

impl<K, V> Shard<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// Builds a new shard. `node` stamps this shard's local writes.
    ///
    /// Tombstone GC uses [`ClusterConfig::default`]'s `tombstone_ttl` until
    /// overridden via [`Shard::with_tombstone_ttl`]: `Shard::new`'s signature
    /// is fixed by `docs/INTERFACES.md` and takes no `ClusterConfig`, so a
    /// live cluster's configured value reaches this shard through that
    /// follow-up call instead.
    ///
    /// # Panics
    ///
    /// The eviction listener installed here panics only if a key already
    /// admitted into the cache (thus already known to postcard-encode) were
    /// to somehow fail re-encoding — not expected to happen in practice.
    #[must_use]
    pub fn new(
        name: SmolStr,
        mode: Mode,
        node: NodeId,
        max_capacity: u64,
        ttl: Option<Duration>,
        tti: Option<Duration>,
    ) -> Self {
        let digest: Arc<[AtomicU64]> = (0..BUCKET_COUNT)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>()
            .into();
        let cache = Self::build_cache(max_capacity, tti, &digest, None);

        Self {
            name,
            mode,
            cache,
            events: broadcast::channel(EVENTS_CAPACITY).0,
            fan_out: broadcast::channel(EVENTS_CAPACITY).0,
            clock: StdMutex::new(HlcClock::new(node)),
            tombstones: (0..TOMBSTONE_STRIPES)
                .map(|_| AsyncMutex::new(HashMap::new()))
                .collect::<Vec<_>>()
                .into(),
            digest,
            ttl,
            tombstone_ttl_ms: duration_ms(ClusterConfig::default().tombstone_ttl),
            resolver: Arc::new(LwwResolver),
            max_frame: MAX_FRAME,
            max_capacity,
            tti,
        }
    }

    /// Builds the underlying `moka` cache with the digest-maintaining
    /// eviction listener wired in (plan §8) and an optional custom weigher.
    /// Shared by [`Shard::new`] and [`Shard::with_weigher`], which rebuilds
    /// from scratch since `moka` only accepts a weigher at build time.
    fn build_cache(
        max_capacity: u64,
        tti: Option<Duration>,
        digest: &Arc<[AtomicU64]>,
        weigher: Option<Weigher<K, V>>,
    ) -> moka::future::Cache<K, Arc<Stored<V>>> {
        let digest_for_listener = Arc::clone(digest);
        let mut builder = moka::future::Cache::<K, Arc<Stored<V>>>::builder()
            .max_capacity(max_capacity)
            .expire_after(AbsoluteExpiry)
            .eviction_listener(
                move |key: Arc<K>, value: Arc<Stored<V>>, cause: RemovalCause| {
                    // Only moka-decided removals land here: TTL/TTI expiry and
                    // size eviction, both driven by housekeeping this function
                    // has no visibility into (plan §8, §13). Anything *we*
                    // cause — a replace or an explicit remove inside `apply`,
                    // `invalidate_local`, or `ShardOps::invalidate` — is already
                    // subtracted there directly, using the value that call
                    // itself observed; relying on this listener for that too
                    // would double-subtract, since moka may batch its
                    // notification arbitrarily far past the call that caused it.
                    if !matches!(cause, RemovalCause::Expired | RemovalCause::Size) {
                        return;
                    }
                    let key_bytes = postcard::to_stdvec(&*key)
                        .expect("invariant: keys admitted into the cache always postcard-encode");
                    let bucket = usize::from(bucket_of(&key_bytes));
                    digest_for_listener[bucket]
                        .fetch_xor(entry_fingerprint(&key_bytes, value.ver), Ordering::Relaxed);
                },
            );
        if let Some(weigher) = weigher {
            builder =
                builder.weigher(move |key: &K, value: &Arc<Stored<V>>| weigher(key, &value.value));
        }
        if let Some(tti) = tti {
            builder = builder.time_to_idle(tti);
        }
        builder.build()
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
    /// [`crate::cache::CacheBuilder::weigher`]. Rebuilds the underlying
    /// `moka` cache (a weigher can only be installed at `moka` build time),
    /// so this must be called immediately after [`Shard::new`], before any
    /// reads or writes reach this shard — harmless at that call site, since
    /// the cache is still empty there.
    #[must_use]
    pub fn with_weigher<W>(mut self, weigher: W) -> Self
    where
        W: Fn(&K, &V) -> u32 + Send + Sync + 'static,
    {
        self.cache = Self::build_cache(
            self.max_capacity,
            self.tti,
            &self.digest,
            Some(Box::new(weigher)),
        );
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

    fn stamp_local(&self) -> Hlc {
        self.clock
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .now(now_ms())
    }

    fn observe_remote(&self, remote: Hlc) {
        self.clock
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .observe(now_ms(), remote);
    }

    fn ttl_expiry(&self) -> Option<u64> {
        self.ttl.map(|d| now_ms().saturating_add(duration_ms(d)))
    }

    /// The versioned-apply core (plan §4): applies `incoming` at `ver` iff
    /// the configured [`ConflictResolver`] picks it over whatever this shard
    /// currently holds for `key` (a live entry or a tombstone), publishing
    /// the resulting [`Event`] on success. Idempotent and commutative — the
    /// single path shared by local writes, replicated writes, state
    /// transfer, and anti-entropy repair. With the default [`LwwResolver`]
    /// this is exactly "apply iff `ver` is newer than the stored version",
    /// unchanged from before `ConflictResolver` existed.
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
        let mut tombstones = self.tombstones[stripe_of(&key_bytes)].lock().await;
        self.apply_locked(&mut tombstones, key, key_bytes, ver, incoming, origin)
            .await;
    }

    /// Whether `incoming` (at `ver`) loses to what's already stored at `sv`
    /// (a live entry's value is `stored_live`, `None` if the current record
    /// is a tombstone) — the [`ConflictResolver`]-consultation half of
    /// [`Shard::apply_locked`]'s decision, factored out only to keep that
    /// function's line count in check; behavior is identical to inlining it.
    /// An equal version is always a loss (see [`Shard::apply`]'s docs), so
    /// this only builds [`RecordView`]s and consults the resolver when
    /// versions actually differ. Value bytes are populated only if
    /// [`ConflictResolver::needs_value_bytes`] says the resolver reads them
    /// — the built-in [`LwwResolver`] never does. Both sides borrow straight
    /// from an already-cached [`Stored::encoded`]/`Incoming::Put`'s `encoded`,
    /// so this never allocates or re-encodes.
    fn incoming_loses(
        &self,
        key_bytes: &Bytes,
        sv: Hlc,
        stored_live: Option<&Stored<V>>,
        ver: Hlc,
        incoming: &Incoming<V>,
    ) -> bool {
        if sv == ver {
            return true;
        }
        let needs_value_bytes = self.resolver.needs_value_bytes();
        let stored_view = RecordView {
            value: stored_live
                .filter(|_| needs_value_bytes)
                .map(|s| s.encoded.as_ref()),
            ver: sv,
            expires_at_ms: stored_live.and_then(|s| s.expires_at_ms),
        };
        let (incoming_encoded, incoming_expires_at_ms) = match incoming {
            Incoming::Put {
                encoded,
                expires_at_ms,
                ..
            } => (Some(encoded), *expires_at_ms),
            Incoming::Tombstone => (None, None),
        };
        let incoming_view = RecordView {
            value: incoming_encoded
                .filter(|_| needs_value_bytes)
                .map(Bytes::as_ref),
            ver,
            expires_at_ms: incoming_expires_at_ms,
        };
        self.resolver.winner(key_bytes, stored_view, incoming_view) == Winner::A
    }

    /// The versioned-apply core, operating under a tombstone-map lock the
    /// caller already holds — shared by [`Shard::apply`] (one record, one
    /// acquisition) and [`ShardOps::apply_remote_batch`] (many records, one
    /// acquisition held across the whole batch, per plan §4's "amortized
    /// lock path"). See [`Shard::apply`]'s docs for the correctness contract;
    /// identical here, just parameterized on the lock the caller supplies.
    async fn apply_locked(
        &self,
        tombstones: &mut HashMap<Bytes, Tombstone>,
        key: K,
        key_bytes: Bytes,
        ver: Hlc,
        incoming: Incoming<V>,
        origin: Origin,
    ) {
        let prior_tombstone = tombstones.get(&key_bytes).copied();
        let stored_live = if prior_tombstone.is_none() {
            self.cache.get(&key).await
        } else {
            None
        };
        let stored_ver = prior_tombstone
            .map(|t| t.ver)
            .or_else(|| stored_live.as_ref().map(|s| s.ver));

        if let Some(sv) = stored_ver
            && self.incoming_loses(&key_bytes, sv, stored_live.as_deref(), ver, &incoming)
        {
            return;
        }

        let bucket = usize::from(bucket_of(&key_bytes));
        let new_fp = entry_fingerprint(&key_bytes, ver);
        let had_live = prior_tombstone.is_none() && stored_ver.is_some();

        // Subtract whatever this write displaces ourselves rather than
        // leaning on the eviction listener: moka may batch a `Replaced`
        // notification past this point (housekeeping is opportunistic), and
        // `digests()` must be correct the instant this call returns. The
        // listener is reserved for evictions moka decides on its own
        // (TTL/TTI/size) that this function has no visibility into.
        if let Some(t) = prior_tombstone {
            self.digest[bucket].fetch_xor(entry_fingerprint(&key_bytes, t.ver), Ordering::Relaxed);
            tombstones.remove(&key_bytes);
        } else if let Some(sv) = stored_ver {
            self.digest[bucket].fetch_xor(entry_fingerprint(&key_bytes, sv), Ordering::Relaxed);
        }
        self.digest[bucket].fetch_xor(new_fp, Ordering::Relaxed);

        match incoming {
            Incoming::Put {
                value,
                expires_at_ms,
                encoded,
            } => {
                let stored = Arc::new(Stored {
                    value,
                    encoded,
                    ver,
                    expires_at_ms,
                });
                self.cache.insert(key.clone(), Arc::clone(&stored)).await;
                let _ = self.fan_out.send((key.clone(), origin));
                if self.events.receiver_count() > 0 {
                    let event = if had_live {
                        Event::Updated {
                            key,
                            value: stored.value.clone(),
                            origin,
                        }
                    } else {
                        Event::Created {
                            key,
                            value: stored.value.clone(),
                            origin,
                        }
                    };
                    let _ = self.events.send(event);
                }
            }
            Incoming::Tombstone => {
                let deadline_ms = now_ms().saturating_add(self.tombstone_ttl_ms);
                tombstones.insert(
                    key_bytes,
                    Tombstone {
                        ver,
                        gc_deadline_ms: deadline_ms,
                    },
                );
                if had_live {
                    let _ = self.cache.remove(&key).await;
                }
                let _ = self.fan_out.send((key.clone(), origin));
                if self.events.receiver_count() > 0 {
                    let _ = self.events.send(Event::Removed { key, origin });
                }
            }
        }
    }

    /// Records the version of a fresh read-through fill for digest/tombstone
    /// bookkeeping (the half of `Shard::apply`'s `Put` arm that isn't
    /// "insert into moka", since moka does that itself inside
    /// `try_get_with_by_ref`). Called from within that call's stampede-
    /// collapsed init future, so it runs at most once per genuine miss.
    async fn record_fresh_load(&self, key_bytes: &Bytes, ver: Hlc) {
        let mut tombstones = self.tombstones[stripe_of(key_bytes)].lock().await;
        let bucket = usize::from(bucket_of(key_bytes));
        if let Some(t) = tombstones.remove(key_bytes) {
            self.digest[bucket].fetch_xor(entry_fingerprint(key_bytes, t.ver), Ordering::Relaxed);
        }
        self.digest[bucket].fetch_xor(entry_fingerprint(key_bytes, ver), Ordering::Relaxed);
    }

    /// Reads `key`, without triggering read-through. Tombstones never enter
    /// `moka`, so a deleted key simply isn't present here.
    pub async fn get(&self, key: &K) -> Option<V> {
        self.cache.get(key).await.map(|stored| stored.value.clone())
    }

    /// The number of live entries this node currently holds, with `moka`'s
    /// pending housekeeping flushed first so completed expirations and
    /// evictions are reflected rather than estimated.
    pub async fn entry_count(&self) -> u64 {
        self.cache.run_pending_tasks().await;
        self.cache.entry_count()
    }

    /// One pass over tombstones and live entries, distributing each into the
    /// requested buckets. Every requested bucket appears in the result, an
    /// empty list included — an anti-entropy responder must report even the
    /// buckets where it holds nothing, so the initiator learns to push.
    async fn collect_buckets(&self, wanted: &HashSet<u16>) -> BucketEntries {
        let mut by_bucket: HashMap<u16, Vec<(Bytes, Hlc)>> =
            wanted.iter().map(|&bucket| (bucket, Vec::new())).collect();
        for stripe in self.tombstones.iter() {
            let stripe = stripe.lock().await;
            for (key, tomb) in stripe.iter() {
                if let Some(slot) = by_bucket.get_mut(&bucket_of(key)) {
                    slot.push((key.clone(), tomb.ver));
                }
            }
        }
        for (key, stored) in &self.cache {
            let Ok(key_bytes) = postcard::to_stdvec(&*key) else {
                continue;
            };
            if let Some(slot) = by_bucket.get_mut(&bucket_of(&key_bytes)) {
                slot.push((Bytes::from(key_bytes), stored.ver));
            }
        }
        let mut out: Vec<_> = by_bucket.into_iter().collect();
        out.sort_unstable_by_key(|&(bucket, _)| bucket);
        out
    }

    /// Reads `key`, invoking `loader` on a miss. Concurrent callers racing on
    /// the same missing key are collapsed into one `loader` call (moka
    /// `get_with`'s stampede protection).
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
        let stored = self
            .cache
            .try_get_with_by_ref(key, async {
                let value = loader(key).await?;
                let ver = self.stamp_local();
                self.record_fresh_load(&key_bytes, ver).await;
                let encoded = Bytes::from(
                    postcard::to_stdvec(&value)
                        .expect("invariant: a value returned by the loader postcard-encodes"),
                );
                let stored = Arc::new(Stored {
                    value,
                    encoded,
                    ver,
                    expires_at_ms: self.ttl_expiry(),
                });
                let _ = self.fan_out.send((key.clone(), Origin::Local));
                if self.events.receiver_count() > 0 {
                    let _ = self.events.send(Event::Created {
                        key: key.clone(),
                        value: stored.value.clone(),
                        origin: Origin::Local,
                    });
                }
                Ok(stored)
            })
            .await
            .map_err(|err: Arc<E>| CacheError::Loader(Box::new(LoaderFailure(err))))?;
        Ok(stored.value.clone())
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
        let key_bytes = encode_key(&key)?;
        let encoded = Bytes::from(postcard::to_stdvec(&value).map_err(CodecError::from)?);
        let ver = self.stamp_local();
        let expires_at_ms = self.ttl_expiry();
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

    /// [`Shard::insert`] for many entries, grouped by key stripe
    /// ([`stripe_of`]) and applied under one acquisition per touched stripe
    /// rather than one per entry — the "amortized lock path" a bulk local
    /// fill wants, bounded to a single stripe at a time so unrelated local
    /// writers and inbound applies to other stripes aren't blocked for the
    /// whole batch. Each entry still gets its own [`Hlc`] stamp and its own
    /// [`Event`]; this is not a transaction, just a cheaper way to apply many
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
        let mut prepared = Vec::new();
        for (key, value) in entries {
            let key_bytes = encode_key(&key)?;
            let encoded = Bytes::from(postcard::to_stdvec(&value).map_err(CodecError::from)?);
            let ver = self.stamp_local();
            let expires_at_ms = self.ttl_expiry();
            let wire_size =
                wire::replicate_frame_len(self.name.len(), key_bytes.len(), encoded.len());
            if wire_size > self.max_frame {
                return Err(CacheError::ValueTooLarge {
                    cache: self.name.clone(),
                    size: encoded.len(),
                    limit: self.max_frame,
                });
            }
            prepared.push((key, key_bytes, ver, value, expires_at_ms, encoded));
        }

        let mut by_stripe: Vec<Vec<_>> = (0..TOMBSTONE_STRIPES).map(|_| Vec::new()).collect();
        for entry in prepared {
            by_stripe[stripe_of(&entry.1)].push(entry);
        }
        for (stripe_idx, group) in by_stripe.into_iter().enumerate() {
            if group.is_empty() {
                continue;
            }
            let mut tombstones = self.tombstones[stripe_idx].lock().await;
            for (key, key_bytes, ver, value, expires_at_ms, encoded) in group {
                self.apply_locked(
                    &mut tombstones,
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

    /// Drops the local copy of `key` without writing a tombstone or fanning
    /// out — an escape hatch for tests and manual cache-busting; the entry
    /// may reappear on the next replicated write or anti-entropy round.
    pub async fn invalidate_local(&self, key: &K) {
        let Ok(key_bytes) = postcard::to_stdvec(key) else {
            return;
        };
        // Serializes against `apply` on the same key (same stripe); `remove`
        // (not `invalidate`) so the departing version comes back directly
        // rather than through the eviction listener, which may batch its
        // notification arbitrarily far past this call.
        let _guard = self.tombstones[stripe_of(&key_bytes)].lock().await;
        if let Some(old) = self.cache.remove(key).await {
            let bucket = usize::from(bucket_of(&key_bytes));
            self.digest[bucket]
                .fetch_xor(entry_fingerprint(&key_bytes, old.ver), Ordering::Relaxed);
        }
    }

    /// Subscribes to this shard's change events.
    #[must_use]
    pub fn events(&self) -> broadcast::Receiver<Event<K, V>> {
        self.events.subscribe()
    }

    /// Subscribes to this shard's lightweight `(key, origin)` fan-out
    /// notifications — `fan_out_task`'s cheaper alternative to
    /// subscribing on [`Shard::events`] directly, since it never reads
    /// `Event`'s `value` at all.
    pub(crate) fn fan_out_events(&self) -> broadcast::Receiver<(K, Origin)> {
        self.fan_out.subscribe()
    }

    /// [`ShardOps::records_for`], but for callers that already hold typed
    /// `K`s (`cluster::fan_out_batch`, re-fetching for keys it just read off
    /// its own `Event<K, V>`s) — skips the postcard-decode-back-to-`K` step
    /// the `Bytes`-keyed trait method needs, since there is no encode/decode
    /// round trip to begin with.
    pub(crate) async fn records_for_typed(&self, keys: &[K]) -> Vec<WireRecord> {
        let mut pairs = Vec::with_capacity(keys.len());
        for key in keys {
            if let Ok(key_bytes) = encode_key(key) {
                pairs.push((key.clone(), key_bytes));
            }
        }
        self.records_for_pairs(pairs).await
    }

    /// Shared implementation of [`ShardOps::records_for`] and
    /// [`Shard::records_for_typed`]: snapshots the tombstone entries for
    /// `pairs`' keys grouped by stripe ([`stripe_of`]), one acquisition per
    /// touched stripe (rather than one per key, and never
    /// more than one stripe locked at a time), then reads live entries via
    /// `moka`'s own concurrent-safe `get` outside any lock — mirroring
    /// [`Shard::collect_buckets`]'s per-stripe scan pattern.
    async fn records_for_pairs(&self, pairs: Vec<(K, Bytes)>) -> Vec<WireRecord> {
        let mut by_stripe: Vec<Vec<usize>> = (0..TOMBSTONE_STRIPES).map(|_| Vec::new()).collect();
        for (idx, (_, key_bytes)) in pairs.iter().enumerate() {
            by_stripe[stripe_of(key_bytes)].push(idx);
        }
        let mut tomb_snapshot: HashMap<Bytes, Tombstone> = HashMap::new();
        for (stripe_idx, indices) in by_stripe.into_iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let stripe = self.tombstones[stripe_idx].lock().await;
            for idx in indices {
                let key_bytes = &pairs[idx].1;
                if let Some(t) = stripe.get(key_bytes) {
                    tomb_snapshot.insert(key_bytes.clone(), *t);
                }
            }
        }
        let mut out = Vec::with_capacity(pairs.len());
        for (key, key_bytes) in pairs {
            if let Some(t) = tomb_snapshot.get(&key_bytes) {
                out.push(WireRecord {
                    key: key_bytes,
                    value: None,
                    ver: t.ver,
                    expires_at_ms: None,
                });
                continue;
            }
            if let Some(stored) = self.cache.get(&key).await {
                out.push(WireRecord {
                    key: key_bytes,
                    value: Some(stored.encoded.clone()),
                    ver: stored.ver,
                    expires_at_ms: stored.expires_at_ms,
                });
            }
        }
        out
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
            // Grouping by raw key bytes' stripe needs no decode: `stripe_of`
            // reuses `bucket_of`'s hash, which only ever looks at the wire
            // bytes. Preserves each group's relative order (same key always
            // lands in the same stripe, in the order it appeared in `recs`),
            // which is all `apply_locked`'s per-key serialization needs —
            // order between different keys was never a requirement.
            let mut by_stripe: Vec<Vec<WireRecord>> =
                (0..TOMBSTONE_STRIPES).map(|_| Vec::new()).collect();
            for rec in recs {
                self.observe_remote(rec.ver);
                by_stripe[stripe_of(&rec.key)].push(rec);
            }
            for (stripe_idx, group) in by_stripe.into_iter().enumerate() {
                if group.is_empty() {
                    continue;
                }
                let mut tombstones = self.tombstones[stripe_idx].lock().await;
                for rec in group {
                    let Ok(key) = postcard::from_bytes::<K>(&rec.key) else {
                        tracing::warn!(cache = %self.name, "apply_remote_batch: undecodable key bytes");
                        continue;
                    };
                    let origin = Origin::Remote(rec.ver.node);
                    match rec.value {
                        Some(value_bytes) => {
                            let Ok(value) = postcard::from_bytes::<V>(&value_bytes) else {
                                tracing::warn!(
                                    cache = %self.name,
                                    "apply_remote_batch: undecodable value bytes"
                                );
                                continue;
                            };
                            self.apply_locked(
                                &mut tombstones,
                                key,
                                rec.key,
                                rec.ver,
                                Incoming::Put {
                                    value,
                                    expires_at_ms: rec.expires_at_ms,
                                    // The verbatim bytes just received off
                                    // the wire, not a fresh re-encode of
                                    // `value` — decoded once above only to
                                    // satisfy the resolver/typed-cache
                                    // boundary.
                                    encoded: value_bytes,
                                },
                                origin,
                            )
                            .await;
                        }
                        None => {
                            self.apply_locked(
                                &mut tombstones,
                                key,
                                rec.key,
                                rec.ver,
                                Incoming::Tombstone,
                                origin,
                            )
                            .await;
                        }
                    }
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

            let tombstones = self.tombstones[stripe_of(&key)].lock().await;
            let prior_tombstone = tombstones.get(&key).copied();
            let stored_ver = match prior_tombstone {
                Some(t) => Some(t.ver),
                None => self.cache.get(&decoded_key).await.map(|s| s.ver),
            };
            if stored_ver.is_some_and(|sv| ver <= sv) {
                return;
            }
            let had_live = prior_tombstone.is_none() && stored_ver.is_some();
            let removed = if had_live {
                self.cache.remove(&decoded_key).await
            } else {
                None
            };
            drop(tombstones);
            if let Some(old) = removed {
                let bucket = usize::from(bucket_of(&key));
                self.digest[bucket].fetch_xor(entry_fingerprint(&key, old.ver), Ordering::Relaxed);
                let _ = self.events.send(Event::Removed {
                    key: decoded_key,
                    origin: Origin::Remote(ver.node),
                });
            }
        })
    }

    fn digests(&self) -> BoxFuture<'_, Vec<(u16, u64)>> {
        Box::pin(async move {
            self.digest
                .iter()
                .enumerate()
                .map(|(bucket, d)| {
                    let bucket = u16::try_from(bucket)
                        .expect("invariant: bucket index < BUCKET_COUNT fits in u16");
                    (bucket, d.load(Ordering::Relaxed))
                })
                .collect()
        })
    }

    fn bucket_entries(&self, bucket: u16) -> BoxFuture<'_, Vec<(Bytes, Hlc)>> {
        Box::pin(async move {
            self.collect_buckets(&HashSet::from([bucket]))
                .await
                .pop()
                .map(|(_, entries)| entries)
                .unwrap_or_default()
        })
    }

    fn entries_for_buckets(&self, buckets: Vec<u16>) -> BoxFuture<'_, BucketEntries> {
        Box::pin(async move {
            let wanted: HashSet<u16> = buckets.into_iter().collect();
            self.collect_buckets(&wanted).await
        })
    }

    fn records_for(&self, keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>> {
        Box::pin(async move {
            let pairs: Vec<(K, Bytes)> = keys
                .into_iter()
                .filter_map(|key_bytes| {
                    postcard::from_bytes::<K>(&key_bytes)
                        .ok()
                        .map(|key| (key, key_bytes))
                })
                .collect();
            self.records_for_pairs(pairs).await
        })
    }

    fn snapshot_chunks(&self) -> BoxStream<'static, Vec<WireRecord>> {
        let cache = self.cache.clone();
        let tombstones = Arc::clone(&self.tombstones);
        let fut = async move {
            let mut records: Vec<WireRecord> = cache
                .iter()
                .filter_map(|(key, stored)| {
                    let key_bytes = postcard::to_stdvec(&*key).ok()?;
                    Some(WireRecord {
                        key: Bytes::from(key_bytes),
                        value: Some(stored.encoded.clone()),
                        ver: stored.ver,
                        expires_at_ms: stored.expires_at_ms,
                    })
                })
                .collect();
            for stripe in tombstones.iter() {
                records.extend(stripe.lock().await.iter().map(|(key_bytes, t)| WireRecord {
                    key: key_bytes.clone(),
                    value: None,
                    ver: t.ver,
                    expires_at_ms: None,
                }));
            }
            chunk_records_for_snapshot(records)
        };
        Box::pin(stream::once(fut).flat_map(stream::iter))
    }

    fn gc_tombstones(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let now = now_ms();
            let digest = &self.digest;
            for stripe in self.tombstones.iter() {
                stripe.lock().await.retain(|key_bytes, t| {
                    let keep = t.gc_deadline_ms > now;
                    if !keep {
                        let bucket = usize::from(bucket_of(key_bytes));
                        digest[bucket]
                            .fetch_xor(entry_fingerprint(key_bytes, t.ver), Ordering::Relaxed);
                    }
                    keep
                });
            }
        })
    }

    fn run_pending_tasks(&self) -> BoxFuture<'_, ()> {
        Box::pin(self.cache.run_pending_tasks())
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

    #[test]
    fn mode_is_copy_and_comparable() {
        assert_eq!(Mode::Local, Mode::Local);
        assert_ne!(Mode::Local, Mode::Replicated);
    }

    #[test]
    fn origin_distinguishes_local_from_remote() {
        assert_ne!(Origin::Local, Origin::Remote(NodeId::from(1)));
    }

    #[test]
    fn remaining_from_absolute_converts_and_floors_at_zero() {
        assert_eq!(remaining_from_absolute(None), None);
        let past = remaining_from_absolute(Some(1)).expect("some");
        assert_eq!(past, Duration::ZERO);
        let future_ms = now_ms() + 5_000;
        let d = remaining_from_absolute(Some(future_ms)).expect("some");
        assert!(d <= Duration::from_secs(5) && d > Duration::from_secs(4));
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

    /// The first `u32` from `1` on whose stripe differs from key `0`'s —
    /// used by the striping tests below to get two real keys guaranteed to
    /// serialize independently, without depending on any hash's exact
    /// output.
    fn two_keys_in_different_stripes() -> (u32, u32) {
        let a = 0u32;
        let a_stripe = stripe_of(&key_bytes(&a));
        let b = (1u32..10_000)
            .find(|b| stripe_of(&key_bytes(b)) != a_stripe)
            .expect("some key among the first 10,000 lands in a different stripe from key 0");
        (a, b)
    }

    /// Direct proof of the striping property: an `insert` for a key in one
    /// stripe must not block on another stripe's lock being held.
    /// Would fail if the striping regressed back to a single global mutex —
    /// see the companion `_blocks_` test below for the negative control that
    /// confirms this held lock is the very one `apply_locked` acquires.
    #[tokio::test]
    async fn insert_to_a_different_stripe_proceeds_while_another_stripe_is_locked() {
        let s = shard::<u32, String>(1);
        let (key_a, key_b) = two_keys_in_different_stripes();
        let stripe_a = stripe_of(&key_bytes(&key_a));

        let _guard = s.tombstones[stripe_a].lock().await;
        let result =
            tokio::time::timeout(Duration::from_millis(200), s.insert(key_b, "b".into())).await;
        assert!(
            result.is_ok(),
            "an insert to a different stripe must not block on another stripe's held lock"
        );
    }

    /// Negative control for the test above: an `insert` for a key *in* the
    /// held stripe must block until that stripe's lock is released, proving
    /// the held guard exercises the same lock `apply_locked` acquires (so
    /// the "proceeds" test above is actually exercising cross-stripe
    /// concurrency, not just a no-op lock).
    #[tokio::test]
    async fn insert_to_the_same_stripe_blocks_while_that_stripe_is_locked() {
        let s = shard::<u32, String>(1);
        let (key_a, _) = two_keys_in_different_stripes();
        let stripe_a = stripe_of(&key_bytes(&key_a));

        let _guard = s.tombstones[stripe_a].lock().await;
        let result =
            tokio::time::timeout(Duration::from_millis(200), s.insert(key_a, "a".into())).await;
        assert!(
            result.is_err(),
            "an insert to a key whose stripe is already held must block until released"
        );
    }

    /// Spawns concurrent `insert` tasks against keys chosen to land in
    /// distinct stripes and confirms every one lands — the end-to-end
    /// counterpart to the lock-holding tests above, exercising real
    /// concurrent scheduling (not just a manually held guard) across
    /// [`TOMBSTONE_STRIPES`]-many independent stripes at once.
    #[tokio::test]
    async fn concurrent_inserts_across_many_stripes_all_land() {
        let s = Arc::new(shard::<u32, String>(1));
        let mut keys = vec![0u32];
        let mut seen_stripes = HashSet::from([stripe_of(&key_bytes(&0u32))]);
        let mut candidate = 1u32;
        while seen_stripes.len() < TOMBSTONE_STRIPES.min(16) {
            if seen_stripes.insert(stripe_of(&key_bytes(&candidate))) {
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

    /// Plan §7: lifespan travels as an *absolute* `expires_at_ms`, computed
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

        // See `run_pending_tasks_flushes_a_quiet_shards_stale_ttl_eviction_digest`
        // for why this waits well past both the 50ms TTL and moka's timer
        // wheel granularity floor.
        tokio::time::sleep(Duration::from_millis(1300)).await;
        assert_eq!(a.get(&1).await, None, "the origin's own copy must expire");
        assert_eq!(
            b.get(&1).await,
            None,
            "b must expire the entry from the wire-carried deadline alone"
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
        {
            let stripe = stripe_of(&key_bytes(&1u32));
            let mut tombstones = s.tombstones[stripe].lock().await;
            for t in tombstones.values_mut() {
                t.gc_deadline_ms = 0;
            }
        }

        ShardOps::gc_tombstones(&s).await;
        assert!(
            ShardOps::records_for(&s, vec![key_bytes(&1u32)])
                .await
                .is_empty()
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

        // `moka`'s hierarchical timer wheel only sweeps a per-entry TTL
        // expiry once real time has advanced roughly a full level-0 span
        // (~1s) past scheduling it, regardless of how short the configured
        // TTL itself is — so this waits past both the 50ms TTL and that
        // wheel granularity floor before the single `run_pending_tasks`
        // call below that this test exists to prove matters.
        tokio::time::sleep(Duration::from_millis(1300)).await;

        // Logical expiry is visible to reads immediately once it's past —
        // before moka's eviction listener has run to correct the digest.
        assert!(
            ShardOps::bucket_entries(&s, bucket_of(&key_bytes(&1u32)))
                .await
                .is_empty()
        );

        // Without this periodic flush, moka's eviction listener would never
        // fire on this now-quiet shard, and the digest would disagree with
        // `bucket_entries` forever.
        ShardOps::run_pending_tasks(&s).await;
        assert_digest_matches_full_recompute(&s).await;
    }

    /// One full pass over live entries + tombstones (not the 1024 separate
    /// `bucket_entries` calls that would otherwise be needed), so this stays
    /// cheap enough to call after every op in a several-hundred-op sequence.
    async fn assert_digest_matches_full_recompute<K, V>(s: &Shard<K, V>)
    where
        K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
        V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    {
        let mut expected = vec![0u64; BUCKET_COUNT];
        for (key, stored) in &s.cache {
            let key_bytes = postcard::to_stdvec(&*key).expect("test key encodes");
            expected[usize::from(bucket_of(&key_bytes))] ^=
                entry_fingerprint(&key_bytes, stored.ver);
        }
        for stripe in s.tombstones.iter() {
            for (key_bytes, t) in stripe.lock().await.iter() {
                expected[usize::from(bucket_of(key_bytes))] ^= entry_fingerprint(key_bytes, t.ver);
            }
        }
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
    async fn with_weigher_drives_mokas_weighted_size() {
        let s = shard::<u32, Vec<u8>>(1).with_weigher(|_key: &u32, value: &Vec<u8>| {
            u32::try_from(value.len()).unwrap_or(u32::MAX)
        });
        s.insert(1, vec![0u8; 7]).await.expect("insert");
        s.cache.run_pending_tasks().await;
        assert_eq!(
            s.cache.weighted_size(),
            7,
            "a custom weigher must drive moka's weighted size, not the default of 1 per entry"
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

    /// The permutation-convergence property (plan §4, §11), specialized to a
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
}

#[cfg(test)]
mod prop_tests;

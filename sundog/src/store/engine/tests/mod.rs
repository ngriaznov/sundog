//! Unit tests for the engine: the CRDT write path, reads, expiry, eviction, loads and the spill tier.

use std::time::Duration;

#[cfg(feature = "spill")]
use super::apply::prefetch_spilled_conflict_bytes;
#[cfg(feature = "spill")]
use super::apply::read_stored_live;
use super::apply::{ApplyCtx, EntryKey, apply_locked};
use super::evict::{
    EVICTION_BATCH, EVICTION_BATCH_SAMPLE, EVICTION_SAMPLE, EvictOutcome, defer_to_flusher,
    deficit_once_still_over_capacity, eviction_batch_size,
};
use super::expiry::{
    LONG_TTL, MAX_INLINE_TTL_MS, MAX_STAMP_LEAD_MS, MAX_TTI_MS, NEVER_EXPIRES, decode_expiry,
    encode_expiry, idle_elapsed_ms, lapse_cause,
};
use super::housekeeping::should_shrink_stripe;
use super::inflight::fill_supersedes;
use super::prefold::BatchEntry;
#[cfg(feature = "spill")]
use super::promote::{claimed_total, promotion_fits};
use super::record::{
    ENTRY_OVERHEAD_BYTES, build_key_only_record, build_resident_record, decode_key, decode_value,
    record_key_bytes, record_value, record_value_bytes, split_record,
};
use super::*;
use crate::explain::{Lapse, LocalRecord};
use crate::hlc::HlcClock;
use crate::node::NodeId;
use crate::store::{
    BucketPart, ConflictResolver, Incoming, KeyVersion, LwwResolver, Merged, PartId, Quiescence,
    RecordView, Ttl, Winner, later_deadline,
};
use xxhash_rust::xxh3::xxh3_64;

fn engine_u32_string(max_capacity: u64, tti: Option<Duration>) -> Engine<u32, String> {
    Engine::new(max_capacity, tti, None, None)
}

fn key_bytes(key: u32) -> Bytes {
    Bytes::from(postcard::to_stdvec(&key).expect("u32 encodes"))
}

/// [`key_bytes`]'s `String`-keyed counterpart, for a key long enough to
/// force [`Record`]'s heap path.
#[cfg(feature = "spill")]
fn string_key_bytes(key: &str) -> Bytes {
    Bytes::from(postcard::to_stdvec(key).expect("a string key encodes"))
}

fn hlc(wall_ms: u64, node: u64) -> Hlc {
    Hlc {
        wall_ms,
        logical: 0,
        node: NodeId::from(node),
    }
}

/// Always empty: every raw `apply_locked` test site below skips real spill prefetch.
#[cfg(feature = "spill")]
fn empty_prefetched() -> &'static HashMap<SpillLoc, Bytes> {
    static EMPTY: std::sync::OnceLock<HashMap<SpillLoc, Bytes>> = std::sync::OnceLock::new();
    EMPTY.get_or_init(HashMap::new)
}

/// An [`ApplyCtx`] against `engine`'s own weigher/tti, no spill prefetch or tier, for a test driving [`apply_locked`] directly.
fn test_ctx<'a, K, V>(
    engine: &'a Engine<K, V>,
    bucket: usize,
    hash: u64,
    resolver: &'a dyn ConflictResolver,
    tombstone_ttl_ms: u64,
    tombstone_max_ttl_ms: u64,
    now_ms: u64,
) -> ApplyCtx<'a, K, V> {
    ApplyCtx {
        digest_bucket: &engine.digest[digest_slot(bucket, part_index_from_hash(hash))],
        total_weight: &engine.total_weight,
        live_count: &engine.live_count,
        weigher: engine.weigher.as_ref(),
        tti_ms: engine.tti_ms,
        resolver,
        #[cfg(feature = "spill")]
        prefetched_spilled: empty_prefetched(),
        #[cfg(feature = "spill")]
        spill: None,
        tombstone_ttl_ms,
        tombstone_max_ttl_ms,
        now_ms,
        held_only: false,
    }
}

fn put<K, V>(
    engine: &Engine<K, V>,
    key: K,
    key_bytes: Bytes,
    value: V,
    ver: Hlc,
    expires_at_ms: Option<u64>,
    now_ms: u64,
) -> ApplyOutcome<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    put_with_resolver(
        engine,
        key,
        key_bytes,
        value,
        ver,
        expires_at_ms,
        now_ms,
        &LwwResolver,
    )
}

/// [`put`]'s tombstone counterpart, for a test that needs a real
/// tombstone in place rather than a live entry.
fn tombstone<K, V>(engine: &Engine<K, V>, key: K, key_bytes: Bytes, ver: Hlc, now_ms: u64)
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let hash = hash_key_bytes(key_bytes.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let resolver = LwwResolver;
    #[cfg(feature = "spill")]
    let prefetched_spilled = prefetch_spilled_conflict_bytes(
        &engine.stripes[bucket],
        std::iter::once((hash, &key_bytes)),
        &resolver,
        engine.spill().map(Arc::as_ref),
        engine.tti_ms,
        now_ms,
    );
    let mut stripe = engine.stripes[bucket].write();
    let ctx = ApplyCtx {
        digest_bucket: &engine.digest[digest_slot(bucket, part_index_from_hash(hash))],
        total_weight: &engine.total_weight,
        live_count: &engine.live_count,
        weigher: engine.weigher.as_ref(),
        tti_ms: engine.tti_ms,
        resolver: &resolver,
        #[cfg(feature = "spill")]
        prefetched_spilled: &prefetched_spilled,
        #[cfg(feature = "spill")]
        spill: engine.spill().map(Arc::as_ref),
        tombstone_ttl_ms: 60_000,
        tombstone_max_ttl_ms: 600_000,
        now_ms,
        held_only: false,
    };
    let _ = apply_locked(
        &mut stripe,
        &ctx,
        EntryKey {
            hash,
            key,
            key_bytes,
        },
        ver,
        Incoming::Tombstone,
    );
}

/// [`put`]'s counterpart for a test that needs a resolver other than
/// [`LwwResolver`], in particular one whose [`ConflictResolver::merge`]
/// can return `Some`.
#[allow(clippy::too_many_arguments)]
fn put_with_resolver<K, V>(
    engine: &Engine<K, V>,
    key: K,
    key_bytes: Bytes,
    value: V,
    ver: Hlc,
    expires_at_ms: Option<u64>,
    now_ms: u64,
    resolver: &dyn ConflictResolver,
) -> ApplyOutcome<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let hash = hash_key_bytes(key_bytes.as_ref());
    let encoded = Bytes::from(postcard::to_stdvec(&value).expect("test value encodes"));
    let bucket = stripe_index_from_hash(hash);
    // Mirrors `Engine::apply_many`'s own prefetch (see `put` above), so
    // a merge against an already-spilled stored side, exercised
    // directly through this helper, reads its bytes back off-lock
    // exactly as the real write path does.
    #[cfg(feature = "spill")]
    let prefetched_spilled = prefetch_spilled_conflict_bytes(
        &engine.stripes[bucket],
        std::iter::once((hash, &key_bytes)),
        resolver,
        engine.spill().map(Arc::as_ref),
        engine.tti_ms,
        now_ms,
    );
    let ctx = ApplyCtx {
        digest_bucket: &engine.digest[digest_slot(bucket, part_index_from_hash(hash))],
        total_weight: &engine.total_weight,
        live_count: &engine.live_count,
        weigher: engine.weigher.as_ref(),
        tti_ms: engine.tti_ms,
        resolver,
        #[cfg(feature = "spill")]
        prefetched_spilled: &prefetched_spilled,
        #[cfg(feature = "spill")]
        spill: engine.spill().map(Arc::as_ref),
        tombstone_ttl_ms: 60_000,
        tombstone_max_ttl_ms: 600_000,
        now_ms,
        held_only: false,
    };
    let (outcome, displaced_spilled) = {
        let mut stripe = engine.stripes[bucket].write();
        apply_locked(
            &mut stripe,
            &ctx,
            EntryKey {
                hash,
                key,
                key_bytes,
            },
            ver,
            Incoming::Put {
                value,
                expires_at_ms,
                encoded,
            },
        )
    };
    engine.note_spill_departure(displaced_spilled);
    outcome
}

/// A one-entry [`BatchEntry`] putting `value` at `ver`, expiring at
/// `expires_at_ms`.
fn put_entry(
    key: u32,
    value: &str,
    ver: Hlc,
    expires_at_ms: Option<u64>,
) -> BatchEntry<u32, String> {
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let encoded = Bytes::from(postcard::to_stdvec(value).expect("test value encodes"));
    (
        hash,
        key,
        kb,
        ver,
        Incoming::Put {
            value: value.to_string(),
            expires_at_ms,
            encoded,
        },
    )
}

fn remaining(ms: u64) -> Ttl {
    Ttl::Remaining(Duration::from_millis(ms))
}

/// `last_access_ms` of key `key`'s live entry.
fn last_access(engine: &Engine<u32, String>, key: u32) -> u32 {
    let kb = key_bytes(key);
    let stripe = engine
        .stripe_lock(stripe_index_from_hash(hash_key_bytes(kb.as_ref())))
        .read();
    stripe
        .live
        .iter()
        .find(|live| record_key(&live.record) == kb.as_ref())
        .expect("the entry is present")
        .last_access_ms
        .load(Ordering::Relaxed)
}

/// Test-only resolver: merges by set union of postcard-decoded
/// `BTreeSet<String>` values, a trivial join-semilattice standing in for
/// a real CRDT like [`super::super::crdt::PnCounter`] or
/// [`super::super::crdt::OrSet`]: content that grows whenever either
/// side contributes an element the other lacks, unlike
/// [`MaxStringResolver`]'s `max`, which can reproduce one side's bytes
/// exactly and so cannot exercise the case below. `merges()` is `true`:
/// besides the mint-arm test below, this is also `apply_many_prefold`'s
/// resolver, since set union is a real join-semilattice (commutative,
/// associative, idempotent) and so a fold-order-independence property
/// test can lean on it.
struct UnionSetResolver;

impl ConflictResolver for UnionSetResolver {
    fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        if a.ver >= b.ver { Winner::A } else { Winner::B }
    }

    fn merges(&self) -> bool {
        true
    }

    fn merge(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Option<Merged> {
        let (Some(av), Some(bv)) = (a.value, b.value) else {
            return None;
        };
        let da: std::collections::BTreeSet<String> =
            postcard::from_bytes(av).expect("test value decodes");
        let db: std::collections::BTreeSet<String> =
            postcard::from_bytes(bv).expect("test value decodes");
        let merged: std::collections::BTreeSet<String> = da.union(&db).cloned().collect();
        Some(Merged {
            value: Bytes::from(postcard::to_stdvec(&merged).expect("merged value encodes")),
            expires_at_ms: later_deadline(a.expires_at_ms, b.expires_at_ms),
        })
    }
}

fn string_set(elems: &[&str]) -> std::collections::BTreeSet<String> {
    elems.iter().map(|s| (*s).to_string()).collect()
}

/// A pair of `u32` keys, scanned from `1` up to `limit`, that land in
/// the same bucket, plus that bucket's own index.
fn same_bucket_pair(limit: u32) -> (u32, u32, usize) {
    let mut by_bucket: HashMap<usize, Vec<u32>> = HashMap::new();
    for k in 1..limit {
        let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(k).as_ref()));
        let group = by_bucket.entry(bucket).or_default();
        group.push(k);
        if group.len() == 2 {
            return (group[0], group[1], bucket);
        }
    }
    panic!("no colliding pair found within the first {limit} keys");
}

trait PairedDigests {
    fn recompute_digests_paired(&self) -> Vec<(u16, u64)>;
}
impl<K, V> PairedDigests for Engine<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    fn recompute_digests_paired(&self) -> Vec<(u16, u64)> {
        let parts = self.recompute_digests();
        (0..BUCKET_COUNT)
            .map(|bucket| {
                let digest =
                    (0..PART_COUNT).fold(0u64, |acc, part| acc ^ parts[digest_slot(bucket, part)]);
                (u16::try_from(bucket).expect("fits"), digest)
            })
            .collect()
    }
}

mod accounting;
mod apply;
mod entry;
mod evict;
mod expiry;
mod held_only;
mod housekeeping;
mod inflight;
mod read;
mod release;
mod serve;

#[cfg(feature = "spill")]
mod spill_payload;

/// Coverage for [`Engine::compact`]: how its cursor rotates across
/// calls, that it never locks more of the engine than its budget needs,
/// and that a spilled entry is never handed to the resolver.
/// [`crate::store::crdt::PnCounter`]/[`crate::store::crdt::PnCounterResolver`]
/// (real types, not a stub) exercise the resolver-hook plumbing end to
/// end, since a fake resolver would only prove this method calls
/// *something*, not that it hands the real hook real arguments.
mod compact_sweep;

/// Property coverage for [`merge_version`] alone: every claim its doc
/// comment's convergence argument rests on is checked here rather than
/// assumed from the construction, namely that the mint arm strictly
/// dominates both inputs, that different merged bytes mint different
/// nodes, that each of the other three arms returns exactly what the
/// rule says, and that a mint is always recognizable as one (never a
/// real node's id).
mod merge_version;

/// Coverage for the fold decision: the pure [`fold_sides`] and
/// [`lapsed_before`], and [`apply_locked`] under a merging resolver when a
/// side is expired, idle or lapsed before the other side's write, so a dead
/// value never folds into a live one.
mod merge_expiry;

/// Coverage for [`Engine::apply_many`]'s pre-fold: that a non-merging
/// resolver never triggers it, that a run never crosses a tombstone,
/// and, the property [`prefold_batch`]'s and [`fold_run`]'s docs argue
/// for from the resolver's join-semilattice contract, that
/// pre-fold changes only how many `apply_locked` calls a batch costs,
/// never the `(version, bytes)` it leaves stored or the set of keys it
/// reports a real outcome for.
mod apply_many_prefold;

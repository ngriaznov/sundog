//! Unit tests for the engine: the CRDT write path, reads, expiry, eviction, loads and the spill tier.

use std::collections::HashSet;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use super::*;
use crate::hlc::HlcClock;
use crate::node::NodeId;
use crate::store::LwwResolver;

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

#[test]
fn lapse_cause_prefers_expiry_and_defaults_to_idle() {
    assert_eq!(lapse_cause(Some(100), 100), Lapse::Expired);
    assert_eq!(lapse_cause(Some(100), 101), Lapse::Expired);
    assert_eq!(
        lapse_cause(Some(101), 100),
        Lapse::Idle,
        "an entry absent before its expiry went idle"
    );
    assert_eq!(lapse_cause(None, 100), Lapse::Idle);
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
            expires_at_ms: None,
        })
    }
}

fn string_set(elems: &[&str]) -> std::collections::BTreeSet<String> {
    elems.iter().map(|s| (*s).to_string()).collect()
}

#[test]
fn read_of_an_expired_entry_is_absent_before_any_sweep() {
    let engine = engine_u32_string(u64::MAX, None);
    let _ = put(&engine, 1, key_bytes(1), "a".into(), hlc(1, 1), Some(50), 0);
    assert_eq!(engine.get(&1, 0), Some("a".to_string()));
    // Past the deadline, but no sweep has run yet.
    assert_eq!(
        engine.get(&1, 100),
        None,
        "an expired entry reads as absent immediately"
    );
}

#[test]
fn contains_key_and_record_for_read_an_expired_entry_as_absent_before_any_sweep() {
    let engine = engine_u32_string(u64::MAX, None);
    let kb = key_bytes(1);
    let _ = put(&engine, 1, kb.clone(), "a".into(), hlc(1, 1), Some(50), 0);
    assert!(engine.contains_key(&1, 0));
    assert!(engine.record_for(kb.as_ref(), 0).is_some());

    // Past the deadline, but no sweep has run yet.
    assert!(
        !engine.contains_key(&1, 100),
        "an expired entry reads as absent from contains_key immediately"
    );
    assert!(
        engine.record_for(kb.as_ref(), 100).is_none(),
        "record_for treats an expired live entry as absent, no sweep needed"
    );
}

#[test]
fn for_each_key_skips_an_expired_entry_and_visits_every_live_key_exactly_once() {
    let engine = engine_u32_string(u64::MAX, None);
    let _ = put(&engine, 1, key_bytes(1), "a".into(), hlc(1, 1), Some(50), 0);
    let _ = put(&engine, 2, key_bytes(2), "b".into(), hlc(1, 1), None, 0);
    let _ = put(&engine, 3, key_bytes(3), "c".into(), hlc(1, 1), None, 0);

    let mut visited = Vec::new();
    engine.for_each_key(100, |k| visited.push(k));
    visited.sort_unstable();
    assert_eq!(
        visited,
        vec![2, 3],
        "the expired key is skipped, every live key is visited exactly once"
    );
}

#[test]
fn sweep_removes_expired_entries_and_corrects_the_digest() {
    let engine = engine_u32_string(u64::MAX, None);
    let _ = put(&engine, 1, key_bytes(1), "a".into(), hlc(1, 1), Some(50), 0);
    let _ = put(&engine, 2, key_bytes(2), "b".into(), hlc(1, 1), None, 0);

    engine.sweep(100);
    assert_eq!(engine.get(&1, 100), None);
    assert_eq!(engine.get(&2, 100), Some("b".to_string()));
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
    let (entries, _) = engine.debug_totals();
    assert_eq!(entries, 1, "only the non-expired entry survives the sweep");
}

#[test]
fn removing_a_long_ttl_entry_also_clears_its_side_table_row() {
    let engine = engine_u32_string(u64::MAX, None);
    let kb = key_bytes(1);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let long_deadline = MAX_INLINE_TTL_MS + 5_000;
    let _ = put(
        &engine,
        1,
        kb.clone(),
        "a".into(),
        hlc(1, 1),
        Some(long_deadline),
        0,
    );
    assert!(
        engine
            .stripe_lock(bucket)
            .read()
            .long_ttl
            .contains_key(kb.as_ref()),
        "a long TTL write leaves its absolute deadline in the side table"
    );

    engine.invalidate_local(kb.as_ref(), hash);

    assert!(
        !engine
            .stripe_lock(bucket)
            .read()
            .long_ttl
            .contains_key(kb.as_ref()),
        "removing the entry clears its side-table row"
    );
}

#[test]
fn sweep_of_an_expired_long_ttl_entry_clears_its_side_table_row() {
    let engine = engine_u32_string(u64::MAX, None);
    let kb = key_bytes(1);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let deadline = MAX_INLINE_TTL_MS + 5_000;
    let _ = put(
        &engine,
        1,
        kb.clone(),
        "a".into(),
        hlc(1, 1),
        Some(deadline),
        0,
    );
    assert!(
        engine
            .stripe_lock(bucket)
            .read()
            .long_ttl
            .contains_key(kb.as_ref())
    );

    engine.sweep(deadline + 1);

    assert_eq!(engine.get(&1, deadline + 1), None, "swept: reads as absent");
    assert!(
        !engine
            .stripe_lock(bucket)
            .read()
            .long_ttl
            .contains_key(kb.as_ref()),
        "sweep clears the side-table row of what it expires"
    );
}

#[test]
fn sweep_of_a_tti_idle_long_ttl_entry_clears_its_side_table_row() {
    // A TTI far shorter than the long-TTL deadline, so this entry
    // goes idle and is swept through absent_at's TTI branch first.
    let engine = engine_u32_string(u64::MAX, Some(Duration::from_millis(100)));
    let kb = key_bytes(1);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let deadline = MAX_INLINE_TTL_MS + 5_000;
    let _ = put(
        &engine,
        1,
        kb.clone(),
        "a".into(),
        hlc(1, 1),
        Some(deadline),
        0,
    );
    assert!(
        engine
            .stripe_lock(bucket)
            .read()
            .long_ttl
            .contains_key(kb.as_ref()),
        "a long TTL write leaves its absolute deadline in the side table"
    );
    let (entries_before, _) = engine.debug_totals();
    assert_eq!(entries_before, 1);

    // Idle past the 100ms TTI, nowhere near `deadline`.
    engine.sweep(500);

    let (entries_after, _) = engine.debug_totals();
    assert_eq!(entries_after, 0, "sweep evicts the tti-idle entry");
    assert_eq!(engine.get(&1, 500), None, "swept: reads as absent");
    assert!(
        !engine
            .stripe_lock(bucket)
            .read()
            .long_ttl
            .contains_key(kb.as_ref()),
        "sweep clears the side-table row of what it evicts for idleness, \
         not only what it expires"
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one scenario per named overwrite site (apply_put, complete_fresh_load, \
              compact_replace_if_current), matching the spec's single test name; splitting \
              it would only scatter the shared long_deadline setup"
)]
fn overwriting_a_long_ttl_key_clears_its_stale_side_table_row() {
    let long_deadline = MAX_INLINE_TTL_MS + 5_000;

    // `apply_put`'s overwrite path.
    {
        let engine = engine_u32_string(u64::MAX, None);
        let kb = key_bytes(1);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let _ = put(
            &engine,
            1,
            kb.clone(),
            "a".into(),
            hlc(1, 1),
            Some(long_deadline),
            0,
        );
        assert!(
            engine
                .stripe_lock(bucket)
                .read()
                .long_ttl
                .contains_key(kb.as_ref())
        );
        let _ = put(&engine, 1, kb.clone(), "b".into(), hlc(2, 1), None, 0);
        assert!(
            !engine
                .stripe_lock(bucket)
                .read()
                .long_ttl
                .contains_key(kb.as_ref()),
            "apply_put clears the displaced entry's stale side-table row"
        );
    }

    // `complete_fresh_load`'s overwrite path.
    {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 2u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let JoinOutcome::Owner(inflight) = engine.miss_or_join(&kb, hash, 0) else {
            panic!("first caller becomes the owner");
        };
        let _ = put(
            &engine,
            key,
            kb.clone(),
            "a".into(),
            hlc(1, 1),
            Some(long_deadline),
            0,
        );
        assert!(
            engine
                .stripe_lock(bucket)
                .read()
                .long_ttl
                .contains_key(kb.as_ref())
        );
        let encoded = Bytes::from(postcard::to_stdvec("loaded").expect("encode"));
        let _ = engine.complete_fresh_load(
            FreshLoad {
                key: &key,
                key_bytes: &kb,
                hash,
                ver: hlc(2, 1),
                value: "loaded".to_string(),
                encoded,
                expires_at_ms: None,
            },
            0,
            &inflight,
        );
        assert!(
            !engine
                .stripe_lock(bucket)
                .read()
                .long_ttl
                .contains_key(kb.as_ref()),
            "complete_fresh_load clears the displaced entry's stale side-table row"
        );
    }

    // compact_replace_if_current's path: a wall_ms jump alone can make
    // a stored LONG_TTL row stale by making the deadline inline again.
    {
        let engine = engine_u32_string(u64::MAX, None).count_resident_bytes();
        let key = 3u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let _ = put(
            &engine,
            key,
            kb.clone(),
            "a".into(),
            hlc(1, 1),
            Some(long_deadline),
            0,
        );
        assert!(
            engine
                .stripe_lock(bucket)
                .read()
                .long_ttl
                .contains_key(kb.as_ref())
        );
        let new_ver = hlc(long_deadline - 10, 1);
        let replaced = engine.compact_replace_if_current(CompactReplace {
            key: &key,
            key_bytes: kb.as_ref(),
            hash,
            expected_ver: hlc(1, 1),
            new_ver,
            value: "compacted".to_string(),
            encoded: Bytes::from(postcard::to_stdvec("compacted").expect("encode")),
        });
        assert!(replaced);
        assert_eq!(
            engine.resident_bytes(),
            Some(engine.recount_resident_bytes()),
            "an in-place compaction moves the resident byte total by its heap delta"
        );
        assert!(
            !engine
                .stripe_lock(bucket)
                .read()
                .long_ttl
                .contains_key(kb.as_ref()),
            "compact_replace_if_current clears a row a wall_ms jump made stale"
        );
        assert_eq!(
            engine.get(&key, long_deadline - 1),
            Some("compacted".to_string()),
            "still readable just before its unchanged absolute deadline"
        );
        assert_eq!(
            engine.get(&key, long_deadline + 1),
            None,
            "expired exactly at its unchanged absolute deadline"
        );
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one scenario per named overwrite site (apply_put, complete_fresh_load), \
              matching the spec's single test name; splitting it would only scatter the \
              shared long_deadline setup"
)]
fn overwriting_a_short_ttl_key_with_a_long_ttl_value_installs_its_side_table_row() {
    let long_deadline = MAX_INLINE_TTL_MS + 5_000;

    // `apply_put`'s overwrite path: a short-TTL live entry, then a fresh
    // write past `MAX_INLINE_TTL_MS` lands on the same key.
    {
        let engine = engine_u32_string(u64::MAX, None);
        let kb = key_bytes(1);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let _ = put(
            &engine,
            1,
            kb.clone(),
            "a".into(),
            hlc(1, 1),
            Some(5_000),
            0,
        );
        assert!(
            !engine
                .stripe_lock(bucket)
                .read()
                .long_ttl
                .contains_key(kb.as_ref()),
            "the displaced entry's TTL fit inline: no side-table row yet"
        );
        let _ = put(
            &engine,
            1,
            kb.clone(),
            "b".into(),
            hlc(2, 1),
            Some(long_deadline),
            0,
        );
        assert_eq!(
            engine
                .stripe_lock(bucket)
                .read()
                .long_ttl
                .get(kb.as_ref())
                .copied(),
            Some(long_deadline),
            "apply_put installs a fresh side-table row when the new write is the long one"
        );
        assert_eq!(
            engine.get(&1, long_deadline - 1),
            Some("b".to_string()),
            "still readable just before its long deadline"
        );
        assert_eq!(
            engine.get(&1, long_deadline + 1),
            None,
            "expired exactly at its long deadline"
        );
    }

    // `complete_fresh_load`'s overwrite path: a no-TTL live entry, then a
    // fresh load installs a long TTL.
    {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 2u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let JoinOutcome::Owner(inflight) = engine.miss_or_join(&kb, hash, 0) else {
            panic!("first caller becomes the owner");
        };
        let _ = put(&engine, key, kb.clone(), "a".into(), hlc(1, 1), None, 0);
        assert!(
            !engine
                .stripe_lock(bucket)
                .read()
                .long_ttl
                .contains_key(kb.as_ref()),
            "the displaced entry never expires: no side-table row yet"
        );
        let encoded = Bytes::from(postcard::to_stdvec("loaded").expect("encode"));
        let _ = engine.complete_fresh_load(
            FreshLoad {
                key: &key,
                key_bytes: &kb,
                hash,
                ver: hlc(2, 1),
                value: "loaded".to_string(),
                encoded,
                expires_at_ms: Some(long_deadline),
            },
            0,
            &inflight,
        );
        assert_eq!(
            engine
                .stripe_lock(bucket)
                .read()
                .long_ttl
                .get(kb.as_ref())
                .copied(),
            Some(long_deadline),
            "complete_fresh_load installs a fresh side-table row when the new write is the long one"
        );
        assert_eq!(
            engine.get(&key, long_deadline - 1),
            Some("loaded".to_string()),
            "still readable just before its long deadline"
        );
        assert_eq!(
            engine.get(&key, long_deadline + 1),
            None,
            "expired exactly at its long deadline"
        );
    }
}

#[test]
fn next_expiry_ms_skip_logic_leaves_stripes_with_nothing_due_untouched() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let _ = put(&engine, key, kb, "a".into(), hlc(1, 1), Some(10_000), 0);
    assert_eq!(engine.stripe_lock(bucket).read().next_expiry_ms, 10_000);

    // Sweeping well before the deadline does not touch this stripe.
    engine.sweep(1);
    assert_eq!(engine.get(&key, 1), Some("a".to_string()));
    assert_eq!(engine.stripe_lock(bucket).read().next_expiry_ms, 10_000);
}

#[test]
fn tti_idle_eviction() {
    let engine = engine_u32_string(u64::MAX, Some(Duration::from_millis(100)));
    let _ = put(&engine, 1, key_bytes(1), "a".into(), hlc(1, 1), None, 0);
    assert_eq!(
        engine.get(&1, 50),
        Some("a".to_string()),
        "read at 50ms refreshes idle clock"
    );
    assert_eq!(
        engine.get(&1, 140),
        Some("a".to_string()),
        "90ms since the last read, still alive"
    );
    assert_eq!(engine.get(&1, 400), None, "idle past the 100ms TTI");
}

#[test]
fn weighted_capacity_eviction_stays_within_bound_and_evicts_colder_first() {
    // "Coldest first" holds only within one bucket. Find several keys in
    // the same bucket so a single sampling pass sees all of them.
    let target_bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(0).as_ref()));
    let mut same_bucket_keys = vec![0u32];
    let mut candidate = 1u32;
    while same_bucket_keys.len() < 5 {
        if stripe_index_from_hash(hash_key_bytes(key_bytes(candidate).as_ref())) == target_bucket {
            same_bucket_keys.push(candidate);
        }
        candidate += 1;
    }

    let weigher: Weigher<u32, String> =
        Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
    // Five 5-unit entries under a 20-unit cap: exactly one goes.
    let engine = Engine::<u32, String>::new(20, None, Some(weigher), None);
    for (i, &k) in same_bucket_keys.iter().enumerate() {
        let now = u64::try_from(i).expect("small") * 100;
        let _ = put(
            &engine,
            k,
            key_bytes(k),
            "x".repeat(5),
            hlc(u64::from(k) + 1, 1),
            None,
            now,
        );
    }
    let (entries_before, weight_before) = engine.debug_totals();
    assert_eq!(entries_before, 5);
    assert_eq!(weight_before, 25);

    engine.enforce_capacity(target_bucket, &mut Written::default(), 500);

    let (entries_after, weight_after) = engine.debug_totals();
    assert!(
        weight_after <= 20,
        "total weight {weight_after} stays within the 20-unit cap"
    );
    assert_eq!(
        entries_after, 4,
        "exactly one 5-unit entry is evicted to clear a 5-unit overage"
    );
    assert_eq!(
        engine.get(&same_bucket_keys[0], 1_000),
        None,
        "the coldest (first-inserted) entry is the one evicted"
    );
    for &k in &same_bucket_keys[1..] {
        assert!(engine.get(&k, 1_000).is_some(), "hotter entries survive");
    }
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

#[test]
fn release_buckets_removes_every_live_entry_and_tombstone_and_resets_the_digest() {
    let engine = engine_u32_string(u64::MAX, None);
    let (live_key, tomb_key, bucket) = same_bucket_pair(100_000);
    let bucket_u16 = u16::try_from(bucket).expect("invariant: bucket < BUCKET_COUNT");

    let _ = put(
        &engine,
        live_key,
        key_bytes(live_key),
        "live".to_string(),
        hlc(1, 1),
        None,
        0,
    );
    tombstone(&engine, tomb_key, key_bytes(tomb_key), hlc(1, 1), 0);

    let removed = engine.release_buckets(&[bucket_u16]);

    assert_eq!(removed, 2, "one live entry and one tombstone removed");
    assert_eq!(engine.get(&live_key, 0), None, "the live entry is gone");
    assert!(
        engine.collect_buckets(&[bucket_u16], 0)[0].1.is_empty(),
        "the tombstone is gone too: nothing is left in the released bucket"
    );
    assert_eq!(engine.debug_totals(), (0, 0));
    for part in 0..PART_COUNT {
        assert_eq!(
            engine.digest[digest_slot(bucket, part)].load(Ordering::Relaxed),
            0,
            "every part digest in the released bucket is reset to zero"
        );
    }
}

#[test]
fn release_buckets_leaves_a_different_buckets_digest_and_entry_untouched() {
    let engine = engine_u32_string(u64::MAX, None);
    let (live_key, _, released_bucket) = same_bucket_pair(100_000);
    let other_key = (1..100_000u32)
        .find(|&k| stripe_index_from_hash(hash_key_bytes(key_bytes(k).as_ref())) != released_bucket)
        .expect("a distinct bucket is found quickly");
    let other_bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(other_key).as_ref()));
    let other_part = part_index_from_hash(hash_key_bytes(key_bytes(other_key).as_ref()));

    let _ = put(
        &engine,
        live_key,
        key_bytes(live_key),
        "live".to_string(),
        hlc(1, 1),
        None,
        0,
    );
    let _ = put(
        &engine,
        other_key,
        key_bytes(other_key),
        "untouched".to_string(),
        hlc(1, 1),
        None,
        0,
    );
    let digest_before =
        engine.digest[digest_slot(other_bucket, other_part)].load(Ordering::Relaxed);

    let removed =
        engine.release_buckets(&[u16::try_from(released_bucket).expect("invariant: fits")]);

    assert_eq!(removed, 1);
    assert_eq!(
        engine.digest[digest_slot(other_bucket, other_part)].load(Ordering::Relaxed),
        digest_before,
        "an untouched bucket's digest is unaffected"
    );
    assert_eq!(
        engine.get(&other_key, 0),
        Some("untouched".to_string()),
        "a different bucket's entry survives release"
    );
}

/// The first two keys of `0..limit` sharing a bucket but not a part.
fn same_bucket_different_parts(limit: u32) -> (u32, u32) {
    let mut by_bucket: HashMap<usize, Vec<u32>> = HashMap::new();
    for k in 0..limit {
        let hash = hash_key_bytes(key_bytes(k).as_ref());
        let keys = by_bucket.entry(stripe_index_from_hash(hash)).or_default();
        if let Some(&first) = keys.iter().find(|&&other| {
            part_index_from_hash(hash_key_bytes(key_bytes(other).as_ref()))
                != part_index_from_hash(hash)
        }) {
            return (first, k);
        }
        keys.push(k);
    }
    panic!("no same-bucket, different-part pair within the first {limit} keys");
}

#[test]
fn release_parts_drops_only_the_named_part_and_resets_only_its_digest() {
    let engine = engine_u32_string(u64::MAX, None);
    let (released_key, kept_key) = same_bucket_different_parts(100_000);
    let released = PartId::from_hash(hash_key_bytes(key_bytes(released_key).as_ref()));
    let kept = PartId::from_hash(hash_key_bytes(key_bytes(kept_key).as_ref()));
    assert_eq!(released.bucket(), kept.bucket());
    let bucket = usize::from(released.bucket());

    let _ = put(
        &engine,
        released_key,
        key_bytes(released_key),
        "released".to_string(),
        hlc(1, 1),
        None,
        0,
    );
    let _ = put(
        &engine,
        kept_key,
        key_bytes(kept_key),
        "kept".to_string(),
        hlc(1, 1),
        None,
        0,
    );
    let kept_digest =
        engine.digest[digest_slot(bucket, usize::from(kept.part()))].load(Ordering::Relaxed);
    assert_ne!(kept_digest, 0);

    let removed = engine.release_parts(&[released]);

    assert_eq!(removed, 1, "only the released part's entry goes");
    assert_eq!(engine.get(&released_key, 0), None);
    assert_eq!(engine.get(&kept_key, 0), Some("kept".to_string()));
    assert_eq!(
        engine.digest[digest_slot(bucket, usize::from(released.part()))].load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        engine.digest[digest_slot(bucket, usize::from(kept.part()))].load(Ordering::Relaxed),
        kept_digest,
        "the bucket's other part keeps its digest"
    );
    assert_eq!(engine.held_parts(), vec![kept]);
}

#[test]
fn release_parts_drops_a_released_part_s_tombstone_too() {
    let engine = engine_u32_string(u64::MAX, None);
    let (released_key, kept_key) = same_bucket_different_parts(100_000);
    let released = PartId::from_hash(hash_key_bytes(key_bytes(released_key).as_ref()));
    tombstone(&engine, released_key, key_bytes(released_key), hlc(1, 1), 0);
    tombstone(&engine, kept_key, key_bytes(kept_key), hlc(1, 1), 0);
    assert_eq!(engine.held_parts().len(), 2, "a tombstone holds its part");

    assert_eq!(engine.release_parts(&[released]), 1);
    assert_eq!(
        engine.held_parts(),
        vec![PartId::from_hash(hash_key_bytes(
            key_bytes(kept_key).as_ref()
        ))]
    );
}

#[test]
fn release_parts_of_every_part_of_a_bucket_is_releasing_the_bucket() {
    let engine = engine_u32_string(u64::MAX, None);
    let (live_key, tomb_key, bucket) = same_bucket_pair(100_000);
    let _ = put(
        &engine,
        live_key,
        key_bytes(live_key),
        "live".to_string(),
        hlc(1, 1),
        None,
        0,
    );
    tombstone(&engine, tomb_key, key_bytes(tomb_key), hlc(1, 1), 0);
    let whole: Vec<PartId> = PartId::of_bucket(u16::try_from(bucket).expect("fits")).collect();

    assert_eq!(engine.release_parts(&whole), 2);
    assert_eq!(engine.debug_totals(), (0, 0));
    let held = engine.held_parts();
    assert!(held.is_empty(), "{held:?}");
}

#[test]
fn held_parts_names_each_part_holding_an_entry_once() {
    let engine = engine_u32_string(u64::MAX, None);
    let held = engine.held_parts();
    assert!(held.is_empty(), "{held:?}");
    let keys = [1u32, 2, 3, 1];
    for key in keys {
        let _ = put(&engine, key, key_bytes(key), "v".into(), hlc(1, 1), None, 0);
    }
    let mut expected: Vec<PartId> = keys
        .iter()
        .map(|&key| PartId::from_hash(hash_key_bytes(key_bytes(key).as_ref())))
        .collect();
    expected.sort_unstable();
    expected.dedup();
    assert_eq!(engine.held_parts(), expected);
}

#[test]
fn release_buckets_skips_a_bucket_at_or_past_bucket_count_and_returns_zero() {
    let engine = engine_u32_string(u64::MAX, None);
    let _ = put(&engine, 1, key_bytes(1), "a".into(), hlc(1, 1), None, 0);
    let out_of_range = u16::try_from(BUCKET_COUNT).expect("invariant: BUCKET_COUNT fits u16");

    let removed = engine.release_buckets(&[out_of_range]);

    assert_eq!(removed, 0, "a bucket past BUCKET_COUNT removes nothing");
    assert_eq!(
        engine.get(&1, 0),
        Some("a".to_string()),
        "an out-of-range bucket touches nothing"
    );
}

/// `n` keys, each in a stripe of its own, none in `taken`; their
/// stripes join `taken`.
fn keys_in_distinct_stripes(n: usize, taken: &mut HashSet<usize>) -> Vec<u32> {
    let mut keys = Vec::new();
    let mut candidate = 0u32;
    while keys.len() < n {
        let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(candidate).as_ref()));
        if taken.insert(bucket) {
            keys.push(candidate);
        }
        candidate += 1;
    }
    keys
}

/// An engine at its cap of `cap` unit-weight entries, stored the way
/// [`put`] stores, which never enforces the cap: `cap` keys, each in a
/// stripe of its own, written at times `0..cap`.
fn full_engine(cap: u64, taken: &mut HashSet<usize>) -> (Engine<u32, String>, Vec<u32>) {
    let weigher: Weigher<u32, String> = Box::new(|_k, _v| 1);
    let engine = Engine::<u32, String>::new(cap, None, Some(weigher), None);
    let keys = keys_in_distinct_stripes(usize::try_from(cap).expect("small"), taken);
    for (i, &k) in keys.iter().enumerate() {
        let now = u64::try_from(i).expect("small");
        let _ = put(
            &engine,
            k,
            key_bytes(k),
            k.to_string(),
            hlc(now + 1, 1),
            None,
            now,
        );
    }
    (engine, keys)
}

#[test]
fn a_write_never_evicts_its_own_entry_while_another_is_left() {
    let mut taken = HashSet::new();
    let (engine, older) = full_engine(3, &mut taken);
    // A key alone in its stripe: the eviction this write triggers
    // starts there, with nothing in the stripe but the write itself.
    let fresh = keys_in_distinct_stripes(1, &mut taken)[0];
    let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(fresh).as_ref()));

    let _ = engine.apply_many(
        bucket,
        vec![put_entry(fresh, "fresh", hlc(100, 1), None)],
        &LwwResolver,
        60_000,
        600_000,
        100,
    );

    assert_eq!(engine.debug_totals(), (3, 3), "back within the cap");
    assert_eq!(
        engine.get(&fresh, 100),
        Some("fresh".to_string()),
        "the write keeps its own entry"
    );
    let evicted = older
        .iter()
        .filter(|&&k| engine.get(&k, 100).is_none())
        .count();
    assert_eq!(evicted, 1, "an older entry made the room");
}

#[test]
fn a_batch_evicts_its_own_entries_last_and_still_fits_the_cap() {
    let mut taken = HashSet::new();
    let (engine, older) = full_engine(3, &mut taken);
    // Four keys of one stripe no older key is in: one batch.
    let bucket = (0..BUCKET_COUNT)
        .find(|b| !taken.contains(b))
        .expect("a free stripe");
    let batch: Vec<u32> = (1_000u32..)
        .filter(|&k| stripe_index_from_hash(hash_key_bytes(key_bytes(k).as_ref())) == bucket)
        .take(4)
        .collect();

    let _ = engine.apply_many(
        bucket,
        batch
            .iter()
            .map(|&k| put_entry(k, "new", hlc(100, 1), None))
            .collect(),
        &LwwResolver,
        60_000,
        600_000,
        100,
    );

    assert_eq!(engine.debug_totals(), (3, 3), "the cap holds");
    assert!(
        older.iter().all(|&k| engine.get(&k, 100).is_none()),
        "every older entry goes before any of the batch's own"
    );
    let kept = batch
        .iter()
        .filter(|&&k| engine.get(&k, 100).is_some())
        .count();
    assert_eq!(kept, 3, "the batch's own entries fill the cap");
}

/// A write heavier than the whole cap never fits, so its eviction
/// samples it like any other entry: alone in its stripe, where the
/// eviction starts, it goes first and the older entry stays.
#[test]
fn a_write_heavier_than_the_whole_cap_goes_and_the_older_entry_stays() {
    let weigher: Weigher<u32, String> =
        Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
    let engine = Engine::<u32, String>::new(5, None, Some(weigher), None);
    let mut taken = HashSet::new();
    let [small, heavy] = keys_in_distinct_stripes(2, &mut taken)[..] else {
        panic!("two keys");
    };
    let _ = put(
        &engine,
        small,
        key_bytes(small),
        "x".to_string(),
        hlc(1, 1),
        None,
        0,
    );
    let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(heavy).as_ref()));

    let _ = engine.apply_many(
        bucket,
        vec![put_entry(heavy, &"x".repeat(10), hlc(100, 1), None)],
        &LwwResolver,
        60_000,
        600_000,
        100,
    );

    assert_eq!(engine.debug_totals(), (1, 1), "the cap holds");
    assert_eq!(engine.get(&heavy, 100), None, "the heavy write goes");
    assert_eq!(
        engine.get(&small, 100),
        Some("x".to_string()),
        "the older entry stays"
    );
}

/// A scan that finds nothing but the batch's own entries clears
/// `written` and goes back to the write's stripe, so that group evicts
/// one of its own there, and the batch's next stripe group evicts at
/// once rather than scan every stripe again.
#[test]
fn a_scan_finding_only_written_entries_clears_them_for_the_next_group() {
    let weigher: Weigher<u32, String> = Box::new(|_k, _v| 1);
    let engine = Engine::<u32, String>::new(2, None, Some(weigher), None);
    let mut taken = HashSet::new();
    let late = keys_in_distinct_stripes(1, &mut taken)[0];
    let late_bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(late).as_ref()));
    // The stripe a scan from `late`'s visits next.
    let early_bucket = (late_bucket + 1) % BUCKET_COUNT;
    let early: Vec<u32> = (1_000u32..)
        .filter(|&k| stripe_index_from_hash(hash_key_bytes(key_bytes(k).as_ref())) == early_bucket)
        .take(3)
        .collect();
    let mut written = Written::default();

    let first_group = engine.debug_eviction_lock_acquisitions();
    let _ = engine.apply_many_with_reservation(
        early_bucket,
        early
            .iter()
            .map(|&k| put_entry(k, "early", hlc(100, 1), None))
            .collect(),
        &LwwResolver,
        60_000,
        600_000,
        100,
        None,
        &mut written,
    );
    assert_eq!(engine.debug_totals(), (2, 2), "the cap holds");
    assert!(
        written.is_empty(),
        "the scan found nothing but the batch's own entries"
    );
    assert_eq!(
        engine.debug_eviction_lock_acquisitions() - first_group,
        1 + BUCKET_COUNT as u64 + 1,
        "a sample of the group's stripe, a scan of every stripe, then the group's stripe again"
    );

    let before = engine.debug_eviction_lock_acquisitions();
    let _ = engine.apply_many_with_reservation(
        late_bucket,
        vec![put_entry(late, "late", hlc(101, 1), None)],
        &LwwResolver,
        60_000,
        600_000,
        101,
        None,
        &mut written,
    );

    assert_eq!(engine.debug_totals(), (2, 2), "the cap holds");
    assert_eq!(engine.get(&late, 101), Some("late".to_string()));
    assert_eq!(
        engine.debug_eviction_lock_acquisitions() - before,
        3,
        "a sample of `late`'s stripe, then a scan of it and the next"
    );
}

/// Once a scan finds a victim, the next pass samples the stripe it
/// found rather than a random one: a sparse cache clears an overage in
/// a far stripe with one scan, not one scan per victim.
#[test]
fn the_pass_after_a_scan_stays_in_the_stripe_it_found() {
    let weigher: Weigher<u32, String> = Box::new(|_k, _v| 1);
    let engine = Engine::<u32, String>::new(10, None, Some(weigher), None);
    let far = 7;
    let keys: Vec<u32> = (0..100_000u32)
        .filter(|&k| stripe_index_from_hash(hash_key_bytes(key_bytes(k).as_ref())) == far)
        .take(19)
        .collect();
    for (i, &k) in keys.iter().enumerate() {
        let now = u64::try_from(i).expect("small");
        let _ = put(
            &engine,
            k,
            key_bytes(k),
            k.to_string(),
            hlc(now + 1, 1),
            None,
            now,
        );
    }
    assert_eq!(engine.debug_totals(), (19, 19));
    let before = engine.debug_eviction_lock_acquisitions();

    engine.enforce_capacity(far + 1, &mut Written::default(), 100);

    assert_eq!(engine.debug_totals(), (10, 10), "back within the cap");
    assert_eq!(
        engine.debug_eviction_lock_acquisitions() - before,
        1 + BUCKET_COUNT as u64 + 1,
        "the empty start stripe, a scan around to the far stripe, then one batch there"
    );
}

/// A batch that evicts only from a dense stripe takes nothing from one
/// holding fewer than [`EVICTION_SAMPLE`] candidates, and the colder
/// half of one holding that many.
#[test]
fn a_batch_only_if_dense_spares_a_sparse_stripe() {
    let weigher: Weigher<u32, String> = Box::new(|_k, _v| 1);
    let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
    let (sparse, dense) = (7, 9);
    for (stripe, count) in [(sparse, EVICTION_SAMPLE - 1), (dense, EVICTION_SAMPLE)] {
        let keys = (0..100_000u32)
            .filter(|&k| stripe_index_from_hash(hash_key_bytes(key_bytes(k).as_ref())) == stripe)
            .take(count);
        for (i, k) in keys.enumerate() {
            let now = u64::try_from(i).expect("small");
            let _ = put(
                &engine,
                k,
                key_bytes(k),
                k.to_string(),
                hlc(now + 1, 1),
                None,
                now,
            );
        }
    }
    let batch = |stripe, only_if_dense| {
        engine.evict_batch_sampled_with_reservation(
            stripe,
            100,
            None,
            &Written::default(),
            only_if_dense,
            OnRefusal::TierPolicy,
            100,
        )
    };

    assert!(
        batch(sparse, true).made_no_progress(),
        "a sparse stripe gives up nothing"
    );
    assert_eq!(
        batch(dense, true).removed_weight,
        4,
        "a dense stripe gives up its colder half"
    );
    assert_eq!(
        batch(sparse, false).removed_weight,
        4,
        "without the flag, a sparse stripe gives up its colder half too"
    );
}

#[test]
fn a_rearm_keeps_its_own_entry_when_it_enforces_the_cap() {
    let mut taken = HashSet::new();
    let (engine, older) = full_engine(3, &mut taken);
    // Over the cap by one more stored the way `put` stores: the rearm
    // is the first write to enforce the cap.
    let rearmed = keys_in_distinct_stripes(1, &mut taken)[0];
    let _ = put(
        &engine,
        rearmed,
        key_bytes(rearmed),
        "kept".to_string(),
        hlc(50, 1),
        None,
        50,
    );
    assert_eq!(engine.debug_totals(), (4, 4));

    let outcome = engine.apply_if_current(
        put_entry(rearmed, "kept", hlc(60, 1), Some(60_000)),
        hlc(50, 1),
        &LwwResolver,
        60_000,
        600_000,
        60,
    );

    assert!(matches!(outcome, Some(ApplyOutcome::Put { .. })));
    assert_eq!(engine.debug_totals(), (3, 3), "back within the cap");
    assert_eq!(engine.get(&rearmed, 60), Some("kept".to_string()));
    let evicted = older
        .iter()
        .filter(|&&k| engine.get(&k, 60).is_none())
        .count();
    assert_eq!(evicted, 1, "an older entry made the room");
}

#[test]
fn the_sample_and_the_scan_pass_over_the_written_entries() {
    let (first, second, bucket) = same_bucket_pair(100_000);
    let engine = engine_u32_string(u64::MAX, None);
    for k in [first, second] {
        let _ = put(&engine, k, key_bytes(k), k.to_string(), hlc(1, 1), None, 0);
    }
    let hash_of = |k: u32| hash_key_bytes(key_bytes(k).as_ref());
    let sampled = |written: &Written| -> Vec<Bytes> {
        let stripe = engine.stripe_lock(bucket).read();
        engine
            .sample_candidates(&stripe, EVICTION_BATCH_SAMPLE, written)
            .map(|live| record_key_bytes(&live.record))
            .collect()
    };
    assert_eq!(sampled(&Written::default()).len(), 2);
    assert_eq!(
        sampled(&Written::of(hash_of(first))),
        vec![key_bytes(second)],
        "the written entry is not in the window"
    );
    let mut both = Written::of(hash_of(first));
    both.insert(hash_of(second));
    assert_eq!(sampled(&both), Vec::<Bytes>::new());

    assert!(
        engine
            .evict_one_scanning_with_reservation(bucket, None, &both, OnRefusal::TierPolicy, 0)
            .is_none(),
        "a scan finds nothing but written entries to evict"
    );
    assert!(
        engine
            .evict_one_scanning_with_reservation(
                bucket,
                None,
                &Written::of(hash_of(first)),
                OnRefusal::TierPolicy,
                0,
            )
            .is_some()
    );
    assert_eq!(engine.get(&first, 0), Some(first.to_string()));
    assert_eq!(engine.get(&second, 0), None, "the unwritten entry went");
}

#[test]
fn capacity_eviction_rotates_past_an_empty_start_bucket_into_other_stripes() {
    let weigher: Weigher<u32, String> = Box::new(|_k, _v| 1);
    let engine = Engine::<u32, String>::new(3, None, Some(weigher), None);

    // 8 keys landing in 8 distinct, non-empty stripes.
    let mut other_keys = Vec::new();
    let mut used_buckets = std::collections::HashSet::new();
    let mut candidate = 0u32;
    while other_keys.len() < 8 {
        let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(candidate).as_ref()));
        if used_buckets.insert(bucket) {
            other_keys.push(candidate);
        }
        candidate += 1;
    }
    for (i, &k) in other_keys.iter().enumerate() {
        let now = u64::try_from(i).expect("small");
        let _ = put(
            &engine,
            k,
            key_bytes(k),
            k.to_string(),
            hlc(u64::from(k) + 1, 1),
            None,
            now,
        );
    }

    // A key landing in a stripe none of the above touched: the eviction
    // start point, but with only one entry to give up.
    let start_key = loop {
        let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(candidate).as_ref()));
        if !used_buckets.contains(&bucket) {
            break candidate;
        }
        candidate += 1;
    };
    let start_bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(start_key).as_ref()));
    let _ = put(
        &engine,
        start_key,
        key_bytes(start_key),
        start_key.to_string(),
        hlc(1_000, 1),
        None,
        100,
    );

    let (entries_before, weight_before) = engine.debug_totals();
    assert_eq!(entries_before, 9);
    assert_eq!(weight_before, 9);

    engine.enforce_capacity(start_bucket, &mut Written::default(), 500);

    let (entries_after, weight_after) = engine.debug_totals();
    assert!(
        weight_after <= 3,
        "total weight {weight_after} stays within the 3-unit cap"
    );
    assert!(
        entries_after < entries_before - 1,
        "the start stripe alone (1 entry) cannot account for a {}-entry eviction: \
         enforce_capacity rotated into other stripes",
        entries_before - entries_after
    );
    assert_eq!(
        engine.get(&start_key, 100),
        None,
        "the start stripe's own entry is evicted too"
    );
}

#[tokio::test]
async fn stampede_collapses_to_one_loader_n_minus_one_hits() {
    const CONCURRENCY: usize = 32;
    let engine = Arc::new(engine_u32_string(u64::MAX, None));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..CONCURRENCY {
        let engine = Arc::clone(&engine);
        let calls = Arc::clone(&calls);
        tasks.spawn(async move {
            let key = 7u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            loop {
                if let Some(v) = engine.get(&key, 0) {
                    return v;
                }
                match engine.miss_or_join(&kb, hash, 0) {
                    JoinOutcome::Hit(v) => return v,
                    JoinOutcome::Join(inflight, mut done) => {
                        let _ = done.changed().await;
                        if let Some(e) = inflight.error.get() {
                            panic!("unexpected loader failure: {e}");
                        }
                    }
                    JoinOutcome::Owner(inflight) => {
                        let guard = engine.guard_inflight(kb.clone(), hash, Arc::clone(&inflight));
                        calls.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        let value = "loaded-once".to_string();
                        let encoded = Bytes::from(postcard::to_stdvec(&value).expect("encode"));
                        let outcome = engine.complete_fresh_load(
                            FreshLoad {
                                key: &key,
                                key_bytes: &kb,
                                hash,
                                ver: hlc(1, 1),
                                value: value.clone(),
                                encoded,
                                expires_at_ms: None,
                            },
                            0,
                            &inflight,
                        );
                        assert_eq!(
                            outcome,
                            FillOutcome::Installed { had_live: false },
                            "a genuine miss sees no prior live entry"
                        );
                        guard.complete();
                        return value;
                    }
                }
            }
        });
    }
    let mut results = Vec::with_capacity(CONCURRENCY);
    while let Some(r) = tasks.join_next().await {
        results.push(r.expect("spawned call does not panic"));
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "exactly one loader call under a stampede"
    );
    assert!(results.iter().all(|v| v == "loaded-once"));
}

#[tokio::test]
async fn loader_error_is_shared_by_joined_waiters() {
    let engine = Arc::new(engine_u32_string(u64::MAX, None));
    let key = 9u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());

    let JoinOutcome::Owner(inflight) = engine.miss_or_join(&kb, hash, 0) else {
        panic!("first caller becomes the owner");
    };
    let guard = engine.guard_inflight(kb.clone(), hash, Arc::clone(&inflight));

    // The receiver subscribes under the stripe lock, so a late waiter still
    // sees the failure.
    let JoinOutcome::Join(joined, mut done) = engine.miss_or_join(&kb, hash, 0) else {
        panic!("second caller joins the existing load");
    };

    let boom: Arc<dyn std::error::Error + Send + Sync> = Arc::new(std::io::Error::other("boom"));
    engine.fail_inflight(&kb, hash, &inflight, boom);
    guard.complete();

    done.changed().await.expect("the owner finished the fill");
    assert!(
        joined.error.get().is_some(),
        "the joined waiter sees the error"
    );
    assert_eq!(
        engine.get(&key, 0),
        None,
        "a failed load never installs a value"
    );
}

#[tokio::test]
async fn a_cancelled_loader_lets_a_waiter_take_over() {
    let engine = Arc::new(engine_u32_string(u64::MAX, None));
    let key = 11u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());

    let JoinOutcome::Owner(inflight) = engine.miss_or_join(&kb, hash, 0) else {
        panic!("first caller becomes the owner");
    };
    {
        // Dropped without `complete()`, simulating a cancelled loader
        // future.
        let _guard = engine.guard_inflight(kb.clone(), hash, Arc::clone(&inflight));
    }

    assert!(
        !engine
            .stripe_lock(stripe_index_from_hash(hash))
            .read()
            .inflight
            .contains_key(kb.as_ref())
    );
    // A fresh caller becomes the new owner instead of joining a dead entry.
    match engine.miss_or_join(&kb, hash, 0) {
        JoinOutcome::Owner(_) => {}
        _ => panic!("a cancelled load's key is free for a new owner"),
    }
}

#[tokio::test]
async fn a_waiter_that_starts_waiting_after_the_owner_finished_still_wakes() {
    let engine = Arc::new(engine_u32_string(u64::MAX, None));
    let key = 13u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());

    let JoinOutcome::Owner(inflight) = engine.miss_or_join(&kb, hash, 0) else {
        panic!("first caller becomes the owner");
    };
    let JoinOutcome::Join(_, mut done) = engine.miss_or_join(&kb, hash, 0) else {
        panic!("second caller joins the existing load");
    };
    // The owner completes before the waiter ever polls its receiver.
    let encoded = Bytes::from(postcard::to_stdvec("late").expect("encode"));
    engine.complete_fresh_load(
        FreshLoad {
            key: &key,
            key_bytes: &kb,
            hash,
            ver: hlc(1, 1),
            value: "late".to_string(),
            encoded,
            expires_at_ms: None,
        },
        0,
        &inflight,
    );
    tokio::time::timeout(std::time::Duration::from_secs(1), done.changed())
        .await
        .expect("a completion that preceded the wait is not lost")
        .expect("the owner finished the fill");
    assert_eq!(engine.get(&key, 0), Some("late".to_string()));
}

#[test]
fn miss_or_join_hits_on_the_locked_recheck_after_a_concurrent_insert() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 5u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());

    assert_eq!(engine.get(&key, 0), None, "the fast-path read misses first");
    // A write lands on the key between the caller's fast-path miss and its
    // call to `miss_or_join`, e.g. a concurrent `get_or_load` owner or a
    // plain remote write.
    let _ = put(&engine, key, kb.clone(), "late".into(), hlc(1, 1), None, 0);

    match engine.miss_or_join(&kb, hash, 0) {
        JoinOutcome::Hit(v) => assert_eq!(v, "late"),
        _ => panic!("the locked re-check finds the entry that landed after the fast-path miss"),
    }
}

#[test]
fn complete_fresh_load_replaces_a_tombstone_and_keeps_digest_and_weight_correct() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 21u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());

    let _ = put(&engine, key, kb.clone(), "old".into(), hlc(1, 1), None, 0);
    tombstone(&engine, key, kb.clone(), hlc(2, 1), 0);
    assert_eq!(
        engine.get(&key, 0),
        None,
        "a tombstone sits where the load will land"
    );

    let JoinOutcome::Owner(inflight) = engine.miss_or_join(&kb, hash, 0) else {
        panic!("no live entry: this caller becomes the owner");
    };
    let encoded = Bytes::from(postcard::to_stdvec("fresh").expect("encode"));
    let outcome = engine.complete_fresh_load(
        FreshLoad {
            key: &key,
            key_bytes: &kb,
            hash,
            ver: hlc(3, 1),
            value: "fresh".to_string(),
            encoded,
            expires_at_ms: None,
        },
        0,
        &inflight,
    );
    assert_eq!(
        outcome,
        FillOutcome::Installed { had_live: false },
        "an older tombstone gives way, and it is not a live entry"
    );
    assert_eq!(engine.get(&key, 0), Some("fresh".to_string()));
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
    let (entries, weight) = engine.debug_totals();
    assert_eq!(entries, 1);
    assert_eq!(weight, 1);
}

#[test]
fn fill_supersedes_only_an_absent_or_older_entry() {
    assert!(fill_supersedes(None, hlc(1, 1)), "nothing to displace");
    assert!(
        fill_supersedes(Some(hlc(1, 1)), hlc(2, 1)),
        "an older entry gives way"
    );
    assert!(
        !fill_supersedes(Some(hlc(3, 1)), hlc(2, 1)),
        "a newer entry that landed during the load stays"
    );
    assert!(
        !fill_supersedes(Some(hlc(2, 1)), hlc(2, 1)),
        "the same version is not newer"
    );
}

#[test]
fn complete_fresh_load_keeps_a_newer_write_that_landed_during_the_load() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 22u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());

    assert_eq!(engine.get(&key, 0), None);
    let JoinOutcome::Owner(inflight) = engine.miss_or_join(&kb, hash, 0) else {
        panic!("first caller becomes the owner");
    };

    // A write lands on the same key while the load is in flight, e.g. a
    // replicated write racing the local loader, stamped after the fill.
    let _ = put(
        &engine,
        key,
        kb.clone(),
        "raced-in".into(),
        hlc(5, 2),
        None,
        0,
    );

    let encoded = Bytes::from(postcard::to_stdvec("loaded").expect("encode"));
    let outcome = engine.complete_fresh_load(
        FreshLoad {
            key: &key,
            key_bytes: &kb,
            hash,
            ver: hlc(1, 1),
            value: "loaded".to_string(),
            encoded,
            expires_at_ms: None,
        },
        0,
        &inflight,
    );
    assert_eq!(outcome, FillOutcome::Superseded);
    assert_eq!(
        engine.get(&key, 0),
        Some("raced-in".to_string()),
        "the newer write keeps its place over the older fill"
    );
    assert!(
        matches!(engine.miss_or_join(&kb, hash, 0), JoinOutcome::Hit(_)),
        "the superseded fill left no in-flight entry behind"
    );
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
    let (entries, weight) = engine.debug_totals();
    assert_eq!(entries, 1);
    assert_eq!(weight, 1);
}

#[test]
fn complete_fresh_load_keeps_a_newer_tombstone_that_landed_during_the_load() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 23u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());

    let JoinOutcome::Owner(inflight) = engine.miss_or_join(&kb, hash, 0) else {
        panic!("first caller becomes the owner");
    };
    // A removal lands while the loader still reads the old row.
    tombstone(&engine, key, kb.clone(), hlc(4, 1), 0);

    let encoded = Bytes::from(postcard::to_stdvec("stale").expect("encode"));
    let outcome = engine.complete_fresh_load(
        FreshLoad {
            key: &key,
            key_bytes: &kb,
            hash,
            ver: hlc(2, 1),
            value: "stale".to_string(),
            encoded,
            expires_at_ms: None,
        },
        0,
        &inflight,
    );
    assert_eq!(outcome, FillOutcome::Superseded);
    assert_eq!(
        engine.get(&key, 0),
        None,
        "the removal stays removed; the stale fill never resurrects the key"
    );
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
    let (entries, weight) = engine.debug_totals();
    assert_eq!(entries, 0);
    assert_eq!(weight, 0);
}

#[test]
fn complete_fresh_load_stores_nothing_after_an_invalidation_during_the_load() {
    for stamp in [hlc(2, 1), hlc(9, 1)] {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 23u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let JoinOutcome::Owner(inflight) = engine.miss_or_join(&kb, hash, 0) else {
            panic!("first caller becomes the owner");
        };
        // The key changes elsewhere while the loader still reads the old
        // row. The invalidation's version does not matter: the loader's
        // read and the change have no order the versions could show.
        assert_eq!(engine.invalidate(kb.as_ref(), hash, hlc(4, 2)), None);
        let encoded = Bytes::from(postcard::to_stdvec("stale").expect("encode"));
        let outcome = engine.complete_fresh_load(
            FreshLoad {
                key: &key,
                key_bytes: &kb,
                hash,
                ver: stamp,
                value: "stale".to_string(),
                encoded,
                expires_at_ms: None,
            },
            0,
            &inflight,
        );
        assert_eq!(outcome, FillOutcome::Invalidated, "fill stamped {stamp:?}");
        assert_eq!(engine.get(&key, 0), None);
        assert_eq!(
            inflight.value.get().map(String::as_str),
            Some("stale"),
            "joined waiters answer with the loaded value"
        );
        assert_eq!(engine.digests(), engine.recompute_digests_paired());
        assert_eq!(engine.debug_totals(), (0, 0));
        assert!(
            matches!(engine.miss_or_join(&kb, hash, 0), JoinOutcome::Owner(_)),
            "the in-flight entry is gone, so the next read loads afresh"
        );
    }
}

#[test]
fn an_invalidation_before_the_load_starts_leaves_the_next_fill_alone() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 23u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    assert_eq!(engine.invalidate(kb.as_ref(), hash, hlc(4, 2)), None);
    let JoinOutcome::Owner(inflight) = engine.miss_or_join(&kb, hash, 0) else {
        panic!("first caller becomes the owner");
    };
    let encoded = Bytes::from(postcard::to_stdvec("fresh").expect("encode"));
    let outcome = engine.complete_fresh_load(
        FreshLoad {
            key: &key,
            key_bytes: &kb,
            hash,
            ver: hlc(9, 1),
            value: "fresh".to_string(),
            encoded,
            expires_at_ms: None,
        },
        0,
        &inflight,
    );
    assert_eq!(outcome, FillOutcome::Installed { had_live: false });
    assert_eq!(engine.get(&key, 0), Some("fresh".to_string()));
}

#[test]
fn eviction_sampling_starts_from_rotating_offsets() {
    let engine = engine_u32_string(u64::MAX, None);
    let offsets: std::collections::HashSet<usize> =
        (0..64).map(|_| engine.sample_offset(40)).collect();
    assert!(
        offsets.iter().all(|&o| o < 40),
        "an offset always lies inside the stripe"
    );
    assert!(
        offsets.iter().any(|&o| o >= EVICTION_SAMPLE),
        "sampling reaches past the first {EVICTION_SAMPLE} slots: {offsets:?}"
    );
    assert_eq!(engine.sample_offset(0), 0, "an empty stripe has one offset");
}

#[tokio::test]
async fn concurrent_inserts_across_stripes_all_land() {
    let engine = Arc::new(engine_u32_string(u64::MAX, None));
    let keys: Vec<u32> = (0..64).collect();
    let mut tasks = tokio::task::JoinSet::new();
    for k in keys.clone() {
        let engine = Arc::clone(&engine);
        tasks.spawn(async move {
            let kb = key_bytes(k);
            let _ = put(
                &engine,
                k,
                kb,
                k.to_string(),
                hlc(u64::from(k) + 1, 1),
                None,
                0,
            );
        });
    }
    while let Some(r) = tasks.join_next().await {
        r.expect("spawned call does not panic");
    }
    for k in keys {
        assert_eq!(engine.get(&k, 0), Some(k.to_string()));
    }
}

#[test]
fn two_different_stripes_lock_independently() {
    let engine = Arc::new(engine_u32_string(u64::MAX, None));
    let (bucket_a, bucket_b) = (0usize, 1usize);

    let held = Arc::clone(&engine);
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    let handle = std::thread::spawn(move || {
        let _guard = held.stripe_lock(bucket_a).write();
        tx.send(()).expect("send");
        std::thread::sleep(Duration::from_millis(150));
    });
    rx.recv().expect("lock holder signals it has the lock");

    assert!(
        engine.stripe_lock(bucket_b).try_write().is_some(),
        "a different stripe's lock is not blocked"
    );
    assert!(
        engine.stripe_lock(bucket_a).try_write().is_none(),
        "the held stripe's lock is still contended"
    );
    handle.join().expect("lock holder thread does not panic");
    assert!(
        engine.stripe_lock(bucket_a).try_write().is_some(),
        "released after the holder finishes"
    );
}

#[test]
fn enforce_capacity_clears_an_overage_needing_thousands_of_evictions() {
    let weigher: Weigher<u32, String> =
        Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
    let engine = Engine::<u32, String>::new(10_000, None, Some(weigher), None);
    // 6,000 one-unit entries, then one 9,500-unit write: back under the
    // cap only after more than 5,500 evictions of older ones, since the
    // write's own entry goes only once nothing else is left.
    for k in 1..=6_000u32 {
        let _ = put(
            &engine,
            k,
            key_bytes(k),
            "x".into(),
            hlc(u64::from(k), 1),
            None,
            0,
        );
    }
    let big = 7_000u32;
    let big_bytes = key_bytes(big);
    let big_hash = hash_key_bytes(big_bytes.as_ref());
    let start_bucket = stripe_index_from_hash(big_hash);
    let _ = put(
        &engine,
        big,
        big_bytes,
        "y".repeat(9_500),
        hlc(10_000, 1),
        None,
        1,
    );
    assert_eq!(engine.debug_totals().1, 15_500);

    engine.enforce_capacity(start_bucket, &mut Written::of(big_hash), 100);

    let (entries, weight) = engine.debug_totals();
    assert!(
        weight <= 10_000,
        "total weight {weight} is back under the cap"
    );
    assert!(
        entries <= 501,
        "{entries} entries remain; the big write was evicted instead of older ones"
    );
}

#[test]
fn defer_to_flusher_only_while_a_hand_off_is_in_flight_and_eviction_cannot_help() {
    assert!(
        !defer_to_flusher(false, 10, 0, 10),
        "nothing pending: enforce_capacity's ordinary scanning fallback runs"
    );
    assert!(
        !defer_to_flusher(true, 11, 0, 10),
        "a refusal with nothing pending is no full queue: scan for another victim"
    );
    assert!(
        defer_to_flusher(false, 10, 1, 10),
        "within the limit once the pending hand-off installs: trust the flusher"
    );
    assert!(defer_to_flusher(false, 0, u64::MAX, 0));
    assert!(
        !defer_to_flusher(false, 11, 1, 10),
        "still over the limit after every pending hand-off installs: keep evicting"
    );
    assert!(!defer_to_flusher(false, u64::MAX, u64::MAX, 10));
    assert!(
        defer_to_flusher(true, 11, 1, 10),
        "a victim refused while a hand-off is in flight: the queue is full for every stripe"
    );
    assert!(defer_to_flusher(true, u64::MAX, u64::MAX, 0));
}

#[cfg(feature = "spill")]
#[test]
fn claimed_total_adds_a_weight_only_within_the_cap() {
    assert_eq!(claimed_total(3, 1, 4), Some(4));
    assert_eq!(claimed_total(0, 0, 0), Some(0));
    assert_eq!(
        claimed_total(4, 1, 4),
        None,
        "no room left: the entry stays on disk"
    );
    assert_eq!(claimed_total(0, 5, 4), None);
    assert_eq!(claimed_total(u64::MAX, 1, u64::MAX), None, "never wraps");
    assert_eq!(claimed_total(u64::MAX - 1, 1, u64::MAX), Some(u64::MAX));
}

#[cfg(feature = "spill")]
#[test]
fn promotion_fits_any_entry_no_heavier_than_the_whole_cap() {
    assert!(promotion_fits(1, 1));
    assert!(promotion_fits(0, 0));
    assert!(promotion_fits(u32::MAX, u64::MAX));
    assert!(
        !promotion_fits(2, 1),
        "no eviction makes room for an entry heavier than the whole cap"
    );
    assert!(!promotion_fits(u32::MAX, u64::from(u32::MAX) - 1));
}

#[test]
fn should_shrink_stripe_true_well_below_its_allocation() {
    assert!(
        should_shrink_stripe(1, 64),
        "1/64 is well below the eighth threshold"
    );
    assert!(
        should_shrink_stripe(0, 100),
        "a fully drained stripe is always well below its own allocation"
    );
    assert!(
        should_shrink_stripe(8, 64),
        "8/64 sits exactly at the eighth threshold, still shrunk"
    );
}

#[test]
fn should_shrink_stripe_false_near_or_above_its_allocation() {
    assert!(
        !should_shrink_stripe(64, 64),
        "at capacity: nothing to reclaim"
    );
    assert!(
        !should_shrink_stripe(40, 64),
        "40/64 is well past the eighth threshold"
    );
    assert!(
        !should_shrink_stripe(9, 64),
        "9/64 sits one entry past the eighth threshold"
    );
    assert!(
        !should_shrink_stripe(0, 0),
        "never allocated: nothing to shrink further"
    );
}

#[test]
fn deficit_once_still_over_capacity_zeroes_a_deficit_the_cap_no_longer_needs() {
    assert_eq!(
        deficit_once_still_over_capacity(35, 30, 15),
        15,
        "still 5 units over the cap: the deficit is still owed"
    );
    assert_eq!(
        deficit_once_still_over_capacity(25, 30, 15),
        0,
        "already under the cap, by the batch's own other progress or an unrelated \
         concurrent flusher install: the stale deficit is dropped rather than paid down \
         with an avoidable extra reserve() wait"
    );
    assert_eq!(
        deficit_once_still_over_capacity(30, 30, 15),
        0,
        "exactly at the cap counts as satisfied, matching enforce_capacity_with_reservation's \
         own loop-top `current <= max_capacity` check"
    );
    assert_eq!(
        deficit_once_still_over_capacity(31, 30, 15),
        15,
        "one unit over is still over: the deficit survives unchanged"
    );
}

#[test]
fn eviction_batch_size_takes_the_fewest_entries_that_clear_the_overage() {
    assert_eq!(
        eviction_batch_size(0, &[3, 3, 3]),
        0,
        "no overage evicts nothing"
    );
    assert_eq!(
        eviction_batch_size(2, &[5, 5, 5]),
        1,
        "the first sampled entry alone already clears a 2-unit overage"
    );
    assert_eq!(
        eviction_batch_size(6, &[3, 3, 3]),
        2,
        "3 clears none of a 6-unit overage, 3+3 clears all of it"
    );
    assert_eq!(
        eviction_batch_size(100, &[1, 1, 1]),
        2,
        "a large overage takes only the colder half of the sample"
    );
    assert_eq!(
        eviction_batch_size(100, &[1; 32]),
        EVICTION_BATCH,
        "a full sample evicts at most EVICTION_BATCH"
    );
    assert_eq!(
        eviction_batch_size(100, &[7]),
        1,
        "a lone sampled entry is evicted, as the single-entry path did"
    );
    assert_eq!(
        eviction_batch_size(5, &[]),
        0,
        "nothing sampled means nothing to evict"
    );
}

#[test]
fn enforce_capacity_batches_evictions_under_a_burst_ten_thousand_over_the_cap() {
    let weigher: Weigher<u32, String> = Box::new(|_k, _v| 1);
    // A 40,000-unit cap left dense after eviction (~39 entries/stripe)
    // so a random probe rarely lands on an already-empty stripe; the
    // point here is measuring the batch size, not the scanning
    // fallback's cost on a nearly-drained table.
    let engine = Engine::<u32, String>::new(40_000, None, Some(weigher), None);
    // 50,000 one-unit entries: 10,000 over the cap.
    for k in 1..=50_000u32 {
        let _ = put(
            &engine,
            k,
            key_bytes(k),
            "x".into(),
            hlc(u64::from(k), 1),
            None,
            0,
        );
    }
    let (entries_before, weight_before) = engine.debug_totals();
    assert_eq!(entries_before, 50_000);
    assert_eq!(weight_before, 50_000);

    engine.enforce_capacity(0, &mut Written::default(), 0);

    let (entries_after, weight_after) = engine.debug_totals();
    assert!(
        weight_after <= 40_000,
        "total weight {weight_after} is back under the cap"
    );
    let evicted = entries_before - entries_after;
    let acquisitions = engine.debug_eviction_lock_acquisitions();
    assert!(
        acquisitions * 2 < evicted,
        "{acquisitions} lock acquisitions to evict {evicted} entries: batching should need \
         far fewer acquisitions than entries evicted"
    );
}

#[test]
fn enforce_capacity_stops_once_every_stripe_is_empty() {
    let weigher: Weigher<u32, String> =
        Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
    let engine = Engine::<u32, String>::new(5, None, Some(weigher), None);
    // One entry heavier than the whole cap: evicting it is all there is.
    let _ = put(&engine, 1, key_bytes(1), "z".repeat(50), hlc(1, 1), None, 0);
    engine.enforce_capacity(0, &mut Written::default(), 0);
    assert_eq!(engine.debug_totals(), (0, 0));
    assert!(
        engine.evict_one_sampled(0, 0).made_no_progress(),
        "an empty stripe evicts nothing"
    );
    assert_eq!(engine.evict_one_scanning(0, 0), None);
}

/// [`digest_matches_full_recompute_after_random_ops_including_sweeps_and_evictions`]'s
/// shared step: applies `incoming` directly through [`apply_locked`],
/// under a fresh stripe lock, with a 1s/10s tombstone deadline and an
/// [`LwwResolver`].
fn apply_direct<K, V>(
    engine: &Engine<K, V>,
    bucket: usize,
    entry: EntryKey<K>,
    ver: Hlc,
    incoming: Incoming<V>,
    now_ms: u64,
) where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let hash = entry.hash;
    let mut stripe = engine.stripe_lock(bucket).write();
    let resolver = LwwResolver;
    let apply_ctx = test_ctx(engine, bucket, hash, &resolver, 1_000, 10_000, now_ms);
    let _ = apply_locked(&mut stripe, &apply_ctx, entry, ver, incoming);
}

#[test]
fn digest_matches_full_recompute_after_random_ops_including_sweeps_and_evictions() {
    use rand::{RngExt as _, SeedableRng as _, rngs::StdRng};

    let weigher: Weigher<u32, u64> = Box::new(|_k, _v| 1);
    let engine = Engine::<u32, u64>::new(12, None, Some(weigher), None);
    let mut rng = StdRng::seed_from_u64(0x5EED);
    let mut clock = HlcClock::new(NodeId::from(1));

    for i in 0..300u64 {
        let key = rng.random_range(0..24u32);
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let now = i * 10;
        match rng.random_range(0..4u32) {
            0 => {
                let ver = clock.now(now);
                let value = u64::from(key) * 31;
                let encoded = Bytes::from(postcard::to_stdvec(&value).expect("encode"));
                apply_direct(
                    &engine,
                    bucket,
                    EntryKey {
                        hash,
                        key,
                        key_bytes: kb,
                    },
                    ver,
                    Incoming::Put {
                        value,
                        expires_at_ms: Some(now + 500),
                        encoded,
                    },
                    now,
                );
                engine.enforce_capacity(bucket, &mut Written::default(), now);
            }
            1 => {
                let ver = clock.now(now);
                apply_direct(
                    &engine,
                    bucket,
                    EntryKey {
                        hash,
                        key,
                        key_bytes: kb,
                    },
                    ver,
                    Incoming::Tombstone,
                    now,
                );
            }
            2 => engine.sweep(now),
            _ => engine.gc_tombstones(false, now),
        }
        assert_eq!(
            engine.live_entry_count(),
            engine.recompute_live_entry_count(),
            "iteration {i}: the incrementally maintained live count diverged from a full \
             recount"
        );
        if i % 15 == 0 {
            assert_eq!(
                engine.digests(),
                engine.recompute_digests_paired(),
                "iteration {i}"
            );
            assert_eq!(
                engine.recompute_digests(),
                (0..BUCKET_COUNT)
                    .flat_map(|b| (0..PART_COUNT).map(move |p| (b, p)))
                    .map(|(b, p)| engine.part_digests(u16::try_from(b).expect("fits"))[p])
                    .collect::<Vec<u64>>(),
                "iteration {i}: part digests match the full recompute, not only their \
                 bucket aggregate"
            );
        }
    }
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
}

#[test]
fn resident_bytes_is_none_and_slabs_count_nothing_until_counting_is_turned_on() {
    let engine = engine_u32_string(u64::MAX, None);
    let _ = put(
        &engine,
        1,
        key_bytes(1),
        "v".repeat(100),
        hlc(1, 1),
        None,
        0,
    );
    assert_eq!(engine.resident_bytes(), None);
    assert!(
        engine
            .stripes
            .iter()
            .all(|stripe| stripe.read().live.resident_bytes.is_none()),
        "an uncapped engine's slabs carry no counter to move"
    );
    let counted = engine_u32_string(u64::MAX, None).count_resident_bytes();
    assert_eq!(counted.resident_bytes(), Some(0));
    assert!(
        counted
            .stripes
            .iter()
            .all(|stripe| stripe.read().live.resident_bytes.is_some())
    );
}

#[test]
fn resident_bytes_counts_each_entry_overhead_plus_its_heap_record() {
    let engine = engine_u32_string(u64::MAX, None).count_resident_bytes();
    let (short, long) = (1u32, 2u32);
    let _ = put(
        &engine,
        short,
        key_bytes(short),
        "s".to_string(),
        hlc(1, 1),
        None,
        0,
    );
    assert_eq!(
        engine.resident_bytes(),
        Some(ENTRY_OVERHEAD_BYTES),
        "an inline record adds only the entry overhead"
    );
    let long_value = "v".repeat(100);
    let _ = put(
        &engine,
        long,
        key_bytes(long),
        long_value.clone(),
        hlc(2, 1),
        None,
        0,
    );
    // One LEN prefix byte, then the key and value bytes.
    let long_record =
        1 + key_bytes(long).len() + postcard::to_stdvec(&long_value).expect("encode").len();
    assert_eq!(
        engine.resident_bytes(),
        Some(2 * ENTRY_OVERHEAD_BYTES + long_record as u64),
        "a heap record adds its whole length on top of the overhead"
    );
    let _ = put(
        &engine,
        long,
        key_bytes(long),
        "t".to_string(),
        hlc(3, 1),
        None,
        0,
    );
    assert_eq!(
        engine.resident_bytes(),
        Some(2 * ENTRY_OVERHEAD_BYTES),
        "overwriting with an inline value releases the old heap bytes"
    );
    for (key, wall_ms) in [(short, 4), (long, 5)] {
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        apply_direct(
            &engine,
            stripe_index_from_hash(hash),
            EntryKey {
                hash,
                key,
                key_bytes: kb,
            },
            hlc(wall_ms, 1),
            Incoming::Tombstone,
            wall_ms,
        );
    }
    assert_eq!(
        engine.resident_bytes(),
        Some(0),
        "removals release every byte"
    );
    assert_eq!(
        engine.resident_bytes(),
        Some(engine.recount_resident_bytes())
    );
}

#[test]
fn resident_bytes_matches_a_full_recount_after_random_ops_including_sweeps_and_evictions() {
    use rand::{RngExt as _, SeedableRng as _, rngs::StdRng};

    let engine = engine_u32_string(12, None).count_resident_bytes();
    let mut rng = StdRng::seed_from_u64(0xB17E5);
    let mut clock = HlcClock::new(NodeId::from(1));

    for i in 0..400u64 {
        let key = rng.random_range(0..24u32);
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let now = i * 10;
        let entry = EntryKey {
            hash,
            key,
            key_bytes: kb,
        };
        match rng.random_range(0..4u32) {
            0 => {
                // Lengths on both sides of INLINE_CAP, so records move
                // between the inline and heap variants.
                let value = "x".repeat(rng.random_range(0..64usize));
                let encoded = Bytes::from(postcard::to_stdvec(&value).expect("encode"));
                let incoming = Incoming::Put {
                    value,
                    expires_at_ms: Some(now + 500),
                    encoded,
                };
                apply_direct(&engine, bucket, entry, clock.now(now), incoming, now);
                engine.enforce_capacity(bucket, &mut Written::default(), now);
            }
            1 => {
                let ver = clock.now(now);
                apply_direct(&engine, bucket, entry, ver, Incoming::Tombstone, now);
            }
            2 => engine.sweep(now),
            _ => engine.gc_tombstones(false, now),
        }
        assert_eq!(
            engine.resident_bytes(),
            Some(engine.recount_resident_bytes()),
            "iteration {i}: the maintained resident byte total diverged from a full recount"
        );
    }
}

#[test]
fn compact_replace_moves_resident_bytes_by_the_heap_delta() {
    let engine = engine_u32_string(u64::MAX, None).count_resident_bytes();
    let key = 7u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let _ = put(&engine, key, kb.clone(), "a".repeat(80), hlc(1, 1), None, 0);
    let before = engine.resident_bytes().expect("counting is on");
    let compacted = "a".repeat(30);
    assert!(engine.compact_replace_if_current(CompactReplace {
        key: &key,
        key_bytes: kb.as_ref(),
        hash,
        expected_ver: hlc(1, 1),
        new_ver: hlc(2, 1),
        value: compacted.clone(),
        encoded: Bytes::from(postcard::to_stdvec(&compacted).expect("encode")),
    }));
    assert_eq!(
        before - engine.resident_bytes().expect("counting is on"),
        50,
        "the record shrinks by exactly the 50 value bytes compaction dropped"
    );
    assert_eq!(
        engine.resident_bytes(),
        Some(engine.recount_resident_bytes())
    );
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

#[test]
fn is_resident_true_for_resident_false_for_spilled() {
    let resident = Live::<u32, String>::new(
        build_resident_record(key_bytes(1).as_ref(), b"v"),
        hlc(1, 1),
        NEVER_EXPIRES,
        1,
        0,
        #[cfg(feature = "spill")]
        EntryState::Resident,
    );
    assert!(is_resident(&resident));

    #[cfg(feature = "spill")]
    {
        let spilled = Live::<u32, String>::new(
            build_key_only_record(key_bytes(1).as_ref()),
            hlc(1, 1),
            NEVER_EXPIRES,
            0,
            0,
            EntryState::Spilled(SpillLoc {
                region: 0,
                offset: 0,
                len: 1,
                generation: 0,
            }),
        );
        assert!(!is_resident(&spilled));
    }
}

#[test]
fn is_spill_candidate_true_only_for_a_resident_entry_with_nonzero_weight() {
    let resident_hot = Live::<u32, String>::new(
        build_resident_record(key_bytes(1).as_ref(), b"v"),
        hlc(1, 1),
        NEVER_EXPIRES,
        3,
        0,
        #[cfg(feature = "spill")]
        EntryState::Resident,
    );
    assert!(is_spill_candidate(&resident_hot));

    let resident_pending = Live::<u32, String>::new(
        build_resident_record(key_bytes(1).as_ref(), b"v"),
        hlc(1, 1),
        NEVER_EXPIRES,
        0,
        0,
        #[cfg(feature = "spill")]
        EntryState::Resident,
    );
    assert!(
        !is_spill_candidate(&resident_pending),
        "weight zero means a hand-off to the spill tier is already in flight"
    );

    #[cfg(feature = "spill")]
    {
        let spilled = Live::<u32, String>::new(
            build_key_only_record(key_bytes(1).as_ref()),
            hlc(1, 1),
            NEVER_EXPIRES,
            0,
            0,
            EntryState::Spilled(SpillLoc {
                region: 0,
                offset: 0,
                len: 1,
                generation: 0,
            }),
        );
        assert!(!is_spill_candidate(&spilled));
    }
}

#[test]
fn build_resident_record_round_trips_key_and_value() {
    for (key_bytes, value_bytes) in [
        (&b""[..], &b""[..]),
        (&b"k"[..], &b"v"[..]),
        (&b"a-longer-key"[..], &b"a-longer-value-too"[..]),
    ] {
        let record = build_resident_record(key_bytes, value_bytes);
        assert_eq!(record_key(&record), key_bytes);
        assert_eq!(record_key_bytes(&record).as_ref(), key_bytes);
        assert_eq!(record_value_bytes(&record).as_ref(), value_bytes);
    }
}

#[test]
fn split_record_recovers_the_value_half_at_every_key_length_boundary() {
    // 127 and below encode LEN in one postcard varint byte; 128 and
    // above spill into a second, crossing the continuation-bit boundary.
    for key_len in [0usize, 1, 127, 128, 129] {
        let key_bytes = vec![b'k'; key_len];
        let value_bytes = b"value".to_vec();
        let record = build_resident_record(&key_bytes, &value_bytes);
        let (key_half, value_half) = split_record(&record);
        assert_eq!(key_half, key_bytes.as_slice(), "key_len = {key_len}");
        assert_eq!(value_half, value_bytes.as_slice(), "key_len = {key_len}");
    }
}

#[test]
fn build_key_only_record_produces_an_empty_value_half() {
    let record = build_key_only_record(b"a-key");
    let (key_half, value_half) = split_record(&record);
    assert_eq!(key_half, b"a-key");
    assert!(value_half.is_empty(), "{value_half:?}");
}

#[test]
fn record_value_matches_split_record_and_is_zero_copy() {
    for (key_bytes, value_bytes) in [
        (&b""[..], &b""[..]),
        (&b"k"[..], &b"v"[..]),
        (&b"a-longer-key"[..], &b"a-longer-value-too"[..]),
    ] {
        let record = build_resident_record(key_bytes, value_bytes);
        let value = record_value(&record);
        assert_eq!(value, split_record(&record).1);
        assert_eq!(value, value_bytes);
    }
}

#[test]
fn encode_expiry_round_trips_never_a_short_delta_and_the_max_inline_ttl_boundary() {
    let wall_ms = 1_000u64;
    let empty: HashMap<Bytes, u64> = HashMap::new();

    let (expiry, side_table) = encode_expiry(wall_ms, None);
    assert_eq!(expiry, NEVER_EXPIRES);
    assert_eq!(side_table, None);
    assert_eq!(decode_expiry(wall_ms, expiry, b"k", &empty), None);

    for delta in [0u64, 1, 50, MAX_INLINE_TTL_MS] {
        let exp = wall_ms + delta;
        let (expiry, side_table) = encode_expiry(wall_ms, Some(exp));
        assert_eq!(side_table, None, "delta = {delta} fits inline");
        assert_ne!(expiry, NEVER_EXPIRES, "delta = {delta}");
        assert_ne!(expiry, LONG_TTL, "delta = {delta}");
        assert_eq!(
            decode_expiry(wall_ms, expiry, b"k", &empty),
            Some(exp),
            "delta = {delta}"
        );
    }
}

#[test]
fn encode_expiry_falls_back_to_the_long_ttl_marker_past_max_inline_ttl_ms() {
    let wall_ms = 1_000u64;
    for exp in [
        wall_ms + MAX_INLINE_TTL_MS + 1,
        wall_ms + MAX_INLINE_TTL_MS + 1_000,
        u64::MAX,
    ] {
        let (expiry, side_table) = encode_expiry(wall_ms, Some(exp));
        assert_eq!(expiry, LONG_TTL, "exp = {exp}");
        assert_eq!(side_table, Some(exp), "exp = {exp}");
    }
}

#[test]
fn encode_expiry_falls_back_to_the_long_ttl_marker_for_a_dead_on_arrival_expiry_before_wall_ms() {
    // expires_at_ms < wall_ms happens (a dead-on-arrival TTL); expiry
    // must not silently round up to wall_ms.
    let wall_ms = 20_000u64;
    for exp in [0u64, wall_ms - 1, wall_ms - 10_135] {
        let (expiry, side_table) = encode_expiry(wall_ms, Some(exp));
        assert_eq!(expiry, LONG_TTL, "exp = {exp}");
        assert_eq!(side_table, Some(exp), "exp = {exp}");
        let mut long_ttl: HashMap<Bytes, u64> = HashMap::new();
        long_ttl.insert(Bytes::from_static(b"k"), exp);
        assert_eq!(
            decode_expiry(wall_ms, expiry, b"k", &long_ttl),
            Some(exp),
            "exp = {exp}"
        );
    }
}

#[test]
fn decode_expiry_reads_the_long_ttl_side_table_for_the_long_ttl_marker() {
    let wall_ms = 1_000u64;
    let mut long_ttl: HashMap<Bytes, u64> = HashMap::new();
    long_ttl.insert(key_bytes(1), 9_999_999_999u64);
    assert_eq!(
        decode_expiry(wall_ms, LONG_TTL, key_bytes(1).as_ref(), &long_ttl),
        Some(9_999_999_999)
    );
    assert_eq!(
        decode_expiry(wall_ms, LONG_TTL, key_bytes(2).as_ref(), &long_ttl),
        None,
        "no row for this key: never invents a deadline that isn't there"
    );
}

#[test]
fn idle_elapsed_ms_is_correct_across_a_touch_stamp_rollover() {
    // No rollover: an ordinary elapsed duration, well under u32::MAX.
    assert_eq!(idle_elapsed_ms(1_000, touch_stamp(400)), 600);
    assert_eq!(idle_elapsed_ms(1_000, touch_stamp(1_000)), 0);

    // now_ms crossed one u32::MAX rollover after the touch: both
    // truncate to their low 32 bits, so wrapping subtraction still works.
    let touched_at = u64::from(u32::MAX) - 50;
    let now = touched_at + 200;
    assert!(
        now > u64::from(u32::MAX),
        "now must actually cross the rollover for this case to mean anything"
    );
    assert_eq!(idle_elapsed_ms(now, touch_stamp(touched_at)), 200);

    // The touch lands just past a rollover, exercised on the truncated
    // stamp directly.
    let last_access_ms: u32 = 5; // wrapped just past 0
    let now_ms = (1u64 << 32) + 105; // one rollover past the touch, true elapsed: 100ms
    assert_eq!(idle_elapsed_ms(now_ms, last_access_ms), 100);
}

#[test]
fn clamp_tti_ms_caps_a_tti_past_max_inline_ttl_ms_to_it() {
    assert_eq!(
        clamp_tti_ms(1_000),
        1_000,
        "well under the ceiling: untouched"
    );
    assert_eq!(
        clamp_tti_ms(MAX_INLINE_TTL_MS),
        MAX_INLINE_TTL_MS,
        "exactly at the ceiling: untouched"
    );
    assert_eq!(
        clamp_tti_ms(MAX_INLINE_TTL_MS + 1),
        MAX_INLINE_TTL_MS,
        "one past the ceiling: capped"
    );
    assert_eq!(
        clamp_tti_ms(u64::MAX),
        MAX_INLINE_TTL_MS,
        "far past the ceiling: capped"
    );
}

#[test]
fn engine_new_clamps_a_configured_tti_past_max_inline_ttl_ms() {
    let past_ceiling = Duration::from_millis(MAX_INLINE_TTL_MS + 10_000);
    let engine = engine_u32_string(u64::MAX, Some(past_ceiling));
    assert_eq!(engine.tti_ms, Some(MAX_INLINE_TTL_MS));
}

#[test]
fn engine_capacity_hint_presizes_each_stripe_to_ceil_hint_over_bucket_count() {
    // Not a multiple of BUCKET_COUNT, so every stripe's own share only
    // divides evenly once rounded up.
    let hint = 3_000u64;
    let expected = usize::try_from(hint.div_ceil(BUCKET_COUNT as u64))
        .expect("a hint this small divided by BUCKET_COUNT fits in usize");
    let engine = Engine::<u32, String>::new(u64::MAX, None, None, Some(hint));
    assert_eq!(
        engine.stripe_capacities(),
        vec![expected; BUCKET_COUNT],
        "every stripe presizes to ceil(hint / BUCKET_COUNT), none left at zero or grown past it"
    );
}

#[test]
fn engine_no_capacity_hint_allocates_nothing_until_first_write() {
    let engine = Engine::<u32, String>::new(u64::MAX, None, None, None);
    assert_eq!(
        engine.stripe_capacities(),
        vec![0; BUCKET_COUNT],
        "no hint: every stripe starts exactly as Stripe::new leaves it, zero allocation"
    );
    let _ = put(&engine, 1, key_bytes(1), "a".into(), hlc(1, 1), None, 0);
    let total: usize = engine.stripe_capacities().into_iter().sum();
    assert!(
        total > 0,
        "the first write grows only the one stripe its key lands in, matching today's \
         unhinted behavior exactly"
    );
}

#[test]
fn tti_past_max_inline_ttl_ms_still_expires_within_the_clamped_bound() {
    // With no clamp, a tti this far past MAX_INLINE_TTL_MS would leave
    // the entry readable forever. Two separate engines, since a read
    // would reset the idle clock the second assertion checks.
    let past_ceiling = Duration::from_millis(MAX_INLINE_TTL_MS + 10_000);

    let still_within = engine_u32_string(u64::MAX, Some(past_ceiling));
    let _ = put(
        &still_within,
        1,
        key_bytes(1),
        "a".into(),
        hlc(1, 1),
        None,
        0,
    );
    assert_eq!(
        still_within.get(&1, MAX_INLINE_TTL_MS - 1),
        Some("a".to_string()),
        "still within the clamped tti"
    );

    let past_clamp = engine_u32_string(u64::MAX, Some(past_ceiling));
    let _ = put(&past_clamp, 1, key_bytes(1), "a".into(), hlc(1, 1), None, 0);
    assert_eq!(
        past_clamp.get(&1, MAX_INLINE_TTL_MS),
        None,
        "idle past the clamped tti"
    );
}

#[test]
fn decode_key_and_decode_value_round_trip_postcard_bytes() {
    let key_bytes = postcard::to_stdvec(&42u32).expect("u32 encodes");
    let value_bytes = postcard::to_stdvec(&"a value".to_string()).expect("string encodes");
    assert_eq!(decode_key::<u32>(&key_bytes), 42u32);
    assert_eq!(decode_value::<String>(&value_bytes), "a value".to_string());
}

/// Pins `Live`'s non-spill size: `record` (24) + `wall_ms` (8) +
/// `node` (8) + four 4-byte fields (16) = 56, no padding.
#[test]
#[cfg(not(feature = "spill"))]
fn live_size_is_56_bytes_without_spill() {
    assert_eq!(std::mem::size_of::<Live<String, String>>(), 56);
}

/// Pins `Live`'s spill-enabled size: `EntryState` adds a 1-byte tag
/// plus padding, 76 bytes rounded up to 80 for 8-byte alignment.
#[test]
#[cfg(feature = "spill")]
fn live_size_is_80_bytes_with_spill() {
    assert_eq!(std::mem::size_of::<Live<String, String>>(), 80);
}

#[test]
fn live_ver_round_trips_through_set_ver() {
    let mut live = Live::<u32, String>::new(
        build_resident_record(key_bytes(1).as_ref(), b"v"),
        hlc(1, 1),
        NEVER_EXPIRES,
        1,
        0,
        #[cfg(feature = "spill")]
        EntryState::Resident,
    );
    assert_eq!(live.ver(), hlc(1, 1));
    let new_ver = Hlc {
        wall_ms: 9,
        logical: 3,
        node: NodeId::merge_derived(7),
    };
    live.set_ver(new_ver);
    assert_eq!(live.ver(), new_ver);
}

#[test]
fn evict_outcome_made_no_progress_only_when_nothing_was_freed() {
    assert!(EvictOutcome::default().made_no_progress());
    assert!(
        !EvictOutcome {
            removed_weight: 1,
            deficit: 0,
            refused: false,
        }
        .made_no_progress()
    );
}

#[test]
fn a_pending_spill_entry_is_never_sampled_as_a_victim() {
    // A weight-0 Resident entry, exactly what a successful hand-off
    // leaves behind while the flusher's install is still in flight,
    // must never be picked a second time: not by the single-victim
    // sampler, and not by the batch one.
    let weigher: Weigher<u32, String> =
        Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
    let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let _ = put(&engine, key, kb.clone(), "x".repeat(5), hlc(1, 1), None, 0);
    {
        let mut stripe = engine.stripe_lock(bucket).write();
        let live = stripe
            .live
            .find_mut(hash, |l| record_key(&l.record) == kb.as_ref())
            .expect("entry is present");
        live.weight = 0;
    }

    assert!(
        engine.evict_one_sampled(bucket, 0).made_no_progress(),
        "the only entry in this stripe is pending; single-victim sampling finds nothing"
    );
    assert!(
        engine
            .evict_batch_sampled(bucket, 100, 0)
            .made_no_progress(),
        "the only entry in this stripe is pending; batch sampling finds nothing either"
    );
    assert_eq!(
        engine.get(&key, 0),
        Some("x".repeat(5)),
        "the pending entry is untouched: still resident, still readable"
    );
}

#[test]
fn sampled_eviction_still_picks_the_coldest_entry_across_a_last_access_rollover() {
    // key_old's touch predates a touch_stamp rollover; a naive
    // min_by_key over raw stamps would wrongly call key_new colder.
    let target_bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(0).as_ref()));
    let key_old = 0u32;
    let mut candidate = 1u32;
    let key_new = loop {
        if stripe_index_from_hash(hash_key_bytes(key_bytes(candidate).as_ref())) == target_bucket {
            break candidate;
        }
        candidate += 1;
    };

    let engine = Engine::<u32, String>::new(u64::MAX, None, None, None);
    let before_rollover = (1u64 << 32) - 100;
    let after_rollover = (1u64 << 32) + 50;
    let _ = put(
        &engine,
        key_old,
        key_bytes(key_old),
        "old".to_string(),
        hlc(1, 1),
        None,
        before_rollover,
    );
    let _ = put(
        &engine,
        key_new,
        key_bytes(key_new),
        "new".to_string(),
        hlc(2, 1),
        None,
        after_rollover,
    );

    let now_ms = after_rollover + 10;
    let outcome = engine.evict_one_sampled(target_bucket, now_ms);
    assert_eq!(outcome.removed_weight, 1, "one entry evicted");
    assert_eq!(
        engine.get(&key_old, now_ms),
        None,
        "key_old, touched longest ago in true time, is the one evicted"
    );
    assert_eq!(
        engine.get(&key_new, now_ms),
        Some("new".to_string()),
        "key_new survives: its truncated stamp is smaller, but it is the true warmer entry"
    );
}

#[test]
fn evict_one_sampled_breaks_a_last_access_tie_by_arena_order() {
    // key_first/key_second share a bucket and the same now_ms, so
    // idle_elapsed_ms ties; the winner comes down to arena order in
    // sample_candidates feeding max_by_key, which returns the last
    // equally-maximal element. Pins today's resolution as a regression
    // guard, not a documented contract.
    let (key_first, key_second, bucket) = same_bucket_pair(100_000);
    let engine = Engine::<u32, String>::new(u64::MAX, None, None, None);
    let now = 0u64;
    let _ = put(
        &engine,
        key_first,
        key_bytes(key_first),
        "first".to_string(),
        hlc(1, 1),
        None,
        now,
    );
    let _ = put(
        &engine,
        key_second,
        key_bytes(key_second),
        "second".to_string(),
        hlc(2, 1),
        None,
        now,
    );

    let outcome = engine.evict_one_sampled(bucket, now);
    assert_eq!(outcome.removed_weight, 1, "one entry evicted");
    assert_eq!(
        engine.get(&key_first, now),
        None,
        "today's concrete tie-break: the first-inserted key is evicted"
    );
    assert_eq!(
        engine.get(&key_second, now),
        Some("second".to_string()),
        "today's concrete tie-break: the second-inserted key survives"
    );
}

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

/// Coverage for [`Engine::apply_many`]'s pre-fold: that a non-merging
/// resolver never triggers it, that a run never crosses a tombstone,
/// and, the property [`prefold_batch`]'s and [`fold_run`]'s docs argue
/// for from the resolver's join-semilattice contract, that
/// pre-fold changes only how many `apply_locked` calls a batch costs,
/// never the `(version, bytes)` it leaves stored or the set of keys it
/// reports a real outcome for.
mod apply_many_prefold;

mod apply;
mod read;
mod serve;

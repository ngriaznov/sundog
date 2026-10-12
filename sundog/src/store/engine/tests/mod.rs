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

/// Test-only resolver: whenever both sides carry a value, merges by
/// taking the lexicographically greater decoded `String`, an
/// intentionally trivial join-semilattice (`max` is commutative,
/// associative, and idempotent) that lets a test assert the engine wires
/// a real [`ConflictResolver::merge`] reply through end to end, including
/// the redelivery no-op case. Falls back to plain `Hlc` order whenever
/// either side is a tombstone or spilled.
struct MaxStringResolver;

impl ConflictResolver for MaxStringResolver {
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
        let da: String = postcard::from_bytes(av).expect("test value decodes");
        let db: String = postcard::from_bytes(bv).expect("test value decodes");
        let merged = da.max(db);
        Some(Merged {
            value: Bytes::from(postcard::to_stdvec(&merged).expect("merged value encodes")),
            expires_at_ms: None,
        })
    }
}

/// Test-only resolver whose `merge` always claims a merge, regardless of
/// whether those bytes decode as the shard's value type, and whose
/// `winner` falls back to plain `Hlc` order, like `LwwResolver`: the
/// buggy-resolver shape [`resolve_conflict`]'s engine-enforced
/// value-presence guard exists to contain.
struct AlwaysMergeResolver {
    /// The bytes to hand back as the "merged" value; deliberately
    /// invalid postcard for the decode-failure test.
    value: &'static [u8],
}

impl ConflictResolver for AlwaysMergeResolver {
    fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        if a.ver >= b.ver { Winner::A } else { Winner::B }
    }

    fn merges(&self) -> bool {
        true
    }

    fn merge(&self, _key: &[u8], _a: RecordView<'_>, _b: RecordView<'_>) -> Option<Merged> {
        Some(Merged {
            value: Bytes::from_static(self.value),
            expires_at_ms: None,
        })
    }
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

#[test]
fn apply_if_current_applies_only_while_the_entry_is_at_the_expected_version() {
    let engine = engine_u32_string(u64::MAX, None);
    let now = 2_000;
    let base = hlc(1_000, 1);
    put(
        &engine,
        1u32,
        key_bytes(1),
        "a".to_string(),
        base,
        Some(5_000),
        now,
    );
    let hash = hash_key_bytes(key_bytes(1).as_ref());

    let stale = engine.apply_if_current(
        put_entry(1, "a", hlc(1_000, 2), Some(9_000)),
        hlc(999, 1),
        &LwwResolver,
        60_000,
        600_000,
        now,
    );
    assert!(
        stale.is_none(),
        "an expected version the entry is not at applies nothing"
    );
    assert_eq!(
        engine.ttl_of(key_bytes(1).as_ref(), hash, now),
        Some(remaining(3_000))
    );

    let rearmed = engine.apply_if_current(
        put_entry(1, "a", hlc(1_000, 2), Some(9_000)),
        base,
        &LwwResolver,
        60_000,
        600_000,
        now,
    );
    assert!(matches!(
        rearmed,
        Some(ApplyOutcome::Put { created: false, .. })
    ));
    assert_eq!(
        engine.ttl_of(key_bytes(1).as_ref(), hash, now),
        Some(remaining(7_000))
    );
    assert_eq!(engine.get(&1, now), Some("a".to_string()));
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
    assert_eq!(engine.live_entry_count(), 1);

    tombstone::<u32, String>(&engine, 1, key_bytes(1), hlc(1_500, 1), now);
    let over_tombstone = engine.apply_if_current(
        put_entry(1, "a", hlc(1_600, 1), None),
        hlc(1_000, 2),
        &LwwResolver,
        60_000,
        600_000,
        now,
    );
    assert!(
        over_tombstone.is_none(),
        "a tombstoned key never takes a gated put"
    );
    assert_eq!(engine.get(&1, now), None);
}

#[test]
fn apply_if_current_refuses_an_entry_past_its_expiry() {
    let engine = engine_u32_string(u64::MAX, None);
    let base = hlc(1_000, 1);
    put(
        &engine,
        1u32,
        key_bytes(1),
        "a".to_string(),
        base,
        Some(1_500),
        1_200,
    );
    let applied = engine.apply_if_current(
        put_entry(1, "a", hlc(1_000, 2), Some(9_000)),
        base,
        &LwwResolver,
        60_000,
        600_000,
        2_000,
    );
    assert!(
        applied.is_none(),
        "an expired entry is absent, whatever its version"
    );
    assert_eq!(engine.get(&1, 2_000), None);
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

/// One engine holding each kind of record [`Engine::inspect`] tells
/// apart at `now`: key 1 live with no TTL, 2 live with a TTL, 3 live
/// past the inline deadline range, 4 expired and not swept, 5 a
/// tombstone; 6 is never written.
fn engine_with_every_record(now: u64) -> (Engine<u32, String>, u64) {
    let engine = engine_u32_string(u64::MAX, None);
    let far = 1_000 + u64::from(u32::MAX) * 2;
    for (key, expires_at_ms) in [
        (1, None),
        (2, Some(7_000)),
        (3, Some(far)),
        (4, Some(1_500)),
    ] {
        put(
            &engine,
            key,
            key_bytes(key),
            format!("v{key}"),
            hlc(1_000, 1),
            expires_at_ms,
            now,
        );
    }
    tombstone(&engine, 5u32, key_bytes(5), hlc(1_200, 2), now);
    (engine, far)
}

#[test]
fn inspect_reports_absent_tombstone_live_and_lapsed() {
    let now = 2_000;
    let (engine, far) = engine_with_every_record(now);
    let inspect = |key: u32, at: u64| engine.inspect(key_bytes(key).as_ref(), at);
    let live = |expires_at_ms| LocalRecord::Live {
        version: hlc(1_000, 1),
        expires_at_ms,
        spilled: false,
    };
    assert_eq!(inspect(1, now), live(None));
    assert_eq!(inspect(2, now), live(Some(7_000)));
    assert_eq!(
        inspect(3, now),
        live(Some(far)),
        "a deadline past the inline range is read from the long-TTL table"
    );
    assert_eq!(
        inspect(4, now),
        LocalRecord::Lapsed {
            version: hlc(1_000, 1),
            expires_at_ms: Some(1_500),
            cause: Lapse::Expired,
        }
    );
    assert_eq!(
        inspect(5, now),
        LocalRecord::Tombstone {
            version: hlc(1_200, 2)
        }
    );
    assert_eq!(inspect(6, now), LocalRecord::Absent);
    assert_eq!(
        inspect(2, 7_000),
        LocalRecord::Lapsed {
            version: hlc(1_000, 1),
            expires_at_ms: Some(7_000),
            cause: Lapse::Expired,
        },
        "an expiry equal to now has passed"
    );

    engine.sweep(now);
    assert_eq!(
        inspect(4, now),
        LocalRecord::Absent,
        "a swept expired entry is gone"
    );
    assert_eq!(inspect(1, now), live(None), "the sweep keeps a live entry");
}

#[test]
fn inspect_agrees_with_record_for_and_lifetime_of() {
    let now = 2_000;
    let (engine, _) = engine_with_every_record(now);
    for key in 1..=6 {
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let inspected = engine.inspect(kb.as_ref(), now);
        let record = engine.record_for(kb.as_ref(), now);
        assert_eq!(inspected.is_held(), record.is_some(), "key {key}");
        let lifetime = engine.lifetime_of(kb.as_ref(), hash, now);
        assert_eq!(inspected.is_live(), lifetime.is_some(), "key {key}");
        match inspected {
            LocalRecord::Live {
                version,
                expires_at_ms,
                ..
            } => {
                assert_eq!(lifetime, Some((version, expires_at_ms)), "key {key}");
                let record = record.expect("a live entry has a record");
                assert_eq!(
                    (record.ver, record.expires_at_ms),
                    (version, expires_at_ms),
                    "key {key}"
                );
            }
            LocalRecord::Tombstone { version } => {
                assert_eq!(record.map(|r| r.ver), Some(version), "key {key}");
            }
            LocalRecord::Absent | LocalRecord::Lapsed { .. } => {}
        }
    }
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

#[test]
fn inspect_reports_an_idle_entry_lapsed_and_never_resets_the_idle_clock() {
    let engine = engine_u32_string(10, Some(Duration::from_millis(100)));
    put(
        &engine,
        1u32,
        key_bytes(1),
        "idle".to_string(),
        hlc(0, 1),
        None,
        0,
    );
    put(
        &engine,
        2u32,
        key_bytes(2),
        "both".to_string(),
        hlc(0, 1),
        Some(120),
        0,
    );
    let inspect = |key: u32, at: u64| engine.inspect(key_bytes(key).as_ref(), at);
    assert!(inspect(1, 90).is_live());
    assert_eq!(last_access(&engine, 1), 0, "an inspect is no access");
    assert_eq!(
        engine.get(&1, 140),
        None,
        "idle since the write at 0, not the inspect at 90"
    );
    assert_eq!(
        inspect(1, 140),
        LocalRecord::Lapsed {
            version: hlc(0, 1),
            expires_at_ms: None,
            cause: Lapse::Idle,
        }
    );
    assert_eq!(
        inspect(2, 140),
        LocalRecord::Lapsed {
            version: hlc(0, 1),
            expires_at_ms: Some(120),
            cause: Lapse::Expired,
        },
        "an entry both idle and expired reads expired"
    );
    assert_eq!(last_access(&engine, 2), 0);
}

#[test]
fn ttl_of_reads_each_kind_of_deadline_and_nothing_for_an_absent_key() {
    let engine = engine_u32_string(u64::MAX, None);
    let now = 2_000;
    let deadline = |key: u32| {
        let kb = key_bytes(key);
        engine.ttl_of(kb.as_ref(), hash_key_bytes(kb.as_ref()), now)
    };
    put(
        &engine,
        1u32,
        key_bytes(1),
        "never".to_string(),
        hlc(1_000, 1),
        None,
        now,
    );
    put(
        &engine,
        2u32,
        key_bytes(2),
        "soon".to_string(),
        hlc(1_000, 1),
        Some(7_000),
        now,
    );
    let far = 1_000 + u64::from(u32::MAX) * 2;
    put(
        &engine,
        3u32,
        key_bytes(3),
        "far".to_string(),
        hlc(1_000, 1),
        Some(far),
        now,
    );
    put(
        &engine,
        4u32,
        key_bytes(4),
        "gone".to_string(),
        hlc(1_000, 1),
        Some(1_500),
        now,
    );

    assert_eq!(
        deadline(1),
        Some(Ttl::Never),
        "an entry with no TTL never expires"
    );
    assert_eq!(deadline(2), Some(remaining(5_000)));
    assert_eq!(
        deadline(3),
        Some(remaining(far - now)),
        "a deadline past the inline range"
    );
    assert_eq!(
        deadline(4),
        None,
        "an expired entry has no deadline to read"
    );
    assert_eq!(deadline(5), None, "nor does a key never written");
}

#[test]
fn apply_locked_merge_of_identical_content_adopts_the_greater_version_or_no_ops() {
    let engine = engine_u32_string(u64::MAX, None);
    let resolver = MaxStringResolver;
    let va = hlc(1, 1);
    let vb = hlc(5, 2);
    let created = put_with_resolver(&engine, 1, key_bytes(1), "x".into(), va, None, 0, &resolver);
    assert!(matches!(created, ApplyOutcome::Put { created: true, .. }));

    // `max("x", "x") == "x"`: the merge reduces to bytes both sides
    // already share, so this is pure version reconciliation. `vb` is the
    // real greater version, so it is adopted verbatim.
    let reconciled =
        put_with_resolver(&engine, 1, key_bytes(1), "x".into(), vb, None, 0, &resolver);
    assert!(matches!(reconciled, ApplyOutcome::Put { .. }));
    let after_vb = engine
        .record_for(key_bytes(1).as_ref(), 0)
        .expect("key is live");
    assert_eq!(
        after_vb.ver, vb,
        "identical content adopts whichever input version is greater"
    );

    // `vc` carries the same content again but is the lesser version:
    // `sv` (now `vb`) is already the greater side, so this no-ops.
    let vc = hlc(3, 3);
    let no_op = put_with_resolver(&engine, 1, key_bytes(1), "x".into(), vc, None, 0, &resolver);
    assert!(
        matches!(no_op, ApplyOutcome::Rejected),
        "identical content at a lesser version is a no-op once the greater version is stored"
    );
    let still_stored = engine
        .record_for(key_bytes(1).as_ref(), 0)
        .expect("key is still live");
    assert_eq!(
        still_stored.ver, vb,
        "the stored version is untouched by the no-op"
    );
}

#[test]
fn apply_locked_merge_that_reduces_to_incoming_adopts_its_exact_version() {
    let engine = engine_u32_string(u64::MAX, None);
    let resolver = MaxStringResolver;
    let va = hlc(1, 1);
    let vb = hlc(2, 2);
    let created = put_with_resolver(&engine, 1, key_bytes(1), "a".into(), va, None, 0, &resolver);
    assert!(matches!(created, ApplyOutcome::Put { created: true, .. }));

    let merged = put_with_resolver(&engine, 1, key_bytes(1), "z".into(), vb, None, 0, &resolver);
    let ApplyOutcome::Put { value, created, .. } = merged else {
        panic!("a Merged outcome must land as an ApplyOutcome::Put");
    };
    assert_eq!(value, "z", "max(\"a\", \"z\") == \"z\"");
    assert!(!created, "a merge replaces an already-live entry");
    assert_eq!(engine.get(&1, 0), Some("z".to_string()));

    let stored = engine
        .record_for(key_bytes(1).as_ref(), 0)
        .expect("merged key is live");
    assert_eq!(
        stored.ver, vb,
        "the merge reduces to incoming's own bytes and incoming is the real greater \
         version, so its exact version is adopted verbatim"
    );
    assert!(
        !stored.ver.node.is_merge_derived(),
        "adopting incoming's own version carries its real node id, never a minted one"
    );
}

#[test]
fn apply_locked_merge_that_reduces_to_stored_is_a_no_op_when_stored_already_dominates() {
    let engine = engine_u32_string(u64::MAX, None);
    let resolver = MaxStringResolver;
    let va = hlc(5, 1);
    let created = put_with_resolver(&engine, 1, key_bytes(1), "z".into(), va, None, 0, &resolver);
    assert!(matches!(created, ApplyOutcome::Put { created: true, .. }));

    // `max("z", "a") == "z"`: the merge reduces to what's already
    // stored, and `sv` is the real greater version, so nothing changes.
    let vb = hlc(1, 2);
    let no_op = put_with_resolver(&engine, 1, key_bytes(1), "a".into(), vb, None, 0, &resolver);
    assert!(
        matches!(no_op, ApplyOutcome::Rejected),
        "incoming's content and version are both already fully absorbed"
    );
    let after = engine
        .record_for(key_bytes(1).as_ref(), 0)
        .expect("key is still live");
    assert_eq!(after.ver, va);
    assert_eq!(engine.get(&1, 0), Some("z".to_string()));
}

#[test]
fn apply_locked_treats_a_redelivered_absorbed_merge_as_a_no_op() {
    let engine: Engine<u32, std::collections::BTreeSet<String>> =
        Engine::new(u64::MAX, None, None, None);
    let resolver = UnionSetResolver;
    let va = hlc(1, 1);
    let vb = hlc(5, 2);
    let _ = put_with_resolver(
        &engine,
        1,
        key_bytes(1),
        string_set(&["a"]),
        va,
        None,
        0,
        &resolver,
    );
    let _ = put_with_resolver(
        &engine,
        1,
        key_bytes(1),
        string_set(&["b"]),
        vb,
        None,
        0,
        &resolver,
    );
    let before = engine
        .record_for(key_bytes(1).as_ref(), 0)
        .expect("merged key is live");

    // Redeliver `vb`'s own value: its content is already fully absorbed
    // into the stored union (the merge reduces to stored), and the mint
    // above strictly outranks `vb` under `Hlc`'s real order even though
    // their `wall_ms` ties (the mint's `logical` is one higher), so
    // this lands on the no-op arm rather than re-publishing.
    let redelivered = put_with_resolver(
        &engine,
        1,
        key_bytes(1),
        string_set(&["b"]),
        vb,
        None,
        0,
        &resolver,
    );
    assert!(
        matches!(redelivered, ApplyOutcome::Rejected),
        "an absorbed input redelivered unchanged is a no-op, not a fresh write"
    );
    let after = engine
        .record_for(key_bytes(1).as_ref(), 0)
        .expect("merged key is still live");
    assert_eq!(before.ver, after.ver);
    assert_eq!(before.value, after.value);
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
fn apply_locked_merge_that_grows_content_mints_a_strictly_greater_version_even_on_a_wall_ms_tie() {
    let engine: Engine<u32, std::collections::BTreeSet<String>> =
        Engine::new(u64::MAX, None, None, None);
    let resolver = UnionSetResolver;

    let va = hlc(1, 1);
    let vb = hlc(5, 2);
    let _ = put_with_resolver(
        &engine,
        1,
        key_bytes(1),
        string_set(&["a"]),
        va,
        None,
        0,
        &resolver,
    );
    let first = put_with_resolver(
        &engine,
        1,
        key_bytes(1),
        string_set(&["b"]),
        vb,
        None,
        0,
        &resolver,
    );
    assert!(matches!(first, ApplyOutcome::Put { .. }));

    let after_first = engine
        .record_for(key_bytes(1).as_ref(), 0)
        .expect("merged key is live");
    let first_bytes = postcard::to_stdvec(&string_set(&["a", "b"])).expect("test value encodes");
    assert_eq!(
        after_first.ver,
        Hlc {
            wall_ms: 5,
            logical: 1,
            node: NodeId::merge_derived(xxh3_64(&first_bytes)),
        },
        "the union of two disjoint sides mints wall_ms = max(inputs), \
         logical = max(inputs) + 1, and a node derived from the merged bytes"
    );

    // `vc`'s own content, "c", is absent from both prior inputs, and its
    // `wall_ms` ties the stored version's exactly: the case a bare
    // componentwise max cannot tell apart from a no-op. The `+ 1` on
    // `logical` at mint time keeps this merge strictly ahead of the
    // prior one despite the tie.
    let vc = hlc(5, 3);
    let second = put_with_resolver(
        &engine,
        1,
        key_bytes(1),
        string_set(&["c"]),
        vc,
        None,
        0,
        &resolver,
    );
    assert!(
        matches!(second, ApplyOutcome::Put { .. }),
        "a resolver-driven merge of two real values is never dropped"
    );

    let after_second = engine
        .record_for(key_bytes(1).as_ref(), 0)
        .expect("key is still live");
    assert!(
        after_second.ver > after_first.ver,
        "the second merge's version strictly outranks the first, even though wall_ms tied"
    );
    assert_eq!(
        engine.get(&1, 0),
        Some(string_set(&["a", "b", "c"])),
        "the write's content is never lost"
    );
}

#[test]
fn resolve_conflict_never_consults_merge_against_a_tombstone() {
    let engine = engine_u32_string(u64::MAX, None);
    // Valid postcard for the `String` "hi": what would land if the
    // guard failed to hold and `merge` were wrongly consulted against
    // the tombstone below, instead of the ordinary Hlc-order `winner`
    // pick.
    let resolver = AlwaysMergeResolver {
        value: &[2, b'h', b'i'],
    };
    tombstone(&engine, 1, key_bytes(1), hlc(1, 1), 0);
    assert_eq!(engine.get(&1, 0), None);

    // Genuinely newer than the tombstone by `Hlc`: an ordinary
    // winner-only resolver would let this win outright, and this one
    // must too, since the tombstone's value-less side means `merge` is
    // never consulted no matter what `merges()` claims.
    let outcome = put_with_resolver(
        &engine,
        1,
        key_bytes(1),
        "new".into(),
        hlc(2, 2),
        None,
        0,
        &resolver,
    );
    assert!(matches!(outcome, ApplyOutcome::Put { .. }));
    assert_eq!(
        engine.get(&1, 0),
        Some("new".to_string()),
        "the incoming write's own value lands via the ordinary winner fallback, never the \
         resolver's synthesized merge bytes, since a tombstone's value-less side means \
         merge is never consulted"
    );
}

#[test]
fn apply_locked_rejects_a_merge_whose_bytes_fail_to_decode() {
    let engine = engine_u32_string(u64::MAX, None);
    let lww = LwwResolver;
    let _ = put_with_resolver(
        &engine,
        1,
        key_bytes(1),
        "a".into(),
        hlc(1, 1),
        None,
        0,
        &lww,
    );
    // Not valid postcard for a `String`: an over-long varint length
    // prefix with no data behind it.
    let garbage = AlwaysMergeResolver {
        value: &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
    };
    let outcome = put_with_resolver(
        &engine,
        1,
        key_bytes(1),
        "b".into(),
        hlc(2, 2),
        None,
        0,
        &garbage,
    );
    assert!(
        matches!(outcome, ApplyOutcome::Rejected),
        "bytes that fail to decode as V are rejected, never panicked on"
    );
    assert_eq!(
        engine.get(&1, 0),
        Some("a".to_string()),
        "a rejected merge leaves the previously stored value untouched"
    );
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

#[test]
fn collect_buckets_reports_a_removed_key_with_its_tombstone_version() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 3u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let _ = put(&engine, key, kb.clone(), "a".into(), hlc(1, 1), None, 0);
    tombstone(&engine, key, kb.clone(), hlc(2, 1), 0);
    assert_eq!(engine.get(&key, 0), None);

    let bucket_u16 = u16::try_from(bucket).expect("invariant: bucket < BUCKET_COUNT");
    let entries = engine.collect_buckets(&[bucket_u16], 0);
    assert_eq!(entries.len(), 1);
    let (_, records) = &entries[0];
    assert!(
        records
            .iter()
            .any(|kv| kv.key.as_ref() == kb.as_ref() && kv.version == hlc(2, 1)),
        "a removed key still appears in its bucket's entries, carrying the tombstone's \
         version: {records:?}"
    );
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
fn get_by_bytes_reads_what_get_reads() {
    let engine = engine_u32_string(u64::MAX, None);
    let kb = key_bytes(7);
    let hash = hash_key_bytes(kb.as_ref());
    assert_eq!(engine.get_by_bytes(kb.as_ref(), hash, 0), None);
    let _ = put(
        &engine,
        7,
        kb.clone(),
        "seven".into(),
        hlc(1, 1),
        Some(50),
        0,
    );
    assert_eq!(
        engine.get_by_bytes(kb.as_ref(), hash, 0),
        Some("seven".to_string())
    );
    assert_eq!(engine.get_by_bytes(kb.as_ref(), hash, 0), engine.get(&7, 0));
    assert_eq!(
        engine.get_by_bytes(kb.as_ref(), hash, 100),
        None,
        "an expired entry reads as absent by bytes too"
    );
}

#[test]
fn collect_buckets_ignores_a_bucket_outside_the_stripe_range() {
    let engine = engine_u32_string(u64::MAX, None);
    let kb = key_bytes(1);
    let bucket =
        u16::try_from(stripe_index_from_hash(hash_key_bytes(kb.as_ref()))).expect("bucket fits");
    let _ = put(&engine, 1, kb.clone(), "a".into(), hlc(1, 1), None, 0);

    assert!(
        engine.collect_buckets(&[u16::MAX], 0).is_empty(),
        "a bucket past BUCKET_COUNT yields nothing instead of indexing past the stripes"
    );
    let mixed = engine.collect_buckets(&[u16::MAX, bucket, 1024], 0);
    assert_eq!(mixed.len(), 1, "only the in-range bucket is answered");
    assert_eq!(mixed[0].0, bucket);
    assert_eq!(
        mixed[0].1,
        vec![KeyVersion {
            key: kb,
            version: hlc(1, 1)
        }]
    );
}

#[test]
fn a_write_over_an_expired_unswept_entry_reports_created() {
    let engine = engine_u32_string(u64::MAX, None);
    let kb = key_bytes(1);
    let _ = put(&engine, 1, kb.clone(), "a".into(), hlc(1, 1), Some(50), 0);

    let replaced = put(&engine, 1, kb.clone(), "b".into(), hlc(2, 1), Some(50), 10);
    assert!(
        matches!(replaced, ApplyOutcome::Put { created: false, .. }),
        "a write over a readable entry is an update"
    );
    let over_expired = put(&engine, 1, kb.clone(), "c".into(), hlc(3, 1), Some(100), 60);
    assert!(
        matches!(over_expired, ApplyOutcome::Put { created: true, .. }),
        "a write over an entry a read no longer sees is a creation, sweep or no sweep"
    );
    // The expired entry was still displaced: one live entry, one weight.
    assert_eq!(engine.debug_totals(), (1, 1));
    assert_eq!(engine.get(&1, 60), Some("c".to_string()));
}

#[test]
fn a_write_over_an_idle_entry_reports_created() {
    let engine = engine_u32_string(u64::MAX, Some(Duration::from_millis(100)));
    let kb = key_bytes(1);
    let _ = put(&engine, 1, kb.clone(), "a".into(), hlc(1, 1), None, 0);
    assert_eq!(
        engine.get(&1, 150),
        None,
        "idle past the TTI reads as absent"
    );
    let over_idle = put(&engine, 1, kb, "b".into(), hlc(2, 1), None, 150);
    assert!(matches!(over_idle, ApplyOutcome::Put { created: true, .. }));
    assert_eq!(engine.debug_totals(), (1, 1));
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
fn engine_reads_back_an_arc_string_value_serialized_via_serdes_rc_feature() {
    let engine = Engine::<u32, Arc<String>>::new(u64::MAX, None, None, None);
    let value = Arc::new("shared".to_string());
    let _ = put(
        &engine,
        1,
        key_bytes(1),
        Arc::clone(&value),
        hlc(1, 1),
        None,
        0,
    );
    assert_eq!(
        engine.get(&1, 0),
        Some(value),
        "an Arc<String> value round-trips through postcard's serde `rc` support"
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

/// The recomputed part digests as `(bucket, part, digest)` triples, paired
/// with the bucket each flat index belongs to, for tests that check part
/// digests directly rather than only their bucket aggregate.
fn recompute_part_digests_paired<K, V>(engine: &Engine<K, V>) -> Vec<(u16, u8, u64)>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let parts = engine.recompute_digests();
    (0..BUCKET_COUNT)
        .flat_map(|bucket| {
            let parts = &parts;
            (0..PART_COUNT).map(move |part| {
                (
                    u16::try_from(bucket).expect("fits"),
                    u8::try_from(part).expect("fits"),
                    parts[digest_slot(bucket, part)],
                )
            })
        })
        .collect()
}

/// One key's mirror state for the interleaving test below.
enum MirrorState {
    Live(String),
    Tombstoned,
}

/// Pins that `get_by_bytes`/`keys`/`digests`/`bucket_len` agree with a
/// fixed-seed put/overwrite/tombstone/evict interleaving's own mirror.
#[test]
fn apply_locked_put_overwrite_tombstone_evict_interleaving_matches_pre_slab_behavior() {
    use rand::{RngExt as _, SeedableRng as _, rngs::StdRng};

    let weigher: Weigher<u32, String> =
        Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
    let engine = Engine::<u32, String>::new(60, None, Some(weigher), None);
    let mut rng = StdRng::seed_from_u64(0x5AB5_1AB5);
    let mut clock = HlcClock::new(NodeId::from(1));
    let mut mirror: HashMap<u32, MirrorState> = HashMap::new();

    for i in 0..2000u64 {
        let key = rng.random_range(0..16u32);
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let now = i * 10;
        match rng.random_range(0..4u32) {
            // Put and overwrite are the same call: a fresh HLC tick
            // always wins.
            0 | 1 => {
                let ver = clock.now(now);
                let value = format!("v{key}-{i}");
                let _ = put(&engine, key, kb, value.clone(), ver, None, now);
                mirror.insert(key, MirrorState::Live(value));
                engine.enforce_capacity(bucket, &mut Written::default(), now);
            }
            2 => {
                let ver = clock.now(now);
                tombstone(&engine, key, kb, ver, now);
                mirror.insert(key, MirrorState::Tombstoned);
            }
            _ => {
                let _ = engine.evict_one_sampled(bucket, now);
            }
        }

        // enforce_capacity/evict_one_sampled can remove any resident
        // entry, so resync every Live mirror row before checking below.
        mirror.retain(|&k, state| match state {
            MirrorState::Live(_) => {
                let kb = key_bytes(k);
                let khash = hash_key_bytes(kb.as_ref());
                engine.get_by_bytes(kb.as_ref(), khash, now).is_some()
            }
            MirrorState::Tombstoned => true,
        });

        for (&k, state) in &mirror {
            if let MirrorState::Live(v) = state {
                let kb = key_bytes(k);
                let khash = hash_key_bytes(kb.as_ref());
                assert_eq!(
                    engine.get_by_bytes(kb.as_ref(), khash, now),
                    Some(v.clone()),
                    "iteration {i}: key {k} diverged from the mirror"
                );
            }
        }

        let mut expected_keys: Vec<u32> = mirror
            .iter()
            .filter_map(|(&k, state)| matches!(state, MirrorState::Live(_)).then_some(k))
            .collect();
        expected_keys.sort_unstable();
        let mut actual_keys = engine.keys(now);
        actual_keys.sort_unstable();
        assert_eq!(
            actual_keys, expected_keys,
            "iteration {i}: keys() diverged from the mirror"
        );

        assert_eq!(
            engine.digests(),
            engine.recompute_digests_paired(),
            "iteration {i}: digests() diverged from a full recompute"
        );

        let mut expected_bucket_counts = vec![0usize; BUCKET_COUNT];
        for &k in mirror.keys() {
            let kb = key_bytes(k);
            let khash = hash_key_bytes(kb.as_ref());
            expected_bucket_counts[stripe_index_from_hash(khash)] += 1;
        }
        for (b, &expected) in expected_bucket_counts.iter().enumerate() {
            let bucket_u16 = u16::try_from(b).expect("fits");
            assert_eq!(
                engine.bucket_len(bucket_u16),
                expected,
                "iteration {i}: bucket {b}'s len diverged from the mirror"
            );
        }
    }
}

#[test]
fn bucket_len_counts_live_entries_and_tombstones_without_materializing_them() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 5u32;
    let kb = key_bytes(key);
    let bucket = u16::try_from(stripe_index_from_hash(hash_key_bytes(kb.as_ref()))).expect("fits");
    assert_eq!(engine.bucket_len(bucket), 0);
    let _ = put(&engine, key, kb, "a".into(), hlc(1, 1), None, 0);
    assert_eq!(engine.bucket_len(bucket), 1);
    assert_eq!(
        engine.bucket_len(u16::MAX),
        0,
        "a bucket past BUCKET_COUNT counts as 0, not an index panic"
    );
}

#[test]
fn part_digests_xor_to_the_bucket_digest() {
    use rand::{RngExt as _, SeedableRng as _, rngs::StdRng};

    let engine = engine_u32_string(u64::MAX, None);
    let mut rng = StdRng::seed_from_u64(0xFEED_1234);
    for i in 0..200u32 {
        let ver = hlc(u64::from(i) + 1, 1);
        let _ = put(&engine, i, key_bytes(i), i.to_string(), ver, None, 0);
        let _ = rng.random_range(0..1u32);
    }
    for (bucket, digest) in engine.digests() {
        let parts = engine.part_digests(bucket);
        assert_eq!(parts.len(), PART_COUNT);
        let xored = parts.iter().fold(0u64, |acc, d| acc ^ d);
        assert_eq!(
            xored, digest,
            "bucket {bucket}'s part digests XOR to its bucket digest"
        );
    }
    assert_eq!(
        recompute_part_digests_paired(&engine)
            .into_iter()
            .map(|(bucket, part, digest)| {
                let _ = part;
                (bucket, digest)
            })
            .fold(std::collections::HashMap::new(), |mut acc, (b, d)| {
                *acc.entry(b).or_insert(0u64) ^= d;
                acc
            }),
        engine.digests().into_iter().collect(),
        "the full recompute agrees with the incrementally maintained part digests"
    );
}

#[test]
fn collect_parts_returns_exactly_that_parts_entries_including_tombstones() {
    let engine = engine_u32_string(u64::MAX, None);
    // Find two keys sharing a bucket but landing in different parts, and a
    // third key in a different bucket entirely.
    let mut by_bucket_part: HashMap<(usize, usize), u32> = HashMap::new();
    let mut same_bucket_diff_part: Option<(u32, u32)> = None;
    let mut candidate = 0u32;
    while same_bucket_diff_part.is_none() {
        let kb = key_bytes(candidate);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let part = part_index_from_hash(hash);
        if let Some(&other) = by_bucket_part
            .iter()
            .find(|((b, p), _)| *b == bucket && *p != part)
            .map(|(_, k)| k)
        {
            same_bucket_diff_part = Some((other, candidate));
        }
        by_bucket_part.insert((bucket, part), candidate);
        candidate += 1;
    }
    let (key_a, key_b) = same_bucket_diff_part.expect("found within the loop above");
    let kb_a = key_bytes(key_a);
    let kb_b = key_bytes(key_b);
    let bucket = stripe_index_from_hash(hash_key_bytes(kb_a.as_ref()));
    let part_a = part_index_from_hash(hash_key_bytes(kb_a.as_ref()));
    let part_b = part_index_from_hash(hash_key_bytes(kb_b.as_ref()));

    let _ = put(&engine, key_a, kb_a.clone(), "a".into(), hlc(1, 1), None, 0);
    // key_b becomes a tombstone, still expected in its part's listing.
    let _ = put(&engine, key_b, kb_b.clone(), "b".into(), hlc(1, 1), None, 0);
    tombstone(&engine, key_b, kb_b.clone(), hlc(2, 1), 0);

    let bucket_u16 = u16::try_from(bucket).expect("fits");
    let req_a = BucketPart {
        bucket: bucket_u16,
        part: u8::try_from(part_a).expect("fits"),
    };
    let req_b = BucketPart {
        bucket: bucket_u16,
        part: u8::try_from(part_b).expect("fits"),
    };
    let result = engine.collect_parts(&[req_a, req_b], 0);
    assert_eq!(result.len(), 2);
    let a_entries = &result
        .iter()
        .find(|(key, _)| *key == req_a)
        .expect("part_a present")
        .1;
    assert_eq!(
        a_entries,
        &vec![KeyVersion {
            key: kb_a,
            version: hlc(1, 1)
        }]
    );
    let b_entries = &result
        .iter()
        .find(|(key, _)| *key == req_b)
        .expect("part_b present")
        .1;
    assert_eq!(
        b_entries,
        &vec![KeyVersion {
            key: kb_b,
            version: hlc(2, 1)
        }],
        "a tombstoned key still appears in its part's listing, at its tombstone version"
    );
}

#[test]
fn collect_parts_ignores_out_of_range_bucket_or_part() {
    let engine = engine_u32_string(u64::MAX, None);
    let kb = key_bytes(1);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = u16::try_from(stripe_index_from_hash(hash)).expect("fits");
    let part = u8::try_from(part_index_from_hash(hash)).expect("fits");
    let _ = put(&engine, 1, kb.clone(), "a".into(), hlc(1, 1), None, 0);

    assert!(
        engine
            .collect_parts(
                &[BucketPart {
                    bucket: u16::MAX,
                    part
                }],
                0
            )
            .is_empty(),
        "a bucket past BUCKET_COUNT yields nothing"
    );
    assert!(
        engine
            .collect_parts(
                &[BucketPart {
                    bucket,
                    part: u8::MAX
                }],
                0
            )
            .is_empty(),
        "a part past PART_COUNT yields nothing"
    );
    let mixed = engine.collect_parts(
        &[
            BucketPart {
                bucket: u16::MAX,
                part,
            },
            BucketPart { bucket, part },
            BucketPart {
                bucket,
                part: u8::MAX,
            },
        ],
        0,
    );
    assert_eq!(
        mixed.len(),
        1,
        "only the in-range (bucket, part) is answered"
    );
    assert_eq!(mixed[0].0, BucketPart { bucket, part });
    assert_eq!(
        mixed[0].1,
        vec![KeyVersion {
            key: kb,
            version: hlc(1, 1)
        }]
    );
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
mod spill_payload {
    use super::*;
    use crate::store::spill::{ReclaimRef, SpillSink};
    #[cfg(not(feature = "sim"))]
    use crate::store::spill::{SpillConfig, SpillTier};

    fn loc(region: u32, offset: u32, len: u32, generation: u32) -> SpillLoc {
        SpillLoc {
            region,
            offset,
            len,
            generation,
        }
    }

    /// [`spilled_read_in_flight`]'s engine, key bytes, hash, the
    /// owner's in-flight entry and the joined waiter's receiver.
    type SpilledReadInFlight = (
        Engine<u32, String>,
        Bytes,
        u64,
        Arc<Inflight<String>>,
        watch::Receiver<bool>,
    );

    /// An engine holding key 7 spilled at `hlc(10, 1)`, unexpiring or
    /// expiring at `expires_at_ms`, with an in-flight read of it that
    /// one owner registered and one waiter joined.
    fn spilled_read_in_flight(expires_at_ms: Option<u64>) -> SpilledReadInFlight {
        let engine = engine_u32_string(u64::MAX, None);
        let kb = key_bytes(7);
        let hash = hash_key_bytes(kb.as_ref());
        engine.debug_insert_spilled(&kb, hlc(10, 1), expires_at_ms, loc(0, 0, 10, 0), 0);
        let JoinOutcome::Owner(inflight) = engine.miss_or_join(&kb, hash, 1) else {
            panic!("a spilled key's first miss owns the read");
        };
        let JoinOutcome::Join(_, done) = engine.miss_or_join(&kb, hash, 1) else {
            panic!("a second miss joins the read in flight");
        };
        (engine, kb, hash, inflight, done)
    }

    #[test]
    fn a_spilled_read_hands_its_value_to_waiters_while_the_key_still_holds_it() {
        let (engine, kb, hash, inflight, done) = spilled_read_in_flight(None);
        engine.finish_spilled_read(&kb, hash, &inflight, hlc(10, 1), "v".to_string(), 2);
        assert_eq!(inflight.value.get(), Some(&"v".to_string()));
        assert!(*done.borrow(), "every waiter wakes");
        assert!(
            matches!(engine.miss_or_join(&kb, hash, 3), JoinOutcome::Owner(_)),
            "the in-flight entry is gone"
        );
    }

    #[test]
    fn a_spilled_read_hands_nothing_to_waiters_once_the_key_moved_on() {
        // Removed after the read.
        let (engine, kb, hash, inflight, done) = spilled_read_in_flight(None);
        tombstone(&engine, 7, kb.clone(), hlc(11, 1), 2);
        engine.finish_spilled_read(&kb, hash, &inflight, hlc(10, 1), "v".to_string(), 2);
        assert_eq!(
            inflight.value.get(),
            None,
            "a removal leaves waiters to read again"
        );
        assert!(*done.borrow());

        // Overwritten after the read.
        let (engine, kb, hash, inflight, _done) = spilled_read_in_flight(None);
        let _ = put(
            &engine,
            7,
            kb.clone(),
            "newer".to_string(),
            hlc(11, 1),
            None,
            2,
        );
        engine.finish_spilled_read(&kb, hash, &inflight, hlc(10, 1), "v".to_string(), 2);
        assert_eq!(
            inflight.value.get(),
            None,
            "a newer write leaves waiters to read again"
        );

        // Expired after the read.
        let (engine, kb, hash, inflight, _done) = spilled_read_in_flight(Some(20));
        engine.finish_spilled_read(&kb, hash, &inflight, hlc(10, 1), "v".to_string(), 30);
        assert_eq!(
            inflight.value.get(),
            None,
            "an expiry leaves waiters to read again"
        );
        assert!(
            matches!(engine.miss_or_join(&kb, hash, 31), JoinOutcome::Owner(_)),
            "the in-flight entry is gone either way"
        );
    }

    #[test]
    fn a_claimed_promotion_that_fails_its_recheck_gives_the_claim_back() {
        let engine = engine_u32_string(100, None);
        let kb = key_bytes(7);
        let hash = hash_key_bytes(kb.as_ref());
        let encoded = Bytes::from(postcard::to_stdvec(&"v".to_string()).expect("string encodes"));
        engine.debug_insert_spilled(&kb, hlc(10, 1), None, loc(0, 0, 10, 0), 0);
        tombstone(&engine, 7, kb.clone(), hlc(11, 1), 1);

        assert!(engine.claim_weight(1));
        assert_eq!(engine.debug_totals().1, 1, "the claim counts at once");
        assert!(
            !engine.reinstall_claimed(kb.as_ref(), hash, hlc(10, 1), 1, &encoded),
            "a removal since the read fails the recheck"
        );
        assert_eq!(
            engine.debug_totals().1,
            0,
            "the failed recheck gives the claim back"
        );

        let kb = key_bytes(8);
        let hash = hash_key_bytes(kb.as_ref());
        engine.debug_insert_spilled(&kb, hlc(10, 1), None, loc(0, 0, 10, 0), 0);
        assert!(engine.claim_weight(1));
        assert!(engine.reinstall_claimed(kb.as_ref(), hash, hlc(10, 1), 1, &encoded));
        assert_eq!(engine.debug_totals().1, 1, "a reinstall keeps its claim");
        assert_eq!(
            engine.get(&8, 2),
            Some("v".to_string()),
            "and the entry is resident"
        );
    }

    #[test]
    fn release_buckets_decrements_the_spilled_entries_gauge_for_a_departing_spilled_entry() {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 1u32;
        let kb = key_bytes(key);
        let bucket = stripe_index_from_hash(hash_key_bytes(kb.as_ref()));
        engine.debug_insert_spilled(&kb, hlc(1, 1), None, loc(0, 0, 4, 0), 0);
        assert_eq!(engine.debug_spill_entries_count(), 1);

        let removed = engine.release_buckets(&[u16::try_from(bucket).expect("invariant: fits")]);

        assert_eq!(
            removed, 1,
            "the spilled entry counts toward the removed total same as a resident one"
        );
        assert_eq!(
            engine.debug_spill_entries_count(),
            0,
            "release_buckets corrects the spilled-entries gauge for a departing spilled entry"
        );
        assert_eq!(engine.live_entry_count(), 0);
    }

    #[test]
    fn snapshot_spilled_reports_pointers_for_spilled_entries_only() {
        let engine = engine_u32_string(u64::MAX, None);
        let key1 = 1u32;
        let kb1 = key_bytes(key1);
        let l = loc(0, 0, 4, 0);
        engine.debug_insert_spilled(&kb1, hlc(1, 1), Some(500), l, 0);
        let key2 = 2u32;
        let kb2 = key_bytes(key2);
        let _ = put(
            &engine,
            key2,
            kb2.clone(),
            "resident".to_string(),
            hlc(1, 1),
            None,
            0,
        );

        let spilled = engine.snapshot_spilled(0);
        assert_eq!(spilled.len(), 1, "only the spilled key is reported here");
        let (k, ver, expires_at_ms, reported_loc) = &spilled[0];
        assert_eq!(k.as_ref(), kb1.as_ref());
        assert_eq!(*ver, hlc(1, 1));
        assert_eq!(*expires_at_ms, Some(500));
        assert_eq!(*reported_loc, l);

        let resident = engine.snapshot_records(0);
        assert_eq!(
            resident.len(),
            1,
            "snapshot_records reports only the resident key"
        );
        assert_eq!(resident[0].key.as_ref(), kb2.as_ref());
    }

    #[test]
    fn records_for_or_spilled_splits_a_resident_and_a_spilled_key() {
        let engine = engine_u32_string(u64::MAX, None);
        let spilled_key = 1u32;
        let kb_spilled = key_bytes(spilled_key);
        let l = loc(0, 0, 4, 0);
        let spilled_ver = hlc(1, 1);
        engine.debug_insert_spilled(&kb_spilled, spilled_ver, Some(500), l, 0);

        let resident_key = 2u32;
        let kb_resident = key_bytes(resident_key);
        let _ = put(
            &engine,
            resident_key,
            kb_resident.clone(),
            "resident".to_string(),
            hlc(2, 1),
            None,
            0,
        );

        let (records, spilled) =
            engine.records_for_or_spilled(&[kb_resident.clone(), kb_spilled.clone()], 0);

        assert_eq!(
            records.len(),
            1,
            "only the resident key comes back as a WireRecord"
        );
        assert_eq!(records[0].key.as_ref(), kb_resident.as_ref());
        assert_eq!(
            records[0].value.as_deref(),
            Some(
                postcard::to_stdvec(&"resident".to_string())
                    .expect("test value encodes")
                    .as_slice()
            )
        );
        assert_eq!(records[0].ver, hlc(2, 1));

        assert_eq!(
            spilled.len(),
            1,
            "only the spilled key comes back as a pointer"
        );
        let (k, ver, expires_at_ms, reported_loc) = &spilled[0];
        assert_eq!(k.as_ref(), kb_spilled.as_ref());
        assert_eq!(*ver, spilled_ver);
        assert_eq!(*expires_at_ms, Some(500));
        assert_eq!(*reported_loc, l);
    }

    #[test]
    fn get_by_bytes_and_record_for_return_none_for_a_spilled_entry() {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 1u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        engine.debug_insert_spilled(&kb, hlc(1, 1), None, loc(0, 0, 4, 0), 0);

        assert_eq!(
            engine.get(&key, 0),
            None,
            "a spilled entry never answers get with a value"
        );
        assert_eq!(engine.get_by_bytes(kb.as_ref(), hash, 0), None);
        assert!(
            engine.contains_key(&key, 0),
            "existence doesn't need the value bytes"
        );
        assert!(
            engine.record_for(kb.as_ref(), 0).is_none(),
            "record_for skips a spilled entry; fan-out simply drops it, repaired later by AE"
        );
    }

    #[test]
    fn miss_or_join_never_returns_hit_for_a_spilled_entry() {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 1u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        engine.debug_insert_spilled(&kb, hlc(1, 1), None, loc(0, 0, 4, 0), 0);

        match engine.miss_or_join(&kb, hash, 0) {
            JoinOutcome::Hit(_) => panic!("a spilled entry has no resident value to hit on"),
            JoinOutcome::Owner(_) | JoinOutcome::Join(..) => {}
        }
    }

    #[test]
    fn inspect_reports_a_spilled_entry_and_leaves_it_spilled() {
        let engine = Engine::<u32, String>::new(10, None, None, None);
        let kb = key_bytes(1);
        let hash = hash_key_bytes(kb.as_ref());
        let ver = hlc(1, 1);
        let l = loc(2, 8, 4, 1);
        engine.debug_insert_spilled(&kb, ver, Some(5_000), l, 0);

        assert_eq!(
            engine.inspect(kb.as_ref(), 100),
            LocalRecord::Live {
                version: ver,
                expires_at_ms: Some(5_000),
                spilled: true,
            }
        );
        assert_eq!(last_access(&engine, 1), 0, "an inspect is no access");
        assert_eq!(
            engine.spilled_loc(kb.as_ref(), hash, 100),
            Some((ver, l)),
            "the entry stays spilled at the same location"
        );
    }

    #[test]
    fn spilled_loc_snapshots_the_pointer_and_touches_last_access() {
        let engine = Engine::<u32, String>::new(10, None, None, None);
        let key = 1u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let ver = hlc(1, 1);
        let l = loc(2, 8, 4, 1);
        engine.debug_insert_spilled(&kb, ver, None, l, 0);

        assert_eq!(engine.spilled_loc(kb.as_ref(), hash, 100), Some((ver, l)));

        let bucket = stripe_index_from_hash(hash);
        let stripe = engine.stripe_lock(bucket).read();
        let live = stripe
            .live
            .iter()
            .find(|live| record_key(&live.record) == kb.as_ref())
            .expect("the entry is present");
        assert_eq!(
            live.last_access_ms.load(Ordering::Relaxed),
            100,
            "spilled_loc touches last_access for a capacity-tracking engine"
        );
        drop(stripe);

        let missing = key_bytes(999);
        assert_eq!(
            engine.spilled_loc(missing.as_ref(), hash_key_bytes(missing.as_ref()), 100),
            None,
            "an absent key yields None"
        );

        let key2 = 2u32;
        let kb2 = key_bytes(key2);
        let _ = put(
            &engine,
            key2,
            kb2.clone(),
            "resident".to_string(),
            hlc(1, 1),
            None,
            0,
        );
        assert_eq!(
            engine.spilled_loc(kb2.as_ref(), hash_key_bytes(kb2.as_ref()), 100),
            None,
            "a resident entry yields None too"
        );
    }

    #[test]
    fn promote_locked_restores_residency_without_touching_the_digest_or_live_count() {
        let engine = engine_u32_string(u64::MAX, None).count_resident_bytes();
        let key = 1u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let ver = hlc(5, 1);
        engine.debug_insert_spilled(&kb, ver, None, loc(0, 0, 10, 0), 0);

        let digest_before = engine.digests();
        let (live_count_before, weight_before) = engine.debug_totals();
        assert_eq!(weight_before, 0, "a spilled entry contributes zero weight");

        let promoted = engine.promote_locked(
            kb.as_ref(),
            hash,
            ver,
            &"restored".to_string(),
            &Bytes::from(postcard::to_stdvec(&"restored".to_string()).expect("string encodes")),
            0,
        );
        assert!(
            promoted,
            "the version matches and nothing displaced it: promotion succeeds"
        );
        assert_eq!(
            engine.resident_bytes(),
            Some(engine.recount_resident_bytes()),
            "promotion adds the restored value's heap bytes to the resident total"
        );

        assert_eq!(engine.get(&key, 0), Some("restored".to_string()));
        assert_eq!(
            engine.digests(),
            digest_before,
            "promotion never touches the digest"
        );
        let (live_count_after, weight_after) = engine.debug_totals();
        assert_eq!(
            live_count_after, live_count_before,
            "promotion never touches live_count"
        );
        assert_eq!(
            weight_after, 1,
            "promotion adds the freshly weighed entry's weight back to total_weight"
        );
    }

    #[test]
    fn promote_locked_adds_the_restored_value_heap_bytes_to_resident_bytes() {
        let engine = engine_u32_string(u64::MAX, None).count_resident_bytes();
        let key = 1u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let ver = hlc(5, 1);
        engine.debug_insert_spilled(&kb, ver, None, loc(0, 0, 10, 0), 0);
        assert_eq!(
            engine.resident_bytes(),
            Some(ENTRY_OVERHEAD_BYTES),
            "a spilled entry keeps only its inline key-only record resident"
        );
        let restored = "r".repeat(100);
        let encoded = Bytes::from(postcard::to_stdvec(&restored).expect("string encodes"));
        assert!(engine.promote_locked(kb.as_ref(), hash, ver, &restored, &encoded, 0));
        assert_eq!(
            engine.resident_bytes(),
            Some(ENTRY_OVERHEAD_BYTES + (1 + kb.len() + encoded.len()) as u64),
            "promotion brings the whole record back onto the heap"
        );
        assert_eq!(
            engine.resident_bytes(),
            Some(engine.recount_resident_bytes())
        );
    }

    #[test]
    fn promote_locked_is_a_noop_once_already_resident() {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 1u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let ver = hlc(5, 1);
        let _ = put(
            &engine,
            key,
            kb.clone(),
            "already-here".to_string(),
            ver,
            None,
            0,
        );

        let promoted = engine.promote_locked(
            kb.as_ref(),
            hash,
            ver,
            &"stale-read".to_string(),
            &Bytes::from_static(b"stale"),
            0,
        );
        assert!(
            !promoted,
            "nothing to promote: the entry is already resident"
        );
        assert_eq!(engine.get(&key, 0), Some("already-here".to_string()));
    }

    #[test]
    fn promote_locked_invokes_the_weigher_with_the_original_key_and_value() {
        let calls: Arc<std::sync::Mutex<Vec<(u32, String)>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = Arc::clone(&calls);
        let weigher: Weigher<u32, String> = Box::new(move |k, v| {
            recorded.lock().expect("lock").push((*k, v.clone()));
            1
        });
        let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
        let key = 1u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let ver = hlc(5, 1);
        engine.debug_insert_spilled(&kb, ver, None, loc(0, 0, 10, 0), 0);

        let promoted = engine.promote_locked(
            kb.as_ref(),
            hash,
            ver,
            &"restored".to_string(),
            &Bytes::from(postcard::to_stdvec(&"restored".to_string()).expect("string encodes")),
            0,
        );
        assert!(promoted);

        let calls = calls.lock().expect("lock");
        assert_eq!(
            calls.as_slice(),
            &[(key, "restored".to_string())],
            "the weigher sees the original key, decoded from key_bytes, and the original \
             value passed in, not a stale or spilled-pointer substitute"
        );
    }

    #[test]
    fn flusher_install_is_a_noop_when_a_newer_write_lands_first() {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 1u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let old_ver = hlc(1, 1);
        let _ = put(
            &engine,
            key,
            kb.clone(),
            "old".to_string(),
            old_ver,
            None,
            0,
        );
        let _ = put(
            &engine,
            key,
            kb.clone(),
            "new".to_string(),
            hlc(2, 1),
            None,
            0,
        );

        let installed = SpillSink::install(&engine, bucket, &kb, hash, old_ver, loc(0, 0, 4, 0), 0);
        assert!(
            !installed,
            "a stale flush is discarded once a newer write has landed"
        );
        assert_eq!(
            engine.get(&key, 0),
            Some("new".to_string()),
            "the newer write is untouched"
        );
    }

    #[test]
    fn flusher_install_is_a_noop_when_a_tombstone_lands_first() {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 1u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let ver = hlc(1, 1);
        let _ = put(&engine, key, kb.clone(), "old".to_string(), ver, None, 0);
        tombstone(&engine, key, kb.clone(), hlc(2, 1), 0);

        let installed = SpillSink::install(&engine, bucket, &kb, hash, ver, loc(0, 0, 4, 0), 0);
        assert!(
            !installed,
            "a stale flush is discarded once a tombstone has landed"
        );
        assert_eq!(
            engine.get(&key, 0),
            None,
            "the key stays deleted; a late flush never resurrects it"
        );
    }

    #[test]
    fn region_reclaim_purges_a_key_still_pointing_at_the_reclaimed_generation() {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 1u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        engine.debug_insert_spilled(&kb, hlc(1, 1), None, loc(3, 0, 10, 0), 0);

        let removed = SpillSink::reclaim(&engine, 3, 0, &[ReclaimRef::new(bucket, hash, 0)]);
        assert_eq!(
            removed, 1,
            "the key's pointer still names this exact region and generation"
        );
        assert_eq!(engine.get(&key, 0), None);
        let (live_count, weight) = engine.debug_totals();
        assert_eq!((live_count, weight), (0, 0));
    }

    #[test]
    fn region_reclaim_of_a_long_ttl_key_clears_its_side_table_row() {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 1u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let long_deadline = MAX_INLINE_TTL_MS + 5_000;
        engine.debug_insert_spilled(&kb, hlc(1, 1), Some(long_deadline), loc(3, 0, 10, 0), 0);
        assert!(
            engine
                .stripe_lock(bucket)
                .read()
                .long_ttl
                .contains_key(kb.as_ref()),
            "a long TTL spilled entry leaves its absolute deadline in the side table"
        );

        let removed = SpillSink::reclaim(&engine, 3, 0, &[ReclaimRef::new(bucket, hash, 0)]);
        assert_eq!(
            removed, 1,
            "the key's pointer still names this exact region and generation"
        );
        assert!(
            !engine
                .stripe_lock(bucket)
                .read()
                .long_ttl
                .contains_key(kb.as_ref()),
            "reclaiming the entry clears its side-table row; no row outlives its entry"
        );
    }

    #[test]
    fn region_reclaim_skips_a_key_that_was_promoted_since_being_recorded() {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 1u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let ver = hlc(1, 1);
        engine.debug_insert_spilled(&kb, ver, None, loc(3, 0, 10, 0), 0);
        assert!(engine.promote_locked(
            kb.as_ref(),
            hash,
            ver,
            &"restored".to_string(),
            &Bytes::from(postcard::to_stdvec(&"restored".to_string()).expect("string encodes")),
            0,
        ));

        let digest_before = engine.digests();
        let removed = SpillSink::reclaim(&engine, 3, 0, &[ReclaimRef::new(bucket, hash, 0)]);
        assert_eq!(
            removed, 0,
            "a key promoted back to resident survives its old region's reclaim"
        );
        assert_eq!(engine.get(&key, 0), Some("restored".to_string()));
        assert_eq!(engine.digests(), digest_before);
    }

    /// A spilled entry at the expected version takes a gated put like a
    /// resident one, and comes back resident with the put's expiry; its
    /// lifetime reads from RAM before and after, with no disk read.
    #[test]
    fn apply_if_current_takes_a_spilled_entry_at_the_expected_version() {
        let engine = engine_u32_string(u64::MAX, None);
        let kb = key_bytes(7);
        let hash = hash_key_bytes(kb.as_ref());
        let base = hlc(1_000, 1);
        engine.debug_insert_spilled(&kb, base, Some(5_000), loc(0, 0, 10, 0), 0);
        assert_eq!(
            engine.ttl_of(kb.as_ref(), hash, 2_000),
            Some(remaining(3_000))
        );

        let encoded = Bytes::from(postcard::to_stdvec("v").expect("encodes"));
        let outcome = engine.apply_if_current(
            (
                hash,
                7,
                kb.clone(),
                hlc(1_000, 2),
                Incoming::Put {
                    value: "v".to_string(),
                    expires_at_ms: Some(9_000),
                    encoded,
                },
            ),
            base,
            &LwwResolver,
            60_000,
            600_000,
            2_000,
        );
        assert!(matches!(outcome, Some(ApplyOutcome::Put { .. })));
        assert_eq!(
            engine.ttl_of(kb.as_ref(), hash, 2_000),
            Some(remaining(7_000))
        );
        assert_eq!(engine.get(&7, 2_000), Some("v".to_string()), "now resident");
        assert_eq!(engine.debug_spill_entries_count(), 0);
    }

    /// Location, not the key hash, decides what a reclaim removes: a row
    /// naming another offset of the same region and generation leaves
    /// the entry alone, and a row naming another stripe removes
    /// nothing.
    #[test]
    fn region_reclaim_removes_only_the_entry_at_the_listed_offset() {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 1u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        engine.debug_insert_spilled(&kb, hlc(1, 1), None, loc(3, 40, 10, 0), 0);

        let elsewhere = SpillSink::reclaim(&engine, 3, 0, &[ReclaimRef::new(bucket, hash, 0)]);
        assert_eq!(
            elsewhere, 0,
            "a row for offset 0 leaves the entry that now sits at offset 40"
        );
        let other_stripe = (bucket + 1) % crate::store::BUCKET_COUNT;
        let unknown = SpillSink::reclaim(&engine, 3, 0, &[ReclaimRef::new(other_stripe, hash, 40)]);
        assert_eq!(unknown, 0, "a row naming another stripe removes nothing");
        assert_eq!(
            engine.spilled_loc(kb.as_ref(), hash, 0).map(|(_, at)| at),
            Some(loc(3, 40, 10, 0)),
            "the entry still points at offset 40"
        );

        let here = SpillSink::reclaim(&engine, 3, 0, &[ReclaimRef::new(bucket, hash, 40)]);
        assert_eq!(here, 1, "the row naming the entry's own offset removes it");
        assert_eq!(engine.get(&key, 0), None);
    }

    #[test]
    fn region_reclaim_skips_a_key_that_was_overwritten_since_being_recorded() {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 1u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        engine.debug_insert_spilled(&kb, hlc(1, 1), None, loc(3, 0, 10, 0), 0);
        let _ = put(
            &engine,
            key,
            kb.clone(),
            "fresh".to_string(),
            hlc(2, 1),
            None,
            0,
        );

        let removed = SpillSink::reclaim(&engine, 3, 0, &[ReclaimRef::new(bucket, hash, 0)]);
        assert_eq!(
            removed, 0,
            "an overwritten key's stale reverse-index row is left alone"
        );
        assert_eq!(engine.get(&key, 0), Some("fresh".to_string()));
    }

    /// Puts `live.weight` at `hash`/`kb` back to `0`, moves `weight` out
    /// of `total_weight` and into `pending_spill_weight`, exactly the
    /// state a successful hand-off to a spill tier leaves behind while
    /// the flusher's write is still in flight. Lets a test drive
    /// `SpillSink::abandon`/`SpillSink::install` directly, the same way
    /// this module already drives `reclaim` directly, with no real disk
    /// or flusher thread needed.
    fn simulate_pending_handoff<K, V>(
        engine: &Engine<K, V>,
        bucket: usize,
        hash: u64,
        kb: &Bytes,
        weight: u32,
    ) where
        K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
        V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    {
        {
            let mut stripe = engine.stripe_lock(bucket).write();
            let live = stripe
                .live
                .find_mut(hash, |l| record_key(&l.record) == kb.as_ref())
                .expect("entry is present");
            live.weight = 0;
        }
        engine
            .total_weight
            .fetch_sub(u64::from(weight), Ordering::Relaxed);
        engine
            .pending_spill_weight
            .fetch_add(u64::from(weight), Ordering::Relaxed);
    }

    #[test]
    fn abandon_restores_the_weight_of_a_still_pending_entry() {
        let weigher: Weigher<u32, String> =
            Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
        let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
        let key = 1u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let ver = hlc(1, 1);
        let _ = put(&engine, key, kb.clone(), "x".repeat(7), ver, None, 0);
        let (_, weight_before) = engine.debug_totals();
        assert_eq!(weight_before, 7);

        simulate_pending_handoff(&engine, bucket, hash, &kb, 7);
        let (_, weight_pending) = engine.debug_totals();
        assert_eq!(weight_pending, 0);
        assert_eq!(
            engine.debug_pending_spill_weight(),
            7,
            "the hand-off's weight moved into pending_spill_weight"
        );

        SpillSink::abandon(&engine, bucket, &kb, hash, ver, 7);

        let (_, weight_after) = engine.debug_totals();
        assert_eq!(
            weight_after, 7,
            "abandon recomputes the weight through the weigher and adds it back to \
             total_weight"
        );
        assert_eq!(
            engine.debug_pending_spill_weight(),
            0,
            "abandon moves the weight back out of pending_spill_weight too"
        );
        let stripe = engine.stripe_lock(bucket).read();
        let live = stripe
            .live
            .iter()
            .find(|l| record_key(&l.record) == kb.as_ref())
            .expect("entry is present");
        assert_eq!(
            live.weight, 7,
            "the entry's own weight field is restored too, not just the total"
        );
    }

    #[test]
    fn abandon_is_a_noop_once_the_keys_stored_state_has_changed() {
        let weigher: Weigher<u32, String> =
            Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
        let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
        let key = 1u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let old_ver = hlc(1, 1);
        let _ = put(&engine, key, kb.clone(), "x".repeat(7), old_ver, None, 0);
        simulate_pending_handoff(&engine, bucket, hash, &kb, 7);

        // A fresh write lands before the abandoned job is ever noticed:
        // this is exactly what `apply_put`'s own weight bookkeeping
        // already resolved correctly, so abandon must leave it alone.
        let _ = put(
            &engine,
            key,
            kb.clone(),
            "fresh".to_string(),
            hlc(2, 1),
            None,
            0,
        );
        let (_, weight_before_abandon) = engine.debug_totals();
        assert_eq!(weight_before_abandon, 5, "\"fresh\".len() == 5");

        SpillSink::abandon(&engine, bucket, &kb, hash, old_ver, 7);

        let (_, weight_after) = engine.debug_totals();
        assert_eq!(
            weight_after, weight_before_abandon,
            "a key whose stored state changed since hand-off is left alone"
        );
        assert_eq!(engine.get(&key, 0), Some("fresh".to_string()));
        assert_eq!(
            engine.debug_pending_spill_weight(),
            0,
            "the job's weight leaves pending_spill_weight even though the key moved on"
        );
    }

    #[test]
    fn abandon_on_a_key_that_is_no_longer_present_is_a_noop() {
        let engine = engine_u32_string(u64::MAX, None);
        let kb = key_bytes(999);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let (_, weight_before) = engine.debug_totals();

        SpillSink::abandon(&engine, bucket, &kb, hash, hlc(1, 1), 0);

        let (_, weight_after) = engine.debug_totals();
        assert_eq!(weight_after, weight_before);
    }

    #[test]
    fn install_releases_its_share_of_pending_spill_weight() {
        let weigher: Weigher<u32, String> =
            Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
        let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
        let key = 1u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let ver = hlc(1, 1);
        let _ = put(&engine, key, kb.clone(), "x".repeat(9), ver, None, 0);
        simulate_pending_handoff(&engine, bucket, hash, &kb, 9);
        assert_eq!(engine.debug_pending_spill_weight(), 9);

        let installed = SpillSink::install(&engine, bucket, &kb, hash, ver, loc(0, 0, 4, 0), 9);
        assert!(installed);

        assert_eq!(
            engine.debug_pending_spill_weight(),
            0,
            "install moves the hand-off's weight out of pending_spill_weight, not just \
             the entry's own payload"
        );
        let (_, weight_after) = engine.debug_totals();
        assert_eq!(
            weight_after, 0,
            "install never adds to total_weight; the entry stays at weight 0, Spilled"
        );
    }

    #[test]
    fn install_copies_the_key_into_a_fresh_allocation_instead_of_slicing() {
        // A key over INLINE_CAP forces the heap path, where a
        // pointer-identity check is meaningful.
        let engine =
            Engine::<String, String>::new(u64::MAX, None, None, None).count_resident_bytes();
        let key = "k".repeat(40);
        let kb = string_key_bytes(&key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let ver = hlc(1, 1);
        let _ = put(&engine, key, kb.clone(), "value".to_string(), ver, None, 0);
        simulate_pending_handoff(&engine, bucket, hash, &kb, 1);

        let record_ptr_before = {
            let stripe = engine.stripe_lock(bucket).read();
            let live = stripe
                .live
                .iter()
                .find(|l| record_key(&l.record) == kb.as_ref())
                .expect("entry is present");
            live.record.as_ref().as_ptr()
        };

        let installed = SpillSink::install(&engine, bucket, &kb, hash, ver, loc(0, 0, 4, 0), 1);
        assert!(installed);

        let record_ptr_after = {
            let stripe = engine.stripe_lock(bucket).read();
            let live = stripe
                .live
                .iter()
                .find(|l| record_key(&l.record) == kb.as_ref())
                .expect("entry is present");
            live.record.as_ref().as_ptr()
        };

        assert_eq!(
            engine.resident_bytes(),
            Some(engine.recount_resident_bytes()),
            "install drops the value's heap bytes from the resident total"
        );
        assert_ne!(
            record_ptr_before, record_ptr_after,
            "install builds a fresh key-only allocation rather than slicing the old \
             resident record: a slice would share its refcount and keep the value bytes \
             physically resident in RAM"
        );
    }

    #[test]
    fn install_of_an_inline_record_reuses_the_slot_with_no_heap_allocation() {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 1u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let ver = hlc(1, 1);
        let _ = put(&engine, key, kb.clone(), "value".to_string(), ver, None, 0);
        simulate_pending_handoff(&engine, bucket, hash, &kb, 1);

        let installed = SpillSink::install(&engine, bucket, &kb, hash, ver, loc(0, 0, 4, 0), 1);
        assert!(installed);

        let stripe = engine.stripe_lock(bucket).read();
        let live = stripe
            .live
            .iter()
            .find(|l| record_key(&l.record) == kb.as_ref())
            .expect("entry is present");
        assert!(
            matches!(live.record, Record::Inline { .. }),
            "a key-only record for a small u32 key stays inline: no heap allocation"
        );
        assert_eq!(record_key(&live.record), kb.as_ref());
        assert!(
            record_value(&live.record).is_empty(),
            "no old value bytes are retained anywhere"
        );
    }

    #[test]
    fn install_new_never_resurrects_a_tombstoned_key() {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 1u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        tombstone(&engine, key, kb.clone(), hlc(1, 1), 0);

        let inserted = SpillSink::install_new(
            &engine,
            bucket,
            &kb,
            hash,
            hlc(0, 0),
            None,
            loc(0, 0, 4, 0),
            1,
        );

        assert!(
            !inserted,
            "install_new never resurrects a key a tombstone already covers"
        );
        assert_eq!(engine.get(&key, 0), None);
        let (live_count, _) = engine.debug_totals();
        assert_eq!(live_count, 0);
    }

    #[test]
    fn install_new_never_overwrites_an_already_present_live_entry() {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 1u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let ver = hlc(2, 1);
        let _ = put(
            &engine,
            key,
            kb.clone(),
            "resident".to_string(),
            ver,
            None,
            0,
        );
        let digest_before = engine.digests();

        let inserted = SpillSink::install_new(
            &engine,
            bucket,
            &kb,
            hash,
            hlc(1, 1),
            None,
            loc(0, 0, 4, 0),
            1,
        );

        assert!(
            !inserted,
            "install_new never overwrites a key that is already present, even at an \
             older version"
        );
        assert_eq!(engine.get(&key, 0), Some("resident".to_string()));
        assert_eq!(
            engine.digests(),
            digest_before,
            "a refused install_new never touches the digest"
        );
    }

    #[test]
    fn install_releases_pending_weight_even_when_a_newer_write_displaced_the_key() {
        let weigher: Weigher<u32, String> =
            Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
        let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
        let key = 1u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let old_ver = hlc(1, 1);
        let _ = put(&engine, key, kb.clone(), "x".repeat(9), old_ver, None, 0);
        simulate_pending_handoff(&engine, bucket, hash, &kb, 9);
        let _ = put(
            &engine,
            key,
            kb.clone(),
            "fresh".to_string(),
            hlc(2, 1),
            None,
            0,
        );

        let installed = SpillSink::install(&engine, bucket, &kb, hash, old_ver, loc(0, 0, 4, 0), 9);

        assert!(!installed, "the newer write wins; nothing flips to Spilled");
        assert_eq!(
            engine.debug_pending_spill_weight(),
            0,
            "the job's weight leaves pending_spill_weight even though the key moved on"
        );
        let (_, weight_after) = engine.debug_totals();
        assert_eq!(
            weight_after, 5,
            "total_weight holds only the fresh value's weight"
        );
    }

    #[test]
    fn abandon_releases_pending_weight_even_when_a_tombstone_displaced_the_key() {
        let weigher: Weigher<u32, String> =
            Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
        let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
        let key = 1u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let old_ver = hlc(1, 1);
        let _ = put(&engine, key, kb.clone(), "x".repeat(9), old_ver, None, 0);
        simulate_pending_handoff(&engine, bucket, hash, &kb, 9);
        tombstone(&engine, key, kb.clone(), hlc(2, 1), 0);

        SpillSink::abandon(&engine, bucket, &kb, hash, old_ver, 9);

        assert_eq!(
            engine.debug_pending_spill_weight(),
            0,
            "the job's weight leaves pending_spill_weight although the key is tombstoned"
        );
        let (_, weight_after) = engine.debug_totals();
        assert_eq!(weight_after, 0, "nothing is restored onto a tombstoned key");
    }

    #[test]
    fn enforce_capacity_counts_pending_spill_weight_toward_the_cap_and_defers_to_the_flusher() {
        let weigher: Weigher<u32, String> =
            Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
        let engine = Engine::<u32, String>::new(50, None, Some(weigher), None);
        let key = 1u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let _ = put(&engine, key, kb.clone(), "x".repeat(80), hlc(1, 1), None, 0);
        simulate_pending_handoff(&engine, bucket, hash, &kb, 80);
        // total_weight alone (0) already fits the 50-unit cap; only
        // adding pending_spill_weight (80) back in shows this bucket is
        // still, in effect, 30 units over.
        assert_eq!(engine.debug_totals().1, 0);
        assert_eq!(engine.debug_pending_spill_weight(), 80);

        engine.enforce_capacity(bucket, &mut Written::default(), 0);

        assert_eq!(
            engine.debug_eviction_lock_acquisitions(),
            1,
            "one batch pass finds nothing else to evict in this bucket; the stop rule \
             returns immediately rather than paying for a full-stripe scan"
        );
        assert_eq!(
            engine.get(&key, 0),
            Some("x".repeat(80)),
            "the pending entry itself is left alone: still resident, still readable"
        );
        assert_eq!(
            engine.debug_pending_spill_weight(),
            80,
            "still nobody resolved it"
        );
    }

    #[test]
    fn insert_over_a_spilled_key_replaces_it_with_a_fresh_resident_entry() {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 1u32;
        let kb = key_bytes(key);
        engine.debug_insert_spilled(&kb, hlc(1, 1), None, loc(0, 0, 4, 0), 0);
        let (live_count_before, weight_before) = engine.debug_totals();
        assert_eq!(weight_before, 0);
        assert_eq!(
            engine.debug_spill_entries_count(),
            1,
            "debug_insert_spilled counts against sundog_spill_entries like a real install"
        );

        let outcome = put(
            &engine,
            key,
            kb.clone(),
            "fresh".to_string(),
            hlc(2, 1),
            None,
            0,
        );
        assert!(
            matches!(outcome, ApplyOutcome::Put { created: false, .. }),
            "a spilled key is still live: a write over it is an update, not a creation"
        );
        assert_eq!(engine.get(&key, 0), Some("fresh".to_string()));
        let (live_count_after, weight_after) = engine.debug_totals();
        assert_eq!(
            live_count_after, live_count_before,
            "one spilled entry is replaced by one resident one: no net live_count change"
        );
        assert_eq!(weight_after, 1);
        assert_eq!(engine.digests(), engine.recompute_digests_paired());
        assert_eq!(
            engine.debug_spill_entries_count(),
            0,
            "an overwrite of a spilled key must decrement sundog_spill_entries, the same as \
             a promotion would"
        );
    }

    #[test]
    fn tombstone_over_a_spilled_key_removes_it_and_corrects_weight_and_live_count() {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 1u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        engine.debug_insert_spilled(&kb, hlc(1, 1), None, loc(0, 0, 4, 0), 0);
        assert_eq!(engine.debug_spill_entries_count(), 1);

        {
            let mut stripe = engine.stripe_lock(bucket).write();
            let resolver = LwwResolver;
            let apply_ctx = test_ctx(&engine, bucket, hash, &resolver, 60_000, 600_000, 0);
            let (outcome, displaced_spilled) = apply_locked(
                &mut stripe,
                &apply_ctx,
                EntryKey {
                    hash,
                    key,
                    key_bytes: kb.clone(),
                },
                hlc(2, 1),
                Incoming::Tombstone,
            );
            assert!(matches!(outcome, ApplyOutcome::Tombstoned { .. }));
            assert!(
                displaced_spilled,
                "apply_locked reports that the tombstoned entry was spilled"
            );
            engine.note_spill_departure(displaced_spilled);
        }
        assert_eq!(engine.get(&key, 0), None);
        let (live_count, weight) = engine.debug_totals();
        assert_eq!((live_count, weight), (0, 0));
        assert_eq!(engine.digests(), engine.recompute_digests_paired());
        assert_eq!(
            engine.debug_spill_entries_count(),
            0,
            "a tombstone over a spilled key must decrement sundog_spill_entries"
        );
    }

    #[test]
    fn invalidate_removes_a_spilled_key_at_a_newer_version() {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 1u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let ver = hlc(1, 1);
        engine.debug_insert_spilled(&kb, ver, None, loc(0, 0, 4, 0), 0);
        assert_eq!(engine.debug_spill_entries_count(), 1);

        let removed_ver = engine.invalidate(kb.as_ref(), hash, hlc(2, 1));
        assert_eq!(removed_ver, Some(ver));
        assert_eq!(engine.get(&key, 0), None);
        let (live_count, weight) = engine.debug_totals();
        assert_eq!((live_count, weight), (0, 0));
        assert_eq!(
            engine.debug_spill_entries_count(),
            0,
            "invalidate of a spilled key must decrement sundog_spill_entries"
        );
    }

    #[test]
    fn invalidate_local_removes_a_spilled_key_unconditionally() {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 1u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        engine.debug_insert_spilled(&kb, hlc(1, 1), None, loc(0, 0, 4, 0), 0);
        assert_eq!(engine.debug_spill_entries_count(), 1);

        engine.invalidate_local(kb.as_ref(), hash);
        assert_eq!(engine.get(&key, 0), None);
        let (live_count, weight) = engine.debug_totals();
        assert_eq!((live_count, weight), (0, 0));
        assert_eq!(
            engine.debug_spill_entries_count(),
            0,
            "invalidate_local of a spilled key must decrement sundog_spill_entries"
        );
    }

    #[test]
    fn sweep_removes_an_expired_spilled_key_and_corrects_the_digest() {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 1u32;
        let kb = key_bytes(key);
        engine.debug_insert_spilled(&kb, hlc(1, 1), Some(50), loc(0, 0, 4, 0), 0);
        assert_eq!(engine.debug_spill_entries_count(), 1);

        engine.sweep(100);
        assert_eq!(engine.get(&key, 100), None);
        let (live_count, weight) = engine.debug_totals();
        assert_eq!((live_count, weight), (0, 0));
        assert_eq!(engine.digests(), engine.recompute_digests_paired());
        assert_eq!(
            engine.debug_spill_entries_count(),
            0,
            "an expiry sweep of a spilled key must decrement sundog_spill_entries"
        );
    }

    #[test]
    fn complete_fresh_load_over_a_spilled_key_decrements_spill_entries() {
        // A rare race: `get_spilled_by_bytes` already failed to promote
        // this key, since a concurrent tombstone or newer write raced
        // its read, yet the entry, sampled independently right here,
        // is still `Spilled` when the loader's fill lands.
        // `complete_fresh_load` replaces it, being newer, and must
        // still keep `sundog_spill_entries` correct.
        let engine = engine_u32_string(u64::MAX, None);
        let key = 1u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        engine.debug_insert_spilled(&kb, hlc(1, 1), None, loc(0, 0, 4, 0), 0);
        assert_eq!(engine.debug_spill_entries_count(), 1);

        let inflight = Arc::new(Inflight::<String>::new());
        let encoded = Bytes::from(postcard::to_stdvec(&"loaded".to_string()).unwrap());
        let outcome = engine.complete_fresh_load(
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
        assert_eq!(
            outcome,
            FillOutcome::Installed { had_live: true },
            "the spilled entry counted as already-live for complete_fresh_load's purposes"
        );
        assert_eq!(engine.get(&key, 0), Some("loaded".to_string()));
        assert_eq!(
            engine.debug_spill_entries_count(),
            0,
            "complete_fresh_load displacing a spilled key must decrement \
             sundog_spill_entries"
        );
    }

    #[test]
    fn gc_tombstones_never_touches_a_spilled_live_entry() {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 1u32;
        let kb = key_bytes(key);
        engine.debug_insert_spilled(&kb, hlc(1, 1), None, loc(0, 0, 4, 0), 0);
        let digest_before = engine.digests();

        engine.gc_tombstones(true, u64::MAX);

        assert_eq!(
            engine.digests(),
            digest_before,
            "gc_tombstones only ever touches stripe.tombstones"
        );
        let (live_count, _) = engine.debug_totals();
        assert_eq!(live_count, 1, "the spilled live entry is untouched");
    }

    #[test]
    fn is_spill_candidate_excludes_a_weight_zeroed_entry_across_a_longer_pending_window() {
        // A `Reservation` wait suspends longer than a non-blocking pass
        // leaves open.
        let weigher: Weigher<u32, String> =
            Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
        let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
        let key = 1u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let _ = put(&engine, key, kb.clone(), "x".repeat(9), hlc(1, 1), None, 0);
        simulate_pending_handoff(&engine, bucket, hash, &kb, 9);

        for _ in 0..1_000 {
            assert!(
                engine.evict_one_sampled(bucket, 0).made_no_progress(),
                "a weight-zeroed pending entry is never re-selected, however long it \
                 stays pending"
            );
        }
        assert_eq!(
            engine.get(&key, 0),
            Some("x".repeat(9)),
            "still resident and readable throughout"
        );
    }

    // --- Real disk: the flusher's actual eviction+install lifecycle.
    // Never combined with `sim`: its virtual clock gives no determinism
    // over real filesystem I/O or the flusher's OS thread.
    #[cfg(not(feature = "sim"))]
    mod io {
        use std::time::{Duration, Instant};

        use super::*;

        fn temp_dir(label: &str) -> std::path::PathBuf {
            let dir = std::env::temp_dir().join(format!(
                "sundog-engine-spill-test-{label}-{}-{:?}",
                std::process::id(),
                std::thread::current().id(),
            ));
            let _ = std::fs::remove_dir_all(&dir);
            dir
        }

        /// Polls `cond` until it returns `true` or `timeout` elapses,
        /// returning the final result either way. Never a fixed sleep.
        fn poll_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
            let start = Instant::now();
            loop {
                if cond() {
                    return true;
                }
                if start.elapsed() >= timeout {
                    return cond();
                }
                std::thread::sleep(Duration::from_millis(5));
            }
        }

        const POLL_TIMEOUT: Duration = Duration::from_secs(5);

        fn is_spilled<V>(engine: &Engine<u32, V>, kb: &Bytes) -> bool
        where
            V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
        {
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            let stripe = engine.stripe_lock(bucket).read();
            stripe.live.iter().any(|l| {
                record_key(&l.record) == kb.as_ref() && matches!(l.state, EntryState::Spilled(_))
            })
        }

        #[test]
        fn set_spill_makes_it_visible_via_spill_accessor() {
            let dir = temp_dir("set-spill-accessor");
            let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
            let tier = Arc::new(SpillTier::open(&cfg, "accessor").expect("tier opens"));
            let engine = Engine::<u32, String>::new(u64::MAX, None, None, None);
            assert!(engine.spill().is_none());
            engine.set_spill(Arc::clone(&tier));
            assert!(engine.spill().is_some());
            let _ = std::fs::remove_dir_all(&dir);
        }

        #[test]
        fn eviction_hands_off_weight_immediately_and_install_never_double_subtracts_it() {
            let dir = temp_dir("evict");
            let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
            let tier = Arc::new(SpillTier::open(&cfg, "evict").expect("tier opens"));
            let weigher: Weigher<u32, String> =
                Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
            let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
            engine.set_spill(Arc::clone(&tier));
            let engine = Arc::new(engine);
            tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

            let key = 1u32;
            let kb = key_bytes(key);
            let bucket = stripe_index_from_hash(hash_key_bytes(kb.as_ref()));
            let _ = put(&engine, key, kb.clone(), "x".repeat(20), hlc(1, 1), None, 0);

            let digest_before = engine.digests();
            let (live_count_before, weight_before) = engine.debug_totals();

            // `evict_one_sampled` is the real caller: it folds
            // `evict_victim_locked`'s freed weight into `total_weight`
            // right there, synchronously, with no dependency on the
            // flusher thread ever running.
            let pass_outcome = engine.evict_one_sampled(bucket, 0);
            assert_eq!(
                pass_outcome.removed_weight, 20,
                "a spill hand-off's freed weight counts the same as a physical removal's"
            );
            let (_, weight_at_handoff) = engine.debug_totals();
            assert_eq!(
                weight_at_handoff,
                weight_before - 20,
                "hand-off zeroes the victim's weight and frees it from total_weight \
                 immediately, before any disk write or install has happened"
            );
            // The flusher may already have installed the victim by now;
            // either way its weight is 0 from the hand-off on.
            assert_eq!(
                {
                    let stripe = engine.stripe_lock(bucket).read();
                    stripe
                        .live
                        .iter()
                        .find(|l| record_key(&l.record) == kb.as_ref())
                        .map(|l| l.weight)
                },
                Some(0),
                "the victim keeps weight 0 from the hand-off, resident or spilled"
            );

            // A second sampling pass, called before the flusher's
            // install is known to have landed, must not pick the same
            // key again: whether it is still the pending hand-off or
            // has already been flipped to `Spilled` by now, either way
            // `is_spill_candidate` excludes it.
            assert!(
                engine.evict_one_sampled(bucket, 0).made_no_progress(),
                "the only entry in this stripe is either still pending or already \
                 spilled; double-sampling before install must find nothing"
            );

            assert!(
                poll_until(POLL_TIMEOUT, || is_spilled(&engine, &kb)),
                "the flusher installs the spilled entry"
            );

            assert_eq!(
                engine.digests(),
                digest_before,
                "spilling and installing never touch the digest"
            );
            let (live_count_after, weight_after) = engine.debug_totals();
            assert_eq!(
                live_count_after, live_count_before,
                "spilling and installing never touch live_count"
            );
            assert_eq!(
                weight_after, weight_at_handoff,
                "install only flips the payload to Spilled; the weight was already zeroed \
                 and freed at hand-off, so total_weight does not move again here"
            );

            let _ = std::fs::remove_dir_all(&dir);
        }

        #[test]
        fn install_new_creates_a_fresh_spilled_entry_readable_via_read_at_and_folds_the_digest() {
            let dir = temp_dir("install-new");
            let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
            let tier = Arc::new(SpillTier::open(&cfg, "install-new").expect("tier opens"));

            // A throwaway producer spills one record to disk to get a
            // genuine SpillLoc.
            let producer: Arc<Engine<u32, String>> =
                Arc::new(Engine::new(u64::MAX, None, None, None));
            producer.set_spill(Arc::clone(&tier));
            tier.attach(Arc::downgrade(
                &(Arc::clone(&producer) as Arc<dyn SpillSink>),
            ));
            let key = 7u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            let ver = hlc(1, 1);
            let _ = put(
                &producer,
                key,
                kb.clone(),
                "spilled-value".to_string(),
                ver,
                Some(999),
                0,
            );
            let _ = producer.evict_one_sampled(bucket, 0);
            assert!(poll_until(POLL_TIMEOUT, || is_spilled(&producer, &kb)));
            let loc = {
                let stripe = producer.stripe_lock(bucket).read();
                let live = stripe
                    .live
                    .iter()
                    .find(|l| record_key(&l.record) == kb.as_ref())
                    .expect("entry is present");
                match live.state {
                    EntryState::Spilled(loc) => loc,
                    EntryState::Resident => {
                        panic!("poll_until above confirms the producer's entry is spilled")
                    }
                }
            };

            let target = Engine::<u32, String>::new(u64::MAX, None, None, None);
            let digest_before = target.digests();

            let inserted =
                SpillSink::install_new(&target, bucket, &kb, hash, ver, Some(999), loc, 1);
            assert!(inserted);

            // Resident-free: `get` reads nothing, since no value ever
            // landed in RAM on the target engine.
            assert_eq!(target.get(&key, 0), None);
            {
                let stripe = target.stripe_lock(bucket).read();
                let live = stripe
                    .live
                    .iter()
                    .find(|l| record_key(&l.record) == kb.as_ref())
                    .expect("entry is present");
                assert!(
                    matches!(live.state, EntryState::Spilled(l) if l == loc),
                    "Spilled(loc)-only, pointing at the exact same on-disk location"
                );
                assert_eq!(live.weight, 0, "a Spilled entry always carries weight 0");
            }

            // Readable via read_at, since install_new never re-writes
            // or copies the value bytes itself.
            let read_back = tier
                .read_at(loc)
                .expect("read_at succeeds")
                .expect("the record is still there");
            assert_eq!(read_back.ver, ver);
            assert_eq!(read_back.expires_at_ms, Some(999));
            assert_eq!(decode_value::<String>(&read_back.encoded), "spilled-value");

            let (live_count, weight) = target.debug_totals();
            assert_eq!(live_count, 1);
            assert_eq!(
                weight, 0,
                "a spilled entry never counts toward total_weight"
            );

            let mut expected_digest = digest_before;
            expected_digest[bucket].1 ^= entry_fingerprint(kb.as_ref(), ver);
            assert_eq!(
                target.digests(),
                expected_digest,
                "install_new folds the fresh entry's fingerprint into the bucket digest"
            );
            assert_eq!(
                target.digests(),
                target.recompute_digests_paired(),
                "the incrementally folded digest matches a full recompute from scratch"
            );

            tier.close();
            let _ = std::fs::remove_dir_all(&dir);
        }

        #[test]
        fn evict_one_sampled_abandons_the_victim_when_the_flush_queue_channel_is_full() {
            let dir = temp_dir("enqueue-err-abandon");
            // A generous flush_queue_bytes exercises the channel's
            // slot-count limit, not the byte bound.
            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .flush_queue_bytes(1 << 20);
            let slots = crate::store::spill::flush_queue_slots(cfg.flush_queue_bytes_value());
            let tier = Arc::new(SpillTier::open(&cfg, "channel-full").expect("tier opens"));
            // Paused before the flusher thread is even spawned, so it
            // never gets a chance to drain any of the filler jobs
            // below before the real eviction's own `enqueue` needs the
            // channel to still be completely full.
            tier.pause_flusher();
            let weigher: Weigher<u32, String> =
                Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
            let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
            engine.set_spill(Arc::clone(&tier));
            let engine = Arc::new(engine);
            tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

            // Fill the flusher's channel to its exact slot capacity
            // with throwaway jobs, so the real eviction below finds no
            // room: the one way `finish_spill_handoff` reaches
            // `abandon` rather than `install`.
            for i in 0..slots {
                let filler = SpillJob {
                    stripe_idx: 0,
                    hash: 0,
                    key_bytes: Bytes::from(format!("filler-{i}")),
                    ver: hlc(0, 0),
                    expires_at_ms: None,
                    encoded: Bytes::from_static(b"f"),
                    weight: 1,
                    // These fillers bypass would_accept, sent straight
                    // to enqueue to exercise the channel's slot limit.
                    admitted_bytes: 0,
                };
                tier.enqueue(OnRefusal::TierPolicy, filler)
                    .unwrap_or_else(|_| panic!("channel has room for filler {i}"));
            }

            let key = 1u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            let _ = put(&engine, key, kb.clone(), "x".repeat(30), hlc(1, 1), None, 0);

            let outcome = engine.evict_one_sampled(bucket, 0);
            assert_eq!(
                outcome.removed_weight, 30,
                "the hand-off still commits and frees the weight from total_weight \
                 right away, exactly as a physical removal would; abandon only puts it \
                 back once the enqueue that follows is found to have failed"
            );

            assert_eq!(
                engine.get(&key, 0),
                Some("x".repeat(30)),
                "abandon restores full residency: the value is still there and readable"
            );
            let (_, weight_after) = engine.debug_totals();
            assert_eq!(
                weight_after, 30,
                "abandon restores the victim's weight to total_weight"
            );
            assert_eq!(
                engine.debug_pending_spill_weight(),
                0,
                "abandon moves the weight back out of pending_spill_weight too"
            );
            let stripe = engine.stripe_lock(bucket).read();
            let live = stripe
                .live
                .iter()
                .find(|l| record_key(&l.record) == kb.as_ref())
                .expect("entry is present");
            assert_eq!(
                live.weight, 30,
                "the entry's own weight field is restored, not just the total"
            );
            drop(stripe);

            let _ = std::fs::remove_dir_all(&dir);
        }

        // The mirror of `enforce_capacity_leaves_a_refused_victim_resident_
        // and_retries_once_the_queue_drains` below: `keep_resident_when_
        // refused` is left at its default of `false` (never set), so a
        // refused hand-off falls back to the ordinary delete exactly as
        // every mode did before that policy existed.
        #[test]
        fn enforce_capacity_via_real_hand_off_bounds_ram_and_falls_back_to_queue_full() {
            let dir = temp_dir("real-backlog");
            // A flush queue that holds one small record but not two, so
            // `would_accept` refuses a second hand-off while the first
            // is still queued: the flusher is paused, so nothing ever
            // drains it.
            let record_len_upper_bound = 300u64;
            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .flush_queue_bytes(record_len_upper_bound);
            let tier = Arc::new(SpillTier::open(&cfg, "backlog").expect("tier opens"));
            tier.pause_flusher();
            let weigher: Weigher<u32, String> =
                Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
            let engine = Engine::<u32, String>::new(250, None, Some(weigher), None);
            engine.set_spill(Arc::clone(&tier));
            let engine = Arc::new(engine);
            tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

            // Two keys landing in the same stripe, so a single bucket's
            // sampling sees both, colder one first.
            let mut same_bucket: HashMap<usize, Vec<u32>> = HashMap::new();
            let mut keys = Vec::new();
            for k in 1..100_000u32 {
                let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(k).as_ref()));
                let group = same_bucket.entry(bucket).or_default();
                group.push(k);
                if group.len() == 2 {
                    keys = group.clone();
                    break;
                }
            }
            assert_eq!(
                keys.len(),
                2,
                "1024 stripes; two collisions are found quickly"
            );
            let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(keys[0]).as_ref()));

            let _ = put(
                &engine,
                keys[0],
                key_bytes(keys[0]),
                "x".repeat(200),
                hlc(1, 1),
                None,
                0,
            );
            let _ = put(
                &engine,
                keys[1],
                key_bytes(keys[1]),
                "y".repeat(200),
                hlc(2, 1),
                None,
                1,
            );

            // First eviction: the colder key (`now_ms: 0`) hands off
            // cleanly, with the queue empty.
            engine.evict_one_sampled(bucket, 2);
            assert_eq!(
                engine.get(&keys[0], 0),
                Some("x".repeat(200)),
                "still fully resident: a hand-off, not a removal"
            );
            assert_eq!(engine.debug_pending_spill_weight(), 200);
            let combined_after_first =
                engine.debug_totals().1 + engine.debug_pending_spill_weight();
            assert!(
                combined_after_first <= 250 + 200,
                "combined weight {combined_after_first} exceeds the cap by more than one \
                 victim's worth"
            );

            // Second eviction: the queue already holds one record's
            // worth and the flusher is paused, so `would_accept` now
            // refuses; the victim falls back to an ordinary delete
            // instead of piling up a second fully-resident value.
            engine.evict_one_sampled(bucket, 2);
            assert_eq!(
                engine.get(&keys[1], 0),
                None,
                "queue_full falls back to the ordinary delete-and-XOR path"
            );
            let (total_after, pending_after) =
                (engine.debug_totals().1, engine.debug_pending_spill_weight());
            assert_eq!(total_after, 0);
            assert_eq!(pending_after, 200);
            assert!(
                total_after + pending_after <= 250 + 200,
                "combined weight never exceeds the cap by more than one victim's worth"
            );

            // The public entry point sees the same combined accounting
            // and returns immediately: nothing left to evict in this
            // bucket, and total_weight alone is within the cap, so the
            // pending hand-off's install is all that is left.
            engine.enforce_capacity(bucket, &mut Written::default(), 2);

            // Once the flusher is allowed to run, the one queued job
            // installs and pending_spill_weight drains back to zero.
            tier.resume_flusher();
            assert!(poll_until(POLL_TIMEOUT, || engine
                .debug_pending_spill_weight()
                == 0));
            assert!(poll_until(POLL_TIMEOUT, || is_spilled(
                &engine,
                &key_bytes(keys[0])
            )));

            let _ = std::fs::remove_dir_all(&dir);
        }

        /// The `Mode::Replicated` mirror of
        /// `enforce_capacity_via_real_hand_off_bounds_ram_and_falls_back_to_queue_full`:
        /// identical setup, `tier.set_keep_resident_when_refused(true)`
        /// the only difference, so the second victim's refused hand-off
        /// leaves it fully resident instead of falling back to a
        /// delete, and a later `enforce_capacity` pass, once the queue
        /// has room again, retries and spills it.
        #[test]
        fn enforce_capacity_leaves_a_refused_victim_resident_and_retries_once_the_queue_drains() {
            let dir = temp_dir("real-backlog-keep-resident");
            let record_len_upper_bound = 300u64;
            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .flush_queue_bytes(record_len_upper_bound);
            let tier =
                Arc::new(SpillTier::open(&cfg, "backlog-keep-resident").expect("tier opens"));
            tier.set_keep_resident_when_refused(true);
            tier.pause_flusher();
            let weigher: Weigher<u32, String> =
                Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
            // A cap well under either single entry's own weight (200),
            // unlike the flag-off mirror's 250: that test only needs
            // one hand-off to land to fall back under its cap, but this
            // one means to keep the deferred victim's weight alone
            // over the cap even after the first hand-off fully
            // resolves, so the later retry has something left to do.
            let engine = Engine::<u32, String>::new(50, None, Some(weigher), None);
            engine.set_spill(Arc::clone(&tier));
            let engine = Arc::new(engine);
            tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

            let mut same_bucket: HashMap<usize, Vec<u32>> = HashMap::new();
            let mut keys = Vec::new();
            for k in 1..100_000u32 {
                let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(k).as_ref()));
                let group = same_bucket.entry(bucket).or_default();
                group.push(k);
                if group.len() == 2 {
                    keys = group.clone();
                    break;
                }
            }
            assert_eq!(
                keys.len(),
                2,
                "1024 stripes; two collisions are found quickly"
            );
            let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(keys[0]).as_ref()));

            let _ = put(
                &engine,
                keys[0],
                key_bytes(keys[0]),
                "x".repeat(200),
                hlc(1, 1),
                None,
                0,
            );
            let _ = put(
                &engine,
                keys[1],
                key_bytes(keys[1]),
                "y".repeat(200),
                hlc(2, 1),
                None,
                1,
            );

            // First eviction: the colder key hands off cleanly, with
            // the queue empty.
            engine.evict_one_sampled(bucket, 2);
            assert_eq!(engine.debug_pending_spill_weight(), 200);

            // Second eviction: the queue already holds one record's
            // worth and the flusher is paused, so `would_accept`
            // refuses again, but this tier's `keep_resident_when_
            // refused` is set, so the victim is left fully resident
            // instead of deleted.
            engine.evict_one_sampled(bucket, 2);
            assert_eq!(
                engine.get(&keys[1], 0),
                Some("y".repeat(200)),
                "a Mode::Replicated victim refused by the tier stays fully resident, never \
                 deleted"
            );
            let (total_after, pending_after) =
                (engine.debug_totals().1, engine.debug_pending_spill_weight());
            assert_eq!(
                total_after, 200,
                "the deferred victim's weight is untouched, still counted in total_weight"
            );
            assert_eq!(pending_after, 200, "unchanged from the first hand-off");
            assert!(
                total_after + pending_after > 50,
                "combined weight is still over the cap: nothing was freed by deferring"
            );
            {
                let stripe = engine.stripe_lock(bucket).read();
                let live = stripe
                    .live
                    .iter()
                    .find(|l| record_key(&l.record) == key_bytes(keys[1]).as_ref())
                    .expect("the deferred entry is still present");
                assert_eq!(live.weight, 200, "its own weight field is untouched too");
            }

            // The public entry point sees the same combined accounting,
            // finds its victim refused by the full queue while a
            // hand-off is pending, and returns after that one stripe
            // rather than scan every other stripe for a victim the
            // queue would refuse too.
            let locks_before = engine.debug_eviction_lock_acquisitions();
            engine.enforce_capacity(bucket, &mut Written::default(), 2);
            assert!(
                engine.debug_eviction_lock_acquisitions() - locks_before <= 2,
                "a full queue ends the pass at once, took {} stripe locks",
                engine.debug_eviction_lock_acquisitions() - locks_before
            );
            assert_eq!(
                engine.debug_totals().1,
                200,
                "enforce_capacity made no further change: still over the cap"
            );

            // Once the flusher drains the first job, `would_accept` has
            // room again for the deferred victim, and the next
            // `enforce_capacity` pass spills it.
            tier.resume_flusher();
            assert!(poll_until(POLL_TIMEOUT, || tier.queued_bytes() == 0));
            engine.enforce_capacity(bucket, &mut Written::default(), 2);
            assert!(
                poll_until(POLL_TIMEOUT, || is_spilled(&engine, &key_bytes(keys[1]))),
                "the retried victim is spilled once the tier has room again"
            );

            let _ = std::fs::remove_dir_all(&dir);
        }

        /// An engine capped at `cap` under a length weigher, over a
        /// keep-resident tier whose one-record flush queue holds a
        /// hand-off the paused flusher never writes, and one resident
        /// 200-byte entry that no eviction can move while the queue
        /// stays full. Returns the engine, its tier, the scratch
        /// directory and the stuck entry's key.
        fn engine_with_a_full_queue(
            label: &str,
            cap: u64,
        ) -> (
            Arc<Engine<u32, String>>,
            Arc<SpillTier>,
            std::path::PathBuf,
            u32,
        ) {
            engine_with_a_full_queue_on(label, cap, true)
        }

        /// [`engine_with_a_full_queue`] over a tier whose own policy is
        /// `keep_resident`: `false` is a `Mode::Local` or
        /// `Mode::Invalidation` tier, which deletes the victim a write's
        /// eviction finds refused.
        fn engine_with_a_full_queue_on(
            label: &str,
            cap: u64,
            keep_resident: bool,
        ) -> (
            Arc<Engine<u32, String>>,
            Arc<SpillTier>,
            std::path::PathBuf,
            u32,
        ) {
            let dir = temp_dir(label);
            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .flush_queue_bytes(300);
            let tier = Arc::new(SpillTier::open(&cfg, label).expect("tier opens"));
            tier.set_keep_resident_when_refused(keep_resident);
            tier.pause_flusher();
            let weigher: Weigher<u32, String> =
                Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
            let engine = Engine::<u32, String>::new(cap, None, Some(weigher), None);
            engine.set_spill(Arc::clone(&tier));
            let engine = Arc::new(engine);
            tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

            let _ = put(
                &engine,
                1,
                key_bytes(1),
                "x".repeat(200),
                hlc(1, 1),
                None,
                0,
            );
            let first = stripe_index_from_hash(hash_key_bytes(key_bytes(1).as_ref()));
            engine.evict_one_sampled(first, 1);
            assert_eq!(
                engine.debug_pending_spill_weight(),
                200,
                "the first entry's hand-off fills the queue"
            );
            let _ = put(
                &engine,
                2,
                key_bytes(2),
                "y".repeat(200),
                hlc(2, 1),
                None,
                1,
            );
            assert_eq!(engine.debug_totals().1, 200);
            (engine, tier, dir, 2)
        }

        /// A spilled entry `key` with nothing resident for it, as a
        /// flusher install leaves one, and its encoded `value`.
        fn spilled_entry(engine: &Engine<u32, String>, key: u32, value: &str) -> Bytes {
            engine.debug_insert_spilled(
                &key_bytes(key),
                hlc(10, 1),
                None,
                super::loc(0, 0, 10, 0),
                2,
            );
            Bytes::from(postcard::to_stdvec(&value.to_string()).expect("string encodes"))
        }

        /// With no room to make, a full flush queue holding the only
        /// resident entry in place, a promotion leaves the read's entry
        /// on disk rather than take RAM past the cap.
        #[test]
        fn a_promotion_with_no_room_to_make_leaves_the_entry_on_disk() {
            let (engine, tier, dir, stuck) = engine_with_a_full_queue("promote-no-room", 50);
            let value = "z".repeat(10);
            let encoded = spilled_entry(&engine, 7, &value);
            let kb = key_bytes(7);
            let hash = hash_key_bytes(kb.as_ref());

            assert!(!engine.promote_locked(kb.as_ref(), hash, hlc(10, 1), &value, &encoded, 3));
            assert!(is_spilled(&engine, &kb), "the read's entry stays on disk");
            assert_eq!(engine.debug_totals().1, 200, "total_weight is unchanged");
            assert_eq!(
                engine.get(&stuck, 3),
                Some("y".repeat(200)),
                "the stuck entry stays resident"
            );

            tier.resume_flusher();
            drop(engine);
            let _ = std::fs::remove_dir_all(&dir);
        }

        /// A promotion's eviction never deletes the victim its tier
        /// refuses, whatever the tier's own policy: behind a full flush
        /// queue on a tier that deletes (`Mode::Local`,
        /// `Mode::Invalidation`), every resident entry stays resident,
        /// the promotion finds no room, and the refusal counts as
        /// `deferred`, never `queue_full`.
        #[test]
        fn a_promotion_behind_a_full_queue_keeps_every_refused_victim_resident_on_a_tier_that_deletes()
         {
            let recorder = metrics_util::debugging::DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();
            let _guard = metrics::set_default_local_recorder(&recorder);
            let label = "promote-keeps-victims";
            let (engine, tier, dir, stuck) = engine_with_a_full_queue_on(label, 50, false);
            assert!(
                !tier.keep_resident_when_refused(),
                "the tier deletes on a write"
            );
            for key in [3u32, 4] {
                let _ = put(
                    &engine,
                    key,
                    key_bytes(key),
                    "w".repeat(200),
                    hlc(u64::from(key), 1),
                    None,
                    1,
                );
            }
            let value = "z".repeat(10);
            let encoded = spilled_entry(&engine, 7, &value);
            let kb = key_bytes(7);
            let hash = hash_key_bytes(kb.as_ref());
            let totals_before = engine.debug_totals();
            assert_eq!(totals_before.1, 600, "three resident 200-byte entries");

            assert!(!engine.promote_locked(kb.as_ref(), hash, hlc(10, 1), &value, &encoded, 3));

            assert_eq!(
                engine.debug_totals(),
                totals_before,
                "no entry was deleted and none was added"
            );
            assert!(is_spilled(&engine, &kb), "the read's entry stays on disk");
            for (key, ch) in [(stuck, 'y'), (3, 'w'), (4, 'w')] {
                assert_eq!(
                    engine.get(&key, 3),
                    Some(ch.to_string().repeat(200)),
                    "key {key} stays resident"
                );
            }
            assert_eq!(
                crate::store::spill::dropped_since_last_snapshot(&snapshotter, label),
                std::collections::BTreeMap::from([("deferred".to_string(), 1)]),
                "the full queue refuses the one victim of the pass and ends it; \
                 nothing counts as dropped under a delete reason"
            );

            tier.resume_flusher();
            drop(engine);
            let _ = std::fs::remove_dir_all(&dir);
        }

        /// The write-side guard for
        /// `a_promotion_behind_a_full_queue_keeps_every_refused_victim_resident_on_a_tier_that_deletes`:
        /// on the same full queue and the same tier, a write's eviction
        /// keeps the tier's policy, deleting the victim it finds
        /// refused and counting it `queue_full`, never `deferred`.
        #[test]
        fn a_write_behind_a_full_queue_still_drops_its_refused_victim_as_queue_full_on_a_tier_that_deletes()
         {
            let recorder = metrics_util::debugging::DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();
            let _guard = metrics::set_default_local_recorder(&recorder);
            let label = "write-drops-victim";
            let (engine, tier, dir, stuck) = engine_with_a_full_queue_on(label, 50, false);
            let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(stuck).as_ref()));

            engine.enforce_capacity(bucket, &mut Written::default(), 2);

            assert_eq!(
                engine.get(&stuck, 2),
                None,
                "the tier's policy deletes the refused victim"
            );
            assert_eq!(engine.debug_totals().1, 0);
            assert_eq!(
                crate::store::spill::dropped_since_last_snapshot(&snapshotter, label),
                std::collections::BTreeMap::from([("queue_full".to_string(), 1)]),
            );

            tier.resume_flusher();
            drop(engine);
            let _ = std::fs::remove_dir_all(&dir);
        }

        /// The batch pass is the first and main path of a promotion whose
        /// stripe holds other residents: two residents sharing the read
        /// key's stripe make the pass sample the colder one, the full
        /// queue refuses it, and a tier that deletes on a write keeps it
        /// resident, counted `deferred`, with the pending hand-off
        /// ending the pass before any scan.
        #[test]
        fn a_promotion_keeps_the_refused_victim_its_own_stripe_batch_samples() {
            let recorder = metrics_util::debugging::DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();
            let _guard = metrics::set_default_local_recorder(&recorder);
            let label = "promote-batch-keeps";
            let (engine, tier, dir, stuck) = engine_with_a_full_queue_on(label, 50, false);
            let value = "z".repeat(10);
            let encoded = spilled_entry(&engine, 7, &value);
            let kb = key_bytes(7);
            let hash = hash_key_bytes(kb.as_ref());
            let read_stripe = stripe_index_from_hash(hash);
            let sharing: Vec<u32> = (100u32..)
                .filter(|&key| {
                    stripe_index_from_hash(hash_key_bytes(key_bytes(key).as_ref())) == read_stripe
                })
                .take(2)
                .collect();
            for &key in &sharing {
                let _ = put(
                    &engine,
                    key,
                    key_bytes(key),
                    "w".repeat(200),
                    hlc(u64::from(key), 1),
                    None,
                    1,
                );
            }
            let totals_before = engine.debug_totals();
            assert_eq!(totals_before.1, 600);

            assert!(!engine.promote_locked(kb.as_ref(), hash, hlc(10, 1), &value, &encoded, 3));

            assert_eq!(engine.debug_totals(), totals_before);
            assert!(is_spilled(&engine, &kb), "the read's entry stays on disk");
            for &key in &sharing {
                assert_eq!(engine.get(&key, 3), Some("w".repeat(200)));
            }
            assert_eq!(engine.get(&stuck, 3), Some("y".repeat(200)));
            assert_eq!(
                crate::store::spill::dropped_since_last_snapshot(&snapshotter, label),
                std::collections::BTreeMap::from([("deferred".to_string(), 1)]),
                "the batch evicts the colder half of two candidates, one victim, and the \
                 refusal under a pending hand-off ends the pass"
            );

            tier.resume_flusher();
            drop(engine);
            let _ = std::fs::remove_dir_all(&dir);
        }

        /// [`Engine::evict_batch_sampled_with_reservation`] and
        /// [`Engine::evict_one_sampled_with_reservation`] apply the
        /// pass's [`OnRefusal`] to the victim a full queue refuses, on a
        /// tier that deletes on a write: `KeepResident` leaves it
        /// resident, reports the pass as refused and counts `deferred`;
        /// `TierPolicy` removes it and counts `queue_full`.
        #[test]
        fn the_batch_and_single_passes_apply_the_on_refusal_policy_to_a_refused_victim() {
            let recorder = metrics_util::debugging::DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();
            let _guard = metrics::set_default_local_recorder(&recorder);
            for (pass, batch) in [("batch", true), ("one", false)] {
                for (on_refusal, kept, reason) in [
                    (OnRefusal::KeepResident, true, "deferred"),
                    (OnRefusal::TierPolicy, false, "queue_full"),
                ] {
                    let label = format!("on-refusal-{pass}-{reason}");
                    let (engine, tier, dir, stuck) = engine_with_a_full_queue_on(&label, 50, false);
                    let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(stuck).as_ref()));
                    let written = Written::default();
                    let outcome = if batch {
                        engine.evict_batch_sampled_with_reservation(
                            bucket, 150, None, &written, false, on_refusal, 3,
                        )
                    } else {
                        engine.evict_one_sampled_with_reservation(
                            bucket, None, &written, on_refusal, 3,
                        )
                    };
                    let what = format!("{pass} pass, {on_refusal:?}");
                    assert_eq!(outcome.refused, kept, "{what}");
                    assert_eq!(outcome.removed_weight, if kept { 0 } else { 200 }, "{what}");
                    assert_eq!(
                        engine.get(&stuck, 3),
                        kept.then(|| "y".repeat(200)),
                        "{what}"
                    );
                    assert_eq!(
                        crate::store::spill::dropped_since_last_snapshot(&snapshotter, &label),
                        std::collections::BTreeMap::from([(reason.to_string(), 1)]),
                        "{what}"
                    );
                    tier.resume_flusher();
                    drop(engine);
                    let _ = std::fs::remove_dir_all(&dir);
                }
            }
        }

        /// A hand-off a promotion's eviction is admitted for can still
        /// fail at the channel, once its slots run out before the byte
        /// budget does. Nothing is deleted, the abandoned victim stays
        /// resident, and the failure counts `deferred` for a keep-resident
        /// pass on a tier that deletes, `queue_full` for the tier's own
        /// policy.
        #[test]
        fn a_hand_off_failing_at_the_channel_records_the_reason_of_the_pass() {
            let recorder = metrics_util::debugging::DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();
            let _guard = metrics::set_default_local_recorder(&recorder);
            for (on_refusal, reason) in [
                (OnRefusal::KeepResident, "deferred"),
                (OnRefusal::TierPolicy, "queue_full"),
            ] {
                let label = format!("channel-full-{reason}");
                let dir = temp_dir(&label);
                let cfg = SpillConfig::new(&dir, 1 << 20)
                    .region_bytes(4096)
                    .flush_queue_bytes(1 << 20);
                let slots = crate::store::spill::flush_queue_slots(cfg.flush_queue_bytes_value());
                let tier = Arc::new(SpillTier::open(&cfg, &label).expect("tier opens"));
                assert!(!tier.keep_resident_when_refused());
                tier.pause_flusher();
                let weigher: Weigher<u32, String> =
                    Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
                let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
                engine.set_spill(Arc::clone(&tier));
                let engine = Arc::new(engine);
                tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));
                for i in 0..slots {
                    let filler = SpillJob {
                        stripe_idx: 0,
                        hash: 0,
                        key_bytes: Bytes::from(format!("filler-{i}")),
                        ver: hlc(0, 0),
                        expires_at_ms: None,
                        encoded: Bytes::from_static(b"f"),
                        weight: 1,
                        admitted_bytes: 0,
                    };
                    tier.enqueue(OnRefusal::TierPolicy, filler)
                        .unwrap_or_else(|_| panic!("channel has room for filler {i}"));
                }
                assert_eq!(
                    crate::store::spill::dropped_since_last_snapshot(&snapshotter, &label),
                    std::collections::BTreeMap::new(),
                    "the fillers fit"
                );

                let key = 1u32;
                let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(key).as_ref()));
                let _ = put(
                    &engine,
                    key,
                    key_bytes(key),
                    "x".repeat(30),
                    hlc(1, 1),
                    None,
                    0,
                );
                let _ = engine.evict_one_sampled_with_reservation(
                    bucket,
                    None,
                    &Written::default(),
                    on_refusal,
                    0,
                );

                assert_eq!(engine.get(&key, 0), Some("x".repeat(30)), "{on_refusal:?}");
                assert_eq!(engine.debug_totals().1, 30, "{on_refusal:?}");
                assert_eq!(
                    crate::store::spill::dropped_since_last_snapshot(&snapshotter, &label),
                    std::collections::BTreeMap::from([(reason.to_string(), 1)]),
                    "{on_refusal:?}"
                );
                drop(engine);
                let _ = std::fs::remove_dir_all(&dir);
            }
        }

        /// A refusal no flusher install can resolve, a record larger
        /// than a region with no hand-off in flight, leaves a
        /// promotion's eviction nothing to wait for: its scan visits
        /// every stripe once, defers each victim, and returns with
        /// every entry resident and the read's entry still on disk,
        /// on a tier that deletes as on one that keeps.
        #[test]
        fn a_promotion_whose_victims_are_too_large_for_a_region_ends_with_every_entry_resident() {
            let recorder = Arc::new(metrics_util::debugging::DebuggingRecorder::new());
            let snapshotter = recorder.snapshotter();
            let label = "promote-too-large";
            let dir = temp_dir(label);
            let cfg = SpillConfig::new(&dir, 128).region_bytes(64);
            let tier = Arc::new(SpillTier::open(&cfg, label).expect("tier opens"));
            assert!(!tier.keep_resident_when_refused());
            let weigher: Weigher<u32, String> =
                Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
            let engine = Engine::<u32, String>::new(50, None, Some(weigher), None);
            engine.set_spill(Arc::clone(&tier));
            let engine = Arc::new(engine);
            tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

            let value = "z".repeat(10);
            let encoded = spilled_entry(&engine, 7, &value);
            let kb = key_bytes(7);
            let hash = hash_key_bytes(kb.as_ref());
            // Three resident entries, each alone in a stripe other than
            // the read's: the pass's first batch finds an empty stripe,
            // and the scan refuses one victim per resident.
            let mut taken = vec![stripe_index_from_hash(hash)];
            let mut residents = Vec::new();
            for key in 100u32.. {
                let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(key).as_ref()));
                if !taken.contains(&bucket) {
                    taken.push(bucket);
                    residents.push(key);
                }
                if residents.len() == 3 {
                    break;
                }
            }
            for &key in &residents {
                let _ = put(
                    &engine,
                    key,
                    key_bytes(key),
                    "w".repeat(200),
                    hlc(u64::from(key), 1),
                    None,
                    1,
                );
            }
            let totals_before = engine.debug_totals();

            let (done_tx, done_rx) = std::sync::mpsc::channel();
            let promoting = Arc::clone(&engine);
            let (promote_kb, promote_value, promote_encoded) =
                (kb.clone(), value.clone(), encoded.clone());
            let thread_recorder = Arc::clone(&recorder);
            std::thread::spawn(move || {
                // The recorder is per thread.
                let _guard = metrics::set_default_local_recorder(&*thread_recorder);
                let promoted = promoting.promote_locked(
                    promote_kb.as_ref(),
                    hash,
                    hlc(10, 1),
                    &promote_value,
                    &promote_encoded,
                    3,
                );
                let _ = done_tx.send(promoted);
            });
            assert_eq!(
                done_rx.recv_timeout(Duration::from_secs(10)),
                Ok(false),
                "the promotion returns without room rather than loop over the refusals"
            );

            assert_eq!(engine.debug_totals(), totals_before);
            assert!(is_spilled(&engine, &kb));
            for &key in &residents {
                assert_eq!(engine.get(&key, 3), Some("w".repeat(200)));
            }
            assert_eq!(
                crate::store::spill::dropped_since_last_snapshot(&snapshotter, label),
                std::collections::BTreeMap::from([("deferred".to_string(), 3)]),
                "one deferred victim per resident stripe the scan visits"
            );

            drop(engine);
            let _ = std::fs::remove_dir_all(&dir);
        }

        /// A pass that starts on an empty stripe scans on to the stripe
        /// whose victim the full queue refuses, and stops there: the
        /// queue would refuse every other stripe's victim too, so
        /// scanning all 1024 stripes, or sampling more of them, would
        /// buy nothing.
        #[test]
        fn the_scan_stops_at_a_victim_the_full_queue_refuses() {
            let (engine, tier, dir, stuck) = engine_with_a_full_queue("scan-refusal-stop", 50);
            let stuck_bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(stuck).as_ref()));
            let start = (stuck_bucket + BUCKET_COUNT - 1) % BUCKET_COUNT;
            let locks_before = engine.debug_eviction_lock_acquisitions();
            let (done_tx, done_rx) = std::sync::mpsc::channel();
            let enforcing = Arc::clone(&engine);
            std::thread::spawn(move || {
                enforcing.enforce_capacity(start, &mut Written::default(), 2);
                let _ = done_tx.send(());
            });
            assert!(
                done_rx.recv_timeout(Duration::from_secs(10)).is_ok(),
                "enforce_capacity returns rather than loop over the refusal"
            );
            let locks = engine.debug_eviction_lock_acquisitions() - locks_before;
            assert!(
                locks <= 4,
                "the pass and its scan stop at the refused victim, took {locks} stripe locks"
            );
            assert_eq!(
                engine.debug_totals().1,
                200,
                "the refused victim stays resident"
            );

            tier.resume_flusher();
            drop(engine);
            let _ = std::fs::remove_dir_all(&dir);
        }

        /// Two promotions racing for the one free slot under the cap,
        /// with nothing evictable to make more: exactly one claims it,
        /// and `total_weight` lands on the cap, never past it.
        #[test]
        fn concurrent_promotions_never_claim_the_same_room_twice() {
            const ROUNDS: u32 = 50;
            for round in 0..ROUNDS {
                let (engine, tier, dir, _) =
                    engine_with_a_full_queue(&format!("promote-race-{round}"), 210);
                let value = "z".repeat(10);
                let encoded = spilled_entry(&engine, 7, &value);
                let _ = spilled_entry(&engine, 8, &value);
                let barrier = Arc::new(std::sync::Barrier::new(2));
                let promoted: Vec<bool> = [7u32, 8]
                    .into_iter()
                    .map(|key| {
                        let engine = Arc::clone(&engine);
                        let barrier = Arc::clone(&barrier);
                        let (value, encoded) = (value.clone(), encoded.clone());
                        std::thread::spawn(move || {
                            let kb = key_bytes(key);
                            let hash = hash_key_bytes(kb.as_ref());
                            barrier.wait();
                            engine.promote_locked(
                                kb.as_ref(),
                                hash,
                                hlc(10, 1),
                                &value,
                                &encoded,
                                3,
                            )
                        })
                    })
                    .collect::<Vec<_>>()
                    .into_iter()
                    .map(|handle| handle.join().expect("the promoting thread finishes"))
                    .collect();
                assert_eq!(
                    promoted.iter().filter(|&&won| won).count(),
                    1,
                    "round {round}: exactly one promotion claims the slot, got {promoted:?}"
                );
                assert_eq!(engine.debug_totals().1, 210, "round {round}: at the cap");

                tier.resume_flusher();
                drop(engine);
                let _ = std::fs::remove_dir_all(&dir);
            }
        }

        #[test]
        fn finish_spill_handoff_err_releases_the_jobs_admitted_bytes() {
            let dir = temp_dir("release-admitted-bytes-on-err");
            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .flush_queue_bytes(1 << 20);
            let slots = crate::store::spill::flush_queue_slots(cfg.flush_queue_bytes_value());
            let tier = Arc::new(SpillTier::open(&cfg, "release-on-err").expect("tier opens"));
            // Paused before the flusher spawns, so the channel stays
            // completely full when the real eviction's enqueue needs it.
            tier.pause_flusher();
            let weigher: Weigher<u32, String> =
                Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
            let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
            engine.set_spill(Arc::clone(&tier));
            let engine = Arc::new(engine);
            tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

            for i in 0..slots {
                let filler = SpillJob {
                    stripe_idx: 0,
                    hash: 0,
                    key_bytes: Bytes::from(format!("filler-{i}")),
                    ver: hlc(0, 0),
                    expires_at_ms: None,
                    encoded: Bytes::from_static(b"f"),
                    weight: 1,
                    admitted_bytes: 0,
                };
                tier.enqueue(OnRefusal::TierPolicy, filler)
                    .unwrap_or_else(|_| panic!("channel has room for filler {i}"));
            }

            let key = 1u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            let _ = put(&engine, key, kb.clone(), "x".repeat(30), hlc(1, 1), None, 0);

            let queued_before = tier.queued_bytes();
            let outcome = engine.evict_one_sampled(bucket, 0);
            assert_eq!(
                outcome.removed_weight, 30,
                "the hand-off still commits and frees the weight right away"
            );
            let queued_after = tier.queued_bytes();

            assert_eq!(
                queued_after, queued_before,
                "the abandoned job's admitted bytes return to admit: queued_bytes ends \
                 where it started, not permanently inflated by a job that never reached \
                 the flusher's channel"
            );
            assert_eq!(
                engine.get(&key, 0),
                Some("x".repeat(30)),
                "abandon restores full residency"
            );

            let _ = std::fs::remove_dir_all(&dir);
        }

        #[tokio::test]
        async fn a_sufficient_reservation_never_falls_back_to_the_global_admit_check() {
            let dir = temp_dir("reservation-never-falls-back");
            let value = "x".repeat(20);
            // The real postcard-encoded byte length, what
            // spill_record_len/would_accept see, not the char count.
            let encoded_len = postcard::to_stdvec(&value)
                .expect("test value encodes")
                .len();
            let key = 1u32;
            let kb = key_bytes(key);
            let record_len = spill_record_len(kb.len(), encoded_len);
            // A flush queue sized to exactly one record, so the next
            // admission is refused once admit is drained.
            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .flush_queue_bytes(u64::from(record_len));
            let tier = Arc::new(SpillTier::open(&cfg, "reservation-never").expect("tier opens"));
            tier.pause_flusher();
            let weigher: Weigher<u32, String> =
                Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
            let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
            engine.set_spill(Arc::clone(&tier));
            let engine = Arc::new(engine);
            tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            let _ = put(&engine, key, kb.clone(), value.clone(), hlc(1, 1), None, 0);

            // Drains admit entirely into a caller-held Reservation
            // before eviction ever samples the victim.
            let mut reservation = tier
                .reserve(record_len, Duration::from_secs(5))
                .await
                .expect("the whole (single-record) queue is free before this call");
            assert_eq!(
                tier.would_accept(None, kb.len(), encoded_len),
                Admission::RefusedFinal,
                "with no reservation threaded through, admit's own fallback refuses: \
                 fully drained by the reservation above"
            );

            let outcome = engine.evict_one_sampled_with_reservation(
                bucket,
                Some(&mut reservation),
                &Written::default(),
                OnRefusal::TierPolicy,
                0,
            );
            assert_eq!(
                outcome.removed_weight,
                value.len() as u64,
                "the reservation's own pre-paid budget admits the victim; admit itself \
                 never needed to grant anything"
            );
            assert_eq!(
                engine.get(&key, 0),
                Some(value),
                "still fully resident: a hand-off, not a removal"
            );

            drop(reservation);
            let _ = std::fs::remove_dir_all(&dir);
        }

        #[tokio::test]
        async fn committed_bytes_returned_match_the_sum_of_admitted_job_lengths() {
            let dir = temp_dir("admitted-bytes-sum");
            let value_a = "x".repeat(20);
            let value_b = "y".repeat(20);
            // The real postcard-encoded byte lengths, what
            // spill_record_len/would_accept see.
            let encoded_len_a = postcard::to_stdvec(&value_a)
                .expect("test value encodes")
                .len();
            let encoded_len_b = postcard::to_stdvec(&value_b)
                .expect("test value encodes")
                .len();
            let key_a = 1u32;
            let kb_a = key_bytes(key_a);
            let hash_a = hash_key_bytes(kb_a.as_ref());
            let bucket = stripe_index_from_hash(hash_a);
            let record_len_a = spill_record_len(kb_a.len(), encoded_len_a);

            let mut key_b = None;
            for k in 2..100_000u32 {
                let kb = key_bytes(k);
                if stripe_index_from_hash(hash_key_bytes(kb.as_ref())) == bucket {
                    key_b = Some(k);
                    break;
                }
            }
            let key_b = key_b.expect("a second key collides into the same bucket quickly");
            let kb_b = key_bytes(key_b);
            let record_len_b = spill_record_len(kb_b.len(), encoded_len_b);

            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .flush_queue_bytes(u64::from(record_len_a) + u64::from(record_len_b));
            let tier = Arc::new(SpillTier::open(&cfg, "admitted-bytes").expect("tier opens"));
            tier.pause_flusher();
            let weigher: Weigher<u32, String> =
                Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
            let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
            engine.set_spill(Arc::clone(&tier));
            let engine = Arc::new(engine);
            tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

            let _ = put(
                &engine,
                key_a,
                kb_a.clone(),
                value_a.clone(),
                hlc(1, 1),
                None,
                0,
            );
            let _ = put(
                &engine,
                key_b,
                kb_b.clone(),
                value_b.clone(),
                hlc(2, 1),
                None,
                1,
            );

            // A reservation covering exactly one victim's record: the
            // colder key spends it fully; the second falls back to admit.
            let mut reservation = tier
                .reserve(record_len_a, Duration::from_secs(5))
                .await
                .expect("the whole queue is free before this call");

            let first = engine.evict_one_sampled_with_reservation(
                bucket,
                Some(&mut reservation),
                &Written::default(),
                OnRefusal::TierPolicy,
                2,
            );
            assert_eq!(
                first.removed_weight,
                value_a.len() as u64,
                "the colder key hands off from the reservation's own budget"
            );
            let second = engine.evict_one_sampled_with_reservation(
                bucket,
                Some(&mut reservation),
                &Written::default(),
                OnRefusal::TierPolicy,
                2,
            );
            assert_eq!(
                second.removed_weight,
                value_b.len() as u64,
                "the second key hands off from admit's own fallback, the reservation's \
                 budget already spent"
            );

            drop(reservation);

            assert_eq!(
                tier.queued_bytes(),
                u64::from(record_len_a) + u64::from(record_len_b),
                "every byte admit ever handed out across this call, whether through the \
                 caller's own reservation or its own try_acquire_many fallback, is \
                 accounted for by exactly these two committed jobs"
            );

            let _ = std::fs::remove_dir_all(&dir);
        }

        /// Pins that [`Engine::enforce_capacity_with_reservation`]
        /// reports a deficit once both budgets are exhausted mid-batch.
        #[tokio::test]
        async fn enforce_capacity_with_reservation_returns_the_deficit_once_both_budgets_are_exhausted()
         {
            let dir = temp_dir("enforce-capacity-deficit");
            let value = "x".repeat(20);
            let encoded_len = postcard::to_stdvec(&value)
                .expect("test value encodes")
                .len();

            let mut bucket = None;
            let mut keys = Vec::new();
            for k in 1..200_000u32 {
                let kb = key_bytes(k);
                let b = stripe_index_from_hash(hash_key_bytes(kb.as_ref()));
                match bucket {
                    Some(target) if b != target => continue,
                    None => bucket = Some(b),
                    Some(_) => {}
                }
                keys.push(k);
                if keys.len() == 4 {
                    break;
                }
            }
            assert_eq!(
                keys.len(),
                4,
                "1024 stripes; four collisions are found quickly"
            );
            let bucket = bucket.expect("set once the first key fixed the target stripe");

            let record_len_0 = spill_record_len(key_bytes(keys[0]).len(), encoded_len);
            let record_len_1 = spill_record_len(key_bytes(keys[1]).len(), encoded_len);

            // Sized to exactly the coldest victim's record, so reserve
            // drains the tier's whole admit budget into the reservation.
            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .flush_queue_bytes(u64::from(record_len_0));
            let tier =
                Arc::new(SpillTier::open(&cfg, "enforce-capacity-deficit").expect("tier opens"));
            tier.pause_flusher();
            let weigher: Weigher<u32, String> =
                Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
            // Four 20-byte values against a 30-unit cap attempts the
            // second victim within this same lock hold.
            let engine = Engine::<u32, String>::new(30, None, Some(weigher), None);
            engine.set_spill(Arc::clone(&tier));
            let engine = Arc::new(engine);
            tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

            for (i, &k) in keys.iter().enumerate() {
                let now_ms = u64::try_from(i).expect("four keys, fits comfortably");
                let _ = put(
                    &engine,
                    k,
                    key_bytes(k),
                    value.clone(),
                    hlc(now_ms + 1, 1),
                    None,
                    now_ms,
                );
            }

            let mut reservation = tier
                .reserve(record_len_0, Duration::from_secs(5))
                .await
                .expect("the whole (single-record) queue is free before this call");

            let deficit = engine.enforce_capacity_with_reservation(
                bucket,
                Some(&mut reservation),
                &mut Written::default(),
                10,
            );
            assert_eq!(
                deficit,
                u64::from(record_len_1),
                "keys[1]'s own record length: right now, neither the reservation's \
                 remaining budget nor admit's own headroom covers it"
            );

            assert_eq!(
                engine.get(&keys[0], 0),
                Some(value.clone()),
                "the coldest victim hands off cleanly, spent from the reservation"
            );
            assert_eq!(engine.debug_pending_spill_weight(), 20);
            assert_eq!(
                engine.get(&keys[1], 0),
                Some(value.clone()),
                "left fully resident: a pending refusal, not a removal and not a \
                 keep-resident deferral either, no drop of any kind recorded for it"
            );
            assert_eq!(
                engine.get(&keys[2], 0),
                Some(value.clone()),
                "never even sampled: the deficit stopped the pass before a second batch \
                 or scan could reach it"
            );
            assert_eq!(engine.get(&keys[3], 0), Some(value));
            let (_, total_after) = engine.debug_totals();
            assert_eq!(
                total_after, 60,
                "keys[1..4] still fully resident and counted: three victims' worth"
            );

            drop(reservation);
            let _ = std::fs::remove_dir_all(&dir);
        }

        /// Pins that the closing `enforce_capacity` call physically
        /// deletes a deficit-left victim once retries are exhausted.
        #[tokio::test]
        async fn enforce_capacity_deletes_a_deficit_left_victim_once_reservation_retries_give_up() {
            let dir = temp_dir("enforce-capacity-deficit-fallback");
            let value = "x".repeat(20);
            let encoded_len = postcard::to_stdvec(&value)
                .expect("test value encodes")
                .len();

            let mut bucket = None;
            let mut keys = Vec::new();
            for k in 1..200_000u32 {
                let kb = key_bytes(k);
                let b = stripe_index_from_hash(hash_key_bytes(kb.as_ref()));
                match bucket {
                    Some(target) if b != target => continue,
                    None => bucket = Some(b),
                    Some(_) => {}
                }
                keys.push(k);
                if keys.len() == 4 {
                    break;
                }
            }
            assert_eq!(
                keys.len(),
                4,
                "1024 stripes; four collisions are found quickly"
            );
            let bucket = bucket.expect("set once the first key fixed the target stripe");

            let record_len_0 = spill_record_len(key_bytes(keys[0]).len(), encoded_len);
            let record_len_1 = spill_record_len(key_bytes(keys[1]).len(), encoded_len);

            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .flush_queue_bytes(u64::from(record_len_0));
            let tier = Arc::new(
                SpillTier::open(&cfg, "enforce-capacity-deficit-fallback").expect("tier opens"),
            );
            tier.pause_flusher();
            let weigher: Weigher<u32, String> =
                Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
            let engine = Engine::<u32, String>::new(30, None, Some(weigher), None);
            engine.set_spill(Arc::clone(&tier));
            let engine = Arc::new(engine);
            tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

            for (i, &k) in keys.iter().enumerate() {
                let now_ms = u64::try_from(i).expect("four keys, fits comfortably");
                let _ = put(
                    &engine,
                    k,
                    key_bytes(k),
                    value.clone(),
                    hlc(now_ms + 1, 1),
                    None,
                    now_ms,
                );
            }

            let mut reservation = tier
                .reserve(record_len_0, Duration::from_secs(5))
                .await
                .expect("the whole (single-record) queue is free before this call");

            let deficit = engine.enforce_capacity_with_reservation(
                bucket,
                Some(&mut reservation),
                &mut Written::default(),
                10,
            );
            assert_eq!(deficit, u64::from(record_len_1));
            assert_eq!(
                engine.get(&keys[1], 0),
                Some(value.clone()),
                "left fully resident by the first pass, exactly like the sibling test above"
            );

            // The reservation returns its spent remainder on drop, so
            // nothing is left for the closing call below to spend.
            drop(reservation);

            // The closing call falls back to the ordinary
            // `reservation: None` path and keeps evicting.
            engine.enforce_capacity(bucket, &mut Written::default(), 10);

            assert_eq!(
                engine.get(&keys[1], 0),
                None,
                "keep_resident_when_refused defaults to false: the closing fallback must \
                 physically delete the leftover victim, not leave max_capacity exceeded \
                 forever because a reservation retry gave up on it"
            );
            // The fallback stops once keys[0]'s pending hand-off is all
            // that holds the engine over its cap, or nothing is left to
            // evict.
            let (_, total_after_fallback) = engine.debug_totals();
            assert!(
                total_after_fallback < 60,
                "the closing fallback must make real progress against the cap, not just \
                 record a metric: total={total_after_fallback}"
            );

            let _ = std::fs::remove_dir_all(&dir);
        }

        /// `resolve_conflict`'s tombstone/spill guard only ever skips
        /// calling a misbehaving resolver's own `merge`; a well-behaved
        /// value-aware resolver's *own* fallback (`winner` returning
        /// `A`/`B` by `Hlc` order whenever it is handed a value-less
        /// side) must never fire here merely because the stored side
        /// is spilled rather than genuinely value-less. This
        /// pins that `apply_locked` reads a spilled stored record's real
        /// bytes back off disk before consulting the resolver, so a
        /// merge against it folds exactly as it would against a resident
        /// record, rather than discarding whichever side loses the
        /// outright `Hlc` fallback.
        #[test]
        fn merge_against_a_spilled_counter_folds_the_stored_side_instead_of_dropping_it() {
            use crate::store::crdt::{PnCounter, PnCounterResolver};

            let dir = temp_dir("merge-spilled");
            let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
            let tier = Arc::new(SpillTier::open(&cfg, "merge-spilled").expect("tier opens"));
            let engine = Engine::<u32, PnCounter>::new(u64::MAX, None, None, None);
            engine.set_spill(Arc::clone(&tier));
            let engine = Arc::new(engine);
            tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

            let resolver = PnCounterResolver;
            let key = 1u32;
            let kb = key_bytes(key);
            let bucket = stripe_index_from_hash(hash_key_bytes(kb.as_ref()));
            let node_a = crate::store::crdt::WriterId::new(NodeId::from(11), 1);
            let node_b = crate::store::crdt::WriterId::new(NodeId::from(22), 1);

            let _ = put_with_resolver(
                &engine,
                key,
                kb.clone(),
                PnCounter::local_delta(node_a, 3),
                hlc(1, 1),
                None,
                0,
                &resolver,
            );

            let _ = engine.evict_one_sampled(bucket, 0);
            assert!(
                poll_until(POLL_TIMEOUT, || is_spilled(&engine, &kb)),
                "the flusher installs the spilled entry"
            );

            // Colliding write from a second, disjoint writer: before the
            // spilled-value read existed, the stored side's value-less
            // view would send `PnCounterResolver` to its plain-`Hlc`
            // fallback, and this strictly newer incoming version would
            // win outright, discarding node_a's spilled contribution
            // instead of folding it in.
            let outcome = put_with_resolver(
                &engine,
                key,
                kb.clone(),
                PnCounter::local_delta(node_b, 4),
                hlc(5, 2),
                None,
                0,
                &resolver,
            );

            match outcome {
                ApplyOutcome::Put { value, .. } => {
                    assert_eq!(
                        value.value(),
                        7,
                        "merging against a spilled stored record folds its contribution \
                         instead of dropping it"
                    );
                }
                ApplyOutcome::Rejected => {
                    panic!("expected a Put outcome carrying the merged counter, got Rejected")
                }
                ApplyOutcome::Tombstoned { .. } => {
                    panic!("expected a Put outcome carrying the merged counter, got Tombstoned")
                }
            }
            assert_eq!(
                engine.get(&key, 0).map(|c| c.value()),
                Some(7),
                "the merged counter reads back resident, carrying both sides' \
                 contributions"
            );

            let _ = std::fs::remove_dir_all(&dir);
        }

        /// `peek_stored_seed`'s prefold seeding is spill-aware exactly
        /// like `apply_locked`'s own stored-side lookup: a same-key,
        /// multi-entry batch landing on an already-spilled stored record
        /// folds `P` in *first*, ahead of the batch's own entries, the
        /// same left-to-right order sequential per-record application
        /// uses, so the version the batched fold mints matches
        /// sequential application's exactly, not only its content.
        /// Seeding from a spilled `P` this way is what this test pins:
        /// treating a spilled `P` as absent, seeding the fold with the
        /// batch's own first entry instead, would still mint correct
        /// content (`apply_locked`'s own final call still re-folds the
        /// real `P` in) but not the identical `Hlc` sequential
        /// application mints.
        /// This test's own `(version, value)` reader, shared by its
        /// sequential-reference and batched-under-test engines.
        fn stored_ver_and_value(
            engine: &Engine<u32, crate::store::crdt::PnCounter>,
            key: u32,
            kb: &Bytes,
            bucket: usize,
        ) -> (Hlc, i128) {
            let ver = engine
                .collect_buckets(&[u16::try_from(bucket).expect("bucket fits u16")], 0)
                .into_iter()
                .flat_map(|(_, entries)| entries)
                .find(|kv| kv.key.as_ref() == kb.as_ref())
                .map(|kv| kv.version)
                .expect("the key is live");
            let value = engine
                .get(&key, 0)
                .expect("the key reads back resident")
                .value();
            (ver, value)
        }

        /// A fresh engine, seeded with one `(seed, seed_ver)` record at
        /// `key`, then evicted and confirmed spilled: this test's own
        /// `P`, shared by its batched-under-test engine. Returns the temp
        /// dir (removed by the caller once done) alongside the engine.
        fn spilled_pn_counter_engine(
            dir_name: &str,
            key: u32,
            kb: &Bytes,
            bucket: usize,
            seed: crate::store::crdt::PnCounter,
            seed_ver: Hlc,
            resolver: &dyn ConflictResolver,
        ) -> (
            std::path::PathBuf,
            Arc<Engine<u32, crate::store::crdt::PnCounter>>,
        ) {
            let dir = temp_dir(dir_name);
            let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
            let tier = Arc::new(SpillTier::open(&cfg, dir_name).expect("tier opens"));
            let engine = Engine::new(u64::MAX, None, None, None);
            engine.set_spill(Arc::clone(&tier));
            let engine = Arc::new(engine);
            tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));
            let _ = put_with_resolver(&engine, key, kb.clone(), seed, seed_ver, None, 0, resolver);
            let _ = engine.evict_one_sampled(bucket, 0);
            assert!(
                poll_until(POLL_TIMEOUT, || is_spilled(&engine, kb)),
                "the flusher installs the spilled entry"
            );
            (dir, engine)
        }

        #[test]
        fn prefold_seeds_a_spilled_stored_record_so_the_batch_matches_sequential_application() {
            use crate::store::crdt::{PnCounter, PnCounterResolver};

            let resolver = PnCounterResolver;
            let key = 1u32;
            let kb = key_bytes(key);
            let bucket = stripe_index_from_hash(hash_key_bytes(kb.as_ref()));
            let node_p_id = NodeId::from(1);
            let node_p = crate::store::crdt::WriterId::new(node_p_id, 1);
            let node_b = crate::store::crdt::WriterId::new(NodeId::from(22), 1);
            let node_c = crate::store::crdt::WriterId::new(NodeId::from(33), 1);
            // A nonzero `logical` simulates `P` already carrying prior
            // merge history (a realistic starting point for a spilled
            // counter): with an all-fresh, all-zero-`logical` `P`, the
            // mint arm's `max(..) + 1` chain lands on the identical
            // `(wall_ms, logical)` regardless of fold order (both paths
            // always cost exactly two total merge steps for three
            // leaves), which would let this test pass even with the bug
            // it's meant to catch.
            let p_ver = Hlc {
                wall_ms: 1,
                logical: 5,
                node: node_p_id,
            };
            let e0 = (hlc(5, 2), PnCounter::local_delta(node_b, 4));
            let e1 = (hlc(6, 3), PnCounter::local_delta(node_c, 9));

            // The reference: `P`, then each batch entry, one real
            // `apply_many` call at a time against a plain resident
            // engine, never spilled, since spilling is this test's own
            // artifact, not part of the CRDT history being compared.
            let sequential = Engine::<u32, PnCounter>::new(u64::MAX, None, None, None);
            for (value, ver) in [
                (PnCounter::local_delta(node_p, 3), p_ver),
                (e0.1.clone(), e0.0),
                (e1.1.clone(), e1.0),
            ] {
                let _ =
                    put_with_resolver(&sequential, key, kb.clone(), value, ver, None, 0, &resolver);
            }
            let (sequential_ver, sequential_value) =
                stored_ver_and_value(&sequential, key, &kb, bucket);

            // The batch under test: `P` applied and spilled exactly like
            // the sibling test above, then `e0` and `e1` folded through
            // one real `apply_many` call, prefold-eligible since both
            // share a key and `PnCounterResolver::merges()` is `true`.
            let (dir, batched) = spilled_pn_counter_engine(
                "prefold-seed-spilled",
                key,
                &kb,
                bucket,
                PnCounter::local_delta(node_p, 3),
                p_ver,
                &resolver,
            );
            assert!(
                batched.prefold_enabled(),
                "prefold is on by default: this batch must actually exercise it"
            );

            let hash = hash_key_bytes(kb.as_ref());
            let e0_encoded = Bytes::from(postcard::to_stdvec(&e0.1).expect("encodes"));
            let e1_encoded = Bytes::from(postcard::to_stdvec(&e1.1).expect("encodes"));
            let outcomes = batched.apply_many(
                bucket,
                vec![
                    (
                        hash,
                        key,
                        kb.clone(),
                        e0.0,
                        Incoming::Put {
                            value: e0.1,
                            expires_at_ms: None,
                            encoded: e0_encoded,
                        },
                    ),
                    (
                        hash,
                        key,
                        kb.clone(),
                        e1.0,
                        Incoming::Put {
                            value: e1.1,
                            expires_at_ms: None,
                            encoded: e1_encoded,
                        },
                    ),
                ],
                &resolver,
                60_000,
                600_000,
                0,
            );
            assert_eq!(
                outcomes.len(),
                2,
                "one outcome per entry given, pre-fold or not"
            );

            let (batched_ver, batched_value) = stored_ver_and_value(&batched, key, &kb, bucket);

            assert_eq!(
                batched_value, sequential_value,
                "both paths fold every side's contribution into the same total"
            );
            assert_eq!(
                batched_ver, sequential_ver,
                "seeding the fold with the real (spilled) stored record first mints the \
                 exact version sequential per-record application would have, not merely \
                 the same content"
            );

            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}

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

//! Tests for `Engine::apply_many`, `apply_if_current` and `apply_locked`: conflict resolution, merges and the version rules.

use super::*;

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

//! Tests for the `Engine::apply_many` pre-fold: resolver gating, tombstone barriers and outcome equivalence.

use proptest::prelude::*;

use super::*;

type SetEngine = Engine<u32, std::collections::BTreeSet<String>>;
type SetEntry = (
    u64,
    u32,
    Bytes,
    Hlc,
    Incoming<std::collections::BTreeSet<String>>,
);

/// Builds one `apply_many` entry: a `Put` of the single element
/// `elem` for `key` under `ver`.
fn put_entry(key: u32, ver: Hlc, elem: &str) -> SetEntry {
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let value = string_set(&[elem]);
    let encoded = Bytes::from(postcard::to_stdvec(&value).expect("test value encodes"));
    (
        hash,
        key,
        kb,
        ver,
        Incoming::Put {
            value,
            expires_at_ms: None,
            encoded,
        },
    )
}

/// Applies `entries` to `engine` the way a real caller does: grouped
/// by the stripe each entry's hash falls into, one
/// [`Engine::apply_many`] call per touched stripe.
/// [`Engine::apply_many`] itself trusts its caller to have already
/// grouped a batch by stripe; it never checks `bucket` against the
/// entries it's given.
fn apply_batch(
    engine: &SetEngine,
    entries: Vec<SetEntry>,
    resolver: &dyn ConflictResolver,
) -> Vec<ApplyOutcome<u32, std::collections::BTreeSet<String>>> {
    let mut by_bucket: HashMap<usize, Vec<SetEntry>> = HashMap::new();
    for entry in entries {
        by_bucket
            .entry(stripe_index_from_hash(entry.0))
            .or_default()
            .push(entry);
    }
    let mut outcomes = Vec::new();
    for (bucket, group) in by_bucket {
        outcomes.extend(engine.apply_many(bucket, group, resolver, 60_000, 600_000, 0));
    }
    outcomes
}

fn arb_elem() -> impl Strategy<Value = &'static str> {
    prop_oneof![Just("a"), Just("b"), Just("c"), Just("d")]
}

/// One batch entry spec: a small key, so several entries collide on
/// one key within a batch; a real per-writer [`Hlc`] over a narrow
/// `wall_ms` range and a handful of `node`s, so ties and near-ties,
/// the cases `merge_version` treats specially, are common; and a
/// one-element `Put`.
fn arb_entry_spec() -> impl Strategy<Value = (u32, u64, u64, &'static str)> {
    (0u32..4, 0u64..12, 1u64..4, arb_elem())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Replaying the identical batch through two fresh engines, one
    /// pre-folding (the default) and one with it forced off, stores
    /// byte-for-byte, `Hlc`-for-`Hlc` the same `(version, bytes)`
    /// per key: `UnionSetResolver`'s join (set union) is
    /// commutative, associative, and idempotent, which is exactly
    /// what makes fold order irrelevant to the final *content*, the
    /// same argument `merge_version`'s doc makes for two
    /// anti-entropy replicas folding in either order, and
    /// `prefold_batch`'s seeding with the key's real stored record
    /// is what extends that fold-order independence to the minted
    /// *version* too, and reports a real,
    /// non-[`ApplyOutcome::Rejected`] outcome for exactly the same
    /// set of keys either way.
    ///
    /// A `setup` batch is applied to both engines first, through
    /// the exact same on/off split, so every key the later `main`
    /// batch touches already has a real stored record with a
    /// non-trivial version behind it, the case
    /// [`prefold_batch`]'s seeding exists for: without it, folding
    /// `main`'s own entries together before ever consulting that
    /// stored record mints a different version than sequential
    /// application, even though both still converge on the same
    /// bytes.
    #[test]
    fn prefold_on_and_off_store_identical_state_and_publish_the_same_keys(
        setup in proptest::collection::vec(arb_entry_spec(), 0..12),
        main in proptest::collection::vec(arb_entry_spec(), 1..40),
    ) {
        let resolver = UnionSetResolver;
        let build = |specs: &[(u32, u64, u64, &'static str)]| -> Vec<SetEntry> {
            specs
                .iter()
                .map(|&(key, wall_ms, node, elem)| put_entry(key, hlc(wall_ms, node), elem))
                .collect()
        };

        let on: SetEngine = Engine::new(u64::MAX, None, None, None);
        let off: SetEngine = Engine::new(u64::MAX, None, None, None);
        off.set_prefold_enabled(false);

        // Seeds each engine with real stored state per key before
        // the batch under comparison ever runs, through the same
        // on/off split as `main` below.
        apply_batch(&on, build(&setup), &resolver);
        apply_batch(&off, build(&setup), &resolver);

        let on_outcomes = apply_batch(&on, build(&main), &resolver);
        let off_outcomes = apply_batch(&off, build(&main), &resolver);
        prop_assert_eq!(on_outcomes.len(), main.len());
        prop_assert_eq!(off_outcomes.len(), main.len());

        let touched_keys: std::collections::BTreeSet<u32> = setup
            .iter()
            .chain(main.iter())
            .map(|&(key, ..)| key)
            .collect();
        for key in touched_keys {
            let kb = key_bytes(key);
            prop_assert_eq!(
                on.record_for(kb.as_ref(), 0),
                off.record_for(kb.as_ref(), 0),
                "pre-fold changes only how many apply_locked calls a batch costs, \
                 never the stored (version, bytes) — including once a key already \
                 has a real stored record behind it"
            );
        }

        let on_keys: std::collections::BTreeSet<u32> =
            on_outcomes.iter().filter_map(ApplyOutcome::key).copied().collect();
        let off_keys: std::collections::BTreeSet<u32> =
            off_outcomes.iter().filter_map(ApplyOutcome::key).copied().collect();
        prop_assert_eq!(
            on_keys, off_keys,
            "pre-fold on and off publish a real outcome for the same set of keys"
        );
    }
}

#[test]
fn a_non_merging_resolver_never_prefolds() {
    let engine: Engine<u32, String> = engine_u32_string(u64::MAX, None);
    let resolver = LwwResolver;
    let k = 1u32;
    let kb = key_bytes(k);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let value_entry = |ver: Hlc, value: &str| {
        let value = value.to_string();
        let encoded = Bytes::from(postcard::to_stdvec(&value).expect("test value encodes"));
        (
            hash,
            k,
            kb.clone(),
            ver,
            Incoming::Put {
                value,
                expires_at_ms: None,
                encoded,
            },
        )
    };
    let entries = vec![
        value_entry(hlc(1, 1), "a"),
        value_entry(hlc(2, 1), "b"),
        value_entry(hlc(3, 1), "c"),
    ];

    let outcomes = engine.apply_many(bucket, entries, &resolver, 60_000, 600_000, 0);
    assert_eq!(outcomes.len(), 3);
    assert!(
        matches!(outcomes[0], ApplyOutcome::Put { created: true, .. }),
        "the first entry for a fresh key always creates it"
    );
    // A pre-folding gate that (wrongly) fired here would report
    // `Rejected` for every index a run absorbed; `LwwResolver`'s
    // `merges()` is `false`, so every entry instead applies
    // individually, exactly as before pre-fold existed.
    assert!(
        matches!(outcomes[1], ApplyOutcome::Put { created: false, .. }),
        "a non-merging resolver applies every entry on its own, never folded away"
    );
    assert!(matches!(
        outcomes[2],
        ApplyOutcome::Put { created: false, .. }
    ));
    assert_eq!(engine.get(&k, 0), Some("c".to_string()));
}

#[test]
fn prefold_never_folds_a_run_across_a_tombstone() {
    let resolver = UnionSetResolver;
    let k = 1u32;
    let kb = key_bytes(k);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let build = || -> Vec<SetEntry> {
        vec![
            put_entry(k, hlc(1, 1), "a"),
            put_entry(k, hlc(2, 1), "b"),
            (hash, k, kb.clone(), hlc(3, 1), Incoming::Tombstone),
            put_entry(k, hlc(4, 1), "c"),
        ]
    };

    let prefolded: SetEngine = Engine::new(u64::MAX, None, None, None);
    let outcomes = prefolded.apply_many(bucket, build(), &resolver, 60_000, 600_000, 0);
    assert_eq!(outcomes.len(), 4);
    assert!(
        matches!(outcomes[0], ApplyOutcome::Rejected),
        "folded into the pre-tombstone run's survivor at index 1"
    );
    assert!(
        matches!(outcomes[1], ApplyOutcome::Put { created: true, .. }),
        "the pre-tombstone run's survivor: the union of the first two puts"
    );
    assert!(matches!(outcomes[2], ApplyOutcome::Tombstoned { .. }));
    assert!(
        matches!(outcomes[3], ApplyOutcome::Put { created: true, .. }),
        "a fresh key again after the tombstone, never folded with anything before it"
    );
    assert_eq!(
        prefolded.get(&k, 0),
        Some(string_set(&["c"])),
        "the tombstone must cut the fold: the post-tombstone put never sees \"a\"/\"b\""
    );

    let sequential: SetEngine = Engine::new(u64::MAX, None, None, None);
    sequential.set_prefold_enabled(false);
    let sequential_outcomes = sequential.apply_many(bucket, build(), &resolver, 60_000, 600_000, 0);
    assert!(matches!(
        sequential_outcomes[0],
        ApplyOutcome::Put { created: true, .. }
    ));
    assert!(matches!(
        sequential_outcomes[1],
        ApplyOutcome::Put { created: false, .. }
    ));
    assert!(matches!(
        sequential_outcomes[2],
        ApplyOutcome::Tombstoned { .. }
    ));
    assert!(matches!(
        sequential_outcomes[3],
        ApplyOutcome::Put { created: true, .. }
    ));

    assert_eq!(
        prefolded.record_for(kb.as_ref(), 0),
        sequential.record_for(kb.as_ref(), 0),
        "pre-fold changes only which position carries the real outcome, \
         never the final stored record"
    );
}

#[test]
fn set_prefold_enabled_toggles_the_flag_the_getter_reports() {
    let engine: SetEngine = Engine::new(u64::MAX, None, None, None);
    assert!(
        engine.prefold_enabled(),
        "pre-fold defaults to on, matching apply_many's own default"
    );

    engine.set_prefold_enabled(false);
    assert!(!engine.prefold_enabled());

    engine.set_prefold_enabled(true);
    assert!(engine.prefold_enabled());
}

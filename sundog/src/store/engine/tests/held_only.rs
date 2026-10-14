//! Tests for the held-only batch apply: a peer's merged record folds into a key held visible, and is rejected, storing nothing, for any other.

use std::collections::BTreeSet;

use super::super::apply::ApplyScope;
use super::*;

type SetEngine = Engine<u32, BTreeSet<String>>;

const KEY: u32 = 1;

fn set_engine(tti: Option<Duration>) -> SetEngine {
    Engine::new(u64::MAX, tti, None, None)
}

/// A batch entry putting the set `elems` at `key` at `ver`.
fn set_entry(
    key: u32,
    elems: &[&str],
    ver: Hlc,
    expires_at_ms: Option<u64>,
) -> BatchEntry<u32, BTreeSet<String>> {
    let kb = key_bytes(key);
    let value = string_set(elems);
    let encoded = Bytes::from(postcard::to_stdvec(&value).expect("test value encodes"));
    (
        hash_key_bytes(kb.as_ref()),
        key,
        kb,
        ver,
        Incoming::Put {
            value,
            expires_at_ms,
            encoded,
        },
    )
}

/// A batch entry removing `key` at `ver`.
fn removal_entry(key: u32, ver: Hlc) -> BatchEntry<u32, BTreeSet<String>> {
    let kb = key_bytes(key);
    (hash_key_bytes(kb.as_ref()), key, kb, ver, Incoming::DELETE)
}

/// Applies `entries` to `engine` under `scope` and the union resolver.
fn apply_in(
    engine: &SetEngine,
    scope: ApplyScope,
    entries: Vec<BatchEntry<u32, BTreeSet<String>>>,
    now_ms: u64,
) -> Vec<ApplyOutcome<u32, BTreeSet<String>>> {
    let bucket = stripe_index_from_hash(entries[0].0);
    engine
        .apply_many_in(
            scope,
            bucket,
            entries,
            &UnionSetResolver,
            60_000,
            600_000,
            now_ms,
            None,
            &mut Written::default(),
        )
        .0
}

fn apply_held(
    engine: &SetEngine,
    entry: BatchEntry<u32, BTreeSet<String>>,
    now_ms: u64,
) -> ApplyOutcome<u32, BTreeSet<String>> {
    apply_in(engine, ApplyScope::HeldOnly, vec![entry], now_ms).remove(0)
}

fn put_held_key(engine: &SetEngine, elems: &[&str], ver: Hlc, expires_at_ms: Option<u64>) {
    put_with_resolver(
        engine,
        KEY,
        key_bytes(KEY),
        string_set(elems),
        ver,
        expires_at_ms,
        ver.wall_ms,
        &UnionSetResolver,
    );
}

/// Starts a load of `KEY` and returns its in-flight entry.
fn start_load(engine: &SetEngine, now_ms: u64) -> Arc<Inflight<BTreeSet<String>>> {
    let kb = key_bytes(KEY);
    let JoinOutcome::Owner(inflight) =
        engine.miss_or_join(&kb, hash_key_bytes(kb.as_ref()), now_ms)
    else {
        panic!("the first caller becomes the owner");
    };
    inflight
}

/// Completes the load of `KEY` that [`start_load`] started, filling `elems`
/// stamped `stamp`.
fn finish_load(
    engine: &SetEngine,
    inflight: &Inflight<BTreeSet<String>>,
    elems: &[&str],
    stamp: Hlc,
    now_ms: u64,
) -> FillOutcome {
    let kb = key_bytes(KEY);
    let value = string_set(elems);
    let encoded = Bytes::from(postcard::to_stdvec(&value).expect("test value encodes"));
    engine.complete_fresh_load(
        FreshLoad {
            key: &KEY,
            key_bytes: &kb,
            hash: hash_key_bytes(kb.as_ref()),
            ver: stamp,
            value,
            encoded,
            expires_at_ms: None,
        },
        now_ms,
        inflight,
    )
}

#[test]
fn a_held_key_folds_the_incoming_record() {
    let engine = set_engine(None);
    put_held_key(&engine, &["a"], hlc(1_000, 1), None);

    let outcome = apply_held(&engine, set_entry(KEY, &["b"], hlc(2_000, 2), None), 2_000);

    assert!(
        matches!(outcome, ApplyOutcome::Put { created: false, .. }),
        "a fold over a visible entry is an update"
    );
    assert_eq!(engine.get(&KEY, 2_000), Some(string_set(&["a", "b"])));
    assert!(
        engine
            .record_for(key_bytes(KEY).as_ref(), 2_000)
            .expect("key is live")
            .ver
            .node
            .is_merge_derived(),
        "the fold mints a merge-derived version like any other merge"
    );
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
}

#[test]
fn a_held_fold_keeps_the_later_deadline() {
    let engine = set_engine(None);
    put_held_key(&engine, &["a"], hlc(1_000, 1), Some(9_000));
    apply_held(
        &engine,
        set_entry(KEY, &["b"], hlc(2_000, 2), Some(5_000)),
        2_000,
    );
    assert_eq!(
        engine
            .record_for(key_bytes(KEY).as_ref(), 2_000)
            .expect("key is live")
            .expires_at_ms,
        Some(9_000)
    );
}

#[test]
fn an_older_record_folds_into_a_held_key_too() {
    let engine = set_engine(None);
    put_held_key(&engine, &["a"], hlc(5_000, 1), None);
    let outcome = apply_held(&engine, set_entry(KEY, &["b"], hlc(2_000, 2), None), 5_000);
    assert!(matches!(outcome, ApplyOutcome::Put { .. }));
    assert_eq!(engine.get(&KEY, 5_000), Some(string_set(&["a", "b"])));
}

#[test]
fn an_absent_key_is_rejected_and_stores_nothing() {
    let engine = set_engine(None);
    let before = engine.digests();

    let outcome = apply_held(&engine, set_entry(KEY, &["b"], hlc(2_000, 2), None), 2_000);

    assert!(matches!(outcome, ApplyOutcome::Rejected));
    assert_eq!(engine.get(&KEY, 2_000), None);
    assert_eq!(engine.debug_totals(), (0, 0));
    assert_eq!(engine.digests(), before);
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
}

#[test]
fn a_tombstoned_key_is_rejected_and_the_tombstone_stays() {
    let engine = set_engine(None);
    put_held_key(&engine, &["a"], hlc(1_000, 1), None);
    let removed = apply_in(
        &engine,
        ApplyScope::Any,
        vec![removal_entry(KEY, hlc(2_000, 1))],
        2_000,
    );
    assert!(matches!(removed[0], ApplyOutcome::Tombstoned { .. }));
    let before = engine.digests();

    let outcome = apply_held(&engine, set_entry(KEY, &["b"], hlc(3_000, 2), None), 3_000);

    assert!(matches!(outcome, ApplyOutcome::Rejected));
    assert_eq!(
        engine.get(&KEY, 3_000),
        None,
        "the record does not resurrect the key"
    );
    assert_eq!(engine.debug_totals(), (0, 0));
    assert_eq!(engine.digests(), before, "the tombstone is untouched");
}

#[test]
fn an_expired_unswept_entry_is_rejected_and_left_in_place() {
    let engine = set_engine(None);
    put_held_key(&engine, &["a"], hlc(1_000, 1), Some(4_000));
    let before = engine.digests();

    let outcome = apply_held(&engine, set_entry(KEY, &["b"], hlc(5_000, 2), None), 5_000);

    assert!(matches!(outcome, ApplyOutcome::Rejected));
    assert_eq!(engine.get(&KEY, 5_000), None);
    assert_eq!(
        engine.digests(),
        before,
        "the sweep, not this write, removes it"
    );
    assert_eq!(
        engine.debug_totals(),
        (1, 1),
        "the expired entry is not replaced by the record"
    );
}

#[test]
fn an_idle_entry_is_rejected() {
    let engine = set_engine(Some(Duration::from_secs(1)));
    put_held_key(&engine, &["a"], hlc(1_000, 1), None);
    assert_eq!(engine.get(&KEY, 1_500), Some(string_set(&["a"])));

    let outcome = apply_held(&engine, set_entry(KEY, &["b"], hlc(5_000, 2), None), 5_000);

    assert!(matches!(outcome, ApplyOutcome::Rejected));
    assert_eq!(engine.get(&KEY, 5_000), None);
}

#[test]
fn a_removal_record_is_rejected_and_the_held_entry_stays() {
    let engine = set_engine(None);
    put_held_key(&engine, &["a"], hlc(1_000, 1), None);

    let outcome = apply_held(&engine, removal_entry(KEY, hlc(2_000, 2)), 2_000);

    assert!(matches!(outcome, ApplyOutcome::Rejected));
    assert_eq!(
        engine.get(&KEY, 2_000),
        Some(string_set(&["a"])),
        "removals reach the engine through `invalidate`"
    );
}

#[test]
fn a_batch_folds_its_held_keys_and_rejects_the_rest_in_order() {
    let (held, unheld, bucket) = same_bucket_pair(10_000);
    let engine = set_engine(None);
    put_with_resolver(
        &engine,
        held,
        key_bytes(held),
        string_set(&["a"]),
        hlc(1_000, 1),
        None,
        1_000,
        &UnionSetResolver,
    );

    let outcomes = engine
        .apply_many_in(
            ApplyScope::HeldOnly,
            bucket,
            vec![
                set_entry(unheld, &["x"], hlc(2_000, 2), None),
                set_entry(held, &["b"], hlc(2_000, 2), None),
            ],
            &UnionSetResolver,
            60_000,
            600_000,
            2_000,
            None,
            &mut Written::default(),
        )
        .0;

    assert!(matches!(outcomes[0], ApplyOutcome::Rejected));
    assert!(matches!(
        outcomes[1],
        ApplyOutcome::Put { created: false, .. }
    ));
    assert_eq!(engine.get(&unheld, 2_000), None);
    assert_eq!(engine.get(&held, 2_000), Some(string_set(&["a", "b"])));
    assert_eq!(engine.debug_totals(), (1, 1));
}

#[test]
fn repeated_records_for_a_held_key_each_fold_without_a_pre_fold() {
    let engine = set_engine(None);
    put_held_key(&engine, &["a"], hlc(1_000, 1), None);

    let outcomes = apply_in(
        &engine,
        ApplyScope::HeldOnly,
        vec![
            set_entry(KEY, &["b"], hlc(2_000, 2), None),
            set_entry(KEY, &["c"], hlc(3_000, 3), None),
        ],
        3_000,
    );

    assert!(
        outcomes
            .iter()
            .all(|o| matches!(o, ApplyOutcome::Put { .. })),
        "a pre-fold would report the absorbed record as rejected"
    );
    assert_eq!(engine.get(&KEY, 3_000), Some(string_set(&["a", "b", "c"])));
}

#[test]
fn the_any_scope_stores_a_record_for_an_absent_key() {
    let engine = set_engine(None);
    let outcomes = apply_in(
        &engine,
        ApplyScope::Any,
        vec![set_entry(KEY, &["b"], hlc(2_000, 2), None)],
        2_000,
    );
    assert!(matches!(
        outcomes[0],
        ApplyOutcome::Put { created: true, .. }
    ));
    assert_eq!(engine.get(&KEY, 2_000), Some(string_set(&["b"])));
}

#[test]
fn a_rejected_record_marks_a_load_in_flight_for_an_absent_key_invalidated() {
    let engine = set_engine(None);
    let inflight = start_load(&engine, 1_000);

    let outcome = apply_held(&engine, set_entry(KEY, &["b"], hlc(2_000, 2), None), 2_000);
    assert!(matches!(outcome, ApplyOutcome::Rejected));

    assert_eq!(
        finish_load(&engine, &inflight, &["stale"], hlc(1_000, 1), 2_000),
        FillOutcome::Invalidated,
        "the record is a change the fill's read may predate"
    );
    assert_eq!(engine.get(&KEY, 2_000), None);
}

#[test]
fn a_rejected_record_marks_a_load_in_flight_over_a_newer_record_invalidated_only_if_it_is_newer() {
    let engine = set_engine(None);
    put_held_key(&engine, &["a"], hlc(1_000, 1), Some(1_500));
    let inflight = start_load(&engine, 2_000);

    // The stored entry is expired, so the record is rejected. It is newer
    // than the stored version, so the fill is marked.
    let outcome = apply_held(&engine, set_entry(KEY, &["b"], hlc(3_000, 2), None), 3_000);
    assert!(matches!(outcome, ApplyOutcome::Rejected));
    assert_eq!(
        finish_load(&engine, &inflight, &["fresh"], hlc(2_500, 1), 3_000),
        FillOutcome::Invalidated
    );
}

#[test]
fn a_rejected_record_older_than_a_tombstone_leaves_a_load_in_flight_alone() {
    let engine = set_engine(None);
    apply_in(
        &engine,
        ApplyScope::Any,
        vec![removal_entry(KEY, hlc(10_000, 1))],
        10_000,
    );
    let inflight = start_load(&engine, 10_000);

    let outcome = apply_held(&engine, set_entry(KEY, &["b"], hlc(5_000, 2), None), 10_000);
    assert!(matches!(outcome, ApplyOutcome::Rejected));

    assert_eq!(
        finish_load(&engine, &inflight, &["fresh"], hlc(11_000, 1), 11_000),
        FillOutcome::Installed { had_live: false },
        "a record older than the stored tombstone announces nothing new"
    );
    assert_eq!(engine.get(&KEY, 11_000), Some(string_set(&["fresh"])));
}

#[test]
fn a_folded_record_leaves_a_refresh_in_flight_to_the_version_rule() {
    let engine = set_engine(None);
    put_held_key(&engine, &["a"], hlc(1_000, 1), None);
    let kb = key_bytes(KEY);
    let inflight = engine
        .begin_refresh(&kb, hash_key_bytes(kb.as_ref()))
        .expect("no load is in flight");

    let outcome = apply_held(&engine, set_entry(KEY, &["b"], hlc(2_000, 2), None), 2_000);
    assert!(matches!(outcome, ApplyOutcome::Put { .. }));

    assert_eq!(
        finish_load(&engine, &inflight, &["fresh"], hlc(1_500, 1), 2_500),
        FillOutcome::Superseded,
        "the fold is newer than the fill's stamp and stays"
    );
    assert_eq!(engine.get(&KEY, 2_500), Some(string_set(&["a", "b"])));
}

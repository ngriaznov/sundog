//! Tests for the merge fold decision: `lapsed_before`, `fold_sides` and `apply_locked` with an expired, idle or lapsed side.

use std::collections::BTreeSet;

use super::super::merge::{Fold, fold_sides, lapsed_before};
use super::*;

type SetEngine = Engine<u32, BTreeSet<String>>;

const KEY: u32 = 1;

/// Puts the set `elems` at `key` 1 under [`UnionSetResolver`].
fn put_set(
    engine: &SetEngine,
    elems: &[&str],
    ver: Hlc,
    expires_at_ms: Option<u64>,
    now_ms: u64,
) -> ApplyOutcome<u32, BTreeSet<String>> {
    put_with_resolver(
        engine,
        KEY,
        key_bytes(KEY),
        string_set(elems),
        ver,
        expires_at_ms,
        now_ms,
        &UnionSetResolver,
    )
}

fn stored_version(engine: &SetEngine, now_ms: u64) -> Hlc {
    engine
        .record_for(key_bytes(KEY).as_ref(), now_ms)
        .expect("key is live")
        .ver
}

fn stored_deadline(engine: &SetEngine, now_ms: u64) -> Option<u64> {
    engine
        .record_for(key_bytes(KEY).as_ref(), now_ms)
        .expect("key is live")
        .expires_at_ms
}

#[test]
fn lapsed_before_is_true_for_a_deadline_at_or_before_the_other_write() {
    let write = hlc(1_000, 1);
    assert!(lapsed_before(Some(999), write));
    assert!(
        lapsed_before(Some(1_000), write),
        "a record's lifetime ends at its deadline, as a read sees it"
    );
    assert!(!lapsed_before(Some(1_001), write));
    assert!(!lapsed_before(None, write), "no deadline never lapses");
}

#[test]
fn lapsed_before_reads_only_the_wall_clock_of_the_other_version() {
    let low_logical = Hlc {
        wall_ms: 1_000,
        logical: 0,
        node: NodeId::from(1u64),
    };
    let high_logical = Hlc {
        logical: u32::MAX,
        ..low_logical
    };
    assert_eq!(
        lapsed_before(Some(1_000), low_logical),
        lapsed_before(Some(1_000), high_logical)
    );
}

#[test]
fn fold_sides_folds_both_when_neither_side_lapsed() {
    let (sv, ver) = (hlc(1_000, 1), hlc(2_000, 2));
    assert_eq!(fold_sides(false, sv, None, ver, None), Fold::Both);
    assert_eq!(
        fold_sides(false, sv, Some(5_000), ver, Some(9_000)),
        Fold::Both
    );
}

#[test]
fn fold_sides_keeps_the_incoming_side_when_the_stored_side_is_dead() {
    let (sv, ver) = (hlc(1_000, 1), hlc(2_000, 2));
    assert_eq!(
        fold_sides(false, sv, Some(2_000), ver, None),
        Fold::Incoming,
        "stored lapsed before the incoming write"
    );
    assert_eq!(
        fold_sides(true, sv, None, ver, None),
        Fold::Incoming,
        "stored reads as absent on this node"
    );
}

#[test]
fn fold_sides_keeps_the_stored_side_when_the_incoming_side_is_dead() {
    let (sv, ver) = (hlc(2_000, 1), hlc(1_000, 2));
    assert_eq!(
        fold_sides(false, sv, None, ver, Some(2_000)),
        Fold::Stored,
        "incoming lapsed before the stored write"
    );
}

#[test]
fn fold_sides_defers_to_the_winner_when_both_sides_are_dead() {
    let (sv, ver) = (hlc(2_000, 1), hlc(3_000, 2));
    assert_eq!(
        fold_sides(false, sv, Some(2_500), ver, Some(2_000)),
        Fold::Winner
    );
    assert_eq!(fold_sides(true, sv, None, ver, Some(2_000)), Fold::Winner);
}

#[test]
fn fold_sides_is_symmetric_in_its_two_sides() {
    let versions = [hlc(500, 1), hlc(1_000, 2), hlc(2_000, 3)];
    let deadlines = [None, Some(400), Some(1_000), Some(1_500), Some(5_000)];
    for sv in versions {
        for ver in versions {
            for stored_exp in deadlines {
                for incoming_exp in deadlines {
                    let forward = fold_sides(false, sv, stored_exp, ver, incoming_exp);
                    let swapped = fold_sides(false, ver, incoming_exp, sv, stored_exp);
                    let expected = match forward {
                        Fold::Stored => Fold::Incoming,
                        Fold::Incoming => Fold::Stored,
                        other => other,
                    };
                    assert_eq!(
                        swapped, expected,
                        "swapping the sides swaps Stored and Incoming and keeps Both and Winner"
                    );
                }
            }
        }
    }
}

#[test]
fn an_expired_unswept_stored_entry_never_folds_into_a_merge() {
    let engine: SetEngine = Engine::new(u64::MAX, None, None, None);
    put_set(&engine, &["old"], hlc(1_000, 1), Some(5_000), 1_000);
    assert_eq!(engine.live_entry_count(), 1);

    // The incoming write predates the stored deadline, so only this node's
    // clock (6000) says the stored entry is gone.
    let outcome = put_set(&engine, &["new"], hlc(2_000, 2), None, 6_000);
    assert!(
        matches!(outcome, ApplyOutcome::Put { created: true, .. }),
        "a write over an expired entry creates"
    );
    assert_eq!(engine.get(&KEY, 6_000), Some(string_set(&["new"])));
    assert_eq!(stored_version(&engine, 6_000), hlc(2_000, 2));
    assert_eq!(stored_deadline(&engine, 6_000), None);
    assert_eq!(engine.live_entry_count(), 1);
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
}

#[test]
fn an_expired_stored_entry_with_a_newer_version_is_still_displaced() {
    let engine: SetEngine = Engine::new(u64::MAX, None, None, None);
    put_set(&engine, &["old"], hlc(3_000, 1), Some(5_000), 3_000);

    let outcome = put_set(&engine, &["new"], hlc(2_000, 2), None, 6_000);
    assert!(matches!(outcome, ApplyOutcome::Put { created: true, .. }));
    assert_eq!(engine.get(&KEY, 6_000), Some(string_set(&["new"])));
    let ver = stored_version(&engine, 6_000);
    assert!(
        ver > hlc(3_000, 1),
        "the result outranks the dead entry's version: {ver:?}"
    );
    assert!(ver.node.is_merge_derived());
}

#[test]
fn an_idle_stored_entry_never_folds_into_a_merge() {
    let engine: SetEngine = Engine::new(u64::MAX, Some(Duration::from_millis(1_000)), None, None);
    put_set(&engine, &["old"], hlc(100, 1), None, 0);

    let outcome = put_set(&engine, &["new"], hlc(200, 2), None, 5_000);
    assert!(
        matches!(outcome, ApplyOutcome::Put { created: true, .. }),
        "a write over an idle entry creates"
    );
    assert_eq!(engine.get(&KEY, 5_000), Some(string_set(&["new"])));
}

#[test]
fn a_live_stored_entry_still_folds_into_a_merge() {
    let engine: SetEngine = Engine::new(u64::MAX, Some(Duration::from_millis(10_000)), None, None);
    put_set(&engine, &["old"], hlc(100, 1), None, 0);

    let outcome = put_set(&engine, &["new"], hlc(200, 2), None, 5_000);
    assert!(matches!(outcome, ApplyOutcome::Put { created: false, .. }));
    assert_eq!(engine.get(&KEY, 5_000), Some(string_set(&["new", "old"])));
}

#[test]
fn an_incoming_record_that_lapsed_before_the_stored_write_never_folds() {
    let engine: SetEngine = Engine::new(u64::MAX, None, None, None);
    put_set(&engine, &["live"], hlc(3_000, 1), None, 3_000);

    // The incoming write is newer, but its deadline is before the stored
    // write was made.
    let outcome = put_set(&engine, &["dead"], hlc(4_000, 2), Some(2_500), 4_000);
    assert!(matches!(outcome, ApplyOutcome::Put { .. }));
    assert_eq!(engine.get(&KEY, 4_000), Some(string_set(&["live"])));
    let ver = stored_version(&engine, 4_000);
    assert!(ver > hlc(3_000, 1) && ver > hlc(4_000, 2));
    assert!(ver.node.is_merge_derived());
    assert_eq!(stored_deadline(&engine, 4_000), None);
}

#[test]
fn an_older_incoming_record_that_lapsed_before_the_stored_write_is_rejected() {
    let engine: SetEngine = Engine::new(u64::MAX, None, None, None);
    put_set(&engine, &["live"], hlc(3_000, 1), None, 3_000);

    let outcome = put_set(&engine, &["dead"], hlc(1_000, 2), Some(500), 3_000);
    assert!(
        matches!(outcome, ApplyOutcome::Rejected),
        "the stored record already is the result"
    );
    assert_eq!(engine.get(&KEY, 3_000), Some(string_set(&["live"])));
    assert_eq!(stored_version(&engine, 3_000), hlc(3_000, 1));
}

#[test]
fn two_live_records_union_and_keep_the_later_deadline() {
    for (stored_deadline_ms, incoming_deadline_ms) in [
        (Some(5_000), Some(9_000)),
        (Some(9_000), Some(5_000)),
        (None, Some(9_000)),
        (Some(9_000), None),
    ] {
        let engine: SetEngine = Engine::new(u64::MAX, None, None, None);
        put_set(&engine, &["a"], hlc(1_000, 1), stored_deadline_ms, 1_000);

        let outcome = put_set(&engine, &["b"], hlc(2_000, 2), incoming_deadline_ms, 3_000);
        assert!(matches!(outcome, ApplyOutcome::Put { created: false, .. }));
        assert_eq!(engine.get(&KEY, 3_000), Some(string_set(&["a", "b"])));
        assert_eq!(
            stored_deadline(&engine, 3_000),
            Some(9_000),
            "stored {stored_deadline_ms:?} with incoming {incoming_deadline_ms:?}"
        );
        let ver = stored_version(&engine, 3_000);
        assert!(ver.node.is_merge_derived());
    }
}

#[test]
fn two_live_records_without_a_deadline_stay_without_one() {
    let engine: SetEngine = Engine::new(u64::MAX, None, None, None);
    put_set(&engine, &["a"], hlc(1_000, 1), None, 1_000);
    put_set(&engine, &["b"], hlc(2_000, 2), None, 3_000);
    assert_eq!(stored_deadline(&engine, 3_000), None);
}

#[test]
fn the_mint_for_a_merge_without_a_deadline_hashes_the_bytes_alone() {
    let engine: SetEngine = Engine::new(u64::MAX, None, None, None);
    put_set(&engine, &["a"], hlc(1_000, 1), None, 1_000);
    put_set(&engine, &["b"], hlc(2_000, 2), None, 3_000);
    let merged = Bytes::from(postcard::to_stdvec(&string_set(&["a", "b"])).expect("encodes"));
    assert_eq!(
        stored_version(&engine, 3_000).node,
        NodeId::merge_derived(xxh3_64(merged.as_ref())),
        "a no-deadline merge mints the node a release without merged deadlines does"
    );
}

#[test]
fn a_deadline_bearing_merge_mints_a_node_the_deadline_changes() {
    let build = |incoming_deadline_ms| {
        let engine: SetEngine = Engine::new(u64::MAX, None, None, None);
        put_set(&engine, &["a"], hlc(1_000, 1), Some(5_000), 1_000);
        put_set(
            &engine,
            &["b"],
            hlc(2_000, 2),
            Some(incoming_deadline_ms),
            3_000,
        );
        stored_version(&engine, 3_000)
    };
    assert_ne!(build(6_000).node, build(7_000).node);
    assert_eq!(build(6_000), build(6_000));
}

/// A record's content, version and deadline.
type Record = (BTreeSet<String>, Hlc, Option<u64>);

fn engine_put(engine: &SetEngine, record: &Record) {
    put_with_resolver(
        engine,
        KEY,
        key_bytes(KEY),
        record.0.clone(),
        record.1,
        record.2,
        3_000,
        &UnionSetResolver,
    );
}

#[test]
fn replicas_exchanging_records_with_different_deadlines_converge() {
    let record_a = (string_set(&["a"]), hlc(1_000, 1), Some(5_000));
    let record_b = (string_set(&["b"]), hlc(2_000, 2), Some(9_000));
    let converge = |own: &Record, other: &Record| {
        let engine: SetEngine = Engine::new(u64::MAX, None, None, None);
        engine_put(&engine, own);
        engine_put(&engine, other);
        engine
            .record_for(key_bytes(KEY).as_ref(), 3_000)
            .expect("key is live")
    };
    assert_eq!(
        converge(&record_a, &record_b),
        converge(&record_b, &record_a),
        "both replicas end at the same bytes, deadline and version"
    );
}

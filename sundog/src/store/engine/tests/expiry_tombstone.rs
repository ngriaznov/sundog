//! The tombstone an expired entry leaves: the sweep's verdict and deadlines,
//! the digest it keeps, what it serves and the receipt rule that stops
//! replicas handing a collected one back and forth.

use std::sync::Arc;

use super::super::expiry::{
    SweepFate, TombstoneRetention, drops_lapsed_expiry_tombstone, expiry_tombstone_retention,
    sweep_fate, swept_tombstone_deadlines, tombstone_deadlines, tombstone_lapsed,
};
use super::*;
use crate::store::{Mode, Tombstone};

const RETENTION: TombstoneRetention = TombstoneRetention {
    ttl_ms: 60_000,
    max_ttl_ms: 600_000,
};

fn tombstone_state(engine: &Engine<u32, String>, key: u32) -> Option<Tombstone> {
    let kb = key_bytes(key);
    let bucket = stripe_index_from_hash(hash_key_bytes(kb.as_ref()));
    engine
        .stripe_lock(bucket)
        .read()
        .tombstones
        .get(kb.as_ref())
        .copied()
}

/// An engine holding key 1 at `hlc(1_000, 1)` with the deadline `expires_at`,
/// written at `1_000`.
fn engine_with_an_expiring_entry(expires_at: Option<u64>) -> Engine<u32, String> {
    let engine = engine_u32_string(u64::MAX, None);
    let _ = put(
        &engine,
        1,
        key_bytes(1),
        "v".into(),
        hlc(1_000, 1),
        expires_at,
        1_000,
    );
    engine
}

#[test]
fn sweep_fate_keeps_a_readable_entry_whatever_the_mode_leaves() {
    for tombstones in [false, true] {
        assert_eq!(
            sweep_fate(false, Some(10), 5, tombstones),
            SweepFate::Keep,
            "not yet expired"
        );
        assert_eq!(sweep_fate(false, None, 5, tombstones), SweepFate::Keep);
    }
}

#[test]
fn sweep_fate_leaves_a_tombstone_only_for_a_passed_expiry_in_a_mode_that_keeps_one() {
    assert_eq!(
        sweep_fate(true, Some(10), 10, true),
        SweepFate::Tombstone,
        "the deadline itself counts as expired"
    );
    assert_eq!(sweep_fate(true, Some(10), 99, true), SweepFate::Tombstone);
    assert_eq!(
        sweep_fate(true, Some(10), 99, false),
        SweepFate::Drop,
        "a mode that runs no anti-entropy leaves nothing"
    );
    assert_eq!(
        sweep_fate(true, Some(100), 50, true),
        SweepFate::Drop,
        "an idle entry whose deadline has not passed is dropped: idleness is local"
    );
    assert_eq!(
        sweep_fate(true, None, 50, true),
        SweepFate::Drop,
        "an idle entry with no deadline is dropped"
    );
}

#[test]
fn tombstone_deadlines_count_a_delete_from_now_and_an_expiry_from_the_expiry() {
    assert_eq!(
        tombstone_deadlines(None, 5_000, 60_000, 600_000),
        (65_000, 605_000)
    );
    assert_eq!(
        tombstone_deadlines(Some(1_000), 5_000, 60_000, 600_000),
        (61_000, 601_000),
        "an expired entry's tombstone counts from the expiry"
    );
}

#[test]
fn tombstone_deadlines_never_count_from_an_expiry_ahead_of_now() {
    assert_eq!(
        tombstone_deadlines(Some(9_000_000), 5_000, 60_000, 600_000),
        (65_000, 605_000),
        "a skewed stamp never extends the hold"
    );
}

#[test]
fn tombstone_deadlines_saturate() {
    assert_eq!(
        tombstone_deadlines(None, u64::MAX, 60_000, 600_000),
        (u64::MAX, u64::MAX)
    );
    assert_eq!(
        tombstone_deadlines(Some(u64::MAX - 1), u64::MAX, u64::MAX, u64::MAX),
        (u64::MAX, u64::MAX)
    );
}

#[test]
fn swept_tombstone_deadlines_are_never_earlier_than_the_sweep() {
    assert_eq!(
        swept_tombstone_deadlines(1_000, 5_000, RETENTION),
        (61_000, 601_000),
        "a prompt sweep counts from the expiry"
    );
    assert_eq!(
        swept_tombstone_deadlines(1_000, 700_000, RETENTION),
        (700_000, 700_000),
        "a sweep long after the expiry keeps it to the sweep itself"
    );
}

#[test]
fn tombstone_lapsed_reads_the_ttl_deadline_or_the_cap_while_deferring() {
    assert!(!tombstone_lapsed(100, 1_000, 99, false));
    assert!(tombstone_lapsed(100, 1_000, 100, false));
    assert!(
        !tombstone_lapsed(100, 1_000, 500, true),
        "collection is deferred past the ttl deadline"
    );
    assert!(tombstone_lapsed(100, 1_000, 1_000, true));
}

#[test]
fn drops_lapsed_expiry_tombstone_only_drops_a_lapsed_one_that_displaces_nothing() {
    let lapsed = (100, 1_000);
    assert!(drops_lapsed_expiry_tombstone(
        Some(40),
        false,
        lapsed,
        200,
        false
    ));
    assert!(
        !drops_lapsed_expiry_tombstone(Some(40), true, lapsed, 200, false),
        "it still removes the entry it displaces"
    );
    assert!(
        !drops_lapsed_expiry_tombstone(Some(40), false, lapsed, 99, false),
        "a tombstone within its retention is kept"
    );
    assert!(
        !drops_lapsed_expiry_tombstone(Some(40), false, lapsed, 200, true),
        "collection is deferred, so the cap is the horizon"
    );
    assert!(drops_lapsed_expiry_tombstone(
        Some(40),
        false,
        lapsed,
        1_000,
        true
    ));
    assert!(
        !drops_lapsed_expiry_tombstone(None, false, lapsed, 5_000, false),
        "a delete's deadlines count from its arrival and it is never dropped"
    );
}

#[test]
fn expiry_tombstone_retention_follows_the_mode_and_the_resolver() {
    let owners = std::num::NonZeroU8::new(2).expect("nonzero");
    for (mode, merges, kept) in [
        (Mode::Local, false, false),
        (Mode::Invalidation, false, false),
        (Mode::Replicated, false, true),
        (Mode::Distributed { owners }, false, true),
        (Mode::Replicated, true, false),
        (Mode::Distributed { owners }, true, false),
        (Mode::Local, true, false),
    ] {
        assert_eq!(
            expiry_tombstone_retention(mode, merges, 60_000, 600_000),
            kept.then_some(RETENTION),
            "{mode:?} merges: {merges}"
        );
    }
}

#[test]
fn only_the_replicated_modes_tombstone_an_expired_entry() {
    let owners = std::num::NonZeroU8::new(3).expect("nonzero");
    assert!(!Mode::Local.tombstones_expired_entries());
    assert!(!Mode::Invalidation.tombstones_expired_entries());
    assert!(Mode::Replicated.tombstones_expired_entries());
    assert!(Mode::Distributed { owners }.tombstones_expired_entries());
}

#[test]
fn sweep_with_retention_turns_an_expired_entry_into_a_tombstone_at_its_version() {
    let engine = engine_with_an_expiring_entry(Some(1_500));
    assert_eq!(engine.tombstone_count(), 0);

    engine.sweep(2_000, Some(RETENTION));

    assert_eq!(engine.get(&1, 2_000), None);
    let t = tombstone_state(&engine, 1).expect("the sweep left a tombstone");
    assert_eq!(t.ver, hlc(1_000, 1), "at the entry's own version");
    assert_eq!(
        (t.ttl_deadline_ms, t.max_deadline_ms),
        (61_500, 601_500),
        "its retentions count from the expiry"
    );
    assert_eq!(t.expired_at_ms, Some(1_500));
    assert_eq!(engine.tombstone_count(), 1);
    assert_eq!(
        engine.debug_totals(),
        (0, 0),
        "the tombstone is not a live entry and holds no weight"
    );
    assert_eq!(
        engine.inspect(key_bytes(1).as_ref(), 2_000),
        LocalRecord::Tombstone {
            version: hlc(1_000, 1)
        }
    );
}

#[test]
fn sweep_without_retention_leaves_no_trace() {
    let engine = engine_with_an_expiring_entry(Some(1_500));
    engine.sweep(2_000, None);
    assert_eq!(engine.tombstone_count(), 0);
    assert_eq!(
        engine.inspect(key_bytes(1).as_ref(), 2_000),
        LocalRecord::Absent
    );
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
}

#[test]
fn sweep_with_retention_keeps_the_digest_until_the_tombstone_is_collected() {
    let engine = engine_with_an_expiring_entry(Some(1_500));
    let before = engine.digests();

    engine.sweep(2_000, Some(RETENTION));
    assert_eq!(
        engine.digests(),
        before,
        "the tombstone has the entry's fingerprint, so the sweep moves nothing"
    );
    assert_eq!(engine.digests(), engine.recompute_digests_paired());

    engine.gc_tombstones(false, 61_499);
    assert_eq!(engine.tombstone_count(), 1, "kept until its retention ends");
    engine.gc_tombstones(false, 61_500);
    assert_eq!(engine.tombstone_count(), 0);
    assert_ne!(engine.digests(), before, "collecting drops the fingerprint");
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
    assert!(
        engine.digests().iter().all(|&(_, digest)| digest == 0),
        "an empty engine digests to zero"
    );
}

#[test]
fn sweep_with_retention_holds_the_cap_while_collection_is_deferred() {
    let engine = engine_with_an_expiring_entry(Some(1_500));
    engine.sweep(2_000, Some(RETENTION));
    engine.gc_tombstones(true, 100_000);
    assert_eq!(engine.tombstone_count(), 1, "an absent member holds it");
    engine.gc_tombstones(true, 601_499);
    assert_eq!(engine.tombstone_count(), 1);
    engine.gc_tombstones(true, 601_500);
    assert_eq!(
        engine.tombstone_count(),
        0,
        "the cap collects it regardless"
    );
}

#[test]
fn a_late_sweep_keeps_the_tombstone_to_the_sweep_instant() {
    let engine = engine_with_an_expiring_entry(Some(1_500));
    engine.sweep(700_000, Some(RETENTION));
    let t = tombstone_state(&engine, 1).expect("tombstone");
    assert_eq!((t.ttl_deadline_ms, t.max_deadline_ms), (700_000, 700_000));
}

#[test]
fn sweep_with_retention_clears_the_long_ttl_row() {
    let engine = engine_u32_string(u64::MAX, None);
    let kb = key_bytes(1);
    let bucket = stripe_index_from_hash(hash_key_bytes(kb.as_ref()));
    let far = MAX_INLINE_TTL_MS + 5_000;
    let _ = put(&engine, 1, kb.clone(), "a".into(), hlc(1, 1), Some(far), 0);
    assert!(
        engine
            .stripe_lock(bucket)
            .read()
            .long_ttl
            .contains_key(kb.as_ref())
    );

    engine.sweep(far + 1, Some(RETENTION));

    assert!(
        !engine
            .stripe_lock(bucket)
            .read()
            .long_ttl
            .contains_key(kb.as_ref()),
        "the tombstone needs no side-table row"
    );
    assert_eq!(
        tombstone_state(&engine, 1).map(|t| t.expired_at_ms),
        Some(Some(far)),
        "it keeps the long deadline as its anchor"
    );
}

#[test]
fn sweep_with_retention_drops_an_idle_entry_without_a_tombstone() {
    let engine = engine_u32_string(u64::MAX, Some(Duration::from_millis(100)));
    let _ = put(&engine, 1, key_bytes(1), "a".into(), hlc(1, 1), None, 1_000);
    let _ = put(
        &engine,
        2,
        key_bytes(2),
        "b".into(),
        hlc(1, 1),
        Some(1_050),
        1_000,
    );

    engine.sweep(5_000, Some(RETENTION));

    assert_eq!(engine.live_entry_count(), 0);
    assert!(
        tombstone_state(&engine, 1).is_none(),
        "idleness is local to a node: a tombstone would delete the key on its peers"
    );
    assert!(
        tombstone_state(&engine, 2).is_some(),
        "an entry both expired and idle still left one: the verdict reads the expiry alone"
    );
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
}

#[test]
fn a_swept_expiry_tombstone_rejects_an_older_apply_and_accepts_a_newer_one() {
    let engine = engine_with_an_expiring_entry(Some(1_500));
    engine.sweep(2_000, Some(RETENTION));

    let older = put(
        &engine,
        1,
        key_bytes(1),
        "old".into(),
        hlc(500, 2),
        None,
        2_100,
    );
    assert!(
        matches!(older, ApplyOutcome::Rejected),
        "the overwritten value stays out"
    );
    let same = put(
        &engine,
        1,
        key_bytes(1),
        "same".into(),
        hlc(1_000, 1),
        None,
        2_100,
    );
    assert!(
        matches!(same, ApplyOutcome::Rejected),
        "the expired version itself does not come back"
    );
    assert_eq!(engine.get(&1, 2_200), None);

    let newer = put(
        &engine,
        1,
        key_bytes(1),
        "new".into(),
        hlc(3_000, 2),
        None,
        2_300,
    );
    assert!(matches!(newer, ApplyOutcome::Put { .. }));
    assert_eq!(engine.get(&1, 2_400), Some("new".to_string()));
    assert_eq!(engine.tombstone_count(), 0, "a live write replaces it");
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
}

#[test]
fn a_swept_expiry_tombstone_is_served_with_its_expiry() {
    let engine = engine_with_an_expiring_entry(Some(1_500));
    let kb = key_bytes(1);
    engine.sweep(2_000, Some(RETENTION));

    let record = engine
        .record_for(kb.as_ref(), 2_000)
        .expect("a held tombstone is served");
    assert_eq!(record.value, None);
    assert_eq!(record.ver, hlc(1_000, 1));
    assert_eq!(
        record.expires_at_ms,
        Some(1_500),
        "the expiry travels so a receiver anchors on it"
    );
    let snapshot = engine.snapshot_records(2_000);
    assert_eq!(snapshot, vec![record]);

    let bucket = u16::try_from(stripe_index_from_hash(hash_key_bytes(kb.as_ref())))
        .expect("a bucket index fits");
    let listed = engine.collect_buckets(&[bucket], 2_000);
    assert_eq!(
        listed[0].1,
        vec![KeyVersion {
            key: kb,
            version: hlc(1_000, 1)
        }],
        "the listing carries it at the entry's version"
    );
}

#[test]
fn a_delete_tombstone_is_served_without_an_expiry() {
    let engine = engine_u32_string(u64::MAX, None);
    let _ = tombstone(&engine, 1, key_bytes(1), hlc(1_000, 1), 2_000);
    let record = engine
        .record_for(key_bytes(1).as_ref(), 2_000)
        .expect("tombstone");
    assert_eq!(record.expires_at_ms, None);
}

#[test]
fn an_expiry_tombstone_arrives_with_deadlines_anchored_on_its_expiry() {
    let engine = engine_u32_string(u64::MAX, None);
    let outcome = tombstone_of(
        &engine,
        1,
        key_bytes(1),
        hlc(1_000, 1),
        Some(1_500),
        2_000,
        false,
    );
    assert!(matches!(outcome, ApplyOutcome::Tombstoned { .. }));
    let t = tombstone_state(&engine, 1).expect("stored within its retention");
    assert_eq!((t.ttl_deadline_ms, t.max_deadline_ms), (61_500, 601_500));
    assert_eq!(t.expired_at_ms, Some(1_500));
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
}

#[test]
fn a_lapsed_expiry_tombstone_for_a_key_holding_nothing_is_dropped() {
    let engine = engine_u32_string(u64::MAX, None);
    let digests = engine.digests();
    let outcome = tombstone_of(
        &engine,
        1,
        key_bytes(1),
        hlc(1_000, 1),
        Some(1_500),
        70_000,
        false,
    );
    assert!(matches!(outcome, ApplyOutcome::Rejected));
    assert_eq!(engine.tombstone_count(), 0);
    assert_eq!(engine.digests(), digests, "nothing is fingerprinted");
}

#[test]
fn a_lapsed_expiry_tombstone_still_removes_the_older_entry_it_displaces() {
    let engine = engine_u32_string(u64::MAX, None);
    let _ = put(
        &engine,
        1,
        key_bytes(1),
        "old".into(),
        hlc(500, 2),
        None,
        1_000,
    );

    let outcome = tombstone_of(
        &engine,
        1,
        key_bytes(1),
        hlc(1_000, 1),
        Some(1_500),
        70_000,
        false,
    );

    assert!(matches!(outcome, ApplyOutcome::Tombstoned { .. }));
    assert_eq!(engine.get(&1, 70_000), None, "the older copy is deleted");
    assert_eq!(engine.live_entry_count(), 0);
    let t = tombstone_state(&engine, 1).expect("kept until the next collection");
    assert!(
        tombstone_lapsed(t.ttl_deadline_ms, t.max_deadline_ms, 70_000, false),
        "already collectible"
    );
    engine.gc_tombstones(false, 70_000);
    assert_eq!(engine.tombstone_count(), 0);
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
}

#[test]
fn a_lapsed_expiry_tombstone_replaces_an_older_tombstone() {
    let engine = engine_u32_string(u64::MAX, None);
    let _ = tombstone(&engine, 1, key_bytes(1), hlc(500, 2), 69_000);

    let outcome = tombstone_of(
        &engine,
        1,
        key_bytes(1),
        hlc(1_000, 1),
        Some(1_500),
        70_000,
        false,
    );

    assert!(matches!(outcome, ApplyOutcome::Tombstoned { .. }));
    assert_eq!(
        tombstone_state(&engine, 1).map(|t| t.ver),
        Some(hlc(1_000, 1)),
        "the newer version is the floor"
    );
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
}

#[test]
fn a_lapsed_delete_tombstone_is_always_stored() {
    let engine = engine_u32_string(u64::MAX, None);
    let outcome = tombstone_of(&engine, 1, key_bytes(1), hlc(1, 2), None, 9_000_000, false);
    assert!(matches!(outcome, ApplyOutcome::Tombstoned { .. }));
    let t = tombstone_state(&engine, 1).expect("stored");
    assert_eq!(
        (t.ttl_deadline_ms, t.max_deadline_ms),
        (9_060_000, 9_600_000),
        "a delete counts its retention from its arrival, whatever its version says"
    );
}

#[test]
fn a_deferring_collection_keeps_an_expiry_tombstone_past_its_ttl_deadline() {
    let engine = engine_u32_string(u64::MAX, None);
    let outcome = tombstone_of(
        &engine,
        1,
        key_bytes(1),
        hlc(1_000, 1),
        Some(1_500),
        70_000,
        true,
    );
    assert!(matches!(outcome, ApplyOutcome::Tombstoned { .. }));
    let t = tombstone_state(&engine, 1).expect("the floor an absent member may need");
    assert_eq!((t.ttl_deadline_ms, t.max_deadline_ms), (61_500, 601_500));
}

#[test]
fn collection_tells_the_apply_path_whether_it_is_deferring() {
    let engine = engine_u32_string(u64::MAX, None);
    let apply = |engine: &Engine<u32, String>, key: u32| {
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        engine.apply_many(
            stripe_index_from_hash(hash),
            vec![(
                hash,
                key,
                kb,
                hlc(1_000, 1),
                Incoming::Tombstone {
                    expired_at_ms: Some(1_500),
                },
            )],
            &LwwResolver,
            60_000,
            600_000,
            70_000,
        )
    };

    engine.gc_tombstones(false, 70_000);
    assert!(matches!(apply(&engine, 1)[0], ApplyOutcome::Rejected));
    engine.gc_tombstones(true, 70_000);
    assert!(
        matches!(apply(&engine, 2)[0], ApplyOutcome::Tombstoned { .. }),
        "an absent member holds the tombstone to its cap"
    );
    engine.gc_tombstones(false, 70_000);
    assert!(matches!(apply(&engine, 3)[0], ApplyOutcome::Rejected));
}

#[test]
fn a_sweep_after_a_fill_over_the_expiry_tombstone_keeps_the_accounting_exact() {
    let engine = Arc::new(engine_with_an_expiring_entry(Some(1_500)));
    engine.sweep(2_000, Some(RETENTION));
    let _ = put(
        &engine,
        1,
        key_bytes(1),
        "v2".into(),
        hlc(3_000, 2),
        Some(3_500),
        3_000,
    );
    engine.sweep(4_000, Some(RETENTION));
    let t = tombstone_state(&engine, 1).expect("the second expiry left its own");
    assert_eq!(t.ver, hlc(3_000, 2));
    assert_eq!(t.expired_at_ms, Some(3_500));
    assert_eq!(engine.tombstone_count(), 1);
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
}

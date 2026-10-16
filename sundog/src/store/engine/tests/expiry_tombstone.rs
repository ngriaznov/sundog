//! The tombstone an expired entry leaves: the sweep's verdict and deadlines,
//! the digest it keeps, what it serves and the lapsed-arrival rule that
//! stops replicas handing a collected one back and forth.

use std::sync::Arc;

use super::super::expiry::{
    SweepFate, TombstoneRetention, drops_lapsed_tombstone, expiry_tombstone_retention,
    gc_defer_decision, gc_period_ms, pause_gap_ms, pause_hold_after, paused_since, sweep_fate,
    swept_tombstone_deadlines, tombstone_anchor_ms, tombstone_deadlines, tombstone_lapsed,
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
fn tombstone_anchor_counts_a_delete_from_its_stamp_and_an_expiry_from_the_expiry() {
    assert_eq!(
        tombstone_anchor_ms(None, 1_000, 5_000),
        1_000,
        "a delete counts from its version stamp"
    );
    assert_eq!(
        tombstone_anchor_ms(Some(2_000), 1_000, 5_000),
        2_000,
        "an expired entry's tombstone counts from the expiry, not the stamp"
    );
    assert_eq!(
        tombstone_anchor_ms(None, 0, 5_000),
        0,
        "a stamp of zero is a valid anchor"
    );
    assert_eq!(
        tombstone_anchor_ms(None, 5_000, 5_000),
        5_000,
        "a stamp at now is itself"
    );
}

#[test]
fn tombstone_anchor_never_counts_from_beyond_now() {
    assert_eq!(
        tombstone_anchor_ms(None, 9_000_000, 5_000),
        5_000,
        "a delete stamped ahead of this clock counts from this clock"
    );
    assert_eq!(
        tombstone_anchor_ms(Some(9_000_000), 1_000, 5_000),
        5_000,
        "so does an expiry ahead of it"
    );
    assert_eq!(tombstone_anchor_ms(None, u64::MAX, u64::MAX), u64::MAX);
    assert_eq!(
        tombstone_anchor_ms(None, 7, 0),
        0,
        "now at zero caps at zero"
    );
}

#[test]
fn tombstone_deadlines_add_the_two_retentions_to_the_anchor() {
    assert_eq!(
        tombstone_deadlines(tombstone_anchor_ms(None, 1_000, 5_000), 60_000, 600_000),
        (61_000, 601_000),
        "a delete stamped at 1,000 and received at 5,000 counts from the stamp"
    );
    assert_eq!(
        tombstone_deadlines(
            tombstone_anchor_ms(Some(1_000), 3_000, 5_000),
            60_000,
            600_000
        ),
        (61_000, 601_000),
        "an expired entry's tombstone counts from the expiry"
    );
}

#[test]
fn tombstone_deadlines_never_count_from_an_expiry_ahead_of_now() {
    assert_eq!(
        tombstone_deadlines(
            tombstone_anchor_ms(Some(9_000_000), 0, 5_000),
            60_000,
            600_000
        ),
        (65_000, 605_000),
        "a skewed stamp never extends the hold"
    );
}

#[test]
fn tombstone_deadlines_saturate() {
    assert_eq!(
        tombstone_deadlines(u64::MAX, 60_000, 600_000),
        (u64::MAX, u64::MAX)
    );
    assert_eq!(
        tombstone_deadlines(u64::MAX - 1, u64::MAX, u64::MAX),
        (u64::MAX, u64::MAX)
    );
    assert_eq!(tombstone_deadlines(0, 0, 0), (0, 0));
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

/// `drops_lapsed_tombstone`'s whole truth table: (from a peer) x (deferring)
/// x (within the ttl deadline, past it only, past the cap), with deadlines
/// `(100, 1_000)`.
#[test]
fn drops_lapsed_tombstone_drops_exactly_a_lapsed_peer_tombstone() {
    let deadlines = (100, 1_000);
    for (from_peer, deferring, now_ms, dropped) in [
        (true, false, 99, false),
        (true, false, 100, true),
        (true, false, 999, true),
        (true, false, 1_000, true),
        (true, true, 99, false),
        (true, true, 100, false),
        (true, true, 999, false),
        (true, true, 1_000, true),
        (true, true, u64::MAX, true),
        (false, false, 99, false),
        (false, false, 100, false),
        (false, false, 1_000, false),
        (false, true, 100, false),
        (false, true, 1_000, false),
        (false, true, u64::MAX, false),
    ] {
        assert_eq!(
            drops_lapsed_tombstone(from_peer, deadlines, now_ms, deferring),
            dropped,
            "from_peer {from_peer}, deferring {deferring}, now {now_ms}"
        );
    }
}

#[test]
fn drops_lapsed_tombstone_agrees_with_tombstone_lapsed_for_a_peer() {
    for (ttl, max) in [(0, 0), (100, 1_000), (100, 100), (u64::MAX, u64::MAX)] {
        for now_ms in [0, 99, 100, 101, 999, 1_000, 1_001, u64::MAX] {
            for deferring in [false, true] {
                assert_eq!(
                    drops_lapsed_tombstone(true, (ttl, max), now_ms, deferring),
                    tombstone_lapsed(ttl, max, now_ms, deferring),
                    "({ttl}, {max}) at {now_ms}, deferring {deferring}"
                );
            }
        }
    }
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
fn only_the_replicated_modes_defer_tombstone_collection() {
    let owners = std::num::NonZeroU8::new(3).expect("nonzero");
    assert!(!Mode::Local.defers_tombstone_gc());
    assert!(!Mode::Invalidation.defers_tombstone_gc());
    assert!(Mode::Replicated.defers_tombstone_gc());
    assert!(Mode::Distributed { owners }.defers_tombstone_gc());
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

    engine.gc_tombstones(false, 61_499, RETENTION.ttl_ms);
    assert_eq!(engine.tombstone_count(), 1, "kept until its retention ends");
    engine.gc_tombstones(false, 61_500, RETENTION.ttl_ms);
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
    engine.gc_tombstones(true, 100_000, RETENTION.ttl_ms);
    assert_eq!(engine.tombstone_count(), 1, "an absent member holds it");
    engine.gc_tombstones(true, 601_499, RETENTION.ttl_ms);
    assert_eq!(engine.tombstone_count(), 1);
    engine.gc_tombstones(true, 601_500, RETENTION.ttl_ms);
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
fn a_lapsed_expiry_tombstone_leaves_the_older_entry_it_would_displace() {
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

    assert!(
        matches!(outcome, ApplyOutcome::Rejected),
        "the outcome does not depend on what the key holds"
    );
    assert_eq!(engine.get(&1, 70_000), Some("old".into()));
    assert_eq!(engine.tombstone_count(), 0);
    assert_eq!(engine.digests(), digests);
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
}

#[test]
fn a_lapsed_expiry_tombstone_within_its_cap_removes_the_older_entry_while_deferring() {
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
        true,
    );

    assert!(matches!(outcome, ApplyOutcome::Tombstoned { .. }));
    assert_eq!(engine.get(&1, 70_000), None, "the older copy is deleted");
    assert_eq!(engine.live_entry_count(), 0);
    let t = tombstone_state(&engine, 1).expect("kept until the next collection");
    assert!(
        tombstone_lapsed(t.ttl_deadline_ms, t.max_deadline_ms, 70_000, false),
        "already collectible once collection stops deferring"
    );
    engine.gc_tombstones(false, 70_000, RETENTION.ttl_ms);
    assert_eq!(engine.tombstone_count(), 0);
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
}

#[test]
fn a_lapsed_expiry_tombstone_past_its_cap_leaves_the_older_entry_even_while_deferring() {
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
        601_500,
        true,
    );

    assert!(matches!(outcome, ApplyOutcome::Rejected));
    assert_eq!(engine.get(&1, 601_500), Some("old".into()));
    assert_eq!(engine.tombstone_count(), 0);
}

#[test]
fn a_lapsed_expiry_tombstone_leaves_an_older_tombstone() {
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

    assert!(matches!(outcome, ApplyOutcome::Rejected));
    assert_eq!(
        tombstone_state(&engine, 1).map(|t| t.ver),
        Some(hlc(500, 2)),
        "the older tombstone stays until its own collection"
    );
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
}

#[test]
fn a_lapsed_expiry_tombstone_within_its_cap_replaces_an_older_tombstone_while_deferring() {
    let engine = engine_u32_string(u64::MAX, None);
    let _ = tombstone(&engine, 1, key_bytes(1), hlc(500, 2), 69_000);

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
    let outcome = tombstone_arriving(
        &engine,
        1,
        key_bytes(1),
        hlc(1, 2),
        None,
        9_000_000,
        false,
        Arrival::Local,
    );
    assert!(
        matches!(outcome, ApplyOutcome::Tombstoned { .. }),
        "this node's own delete is stored however late it applies"
    );
    let t = tombstone_state(&engine, 1).expect("stored");
    assert_eq!(
        (t.ttl_deadline_ms, t.max_deadline_ms),
        (60_001, 600_001),
        "a delete counts its retention from its stamp, so this one is collectible at once"
    );
    assert_eq!(engine.tombstone_count(), 1);
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

/// `engine.apply_many` of the tombstone of an entry that expired at 1,500,
/// version `hlc(1_000, 1)`, for `key`, at 70,000: past its 60 s `ttl`
/// deadline, within its 600 s cap.
fn apply_expiry_tombstone(engine: &Engine<u32, String>, key: u32) -> ApplyOutcome<u32, String> {
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    engine
        .apply_many(
            Arrival::Peer,
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
        .remove(0)
}

#[test]
fn collection_tells_the_apply_path_whether_it_is_deferring() {
    let engine = engine_u32_string(u64::MAX, None);

    engine.gc_tombstones(false, 70_000, RETENTION.ttl_ms);
    assert!(matches!(
        apply_expiry_tombstone(&engine, 1),
        ApplyOutcome::Rejected
    ));
    engine.gc_tombstones(true, 70_000, RETENTION.ttl_ms);
    assert!(
        matches!(
            apply_expiry_tombstone(&engine, 2),
            ApplyOutcome::Tombstoned { .. }
        ),
        "a deferred collection holds the tombstone to its cap"
    );
    engine.gc_tombstones(false, 70_000, RETENTION.ttl_ms);
    assert!(matches!(
        apply_expiry_tombstone(&engine, 3),
        ApplyOutcome::Rejected
    ));
}

#[test]
fn an_engine_built_to_start_deferred_judges_arrivals_against_the_cap_until_a_collection() {
    let old = |engine: &Engine<u32, String>| {
        let _ = put(
            engine,
            1,
            key_bytes(1),
            "old".into(),
            hlc(500, 2),
            None,
            1_000,
        );
    };

    let plain = engine_u32_string(u64::MAX, None);
    old(&plain);
    assert!(matches!(
        apply_expiry_tombstone(&plain, 1),
        ApplyOutcome::Rejected
    ));
    assert_eq!(
        plain.get(&1, 70_000),
        Some("old".into()),
        "an engine not told to defer drops it"
    );

    let settling = engine_u32_string(u64::MAX, None).starting_gc_deferred(true);
    old(&settling);
    assert!(
        matches!(
            apply_expiry_tombstone(&settling, 1),
            ApplyOutcome::Tombstoned { .. }
        ),
        "before any collection it keeps a tombstone lapsed by ttl alone"
    );
    assert_eq!(
        settling.get(&1, 70_000),
        None,
        "and so deletes the old copy"
    );

    settling.gc_tombstones(false, 70_000, RETENTION.ttl_ms);
    assert_eq!(settling.tombstone_count(), 0);
    let _ = put(
        &settling,
        2,
        key_bytes(2),
        "old".into(),
        hlc(500, 2),
        None,
        1_000,
    );
    assert!(
        matches!(apply_expiry_tombstone(&settling, 2), ApplyOutcome::Rejected),
        "the first collection ends the deferral"
    );
    assert_eq!(settling.get(&2, 70_000), Some("old".into()));
    assert_eq!(settling.digests(), settling.recompute_digests_paired());
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

#[test]
fn gc_period_is_a_quarter_of_the_ttl_with_a_floor_of_a_second() {
    for (ttl_ms, period_ms) in [
        (0, 1_000),
        (1_000, 1_000),
        (3_999, 1_000),
        (4_000, 1_000),
        (7_999, 1_999),
        (8_000, 2_000),
        (600_000, 150_000),
        (u64::MAX, u64::MAX / 4),
    ] {
        assert_eq!(gc_period_ms(ttl_ms), period_ms, "ttl {ttl_ms}");
    }
}

#[test]
fn pause_gap_is_twice_the_gc_period_and_half_the_ttl_from_four_seconds_up() {
    for ttl_ms in [0, 1_000, 2_000, 3_000, 4_000, 8_000, 600_000, u64::MAX] {
        assert_eq!(
            pause_gap_ms(ttl_ms),
            gc_period_ms(ttl_ms).saturating_mul(2),
            "ttl {ttl_ms}"
        );
    }
    assert_eq!(
        pause_gap_ms(2_000),
        2_000,
        "the floor: a gap as long as the ttl"
    );
    assert_eq!(pause_gap_ms(4_000), 2_000, "half the ttl from here up");
    assert_eq!(pause_gap_ms(600_000), 300_000);
    assert_eq!(pause_gap_ms(u64::MAX), u64::MAX / 4 * 2);
}

#[test]
fn paused_since_is_strictly_over_the_gap_and_never_before_a_collection() {
    let ttl_ms = 60_000;
    let gap = pause_gap_ms(ttl_ms);
    assert_eq!(gap, 30_000);
    assert!(
        !paused_since(0, u64::MAX, ttl_ms),
        "0 means no collection yet"
    );
    assert!(!paused_since(100_000, 100_000, ttl_ms));
    assert!(
        !paused_since(100_000, 100_000 + gap, ttl_ms),
        "exactly the gap"
    );
    assert!(paused_since(100_000, 100_000 + gap + 1, ttl_ms), "one over");
    assert!(
        !paused_since(100_000, 99_999, ttl_ms),
        "a clock that stepped back"
    );
    assert!(!paused_since(100_000, 0, ttl_ms));
    assert!(paused_since(1, u64::MAX, ttl_ms));
    assert!(!paused_since(u64::MAX, u64::MAX, ttl_ms));
    assert!(
        !paused_since(1, 1 + gap, u64::MAX),
        "a gap of u64::MAX / 2 saturates without wrapping"
    );
}

#[test]
fn a_longer_gap_is_never_less_paused() {
    for last in [1, 1_000, 1_000_000] {
        let mut was = false;
        for now in (0..200_000).step_by(997).map(|n| last + n) {
            let is = paused_since(last, now, 60_000);
            assert!(is || !was, "last {last} now {now}");
            was = is;
        }
    }
}

#[test]
fn pause_hold_after_extends_a_hold_for_a_pause_and_leaves_it_otherwise() {
    assert_eq!(pause_hold_after(0, true, 100_000, 60_000), 160_000);
    assert_eq!(pause_hold_after(500_000, true, 100_000, 60_000), 500_000);
    assert_eq!(pause_hold_after(150_000, true, 100_000, 60_000), 160_000);
    assert_eq!(pause_hold_after(0, false, 100_000, 60_000), 0);
    assert_eq!(pause_hold_after(500_000, false, 100_000, 60_000), 500_000);
    assert_eq!(pause_hold_after(0, true, u64::MAX, 60_000), u64::MAX);
    assert_eq!(pause_hold_after(0, true, 100_000, 0), 100_000);
}

#[test]
fn gc_defer_decision_defers_for_the_caller_a_pause_or_a_hold() {
    // (defer, paused, hold_until, now) -> (effective, entered_hold)
    for (defer, paused, hold_until, now, expected) in [
        (false, false, 0, 100, (false, false)),
        (true, false, 0, 100, (true, false)),
        (false, true, 0, 100, (true, true)),
        (true, true, 0, 100, (true, true)),
        (false, false, 101, 100, (true, false)),
        (false, false, 100, 100, (false, false)),
        (false, false, 99, 100, (false, false)),
        (true, false, 101, 100, (true, false)),
        (false, true, 101, 100, (true, false)),
        (true, true, 101, 100, (true, false)),
        (false, true, 100, 100, (true, true)),
        (false, false, u64::MAX, u64::MAX, (false, false)),
        (false, false, u64::MAX, u64::MAX - 1, (true, false)),
    ] {
        assert_eq!(
            gc_defer_decision(defer, paused, hold_until, now),
            expected,
            "defer {defer} paused {paused} hold {hold_until} now {now}"
        );
    }
}

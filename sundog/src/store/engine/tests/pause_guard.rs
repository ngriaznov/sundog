//! The pause guard: a node whose wall clock shows a gap longer than twice the
//! gc period since its last collection defers for `tombstone_ttl`, judging
//! arrivals against the cap deadline and keeping its tombstones to it.
//!
//! With a 60 s retention the gc period is 15 s and the gap 30 s.

use super::super::expiry::{gc_period_ms, paused_since};
use super::tombstone_arrival::{apply_records, entry, put_of};
use super::*;

const TTL: u64 = 60_000;
const GAP: u64 = 30_000;

/// An engine with the pause guard on, its first collection at `first_gc_ms`.
fn guarded(first_gc_ms: u64) -> Engine<u32, String> {
    let engine = engine_u32_string(u64::MAX, None).guarding_pauses(true);
    assert_eq!(engine.gc_tombstones(false, first_gc_ms, TTL), None);
    engine
}

/// A delete stamped at 1,000 (ttl deadline 61,000, cap 601,000) from a peer
/// at `now_ms`; whether it was stored.
fn peer_delete_is_stored(engine: &Engine<u32, String>, now_ms: u64) -> bool {
    let (outcomes, _) = apply_records(
        engine,
        Arrival::Peer,
        now_ms,
        vec![entry(1, hlc(1_000, 1), Incoming::DELETE)],
    );
    matches!(outcomes[0], ApplyOutcome::Tombstoned { .. })
}

fn holding_an_older_value(first_gc_ms: u64) -> Engine<u32, String> {
    let engine = guarded(first_gc_ms);
    let _ = apply_records(
        &engine,
        Arrival::Peer,
        first_gc_ms,
        vec![entry(1, hlc(500, 2), put_of("old"))],
    );
    assert_eq!(engine.get(&1, first_gc_ms), Some("old".into()));
    engine
}

#[test]
fn an_engine_without_the_guard_never_sees_a_pause() {
    let engine = engine_u32_string(u64::MAX, None);
    assert_eq!(engine.gc_tombstones(false, 100_000, TTL), None);
    for now_ms in [100_000 + GAP + 1, 10_000_000, u64::MAX] {
        assert!(!engine.deferring_at(now_ms, TTL), "now {now_ms}");
    }
    assert_eq!(engine.gc_tombstones(false, 10_000_000, TTL), None);
    assert!(!engine.gc_deferring());
    assert_eq!(engine.last_gc_ms(), 0, "nothing is recorded");
    assert_eq!(engine.pause_hold_until_ms(), 0);
}

#[test]
fn the_guard_needs_a_collection_to_measure_from() {
    let engine = engine_u32_string(u64::MAX, None).guarding_pauses(true);
    assert_eq!(engine.last_gc_ms(), 0);
    assert!(!engine.deferring_at(u64::MAX, TTL), "no collection yet");
}

#[test]
fn a_collection_at_clock_zero_counts_as_a_collection() {
    let engine = guarded(0);
    assert_eq!(engine.last_gc_ms(), 1, "stored as 1, not the 0 sentinel");
    assert!(!engine.deferring_at(GAP + 1, TTL), "a gap of GAP exactly");
    assert!(engine.deferring_at(GAP + 2, TTL), "one over");
}

/// The gap is judged at apply time, before any collection runs: a delete
/// lapsed past its ttl is ignored at exactly the gap since the last
/// collection and stored, deleting the older value, one millisecond later.
#[test]
fn the_guard_is_in_force_at_apply_time_one_millisecond_past_the_gap() {
    let t0 = 100_000;
    let engine = holding_an_older_value(t0);
    assert!(!engine.deferring_at(t0 + GAP, TTL));
    assert!(
        !peer_delete_is_stored(&engine, t0 + GAP),
        "ignored at the gap"
    );
    assert_eq!(engine.get(&1, t0 + GAP), Some("old".into()));

    assert!(engine.deferring_at(t0 + GAP + 1, TTL));
    assert!(
        peer_delete_is_stored(&engine, t0 + GAP + 1),
        "stored past it"
    );
    assert_eq!(engine.get(&1, t0 + GAP + 1), None, "and deletes the value");
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
}

#[test]
fn the_guard_judges_against_the_cap_and_no_further() {
    let t0 = 100_000;
    let engine = holding_an_older_value(t0);
    // A delete stamped at 1,000 has its cap at 601,000.
    assert!(peer_delete_is_stored(&engine, 600_999));
    let engine = holding_an_older_value(t0);
    assert!(!peer_delete_is_stored(&engine, 601_000), "past the cap");
    assert_eq!(engine.get(&1, 601_000), Some("old".into()));
}

#[test]
fn a_collection_after_a_pause_starts_a_hold_and_reports_the_gap() {
    let engine = guarded(50_000);
    let _ = tombstone_of(&engine, 1, key_bytes(1), hlc(1_000, 1), None, 50_000, false);
    assert_eq!(engine.tombstone_count(), 1);

    let t1 = 50_000 + GAP + 1;
    assert_eq!(
        engine.gc_tombstones(false, t1, TTL),
        Some(GAP + 1),
        "the call that enters the hold reports its gap"
    );
    assert!(engine.gc_deferring(), "the paused call defers");
    assert_eq!(engine.last_gc_ms(), t1);
    assert_eq!(engine.pause_hold_until_ms(), t1 + TTL);
    assert_eq!(
        engine.tombstone_count(),
        1,
        "past its ttl deadline at 61,000, kept through the call"
    );
}

#[test]
fn the_hold_keeps_a_tombstone_to_its_cap_and_collects_it_there() {
    let engine = guarded(50_000);
    let _ = tombstone_of(&engine, 1, key_bytes(1), hlc(1_000, 1), None, 50_000, false);
    let t1 = 50_000 + GAP + 1;
    assert!(engine.gc_tombstones(false, t1, TTL).is_some());

    // A pause that ends past the cap (601,000) collects it although the
    // call defers.
    let t2 = 601_000;
    assert_eq!(engine.gc_tombstones(false, t2, TTL), Some(t2 - t1));
    assert_eq!(engine.tombstone_count(), 0, "past its cap");
    assert!(engine.gc_deferring(), "while the call still defers");
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
}

#[test]
fn the_hold_ends_a_ttl_after_the_resume_at_the_next_collection() {
    let engine = guarded(50_000);
    let _ = tombstone_of(&engine, 1, key_bytes(1), hlc(1_000, 1), None, 50_000, false);
    let t1 = 50_000 + GAP + 1;
    assert!(engine.gc_tombstones(false, t1, TTL).is_some());
    let hold_ends = t1 + TTL;

    // Collections one gap apart: not paused, the hold lasts.
    let mut now = t1;
    while now + GAP < hold_ends {
        now += GAP;
        assert_eq!(engine.gc_tombstones(false, now, TTL), None, "now {now}");
        assert!(engine.gc_deferring(), "inside the hold at {now}");
        assert_eq!(engine.tombstone_count(), 1);
    }
    // One millisecond before the hold ends: still deferring.
    assert!(now < hold_ends);
    let last = hold_ends - 1;
    assert!(last - now <= GAP);
    assert_eq!(engine.gc_tombstones(false, last, TTL), None);
    assert!(engine.gc_deferring());
    assert_eq!(
        engine.tombstone_count(),
        1,
        "a tick inside the hold keeps it"
    );

    assert_eq!(engine.gc_tombstones(false, hold_ends, TTL), None);
    assert!(!engine.gc_deferring(), "the hold has ended");
    assert_eq!(engine.tombstone_count(), 0, "and the lapsed tombstone goes");
    assert!(!engine.deferring_at(hold_ends, TTL));
}

/// Between the collection that set the hold and its end, an arrival is
/// judged against the cap by the stored decision and the hold alone: the
/// gap since the last collection is small.
#[test]
fn an_arrival_inside_the_hold_is_judged_against_the_cap_though_no_longer_paused() {
    let engine = guarded(50_000);
    let t1 = 50_000 + GAP + 1;
    assert!(engine.gc_tombstones(false, t1, TTL).is_some());
    let now = t1 + 1_000;
    assert_eq!(engine.last_gc_ms(), t1);
    assert!(!paused_since(engine.last_gc_ms(), now, TTL), "not paused");
    assert!(engine.deferring_at(now, TTL));
    assert!(
        peer_delete_is_stored(&engine, now),
        "judged against the cap"
    );
}

/// Arrivals stay judged against the cap until the first collection at or
/// after the hold's end says otherwise, because the collection stores its
/// decision.
#[test]
fn arrivals_stay_judged_against_the_cap_until_the_first_collection_after_the_hold() {
    let engine = guarded(50_000);
    let t1 = 50_000 + GAP + 1;
    assert!(engine.gc_tombstones(false, t1, TTL).is_some());
    for tick in [t1 + 1_000, t1 + 31_000, t1 + TTL - 1] {
        assert_eq!(engine.gc_tombstones(false, tick, TTL), None, "tick {tick}");
        assert!(engine.gc_deferring(), "told not to defer, held by the hold");
    }
    assert!(engine.deferring_at(t1 + TTL - 1, TTL));
    let after = t1 + TTL + 5;
    assert!(engine.deferring_at(after, TTL), "no collection since");
    assert_eq!(engine.gc_tombstones(false, after, TTL), None);
    assert!(!engine.deferring_at(after, TTL), "the collection ends it");
}

#[test]
fn a_second_pause_extends_the_hold_without_starting_another() {
    let engine = guarded(50_000);
    let t1 = 50_000 + GAP + 1;
    assert_eq!(engine.gc_tombstones(false, t1, TTL), Some(GAP + 1));
    assert_eq!(engine.pause_hold_until_ms(), t1 + TTL);

    let t2 = t1 + GAP + 1;
    assert_eq!(
        engine.gc_tombstones(false, t2, TTL),
        None,
        "inside the hold, not a new one to report"
    );
    assert_eq!(engine.pause_hold_until_ms(), t2 + TTL, "extended");

    let t3 = t2 + 10 * TTL;
    assert_eq!(engine.gc_tombstones(false, t3, TTL), Some(10 * TTL));
    assert_eq!(engine.pause_hold_until_ms(), t3 + TTL, "a fresh hold");
}

#[test]
fn collections_one_period_apart_never_hold() {
    let engine = guarded(1_000);
    let period = gc_period_ms(TTL);
    let mut now = 1_000;
    for _ in 0..1_000 {
        now += period;
        assert_eq!(engine.gc_tombstones(false, now, TTL), None);
        assert!(!engine.gc_deferring());
    }
    // A tick delayed to the gap exactly is on time.
    now += GAP;
    assert_eq!(engine.gc_tombstones(false, now, TTL), None);
    assert_eq!(engine.pause_hold_until_ms(), 0);
}

#[test]
fn a_clock_that_steps_back_never_reads_as_a_pause() {
    let engine = guarded(1_000_000);
    assert!(!engine.deferring_at(1_000, TTL));
    assert_eq!(engine.gc_tombstones(false, 1_000, TTL), None);
    assert_eq!(
        engine.last_gc_ms(),
        1_000,
        "the new reading is the baseline"
    );
    assert_eq!(engine.pause_hold_until_ms(), 0);
    assert!(!engine.gc_deferring());
}

#[test]
fn the_caller_deferral_composes_with_the_guard() {
    let engine = guarded(50_000);
    assert_eq!(engine.gc_tombstones(true, 51_000, TTL), None);
    assert!(engine.gc_deferring(), "the caller's deferral");
    assert_eq!(engine.pause_hold_until_ms(), 0, "and no hold");
    assert_eq!(engine.gc_tombstones(false, 52_000, TTL), None);
    assert!(!engine.gc_deferring());
}

/// An arrival on another thread never sees the collection that detects a
/// pause half published. The collection records its time last, so a reader
/// that sees the new time sees the hold and the deferral flag that go with
/// it. Before that instant the reader sees the old time and reads the gap as
/// a pause; after it the hold. At no instant in between is the node not
/// deferring.
#[test]
fn a_concurrent_arrival_never_sees_the_gap_closed_before_the_hold_is_set() {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    const ROUNDS: u64 = 4_000;
    let engine = guarded(1_000);
    let target = AtomicU64::new(0);
    let stop = AtomicBool::new(false);
    let lapses = AtomicU64::new(0);

    std::thread::scope(|scope| {
        for _ in 0..2 {
            scope.spawn(|| {
                while !stop.load(Ordering::Acquire) {
                    let now = target.load(Ordering::Acquire);
                    if now != 0 && !engine.deferring_at(now, TTL) {
                        lapses.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
        let mut now = 1_000;
        for _ in 0..ROUNDS {
            // The resume: a gap past the pause threshold, then the reader's
            // clock reads the resume instant.
            now += GAP + 1;
            target.store(now, Ordering::Release);
            assert!(engine.gc_tombstones(false, now, TTL).is_some());
            // Ticks one period apart until the hold ends, so the next round
            // starts from a collection that does not defer.
            for _ in 0..TTL / gc_period_ms(TTL) {
                now += gc_period_ms(TTL);
                assert_eq!(engine.gc_tombstones(false, now, TTL), None);
            }
            assert!(!engine.gc_deferring(), "the hold has ended at {now}");
            if lapses.load(Ordering::Relaxed) > 0 {
                break;
            }
        }
        stop.store(true, Ordering::Release);
    });

    assert_eq!(
        lapses.load(Ordering::Relaxed),
        0,
        "a reader saw the new collection time without the hold"
    );
}

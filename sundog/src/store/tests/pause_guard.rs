//! The pause guard at the shard: a node whose collections are more than
//! twice the gc period apart defers for `tombstone_ttl`, judging a delete
//! from a peer against the cap and keeping its tombstones to it. Every test
//! reads a manual clock and calls `gc_tombstones` itself, so none depends on
//! real time or on the gc task.

use std::sync::atomic::{AtomicU64, Ordering};

use super::tombstone_arrival::{START_MS, clocked, delete_record, manual_clock};
use super::*;

const TTL: Duration = Duration::from_secs(60);
const TTL_MS: u64 = 60_000;
/// Twice the 15 s gc period of a 60 s retention.
const GAP_MS: u64 = 30_000;

/// A shard on `clock` that has collected once, at the clock's reading.
async fn collected(clock: &Arc<AtomicU64>, ttl: Duration) -> Shard<u32, String> {
    let s = clocked(clock, ttl);
    ShardOps::gc_tombstones(&s, false).await;
    s
}

/// Gives key `key` the value `"old"` at a stamp older than any delete here.
async fn hold_old(s: &Shard<u32, String>, key: u32) {
    ShardOps::apply_remote(s, wire_record(key, "old", hlc(1_000, 3))).await;
    assert_eq!(s.get(&key).await.as_deref(), Some("old"));
}

/// A delete of `key` stamped twice the retention before the start: lapsed
/// past its `ttl` deadline, inside the cap.
fn lapsed_delete(key: u32, ttl_ms: u64) -> WireRecord {
    delete_record(key, START_MS - 2 * ttl_ms)
}

/// Whether the lapsed delete of `key` from a peer deleted the old value.
async fn peer_delete_lands(s: &Shard<u32, String>, key: u32, ttl_ms: u64) -> bool {
    hold_old(s, key).await;
    ShardOps::apply_remote(s, lapsed_delete(key, ttl_ms)).await;
    s.get(&key).await.is_none()
}

#[tokio::test]
async fn only_a_replicated_or_distributed_shard_guards_pauses() {
    let owners = NonZeroU8::new(2).expect("nonzero");
    for (mode, guarded) in [
        (Mode::Local, false),
        (Mode::Invalidation, false),
        (Mode::Replicated, true),
        (Mode::Distributed { owners }, true),
    ] {
        let clock = manual_clock();
        let s = Shard::<u32, String>::new(
            SmolStr::new("guard"),
            mode,
            NodeId::from(1),
            10_000,
            None,
            None,
        )
        .with_tombstone_ttl(TTL);
        let reader = Arc::clone(&clock);
        let s = s.with_clock(Arc::new(move || reader.load(Ordering::SeqCst)));
        ShardOps::gc_tombstones(&s, false).await;
        clock.store(START_MS + GAP_MS + 1, Ordering::SeqCst);
        assert_eq!(
            s.engine.deferring_at(START_MS + GAP_MS + 1, TTL_MS),
            guarded,
            "{mode:?}"
        );
        assert_eq!(s.engine.last_gc_ms() != 0, guarded, "{mode:?}");
    }
}

#[tokio::test]
async fn every_engine_rebuild_keeps_the_pause_guard() {
    let clock = manual_clock();
    let rebuilt = [
        clocked(&clock, TTL).with_capacity_hint(16),
        clocked(&clock, TTL).with_weigher(|_, _| 1),
        clocked(&clock, TTL).with_max_resident_bytes(1 << 30),
    ];
    for (index, s) in rebuilt.iter().enumerate() {
        ShardOps::gc_tombstones(s, false).await;
        assert!(
            !s.engine.deferring_at(START_MS + GAP_MS, TTL_MS),
            "rebuild {index}: at the gap"
        );
        assert!(
            s.engine.deferring_at(START_MS + GAP_MS + 1, TTL_MS),
            "rebuild {index}: past it"
        );
    }
    let local = Shard::<u32, String>::new(
        SmolStr::new("local"),
        Mode::Local,
        NodeId::from(1),
        10_000,
        None,
        None,
    )
    .with_capacity_hint(16);
    ShardOps::gc_tombstones(&local, false).await;
    assert!(!local.engine.deferring_at(u64::MAX, TTL_MS));
}

/// A delete lapsed past its `ttl` is ignored a gap after the last collection
/// and applied a millisecond later, for every retention: the gap is twice the
/// shard's own gc period, and the shard reads its retention at call time.
#[tokio::test]
async fn the_gap_is_twice_the_gc_period_of_the_shards_own_retention() {
    for (ttl_secs, gap_ms) in [(60, 30_000), (8, 4_000), (2, 2_000), (1, 2_000)] {
        let ttl = Duration::from_secs(ttl_secs);
        let ttl_ms = ttl_secs * 1_000;
        for (offset, lands) in [(gap_ms, false), (gap_ms + 1, true)] {
            let clock = manual_clock();
            let s = collected(&clock, ttl).await;
            clock.store(START_MS + offset, Ordering::SeqCst);
            // Stamped so that it is lapsed past its ttl at this reading.
            let key = 1;
            hold_old(&s, key).await;
            ShardOps::apply_remote(&s, delete_record(key, START_MS + offset - 2 * ttl_ms)).await;
            assert_eq!(
                s.get(&key).await.is_none(),
                lands,
                "ttl {ttl_secs} s, {offset} ms after the last collection"
            );
            assert_eq!(s.tombstone_count(), u64::from(lands));
        }
    }
}

#[tokio::test]
async fn a_shard_with_no_collection_yet_is_never_paused() {
    let clock = manual_clock();
    let s = clocked(&clock, TTL);
    clock.store(START_MS + 100 * TTL_MS, Ordering::SeqCst);
    assert_eq!(s.engine.last_gc_ms(), 0);
    // The start deferral covers it: judged against the cap, not the guard.
    assert!(s.engine.deferring_at(START_MS + 100 * TTL_MS, TTL_MS));
    assert!(s.engine.gc_deferring());
    ShardOps::gc_tombstones(&s, false).await;
    assert!(
        !s.engine.deferring_at(START_MS + 100 * TTL_MS, TTL_MS),
        "the first collection is a baseline, not a pause"
    );
    assert_eq!(s.engine.pause_hold_until_ms(), 0);
}

/// A tombstone past its ttl but inside its cap survives every collection of
/// the hold and goes with the first one after it; arrivals in between are
/// judged against the cap.
#[tokio::test]
async fn the_hold_keeps_tombstones_and_judges_arrivals_against_the_cap_until_a_ttl_after_the_resume()
 {
    let clock = manual_clock();
    let s = collected(&clock, TTL).await;
    // Stamped 59 s before the start: collectible a second in.
    ShardOps::apply_remote(&s, delete_record(1, START_MS - 59_000)).await;
    assert_eq!(s.tombstone_count(), 1);

    let resume = START_MS + GAP_MS + 1;
    clock.store(resume, Ordering::SeqCst);
    ShardOps::gc_tombstones(&s, false).await;
    assert_eq!(s.engine.pause_hold_until_ms(), resume + TTL_MS);
    assert_eq!(
        s.tombstone_count(),
        1,
        "past its ttl, held through the pause"
    );

    // Ticks one gap apart through the hold: a delete lapsed past its ttl
    // lands every time.
    let mut now = resume;
    let mut key = 10;
    while now + GAP_MS < resume + TTL_MS {
        now += GAP_MS;
        clock.store(now, Ordering::SeqCst);
        ShardOps::gc_tombstones(&s, false).await;
        assert!(s.tombstone_count() >= 1, "kept at {now}");
        hold_old(&s, key).await;
        ShardOps::apply_remote(&s, delete_record(key, now - 2 * TTL_MS)).await;
        assert_eq!(s.get(&key).await, None, "judged against the cap at {now}");
        key += 1;
    }

    clock.store(resume + TTL_MS, Ordering::SeqCst);
    ShardOps::gc_tombstones(&s, false).await;
    assert_eq!(s.engine.pause_hold_until_ms(), resume + TTL_MS);
    assert!(!s.engine.gc_deferring(), "the hold has ended");
    assert_eq!(s.tombstone_count(), 0, "everything past its ttl goes");

    hold_old(&s, 99).await;
    ShardOps::apply_remote(&s, delete_record(99, resume + TTL_MS - 2 * TTL_MS)).await;
    assert_eq!(
        s.get(&99).await.as_deref(),
        Some("old"),
        "ignored again once the hold is over"
    );
}

#[tokio::test]
async fn a_second_pause_inside_the_hold_extends_it() {
    let clock = manual_clock();
    let s = collected(&clock, TTL).await;
    ShardOps::apply_remote(&s, delete_record(1, START_MS - 59_000)).await;

    let first = START_MS + GAP_MS + 1;
    clock.store(first, Ordering::SeqCst);
    ShardOps::gc_tombstones(&s, false).await;
    let second = first + GAP_MS + 1;
    clock.store(second, Ordering::SeqCst);
    ShardOps::gc_tombstones(&s, false).await;
    assert_eq!(s.engine.pause_hold_until_ms(), second + TTL_MS);

    // The first hold would have ended here; the extended one has not.
    let mut now = second;
    while now + GAP_MS < second + TTL_MS {
        now += GAP_MS;
        clock.store(now, Ordering::SeqCst);
        ShardOps::gc_tombstones(&s, false).await;
    }
    assert!(now >= first + TTL_MS);
    assert!(now < second + TTL_MS);
    assert_eq!(s.tombstone_count(), 1, "still held by the second pause");
    assert!(s.engine.gc_deferring());

    clock.store(second + TTL_MS, Ordering::SeqCst);
    ShardOps::gc_tombstones(&s, false).await;
    assert_eq!(s.tombstone_count(), 0);
}

#[tokio::test]
async fn a_clock_that_steps_back_does_not_pause_the_node() {
    let clock = manual_clock();
    let s = collected(&clock, TTL).await;
    let before = START_MS - 100 * TTL_MS;
    clock.store(before, Ordering::SeqCst);
    assert!(!s.engine.deferring_at(before, TTL_MS));
    ShardOps::gc_tombstones(&s, false).await;
    assert_eq!(s.engine.pause_hold_until_ms(), 0, "no hold");
    assert!(!s.engine.gc_deferring());
    assert_eq!(
        s.engine.last_gc_ms(),
        before,
        "the new reading is the baseline"
    );

    hold_old(&s, 1).await;
    ShardOps::apply_remote(&s, delete_record(1, before - 2 * TTL_MS)).await;
    assert_eq!(
        s.get(&1).await.as_deref(),
        Some("old"),
        "a lapsed delete is ignored"
    );
}

#[tokio::test]
async fn collections_a_period_apart_never_hold() {
    let clock = manual_clock();
    let s = collected(&clock, TTL).await;
    let mut now = START_MS;
    for _ in 0..200 {
        now += 15_000;
        clock.store(now, Ordering::SeqCst);
        ShardOps::gc_tombstones(&s, false).await;
        assert_eq!(s.engine.pause_hold_until_ms(), 0, "at {now}");
        assert!(!s.engine.gc_deferring(), "at {now}");
    }
    assert!(!peer_delete_lands(&s, 1, TTL_MS).await);
}

#[tokio::test]
async fn an_absent_member_deferral_composes_with_the_guard() {
    let clock = manual_clock();
    let s = collected(&clock, TTL).await;
    clock.store(START_MS + 1_000, Ordering::SeqCst);
    ShardOps::gc_tombstones(&s, true).await;
    assert!(s.engine.gc_deferring());
    assert_eq!(
        s.engine.pause_hold_until_ms(),
        0,
        "a plain deferral sets no hold"
    );
    clock.store(START_MS + 2_000, Ordering::SeqCst);
    ShardOps::gc_tombstones(&s, false).await;
    assert!(!s.engine.gc_deferring());
}

/// Tests that read the log, which the `sim` build's cluster test support
/// does not provide a subscriber for.
#[cfg(not(feature = "sim"))]
mod logged {
    use tracing_subscriber::layer::SubscriberExt as _;

    use super::super::tombstone_arrival::logged::Warnings;
    use super::*;

    #[tokio::test]
    async fn a_pause_hold_is_logged_once_with_its_gap_and_an_extension_is_not() {
        let warnings = Warnings::default();
        let _log = crate::cluster::test_support::scoped_subscriber(
            tracing_subscriber::registry().with(warnings.clone()),
        );
        let clock = manual_clock();
        let s = collected(&clock, TTL).await;
        let mut now = START_MS;
        for _ in 0..4 {
            now += 15_000;
            clock.store(now, Ordering::SeqCst);
            ShardOps::gc_tombstones(&s, false).await;
        }
        assert!(warnings.taken().is_empty(), "no pause, no warning");

        now += GAP_MS + 5;
        clock.store(now, Ordering::SeqCst);
        ShardOps::gc_tombstones(&s, false).await;
        let logged = warnings.taken();
        assert_eq!(logged.len(), 1, "{logged:?}");
        assert!(
            logged[0].contains(&format!("{} ms", GAP_MS + 5)),
            "names the gap: {logged:?}"
        );

        now += GAP_MS + 1;
        clock.store(now, Ordering::SeqCst);
        ShardOps::gc_tombstones(&s, false).await;
        assert!(
            warnings.taken().is_empty(),
            "a pause inside the hold extends it and does not warn again"
        );

        now += 2 * TTL_MS;
        clock.store(now, Ordering::SeqCst);
        ShardOps::gc_tombstones(&s, false).await;
        let logged = warnings.taken();
        assert_eq!(logged.len(), 1, "a pause after the hold ends: {logged:?}");
        assert!(
            logged[0].contains(&format!("{} ms", 2 * TTL_MS)),
            "{logged:?}"
        );
    }
}

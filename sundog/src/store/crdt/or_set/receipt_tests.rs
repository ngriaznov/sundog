//! The fold receipt's stamp and lifetime: a receipt lives `receipt_ttl_ms`
//! from the fold, however long after the retirement the fold happens.

use super::*;
use crate::store::test_support::wid;
use proptest::prelude::*;

const RETIRE_AFTER_MS: u64 = 100;

fn bounds() -> CompactionBounds {
    CompactionBounds::three_bounds(RETIRE_AFTER_MS)
}

/// A set where `w` added `x` and then removed it, with `w` retired at
/// `retired_at`.
fn retired_at(w: WriterId, retired_at: u64) -> OrSet<String> {
    let added = OrSet::add(w, 0, "x".to_string());
    let removed = OrSet::remove(&added, &"x".to_string());
    added
        .merge(&removed)
        .compact(
            retired_at,
            bounds(),
            &|writer| writer == w,
            Quiescence::Churning,
        )
        .expect("stage one fires")
}

/// `set` folded at `now`, with every peer quiet.
fn folded_at(set: &OrSet<String>, now: u64) -> OrSet<String> {
    set.compact(now, bounds(), &|_| false, Quiescence::Settled)
        .expect("stage two fires")
}

/// A fold that membership churn delays past the receipt lifetime still
/// vouches for the writer: a replica that never saw the remove, retired the
/// writer and never folded it, merges the folded copy without bringing the
/// removed element back.
#[test]
fn a_fold_delayed_past_the_receipt_ttl_still_vouches_for_an_unfolded_replica() {
    let w = wid(1, 0);
    let added = OrSet::add(w, 0, "x".to_string());
    // The stale replica holds the element and retires the writer.
    let stale = added
        .compact(1_000, bounds(), &|writer| writer == w, Quiescence::Churning)
        .expect("stage one fires");
    assert!(stale.contains(&"x".to_string()));
    // The folded replica saw the remove.
    let folded = folded_at(&retired_at(w, 1_000), 10_000);
    // The merge apply on the folded side settles its receipt.
    let settled = folded
        .prune_receipts(10_001, bounds().receipt_ttl_ms)
        .unwrap_or_else(|| folded.clone());
    assert!(
        settled.folded_at.contains_key(&w),
        "a receipt written at 10_000 is far from its end at 10_001"
    );

    for merged in [stale.merge(&settled), settled.merge(&stale)] {
        assert!(
            !merged.contains(&"x".to_string()),
            "the removed element stays absent: {merged:?}"
        );
        assert!(
            merged.retired.is_empty(),
            "the receipt drops the retired entry"
        );
    }
}

#[test]
fn stage_two_stamps_the_receipt_with_the_fold_instant_not_the_retirement_instant() {
    let w = wid(1, 0);
    let retired = retired_at(w, 1_000);
    for fold_at in [1_201, 7_777] {
        assert_eq!(
            folded_at(&retired, fold_at).folded_at.get(&w),
            Some(&fold_at),
            "the receipt names the instant of the fold, not the retirement at 1_000"
        );
    }
}

#[test]
fn a_receipt_lives_receipt_ttl_after_the_fold_even_when_the_fold_is_late() {
    let w = wid(1, 0);
    let folded = folded_at(&retired_at(w, 1_000), 10_000);
    let ttl = bounds().receipt_ttl_ms;
    assert!(
        folded.prune_receipts(10_000 + ttl, ttl).is_none(),
        "exactly the lifetime after the fold: kept"
    );
    let pruned = folded
        .prune_receipts(10_000 + ttl + 1, ttl)
        .expect("one millisecond past the lifetime: dropped");
    assert!(pruned.folded_at.is_empty());
    assert!(
        !pruned.contains(&"x".to_string()),
        "dropping a receipt keeps membership"
    );
}

#[test]
fn stage_two_never_lowers_an_existing_receipt_stamp() {
    let w = wid(1, 0);
    let mut retired = retired_at(w, 1_000);
    retired.folded_at.insert(w, 1_400);
    assert_eq!(folded_at(&retired, 1_201).folded_at.get(&w), Some(&1_400));
    let mut retired = retired_at(w, 1_000);
    retired.folded_at.insert(w, 1_100);
    assert_eq!(folded_at(&retired, 1_201).folded_at.get(&w), Some(&1_201));
}

#[test]
fn a_receipt_stamped_more_than_a_lifetime_ahead_of_now_expires() {
    let (kept, ahead) = (wid(1, 0), wid(2, 0));
    let ttl = 300;
    let mut set = OrSet::add(wid(3, 0), 0, "y".to_string());
    set.folded_at.insert(kept, 5_000 + ttl);
    set.folded_at.insert(ahead, 5_000 + ttl + 1);
    let pruned = set
        .prune_receipts(5_000, ttl)
        .expect("the stamp past a lifetime ahead is dropped");
    assert_eq!(
        pruned.folded_at.keys().copied().collect::<Vec<_>>(),
        vec![kept]
    );
    assert!(
        pruned.prune_receipts(5_000, ttl).is_none(),
        "a lead of exactly one lifetime is kept"
    );
}

#[test]
fn replicas_folding_at_different_instants_merge_to_identical_bytes_stamped_with_the_later() {
    let w = wid(1, 0);
    let retired = retired_at(w, 1_000);
    let early = folded_at(&retired, 1_201);
    let late = folded_at(&retired, 1_290);
    assert_ne!(early, late, "different stamps are different bytes");
    let forward = early.merge(&late);
    let backward = late.merge(&early);
    assert_eq!(forward, backward);
    assert_eq!(forward.encode().unwrap(), backward.encode().unwrap());
    assert_eq!(forward.folded_at.get(&w), Some(&1_290));
    assert!(!forward.contains(&"x".to_string()));
}

proptest! {
    /// A sweep never writes a receipt that the same instant's settle drops.
    #[test]
    fn a_receipt_the_sweep_wrote_survives_the_settle_at_the_same_instant(
        retired_ms in 0u64..50_000,
        late_by in 201u64..20_000,
        ttl_bounds in 1u64..8,
    ) {
        let w = wid(1, 0);
        let b = CompactionBounds {
            retire_after_ms: RETIRE_AFTER_MS,
            receipt_ttl_ms: RETIRE_AFTER_MS * ttl_bounds,
        };
        let retired = OrSet::add(w, 0, "x".to_string())
            .compact(retired_ms, b, &|x| x == w, Quiescence::Churning)
            .expect("stage one fires");
        let now = retired_ms + late_by;
        let folded = retired
            .compact(now, b, &|_| false, Quiescence::Settled)
            .expect("stage two fires");
        prop_assert!(folded.folded_at.contains_key(&w));
        prop_assert!(folded.prune_receipts(now, b.receipt_ttl_ms).is_none());
    }
}

//! The fold receipt's stamp and lifetime: a receipt lives `receipt_ttl_ms`
//! from the fold, however long after the retirement the fold happens.

use super::*;
use crate::store::test_support::wid;
use proptest::prelude::*;

const RETIRE_AFTER_MS: u64 = 1_000;

fn bounds() -> CompactionBounds {
    CompactionBounds::three_bounds(RETIRE_AFTER_MS)
}

/// A counter holding `total` from `w` and retired at `retired_at`.
fn retired_at(w: WriterId, total: u64, retired_at: u64) -> PnCounter {
    PnCounter::local_delta(w, total)
        .compact(
            retired_at,
            &|writer| writer == w,
            Quiescence::Churning,
            bounds(),
        )
        .expect("stage one fires")
}

/// `counter` folded at `now`, with every peer quiet.
fn folded_at(counter: &PnCounter, now: u64) -> PnCounter {
    counter
        .compact(now, &|_| false, Quiescence::Settled, bounds())
        .expect("stage two fires")
}

/// A fold that membership churn delays past the receipt lifetime still
/// vouches for the writer: a replica that retired it and never folded it
/// merges the folded copy without counting the writer twice.
#[test]
fn a_fold_delayed_past_the_receipt_ttl_still_vouches_for_an_unfolded_replica() {
    let w = wid(1, 0);
    let unfolded = retired_at(w, 10, 0);
    let folded = folded_at(&unfolded, 10_000);
    // The merge apply on the folded side settles its receipt.
    let settled = folded
        .prune_receipts(10_001, bounds().receipt_ttl_ms)
        .unwrap_or_else(|| folded.clone());
    assert!(
        settled.folded_at.contains_key(&w),
        "a receipt written at 10_000 is far from its end at 10_001"
    );

    for merged in [unfolded.merge(&settled), settled.merge(&unfolded)] {
        assert_eq!(merged.value(), 10, "the writer counts once: {merged:?}");
        assert!(
            merged.retired.is_empty(),
            "the receipt drops the retired entry"
        );
        let refolded = merged
            .compact(10_001, &|_| false, Quiescence::Settled, bounds())
            .unwrap_or_else(|| merged.clone());
        assert_eq!(refolded.value(), 10, "and still counts once after a sweep");
    }
}

#[test]
fn stage_two_stamps_the_receipt_with_the_fold_instant_not_the_retirement_instant() {
    let w = wid(1, 0);
    let retired = retired_at(w, 8, 0);
    for fold_at in [2_001, 7_777] {
        let folded = folded_at(&retired, fold_at);
        assert_eq!(
            folded.folded_at.get(&w),
            Some(&fold_at),
            "the receipt names the instant of the fold, not the retirement at 0"
        );
    }
}

#[test]
fn a_receipt_lives_receipt_ttl_after_the_fold_even_when_the_fold_is_late() {
    let w = wid(1, 0);
    let folded = folded_at(&retired_at(w, 8, 0), 10_000);
    let ttl = bounds().receipt_ttl_ms;
    assert!(
        folded.prune_receipts(10_000 + ttl, ttl).is_none(),
        "exactly the lifetime after the fold: kept"
    );
    let pruned = folded
        .prune_receipts(10_000 + ttl + 1, ttl)
        .expect("one millisecond past the lifetime: dropped");
    assert!(pruned.folded_at.is_empty());
    assert_eq!(
        pruned.folded_p, 8,
        "dropping a receipt keeps the folded total"
    );
    assert_eq!(pruned.value(), 8);
}

/// An existing receipt for the writer stays as late as it was: a stamp is
/// never lowered by a fold from a clock that runs behind it.
#[test]
fn stage_two_never_lowers_an_existing_receipt_stamp() {
    let w = wid(1, 0);
    let mut retired = retired_at(w, 8, 0);
    retired.folded_at.insert(w, 7_000);
    let folded = folded_at(&retired, 5_000);
    assert_eq!(folded.folded_at.get(&w), Some(&7_000));
    let mut retired = retired_at(w, 8, 0);
    retired.folded_at.insert(w, 3_000);
    let folded = folded_at(&retired, 5_000);
    assert_eq!(folded.folded_at.get(&w), Some(&5_000));
}

/// A receipt stamped further ahead of the clock than a whole lifetime comes
/// from a clock that is badly wrong; it expires instead of outliving every
/// honest replica's.
#[test]
fn a_receipt_stamped_more_than_a_lifetime_ahead_of_now_expires() {
    let (kept, ahead) = (wid(1, 0), wid(2, 0));
    let ttl = 3_000;
    let mut counter = PnCounter::local_delta(wid(3, 0), 4);
    counter.folded_at.insert(kept, 5_000 + ttl);
    counter.folded_at.insert(ahead, 5_000 + ttl + 1);
    let pruned = counter
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

/// Two replicas that fold one retired writer at different instants carry
/// different stamps and the same total; merged in either order they are the
/// same bytes, stamped with the later instant.
#[test]
fn replicas_folding_at_different_instants_merge_to_identical_bytes_stamped_with_the_later() {
    let w = wid(1, 0);
    let retired = retired_at(w, 8, 0);
    let early = folded_at(&retired, 2_001);
    let late = folded_at(&retired, 2_900);
    assert_ne!(early, late, "different stamps are different bytes");
    let forward = early.merge(&late);
    let backward = late.merge(&early);
    assert_eq!(forward, backward);
    assert_eq!(forward.encode().unwrap(), backward.encode().unwrap());
    assert_eq!(forward.folded_at.get(&w), Some(&2_900));
    assert_eq!(forward.value(), 8);
}

proptest! {
    /// A sweep never writes a receipt that the same instant's settle drops.
    #[test]
    fn a_receipt_the_sweep_wrote_survives_the_settle_at_the_same_instant(
        value in 1u64..1_000,
        retired_ms in 0u64..50_000,
        late_by in 2_001u64..200_000,
        ttl_bounds in 1u64..8,
    ) {
        let w = wid(1, 0);
        let b = CompactionBounds {
            retire_after_ms: RETIRE_AFTER_MS,
            receipt_ttl_ms: RETIRE_AFTER_MS * ttl_bounds,
        };
        let retired = PnCounter::local_delta(w, value)
            .compact(retired_ms, &|x| x == w, Quiescence::Churning, b)
            .expect("stage one fires");
        let now = retired_ms + late_by;
        let folded = retired
            .compact(now, &|_| false, Quiescence::Settled, b)
            .expect("stage two fires");
        prop_assert!(folded.folded_at.contains_key(&w));
        prop_assert!(folded.prune_receipts(now, b.receipt_ttl_ms).is_none());
    }
}

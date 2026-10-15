//! Kani proofs over the `Hlc`, expiry, hash-slot, merge and stripe-shrink arithmetic.

use super::expiry::{
    LONG_TTL, MAX_INLINE_TTL_MS, MAX_STAMP_LEAD_MS, MAX_TTI_MS, NEVER_EXPIRES, clamp_tti_ms,
    drops_lapsed_tombstone, encode_expiry, idle_elapsed_ms, tombstone_anchor_ms,
    tombstone_deadlines, touch_stamp,
};
use super::housekeeping::should_shrink_stripe;
use super::keys::{digest_slot, part_index_from_hash, stripe_index_from_hash};
use super::merge::{MergedVersion, merge_decision, mint_stamp};
use super::*;

fn any_hlc() -> Hlc {
    Hlc {
        wall_ms: kani::any(),
        logical: kani::any(),
        node: NodeId::from(kani::any::<u64>()),
    }
}

/// A minted stamp is at least both inputs on `wall_ms` and `logical`,
/// and strictly above both everywhere short of the maximum stamp.
#[kani::proof]
fn a_minted_stamp_dominates_both_inputs() {
    let (sv, ver) = (any_hlc(), any_hlc());
    let node = NodeId::from(kani::any::<u64>());
    let minted = mint_stamp(sv, ver, node);
    assert_eq!(minted.node, node);
    for input in [sv, ver] {
        assert!((minted.wall_ms, minted.logical) >= (input.wall_ms, input.logical));
    }
    let at_max = sv.wall_ms.max(ver.wall_ms) == u64::MAX && sv.logical.max(ver.logical) == u32::MAX;
    if !at_max {
        assert!(minted > sv && minted > ver);
    }
}

/// A merge never stores a version below the stored one, stores a new
/// version strictly above it off the mint arm, and no-ops only when the
/// merged bytes are what is already stored.
#[kani::proof]
fn a_merge_never_moves_the_stored_version_back() {
    let (sv, ver) = (any_hlc(), any_hlc());
    let (is_stored, is_incoming): (bool, bool) = (kani::any(), kani::any());
    let node = NodeId::from(kani::any::<u64>());
    let minted = mint_stamp(sv, ver, node);
    match merge_decision(sv, ver, is_stored, is_incoming, || node) {
        MergedVersion::NoOp => {
            assert!(is_stored);
        }
        MergedVersion::Store(v) if v == minted => {
            assert!((v.wall_ms, v.logical) >= (sv.wall_ms, sv.logical));
        }
        MergedVersion::Store(v) => {
            assert!(v > sv);
        }
    }
}

/// Both nodes of a collision, each holding the other's write as
/// incoming, end at the same version: the one node keeps or stores what
/// the other does. `stored_eq` says the two sides' content (bytes and
/// deadline) match, which the merged content can only equal both of when it
/// holds. Both stored sides are readable here; a stored side that reads as
/// absent is `an_absent_stored_collision_converges_in_two_exchanges`.
#[kani::proof]
fn a_collision_converges_in_either_order() {
    let (sv, ver) = (any_hlc(), any_hlc());
    let (is_stored, is_incoming, stored_eq): (bool, bool, bool) =
        (kani::any(), kani::any(), kani::any());
    kani::assume(!(is_stored && is_incoming) || stored_eq);
    kani::assume(!stored_eq || is_stored == is_incoming);
    let node = NodeId::from(kani::any::<u64>());
    let settle = |own: Hlc, other: Hlc, is_own: bool, is_other: bool| match merge_decision(
        own,
        other,
        is_own,
        is_other,
        || node,
    ) {
        MergedVersion::NoOp => own,
        MergedVersion::Store(v) => v,
    };
    assert_eq!(
        settle(sv, ver, is_stored, is_incoming),
        settle(ver, sv, is_incoming, is_stored)
    );
}

/// A collision whose stored side reads as absent on one node, so its
/// `is_stored` is false while both nodes hold equal content, ends at one
/// version within a second exchange: the first exchange leaves the absent
/// side at the other's version or above it, and the second adopts the
/// greater of the two on both nodes.
#[kani::proof]
fn an_absent_stored_collision_converges_in_two_exchanges() {
    let (vx, vy) = (any_hlc(), any_hlc());
    let node = NodeId::from(kani::any::<u64>());
    let settle = |own: Hlc, other: Hlc, is_own: bool, is_other: bool| match merge_decision(
        own,
        other,
        is_own,
        is_other,
        || node,
    ) {
        MergedVersion::NoOp => own,
        MergedVersion::Store(v) => v,
    };
    // Node X holds `vx` as absent; node Y holds `vy` as live.
    let x1 = settle(vx, vy, false, true);
    let y1 = settle(vy, vx, true, true);
    assert_eq!(settle(x1, y1, true, true), settle(y1, x1, true, true));
}

/// Every deadline encodes without panicking and reads back as itself:
/// `None` as never, a deadline inside the inline range as an exact
/// delta, anything else through the side table.
#[kani::proof]
fn expiry_encodes_and_decodes_every_deadline() {
    let wall_ms: u64 = kani::any();
    let expires_at_ms: Option<u64> = kani::any();
    let (expiry, side) = encode_expiry(wall_ms, expires_at_ms);
    match expires_at_ms {
        None => {
            assert_eq!(expiry, NEVER_EXPIRES);
            assert!(side.is_none());
        }
        Some(exp) if exp >= wall_ms && exp - wall_ms <= MAX_INLINE_TTL_MS => {
            assert_ne!(expiry, NEVER_EXPIRES);
            assert_ne!(expiry, LONG_TTL);
            assert!(side.is_none());
            assert_eq!(wall_ms + u64::from(expiry), exp);
        }
        Some(exp) => {
            assert_eq!(expiry, LONG_TTL);
            assert_eq!(side, Some(exp));
        }
    }
}

/// The u32 touch stamp recovers any idle gap up to the clamped
/// time-to-idle ceiling, across a rollover of the stamp.
#[kani::proof]
fn idle_elapsed_recovers_any_gap_under_the_stamp_span() {
    let earlier: u64 = kani::any();
    let now: u64 = kani::any();
    kani::assume(now >= earlier);
    kani::assume(now - earlier <= MAX_TTI_MS);
    assert_eq!(idle_elapsed_ms(now, touch_stamp(earlier)), now - earlier);
}

/// A stamp ahead of the observer's `now` by up to the lead window reads as
/// zero idle, across a rollover of the stamp.
#[kani::proof]
fn idle_elapsed_is_zero_for_a_stamp_ahead_of_now() {
    let earlier: u64 = kani::any();
    let now: u64 = kani::any();
    kani::assume(earlier > now && earlier - now <= MAX_STAMP_LEAD_MS);
    assert_eq!(idle_elapsed_ms(now, touch_stamp(earlier)), 0);
}

/// No stamp and no `now` ever reads as more idle than the ceiling a
/// configured time-to-idle is clamped to.
#[kani::proof]
fn idle_elapsed_never_exceeds_the_clamped_tti_ceiling() {
    let now: u64 = kani::any();
    let stamp: u32 = kani::any();
    assert!(idle_elapsed_ms(now, stamp) <= MAX_TTI_MS);
}

/// A clamped time-to-idle never exceeds the stamp's span or the value asked for.
#[kani::proof]
fn clamped_tti_stays_within_the_stamp_span() {
    let tti_ms: u64 = kani::any();
    let clamped = clamp_tti_ms(tti_ms);
    assert!(clamped <= MAX_TTI_MS);
    assert!(clamped <= tti_ms);
    assert!(clamped + MAX_STAMP_LEAD_MS <= u64::from(u32::MAX));
}

/// Any hash lands inside the bucket table, the part table and the flat digest table.
#[kani::proof]
fn hash_indexes_stay_within_the_bucket_and_part_tables() {
    let hash: u64 = kani::any();
    let bucket = stripe_index_from_hash(hash);
    let part = part_index_from_hash(hash);
    assert!(bucket < BUCKET_COUNT);
    assert!(part < PART_COUNT);
    assert!(digest_slot(bucket, part) < BUCKET_COUNT * PART_COUNT);
}

/// A stripe shrinks only at or under an eighth of its allocation, never
/// with nothing allocated and never while holding more than it has room for.
#[kani::proof]
fn shrink_fires_only_at_an_eighth_of_the_allocation() {
    let len: usize = kani::any();
    let capacity: usize = kani::any();
    let shrink = should_shrink_stripe(len, capacity);
    if shrink {
        assert!(capacity > 0);
        assert!(len.checked_mul(8).is_some_and(|eight| eight <= capacity));
    }
    if capacity == 0 || len > capacity / 8 {
        assert!(!shrink);
    }
}

/// A tombstone's anchor is never beyond the receiving clock, and its
/// deadlines are never before the anchor, whatever the stamps and
/// retentions.
#[kani::proof]
fn a_tombstone_anchor_is_never_ahead_of_now_and_its_deadlines_never_before_it() {
    let (expired_at_ms, ver_wall_ms, now_ms): (Option<u64>, u64, u64) =
        (kani::any(), kani::any(), kani::any());
    let anchor = tombstone_anchor_ms(expired_at_ms, ver_wall_ms, now_ms);
    assert!(anchor <= now_ms);
    let (ttl_ms, max_ttl_ms): (u64, u64) = (kani::any(), kani::any());
    let (ttl_deadline, max_deadline) = tombstone_deadlines(anchor, ttl_ms, max_ttl_ms);
    assert!(ttl_deadline >= anchor && max_deadline >= anchor);
}

/// This node's own tombstone is never dropped, and a peer's is dropped
/// exactly when the next collection would collect it.
#[kani::proof]
fn only_a_peer_tombstone_that_the_next_collection_would_collect_is_dropped() {
    let deadlines: (u64, u64) = (kani::any(), kani::any());
    let (now_ms, deferring): (u64, bool) = (kani::any(), kani::any());
    assert!(!drops_lapsed_tombstone(false, deadlines, now_ms, deferring));
    let horizon = if deferring { deadlines.1 } else { deadlines.0 };
    assert_eq!(
        drops_lapsed_tombstone(true, deadlines, now_ms, deferring),
        now_ms >= horizon
    );
}

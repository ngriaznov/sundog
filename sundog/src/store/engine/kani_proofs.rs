//! Kani proofs over the `Hlc`, expiry, hash-slot, merge and stripe-shrink arithmetic.

use super::expiry::{
    LONG_TTL, MAX_INLINE_TTL_MS, NEVER_EXPIRES, clamp_tti_ms, encode_expiry, idle_elapsed_ms,
    touch_stamp,
};
use super::keys::{digest_slot, part_index_from_hash, stripe_index_from_hash};
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
/// the other does. `stored_eq` says the two sides' bytes match, which
/// the merged bytes can only equal both of when it holds.
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
    kani::assume(now - earlier <= MAX_INLINE_TTL_MS);
    assert_eq!(idle_elapsed_ms(now, touch_stamp(earlier)), now - earlier);
}

/// A clamped time-to-idle never exceeds the stamp's span or the value asked for.
#[kani::proof]
fn clamped_tti_stays_within_the_stamp_span() {
    let tti_ms: u64 = kani::any();
    let clamped = clamp_tti_ms(tti_ms);
    assert!(clamped <= MAX_INLINE_TTL_MS);
    assert!(clamped <= tti_ms);
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

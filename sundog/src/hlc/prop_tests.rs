//! Property tests for the hybrid logical clock: strict
//! monotonicity under an arbitrary (possibly frozen or rewinding) wall clock,
//! total order across nodes, `observe` never regressing, and skew absorption
//! (logical growth while the wall clock stalls, reset once it advances).

use proptest::prelude::*;

use super::{Hlc, HlcClock};
use crate::node::NodeId;

fn hlc_strategy() -> impl Strategy<Value = Hlc> {
    (any::<u64>(), any::<u32>(), any::<u64>()).prop_map(|(wall_ms, logical, node)| Hlc {
        wall_ms,
        logical,
        node: NodeId::from(node),
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// `HlcClock::now` must strictly increase on every call regardless of
    /// what the physical clock reports: frozen, jumping forward, or
    /// rewinding entirely.
    #[test]
    fn now_is_strictly_monotonic_under_arbitrary_wall_clock(
        node in any::<u64>(),
        ticks in proptest::collection::vec(0u64..10_000_000, 2..64),
    ) {
        let mut clock = HlcClock::new(NodeId::from(node));
        let mut prev = clock.now(ticks[0]);
        for &physical in &ticks[1..] {
            let cur = clock.now(physical);
            prop_assert!(cur > prev, "now() must never regress: {cur:?} <= {prev:?}");
            prev = cur;
        }
    }

    /// `HlcClock::observe` must strictly increase local time and dominate
    /// both the previous local stamp and the merged remote stamp, however
    /// skewed or stale the remote stamp or the local physical reading.
    #[test]
    fn observe_is_strictly_monotonic_and_dominates_remote(
        node in any::<u64>(),
        remote_node in any::<u64>(),
        events in proptest::collection::vec(
            (0u64..10_000_000, 0u64..10_000_000, 0u32..10_000),
            1..64,
        ),
    ) {
        let mut clock = HlcClock::new(NodeId::from(node));
        let mut prev = clock.now(0);
        for (physical, remote_wall, remote_logical) in events {
            let remote = Hlc {
                wall_ms: remote_wall,
                logical: remote_logical,
                node: NodeId::from(remote_node),
            };
            let merged = clock.observe(physical, remote);
            prop_assert!(merged > prev, "observe() must never regress local time");
            prop_assert!(merged > remote, "observe() must dominate the remote stamp");
            prev = merged;
        }
    }

    /// Any two distinct [`Hlc`] stamps compare unequal, and the ordering is a
    /// strict total order, trichotomous and antisymmetric; the node-id tiebreak
    /// is what makes two concurrent writers' stamps never collide.
    #[test]
    fn distinct_stamps_are_totally_ordered_and_never_compare_equal(
        a in hlc_strategy(),
        b in hlc_strategy(),
    ) {
        if a == b {
            prop_assert_eq!(a.cmp(&b), std::cmp::Ordering::Equal);
        } else {
            prop_assert_ne!(a.cmp(&b), std::cmp::Ordering::Equal, "distinct stamps must never compare equal");
            prop_assert_eq!(a.cmp(&b), b.cmp(&a).reverse(), "ordering must be antisymmetric");
        }
    }

    /// Skew absorption: while the physical clock is stalled at the same
    /// millisecond, `logical` strictly grows on every call; the instant the
    /// physical clock advances past the held millisecond, `logical` resets
    /// to zero.
    #[test]
    fn logical_grows_while_stalled_and_resets_once_wall_advances(
        node in any::<u64>(),
        base_wall in 0u64..10_000_000,
        stall_count in 1usize..32,
        advance in 1u64..10_000,
    ) {
        let mut clock = HlcClock::new(NodeId::from(node));
        let first = clock.now(base_wall);
        prop_assert_eq!(first.wall_ms, base_wall);
        let mut last_logical = first.logical;
        for _ in 0..stall_count {
            let stamp = clock.now(base_wall);
            prop_assert_eq!(stamp.wall_ms, base_wall, "wall clock must stay held while stalled");
            prop_assert!(stamp.logical > last_logical, "logical must grow every call while stalled");
            last_logical = stamp.logical;
        }
        let advanced = clock.now(base_wall + advance);
        prop_assert_eq!(advanced.wall_ms, base_wall + advance);
        prop_assert_eq!(advanced.logical, 0, "logical must reset once the wall clock genuinely advances");
    }
}

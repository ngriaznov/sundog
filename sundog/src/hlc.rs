//! Hybrid logical clock: version stamps that give total order to writes across
//! nodes even under clock skew. Hand-rolled rather than built on an existing
//! HLC crate, for exact control over the semantics, deterministic postcard
//! encoding, and a clock that's trivially property-testable on its own.

use serde::{Deserialize, Serialize};

use crate::node::NodeId;

/// A hybrid-logical-clock version stamp: `(wall_ms, logical, node)`, compared
/// lexicographically in that field order.
///
/// The derived [`Ord`] is the whole design: wall-clock time dominates when
/// clocks are sane, the logical counter breaks ties within the same
/// millisecond, and the node id gives a final total-order tiebreak so two
/// concurrent writes never compare equal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Hlc {
    /// Wall-clock milliseconds since the Unix epoch, the primary ordering key.
    pub wall_ms: u64,
    /// Tiebreaker within the same `wall_ms`, reset whenever `wall_ms` advances.
    pub logical: u32,
    /// Final tiebreaker: the stamping node, guaranteeing total order.
    pub node: NodeId,
}

/// A node's local hybrid logical clock, generating and merging [`Hlc`] stamps.
///
/// Not `Sync`: callers serialize access with a mutex or actor, matching how
/// a single per-node clock is used.
#[derive(Debug)]
pub struct HlcClock {
    node: NodeId,
    last: Hlc,
}

impl HlcClock {
    /// Creates a clock for `node`, initialized at the epoch.
    #[must_use]
    pub fn new(node: NodeId) -> Self {
        Self {
            node,
            last: Hlc {
                wall_ms: 0,
                logical: 0,
                node,
            },
        }
    }

    /// Stamps a local event: the standard HLC "send" rule.
    ///
    /// Advances `wall_ms` to `max(physical_now, last.wall_ms)`; when the
    /// physical clock hasn't caught up to the last stamp (skew, or two calls
    /// within the same millisecond), the millisecond is held and `logical`
    /// increments instead, guaranteeing strict monotonicity of the returned
    /// stamp regardless of wall-clock behavior.
    pub fn now(&mut self, physical_now_ms: u64) -> Hlc {
        let wall_ms = physical_now_ms.max(self.last.wall_ms);
        let logical = if wall_ms == self.last.wall_ms {
            self.last.logical + 1
        } else {
            0
        };
        self.last = Hlc {
            wall_ms,
            logical,
            node: self.node,
        };
        self.last
    }

    /// Merges an observed remote stamp: the standard HLC "receive" rule.
    ///
    /// Advances local time to stay causally after both the physical clock and
    /// the remote stamp, so an event caused by (or observed from) `remote`
    /// always compares greater than `remote` afterward.
    pub fn observe(&mut self, physical_now_ms: u64, remote: Hlc) -> Hlc {
        let wall_ms = physical_now_ms.max(self.last.wall_ms).max(remote.wall_ms);
        let logical = if wall_ms == self.last.wall_ms && wall_ms == remote.wall_ms {
            self.last.logical.max(remote.logical) + 1
        } else if wall_ms == self.last.wall_ms {
            self.last.logical + 1
        } else if wall_ms == remote.wall_ms {
            remote.logical + 1
        } else {
            0
        };
        self.last = Hlc {
            wall_ms,
            logical,
            node: self.node,
        };
        self.last
    }

    /// [`HlcClock::observe`] for a stamp no more than `max_skew_ms` ahead of
    /// `physical_now_ms`; a stamp further ahead is refused, leaving the
    /// clock where it was, and returns `None`. Without the bound one node
    /// with a clock an hour fast stamps every write an hour ahead, wins
    /// every conflict, and drags every clock that observes it forward by the
    /// same hour.
    pub fn observe_bounded(
        &mut self,
        physical_now_ms: u64,
        remote: Hlc,
        max_skew_ms: u64,
    ) -> Option<Hlc> {
        if exceeds_skew(physical_now_ms, remote.wall_ms, max_skew_ms) {
            return None;
        }
        Some(self.observe(physical_now_ms, remote))
    }

    /// How far this clock's last stamp runs ahead of `physical_now_ms`, in
    /// milliseconds: `0` while the physical clock is at or past it. Past a
    /// few milliseconds it means the physical clock stepped back, or the
    /// clock observed a stamp from a node whose clock runs fast.
    #[must_use]
    pub fn lead_ms(&self, physical_now_ms: u64) -> u64 {
        self.last.wall_ms.saturating_sub(physical_now_ms)
    }
}

/// Whether a stamp at `wall_ms` is more than `max_skew_ms` ahead of
/// `physical_now_ms`. A stamp behind the physical clock never exceeds it:
/// an old write is ordinary, a write from the future is the skew.
#[must_use]
pub(crate) fn exceeds_skew(physical_now_ms: u64, wall_ms: u64, max_skew_ms: u64) -> bool {
    wall_ms > physical_now_ms.saturating_add(max_skew_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(n: u64) -> NodeId {
        NodeId::from(n)
    }

    #[test]
    fn now_is_monotonic_even_with_stalled_physical_clock() {
        let mut clock = HlcClock::new(node(1));
        let a = clock.now(1_000);
        let b = clock.now(1_000);
        let c = clock.now(999);
        assert!(a < b, "same millisecond must advance logical");
        assert!(b < c, "clock going backwards must still advance");
    }

    #[test]
    fn tiebreak_is_total_across_equal_wall_and_logical() {
        let stamp_a = Hlc {
            wall_ms: 5,
            logical: 0,
            node: node(1),
        };
        let stamp_b = Hlc {
            wall_ms: 5,
            logical: 0,
            node: node(2),
        };
        assert_ne!(stamp_a, stamp_b);
        assert!(stamp_a < stamp_b);
    }

    #[test]
    fn observe_absorbs_skewed_remote_stamp() {
        let mut clock = HlcClock::new(node(1));
        let local_before = clock.now(1_000);
        let remote = Hlc {
            wall_ms: 50_000,
            logical: 3,
            node: node(2),
        };
        let merged = clock.observe(1_000, remote);
        assert!(merged > local_before);
        assert!(merged > remote);
        assert_eq!(merged.wall_ms, 50_000);
        assert_eq!(merged.logical, 4);
    }

    #[test]
    fn observe_of_stale_remote_still_advances_past_local() {
        let mut clock = HlcClock::new(node(1));
        let local_before = clock.now(10_000);
        let stale_remote = Hlc {
            wall_ms: 1,
            logical: 0,
            node: node(2),
        };
        let merged = clock.observe(10_000, stale_remote);
        assert!(merged > local_before);
    }

    #[test]
    fn exceeds_skew_is_strictly_past_the_bound_and_never_for_a_stamp_behind() {
        assert!(!exceeds_skew(1_000, 1_000, 0));
        assert!(!exceeds_skew(1_000, 61_000, 60_000));
        assert!(exceeds_skew(1_000, 61_001, 60_000));
        assert!(!exceeds_skew(1_000, 1, 0), "an old stamp is never skew");
        assert!(
            !exceeds_skew(u64::MAX - 5, u64::MAX, 60_000),
            "the bound saturates instead of wrapping"
        );
    }

    #[test]
    fn observe_bounded_refuses_a_stamp_past_the_bound_and_leaves_the_clock() {
        let mut clock = HlcClock::new(node(1));
        let before = clock.now(1_000);
        let ahead = Hlc {
            wall_ms: 1_000 + 3_600_000,
            logical: 0,
            node: node(2),
        };
        assert_eq!(clock.observe_bounded(1_000, ahead, 60_000), None);
        assert_eq!(clock.lead_ms(1_000), 0, "the refused stamp moved nothing");
        let next = clock.now(1_000);
        assert!(next > before);
        assert_eq!(next.wall_ms, 1_000);
    }

    #[test]
    fn observe_bounded_merges_a_stamp_within_the_bound_like_observe() {
        let mut bounded = HlcClock::new(node(1));
        let mut plain = HlcClock::new(node(1));
        let remote = Hlc {
            wall_ms: 31_000,
            logical: 2,
            node: node(2),
        };
        assert_eq!(
            bounded.observe_bounded(1_000, remote, 60_000),
            Some(plain.observe(1_000, remote))
        );
        assert_eq!(bounded.lead_ms(1_000), 30_000);
        assert_eq!(bounded.lead_ms(40_000), 0);
    }

    #[test]
    fn repeated_observe_never_goes_backwards() {
        let mut clock = HlcClock::new(node(1));
        let mut prev = clock.now(0);
        for wall in [10, 10, 5, 20, 20, 20] {
            let remote = Hlc {
                wall_ms: wall,
                logical: 0,
                node: node(2),
            };
            let merged = clock.observe(wall, remote);
            assert!(merged > prev);
            prev = merged;
        }
    }
}

#[cfg(test)]
mod prop_tests;

/// Kani proofs of the clock's ordering rules over every stamp.
#[cfg(kani)]
mod kani_proofs {
    use super::*;

    fn any_hlc() -> Hlc {
        Hlc {
            wall_ms: kani::any(),
            logical: kani::any(),
            node: NodeId::from(kani::any::<u64>()),
        }
    }

    /// A local stamp is greater than the previous one and never behind the physical clock.
    #[kani::proof]
    fn now_advances_past_the_last_stamp() {
        let last = any_hlc();
        kani::assume(last.logical < u32::MAX);
        let mut clock = HlcClock {
            node: last.node,
            last,
        };
        let physical_now_ms: u64 = kani::any();
        let stamp = clock.now(physical_now_ms);
        assert!(stamp > last);
        assert!(stamp.wall_ms >= physical_now_ms);
        assert_eq!(stamp.node, last.node);
    }

    /// An observed stamp is greater than both the previous local stamp and the remote one.
    #[kani::proof]
    fn observe_advances_past_the_last_and_the_remote_stamp() {
        let last = any_hlc();
        let remote = any_hlc();
        kani::assume(last.logical < u32::MAX);
        kani::assume(remote.logical < u32::MAX);
        let mut clock = HlcClock {
            node: last.node,
            last,
        };
        let physical_now_ms: u64 = kani::any();
        let stamp = clock.observe(physical_now_ms, remote);
        assert!(stamp > last);
        assert!(stamp > remote);
        assert!(stamp.wall_ms >= physical_now_ms);
    }

    /// A bounded observe never carries the clock past the bound: a clock
    /// that started within `max_skew_ms` of the physical clock is still
    /// within it afterward, whether the remote stamp was merged or refused.
    #[kani::proof]
    fn observe_bounded_keeps_the_clock_within_the_bound() {
        let last = any_hlc();
        let remote = any_hlc();
        kani::assume(last.logical < u32::MAX);
        kani::assume(remote.logical < u32::MAX);
        let physical_now_ms: u64 = kani::any();
        let max_skew_ms: u64 = kani::any();
        kani::assume(last.wall_ms <= physical_now_ms.saturating_add(max_skew_ms));
        let mut clock = HlcClock {
            node: last.node,
            last,
        };
        match clock.observe_bounded(physical_now_ms, remote, max_skew_ms) {
            Some(stamp) => {
                assert!(stamp > last);
                assert!(stamp > remote);
            }
            None => assert_eq!(clock.last, last),
        }
        assert!(clock.last.wall_ms <= physical_now_ms.saturating_add(max_skew_ms));
    }
}

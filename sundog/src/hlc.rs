//! Hybrid logical clock: version stamps that give total order to writes across
//! nodes even under clock skew, per house rules (hand-rolled, not `uhlc`).

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
/// Not `Sync`: callers serialize access with a mutex or actor, matching how a
/// single per-node clock is used in practice.
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

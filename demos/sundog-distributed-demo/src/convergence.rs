//! Cross-node convergence check for the distributed demo cache: the sum of
//! every live node's local entry count should equal `owners * surviving
//! keys` once rebalancing has settled. Unlike the replicated demo, where
//! every node's count should match every other's, a distributed cache
//! spreads entries across owners, so the check is on the *sum*, and it
//! tolerates a bounded settling window — `distributed_disown_grace_rounds`
//! anti-entropy intervals plus slack — during which a node that just lost a
//! bucket still holds it while a new owner's rebalance pull lands.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::node::NodeSlot;

/// Whether the live nodes' summed entry count currently matches
/// `owners * surviving_keys`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Convergence {
    NoLiveNodes,
    Converged {
        total: u64,
        live: usize,
    },
    Diverged {
        total: u64,
        expected: u64,
        live: usize,
    },
}

impl Convergence {
    #[must_use]
    pub(crate) fn is_diverged(self) -> bool {
        matches!(self, Self::Diverged { .. })
    }
}

impl fmt::Display for Convergence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoLiveNodes => write!(f, "no live nodes"),
            Self::Converged { total, live } => {
                write!(f, "CONVERGED — {live} live node(s), {total} entries total")
            }
            Self::Diverged {
                total,
                expected,
                live,
            } => {
                write!(
                    f,
                    "DIVERGED — {live} live node(s) hold {total} entries total, expected {expected}"
                )
            }
        }
    }
}

/// `owners * surviving_keys`, the entry count every live node's local
/// counts should sum to once rebalancing settles.
#[must_use]
pub(crate) fn expected_entries(owners: u64, surviving_keys: usize) -> u64 {
    owners.saturating_mul(u64::try_from(surviving_keys).unwrap_or(u64::MAX))
}

/// Compares a summed entry count against the expected total. Pure: the
/// summing and the live-node count are computed by the caller.
#[must_use]
pub(crate) fn check(total: u64, expected: u64, live: usize) -> Convergence {
    if live == 0 {
        return Convergence::NoLiveNodes;
    }
    if total == expected {
        Convergence::Converged { total, live }
    } else {
        Convergence::Diverged {
            total,
            expected,
            live,
        }
    }
}

/// How long the poll loop waits for convergence before giving up: enough
/// anti-entropy rounds for a lost bucket's disown grace to run out, plus
/// slack for the rebalance pull and one more round of settling.
#[must_use]
pub(crate) fn poll_deadline(ae_interval: Duration, disown_grace_rounds: u32) -> Duration {
    ae_interval.saturating_mul(disown_grace_rounds.saturating_add(4))
}

/// Sums every live node's local entry count.
#[must_use]
pub(crate) fn total_live_entries(nodes: &[Arc<NodeSlot>]) -> (u64, usize) {
    let mut total = 0u64;
    let mut live = 0usize;
    for node in nodes {
        if node.is_alive() {
            live += 1;
            let count = node.status.entry_count.load(Ordering::Relaxed);
            total += u64::try_from(count.max(0)).unwrap_or(0);
        }
    }
    (total, live)
}

/// Polls [`total_live_entries`] against `expected_entries(owners,
/// surviving_keys())` until it converges or `deadline` elapses, sleeping
/// briefly between attempts.
pub(crate) async fn poll(
    nodes: &[Arc<NodeSlot>],
    owners: u64,
    surviving_keys: impl Fn() -> usize,
    deadline: Duration,
) -> Convergence {
    let started = tokio::time::Instant::now();
    loop {
        let (total, live) = total_live_entries(nodes);
        let expected = expected_entries(owners, surviving_keys());
        let report = check(total, expected, live);
        if !report.is_diverged() || started.elapsed() >= deadline {
            return report;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::build_slots;

    #[test]
    fn expected_entries_multiplies_owners_by_surviving_keys() {
        assert_eq!(expected_entries(2, 1000), 2000);
        assert_eq!(expected_entries(3, 0), 0);
    }

    #[test]
    fn no_live_nodes_reports_no_live_nodes_regardless_of_totals() {
        assert_eq!(check(0, 100, 0), Convergence::NoLiveNodes);
    }

    #[test]
    fn matching_total_converges() {
        assert_eq!(
            check(2000, 2000, 3),
            Convergence::Converged {
                total: 2000,
                live: 3
            }
        );
    }

    #[test]
    fn mismatched_total_diverges() {
        assert_eq!(
            check(1900, 2000, 3),
            Convergence::Diverged {
                total: 1900,
                expected: 2000,
                live: 3
            }
        );
        assert!(check(1900, 2000, 3).is_diverged());
    }

    #[test]
    fn poll_deadline_scales_with_grace_rounds_plus_slack() {
        assert_eq!(
            poll_deadline(Duration::from_secs(3), 2),
            Duration::from_secs(18)
        );
        assert_eq!(
            poll_deadline(Duration::from_secs(3), 3),
            Duration::from_secs(21)
        );
    }

    #[test]
    fn total_live_entries_sums_only_alive_nodes() {
        let slots = build_slots(3, 43_000);
        slots[0].status.alive.store(true, Ordering::Relaxed);
        slots[0].status.entry_count.store(5, Ordering::Relaxed);
        slots[1].status.alive.store(true, Ordering::Relaxed);
        slots[1].status.entry_count.store(7, Ordering::Relaxed);
        slots[2].status.entry_count.store(999, Ordering::Relaxed);
        assert_eq!(total_live_entries(&slots), (12, 2));
    }

    #[tokio::test]
    async fn poll_returns_immediately_once_converged() {
        let slots: Vec<Arc<NodeSlot>> = build_slots(1, 43_100);
        slots[0].status.alive.store(true, Ordering::Relaxed);
        slots[0].status.entry_count.store(10, Ordering::Relaxed);
        let report = poll(&slots, 1, || 10, Duration::from_secs(5)).await;
        assert_eq!(report, Convergence::Converged { total: 10, live: 1 });
    }

    #[tokio::test]
    async fn poll_gives_up_at_the_deadline_on_persistent_divergence() {
        let slots: Vec<Arc<NodeSlot>> = build_slots(1, 43_200);
        slots[0].status.alive.store(true, Ordering::Relaxed);
        slots[0].status.entry_count.store(1, Ordering::Relaxed);
        let started = tokio::time::Instant::now();
        let report = poll(&slots, 1, || 10, Duration::from_millis(400)).await;
        assert!(report.is_diverged());
        assert!(started.elapsed() >= Duration::from_millis(400));
    }
}

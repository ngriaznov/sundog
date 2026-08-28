//! Cross-node convergence check: the soak-test signal (plan §11.4) — do all
//! currently live nodes report the same local entry count for the
//! replicated demo cache?

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::node::NodeSlot;

/// Whether the live nodes currently agree on how many entries they hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Convergence {
    NoLiveNodes,
    Converged { entries: i64, live: usize },
    Diverged { min: i64, max: i64, live: usize },
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
            Self::Converged { entries, live } => {
                write!(
                    f,
                    "CONVERGED — {live} live node(s) agree on {entries} entries"
                )
            }
            Self::Diverged { min, max, live } => {
                write!(
                    f,
                    "DIVERGED — {live} live node(s) range {min}..={max} entries"
                )
            }
        }
    }
}

/// Compares local entry counts across every currently-alive node.
#[must_use]
pub(crate) fn check(nodes: &[Arc<NodeSlot>]) -> Convergence {
    let counts: Vec<i64> = nodes
        .iter()
        .filter(|n| n.is_alive())
        .map(|n| n.status.entry_count.load(Ordering::Relaxed))
        .collect();
    let Some(&first) = counts.first() else {
        return Convergence::NoLiveNodes;
    };
    let live = counts.len();
    if counts.iter().all(|&c| c == first) {
        Convergence::Converged {
            entries: first,
            live,
        }
    } else {
        let min = *counts.iter().min().expect("invariant: counts is non-empty");
        let max = *counts.iter().max().expect("invariant: counts is non-empty");
        Convergence::Diverged { min, max, live }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::build_slots;

    #[test]
    fn no_nodes_alive_reports_no_live_nodes() {
        let slots = build_slots(3, 30_000);
        assert_eq!(check(&slots), Convergence::NoLiveNodes);
    }

    #[test]
    fn matching_counts_converge() {
        let slots = build_slots(3, 30_100);
        for slot in &slots {
            slot.status.alive.store(true, Ordering::Relaxed);
            slot.status.entry_count.store(7, Ordering::Relaxed);
        }
        assert_eq!(
            check(&slots),
            Convergence::Converged {
                entries: 7,
                live: 3
            }
        );
    }

    #[test]
    fn mismatched_counts_diverge() {
        let slots = build_slots(2, 30_200);
        slots[0].status.alive.store(true, Ordering::Relaxed);
        slots[0].status.entry_count.store(3, Ordering::Relaxed);
        slots[1].status.alive.store(true, Ordering::Relaxed);
        slots[1].status.entry_count.store(5, Ordering::Relaxed);
        assert_eq!(
            check(&slots),
            Convergence::Diverged {
                min: 3,
                max: 5,
                live: 2
            }
        );
        assert!(check(&slots).is_diverged());
    }

    #[test]
    fn killed_nodes_are_excluded() {
        let slots = build_slots(2, 30_300);
        slots[0].status.alive.store(true, Ordering::Relaxed);
        slots[0].status.entry_count.store(9, Ordering::Relaxed);
        slots[1].status.entry_count.store(2, Ordering::Relaxed);
        assert_eq!(
            check(&slots),
            Convergence::Converged {
                entries: 9,
                live: 1
            }
        );
    }
}

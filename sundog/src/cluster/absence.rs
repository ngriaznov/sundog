//! Partition-aware tombstone retention. Tracks which recently known members
//! are absent from the live peer set, so tombstone collection defers while a
//! member that may still hold the deleted entry is unreachable. Collecting
//! anyway would let anti-entropy resurrect the entry when that member
//! returns.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex as StdMutex, PoisonError};
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::node::NodeId;
use crate::ownership::OwnershipView;
use crate::store::Mode;

#[derive(Default)]
struct AbsenceState {
    live: HashSet<NodeId>,
    /// The last-seen graceful-departure flag for every currently live peer,
    /// consulted the moment a peer drops out of `live` so a crash (flag never
    /// set) and a graceful leave (flag set just before disappearing) are
    /// told apart.
    last_departing: HashMap<NodeId, bool>,
    absent_since: HashMap<NodeId, Instant>,
}

/// Cheap-to-clone, cluster-wide view of which recently-known members are
/// currently absent, fed by [`tracking_task`] and sampled by
/// `tombstone_gc_task` via [`should_defer_gc`]. On a single-node cluster the
/// live peer set is always empty, so [`AbsenceTracker::any_absent`] stays
/// `false`.
#[derive(Clone, Default)]
pub(crate) struct AbsenceTracker {
    state: Arc<StdMutex<AbsenceState>>,
}

impl AbsenceTracker {
    /// Applies one membership snapshot, the live peers and whether each has
    /// gossiped a graceful departure: a peer newly dropped from `live`
    /// starts being tracked absent unless its last flag was set; a peer back
    /// in `live` clears it.
    fn observe(&self, live: &HashMap<NodeId, bool>) {
        let live_ids: HashSet<NodeId> = live.keys().copied().collect();
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let departed: Vec<NodeId> = state.live.difference(&live_ids).copied().collect();
        for node in departed {
            let departed_gracefully = state.last_departing.get(&node).copied().unwrap_or(false);
            if counts_as_absent(departed_gracefully) {
                state.absent_since.entry(node).or_insert_with(Instant::now);
            }
            state.last_departing.remove(&node);
        }
        for (&node, &departing) in live {
            state.absent_since.remove(&node);
            state.last_departing.insert(node, departing);
        }
        state.live = live_ids;
    }

    /// Whether any recently-known member is absent and not yet aged past
    /// `hard_cap`. Prunes older entries as a side effect.
    pub(crate) fn any_absent(&self, hard_cap: Duration) -> bool {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let now = Instant::now();
        state
            .absent_since
            .retain(|_, since| now.saturating_duration_since(*since) < hard_cap);
        !state.absent_since.is_empty()
    }

    /// Every recently-known member currently absent and not yet aged past
    /// `hard_cap`, for [`should_defer_gc`]'s `Mode::Distributed` rule.
    /// Prunes older entries as a side effect, same as [`AbsenceTracker::any_absent`].
    pub(crate) fn absent_nodes(&self, hard_cap: Duration) -> Vec<NodeId> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let now = Instant::now();
        state
            .absent_since
            .retain(|_, since| now.saturating_duration_since(*since) < hard_cap);
        state.absent_since.keys().copied().collect()
    }
}

/// Whether `tombstone_gc_task` defers a tombstone past `tombstone_ttl`
/// this tick. [`Mode::Replicated`] defers on any absence at all: any
/// absent member could hold a copy anti-entropy would otherwise
/// resurrect. `Mode::Distributed` narrows the question to buckets `ownership`
/// says this node owns: an absent member matters only if it currently
/// co-owns at least one of them too — an absent member sharing no bucket
/// holds nothing this node's tombstones could be resurrected by.
/// `ownership` is `None` for a `Mode::Distributed` cache with no view
/// attached yet, which never defers. `Mode::Local`/`Mode::Invalidation`
/// never run anti-entropy, the mechanism that could resurrect a
/// tombstone, so they never defer either.
pub(crate) fn should_defer_gc(
    mode: Mode,
    tracker: &AbsenceTracker,
    hard_cap: Duration,
    ownership: Option<&OwnershipView>,
) -> bool {
    match mode {
        Mode::Replicated => tracker.any_absent(hard_cap),
        Mode::Distributed { .. } => {
            let Some(view) = ownership else {
                return false;
            };
            tracker.absent_nodes(hard_cap).into_iter().any(|node| {
                view.owned_buckets()
                    .any(|b| view.owners_of(b).contains(&node))
            })
        }
        Mode::Local | Mode::Invalidation => false,
    }
}

/// Whether a node that just dropped out of the live set should start being
/// tracked absent: true unless `departed_gracefully` shows it gossiped its
/// departure (chitchat's `departing` key) before it left. A crash carries no
/// such signal, so it always counts.
fn counts_as_absent(departed_gracefully: bool) -> bool {
    !departed_gracefully
}

/// Republishes [`crate::membership::Membership::departing_flags`] changes
/// into `tracker`, keeping [`AbsenceTracker`] current.
pub(crate) async fn tracking_task(
    mut live: watch::Receiver<HashMap<NodeId, bool>>,
    tracker: AbsenceTracker,
    cancel: CancellationToken,
) {
    tracker.observe(&live.borrow_and_update());
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            changed = live.changed() => {
                if changed.is_err() {
                    return; // membership shut down
                }
                tracker.observe(&live.borrow_and_update());
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    /// A live set of `(node, departing)` pairs.
    fn live(peers: &[(u64, bool)]) -> HashMap<NodeId, bool> {
        peers
            .iter()
            .map(|&(node, departing)| (NodeId::from(node), departing))
            .collect()
    }

    #[test]
    fn no_peers_ever_observed_means_never_absent() {
        let tracker = AbsenceTracker::default();
        assert!(!tracker.any_absent(Duration::from_secs(3600)));
    }

    #[test]
    fn a_peer_that_leaves_the_live_set_is_tracked_absent() {
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(1, false)]));
        assert!(
            !tracker.any_absent(Duration::from_secs(3600)),
            "still live: not absent"
        );

        tracker.observe(&live(&[]));
        assert!(
            tracker.any_absent(Duration::from_secs(3600)),
            "dropped out of the live set without gossiping a departure: now tracked absent"
        );
    }

    #[test]
    fn a_returning_peer_clears_its_tracked_absence() {
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(1, false)]));
        tracker.observe(&live(&[]));
        assert!(tracker.any_absent(Duration::from_secs(3600)));

        tracker.observe(&live(&[(1, false)]));
        assert!(
            !tracker.any_absent(Duration::from_secs(3600)),
            "a live member is not tracked absent"
        );
    }

    #[test]
    fn a_peer_that_gossiped_departing_before_leaving_is_never_tracked_absent() {
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(1, true)]));

        tracker.observe(&live(&[]));
        assert!(
            !tracker.any_absent(Duration::from_secs(3600)),
            "a graceful departure never counts as absence"
        );
    }

    #[tokio::test]
    async fn absence_ages_out_past_the_hard_cap() {
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(1, false)]));
        tracker.observe(&live(&[]));

        let tiny_cap = Duration::from_millis(1);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !tracker.any_absent(tiny_cap),
            "absence older than the hard cap ages out"
        );
    }

    #[test]
    fn counts_as_absent_is_false_only_for_a_graceful_departure() {
        assert!(counts_as_absent(false), "a crash carries no departing flag");
        assert!(!counts_as_absent(true), "a graceful departure never counts");
    }

    #[test]
    fn should_defer_gc_ignores_absence_outside_replicated_mode() {
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(1, false)]));
        tracker.observe(&live(&[]));
        let hard_cap = Duration::from_secs(3600);

        assert!(should_defer_gc(Mode::Replicated, &tracker, hard_cap, None));
        assert!(!should_defer_gc(Mode::Local, &tracker, hard_cap, None));
        assert!(!should_defer_gc(
            Mode::Invalidation,
            &tracker,
            hard_cap,
            None
        ));
    }

    #[test]
    fn should_defer_gc_is_false_for_replicated_mode_with_no_absence() {
        let tracker = AbsenceTracker::default();
        assert!(!should_defer_gc(
            Mode::Replicated,
            &tracker,
            Duration::from_secs(3600),
            None,
        ));
    }

    fn distributed_view(self_node: NodeId, eligible: Vec<NodeId>) -> OwnershipView {
        OwnershipView::compute(
            self_node,
            eligible,
            std::num::NonZeroU8::new(2).expect("nonzero"),
        )
    }

    #[test]
    fn should_defer_gc_is_false_for_distributed_mode_with_no_view_attached() {
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(1, false)]));
        tracker.observe(&live(&[]));
        let mode = Mode::Distributed {
            owners: std::num::NonZeroU8::new(2).expect("nonzero"),
        };

        assert!(!should_defer_gc(
            mode,
            &tracker,
            Duration::from_secs(3600),
            None,
        ));
    }

    #[test]
    fn should_defer_gc_ignores_an_absent_member_sharing_no_owned_bucket() {
        let self_node = NodeId::from(1);
        // A view with just self as eligible: the absent stranger (node 99)
        // never shows up in any bucket's owner list.
        let view = distributed_view(self_node, vec![self_node]);
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(99, false)]));
        tracker.observe(&live(&[]));
        let mode = Mode::Distributed {
            owners: std::num::NonZeroU8::new(2).expect("nonzero"),
        };

        assert!(!should_defer_gc(
            mode,
            &tracker,
            Duration::from_secs(3600),
            Some(&view),
        ));
    }

    #[test]
    fn should_defer_gc_defers_for_an_absent_member_sharing_an_owned_bucket() {
        let self_node = NodeId::from(1);
        let co_owner = NodeId::from(2);
        // Both nodes eligible with owners=2: every bucket's owner set is
        // exactly {self_node, co_owner}, so they share everything self owns.
        let view = distributed_view(self_node, vec![self_node, co_owner]);
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(2, false)]));
        tracker.observe(&live(&[]));
        let mode = Mode::Distributed {
            owners: std::num::NonZeroU8::new(2).expect("nonzero"),
        };

        assert!(should_defer_gc(
            mode,
            &tracker,
            Duration::from_secs(3600),
            Some(&view),
        ));
    }
}

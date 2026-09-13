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

use crate::membership::LiveFlags;
use crate::node::NodeId;
use crate::ownership::OwnershipView;
use crate::store::Mode;

#[derive(Default)]
struct AbsenceState {
    live: HashSet<NodeId>,
    /// The last-seen flags for every currently live peer, consulted the
    /// moment a peer drops out of `live`: `departing` tells a crash (never
    /// set) from a graceful leave (set just before disappearing) apart.
    last_flags: HashMap<NodeId, LiveFlags>,
    absent_since: HashMap<NodeId, Instant>,
    /// When each member that gossiped a graceful departure left, kept
    /// apart from `absent_since`: a graceful leaver holds no tombstone
    /// hostage, so tombstone collection never defers for it, but its
    /// writer incarnation is gone for good and the CRDT compaction sweep
    /// retires it exactly like a crashed one.
    departed_since: HashMap<NodeId, Instant>,
    /// How long each live member has been continuously present, keyed only
    /// while the member is live. Reset (removed) only when the member is
    /// marked absent below — never touched by an `observe()` call that
    /// leaves the member live throughout, so gossip jitter that never trips
    /// the failure detector (never drops the member from `live`) never
    /// resets it.
    present_since: HashMap<NodeId, Instant>,
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
    /// Applies one membership snapshot, the live peers and each one's
    /// [`LiveFlags`]: a peer newly dropped from `live` starts being tracked
    /// absent unless its last flag showed a graceful departure, in which
    /// case it is tracked departed instead; a peer back in `live` clears
    /// either and starts a fresh continuous-presence timer.
    pub(crate) fn observe(&self, live: &HashMap<NodeId, LiveFlags>) {
        let live_ids: HashSet<NodeId> = live.keys().copied().collect();
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let departed: Vec<NodeId> = state.live.difference(&live_ids).copied().collect();
        for node in departed {
            let flags = state.last_flags.remove(&node);
            let departed_gracefully = flags.is_some_and(|f| f.departing);
            if counts_as_absent(departed_gracefully) {
                state.absent_since.entry(node).or_insert_with(Instant::now);
            } else {
                state
                    .departed_since
                    .entry(node)
                    .or_insert_with(Instant::now);
            }
            state.present_since.remove(&node);
        }
        for (&node, &flags) in live {
            state.absent_since.remove(&node);
            state.departed_since.remove(&node);
            state.present_since.entry(node).or_insert_with(Instant::now);
            state.last_flags.insert(node, flags);
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

    /// Every recently-known member gone for at least `retire_after`,
    /// whether it crashed (tracked absent) or gossiped a graceful departure
    /// (tracked departed): the members whose writer incarnations the CRDT
    /// compaction sweep may retire. Unlike [`AbsenceTracker::any_absent`]/
    /// [`AbsenceTracker::absent_nodes`], never prunes — a compaction sweep and a
    /// tombstone-GC tick share this tracker and, since `crdt_retire_after`
    /// defaults to the same value as `tombstone_max_ttl`, a destructive read
    /// here could non-deterministically win a race against `any_absent`'s
    /// own pruning at the same age boundary.
    pub(crate) fn gone_longer_than(&self, retire_after: Duration) -> Vec<NodeId> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let now = Instant::now();
        state
            .absent_since
            .iter()
            .chain(state.departed_since.iter())
            .filter(|(_, since)| now.saturating_duration_since(**since) >= retire_after)
            .map(|(&node, _)| node)
            .collect()
    }

    /// This member's continuous-presence start time, if it is currently
    /// live and tracked; `None` if it has never been observed live or is
    /// currently tracked absent. Reset only when the member is marked
    /// absent (dropped from the live set without a graceful departure), so
    /// gossip jitter that never trips the failure detector never moves this
    /// forward.
    pub(crate) fn present_since(&self, node: NodeId) -> Option<Instant> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.present_since.get(&node).copied()
    }

    /// When this member went away, if it is currently tracked absent (a
    /// crash) or departed (a graceful leave); `None` if it is live or has
    /// never been observed.
    pub(crate) fn gone_since(&self, node: NodeId) -> Option<Instant> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state
            .absent_since
            .get(&node)
            .or_else(|| state.departed_since.get(&node))
            .copied()
    }

    /// Drops `node`'s tracked absence or departure once its writer slot is
    /// actually retired by the compaction sweep, so a node that later
    /// returns starts from a clean slate rather than an entry this tracker
    /// would otherwise carry forever.
    pub(crate) fn mark_retired(&self, node: NodeId) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.absent_since.remove(&node);
        state.departed_since.remove(&node);
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
    mut live: watch::Receiver<HashMap<NodeId, LiveFlags>>,
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
    fn live(peers: &[(u64, bool)]) -> HashMap<NodeId, LiveFlags> {
        peers
            .iter()
            .map(|&(node, departing)| (NodeId::from(node), LiveFlags { departing }))
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
    fn a_graceful_departure_is_gone_but_never_absent() {
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(1, true)]));
        tracker.observe(&live(&[]));
        assert!(
            !tracker.any_absent(Duration::from_secs(3600)),
            "tombstone deferral never waits on a graceful leaver"
        );
        assert!(
            tracker.gone_since(NodeId::from(1)).is_some(),
            "its writer incarnation is gone for good"
        );
        assert_eq!(
            tracker.gone_longer_than(Duration::ZERO),
            vec![NodeId::from(1)],
            "listed among the members the compaction sweep may retire"
        );
        assert!(
            tracker.present_since(NodeId::from(1)).is_none(),
            "no longer continuously present"
        );
    }

    #[test]
    fn a_crash_is_gone_and_absent() {
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(1, false)]));
        tracker.observe(&live(&[]));
        assert!(tracker.any_absent(Duration::from_secs(3600)));
        assert!(tracker.gone_since(NodeId::from(1)).is_some());
        assert_eq!(
            tracker.gone_longer_than(Duration::ZERO),
            vec![NodeId::from(1)]
        );
    }

    #[test]
    fn a_returning_peer_clears_its_tracked_departure() {
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(1, true)]));
        tracker.observe(&live(&[]));
        assert!(tracker.gone_since(NodeId::from(1)).is_some());
        tracker.observe(&live(&[(1, false)]));
        assert!(tracker.gone_since(NodeId::from(1)).is_none());
        assert!(tracker.gone_longer_than(Duration::ZERO).is_empty());
    }

    #[test]
    fn mark_retired_clears_a_tracked_departure() {
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(1, true)]));
        tracker.observe(&live(&[]));
        tracker.mark_retired(NodeId::from(1));
        assert!(tracker.gone_since(NodeId::from(1)).is_none());
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

    #[test]
    fn present_since_is_not_reset_by_jitter_that_never_reaches_the_detector() {
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(1, false)]));
        let first = tracker
            .present_since(NodeId::from(1))
            .expect("live member is tracked present");

        // Further observations that still show the member live are the
        // failure-detector-invisible jitter case: the member never actually
        // drops out of the live set, so its continuous-presence start time
        // must not move forward.
        tracker.observe(&live(&[(1, false)]));
        tracker.observe(&live(&[(1, false)]));
        assert_eq!(
            tracker.present_since(NodeId::from(1)),
            Some(first),
            "repeated observations of a still-live member never reset present_since"
        );
    }

    #[test]
    fn present_since_is_reset_once_the_tracker_marks_the_member_absent() {
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(1, false)]));
        assert!(tracker.present_since(NodeId::from(1)).is_some());

        tracker.observe(&live(&[]));
        assert_eq!(
            tracker.present_since(NodeId::from(1)),
            None,
            "an absent member has no continuous-presence start time"
        );

        tracker.observe(&live(&[(1, false)]));
        assert!(
            tracker.present_since(NodeId::from(1)).is_some(),
            "a returning member starts a fresh presence timer"
        );
    }

    #[test]
    fn present_since_is_none_for_a_never_observed_member() {
        let tracker = AbsenceTracker::default();
        assert_eq!(tracker.present_since(NodeId::from(1)), None);
    }

    #[test]
    fn absent_since_is_none_while_live_and_some_once_absent() {
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(1, false)]));
        assert_eq!(tracker.gone_since(NodeId::from(1)), None);

        tracker.observe(&live(&[]));
        assert!(tracker.gone_since(NodeId::from(1)).is_some());
    }

    #[test]
    fn absent_longer_than_finds_a_member_aged_past_the_bound_without_pruning() {
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(1, false)]));
        tracker.observe(&live(&[]));

        let retire_after = Duration::from_millis(1);
        // A tombstone-GC-style hard cap the member is nowhere near yet, so a
        // concurrent destructive `any_absent` call at this cap must not
        // prune the entry `absent_longer_than` is about to read.
        let tombstone_hard_cap = Duration::from_secs(3600);
        std::thread::sleep(Duration::from_millis(20));

        assert_eq!(
            tracker.gone_longer_than(retire_after),
            vec![NodeId::from(1)]
        );
        assert!(
            tracker.any_absent(tombstone_hard_cap),
            "a concurrent tombstone-GC read at its own, much larger, hard cap"
        );
        assert_eq!(
            tracker.gone_longer_than(retire_after),
            vec![NodeId::from(1)],
            "absent_longer_than never prunes, so a second read still sees the member"
        );
    }

    #[test]
    fn absent_longer_than_excludes_a_member_not_yet_aged_past_the_bound() {
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(1, false)]));
        tracker.observe(&live(&[]));

        assert_eq!(
            tracker.gone_longer_than(Duration::from_secs(3600)),
            Vec::new(),
            "a member absent for less than the bound is not yet retirement-eligible"
        );
    }

    #[test]
    fn mark_retired_clears_absent_since_too() {
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(1, false)]));
        tracker.observe(&live(&[]));
        assert!(tracker.gone_since(NodeId::from(1)).is_some());

        tracker.mark_retired(NodeId::from(1));
        assert_eq!(tracker.gone_since(NodeId::from(1)), None);
    }
}

//! Partition-aware tombstone retention. Tracks which recently known members
//! are absent from the live peer set, so tombstone collection defers while a
//! member that may still hold the deleted entry is unreachable. Collecting
//! anyway lets anti-entropy resurrect the entry when that member returns.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::membership::LiveFlags;
use crate::node::NodeId;
use crate::ownership::OwnershipView;
use crate::store::Mode;

/// Everything this tracker keeps about one member, replacing six maps keyed
/// by [`NodeId`]. Liveness is `present_since.is_some()`.
#[derive(Default, Clone, Copy)]
struct MemberRecord {
    /// Last-seen flags while live; `departing` marks a graceful leave.
    flags: LiveFlags,
    /// Set when this member drops out of live without a graceful departure.
    absent_since: Option<Instant>,
    /// Set when this member leaves live, crashed or graceful; clears on return.
    gone_since: Option<Instant>,
    /// The first time this member is seen live; cleared only by pruning.
    known_since: Option<Instant>,
    /// Continuous-presence start; cleared only when the member goes absent.
    present_since: Option<Instant>,
}

impl MemberRecord {
    /// Whether every field is unset: nothing here is worth keeping.
    fn is_empty(&self) -> bool {
        self.absent_since.is_none()
            && self.gone_since.is_none()
            && self.known_since.is_none()
            && self.present_since.is_none()
    }
}

#[derive(Default)]
struct AbsenceState {
    members: HashMap<NodeId, MemberRecord>,
}

/// Cheap-to-clone view of which recently known members are absent, fed by
/// [`tracking_task`] and read by `tombstone_gc_task` via [`should_defer_gc`].
#[derive(Clone, Default)]
pub(crate) struct AbsenceTracker {
    state: Arc<StdMutex<AbsenceState>>,
}

impl AbsenceTracker {
    /// One lock acquisition for every method below.
    fn state(&self) -> MutexGuard<'_, AbsenceState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Applies one membership snapshot: a member newly missing from `live`
    /// is tracked gone, and absent too unless it gossiped a graceful
    /// departure; a member back in `live` clears both and restarts its
    /// presence timer.
    pub(crate) fn observe(&self, live: &HashMap<NodeId, LiveFlags>) {
        let mut state = self.state();
        let departed: Vec<NodeId> = state
            .members
            .iter()
            .filter(|(node, record)| record.present_since.is_some() && !live.contains_key(node))
            .map(|(&node, _)| node)
            .collect();
        for node in departed {
            let record = state
                .members
                .get_mut(&node)
                .expect("invariant: a departing node is live in this same map");
            if counts_as_absent(record.flags.departing) {
                record.absent_since.get_or_insert_with(Instant::now);
            }
            record.gone_since.get_or_insert_with(Instant::now);
            record.present_since = None;
        }
        for (&node, &flags) in live {
            let record = state.members.entry(node).or_default();
            record.absent_since = None;
            record.gone_since = None;
            record.present_since.get_or_insert_with(Instant::now);
            record.known_since.get_or_insert_with(Instant::now);
            record.flags = flags;
        }
    }

    /// Forgets every member gone longer than `horizon`. The CRDT sweep
    /// calls this with the fold receipt lifetime, past which nothing
    /// reconciles a straggling copy of the member's writer either.
    pub(crate) fn prune_gone_older_than(&self, horizon: Duration) {
        let mut state = self.state();
        let now = Instant::now();
        for record in state.members.values_mut() {
            if record
                .gone_since
                .is_some_and(|since| now.saturating_duration_since(since) > horizon)
            {
                record.gone_since = None;
                record.known_since = None;
            }
        }
        state.members.retain(|_, record| !record.is_empty());
    }

    /// Whether any recently known member is absent and not yet aged past
    /// `hard_cap`. Prunes older entries as a side effect.
    pub(crate) fn any_absent(&self, hard_cap: Duration) -> bool {
        let mut state = self.state();
        let now = Instant::now();
        for record in state.members.values_mut() {
            if record
                .absent_since
                .is_some_and(|since| now.saturating_duration_since(since) >= hard_cap)
            {
                record.absent_since = None;
            }
        }
        state.members.retain(|_, record| !record.is_empty());
        state
            .members
            .values()
            .any(|record| record.absent_since.is_some())
    }

    /// Every recently known member absent and not yet aged past `hard_cap`,
    /// for [`should_defer_gc`]'s `Mode::Distributed` rule. Prunes older
    /// entries as a side effect, same as [`AbsenceTracker::any_absent`].
    pub(crate) fn absent_nodes(&self, hard_cap: Duration) -> Vec<NodeId> {
        let mut state = self.state();
        let now = Instant::now();
        for record in state.members.values_mut() {
            if record
                .absent_since
                .is_some_and(|since| now.saturating_duration_since(since) >= hard_cap)
            {
                record.absent_since = None;
            }
        }
        state.members.retain(|_, record| !record.is_empty());
        state
            .members
            .iter()
            .filter(|(_, record)| record.absent_since.is_some())
            .map(|(&node, _)| node)
            .collect()
    }

    /// Every member gone at least `retire_after`, crashed or graceful: the
    /// members whose writer incarnations the CRDT sweep may retire. Reads
    /// the never-pruned `gone_since` field, so unlike
    /// [`AbsenceTracker::any_absent`]/[`AbsenceTracker::absent_nodes`] it
    /// never ages an absence out.
    pub(crate) fn gone_longer_than(&self, retire_after: Duration) -> Vec<NodeId> {
        let state = self.state();
        let now = Instant::now();
        state
            .members
            .iter()
            .filter(|(_, record)| {
                record
                    .gone_since
                    .is_some_and(|since| now.saturating_duration_since(since) >= retire_after)
            })
            .map(|(&node, _)| node)
            .collect()
    }

    /// This member's continuous-presence start, if live and tracked; `None`
    /// if never observed live or currently gone.
    pub(crate) fn present_since(&self, node: NodeId) -> Option<Instant> {
        self.state()
            .members
            .get(&node)
            .and_then(|r| r.present_since)
    }

    /// When this member last left the live set; `None` if live or unknown.
    pub(crate) fn gone_since(&self, node: NodeId) -> Option<Instant> {
        self.state().members.get(&node).and_then(|r| r.gone_since)
    }

    /// When this member is first observed live; `None` until then, or once
    /// [`AbsenceTracker::prune_gone_older_than`] forgets it.
    pub(crate) fn known_since(&self, node: NodeId) -> Option<Instant> {
        self.state().members.get(&node).and_then(|r| r.known_since)
    }
}

/// Whether `tombstone_gc_task` defers a tombstone past `tombstone_ttl` this
/// tick. `Mode::Replicated` defers on any absence at all; `Mode::Distributed`
/// narrows this to an absent member that currently co-owns a bucket this
/// node also owns. `Mode::Local`/`Mode::Invalidation` never defer.
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

/// Whether a node that dropped out of the live set counts as absent: true
/// unless `departed_gracefully` shows it gossiped a departure first.
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
        let Some(changed) = cancel.run_until_cancelled(live.changed()).await else {
            return;
        };
        if changed.is_err() {
            return; // membership shut down
        }
        tracker.observe(&live.borrow_and_update());
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    /// The hard cap most tests use: far longer than any sleep in this file.
    const HOUR: Duration = Duration::from_secs(3600);

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
        assert!(!tracker.any_absent(HOUR));
    }

    #[test]
    fn a_peer_that_leaves_the_live_set_is_tracked_absent() {
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(1, false)]));
        assert!(!tracker.any_absent(HOUR), "still live: not absent");

        tracker.observe(&live(&[]));
        assert!(
            tracker.any_absent(HOUR),
            "dropped out of the live set without gossiping a departure: now tracked absent"
        );
    }

    #[test]
    fn a_returning_peer_clears_its_tracked_absence() {
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(1, false)]));
        tracker.observe(&live(&[]));
        assert!(tracker.any_absent(HOUR));

        tracker.observe(&live(&[(1, false)]));
        assert!(
            !tracker.any_absent(HOUR),
            "a live member is not tracked absent"
        );
    }

    #[test]
    fn a_peer_that_gossiped_departing_before_leaving_is_never_tracked_absent() {
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(1, true)]));

        tracker.observe(&live(&[]));
        assert!(
            !tracker.any_absent(HOUR),
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
            !tracker.any_absent(HOUR),
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
            "not continuously present anymore"
        );
    }

    #[test]
    fn a_crash_is_gone_and_absent() {
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(1, false)]));
        tracker.observe(&live(&[]));
        assert!(tracker.any_absent(HOUR));
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
    fn known_since_is_first_observation_and_survives_flapping() {
        let tracker = AbsenceTracker::default();
        assert!(tracker.known_since(NodeId::from(1)).is_none());
        tracker.observe(&live(&[(1, false)]));
        let first = tracker
            .known_since(NodeId::from(1))
            .expect("known once seen live");
        std::thread::sleep(Duration::from_millis(5));
        tracker.observe(&live(&[]));
        tracker.observe(&live(&[(1, false)]));
        assert_eq!(
            tracker.known_since(NodeId::from(1)),
            Some(first),
            "leaving and returning never moves the first observation"
        );
        assert!(
            tracker.present_since(NodeId::from(1)).expect("live again") > first,
            "continuous presence restarted on return"
        );
    }

    #[test]
    fn prune_gone_older_than_forgets_only_members_gone_past_the_horizon() {
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(1, false), (2, true)]));
        tracker.observe(&live(&[(2, true)]));
        std::thread::sleep(Duration::from_millis(20));
        tracker.observe(&live(&[]));
        tracker.prune_gone_older_than(Duration::from_millis(10));
        assert!(
            tracker.gone_since(NodeId::from(1)).is_none()
                && tracker.known_since(NodeId::from(1)).is_none(),
            "gone past the horizon: forgotten entirely"
        );
        assert!(
            tracker.gone_since(NodeId::from(2)).is_some(),
            "gone for less than the horizon: still tracked"
        );
        assert!(
            tracker.any_absent(HOUR),
            "pruning never touches tombstone absence"
        );
    }

    #[test]
    fn a_gone_member_outlives_the_tombstone_hard_cap() {
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(1, false)]));
        tracker.observe(&live(&[]));
        let tiny_cap = Duration::from_millis(1);
        std::thread::sleep(Duration::from_millis(20));
        assert!(
            !tracker.any_absent(tiny_cap),
            "the absence aged out of tombstone deferral"
        );
        assert!(
            tracker.gone_since(NodeId::from(1)).is_some(),
            "but the member stays gone for writer retirement until it returns"
        );
        assert_eq!(tracker.gone_longer_than(tiny_cap), vec![NodeId::from(1)]);
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

        assert!(should_defer_gc(Mode::Replicated, &tracker, HOUR, None));
        assert!(!should_defer_gc(Mode::Local, &tracker, HOUR, None));
        assert!(!should_defer_gc(Mode::Invalidation, &tracker, HOUR, None));
    }

    #[test]
    fn should_defer_gc_is_false_for_replicated_mode_with_no_absence() {
        let tracker = AbsenceTracker::default();
        assert!(!should_defer_gc(Mode::Replicated, &tracker, HOUR, None));
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

        assert!(!should_defer_gc(mode, &tracker, HOUR, None));
    }

    #[test]
    fn should_defer_gc_ignores_an_absent_member_sharing_no_owned_bucket() {
        let self_node = NodeId::from(1);
        // Self is the only eligible node, so the absent stranger (node 99) owns nothing.
        let view = distributed_view(self_node, vec![self_node]);
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(99, false)]));
        tracker.observe(&live(&[]));
        let mode = Mode::Distributed {
            owners: std::num::NonZeroU8::new(2).expect("nonzero"),
        };

        assert!(!should_defer_gc(mode, &tracker, HOUR, Some(&view)));
    }

    #[test]
    fn should_defer_gc_defers_for_an_absent_member_sharing_an_owned_bucket() {
        let self_node = NodeId::from(1);
        let co_owner = NodeId::from(2);
        // Both nodes eligible with owners=2: every bucket's owners are {self_node, co_owner}.
        let view = distributed_view(self_node, vec![self_node, co_owner]);
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(2, false)]));
        tracker.observe(&live(&[]));
        let mode = Mode::Distributed {
            owners: std::num::NonZeroU8::new(2).expect("nonzero"),
        };

        assert!(should_defer_gc(mode, &tracker, HOUR, Some(&view)));
    }

    #[test]
    fn present_since_is_not_reset_by_jitter_that_never_reaches_the_detector() {
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(1, false)]));
        let first = tracker
            .present_since(NodeId::from(1))
            .expect("live member is tracked present");

        // The member stays live through this jitter, so its presence start
        // time must not move forward.
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
        // HOUR is far from tripped: a concurrent `any_absent` read at that
        // cap must not prune the entry this test reads next.
        std::thread::sleep(Duration::from_millis(20));

        assert_eq!(
            tracker.gone_longer_than(retire_after),
            vec![NodeId::from(1)]
        );
        assert!(
            tracker.any_absent(HOUR),
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
            tracker.gone_longer_than(HOUR),
            Vec::new(),
            "a member absent for less than the bound is not yet retirement-eligible"
        );
    }
}

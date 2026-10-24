//! Partition-aware tombstone retention. Tracks which recently known members
//! are absent from the live peer set, so tombstone collection defers while a
//! member that may still hold the deleted entry is unreachable. Collecting
//! anyway lets anti-entropy resurrect the entry when that member returns.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use crate::config::ClusterConfig;
use crate::membership::LiveFlags;
use crate::node::NodeId;
use crate::ownership::OwnershipView;
use crate::store::{Mode, gc_period_ms};

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
    /// Set when an absent member comes back live: tombstone collection
    /// keeps deferring for a settle bound after it, until anti-entropy has
    /// had time to hand the member the deletes made while it was away.
    returned_since: Option<Instant>,
    /// Set when an absent member comes back live, and never aged by
    /// tombstone collection: an ownership view built while it was away can
    /// still be the one a caller acts on. Cleared by
    /// [`AbsenceTracker::forget_departed`] once older than
    /// [`ForgetPolicy::back`].
    back_since: Option<Instant>,
}

impl MemberRecord {
    /// Whether every field is unset: nothing here is worth keeping.
    fn is_empty(&self) -> bool {
        self.absent_since.is_none()
            && self.gone_since.is_none()
            && self.known_since.is_none()
            && self.present_since.is_none()
            && self.returned_since.is_none()
            && self.back_since.is_none()
    }

    /// This record after a forgetting pass at `now`: a member gone longer
    /// than `gone` loses `gone_since` and `known_since`, and a return older
    /// than `back` loses `back_since`. `absent_since` and `returned_since`
    /// belong to tombstone collection ([`AbsenceTracker::holding_gc`] ages
    /// them), and the marks of a live member are not departure marks, so
    /// all of those stay. A record left [empty](Self::is_empty) is dropped
    /// by the caller. Pure; unit tested directly.
    fn forgotten(mut self, now: Instant, gone: Duration, back: Duration) -> Self {
        let older_than = |since: Option<Instant>, horizon: Duration| {
            since.is_some_and(|since| now.saturating_duration_since(since) > horizon)
        };
        if older_than(self.gone_since, gone) {
            self.gone_since = None;
            self.known_since = None;
        }
        if older_than(self.back_since, back) {
            self.back_since = None;
        }
        self
    }
}

/// How long the tracker keeps what it knows about a member that left, and
/// how often [`AbsenceTracker::forget_departed`] looks. The default keeps
/// everything until [`AbsenceTracker::prune_gone_older_than`] is called.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ForgetPolicy {
    /// A member gone longer than this loses `gone_since` and `known_since`:
    /// the CRDT fold receipt lifetime, the span the writer-retirement sweep
    /// reads them for.
    gone: Duration,
    /// A return older than this loses `back_since`. At least `gone`, and at
    /// least the `state_transfer_budget` every
    /// [`AbsenceTracker::away_within`] caller passes, so no reader loses a
    /// mark it still counts.
    back: Duration,
    /// The shortest time between two passes: half a tombstone gc period, so
    /// the gc tasks of one tick, one for each cache and all sharing the
    /// tracker, walk its records once between them.
    every: Duration,
}

impl ForgetPolicy {
    /// The policy for a CRDT fold receipt living `receipt_lifetime`,
    /// `away_within` callers passing `transfer_budget`, and gc tasks ticking
    /// every `gc_period`. Pure; unit tested directly.
    pub(crate) fn new(
        receipt_lifetime: Duration,
        transfer_budget: Duration,
        gc_period: Duration,
    ) -> Self {
        Self {
            gone: receipt_lifetime,
            back: receipt_lifetime.max(transfer_budget),
            every: gc_period / 2,
        }
    }

    /// The policy `config` implies: the receipt lifetime the CRDT sweep
    /// prunes with ([`ClusterConfig::crdt_compaction_bounds`]), the
    /// `state_transfer_budget`, and the period `tombstone_gc_task` ticks on.
    pub(crate) fn for_config(config: &ClusterConfig) -> Self {
        let ttl_ms = u64::try_from(config.tombstone_ttl.as_millis()).unwrap_or(u64::MAX);
        Self::new(
            Duration::from_millis(config.crdt_compaction_bounds().receipt_ttl_ms),
            config.state_transfer_budget,
            Duration::from_millis(gc_period_ms(ttl_ms)),
        )
    }
}

impl Default for ForgetPolicy {
    fn default() -> Self {
        Self {
            gone: Duration::MAX,
            back: Duration::MAX,
            every: Duration::MAX,
        }
    }
}

/// The capacity from which the member map shrinks after a drop.
const MEMBERS_SHRINK_FLOOR: usize = 256;

/// Whether a member map of `len` records in `capacity` slots gives memory
/// back: from [`MEMBERS_SHRINK_FLOOR`] slots up, once a quarter is in use.
const fn members_oversized(len: usize, capacity: usize) -> bool {
    capacity >= MEMBERS_SHRINK_FLOOR && len <= capacity / 4
}

/// Whether a forgetting pass runs at `now`: on the first call, then once
/// `every` has passed since the last pass at `forgot_at`. Pure; unit tested
/// directly.
fn forget_is_due(forgot_at: Option<Instant>, now: Instant, every: Duration) -> bool {
    forgot_at.is_none_or(|at| now.saturating_duration_since(at) >= every)
}

#[derive(Default)]
struct AbsenceState {
    members: HashMap<NodeId, MemberRecord>,
    /// When the last [`AbsenceTracker::forget_departed`] pass ran.
    forgot_at: Option<Instant>,
}

impl AbsenceState {
    /// Applies [`MemberRecord::forgotten`] to every record at `now`, drops
    /// the records left empty, and gives the map's memory back after a
    /// large drop.
    fn forget(&mut self, now: Instant, gone: Duration, back: Duration) {
        for record in self.members.values_mut() {
            *record = record.forgotten(now, gone, back);
        }
        self.members.retain(|_, record| !record.is_empty());
        if members_oversized(self.members.len(), self.members.capacity()) {
            self.members.shrink_to_fit();
        }
    }
}

/// Cheap-to-clone view of which recently known members are absent, fed
/// every live set membership publishes, before its peers and cache modes,
/// and read by `tombstone_gc_task` via [`should_defer_gc`]. That task also
/// calls [`AbsenceTracker::forget_departed`], so a cluster whose caches
/// never merge forgets departed members too.
#[derive(Clone, Default)]
pub(crate) struct AbsenceTracker {
    state: Arc<StdMutex<AbsenceState>>,
    policy: ForgetPolicy,
}

impl AbsenceTracker {
    /// A tracker whose [`AbsenceTracker::forget_departed`] follows `policy`.
    pub(crate) fn new(policy: ForgetPolicy) -> Self {
        Self {
            state: Arc::default(),
            policy,
        }
    }

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
            if record.absent_since.take().is_some() {
                let now = Instant::now();
                record.returned_since = Some(now);
                record.back_since = Some(now);
            }
            record.gone_since = None;
            record.present_since.get_or_insert_with(Instant::now);
            record.known_since.get_or_insert_with(Instant::now);
            record.flags = flags;
        }
    }

    /// Forgets every member gone longer than `horizon`. The CRDT sweep
    /// calls this with the fold receipt lifetime, which a fold's receipt
    /// outlasts only by the time its fold trails the member's departure.
    /// [`AbsenceTracker::forget_departed`] applies the same rule to every
    /// cluster, merging caches or not.
    pub(crate) fn prune_gone_older_than(&self, horizon: Duration) {
        self.state().forget(Instant::now(), horizon, Duration::MAX);
    }

    /// Forgets what the [`ForgetPolicy`] says is spent: a member gone longer
    /// than the CRDT fold receipt lifetime, and a return older than that
    /// lifetime and the `state_transfer_budget`. Every cache's
    /// `tombstone_gc_task` calls this each tick. The tracker is one for the
    /// cluster, so the first call of a tick walks the records and the
    /// others, for the rest of the cache tasks of that tick, return at once.
    /// No reader sees a difference: the CRDT sweep reads `gone_since` and
    /// `known_since` for a member only inside the receipt lifetime, and
    /// [`AbsenceTracker::away_within`] counts `back_since` only inside the
    /// bound its callers pass.
    pub(crate) fn forget_departed(&self) {
        self.forget_departed_at(Instant::now());
    }

    /// [`AbsenceTracker::forget_departed`] at `now`.
    fn forget_departed_at(&self, now: Instant) {
        let mut state = self.state();
        if !forget_is_due(state.forgot_at, now, self.policy.every) {
            return;
        }
        state.forgot_at = Some(now);
        state.forget(now, self.policy.gone, self.policy.back);
    }

    /// How many members the tracker holds a record of.
    #[cfg(test)]
    pub(crate) fn record_count(&self) -> usize {
        self.state().members.len()
    }

    /// Whether any recently known member is absent and not yet aged past
    /// `hard_cap`. Prunes older entries as a side effect.
    #[cfg(test)]
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

    /// Every member that holds tombstone collection back: absent and not yet
    /// aged past `hard_cap`, or back from an absence for less than `settle`.
    /// Prunes aged entries as a side effect.
    pub(crate) fn holding_gc(&self, hard_cap: Duration, settle: Duration) -> Vec<NodeId> {
        let mut state = self.state();
        let now = Instant::now();
        let aged = |since: Option<Instant>, bound: Duration| {
            since.is_some_and(|since| now.saturating_duration_since(since) >= bound)
        };
        for record in state.members.values_mut() {
            if aged(record.absent_since, hard_cap) {
                record.absent_since = None;
            }
            if aged(record.returned_since, settle) {
                record.returned_since = None;
            }
        }
        state.members.retain(|_, record| !record.is_empty());
        state
            .members
            .iter()
            .filter(|(_, record)| record.absent_since.is_some() || record.returned_since.is_some())
            .map(|(&node, _)| node)
            .collect()
    }

    /// Every member away less than `bound` ago: dropped out of the live set
    /// without a graceful departure and either not back since, or back for
    /// less than `bound`. A member that returned still counts, since an
    /// ownership view built while it was gone can still be the one the
    /// caller acts on. A read only: unlike [`AbsenceTracker::holding_gc`]
    /// it ages nothing out, so a shorter `bound` here never shortens
    /// tombstone retention.
    pub(crate) fn away_within(&self, bound: Duration) -> Vec<NodeId> {
        let state = self.state();
        let now = Instant::now();
        let recent = |since: Option<Instant>| {
            since.is_some_and(|since| now.saturating_duration_since(since) < bound)
        };
        state
            .members
            .iter()
            .filter(|(_, record)| recent(record.absent_since) || recent(record.back_since))
            .map(|(&node, _)| node)
            .collect()
    }

    /// Every member gone at least `retire_after`, crashed or graceful: the
    /// members whose writer incarnations the CRDT sweep may retire. Reads
    /// the never-pruned `gone_since` field, so unlike
    /// [`AbsenceTracker::holding_gc`] it never ages an absence out.
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

/// Whether the members absent or only just back make `tombstone_gc_task`
/// defer a tombstone past `tombstone_ttl` this tick: [`defers_for`] over the
/// members [`AbsenceTracker::holding_gc`] names, absent up to `hard_cap` or
/// back for less than `settle`. The task also defers while the cache is new;
/// see `gc_defers` in the parent module.
pub(crate) fn should_defer_gc(
    mode: Mode,
    tracker: &AbsenceTracker,
    hard_cap: Duration,
    settle: Duration,
    ownership: Option<&OwnershipView>,
) -> bool {
    defers_for(mode, &tracker.holding_gc(hard_cap, settle), ownership)
}

/// Whether `holders`, members absent or only just back, defer tombstone
/// collection. `Mode::Replicated` defers for any of them. `Mode::Distributed`
/// defers for one that co-owns a part with this node once it is eligible
/// again: the current view drops an absent member, yet a member alive on
/// the far side of a false partition still holds its copy of every part it
/// owned, and a delete made here meanwhile never reached it.
/// `Mode::Local`/`Mode::Invalidation` never defer.
fn defers_for(mode: Mode, holders: &[NodeId], ownership: Option<&OwnershipView>) -> bool {
    match mode {
        Mode::Replicated => !holders.is_empty(),
        Mode::Distributed { .. } => {
            let Some(view) = ownership else {
                return false;
            };
            if holders.is_empty() {
                return false;
            }
            let co_owners = view.co_owners_with(holders);
            holders.iter().any(|node| co_owners.contains(node))
        }
        Mode::Local | Mode::Invalidation => false,
    }
}

/// Whether a node that dropped out of the live set counts as absent: true
/// unless `departed_gracefully` shows it gossiped a departure first.
fn counts_as_absent(departed_gracefully: bool) -> bool {
    !departed_gracefully
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
        let gone = tracker.gone_longer_than(Duration::ZERO);
        assert!(gone.is_empty(), "{gone:?}");
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

        assert!(should_defer_gc(
            Mode::Replicated,
            &tracker,
            HOUR,
            HOUR,
            None
        ));
        assert!(!should_defer_gc(Mode::Local, &tracker, HOUR, HOUR, None));
        assert!(!should_defer_gc(
            Mode::Invalidation,
            &tracker,
            HOUR,
            HOUR,
            None
        ));
    }

    #[test]
    fn should_defer_gc_is_false_for_replicated_mode_with_no_absence() {
        let tracker = AbsenceTracker::default();
        assert!(!should_defer_gc(
            Mode::Replicated,
            &tracker,
            HOUR,
            HOUR,
            None
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

        assert!(!should_defer_gc(mode, &tracker, HOUR, HOUR, None));
    }

    #[test]
    fn should_defer_gc_ignores_an_absent_member_sharing_no_owned_bucket() {
        let self_node = NodeId::from(1);
        let everyone: Vec<NodeId> = (1..=200).map(NodeId::from).collect();
        let full = distributed_view(self_node, everyone.clone());
        let stranger_id = (2..=200u64)
            .find(|&id| !full.co_owners().contains(&NodeId::from(id)))
            .expect("some node shares no bucket with self");
        let stranger = NodeId::from(stranger_id);
        // The current view already dropped the absent stranger.
        let view = distributed_view(
            self_node,
            everyone.into_iter().filter(|&n| n != stranger).collect(),
        );
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(stranger_id, false)]));
        tracker.observe(&live(&[]));
        let mode = Mode::Distributed {
            owners: std::num::NonZeroU8::new(2).expect("nonzero"),
        };

        assert!(!should_defer_gc(mode, &tracker, HOUR, HOUR, Some(&view)));
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

        assert!(should_defer_gc(mode, &tracker, HOUR, HOUR, Some(&view)));
    }

    /// A survivor keeps deferring GC for an absent co-owner after its view
    /// reassigns that co-owner's parts. The view drops an absent member on
    /// the failure detector's timescale, yet a member alive on the far side
    /// of a false partition still holds every part it owned and missed the
    /// deletes made here meanwhile. Only the absence aging past the hard
    /// cap ends the deferral.
    #[test]
    fn should_defer_gc_keeps_protecting_an_absent_co_owner_after_ownership_reassigns_its_parts() {
        let self_node = NodeId::from(1);
        let absent = NodeId::from(2);
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(2, false)]));
        tracker.observe(&live(&[]));
        let mode = Mode::Distributed {
            owners: std::num::NonZeroU8::new(2).expect("nonzero"),
        };

        let view_before = distributed_view(self_node, vec![self_node, absent]);
        assert!(
            should_defer_gc(mode, &tracker, HOUR, HOUR, Some(&view_before)),
            "absent still co-owns every bucket, so its absence defers GC"
        );

        // The failure detector's recompute drops `absent`: this node now
        // owns every part alone.
        let view_after = distributed_view(self_node, vec![self_node]);
        assert!(
            should_defer_gc(mode, &tracker, HOUR, HOUR, Some(&view_after)),
            "absent co-owns every part once it is eligible again, so GC still defers"
        );

        let tiny_cap = Duration::from_millis(1);
        std::thread::sleep(Duration::from_millis(5));
        assert!(
            !should_defer_gc(mode, &tracker, tiny_cap, HOUR, Some(&view_after)),
            "an absence past the hard cap no longer defers"
        );
    }

    #[test]
    fn defers_for_a_distributed_holder_only_when_it_co_owns_a_part_once_eligible_again() {
        let mode = Mode::Distributed {
            owners: std::num::NonZeroU8::new(2).expect("nonzero"),
        };
        let self_node = NodeId::from(1);
        // Two hundred nodes, two owners per bucket: self owns about ten
        // buckets, so most nodes share none with it.
        let others: Vec<NodeId> = (2..=200).map(NodeId::from).collect();
        let mut eligible = others.clone();
        eligible.push(self_node);
        let full = distributed_view(self_node, eligible);
        let sharing = *full
            .co_owners()
            .first()
            .expect("self shares some bucket with another node");
        let stranger = *others
            .iter()
            .find(|node| !full.co_owners().contains(node))
            .expect("some node shares no bucket with self");

        // The current view dropped both: they are absent.
        let remaining: Vec<NodeId> = std::iter::once(self_node)
            .chain(
                others
                    .iter()
                    .copied()
                    .filter(|&n| n != sharing && n != stranger),
            )
            .collect();
        let view = distributed_view(self_node, remaining);

        assert!(defers_for(mode, &[sharing], Some(&view)));
        assert!(!defers_for(mode, &[stranger], Some(&view)));
        assert!(defers_for(mode, &[stranger, sharing], Some(&view)));
        assert!(!defers_for(mode, &[], Some(&view)));
        assert!(!defers_for(mode, &[sharing], None), "no view attached yet");
        assert!(defers_for(Mode::Replicated, &[stranger], None));
        assert!(!defers_for(Mode::Replicated, &[], None));
        assert!(!defers_for(Mode::Local, &[sharing], Some(&view)));
    }

    #[test]
    fn holding_gc_names_an_absent_member_until_the_hard_cap() {
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(1, false), (2, true)]));
        tracker.observe(&live(&[]));

        assert_eq!(
            tracker.holding_gc(HOUR, HOUR),
            vec![NodeId::from(1)],
            "the crashed member holds GC; the graceful leaver does not"
        );
        std::thread::sleep(Duration::from_millis(5));
        assert!(
            tracker
                .holding_gc(Duration::from_millis(1), HOUR)
                .is_empty(),
            "an absence past the hard cap no longer holds GC"
        );
    }

    #[test]
    fn holding_gc_names_a_returned_member_until_it_settles() {
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(1, false)]));
        tracker.observe(&live(&[]));
        tracker.observe(&live(&[(1, false)]));

        assert!(!tracker.any_absent(HOUR), "the member is back");
        assert_eq!(
            tracker.holding_gc(HOUR, HOUR),
            vec![NodeId::from(1)],
            "a member just back from an absence still holds GC"
        );
        std::thread::sleep(Duration::from_millis(5));
        assert!(
            tracker
                .holding_gc(HOUR, Duration::from_millis(1))
                .is_empty(),
            "once settled it no longer does"
        );
    }

    #[test]
    fn away_within_names_a_crashed_member_until_the_bound_and_prunes_nothing() {
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(1, false), (2, true), (3, false)]));
        tracker.observe(&live(&[(3, false)]));

        assert_eq!(
            tracker.away_within(HOUR),
            vec![NodeId::from(1)],
            "the crashed member is away; the graceful leaver and the live one are not"
        );
        std::thread::sleep(Duration::from_millis(5));
        assert!(
            tracker.away_within(Duration::from_millis(1)).is_empty(),
            "past the bound it is no longer named"
        );
        assert_eq!(
            tracker.holding_gc(HOUR, HOUR),
            vec![NodeId::from(1)],
            "a short bound here leaves tombstone retention untouched"
        );
    }

    #[test]
    fn away_within_keeps_naming_a_member_that_came_back_until_the_bound() {
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(1, false), (2, false)]));
        tracker.observe(&live(&[(2, false)]));
        tracker.observe(&live(&[(1, false), (2, false)]));

        assert_eq!(
            tracker.away_within(HOUR),
            vec![NodeId::from(1)],
            "a view built while it was gone can still be the one acted on"
        );
        assert!(
            tracker
                .holding_gc(Duration::ZERO, Duration::ZERO)
                .is_empty(),
            "tombstone collection ages its own marks out"
        );
        assert_eq!(
            tracker.away_within(HOUR),
            vec![NodeId::from(1)],
            "and never the return this bound reads"
        );
        std::thread::sleep(Duration::from_millis(5));
        assert!(
            tracker.away_within(Duration::from_millis(1)).is_empty(),
            "past the bound it is no longer named"
        );
    }

    #[test]
    fn holding_gc_ignores_a_member_that_was_never_absent() {
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(1, false)]));
        tracker.observe(&live(&[(1, false)]));
        let pruned = tracker.holding_gc(HOUR, HOUR);
        assert!(pruned.is_empty(), "{pruned:?}");

        // A graceful leave and return is never an absence either.
        tracker.observe(&live(&[(1, true)]));
        tracker.observe(&live(&[]));
        tracker.observe(&live(&[(1, false)]));
        let pruned_again = tracker.holding_gc(HOUR, HOUR);
        assert!(pruned_again.is_empty(), "{pruned_again:?}");
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

    /// The policy most forgetting tests use: a gone member is forgotten
    /// after 10 s, a return after 30 s, and a pass runs at most every second.
    fn policy() -> ForgetPolicy {
        ForgetPolicy::new(
            Duration::from_secs(10),
            Duration::from_secs(30),
            Duration::from_secs(2),
        )
    }

    /// An instant `secs` seconds from now, for a pass that runs that much
    /// later than the records it reads were stamped.
    fn after(secs: u64) -> Instant {
        Instant::now() + Duration::from_secs(secs)
    }

    #[test]
    fn a_member_that_returned_and_left_for_good_is_forgotten() {
        let tracker = AbsenceTracker::new(policy());
        tracker.observe(&live(&[(1, false)]));
        tracker.observe(&live(&[]));
        tracker.observe(&live(&[(1, false)]));
        tracker.observe(&live(&[]));
        tracker.holding_gc(Duration::ZERO, Duration::ZERO);
        tracker.forget_departed_at(after(31));
        let held = tracker.record_count();
        assert_eq!(
            held, 0,
            "a member gone past every horizon is forgotten; {held} records remain"
        );
    }

    #[test]
    fn departed_members_are_forgotten_without_the_crdt_sweep() {
        let tracker = AbsenceTracker::new(policy());
        for node in 0..1000u64 {
            tracker.observe(&live(&[(node, false)]));
        }
        tracker.observe(&live(&[]));
        tracker.holding_gc(Duration::ZERO, Duration::ZERO);
        tracker.forget_departed_at(after(11));
        let held = tracker.record_count();
        assert_eq!(
            held, 0,
            "departed members are bounded without the CRDT sweep; {held} records remain"
        );
        assert!(
            tracker.state().members.capacity() < MEMBERS_SHRINK_FLOOR,
            "the map gives its memory back after the drop"
        );
    }

    #[test]
    fn a_return_outlasts_the_gone_horizon_by_the_larger_bound() {
        let tracker = AbsenceTracker::new(policy());
        tracker.observe(&live(&[(1, false)]));
        tracker.observe(&live(&[]));
        tracker.observe(&live(&[(1, false)]));
        tracker.observe(&live(&[]));
        tracker.holding_gc(Duration::ZERO, Duration::ZERO);

        tracker.forget_departed_at(after(11));
        assert_eq!(
            tracker.record_count(),
            1,
            "past the gone horizon, inside the return bound: the return mark stays"
        );
        assert!(
            tracker.gone_since(NodeId::from(1)).is_none()
                && tracker.known_since(NodeId::from(1)).is_none(),
            "while the departure marks are forgotten"
        );

        tracker.forget_departed_at(after(32));
        assert_eq!(tracker.record_count(), 0, "past the return bound it goes");
    }

    /// Every record a reader still counts survives a pass: a crashed member
    /// is named by `holding_gc`, a member just back by `away_within`, and a
    /// member gone inside the receipt lifetime by the CRDT sweep's reads.
    #[test]
    fn forgetting_keeps_a_record_inside_a_horizon_a_reader_uses() {
        let tracker = AbsenceTracker::new(policy());
        // 1: crashed and still absent; 2: crashed, back, live again; 3: gone.
        tracker.observe(&live(&[(1, false), (2, false), (3, true)]));
        tracker.observe(&live(&[(2, false)]));
        tracker.observe(&live(&[]));
        tracker.observe(&live(&[(2, false)]));
        tracker.forget_departed_at(after(5));

        assert_eq!(tracker.record_count(), 3);
        let mut holding = tracker.holding_gc(HOUR, HOUR);
        holding.sort_by_key(|node| node.as_u64());
        assert_eq!(
            holding,
            vec![NodeId::from(1), NodeId::from(2)],
            "tombstone collection still waits on both"
        );
        let mut away = tracker.away_within(HOUR);
        away.sort_by_key(|node| node.as_u64());
        assert_eq!(
            away,
            vec![NodeId::from(1), NodeId::from(2)],
            "the absence and the return are both still counted"
        );
        let mut gone = tracker.gone_longer_than(Duration::ZERO);
        gone.sort_by_key(|node| node.as_u64());
        assert_eq!(
            gone,
            vec![NodeId::from(1), NodeId::from(3)],
            "the sweep still sees both departures"
        );
        assert!(tracker.known_since(NodeId::from(3)).is_some());
        assert!(tracker.present_since(NodeId::from(2)).is_some());
    }

    #[test]
    fn a_live_member_is_never_forgotten() {
        let tracker = AbsenceTracker::new(policy());
        tracker.observe(&live(&[(1, false)]));
        tracker.forget_departed_at(after(24 * 3600));
        assert_eq!(tracker.record_count(), 1);
        assert!(tracker.known_since(NodeId::from(1)).is_some());
    }

    #[test]
    fn forgetting_runs_at_most_once_per_interval() {
        let tracker = AbsenceTracker::new(policy());
        tracker.forget_departed_at(after(11));
        tracker.observe(&live(&[(1, true)]));
        tracker.observe(&live(&[]));

        tracker.forget_departed_at(after(11) + Duration::from_millis(500));
        assert_eq!(
            tracker.record_count(),
            1,
            "a second caller in the same tick does not walk the records"
        );
        tracker.forget_departed_at(after(12));
        assert_eq!(tracker.record_count(), 0, "the next tick does");
    }

    #[test]
    fn a_default_tracker_forgets_nothing_on_its_own() {
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(1, true)]));
        tracker.observe(&live(&[]));
        tracker.forget_departed_at(after(24 * 3600 * 365));
        assert_eq!(tracker.record_count(), 1);
    }

    #[test]
    fn prune_gone_older_than_leaves_the_return_marks() {
        let tracker = AbsenceTracker::default();
        tracker.observe(&live(&[(1, false)]));
        tracker.observe(&live(&[]));
        tracker.observe(&live(&[(1, false)]));
        tracker.observe(&live(&[]));
        tracker.holding_gc(Duration::ZERO, Duration::ZERO);
        std::thread::sleep(Duration::from_millis(5));
        tracker.prune_gone_older_than(Duration::from_millis(1));
        assert_eq!(
            tracker.away_within(HOUR),
            vec![NodeId::from(1)],
            "the CRDT sweep's prune does not touch what only `away_within` reads"
        );
    }

    /// The ages, in seconds before a pass, of the marks a record holds.
    #[derive(Clone, Copy, Default)]
    struct Ages {
        absent: Option<u64>,
        gone: Option<u64>,
        known: Option<u64>,
        present: Option<u64>,
        returned: Option<u64>,
        back: Option<u64>,
    }

    impl Ages {
        /// The record whose marks are that old at `now`.
        fn record(self, now: Instant) -> MemberRecord {
            let at = |age: Option<u64>| {
                age.map(|age| {
                    now.checked_sub(Duration::from_secs(age))
                        .expect("a test pass reads far ahead of the clock's start")
                })
            };
            MemberRecord {
                flags: LiveFlags::default(),
                absent_since: at(self.absent),
                gone_since: at(self.gone),
                known_since: at(self.known),
                present_since: at(self.present),
                returned_since: at(self.returned),
                back_since: at(self.back),
            }
        }
    }

    /// Whether `record` holds exactly the marks `ages` names, at `now`.
    fn holds(record: MemberRecord, ages: Ages, now: Instant) -> bool {
        let want = ages.record(now);
        record.absent_since == want.absent_since
            && record.gone_since == want.gone_since
            && record.known_since == want.known_since
            && record.present_since == want.present_since
            && record.returned_since == want.returned_since
            && record.back_since == want.back_since
    }

    #[test]
    fn forgotten_decides_each_mark_by_its_own_horizon() {
        // A pass 1,000 s past the start of the clock this test reads, so no
        // age below subtracts past it.
        let now = Instant::now() + Duration::from_secs(1000);
        let (gone, back) = (Duration::from_secs(10), Duration::from_secs(30));
        let gone_for = |secs| Ages {
            gone: Some(secs),
            known: Some(secs + 5),
            ..Ages::default()
        };
        let back_for = |secs| Ages {
            back: Some(secs),
            ..Ages::default()
        };
        let cases: [(&str, Ages, Ages); 12] = [
            ("gone inside the horizon: kept", gone_for(9), gone_for(9)),
            (
                "gone exactly at the horizon: kept",
                gone_for(10),
                gone_for(10),
            ),
            (
                "gone past the horizon: departure marks go",
                gone_for(11),
                Ages::default(),
            ),
            (
                "gone past the horizon, absence not aged yet: only the absence stays",
                Ages {
                    absent: Some(40),
                    ..gone_for(11)
                },
                Ages {
                    absent: Some(40),
                    ..Ages::default()
                },
            ),
            ("back inside its bound: kept", back_for(29), back_for(29)),
            (
                "back exactly at its bound: kept",
                back_for(30),
                back_for(30),
            ),
            ("back past its bound: goes", back_for(31), Ages::default()),
            (
                "back past the gone horizon but inside its own bound: kept",
                Ages {
                    back: Some(20),
                    ..gone_for(11)
                },
                back_for(20),
            ),
            (
                "tombstone collection's marks are not aged here",
                Ages {
                    absent: Some(500),
                    returned: Some(500),
                    ..Ages::default()
                },
                Ages {
                    absent: Some(500),
                    returned: Some(500),
                    ..Ages::default()
                },
            ),
            (
                "a live member keeps every mark, whatever their age",
                Ages {
                    present: Some(500),
                    known: Some(500),
                    back: Some(31),
                    ..Ages::default()
                },
                Ages {
                    present: Some(500),
                    known: Some(500),
                    ..Ages::default()
                },
            ),
            (
                "a mark stamped after the pass is kept",
                Ages {
                    gone: Some(0),
                    back: Some(0),
                    ..Ages::default()
                },
                Ages {
                    gone: Some(0),
                    back: Some(0),
                    ..Ages::default()
                },
            ),
            (
                "an empty record stays empty",
                Ages::default(),
                Ages::default(),
            ),
        ];
        for (name, before, after) in cases {
            let forgotten = before.record(now).forgotten(now, gone, back);
            assert!(holds(forgotten, after, now), "{name}");
        }
    }

    #[test]
    fn forgotten_leaves_a_record_empty_only_when_no_mark_is_left() {
        let now = Instant::now() + Duration::from_secs(1000);
        let (gone, back) = (Duration::from_secs(10), Duration::from_secs(30));
        let spent = Ages {
            gone: Some(11),
            known: Some(11),
            back: Some(31),
            ..Ages::default()
        };
        assert!(spent.record(now).forgotten(now, gone, back).is_empty());
        let absent_too = Ages {
            absent: Some(1),
            ..spent
        };
        assert!(!absent_too.record(now).forgotten(now, gone, back).is_empty());
    }

    #[test]
    fn a_forget_policy_keeps_a_return_at_least_as_long_as_either_reader_needs() {
        let secs = Duration::from_secs;
        let cases = [
            // (receipt lifetime, transfer budget, gc period) -> (gone, back, every)
            ((100, 20, 4), (100, 100, 2)),
            ((20, 100, 4), (20, 100, 2)),
            ((60, 60, 1), (60, 60, 0)),
        ];
        for ((receipt, budget, period), (gone, back, every)) in cases {
            let policy = ForgetPolicy::new(secs(receipt), secs(budget), secs(period));
            assert_eq!(policy.gone, secs(gone), "{receipt}/{budget}/{period}");
            assert_eq!(policy.back, secs(back), "{receipt}/{budget}/{period}");
            if every > 0 {
                assert_eq!(policy.every, secs(every), "{receipt}/{budget}/{period}");
            } else {
                assert_eq!(policy.every, Duration::from_millis(500));
            }
        }
    }

    #[test]
    fn the_forget_policy_of_a_config_reads_the_receipt_lifetime_budget_and_gc_period() {
        let config = ClusterConfig {
            crdt_retire_after: Duration::from_secs(100),
            crdt_sweep_interval: Some(Duration::from_secs(10)),
            state_transfer_budget: Duration::from_secs(20),
            tombstone_ttl: Duration::from_secs(40),
            ..ClusterConfig::default()
        };
        assert_eq!(
            ForgetPolicy::for_config(&config),
            ForgetPolicy::new(
                Duration::from_millis(config.crdt_compaction_bounds().receipt_ttl_ms),
                Duration::from_secs(20),
                Duration::from_secs(10),
            ),
            "300 s receipts (three bounds), a 20 s budget, a gc tick every 10 s"
        );
        assert_eq!(
            ForgetPolicy::for_config(&config).gone,
            Duration::from_secs(300)
        );

        let slow_transfer = ClusterConfig {
            state_transfer_budget: Duration::from_mins(15),
            ..config
        };
        assert_eq!(
            ForgetPolicy::for_config(&slow_transfer).back,
            Duration::from_mins(15),
            "a transfer budget longer than the receipt lifetime sets the return bound"
        );
    }

    #[test]
    fn forget_is_due_on_the_first_call_and_once_per_interval() {
        let at = Instant::now();
        let every = Duration::from_secs(5);
        assert!(forget_is_due(None, at, every), "no pass yet");
        let cases = [(0, false), (4, false), (5, true), (6, true), (3600, true)];
        for (secs, due) in cases {
            assert_eq!(
                forget_is_due(Some(at), at + Duration::from_secs(secs), every),
                due,
                "{secs} s after the last pass"
            );
        }
        assert!(
            !forget_is_due(Some(at + every), at, every),
            "a clock that reads earlier than the last pass is not due"
        );
    }

    #[test]
    fn members_are_oversized_only_when_large_and_mostly_empty() {
        let cases = [
            (0, 0, false),
            (0, MEMBERS_SHRINK_FLOOR - 1, false),
            (0, MEMBERS_SHRINK_FLOOR, true),
            (MEMBERS_SHRINK_FLOOR / 4, MEMBERS_SHRINK_FLOOR, true),
            (MEMBERS_SHRINK_FLOOR / 4 + 1, MEMBERS_SHRINK_FLOOR, false),
            (10, 4096, true),
            (2000, 4096, false),
        ];
        for (len, capacity, oversized) in cases {
            assert_eq!(
                members_oversized(len, capacity),
                oversized,
                "{len} records in {capacity} slots"
            );
        }
    }
}

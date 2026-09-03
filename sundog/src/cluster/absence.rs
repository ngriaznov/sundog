//! Partition-aware tombstone retention: tracks which recently-known cluster
//! members are currently absent from the live peer set, so
//! `tombstone_gc_task` can defer collecting a tombstone while a member that
//! might still hold the deleted entry is unreachable. Without this, an
//! unconditional GC would let anti-entropy resurrect a removed entry once
//! that member returns.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex as StdMutex, PoisonError};
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::membership::Peer;
use crate::node::NodeId;
use crate::store::Mode;

#[derive(Default)]
struct AbsenceState {
    live: HashSet<NodeId>,
    absent_since: HashMap<NodeId, Instant>,
}

/// Cheap-to-clone, cluster-wide view of which recently-known members are
/// currently absent from the live peer set — fed by [`tracking_task`],
/// sampled once per tick by `tombstone_gc_task` via [`should_defer_gc`].
///
/// On a single-node cluster the live peer set is always empty, so no
/// departure is ever observed and [`AbsenceTracker::any_absent`] stays
/// `false`. [`should_defer_gc`] never defers a `Mode::Local` cache either,
/// since such a cache never runs anti-entropy.
#[derive(Clone, Default)]
pub(crate) struct AbsenceTracker {
    state: Arc<StdMutex<AbsenceState>>,
}

impl AbsenceTracker {
    /// Applies one membership-watch snapshot: a peer that dropped out of
    /// `live` since the last call starts being tracked absent (if it isn't
    /// already); a peer back in `live` clears its tracked absence.
    fn observe(&self, live: &[Peer]) {
        let live_ids: HashSet<NodeId> = live.iter().map(|peer| peer.node).collect();
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let departed: Vec<NodeId> = state.live.difference(&live_ids).copied().collect();
        for node in departed {
            state.absent_since.entry(node).or_insert_with(Instant::now);
        }
        for &node in &live_ids {
            state.absent_since.remove(&node);
        }
        state.live = live_ids;
    }

    /// Whether any recently-known member is currently absent and has not yet
    /// aged past `hard_cap` — pruning entries older than `hard_cap` as a side
    /// effect, so a member that never returns doesn't grow this tracker's
    /// memory forever.
    pub(crate) fn any_absent(&self, hard_cap: Duration) -> bool {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let now = Instant::now();
        state
            .absent_since
            .retain(|_, since| now.saturating_duration_since(*since) < hard_cap);
        !state.absent_since.is_empty()
    }
}

/// Whether `tombstone_gc_task` should defer a tombstone past `tombstone_ttl`
/// on this tick. Only [`Mode::Replicated`] caches run anti-entropy, the
/// only mechanism that could resurrect a tombstone from a peer that was out
/// of contact, so `Mode::Local`/`Mode::Invalidation` caches are never
/// deferred.
pub(crate) fn should_defer_gc(mode: Mode, tracker: &AbsenceTracker, hard_cap: Duration) -> bool {
    matches!(mode, Mode::Replicated) && tracker.any_absent(hard_cap)
}

/// Republishes [`crate::membership::Membership::peers`] changes into
/// `tracker`, keeping [`AbsenceTracker`] current for the lifetime of the
/// cluster.
pub(crate) async fn tracking_task(
    mut peers: watch::Receiver<Vec<Peer>>,
    tracker: AbsenceTracker,
    cancel: CancellationToken,
) {
    tracker.observe(&peers.borrow_and_update());
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            changed = peers.changed() => {
                if changed.is_err() {
                    return; // membership shut down
                }
                tracker.observe(&peers.borrow_and_update());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};

    use super::*;
    use crate::node::NodeName;

    fn peer(node: u64) -> Peer {
        let id = NodeId::from(node);
        Peer {
            node: id,
            name: NodeName::new("host", id),
            gossip_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 7000)),
            data_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 8000)),
            incarnation: 1,
        }
    }

    #[test]
    fn no_peers_ever_observed_means_never_absent() {
        let tracker = AbsenceTracker::default();
        assert!(!tracker.any_absent(Duration::from_secs(3600)));
    }

    #[test]
    fn a_peer_that_leaves_the_live_set_is_tracked_absent() {
        let tracker = AbsenceTracker::default();
        let p = peer(1);
        tracker.observe(std::slice::from_ref(&p));
        assert!(
            !tracker.any_absent(Duration::from_secs(3600)),
            "still live: not absent"
        );

        tracker.observe(&[]);
        assert!(
            tracker.any_absent(Duration::from_secs(3600)),
            "dropped out of the live set: now tracked absent"
        );
    }

    #[test]
    fn a_returning_peer_clears_its_tracked_absence() {
        let tracker = AbsenceTracker::default();
        let p = peer(1);
        tracker.observe(std::slice::from_ref(&p));
        tracker.observe(&[]);
        assert!(tracker.any_absent(Duration::from_secs(3600)));

        tracker.observe(&[p]);
        assert!(
            !tracker.any_absent(Duration::from_secs(3600)),
            "a live member is not tracked absent"
        );
    }

    #[tokio::test]
    async fn absence_ages_out_past_the_hard_cap() {
        let tracker = AbsenceTracker::default();
        let p = peer(1);
        tracker.observe(&[p]);
        tracker.observe(&[]);

        let tiny_cap = Duration::from_millis(1);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !tracker.any_absent(tiny_cap),
            "absence older than the hard cap ages out"
        );
    }

    #[test]
    fn should_defer_gc_ignores_absence_outside_replicated_mode() {
        let tracker = AbsenceTracker::default();
        tracker.observe(&[peer(1)]);
        tracker.observe(&[]); // peer 1 now absent
        let hard_cap = Duration::from_secs(3600);

        assert!(should_defer_gc(Mode::Replicated, &tracker, hard_cap));
        assert!(!should_defer_gc(Mode::Local, &tracker, hard_cap));
        assert!(!should_defer_gc(Mode::Invalidation, &tracker, hard_cap));
    }

    #[test]
    fn should_defer_gc_is_false_for_replicated_mode_with_no_absence() {
        let tracker = AbsenceTracker::default();
        assert!(!should_defer_gc(
            Mode::Replicated,
            &tracker,
            Duration::from_secs(3600)
        ));
    }
}

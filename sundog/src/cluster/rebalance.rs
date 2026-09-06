//! Rebalance for a `Mode::Distributed` cache: pulls a bucket from its
//! current owners the moment this node's [`OwnershipView`] says it gained
//! it, and releases a bucket's local data once this node has kept it
//! resident past the disown grace after losing it. The open()-time initial
//! pull and this module's ongoing loop share one mechanism,
//! [`pull_buckets`], scoped to whichever bucket set is at hand.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use smol_str::SmolStr;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use super::Cluster;
use super::anti_entropy::{self, RoundOutcome};
use super::state_transfer::{self, DonorResult, Outcome};
use crate::net::Mesh;
use crate::node::NodeId;
use crate::ownership::{OwnershipTracker, OwnershipView, ResidencySet, ownership_diff};
use crate::store::ShardOps;

/// Groups `buckets` by the exact donor set each should be pulled from — the
/// bucket's live owners under `view`, self excluded, in the view's own
/// rendezvous order — so two buckets sharing an owner-set-minus-self land in
/// one `StBuckets` round trip. The bucket-scoped analogue of
/// `cluster::group_by_owner_set`, grouping bucket numbers instead of
/// records, and preserving rendezvous order (not sorting it) since that
/// order is exactly this group's donor try-order.
fn group_buckets_by_donor_set(
    view: &OwnershipView,
    self_node: NodeId,
    buckets: Vec<u16>,
) -> Vec<(Vec<NodeId>, Vec<u16>)> {
    let mut groups: Vec<(Vec<NodeId>, Vec<u16>)> = Vec::new();
    for bucket in buckets {
        let donors: Vec<NodeId> = view
            .owners_of(bucket)
            .iter()
            .copied()
            .filter(|&n| n != self_node)
            .collect();
        match groups.iter_mut().find(|(d, _)| *d == donors) {
            Some((_, group_buckets)) => group_buckets.push(bucket),
            None => groups.push((donors, vec![bucket])),
        }
    }
    groups
}

/// [`state_transfer::try_donor`]'s bucket-scoped counterpart: pulls
/// `buckets` from `donor` via [`Mesh::request_buckets`] instead of a
/// whole-cache [`Mesh::request_state`], sharing the per-donor stream-pull-
/// and-apply logic through [`state_transfer::pull_from_donor`].
async fn try_donor_buckets(
    shard: &Arc<dyn ShardOps>,
    mesh: &Mesh,
    cache: &SmolStr,
    donor: NodeId,
    buckets: Vec<u16>,
    view_hash: u64,
) -> (DonorResult, u64) {
    state_transfer::pull_from_donor(
        shard,
        donor,
        mesh.request_buckets(donor, cache.clone(), buckets, view_hash),
    )
    .await
}

/// Delay between retry passes over a group's whole donor list once every
/// candidate has declined or failed once: long enough for both sides'
/// `refresh_task`s to have a real chance at converging their views before
/// trying again.
const GROUP_RETRY_BACKOFF: Duration = Duration::from_millis(100);

/// Tries `donors` in order for one bucket group, applying the first that
/// donates. A donor's `StaleView` decline is expected while this side's and
/// that donor's `refresh_task`s are still converging on the same eligible
/// set right after a membership change — see
/// [`crate::store::ShardOps::ae_peer_filter`]'s doc for the same race — so a
/// pass where every donor declines or fails retries the whole list after
/// [`GROUP_RETRY_BACKOFF`] rather than giving up, unless this node's own
/// view has moved on from `view_hash` since the pull was planned: then no
/// donor will ever agree, and the pull answers `None` so the caller plans
/// afresh against the current view; the caller's own `budget`
/// timeout is the only bound on how long this keeps trying. Returns the
/// count of records actually landed once a donor succeeds.
#[allow(clippy::too_many_arguments)]
async fn pull_one_group(
    shard: &Arc<dyn ShardOps>,
    mesh: &Mesh,
    cache: &SmolStr,
    ownership: &OwnershipTracker,
    donors: Vec<NodeId>,
    buckets: Vec<u16>,
    view_hash: u64,
    per_donor: Duration,
) -> Option<u64> {
    loop {
        for &donor in &donors {
            let (result, count) = tokio::time::timeout(
                per_donor,
                try_donor_buckets(shard, mesh, cache, donor, buckets.clone(), view_hash),
            )
            .await
            .unwrap_or_else(|_| {
                tracing::debug!(%donor, "rebalance bucket pull to donor timed out; trying the next");
                (DonorResult::Failed, 0)
            });
            if result == DonorResult::Done {
                return Some(count);
            }
        }
        if ownership.current().view_hash() != view_hash {
            tracing::debug!(cache = %cache, "ownership view moved mid-pull; this pull is superseded");
            return None;
        }
        tokio::time::sleep(GROUP_RETRY_BACKOFF).await;
    }
}

/// Pulls `buckets` from any current owner (per `ownership.current()`),
/// grouped by donor set exactly as `cluster::fan_out_by_owner_set` groups
/// writes: two buckets sharing an owner-set-minus-self go in one
/// `StBuckets` round trip, tried in rendezvous order per group. Bounded to
/// at most `concurrency` simultaneous donor streams via a
/// [`tokio::sync::Semaphore`], so a mass-membership event can't open dozens
/// of simultaneous transfer streams. Applies replies through
/// [`ShardOps::apply_remote_batch`], so the inbound-apply guard is the
/// final safety net regardless of what a donor sends.
///
/// An empty `buckets` short-circuits to [`Outcome::Completed`] — nothing to
/// pull. A zero `budget` short-circuits to [`Outcome::Skipped`], matching
/// [`state_transfer::run`]'s same rule. A bucket set whose every group has
/// no donor at all (every owner is this node alone, the degenerate
/// small-cluster case) answers [`Outcome::NoPeers`]. Otherwise the whole
/// operation races `budget`: every group either lands or declines within it
/// and this returns [`Outcome::Completed`], or the budget runs out first
/// and this returns [`Outcome::TimedOut`], leaving the rest to a retry or
/// to anti-entropy's self-healing backstop. A view that moves on from
/// `ownership.current()` mid-pull answers [`Outcome::Superseded`] once
/// every group has landed or given up: the caller plans afresh against
/// the current view.
pub(crate) async fn pull_buckets(
    cluster: &Cluster,
    shard: &Arc<dyn ShardOps>,
    ownership: &OwnershipTracker,
    cache: &SmolStr,
    buckets: Vec<u16>,
    budget: Duration,
    concurrency: usize,
) -> Outcome {
    if buckets.is_empty() {
        return Outcome::Completed;
    }
    if budget.is_zero() {
        tracing::debug!(cache = %cache, "rebalance transfer budget is zero; leaving gained buckets to anti-entropy");
        return Outcome::Skipped;
    }

    let view = ownership.current();
    let view_hash = view.view_hash();
    let groups: Vec<(Vec<NodeId>, Vec<u16>)> =
        group_buckets_by_donor_set(&view, cluster.node_id(), buckets)
            .into_iter()
            .filter(|(donors, _)| !donors.is_empty())
            .collect();
    if groups.is_empty() {
        tracing::debug!(cache = %cache, "no live co-owner for any gained bucket; nothing to pull");
        return Outcome::NoPeers;
    }

    let per_donor = state_transfer::per_donor_budget(budget);
    let semaphore = Arc::new(Semaphore::new(concurrency.max(1)));
    let mesh = cluster.mesh().clone();

    let run_all = async {
        let mut set = tokio::task::JoinSet::new();
        for (donors, group_buckets) in groups {
            let semaphore = Arc::clone(&semaphore);
            let shard = Arc::clone(shard);
            let mesh = mesh.clone();
            let cache = cache.clone();
            let ownership = ownership.clone();
            set.spawn(async move {
                let _permit = semaphore
                    .acquire_owned()
                    .await
                    .expect("invariant: semaphore is never closed");
                pull_one_group(
                    &shard,
                    &mesh,
                    &cache,
                    &ownership,
                    donors,
                    group_buckets,
                    view_hash,
                    per_donor,
                )
                .await
            });
        }
        let mut landed = 0u64;
        let mut superseded = false;
        while let Some(result) = set.join_next().await {
            match result.ok().flatten() {
                Some(count) => landed += count,
                None => superseded = true,
            }
        }
        (landed, superseded)
    };

    match tokio::time::timeout(budget, run_all).await {
        Ok((landed, superseded)) => {
            if landed > 0 {
                metrics::counter!(
                    "sundog_rebalance_buckets_total",
                    "cache" => cache.to_string(),
                    "direction" => "in"
                )
                .increment(landed);
            }
            if superseded {
                Outcome::Superseded
            } else {
                Outcome::Completed
            }
        }
        Err(_) => Outcome::TimedOut,
    }
}

/// Retries [`pull_buckets`] for whatever this node's [`OwnershipTracker`]
/// currently owns, after an initial `open()`-time pull that
/// [`state_transfer::Outcome::needs_warm_up`] left cold: with no co-owner
/// in the current view it waits for the view to change, since only a new
/// view can bring one, then pulls again every `retry_interval` until a
/// pass lands or repeated timeouts give up and mark the cache warm with
/// whatever landed, leaving the rest to anti-entropy. The bucket-scoped
/// analogue of [`state_transfer::warm_up_task`], sharing its
/// [`state_transfer::next_warm_up_step`] decision and
/// [`state_transfer::MAX_WARM_UP_ATTEMPTS`] cap.
pub(crate) async fn warm_up_task(
    cluster: Cluster,
    shard: Arc<dyn ShardOps>,
    ownership: OwnershipTracker,
    cache: SmolStr,
    budget: Duration,
    concurrency: usize,
    cancel: CancellationToken,
) {
    let retry_interval = cluster.config().ae_interval;
    let mut attempt: u32 = 0;
    let mut view_rx = ownership.subscribe();
    loop {
        let buckets: Vec<u16> = view_rx.borrow_and_update().owned_buckets().collect();
        let outcome = tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            outcome = pull_buckets(&cluster, &shard, &ownership, &cache, buckets, budget, concurrency) => outcome,
        };
        attempt += 1;
        match state_transfer::next_warm_up_step(outcome, attempt) {
            state_transfer::WarmUpStep::Done => {
                cluster.mark_warm(&cache);
                return;
            }
            state_transfer::WarmUpStep::WaitForPeer => {
                tracing::debug!(cache = %cache, "no co-owner for any owned bucket yet; waiting for the ownership view to change");
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => return,
                    changed = view_rx.changed() => {
                        if changed.is_err() {
                            return;
                        }
                    }
                }
            }
            state_transfer::WarmUpStep::RetryNow => {}
            state_transfer::WarmUpStep::RetryLater => {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => return,
                    () = tokio::time::sleep(retry_interval) => {}
                }
            }
            state_transfer::WarmUpStep::WarmAnyway => {
                tracing::warn!(
                    cache = %cache,
                    attempts = attempt,
                    "rebalance pull timed out repeatedly; opening warm with what landed, anti-entropy carries the rest"
                );
                cluster.mark_warm(&cache);
                return;
            }
        }
    }
}

/// The distinct live current owners, other than `self_node`, of the
/// buckets in `due`: the peers a release hands each bucket to first.
pub(crate) fn hand_off_owners(
    view: &OwnershipView,
    self_node: NodeId,
    due: &[u16],
    live: &HashSet<NodeId>,
) -> Vec<NodeId> {
    let mut owners: Vec<NodeId> = Vec::new();
    for &bucket in due {
        for &owner in view.owners_of(bucket) {
            if owner != self_node && live.contains(&owner) && !owners.contains(&owner) {
                owners.push(owner);
            }
        }
    }
    owners
}

/// The buckets in `due` a release may drop now: those whose every other
/// current owner either is in `reconciled` (an anti-entropy round against
/// it completed since the bucket came due, so it holds what this node
/// held) or, for a bucket in `overdue` (held past the hard cap), is in
/// `unreachable` (its round failed outright, or it is not a live peer), so
/// an owner that never answers cannot pin memory forever. An owner whose
/// view still differs from this node's is neither: its round ends `Stale`,
/// the bucket stays resident, and the next tick tries again once the views
/// converge, however long that takes.
pub(crate) fn buckets_to_release(
    view: &OwnershipView,
    self_node: NodeId,
    due: &[u16],
    overdue: &[u16],
    reconciled: &HashSet<NodeId>,
    unreachable: &HashSet<NodeId>,
) -> Vec<u16> {
    due.iter()
        .copied()
        .filter(|&bucket| {
            view.owners_of(bucket).iter().all(|owner| {
                *owner == self_node
                    || reconciled.contains(owner)
                    || (overdue.contains(&bucket) && unreachable.contains(owner))
            })
        })
        .collect()
}

/// Reacts to every change in `ownership`'s view for as long as `cancel`
/// stays live: marks newly lost buckets releasing, unmarks newly regained
/// ones (a flap never accumulates toward release), and pulls newly gained
/// ones from their current owners. On a tick piggybacked on `ae_interval`
/// (no separate ticker), hands off whichever buckets' disown grace has
/// elapsed: one anti-entropy round against each of their live current
/// owners ([`hand_off_owners`]), pushing what an owner lacks, then
/// [`ShardOps::release_buckets`] for the buckets every owner answered
/// ([`buckets_to_release`]), counting
/// `sundog_rebalance_buckets_total{direction="out"}`. A bucket whose owner
/// did not answer stays resident until the next tick: for as long as the
/// owner's view differs from this node's, or up to twice the grace when
/// the owner is unreachable. The hand-off closes the one gap a pull alone
/// leaves: two nodes joining at once can both own a bucket neither has
/// yet, each pulling it from the other.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn rebalance_task(
    cluster: Cluster,
    shard: Arc<dyn ShardOps>,
    ownership: OwnershipTracker,
    residency: Arc<ResidencySet>,
    cache: SmolStr,
    disown_grace: Duration,
    concurrency: usize,
    cancel: CancellationToken,
) {
    let budget = cluster.config().state_transfer_budget;
    let ae_interval = cluster.config().ae_interval;
    let mut view_rx = ownership.subscribe();
    let mut old_view = view_rx.borrow_and_update().clone();
    let mut ticker = tokio::time::interval(ae_interval.max(Duration::from_millis(1)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            changed = view_rx.changed() => {
                if changed.is_err() {
                    return; // the tracker's sender dropped
                }
                let new_view = view_rx.borrow_and_update().clone();
                let (gained, lost) = ownership_diff(&old_view, &new_view);
                if !lost.is_empty() {
                    residency.mark_releasing(&lost);
                    tracing::debug!(cache = %cache, count = lost.len(), "buckets lost; disown grace started");
                }
                let outcome = if gained.is_empty() {
                    Outcome::Completed
                } else {
                    residency.unmark(&gained);
                    tracing::debug!(cache = %cache, count = gained.len(), "buckets gained; pulling from current owners");
                    pull_buckets(&cluster, &shard, &ownership, &cache, gained, budget, concurrency).await
                };
                // A pull the view moved past is planned again from the
                // same starting point on the next change, which has
                // already been published, so nothing gained is skipped.
                if outcome != Outcome::Superseded {
                    old_view = new_view;
                }
            }
            _ = ticker.tick() => {
                let due = residency.expired(disown_grace);
                if !due.is_empty() {
                    let view = ownership.current();
                    let self_node = cluster.node_id();
                    let live: HashSet<NodeId> = cluster.peers().iter().map(|peer| peer.node).collect();
                    let mut reconciled: HashSet<NodeId> = HashSet::new();
                    // An owner gossip no longer lists is unreachable by
                    // definition; the view drops it on its next refresh.
                    let mut unreachable: HashSet<NodeId> = due
                        .iter()
                        .flat_map(|&bucket| view.owners_of(bucket).iter().copied())
                        .filter(|owner| *owner != self_node && !live.contains(owner))
                        .collect();
                    for owner in hand_off_owners(&view, self_node, &due, &live) {
                        let outcome = tokio::select! {
                            biased;
                            () = cancel.cancelled() => return,
                            outcome = anti_entropy::run_round_against(&cluster, &shard, &cache, owner) => outcome,
                        };
                        match outcome {
                            RoundOutcome::Reconciled => {
                                reconciled.insert(owner);
                            }
                            RoundOutcome::Failed => {
                                unreachable.insert(owner);
                            }
                            RoundOutcome::Stale => {}
                        }
                    }
                    let overdue = residency.expired(disown_grace * 2);
                    let due = buckets_to_release(&view, self_node, &due, &overdue, &reconciled, &unreachable);
                    if due.is_empty() {
                        tracing::debug!(cache = %cache, "no released bucket's owners all answered its hand-off; holding until the next tick");
                        continue;
                    }
                    let removed = shard.release_buckets(&due).await;
                    residency.unmark(&due);
                    if removed > 0 {
                        metrics::counter!(
                            "sundog_rebalance_buckets_total",
                            "cache" => cache.to_string(),
                            "direction" => "out"
                        )
                        .increment(removed);
                    }
                    tracing::debug!(cache = %cache, buckets = due.len(), removed, "released buckets past their disown grace");
                }
            }
        }
    }
}

// The pure grouping tests need no cluster; the `pull_buckets` tests build a
// real `Cluster` with a real `Mesh`, which panics under `sim` outside a
// driven `turmoil::Sim`.
#[cfg(all(test, not(feature = "sim")))]
mod tests {
    use std::num::NonZeroU8;

    use super::*;
    use crate::node::NodeId;

    fn view(self_node: NodeId, eligible: Vec<NodeId>, k: u8) -> OwnershipView {
        OwnershipView::compute(self_node, eligible, NonZeroU8::new(k).expect("nonzero"))
    }

    #[test]
    fn group_buckets_by_donor_set_groups_buckets_sharing_the_same_donors() {
        let self_node = NodeId::from(1);
        let eligible: Vec<NodeId> = (1..=5u64).map(NodeId::from).collect();
        let view = view(self_node, eligible, 2);
        let buckets: Vec<u16> = (0..64).collect();

        let groups = group_buckets_by_donor_set(&view, self_node, buckets.clone());

        // Every bucket appears in exactly one group, and each group's donor
        // set is precisely that bucket's owners minus self.
        let mut regrouped: Vec<u16> = groups.iter().flat_map(|(_, bs)| bs.clone()).collect();
        regrouped.sort_unstable();
        assert_eq!(regrouped, buckets);
        for (donors, group_buckets) in &groups {
            for &bucket in group_buckets {
                let expected: Vec<NodeId> = view
                    .owners_of(bucket)
                    .iter()
                    .copied()
                    .filter(|&n| n != self_node)
                    .collect();
                assert_eq!(donors, &expected);
            }
        }
    }

    #[test]
    fn group_buckets_by_donor_set_excludes_self_from_every_donor_list() {
        let self_node = NodeId::from(1);
        // Solo eligible set: self is every bucket's only owner, so every
        // donor list is empty once self is excluded.
        let view = view(self_node, vec![self_node], 2);
        let groups = group_buckets_by_donor_set(&view, self_node, vec![0, 1, 2]);

        assert_eq!(groups.len(), 1);
        assert!(groups[0].0.is_empty());
    }

    #[test]
    fn hand_off_owners_lists_each_live_current_owner_once_and_never_self() {
        let self_node = NodeId::from(1);
        let eligible: Vec<NodeId> = (1..=5u64).map(NodeId::from).collect();
        let view = view(self_node, eligible.clone(), 2);
        let due: Vec<u16> = (0..256).collect();
        let live: HashSet<NodeId> = eligible
            .iter()
            .copied()
            .filter(|n| *n != self_node)
            .collect();

        let owners = hand_off_owners(&view, self_node, &due, &live);
        let distinct: HashSet<NodeId> = owners.iter().copied().collect();
        assert_eq!(owners.len(), distinct.len(), "each owner listed once");
        assert!(
            !owners.contains(&self_node),
            "self is never a hand-off target"
        );
        for owner in &owners {
            assert!(
                due.iter().any(|&b| view.owners_of(b).contains(owner)),
                "every listed owner owns a due bucket: {owner:?}"
            );
        }
        for &bucket in &due {
            for owner in view.owners_of(bucket) {
                if *owner != self_node {
                    assert!(
                        owners.contains(owner),
                        "every other owner of a due bucket is listed"
                    );
                }
            }
        }

        let dead = eligible[1];
        let mut live_minus_one = live.clone();
        live_minus_one.remove(&dead);
        assert!(
            !hand_off_owners(&view, self_node, &due, &live_minus_one).contains(&dead),
            "a dead owner is skipped"
        );
    }

    #[test]
    fn buckets_to_release_holds_a_bucket_until_every_owner_answered_or_it_is_overdue() {
        let self_node = NodeId::from(1);
        let eligible: Vec<NodeId> = (1..=5u64).map(NodeId::from).collect();
        let view = view(self_node, eligible, 2);
        let due: Vec<u16> = (0..256).collect();

        let none: HashSet<NodeId> = HashSet::new();
        let held = buckets_to_release(&view, self_node, &due, &[], &none, &none);
        for &bucket in &held {
            assert!(
                view.owners_of(bucket).iter().all(|o| *o == self_node),
                "with no owner answered, only a bucket owned by self alone is released: {bucket}"
            );
        }

        let answered: HashSet<NodeId> = [NodeId::from(2)].into_iter().collect();
        let released = buckets_to_release(&view, self_node, &due, &[], &answered, &none);
        for &bucket in &due {
            let all_answered = view
                .owners_of(bucket)
                .iter()
                .all(|o| *o == self_node || answered.contains(o));
            assert_eq!(
                released.contains(&bucket),
                all_answered,
                "bucket {bucket} is released exactly when every other owner answered"
            );
        }
        assert!(
            released.len() < due.len(),
            "some bucket waits on an owner that did not answer"
        );

        // Overdue: a stale owner still holds the bucket, an unreachable
        // one no longer does.
        let overdue: Vec<u16> = due.clone();
        let still_held = buckets_to_release(&view, self_node, &due, &overdue, &none, &none);
        assert_eq!(
            still_held, held,
            "an overdue bucket whose owners are merely stale stays resident"
        );
        let everyone: HashSet<NodeId> = (2..=5u64).map(NodeId::from).collect();
        let dropped = buckets_to_release(&view, self_node, &due, &overdue, &none, &everyone);
        assert_eq!(
            dropped, due,
            "an overdue bucket whose owners are all unreachable is released"
        );
        let mixed = buckets_to_release(&view, self_node, &due, &overdue, &answered, &everyone);
        assert_eq!(
            mixed, due,
            "reconciled and unreachable owners together release"
        );
    }

    #[tokio::test]
    async fn pull_buckets_completes_trivially_with_no_buckets_to_pull() {
        let cluster = crate::cluster::Cluster::builder("rebalance-unit-test-empty")
            .seeds(std::iter::empty())
            .config(crate::config::ClusterConfig {
                gossip_bind_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
                data_bind_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
                ..crate::config::ClusterConfig::default()
            })
            .build()
            .await
            .expect("solo cluster builds");
        let name = SmolStr::new("prices");
        let k = NonZeroU8::new(2).expect("nonzero");
        let (tracker, _tx) = OwnershipTracker::seed(
            cluster.node_id(),
            &cluster.peers(),
            &cluster.advertised_cache_modes(),
            &name,
            k,
        );
        let shard = empty_shard();

        let outcome = pull_buckets(
            &cluster,
            &shard,
            &tracker,
            &name,
            Vec::new(),
            Duration::from_secs(1),
            4,
        )
        .await;
        assert_eq!(outcome, Outcome::Completed);

        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn pull_buckets_reports_no_peers_when_every_bucket_has_no_other_owner() {
        let cluster = crate::cluster::Cluster::builder("rebalance-unit-test-solo")
            .seeds(std::iter::empty())
            .config(crate::config::ClusterConfig {
                gossip_bind_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
                data_bind_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
                ..crate::config::ClusterConfig::default()
            })
            .build()
            .await
            .expect("solo cluster builds");
        let name = SmolStr::new("prices");
        let k = NonZeroU8::new(2).expect("nonzero");
        let (tracker, _tx) = OwnershipTracker::seed(
            cluster.node_id(),
            &cluster.peers(),
            &cluster.advertised_cache_modes(),
            &name,
            k,
        );
        let shard = empty_shard();

        let outcome = pull_buckets(
            &cluster,
            &shard,
            &tracker,
            &name,
            vec![0, 1, 2],
            Duration::from_secs(1),
            4,
        )
        .await;
        assert_eq!(outcome, Outcome::NoPeers);

        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn warm_up_task_waits_for_a_co_owner_and_warms_once_its_pulls_give_up() {
        let cluster = crate::cluster::Cluster::builder("rebalance-unit-test-warm-up")
            .seeds(std::iter::empty())
            .config(crate::config::ClusterConfig {
                gossip_bind_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
                data_bind_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
                ae_interval: Duration::from_millis(50),
                ..crate::config::ClusterConfig::default()
            })
            .build()
            .await
            .expect("solo cluster builds");
        let name = SmolStr::new("prices");
        let k = NonZeroU8::new(2).expect("nonzero");
        let (tracker, tx) = OwnershipTracker::seed(
            cluster.node_id(),
            &cluster.peers(),
            &cluster.advertised_cache_modes(),
            &name,
            k,
        );
        let cancel = CancellationToken::new();
        let task = tokio::spawn(warm_up_task(
            cluster.clone(),
            empty_shard(),
            tracker,
            name.clone(),
            Duration::from_millis(150),
            4,
            cancel.clone(),
        ));

        // Self-only view: no co-owner anywhere, so the task waits on the
        // view instead of spinning or marking the cache warm.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!cluster.is_warm(&name), "no co-owner in sight: still cold");
        assert!(!task.is_finished(), "the task waits for a view change");

        // A view with an unreachable co-owner: every pull times out, and
        // after the attempt cap the cache opens warm with what landed.
        let phantom = NodeId::from(u64::MAX);
        tx.send(Arc::new(view(
            cluster.node_id(),
            vec![cluster.node_id(), phantom],
            2,
        )))
        .expect("the task still holds its receiver");
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("the task ends within the bound")
            .expect("the task does not panic");
        assert!(cluster.is_warm(&name), "warm after the pulls gave up");

        cancel.cancel();
        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn pull_buckets_skips_with_a_zero_budget() {
        let cluster = crate::cluster::Cluster::builder("rebalance-unit-test-zero-budget")
            .seeds(std::iter::empty())
            .config(crate::config::ClusterConfig {
                gossip_bind_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
                data_bind_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
                ..crate::config::ClusterConfig::default()
            })
            .build()
            .await
            .expect("solo cluster builds");
        let name = SmolStr::new("prices");
        let k = NonZeroU8::new(2).expect("nonzero");
        let (tracker, _tx) = OwnershipTracker::seed(
            cluster.node_id(),
            &cluster.peers(),
            &cluster.advertised_cache_modes(),
            &name,
            k,
        );
        let shard = empty_shard();

        let outcome = pull_buckets(
            &cluster,
            &shard,
            &tracker,
            &name,
            vec![0],
            Duration::ZERO,
            4,
        )
        .await;
        assert_eq!(outcome, Outcome::Skipped);

        cluster.shutdown().await;
    }

    fn empty_shard() -> Arc<dyn ShardOps> {
        Arc::new(crate::store::Shard::<u32, u32>::new(
            SmolStr::new("prices"),
            crate::store::Mode::Distributed {
                owners: NonZeroU8::new(2).expect("nonzero"),
            },
            crate::node::NodeId::random(),
            1024,
            None,
            None,
        )) as Arc<dyn ShardOps>
    }
}

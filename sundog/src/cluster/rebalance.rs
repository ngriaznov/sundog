//! Rebalance for a `Mode::Distributed` cache: pulls a bucket from its
//! current owners the moment this node's [`OwnershipView`] says it gained
//! it, and releases a bucket's local data once this node has kept it
//! resident past the disown grace after losing it. The open()-time initial
//! pull and this module's ongoing loop share one mechanism,
//! [`pull_buckets`], scoped to whichever bucket set is at hand.

use std::sync::Arc;
use std::time::Duration;

use smol_str::SmolStr;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use super::Cluster;
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
/// [`GROUP_RETRY_BACKOFF`] rather than giving up; the caller's own `budget`
/// timeout is the only bound on how long this keeps trying. Returns the
/// count of records actually landed once a donor succeeds.
async fn pull_one_group(
    shard: &Arc<dyn ShardOps>,
    mesh: &Mesh,
    cache: &SmolStr,
    donors: Vec<NodeId>,
    buckets: Vec<u16>,
    view_hash: u64,
    per_donor: Duration,
) -> u64 {
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
                return count;
            }
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
/// to anti-entropy's self-healing backstop.
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
            set.spawn(async move {
                let _permit = semaphore
                    .acquire_owned()
                    .await
                    .expect("invariant: semaphore is never closed");
                pull_one_group(
                    &shard,
                    &mesh,
                    &cache,
                    donors,
                    group_buckets,
                    view_hash,
                    per_donor,
                )
                .await
            });
        }
        let mut landed = 0u64;
        while let Some(result) = set.join_next().await {
            landed += result.unwrap_or(0);
        }
        landed
    };

    match tokio::time::timeout(budget, run_all).await {
        Ok(landed) => {
            if landed > 0 {
                metrics::counter!(
                    "sundog_rebalance_buckets_total",
                    "cache" => cache.to_string(),
                    "direction" => "in"
                )
                .increment(landed);
            }
            Outcome::Completed
        }
        Err(_) => Outcome::TimedOut,
    }
}

/// Retries [`pull_buckets`] for whatever this node's [`OwnershipTracker`]
/// currently owns, after an initial `open()`-time pull that
/// [`state_transfer::Outcome::needs_warm_up`] left cold: waits for a first
/// peer when there is none, then pulls again every `retry_interval` until a
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
    loop {
        let mut peers = cluster.peers_watch();
        loop {
            if !peers.borrow_and_update().is_empty() {
                break;
            }
            tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                changed = peers.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
            }
        }
        tracing::info!(cache = %cache, "peers present; retrying the rebalance pull to warm this cache");
        let buckets: Vec<u16> = ownership.current().owned_buckets().collect();
        let outcome = tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            outcome = pull_buckets(&cluster, &shard, &ownership, &cache, buckets, budget, concurrency) => outcome,
        };
        attempt += 1;
        match state_transfer::next_warm_up_step(outcome, attempt) {
            state_transfer::WarmUpStep::Done => return,
            state_transfer::WarmUpStep::WaitForPeer => {}
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

/// Reacts to every change in `ownership`'s view for as long as `cancel`
/// stays live: marks newly lost buckets releasing, unmarks newly regained
/// ones (a flap never accumulates toward release), and pulls newly gained
/// ones from their current owners. On a tick piggybacked on `ae_interval`
/// (no separate ticker), releases whichever buckets' disown grace has
/// elapsed via [`ShardOps::release_buckets`], counting
/// `sundog_rebalance_buckets_total{direction="out"}`.
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
                old_view = new_view;
                if !lost.is_empty() {
                    residency.mark_releasing(&lost);
                    tracing::debug!(cache = %cache, count = lost.len(), "buckets lost; disown grace started");
                }
                if !gained.is_empty() {
                    residency.unmark(&gained);
                    tracing::debug!(cache = %cache, count = gained.len(), "buckets gained; pulling from current owners");
                    pull_buckets(&cluster, &shard, &ownership, &cache, gained, budget, concurrency).await;
                }
            }
            _ = ticker.tick() => {
                let due = residency.expired(disown_grace);
                if !due.is_empty() {
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

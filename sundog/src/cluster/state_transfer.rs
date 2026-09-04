//! State transfer on cache open: a [`Mode::Replicated`] cache pulls a full
//! snapshot from the live donor with the lowest node id before `open()`
//! returns, then runs one anti-entropy round against that donor.
//! `Invalidation` caches skip it: their nodes hold different subsets by
//! design, so there is no snapshot to warm from.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt as _;
use smol_str::SmolStr;

use super::{Cluster, anti_entropy};
use crate::net::Mesh;
use crate::node::NodeId;
use crate::store::ShardOps;

/// Delay between retrying the same still-live donor after a transient failure.
const RETRY_BACKOFF: Duration = Duration::from_millis(200);

fn per_donor_budget(total: Duration) -> Duration {
    total * 2 / 5
}

/// How long a node with no live peer in sight waits for gossip to show one
/// before deciding it is the origin, and how long a node every peer
/// declines keeps retrying before deciding the cluster has nothing to give:
/// a fifth of `state_transfer_budget`, 4 s at the default, zero when the
/// budget is zero.
fn first_peer_grace(total: Duration) -> Duration {
    total / 5
}

/// How [`run`] ended, and so how the caller carries on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// One donor's snapshot landed and the cache is warm.
    Completed,
    /// No live peer within the first-peer grace: this node is the origin,
    /// warm with nothing to receive.
    NoPeers,
    /// Every live peer declined, none of them warm for this cache either, so
    /// there is nothing anywhere to receive: the cache is warm with what it
    /// has.
    NoDonor,
    /// A zero `state_transfer_budget`: no transfer runs at all, the cache is
    /// warm with what it has and anti-entropy carries the rest.
    Skipped,
    /// The budget ran out before any donor finished; the cache stays cold
    /// and keeps trying.
    TimedOut,
}

impl Outcome {
    /// Whether [`warm_up_task`] has work left after this outcome: a cold
    /// cache to warm, or an origin that receives the cache should a peer
    /// with it appear.
    pub(crate) const fn needs_warm_up(self) -> bool {
        matches!(self, Self::NoPeers | Self::TimedOut)
    }
}

/// Timed-out transfers [`warm_up_task`] retries before it marks the cache
/// warm with what has landed and leaves the rest to anti-entropy.
const MAX_WARM_UP_ATTEMPTS: u32 = 3;

/// What [`warm_up_task`] does after one [`run`] that ended `outcome` on its
/// `attempt`th try.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WarmUpStep {
    /// The cache is warm; the task ends.
    Done,
    /// Nobody to receive from yet; wait for a first peer, then run again.
    WaitForPeer,
    /// The transfer timed out; wait one `ae_interval`, then run again.
    RetryLater,
    /// Timed out [`MAX_WARM_UP_ATTEMPTS`] times running; mark the cache warm
    /// with what landed and end.
    WarmAnyway,
}

fn next_warm_up_step(outcome: Outcome, attempt: u32) -> WarmUpStep {
    match outcome {
        Outcome::Completed | Outcome::NoDonor | Outcome::Skipped => WarmUpStep::Done,
        Outcome::NoPeers => WarmUpStep::WaitForPeer,
        Outcome::TimedOut if attempt >= MAX_WARM_UP_ATTEMPTS => WarmUpStep::WarmAnyway,
        Outcome::TimedOut => WarmUpStep::RetryLater,
    }
}

/// How long the same set of candidates has been declining, so a pass of
/// nothing but declines counts as final only once that exact set has
/// declined for a whole grace. A candidate set that changes, a peer
/// appearing or one warming up, restarts the clock.
struct DeclineClock {
    set: Vec<NodeId>,
    since: tokio::time::Instant,
}

impl DeclineClock {
    fn new(set: Vec<NodeId>, now: tokio::time::Instant) -> Self {
        Self { set, since: now }
    }

    /// Notes one all-declined pass over `set` at `now`, returning how long
    /// this exact set has been declining.
    fn note(&mut self, set: &[NodeId], now: tokio::time::Instant) -> Duration {
        if self.set != set {
            self.set = set.to_vec();
            self.since = now;
        }
        now.saturating_duration_since(self.since)
    }
}

/// One donor's answer within a pass over the candidates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DonorResult {
    /// The whole snapshot landed.
    Done,
    /// The donor declined: it is not warm for this cache.
    Declined,
    /// The request or stream failed part way; the donor may be warm.
    Failed,
}

/// How a pass over every candidate ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pass {
    /// The candidate at this index donated.
    Done(usize),
    /// Every candidate declined: no warm donor exists.
    AllDeclined,
    /// At least one candidate failed rather than declined: worth another
    /// pass after a backoff.
    Retry,
}

/// Classifies one pass's results, in candidate order: the first `Done`
/// wins; with none, a pass of nothing but `Declined` answers is final while
/// any `Failed` earns a retry. An empty pass counts as all declined, since
/// the caller only runs one with candidates to try.
fn pass_outcome(results: &[DonorResult]) -> Pass {
    if let Some(idx) = results.iter().position(|r| *r == DonorResult::Done) {
        return Pass::Done(idx);
    }
    if results.contains(&DonorResult::Failed) {
        Pass::Retry
    } else {
        Pass::AllDeclined
    }
}

/// How [`transfer_loop`] ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Transfer {
    From(NodeId),
    NoPeers,
    AllDeclined,
}

/// Pulls `cache` from a live peer within `state_transfer_budget`, then
/// reconciles with every live peer through one anti-entropy round each, so
/// the joiner also holds whatever its donor lacked. Marks `cache` warm on
/// every outcome but [`Outcome::TimedOut`].
#[tracing::instrument(skip_all, fields(cache = %cache))]
pub(crate) async fn run(cluster: &Cluster, shard: &Arc<dyn ShardOps>, cache: &SmolStr) -> Outcome {
    let budget = cluster.config().state_transfer_budget;
    if budget.is_zero() {
        tracing::debug!("state transfer budget is zero; opening warm with what is held");
        cluster.mark_warm(cache);
        return Outcome::Skipped;
    }
    let started = tokio::time::Instant::now();
    match tokio::time::timeout(budget, transfer_loop(cluster, shard, cache)).await {
        Ok(Transfer::From(donor)) => {
            cluster.mark_warm(cache);
            reconcile_with_every_peer(cluster, shard, cache, donor, budget, started).await;
            Outcome::Completed
        }
        Ok(Transfer::NoPeers) => {
            tracing::debug!("no live peer within the grace; this node is the origin");
            cluster.mark_warm(cache);
            Outcome::NoPeers
        }
        Ok(Transfer::AllDeclined) => {
            tracing::debug!("every live peer is still warming this cache; nothing to receive");
            cluster.mark_warm(cache);
            Outcome::NoDonor
        }
        Err(_) => {
            tracing::warn!(
                budget = ?budget,
                "state transfer timed out before any donor finished; opening with a possibly partial cache"
            );
            Outcome::TimedOut
        }
    }
}

/// One anti-entropy round against `donor` first, then every other live peer,
/// each bounded by what is left of `budget` since `started`. A donor that
/// itself lost a replicate leaves a gap the other peers close here.
async fn reconcile_with_every_peer(
    cluster: &Cluster,
    shard: &Arc<dyn ShardOps>,
    cache: &SmolStr,
    donor: NodeId,
    budget: Duration,
    started: tokio::time::Instant,
) {
    let mut peers = vec![donor];
    peers.extend(
        cluster
            .live_peer_ids()
            .into_iter()
            .filter(|peer| *peer != donor),
    );
    for peer in peers {
        let remaining = budget.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            tracing::debug!(%peer, "post-transfer anti-entropy budget exhausted; skipping");
            return;
        }
        if tokio::time::timeout(
            remaining,
            anti_entropy::run_round_against(cluster, shard, cache, peer),
        )
        .await
        .is_err()
        {
            tracing::debug!(%peer, "post-transfer anti-entropy round timed out; skipping");
        }
    }
}

/// Keeps pulling for `cache` after a [`run`] that landed no snapshot: waits
/// for a first peer when there is none, then runs [`run`] again every
/// `ae_interval` until a snapshot lands or no peer has one to give. An
/// origin node that later meets a peer with the cache receives it here.
pub(crate) async fn warm_up_task(
    cluster: Cluster,
    shard: Arc<dyn ShardOps>,
    cache: SmolStr,
    cancel: tokio_util::sync::CancellationToken,
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
        tracing::info!(cache = %cache, "peers present; running state transfer to warm this cache");
        let outcome = tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            outcome = run(&cluster, &shard, &cache) => outcome,
        };
        attempt += 1;
        match next_warm_up_step(outcome, attempt) {
            WarmUpStep::Done => return,
            WarmUpStep::WaitForPeer => {}
            WarmUpStep::RetryLater => {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => return,
                    () = tokio::time::sleep(retry_interval) => {}
                }
            }
            WarmUpStep::WarmAnyway => {
                tracing::warn!(
                    cache = %cache,
                    attempts = attempt,
                    "state transfer timed out repeatedly; opening warm with what landed, anti-entropy carries the rest"
                );
                cluster.mark_warm(&cache);
                return;
            }
        }
    }
}

/// Tries every live peer in `NodeId` order, one pass at a time, until one
/// donates. With no peer in sight it first waits [`first_peer_grace`] for
/// gossip to show one. A pass with a failure backs off and runs again; a
/// pass in which every peer declines does the same until the grace has
/// passed since the loop began, then ends it: no warm donor exists.
async fn transfer_loop(cluster: &Cluster, shard: &Arc<dyn ShardOps>, cache: &SmolStr) -> Transfer {
    let local_node = cluster.node_id();
    let budget = cluster.config().state_transfer_budget;
    let per_donor = per_donor_budget(budget);
    let grace = first_peer_grace(budget);
    let started = tokio::time::Instant::now();
    let mut declines = DeclineClock::new(Vec::new(), started);
    loop {
        let mut candidates: Vec<NodeId> = cluster
            .live_peer_ids()
            .into_iter()
            .filter(|peer| *peer != local_node)
            .collect();
        candidates.sort_unstable();
        if candidates.is_empty() {
            let remaining = grace.saturating_sub(started.elapsed());
            if remaining.is_zero() || !wait_for_a_peer(cluster, remaining).await {
                return Transfer::NoPeers;
            }
            continue;
        }

        let mut results = Vec::with_capacity(candidates.len());
        for &donor in &candidates {
            let result = tokio::time::timeout(
                per_donor,
                try_donor(shard, cluster.mesh(), cache, donor),
            )
            .await
            .unwrap_or_else(|_| {
                tracing::debug!(%donor, "state transfer to donor timed out; trying the next");
                DonorResult::Failed
            });
            results.push(result);
            if result == DonorResult::Done {
                break;
            }
        }
        match pass_outcome(&results) {
            Pass::Done(idx) => return Transfer::From(candidates[idx]),
            Pass::AllDeclined
                if declines.note(&candidates, tokio::time::Instant::now()) >= grace =>
            {
                return Transfer::AllDeclined;
            }
            Pass::AllDeclined | Pass::Retry => tokio::time::sleep(RETRY_BACKOFF).await,
        }
    }
}

/// Waits up to `limit` for the peer list to become non-empty; `false` if it
/// is still empty then.
async fn wait_for_a_peer(cluster: &Cluster, limit: Duration) -> bool {
    let mut peers = cluster.peers_watch();
    tokio::time::timeout(limit, async {
        while peers.borrow_and_update().is_empty() {
            if peers.changed().await.is_err() {
                return false;
            }
        }
        true
    })
    .await
    .unwrap_or(false)
}

async fn try_donor(
    shard: &Arc<dyn ShardOps>,
    mesh: &Mesh,
    cache: &SmolStr,
    donor: NodeId,
) -> DonorResult {
    let mut stream = match mesh.request_state(donor, cache.clone()).await {
        Ok(Some(stream)) => stream,
        Ok(None) => {
            tracing::debug!(%donor, "donor declined: not warm for this cache");
            return DonorResult::Declined;
        }
        Err(error) => {
            tracing::debug!(%donor, %error, "state transfer request failed");
            return DonorResult::Failed;
        }
    };

    let mut applied: u64 = 0;
    loop {
        match stream.next().await {
            Some(Ok(chunk)) => {
                // Weakly consistent donor-side iteration is safe: this is
                // the same versioned-apply path live `Replicate` uses, so
                // concurrency delivers whatever the snapshot misses.
                applied += chunk.len() as u64;
                shard.apply_remote_batch(chunk).await;
            }
            Some(Err(error)) => {
                tracing::warn!(
                    %donor,
                    %error,
                    applied,
                    "state transfer stream broke mid-transfer; will retry"
                );
                return DonorResult::Failed;
            }
            None => {
                metrics::counter!("sundog_state_transfer_records_total", "cache" => cache.to_string())
                    .increment(applied);
                tracing::info!(%donor, applied, "state transfer complete");
                return DonorResult::Done;
            }
        }
    }
}

// Real-transport-only: this builds a live `Cluster` with a real `Mesh`.
#[cfg(all(test, not(feature = "sim")))]
mod tests {
    use super::*;

    #[test]
    fn per_donor_budget_keeps_the_twenty_second_default_ratio() {
        assert_eq!(
            per_donor_budget(Duration::from_secs(20)),
            Duration::from_secs(8)
        );
        assert_eq!(per_donor_budget(Duration::ZERO), Duration::ZERO);
    }

    #[test]
    fn first_peer_grace_is_a_fifth_of_the_budget_and_zero_for_a_zero_budget() {
        assert_eq!(
            first_peer_grace(Duration::from_secs(20)),
            Duration::from_secs(4)
        );
        assert_eq!(first_peer_grace(Duration::ZERO), Duration::ZERO);
    }

    #[test]
    fn next_warm_up_step_ends_on_warm_outcomes_and_gives_up_after_repeated_timeouts() {
        assert_eq!(next_warm_up_step(Outcome::Completed, 1), WarmUpStep::Done);
        assert_eq!(next_warm_up_step(Outcome::NoDonor, 1), WarmUpStep::Done);
        assert_eq!(next_warm_up_step(Outcome::Skipped, 1), WarmUpStep::Done);
        assert_eq!(
            next_warm_up_step(Outcome::NoPeers, 5),
            WarmUpStep::WaitForPeer
        );
        assert_eq!(
            next_warm_up_step(Outcome::TimedOut, 1),
            WarmUpStep::RetryLater
        );
        assert_eq!(
            next_warm_up_step(Outcome::TimedOut, MAX_WARM_UP_ATTEMPTS - 1),
            WarmUpStep::RetryLater
        );
        assert_eq!(
            next_warm_up_step(Outcome::TimedOut, MAX_WARM_UP_ATTEMPTS),
            WarmUpStep::WarmAnyway
        );
    }

    #[test]
    fn needs_warm_up_only_after_no_peers_or_a_timeout() {
        assert!(Outcome::NoPeers.needs_warm_up());
        assert!(Outcome::TimedOut.needs_warm_up());
        assert!(!Outcome::Completed.needs_warm_up());
        assert!(!Outcome::NoDonor.needs_warm_up());
        assert!(!Outcome::Skipped.needs_warm_up());
    }

    #[tokio::test(start_paused = true)]
    async fn decline_clock_restarts_when_the_candidate_set_changes() {
        let a = crate::node::NodeId::from(1);
        let b = crate::node::NodeId::from(2);
        let t0 = tokio::time::Instant::now();
        let mut clock = DeclineClock::new(Vec::new(), t0);
        assert_eq!(
            clock.note(&[a], t0),
            Duration::ZERO,
            "a new set starts at zero"
        );
        tokio::time::advance(Duration::from_secs(3)).await;
        let t3 = tokio::time::Instant::now();
        assert_eq!(
            clock.note(&[a], t3),
            Duration::from_secs(3),
            "the same set accrues"
        );
        assert_eq!(
            clock.note(&[a, b], t3),
            Duration::ZERO,
            "a peer appearing restarts the clock"
        );
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(
            clock.note(&[a, b], tokio::time::Instant::now()),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn pass_outcome_prefers_the_first_donor_that_finished() {
        use DonorResult::{Declined, Done, Failed};
        assert_eq!(pass_outcome(&[Declined, Done, Done]), Pass::Done(1));
        assert_eq!(pass_outcome(&[Failed, Done]), Pass::Done(1));
    }

    #[test]
    fn pass_outcome_ends_on_a_pass_of_nothing_but_declines_and_retries_on_a_failure() {
        use DonorResult::{Declined, Failed};
        assert_eq!(pass_outcome(&[Declined, Declined]), Pass::AllDeclined);
        assert_eq!(pass_outcome(&[]), Pass::AllDeclined);
        assert_eq!(pass_outcome(&[Declined, Failed]), Pass::Retry);
        assert_eq!(pass_outcome(&[Failed]), Pass::Retry);
    }

    #[tokio::test]
    async fn transfer_loop_returns_no_peers_immediately_with_no_live_peers() {
        let cluster = crate::cluster::Cluster::builder("state-transfer-unit-test")
            .seeds(std::iter::empty())
            .config(crate::config::ClusterConfig {
                gossip_bind_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
                data_bind_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
                ..crate::config::ClusterConfig::default()
            })
            .build()
            .await
            .expect("solo cluster builds");

        let cache: SmolStr = SmolStr::new("nobody-home");
        let started = tokio::time::Instant::now();
        let donor = tokio::time::timeout(
            Duration::from_secs(10),
            transfer_loop(&cluster, &empty_shard(), &cache),
        )
        .await
        .expect("resolves once the first-peer grace passes with no peer to try");
        assert_eq!(donor, Transfer::NoPeers);
        assert!(
            started.elapsed() >= Duration::from_secs(4),
            "the default budget's 4s grace is waited out first"
        );

        cluster.shutdown().await;
    }

    fn empty_shard() -> Arc<dyn ShardOps> {
        Arc::new(crate::store::Shard::<u32, u32>::new(
            SmolStr::new("nobody-home"),
            crate::store::Mode::Replicated,
            crate::node::NodeId::random(),
            1024,
            None,
            None,
        )) as Arc<dyn ShardOps>
    }
}

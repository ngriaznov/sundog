//! State transfer on cache open: pulls a full snapshot of a
//! cluster-wide [`Mode::Replicated`] cache from the lowest-live-node-id
//! donor before `open()` returns, then runs one immediate anti-entropy round
//! against that donor as a belt-and-braces sweep.
//!
//! Runs only for `Mode::Replicated`, for the same reason anti-entropy does
//! (see `cluster::anti_entropy`'s module docs): `Invalidation`-mode nodes
//! are supposed to hold different, independent subsets of a cache, so there
//! is no cluster-wide snapshot for a joiner to warm from.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt as _;
use smol_str::SmolStr;

use super::{Cluster, anti_entropy};
use crate::net::Mesh;
use crate::node::NodeId;
use crate::store::ShardOps;

/// Delay between retrying the same still-live donor after a transient
/// failure (e.g. the mesh hasn't finished dialing it yet).
const RETRY_BACKOFF: Duration = Duration::from_millis(200);

/// Per-donor slice of the overall budget: how long a single donor gets to
/// keep the stream moving before this node gives up and re-picks (a
/// different node, if this donor died meanwhile). Two fifths of the total,
/// so a wedged first donor still leaves room for a second full attempt plus
/// the belt-and-braces anti-entropy sweep.
fn per_donor_budget(total: Duration) -> Duration {
    total * 2 / 5
}

/// How [`run`] ended — [`Outcome::NoPeers`] is the caller's cue to arm
/// [`late_sync_task`], since `open()` racing gossip convergence is normal on
/// a cold join.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    Completed,
    NoPeers,
    TimedOut,
}

/// Runs state transfer for `cache`, blocking until either a donor finishes
/// (followed by one immediate anti-entropy round against it), no live peers
/// remain to try, or [`crate::config::ClusterConfig::state_transfer_budget`]
/// elapses.
#[tracing::instrument(skip_all, fields(cache = %cache))]
pub(crate) async fn run(cluster: &Cluster, shard: &Arc<dyn ShardOps>, cache: &SmolStr) -> Outcome {
    let budget = cluster.config().state_transfer_budget;
    let started = tokio::time::Instant::now();
    match tokio::time::timeout(budget, transfer_loop(cluster, shard, cache)).await {
        Ok(Some(donor)) => {
            // Keeps the belt-and-braces sweep inside the same overall
            // budget, rather than letting `run_round_against`'s own
            // internal timeout run on top of the transfer loop's elapsed
            // time.
            let remaining = budget.saturating_sub(started.elapsed());
            if tokio::time::timeout(
                remaining,
                anti_entropy::run_round_against(cluster, shard, cache, donor),
            )
            .await
            .is_err()
            {
                tracing::debug!(%donor, "post-transfer anti-entropy sweep timed out; skipping");
            }
            Outcome::Completed
        }
        Ok(None) => {
            tracing::debug!("no live peers at open; starting with an empty cache");
            Outcome::NoPeers
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

/// One-shot deferred warm-up for a cache opened before gossip had converged:
/// waits until the first live peer appears (or `cancel`), then runs [`run`]
/// once. Without this, a cold joiner's `open()` finds nobody, skips state
/// transfer, and warms up only as fast as anti-entropy intervals allow.
pub(crate) async fn late_sync_task(
    cluster: Cluster,
    shard: Arc<dyn ShardOps>,
    cache: SmolStr,
    cancel: tokio_util::sync::CancellationToken,
) {
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
    tracing::info!(cache = %cache, "first peer appeared after open; running deferred state transfer");
    tokio::select! {
        biased;
        () = cancel.cancelled() => {}
        _outcome = run(&cluster, &shard, &cache) => {}
    }
}

/// Retries donors — always the current lowest-id live peer — until one
/// completes a full snapshot or the live peer set is empty. No donor is
/// permanently excluded: a failed donor is retried after `RETRY_BACKOFF`,
/// re-reading the live set each time so a donor that died mid-stream is
/// naturally replaced by the next-lowest survivor. Returns the donor that
/// succeeded, or `None` if there was nobody to transfer from.
async fn transfer_loop(
    cluster: &Cluster,
    shard: &Arc<dyn ShardOps>,
    cache: &SmolStr,
) -> Option<NodeId> {
    let local_node = cluster.node_id();
    let per_donor = per_donor_budget(cluster.config().state_transfer_budget);
    loop {
        let mut candidates: Vec<NodeId> = cluster
            .live_peer_ids()
            .into_iter()
            .filter(|peer| *peer != local_node)
            .collect();
        candidates.sort_unstable();
        let donor = *candidates.first()?;

        match tokio::time::timeout(per_donor, try_donor(shard, cluster.mesh(), cache, donor)).await
        {
            Ok(true) => return Some(donor),
            Ok(false) => tokio::time::sleep(RETRY_BACKOFF).await,
            Err(_) => tracing::debug!(%donor, "state transfer to donor timed out; retrying"),
        }
    }
}

/// Requests and applies one donor's full snapshot. Returns `true` iff the
/// stream completed normally (a `done: true` chunk was observed —
/// `net::conn::state_stream` distinguishes this from a mid-stream close).
async fn try_donor(shard: &Arc<dyn ShardOps>, mesh: &Mesh, cache: &SmolStr, donor: NodeId) -> bool {
    let mut stream = match mesh.request_state(donor, cache.clone()).await {
        Ok(stream) => stream,
        Err(error) => {
            tracing::debug!(%donor, %error, "state transfer request failed");
            return false;
        }
    };

    let mut applied: u64 = 0;
    loop {
        match stream.next().await {
            Some(Ok(chunk)) => {
                // Weakly consistent donor-side iteration is safe: this is
                // the same versioned-apply path live `Replicate` traffic
                // uses, so whatever the snapshot misses, concurrency
                // delivers, and whatever both deliver, the version check
                // deduplicates. Applied as one donor chunk (~500 records)
                // under one lock acquisition.
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
                return false;
            }
            None => {
                metrics::counter!("sundog_state_transfer_records_total", "cache" => cache.to_string())
                    .increment(applied);
                tracing::info!(%donor, applied, "state transfer complete");
                return true;
            }
        }
    }
}

// Real-transport-only, same reason as `net::mod`'s test module: this builds
// a live `Cluster` with a real `Mesh`.
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

    #[tokio::test]
    async fn transfer_loop_returns_none_immediately_with_no_live_peers() {
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
        let donor = tokio::time::timeout(
            Duration::from_secs(5),
            transfer_loop(&cluster, &empty_shard(), &cache),
        )
        .await
        .expect("resolves promptly with no peers to try");
        assert!(donor.is_none());

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

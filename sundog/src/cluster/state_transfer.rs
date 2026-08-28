//! State transfer on cache open (plan §9): pulls a full snapshot of a
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

/// Overall wall-clock budget for the whole state-transfer attempt (across
/// every donor retry) before `open()` gives up waiting and proceeds with
/// whatever this node already has. Live traffic and the periodic
/// anti-entropy loop repair the gap afterward, so this is a startup-latency
/// bound, not a correctness one.
const TOTAL_BUDGET: Duration = Duration::from_secs(20);
/// Per-donor budget: how long a single donor gets to keep the stream moving
/// before this node gives up on it and re-picks (the lowest-id live peer may
/// by then be a different node, if this donor died in the meantime).
const PER_DONOR_BUDGET: Duration = Duration::from_secs(8);
/// Delay between retrying the same still-live donor after a transient
/// failure (e.g. the mesh hasn't finished dialing it yet).
const RETRY_BACKOFF: Duration = Duration::from_millis(200);

/// Runs state transfer for `cache`, blocking until either a donor finishes
/// (followed by one immediate anti-entropy round against it), no live peers
/// remain to try, or `TOTAL_BUDGET` elapses.
#[tracing::instrument(skip_all, fields(cache = %cache))]
pub(crate) async fn run(cluster: &Cluster, shard: &Arc<dyn ShardOps>, cache: &SmolStr) {
    let started = tokio::time::Instant::now();
    match tokio::time::timeout(TOTAL_BUDGET, transfer_loop(cluster, shard, cache)).await {
        Ok(Some(donor)) => {
            // `run_round_against` already bounds its own network calls
            // (`Mesh::ae_round`/`ae_pull`'s internal `REQUEST_TIMEOUT`), but
            // this additionally keeps the belt-and-braces sweep inside the
            // budget `open()`'s own docs promise end to end, rather than
            // letting it run for however long that internal bound allows on
            // top of the transfer loop's own elapsed time.
            let remaining = TOTAL_BUDGET.saturating_sub(started.elapsed());
            if tokio::time::timeout(
                remaining,
                anti_entropy::run_round_against(cluster, shard, cache, donor),
            )
            .await
            .is_err()
            {
                tracing::debug!(%donor, "post-transfer anti-entropy sweep timed out; skipping");
            }
        }
        Ok(None) => {
            tracing::debug!("no live peers at open; starting with an empty cache");
        }
        Err(_) => {
            tracing::warn!(
                budget = ?TOTAL_BUDGET,
                "state transfer timed out before any donor finished; opening with a possibly partial cache"
            );
        }
    }
}

/// Retries donors — always the current lowest-id live peer — until one
/// completes a full snapshot, or the live peer set is empty (a healthy
/// single-node cluster, or every donor has been excluded... except nothing
/// is ever permanently excluded: a donor that fails is simply retried after
/// `RETRY_BACKOFF`, re-reading the live set each time so a donor that died
/// mid-stream is naturally replaced by the next-lowest survivor). Returns
/// the donor that succeeded, or `None` if there was nobody to transfer from.
async fn transfer_loop(
    cluster: &Cluster,
    shard: &Arc<dyn ShardOps>,
    cache: &SmolStr,
) -> Option<NodeId> {
    let local_node = cluster.node_id();
    loop {
        let mut candidates: Vec<NodeId> = cluster
            .live_peer_ids()
            .into_iter()
            .filter(|peer| *peer != local_node)
            .collect();
        candidates.sort_unstable();
        let donor = *candidates.first()?;

        match tokio::time::timeout(
            PER_DONOR_BUDGET,
            try_donor(shard, cluster.mesh(), cache, donor),
        )
        .await
        {
            Ok(true) => return Some(donor),
            Ok(false) => tokio::time::sleep(RETRY_BACKOFF).await,
            Err(_) => tracing::debug!(%donor, "state transfer to donor timed out; retrying"),
        }
    }
}

/// Requests and applies one donor's full snapshot. Returns `true` iff the
/// stream completed normally (a `done: true` chunk was observed, plan §9's
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
            Some(Ok(rec)) => {
                // Weakly consistent donor-side iteration (plan §9/§13) is
                // safe here because this is the same versioned-apply path
                // live `Replicate` traffic uses — whatever the snapshot
                // missed, concurrency delivers; whatever both deliver, the
                // version check deduplicates.
                shard.apply_remote(rec).await;
                applied += 1;
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

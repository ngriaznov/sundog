//! Anti-entropy: a per-cache, jittered-interval loop that
//! reconciles one shard against one live peer per round — a digest exchange
//! followed by a bucket-diff push/pull, computed on the initiating side, so
//! both a peer that is behind and a peer that is ahead of this node end up
//! converged after one round. Tombstones participate identically to live
//! entries throughout, since a tombstone is just a [`crate::wire::WireRecord`]
//! with `value: None`.
//!
//! Runs only for [`Mode::Replicated`] caches: `Invalidation` mode nodes
//! deliberately hold different, independent subsets of a cache, so a
//! full-record digest reconciliation between them would defeat that
//! design rather than repair it.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rand::RngExt as _;
use rand::seq::IndexedRandom as _;
use smol_str::SmolStr;
use tokio_util::sync::CancellationToken;

use super::Cluster;
use crate::net::MsgClass;
use crate::node::NodeId;
use crate::store::ShardOps;
use crate::wire::Msg;

/// Runs anti-entropy for one shard for as long as `cancel` stays live: every
/// jittered `ae_interval`, picks one live peer (a dirty-marked one first —
/// the target of a dropped `Replicate` message) and runs one round against
/// it.
pub(crate) async fn scheduler_task(
    cluster: Cluster,
    shard: Arc<dyn ShardOps>,
    cache: SmolStr,
    ae_interval: Duration,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            () = tokio::time::sleep(jittered(ae_interval)) => {}
        }
        let Some(peer) = pick_peer(&cluster) else {
            continue;
        };
        // `run_round_against`'s own network calls carry an internal
        // `REQUEST_TIMEOUT`, but racing the whole round against `cancel`
        // too means a `Cluster::shutdown()` in progress never has to wait
        // out that timeout for this task's `TaskTracker::wait()` to return.
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            () = run_round_against(&cluster, &shard, &cache, peer) => {}
        }
    }
}

/// Keys per push/pull batch within one round. Bounds each `ae_pull`
/// request/response well under the frame cap and its request timeout, so a
/// large divergence repairs incrementally across batches (and rounds)
/// rather than betting everything on one oversized exchange.
const REPAIR_BATCH: usize = 4096;

/// A jittered delay around `interval` (uniformly in `[0.5, 1.5) * interval`,
/// floored at 1ms) — avoids every node's anti-entropy rounds landing in
/// lockstep across the cluster.
fn jittered(interval: Duration) -> Duration {
    let base = interval.max(Duration::from_millis(1)).as_secs_f64();
    let factor = rand::rng().random_range(0.5..1.5);
    Duration::from_secs_f64((base * factor).max(0.001))
}

fn pick_peer(cluster: &Cluster) -> Option<NodeId> {
    let mut rng = rand::rng();
    let dirty = cluster.mesh().take_dirty_peers();
    if let Some(&peer) = dirty.choose(&mut rng) {
        return Some(peer);
    }
    cluster.live_peer_ids().choose(&mut rng).copied()
}

/// One anti-entropy round against `peer`: exchanges digests, then diffs the
/// mismatched buckets in both directions — keys this node has newer (or
/// `peer` lacks entirely) are pushed via the normal `Replicate` fan-out path;
/// keys `peer` has newer (or this node lacks) are pulled and applied
/// directly. Both directions run from one side's diff because the responder
/// already reported its own entries for every mismatched bucket — no second
/// round trip is needed for convergence.
#[tracing::instrument(skip_all, fields(cache = %cache, peer = %peer))]
pub(crate) async fn run_round_against(
    cluster: &Cluster,
    shard: &Arc<dyn ShardOps>,
    cache: &SmolStr,
    peer: NodeId,
) {
    let local_buckets = shard.digests().await;
    let mesh = cluster.mesh();
    let mismatched = match mesh.ae_round(peer, cache.clone(), local_buckets).await {
        Ok(mismatched) => mismatched,
        Err(error) => {
            tracing::debug!(%error, "anti-entropy digest exchange failed");
            return;
        }
    };
    if mismatched.is_empty() {
        tracing::trace!("no mismatched buckets");
        return;
    }

    // One local shard pass for every mismatched bucket (a mostly-divergent
    // peer mismatches all 1,024; per-bucket scans would be quadratic here,
    // exactly as on the serving side).
    let local_entries = shard
        .entries_for_buckets(mismatched.iter().map(|&(bucket, _)| bucket).collect())
        .await;
    let local_by_bucket: HashMap<u16, Vec<(Bytes, crate::hlc::Hlc)>> =
        local_entries.into_iter().collect();

    let mut push_keys: Vec<Bytes> = Vec::new();
    let mut pull_keys: Vec<Bytes> = Vec::new();
    for (bucket, peer_entries) in mismatched {
        diff_bucket(
            local_by_bucket.get(&bucket).map_or(&[], Vec::as_slice),
            &peer_entries,
            &mut push_keys,
            &mut pull_keys,
        );
    }

    let mut repaired: u64 = 0;
    // Batched so a large divergence makes durable incremental progress: each
    // batch that lands raises local versions, shrinking the next round's
    // diff, instead of one all-or-nothing exchange racing a request timeout.
    for batch in push_keys.chunks(REPAIR_BATCH) {
        let msgs = shard
            .records_for(batch.to_vec())
            .await
            .into_iter()
            .map(|rec| Msg::Replicate {
                cache: cache.clone(),
                rec,
            });
        // One peer-table lock acquisition for the whole batch rather than
        // one per record (see `Mesh::send_many`'s docs).
        mesh.send_many(peer, MsgClass::Replicate, msgs);
        repaired += batch.len() as u64;
    }
    for batch in pull_keys.chunks(REPAIR_BATCH) {
        match mesh.ae_pull(peer, cache.clone(), batch.to_vec()).await {
            Ok(records) => {
                repaired += records.len() as u64;
                shard.apply_remote_batch(records).await;
            }
            Err(error) => {
                tracing::debug!(%error, repaired, "anti-entropy pull failed; keeping progress");
                break;
            }
        }
    }

    if repaired > 0 {
        metrics::counter!("sundog_ae_repaired_total", "cache" => cache.to_string())
            .increment(repaired);
    }
    tracing::debug!(repaired, "anti-entropy round complete");
}

fn diff_bucket(
    local_entries: &[(Bytes, crate::hlc::Hlc)],
    peer_entries: &[(Bytes, crate::hlc::Hlc)],
    push_keys: &mut Vec<Bytes>,
    pull_keys: &mut Vec<Bytes>,
) {
    let peer_by_key: HashMap<&Bytes, crate::hlc::Hlc> =
        peer_entries.iter().map(|(k, v)| (k, *v)).collect();
    let mut local_keys = HashSet::with_capacity(local_entries.len());

    for (key, local_ver) in local_entries {
        local_keys.insert(key);
        match peer_by_key.get(key) {
            Some(&peer_ver) if *local_ver > peer_ver => push_keys.push(key.clone()),
            Some(&peer_ver) if *local_ver < peer_ver => pull_keys.push(key.clone()),
            Some(_) => {}
            None => push_keys.push(key.clone()),
        }
    }
    for (key, _) in peer_entries {
        if !local_keys.contains(key) {
            pull_keys.push(key.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn jittered_stays_within_the_expected_band() {
        for _ in 0..200 {
            let d = jittered(Duration::from_millis(1000));
            assert!(d >= Duration::from_millis(500) && d < Duration::from_millis(1500));
        }
    }

    #[test]
    fn jittered_floors_at_one_millisecond_for_a_zero_interval() {
        assert!(jittered(Duration::ZERO) >= Duration::from_millis(1));
    }
}

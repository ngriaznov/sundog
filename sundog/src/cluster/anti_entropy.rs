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
//!
//! A mismatched bucket's reply takes one of two shapes
//! ([`crate::net::AeMismatch`]): a small bucket's full `(key, version)`
//! listing, diffed by [`diff_bucket`] exactly as before; a large bucket's
//! IBLT sketch instead (`super::sketch`'s module docs), diffed by building
//! the matching local sketch, subtracting, and peeling
//! ([`diff_decoded`] classifies the peeled result). A sketch that fails to
//! peel ([`super::sketch::Undecodable`]) falls back to requesting that
//! bucket's full listing via `Msg::AeEntries`, once, after every reply in
//! the round has been processed — so one hard-to-decode bucket never blocks
//! the rest of the round's sketches from being used.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rand::RngExt as _;
use rand::seq::IndexedRandom as _;
use smol_str::SmolStr;
use tokio_util::sync::CancellationToken;
use xxhash_rust::xxh3::xxh3_64;

use super::Cluster;
use super::sketch::{Decoded, Iblt};
use crate::hlc::Hlc;
use crate::net::{AeMismatch, MsgClass};
use crate::node::NodeId;
use crate::store::ShardOps;

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
    // exactly as on the serving side) — covers both the plain-listing and
    // sketch replies, since a sketch diff needs the local bucket's entries
    // to build its own comparison sketch from just as much as a listing
    // diff needs them.
    let local_entries = shard
        .entries_for_buckets(mismatched.iter().map(AeMismatch::bucket).collect())
        .await;
    let local_by_bucket: HashMap<u16, Vec<(Bytes, Hlc)>> = local_entries.into_iter().collect();

    let mut push_keys: Vec<Bytes> = Vec::new();
    let mut pull_keys: Vec<Bytes> = Vec::new();
    // Sketch-decoded pulls: only a key *hash* is known (never key bytes),
    // and `Msg::AePullHashes` is answered per bucket, so these queue
    // separately from `pull_keys` rather than merging with it.
    let mut pull_hashes: Vec<(u16, Vec<u64>)> = Vec::new();
    let mut undecodable_buckets: Vec<u16> = Vec::new();

    for mismatch in mismatched {
        match mismatch {
            AeMismatch::Bucket(bucket, peer_entries) => {
                diff_bucket(
                    local_by_bucket.get(&bucket).map_or(&[], Vec::as_slice),
                    &peer_entries,
                    &mut push_keys,
                    &mut pull_keys,
                );
            }
            AeMismatch::Sketch(bucket, cells) => {
                let entries: &[(Bytes, Hlc)] =
                    local_by_bucket.get(&bucket).map_or(&[], Vec::as_slice);
                // Sized from the *received* sketch's own cell count, not
                // this node's own `ae_sketch_cells` config — see
                // `sketch::Iblt::new`'s docs for why that keeps the two
                // sketches shape-compatible even if the two nodes' configs
                // have drifted apart.
                let mut local_sketch = Iblt::new(cells.len());
                for (key, ver) in entries {
                    local_sketch.insert(xxh3_64(key), *ver);
                }
                let remote_sketch = Iblt::from_cells(cells);
                match local_sketch.subtract(&remote_sketch).peel() {
                    Ok(decoded) => {
                        let mut hashes = Vec::new();
                        diff_decoded(entries, &decoded, &mut push_keys, &mut hashes);
                        if !hashes.is_empty() {
                            pull_hashes.push((bucket, hashes));
                        }
                        metrics::counter!(
                            "sundog_ae_sketch_total",
                            "cache" => cache.to_string(),
                            "outcome" => "decoded"
                        )
                        .increment(1);
                    }
                    Err(_) => {
                        undecodable_buckets.push(bucket);
                        metrics::counter!(
                            "sundog_ae_sketch_total",
                            "cache" => cache.to_string(),
                            "outcome" => "fallback"
                        )
                        .increment(1);
                    }
                }
            }
        }
    }

    // One fallback request for every bucket whose sketch failed to decode,
    // sent after every reply in the round has been classified rather than
    // per-bucket as they're found — a peer that sends several oversized
    // sketches gets one `AeEntries` round trip for all of them, not one
    // each.
    if !undecodable_buckets.is_empty() {
        match mesh.ae_entries(peer, cache.clone(), undecodable_buckets).await {
            Ok(fallback_buckets) => {
                for (bucket, peer_entries) in fallback_buckets {
                    diff_bucket(
                        local_by_bucket.get(&bucket).map_or(&[], Vec::as_slice),
                        &peer_entries,
                        &mut push_keys,
                        &mut pull_keys,
                    );
                }
            }
            Err(error) => {
                tracing::debug!(%error, "anti-entropy sketch-fallback listing failed");
            }
        }
    }

    let mut repaired: u64 = 0;
    // Batched so a large divergence makes durable incremental progress: each
    // batch that lands raises local versions, shrinking the next round's
    // diff, instead of one all-or-nothing exchange racing a request timeout.
    for batch in push_keys.chunks(REPAIR_BATCH) {
        let records = shard.records_for(batch.to_vec()).await;
        repaired += records.len() as u64;
        // The same budgeted `Msg::ReplicateBatch` chunking the live fan-out
        // uses (`cluster::batch_replicate`): a large repair travels as a
        // handful of full frames and outbox slots, not one `Msg::Replicate`
        // per record.
        let msgs = super::batch_replicate(cache, records);
        // One peer-table lock acquisition for the whole batch rather than
        // one per record (see `Mesh::send_many`'s docs).
        mesh.send_many(peer, MsgClass::Replicate, msgs);
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
    'buckets: for (bucket, hashes) in pull_hashes {
        for batch in hashes.chunks(REPAIR_BATCH) {
            match mesh
                .ae_pull_hashes(peer, cache.clone(), bucket, batch.to_vec())
                .await
            {
                Ok(records) => {
                    repaired += records.len() as u64;
                    shard.apply_remote_batch(records).await;
                }
                Err(error) => {
                    tracing::debug!(
                        %error, repaired,
                        "anti-entropy hash pull failed; keeping progress"
                    );
                    break 'buckets;
                }
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
    local_entries: &[(Bytes, Hlc)],
    peer_entries: &[(Bytes, Hlc)],
    push_keys: &mut Vec<Bytes>,
    pull_keys: &mut Vec<Bytes>,
) {
    let peer_by_key: HashMap<&Bytes, Hlc> = peer_entries.iter().map(|(k, v)| (k, *v)).collect();
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

/// The initiator's push/pull classification once an `AeSketch` reply
/// decodes ([`Iblt::peel`]'s [`Decoded`] success case) — mirrors
/// [`diff_bucket`]'s own rules over the peeled element lists instead of two
/// full listings: a key present (at different versions) in both
/// `decoded.only_left` (this node's contribution) and `decoded.only_right`
/// (the peer's) pushes if the local version is newer, pulls if the peer's
/// is; a key present only in `only_left` pushes; a key present only in
/// `only_right` pulls. The one departure from `diff_bucket`: a sketch never
/// carries key bytes, only a `key_hash`, so every pull here queues a hash
/// into `pull_hashes` (resolved through `Msg::AePullHashes`) rather than a
/// key — only a push, whose key hash always resolves back to an entry in
/// `local_entries` (the same list the local sketch was built from), queues
/// actual key bytes.
fn diff_decoded(
    local_entries: &[(Bytes, Hlc)],
    decoded: &Decoded,
    push_keys: &mut Vec<Bytes>,
    pull_hashes: &mut Vec<u64>,
) {
    let local_by_hash: HashMap<u64, &Bytes> = local_entries
        .iter()
        .map(|(key, _)| (xxh3_64(key), key))
        .collect();
    let local_only: HashMap<u64, Hlc> = decoded
        .only_left
        .iter()
        .map(|elem| (elem.key_hash, elem.ver))
        .collect();
    let remote_only: HashMap<u64, Hlc> = decoded
        .only_right
        .iter()
        .map(|elem| (elem.key_hash, elem.ver))
        .collect();

    for (&key_hash, &local_ver) in &local_only {
        match remote_only.get(&key_hash) {
            Some(&remote_ver) if remote_ver > local_ver => pull_hashes.push(key_hash),
            _ => {
                if let Some(&key) = local_by_hash.get(&key_hash) {
                    push_keys.push(key.clone());
                }
            }
        }
    }
    for &key_hash in remote_only.keys() {
        if !local_only.contains_key(&key_hash) {
            pull_hashes.push(key_hash);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::super::sketch::Elem;
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

    fn hlc(wall_ms: u64) -> Hlc {
        Hlc {
            wall_ms,
            logical: 0,
            node: NodeId::from(1),
        }
    }

    fn entry(key: &[u8]) -> (Bytes, u64) {
        (Bytes::copy_from_slice(key), xxh3_64(key))
    }

    #[test]
    fn diff_decoded_pushes_the_same_key_when_the_local_version_is_newer() {
        let (key, hash) = entry(b"k1");
        let local_entries = vec![(key.clone(), hlc(20))];
        let decoded = Decoded {
            only_left: vec![Elem {
                key_hash: hash,
                ver: hlc(20),
            }],
            only_right: vec![Elem {
                key_hash: hash,
                ver: hlc(10),
            }],
        };
        let (mut push, mut pull) = (Vec::new(), Vec::new());
        diff_decoded(&local_entries, &decoded, &mut push, &mut pull);
        assert_eq!(push, vec![key]);
        assert!(pull.is_empty());
    }

    #[test]
    fn diff_decoded_pulls_by_hash_when_the_remote_version_is_newer() {
        let (key, hash) = entry(b"k1");
        let local_entries = vec![(key, hlc(10))];
        let decoded = Decoded {
            only_left: vec![Elem {
                key_hash: hash,
                ver: hlc(10),
            }],
            only_right: vec![Elem {
                key_hash: hash,
                ver: hlc(20),
            }],
        };
        let (mut push, mut pull) = (Vec::new(), Vec::new());
        diff_decoded(&local_entries, &decoded, &mut push, &mut pull);
        assert!(push.is_empty());
        assert_eq!(pull, vec![hash]);
    }

    #[test]
    fn diff_decoded_pushes_a_local_only_key() {
        let (key, hash) = entry(b"k2");
        let local_entries = vec![(key.clone(), hlc(5))];
        let decoded = Decoded {
            only_left: vec![Elem {
                key_hash: hash,
                ver: hlc(5),
            }],
            only_right: Vec::new(),
        };
        let (mut push, mut pull) = (Vec::new(), Vec::new());
        diff_decoded(&local_entries, &decoded, &mut push, &mut pull);
        assert_eq!(push, vec![key]);
        assert!(pull.is_empty());
    }

    #[test]
    fn diff_decoded_pulls_a_remote_only_key_by_hash_alone() {
        // This node never held the key at all — `local_entries` is empty —
        // so the only thing `diff_decoded` can possibly queue for it is the
        // hash the peeled sketch reported, never key bytes it never had.
        let hash = xxh3_64(b"k3");
        let local_entries: Vec<(Bytes, Hlc)> = Vec::new();
        let decoded = Decoded {
            only_left: Vec::new(),
            only_right: vec![Elem {
                key_hash: hash,
                ver: hlc(7),
            }],
        };
        let (mut push, mut pull) = (Vec::new(), Vec::new());
        diff_decoded(&local_entries, &decoded, &mut push, &mut pull);
        assert!(push.is_empty());
        assert_eq!(pull, vec![hash]);
    }
}

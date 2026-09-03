//! Anti-entropy: every jittered `ae_interval`, each [`Mode::Replicated`]
//! cache reconciles itself against one live peer. The initiator sends its
//! 1,024 bucket digests; for each mismatch the peer answers with the bucket's
//! entry listing, or an IBLT sketch for a large bucket. The initiator diffs,
//! pushes what it has newer, and pulls what the peer has newer, so both sides
//! converge in one round. Tombstones take part as records with no value.
//!
//! A sketch that fails to peel is retried once as a full listing, after every
//! other reply in the round is handled. `Invalidation` caches never run this:
//! their nodes hold different subsets by design.

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
use super::sketch::{Cell, Decoded, Iblt};
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
    let mut skipped: HashMap<NodeId, u32> = HashMap::new();
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            () = tokio::time::sleep(jittered(ae_interval)) => {}
        }
        let Some((peer, was_dirty)) = pick_peer(&cluster) else {
            continue;
        };
        let skips = skipped.entry(peer).or_insert(0);
        if should_skip_round(cluster.peer_is_streaming(peer), *skips) {
            *skips += 1;
            if was_dirty {
                cluster.mesh().mark_dirty(peer);
            }
            tracing::trace!(%peer, "replicate traffic in motion; skipping this round");
            continue;
        }
        *skips = 0;
        // `run_round_against`'s own network calls carry an internal
        // `REQUEST_TIMEOUT`, but racing the whole round against `cancel`
        // too means a `Cluster::shutdown()` in progress never waits out
        // that timeout for `TaskTracker::wait()` to return.
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

/// Rounds in a row the scheduler will leave a streaming peer alone before
/// running one anyway: a steady trickle of writes must not starve
/// anti-entropy, only a burst defers it.
const MAX_STREAMING_SKIPS: u32 = 3;

/// Whether to skip this round against a peer: only while replicate traffic
/// is in motion, and never more than [`MAX_STREAMING_SKIPS`] times in a row.
fn should_skip_round(streaming: bool, skipped_so_far: u32) -> bool {
    streaming && skipped_so_far < MAX_STREAMING_SKIPS
}

/// The peer to run this round against and whether it was taken from the
/// dirty set (so a skipped round can hand the mark back).
fn pick_peer(cluster: &Cluster) -> Option<(NodeId, bool)> {
    let mut rng = rand::rng();
    let dirty = cluster.mesh().take_dirty_peers();
    if let Some(&peer) = dirty.choose(&mut rng) {
        for &other in dirty.iter().filter(|&&other| other != peer) {
            cluster.mesh().mark_dirty(other);
        }
        return Some((peer, true));
    }
    cluster
        .live_peer_ids()
        .choose(&mut rng)
        .map(|&peer| (peer, false))
}

/// One anti-entropy round against `peer`: exchanges digests, then diffs the
/// mismatched buckets in both directions. Keys this node has newer (or
/// `peer` lacks entirely) are pushed via the normal `Replicate` fan-out
/// path; keys `peer` has newer (or this node lacks) are pulled and applied
/// directly. Both directions run from one side's diff, since the responder
/// already reported its own entries for every mismatched bucket.
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

    // One local shard pass for every mismatched bucket, not one per bucket
    // (a mostly-divergent peer mismatches all 1,024, and per-bucket scans
    // would be quadratic). Covers both the plain-listing and sketch
    // replies: a sketch diff needs the local bucket's entries to build its
    // own comparison sketch, the same as a listing diff needs them.
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
                handle_sketch_mismatch(
                    cache,
                    bucket,
                    cells,
                    entries,
                    &mut push_keys,
                    &mut pull_hashes,
                    &mut undecodable_buckets,
                );
            }
        }
    }

    // One fallback request for every bucket whose sketch failed to decode,
    // sent once the round's replies are all classified: a peer that sends
    // several oversized sketches gets one `AeEntries` round trip for all of
    // them, not one each.
    if !undecodable_buckets.is_empty() {
        match mesh
            .ae_entries(peer, cache.clone(), undecodable_buckets)
            .await
        {
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

    apply_repairs(mesh, shard, cache, peer, push_keys, pull_keys, pull_hashes).await;
}

/// Applies a round's classified push/pull/hash-pull sets against `peer`:
/// pushes replicate outbound in [`REPAIR_BATCH`] chunks, pulls full records
/// and applies them locally the same way, and pulls the sketch-decoded
/// hash-only results per bucket through `Msg::AePullHashes`. Emits
/// `sundog_ae_repaired_total{cache}` for the round's total once done.
async fn apply_repairs(
    mesh: &crate::net::Mesh,
    shard: &Arc<dyn ShardOps>,
    cache: &SmolStr,
    peer: NodeId,
    push_keys: Vec<Bytes>,
    pull_keys: Vec<Bytes>,
    pull_hashes: Vec<(u16, Vec<u64>)>,
) {
    let mut repaired: u64 = 0;
    // Batched so a large divergence makes durable incremental progress:
    // each batch that lands raises local versions, shrinking the next
    // round's diff, instead of one all-or-nothing exchange racing a
    // request timeout.
    for batch in push_keys.chunks(REPAIR_BATCH) {
        let records = shard.records_for(batch.to_vec()).await;
        repaired += records.len() as u64;
        // `net::batch_replicate`'s budgeted chunking: a large repair
        // travels as a handful of full frames, not one `Msg::Replicate`
        // per record.
        let msgs = crate::net::batch_replicate(cache, records);
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

/// Classifies one `AeMismatch::Sketch(bucket, cells)` reply: builds the
/// local comparison sketch from `local_entries`, subtracts the received
/// one, and peels it. On success, [`diff_decoded`] classifies the peeled
/// result into `push_keys`/`pull_hashes` for `bucket`; on failure queues
/// `bucket` into `undecodable_buckets` for the `Msg::AeEntries` fallback.
/// Emits `sundog_ae_sketch_total{outcome}` and a matching `tracing` event
/// either way.
fn handle_sketch_mismatch(
    cache: &SmolStr,
    bucket: u16,
    cells: Vec<Cell>,
    local_entries: &[(Bytes, Hlc)],
    push_keys: &mut Vec<Bytes>,
    pull_hashes: &mut Vec<(u16, Vec<u64>)>,
    undecodable_buckets: &mut Vec<u16>,
) {
    // Sized from the received sketch's cell count, not this node's own
    // `ae_sketch_cells` config, so the two sketches stay shape-compatible
    // even if the two nodes' configs have drifted apart.
    let mut local_sketch = Iblt::new(cells.len());
    for (key, ver) in local_entries {
        local_sketch.insert(xxh3_64(key), *ver);
    }
    let remote_sketch = Iblt::from_cells(cells);
    if let Ok(decoded) = local_sketch.subtract(&remote_sketch).peel() {
        let mut hashes = Vec::new();
        diff_decoded(local_entries, &decoded, push_keys, &mut hashes);
        if !hashes.is_empty() {
            pull_hashes.push((bucket, hashes));
        }
        metrics::counter!(
            "sundog_ae_sketch_total",
            "cache" => cache.to_string(),
            "outcome" => "decoded"
        )
        .increment(1);
        tracing::debug!(outcome = "decoded", bucket, "anti-entropy sketch decoded");
    } else {
        undecodable_buckets.push(bucket);
        tracing::debug!(
            outcome = "fallback",
            bucket,
            "anti-entropy sketch undecodable; falling back to a full listing"
        );
        metrics::counter!(
            "sundog_ae_sketch_total",
            "cache" => cache.to_string(),
            "outcome" => "fallback"
        )
        .increment(1);
    }
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
/// decodes ([`Iblt::peel`]'s [`Decoded`] success case): mirrors
/// [`diff_bucket`]'s rules over the peeled element lists instead of two
/// full listings. A key present at different versions in both
/// `decoded.only_left` (this node's contribution) and `decoded.only_right`
/// (the peer's) pushes if the local version is newer, pulls if the peer's
/// is; a key present only in `only_left` pushes, only in `only_right`
/// pulls. Unlike `diff_bucket`, a sketch carries no key bytes, only a
/// `key_hash`, so every pull queues a hash into `pull_hashes` instead of a
/// key; only a push, whose hash resolves back to an entry in
/// `local_entries`, queues actual key bytes.
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
    fn a_streaming_peer_is_skipped_a_bounded_number_of_times() {
        assert!(!should_skip_round(false, 0), "idle peers are never skipped");
        for skipped in 0..MAX_STREAMING_SKIPS {
            assert!(should_skip_round(true, skipped));
        }
        assert!(
            !should_skip_round(true, MAX_STREAMING_SKIPS),
            "a steady trickle cannot starve anti-entropy"
        );
    }

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

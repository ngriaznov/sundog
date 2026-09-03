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
use crate::net::{AeMismatch, AePartReply, MsgClass};
use crate::node::NodeId;
use crate::store::ShardOps;

/// Runs anti-entropy for one shard while `cancel` stays live: every jittered
/// `ae_interval`, picks one live peer, a dirty-marked one first, and runs
/// one round against it.
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
        // Races the whole round against `cancel` too, so a `shutdown()` in
        // progress never waits out the round's internal `REQUEST_TIMEOUT`.
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            () = run_round_against(&cluster, &shard, &cache, peer) => {}
        }
    }
}

/// Keys per push/pull batch within one round. Bounds each `ae_pull`
/// request/response under the frame cap, so a large divergence repairs
/// incrementally across batches rather than in one oversized exchange.
const REPAIR_BATCH: usize = 4096;

/// A jittered delay around `interval`, uniformly in `[0.5, 1.5) * interval`
/// and floored at 1ms, so anti-entropy rounds don't land in lockstep.
fn jittered(interval: Duration) -> Duration {
    let base = interval.max(Duration::from_millis(1)).as_secs_f64();
    let factor = rand::rng().random_range(0.5..1.5);
    Duration::from_secs_f64((base * factor).max(0.001))
}

/// Rounds in a row the scheduler leaves a streaming peer alone before
/// running one anyway; a steady trickle must not starve anti-entropy.
const MAX_STREAMING_SKIPS: u32 = 3;

/// Whether to skip this round: only while traffic is in motion, never more than
/// [`MAX_STREAMING_SKIPS`] times running.
fn should_skip_round(streaming: bool, skipped_so_far: u32) -> bool {
    streaming && skipped_so_far < MAX_STREAMING_SKIPS
}

/// The choice for one round: a `dirty` peer wins whenever there is one,
/// leaving every other dirty peer in the returned give-back list for the
/// caller to re-mark; with none dirty, a `live` peer is chosen instead, with
/// an empty give-back. `None` when both are empty.
///
/// `live` takes ownership, not a slice, to match `dirty`'s shape: both come
/// from a caller that just built them fresh (`take_dirty_peers`,
/// `live_peer_ids`), and only `dirty` needs the transfer, on the branch that
/// consumes it into `give_back`.
#[allow(clippy::needless_pass_by_value)]
fn choose_peer(
    dirty: Vec<NodeId>,
    live: Vec<NodeId>,
    rng: &mut impl rand::Rng,
) -> Option<(NodeId, bool, Vec<NodeId>)> {
    if let Some(&peer) = dirty.choose(rng) {
        let give_back = dirty.into_iter().filter(|&other| other != peer).collect();
        return Some((peer, true, give_back));
    }
    live.choose(rng).map(|&peer| (peer, false, Vec::new()))
}

/// The peer for this round, and whether it came from the dirty set; a
/// skipped round can hand the mark back.
fn pick_peer(cluster: &Cluster) -> Option<(NodeId, bool)> {
    let mut rng = rand::rng();
    let dirty = cluster.mesh().take_dirty_peers();
    let live = cluster.live_peer_ids();
    let (peer, was_dirty, give_back) = choose_peer(dirty, live, &mut rng)?;
    for other in give_back {
        cluster.mesh().mark_dirty(other);
    }
    Some((peer, was_dirty))
}

/// One anti-entropy round against `peer`: exchanges digests, then diffs the
/// mismatched buckets. Keys this node has newer, or `peer` lacks, push via
/// the normal `Replicate` path; keys `peer` has newer, or this node lacks,
/// pull and apply directly.
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

    // Buckets past `ae_part_min_bucket` answered with part digests never
    // load their full entries here: that's the cost this feature removes.
    // Only the buckets answered with a listing or sketch go through
    // `entries_for_buckets`.
    let (part_digest_mismatches, bucket_mismatches): (Vec<AeMismatch>, Vec<AeMismatch>) =
        mismatched
            .into_iter()
            .partition(|m| matches!(m, AeMismatch::PartDigests(..)));

    let mut push_keys: Vec<Bytes> = Vec::new();
    let mut pull_keys: Vec<Bytes> = Vec::new();
    // Sketch-decoded pulls know only a key hash and are answered per bucket, so
    // they queue separately from `pull_keys`.
    let mut pull_hashes: Vec<(u16, Vec<u64>)> = Vec::new();
    let mut undecodable_buckets: Vec<u16> = Vec::new();

    classify_bucket_mismatches(
        shard,
        cache,
        bucket_mismatches,
        &mut push_keys,
        &mut pull_keys,
        &mut pull_hashes,
        &mut undecodable_buckets,
    )
    .await;
    classify_part_digest_mismatches(
        mesh,
        shard,
        cache,
        peer,
        part_digest_mismatches,
        &mut push_keys,
        &mut pull_keys,
        &mut pull_hashes,
        &mut undecodable_buckets,
    )
    .await;

    // One fallback request for every bucket whose sketch failed to decode,
    // sent once the round's replies are classified: several oversized
    // sketches get one `AeEntries` round trip, not one each. Buckets that
    // reach here may come from either the bucket path or the part path, so
    // their entries are fetched fresh rather than reusing either path's
    // already-scoped local lookup.
    if !undecodable_buckets.is_empty() {
        match mesh
            .ae_entries(peer, cache.clone(), undecodable_buckets)
            .await
        {
            Ok(fallback_buckets) => {
                let wanted: Vec<u16> = fallback_buckets.iter().map(|(bucket, _)| *bucket).collect();
                let local_entries = shard.entries_for_buckets(wanted).await;
                let local_by_bucket: HashMap<u16, Vec<(Bytes, Hlc)>> =
                    local_entries.into_iter().collect();
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

/// Classifies buckets answered with a listing or sketch
/// (`AeMismatch::Bucket`/`Sketch`): one local shard pass for all of them at
/// once, not one per bucket, since a mostly-divergent peer mismatches many
/// and per-bucket scans would be quadratic; each is then classified into
/// `push_keys`/`pull_keys` directly, or, for a sketch, via
/// [`handle_sketch_mismatch`]. A no-op when `mismatches` is empty.
async fn classify_bucket_mismatches(
    shard: &Arc<dyn ShardOps>,
    cache: &SmolStr,
    mismatches: Vec<AeMismatch>,
    push_keys: &mut Vec<Bytes>,
    pull_keys: &mut Vec<Bytes>,
    pull_hashes: &mut Vec<(u16, Vec<u64>)>,
    undecodable_buckets: &mut Vec<u16>,
) {
    if mismatches.is_empty() {
        return;
    }
    let local_entries = shard
        .entries_for_buckets(mismatches.iter().map(AeMismatch::bucket).collect())
        .await;
    let local_by_bucket: HashMap<u16, Vec<(Bytes, Hlc)>> = local_entries.into_iter().collect();

    for mismatch in mismatches {
        match mismatch {
            AeMismatch::Bucket(bucket, peer_entries) => {
                diff_bucket(
                    local_by_bucket.get(&bucket).map_or(&[], Vec::as_slice),
                    &peer_entries,
                    push_keys,
                    pull_keys,
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
                    push_keys,
                    pull_hashes,
                    undecodable_buckets,
                );
            }
            AeMismatch::PartDigests(..) => {
                unreachable!("invariant: run_round_against partitions this variant out")
            }
        }
    }
}

/// Classifies buckets answered with part digests (`AeMismatch::PartDigests`):
/// compares each against this node's own part digests for the same bucket,
/// one shard call for all of them, then one [`Mesh::ae_parts`] request for
/// every part that actually differs, classifying each reply the same way
/// [`classify_bucket_mismatches`] does at bucket scale. A no-op when
/// `mismatches` is empty.
///
/// [`Mesh::ae_parts`]: crate::net::Mesh::ae_parts
#[allow(clippy::too_many_arguments)]
async fn classify_part_digest_mismatches(
    mesh: &crate::net::Mesh,
    shard: &Arc<dyn ShardOps>,
    cache: &SmolStr,
    peer: NodeId,
    mismatches: Vec<AeMismatch>,
    push_keys: &mut Vec<Bytes>,
    pull_keys: &mut Vec<Bytes>,
    pull_hashes: &mut Vec<(u16, Vec<u64>)>,
    undecodable_buckets: &mut Vec<u16>,
) {
    if mismatches.is_empty() {
        return;
    }
    let buckets: Vec<u16> = mismatches.iter().map(AeMismatch::bucket).collect();
    let local_part_digests = shard.part_digests(buckets).await;
    let local_digests_by_bucket: HashMap<u16, Vec<u64>> = local_part_digests.into_iter().collect();

    let mut wanted_parts: Vec<(u16, u8)> = Vec::new();
    for mismatch in &mismatches {
        let AeMismatch::PartDigests(bucket, remote_parts) = mismatch else {
            unreachable!("invariant: run_round_against partitions in only this variant")
        };
        let local_parts = local_digests_by_bucket
            .get(bucket)
            .map_or(&[][..], Vec::as_slice);
        wanted_parts.extend(
            mismatched_parts(local_parts, remote_parts)
                .into_iter()
                .map(|part| (*bucket, part)),
        );
    }
    if wanted_parts.is_empty() {
        return;
    }

    match mesh
        .ae_parts(peer, cache.clone(), wanted_parts.clone())
        .await
    {
        Ok(replies) => {
            let local_part_entries = shard.entries_for_parts(wanted_parts).await;
            let local_by_part: HashMap<(u16, u8), Vec<(Bytes, Hlc)>> =
                local_part_entries.into_iter().collect();
            for reply in replies {
                match reply {
                    AePartReply::Listing {
                        bucket,
                        part,
                        entries,
                    } => {
                        diff_bucket(
                            local_by_part
                                .get(&(bucket, part))
                                .map_or(&[], Vec::as_slice),
                            &entries,
                            push_keys,
                            pull_keys,
                        );
                        metrics::counter!(
                            "sundog_ae_parts_total",
                            "cache" => cache.to_string(),
                            "outcome" => "listing"
                        )
                        .increment(1);
                        tracing::debug!(
                            outcome = "listing",
                            bucket,
                            part,
                            "anti-entropy part listing"
                        );
                    }
                    AePartReply::Sketch {
                        bucket,
                        part,
                        cells,
                    } => {
                        let entries: &[(Bytes, Hlc)] = local_by_part
                            .get(&(bucket, part))
                            .map_or(&[], Vec::as_slice);
                        handle_part_sketch_mismatch(
                            cache,
                            bucket,
                            cells,
                            entries,
                            push_keys,
                            pull_hashes,
                            undecodable_buckets,
                        );
                    }
                }
            }
        }
        Err(error) => {
            tracing::debug!(%error, "anti-entropy part exchange failed");
        }
    }
}

/// Applies a round's classified push/pull/hash-pull sets against `peer`, in
/// [`REPAIR_BATCH`] chunks, and emits `sundog_ae_repaired_total{cache}`.
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
    // each landed batch shrinks the next round's diff, instead of one
    // all-or-nothing exchange racing a request timeout.
    for batch in push_keys.chunks(REPAIR_BATCH) {
        let records = shard.records_for(batch.to_vec()).await;
        repaired += records.len() as u64;
        // `net::batch_replicate` chunks this into a handful of full frames, not
        // one `Msg::Replicate` per record.
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

/// Classifies one `AeMismatch::Sketch(bucket, cells)` reply: builds a local
/// comparison sketch, subtracts the received one, and peels it. On success,
/// [`diff_decoded`] classifies the result into `push_keys`/`pull_hashes`; on
/// failure queues `bucket` into `undecodable_buckets` for the
/// `Msg::AeEntries` fallback. Emits `sundog_ae_sketch_total{outcome}` either
/// way.
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
    // config, so the two sketches stay shape-compatible if configs drift. A
    // cell count `Iblt::new` cannot reproduce fails `subtract`, and falls
    // back like any other undecodable sketch.
    let mut local_sketch = Iblt::new(cells.len());
    for (key, ver) in local_entries {
        local_sketch.insert(xxh3_64(key), *ver);
    }
    let remote_sketch = Iblt::from_cells(cells);
    if let Ok(decoded) = local_sketch.subtract(&remote_sketch).and_then(Iblt::peel) {
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

/// The parts, of a bucket answered with [`AeMismatch::PartDigests`], whose
/// local and remote digest differ: an index-wise comparison of `local` and
/// `remote`'s [`crate::store::PART_COUNT`] values. A ragged pair, which only
/// a misbehaving peer sends, treats any index either side lacks as mismatched
/// rather than panicking or silently skipping it.
///
/// Reachable outside `cluster::anti_entropy` only because `tests/sim.rs`
/// re-exports it as `crate::mismatched_parts` under `feature = "sim"`, to
/// classify a peer's `AeMismatch::PartDigests` reply the same way
/// `run_round_against` does here, without duplicating this comparison.
#[must_use]
pub fn mismatched_parts(local: &[u64], remote: &[u64]) -> Vec<u8> {
    let len = local.len().max(remote.len());
    (0..len)
        .filter(|&i| local.get(i).copied().unwrap_or(0) != remote.get(i).copied().unwrap_or(0))
        .filter_map(|i| u8::try_from(i).ok())
        .collect()
}

/// Classifies one part's `AePartReply::Sketch` reply: the same peel logic as
/// [`handle_sketch_mismatch`], scoped to a single part's local entries, but
/// emitting `sundog_ae_parts_total{outcome}` in place of
/// `sundog_ae_sketch_total`. On failure, `bucket` queues into
/// `undecodable_buckets` for the existing whole-bucket `Msg::AeEntries`
/// fallback: a part sketch never gets its own part-scoped fallback.
fn handle_part_sketch_mismatch(
    cache: &SmolStr,
    bucket: u16,
    cells: Vec<Cell>,
    local_entries: &[(Bytes, Hlc)],
    push_keys: &mut Vec<Bytes>,
    pull_hashes: &mut Vec<(u16, Vec<u64>)>,
    undecodable_buckets: &mut Vec<u16>,
) {
    let mut local_sketch = Iblt::new(cells.len());
    for (key, ver) in local_entries {
        local_sketch.insert(xxh3_64(key), *ver);
    }
    let remote_sketch = Iblt::from_cells(cells);
    if let Ok(decoded) = local_sketch.subtract(&remote_sketch).and_then(Iblt::peel) {
        let mut hashes = Vec::new();
        diff_decoded(local_entries, &decoded, push_keys, &mut hashes);
        if !hashes.is_empty() {
            pull_hashes.push((bucket, hashes));
        }
        metrics::counter!(
            "sundog_ae_parts_total",
            "cache" => cache.to_string(),
            "outcome" => "sketch"
        )
        .increment(1);
        tracing::debug!(
            outcome = "sketch",
            bucket,
            "anti-entropy part sketch decoded"
        );
    } else {
        undecodable_buckets.push(bucket);
        tracing::debug!(
            outcome = "fallback",
            bucket,
            "anti-entropy part sketch undecodable; falling back to a full bucket listing"
        );
        metrics::counter!(
            "sundog_ae_parts_total",
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
/// decodes: mirrors [`diff_bucket`]'s rules over [`Iblt::peel`]'s peeled
/// element lists instead of two full listings. A key newer in
/// `decoded.only_left` (this node) than in `only_right` (the peer's) pushes;
/// newer on the peer's side pulls; present in only one side follows suit.
/// A sketch carries no key bytes, only a `key_hash`, so every pull queues a
/// hash into `pull_hashes`; only a push resolves its hash back to
/// `local_entries` and queues actual key bytes.
///
/// Reachable outside `cluster::anti_entropy` only because `tests/sim.rs`
/// re-exports it as `crate::diff_decoded` under `feature = "sim"`, to
/// reconcile a peeled sketch the same way [`handle_sketch_mismatch`] does
/// here, without duplicating this classification.
pub fn diff_decoded(
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
    fn choose_peer_prefers_a_dirty_peer_over_live_ones() {
        let mut rng = rand::rng();
        let dirty = vec![NodeId::from(1)];
        let live = vec![NodeId::from(2), NodeId::from(3)];
        let (peer, was_dirty, give_back) =
            choose_peer(dirty, live, &mut rng).expect("a peer is chosen");
        assert_eq!(peer, NodeId::from(1));
        assert!(was_dirty);
        assert!(give_back.is_empty());
    }

    #[test]
    fn choose_peer_hands_back_the_unchosen_dirty_peer() {
        let mut rng = rand::rng();
        let dirty = vec![NodeId::from(1), NodeId::from(2)];
        let (peer, was_dirty, give_back) =
            choose_peer(dirty.clone(), Vec::new(), &mut rng).expect("a peer is chosen");
        assert!(was_dirty);
        let other = dirty
            .into_iter()
            .find(|&p| p != peer)
            .expect("two dirty peers, one chosen");
        assert_eq!(give_back, vec![other]);
    }

    #[test]
    fn choose_peer_falls_back_to_a_live_peer_with_no_dirty_ones() {
        let mut rng = rand::rng();
        let live = vec![NodeId::from(5)];
        let (peer, was_dirty, give_back) =
            choose_peer(Vec::new(), live, &mut rng).expect("a peer is chosen");
        assert_eq!(peer, NodeId::from(5));
        assert!(!was_dirty);
        assert!(give_back.is_empty());
    }

    #[test]
    fn choose_peer_returns_none_with_no_peers_at_all() {
        let mut rng = rand::rng();
        assert_eq!(choose_peer(Vec::new(), Vec::new(), &mut rng), None);
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

    fn mismatch_of(cells: Vec<Cell>, local_entries: &[(Bytes, Hlc)]) -> SketchOutcome {
        let mut out = SketchOutcome::default();
        handle_sketch_mismatch(
            &SmolStr::new("users"),
            7,
            cells,
            local_entries,
            &mut out.push_keys,
            &mut out.pull_hashes,
            &mut out.undecodable_buckets,
        );
        out
    }

    #[derive(Default)]
    struct SketchOutcome {
        push_keys: Vec<Bytes>,
        pull_hashes: Vec<(u16, Vec<u64>)>,
        undecodable_buckets: Vec<u16>,
    }

    #[test]
    fn a_well_formed_sketch_decodes_into_hashes_to_pull() {
        let mut remote = Iblt::new(240);
        remote.insert(xxh3_64(b"k1"), hlc(5));
        let out = mismatch_of(remote.into_cells(), &[]);
        assert!(out.push_keys.is_empty());
        assert_eq!(out.pull_hashes, vec![(7, vec![xxh3_64(b"k1")])]);
        assert!(out.undecodable_buckets.is_empty());
    }

    #[test]
    fn an_empty_sketch_off_the_wire_falls_back_to_a_full_listing() {
        let out = mismatch_of(Vec::new(), &[(Bytes::from_static(b"k1"), hlc(5))]);
        assert!(out.push_keys.is_empty());
        assert!(out.pull_hashes.is_empty());
        assert_eq!(out.undecodable_buckets, vec![7]);
    }

    #[test]
    fn a_sketch_of_an_unreproducible_shape_falls_back_to_a_full_listing() {
        // 100 cells: `Iblt::new(100)` builds 99, so the two never line up.
        let out = mismatch_of(
            vec![Cell::default(); 100],
            &[(Bytes::from_static(b"k1"), hlc(5))],
        );
        assert!(out.push_keys.is_empty());
        assert!(out.pull_hashes.is_empty());
        assert_eq!(out.undecodable_buckets, vec![7]);
    }

    #[test]
    fn mismatched_parts_reports_only_the_differing_indices() {
        let local = vec![1u64, 2, 3, 4];
        let remote = vec![1u64, 9, 3, 8];
        assert_eq!(mismatched_parts(&local, &remote), vec![1, 3]);
    }

    #[test]
    fn mismatched_parts_is_empty_for_identical_digests() {
        let digests: Vec<u64> = (0..64).collect();
        assert!(mismatched_parts(&digests, &digests).is_empty());
    }

    #[test]
    fn mismatched_parts_treats_a_ragged_pair_as_mismatched_at_the_missing_index() {
        let local = vec![1u64, 2, 3];
        let remote = vec![1u64, 2];
        assert_eq!(
            mismatched_parts(&local, &remote),
            vec![2],
            "an index only one side has counts as mismatched, not skipped"
        );
    }

    fn part_mismatch_of(cells: Vec<Cell>, local_entries: &[(Bytes, Hlc)]) -> SketchOutcome {
        let mut out = SketchOutcome::default();
        handle_part_sketch_mismatch(
            &SmolStr::new("users"),
            7,
            cells,
            local_entries,
            &mut out.push_keys,
            &mut out.pull_hashes,
            &mut out.undecodable_buckets,
        );
        out
    }

    #[test]
    fn a_well_formed_part_sketch_decodes_into_hashes_to_pull() {
        let mut remote = Iblt::new(240);
        remote.insert(xxh3_64(b"k1"), hlc(5));
        let out = part_mismatch_of(remote.into_cells(), &[]);
        assert!(out.push_keys.is_empty());
        assert_eq!(out.pull_hashes, vec![(7, vec![xxh3_64(b"k1")])]);
        assert!(out.undecodable_buckets.is_empty());
    }

    #[test]
    fn an_undecodable_part_sketch_queues_its_bucket_for_the_whole_bucket_fallback() {
        let out = part_mismatch_of(
            vec![Cell::default(); 100],
            &[(Bytes::from_static(b"k1"), hlc(5))],
        );
        assert!(out.push_keys.is_empty());
        assert!(out.pull_hashes.is_empty());
        assert_eq!(out.undecodable_buckets, vec![7]);
    }

    #[test]
    fn diff_decoded_pulls_a_remote_only_key_by_hash_alone() {
        // This node never held the key; `local_entries` is empty, so the
        // only thing to queue is the hash the peeled sketch reported.
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

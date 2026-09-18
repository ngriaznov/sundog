//! Anti-entropy: every jittered `ae_interval`, each [`Mode::Replicated`]
//! cache reconciles itself against one live peer. The initiator sends its
//! 1,024 bucket digests; for each mismatch the peer answers with the bucket's
//! entry listing, or an IBLT sketch for a large bucket. The initiator diffs,
//! pushes what it has newer, and pulls what the peer has newer, so both sides
//! converge in one round. When the shard's resolver merges
//! (`ShardOps::merges` is `true`), a version-mismatched key
//! is pushed *and* pulled instead of only in the greater side's direction, so
//! two replicas each holding half of a merge exchange records in this same
//! round rather than needing a second round to carry the minted result back.
//! Tombstones take part as records with no value.
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
use crate::net::{AeMismatch, AePartReply, AeRoundOutcome, Mesh, MsgClass};
use crate::node::NodeId;
use crate::store::{BucketPart, ShardOps, bucket_of};
use crate::wire::{self, WireRecord};

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
        if cancel
            .run_until_cancelled(tokio::time::sleep(jittered(ae_interval)))
            .await
            .is_none()
        {
            return;
        }
        let Some((peer, was_dirty)) = pick_peer(&cluster, &shard) else {
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
            _ = run_round_against(cluster.mesh(), &shard, &cache, peer) => {}
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
/// from a caller that builds them fresh this round (`take_dirty_peers`,
/// `live_peer_ids`), and only `dirty` needs the transfer, on the branch that
/// consumes it into `give_back`.
#[expect(
    clippy::needless_pass_by_value,
    reason = "live takes ownership, not a slice, to match dirty's shape: both come from a \
              caller that builds them fresh this round, and only dirty needs the transfer, on \
              the branch that consumes it into give_back"
)]
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
/// skipped round can hand the mark back. `shard`'s
/// [`ShardOps::ae_peer_filter`] narrows both candidate sets to its current
/// cohort first: identity for every mode but `Mode::Distributed`, whose
/// override drops a peer sharing no bucket this shard currently owns or is
/// mid disown-grace on, since such a peer has nothing to reconcile.
fn pick_peer(cluster: &Cluster, shard: &Arc<dyn ShardOps>) -> Option<(NodeId, bool)> {
    let mut rng = rand::rng();
    let dirty = cluster.mesh().take_dirty_peers();
    let live = cluster.live_peer_ids();
    let (dirty, live) = shard.ae_peer_filter(dirty, live);
    let (peer, was_dirty, give_back) = choose_peer(dirty, live, &mut rng)?;
    for other in give_back {
        cluster.mesh().mark_dirty(other);
    }
    Some((peer, was_dirty))
}

/// How one [`run_round_against`] ended: whether the digest exchange with
/// the peer completed, so its repairs ran, or the round stopped before any
/// repair. A bucket hand-off in `rebalance::rebalance_task` releases a
/// bucket only once a round against each of its new owners reports
/// [`RoundOutcome::Reconciled`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoundOutcome {
    /// The digest exchange completed and every mismatch is repaired as far
    /// as the peer answered.
    Reconciled,
    /// The peer's ownership view differs from this node's: no repair ran.
    Stale,
    /// The digest exchange itself failed: no repair ran.
    Failed,
}

/// One anti-entropy round against `peer`: exchanges digests, then diffs the
/// mismatched buckets. A key this node has newer, or `peer` lacks, pushes
/// via the normal `Replicate` path; a key `peer` has newer, or this node
/// lacks, pulls and applies directly. When `shard`'s resolver reports
/// [`ShardOps::merges`], a key present on both sides under different
/// versions pushes *and* pulls, so two replicas each holding half of a
/// merge converge in this one round rather than needing a second.
///
/// A `Mode::Distributed` shard exchanges digests through
/// `Mesh::ae_round_scoped` instead of [`Mesh::ae_round`], carrying its
/// [`ShardOps::ownership_view_hash`] for the epoch check; a `Stale` reply
/// ends the round at once, counted in `sundog_stale_view_total`.
///
/// Takes `mesh` directly, not a whole `&Cluster`: `tests/sim.rs` drives
/// this same seam under `feature = "sim"` against a hand-built
/// `Mesh`/`ShardOps` pair, skipping a real `Cluster`'s gossip and discovery.
#[tracing::instrument(skip_all, fields(cache = %cache, peer = %peer))]
pub async fn run_round_against(
    mesh: &Mesh,
    shard: &Arc<dyn ShardOps>,
    cache: &SmolStr,
    peer: NodeId,
) -> RoundOutcome {
    // Read once per round: a merging resolver has both sides exchange a
    // mismatched key instead of only the greater version pushing to the
    // lesser side. See `ShardOps::merges`.
    let merging = shard.merges();
    let mismatched = match shard.ownership_view_hash() {
        Some(view_hash) => {
            // Only the buckets `peer` co-owns: the whole resident list
            // would have every other bucket reported as a mismatch and
            // its entries pushed only to be dropped by the peer's inbound
            // guard.
            let local_buckets: Vec<(u16, u64)> = shard
                .ae_digests_for(peer)
                .await
                .into_iter()
                .map(|bd| (bd.bucket, bd.digest))
                .collect();
            match mesh
                .ae_round_scoped(peer, cache.clone(), view_hash, local_buckets)
                .await
            {
                Ok(AeRoundOutcome::Mismatches(mismatched)) => mismatched,
                Ok(AeRoundOutcome::Stale {
                    responder_view_hash,
                }) => {
                    metrics::counter!("sundog_stale_view_total", "cache" => cache.to_string())
                        .increment(1);
                    tracing::debug!(
                        responder_view_hash,
                        "anti-entropy round ended: responder's view has diverged"
                    );
                    return RoundOutcome::Stale;
                }
                Err(error) => {
                    tracing::debug!(%error, "anti-entropy scoped digest exchange failed");
                    return RoundOutcome::Failed;
                }
            }
        }
        None => match mesh
            .ae_round(
                peer,
                cache.clone(),
                shard
                    .digests()
                    .await
                    .into_iter()
                    .map(|bd| (bd.bucket, bd.digest))
                    .collect(),
            )
            .await
        {
            Ok(mismatched) => mismatched,
            Err(error) => {
                tracing::debug!(%error, "anti-entropy digest exchange failed");
                return RoundOutcome::Failed;
            }
        },
    };
    if mismatched.is_empty() {
        tracing::trace!("no mismatched buckets");
        return RoundOutcome::Reconciled;
    }

    let _ = reconcile_mismatches(mesh, shard, cache, peer, mismatched, merging).await;
    RoundOutcome::Reconciled
}

/// The classify-through-repair pipeline shared by [`run_round_against`] and
/// [`run_round_for_buckets`]: classifies one round's `mismatched` reply
/// into a [`RepairPlan`], issues the `Msg::AeEntries` fallback for a
/// sketch that failed to decode, then runs [`apply_repairs`]. Returns the
/// total wire bytes moved.
///
/// Buckets past `ae_part_min_bucket` answered with part digests never
/// load their full entries here; only listing/sketch buckets go through
/// `entries_for_buckets`.
async fn reconcile_mismatches(
    mesh: &Mesh,
    shard: &Arc<dyn ShardOps>,
    cache: &SmolStr,
    peer: NodeId,
    mismatched: Vec<AeMismatch>,
    merging: bool,
) -> u64 {
    let (part_digest_mismatches, bucket_mismatches): (Vec<AeMismatch>, Vec<AeMismatch>) =
        mismatched
            .into_iter()
            .partition(|m| matches!(m, AeMismatch::PartDigests(..)));

    let mut plan = RepairPlan::default();
    classify_bucket_mismatches(shard, cache, bucket_mismatches, &mut plan, merging).await;
    classify_part_digest_mismatches(
        mesh,
        shard,
        cache,
        peer,
        part_digest_mismatches,
        &mut plan,
        merging,
    )
    .await;

    // One fallback request for every bucket whose sketch failed to decode,
    // sent once the round's replies are classified: several oversized
    // sketches get one `AeEntries` round trip, not one each. Buckets that
    // reach here may come from either the bucket path or the part path, so
    // their entries are fetched fresh rather than reusing either path's
    // already-scoped local lookup.
    if !plan.undecodable_buckets.is_empty() {
        match mesh
            .ae_entries(
                peer,
                cache.clone(),
                std::mem::take(&mut plan.undecodable_buckets),
            )
            .await
        {
            Ok(fallback_buckets) => {
                let wanted: Vec<u16> = fallback_buckets.iter().map(|(bucket, _)| *bucket).collect();
                let local_entries = shard.entries_for_buckets(wanted).await;
                let local_by_bucket: HashMap<u16, Vec<(Bytes, Hlc)>> = local_entries
                    .into_iter()
                    .map(|(bucket, entries)| (bucket, key_versions_to_tuples(entries)))
                    .collect();
                for (bucket, peer_entries) in fallback_buckets {
                    diff_bucket(
                        local_by_bucket.get(&bucket).map_or(&[], Vec::as_slice),
                        &key_versions_to_tuples(peer_entries),
                        &mut plan.push_keys,
                        &mut plan.pull_keys,
                        merging,
                    );
                }
            }
            Err(error) => {
                tracing::debug!(%error, "anti-entropy sketch-fallback listing failed");
            }
        }
    }

    retain_owned_pulls(shard, &mut plan.pull_keys, &mut plan.pull_hashes);
    apply_repairs(
        mesh,
        shard,
        cache,
        peer,
        plan.push_keys,
        plan.pull_keys,
        plan.pull_hashes,
    )
    .await
}

/// How one [`run_round_for_buckets`] round ended, per bucket: which
/// matched, which were mismatched, wire bytes moved, and whether it failed.
// Only exercised by this file's own real-transport tests today;
// `Cache::reconcile_warm_buckets` wires it in once that rewrite lands.
#[cfg_attr(any(not(test), feature = "sim"), allow(dead_code))]
#[derive(Debug, Clone, Default)]
pub(crate) struct BucketRoundOutcome {
    /// No mismatch this round: converged at this instant, not a claim of
    /// permanent equality.
    pub(crate) matched: HashSet<u16>,
    /// Named mismatched, or requested but dropped because this shard's
    /// fresh ownership read disagrees `peer` co-owns it; either way never
    /// compared this round, so never `matched`.
    pub(crate) still_diverged: HashSet<u16>,
    /// Wire bytes pushed plus pulled this round: `0` when nothing
    /// mismatched or the round failed.
    pub(crate) bytes_moved: u64,
    /// The digest exchange errored, the shard has no ownership view, or
    /// the peer answered `Stale`: no repair ran.
    pub(crate) failed: bool,
}

/// [`run_round_for_buckets`]'s outcome for a round that could not be
/// scoped or answered at all: everything requested counts as `still_diverged`.
#[cfg_attr(any(not(test), feature = "sim"), allow(dead_code))]
fn every_bucket_diverged(requested: &HashSet<u16>) -> BucketRoundOutcome {
    BucketRoundOutcome {
        matched: HashSet::new(),
        still_diverged: requested.clone(),
        bytes_moved: 0,
        failed: true,
    }
}

/// Bucket-scoped sibling of [`run_round_against`]: scoped to `buckets`
/// and reports which specifically matched instead of collapsing to one
/// [`RoundOutcome`]. `Cache::reconcile_warm_buckets`'s converge-before-
/// serving loop calls this per live co-owner, per round.
///
/// Filters local digests to `buckets` before the request goes out; a
/// requested bucket the live ownership view says `peer` does not
/// co-own is silently absent from `sent` and lands in `still_diverged`
/// rather than `matched`. A shard with no ownership view, a `Stale`
/// reply, or a failed exchange reports everything `still_diverged` with
/// `failed: true`.
// Only exercised by this file's own real-transport tests today;
// `Cache::reconcile_warm_buckets` wires it in once that rewrite lands.
#[cfg_attr(any(not(test), feature = "sim"), allow(dead_code))]
pub(crate) async fn run_round_for_buckets(
    mesh: &Mesh,
    shard: &Arc<dyn ShardOps>,
    cache: &SmolStr,
    peer: NodeId,
    buckets: &[u16],
) -> BucketRoundOutcome {
    let requested: HashSet<u16> = buckets.iter().copied().collect();
    let Some(view_hash) = shard.ownership_view_hash() else {
        return every_bucket_diverged(&requested);
    };
    let merging = shard.merges();
    let local_buckets: Vec<(u16, u64)> = shard
        .ae_digests_for(peer)
        .await
        .into_iter()
        .filter(|bd| requested.contains(&bd.bucket))
        .map(|bd| (bd.bucket, bd.digest))
        .collect();
    // Every bucket this round sent, per the fresh ownership read
    // above, not `requested`; only `sent` buckets get a real answer.
    let sent: HashSet<u16> = local_buckets.iter().map(|&(bucket, _)| bucket).collect();
    let mismatched = match mesh
        .ae_round_scoped(peer, cache.clone(), view_hash, local_buckets)
        .await
    {
        Ok(AeRoundOutcome::Mismatches(mismatched)) => mismatched,
        Ok(AeRoundOutcome::Stale {
            responder_view_hash,
        }) => {
            metrics::counter!("sundog_stale_view_total", "cache" => cache.to_string()).increment(1);
            tracing::debug!(
                responder_view_hash,
                "anti-entropy bucket-scoped round ended: responder's view has diverged"
            );
            return every_bucket_diverged(&requested);
        }
        Err(error) => {
            tracing::debug!(%error, "anti-entropy bucket-scoped digest exchange failed");
            return every_bucket_diverged(&requested);
        }
    };
    let named_mismatched: HashSet<u16> = mismatched.iter().map(AeMismatch::bucket).collect();
    // matched: sent buckets that came back unnamed; a bucket dropped
    // from sent was never digest-compared, so it folds into still_diverged.
    let matched: HashSet<u16> = sent.difference(&named_mismatched).copied().collect();
    let still_diverged: HashSet<u16> = requested.difference(&matched).copied().collect();
    if mismatched.is_empty() {
        return BucketRoundOutcome {
            matched,
            still_diverged,
            bytes_moved: 0,
            failed: false,
        };
    }
    let bytes_moved = reconcile_mismatches(mesh, shard, cache, peer, mismatched, merging).await;
    BucketRoundOutcome {
        matched,
        still_diverged,
        bytes_moved,
        failed: false,
    }
}

/// Drops every queued pull for a bucket a `Mode::Distributed` shard does
/// not own: a round against a new owner of a bucket this node is releasing
/// pushes what the owner lacks and leaves the owner's own entries where
/// they are, instead of pulling them into the inbound guard. Identity for
/// a shard with no ownership view.
fn retain_owned_pulls(
    shard: &Arc<dyn ShardOps>,
    pull_keys: &mut Vec<Bytes>,
    pull_hashes: &mut Vec<(u16, Vec<u64>)>,
) {
    if let Some(view) = shard.ownership_view() {
        pull_keys.retain(|key| view.owns(bucket_of(key)));
        pull_hashes.retain(|(bucket, _)| view.owns(*bucket));
    }
}

/// One round's push/pull/hash-pull/fallback classification, threaded as
/// `&mut RepairPlan` instead of four separate out-parameters.
#[derive(Default)]
struct RepairPlan {
    push_keys: Vec<Bytes>,
    pull_keys: Vec<Bytes>,
    pull_hashes: Vec<(u16, Vec<u64>)>,
    undecodable_buckets: Vec<u16>,
}

impl RepairPlan {
    /// Queues `hashes` to pull from `bucket` by hash; a no-op if empty.
    fn pull_by_hash(&mut self, bucket: u16, hashes: Vec<u64>) {
        if !hashes.is_empty() {
            self.pull_hashes.push((bucket, hashes));
        }
    }

    /// Queues `bucket` for the whole-bucket `Msg::AeEntries` fallback.
    fn mark_undecodable(&mut self, bucket: u16) {
        self.undecodable_buckets.push(bucket);
    }
}

/// Whether a version mismatch pushes, pulls, or both, as `(push, pull)`:
/// either side missing sends toward the side that holds it; both present
/// sends the greater version's side, and `merging` additionally sends the
/// other direction too.
fn classify_versions(local: Option<Hlc>, peer: Option<Hlc>, merging: bool) -> (bool, bool) {
    match (local, peer) {
        (Some(local), Some(peer)) if local != peer => {
            (local > peer || merging, local < peer || merging)
        }
        (Some(_), Some(_)) | (None, None) => (false, false),
        (Some(_), None) => (true, false),
        (None, Some(_)) => (false, true),
    }
}

/// Bound on mismatched buckets one [`classify_bucket_mismatches`] call
/// materializes listings for; a round can name every co-owned bucket at
/// once, which would otherwise be unbounded.
const CLASSIFY_BUCKET_CHUNK: usize = 64;

/// Splits `mismatches` into chunks of at most `chunk_size`. Pure so
/// [`classify_bucket_mismatches`]'s batching boundary gets a direct unit
/// test independent of any real [`ShardOps`].
fn chunk_mismatches(mismatches: Vec<AeMismatch>, chunk_size: usize) -> Vec<Vec<AeMismatch>> {
    let chunk_size = chunk_size.max(1);
    let mut remaining = mismatches;
    let mut chunks = Vec::new();
    while !remaining.is_empty() {
        let take = chunk_size.min(remaining.len());
        chunks.push(remaining.drain(..take).collect());
    }
    chunks
}

/// Classifies buckets answered with a listing or sketch
/// (`AeMismatch::Bucket`/`Sketch`): local shard passes in chunks of at
/// most [`CLASSIFY_BUCKET_CHUNK`] buckets; each mismatch is classified
/// into `plan` directly, or, for a sketch, via [`handle_sketch_mismatch`].
async fn classify_bucket_mismatches(
    shard: &Arc<dyn ShardOps>,
    cache: &SmolStr,
    mismatches: Vec<AeMismatch>,
    plan: &mut RepairPlan,
    merging: bool,
) {
    for chunk in chunk_mismatches(mismatches, CLASSIFY_BUCKET_CHUNK) {
        let local_entries = shard
            .entries_for_buckets(chunk.iter().map(AeMismatch::bucket).collect())
            .await;
        let local_by_bucket: HashMap<u16, Vec<(Bytes, Hlc)>> = local_entries
            .into_iter()
            .map(|(bucket, entries)| (bucket, key_versions_to_tuples(entries)))
            .collect();

        for mismatch in chunk {
            match mismatch {
                AeMismatch::Bucket(bucket, peer_entries) => {
                    diff_bucket(
                        local_by_bucket.get(&bucket).map_or(&[], Vec::as_slice),
                        &key_versions_to_tuples(peer_entries),
                        &mut plan.push_keys,
                        &mut plan.pull_keys,
                        merging,
                    );
                }
                AeMismatch::Sketch(bucket, cells) => {
                    let entries: &[(Bytes, Hlc)] =
                        local_by_bucket.get(&bucket).map_or(&[], Vec::as_slice);
                    handle_sketch_mismatch(cache, bucket, cells, entries, plan, merging);
                }
                AeMismatch::PartDigests(..) => {
                    unreachable!("invariant: run_round_against partitions this variant out")
                }
            }
        }
    }
}

/// [`crate::store::KeyVersion`]s back to this module's own `(key, version)`
/// tuple shape, at the boundary where a [`ShardOps`]/[`AeMismatch`] result
/// meets this file's tuple-based classification helpers.
fn key_versions_to_tuples(entries: Vec<crate::store::KeyVersion>) -> Vec<(Bytes, Hlc)> {
    entries.into_iter().map(|kv| (kv.key, kv.version)).collect()
}

/// Classifies buckets answered with part digests (`AeMismatch::PartDigests`):
/// local shard passes in chunks of at most [`CLASSIFY_BUCKET_CHUNK`]
/// buckets, the same bound [`classify_bucket_mismatches`] applies, since
/// each bucket carries up to `PART_COUNT` parts.
///
/// Stops at the first chunk whose [`Mesh::ae_parts`] call fails, so a
/// peer that stalls mid-round costs at most one `REQUEST_TIMEOUT`, not
/// one per remaining chunk.
///
/// [`Mesh::ae_parts`]: crate::net::Mesh::ae_parts
async fn classify_part_digest_mismatches(
    mesh: &crate::net::Mesh,
    shard: &Arc<dyn ShardOps>,
    cache: &SmolStr,
    peer: NodeId,
    mismatches: Vec<AeMismatch>,
    plan: &mut RepairPlan,
    merging: bool,
) {
    for chunk in chunk_mismatches(mismatches, CLASSIFY_BUCKET_CHUNK) {
        let ok =
            classify_part_digest_mismatch_chunk(mesh, shard, cache, peer, chunk, plan, merging)
                .await;
        if !ok {
            break;
        }
    }
}

/// One [`classify_part_digest_mismatches`] chunk: compares each bucket's
/// part digests against this node's own, then one [`Mesh::ae_parts`]
/// request for every differing part, classifying replies the same way
/// [`classify_bucket_mismatches`] does. Returns `false` when `ae_parts`
/// fails, so the caller stops issuing further chunk RPCs.
///
/// [`Mesh::ae_parts`]: crate::net::Mesh::ae_parts
async fn classify_part_digest_mismatch_chunk(
    mesh: &crate::net::Mesh,
    shard: &Arc<dyn ShardOps>,
    cache: &SmolStr,
    peer: NodeId,
    mismatches: Vec<AeMismatch>,
    plan: &mut RepairPlan,
    merging: bool,
) -> bool {
    let buckets: Vec<u16> = mismatches.iter().map(AeMismatch::bucket).collect();
    let local_part_digests = shard.part_digests(buckets).await;
    let local_digests_by_bucket: HashMap<u16, Vec<u64>> = local_part_digests
        .into_iter()
        .map(|bpd| (bpd.bucket, bpd.digests))
        .collect();

    let mut wanted_parts: Vec<BucketPart> = Vec::new();
    for mismatch in &mismatches {
        let AeMismatch::PartDigests(bucket, digests) = mismatch else {
            unreachable!("invariant: run_round_against partitions in only this variant")
        };
        let bucket = *bucket;
        let local_parts = local_digests_by_bucket
            .get(&bucket)
            .map_or(&[][..], Vec::as_slice);
        wanted_parts.extend(
            mismatched_parts(local_parts, digests)
                .into_iter()
                .map(|part| BucketPart { bucket, part }),
        );
    }
    if wanted_parts.is_empty() {
        return true;
    }

    let wanted_parts_wire: Vec<(u16, u8)> =
        wanted_parts.iter().map(|p| (p.bucket, p.part)).collect();
    match mesh.ae_parts(peer, cache.clone(), wanted_parts_wire).await {
        Ok(replies) => {
            let local_part_entries = shard.entries_for_parts(wanted_parts).await;
            let local_by_part: HashMap<(u16, u8), Vec<(Bytes, Hlc)>> = local_part_entries
                .into_iter()
                .map(|(bp, entries)| ((bp.bucket, bp.part), key_versions_to_tuples(entries)))
                .collect();
            let mut gathered: HashMap<u16, RepairPlan> = HashMap::new();
            for reply in replies {
                let slot = gathered.entry(reply.bucket()).or_default();
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
                            &key_versions_to_tuples(entries),
                            &mut slot.push_keys,
                            &mut slot.pull_keys,
                            merging,
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
                        handle_part_sketch_mismatch(cache, bucket, cells, entries, slot, merging);
                    }
                }
            }
            settle_bucket_parts(gathered, plan);
            true
        }
        Err(error) => {
            tracing::debug!(%error, "anti-entropy part exchange failed");
            false
        }
    }
}

/// This record's wire size framed as a [`crate::wire::Msg::Replicate`]
/// under a `cache_len`-byte cache name, so [`apply_repairs`] can total it
/// up per direction.
fn wire_record_len(cache_len: usize, record: &WireRecord) -> u64 {
    wire::replicate_frame_len(
        cache_len,
        record.key.len(),
        record.value.as_ref().map_or(0, Bytes::len),
    ) as u64
}

/// Applies a round's classified push/pull/hash-pull sets against `peer`, in
/// [`REPAIR_BATCH`] chunks, emits `sundog_ae_repaired_total{cache}`, and
/// returns the total wire bytes moved (pushed plus pulled).
async fn apply_repairs(
    mesh: &crate::net::Mesh,
    shard: &Arc<dyn ShardOps>,
    cache: &SmolStr,
    peer: NodeId,
    push_keys: Vec<Bytes>,
    pull_keys: Vec<Bytes>,
    pull_hashes: Vec<(u16, Vec<u64>)>,
) -> u64 {
    let mut repaired: u64 = 0;
    let mut bytes_moved: u64 = 0;
    // Batched so a large divergence makes durable incremental progress:
    // each landed batch shrinks the next round's diff, instead of one
    // all-or-nothing exchange racing a request timeout.
    for batch in push_keys.chunks(REPAIR_BATCH) {
        let records = shard.records_for(batch.to_vec()).await;
        repaired += records.len() as u64;
        bytes_moved += records
            .iter()
            .map(|rec| wire_record_len(cache.len(), rec))
            .sum::<u64>();
        // `net::batch_replicate` chunks this into a handful of full frames, not
        // one `Msg::Replicate` per record.
        let msgs = crate::net::batch_replicate(cache, records);
        mesh.send_many(peer, MsgClass::Replicate, msgs);
    }
    for batch in pull_keys.chunks(REPAIR_BATCH) {
        match mesh.ae_pull(peer, cache.clone(), batch.to_vec()).await {
            Ok(records) => {
                repaired += records.len() as u64;
                bytes_moved += records
                    .iter()
                    .map(|rec| wire_record_len(cache.len(), rec))
                    .sum::<u64>();
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
                    bytes_moved += records
                        .iter()
                        .map(|rec| wire_record_len(cache.len(), rec))
                        .sum::<u64>();
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
    tracing::debug!(repaired, bytes_moved, "anti-entropy round complete");
    bytes_moved
}

/// Classifies one `AeMismatch::Sketch(bucket, cells)` reply through
/// [`peel_sketch_into`] at bucket scope; on failure marks `bucket`
/// undecodable in `plan` for the `Msg::AeEntries` fallback.
fn handle_sketch_mismatch(
    cache: &SmolStr,
    bucket: u16,
    cells: Vec<Cell>,
    local_entries: &[(Bytes, Hlc)],
    plan: &mut RepairPlan,
    merging: bool,
) {
    if !peel_sketch_into(
        SketchScope::Bucket,
        cache,
        bucket,
        cells,
        local_entries,
        plan,
        merging,
    ) {
        plan.mark_undecodable(bucket);
    }
}

/// Which reply a sketch came in, deciding the metric it counts under.
#[derive(Debug, Clone, Copy)]
enum SketchScope {
    /// An `AeSketch` over a whole bucket: `sundog_ae_sketch_total`.
    Bucket,
    /// An `AePartSketch` over one part: `sundog_ae_parts_total`.
    Part,
}

impl SketchScope {
    const fn metric(self) -> &'static str {
        match self {
            Self::Bucket => "sundog_ae_sketch_total",
            Self::Part => "sundog_ae_parts_total",
        }
    }

    const fn decoded_outcome(self) -> &'static str {
        match self {
            Self::Bucket => "decoded",
            Self::Part => "sketch",
        }
    }
}

/// The one peel path for bucket and part sketches: builds the local
/// comparison sketch over `local_entries`, subtracts the received `cells`,
/// and peels. On success [`diff_decoded`] classifies the result into
/// `plan`, scoped to `bucket`, and this returns `true`; on failure it
/// returns `false` for the caller to mark undecodable. Emits `scope`'s
/// metric either way.
fn peel_sketch_into(
    scope: SketchScope,
    cache: &SmolStr,
    bucket: u16,
    cells: Vec<Cell>,
    local_entries: &[(Bytes, Hlc)],
    plan: &mut RepairPlan,
    merging: bool,
) -> bool {
    // Sized from the received sketch's cell count, not this node's own
    // config, so the two sketches stay shape-compatible if configs drift. A
    // cell count `Iblt::new` cannot reproduce fails `subtract`, and falls
    // back like any other undecodable sketch.
    let mut local_sketch = Iblt::new(cells.len());
    for (key, ver) in local_entries {
        local_sketch.insert(xxh3_64(key), *ver);
    }
    let remote_sketch = Iblt::from_cells(cells);
    let decoded = local_sketch.subtract(&remote_sketch).and_then(Iblt::peel);
    let outcome = if decoded.is_ok() {
        scope.decoded_outcome()
    } else {
        "fallback"
    };
    metrics::counter!(scope.metric(), "cache" => cache.to_string(), "outcome" => outcome)
        .increment(1);
    tracing::debug!(outcome, bucket, ?scope, "anti-entropy sketch peeled");
    let Ok(decoded) = decoded else {
        return false;
    };
    let mut hashes = Vec::new();
    diff_decoded(
        local_entries,
        &decoded,
        &mut plan.push_keys,
        &mut hashes,
        merging,
    );
    plan.pull_by_hash(bucket, hashes);
    true
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

/// Classifies one part's `AePartReply::Sketch` reply through
/// [`peel_sketch_into`] at part scope. On failure, marks `bucket`
/// undecodable in `plan` (a per-bucket-part [`RepairPlan`] here, not the
/// round's own): a part sketch never gets its own part-scoped fallback.
fn handle_part_sketch_mismatch(
    cache: &SmolStr,
    bucket: u16,
    cells: Vec<Cell>,
    local_entries: &[(Bytes, Hlc)],
    plan: &mut RepairPlan,
    merging: bool,
) {
    if !peel_sketch_into(
        SketchScope::Part,
        cache,
        bucket,
        cells,
        local_entries,
        plan,
        merging,
    ) {
        plan.mark_undecodable(bucket);
    }
}

/// Folds every bucket's gathered part classification (each a [`RepairPlan`]
/// at bucket-part scope) into the round's own `plan`: a bucket with any
/// undecodable part marks `plan` undecodable for it once and contributes
/// nothing else; every other bucket's keys and hashes are kept.
fn settle_bucket_parts(gathered: HashMap<u16, RepairPlan>, plan: &mut RepairPlan) {
    let mut buckets: Vec<(u16, RepairPlan)> = gathered.into_iter().collect();
    buckets.sort_unstable_by_key(|(bucket, _)| *bucket);
    for (bucket, parts) in buckets {
        if !parts.undecodable_buckets.is_empty() {
            plan.mark_undecodable(bucket);
            continue;
        }
        plan.push_keys.extend(parts.push_keys);
        plan.pull_keys.extend(parts.pull_keys);
        plan.pull_hashes.extend(parts.pull_hashes);
    }
}

/// Classifies one bucket's local listing against `peer_entries` via
/// [`classify_versions`], bytes-keyed: each local key looks up its peer
/// entry by the key itself, and a key only the peer holds pulls. See
/// [`diff_decoded`] for the hash-keyed sketch analogue of this same rule.
fn diff_bucket(
    local_entries: &[(Bytes, Hlc)],
    peer_entries: &[(Bytes, Hlc)],
    push_keys: &mut Vec<Bytes>,
    pull_keys: &mut Vec<Bytes>,
    merging: bool,
) {
    let peer_by_key: HashMap<&Bytes, Hlc> = peer_entries.iter().map(|(k, v)| (k, *v)).collect();
    let mut local_keys = HashSet::with_capacity(local_entries.len());

    for (key, local_ver) in local_entries {
        local_keys.insert(key);
        let (push, pull) =
            classify_versions(Some(*local_ver), peer_by_key.get(key).copied(), merging);
        if push {
            push_keys.push(key.clone());
        }
        if pull {
            pull_keys.push(key.clone());
        }
    }
    for (key, _) in peer_entries {
        if !local_keys.contains(key) {
            pull_keys.push(key.clone());
        }
    }
}

/// The initiator's push/pull classification once an `AeSketch` reply
/// decodes: the same version-mismatch rule as `diff_bucket`, over
/// [`Iblt::peel`]'s peeled element lists, hash-keyed instead of
/// `diff_bucket`'s bytes-keyed lookup. A sketch carries no key bytes, only a
/// `key_hash`, so every pull queues the hash into `pull_hashes`; only a push
/// resolves its hash back to `local_entries` first, to queue the actual key
/// bytes.
///
/// Reachable outside `cluster::anti_entropy` only because `tests/sim.rs`
/// re-exports it as `crate::diff_decoded` under `feature = "sim"`, to
/// reconcile a peeled sketch the same way `handle_sketch_mismatch` does
/// here, without duplicating this classification.
pub fn diff_decoded(
    local_entries: &[(Bytes, Hlc)],
    decoded: &Decoded,
    push_keys: &mut Vec<Bytes>,
    pull_hashes: &mut Vec<u64>,
    merging: bool,
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

    for elem in &decoded.only_left {
        let (push, pull) = classify_versions(
            Some(elem.ver),
            remote_only.get(&elem.key_hash).copied(),
            merging,
        );
        if push && let Some(&key) = local_by_hash.get(&elem.key_hash) {
            push_keys.push(key.clone());
        }
        if pull {
            pull_hashes.push(elem.key_hash);
        }
    }
    for elem in &decoded.only_right {
        if !local_only.contains_key(&elem.key_hash) {
            pull_hashes.push(elem.key_hash);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::super::sketch::Elem;
    use super::*;

    /// A postcard-encoded `(key, value)` pair and the `WireRecord`
    /// `apply_remote` takes. Real-transport only.
    #[cfg(not(feature = "sim"))]
    fn encode_test_record(key: u32, value: &str, node: NodeId) -> (Bytes, WireRecord) {
        let key_bytes = Bytes::from(postcard::to_stdvec(&key).expect("test key encodes"));
        let value_bytes = Bytes::from(postcard::to_stdvec(value).expect("test value encodes"));
        let rec = WireRecord {
            key: key_bytes.clone(),
            value: Some(value_bytes),
            ver: Hlc {
                wall_ms: 1,
                logical: 0,
                node,
            },
            expires_at_ms: None,
        };
        (key_bytes, rec)
    }

    /// Pins that `apply_repairs`'s pushed/pulled byte count matches
    /// `wire_record_len` computed independently for one pushed and one
    /// pulled record between two real nodes.
    #[tokio::test]
    #[cfg(not(feature = "sim"))]
    async fn apply_repairs_reports_the_bytes_it_moved() {
        use super::super::test_support::{loopback_config, registered_shard, wait_for_peer_count};
        use crate::store::Mode;

        let config = loopback_config();
        let cluster_a = Cluster::builder("cluster-it-apply-repairs-bytes")
            .seeds(std::iter::empty())
            .config(config.clone())
            .build()
            .await
            .expect("node a builds");
        let node_a = cluster_a.node_id();
        cluster_a
            .cache::<u32, String>("users")
            .mode(Mode::Replicated)
            .open()
            .await
            .expect("a opens");

        let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;
        let cluster_b = Cluster::builder("cluster-it-apply-repairs-bytes")
            .seeds([gossip_a])
            .config(config)
            .build()
            .await
            .expect("node b builds");
        wait_for_peer_count(&cluster_b, 1).await;
        let cache_b = tokio::time::timeout(
            Duration::from_secs(20),
            cluster_b
                .cache::<u32, String>("users")
                .mode(Mode::Replicated)
                .open(),
        )
        .await
        .expect("open completes within the state-transfer budget")
        .expect("b opens");

        let name = SmolStr::new("users");
        let shard_a = registered_shard(&cluster_a, &name);
        let shard_b = registered_shard(&cluster_b, &name);

        let (pull_key_bytes, pull_rec) = encode_test_record(1, "a-value", node_a);
        shard_a.apply_remote(pull_rec.clone()).await;
        let (push_key_bytes, push_rec) = encode_test_record(2, "b-value", cluster_b.node_id());
        shard_b.apply_remote(push_rec.clone()).await;

        let expected =
            wire_record_len(name.len(), &push_rec) + wire_record_len(name.len(), &pull_rec);

        let bytes_moved = apply_repairs(
            cluster_b.mesh(),
            &shard_b,
            &name,
            node_a,
            vec![push_key_bytes],
            vec![pull_key_bytes],
            Vec::new(),
        )
        .await;

        assert_eq!(
            bytes_moved, expected,
            "the byte count covers exactly the pushed and pulled record, no more and no less"
        );
        assert_eq!(
            cache_b.get(&1u32).await,
            Some("a-value".to_string()),
            "the pull side of apply_repairs actually landed the record it counted"
        );

        cluster_a.shutdown().await;
        cluster_b.shutdown().await;
    }

    /// `count` distinct buckets among `0..n` u32 keys with more than
    /// `min_count` keys each, enough to span more than one
    /// [`CLASSIFY_BUCKET_CHUNK`]. Real-transport only.
    #[cfg(not(feature = "sim"))]
    fn dense_buckets(n: u32, min_count: usize, count: usize) -> Vec<Vec<u32>> {
        let mut by_bucket: HashMap<u16, Vec<u32>> = HashMap::new();
        for key in 0..n {
            let bytes = crate::store::encode_key(&key).expect("u32 key encodes");
            by_bucket.entry(bucket_of(&bytes)).or_default().push(key);
        }
        let dense: Vec<Vec<u32>> = by_bucket
            .into_values()
            .filter(|keys| keys.len() > min_count)
            .take(count)
            .collect();
        assert_eq!(
            dense.len(),
            count,
            "at least `count` buckets exceed min_count among this many keys"
        );
        dense
    }

    /// Pins that a mismatched bucket past the first
    /// [`CLASSIFY_BUCKET_CHUNK`] chunk is still classified and repaired,
    /// not dropped by an off-by-one. One key per dense bucket is dropped
    /// on `b`, across more buckets than one chunk holds.
    #[tokio::test]
    #[cfg(not(feature = "sim"))]
    async fn classify_part_digest_mismatches_repairs_every_bucket_across_more_than_one_chunk() {
        use super::super::test_support::{loopback_config, wait_for_peer_count, wait_until};
        use crate::config::ClusterConfig;
        use crate::store::Mode;

        const N: u32 = 60_000;
        let bucket_count = CLASSIFY_BUCKET_CHUNK + 5;

        let config = ClusterConfig {
            ae_part_min_bucket: 8,
            // Keeps every mismatched part on the listing path, not sketch.
            ae_sketch_min_bucket: 1_000_000,
            ..loopback_config()
        };

        let cluster_a = Cluster::builder("cluster-it-ae-part-digest-chunked")
            .seeds(std::iter::empty())
            .config(config.clone())
            .build()
            .await
            .expect("node a builds");
        let cache_a = cluster_a
            .cache::<u32, String>("users")
            .mode(Mode::Replicated)
            .open()
            .await
            .expect("a opens");

        cache_a
            .insert_many((0..N).map(|k| (k, k.to_string())))
            .await
            .expect("a inserts before b ever joins");

        let dense = dense_buckets(N, config.ae_part_min_bucket + 1, bucket_count);
        let target_keys: Vec<u32> = dense.iter().map(|keys| keys[0]).collect();

        let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;
        let cluster_b = Cluster::builder("cluster-it-ae-part-digest-chunked")
            .seeds([gossip_a])
            .config(config)
            .build()
            .await
            .expect("node b builds");
        wait_for_peer_count(&cluster_b, 1).await;

        let cache_b = tokio::time::timeout(
            Duration::from_secs(30),
            cluster_b
                .cache::<u32, String>("users")
                .mode(Mode::Replicated)
                .open(),
        )
        .await
        .expect("open completes within the state-transfer budget")
        .expect("b opens");
        assert_eq!(cache_b.entry_count().await, u64::from(N));

        let node_a = cluster_a.node_id();
        let node_b = cluster_b.node_id();
        tokio::time::timeout(Duration::from_secs(30), async {
            while cluster_a.peer_is_streaming(node_b) || cluster_b.peer_is_streaming(node_a) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("replicate traffic settles within the bound");

        for &key in &target_keys {
            cache_b.invalidate_local(&key).await;
        }
        for &key in &target_keys {
            assert_eq!(
                cache_b.get(&key).await,
                None,
                "dropped before the round runs"
            );
        }

        wait_until(
            Duration::from_secs(25),
            "anti-entropy repairs every dropped key even though their buckets span more than \
             one classify_part_digest_mismatches chunk",
            async || {
                let mut all_back = true;
                for &key in &target_keys {
                    if cache_b.get(&key).await.is_none() {
                        all_back = false;
                    }
                }
                all_back
            },
        )
        .await;

        cluster_a.shutdown().await;
        cluster_b.shutdown().await;
    }

    /// Wraps a real [`ShardOps`], counting `part_digests` calls on a
    /// shared counter, to observe how many chunks a loop visited when a
    /// failed `ae_parts` call leaves no other trace.
    #[cfg(not(feature = "sim"))]
    struct CountingPartDigestsShard {
        inner: Arc<dyn ShardOps>,
        part_digests_calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[cfg(not(feature = "sim"))]
    impl ShardOps for CountingPartDigestsShard {
        fn apply_remote(&self, rec: WireRecord) -> futures::future::BoxFuture<'_, ()> {
            self.inner.apply_remote(rec)
        }
        fn apply_remote_batch(&self, recs: Vec<WireRecord>) -> futures::future::BoxFuture<'_, ()> {
            self.inner.apply_remote_batch(recs)
        }
        fn invalidate(&self, key: Bytes, ver: Hlc) -> futures::future::BoxFuture<'_, ()> {
            self.inner.invalidate(key, ver)
        }
        fn digests(&self) -> futures::future::BoxFuture<'_, Vec<crate::store::BucketDigest>> {
            self.inner.digests()
        }
        fn bucket_entries(
            &self,
            bucket: u16,
        ) -> futures::future::BoxFuture<'_, Vec<crate::store::KeyVersion>> {
            self.inner.bucket_entries(bucket)
        }
        fn entries_for_buckets(
            &self,
            buckets: Vec<u16>,
        ) -> futures::future::BoxFuture<'_, crate::store::BucketEntries> {
            self.inner.entries_for_buckets(buckets)
        }
        fn bucket_lens(
            &self,
            buckets: Vec<u16>,
        ) -> futures::future::BoxFuture<'_, Vec<crate::store::BucketLen>> {
            self.inner.bucket_lens(buckets)
        }
        fn part_digests(
            &self,
            buckets: Vec<u16>,
        ) -> futures::future::BoxFuture<'_, Vec<crate::store::BucketPartDigests>> {
            self.part_digests_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.part_digests(buckets)
        }
        fn entries_for_parts(
            &self,
            parts: Vec<BucketPart>,
        ) -> futures::future::BoxFuture<'_, crate::store::PartEntries> {
            self.inner.entries_for_parts(parts)
        }
        fn records_for(&self, keys: Vec<Bytes>) -> futures::future::BoxFuture<'_, Vec<WireRecord>> {
            self.inner.records_for(keys)
        }
        fn snapshot_chunks(&self) -> futures::stream::BoxStream<'static, Vec<WireRecord>> {
            self.inner.snapshot_chunks()
        }
        fn gc_tombstones(&self, any_member_absent: bool) -> futures::future::BoxFuture<'_, ()> {
            self.inner.gc_tombstones(any_member_absent)
        }
        fn run_pending_tasks(&self) -> futures::future::BoxFuture<'_, ()> {
            self.inner.run_pending_tasks()
        }
    }

    /// Pins that a failed `ae_parts` call on the first of two chunks
    /// stops the loop: the second chunk's `part_digests` lookup, a
    /// faithful proxy for "visited", must never happen.
    #[tokio::test]
    #[cfg(not(feature = "sim"))]
    async fn classify_part_digest_mismatches_stops_issuing_ae_parts_after_the_first_chunk_fails() {
        use super::super::test_support::{loopback_config, registered_shard};
        use crate::store::Mode;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let name = SmolStr::new("part-digest-chunk-break");
        let cluster = Cluster::builder("cluster-it-part-digest-chunk-break")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("cluster builds alone");
        cluster
            .cache::<u32, String>(name.clone())
            .mode(Mode::distributed())
            .open()
            .await
            .expect("opens alone, owning every bucket");

        let part_digests_calls = Arc::new(AtomicUsize::new(0));
        let shard: Arc<dyn ShardOps> = Arc::new(CountingPartDigestsShard {
            inner: registered_shard(&cluster, &name),
            part_digests_calls: Arc::clone(&part_digests_calls),
        });

        // Empty local part digests make every remote one mismatched, so
        // every chunk calls Mesh::ae_parts.
        let bucket_count = CLASSIFY_BUCKET_CHUNK + 5;
        let mismatches: Vec<AeMismatch> = (0..u16::try_from(bucket_count).expect("fits u16"))
            .map(|bucket| AeMismatch::PartDigests(bucket, vec![1u64]))
            .collect();

        let unreachable_peer = NodeId::from(u64::MAX);
        let mut plan = RepairPlan::default();
        classify_part_digest_mismatches(
            cluster.mesh(),
            &shard,
            &name,
            unreachable_peer,
            mismatches,
            &mut plan,
            false,
        )
        .await;

        assert_eq!(
            part_digests_calls.load(Ordering::SeqCst),
            1,
            "the loop must stop after the first chunk's ae_parts call fails, never reaching \
             the second chunk's local part_digests lookup"
        );
        assert!(
            plan.push_keys.is_empty() && plan.pull_keys.is_empty(),
            "an unreachable peer repairs nothing"
        );

        cluster.shutdown().await;
    }

    /// `n` distinct u32 keys, each landing in a different bucket.
    /// Real-transport only.
    #[cfg(not(feature = "sim"))]
    fn keys_in_distinct_buckets(n: usize) -> Vec<(u32, u16)> {
        let mut found = Vec::new();
        let mut seen = HashSet::new();
        for key in 0u32.. {
            let bytes = crate::store::encode_key(&key).expect("u32 key encodes");
            let bucket = bucket_of(&bytes);
            if seen.insert(bucket) {
                found.push((key, bucket));
                if found.len() == n {
                    break;
                }
            }
        }
        found
    }

    /// Pins the listing/sketch chunking analogue of the part-digest test
    /// above: one key per bucket, under `ae_sketch_min_bucket` so every
    /// mismatch answers `AeMismatch::Bucket`. Every dropped key, across
    /// more buckets than one chunk holds, must come back.
    #[tokio::test]
    #[cfg(not(feature = "sim"))]
    async fn classify_bucket_mismatches_repairs_every_bucket_across_more_than_one_chunk() {
        use super::super::test_support::{loopback_config, wait_for_peer_count, wait_until};
        use crate::store::Mode;

        let bucket_count = CLASSIFY_BUCKET_CHUNK + 5;
        let targets = keys_in_distinct_buckets(bucket_count);
        let target_keys: Vec<u32> = targets.iter().map(|(key, _)| *key).collect();

        let config = loopback_config();
        let cluster_a = Cluster::builder("cluster-it-ae-bucket-listing-chunked")
            .seeds(std::iter::empty())
            .config(config.clone())
            .build()
            .await
            .expect("node a builds");
        let cache_a = cluster_a
            .cache::<u32, String>("users")
            .mode(Mode::Replicated)
            .open()
            .await
            .expect("a opens");

        cache_a
            .insert_many(target_keys.iter().map(|&k| (k, k.to_string())))
            .await
            .expect("a inserts before b ever joins");

        let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;
        let cluster_b = Cluster::builder("cluster-it-ae-bucket-listing-chunked")
            .seeds([gossip_a])
            .config(config)
            .build()
            .await
            .expect("node b builds");
        wait_for_peer_count(&cluster_b, 1).await;

        let cache_b = tokio::time::timeout(
            Duration::from_secs(30),
            cluster_b
                .cache::<u32, String>("users")
                .mode(Mode::Replicated)
                .open(),
        )
        .await
        .expect("open completes within the state-transfer budget")
        .expect("b opens");
        assert_eq!(cache_b.entry_count().await, target_keys.len() as u64);

        let node_a = cluster_a.node_id();
        let node_b = cluster_b.node_id();
        tokio::time::timeout(Duration::from_secs(30), async {
            while cluster_a.peer_is_streaming(node_b) || cluster_b.peer_is_streaming(node_a) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("replicate traffic settles within the bound");

        for &key in &target_keys {
            cache_b.invalidate_local(&key).await;
        }
        for &key in &target_keys {
            assert_eq!(
                cache_b.get(&key).await,
                None,
                "dropped before the round runs"
            );
        }

        wait_until(
            Duration::from_secs(25),
            "anti-entropy repairs every dropped key even though their buckets span more than \
             one classify_bucket_mismatches chunk",
            async || {
                let mut all_back = true;
                for &key in &target_keys {
                    if cache_b.get(&key).await.is_none() {
                        all_back = false;
                    }
                }
                all_back
            },
        )
        .await;

        cluster_a.shutdown().await;
        cluster_b.shutdown().await;
    }

    /// Two real, joined `Mode::Distributed` nodes co-owning every bucket,
    /// for `run_round_for_buckets`'s real-transport tests. Real-transport only.
    #[cfg(not(feature = "sim"))]
    async fn two_distributed_co_owners(
        test_id: &str,
        name: &SmolStr,
    ) -> (
        Cluster,
        Cluster,
        NodeId,
        Arc<dyn ShardOps>,
        Arc<dyn ShardOps>,
    ) {
        use super::super::test_support::{loopback_config, registered_shard, wait_for_peer_count};
        use crate::store::Mode;

        let b = Cluster::builder(format!("cluster-it-{test_id}"))
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("b builds");
        let node_b = b.node_id();
        b.cache::<u32, String>(name.clone())
            .mode(Mode::distributed())
            .open()
            .await
            .expect("b opens alone, owning every bucket");

        let c = Cluster::builder(format!("cluster-it-{test_id}"))
            .seeds([b.local_gossip_addr()])
            .config(loopback_config())
            .build()
            .await
            .expect("c builds");
        wait_for_peer_count(&b, 1).await;
        wait_for_peer_count(&c, 1).await;
        c.cache::<u32, String>(name.clone())
            .mode(Mode::distributed())
            .open()
            .await
            .expect("c opens, co-owning every bucket alongside b");

        let shard_b = registered_shard(&b, name);
        let shard_c = registered_shard(&c, name);
        (b, c, node_b, shard_b, shard_c)
    }

    /// Pins that `still_diverged` names only the requested bucket even
    /// when a second, un-requested bucket also diverges, proving the
    /// request is scoped to `buckets`.
    #[tokio::test]
    #[cfg(not(feature = "sim"))]
    async fn run_round_for_buckets_reports_only_the_requested_buckets_still_diverged() {
        use super::super::test_support::wait_until;

        let name = SmolStr::new("scoped-round-diverged");
        let (b, c, node_b, shard_b, shard_c) =
            two_distributed_co_owners("scoped-diverged", &name).await;

        let keys = keys_in_distinct_buckets(2);
        let (requested_key, requested_bucket) = keys[0];
        let (extra_key, extra_bucket) = keys[1];

        let (_, requested_rec) = encode_test_record(requested_key, "requested", node_b);
        shard_b.apply_remote(requested_rec).await;
        let (_, extra_rec) = encode_test_record(extra_key, "extra", node_b);
        shard_b.apply_remote(extra_rec).await;

        let mut outcome = BucketRoundOutcome::default();
        wait_until(
            Duration::from_secs(10),
            "b's view catches up to c joining, so the round runs instead of reporting stale",
            async || {
                outcome =
                    run_round_for_buckets(c.mesh(), &shard_c, &name, node_b, &[requested_bucket])
                        .await;
                !outcome.failed
            },
        )
        .await;

        assert_eq!(
            outcome.still_diverged,
            HashSet::from([requested_bucket]),
            "the requested bucket's real divergence is reported"
        );
        assert!(
            outcome.matched.is_empty(),
            "the only requested bucket diverged, so nothing is reported matched"
        );
        assert!(
            !outcome.still_diverged.contains(&extra_bucket)
                && !outcome.matched.contains(&extra_bucket),
            "the un-requested bucket's real divergence never surfaces in this scoped round's outcome"
        );

        b.shutdown().await;
        c.shutdown().await;
    }

    /// Pins the view-disagreement case: `c` requests one bucket its
    /// fresh view says `b` does not co-own. That bucket is never sent
    /// to `b`, so it must land in `still_diverged`, never `matched`.
    #[tokio::test]
    #[cfg(not(feature = "sim"))]
    #[expect(
        clippy::too_many_lines,
        reason = "one end-to-end three-node scenario (settle b/c/d, find a bucket dropped from \
                  the request, run and assert the round): splitting it would only scatter state \
                  (b, c, d, node_b, shard_c, the split buckets) across helper signatures"
    )]
    async fn run_round_for_buckets_never_reports_matched_for_a_bucket_the_peer_does_not_co_own() {
        use super::super::test_support::{
            loopback_config, registered_shard, wait_for_peer_count, wait_until,
        };
        use crate::store::Mode;

        let name = SmolStr::new("scoped-round-dropped-co-owner");
        let b = Cluster::builder("cluster-it-scoped-dropped")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("b builds");
        let node_b = b.node_id();
        b.cache::<u32, String>(name.clone())
            .mode(Mode::distributed())
            .open()
            .await
            .expect("b opens alone, owning every bucket");

        let seed = b.local_gossip_addr();
        let c = Cluster::builder("cluster-it-scoped-dropped")
            .seeds([seed])
            .config(loopback_config())
            .build()
            .await
            .expect("c builds");
        let d = Cluster::builder("cluster-it-scoped-dropped")
            .seeds([seed])
            .config(loopback_config())
            .build()
            .await
            .expect("d builds");
        wait_for_peer_count(&b, 2).await;
        wait_for_peer_count(&c, 2).await;
        wait_for_peer_count(&d, 2).await;
        let (cache_c, cache_d) = tokio::join!(
            c.cache::<u32, String>(name.clone())
                .mode(Mode::distributed())
                .open(),
            d.cache::<u32, String>(name.clone())
                .mode(Mode::distributed())
                .open(),
        );
        cache_c.expect("c opens");
        cache_d.expect("d opens");

        let shard_c = registered_shard(&c, &name);

        // c's ownership view catches up to d's cache mode on its own
        // cadence, so this polls rather than assuming the first view is settled.
        let mut split: Option<(u16, u16)> = None;
        wait_until(
            Duration::from_secs(10),
            "c's own view catches up to d joining, splitting c's owned buckets across both b \
             and d instead of naming b the only co-owner",
            async || {
                let Some(view) = shard_c.ownership_view() else {
                    return false;
                };
                let mut shared_with_b = None;
                let mut dropped_by_b = None;
                for bucket in
                    0..u16::try_from(crate::store::BUCKET_COUNT).expect("BUCKET_COUNT fits u16")
                {
                    if !view.owns(bucket) {
                        continue;
                    }
                    if view.owners_of(bucket).contains(&node_b) {
                        shared_with_b.get_or_insert(bucket);
                    } else {
                        dropped_by_b.get_or_insert(bucket);
                    }
                    if shared_with_b.is_some() && dropped_by_b.is_some() {
                        break;
                    }
                }
                match (shared_with_b, dropped_by_b) {
                    (Some(shared), Some(dropped)) => {
                        split = Some((shared, dropped));
                        true
                    }
                    _ => false,
                }
            },
        )
        .await;
        let (shared_with_b, dropped_by_b) =
            split.expect("wait_until only returns once the closure itself reported ready");

        // b's own OwnershipTracker catches up to c/d joining on its own
        // cadence, so this polls rather than assuming a shared view hash.
        let mut outcome = BucketRoundOutcome::default();
        wait_until(
            Duration::from_secs(10),
            "b's view catches up to c and d joining, so the round against it runs instead of \
             reporting stale",
            async || {
                outcome = run_round_for_buckets(
                    c.mesh(),
                    &shard_c,
                    &name,
                    node_b,
                    &[shared_with_b, dropped_by_b],
                )
                .await;
                !outcome.failed
            },
        )
        .await;
        assert!(
            outcome.still_diverged.contains(&dropped_by_b),
            "a bucket c's own fresh view says b does not co-own was never sent to b this \
             round, so its digest was never compared -- it must count as still diverged"
        );
        assert!(
            !outcome.matched.contains(&dropped_by_b),
            "a bucket dropped from the outbound request must never be reported matched"
        );
        assert!(
            outcome.matched.contains(&shared_with_b),
            "the bucket genuinely co-owned with b and untouched by either side still matches \
             normally"
        );

        b.shutdown().await;
        c.shutdown().await;
        d.shutdown().await;
    }

    /// Pins that an untouched bucket matches on the first round: no
    /// divergence, no bytes moved.
    #[tokio::test]
    #[cfg(not(feature = "sim"))]
    async fn run_round_for_buckets_marks_a_bucket_matched_once_its_digest_exchange_reports_no_mismatch()
     {
        use super::super::test_support::wait_until;

        let name = SmolStr::new("scoped-round-matched");
        let (b, c, node_b, _shard_b, shard_c) =
            two_distributed_co_owners("scoped-matched", &name).await;

        let (_, matched_bucket) = keys_in_distinct_buckets(1)[0];

        let mut outcome = BucketRoundOutcome::default();
        wait_until(
            Duration::from_secs(10),
            "b's view catches up to c joining, so the round runs instead of reporting stale",
            async || {
                outcome =
                    run_round_for_buckets(c.mesh(), &shard_c, &name, node_b, &[matched_bucket])
                        .await;
                !outcome.failed
            },
        )
        .await;

        assert_eq!(
            outcome.matched,
            HashSet::from([matched_bucket]),
            "an untouched bucket's digests already agree, so it matches at once"
        );
        assert!(outcome.still_diverged.is_empty());
        assert_eq!(
            outcome.bytes_moved, 0,
            "nothing needed repair, so nothing moved"
        );

        b.shutdown().await;
        c.shutdown().await;
    }

    /// Pins that an unknown peer fails the round: everything requested
    /// lands in `still_diverged`.
    #[tokio::test]
    #[cfg(not(feature = "sim"))]
    async fn run_round_for_buckets_treats_every_requested_bucket_as_still_diverged_when_the_peer_is_unreachable()
     {
        use super::super::test_support::{loopback_config, registered_shard};
        use crate::store::Mode;

        let name = SmolStr::new("scoped-round-unreachable");
        let cluster = Cluster::builder("cluster-it-scoped-unreachable")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("cluster builds alone");
        cluster
            .cache::<u32, String>(name.clone())
            .mode(Mode::distributed())
            .open()
            .await
            .expect("opens alone, owning every bucket");

        let shard = registered_shard(&cluster, &name);
        let unknown_peer = NodeId::from(u64::MAX);
        let requested = [1u16, 2, 3];

        let outcome =
            run_round_for_buckets(cluster.mesh(), &shard, &name, unknown_peer, &requested).await;

        assert!(
            outcome.failed,
            "an unknown peer's digest exchange cannot succeed"
        );
        assert_eq!(
            outcome.still_diverged,
            requested.iter().copied().collect::<HashSet<u16>>(),
            "every requested bucket is treated as still diverged when the round cannot run"
        );
        assert!(outcome.matched.is_empty());
        assert_eq!(outcome.bytes_moved, 0);

        cluster.shutdown().await;
    }

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

    fn mismatch_bucket(bucket: u16) -> AeMismatch {
        AeMismatch::Bucket(bucket, Vec::new())
    }

    /// Pins `chunk_mismatches`'s chunk sizes and order, and that `0`
    /// floors to `1` rather than looping forever.
    #[test]
    fn chunk_mismatches_splits_into_bounded_chunks_preserving_order() {
        struct Case {
            name: &'static str,
            count: u16,
            chunk_size: usize,
            expected_chunk_lens: &'static [usize],
        }
        let cases = [
            Case {
                name: "fewer mismatches than one chunk: a single chunk",
                count: 3,
                chunk_size: 64,
                expected_chunk_lens: &[3],
            },
            Case {
                name: "exactly one chunk's worth: a single chunk",
                count: 4,
                chunk_size: 4,
                expected_chunk_lens: &[4],
            },
            Case {
                name: "more than one chunk: full chunks then a remainder",
                count: 10,
                chunk_size: 4,
                expected_chunk_lens: &[4, 4, 2],
            },
            Case {
                name: "no mismatches: no chunks at all",
                count: 0,
                chunk_size: 4,
                expected_chunk_lens: &[],
            },
            Case {
                name: "a zero chunk size is floored to one, not an infinite loop",
                count: 3,
                chunk_size: 0,
                expected_chunk_lens: &[1, 1, 1],
            },
        ];
        for case in cases {
            let mismatches: Vec<AeMismatch> = (0..case.count).map(mismatch_bucket).collect();
            let chunks = chunk_mismatches(mismatches, case.chunk_size);
            let lens: Vec<usize> = chunks.iter().map(Vec::len).collect();
            assert_eq!(lens, case.expected_chunk_lens, "{}", case.name);
            let flattened: Vec<u16> = chunks
                .into_iter()
                .flatten()
                .map(|m| AeMismatch::bucket(&m))
                .collect();
            let expected: Vec<u16> = (0..case.count).collect();
            assert_eq!(
                flattened, expected,
                "{}: every mismatch survives, in order",
                case.name
            );
        }
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
        diff_decoded(&local_entries, &decoded, &mut push, &mut pull, false);
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
        diff_decoded(&local_entries, &decoded, &mut push, &mut pull, false);
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
        diff_decoded(&local_entries, &decoded, &mut push, &mut pull, false);
        assert_eq!(push, vec![key]);
        assert!(pull.is_empty());
    }

    #[test]
    fn diff_decoded_queues_both_directions_for_a_both_sides_mismatch_when_merging() {
        let (key, hash) = entry(b"k1");
        let local_entries = vec![(key.clone(), hlc(10))];
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
        diff_decoded(&local_entries, &decoded, &mut push, &mut pull, true);
        assert_eq!(
            push,
            vec![key],
            "merging queues the push even though the peer's version is greater"
        );
        assert_eq!(
            pull,
            vec![hash],
            "merging keeps the pull the non-merging rule already queues"
        );
    }

    #[test]
    fn diff_decoded_leaves_a_one_sided_key_alone_regardless_of_merging() {
        // Present only on this node's side of the peel: not a differing
        // version present on both sides, so `merging` changes nothing.
        let (key, hash) = entry(b"k2");
        let local_entries = vec![(key.clone(), hlc(5))];
        let decoded = Decoded {
            only_left: vec![Elem {
                key_hash: hash,
                ver: hlc(5),
            }],
            only_right: Vec::new(),
        };
        for merging in [false, true] {
            let (mut push, mut pull) = (Vec::new(), Vec::new());
            diff_decoded(&local_entries, &decoded, &mut push, &mut pull, merging);
            assert_eq!(push, vec![key.clone()]);
            assert!(pull.is_empty());
        }
    }

    fn mismatch_of(cells: Vec<Cell>, local_entries: &[(Bytes, Hlc)]) -> RepairPlan {
        let mut plan = RepairPlan::default();
        handle_sketch_mismatch(
            &SmolStr::new("users"),
            7,
            cells,
            local_entries,
            &mut plan,
            false,
        );
        plan
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

    /// A merging bucket above the threshold that routes it to the sketch
    /// path (`ClusterConfig::ae_sketch_min_bucket`) converges in this one
    /// round: every one of its mismatched keys is a two-sided version
    /// mismatch, the shape `sketch.rs`'s own
    /// `two_sided_version_mismatches_decode_at_the_default_shape` pins as
    /// decodable at this element count, so the peel here succeeds with no
    /// fallback, and `merging` queues both directions for every key.
    #[test]
    fn a_merging_bucket_above_the_threshold_converges_through_the_sketch_path() {
        const KEYS: u64 = 40;
        let local_entries: Vec<(Bytes, Hlc)> = (0..KEYS)
            .map(|i| bucket_entry(format!("k{i}").as_bytes(), 2 * i + 1))
            .collect();
        let mut remote = Iblt::new(240);
        for i in 0..KEYS {
            remote.insert(xxh3_64(format!("k{i}").as_bytes()), hlc(2 * i));
        }

        let mut plan = RepairPlan::default();
        handle_sketch_mismatch(
            &SmolStr::new("users"),
            7,
            remote.into_cells(),
            &local_entries,
            &mut plan,
            true,
        );

        assert!(
            plan.undecodable_buckets.is_empty(),
            "a two-sided diff this size peels at the default sketch shape, no fallback"
        );
        assert_eq!(
            plan.push_keys.len(),
            usize::try_from(KEYS).expect("KEYS is a small literal, always fits"),
            "merging pushes every mismatched key, not only the greater-version side"
        );
        let (bucket, hashes) = plan
            .pull_hashes
            .first()
            .expect("one bucket entry carries every pulled hash");
        assert_eq!(*bucket, 7);
        assert_eq!(
            hashes.len(),
            usize::try_from(KEYS).expect("KEYS is a small literal, always fits"),
            "merging pulls every mismatched key's hash too, converging both directions in one round"
        );
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

    fn part_mismatch_of(cells: Vec<Cell>, local_entries: &[(Bytes, Hlc)]) -> RepairPlan {
        let mut plan = RepairPlan::default();
        handle_part_sketch_mismatch(
            &SmolStr::new("users"),
            7,
            cells,
            local_entries,
            &mut plan,
            false,
        );
        plan
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
    fn settle_bucket_parts_drops_a_bucket_with_an_undecodable_part_and_queues_it_once() {
        let mut gathered: HashMap<u16, RepairPlan> = HashMap::new();
        gathered.insert(
            3,
            RepairPlan {
                push_keys: vec![Bytes::from_static(b"p3")],
                pull_keys: vec![Bytes::from_static(b"q3")],
                pull_hashes: vec![(3, vec![33])],
                undecodable_buckets: vec![3],
            },
        );
        gathered.insert(
            5,
            RepairPlan {
                push_keys: vec![Bytes::from_static(b"p5")],
                pull_keys: Vec::new(),
                pull_hashes: vec![(5, vec![55])],
                undecodable_buckets: Vec::new(),
            },
        );
        let mut plan = RepairPlan::default();
        settle_bucket_parts(gathered, &mut plan);
        assert_eq!(plan.push_keys, vec![Bytes::from_static(b"p5")]);
        assert!(plan.pull_keys.is_empty());
        assert_eq!(plan.pull_hashes, vec![(5, vec![55])]);
        assert_eq!(
            plan.undecodable_buckets,
            vec![3],
            "the bucket falls back once, its part work dropped"
        );
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
        diff_decoded(&local_entries, &decoded, &mut push, &mut pull, false);
        assert!(push.is_empty());
        assert_eq!(pull, vec![hash]);
    }

    fn bucket_entry(key: &[u8], wall_ms: u64) -> (Bytes, Hlc) {
        (Bytes::copy_from_slice(key), hlc(wall_ms))
    }

    #[test]
    fn diff_bucket_pushes_only_the_greater_local_version_when_not_merging() {
        let local = vec![bucket_entry(b"k1", 20)];
        let peer = vec![bucket_entry(b"k1", 10)];
        let (mut push, mut pull) = (Vec::new(), Vec::new());
        diff_bucket(&local, &peer, &mut push, &mut pull, false);
        assert_eq!(push, vec![Bytes::from_static(b"k1")]);
        assert!(pull.is_empty());
    }

    #[test]
    fn diff_bucket_pulls_only_the_greater_peer_version_when_not_merging() {
        let local = vec![bucket_entry(b"k1", 10)];
        let peer = vec![bucket_entry(b"k1", 20)];
        let (mut push, mut pull) = (Vec::new(), Vec::new());
        diff_bucket(&local, &peer, &mut push, &mut pull, false);
        assert!(push.is_empty());
        assert_eq!(pull, vec![Bytes::from_static(b"k1")]);
    }

    #[test]
    fn diff_bucket_queues_both_directions_for_a_mismatch_when_merging() {
        let local = vec![bucket_entry(b"k1", 10)];
        let peer = vec![bucket_entry(b"k1", 20)];
        let (mut push, mut pull) = (Vec::new(), Vec::new());
        diff_bucket(&local, &peer, &mut push, &mut pull, true);
        assert_eq!(
            push,
            vec![Bytes::from_static(b"k1")],
            "merging pushes this side's record even though the peer's version is greater"
        );
        assert_eq!(
            pull,
            vec![Bytes::from_static(b"k1")],
            "merging keeps the pull the non-merging rule already queues"
        );
    }

    #[test]
    fn diff_bucket_agreeing_versions_queue_neither_direction_regardless_of_merging() {
        let local = vec![bucket_entry(b"k1", 10)];
        let peer = vec![bucket_entry(b"k1", 10)];
        for merging in [false, true] {
            let (mut push, mut pull) = (Vec::new(), Vec::new());
            diff_bucket(&local, &peer, &mut push, &mut pull, merging);
            assert!(push.is_empty());
            assert!(pull.is_empty());
        }
    }

    #[test]
    fn diff_bucket_a_key_only_one_side_holds_is_unaffected_by_merging() {
        let local_only = vec![bucket_entry(b"local-key", 1)];
        let peer_only = vec![bucket_entry(b"peer-key", 1)];
        for merging in [false, true] {
            let (mut push, mut pull) = (Vec::new(), Vec::new());
            diff_bucket(&local_only, &peer_only, &mut push, &mut pull, merging);
            assert_eq!(push, vec![Bytes::from_static(b"local-key")]);
            assert_eq!(pull, vec![Bytes::from_static(b"peer-key")]);
        }
    }
}

//! Anti-entropy: every jittered `ae_interval`, each [`Mode::Replicated`]
//! cache reconciles itself against one live peer. The initiator sends its
//! 1,024 bucket digests; for each mismatch the peer answers with the bucket's
//! entry listing, or an IBLT sketch for a large bucket. The initiator diffs,
//! pushes what it has newer, and pulls what the peer has newer, so both sides
//! converge in one round. When the shard's resolver merges or reads values
//! (`ShardOps::ae_exchanges_both` is `true`), a version-mismatched key is
//! pushed *and* pulled instead of only in the greater side's direction, so
//! a replica holding the resolver's winner at the lower version sends it, and
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
use crate::error::CodecError;
use crate::hlc::Hlc;
use crate::net::{AeMismatch, AePartReply, AeRoundOutcome, Mesh, MsgClass};
use crate::node::NodeId;
use crate::store::part::PartSet;
use crate::store::{BucketPart, PartId, PartMask, Scope, ScopedDigests, ShardOps};
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
    /// The digest exchange failed, so no repair ran, or a repair request
    /// after it failed, so the repairs are incomplete.
    Failed,
}

/// One anti-entropy round against `peer`: exchanges digests, then diffs the
/// mismatched buckets. A key this node has newer, or `peer` lacks, pushes
/// via the normal `Replicate` path; a key `peer` has newer, or this node
/// lacks, pulls and applies directly. When `shard`'s resolver merges or reads
/// values ([`ShardOps::ae_exchanges_both`]), a key present on both sides
/// under different versions pushes *and* pulls, so a replica holding the
/// winner at the lower version sends it, and two replicas each holding half
/// of a merge converge in this one round rather than needing a second.
///
/// A `Mode::Distributed` shard exchanges digests through
/// `Mesh::ae_round_scoped`, or `Mesh::ae_round_masked` under a part view,
/// instead of [`Mesh::ae_round`], carrying its
/// [`ShardOps::ownership_view_hash`] for the epoch check; a `Stale` reply
/// ends the round at once, counted in `sundog_stale_view_total`.
///
/// Takes `mesh` directly, not a whole `&Cluster`: `tests/sim/` drives
/// this same seam under `feature = "sim"` against a hand-built
/// `Mesh`/`ShardOps` pair, skipping a real `Cluster`'s gossip and discovery.
#[tracing::instrument(skip_all, fields(cache = %cache, peer = %peer))]
pub async fn run_round_against(
    mesh: &Mesh,
    shard: &Arc<dyn ShardOps>,
    cache: &SmolStr,
    peer: NodeId,
) -> RoundOutcome {
    // Read once per round: a resolver that merges or reads values has both
    // sides exchange a mismatched key instead of only the greater version
    // pushing to the lesser side. See `ShardOps::ae_exchanges_both`.
    let both_ways = shard.ae_exchanges_both();
    let (mismatched, in_scope) = match shard.ae_scoped_digests_for(peer).await {
        Some(scoped) => {
            // Only what `peer` co-owns, read under the view whose hash it
            // travels with: the whole resident list would have everything
            // else reported as a mismatch and its entries pushed only to be
            // dropped by the peer's inbound guard.
            let in_scope = scoped.scope.coverage().into_iter().collect();
            match scoped_exchange(mesh, cache, peer, scoped).await {
                Ok(AeRoundOutcome::Mismatches(mismatched)) => (mismatched, Some(in_scope)),
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
            Ok(mismatched) => (mismatched, None),
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

    let repairs =
        reconcile_mismatches(mesh, shard, cache, peer, mismatched, in_scope, both_ways).await;
    if repairs.complete {
        RoundOutcome::Reconciled
    } else {
        RoundOutcome::Failed
    }
}

/// What one round's repairs did: the wire bytes they moved, and whether
/// every request they issued to the peer was answered. A request that
/// fails, an unknown peer after a failure-detector flap among the causes,
/// leaves some mismatch unrepaired.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Repairs {
    bytes_moved: u64,
    complete: bool,
}

/// Sends `scoped`'s digests to `peer` in the exchange its scope calls for:
/// bucket digests, or masked part digests under a part view.
async fn scoped_exchange(
    mesh: &Mesh,
    cache: &SmolStr,
    peer: NodeId,
    scoped: ScopedDigests,
) -> Result<AeRoundOutcome, CodecError> {
    let ScopedDigests { view_hash, scope } = scoped;
    match scope {
        Scope::Buckets(ids) => {
            mesh.ae_round_scoped(peer, cache.clone(), view_hash, ids)
                .await
        }
        Scope::Parts(masked) => {
            let buckets = masked
                .iter()
                .map(|m| (m.bucket, m.mask.bits(), m.digest))
                .collect();
            mesh.ae_round_masked(peer, cache.clone(), view_hash, buckets)
                .await
        }
    }
}

/// The classify-through-repair pipeline shared by [`run_round_against`] and
/// [`run_round_for_buckets`]: classifies one round's `mismatched` reply
/// into a [`RepairPlan`], issues the `Msg::AeEntries` fallback for a
/// sketch that failed to decode, then runs [`apply_repairs`]. Returns the
/// total wire bytes moved, and whether every request to `peer` was
/// answered.
///
/// Buckets past `ae_part_min_bucket` answered with part digests never
/// load their full entries here; only listing/sketch buckets go through
/// `entries_for_buckets`. A part-digest reply repairs only the parts
/// `in_scope` names for its bucket; `None` repairs every part.
async fn reconcile_mismatches(
    mesh: &Mesh,
    shard: &Arc<dyn ShardOps>,
    cache: &SmolStr,
    peer: NodeId,
    mismatched: Vec<AeMismatch>,
    in_scope: Option<HashMap<u16, PartMask>>,
    both_ways: bool,
) -> Repairs {
    let (part_digest_mismatches, bucket_mismatches): (Vec<AeMismatch>, Vec<AeMismatch>) =
        mismatched
            .into_iter()
            .partition(|m| matches!(m, AeMismatch::PartDigests(..)));

    let mut plan = RepairPlan {
        in_scope,
        ..RepairPlan::default()
    };
    classify_bucket_mismatches(shard, cache, bucket_mismatches, &mut plan, both_ways).await;
    let mut complete = classify_part_digest_mismatches(
        mesh,
        shard,
        cache,
        peer,
        part_digest_mismatches,
        &mut plan,
        both_ways,
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
                        both_ways,
                    );
                }
            }
            Err(error) => {
                tracing::debug!(%error, "anti-entropy sketch-fallback listing failed");
                complete = false;
            }
        }
    }

    retain_owned_pulls(shard, &mut plan.pull_keys, &mut plan.pull_hashes);
    retain_peer_owned_pushes(shard, peer, &mut plan.push_keys);
    let repairs = apply_repairs(
        mesh,
        shard,
        cache,
        peer,
        plan.push_keys,
        plan.pull_keys,
        plan.pull_hashes,
        both_ways,
    )
    .await;
    Repairs {
        bytes_moved: repairs.bytes_moved,
        complete: complete && repairs.complete,
    }
}

/// How one [`run_round_for_parts`] round ended, per part: which matched,
/// which stayed diverged, wire bytes moved, and whether it failed.
#[derive(Debug, Clone, Default)]
pub(crate) struct PartRoundOutcome {
    /// No mismatch this round: converged at this instant, not a claim of
    /// permanent equality.
    pub(crate) matched: HashSet<PartId>,
    /// Named mismatched, or requested but dropped because this shard's
    /// fresh ownership read disagrees `peer` co-owns it; either way never
    /// compared this round, so never `matched`.
    #[cfg_attr(
        any(not(test), feature = "sim"),
        allow(
            dead_code,
            reason = "only tests read which requested parts stayed diverged"
        )
    )]
    pub(crate) still_diverged: HashSet<PartId>,
    /// Wire bytes pushed plus pulled this round: `0` when nothing
    /// mismatched or the round failed.
    pub(crate) bytes_moved: u64,
    /// The digest exchange errored, the shard has no ownership view, or
    /// the peer answered `Stale`, so no repair ran; or a repair request
    /// after the exchange failed, so the repairs are incomplete.
    pub(crate) failed: bool,
}

/// [`run_round_for_parts`]'s outcome for a round that could not be scoped
/// or answered at all: everything requested counts as `still_diverged`.
fn every_part_diverged(requested: &HashSet<PartId>) -> PartRoundOutcome {
    PartRoundOutcome {
        matched: HashSet::new(),
        still_diverged: requested.clone(),
        bytes_moved: 0,
        failed: true,
    }
}

/// Which of `requested` a round compared equal: every part in scope of a
/// `covered` bucket the reply did not name mismatched. A pure function of
/// the round's scope, split out of [`run_round_for_parts`] for its unit
/// test.
fn matched_parts(
    requested: &HashSet<PartId>,
    covered: &[(u16, PartMask)],
    named_mismatched: &HashSet<u16>,
) -> HashSet<PartId> {
    covered
        .iter()
        .filter(|(bucket, _)| !named_mismatched.contains(bucket))
        .flat_map(|&(bucket, mask)| mask.parts().map(move |part| PartId::new(bucket, part)))
        .filter(|part| requested.contains(part))
        .collect()
}

/// Part-scoped sibling of [`run_round_against`]: scoped to `parts` and
/// reports which specifically matched instead of collapsing to one
/// [`RoundOutcome`]. `Cache::reconcile_warm_buckets`'s converge-before-
/// serving loop calls this per live co-owner, per round, and
/// `rebalance_task` calls it to push a lost part no co-owner can hand over
/// to that part's new owners.
///
/// Reads the shard's scoped digests once, so the scope it sends and the
/// view hash it travels with come from one view, then keeps the buckets
/// whose scope holds a requested part. A requested part the live view says
/// `peer` does not co-own is out of scope and lands in `still_diverged`
/// rather than `matched`. A shard with no ownership view,
/// a `Stale` reply, or a failed exchange reports everything
/// `still_diverged` with `failed: true`.
pub(crate) async fn run_round_for_parts(
    mesh: &Mesh,
    shard: &Arc<dyn ShardOps>,
    cache: &SmolStr,
    peer: NodeId,
    parts: &[PartId],
) -> PartRoundOutcome {
    let requested: HashSet<PartId> = parts.iter().copied().collect();
    let Some(mut scoped) = shard.ae_scoped_digests_for(peer).await else {
        return every_part_diverged(&requested);
    };
    let wanted: PartSet = parts.iter().copied().collect();
    scoped.scope.retain(|bucket, mask| {
        mask.parts()
            .any(|part| wanted.contains(PartId::new(bucket, part)))
    });
    let covered = scoped.scope.coverage();
    let both_ways = shard.ae_exchanges_both();
    let mismatched = match scoped_exchange(mesh, cache, peer, scoped).await {
        Ok(AeRoundOutcome::Mismatches(mismatched)) => mismatched,
        Ok(AeRoundOutcome::Stale {
            responder_view_hash,
        }) => {
            metrics::counter!("sundog_stale_view_total", "cache" => cache.to_string()).increment(1);
            tracing::debug!(
                responder_view_hash,
                "anti-entropy part-scoped round ended: responder's view has diverged"
            );
            return every_part_diverged(&requested);
        }
        Err(error) => {
            tracing::debug!(%error, "anti-entropy part-scoped digest exchange failed");
            return every_part_diverged(&requested);
        }
    };
    let named_mismatched: HashSet<u16> = mismatched.iter().map(AeMismatch::bucket).collect();
    let matched = matched_parts(&requested, &covered, &named_mismatched);
    let still_diverged: HashSet<PartId> = requested.difference(&matched).copied().collect();
    if mismatched.is_empty() {
        return PartRoundOutcome {
            matched,
            still_diverged,
            bytes_moved: 0,
            failed: false,
        };
    }
    let in_scope = covered.into_iter().collect();
    let repairs = reconcile_mismatches(
        mesh,
        shard,
        cache,
        peer,
        mismatched,
        Some(in_scope),
        both_ways,
    )
    .await;
    PartRoundOutcome {
        matched,
        still_diverged,
        bytes_moved: repairs.bytes_moved,
        failed: !repairs.complete,
    }
}

/// Drops every queued push of a key whose part `peer` does not own under a
/// `Mode::Distributed` shard's current view: a whole-bucket fallback
/// listing covers parts the peer never holds, and pushing them only reaches
/// the peer's inbound guard to forward or drop. Identity for a shard with
/// no ownership view.
fn retain_peer_owned_pushes(shard: &Arc<dyn ShardOps>, peer: NodeId, push_keys: &mut Vec<Bytes>) {
    if let Some(view) = shard.ownership_view() {
        push_keys.retain(|key| view.owners_of(PartId::of_key(key)).contains(&peer));
    }
}

/// Drops every queued pull for a part a `Mode::Distributed` shard does not
/// own: a round against a new owner of a part this node is releasing pushes
/// what the owner lacks and leaves the owner's own entries where they are,
/// instead of pulling them into the inbound guard. A hash pull names each
/// entry by its full key hash, so its part is exact whatever the round's
/// ids name. Identity for a shard with no ownership view.
fn retain_owned_pulls(
    shard: &Arc<dyn ShardOps>,
    pull_keys: &mut Vec<Bytes>,
    pull_hashes: &mut Vec<(u16, Vec<u64>)>,
) {
    if let Some(view) = shard.ownership_view() {
        pull_keys.retain(|key| view.owns(PartId::of_key(key)));
        for (_, hashes) in pull_hashes.iter_mut() {
            hashes.retain(|&hash| view.owns(PartId::from_hash(hash)));
        }
        pull_hashes.retain(|(_, hashes)| !hashes.is_empty());
    }
}

/// One round's push/pull/hash-pull/fallback classification, threaded as
/// `&mut RepairPlan` instead of four separate out-parameters, and the
/// parts of each bucket it may repair: `in_scope`'s, or every part when
/// `None`.
#[derive(Default)]
struct RepairPlan {
    in_scope: Option<HashMap<u16, PartMask>>,
    push_keys: Vec<Bytes>,
    pull_keys: Vec<Bytes>,
    pull_hashes: Vec<(u16, Vec<u64>)>,
    undecodable_buckets: Vec<u16>,
}

impl RepairPlan {
    /// The parts of `bucket` this round may repair.
    fn repairable(&self, bucket: u16) -> PartMask {
        self.in_scope.as_ref().map_or(PartMask::ALL, |scope| {
            scope.get(&bucket).copied().unwrap_or_default()
        })
    }

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
/// sends the greater version's side, and `both_ways` (a resolver that merges
/// or reads values) additionally sends the other direction too.
fn classify_versions(local: Option<Hlc>, peer: Option<Hlc>, both_ways: bool) -> (bool, bool) {
    match (local, peer) {
        (Some(local), Some(peer)) if local != peer => {
            (local > peer || both_ways, local < peer || both_ways)
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
    both_ways: bool,
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
                        both_ways,
                    );
                }
                AeMismatch::Sketch(bucket, cells) => {
                    let entries: &[(Bytes, Hlc)] =
                        local_by_bucket.get(&bucket).map_or(&[], Vec::as_slice);
                    handle_sketch_mismatch(cache, bucket, cells, entries, plan, both_ways);
                }
                AeMismatch::PartDigests(..) => {
                    unreachable!("invariant: reconcile_mismatches partitions this variant out")
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
/// one per remaining chunk. Returns `false` when a call failed, leaving
/// the rest of the mismatches unclassified.
///
/// [`Mesh::ae_parts`]: crate::net::Mesh::ae_parts
async fn classify_part_digest_mismatches(
    mesh: &crate::net::Mesh,
    shard: &Arc<dyn ShardOps>,
    cache: &SmolStr,
    peer: NodeId,
    mismatches: Vec<AeMismatch>,
    plan: &mut RepairPlan,
    both_ways: bool,
) -> bool {
    for chunk in chunk_mismatches(mismatches, CLASSIFY_BUCKET_CHUNK) {
        let ok =
            classify_part_digest_mismatch_chunk(mesh, shard, cache, peer, chunk, plan, both_ways)
                .await;
        if !ok {
            return false;
        }
    }
    true
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
    both_ways: bool,
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
        let mask = plan.repairable(bucket);
        wanted_parts.extend(
            mismatched_parts(local_parts, digests)
                .into_iter()
                .filter(|&part| mask.contains(part))
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
            classify_part_replies(shard, cache, replies, plan, both_ways).await;
            true
        }
        Err(error) => {
            tracing::debug!(%error, "anti-entropy part exchange failed");
            false
        }
    }
}

/// Classifies part listings and sketches against this node's own entries
/// for the same parts, the way [`classify_bucket_mismatches`] does for
/// whole buckets: the second half of a part-digest exchange, and the
/// whole of a part-granular scoped round's answer. A part sketch that
/// fails to peel marks its bucket for the `Msg::AeEntries` fallback.
async fn classify_part_replies(
    shard: &Arc<dyn ShardOps>,
    cache: &SmolStr,
    replies: Vec<AePartReply>,
    plan: &mut RepairPlan,
    both_ways: bool,
) {
    if replies.is_empty() {
        return;
    }
    let wanted_parts: Vec<BucketPart> = replies
        .iter()
        .map(|reply| BucketPart {
            bucket: reply.bucket(),
            part: reply.part(),
        })
        .collect();
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
                    both_ways,
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
                handle_part_sketch_mismatch(cache, bucket, cells, entries, slot, both_ways);
            }
        }
    }
    settle_bucket_parts(gathered, plan);
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
/// [`REPAIR_BATCH`] chunks, counts each chunk under
/// `sundog_ae_repaired_total{cache}` as it moves ([`RepairTally::add`]), and
/// returns the total wire bytes moved (pushed plus pulled) and whether
/// every pull was answered and every push reached `peer`'s outbox.
///
/// Without `both_ways`, pushes go out first: the two sets are disjoint. With
/// `both_ways` (a resolver that merges or reads values), a key both sides
/// hold sits in both sets, so pulls run first
/// and fold the peer's record in, and a key whose stored version is then
/// exactly the version the peer sent is not pushed back: the peer already
/// holds it.
#[allow(
    clippy::too_many_arguments,
    reason = "one round's classified sets plus the peer and resolver they repair against"
)]
async fn apply_repairs(
    mesh: &crate::net::Mesh,
    shard: &Arc<dyn ShardOps>,
    cache: &SmolStr,
    peer: NodeId,
    push_keys: Vec<Bytes>,
    pull_keys: Vec<Bytes>,
    pull_hashes: Vec<(u16, Vec<u64>)>,
    both_ways: bool,
) -> Repairs {
    let mut tally = RepairTally::default();
    let complete = if both_ways {
        let (pulled, pulled_all) =
            pull_repairs(mesh, shard, cache, peer, pull_keys, pull_hashes, &mut tally).await;
        let pushed_all =
            push_repairs(mesh, shard, cache, peer, &push_keys, &pulled, &mut tally).await;
        pulled_all && pushed_all
    } else {
        let pushed_all = push_repairs(
            mesh,
            shard,
            cache,
            peer,
            &push_keys,
            &HashMap::new(),
            &mut tally,
        )
        .await;
        let (_, pulled_all) =
            pull_repairs(mesh, shard, cache, peer, pull_keys, pull_hashes, &mut tally).await;
        pushed_all && pulled_all
    };

    tracing::debug!(
        repaired = tally.repaired,
        bytes_moved = tally.bytes_moved,
        "anti-entropy round complete"
    );
    Repairs {
        bytes_moved: tally.bytes_moved,
        complete,
    }
}

/// Records and wire bytes one round's repairs moved, both directions.
#[derive(Default)]
struct RepairTally {
    repaired: u64,
    bytes_moved: u64,
}

impl RepairTally {
    /// Counts one chunk of `records` moved for `cache`, and adds it to
    /// `sundog_ae_repaired_total{cache}` at once, not at the end of the
    /// round, so the counter keeps up with the bytes a long round has
    /// already sent.
    fn add(&mut self, cache: &SmolStr, records: &[WireRecord]) {
        if !records.is_empty() {
            metrics::counter!("sundog_ae_repaired_total", "cache" => cache.to_string())
                .increment(records.len() as u64);
        }
        self.repaired += records.len() as u64;
        self.bytes_moved += records
            .iter()
            .map(|rec| wire_record_len(cache.len(), rec))
            .sum::<u64>();
    }
}

/// Whether a record stored at `local` still needs pushing to a peer whose
/// record for the key this round pulled at `pulled`: always, unless the
/// peer sent exactly that version.
fn push_needed(local: Hlc, pulled: Option<Hlc>) -> bool {
    pulled != Some(local)
}

/// Pushes `push_keys`' current records to `peer`, skipping any whose stored
/// version `pulled` shows the peer already holds. Answers whether every
/// batch reached `peer`'s outbox: a peer a flap dropped from the mesh
/// receives none of it.
async fn push_repairs(
    mesh: &crate::net::Mesh,
    shard: &Arc<dyn ShardOps>,
    cache: &SmolStr,
    peer: NodeId,
    push_keys: &[Bytes],
    pulled: &HashMap<Bytes, Hlc>,
    tally: &mut RepairTally,
) -> bool {
    let mut enqueued = true;
    // Batched so a large divergence makes durable incremental progress:
    // each landed batch shrinks the next round's diff, instead of one
    // all-or-nothing exchange racing a request timeout.
    for batch in push_keys.chunks(REPAIR_BATCH) {
        let mut records = shard.records_for(batch.to_vec()).await;
        records.retain(|rec| push_needed(rec.ver, pulled.get(&rec.key).copied()));
        tally.add(cache, &records);
        // `net::batch_replicate` chunks this into a handful of full frames, not
        // one `Msg::Replicate` per record.
        let msgs = crate::net::batch_replicate(cache, records);
        enqueued &= mesh.send_many_enqueued(peer, MsgClass::Replicate, msgs);
    }
    enqueued
}

/// Pulls `pull_keys` and then `pull_hashes` from `peer` and applies them.
/// A failed request ends its own list, keeping what earlier batches
/// landed. Returns the version of every record the peer sent, by key, and
/// whether every request was answered.
async fn pull_repairs(
    mesh: &crate::net::Mesh,
    shard: &Arc<dyn ShardOps>,
    cache: &SmolStr,
    peer: NodeId,
    pull_keys: Vec<Bytes>,
    pull_hashes: Vec<(u16, Vec<u64>)>,
    tally: &mut RepairTally,
) -> (HashMap<Bytes, Hlc>, bool) {
    let mut pulled = HashMap::new();
    let mut complete = true;
    let mut land = async |records: Vec<WireRecord>, pulled: &mut HashMap<Bytes, Hlc>| {
        tally.add(cache, &records);
        pulled.extend(records.iter().map(|rec| (rec.key.clone(), rec.ver)));
        shard.apply_remote_batch(records).await;
    };
    for batch in pull_keys.chunks(REPAIR_BATCH) {
        match mesh.ae_pull(peer, cache.clone(), batch.to_vec()).await {
            Ok(records) => land(records, &mut pulled).await,
            Err(error) => {
                tracing::debug!(%error, "anti-entropy pull failed; keeping progress");
                complete = false;
                break;
            }
        }
    }
    for (bucket, hashes) in pull_hashes {
        for batch in hashes.chunks(REPAIR_BATCH) {
            match mesh
                .ae_pull_hashes(peer, cache.clone(), bucket, batch.to_vec())
                .await
            {
                Ok(records) => land(records, &mut pulled).await,
                Err(error) => {
                    tracing::debug!(%error, "anti-entropy hash pull failed; keeping progress");
                    return (pulled, false);
                }
            }
        }
    }
    (pulled, complete)
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
    both_ways: bool,
) {
    if !peel_sketch_into(
        SketchScope::Bucket,
        cache,
        bucket,
        cells,
        local_entries,
        plan,
        both_ways,
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
    both_ways: bool,
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
        both_ways,
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
/// Reachable outside `cluster::anti_entropy` only because `tests/sim/`
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
    both_ways: bool,
) {
    if !peel_sketch_into(
        SketchScope::Part,
        cache,
        bucket,
        cells,
        local_entries,
        plan,
        both_ways,
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
    both_ways: bool,
) {
    let peer_by_key: HashMap<&Bytes, Hlc> = peer_entries.iter().map(|(k, v)| (k, *v)).collect();
    let mut local_keys = HashSet::with_capacity(local_entries.len());

    for (key, local_ver) in local_entries {
        local_keys.insert(key);
        let (push, pull) =
            classify_versions(Some(*local_ver), peer_by_key.get(key).copied(), both_ways);
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
/// Reachable outside `cluster::anti_entropy` only because `tests/sim/`
/// re-exports it as `crate::diff_decoded` under `feature = "sim"`, to
/// reconcile a peeled sketch the same way `handle_sketch_mismatch` does
/// here, without duplicating this classification.
pub fn diff_decoded(
    local_entries: &[(Bytes, Hlc)],
    decoded: &Decoded,
    push_keys: &mut Vec<Bytes>,
    pull_hashes: &mut Vec<u64>,
    both_ways: bool,
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
            both_ways,
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
mod tests;

/// Kani proof over the scheduler's skip rule.
#[cfg(kani)]
mod kani_proofs {
    use super::*;

    /// A streaming peer is skipped at most `MAX_STREAMING_SKIPS` rounds running; an idle peer never.
    #[kani::proof]
    fn streaming_skips_are_bounded() {
        let streaming: bool = kani::any();
        let skipped_so_far: u32 = kani::any();
        let skip = should_skip_round(streaming, skipped_so_far);
        assert_eq!(skip, streaming && skipped_so_far < MAX_STREAMING_SKIPS);
    }
}

//! The single consumer of the mesh's inbound-message channel, and the re-forward of a `ForwardBatch` routed under another view.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use smol_str::SmolStr;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::{InboundActivity, ShardRegistry, ShardRegistryExt, fan_out};
use crate::net::{InboundMsg, Mesh, OutFrame, batch_forward};
use crate::node::NodeId;
use crate::ownership::OwnershipView;
use crate::store::ShardOps;
use crate::wire::{Msg, WireRecord};

fn lookup_shard(shards: &ShardRegistry, cache: &SmolStr) -> Option<Arc<dyn ShardOps>> {
    shards.read_shards().get(cache).cloned()
}

/// Cap on how many queued inbound messages [`inbound_loop`] drains per wake.
const INBOUND_DRAIN_CAP: usize = 1024;

/// One accumulated run of records bound for one shard in one call: the
/// records of consecutive `Replicate`/`ReplicateBatch` messages for a cache,
/// or of consecutive `ForwardBatch` messages for it.
struct PendingRun {
    cache: SmolStr,
    /// Whether the records came in a `ForwardBatch`, which the shard applies
    /// through [`ShardOps::apply_forwarded_batch`].
    forwarded: bool,
    recs: Vec<WireRecord>,
}

/// Whether a message for `cache` (`forwarded` says it is a `ForwardBatch`)
/// joins the `pending` run `(cache, forwarded)`. A forward and a replicate
/// never share a call: the shard holds a forward it refuses for clock skew
/// and drops a replicate it refuses. Pure; unit tested directly.
pub(super) fn same_run(
    pending: Option<(&SmolStr, bool)>,
    cache: &SmolStr,
    forwarded: bool,
) -> bool {
    pending.is_some_and(|(pending_cache, pending_forwarded)| {
        pending_cache == cache && pending_forwarded == forwarded
    })
}

/// Applies one accumulated run under one `apply_remote_batch` call, or one
/// `apply_forwarded_batch` call for a run of forwards. `shard_cache`
/// memoizes the lookup per drained batch, so a cache name costs at most one
/// lock acquisition.
async fn apply_pending_run(
    shards: &ShardRegistry,
    shard_cache: &mut HashMap<SmolStr, Option<Arc<dyn ShardOps>>>,
    run: PendingRun,
) {
    let PendingRun {
        cache,
        forwarded,
        recs,
    } = run;
    let shard = shard_cache
        .entry(cache.clone())
        .or_insert_with(|| lookup_shard(shards, &cache));
    if let Some(shard) = shard {
        if forwarded {
            shard.apply_forwarded_batch(recs).await;
        } else {
            shard.apply_remote_batch(recs).await;
        }
    } else {
        tracing::trace!(%cache, "replicate batch for unknown cache; dropped");
    }
}

/// Appends `recs` to the pending run for `cache`, or applies the run in
/// progress first when `cache` or `forwarded` differs and starts a new one.
async fn queue_run(
    shards: &ShardRegistry,
    shard_cache: &mut HashMap<SmolStr, Option<Arc<dyn ShardOps>>>,
    pending: &mut Option<PendingRun>,
    cache: SmolStr,
    forwarded: bool,
    mut recs: Vec<WireRecord>,
) {
    let joins = same_run(
        pending.as_ref().map(|run| (&run.cache, run.forwarded)),
        &cache,
        forwarded,
    );
    match pending {
        Some(run) if joins => run.recs.append(&mut recs),
        _ => {
            if let Some(old) = pending.take() {
                apply_pending_run(shards, shard_cache, old).await;
            }
            *pending = Some(PendingRun {
                cache,
                forwarded,
                recs,
            });
        }
    }
}

/// Notes every peer with a record-carrying message in `drained` as
/// streaming toward this node, once per peer per drain.
fn note_streaming_peers(activity: &InboundActivity, drained: &[InboundMsg]) {
    let mut streaming_from: HashSet<NodeId> = HashSet::new();
    for InboundMsg { from, msg } in drained {
        if matches!(
            msg,
            Msg::Replicate { .. } | Msg::ReplicateBatch { .. } | Msg::ForwardBatch { .. }
        ) {
            streaming_from.insert(*from);
        }
    }
    for from in streaming_from {
        activity.note(from);
    }
}

/// The shard `cache` names, looked up in the registry once per drained
/// burst and remembered in `shard_cache`; `None` for a cache not open here.
fn burst_shard<'a>(
    shard_cache: &'a mut HashMap<SmolStr, Option<Arc<dyn ShardOps>>>,
    shards: &ShardRegistry,
    cache: &SmolStr,
) -> Option<&'a Arc<dyn ShardOps>> {
    shard_cache
        .entry(cache.clone())
        .or_insert_with(|| lookup_shard(shards, cache))
        .as_ref()
}

/// The single consumer of `Mesh`'s inbound-message channel: dispatches
/// `Invalidate`/`Replicate`/`ReplicateBatch`/`ForwardBatch` to the named
/// shard, dropping a message for an unregistered cache with a trace event.
/// Drains a bounded batch per wake with `recv_many`, so a run of same-cache
/// messages coalesces into one `apply_remote_batch` call, and a run of
/// `ForwardBatch` messages into one `apply_forwarded_batch` call, which holds a
/// record the shard refuses for clock skew instead of dropping it. A `ForwardBatch`
/// under a stale view hash is handed to the [`Reforwarder`], which sends it
/// on to its records' current owners from a lane per peer, then applied like
/// any other: the loop waits on no socket, so a peer that stops reading
/// holds back only the re-forwards queued toward it. On cancel, or once the
/// channel closes, the loop gives the lanes [`REFORWARD_DRAIN_DEADLINE`] to
/// hand their batches to the mesh before it returns.
#[allow(
    clippy::too_many_lines,
    reason = "one match arm per inbound message kind; splitting it up would obscure the dispatch table"
)]
pub(super) async fn inbound_loop(
    shards: ShardRegistry,
    activity: Arc<InboundActivity>,
    mesh: Mesh,
    self_node: NodeId,
    mut inbound: mpsc::Receiver<InboundMsg>,
    cancel: CancellationToken,
) {
    let mut reforwarder = Reforwarder::new(mesh);
    let mut drained: Vec<InboundMsg> = Vec::with_capacity(INBOUND_DRAIN_CAP);
    loop {
        drained.clear();
        tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            received = inbound.recv_many(&mut drained, INBOUND_DRAIN_CAP) => {
                if received == 0 {
                    break; // channel closed, nothing left to drain
                }
            }
        }

        note_streaming_peers(&activity, &drained);
        reforwarder.retire_departed();

        let mut shard_cache: HashMap<SmolStr, Option<Arc<dyn ShardOps>>> = HashMap::new();
        let mut pending: Option<PendingRun> = None;
        for InboundMsg { from, msg } in drained.drain(..) {
            match msg {
                Msg::Replicate { cache, rec } => {
                    queue_run(
                        &shards,
                        &mut shard_cache,
                        &mut pending,
                        cache,
                        false,
                        vec![rec],
                    )
                    .await;
                }
                Msg::ReplicateBatch { cache, recs } => {
                    queue_run(&shards, &mut shard_cache, &mut pending, cache, false, recs).await;
                }
                Msg::ForwardBatch {
                    cache,
                    view_hash,
                    hops,
                    recs,
                } => {
                    if let Some(shard) = burst_shard(&mut shard_cache, &shards, &cache) {
                        reforward_stale_view(
                            &mut reforwarder,
                            ReforwardBatch {
                                self_node,
                                from,
                                shard: shard.as_ref(),
                                cache_name: &cache,
                                sender_view_hash: view_hash,
                                hops,
                                records: &recs,
                            },
                        );
                    }
                    queue_run(&shards, &mut shard_cache, &mut pending, cache, true, recs).await;
                }
                Msg::Invalidate { cache, key, ver } => {
                    if let Some(run) = pending.take() {
                        apply_pending_run(&shards, &mut shard_cache, run).await;
                    }
                    let Some(shard) = burst_shard(&mut shard_cache, &shards, &cache) else {
                        tracing::trace!(%cache, %from, "invalidate for unknown cache; dropped");
                        continue;
                    };
                    shard.invalidate(key, ver).await;
                }
                Msg::RefreshHint { cache, key, ver } => {
                    if let Some(shard) = burst_shard(&mut shard_cache, &shards, &cache) {
                        shard.refresh_hint(key, ver).await;
                    }
                }
                // `Hello` and the request/response messages never reach this channel.
                Msg::Hello { .. }
                | Msg::StRequest { .. }
                | Msg::StChunk { .. }
                | Msg::AeDigest { .. }
                | Msg::AeBucket { .. }
                | Msg::AeSketch { .. }
                | Msg::AeEntries { .. }
                | Msg::AePull { .. }
                | Msg::AePullHashes { .. }
                | Msg::AePartDigests { .. }
                | Msg::AeParts { .. }
                | Msg::AePart { .. }
                | Msg::AePartSketch { .. }
                | Msg::StUnavailable { .. }
                | Msg::Fetch { .. }
                | Msg::FetchReply { .. }
                | Msg::FetchDeclined { .. }
                | Msg::AeDigestScoped { .. }
                | Msg::AeDigestMasked { .. }
                | Msg::StBuckets { .. }
                | Msg::StBucketChunk { .. }
                | Msg::StaleView { .. }
                | Msg::StBucketDone { .. }
                | Msg::StBucketAck { .. }
                | Msg::Load { .. }
                | Msg::Loaded { .. }
                | Msg::LoadFailed { .. }
                | Msg::LoadDeclined { .. }
                | Msg::ReqDone => {}
            }
        }
        if let Some(run) = pending.take() {
            apply_pending_run(&shards, &mut shard_cache, run).await;
        }
    }
    reforwarder.shutdown().await;
}

/// The most hops a `Mode::Distributed` write travels: one from the writer
/// to the owners under its view, and at most one more from a receiver
/// whose view disagrees to the owners under its own. A receiver never
/// re-forwards a batch already at this count, so two disagreeing views
/// cost one extra hop and never a loop.
pub(super) const MAX_FORWARD_HOPS: u8 = 1;

/// Which peers a `ForwardBatch` received from `from` under a view hash
/// other than this node's own re-forwards to: each record's owners under
/// `view`, this node and `from` excluded (this node applies its own share
/// directly; `from` is either an owner that already holds the record or a
/// non-owner that never wanted it), grouped by target set like
/// [`group_by_owner_set`]. Pure and mesh-free. `None` when `hops` already
/// reached [`MAX_FORWARD_HOPS`] or the hashes agree, so the caller sends
/// nothing.
pub(super) fn reforward_groups(
    view: &OwnershipView,
    self_node: NodeId,
    from: NodeId,
    sender_view_hash: u64,
    hops: u8,
    records: &[WireRecord],
) -> Option<Vec<fan_out::OwnerGroup>> {
    if hops >= MAX_FORWARD_HOPS || sender_view_hash == view.view_hash() {
        return None;
    }
    let groups: Vec<fan_out::OwnerGroup> = fan_out::group_by_owner_set(
        view,
        self_node,
        records.iter().map(WireRecord::detached).collect(),
    )
    .into_iter()
    .map(|group| fan_out::OwnerGroup {
        owners: group
            .owners
            .into_iter()
            .filter(|&n| n != from)
            .collect::<Vec<_>>(),
        records: group.records,
    })
    .filter(|group| !group.owners.is_empty())
    .collect();
    Some(groups)
}

/// A `ForwardBatch` re-forwarded to its records' true owners under this
/// node's ownership view, gathered here so [`reforward_stale_view`] takes
/// one argument beyond the [`Reforwarder`] it sends through.
#[derive(Clone, Copy)]
struct ReforwardBatch<'a> {
    self_node: NodeId,
    from: NodeId,
    shard: &'a dyn ShardOps,
    cache_name: &'a SmolStr,
    sender_view_hash: u64,
    hops: u8,
    records: &'a [WireRecord],
}

/// Re-forwards a `ForwardBatch` the sender routed under a stale or merely
/// different ownership view to the records' owners under this node's
/// view, so a write whose target set the writer got wrong still reaches
/// every current owner: a tombstone from a writer that has not yet seen a
/// replacement owner join lands on that owner too, instead of waiting for
/// an anti-entropy round to pair the two, and a record for a bucket this
/// node does not own at all travels on instead of being dropped by the
/// inbound guard. Sent as a `hops + 1` batch stamped with this node's own
/// view hash. A no-op for a shard without an ownership view (every mode
/// but `Mode::Distributed`). Encodes and enqueues on the owners' lanes and
/// never waits on a peer.
fn reforward_stale_view(reforwarder: &mut Reforwarder, batch: ReforwardBatch<'_>) {
    let ReforwardBatch {
        self_node,
        from,
        shard,
        cache_name,
        sender_view_hash,
        hops,
        records,
    } = batch;
    let Some(view) = shard.ownership_view() else {
        return;
    };
    let Some(groups) = reforward_groups(&view, self_node, from, sender_view_hash, hops, records)
    else {
        return;
    };
    if groups.is_empty() {
        return;
    }
    let forwarded: usize = groups.iter().map(|g| g.records.len()).sum();
    metrics::counter!(
        "sundog_forwarded_writes_total",
        "cache" => cache_name.to_string()
    )
    .increment(u64::try_from(forwarded).unwrap_or(u64::MAX));
    tracing::debug!(
        cache = %cache_name,
        peer = %from,
        sender_view_hash,
        local_view_hash = view.view_hash(),
        records = forwarded,
        "forward batch routed under another view; re-forwarded to its owners under ours"
    );
    for fan_out::OwnerGroup { owners, records } in groups {
        let frames = fan_out::encode_frames_for(
            &reforwarder.mesh,
            &owners,
            batch_forward(cache_name, view.view_hash(), hops + 1, records),
        );
        for peer in owners {
            reforwarder.dispatch(peer, frames.clone());
        }
    }
}

/// The bytes a lane may hold toward one peer, its frames counted encoded and
/// decoded ([`OutFrame::resident_bytes`]): a batch is a few hundred kilobytes
/// at most, so this is a hundred batches or more queued behind a peer that
/// stopped reading. Bytes, not batches, bound the memory.
pub(super) const REFORWARD_LANE_BYTES: usize = 64 * 1024 * 1024;

/// How long [`Reforwarder::shutdown`] lets the lanes hand their batches to
/// the mesh before it abandons the ones behind a peer that does not read.
/// Matches the wait [`Mesh::shutdown`] gives the outboxes after it.
const REFORWARD_DRAIN_DEADLINE: Duration = Duration::from_millis(500);

/// The least time between two warnings about one lane dropping batches.
const REFORWARD_WARN_SPACING: Duration = Duration::from_secs(5);

/// Whether a lane holding `queued` bytes takes a batch of `batch` bytes
/// under `cap`: a batch that fits, or any batch for an empty lane, so one
/// larger than the cap is not refused forever. Pure; unit tested directly.
pub(super) const fn lane_admits(queued: usize, batch: usize, cap: usize) -> bool {
    queued == 0 || queued.saturating_add(batch) <= cap
}

/// Whether the lane toward a peer is dropped: the peer left the peer table,
/// or the worker ended. Pure; unit tested directly.
pub(super) const fn lane_retired(peer_live: bool, worker_ended: bool) -> bool {
    !peer_live || worker_ended
}

/// Whether a lane that last warned at `last_warn` warns again at `now`.
/// Pure; unit tested directly.
pub(super) fn warn_due(last_warn: Option<Instant>, now: Instant) -> bool {
    last_warn.is_none_or(|at| now.saturating_duration_since(at) >= REFORWARD_WARN_SPACING)
}

/// The bytes `frames` hold, encoded and decoded.
fn frames_bytes(frames: &[OutFrame]) -> usize {
    frames.iter().map(OutFrame::resident_bytes).sum()
}

/// One batch of frames waiting in a lane, with the bytes it counts for.
struct LaneBatch {
    frames: Vec<OutFrame>,
    bytes: usize,
}

/// The queue of re-forwarded batches toward one peer, drained in order by a
/// worker task.
struct Lane {
    tx: mpsc::UnboundedSender<LaneBatch>,
    /// Bytes of the batches queued or being sent, which the worker releases
    /// once a batch has reached the peer's outbox.
    queued: Arc<AtomicUsize>,
    /// Batches dropped since the last warning.
    dropped_unreported: u64,
    last_warn: Option<Instant>,
}

/// Sends the batches of one lane to `peer` in order, waiting for outbox
/// space as [`Mesh::send_frames_awaiting`] does: a frame for a live peer is
/// not dropped, and a peer that left the table ends the wait.
async fn reforward_lane_worker(
    mesh: Mesh,
    peer: NodeId,
    mut batches: mpsc::UnboundedReceiver<LaneBatch>,
    queued: Arc<AtomicUsize>,
) {
    while let Some(LaneBatch { frames, bytes }) = batches.recv().await {
        mesh.send_frames_awaiting(peer, frames).await;
        queued.fetch_sub(bytes, Ordering::Relaxed);
    }
}

/// Where [`inbound_loop`] sends a re-forwarded batch: a lane and worker task
/// per destination peer, so a peer that stopped reading stalls its own lane
/// and nothing else. [`Reforwarder::dispatch`] only enqueues. A lane is
/// bounded in bytes ([`REFORWARD_LANE_BYTES`]); a batch past the bound is
/// dropped, counted in `sundog_backlog_dropped_total{peer}`, the peer marked
/// dirty and a warning logged. Anti-entropy cannot repair that drop for a
/// receiver that does not own the records: its pass-through copy was the
/// only one.
pub(super) struct Reforwarder {
    mesh: Mesh,
    lanes: HashMap<NodeId, Lane>,
    workers: JoinSet<()>,
    lane_bytes: usize,
}

impl Reforwarder {
    pub(super) fn new(mesh: Mesh) -> Self {
        Self::with_lane_bytes(mesh, REFORWARD_LANE_BYTES)
    }

    /// A reforwarder whose lanes hold `lane_bytes`.
    pub(super) fn with_lane_bytes(mesh: Mesh, lane_bytes: usize) -> Self {
        Self {
            mesh,
            lanes: HashMap::new(),
            workers: JoinSet::new(),
            lane_bytes,
        }
    }

    /// Queues `frames` on the lane toward `peer`, starting the lane on first
    /// use. A `peer` not in the mesh's table gets no lane: nothing could be
    /// sent to it.
    pub(super) fn dispatch(&mut self, peer: NodeId, frames: Vec<OutFrame>) {
        if frames.is_empty() || !self.mesh.peer_is_live(peer) {
            return;
        }
        let mut batch = LaneBatch {
            bytes: frames_bytes(&frames),
            frames,
        };
        // A second attempt covers a lane whose worker ended.
        for _ in 0..2 {
            let mesh = &self.mesh;
            let workers = &mut self.workers;
            let lane = self.lanes.entry(peer).or_insert_with(|| {
                let (tx, rx) = mpsc::unbounded_channel();
                let queued = Arc::new(AtomicUsize::new(0));
                workers.spawn(reforward_lane_worker(
                    mesh.clone(),
                    peer,
                    rx,
                    Arc::clone(&queued),
                ));
                Lane {
                    tx,
                    queued,
                    dropped_unreported: 0,
                    last_warn: None,
                }
            });
            if !lane_admits(
                lane.queued.load(Ordering::Relaxed),
                batch.bytes,
                self.lane_bytes,
            ) {
                self.mesh.note_unsent(peer, batch.frames.len());
                lane.dropped_unreported += 1;
                let now = Instant::now();
                if warn_due(lane.last_warn, now) {
                    tracing::warn!(
                        %peer,
                        batches = lane.dropped_unreported,
                        "re-forward lane is full behind a peer that is not reading; batches \
                         dropped, and a record this node does not own reaches no owner through \
                         this node"
                    );
                    lane.last_warn = Some(now);
                    lane.dropped_unreported = 0;
                }
                return;
            }
            lane.queued.fetch_add(batch.bytes, Ordering::Relaxed);
            match lane.tx.send(batch) {
                Ok(()) => return,
                Err(mpsc::error::SendError(returned)) => {
                    lane.queued.fetch_sub(returned.bytes, Ordering::Relaxed);
                    batch = returned;
                    self.lanes.remove(&peer);
                }
            }
        }
        self.mesh.note_unsent(peer, batch.frames.len());
    }

    /// Drops the lane of every peer that left the mesh's table, which ends
    /// its worker once the batches queued toward the departed peer have
    /// drained, and reaps the workers that ended.
    pub(super) fn retire_departed(&mut self) {
        if !self.lanes.is_empty() {
            let mesh = &self.mesh;
            self.lanes
                .retain(|&peer, lane| !lane_retired(mesh.peer_is_live(peer), lane.tx.is_closed()));
        }
        while self.workers.try_join_next().is_some() {}
    }

    /// Closes every lane and waits up to [`REFORWARD_DRAIN_DEADLINE`] for the
    /// workers to hand the queued batches to the mesh, then abandons the
    /// rest: the batches queued behind a peer that does not read.
    pub(super) async fn shutdown(mut self) {
        self.lanes.clear();
        let workers = &mut self.workers;
        let drained = tokio::time::timeout(REFORWARD_DRAIN_DEADLINE, async {
            while workers.join_next().await.is_some() {}
        })
        .await;
        if drained.is_err() {
            tracing::debug!(
                "re-forward lanes behind a peer that is not reading abandoned at shutdown"
            );
            self.workers.abort_all();
        }
    }

    /// How many lanes are open.
    #[cfg(all(test, not(feature = "sim")))]
    pub(super) fn lane_count(&self) -> usize {
        self.lanes.len()
    }

    /// How many worker tasks have not been reaped.
    #[cfg(all(test, not(feature = "sim")))]
    pub(super) fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// The bytes queued toward `peer`.
    #[cfg(all(test, not(feature = "sim")))]
    pub(super) fn queued_bytes(&self, peer: NodeId) -> usize {
        self.lanes
            .get(&peer)
            .map_or(0, |lane| lane.queued.load(Ordering::Relaxed))
    }
}

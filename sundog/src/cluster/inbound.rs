//! The single consumer of the mesh's inbound-message channel, and the re-forward of a `ForwardBatch` routed under another view.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use smol_str::SmolStr;
use tokio::sync::mpsc;
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

/// Applies one accumulated same-cache run of `Replicate`/`ReplicateBatch`
/// records under one `apply_remote_batch` call. `shard_cache` memoizes the
/// lookup per drained batch, so a cache name costs at most one lock
/// acquisition.
async fn apply_pending_replicate(
    shards: &ShardRegistry,
    shard_cache: &mut HashMap<SmolStr, Option<Arc<dyn ShardOps>>>,
    cache: SmolStr,
    recs: Vec<WireRecord>,
) {
    let shard = shard_cache
        .entry(cache.clone())
        .or_insert_with(|| lookup_shard(shards, &cache));
    if let Some(shard) = shard {
        shard.apply_remote_batch(recs).await;
    } else {
        tracing::trace!(%cache, "replicate batch for unknown cache; dropped");
    }
}

/// Appends `recs` to the pending same-cache run, or applies the run in
/// progress first when `cache` differs and starts a new one.
async fn queue_replicate(
    shards: &ShardRegistry,
    shard_cache: &mut HashMap<SmolStr, Option<Arc<dyn ShardOps>>>,
    pending: &mut Option<(SmolStr, Vec<WireRecord>)>,
    cache: SmolStr,
    mut recs: Vec<WireRecord>,
) {
    match pending {
        Some((pending_cache, pending_recs)) if *pending_cache == cache => {
            pending_recs.append(&mut recs);
        }
        _ => {
            if let Some((old_cache, old_recs)) = pending.take() {
                apply_pending_replicate(shards, shard_cache, old_cache, old_recs).await;
            }
            *pending = Some((cache, recs));
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

/// The single consumer of `Mesh`'s inbound-message channel: dispatches
/// `Invalidate`/`Replicate`/`ReplicateBatch`/`ForwardBatch` to the named
/// shard, dropping a message for an unregistered cache with a trace event.
/// Drains a bounded batch per wake with `recv_many`, so a run of same-cache
/// messages coalesces into one `apply_remote_batch` call; a `ForwardBatch`
/// under a stale view hash is first re-forwarded to its records' current
/// owners (see [`reforward_stale_view`]), then applied like any other.
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

pub(super) async fn inbound_loop(
    shards: ShardRegistry,
    activity: Arc<InboundActivity>,
    mesh: Mesh,
    self_node: NodeId,
    mut inbound: mpsc::Receiver<InboundMsg>,
    cancel: CancellationToken,
) {
    let mut drained: Vec<InboundMsg> = Vec::with_capacity(INBOUND_DRAIN_CAP);
    loop {
        drained.clear();
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            received = inbound.recv_many(&mut drained, INBOUND_DRAIN_CAP) => {
                if received == 0 {
                    return; // channel closed, nothing left to drain
                }
            }
        }

        note_streaming_peers(&activity, &drained);

        let mut shard_cache: HashMap<SmolStr, Option<Arc<dyn ShardOps>>> = HashMap::new();
        let mut pending: Option<(SmolStr, Vec<WireRecord>)> = None;
        for InboundMsg { from, msg } in drained.drain(..) {
            match msg {
                Msg::Replicate { cache, rec } => {
                    queue_replicate(&shards, &mut shard_cache, &mut pending, cache, vec![rec])
                        .await;
                }
                Msg::ReplicateBatch { cache, recs } => {
                    queue_replicate(&shards, &mut shard_cache, &mut pending, cache, recs).await;
                }
                Msg::ForwardBatch {
                    cache,
                    view_hash,
                    hops,
                    recs,
                } => {
                    if let Some(shard) = burst_shard(&mut shard_cache, &shards, &cache) {
                        reforward_stale_view(
                            &mesh,
                            ReforwardBatch {
                                self_node,
                                from,
                                shard: shard.as_ref(),
                                cache_name: &cache,
                                sender_view_hash: view_hash,
                                hops,
                                records: &recs,
                            },
                        )
                        .await;
                    }
                    queue_replicate(&shards, &mut shard_cache, &mut pending, cache, recs).await;
                }
                Msg::Invalidate { cache, key, ver } => {
                    if let Some((old_cache, old_recs)) = pending.take() {
                        apply_pending_replicate(&shards, &mut shard_cache, old_cache, old_recs)
                            .await;
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
        if let Some((cache, recs)) = pending.take() {
            apply_pending_replicate(&shards, &mut shard_cache, cache, recs).await;
        }
    }
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
    let groups: Vec<fan_out::OwnerGroup> =
        fan_out::group_by_owner_set(view, self_node, records.to_vec())
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

/// Re-forwards a `ForwardBatch` the sender routed under a stale or merely
/// different ownership view to the records' owners under this node's
/// view, so a write whose target set the writer got wrong still reaches
/// every current owner: a tombstone from a writer that has not yet seen a
/// replacement owner join lands on that owner too, instead of waiting for
/// an anti-entropy round to pair the two, and a record for a bucket this
/// node does not own at all travels on instead of being dropped by the
/// inbound guard. Sent as a `hops + 1` batch stamped with this node's own
/// view hash. A no-op for a shard without an ownership view (every mode
/// but `Mode::Distributed`).
/// A `ForwardBatch` re-forwarded to its records' true owners under this
/// node's ownership view, gathered here so [`reforward_stale_view`] takes
/// one argument beyond the mesh it sends through.
struct ReforwardBatch<'a> {
    self_node: NodeId,
    from: NodeId,
    shard: &'a dyn ShardOps,
    cache_name: &'a SmolStr,
    sender_view_hash: u64,
    hops: u8,
    records: &'a [WireRecord],
}

async fn reforward_stale_view(mesh: &Mesh, batch: ReforwardBatch<'_>) {
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
        let frames: Vec<OutFrame> = batch_forward(cache_name, view.view_hash(), hops + 1, records)
            .into_iter()
            .filter_map(|msg| match OutFrame::new(msg) {
                Ok(frame) => Some(frame),
                Err(error) => {
                    tracing::warn!(%error, "failed to encode outbound message; dropped");
                    None
                }
            })
            .collect();
        for peer in owners {
            mesh.send_frames_awaiting(peer, frames.clone()).await;
        }
    }
}

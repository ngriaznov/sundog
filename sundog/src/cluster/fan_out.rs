//! Fans a cache's locally applied and forwarded writes out over the mesh,
//! per [`Mode`]: broadcast for `Invalidation`/`Replicated`, grouped by exact
//! target-peer set for `Distributed`. `Shard` holds no handle to `net::Mesh`,
//! so this is the one place a write's mode decides who hears about it.

use std::collections::HashSet;
use std::hash::Hash;
use std::sync::Arc;

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;
use smol_str::SmolStr;
use tokio_util::sync::CancellationToken;

use super::{Cluster, group_in_order};
use crate::net::{MsgClass, OutFrame, batch_forward, batch_replicate};
use crate::node::NodeId;
use crate::ownership::OwnershipView;
use crate::store::{FanOutItem, FanOutQueue, Mode, PartId, Shard, ShardOps};
use crate::wire::{Msg, WireRecord};

/// Drains one opened cache's queue of locally written keys and fans them out
/// over the mesh per [`Mode`]; `Shard` holds no handle to `net::Mesh`. A
/// `get_or_load` read-through fill fans out too, letting other
/// `Replicated`-mode peers skip their own loader call.
///
/// Each iteration takes the whole backlog at once, so a burst of writes
/// costs one round of per-peer sends, not one per write. See [`FanOutQueue`]
/// for why nothing drops for arriving too fast.
pub(crate) async fn fan_out_task<K, V>(
    shard: Arc<Shard<K, V>>,
    cluster: Cluster,
    queue: Arc<FanOutQueue<FanOutItem<K>>>,
    cache_name: SmolStr,
    mode: Mode,
    cancel: CancellationToken,
) where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    loop {
        // A batch in flight is never raced against `cancel`: a write
        // already acknowledged to the caller goes out, or is forwarded,
        // before this task ends. Cancellation is observed between batches,
        // and one last drain covers what arrived after the final wake-up.
        let items = tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            () = queue.wait_nonempty() => queue.drain(),
        };
        fan_out_batch(&shard, &cluster, &cache_name, mode, items).await;
    }
    let items = queue.drain();
    if !items.is_empty() {
        fan_out_batch(&shard, &cluster, &cache_name, mode, items).await;
    }
}

/// One [`group_by_owner_set`] group: `records`, all sharing `owners` as
/// their bucket's exact target-peer set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct OwnerGroup {
    pub(super) owners: Vec<NodeId>,
    pub(super) records: Vec<WireRecord>,
}

/// Groups `records` by the exact target-peer set each replicates to: its
/// part's live owners under `view`, self excluded. Pure and mesh-free, so
/// [`fan_out_by_owner_set`] and its unit tests build on it directly.
/// Records whose parts share an owner set land in the same group, so
/// replicating them costs one round trip per group, not one per record.
pub(super) fn group_by_owner_set(
    view: &OwnershipView,
    self_node: NodeId,
    records: Vec<WireRecord>,
) -> Vec<OwnerGroup> {
    group_in_order(records.into_iter().map(|rec| {
        let mut owners: Vec<NodeId> = view
            .owners_of(PartId::of_key(&rec.key))
            .iter()
            .copied()
            .filter(|&n| n != self_node)
            .collect();
        owners.sort_unstable();
        (owners, [rec])
    }))
    .into_iter()
    .map(|(owners, records)| OwnerGroup { owners, records })
    .collect()
}

/// Encodes each of `msgs` into an [`OutFrame`], dropping (and logging) any
/// that fails to encode: the one fan-out encoding policy every send site
/// in this module shares.
fn encode_frames(msgs: Vec<Msg>) -> Vec<OutFrame> {
    msgs.into_iter()
        .filter_map(|msg| match OutFrame::new(msg) {
            Ok(frame) => Some(frame),
            Err(error) => {
                tracing::warn!(%error, "failed to encode outbound message; dropped");
                None
            }
        })
        .collect()
}

/// Groups `records` by the exact target-peer set each replicates to: its
/// bucket's live owners under `view`, self excluded. Then sends each
/// group as `ForwardBatch` frames stamped with `view`'s hash through the
/// mesh's existing per-peer outboxes ([`Mesh::send_frames_awaiting`],
/// waiting for space rather than dropping on overflow, since a forwarded
/// write's only copy is the frame -- indefinitely, in
/// [`crate::net::FAN_OUT_SEND_DEADLINE`] slices, so long as the target peer
/// stays live).
/// The single function both an owner's normal fan-out and a non-owner's
/// forwarded writes route through: "group by owner set instead of
/// broadcast" has one implementation, not two.
async fn fan_out_by_owner_set(
    mesh: &crate::net::Mesh,
    cache_name: &SmolStr,
    view: &OwnershipView,
    self_node: NodeId,
    records: Vec<WireRecord>,
) {
    for OwnerGroup { owners, records } in group_by_owner_set(view, self_node, records) {
        if owners.is_empty() {
            continue;
        }
        let frames = encode_frames(batch_forward(cache_name, view.view_hash(), 0, records));
        for peer in owners {
            mesh.send_frames_awaiting(peer, frames.clone()).await;
        }
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "one branch per Mode, each with its own send path"
)]
async fn fan_out_batch<K, V>(
    shard: &Shard<K, V>,
    cluster: &Cluster,
    cache_name: &SmolStr,
    mode: Mode,
    notified: Vec<FanOutItem<K>>,
) where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    // The queue carries local writes (every mode) and, under
    // `Mode::Distributed`, forwarded non-owner writes. This only dedups the
    // drained burst's applied keys; a forwarded record is used as-is, with
    // nothing to re-fetch.
    let mut seen: HashSet<K> = HashSet::new();
    let mut applied_keys: Vec<K> = Vec::new();
    let mut forwarded: Vec<WireRecord> = Vec::new();
    for item in notified {
        match item {
            FanOutItem::Applied(key) => {
                if seen.insert(key.clone()) {
                    applied_keys.push(key);
                }
            }
            FanOutItem::Forward(rec) => forwarded.push(rec),
        }
    }
    if applied_keys.is_empty() && forwarded.is_empty() {
        return;
    }

    // Re-fetches applied keys through `Shard::records_for_typed` rather
    // than carrying the `Hlc`/wire bytes on `Event` itself. A missing key
    // on re-fetch means a later write or GC already covers it, so nothing
    // stale needs fanning out. A forwarded record never applies locally,
    // so it travels exactly as built.
    let mut records = shard.records_for_typed(&applied_keys).await;
    let applied_keys_bytes: HashSet<Bytes> = records.iter().map(|rec| rec.key.clone()).collect();
    records.extend(forwarded);
    if records.is_empty() {
        return;
    }

    if let Mode::Distributed { .. } = mode {
        let Some(view) = shard.ownership_view() else {
            return;
        };
        // A write forwarded before this node owned its part, but owned by
        // the time the queue drains, lands here too: sending it only to
        // the other owners (or nobody, if this node is the sole owner)
        // would lose the one copy that exists.
        let mine: Vec<WireRecord> = records
            .iter()
            .filter(|rec| {
                view.owns(PartId::of_key(rec.key.as_ref()))
                    && !applied_keys_bytes.contains(rec.key.as_ref())
            })
            .cloned()
            .collect();
        if !mine.is_empty() {
            shard.apply_remote_batch(mine).await;
        }
        fan_out_by_owner_set(
            cluster.mesh(),
            cache_name,
            &view,
            cluster.node_id(),
            records,
        )
        .await;
        return;
    }

    let peers = cluster.live_peer_ids();
    match mode {
        // Distributed is handled, and returned, above.
        Mode::Local | Mode::Distributed { .. } => (),
        Mode::Invalidation => {
            // No value ever rides an invalidation, so there is nothing a
            // low-protocol peer could misdecode: every peer gets the same
            // frames, exactly as before.
            let frames = encode_frames(
                records
                    .into_iter()
                    .map(|rec| Msg::Invalidate {
                        cache: cache_name.clone(),
                        key: rec.key,
                        ver: rec.ver,
                    })
                    .collect(),
            );
            for &peer in &peers {
                cluster
                    .mesh()
                    .send_frames(peer, MsgClass::Invalidate, frames.iter().cloned());
            }
        }
        // Pre-batched by the same budget/count rules `net::conn`'s writer
        // uses for coalescing, so a drained burst can't flood the outbox
        // into drop-newest. The writer-side coalescer still catches trickle
        // writes arriving one drained event at a time.
        Mode::Replicated => {
            let frames = encode_frames(batch_replicate(cache_name, records));
            for &peer in &peers {
                cluster
                    .mesh()
                    .send_frames(peer, MsgClass::Replicate, frames.iter().cloned());
            }
        }
    }
}

// The grouping logic is pure and mesh-free; this is `fan_out_by_owner_set`'s
// own test.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::Hlc;
    use crate::ownership::OwnershipView;

    /// Mirrors `store::bucket_of`'s formula so a test can compute which
    /// anti-entropy bucket a key lands in without that private function.
    fn bucket_of_u32(key: u32) -> u16 {
        let bytes = postcard::to_stdvec(&key).expect("a u32 key always postcard-encodes");
        let bucket = xxhash_rust::xxh3::xxh3_64(&bytes) & (crate::store::BUCKET_COUNT as u64 - 1);
        u16::try_from(bucket).expect("masked to BUCKET_COUNT - 1, always fits in u16")
    }

    /// A `WireRecord` for `key`, its own postcard-encoded bytes as `key`.
    fn wire_record_for_u32(key: u32) -> WireRecord {
        WireRecord {
            key: postcard::to_stdvec(&key)
                .expect("a u32 key always postcard-encodes")
                .into(),
            value: Some(Bytes::from_static(b"\x01v")),
            ver: Hlc {
                wall_ms: 1,
                logical: 0,
                node: NodeId::from(1),
            },
            expires_at_ms: None,
        }
    }

    /// `group_by_owner_set`'s grouping is a pure function of an
    /// `OwnershipView` and a record list, with no `Mesh` or live cluster
    /// involved: this is `fan_out_by_owner_set`'s own test, and the
    /// pinned guarantee that an owner's write fans out only to the
    /// bucket's other owners, never to every live peer.
    #[test]
    fn insert_on_an_owner_fans_out_only_to_the_bucket_other_owners_not_every_live_peer() {
        let self_node = NodeId::from(1);
        let eligible: Vec<NodeId> = (1..=5u64).map(NodeId::from).collect();
        let k = std::num::NonZeroU8::new(2).expect("nonzero");
        let view = OwnershipView::compute(self_node, eligible.clone(), k);

        // Two keys in different buckets, so their owner sets can differ.
        let key_a = 0u32;
        let key_b = (1u32..100_000)
            .find(|&k| bucket_of_u32(k) != bucket_of_u32(key_a))
            .expect("a second, distinct bucket is found quickly");

        let groups = group_by_owner_set(
            &view,
            self_node,
            vec![wire_record_for_u32(key_a), wire_record_for_u32(key_b)],
        );

        let mut total = 0usize;
        for OwnerGroup { owners, records } in &groups {
            assert!(
                !owners.contains(&self_node),
                "self is never its own fan-out target"
            );
            assert!(
                owners.len() < eligible.len(),
                "a group's peer set is never every eligible/live node, only a bucket's other \
                 owners: {owners:?}"
            );
            for rec in records {
                let part = PartId::of_key(&rec.key);
                let mut expected: Vec<NodeId> = view
                    .owners_of(part)
                    .iter()
                    .copied()
                    .filter(|&n| n != self_node)
                    .collect();
                expected.sort_unstable();
                assert_eq!(
                    owners, &expected,
                    "a record's group is exactly its bucket's live owners minus self"
                );
            }
            total += records.len();
        }
        assert_eq!(total, 2, "every record lands in exactly one group");
    }

    #[test]
    fn group_by_owner_set_coalesces_records_sharing_the_same_owner_set() {
        let self_node = NodeId::from(1);
        // A single eligible peer besides self, so every bucket's owner set
        // minus self is either empty (self is sole owner) or exactly that
        // one peer: every non-empty group is the same peer set.
        let eligible = vec![self_node, NodeId::from(2)];
        let k = std::num::NonZeroU8::new(1).expect("nonzero");
        let view = OwnershipView::compute(self_node, eligible, k);

        let records: Vec<WireRecord> = (0u32..50).map(wire_record_for_u32).collect();
        let groups = group_by_owner_set(&view, self_node, records.clone());

        let non_empty_groups: Vec<_> = groups.iter().filter(|g| !g.owners.is_empty()).collect();
        assert!(
            non_empty_groups.len() <= 1,
            "with only one other eligible node, every non-empty group is the same one peer set: \
             {non_empty_groups:?}"
        );
        let total: usize = groups.iter().map(|g| g.records.len()).sum();
        assert_eq!(
            total,
            records.len(),
            "no record is lost or duplicated by grouping"
        );
    }
}

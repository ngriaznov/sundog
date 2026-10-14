//! Fans a cache's locally applied and forwarded writes out over the mesh,
//! per [`Mode`]: broadcast for `Invalidation`/`Replicated`, grouped by exact
//! target-peer set for `Distributed`. `Shard` holds no handle to `net::Mesh`,
//! so this is the one place a write's mode decides who hears about it.
//!
//! The queue holds keys and the version each was written at. The drain reads
//! the written view back: a write still stored travels as a record, a lapsed
//! one with its past deadline, and a write whose entry is gone travels as an
//! invalidation of the version written, so a peer never keeps serving the
//! value the write replaced.

use std::collections::HashSet;
use std::hash::Hash;
use std::sync::Arc;

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;
use smol_str::SmolStr;
use tokio_util::sync::CancellationToken;

use super::{Cluster, group_in_order};
use crate::hlc::Hlc;
use crate::net::{Mesh, MsgClass, OutFrame, batch_forward, batch_replicate};
use crate::node::NodeId;
use crate::ownership::OwnershipView;
use crate::store::{
    FanOutItem, FanOutQueue, FanOutRecords, Mode, PartId, Shard, ShardOps, newest_per_key,
};
use crate::wire::{self, Msg, WireRecord};

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

/// Whether a `Mode::Invalidation` write travels to a peer speaking
/// `peer_protocol` as the writer's merged record instead of a bare
/// [`Msg::Invalidate`]: the cache's resolver merges, and the peer speaks
/// [`wire::PROTOCOL_INVALIDATION_FOLD`], so it folds the record into a key it
/// holds and ignores it otherwise. Any other peer, and any cache whose
/// resolver does not merge, gets the invalidation. Pure; unit tested
/// directly.
pub(super) const fn invalidation_ships_values(merges: bool, peer_protocol: u16) -> bool {
    merges && wire::peer_supports(peer_protocol, wire::PROTOCOL_INVALIDATION_FOLD)
}

/// One [`Msg::Invalidate`] per record in `records`.
fn invalidations(cache_name: &SmolStr, records: Vec<WireRecord>) -> Vec<Msg> {
    records
        .into_iter()
        .map(|rec| Msg::Invalidate {
            cache: cache_name.clone(),
            key: rec.key,
            ver: rec.ver,
        })
        .collect()
}

/// What a peer that folds receives for `records`: each live record as a
/// replicate frame, batched, and each tombstone as an invalidation. A live
/// record whose frame would pass [`wire::MAX_FRAME`] cannot encode, and an
/// `Invalidation` cache runs no anti-entropy to repair a record that never
/// arrives, so it travels as an invalidation too and the peer drops its stale
/// copy. Pure; unit tested directly.
fn fold_msgs(cache_name: &SmolStr, records: Vec<WireRecord>) -> Vec<Msg> {
    let (shipped, invalidated): (Vec<_>, Vec<_>) = records.into_iter().partition(|rec| {
        !rec.is_tombstone()
            && wire::replicate_frame_len(
                cache_name.len(),
                rec.key.len(),
                rec.value.as_ref().map_or(0, Bytes::len),
            ) <= wire::MAX_FRAME
    });
    let mut msgs = batch_replicate(cache_name, shipped);
    msgs.extend(invalidations(cache_name, invalidated));
    msgs
}

/// The peers that receive one set of messages from a `Mode::Invalidation`
/// write.
#[derive(Debug, Clone, PartialEq, Eq)]
struct InvalidationSend {
    peers: Vec<NodeId>,
    msgs: Vec<Msg>,
}

/// What a `Mode::Invalidation` write of `records` sends each of `peers`, each
/// a node with the protocol it speaks: the sends, one per distinct message
/// set, in the order folding peers then older peers. A peer for which
/// [`invalidation_ships_values`] holds receives [`fold_msgs`], every other
/// peer [`invalidations`]; a set with no peer is left out. Pure; unit tested
/// directly.
fn invalidation_sends(
    merges: bool,
    peers: &[(NodeId, u16)],
    cache_name: &SmolStr,
    records: Vec<WireRecord>,
) -> Vec<InvalidationSend> {
    let (folding, dropping): (Vec<_>, Vec<_>) = peers
        .iter()
        .partition(|(_, protocol)| invalidation_ships_values(merges, *protocol));
    let nodes = |group: Vec<&(NodeId, u16)>| group.into_iter().map(|&(node, _)| node).collect();
    let mut sends = Vec::new();
    if !folding.is_empty() {
        sends.push(InvalidationSend {
            peers: nodes(folding),
            msgs: fold_msgs(cache_name, records.clone()),
        });
    }
    if !dropping.is_empty() {
        sends.push(InvalidationSend {
            peers: nodes(dropping),
            msgs: invalidations(cache_name, records),
        });
    }
    sends
}

/// Encodes each of `msgs` into an [`OutFrame`], dropping (and logging) any
/// that fails to encode, and answers how many did: the one fan-out encoding
/// policy every send site in this module shares.
fn encode_frames(msgs: Vec<Msg>) -> (Vec<OutFrame>, usize) {
    let mut unencoded = 0;
    let frames = msgs
        .into_iter()
        .filter_map(|msg| match OutFrame::new(msg) {
            Ok(frame) => Some(frame),
            Err(error) => {
                tracing::warn!(%error, "failed to encode outbound message; dropped");
                unencoded += 1;
                None
            }
        })
        .collect();
    (frames, unencoded)
}

/// [`encode_frames`] for a send to `peers`: a message that does not encode
/// reaches none of them, so each is marked dirty and the drop counted
/// ([`Mesh::note_unsent`]), and anti-entropy covers the write where it can.
pub(super) fn encode_frames_for(mesh: &Mesh, peers: &[NodeId], msgs: Vec<Msg>) -> Vec<OutFrame> {
    let (frames, unencoded) = encode_frames(msgs);
    for &peer in peers {
        mesh.note_unsent(peer, unencoded);
    }
    frames
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
    mesh: &Mesh,
    cache_name: &SmolStr,
    view: &OwnershipView,
    self_node: NodeId,
    records: Vec<WireRecord>,
) {
    for OwnerGroup { owners, records } in group_by_owner_set(view, self_node, records) {
        if owners.is_empty() {
            continue;
        }
        let frames = encode_frames_for(
            mesh,
            &owners,
            batch_forward(cache_name, view.view_hash(), 0, records),
        );
        for peer in owners {
            mesh.send_frames_awaiting(peer, frames.clone()).await;
        }
    }
}

/// How a departed write's invalidation is sent to a peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DepartedClass {
    /// [`MsgClass::Invalidate`]: an overflowing outbox drops the oldest
    /// frame. For `Invalidation` and `Replicated`, where a lost invalidation
    /// is the same loss any invalidation has.
    Drop,
    /// [`Mesh::send_frames_awaiting`]: waits for outbox space, so a
    /// co-owner of a part never keeps the older value that anti-entropy
    /// would push back onto this node.
    Awaiting,
}

/// The peers that receive one set of departed writes, and how.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DepartedSend {
    targets: Vec<NodeId>,
    class: DepartedClass,
    records: Vec<WireRecord>,
}

/// Who hears of each departed write in `mode`: every peer in `peers` for
/// `Invalidation` and `Replicated`, sent drop-oldest; the other owners of a
/// write's part under `view` for `Distributed`, sent awaiting, with a part
/// this node solely owns left out. `Local` sends nothing, and `Distributed`
/// without a view has no owners to name. A `merges` resolver sends nothing in
/// any mode: a peer's merged copy is the best one left, and a bare
/// invalidation cannot say which contributions the writer's gone copy
/// lacked. Pure; unit tested directly.
fn plan_departed_sends(
    mode: Mode,
    peers: &[NodeId],
    view: Option<&OwnershipView>,
    self_node: NodeId,
    merges: bool,
    departed: Vec<WireRecord>,
) -> Vec<DepartedSend> {
    if departed.is_empty() || merges {
        return Vec::new();
    }
    match mode {
        Mode::Local => Vec::new(),
        Mode::Invalidation | Mode::Replicated => {
            if peers.is_empty() {
                return Vec::new();
            }
            vec![DepartedSend {
                targets: peers.to_vec(),
                class: DepartedClass::Drop,
                records: departed,
            }]
        }
        Mode::Distributed { .. } => view.map_or_else(Vec::new, |view| {
            group_by_owner_set(view, self_node, departed)
                .into_iter()
                .filter(|group| !group.owners.is_empty())
                .map(|OwnerGroup { owners, records }| DepartedSend {
                    targets: owners,
                    class: DepartedClass::Awaiting,
                    records,
                })
                .collect()
        }),
    }
}

/// Sends each of `plan`'s departed writes as [`Msg::Invalidate`].
async fn send_departed(cluster: &Cluster, cache_name: &SmolStr, plan: Vec<DepartedSend>) {
    for DepartedSend {
        targets,
        class,
        records,
    } in plan
    {
        let frames =
            encode_frames_for(cluster.mesh(), &targets, invalidations(cache_name, records));
        for peer in targets {
            match class {
                DepartedClass::Drop => {
                    cluster
                        .mesh()
                        .send_frames(peer, MsgClass::Invalidate, frames.iter().cloned());
                }
                DepartedClass::Awaiting => {
                    cluster
                        .mesh()
                        .send_frames_awaiting(peer, frames.clone())
                        .await;
                }
            }
        }
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "one branch per Mode, each with its own send path"
)]
pub(crate) async fn fan_out_batch<K, V>(
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
    // `Mode::Distributed`, forwarded non-owner writes. A burst that wrote a
    // key twice fans out its newest write once; a forwarded record is used
    // as-is, with nothing to re-fetch.
    let mut applied: Vec<(K, Hlc)> = Vec::new();
    let mut forwarded: Vec<WireRecord> = Vec::new();
    for item in notified {
        match item {
            FanOutItem::Applied(key, ver) => applied.push((key, ver)),
            FanOutItem::Forward(rec) => forwarded.push(rec),
        }
    }
    let applied = newest_per_key(applied);
    if applied.is_empty() && forwarded.is_empty() {
        return;
    }

    // Reads the written view back rather than carrying wire bytes on the
    // queue. A write with no entry left at its version (evicted, swept,
    // disowned, idle, or unreadable on disk) is departed: no later write
    // and no anti-entropy round covers it, so its version is sent to the
    // peers as an invalidation, after the records. A forwarded record never
    // applies locally, so it travels exactly as built.
    let FanOutRecords {
        mut records,
        departed,
    } = shard.fan_out_records(&applied).await;
    let applied_keys_bytes: HashSet<Bytes> = records.iter().map(|rec| rec.key.clone()).collect();
    records.extend(forwarded);
    if records.is_empty() && departed.is_empty() {
        return;
    }

    if let Mode::Distributed { .. } = mode {
        let Some(view) = shard.ownership_view() else {
            if !departed.is_empty() {
                tracing::debug!(
                    cache = %cache_name,
                    departed = departed.len(),
                    "no ownership view; departed writes not invalidated"
                );
            }
            return;
        };
        // A write forwarded before this node owned its part, but owned by
        // the time the queue drains, lands here too: sending it only to
        // the other owners (or nobody, if this node is the sole owner)
        // would lose the one copy that exists. It applies as a forward, so a
        // stamp this node's clock refuses is held rather than dropped.
        let mine: Vec<WireRecord> = records
            .iter()
            .filter(|rec| {
                view.owns(PartId::of_key(rec.key.as_ref()))
                    && !applied_keys_bytes.contains(rec.key.as_ref())
            })
            .cloned()
            .collect();
        if !mine.is_empty() {
            shard.apply_forwarded_batch(mine).await;
        }
        fan_out_by_owner_set(
            cluster.mesh(),
            cache_name,
            &view,
            cluster.node_id(),
            records,
        )
        .await;
        let plan = plan_departed_sends(
            mode,
            &[],
            Some(&view),
            cluster.node_id(),
            shard.merges(),
            departed,
        );
        send_departed(cluster, cache_name, plan).await;
        return;
    }

    match mode {
        // Distributed is handled, and returned, above.
        Mode::Local | Mode::Distributed { .. } => (),
        Mode::Invalidation => {
            // A peer that folds gets each live record as a replicate frame
            // and each tombstone as an invalidation; every other peer
            // gets an invalidation per record, which carries no value for a
            // low-protocol peer to misdecode.
            let peers: Vec<(NodeId, u16)> = cluster
                .peers()
                .into_iter()
                .map(|peer| (peer.node, peer.protocol))
                .collect();
            for InvalidationSend { peers, msgs } in
                invalidation_sends(shard.merges(), &peers, cache_name, records)
            {
                let frames = encode_frames_for(cluster.mesh(), &peers, msgs);
                for peer in peers {
                    cluster
                        .mesh()
                        .send_frames(peer, MsgClass::Invalidate, frames.iter().cloned());
                }
            }
        }
        // Pre-batched by the same budget/count rules `net::conn`'s writer
        // uses for coalescing, so a drained burst can't flood the outbox
        // into drop-newest. The writer-side coalescer still catches trickle
        // writes arriving one drained event at a time.
        Mode::Replicated => {
            let live = cluster.live_peer_ids();
            let frames =
                encode_frames_for(cluster.mesh(), &live, batch_replicate(cache_name, records));
            for peer in live {
                cluster
                    .mesh()
                    .send_frames(peer, MsgClass::Replicate, frames.iter().cloned());
            }
        }
    }
    let peers = cluster.live_peer_ids();
    let plan = plan_departed_sends(
        mode,
        &peers,
        None,
        cluster.node_id(),
        shard.merges(),
        departed,
    );
    send_departed(cluster, cache_name, plan).await;
}

// The grouping logic is pure and mesh-free; this is `fan_out_by_owner_set`'s
// own test.
#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use super::*;
    use crate::hlc::Hlc;
    use crate::ownership::OwnershipView;
    use crate::store::crdt::{PnCounter, PnCounterResolver, WriterId};
    use crate::store::encode_key;

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

    #[test]
    fn a_merging_cache_ships_values_only_to_a_peer_that_folds_them() {
        assert!(invalidation_ships_values(
            true,
            wire::PROTOCOL_INVALIDATION_FOLD
        ));
        assert!(invalidation_ships_values(true, wire::PROTOCOL_VERSION));
        assert!(
            !invalidation_ships_values(true, wire::PROTOCOL_INVALIDATION_FOLD - 1),
            "a peer older than the fold protocol gets an invalidation"
        );
        assert!(!invalidation_ships_values(true, wire::MIN_PROTOCOL_VERSION));
        assert!(!invalidation_ships_values(true, 0));
    }

    #[test]
    fn a_cache_whose_resolver_does_not_merge_ships_no_value_to_any_peer() {
        for protocol in [
            0,
            wire::MIN_PROTOCOL_VERSION,
            wire::PROTOCOL_INVALIDATION_FOLD - 1,
            wire::PROTOCOL_INVALIDATION_FOLD,
            wire::PROTOCOL_VERSION,
            wire::PROTOCOL_VERSION + 1,
        ] {
            assert!(
                !invalidation_ships_values(false, protocol),
                "protocol {protocol}"
            );
        }
    }

    #[test]
    fn a_peer_newer_than_this_build_still_folds() {
        assert!(invalidation_ships_values(true, wire::PROTOCOL_VERSION + 1));
    }

    #[test]
    fn a_folding_peer_gets_live_records_as_replicates_and_tombstones_as_invalidations() {
        let live_one = wire_record_for_u32(1);
        let live_two = wire_record_for_u32(2);
        let removed = WireRecord {
            value: None,
            ..wire_record_for_u32(3)
        };
        let cache = SmolStr::new("c");

        let msgs = fold_msgs(
            &cache,
            vec![live_one.clone(), removed.clone(), live_two.clone()],
        );

        assert_eq!(
            msgs,
            vec![
                Msg::ReplicateBatch {
                    cache: cache.clone(),
                    recs: vec![live_one, live_two],
                },
                Msg::Invalidate {
                    cache,
                    key: removed.key,
                    ver: removed.ver,
                },
            ]
        );
    }

    #[test]
    fn a_lone_live_record_travels_as_a_plain_replicate() {
        let rec = wire_record_for_u32(1);
        let cache = SmolStr::new("c");
        assert_eq!(
            fold_msgs(&cache, vec![rec.clone()]),
            vec![Msg::Replicate { cache, rec }]
        );
    }

    /// A record whose frame would pass `MAX_FRAME` cannot encode, and no
    /// anti-entropy repairs an `Invalidation` cache, so it travels as an
    /// invalidation that drops the holder's stale copy.
    #[test]
    fn a_record_too_large_for_a_frame_travels_as_an_invalidation() {
        let cache = SmolStr::new("c");
        let sized = |key: u32, over: usize| {
            let rec = wire_record_for_u32(key);
            let base = wire::replicate_frame_len(cache.len(), rec.key.len(), 0);
            WireRecord {
                value: Some(Bytes::from(vec![0u8; wire::MAX_FRAME - base + over])),
                ..rec
            }
        };
        let fits = sized(1, 0);
        let too_large = sized(2, 1);

        let msgs = fold_msgs(&cache, vec![fits.clone(), too_large.clone()]);

        assert_eq!(
            msgs,
            vec![
                Msg::Replicate {
                    cache: cache.clone(),
                    rec: fits,
                },
                Msg::Invalidate {
                    cache,
                    key: too_large.key,
                    ver: too_large.ver,
                },
            ]
        );
        assert_eq!(
            encode_frames(msgs.clone()).0.len(),
            msgs.len(),
            "every message encodes, so none is dropped"
        );
    }

    fn sends_for(
        merges: bool,
        peers: &[(u64, u16)],
        records: Vec<WireRecord>,
    ) -> Vec<InvalidationSend> {
        let peers: Vec<(NodeId, u16)> = peers
            .iter()
            .map(|&(node, protocol)| (NodeId::from(node), protocol))
            .collect();
        invalidation_sends(merges, &peers, &SmolStr::new("c"), records)
    }

    #[test]
    fn a_merging_cache_sends_an_older_peer_invalidations_and_a_current_peer_its_records() {
        let live = wire_record_for_u32(1);
        let removed = WireRecord {
            value: None,
            ..wire_record_for_u32(2)
        };
        let records = vec![live.clone(), removed.clone()];
        let cache = SmolStr::new("c");
        let old = wire::PROTOCOL_INVALIDATION_FOLD - 1;
        let current = wire::PROTOCOL_INVALIDATION_FOLD;

        let sends = sends_for(true, &[(1, old), (2, current), (3, old)], records.clone());

        assert_eq!(
            sends,
            vec![
                InvalidationSend {
                    peers: vec![NodeId::from(2)],
                    msgs: fold_msgs(&cache, records.clone()),
                },
                InvalidationSend {
                    peers: vec![NodeId::from(1), NodeId::from(3)],
                    msgs: invalidations(&cache, records),
                },
            ]
        );
        assert!(
            matches!(&sends[0].msgs[0], Msg::Replicate { rec, .. } if *rec == live),
            "the current peer receives the live record's value"
        );
        assert!(
            sends[1]
                .msgs
                .iter()
                .all(|msg| matches!(msg, Msg::Invalidate { .. })),
            "the older peer receives no value, a live record included"
        );
        assert!(matches!(&sends[1].msgs[1], Msg::Invalidate { key, .. } if *key == removed.key));
    }

    #[test]
    fn a_cache_whose_resolver_does_not_merge_sends_every_peer_invalidations() {
        let records: Vec<WireRecord> = (0u32..2).map(wire_record_for_u32).collect();
        let cache = SmolStr::new("c");
        let sends = sends_for(
            false,
            &[
                (1, wire::PROTOCOL_INVALIDATION_FOLD - 1),
                (2, wire::PROTOCOL_VERSION),
            ],
            records.clone(),
        );
        assert_eq!(
            sends,
            vec![InvalidationSend {
                peers: vec![NodeId::from(1), NodeId::from(2)],
                msgs: invalidations(&cache, records),
            }]
        );
    }

    #[test]
    fn a_send_set_with_no_peer_is_left_out() {
        let records = vec![wire_record_for_u32(1)];
        assert_eq!(sends_for(true, &[], records.clone()), Vec::new());
        let only_current = sends_for(true, &[(1, wire::PROTOCOL_VERSION)], records);
        assert_eq!(only_current.len(), 1);
        assert_eq!(only_current[0].peers, vec![NodeId::from(1)]);
    }

    #[test]
    fn invalidations_name_each_records_key_and_version() {
        let recs: Vec<WireRecord> = (0u32..3).map(wire_record_for_u32).collect();
        let msgs = invalidations(&SmolStr::new("c"), recs.clone());
        assert_eq!(msgs.len(), recs.len());
        for (msg, rec) in msgs.iter().zip(&recs) {
            assert_eq!(
                msg,
                &Msg::Invalidate {
                    cache: SmolStr::new("c"),
                    key: rec.key.clone(),
                    ver: rec.ver,
                }
            );
        }
    }

    fn departed_for(keys: impl IntoIterator<Item = u32>) -> Vec<WireRecord> {
        keys.into_iter()
            .map(|key| WireRecord {
                value: None,
                ..wire_record_for_u32(key)
            })
            .collect()
    }

    fn nodes(ids: &[u64]) -> Vec<NodeId> {
        ids.iter().copied().map(NodeId::from).collect()
    }

    #[test]
    fn departed_frames_encode_one_invalidate_per_departed_record() {
        let departed = departed_for(0..3);
        let msgs = invalidations(&SmolStr::new("c"), departed.clone());
        let frames = encode_frames(msgs.clone()).0;
        assert_eq!(frames.len(), departed.len(), "every departed write encodes");
        for (msg, rec) in msgs.iter().zip(&departed) {
            assert!(matches!(
                msg,
                Msg::Invalidate { key, ver, .. } if *key == rec.key && *ver == rec.ver
            ));
        }
    }

    #[test]
    fn an_invalidation_or_replicated_cache_invalidates_every_peer_drop_oldest() {
        let departed = departed_for(0..2);
        for mode in [Mode::Invalidation, Mode::Replicated] {
            assert_eq!(
                plan_departed_sends(
                    mode,
                    &nodes(&[2, 3]),
                    None,
                    NodeId::from(1),
                    false,
                    departed.clone()
                ),
                vec![DepartedSend {
                    targets: nodes(&[2, 3]),
                    class: DepartedClass::Drop,
                    records: departed.clone(),
                }],
                "{mode:?}"
            );
        }
    }

    #[test]
    fn nothing_departed_or_nobody_to_tell_plans_no_send() {
        for mode in [Mode::Invalidation, Mode::Replicated, Mode::Local] {
            assert!(
                plan_departed_sends(mode, &nodes(&[2]), None, NodeId::from(1), false, Vec::new())
                    .is_empty(),
                "{mode:?}: no departed write"
            );
            assert!(
                plan_departed_sends(mode, &[], None, NodeId::from(1), false, departed_for(0..1))
                    .is_empty(),
                "{mode:?}: no peer"
            );
        }
        assert!(
            plan_departed_sends(
                Mode::Local,
                &nodes(&[2]),
                None,
                NodeId::from(1),
                false,
                departed_for(0..1)
            )
            .is_empty(),
            "a local cache has no peers to invalidate"
        );
    }

    #[test]
    fn distributed_departed_writes_group_by_the_part_other_owners_and_await() {
        let self_node = NodeId::from(1);
        let eligible: Vec<NodeId> = (1..=5u64).map(NodeId::from).collect();
        let owners = std::num::NonZeroU8::new(2).expect("nonzero");
        let view = OwnershipView::compute(self_node, eligible, owners);
        let departed = departed_for(0..40);

        let plan = plan_departed_sends(
            Mode::Distributed { owners },
            &[],
            Some(&view),
            self_node,
            false,
            departed.clone(),
        );

        let expected: Vec<OwnerGroup> = group_by_owner_set(&view, self_node, departed.clone())
            .into_iter()
            .filter(|group| !group.owners.is_empty())
            .collect();
        assert!(!expected.is_empty(), "some part has another owner");
        assert_eq!(plan.len(), expected.len());
        for (send, group) in plan.iter().zip(&expected) {
            assert_eq!(send.targets, group.owners);
            assert_eq!(send.records, group.records);
            assert_eq!(
                send.class,
                DepartedClass::Awaiting,
                "a co-owner must not keep the older value anti-entropy would push back"
            );
            assert!(!send.targets.contains(&self_node));
        }
        let sent: usize = plan.iter().map(|send| send.records.len()).sum();
        let sole_owned = departed
            .iter()
            .filter(|rec| {
                view.owners_of(PartId::of_key(&rec.key))
                    .iter()
                    .all(|&n| n == self_node)
            })
            .count();
        assert_eq!(
            sent + sole_owned,
            departed.len(),
            "a part this node solely owns has nobody to tell"
        );
    }

    #[test]
    fn distributed_departed_writes_without_a_view_plan_no_send() {
        let owners = std::num::NonZeroU8::new(2).expect("nonzero");
        let plan = plan_departed_sends(
            Mode::Distributed { owners },
            &nodes(&[2]),
            None,
            NodeId::from(1),
            false,
            departed_for(0..3),
        );
        assert_eq!(plan, Vec::new(), "no view names no owner");
    }

    #[test]
    fn a_merging_cache_plans_no_departed_send_in_any_mode() {
        let self_node = NodeId::from(1);
        let eligible: Vec<NodeId> = (1..=5u64).map(NodeId::from).collect();
        let owners = std::num::NonZeroU8::new(2).expect("nonzero");
        let view = OwnershipView::compute(self_node, eligible, owners);
        for mode in [
            Mode::Local,
            Mode::Invalidation,
            Mode::Replicated,
            Mode::Distributed { owners },
        ] {
            for merges in [false, true] {
                let plan =
                    plan_departed_sends(mode, &nodes(&[2, 3]), Some(&view), self_node, merges, {
                        departed_for(0..40)
                    });
                assert_eq!(
                    plan.is_empty(),
                    merges || matches!(mode, Mode::Local),
                    "{mode:?}, merges = {merges}: a merging resolver's peers keep their merged copy"
                );
            }
        }
    }

    /// A counter shard in `mode` on the clock `clock` reads.
    fn counter_shard(
        mode: Mode,
        node: u64,
        max_capacity: u64,
        clock: &Arc<AtomicU64>,
    ) -> Shard<u32, PnCounter> {
        let reader = Arc::clone(clock);
        Shard::<u32, PnCounter>::new(
            SmolStr::new("c"),
            mode,
            NodeId::from(node),
            max_capacity,
            None,
            None,
        )
        .with_resolver(Arc::new(PnCounterResolver))
        .with_clock(Arc::new(move || reader.load(Ordering::SeqCst)))
    }

    fn delta(node: u64, by: u64) -> PnCounter {
        PnCounter::local_delta(WriterId::new(NodeId::from(node), 1), by)
    }

    /// The writes `shard` queued, newest per key, as `fan_out_batch` drains them.
    fn queued_writes(shard: &Shard<u32, PnCounter>) -> Vec<(u32, Hlc)> {
        newest_per_key(
            shard
                .fan_out_queue()
                .drain()
                .into_iter()
                .map(|item| match item {
                    FanOutItem::Applied(key, ver) => (key, ver),
                    FanOutItem::Forward(_) => unreachable!("a local write queues as applied"),
                })
                .collect(),
        )
    }

    /// Seeds `peer` with counters for keys 1 and 2, written by node 2, then
    /// moves the clock on, so a later write at the writer is the newer version.
    async fn peer_holds_counters(peer: &Shard<u32, PnCounter>, clock: &Arc<AtomicU64>) {
        for key in [1, 2] {
            peer.merge(key, delta(2, 1_000)).await.expect("merge");
        }
        let _ = peer.fan_out_queue().drain();
        clock.fetch_add(10, Ordering::SeqCst);
    }

    /// Delivers `plan` to `peer` as the mesh would, on the receive path.
    async fn deliver(plan: &[DepartedSend], peer: &Shard<u32, PnCounter>) {
        for send in plan {
            for rec in &send.records {
                ShardOps::invalidate(peer, rec.key.clone(), rec.ver).await;
            }
        }
    }

    /// How many of keys 1 and 2 `peer` still reads as a counter at 1,000.
    async fn counters_kept(peer: &Shard<u32, PnCounter>) -> usize {
        let mut kept = 0;
        for key in [1, 2] {
            kept += usize::from(peer.get(&key).await.map(|c| c.value()) == Some(1_000));
        }
        kept
    }

    /// Departs a write of `writer` (the `departed` carriers), plans it in
    /// `mode` for the merging resolver and delivers the plan to `peer`: the
    /// peer keeps both counters. The same carriers planned for a resolver
    /// that does not merge drop the peer's copy of the departed key.
    async fn assert_departed_spares_peer(
        mode: Mode,
        writer: &Shard<u32, PnCounter>,
        peer: &Shard<u32, PnCounter>,
        departed: Vec<WireRecord>,
    ) {
        assert!(!departed.is_empty(), "the scenario departs a write");
        assert!(writer.merges());
        let plan = plan_departed_sends(
            mode,
            &nodes(&[2]),
            None,
            NodeId::from(1),
            writer.merges(),
            departed.clone(),
        );
        assert!(plan.is_empty(), "{mode:?}: nothing is sent");
        deliver(&plan, peer).await;
        assert_eq!(counters_kept(peer).await, 2, "{mode:?}");

        let lww = plan_departed_sends(mode, &nodes(&[2]), None, NodeId::from(1), false, departed);
        deliver(&lww, peer).await;
        assert!(
            counters_kept(peer).await < 2,
            "{mode:?}: the same carrier drops a peer's copy where the resolver does not merge"
        );
    }

    #[tokio::test]
    async fn an_evicted_merging_write_leaves_the_peers_counter_alone_in_invalidation() {
        let clock = Arc::new(AtomicU64::new(1_000_000));
        let writer = counter_shard(Mode::Invalidation, 1, 1, &clock);
        let peer = counter_shard(Mode::Invalidation, 2, u64::MAX, &clock);
        peer_holds_counters(&peer, &clock).await;
        writer.merge(1, delta(1, 5)).await.expect("merge");
        writer.merge(2, delta(1, 5)).await.expect("merge");
        assert_eq!(writer.entry_count().await, 1, "one counter fits");

        let fan = writer.fan_out_records(&queued_writes(&writer)).await;

        assert_departed_spares_peer(Mode::Invalidation, &writer, &peer, fan.departed).await;
    }

    #[tokio::test]
    async fn a_swept_merging_write_leaves_the_peers_counter_alone_in_replicated() {
        let clock = Arc::new(AtomicU64::new(1_000_000));
        let writer = counter_shard(Mode::Replicated, 1, u64::MAX, &clock);
        let peer = counter_shard(Mode::Replicated, 2, u64::MAX, &clock);
        peer_holds_counters(&peer, &clock).await;
        for key in [1, 2] {
            writer
                .merge_with_ttl(key, delta(1, 5), Duration::from_millis(50))
                .await
                .expect("merge");
        }
        let queued = queued_writes(&writer);
        clock.fetch_add(100, Ordering::SeqCst);
        ShardOps::run_pending_tasks(&writer).await;

        let fan = writer.fan_out_records(&queued).await;

        assert!(
            fan.records.is_empty(),
            "a merging sweep keeps no tombstone, so both writes depart"
        );
        assert_departed_spares_peer(Mode::Replicated, &writer, &peer, fan.departed).await;
    }

    #[tokio::test]
    async fn a_released_merging_write_leaves_the_owners_counter_alone_in_distributed() {
        let self_node = NodeId::from(1);
        let owners = std::num::NonZeroU8::new(2).expect("nonzero");
        let view =
            OwnershipView::compute(self_node, (1..=3u64).map(NodeId::from).collect(), owners);
        let clock = Arc::new(AtomicU64::new(1_000_000));
        let peer = counter_shard(Mode::Replicated, 2, u64::MAX, &clock);
        peer_holds_counters(&peer, &clock).await;
        let departed = |keys: [u32; 2]| -> Vec<WireRecord> {
            keys.into_iter()
                .map(|key| WireRecord {
                    key: encode_key(&key).expect("key"),
                    value: None,
                    ver: Hlc {
                        wall_ms: 1_000_100,
                        logical: 0,
                        node: self_node,
                    },
                    expires_at_ms: None,
                })
                .collect()
        };
        let carriers = departed([1, 2]);

        let merging = plan_departed_sends(
            Mode::Distributed { owners },
            &[],
            Some(&view),
            self_node,
            true,
            carriers.clone(),
        );
        assert!(
            merging.is_empty(),
            "a part's other owners keep their counter"
        );
        deliver(&merging, &peer).await;
        assert_eq!(counters_kept(&peer).await, 2);

        let lww = plan_departed_sends(
            Mode::Distributed { owners },
            &[],
            Some(&view),
            self_node,
            false,
            carriers,
        );
        assert!(
            !lww.is_empty(),
            "the same carriers go out where the resolver does not merge"
        );
    }
}

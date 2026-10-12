//! Unit tests for [`Shard`](super::Shard) and the store's pure functions, grouped by topic.

use std::collections::{HashMap, HashSet};
use std::num::NonZeroU8;
use std::sync::atomic::Ordering;

use futures::stream::StreamExt;
use rand::{RngExt as _, SeedableRng as _, rngs::StdRng};
use tokio::sync::watch;

use super::*;
use crate::config::ClusterConfig;
use crate::error::CacheError;
use crate::explain::{LocalRecord, Residency};
use crate::ownership::{Granularity, OwnershipView};

mod apply;
mod builders;
mod capacity;
/// [`ShardOps::compact_pass`]'s real, non-default implementation on
/// [`Shard`]: shard-level plumbing tests covering a fake retire
/// predicate injected directly, the compacted record's shape, its
/// unchanged `Hlc` and lack of any `Event`, the `Mode::Distributed`
/// skip-not-forward rule for an unowned bucket, the spilled-entry
/// skip, rate-limited cursor rotation across several calls with
/// returned counts matching actual work, the non-merging early-out,
/// and two shards compacting independently converging once
/// cross-applied.
mod compact_pass_tests;
mod conflict_resolvers;
mod distributed;
mod gc_digests;
mod merging;
mod ownership;
mod read_through;
#[cfg(feature = "spill")]
mod spill_lifecycle;
/// `Shard`-layer coverage of the spill read path: every method here
/// goes through a real [`crate::store::spill::SpillTier`] and its
/// dedicated flusher thread, never a synthetic engine-level shortcut, so
/// I/O tests are gated like `spill.rs`'s own.
#[cfg(all(feature = "spill", not(feature = "sim")))]
mod spill_reads;
mod ttl;
mod values;
mod wire_surface;
mod write_api;

/// A `Mode::Distributed` shard for `self_node`, its ownership view
/// computed directly from `eligible` via `OwnershipView::compute` (no
/// cluster, no `Peer`/`CacheModes` fixture), attached before the shard
/// is ever shared, matching production's `attach_ownership`. Returns
/// the shard's own view alongside for a test to compute expected
/// owned/unowned keys against, and the tracker's `watch::Sender` so a
/// test can publish a further view.
fn distributed_shard<K, V>(
    self_node: NodeId,
    eligible: Vec<NodeId>,
    k: u8,
    residency: Arc<ResidencySet>,
) -> (
    Shard<K, V>,
    Arc<OwnershipView>,
    watch::Sender<Arc<OwnershipView>>,
)
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let owners = NonZeroU8::new(k).expect("nonzero");
    let (tracker, tx) = OwnershipTracker::seed(
        self_node,
        &[],
        &HashMap::new(),
        &SmolStr::new("test"),
        owners,
    );
    let view = Arc::new(OwnershipView::compute(self_node, eligible, owners));
    tx.send(Arc::clone(&view)).expect("receiver still alive");
    let shard = Shard::<K, V>::new(
        SmolStr::new("test"),
        Mode::Distributed { owners },
        self_node,
        u64::MAX,
        None,
        None,
    )
    .with_ownership(tracker, residency);
    (shard, view, tx)
}

/// Every part of `bucket`: what a bucket-granularity view owns or
/// releases together.
fn whole(bucket: u16) -> Vec<PartId> {
    PartId::of_bucket(bucket).collect()
}

/// The first `u32` key, scanning from `0`, whose part `view.owns`
/// matches `owns`.
fn find_key_by_ownership(view: &OwnershipView, owns: bool) -> u32 {
    (0..1_000_000u32)
        .find(|&key| view.owns(PartId::of_key(&key_bytes(&key))) == owns)
        .expect("a matching key is found within a generous scan range")
}

fn shard<K, V>(node: u64) -> Shard<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    Shard::new(
        SmolStr::new("test"),
        Mode::Replicated,
        NodeId::from(node),
        10_000,
        None,
        None,
    )
}

fn hlc(wall_ms: u64, node: u64) -> Hlc {
    Hlc {
        wall_ms,
        logical: 0,
        node: NodeId::from(node),
    }
}

/// A wall clock one second ahead of the real one: newer than anything a
/// test shard stamps, and well inside the default `max_clock_skew`.
fn ahead_ms() -> u64 {
    now_ms() + 1_000
}

fn key_bytes<K: Serialize>(key: &K) -> Bytes {
    Bytes::from(postcard::to_stdvec(key).expect("test key encodes"))
}

/// Unwraps every item as [`FanOutItem::Applied`], panicking on a
/// [`FanOutItem::Forward`]: every test that calls this drains an
/// owner's or a non-`Distributed` shard's queue, which never forwards.
fn applied_keys<K: std::fmt::Debug>(items: Vec<FanOutItem<K>>) -> Vec<K> {
    items
        .into_iter()
        .map(|item| match item {
            FanOutItem::Applied(key) => key,
            FanOutItem::Forward(rec) => {
                panic!("expected only FanOutItem::Applied, found a forwarded record: {rec:?}")
            }
        })
        .collect()
}

#[derive(Debug, thiserror::Error)]
#[error("loader boom")]
struct BoomError;

/// Compares the incrementally-maintained digest against a full recompute.
async fn assert_digest_matches_full_recompute<K, V>(s: &Shard<K, V>)
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let expected_parts = s.engine.recompute_digests();
    for BucketDigest { bucket, digest } in ShardOps::digests(s).await {
        let expected = (0..PART_COUNT).fold(0u64, |acc, part| {
            acc ^ expected_parts[usize::from(bucket) * PART_COUNT + part]
        });
        assert_eq!(
            digest, expected,
            "bucket {bucket} incremental digest diverged from full recompute"
        );
    }
}

/// Always prefers whichever record is passed as `a`, violating
/// `ConflictResolver::winner`'s antisymmetry requirement.
#[derive(Debug, Clone, Copy)]
struct AlwaysA;

impl ConflictResolver for AlwaysA {
    fn winner(&self, _key: &[u8], _a: RecordView<'_>, _b: RecordView<'_>) -> Winner {
        Winner::A
    }
}

/// A resolver whose `merge` always claims to have folded both sides into
/// fixed bytes, regardless of whether those bytes decode as the shard's
/// value type, and whose `winner` falls back to plain `Hlc` order, like
/// `LwwResolver`. `resolve_conflict` itself is private to `engine.rs`, so
/// this is how its engine-enforced value-presence guard and
/// decode-failure rejection get exercised from out here, through the
/// public `Shard`/`ShardOps` surface only.
#[derive(Debug, Clone)]
struct AlwaysMerge {
    value: Bytes,
    expires_at_ms: Option<u64>,
}

impl ConflictResolver for AlwaysMerge {
    fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        if a.ver >= b.ver { Winner::A } else { Winner::B }
    }

    fn merges(&self) -> bool {
        true
    }

    fn merge(&self, _key: &[u8], _a: RecordView<'_>, _b: RecordView<'_>) -> Option<Merged> {
        Some(Merged {
            value: self.value.clone(),
            expires_at_ms: self.expires_at_ms,
        })
    }
}

/// A `WireRecord` for `key`/`value` at `ver`, matching this test
/// module's other hand-built remote records.
fn wire_record(key: u32, value: &str, ver: Hlc) -> WireRecord {
    WireRecord {
        key: key_bytes(&key),
        value: Some(Bytes::from(
            postcard::to_stdvec(&value.to_string()).expect("test value encodes"),
        )),
        ver,
        expires_at_ms: None,
    }
}

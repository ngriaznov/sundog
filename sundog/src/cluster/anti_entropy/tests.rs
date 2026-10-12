//! Shared helpers and the topic modules of the anti-entropy unit tests.

#[cfg(not(feature = "sim"))]
use std::time::Duration;

use super::*;

mod diff;
mod rules;

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

/// A merging round pulls before it pushes. A key whose pulled record
/// already contains the local one lands at exactly the peer's version and
/// is not pushed back; a key whose merge mints a new version still is.
#[tokio::test]
#[cfg(not(feature = "sim"))]
async fn a_merging_round_does_not_push_back_a_key_it_adopted_from_the_peer() {
    use super::super::test_support::{loopback_config, registered_shard, wait_for_peer_count};
    use crate::store::Mode;
    use crate::store::crdt::{PnCounter, PnCounterResolver, WriterId};

    let config = loopback_config();
    let cluster_a = Cluster::builder("cluster-it-apply-repairs-merging")
        .seeds(std::iter::empty())
        .config(config.clone())
        .build()
        .await
        .expect("node a builds");
    let node_a = cluster_a.node_id();
    cluster_a
        .cache::<u32, PnCounter>("counters")
        .mode(Mode::Replicated)
        .resolver(Arc::new(PnCounterResolver))
        .open()
        .await
        .expect("a opens");
    let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;
    let cluster_b = Cluster::builder("cluster-it-apply-repairs-merging")
        .seeds([gossip_a])
        .config(config)
        .build()
        .await
        .expect("node b builds");
    wait_for_peer_count(&cluster_b, 1).await;
    let node_b = cluster_b.node_id();
    let cache_b = tokio::time::timeout(
        Duration::from_secs(20),
        cluster_b
            .cache::<u32, PnCounter>("counters")
            .mode(Mode::Replicated)
            .resolver(Arc::new(PnCounterResolver))
            .open(),
    )
    .await
    .expect("open completes within the state-transfer budget")
    .expect("b opens");

    let name = SmolStr::new("counters");
    let shard_a = registered_shard(&cluster_a, &name);
    let shard_b = registered_shard(&cluster_b, &name);
    let record = |key: u32, counter: &PnCounter, wall_ms, node| {
        let key_bytes = Bytes::from(postcard::to_stdvec(&key).expect("key encodes"));
        let rec = WireRecord {
            key: key_bytes.clone(),
            value: Some(Bytes::from(
                postcard::to_stdvec(counter).expect("counter encodes"),
            )),
            ver: Hlc {
                wall_ms,
                logical: 0,
                node,
            },
            expires_at_ms: None,
        };
        (key_bytes, rec)
    };
    let one = |writer: u64| PnCounter::local_delta(WriterId::new(NodeId::from(writer), 1), 1);

    // Key 1: a's counter already holds b's increment.
    let (adopted_key, b_adopted) = record(1, &one(1), 1, node_b);
    let (_, a_adopted) = record(1, &one(1).merge(&one(2)), 2, node_a);
    // Key 2: each side holds only its own increment.
    let (minted_key, b_minted) = record(2, &one(3), 1, node_b);
    let (_, a_minted) = record(2, &one(4), 2, node_a);
    shard_b.apply_remote_batch(vec![b_adopted, b_minted]).await;
    shard_a
        .apply_remote_batch(vec![a_adopted.clone(), a_minted.clone()])
        .await;

    let repairs = apply_repairs(
        cluster_b.mesh(),
        &shard_b,
        &name,
        node_a,
        vec![adopted_key.clone(), minted_key.clone()],
        vec![adopted_key.clone(), minted_key.clone()],
        Vec::new(),
        true,
    )
    .await;
    assert!(repairs.complete, "a reachable peer answers every pull");

    let stored = shard_b.records_for(vec![adopted_key, minted_key]).await;
    let (adopted, minted) = (&stored[0], &stored[1]);
    assert_eq!(
        adopted.ver, a_adopted.ver,
        "b adopts a's record for key 1 as is"
    );
    assert!(
        minted.ver.node.is_merge_derived(),
        "key 2 merges to a minted version"
    );
    let pulled = wire_record_len(name.len(), &a_adopted) + wire_record_len(name.len(), &a_minted);
    assert_eq!(
        repairs.bytes_moved,
        pulled + wire_record_len(name.len(), minted),
        "both keys are pulled and only the minted one is pushed back"
    );
    assert_eq!(
        cache_b.get(&1u32).await.map(|c| c.value()),
        Some(2),
        "b's counter for key 1 holds both increments"
    );
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

    let expected = wire_record_len(name.len(), &push_rec) + wire_record_len(name.len(), &pull_rec);

    let repairs = apply_repairs(
        cluster_b.mesh(),
        &shard_b,
        &name,
        node_a,
        vec![push_key_bytes],
        vec![pull_key_bytes],
        Vec::new(),
        false,
    )
    .await;
    assert!(repairs.complete, "a reachable peer answers every pull");
    let bytes_moved = repairs.bytes_moved;

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
        by_bucket
            .entry(crate::store::bucket_of(&bytes))
            .or_default()
            .push(key);
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

/// A pull, by key or by hash, that the peer never answers leaves the
/// round's repairs incomplete, and so does a push to a peer missing
/// from the mesh, whose records never leave this node.
#[tokio::test]
#[cfg(not(feature = "sim"))]
async fn apply_repairs_reports_incomplete_when_a_peer_is_out_of_reach() {
    use super::super::test_support::{loopback_config, registered_shard};
    use crate::store::Mode;

    let name = SmolStr::new("users");
    let cluster = Cluster::builder("cluster-it-apply-repairs-unanswered")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("cluster builds alone");
    cluster
        .cache::<u32, String>(name.clone())
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("opens alone");
    let shard = registered_shard(&cluster, &name);
    let (key, rec) = encode_test_record(1, "held", cluster.node_id());
    shard.apply_remote(rec).await;
    // A node id gossip never named: every request to it fails at once,
    // as one to a peer a failure-detector flap just dropped does.
    let gone = NodeId::from(u64::MAX);

    let by_key = apply_repairs(
        cluster.mesh(),
        &shard,
        &name,
        gone,
        Vec::new(),
        vec![key.clone()],
        Vec::new(),
        false,
    )
    .await;
    assert!(!by_key.complete, "an unanswered pull by key is incomplete");

    let by_hash = apply_repairs(
        cluster.mesh(),
        &shard,
        &name,
        gone,
        Vec::new(),
        Vec::new(),
        vec![(0, vec![1])],
        true,
    )
    .await;
    assert!(
        !by_hash.complete,
        "an unanswered pull by hash is incomplete"
    );

    let push_only = apply_repairs(
        cluster.mesh(),
        &shard,
        &name,
        gone,
        vec![key],
        Vec::new(),
        Vec::new(),
        false,
    )
    .await;
    assert!(
        !push_only.complete,
        "a push to a peer missing from the mesh is incomplete"
    );

    cluster.shutdown().await;
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
    let complete = classify_part_digest_mismatches(
        cluster.mesh(),
        &shard,
        &name,
        unreachable_peer,
        mismatches,
        &mut plan,
        false,
    )
    .await;
    assert!(
        !complete,
        "a failed ae_parts call leaves the classification incomplete"
    );

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

/// The ownership part a u32 test key hashes into.
#[cfg(not(feature = "sim"))]
fn part_of_u32(key: u32) -> PartId {
    PartId::of_key(&crate::store::encode_key(&key).expect("u32 key encodes"))
}

/// `n` distinct u32 keys, each landing in a different bucket.
/// Real-transport only.
#[cfg(not(feature = "sim"))]
fn keys_in_distinct_buckets(n: usize) -> Vec<(u32, u16)> {
    let mut found = Vec::new();
    let mut seen = HashSet::new();
    for key in 0u32.. {
        let bytes = crate::store::encode_key(&key).expect("u32 key encodes");
        let bucket = crate::store::bucket_of(&bytes);
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
/// for `run_round_for_parts`'s real-transport tests. Real-transport only.
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
async fn run_round_for_parts_reports_only_the_requested_buckets_still_diverged() {
    use super::super::test_support::wait_until;

    let name = SmolStr::new("scoped-round-diverged");
    let (b, c, node_b, shard_b, shard_c) =
        two_distributed_co_owners("scoped-diverged", &name).await;

    let keys = keys_in_distinct_buckets(2);
    let (requested_key, _) = keys[0];
    let (extra_key, _) = keys[1];
    let requested_part = part_of_u32(requested_key);
    let extra_part = part_of_u32(extra_key);

    let (_, requested_rec) = encode_test_record(requested_key, "requested", node_b);
    shard_b.apply_remote(requested_rec).await;
    let (_, extra_rec) = encode_test_record(extra_key, "extra", node_b);
    shard_b.apply_remote(extra_rec).await;

    let mut outcome = PartRoundOutcome::default();
    wait_until(
        Duration::from_secs(10),
        "b's view catches up to c joining, so the round runs instead of reporting stale",
        async || {
            outcome =
                run_round_for_parts(c.mesh(), &shard_c, &name, node_b, &[requested_part]).await;
            !outcome.failed
        },
    )
    .await;

    assert_eq!(
        outcome.still_diverged,
        HashSet::from([requested_part]),
        "the requested bucket's real divergence is reported"
    );
    assert!(
        outcome.matched.is_empty(),
        "the only requested bucket diverged, so nothing is reported matched"
    );
    assert!(
        !outcome.still_diverged.contains(&extra_part) && !outcome.matched.contains(&extra_part),
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
async fn run_round_for_parts_never_reports_matched_for_a_bucket_the_peer_does_not_co_own() {
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
    let mut split: Option<(PartId, PartId)> = None;
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
            for part in view.owned_parts() {
                if view.owners_of(part).contains(&node_b) {
                    shared_with_b.get_or_insert(part);
                } else {
                    dropped_by_b.get_or_insert(part);
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
    let mut outcome = PartRoundOutcome::default();
    wait_until(
        Duration::from_secs(10),
        "b's view catches up to c and d joining, so the round against it runs instead of \
         reporting stale",
        async || {
            outcome = run_round_for_parts(
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
async fn run_round_for_parts_marks_a_bucket_matched_once_its_digest_exchange_reports_no_mismatch() {
    use super::super::test_support::wait_until;

    let name = SmolStr::new("scoped-round-matched");
    let (b, c, node_b, _shard_b, shard_c) =
        two_distributed_co_owners("scoped-matched", &name).await;

    let matched_part = part_of_u32(keys_in_distinct_buckets(1)[0].0);

    let mut outcome = PartRoundOutcome::default();
    wait_until(
        Duration::from_secs(10),
        "b's view catches up to c joining, so the round runs instead of reporting stale",
        async || {
            outcome = run_round_for_parts(c.mesh(), &shard_c, &name, node_b, &[matched_part]).await;
            !outcome.failed
        },
    )
    .await;

    assert_eq!(
        outcome.matched,
        HashSet::from([matched_part]),
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
async fn run_round_for_parts_treats_every_requested_bucket_as_still_diverged_when_the_peer_is_unreachable()
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
    let requested = [1u16, 2, 3].map(PartId::from_raw);

    let outcome =
        run_round_for_parts(cluster.mesh(), &shard, &name, unknown_peer, &requested).await;

    assert!(
        outcome.failed,
        "an unknown peer's digest exchange cannot succeed"
    );
    assert_eq!(
        outcome.still_diverged,
        requested.iter().copied().collect::<HashSet<PartId>>(),
        "every requested bucket is treated as still diverged when the round cannot run"
    );
    assert!(outcome.matched.is_empty());
    assert_eq!(outcome.bytes_moved, 0);

    cluster.shutdown().await;
}

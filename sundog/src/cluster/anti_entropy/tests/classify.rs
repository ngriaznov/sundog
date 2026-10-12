//! Tests for bucket and part-digest classification across chunks, over real transport.

use std::time::Duration;

use super::*;

/// `count` distinct buckets among `0..n` u32 keys with more than
/// `min_count` keys each, enough to span more than one
/// [`CLASSIFY_BUCKET_CHUNK`]. Real-transport only.
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
async fn classify_part_digest_mismatches_repairs_every_bucket_across_more_than_one_chunk() {
    use crate::cluster::test_support::{loopback_config, wait_for_peer_count, wait_until};
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
struct CountingPartDigestsShard {
    inner: Arc<dyn ShardOps>,
    part_digests_calls: Arc<std::sync::atomic::AtomicUsize>,
}

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
async fn classify_part_digest_mismatches_stops_issuing_ae_parts_after_the_first_chunk_fails() {
    use crate::cluster::test_support::{loopback_config, registered_shard};
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

/// Pins the listing/sketch chunking analogue of the part-digest test
/// above: one key per bucket, under `ae_sketch_min_bucket` so every
/// mismatch answers `AeMismatch::Bucket`. Every dropped key, across
/// more buckets than one chunk holds, must come back.
#[tokio::test]
async fn classify_bucket_mismatches_repairs_every_bucket_across_more_than_one_chunk() {
    use crate::cluster::test_support::{loopback_config, wait_for_peer_count, wait_until};
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

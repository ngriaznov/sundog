//! Tests for sketch-based anti-entropy repair through a running cluster.

use super::*;
use crate::cache::Cache;
use crate::cluster::anti_entropy::{RoundOutcome, run_round_against};
use crate::cluster::test_support::{registered_shard, scoped_subscriber};
use crate::hlc::Hlc;
use crate::store::{ConflictResolver, RecordView, Winner};
use crate::wire::WireRecord;
use bytes::Bytes;
use std::sync::atomic::{AtomicUsize, Ordering};

// --- Sketch-based anti-entropy (`cluster::sketch`) ---

/// Among `0..n`, every key in a bucket holding more than `min_count` of
/// them. Deterministic given a fixed key range.
fn dense_bucket_keys(n: u32, min_count: usize) -> Vec<u32> {
    let mut by_bucket: HashMap<u16, Vec<u32>> = HashMap::new();
    for key in 0..n {
        by_bucket.entry(bucket_of_u32(key)).or_default().push(key);
    }
    by_bucket
        .into_values()
        .find(|keys| keys.len() > min_count)
        .expect("at least one bucket exceeds min_count among this many keys")
}

/// `count` distinct keys that all hash into the same anti-entropy bucket
/// as key `0`, for the sketch-fallback test below.
fn keys_colliding_with_zero(count: usize) -> Vec<u32> {
    let target = bucket_of_u32(0);
    (0..)
        .filter(|&k| bucket_of_u32(k) == target)
        .take(count)
        .collect()
}

/// A [`tracing::Subscriber`] that counts `cluster::anti_entropy`'s
/// `outcome = "decoded"`/`"fallback"` events, standing in for the
/// process-global `metrics` recorder, too fragile to install per-test.
/// Installed through [`scoped_subscriber`], thread-local to this
/// single-threaded test.
struct AeSketchOutcomeSubscriber {
    decoded: Arc<AtomicUsize>,
    fallback: Arc<AtomicUsize>,
}

/// Captures an `outcome = "..."` field verbatim, shared by every
/// `tracing::Subscriber` in this module that stands in for the
/// process-global `metrics` recorder: `AeSketchOutcomeSubscriber` reads
/// `decoded`/`fallback` off it, `AePartsOutcomeSubscriber` reads
/// `listing`/`sketch`/`fallback`.
struct OutcomeVisitor(Option<String>);

impl tracing::field::Visit for OutcomeVisitor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "outcome" {
            self.0 = Some(value.to_string());
        }
    }

    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}
}

impl tracing::Subscriber for AeSketchOutcomeSubscriber {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        if event.metadata().target() != "sundog::cluster::anti_entropy" {
            return;
        }
        let mut visitor = OutcomeVisitor(None);
        event.record(&mut visitor);
        match visitor.0.as_deref() {
            Some("decoded") => {
                self.decoded.fetch_add(1, Ordering::SeqCst);
            }
            Some("fallback") => {
                self.fallback.fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        }
    }

    fn enter(&self, _span: &tracing::span::Id) {}
    fn exit(&self, _span: &tracing::span::Id) {}
}

/// A [`tracing::Subscriber`] that counts `cluster::anti_entropy`'s
/// part-path `outcome = "listing"`/`"sketch"`/`"fallback"` events, the
/// part-grained counterpart of [`AeSketchOutcomeSubscriber`], standing in
/// for `sundog_ae_parts_total` the same way that one stands in for
/// `sundog_ae_sketch_total`.
struct AePartsOutcomeSubscriber {
    listing: Arc<AtomicUsize>,
}

impl tracing::Subscriber for AePartsOutcomeSubscriber {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        if event.metadata().target() != "sundog::cluster::anti_entropy" {
            return;
        }
        let mut visitor = OutcomeVisitor(None);
        event.record(&mut visitor);
        if visitor.0.as_deref() == Some("listing") {
            self.listing.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn enter(&self, _span: &tracing::span::Id) {}
    fn exit(&self, _span: &tracing::span::Id) {}
}

#[tokio::test]
async fn anti_entropy_repairs_a_dropped_key_via_the_part_digest_path() {
    const N: u32 = 20_000;

    let config = ClusterConfig {
        ae_part_min_bucket: 8,
        // Large enough that a mismatched part, ~20 entries at most,
        // never itself exceeds it: every mismatched part answers with a
        // listing, never a sketch.
        ae_sketch_min_bucket: 1_000_000,
        ..loopback_config()
    };

    let cluster_a = Cluster::builder("cluster-it-ae-part-digest")
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
        .expect("a inserts a few thousand entries before b ever joins");

    // A bucket with more than `ae_part_min_bucket + 1` entries, so it
    // still exceeds the threshold once one entry drops below.
    let bucket_keys = dense_bucket_keys(N, config.ae_part_min_bucket + 1);
    let target_key = bucket_keys[0];

    let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;
    let cluster_b = Cluster::builder("cluster-it-ae-part-digest")
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
    assert_eq!(cache_b.entry_count().await, u64::from(N));

    // A's fan-out of the bulk insert can still be draining toward B; a
    // late `Replicate` would restore the dropped key without any
    // anti-entropy round. Wait for replicate traffic to settle in both
    // directions first.
    let (node_a, node_b) = (cluster_a.inner.node, cluster_b.inner.node);
    tokio::time::timeout(Duration::from_secs(20), async {
        while cluster_a.peer_is_streaming(node_b) || cluster_b.peer_is_streaming(node_a) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("replicate traffic settles within the bound");

    let listing = Arc::new(AtomicUsize::new(0));
    let _guard = scoped_subscriber(AePartsOutcomeSubscriber {
        listing: Arc::clone(&listing),
    });

    // Simulate a dropped `Replicate` message on B; this bucket is dense
    // enough that the responder answers with part digests, and each
    // mismatched part with a listing.
    cache_b.invalidate_local(&target_key).await;
    assert_eq!(cache_b.get(&target_key).await, None);

    wait_until(
        Duration::from_secs(15),
        "anti-entropy repairs the dropped entry via the part-digest path within the bound",
        async || cache_b.get(&target_key).await.is_some(),
    )
    .await;

    assert!(
        listing.load(Ordering::SeqCst) > 0,
        "expected at least one part to reconcile through a listing"
    );

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

#[tokio::test]
async fn anti_entropy_repairs_a_large_bucket_via_the_sketch_path() {
    const N: u32 = 4096;

    let config = ClusterConfig {
        ae_sketch_min_bucket: 4,
        ..loopback_config()
    };

    let cluster_a = Cluster::builder("cluster-it-ae-sketch-decode")
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
        .expect("a inserts a few thousand entries before b ever joins");

    // A bucket with more than `ae_sketch_min_bucket + 1` entries, so it
    // still exceeds the threshold once one entry drops below.
    let bucket_keys = dense_bucket_keys(N, config.ae_sketch_min_bucket + 1);
    let target_key = bucket_keys[0];

    let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;
    let cluster_b = Cluster::builder("cluster-it-ae-sketch-decode")
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
    assert_eq!(cache_b.entry_count().await, u64::from(N));

    let decoded = Arc::new(AtomicUsize::new(0));
    let fallback = Arc::new(AtomicUsize::new(0));
    let _guard = scoped_subscriber(AeSketchOutcomeSubscriber {
        decoded: Arc::clone(&decoded),
        fallback: Arc::clone(&fallback),
    });

    // Simulate a dropped `Replicate` message on B; this bucket is large
    // enough that the responder answers with `Msg::AeSketch`.
    cache_b.invalidate_local(&target_key).await;
    assert_eq!(cache_b.get(&target_key).await, None);

    wait_until(
        Duration::from_secs(15),
        "anti-entropy repairs the dropped entry via the sketch path within the bound",
        async || cache_b.get(&target_key).await.is_some(),
    )
    .await;

    assert!(
        decoded.load(Ordering::SeqCst) > 0,
        "expected the sketch reply for this bucket to decode at least once"
    );
    assert_eq!(
        fallback.load(Ordering::SeqCst),
        0,
        "a single-entry diff in a large bucket must decode, never fall back to a listing"
    );

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

#[tokio::test]
async fn anti_entropy_falls_back_to_the_listing_when_a_sketch_cannot_decode() {
    let config = ClusterConfig {
        ae_sketch_min_bucket: 4,
        // 2 cells per IBLT partition, too small to peel the 25-element
        // difference this test forces below.
        ae_sketch_cells: 6,
        ..loopback_config()
    };

    let cluster_a = Cluster::builder("cluster-it-ae-sketch-fallback")
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

    // 30 keys in the same anti-entropy bucket, an oversized true
    // difference once most of them are dropped below.
    let colliding_keys = keys_colliding_with_zero(30);
    let total = u64::try_from(colliding_keys.len()).expect("30 fits in a u64");
    cache_a
        .insert_many(colliding_keys.iter().map(|&k| (k, k.to_string())))
        .await
        .expect("a inserts the colliding keys before b ever joins");

    let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;
    let cluster_b = Cluster::builder("cluster-it-ae-sketch-fallback")
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
    assert_eq!(cache_b.entry_count().await, total);

    let decoded = Arc::new(AtomicUsize::new(0));
    let fallback = Arc::new(AtomicUsize::new(0));
    let _guard = scoped_subscriber(AeSketchOutcomeSubscriber {
        decoded: Arc::clone(&decoded),
        fallback: Arc::clone(&fallback),
    });

    // Drop all but the last 5 keys on B: a true difference of 25
    // elements in one bucket, past what a 6-cell sketch can peel. A
    // round that lands in the middle of the drops sees a smaller
    // difference the sketch still peels, so the cycle repeats until the
    // oversized difference reaches the fallback path.
    for attempt in 1..=5 {
        for &key in &colliding_keys[..colliding_keys.len() - 5] {
            cache_b.invalidate_local(&key).await;
        }

        wait_until(
            Duration::from_secs(15),
            &format!(
                "attempt {attempt}: anti-entropy repairs every dropped entry within the bound"
            ),
            async || cache_b.entry_count().await == total,
        )
        .await;

        for &key in &colliding_keys {
            assert_eq!(cache_b.get(&key).await, Some(key.to_string()));
        }
        if fallback.load(Ordering::SeqCst) > 0 {
            break;
        }
    }

    assert!(
        fallback.load(Ordering::SeqCst) > 0,
        "expected the oversized diff to be reported undecodable at least once"
    );

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

#[tokio::test]
async fn run_round_against_pulls_a_dropped_key_via_the_decoded_sketch() {
    const N: u32 = 4096;
    let config = ClusterConfig {
        ae_sketch_min_bucket: 4,
        ..loopback_config()
    };

    let cluster_a = Cluster::builder("cluster-it-ae-round-pull")
        .seeds(std::iter::empty())
        .config(config.clone())
        .build()
        .await
        .expect("node a builds");
    let node_a = cluster_a.node_id();
    let cache_a = cluster_a
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("a opens");
    cache_a
        .insert_many((0..N).map(|k| (k, k.to_string())))
        .await
        .expect("a inserts a few thousand entries before b ever joins");

    let bucket_keys = dense_bucket_keys(N, config.ae_sketch_min_bucket + 1);
    let target_key = bucket_keys[0];

    let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;
    let cluster_b = Cluster::builder("cluster-it-ae-round-pull")
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
    assert_eq!(cache_b.entry_count().await, u64::from(N));

    // Simulate a dropped `Replicate` message, the way the sketch-decode
    // integration test above does, then run the round directly instead
    // of waiting on the scheduler: the two-node sketch test above races
    // both peers' schedulers, so it never pins the pull-by-hash path
    // this exercises deterministically.
    cache_b.invalidate_local(&target_key).await;
    assert_eq!(cache_b.get(&target_key).await, None);

    // A round reports `Failed` when a failure-detector flap drops the
    // peer between its digest exchange and its pull, and `Reconciled`
    // only once every pull was answered.
    let name = SmolStr::new("users");
    let shard_b = registered_shard(&cluster_b, &name);
    wait_until(
        Duration::from_secs(10),
        "a round against a reconciles",
        async || {
            crate::cluster::anti_entropy::run_round_against(
                cluster_b.mesh(),
                &shard_b,
                &name,
                node_a,
            )
            .await
                == crate::cluster::anti_entropy::RoundOutcome::Reconciled
        },
    )
    .await;

    assert_eq!(
        cache_b.get(&target_key).await,
        Some(target_key.to_string()),
        "the pull-by-hash path repairs the dropped key by the time a round reconciles"
    );

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

#[tokio::test]
async fn run_round_against_pushes_a_locally_held_key_via_the_decoded_sketch() {
    const N: u32 = 4096;
    let config = ClusterConfig {
        ae_sketch_min_bucket: 4,
        ..loopback_config()
    };

    let cluster_a = Cluster::builder("cluster-it-ae-round-push")
        .seeds(std::iter::empty())
        .config(config.clone())
        .build()
        .await
        .expect("node a builds");
    let node_a = cluster_a.node_id();
    let cache_a = cluster_a
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("a opens");
    cache_a
        .insert_many((0..N).map(|k| (k, k.to_string())))
        .await
        .expect("a inserts a few thousand entries before b ever joins");

    let bucket_keys = dense_bucket_keys(N, config.ae_sketch_min_bucket + 1);
    let target_bucket = bucket_of_u32(bucket_keys[0]);
    // Bounded to ~200 expected hits against 1,024 buckets, so the search
    // is certain in practice without an unbounded iterator.
    let extra_key = (N..)
        .take(200_000)
        .find(|&k| bucket_of_u32(k) == target_bucket)
        .expect("some key beyond n lands in the same dense bucket");

    let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;
    let cluster_b = Cluster::builder("cluster-it-ae-round-push")
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
    assert_eq!(cache_b.entry_count().await, u64::from(N));

    let name = SmolStr::new("users");
    let shard_b = registered_shard(&cluster_b, &name);

    // Applied straight to the shard, not through `cache_b.insert`, so
    // nothing fans out to a on its own: only the round's push path is
    // exercised below.
    let extra_value = extra_key.to_string();
    shard_b
        .apply_remote(WireRecord {
            key: Bytes::from(postcard::to_stdvec(&extra_key).expect("test key encodes")),
            value: Some(Bytes::from(
                postcard::to_stdvec(&extra_value).expect("test value encodes"),
            )),
            ver: Hlc {
                wall_ms: crate::store::now_ms() + 1_000,
                logical: 0,
                node: cluster_b.node_id(),
            },
            expires_at_ms: None,
        })
        .await;
    assert_eq!(cache_b.get(&extra_key).await, Some(extra_value.clone()));
    assert_eq!(cache_a.get(&extra_key).await, None, "a never had this key");

    assert_eq!(
        crate::cluster::anti_entropy::run_round_against(cluster_b.mesh(), &shard_b, &name, node_a)
            .await,
        crate::cluster::anti_entropy::RoundOutcome::Reconciled
    );

    wait_until(
        Duration::from_secs(10),
        "the push path delivers b's extra key to a within the bound",
        async || cache_a.get(&extra_key).await.is_some(),
    )
    .await;
    assert_eq!(cache_a.get(&extra_key).await, Some(extra_value));

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

#[tokio::test]
async fn run_round_against_ends_on_stale_without_pushing_or_pulling() {
    // Two solo `Mode::Distributed` nodes of one cluster, with no seeds so
    // never gossip-joined to each other, so each independently computes
    // its own view over only itself: genuinely different view hashes,
    // since their `NodeId`s differ. One cluster name, so the data plane
    // accepts the pair. `Mesh::update_peers` (a real, public mesh API) registers
    // `b` on `a`'s mesh directly, the deterministic way to drive a real
    // `AeDigestScoped` exchange to a real `StaleView` reply without
    // racing gossip's own convergence timing.
    let cluster_a = solo_cluster("cluster-it-ae-stale").await;
    let cluster_b = solo_cluster("cluster-it-ae-stale").await;
    let name = SmolStr::new("prices");
    let owners = std::num::NonZeroU8::new(2).expect("nonzero");
    let cache_a = cluster_a
        .cache::<u32, String>(name.as_str())
        .mode(Mode::Distributed { owners })
        .open()
        .await
        .expect("a opens alone");
    let cache_b = cluster_b
        .cache::<u32, String>(name.as_str())
        .mode(Mode::Distributed { owners })
        .open()
        .await
        .expect("b opens alone");
    cache_b
        .insert(1, "b-only".to_string())
        .await
        .expect("insert");

    let peer_b = cluster_b.inner.membership.local_peer();
    assert_ne!(
        peer_b.gossip_addr,
        cluster_a.inner.membership.local_peer().gossip_addr,
        "distinct real nodes"
    );
    cluster_a.mesh().update_peers(vec![peer_b.clone()]);

    let shard_a = registered_shard(&cluster_a, &name);
    let shard_b = registered_shard(&cluster_b, &name);
    let hash_a = shard_a.ownership_view_hash().expect("a is distributed");
    let hash_b = shard_b.ownership_view_hash().expect("b is distributed");
    assert_ne!(
        hash_a, hash_b,
        "two independently-solo nodes compute different view hashes"
    );

    assert_eq!(
        crate::cluster::anti_entropy::run_round_against(
            cluster_a.mesh(),
            &shard_a,
            &name,
            peer_b.node
        )
        .await,
        crate::cluster::anti_entropy::RoundOutcome::Stale
    );

    assert_eq!(
        cache_a.get(&1).await,
        None,
        "a stale round ends before any push or pull lands b's key on a"
    );

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

// --- Resolvers whose winner is not the greater version ---

/// Keeps the longer value: a deterministic antisymmetric order that does not
/// follow versions, and a tombstone reads as the empty value.
#[derive(Debug, Clone, Copy)]
struct LongerValueWins;

impl ConflictResolver for LongerValueWins {
    fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        let len_a = a.value.map_or(0, <[u8]>::len);
        let len_b = b.value.map_or(0, <[u8]>::len);
        if len_b > len_a { Winner::B } else { Winner::A }
    }
}

/// Prefers the greater version but leaves `needs_value_bytes` at its default
/// `true`, so anti-entropy treats it as a value reader.
#[derive(Debug, Clone, Copy)]
struct GreaterVersionReadsValues;

impl ConflictResolver for GreaterVersionReadsValues {
    fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        if a.ver >= b.ver { Winner::A } else { Winner::B }
    }
}

/// Prefers the greater version, declares `needs_value_bytes` `false`, and
/// counts every collision it is asked to settle.
#[derive(Debug, Clone)]
struct CountingVersionOnly(Arc<AtomicUsize>);

impl ConflictResolver for CountingVersionOnly {
    fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        self.0.fetch_add(1, Ordering::SeqCst);
        if a.ver >= b.ver { Winner::A } else { Winner::B }
    }

    fn needs_value_bytes(&self) -> bool {
        false
    }
}

const LONG_VALUE: &str = "the longer value, at the lower version";

/// Two joined `Mode::Replicated` nodes with anti-entropy off, so only the
/// rounds a test drives run, each opening cache `resolved` with its own
/// resolver.
struct ResolverPair {
    cluster_a: Cluster,
    cluster_b: Cluster,
    cache_a: Cache<u32, String>,
    cache_b: Cache<u32, String>,
    name: SmolStr,
    shard_a: Arc<dyn ShardOps>,
    shard_b: Arc<dyn ShardOps>,
}

impl ResolverPair {
    async fn open(
        cluster_name: &str,
        config: ClusterConfig,
        resolver_a: Arc<dyn ConflictResolver>,
        resolver_b: Arc<dyn ConflictResolver>,
    ) -> Self {
        let config = ClusterConfig {
            ae_interval: Duration::from_hours(1),
            tombstone_ttl: Duration::from_hours(24),
            ..config
        };
        let (cluster_a, cluster_b) = two_node_cluster_with_config(cluster_name, config).await;
        let open = |cluster: &Cluster, resolver: Arc<dyn ConflictResolver>| {
            cluster
                .cache::<u32, String>("resolved")
                .mode(Mode::Replicated)
                .resolver(resolver)
                .open()
        };
        let cache_a = open(&cluster_a, resolver_a).await.expect("a opens");
        let cache_b = open(&cluster_b, resolver_b).await.expect("b opens");
        let name = SmolStr::new("resolved");
        let shard_a = registered_shard(&cluster_a, &name);
        let shard_b = registered_shard(&cluster_b, &name);
        Self {
            cluster_a,
            cluster_b,
            cache_a,
            cache_b,
            name,
            shard_a,
            shard_b,
        }
    }

    async fn open_with(cluster_name: &str, resolver: impl ConflictResolver + Clone) -> Self {
        Self::open(
            cluster_name,
            loopback_config(),
            Arc::new(resolver.clone()),
            Arc::new(resolver),
        )
        .await
    }

    /// Applies `value` (a tombstone for `None`) at `wall_ms` on one side
    /// only, so the other side never sees it, as after a dropped replicate.
    async fn apply(&self, on_a: bool, key: u32, value: Option<&str>, wall_ms: u64) {
        let (shard, cluster) = if on_a {
            (&self.shard_a, &self.cluster_a)
        } else {
            (&self.shard_b, &self.cluster_b)
        };
        shard
            .apply_remote(test_record(key, value, wall_ms, cluster.node_id()))
            .await;
    }

    /// One anti-entropy round, initiated by a when `from_a` and by b
    /// otherwise.
    async fn round(&self, from_a: bool) -> RoundOutcome {
        if from_a {
            run_round_against(
                self.cluster_a.mesh(),
                &self.shard_a,
                &self.name,
                self.cluster_b.node_id(),
            )
            .await
        } else {
            run_round_against(
                self.cluster_b.mesh(),
                &self.shard_b,
                &self.name,
                self.cluster_a.node_id(),
            )
            .await
        }
    }

    /// Waits until both sides hold the same digests, which a push applied
    /// asynchronously on the receiver reaches shortly after a round returns.
    async fn wait_converged(&self, msg: &str) {
        wait_until(Duration::from_secs(10), msg, async || {
            self.shard_a.digests().await == self.shard_b.digests().await
        })
        .await;
    }

    async fn shutdown(self) {
        self.cluster_a.shutdown().await;
        self.cluster_b.shutdown().await;
    }
}

/// A wire record for u32 `key`: a put of `value`, or a tombstone for `None`.
fn test_record(key: u32, value: Option<&str>, wall_ms: u64, node: NodeId) -> WireRecord {
    WireRecord {
        key: Bytes::from(postcard::to_stdvec(&key).expect("key encodes")),
        value: value.map(|v| Bytes::from(postcard::to_stdvec(v).expect("value encodes"))),
        ver: Hlc {
            wall_ms,
            logical: 0,
            node,
        },
        expires_at_ms: None,
    }
}

#[tokio::test]
async fn anti_entropy_converges_on_a_value_reading_resolvers_winner_held_at_the_lower_version() {
    for (label, from_a) in [("a-initiates", true), ("b-initiates", false)] {
        let pair =
            ResolverPair::open_with(&format!("cluster-it-ae-winner-{label}"), LongerValueWins)
                .await;
        // Each side missed the other's write: no fan-out.
        pair.apply(true, 1, Some(LONG_VALUE), 100).await;
        pair.apply(false, 1, Some("short"), 200).await;
        assert_eq!(pair.cache_a.get(&1).await.as_deref(), Some(LONG_VALUE));
        assert_eq!(pair.cache_b.get(&1).await.as_deref(), Some("short"));

        assert_eq!(
            pair.round(from_a).await,
            RoundOutcome::Reconciled,
            "{label}"
        );
        pair.wait_converged(label).await;
        assert_eq!(pair.cache_a.get(&1).await.as_deref(), Some(LONG_VALUE));
        assert_eq!(
            pair.cache_b.get(&1).await.as_deref(),
            Some(LONG_VALUE),
            "{label}: both replicas converge on the resolver's winner in one round"
        );

        // A converged pair has nothing left to move.
        let digests = pair.shard_b.digests().await;
        assert_eq!(
            pair.round(from_a).await,
            RoundOutcome::Reconciled,
            "{label}"
        );
        assert_eq!(pair.shard_b.digests().await, digests, "{label}");
        pair.shutdown().await;
    }
}

#[tokio::test]
async fn anti_entropy_converges_on_a_value_reading_resolvers_winner_through_the_sketch_path() {
    let config = ClusterConfig {
        ae_sketch_min_bucket: 4,
        ..loopback_config()
    };
    let keys = dense_bucket_keys(4096, config.ae_sketch_min_bucket + 1);
    let target = keys[0];
    let pair = ResolverPair::open(
        "cluster-it-ae-winner-sketch",
        config,
        Arc::new(LongerValueWins),
        Arc::new(LongerValueWins),
    )
    .await;
    // The bucket agrees everywhere but the target key.
    let writer = NodeId::from(9);
    for &key in &keys[1..] {
        let same = test_record(key, Some("same"), 50, writer);
        pair.shard_a.apply_remote(same.clone()).await;
        pair.shard_b.apply_remote(same).await;
    }
    pair.apply(true, target, Some(LONG_VALUE), 100).await;
    pair.apply(false, target, Some("short"), 200).await;

    let decoded = Arc::new(AtomicUsize::new(0));
    let fallback = Arc::new(AtomicUsize::new(0));
    let _guard = scoped_subscriber(AeSketchOutcomeSubscriber {
        decoded: Arc::clone(&decoded),
        fallback: Arc::clone(&fallback),
    });
    assert_eq!(pair.round(true).await, RoundOutcome::Reconciled);
    pair.wait_converged("the sketch path settles the lower-version winner")
        .await;
    assert_eq!(pair.cache_b.get(&target).await.as_deref(), Some(LONG_VALUE));
    assert_eq!(
        (
            decoded.load(Ordering::SeqCst),
            fallback.load(Ordering::SeqCst)
        ),
        (1, 0),
        "the one-key difference decodes from the sketch"
    );
    pair.shutdown().await;
}

#[tokio::test]
async fn anti_entropy_converges_on_a_value_reading_resolvers_winner_through_the_listing_fallback() {
    let config = ClusterConfig {
        ae_sketch_min_bucket: 4,
        // 2 cells per partition, too small to peel the difference below.
        ae_sketch_cells: 6,
        ..loopback_config()
    };
    let keys = keys_colliding_with_zero(30);
    let pair = ResolverPair::open(
        "cluster-it-ae-winner-fallback",
        config,
        Arc::new(LongerValueWins),
        Arc::new(LongerValueWins),
    )
    .await;
    // Thirty keys in one bucket, each held with the winner at the lower
    // version on one side and the loser at the higher on the other.
    for (i, &key) in keys.iter().enumerate() {
        let a_holds_winner = i % 2 == 0;
        pair.apply(a_holds_winner, key, Some(LONG_VALUE), 100).await;
        pair.apply(!a_holds_winner, key, Some("short"), 200).await;
    }

    let decoded = Arc::new(AtomicUsize::new(0));
    let fallback = Arc::new(AtomicUsize::new(0));
    let _guard = scoped_subscriber(AeSketchOutcomeSubscriber {
        decoded: Arc::clone(&decoded),
        fallback: Arc::clone(&fallback),
    });
    assert_eq!(pair.round(true).await, RoundOutcome::Reconciled);
    pair.wait_converged("the listing fallback settles every lower-version winner")
        .await;
    for &key in &keys {
        assert_eq!(pair.cache_a.get(&key).await.as_deref(), Some(LONG_VALUE));
        assert_eq!(pair.cache_b.get(&key).await.as_deref(), Some(LONG_VALUE));
    }
    assert!(
        fallback.load(Ordering::SeqCst) > 0,
        "the oversized difference reaches the listing fallback"
    );
    pair.shutdown().await;
}

#[tokio::test]
async fn a_resolver_that_declares_needs_value_bytes_false_repairs_one_way() {
    for (label, from_a) in [("a-initiates", true), ("b-initiates", false)] {
        let greater_side_collisions = Arc::new(AtomicUsize::new(0));
        let (resolver_a, resolver_b): (Arc<dyn ConflictResolver>, Arc<dyn ConflictResolver>) =
            if from_a {
                (
                    Arc::new(CountingVersionOnly(Arc::clone(&greater_side_collisions))),
                    Arc::new(CountingVersionOnly(Arc::default())),
                )
            } else {
                (
                    Arc::new(CountingVersionOnly(Arc::default())),
                    Arc::new(CountingVersionOnly(Arc::clone(&greater_side_collisions))),
                )
            };
        let pair = ResolverPair::open(
            &format!("cluster-it-ae-one-way-{label}"),
            loopback_config(),
            resolver_a,
            resolver_b,
        )
        .await;
        // The initiator holds the greater version, so it only pushes.
        pair.apply(from_a, 1, Some("greater"), 200).await;
        pair.apply(!from_a, 1, Some("lesser"), 100).await;

        assert_eq!(
            pair.round(from_a).await,
            RoundOutcome::Reconciled,
            "{label}"
        );
        pair.wait_converged(label).await;
        assert_eq!(pair.cache_a.get(&1).await.as_deref(), Some("greater"));
        assert_eq!(pair.cache_b.get(&1).await.as_deref(), Some("greater"));
        assert_eq!(
            greater_side_collisions.load(Ordering::SeqCst),
            0,
            "{label}: the greater holder is never asked to take the lesser record"
        );
        pair.shutdown().await;
    }
}

#[tokio::test]
async fn a_version_ordered_value_reading_resolver_still_converges_in_one_round() {
    // The greater holder initiates: it pulls the lesser record (rejected)
    // and pushes its own, which the lesser holder takes.
    let pair =
        ResolverPair::open_with("cluster-it-ae-reads-greater", GreaterVersionReadsValues).await;
    pair.apply(true, 1, Some("greater"), 200).await;
    pair.apply(false, 1, Some("lesser"), 100).await;
    assert_eq!(pair.round(true).await, RoundOutcome::Reconciled);
    pair.wait_converged("the greater record reaches the lesser holder")
        .await;
    assert_eq!(pair.cache_b.get(&1).await.as_deref(), Some("greater"));
    pair.shutdown().await;
}

#[tokio::test]
async fn a_newer_tombstone_beats_a_value_under_a_value_reading_resolver_whatever_the_initiator() {
    for (label, from_a) in [("a-initiates", true), ("b-initiates", false)] {
        let pair = ResolverPair::open_with(
            &format!("cluster-it-ae-tombstone-newer-{label}"),
            GreaterVersionReadsValues,
        )
        .await;
        pair.apply(true, 1, Some("value"), 100).await;
        pair.apply(false, 1, None, 200).await;

        assert_eq!(
            pair.round(from_a).await,
            RoundOutcome::Reconciled,
            "{label}"
        );
        pair.wait_converged(label).await;
        assert_eq!(pair.cache_a.get(&1).await, None, "{label}: no resurrection");
        assert_eq!(pair.cache_b.get(&1).await, None, "{label}");
        pair.shutdown().await;
    }
}

#[tokio::test]
async fn a_tombstone_and_a_value_settle_by_version_even_for_a_resolver_preferring_values() {
    for (label, from_a) in [("a-initiates", true), ("b-initiates", false)] {
        // `LongerValueWins` ranks a tombstone below every value, yet the
        // newer tombstone still wins and the deleted key stays deleted.
        let pair = ResolverPair::open_with(
            &format!("cluster-it-ae-tombstone-longer-{label}"),
            LongerValueWins,
        )
        .await;
        pair.apply(true, 1, Some(LONG_VALUE), 100).await;
        pair.apply(false, 1, None, 200).await;
        assert_eq!(
            pair.round(from_a).await,
            RoundOutcome::Reconciled,
            "{label}"
        );
        pair.wait_converged(label).await;
        assert_eq!(pair.cache_a.get(&1).await, None, "{label}: tombstone@200");
        assert_eq!(pair.cache_b.get(&1).await, None, "{label}");

        // A value newer than the tombstone wins on both replicas.
        pair.apply(true, 2, Some("short"), 300).await;
        pair.apply(false, 2, None, 250).await;
        assert_eq!(
            pair.round(from_a).await,
            RoundOutcome::Reconciled,
            "{label}"
        );
        pair.wait_converged(label).await;
        assert_eq!(pair.cache_a.get(&2).await.as_deref(), Some("short"));
        assert_eq!(pair.cache_b.get(&2).await.as_deref(), Some("short"));
        pair.shutdown().await;
    }
}

// --- A sketch-fallback listing larger than one frame ---

/// `count` distinct `len`-byte `Vec<u8>` cache keys that all hash into one
/// anti-entropy bucket, found by extending one hashed prefix with a counter
/// so the search costs a state clone per candidate rather than a hash of the
/// whole key.
fn fat_keys_in_one_bucket(count: usize, len: usize) -> Vec<Vec<u8>> {
    use xxhash_rust::xxh3::Xxh3;

    let template = postcard::to_stdvec(&vec![0u8; len]).expect("key encodes");
    let (prefix, _) = template.split_at(template.len() - 8);
    let mut state = Xxh3::new();
    state.update(prefix);
    let mut by_bucket: HashMap<u64, Vec<u64>> = HashMap::new();
    let suffixes = (0u64..)
        .find_map(|i| {
            let mut candidate = state.clone();
            candidate.update(&i.to_be_bytes());
            let bucket = candidate.digest() & 0x3FF;
            let found = by_bucket.entry(bucket).or_default();
            found.push(i);
            (found.len() == count).then(|| found.clone())
        })
        .expect("some bucket fills up");
    suffixes
        .into_iter()
        .map(|i| {
            let mut key = vec![0u8; len];
            key[len - 8..].copy_from_slice(&i.to_be_bytes());
            key
        })
        .collect()
}

#[test]
fn fat_keys_in_one_bucket_share_the_bucket_the_store_assigns() {
    let keys = fat_keys_in_one_bucket(8, 1_000);
    assert_eq!(keys.len(), 8);
    let buckets: HashSet<u16> = keys
        .iter()
        .map(|key| crate::store::bucket_of(&postcard::to_stdvec(key).expect("key encodes")))
        .collect();
    assert_eq!(buckets.len(), 1);
    assert_eq!(keys.iter().collect::<HashSet<_>>().len(), 8, "all distinct");
}

#[tokio::test]
async fn anti_entropy_repairs_a_sketch_fallback_listing_larger_than_one_frame() {
    // 96 keys of 48 KiB make one bucket whose listing is about 4.7 MB, past
    // the 4 MiB frame cap, so the fallback listing arrives in three messages.
    const KEYS: usize = 96;
    let config = ClusterConfig {
        ae_sketch_min_bucket: 4,
        // 2 cells per partition, too small to peel the 24-key difference.
        ae_sketch_cells: 6,
        ae_interval: Duration::from_hours(1),
        ..loopback_config()
    };
    let (cluster_a, cluster_b) =
        two_node_cluster_with_config("cluster-it-ae-fat-listing", config).await;
    let open = |cluster: &Cluster| {
        cluster
            .cache::<Vec<u8>, String>("fat")
            .mode(Mode::Replicated)
            .open()
    };
    let cache_a = open(&cluster_a).await.expect("a opens");
    let cache_b = open(&cluster_b).await.expect("b opens");
    let name = SmolStr::new("fat");
    let shard_a = registered_shard(&cluster_a, &name);
    let shard_b = registered_shard(&cluster_b, &name);

    let keys = fat_keys_in_one_bucket(KEYS, 48 * 1024);
    let writer = NodeId::from(9);
    let record = |key: &Vec<u8>| WireRecord {
        key: Bytes::from(postcard::to_stdvec(key).expect("key encodes")),
        value: Some(Bytes::from(
            postcard::to_stdvec("v").expect("value encodes"),
        )),
        ver: Hlc {
            wall_ms: 50,
            logical: 0,
            node: writer,
        },
        expires_at_ms: None,
    };
    for key in &keys {
        shard_b.apply_remote(record(key)).await;
    }
    // a lacks the 24 keys in the middle of b's listing and holds the rest,
    // so the keys it shares with b sit in the first and last slices and the
    // keys it pulls in the middle one.
    let first_missing = KEYS / 2 - 12;
    let missing = first_missing..first_missing + 24;
    for (i, key) in keys.iter().enumerate() {
        if !missing.contains(&i) {
            shard_a.apply_remote(record(key)).await;
        }
    }
    assert_eq!(cache_a.entry_count().await, (KEYS - 24) as u64);
    assert_eq!(cache_b.entry_count().await, KEYS as u64);

    let decoded = Arc::new(AtomicUsize::new(0));
    let fallback = Arc::new(AtomicUsize::new(0));
    let _guard = scoped_subscriber(AeSketchOutcomeSubscriber {
        decoded: Arc::clone(&decoded),
        fallback: Arc::clone(&fallback),
    });
    let outcome = run_round_against(cluster_a.mesh(), &shard_a, &name, cluster_b.node_id()).await;

    assert_eq!(outcome, RoundOutcome::Reconciled);
    assert_eq!(
        fallback.load(Ordering::SeqCst),
        1,
        "the difference is past the sketch, so the round lists the bucket"
    );
    wait_until(
        Duration::from_secs(10),
        "a pulls every key it lacked, whichever slice of the listing named it",
        async || cache_a.entry_count().await == KEYS as u64,
    )
    .await;
    assert_eq!(shard_a.digests().await, shard_b.digests().await);

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

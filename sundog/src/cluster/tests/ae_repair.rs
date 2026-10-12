//! Tests for sketch-based anti-entropy repair through a running cluster.

use super::*;
use crate::cluster::test_support::{registered_shard, scoped_subscriber};
use crate::hlc::Hlc;
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

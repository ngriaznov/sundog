//! Shared helpers and the topic modules of the cluster unit tests.

use super::test_support::{
    loopback_config, registered_shard, scoped_subscriber, wait_for_no_peers, wait_for_peer_count,
    wait_until,
};
use super::*;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

mod builder;
mod modes;
mod resolvers;
mod solo;

/// A single-node cluster over loopback, the common case for a test that
/// needs no peers at all.
async fn solo_cluster(name: &str) -> Cluster {
    Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds")
}

async fn two_node_cluster(cluster_name: &str) -> (Cluster, Cluster) {
    two_node_cluster_with_config(cluster_name, loopback_config()).await
}

/// A fresh two-node loopback pair on `config`, joined and converged
/// before this returns: `b` seeds off `a`'s gossip address, then both
/// see one live peer.
async fn two_node_cluster_with_config(
    cluster_name: &str,
    config: ClusterConfig,
) -> (Cluster, Cluster) {
    let cluster_a = Cluster::builder(cluster_name)
        .seeds(std::iter::empty())
        .config(config.clone())
        .build()
        .await
        .expect("node a builds");
    let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;

    let cluster_b = Cluster::builder(cluster_name)
        .seeds([gossip_a])
        .config(config)
        .build()
        .await
        .expect("node b builds");

    wait_for_peer_count(&cluster_a, 1).await;
    wait_for_peer_count(&cluster_b, 1).await;
    (cluster_a, cluster_b)
}

/// A fresh 3-node cluster, all-to-all: every node sees the other two
/// before this returns.
async fn three_node_cluster(cluster_name: &str) -> (Cluster, Cluster, Cluster) {
    let (cluster_a, cluster_b) = two_node_cluster(cluster_name).await;
    let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;
    let gossip_b = cluster_b.inner.membership.local_peer().gossip_addr;
    let cluster_c = Cluster::builder(cluster_name)
        .seeds([gossip_a, gossip_b])
        .config(loopback_config())
        .build()
        .await
        .expect("node c builds");
    wait_for_peer_count(&cluster_a, 2).await;
    wait_for_peer_count(&cluster_b, 2).await;
    wait_for_peer_count(&cluster_c, 2).await;
    (cluster_a, cluster_b, cluster_c)
}

/// Polls until `cluster` sees a live peer advertising `cache` under some
/// [`Mode`]. Needed before a second `open()`, or it could race the first.
async fn wait_for_cache_advertised(cluster: &Cluster, cache: &str) {
    wait_until(
        Duration::from_secs(15),
        "peer's cache-mode advertisement converges within the bound",
        async || {
            cluster
                .advertised_cache_modes()
                .values()
                .any(|caches| caches.contains_key(cache))
        },
    )
    .await;
}

#[test]
fn inbound_activity_is_recent_only_inside_the_window() {
    let activity = InboundActivity::default();
    let peer = NodeId::from(5);
    assert!(
        !activity.recent(peer, Duration::from_secs(10)),
        "never seen"
    );
    activity.note(peer);
    assert!(activity.recent(peer, Duration::from_secs(10)));
    std::thread::sleep(Duration::from_millis(15));
    assert!(
        !activity.recent(peer, Duration::from_millis(5)),
        "a stamp older than the window does not count"
    );
    assert!(!activity.recent(NodeId::from(6), Duration::from_secs(10)));
}

#[test]
fn mode_conflicts_are_reported_once_per_peer_and_cache() {
    let peer_a = NodeId::from(1);
    let peer_b = NodeId::from(2);
    let local: HashMap<SmolStr, Mode> = [
        (SmolStr::new("users"), Mode::Replicated),
        (SmolStr::new("orders"), Mode::Invalidation),
    ]
    .into_iter()
    .collect();
    let mut advertised: CacheModes = HashMap::new();
    advertised.insert(
        peer_a,
        [
            (SmolStr::new("users"), Mode::Invalidation),
            (SmolStr::new("orders"), Mode::Invalidation),
            (SmolStr::new("unrelated"), Mode::Replicated),
        ]
        .into_iter()
        .collect(),
    );
    advertised.insert(
        peer_b,
        [(SmolStr::new("users"), Mode::Replicated)]
            .into_iter()
            .collect(),
    );
    let mut warned = HashSet::new();

    assert_eq!(
        new_mode_conflicts(&advertised, &local, &mut warned),
        vec![ModeConflict {
            peer: peer_a,
            cache: SmolStr::new("users"),
            local: Mode::Replicated,
            remote: Mode::Invalidation,
        }],
        "only the pair whose modes differ counts; matching and unknown caches are ignored"
    );
    assert!(
        new_mode_conflicts(&advertised, &local, &mut warned).is_empty(),
        "the same view again reports nothing new"
    );

    advertised
        .get_mut(&peer_b)
        .expect("peer b advertised")
        .insert(SmolStr::new("users"), Mode::Invalidation);
    assert_eq!(
        new_mode_conflicts(&advertised, &local, &mut warned),
        vec![ModeConflict {
            peer: peer_b,
            cache: SmolStr::new("users"),
            local: Mode::Replicated,
            remote: Mode::Invalidation,
        }],
        "a peer that flips later is reported once, without repeating peer a"
    );
    let conflicts = new_mode_conflicts(&advertised, &local, &mut warned);
    assert!(conflicts.is_empty(), "{conflicts:?}");
}

#[tokio::test]
async fn a_lone_node_waits_the_first_peer_grace_and_then_opens_warm() {
    let name = SmolStr::new("users");
    let cluster = solo_cluster("cluster-it-origin-grace").await;
    let started = tokio::time::Instant::now();
    let _cache = cluster
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("opens alone");
    let elapsed = started.elapsed();
    // loopback_config's 5s budget gives a 1s grace.
    assert!(
        elapsed >= Duration::from_millis(900),
        "open waits the grace for a first peer before deciding it is the origin: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(4),
        "and no longer: {elapsed:?}"
    );
    assert!(
        cluster.is_warm(&name),
        "the origin is warm with nothing to receive"
    );
    cluster.shutdown().await;
}

#[tokio::test]
async fn nodes_opening_together_in_a_fresh_cluster_all_end_warm() {
    let name = SmolStr::new("users");
    let (cluster_a, cluster_b) = two_node_cluster("cluster-it-fresh-together").await;
    // Both open at once: each sees the other cold and declining, so both
    // become warm by vacancy once the grace passes.
    let (cache_a, cache_b) = tokio::time::timeout(
        Duration::from_secs(10),
        futures::future::join(
            cluster_a
                .cache::<u32, String>("users")
                .mode(Mode::Replicated)
                .open(),
            cluster_b
                .cache::<u32, String>("users")
                .mode(Mode::Replicated)
                .open(),
        ),
    )
    .await
    .expect("two cold nodes do not stall each other's open");
    let cache_a = cache_a.expect("a opens");
    let cache_b = cache_b.expect("b opens");
    assert!(cluster_a.is_warm(&name));
    assert!(cluster_b.is_warm(&name));

    cache_a.insert(1, "a".into()).await.expect("a writes");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while cache_b.get(&1).await.is_none() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "replication runs as usual"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

#[tokio::test]
async fn local_and_invalidation_caches_are_warm_the_moment_they_open() {
    let cluster = solo_cluster("cluster-it-warmth-modes").await;
    let _local = cluster
        .cache::<u32, String>("local")
        .mode(Mode::Local)
        .open()
        .await
        .expect("local opens");
    let _inval = cluster
        .cache::<u32, String>("inval")
        .mode(Mode::Invalidation)
        .open()
        .await
        .expect("invalidation opens");
    assert!(cluster.is_warm(&SmolStr::new("local")));
    assert!(cluster.is_warm(&SmolStr::new("inval")));
    assert!(!cluster.is_warm(&SmolStr::new("never-opened")));
    cluster.shutdown().await;
}

#[tokio::test]
async fn a_joiner_reconciles_with_every_peer_after_its_snapshot() {
    // Anti-entropy far apart, so only the reconcile step that follows a
    // snapshot can close the gap within the test.
    let slow_ae = ClusterConfig {
        ae_interval: Duration::from_secs(60),
        ..loopback_config()
    };
    let (cluster_a, cluster_b) =
        two_node_cluster_with_config("cluster-it-reconcile-all", slow_ae.clone()).await;
    let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;
    let cache_a = cluster_a
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("a opens");
    let cache_b = cluster_b
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("b opens");
    cache_a
        .insert(1, "held-by-a".into())
        .await
        .expect("a writes");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while cache_b.get(&1).await.is_none() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "b receives a's write"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // B loses its copy, the way a dropped replicate leaves it: whichever
    // of A and B donates to C, C must hold the key once open returns.
    cache_b.invalidate_local(&1).await;
    assert_eq!(cache_b.get(&1).await, None);

    let gossip_b = cluster_b.inner.membership.local_peer().gossip_addr;
    let cluster_c = Cluster::builder("cluster-it-reconcile-all")
        .seeds([gossip_a, gossip_b])
        .config(slow_ae)
        .build()
        .await
        .expect("node c builds");
    wait_for_peer_count(&cluster_c, 2).await;
    let cache_c = cluster_c
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("c opens");
    assert_eq!(
        cache_c.get(&1).await,
        Some("held-by-a".to_string()),
        "the post-snapshot round against every peer covers a donor's gap"
    );

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
    cluster_c.shutdown().await;
}

#[tokio::test]
async fn state_transfer_warms_a_late_joiner_from_the_existing_donor() {
    let cluster_a = solo_cluster("cluster-it-state-transfer").await;
    let cache_a = cluster_a
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("a opens");
    cache_a
        .insert(1, "pre-existing".into())
        .await
        .expect("a writes before b ever joins");

    let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;
    let cluster_b = Cluster::builder("cluster-it-state-transfer")
        .seeds([gossip_a])
        .config(loopback_config())
        .build()
        .await
        .expect("node b builds");
    wait_for_peer_count(&cluster_b, 1).await;

    // `open()` blocks for state transfer, so B's copy of a key A wrote
    // before B joined is already warm, with no wait for a live write.
    let cache_b = tokio::time::timeout(
        Duration::from_secs(15),
        cluster_b
            .cache::<u32, String>("users")
            .mode(Mode::Replicated)
            .open(),
    )
    .await
    .expect("open completes within the state-transfer budget")
    .expect("b opens");

    assert_eq!(cache_b.get(&1).await, Some("pre-existing".to_string()));

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

#[tokio::test]
async fn anti_entropy_repairs_a_locally_dropped_entry() {
    let (cluster_a, cluster_b) = two_node_cluster("cluster-it-anti-entropy").await;

    let cache_a = cluster_a
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("a opens");
    let cache_b = cluster_b
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("b opens");

    cache_a.insert(1, "hello".into()).await.expect("a inserts");
    wait_until(
        Duration::from_secs(10),
        "live fan-out delivers the insert to b",
        async || cache_b.get(&1).await.is_some(),
    )
    .await;

    // Simulate a dropped `Replicate` message: wipe B's copy without a
    // tombstone. Only anti-entropy can bring it back.
    cache_b.invalidate_local(&1).await;
    assert_eq!(cache_b.get(&1).await, None);

    wait_until(
        Duration::from_secs(10),
        "anti-entropy repairs the dropped entry within a few rounds",
        async || cache_b.get(&1).await.is_some(),
    )
    .await;

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

#[tokio::test]
async fn clear_fans_tombstones_out_to_an_empty_peer() {
    let (cluster_a, cluster_b) = two_node_cluster("cluster-it-clear").await;

    let cache_a = cluster_a
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("a opens");
    let cache_b = cluster_b
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("b opens");

    cache_a
        .insert_many((0..10u32).map(|k| (k, k.to_string())))
        .await
        .expect("a inserts");

    tokio::time::timeout(Duration::from_secs(10), async {
        while cache_b.entry_count().await < 10 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("b observes all ten entries within the bound");

    cache_a.clear().await.expect("a clears");

    tokio::time::timeout(Duration::from_secs(10), async {
        while cache_b.entry_count().await != 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("b's copy converges to empty within the bound");

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

// --- Sketch-based anti-entropy (`cluster::sketch`) ---

/// `key`'s part, from its postcard-encoded bytes.
fn part_of_u32(key: u32) -> crate::store::PartId {
    crate::store::PartId::of_key(
        &postcard::to_stdvec(&key).expect("a u32 key always postcard-encodes"),
    )
}

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

/// A batch routed under another view hash at hop zero re-forwards to
/// every current owner but this node and its sender; the same batch at
/// the hop cap, or under this node's own hash, re-forwards to nobody.
#[test]
fn reforward_groups_exclude_self_and_sender_and_stop_at_the_hop_cap() {
    let self_node = NodeId::from(1);
    let sender = NodeId::from(2);
    let third = NodeId::from(3);
    let eligible = vec![self_node, sender, third];
    let k = std::num::NonZeroU8::new(2).expect("nonzero");
    let view = OwnershipView::compute(self_node, eligible, k);
    let stale_hash = view.view_hash() ^ 1;

    let records: Vec<WireRecord> = (0u32..200).map(wire_record_for_u32).collect();
    let groups = reforward_groups(&view, self_node, sender, stale_hash, 0, &records)
        .expect("a hop-zero batch under another view re-forwards");
    assert!(
        !groups.is_empty(),
        "some bucket has an owner besides self and the sender"
    );
    for group in &groups {
        assert_eq!(
            group.owners.as_slice(),
            [third],
            "with three eligible nodes the only other target is the third node"
        );
        for rec in &group.records {
            assert!(
                view.owners_of(crate::store::PartId::of_key(rec.key.as_ref()))
                    .contains(&third),
                "a record only re-forwards to a node that owns its bucket"
            );
        }
    }
    let re_forwarded: usize = groups.iter().map(|g| g.records.len()).sum();
    let expected = records
        .iter()
        .filter(|rec| {
            view.owners_of(crate::store::PartId::of_key(rec.key.as_ref()))
                .contains(&third)
        })
        .count();
    assert_eq!(
        re_forwarded, expected,
        "every record the third node owns, and no other"
    );

    assert!(
        reforward_groups(
            &view,
            self_node,
            sender,
            stale_hash,
            MAX_FORWARD_HOPS,
            &records
        )
        .is_none(),
        "a batch at the hop cap never re-forwards"
    );
    assert!(
        reforward_groups(&view, self_node, sender, view.view_hash(), 0, &records).is_none(),
        "a batch routed under this node's own view needs no re-forward"
    );
}

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
async fn refresh_task_republishes_on_a_membership_change_and_skips_an_unchanged_hash() {
    let cluster_a = solo_cluster("cluster-it-refresh-task").await;
    let name = SmolStr::new("prices");
    let k = std::num::NonZeroU8::new(2).expect("nonzero");

    let (tracker, tx) = crate::ownership::OwnershipTracker::seed(
        cluster_a.node_id(),
        &cluster_a.peers(),
        &cluster_a.advertised_cache_modes(),
        &name,
        k,
    );
    let mut sub = tracker.subscribe();
    let initial_hash = sub.borrow().view_hash();

    let cancel = CancellationToken::new();
    tokio::spawn(crate::ownership::refresh_task(
        cluster_a.clone(),
        name.clone(),
        k,
        tx,
        Arc::new(crate::ownership::ResidencySet::new()),
        cancel.clone(),
    ));

    let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;
    let cluster_b = Cluster::builder("cluster-it-refresh-task")
        .seeds([gossip_a])
        .config(loopback_config())
        .build()
        .await
        .expect("b builds");
    cluster_b.advertise_cache_mode(&name, Mode::Distributed { owners: k });
    wait_for_peer_count(&cluster_a, 1).await;

    tokio::time::timeout(Duration::from_secs(10), sub.changed())
        .await
        .expect("refresh_task republishes within the bound")
        .expect("the sender stays alive");
    let republished_hash = sub.borrow_and_update().view_hash();
    assert_ne!(
        republished_hash, initial_hash,
        "b becoming eligible changes the computed view"
    );

    // Re-advertising the exact same mode bumps b's gossip version (a
    // real membership change `cache_modes_watch` fires on) without
    // changing the eligible set at all: the recomputed view hash is
    // identical, so `send_if_modified` never republishes. A bounded
    // quiescence window with no further `changed()` is the direct
    // observation of that skip.
    cluster_b.advertise_cache_mode(&name, Mode::Distributed { owners: k });
    let unchanged = tokio::time::timeout(Duration::from_millis(800), sub.changed()).await;
    assert!(
        unchanged.is_err(),
        "an unchanged eligible set never republishes a new view"
    );
    assert_eq!(
        tracker.current().view_hash(),
        republished_hash,
        "the view stayed exactly what it was"
    );

    cancel.cancel();
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

/// A short-`ae_interval`, short-disown-grace config for the rebalance
/// test below: a real grace elapses in ten seconds, not the default 30.
/// A joiner's pull of its share of the 65,536 parts lands about two
/// seconds after it starts in a debug build, so the grace outlasts it
/// on a runner several times slower.
fn rebalance_config() -> ClusterConfig {
    let mut config = ClusterConfig {
        ae_interval: Duration::from_millis(50),
        // A 30 s grace: long enough that the mid-grace check below
        // still lands inside it on a loaded runner.
        distributed_disown_grace_rounds: 600,
        ..loopback_config()
    };
    config.tombstone_ttl = config.bucket_release_window();
    config
}

#[test]
fn hand_off_groups_pair_each_owned_part_with_its_live_co_owners_only() {
    let (me, live_peer, dead_peer) = (NodeId::from(1), NodeId::from(2), NodeId::from(3));
    let view = OwnershipView::compute(
        me,
        vec![me, live_peer, dead_peer],
        std::num::NonZeroU8::new(2).expect("nonzero"),
    );
    let live: HashSet<NodeId> = [live_peer].into_iter().collect();
    let groups = hand_off_groups(&view, me, &live);
    assert_eq!(groups.len(), 1, "a co-owner that is not live gets nothing");
    let (peer, parts) = &groups[0];
    assert_eq!(*peer, live_peer);
    let expected: Vec<PartId> = view
        .owned_parts()
        .filter(|&part| view.owners_of(part).contains(&live_peer))
        .collect();
    assert_eq!(
        parts, &expected,
        "every part this node co-owns with the live peer"
    );
    assert!(!parts.is_empty(), "{parts:?}");
}

#[allow(
    clippy::too_many_lines,
    reason = "one scripted three-node join-then-release scenario"
)]
// Multi-threaded: every node's grace expires in the same tick, and on
// one shared thread their hand-off rounds, each over about 22k parts,
// stall the others' gossip long enough for the failure detector to flap
// the view, which restarts the grace.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rebalance_moves_a_bucket_to_a_joiner_and_the_old_owner_releases_it_after_the_grace() {
    let config = rebalance_config();
    let owners = std::num::NonZeroU8::new(2).expect("nonzero");
    let name = SmolStr::new("prices");

    let cluster_a = Cluster::builder("cluster-it-rebalance-join")
        .seeds(std::iter::empty())
        .config(config.clone())
        .build()
        .await
        .expect("a builds");
    let cache_a = cluster_a
        .cache::<u32, String>(name.as_str())
        .mode(Mode::Distributed { owners })
        .open()
        .await
        .expect("a opens alone");

    let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;
    let cluster_b = Cluster::builder("cluster-it-rebalance-join")
        .seeds([gossip_a])
        .config(config.clone())
        .build()
        .await
        .expect("b builds");
    wait_for_peer_count(&cluster_a, 1).await;
    let cache_b = tokio::time::timeout(
        Duration::from_secs(10),
        cluster_b
            .cache::<u32, String>(name.as_str())
            .mode(Mode::Distributed { owners })
            .open(),
    )
    .await
    .expect("b opens within the bound")
    .expect("b opens");
    wait_for_peer_count(&cluster_b, 1).await;

    // With only a and b live, owners = 2 means both hold everything.
    cache_a
        .insert_many((0..500u32).map(|k| (k, k.to_string())))
        .await
        .expect("a fills the cache before c ever joins");
    wait_until(
        Duration::from_secs(10),
        "b holds everything too, before c joins",
        async || cache_b.entry_count().await == 500,
    )
    .await;

    let grace = config.disown_grace();
    // No displaced owner's grace starts before c exists.
    let before_c = tokio::time::Instant::now();
    let cluster_c = Cluster::builder("cluster-it-rebalance-join")
        .seeds([gossip_a])
        .config(config)
        .build()
        .await
        .expect("c builds");
    let cache_c = tokio::time::timeout(
        Duration::from_secs(10),
        cluster_c
            .cache::<u32, String>(name.as_str())
            .mode(Mode::Distributed { owners })
            .open(),
    )
    .await
    .expect("c opens within the bound")
    .expect("c opens");
    wait_for_peer_count(&cluster_a, 2).await;
    wait_for_peer_count(&cluster_b, 2).await;
    wait_for_peer_count(&cluster_c, 2).await;

    // A key whose bucket c has newly gained, once c's own ownership
    // view has recomputed around a and b's cache-mode gossip landing.
    let (moved_key, a_displaced) = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            for key in 0..500u32 {
                let owners_now = cache_c.owners_of(&key);
                if owners_now.contains(&cluster_c.node_id()) {
                    let a_displaced = !owners_now.contains(&cluster_a.node_id());
                    return (key, a_displaced);
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("c's view converges to own at least one of the 500 keys' buckets");
    let displaced_cache = if a_displaced { &cache_a } else { &cache_b };

    wait_until(
        Duration::from_secs(10),
        "c's rebalance pull lands the moved key's value",
        async || cache_c.get(&moved_key).await == Some(moved_key.to_string()),
    )
    .await;

    // Right after the pull lands, still inside the disown grace, the
    // displaced node has not released anything yet.
    let elapsed = before_c.elapsed();
    assert!(
        elapsed < grace,
        "setup ran {elapsed:?}, past the {grace:?} grace, so the check below cannot land mid-grace"
    );
    assert_eq!(
        displaced_cache.get(&moved_key).await,
        Some(moved_key.to_string()),
        "the displaced owner still resides the key {elapsed:?} into a {grace:?} grace"
    );

    wait_until(
        grace * 2,
        "the displaced owner releases the key once the disown grace elapses",
        async || displaced_cache.get(&moved_key).await.is_none(),
    )
    .await;
    assert!(
        before_c.elapsed() >= grace,
        "the displaced owner released the key {:?} after c joined, before the {grace:?} grace",
        before_c.elapsed()
    );

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
    cluster_c.shutdown().await;
}

/// A node that owned every bucket alone, because its peers had not
/// opened the cache yet, hands the buckets it then loses to both new
/// owners as soon as its view changes. Anti-entropy and the disown
/// grace both run once an hour here, so only the push on the view
/// change can move the entries.
#[allow(
    clippy::too_many_lines,
    reason = "one scripted lone-owner-then-three-node scenario"
)]
#[tokio::test]
async fn a_lone_owner_pushes_each_bucket_it_loses_to_the_new_owners_on_the_view_change() {
    const TOTAL: u32 = 300;
    let mut config = ClusterConfig {
        ae_interval: Duration::from_secs(3600),
        state_transfer_budget: Duration::from_secs(2),
        ..loopback_config()
    };
    config.tombstone_ttl = config.bucket_release_window();
    let owners = std::num::NonZeroU8::new(2).expect("nonzero");
    let name = SmolStr::new("prices");
    let cluster_name = "cluster-it-lone-owner-push";

    let cluster_a = Cluster::builder(cluster_name)
        .seeds(std::iter::empty())
        .config(config.clone())
        .build()
        .await
        .expect("a builds");
    let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;
    let cluster_b = Cluster::builder(cluster_name)
        .seeds([gossip_a])
        .config(config.clone())
        .build()
        .await
        .expect("b builds");
    let cluster_c = Cluster::builder(cluster_name)
        .seeds([gossip_a])
        .config(config)
        .build()
        .await
        .expect("c builds");
    for cluster in [&cluster_a, &cluster_b, &cluster_c] {
        wait_for_peer_count(cluster, 2).await;
    }
    let open = |cluster: &Cluster| {
        cluster
            .cache::<u32, String>(name.as_str())
            .mode(Mode::Distributed { owners })
            .open()
    };
    let (cache_b, cache_c) = tokio::join!(open(&cluster_b), open(&cluster_c));
    let cache_b = cache_b.expect("b opens");
    let cache_c = cache_c.expect("c opens");

    // a's shard under the view it seeded before any peer advertised
    // the cache: a owns every bucket alone and holds every entry.
    let (tracker, tx) = crate::ownership::OwnershipTracker::seed(
        cluster_a.node_id(),
        &[],
        &HashMap::new(),
        &name,
        owners,
    );
    let residency = Arc::new(crate::ownership::ResidencySet::new());
    let shard_a: Arc<dyn ShardOps> = Arc::new(
        Shard::<u32, String>::new(
            name.clone(),
            Mode::Distributed { owners },
            cluster_a.node_id(),
            u64::MAX,
            None,
            None,
        )
        .with_ownership(tracker.clone(), Arc::clone(&residency)),
    );
    shard_a
        .apply_remote_batch(
            (0..TOTAL)
                .map(|key| WireRecord {
                    key: Bytes::from(postcard::to_stdvec(&key).expect("key encodes")),
                    value: Some(Bytes::from(
                        postcard::to_stdvec(&format!("v{key}")).expect("value encodes"),
                    )),
                    ver: Hlc {
                        wall_ms: 1,
                        logical: 0,
                        node: cluster_a.node_id(),
                    },
                    expires_at_ms: None,
                })
                .collect(),
        )
        .await;

    // b and c take a into their views once it advertises the cache.
    cluster_a.advertise_cache_mode(&name, Mode::Distributed { owners });
    let three = Arc::new(OwnershipView::compute_at(
        cluster_a.node_id(),
        vec![
            cluster_a.node_id(),
            cluster_b.node_id(),
            cluster_c.node_id(),
        ],
        owners,
        Granularity::Part,
    ));
    wait_until(
        Duration::from_secs(10),
        "b and c compute the same three-node view a is about to publish",
        async || {
            (0..TOTAL).all(|key| {
                let mut expected = three.owners_of(part_of_u32(key)).to_vec();
                expected.sort_unstable();
                [&cache_b, &cache_c].iter().all(|cache| {
                    let mut seen = cache.owners_of(&key);
                    seen.sort_unstable();
                    seen == expected
                })
            })
        },
    )
    .await;
    let lost: Vec<u32> = (0..TOTAL)
        .filter(|&key| !three.owns(part_of_u32(key)))
        .collect();
    assert!(
        !lost.is_empty(),
        "a three-node, two-owner split leaves a some key it does not own"
    );

    let cancel = CancellationToken::new();
    let task = tokio::spawn(crate::cluster::rebalance::rebalance_task(
        cluster_a.clone(),
        Arc::clone(&shard_a),
        tracker,
        Arc::clone(&residency),
        name.clone(),
        Duration::from_secs(3600),
        4,
        cancel.clone(),
    ));
    tokio::time::sleep(Duration::from_millis(100)).await;
    tx.send(Arc::clone(&three))
        .expect("the tracker's own receiver keeps the channel open");

    wait_until(
        Duration::from_secs(10),
        "every entry a lost reaches both of its new owners",
        async || {
            for &key in &lost {
                let expected = Some(format!("v{key}"));
                if cache_b.get(&key).await != expected || cache_c.get(&key).await != expected {
                    return false;
                }
            }
            true
        },
    )
    .await;
    for &key in &lost {
        assert!(
            residency.is_releasing(part_of_u32(key)),
            "a keeps serving part {} through its disown grace",
            part_of_u32(key)
        );
    }

    cancel.cancel();
    task.await.expect("rebalance_task exits on cancel");
    cluster_c.shutdown().await;
    cluster_b.shutdown().await;
    cluster_a.shutdown().await;
}

#[tokio::test]
async fn records_for_hashes_answers_exactly_the_records_whose_hash_matches() {
    let cluster = solo_cluster("cluster-it-records-for-hashes").await;
    let cache = cluster
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("open succeeds");
    cache
        .insert_many([
            (1, "one".to_string()),
            (2, "two".into()),
            (3, "three".into()),
        ])
        .await
        .expect("insert");

    // Constructed the way `ClusterBuilder::build` constructs the handler
    // it hands to `Mesh::spawn`, against this single-node cluster's own
    // registry.
    let handler = ClusterRequestHandler {
        shards: cluster.shards(),
        warmth: Arc::clone(&cluster.inner.warmth),
        ae_part_min_bucket: cluster.config().ae_part_min_bucket,
        ae_sketch_min_bucket: cluster.config().ae_sketch_min_bucket,
        ae_sketch_cells: cluster.config().ae_sketch_cells,
        rebalance_chunk_bytes: cluster.config().rebalance_chunk_bytes_value(),
    };
    let name = SmolStr::new("users");
    let key_one = Bytes::from(postcard::to_stdvec(&1u32).expect("test key encodes"));
    let bucket = bucket_of_u32(1);
    let hash_one = xxhash_rust::xxh3::xxh3_64(&key_one);

    let records = handler
        .records_for_hashes(name.clone(), bucket, vec![hash_one])
        .await;
    assert_eq!(records.len(), 1, "exactly the one matching hash comes back");
    assert_eq!(records[0].key, key_one);

    let none = handler
        .records_for_hashes(name, bucket, vec![hash_one.wrapping_add(1)])
        .await;
    assert!(none.is_empty(), "a hash matching nothing yields nothing");

    cluster.shutdown().await;
}

#[tokio::test]
async fn cluster_request_handler_degrades_gracefully_for_an_unregistered_cache() {
    // An unregistered cache name answers every distribution-mode method
    // with its `Unavailable` shape, the same "unknown cache degrades
    // gracefully" convention every other method on this trait already
    // has.
    let handler = ClusterRequestHandler {
        shards: Arc::new(RwLock::new(HashMap::new())),
        warmth: Arc::new(Warmth::default()),
        ae_part_min_bucket: ClusterConfig::default().ae_part_min_bucket,
        ae_sketch_min_bucket: ClusterConfig::default().ae_sketch_min_bucket,
        ae_sketch_cells: ClusterConfig::default().ae_sketch_cells,
        rebalance_chunk_bytes: ClusterConfig::default().rebalance_chunk_bytes_value(),
    };
    let name = SmolStr::new("users");

    assert_eq!(handler.ownership_view_hash(name.clone()), None);
    assert!(matches!(
        handler
            .fetch(name.clone(), Bytes::from_static(b"k1"), 1)
            .await,
        FetchServe::Unavailable
    ));
    assert!(matches!(
        handler.ae_digest_scoped(name.clone(), 1, Vec::new()).await,
        AeServeOutcome::Unavailable
    ));
    assert!(matches!(
        handler.ae_digest_masked(name.clone(), 1, Vec::new()).await,
        AeServeOutcome::Unavailable
    ));
    assert!(!handler.st_buckets_available(name.clone(), 1).await);
    let mut chunks = handler.st_bucket_chunks(name, vec![0]);
    assert!(
        chunks.next().await.is_none(),
        "an unregistered cache's chunk stream is immediately empty"
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines, reason = "one handler walk-through per RPC")]
async fn cluster_request_handler_serves_a_distributed_cache_by_view_hash() {
    let cluster = solo_cluster("cluster-it-request-handler-distributed").await;
    let cache = cluster
        .cache::<u32, String>("prices")
        .mode(Mode::Distributed {
            owners: std::num::NonZeroU8::new(2).expect("nonzero"),
        })
        .open()
        .await
        .expect("a solo node opens a distributed cache, owning every bucket");
    cache.insert(1, "one".to_string()).await.expect("insert");
    let other_key = (2..1_000u32)
        .find(|k| bucket_of_u32(*k) != bucket_of_u32(1))
        .expect("a key in another bucket is found within a generous scan range");
    let part_one = part_of_u32(1);
    let part_other = part_of_u32(other_key);
    cache
        .insert(other_key, "other".to_string())
        .await
        .expect("insert");

    let handler = ClusterRequestHandler {
        shards: cluster.shards(),
        warmth: Arc::clone(&cluster.inner.warmth),
        ae_part_min_bucket: cluster.config().ae_part_min_bucket,
        ae_sketch_min_bucket: cluster.config().ae_sketch_min_bucket,
        ae_sketch_cells: cluster.config().ae_sketch_cells,
        rebalance_chunk_bytes: cluster.config().rebalance_chunk_bytes_value(),
    };
    let name = SmolStr::new("prices");
    let view_hash = handler
        .ownership_view_hash(name.clone())
        .expect("a distributed cache reports a view hash");
    let key_one = Bytes::from(postcard::to_stdvec(&1u32).expect("test key encodes"));

    let FetchServe::Found(Some(rec)) = handler
        .fetch(name.clone(), key_one.clone(), view_hash)
        .await
    else {
        panic!("expected a found record for a key this sole owner holds");
    };
    assert_eq!(rec.key, key_one);
    // A held record answers even against a mismatched view hash; only
    // a miss is declined as stale.
    assert!(matches!(
        handler
            .fetch(name.clone(), key_one.clone(), view_hash.wrapping_add(1))
            .await,
        FetchServe::Found(Some(rec)) if rec.key == key_one
    ));
    let key_absent = Bytes::from(postcard::to_stdvec(&999_999u32).expect("test key encodes"));
    assert!(matches!(
        handler
            .fetch(name.clone(), key_absent.clone(), view_hash.wrapping_add(1))
            .await,
        FetchServe::Stale { responder_view_hash } if responder_view_hash == view_hash
    ));
    assert!(matches!(
        handler.fetch(name.clone(), key_absent, view_hash).await,
        FetchServe::Found(None)
    ));

    // Every eligible node speaks part ownership, so the view is
    // part-granular and rounds arrive masked.
    let masked = |part: PartId, digest| {
        let mask: PartMask = [part.part()].into_iter().collect();
        (part.bucket(), mask.bits(), digest)
    };
    let AeServeOutcome::Digests(mismatched) = handler
        .ae_digest_masked(
            name.clone(),
            view_hash,
            vec![masked(part_one, 0), masked(part_other, 0)],
        )
        .await
    else {
        panic!("expected the owner's part digests when the view hash matches");
    };
    let by_bucket: HashMap<u16, Vec<u64>> = mismatched
        .into_iter()
        .map(|bpd| (bpd.bucket, bpd.digests))
        .collect();
    for part in [part_one, part_other] {
        let digest = by_bucket[&part.bucket()][usize::from(part.part())];
        assert_ne!(digest, 0, "each requested part holds a record");
        let AeServeOutcome::Digests(settled) = handler
            .ae_digest_masked(name.clone(), view_hash, vec![masked(part, digest)])
            .await
        else {
            panic!("the view hash still matches");
        };
        assert!(settled.is_empty(), "a matching fold draws no reply");
    }
    assert!(matches!(
        handler
            .ae_digest_masked(name.clone(), view_hash.wrapping_add(1), Vec::new())
            .await,
        AeServeOutcome::Stale { responder_view_hash } if responder_view_hash == view_hash
    ));
    assert!(matches!(
        handler
            .ae_digest_scoped(name.clone(), view_hash.wrapping_add(1), Vec::new())
            .await,
        AeServeOutcome::Stale { responder_view_hash } if responder_view_hash == view_hash
    ));

    assert!(handler.st_buckets_available(name.clone(), view_hash).await);
    assert!(
        !handler
            .st_buckets_cold(name.clone(), vec![part_one.raw()])
            .await,
        "a warm owner is a source for its part"
    );
    assert!(matches!(
        handler
            .st_serve(name.clone(), vec![part_one.raw()], view_hash.wrapping_add(1))
            .await,
        StServe::Stale { responder_view_hash } if responder_view_hash == view_hash
    ));
    let StServe::Stream {
        mut chunks, parts, ..
    } = handler
        .st_serve(
            name.clone(),
            vec![part_one.raw(), part_other.raw()],
            view_hash,
        )
        .await
    else {
        panic!("a warm owner at the requester's view hash donates");
    };
    assert_eq!(parts, 2, "two part ids cover two parts");
    let mut served: Vec<u16> = Vec::new();
    while let Some((id, _)) = chunks.next().await {
        served.push(id);
    }
    assert_eq!(served, vec![part_one.raw(), part_other.raw()]);
    assert!(
        !handler
            .st_buckets_available(name.clone(), view_hash.wrapping_add(1))
            .await
    );

    let owned_part = part_one.raw();
    let mut chunks = handler.st_bucket_chunks(name.clone(), vec![owned_part]);
    let mut got_key = false;
    while let Some((id, chunk)) = chunks.next().await {
        assert_eq!(
            id, owned_part,
            "every chunk is tagged with the requested part"
        );
        if chunk.iter().any(|r| r.key == key_one) {
            got_key = true;
        }
    }
    assert!(
        got_key,
        "bucket_chunks streams the requested owned part's contents"
    );

    // A multi-part pull streams one part at a time, bucket by bucket
    // whatever the request order, each chunk tagged with its part.
    let key_other = Bytes::from(postcard::to_stdvec(&other_key).expect("test key encodes"));
    let mut chunks = handler.st_bucket_chunks(name, vec![part_other.raw(), owned_part]);
    let mut order: Vec<Bytes> = Vec::new();
    while let Some((id, chunk)) = chunks.next().await {
        assert!(
            chunk.iter().all(|r| PartId::of_key(&r.key).raw() == id),
            "a chunk never spans two parts, and its tag matches its records"
        );
        order.extend(chunk.into_iter().map(|r| r.key));
    }
    let expected = if part_one.bucket() < part_other.bucket() {
        vec![key_one, key_other]
    } else {
        vec![key_other, key_one]
    };
    assert_eq!(
        order, expected,
        "parts stream in bucket order, one at a time"
    );

    cluster.shutdown().await;
}

#[test]
fn serve_reply_maps_each_verdict() {
    let rec = WireRecord {
        key: Bytes::from_static(b"k"),
        value: None,
        ver: Hlc {
            wall_ms: 1,
            logical: 0,
            node: NodeId::from(1u64),
        },
        expires_at_ms: None,
    };
    assert!(matches!(
        serve_reply(ServeVerdict::Serve, Some(rec.clone()), 7),
        FetchServe::Found(Some(served)) if served == rec
    ));
    assert!(matches!(
        serve_reply(ServeVerdict::Miss, None, 7),
        FetchServe::Found(None)
    ));
    assert!(matches!(
        serve_reply(ServeVerdict::Stale, None, 7),
        FetchServe::Stale {
            responder_view_hash: 7
        }
    ));
    for declined in [ServeVerdict::DeclineDistrusted, ServeVerdict::DeclineCold] {
        assert!(matches!(
            serve_reply(declined, Some(rec.clone()), 7),
            FetchServe::Unavailable
        ));
    }
}

/// A responder with `shard` registered under `name`, and nothing else.
fn handler_for(name: &SmolStr, shard: Arc<dyn ShardOps>) -> ClusterRequestHandler {
    let shards: ShardRegistry = Arc::new(RwLock::new(HashMap::from([(name.clone(), shard)])));
    ClusterRequestHandler {
        shards,
        warmth: Arc::new(Warmth::default()),
        ae_part_min_bucket: ClusterConfig::default().ae_part_min_bucket,
        ae_sketch_min_bucket: ClusterConfig::default().ae_sketch_min_bucket,
        ae_sketch_cells: ClusterConfig::default().ae_sketch_cells,
        rebalance_chunk_bytes: ClusterConfig::default().rebalance_chunk_bytes_value(),
    }
}

/// The responder over every residency state a read tells apart, on an
/// equal and an unequal view hash, against its serve rule written out
/// longhand: distrust declines even a held record, a held
/// record in a resident part is served whatever the views say, a view
/// mismatch then answers stale, a cold part declines, and only a warm
/// part's miss is definitive.
#[tokio::test]
async fn the_fetch_responder_follows_its_serve_rule_over_every_residency_state() {
    use crate::store::read_cases::{case_shard, every_read_case};
    let name = SmolStr::new("serve-cases");
    for case in every_read_case() {
        let built = case_shard(&name, &case).await;
        let local_hash = built.view.view_hash();
        let handler = handler_for(&name, Arc::new(built.shard));
        for views_match in [true, false] {
            let asked_hash = if views_match {
                local_hash
            } else {
                local_hash ^ 1
            };
            let reply = handler
                .fetch(name.clone(), built.key_bytes.clone(), asked_hash)
                .await;
            let expected = if case.distrusted() {
                "unavailable"
            } else if case.served() {
                "found"
            } else if !views_match {
                "stale"
            } else if case.cold() {
                "unavailable"
            } else {
                "miss"
            };
            let got = match &reply {
                FetchServe::Unavailable => "unavailable",
                FetchServe::Found(Some(rec)) => {
                    assert_eq!(rec.key, built.key_bytes, "{case:?}");
                    assert_eq!(
                        rec.value.is_some(),
                        case.live(),
                        "the held record is served as held: {case:?}"
                    );
                    "found"
                }
                FetchServe::Found(None) => "miss",
                FetchServe::Stale {
                    responder_view_hash,
                } => {
                    assert_eq!(*responder_view_hash, local_hash, "{case:?}");
                    "stale"
                }
            };
            assert_eq!(got, expected, "{case:?} views_match={views_match}");
        }
    }
}

/// A shard whose `records_for` lets a pull land right after its read:
/// once the inner shard's records are read, the pull's record is
/// applied and `part` marked serving, as a pull applies its records and
/// then clears the part's cold mark. Every other method the fetch
/// responder does not call panics.
struct PullDuringRead {
    inner: Arc<Shard<u32, String>>,
    residency: Arc<crate::ownership::ResidencySet>,
    part: PartId,
    pulled: WireRecord,
}

impl ShardOps for PullDuringRead {
    fn apply_remote(&self, _rec: WireRecord) -> BoxFuture<'_, ()> {
        unimplemented!("the fetch responder never applies")
    }

    fn apply_remote_batch(&self, _recs: Vec<WireRecord>) -> BoxFuture<'_, ()> {
        unimplemented!("the fetch responder never applies")
    }

    fn invalidate(&self, _key: Bytes, _ver: Hlc) -> BoxFuture<'_, ()> {
        unimplemented!("the fetch responder never invalidates")
    }

    fn digests(&self) -> BoxFuture<'_, Vec<BucketDigest>> {
        unimplemented!("the fetch responder never reads digests")
    }

    fn bucket_entries(&self, _bucket: u16) -> BoxFuture<'_, Vec<KeyVersion>> {
        unimplemented!("the fetch responder never lists a bucket")
    }

    fn entries_for_buckets(
        &self,
        _buckets: Vec<u16>,
    ) -> BoxFuture<'_, crate::store::BucketEntries> {
        unimplemented!("the fetch responder never lists a bucket")
    }

    fn bucket_lens(&self, _buckets: Vec<u16>) -> BoxFuture<'_, Vec<BucketLen>> {
        unimplemented!("the fetch responder never counts a bucket")
    }

    fn part_digests(&self, _buckets: Vec<u16>) -> BoxFuture<'_, Vec<BucketPartDigests>> {
        unimplemented!("the fetch responder never reads digests")
    }

    fn entries_for_parts(
        &self,
        _parts: Vec<BucketPart>,
    ) -> BoxFuture<'_, crate::store::PartEntries> {
        unimplemented!("the fetch responder never lists a part")
    }

    fn records_for(&self, keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>> {
        Box::pin(async move {
            let read = ShardOps::records_for(self.inner.as_ref(), keys).await;
            ShardOps::apply_remote_batch(self.inner.as_ref(), vec![self.pulled.clone()]).await;
            self.residency.mark_serving(&[self.part]);
            read
        })
    }

    fn snapshot_chunks(&self) -> BoxStream<'static, Vec<WireRecord>> {
        unimplemented!("the fetch responder never snapshots")
    }

    fn gc_tombstones(&self, _any_member_absent: bool) -> BoxFuture<'_, ()> {
        unimplemented!("the fetch responder never collects tombstones")
    }

    fn run_pending_tasks(&self) -> BoxFuture<'_, ()> {
        unimplemented!("the fetch responder never sweeps")
    }

    fn ownership_view_hash(&self) -> Option<u64> {
        ShardOps::ownership_view_hash(self.inner.as_ref())
    }

    fn is_cold_part(&self, part: PartId) -> bool {
        ShardOps::is_cold_part(self.inner.as_ref(), part)
    }

    fn is_unverified_part(&self, part: PartId) -> bool {
        ShardOps::is_unverified_part(self.inner.as_ref(), part)
    }
}

/// The responder reads a part's marks before its record: a pull that
/// lands its record and clears the part's cold mark after the
/// responder read a miss leaves that miss undefinitive, so the responder
/// declines, and a fetch after the pull is served the pulled record.
#[tokio::test]
async fn the_fetch_responder_reads_the_cold_mark_before_the_record() {
    use crate::store::read_cases::{Held, ReadCase, case_shard};
    let name = SmolStr::new("serve-pull-during-read");
    let built = case_shard(
        &name,
        &ReadCase {
            owns: true,
            cold_marked: true,
            unsettled: false,
            stale: false,
            unverified: false,
            releasing: false,
            held: Held::Nothing,
        },
    )
    .await;
    let pulled = WireRecord {
        key: built.key_bytes.clone(),
        value: Some(Bytes::from(
            postcard::to_stdvec(&"pulled".to_string()).expect("a string encodes"),
        )),
        ver: Hlc {
            wall_ms: crate::store::now_ms(),
            logical: 1,
            node: NodeId::from(2u64),
        },
        expires_at_ms: None,
    };
    let inner = Arc::new(built.shard);
    let handler = handler_for(
        &name,
        Arc::new(PullDuringRead {
            inner: Arc::clone(&inner),
            residency: Arc::clone(&built.residency),
            part: built.part,
            pulled: pulled.clone(),
        }),
    );
    let hash = built.view.view_hash();

    let reply = handler
        .fetch(name.clone(), built.key_bytes.clone(), hash)
        .await;
    assert!(
        matches!(reply, FetchServe::Unavailable),
        "a miss read while the part was cold is no answer"
    );
    assert!(!ShardOps::is_cold_part(inner.as_ref(), built.part));

    let reply = handler_for(&name, inner)
        .fetch(name.clone(), built.key_bytes.clone(), hash)
        .await;
    assert!(
        matches!(reply, FetchServe::Found(Some(ref rec)) if *rec == pulled),
        "the pulled record is served once it landed"
    );
}

/// `verdict::serves_peers`, what `Cache::explain` reports this node
/// answers a peer, over every residency state a read tells apart: the
/// responder's reply to a fetch sent with an equal view hash.
#[tokio::test]
async fn serves_peers_is_the_responders_answer_on_an_equal_view_hash() {
    use crate::explain::ServeVerdict;
    use crate::store::read_cases::{case_shard, every_read_case};
    let name = SmolStr::new("serves-peers-cases");
    for case in every_read_case() {
        let built = case_shard(&name, &case).await;
        let shard = Arc::new(built.shard);
        let predicted = verdict::serves_peers(
            &shard.residency_of(&built.view, built.part),
            &shard.local_record(&built.key_bytes).1,
        );
        let handler = handler_for(&name, Arc::clone(&shard) as Arc<dyn ShardOps>);
        let reply = handler
            .fetch(
                name.clone(),
                built.key_bytes.clone(),
                built.view.view_hash(),
            )
            .await;
        let answered = match reply {
            FetchServe::Found(Some(_)) => ServeVerdict::Serve,
            FetchServe::Found(None) => ServeVerdict::Miss,
            FetchServe::Stale { .. } => ServeVerdict::Stale,
            FetchServe::Unavailable => match predicted {
                ServeVerdict::DeclineDistrusted | ServeVerdict::DeclineCold => predicted,
                _ => panic!("declined where explain predicts {predicted:?}: {case:?}"),
            },
        };
        assert_eq!(answered, predicted, "{case:?}");
    }
}

/// A hit in an unverified bucket answers `Unavailable`, never the
/// record itself, as if this responder held nothing at all.
#[cfg(feature = "spill")]
#[tokio::test]
async fn cluster_request_handler_answers_unavailable_for_a_hit_in_an_unverified_bucket() {
    let name = SmolStr::new("prices");
    let owners = std::num::NonZeroU8::new(2).expect("nonzero");
    let node = NodeId::from(1u64);
    let (tracker, tx) =
        crate::ownership::OwnershipTracker::seed(node, &[], &HashMap::new(), &name, owners);
    let view = Arc::new(OwnershipView::compute(node, vec![node], owners));
    tx.send(Arc::clone(&view)).expect("receiver alive");
    let residency = Arc::new(crate::ownership::ResidencySet::new());
    let shard = Shard::<u32, String>::new(
        name.clone(),
        Mode::Distributed { owners },
        node,
        u64::MAX,
        None,
        None,
    )
    .with_ownership(tracker, Arc::clone(&residency));
    let shard_ops: Arc<dyn ShardOps> = Arc::new(shard);

    let key = 1u32;
    let key_bytes = Bytes::from(postcard::to_stdvec(&key).expect("test key encodes"));
    let record = WireRecord {
        key: key_bytes.clone(),
        value: Some(Bytes::from(
            postcard::to_stdvec(&"one".to_string()).expect("test value encodes"),
        )),
        ver: Hlc {
            wall_ms: 1,
            logical: 0,
            node,
        },
        expires_at_ms: None,
    };
    shard_ops.apply_remote_batch(vec![record]).await;

    let shards: ShardRegistry = Arc::new(RwLock::new(HashMap::from([(
        name.clone(),
        Arc::clone(&shard_ops),
    )])));
    let handler = ClusterRequestHandler {
        shards,
        warmth: Arc::new(Warmth::default()),
        ae_part_min_bucket: ClusterConfig::default().ae_part_min_bucket,
        ae_sketch_min_bucket: ClusterConfig::default().ae_sketch_min_bucket,
        ae_sketch_cells: ClusterConfig::default().ae_sketch_cells,
        rebalance_chunk_bytes: ClusterConfig::default().rebalance_chunk_bytes_value(),
    };
    let view_hash = handler
        .ownership_view_hash(name.clone())
        .expect("a registered distributed cache reports a view hash");

    let part = PartId::of_key(&key_bytes);
    residency.mark_cold(&[part]);
    residency.mark_unverified(&[part]);

    assert!(
        matches!(
            handler
                .fetch(name.clone(), key_bytes.clone(), view_hash)
                .await,
            FetchServe::Unavailable
        ),
        "a hit in an unverified bucket answers Unavailable, never the record itself"
    );

    residency.clear_cold(&[part]);
    residency.clear_unverified(&[part]);
    let FetchServe::Found(Some(rec)) = handler
        .fetch(name.clone(), key_bytes.clone(), view_hash)
        .await
    else {
        panic!("expected the record again once the bucket is no longer unverified");
    };
    assert_eq!(rec.key, key_bytes);
}

/// `st_serve` reads the requester's ids at the matching view's
/// granularity for the cold check too: a cold part declines the pull,
/// a warm one in the same bucket does not.
#[tokio::test]
async fn cluster_request_handler_st_serve_declines_a_pull_naming_a_cold_part() {
    let name = SmolStr::new("prices");
    let owners = std::num::NonZeroU8::new(2).expect("nonzero");
    let node = NodeId::from(1u64);
    let (tracker, _tx) =
        crate::ownership::OwnershipTracker::seed(node, &[], &HashMap::new(), &name, owners);
    let view = tracker.current();
    assert_eq!(
        view.granularity(),
        Granularity::Part,
        "a lone current node ranks parts"
    );
    let residency = Arc::new(crate::ownership::ResidencySet::new());
    let shard_ops: Arc<dyn ShardOps> = Arc::new(
        Shard::<u32, String>::new(
            name.clone(),
            Mode::Distributed { owners },
            node,
            u64::MAX,
            None,
            None,
        )
        .with_ownership(tracker, Arc::clone(&residency)),
    );
    let shards: ShardRegistry = Arc::new(RwLock::new(HashMap::from([(name.clone(), shard_ops)])));
    let handler = ClusterRequestHandler {
        shards,
        warmth: Arc::new(Warmth::default()),
        ae_part_min_bucket: ClusterConfig::default().ae_part_min_bucket,
        ae_sketch_min_bucket: ClusterConfig::default().ae_sketch_min_bucket,
        ae_sketch_cells: ClusterConfig::default().ae_sketch_cells,
        rebalance_chunk_bytes: ClusterConfig::default().rebalance_chunk_bytes_value(),
    };
    let cold = PartId::new(9, 1);
    let warm = PartId::new(9, 2);
    residency.mark_cold(&[cold]);
    assert!(matches!(
        handler
            .st_serve(name.clone(), vec![warm.raw(), cold.raw()], view.view_hash())
            .await,
        StServe::Cold
    ));
    let StServe::Stream { order, parts, .. } = handler
        .st_serve(name.clone(), vec![warm.raw()], view.view_hash())
        .await
    else {
        panic!("a warm part donates");
    };
    assert_eq!((order, parts), (vec![warm.raw()], 1));
    assert!(matches!(
        handler
            .st_serve(SmolStr::new("unknown"), vec![warm.raw()], view.view_hash())
            .await,
        StServe::Stale {
            responder_view_hash: 0
        }
    ));
}

#[test]
fn group_in_order_keeps_first_seen_key_order_and_input_value_order() {
    let groups = group_in_order([('b', [1]), ('a', [2]), ('b', [3])]);
    assert_eq!(groups, vec![('b', vec![1, 3]), ('a', vec![2])]);
    let merged = group_in_order([("k", vec![1, 2]), ("k", vec![]), ("k", vec![3])]);
    assert_eq!(merged, vec![("k", vec![1, 2, 3])]);
    let groups = group_in_order(Vec::<(u8, [u8; 0])>::new());
    assert!(groups.is_empty(), "{groups:?}");
}

#[test]
fn serve_order_sorts_bucket_ids_and_groups_part_ids_by_bucket() {
    assert_eq!(serve_order(Granularity::Bucket, vec![9, 3, 9]), vec![3, 9]);
    let (a, b, c) = (PartId::new(5, 2), PartId::new(3, 9), PartId::new(5, 0));
    assert_eq!(
        serve_order(Granularity::Part, vec![a.raw(), b.raw(), c.raw(), a.raw()]),
        vec![b.raw(), c.raw(), a.raw()],
        "bucket 3 first, then bucket 5's parts in part order, each once"
    );
}

#[test]
fn bucket_runs_groups_consecutive_part_ids_of_one_bucket() {
    let runs = |granularity, ids: &[u16]| -> Vec<Vec<u16>> {
        bucket_runs(granularity, ids).map(<[u16]>::to_vec).collect()
    };
    assert_eq!(
        runs(Granularity::Bucket, &[3, 9]),
        vec![vec![3], vec![9]],
        "a bucket id is a run of its own"
    );
    let ids = serve_order(
        Granularity::Part,
        vec![
            PartId::new(5, 2).raw(),
            PartId::new(3, 9).raw(),
            PartId::new(5, 0).raw(),
        ],
    );
    assert_eq!(
        runs(Granularity::Part, &ids),
        vec![
            vec![PartId::new(3, 9).raw()],
            vec![PartId::new(5, 0).raw(), PartId::new(5, 2).raw()],
        ]
    );
    let no_runs = runs(Granularity::Part, &[]);
    assert!(no_runs.is_empty(), "{no_runs:?}");
}

#[test]
fn st_bucket_key_sub_batches_of_no_keys_is_empty() {
    assert_eq!(
        st_bucket_key_sub_batches(Vec::new(), 1024),
        Vec::<Vec<Bytes>>::new()
    );
}

#[test]
fn st_bucket_key_sub_batches_splits_by_estimated_size() {
    let keys: Vec<Bytes> = (0u8..5).map(|n| Bytes::from(vec![n])).collect();
    let per_key = (wire::RECORD_HEADER_LEN + 1) as u64;
    let sub_batches = st_bucket_key_sub_batches(keys.clone(), per_key * 2);
    assert_eq!(
        sub_batches,
        vec![
            vec![keys[0].clone(), keys[1].clone()],
            vec![keys[2].clone(), keys[3].clone()],
            vec![keys[4].clone()],
        ],
        "two one-byte keys fit the two-key budget exactly before a group closes"
    );
}

#[test]
fn st_bucket_key_sub_batches_gives_an_oversized_key_its_own_batch() {
    let small = Bytes::from_static(b"a");
    let big = Bytes::from(vec![0u8; 4096]);
    // Fits `small`; nowhere near enough for `big`.
    let budget = (wire::RECORD_HEADER_LEN + 1) as u64;
    let sub_batches = st_bucket_key_sub_batches(vec![small.clone(), big.clone()], budget);
    assert_eq!(
        sub_batches,
        vec![vec![small], vec![big]],
        "a key whose own estimate exceeds the budget still gets a batch of its own"
    );
}

/// A [`ShardOps`] test double recording every `records_for` call's key
/// list. Every method but `entries_for_buckets`, `records_for`, and
/// `close_spill` panics if called, since that would be a test bug.
struct RecordingShard {
    entries: Vec<(u16, Vec<KeyVersion>)>,
    held: Vec<u16>,
    records: HashMap<Bytes, WireRecord>,
    records_for_calls: Mutex<Vec<Vec<Bytes>>>,
    close_spill_calls: AtomicUsize,
}

impl ShardOps for RecordingShard {
    fn apply_remote(&self, _rec: WireRecord) -> BoxFuture<'_, ()> {
        unimplemented!("st_bucket_chunks never calls apply_remote")
    }

    fn apply_remote_batch(&self, _recs: Vec<WireRecord>) -> BoxFuture<'_, ()> {
        unimplemented!("st_bucket_chunks never calls apply_remote_batch")
    }

    fn invalidate(&self, _key: Bytes, _ver: Hlc) -> BoxFuture<'_, ()> {
        unimplemented!("st_bucket_chunks never calls invalidate")
    }

    fn digests(&self) -> BoxFuture<'_, Vec<BucketDigest>> {
        unimplemented!("st_bucket_chunks never calls digests")
    }

    fn bucket_entries(&self, _bucket: u16) -> BoxFuture<'_, Vec<KeyVersion>> {
        unimplemented!("st_bucket_chunks calls entries_for_buckets, not bucket_entries")
    }

    fn entries_for_buckets(&self, buckets: Vec<u16>) -> BoxFuture<'_, crate::store::BucketEntries> {
        let entries = self.entries.clone();
        Box::pin(async move {
            entries
                .into_iter()
                .filter(|(bucket, _)| buckets.contains(bucket))
                .collect()
        })
    }

    fn bucket_lens(&self, _buckets: Vec<u16>) -> BoxFuture<'_, Vec<BucketLen>> {
        unimplemented!("st_bucket_chunks never calls bucket_lens")
    }

    fn part_digests(&self, _buckets: Vec<u16>) -> BoxFuture<'_, Vec<BucketPartDigests>> {
        unimplemented!("st_bucket_chunks never calls part_digests")
    }

    fn entries_for_parts(
        &self,
        _parts: Vec<BucketPart>,
    ) -> BoxFuture<'_, crate::store::PartEntries> {
        unimplemented!("st_bucket_chunks never calls entries_for_parts")
    }

    fn records_for(&self, keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>> {
        self.records_for_calls
            .lock()
            .expect("invariant: fixture mutex is never poisoned")
            .push(keys.clone());
        Box::pin(async move {
            keys.into_iter()
                .filter_map(|k| self.records.get(&k).cloned())
                .collect()
        })
    }

    fn snapshot_chunks(&self) -> BoxStream<'static, Vec<WireRecord>> {
        unimplemented!("st_bucket_chunks never calls snapshot_chunks")
    }

    fn gc_tombstones(&self, _any_member_absent: bool) -> BoxFuture<'_, ()> {
        unimplemented!("st_bucket_chunks never calls gc_tombstones")
    }

    fn run_pending_tasks(&self) -> BoxFuture<'_, ()> {
        unimplemented!("st_bucket_chunks never calls run_pending_tasks")
    }

    fn close_spill(&self) {
        self.close_spill_calls.fetch_add(1, Ordering::Relaxed);
    }

    fn held_buckets(&self) -> BoxFuture<'_, Vec<u16>> {
        let held = self.held.clone();
        Box::pin(async move { held })
    }
}

/// `ShardOps`' part-granular defaults, on a stub that implements only
/// the bucket forms: held parts are every part of each held bucket,
/// nothing releases or reads cold or unverified, and a stub with no
/// view has no scoped digests.
#[tokio::test]
async fn shard_ops_part_defaults_follow_the_bucket_forms() {
    let stub = RecordingShard {
        entries: Vec::new(),
        held: vec![3, 900],
        records: HashMap::new(),
        records_for_calls: Mutex::new(Vec::new()),
        close_spill_calls: AtomicUsize::new(0),
    };
    let held = stub.held_parts().await;
    assert_eq!(held.len(), 2 * crate::store::PART_COUNT);
    assert!(held.iter().all(|part| [3, 900].contains(&part.bucket())));
    assert_eq!(stub.release_parts(&held).await, 0);
    assert!(!stub.is_cold_part(held[0]));
    assert!(!stub.is_unverified_part(held[0]));
    assert!(stub.ae_scoped_digests_for(NodeId::from(2)).await.is_none());
}

#[tokio::test]
async fn st_bucket_chunks_never_materializes_more_than_rebalance_chunk_bytes_for_a_bucket_larger_than_the_budget()
 {
    let bucket: u16 = 7;
    let key_count = 10u8;
    let keys: Vec<Bytes> = (0..key_count).map(|n| Bytes::from(vec![n])).collect();
    let ver = Hlc {
        wall_ms: 1,
        logical: 0,
        node: NodeId::from(1),
    };
    let entries: Vec<KeyVersion> = keys
        .iter()
        .map(|key| KeyVersion {
            key: key.clone(),
            version: ver,
        })
        .collect();
    let records: HashMap<Bytes, WireRecord> = keys
        .iter()
        .map(|key| {
            (
                key.clone(),
                WireRecord {
                    key: key.clone(),
                    value: Some(Bytes::from_static(b"v")),
                    ver,
                    expires_at_ms: None,
                },
            )
        })
        .collect();

    // A budget of three keys' worth forces the ten-key bucket into
    // more than one `records_for` call.
    let per_key_size = (wire::RECORD_HEADER_LEN + 1) as u64;
    let chunk_bytes = per_key_size * 3;

    let recording = Arc::new(RecordingShard {
        entries: vec![(bucket, entries)],
        held: Vec::new(),
        records,
        records_for_calls: Mutex::new(Vec::new()),
        close_spill_calls: AtomicUsize::new(0),
    });
    let shard: Arc<dyn ShardOps> = recording.clone();
    let mut registry = HashMap::new();
    registry.insert(SmolStr::new("cache"), shard);
    let handler = ClusterRequestHandler {
        shards: Arc::new(RwLock::new(registry)),
        warmth: Arc::new(Warmth::default()),
        ae_part_min_bucket: ClusterConfig::default().ae_part_min_bucket,
        ae_sketch_min_bucket: ClusterConfig::default().ae_sketch_min_bucket,
        ae_sketch_cells: ClusterConfig::default().ae_sketch_cells,
        rebalance_chunk_bytes: chunk_bytes,
    };

    let mut got_keys: Vec<Bytes> = Vec::new();
    let mut chunks = handler.st_bucket_chunks(SmolStr::new("cache"), vec![bucket]);
    while let Some((got_bucket, chunk)) = chunks.next().await {
        assert_eq!(
            got_bucket, bucket,
            "every yielded chunk is tagged with the bucket"
        );
        got_keys.extend(chunk.into_iter().map(|r| r.key));
    }
    got_keys.sort_unstable_by(|a, b| a.as_ref().cmp(b.as_ref()));
    let mut expected_keys = keys.clone();
    expected_keys.sort_unstable_by(|a, b| a.as_ref().cmp(b.as_ref()));
    assert_eq!(
        got_keys, expected_keys,
        "every key's record is served exactly once, regardless of sub-batching"
    );

    let calls = recording
        .records_for_calls
        .lock()
        .expect("invariant: fixture mutex is never poisoned");
    assert!(
        calls.len() > 1,
        "a bucket larger than the budget is fetched over more than one records_for call, not all at once"
    );
    for call in calls.iter() {
        let call_size: u64 = call
            .iter()
            .map(|k| (wire::RECORD_HEADER_LEN + k.len()) as u64)
            .sum();
        assert!(
            call_size <= chunk_bytes,
            "one records_for call never exceeds the rebalance_chunk_bytes estimate: {call_size} > {chunk_bytes}"
        );
    }
    let total_keys: usize = calls.iter().map(Vec::len).sum();
    assert_eq!(
        total_keys, key_count as usize,
        "every key is fetched exactly once across sub-batches"
    );
}

/// [`ShardOps::close_spill_checkpointed`]'s default, for an implementor
/// that never overrides it, calls the sync [`ShardOps::close_spill`]
/// once.
#[tokio::test]
async fn close_spill_checkpointed_default_resolves_and_calls_the_sync_close() {
    let recording = Arc::new(RecordingShard {
        entries: Vec::new(),
        held: Vec::new(),
        records: HashMap::new(),
        records_for_calls: Mutex::new(Vec::new()),
        close_spill_calls: AtomicUsize::new(0),
    });
    let shard: Arc<dyn ShardOps> = recording.clone();

    shard.close_spill_checkpointed().await;

    assert_eq!(
        recording.close_spill_calls.load(Ordering::Relaxed),
        1,
        "the default close_spill_checkpointed calls the sync close_spill exactly once"
    );
}

#[tokio::test]
async fn report_mode_conflicts_logs_a_late_conflict_against_a_live_peer() {
    let cluster = solo_cluster("cluster-it-mode-conflict-log").await;
    let _cache = cluster
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("open succeeds");

    // Hand-built rather than gossiped: `report_mode_conflicts` only
    // reads `cluster`'s own advertised mode, so no live peer is needed
    // to exercise its late-conflict log line deterministically.
    let peer = NodeId::from(99);
    let mut advertised: CacheModes = HashMap::new();
    advertised.insert(
        peer,
        [(SmolStr::new("users"), Mode::Invalidation)]
            .into_iter()
            .collect(),
    );
    let mut warned = HashSet::new();

    report_mode_conflicts(&advertised, &cluster, &mut warned);
    assert!(
        warned.contains(&(peer, SmolStr::new("users"))),
        "the conflict is recorded so a repeated view doesn't re-warn"
    );

    cluster.shutdown().await;
}

#[test]
fn is_ready_from_gates_only_on_replicated_caches() {
    let all_warm = [
        CacheHealth {
            name: SmolStr::new("sessions"),
            mode: Mode::Invalidation,
            warm: true,
        },
        CacheHealth {
            name: SmolStr::new("users"),
            mode: Mode::Replicated,
            warm: true,
        },
    ];
    assert!(is_ready_from(&all_warm));

    let one_cold_replicated = [
        CacheHealth {
            name: SmolStr::new("scratch"),
            mode: Mode::Local,
            warm: false,
        },
        CacheHealth {
            name: SmolStr::new("users"),
            mode: Mode::Replicated,
            warm: false,
        },
    ];
    assert!(
        !is_ready_from(&one_cold_replicated),
        "a cold Replicated cache blocks readiness even if a Local one is also cold"
    );

    assert!(is_ready_from(&[]), "no open caches at all: vacuously ready");
}

#[tokio::test]
async fn graceful_shutdown_never_counts_absent_but_a_crash_does() {
    let hard_cap = Duration::from_secs(3600);

    let cluster_a = solo_cluster("cluster-it-absence-graceful-vs-crash").await;
    let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;

    // Graceful case: b leaves through `Cluster::shutdown`.
    let cluster_b = Cluster::builder("cluster-it-absence-graceful-vs-crash")
        .seeds([gossip_a])
        .config(loopback_config())
        .build()
        .await
        .expect("node b builds");
    wait_for_peer_count(&cluster_a, 1).await;

    let node_b = cluster_b.node_id();
    cluster_b.shutdown().await;
    wait_for_no_peers(&cluster_a).await;
    assert!(
        !cluster_a.absence_tracker().any_absent(hard_cap),
        "a graceful departure is never counted absent"
    );
    assert!(
        !cluster_a.away_within(hard_cap).contains(&node_b),
        "away_within never names a graceful leaver"
    );

    // Crash case: c's background tasks are cancelled and it is dropped
    // with no `shutdown()` call, so its membership loop's channel
    // closes without ever gossiping a departure, indistinguishable
    // from a process that just died.
    let cluster_c = Cluster::builder("cluster-it-absence-graceful-vs-crash")
        .seeds([gossip_a])
        .config(loopback_config())
        .build()
        .await
        .expect("node c builds");
    wait_for_peer_count(&cluster_a, 1).await;

    let node_c = cluster_c.node_id();
    cluster_c.inner.cancel.cancel();
    drop(cluster_c);
    wait_for_no_peers(&cluster_a).await;
    // Membership hands the tracker each live set before publishing its
    // peers, so the moment `peers()` drops c, away_within names it.
    assert_eq!(
        cluster_a.away_within(hard_cap),
        vec![node_c],
        "away_within names the crashed node as soon as the peer list drops it"
    );
    assert!(
        cluster_a.absence_tracker().any_absent(hard_cap),
        "a crash, with no graceful departure gossiped, is counted absent"
    );

    cluster_a.shutdown().await;
}

#[tokio::test]
async fn readiness_flips_true_once_a_replicated_cache_finishes_warming() {
    let (cluster_a, cluster_b) = two_node_cluster("cluster-it-readiness").await;

    assert!(cluster_a.is_ready(), "no caches open yet: vacuously ready");

    let cache_a = cluster_a
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("a opens and is warm immediately: no peer holds this cache yet");
    assert!(cluster_a.is_ready());
    let health_a = cluster_a.health();
    assert!(health_a.ready);
    assert_eq!(health_a.live_peers, 1);
    assert_eq!(health_a.caches.len(), 1);
    assert_eq!(health_a.caches[0].mode, Mode::Replicated);
    assert!(health_a.caches[0].warm);

    cache_a.insert(1, "hello".into()).await.expect("a inserts");
    wait_for_cache_advertised(&cluster_b, "users").await;

    // b's own open() runs state transfer against a, so it may already be
    // warm by the time `open()` returns; either way `is_ready` must
    // agree with `health().caches[0].warm` throughout.
    let cache_b = cluster_b
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("b opens");
    tokio::time::timeout(Duration::from_secs(15), async {
        while !cluster_b.is_ready() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("b's replicated cache finishes warming within the bound");
    let health_b = cluster_b.health();
    assert!(health_b.ready);
    assert_eq!(health_b.caches.len(), 1);
    assert!(health_b.caches[0].warm);
    assert_eq!(cache_b.get(&1).await, Some("hello".to_string()));

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

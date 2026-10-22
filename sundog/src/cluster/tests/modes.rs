//! Tests for cache modes across nodes: mode conflicts, replication, invalidation and per-entry TTL.

use super::*;
use crate::cluster::test_support::wait_until;
use crate::error::CacheError;
use crate::store::{Event, Origin};

#[tokio::test]
async fn open_rejects_a_cache_mode_that_conflicts_with_a_live_peer() {
    let (cluster_a, cluster_b) = two_node_cluster("cluster-it-mode-mismatch").await;

    let _cache_a = cluster_a
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("a opens as Replicated");
    wait_for_cache_advertised(&cluster_b, "users").await;

    match cluster_b
        .cache::<u32, String>("users")
        .mode(Mode::Invalidation)
        .open()
        .await
    {
        Err(CacheError::ModeMismatch {
            cache,
            local,
            remote,
        }) => {
            assert_eq!(cache, "users");
            assert_eq!(local, Mode::Invalidation);
            assert_eq!(remote, Mode::Replicated);
        }
        other => panic!("expected ModeMismatch, got {:?}", other.map(|_| ())),
    }

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

#[tokio::test]
async fn open_allows_matching_modes_across_nodes() {
    let (cluster_a, cluster_b) = two_node_cluster("cluster-it-mode-match").await;

    let _cache_a = cluster_a
        .cache::<u32, String>("users")
        .mode(Mode::Invalidation)
        .open()
        .await
        .expect("a opens as Invalidation");
    wait_for_cache_advertised(&cluster_b, "users").await;

    let _cache_b = cluster_b
        .cache::<u32, String>("users")
        .mode(Mode::Invalidation)
        .open()
        .await
        .expect("b opens under the same mode a already advertises");

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

#[tokio::test]
async fn replicated_mode_fans_a_local_insert_out_to_the_peer() {
    let (cluster_a, cluster_b) = two_node_cluster("cluster-it-replicate").await;

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
    let mut events_b = cache_b.events();

    cache_a.insert(1, "hello".into()).await.expect("a inserts");

    let event = tokio::time::timeout(Duration::from_secs(10), events_b.recv())
        .await
        .expect("event arrives within the bound")
        .expect("event channel stays open");
    match event {
        Event::Created {
            key: 1,
            value,
            origin: Origin::Remote(_),
        } => assert_eq!(value, "hello"),
        other => panic!("unexpected event: {other:?}"),
    }
    assert_eq!(cache_b.get(&1).await, Some("hello".to_string()));

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

#[tokio::test]
async fn closing_a_replicated_cache_on_one_node_stops_it_serving_or_advertising() {
    let (cluster_a, cluster_b) = two_node_cluster("cluster-it-close-two-node").await;

    let cache_a = cluster_a
        .cache::<u32, String>("orders")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("a opens");
    let cache_b = cluster_b
        .cache::<u32, String>("orders")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("b opens");

    cache_a.insert(1, "seed".into()).await.expect("a inserts");
    tokio::time::timeout(Duration::from_secs(10), async {
        while !cache_b.contains_key(&1).await {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the seed value replicates to b before a closes");

    cache_a.close().await;
    assert!(
        !cluster_a
            .shards()
            .read()
            .expect("shard registry lock is never poisoned")
            .contains_key("orders"),
        "close removes a's shard from its own registry"
    );

    cache_b
        .insert(2, "after-close".into())
        .await
        .expect("b inserts after a closes");

    // Give any in-flight replicate traffic and a couple of anti-entropy
    // rounds a chance to have reached a, if it were still listening.
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(
        !cluster_a
            .shards()
            .read()
            .expect("shard registry lock is never poisoned")
            .contains_key("orders"),
        "a never re-registers the closed cache on its own"
    );

    wait_until(
        Duration::from_secs(10),
        "b stops seeing a advertise the closed cache within a few gossip intervals",
        async || {
            !cluster_b
                .advertised_cache_modes()
                .values()
                .any(|caches| caches.contains_key("orders"))
        },
    )
    .await;

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

#[tokio::test]
async fn replicated_mode_fans_a_sync_insert_out_to_the_peer() {
    let (cluster_a, cluster_b) = two_node_cluster("cluster-it-replicate-sync").await;

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
    let mut events_b = cache_b.events();

    cache_a.insert_sync(1, "hello".into()).expect("a inserts");

    let event = tokio::time::timeout(Duration::from_secs(10), events_b.recv())
        .await
        .expect("event arrives within the bound")
        .expect("event channel stays open");
    match event {
        Event::Created {
            key: 1,
            value,
            origin: Origin::Remote(_),
        } => assert_eq!(value, "hello"),
        other => panic!("unexpected event: {other:?}"),
    }
    assert_eq!(cache_b.get(&1).await, Some("hello".to_string()));

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

#[tokio::test]
async fn cache_api_batch_writes_and_reads_replicate_across_nodes() {
    let (cluster_a, cluster_b) = two_node_cluster("cluster-it-cache-api").await;

    let cache_a = cluster_a
        .cache::<u32, String>("catalog")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("a opens");
    let cache_b = cluster_b
        .cache::<u32, String>("catalog")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("b opens");

    cache_a
        .insert_many([
            (1, "one".to_string()),
            (2, "two".into()),
            (3, "three".into()),
        ])
        .await
        .expect("a inserts a batch");
    cache_a
        .insert_many_with_ttl(
            [(4, "brief".to_string()), (5, "brief".into())],
            Duration::from_millis(300),
        )
        .await
        .expect("a inserts an expiring batch");

    tokio::time::timeout(Duration::from_secs(10), async {
        while !(cache_b.contains_key(&3).await && cache_b.contains_key(&5).await) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("both batches replicate to b within the bound");
    let mut keys_b = cache_b.keys();
    keys_b.sort_unstable();
    assert_eq!(keys_b, [1, 2, 3, 4, 5]);

    let made = cache_b
        .get_or_insert_with(&6, async |_key| "six".to_string())
        .await
        .expect("b fills on miss");
    assert_eq!(made, "six");
    let kept = cache_b
        .get_or_insert_with(&6, async |_key| "never".to_string())
        .await
        .expect("b reads on hit");
    assert_eq!(kept, "six", "a hit never runs make");

    cache_a
        .remove_many([1, 2])
        .await
        .expect("a removes a batch");

    wait_until(
        Duration::from_secs(10),
        "the removal reaches b and the fill reaches a within the bound",
        async || {
            let removed_on_b = !cache_b.contains_key(&1).await && !cache_b.contains_key(&2).await;
            removed_on_b && cache_a.contains_key(&6).await
        },
    )
    .await;

    tokio::time::sleep(Duration::from_millis(1500)).await;
    for cache in [&cache_a, &cache_b] {
        let mut keys = cache.keys();
        keys.sort_unstable();
        assert_eq!(keys, [3, 6], "the expiring batch is gone on both nodes");
        assert_eq!(cache.entry_count().await, 2);
    }

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

#[tokio::test]
async fn per_entry_ttl_replicates_and_expires_on_the_peer() {
    let (cluster_a, cluster_b) = two_node_cluster("cluster-it-per-entry-ttl").await;

    let cache_a = cluster_a
        .cache::<u32, String>("sessions")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("a opens");
    let cache_b = cluster_b
        .cache::<u32, String>("sessions")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("b opens");

    cache_a
        .insert_with_ttl(1, "ephemeral".into(), Duration::from_millis(300))
        .await
        .expect("a inserts with ttl");
    cache_a
        .insert(2, "durable".into())
        .await
        .expect("a inserts");

    tokio::time::timeout(Duration::from_secs(10), async {
        while cache_b.get(&1).await.is_none() || cache_b.get(&2).await.is_none() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("both entries replicate to b within the bound");

    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(cache_a.get(&1).await, None, "origin expires its own copy");
    assert_eq!(
        cache_b.get(&1).await,
        None,
        "the replica expires from the deadline a stamped, with no TTL of its own"
    );
    assert_eq!(cache_b.get(&2).await.as_deref(), Some("durable"));

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

/// `Mode::Local` publishes no wire message, so this polls past a settle
/// window a real message would need, then asserts it never showed.
#[tokio::test]
async fn local_mode_never_leaks_a_write_to_the_peer() {
    let (cluster_a, cluster_b) = two_node_cluster("cluster-it-local-no-fan-out").await;

    let cache_a = cluster_a
        .cache::<u32, String>("scratch")
        .mode(Mode::Local)
        .open()
        .await
        .expect("a opens");
    let cache_b = cluster_b
        .cache::<u32, String>("scratch")
        .mode(Mode::Local)
        .open()
        .await
        .expect("b opens");

    cache_a
        .insert(1, "only-on-a".into())
        .await
        .expect("a inserts");

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        cache_b.get(&1).await,
        None,
        "Mode::Local must never fan a write out to peers"
    );
    assert_eq!(cache_a.get(&1).await, Some("only-on-a".to_string()));

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

#[tokio::test]
async fn invalidation_mode_drops_a_stale_remote_copy() {
    let (cluster_a, cluster_b) = two_node_cluster("cluster-it-invalidate").await;

    let cache_a = cluster_a
        .cache::<u32, String>("users")
        .mode(Mode::Invalidation)
        .open()
        .await
        .expect("a opens");
    let cache_b = cluster_b
        .cache::<u32, String>("users")
        .mode(Mode::Invalidation)
        .open()
        .await
        .expect("b opens");

    // B warms its own local copy first; Invalidation mode never
    // replicates values. The sleep guarantees A's HLC stamp is later.
    cache_b
        .insert(1, "stale".into())
        .await
        .expect("b warms locally");
    assert_eq!(cache_b.get(&1).await, Some("stale".to_string()));
    tokio::time::sleep(Duration::from_millis(5)).await;

    cache_a
        .insert(1, "fresh".into())
        .await
        .expect("a writes a newer version");

    wait_until(
        Duration::from_secs(10),
        "b's stale copy is invalidated within the bound",
        async || cache_b.get(&1).await.is_none(),
    )
    .await;

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

/// The invalidations node A queued for node B go out before A leaves, so B
/// serves none of the values A replaced.
#[tokio::test]
async fn shutdown_sends_the_invalidations_queued_for_a_peer_before_the_node_leaves() {
    const KEYS: u32 = 200;
    let (cluster_a, cluster_b) = two_node_cluster("cluster-it-invalidate-shutdown").await;
    let cache_a = cluster_a
        .cache::<u32, String>("users")
        .mode(Mode::Invalidation)
        .open()
        .await
        .expect("a opens");
    let cache_b = cluster_b
        .cache::<u32, String>("users")
        .mode(Mode::Invalidation)
        .open()
        .await
        .expect("b opens");

    for key in 0..KEYS {
        cache_b
            .insert(key, "stale".into())
            .await
            .expect("b holds a copy");
    }
    // The sleep guarantees A's stamps are later than B's.
    tokio::time::sleep(Duration::from_millis(5)).await;
    for key in 0..KEYS {
        cache_a
            .insert(key, "fresh".into())
            .await
            .expect("a replaces the value");
    }
    cluster_a.shutdown().await;

    wait_until(
        Duration::from_secs(10),
        "b drops every copy a replaced before it left",
        async || {
            for key in 0..KEYS {
                if cache_b.get(&key).await.is_some() {
                    return false;
                }
            }
            true
        },
    )
    .await;
    cluster_b.shutdown().await;
}

#[tokio::test]
async fn invalidation_fills_on_two_nodes_keep_both_copies() {
    let (cluster_a, cluster_b) = two_node_cluster("cluster-it-invalidate-fill").await;

    let cache_a = cluster_a
        .cache::<u32, String>("users")
        .mode(Mode::Invalidation)
        .open()
        .await
        .expect("a opens");
    let cache_b = cluster_b
        .cache::<u32, String>("users")
        .mode(Mode::Invalidation)
        .open()
        .await
        .expect("b opens");

    let load =
        async |_key: &u32| -> Result<String, std::convert::Infallible> { Ok("row".to_string()) };
    cache_b.get_or_load(&1, load).await.expect("b fills");
    cache_b
        .insert(2, "marker".into())
        .await
        .expect("b holds the marker");
    tokio::time::sleep(Duration::from_millis(5)).await;

    // A fills the same key later, then writes the marker. The marker's
    // invalidation reaches B behind anything the fill sent, over the
    // same ordered link.
    cache_a.get_or_load(&1, load).await.expect("a fills");
    cache_a
        .insert(2, "newer".into())
        .await
        .expect("a writes the marker");
    wait_until(
        Duration::from_secs(10),
        "b receives the marker's invalidation",
        async || cache_b.get(&2).await.is_none(),
    )
    .await;

    assert_eq!(
        cache_b.get(&1).await,
        Some("row".to_string()),
        "a's fill left b's copy in place"
    );
    assert_eq!(cache_a.get(&1).await, Some("row".to_string()));

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

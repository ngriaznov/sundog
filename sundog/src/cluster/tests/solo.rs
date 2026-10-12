//! Tests for a cluster of one node: the local cache API, mode validation on open and the lifecycle.

use super::*;
use crate::error::CacheError;

#[tokio::test]
async fn single_node_cluster_forms_with_no_seeds_and_local_cache_round_trips() {
    let cluster = solo_cluster("cluster-it-single").await;

    let cache = cluster
        .cache::<u32, String>("solo")
        .mode(Mode::Local)
        .open()
        .await
        .expect("open succeeds");
    cache.insert(1, "a".into()).await.expect("insert");
    assert_eq!(cache.get(&1).await, Some("a".to_string()));

    cluster.shutdown().await;
}

#[tokio::test]
async fn cache_insert_sync_and_get_sync_round_trip_with_no_await() {
    let cluster = solo_cluster("cluster-it-insert-sync").await;
    let cache = cluster
        .cache::<u32, String>("solo")
        .mode(Mode::Local)
        .open()
        .await
        .expect("open succeeds");

    cache.insert_sync(1, "a".into()).expect("insert_sync");
    assert_eq!(cache.get_sync(&1), Some("a".to_string()));

    cluster.shutdown().await;
}

#[tokio::test]
async fn cache_contains_key_sync_and_remove_sync_agree_with_their_async_twins() {
    let cluster = solo_cluster("cluster-it-contains-remove-sync").await;
    let cache = cluster
        .cache::<u32, String>("solo")
        .mode(Mode::Local)
        .open()
        .await
        .expect("open succeeds");

    assert!(!cache.contains_key_sync(&1), "missing key");
    cache.insert(1, "a".into()).await.expect("insert");
    assert!(cache.contains_key_sync(&1), "present after insert");

    cache.remove_sync(&1).expect("remove_sync");
    assert!(!cache.contains_key(&1).await, "gone after remove_sync");

    cluster.shutdown().await;
}

#[tokio::test]
async fn cache_for_each_key_visits_the_same_set_as_keys() {
    let cluster = solo_cluster("cluster-it-for-each-key").await;
    let cache = cluster
        .cache::<u32, String>("solo")
        .mode(Mode::Local)
        .open()
        .await
        .expect("open succeeds");

    for k in 0..5u32 {
        cache.insert(k, k.to_string()).await.expect("insert");
    }
    cache.remove(&2).await.expect("remove");

    let mut visited = Vec::new();
    cache.for_each_key(|k| visited.push(k));
    visited.sort_unstable();
    let mut keys = cache.keys();
    keys.sort_unstable();
    assert_eq!(visited, keys);

    cluster.shutdown().await;
}

#[tokio::test]
async fn open_rejects_replicated_mode_combined_with_a_finite_max_capacity() {
    let cluster = solo_cluster("cluster-it-replicated-capacity-guard").await;

    match cluster
        .cache::<u32, String>("bounded")
        .mode(Mode::Replicated)
        .max_capacity(10)
        .open()
        .await
    {
        Err(CacheError::ReplicatedWithLocalEviction { cache }) => {
            assert_eq!(cache, "bounded");
        }
        other => panic!(
            "expected ReplicatedWithLocalEviction, got {:?}",
            other.map(|_| ())
        ),
    }

    cluster.shutdown().await;
}

#[tokio::test]
async fn open_rejects_replicated_mode_combined_with_tti() {
    let cluster = solo_cluster("cluster-it-replicated-tti-guard").await;

    let err = cluster
        .cache::<u32, String>("idle-bounded")
        .mode(Mode::Replicated)
        .tti(Duration::from_secs(60))
        .open()
        .await
        .expect_err("Replicated + tti must be rejected");
    assert!(matches!(
        err,
        CacheError::ReplicatedWithLocalEviction { .. }
    ));

    cluster.shutdown().await;
}

#[tokio::test]
async fn cache_and_cluster_debug_impls_surface_identifying_fields() {
    let cluster = solo_cluster("cluster-it-debug-fmt").await;
    let cache = cluster
        .cache::<u32, String>("debug-fmt")
        .open()
        .await
        .expect("open succeeds");

    let cluster_fmt = format!("{cluster:?}");
    assert!(cluster_fmt.contains("Cluster"));
    let cache_fmt = format!("{cache:?}");
    assert!(cache_fmt.contains("debug-fmt"));

    cluster.shutdown().await;
}

#[tokio::test]
async fn cache_name_returns_the_opened_name() {
    let cluster = solo_cluster("cluster-it-cache-name").await;

    let cache = cluster
        .cache::<u32, String>("named-cache")
        .open()
        .await
        .expect("open succeeds");
    assert_eq!(cache.name(), "named-cache");

    cluster.shutdown().await;
}

#[tokio::test]
async fn local_cache_with_weigher_and_ttl_stays_bounded_and_expires() {
    let cluster = solo_cluster("cluster-it-weigher-ttl").await;

    let cache = cluster
        .cache::<u32, Vec<u8>>("weighed")
        .mode(Mode::Local)
        .max_capacity(50)
        .weigher(|_key: &u32, value: &Vec<u8>| u32::try_from(value.len()).unwrap_or(u32::MAX))
        .ttl(Duration::from_millis(150))
        .open()
        .await
        .expect("open succeeds");

    // Checked before the burst below, so capacity eviction never
    // competes with it for survival.
    cache.insert(999, vec![0u8; 5]).await.expect("insert");
    assert_eq!(cache.get(&999).await, Some(vec![0u8; 5]));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        cache.get(&999).await,
        None,
        "the entry expires by its own TTL"
    );

    // 40 entries at weight 5 each (200) against a 50-unit cap: at most 10
    // survive.
    for i in 0..40u32 {
        cache.insert(i, vec![0u8; 5]).await.expect("insert");
    }
    assert!(
        cache.entry_count().await <= 10,
        "a weigher-and-capacity cache stays within its weight bound after a burst of inserts"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn reopening_the_same_cache_name_fails_cleanly() {
    let cluster = solo_cluster("cluster-it-reopen").await;

    let _first = cluster
        .cache::<u32, String>("dup")
        .open()
        .await
        .expect("first open succeeds");
    match cluster.cache::<u32, String>("dup").open().await {
        Err(CacheError::AlreadyOpen { cache }) => assert_eq!(cache, "dup"),
        other => panic!("expected AlreadyOpen, got {}", other.is_ok()),
    }

    cluster.shutdown().await;
}

#[tokio::test]
async fn closing_a_cache_clears_its_local_mode_and_warmth_and_frees_the_name() {
    let cluster = solo_cluster("cluster-it-close-clears-state").await;

    let name = SmolStr::new("closable");
    let cache = cluster
        .cache::<u32, String>("closable")
        .open()
        .await
        .expect("open succeeds");
    assert!(cluster.is_warm(&name), "an Invalidation cache opens warm");
    assert!(
        cluster
            .inner
            .local_modes
            .read()
            .expect("local-modes lock is never poisoned")
            .contains_key(&name),
        "open() records the cache's local mode"
    );

    cache.close().await;

    assert!(!cluster.is_warm(&name), "close forgets the cache's warmth");
    assert!(
        !cluster
            .inner
            .local_modes
            .read()
            .expect("local-modes lock is never poisoned")
            .contains_key(&name),
        "close removes the cache's local mode"
    );
    assert!(
        !cluster
            .shards()
            .read()
            .expect("shard registry lock is never poisoned")
            .contains_key(&name),
        "close removes the shard from the registry"
    );

    let _reopened = cluster
        .cache::<u32, String>("closable")
        .open()
        .await
        .expect("closing frees the name for a fresh open");

    cluster.shutdown().await;
}

#[tokio::test]
async fn cache_handle_survives_shutdown_without_panicking() {
    let cluster = solo_cluster("cluster-it-shutdown").await;
    let cache = cluster
        .cache::<u32, String>("post-shutdown")
        .open()
        .await
        .expect("open succeeds");

    cluster.shutdown().await;

    cache
        .insert(1, "still local".into())
        .await
        .expect("a local insert after shutdown neither errors nor panics");
    assert_eq!(cache.get(&1).await, Some("still local".to_string()));
}

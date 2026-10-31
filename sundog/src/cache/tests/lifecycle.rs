//! Tests of leaving, closing and reopening a cache.

use super::*;

/// Unlike a crash, a graceful leave hands a key on: when both of its
/// owners shut down one after the other, the remaining node inherits
/// the key and still reads it.
#[tokio::test]
async fn both_owners_leaving_gracefully_hand_the_key_to_the_remaining_node() {
    // The default failure detector backs up each departure: a node whose
    // runtime stalls through its departure notice still leaves the others'
    // views once it goes quiet.
    let ((a, cache_a), (b, cache_b), (c, _cache_c), unowned_key) = three_node_distributed_on(
        "cache-it-graceful-hand-off",
        "prices",
        loopback_config(),
        loopback_config(),
    )
    .await;
    cache_b
        .insert(unowned_key, "value".to_string())
        .await
        .expect("insert");
    wait_until(
        Duration::from_secs(10),
        "the value reaches its owners",
        async || matches!(cache_a.fetch(&unowned_key).await, Ok(Some(value)) if value == "value"),
    )
    .await;

    b.shutdown().await;
    c.shutdown().await;

    wait_until(
        Duration::from_secs(10),
        "a owns the key alone and holds it",
        async || cache_a.owners_of(&unowned_key) == vec![a.node_id()],
    )
    .await;
    assert_eq!(
        cache_a.get(&unowned_key).await,
        Some("value".to_string()),
        "the hand-off put the value on a before its last owner left"
    );

    a.shutdown().await;
}

#[tokio::test]
async fn close_then_reopen_the_same_name_succeeds() {
    let cluster = Cluster::builder("cache-it-close-reopen")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let cache = cluster
        .cache::<u32, String>("scratch")
        .open()
        .await
        .expect("first open succeeds");
    cache.insert(1, "a".into()).await.expect("insert");
    cache.close().await;

    let reopened = cluster
        .cache::<u32, String>("scratch")
        .open()
        .await
        .expect("closing frees the name for a fresh open");
    assert_eq!(
        reopened.get(&1).await,
        None,
        "the reopened cache starts empty, not resuming the closed shard's state"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn close_returns_only_once_every_background_task_has_stopped() {
    let cluster = Cluster::builder("cache-it-close-waits")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");
    let cache = cluster
        .cache::<u32, String>("orders")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("open succeeds");
    let tasks = cache.tasks.clone();
    assert!(
        !tasks.is_empty(),
        "a Replicated cache runs background tasks"
    );

    cache.close().await;

    assert!(tasks.is_closed() && tasks.is_empty());
    let caches = cluster.health().caches;
    assert!(caches.is_empty(), "{caches:?}");
    cluster.shutdown().await;
}

#[tokio::test]
async fn closing_one_clone_leaves_another_clone_usable() {
    let cluster = Cluster::builder("cache-it-close-clone")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let cache = cluster
        .cache::<u32, String>("shared")
        .open()
        .await
        .expect("open succeeds");
    let surviving = cache.clone();

    cache.close().await;

    surviving
        .insert(2, "still here".into())
        .await
        .expect("a surviving clone keeps writing locally after another clone closes");
    assert_eq!(
        surviving.get(&2).await,
        Some("still here".to_string()),
        "a surviving clone keeps reading locally after another clone closes"
    );
    assert!(
        surviving.shard.fan_out_queue().drain().is_empty(),
        "a detached clone queues nothing for a fan-out task that no longer runs"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn a_forwarded_write_after_close_fails_with_closed_instead_of_vanishing() {
    let ((a, cache_a), (b, cache_b), (c, cache_c), key_a_does_not_own) =
        three_node_distributed("distributed-write-after-close", "write-after-close").await;
    let owned_by_a = (0..1_000_000u32)
        .find(|key| cache_a.owners_of(key).contains(&a.node_id()))
        .expect("a owns some bucket");
    let late_handle = cache_a.clone();
    cache_a.close().await;
    assert!(
        matches!(
            late_handle
                .insert(key_a_does_not_own, "late".to_string())
                .await,
            Err(CacheError::Closed { .. })
        ),
        "a forwarded write after close is refused"
    );
    assert!(matches!(
        late_handle.remove(&key_a_does_not_own).await,
        Err(CacheError::Closed { .. })
    ));
    late_handle
        .insert(owned_by_a, "detached".to_string())
        .await
        .expect("a write this node applies itself still lands, detached");
    assert_eq!(
        late_handle.get(&owned_by_a).await.as_deref(),
        Some("detached")
    );

    cache_c.close().await;
    cache_b.close().await;
    c.shutdown().await;
    b.shutdown().await;
    a.shutdown().await;
}

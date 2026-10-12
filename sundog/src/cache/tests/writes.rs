//! Tests of lifetimes, writer ids and merge coalescing.

use super::*;

#[tokio::test]
async fn expire_touch_persist_and_ttl_of_on_one_node() {
    let cluster = Cluster::builder("cache-it-ttl-surface")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node builds");
    let cache = cluster
        .cache::<u32, String>("sessions")
        .mode(Mode::Local)
        .ttl(Duration::from_secs(600))
        .open()
        .await
        .expect("opens");
    cache.insert(1, "a".into()).await.expect("insert");
    let left = |cache: &Cache<u32, String>| match cache.ttl_of(&1) {
        Some(Ttl::Remaining(left)) => left,
        other => panic!("a remaining lifetime, got {other:?}"),
    };
    assert!(left(&cache) > Duration::from_secs(590));

    assert!(
        cache
            .expire(&1, Duration::from_secs(30))
            .await
            .expect("expire")
    );
    assert!(left(&cache) <= Duration::from_secs(30));
    assert!(cache.touch(&1).await.expect("touch"));
    assert!(
        left(&cache) > Duration::from_secs(590),
        "touch restarts the default"
    );
    assert!(cache.persist(&1).await.expect("persist"));
    assert_eq!(cache.ttl_of(&1), Some(Ttl::Never));
    assert_eq!(cache.get(&1).await, Some("a".to_string()));

    assert!(
        !cache
            .expire(&2, Duration::from_secs(1))
            .await
            .expect("expire")
    );
    assert_eq!(cache.ttl_of(&2), None);
    assert!(cache.expire(&1, Duration::ZERO).await.expect("expire"));
    assert_eq!(cache.get(&1).await, None);

    cluster.shutdown().await;
}

#[tokio::test]
async fn a_new_lifetime_replicates_to_every_replica() {
    let name = "cache-it-ttl-replicated";
    let a = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("a builds");
    let b = Cluster::builder(name)
        .seeds([a.local_gossip_addr()])
        .config(loopback_config())
        .build()
        .await
        .expect("b builds");
    wait_for_peer_count(&a, 1).await;
    wait_for_peer_count(&b, 1).await;
    let open = |cluster: &Cluster| {
        cluster
            .cache::<u32, String>("sessions")
            .mode(Mode::Replicated)
            .open()
    };
    let cache_a = open(&a).await.expect("a opens");
    let cache_b = open(&b).await.expect("b opens");

    cache_a.insert(1, "a".into()).await.expect("insert");
    wait_until(Duration::from_secs(10), "b holds the entry", async || {
        cache_b.ttl_of(&1) == Some(Ttl::Never)
    })
    .await;

    assert!(
        cache_a
            .expire(&1, Duration::from_secs(60))
            .await
            .expect("expire")
    );
    wait_until(
        Duration::from_secs(10),
        "b takes the new lifetime",
        async || {
            matches!(cache_b.ttl_of(&1), Some(Ttl::Remaining(left))
                if left <= Duration::from_secs(60) && left > Duration::from_secs(50))
        },
    )
    .await;
    assert!(cache_b.persist(&1).await.expect("persist"));
    wait_until(
        Duration::from_secs(10),
        "a takes b's persist back",
        async || cache_a.ttl_of(&1) == Some(Ttl::Never),
    )
    .await;
    assert_eq!(cache_a.get(&1).await, Some("a".to_string()));

    b.shutdown().await;
    a.shutdown().await;
}

/// `a` owns none of the key's part, so it asks an owner for the record
/// and forwards the re-arm: both owners take the lifetime and `a` still
/// holds nothing. A key no owner holds answers `false`.
#[tokio::test]
async fn expire_from_a_node_that_does_not_own_the_key_reaches_its_owners() {
    let ((a, cache_a), (b, cache_b), (c, cache_c), unowned_key) =
        three_node_distributed("cache-it-ttl-forward", "prices").await;
    cache_b
        .insert(unowned_key, "value".to_string())
        .await
        .expect("insert");
    wait_until(
        Duration::from_secs(10),
        "both owners hold the value",
        async || cache_b.ttl_of(&unowned_key).is_some() && cache_c.ttl_of(&unowned_key).is_some(),
    )
    .await;

    assert!(
        cache_a
            .expire(&unowned_key, Duration::from_secs(60))
            .await
            .expect("expire")
    );
    let rearmed = |cache: &Cache<u32, String>| {
        matches!(cache.ttl_of(&unowned_key), Some(Ttl::Remaining(left))
            if left <= Duration::from_secs(60) && left > Duration::from_secs(50))
    };
    wait_until(
        Duration::from_secs(10),
        "both owners take the new lifetime",
        async || rearmed(&cache_b) && rearmed(&cache_c),
    )
    .await;
    assert_eq!(cache_a.ttl_of(&unowned_key), None, "a still holds no copy");
    assert_eq!(
        cache_a.fetch(&unowned_key).await.expect("fetch"),
        Some("value".to_string())
    );

    let absent = (unowned_key + 1..u32::MAX)
        .find(|k| !cache_a.owners_of(k).contains(&a.node_id()))
        .expect("another key a does not own");
    assert!(
        !cache_a.persist(&absent).await.expect("the owners answer"),
        "no owner holds the key"
    );

    c.shutdown().await;
    b.shutdown().await;
    a.shutdown().await;
}

#[tokio::test]
async fn writer_id_pairs_the_node_with_the_cluster_incarnation() {
    let cluster = Cluster::builder("cache-it-writer-id")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");
    let cache = cluster
        .cache::<u32, PnCounter>("counters")
        .resolver(Arc::new(PnCounterResolver))
        .open()
        .await
        .expect("open succeeds");

    let writer = cache.writer_id();
    assert_eq!(
        writer,
        WriterId::new(cluster.node_id(), cluster.local_incarnation()),
        "Cache::writer_id pairs this node's id with the cluster's own \
         membership incarnation, the same pairing WriterId::new takes"
    );

    cache.close().await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn writer_id_is_stable_across_clones_and_repeated_calls() {
    let cluster = Cluster::builder("cache-it-writer-id-stable")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");
    let cache = cluster
        .cache::<u32, PnCounter>("counters")
        .resolver(Arc::new(PnCounterResolver))
        .open()
        .await
        .expect("open succeeds");
    let clone = cache.clone();

    assert_eq!(
        cache.writer_id(),
        cache.writer_id(),
        "two calls on the same handle agree"
    );
    assert_eq!(
        cache.writer_id(),
        clone.writer_id(),
        "a clone shares the same underlying cluster membership, so it \
         reports the identical writer identity, never a fresh one"
    );

    cache.close().await;
    cluster.shutdown().await;
}

/// The end-to-end reason `Cache::writer_id` exists: a value merged under
/// it round-trips through the exact merging path CRDT compaction relies
/// on, exercised at the `Cache` layer rather than only unit-tested on
/// `WriterId` itself.
#[tokio::test]
async fn a_value_merged_under_writer_id_reads_back_correctly() {
    let cluster = Cluster::builder("cache-it-writer-id-merge")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");
    let cache = cluster
        .cache::<u32, PnCounter>("counters")
        .resolver(Arc::new(PnCounterResolver))
        .open()
        .await
        .expect("open succeeds");

    cache
        .merge(1, PnCounter::local_delta(cache.writer_id(), 5))
        .await
        .expect("merge");
    assert_eq!(cache.get(&1).await.map(|c| c.value()), Some(5));

    cache.close().await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn merge_with_a_zero_window_applies_immediately() {
    let cluster = Cluster::builder("cache-it-merge-zero-window")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");
    let cache = cluster
        .cache::<u32, PnCounter>("counters")
        .resolver(Arc::new(PnCounterResolver))
        .open()
        .await
        .expect("open succeeds");
    let writer = cache.writer_id();

    cache
        .merge(1, PnCounter::local_delta(writer, 1))
        .await
        .expect("merge");
    assert_eq!(
        cache.get(&1).await.map(|c| c.value()),
        Some(1),
        "a zero coalesce window (the default) applies every merge call at once, \
         like insert"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn merge_within_a_window_coalesces_into_one_apply_and_one_event() {
    let cluster = Cluster::builder("cache-it-merge-coalesce-window")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");
    let window = Duration::from_millis(150);
    let cache = cluster
        .cache::<u32, PnCounter>("counters")
        .resolver(Arc::new(PnCounterResolver))
        .merge_coalesce_window(window)
        .open()
        .await
        .expect("open succeeds");
    let writer = cache.writer_id();
    let mut events = cache.events();

    cache
        .merge(1, PnCounter::local_delta(writer, 1))
        .await
        .expect("merge 1");
    cache
        .merge(1, PnCounter::local_delta(writer, 2))
        .await
        .expect("merge 2");
    cache
        .merge(1, PnCounter::local_delta(writer, 3))
        .await
        .expect("merge 3");

    assert_eq!(
        cache.get(&1).await,
        None,
        "a pending coalesced fold is invisible to get until its window flushes"
    );
    assert!(
        events.try_recv().is_err(),
        "nothing applies, so nothing publishes, before the window elapses"
    );

    tokio::time::sleep(window * 3).await;

    assert_eq!(
        cache.get(&1).await.map(|c| c.value()),
        Some(3),
        "the coalesced fold of all three calls lands as one write once the window \
         elapses"
    );
    match events
        .recv()
        .await
        .expect("exactly one event for the whole window")
    {
        Event::Created { key, .. } => assert_eq!(key, 1),
        other => panic!("expected Event::Created for the window's one apply, got {other:?}"),
    }
    assert!(
        events.try_recv().is_err(),
        "three merge calls in one window publish exactly one event, not three"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn close_flushes_a_pending_coalesced_merge() {
    let cluster = Cluster::builder("cache-it-merge-close-flush")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");
    let cache = cluster
        .cache::<u32, PnCounter>("counters")
        .resolver(Arc::new(PnCounterResolver))
        .merge_coalesce_window(Duration::from_secs(60))
        .open()
        .await
        .expect("open succeeds");
    let survivor = cache.clone();
    let writer = cache.writer_id();

    cache
        .merge(1, PnCounter::local_delta(writer, 7))
        .await
        .expect("merge");
    assert_eq!(
        survivor.get(&1).await,
        None,
        "still pending: the 60s window has not elapsed"
    );

    cache.close().await;

    assert_eq!(
        survivor.get(&1).await.map(|c| c.value()),
        Some(7),
        "close flushes whatever was still pending, regardless of the window"
    );

    cluster.shutdown().await;
}

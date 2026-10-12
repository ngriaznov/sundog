//! Tests of the registered loader, batch windows and refresh-ahead.

use super::*;

#[tokio::test]
async fn load_and_load_many_read_through_the_registered_loader() {
    let cluster = Cluster::builder("cache-it-load")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node builds");
    let calls = Arc::new(std::sync::Mutex::new(Vec::<Vec<u32>>::new()));
    let cache = cluster
        .cache::<u32, String>("prices")
        .mode(Mode::Local)
        .ttl(Duration::from_secs(600))
        .batch_loader({
            let calls = Arc::clone(&calls);
            move |keys: Vec<u32>| {
                let calls = Arc::clone(&calls);
                async move {
                    let mut sorted = keys.clone();
                    sorted.sort_unstable();
                    calls.lock().expect("calls lock").push(sorted);
                    Ok::<_, SourceDown>(
                        keys.into_iter()
                            .filter(|key| key.is_multiple_of(2))
                            .map(|key| (key, format!("v{key}")))
                            .collect::<HashMap<_, _>>(),
                    )
                }
            }
        })
        .open()
        .await
        .expect("opens");

    assert_eq!(cache.load(&2).await.expect("load"), Some("v2".to_string()));
    assert_eq!(cache.get(&2).await, Some("v2".to_string()));
    assert!(
        matches!(cache.ttl_of(&2), Some(Ttl::Remaining(left)) if left > Duration::from_secs(590)),
        "a loaded value takes the cache's default TTL"
    );
    assert_eq!(cache.load(&3).await.expect("load"), None);
    assert_eq!(
        cache.load_many([2, 3, 4, 6]).await.expect("load_many"),
        HashMap::from([
            (2, "v2".to_string()),
            (4, "v4".to_string()),
            (6, "v6".to_string())
        ])
    );
    assert_eq!(
        calls.lock().expect("calls lock").clone(),
        vec![vec![2], vec![3], vec![3, 4, 6]],
        "a hit never reaches the loader; a key the source lacks is asked again"
    );

    let single = cluster
        .cache::<u32, String>("names")
        .mode(Mode::Local)
        .loader(|key: u32| async move {
            if key == 0 {
                Err(SourceDown)
            } else {
                Ok((key < 10).then(|| format!("n{key}")))
            }
        })
        .open()
        .await
        .expect("opens");
    assert_eq!(single.load(&1).await.expect("load"), Some("n1".to_string()));
    assert_eq!(single.load(&11).await.expect("load"), None);
    assert!(matches!(single.load(&0).await, Err(CacheError::Loader(_))));

    let plain = cluster
        .cache::<u32, String>("plain")
        .mode(Mode::Local)
        .open()
        .await
        .expect("opens");
    assert!(matches!(
        plain.load(&1).await,
        Err(CacheError::NoLoader { cache }) if cache == "plain"
    ));
    assert!(matches!(
        cluster
            .cache::<u32, String>("windowed")
            .batch_window(Duration::from_millis(5), 10)
            .open()
            .await,
        Err(CacheError::InvalidLoadConfig { cache, .. }) if cache == "windowed"
    ));

    cluster.shutdown().await;
}

#[tokio::test]
async fn a_batch_window_groups_misses_from_concurrent_loads() {
    let cluster = Cluster::builder("cache-it-load-window")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node builds");
    let calls = Arc::new(std::sync::Mutex::new(Vec::<Vec<u32>>::new()));
    let cache = cluster
        .cache::<u32, String>("prices")
        .mode(Mode::Local)
        .batch_loader({
            let calls = Arc::clone(&calls);
            move |keys: Vec<u32>| {
                let calls = Arc::clone(&calls);
                async move {
                    let mut sorted = keys.clone();
                    sorted.sort_unstable();
                    calls.lock().expect("calls lock").push(sorted);
                    Ok::<_, SourceDown>(
                        keys.into_iter()
                            .map(|key| (key, format!("v{key}")))
                            .collect::<HashMap<_, _>>(),
                    )
                }
            }
        })
        .batch_window(Duration::from_millis(200), 3)
        .open()
        .await
        .expect("opens");
    let (a, b, c) = tokio::join!(cache.load(&1), cache.load(&2), cache.load(&3));
    for (answer, key) in [(a, 1), (b, 2), (c, 3)] {
        assert_eq!(answer.expect("load"), Some(format!("v{key}")));
    }
    assert_eq!(
        calls.lock().expect("calls lock").clone(),
        vec![vec![1, 2, 3]],
        "three misses fill the batch and load in one call"
    );
    cluster.shutdown().await;
}

#[tokio::test]
async fn a_loaded_value_replicates_to_every_replica() {
    let name = "cache-it-load-replicated";
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
            .cache::<u32, String>("prices")
            .mode(Mode::Replicated)
            .loader(|key: u32| async move { Ok::<_, SourceDown>(Some(format!("v{key}"))) })
            .open()
    };
    let cache_a = open(&a).await.expect("a opens");
    let cache_b = open(&b).await.expect("b opens");

    assert_eq!(
        cache_a.load(&7).await.expect("load"),
        Some("v7".to_string())
    );
    wait_until(
        Duration::from_secs(10),
        "b holds the loaded value",
        async || cache_b.get(&7).await == Some("v7".to_string()),
    )
    .await;

    b.shutdown().await;
    a.shutdown().await;
}

#[tokio::test]
async fn concurrent_invalidation_loads_of_a_key_gather_into_one_loader_call() {
    let log = LoadLog::default();
    let nodes = three_loading_nodes("cache-it-load-gather", Mode::Invalidation, &log, None).await;
    let (answers, ()) = tokio::join!(
        futures::future::join_all(nodes.iter().map(|(_, cache)| cache.load(&7))),
        async {}
    );
    for answer in answers {
        assert_eq!(answer.expect("load"), Some("v7".to_string()));
    }
    assert_eq!(
        log.lock().expect("log lock").len(),
        1,
        "three nodes asking at once share one loader call: {:?}",
        log.lock().expect("log lock")
    );
    for (_, cache) in &nodes {
        assert_eq!(
            cache.get(&7).await,
            Some("v7".to_string()),
            "in Invalidation mode each node keeps what it loaded"
        );
    }
    let loads_before = log.lock().expect("log lock").len();
    let found = nodes[1].1.load_many([7, 8, 9]).await.expect("load_many");
    assert_eq!(found.len(), 3);
    assert_eq!(
        nodes[1].1.load(&7).await.expect("load"),
        Some("v7".to_string())
    );
    assert!(
        log.lock().expect("log lock").len() > loads_before,
        "the missing keys reach a loader"
    );
    shut_down_all(nodes).await;
}

#[tokio::test]
async fn a_distributed_load_runs_on_the_first_owner_and_stays_off_a_non_owner() {
    let log = LoadLog::default();
    let nodes = three_loading_nodes("cache-it-load-owner", Mode::distributed(), &log, None).await;
    let (a, cache_a) = &nodes[0];
    let key = (0..10_000u32)
        .find(|key| !cache_a.owners_of(key).contains(&a.node_id()))
        .expect("a key a does not own");
    let owners = cache_a.owners_of(&key);
    wait_until(
        CONVERGE,
        "every node agrees on the key's owners",
        async || {
            nodes
                .iter()
                .all(|(_, cache)| cache.owners_of(&key) == owners)
        },
    )
    .await;
    // A cold owner declines a load, which then runs on the next owner.
    wait_for_warm_owners(&nodes, key).await;

    assert_eq!(
        cache_a.load(&key).await.expect("load"),
        Some(format!("v{key}"))
    );
    assert_eq!(
        log.lock().expect("log lock").clone(),
        vec![(owners[0], vec![key])],
        "the key's first owner runs the one load"
    );
    assert_eq!(cache_a.get(&key).await, None, "a non-owner keeps no copy");
    wait_until(CONVERGE, "both owners hold the loaded value", async || {
        let mut held = 0;
        for (cluster, cache) in &nodes {
            if owners.contains(&cluster.node_id()) && cache.get(&key).await.is_some() {
                held += 1;
            }
        }
        held == 2
    })
    .await;
    assert_eq!(
        cache_a.load(&key).await.expect("load"),
        Some(format!("v{key}"))
    );
    assert_eq!(
        log.lock().expect("log lock").len(),
        1,
        "the owner answers the second read from its copy"
    );
    shut_down_all(nodes).await;
}

#[tokio::test]
async fn a_failing_remote_loader_answers_with_a_remote_loader_error() {
    let log = LoadLog::default();
    let nodes = three_loading_nodes(
        "cache-it-load-remote-failure",
        Mode::Invalidation,
        &log,
        Some(1),
    )
    .await;
    let (a, cache_a) = &nodes[0];
    let failing = nodes[1].0.node_id();
    let capable = cache_a.loader_peers();
    let key = (0..10_000u32)
        .find(|key| {
            let part = PartId::of_key(&encode_key(key).expect("u32 encodes"));
            load_target(Mode::Invalidation, part, a.node_id(), &[], &capable)
                == LoadTarget::Peer(failing)
        })
        .expect("a key whose loads gather on the failing node");
    let err = cache_a
        .load(&key)
        .await
        .expect_err("the remote loader fails");
    let CacheError::Loader(source) = &err else {
        panic!("a loader error, got {err:?}");
    };
    let remote = std::iter::successors(Some(source.as_ref() as &dyn std::error::Error), |err| {
        err.source()
    })
    .find_map(|err| err.downcast_ref::<RemoteLoaderError>())
    .expect("the error chain names the remote loader");
    assert_eq!(remote.node, failing);
    assert_eq!(remote.message, "source unavailable");
    assert_eq!(cache_a.get(&key).await, None);
    assert_eq!(
        log.lock().expect("log lock").clone(),
        vec![(failing, vec![key])],
        "a remote failure is not retried here"
    );
    shut_down_all(nodes).await;
}

/// A 10s TTL with refresh-ahead at 0.1: a key falls due 1s after its
/// write and expires 9s after that, so a stalled runner reads a due key
/// long before it expires.
const LONG_REFRESH: (Duration, f64) = (Duration::from_secs(10), 0.1);

/// `count` nodes with `name` open under `mode` with `lifetime`'s TTL and
/// refresh-ahead fraction, each with a batch loader that records its
/// calls in `log` and holds `v{key}.{n}`, `n` counting calls across all
/// nodes. Returns once every node sees the others advertise a loader
/// and, under `Mode::Distributed`, ranks them all in its ownership view.
async fn refreshing_nodes(
    name: &'static str,
    mode: Mode,
    count: usize,
    lifetime: (Duration, f64),
    log: &LoadLog,
) -> Vec<(Cluster, Cache<u32, String>)> {
    let calls = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut nodes: Vec<(Cluster, Cache<u32, String>)> = Vec::new();
    for index in 0..count {
        let builder = Cluster::builder(name).config(loopback_config());
        let cluster = match nodes.first() {
            None => builder.seeds(std::iter::empty()),
            Some((seed, _)) => builder.seeds([seed.local_gossip_addr()]),
        }
        .build()
        .await
        .expect("node builds");
        wait_for_peer_count(&cluster, index).await;
        let node = cluster.node_id();
        let (log, calls) = (Arc::clone(log), Arc::clone(&calls));
        let cache = cluster
            .cache::<u32, String>(name)
            .mode(mode)
            .ttl(lifetime.0)
            .refresh_ahead(lifetime.1)
            .batch_loader(move |keys: Vec<u32>| {
                let (log, calls) = (Arc::clone(&log), Arc::clone(&calls));
                async move {
                    let call = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                    let mut sorted = keys.clone();
                    sorted.sort_unstable();
                    log.lock().expect("log lock").push((node, sorted));
                    Ok::<_, SourceDown>(
                        keys.into_iter()
                            .map(|key| (key, format!("v{key}.{call}")))
                            .collect::<HashMap<_, _>>(),
                    )
                }
            })
            .open()
            .await
            .expect("opens");
        nodes.push((cluster, cache));
    }
    wait_until(
        Duration::from_secs(10),
        "every node sees the others advertise a loader",
        async || {
            nodes
                .iter()
                .all(|(_, cache)| cache.loader_peers().len() == count - 1)
        },
    )
    .await;
    wait_for_full_views(&nodes).await;
    nodes
}

#[tokio::test]
async fn refresh_ahead_keeps_a_read_key_alive_past_its_expiry_on_one_node() {
    let log = LoadLog::default();
    // A 5s TTL refreshed a fifth of the way in: a key falls due 1s
    // after its write and expires 4s after that, so a runner stalled
    // for up to 4s still reads it in time, and the 7s of reads below
    // outlive the lifetime.
    let lifetime = (Duration::from_secs(5), 0.2);
    let nodes = refreshing_nodes("cache-it-refresh-local", Mode::Local, 1, lifetime, &log).await;
    let cache = &nodes[0].1;
    assert_eq!(
        cache.load(&1).await.expect("load"),
        Some("v1.1".to_string())
    );
    let loaded_at = Instant::now();
    while loaded_at.elapsed() < Duration::from_secs(7) {
        assert!(
            cache.get(&1).await.is_some(),
            "a key read through its refresh window never expires"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // A reload restarts the 5s lifetime, so the next refresh point is
    // 1s later: over 7s of reads, the load and about six reloads, fewer
    // when the runner stalls between reads.
    let loads = log.lock().expect("log lock").len();
    assert!(
        (2..=8).contains(&loads),
        "about one reload a refresh interval, not one per read: {loads} loads"
    );
    shut_down_all(nodes).await;
}

#[tokio::test]
async fn refresh_ahead_reloads_a_replicated_key_once_on_its_refresher() {
    let log = LoadLog::default();
    let nodes = refreshing_nodes(
        "cache-it-refresh-replicated",
        Mode::Replicated,
        2,
        LONG_REFRESH,
        &log,
    )
    .await;
    let (a, cache_a) = &nodes[0];
    let (b, cache_b) = &nodes[1];
    let key = 7u32;
    assert_eq!(
        cache_a.load(&key).await.expect("load"),
        Some("v7.1".to_string())
    );
    wait_until(
        Duration::from_secs(5),
        "b holds the loaded value",
        async || cache_b.get(&key).await.is_some(),
    )
    .await;
    let part = PartId::of_key(&encode_key(&key).expect("u32 encodes"));
    let refresher = match load_target(
        Mode::Replicated,
        part,
        a.node_id(),
        &[],
        &[b.node_id()].into_iter().collect(),
    ) {
        LoadTarget::Here => a.node_id(),
        LoadTarget::Peer(peer) => peer,
    };

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    for _ in 0..20 {
        assert!(cache_a.get(&key).await.is_some());
        assert!(cache_b.get(&key).await.is_some());
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    wait_until(
        Duration::from_secs(5),
        "both nodes hold the reloaded value",
        async || {
            cache_a.get(&key).await == Some("v7.2".to_string())
                && cache_b.get(&key).await == Some("v7.2".to_string())
        },
    )
    .await;
    let log = log.lock().expect("log lock").clone();
    assert_eq!(
        log.len(),
        2,
        "one load and one reload for both nodes' reads: {log:?}"
    );
    assert_eq!(log[1], (refresher, vec![key]), "the refresher reloads");
    shut_down_all(nodes).await;
}

#[tokio::test]
async fn refresh_ahead_reloads_a_distributed_key_a_non_owner_fetches_on_its_owner() {
    let log = LoadLog::default();
    let nodes = refreshing_nodes(
        "cache-it-refresh-distributed",
        Mode::distributed(),
        3,
        LONG_REFRESH,
        &log,
    )
    .await;
    let (a, cache_a) = &nodes[0];
    let key = (0..10_000u32)
        .find(|key| !cache_a.owners_of(key).contains(&a.node_id()))
        .expect("a key a does not own");
    let owners = cache_a.owners_of(&key);
    wait_until(
        CONVERGE,
        "every node agrees on the key's owners",
        async || {
            nodes
                .iter()
                .all(|(_, cache)| cache.owners_of(&key) == owners)
        },
    )
    .await;
    assert!(cache_a.load(&key).await.expect("load").is_some());
    let first = log.lock().expect("log lock").len();

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert!(cache_a.fetch(&key).await.expect("fetch").is_some());
    wait_until(
        Duration::from_secs(5),
        "the first owner reloads the key a non-owner read",
        async || log.lock().expect("log lock").len() > first,
    )
    .await;
    assert_eq!(
        log.lock().expect("log lock")[first],
        (owners[0], vec![key]),
        "the first owner reloads"
    );
    shut_down_all(nodes).await;
}

/// `explain` inside a key's refresh window is not a read: asked of every
/// node, the owners that hold the entry and the non-owner whose `fetch`
/// hints the owner included, it queues no reload and no loader runs,
/// where the `fetch` of the same key right after asks for one.
#[tokio::test]
async fn explain_does_not_start_a_refresh_inside_the_refresh_window() {
    let log = LoadLog::default();
    let nodes = refreshing_nodes(
        "cache-it-explain-refresh",
        Mode::distributed(),
        3,
        LONG_REFRESH,
        &log,
    )
    .await;
    let (a, cache_a) = &nodes[0];
    let key = (0..10_000u32)
        .find(|key| !cache_a.owners_of(key).contains(&a.node_id()))
        .expect("a key a does not own");
    let owners = cache_a.owners_of(&key);
    wait_until(
        CONVERGE,
        "every node agrees on the key's owners",
        async || {
            nodes
                .iter()
                .all(|(_, cache)| cache.owners_of(&key) == owners)
        },
    )
    .await;
    assert!(cache_a.load(&key).await.expect("load").is_some());
    let loaded = log.lock().expect("log lock").len();
    wait_for_local_record(
        &[cache_of(&nodes, owners[0]), cache_of(&nodes, owners[1])],
        key,
        LocalRecord::is_live,
    )
    .await;

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    for (_, cache) in &nodes {
        let explanation = cache.explain(&key).await.expect("explains");
        assert!(
            explanation.source.returns_value(),
            "the entry is live: {explanation}"
        );
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        log.lock().expect("log lock").len(),
        loaded,
        "explain asks for no reload"
    );

    assert!(cache_a.fetch(&key).await.expect("fetch").is_some());
    wait_until(
        Duration::from_secs(5),
        "the first owner reloads the key a non-owner read",
        async || log.lock().expect("log lock").len() > loaded,
    )
    .await;
    assert_eq!(
        log.lock().expect("log lock")[loaded],
        (owners[0], vec![key]),
        "the first owner reloads"
    );
    shut_down_all(nodes).await;
}

#[tokio::test]
async fn refresh_ahead_needs_a_loader_a_ttl_and_a_mode_that_carries_values() {
    let cluster = Cluster::builder("cache-it-refresh-config")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node builds");
    let loader = |key: u32| async move { Ok::<_, SourceDown>(Some(format!("v{key}"))) };
    let reason = |result: Result<Cache<u32, String>, CacheError>| match result {
        Err(CacheError::InvalidLoadConfig { reason, .. }) => reason,
        other => panic!("an invalid load config, got {other:?}"),
    };
    assert_eq!(
        reason(
            cluster
                .cache::<u32, String>("no-ttl")
                .loader(loader)
                .refresh_ahead(0.8)
                .open()
                .await
        ),
        "refresh_ahead needs a ttl"
    );
    assert_eq!(
        reason(
            cluster
                .cache::<u32, String>("invalidation")
                .mode(Mode::Invalidation)
                .ttl(Duration::from_secs(1))
                .loader(loader)
                .refresh_ahead(0.8)
                .open()
                .await
        ),
        "refresh_ahead does not apply to Mode::Invalidation"
    );
    assert_eq!(
        reason(
            cluster
                .cache::<u32, String>("whole")
                .mode(Mode::Local)
                .ttl(Duration::from_secs(1))
                .loader(loader)
                .refresh_ahead(1.0)
                .open()
                .await
        ),
        "refresh_ahead takes a fraction strictly between 0 and 1"
    );
    cluster.shutdown().await;
}

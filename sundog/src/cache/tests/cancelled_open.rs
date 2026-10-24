//! A dropped `CacheBuilder::open` leaves nothing behind: the cache it registered closes, and an
//! `open` of the same name waits for that close.

use super::startup::dead_gossip_addr;
use super::*;
use crate::cache::open::{OpenGuard, reserve_name};

/// How long a cancelled open runs before its timeout drops it: past the registry insert, inside
/// the wait for a donor or a pull.
const CANCEL_AFTER: Duration = Duration::from_millis(300);

/// A cluster whose only seed answers nobody and whose transfers give up after `budget`.
async fn lone_cluster_with_dead_seed(label: &'static str, budget: Duration) -> Cluster {
    let config = ClusterConfig {
        state_transfer_budget: budget,
        ..loopback_config()
    };
    Cluster::builder(label)
        .seeds([dead_gossip_addr().await])
        .config(config)
        .build()
        .await
        .expect("cluster builds")
}

/// A node `b` that knows a co-owner of `x` (`Mode::distributed()`, two owners) that has crashed.
/// The failure detector has not dropped the co-owner yet, so `b`'s open of `x` pulls from it and
/// waits out `budget`.
async fn node_beside_a_crashed_co_owner(label: &'static str, budget: Duration) -> Cluster {
    let config = ClusterConfig {
        state_transfer_budget: budget,
        ..loopback_config()
    };
    let a = Cluster::builder(label)
        .seeds(std::iter::empty())
        .config(config.clone())
        .build()
        .await
        .expect("a builds");
    let b = Cluster::builder(label)
        .seeds([a.local_gossip_addr()])
        .config(config)
        .build()
        .await
        .expect("b builds");
    wait_for_peer_count(&a, 1).await;
    wait_for_peer_count(&b, 1).await;
    let a_node = a.node_id();
    let cache_a = a
        .cache::<u32, String>("x")
        .mode(Mode::distributed())
        .open()
        .await
        .expect("a opens x");
    wait_until(
        Duration::from_secs(10),
        "b sees a advertise x",
        async || {
            b.advertised_cache_modes()
                .get(&a_node)
                .is_some_and(|caches| caches.contains_key("x"))
        },
    )
    .await;
    drop(cache_a);
    a.crash().await;
    b
}

/// Polls `cluster.health()` until it lists no cache named `name`.
async fn wait_until_unlisted(cluster: &Cluster, name: &str) {
    wait_until(
        Duration::from_secs(10),
        "the cancelled open's cache leaves health()",
        async || cluster.health().caches.iter().all(|c| c.name != name),
    )
    .await;
}

/// Checks `cluster.health()` lists no cache.
fn assert_nothing_listed(cluster: &Cluster) {
    let caches = cluster.health().caches;
    assert!(caches.is_empty(), "{caches:?}");
}

/// Opens `x` in `mode` and drops the open after `after`, returning whether it was still waiting.
async fn open_and_cancel(cluster: &Cluster, mode: Mode, after: Duration) -> bool {
    tokio::time::timeout(after, cluster.cache::<u32, String>("x").mode(mode).open())
        .await
        .is_err()
}

/// The retry of a cancelled `x` open is not refused, waits like the first and, cancelled in turn,
/// leaves `health()` with no entry for `x`.
async fn assert_retry_is_not_refused_and_leaves_nothing(cluster: &Cluster, mode: Mode) {
    assert!(
        open_and_cancel(cluster, mode, CANCEL_AFTER).await,
        "the open is still waiting and gets cancelled"
    );
    let retry = tokio::time::timeout(
        CANCEL_AFTER,
        cluster.cache::<u32, String>("x").mode(mode).open(),
    )
    .await;
    assert!(
        !matches!(retry, Ok(Err(_))),
        "retry after a cancelled open must not be refused: {:?}",
        retry.as_ref().map(|r| r.as_ref().err())
    );
    assert!(
        retry.is_err(),
        "the retry waits for its transfer, too, and is cancelled"
    );
    wait_until_unlisted(cluster, "x").await;
}

#[tokio::test]
async fn a_cancelled_replicated_open_frees_the_name_for_a_retry() {
    let cluster =
        lone_cluster_with_dead_seed("cache-it-cancelled-replicated", Duration::from_secs(5)).await;

    assert_retry_is_not_refused_and_leaves_nothing(&cluster, Mode::Replicated).await;

    cluster.shutdown().await;
}

#[tokio::test]
async fn a_cancelled_distributed_open_frees_the_name_for_a_retry() {
    let cluster =
        node_beside_a_crashed_co_owner("cache-it-cancelled-distributed", Duration::from_secs(5))
            .await;

    assert_retry_is_not_refused_and_leaves_nothing(&cluster, Mode::distributed()).await;

    cluster.shutdown().await;
}

/// Inserts and reads `cache`, closes it and checks `health()` lists nothing.
async fn assert_cache_works_then_closes(cluster: &Cluster, cache: Cache<u32, String>) {
    cache.insert(1, "one".to_string()).await.expect("insert");
    assert_eq!(cache.get(&1).await, Some("one".to_string()));
    let listed: Vec<_> = cluster
        .health()
        .caches
        .iter()
        .map(|c| c.name.clone())
        .collect();
    assert_eq!(listed, ["x"], "the retry is the one cache listed");
    cache.close().await;
    assert_nothing_listed(cluster);
}

#[tokio::test]
async fn a_retry_after_a_cancelled_replicated_open_opens_a_working_cache() {
    let cluster = lone_cluster_with_dead_seed(
        "cache-it-cancelled-replicated-retry",
        Duration::from_secs(5),
    )
    .await;
    assert!(open_and_cancel(&cluster, Mode::Replicated, CANCEL_AFTER).await);

    let cache = cluster
        .cache::<u32, String>("x")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("the retry waits for the close and opens");

    assert_cache_works_then_closes(&cluster, cache).await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn a_retry_after_a_cancelled_distributed_open_opens_a_working_cache() {
    let cluster = node_beside_a_crashed_co_owner(
        "cache-it-cancelled-distributed-retry",
        Duration::from_millis(1500),
    )
    .await;
    assert!(open_and_cancel(&cluster, Mode::distributed(), CANCEL_AFTER).await);

    let cache = cluster
        .cache::<u32, String>("x")
        .mode(Mode::distributed())
        .open()
        .await
        .expect("the retry waits for the close and opens");

    assert_cache_works_then_closes(&cluster, cache).await;
    cluster.shutdown().await;
}

/// A peer stops seeing the mode and the loader a cancelled open advertised.
#[tokio::test]
async fn a_cancelled_open_clears_the_mode_and_loader_it_advertised() {
    let a = Cluster::builder("cache-it-cancelled-advert")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("a builds");
    let b = Cluster::builder("cache-it-cancelled-advert")
        .seeds([a.local_gossip_addr()])
        .config(loopback_config())
        .build()
        .await
        .expect("b builds");
    wait_for_peer_count(&a, 1).await;
    wait_for_peer_count(&b, 1).await;
    let b_node = b.node_id();
    let sees_mode = || {
        a.advertised_cache_modes()
            .get(&b_node)
            .is_some_and(|caches| caches.contains_key("x"))
    };
    let sees_loader = || {
        a.advertised_loaders()
            .get(&b_node)
            .is_some_and(|caches| caches.contains("x"))
    };

    let open = b
        .cache::<u32, String>("x")
        .mode(Mode::Replicated)
        .loader(|key: u32| async move { Ok::<_, std::io::Error>(Some(key.to_string())) })
        .open();
    tokio::select! {
        opened = open => panic!("the open ends before the advert is seen: {:?}", opened.err()),
        () = wait_until(
            Duration::from_secs(10),
            "a sees b advertise x and its loader",
            async || sees_mode() && sees_loader(),
        ) => {}
    }

    wait_until(
        Duration::from_secs(10),
        "a stops seeing b's mode and loader for x",
        async || !sees_mode() && !sees_loader(),
    )
    .await;
    b.shutdown().await;
    a.shutdown().await;
}

/// A shard registered under `name` in `cluster` the way `open` registers one, and its guard.
struct Registered {
    #[cfg(feature = "spill")]
    shard: Arc<Shard<u32, String>>,
    cancel: CancellationToken,
    tasks: TaskTracker,
    guard: OpenGuard<u32, String>,
}

async fn register(cluster: &Cluster, name: &str) -> Registered {
    let name = SmolStr::new(name);
    let shard = Arc::new(Shard::<u32, String>::new(
        name.clone(),
        Mode::Local,
        cluster.node_id(),
        u64::MAX,
        None,
        None,
    ));
    reserve_name(cluster, &name, &shard)
        .await
        .expect("the name is free");
    cluster.advertise_cache_mode(&name, Mode::Local);
    cluster.mark_warm(&name);
    let cancel = cluster.cancel_token().child_token();
    let tasks = TaskTracker::new();
    let guard = OpenGuard::new(cluster, &shard, &cancel, &tasks);
    Registered {
        #[cfg(feature = "spill")]
        shard,
        cancel,
        tasks,
        guard,
    }
}

fn is_registered(cluster: &Cluster, name: &str) -> bool {
    cluster
        .shards()
        .read()
        .expect("invariant: shard registry lock is never poisoned")
        .contains_key(name)
}

async fn solo_cluster(label: &'static str) -> Cluster {
    Cluster::builder(label)
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("cluster builds")
}

#[tokio::test]
async fn a_dropped_guard_closes_the_cache_and_frees_the_name() {
    let cluster = solo_cluster("cache-it-guard-drop").await;
    let registered = register(&cluster, "g").await;
    let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    {
        let (cancel, running) = (registered.cancel.clone(), Arc::clone(&running));
        cluster.spawn_tracked_in(&registered.tasks, async move {
            cancel.cancelled().await;
            running.store(false, std::sync::atomic::Ordering::SeqCst);
        });
    }

    drop(registered.guard);

    assert!(
        cluster.is_closing("g"),
        "the mark is set before the drop returns"
    );
    assert!(
        registered.cancel.is_cancelled(),
        "the tasks are cancelled before it returns"
    );
    wait_until(Duration::from_secs(10), "the name is freed", async || {
        !is_registered(&cluster, "g")
    })
    .await;
    assert!(
        !running.load(std::sync::atomic::Ordering::SeqCst),
        "the name is freed after the tasks have stopped"
    );
    assert!(
        cluster.health().caches.is_empty(),
        "the mode is no longer listed"
    );
    wait_until(Duration::from_secs(10), "the mark clears", async || {
        !cluster.is_closing("g")
    })
    .await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn a_disarmed_guard_leaves_the_cache_registered() {
    let cluster = solo_cluster("cache-it-guard-disarm").await;
    let registered = register(&cluster, "g").await;

    registered.guard.disarm();

    assert!(is_registered(&cluster, "g"));
    assert!(!cluster.is_closing("g"));
    assert!(!registered.cancel.is_cancelled());
    assert_eq!(cluster.health().caches.len(), 1);
    cluster.shutdown().await;
}

#[tokio::test]
async fn an_open_of_a_closing_name_waits_for_the_close_instead_of_failing() {
    let cluster = solo_cluster("cache-it-guard-reserve-waits").await;
    let registered = register(&cluster, "g").await;
    drop(registered.guard);

    let next = Arc::new(Shard::<u32, String>::new(
        SmolStr::new("g"),
        Mode::Local,
        cluster.node_id(),
        u64::MAX,
        None,
        None,
    ));
    tokio::time::timeout(
        Duration::from_secs(10),
        reserve_name(&cluster, &SmolStr::new("g"), &next),
    )
    .await
    .expect("the reservation ends")
    .expect("the name is reserved once the close has freed it");

    assert!(is_registered(&cluster, "g"));
    cluster.shutdown().await;
}

#[tokio::test]
async fn an_open_of_a_name_another_open_holds_is_refused() {
    let cluster = solo_cluster("cache-it-guard-reserve-refuses").await;
    let registered = register(&cluster, "g").await;

    let next = Arc::new(Shard::<u32, String>::new(
        SmolStr::new("g"),
        Mode::Local,
        cluster.node_id(),
        u64::MAX,
        None,
        None,
    ));
    let refused = reserve_name(&cluster, &SmolStr::new("g"), &next).await;

    assert!(
        matches!(refused, Err(CacheError::AlreadyOpen { .. })),
        "{refused:?}"
    );
    registered.guard.disarm();
    cluster.shutdown().await;
}

#[tokio::test]
async fn a_guard_dropped_with_no_runtime_frees_the_name_without_panicking() {
    let cluster = solo_cluster("cache-it-guard-no-runtime").await;
    let registered = register(&cluster, "g").await;

    // A plain thread has no runtime to spawn the teardown on.
    std::thread::spawn(move || drop(registered.guard))
        .join()
        .expect("the drop does not panic");

    assert!(!is_registered(&cluster, "g"));
    assert_nothing_listed(&cluster);
    assert!(!cluster.is_closing("g"));
    assert!(registered.cancel.is_cancelled());
    cluster.shutdown().await;
}

#[tokio::test]
async fn a_teardown_dropped_before_it_runs_still_frees_the_name() {
    let cluster = solo_cluster("cache-it-guard-runtime-gone").await;
    let registered = register(&cluster, "g").await;

    // The guard drops inside a runtime that is dropped before the teardown it spawned is ever
    // polled, as at the teardown of a runtime.
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a runtime builds");
        runtime.block_on(async move { drop(registered.guard) });
        drop(runtime);
    })
    .join()
    .expect("the drop does not panic");

    assert!(!is_registered(&cluster, "g"));
    assert_nothing_listed(&cluster);
    assert!(!cluster.is_closing("g"));
    assert!(registered.cancel.is_cancelled());
    cluster.shutdown().await;
}

#[tokio::test]
async fn shutdown_returns_while_a_dropped_opens_teardown_runs() {
    let cluster = solo_cluster("cache-it-guard-shutdown").await;
    let registered = register(&cluster, "g").await;
    {
        let cancel = registered.cancel.clone();
        cluster.spawn_tracked_in(&registered.tasks, async move { cancel.cancelled().await });
    }
    drop(registered.guard);

    tokio::time::timeout(Duration::from_secs(30), cluster.clone().shutdown())
        .await
        .expect("shutdown does not wait on the teardown forever");

    assert!(!is_registered(&cluster, "g"), "the teardown ran to the end");
    assert!(!cluster.is_closing("g"));
}

#[tokio::test]
async fn shutdown_returns_right_after_an_open_is_cancelled() {
    let cluster =
        lone_cluster_with_dead_seed("cache-it-cancelled-shutdown", Duration::from_secs(5)).await;
    assert!(open_and_cancel(&cluster, Mode::Replicated, CANCEL_AFTER).await);

    tokio::time::timeout(Duration::from_secs(30), cluster.clone().shutdown())
        .await
        .expect("shutdown does not wait on the teardown forever");

    assert_nothing_listed(&cluster);
    assert!(!is_registered(&cluster, "x"));
}

#[cfg(feature = "spill")]
mod spill {
    use super::super::spill_gate::{fresh_spill_dir, snapshot_under};
    use super::*;
    use crate::store::spill::SpillConfig;

    fn warm_config(dir: &std::path::Path) -> SpillConfig {
        SpillConfig::new(dir, 1 << 20)
            .region_bytes(4096)
            .warm_reopen(true)
    }

    /// A cancelled open of a spill cache closes its tier checkpointed, and the retry replays what
    /// an earlier run stored.
    #[tokio::test]
    async fn a_retry_after_a_cancelled_spill_open_replays_what_an_earlier_run_stored() {
        let dir = fresh_spill_dir("cancelled-warm");
        let first = solo_cluster("cache-it-cancelled-spill-first").await;
        let cache = first
            .cache::<u32, String>("x")
            .mode(Mode::Replicated)
            .spill(warm_config(&dir))
            .open()
            .await
            .expect("opens");
        cache.insert(1, "one".to_string()).await.expect("insert");
        cache.close().await;
        first.shutdown().await;

        let cluster =
            lone_cluster_with_dead_seed("cache-it-cancelled-spill-second", Duration::from_secs(5))
                .await;
        let cancelled = tokio::time::timeout(
            CANCEL_AFTER,
            cluster
                .cache::<u32, String>("x")
                .mode(Mode::Replicated)
                .spill(warm_config(&dir))
                .open(),
        )
        .await;
        assert!(cancelled.is_err(), "the open is still waiting for a donor");

        let retry = cluster
            .cache::<u32, String>("x")
            .mode(Mode::Replicated)
            .spill(warm_config(&dir))
            .open()
            .await
            .expect("the retry waits for the old tier to close and opens");
        assert_eq!(
            retry.get(&1).await,
            Some("one".to_string()),
            "the warm reopen replays the entry"
        );
        retry.close().await;
        cluster.shutdown().await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The name is free only after the old tier has closed and checkpointed.
    #[tokio::test]
    async fn a_dropped_guard_closes_the_spill_tier_before_it_frees_the_name() {
        let dir = fresh_spill_dir("guard-tier-first");
        let cluster = solo_cluster("cache-it-guard-spill-order").await;
        let registered = register(&cluster, "g").await;
        registered
            .shard
            .attach_spill(&warm_config(&dir))
            .expect("the tier attaches");
        registered
            .shard
            .insert(1, "one".to_string())
            .await
            .expect("insert");
        assert!(
            !registered.shard.spill_tier_closed(),
            "the tier starts open"
        );

        drop(registered.guard);

        wait_until(Duration::from_secs(10), "the name is freed", async || {
            !is_registered(&cluster, "g")
        })
        .await;
        assert!(
            registered.shard.spill_tier_closed(),
            "the tier is closed by the time the name is free"
        );
        assert!(
            snapshot_under(&dir).is_file(),
            "the close checkpointed the tier for a warm reopen"
        );
        cluster.shutdown().await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An `open` that fails to attach its tier frees the name before it returns the error.
    #[tokio::test]
    async fn an_open_whose_spill_attach_fails_frees_the_name_at_once() {
        let cluster = solo_cluster("cache-it-spill-attach-fails").await;
        let path = fresh_spill_dir("attach-fails");
        std::fs::write(&path, b"not a directory").expect("a regular file at the spill path");

        let err = cluster
            .cache::<u32, String>("x")
            .spill(SpillConfig::new(&path, 1 << 20).region_bytes(4096))
            .open()
            .await
            .expect_err("a regular file cannot hold the tier");
        assert!(
            matches!(err, CacheError::SpillUnavailable { .. }),
            "{err:?}"
        );

        assert!(
            !is_registered(&cluster, "x"),
            "the name is free when open returns"
        );
        assert!(!cluster.is_closing("x"));
        assert_nothing_listed(&cluster);
        let retry = cluster
            .cache::<u32, String>("x")
            .open()
            .await
            .expect("an immediate retry is not refused");
        retry.close().await;
        cluster.shutdown().await;
        let _ = std::fs::remove_file(&path);
    }
}

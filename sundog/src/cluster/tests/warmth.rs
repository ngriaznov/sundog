//! Tests for warmth and readiness: the first-peer grace, joiner reconciliation, state transfer and shutdown.

use super::*;
use crate::cluster::test_support::wait_for_no_peers;

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

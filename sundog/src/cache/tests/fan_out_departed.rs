//! Writes that have lapsed, been evicted or been spilled by the time they fan out still reach the peers.

use super::*;

/// How long a lapsed write is watched after the peer has taken it: several
/// anti-entropy rounds and at least two sweeps (`loopback_config` runs
/// anti-entropy every 200 ms and sweeps about once a second), enough for
/// the old value to be pulled back onto the writer if any peer kept it.
const SETTLE: Duration = Duration::from_millis(2_600);

async fn two_nodes(name: &'static str) -> (Cluster, Cluster) {
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
    (a, b)
}

async fn open_in(cluster: &Cluster, mode: Mode) -> Cache<u32, String> {
    cluster
        .cache::<u32, String>("sessions")
        .mode(mode)
        .open()
        .await
        .expect("opens")
}

/// Waits until `b` holds `old` for key 1, written by `a`.
async fn seed_old_value(cache_a: &Cache<u32, String>, cache_b: &Cache<u32, String>) {
    cache_a.insert(1, "old".into()).await.expect("insert");
    wait_until(Duration::from_secs(10), "b holds the entry", async || {
        cache_b.get(&1).await.as_deref() == Some("old")
    })
    .await;
}

/// Watches both nodes for [`SETTLE`]: neither may read the key again.
async fn assert_stays_gone(cache_a: &Cache<u32, String>, cache_b: &Cache<u32, String>) {
    let deadline = tokio::time::Instant::now() + SETTLE;
    while tokio::time::Instant::now() < deadline {
        assert_eq!(cache_a.get(&1).await, None, "a read the old value again");
        assert_eq!(cache_b.get(&1).await, None, "b read the old value again");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn expire_to_zero_reaches_every_replica() {
    let (a, b) = two_nodes("cache-it-fan-out-expire-zero").await;
    let cache_a = open_in(&a, Mode::Replicated).await;
    let cache_b = open_in(&b, Mode::Replicated).await;
    seed_old_value(&cache_a, &cache_b).await;

    assert!(cache_a.expire(&1, Duration::ZERO).await.expect("expire"));
    wait_until(Duration::from_secs(10), "b drops the entry", async || {
        cache_b.get(&1).await.is_none()
    })
    .await;
    assert_stays_gone(&cache_a, &cache_b).await;

    b.shutdown().await;
    a.shutdown().await;
}

#[tokio::test]
async fn insert_with_a_zero_ttl_reaches_every_replica() {
    let (a, b) = two_nodes("cache-it-fan-out-insert-zero").await;
    let cache_a = open_in(&a, Mode::Replicated).await;
    let cache_b = open_in(&b, Mode::Replicated).await;
    seed_old_value(&cache_a, &cache_b).await;

    cache_a
        .insert_with_ttl(1, "new".into(), Duration::ZERO)
        .await
        .expect("insert");
    wait_until(
        Duration::from_secs(10),
        "b drops the old value",
        async || cache_b.get(&1).await.is_none(),
    )
    .await;
    assert_stays_gone(&cache_a, &cache_b).await;

    b.shutdown().await;
    a.shutdown().await;
}

#[tokio::test]
async fn an_invalidation_cache_drops_the_peers_copy_for_a_zero_ttl_write() {
    let (a, b) = two_nodes("cache-it-fan-out-invalidation-zero").await;
    let cache_a = open_in(&a, Mode::Invalidation).await;
    let cache_b = open_in(&b, Mode::Invalidation).await;
    cache_b.insert(1, "old".into()).await.expect("insert");
    // Distinct milliseconds, so the later write has the greater version
    // whatever the node ids.
    tokio::time::sleep(Duration::from_millis(10)).await;

    cache_a
        .insert_with_ttl(1, "new".into(), Duration::ZERO)
        .await
        .expect("insert");

    wait_until(
        Duration::from_secs(10),
        "b drops its older copy",
        async || cache_b.get(&1).await.is_none(),
    )
    .await;

    b.shutdown().await;
    a.shutdown().await;
}

#[tokio::test]
async fn a_distributed_owner_tells_the_co_owner_of_a_zero_ttl_rearm() {
    let ((a, _cache_a), (b, cache_b), (c, cache_c), key) =
        three_node_distributed("cache-it-fan-out-distributed-zero", "prices").await;
    cache_b.insert(key, "old".into()).await.expect("insert");
    wait_until(
        Duration::from_secs(10),
        "both owners hold the value",
        async || cache_b.ttl_of(&key).is_some() && cache_c.ttl_of(&key).is_some(),
    )
    .await;

    assert!(cache_b.expire(&key, Duration::ZERO).await.expect("expire"));

    wait_until(
        Duration::from_secs(10),
        "the co-owner drops it",
        async || cache_c.get(&key).await.is_none(),
    )
    .await;
    let deadline = tokio::time::Instant::now() + SETTLE;
    while tokio::time::Instant::now() < deadline {
        assert_eq!(cache_b.get(&key).await, None);
        assert_eq!(cache_c.get(&key).await, None);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    c.shutdown().await;
    b.shutdown().await;
    a.shutdown().await;
}

/// A write the owner's fan-out finds gone reaches the other owner through the
/// awaiting path: `b` wrote the key, its entry left before the drain, and
/// `c` still holds the older value. The drain is driven by hand so the entry
/// is gone when it reads the written view. Anti-entropy is effectively off:
/// until `c` takes the invalidation, `b` holds nothing for the key and `c`
/// the older value, which a round between the two would carry back onto
/// `b`. Rounds run by hand once `c` has dropped it.
#[tokio::test]
async fn a_distributed_owner_invalidates_the_co_owner_of_a_departed_write() {
    use crate::cluster::anti_entropy::{RoundOutcome, run_round_against};
    use crate::cluster::fan_out::fan_out_batch;
    use crate::cluster::test_support::registered_shard;
    use crate::store::{FanOutItem, ShardOps};

    let config = steady_config().with(|c| {
        c.ae_interval = Duration::from_secs(3600);
        c.tombstone_ttl = c.bucket_release_window();
    });
    let ((a, _cache_a), (b, cache_b), (c, cache_c), key) = three_node_distributed_on(
        "cache-it-fan-out-distributed-departed",
        "prices",
        config.clone(),
        config,
    )
    .await;
    cache_b.insert(key, "old".into()).await.expect("insert");
    wait_until(
        Duration::from_secs(10),
        "both owners hold the value",
        async || cache_b.ttl_of(&key).is_some() && cache_c.ttl_of(&key).is_some(),
    )
    .await;
    let key_bytes = encode_key(&key).expect("u32 encodes");
    let stored = ShardOps::records_for(&*cache_b.shard, vec![key_bytes.clone()]).await;
    let written = crate::hlc::Hlc {
        wall_ms: stored[0].ver.wall_ms + 5,
        ..stored[0].ver
    };
    // The write the drain is about to send is gone from `b`: a release, an
    // idle drop or an unreadable spill would leave the same, and no record of
    // it remains to ship.
    ShardOps::invalidate(&*cache_b.shard, key_bytes, written).await;
    assert_eq!(cache_b.get(&key).await, None, "b's entry is gone");
    assert_eq!(cache_c.get(&key).await.as_deref(), Some("old"));
    assert!(
        cache_b.owners_of(&key).contains(&c.node_id()),
        "b's view names c an owner of the key"
    );

    fan_out_batch(
        &cache_b.shard,
        &b,
        &SmolStr::new("prices"),
        Mode::distributed(),
        vec![FanOutItem::Applied(key, written)],
    )
    .await;

    wait_until(
        Duration::from_secs(10),
        "the co-owner drops the older value",
        async || cache_c.get(&key).await.is_none(),
    )
    .await;
    // Neither owner kept the older value, so a round each way between the
    // two carries nothing back.
    let name = SmolStr::new("prices");
    for (from, to) in [(&b, &c), (&c, &b)] {
        let shard = registered_shard(from, &name);
        wait_until(
            CONVERGE,
            "a round between the owners reconciles",
            async || {
                run_round_against(from.mesh(), &shard, &name, to.node_id()).await
                    == RoundOutcome::Reconciled
            },
        )
        .await;
    }
    assert_eq!(cache_b.get(&key).await, None, "b read the old value again");
    assert_eq!(cache_c.get(&key).await, None, "c read the old value again");

    c.shutdown().await;
    b.shutdown().await;
    a.shutdown().await;
}

/// A node whose shard has no ownership view yet names no owner to tell, so a
/// departed write sends nothing: the co-owner keeps its value.
#[tokio::test]
async fn a_distributed_departed_write_without_a_view_tells_nobody() {
    use crate::cluster::fan_out::fan_out_batch;
    use crate::store::FanOutItem;

    let ((a, _cache_a), (b, cache_b), (c, cache_c), key) =
        three_node_distributed("cache-it-fan-out-distributed-no-view", "prices").await;
    cache_b.insert(key, "old".into()).await.expect("insert");
    wait_until(
        Duration::from_secs(10),
        "both owners hold the value",
        async || cache_b.ttl_of(&key).is_some() && cache_c.ttl_of(&key).is_some(),
    )
    .await;
    let viewless = Shard::<u32, String>::new(
        SmolStr::new("prices"),
        Mode::distributed(),
        b.node_id(),
        u64::MAX,
        None,
        None,
    );
    assert!(viewless.ownership_view().is_none());
    let written = crate::hlc::Hlc {
        wall_ms: now_ms() + 60_000,
        logical: 0,
        node: b.node_id(),
    };

    fan_out_batch(
        &viewless,
        &b,
        &SmolStr::new("prices"),
        Mode::distributed(),
        vec![FanOutItem::Applied(key, written)],
    )
    .await;

    let deadline = tokio::time::Instant::now() + Duration::from_millis(1_000);
    while tokio::time::Instant::now() < deadline {
        assert_eq!(cache_c.get(&key).await.as_deref(), Some("old"));
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    c.shutdown().await;
    b.shutdown().await;
    a.shutdown().await;
}

/// `b` holds an older value for every key; `a`, capped at ten entries,
/// writes a hundred new ones in a single call, so most are evicted before
/// the fan-out drains. Every old key leaves `b` all the same.
#[tokio::test]
async fn an_eviction_burst_invalidates_every_peer_copy() {
    let (a, b) = two_nodes("cache-it-fan-out-eviction-burst").await;
    let cache_a = a
        .cache::<u32, String>("sessions")
        .mode(Mode::Invalidation)
        .max_capacity(10)
        .open()
        .await
        .expect("a opens");
    let cache_b = open_in(&b, Mode::Invalidation).await;
    for key in 0..100u32 {
        cache_b
            .insert(key, format!("old{key}"))
            .await
            .expect("insert");
    }
    tokio::time::sleep(Duration::from_millis(10)).await;

    cache_a
        .insert_many((0..100u32).map(|key| (key, format!("new{key}"))))
        .await
        .expect("insert_many");

    wait_until(
        Duration::from_secs(10),
        "every old copy leaves b",
        async || {
            for key in 0..100u32 {
                if cache_b.get(&key).await.is_some() {
                    return false;
                }
            }
            true
        },
    )
    .await;

    b.shutdown().await;
    a.shutdown().await;
}

/// `b` holds a counter of 1,000 for every key; `a`, capped at ten entries,
/// merges into a hundred of them, so most of `a`'s counters are evicted
/// before the fan-out drains. A merging resolver sends nothing for an
/// evicted write: `b`'s merged copy is the best one left, and it keeps
/// every counter. (A `Replicated` cache cannot evict; its departed writes
/// are swept ones, covered with a real shard in `cluster::fan_out`.)
#[tokio::test]
async fn an_eviction_burst_spares_every_peer_counter_in_invalidation() {
    let mode = Mode::Invalidation;
    let (a, b) = two_nodes("cache-it-fan-out-merging-evicted").await;
    let open_counters = |cluster: &Cluster, capacity: u64| {
        cluster
            .cache::<u32, PnCounter>("counters")
            .mode(mode)
            .max_capacity(capacity)
            .resolver(Arc::new(PnCounterResolver))
            .open()
    };
    let cache_a = open_counters(&a, 10).await.expect("a opens");
    let cache_b = open_counters(&b, u64::MAX).await.expect("b opens");
    for key in 0..100u32 {
        cache_b
            .merge(key, PnCounter::local_delta(cache_b.writer_id(), 1_000))
            .await
            .expect("merge");
    }
    tokio::time::sleep(Duration::from_millis(10)).await;

    for key in 0..100u32 {
        cache_a
            .merge(key, PnCounter::local_delta(cache_a.writer_id(), 5))
            .await
            .expect("merge");
    }

    let deadline = tokio::time::Instant::now() + SETTLE;
    while tokio::time::Instant::now() < deadline {
        for key in 0..100u32 {
            let kept = cache_b.get(&key).await.map(|counter| counter.value());
            assert!(
                kept.is_some_and(|value| value >= 1_000),
                "b lost its counter for key {key}: {kept:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    b.shutdown().await;
    a.shutdown().await;
}

#[cfg(feature = "spill")]
mod spilled {
    use super::*;

    /// As the eviction burst, with a spill tier: the entries spill rather
    /// than vanish, and the peers' older copies still go.
    #[tokio::test]
    async fn a_spilled_write_invalidates_every_peer_copy() {
        let dir = std::env::temp_dir().join(format!(
            "sundog-fan-out-departed-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let (a, b) = two_nodes("cache-it-fan-out-spill-burst").await;
        let cache_a = a
            .cache::<u32, String>("sessions")
            .mode(Mode::Invalidation)
            .max_capacity(10)
            .spill(SpillConfig::new(&dir, 256 * 1024 * 1024))
            .open()
            .await
            .expect("a opens");
        let cache_b = open_in(&b, Mode::Invalidation).await;
        for key in 0..100u32 {
            cache_b
                .insert(key, format!("old{key}"))
                .await
                .expect("insert");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;

        cache_a
            .insert_many((0..100u32).map(|key| (key, format!("new{key}"))))
            .await
            .expect("insert_many");

        wait_until(
            Duration::from_secs(10),
            "every old copy leaves b",
            async || {
                for key in 0..100u32 {
                    if cache_b.get(&key).await.is_some() {
                        return false;
                    }
                }
                true
            },
        )
        .await;

        cache_a.close().await;
        b.shutdown().await;
        a.shutdown().await;
        let _ = std::fs::remove_dir_all(&dir);
    }
}

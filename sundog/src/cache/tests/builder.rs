//! Tests of the builder and `open`: capacity limits, the capacity hint, merge windows and the
//! checks on distributed caches.

use super::*;

/// Through `Cache`: on a capped cache with more stripes than entries,
/// every write past the cap reads back at once and the cap holds.
#[tokio::test]
async fn every_write_past_max_capacity_reads_back_through_the_cache() {
    let cluster = Cluster::builder("cache-it-write-past-cap")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node builds");
    let cache = cluster
        .cache::<u32, String>("capped")
        .mode(Mode::Local)
        .max_capacity(50)
        .open()
        .await
        .expect("opens");
    for k in 0..200u32 {
        cache.insert(k, k.to_string()).await.expect("insert");
        assert_eq!(cache.get(&k).await, Some(k.to_string()), "key {k}");
    }
    assert_eq!(cache.entry_count().await, 50);
    cluster.shutdown().await;
}

/// Through `Cache`: a value heavier than the whole cap goes, and an
/// older entry in another stripe stays.
#[tokio::test]
async fn a_value_heavier_than_max_capacity_leaves_the_older_entry() {
    let cluster = Cluster::builder("cache-it-heavier-than-cap")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node builds");
    let cache = cluster
        .cache::<u32, String>("heavy")
        .mode(Mode::Local)
        .max_capacity(5)
        .weigher(|_k: &u32, v: &String| u32::try_from(v.len()).unwrap_or(u32::MAX))
        .open()
        .await
        .expect("opens");
    let stripe = |k: u32| {
        crate::store::hash_key_bytes(&postcard::to_stdvec(&k).expect("key encodes"))
            % crate::store::BUCKET_COUNT as u64
    };
    let heavy = (2..100_000u32)
        .find(|&k| stripe(k) != stripe(1))
        .expect("another stripe");
    cache.insert(1, "x".to_string()).await.expect("insert");

    cache.insert(heavy, "x".repeat(10)).await.expect("insert");

    assert_eq!(cache.get(&heavy).await, None, "the heavy value goes");
    assert_eq!(
        cache.get(&1).await.as_deref(),
        Some("x"),
        "the older entry stays"
    );
    assert_eq!(cache.entry_count().await, 1);
    cluster.shutdown().await;
}

#[tokio::test]
async fn merge_coalesce_window_rejects_a_non_merging_resolver() {
    let cluster = Cluster::builder("cache-it-merge-window-rejects-non-merging")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let err = cluster
        .cache::<u32, u32>("counters")
        .merge_coalesce_window(Duration::from_millis(5))
        .open()
        .await
        .expect_err("the default LwwResolver does not merge, so a nonzero window is rejected");
    assert!(matches!(
        err,
        CacheError::MergeWindowRequiresMergingResolver { .. }
    ));

    cluster.shutdown().await;
}

#[tokio::test]
async fn prefold_enabled_defaults_to_true_and_the_toggle_reaches_the_shard() {
    let cluster = Cluster::builder("cache-it-prefold-enabled")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let on = cluster
        .cache::<u32, u32>("counters-on")
        .mode(Mode::Local)
        .open()
        .await
        .expect("open succeeds");
    assert!(
        on.shard.prefold_enabled(),
        "pre-fold defaults to on, matching the engine's own default"
    );

    let off = cluster
        .cache::<u32, u32>("counters-off")
        .mode(Mode::Local)
        .prefold_enabled(false)
        .open()
        .await
        .expect("open succeeds");
    assert!(
        !off.shard.prefold_enabled(),
        "CacheBuilder::prefold_enabled(false) reaches the shard's engine"
    );

    on.close().await;
    off.close().await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn cache_builder_capacity_hint_reaches_the_shard() {
    let cluster = Cluster::builder("cache-it-capacity-hint")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let hint = 3_000u64;
    let expected =
        usize::try_from(hint.div_ceil(crate::store::BUCKET_COUNT as u64)).expect("hint fits usize");
    let cache = cluster
        .cache::<u32, u32>("counters")
        .mode(Mode::Local)
        .capacity_hint(hint)
        .open()
        .await
        .expect("open succeeds");
    assert_eq!(
        cache.shard.stripe_capacities(),
        vec![expected; crate::store::BUCKET_COUNT],
        "the hint reached the shard's engine, presizing every stripe"
    );

    cache.close().await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn cache_stripe_capacities_matches_the_shards() {
    let cluster = Cluster::builder("cache-it-cache-stripe-capacities")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let hint = 500u64;
    let cache = cluster
        .cache::<u32, u32>("counters")
        .mode(Mode::Local)
        .capacity_hint(hint)
        .open()
        .await
        .expect("open succeeds");
    assert_eq!(
        cache.stripe_capacities(),
        cache.shard.stripe_capacities(),
        "Cache::stripe_capacities is a plain forward to the shard's own accessor, for a \
         crate outside sundog to reach it without seeing the private `shard` field"
    );

    cache.close().await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn capacity_hint_above_max_capacity_is_clamped_without_a_weigher() {
    let cluster = Cluster::builder("cache-it-capacity-hint-clamped")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let max_capacity = 100u64;
    let hint = 3_000u64;
    let expected = usize::try_from(max_capacity.div_ceil(crate::store::BUCKET_COUNT as u64))
        .expect("clamped hint fits usize");
    let cache = cluster
        .cache::<u32, u32>("counters")
        .mode(Mode::Local)
        .max_capacity(max_capacity)
        .capacity_hint(hint)
        .open()
        .await
        .expect("open succeeds");
    assert_eq!(
        cache.shard.stripe_capacities(),
        vec![expected; crate::store::BUCKET_COUNT],
        "with no weigher, max_capacity bounds entry count, so a hint above it is clamped"
    );

    cache.close().await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn capacity_hint_above_max_capacity_is_kept_as_is_with_a_weigher() {
    let cluster = Cluster::builder("cache-it-capacity-hint-unclamped")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let max_capacity = 100u64;
    let hint = 3_000u64;
    let expected =
        usize::try_from(hint.div_ceil(crate::store::BUCKET_COUNT as u64)).expect("hint fits usize");
    let cache = cluster
        .cache::<u32, u32>("counters")
        .mode(Mode::Local)
        .max_capacity(max_capacity)
        .weigher(|_key: &u32, _value: &u32| 1)
        .capacity_hint(hint)
        .open()
        .await
        .expect("open succeeds");
    assert_eq!(
        cache.shard.stripe_capacities(),
        vec![expected; crate::store::BUCKET_COUNT],
        "with a weigher, max_capacity bounds weight, not entry count, so the hint is kept as is"
    );

    cache.close().await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn capacity_hint_defaults_to_none_and_matches_pre_change_rss() {
    let cluster = Cluster::builder("cache-it-capacity-hint-none")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let cache = cluster
        .cache::<u32, u32>("counters")
        .mode(Mode::Local)
        .open()
        .await
        .expect("open succeeds");
    assert_eq!(
        cache.shard.stripe_capacities(),
        vec![0; crate::store::BUCKET_COUNT],
        "no hint: every stripe starts at zero allocation, matching an engine with no \
         capacity_hint at all"
    );

    cache.close().await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn distributed_cache_rejects_a_tombstone_ttl_inside_the_release_window() {
    let name = "distributed-release-window-guard";
    let config = loopback_config().with(|c| {
        c.ae_interval = Duration::from_secs(3);
        c.distributed_disown_grace_rounds = 3;
        c.tombstone_ttl = Duration::from_secs(15);
    });
    let cluster = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(config)
        .build()
        .await
        .expect("the cluster builds; the rule binds distributed caches only");

    let err = cluster
        .cache::<u32, String>(name)
        .mode(Mode::distributed())
        .open()
        .await
        .expect_err("a distributed cache under a short retention is rejected");
    assert!(
        matches!(
            &err,
            CacheError::TombstoneTtlInsideReleaseWindow { cache, tombstone_ttl, window }
                if cache == name
                    && *tombstone_ttl == Duration::from_secs(15)
                    && *window == Duration::from_secs(24)
        ),
        "{err:?}"
    );

    let replicated = cluster
        .cache::<u32, String>(name)
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("a replicated cache is not bound by the release window");
    replicated.close().await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn distributed_cache_rejects_owners_below_two() {
    let cluster = Cluster::builder("cache-it-distributed-owners-below-two")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let err = cluster
        .cache::<u32, String>("scratch")
        .mode(Mode::Distributed {
            owners: std::num::NonZeroU8::new(1).expect("nonzero"),
        })
        .open()
        .await
        .expect_err("a single owner is rejected");

    assert!(
        matches!(err, CacheError::TooFewOwners { .. }),
        "expected TooFewOwners, got {err:?}"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn distributed_cache_open_attaches_an_ownership_view() {
    let cluster = Cluster::builder("cache-it-distributed-attaches-view")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let cache = cluster
        .cache::<u32, String>("scratch")
        .mode(Mode::Distributed {
            owners: std::num::NonZeroU8::new(2).expect("nonzero"),
        })
        .open()
        .await
        .expect("open succeeds");

    assert!(
        ShardOps::ownership_view(&*cache.shard).is_some(),
        "opening a Distributed cache attaches a real ownership view before it's shared"
    );

    cache.close().await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn distributed_cache_rejects_tti() {
    let cluster = Cluster::builder("cache-it-distributed-rejects-tti")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let err = cluster
        .cache::<u32, String>("scratch")
        .mode(Mode::Distributed {
            owners: std::num::NonZeroU8::new(2).expect("nonzero"),
        })
        .tti(Duration::from_secs(30))
        .open()
        .await
        .expect_err("tti is rejected for Distributed just like Replicated");

    assert!(
        matches!(err, CacheError::ReplicatedWithLocalEviction { .. }),
        "expected ReplicatedWithLocalEviction, got {err:?}"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn replicated_node_at_its_memory_ceiling_refuses_local_writes_and_takes_its_peers() {
    let name = "cache-it-memory-ceiling";
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
    let cache_a = a
        .cache::<u32, String>("sessions")
        .mode(Mode::Replicated)
        .max_resident_bytes(1)
        .open()
        .await
        .expect("a ceiling needs no spill tier in Replicated mode");
    let cache_b = b
        .cache::<u32, String>("sessions")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("b opens");

    assert_eq!(cache_a.resident_bytes(), Some(0));
    assert_eq!(
        cache_b.resident_bytes(),
        None,
        "an uncapped cache counts nothing"
    );
    cache_a
        .insert(1, "a".into())
        .await
        .expect("under the ceiling");
    assert!(cache_a.resident_bytes() > Some(0));
    assert!(matches!(
        cache_a.insert(2, "a".into()).await,
        Err(CacheError::OverMemoryCeiling { ceiling: 1, .. })
    ));
    let loaded = cache_a
        .get_or_load(&9, async |_key: &u32| -> Result<String, std::io::Error> {
            Ok("loaded".into())
        })
        .await
        .expect("the loader succeeds");
    assert_eq!(loaded, "loaded");
    assert_eq!(
        cache_a.get(&9).await,
        None,
        "a fill at the ceiling is not stored"
    );

    cache_b.insert(3, "from b".into()).await.expect("insert");
    wait_until(
        Duration::from_secs(10),
        "b's write lands on a past a's ceiling",
        async || cache_a.get(&3).await.is_some(),
    )
    .await;
    wait_until(
        Duration::from_secs(10),
        "a's accepted write reaches b",
        async || cache_b.get(&1).await.is_some(),
    )
    .await;
    assert_eq!(
        cache_b.get(&2).await,
        None,
        "a refused write never replicates"
    );

    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn distributed_cache_opens_with_a_memory_ceiling_and_no_spill() {
    let cluster = Cluster::builder("cache-it-distributed-memory-ceiling")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");
    let cache = cluster
        .cache::<u32, String>("scratch")
        .mode(Mode::Distributed {
            owners: std::num::NonZeroU8::new(2).expect("nonzero"),
        })
        .max_resident_bytes(1)
        .open()
        .await
        .expect("a ceiling evicts nothing, so Distributed accepts it without spill");
    cache
        .insert(1, "a".into())
        .await
        .expect("under the ceiling");
    assert!(matches!(
        cache.insert(2, "b".into()).await,
        Err(CacheError::OverMemoryCeiling { .. })
    ));
    cluster.shutdown().await;
}

#[tokio::test]
async fn distributed_cache_rejects_finite_max_capacity_without_spill() {
    let cluster = Cluster::builder("cache-it-distributed-no-spill-max-capacity")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let err = cluster
        .cache::<u32, String>("scratch")
        .mode(Mode::Distributed {
            owners: std::num::NonZeroU8::new(2).expect("nonzero"),
        })
        .max_capacity(100)
        .open()
        .await
        .expect_err("Distributed + finite max_capacity + no spill is rejected");

    assert!(
        matches!(err, CacheError::ReplicatedWithLocalEviction { .. }),
        "expected ReplicatedWithLocalEviction, got {err:?}"
    );

    cluster.shutdown().await;
}

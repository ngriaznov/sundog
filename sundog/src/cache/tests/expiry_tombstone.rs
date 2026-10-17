//! What a sweep leaves of an expired entry, seen through the cache: a
//! tombstone at the entry's version in the replicated modes, nothing in
//! `Local` and `Invalidation`.
//!
//! The cache reads a clock the test moves, so no test waits out a TTL. A
//! cache defers collection for its first `tombstone_ttl` after it opens, so
//! a delete that reaches it already past that retention is kept, and dropped
//! once the window ends. The tombstone of an expired entry gets no such
//! deferral: past `tombstone_ttl` from its expiry it is dropped at once.

use std::sync::atomic::{AtomicU64, Ordering};

use super::*;

const KEY: u32 = 1;
const TTL: Duration = Duration::from_secs(30);

fn manual_clock() -> Arc<AtomicU64> {
    Arc::new(AtomicU64::new(now_ms() + 3_600_000))
}

async fn open(
    name: &'static str,
    mode: Mode,
    clock: &Arc<AtomicU64>,
) -> (Cluster, Cache<u32, String>) {
    open_with(name, mode, clock, loopback_config()).await
}

async fn open_with(
    name: &'static str,
    mode: Mode,
    clock: &Arc<AtomicU64>,
    config: ClusterConfig,
) -> (Cluster, Cache<u32, String>) {
    let cluster = Cluster::builder(name)
        .config(config)
        .seeds(std::iter::empty())
        .build()
        .await
        .expect("node builds");
    let reader = Arc::clone(clock);
    let cache = cluster
        .cache::<u32, String>(name)
        .mode(mode)
        .clock(Arc::new(move || reader.load(Ordering::SeqCst)))
        .open()
        .await
        .expect("opens");
    (cluster, cache)
}

fn pass_the_ttl(clock: &Arc<AtomicU64>) {
    let past = u64::try_from(TTL.as_millis()).expect("fits") + 1_000;
    clock.fetch_add(past, Ordering::SeqCst);
}

#[tokio::test]
async fn a_replicated_cache_keeps_a_tombstone_for_an_entry_its_sweep_removed() {
    let clock = manual_clock();
    let (cluster, cache) = open(
        "cache-it-expiry-tombstone-replicated",
        Mode::Replicated,
        &clock,
    )
    .await;
    cache
        .insert_with_ttl(KEY, "v".to_string(), TTL)
        .await
        .expect("insert");
    let LocalRecord::Live { version, .. } = cache.explain(&KEY).await.expect("explains").local
    else {
        panic!("the entry is live before it expires");
    };

    pass_the_ttl(&clock);
    assert_eq!(cache.get(&KEY).await, None, "the entry expired");
    assert!(
        matches!(
            cache.explain(&KEY).await.expect("explains").local,
            LocalRecord::Lapsed { .. }
        ),
        "an expired entry no sweep has removed reads as lapsed"
    );
    assert_eq!(cache.entry_count().await, 0, "the count sweeps first");

    assert_eq!(
        cache.explain(&KEY).await.expect("explains").local,
        LocalRecord::Tombstone { version },
        "the sweep kept the expired entry's version as a tombstone"
    );
    assert_eq!(cache.get(&KEY).await, None);

    cache
        .insert(KEY, "again".to_string())
        .await
        .expect("insert over it");
    assert_eq!(cache.get(&KEY).await, Some("again".to_string()));
    cluster.shutdown().await;
}

#[tokio::test]
async fn a_distributed_cache_keeps_a_tombstone_for_an_entry_its_sweep_removed() {
    let clock = manual_clock();
    let (cluster, cache) = open(
        "cache-it-expiry-tombstone-distributed",
        Mode::distributed(),
        &clock,
    )
    .await;
    cache
        .insert_with_ttl(KEY, "v".to_string(), TTL)
        .await
        .expect("insert");
    pass_the_ttl(&clock);
    assert_eq!(cache.entry_count().await, 0);

    assert!(
        matches!(
            cache.explain(&KEY).await.expect("explains").local,
            LocalRecord::Tombstone { .. }
        ),
        "the sole owner keeps the floor"
    );
    cluster.shutdown().await;
}

#[tokio::test]
async fn a_local_or_invalidation_cache_keeps_nothing_of_an_entry_its_sweep_removed() {
    for (name, mode) in [
        ("cache-it-expiry-tombstone-local", Mode::Local),
        ("cache-it-expiry-tombstone-invalidation", Mode::Invalidation),
    ] {
        let clock = manual_clock();
        let (cluster, cache) = open(name, mode, &clock).await;
        cache
            .insert_with_ttl(KEY, "v".to_string(), TTL)
            .await
            .expect("insert");
        pass_the_ttl(&clock);
        assert_eq!(cache.entry_count().await, 0);

        assert_eq!(
            cache.explain(&KEY).await.expect("explains").local,
            LocalRecord::Absent,
            "{mode:?}"
        );
        cluster.shutdown().await;
    }
}

/// Key `key` holding an older copy, then the tombstone of an entry that
/// expired `expired_ago` before the cache's clock reading arriving from a
/// peer: whether the cache still holds the older copy.
async fn holds_the_older_copy_after_the_expiry_tombstone(
    cache: &Cache<u32, String>,
    clock: &Arc<AtomicU64>,
    key: u32,
    expired_ago: Duration,
) -> bool {
    let node = NodeId::from(77);
    let mut old = test_record(key, "old", node);
    old.ver.wall_ms = 1_000;
    let expired_at = clock
        .load(Ordering::SeqCst)
        .saturating_sub(u64::try_from(expired_ago.as_millis()).expect("fits"));
    let tombstone = WireRecord {
        key: old.key.clone(),
        value: None,
        ver: crate::hlc::Hlc {
            wall_ms: 2_000,
            logical: 0,
            node,
        },
        expires_at_ms: Some(expired_at),
    };
    ShardOps::apply_remote(cache.shard.as_ref(), old).await;
    ShardOps::apply_remote(cache.shard.as_ref(), tombstone).await;
    cache.get(&key).await.is_some()
}

/// Key `key` holding an older copy, then a peer's delete stamped
/// `deleted_ago` before the cache's clock reading: whether the cache still
/// holds the older copy.
async fn holds_the_older_copy_after_the_delete(
    cache: &Cache<u32, String>,
    clock: &Arc<AtomicU64>,
    key: u32,
    deleted_ago: Duration,
) -> bool {
    let node = NodeId::from(77);
    let mut old = test_record(key, "old", node);
    old.ver.wall_ms = 1_000;
    let deleted_at = clock
        .load(Ordering::SeqCst)
        .saturating_sub(u64::try_from(deleted_ago.as_millis()).expect("fits"));
    let delete = WireRecord {
        key: old.key.clone(),
        value: None,
        ver: crate::hlc::Hlc {
            wall_ms: deleted_at,
            logical: 0,
            node,
        },
        expires_at_ms: None,
    };
    ShardOps::apply_remote(cache.shard.as_ref(), old).await;
    ShardOps::apply_remote(cache.shard.as_ref(), delete).await;
    cache.get(&key).await.is_some()
}

#[tokio::test]
async fn a_newly_opened_cache_keeps_a_delete_lapsed_by_ttl_and_deletes_its_older_copy() {
    for (name, mode) in [
        (
            "cache-it-expiry-tombstone-start-replicated",
            Mode::Replicated,
        ),
        (
            "cache-it-expiry-tombstone-start-distributed",
            Mode::distributed(),
        ),
    ] {
        let clock = manual_clock();
        let config = ClusterConfig {
            tombstone_ttl: Duration::from_mins(10),
            ..loopback_config()
        };
        let (cluster, cache) = open_with(name, mode, &clock, config).await;

        // Past the 10 min ttl by a minute, far inside the 24 h cap.
        let held =
            holds_the_older_copy_after_the_delete(&cache, &clock, KEY, Duration::from_mins(11))
                .await;

        assert!(!held, "{mode:?}: the older copy is deleted");
        assert!(
            matches!(
                cache.explain(&KEY).await.expect("explains").local,
                LocalRecord::Tombstone { .. }
            ),
            "{mode:?}: the tombstone is kept"
        );
        let past_the_cap = Duration::from_hours(25);
        assert!(
            holds_the_older_copy_after_the_delete(&cache, &clock, KEY + 1, past_the_cap).await,
            "{mode:?}: one lapsed past its cap is dropped"
        );
        cluster.shutdown().await;
    }
}

#[tokio::test]
async fn a_newly_opened_cache_drops_an_expiry_tombstone_lapsed_by_ttl() {
    for (name, mode) in [
        ("cache-it-expiry-tombstone-ttl-replicated", Mode::Replicated),
        (
            "cache-it-expiry-tombstone-ttl-distributed",
            Mode::distributed(),
        ),
    ] {
        let clock = manual_clock();
        let config = ClusterConfig {
            tombstone_ttl: Duration::from_mins(10),
            ..loopback_config()
        };
        let (cluster, cache) = open_with(name, mode, &clock, config).await;
        assert!(
            !holds_the_older_copy_after_the_expiry_tombstone(
                &cache,
                &clock,
                KEY + 2,
                Duration::from_mins(9)
            )
            .await,
            "{mode:?}: within its ttl it deletes the older copy"
        );
        assert!(
            holds_the_older_copy_after_the_expiry_tombstone(
                &cache,
                &clock,
                KEY,
                Duration::from_mins(11)
            )
            .await,
            "{mode:?}: past its ttl it is dropped, though the new cache defers"
        );
        assert!(
            matches!(
                cache.explain(&KEY).await.expect("explains").local,
                LocalRecord::Live { .. }
            ),
            "{mode:?}: no tombstone is kept"
        );
        cluster.shutdown().await;
    }
}

#[tokio::test]
async fn a_cache_drops_a_lapsed_delete_once_its_first_tombstone_ttl_has_passed() {
    let clock = manual_clock();
    let config = ClusterConfig {
        tombstone_ttl: Duration::from_secs(1),
        ..loopback_config()
    };
    let (cluster, cache) = open_with(
        "cache-it-expiry-tombstone-after-start",
        Mode::Replicated,
        &clock,
        config,
    )
    .await;

    // The gc task's first collection after the ttl lifts the deferral, at the
    // latest a period (a second) after it.
    let lifted = tokio::time::timeout(Duration::from_secs(15), async {
        for key in 0u32.. {
            if holds_the_older_copy_after_the_delete(&cache, &clock, key, Duration::from_secs(5))
                .await
            {
                return key;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        unreachable!("the loop returns or the timeout fires")
    })
    .await
    .expect("the deferral ends within a few ticks of the tombstone ttl");
    assert!(
        cache.get(&lifted).await.is_some(),
        "the older copy stays under a lapsed delete"
    );
    cluster.shutdown().await;
}

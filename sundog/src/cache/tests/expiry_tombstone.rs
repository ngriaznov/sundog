//! What a sweep leaves of an expired entry, seen through the cache: a
//! tombstone at the entry's version in the replicated modes, nothing in
//! `Local` and `Invalidation`.
//!
//! The cache reads a clock the test moves, so no test waits out a TTL.

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
    let cluster = Cluster::builder(name)
        .config(loopback_config())
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

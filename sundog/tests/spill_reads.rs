//! Reads of spilled keys through the public API: a read that brings a
//! spilled entry back into RAM keeps the cache within `max_capacity`, as a
//! write does.

#![cfg(all(feature = "spill", not(feature = "sim")))]

mod common;

use std::time::Duration;

use sundog::{Cache, Cluster, Mode, SpillConfig};

fn fresh_temp_dir(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "sundog-it-spill-reads-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is after the unix epoch")
            .as_nanos()
    ))
}

/// How many of `keys` `cache` holds in RAM: [`Cache::get_sync`] never
/// reads the disk, so it answers only for a resident entry.
fn resident(cache: &Cache<u32, String>, keys: std::ops::Range<u32>) -> u64 {
    keys.filter(|key| cache.get_sync(key).is_some())
        .map(|_| 1)
        .sum()
}

/// Polls until `cache` holds at most `cap` of `keys` in RAM, or `timeout`
/// elapses, and returns the last count.
async fn settle_resident(
    cache: &Cache<u32, String>,
    keys: std::ops::Range<u32>,
    cap: u64,
    timeout: Duration,
) -> u64 {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let count = resident(cache, keys.clone());
        if count <= cap || tokio::time::Instant::now() >= deadline {
            return count;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn reading_every_spilled_key_keeps_the_cache_within_max_capacity() {
    const MAX_CAPACITY: u64 = 2_000;
    const ENTRIES: u32 = 8_000;
    const SETTLE: Duration = Duration::from_secs(10);

    let cluster = Cluster::builder("it-spill-reads-cap")
        .seeds(std::iter::empty())
        .config(common::fast_config())
        .build()
        .await
        .expect("solo node builds");
    let dir = fresh_temp_dir("within-cap");
    let cache = cluster
        .cache::<u32, String>("spill-reads")
        .mode(Mode::Local)
        .max_capacity(MAX_CAPACITY)
        .spill(SpillConfig::new(&dir, 4 * 1024 * 1024).region_bytes(256 * 1024))
        .open()
        .await
        .expect("cache opens");

    cache
        .insert_many((0..ENTRIES).map(|key| (key, format!("value-{key}"))))
        .await
        .expect("insert_many");
    let after_writes = settle_resident(&cache, 0..ENTRIES, MAX_CAPACITY, SETTLE).await;
    assert!(
        after_writes <= MAX_CAPACITY,
        "the writes leave at most {MAX_CAPACITY} resident, {after_writes} are"
    );

    for key in 0..ENTRIES {
        assert_eq!(
            cache.get(&key).await,
            Some(format!("value-{key}")),
            "key {key} reads back"
        );
    }
    let after_reads = settle_resident(&cache, 0..ENTRIES, MAX_CAPACITY, SETTLE).await;
    assert!(
        after_reads <= MAX_CAPACITY,
        "reading every key leaves at most {MAX_CAPACITY} resident, {after_reads} are"
    );
    assert_eq!(
        cache.entry_count().await,
        u64::from(ENTRIES),
        "every key stays live, in RAM or on disk"
    );

    cluster.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

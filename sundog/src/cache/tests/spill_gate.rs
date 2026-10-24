//! Tests of the spill configuration checks and of reads over spilled keys.

use super::*;

/// A directory path under the OS temp dir, unique to this test
/// process and call. Never created on disk: config validation in
/// `open()` is pure arithmetic and never touches the filesystem.
pub(super) fn fresh_spill_dir(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "sundog-spill-gate-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is after the unix epoch")
            .as_nanos()
    ))
}

#[tokio::test]
async fn open_returns_spill_unavailable_when_dir_is_a_regular_file() {
    let cluster = Cluster::builder("cache-it-spill-dir-is-a-file")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let path = fresh_spill_dir("dir-is-a-file");
    std::fs::write(&path, b"not a directory").expect("create a regular file at the spill dir path");

    let cfg = SpillConfig::new(&path, 1 << 20).region_bytes(4096);
    let err = cluster
        .cache::<u32, String>("scratch")
        .spill(cfg)
        .open()
        .await
        .expect_err("a spill dir that is actually a regular file cannot be created under");
    assert!(
        matches!(err, CacheError::SpillUnavailable { .. }),
        "expected SpillUnavailable, got {err:?}"
    );

    let _ = std::fs::remove_file(&path);
    cluster.shutdown().await;
}

#[tokio::test]
async fn open_rejects_a_spill_config_whose_capacity_is_under_two_regions() {
    let cluster = Cluster::builder("cache-it-spill-under-two-regions")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    // Default region_bytes is 64 MiB; one byte under two regions.
    let cfg = SpillConfig::new(fresh_spill_dir("under-two-regions"), 128 * 1024 * 1024 - 1);
    let err = cluster
        .cache::<u32, String>("scratch")
        .spill(cfg)
        .open()
        .await
        .expect_err("a capacity under two regions is rejected");

    assert!(
        matches!(err, CacheError::InvalidSpillConfig { .. }),
        "expected InvalidSpillConfig, got {err:?}"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn open_rejects_replicated_with_max_capacity_and_no_spill() {
    let cluster = Cluster::builder("cache-it-spill-no-spill-max-capacity")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let err = cluster
        .cache::<u32, String>("scratch")
        .mode(Mode::Replicated)
        .max_capacity(100)
        .open()
        .await
        .expect_err("Replicated + finite max_capacity + no spill is rejected");

    assert!(
        matches!(err, CacheError::ReplicatedWithLocalEviction { .. }),
        "expected ReplicatedWithLocalEviction, got {err:?}"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn open_accepts_replicated_with_max_capacity_once_spill_is_configured() {
    let cluster = Cluster::builder("cache-it-spill-relaxes-gate")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let cfg = SpillConfig::new(fresh_spill_dir("relaxes-gate"), 256 * 1024 * 1024);
    let cache = cluster
        .cache::<u32, String>("scratch")
        .mode(Mode::Replicated)
        .max_capacity(100)
        .spill(cfg)
        .open()
        .await
        .expect("Replicated + finite max_capacity is accepted once spill is configured");

    cache.close().await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn distributed_cache_opens_with_spill_and_a_capacity() {
    let cluster = Cluster::builder("cache-it-distributed-spill-relaxes-gate")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let cfg = SpillConfig::new(
        fresh_spill_dir("distributed-relaxes-gate"),
        256 * 1024 * 1024,
    );
    let cache = cluster
        .cache::<u32, String>("scratch")
        .mode(Mode::Distributed {
            owners: std::num::NonZeroU8::new(2).expect("nonzero"),
        })
        .max_capacity(100)
        .spill(cfg)
        .open()
        .await
        .expect("Distributed + finite max_capacity is accepted once spill is configured");

    cache.close().await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn open_still_rejects_replicated_with_tti_even_with_spill() {
    let cluster = Cluster::builder("cache-it-spill-tti-still-rejected")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let cfg = SpillConfig::new(fresh_spill_dir("tti-still-rejected"), 256 * 1024 * 1024);
    let err = cluster
        .cache::<u32, String>("scratch")
        .mode(Mode::Replicated)
        .tti(Duration::from_secs(30))
        .spill(cfg)
        .open()
        .await
        .expect_err("tti stays an unconditional error for Replicated, spill or not");

    assert!(
        matches!(err, CacheError::ReplicatedWithLocalEviction { .. }),
        "expected ReplicatedWithLocalEviction, got {err:?}"
    );

    cluster.shutdown().await;
}

/// Polls `cond` until it returns `true` or `timeout` elapses,
/// returning the final result either way. Never a fixed sleep.
async fn poll_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if cond() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return cond();
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Opens a cache under `mode` with a tiny `max_capacity` and a real
/// spill tier, inserts past that capacity, then waits up to a
/// bound for eviction to spill one of the two keys. Spilling is
/// observed via `get_sync` reading a miss, `Shard::get_sync`'s
/// documented contract for a currently-spilled entry. It then
/// reads the key back through `get` and asserts the disk
/// round-trip.
async fn spill_composes_with_max_capacity(mode: Mode, label: &str) {
    let cluster = Cluster::builder("cache-it-spill-compose")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let cfg = SpillConfig::new(fresh_spill_dir(label), 1 << 20).region_bytes(4096);
    let cache = cluster
        .cache::<u32, String>("scratch")
        .mode(mode)
        .max_capacity(1)
        .spill(cfg)
        .open()
        .await
        .expect("opens with spill composing with max_capacity");

    cache.insert(1, "one".to_string()).await.expect("insert 1");
    cache.insert(2, "two".to_string()).await.expect("insert 2");

    assert!(
        poll_until(Duration::from_secs(5), || {
            cache.get_sync(&1).is_none() || cache.get_sync(&2).is_none()
        })
        .await,
        "eviction spills exactly one of the two keys under a tiny max_capacity"
    );
    let (spilled_key, expected) = if cache.get_sync(&1).is_none() {
        (1u32, "one".to_string())
    } else {
        (2u32, "two".to_string())
    };
    assert_eq!(
        cache.get(&spilled_key).await,
        Some(expected),
        "the spilled key reads back correctly through the disk tier"
    );

    cache.close().await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn spill_composes_with_max_capacity_under_mode_local() {
    spill_composes_with_max_capacity(Mode::Local, "compose-local").await;
}

#[tokio::test]
async fn spill_composes_with_max_capacity_under_mode_invalidation() {
    spill_composes_with_max_capacity(Mode::Invalidation, "compose-invalidation").await;
}

/// A `Mode::Local` cache whose flush queue is full answers a read of
/// every spilled key from disk, with `get` or with `get_or_load`,
/// and deletes nothing: the read's promotion finds no room, because
/// the tier refuses every victim and a read never deletes one, so
/// the keys stay spilled, `entry_count` is unchanged and the loader
/// is never called.
#[tokio::test]
async fn a_local_cache_reads_spilled_keys_behind_a_full_queue_and_keeps_every_entry() {
    full_queue_reads_keep_every_entry("cache-it-spill-full-queue-reads", false).await;
}

/// [`a_local_cache_reads_spilled_keys_behind_a_full_queue_and_keeps_every_entry`]
/// through `Cache::get_or_load`.
#[tokio::test]
async fn a_local_cache_get_or_load_behind_a_full_queue_keeps_every_entry_and_skips_the_loader() {
    full_queue_reads_keep_every_entry("cache-it-spill-full-queue-loads", true).await;
}

async fn full_queue_reads_keep_every_entry(name: &str, load: bool) {
    const KEYS: u32 = 24;
    const CAP: u64 = 4;
    let cluster = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");
    let dir = fresh_spill_dir(name);
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let cache = cluster
        .cache::<u32, String>("scratch")
        .mode(Mode::Local)
        .max_capacity(CAP)
        .spill(cfg)
        .open()
        .await
        .expect("opens with a spill tier");
    cache
        .insert_many((0..KEYS).map(|k| (k, format!("value-{k}"))))
        .await
        .expect("insert_many");
    let resident = || (0..KEYS).filter(|k| cache.get_sync(k).is_some()).count();
    assert!(
        poll_until(Duration::from_secs(5), || resident() as u64 <= CAP).await,
        "the writes spill all but the cap"
    );
    let spilled: Vec<u32> = (0..KEYS).filter(|k| cache.get_sync(k).is_none()).collect();
    assert_eq!(spilled.len() as u64, u64::from(KEYS) - CAP);
    assert_eq!(cache.entry_count().await, u64::from(KEYS));

    let hold = cache.shard.debug_hold_flush_queue().await;
    let loads = std::sync::atomic::AtomicUsize::new(0);
    for &key in &spilled {
        let value = if load {
            cache
                .get_or_load(&key, async |_| {
                    loads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Ok::<_, std::io::Error>(String::new())
                })
                .await
                .ok()
        } else {
            cache.get(&key).await
        };
        assert_eq!(
            value,
            Some(format!("value-{key}")),
            "key {key} answers from disk"
        );
    }
    assert_eq!(
        loads.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "no read calls the loader"
    );
    assert_eq!(
        cache.entry_count().await,
        u64::from(KEYS),
        "no read deleted an entry"
    );
    assert_eq!(
        (0..KEYS).filter(|k| cache.get_sync(k).is_none()).count(),
        spilled.len(),
        "the reads left every spilled key on disk"
    );
    drop(hold);

    cache.close().await;
    cluster.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// `Shard::attach_spill` runs only for the `open()` that wins the
/// registry reservation, so the losing `open()` never touches the
/// first cache's region files.
#[tokio::test]
async fn reopening_the_same_spill_cache_name_returns_already_open_without_corrupting_it() {
    let cluster = Cluster::builder("cache-it-spill-reopen-guard")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let dir = fresh_spill_dir("reopen-guard");
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let cache = cluster
        .cache::<u32, String>("scratch")
        .max_capacity(1)
        .spill(cfg)
        .open()
        .await
        .expect("first open succeeds");

    cache.insert(1, "one".to_string()).await.expect("insert 1");
    cache.insert(2, "two".to_string()).await.expect("insert 2");
    assert!(
        poll_until(Duration::from_secs(5), || {
            cache.get_sync(&1).is_none() || cache.get_sync(&2).is_none()
        })
        .await,
        "eviction spills exactly one of the two keys under a tiny max_capacity"
    );
    let (spilled_key, expected) = if cache.get_sync(&1).is_none() {
        (1u32, "one".to_string())
    } else {
        (2u32, "two".to_string())
    };

    // Same name and SpillConfig, same directory. The registry
    // reservation must reject this second open before
    // `Shard::attach_spill` ever wipes and preallocates the first
    // cache's region files.
    let cfg2 = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let err = cluster
        .cache::<u32, String>("scratch")
        .max_capacity(1)
        .spill(cfg2)
        .open()
        .await
        .expect_err("the name is already open");
    assert!(
        matches!(err, CacheError::AlreadyOpen { .. }),
        "expected AlreadyOpen, got {err:?}"
    );

    assert_eq!(
        cache.get(&spilled_key).await,
        Some(expected),
        "the first cache's spilled value must still be readable after the rejected \
         second open"
    );

    cache.close().await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn close_stops_the_tier_and_a_surviving_clone_evicts_by_deleting() {
    let cluster = Cluster::builder("cache-it-spill-close-stops-tier")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let cfg = SpillConfig::new(fresh_spill_dir("close-stops-tier"), 1 << 20).region_bytes(4096);
    let cache = cluster
        .cache::<u32, String>("scratch")
        .max_capacity(1)
        .spill(cfg)
        .open()
        .await
        .expect("opens with spill");
    let surviving = cache.clone();

    assert!(!surviving.shard.spill_tier_closed(), "the tier starts open");
    cache.close().await;
    assert!(
        surviving.shard.spill_tier_closed(),
        "Cache::close stops the engine's attached spill tier, visible through any \
         surviving clone since they share one Shard"
    );

    // A surviving clone keeps working locally. With the tier
    // closed, capacity eviction on it deletes rather than spills,
    // since there is no live tier to hand a victim to. A spilled
    // entry still counts as live, so `entry_count` distinguishes a
    // delete from a spill.
    surviving
        .insert(1, "one".to_string())
        .await
        .expect("insert 1");
    surviving
        .insert(2, "two".to_string())
        .await
        .expect("insert 2");
    assert_eq!(
        surviving.entry_count().await,
        1,
        "eviction deletes rather than spills once the tier is closed"
    );

    cluster.shutdown().await;
}

/// Cache-level counterpart to the engine- and shard-level spilled-merge
/// tests: a counter spills between two colliding `Cache::merge` calls,
/// and the second still folds against the spilled side's real bytes
/// instead of dropping them.
#[tokio::test]
async fn merging_into_a_spilled_counter_folds_instead_of_dropping_a_side() {
    let cluster = Cluster::builder("cache-it-merge-spilled")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let cfg = SpillConfig::new(fresh_spill_dir("merge-spilled"), 1 << 20).region_bytes(4096);
    let cache = cluster
        .cache::<u32, PnCounter>("counters")
        .resolver(Arc::new(PnCounterResolver))
        .max_capacity(1)
        // Always exceeds the cap of 1 on its own, so the single
        // counter key spills right after the first merge instead of
        // needing a second key to create eviction pressure.
        .weigher(|_k: &u32, _v: &PnCounter| 2)
        .spill(cfg)
        .open()
        .await
        .expect("opens with spill");

    let writer_a = WriterId::new(NodeId::from(11), 1);
    let writer_b = WriterId::new(NodeId::from(22), 1);

    cache
        .merge(1, PnCounter::local_delta(writer_a, 3))
        .await
        .expect("first increment");

    assert!(
        poll_until(Duration::from_secs(5), || cache.get_sync(&1).is_none()).await,
        "the over-weight entry spills once eviction runs"
    );

    cache
        .merge(1, PnCounter::local_delta(writer_b, 4))
        .await
        .expect("second increment, colliding with the spilled record");

    assert_eq!(
        cache.get(&1).await.map(|c| c.value()),
        Some(7),
        "merging into a spilled record folds both sides' contributions instead of \
         dropping the one that was on disk"
    );

    cache.close().await;
    cluster.shutdown().await;
}

/// A cache in `mode` stamps key 42 a minute ahead of the system clock and
/// closes checkpointed; its successor on a fresh cluster, built without that
/// clock, reopens the same spill directory warm and so trails the replayed
/// stamp. Returns the successor, its cluster and the directory.
#[cfg(not(feature = "sim"))]
async fn cache_reopened_warm_behind_its_replayed_stamps(
    mode: Mode,
    label: &'static str,
) -> (Cluster, Cache<u32, String>, std::path::PathBuf) {
    let dir = fresh_spill_dir(label);
    let spill = || {
        SpillConfig::new(&dir, 1 << 20)
            .region_bytes(4096)
            .warm_reopen(true)
    };
    let cluster = Cluster::builder(label)
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");
    let first = cluster
        .cache::<u32, String>("warm-clock")
        .mode(mode)
        .clock(Arc::new(|| now_ms() + 60_000))
        .spill(spill())
        .open()
        .await
        .expect("opens");
    first.insert(42, "old".to_string()).await.expect("insert");
    first.close().await;
    cluster.shutdown().await;

    let cluster = Cluster::builder(label)
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");
    let second = cluster
        .cache::<u32, String>("warm-clock")
        .mode(mode)
        .spill(spill())
        .open()
        .await
        .expect("reopens");
    assert_eq!(
        second.get(&42).await,
        Some("old".to_string()),
        "the replay is warm: the old entry is readable"
    );
    (cluster, second, dir)
}

#[cfg(not(feature = "sim"))]
#[tokio::test]
async fn a_local_cache_reopened_warm_behind_its_replayed_stamps_accepts_a_new_write() {
    let (cluster, cache, dir) = cache_reopened_warm_behind_its_replayed_stamps(
        Mode::Local,
        "cache-it-warm-clock-local-write",
    )
    .await;
    cache.insert(42, "new".to_string()).await.expect("insert");
    assert_eq!(cache.get(&42).await, Some("new".to_string()));
    cache.close().await;
    cluster.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(not(feature = "sim"))]
#[tokio::test]
async fn a_local_cache_reopened_warm_behind_its_replayed_stamps_honours_a_remove() {
    let (cluster, cache, dir) = cache_reopened_warm_behind_its_replayed_stamps(
        Mode::Local,
        "cache-it-warm-clock-local-remove",
    )
    .await;
    cache.remove(&42).await.expect("remove");
    assert_eq!(cache.get(&42).await, None);
    cache.close().await;
    cluster.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(not(feature = "sim"))]
#[tokio::test]
async fn an_invalidation_cache_reopened_warm_behind_its_replayed_stamps_accepts_a_new_write() {
    let (cluster, cache, dir) = cache_reopened_warm_behind_its_replayed_stamps(
        Mode::Invalidation,
        "cache-it-warm-clock-inval-write",
    )
    .await;
    cache.insert(42, "new".to_string()).await.expect("insert");
    assert_eq!(cache.get(&42).await, Some("new".to_string()));
    cache.close().await;
    cluster.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(not(feature = "sim"))]
#[tokio::test]
async fn an_invalidation_cache_reopened_warm_behind_its_replayed_stamps_honours_a_remove() {
    let (cluster, cache, dir) = cache_reopened_warm_behind_its_replayed_stamps(
        Mode::Invalidation,
        "cache-it-warm-clock-inval-remove",
    )
    .await;
    cache.remove(&42).await.expect("remove");
    assert_eq!(cache.get(&42).await, None);
    cache.close().await;
    cluster.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// A `Mode::Local` cache over a warm-reopen tier in `dir`.
async fn open_warm(cluster: &Cluster, dir: &std::path::Path) -> Cache<u32, String> {
    cluster
        .cache::<u32, String>("warm-close")
        .mode(Mode::Local)
        .spill(
            SpillConfig::new(dir, 1 << 20)
                .region_bytes(4096)
                .warm_reopen(true),
        )
        .open()
        .await
        .expect("opens")
}

async fn solo_cluster_for(label: &'static str) -> Cluster {
    Cluster::builder(label)
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds")
}

/// The snapshot file a checkpointed close wrote under `dir`.
pub(super) fn snapshot_under(dir: &std::path::Path) -> std::path::PathBuf {
    let mut pending = vec![dir.to_path_buf()];
    while let Some(next) = pending.pop() {
        for entry in std::fs::read_dir(next).expect("the spill directory lists") {
            let path = entry.expect("a directory entry").path();
            if path.is_dir() {
                pending.push(path);
            } else if path.file_name().is_some_and(|name| name == "snapshot") {
                return path;
            }
        }
    }
    panic!(
        "a checkpointed close writes a snapshot under {}",
        dir.display()
    );
}

/// A clone closed after the name was reopened does not checkpoint its old
/// tier over the files the reopened cache wrote: its entries survive the
/// next warm reopen.
#[tokio::test]
async fn a_stale_close_after_a_warm_reopen_leaves_the_reopened_spill_tier_intact() {
    let dir = fresh_spill_dir("stale-close");
    let cluster = solo_cluster_for("cache-it-spill-stale-close").await;
    let first = open_warm(&cluster, &dir).await;
    first.insert(1, "one".to_string()).await.expect("insert");
    let stale = first.clone();
    first.close().await;

    let second = open_warm(&cluster, &dir).await;
    assert_eq!(second.get(&1).await, Some("one".to_string()), "warm replay");
    second.insert(2, "two".to_string()).await.expect("insert");
    second.close().await;
    stale.close().await;

    let third = open_warm(&cluster, &dir).await;
    assert_eq!(third.get(&1).await, Some("one".to_string()));
    assert_eq!(
        third.get(&2).await,
        Some("two".to_string()),
        "the stale close did not rewrite the snapshot the reopened cache wrote"
    );
    third.close().await;
    cluster.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// Two clones closing together run one checkpoint: both return once it is
/// done, and the next warm reopen replays the entry.
#[tokio::test]
async fn two_clones_closing_at_once_leave_one_checkpoint_to_reopen_warm() {
    let dir = fresh_spill_dir("join-close");
    let cluster = solo_cluster_for("cache-it-spill-join-close").await;
    let first = open_warm(&cluster, &dir).await;
    first.insert(1, "one".to_string()).await.expect("insert");
    let other = first.clone();
    tokio::join!(first.close(), other.close());

    let second = open_warm(&cluster, &dir).await;
    assert_eq!(second.get(&1).await, Some("one".to_string()));
    second.close().await;
    cluster.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// A close after the cluster's shutdown sweep has already checkpointed the
/// cache writes no second checkpoint.
#[tokio::test]
async fn a_close_after_the_shutdown_sweep_does_not_checkpoint_again() {
    let dir = fresh_spill_dir("after-shutdown");
    let cluster = solo_cluster_for("cache-it-spill-close-after-shutdown").await;
    let cache = open_warm(&cluster, &dir).await;
    cache.insert(1, "one".to_string()).await.expect("insert");
    cluster.shutdown().await;
    let snapshot = snapshot_under(&dir);
    std::fs::write(&snapshot, b"written by someone else").expect("overwrites the snapshot");

    cache.close().await;

    assert_eq!(
        std::fs::read(&snapshot).expect("reads the snapshot"),
        b"written by someone else"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A `close` cut short by a timeout while its checkpoint is joining the
/// flusher leaves the checkpoint running. The next `close` waits for it,
/// starts no second one, and the cache reopens warm.
#[tokio::test]
async fn a_close_dropped_in_a_blocking_stage_is_not_checkpointed_twice() {
    let dir = fresh_spill_dir("dropped-blocking-close");
    let cluster = solo_cluster_for("cache-it-spill-dropped-blocking-close").await;
    let first = cluster
        .cache::<u32, String>("warm-close")
        .mode(Mode::Local)
        .max_capacity(3)
        .spill(
            SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .warm_reopen(true),
        )
        .open()
        .await
        .expect("opens");
    // The flusher parks after the first job it takes, so the checkpoint's
    // join on it waits.
    first.shard.debug_pause_spill_flusher();
    for k in 0..20 {
        first.insert(k, format!("value-{k}")).await.expect("insert");
    }
    let other = first.clone();

    assert!(
        tokio::time::timeout(Duration::from_millis(300), first.clone().close())
            .await
            .is_err(),
        "the checkpoint waits on the paused flusher"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(300), other.clone().close())
            .await
            .is_err(),
        "the second close waits for the checkpoint under way"
    );
    assert!(
        !first.shard.spill_tier_closed(),
        "a second checkpoint did not start beside the first"
    );

    first.shard.debug_resume_spill_flusher();
    tokio::time::timeout(Duration::from_secs(10), other.close())
        .await
        .expect("the close ends once the flusher runs");
    assert!(first.shard.spill_tier_closed());
    assert_eq!(
        first.shard.debug_spill_checkpoint_runs(),
        1,
        "the retry waited for the checkpoint under way and wrote no second one"
    );

    let second = open_warm(&cluster, &dir).await;
    for k in 0..20 {
        assert_eq!(second.get(&k).await, Some(format!("value-{k}")));
    }
    second.close().await;
    cluster.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// A close whose checkpoint task ends early, here by a panic before its
/// first stage, leaves the tier open; the close of a clone after it runs the
/// checkpoint, and the cache reopens warm.
#[tokio::test]
async fn a_close_whose_checkpoint_task_panics_is_run_by_the_next_close() {
    let dir = fresh_spill_dir("panicked-close");
    let cluster = solo_cluster_for("cache-it-spill-panicked-close").await;
    let first = open_warm(&cluster, &dir).await;
    first.insert(1, "one".to_string()).await.expect("insert");
    let other = first.clone();
    first.shard.debug_panic_next_spill_checkpoint();

    first.close().await;
    assert!(
        !other.shard.spill_tier_closed(),
        "the panicked checkpoint closed nothing"
    );
    assert_eq!(other.shard.debug_spill_checkpoint_runs(), 0);

    other.clone().close().await;
    assert!(other.shard.spill_tier_closed());
    assert_eq!(
        other.shard.debug_spill_checkpoint_runs(),
        1,
        "the next close ran one checkpoint"
    );

    let second = open_warm(&cluster, &dir).await;
    assert_eq!(second.get(&1).await, Some("one".to_string()));
    second.close().await;
    cluster.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

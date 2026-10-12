//! Spill-tier traffic whose exact metric values the exporter test pins.

use crate::common;
use crate::support::{scrape_metrics, scraped_metric_value};
use std::net::SocketAddr;
use std::process::{Command, Stdio};
use std::time::Duration;
use sundog::{Cluster, Mode};

/// A directory path under the OS temp dir, unique to this test process,
/// call, and `label`. Never created on disk; [`sundog::SpillConfig::new`]'s
/// `SpillTier::open` creates it.
#[cfg(feature = "spill")]
fn fresh_spill_dir(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "sundog-it-prometheus-spill-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is after the unix epoch")
            .as_nanos()
    ))
}

/// The spill metrics pin: a tiny `max_capacity` plus a `spill` tier on
/// `cluster` forces one eviction-to-spill, and a single `get` of the
/// spilled key forces one disk hit and one promotion. Two further,
/// isolated caches then pin an overwrite-after-spill and a
/// remove-after-spill, each sized so the one operation under test settles
/// without triggering a second eviction of its own. Batch eviction can
/// otherwise clear more than one unit of weight per pass, which would make
/// an exact count depend on internal batching details rather than on the
/// behavior under test.
///
/// Runs inside `metrics_endpoint_serves_sundog_metrics_after_cache_ops`
/// rather than as its own `#[tokio::test]`, for the same reason
/// `seed_sketch_mismatch`/`seed_part_mismatch`/`count_hits_and_misses`
/// in `seeds` and `pins` do: a cache's `hits`/`misses`-style `metrics::Counter` handles
/// bind to whichever recorder is installed at the moment
/// `Shard::new`/`Shard::attach_spill` calls `metrics::counter!`, not
/// whatever gets installed later. Only the one test in this binary that
/// reliably owns the process-global recorder from the start, via
/// `prometheus_listen` synchronously early in `Cluster::builder(..).build()`,
/// can pin exact metric values. A second, independent `#[tokio::test]`
/// racing for the same slot either is a no-op, if it loses, or breaks the
/// first test's own `build()`, if it wins.
///
/// Returns every tier's scratch directory for the caller to clean up.
#[allow(
    clippy::too_many_lines,
    reason = "three isolated cache scenarios (promote, overwrite, remove), each with its own \
              setup and bounded wait, read better inline than split across helpers that would \
              each retake the same handful of parameters"
)]
#[cfg(feature = "spill")]
pub(crate) async fn spill_writes_and_promotes_pin_metrics(
    cluster: &Cluster,
) -> Vec<std::path::PathBuf> {
    let mut dirs = Vec::new();

    // --- "spilled": two evictions to spill, one disk-read promotion. The
    // read of the spilled key makes room for it under the one-entry cap by
    // spilling the other key first, so two writes land and one entry stays
    // on disk. ---
    let dir = fresh_spill_dir("promote");
    let cfg = sundog::SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let cache = cluster
        .cache::<u32, String>("spilled")
        .mode(Mode::Local)
        .max_capacity(1)
        .spill(cfg)
        .open()
        .await
        .expect("cache opens");

    cache.insert(1, "one".to_string()).await.expect("insert 1");
    cache.insert(2, "two".to_string()).await.expect("insert 2");
    common::eventually(Duration::from_secs(5), || async {
        cache.get_sync(&1).is_none() || cache.get_sync(&2).is_none()
    })
    .await;
    let spilled_key = if cache.get_sync(&1).is_none() {
        1u32
    } else {
        2u32
    };

    // One promotion: a single disk read of the spilled key, which spills
    // the other key to make room.
    let _ = cache.get(&spilled_key).await;
    let other_key = 3 - spilled_key;
    common::eventually(Duration::from_secs(5), || async {
        cache.get_sync(&other_key).is_none()
    })
    .await;
    dirs.push(dir);

    // --- "spill-overwrite": an overwrite of a currently-spilled key must
    // decrement sundog_spill_entries with no further disk write. `1`/`2`/`3`
    // fill a `max_capacity(2)` cache past its limit, spilling one of them;
    // removing one of the two others first frees the one unit of headroom
    // the overwrite below needs, so it settles at the cap instead of
    // forcing a second eviction.
    let dir = fresh_spill_dir("overwrite");
    let cfg = sundog::SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let cache = cluster
        .cache::<u32, String>("spill-overwrite")
        .mode(Mode::Local)
        .max_capacity(2)
        .spill(cfg)
        .open()
        .await
        .expect("cache opens");

    cache.insert(1, "one".to_string()).await.expect("insert 1");
    cache.insert(2, "two".to_string()).await.expect("insert 2");
    cache
        .insert(3, "three".to_string())
        .await
        .expect("insert 3");
    common::eventually(Duration::from_secs(5), || async {
        [1u32, 2, 3]
            .into_iter()
            .any(|k| cache.get_sync(&k).is_none())
    })
    .await;
    let spilled_key = [1u32, 2, 3]
        .into_iter()
        .find(|&k| cache.get_sync(&k).is_none())
        .expect("exactly one of the three keys spilled");
    let other_resident_key = [1u32, 2, 3]
        .into_iter()
        .find(|&k| k != spilled_key)
        .expect("the other two keys stay resident");
    cache
        .remove(&other_resident_key)
        .await
        .expect("free headroom for the overwrite below");
    cache
        .insert(spilled_key, "overwritten".to_string())
        .await
        .expect("overwrite the spilled key");
    dirs.push(dir);

    // --- "spill-remove": removing a currently-spilled key must decrement
    // sundog_spill_entries too, via apply_tombstone rather than apply_put.
    let dir = fresh_spill_dir("remove");
    let cfg = sundog::SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let cache = cluster
        .cache::<u32, String>("spill-remove")
        .mode(Mode::Local)
        .max_capacity(1)
        .spill(cfg)
        .open()
        .await
        .expect("cache opens");

    cache
        .insert(10, "ten".to_string())
        .await
        .expect("insert 10");
    cache
        .insert(11, "eleven".to_string())
        .await
        .expect("insert 11");
    common::eventually(Duration::from_secs(5), || async {
        cache.get_sync(&10).is_none() || cache.get_sync(&11).is_none()
    })
    .await;
    let spilled_key = if cache.get_sync(&10).is_none() {
        10u32
    } else {
        11u32
    };
    cache
        .remove(&spilled_key)
        .await
        .expect("remove the spilled key");
    dirs.push(dir);

    dirs
}

/// `sundog_spill_reopen_total`/`records_total`: a tiny-capacity
/// `Mode::Local` cache, `warm_reopen(true)`, spills one entry while a
/// second stays resident, closes cleanly, then reopens the same
/// directory, landing `outcome="warm"` with both installed. Four further
/// directories each pin one `outcome="cold_fallback"` reason:
/// `no_snapshot` (never closed), `config_mismatch` (`region_bytes`
/// changed), `downtime_exceeded` (past `tombstone_ttl`), and `bad_region`
/// (a truncated region file). `reason="disabled"` is pinned off the
/// `"spilled"` cache's first open in `main.rs`;
/// `reason="stale_snapshot"` is pinned at the unit level, since forging a
/// snapshot with a wrong `format_version` has no legitimate construction
/// here.
///
/// Returns every directory opened, for the caller to clean up.
#[cfg(feature = "spill")]
#[allow(
    clippy::too_many_lines,
    reason = "one scenario per cold-fallback reason, kept together"
)]
pub(crate) async fn spill_reopen_pins_metrics(cluster: &Cluster) -> Vec<std::path::PathBuf> {
    let mut dirs = Vec::new();

    // warm reopen: close cleanly, then reopen the same directory.
    let dir = fresh_spill_dir("reopen-warm");
    let cfg = sundog::SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .warm_reopen(true);
    let cache = cluster
        .cache::<u32, String>("spill-reopen-warm")
        .mode(Mode::Local)
        .max_capacity(1)
        .spill(cfg.clone())
        .open()
        .await
        .expect("first open, against a brand-new directory, opens cold");
    cache.insert(1, "one".to_string()).await.expect("insert 1");
    cache.insert(2, "two".to_string()).await.expect("insert 2");
    common::eventually(Duration::from_secs(5), || async {
        cache.get_sync(&1).is_none() || cache.get_sync(&2).is_none()
    })
    .await;
    // Closes without promoting the spilled key back resident; the
    // reopen recovers it from disk and the other from RAM.
    cache.close().await;

    let _reopened = cluster
        .cache::<u32, String>("spill-reopen-warm")
        .mode(Mode::Local)
        .max_capacity(1)
        .spill(cfg)
        .open()
        .await
        .expect("second open, against the same directory, reopens warm");
    dirs.push(dir);

    // no_snapshot: a brand-new directory, warm_reopen(true), never closed.
    let dir = fresh_spill_dir("reopen-no-snapshot");
    let cfg = sundog::SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .warm_reopen(true);
    let _fresh = cluster
        .cache::<u32, String>("spill-reopen-no-snapshot")
        .mode(Mode::Local)
        .spill(cfg)
        .open()
        .await
        .expect("a brand-new directory still opens, cold, with nothing to replay");
    dirs.push(dir);

    // config_mismatch: reopen with a different region_bytes than the snapshot.
    let dir = fresh_spill_dir("reopen-config-mismatch");
    let cfg = sundog::SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .warm_reopen(true);
    let cache = cluster
        .cache::<u32, String>("spill-reopen-config-mismatch")
        .mode(Mode::Local)
        .spill(cfg)
        .open()
        .await
        .expect("first open, against a brand-new directory, opens cold");
    cache.close().await;
    let resized = sundog::SpillConfig::new(&dir, 1 << 20)
        .region_bytes(8192)
        .warm_reopen(true);
    let _mismatched = cluster
        .cache::<u32, String>("spill-reopen-config-mismatch")
        .mode(Mode::Local)
        .spill(resized)
        .open()
        .await
        .expect("a resized tier still opens, cold, via the wipe-and-recreate fallback");
    dirs.push(dir);

    // downtime_exceeded: reopen once closed_at_ms is older than tombstone_ttl.
    let dir = fresh_spill_dir("reopen-downtime-exceeded");
    let cfg = sundog::SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .warm_reopen(true);
    let cache = cluster
        .cache::<u32, String>("spill-reopen-downtime-exceeded")
        .mode(Mode::Local)
        .spill(cfg.clone())
        .open()
        .await
        .expect("first open, against a brand-new directory, opens cold");
    cache.close().await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let _stale = cluster
        .cache::<u32, String>("spill-reopen-downtime-exceeded")
        .mode(Mode::Local)
        .spill(cfg)
        .open()
        .await
        .expect("a tier closed longer than tombstone_ttl still opens, cold");
    dirs.push(dir);

    // bad_region: reopen after one region file's length mismatches region_bytes.
    let dir = fresh_spill_dir("reopen-bad-region");
    let cfg = sundog::SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .warm_reopen(true);
    let cache = cluster
        .cache::<u32, String>("spill-reopen-bad-region")
        .mode(Mode::Local)
        .spill(cfg.clone())
        .open()
        .await
        .expect("first open, against a brand-new directory, opens cold");
    cache.close().await;
    let tier_dir = dir.join("spill-reopen-bad-region");
    let region_file = std::fs::read_dir(&tier_dir)
        .expect("the tier's own directory exists once opened")
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .find(|path| path.extension().and_then(|ext| ext.to_str()) == Some("reg"))
        .expect("a tier with at least one region has at least one .reg file on disk");
    std::fs::write(&region_file, b"short")
        .expect("truncate one region file to a length region_bytes no longer matches");
    let _bad_region = cluster
        .cache::<u32, String>("spill-reopen-bad-region")
        .mode(Mode::Local)
        .spill(cfg)
        .open()
        .await
        .expect("a tier with one corrupt region file still opens, cold");
    dirs.push(dir);

    dirs
}

/// Best-effort: makes every entry directly under `dir` immutable
/// (`chattr +i`), or mutable again on the reverse call. Checked by
/// ext2/3/4 on every write syscall, so it fails an already-open write
/// handle's `pwrite` with `EPERM`. Requires `CAP_LINUX_IMMUTABLE` and an
/// ext2/3/4 filesystem; returns `false` without changing anything
/// otherwise, so a caller can skip the scenario instead of failing.
#[cfg(feature = "spill")]
fn chattr_dir_entries(dir: &std::path::Path, immutable: bool) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    let paths: Vec<_> = entries
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .collect();
    if paths.is_empty() {
        return false;
    }
    let flag = if immutable { "+i" } else { "-i" };
    let run = |program: &str, prefix: &[&str]| {
        Command::new(program)
            .args(prefix)
            .arg(flag)
            .args(&paths)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
    };
    run("chattr", &[]) || run("sudo", &["-n", "chattr"])
}

/// Exercises the `reserve()`-backed metrics via `insert_many`, the one
/// public-API producer that threads a `Reservation` through
/// `apply_grouped` (single-key `insert` never calls `reserve()`). Also
/// exercises `sundog_spill_dropped_total{reason="disk_error"}`:
/// [`chattr_dir_entries`] makes the tier's region files immutable, so the
/// flusher's later `pwrite` fails with a real `EPERM`.
///
/// Returns the scratch directory, whether the fault was set up, and, if
/// so, the observed `disk_error` count. The caller skips the assertion
/// when the fault couldn't be set up; once set up, `None` is a real
/// failure.
#[cfg(feature = "spill")]
pub(crate) async fn disk_error_and_reserve_wait_pin_metrics(
    cluster: &Cluster,
    metrics_addr: SocketAddr,
) -> (std::path::PathBuf, bool, Option<f64>) {
    let dir = fresh_spill_dir("disk-error");
    let cache_name = "disk-error-pin";
    let cfg = sundog::SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let cache = cluster
        .cache::<u32, String>(cache_name)
        .mode(Mode::Local)
        .max_capacity(1)
        .spill(cfg)
        .open()
        .await
        .expect("cache opens");

    // SpillTier::open preallocates every region file synchronously,
    // so the directory is fully populated here already.
    let region_dir = dir.join(cache_name);
    let immutable = chattr_dir_entries(&region_dir, true);

    // insert_many threads one Reservation through apply_grouped for the
    // whole call, applying both entries regardless of a later eviction failure.
    cache
        .insert_many([(1u32, "one".to_string()), (2u32, "two".to_string())])
        .await
        .expect("insert_many applies both entries even when the eviction they force later fails");

    let observed_disk_error = if immutable {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(body) = scrape_metrics(metrics_addr).await
                && let Some(count) = scraped_metric_value(
                    &body,
                    "sundog_spill_dropped_total",
                    &[("cache", cache_name), ("reason", "disk_error")],
                )
            {
                break Some(count);
            }
            if tokio::time::Instant::now() >= deadline {
                break None;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    } else {
        None
    };

    // Restore permissions so the caller's cleanup can delete these files.
    let _ = chattr_dir_entries(&region_dir, false);

    (dir, immutable, observed_disk_error)
}

/// Drives a genuine `sundog_spill_wait_timeouts_total` increment through
/// the public API: `disk_error_and_reserve_wait_pin_metrics`'s tiny
/// scenario never needs `reserve()` to suspend, so that series is never
/// registered there.
///
/// A local `insert_many` burst doesn't reproduce this reliably, since
/// the flusher frees permits the instant it dequeues a job, well before
/// the disk write starts, racing ahead of even a 1-microsecond timeout.
/// A real state-transfer pull between two nodes does: a loopback TCP
/// round trip is consistently slower than the flusher's local dequeue,
/// so the joiner's reservation waits on network I/O, and the
/// 1-microsecond timeout loses that race deterministically.
///
/// Two scenario-local nodes, independent of the caller's own
/// `cluster`/`peer`: a donor fills `ENTRIES` records, then a joiner opens
/// with a tiny `flush_queue_bytes` and the too-short timeout, pulling the
/// whole dataset at `open()` time. Both nodes are shut down before this
/// returns. Returns the scratch directory. Makes no zero-drop claim;
/// `spill_backpressure.rs` covers that with a realistic timeout.
#[cfg(feature = "spill")]
pub(crate) async fn reserve_timeout_pin_metric() -> std::path::PathBuf {
    const REGION_BYTES: u64 = 2 * 1024 * 1024;
    const CAPACITY_BYTES: u64 = 32 * 1024 * 1024;
    const FLUSH_QUEUE_BYTES: u64 = 64 * 1024;
    const MAX_CAPACITY: u64 = 64;
    const ENTRIES: u32 = 80_000;
    /// Far too short to ever win a race against a genuine wait.
    const TOO_SHORT_TIMEOUT: Duration = Duration::from_micros(1);
    let cache_name = "reserve-timeout-pin";
    let cluster_name = "it-prometheus-exporter-reserve-timeout";

    let donor = Cluster::builder(cluster_name)
        .seeds(std::iter::empty())
        .config(common::fast_config())
        .build()
        .await
        .expect("donor builds");
    let donor_cache = donor
        .cache::<u32, String>(cache_name)
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("donor opens alone, unbounded and spill-free, owning everything");

    let mut start = 0u32;
    while start < ENTRIES {
        let end = (start + 500).min(ENTRIES);
        donor_cache
            .insert_many((start..end).map(|i| (i, "v".repeat(200))))
            .await
            .expect("bulk insert on the donor succeeds");
        start = end;
    }

    let dir = fresh_spill_dir("reserve-timeout");
    let cfg = sundog::SpillConfig::new(&dir, CAPACITY_BYTES)
        .region_bytes(REGION_BYTES)
        .flush_queue_bytes(FLUSH_QUEUE_BYTES)
        .spill_wait_timeout(TOO_SHORT_TIMEOUT);

    let joiner = Cluster::builder(cluster_name)
        .seeds([donor.local_gossip_addr()])
        .config(common::fast_config())
        .build()
        .await
        .expect("joiner builds");
    common::wait_for_peer_count(&joiner, 1, Duration::from_secs(15)).await;

    let started = std::time::Instant::now();
    let _joiner_cache = joiner
        .cache::<u32, String>(cache_name)
        .mode(Mode::Replicated)
        .max_capacity(MAX_CAPACITY)
        .spill(cfg)
        .open()
        .await
        .expect(
            "joiner opens, pulling the whole dataset from the donor even though its own \
             spill_wait_timeout is absurdly short",
        );
    eprintln!(
        "reserve-timeout-pin: joiner open() took {:?}",
        started.elapsed()
    );

    donor.shutdown().await;
    joiner.shutdown().await;

    dir
}

//! Integration test for spill admission control
//! (`SpillTier::reserve`/`Reservation`) through the public API only: a
//! bulk `insert_many` over a deliberately tiny flush queue drops nothing.
//! `pause_flusher`/`resume_flusher` are `pub(crate)` test-only hooks used
//! by `spill.rs`'s unit tests and unreachable here, so this drives a real
//! `Cluster` and on-disk `SpillTier` and saturates the admission
//! semaphore against real disk and channel throughput instead.
//!
//! The larger, multi-tens-of-MB pacing scenarios live in
//! `spill_replication.rs` instead of here: running them in the same
//! binary as this file's small, tightly-margined scenario measurably
//! raised this test's flake rate under real disk contention, since their
//! writes could delay this scenario's flusher long enough for
//! `pending_spill_weight` to outgrow one chunk's reservation.
//!
//! `sundog_spill_waiters` bumps on every `reserve()` call before the
//! acquire is awaited and drops on exit, so a nonzero reading only proves
//! `reserve()` ran, not that it suspended; the assertion using
//! it is a sanity check, not proof of a wait.
//!
//! Own test binary, so installing the process-global Prometheus recorder
//! here never races another test for the slot.

#![cfg(all(feature = "spill", feature = "prometheus", not(feature = "sim")))]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use sundog::{Cluster, Mode, SpillConfig};

/// This binary's lazily-installed claim on the process-global Prometheus
/// recorder slot, shared so concurrent `#[tokio::test]`s in this binary
/// don't race each other for it. Mirrors `spill_replication.rs::metrics_handle`.
fn metrics_handle() -> &'static sundog::PrometheusHandle {
    static HANDLE: std::sync::OnceLock<sundog::PrometheusHandle> = std::sync::OnceLock::new();
    HANDLE.get_or_init(|| {
        sundog::prometheus_handle()
            .expect("this file's own test binary is the sole claimant of the recorder slot")
    })
}

/// Fixed length every value in this file is padded to, so the
/// `flush_queue_bytes`/`capacity_bytes` choices below are exact multiples
/// of one record's real size.
const VALUE_LEN: usize = 256;

/// A fixed-length, easily eyeballed value: `v0000000042-xxxx...`, padded
/// with `x` to [`VALUE_LEN`] bytes.
fn fixed_value(i: u32) -> String {
    let prefix = format!("v{i:010}-");
    let pad = VALUE_LEN.saturating_sub(prefix.len());
    let mut value = String::with_capacity(VALUE_LEN);
    value.push_str(&prefix);
    value.extend(std::iter::repeat_n('x', pad));
    value
}

/// A directory under [`std::env::temp_dir`], unique to this process and
/// call; `SpillTier::open` creates it.
fn fresh_temp_dir(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "sundog-it-spill-backpressure-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is after the unix epoch")
            .as_nanos()
    ))
}

/// Finds `metric{label1="value1",...} <number>` in Prometheus
/// text-exposition `body`, tolerant of label ordering.
fn scraped_metric(body: &str, metric: &str, labels: &[(&str, &str)]) -> Option<f64> {
    let wanted: Vec<String> = labels
        .iter()
        .map(|&(k, v)| format!("{k}=\"{v}\""))
        .collect();
    body.lines().find_map(|line| {
        let rest = line.strip_prefix(metric)?;
        let rest = rest.strip_prefix('{')?;
        let (line_labels, value) = rest.split_once('}')?;
        let line_labels: Vec<&str> = line_labels.split(',').collect();
        if !wanted.iter().all(|w| line_labels.contains(&w.as_str())) {
            return None;
        }
        value.trim().parse::<f64>().ok()
    })
}

/// Rounds a scraped metric to its exact-integer count.
#[allow(
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    reason = "every metric read here is a nonnegative counter or gauge"
)]
fn metric_count(body: &str, metric: &str, labels: &[(&str, &str)]) -> u64 {
    scraped_metric(body, metric, labels).unwrap_or(0.0).round() as u64
}

/// Bulk `insert_many` over a deliberately tiny flush queue drops nothing.
/// Each `CHUNK`-sized call reserves its whole chunk up front
/// (`Shard::apply_grouped`), so evictions stay covered by
/// `Reservation::spend`; `CHUNK` stays under 40% of `FLUSH_QUEUE_BYTES` so
/// a chunk's reservation is never clamped.
#[allow(
    clippy::too_many_lines,
    reason = "one self-contained scenario: open, a concurrent sampler, the chunked insert loop, and one metric-by-metric assertion block"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bulk_insert_over_a_saturated_flush_queue_drops_nothing() {
    const REGION_BYTES: u64 = 256 * 1024;
    const CAPACITY_BYTES: u64 = 2 * 1024 * 1024;
    /// Small enough, against the ~640 KB this test spills, that a
    /// byte-only `queued_bytes` bound would drop most of the burst.
    const FLUSH_QUEUE_BYTES: u64 = 4 * 1024;
    const MAX_CAPACITY: u64 = 100;
    const ENTRIES: u32 = 2_000;
    /// `CHUNK * ~320` bytes/record sits at about 40% of
    /// `FLUSH_QUEUE_BYTES`, leaving margin so a chunk's reservation is
    /// never clamped.
    const CHUNK: u32 = 5;
    const CACHE_NAME: &str = "bulk";

    let handle = metrics_handle();

    let gossip = common::reserve_gossip_addr().await;
    let cluster = Cluster::builder("it-spill-backpressure-bulk")
        .seeds(std::iter::empty())
        .config(common::fast_config().with(|c| c.gossip_bind_addr = gossip))
        .build()
        .await
        .expect("solo node builds");

    let dir = fresh_temp_dir("bulk-zero-drops");
    let cfg = SpillConfig::new(&dir, CAPACITY_BYTES)
        .region_bytes(REGION_BYTES)
        .flush_queue_bytes(FLUSH_QUEUE_BYTES)
        // Generous past the 2s default: concurrent test binaries can
        // delay this scenario's flusher enough to turn a slow drain into
        // a spurious deferred drop.
        .spill_wait_timeout(Duration::from_secs(20));
    let cache = cluster
        .cache::<u32, String>(CACHE_NAME)
        .mode(Mode::Local)
        .max_capacity(MAX_CAPACITY)
        .spill(cfg)
        .open()
        .await
        .expect("cache opens with a tiny flush queue");

    // Polls sundog_spill_waiters as fast as yield_now allows. Confirms
    // reserve() was exercised repeatedly; cannot prove a genuine wait
    // over an instant, uncontended acquire.
    let seen_waiter = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let sampler = tokio::spawn({
        let seen_waiter = Arc::clone(&seen_waiter);
        let stop = Arc::clone(&stop);
        let handle = handle.clone();
        async move {
            while !stop.load(Ordering::Relaxed) {
                let waiters = metric_count(
                    &handle.render(),
                    "sundog_spill_waiters",
                    &[("cache", CACHE_NAME)],
                );
                if waiters > 0 {
                    seen_waiter.store(true, Ordering::Relaxed);
                }
                tokio::task::yield_now().await;
            }
        }
    });

    let mut start = 0u32;
    while start < ENTRIES {
        let end = (start + CHUNK).min(ENTRIES);
        cache
            .insert_many((start..end).map(|i| (i, fixed_value(i))))
            .await
            .expect("insert_many succeeds even under a saturated flush queue");
        start = end;
    }

    stop.store(true, Ordering::Relaxed);
    sampler.await.expect("the sampler task never panics");

    let body = handle.render();
    let dropped_queue_full = metric_count(
        &body,
        "sundog_spill_dropped_total",
        &[("cache", CACHE_NAME), ("reason", "queue_full")],
    );
    let dropped_deferred = metric_count(
        &body,
        "sundog_spill_dropped_total",
        &[("cache", CACHE_NAME), ("reason", "deferred")],
    );
    let wait_timeouts = metric_count(
        &body,
        "sundog_spill_wait_timeouts_total",
        &[("cache", CACHE_NAME)],
    );
    let waiters_at_rest = metric_count(&body, "sundog_spill_waiters", &[("cache", CACHE_NAME)]);
    let spilled_entries = metric_count(&body, "sundog_spill_entries", &[("cache", CACHE_NAME)]);

    assert_eq!(
        dropped_queue_full, 0,
        "a bulk insert chunked to fit its own reservation must never refuse for queue_full"
    );
    assert_eq!(
        dropped_deferred, 0,
        "a bulk insert chunked to fit its own reservation must never refuse for deferred"
    );
    assert_eq!(
        wait_timeouts, 0,
        "the 20s spill_wait_timeout is never actually reached at this scenario's real-disk, \
         small-chunk scale, even under concurrent contention from other test binaries"
    );
    assert_eq!(
        waiters_at_rest, 0,
        "every reserve() call's RAII waiter guard has dropped by the time insert_many has returned"
    );
    assert!(
        spilled_entries > 0,
        "a 100-entry max_capacity against 2000 inserts must actually spill something"
    );
    assert!(
        seen_waiter.load(Ordering::Relaxed),
        "the sampler never observed sundog_spill_waiters > 0 across the whole load: reserve() \
         may not have been exercised at all, which would mean this scenario is not actually \
         driving apply_grouped's reservation path"
    );

    for k in (0..ENTRIES).step_by(37) {
        assert_eq!(
            cache.get(&k).await,
            Some(fixed_value(k)),
            "key {k} must be fetchable, resident or spilled, with the value it was inserted with"
        );
    }

    cache.close().await;
    cluster.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// Unlike the chunked scenario above, this drives the whole run through
/// **one** `insert_many` call whose reservation structurally cannot cover
/// its eviction backlog, forcing `Shard::apply_grouped` past the
/// reservation and onto `Shard::retry_reservation_deficit`'s bounded
/// retry.
///
/// `ENTRIES` (2,000) records at ~280 bytes each request a ~560,000-byte
/// reservation, but `SpillTier::reserve` clamps it to `FLUSH_QUEUE_BYTES`
/// (4,096), covering under 1% of the run; `retry_reservation_deficit`
/// pays down the rest in rounds, each still capped at 4,096 bytes, so
/// zero drops here proves the retry loop closes the gap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bulk_insert_in_one_call_over_a_flush_queue_too_small_for_the_first_reservation_drops_nothing()
 {
    const REGION_BYTES: u64 = 256 * 1024;
    const CAPACITY_BYTES: u64 = 2 * 1024 * 1024;
    /// Far under one record's share of the 2,000-record run; see this
    /// function's doc for the arithmetic.
    const FLUSH_QUEUE_BYTES: u64 = 4 * 1024;
    const MAX_CAPACITY: u64 = 100;
    const ENTRIES: u32 = 2_000;
    const CACHE_NAME: &str = "bulk-one-call";

    let handle = metrics_handle();

    let gossip = common::reserve_gossip_addr().await;
    let cluster = Cluster::builder("it-spill-backpressure-bulk-one-call")
        .seeds(std::iter::empty())
        .config(common::fast_config().with(|c| c.gossip_bind_addr = gossip))
        .build()
        .await
        .expect("solo node builds");

    let dir = fresh_temp_dir("bulk-one-call-zero-drops");
    let cfg = SpillConfig::new(&dir, CAPACITY_BYTES)
        .region_bytes(REGION_BYTES)
        .flush_queue_bytes(FLUSH_QUEUE_BYTES)
        // Generous: real disk contention plus many small retry-loop drains.
        .spill_wait_timeout(Duration::from_secs(20));
    let cache = cluster
        .cache::<u32, String>(CACHE_NAME)
        .mode(Mode::Local)
        .max_capacity(MAX_CAPACITY)
        .spill(cfg)
        .open()
        .await
        .expect("cache opens with a tiny flush queue");

    // One call, every record: no chunking, so the one reservation is
    // clamped far below the run's Put bytes.
    cache
        .insert_many((0..ENTRIES).map(|i| (i, fixed_value(i))))
        .await
        .expect(
            "insert_many succeeds even though its own reservation covers only a sliver of \
             what this one call needs to evict",
        );

    let body = handle.render();
    let dropped_queue_full = metric_count(
        &body,
        "sundog_spill_dropped_total",
        &[("cache", CACHE_NAME), ("reason", "queue_full")],
    );
    let dropped_deferred = metric_count(
        &body,
        "sundog_spill_dropped_total",
        &[("cache", CACHE_NAME), ("reason", "deferred")],
    );
    let spilled_entries = metric_count(&body, "sundog_spill_entries", &[("cache", CACHE_NAME)]);

    assert_eq!(
        dropped_queue_full, 0,
        "one insert_many call whose own reservation is clamped to {FLUSH_QUEUE_BYTES} bytes, \
         far under this {ENTRIES}-record run's real eviction need, must still drop nothing: \
         Shard::retry_reservation_deficit pays the rest down after the per-bucket loop"
    );
    assert_eq!(
        dropped_deferred, 0,
        "same call, same reservation shortfall against the run: zero deferred drops too"
    );
    assert!(
        spilled_entries > 0,
        "a {MAX_CAPACITY}-entry max_capacity against {ENTRIES} inserts must actually spill \
         something"
    );

    for k in (0..ENTRIES).step_by(37) {
        assert_eq!(
            cache.get(&k).await,
            Some(fixed_value(k)),
            "key {k} must be fetchable, resident or spilled, with the value it was inserted with"
        );
    }

    cache.close().await;
    cluster.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

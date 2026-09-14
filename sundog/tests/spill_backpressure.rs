//! Public-API integration test for the semaphore-based spill admission
//! control (`SpillTier::reserve`/`Reservation`): a bulk `insert_many` over a
//! deliberately tiny flush queue drops nothing.
//!
//! `SpillTier::pause_flusher`/`resume_flusher` are `pub(crate)` test-only
//! hooks (`spill.rs`'s own unit tests use them to hold `admit` saturated
//! for an exact, deterministic duration) and unreachable from an
//! integration-test binary outside the crate. This scenario instead drives
//! a real, loopback, in-process `Cluster` and a real on-disk `SpillTier`
//! through the public API only, sizing `SpillConfig::flush_queue_bytes` far
//! below the data volume the scenario moves so the admission semaphore
//! genuinely saturates against real disk and channel throughput, not a
//! paused flusher.
//!
//! The rebalance/anti-entropy pacing test and the too-short-timeout test
//! this workstream's spec also calls for live in `spill_replication.rs`
//! instead of here, appended alongside its own `Mode::Replicated`
//! scenario: both drive a much larger, multi-tens-of-MB bulk transfer to
//! get a reliable signal, and running that concurrently with this file's
//! own tiny, tightly-margined scenario in the same test binary measurably
//! increased this test's flake rate under real disk contention (observed
//! directly on this suite's own sandbox), the two heavier scenarios'
//! large writes could occasionally delay this scenario's own flusher
//! thread long enough for `pending_spill_weight` (committed but not yet
//! physically installed, `engine.rs`'s own doc on `enforce_capacity`) to
//! grow past what one small chunk's own reservation was sized to cover.
//! Splitting them into separate test binaries removes that specific,
//! avoidable cross-scenario contention.
//!
//! One consequence of measuring through the public API alone, documented
//! here rather than silently worked around: `sundog_spill_waiters` bumps
//! on every `reserve()` call unconditionally (`Inner::record_waiter_delta`
//! runs before the acquire is even awaited) and drops on every exit, so
//! observing it above zero proves only that `reserve()` was called and a
//! concurrent sampler's own OS thread happened to catch it mid-flight:
//! not that the call genuinely suspended rather than resolving on an
//! uncontended, effectively instant acquire. The one assertion below that
//! uses it treats it only as a sanity check that `reserve()` was
//! exercised at all, not as proof of a genuine wait.
//!
//! Its own test binary, a separate process from every other `tests/*.rs`
//! file, so installing the process-global Prometheus recorder here never
//! races another test for the slot.

#![cfg(all(feature = "spill", feature = "prometheus", not(feature = "sim")))]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use sundog::{Cluster, Mode, SpillConfig};

/// This binary's one claim on the process-global Prometheus recorder
/// slot, installed lazily on first use and shared by every test that
/// runs after it: two `#[tokio::test]`s in one binary run concurrently by
/// default, so each calling `sundog::prometheus_handle()` directly would
/// race the other for the single process-global slot instead of sharing
/// it. Mirrors `tests/spill_replication.rs::metrics_handle`.
fn metrics_handle() -> &'static sundog::PrometheusHandle {
    static HANDLE: std::sync::OnceLock<sundog::PrometheusHandle> = std::sync::OnceLock::new();
    HANDLE.get_or_init(|| {
        sundog::prometheus_handle()
            .expect("this file's own test binary is the sole claimant of the recorder slot")
    })
}

/// Every value this file writes is padded to exactly this many bytes,
/// mirroring `tests/spill_bench.rs`'s own fixed-length convention: makes
/// the byte math behind every `flush_queue_bytes`/`capacity_bytes` choice
/// below an intentional multiple of one record's real size rather than a
/// guess.
const VALUE_LEN: usize = 256;

/// A fixed-length, easily eyeballed value: `v0000000042-xxxx...`, padded
/// with `x` out to [`VALUE_LEN`] bytes regardless of `i`'s digit count.
/// Mirrors `tests/spill_bench.rs::bench_value`.
fn fixed_value(i: u32) -> String {
    let prefix = format!("v{i:010}-");
    let pad = VALUE_LEN.saturating_sub(prefix.len());
    let mut value = String::with_capacity(VALUE_LEN);
    value.push_str(&prefix);
    value.extend(std::iter::repeat_n('x', pad));
    value
}

/// A directory under [`std::env::temp_dir`], unique to this process and
/// this call, never created ahead of time: `SpillTier::open` creates it.
/// Mirrors `tests/spill_bench.rs::fresh_temp_dir`.
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
/// text-exposition `body`, tolerant of label ordering and
/// integer-vs-float rendering. Mirrors `tests/spill_bench.rs::scraped_metric`.
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

/// Every `sundog_spill_*` counter/gauge this file reads holds an
/// exact-integer count. Mirrors `tests/spill_bench.rs::metric_count`.
#[allow(
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    reason = "every metric read here is a nonnegative counter or gauge"
)]
fn metric_count(body: &str, metric: &str, labels: &[(&str, &str)]) -> u64 {
    scraped_metric(body, metric, labels).unwrap_or(0.0).round() as u64
}

/// Bulk `insert_many` over a deliberately tiny flush queue drops nothing:
/// each `CHUNK`-sized `insert_many` call reserves its own whole chunk's
/// encoded `Put` bytes up front (`Shard::apply_grouped`'s one
/// `reserve().await`), so every eviction that chunk's own writes cause is
/// structurally covered by `Reservation::spend` and never falls back to
/// the non-blocking `admit.try_acquire_many` refusal this cache's tiny
/// `flush_queue_bytes` would otherwise trigger throughout the load. `CHUNK`
/// is sized well under `FLUSH_QUEUE_BYTES` (roughly 40%) precisely so this
/// holds without ever exercising `reserve`'s own clamp-to-total fallback
/// path (`SpillTier::reserve`'s doc): this test's point is the reservation
/// mechanism working as designed, not the correctness backstop that covers
/// its known sizing-proxy limitation.
#[allow(
    clippy::too_many_lines,
    reason = "one self-contained scenario: open, a concurrent sampler, the chunked insert loop, and one metric-by-metric assertion block"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bulk_insert_over_a_saturated_flush_queue_drops_nothing() {
    const REGION_BYTES: u64 = 256 * 1024;
    const CAPACITY_BYTES: u64 = 2 * 1024 * 1024;
    /// Roughly 1/20th of the ~640 KB this test actually spills: small
    /// enough that a channel whose slot count stays fixed at
    /// `FLUSH_QUEUE_CAPACITY` (8192) regardless of `flush_queue_bytes`,
    /// paired with a byte-only `queued_bytes` bound, would drop most of a
    /// burst this size with `queue_full`.
    const FLUSH_QUEUE_BYTES: u64 = 4 * 1024;
    const MAX_CAPACITY: u64 = 100;
    const ENTRIES: u32 = 2_000;
    /// `CHUNK * ~320` (a real record's header+key+value length, rounded up
    /// generously) sits at about 40% of `FLUSH_QUEUE_BYTES`, leaving
    /// comfortable margin so a whole chunk's reservation is never clamped.
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
        // Generous past the 2s default: `cargo test --workspace` runs many
        // test binaries concurrently, so this scenario's own flusher
        // thread can occasionally see real, contention-driven scheduling
        // delays with nothing to do with disk speed. A short timeout
        // under that contention would turn a merely-slow-to-drain chunk
        // into a spurious deferred drop, which is exactly the false
        // failure this test must not produce.
        .spill_wait_timeout(Duration::from_secs(20));
    let cache = cluster
        .cache::<u32, String>(CACHE_NAME)
        .mode(Mode::Local)
        .max_capacity(MAX_CAPACITY)
        .spill(cfg)
        .open()
        .await
        .expect("cache opens with a tiny flush queue");

    // A concurrent sampler polling `sundog_spill_waiters{cache}` (a gauge
    // `Inner::record_waiter_delta` bumps on every `reserve()` call, before
    // the acquire is even awaited, and drops on every exit) as fast as
    // `tokio::task::yield_now` will schedule it. This cannot, on its own,
    // tell a genuine multi-poll suspension apart from an instant,
    // uncontended acquire that this sampler's other OS thread happened to
    // observe mid-flight; what it does confirm is that `reserve()` itself
    // was exercised, repeatedly, by this chunked load, i.e. that the
    // reservation path this test targets was actually on this run's call
    // graph and not silently bypassed.
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

/// The gap a reservation sized only from one call's own new Put bytes
/// leaves when it cannot cover the eviction backlog a real, merely-slow
/// flusher accumulates: unlike `bulk_insert_over_a_saturated_flush_queue_
/// drops_nothing` above, deliberately chunked so every chunk's own
/// reservation is always sufficient (its own doc comment says so), this
/// scenario drives the whole run through **one** `insert_many` call whose
/// own reservation structurally cannot cover it, forcing
/// `Shard::apply_grouped`'s per-bucket loop past the reservation entirely
/// and onto `Shard::retry_reservation_deficit`'s bounded post-loop retry.
///
/// The arithmetic, made explicit rather than left to be eyeballed:
/// `ENTRIES` (2,000) records at roughly 280 real encoded bytes each
/// (`HEADER_LEN` + a ~11-byte key + the 256-byte value) put this one
/// call's own *requested* reservation at roughly 2,000 × 280 ≈ 560,000
/// bytes. `SpillTier::reserve` clamps any request to at most the tier's
/// own total permit count (`FLUSH_QUEUE_BYTES` here, 4,096), so the one
/// reservation this call actually receives covers at most about 14 of its
/// 2,000 records, under 1% of the run. Every other eviction this call
/// triggers must be paid down by `retry_reservation_deficit`'s repeated
/// `reserve`-then-`enforce_capacity_with_reservation` rounds, each still
/// individually capped at 4,096 bytes by that same clamp, waiting on the
/// real flusher between rounds to free room. Zero `deferred`/`queue_full`
/// drops here is what proves that retry loop, not a bigger up-front
/// reservation, is what closes the gap the high-severity review finding
/// against this workstream's first cut described.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bulk_insert_in_one_call_over_a_flush_queue_too_small_for_the_first_reservation_drops_nothing()
 {
    const REGION_BYTES: u64 = 256 * 1024;
    const CAPACITY_BYTES: u64 = 2 * 1024 * 1024;
    /// Far under one record's own share of the 2,000-record run this
    /// scenario drives through a single `insert_many` call: see this
    /// function's own doc comment for the exact arithmetic this constant
    /// is chosen against.
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
        // Generous: real disk under concurrent test-binary contention,
        // plus this scenario's own retry loop needing many real,
        // individually-small flusher drains to pay its deficit down, not
        // just one.
        .spill_wait_timeout(Duration::from_secs(20));
    let cache = cluster
        .cache::<u32, String>(CACHE_NAME)
        .mode(Mode::Local)
        .max_capacity(MAX_CAPACITY)
        .spill(cfg)
        .open()
        .await
        .expect("cache opens with a tiny flush queue");

    // One call, every record: no chunking anywhere in this scenario, so
    // `Shard::apply_grouped`'s one reservation is sized from, and then
    // immediately clamped far below, this whole run's own Put bytes.
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

//! A spilled entry's digest fingerprint is bit-for-bit identical to a
//! resident one, so anti-entropy repairs a peer's dropped copy from a donor
//! whose own copy is, by the time the repair runs, sitting on disk rather
//! than in RAM. This exercises the AE-pull-reply path's off-lock
//! spilled-value read, end to end.
//!
//! Also carries the rebalance/anti-entropy backpressure-pacing test and
//! its too-short-timeout sibling, both driving a fresh `Mode::Replicated`
//! node's `open()`-time whole-cache pull at tens of MB.
//! `spill_backpressure.rs` does not host them: sharing a binary with its
//! tiny bulk-insert scenario raised its flake rate under disk contention.
//!
//! Own test binary, so installing the process-global Prometheus recorder
//! here never races another test; [`metrics_handle`] shares one
//! installation across this file's three tests.

#![cfg(all(feature = "spill", feature = "prometheus", not(feature = "sim")))]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use sundog::{Cache, Cluster, Mode, PrometheusHandle, SpillConfig};

/// This binary's lazily-installed claim on the process-global Prometheus
/// recorder slot. Mirrors `spill_bench.rs::metrics_handle`.
fn metrics_handle() -> &'static PrometheusHandle {
    static HANDLE: std::sync::OnceLock<PrometheusHandle> = std::sync::OnceLock::new();
    HANDLE.get_or_init(|| {
        sundog::prometheus_handle()
            .expect("this file's own test binary is the sole claimant of the recorder slot")
    })
}

/// Finds `metric{...,label="value",...} <number>` in Prometheus
/// text-exposition `body`, tolerant of label ordering and
/// integer-vs-float rendering. Mirrors `tests/prometheus_exporter.rs`'s own
/// `scraped_metric_value`, kept local since integration test binaries don't
/// share code beyond `mod common`.
fn metric_value(body: &str, metric: &str, label: (&str, &str)) -> Option<f64> {
    let wanted = format!("{}=\"{}\"", label.0, label.1);
    body.lines().find_map(|line| {
        let rest = line.strip_prefix(metric)?;
        let rest = rest.strip_prefix('{')?;
        let (labels, value) = rest.split_once('}')?;
        if !labels.split(',').any(|pair| pair == wanted) {
            return None;
        }
        value.trim().parse::<f64>().ok()
    })
}

/// [`metric_value`]'s multi-label counterpart.
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

/// Fixed length every value [`join_after_bulk_insert`] writes is padded to.
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
        "sundog-it-spill-repl-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is after the unix epoch")
            .as_nanos()
    ))
}

/// A tiny `max_capacity` plus a `SpillConfig` on node `a` forces it to
/// spill some of what it inserts; node `b` stays unbounded and spill-free.
/// Live fan-out delivers every key to `b` first. Wiping one of `b`'s
/// entries without a tombstone means only anti-entropy can bring it back,
/// and by the time it runs, `a`'s own copy may already be on disk, so the
/// repair only succeeds if the AE-pull-reply path's spilled-value read,
/// `ShardOps::records_for`, works. Once repaired, further anti-entropy
/// rounds with nothing new to reconcile settle the repair counter at a
/// fixed value.
#[tokio::test]
async fn replicated_two_node_spill_converges_and_settles_to_zero_repairs() {
    let handle = metrics_handle();

    let gossip_a = common::reserve_gossip_addr().await;
    let gossip_b = common::reserve_gossip_addr().await;
    let cluster_a = Cluster::builder("it-spill-repl")
        .seeds([gossip_b])
        .config(common::fast_config().with(|c| c.gossip_bind_addr = gossip_a))
        .build()
        .await
        .expect("node a builds");
    let cluster_b = Cluster::builder("it-spill-repl")
        .seeds([gossip_a])
        .config(common::fast_config().with(|c| c.gossip_bind_addr = gossip_b))
        .build()
        .await
        .expect("node b builds");
    common::wait_for_peer_count(&cluster_a, 1, Duration::from_secs(15)).await;
    common::wait_for_peer_count(&cluster_b, 1, Duration::from_secs(15)).await;

    let dir = std::env::temp_dir().join(format!(
        "sundog-it-spill-repl-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is after the unix epoch")
            .as_nanos()
    ));
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let cache_a = cluster_a
        .cache::<u32, String>("orders")
        .mode(Mode::Replicated)
        .max_capacity(2)
        .spill(cfg)
        .open()
        .await
        .expect("a opens with spill composing with Replicated's max_capacity");
    let cache_b = cluster_b
        .cache::<u32, String>("orders")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("b opens, unbounded and spill-free");

    for k in 0..5u32 {
        cache_a
            .insert(k, format!("value-{k}"))
            .await
            .expect("insert");
    }

    // Live fan-out delivers every key to b.
    common::eventually(Duration::from_secs(10), || async {
        for k in 0..5u32 {
            if cache_b.get(&k).await.is_none() {
                return false;
            }
        }
        true
    })
    .await;

    // a's tiny max_capacity spills at least one of the five under eviction.
    common::eventually(Duration::from_secs(10), || async {
        (0..5u32).any(|k| cache_a.get_sync(&k).is_none())
    })
    .await;

    // Wipe one key on b without a tombstone: only anti-entropy repairs it,
    // and a's own copy may already be sitting on disk rather than in RAM.
    cache_b.invalidate_local(&0).await;
    assert_eq!(cache_b.get(&0).await, None);

    common::eventually(Duration::from_secs(15), || async {
        cache_b.get(&0).await.is_some()
    })
    .await;
    assert_eq!(
        cache_b.get(&0).await,
        Some("value-0".to_string()),
        "anti-entropy repairs the dropped entry with the correct value even when the donor's \
         own copy is currently spilled"
    );

    // Steady state: with nothing left to reconcile, the repair counter
    // stops moving across further anti-entropy rounds. This is a quiescence
    // check, "nothing happens for a while," which a bounded poll cannot
    // express. A poll returns the instant its condition first holds, so it
    // could observe `before == after` after only one round, missing a
    // repair that lands one round later. Two fixed windows are the
    // deliberate exception to this file's own bounded-poll rule. `fast_
    // config`'s 150ms ae_interval means two 500ms windows span several
    // rounds each, giving the counter ample opportunity to move, should it
    // move at all.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let before = metric_value(
        &handle.render(),
        "sundog_ae_repaired_total",
        ("cache", "orders"),
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    let after = metric_value(
        &handle.render(),
        "sundog_ae_repaired_total",
        ("cache", "orders"),
    );
    assert!(
        before.is_some_and(|v| v >= 1.0),
        "expected at least the one repair above to have been counted; got {before:?}"
    );
    assert_eq!(
        before, after,
        "a steady state with nothing new to write settles to zero further repairs"
    );

    cache_a.close().await;
    cache_b.close().await;
    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// Builds a solo warm `Mode::Replicated` node `a` with `entries` values,
/// then joins node `b` against it, opening `b` with `spill_cfg`/
/// `max_capacity`. Returns both clusters/caches and `b`'s `open()`
/// wall-clock duration.
async fn join_after_bulk_insert(
    cluster_name: &str,
    cache_name: &str,
    entries: u32,
    spill_cfg: SpillConfig,
    max_capacity: u64,
) -> (
    Cluster,
    Cache<u32, String>,
    Cluster,
    Cache<u32, String>,
    Duration,
) {
    let gossip_a = common::reserve_gossip_addr().await;
    let cluster_a = Cluster::builder(cluster_name)
        .seeds(std::iter::empty())
        .config(common::fast_config().with(|c| c.gossip_bind_addr = gossip_a))
        .build()
        .await
        .expect("node a builds");
    let cache_a = cluster_a
        .cache::<u32, String>(cache_name)
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("a opens alone, unbounded and spill-free, owning everything");

    let mut start = 0u32;
    while start < entries {
        let end = (start + 500).min(entries);
        cache_a
            .insert_many((start..end).map(|i| (i, fixed_value(i))))
            .await
            .expect("bulk insert on the donor succeeds");
        start = end;
    }

    let gossip_b = common::reserve_gossip_addr().await;
    let cluster_b = Cluster::builder(cluster_name)
        .seeds([gossip_a])
        .config(common::fast_config().with(|c| c.gossip_bind_addr = gossip_b))
        .build()
        .await
        .expect("node b builds");
    common::wait_for_peer_count(&cluster_b, 1, Duration::from_secs(15)).await;

    let started = Instant::now();
    let cache_b = cluster_b
        .cache::<u32, String>(cache_name)
        .mode(Mode::Replicated)
        .max_capacity(max_capacity)
        .spill(spill_cfg)
        .open()
        .await
        .expect("b opens, pulling the whole dataset from a");
    let elapsed = started.elapsed();

    (cluster_a, cache_a, cluster_b, cache_b, elapsed)
}

/// Runs `fut` while a sampler polls `sundog_spill_waiters{cache=cache_name}`
/// as fast as `yield_now` allows. Returns `fut`'s output plus whether the
/// gauge was ever seen above zero; that observation is logged context,
/// not proof of a genuine suspension (see
/// `a_saturated_flush_queue_measurably_slows_the_joiners_pull_from_donor`).
async fn observe_waiters_during<F, T>(cache_name: &'static str, fut: F) -> (T, bool)
where
    F: std::future::Future<Output = T>,
{
    let seen_waiter = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let sampler = tokio::spawn({
        let seen_waiter = Arc::clone(&seen_waiter);
        let stop = Arc::clone(&stop);
        async move {
            while !stop.load(Ordering::Relaxed) {
                let waiters = metric_count(
                    &metrics_handle().render(),
                    "sundog_spill_waiters",
                    &[("cache", cache_name)],
                );
                if waiters > 0 {
                    seen_waiter.store(true, Ordering::Relaxed);
                }
                tokio::task::yield_now().await;
            }
        }
    });
    let result = fut.await;
    stop.store(true, Ordering::Relaxed);
    sampler.await.expect("the sampler task never panics");
    (result, seen_waiter.load(Ordering::Relaxed))
}

/// A saturated flush queue measurably slows node `b`'s joiner-bootstrap
/// pull, confirming admission backpressure reaches the replication path
/// and not only the local write path it shares.
///
/// Compares saturated `flush_queue_bytes` (`reserve()` repeatedly waits)
/// against generous (`reserve` essentially never waits). The proof is the
/// wall-clock difference in `b`'s `open()` time; `sundog_spill_waiters`
/// is logged context only, since it bumps before the acquire is awaited
/// and can't tell "never waited" from "waited and got room instantly".
/// `sundog_spill_wait_timeouts_total` is likewise logged, not asserted at
/// zero: the retry loop can legitimately exhaust the timeout here, so a
/// real timeout is expected, and every key staying correct below is what
/// proves it costs no correctness.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_saturated_flush_queue_measurably_slows_the_joiners_pull_from_donor() {
    /// Large enough that the variable cost (disk/channel throughput under
    /// a saturated semaphore) dominates the fixed cost every joiner pays
    /// (gossip settle, one anti-entropy round); at 10k entries the fixed
    /// cost alone swamped any real admission delay.
    const ENTRIES: u32 = 80_000;
    const MAX_CAPACITY: u64 = 64;
    const REGION_BYTES: u64 = 2 * 1024 * 1024;
    const CAPACITY_BYTES: u64 = 32 * 1024 * 1024;
    /// A small fraction of the ~22 MB [`ENTRIES`] moves.
    const SATURATED_FLUSH_QUEUE_BYTES: u64 = 64 * 1024;
    /// The whole disk budget; `reserve` should never have to wait here.
    const GENEROUS_FLUSH_QUEUE_BYTES: u64 = CAPACITY_BYTES;
    /// Observed: generous ~0.6-0.7s; saturated ~1.9s or more, always
    /// over a second. 300ms is a conservative fraction of the smallest
    /// observed gap.
    const MIN_SLOWDOWN: Duration = Duration::from_millis(300);

    let handle = metrics_handle();

    let dir_saturated = fresh_temp_dir("pace-saturated");
    let cfg_saturated = SpillConfig::new(&dir_saturated, CAPACITY_BYTES)
        .region_bytes(REGION_BYTES)
        .flush_queue_bytes(SATURATED_FLUSH_QUEUE_BYTES)
        .spill_wait_timeout(Duration::from_secs(15));
    let (
        (
            cluster_donor_sat,
            cache_donor_sat,
            cluster_joiner_sat,
            cache_joiner_sat,
            saturated_elapsed,
        ),
        saw_waiter_saturated,
    ) = observe_waiters_during(
        "pace-saturated",
        join_after_bulk_insert(
            "it-pace-saturated",
            "pace-saturated",
            ENTRIES,
            cfg_saturated,
            MAX_CAPACITY,
        ),
    )
    .await;

    let dir_generous = fresh_temp_dir("pace-generous");
    let cfg_generous = SpillConfig::new(&dir_generous, CAPACITY_BYTES)
        .region_bytes(REGION_BYTES)
        .flush_queue_bytes(GENEROUS_FLUSH_QUEUE_BYTES)
        .spill_wait_timeout(Duration::from_secs(15));
    let (
        (
            cluster_donor_gen,
            cache_donor_gen,
            cluster_joiner_gen,
            cache_joiner_gen,
            generous_elapsed,
        ),
        saw_waiter_generous,
    ) = observe_waiters_during(
        "pace-generous",
        join_after_bulk_insert(
            "it-pace-generous",
            "pace-generous",
            ENTRIES,
            cfg_generous,
            MAX_CAPACITY,
        ),
    )
    .await;

    eprintln!(
        "pacing: saturated_open={saturated_elapsed:?} (waiter seen: {saw_waiter_saturated}) \
         generous_open={generous_elapsed:?} (waiter seen: {saw_waiter_generous})"
    );

    for k in (0..ENTRIES).step_by(1777) {
        assert_eq!(cache_joiner_sat.get(&k).await, Some(fixed_value(k)));
        assert_eq!(cache_joiner_gen.get(&k).await, Some(fixed_value(k)));
    }

    let body = handle.render();
    // Logged, not asserted at zero: a real timeout here is an expected
    // outcome of the retry design (see the test doc), not a bug.
    let saturated_timeouts = metric_count(
        &body,
        "sundog_spill_wait_timeouts_total",
        &[("cache", "pace-saturated")],
    );
    eprintln!("pacing: saturated_timeouts={saturated_timeouts}");

    // sundog_spill_waiters can't distinguish "never waited" from "waited
    // and got room instantly", so it is logged context only; the
    // wall-clock comparison below is the actual proof.
    assert!(
        saturated_elapsed >= generous_elapsed + MIN_SLOWDOWN,
        "a saturated flush queue must measurably slow the joiner's own bulk pull: \
         saturated={saturated_elapsed:?} generous={generous_elapsed:?} (need at least \
         {MIN_SLOWDOWN:?} more)"
    );

    cache_donor_sat.close().await;
    cache_joiner_sat.close().await;
    cluster_donor_sat.shutdown().await;
    cluster_joiner_sat.shutdown().await;
    cache_donor_gen.close().await;
    cache_joiner_gen.close().await;
    cluster_donor_gen.shutdown().await;
    cluster_joiner_gen.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir_saturated);
    let _ = std::fs::remove_dir_all(&dir_generous);
}

/// A `spill_wait_timeout` far shorter than any real wait degrades to the
/// non-blocking refuse/keep-resident fallback instead of hanging the
/// pull: `b`'s `open()` still completes, and since `b` is
/// `Mode::Replicated`, a refused eviction stays resident rather than
/// being lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_too_short_spill_wait_timeout_degrades_to_a_clean_retry_not_a_hang() {
    /// Same scale as the saturated-pacing test; at 10k entries `reserve()`
    /// resolved too quickly to ever hit a too-short timeout.
    const ENTRIES: u32 = 80_000;
    const MAX_CAPACITY: u64 = 64;
    const REGION_BYTES: u64 = 2 * 1024 * 1024;
    const CAPACITY_BYTES: u64 = 32 * 1024 * 1024;
    const FLUSH_QUEUE_BYTES: u64 = 64 * 1024;
    /// Shorter than a real wake-up (context switch plus timer-wheel
    /// granularity), so any suspending `reserve()` call loses the race.
    /// `Duration::ZERO` is avoided since it takes a different, non-blocking
    /// code path instead of a genuine timeout race.
    const TOO_SHORT_TIMEOUT: Duration = Duration::from_micros(1);
    /// Generous ceiling past the 20s default budget; not an expected duration.
    const MUST_NOT_HANG_WITHIN: Duration = Duration::from_secs(30);

    let handle = metrics_handle();

    let dir = fresh_temp_dir("timeout-clean-retry");
    let cfg = SpillConfig::new(&dir, CAPACITY_BYTES)
        .region_bytes(REGION_BYTES)
        .flush_queue_bytes(FLUSH_QUEUE_BYTES)
        .spill_wait_timeout(TOO_SHORT_TIMEOUT);

    let (cluster_a, cache_a, cluster_b, cache_b, elapsed) = tokio::time::timeout(
        MUST_NOT_HANG_WITHIN,
        join_after_bulk_insert(
            "it-timeout-clean-retry",
            "timeout-clean-retry",
            ENTRIES,
            cfg,
            MAX_CAPACITY,
        ),
    )
    .await
    .expect(
        "a too-short spill_wait_timeout must fall through to the non-blocking fallback and \
         return, never hang the whole join past a generous ceiling",
    );

    eprintln!("timeout-fallback: b's own open() took {elapsed:?}");

    let body = handle.render();
    let wait_timeouts = metric_count(
        &body,
        "sundog_spill_wait_timeouts_total",
        &[("cache", "timeout-clean-retry")],
    );
    assert!(
        wait_timeouts > 0,
        "a 1-microsecond spill_wait_timeout against a genuinely saturated flush queue must \
         actually time out at least once, proving the fallback path this test targets was \
         exercised rather than every reserve() call happening to win its race"
    );

    for k in (0..ENTRIES).step_by(1777) {
        assert_eq!(
            cache_b.get(&k).await,
            Some(fixed_value(k)),
            "every key must still be fetchable with the right value: a refused eviction stays \
             resident on a Mode::Replicated cache, it is never lost"
        );
    }

    cache_a.close().await;
    cache_b.close().await;
    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

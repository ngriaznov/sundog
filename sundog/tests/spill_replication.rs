//! A spilled entry's digest fingerprint is bit-for-bit identical to a
//! resident one, so anti-entropy repairs a peer's dropped copy from a donor
//! whose own copy is, by the time the repair runs, sitting on disk rather
//! than in RAM. This exercises the AE-pull-reply path's off-lock
//! spilled-value read, end to end.
//!
//! Also carries the workstream's rebalance/anti-entropy backpressure-pacing
//! test and its too-short-`spill_wait_timeout` sibling
//! (`a_saturated_flush_queue_measurably_slows_the_joiners_pull_from_donor`,
//! `a_too_short_spill_wait_timeout_degrades_to_a_clean_retry_not_a_hang`):
//! both drive a fresh `Mode::Replicated` node's own `open()`-time
//! whole-cache pull, the same `state_transfer::run`/`pull_from_donor`/
//! `apply_remote_batch` chain `rebalance.rs::try_donor_buckets` and
//! anti-entropy repair share, at tens of MB rather than this file's own
//! five-key scenario. `tests/spill_backpressure.rs` deliberately does not
//! host them: its own tiny, tightly-margined bulk-insert scenario measured
//! a real, higher flake rate under the disk contention these two heavier
//! scenarios produce when sharing one test binary.
//!
//! Its own test binary, a separate process from every other `tests/*.rs`
//! file, so installing the process-global Prometheus recorder here never
//! races another test for the slot; three tests share it, so
//! [`metrics_handle`] lazily installs it once instead of each test racing
//! its own direct `sundog::prometheus_handle()` call.

#![cfg(all(feature = "spill", feature = "prometheus", not(feature = "sim")))]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use sundog::{Cache, Cluster, Mode, PrometheusHandle, SpillConfig};

/// This binary's one claim on the process-global Prometheus recorder slot,
/// installed lazily on first use and shared by every test that runs after
/// it. Mirrors `tests/spill_bench.rs::metrics_handle`.
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

/// [`metric_value`]'s multi-label counterpart, for a metric keyed by more
/// than one label (e.g. `cache` and `reason` together). Mirrors
/// `tests/spill_bench.rs::scraped_metric`.
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

/// Every value [`join_after_bulk_insert`]'s scenarios write is padded to
/// exactly this many bytes. Mirrors `tests/spill_bench.rs::VALUE_LEN`.
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

/// Builds a solo `Mode::Replicated` node `a`, no spill, fully warm with
/// `entries` fixed-size values already inserted, then joins a second node
/// `b` against it and opens `b`'s own `Mode::Replicated` cache with
/// `spill_cfg`/`max_capacity`. `b`'s `open()` call is exactly
/// `state_transfer::run`'s whole-cache joiner-bootstrap pull, the same
/// mechanism a fresh `Mode::Replicated` node always runs at `open()`:
/// `apply_remote_batch`'s per-chunk `reserve()` gate applies here exactly
/// as it does to live replication or anti-entropy repair (`spec.md` §2).
///
/// Returns both clusters/caches, kept alive so a caller can verify
/// correctness before tearing down, `b`'s own `open()` wall-clock duration,
/// and `b`'s spill directory (for the caller to clean up).
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

/// Runs `fut` to completion while a concurrent sampler polls
/// `sundog_spill_waiters{cache=cache_name}` as fast as this runtime will
/// schedule it (`tokio::task::yield_now`, not a timed sleep: a genuine
/// `reserve()` suspension here is real-disk-and-channel-bound, often well
/// under a millisecond). Returns `fut`'s own output alongside whether the
/// gauge was ever observed above zero during that window. See this file's
/// own module doc and
/// `a_saturated_flush_queue_measurably_slows_the_joiners_pull_from_donor`'s
/// doc for why that observation is logged context, not load-bearing proof
/// of a genuine suspension.
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

/// A saturated flush queue measurably slows down node `b`'s whole-cache
/// joiner-bootstrap pull from `a`, confirming admission backpressure
/// genuinely reaches `apply_remote_batch`'s replication path and not only
/// the local write path it shares a mechanism with (`spec.md` §2's
/// rebalance/anti-entropy/replication coupling, exercised here through the
/// simplest producer that shares the same `pull_from_donor`/
/// `apply_remote_batch` chain: a fresh `Mode::Replicated` joiner's own
/// `open()`-time pull).
///
/// Compares two otherwise-identical scenarios differing only in
/// `flush_queue_bytes`: "saturated" (a small fraction of the whole
/// dataset's bytes, so `reserve()` genuinely has to wait for the flusher to
/// free room, repeatedly, across the pull) versus "generous" (the whole
/// disk budget, comfortably more than the whole dataset, so `reserve`
/// essentially never has to wait). `SpillTier::pause_flusher` is
/// `pub(crate)` and unreachable here (this file's own module doc), so the
/// proof this test rests on is a real wall-clock difference in `b`'s own
/// `open()` time, attributable to the one variable that changed, at a data
/// volume large enough for that variable cost to dominate the fixed cost
/// every joiner pays regardless (loopback gossip settle, the one
/// anti-entropy reconcile round `state_transfer::run` always runs once a
/// donor pull lands). `sundog_spill_waiters` is *not* that proof: it bumps
/// on every `reserve()` call unconditionally (`Inner::record_waiter_delta`
/// runs before the acquire is even awaited), so it cannot by itself
/// distinguish "never had to wait" from "waited and immediately got room":
/// [`observe_waiters_during`]'s result is logged below purely as extra
/// context, not asserted on.
///
/// `sundog_spill_wait_timeouts_total` is also logged rather than asserted
/// at zero: `Shard::retry_reservation_deficit`'s own post-loop retry pays
/// down whatever deficit one reservation, sized only from one chunk's own
/// bytes, could not cover, against this scenario's genuinely,
/// persistently saturated queue (`MAX_CAPACITY`/`SATURATED_FLUSH_QUEUE_
/// BYTES` far below what the pull needs throughout its run, not a
/// transient spike), that retry legitimately spends whatever is left of
/// this whole chunk's own `spill_wait_timeout` budget (shared with its own
/// initial reservation, never a fresh budget of its own) before giving up
/// on one chunk, so a real timeout firing here is an expected outcome of
/// the design, not a bug: the reservation mechanism's correctness backstop
/// (§7 of the spec) is exactly this bounded wait-then-fall-back, one
/// `spill_wait_timeout` per chunk total, and every key's own value staying
/// correct below is what proves it never costs correctness.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_saturated_flush_queue_measurably_slows_the_joiners_pull_from_donor() {
    /// Large enough that the two scenarios' *variable* cost (real disk and
    /// channel throughput under a saturated admission semaphore) dominates
    /// the *fixed* cost every joiner pays regardless (loopback gossip
    /// settle, the anti-entropy reconcile round `state_transfer::run`
    /// always runs once a donor pull lands): at a much smaller 10k-entry
    /// scale that fixed cost alone (measured around 110-140ms on this
    /// suite's own sandbox) swamped any real admission delay entirely.
    const ENTRIES: u32 = 80_000;
    const MAX_CAPACITY: u64 = 64;
    const REGION_BYTES: u64 = 2 * 1024 * 1024;
    const CAPACITY_BYTES: u64 = 32 * 1024 * 1024;
    /// A small fraction of the ~22 MB [`ENTRIES`] worth of 256-byte values
    /// actually moves.
    const SATURATED_FLUSH_QUEUE_BYTES: u64 = 64 * 1024;
    /// The whole disk budget, comfortably more than the whole dataset:
    /// `reserve` should never genuinely have to wait for room here.
    const GENEROUS_FLUSH_QUEUE_BYTES: u64 = CAPACITY_BYTES;
    /// Observed on this suite's own sandbox: generous consistently
    /// ~0.6-0.7s; saturated ranges from ~1.9s up into the tens of seconds
    /// once `Shard::retry_reservation_deficit`'s own post-loop retry
    /// spends a real chunk of this scenario's 15s `spill_wait_timeout`
    /// waiting for the flusher to catch up (this scenario's queue is
    /// genuinely, persistently saturated throughout the whole pull, not
    /// just briefly), always comfortably over a second regardless. 300ms
    /// is a deliberately conservative fraction of the smallest observed
    /// gap, proof against a slower or more loaded machine without
    /// weakening the claim this test makes.
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
    // Logged, not asserted at zero: see this test's own doc for why a
    // genuine timeout here, from `Shard::retry_reservation_deficit`'s own
    // post-loop retry against this scenario's persistently saturated
    // queue, is an expected outcome of the reservation-deficit design, not
    // a bug: the `get` loop just above is what proves it costs no
    // correctness regardless of how many fire.
    let saturated_timeouts = metric_count(
        &body,
        "sundog_spill_wait_timeouts_total",
        &[("cache", "pace-saturated")],
    );
    eprintln!("pacing: saturated_timeouts={saturated_timeouts}");

    // `sundog_spill_waiters` bumps on every `reserve()` call unconditionally
    // (`Inner::record_waiter_delta`, called before the acquire is even
    // awaited), so an instant, uncontended acquire can still transiently
    // touch it on a multi-thread runtime racing a concurrent sampler onto
    // another core; it cannot, by itself, distinguish "never had to wait"
    // from "waited and immediately got room." It is not this test's load-
    // bearing signal (kept only as one more data point in the log line
    // above): the actual proof a saturated flush queue measurably slows
    // this pull is the wall-clock comparison below, at a scale where the
    // two scenarios' fixed costs (loopback settle, one anti-entropy
    // reconcile round) are the same and only the admission-semaphore
    // behavior differs.
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

/// A `spill_wait_timeout` far shorter than any of this scenario's real
/// waits degrades to the existing non-blocking refuse/keep-resident
/// fallback instead of hanging the whole joiner-bootstrap pull: every
/// `apply_remote_batch` call still returns, `b`'s `open()` still completes
/// well inside the 20s `state_transfer_budget`, and, since `b` is
/// `Mode::Replicated` (`keep_resident_when_refused` is set for it), every
/// refused eviction simply stays resident rather than being lost, so
/// correctness holds regardless of how many timeouts fire.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_too_short_spill_wait_timeout_degrades_to_a_clean_retry_not_a_hang() {
    /// The same scale
    /// `a_saturated_flush_queue_measurably_slows_the_joiners_pull_from_donor`
    /// uses for its own "saturated" scenario: at a much smaller 10k-entry
    /// scale, `reserve()` resolves so quickly against real disk that a
    /// too-short timeout was never actually reached.
    const ENTRIES: u32 = 80_000;
    const MAX_CAPACITY: u64 = 64;
    const REGION_BYTES: u64 = 2 * 1024 * 1024;
    const CAPACITY_BYTES: u64 = 32 * 1024 * 1024;
    const FLUSH_QUEUE_BYTES: u64 = 64 * 1024;
    /// Far too short to ever win the race against a genuine wait: a real
    /// wake-up alone (context switch plus timer-wheel granularity) takes
    /// longer than this, so any `reserve()` call that actually needs to
    /// suspend loses to the timeout, exercising the fallback rather than
    /// happening to succeed anyway. `Duration::ZERO` is deliberately not
    /// used here: it takes a documented, different code path (`reserve`
    /// degenerates to a single non-blocking check with no `.await` at
    /// all) rather than a genuine timeout race.
    const TOO_SHORT_TIMEOUT: Duration = Duration::from_micros(1);
    /// Generous past the 20s default `state_transfer_budget`: this is the
    /// "must not hang" ceiling, not an expected duration.
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

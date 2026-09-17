//! `--headless <SECS>`: preload, run the write load and fetch sampler for a
//! fixed duration, killing and restarting one node partway through to
//! exercise rebalance, pause it, poll a bounded convergence check, verify a
//! random sample of surviving keys, and print a one-line report. Returns a
//! nonzero status on divergence or a failed sample: the CI-friendly smoke
//! test.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rand::random_range;

use crate::cli::Args;
use crate::convergence::{self, Convergence};
use crate::metrics::{self, Metrics};
use crate::node::{DISOWN_GRACE_ROUNDS, NodeSlot};
use crate::preload::key_for;
use crate::report::{self, Report};
use crate::rss;
use crate::setup;

/// How many surviving keys the post-convergence sample check fetches and
/// verifies against the load's expected value.
const SAMPLE_SIZE: usize = 2_000;
/// Bounds the retry loop that skips already-removed keys while sampling.
const SAMPLE_ATTEMPT_CAP: usize = SAMPLE_SIZE * 20;

/// How long the killed node stays down before the headless run restarts it:
/// `min(duration / 4, tombstone_ttl / 2)`. Capping at half the tombstone TTL
/// keeps the downtime inside `SpillConfig::warm_reopen`'s budget (it falls
/// back cold once downtime exceeds the tombstone TTL), so a spill run
/// exercises the warm reopen instead of always cold-falling-back.
#[must_use]
fn restart_delay(duration: Duration, tombstone_ttl: Duration) -> Duration {
    (duration / 4).min(tombstone_ttl / 2)
}

/// Runs the headless smoke check, returning `0` if the live nodes converged
/// and the sample check passed, `1` otherwise.
///
/// # Errors
///
/// Returns an error if the cluster fails to bootstrap.
#[allow(
    clippy::too_many_lines,
    reason = "one scripted end-to-end run: preload, kill, restart, converge, sample, report"
)]
pub(crate) async fn run(args: &Args, duration: Duration) -> anyhow::Result<i32> {
    // The recorder must exist before the first node opens, or every metric
    // registered until then stays on the no-op recorder.
    let metrics = match args.metrics {
        Some(interval) => Some((Arc::new(Metrics::install()?), interval)),
        None => None,
    };
    let started = Instant::now();
    let mut demo = setup::bootstrap(args).await?;
    // The event feed is unbounded; drain it since only the TUI reads it.
    let (drained_tx, drained_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut feed = std::mem::replace(&mut demo.feed_rx, drained_rx);
    drop(drained_tx);
    tokio::spawn(async move { while feed.recv().await.is_some() {} });
    let demo = demo;

    print_banner(args, duration);
    // Tracks the largest RSS reading across every sample this run takes
    // (preload, each periodic tick, and just before the kill), in
    // kibibytes to match `rss::read_rss_kb`.
    let peak_rss_kb = Arc::new(AtomicU64::new(0));
    let reporter = metrics.as_ref().map(|(metrics, interval)| {
        tokio::spawn(report_periodically(
            Arc::clone(metrics),
            *interval,
            Arc::clone(&demo.nodes),
            args.tuning.spill_dir.clone(),
            started,
            Arc::clone(&peak_rss_kb),
        ))
    });

    let preload_report = demo.wait_for_preload().await;
    let preload_rss_kb = record_rss_sample(&peak_rss_kb);
    println!(
        "preload: {} keys in {:.1}s ({:.0} keys/s), RSS {}",
        preload_report.keys,
        preload_report.elapsed.as_secs_f64(),
        preload_report.keys_per_sec(),
        rss::format_rss(preload_rss_kb)
    );
    if let Some((metrics, _)) = &metrics {
        println!("metrics after preload:\n{}", metrics.dump());
    }

    // Kill one node at the midpoint and bring it back after a bounded
    // downtime, so the run exercises a real rebalance under live load and
    // the reopen lands inside the tombstone TTL's warm-reopen budget.
    let killed_index = 0usize;
    let half = duration / 2;
    let downtime = restart_delay(duration, setup::TOMBSTONE_TTL);
    let restart_at = half + downtime;
    tokio::time::sleep(half).await;
    let steady_rss_kb = record_rss_sample(&peak_rss_kb);
    demo.nodes[killed_index].kill(&demo.feed_tx).await;
    println!(
        "headless: killed node{killed_index} at {}s to exercise rebalance",
        half.as_secs()
    );

    tokio::time::sleep(downtime).await;
    let restarted = demo.nodes[killed_index]
        .restart(&demo.topology, &demo.feed_tx)
        .await;
    record_rss_sample(&peak_rss_kb);
    if restarted {
        println!(
            "headless: restarted node{killed_index} at {}s",
            restart_at.as_secs()
        );
    } else {
        println!(
            "headless: restart of node{killed_index} at {}s FAILED",
            restart_at.as_secs()
        );
    }

    tokio::time::sleep(duration.saturating_sub(restart_at)).await;
    demo.paused.store(true, Ordering::Relaxed);
    // Grace for whatever write/fetch tick is in flight to land.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let deadline = convergence::poll_deadline(setup::AE_INTERVAL, DISOWN_GRACE_ROUNDS);
    let convergence_report = convergence::poll(
        &demo.nodes,
        u64::from(demo.topology.owners.get()),
        || demo.state.surviving_keys(),
        deadline,
    )
    .await;
    record_rss_sample(&peak_rss_kb);

    let fetch_summary = report_fetches_and_sample(&demo, &convergence_report).await;
    let mut failed = convergence_report.is_diverged() || fetch_summary.sample_failed || !restarted;

    if let Some(reporter) = reporter {
        reporter.abort();
    }
    if let Some((metrics, _)) = &metrics {
        println!(
            "{}",
            status_line(
                metrics,
                &demo.nodes,
                args.tuning.spill_dir.as_deref(),
                started
            )
        );
        println!("metrics at end:\n{}", metrics.dump());
    }

    if args.report_json.is_some() || args.gate.is_some() {
        let body = metrics
            .as_ref()
            .map_or_else(String::new, |(metrics, _)| metrics.render());
        let report = build_report(
            args,
            duration,
            &preload_report,
            preload_rss_kb,
            steady_rss_kb,
            peak_rss_kb.load(Ordering::Relaxed),
            demo.state.surviving_keys(),
            &convergence_report,
            &fetch_summary,
            &body,
        );
        if let Some(path) = &args.report_json {
            report.write_to(path)?;
            println!("report: wrote {}", path.display());
        }
        if let Some(gate_path) = &args.gate {
            let gate = report::read_gate(gate_path)?;
            let violations = report::check(&report, &gate);
            if violations.is_empty() {
                println!("gate: every threshold satisfied");
            } else {
                println!("gate: {} threshold(s) violated:", violations.len());
                for violation in &violations {
                    println!("  - {violation}");
                }
                failed = true;
            }
        }
    }

    demo.shutdown().await;
    Ok(i32::from(failed))
}

/// Builds the `--report-json` [`Report`] from a headless run's collected
/// state and its final metrics scrape.
#[allow(clippy::too_many_arguments, reason = "one report, one call site")]
fn build_report(
    args: &Args,
    duration: Duration,
    preload_report: &crate::preload::Report,
    preload_rss_kb: Option<u64>,
    steady_rss_kb: Option<u64>,
    peak_rss_kb: u64,
    surviving_keys: usize,
    convergence_report: &Convergence,
    fetch_summary: &FetchSummary,
    metrics_body: &str,
) -> Report {
    let steady_rss_bytes = steady_rss_kb.unwrap_or(0) * 1024;
    let copies_expected = report::copies_expected(args.owners.get(), surviving_keys);
    let totals = metrics::totals(metrics_body);
    let total_of = |metric: &str| totals.get(metric).copied().unwrap_or(0.0);
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "a sundog_* counter never reports a fractional or negative sample"
    )]
    let as_u64 = |value: f64| value.round() as u64;

    Report {
        nodes: args.nodes,
        keys: args.keys,
        owners: args.owners.get(),
        value_bytes: args.value_bytes,
        max_entries: args.tuning.max_entries,
        spill: args.tuning.spill_dir.is_some(),
        duration_secs: duration.as_secs_f64(),
        preload_keys_per_sec: preload_report.keys_per_sec(),
        preload_rss_bytes: preload_rss_kb.unwrap_or(0) * 1024,
        steady_rss_bytes,
        peak_rss_bytes: peak_rss_kb * 1024,
        copies_expected,
        bytes_per_copy: report::bytes_per_copy(steady_rss_bytes, copies_expected),
        fetch_p50_us: fetch_summary.p50_us,
        fetch_p99_us: fetch_summary.p99_us,
        fetch_misses: fetch_summary.misses,
        fetch_errors: fetch_summary.errors,
        sample_checked: fetch_summary.sample_checked,
        sample_ok: fetch_summary.sample_ok,
        converged: matches!(convergence_report, Convergence::Converged { .. }),
        spill_dropped_deferred: as_u64(metrics::labeled_total(
            metrics_body,
            "sundog_spill_dropped_total",
            "reason",
            "deferred",
        )),
        pull_timeouts: as_u64(total_of("sundog_rebalance_pull_timeouts_total")),
        spill_writes: as_u64(total_of("sundog_spill_writes_total")),
        ae_repaired: as_u64(total_of("sundog_ae_repaired_total")),
        rebalance_in: as_u64(metrics::labeled_total(
            metrics_body,
            "sundog_rebalance_buckets_total",
            "direction",
            "in",
        )),
        rebalance_out: as_u64(metrics::labeled_total(
            metrics_body,
            "sundog_rebalance_buckets_total",
            "direction",
            "out",
        )),
        backlog_dropped: as_u64(total_of("sundog_backlog_dropped_total")),
        fan_out_wait_timeouts: as_u64(total_of("sundog_fan_out_wait_timeouts_total")),
        spill_reopen_warm: as_u64(metrics::labeled_total(
            metrics_body,
            "sundog_spill_reopen_total",
            "outcome",
            "warm",
        )),
        spill_reopen_cold_fallback: as_u64(metrics::labeled_total(
            metrics_body,
            "sundog_spill_reopen_total",
            "outcome",
            "cold_fallback",
        )),
    }
}

/// Reads the current RSS, folding it into `peak_kb` via
/// [`report::track_peak`] with a compare-and-swap loop rather than a plain
/// load-then-store, since the periodic reporter task and the main run
/// loop both call this concurrently.
fn record_rss_sample(peak_kb: &AtomicU64) -> Option<u64> {
    let sample = rss::read_rss_kb()?;
    let mut current = peak_kb.load(Ordering::Relaxed);
    loop {
        let updated = report::track_peak(current, sample);
        if updated == current {
            return Some(sample);
        }
        match peak_kb.compare_exchange_weak(current, updated, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => return Some(sample),
            Err(actual) => current = actual,
        }
    }
}

/// The fetch counters, latency percentiles, and sample-check outcome
/// [`report_fetches_and_sample`] gathers and prints, kept together so
/// `--report-json` can reuse exactly what the console already reported.
struct FetchSummary {
    sample_checked: usize,
    sample_ok: usize,
    p50_us: u64,
    p99_us: u64,
    misses: u64,
    errors: u64,
    sample_failed: bool,
}

/// Prints the fetch counters, the sample check and the convergence report,
/// with the per-node explanation on divergence. Returns the gathered
/// counts, including whether the sample check failed.
async fn report_fetches_and_sample(
    demo: &setup::Demo,
    convergence_report: &Convergence,
) -> FetchSummary {
    let (sample_checked, sample_ok) = verify_sample(demo, SAMPLE_SIZE).await;
    let (p50_us, p99_us) = demo.state.latency_percentiles();
    let hits = demo.state.fetch_hits.load(Ordering::Relaxed);
    let misses = demo.state.fetch_misses.load(Ordering::Relaxed);
    let errors = demo.state.fetch_errors.load(Ordering::Relaxed);

    println!("fetch: {hits} hits, {misses} misses, {errors} errors, p50={p50_us}us p99={p99_us}us");
    println!(
        "sample check: {sample_ok}/{sample_checked} surviving keys fetched with the expected value"
    );
    println!("convergence: {convergence_report}");
    if convergence_report.is_diverged() {
        explain_divergence(demo);
    }
    let sample_failed = sample_ok != sample_checked || sample_checked == 0;
    FetchSummary {
        sample_checked,
        sample_ok,
        p50_us,
        p99_us,
        misses,
        errors,
        sample_failed,
    }
}

/// Prints the run's shape: cluster, key space, value size, RAM cap and
/// spill tier.
fn print_banner(args: &Args, duration: Duration) {
    println!(
        "sundog-distributed-demo headless: {} nodes, {} keys, {} owners, cluster {:?}, running for {}s",
        args.nodes,
        args.keys,
        args.owners.get(),
        args.cluster_name,
        duration.as_secs()
    );
    println!(
        "sizing: {} value bytes, RAM cap {} entries per node, spill {}",
        args.value_bytes,
        args.tuning
            .max_entries
            .map_or_else(|| "unbounded".to_owned(), |n| n.to_string()),
        args.tuning.spill_dir.as_ref().map_or_else(
            || "off".to_owned(),
            |dir| format!(
                "under {} with {} per node, regions of {}, flush queue {}",
                dir.display(),
                metrics::format_bytes(args.tuning.spill_capacity_bytes),
                args.tuning
                    .spill_region_bytes
                    .map_or_else(|| "64.0 MiB (default)".to_owned(), metrics::format_bytes),
                args.tuning
                    .spill_flush_queue_bytes
                    .map_or_else(|| "one region (default)".to_owned(), metrics::format_bytes),
            )
        )
    );
}

/// Prints [`status_line`] every `interval` until aborted, folding each
/// tick's RSS reading into `peak_rss_kb`.
async fn report_periodically(
    metrics: Arc<Metrics>,
    interval: Duration,
    nodes: Arc<Vec<Arc<NodeSlot>>>,
    spill_dir: Option<PathBuf>,
    started: Instant,
    peak_rss_kb: Arc<AtomicU64>,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    loop {
        ticker.tick().await;
        record_rss_sample(&peak_rss_kb);
        println!(
            "{}",
            status_line(&metrics, &nodes, spill_dir.as_deref(), started)
        );
    }
}

/// One line of run state: elapsed time, RSS, each node's own entry and
/// bucket counts, the size of each node's spill directory on disk, and the
/// watched metric totals.
fn status_line(
    metrics: &Metrics,
    nodes: &[Arc<NodeSlot>],
    spill_dir: Option<&Path>,
    started: Instant,
) -> String {
    let per_node: Vec<String> = nodes
        .iter()
        .map(|node| {
            let disk = spill_dir.map_or_else(String::new, |root| {
                format!(
                    " disk={}",
                    metrics::format_bytes(metrics::dir_bytes(
                        &root.join(format!("node{}", node.index))
                    ))
                )
            });
            let state = if node.status.alive.load(Ordering::Relaxed) {
                if node.status.warm.load(Ordering::Relaxed) {
                    "warm"
                } else {
                    "cold"
                }
            } else {
                "down"
            };
            format!(
                "node{} {state} entries={} buckets={}{disk}",
                node.index,
                node.status.entry_count.load(Ordering::Relaxed),
                node.status.owned_buckets.load(Ordering::Relaxed),
            )
        })
        .collect();
    format!(
        "t={}s rss={} | {} | {}",
        started.elapsed().as_secs(),
        rss::format_rss(rss::read_rss_kb()),
        per_node.join("; "),
        metrics::summary_line(&metrics.totals())
    )
}

/// Fetches `sample_size` random surviving keys from a random live node and
/// checks each against the load's recorded expected value. Returns
/// `(checked, matched)`.
async fn verify_sample(demo: &setup::Demo, sample_size: usize) -> (usize, usize) {
    let mut checked = 0usize;
    let mut matched = 0usize;
    let mut attempts = 0usize;
    while checked < sample_size && attempts < SAMPLE_ATTEMPT_CAP {
        attempts += 1;
        let index = random_range(0..demo.keys);
        if demo.state.is_removed(index) {
            continue;
        }
        let live: Vec<_> = demo.nodes.iter().filter(|n| n.is_alive()).collect();
        let Some(node) = live.get(random_range(0..live.len().max(1))) else {
            continue;
        };
        let Some(cache) = node.cache() else { continue };
        checked += 1;
        let expected = demo.state.expected_value(index);
        match cache.fetch(&key_for(index)).await {
            Ok(Some(value)) if value == expected => matched += 1,
            _ => {}
        }
    }
    (checked, matched)
}

/// Prints what a diverged sum is made of: per live node, keys held in
/// buckets the node does not own; how many nodes hold each key; and how
/// many removed keys some node still holds.
fn explain_divergence(demo: &setup::Demo) {
    let mut copies: HashMap<String, u8> = HashMap::new();
    for (index, node) in demo.nodes.iter().enumerate() {
        if !node.is_alive() {
            continue;
        }
        let Some(cache) = node.cache() else { continue };
        let node_id = sundog::NodeId::from(node.status.node_id.load(Ordering::Relaxed));
        let keys = cache.keys();
        let foreign = keys
            .iter()
            .inspect(|key| *copies.entry((*key).clone()).or_insert(0) += 1)
            .filter(|key| !cache.owners_of(key).contains(&node_id))
            .count();
        println!(
            "divergence: node{index} holds {} keys, {foreign} in buckets it does not own",
            keys.len()
        );
    }
    let mut by_copies = [0usize; 4];
    for &n in copies.values() {
        by_copies[usize::from(n).min(3)] += 1;
    }
    println!(
        "divergence: keys held once={} twice={} three or more={}",
        by_copies[1], by_copies[2], by_copies[3]
    );
    let removed_but_held = copies
        .keys()
        .filter_map(|key| key.strip_prefix('k')?.parse::<usize>().ok())
        .filter(|&index| demo.state.is_removed(index))
        .count();
    println!("divergence: removed keys some node still holds: {removed_but_held}");
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU8;

    use super::*;

    const METRICS_BODY: &str = "sundog_rebalance_pull_timeouts_total 7\n\
        sundog_spill_writes_total{cache=\"demo\"} 12\n\
        sundog_spill_writes_total{cache=\"other\"} 3\n\
        sundog_ae_repaired_total 4\n\
        sundog_spill_dropped_total{reason=\"deferred\"} 9\n\
        sundog_spill_dropped_total{reason=\"other\"} 100\n\
        sundog_rebalance_buckets_total{direction=\"in\"} 6\n\
        sundog_rebalance_buckets_total{direction=\"out\"} 2\n\
        sundog_backlog_dropped_total{peer=\"1\"} 5\n\
        sundog_backlog_dropped_total{peer=\"2\"} 3\n\
        sundog_fan_out_wait_timeouts_total{cache=\"demo\"} 8\n\
        sundog_fan_out_wait_timeouts_total{cache=\"other\"} 1\n\
        sundog_spill_reopen_total{cache=\"demo\",outcome=\"warm\",reason=\"\"} 1\n\
        sundog_spill_reopen_total{cache=\"demo\",outcome=\"cold_fallback\",reason=\"downtime_exceeded\"} 2\n";

    #[test]
    fn restart_delay_is_a_quarter_of_the_duration_when_that_stays_under_half_the_tombstone_ttl() {
        assert_eq!(
            restart_delay(Duration::from_secs(60), Duration::from_secs(60)),
            Duration::from_secs(15)
        );
    }

    #[test]
    fn restart_delay_caps_at_half_the_tombstone_ttl_on_a_long_run() {
        assert_eq!(
            restart_delay(Duration::from_secs(300), Duration::from_secs(60)),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn build_report_maps_rss_units_and_metric_names_onto_report_fields() {
        let args = Args {
            nodes: 3,
            keys: 4_000_000,
            owners: NonZeroU8::new(2).expect("2 is nonzero"),
            value_bytes: 256,
            tuning: crate::cli::CacheTuning {
                max_entries: Some(800_000),
                spill_dir: Some(PathBuf::from("/tmp/sundog-spill")),
                ..crate::cli::CacheTuning::default()
            },
            ..Args::default()
        };
        let preload_report = crate::preload::Report {
            keys: 100,
            elapsed: Duration::from_secs(1),
        };
        let convergence_report = Convergence::Converged { total: 20, live: 3 };
        let fetch_summary = FetchSummary {
            sample_checked: 2_000,
            sample_ok: 1_999,
            p50_us: 138,
            p99_us: 43_800,
            misses: 5,
            errors: 1,
            sample_failed: true,
        };

        let report = build_report(
            &args,
            Duration::from_secs(300),
            &preload_report,
            Some(1_000),
            Some(2_000),
            3_000,
            10,
            &convergence_report,
            &fetch_summary,
            METRICS_BODY,
        );

        assert_eq!(report.nodes, 3);
        assert_eq!(report.keys, 4_000_000);
        assert_eq!(report.owners, 2);
        assert_eq!(report.value_bytes, 256);
        assert_eq!(report.max_entries, Some(800_000));
        assert!(report.spill);
        assert!((report.duration_secs - 300.0).abs() < f64::EPSILON);
        assert!((report.preload_keys_per_sec - 100.0).abs() < f64::EPSILON);
        assert_eq!(report.preload_rss_bytes, 1_000 * 1024);
        assert_eq!(report.steady_rss_bytes, 2_000 * 1024);
        assert_eq!(report.peak_rss_bytes, 3_000 * 1024);
        assert_eq!(report.copies_expected, 20);
        assert!((report.bytes_per_copy - (2_000.0 * 1024.0 / 20.0)).abs() < f64::EPSILON);
        assert_eq!(report.fetch_p50_us, 138);
        assert_eq!(report.fetch_p99_us, 43_800);
        assert_eq!(report.fetch_misses, 5);
        assert_eq!(report.fetch_errors, 1);
        assert_eq!(report.sample_checked, 2_000);
        assert_eq!(report.sample_ok, 1_999);
        assert!(report.converged);
        assert_eq!(report.spill_dropped_deferred, 9);
        assert_eq!(report.pull_timeouts, 7);
        assert_eq!(report.spill_writes, 15);
        assert_eq!(report.ae_repaired, 4);
        assert_eq!(report.rebalance_in, 6);
        assert_eq!(report.rebalance_out, 2);
        assert_eq!(report.backlog_dropped, 8);
        assert_eq!(report.fan_out_wait_timeouts, 9);
        assert_eq!(report.spill_reopen_warm, 1);
        assert_eq!(report.spill_reopen_cold_fallback, 2);
    }

    #[test]
    fn build_report_defaults_missing_rss_samples_to_zero_and_reports_not_converged() {
        let args = Args::default();
        let preload_report = crate::preload::Report {
            keys: 0,
            elapsed: Duration::from_secs(0),
        };
        let fetch_summary = FetchSummary {
            sample_checked: 0,
            sample_ok: 0,
            p50_us: 0,
            p99_us: 0,
            misses: 0,
            errors: 0,
            sample_failed: true,
        };

        let report = build_report(
            &args,
            Duration::from_secs(1),
            &preload_report,
            None,
            None,
            0,
            0,
            &Convergence::NoLiveNodes,
            &fetch_summary,
            "",
        );

        assert_eq!(report.preload_rss_bytes, 0);
        assert_eq!(report.steady_rss_bytes, 0);
        assert_eq!(report.peak_rss_bytes, 0);
        assert_eq!(report.copies_expected, 0);
        assert!(!report.converged);
        assert_eq!(report.pull_timeouts, 0);
        assert_eq!(report.spill_dropped_deferred, 0);
        assert_eq!(report.backlog_dropped, 0);
        assert_eq!(report.fan_out_wait_timeouts, 0);
        assert_eq!(report.spill_reopen_warm, 0);
        assert_eq!(report.spill_reopen_cold_fallback, 0);
    }
}

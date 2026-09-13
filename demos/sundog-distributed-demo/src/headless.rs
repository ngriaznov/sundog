//! `--headless <SECS>`: preload, run the write load and fetch sampler for a
//! fixed duration, killing and restarting one node partway through to
//! exercise rebalance, pause it, poll a bounded convergence check, verify a
//! random sample of surviving keys, and print a one-line report. Returns a
//! nonzero status on divergence or a failed sample: the CI-friendly smoke
//! test.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use rand::random_range;

use crate::cli::Args;
use crate::convergence;
use crate::metrics::{self, Metrics};
use crate::node::{DISOWN_GRACE_ROUNDS, NodeSlot};
use crate::preload::key_for;
use crate::rss;
use crate::setup;

/// How many surviving keys the post-convergence sample check fetches and
/// verifies against the load's expected value.
const SAMPLE_SIZE: usize = 2_000;
/// Bounds the retry loop that skips already-removed keys while sampling.
const SAMPLE_ATTEMPT_CAP: usize = SAMPLE_SIZE * 20;

/// Runs the headless smoke check, returning `0` if the live nodes converged
/// and the sample check passed, `1` otherwise.
///
/// # Errors
///
/// Returns an error if the cluster fails to bootstrap.
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
    let reporter = metrics.as_ref().map(|(metrics, interval)| {
        tokio::spawn(report_periodically(
            Arc::clone(metrics),
            *interval,
            Arc::clone(&demo.nodes),
            args.tuning.spill_dir.clone(),
            started,
        ))
    });

    let report = demo.wait_for_preload().await;
    let rss_kb = rss::read_rss_kb();
    println!(
        "preload: {} keys in {:.1}s ({:.0} keys/s), RSS {}",
        report.keys,
        report.elapsed.as_secs_f64(),
        report.keys_per_sec(),
        rss::format_rss(rss_kb)
    );
    if let Some((metrics, _)) = &metrics {
        println!("metrics after preload:\n{}", metrics.dump());
    }

    // Kill one node at the midpoint and restart it three-quarters through,
    // so the run exercises a real rebalance under live load.
    let killed_index = 0usize;
    let half = duration / 2;
    let three_quarter = duration.mul_f64(0.75);
    tokio::time::sleep(half).await;
    demo.nodes[killed_index].kill(&demo.feed_tx).await;
    println!(
        "headless: killed node{killed_index} at {}s to exercise rebalance",
        half.as_secs()
    );

    tokio::time::sleep(three_quarter.saturating_sub(half)).await;
    let restarted = demo.nodes[killed_index]
        .restart(&demo.topology, &demo.feed_tx)
        .await;
    if restarted {
        println!(
            "headless: restarted node{killed_index} at {}s",
            three_quarter.as_secs()
        );
    } else {
        println!(
            "headless: restart of node{killed_index} at {}s FAILED",
            three_quarter.as_secs()
        );
    }

    tokio::time::sleep(duration.saturating_sub(three_quarter)).await;
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

    let sample_failed = report_fetches_and_sample(&demo, &convergence_report).await;
    let exit_code = i32::from(convergence_report.is_diverged() || sample_failed || !restarted);

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
    demo.shutdown().await;
    Ok(exit_code)
}

/// Prints the fetch counters, the sample check and the convergence report,
/// with the per-node explanation on divergence. Returns whether the sample
/// check failed.
async fn report_fetches_and_sample(
    demo: &setup::Demo,
    convergence_report: &convergence::Convergence,
) -> bool {
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
    sample_ok != sample_checked || sample_checked == 0
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

/// Prints [`status_line`] every `interval` until aborted.
async fn report_periodically(
    metrics: Arc<Metrics>,
    interval: Duration,
    nodes: Arc<Vec<Arc<NodeSlot>>>,
    spill_dir: Option<PathBuf>,
    started: Instant,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    loop {
        ticker.tick().await;
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

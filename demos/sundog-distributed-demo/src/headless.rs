//! `--headless <SECS>`: preload, run the write load and fetch sampler for a
//! fixed duration — killing and restarting one node partway through to
//! exercise rebalance — pause it, poll a bounded convergence check, verify a
//! random sample of surviving keys, and print a one-line report. Returns a
//! nonzero status on divergence or a failed sample — the CI-friendly smoke
//! test.

use std::sync::atomic::Ordering;
use std::time::Duration;

use rand::random_range;

use crate::cli::Args;
use crate::convergence;
use crate::node::DISOWN_GRACE_ROUNDS;
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
    let mut demo = setup::bootstrap(args).await?;
    // The event feed is unbounded; drain it since only the TUI reads it.
    let (drained_tx, drained_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut feed = std::mem::replace(&mut demo.feed_rx, drained_rx);
    drop(drained_tx);
    tokio::spawn(async move { while feed.recv().await.is_some() {} });
    let demo = demo;

    println!(
        "sundog-distributed-demo headless: {} nodes, {} keys, {} owners, cluster {:?}, running for {}s",
        args.nodes,
        args.keys,
        args.owners.get(),
        args.cluster_name,
        duration.as_secs()
    );

    let report = demo.wait_for_preload().await;
    let rss_kb = rss::read_rss_kb();
    println!(
        "preload: {} keys in {:.1}s ({:.0} keys/s), RSS {}",
        report.keys,
        report.elapsed.as_secs_f64(),
        report.keys_per_sec(),
        rss::format_rss(rss_kb)
    );

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
        .restart(
            &demo.cluster_name,
            &demo.seeds,
            setup::AE_INTERVAL,
            setup::TOMBSTONE_TTL,
            args.owners,
            &demo.feed_tx,
        )
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
    // Grace for whatever write/fetch tick was in flight to land.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let deadline = convergence::poll_deadline(setup::AE_INTERVAL, DISOWN_GRACE_ROUNDS);
    let convergence_report = convergence::poll(
        &demo.nodes,
        u64::from(demo.owners.get()),
        || demo.state.surviving_keys(),
        deadline,
    )
    .await;

    let (sample_checked, sample_ok) = verify_sample(&demo, SAMPLE_SIZE).await;
    let (p50_us, p99_us) = demo.state.latency_percentiles();
    let hits = demo.state.fetch_hits.load(Ordering::Relaxed);
    let misses = demo.state.fetch_misses.load(Ordering::Relaxed);
    let errors = demo.state.fetch_errors.load(Ordering::Relaxed);

    println!("fetch: {hits} hits, {misses} misses, {errors} errors, p50={p50_us}us p99={p99_us}us");
    println!(
        "sample check: {sample_ok}/{sample_checked} surviving keys fetched with the expected value"
    );
    println!("convergence: {convergence_report}");

    let diverged = convergence_report.is_diverged();
    let sample_failed = sample_ok != sample_checked || sample_checked == 0;
    let exit_code = i32::from(diverged || sample_failed || !restarted);

    demo.shutdown().await;
    Ok(exit_code)
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

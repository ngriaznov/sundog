//! `--headless <SECS>`: run the write load without a TUI for a fixed
//! duration, then print a one-line convergence report and return a nonzero
//! status on divergence — the CI-friendly smoke test.

use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::cli::Args;
use crate::convergence;
use crate::setup;

/// Runs the headless smoke check. Returns the process exit code: `0` if the
/// live nodes converged on the same entry count, `1` if they diverged.
///
/// # Errors
///
/// Returns an error if the cluster fails to bootstrap.
pub(crate) async fn run(args: &Args, duration: Duration) -> anyhow::Result<i32> {
    let demo = setup::bootstrap(args).await?;
    println!(
        "sundog-demo headless: {} nodes, cluster {:?}, running for {}s",
        args.nodes,
        args.cluster_name,
        duration.as_secs()
    );

    tokio::time::sleep(duration).await;
    demo.paused.store(true, Ordering::Relaxed);

    // Convergence is eventual: random peer selection needs several rounds to
    // pair every lagging node, so poll under a bound instead of judging one
    // arbitrary instant.
    let deadline = tokio::time::Instant::now() + setup::AE_INTERVAL * 10;
    let report = loop {
        let report = convergence::check(&demo.nodes);
        if !report.is_diverged() || tokio::time::Instant::now() >= deadline {
            break report;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    let exit_code = i32::from(report.is_diverged());
    let total_writes: u64 = demo
        .nodes
        .iter()
        .map(|n| n.status.writes_applied.load(Ordering::Relaxed))
        .sum();
    println!("convergence: {report} (total applied ops across nodes: {total_writes})");

    demo.shutdown().await;
    Ok(exit_code)
}

//! `sundog-distributed-demo`: a distribution-mode TUI for a `sundog`
//! cluster. Spawns N in-process nodes over static loopback seeds, preloads
//! a large key set, and drives a background write load and fetch sampler
//! against it.

mod cli;
mod load;
mod node;
mod preload;
mod rss;
mod setup;

use std::sync::atomic::Ordering;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = cli::parse(std::env::args().skip(1))?;

    let demo = setup::bootstrap(&args).await?;
    println!(
        "sundog-distributed-demo: {} nodes formed cluster {:?} ({} owners per bucket)",
        demo.nodes.len(),
        demo.cluster_name,
        demo.owners.get()
    );

    let report = demo.wait_for_preload().await;
    println!(
        "preload: {} keys in {:.1}s ({:.0} keys/s), RSS {}",
        report.keys,
        report.elapsed.as_secs_f64(),
        report.keys_per_sec(),
        rss::format_rss(rss::read_rss_kb())
    );

    // No TUI or --headless dispatch yet: run the load for a few seconds so
    // there is something to report.
    tokio::time::sleep(Duration::from_secs(5)).await;
    demo.paused.store(true, Ordering::Relaxed);

    let (p50_us, p99_us) = demo.state.latency_percentiles();
    println!(
        "fetch: {} hits, {} misses, {} errors, p50={p50_us}us p99={p99_us}us",
        demo.state.fetch_hits.load(Ordering::Relaxed),
        demo.state.fetch_misses.load(Ordering::Relaxed),
        demo.state.fetch_errors.load(Ordering::Relaxed),
    );

    demo.shutdown().await;
    Ok(())
}

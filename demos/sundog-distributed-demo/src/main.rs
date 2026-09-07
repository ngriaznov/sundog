//! `sundog-distributed-demo`: a distribution-mode TUI for a `sundog`
//! cluster. Spawns N in-process nodes over static loopback seeds, opening a
//! `Mode::Distributed` cache on each.

mod cli;
mod node;
mod preload;
mod rss;
mod setup;

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

    let progress = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let report = preload::run(&demo.nodes, demo.keys, &progress).await?;
    println!(
        "preload: {} keys in {:.1}s ({:.0} keys/s), RSS {}",
        report.keys,
        report.elapsed.as_secs_f64(),
        report.keys_per_sec(),
        rss::format_rss(rss::read_rss_kb())
    );

    demo.shutdown().await;
    Ok(())
}

//! `sundog-distributed-demo`: a distribution-mode TUI for a `sundog`
//! cluster. Spawns N in-process nodes over static loopback seeds, opening a
//! `Mode::Distributed` cache on each.

mod cli;
mod node;
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
    demo.shutdown().await;
    Ok(())
}

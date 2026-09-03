//! Shared bootstrap for both the interactive TUI and the `--headless` smoke
//! run: build N node slots on fixed loopback gossip ports, start them all,
//! and kick off the background write-load generator.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;

use crate::cli::Args;
use crate::load;
use crate::node::{self, NodeSlot};

/// Anti-entropy tuned faster than the library default (30s) so the chaos
/// demo's convergence indicator visibly settles within a handful of
/// seconds instead of half a minute.
pub(crate) const AE_INTERVAL: Duration = Duration::from_secs(3);
/// `>= 3 * AE_INTERVAL`, satisfying the tombstone-GC safety rule.
pub(crate) const TOMBSTONE_TTL: Duration = Duration::from_secs(15);

/// Everything one run of the demo needs: the node slots, the merged event
/// feed, and the write-load's pause switch — shared verbatim by the TUI and
/// headless entry points.
pub(crate) struct Demo {
    pub(crate) nodes: Arc<Vec<Arc<NodeSlot>>>,
    pub(crate) feed_rx: UnboundedReceiver<String>,
    pub(crate) feed_tx: UnboundedSender<String>,
    pub(crate) paused: Arc<AtomicBool>,
    pub(crate) cluster_name: String,
    pub(crate) seeds: Vec<SocketAddr>,
    load_handle: JoinHandle<()>,
}

impl Demo {
    /// Stops the write load and gracefully shuts down every still-alive
    /// node.
    pub(crate) async fn shutdown(self) {
        self.load_handle.abort();
        for node in self.nodes.iter() {
            node.kill(&self.feed_tx).await;
        }
    }
}

/// Builds `args.nodes` node slots, starts every one of them, and spawns the
/// background write-load routine.
///
/// # Errors
///
/// Returns an error if any node fails to form its cluster or open the demo
/// cache (most commonly a fixed gossip port already in use).
pub(crate) async fn bootstrap(args: &Args) -> anyhow::Result<Demo> {
    let base_port = args
        .gossip_base_port
        .unwrap_or_else(|| rand::random_range(20_000..60_000));
    let nodes = Arc::new(node::build_slots(args.nodes, base_port));
    let seeds = node::seed_list(&nodes);
    let (feed_tx, feed_rx) = mpsc::unbounded_channel();

    for slot in nodes.iter() {
        slot.start(
            &args.cluster_name,
            &seeds,
            AE_INTERVAL,
            TOMBSTONE_TTL,
            &feed_tx,
        )
        .await?;
    }

    let paused = Arc::new(AtomicBool::new(false));
    let load_handle = tokio::spawn(load::run(
        Arc::clone(&nodes),
        args.key_space,
        args.write_interval,
        Arc::clone(&paused),
    ));

    Ok(Demo {
        nodes,
        feed_rx,
        feed_tx,
        paused,
        cluster_name: args.cluster_name.clone(),
        seeds,
        load_handle,
    })
}

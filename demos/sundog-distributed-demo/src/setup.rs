//! Shared bootstrap for the interactive TUI and the `--headless` run: builds
//! node slots and starts every one with a `Mode::Distributed` cache.

use std::net::SocketAddr;
use std::num::NonZeroU8;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use crate::cli::Args;
use crate::node::{self, NodeSlot};

/// Faster than the library default so convergence and rebalancing visibly
/// settle quickly.
pub(crate) const AE_INTERVAL: Duration = Duration::from_secs(3);
/// `>= 3 * AE_INTERVAL`, satisfying the tombstone-GC safety rule.
pub(crate) const TOMBSTONE_TTL: Duration = Duration::from_secs(15);

/// Everything one run of the demo needs: node slots and the merged event
/// feed, shared by the TUI and headless paths.
pub(crate) struct Demo {
    pub(crate) nodes: Arc<Vec<Arc<NodeSlot>>>,
    pub(crate) feed_rx: UnboundedReceiver<String>,
    pub(crate) feed_tx: UnboundedSender<String>,
    pub(crate) paused: Arc<AtomicBool>,
    pub(crate) cluster_name: String,
    pub(crate) seeds: Vec<SocketAddr>,
    pub(crate) owners: NonZeroU8,
    pub(crate) keys: usize,
}

impl Demo {
    /// Shuts down every still-alive node.
    pub(crate) async fn shutdown(self) {
        for node in self.nodes.iter() {
            node.kill(&self.feed_tx).await;
        }
    }
}

/// Builds `args.nodes` node slots and starts every one with a
/// `Mode::Distributed` cache.
///
/// # Errors
///
/// Returns an error if any node fails to form its cluster or open the cache.
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
            args.owners,
            &feed_tx,
        )
        .await?;
    }

    Ok(Demo {
        nodes,
        feed_rx,
        feed_tx,
        paused: Arc::new(AtomicBool::new(false)),
        cluster_name: args.cluster_name.clone(),
        seeds,
        owners: args.owners,
        keys: args.keys,
    })
}

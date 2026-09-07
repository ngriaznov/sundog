//! Shared bootstrap for the interactive TUI and the `--headless` run: builds
//! node slots, starts them, kicks off the preload, and starts the
//! background write-load/fetch generator once the preload finishes.

use std::net::SocketAddr;
use std::num::NonZeroU8;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;

use crate::cli::Args;
use crate::load::{self, LoadState};
use crate::node::{self, NodeSlot};
use crate::preload;

/// Faster than the library default so convergence and rebalancing visibly
/// settle quickly.
pub(crate) const AE_INTERVAL: Duration = Duration::from_secs(3);
/// `>= 3 * AE_INTERVAL`, satisfying the tombstone-GC safety rule.
pub(crate) const TOMBSTONE_TTL: Duration = Duration::from_secs(15);

/// Everything one run of the demo needs: node slots, the merged event feed,
/// the load's pause switch, and preload progress, shared by the TUI and
/// headless paths.
pub(crate) struct Demo {
    pub(crate) nodes: Arc<Vec<Arc<NodeSlot>>>,
    pub(crate) feed_rx: UnboundedReceiver<String>,
    pub(crate) feed_tx: UnboundedSender<String>,
    pub(crate) paused: Arc<AtomicBool>,
    pub(crate) cluster_name: String,
    pub(crate) seeds: Vec<SocketAddr>,
    pub(crate) owners: NonZeroU8,
    pub(crate) keys: usize,
    pub(crate) state: Arc<LoadState>,
    pub(crate) preload_progress: Arc<AtomicU64>,
    pub(crate) preload_done: Arc<AtomicBool>,
    preload_report: Arc<StdMutex<Option<preload::Report>>>,
    load_handle: JoinHandle<()>,
    preload_handle: JoinHandle<()>,
}

impl Demo {
    /// Stops the load and preload tasks and shuts down every still-alive
    /// node.
    pub(crate) async fn shutdown(self) {
        self.load_handle.abort();
        self.preload_handle.abort();
        for node in self.nodes.iter() {
            node.kill(&self.feed_tx).await;
        }
    }

    /// Polls until the preload finishes, returning its report.
    pub(crate) async fn wait_for_preload(&self) -> preload::Report {
        loop {
            if let Some(report) = *self
                .preload_report
                .lock()
                .expect("invariant: preload report lock is never poisoned")
            {
                return report;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// The preload's progress so far, out of `self.keys`; `None` once done
    /// (nothing left to show a bar for).
    #[must_use]
    pub(crate) fn preload_progress(&self) -> Option<(u64, usize)> {
        if self.preload_done.load(Ordering::Relaxed) {
            return None;
        }
        Some((self.preload_progress.load(Ordering::Relaxed), self.keys))
    }
}

/// Builds `args.nodes` node slots, starts every one with a
/// `Mode::Distributed` cache, spawns the preload, and spawns the background
/// write-load/fetch generator (which itself waits for the preload to
/// finish before touching anything).
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

    let paused = Arc::new(AtomicBool::new(false));
    let state = Arc::new(LoadState::new(args.keys));
    let preload_progress = Arc::new(AtomicU64::new(0));
    let preload_done = Arc::new(AtomicBool::new(false));
    let preload_report: Arc<StdMutex<Option<preload::Report>>> = Arc::new(StdMutex::new(None));

    let preload_handle = tokio::spawn({
        let nodes = Arc::clone(&nodes);
        let keys = args.keys;
        let progress = Arc::clone(&preload_progress);
        let done = Arc::clone(&preload_done);
        let report_slot = Arc::clone(&preload_report);
        let feed_tx = feed_tx.clone();
        async move {
            match preload::run(&nodes, keys, &progress).await {
                Ok(report) => {
                    let _ = feed_tx.send(format!(
                        "preload: {} keys in {:.1}s ({:.0} keys/s)",
                        report.keys,
                        report.elapsed.as_secs_f64(),
                        report.keys_per_sec()
                    ));
                    *report_slot
                        .lock()
                        .expect("invariant: preload report lock is never poisoned") = Some(report);
                }
                Err(error) => {
                    let _ = feed_tx.send(format!("preload failed: {error:#}"));
                }
            }
            done.store(true, Ordering::Relaxed);
        }
    });

    let load_handle = tokio::spawn(load::run(
        Arc::clone(&nodes),
        Arc::clone(&state),
        args.write_interval,
        Arc::clone(&paused),
        Arc::clone(&preload_done),
    ));

    Ok(Demo {
        nodes,
        feed_rx,
        feed_tx,
        paused,
        cluster_name: args.cluster_name.clone(),
        seeds,
        owners: args.owners,
        keys: args.keys,
        state,
        preload_progress,
        preload_done,
        preload_report,
        load_handle,
        preload_handle,
    })
}

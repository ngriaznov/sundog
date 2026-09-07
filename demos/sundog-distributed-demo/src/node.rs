//! In-process node lifecycle: builds, kills, and restarts a `sundog`
//! `Cluster` on one fixed loopback gossip port, opening a
//! [`sundog::Mode::Distributed`] cache and tracking [`NodeStatus`].

use std::net::{Ipv4Addr, SocketAddr};
use std::num::NonZeroU8;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, RwLock as StdRwLock};
use std::time::Duration;

use anyhow::Context as _;
use sundog::{Cache, Cluster, ClusterConfig, Event, Mode, Origin};
use tokio::sync::broadcast;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;

use crate::sample::{self, BUCKET_COUNT, PROBE_COUNT};

pub(crate) const CACHE_NAME: &str = "demo";

/// A node keeps a disowned bucket around for two anti-entropy rounds before
/// releasing it — shorter than the library default of three, so a kill/
/// restart cycle visibly rebalances within one headless run.
pub(crate) const DISOWN_GRACE_ROUNDS: u32 = 2;

#[derive(Clone)]
struct Handle {
    cluster: Cluster,
    cache: Cache<String, String>,
}

/// Live counters for one node slot: refreshed on a timer from the store and
/// cluster health, since events alone can lag or coalesce.
#[derive(Debug, Default)]
pub(crate) struct NodeStatus {
    pub(crate) alive: AtomicBool,
    busy: AtomicBool,
    pub(crate) node_id: AtomicU64,
    pub(crate) entry_count: AtomicI64,
    pub(crate) owned_buckets: AtomicI64,
    pub(crate) warm: AtomicBool,
    pub(crate) writes_applied: AtomicU64,
    pub(crate) restarts: AtomicU32,
}

/// One demo node: a fixed loopback gossip address plus its running `Cluster`.
pub(crate) struct NodeSlot {
    pub(crate) index: usize,
    pub(crate) gossip_addr: SocketAddr,
    handle: StdRwLock<Option<Handle>>,
    listener: StdRwLock<Option<JoinHandle<()>>>,
    pub(crate) status: NodeStatus,
}

impl NodeSlot {
    fn new(index: usize, gossip_addr: SocketAddr) -> Self {
        Self {
            index,
            gossip_addr,
            handle: StdRwLock::new(None),
            listener: StdRwLock::new(None),
            status: NodeStatus::default(),
        }
    }

    #[must_use]
    pub(crate) fn is_alive(&self) -> bool {
        self.status.alive.load(Ordering::Relaxed)
    }

    /// The live peer count this node's membership currently reports.
    #[must_use]
    pub(crate) fn peer_count(&self) -> Option<usize> {
        self.read_handle().map(|h| h.cluster.peers().len())
    }

    /// A cheap clone of this node's cache handle, `None` while killed.
    #[must_use]
    pub(crate) fn cache(&self) -> Option<Cache<String, String>> {
        self.read_handle().map(|h| h.cache)
    }

    fn read_handle(&self) -> Option<Handle> {
        self.handle
            .read()
            .expect("invariant: node handle lock is never poisoned")
            .clone()
    }

    fn take(&self) -> (Option<Handle>, Option<JoinHandle<()>>) {
        let handle = self
            .handle
            .write()
            .expect("invariant: node handle lock is never poisoned")
            .take();
        let listener = self
            .listener
            .write()
            .expect("invariant: listener lock is never poisoned")
            .take();
        (handle, listener)
    }

    fn install(&self, handle: Handle, listener: JoinHandle<()>) {
        *self
            .handle
            .write()
            .expect("invariant: node handle lock is never poisoned") = Some(handle);
        *self
            .listener
            .write()
            .expect("invariant: listener lock is never poisoned") = Some(listener);
    }

    /// Starts this node for the first time: opens the cluster and the
    /// distributed cache, and installs the event-feed listener that keeps
    /// `status` current.
    ///
    /// # Errors
    ///
    /// Returns an error if the cluster fails to form or the cache to open.
    pub(crate) async fn start(
        self: &Arc<Self>,
        cluster_name: &str,
        seeds: &[SocketAddr],
        ae_interval: Duration,
        tombstone_ttl: Duration,
        owners: NonZeroU8,
        feed_tx: &UnboundedSender<String>,
    ) -> anyhow::Result<()> {
        let handle = open(
            cluster_name,
            self.gossip_addr,
            seeds,
            ae_interval,
            tombstone_ttl,
            owners,
        )
        .await?;
        self.status
            .node_id
            .store(handle.cluster.node_id().as_u64(), Ordering::Relaxed);
        self.status.entry_count.store(0, Ordering::Relaxed);
        self.status.owned_buckets.store(0, Ordering::Relaxed);
        self.status.warm.store(false, Ordering::Relaxed);
        let listener = spawn_listener(Arc::clone(self), handle.cache.events(), feed_tx.clone());
        self.install(handle, listener);
        self.status.alive.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// Shuts this node down gracefully, a no-op if already killed.
    /// Concurrent kill/restart calls serialize; a late one is dropped.
    pub(crate) async fn kill(self: &Arc<Self>, feed_tx: &UnboundedSender<String>) {
        if self.status.busy.swap(true, Ordering::AcqRel) {
            return;
        }
        self.teardown().await;
        let _ = feed_tx.send(format!("node{}: killed", self.index));
        self.status.busy.store(false, Ordering::Release);
    }

    /// Tears down whatever is running and opens a fresh `Cluster` on the
    /// same gossip port, rejoining via the same seeds like a real restart.
    /// Returns `true` if the reopen succeeded, `false` if it failed (a
    /// failure is still reported on `feed_tx`, but the caller needs the
    /// outcome too rather than only a narrated event).
    ///
    /// A concurrent kill/restart call that finds this node already busy
    /// returns `false` without touching anything — no cluster was reopened.
    pub(crate) async fn restart(
        self: &Arc<Self>,
        cluster_name: &str,
        seeds: &[SocketAddr],
        ae_interval: Duration,
        tombstone_ttl: Duration,
        owners: NonZeroU8,
        feed_tx: &UnboundedSender<String>,
    ) -> bool {
        if self.status.busy.swap(true, Ordering::AcqRel) {
            return false;
        }
        self.teardown().await;
        let _ = feed_tx.send(format!("node{}: restarting…", self.index));
        let succeeded = match open(
            cluster_name,
            self.gossip_addr,
            seeds,
            ae_interval,
            tombstone_ttl,
            owners,
        )
        .await
        {
            Ok(handle) => {
                self.status
                    .node_id
                    .store(handle.cluster.node_id().as_u64(), Ordering::Relaxed);
                self.status.entry_count.store(0, Ordering::Relaxed);
                self.status.owned_buckets.store(0, Ordering::Relaxed);
                self.status.warm.store(false, Ordering::Relaxed);
                let listener =
                    spawn_listener(Arc::clone(self), handle.cache.events(), feed_tx.clone());
                self.install(handle, listener);
                self.status.alive.store(true, Ordering::Relaxed);
                self.status.restarts.fetch_add(1, Ordering::Relaxed);
                let _ = feed_tx.send(format!(
                    "node{}: restarted, pulling its share of buckets",
                    self.index
                ));
                true
            }
            Err(error) => {
                let _ = feed_tx.send(format!("node{}: restart failed: {error:#}", self.index));
                false
            }
        };
        self.status.busy.store(false, Ordering::Release);
        succeeded
    }

    async fn teardown(&self) {
        let (handle, listener) = self.take();
        self.status.alive.store(false, Ordering::Relaxed);
        if let Some(handle) = handle {
            handle.cluster.shutdown().await;
        }
        if let Some(listener) = listener {
            listener.abort();
        }
    }
}

fn config(
    gossip_addr: SocketAddr,
    ae_interval: Duration,
    tombstone_ttl: Duration,
) -> ClusterConfig {
    ClusterConfig::default().with(|c| {
        c.gossip_bind_addr = gossip_addr;
        c.data_bind_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        c.ae_interval = ae_interval;
        c.tombstone_ttl = tombstone_ttl;
        c.distributed_disown_grace_rounds = DISOWN_GRACE_ROUNDS;
    })
}

async fn open(
    cluster_name: &str,
    gossip_addr: SocketAddr,
    seeds: &[SocketAddr],
    ae_interval: Duration,
    tombstone_ttl: Duration,
    owners: NonZeroU8,
) -> anyhow::Result<Handle> {
    let cluster = Cluster::builder(cluster_name)
        .seeds(seeds.iter().copied())
        .config(config(gossip_addr, ae_interval, tombstone_ttl))
        .build()
        .await
        .context("failed to form cluster")?;
    let cache = cluster
        .cache::<String, String>(CACHE_NAME)
        .mode(Mode::Distributed { owners })
        .open()
        .await
        .context("failed to open the demo cache")?;
    Ok(Handle { cluster, cache })
}

fn spawn_listener(
    node: Arc<NodeSlot>,
    mut events: broadcast::Receiver<Event<String, String>>,
    feed_tx: UnboundedSender<String>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        // Entry counts, warmth and owned-bucket share read on a timer,
        // since events can lag and don't carry ownership information.
        let mut refresh = tokio::time::interval(Duration::from_millis(300));
        refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let probes = sample::probe_keys();
        loop {
            tokio::select! {
                received = events.recv() => match received {
                    Ok(event) => {
                        node.status.writes_applied.fetch_add(1, Ordering::Relaxed);
                        let _ = feed_tx.send(describe_event(node.index, &event));
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        let _ = feed_tx.send(format!(
                            "node{}: event feed lagged, skipped {skipped} events",
                            node.index
                        ));
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                },
                _ = refresh.tick() => {
                    let Some(cache) = node.cache() else { break };
                    let count = cache.entry_count().await;
                    node.status
                        .entry_count
                        .store(i64::try_from(count).unwrap_or(i64::MAX), Ordering::Relaxed);

                    let node_id = sundog::NodeId::from(node.status.node_id.load(Ordering::Relaxed));
                    let hits = sample::count_hits(
                        &probes.iter().map(|k| cache.owners_of(k)).collect::<Vec<_>>(),
                        node_id,
                    );
                    let owned = sample::estimate_owned_buckets(hits, PROBE_COUNT, BUCKET_COUNT);
                    node.status
                        .owned_buckets
                        .store(i64::try_from(owned).unwrap_or(i64::MAX), Ordering::Relaxed);

                    let Some(handle) = node.read_handle() else { break };
                    let warm = handle
                        .cluster
                        .health()
                        .caches
                        .iter()
                        .find(|c| c.name == CACHE_NAME)
                        .is_some_and(|c| c.warm);
                    node.status.warm.store(warm, Ordering::Relaxed);
                }
            }
        }
    })
}

fn describe_event(index: usize, event: &Event<String, String>) -> String {
    match event {
        Event::Created { key, value, origin } => {
            format!(
                "node{index}: created {key:?}={value:?} ({})",
                describe_origin(*origin)
            )
        }
        Event::Updated { key, value, origin } => {
            format!(
                "node{index}: updated {key:?}={value:?} ({})",
                describe_origin(*origin)
            )
        }
        Event::Removed { key, origin } => {
            format!(
                "node{index}: removed {key:?} ({})",
                describe_origin(*origin)
            )
        }
    }
}

fn describe_origin(origin: Origin) -> String {
    match origin {
        Origin::Local => "local".to_owned(),
        Origin::Remote(node) => format!("remote:{node}"),
    }
}

/// Builds `n` node slots on consecutive loopback ports from `base_port`.
#[must_use]
pub(crate) fn build_slots(n: usize, base_port: u16) -> Vec<Arc<NodeSlot>> {
    (0..n)
        .map(|i| {
            let offset = u16::try_from(i).unwrap_or(u16::MAX);
            let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, base_port.saturating_add(offset)));
            Arc::new(NodeSlot::new(i, addr))
        })
        .collect()
}

/// The fixed seed list every node and restart uses: all gossip addresses.
#[must_use]
pub(crate) fn seed_list(nodes: &[Arc<NodeSlot>]) -> Vec<SocketAddr> {
    nodes.iter().map(|n| n.gossip_addr).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_slots_assigns_consecutive_ports() {
        let slots = build_slots(3, 40_000);
        let ports: Vec<u16> = slots.iter().map(|s| s.gossip_addr.port()).collect();
        assert_eq!(ports, vec![40_000, 40_001, 40_002]);
    }

    #[test]
    fn seed_list_covers_every_node() {
        let slots = build_slots(4, 41_000);
        let seeds = seed_list(&slots);
        assert_eq!(seeds.len(), 4);
        for slot in &slots {
            assert!(seeds.contains(&slot.gossip_addr));
        }
    }

    #[test]
    fn fresh_slot_starts_dead_with_no_entries_or_buckets() {
        let slots = build_slots(1, 42_000);
        assert!(!slots[0].is_alive());
        assert_eq!(slots[0].status.entry_count.load(Ordering::Relaxed), 0);
        assert_eq!(slots[0].status.owned_buckets.load(Ordering::Relaxed), 0);
        assert!(!slots[0].status.warm.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn restart_of_a_busy_node_reports_failure() {
        let slots = build_slots(1, 43_000);
        let seeds = seed_list(&slots);
        let (feed_tx, mut feed_rx) = tokio::sync::mpsc::unbounded_channel();
        // Simulate a restart already in flight: the guard must bail out
        // before touching the network and report the outcome as `false`.
        slots[0].status.busy.store(true, Ordering::Release);
        let restarted = slots[0]
            .restart(
                "test-cluster",
                &seeds,
                Duration::from_millis(50),
                Duration::from_secs(30),
                NonZeroU8::new(1).expect("1 is nonzero"),
                &feed_tx,
            )
            .await;
        assert!(!restarted);
        drop(feed_tx);
        assert!(feed_rx.recv().await.is_none());
    }
}

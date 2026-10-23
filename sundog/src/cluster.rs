//! The public entry point. `Cluster::builder(name).build()` forms a cluster;
//! `cluster.cache(name)` opens a named cache on it.
//!
//! `build()` resolves the data-plane address, starts [`Membership`] over
//! gossip, announces through [`Discovery`], and starts the [`Mesh`] with a
//! [`RequestHandler`] that answers state-transfer and anti-entropy requests
//! from this cluster's shard registry. Background tasks, all stopped by
//! [`Cluster::shutdown`], carry membership changes into the mesh and the
//! absence tracker, dispatch inbound messages to shards by cache name, and,
//! per opened cache, fan local writes out per [`Mode`] and collect expired
//! tombstones.

pub(crate) mod absence;
pub(crate) mod anti_entropy;
pub(crate) mod crdt_compact;
pub(crate) mod fan_out;
mod handler;
mod inbound;
pub(crate) mod rebalance;
pub(crate) mod sketch;
pub(crate) mod state_transfer;
#[cfg(all(test, not(feature = "sim")))]
pub(crate) mod test_support;

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::hash::Hash;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

use serde::Serialize;
use serde::de::DeserializeOwned;
use smol_str::SmolStr;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::cache::CacheBuilder;
use crate::config::ClusterConfig;
use crate::discovery::mdns::Mdns;
use crate::discovery::statics::Static;
use crate::discovery::{Discovery, DiscoveryKind};
use crate::error::JoinError;
use crate::membership::{CacheModes, Membership, Peer};
use crate::net::{BoundListener, Greeting, InboundMsg, Mesh, RequestHandler};
use crate::node::{NodeId, NodeName};
use crate::ownership::OwnershipView;
use crate::store::{Mode, PartId, Shard, ShardOps, gc_period_ms};
use crate::wire;
use handler::ClusterRequestHandler;
use inbound::inbound_loop;

/// The cluster's type-erased cache registry: `cache name -> Arc<dyn ShardOps>`.
/// Shared with the [`RequestHandler`] passed to [`Mesh::spawn`].
type ShardRegistry = Arc<RwLock<HashMap<SmolStr, Arc<dyn ShardOps>>>>;

/// Whether `registry` holds `shard` itself under `name`: the same allocation,
/// not another cache that took the name since. The test behind
/// [`Cluster::forget_cache`]. Pure; unit tested directly.
fn registry_holds<S: ShardOps + ?Sized>(
    registry: &HashMap<SmolStr, Arc<dyn ShardOps>>,
    name: &str,
    shard: &Arc<S>,
) -> bool {
    registry
        .get(name)
        .is_some_and(|held| std::ptr::addr_eq(Arc::as_ptr(held), Arc::as_ptr(shard)))
}

/// One-line accessors for [`ShardRegistry`]'s lock, on the lock type itself
/// so they resolve through autoderef from an owned or borrowed registry.
trait ShardRegistryExt {
    fn read_shards(&self) -> RwLockReadGuard<'_, HashMap<SmolStr, Arc<dyn ShardOps>>>;
    fn write_shards(&self) -> RwLockWriteGuard<'_, HashMap<SmolStr, Arc<dyn ShardOps>>>;
}

impl ShardRegistryExt for RwLock<HashMap<SmolStr, Arc<dyn ShardOps>>> {
    fn read_shards(&self) -> RwLockReadGuard<'_, HashMap<SmolStr, Arc<dyn ShardOps>>> {
        self.read()
            .expect("invariant: shard registry lock is never poisoned")
    }

    fn write_shards(&self) -> RwLockWriteGuard<'_, HashMap<SmolStr, Arc<dyn ShardOps>>> {
        self.write()
            .expect("invariant: shard registry lock is never poisoned")
    }
}

/// The same one-line accessors as [`ShardRegistryExt`], for the local
/// cache-mode map's lock.
trait LocalModesExt {
    fn read_modes(&self) -> RwLockReadGuard<'_, HashMap<SmolStr, Mode>>;
    fn write_modes(&self) -> RwLockWriteGuard<'_, HashMap<SmolStr, Mode>>;
}

impl LocalModesExt for RwLock<HashMap<SmolStr, Mode>> {
    fn read_modes(&self) -> RwLockReadGuard<'_, HashMap<SmolStr, Mode>> {
        self.read()
            .expect("invariant: local cache-mode map lock is never poisoned")
    }

    fn write_modes(&self) -> RwLockWriteGuard<'_, HashMap<SmolStr, Mode>> {
        self.write()
            .expect("invariant: local cache-mode map lock is never poisoned")
    }
}

/// A running cluster membership: the join point for opening named caches.
///
/// Cheap to `Clone`; every clone shares the same membership, mesh, and cache
/// registry. Call [`Cluster::shutdown`] for a graceful leave; dropping every
/// clone does not tear it down.
#[derive(Clone)]
pub struct Cluster {
    inner: Arc<ClusterInner>,
}

impl std::fmt::Debug for Cluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cluster")
            .field("node", &self.inner.node)
            .field("name", &self.inner.name)
            .finish_non_exhaustive()
    }
}

struct ClusterInner {
    node: NodeId,
    name: SmolStr,
    membership: Membership,
    /// Whether `ClusterBuilder::build` resolved a fixed, nonempty seed
    /// list. `open()` waits briefly for a first peer only when this is
    /// `true` and `Cluster::peers` is still empty; a seedless cluster is a
    /// legitimate one-node cluster, not a membership race to wait out. See
    /// `cache::open::should_await_first_peer`.
    has_seeds: bool,
    mesh: Mesh,
    shards: ShardRegistry,
    /// The [`Mode`] each open cache opens under; [`mode_conflict_task`]'s
    /// local half, shared with the `prometheus` feature's `/readyz` route.
    local_modes: Arc<RwLock<HashMap<SmolStr, Mode>>>,
    config: ClusterConfig,
    absence: absence::AbsenceTracker,
    /// When [`ClusterBuilder::build`] began, before membership started to
    /// observe anyone; see [`Cluster::started_at`].
    started_at: Instant,
    /// When each peer last streamed replicate traffic in; see
    /// [`Cluster::peer_is_streaming`].
    inbound_activity: Arc<InboundActivity>,
    /// Shared with the request handler, which consults it before donating.
    warmth: Arc<Warmth>,
    tracker: TaskTracker,
    cancel: CancellationToken,
}

/// Per-peer stamps of the last inbound replicate traffic, recorded by
/// [`inbound::inbound_loop`] so the anti-entropy scheduler can leave a streaming peer
/// alone.
#[derive(Default)]
pub(crate) struct InboundActivity {
    seen: RwLock<HashMap<NodeId, u64>>,
}

impl InboundActivity {
    fn seen(&self) -> RwLockReadGuard<'_, HashMap<NodeId, u64>> {
        self.seen
            .read()
            .expect("invariant: inbound-activity lock is never poisoned")
    }

    fn seen_mut(&self) -> RwLockWriteGuard<'_, HashMap<NodeId, u64>> {
        self.seen
            .write()
            .expect("invariant: inbound-activity lock is never poisoned")
    }

    fn note(&self, from: NodeId) {
        self.seen_mut().insert(from, crate::net::mono_ms());
    }

    /// Whether `peer` streamed replicate traffic within `window`.
    pub(crate) fn recent(&self, peer: NodeId, window: Duration) -> bool {
        self.seen().get(&peer).is_some_and(|&at| {
            let age = crate::net::mono_ms().saturating_sub(at);
            age <= u64::try_from(window.as_millis()).unwrap_or(u64::MAX)
        })
    }
}

/// The caches this node can donate a snapshot of: every `Local` or
/// `Invalidation` cache from the moment it opens, and a `Replicated` cache
/// once its own state transfer completes or finds no donor to complete from.
/// A cache still warming declines `StRequest`s, so a joiner never copies a
/// snapshot that is itself a partial copy.
#[derive(Default)]
pub(crate) struct Warmth {
    warm: RwLock<HashSet<SmolStr>>,
}

impl Warmth {
    fn warm(&self) -> RwLockReadGuard<'_, HashSet<SmolStr>> {
        self.warm
            .read()
            .expect("invariant: warmth lock is never poisoned")
    }

    fn warm_mut(&self) -> RwLockWriteGuard<'_, HashSet<SmolStr>> {
        self.warm
            .write()
            .expect("invariant: warmth lock is never poisoned")
    }

    fn mark(&self, cache: &SmolStr) {
        self.warm_mut().insert(cache.clone());
    }

    fn is_warm(&self, cache: &SmolStr) -> bool {
        self.warm().contains(cache)
    }

    fn forget(&self, cache: &str) {
        self.warm_mut().remove(cache);
    }
}

/// One open cache's contribution to [`Cluster::health`]: its mode and
/// whether it has finished warming.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct CacheHealth {
    /// The cache's name, as opened.
    pub name: SmolStr,
    /// The [`Mode`] the cache opens under.
    pub mode: Mode,
    /// Whether the cache has finished warming: for a [`Mode::Replicated`]
    /// cache, whether its state transfer has completed; for a
    /// [`Mode::Distributed`] cache, whether the buckets it owns have been
    /// pulled from a co-owner; always `true` for `Local`/`Invalidation`
    /// caches, which are warm from the moment they open.
    pub warm: bool,
}

/// A snapshot of one node's health, returned by [`Cluster::health`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Health {
    /// Whether this node is ready to serve; see [`Cluster::is_ready`].
    pub ready: bool,
    /// The current live peer count.
    pub live_peers: usize,
    /// Every open cache's mode and warmth.
    pub caches: Vec<CacheHealth>,
}

/// Every open cache's mode and warmth, from the maps [`Cluster`] and the
/// `prometheus` listener share.
fn cache_health(local_modes: &RwLock<HashMap<SmolStr, Mode>>, warmth: &Warmth) -> Vec<CacheHealth> {
    local_modes
        .read_modes()
        .iter()
        .map(|(name, &mode)| CacheHealth {
            name: name.clone(),
            mode,
            warm: warmth.is_warm(name),
        })
        .collect()
}

/// Ready once every [`Mode::Replicated`] cache in `caches` is warm; a
/// `Local` or `Invalidation` cache is warm from the moment it opens.
fn is_ready_from(caches: &[CacheHealth]) -> bool {
    caches
        .iter()
        .filter(|cache| cache.mode == Mode::Replicated)
        .all(|cache| cache.warm)
}

/// Builds a [`Cluster`]: own-and-return. `.build()` alone forms a working LAN
/// cluster.
#[must_use]
pub struct ClusterBuilder {
    name: SmolStr,
    discovery: Option<DiscoveryKind>,
    config: ClusterConfig,
    node_id: Option<NodeId>,
    /// What [`ClusterBuilder::tls`] set, applied over `config` at `build()` so
    /// a later [`ClusterBuilder::config`] does not replace it.
    #[cfg(feature = "tls")]
    tls: Option<crate::config::TlsConfig>,
    #[cfg(feature = "prometheus")]
    prometheus_listen: Option<SocketAddr>,
}

impl Cluster {
    /// Starts building a cluster named `name`, chitchat's cluster id.
    pub fn builder(name: impl Into<SmolStr>) -> ClusterBuilder {
        ClusterBuilder {
            name: name.into(),
            discovery: None,
            config: ClusterConfig::default(),
            node_id: None,
            #[cfg(feature = "tls")]
            tls: None,
            #[cfg(feature = "prometheus")]
            prometheus_listen: None,
        }
    }

    /// This node's id.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.inner.node
    }

    /// This node's current membership incarnation: the value CRDT writer
    /// identity (`WriterId { node, incarnation }`) reuses so a restarted
    /// process writes from a fresh, never-before-seen slot instead of
    /// resuming one a compaction sweep may already have retired.
    pub(crate) fn local_incarnation(&self) -> u64 {
        self.inner.membership.local_peer().incarnation
    }

    /// The instant this cluster's build began: the earliest moment this
    /// process could have observed any member, so the earliest a writer of
    /// a node it never saw counts as absent for CRDT retirement.
    pub(crate) fn started_at(&self) -> Instant {
        self.inner.started_at
    }

    /// Opens or joins a named cache. Call `.open().await` on the result to
    /// register it.
    pub fn cache<K, V>(&self, name: impl Into<SmolStr>) -> CacheBuilder<K, V>
    where
        K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
        V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    {
        CacheBuilder::new(self.clone(), name.into())
    }

    pub(crate) fn config(&self) -> &ClusterConfig {
        &self.inner.config
    }

    pub(crate) fn shards(&self) -> ShardRegistry {
        Arc::clone(&self.inner.shards)
    }

    /// This cluster's partition-aware tombstone-retention view; see
    /// `cluster::absence`. Cheap to clone.
    pub(crate) fn absence_tracker(&self) -> absence::AbsenceTracker {
        self.inner.absence.clone()
    }

    /// Every member away less than `bound` ago; see
    /// [`absence::AbsenceTracker::away_within`]. Membership hands the
    /// tracker each live set before publishing its peers and cache modes,
    /// so a caller acting on an ownership view never reads the tracker
    /// behind the absence that changed the view.
    pub(crate) fn away_within(&self, bound: Duration) -> Vec<NodeId> {
        self.absence_tracker().away_within(bound)
    }

    pub(crate) fn mesh(&self) -> &Mesh {
        &self.inner.mesh
    }

    /// The live peer set, as membership reports it, for diagnostics.
    #[must_use]
    pub fn peers(&self) -> Vec<Peer> {
        self.inner.membership.peers().borrow().clone()
    }

    /// Whether this cluster was built with a fixed, nonempty seed list; see
    /// [`ClusterInner::has_seeds`].
    pub(crate) fn has_seeds(&self) -> bool {
        self.inner.has_seeds
    }

    /// Whether this node is ready to serve: the data-plane listener is bound
    /// (always true once `build()` has returned) and every open
    /// [`Mode::Replicated`] cache has finished warming; see
    /// [`Cluster::health`] for the detail behind this summary.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.health().ready
    }

    /// A snapshot of this node's health: readiness, the live peer count, and
    /// every open cache's mode and warmth.
    ///
    /// # Panics
    ///
    /// Panics if the local cache-mode map lock is poisoned.
    #[must_use]
    pub fn health(&self) -> Health {
        let caches = cache_health(&self.inner.local_modes, &self.inner.warmth);
        Health {
            ready: is_ready_from(&caches),
            live_peers: self.peers().len(),
            caches,
        }
    }

    /// Records `mode` as this node's [`Mode`] for cache `name` and gossips it
    /// to peers.
    pub(crate) fn advertise_cache_mode(&self, name: &SmolStr, mode: Mode) {
        self.inner
            .local_modes
            .write_modes()
            .insert(name.clone(), mode);
        self.inner.membership.set_cache_mode(name, mode);
    }

    /// Every live peer's advertised cache modes, as membership reports them.
    pub(crate) fn advertised_cache_modes(&self) -> CacheModes {
        self.inner.membership.cache_modes().borrow().clone()
    }

    /// A fresh watch subscription on every live peer's advertised cache
    /// modes, for [`crate::ownership::refresh_task`]'s trigger.
    pub(crate) fn cache_modes_watch(&self) -> watch::Receiver<CacheModes> {
        self.inner.membership.cache_modes()
    }

    /// Records that this node can donate a snapshot of `cache` from now on;
    /// see [`Warmth`].
    pub(crate) fn mark_warm(&self, cache: &SmolStr) {
        self.inner.warmth.mark(cache);
    }

    /// Whether this node donates snapshots of `cache`; see [`Warmth`].
    #[cfg(all(test, not(feature = "sim")))]
    pub(crate) fn is_warm(&self, cache: &SmolStr) -> bool {
        self.inner.warmth.is_warm(cache)
    }

    /// The gossip address this node bound, so a test harness binds port 0
    /// and seeds the next node against the real address rather than
    /// reserving a port up front, which another process can take before
    /// the node binds it. `#[doc(hidden)]`: not part of the supported API.
    #[doc(hidden)]
    #[must_use]
    pub fn local_gossip_addr(&self) -> SocketAddr {
        self.inner.membership.local_peer().gossip_addr
    }

    /// This node's own [`Peer`] record, for a test harness that injects it
    /// into another node's mesh by hand.
    #[cfg(all(test, not(feature = "sim")))]
    pub(crate) fn local_peer(&self) -> Peer {
        self.inner.membership.local_peer().clone()
    }

    /// The cluster-side half of [`crate::cache::Cache::close`]: drops `name`
    /// from the shard registry, this node's local mode map and warmth set,
    /// and clears its gossiped mode and loader so live peers stop seeing
    /// either advertised. Acts only when the registry holds `shard` under
    /// `name` ([`registry_holds`]) and answers whether it did, so a close
    /// that finds the name free, or reopened as another cache, changes
    /// nothing, gossip included: the close that freed the name already
    /// cleared it. The registry stays write-locked through the clears, so an
    /// `open` of the name, which reserves it under that lock before it
    /// advertises, lands strictly after them.
    pub(crate) fn forget_cache<S: ShardOps + ?Sized>(&self, name: &str, shard: &Arc<S>) -> bool {
        let mut registry = self.inner.shards.write_shards();
        if !registry_holds(&registry, name, shard) {
            return false;
        }
        registry.remove(name);
        self.inner.local_modes.write_modes().remove(name);
        self.inner.warmth.forget(name);
        self.inner.membership.clear_cache_mode(name);
        self.inner.membership.clear_loader(name);
        true
    }

    /// Gossips that this node has a loader for cache `name`, so peers pick
    /// it to load and refresh that cache's keys.
    pub(crate) fn advertise_loader(&self, name: &str) {
        self.inner.membership.set_loader(name);
    }

    /// Every live peer's advertised loaders, as membership reports them.
    pub(crate) fn advertised_loaders(&self) -> crate::membership::CacheLoaders {
        self.inner.membership.cache_loaders().borrow().clone()
    }

    /// A fresh watch subscription on the live peer set, for loops that react to
    /// changes.
    pub(crate) fn peers_watch(&self) -> tokio::sync::watch::Receiver<Vec<Peer>> {
        self.inner.membership.peers()
    }

    /// The live peer set as node ids only, what `Cache`'s fan-out loop
    /// iterates.
    pub(crate) fn live_peer_ids(&self) -> Vec<NodeId> {
        self.inner
            .membership
            .peers()
            .borrow()
            .iter()
            .map(|peer| peer.node)
            .collect()
    }

    /// Whether replicate traffic between this node and `peer` is in motion
    /// in either direction, judged over one `ae_interval`. Anti-entropy
    /// leaves such a peer alone, since repairing in parallel would ship records
    /// twice.
    pub(crate) fn peer_is_streaming(&self, peer: NodeId) -> bool {
        let window = self.inner.config.ae_interval;
        self.inner.inbound_activity.recent(peer, window)
            || self.inner.mesh.replicate_in_flight(peer, window)
    }

    /// A child of this cluster's shutdown token: cancelled the moment
    /// [`Cluster::shutdown`] runs.
    pub(crate) fn cancel_token(&self) -> CancellationToken {
        self.inner.cancel.child_token()
    }

    /// Spawns `fut` on this cluster's [`TaskTracker`], so [`Cluster::shutdown`]
    /// waits for it.
    pub(crate) fn spawn_tracked<F>(&self, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.inner.tracker.spawn(fut);
    }

    /// [`Cluster::spawn_tracked`], also tracked by `tasks`, the tracker one
    /// cache's `close` waits on.
    pub(crate) fn spawn_tracked_in<F>(&self, tasks: &TaskTracker, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.inner.tracker.spawn(tasks.track_future(fut));
    }

    /// Leaves the cluster gracefully: every still-registered cache first
    /// applies the folds still pending in its
    /// [`merge_coalesce_window`](crate::cache::CacheBuilder::merge_coalesce_window)
    /// (a crash loses them) and seals its fan-out queue, so those folds and
    /// every write accepted until now are sent. From then on a write that
    /// must be forwarded to another owner fails with
    /// [`CacheError::Closed`](crate::error::CacheError::Closed), and a write
    /// this node applies itself lands locally, detached, and is not sent.
    /// A node with [`Mode::Distributed`] caches then reconciles the parts it
    /// owns, folds included, with each live co-owner, retrying a round that
    /// fails or answers `Stale` within that co-owner's share of
    /// [`ClusterConfig::state_transfer_budget`], so the shutdown can take
    /// that long and logs at `warn` the parts still diverging when a
    /// group ends. Then background loops are cancelled and joined, every
    /// still-registered cache has its spill tier closed (see
    /// [`ShardOps::close_spill_checkpointed`], a no-op if already closed),
    /// chitchat departs and the data plane sends the invalidations and writes
    /// still queued for each peer, waiting up to 500 ms for them, and closes
    /// its connections. A frame still queued then is discarded and logged at
    /// `warn`. No further calls on any clone of this handle; cache handles
    /// opened before this call keep working for local reads/writes.
    ///
    /// # Panics
    ///
    /// Panics if the shard registry lock is poisoned.
    pub async fn shutdown(self) {
        // Every cache's fan-out queue is sealed before any task is
        // cancelled: a write accepted until now stays in the backlog its
        // fan-out task drains on the way out, a write that must be
        // forwarded to another owner fails with `CacheError::Closed`
        // instead of being accepted and never sent, and a write this node
        // applies itself lands locally, detached. Each seal applies the folds still pending in a
        // merge-coalesce window first, so they join the backlog, and the
        // leave hand-off below sees the keys they changed.
        for shard in self.inner.shards.read_shards().values() {
            shard.seal_fan_out();
        }
        self.hand_off_before_leaving().await;
        self.inner.cancel.cancel();
        self.inner.tracker.close();
        self.inner.tracker.wait().await;
        // Collected first so the read guard is never held across an .await.
        let shards: Vec<Arc<dyn ShardOps>> =
            self.inner.shards.read_shards().values().cloned().collect();
        for shard in shards {
            shard.close_spill_checkpointed().await;
        }
        self.inner.membership.clone().shutdown().await;
        self.inner.mesh.clone().shutdown().await;
        tracing::info!(node = %self.inner.node, cluster = %self.inner.name, "cluster shut down");
    }

    /// Converges every part this node owns with each live co-owner before
    /// it leaves, for every `Mode::Distributed` cache. After a membership
    /// change a co-owner can still be receiving its copy, and once this node
    /// departs the survivors pull only from each other, so a copy only this
    /// node held would be lost. Both sides still share the view that makes
    /// them co-owners, so every part is in anti-entropy scope.
    ///
    /// Every (cache, co-owner) group gets an equal share of the time left in
    /// `state_transfer_budget` ([`group_deadline`]), and the time a quick
    /// group leaves unused passes to the groups after it. Within its share a
    /// group runs up to [`LEAVE_HAND_OFF_ROUNDS`] completed rounds; a failed
    /// or `Stale` round does not count and is retried after
    /// [`rebalance::GROUP_RETRY_BACKOFF`] while the co-owner is live
    /// ([`hand_off_step`]). Parts still diverging when a group ends log at
    /// `warn`.
    async fn hand_off_before_leaving(&self) {
        let overall = tokio::time::Instant::now() + self.config().state_transfer_budget;
        let live: HashSet<NodeId> = self.live_peer_ids().into_iter().collect();
        let shards: Vec<(SmolStr, Arc<dyn ShardOps>)> = self
            .inner
            .shards
            .read_shards()
            .iter()
            .map(|(name, shard)| (name.clone(), Arc::clone(shard)))
            .collect();
        let mut groups: Vec<HandOffGroup<Arc<dyn ShardOps>>> = Vec::new();
        for (cache, shard) in shards {
            let Some(view) = shard.ownership_view() else {
                continue;
            };
            for (peer, parts) in hand_off_groups(&view, self.node_id(), &live) {
                groups.push(HandOffGroup {
                    cache: cache.clone(),
                    shard: Arc::clone(&shard),
                    peer,
                    parts,
                });
            }
        }
        let skipped = hand_off_in_shares(
            groups,
            overall,
            |shard, cache, peer, diverging| async move {
                anti_entropy::run_round_for_parts(self.mesh(), &shard, &cache, peer, &diverging)
                    .await
            },
            |peer| self.live_peer_ids().contains(&peer),
        )
        .await;
        if skipped > 0 {
            tracing::warn!(
                skipped_groups = skipped,
                "leave hand-off ran out of state_transfer_budget"
            );
        }
    }

    /// Stops this node as a killed process would: background loops are
    /// cancelled mid-flight, nothing queued for fan-out is flushed, gossip
    /// stops with no departure, and the data plane drops its connections.
    /// Peers learn of it only through the failure detector. A test seam for
    /// crash scenarios run in-process; `#[doc(hidden)]`: not part of the
    /// supported API.
    #[doc(hidden)]
    pub async fn crash(self) {
        self.inner.cancel.cancel();
        self.inner.tracker.close();
        self.inner.tracker.wait().await;
        self.inner.membership.clone().crash().await;
        self.inner.mesh.clone().shutdown().await;
        tracing::info!(node = %self.inner.node, cluster = %self.inner.name, "cluster crashed");
    }
}

/// Completed anti-entropy rounds [`Cluster::hand_off_before_leaving`] runs
/// against each co-owner. A failed or `Stale` round does not count: it is
/// retried after [`rebalance::GROUP_RETRY_BACKOFF`] until the group's deadline.
const LEAVE_HAND_OFF_ROUNDS: u32 = 3;

/// The parts one cache shares with one live co-owner: a unit of
/// [`Cluster::hand_off_before_leaving`], collected before its first round.
struct HandOffGroup<S> {
    cache: SmolStr,
    /// What a round needs besides the cache and the co-owner: the cache's
    /// shard.
    shard: S,
    peer: NodeId,
    parts: Vec<PartId>,
}

/// The deadline of the group about to start, one of `groups_left` including
/// itself: an equal share of the time left before `overall`. The last group
/// gets all of it, so a quick group's unused time passes to the groups after
/// it, and an unreachable co-owner uses at most its share. A count of zero
/// reads as one.
fn group_deadline(
    now: tokio::time::Instant,
    overall: tokio::time::Instant,
    groups_left: usize,
) -> tokio::time::Instant {
    let remaining = overall.saturating_duration_since(now);
    let groups = u32::try_from(groups_left.max(1)).unwrap_or(u32::MAX);
    now + remaining / groups
}

/// What a finished round of [`hand_off_with_peer`] leaves for
/// [`hand_off_step`] to judge.
#[derive(Debug, Clone, Copy)]
struct RoundEnd {
    /// The round failed or the co-owner answered `Stale`.
    failed: bool,
    /// Parts still diverging after the round.
    diverging: usize,
    /// Rounds that completed so far, this one included when it did.
    completed_rounds: u32,
    /// The group's deadline has passed.
    deadline_passed: bool,
    /// The co-owner is in the live set. Read before a retry only: a round
    /// that completed shows the co-owner answers.
    peer_live: bool,
}

/// Why a group's hand-off ended: the cause a `warn` names when parts are left
/// diverging, so an operator sees which knob to turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HandOffEnd {
    /// Every part matches.
    Matched,
    /// The group's share of `state_transfer_budget` ran out.
    DeadlinePassed,
    /// The co-owner left the live set.
    PeerLeft,
    /// The completed rounds are spent.
    RoundsSpent,
}

impl std::fmt::Display for HandOffEnd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Matched => "every part matches",
            Self::DeadlinePassed => "the group's share of state_transfer_budget ran out",
            Self::PeerLeft => "the co-owner left the live set",
            Self::RoundsSpent => "the completed rounds were spent",
        })
    }
}

/// What a group does after a round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HandOffStep {
    /// Sleep [`rebalance::GROUP_RETRY_BACKOFF`] and run again; the failed
    /// round does not count against [`LEAVE_HAND_OFF_ROUNDS`].
    RetryAfterBackoff,
    /// Run the next round over the parts still diverging.
    NextRound,
    /// Nothing is left to do, for the reason given.
    Done(HandOffEnd),
}

/// The step after a round: pure, so a table pins every case.
fn hand_off_step(end: RoundEnd) -> HandOffStep {
    if end.diverging == 0 {
        HandOffStep::Done(HandOffEnd::Matched)
    } else if end.deadline_passed {
        HandOffStep::Done(HandOffEnd::DeadlinePassed)
    } else if !end.peer_live {
        HandOffStep::Done(HandOffEnd::PeerLeft)
    } else if end.failed {
        HandOffStep::RetryAfterBackoff
    } else if end.completed_rounds >= LEAVE_HAND_OFF_ROUNDS {
        HandOffStep::Done(HandOffEnd::RoundsSpent)
    } else {
        HandOffStep::NextRound
    }
}

/// Runs rounds against one co-owner over the parts still diverging, until
/// they match, `deadline` passes, `peer_live` answers `false` before a retry,
/// or [`LEAVE_HAND_OFF_ROUNDS`] rounds complete ([`hand_off_step`]). A round
/// still running at `deadline` is dropped. Returns the parts still
/// diverging, and logs them at `warn`, with why the group ended, when there
/// are any.
async fn hand_off_with_peer<F, Fut, L>(
    cache: &str,
    peer: NodeId,
    mut run_round: F,
    mut peer_live: L,
    parts: Vec<PartId>,
    deadline: tokio::time::Instant,
) -> Vec<PartId>
where
    F: FnMut(Vec<PartId>) -> Fut,
    Fut: Future<Output = anti_entropy::PartRoundOutcome>,
    L: FnMut() -> bool,
{
    let mut diverging = parts;
    let mut completed_rounds = 0;
    // A loop that stops on its condition or on a dropped round ran into the
    // deadline.
    let mut ended = HandOffEnd::DeadlinePassed;
    while !diverging.is_empty() && tokio::time::Instant::now() < deadline {
        let Ok(outcome) = tokio::time::timeout_at(deadline, run_round(diverging.clone())).await
        else {
            break;
        };
        if !outcome.failed {
            completed_rounds += 1;
            diverging.retain(|part| !outcome.matched.contains(part));
        }
        let step = hand_off_step(RoundEnd {
            failed: outcome.failed,
            diverging: diverging.len(),
            completed_rounds,
            deadline_passed: tokio::time::Instant::now() >= deadline,
            peer_live: !outcome.failed || peer_live(),
        });
        match step {
            HandOffStep::Done(why) => {
                ended = why;
                break;
            }
            HandOffStep::NextRound => {}
            HandOffStep::RetryAfterBackoff => {
                tracing::debug!(
                    cache = %cache,
                    %peer,
                    diverging = diverging.len(),
                    "leave hand-off round failed; retrying after the backoff"
                );
                let left = deadline.saturating_duration_since(tokio::time::Instant::now());
                tokio::time::sleep(rebalance::GROUP_RETRY_BACKOFF.min(left)).await;
            }
        }
    }
    if diverging.is_empty() {
        tracing::debug!(cache = %cache, %peer, "leave hand-off with co-owner done");
    } else {
        tracing::warn!(
            cache = %cache,
            %peer,
            diverging = diverging.len(),
            reason = %ended,
            "leave hand-off ended with parts still diverging"
        );
    }
    diverging
}

/// Runs [`hand_off_with_peer`] for each of `groups` in order, each with the
/// deadline [`group_deadline`] gives it. `run_round` runs one round for a
/// group's shard, cache and co-owner over the parts it is handed, and
/// `peer_live` answers whether a co-owner is in the live set. A group that
/// would start after `overall` has passed is skipped, logged at `debug`, and
/// counted in the answer.
async fn hand_off_in_shares<S, F, Fut, L>(
    groups: Vec<HandOffGroup<S>>,
    overall: tokio::time::Instant,
    mut run_round: F,
    mut peer_live: L,
) -> usize
where
    S: Clone,
    F: FnMut(S, SmolStr, NodeId, Vec<PartId>) -> Fut,
    Fut: Future<Output = anti_entropy::PartRoundOutcome>,
    L: FnMut(NodeId) -> bool,
{
    let total = groups.len();
    let mut skipped = 0;
    for (index, group) in groups.into_iter().enumerate() {
        let HandOffGroup {
            cache,
            shard,
            peer,
            parts,
        } = group;
        let now = tokio::time::Instant::now();
        if now >= overall {
            tracing::debug!(
                cache = %cache,
                %peer,
                "leave hand-off skips a group: state_transfer_budget ran out before it started"
            );
            skipped += 1;
            continue;
        }
        hand_off_with_peer(
            &cache,
            peer,
            |diverging| run_round(shard.clone(), cache.clone(), peer, diverging),
            || peer_live(peer),
            parts,
            group_deadline(now, overall, total - index),
        )
        .await;
    }
    skipped
}

/// The parts `view` owns, grouped by each co-owner in `live`, in node order:
/// what a leaving node converges with whom before it goes.
fn hand_off_groups(
    view: &OwnershipView,
    self_node: NodeId,
    live: &HashSet<NodeId>,
) -> Vec<(NodeId, Vec<PartId>)> {
    let mut groups: std::collections::BTreeMap<NodeId, Vec<PartId>> =
        std::collections::BTreeMap::new();
    for part in view.owned_parts() {
        for &owner in view.owners_of(part) {
            if owner != self_node && live.contains(&owner) {
                groups.entry(owner).or_default().push(part);
            }
        }
    }
    groups.into_iter().collect()
}

impl ClusterBuilder {
    /// A fixed seed list, switching discovery from the zeroconf default
    /// (`Mdns`) to [`Static`].
    pub fn seeds(mut self, seeds: impl IntoIterator<Item = SocketAddr>) -> Self {
        self.discovery = Some(DiscoveryKind::Static(Static::new(seeds)));
        self
    }

    /// Overrides the zeroconf default (`Mdns`) discovery mechanism, e.g. with
    /// `DnsSrv`.
    pub fn discovery(mut self, discovery: impl Discovery + 'static) -> Self {
        self.discovery = Some(DiscoveryKind::Custom(Box::new(discovery)));
        self
    }

    /// Overrides the default [`ClusterConfig`] as a whole, except that a
    /// setting made with `ClusterBuilder::tls` before this call is kept and
    /// takes precedence over `ClusterConfig::tls`.
    pub fn config(mut self, config: ClusterConfig) -> Self {
        self.config = config;
        self
    }

    /// Uses `id` as this node's identity instead of a fresh
    /// [`NodeId::random`]. Persist it and pass the same value on restart:
    /// the node rejoins as the same member, so its prior incarnation is
    /// not tracked absent for `tombstone_max_ttl` after every restart.
    /// Validated at [`build`](Self::build): a merge-derived id is rejected
    /// with [`JoinError::InvalidConfig`] rather than silently remapped.
    pub fn node_id(mut self, id: NodeId) -> Self {
        self.node_id = Some(id);
        self
    }

    /// Installs a Prometheus recorder and serves `GET /metrics`, `GET /readyz`,
    /// and `GET /healthz` on `addr` for the life of the process, once
    /// [`build`](Self::build) succeeds.
    ///
    /// The listener also serves `GET /health` as an alias of `/healthz`,
    /// answers `HEAD` with the headers of the `GET` and no body, ignores a
    /// query string, and answers any other method on those paths with `405`
    /// and an unknown path with `404` whatever the method. A client has 5
    /// seconds to send a request head of at most 8 KiB and 30 seconds to take
    /// the response, and at most 64 connections are served at once. Open
    /// `addr` to scrapers and probes only.
    ///
    /// `metrics`'s recorder is a single process-global slot: a second call to
    /// this, or mixing it with [`crate::telemetry::prometheus_handle`], fails
    /// `build()` with [`JoinError::Bind`] rather than panicking.
    #[cfg(feature = "prometheus")]
    pub fn prometheus_listen(mut self, addr: SocketAddr) -> Self {
        self.prometheus_listen = Some(addr);
        self
    }

    /// Enables mutual TLS on the mesh. The setting takes precedence over
    /// [`ClusterConfig::tls`], whatever the order of this call and
    /// [`Self::config`].
    #[cfg(feature = "tls")]
    pub fn tls(mut self, tls: crate::config::TlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Starts discovery, membership, and the data-plane mesh, and returns
    /// once this node has joined or begun forming the cluster. mDNS finding
    /// nobody is not an error; it is a healthy single-node cluster.
    ///
    /// # Errors
    ///
    /// Returns [`JoinError`] if the sockets cannot bind, the membership
    /// backend fails to start, or [`Self::node_id`] receives a
    /// merge-derived id.
    pub async fn build(self) -> Result<Cluster, JoinError> {
        let started_at = Instant::now();
        let Self {
            name,
            discovery,
            config,
            node_id,
            #[cfg(feature = "tls")]
            tls,
            #[cfg(feature = "prometheus")]
            prometheus_listen,
        } = self;

        #[cfg(feature = "tls")]
        let config = with_explicit_tls(config, tls);
        validate_config(&config)?;
        if let Some(id) = node_id.filter(|id| id.is_merge_derived()) {
            // Mirrors `NodeId::MERGE_BIT`: a real id always has this bit
            // clear, so any id at or past it is reserved for merge versions.
            return Err(JoinError::InvalidConfig(format!(
                "node_id ({}) is merge-derived, over the {} limit",
                id.as_u64(),
                1u64 << 63
            )));
        }

        let local_modes: Arc<RwLock<HashMap<SmolStr, Mode>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let warmth = Arc::new(Warmth::default());

        #[cfg(feature = "prometheus")]
        if let Some(addr) = prometheus_listen {
            install_readiness_listener(addr, &local_modes, &warmth)?;
        }

        let node = node_id.unwrap_or_else(NodeId::random);
        let hostname = local_hostname();
        let node_name = NodeName::new(&hostname, node);
        let discovery = resolve_discovery(discovery, &name, &node_name);
        let has_seeds = discovery.has_seeds();

        let data_listener = BoundListener::bind(config.data_bind_addr).await?;
        let data_bind_addr = data_listener.local_addr();
        let advertise_ip = crate::membership::advertise_ip_for(&config, data_bind_addr.ip());
        let advertise_data_addr = SocketAddr::new(advertise_ip, data_bind_addr.port());

        let absence = absence::AbsenceTracker::default();
        let observed = absence.clone();
        let membership = Membership::spawn_observed(
            name.clone(),
            node,
            &hostname,
            advertise_data_addr,
            &config,
            discovery.candidates(),
            Some(Box::new(move |live| observed.observe(live))),
        )
        .await?;

        let gossip_addr = membership.local_peer().gossip_addr;
        if let Err(error) = discovery.announce(gossip_addr).await {
            tracing::warn!(%error, "discovery announce failed; continuing as a healthy single-node cluster");
        }

        let incarnation = membership.local_peer().incarnation;
        let shards: ShardRegistry = Arc::new(RwLock::new(HashMap::new()));
        let handler: Arc<dyn RequestHandler> = Arc::new(ClusterRequestHandler {
            shards: Arc::clone(&shards),
            warmth: Arc::clone(&warmth),
            ae_part_min_bucket: config.ae_part_min_bucket,
            ae_sketch_min_bucket: config.ae_sketch_min_bucket,
            ae_sketch_cells: config.ae_sketch_cells,
            rebalance_chunk_bytes: config.rebalance_chunk_bytes_value(),
        });
        let greeting = Greeting {
            node,
            incarnation,
            cluster: wire::cluster_id(&name),
        };
        let (mesh, inbound_rx) = Mesh::spawn_on(data_listener, greeting, &config, handler)?;

        let cluster = Cluster {
            inner: Arc::new(ClusterInner {
                node,
                name: name.clone(),
                membership,
                has_seeds,
                mesh,
                shards,
                local_modes,
                config,
                absence,
                started_at,
                inbound_activity: Arc::new(InboundActivity::default()),
                warmth,
                tracker: TaskTracker::new(),
                cancel: CancellationToken::new(),
            }),
        };

        spawn_cluster_background_tasks(&cluster, inbound_rx);

        tracing::info!(
            %node,
            cluster = %name,
            data_addr = %advertise_data_addr,
            gossip_addr = %gossip_addr,
            "cluster formed"
        );

        Ok(cluster)
    }
}

/// The `/readyz` view over the maps the eventual [`Cluster`] shares.
#[cfg(feature = "prometheus")]
struct ClusterReadiness {
    local_modes: Arc<RwLock<HashMap<SmolStr, Mode>>>,
    warmth: Arc<Warmth>,
}

#[cfg(feature = "prometheus")]
impl crate::telemetry::ReadinessSource for ClusterReadiness {
    fn is_ready(&self) -> bool {
        is_ready_from(&cache_health(&self.local_modes, &self.warmth))
    }
}

/// Whether an unset discovery mechanism falls back to
/// [`Static::from_env`] instead of the zeroconf [`Mdns`] default: `true`
/// exactly when no discovery source is configured and `SUNDOG_SEEDS` names
/// at least one seed.
fn use_static_from_env(discovery_set: bool, seeds_env: Option<&str>) -> bool {
    !discovery_set && seeds_env.is_some_and(|raw| !raw.trim().is_empty())
}

/// `config` with the TLS setting of [`ClusterBuilder::tls`] applied: `explicit`
/// replaces [`ClusterConfig::tls`] when set, and `config` is unchanged when
/// not. Pure; unit tested directly.
#[cfg(feature = "tls")]
fn with_explicit_tls(
    mut config: ClusterConfig,
    explicit: Option<crate::config::TlsConfig>,
) -> ClusterConfig {
    if explicit.is_some() {
        config.tls = explicit;
    }
    config
}

/// Validates the `ClusterConfig` invariants `build()` cannot recover from:
/// `max_frame` against the wire codec's hard cap and the sketch frame size
/// it implies. `rebalance_chunk_bytes` and `rebalance_ack_window` are not
/// checked here: both have a bound of their own
/// ([`ClusterConfig::rebalance_chunk_bytes_value`],
/// [`ClusterConfig::rebalance_ack_window_max`]) applied as a clamp at the
/// point of use, so tuning `ae_interval` without separately re-tuning
/// `rebalance_ack_window` still builds.
fn validate_config(config: &ClusterConfig) -> Result<(), JoinError> {
    if config.max_frame > wire::MAX_FRAME {
        return Err(JoinError::InvalidConfig(format!(
            "ClusterConfig::max_frame ({}) exceeds the wire codec's hard cap of {} bytes",
            config.max_frame,
            wire::MAX_FRAME
        )));
    }
    // Sized for a cache name of up to 255 bytes; the wire codec still
    // refuses a frame that a longer name pushes past `max_frame`.
    let sketch_frame = wire::ae_sketch_frame_max_len(255, config.ae_sketch_cells);
    if sketch_frame > config.max_frame {
        return Err(JoinError::InvalidConfig(format!(
            "ClusterConfig::ae_sketch_cells implies an anti-entropy sketch frame of {sketch_frame} bytes, over the {}-byte max_frame limit",
            config.max_frame
        )));
    }
    if config.fan_out_backlog_capacity == 0 {
        return Err(JoinError::InvalidConfig(
            "ClusterConfig::fan_out_backlog_capacity must be nonzero".to_string(),
        ));
    }
    Ok(())
}

/// Resolves the discovery mechanism `build()` uses: the caller's explicit
/// choice, else [`use_static_from_env`]'s pick between `Static::from_env()`
/// and the zeroconf `Mdns` default.
pub(crate) fn resolve_discovery(
    discovery: Option<DiscoveryKind>,
    name: &SmolStr,
    node_name: &NodeName,
) -> DiscoveryKind {
    let discovery_set = discovery.is_some();
    let seeds_env = std::env::var(crate::discovery::statics::SUNDOG_SEEDS_ENV).ok();
    discovery.unwrap_or_else(|| {
        if use_static_from_env(discovery_set, seeds_env.as_deref()) {
            DiscoveryKind::Static(Static::from_env())
        } else {
            DiscoveryKind::Mdns(Mdns::new(name.clone(), node_name.to_string()))
        }
    })
}

/// Installs the `/metrics`, `/readyz`, `/healthz` listener on `addr`.
#[cfg(feature = "prometheus")]
fn install_readiness_listener(
    addr: SocketAddr,
    local_modes: &Arc<RwLock<HashMap<SmolStr, Mode>>>,
    warmth: &Arc<Warmth>,
) -> Result<(), JoinError> {
    let readiness: Arc<dyn crate::telemetry::ReadinessSource> = Arc::new(ClusterReadiness {
        local_modes: Arc::clone(local_modes),
        warmth: Arc::clone(warmth),
    });
    crate::telemetry::install_listener(addr, readiness).map_err(|source| JoinError::Bind {
        addr,
        source: std::io::Error::other(source),
    })
}

/// Spawns every background loop a freshly built [`Cluster`] keeps running: peer
/// republishing, the mode-mismatch sweep, absence tracking, inbound dispatch,
/// and the open-cache gauge.
fn spawn_cluster_background_tasks(cluster: &Cluster, inbound_rx: mpsc::Receiver<InboundMsg>) {
    cluster.spawn_tracked(membership_to_mesh_task(
        cluster.inner.membership.peers(),
        cluster.inner.mesh.clone(),
        cluster.cancel_token(),
    ));
    cluster.spawn_tracked(mode_conflict_task(cluster.clone(), cluster.cancel_token()));
    cluster.spawn_tracked(inbound_loop(
        cluster.shards(),
        Arc::clone(&cluster.inner.inbound_activity),
        cluster.inner.mesh.clone(),
        cluster.node_id(),
        inbound_rx,
        cluster.cancel_token(),
    ));
    cluster.spawn_tracked(open_cache_gauge_task(
        cluster.shards(),
        cluster.cancel_token(),
    ));
}

pub(crate) fn local_hostname() -> String {
    hostname::get()
        .ok()
        .and_then(|name| name.into_string().ok())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "localhost".to_string())
}

/// Groups `items` by key, keys in first-seen order, each group's values in
/// input order.
pub(crate) fn group_in_order<K, V>(
    items: impl IntoIterator<Item = (K, impl IntoIterator<Item = V>)>,
) -> Vec<(K, Vec<V>)>
where
    K: Eq + Hash + Clone,
{
    let mut index: HashMap<K, usize> = HashMap::new();
    let mut groups: Vec<(K, Vec<V>)> = Vec::new();
    for (key, values) in items {
        let at = *index.entry(key.clone()).or_insert_with(|| {
            groups.push((key, Vec::new()));
            groups.len() - 1
        });
        groups[at].1.extend(values);
    }
    groups
}

/// Republishes [`Membership::peers`] changes as [`Mesh::update_peers`] calls.
async fn membership_to_mesh_task(
    mut peers: watch::Receiver<Vec<Peer>>,
    mesh: Mesh,
    cancel: CancellationToken,
) {
    let initial = peers.borrow_and_update().clone();
    set_live_peers_gauge(initial.len());
    mesh.update_peers(initial);
    loop {
        let Some(changed) = cancel.run_until_cancelled(peers.changed()).await else {
            return;
        };
        if changed.is_err() {
            return; // membership shut down
        }
        let current = peers.borrow_and_update().clone();
        tracing::info!(peer_count = current.len(), "membership view changed");
        set_live_peers_gauge(current.len());
        mesh.update_peers(current);
    }
}

/// The cache-mode-mismatch late-detection sweep. Two nodes opening the same
/// name concurrently under different `Mode`s can both pass `open()`'s
/// best-effort check, so this re-checks every membership view instead.
async fn mode_conflict_task(cluster: Cluster, cancel: CancellationToken) {
    let mut modes = cluster.inner.membership.cache_modes();
    let mut warned: HashSet<(NodeId, SmolStr)> = HashSet::new();
    report_mode_conflicts(&modes.borrow_and_update(), &cluster, &mut warned);
    loop {
        let Some(changed) = cancel.run_until_cancelled(modes.changed()).await else {
            return;
        };
        if changed.is_err() {
            return;
        }
        let current = modes.borrow_and_update().clone();
        report_mode_conflicts(&current, &cluster, &mut warned);
    }
}

/// One (peer, cache) pair where a live peer advertises a different [`Mode`]
/// than this node's own.
#[derive(Debug, PartialEq, Eq)]
struct ModeConflict {
    peer: NodeId,
    cache: SmolStr,
    local: Mode,
    remote: Mode,
}

/// The conflicts in `advertised` against `local_modes` that `warned` has not
/// seen yet. Each is recorded in `warned`, so a mismatch is reported once
/// and clears only via a re-`open()` under a fresh [`NodeId`].
fn new_mode_conflicts(
    advertised: &CacheModes,
    local_modes: &HashMap<SmolStr, Mode>,
    warned: &mut HashSet<(NodeId, SmolStr)>,
) -> Vec<ModeConflict> {
    let mut found = Vec::new();
    for (&peer, caches) in advertised {
        for (cache, &remote) in caches {
            let Some(&local) = local_modes.get(cache) else {
                continue;
            };
            if local != remote && warned.insert((peer, cache.clone())) {
                found.push(ModeConflict {
                    peer,
                    cache: cache.clone(),
                    local,
                    remote,
                });
            }
        }
    }
    found
}

/// Logs a `tracing::error!` for every conflict [`new_mode_conflicts`] finds.
fn report_mode_conflicts(
    advertised: &CacheModes,
    cluster: &Cluster,
    warned: &mut HashSet<(NodeId, SmolStr)>,
) {
    let local_modes = cluster.inner.local_modes.read_modes();
    for ModeConflict {
        peer,
        cache,
        local,
        remote,
    } in new_mode_conflicts(advertised, &local_modes, warned)
    {
        tracing::error!(
            %cache,
            %peer,
            local = ?local,
            remote = ?remote,
            "cache mode mismatch detected against a live peer"
        );
    }
}

fn set_live_peers_gauge(count: usize) {
    metrics::gauge!("sundog_live_peers").set(f64::from(u32::try_from(count).unwrap_or(u32::MAX)));
}

/// Periodically republishes the count of open caches as the
/// `sundog_open_caches` gauge. A gauge, not an event-driven update, since
/// [`crate::cache::CacheBuilder::open`] has no hook back into telemetry.
async fn open_cache_gauge_task(shards: ShardRegistry, cancel: CancellationToken) {
    let mut ticker = tokio::time::interval(Duration::from_secs(5));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        if cancel.run_until_cancelled(ticker.tick()).await.is_none() {
            return;
        }
        let count = shards.read_shards().len();
        metrics::gauge!("sundog_open_caches")
            .set(f64::from(u32::try_from(count).unwrap_or(u32::MAX)));
    }
}

/// The wait from `now_ms`, in epoch milliseconds, to the next multiple of
/// `period` on the wall clock: the next instant of the grid every node
/// shares, strictly after `now_ms`, so a tick that lands on the grid waits a
/// whole period for the next. A zero `period` counts as one millisecond.
/// Pure; unit tested directly.
fn until_next_grid_tick(now_ms: u64, period: Duration) -> Duration {
    let period_ms = u64::try_from(period.as_millis()).unwrap_or(u64::MAX).max(1);
    Duration::from_millis(period_ms - now_ms % period_ms)
}

/// Whether `tombstone_gc_task` defers tombstone collection this tick, given
/// `absent_defers`, what [`absence::should_defer_gc`] says of the members
/// absent or only just back. A cache in a mode that defers at all
/// ([`Mode::defers_tombstone_gc`]) also defers while it has run for less than
/// `settle`, the `tombstone_ttl`, whoever is absent: a node that just opened
/// may hold a copy from before it went down, or have no memory of which
/// members were absent, and a tombstone a peer has held past its `ttl` still
/// has to reach it. `uptime` counts from when the gc task started. `Local` and
/// `Invalidation` never defer. The shard adds one more reason of its own,
/// which this task does not decide: a pause, a gap between its collections of
/// more than twice the period [`tombstone_gc_task`] ticks on, which it
/// measures on the wall clock and holds for `settle` after the gap. Pure; unit
/// tested directly.
fn gc_defers(mode: Mode, absent_defers: bool, uptime: Duration, settle: Duration) -> bool {
    mode.defers_tombstone_gc() && (absent_defers || uptime < settle)
}

/// Periodically garbage-collects one shard's expired tombstones and flushes
/// pending housekeeping via `ShardOps::run_pending_tasks`, once on start and
/// then at every multiple of a quarter of `tombstone_ttl` on the wall clock,
/// so a tombstone is never held much past its deadline. The ticks sit on a
/// grid every node shares ([`until_next_grid_tick`]), not on a phase counted
/// from when the node started. Every tombstone counts its deadlines from an
/// instant all replicas agree on, the expiry for the tombstone of an expired
/// entry and the version stamp for a delete, so replicas collect it at the
/// same tick, up to the clock skew between them. One that a replica has
/// collected and another still holds, which anti-entropy offers again each
/// round, the collector drops on arrival, so it lasts as long as that skew,
/// not up to a whole period.
/// `mode` and `absence` decide each tick whether collection past
/// `tombstone_ttl` defers via [`gc_defers`]: a `Mode::Replicated` delete
/// survives up to `tombstone_max_ttl` while any recently known member is
/// absent ([`absence::should_defer_gc`]), and for another `tombstone_ttl`
/// once it is back; `Mode::Distributed` narrows that to a member that
/// co-owns a part with this node when it is eligible again. Either mode also
/// defers for the first `tombstone_ttl` after the task starts. The shard
/// itself starts deferring when it is built, so the records that arrive
/// before the first tick, state transfer among them, meet the same rule. The
/// shard also defers on its own for `tombstone_ttl` after a pause, a wall-clock
/// gap of more than twice this period between two ticks, and judges arrivals
/// against that gap before the task wakes; the task only has to tick on
/// [`gc_period_ms`], which the gap is measured against. The tombstone of an
/// expired entry has its cap at its ttl deadline, so no deferral keeps it.
/// `now_ms` reads the wall clock in epoch milliseconds that places the grid;
/// the cache's tasks pass [`crate::store::now_ms`]. The uptime comes from
/// the tokio clock.
pub(crate) async fn tombstone_gc_task(
    shard: Arc<dyn ShardOps>,
    mode: Mode,
    tombstone_ttl: Duration,
    tombstone_max_ttl: Duration,
    absence: absence::AbsenceTracker,
    now_ms: impl Fn() -> u64,
    cancel: CancellationToken,
) {
    let period = Duration::from_millis(gc_period_ms(
        u64::try_from(tombstone_ttl.as_millis()).unwrap_or(u64::MAX),
    ));
    let started = tokio::time::Instant::now();
    loop {
        if cancel.is_cancelled() {
            return;
        }
        shard.run_pending_tasks().await;
        let view = shard.ownership_view();
        let absent_defers = absence::should_defer_gc(
            mode,
            &absence,
            tombstone_max_ttl,
            tombstone_ttl,
            view.as_deref(),
        );
        let defer = gc_defers(mode, absent_defers, started.elapsed(), tombstone_ttl);
        shard.gc_tombstones(defer).await;
        let wait = until_next_grid_tick(now_ms(), period);
        if cancel
            .run_until_cancelled(tokio::time::sleep(wait))
            .await
            .is_none()
        {
            return;
        }
    }
}

/// Publishes `sundog_cache_entries{cache}` and `sundog_tombstones{cache}` for
/// one opened cache, and `sundog_cache_bytes{cache}` for one with a memory
/// ceiling: set immediately, then refreshed every 5 seconds from
/// [`Shard::entry_count`], [`Shard::tombstone_count`] and
/// [`Shard::resident_bytes`] for as long as the cache stays open. The counts
/// are only advisory until pending housekeeping flushes.
pub(crate) async fn cache_entries_gauge_task<K, V>(
    shard: Arc<Shard<K, V>>,
    name: SmolStr,
    cancel: CancellationToken,
) where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let gauge = metrics::gauge!("sundog_cache_entries", "cache" => name.to_string());
    let tombstone_gauge = metrics::gauge!("sundog_tombstones", "cache" => name.to_string());
    let bytes_gauge = shard
        .resident_bytes()
        .is_some()
        .then(|| metrics::gauge!("sundog_cache_bytes", "cache" => name.to_string()));
    let publish_bytes = || {
        if let (Some(gauge), Some(bytes)) = (&bytes_gauge, shard.resident_bytes()) {
            gauge.set(resident_bytes_f64(bytes));
        }
    };
    // The sweep inside `entry_count` runs first, so the tombstones it leaves
    // are counted in the same pass.
    gauge.set(entry_count_f64(shard.entry_count().await));
    tombstone_gauge.set(entry_count_f64(shard.tombstone_count()));
    publish_bytes();

    let mut ticker = tokio::time::interval(Duration::from_secs(5));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        if cancel.run_until_cancelled(ticker.tick()).await.is_none() {
            return;
        }
        gauge.set(entry_count_f64(shard.entry_count().await));
        tombstone_gauge.set(entry_count_f64(shard.tombstone_count()));
        publish_bytes();
    }
}

/// Saturating `u64` -> `f64` conversion for gauge values.
fn entry_count_f64(count: u64) -> f64 {
    f64::from(u32::try_from(count).unwrap_or(u32::MAX))
}

#[expect(
    clippy::cast_precision_loss,
    reason = "a gauge only needs f64's exact-integer range, up to 2^53 bytes, \
              far past any node's memory"
)]
fn resident_bytes_f64(bytes: u64) -> f64 {
    bytes as f64
}

// Real-transport-only: these build a live `Cluster` with a real `Mesh` and
// sockets, which panics under `sim` outside a driven `turmoil::Sim`.
#[cfg(all(test, not(feature = "sim")))]
mod tests;

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
use std::time::Duration;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::{self, BoxStream, StreamExt as _};
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
use crate::explain::ServeVerdict;
use crate::membership::{CacheModes, Membership, Peer};
use crate::net::{
    AeServeOutcome, BoundListener, FetchServe, Greeting, InboundMsg, LoadServe, Mesh, OutFrame,
    RequestHandler, StServe, batch_forward,
};
use crate::node::{NodeId, NodeName};
use crate::ownership::{Granularity, OwnershipView, parts_of_wire_id};
use crate::store::{
    BucketDigest, BucketLen, BucketPart, BucketPartDigests, KeyVersion, Mode, PartId, PartMask,
    Shard, ShardOps, chunk_records_for_snapshot,
};
use crate::verdict;
use crate::wire::{self, Msg, WireRecord};

/// The cluster's type-erased cache registry: `cache name -> Arc<dyn ShardOps>`.
/// Shared with the [`RequestHandler`] passed to [`Mesh::spawn`].
type ShardRegistry = Arc<RwLock<HashMap<SmolStr, Arc<dyn ShardOps>>>>;

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
    /// `cache::should_await_first_peer`.
    has_seeds: bool,
    mesh: Mesh,
    shards: ShardRegistry,
    /// The [`Mode`] each open cache opens under; [`mode_conflict_task`]'s
    /// local half, shared with the `prometheus` feature's `/readyz` route.
    local_modes: Arc<RwLock<HashMap<SmolStr, Mode>>>,
    config: ClusterConfig,
    absence: absence::AbsenceTracker,
    /// When each peer last streamed replicate traffic in; see
    /// [`Cluster::peer_is_streaming`].
    inbound_activity: Arc<InboundActivity>,
    /// Shared with the request handler, which consults it before donating.
    warmth: Arc<Warmth>,
    tracker: TaskTracker,
    cancel: CancellationToken,
}

/// Per-peer stamps of the last inbound replicate traffic, recorded by
/// [`inbound_loop`] so the anti-entropy scheduler can leave a streaming peer
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
    /// either advertised.
    /// Idempotent: closing an already-forgotten name changes nothing.
    pub(crate) fn forget_cache(&self, name: &str) {
        self.inner.shards.write_shards().remove(name);
        self.inner.local_modes.write_modes().remove(name);
        self.inner.warmth.forget(name);
        self.inner.membership.clear_cache_mode(name);
        self.inner.membership.clear_loader(name);
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

    /// Leaves the cluster gracefully: background loops are cancelled and
    /// joined, every still-registered cache has its spill tier closed (see
    /// [`ShardOps::close_spill_checkpointed`], a no-op if already closed), then chitchat
    /// departs and the data plane closes its connections. No further calls
    /// on any clone of this handle; cache handles opened before this call
    /// keep working for local reads/writes.
    ///
    /// # Panics
    ///
    /// Panics if the shard registry lock is poisoned.
    pub async fn shutdown(self) {
        // Every cache's fan-out queue is sealed before any task is
        // cancelled: a write accepted until now stays in the backlog its
        // fan-out task drains on the way out, and a write from now on
        // fails with `CacheError::Closed` instead of being accepted and
        // never sent.
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
    /// them co-owners, so every part is in anti-entropy scope. Up to
    /// [`LEAVE_HAND_OFF_ROUNDS`] rounds per co-owner, all within
    /// `state_transfer_budget`.
    async fn hand_off_before_leaving(&self) {
        let deadline = tokio::time::Instant::now() + self.config().state_transfer_budget;
        let live: HashSet<NodeId> = self.live_peer_ids().into_iter().collect();
        let shards: Vec<(SmolStr, Arc<dyn ShardOps>)> = self
            .inner
            .shards
            .read_shards()
            .iter()
            .map(|(name, shard)| (name.clone(), Arc::clone(shard)))
            .collect();
        for (name, shard) in shards {
            let Some(view) = shard.ownership_view() else {
                continue;
            };
            for (peer, parts) in hand_off_groups(&view, self.node_id(), &live) {
                let mut diverging = parts;
                for _ in 0..LEAVE_HAND_OFF_ROUNDS {
                    let Ok(outcome) = tokio::time::timeout_at(
                        deadline,
                        anti_entropy::run_round_for_parts(
                            self.mesh(),
                            &shard,
                            &name,
                            peer,
                            &diverging,
                        ),
                    )
                    .await
                    else {
                        tracing::warn!(cache = %name, %peer, "leave hand-off ran out of state_transfer_budget");
                        return;
                    };
                    if outcome.failed {
                        break;
                    }
                    diverging.retain(|part| !outcome.matched.contains(part));
                    if diverging.is_empty() {
                        break;
                    }
                }
                tracing::debug!(cache = %name, %peer, diverging = diverging.len(), "leave hand-off with co-owner done");
            }
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

/// Anti-entropy rounds [`Cluster::hand_off_before_leaving`] runs against
/// each co-owner.
const LEAVE_HAND_OFF_ROUNDS: u32 = 3;

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

    /// Overrides the default [`ClusterConfig`].
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
    /// `metrics`'s recorder is a single process-global slot: a second call to
    /// this, or mixing it with [`crate::telemetry::prometheus_handle`], fails
    /// `build()` with [`JoinError::Bind`] rather than panicking.
    #[cfg(feature = "prometheus")]
    pub fn prometheus_listen(mut self, addr: SocketAddr) -> Self {
        self.prometheus_listen = Some(addr);
        self
    }

    /// Enables mutual TLS on the mesh; equivalent to setting
    /// [`ClusterConfig::tls`] via [`Self::config`].
    #[cfg(feature = "tls")]
    pub fn tls(mut self, tls: crate::config::TlsConfig) -> Self {
        self.config.tls = Some(tls);
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
        let Self {
            name,
            discovery,
            config,
            node_id,
            #[cfg(feature = "prometheus")]
            prometheus_listen,
        } = self;

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

/// Answers inbound state-transfer/anti-entropy requests over this cluster's
/// shard registry, for whichever cache a peer names. An unregistered cache
/// name degrades to an empty result rather than an error.
struct ClusterRequestHandler {
    shards: ShardRegistry,
    warmth: Arc<Warmth>,
    ae_part_min_bucket: usize,
    ae_sketch_min_bucket: usize,
    ae_sketch_cells: usize,
    /// [`crate::config::ClusterConfig::rebalance_chunk_bytes_value`],
    /// resolved once at construction.
    rebalance_chunk_bytes: u64,
}

impl ClusterRequestHandler {
    fn lookup(&self, cache: &SmolStr) -> Option<Arc<dyn ShardOps>> {
        self.shards.read_shards().get(cache).cloned()
    }

    /// `cache`'s shard when its view's hash is `view_hash`, otherwise the
    /// decline a scoped anti-entropy round answers with.
    fn matching_view<T>(
        &self,
        cache: &SmolStr,
        view_hash: u64,
    ) -> Result<Arc<dyn ShardOps>, AeServeOutcome<T>> {
        let shard = self.lookup(cache).ok_or(AeServeOutcome::Unavailable)?;
        let view = shard.ownership_view().ok_or(AeServeOutcome::Unavailable)?;
        if view.view_hash() == view_hash {
            Ok(shard)
        } else {
            Err(AeServeOutcome::Stale {
                responder_view_hash: view.view_hash(),
            })
        }
    }
}

/// The reply [`ClusterRequestHandler::fetch`] sends for `verdict`: `rec`,
/// the record it holds for the key, when serving it, and `local_hash`, its
/// own view hash, when the asker's view is stale. Pure; unit tested
/// directly.
fn serve_reply(verdict: ServeVerdict, rec: Option<WireRecord>, local_hash: u64) -> FetchServe {
    match verdict {
        ServeVerdict::Serve => FetchServe::Found(rec),
        ServeVerdict::Miss => FetchServe::Found(None),
        ServeVerdict::Stale => FetchServe::Stale {
            responder_view_hash: local_hash,
        },
        ServeVerdict::DeclineDistrusted | ServeVerdict::DeclineCold => FetchServe::Unavailable,
    }
}

impl RequestHandler for ClusterRequestHandler {
    fn snapshot_available(&self, cache: SmolStr) -> bool {
        self.warmth.is_warm(&cache)
    }

    fn snapshot_chunks(&self, cache: SmolStr) -> BoxStream<'static, Vec<WireRecord>> {
        match self.lookup(&cache) {
            Some(shard) => shard.snapshot_chunks(),
            None => stream::empty().boxed(),
        }
    }

    fn digests(&self, cache: SmolStr) -> BoxFuture<'_, Vec<BucketDigest>> {
        Box::pin(async move {
            match self.lookup(&cache) {
                Some(shard) => shard.digests().await,
                None => Vec::new(),
            }
        })
    }

    fn bucket_entries(&self, cache: SmolStr, bucket: u16) -> BoxFuture<'_, Vec<KeyVersion>> {
        Box::pin(async move {
            match self.lookup(&cache) {
                Some(shard) => shard.bucket_entries(bucket).await,
                None => Vec::new(),
            }
        })
    }

    fn entries_for_buckets(
        &self,
        cache: SmolStr,
        buckets: Vec<u16>,
    ) -> BoxFuture<'_, crate::store::BucketEntries> {
        Box::pin(async move {
            match self.lookup(&cache) {
                Some(shard) => shard.entries_for_buckets(buckets).await,
                None => Vec::new(),
            }
        })
    }

    fn records_for(&self, cache: SmolStr, keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>> {
        Box::pin(async move {
            match self.lookup(&cache) {
                Some(shard) => shard.records_for(keys).await,
                None => Vec::new(),
            }
        })
    }

    fn bucket_lens(&self, cache: SmolStr, buckets: Vec<u16>) -> BoxFuture<'_, Vec<BucketLen>> {
        Box::pin(async move {
            match self.lookup(&cache) {
                Some(shard) => shard.bucket_lens(buckets).await,
                None => Vec::new(),
            }
        })
    }

    fn part_digests(
        &self,
        cache: SmolStr,
        buckets: Vec<u16>,
    ) -> BoxFuture<'_, Vec<BucketPartDigests>> {
        Box::pin(async move {
            match self.lookup(&cache) {
                Some(shard) => shard.part_digests(buckets).await,
                None => Vec::new(),
            }
        })
    }

    fn entries_for_parts(
        &self,
        cache: SmolStr,
        parts: Vec<BucketPart>,
    ) -> BoxFuture<'_, crate::store::PartEntries> {
        Box::pin(async move {
            match self.lookup(&cache) {
                Some(shard) => shard.entries_for_parts(parts).await,
                None => Vec::new(),
            }
        })
    }

    fn ae_part_min_bucket(&self) -> usize {
        self.ae_part_min_bucket
    }

    fn ae_sketch_min_bucket(&self) -> usize {
        self.ae_sketch_min_bucket
    }

    fn ae_sketch_cells(&self) -> usize {
        self.ae_sketch_cells
    }

    // Distribution-mode serving: every method here answers `Unavailable`
    // (or its shape's equivalent) for a cache that isn't registered, or one
    // registered but not a `Mode::Distributed` cache (`ownership_view_hash`
    // is `None` either way). Same "unknown cache degrades gracefully"
    // convention every other method on this trait already has.
    fn ownership_view_hash(&self, cache: SmolStr) -> Option<u64> {
        self.lookup(&cache)
            .and_then(|shard| shard.ownership_view_hash())
    }

    fn fetch(&self, cache: SmolStr, key: Bytes, view_hash: u64) -> BoxFuture<'_, FetchServe> {
        Box::pin(async move {
            let Some(shard) = self.lookup(&cache) else {
                return FetchServe::Unavailable;
            };
            let Some(local_hash) = shard.ownership_view_hash() else {
                return FetchServe::Unavailable;
            };
            let part = PartId::of_key(&key);
            // Both marks are read before the record, so a pull landing
            // during the read cannot clear a cold mark this answer then
            // trusts. A distrusted part reads nothing: it declines even a
            // hit.
            let distrusted = shard.is_unverified_part(part);
            let cold = shard.is_cold_part(part);
            let rec = if distrusted {
                None
            } else {
                shard.records_for(vec![key]).await.into_iter().next()
            };
            let verdict = verdict::fetch_serve_verdict(verdict::ServeInputs {
                distrusted,
                served: rec.is_some(),
                views_match: local_hash == view_hash,
                cold,
            });
            serve_reply(verdict, rec, local_hash)
        })
    }

    fn load(&self, cache: SmolStr, keys: Vec<Bytes>) -> BoxFuture<'_, LoadServe> {
        Box::pin(async move {
            match self.lookup(&cache) {
                Some(shard) => shard.serve_load(keys).await,
                None => LoadServe::Unavailable,
            }
        })
    }

    fn ae_digest_scoped(
        &self,
        cache: SmolStr,
        view_hash: u64,
        _buckets: Vec<BucketDigest>,
    ) -> BoxFuture<'_, AeServeOutcome<Vec<BucketDigest>>> {
        // The requester's buckets only matter once its epoch is confirmed
        // current; `conn::serve_ae_digest_scoped` classifies them.
        Box::pin(async move {
            match self.matching_view(&cache, view_hash) {
                Ok(shard) => AeServeOutcome::Digests(shard.digests().await),
                Err(declined) => declined,
            }
        })
    }

    fn ae_digest_masked(
        &self,
        cache: SmolStr,
        view_hash: u64,
        buckets: Vec<(u16, u64, u64)>,
    ) -> BoxFuture<'_, AeServeOutcome<Vec<BucketPartDigests>>> {
        Box::pin(async move {
            let shard = match self.matching_view(&cache, view_hash) {
                Ok(shard) => shard,
                Err(declined) => return declined,
            };
            let remote: HashMap<u16, (PartMask, u64)> = buckets
                .into_iter()
                .map(|(bucket, mask, digest)| (bucket, (PartMask::from_bits(mask), digest)))
                .collect();
            let local = shard.part_digests(remote.keys().copied().collect()).await;
            AeServeOutcome::Digests(
                local
                    .into_iter()
                    .filter(|bpd| {
                        remote
                            .get(&bpd.bucket)
                            .is_some_and(|&(mask, digest)| mask.fold(&bpd.digests) != digest)
                    })
                    .collect(),
            )
        })
    }

    fn st_serve(&self, cache: SmolStr, ids: Vec<u16>, view_hash: u64) -> BoxFuture<'_, StServe> {
        Box::pin(async move {
            let serving = self
                .lookup(&cache)
                .and_then(|shard| shard.ownership_view().map(|view| (shard, view)));
            let Some((shard, view)) = serving else {
                return StServe::Stale {
                    responder_view_hash: 0,
                };
            };
            if view.view_hash() != view_hash {
                return StServe::Stale {
                    responder_view_hash: view.view_hash(),
                };
            }
            let granularity = view.granularity();
            let order = serve_order(granularity, ids);
            let parts = || {
                order
                    .iter()
                    .flat_map(|&id| parts_of_wire_id(granularity, id))
            };
            if parts().any(|part| shard.is_cold_part(part)) {
                return StServe::Cold;
            }
            let parts = parts().count() as u64;
            StServe::Stream {
                chunks: id_chunks(shard, granularity, &order, self.rebalance_chunk_bytes),
                order,
                parts,
            }
        })
    }

    fn st_buckets_available(&self, cache: SmolStr, view_hash: u64) -> BoxFuture<'_, bool> {
        Box::pin(async move {
            self.lookup(&cache)
                .and_then(|shard| shard.ownership_view_hash())
                .is_some_and(|local_hash| local_hash == view_hash)
        })
    }

    fn st_buckets_cold(&self, cache: SmolStr, buckets: Vec<u16>) -> BoxFuture<'_, bool> {
        Box::pin(async move {
            self.lookup(&cache).is_some_and(|shard| {
                let granularity = current_granularity(shard.as_ref());
                buckets
                    .iter()
                    .flat_map(|&id| parts_of_wire_id(granularity, id))
                    .any(|part| shard.is_cold_part(part))
            })
        })
    }

    fn st_bucket_chunks(
        &self,
        cache: SmolStr,
        buckets: Vec<u16>,
    ) -> BoxStream<'static, (u16, Vec<WireRecord>)> {
        let Some(shard) = self.lookup(&cache) else {
            return stream::empty().boxed();
        };
        let granularity = current_granularity(shard.as_ref());
        let order = serve_order(granularity, buckets);
        id_chunks(shard, granularity, &order, self.rebalance_chunk_bytes)
    }
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

/// The order a donor streams `ids` in: bucket by bucket, each bucket's parts
/// together, so one entries read per bucket serves every requested part of
/// it. Bucket ids already are that order once sorted.
fn serve_order(granularity: Granularity, mut ids: Vec<u16>) -> Vec<u16> {
    match granularity {
        Granularity::Bucket => ids.sort_unstable(),
        Granularity::Part => ids.sort_unstable_by_key(|&id| {
            let part = PartId::from_raw(id);
            (part.bucket(), part.part())
        }),
    }
    ids.dedup();
    ids
}

/// The granularity `shard`'s current view reads wire ids at: `Bucket` for a
/// shard with no view.
fn current_granularity(shard: &dyn ShardOps) -> Granularity {
    shard
        .ownership_view()
        .map_or(Granularity::Bucket, |view| view.granularity())
}

/// Splits `ids`, already in [`serve_order`], into one run per bucket: a
/// bucket id alone, or every requested part of one bucket.
fn bucket_runs(granularity: Granularity, ids: &[u16]) -> impl Iterator<Item = &[u16]> {
    ids.chunk_by(move |&a, &b| {
        granularity == Granularity::Part
            && PartId::from_raw(a).bucket() == PartId::from_raw(b).bucket()
    })
}

/// Streams the records of `ids`, in [`serve_order`], as frames tagged with
/// their id: one entries read per bucket run, then `chunk_bytes`-sized key
/// batches read one at a time, each split on the frame cap. An id holding
/// nothing yields no frame.
fn id_chunks(
    shard: Arc<dyn ShardOps>,
    granularity: Granularity,
    ids: &[u16],
    chunk_bytes: u64,
) -> BoxStream<'static, (u16, Vec<WireRecord>)> {
    let runs: Vec<Vec<u16>> = bucket_runs(granularity, ids).map(<[u16]>::to_vec).collect();
    let keys_shard = Arc::clone(&shard);
    stream::iter(runs)
        .then(move |run| id_keys(Arc::clone(&keys_shard), granularity, run))
        .flat_map(move |keyed| {
            stream::iter(keyed.into_iter().flat_map(move |(id, keys)| {
                st_bucket_key_sub_batches(keys, chunk_bytes)
                    .into_iter()
                    .map(move |batch| (id, batch))
            }))
        })
        .then(move |(id, batch)| {
            let shard = Arc::clone(&shard);
            async move { (id, shard.records_for(batch).await) }
        })
        .flat_map(|(id, recs)| {
            stream::iter(
                chunk_records_for_snapshot(recs)
                    .into_iter()
                    .map(move |chunk| (id, chunk)),
            )
        })
        .boxed()
}

/// The keys each id of one bucket run holds, in one entries read; an id
/// holding nothing is left out.
async fn id_keys(
    shard: Arc<dyn ShardOps>,
    granularity: Granularity,
    run: Vec<u16>,
) -> Vec<(u16, Vec<Bytes>)> {
    let listed: Vec<(u16, Vec<KeyVersion>)> = match granularity {
        Granularity::Bucket => shard.entries_for_buckets(run).await,
        Granularity::Part => shard
            .entries_for_parts(
                run.iter()
                    .map(|&id| PartId::from_raw(id).bucket_part())
                    .collect(),
            )
            .await
            .into_iter()
            .map(|(bp, entries)| (PartId::from(bp).raw(), entries))
            .collect(),
    };
    listed
        .into_iter()
        .filter(|(_, entries)| !entries.is_empty())
        .map(|(id, entries)| (id, entries.into_iter().map(|kv| kv.key).collect()))
        .collect()
}

fn lookup_shard(shards: &ShardRegistry, cache: &SmolStr) -> Option<Arc<dyn ShardOps>> {
    shards.read_shards().get(cache).cloned()
}

/// Splits one bucket's `keys` into groups whose estimated encoded size
/// (`wire::RECORD_HEADER_LEN + key.len()` per key, the only size known
/// before `records_for` reads each value) is `<= chunk_bytes`. A key whose
/// own estimate already meets or exceeds `chunk_bytes` still gets a
/// one-key group of its own rather than being dropped.
fn st_bucket_key_sub_batches(keys: Vec<Bytes>, chunk_bytes: u64) -> Vec<Vec<Bytes>> {
    let mut sub_batches = Vec::new();
    let mut current = Vec::new();
    let mut current_size: u64 = 0;
    for key in keys {
        let key_size = (wire::RECORD_HEADER_LEN + key.len()) as u64;
        let over_budget = current_size.saturating_add(key_size) > chunk_bytes;
        if over_budget && !current.is_empty() {
            sub_batches.push(std::mem::take(&mut current));
            current_size = 0;
        }
        current_size += key_size;
        current.push(key);
    }
    if !current.is_empty() {
        sub_batches.push(current);
    }
    sub_batches
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

/// Cap on how many queued inbound messages [`inbound_loop`] drains per wake.
const INBOUND_DRAIN_CAP: usize = 1024;

/// Applies one accumulated same-cache run of `Replicate`/`ReplicateBatch`
/// records under one `apply_remote_batch` call. `shard_cache` memoizes the
/// lookup per drained batch, so a cache name costs at most one lock
/// acquisition.
async fn apply_pending_replicate(
    shards: &ShardRegistry,
    shard_cache: &mut HashMap<SmolStr, Option<Arc<dyn ShardOps>>>,
    cache: SmolStr,
    recs: Vec<WireRecord>,
) {
    let shard = shard_cache
        .entry(cache.clone())
        .or_insert_with(|| lookup_shard(shards, &cache));
    if let Some(shard) = shard {
        shard.apply_remote_batch(recs).await;
    } else {
        tracing::trace!(%cache, "replicate batch for unknown cache; dropped");
    }
}

/// Appends `recs` to the pending same-cache run, or applies the run in
/// progress first when `cache` differs and starts a new one.
async fn queue_replicate(
    shards: &ShardRegistry,
    shard_cache: &mut HashMap<SmolStr, Option<Arc<dyn ShardOps>>>,
    pending: &mut Option<(SmolStr, Vec<WireRecord>)>,
    cache: SmolStr,
    mut recs: Vec<WireRecord>,
) {
    match pending {
        Some((pending_cache, pending_recs)) if *pending_cache == cache => {
            pending_recs.append(&mut recs);
        }
        _ => {
            if let Some((old_cache, old_recs)) = pending.take() {
                apply_pending_replicate(shards, shard_cache, old_cache, old_recs).await;
            }
            *pending = Some((cache, recs));
        }
    }
}

/// Notes every peer with a record-carrying message in `drained` as
/// streaming toward this node, once per peer per drain.
fn note_streaming_peers(activity: &InboundActivity, drained: &[InboundMsg]) {
    let mut streaming_from: HashSet<NodeId> = HashSet::new();
    for InboundMsg { from, msg } in drained {
        if matches!(
            msg,
            Msg::Replicate { .. } | Msg::ReplicateBatch { .. } | Msg::ForwardBatch { .. }
        ) {
            streaming_from.insert(*from);
        }
    }
    for from in streaming_from {
        activity.note(from);
    }
}

/// The single consumer of `Mesh`'s inbound-message channel: dispatches
/// `Invalidate`/`Replicate`/`ReplicateBatch`/`ForwardBatch` to the named
/// shard, dropping a message for an unregistered cache with a trace event.
/// Drains a bounded batch per wake with `recv_many`, so a run of same-cache
/// messages coalesces into one `apply_remote_batch` call; a `ForwardBatch`
/// under a stale view hash is first re-forwarded to its records' current
/// owners (see [`reforward_stale_view`]), then applied like any other.
/// The shard `cache` names, looked up in the registry once per drained
/// burst and remembered in `shard_cache`; `None` for a cache not open here.
fn burst_shard<'a>(
    shard_cache: &'a mut HashMap<SmolStr, Option<Arc<dyn ShardOps>>>,
    shards: &ShardRegistry,
    cache: &SmolStr,
) -> Option<&'a Arc<dyn ShardOps>> {
    shard_cache
        .entry(cache.clone())
        .or_insert_with(|| lookup_shard(shards, cache))
        .as_ref()
}

async fn inbound_loop(
    shards: ShardRegistry,
    activity: Arc<InboundActivity>,
    mesh: Mesh,
    self_node: NodeId,
    mut inbound: mpsc::Receiver<InboundMsg>,
    cancel: CancellationToken,
) {
    let mut drained: Vec<InboundMsg> = Vec::with_capacity(INBOUND_DRAIN_CAP);
    loop {
        drained.clear();
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            received = inbound.recv_many(&mut drained, INBOUND_DRAIN_CAP) => {
                if received == 0 {
                    return; // channel closed, nothing left to drain
                }
            }
        }

        note_streaming_peers(&activity, &drained);

        let mut shard_cache: HashMap<SmolStr, Option<Arc<dyn ShardOps>>> = HashMap::new();
        let mut pending: Option<(SmolStr, Vec<WireRecord>)> = None;
        for InboundMsg { from, msg } in drained.drain(..) {
            match msg {
                Msg::Replicate { cache, rec } => {
                    queue_replicate(&shards, &mut shard_cache, &mut pending, cache, vec![rec])
                        .await;
                }
                Msg::ReplicateBatch { cache, recs } => {
                    queue_replicate(&shards, &mut shard_cache, &mut pending, cache, recs).await;
                }
                Msg::ForwardBatch {
                    cache,
                    view_hash,
                    hops,
                    recs,
                } => {
                    if let Some(shard) = burst_shard(&mut shard_cache, &shards, &cache) {
                        reforward_stale_view(
                            &mesh,
                            ReforwardBatch {
                                self_node,
                                from,
                                shard: shard.as_ref(),
                                cache_name: &cache,
                                sender_view_hash: view_hash,
                                hops,
                                records: &recs,
                            },
                        )
                        .await;
                    }
                    queue_replicate(&shards, &mut shard_cache, &mut pending, cache, recs).await;
                }
                Msg::Invalidate { cache, key, ver } => {
                    if let Some((old_cache, old_recs)) = pending.take() {
                        apply_pending_replicate(&shards, &mut shard_cache, old_cache, old_recs)
                            .await;
                    }
                    let Some(shard) = burst_shard(&mut shard_cache, &shards, &cache) else {
                        tracing::trace!(%cache, %from, "invalidate for unknown cache; dropped");
                        continue;
                    };
                    shard.invalidate(key, ver).await;
                }
                Msg::RefreshHint { cache, key, ver } => {
                    if let Some(shard) = burst_shard(&mut shard_cache, &shards, &cache) {
                        shard.refresh_hint(key, ver).await;
                    }
                }
                // `Hello` and the request/response messages never reach this channel.
                Msg::Hello { .. }
                | Msg::StRequest { .. }
                | Msg::StChunk { .. }
                | Msg::AeDigest { .. }
                | Msg::AeBucket { .. }
                | Msg::AeSketch { .. }
                | Msg::AeEntries { .. }
                | Msg::AePull { .. }
                | Msg::AePullHashes { .. }
                | Msg::AePartDigests { .. }
                | Msg::AeParts { .. }
                | Msg::AePart { .. }
                | Msg::AePartSketch { .. }
                | Msg::StUnavailable { .. }
                | Msg::Fetch { .. }
                | Msg::FetchReply { .. }
                | Msg::FetchDeclined { .. }
                | Msg::AeDigestScoped { .. }
                | Msg::AeDigestMasked { .. }
                | Msg::StBuckets { .. }
                | Msg::StBucketChunk { .. }
                | Msg::StaleView { .. }
                | Msg::StBucketDone { .. }
                | Msg::StBucketAck { .. }
                | Msg::Load { .. }
                | Msg::Loaded { .. }
                | Msg::LoadFailed { .. }
                | Msg::LoadDeclined { .. }
                | Msg::ReqDone => {}
            }
        }
        if let Some((cache, recs)) = pending.take() {
            apply_pending_replicate(&shards, &mut shard_cache, cache, recs).await;
        }
    }
}

/// The most hops a `Mode::Distributed` write travels: one from the writer
/// to the owners under its view, and at most one more from a receiver
/// whose view disagrees to the owners under its own. A receiver never
/// re-forwards a batch already at this count, so two disagreeing views
/// cost one extra hop and never a loop.
const MAX_FORWARD_HOPS: u8 = 1;

/// Which peers a `ForwardBatch` received from `from` under a view hash
/// other than this node's own re-forwards to: each record's owners under
/// `view`, this node and `from` excluded (this node applies its own share
/// directly; `from` is either an owner that already holds the record or a
/// non-owner that never wanted it), grouped by target set like
/// [`group_by_owner_set`]. Pure and mesh-free. `None` when `hops` already
/// reached [`MAX_FORWARD_HOPS`] or the hashes agree, so the caller sends
/// nothing.
fn reforward_groups(
    view: &OwnershipView,
    self_node: NodeId,
    from: NodeId,
    sender_view_hash: u64,
    hops: u8,
    records: &[WireRecord],
) -> Option<Vec<fan_out::OwnerGroup>> {
    if hops >= MAX_FORWARD_HOPS || sender_view_hash == view.view_hash() {
        return None;
    }
    let groups: Vec<fan_out::OwnerGroup> =
        fan_out::group_by_owner_set(view, self_node, records.to_vec())
            .into_iter()
            .map(|group| fan_out::OwnerGroup {
                owners: group
                    .owners
                    .into_iter()
                    .filter(|&n| n != from)
                    .collect::<Vec<_>>(),
                records: group.records,
            })
            .filter(|group| !group.owners.is_empty())
            .collect();
    Some(groups)
}

/// Re-forwards a `ForwardBatch` the sender routed under a stale or merely
/// different ownership view to the records' owners under this node's
/// view, so a write whose target set the writer got wrong still reaches
/// every current owner: a tombstone from a writer that has not yet seen a
/// replacement owner join lands on that owner too, instead of waiting for
/// an anti-entropy round to pair the two, and a record for a bucket this
/// node does not own at all travels on instead of being dropped by the
/// inbound guard. Sent as a `hops + 1` batch stamped with this node's own
/// view hash. A no-op for a shard without an ownership view (every mode
/// but `Mode::Distributed`).
/// A `ForwardBatch` re-forwarded to its records' true owners under this
/// node's ownership view, gathered here so [`reforward_stale_view`] takes
/// one argument beyond the mesh it sends through.
struct ReforwardBatch<'a> {
    self_node: NodeId,
    from: NodeId,
    shard: &'a dyn ShardOps,
    cache_name: &'a SmolStr,
    sender_view_hash: u64,
    hops: u8,
    records: &'a [WireRecord],
}

async fn reforward_stale_view(mesh: &Mesh, batch: ReforwardBatch<'_>) {
    let ReforwardBatch {
        self_node,
        from,
        shard,
        cache_name,
        sender_view_hash,
        hops,
        records,
    } = batch;
    let Some(view) = shard.ownership_view() else {
        return;
    };
    let Some(groups) = reforward_groups(&view, self_node, from, sender_view_hash, hops, records)
    else {
        return;
    };
    if groups.is_empty() {
        return;
    }
    let forwarded: usize = groups.iter().map(|g| g.records.len()).sum();
    metrics::counter!(
        "sundog_forwarded_writes_total",
        "cache" => cache_name.to_string()
    )
    .increment(u64::try_from(forwarded).unwrap_or(u64::MAX));
    tracing::debug!(
        cache = %cache_name,
        peer = %from,
        sender_view_hash,
        local_view_hash = view.view_hash(),
        records = forwarded,
        "forward batch routed under another view; re-forwarded to its owners under ours"
    );
    for fan_out::OwnerGroup { owners, records } in groups {
        let frames: Vec<OutFrame> = batch_forward(cache_name, view.view_hash(), hops + 1, records)
            .into_iter()
            .filter_map(|msg| match OutFrame::new(msg) {
                Ok(frame) => Some(frame),
                Err(error) => {
                    tracing::warn!(%error, "failed to encode outbound message; dropped");
                    None
                }
            })
            .collect();
        for peer in owners {
            mesh.send_frames_awaiting(peer, frames.clone()).await;
        }
    }
}

/// Periodically garbage-collects one shard's expired tombstones and flushes
/// pending housekeeping via `ShardOps::run_pending_tasks`, at a quarter of
/// `tombstone_ttl` so a tombstone is never held much past its deadline.
/// `mode` and `absence` decide each tick whether collection past
/// `tombstone_ttl` defers via [`absence::should_defer_gc`]: a
/// `Mode::Replicated` tombstone survives up to `tombstone_max_ttl` while any
/// recently known member is absent, and for another `tombstone_ttl` once it
/// is back; `Mode::Distributed` narrows that to a member that co-owns a
/// part with this node when it is eligible again.
pub(crate) async fn tombstone_gc_task(
    shard: Arc<dyn ShardOps>,
    mode: Mode,
    tombstone_ttl: Duration,
    tombstone_max_ttl: Duration,
    absence: absence::AbsenceTracker,
    cancel: CancellationToken,
) {
    let period = (tombstone_ttl / 4).max(Duration::from_secs(1));
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        if cancel.run_until_cancelled(ticker.tick()).await.is_none() {
            return;
        }
        shard.run_pending_tasks().await;
        let view = shard.ownership_view();
        let defer = absence::should_defer_gc(
            mode,
            &absence,
            tombstone_max_ttl,
            tombstone_ttl,
            view.as_deref(),
        );
        shard.gc_tombstones(defer).await;
    }
}

/// Publishes `sundog_cache_entries{cache}` for one opened cache, and
/// `sundog_cache_bytes{cache}` for one with a memory ceiling: set
/// immediately, then refreshed every 5 seconds from [`Shard::entry_count`]
/// and [`Shard::resident_bytes`] for as long as the cache stays open. The
/// count is only advisory until pending housekeeping flushes.
pub(crate) async fn cache_entries_gauge_task<K, V>(
    shard: Arc<Shard<K, V>>,
    name: SmolStr,
    cancel: CancellationToken,
) where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let gauge = metrics::gauge!("sundog_cache_entries", "cache" => name.to_string());
    let bytes_gauge = shard
        .resident_bytes()
        .is_some()
        .then(|| metrics::gauge!("sundog_cache_bytes", "cache" => name.to_string()));
    let publish_bytes = || {
        if let (Some(gauge), Some(bytes)) = (&bytes_gauge, shard.resident_bytes()) {
            gauge.set(resident_bytes_f64(bytes));
        }
    };
    gauge.set(entry_count_f64(shard.entry_count().await));
    publish_bytes();

    let mut ticker = tokio::time::interval(Duration::from_secs(5));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        if cancel.run_until_cancelled(ticker.tick()).await.is_none() {
            return;
        }
        gauge.set(entry_count_f64(shard.entry_count().await));
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

#[cfg(kani)]
mod kani_proofs {
    use super::*;

    /// Bucket runs split a serve order where the bucket changes: every run
    /// is one bucket, runs ascend by bucket, and together the runs are the
    /// order. Ids span four buckets of four parts, where raw id order and
    /// bucket order disagree.
    #[kani::proof]
    #[kani::unwind(8)]
    fn bucket_runs_split_the_serve_order_at_each_bucket() {
        let granularity = if kani::any() {
            Granularity::Part
        } else {
            Granularity::Bucket
        };
        let key = |id: u16| match granularity {
            Granularity::Bucket => (id, 0),
            Granularity::Part => (PartId::from_raw(id).bucket(), PartId::from_raw(id).part()),
        };
        let order: [u16; 3] = std::array::from_fn(|_| {
            let (bucket, part): (u8, u8) = (kani::any(), kani::any());
            kani::assume(bucket < 4 && part < 4);
            match granularity {
                Granularity::Bucket => u16::from(bucket),
                Granularity::Part => PartId::new(u16::from(bucket), part).raw(),
            }
        });
        kani::assume(order.windows(2).all(|pair| key(pair[0]) < key(pair[1])));
        let mut next = 0;
        let mut last_bucket = None;
        for run in bucket_runs(granularity, &order) {
            let bucket = key(run[0]).0;
            assert!(last_bucket < Some(bucket));
            last_bucket = Some(bucket);
            for &id in run {
                assert_eq!((id, key(id).0), (order[next], bucket));
                next += 1;
            }
            if granularity == Granularity::Bucket {
                assert_eq!(run.len(), 1);
            }
        }
        assert_eq!(next, order.len());
    }
}

// Real-transport-only: these build a live `Cluster` with a real `Mesh` and
// sockets, which panics under `sim` outside a driven `turmoil::Sim`.
#[cfg(all(test, not(feature = "sim")))]
mod tests;

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
pub(crate) mod sketch;
pub(crate) mod state_transfer;

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::hash::Hash;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::{self, BoxStream, StreamExt as _};
use serde::Serialize;
use serde::de::DeserializeOwned;
use smol_str::SmolStr;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::cache::CacheBuilder;
use crate::config::ClusterConfig;
use crate::discovery::mdns::Mdns;
use crate::discovery::statics::Static;
use crate::discovery::{Discovery, DiscoveryKind};
use crate::error::JoinError;
use crate::hlc::Hlc;
use crate::membership::{CacheModes, Membership, Peer};
use crate::net::{InboundMsg, Mesh, MsgClass, OutFrame, RequestHandler, batch_replicate};
use crate::node::{NodeId, NodeName};
use crate::store::{FanOutQueue, Mode, Shard, ShardOps};
use crate::wire::{self, Msg, WireRecord};

/// The cluster's type-erased cache registry: `cache name -> Arc<dyn ShardOps>`.
/// Shared with the [`RequestHandler`] passed to [`Mesh::spawn`].
type ShardRegistry = Arc<RwLock<HashMap<SmolStr, Arc<dyn ShardOps>>>>;

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
    mesh: Mesh,
    shards: ShardRegistry,
    /// The [`Mode`] each open cache was opened under; [`mode_conflict_task`]'s
    /// local half.
    local_modes: RwLock<HashMap<SmolStr, Mode>>,
    config: ClusterConfig,
    absence: absence::AbsenceTracker,
    /// When each peer last streamed replicate traffic in; see
    /// [`Cluster::peer_is_streaming`].
    inbound_activity: Arc<InboundActivity>,
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
    fn note(&self, from: NodeId) {
        self.seen
            .write()
            .expect("invariant: inbound-activity lock is never poisoned")
            .insert(from, crate::net::mono_ms());
    }

    /// Whether `peer` streamed replicate traffic within `window`.
    pub(crate) fn recent(&self, peer: NodeId, window: Duration) -> bool {
        let seen = self
            .seen
            .read()
            .expect("invariant: inbound-activity lock is never poisoned");
        seen.get(&peer).is_some_and(|&at| {
            let age = crate::net::mono_ms().saturating_sub(at);
            age <= u64::try_from(window.as_millis()).unwrap_or(u64::MAX)
        })
    }
}

/// Builds a [`Cluster`]: own-and-return. `.build()` alone forms a working LAN
/// cluster.
#[must_use]
pub struct ClusterBuilder {
    name: SmolStr,
    discovery: Option<DiscoveryKind>,
    config: ClusterConfig,
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
            #[cfg(feature = "prometheus")]
            prometheus_listen: None,
        }
    }

    /// This node's id.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.inner.node
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

    pub(crate) fn mesh(&self) -> &Mesh {
        &self.inner.mesh
    }

    /// The live peer set, as membership reports it, for diagnostics.
    #[must_use]
    pub fn peers(&self) -> Vec<Peer> {
        self.inner.membership.peers().borrow().clone()
    }

    /// Records `mode` as this node's [`Mode`] for cache `name` and gossips it
    /// to peers.
    pub(crate) fn advertise_cache_mode(&self, name: &SmolStr, mode: Mode) {
        self.inner
            .local_modes
            .write()
            .expect("invariant: local cache-mode map lock is never poisoned")
            .insert(name.clone(), mode);
        self.inner.membership.set_cache_mode(name, mode);
    }

    /// Every live peer's advertised cache modes, as membership reports them.
    pub(crate) fn advertised_cache_modes(&self) -> CacheModes {
        self.inner.membership.cache_modes().borrow().clone()
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

    /// Leaves the cluster gracefully: background loops are cancelled and
    /// joined, then chitchat departs and the data plane closes its
    /// connections. No further calls on any clone of this handle.
    ///
    /// Cache handles opened before this call keep working for local
    /// reads/writes.
    pub async fn shutdown(self) {
        self.inner.cancel.cancel();
        self.inner.tracker.close();
        self.inner.tracker.wait().await;
        self.inner.membership.clone().shutdown().await;
        self.inner.mesh.clone().shutdown().await;
        tracing::info!(node = %self.inner.node, cluster = %self.inner.name, "cluster shut down");
    }
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

    /// Installs a Prometheus recorder and serves `GET /metrics` on `addr` for
    /// the life of the process, once [`build`](Self::build) succeeds.
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
    /// Returns [`JoinError`] if the gossip or data-plane sockets cannot bind,
    /// or the membership backend fails to start.
    pub async fn build(self) -> Result<Cluster, JoinError> {
        let Self {
            name,
            discovery,
            config,
            #[cfg(feature = "prometheus")]
            prometheus_listen,
        } = self;

        #[cfg(feature = "prometheus")]
        if let Some(addr) = prometheus_listen {
            crate::telemetry::install_listener(addr).map_err(|source| JoinError::Bind {
                addr,
                source: std::io::Error::other(source),
            })?;
        }

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
                "ClusterConfig::ae_sketch_cells ({}) encodes to up to {sketch_frame} bytes, \
                 more than max_frame ({}) allows",
                config.ae_sketch_cells, config.max_frame
            )));
        }

        let node = NodeId::random();
        let hostname = local_hostname();
        let node_name = NodeName::new(&hostname, node);

        let discovery = discovery
            .unwrap_or_else(|| DiscoveryKind::Mdns(Mdns::new(name.clone(), node_name.to_string())));

        let data_bind_addr = reserve_data_bind_addr(config.data_bind_addr).await?;
        let advertise_ip =
            crate::membership::resolve_advertise_ip(data_bind_addr.ip()).map_err(|source| {
                JoinError::Bind {
                    addr: data_bind_addr,
                    source,
                }
            })?;
        let advertise_data_addr = SocketAddr::new(advertise_ip, data_bind_addr.port());

        let membership = Membership::spawn(
            name.clone(),
            node,
            &hostname,
            advertise_data_addr,
            &config,
            discovery.candidates(),
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
            ae_part_min_bucket: config.ae_part_min_bucket,
            ae_sketch_min_bucket: config.ae_sketch_min_bucket,
            ae_sketch_cells: config.ae_sketch_cells,
        });
        let (mesh, inbound_rx) =
            Mesh::spawn(data_bind_addr, node, incarnation, &config, handler).await?;

        let cluster = Cluster {
            inner: Arc::new(ClusterInner {
                node,
                name: name.clone(),
                membership,
                mesh,
                shards,
                local_modes: RwLock::new(HashMap::new()),
                config,
                absence: absence::AbsenceTracker::default(),
                inbound_activity: Arc::new(InboundActivity::default()),
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
    cluster.spawn_tracked(absence::tracking_task(
        cluster.inner.membership.peers(),
        cluster.absence_tracker(),
        cluster.cancel_token(),
    ));
    cluster.spawn_tracked(inbound_loop(
        cluster.shards(),
        Arc::clone(&cluster.inner.inbound_activity),
        inbound_rx,
        cluster.cancel_token(),
    ));
    cluster.spawn_tracked(open_cache_gauge_task(
        cluster.shards(),
        cluster.cancel_token(),
    ));
}

/// Resolves the address `Mesh::spawn` binds to and `Membership::spawn`
/// advertises. A non-zero configured port is used as-is; the zeroconf port
/// `0` is claimed here and released for `Mesh::spawn` to reclaim, so
/// `Membership::spawn` has a real port to gossip.
async fn reserve_data_bind_addr(configured: SocketAddr) -> Result<SocketAddr, JoinError> {
    if configured.port() != 0 {
        return Ok(configured);
    }
    let probe = TcpListener::bind(configured)
        .await
        .map_err(|source| JoinError::Bind {
            addr: configured,
            source,
        })?;
    probe.local_addr().map_err(|source| JoinError::Bind {
        addr: configured,
        source,
    })
}

fn local_hostname() -> String {
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
    ae_part_min_bucket: usize,
    ae_sketch_min_bucket: usize,
    ae_sketch_cells: usize,
}

impl ClusterRequestHandler {
    fn lookup(&self, cache: &SmolStr) -> Option<Arc<dyn ShardOps>> {
        self.shards
            .read()
            .expect("invariant: shard registry lock is never poisoned")
            .get(cache)
            .cloned()
    }
}

impl RequestHandler for ClusterRequestHandler {
    fn snapshot_chunks(&self, cache: SmolStr) -> BoxStream<'static, Vec<WireRecord>> {
        match self.lookup(&cache) {
            Some(shard) => shard.snapshot_chunks(),
            None => stream::empty().boxed(),
        }
    }

    fn digests(&self, cache: SmolStr) -> BoxFuture<'_, Vec<(u16, u64)>> {
        Box::pin(async move {
            match self.lookup(&cache) {
                Some(shard) => shard.digests().await,
                None => Vec::new(),
            }
        })
    }

    fn bucket_entries(&self, cache: SmolStr, bucket: u16) -> BoxFuture<'_, Vec<(Bytes, Hlc)>> {
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

    fn bucket_lens(&self, cache: SmolStr, buckets: Vec<u16>) -> BoxFuture<'_, Vec<(u16, usize)>> {
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
    ) -> BoxFuture<'_, Vec<(u16, Vec<u64>)>> {
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
        parts: Vec<(u16, u8)>,
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
}

fn lookup_shard(shards: &ShardRegistry, cache: &SmolStr) -> Option<Arc<dyn ShardOps>> {
    shards
        .read()
        .expect("invariant: shard registry lock is never poisoned")
        .get(cache)
        .cloned()
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
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            changed = peers.changed() => {
                if changed.is_err() {
                    return; // membership shut down
                }
                let current = peers.borrow_and_update().clone();
                tracing::info!(peer_count = current.len(), "membership view changed");
                set_live_peers_gauge(current.len());
                mesh.update_peers(current);
            }
        }
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
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            changed = modes.changed() => {
                if changed.is_err() {
                    return;
                }
                let current = modes.borrow_and_update().clone();
                report_mode_conflicts(&current, &cluster, &mut warned);
            }
        }
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
    let local_modes = cluster
        .inner
        .local_modes
        .read()
        .expect("invariant: local cache-mode map lock is never poisoned");
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
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            _ = ticker.tick() => {
                let count = shards
                    .read()
                    .expect("invariant: shard registry lock is never poisoned")
                    .len();
                metrics::gauge!("sundog_open_caches")
                    .set(f64::from(u32::try_from(count).unwrap_or(u32::MAX)));
            }
        }
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

/// The single consumer of `Mesh`'s inbound-message channel: dispatches
/// `Invalidate`/`Replicate`/`ReplicateBatch` to the named shard. A message
/// for an unregistered cache is dropped with a trace event.
///
/// Drains a bounded batch per wake with `recv_many`, so a cache name is
/// looked up at most once per batch and a run of same-cache
/// `Replicate`/`ReplicateBatch` messages coalesces into one
/// `apply_remote_batch` call, applied in drain order.
async fn inbound_loop(
    shards: ShardRegistry,
    activity: Arc<InboundActivity>,
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

        let mut streaming_from: HashSet<NodeId> = HashSet::new();
        for InboundMsg { from, msg } in &drained {
            if matches!(msg, Msg::Replicate { .. } | Msg::ReplicateBatch { .. }) {
                streaming_from.insert(*from);
            }
        }
        for from in streaming_from {
            activity.note(from);
        }

        let mut shard_cache: HashMap<SmolStr, Option<Arc<dyn ShardOps>>> = HashMap::new();
        let mut pending: Option<(SmolStr, Vec<WireRecord>)> = None;
        for InboundMsg { from, msg } in drained.drain(..) {
            match msg {
                Msg::Replicate { cache, rec } => match &mut pending {
                    Some((pending_cache, recs)) if *pending_cache == cache => recs.push(rec),
                    _ => {
                        if let Some((old_cache, old_recs)) = pending.take() {
                            apply_pending_replicate(&shards, &mut shard_cache, old_cache, old_recs)
                                .await;
                        }
                        pending = Some((cache, vec![rec]));
                    }
                },
                Msg::ReplicateBatch { cache, mut recs } => match &mut pending {
                    Some((pending_cache, pending_recs)) if *pending_cache == cache => {
                        pending_recs.append(&mut recs);
                    }
                    _ => {
                        if let Some((old_cache, old_recs)) = pending.take() {
                            apply_pending_replicate(&shards, &mut shard_cache, old_cache, old_recs)
                                .await;
                        }
                        pending = Some((cache, recs));
                    }
                },
                Msg::Invalidate { cache, key, ver } => {
                    if let Some((old_cache, old_recs)) = pending.take() {
                        apply_pending_replicate(&shards, &mut shard_cache, old_cache, old_recs)
                            .await;
                    }
                    let shard = shard_cache
                        .entry(cache.clone())
                        .or_insert_with(|| lookup_shard(&shards, &cache));
                    if let Some(shard) = shard {
                        shard.invalidate(key, ver).await;
                    } else {
                        tracing::trace!(%cache, %from, "invalidate for unknown cache; dropped");
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
                | Msg::ReqDone => {}
            }
        }
        if let Some((cache, recs)) = pending.take() {
            apply_pending_replicate(&shards, &mut shard_cache, cache, recs).await;
        }
    }
}

/// Drains one opened cache's queue of locally written keys and fans them out
/// over the mesh per [`Mode`]; `Shard` holds no handle to `net::Mesh`. A
/// `get_or_load` read-through fill fans out too, letting other
/// `Replicated`-mode peers skip their own loader call.
///
/// Each iteration takes the whole backlog at once, so a burst of writes
/// costs one round of per-peer sends, not one per write. See [`FanOutQueue`]
/// for why nothing drops for arriving too fast.
pub(crate) async fn fan_out_task<K, V>(
    shard: Arc<Shard<K, V>>,
    cluster: Cluster,
    queue: Arc<FanOutQueue<K>>,
    cache_name: SmolStr,
    mode: Mode,
    cancel: CancellationToken,
) where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            () = queue.wait_nonempty() => {
                let keys = queue.drain();
                fan_out_batch(&shard, &cluster, &cache_name, mode, keys).await;
            }
        }
    }
}

async fn fan_out_batch<K, V>(
    shard: &Shard<K, V>,
    cluster: &Cluster,
    cache_name: &SmolStr,
    mode: Mode,
    notified: Vec<K>,
) where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    // The queue carries local writes only, so this only dedups the drained
    // burst.
    let mut seen: HashSet<&K> = HashSet::new();
    let mut keys: Vec<K> = Vec::new();
    for key in &notified {
        if seen.insert(key) {
            keys.push(key.clone());
        }
    }
    if keys.is_empty() {
        return;
    }

    // Re-fetches through `Shard::records_for_typed` rather than carrying the
    // `Hlc`/wire bytes on `Event` itself. A missing key on re-fetch means a
    // later write or GC already covers it, so nothing stale needs fanning out.
    let records = shard.records_for_typed(&keys).await;
    let peers = cluster.live_peer_ids();
    let (class, msgs): (MsgClass, Vec<Msg>) = match mode {
        Mode::Local => return,
        Mode::Invalidation => (
            MsgClass::Invalidate,
            records
                .into_iter()
                .map(|rec| Msg::Invalidate {
                    cache: cache_name.clone(),
                    key: rec.key,
                    ver: rec.ver,
                })
                .collect(),
        ),
        // Pre-batched by the same budget/count rules `net::conn`'s writer
        // uses for coalescing, so a drained burst can't flood the outbox
        // into drop-newest. The writer-side coalescer still catches trickle
        // writes arriving one drained event at a time.
        Mode::Replicated => (MsgClass::Replicate, batch_replicate(cache_name, records)),
    };
    // Encodes each message once, so every live peer gets a cheap `Bytes` clone
    // of the same frame.
    let frames: Vec<OutFrame> = msgs
        .into_iter()
        .filter_map(|msg| match OutFrame::new(msg) {
            Ok(frame) => Some(frame),
            Err(error) => {
                tracing::warn!(%error, "failed to encode outbound message; dropped");
                None
            }
        })
        .collect();
    // Resolves each peer's handle once and reuses it for the whole batch.
    for &peer in &peers {
        cluster
            .mesh()
            .send_frames(peer, class, frames.iter().cloned());
    }
}

/// Periodically garbage-collects one shard's expired tombstones and flushes
/// pending housekeeping via `ShardOps::run_pending_tasks`. Runs at a quarter
/// of `tombstone_ttl` so a tombstone is never held much past its deadline.
///
/// `mode` and `absence` decide each tick whether collection past
/// `tombstone_ttl` defers via [`absence::should_defer_gc`]: while a
/// recently known member is absent, a `Mode::Replicated` tombstone survives
/// up to `tombstone_max_ttl`, so that member can't resurrect the entry on
/// return. `Mode::Local`/`Mode::Invalidation` never defer.
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
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            _ = ticker.tick() => {
                shard.run_pending_tasks().await;
                let defer = absence::should_defer_gc(mode, &absence, tombstone_max_ttl);
                shard.gc_tombstones(defer).await;
            }
        }
    }
}

/// Publishes `sundog_cache_entries{cache}` for one opened cache: set
/// immediately, then refreshed every 5 seconds from [`Shard::entry_count`]
/// for as long as the cache stays open. The count is only advisory until
/// pending housekeeping flushes.
pub(crate) async fn cache_entries_gauge_task<K, V>(
    shard: Arc<Shard<K, V>>,
    name: SmolStr,
    cancel: CancellationToken,
) where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let gauge = metrics::gauge!("sundog_cache_entries", "cache" => name.to_string());
    gauge.set(entry_count_f64(shard.entry_count().await));

    let mut ticker = tokio::time::interval(Duration::from_secs(5));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            _ = ticker.tick() => {
                gauge.set(entry_count_f64(shard.entry_count().await));
            }
        }
    }
}

/// Saturating `u64` -> `f64` conversion for gauge values.
fn entry_count_f64(count: u64) -> f64 {
    f64::from(u32::try_from(count).unwrap_or(u32::MAX))
}

// Real-transport-only: these build a live `Cluster` with a real `Mesh` and
// sockets, which panics under `sim` outside a driven `turmoil::Sim`.
#[cfg(all(test, not(feature = "sim")))]
mod tests {
    use std::net::Ipv4Addr;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::error::CacheError;
    use crate::store::{ConflictResolver, Event, Origin, RecordView, Winner};

    /// Loopback-only config: skips the outbound-interface probe and keeps
    /// anti-entropy/tombstone timing tight for fast, deterministic tests.
    fn loopback_config() -> ClusterConfig {
        let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        ClusterConfig {
            gossip_bind_addr: loopback,
            data_bind_addr: loopback,
            ae_interval: Duration::from_millis(200),
            tombstone_ttl: Duration::from_secs(2),
            ..ClusterConfig::default()
        }
    }

    async fn wait_for_peer_count(cluster: &Cluster, expected: usize) {
        let mut peers = cluster.inner.membership.peers();
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if peers.borrow().len() >= expected {
                    return;
                }
                if peers.changed().await.is_err() {
                    return;
                }
            }
        })
        .await
        .expect("peers converge within the bound");
    }

    #[tokio::test]
    async fn single_node_cluster_forms_with_no_seeds_and_local_cache_round_trips() {
        let cluster = Cluster::builder("cluster-it-single")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds even with nobody else on the seed list");

        let cache = cluster
            .cache::<u32, String>("solo")
            .mode(Mode::Local)
            .open()
            .await
            .expect("open succeeds");
        cache.insert(1, "a".into()).await.expect("insert");
        assert_eq!(cache.get(&1).await, Some("a".to_string()));

        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn open_rejects_replicated_mode_combined_with_a_finite_max_capacity() {
        let cluster = Cluster::builder("cluster-it-replicated-capacity-guard")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");

        match cluster
            .cache::<u32, String>("bounded")
            .mode(Mode::Replicated)
            .max_capacity(10)
            .open()
            .await
        {
            Err(CacheError::ReplicatedWithLocalEviction { cache }) => assert_eq!(cache, "bounded"),
            other => panic!(
                "expected ReplicatedWithLocalEviction, got {:?}",
                other.map(|_| ())
            ),
        }

        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn open_rejects_replicated_mode_combined_with_tti() {
        let cluster = Cluster::builder("cluster-it-replicated-tti-guard")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");

        let err = cluster
            .cache::<u32, String>("idle-bounded")
            .mode(Mode::Replicated)
            .tti(Duration::from_secs(60))
            .open()
            .await
            .expect_err("Replicated + tti must be rejected");
        assert!(matches!(
            err,
            CacheError::ReplicatedWithLocalEviction { .. }
        ));

        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn build_rejects_a_max_frame_above_the_wire_codec_cap() {
        let mut config = loopback_config();
        config.max_frame = crate::wire::MAX_FRAME + 1;

        let err = Cluster::builder("cluster-it-max-frame-guard")
            .seeds(std::iter::empty())
            .config(config)
            .build()
            .await
            .expect_err("max_frame above the wire cap must be rejected at build() time");
        assert!(matches!(err, JoinError::InvalidConfig(_)));
    }

    #[tokio::test]
    async fn build_rejects_a_sketch_that_cannot_fit_one_frame() {
        let mut config = loopback_config();
        config.ae_sketch_cells = crate::wire::MAX_FRAME;

        let err = Cluster::builder("cluster-it-sketch-cells-guard")
            .seeds(std::iter::empty())
            .config(config)
            .build()
            .await
            .expect_err("a sketch wider than max_frame must be rejected at build() time");
        assert!(matches!(err, JoinError::InvalidConfig(_)), "{err:?}");
    }

    #[tokio::test]
    async fn cache_and_cluster_debug_impls_surface_identifying_fields() {
        let cluster = Cluster::builder("cluster-it-debug-fmt")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");
        let cache = cluster
            .cache::<u32, String>("debug-fmt")
            .open()
            .await
            .expect("open succeeds");

        let cluster_fmt = format!("{cluster:?}");
        assert!(cluster_fmt.contains("Cluster"));
        let cache_fmt = format!("{cache:?}");
        assert!(cache_fmt.contains("debug-fmt"));

        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn cache_name_returns_the_opened_name() {
        let cluster = Cluster::builder("cluster-it-cache-name")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");

        let cache = cluster
            .cache::<u32, String>("named-cache")
            .open()
            .await
            .expect("open succeeds");
        assert_eq!(cache.name(), "named-cache");

        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn local_cache_with_weigher_and_ttl_stays_bounded_and_expires() {
        let cluster = Cluster::builder("cluster-it-weigher-ttl")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");

        let cache = cluster
            .cache::<u32, Vec<u8>>("weighed")
            .mode(Mode::Local)
            .max_capacity(50)
            .weigher(|_key: &u32, value: &Vec<u8>| u32::try_from(value.len()).unwrap_or(u32::MAX))
            .ttl(Duration::from_millis(150))
            .open()
            .await
            .expect("open succeeds");

        // Checked before the burst below, so capacity eviction never
        // competes with it for survival.
        cache.insert(999, vec![0u8; 5]).await.expect("insert");
        assert_eq!(cache.get(&999).await, Some(vec![0u8; 5]));
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            cache.get(&999).await,
            None,
            "the entry expires by its own TTL"
        );

        // 40 entries at weight 5 each (200) against a 50-unit cap: at most 10
        // survive.
        for i in 0..40u32 {
            cache.insert(i, vec![0u8; 5]).await.expect("insert");
        }
        assert!(
            cache.entry_count().await <= 10,
            "a weigher-and-capacity cache stays within its weight bound after a burst of inserts"
        );

        cluster.shutdown().await;
    }

    /// A resolver that keeps whichever record has the longer value, for
    /// exercising [`crate::cache::CacheBuilder::resolver`] end to end.
    #[derive(Debug, Clone, Copy)]
    struct KeepsTheLongerValue;

    impl ConflictResolver for KeepsTheLongerValue {
        fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
            let len_a = a.value.map_or(0, <[u8]>::len);
            let len_b = b.value.map_or(0, <[u8]>::len);
            if len_b > len_a { Winner::B } else { Winner::A }
        }
    }

    #[tokio::test]
    async fn cache_opened_with_a_custom_resolver_keeps_the_longer_value_over_a_newer_shorter_remote_write()
     {
        let cluster = Cluster::builder("cluster-it-custom-resolver")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");

        let cache = cluster
            .cache::<u32, Vec<u8>>("resolved")
            .mode(Mode::Local)
            .resolver(Arc::new(KeepsTheLongerValue))
            .open()
            .await
            .expect("open succeeds");
        cache.insert(1, vec![0u8; 10]).await.expect("insert");

        let name = SmolStr::new("resolved");
        let shard = cluster
            .shards()
            .read()
            .expect("shard registry lock is never poisoned")
            .get(&name)
            .cloned()
            .expect("shard is registered under the name it was opened with");

        let shorter_but_newer = WireRecord {
            key: Bytes::from(postcard::to_stdvec(&1u32).expect("u32 key encodes")),
            value: Some(Bytes::from(
                postcard::to_stdvec(&vec![0u8; 3]).expect("value encodes"),
            )),
            ver: Hlc {
                wall_ms: u64::MAX / 2,
                logical: 0,
                node: NodeId::from(9),
            },
            expires_at_ms: None,
        };
        ShardOps::apply_remote(shard.as_ref(), shorter_but_newer).await;

        assert_eq!(
            cache.get(&1).await,
            Some(vec![0u8; 10]),
            "the custom resolver keeps the longer value over a newer-but-shorter remote write"
        );

        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn reopening_the_same_cache_name_fails_cleanly() {
        let cluster = Cluster::builder("cluster-it-reopen")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");

        let _first = cluster
            .cache::<u32, String>("dup")
            .open()
            .await
            .expect("first open succeeds");
        match cluster.cache::<u32, String>("dup").open().await {
            Err(CacheError::AlreadyOpen { cache }) => assert_eq!(cache, "dup"),
            other => panic!("expected AlreadyOpen, got {}", other.is_ok()),
        }

        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn cache_handle_survives_shutdown_without_panicking() {
        let cluster = Cluster::builder("cluster-it-shutdown")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");
        let cache = cluster
            .cache::<u32, String>("post-shutdown")
            .open()
            .await
            .expect("open succeeds");

        cluster.shutdown().await;

        cache
            .insert(1, "still local".into())
            .await
            .expect("a local insert after shutdown neither errors nor panics");
        assert_eq!(cache.get(&1).await, Some("still local".to_string()));
    }

    async fn two_node_cluster(cluster_name: &str) -> (Cluster, Cluster) {
        let cluster_a = Cluster::builder(cluster_name)
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("node a builds");
        let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;

        let cluster_b = Cluster::builder(cluster_name)
            .seeds([gossip_a])
            .config(loopback_config())
            .build()
            .await
            .expect("node b builds");

        wait_for_peer_count(&cluster_a, 1).await;
        wait_for_peer_count(&cluster_b, 1).await;
        (cluster_a, cluster_b)
    }

    /// Polls until `cluster` sees a live peer advertising `cache` under some
    /// [`Mode`]. Needed before a second `open()`, or it could race the first.
    async fn wait_for_cache_advertised(cluster: &Cluster, cache: &str) {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if cluster
                    .advertised_cache_modes()
                    .values()
                    .any(|caches| caches.contains_key(cache))
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("peer's cache-mode advertisement converges within the bound");
    }

    #[tokio::test]
    async fn open_rejects_a_cache_mode_that_conflicts_with_a_live_peer() {
        let (cluster_a, cluster_b) = two_node_cluster("cluster-it-mode-mismatch").await;

        let _cache_a = cluster_a
            .cache::<u32, String>("users")
            .mode(Mode::Replicated)
            .open()
            .await
            .expect("a opens as Replicated");
        wait_for_cache_advertised(&cluster_b, "users").await;

        match cluster_b
            .cache::<u32, String>("users")
            .mode(Mode::Invalidation)
            .open()
            .await
        {
            Err(CacheError::ModeMismatch {
                cache,
                local,
                remote,
            }) => {
                assert_eq!(cache, "users");
                assert_eq!(local, Mode::Invalidation);
                assert_eq!(remote, Mode::Replicated);
            }
            other => panic!("expected ModeMismatch, got {:?}", other.map(|_| ())),
        }

        cluster_a.shutdown().await;
        cluster_b.shutdown().await;
    }

    #[tokio::test]
    async fn open_allows_matching_modes_across_nodes() {
        let (cluster_a, cluster_b) = two_node_cluster("cluster-it-mode-match").await;

        let _cache_a = cluster_a
            .cache::<u32, String>("users")
            .mode(Mode::Invalidation)
            .open()
            .await
            .expect("a opens as Invalidation");
        wait_for_cache_advertised(&cluster_b, "users").await;

        let _cache_b = cluster_b
            .cache::<u32, String>("users")
            .mode(Mode::Invalidation)
            .open()
            .await
            .expect("b opens under the same mode a already advertises");

        cluster_a.shutdown().await;
        cluster_b.shutdown().await;
    }

    #[tokio::test]
    async fn replicated_mode_fans_a_local_insert_out_to_the_peer() {
        let (cluster_a, cluster_b) = two_node_cluster("cluster-it-replicate").await;

        let cache_a = cluster_a
            .cache::<u32, String>("users")
            .mode(Mode::Replicated)
            .open()
            .await
            .expect("a opens");
        let cache_b = cluster_b
            .cache::<u32, String>("users")
            .mode(Mode::Replicated)
            .open()
            .await
            .expect("b opens");
        let mut events_b = cache_b.events();

        cache_a.insert(1, "hello".into()).await.expect("a inserts");

        let event = tokio::time::timeout(Duration::from_secs(10), events_b.recv())
            .await
            .expect("event arrives within the bound")
            .expect("event channel stays open");
        match event {
            Event::Created {
                key: 1,
                value,
                origin: Origin::Remote(_),
            } => assert_eq!(value, "hello"),
            other => panic!("unexpected event: {other:?}"),
        }
        assert_eq!(cache_b.get(&1).await, Some("hello".to_string()));

        cluster_a.shutdown().await;
        cluster_b.shutdown().await;
    }

    #[test]
    fn inbound_activity_is_recent_only_inside_the_window() {
        let activity = InboundActivity::default();
        let peer = NodeId::from(5);
        assert!(
            !activity.recent(peer, Duration::from_secs(10)),
            "never seen"
        );
        activity.note(peer);
        assert!(activity.recent(peer, Duration::from_secs(10)));
        std::thread::sleep(Duration::from_millis(15));
        assert!(
            !activity.recent(peer, Duration::from_millis(5)),
            "a stamp older than the window does not count"
        );
        assert!(!activity.recent(NodeId::from(6), Duration::from_secs(10)));
    }

    #[test]
    fn mode_conflicts_are_reported_once_per_peer_and_cache() {
        let peer_a = NodeId::from(1);
        let peer_b = NodeId::from(2);
        let local: HashMap<SmolStr, Mode> = [
            (SmolStr::new("users"), Mode::Replicated),
            (SmolStr::new("orders"), Mode::Invalidation),
        ]
        .into_iter()
        .collect();
        let mut advertised: CacheModes = HashMap::new();
        advertised.insert(
            peer_a,
            [
                (SmolStr::new("users"), Mode::Invalidation),
                (SmolStr::new("orders"), Mode::Invalidation),
                (SmolStr::new("unrelated"), Mode::Replicated),
            ]
            .into_iter()
            .collect(),
        );
        advertised.insert(
            peer_b,
            [(SmolStr::new("users"), Mode::Replicated)]
                .into_iter()
                .collect(),
        );
        let mut warned = HashSet::new();

        assert_eq!(
            new_mode_conflicts(&advertised, &local, &mut warned),
            vec![ModeConflict {
                peer: peer_a,
                cache: SmolStr::new("users"),
                local: Mode::Replicated,
                remote: Mode::Invalidation,
            }],
            "only the pair whose modes differ counts; matching and unknown caches are ignored"
        );
        assert!(
            new_mode_conflicts(&advertised, &local, &mut warned).is_empty(),
            "the same view again reports nothing new"
        );

        advertised
            .get_mut(&peer_b)
            .expect("peer b advertised")
            .insert(SmolStr::new("users"), Mode::Invalidation);
        assert_eq!(
            new_mode_conflicts(&advertised, &local, &mut warned),
            vec![ModeConflict {
                peer: peer_b,
                cache: SmolStr::new("users"),
                local: Mode::Replicated,
                remote: Mode::Invalidation,
            }],
            "a peer that flips later is reported once, without repeating peer a"
        );
        assert!(new_mode_conflicts(&advertised, &local, &mut warned).is_empty());
    }

    #[tokio::test]
    async fn cache_api_batch_writes_and_reads_replicate_across_nodes() {
        let (cluster_a, cluster_b) = two_node_cluster("cluster-it-cache-api").await;

        let cache_a = cluster_a
            .cache::<u32, String>("catalog")
            .mode(Mode::Replicated)
            .open()
            .await
            .expect("a opens");
        let cache_b = cluster_b
            .cache::<u32, String>("catalog")
            .mode(Mode::Replicated)
            .open()
            .await
            .expect("b opens");

        cache_a
            .insert_many([
                (1, "one".to_string()),
                (2, "two".into()),
                (3, "three".into()),
            ])
            .await
            .expect("a inserts a batch");
        cache_a
            .insert_many_with_ttl(
                [(4, "brief".to_string()), (5, "brief".into())],
                Duration::from_millis(300),
            )
            .await
            .expect("a inserts an expiring batch");

        tokio::time::timeout(Duration::from_secs(10), async {
            while !(cache_b.contains_key(&3).await && cache_b.contains_key(&5).await) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("both batches replicate to b within the bound");
        let mut keys_b = cache_b.keys();
        keys_b.sort_unstable();
        assert_eq!(keys_b, [1, 2, 3, 4, 5]);

        let made = cache_b
            .get_or_insert_with(&6, async |_key| "six".to_string())
            .await
            .expect("b fills on miss");
        assert_eq!(made, "six");
        let kept = cache_b
            .get_or_insert_with(&6, async |_key| "never".to_string())
            .await
            .expect("b reads on hit");
        assert_eq!(kept, "six", "a hit never runs make");

        cache_a
            .remove_many([1, 2])
            .await
            .expect("a removes a batch");

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let removed_on_b =
                    !cache_b.contains_key(&1).await && !cache_b.contains_key(&2).await;
                if removed_on_b && cache_a.contains_key(&6).await {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the removal reaches b and the fill reaches a within the bound");

        tokio::time::sleep(Duration::from_millis(1500)).await;
        for cache in [&cache_a, &cache_b] {
            let mut keys = cache.keys();
            keys.sort_unstable();
            assert_eq!(keys, [3, 6], "the expiring batch is gone on both nodes");
            assert_eq!(cache.entry_count().await, 2);
        }

        cluster_a.shutdown().await;
        cluster_b.shutdown().await;
    }

    #[tokio::test]
    async fn per_entry_ttl_replicates_and_expires_on_the_peer() {
        let (cluster_a, cluster_b) = two_node_cluster("cluster-it-per-entry-ttl").await;

        let cache_a = cluster_a
            .cache::<u32, String>("sessions")
            .mode(Mode::Replicated)
            .open()
            .await
            .expect("a opens");
        let cache_b = cluster_b
            .cache::<u32, String>("sessions")
            .mode(Mode::Replicated)
            .open()
            .await
            .expect("b opens");

        cache_a
            .insert_with_ttl(1, "ephemeral".into(), Duration::from_millis(300))
            .await
            .expect("a inserts with ttl");
        cache_a
            .insert(2, "durable".into())
            .await
            .expect("a inserts");

        tokio::time::timeout(Duration::from_secs(10), async {
            while cache_b.get(&1).await.is_none() || cache_b.get(&2).await.is_none() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("both entries replicate to b within the bound");

        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(cache_a.get(&1).await, None, "origin expires its own copy");
        assert_eq!(
            cache_b.get(&1).await,
            None,
            "the replica expires from the deadline a stamped, with no TTL of its own"
        );
        assert_eq!(cache_b.get(&2).await.as_deref(), Some("durable"));

        cluster_a.shutdown().await;
        cluster_b.shutdown().await;
    }

    /// `Mode::Local` publishes no wire message, so this polls past a settle
    /// window a real message would need, then asserts it never showed.
    #[tokio::test]
    async fn local_mode_never_leaks_a_write_to_the_peer() {
        let (cluster_a, cluster_b) = two_node_cluster("cluster-it-local-no-fan-out").await;

        let cache_a = cluster_a
            .cache::<u32, String>("scratch")
            .mode(Mode::Local)
            .open()
            .await
            .expect("a opens");
        let cache_b = cluster_b
            .cache::<u32, String>("scratch")
            .mode(Mode::Local)
            .open()
            .await
            .expect("b opens");

        cache_a
            .insert(1, "only-on-a".into())
            .await
            .expect("a inserts");

        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            cache_b.get(&1).await,
            None,
            "Mode::Local must never fan a write out to peers"
        );
        assert_eq!(cache_a.get(&1).await, Some("only-on-a".to_string()));

        cluster_a.shutdown().await;
        cluster_b.shutdown().await;
    }

    #[tokio::test]
    async fn invalidation_mode_drops_a_stale_remote_copy() {
        let (cluster_a, cluster_b) = two_node_cluster("cluster-it-invalidate").await;

        let cache_a = cluster_a
            .cache::<u32, String>("users")
            .mode(Mode::Invalidation)
            .open()
            .await
            .expect("a opens");
        let cache_b = cluster_b
            .cache::<u32, String>("users")
            .mode(Mode::Invalidation)
            .open()
            .await
            .expect("b opens");

        // B warms its own local copy first; Invalidation mode never
        // replicates values. The sleep guarantees A's HLC stamp is later.
        cache_b
            .insert(1, "stale".into())
            .await
            .expect("b warms locally");
        assert_eq!(cache_b.get(&1).await, Some("stale".to_string()));
        tokio::time::sleep(Duration::from_millis(5)).await;

        cache_a
            .insert(1, "fresh".into())
            .await
            .expect("a writes a newer version");

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if cache_b.get(&1).await.is_none() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("b's stale copy is invalidated within the bound");

        cluster_a.shutdown().await;
        cluster_b.shutdown().await;
    }

    #[tokio::test]
    async fn state_transfer_warms_a_late_joiner_from_the_existing_donor() {
        let cluster_a = Cluster::builder("cluster-it-state-transfer")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("node a builds");
        let cache_a = cluster_a
            .cache::<u32, String>("users")
            .mode(Mode::Replicated)
            .open()
            .await
            .expect("a opens");
        cache_a
            .insert(1, "pre-existing".into())
            .await
            .expect("a writes before b ever joins");

        let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;
        let cluster_b = Cluster::builder("cluster-it-state-transfer")
            .seeds([gossip_a])
            .config(loopback_config())
            .build()
            .await
            .expect("node b builds");
        wait_for_peer_count(&cluster_b, 1).await;

        // `open()` blocks for state transfer, so B's copy of a key A wrote
        // before B joined is already warm, with no wait for a live write.
        let cache_b = tokio::time::timeout(
            Duration::from_secs(15),
            cluster_b
                .cache::<u32, String>("users")
                .mode(Mode::Replicated)
                .open(),
        )
        .await
        .expect("open completes within the state-transfer budget")
        .expect("b opens");

        assert_eq!(cache_b.get(&1).await, Some("pre-existing".to_string()));

        cluster_a.shutdown().await;
        cluster_b.shutdown().await;
    }

    #[tokio::test]
    async fn anti_entropy_repairs_a_locally_dropped_entry() {
        let (cluster_a, cluster_b) = two_node_cluster("cluster-it-anti-entropy").await;

        let cache_a = cluster_a
            .cache::<u32, String>("users")
            .mode(Mode::Replicated)
            .open()
            .await
            .expect("a opens");
        let cache_b = cluster_b
            .cache::<u32, String>("users")
            .mode(Mode::Replicated)
            .open()
            .await
            .expect("b opens");

        cache_a.insert(1, "hello".into()).await.expect("a inserts");
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if cache_b.get(&1).await.is_some() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("live fan-out delivers the insert to b");

        // Simulate a dropped `Replicate` message: wipe B's copy without a
        // tombstone. Only anti-entropy can bring it back.
        cache_b.invalidate_local(&1).await;
        assert_eq!(cache_b.get(&1).await, None);

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if cache_b.get(&1).await.is_some() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("anti-entropy repairs the dropped entry within a few rounds");

        cluster_a.shutdown().await;
        cluster_b.shutdown().await;
    }

    #[tokio::test]
    async fn clear_fans_tombstones_out_to_an_empty_peer() {
        let (cluster_a, cluster_b) = two_node_cluster("cluster-it-clear").await;

        let cache_a = cluster_a
            .cache::<u32, String>("users")
            .mode(Mode::Replicated)
            .open()
            .await
            .expect("a opens");
        let cache_b = cluster_b
            .cache::<u32, String>("users")
            .mode(Mode::Replicated)
            .open()
            .await
            .expect("b opens");

        cache_a
            .insert_many((0..10u32).map(|k| (k, k.to_string())))
            .await
            .expect("a inserts");

        tokio::time::timeout(Duration::from_secs(10), async {
            while cache_b.entry_count().await < 10 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("b observes all ten entries within the bound");

        cache_a.clear().await.expect("a clears");

        tokio::time::timeout(Duration::from_secs(10), async {
            while cache_b.entry_count().await != 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("b's copy converges to empty within the bound");

        cluster_a.shutdown().await;
        cluster_b.shutdown().await;
    }

    // --- Sketch-based anti-entropy (`cluster::sketch`) ---

    /// Mirrors `store::bucket_of`'s formula so a test can compute which
    /// anti-entropy bucket a key lands in without that private function.
    fn bucket_of_u32(key: u32) -> u16 {
        let bytes = postcard::to_stdvec(&key).expect("a u32 key always postcard-encodes");
        let bucket = xxhash_rust::xxh3::xxh3_64(&bytes) & (crate::store::BUCKET_COUNT as u64 - 1);
        u16::try_from(bucket).expect("masked to BUCKET_COUNT - 1, always fits in u16")
    }

    /// Among `0..n`, every key in a bucket holding more than `min_count` of
    /// them. Deterministic given a fixed key range.
    fn dense_bucket_keys(n: u32, min_count: usize) -> Vec<u32> {
        let mut by_bucket: HashMap<u16, Vec<u32>> = HashMap::new();
        for key in 0..n {
            by_bucket.entry(bucket_of_u32(key)).or_default().push(key);
        }
        by_bucket
            .into_values()
            .find(|keys| keys.len() > min_count)
            .expect("at least one bucket exceeds min_count among this many keys")
    }

    /// `count` distinct keys that all hash into the same anti-entropy bucket
    /// as key `0`, for the sketch-fallback test below.
    fn keys_colliding_with_zero(count: usize) -> Vec<u32> {
        let target = bucket_of_u32(0);
        (0..)
            .filter(|&k| bucket_of_u32(k) == target)
            .take(count)
            .collect()
    }

    /// A [`tracing::Subscriber`] that counts `cluster::anti_entropy`'s
    /// `outcome = "decoded"`/`"fallback"` events, standing in for the
    /// process-global `metrics` recorder, too fragile to install per-test.
    /// `set_default` is thread-local, scoped to this single-threaded test.
    struct AeSketchOutcomeSubscriber {
        decoded: Arc<AtomicUsize>,
        fallback: Arc<AtomicUsize>,
    }

    struct OutcomeVisitor(Option<&'static str>);

    impl tracing::field::Visit for OutcomeVisitor {
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            if field.name() == "outcome" {
                self.0 = match value {
                    "decoded" => Some("decoded"),
                    "fallback" => Some("fallback"),
                    _ => None,
                };
            }
        }

        fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}
    }

    impl tracing::Subscriber for AeSketchOutcomeSubscriber {
        fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

        fn event(&self, event: &tracing::Event<'_>) {
            if event.metadata().target() != "sundog::cluster::anti_entropy" {
                return;
            }
            let mut visitor = OutcomeVisitor(None);
            event.record(&mut visitor);
            match visitor.0 {
                Some("decoded") => {
                    self.decoded.fetch_add(1, Ordering::SeqCst);
                }
                Some("fallback") => {
                    self.fallback.fetch_add(1, Ordering::SeqCst);
                }
                _ => {}
            }
        }

        fn enter(&self, _span: &tracing::span::Id) {}
        fn exit(&self, _span: &tracing::span::Id) {}
    }

    #[tokio::test]
    async fn anti_entropy_repairs_a_large_bucket_via_the_sketch_path() {
        const N: u32 = 4096;

        let config = ClusterConfig {
            ae_sketch_min_bucket: 4,
            ..loopback_config()
        };

        let cluster_a = Cluster::builder("cluster-it-ae-sketch-decode")
            .seeds(std::iter::empty())
            .config(config.clone())
            .build()
            .await
            .expect("node a builds");
        let cache_a = cluster_a
            .cache::<u32, String>("users")
            .mode(Mode::Replicated)
            .open()
            .await
            .expect("a opens");

        cache_a
            .insert_many((0..N).map(|k| (k, k.to_string())))
            .await
            .expect("a inserts a few thousand entries before b ever joins");

        // A bucket with more than `ae_sketch_min_bucket + 1` entries, so it
        // still exceeds the threshold once one entry drops below.
        let bucket_keys = dense_bucket_keys(N, config.ae_sketch_min_bucket + 1);
        let target_key = bucket_keys[0];

        let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;
        let cluster_b = Cluster::builder("cluster-it-ae-sketch-decode")
            .seeds([gossip_a])
            .config(config)
            .build()
            .await
            .expect("node b builds");
        wait_for_peer_count(&cluster_b, 1).await;

        let cache_b = tokio::time::timeout(
            Duration::from_secs(20),
            cluster_b
                .cache::<u32, String>("users")
                .mode(Mode::Replicated)
                .open(),
        )
        .await
        .expect("open completes within the state-transfer budget")
        .expect("b opens");
        assert_eq!(cache_b.entry_count().await, u64::from(N));

        let decoded = Arc::new(AtomicUsize::new(0));
        let fallback = Arc::new(AtomicUsize::new(0));
        let _guard = tracing::subscriber::set_default(AeSketchOutcomeSubscriber {
            decoded: Arc::clone(&decoded),
            fallback: Arc::clone(&fallback),
        });

        // Simulate a dropped `Replicate` message on B; this bucket is large
        // enough that the responder answers with `Msg::AeSketch`.
        cache_b.invalidate_local(&target_key).await;
        assert_eq!(cache_b.get(&target_key).await, None);

        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if cache_b.get(&target_key).await.is_some() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("anti-entropy repairs the dropped entry via the sketch path within the bound");

        assert!(
            decoded.load(Ordering::SeqCst) > 0,
            "expected the sketch reply for this bucket to decode at least once"
        );
        assert_eq!(
            fallback.load(Ordering::SeqCst),
            0,
            "a single-entry diff in a large bucket must decode, never fall back to a listing"
        );

        cluster_a.shutdown().await;
        cluster_b.shutdown().await;
    }

    #[tokio::test]
    async fn anti_entropy_falls_back_to_the_listing_when_a_sketch_cannot_decode() {
        let config = ClusterConfig {
            ae_sketch_min_bucket: 4,
            // 2 cells per IBLT partition, too small to peel the 25-element
            // difference this test forces below.
            ae_sketch_cells: 6,
            ..loopback_config()
        };

        let cluster_a = Cluster::builder("cluster-it-ae-sketch-fallback")
            .seeds(std::iter::empty())
            .config(config.clone())
            .build()
            .await
            .expect("node a builds");
        let cache_a = cluster_a
            .cache::<u32, String>("users")
            .mode(Mode::Replicated)
            .open()
            .await
            .expect("a opens");

        // 30 keys in the same anti-entropy bucket, an oversized true
        // difference once most of them are dropped below.
        let colliding_keys = keys_colliding_with_zero(30);
        let total = u64::try_from(colliding_keys.len()).expect("30 fits in a u64");
        cache_a
            .insert_many(colliding_keys.iter().map(|&k| (k, k.to_string())))
            .await
            .expect("a inserts the colliding keys before b ever joins");

        let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;
        let cluster_b = Cluster::builder("cluster-it-ae-sketch-fallback")
            .seeds([gossip_a])
            .config(config)
            .build()
            .await
            .expect("node b builds");
        wait_for_peer_count(&cluster_b, 1).await;

        let cache_b = tokio::time::timeout(
            Duration::from_secs(20),
            cluster_b
                .cache::<u32, String>("users")
                .mode(Mode::Replicated)
                .open(),
        )
        .await
        .expect("open completes within the state-transfer budget")
        .expect("b opens");
        assert_eq!(cache_b.entry_count().await, total);

        let decoded = Arc::new(AtomicUsize::new(0));
        let fallback = Arc::new(AtomicUsize::new(0));
        let _guard = tracing::subscriber::set_default(AeSketchOutcomeSubscriber {
            decoded: Arc::clone(&decoded),
            fallback: Arc::clone(&fallback),
        });

        // Drop all but the last 5 keys on B: a true difference of 25
        // elements in one bucket, past what a 6-cell sketch can peel.
        for &key in &colliding_keys[..colliding_keys.len() - 5] {
            cache_b.invalidate_local(&key).await;
        }
        assert_eq!(cache_b.entry_count().await, 5);

        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if cache_b.entry_count().await == total {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the fallback listing path repairs every dropped entry within the bound");

        for &key in &colliding_keys {
            assert_eq!(cache_b.get(&key).await, Some(key.to_string()));
        }

        assert!(
            fallback.load(Ordering::SeqCst) > 0,
            "expected the oversized diff to be reported undecodable at least once"
        );

        cluster_a.shutdown().await;
        cluster_b.shutdown().await;
    }

    /// `cluster_b`'s registered shard for `cache`, as [`anti_entropy::run_round_against`]
    /// takes it: the same handle `Cache::open` installs in the registry.
    fn registered_shard(cluster: &Cluster, cache: &SmolStr) -> Arc<dyn ShardOps> {
        cluster
            .shards()
            .read()
            .expect("invariant: shard registry lock is never poisoned")
            .get(cache)
            .cloned()
            .expect("the cache was opened, so its shard is registered")
    }

    #[tokio::test]
    async fn run_round_against_pulls_a_dropped_key_via_the_decoded_sketch() {
        const N: u32 = 4096;
        let config = ClusterConfig {
            ae_sketch_min_bucket: 4,
            ..loopback_config()
        };

        let cluster_a = Cluster::builder("cluster-it-ae-round-pull")
            .seeds(std::iter::empty())
            .config(config.clone())
            .build()
            .await
            .expect("node a builds");
        let node_a = cluster_a.node_id();
        let cache_a = cluster_a
            .cache::<u32, String>("users")
            .mode(Mode::Replicated)
            .open()
            .await
            .expect("a opens");
        cache_a
            .insert_many((0..N).map(|k| (k, k.to_string())))
            .await
            .expect("a inserts a few thousand entries before b ever joins");

        let bucket_keys = dense_bucket_keys(N, config.ae_sketch_min_bucket + 1);
        let target_key = bucket_keys[0];

        let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;
        let cluster_b = Cluster::builder("cluster-it-ae-round-pull")
            .seeds([gossip_a])
            .config(config)
            .build()
            .await
            .expect("node b builds");
        wait_for_peer_count(&cluster_b, 1).await;

        let cache_b = tokio::time::timeout(
            Duration::from_secs(20),
            cluster_b
                .cache::<u32, String>("users")
                .mode(Mode::Replicated)
                .open(),
        )
        .await
        .expect("open completes within the state-transfer budget")
        .expect("b opens");
        assert_eq!(cache_b.entry_count().await, u64::from(N));

        // Simulate a dropped `Replicate` message, the way the sketch-decode
        // integration test above does, then run the round directly instead
        // of waiting on the scheduler: the two-node sketch test above races
        // both peers' schedulers, so it never pins the pull-by-hash path
        // this exercises deterministically.
        cache_b.invalidate_local(&target_key).await;
        assert_eq!(cache_b.get(&target_key).await, None);

        let name = SmolStr::new("users");
        let shard_b = registered_shard(&cluster_b, &name);
        crate::cluster::anti_entropy::run_round_against(&cluster_b, &shard_b, &name, node_a).await;

        assert_eq!(
            cache_b.get(&target_key).await,
            Some(target_key.to_string()),
            "the pull-by-hash path repairs the dropped key by the time the round returns"
        );

        cluster_a.shutdown().await;
        cluster_b.shutdown().await;
    }

    #[tokio::test]
    async fn run_round_against_pushes_a_locally_held_key_via_the_decoded_sketch() {
        const N: u32 = 4096;
        let config = ClusterConfig {
            ae_sketch_min_bucket: 4,
            ..loopback_config()
        };

        let cluster_a = Cluster::builder("cluster-it-ae-round-push")
            .seeds(std::iter::empty())
            .config(config.clone())
            .build()
            .await
            .expect("node a builds");
        let node_a = cluster_a.node_id();
        let cache_a = cluster_a
            .cache::<u32, String>("users")
            .mode(Mode::Replicated)
            .open()
            .await
            .expect("a opens");
        cache_a
            .insert_many((0..N).map(|k| (k, k.to_string())))
            .await
            .expect("a inserts a few thousand entries before b ever joins");

        let bucket_keys = dense_bucket_keys(N, config.ae_sketch_min_bucket + 1);
        let target_bucket = bucket_of_u32(bucket_keys[0]);
        // Bounded to ~200 expected hits against 1,024 buckets, so the search
        // is certain in practice without an unbounded iterator.
        let extra_key = (N..)
            .take(200_000)
            .find(|&k| bucket_of_u32(k) == target_bucket)
            .expect("some key beyond n lands in the same dense bucket");

        let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;
        let cluster_b = Cluster::builder("cluster-it-ae-round-push")
            .seeds([gossip_a])
            .config(config)
            .build()
            .await
            .expect("node b builds");
        wait_for_peer_count(&cluster_b, 1).await;

        let cache_b = tokio::time::timeout(
            Duration::from_secs(20),
            cluster_b
                .cache::<u32, String>("users")
                .mode(Mode::Replicated)
                .open(),
        )
        .await
        .expect("open completes within the state-transfer budget")
        .expect("b opens");
        assert_eq!(cache_b.entry_count().await, u64::from(N));

        let name = SmolStr::new("users");
        let shard_b = registered_shard(&cluster_b, &name);

        // Applied straight to the shard, not through `cache_b.insert`, so
        // nothing fans out to a on its own: only the round's push path is
        // exercised below.
        let extra_value = extra_key.to_string();
        shard_b
            .apply_remote(WireRecord {
                key: Bytes::from(postcard::to_stdvec(&extra_key).expect("test key encodes")),
                value: Some(Bytes::from(
                    postcard::to_stdvec(&extra_value).expect("test value encodes"),
                )),
                ver: Hlc {
                    wall_ms: u64::MAX / 2,
                    logical: 0,
                    node: cluster_b.node_id(),
                },
                expires_at_ms: None,
            })
            .await;
        assert_eq!(cache_b.get(&extra_key).await, Some(extra_value.clone()));
        assert_eq!(cache_a.get(&extra_key).await, None, "a never had this key");

        crate::cluster::anti_entropy::run_round_against(&cluster_b, &shard_b, &name, node_a).await;

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if cache_a.get(&extra_key).await.is_some() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the push path delivers b's extra key to a within the bound");
        assert_eq!(cache_a.get(&extra_key).await, Some(extra_value));

        cluster_a.shutdown().await;
        cluster_b.shutdown().await;
    }

    #[tokio::test]
    async fn records_for_hashes_answers_exactly_the_records_whose_hash_matches() {
        let cluster = Cluster::builder("cluster-it-records-for-hashes")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");
        let cache = cluster
            .cache::<u32, String>("users")
            .mode(Mode::Replicated)
            .open()
            .await
            .expect("open succeeds");
        cache
            .insert_many([
                (1, "one".to_string()),
                (2, "two".into()),
                (3, "three".into()),
            ])
            .await
            .expect("insert");

        // Constructed the way `ClusterBuilder::build` constructs the handler
        // it hands to `Mesh::spawn`, against this single-node cluster's own
        // registry.
        let handler = ClusterRequestHandler {
            shards: cluster.shards(),
            ae_part_min_bucket: cluster.config().ae_part_min_bucket,
            ae_sketch_min_bucket: cluster.config().ae_sketch_min_bucket,
            ae_sketch_cells: cluster.config().ae_sketch_cells,
        };
        let name = SmolStr::new("users");
        let key_one = Bytes::from(postcard::to_stdvec(&1u32).expect("test key encodes"));
        let bucket = bucket_of_u32(1);
        let hash_one = xxhash_rust::xxh3::xxh3_64(&key_one);

        let records = handler
            .records_for_hashes(name.clone(), bucket, vec![hash_one])
            .await;
        assert_eq!(records.len(), 1, "exactly the one matching hash comes back");
        assert_eq!(records[0].key, key_one);

        let none = handler
            .records_for_hashes(name, bucket, vec![hash_one.wrapping_add(1)])
            .await;
        assert!(none.is_empty(), "a hash matching nothing yields nothing");

        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn builder_discovery_override_forms_a_working_cluster() {
        let cluster = Cluster::builder("cluster-it-discovery-override")
            .discovery(crate::discovery::statics::Static::new(std::iter::empty()))
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds with a custom Discovery implementor");

        let cache = cluster
            .cache::<u32, String>("solo")
            .mode(Mode::Local)
            .open()
            .await
            .expect("open succeeds");
        cache.insert(1, "a".into()).await.expect("insert");
        assert_eq!(cache.get(&1).await, Some("a".to_string()));

        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn build_returns_bind_error_when_the_configured_data_port_is_taken() {
        let occupied = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .expect("bind an ephemeral loopback port to occupy it");
        let addr = occupied
            .local_addr()
            .expect("a freshly bound listener reports its address");

        let config = ClusterConfig {
            data_bind_addr: addr,
            ..loopback_config()
        };

        let err = Cluster::builder("cluster-it-data-bind-conflict")
            .seeds(std::iter::empty())
            .config(config)
            .build()
            .await
            .expect_err("the configured data-plane port is already taken");
        assert!(matches!(err, JoinError::Bind { .. }));

        drop(occupied);
    }

    #[tokio::test]
    async fn report_mode_conflicts_logs_a_late_conflict_against_a_live_peer() {
        let cluster = Cluster::builder("cluster-it-mode-conflict-log")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");
        let _cache = cluster
            .cache::<u32, String>("users")
            .mode(Mode::Replicated)
            .open()
            .await
            .expect("open succeeds");

        // Hand-built rather than gossiped: `report_mode_conflicts` only
        // reads `cluster`'s own advertised mode, so no live peer is needed
        // to exercise its late-conflict log line deterministically.
        let peer = NodeId::from(99);
        let mut advertised: CacheModes = HashMap::new();
        advertised.insert(
            peer,
            [(SmolStr::new("users"), Mode::Invalidation)]
                .into_iter()
                .collect(),
        );
        let mut warned = HashSet::new();

        report_mode_conflicts(&advertised, &cluster, &mut warned);
        assert!(
            warned.contains(&(peer, SmolStr::new("users"))),
            "the conflict is recorded so a repeated view doesn't re-warn"
        );

        cluster.shutdown().await;
    }
}

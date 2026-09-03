//! The top-level public entry point: `Cluster::builder(name).build()` forms a
//! working zeroconf cluster, and `cluster.cache(name)` opens named caches on
//! it — zero further setup is required for a working LAN cluster.
//!
//! Composition: `build()` resolves the data-plane's advertised address,
//! starts [`Membership`] (gossip), announces via [`Discovery`], then starts
//! [`Mesh`] (the TCP data plane) with a [`RequestHandler`] that answers
//! inbound state-transfer/anti-entropy requests over this cluster's shard
//! registry. Background tasks, all stopped together by [`Cluster::shutdown`],
//! keep the planes in sync: membership changes flow into `Mesh::update_peers`
//! and into `absence`'s partition-aware tombstone-retention tracker, inbound
//! wire messages dispatch to shards by cache name, and — spawned per opened
//! cache — local writes fan out over the mesh per [`Mode`] and expired
//! tombstones are garbage-collected (`tombstone_gc_task`).

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
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use xxhash_rust::xxh3::xxh3_64;

use crate::cache::CacheBuilder;
use crate::config::ClusterConfig;
use crate::discovery::mdns::Mdns;
use crate::discovery::statics::Static;
use crate::discovery::{Discovery, DiscoveryKind};
use crate::error::JoinError;
use crate::hlc::Hlc;
use crate::membership::{CacheModes, Membership, Peer};
use crate::net::{InboundMsg, Mesh, MsgClass, OutFrame, RequestHandler};
use crate::node::{NodeId, NodeName};
use crate::store::{FanOutNotice, Mode, Shard, ShardOps};
use crate::wire::{self, Msg, WireRecord};

/// The cluster's type-erased cache registry: `cache name -> Arc<dyn ShardOps>`.
/// Shared between [`Cluster`] itself and the [`RequestHandler`]
/// handed to [`Mesh::spawn`], so a cache opened after the mesh starts is
/// immediately visible to inbound state-transfer/anti-entropy requests.
type ShardRegistry = Arc<RwLock<HashMap<SmolStr, Arc<dyn ShardOps>>>>;

/// A running cluster membership: the join point for opening named caches.
///
/// Cheap to `Clone`; every clone shares the same membership, mesh, and cache
/// registry. Dropping every clone does not tear the cluster down — call
/// [`Cluster::shutdown`] for a graceful leave.
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
    /// The [`Mode`] each cache open in this process was opened under — the
    /// local half of the cache-config fingerprint that
    /// [`mode_conflict_task`] compares peers' advertisements against.
    local_modes: RwLock<HashMap<SmolStr, Mode>>,
    config: ClusterConfig,
    absence: absence::AbsenceTracker,
    tracker: TaskTracker,
    cancel: CancellationToken,
}

/// Builds a [`Cluster`]: own-and-return. Zero further calls beyond
/// `.build()` form a working LAN cluster — mDNS discovery, ephemeral ports,
/// and the [`ClusterConfig`] defaults.
#[must_use]
pub struct ClusterBuilder {
    name: SmolStr,
    discovery: Option<DiscoveryKind>,
    config: ClusterConfig,
    #[cfg(feature = "prometheus")]
    prometheus_listen: Option<SocketAddr>,
}

impl Cluster {
    /// Starts building a cluster named `name`. The cluster name is chitchat's
    /// cluster id — wrong-cluster gossip is rejected for free.
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

    /// Opens or joins a named cache. Returns a builder — call `.open().await`
    /// to register it in the cluster's shard registry and get back a usable
    /// [`crate::cache::Cache`] handle.
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

    /// This cluster's partition-aware tombstone-retention view — see
    /// `cluster::absence`'s module docs. Cheap to clone; shared by every
    /// opened cache's `tombstone_gc_task`.
    pub(crate) fn absence_tracker(&self) -> absence::AbsenceTracker {
        self.inner.absence.clone()
    }

    pub(crate) fn mesh(&self) -> &Mesh {
        &self.inner.mesh
    }

    /// The live peer set, as membership currently reports it — for
    /// diagnostics (the demo bin's `peers` command; `tracing`/logging).
    #[must_use]
    pub fn peers(&self) -> Vec<Peer> {
        self.inner.membership.peers().borrow().clone()
    }

    /// Records `mode` as this node's [`Mode`] for cache `name` and gossips
    /// it, so peers can compare it against their own choice for the same
    /// name (see `membership`'s cache-mode fingerprint docs). Called once by
    /// [`crate::cache::CacheBuilder::open`] right after a cache is
    /// registered in this cluster's shard registry.
    pub(crate) fn advertise_cache_mode(&self, name: &SmolStr, mode: Mode) {
        self.inner
            .local_modes
            .write()
            .expect("invariant: local cache-mode map lock is never poisoned")
            .insert(name.clone(), mode);
        self.inner.membership.set_cache_mode(name, mode);
    }

    /// Every live peer's advertised cache modes, as membership currently
    /// reports them — what [`crate::cache::CacheBuilder::open`] checks a
    /// requested mode against.
    pub(crate) fn advertised_cache_modes(&self) -> CacheModes {
        self.inner.membership.cache_modes().borrow().clone()
    }

    /// A fresh watch subscription on the live peer set — for tasks that
    /// need to react to membership changes rather than sample them.
    pub(crate) fn peers_watch(&self) -> tokio::sync::watch::Receiver<Vec<Peer>> {
        self.inner.membership.peers()
    }

    /// The live peer set, as node ids only — what `Cache`'s write-fan-out
    /// task iterates to decide who to send `Invalidate`/`Replicate` to.
    pub(crate) fn live_peer_ids(&self) -> Vec<NodeId> {
        self.inner
            .membership
            .peers()
            .borrow()
            .iter()
            .map(|peer| peer.node)
            .collect()
    }

    /// A child of this cluster's shutdown token: cancelled the moment
    /// [`Cluster::shutdown`] is called on any clone of this handle.
    pub(crate) fn cancel_token(&self) -> CancellationToken {
        self.inner.cancel.child_token()
    }

    /// Spawns `fut` on this cluster's [`TaskTracker`], so
    /// [`Cluster::shutdown`] waits for it to observe cancellation and exit.
    pub(crate) fn spawn_tracked<F>(&self, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.inner.tracker.spawn(fut);
    }

    /// Leaves the cluster gracefully: background fan-out/dispatch tasks are
    /// cancelled and joined first, then chitchat departs politely and the
    /// data plane closes its connections. No more calls should be made on any
    /// clone of this handle afterward.
    ///
    /// Cache handles opened before this call keep working for purely local
    /// reads/writes afterward — `Shard` never depends on `Mesh` or
    /// `Membership` directly — they just stop having anywhere to fan out to.
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
    /// (`Mdns`) to [`Static`] — the escape hatch for environments where mDNS
    /// doesn't reach (containers, isolated Wi-Fi) and the workhorse for tests.
    pub fn seeds(mut self, seeds: impl IntoIterator<Item = SocketAddr>) -> Self {
        self.discovery = Some(DiscoveryKind::Static(Static::new(seeds)));
        self
    }

    /// Overrides the zeroconf default (`Mdns`) discovery mechanism, e.g. with
    /// `DnsSrv` for Kubernetes, or a custom implementation.
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
    /// `metrics`'s recorder is a single process-global slot: a second
    /// `prometheus_listen` (on this or another `Cluster`), or a mix of
    /// `prometheus_listen` and [`crate::telemetry::prometheus_handle`] in the
    /// same process, fails `build()` with [`JoinError::Bind`] rather than
    /// panicking — see `telemetry`'s module docs. For embedding the scrape
    /// endpoint in an HTTP server the caller already runs, use
    /// [`crate::telemetry::prometheus_handle`] instead of this method.
    #[cfg(feature = "prometheus")]
    pub fn prometheus_listen(mut self, addr: SocketAddr) -> Self {
        self.prometheus_listen = Some(addr);
        self
    }

    /// Enables mutual TLS on the data-plane mesh, behind the `tls` feature.
    /// Equivalent to setting [`ClusterConfig::tls`] directly via
    /// [`Self::config`] — a dedicated
    /// method for the common case of overriding only this one field. See
    /// [`crate::config::TlsConfig`]'s docs for what this implies (mutual
    /// auth, the fixed required certificate SAN, and why a TLS node and a
    /// plaintext node cannot share a mesh).
    #[cfg(feature = "tls")]
    pub fn tls(mut self, tls: crate::config::TlsConfig) -> Self {
        self.config.tls = Some(tls);
        self
    }

    /// Starts discovery, membership, and the data-plane mesh, and returns
    /// once this node has joined (or begun forming) the cluster.
    ///
    /// mDNS finding nobody (a container with no multicast, a LAN of one) is
    /// not an error — it is a healthy single-node cluster, exactly as
    /// required for `Cluster::builder(name).build()` to be a working
    /// zeroconf happy path on its own.
    ///
    /// # Errors
    ///
    /// Returns [`JoinError`] if the gossip or data-plane sockets cannot be
    /// bound, or the membership backend fails to start.
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
                tracker: TaskTracker::new(),
                cancel: CancellationToken::new(),
            }),
        };

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
            inbound_rx,
            cluster.cancel_token(),
        ));
        cluster.spawn_tracked(open_cache_gauge_task(
            cluster.shards(),
            cluster.cancel_token(),
        ));

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

/// Resolves the concrete address `Mesh::spawn` should bind to, and that
/// `Membership::spawn` advertises for it. A configured non-zero port is used
/// as-is; the zeroconf default (port `0`) needs a real port *before*
/// `Membership::spawn` so it can be gossiped, but only `Mesh::spawn` actually
/// owns the listener — so a free port is claimed here and released for
/// `Mesh::spawn` to rebind moments later. Same reserve-then-release trade-off
/// `membership.rs` already accepts for its own gossip port (a vanishingly
/// unlikely race against another process on a trusted LAN).
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

/// Answers inbound state-transfer/anti-entropy requests (`StRequest`,
/// `AeDigest`, `AePull`) over this cluster's shard registry, for whichever
/// cache a peer names. A cache name this node doesn't (yet) have degrades to
/// an empty result rather than an error — a normal race, not a fault (see
/// [`RequestHandler`]'s own docs).
struct ClusterRequestHandler {
    shards: ShardRegistry,
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

    fn records_for_hashes(
        &self,
        cache: SmolStr,
        bucket: u16,
        hashes: Vec<u64>,
    ) -> BoxFuture<'_, Vec<WireRecord>> {
        Box::pin(async move {
            let Some(shard) = self.lookup(&cache) else {
                return Vec::new();
            };
            // One shard lookup serves both steps: `bucket_entries` already
            // holds every local key in `bucket`, so filtering by
            // `xxh3_64(key)` membership in `hashes` before the
            // `records_for` fetch avoids the default trait
            // implementation's second independent shard pass.
            let wanted: HashSet<u64> = hashes.into_iter().collect();
            let keys: Vec<Bytes> = shard
                .bucket_entries(bucket)
                .await
                .into_iter()
                .filter(|(key, _)| wanted.contains(&xxh3_64(key)))
                .map(|(key, _)| key)
                .collect();
            shard.records_for(keys).await
        })
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

/// Republishes [`Membership::peers`] changes as [`Mesh::update_peers`] calls,
/// for the lifetime of the cluster.
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

/// The cache-mode-mismatch late-detection sweep (see
/// [`report_mode_conflicts`]): `open()`'s own check is best-effort (two nodes
/// opening the same name under different `Mode`s concurrently can both pass
/// it), so this re-checks every live peer's advertised cache modes against
/// this node's own open caches whenever membership publishes a new view —
/// including a view change caused only by this node's own later `open()`
/// advertising a new `cache:<name>` key (self is part of the fingerprint
/// gossip drives `live_nodes_watch_stream` off, so a local-only fingerprint
/// change still wakes this loop).
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

/// One (peer, cache) pair where a live peer advertises a cache this node
/// also has open, under a different [`Mode`].
#[derive(Debug, PartialEq, Eq)]
struct ModeConflict {
    peer: NodeId,
    cache: SmolStr,
    local: Mode,
    remote: Mode,
}

/// The conflicts in `advertised` against `local_modes` that `warned` has
/// not seen yet; each is recorded in `warned`, so a later view reports only
/// pairs new since the last sweep. `warned` accumulates for the life of the
/// cluster rather than clearing entries that stop conflicting: this is a
/// diagnostic trail, not a live status board, and a genuine mismatch is a
/// standing misconfiguration on one side that a re-`open()` (a fresh
/// process, hence a fresh [`NodeId`]) is the normal way to clear.
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

/// Logs a `tracing::error!` for every conflict [`new_mode_conflicts`] finds
/// — the background backstop for what [`CacheBuilder::open`]'s own check
/// can miss under a race.
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

/// Periodically republishes the count of caches open in this process as the
/// `sundog_open_caches` gauge. A gauge, not an event-driven update, because
/// [`crate::cache::CacheBuilder::open`] — the only place a cache is ever
/// added to the registry — lives outside this module's ownership and has no
/// hook to call back into telemetry from.
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

/// Cap on how many already-queued inbound messages [`inbound_loop`] drains
/// per wake — bounds one iteration's work under a multi-peer burst rather
/// than draining an unbounded backlog before dispatching anything (mirrors
/// [`FAN_OUT_DRAIN_CAP`]'s role on the outbound side).
const INBOUND_DRAIN_CAP: usize = 1024;

/// Applies one accumulated same-cache run of `Replicate`/`ReplicateBatch`
/// records under one `apply_remote_batch` call, looking `cache` up in
/// `shard_cache` first (memoized per drained batch by [`inbound_loop`]) so a
/// run's cache name costs at most one `shards` registry lock acquisition no
/// matter how many runs land on it.
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
/// `Invalidate`/`Replicate`/`ReplicateBatch` to the named shard. A cache name
/// with no registered shard is dropped with a trace event rather than an
/// error — the opening side may not have called `open()` for it yet.
///
/// Drains a bounded batch of already-queued messages per wake with
/// `recv_many` (mirroring `fan_out_task`'s own drain-then-dispatch pattern)
/// rather than one `shards` registry lookup per message: within one drained
/// batch, a cache name is looked up at most once, and a run of consecutive
/// same-cache `Replicate`/`ReplicateBatch` messages is coalesced into one
/// `apply_remote_batch` call. `Invalidate` has no batched form, so it is
/// still applied one message at a time, and — like any message that isn't
/// part of the current run — ends whatever replicate run was in progress
/// first. This changes nothing about *what* gets applied or in what order
/// relative to today (every message is still applied exactly once, in the
/// order it was drained): only how many times the registry lock and the
/// per-record apply lock are acquired to do it.
async fn inbound_loop(
    shards: ShardRegistry,
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
                // `Hello` and the request/response messages never reach
                // this channel — `net::Mesh` handles those inline.
                Msg::Hello { .. }
                | Msg::StRequest { .. }
                | Msg::StChunk { .. }
                | Msg::AeDigest { .. }
                | Msg::AeBucket { .. }
                | Msg::AeSketch { .. }
                | Msg::AeEntries { .. }
                | Msg::AePull { .. }
                | Msg::AePullHashes { .. }
                | Msg::ReqDone => {}
            }
        }
        if let Some((cache, recs)) = pending.take() {
            apply_pending_replicate(&shards, &mut shard_cache, cache, recs).await;
        }
    }
}

/// Cap on how many further events [`fan_out_task`] drains in one go past the
/// one it just woke on — bounds one iteration's work under sustained load
/// (e.g. `Cache::insert_many`) rather than draining an unbounded backlog
/// before ever sending anything.
const FAN_OUT_DRAIN_CAP: usize = 1024;

/// Subscribes to one opened cache's local-write notices and fans them out
/// over the mesh per [`Mode`] — the composition-layer half of `Shard`'s
/// design (`store::mod` docs: "`Shard` intentionally holds no handle to
/// `net::Mesh`"). Every local write fans out uniformly, including a
/// `get_or_load` read-through fill: a fresh fill is itself a genuine
/// versioned write (it carries a real `Hlc` stamp), and propagating it lets
/// other `Replicated`-mode peers skip their own loader call — a cache is
/// re-derivable data, so under-propagating costs nothing but an extra
/// loader call elsewhere, never correctness.
///
/// Micro-batches: after waiting for one notice, drains whatever further
/// notices are already available (bounded by [`FAN_OUT_DRAIN_CAP`] notices —
/// one [`FanOutNotice::Many`] carries a whole chunk of keys and still counts
/// as one) before doing any work, so a burst of local writes costs one
/// `ShardOps::records_for` call and one round of per-peer sends for the
/// whole burst, not one of each per write.
pub(crate) async fn fan_out_task<K, V>(
    shard: Arc<Shard<K, V>>,
    cluster: Cluster,
    mut notices: broadcast::Receiver<FanOutNotice<K>>,
    cache_name: SmolStr,
    mode: Mode,
    cancel: CancellationToken,
) where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let push_keys = |keys: &mut Vec<K>, notice: FanOutNotice<K>| match notice {
        FanOutNotice::One(key) => keys.push(key),
        FanOutNotice::Many(chunk) => keys.extend(chunk),
    };
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            received = notices.recv() => {
                let mut keys = Vec::new();
                match received {
                    Ok(notice) => push_keys(&mut keys, notice),
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(
                            cache = %cache_name,
                            skipped,
                            "cache fan-out lagged behind local writes; anti-entropy repairs the gap"
                        );
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
                let mut drained = 1usize;
                while drained < FAN_OUT_DRAIN_CAP {
                    match notices.try_recv() {
                        Ok(notice) => {
                            push_keys(&mut keys, notice);
                            drained += 1;
                        }
                        Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                            tracing::warn!(
                                cache = %cache_name,
                                skipped,
                                "cache fan-out lagged mid-drain; anti-entropy repairs the gap"
                            );
                            break;
                        }
                        Err(_) => break, // Empty (nothing more queued) or Closed (next recv() handles it)
                    }
                }
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
    // The channel carries local writes only (see `store::FanOutNotice`), so
    // no origin filter remains here — just dedup within the drained burst.
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
    // `Hlc`/wire bytes on `Event` itself: a
    // benign race with a fast follow-up write/GC can make a key come back
    // missing, in which case there is nothing stale to fan out for it — a
    // later event (or anti-entropy) covers its current state. Typed keys are
    // already in hand from the events above, so this skips the
    // encode-then-decode round trip `ShardOps::records_for`'s `Bytes`-keyed
    // signature would otherwise need.
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
        // uses for opportunistic coalescing: a drained burst (e.g. an
        // `insert_many` fill) leaves here as a handful of
        // `Msg::ReplicateBatch` frames instead of one `Msg::Replicate` per
        // record — one outbox slot and one encode per ~budget of records,
        // rather than per record, so a bulk burst can't flood the outbox
        // into drop-newest and the anti-entropy repair that follows. The
        // writer-side coalescer still catches what this can't: trickle
        // writes that arrive one drained event at a time.
        Mode::Replicated => (MsgClass::Replicate, batch_replicate(cache_name, records)),
    };
    // Encodes each message exactly once here, before the per-peer loop,
    // rather than once per peer — every live peer then gets a cheap
    // `Bytes` clone of the same frame instead of `net::conn`'s writer
    // independently re-deriving byte-identical content for each of them.
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
    // Resolves each peer's handle once and reuses it for every message in
    // this batch, rather than `Mesh::send`'s per-message peer-table lock
    // acquisition (up to records × peers times per drained fan-out burst).
    for &peer in &peers {
        cluster
            .mesh()
            .send_frames(peer, class, frames.iter().cloned());
    }
}

/// Splits one fan-out burst's records into `Msg::ReplicateBatch` chunks by
/// the same byte budget and count cap `net::conn`'s opportunistic coalescer
/// enforces ([`crate::net::REPLICATE_BATCH_BUDGET`]/
/// [`crate::net::REPLICATE_BATCH_COUNT`]) — sized by each record's
/// single-`Replicate` wire length, the same estimate the coalescer's own
/// budget check runs on. A chunk of exactly one record stays a plain
/// [`Msg::Replicate`], so trickle writes keep their uncoalesced shape.
fn batch_replicate(cache_name: &SmolStr, records: Vec<WireRecord>) -> Vec<Msg> {
    let mut chunks: Vec<Vec<WireRecord>> = Vec::new();
    let mut current: Vec<WireRecord> = Vec::new();
    let mut current_bytes = 0usize;
    for rec in records {
        let rec_bytes = wire::replicate_frame_len(
            cache_name.len(),
            rec.key.len(),
            rec.value.as_ref().map_or(0, Bytes::len),
        );
        if !current.is_empty()
            && (current.len() >= crate::net::REPLICATE_BATCH_COUNT
                || current_bytes + rec_bytes > crate::net::REPLICATE_BATCH_BUDGET)
        {
            chunks.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current_bytes += rec_bytes;
        current.push(rec);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
        .into_iter()
        .map(|mut recs| {
            if recs.len() == 1 {
                Msg::Replicate {
                    cache: cache_name.clone(),
                    rec: recs.pop().expect("invariant: length checked above"),
                }
            } else {
                Msg::ReplicateBatch {
                    cache: cache_name.clone(),
                    recs,
                }
            }
        })
        .collect()
}

/// Periodically garbage-collects one shard's expired tombstones (tombstones
/// must eventually be forgotten) and flushes `moka`'s own
/// housekeeping (`ShardOps::run_pending_tasks`'s docs: without this, a shard
/// that goes quiet right after a TTL/size eviction can keep a stale digest
/// forever). Runs at a quarter of `tombstone_ttl` so a tombstone is never
/// held much past its deadline once nothing defers it.
///
/// `mode` and `absence` together decide, on every tick, whether collection
/// past `tombstone_ttl` is deferred this round (`absence::should_defer_gc`):
/// while any recently-known member is absent from the live peer set, a
/// `Mode::Replicated` cache's tombstone survives past `tombstone_ttl` — up to
/// the hard cap `tombstone_max_ttl` — so that member can't resurrect the
/// deleted entry via anti-entropy once it returns. `Mode::Local` and
/// `Mode::Invalidation` caches are never deferred, and a cluster whose live
/// peer set never shrinks defers nothing either way.
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

/// Publishes `sundog_cache_entries{cache}` for one opened cache: set once
/// immediately, then refreshed on a fixed 5-second cadence from
/// [`Shard::entry_count`] for as long as the cache stays open. Unlike
/// [`open_cache_gauge_task`], this could in principle be event-driven — but
/// `moka`'s own entry count is itself only advisory until pending
/// housekeeping is flushed, so a fixed sampling cadence is what
/// [`Shard::entry_count`] is built around either way.
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

/// Saturating `u64` -> `f64` conversion for gauge values, matching
/// [`set_live_peers_gauge`] and [`open_cache_gauge_task`]'s own count-to-gauge
/// conversions.
fn entry_count_f64(count: u64) -> f64 {
    f64::from(u32::try_from(count).unwrap_or(u32::MAX))
}

// Real-transport-only: these build a live `Cluster` (real `Mesh`, real
// sockets), which panics under `sim` outside a driven `turmoil::Sim` — see
// `net::mod`'s test-module comment for the full rationale.
#[cfg(all(test, not(feature = "sim")))]
mod tests {
    use std::net::Ipv4Addr;

    use super::*;
    use crate::error::CacheError;
    use crate::store::{Event, Origin};

    /// Loopback-only config: skips the outbound-interface probe
    /// `resolve_advertise_ip` would otherwise do for the zeroconf
    /// `0.0.0.0`/`::` default, and keeps anti-entropy/tombstone timing tight
    /// for fast, deterministic tests.
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

    /// Polls `cluster`'s peer set until it sees a live peer advertising
    /// `cache` under some [`Mode`] — the gossip-convergence wait every
    /// mode-mismatch test needs before its second `open()`, since otherwise
    /// that `open()` could race the first node's `cache:<name>` key still in
    /// flight and see no peer advertisement at all.
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

    /// End to end over the real mesh: a per-entry TTL set on node a expires
    /// the replica on node b at the deadline a stamped, while a sibling
    /// entry written with the cache's (absent) default lives on. Neither
    /// cache is opened with a TTL — the override is the only expiry in play.
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

    /// `Mode::Local` publishes no wire message at all — `fan_out_batch`'s
    /// `match mode` above has no arm for it — so there is no "delivered"
    /// event to await and no watch stream to race against; the only
    /// observable proof is polling past a settle window a real cross-node
    /// message would need, then asserting it never showed.
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

        // B warms its own local copy first (Invalidation mode never
        // replicates values). A short sleep before A's write guarantees A's
        // HLC wall-clock stamp is strictly later, so the outcome doesn't
        // hinge on the two nodes' random tie-break `NodeId`s.
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
        // before B ever joined the cluster must already be warm — no
        // waiting for a live write or an anti-entropy round.
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

        // Simulate a dropped `Replicate` message: wipe B's local copy
        // without a tombstone. Only anti-entropy (not live traffic) can
        // bring this back, since nothing writes key 1 again.
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
}

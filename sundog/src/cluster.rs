//! The top-level public entry point: `Cluster::builder(name).build()` forms a
//! working zeroconf cluster, and `cluster.cache(name)` opens named caches on
//! it. Plan §10 — this exact shape is the project's acceptance test.
//!
//! Composition: `build()` resolves the data-plane's advertised address,
//! starts [`Membership`] (gossip), announces via [`Discovery`], then starts
//! [`Mesh`] (the TCP data plane) with a [`RequestHandler`] that answers
//! inbound state-transfer/anti-entropy requests over this cluster's shard
//! registry. Three background tasks, all stopped together by
//! [`Cluster::shutdown`], keep the planes in sync: membership changes flow
//! into `Mesh::update_peers`, inbound wire messages dispatch to shards by
//! cache name, and — spawned per opened cache — local writes fan out over
//! the mesh per [`Mode`].

pub(crate) mod anti_entropy;
pub(crate) mod state_transfer;

use std::collections::HashMap;
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

use crate::cache::CacheBuilder;
use crate::config::ClusterConfig;
use crate::discovery::mdns::Mdns;
use crate::discovery::statics::Static;
use crate::discovery::{Discovery, DiscoveryKind};
use crate::error::JoinError;
use crate::hlc::Hlc;
use crate::membership::{Membership, Peer};
use crate::net::{InboundMsg, Mesh, MsgClass, RequestHandler};
use crate::node::{NodeId, NodeName};
use crate::store::{Event, Mode, Origin, Shard, ShardOps};
use crate::wire::{self, Msg, WireRecord};

/// The cluster's type-erased cache registry: `cache name -> Arc<dyn ShardOps>`
/// (plan §7). Shared between [`Cluster`] itself and the [`RequestHandler`]
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
    config: ClusterConfig,
    tracker: TaskTracker,
    cancel: CancellationToken,
}

/// Builds a [`Cluster`]: own-and-return, per house style. Zero further calls
/// beyond `.build()` must form a working LAN cluster (plan §10) — mDNS
/// discovery, ephemeral ports, and the [`ClusterConfig`] defaults.
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

    pub(crate) fn mesh(&self) -> &Mesh {
        &self.inner.mesh
    }

    /// The live peer set, as membership currently reports it — for
    /// diagnostics (the demo bin's `peers` command; `tracing`/logging).
    #[must_use]
    pub fn peers(&self) -> Vec<Peer> {
        self.inner.membership.peers().borrow().clone()
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
    /// the life of the process, once [`build`](Self::build) succeeds (plan
    /// §12 M7; house rules: "Prometheus exporter implemented behind a
    /// `prometheus` feature flag").
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

    /// Enables mutual TLS on the data-plane mesh (feature `tls`; house rules
    /// "Future plans pulled into v1"). Equivalent to setting
    /// [`ClusterConfig::tls`] directly via [`Self::config`] — a dedicated
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
                config,
                tracker: TaskTracker::new(),
                cancel: CancellationToken::new(),
            }),
        };

        cluster.spawn_tracked(membership_to_mesh_task(
            cluster.inner.membership.peers(),
            cluster.inner.mesh.clone(),
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
}

fn lookup_shard(shards: &ShardRegistry, cache: &SmolStr) -> Option<Arc<dyn ShardOps>> {
    shards
        .read()
        .expect("invariant: shard registry lock is never poisoned")
        .get(cache)
        .cloned()
}

/// Republishes [`Membership::peers`] changes as [`Mesh::update_peers`] calls,
/// for the lifetime of the cluster (plan's composition sketch).
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

/// The single consumer of `Mesh`'s inbound-message channel: dispatches
/// `Invalidate`/`Replicate` to the named shard. A cache name with no
/// registered shard is dropped with a trace event rather than an error — the
/// opening side may simply not have called `open()` for it yet.
async fn inbound_loop(
    shards: ShardRegistry,
    mut inbound: mpsc::Receiver<InboundMsg>,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            received = inbound.recv() => {
                let Some(InboundMsg { from, msg }) = received else { return };
                match msg {
                    Msg::Invalidate { cache, key, ver } => {
                        if let Some(shard) = lookup_shard(&shards, &cache) {
                            shard.invalidate(key, ver).await;
                        } else {
                            tracing::trace!(%cache, %from, "invalidate for unknown cache; dropped");
                        }
                    }
                    Msg::Replicate { cache, rec } => {
                        if let Some(shard) = lookup_shard(&shards, &cache) {
                            shard.apply_remote(rec).await;
                        } else {
                            tracing::trace!(%cache, %from, "replicate for unknown cache; dropped");
                        }
                    }
                    // `Hello` and the request/response messages never reach
                    // this channel — `net::Mesh` handles those inline.
                    Msg::Hello { .. }
                    | Msg::StRequest { .. }
                    | Msg::StChunk { .. }
                    | Msg::AeDigest { .. }
                    | Msg::AeBucket { .. }
                    | Msg::AePull { .. } => {}
                }
            }
        }
    }
}

/// Subscribes to one opened cache's local-write events and fans each one out
/// over the mesh per [`Mode`] — the composition-layer half of `Shard`'s
/// design (`store::mod` docs: "`Shard` intentionally holds no handle to
/// `net::Mesh`"). Every `Origin::Local` event fans out uniformly, including a
/// `get_or_load` read-through fill: a fresh fill is itself a genuine
/// versioned write (it carries a real `Hlc` stamp), and propagating it lets
/// other `Replicated`-mode peers skip their own loader call — consistent with
/// "a cache is re-derivable data" (plan §1), so under-propagating costs
/// nothing but an extra loader call elsewhere, never correctness.
pub(crate) async fn fan_out_task<K, V>(
    shard: Arc<Shard<K, V>>,
    cluster: Cluster,
    mut events: broadcast::Receiver<Event<K, V>>,
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
            received = events.recv() => {
                let event = match received {
                    Ok(event) => event,
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(
                            cache = %cache_name,
                            skipped,
                            "cache fan-out lagged behind local writes; anti-entropy repairs the gap"
                        );
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                };
                fan_out_one(&shard, &cluster, &cache_name, mode, event).await;
            }
        }
    }
}

async fn fan_out_one<K, V>(
    shard: &Shard<K, V>,
    cluster: &Cluster,
    cache_name: &SmolStr,
    mode: Mode,
    event: Event<K, V>,
) where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let (key, origin) = match &event {
        Event::Created { key, origin, .. }
        | Event::Updated { key, origin, .. }
        | Event::Removed { key, origin } => (key, *origin),
    };
    if !matches!(origin, Origin::Local) {
        return;
    }
    let Ok(key_bytes) = postcard::to_stdvec(key) else {
        return;
    };
    // Re-fetches through `ShardOps::records_for` rather than carrying the
    // `Hlc`/wire bytes on `Event` itself (fixed by `docs/INTERFACES.md`): a
    // benign race with a fast follow-up write/GC can make this come back
    // empty, in which case there is nothing stale to fan out — a later event
    // (or anti-entropy) covers the current state.
    let records = ShardOps::records_for(shard, vec![Bytes::from(key_bytes)]).await;
    let Some(rec) = records.into_iter().next() else {
        return;
    };

    let peers = cluster.live_peer_ids();
    match mode {
        Mode::Local => {}
        Mode::Invalidation => {
            let msg = Msg::Invalidate {
                cache: cache_name.clone(),
                key: rec.key,
                ver: rec.ver,
            };
            for peer in peers {
                cluster.mesh().send(peer, MsgClass::Invalidate, msg.clone());
            }
        }
        Mode::Replicated => {
            let msg = Msg::Replicate {
                cache: cache_name.clone(),
                rec,
            };
            for peer in peers {
                cluster.mesh().send(peer, MsgClass::Replicate, msg.clone());
            }
        }
    }
}

/// Periodically garbage-collects one shard's expired tombstones (plan §4:
/// tombstones must eventually be forgotten) and flushes `moka`'s own
/// housekeeping (`ShardOps::run_pending_tasks`'s docs: without this, a shard
/// that goes quiet right after a TTL/size eviction can keep a stale digest
/// forever). Runs at a quarter of `tombstone_ttl` so a tombstone is never
/// held much past its deadline.
pub(crate) async fn tombstone_gc_task(
    shard: Arc<dyn ShardOps>,
    tombstone_ttl: Duration,
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
                shard.gc_tombstones().await;
            }
        }
    }
}

// Real-transport-only: these build a live `Cluster` (real `Mesh`, real
// sockets), which panics under `sim` outside a driven `turmoil::Sim` — see
// `net::mod`'s test-module comment for the full rationale.
#[cfg(all(test, not(feature = "sim")))]
mod tests {
    use std::net::Ipv4Addr;

    use super::*;
    use crate::error::CacheError;

    /// Loopback-only config: skips the outbound-interface probe
    /// `resolve_advertise_ip` would otherwise do for the zeroconf
    /// `0.0.0.0`/`::` default, and keeps anti-entropy/tombstone timing tight
    /// for fast, deterministic tests (plan §11 layer 3).
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

    /// Plan §11.3, layer 3, item 8 (relocated from the deleted
    /// `tests/local_mode_isolation.rs`): `Mode::Local` publishes no wire
    /// message at all — `fan_out_one`'s `match mode` above has no arm for it
    /// — so there is no "delivered" event to await and no watch stream to
    /// race against; the only observable proof is polling past a settle
    /// window a real cross-node message would need, then asserting it never
    /// showed.
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
}

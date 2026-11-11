//! Membership: the chitchat-backed cluster view. Each node gossips its
//! data-plane address, incarnation, and a `cache:<name>` key per open cache
//! holding that cache's [`Mode`]. The live peer set and the per-peer cache
//! modes are published as `watch` streams that drive the net, store, and
//! cluster layers.

use std::collections::{HashMap, HashSet};
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chitchat::transport::UdpTransport;
use chitchat::{
    ChitchatConfig, ChitchatHandle, ChitchatId, FailureDetectorConfig, NodeState, ProtocolVersion,
    spawn_chitchat,
};
use futures::StreamExt;
use futures::stream::{BoxStream, FusedStream};
use smol_str::SmolStr;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time;

use crate::config::ClusterConfig;
use crate::error::JoinError;
use crate::node::{NodeId, NodeName};
use crate::store::Mode;

/// How long `spawn` samples `seeds` to build chitchat's initial static seed
/// list, before handing it to the continuous re-seeding loop. Convergence
/// depends only on that continuous forwarding, letting a cold restart converge.
const INITIAL_SEED_WINDOW: Duration = Duration::from_millis(200);
/// Cap on how many distinct addresses the initial window collects.
const MAX_INITIAL_SEEDS: usize = 16;

const NODE_ID_KEY: &str = "node_id";
const DATA_ADDR_KEY: &str = "data_addr";
const INCARNATION_KEY: &str = "incarnation";
/// The peer's [`crate::wire::PROTOCOL_VERSION`]; absent on a 0.3 node,
/// which speaks protocol 1.
const PROTOCOL_KEY: &str = "protocol";
/// Set by [`Membership::shutdown`] before it tells chitchat to leave, and
/// never cleared afterward since the process exits shortly after. A peer
/// that observes this key before this node drops out of the live set knows
/// the departure is graceful, not a crash; see [`crate::cluster::absence`].
const DEPARTING_KEY: &str = "departing";
/// Prefix for the per-cache mode keys `Membership::set_cache_mode` sets:
/// the full key is `cache:<name>`.
const CACHE_KEY_PREFIX: &str = "cache:";

/// Builds the gossip key one cache's mode is set/read under.
fn cache_key(name: &str) -> String {
    format!("{CACHE_KEY_PREFIX}{name}")
}

/// Prefix for the per-cache keys `Membership::set_loader` sets: the full
/// key is `loader:<name>`, present while this node has `name` open with a
/// registered loader. A node older than protocol 7 reads no such key.
const LOADER_KEY_PREFIX: &str = "loader:";

/// Builds the gossip key that advertises a loader for one cache.
fn loader_key(name: &str) -> String {
    format!("{LOADER_KEY_PREFIX}{name}")
}

/// Prefix for the per-cache keys `Membership::set_refresh` sets: the full
/// key is `refresh:<name>`, present while this node has `name` open with
/// refresh-ahead on, so it takes a peer's refresh hints for it. A node
/// from a release that predates the key never sets it.
const REFRESH_KEY_PREFIX: &str = "refresh:";

/// Builds the gossip key that advertises refresh-ahead for one cache.
fn refresh_key(name: &str) -> String {
    format!("{REFRESH_KEY_PREFIX}{name}")
}

/// One live cluster member as seen through gossip.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    /// The peer's node id.
    pub node: NodeId,
    /// The peer's `{hostname}-{nodeid-hex}` name, chitchat's node id string.
    pub name: NodeName,
    /// The address peers gossip with this node on.
    pub gossip_addr: SocketAddr,
    /// The address the data-plane TCP mesh dials this node on.
    pub data_addr: SocketAddr,
    /// Incremented each time this node leaves and rejoins, distinguishing a
    /// restarted process from a still-live one.
    pub incarnation: u64,
    /// The wire protocol the peer speaks, its
    /// [`crate::wire::PROTOCOL_VERSION`]; `1` for a node whose gossip
    /// state predates the key.
    pub protocol: u16,
}

/// Every cache each live peer advertises as open, keyed by peer, with the
/// [`Mode`] it opened each one under. A cache a peer hasn't opened is
/// absent, never a placeholder value.
pub(crate) type CacheModes = HashMap<NodeId, HashMap<SmolStr, Mode>>;

/// The caches each live peer advertises a loader for, keyed by peer. A peer
/// with no loader for any cache is absent.
pub(crate) type CacheLoaders = HashMap<NodeId, HashSet<SmolStr>>;

/// The caches each live peer advertises refresh-ahead for, keyed by peer.
/// A peer with refresh-ahead on no cache is absent.
pub(crate) type CacheRefreshers = HashMap<NodeId, HashSet<SmolStr>>;

/// A live peer's flags [`cluster::absence`](crate::cluster::absence) needs
/// captured at the instant it drops out of the live set: whether it
/// gossiped a graceful departure ([`DEPARTING_KEY`]).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LiveFlags {
    pub(crate) departing: bool,
}

/// Receives every live set [`Membership::spawn_observed`] publishes; see
/// there.
pub(crate) type LiveObserver = Box<dyn Fn(&HashMap<NodeId, LiveFlags>) + Send>;

/// A request to the background gossip loop, the sole owner of the chitchat
/// handle.
enum Command {
    /// Sets one key of this node's own gossip state.
    SetKey(String, String),
    /// Deletes one key of this node's own gossip state.
    DeleteKey(String),
    Shutdown(oneshot::Sender<()>),
    Crash(oneshot::Sender<()>),
}

/// A cheap-to-clone handle onto a running membership session. Cloning
/// shares the background gossip loop and live-peer view; dropping every
/// clone does not stop it. Call [`Membership::shutdown`] for a graceful
/// chitchat departure.
#[derive(Clone)]
pub struct Membership {
    peers: watch::Receiver<Vec<Peer>>,
    cache_modes: watch::Receiver<CacheModes>,
    cache_loaders: watch::Receiver<CacheLoaders>,
    cache_refreshers: watch::Receiver<CacheRefreshers>,
    local: Peer,
    commands: mpsc::UnboundedSender<Command>,
    /// How many live entries currently report this node's id under another
    /// chitchat id; see [`Membership::twin_count`].
    twins: Arc<AtomicUsize>,
}

impl Membership {
    /// Starts gossip-based membership for one node, returning a handle once
    /// the background gossip loop is running. `seeds` is a continuous
    /// stream of candidate gossip addresses: an initial window seeds
    /// chitchat's static seed list, and every address afterward forwards
    /// to [`ChitchatHandle::gossip`] for this session's lifetime.
    ///
    /// # Errors
    ///
    /// Returns [`JoinError::Bind`] if `config.gossip_bind_addr` cannot be
    /// bound, or [`JoinError::Membership`] if the gossip backend fails to
    /// start.
    pub async fn spawn(
        cluster_name: SmolStr,
        node: NodeId,
        hostname: &str,
        data_addr: SocketAddr,
        config: &ClusterConfig,
        seeds: BoxStream<'static, SocketAddr>,
    ) -> Result<Self, JoinError> {
        Self::spawn_observed(cluster_name, node, hostname, data_addr, config, seeds, None).await
    }

    /// [`Membership::spawn`] with `observer` handed every live set, each
    /// peer with its [`LiveFlags`], before the peers and cache modes of the
    /// same membership change publish: a peer's last flags and its
    /// disappearance arrive together, and an observer never misses a peer
    /// that drops out and returns between two reads of a watch.
    ///
    /// # Errors
    ///
    /// As [`Membership::spawn`].
    pub(crate) async fn spawn_observed(
        cluster_name: SmolStr,
        node: NodeId,
        hostname: &str,
        data_addr: SocketAddr,
        config: &ClusterConfig,
        mut seeds: BoxStream<'static, SocketAddr>,
        observer: Option<LiveObserver>,
    ) -> Result<Self, JoinError> {
        let incarnation = now_incarnation_ms();
        let name = NodeName::new(hostname, node);
        let seed_nodes = collect_initial_seeds(&mut seeds).await;
        let initial_key_values = vec![
            (NODE_ID_KEY.to_string(), node.to_string()),
            (DATA_ADDR_KEY.to_string(), data_addr.to_string()),
            (INCARNATION_KEY.to_string(), incarnation.to_string()),
            (
                PROTOCOL_KEY.to_string(),
                crate::wire::PROTOCOL_VERSION.to_string(),
            ),
        ];

        let (handle, chitchat_id, gossip_advertise_addr) = start_gossip(
            &cluster_name,
            &name,
            incarnation,
            config,
            seed_nodes,
            initial_key_values,
        )
        .await?;

        tracing::info!(
            %cluster_name,
            %node,
            gossip_addr = %gossip_advertise_addr,
            %data_addr,
            "membership started"
        );

        let local = Peer {
            node,
            name,
            gossip_addr: gossip_advertise_addr,
            data_addr,
            incarnation,
            protocol: crate::wire::PROTOCOL_VERSION,
        };

        let (peers_tx, peers_rx) = watch::channel(Vec::new());
        let (cache_modes_tx, cache_modes_rx) = watch::channel(HashMap::new());
        let (cache_loaders_tx, cache_loaders_rx) = watch::channel(HashMap::new());
        let (cache_refreshers_tx, cache_refreshers_rx) = watch::channel(HashMap::new());
        let (commands_tx, commands_rx) = mpsc::unbounded_channel();

        // A departing node's `DEPARTING_KEY` write needs at least one full
        // gossip round to reach every peer before this node leaves; three
        // rounds is generous headroom against a dropped packet or two.
        let departure_notice = config.gossip_interval.saturating_mul(3);

        let twins = Arc::new(AtomicUsize::new(0));
        let publishers = Publishers {
            peers: peers_tx,
            cache_modes: cache_modes_tx,
            cache_loaders: cache_loaders_tx,
            cache_refreshers: cache_refreshers_tx,
            observer,
            twins: Arc::clone(&twins),
        };
        tokio::spawn(run(
            handle,
            seeds,
            publishers,
            LocalIdentity {
                chitchat_id,
                node: local.node,
                name: local.name.clone(),
                incarnation,
            },
            commands_rx,
            departure_notice,
        ));

        Ok(Self {
            peers: peers_rx,
            cache_modes: cache_modes_rx,
            cache_loaders: cache_loaders_rx,
            cache_refreshers: cache_refreshers_rx,
            local,
            commands: commands_tx,
            twins,
        })
    }

    /// This node's own [`Peer`] record, as advertised to the cluster.
    #[must_use]
    pub fn local_peer(&self) -> &Peer {
        &self.local
    }

    /// How many live entries report this node's [`NodeId`] under another
    /// chitchat id: other processes configured with the same persisted id.
    /// A twin is left out of [`Membership::peers`] because the mesh never
    /// dials a node that carries the local id.
    #[must_use]
    pub fn twin_count(&self) -> usize {
        self.twins.load(Ordering::Relaxed)
    }

    /// A live-updating view of the current member set. Downstream
    /// consumers (`net`, `cluster`) `.borrow()` or `.changed().await` this
    /// rather than polling.
    #[must_use]
    pub fn peers(&self) -> watch::Receiver<Vec<Peer>> {
        self.peers.clone()
    }

    /// A live-updating view of every live peer's [`CacheModes`], published
    /// in lockstep with [`Membership::peers`].
    pub(crate) fn cache_modes(&self) -> watch::Receiver<CacheModes> {
        self.cache_modes.clone()
    }

    /// Advertises `mode` as this node's [`Mode`] for cache `name`, under
    /// the `cache:<name>` gossip key. Safe to call unconditionally after
    /// every `open()`; setting the same value twice is a no-op.
    pub(crate) fn set_cache_mode(&self, name: &str, mode: Mode) {
        let _ = self.commands.send(Command::SetKey(
            cache_key(name),
            mode.as_token().to_string(),
        ));
    }

    /// A live-updating view of every live peer's [`CacheLoaders`],
    /// published in lockstep with [`Membership::peers`].
    pub(crate) fn cache_loaders(&self) -> watch::Receiver<CacheLoaders> {
        self.cache_loaders.clone()
    }

    /// Advertises a loader for cache `name` under the `loader:<name>`
    /// gossip key.
    pub(crate) fn set_loader(&self, name: &str) {
        let _ = self
            .commands
            .send(Command::SetKey(loader_key(name), "1".to_string()));
    }

    /// Deletes the `loader:<name>` gossip key. Safe to call on a name never
    /// advertised.
    pub(crate) fn clear_loader(&self, name: &str) {
        let _ = self.commands.send(Command::DeleteKey(loader_key(name)));
    }

    /// A live-updating view of every live peer's [`CacheRefreshers`],
    /// published in lockstep with [`Membership::peers`].
    pub(crate) fn cache_refreshers(&self) -> watch::Receiver<CacheRefreshers> {
        self.cache_refreshers.clone()
    }

    /// Advertises refresh-ahead for cache `name` under the `refresh:<name>`
    /// gossip key.
    pub(crate) fn set_refresh(&self, name: &str) {
        let _ = self
            .commands
            .send(Command::SetKey(refresh_key(name), "1".to_string()));
    }

    /// Deletes the `refresh:<name>` gossip key. Safe to call on a name never
    /// advertised.
    pub(crate) fn clear_refresh(&self, name: &str) {
        let _ = self.commands.send(Command::DeleteKey(refresh_key(name)));
    }

    /// Deletes the `cache:<name>` gossip key, so live peers stop seeing this
    /// node advertise `name` once the deletion propagates. Safe to call on a
    /// name never advertised or already cleared.
    pub(crate) fn clear_cache_mode(&self, name: &str) {
        let _ = self.commands.send(Command::DeleteKey(cache_key(name)));
    }

    /// Gossips a departure, waits three gossip intervals for it to spread,
    /// then leaves the cluster and stops the background gossip loop. Peers
    /// read the departure off the last state they saw, so the leave never
    /// counts as absence; the failure detector still reclaims the silent
    /// node after its grace period.
    pub async fn shutdown(self) {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self.commands.send(Command::Shutdown(reply_tx)).is_ok() {
            let _ = reply_rx.await;
        }
    }

    /// Stops gossiping at once with no departure: peers find out through
    /// the failure detector alone, as they would from a killed process.
    pub async fn crash(self) {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self.commands.send(Command::Crash(reply_tx)).is_ok() {
            let _ = reply_rx.await;
        }
    }
}

/// Samples `seeds` for up to [`INITIAL_SEED_WINDOW`] to build chitchat's
/// static seed list, deduplicating and capping at [`MAX_INITIAL_SEEDS`].
/// Does not exhaust the stream; the caller keeps consuming it afterward.
pub(crate) async fn collect_initial_seeds(
    seeds: &mut BoxStream<'static, SocketAddr>,
) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut seed_nodes = Vec::new();
    let deadline = time::Instant::now() + INITIAL_SEED_WINDOW;
    // Load-bearing: a stream that is always synchronously ready starves
    // `timeout_at`'s own deadline check, spinning this loop forever.
    while seed_nodes.len() < MAX_INITIAL_SEEDS && time::Instant::now() < deadline {
        match time::timeout_at(deadline, seeds.next()).await {
            Ok(Some(addr)) => {
                if seen.insert(addr) {
                    seed_nodes.push(addr.to_string());
                }
            }
            Ok(None) | Err(_) => break,
        }
    }
    seed_nodes
}

/// The advertise IP for a socket bound to `bind_ip`: `config.advertise_ip`
/// verbatim when set (no probe runs at all), otherwise
/// [`resolve_advertise_ip`]'s automatic chain. `pub(crate)`: `cluster.rs`
/// reuses this for the data-plane's address, so both addresses honor the
/// same override.
pub(crate) fn advertise_ip_for(config: &ClusterConfig, bind_ip: IpAddr) -> IpAddr {
    config
        .advertise_ip
        .unwrap_or_else(|| resolve_advertise_ip(bind_ip))
}

/// The address peers use to reach this node's gossip socket, when
/// [`crate::config::ClusterConfig::advertise_ip`] leaves it unset. A
/// concrete bind IP is used as-is; the zeroconf default probes the
/// OS-chosen outbound interface via a UDP "connect" toward a public address
/// (which never sends a packet on a datagram socket). On any probe failure
/// (an unplugged cable, a network with no route to the internet), it falls
/// back to the first non-loopback, non-link-local address `if-addrs`
/// reports for the same family, then to loopback: this never fails.
fn resolve_advertise_ip(bind_ip: IpAddr) -> IpAddr {
    if !bind_ip.is_unspecified() {
        return bind_ip;
    }
    if let Ok(probed) = probe_outbound_ip(bind_ip) {
        return probed;
    }
    let candidates: Vec<IpAddr> = if_addrs::get_if_addrs()
        .map(|interfaces| interfaces.into_iter().map(|iface| iface.ip()).collect())
        .unwrap_or_default();
    fallback_advertise_ip(&candidates, bind_ip.is_ipv6())
}

/// Starts chitchat on `config.gossip_bind_addr` and returns its handle, the
/// id it advertises and the gossip address inside it. With port 0, a probe
/// bind claims a free port and releases it for chitchat's own transport,
/// whose bind of that port can lose a race against another process, or
/// another node in the same test binary, taking it in between: chitchat's
/// address-in-use answer sends the loop back to probe a fresh port, up to
/// [`GOSSIP_BIND_ATTEMPTS`] times. A fixed port never moves.
pub(crate) async fn start_gossip(
    cluster_name: &SmolStr,
    name: &NodeName,
    incarnation: u64,
    config: &ClusterConfig,
    seed_nodes: Vec<String>,
    initial_key_values: Vec<(String, String)>,
) -> Result<(ChitchatHandle, ChitchatId, SocketAddr), JoinError> {
    let bind_addr = config.gossip_bind_addr;
    let mut attempt: u32 = 0;
    loop {
        let port = if bind_addr.port() == 0 {
            let probe = tokio::net::UdpSocket::bind(bind_addr)
                .await
                .map_err(|source| JoinError::Bind {
                    addr: bind_addr,
                    source,
                })?;
            probe
                .local_addr()
                .map_err(|source| JoinError::Bind {
                    addr: bind_addr,
                    source,
                })?
                .port()
        } else {
            bind_addr.port()
        };
        let listen_addr = SocketAddr::new(bind_addr.ip(), port);
        let advertise_ip = advertise_ip_for(config, listen_addr.ip());
        let gossip_advertise_addr = SocketAddr::new(advertise_ip, port);
        let chitchat_id = ChitchatId::new(name.to_string(), incarnation, gossip_advertise_addr);

        let chitchat_config = ChitchatConfig {
            chitchat_id: chitchat_id.clone(),
            cluster_id: cluster_name.to_string(),
            gossip_interval: config.gossip_interval,
            listen_addr,
            seed_nodes: seed_nodes.clone(),
            failure_detector_config: FailureDetectorConfig {
                phi_threshold: config.phi_threshold,
                sampling_window_size: config.phi_sampling_window_size,
                max_interval: config.phi_max_interval,
                initial_interval: config.phi_initial_interval,
                dead_node_grace_period: config.dead_node_grace_period,
            },
            marked_for_deletion_grace_period: config.kv_tombstone_grace_period,
            catchup_callback: None,
            extra_liveness_predicate: None,
            protocol_version: ProtocolVersion::V0,
        };

        match spawn_chitchat(chitchat_config, initial_key_values.clone(), &UdpTransport).await {
            Ok(handle) => return Ok((handle, chitchat_id, gossip_advertise_addr)),
            Err(err) => {
                let lost_the_port = err
                    .chain()
                    .any(|cause| is_addr_in_use(cause.downcast_ref::<io::Error>()));
                if should_retry_gossip_bind(bind_addr.port(), lost_the_port, attempt) {
                    attempt += 1;
                    tracing::warn!(
                        %listen_addr,
                        attempt,
                        "gossip port taken between its probe and chitchat's bind; probing another"
                    );
                    continue;
                }
                return Err(JoinError::Membership(Box::new(io::Error::other(format!(
                    "{err:#}"
                )))));
            }
        }
    }
}

/// Fresh ports [`Membership::spawn`] probes for a port-0 bind before giving
/// up: each attempt loses only to a bind landing on the probed port inside
/// the microseconds between the probe's release and chitchat's own bind,
/// so a handful of tries outlasts any plausible run of collisions.
const GOSSIP_BIND_ATTEMPTS: u32 = 5;

/// Whether `error` is the kernel refusing a bind because the address is
/// taken (`EADDRINUSE`, `WSAEADDRINUSE` on Windows).
fn is_addr_in_use(error: Option<&io::Error>) -> bool {
    error.is_some_and(|error| error.kind() == io::ErrorKind::AddrInUse)
}

/// Pure: whether [`Membership::spawn`] probes another port after chitchat's
/// bind failed. Only a port-0 request moves (a fixed `requested_port` is
/// the operator's choice, and a taken one is an error to report), only for
/// an address-in-use failure (anything else is not a race), and only while
/// `attempt` is under [`GOSSIP_BIND_ATTEMPTS`].
fn should_retry_gossip_bind(requested_port: u16, addr_in_use: bool, attempt: u32) -> bool {
    requested_port == 0 && addr_in_use && attempt < GOSSIP_BIND_ATTEMPTS
}

/// The outbound-interface probe: a UDP "connect" toward a public address,
/// which never sends a packet on a datagram socket but makes the kernel pick
/// a source address as though about to send one.
fn probe_outbound_ip(bind_ip: IpAddr) -> io::Result<IpAddr> {
    let probe_target: SocketAddr = if bind_ip.is_ipv6() {
        (
            Ipv6Addr::new(0x2001, 0x4860, 0x4860, 0, 0, 0, 0, 0x8888),
            80,
        )
            .into()
    } else {
        (Ipv4Addr::new(8, 8, 8, 8), 80).into()
    };
    let probe = std::net::UdpSocket::bind((bind_ip, 0))?;
    probe.connect(probe_target)?;
    probe.local_addr().map(|addr| addr.ip())
}

/// The pure fallback order [`resolve_advertise_ip`] applies once its outbound
/// probe fails: the first `candidates` entry of the requested family (`true`
/// for IPv6) that is neither loopback nor link-local, or that family's
/// loopback address if none qualifies.
fn fallback_advertise_ip(candidates: &[IpAddr], want_ipv6: bool) -> IpAddr {
    candidates
        .iter()
        .copied()
        .find(|ip| ip.is_ipv6() == want_ipv6 && !ip.is_loopback() && !is_link_local(ip))
        .unwrap_or(if want_ipv6 {
            IpAddr::V6(Ipv6Addr::LOCALHOST)
        } else {
            IpAddr::V4(Ipv4Addr::LOCALHOST)
        })
}

fn is_link_local(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_link_local(),
        IpAddr::V6(v6) => v6.is_unicast_link_local(),
    }
}

/// Whether `state` carries [`DEPARTING_KEY`], i.e. its owner gossiped a
/// graceful departure before leaving.
pub(crate) fn is_departing(state: &NodeState) -> bool {
    state.get(DEPARTING_KEY).is_some()
}

pub(crate) fn now_incarnation_ms() -> u64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
}

/// Reconstructs a [`Peer`] from a live chitchat member, or `None` if its
/// gossiped state is missing or malformed. Such peers are excluded rather
/// than surfaced as an error; they appear once their state catches up.
pub(crate) fn parse_peer(chitchat_id: &ChitchatId, node_state: &NodeState) -> Option<Peer> {
    let node_id_value = u64::from_str_radix(node_state.get(NODE_ID_KEY)?, 16).ok()?;
    let node = NodeId::from(node_id_value);

    // Strip the known `-{node_id}` suffix rather than splitting on the
    // last `-`, so a hyphenated hostname round-trips.
    let suffix = format!("-{node}");
    let hostname = chitchat_id.node_id.strip_suffix(suffix.as_str())?;
    let name = NodeName::new(hostname, node);

    let data_addr: SocketAddr = node_state.get(DATA_ADDR_KEY)?.parse().ok()?;
    let incarnation: u64 = node_state.get(INCARNATION_KEY)?.parse().ok()?;
    let protocol: u16 = node_state
        .get(PROTOCOL_KEY)
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(1);

    Some(Peer {
        node,
        name,
        gossip_addr: chitchat_id.gossip_advertise_addr,
        data_addr,
        incarnation,
        protocol,
    })
}

/// What a peer's protocol version means for this node, for the one log line
/// per peer [`Membership`] emits when a view first shows it: `None` when the
/// peer is fully served.
fn protocol_notice(peer_protocol: u16) -> Option<&'static str> {
    if peer_protocol > crate::wire::PROTOCOL_VERSION {
        Some(
            "peer speaks a newer protocol; it limits itself to what this node understands, upgrade this node",
        )
    } else if peer_protocol < crate::wire::MIN_PROTOCOL_VERSION {
        Some("peer speaks a protocol this node no longer serves; upgrade the peer")
    } else {
        None
    }
}

/// Drops the notice of every peer not in `live`.
fn forget_departed_notices(noticed: &mut HashSet<NodeId>, live: &HashMap<NodeId, LiveFlags>) {
    noticed.retain(|peer| live.contains_key(peer));
}

/// The cache modes `node_state` advertises, or none once it gossips a
/// departure: a gracefully leaving node stops being eligible to own any
/// `Mode::Distributed` cache the moment it announces, so ownership moves
/// while the rest of the cluster still holds everything it did, instead of
/// waiting for the failure detector to drop the node from the live set.
fn advertised_cache_modes(node_state: &NodeState) -> HashMap<SmolStr, Mode> {
    if is_departing(node_state) {
        return HashMap::new();
    }
    parse_cache_modes(node_state)
}

/// The caches `node_state` advertises a loader for: none once it gossips a
/// departure, like [`advertised_cache_modes`].
fn advertised_loaders(node_state: &NodeState) -> HashSet<SmolStr> {
    if is_departing(node_state) {
        return HashSet::new();
    }
    parse_loaders(node_state)
}

/// The caches `node_state` advertises refresh-ahead for: none once it
/// gossips a departure, like [`advertised_loaders`].
fn advertised_refreshers(node_state: &NodeState) -> HashSet<SmolStr> {
    if is_departing(node_state) {
        return HashSet::new();
    }
    parse_refreshers(node_state)
}

/// Reads every `refresh:<name>` key off `node_state`.
pub(crate) fn parse_refreshers(node_state: &NodeState) -> HashSet<SmolStr> {
    node_state
        .iter_prefix(REFRESH_KEY_PREFIX)
        .map(|(key, _)| {
            SmolStr::new(
                key.strip_prefix(REFRESH_KEY_PREFIX)
                    .expect("invariant: iter_prefix only yields keys starting with the prefix"),
            )
        })
        .collect()
}

/// Reads every `loader:<name>` key off `node_state`.
pub(crate) fn parse_loaders(node_state: &NodeState) -> HashSet<SmolStr> {
    node_state
        .iter_prefix(LOADER_KEY_PREFIX)
        .map(|(key, _)| {
            SmolStr::new(
                key.strip_prefix(LOADER_KEY_PREFIX)
                    .expect("invariant: iter_prefix only yields keys starting with the prefix"),
            )
        })
        .collect()
}

/// Reads every `cache:<name>` key off `node_state` into a `name -> Mode`
/// map, logging and skipping any value that isn't a recognized [`Mode`] token.
pub(crate) fn parse_cache_modes(node_state: &NodeState) -> HashMap<SmolStr, Mode> {
    node_state
        .iter_prefix(CACHE_KEY_PREFIX)
        .filter_map(|(key, versioned_value)| {
            let name = key
                .strip_prefix(CACHE_KEY_PREFIX)
                .expect("invariant: iter_prefix only yields keys starting with the prefix");
            let mode = Mode::from_token(&versioned_value.value);
            if mode.is_none() {
                tracing::warn!(
                    cache = %name,
                    token = %versioned_value.value,
                    "unrecognized cache mode token in gossip state; skipped"
                );
            }
            mode.map(|mode| (SmolStr::new(name), mode))
        })
        .collect()
}

/// A chitchat id and node state shaped as [`Membership::spawn`] publishes
/// them for `peer`, with a `cache:<name>` key per entry of `caches` and the
/// departure key when `departing`: what [`parse_peer`] and
/// [`parse_cache_modes`] read back.
#[cfg(test)]
pub(crate) fn published_state_for_test(
    peer: &Peer,
    caches: &[(&str, Mode)],
    departing: bool,
) -> (ChitchatId, NodeState) {
    let id = ChitchatId::new(peer.name.to_string(), peer.incarnation, peer.gossip_addr);
    let mut state = NodeState::for_test();
    state.set(NODE_ID_KEY, peer.node.to_string());
    state.set(DATA_ADDR_KEY, peer.data_addr.to_string());
    state.set(INCARNATION_KEY, peer.incarnation.to_string());
    state.set(PROTOCOL_KEY, peer.protocol.to_string());
    for (name, mode) in caches {
        state.set(cache_key(name), mode.as_token().as_str());
    }
    if departing {
        state.set(DEPARTING_KEY, "1");
    }
    (id, state)
}

/// One other member's state, as this node currently sees it, for the CRDT
/// writer-retirement quiet and dead-incarnation predicates below. Built
/// from [`Peer`]/[`crate::cluster::absence::AbsenceTracker`] state by the
/// caller; the predicates take it as plain data so they're testable
/// without a live cluster.
#[derive(Debug, Clone, Copy)]
pub(crate) struct MemberView {
    /// `Some(since)` when the member is currently live in this node's view,
    /// continuously present since `since`
    /// ([`crate::cluster::absence::AbsenceTracker::present_since`]); `None`
    /// when currently tracked absent. Mutually exclusive with
    /// `absent_since`.
    pub(crate) present_since: Option<Instant>,
    /// `Some(since)` when the member is currently tracked gone (dropped
    /// from the live set, by a crash or a graceful departure) since
    /// `since` ([`crate::cluster::absence::AbsenceTracker::gone_since`]);
    /// `None` when live or never observed.
    pub(crate) absent_since: Option<Instant>,
    /// The member's current incarnation, when it is live; `None` when
    /// gone.
    pub(crate) live_incarnation: Option<u64>,
    /// When the member is first observed live
    /// ([`crate::cluster::absence::AbsenceTracker::known_since`]); `None`
    /// if never.
    pub(crate) known_since: Option<Instant>,
}

/// Whether `member` has settled for the CRDT writer-retirement "cache is
/// quiet" rule: continuously present for at least `bound`, continuously
/// gone for at least `bound`, or known for at least twice `bound` however
/// often it has come and gone in between. A member that is none of these,
/// one recently seen for the first time that has already moved, defers
/// stage two for every writer in the cache, since its own view of
/// who's-seen-what can't yet be trusted as settled. The known-for-two-bounds
/// clause keeps one member flapping faster than the bound from holding
/// stage two back for good: any copy such a member can still bring back
/// is at most a bound old, reconciled by the retired entry the fold
/// replaces or by the fold receipt it leaves for three bounds past the fold.
pub(crate) fn member_is_quiet(member: &MemberView, now: Instant, bound: Duration) -> bool {
    let settled = |since: Option<Instant>, for_at_least: Duration| {
        since.is_some_and(|since| now.saturating_duration_since(since) >= for_at_least)
    };
    settled(member.present_since, bound)
        || settled(member.absent_since, bound)
        || settled(member.known_since, bound.saturating_mul(2))
}

/// Whether writer incarnation `w_incarnation` is dead on `member`'s node:
/// `member` has been absent longer than `bound`, or `member` is live with
/// a *different* current incarnation than `w_incarnation` (any difference
/// counts, not only a greater one, so a clock stepping backward across a
/// restart can't pin an old incarnation live forever). A member with no
/// live incarnation and no absent time, one never observed at all, is not
/// dead: no information means not dead. The CRDT sweep avoids that state for
/// the writer of a node it never observed by passing the later of the
/// process start and its first sight of the writer as `absent_since`.
pub(crate) fn incarnation_is_dead(
    member: &MemberView,
    w_incarnation: u64,
    now: Instant,
    bound: Duration,
) -> bool {
    match member.live_incarnation {
        Some(current) => current != w_incarnation,
        None => member
            .absent_since
            .is_some_and(|since| now.saturating_duration_since(since) >= bound),
    }
}

/// Owns the chitchat handle for one membership session: forwards discovered
/// addresses into gossip, republishes live-set changes as `Vec<Peer>`, and
/// performs shutdown on request. Spawned once, so `ChitchatHandle::shutdown`
/// has exactly one owner regardless of how many [`Membership`] clones exist.
/// Where [`run`] republishes membership state, bundled to keep its argument
/// count down: two watch channels, and the observer that sees every live
/// set first.
struct Publishers {
    peers: watch::Sender<Vec<Peer>>,
    cache_modes: watch::Sender<CacheModes>,
    cache_loaders: watch::Sender<CacheLoaders>,
    cache_refreshers: watch::Sender<CacheRefreshers>,
    observer: Option<LiveObserver>,
    twins: Arc<AtomicUsize>,
}

/// This node's identity as [`run`] tells it apart from the other live
/// entries.
struct LocalIdentity {
    chitchat_id: ChitchatId,
    node: NodeId,
    name: NodeName,
    incarnation: u64,
}

/// One membership view split by [`split_live`].
#[derive(Debug, PartialEq, Eq)]
struct LiveSplit<T> {
    /// One entry per remote node: the highest incarnation.
    live: Vec<T>,
    /// The entries carrying the local node id that are not this node's own
    /// earlier incarnation.
    twins: Vec<T>,
}

/// Splits the live entries other than this node's own chitchat id into the
/// peers handed to consumers and the twins of this node.
///
/// An entry with the local node id and the local name (`{hostname}-{id}`)
/// but a lower incarnation is this node's own previous generation, still
/// live because the failure detector has not reaped it; it is dropped. Any
/// other entry with the local node id is a twin: another process configured
/// with the same persisted id. Of several entries for one remote node, a
/// restart that outran the failure detector, only the highest incarnation
/// stays, so consumers dial the generation that is running; a tie on
/// incarnation goes to the greater gossip address, so the pick does not
/// depend on iteration order. The order of the first appearance of each node
/// is kept.
fn split_live<T>(
    entries: Vec<T>,
    peer_of: impl Fn(&T) -> &Peer,
    local: &LocalIdentity,
) -> LiveSplit<T> {
    let mut twins = Vec::new();
    let mut live: Vec<T> = Vec::new();
    for entry in entries {
        let peer = peer_of(&entry);
        if peer.node == local.node {
            if !(peer.name == local.name && peer.incarnation < local.incarnation) {
                twins.push(entry);
            }
        } else if let Some(kept) = live.iter_mut().find(|kept| peer_of(kept).node == peer.node) {
            let (old, new) = (peer_of(kept), peer);
            if (new.incarnation, new.gossip_addr) > (old.incarnation, old.gossip_addr) {
                *kept = entry;
            }
        } else {
            live.push(entry);
        }
    }
    LiveSplit { live, twins }
}

/// The identity of one twin entry, so a twin is logged once while it stays
/// live.
type TwinKey = (NodeName, u64, SocketAddr);

/// The keys of `twins` not in `noticed`, which then holds exactly the keys of
/// `twins`: a twin that leaves the view is forgotten and logged again if it
/// returns.
fn fresh_twins(noticed: &mut HashSet<TwinKey>, twins: &[Peer]) -> Vec<Peer> {
    let current: HashSet<TwinKey> = twins.iter().map(twin_key).collect();
    noticed.retain(|key| current.contains(key));
    twins
        .iter()
        .filter(|twin| noticed.insert(twin_key(twin)))
        .cloned()
        .collect()
}

fn twin_key(twin: &Peer) -> TwinKey {
    (twin.name.clone(), twin.incarnation, twin.gossip_addr)
}

/// The remote live entries of one membership view, one per node, parsed with
/// their gossip state. Reports each new twin of this node once at `error` and
/// stores the number of twins in `twin_count`.
fn view_entries<'a>(
    live: impl IntoIterator<Item = (&'a ChitchatId, &'a NodeState)>,
    local: &LocalIdentity,
    twins_noticed: &mut HashSet<TwinKey>,
    twin_count: &AtomicUsize,
) -> Vec<(Peer, &'a NodeState)> {
    let entries: Vec<(Peer, &NodeState)> = live
        .into_iter()
        .filter(|(id, _)| *id != &local.chitchat_id)
        .filter_map(|(id, state)| Some((parse_peer(id, state)?, state)))
        .collect();
    let LiveSplit {
        live: entries,
        twins,
    } = split_live(entries, |(peer, _)| peer, local);
    let twins: Vec<Peer> = twins.into_iter().map(|(peer, _)| peer).collect();
    for twin in fresh_twins(twins_noticed, &twins) {
        tracing::error!(
            node = %local.node,
            twin_name = %twin.name,
            twin_gossip_addr = %twin.gossip_addr,
            "another live process uses this node's id; it is never dialed and no data flows to it, give each node its own id",
        );
    }
    twin_count.store(twins.len(), Ordering::Relaxed);
    entries
}

async fn run(
    handle: ChitchatHandle,
    seeds: BoxStream<'static, SocketAddr>,
    publishers: Publishers,
    local: LocalIdentity,
    mut commands_rx: mpsc::UnboundedReceiver<Command>,
    departure_notice: Duration,
) {
    let Publishers {
        peers: peers_tx,
        cache_modes: cache_modes_tx,
        cache_loaders: cache_loaders_tx,
        cache_refreshers: cache_refreshers_tx,
        observer,
        twins: twin_count,
    } = publishers;
    let mut seeds = seeds.fuse();
    let chitchat = handle.chitchat();
    let mut live_nodes = chitchat.lock().await.live_nodes_watch_stream().fuse();
    // Peers whose protocol version has been logged once, so a view refresh
    // does not repeat the notice; a peer that leaves the view is dropped and
    // noticed again when it returns.
    let mut protocol_noticed: HashSet<NodeId> = HashSet::new();
    // Twins already reported, so each is logged once while it stays live.
    let mut twins_noticed: HashSet<TwinKey> = HashSet::new();

    loop {
        tokio::select! {
            // The arm retires when the discovery stream ends. The contract
            // forbids that and a custom source can still do it;
            // `select_next_some` would panic on the poll after the end.
            Some(addr) = seeds.next(), if !seeds.is_terminated() => {
                if let Err(error) = handle.gossip(addr) {
                    tracing::debug!(%error, %addr, "failed to queue gossip with discovered peer");
                }
            }
            live = live_nodes.select_next_some() => {
                let mut peers: Vec<Peer> = Vec::new();
                let mut cache_modes: CacheModes = HashMap::new();
                let mut cache_loaders: CacheLoaders = HashMap::new();
                let mut cache_refreshers: CacheRefreshers = HashMap::new();
                let mut live_flags: HashMap<NodeId, LiveFlags> = HashMap::new();
                let entries = view_entries(&live, &local, &mut twins_noticed, &twin_count);
                for (peer, state) in entries {
                    if let Some(notice) = protocol_notice(peer.protocol)
                        && protocol_noticed.insert(peer.node)
                    {
                        tracing::warn!(peer = %peer.node, peer_protocol = peer.protocol, "{notice}");
                    }
                    cache_modes.insert(peer.node, advertised_cache_modes(state));
                    let loaders = advertised_loaders(state);
                    if !loaders.is_empty() {
                        cache_loaders.insert(peer.node, loaders);
                    }
                    let refreshers = advertised_refreshers(state);
                    if !refreshers.is_empty() {
                        cache_refreshers.insert(peer.node, refreshers);
                    }
                    live_flags.insert(
                        peer.node,
                        LiveFlags {
                            departing: is_departing(state),
                        },
                    );
                    peers.push(peer);
                }
                forget_departed_notices(&mut protocol_noticed, &live_flags);
                tracing::debug!(count = peers.len(), "membership view updated");
                // The observer first: an ownership view built from the new
                // peers or modes then never runs ahead of the live set
                // `Cluster::away_within` reads.
                if let Some(observer) = &observer {
                    observer(&live_flags);
                }
                let _ = cache_loaders_tx.send(cache_loaders);
                let _ = cache_refreshers_tx.send(cache_refreshers);
                let _ = cache_modes_tx.send(cache_modes);
                let _ = peers_tx.send(peers);
            }
            command = commands_rx.recv() => {
                match command {
                    Some(Command::SetKey(key, value)) => {
                        chitchat.lock().await.self_node_state().set(key, value);
                    }
                    Some(Command::DeleteKey(key)) => {
                        chitchat.lock().await.self_node_state().delete(&key);
                    }
                    Some(Command::Shutdown(reply)) => {
                        // Gossips the departure before leaving, and waits
                        // for it to reach peers: `AbsenceTracker` reads it
                        // off the last state a peer had before dropping
                        // out of the live set, never counting a graceful
                        // leave as absence.
                        chitchat
                            .lock()
                            .await
                            .self_node_state()
                            .set(DEPARTING_KEY, "1");
                        time::sleep(departure_notice).await;
                        if let Err(error) = handle.shutdown().await {
                            tracing::warn!(%error, "chitchat shutdown reported an error");
                        }
                        tracing::info!("membership stopped");
                        let _ = reply.send(());
                        return;
                    }
                    Some(Command::Crash(reply)) => {
                        handle.abort();
                        let _ = reply.send(());
                        return;
                    }
                    None => {
                        // Every `Membership` clone (hence its `commands`
                        // sender) is gone with no graceful `shutdown()`
                        // call: a crash. Abort the chitchat server outright
                        // rather than leaving it gossiping on, detached from
                        // anything this process still holds.
                        handle.abort();
                        return;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;

/// Kani proof over the gossip bind retry rule.
#[cfg(kani)]
mod kani_proofs {
    use super::*;

    /// Only a port-0 request retries, only on address-in-use, and never past the attempt cap.
    #[kani::proof]
    fn gossip_bind_retries_only_a_port_zero_request_within_the_cap() {
        let requested_port: u16 = kani::any();
        let addr_in_use: bool = kani::any();
        let attempt: u32 = kani::any();
        let retry = should_retry_gossip_bind(requested_port, addr_in_use, attempt);
        assert_eq!(
            retry,
            requested_port == 0 && addr_in_use && attempt < GOSSIP_BIND_ATTEMPTS
        );
    }
}

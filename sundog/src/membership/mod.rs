//! Membership: the chitchat-backed cluster view. Each node gossips its
//! data-plane address, incarnation, and a `cache:<name>` key per open cache
//! holding that cache's [`Mode`]. The live peer set and the per-peer cache
//! modes are published as `watch` streams that drive the net, store, and
//! cluster layers.

use std::collections::{HashMap, HashSet};
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chitchat::transport::UdpTransport;
use chitchat::{
    ChitchatConfig, ChitchatHandle, ChitchatId, FailureDetectorConfig, NodeState, ProtocolVersion,
    spawn_chitchat,
};
use futures::StreamExt;
use futures::stream::BoxStream;
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
/// the departure was graceful, not a crash; see [`crate::cluster::absence`].
const DEPARTING_KEY: &str = "departing";
/// Prefix for the per-cache mode keys `Membership::set_cache_mode` sets:
/// the full key is `cache:<name>`.
const CACHE_KEY_PREFIX: &str = "cache:";

/// Builds the gossip key one cache's mode is set/read under.
fn cache_key(name: &str) -> String {
    format!("{CACHE_KEY_PREFIX}{name}")
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

/// A request to the background gossip loop, the sole owner of the chitchat
/// handle.
enum Command {
    SetCacheMode(SmolStr, Mode),
    ClearCacheMode(SmolStr),
    Shutdown(oneshot::Sender<()>),
}

/// A cheap-to-clone handle onto a running membership session. Cloning
/// shares the background gossip loop and live-peer view; dropping every
/// clone does not stop it. Call [`Membership::shutdown`] for a graceful
/// chitchat departure.
#[derive(Clone)]
pub struct Membership {
    peers: watch::Receiver<Vec<Peer>>,
    cache_modes: watch::Receiver<CacheModes>,
    /// Which live peers have gossiped [`DEPARTING_KEY`], published in
    /// lockstep with `peers`; `pub(crate)`: only `cluster::absence` consumes
    /// it, via [`Membership::departing_flags`].
    departing: watch::Receiver<HashMap<NodeId, bool>>,
    local: Peer,
    commands: mpsc::UnboundedSender<Command>,
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
        mut seeds: BoxStream<'static, SocketAddr>,
    ) -> Result<Self, JoinError> {
        let bind_addr = config.gossip_bind_addr;
        let port = if bind_addr.port() == 0 {
            // Probe-bind to claim a free port, then release it for
            // chitchat's own transport. The resulting race is accepted on a
            // trusted LAN.
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

        let incarnation = now_incarnation_ms();
        let name = NodeName::new(hostname, node);
        let chitchat_id = ChitchatId::new(name.to_string(), incarnation, gossip_advertise_addr);

        let seed_nodes = collect_initial_seeds(&mut seeds).await;

        let chitchat_config = ChitchatConfig {
            chitchat_id: chitchat_id.clone(),
            cluster_id: cluster_name.to_string(),
            gossip_interval: config.gossip_interval,
            listen_addr,
            seed_nodes,
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

        let initial_key_values = vec![
            (NODE_ID_KEY.to_string(), node.to_string()),
            (DATA_ADDR_KEY.to_string(), data_addr.to_string()),
            (INCARNATION_KEY.to_string(), incarnation.to_string()),
            (
                PROTOCOL_KEY.to_string(),
                crate::wire::PROTOCOL_VERSION.to_string(),
            ),
        ];

        let handle = spawn_chitchat(chitchat_config, initial_key_values, &UdpTransport)
            .await
            .map_err(|err| JoinError::Membership(Box::new(io::Error::other(format!("{err:#}")))))?;

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
        let (departing_tx, departing_rx) = watch::channel(HashMap::new());
        let (commands_tx, commands_rx) = mpsc::unbounded_channel();

        // A departing node's `DEPARTING_KEY` write needs at least one full
        // gossip round to reach every peer before this node actually leaves;
        // three rounds is generous headroom against a dropped packet or two.
        let departure_notice = config.gossip_interval.saturating_mul(3);

        let publishers = Publishers {
            peers: peers_tx,
            cache_modes: cache_modes_tx,
            departing: departing_tx,
        };
        tokio::spawn(run(
            handle,
            seeds,
            publishers,
            chitchat_id,
            commands_rx,
            departure_notice,
        ));

        Ok(Self {
            peers: peers_rx,
            cache_modes: cache_modes_rx,
            departing: departing_rx,
            local,
            commands: commands_tx,
        })
    }

    /// This node's own [`Peer`] record, as advertised to the cluster.
    #[must_use]
    pub fn local_peer(&self) -> &Peer {
        &self.local
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

    /// The live peers, each with whether it has gossiped a graceful
    /// departure; the absence tracker's only input, so a peer's last flag
    /// and its disappearance arrive together.
    pub(crate) fn departing_flags(&self) -> watch::Receiver<HashMap<NodeId, bool>> {
        self.departing.clone()
    }

    /// Advertises `mode` as this node's [`Mode`] for cache `name`, under
    /// the `cache:<name>` gossip key. Safe to call unconditionally after
    /// every `open()`; setting the same value twice is a no-op.
    pub(crate) fn set_cache_mode(&self, name: &str, mode: Mode) {
        let _ = self
            .commands
            .send(Command::SetCacheMode(SmolStr::new(name), mode));
    }

    /// Deletes the `cache:<name>` gossip key, so live peers stop seeing this
    /// node advertise `name` once the deletion propagates. Safe to call on a
    /// name never advertised or already cleared.
    pub(crate) fn clear_cache_mode(&self, name: &str) {
        let _ = self
            .commands
            .send(Command::ClearCacheMode(SmolStr::new(name)));
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
}

/// Samples `seeds` for up to [`INITIAL_SEED_WINDOW`] to build chitchat's
/// static seed list, deduplicating and capping at [`MAX_INITIAL_SEEDS`].
/// Does not exhaust the stream; the caller keeps consuming it afterward.
async fn collect_initial_seeds(seeds: &mut BoxStream<'static, SocketAddr>) -> Vec<String> {
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
/// (which never sends a packet on a datagram socket), and, on any probe
/// failure — an unplugged cable, a network with no route to the internet —
/// falls back to the first non-loopback, non-link-local address `if-addrs`
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

/// The outbound-interface probe: a UDP "connect" toward a public address,
/// which never sends a packet on a datagram socket but makes the kernel pick
/// a source address as if it were about to.
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
fn is_departing(state: &NodeState) -> bool {
    state.get(DEPARTING_KEY).is_some()
}

fn now_incarnation_ms() -> u64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
}

/// Reconstructs a [`Peer`] from a live chitchat member, or `None` if its
/// gossiped state is missing or malformed. Such peers are excluded rather
/// than surfaced as an error; they appear once their state catches up.
fn parse_peer(chitchat_id: &ChitchatId, node_state: &NodeState) -> Option<Peer> {
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

/// Reads every `cache:<name>` key off `node_state` into a `name -> Mode`
/// map, logging and skipping any value that isn't a recognized [`Mode`] token.
fn parse_cache_modes(node_state: &NodeState) -> HashMap<SmolStr, Mode> {
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

/// Owns the chitchat handle for one membership session: forwards discovered
/// addresses into gossip, republishes live-set changes as `Vec<Peer>`, and
/// performs shutdown on request. Spawned once, so `ChitchatHandle::shutdown`
/// has exactly one owner regardless of how many [`Membership`] clones exist.
/// The three watch channels [`run`] republishes membership state on, bundled
/// to keep its argument count down.
struct Publishers {
    peers: watch::Sender<Vec<Peer>>,
    cache_modes: watch::Sender<CacheModes>,
    departing: watch::Sender<HashMap<NodeId, bool>>,
}

async fn run(
    handle: ChitchatHandle,
    seeds: BoxStream<'static, SocketAddr>,
    publishers: Publishers,
    self_chitchat_id: ChitchatId,
    mut commands_rx: mpsc::UnboundedReceiver<Command>,
    departure_notice: Duration,
) {
    let Publishers {
        peers: peers_tx,
        cache_modes: cache_modes_tx,
        departing: departing_tx,
    } = publishers;
    let mut seeds = seeds.fuse();
    let chitchat = handle.chitchat();
    let mut live_nodes = chitchat.lock().await.live_nodes_watch_stream().fuse();
    // Peers whose protocol version has been logged once, so a view refresh
    // does not repeat the notice.
    let mut protocol_noticed: HashSet<NodeId> = HashSet::new();

    loop {
        tokio::select! {
            addr = seeds.select_next_some() => {
                if let Err(error) = handle.gossip(addr) {
                    tracing::debug!(%error, %addr, "failed to queue gossip with discovered peer");
                }
            }
            live = live_nodes.select_next_some() => {
                let mut peers: Vec<Peer> = Vec::new();
                let mut cache_modes: CacheModes = HashMap::new();
                let mut departing: HashMap<NodeId, bool> = HashMap::new();
                for (id, state) in live.iter().filter(|(id, _)| *id != &self_chitchat_id) {
                    let Some(peer) = parse_peer(id, state) else { continue };
                    if let Some(notice) = protocol_notice(peer.protocol)
                        && protocol_noticed.insert(peer.node)
                    {
                        tracing::warn!(peer = %peer.node, peer_protocol = peer.protocol, "{notice}");
                    }
                    cache_modes.insert(peer.node, parse_cache_modes(state));
                    departing.insert(peer.node, is_departing(state));
                    peers.push(peer);
                }
                tracing::debug!(count = peers.len(), "membership view updated");
                let _ = cache_modes_tx.send(cache_modes);
                let _ = departing_tx.send(departing);
                let _ = peers_tx.send(peers);
            }
            command = commands_rx.recv() => {
                match command {
                    Some(Command::SetCacheMode(name, mode)) => {
                        chitchat
                            .lock()
                            .await
                            .self_node_state()
                            .set(cache_key(&name), mode.as_token());
                    }
                    Some(Command::ClearCacheMode(name)) => {
                        chitchat
                            .lock()
                            .await
                            .self_node_state()
                            .delete(&cache_key(&name));
                    }
                    Some(Command::Shutdown(reply)) => {
                        // Gossips the departure before actually leaving, and
                        // waits for it to reach peers: `AbsenceTracker`
                        // reads it off the last state a peer had before
                        // dropping out of the live set, never counting a
                        // graceful leave as absence.
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
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr};
    use std::time::Duration;

    use chitchat::NodeState;
    use futures::stream;

    use super::*;

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    fn chitchat_id(name: &str, port: u16) -> ChitchatId {
        ChitchatId::new(name.to_string(), 1, addr(port))
    }

    fn state_with(pairs: &[(&str, &str)]) -> NodeState {
        let mut state = NodeState::for_test();
        for (key, value) in pairs {
            state.set(*key, *value);
        }
        state
    }

    #[test]
    fn parse_peer_reconstructs_full_record() {
        let node = NodeId::from(42u64);
        let name = NodeName::new("host-a", node);
        let id = chitchat_id(name.as_str(), 7000);
        let state = state_with(&[
            (NODE_ID_KEY, &node.to_string()),
            (DATA_ADDR_KEY, "127.0.0.1:8000"),
            (INCARNATION_KEY, "12345"),
        ]);

        let peer = parse_peer(&id, &state).expect("well-formed state parses");
        assert_eq!(peer.node, node);
        assert_eq!(peer.name, name);
        assert_eq!(peer.gossip_addr, addr(7000));
        assert_eq!(peer.data_addr, "127.0.0.1:8000".parse().unwrap());
        assert_eq!(peer.incarnation, 12_345);
        assert_eq!(peer.protocol, 1, "a 0.3 node gossips no protocol key");
    }

    #[test]
    fn parse_peer_reads_the_protocol_key_when_present() {
        let node = NodeId::from(42u64);
        let name = NodeName::new("host-a", node);
        let id = chitchat_id(name.as_str(), 7000);
        let state = state_with(&[
            (NODE_ID_KEY, &node.to_string()),
            (DATA_ADDR_KEY, "127.0.0.1:8000"),
            (INCARNATION_KEY, "12345"),
            (PROTOCOL_KEY, "7"),
        ]);
        let peer = parse_peer(&id, &state).expect("well-formed state parses");
        assert_eq!(peer.protocol, 7);
    }

    #[test]
    fn protocol_notice_flags_only_newer_or_unserved_peers() {
        assert!(protocol_notice(crate::wire::PROTOCOL_VERSION).is_none());
        assert!(protocol_notice(crate::wire::MIN_PROTOCOL_VERSION).is_none());
        assert!(protocol_notice(crate::wire::PROTOCOL_VERSION + 1).is_some());
        assert!(protocol_notice(0).is_some());
    }

    #[test]
    fn parse_peer_preserves_hyphenated_hostnames() {
        let node = NodeId::from(7u64);
        let name = NodeName::new("edge-box-3", node);
        let id = chitchat_id(name.as_str(), 7001);
        let state = state_with(&[
            (NODE_ID_KEY, &node.to_string()),
            (DATA_ADDR_KEY, "127.0.0.1:8001"),
            (INCARNATION_KEY, "1"),
        ]);

        let peer = parse_peer(&id, &state).expect("hyphenated hostname still parses");
        assert_eq!(peer.name, name);
    }

    #[test]
    fn parse_peer_rejects_missing_node_id() {
        let id = chitchat_id("host-a-000000000000002a", 7000);
        let state = state_with(&[(DATA_ADDR_KEY, "127.0.0.1:8000"), (INCARNATION_KEY, "1")]);
        assert!(parse_peer(&id, &state).is_none());
    }

    #[test]
    fn parse_peer_rejects_malformed_node_id() {
        let id = chitchat_id("host-a-not-hex", 7000);
        let state = state_with(&[
            (NODE_ID_KEY, "not-hex"),
            (DATA_ADDR_KEY, "127.0.0.1:8000"),
            (INCARNATION_KEY, "1"),
        ]);
        assert!(parse_peer(&id, &state).is_none());
    }

    #[test]
    fn parse_peer_rejects_name_suffix_mismatch() {
        // The KV state's node_id doesn't match the chitchat id's suffix.
        let other_node = NodeId::from(99u64);
        let id = chitchat_id("host-a-000000000000002a", 7000);
        let state = state_with(&[
            (NODE_ID_KEY, &other_node.to_string()),
            (DATA_ADDR_KEY, "127.0.0.1:8000"),
            (INCARNATION_KEY, "1"),
        ]);
        assert!(parse_peer(&id, &state).is_none());
    }

    #[test]
    fn parse_peer_rejects_malformed_data_addr() {
        let node = NodeId::from(1u64);
        let name = NodeName::new("host-a", node);
        let id = chitchat_id(name.as_str(), 7000);
        let state = state_with(&[
            (NODE_ID_KEY, &node.to_string()),
            (DATA_ADDR_KEY, "not-an-addr"),
            (INCARNATION_KEY, "1"),
        ]);
        assert!(parse_peer(&id, &state).is_none());
    }

    #[test]
    fn parse_peer_collects_cache_mode_keys_and_skips_a_malformed_one() {
        let node = NodeId::from(3u64);
        let name = NodeName::new("host-a", node);
        let id = chitchat_id(name.as_str(), 7000);
        let state = state_with(&[
            (NODE_ID_KEY, &node.to_string()),
            (DATA_ADDR_KEY, "127.0.0.1:8000"),
            (INCARNATION_KEY, "1"),
            ("cache:users", "replicated"),
            ("cache:sessions", "invalidation"),
            ("cache:scratch", "local"),
            ("cache:bogus", "not-a-real-mode"),
        ]);

        assert!(
            parse_peer(&id, &state).is_some(),
            "well-formed state parses"
        );
        let caches = parse_cache_modes(&state);
        assert_eq!(caches.len(), 3, "the malformed token is skipped");
        assert_eq!(caches.get("users"), Some(&Mode::Replicated));
        assert_eq!(caches.get("sessions"), Some(&Mode::Invalidation));
        assert_eq!(caches.get("scratch"), Some(&Mode::Local));
        assert!(!caches.contains_key("bogus"));
    }

    #[test]
    fn parse_cache_modes_omits_a_key_deleted_from_node_state() {
        let mut state = state_with(&[("cache:users", "replicated"), ("cache:orders", "local")]);
        assert_eq!(parse_cache_modes(&state).len(), 2);

        state.delete(&cache_key("users"));

        let caches = parse_cache_modes(&state);
        assert_eq!(caches.len(), 1, "the deleted key no longer appears");
        assert!(!caches.contains_key("users"));
        assert_eq!(caches.get("orders"), Some(&Mode::Local));
    }

    #[test]
    fn is_departing_reads_the_departing_key() {
        let state = state_with(&[(DEPARTING_KEY, "1")]);
        assert!(is_departing(&state));
        assert!(!is_departing(&NodeState::for_test()));
    }

    #[tokio::test]
    async fn collect_initial_seeds_dedupes_and_stops_at_window() {
        let a = addr(1);
        let b = addr(2);
        let mut seeds: BoxStream<'static, SocketAddr> =
            stream::iter(std::iter::repeat([a, b]).flatten()).boxed();
        let collected = collect_initial_seeds(&mut seeds).await;
        assert_eq!(collected.len(), 2);
        assert!(collected.contains(&a.to_string()));
        assert!(collected.contains(&b.to_string()));
    }

    #[tokio::test]
    async fn collect_initial_seeds_on_empty_stream_returns_empty() {
        let mut seeds: BoxStream<'static, SocketAddr> = stream::empty().boxed();
        let collected = collect_initial_seeds(&mut seeds).await;
        assert!(collected.is_empty());
    }

    #[test]
    fn advertise_ip_for_keeps_explicit_bind_ip() {
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5));
        assert_eq!(advertise_ip_for(&ClusterConfig::default(), ip), ip);
    }

    #[test]
    fn advertise_ip_for_honors_the_config_override_and_skips_resolution() {
        let configured = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 9));
        let config = ClusterConfig {
            advertise_ip: Some(configured),
            ..ClusterConfig::default()
        };
        // The bind ip is unspecified, which would otherwise trigger the
        // probe/fallback chain; the override must short-circuit it.
        assert_eq!(
            advertise_ip_for(&config, IpAddr::V4(Ipv4Addr::UNSPECIFIED)),
            configured
        );
    }

    #[test]
    fn fallback_advertise_ip_picks_the_first_routable_candidate_of_the_family() {
        let loopback_v4 = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let link_local_v4 = IpAddr::V4(Ipv4Addr::new(169, 254, 1, 2));
        let lan_v4 = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 7));
        let lan_v6 = IpAddr::V6(Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1));
        let candidates = [loopback_v4, link_local_v4, lan_v4, lan_v6];

        assert_eq!(fallback_advertise_ip(&candidates, false), lan_v4);
        assert_eq!(fallback_advertise_ip(&candidates, true), lan_v6);
    }

    #[test]
    fn fallback_advertise_ip_falls_back_to_loopback_with_no_routable_candidate() {
        let link_local_v4 = IpAddr::V4(Ipv4Addr::new(169, 254, 1, 2));
        let link_local_v6 = IpAddr::V6(Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1));
        let candidates = [link_local_v4, link_local_v6];

        assert_eq!(
            fallback_advertise_ip(&candidates, false),
            IpAddr::V4(Ipv4Addr::LOCALHOST)
        );
        assert_eq!(
            fallback_advertise_ip(&candidates, true),
            IpAddr::V6(Ipv6Addr::LOCALHOST)
        );
    }

    #[test]
    fn fallback_advertise_ip_on_an_empty_candidate_list_is_loopback() {
        assert_eq!(
            fallback_advertise_ip(&[], false),
            IpAddr::V4(Ipv4Addr::LOCALHOST)
        );
    }

    fn repeating_stream(addr: SocketAddr) -> BoxStream<'static, SocketAddr> {
        stream::unfold((), move |()| async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            Some((addr, ()))
        })
        .boxed()
    }

    async fn wait_for_peer_count(
        mut peers: watch::Receiver<Vec<Peer>>,
        expected: usize,
        timeout: Duration,
    ) -> Vec<Peer> {
        tokio::time::timeout(timeout, async {
            loop {
                if peers.borrow().len() >= expected {
                    return peers.borrow().clone();
                }
                if peers.changed().await.is_err() {
                    return peers.borrow().clone();
                }
            }
        })
        .await
        .expect("peer set converged within the bound")
    }

    #[tokio::test]
    async fn single_node_peers_stays_empty_and_shuts_down_cleanly() {
        // No seed stream and no partner ever gossips in: `peers()` must
        // stay at its initial empty value rather than hang or panic, and
        // `set_cache_mode`/`shutdown` must both work with nothing to gossip
        // with.
        let cluster_name: SmolStr = "membership-test-solo".into();
        let config = ClusterConfig {
            gossip_bind_addr: addr(0),
            ..ClusterConfig::default()
        };
        let node = NodeId::random();
        let membership = Membership::spawn(
            cluster_name,
            node,
            "solo",
            addr(9401),
            &config,
            stream::pending().boxed(),
        )
        .await
        .expect("solo node starts");

        assert_eq!(membership.local_peer().node, node);
        assert!(
            membership.peers().borrow().is_empty(),
            "a single node without a partner never gains a peer"
        );

        membership.set_cache_mode("users", Mode::Replicated);

        // No partner is gossiping, so `peers()` never changes; give the
        // background loop a moment to have processed the command, then
        // confirm the view is still empty rather than having errored.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(membership.peers().borrow().is_empty());

        membership.shutdown().await;
    }

    #[tokio::test]
    async fn dropping_every_handle_without_shutdown_aborts_the_gossip_server() {
        let config = ClusterConfig {
            gossip_bind_addr: addr(0),
            ..ClusterConfig::default()
        };
        let membership = Membership::spawn(
            "membership-test-crash".into(),
            NodeId::random(),
            "solo",
            addr(9403),
            &config,
            stream::pending().boxed(),
        )
        .await
        .expect("solo node starts");
        let gossip_addr = membership.local_peer().gossip_addr;
        assert!(
            std::net::UdpSocket::bind(gossip_addr).is_err(),
            "the gossip socket is bound while the server runs"
        );

        drop(membership);

        tokio::time::timeout(Duration::from_secs(5), async {
            while std::net::UdpSocket::bind(gossip_addr).is_err() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the aborted server releases its socket");
    }

    #[tokio::test]
    async fn clear_cache_mode_on_an_unset_name_does_not_panic_or_hang() {
        let cluster_name: SmolStr = "membership-test-clear-solo".into();
        let config = ClusterConfig {
            gossip_bind_addr: addr(0),
            ..ClusterConfig::default()
        };
        let node = NodeId::random();
        let membership = Membership::spawn(
            cluster_name,
            node,
            "solo",
            addr(9402),
            &config,
            stream::pending().boxed(),
        )
        .await
        .expect("solo node starts");

        membership.set_cache_mode("users", Mode::Replicated);
        membership.clear_cache_mode("users");
        // Clearing a name never set, and clearing the same name twice, are
        // both no-ops rather than errors.
        membership.clear_cache_mode("never-set");
        membership.clear_cache_mode("users");

        tokio::time::sleep(Duration::from_millis(50)).await;
        membership.shutdown().await;
    }

    #[tokio::test]
    async fn three_nodes_converge_over_loopback() {
        let cluster_name: SmolStr = "membership-test-converge".into();
        let config = ClusterConfig {
            gossip_bind_addr: addr(0),
            ..ClusterConfig::default()
        };

        let node1 = NodeId::random();
        let membership1 = Membership::spawn(
            cluster_name.clone(),
            node1,
            "node1",
            addr(9101),
            &config,
            stream::pending().boxed(),
        )
        .await
        .expect("node1 starts");
        let gossip1 = membership1.local_peer().gossip_addr;

        let node2 = NodeId::random();
        let membership2 = Membership::spawn(
            cluster_name.clone(),
            node2,
            "node2",
            addr(9102),
            &config,
            repeating_stream(gossip1),
        )
        .await
        .expect("node2 starts");

        let node3 = NodeId::random();
        let membership3 = Membership::spawn(
            cluster_name,
            node3,
            "node3",
            addr(9103),
            &config,
            repeating_stream(gossip1),
        )
        .await
        .expect("node3 starts");

        let bound = Duration::from_secs(15);
        let seen_by_1 = wait_for_peer_count(membership1.peers(), 2, bound).await;
        let seen_by_2 = wait_for_peer_count(membership2.peers(), 2, bound).await;
        let seen_by_3 = wait_for_peer_count(membership3.peers(), 2, bound).await;

        for peers in [&seen_by_1, &seen_by_2, &seen_by_3] {
            assert_eq!(peers.len(), 2);
        }
        assert!(seen_by_1.iter().any(|p| p.node == node2));
        assert!(seen_by_1.iter().any(|p| p.node == node3));
        assert!(!seen_by_1.iter().any(|p| p.node == node1));

        membership1.shutdown().await;
        membership2.shutdown().await;
        membership3.shutdown().await;
    }

    #[tokio::test]
    async fn dead_node_disappears_from_live_set() {
        let cluster_name: SmolStr = "membership-test-death".into();
        let config = ClusterConfig {
            gossip_bind_addr: addr(0),
            ..ClusterConfig::default()
        };

        let node1 = NodeId::random();
        let membership1 = Membership::spawn(
            cluster_name.clone(),
            node1,
            "node1",
            addr(9201),
            &config,
            stream::pending().boxed(),
        )
        .await
        .expect("node1 starts");
        let gossip1 = membership1.local_peer().gossip_addr;

        let node2 = NodeId::random();
        let membership2 = Membership::spawn(
            cluster_name,
            node2,
            "node2",
            addr(9202),
            &config,
            repeating_stream(gossip1),
        )
        .await
        .expect("node2 starts");

        let joined = wait_for_peer_count(membership1.peers(), 1, Duration::from_secs(15)).await;
        assert!(joined.iter().any(|p| p.node == node2));

        let mut peers1 = membership1.peers();
        membership2.shutdown().await;

        let departed = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if peers1.borrow().is_empty() {
                    return;
                }
                if peers1.changed().await.is_err() {
                    return;
                }
            }
        })
        .await;
        assert!(departed.is_ok(), "dead peer did not disappear in time");
        assert!(peers1.borrow().is_empty());

        membership1.shutdown().await;
    }
}

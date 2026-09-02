//! Membership: chitchat-backed cluster view. Answers "who is alive right now"
//! by gossiping each node's data-plane address and incarnation, and exposes
//! the live set as a `watch` stream that drives the net and store layers.
//!
//! Alongside identity, each node also gossips a per-cache config fingerprint:
//! `Membership::set_cache_mode` sets a `cache:<name>` key to the opened
//! [`Mode`]'s wire token, and `parse_peer` reads every such key back off a
//! live peer's state into [`Peer::caches`] — the data `cluster` compares
//! against this node's own shard registry (at `open()` time, and again on
//! every membership-view change) to catch two nodes disagreeing about a
//! cache name's mode.

use std::collections::{HashMap, HashSet};
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chitchat::transport::UdpTransport;
use chitchat::{
    Chitchat, ChitchatConfig, ChitchatHandle, ChitchatId, FailureDetectorConfig, NodeState,
    ProtocolVersion, spawn_chitchat,
};
use futures::StreamExt;
use futures::stream::BoxStream;
use smol_str::SmolStr;
use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot, watch};
use tokio::time;

use crate::config::ClusterConfig;
use crate::error::JoinError;
use crate::node::{NodeId, NodeName};
use crate::store::Mode;

/// How long `spawn` samples the `seeds` stream to build chitchat's initial
/// static seed list, before handing the stream off to the continuous
/// re-seeding task. A short window: convergence does not depend on it, only
/// on the continuous forwarding below, which is what lets a full-cluster
/// cold restart still converge.
const INITIAL_SEED_WINDOW: Duration = Duration::from_millis(200);
/// Cap on how many distinct addresses the initial window collects.
const MAX_INITIAL_SEEDS: usize = 16;

const NODE_ID_KEY: &str = "node_id";
const DATA_ADDR_KEY: &str = "data_addr";
const INCARNATION_KEY: &str = "incarnation";
/// Prefix for the per-cache config fingerprint keys set by
/// `Membership::set_cache_mode`: the full key is `cache:<name>`.
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
    /// Incremented each time this node leaves and rejoins the cluster;
    /// distinguishes a restarted process from a still-live one.
    pub incarnation: u64,
    /// Every cache this peer advertises as open, and the [`Mode`] it opened
    /// each one under — read off its `cache:<name>` gossip keys. A cache
    /// name this peer hasn't opened (or whose mode token this build doesn't
    /// recognize) is simply absent, never a placeholder value.
    pub caches: HashMap<SmolStr, Mode>,
}

/// A cheap-to-clone handle onto a running membership session.
///
/// Cloning shares the same background gossip task and live-peer view;
/// dropping every clone does not stop the task — call [`Membership::shutdown`]
/// explicitly for a graceful chitchat departure.
#[derive(Clone)]
pub struct Membership {
    peers: watch::Receiver<Vec<Peer>>,
    local: Peer,
    shutdown_tx: mpsc::UnboundedSender<oneshot::Sender<()>>,
    /// Captured from [`ChitchatHandle::chitchat`] before the handle itself
    /// moves into the background [`run`] task, so `Membership::set_cache_mode`
    /// can keep setting keys on this node's own state after startup without
    /// needing a round trip through that task.
    chitchat: Arc<AsyncMutex<Chitchat>>,
}

impl Membership {
    /// Starts gossip-based membership for one node and returns a handle once
    /// the background task is running.
    ///
    /// `seeds` is a continuous stream of candidate gossip addresses from a
    /// [`crate::discovery::Discovery`] implementation — continuous, not
    /// one-shot, so a full-cluster cold restart still converges. An
    /// initial window of it seeds chitchat's static seed list; every address
    /// it produces afterward is forwarded to chitchat's dynamic
    /// [`ChitchatHandle::gossip`] for the lifetime of this membership session
    /// — chitchat has no other mechanism to grow the seed set after startup,
    /// so this is how continuous discovery reaches it.
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
            // Probe-bind to claim a free ephemeral port, then release it so
            // chitchat's own transport can bind the same port number. There
            // is an unavoidable race (another process could grab the port in
            // between) inherent to this "reserve then rebind" pattern; on a
            // trusted LAN this is an accepted, standard trade-off.
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
        let advertise_ip =
            resolve_advertise_ip(listen_addr.ip()).map_err(|source| JoinError::Bind {
                addr: listen_addr,
                source,
            })?;
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
            caches: HashMap::new(),
        };

        // Captured before `handle` moves into `run` below — `run` owns the
        // handle for the graceful-shutdown call, but `Membership` itself
        // still needs a way to set keys on this node's own state afterward.
        let chitchat = handle.chitchat();

        let (peers_tx, peers_rx) = watch::channel(Vec::new());
        let (shutdown_tx, shutdown_rx) = mpsc::unbounded_channel();

        tokio::spawn(run(handle, seeds, peers_tx, chitchat_id, shutdown_rx));

        Ok(Self {
            peers: peers_rx,
            local,
            shutdown_tx,
            chitchat,
        })
    }

    /// This node's own [`Peer`] record, as advertised to the cluster.
    #[must_use]
    pub fn local_peer(&self) -> &Peer {
        &self.local
    }

    /// A live-updating view of the current member set. Every membership
    /// change (join, leave, suspect, data-plane address change) publishes a
    /// fresh `Vec<Peer>`; downstream consumers (`net`, `cluster`) `.borrow()`
    /// or `.changed().await` this rather than polling.
    #[must_use]
    pub fn peers(&self) -> watch::Receiver<Vec<Peer>> {
        self.peers.clone()
    }

    /// Advertises `mode` as this node's [`Mode`] for cache `name`, under the
    /// `cache:<name>` gossip key — read back by every peer's `parse_peer`
    /// into [`Peer::caches`]. Setting the same value twice is a no-op on the
    /// wire (chitchat only bumps a key's version when the value changes), so
    /// this is safe to call unconditionally after every successful `open()`.
    pub(crate) async fn set_cache_mode(&self, name: &str, mode: Mode) {
        self.chitchat
            .lock()
            .await
            .self_node_state()
            .set(cache_key(name), mode.as_token());
    }

    /// Leaves the cluster gracefully (chitchat departs politely) and stops
    /// the background gossip task.
    ///
    /// chitchat 0.13 has no protocol-level "leave" broadcast: departure means
    /// stopping the gossip loop cleanly (vs. an abrupt process death) so
    /// peers observe it exactly like any other silence, and the failure
    /// detector reclaims it after the usual grace period.
    pub async fn shutdown(self) {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self.shutdown_tx.send(reply_tx).is_ok() {
            let _ = reply_rx.await;
        }
    }
}

/// Samples `seeds` for up to [`INITIAL_SEED_WINDOW`] to build chitchat's
/// static seed list, deduplicating and capping at [`MAX_INITIAL_SEEDS`].
/// Does not exhaust the stream — the caller keeps consuming it afterward.
async fn collect_initial_seeds(seeds: &mut BoxStream<'static, SocketAddr>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut seed_nodes = Vec::new();
    let deadline = time::Instant::now() + INITIAL_SEED_WINDOW;
    // The `time::Instant::now() < deadline` guard is load-bearing, not
    // redundant with `timeout_at` below: `Timeout::poll` checks its inner
    // future before the deadline, so a stream that is always synchronously
    // ready (never `Pending`) starves the deadline check forever and this
    // loop would otherwise spin without yielding. The plain sync check here
    // runs every iteration regardless of whether the await below suspends.
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

/// Determines the address peers should use to reach this node's gossip
/// socket. A concrete bind IP is used as-is; the zeroconf default
/// (`0.0.0.0`/`::`) is resolved to the OS-chosen outbound interface via a UDP
/// "connect" — for a datagram socket this only sets the kernel's default
/// peer and never sends a packet, so it is a cheap, effectively synchronous
/// call safe to make inline.
///
/// `pub(crate)` rather than private: `cluster.rs` reuses this to resolve the
/// data-plane's advertised address with the identical logic, rather than
/// duplicating it.
pub(crate) fn resolve_advertise_ip(bind_ip: IpAddr) -> io::Result<IpAddr> {
    if !bind_ip.is_unspecified() {
        return Ok(bind_ip);
    }
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

fn now_incarnation_ms() -> u64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
}

/// Reconstructs a [`Peer`] from a live chitchat member, or `None` if its
/// gossiped state is missing or malformed (a peer mid-startup that hasn't
/// published its keys yet, or a node running an incompatible version) — such
/// peers are silently excluded rather than surfaced as an error, since
/// they'll appear once their state catches up.
fn parse_peer(chitchat_id: &ChitchatId, node_state: &NodeState) -> Option<Peer> {
    let node_id_value = u64::from_str_radix(node_state.get(NODE_ID_KEY)?, 16).ok()?;
    let node = NodeId::from(node_id_value);

    // The node id string chitchat carries is exactly `{hostname}-{node_id}`
    // — `NodeName::new` builds it, and `spawn` above passes it as the
    // chitchat node id. Strip the known suffix rather than splitting on the
    // last `-`, so a hyphenated hostname round-trips correctly.
    let suffix = format!("-{node}");
    let hostname = chitchat_id.node_id.strip_suffix(suffix.as_str())?;
    let name = NodeName::new(hostname, node);

    let data_addr: SocketAddr = node_state.get(DATA_ADDR_KEY)?.parse().ok()?;
    let incarnation: u64 = node_state.get(INCARNATION_KEY)?.parse().ok()?;
    let caches = parse_cache_modes(node_state);

    Some(Peer {
        node,
        name,
        gossip_addr: chitchat_id.gossip_advertise_addr,
        data_addr,
        incarnation,
        caches,
    })
}

/// Reads every `cache:<name>` key off `node_state` into a `name -> Mode`
/// map. A key whose value isn't a recognized [`Mode`] token (a peer running
/// a build that added a mode this one doesn't know, or plain corruption) is
/// logged and skipped — it never fails `parse_peer` for the whole node.
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

/// Owns the chitchat handle for the lifetime of one membership session:
/// forwards freshly discovered addresses into chitchat's gossip, republishes
/// chitchat's live-set changes as `Vec<Peer>`, and performs the graceful
/// shutdown on request. Runs as a single task so `ChitchatHandle::shutdown`
/// (which consumes it) has exactly one owner regardless of how many
/// [`Membership`] clones exist.
async fn run(
    handle: ChitchatHandle,
    seeds: BoxStream<'static, SocketAddr>,
    peers_tx: watch::Sender<Vec<Peer>>,
    self_chitchat_id: ChitchatId,
    mut shutdown_rx: mpsc::UnboundedReceiver<oneshot::Sender<()>>,
) {
    let mut seeds = seeds.fuse();
    let mut live_nodes = handle
        .chitchat()
        .lock()
        .await
        .live_nodes_watch_stream()
        .fuse();

    loop {
        tokio::select! {
            addr = seeds.select_next_some() => {
                if let Err(error) = handle.gossip(addr) {
                    tracing::debug!(%error, %addr, "failed to queue gossip with discovered peer");
                }
            }
            live = live_nodes.select_next_some() => {
                let peers: Vec<Peer> = live
                    .iter()
                    .filter(|(id, _)| *id != &self_chitchat_id)
                    .filter_map(|(id, state)| parse_peer(id, state))
                    .collect();
                tracing::debug!(count = peers.len(), "membership view updated");
                let _ = peers_tx.send(peers);
            }
            reply = shutdown_rx.recv() => {
                let Some(reply) = reply else { return };
                if let Err(error) = handle.shutdown().await {
                    tracing::warn!(%error, "chitchat shutdown reported an error");
                }
                tracing::info!("membership stopped");
                let _ = reply.send(());
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;
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
        // node_id in the KV state doesn't match the suffix actually present
        // in the chitchat node-id string (state disagreement / different node).
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

        let peer = parse_peer(&id, &state).expect("well-formed state parses");
        assert_eq!(peer.caches.len(), 3, "the malformed token is skipped");
        assert_eq!(peer.caches.get("users"), Some(&Mode::Replicated));
        assert_eq!(peer.caches.get("sessions"), Some(&Mode::Invalidation));
        assert_eq!(peer.caches.get("scratch"), Some(&Mode::Local));
        assert!(!peer.caches.contains_key("bogus"));
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
    fn resolve_advertise_ip_keeps_explicit_ip() {
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5));
        assert_eq!(resolve_advertise_ip(ip).unwrap(), ip);
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

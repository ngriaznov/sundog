use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, Mutex as StdMutex};
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

fn live_peer(node: u64, host: &str, incarnation: u64, port: u16) -> Peer {
    let node = NodeId::from(node);
    Peer {
        node,
        name: NodeName::new(host, node),
        gossip_addr: addr(port),
        data_addr: addr(port + 1000),
        incarnation,
        protocol: crate::wire::PROTOCOL_VERSION,
    }
}

fn local_identity(node: u64, host: &str, incarnation: u64) -> LocalIdentity {
    let node = NodeId::from(node);
    let name = NodeName::new(host, node);
    LocalIdentity {
        chitchat_id: ChitchatId::new(name.to_string(), incarnation, addr(1)),
        node,
        name,
        incarnation,
    }
}

#[test]
fn split_live_keeps_the_highest_incarnation_of_a_node() {
    let local = local_identity(1, "me", 50);
    let old = live_peer(2, "host-b", 10, 7002);
    let new = live_peer(2, "host-b", 20, 7003);
    let other = live_peer(3, "host-c", 5, 7004);
    for entries in [
        vec![old.clone(), new.clone(), other.clone()],
        vec![new.clone(), other.clone(), old.clone()],
    ] {
        let split = split_live(entries, |peer| peer, &local);
        assert_eq!(split.live.len(), 2);
        assert!(split.live.contains(&new), "the newer generation stays");
        assert!(split.live.contains(&other));
        assert_eq!(split.twins, Vec::<Peer>::new());
    }
}

#[test]
fn split_live_breaks_an_incarnation_tie_by_gossip_address() {
    let local = local_identity(1, "me", 50);
    let low = live_peer(2, "host-b", 10, 7002);
    let high = live_peer(2, "host-b", 10, 7003);
    let forward = split_live(vec![low.clone(), high.clone()], |peer| peer, &local);
    let backward = split_live(vec![high.clone(), low], |peer| peer, &local);
    assert_eq!(forward.live, vec![high.clone()]);
    assert_eq!(backward.live, vec![high]);
}

#[test]
fn split_live_reports_a_second_holder_of_the_local_id_as_a_twin() {
    let local = local_identity(1, "me", 50);
    let twin_elsewhere = live_peer(1, "other-host", 10, 7005);
    let remote = live_peer(2, "host-b", 10, 7002);
    let split = split_live(
        vec![twin_elsewhere.clone(), remote.clone()],
        |peer| peer,
        &local,
    );
    assert_eq!(split.live, vec![remote]);
    assert_eq!(split.twins, vec![twin_elsewhere]);
}

#[test]
fn split_live_drops_the_own_previous_incarnation_without_calling_it_a_twin() {
    let local = local_identity(1, "me", 50);
    let previous = live_peer(1, "me", 40, 7006);
    let split = split_live(vec![previous], |peer| peer, &local);
    assert!(split.live.is_empty(), "the old generation is not a peer");
    assert!(split.twins.is_empty(), "a fast restart is not a twin");
}

#[test]
fn split_live_calls_a_same_host_entry_that_is_not_older_a_twin() {
    let local = local_identity(1, "me", 50);
    let same_stamp = live_peer(1, "me", 50, 7006);
    let newer = live_peer(1, "me", 60, 7007);
    let split = split_live(vec![same_stamp.clone(), newer.clone()], |peer| peer, &local);
    assert_eq!(split.twins, vec![same_stamp, newer]);
}

#[test]
fn fresh_twins_reports_each_twin_once_while_it_stays_live() {
    let mut noticed = HashSet::new();
    let twin = live_peer(1, "other", 10, 7005);
    assert_eq!(
        fresh_twins(&mut noticed, std::slice::from_ref(&twin)),
        vec![twin.clone()]
    );
    assert!(
        fresh_twins(&mut noticed, std::slice::from_ref(&twin)).is_empty(),
        "a later view with the same twin stays quiet"
    );
    assert_eq!(fresh_twins(&mut noticed, &[]), Vec::<Peer>::new());
    assert!(noticed.is_empty(), "a twin that left is forgotten");
    assert_eq!(
        fresh_twins(&mut noticed, std::slice::from_ref(&twin)),
        vec![twin],
        "a twin that returns is reported again"
    );
}

#[test]
fn forget_departed_notices_keeps_exactly_the_notices_of_live_peers() {
    let (live, departed) = (NodeId::from(1), NodeId::from(2));
    let mut noticed = HashSet::from([live, departed]);
    let view = HashMap::from([
        (live, LiveFlags::default()),
        (NodeId::from(3), LiveFlags { departing: true }),
    ]);
    forget_departed_notices(&mut noticed, &view);
    assert_eq!(
        noticed,
        HashSet::from([live]),
        "the live peer stays noticed, the departed one is forgotten, and a live peer never noticed is not added"
    );
    forget_departed_notices(&mut noticed, &HashMap::new());
    assert!(noticed.is_empty(), "an empty view forgets every notice");
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
fn parse_loaders_reads_every_loader_key_and_nothing_else() {
    let mut state = state_with(&[
        ("loader:users", "1"),
        ("loader:orders", "1"),
        ("cache:users", "replicated"),
    ]);
    let expected: HashSet<SmolStr> = ["users", "orders"].into_iter().map(SmolStr::new).collect();
    assert_eq!(parse_loaders(&state), expected);
    assert!(
        !parse_cache_modes(&state).contains_key("loader:users"),
        "a loader key is no cache mode"
    );
    state.delete(&loader_key("users"));
    assert_eq!(
        parse_loaders(&state),
        [SmolStr::new("orders")].into_iter().collect()
    );
    let leaving = state_with(&[("loader:users", "1"), (DEPARTING_KEY, "1")]);
    assert!(
        advertised_loaders(&leaving).is_empty(),
        "a departing node loads for no one"
    );
}

#[test]
fn parse_refreshers_reads_every_refresh_key_and_nothing_else() {
    let mut state = state_with(&[
        ("refresh:users", "1"),
        ("refresh:orders", "1"),
        ("loader:carts", "1"),
        ("cache:users", "replicated"),
    ]);
    let expected: HashSet<SmolStr> = ["users", "orders"].into_iter().map(SmolStr::new).collect();
    assert_eq!(parse_refreshers(&state), expected);
    assert!(
        !parse_loaders(&state).contains("users"),
        "a refresh key is no loader key"
    );
    state.delete(&refresh_key("users"));
    assert_eq!(
        parse_refreshers(&state),
        [SmolStr::new("orders")].into_iter().collect()
    );
    let leaving = state_with(&[("refresh:users", "1"), (DEPARTING_KEY, "1")]);
    assert!(
        advertised_refreshers(&leaving).is_empty(),
        "a departing node takes no refresh hints"
    );
    assert!(
        parse_refreshers(&NodeState::for_test()).is_empty(),
        "a node of a release without the key advertises none"
    );
}

#[test]
fn is_departing_reads_the_departing_key() {
    let state = state_with(&[(DEPARTING_KEY, "1")]);
    assert!(is_departing(&state));
    assert!(!is_departing(&NodeState::for_test()));
}

#[test]
fn a_departing_node_advertises_no_cache_modes() {
    let token = Mode::distributed().as_token();
    let staying = state_with(&[(&cache_key("users"), token.as_ref())]);
    assert_eq!(
        advertised_cache_modes(&staying).get("users"),
        Some(&Mode::distributed())
    );
    let leaving = state_with(&[(&cache_key("users"), token.as_ref()), (DEPARTING_KEY, "1")]);
    assert!(
        advertised_cache_modes(&leaving).is_empty(),
        "a node that announced its departure owns nothing from then on"
    );
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
    assert!(collected.is_empty(), "{collected:?}");
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

/// Table-driven: `should_retry_gossip_bind` moves only a port-0
/// request, only on an address-in-use failure, and only while the
/// attempt count is under `GOSSIP_BIND_ATTEMPTS`.
#[test]
fn should_retry_gossip_bind_moves_only_a_port_zero_request_on_addr_in_use_within_the_cap() {
    struct Case {
        name: &'static str,
        requested_port: u16,
        addr_in_use: bool,
        attempt: u32,
        expect: bool,
    }
    let cases = [
        Case {
            name: "port 0, taken, first attempt",
            requested_port: 0,
            addr_in_use: true,
            attempt: 0,
            expect: true,
        },
        Case {
            name: "port 0, taken, last attempt under the cap",
            requested_port: 0,
            addr_in_use: true,
            attempt: GOSSIP_BIND_ATTEMPTS - 1,
            expect: true,
        },
        Case {
            name: "port 0, taken, cap reached",
            requested_port: 0,
            addr_in_use: true,
            attempt: GOSSIP_BIND_ATTEMPTS,
            expect: false,
        },
        Case {
            name: "port 0, some other failure",
            requested_port: 0,
            addr_in_use: false,
            attempt: 0,
            expect: false,
        },
        Case {
            name: "a fixed port never moves",
            requested_port: 7946,
            addr_in_use: true,
            attempt: 0,
            expect: false,
        },
    ];
    for case in cases {
        assert_eq!(
            should_retry_gossip_bind(case.requested_port, case.addr_in_use, case.attempt),
            case.expect,
            "case: {}",
            case.name
        );
    }
}

#[test]
fn is_addr_in_use_recognizes_only_the_kernel_address_taken_error() {
    assert!(is_addr_in_use(Some(&io::Error::from(
        io::ErrorKind::AddrInUse
    ))));
    assert!(!is_addr_in_use(Some(&io::Error::from(
        io::ErrorKind::PermissionDenied
    ))));
    assert!(!is_addr_in_use(Some(&io::Error::other("failed to bind"))));
    assert!(!is_addr_in_use(None));
}

/// A fixed gossip port that another socket holds fails `spawn` outright:
/// the operator chose that port, so it never silently moves, and the
/// error names the address-in-use cause chitchat reported.
#[tokio::test]
async fn a_fixed_gossip_port_already_taken_fails_without_moving() {
    let holder = std::net::UdpSocket::bind(addr(0)).expect("hold a loopback udp port");
    let taken = holder.local_addr().expect("held socket has an address");
    let config = ClusterConfig {
        gossip_bind_addr: taken,
        ..ClusterConfig::default()
    };
    let error = Membership::spawn(
        "membership-test-fixed-port-taken".into(),
        NodeId::random(),
        "fixed",
        addr(9402),
        &config,
        stream::pending().boxed(),
    )
    .await
    .err()
    .expect("a held fixed port fails to start membership");
    assert!(
        matches!(error, JoinError::Membership(_)),
        "chitchat's own bind reports the taken port: {error:?}"
    );
    let cause = std::error::Error::source(&error)
        .map(ToString::to_string)
        .unwrap_or_default();
    assert!(
        cause.contains(&taken.to_string()),
        "the cause names the held address: {cause}"
    );
    drop(holder);
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

#[tokio::test]
async fn spawn_observed_hands_the_observer_each_live_set_before_the_peers_watch() {
    let cluster_name: SmolStr = "membership-test-observer".into();
    let config = ClusterConfig {
        gossip_bind_addr: addr(0),
        ..ClusterConfig::default()
    };
    let seen: Arc<StdMutex<Vec<HashMap<NodeId, LiveFlags>>>> = Arc::default();
    let log = Arc::clone(&seen);
    let membership1 = Membership::spawn_observed(
        cluster_name.clone(),
        NodeId::random(),
        "node1",
        addr(9301),
        &config,
        stream::pending().boxed(),
        Some(Box::new(move |live| {
            log.lock().expect("unpoisoned").push(live.clone());
        })),
    )
    .await
    .expect("node1 starts");
    let gossip1 = membership1.local_peer().gossip_addr;

    let node2 = NodeId::random();
    let membership2 = Membership::spawn(
        cluster_name,
        node2,
        "node2",
        addr(9302),
        &config,
        repeating_stream(gossip1),
    )
    .await
    .expect("node2 starts");

    let observed_live = |node: NodeId| {
        seen.lock()
            .expect("unpoisoned")
            .iter()
            .any(|live| live.contains_key(&node))
    };
    let mut peers1 = membership1.peers();
    wait_for_peer_count(membership1.peers(), 1, Duration::from_secs(15)).await;
    assert!(
        observed_live(node2),
        "the observer saw node2 before the peers watch did"
    );

    membership2.shutdown().await;
    let departed = tokio::time::timeout(Duration::from_secs(20), async {
        while !peers1.borrow_and_update().is_empty() {
            if peers1.changed().await.is_err() {
                return;
            }
        }
    })
    .await;
    assert!(departed.is_ok(), "node2 did not leave the live set in time");
    let seen = seen.lock().expect("unpoisoned").clone();
    assert!(
        seen.last().is_some_and(HashMap::is_empty),
        "the observer saw node2 leave no later than the peers watch"
    );
    assert!(
        seen.iter()
            .any(|live| live.get(&node2).is_some_and(|flags| flags.departing)),
        "node2's graceful departure reached the observer before node2 left"
    );

    membership1.shutdown().await;
}

/// `secs` seconds before `now`, for building [`MemberView`] fixtures.
fn ago(now: Instant, secs: u64) -> Instant {
    now.checked_sub(Duration::from_secs(secs))
        .expect("test duration fits before `now`")
}

#[test]
fn member_is_quiet_when_continuously_present_past_the_bound() {
    let now = Instant::now();
    let bound = Duration::from_secs(60);
    let member = MemberView {
        known_since: None,
        present_since: Some(ago(now, 61)),
        absent_since: None,
        live_incarnation: Some(1),
    };
    assert!(member_is_quiet(&member, now, bound));
}

#[test]
fn member_is_quiet_defers_when_present_less_than_the_bound() {
    let now = Instant::now();
    let bound = Duration::from_secs(60);
    let member = MemberView {
        known_since: None,
        present_since: Some(ago(now, 1)),
        absent_since: None,
        live_incarnation: Some(1),
    };
    assert!(!member_is_quiet(&member, now, bound));
}

#[test]
fn member_is_quiet_when_continuously_absent_past_the_bound() {
    let now = Instant::now();
    let bound = Duration::from_secs(60);
    let member = MemberView {
        known_since: None,
        present_since: None,
        absent_since: Some(ago(now, 61)),
        live_incarnation: None,
    };
    assert!(member_is_quiet(&member, now, bound));
}

#[test]
fn member_is_quiet_defers_when_absent_less_than_the_bound() {
    let now = Instant::now();
    let bound = Duration::from_secs(60);
    let member = MemberView {
        known_since: None,
        present_since: None,
        absent_since: Some(ago(now, 1)),
        live_incarnation: None,
    };
    assert!(
        !member_is_quiet(&member, now, bound),
        "a member absent for less than the bound defers retirement"
    );
}

#[test]
fn member_is_quiet_is_false_for_a_never_observed_member() {
    let now = Instant::now();
    let member = MemberView {
        known_since: None,
        present_since: None,
        absent_since: None,
        live_incarnation: None,
    };
    assert!(!member_is_quiet(&member, now, Duration::from_secs(60)));
}

#[test]
fn member_is_quiet_once_known_for_two_bounds_however_much_it_flaps() {
    let now = Instant::now();
    let bound = Duration::from_secs(60);
    let flapper = MemberView {
        present_since: Some(now.checked_sub(Duration::from_secs(5)).expect("recent")),
        absent_since: None,
        live_incarnation: Some(7),
        known_since: Some(now.checked_sub(Duration::from_secs(121)).expect("recent")),
    };
    assert!(
        member_is_quiet(&flapper, now, bound),
        "back for five seconds after flapping, but known for two bounds"
    );
    let newcomer = MemberView {
        present_since: Some(now.checked_sub(Duration::from_secs(5)).expect("recent")),
        absent_since: None,
        live_incarnation: Some(7),
        known_since: Some(now.checked_sub(Duration::from_secs(119)).expect("recent")),
    };
    assert!(
        !member_is_quiet(&newcomer, now, bound),
        "known for just under two bounds and only briefly present: not settled"
    );
}

#[test]
fn incarnation_is_dead_when_live_with_a_different_incarnation() {
    let now = Instant::now();
    let member = MemberView {
        known_since: None,
        present_since: Some(now),
        absent_since: None,
        live_incarnation: Some(2),
    };
    assert!(
        incarnation_is_dead(&member, 1, now, Duration::from_secs(60)),
        "any incarnation mismatch is dead, not only a newer one"
    );
}

#[test]
fn incarnation_is_dead_is_false_when_live_with_the_same_incarnation() {
    let now = Instant::now();
    let member = MemberView {
        known_since: None,
        present_since: Some(now),
        absent_since: None,
        live_incarnation: Some(1),
    };
    assert!(!incarnation_is_dead(
        &member,
        1,
        now,
        Duration::from_secs(60)
    ));
}

#[test]
fn incarnation_is_dead_when_absent_longer_than_the_bound() {
    let now = Instant::now();
    let bound = Duration::from_secs(60);
    let member = MemberView {
        known_since: None,
        present_since: None,
        absent_since: Some(ago(now, 61)),
        live_incarnation: None,
    };
    assert!(incarnation_is_dead(&member, 1, now, bound));
}

#[test]
fn incarnation_is_dead_defers_when_absent_less_than_the_bound() {
    let now = Instant::now();
    let bound = Duration::from_secs(60);
    let member = MemberView {
        known_since: None,
        present_since: None,
        absent_since: Some(ago(now, 1)),
        live_incarnation: None,
    };
    assert!(
        !incarnation_is_dead(&member, 1, now, bound),
        "a member absent for less than the bound is not yet known dead"
    );
}

#[test]
fn incarnation_is_dead_is_false_for_a_never_observed_member() {
    let now = Instant::now();
    let member = MemberView {
        known_since: None,
        present_since: None,
        absent_since: None,
        live_incarnation: None,
    };
    assert!(!incarnation_is_dead(
        &member,
        1,
        now,
        Duration::from_secs(60)
    ));
}

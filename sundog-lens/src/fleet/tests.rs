//! Tests of slot addressing, seeds, templates, startup messages, configuration and preflight.

use std::path::Path;

use super::*;

const PER_ADDRESS: Layout = Layout::PerAddress(Ipv4Addr::new(127, 0, 0, 11));

fn config(testnode: &str) -> FleetConfig {
    FleetConfig {
        cluster: "lens-test".to_owned(),
        layout: PER_ADDRESS,
        testnode: PathBuf::from(testnode),
        owners: NonZeroU8::new(2).unwrap(),
        logs: PathBuf::from("target/lens-demo"),
    }
}

fn shared_config(testnode: &str) -> FleetConfig {
    FleetConfig {
        layout: Layout::Shared,
        ..config(testnode)
    }
}

#[test]
fn a_slot_has_its_ports_on_its_own_address() {
    let info = SlotInfo::new(PER_ADDRESS, 3).unwrap();
    assert_eq!(info.slot, 3);
    assert_eq!(info.label, "n3");
    assert_eq!(info.ip, Ipv4Addr::new(127, 0, 0, 13));
    assert_eq!(info.gossip, "127.0.0.13:7946".parse().unwrap());
    assert_eq!(info.control, "127.0.0.13:8080".parse().unwrap());
    assert_eq!(info.metrics, "127.0.0.13:9090".parse().unwrap());
    assert_eq!(SlotInfo::new(PER_ADDRESS, 0), None);
    assert_eq!(
        SlotInfo::new(Layout::PerAddress(Ipv4Addr::BROADCAST), 2),
        None
    );
}

#[test]
fn a_shared_slot_has_its_own_ports_on_the_shared_address() {
    let info = SlotInfo::new(Layout::Shared, 3).unwrap();
    assert_eq!(info.slot, 3);
    assert_eq!(info.label, "n3");
    assert_eq!(info.ip, Ipv4Addr::LOCALHOST);
    assert_eq!(info.gossip, "127.0.0.1:7948".parse().unwrap());
    assert_eq!(info.control, "127.0.0.1:8082".parse().unwrap());
    assert_eq!(info.metrics, "127.0.0.1:9092".parse().unwrap());
    let first = SlotInfo::new(Layout::Shared, 1).unwrap();
    assert_eq!(first.gossip, "127.0.0.1:7946".parse().unwrap());
    assert_eq!(first.control, "127.0.0.1:8080".parse().unwrap());
    assert_eq!(first.metrics, "127.0.0.1:9090".parse().unwrap());
    assert_eq!(SlotInfo::new(Layout::Shared, 0), None);
    assert_eq!(SlotInfo::new(Layout::Shared, 70_000), None);
}

#[test]
fn the_observer_seeds_are_the_first_two_slots() {
    assert_eq!(
        config("x").seed_addrs(),
        [
            "127.0.0.11:7946".parse().unwrap(),
            "127.0.0.12:7946".parse().unwrap()
        ]
    );
    let mut last = config("x");
    last.layout = Layout::PerAddress(Ipv4Addr::BROADCAST);
    assert_eq!(last.seed_addrs().len(), 1);
}

#[test]
fn the_shared_observer_seeds_are_the_first_two_gossip_ports() {
    assert_eq!(
        shared_config("x").seed_addrs(),
        [
            "127.0.0.1:7946".parse().unwrap(),
            "127.0.0.1:7947".parse().unwrap()
        ]
    );
}

#[test]
fn the_watch_line_names_the_cluster_the_first_seed_and_the_exporter() {
    assert_eq!(
        watch_line(&config("x")),
        "sundog-lens watch lens-test --seed 127.0.0.11:7946 \
         --metrics 'http://{ip}:{gossip_port+1144}/metrics'"
    );
    assert_eq!(
        watch_line(&shared_config("x")),
        "sundog-lens watch lens-test --seed 127.0.0.1:7946 \
         --metrics 'http://{ip}:{gossip_port+1144}/metrics'"
    );
}

/// The live member that gossip shows for the node at `info`.
fn live_member(info: &SlotInfo) -> sundog::observe::Member {
    use std::collections::BTreeMap;
    use std::time::SystemTime;

    use sundog::membership::Peer;
    use sundog::node::{NodeId, NodeName};
    use sundog::observe::{Member, MemberStatus};

    let node = NodeId::from(info.slot as u64);
    Member::new(
        Peer {
            node,
            name: NodeName::new("host", node),
            gossip_addr: info.gossip,
            data_addr: SocketAddr::from((info.ip, 40_000)),
            incarnation: 1,
            protocol: 6,
        },
        MemberStatus::Live,
        SystemTime::UNIX_EPOCH,
        BTreeMap::new(),
    )
}

#[test]
fn the_metrics_template_reaches_each_nodes_exporter_in_either_layout() {
    use crate::source::targets::UrlTemplate;

    let template = UrlTemplate::parse(&metrics_template()).unwrap();
    assert_eq!(metrics_template(), "http://{ip}:{gossip_port+1144}/metrics");
    for layout in [PER_ADDRESS, Layout::Shared] {
        for slot in 1..=MAX_SLOTS {
            let info = SlotInfo::new(layout, slot).unwrap();
            assert_eq!(
                template.expand(&live_member(&info)).unwrap(),
                format!("http://{}/metrics", info.metrics),
                "{layout:?} slot {slot}"
            );
        }
    }
    // The second shared slot's exporter, as the fleet documents it.
    let second = SlotInfo::new(Layout::Shared, 2).unwrap();
    assert_eq!(second.metrics, "127.0.0.1:9091".parse().unwrap());
}

#[test]
fn the_control_template_reaches_each_slots_control_port() {
    use crate::source::targets::UrlTemplate;

    assert_eq!(control_template(), "{ip}:{gossip_port+134}");
    let template = UrlTemplate::parse(&control_template()).unwrap();
    for layout in [PER_ADDRESS, Layout::Shared] {
        for slot in 1..=MAX_SLOTS {
            let info = SlotInfo::new(layout, slot).unwrap();
            let url = template.expand(&live_member(&info)).unwrap();
            assert_eq!(url, info.control.to_string(), "{layout:?} slot {slot}");
            assert_eq!(url.parse::<SocketAddr>().unwrap(), info.control);
        }
    }
    // The second shared slot's control port, as the fleet documents it.
    let second = SlotInfo::new(Layout::Shared, 2).unwrap();
    assert_eq!(second.control, "127.0.0.1:8081".parse().unwrap());
}

#[test]
fn the_nodes_asked_are_the_slots_at_their_control_ports_in_order() {
    let targets = |layout, slots: usize| {
        let infos: Vec<SlotInfo> = (1..=slots)
            .map(|slot| SlotInfo::new(layout, slot).unwrap())
            .collect();
        targets_of(&infos)
            .into_iter()
            .map(|target| (target.label.to_string(), target.addr.to_string()))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        targets(Layout::Shared, 3),
        [
            ("n1".to_owned(), "127.0.0.1:8080".to_owned()),
            ("n2".to_owned(), "127.0.0.1:8081".to_owned()),
            ("n3".to_owned(), "127.0.0.1:8082".to_owned()),
        ]
    );
    assert_eq!(
        targets(PER_ADDRESS, 2),
        [
            ("n1".to_owned(), "127.0.0.11:8080".to_owned()),
            ("n2".to_owned(), "127.0.0.12:8080".to_owned()),
        ]
    );
    let none = targets_of(&[]);
    assert!(none.is_empty(), "{none:?}");
}

#[test]
fn the_cluster_command_prints_where_each_node_started() {
    assert_eq!(
        started_line(&SlotInfo::new(PER_ADDRESS, 2).unwrap()),
        "n2 started at 127.0.0.12:7946"
    );
    assert_eq!(
        started_line(&SlotInfo::new(Layout::Shared, 2).unwrap()),
        "n2 started at 127.0.0.1:7947"
    );
}

#[test]
fn the_address_error_keeps_the_macos_alias_hint_and_names_the_flag() {
    let error = io::Error::from(io::ErrorKind::AddrNotAvailable);
    let text = address_error(Ipv4Addr::new(127, 0, 0, 11), &error);
    assert!(text.starts_with("cannot bind 127.0.0.11: "), "{text}");
    assert!(text.contains("sudo ifconfig lo0 alias"), "{text}");
    assert!(text.contains("leave out --base-ip"), "{text}");
}

#[test]
fn a_blocked_port_error_names_the_ports_of_its_layout() {
    let error = io::Error::from(io::ErrorKind::AddrInUse);
    let per_address = port_error(
        PER_ADDRESS,
        "127.0.0.11:8080".parse().unwrap(),
        "control",
        &error,
    );
    assert!(
        per_address.starts_with("cannot bind 127.0.0.11:8080: "),
        "{per_address}"
    );
    assert!(
        per_address.contains("a listener on 8080 or 9090 blocks the fleet"),
        "{per_address}"
    );
    assert!(!per_address.contains("ifconfig"), "{per_address}");

    let shared = port_error(
        Layout::Shared,
        "127.0.0.1:7948".parse().unwrap(),
        "gossip",
        &error,
    );
    assert!(
        shared.starts_with("cannot bind 127.0.0.1:7948 (gossip): "),
        "{shared}"
    );
    for ports in ["7946-7951", "8080-8085", "9090-9095"] {
        assert!(shared.contains(ports), "{ports}: {shared}");
    }
    assert!(!shared.contains("ifconfig"), "{shared}");
    assert!(!shared.contains("alias"), "{shared}");
}

#[test]
fn the_config_follows_the_fleet_flags() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let args = FleetArgs {
        name: "other".to_owned(),
        testnode: Some(manifest.clone()),
        logs: Some(PathBuf::from("/tmp/logs")),
        owners: NonZeroU8::new(3).unwrap(),
        ..FleetArgs::default()
    };
    let config = FleetConfig::from_args(&args).unwrap();
    assert_eq!(config.cluster, "other");
    assert_eq!(config.testnode, manifest);
    assert_eq!(config.logs, PathBuf::from("/tmp/logs"));
    assert_eq!(config.owners.get(), 3);
    assert_eq!(config.layout, Layout::for_host(None));
    let default_logs = FleetConfig::from_args(&FleetArgs {
        testnode: Some(manifest),
        ..FleetArgs::default()
    })
    .unwrap();
    assert_eq!(default_logs.logs, PathBuf::from(DEFAULT_LOGS));
    assert!(
        FleetConfig::from_args(&FleetArgs {
            testnode: Some(PathBuf::from("/no/such/node")),
            ..FleetArgs::default()
        })
        .is_err()
    );
}

#[test]
fn an_explicit_base_ip_gives_the_per_address_layout_on_any_host() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let base = Ipv4Addr::new(127, 0, 0, 21);
    let config = FleetConfig::from_args(&FleetArgs {
        testnode: Some(manifest),
        base_ip: Some(base),
        ..FleetArgs::default()
    })
    .unwrap();
    assert_eq!(config.layout, Layout::PerAddress(base));
}

#[tokio::test]
async fn the_shared_preflight_checks_every_port_of_every_slot() {
    // One test, so no other test holds a port this one probes.
    let fleet = Fleet::new(shared_config("x"));
    fleet.preflight().await.unwrap();

    let blocked = |addr: &str| addr.parse::<SocketAddr>().unwrap();
    let udp = UdpSocket::bind(blocked("127.0.0.1:7949")).await.unwrap();
    let error = fleet.preflight().await.unwrap_err().to_string();
    assert!(error.contains("127.0.0.1:7949 (gossip)"), "{error}");
    assert!(error.contains("7946-7951"), "{error}");
    drop(udp);

    let control = TcpListener::bind(blocked("127.0.0.1:8083")).await.unwrap();
    let error = fleet.preflight().await.unwrap_err().to_string();
    assert!(error.contains("127.0.0.1:8083 (control)"), "{error}");
    drop(control);

    let metrics = TcpListener::bind(blocked("127.0.0.1:9095")).await.unwrap();
    let error = fleet.preflight().await.unwrap_err().to_string();
    assert!(error.contains("127.0.0.1:9095 (exporter)"), "{error}");
    assert!(!error.contains("ifconfig"), "{error}");
    drop(metrics);

    fleet.preflight().await.unwrap();
}

#[test]
fn a_fleet_starts_with_no_slots() {
    let fleet = Fleet::new(config("x"));
    let infos = fleet.infos();
    assert!(infos.is_empty(), "{infos:?}");
    let running = fleet.running();
    assert!(running.is_empty(), "{running:?}");
    assert_eq!(fleet.phase("n1"), None);
    assert_eq!(fleet.config().cluster, "lens-test");
}

#[test]
fn at_most_six_nodes_run() {
    assert_eq!(MAX_SLOTS, 6);
    assert!(MAX_SLOTS <= crate::ui::theme::NODE_COLORS.len());
}

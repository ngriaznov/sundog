//! Tests for the cluster builder: validation, discovery and identity overrides, and the data port.

use super::*;
use crate::cluster::test_support::EndingDiscovery;
use crate::wire::Msg;
use std::net::Ipv4Addr;

#[tokio::test]
async fn local_incarnation_matches_the_membership_local_peer() {
    let cluster = solo_cluster("cluster-it-local-incarnation").await;

    assert_eq!(
        cluster.local_incarnation(),
        cluster.inner.membership.local_peer().incarnation
    );
    assert_ne!(
        cluster.local_incarnation(),
        0,
        "a real membership incarnation is a current-time-derived value, never zero"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn started_at_lies_within_the_build_and_is_the_same_on_every_clone() {
    let before = std::time::Instant::now();
    let cluster = solo_cluster("cluster-it-started-at").await;
    let after = std::time::Instant::now();

    let started = cluster.started_at();
    assert!(
        before <= started && started <= after,
        "the build began between the two instants taken around it"
    );
    assert_eq!(cluster.clone().started_at(), started);

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
    assert!(matches!(err, JoinError::InvalidConfig(ref msg) if msg.contains("max_frame")));
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
    assert!(
        matches!(err, JoinError::InvalidConfig(ref msg) if msg.contains("ae_sketch_cells")),
        "{err:?}"
    );
}

#[tokio::test]
async fn build_rejects_a_zero_fan_out_backlog_capacity() {
    let mut config = loopback_config();
    config.fan_out_backlog_capacity = 0;

    let err = Cluster::builder("cluster-it-fan-out-backlog-capacity-guard")
        .seeds(std::iter::empty())
        .config(config)
        .build()
        .await
        .expect_err("a zero fan_out_backlog_capacity must be rejected at build() time");
    assert!(
        matches!(err, JoinError::InvalidConfig(ref msg) if msg.contains("fan_out_backlog_capacity")),
        "{err:?}"
    );
}

/// `validate_config` does not reject a `rebalance_ack_window` past its
/// own max: unlike `max_frame`, its bound moves with `ae_interval`, and
/// many tests shrink `ae_interval` alone without re-tuning it to match.
/// `rebalance_ack_window_value` clamps the effective value at the point
/// of use instead.
#[tokio::test]
async fn build_succeeds_with_a_rebalance_ack_window_past_its_own_max_since_the_accessor_clamps_it()
{
    let mut config = loopback_config();
    config.ae_interval = Duration::from_millis(1);
    assert!(config.rebalance_ack_window > config.rebalance_ack_window_max());

    let cluster = Cluster::builder("cluster-it-ack-window-no-reject")
        .seeds(std::iter::empty())
        .config(config)
        .build()
        .await
        .expect(
            "an over-max rebalance_ack_window never blocks build(); only the accessor \
             clamps it",
        );
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cluster_keeps_gossiping_after_its_seed_stream_ends() {
    let name = "cluster-it-ending-seeds";
    let a = Cluster::builder(name)
        .discovery(EndingDiscovery {
            addrs: Vec::new(),
            // Past the initial seed window, so the membership loop
            // itself sees the stream end.
            lag: Duration::from_millis(1_500),
        })
        .config(loopback_config())
        .build()
        .await
        .expect("a builds");
    tokio::time::sleep(Duration::from_secs(3)).await;

    let b = Cluster::builder(name)
        .seeds([a.local_gossip_addr()])
        .config(loopback_config())
        .build()
        .await
        .expect("b builds");
    wait_for_peer_count(&a, 1).await;
    wait_for_peer_count(&b, 1).await;

    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn build_advertises_the_data_port_its_mesh_accepts_on() {
    let cluster = Cluster::builder("cluster-it-data-port-advertised")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node builds");

    let accepting = cluster.inner.mesh.local_addr();
    assert_ne!(accepting.port(), 0);
    assert_eq!(
        cluster.inner.membership.local_peer().data_addr,
        accepting,
        "membership gossips the port the mesh's listener holds"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn build_names_its_cluster_in_every_data_plane_hello() {
    let cluster = Cluster::builder("cluster-it-data-plane-name")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node builds");

    let Msg::Hello { cluster: named, .. } = cluster.inner.mesh.greeting().hello() else {
        unreachable!("a greeting always says hello");
    };
    assert_eq!(named, wire::cluster_id("cluster-it-data-plane-name"));
    assert_ne!(named, wire::UNNAMED_CLUSTER);

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

#[test]
fn use_static_from_env_only_when_no_discovery_is_set_and_seeds_env_is_nonempty() {
    assert!(
        use_static_from_env(false, Some("host:4000")),
        "no discovery configured and a populated SUNDOG_SEEDS: fall back to Static"
    );
    assert!(
        !use_static_from_env(true, Some("host:4000")),
        "an explicit .seeds()/.discovery() call always wins"
    );
    assert!(
        !use_static_from_env(false, None),
        "SUNDOG_SEEDS unset: stay on the mDNS default"
    );
    assert!(
        !use_static_from_env(false, Some("   ")),
        "SUNDOG_SEEDS set but blank: stay on the mDNS default"
    );
}

#[tokio::test]
async fn node_id_builder_overrides_the_generated_identity() {
    let fixed = NodeId::random();
    let cluster = Cluster::builder("cluster-it-node-id-override")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .node_id(fixed)
        .build()
        .await
        .expect("build succeeds with a caller-supplied node id");
    assert_eq!(cluster.node_id(), fixed);

    cluster.shutdown().await;
}

#[tokio::test]
async fn node_id_builder_rejects_a_merge_derived_id() {
    let derived = NodeId::merge_derived(0x1234_5678_u64);
    let err = Cluster::builder("cluster-it-node-id-merge-derived-rejected")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .node_id(derived)
        .build()
        .await
        .expect_err("a merge-derived id must never become a real node id");
    match err {
        JoinError::InvalidConfig(msg) => {
            assert!(msg.contains("node_id"));
            assert!(
                msg.contains(&derived.as_u64().to_string()),
                "the rejection names the offending id"
            );
        }
        other => panic!("expected JoinError::InvalidConfig, got {other:?}"),
    }
}

#[tokio::test]
async fn node_id_builder_accepts_a_normal_id() {
    let normal = NodeId::from(0x1234_5678_u64);
    let cluster = Cluster::builder("cluster-it-node-id-normal-accepted")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .node_id(normal)
        .build()
        .await
        .expect("a non-reserved node id is accepted");
    assert_eq!(cluster.node_id(), normal);

    cluster.shutdown().await;
}

#[tokio::test]
async fn advertise_ip_override_skips_the_probe_and_covers_both_addresses() {
    use std::net::IpAddr;

    // TEST-NET-3, guaranteed unroutable: proves the outbound probe never
    // ran, since it would otherwise fail `build()` or resolve to
    // loopback.
    let forced = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 5));
    let config = ClusterConfig {
        advertise_ip: Some(forced),
        ..loopback_config()
    };

    let cluster = Cluster::builder("cluster-it-advertise-ip")
        .seeds(std::iter::empty())
        .config(config)
        .build()
        .await
        .expect("build succeeds even though the forced advertise ip is unroutable");

    let local = cluster.inner.membership.local_peer();
    assert_eq!(local.gossip_addr.ip(), forced);
    assert_eq!(local.data_addr.ip(), forced);

    cluster.shutdown().await;
}

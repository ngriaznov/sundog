//! A `build()` that fails leaves no Prometheus recorder behind, so a retry
//! with the same listener address succeeds. A test binary of its own, so
//! the recorder slot starts empty.

#![cfg(all(feature = "prometheus", not(feature = "sim")))]

mod common;

use std::net::{Ipv4Addr, SocketAddr, TcpListener, UdpSocket};

use sundog::{Cluster, JoinError};

fn loopback() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
}

fn free_tcp_addr() -> SocketAddr {
    TcpListener::bind(loopback())
        .and_then(|listener| listener.local_addr())
        .expect("a free port")
}

#[tokio::test]
async fn a_failed_build_leaves_no_recorder_and_a_retry_succeeds() {
    let metrics_addr = free_tcp_addr();

    // The gossip port is taken: the failure comes after the listener's
    // address is bound and before anything else is set up.
    let occupied = UdpSocket::bind(loopback()).expect("hold a gossip port");
    let mut config = common::fast_config();
    config.gossip_bind_addr = occupied.local_addr().expect("held addr");
    let err = Cluster::builder("it-prometheus-failed-build")
        .seeds(std::iter::empty())
        .config(config)
        .prometheus_listen(metrics_addr)
        .build()
        .await
        .expect_err("the gossip port is taken");
    assert!(
        matches!(err, JoinError::Membership(_) | JoinError::Bind { .. }),
        "{err:?}"
    );
    drop(occupied);

    // The data port is taken.
    let held = TcpListener::bind(loopback()).expect("hold a data port");
    let mut config = common::fast_config();
    config.data_bind_addr = held.local_addr().expect("held addr");
    let err = Cluster::builder("it-prometheus-failed-build")
        .seeds(std::iter::empty())
        .config(config)
        .prometheus_listen(metrics_addr)
        .build()
        .await
        .expect_err("the data port is taken");
    assert!(matches!(err, JoinError::Bind { .. }), "{err:?}");
    drop(held);

    let cluster = Cluster::builder("it-prometheus-failed-build")
        .seeds(std::iter::empty())
        .config(common::fast_config())
        .prometheus_listen(metrics_addr)
        .build()
        .await
        .expect("the retry builds: no recorder was installed by the failures");
    cluster.shutdown().await;
}

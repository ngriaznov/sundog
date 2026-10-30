//! The mDNS daemon a seedless cluster or observer starts stops with it: on
//! shutdown, on crash and when the build fails, so nothing in a process that
//! builds and tears down clusters keeps a daemon thread per cycle.
#![cfg(target_os = "linux")]

use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::time::{Duration, Instant};

use sundog::observe::Observer;
use sundog::{Cluster, ClusterConfig};

/// How many of this process's threads are mDNS daemons.
fn mdns_daemons() -> usize {
    std::fs::read_dir("/proc/self/task")
        .expect("/proc/self/task lists this process's threads")
        .flatten()
        .filter(|task| {
            std::fs::read_to_string(task.path().join("comm"))
                .is_ok_and(|comm| comm.trim_end() == "mDNS_daemon")
        })
        .count()
}

/// Waits up to ten seconds for `mdns_daemons()` to equal `expected`.
async fn wait_for_daemons(expected: usize, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let now = mdns_daemons();
        if now == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{what}: {now} mDNS daemon threads, expected {expected}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Waits for `mdns_daemons()` to equal `expected`, then requires it to stay
/// there for half a second: a daemon that a cluster or observer dropped
/// right after its build would still be counted while it exits.
async fn hold_daemons(expected: usize, what: &str) {
    wait_for_daemons(expected, what).await;
    let until = Instant::now() + Duration::from_millis(500);
    while Instant::now() < until {
        assert_eq!(
            mdns_daemons(),
            expected,
            "{what}: the daemon stops while it is still in use"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn loopback_config() -> ClusterConfig {
    let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    ClusterConfig::default().with(|c| {
        c.gossip_bind_addr = loopback;
        c.data_bind_addr = loopback;
    })
}

/// A cluster with no seeds and no discovery set, so it discovers over mDNS.
async fn seedless(name: &str) -> Cluster {
    Cluster::builder(name)
        .config(loopback_config())
        .build()
        .await
        .expect("cluster builds")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_seedless_cluster_or_observer_stops_its_mdns_daemon_when_it_stops() {
    assert!(
        std::env::var_os("SUNDOG_SEEDS").is_none(),
        "SUNDOG_SEEDS would switch discovery from mDNS to its seed list"
    );
    let baseline = mdns_daemons();

    for round in 0..3 {
        let cluster = seedless("mdns-lifecycle-shutdown").await;
        hold_daemons(baseline + 1, &format!("round {round}: a running cluster")).await;
        cluster.shutdown().await;
        wait_for_daemons(baseline, &format!("round {round}: after shutdown")).await;
    }

    let cluster = seedless("mdns-lifecycle-crash").await;
    hold_daemons(baseline + 1, "a running cluster").await;
    cluster.crash().await;
    wait_for_daemons(baseline, "after a crash").await;

    let observer = Observer::builder("mdns-lifecycle-observer")
        .config(loopback_config())
        .build()
        .await
        .expect("observer builds");
    hold_daemons(baseline + 1, "a running observer").await;
    observer.shutdown().await;
    wait_for_daemons(baseline, "after the observer's shutdown").await;

    let taken = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("a free port binds");
    let gossip_addr = taken.local_addr().expect("bound address");
    let failed = Cluster::builder("mdns-lifecycle-failed")
        .config(loopback_config().with(|c| c.gossip_bind_addr = gossip_addr))
        .build()
        .await;
    assert!(
        failed.is_err(),
        "the gossip port is taken, so the build fails"
    );
    wait_for_daemons(baseline, "after a failed build").await;
}

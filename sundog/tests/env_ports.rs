//! `SUNDOG_GOSSIP_PORT` and `SUNDOG_DATA_PORT` give a cluster the ports its
//! config leaves at 0, and a value that is no port fails the build. The
//! variables are process-global, so each case runs this binary again with
//! them set on the copy alone, never under the harness's other threads.

use std::ffi::OsString;
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::process::Command;

use sundog::{Cluster, ClusterConfig, JoinError};

/// Set on a copy of this binary a parent test runs, so a child test does
/// nothing in an ordinary run of the ignored tests.
const CHILD: &str = "SUNDOG_ENV_PORTS_CHILD";

/// A loopback UDP port free a moment ago.
fn free_udp_port() -> u16 {
    UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
        .and_then(|socket| socket.local_addr())
        .expect("a free UDP port")
        .port()
}

/// A loopback TCP port free a moment ago.
fn free_tcp_port() -> u16 {
    TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .and_then(|listener| listener.local_addr())
        .expect("a free TCP port")
        .port()
}

/// Runs the ignored test `child` in a copy of this binary with `vars` set,
/// and asserts it ran and passed.
fn run_child(child: &str, vars: &[(&str, OsString)]) {
    let mut command = Command::new(std::env::current_exe().expect("the path of this test binary"));
    command
        .args(["--exact", child, "--ignored", "--nocapture"])
        .env(CHILD, "1")
        .env_remove("SUNDOG_SEEDS")
        .env_remove("SUNDOG_GOSSIP_PORT")
        .env_remove("SUNDOG_DATA_PORT");
    for (name, value) in vars {
        command.env(name, value);
    }
    let output = command.output().expect("the child runs");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "{child} failed:\n{stdout}\n{stderr}"
    );
    assert!(
        stdout.contains("1 passed"),
        "{child} did not run:\n{stdout}"
    );
}

/// A loopback config with both ports left at 0.
fn loopback_config() -> ClusterConfig {
    ClusterConfig::default().with(|c| {
        c.gossip_bind_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        c.data_bind_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    })
}

/// The port the parent set in `variable`.
fn port_from(variable: &str) -> u16 {
    std::env::var(variable)
        .expect("set by the parent")
        .parse()
        .expect("a port")
}

#[test]
fn a_cluster_binds_the_ports_the_environment_names() {
    run_child(
        "a_cluster_here_binds_the_ports_the_environment_names",
        &[
            ("SUNDOG_GOSSIP_PORT", free_udp_port().to_string().into()),
            ("SUNDOG_DATA_PORT", free_tcp_port().to_string().into()),
        ],
    );
}

#[test]
fn a_cluster_refuses_a_port_variable_that_is_no_port() {
    run_child(
        "a_cluster_here_refuses_the_data_port_variable",
        &[("SUNDOG_DATA_PORT", "data".into())],
    );
}

#[cfg(unix)]
#[test]
fn a_cluster_refuses_a_port_variable_that_is_not_utf8() {
    use std::os::unix::ffi::OsStringExt;
    run_child(
        "a_cluster_here_refuses_the_data_port_variable",
        &[("SUNDOG_DATA_PORT", OsString::from_vec(b"79\xff47".to_vec()))],
    );
}

#[tokio::test]
#[ignore = "a_cluster_binds_the_ports_the_environment_names runs it with the variables set"]
async fn a_cluster_here_binds_the_ports_the_environment_names() {
    if std::env::var_os(CHILD).is_none() {
        return;
    }
    let (gossip, data) = (
        port_from("SUNDOG_GOSSIP_PORT"),
        port_from("SUNDOG_DATA_PORT"),
    );
    let cluster = Cluster::builder("env-ports")
        .config(loopback_config())
        .seeds(std::iter::empty())
        .build()
        .await
        .expect("the cluster builds");
    assert_eq!(
        cluster.local_gossip_addr(),
        SocketAddr::from((Ipv4Addr::LOCALHOST, gossip)),
        "gossip binds the configured IP on the port the environment names"
    );
    TcpStream::connect((Ipv4Addr::LOCALHOST, data))
        .expect("the data plane listens on the port the environment names");
    cluster.shutdown().await;
}

#[tokio::test]
#[ignore = "a_cluster_refuses_a_port_variable_that_is_no_port runs it with the variable set"]
async fn a_cluster_here_refuses_the_data_port_variable() {
    if std::env::var_os(CHILD).is_none() {
        return;
    }
    let error = Cluster::builder("env-ports")
        .config(loopback_config())
        .seeds(std::iter::empty())
        .build()
        .await
        .expect_err("SUNDOG_DATA_PORT=data is no port");
    assert!(
        matches!(&error, JoinError::InvalidConfig(message) if message.contains("SUNDOG_DATA_PORT")),
        "{error}"
    );
}

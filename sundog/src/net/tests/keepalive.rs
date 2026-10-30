//! Every connection a mesh dials or accepts runs TCP keepalive at
//! `ClusterConfig::mesh_keepalive`, so a link whose peer vanished without
//! closing it errors instead of being held forever. Linux only: the tests find
//! the mesh's own sockets through `/proc/self` and read their options back
//! with `getsockopt`.

use std::os::fd::RawFd;

use super::*;

/// `fd`'s TCP keepalive as `(idle, interval, probes)`, idle and interval in
/// seconds, or `None` when keepalive is off.
pub(in crate::net) fn keepalive_settings(fd: RawFd) -> Option<(i32, i32, i32)> {
    let option = |level: libc::c_int, name: libc::c_int| {
        let mut value: libc::c_int = 0;
        let mut len = libc::socklen_t::try_from(std::mem::size_of::<libc::c_int>())
            .expect("an int's size fits in socklen_t");
        // SAFETY: `fd` is an open socket this process owns, and `value` and
        // `len` are a live `c_int` and its size, as `getsockopt` requires.
        let rc =
            unsafe { libc::getsockopt(fd, level, name, (&raw mut value).cast(), &raw mut len) };
        assert_eq!(rc, 0, "getsockopt({level}, {name}) on fd {fd}");
        value
    };
    (option(libc::SOL_SOCKET, libc::SO_KEEPALIVE) != 0).then(|| {
        (
            option(libc::IPPROTO_TCP, libc::TCP_KEEPIDLE),
            option(libc::IPPROTO_TCP, libc::TCP_KEEPINTVL),
            option(libc::IPPROTO_TCP, libc::TCP_KEEPCNT),
        )
    })
}

/// The fd of this process's established IPv4 TCP socket whose local and
/// remote ports `matches` accepts, from `/proc/self/net/tcp` and
/// `/proc/self/fd`.
fn established_socket(matches: impl Fn(u16, u16) -> bool) -> Option<RawFd> {
    let port = |field: &str| u16::from_str_radix(field.rsplit(':').next()?, 16).ok();
    let inodes: Vec<String> = std::fs::read_to_string("/proc/self/net/tcp")
        .ok()?
        .lines()
        .skip(1)
        .filter_map(|line| {
            let fields: Vec<&str> = line.split_whitespace().collect();
            let (local, remote) = (port(fields.get(1)?)?, port(fields.get(2)?)?);
            (*fields.get(3)? == "01" && matches(local, remote))
                .then(|| fields.get(9).map(|inode| (*inode).to_string()))?
        })
        .collect();
    std::fs::read_dir("/proc/self/fd")
        .ok()?
        .flatten()
        .find_map(|entry| {
            let target = std::fs::read_link(entry.path()).ok()?;
            let target = target.to_str()?;
            inodes
                .iter()
                .any(|inode| target == format!("socket:[{inode}]"))
                .then(|| entry.file_name().to_str()?.parse().ok())?
        })
}

/// Whether `fd` has `TCP_NODELAY` set, which the mesh sets together with
/// keepalive when it readies a socket.
fn nodelay(fd: RawFd) -> bool {
    let mut value: libc::c_int = 0;
    let mut len = libc::socklen_t::try_from(std::mem::size_of::<libc::c_int>())
        .expect("an int's size fits in socklen_t");
    // SAFETY: as in `keepalive_settings`.
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_NODELAY,
            (&raw mut value).cast(),
            &raw mut len,
        )
    };
    assert_eq!(rc, 0, "getsockopt(TCP_NODELAY) on fd {fd}");
    value != 0
}

/// Polls for up to two seconds until `matches` names an established socket
/// with keepalive on, and returns its settings.
async fn keepalive_of(matches: impl Fn(u16, u16) -> bool) -> Option<(i32, i32, i32)> {
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(settings) = established_socket(&matches).and_then(keepalive_settings) {
            return Some(settings);
        }
        if std::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn greeting(node: u64) -> Greeting {
    Greeting {
        node: NodeId::from(node),
        incarnation: 1,
        cluster: wire::UNNAMED_CLUSTER,
    }
}

#[tokio::test]
async fn a_connection_the_mesh_accepts_runs_keepalive_at_mesh_keepalive() {
    let bound = BoundListener::bind("127.0.0.1:0".parse().expect("valid loopback addr"))
        .await
        .expect("bind loopback");
    let port = bound.local_addr().port();
    let config =
        ClusterConfig::default().with(|c| c.mesh_keepalive = Some(Duration::from_secs(20)));
    let (mesh, _inbound) =
        Mesh::spawn_on(bound, greeting(1), &config, empty_handler()).expect("spawn the mesh");
    let _client = tokio::net::TcpStream::connect(mesh.local_addr())
        .await
        .expect("connect to the mesh");

    assert_eq!(
        keepalive_of(|local, _| local == port).await,
        Some((20, 6, 3)),
        "the accepted end runs keepalive"
    );
}

#[tokio::test]
async fn a_connection_the_mesh_dials_runs_keepalive_at_mesh_keepalive() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let peer_addr = listener.local_addr().expect("listener has a local addr");
    let config =
        ClusterConfig::default().with(|c| c.mesh_keepalive = Some(Duration::from_secs(12)));
    let bound = BoundListener::bind("127.0.0.1:0".parse().expect("valid loopback addr"))
        .await
        .expect("bind loopback");
    let (mesh, _inbound) =
        Mesh::spawn_on(bound, greeting(1), &config, empty_handler()).expect("spawn the mesh");
    mesh.update_peers(vec![peer_at(NodeId::from(2), peer_addr)]);
    let (_accepted, _) = listener.accept().await.expect("the writer dials the peer");

    assert_eq!(
        keepalive_of(|_, remote| remote == peer_addr.port()).await,
        Some((12, 4, 3)),
        "the dialed end runs keepalive"
    );
}

#[tokio::test]
async fn mesh_keepalive_none_leaves_keepalive_off() {
    let bound = BoundListener::bind("127.0.0.1:0".parse().expect("valid loopback addr"))
        .await
        .expect("bind loopback");
    let port = bound.local_addr().port();
    let config = ClusterConfig::default().with(|c| c.mesh_keepalive = None);
    let (mesh, _inbound) =
        Mesh::spawn_on(bound, greeting(1), &config, empty_handler()).expect("spawn the mesh");
    let _client = tokio::net::TcpStream::connect(mesh.local_addr())
        .await
        .expect("connect to the mesh");
    // The mesh readied the socket once it has TCP_NODELAY on.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let accepted = loop {
        if let Some(fd) = established_socket(|local, _| local == port)
            && nodelay(fd)
        {
            break fd;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the mesh accepts and readies the connection within two seconds"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_eq!(keepalive_settings(accepted), None);
}

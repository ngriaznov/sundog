//! Connections to the data-plane port that never send a hello leave the nodes' own links alone.

use std::net::SocketAddr;
use std::time::Instant;

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;

use super::*;
use crate::cluster::test_support::wait_until;
use crate::net::HELLO_TIMEOUT;

/// How many silent and how many partial connections each node's port holds.
const HELD_PER_KIND: usize = 8;

/// Opens `HELD_PER_KIND` connections to `addr` that send nothing and as many
/// that send two bytes of a length prefix, and keeps them open.
async fn hold_unfinished_connections(addr: SocketAddr) -> Vec<TcpStream> {
    let mut held = Vec::new();
    for _ in 0..HELD_PER_KIND {
        held.push(TcpStream::connect(addr).await.expect("the port accepts"));
    }
    for _ in 0..HELD_PER_KIND {
        let mut stream = TcpStream::connect(addr).await.expect("the port accepts");
        stream
            .write_all(&[0, 0])
            .await
            .expect("two bytes of a length prefix");
        held.push(stream);
    }
    held
}

#[tokio::test]
async fn silent_and_partial_data_plane_connections_do_not_stop_two_nodes_replicating() {
    let (cluster_a, cluster_b) = two_node_cluster("cluster-it-silent-data-plane").await;
    let cache_a = cluster_a
        .cache::<u32, String>("accept")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("a opens");
    let cache_b = cluster_b
        .cache::<u32, String>("accept")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("b opens");

    let opened = Instant::now();
    let mut held = hold_unfinished_connections(cluster_a.mesh().local_addr()).await;
    held.extend(hold_unfinished_connections(cluster_b.mesh().local_addr()).await);

    cache_a.insert(1, "from a".into()).await.expect("a inserts");
    cache_b.insert(2, "from b".into()).await.expect("b inserts");
    wait_until(
        Duration::from_secs(10),
        "both inserts replicate while the connections are held",
        async || {
            cache_b.get(&1).await.as_deref() == Some("from a")
                && cache_a.get(&2).await.as_deref() == Some("from b")
        },
    )
    .await;

    // Each held connection is closed by its node at the deadline, not before.
    let earliest = HELLO_TIMEOUT.saturating_sub(Duration::from_secs(1));
    for mut stream in held {
        let mut byte = [0u8; 1];
        let read = tokio::time::timeout(
            HELLO_TIMEOUT + Duration::from_secs(15),
            stream.read(&mut byte),
        )
        .await
        .expect("the node closes a connection that sent no hello");
        assert!(matches!(read, Ok(0) | Err(_)), "{read:?}");
        let closed_after = opened.elapsed();
        assert!(
            closed_after >= earliest,
            "a connection closed after {closed_after:?}, before the {HELLO_TIMEOUT:?} deadline"
        );
    }

    // The nodes' own links were up throughout.
    cache_a.insert(3, "after".into()).await.expect("a inserts");
    wait_until(
        Duration::from_secs(10),
        "an insert after the closes replicates",
        async || cache_b.get(&3).await.as_deref() == Some("after"),
    )
    .await;
    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

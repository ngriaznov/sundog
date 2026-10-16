//! The cap on connections from unknown sources that wait for their `Hello`,
//! over a real accept loop on a loopback listener. Every test runs on the
//! real clock: each one needs socket events, and none reaches the deadline.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::{Dropped, framed, hello, invalidate, peer, send_each};
use crate::net::conn::accept::accept_loop;
use crate::net::conn::tests::{EmptyHandler, no_tls};
use crate::net::{InboundMsg, MeshInner};
use crate::node::NodeId;
use crate::wire::{self, Msg};

/// Real-time bound on an event that happens at once.
const AT_ONCE: Duration = Duration::from_secs(5);

const LOOPBACK: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// A mesh with no peers whose real accept loop listens on loopback, with a
/// cap of `cap` connections waiting for their `Hello`.
struct Node {
    addr: SocketAddr,
    mesh: Arc<MeshInner>,
    inbound: mpsc::Receiver<InboundMsg>,
    cancel: CancellationToken,
    accept: JoinHandle<()>,
}

async fn node_with_cap(cap: usize) -> Node {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("listener has a local addr");
    let cancel = CancellationToken::new();
    let mesh = MeshInner::for_tests_with_cap(no_tls(), cancel.clone(), cap);
    let (inbound_tx, inbound) = mpsc::channel(128);
    let accept = tokio::spawn(accept_loop(
        listener,
        inbound_tx,
        Arc::new(EmptyHandler),
        Arc::clone(&mesh),
    ));
    Node {
        addr,
        mesh,
        inbound,
        cancel,
        accept,
    }
}

impl Node {
    async fn connect(&self) -> TcpStream {
        TcpStream::connect(self.addr).await.expect("connect")
    }

    /// Connects and sends a hello followed by `Invalidate(n)`.
    async fn connect_and_greet(&self, n: u8) -> TcpStream {
        let mut client = self.connect().await;
        send_each(&mut client, &[hello(), invalidate(n)]).await;
        client
    }

    /// Waits until `in_flight()` is `want`.
    async fn wait_in_flight(&self, want: usize) {
        tokio::time::timeout(AT_ONCE, async {
            while self.mesh.admission.in_flight() != want {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "in_flight reached {} instead of {want}",
                self.mesh.admission.in_flight()
            )
        });
    }

    /// Waits for the next message the handlers forward.
    async fn next(&mut self) -> InboundMsg {
        tokio::time::timeout(AT_ONCE, self.inbound.recv())
            .await
            .expect("a message is forwarded")
            .expect("the inbound channel is open")
    }

    async fn stop(self) {
        self.cancel.cancel();
        self.accept.await.expect("the accept loop does not panic");
    }
}

/// Asserts `client` reads EOF or a reset at once: the node closed it.
async fn assert_closed_at_once(client: &mut TcpStream) {
    let mut byte = [0u8; 1];
    let read = tokio::time::timeout(AT_ONCE, client.read(&mut byte))
        .await
        .expect("a refused connection is closed at once");
    assert!(matches!(read, Ok(0) | Err(_)), "{read:?}");
}

#[tokio::test]
async fn unknown_sources_over_the_cap_are_closed_at_once_and_an_established_peer_is_unaffected() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let dropped = Dropped::new(recorder.snapshotter());
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let mut node = node_with_cap(3).await;

    // An established peer holds no slot.
    let mut established = node.connect_and_greet(1).await;
    assert_eq!(node.next().await.msg, invalidate(1));
    node.wait_in_flight(0).await;

    // Three silent connections fill the gate.
    let silent = [
        node.connect().await,
        node.connect().await,
        node.connect().await,
    ];
    node.wait_in_flight(3).await;
    assert_eq!(dropped.total("over_cap"), None);

    // The next connection is closed as it is accepted.
    let mut over = node.connect().await;
    assert_closed_at_once(&mut over).await;
    node.wait_in_flight(3).await;
    assert_eq!(dropped.total("over_cap"), Some(1));

    // The established link keeps serving.
    send_each(&mut established, &[invalidate(2)]).await;
    assert_eq!(node.next().await.msg, invalidate(2));

    // A peer that dials now meets the same full gate.
    let mut newcomer = node.connect().await;
    assert_closed_at_once(&mut newcomer).await;
    assert_eq!(dropped.total("over_cap"), Some(2));
    assert_eq!(dropped.total("hello_timeout"), None);

    drop(silent);
    node.stop().await;
}

#[tokio::test]
async fn a_slot_freed_by_a_closed_connection_admits_the_next_connection() {
    let mut node = node_with_cap(3).await;
    let mut silent = vec![
        node.connect().await,
        node.connect().await,
        node.connect().await,
    ];
    node.wait_in_flight(3).await;
    let mut refused = node.connect().await;
    assert_closed_at_once(&mut refused).await;

    // Dropping one socket frees its slot when the handler reads the EOF.
    drop(silent.remove(0));
    node.wait_in_flight(2).await;

    let _greeted = node.connect_and_greet(7).await;
    assert_eq!(node.next().await.msg, invalidate(7));
    node.wait_in_flight(2).await;
    node.stop().await;
}

#[tokio::test]
async fn a_slot_is_freed_by_the_hello_while_the_connection_stays_open() {
    let mut node = node_with_cap(3).await;
    let mut client = node.connect().await;
    node.wait_in_flight(1).await;

    send_each(&mut client, &[hello(), invalidate(1)]).await;

    assert_eq!(node.next().await.msg, invalidate(1));
    node.wait_in_flight(0).await;
    send_each(&mut client, &[invalidate(2)]).await;
    assert_eq!(
        node.next().await.msg,
        invalidate(2),
        "the link still serves"
    );
    node.stop().await;
}

#[tokio::test]
async fn a_known_source_is_admitted_while_the_gate_is_full() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let dropped = Dropped::new(recorder.snapshotter());
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let mut node = node_with_cap(3).await;
    let silent = [
        node.connect().await,
        node.connect().await,
        node.connect().await,
    ];
    node.wait_in_flight(3).await;
    let mut refused = node.connect().await;
    assert_closed_at_once(&mut refused).await;
    assert_eq!(dropped.total("over_cap"), Some(1));

    // The peer table learns this address.
    node.mesh.admission.set_known_sources([LOOPBACK]);
    let _known = node.connect_and_greet(5).await;

    assert_eq!(node.next().await.msg, invalidate(5));
    node.wait_in_flight(3).await;
    assert_eq!(
        dropped.total("over_cap"),
        Some(1),
        "the known source was not refused"
    );

    // A known source is still held to the frame limit before its hello.
    let mut oversized = node.connect().await;
    oversized
        .write_all(&(2u32 << 20).to_be_bytes())
        .await
        .expect("a length prefix declaring 2 MiB");
    assert_closed_at_once(&mut oversized).await;

    drop(silent);
    node.stop().await;
}

#[tokio::test]
async fn a_storm_of_clients_below_the_cap_is_fully_served() {
    const CLIENTS: u64 = 64;
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let dropped = Dropped::new(recorder.snapshotter());
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let mut node = node_with_cap(crate::net::conn::admit::MAX_PRE_HELLO_CONNS).await;

    let mut clients = Vec::new();
    for id in 0..CLIENTS {
        let mut client = node.connect().await;
        let greeting = Msg::Hello {
            node: NodeId::from(id),
            incarnation: 1,
            protocol: wire::PROTOCOL_VERSION,
            cluster: wire::UNNAMED_CLUSTER,
        };
        let mut both = framed(&greeting);
        both.extend(framed(&invalidate(0)));
        client.write_all(&both).await.expect("hello and a message");
        clients.push(client);
    }

    let mut senders = std::collections::BTreeSet::new();
    for _ in 0..CLIENTS {
        senders.insert(node.next().await.from);
    }
    assert_eq!(senders.len(), 64, "every client's message arrived");
    node.wait_in_flight(0).await;
    assert_eq!(dropped.total("over_cap"), None);
    drop(clients);
    node.stop().await;
}

#[cfg(feature = "prometheus")]
#[tokio::test]
async fn over_cap_closes_appear_in_the_prometheus_exposition() {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let node = node_with_cap(1).await;
    let _silent = node.connect().await;
    node.wait_in_flight(1).await;

    let mut over = node.connect().await;
    assert_closed_at_once(&mut over).await;

    let text = handle.render();
    assert!(
        text.contains("sundog_accept_dropped_total{reason=\"over_cap\"} 1"),
        "{text}"
    );
    assert!(
        !text.contains("reason=\"hello_timeout\""),
        "a path never taken exports no series: {text}"
    );
    node.stop().await;
}

#[tokio::test]
async fn the_cap_is_512_on_a_spawned_mesh() {
    let (mesh, _inbound) =
        crate::net::test_support::spawn_mesh(peer(), Arc::new(EmptyHandler)).await;
    assert_eq!(mesh.inner.admission.in_flight(), 0);
    assert_eq!(mesh.inner.admission.cap(), 512);
}

//! Tests of the accept loop's admission over real loopback sockets: the wait
//! between failed accepts, the deadline and frame limit on a connection that
//! has not sent its `Hello`, and the helpers the topic modules share.
//!
//! A paused clock appears only where the asserted event is a timer firing,
//! observed through a `JoinHandle` or a counter, with no `timeout` guard
//! armed while waiting on socket IO: a paused runtime advances to the next
//! timer as soon as it has no ready task, so a socket event that arrives
//! after that point loses to the timer by the whole gap. Tests that need a
//! socket event run on the real clock with real-time guards.

mod backoff;
mod cap;
mod hello;
#[cfg(feature = "tls")]
mod tls;

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use metrics_util::debugging::{DebugValue, Snapshotter};
use tokio::io::AsyncWriteExt as _;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::layer::SubscriberExt as _;

use super::accept::handle_accepted;
use super::admit::Admit;
use super::tests::EmptyHandler;
use crate::hlc::Hlc;
use crate::net::{InboundMsg, MeshInner, TlsCtx};
use crate::node::NodeId;
use crate::wire::{self, Msg, WireRecord};

/// A connected loopback pair: the client end, then the server end as an
/// accept returns it.
async fn loopback_pair() -> (TcpStream, TcpStream, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("listener has a local addr");
    let accept = tokio::spawn(async move { listener.accept().await.expect("accept") });
    let client = TcpStream::connect(addr).await.expect("connect");
    let (server, client_addr) = accept.await.expect("connection accepted");
    (client, server, client_addr)
}

/// One `handle_accepted` task over a loopback pair, with the client end, the
/// channel the handler forwards to and the mesh it runs in.
struct Accepted {
    client: TcpStream,
    task: JoinHandle<()>,
    cancel: CancellationToken,
    inbound: mpsc::Receiver<InboundMsg>,
    mesh: Arc<MeshInner>,
}

/// Spawns `handle_accepted` on the server end of a fresh loopback pair, in a
/// mesh with no peers that runs TLS per `tls`. The connection holds the slot
/// in the cap that the accept loop gives a connection from an unknown source.
async fn spawn_accepted(tls: TlsCtx) -> Accepted {
    let mesh = MeshInner::for_tests(tls, CancellationToken::new());
    spawn_accepted_in(&mesh).await
}

/// [`spawn_accepted`] in an existing `mesh`, which several connections share.
async fn spawn_accepted_in(mesh: &Arc<MeshInner>) -> Accepted {
    let (client, server, client_addr) = loopback_pair().await;
    let (inbound_tx, inbound) = mpsc::channel(8);
    let pre_hello = match mesh.admission.admit(client_addr.ip()) {
        Admit::Known => None,
        Admit::Waiting(permit) => Some(permit),
        Admit::OverCap => panic!("the test mesh is under its cap"),
    };
    let task = tokio::spawn(handle_accepted(
        server,
        inbound_tx,
        Arc::new(EmptyHandler),
        Arc::clone(mesh),
        pre_hello,
    ));
    Accepted {
        client,
        task,
        cancel: mesh.accept_cancel.clone(),
        inbound,
        mesh: Arc::clone(mesh),
    }
}

/// The node every test peer claims to be.
fn peer() -> NodeId {
    NodeId::from(9)
}

fn hello() -> Msg {
    Msg::Hello {
        node: peer(),
        incarnation: 1,
        protocol: wire::PROTOCOL_VERSION,
        cluster: wire::UNNAMED_CLUSTER,
    }
}

/// A small broadcast message, forwarded to the inbound channel once the
/// handler serves the connection.
fn invalidate(n: u8) -> Msg {
    Msg::Invalidate {
        cache: "users".into(),
        key: Bytes::from(vec![n]),
        ver: Hlc {
            wall_ms: u64::from(n),
            logical: 0,
            node: peer(),
        },
    }
}

/// A replicate message whose frame is `value_len` bytes and a little more.
fn replicate_of(value_len: usize) -> Msg {
    Msg::Replicate {
        cache: "users".into(),
        rec: WireRecord {
            key: Bytes::from_static(b"k"),
            value: Some(Bytes::from(vec![7u8; value_len])),
            ver: Hlc {
                wall_ms: 1,
                logical: 0,
                node: peer(),
            },
            expires_at_ms: None,
        },
    }
}

/// `msg` as a length-prefixed frame.
fn framed(msg: &Msg) -> Vec<u8> {
    let body = wire::encode(msg).expect("encodes");
    let mut frame = u32::try_from(body.len())
        .expect("a frame fits u32")
        .to_be_bytes()
        .to_vec();
    frame.extend_from_slice(&body);
    frame
}

/// Writes every message in `msgs` as one frame each, in separate writes.
async fn send_each(client: &mut TcpStream, msgs: &[Msg]) {
    for msg in msgs {
        client.write_all(&framed(msg)).await.expect("write");
    }
}

/// Reads `sundog_accept_dropped_total{reason}` from a recorder. A snapshot
/// resets the counters it reads, so the reader keeps the running totals.
struct Dropped {
    snapshotter: Snapshotter,
    totals: Mutex<HashMap<String, u64>>,
}

impl Dropped {
    fn new(snapshotter: Snapshotter) -> Self {
        Self {
            snapshotter,
            totals: Mutex::new(HashMap::new()),
        }
    }

    /// The total counted under `reason`, `None` while the series does not
    /// exist.
    fn total(&self, reason: &str) -> Option<u64> {
        let mut totals = self.totals.lock().expect("lock");
        for (key, _, _, value) in self.snapshotter.snapshot().into_vec() {
            if key.key().name() != "sundog_accept_dropped_total" {
                continue;
            }
            let Some(label) = key.key().labels().find(|label| label.key() == "reason") else {
                continue;
            };
            let DebugValue::Counter(n) = value else {
                panic!("a counter, got {value:?}");
            };
            *totals.entry(label.value().to_owned()).or_default() += n;
        }
        totals.get(reason).copied()
    }
}

/// A tracing layer that keeps the message of every `WARN` event.
#[derive(Clone, Default)]
struct Warnings(Arc<Mutex<Vec<String>>>);

/// Collects the `message` field of one event.
struct Message(String);

impl tracing::field::Visit for Message {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Warnings {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if *event.metadata().level() != tracing::Level::WARN {
            return;
        }
        let mut message = Message(String::new());
        event.record(&mut message);
        self.0.lock().expect("lock").push(message.0);
    }
}

impl Warnings {
    /// Installs the layer as this thread's subscriber while the guard lives.
    fn capture() -> (Self, crate::cluster::test_support::ScopedSubscriber) {
        let warnings = Self::default();
        let guard = crate::cluster::test_support::scoped_subscriber(
            tracing_subscriber::registry().with(warnings.clone()),
        );
        (warnings, guard)
    }

    /// The warnings logged since the last call.
    fn taken(&self) -> Vec<String> {
        std::mem::take(&mut *self.0.lock().expect("lock"))
    }
}

/// An accept failure that persists until a descriptor frees.
fn descriptor_limit_error() -> io::Error {
    #[cfg(unix)]
    {
        io::Error::from_raw_os_error(24)
    }
    #[cfg(not(unix))]
    {
        io::Error::other("too many open files")
    }
}

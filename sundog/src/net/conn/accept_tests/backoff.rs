//! The accept loop over a scripted source: the wait between failed accepts.

use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures::SinkExt as _;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::codec::LengthDelimitedCodec;
use tokio_util::sync::CancellationToken;

use super::{Warnings, descriptor_limit_error, loopback_pair};
use crate::net::conn::accept::{ConnSource, accept_loop_over};
use crate::net::conn::tests::{EmptyHandler, no_tls};
use crate::net::{InboundMsg, MeshInner};
use crate::node::NodeId;
use crate::wire::{self, MAX_FRAME, Msg};

/// One answer of a [`Script`].
enum Step {
    Fail(io::Error),
    Conn(TcpStream),
}

/// A [`ConnSource`] that plays `steps` in order and then never answers. It
/// records the instant of every `next_conn` call.
struct Script {
    steps: Box<dyn Iterator<Item = Step> + Send>,
    calls: Arc<Mutex<Vec<Instant>>>,
}

impl Script {
    fn new(steps: impl Iterator<Item = Step> + Send + 'static) -> (Self, Arc<Mutex<Vec<Instant>>>) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let script = Self {
            steps: Box::new(steps),
            calls: Arc::clone(&calls),
        };
        (script, calls)
    }
}

impl ConnSource for Script {
    async fn next_conn(&mut self) -> io::Result<(TcpStream, SocketAddr)> {
        self.calls.lock().expect("lock").push(Instant::now());
        match self.steps.next() {
            Some(Step::Fail(error)) => Err(error),
            Some(Step::Conn(stream)) => {
                let peer = stream.peer_addr()?;
                Ok((stream, peer))
            }
            None => std::future::pending().await,
        }
    }
}

fn failures(kind: io::ErrorKind, count: usize) -> impl Iterator<Item = Step> + Send + 'static {
    std::iter::repeat_with(move || Step::Fail(io::Error::from(kind))).take(count)
}

fn endless_failures() -> impl Iterator<Item = Step> + Send + 'static {
    std::iter::repeat_with(|| Step::Fail(descriptor_limit_error()))
}

/// The milliseconds between consecutive recorded calls.
fn gaps_ms(calls: &Mutex<Vec<Instant>>) -> Vec<u128> {
    let calls = calls.lock().expect("lock");
    calls
        .windows(2)
        .map(|pair| (pair[1] - pair[0]).as_millis())
        .collect()
}

/// Spawns the accept loop over `script` on a mesh with no peers.
fn spawn_loop(
    script: Script,
) -> (
    JoinHandle<()>,
    CancellationToken,
    mpsc::Receiver<InboundMsg>,
) {
    let cancel = CancellationToken::new();
    let (inbound_tx, inbound_rx) = mpsc::channel(8);
    let looped = tokio::spawn(accept_loop_over(
        script,
        inbound_tx,
        Arc::new(EmptyHandler),
        MeshInner::for_tests(no_tls(), cancel.clone()),
    ));
    (looped, cancel, inbound_rx)
}

#[tokio::test(start_paused = true)]
async fn accept_errors_back_off_and_a_success_resets_the_schedule() {
    let (_client, server, _) = loopback_pair().await;
    let steps = failures(io::ErrorKind::Other, 9)
        .chain(std::iter::once(Step::Conn(server)))
        .chain(failures(io::ErrorKind::Other, 3));
    let (script, calls) = Script::new(steps);
    let (looped, cancel, _inbound) = spawn_loop(script);

    // The test body's own timer lets the loop's timers fire one by one.
    tokio::time::sleep(Duration::from_secs(5)).await;

    let expected: [u128; 13] = [5, 10, 20, 40, 80, 160, 320, 640, 1000, 0, 5, 10, 20];
    let gaps = gaps_ms(&calls);
    assert_eq!(gaps.len(), expected.len(), "gaps: {gaps:?}");
    for (index, (gap, want)) in gaps.iter().zip(expected).enumerate() {
        assert!(
            (want..=want + 2).contains(gap),
            "gap {index} is {gap} ms, expected {want} ms; gaps: {gaps:?}"
        );
    }
    cancel.cancel();
    looped.await.expect("the loop does not panic");
}

#[tokio::test(start_paused = true)]
async fn a_persistent_accept_error_does_not_spin() {
    let (script, calls) = Script::new(endless_failures());
    let (looped, cancel, _inbound) = spawn_loop(script);

    tokio::time::sleep(Duration::from_secs(10_000)).await;

    let attempts = calls.lock().expect("lock").len();
    assert!(
        (9_990..=10_010).contains(&attempts),
        "one attempt a second after the first nine waits: {attempts} attempts in 10,000 s"
    );
    cancel.cancel();
    looped.await.expect("the loop does not panic");
}

#[tokio::test(start_paused = true)]
async fn cancel_wakes_the_loop_from_its_backoff() {
    let (script, _calls) = Script::new(endless_failures());
    let (looped, cancel, _inbound) = spawn_loop(script);
    tokio::time::sleep(Duration::from_secs(3)).await;

    let cancelled_at = Instant::now();
    cancel.cancel();
    looped.await.expect("the loop does not panic");

    assert!(
        cancelled_at.elapsed() < Duration::from_millis(1),
        "the loop left its wait of up to a second at once"
    );
}

#[tokio::test(start_paused = true)]
async fn per_connection_accept_errors_consume_no_virtual_time() {
    let steps = failures(io::ErrorKind::ConnectionAborted, 20)
        .chain(failures(io::ErrorKind::ConnectionReset, 20))
        .chain(failures(io::ErrorKind::Interrupted, 20))
        .chain(failures(io::ErrorKind::Other, 1));
    let (script, calls) = Script::new(steps);
    let (looped, cancel, _inbound) = spawn_loop(script);

    tokio::time::sleep(Duration::from_secs(1)).await;

    let gaps = gaps_ms(&calls);
    assert_eq!(gaps.len(), 61, "gaps: {gaps:?}");
    assert!(
        gaps[..60].iter().all(|&gap| gap == 0),
        "sixty skipped connections take no time: {gaps:?}"
    );
    assert!(
        (5..=7).contains(&gaps[60]),
        "the first failure that waits starts the schedule at 5 ms, not after the sixty skipped \
         ones: {gaps:?}"
    );
    cancel.cancel();
    looped.await.expect("the loop does not panic");
}

#[tokio::test(start_paused = true)]
async fn an_abort_followed_by_descriptor_limit_errors_still_warns() {
    let (warnings, _log) = Warnings::capture();
    let steps = failures(io::ErrorKind::ConnectionAborted, 1).chain(endless_failures());
    let (script, _calls) = Script::new(steps);
    let (looped, cancel, _inbound) = spawn_loop(script);

    tokio::time::sleep(Duration::from_secs(1)).await;

    let logged = warnings.taken();
    assert_eq!(
        logged.len(),
        1,
        "the abort claims no warning and the errors after the first stay inside the spacing: \
         {logged:?}"
    );
    assert!(logged[0].contains("file descriptors"), "{logged:?}");
    cancel.cancel();
    looped.await.expect("the loop does not panic");
}

#[tokio::test]
async fn a_connection_accepted_after_failed_accepts_is_served() {
    let (mut client, server, _) = loopback_pair().await;
    let steps = failures(io::ErrorKind::Other, 2).chain(std::iter::once(Step::Conn(server)));
    let (script, _calls) = Script::new(steps);
    let (looped, cancel, mut inbound) = spawn_loop(script);

    let mut framed = LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME)
        .new_framed(&mut client);
    let from = NodeId::from(9);
    let hello = Msg::Hello {
        node: from,
        incarnation: 1,
        protocol: wire::PROTOCOL_VERSION,
        cluster: wire::UNNAMED_CLUSTER,
    };
    let invalidate = Msg::Invalidate {
        cache: "users".into(),
        key: Bytes::from_static(b"k"),
        ver: crate::hlc::Hlc {
            wall_ms: 1,
            logical: 0,
            node: from,
        },
    };
    for msg in [&hello, &invalidate] {
        framed
            .send(wire::encode(msg).expect("encodes"))
            .await
            .expect("send");
    }

    let delivered = tokio::time::timeout(Duration::from_secs(5), inbound.recv())
        .await
        .expect("the connection accepted after two failures is served")
        .expect("the inbound channel is open");
    assert_eq!(delivered.from, from);
    assert_eq!(delivered.msg, invalidate);
    cancel.cancel();
    looped.await.expect("the loop does not panic");
}

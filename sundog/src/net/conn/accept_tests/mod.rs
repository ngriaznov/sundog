//! Tests of the accept loop's admission over real loopback sockets: the wait
//! between failed accepts, and the helpers the topic modules share.
//!
//! A paused clock appears only where the asserted event is a timer firing,
//! observed through a `JoinHandle` or a counter, with no `timeout` guard
//! armed while waiting on socket IO: a paused runtime advances to the next
//! timer as soon as it has no ready task, so a socket event that arrives
//! after that point loses to the timer by the whole gap. Tests that need a
//! socket event run on the real clock with real-time guards.

mod backoff;

use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use tokio::net::{TcpListener, TcpStream};
use tracing_subscriber::layer::SubscriberExt as _;

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

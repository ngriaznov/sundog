//! The per-peer request connection pool and reuse probe.

use std::time::{Duration, Instant};

use futures::FutureExt as _;

use super::framing::{PeerFramed, recv_msg};
use crate::wire::Msg;

/// Max idle pooled request/response connections kept per peer: bounds
/// memory/fd use while letting requests skip a fresh dial on a known peer.
const REQ_POOL_MAX_IDLE_CONNS: usize = 4;

/// Idle bound on an accepted request/response connection: torn down after
/// this long without a new request, so a peer that stops checking a pooled
/// connection back in doesn't hold a server-side socket open forever.
pub(super) const REQ_CONN_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Age bound for a pooled connection at [`ReqPool::checkout`], half of
/// [`REQ_CONN_IDLE_TIMEOUT`]. The server may already have closed anything
/// older than this, so a connection past the bound is dropped instead of
/// handed back: reusing it would still write successfully into the
/// half-closed socket, only to see the first read come back `EOF`.
pub(super) const REQ_POOL_MAX_IDLE: Duration = Duration::from_secs(30);

/// True when a connection checked into the pool at `checked_in` is still
/// within [`REQ_POOL_MAX_IDLE`] of `now`.
pub(super) fn pooled_is_fresh(checked_in: Instant, now: Instant) -> bool {
    now.duration_since(checked_in) < REQ_POOL_MAX_IDLE
}

/// Bounds how many requests one accepted connection serves before this
/// side closes it, guarding against one reused forever without going idle.
pub(super) const REQ_CONN_MAX_REQUESTS: usize = 4096;

/// A small per-peer pool of request/response connections that have already
/// completed `Hello`, reused instead of dialing fresh every time. Separate
/// from the persistent per-peer writer connection, so a slow snapshot
/// transfer never backs up live broadcast traffic.
///
/// A connection is checked back in only after a clean end-of-reply; one
/// left in an unknown framing state is dropped instead of pooled.
pub(in crate::net) struct ReqPool {
    idle: std::sync::Mutex<Vec<(PeerFramed, Instant)>>,
}

impl ReqPool {
    pub(in crate::net) fn new() -> Self {
        Self {
            idle: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Takes one idle connection out of the pool, if any, skipping and
    /// dropping every one checked in more than [`REQ_POOL_MAX_IDLE`] ago:
    /// the server may already have torn it down as idle.
    pub(in crate::net) fn checkout(&self) -> Option<PeerFramed> {
        let mut idle = self
            .idle
            .lock()
            .expect("invariant: req pool lock is never poisoned");
        let now = Instant::now();
        while let Some((framed, checked_in)) = idle.pop() {
            if pooled_is_fresh(checked_in, now) {
                return Some(framed);
            }
        }
        None
    }

    /// Returns a connection known to be in a clean, ready-for-reuse framing
    /// state, stamped with the check-in instant [`ReqPool::checkout`] ages
    /// it against. Silently drops it once the pool is at
    /// [`REQ_POOL_MAX_IDLE_CONNS`].
    pub(in crate::net) fn checkin(&self, framed: PeerFramed) {
        let mut idle = self
            .idle
            .lock()
            .expect("invariant: req pool lock is never poisoned");
        if idle.len() < REQ_POOL_MAX_IDLE_CONNS {
            idle.push((framed, Instant::now()));
        }
    }
}

/// Outcome of [`probe_reused`]: what a reused pooled connection shows right
/// after the caller's request is written onto it.
pub(in crate::net) enum ReusedProbe {
    /// Nothing to read yet: the connection is alive and the reply is still
    /// in flight, unread.
    Pending,
    /// A receive-side `EOF` or a broken read: the server already closed
    /// this connection as idle, or it is otherwise unusable.
    Stale,
    /// A reply frame is already there. Handed back so the caller doesn't
    /// lose it, since reading it here already took it off the wire.
    Ready(Msg),
}

/// Polls `framed` exactly once, without waiting, right after
/// [`Mesh::acquire_conn`][acq] writes onto a connection taken out of the
/// pool. A connection the server already timed out is a half-closed
/// socket: the write into it still succeeds, but the receive side is
/// already at `EOF`, which this catches before the caller commits to a
/// connection that would fail its whole request. A live connection has
/// nothing to read yet, so the probe returns [`ReusedProbe::Pending`]
/// instantly rather than waiting on the real reply.
///
/// [acq]: crate::net::Mesh::acquire_conn
pub(in crate::net) fn probe_reused(framed: &mut PeerFramed) -> ReusedProbe {
    match recv_msg(framed).now_or_never() {
        None => ReusedProbe::Pending,
        Some(None | Some(Err(_))) => ReusedProbe::Stale,
        Some(Some(Ok(msg))) => ReusedProbe::Ready(msg),
    }
}

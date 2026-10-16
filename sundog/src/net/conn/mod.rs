//! Framing and connection tasks: the accept-side demux between persistent
//! mesh traffic and pooled request/response, and the per-peer dial-and-write
//! loop over the broadcast outboxes.
//!
//! - `accept`: the accept loop, the `Hello` handshake and per-connection
//!   dispatch.
//! - `admit`: the admission of accepted connections: the `Hello` deadline,
//!   the wait between failed accepts and the counters and warnings that
//!   report them.
//! - `framing`: framed IO primitives and TLS layering.
//! - `pool`: the per-peer request connection pool and reuse probe.
//! - `reply`: requester-side reply readers and lazy streams.
//! - `serve`: responder-side handlers.
//! - `writer`: dial, the persistent per-peer writer and the replicate coalescer.

mod accept;
#[cfg(all(test, not(feature = "sim")))]
mod accept_tests;
mod admit;
mod framing;
mod pool;
mod reply;
mod serve;
#[cfg(test)]
mod tests;
mod writer;
#[cfg(test)]
mod writer_tests;

pub(super) use accept::accept_loop;
pub(super) use admit::Admission;
#[cfg(all(test, not(feature = "sim")))]
pub(crate) use admit::HELLO_TIMEOUT;
pub(super) use framing::{recv_msg, send_msg};
pub(super) use pool::{ReqPool, ReusedProbe, probe_reused};
pub(super) use reply::{
    Reply, bucket_stream, collect_ae_buckets, collect_ae_mismatches, collect_ae_part_replies,
    collect_ae_round_scoped, collect_fetch_reply, collect_load_reply, collect_pulled_records,
    state_stream,
};
pub(super) use writer::{PeerTarget, dial_with_hello_and, run_peer_writer};
pub(crate) use writer::{REPLICATE_BATCH_BUDGET, REPLICATE_BATCH_COUNT};

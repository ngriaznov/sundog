//! The accept loop and per-connection dispatch.

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::admit::{
    Admit, FIRST_FRAME_TIMEOUT, FRAME_STALL_TIMEOUT, HELLO_TIMEOUT, Slots, accept_backoff,
    accept_error_backs_off, note_read_timeout,
};
use super::framing::{
    PeerFramed, establish_accept, new_framed_pre_hello, recv_msg, send_batch_or_cancelled,
    tune_socket,
};
use super::pool::{REQ_CONN_IDLE_TIMEOUT, REQ_CONN_MAX_REQUESTS};
use super::serve::{
    serve_ae_digest, serve_ae_digest_masked, serve_ae_digest_scoped, serve_ae_entries,
    serve_ae_parts, serve_ae_pull, serve_ae_pull_hashes, serve_fetch, serve_load, serve_st_buckets,
    serve_state_transfer,
};
use crate::error::CodecError;
use crate::net::tcp::{TcpListener, TcpStream};
use crate::net::{InboundMsg, MeshInner, RequestHandler, TlsCtx};
use crate::node::NodeId;
use crate::wire::{self, MAX_FRAME, Msg};

/// Why a read off an accepted connection gave up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReadTimeout {
    /// The wait for a frame to start ran out.
    Idle,
    /// A frame had started and stopped arriving.
    Stalled,
}

/// How long a read off an accepted connection waits for more bytes, given
/// whether the codec stands inside a frame and the connection's idle wait
/// for a frame to start (`idle`, `None` for no bound). Inside a frame the
/// wait is [`FRAME_STALL_TIMEOUT`] whatever the connection is; outside one
/// it is `idle`. Pure; unit tested directly.
pub(super) fn read_wait(mid_frame: bool, idle: Option<Duration>) -> Option<Duration> {
    if mid_frame {
        Some(FRAME_STALL_TIMEOUT)
    } else {
        idle
    }
}

/// Whether the wait for a connection's first frame is bounded by
/// [`FIRST_FRAME_TIMEOUT`]: the connection holds a slot among those from
/// sources that are not peer addresses (`unknown_source`), and its `Hello`
/// names no current mesh peer of the node's cluster (`hello_names_peer`). A
/// peer behind NAT, a published container port or an overlay dials from an
/// address it does not gossip, so its source IP looks unknown, yet its
/// `Hello` names a node in the peer table and its idle broadcast link must
/// stay open. The node id in a `Hello` is not authenticated, so a connection
/// that names a peer keeps its slot among the open connections, which is
/// keyed by its source IP, and is bounded by that cap and by
/// [`FRAME_STALL_TIMEOUT`] inside a frame. Pure; unit tested directly.
pub(super) const fn first_frame_bound_applies(
    unknown_source: bool,
    hello_names_peer: bool,
) -> bool {
    unknown_source && !hello_names_peer
}

/// [`first_frame_bound_applies`] for the connection `from` names, asked only
/// while no frame has arrived: the peer table is not read for a link that is
/// already serving frames.
fn first_frame_is_bounded(
    mesh: &MeshInner,
    from: NodeId,
    unknown_source: bool,
    frames_received: usize,
) -> bool {
    frames_received == 0 && first_frame_bound_applies(unknown_source, mesh.is_peer_node(from))
}

/// How long a connection waits for its next frame to start. A connection
/// that has served a request is a pooling candidate and waits
/// [`REQ_CONN_IDLE_TIMEOUT`]. One whose first frame is bounded
/// (`first_frame_bounded`, [`first_frame_bound_applies`]) and that has
/// received no frame since its `Hello` waits [`FIRST_FRAME_TIMEOUT`]. Every
/// other connection waits without bound: a persistent broadcast link from a
/// peer is idle for as long as the peer has nothing to send, before its first
/// frame and between frames, and the acceptor cannot tell it from a
/// connection that has served nothing. Pure; unit tested directly.
pub(super) fn frame_idle_timeout(
    served_requests: usize,
    frames_received: usize,
    first_frame_bounded: bool,
) -> Option<Duration> {
    if served_requests > 0 {
        Some(REQ_CONN_IDLE_TIMEOUT)
    } else if first_frame_bounded && frames_received == 0 {
        Some(FIRST_FRAME_TIMEOUT)
    } else {
        None
    }
}

/// The `sundog_accept_dropped_total` reason a read that gave up counts under:
/// none for a pooled request connection that went idle, which closes as
/// usual. Pure; unit tested directly.
pub(super) const fn read_timeout_reason(
    timeout: ReadTimeout,
    served_requests: usize,
) -> Option<&'static str> {
    match timeout {
        ReadTimeout::Idle if served_requests > 0 => None,
        ReadTimeout::Idle => Some("first_frame_timeout"),
        ReadTimeout::Stalled => Some("frame_stall"),
    }
}

/// Reads the next message off `framed`. Waits `idle` for a frame to start
/// (without bound for `None`) and [`FRAME_STALL_TIMEOUT`] for the rest of a
/// frame whose length prefix has arrived; the stall wait restarts whenever
/// more of the frame arrives. The outer `Err` says which wait ran out, and
/// the caller closes the connection. Cancel safe: the codec keeps what it
/// has read.
async fn recv_msg_bounded(
    framed: &mut PeerFramed,
    idle: Option<Duration>,
) -> Result<Option<Result<Msg, CodecError>>, ReadTimeout> {
    use futures::StreamExt as _;
    use std::task::Poll;

    let mut timer: Option<(Pin<Box<tokio::time::Sleep>>, bool, usize)> = None;
    let polled = std::future::poll_fn(|cx| {
        if let Poll::Ready(item) = framed.poll_next_unpin(cx) {
            return Poll::Ready(Ok(item));
        }
        let mid_frame = framed.codec().mid_frame();
        let buffered = framed.read_buffer().len();
        // A change of state, or more of the frame, starts the wait afresh.
        if timer
            .as_ref()
            .is_none_or(|(_, mid, len)| *mid != mid_frame || *len != buffered)
        {
            timer = read_wait(mid_frame, idle)
                .map(|wait| (Box::pin(tokio::time::sleep(wait)), mid_frame, buffered));
        }
        if let Some((sleep, ..)) = timer.as_mut()
            && sleep.as_mut().poll(cx).is_ready()
        {
            return Poll::Ready(Err(if mid_frame {
                ReadTimeout::Stalled
            } else {
                ReadTimeout::Idle
            }));
        }
        Poll::Pending
    })
    .await?;
    Ok(match polled {
        Some(Ok(bytes)) => Some(wire::decode(&bytes.freeze())),
        Some(Err(source)) => Some(Err(CodecError::Io(source))),
        None => None,
    })
}

/// Where the accept loop takes connections from: the listener, or a script
/// in a test.
pub(super) trait ConnSource: Send {
    fn next_conn(&mut self) -> impl Future<Output = io::Result<(TcpStream, SocketAddr)>> + Send;
}

impl ConnSource for TcpListener {
    async fn next_conn(&mut self) -> io::Result<(TcpStream, SocketAddr)> {
        self.accept().await
    }
}

/// Accepts connections until `mesh`'s accept token fires, spawning a handler
/// per connection.
pub(in crate::net) async fn accept_loop(
    listener: TcpListener,
    inbound_tx: mpsc::Sender<InboundMsg>,
    handler: Arc<dyn RequestHandler>,
    mesh: Arc<MeshInner>,
) {
    accept_loop_over(listener, inbound_tx, handler, mesh).await;
}

/// [`accept_loop`] over any [`ConnSource`].
///
/// A failed accept that concerns one connection ([`accept_error_backs_off`])
/// is skipped after a yield. Any other failure waits [`accept_backoff`],
/// which grows with the failures in a row and resets on the next accepted
/// connection, so a process out of descriptors retries at most once a
/// second. The wait ends when the accept token fires.
///
/// A connection from the IP of a current peer's data address is admitted
/// without limit. At most `MAX_PRE_HELLO_CONNS` connections
/// from any other source wait for their `Hello` at once, and at most
/// `MAX_UNKNOWN_SOURCE_CONNS` are open at once; one that arrives beyond
/// either is closed as it is accepted, before a handshake or a task exists.
pub(super) async fn accept_loop_over<S: ConnSource>(
    mut source: S,
    inbound_tx: mpsc::Sender<InboundMsg>,
    handler: Arc<dyn RequestHandler>,
    mesh: Arc<MeshInner>,
) {
    let cancel = mesh.accept_cancel.clone();
    let mut failures: u32 = 0;
    loop {
        let accepted = tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            accepted = source.next_conn() => accepted,
        };
        let (stream, peer_addr) = match accepted {
            Ok(conn) => {
                failures = 0;
                conn
            }
            Err(error) => {
                let retry_in = if accept_error_backs_off(error.kind()) {
                    failures = failures.saturating_add(1);
                    accept_backoff(failures)
                } else {
                    Duration::ZERO
                };
                mesh.admission.note_accept_error(&error, failures, retry_in);
                if retry_in.is_zero() {
                    tokio::task::yield_now().await;
                } else {
                    tokio::select! {
                        biased;
                        () = cancel.cancelled() => return,
                        () = tokio::time::sleep(retry_in) => {}
                    }
                }
                continue;
            }
        };
        let pre_hello = match mesh.admission.admit(peer_addr.ip()) {
            Admit::Known => None,
            Admit::Waiting(slots) => Some(slots),
            Admit::OverCap => {
                mesh.admission.note_over_cap(peer_addr);
                // `stream` drops: closed before a handshake or a task exists.
                continue;
            }
            Admit::OverOpenCap => {
                mesh.admission.note_open_cap(peer_addr);
                continue;
            }
        };
        tune_socket(&stream, mesh.keepalive);
        tokio::spawn(handle_accepted(
            stream,
            inbound_tx.clone(),
            Arc::clone(&handler),
            Arc::clone(&mesh),
            pre_hello,
        ));
    }
}

/// A connection whose TLS handshake (when the node runs TLS) and `Hello`
/// are done.
struct OpenedLink {
    framed: PeerFramed,
    from: NodeId,
    peer_protocol: u16,
    peer_cluster: u64,
}

/// Layers TLS onto `stream` per `tls` and reads the first message: everything
/// [`HELLO_TIMEOUT`] bounds. `None` for a failed handshake, a stream that
/// closes or breaks first, a frame past the pre-`Hello` limit, or a first
/// message other than `Hello`.
async fn open_link(stream: TcpStream, tls: &TlsCtx) -> Option<OpenedLink> {
    let stream = match establish_accept(stream, tls).await {
        Ok(stream) => stream,
        Err(error) => {
            tracing::debug!(%error, "TLS handshake failed on accept; dropping connection");
            return None;
        }
    };
    let mut framed = new_framed_pre_hello(stream);
    match recv_msg(&mut framed).await {
        Some(Ok(Msg::Hello {
            node,
            protocol,
            cluster,
            ..
        })) => Some(OpenedLink {
            framed,
            from: node,
            peer_protocol: protocol,
            peer_cluster: cluster,
        }),
        Some(Ok(_)) => {
            tracing::debug!("first message on a data-plane connection is not a hello; dropping it");
            None
        }
        Some(Err(error)) => {
            tracing::debug!(%error, "unreadable first frame on a data-plane connection; dropping it");
            None
        }
        None => None,
    }
}

/// Serves one accepted connection: layers TLS on per `tls`, requires
/// `Hello` first, then dispatches each subsequent message.
/// `Invalidate`/`Replicate`/`ReplicateBatch` keep the connection open and
/// forward to `inbound_tx`. A request message is served and the connection
/// stays open for reuse, subject to [`REQ_CONN_IDLE_TIMEOUT`] and
/// [`REQ_CONN_MAX_REQUESTS`]. A failed serve or TLS handshake closes the
/// connection immediately, since it may be mid-frame and unsafe to reuse.
///
/// A connection that has not sent a complete `Hello` within [`HELLO_TIMEOUT`]
/// of the handler first running, TLS handshake included, is closed and
/// counted in `sundog_accept_dropped_total{reason="hello_timeout"}`. Until
/// the `Hello` decodes a frame is at most `HELLO_MAX_FRAME` bytes (1 KiB);
/// afterwards it is at most [`MAX_FRAME`].
///
/// `slots` is what a connection from an unknown source holds against the
/// caps: a slot among the open connections until the handler returns, and a
/// slot among those waiting for their `Hello` until the `Hello` decodes. A
/// known source holds none. After its `Hello` a connection from an unknown
/// source whose `Hello` names no peer in the peer table, and that sends no
/// frame for [`FIRST_FRAME_TIMEOUT`], is closed and counted in
/// `sundog_accept_dropped_total{reason="first_frame_timeout"}`; every
/// connection that stops inside a frame for [`FRAME_STALL_TIMEOUT`] is closed
/// and counted under `reason="frame_stall"`. A connection from a peer's IP,
/// or one whose `Hello` names a peer, that is between frames waits without
/// bound, as a broadcast link does.
///
/// Every read and write below races against `mesh`'s accept token, tearing
/// a slow snapshot or AE reply down promptly on [`crate::net::Mesh::shutdown`].
/// An `AeDigest` from a peer this node still has replicate frames queued
/// toward is answered empty (`MeshInner::defers_ae_digest_from`): the
/// stream delivers what a listing would, and the peer's next round catches
/// the rest.
pub(super) async fn handle_accepted(
    stream: TcpStream,
    inbound_tx: mpsc::Sender<InboundMsg>,
    handler: Arc<dyn RequestHandler>,
    mesh: Arc<MeshInner>,
    mut slots: Option<Slots>,
) {
    let cancel = mesh.accept_cancel.clone();
    let peer_addr = stream.peer_addr().ok();
    let opened = tokio::select! {
        biased;
        () = cancel.cancelled() => return,
        opened = tokio::time::timeout(HELLO_TIMEOUT, open_link(stream, &mesh.tls)) => opened,
    };
    let OpenedLink {
        mut framed,
        from,
        peer_protocol,
        peer_cluster,
    } = match opened {
        Ok(Some(link)) => link,
        Ok(None) => return,
        Err(_elapsed) => {
            mesh.admission.note_hello_timeout(peer_addr);
            return;
        }
    };
    // No longer waiting for its hello: the cap on those does not count it
    // again. It keeps its slot among the open connections until it returns.
    let unknown_source = slots.is_some();
    if let Some(slots) = slots.as_mut() {
        slots.release_pre_hello();
    }
    // Frames buffered behind the `Hello` decode under the full limit.
    framed.codec_mut().set_max_frame_length(MAX_FRAME);
    // A peer of another cluster dialed an address this node now holds,
    // left over from a node of its own that held it before: nothing it
    // sends belongs in this cluster's caches.
    if !wire::same_cluster(mesh.cluster, peer_cluster) {
        tracing::warn!(
            peer = %from,
            "dropping a data-plane connection from another cluster's node"
        );
        return;
    }
    if peer_protocol > wire::PROTOCOL_VERSION {
        tracing::debug!(
            peer = %from,
            peer_protocol,
            "peer speaks a newer protocol; it limits itself to what this node understands"
        );
    }

    let mut served_requests: usize = 0;
    let mut frames_received: usize = 0;
    loop {
        // See `frame_idle_timeout` for which connections wait without bound.
        let bounded = first_frame_is_bounded(&mesh, from, unknown_source, frames_received);
        let idle_timeout = frame_idle_timeout(served_requests, frames_received, bounded);
        let received = tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            received = recv_msg_bounded(&mut framed, idle_timeout) => received,
        };
        let received = match received {
            Ok(received) => received,
            Err(timeout) => {
                if let Some(reason) = read_timeout_reason(timeout, served_requests) {
                    note_read_timeout(reason, peer_addr);
                }
                return;
            }
        };
        let Some(Ok(msg)) = received else {
            return;
        };
        frames_received += 1;
        let is_request = matches!(
            msg,
            Msg::StRequest { .. }
                | Msg::AeDigest { .. }
                | Msg::AePull { .. }
                | Msg::AeEntries { .. }
                | Msg::AePullHashes { .. }
                | Msg::AeParts { .. }
                | Msg::Fetch { .. }
                | Msg::AeDigestScoped { .. }
                | Msg::AeDigestMasked { .. }
                | Msg::StBuckets { .. }
                | Msg::Load { .. }
        );
        let stop = dispatch_one(
            msg,
            from,
            peer_protocol,
            &mut framed,
            &inbound_tx,
            handler.as_ref(),
            mesh.as_ref(),
            &cancel,
        )
        .await;
        if stop {
            return;
        }
        if is_request {
            served_requests += 1;
        }
        if served_requests >= REQ_CONN_MAX_REQUESTS {
            return;
        }
    }
}

/// Dispatches one message off an accepted connection: forwards a broadcast
/// message to `inbound_tx`, serves a request inline, or, for a message only
/// ever sent as a reply on a connection this node initiated, does nothing.
/// Returns `true` when this connection is done.
#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "one match arm per RPC kind; splitting it up would obscure the dispatch table"
)]
async fn dispatch_one(
    msg: Msg,
    from: NodeId,
    peer_protocol: u16,
    framed: &mut PeerFramed,
    inbound_tx: &mpsc::Sender<InboundMsg>,
    handler: &dyn RequestHandler,
    mesh: &MeshInner,
    cancel: &CancellationToken,
) -> bool {
    match msg {
        Msg::Invalidate { .. }
        | Msg::Replicate { .. }
        | Msg::ReplicateBatch { .. }
        | Msg::ForwardBatch { .. }
        | Msg::RefreshHint { .. } => {
            let _ = inbound_tx.send(InboundMsg { from, msg }).await;
            false
        }
        Msg::Load { cache, keys } => serve_load(framed, cache, keys, handler, cancel).await,
        Msg::StRequest { cache } => {
            serve_state_transfer(framed, cache, handler, cancel, peer_protocol).await
        }
        Msg::AeDigest { cache, buckets } => {
            if mesh.defers_ae_digest_from(from) {
                tracing::debug!(
                    peer = %from,
                    %cache,
                    "anti-entropy digest from a peer with replicate frames queued toward it; answered empty"
                );
                send_batch_or_cancelled(framed, &[Msg::ReqDone], cancel).await
            } else {
                serve_ae_digest(framed, cache, buckets, handler, cancel, peer_protocol).await
            }
        }
        Msg::AeEntries { cache, buckets } => {
            serve_ae_entries(framed, cache, buckets, handler, cancel, peer_protocol).await
        }
        Msg::AePull { cache, keys } => serve_ae_pull(framed, cache, keys, handler, cancel).await,
        Msg::AePullHashes {
            cache,
            bucket,
            hashes,
        } => serve_ae_pull_hashes(framed, cache, bucket, hashes, handler, cancel).await,
        Msg::AeParts { cache, parts } => {
            serve_ae_parts(framed, cache, parts, handler, cancel).await
        }
        Msg::Fetch {
            cache,
            key,
            view_hash,
        } => serve_fetch(framed, cache, key, view_hash, handler, cancel).await,
        Msg::AeDigestScoped {
            cache,
            view_hash,
            buckets,
        } => {
            serve_ae_digest_scoped(
                framed,
                cache,
                view_hash,
                buckets,
                handler,
                cancel,
                peer_protocol,
            )
            .await
        }
        Msg::AeDigestMasked {
            cache,
            view_hash,
            buckets,
        } => serve_ae_digest_masked(framed, cache, view_hash, buckets, handler, cancel).await,
        Msg::StBuckets {
            cache,
            buckets,
            view_hash,
        } => {
            serve_st_buckets(
                framed,
                cache,
                buckets,
                view_hash,
                handler,
                mesh,
                from,
                peer_protocol,
                cancel,
            )
            .await
        }
        // The last bucket's `StBucketDone` is sent after `serve_st_buckets`
        // returns, so its ack lands here instead; recording it catches that
        // case. A mid-stream ack it already recorded just re-inserts.
        Msg::StBucketAck {
            cache,
            bucket,
            view_hash,
        } => {
            mesh.record_bucket_ack(cache, bucket, from, view_hash);
            false
        }
        // A duplicate `Hello`, or `StChunk`/`AeBucket`/`AeSketch`/
        // `AePartDigests`/`AePart`/`AePartSketch`/`StUnavailable`/
        // `FetchReply`/`StBucketChunk`/`StaleView`/`Loaded`/`LoadFailed`/
        // `LoadDeclined`/`ReqDone` sent only as replies on a connection this
        // node initiated, never on one being served here. `StBucketDone` is
        // likewise never dispatched here: only `serve_st_buckets` sends it,
        // to a requester.
        Msg::Hello { .. }
        | Msg::StChunk { .. }
        | Msg::AeBucket { .. }
        | Msg::AeSketch { .. }
        | Msg::AePartDigests { .. }
        | Msg::AePart { .. }
        | Msg::AePartSketch { .. }
        | Msg::StUnavailable { .. }
        | Msg::FetchReply { .. }
        | Msg::FetchDeclined { .. }
        | Msg::StBucketChunk { .. }
        | Msg::StaleView { .. }
        | Msg::StBucketDone { .. }
        | Msg::Loaded { .. }
        | Msg::LoadFailed { .. }
        | Msg::LoadDeclined { .. }
        | Msg::ReqDone => false,
    }
}

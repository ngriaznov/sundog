//! The accept loop and per-connection dispatch.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::admit::{HELLO_TIMEOUT, accept_backoff, accept_error_backs_off};
use super::framing::{
    PeerFramed, disable_nagle, establish_accept, new_framed_pre_hello, recv_msg,
    send_batch_or_cancelled,
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

/// Reads the next message off `framed`, bounded by `idle_timeout` when set.
/// Used only once a connection has served at least one request/response
/// exchange; a persistent broadcast link never goes through this path.
/// Timing out is treated like the peer closing the connection.
async fn recv_msg_or_idle_timeout(
    framed: &mut PeerFramed,
    idle_timeout: Option<Duration>,
) -> Option<Result<Msg, CodecError>> {
    match idle_timeout {
        Some(timeout) => tokio::time::timeout(timeout, recv_msg(framed))
            .await
            .unwrap_or(None),
        None => recv_msg(framed).await,
    }
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
        let stream = match accepted {
            Ok((stream, _peer_addr)) => {
                failures = 0;
                stream
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
        disable_nagle(&stream);
        tokio::spawn(handle_accepted(
            stream,
            inbound_tx.clone(),
            Arc::clone(&handler),
            Arc::clone(&mesh),
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
    loop {
        // Only a connection that has served a request/response exchange is
        // a pooling candidate; a persistent broadcast link waits indefinitely.
        let idle_timeout = (served_requests > 0).then_some(REQ_CONN_IDLE_TIMEOUT);
        let received = tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            received = recv_msg_or_idle_timeout(&mut framed, idle_timeout) => received,
        };
        let Some(Ok(msg)) = received else {
            return;
        };
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

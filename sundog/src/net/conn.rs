//! TCP framing and connection tasks: the accept-side demux (persistent mesh
//! traffic vs. one-shot request/response, distinguished by the first message
//! after `Hello`, plan §6) and the per-peer dial/write loop for the
//! broadcast-class outboxes.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt as _, StreamExt as _};
use smol_str::SmolStr;
use tokio::sync::mpsc;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tokio_util::sync::CancellationToken;

use super::outbox::DropOldestQueue;
use super::tcp::{TcpListener, TcpStream};
use super::{InboundMsg, MeshStream, RequestHandler, TlsCtx};
use crate::error::CodecError;
use crate::node::NodeId;
use crate::wire::{self, MAX_FRAME, Msg};

pub(super) type PeerFramed = Framed<MeshStream, LengthDelimitedCodec>;

pub(super) fn new_framed(stream: MeshStream) -> PeerFramed {
    LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME)
        .new_framed(stream)
}

/// Layers TLS onto a freshly dialed `stream` per `tls` (a no-op, `Ok`-wrapped
/// passthrough whenever TLS isn't compiled in for this transport — see
/// `net`'s [`TlsCtx`]/[`MeshStream`] docs).
#[cfg(all(feature = "tls", not(feature = "sim")))]
async fn establish_dial(stream: TcpStream, tls: &TlsCtx) -> std::io::Result<MeshStream> {
    match tls {
        Some(ctx) => ctx.connect(stream).await,
        None => Ok(MeshStream::Plain(stream)),
    }
}
// A genuine (if instantly-ready) `.await` — not just `Ok(stream)` — keeps
// this signature `async fn` like the TLS-active branch above, so every
// caller can write `establish_dial(..).await` unconditionally rather than
// forking on the feature.
#[cfg(not(all(feature = "tls", not(feature = "sim"))))]
async fn establish_dial(stream: TcpStream, _tls: &TlsCtx) -> std::io::Result<MeshStream> {
    std::future::ready(Ok(stream)).await
}

/// Layers TLS onto a freshly accepted `stream` per `tls` — the accept-side
/// counterpart of [`establish_dial`].
#[cfg(all(feature = "tls", not(feature = "sim")))]
async fn establish_accept(stream: TcpStream, tls: &TlsCtx) -> std::io::Result<MeshStream> {
    match tls {
        Some(ctx) => ctx.accept(stream).await,
        None => Ok(MeshStream::Plain(stream)),
    }
}
#[cfg(not(all(feature = "tls", not(feature = "sim"))))]
async fn establish_accept(stream: TcpStream, _tls: &TlsCtx) -> std::io::Result<MeshStream> {
    std::future::ready(Ok(stream)).await
}

pub(super) async fn send_msg(framed: &mut PeerFramed, msg: &Msg) -> Result<(), CodecError> {
    let bytes = wire::encode(msg)?;
    framed
        .send(Bytes::from(bytes))
        .await
        .map_err(CodecError::Io)
}

async fn recv_msg(framed: &mut PeerFramed) -> Option<Result<Msg, CodecError>> {
    match framed.next().await {
        Some(Ok(bytes)) => Some(wire::decode(&bytes)),
        Some(Err(source)) => Some(Err(CodecError::Io(source))),
        None => None,
    }
}

/// Dials `addr` on a fresh connection, layers TLS on per `tls` if
/// configured, and completes the `Hello` handshake — for the one-shot
/// request/response paths (state transfer, anti-entropy) and, via
/// [`connect_with_hello`], the persistent per-peer writer.
pub(super) async fn dial_with_hello(
    addr: SocketAddr,
    node: NodeId,
    incarnation: u64,
    tls: &TlsCtx,
) -> Result<PeerFramed, CodecError> {
    let stream = TcpStream::connect(addr).await?;
    let stream = establish_dial(stream, tls).await?;
    let mut framed = new_framed(stream);
    send_msg(&mut framed, &Msg::Hello { node, incarnation }).await?;
    Ok(framed)
}

/// The long-lived per-peer writer: connects (retrying with a fixed backoff),
/// sends `Hello`, then drains both broadcast-class outboxes onto the wire
/// until told to stop or the connection breaks, in which case it reconnects.
/// Never reads from the connection — broadcast traffic is one-directional by
/// design (plan: "both sides may hold a connection, that is fine in v1").
pub(super) async fn run_peer_writer(
    local_node: NodeId,
    incarnation: u64,
    addr: SocketAddr,
    invalidate: Arc<DropOldestQueue>,
    mut replicate_rx: mpsc::Receiver<Msg>,
    cancel: CancellationToken,
    tls: TlsCtx,
) {
    'reconnect: loop {
        let mut framed = tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            connected = connect_with_hello(addr, local_node, incarnation, &tls, &cancel) => match connected {
                Some(framed) => framed,
                None => return, // cancelled while retrying
            },
        };

        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                msg = invalidate.pop() => {
                    if send_msg(&mut framed, &msg).await.is_err() {
                        continue 'reconnect;
                    }
                }
                received = replicate_rx.recv() => {
                    let Some(msg) = received else { return }; // sender dropped: peer was removed
                    if send_msg(&mut framed, &msg).await.is_err() {
                        continue 'reconnect;
                    }
                }
            }
        }
    }
}

/// Retries `connect` + `Hello` on [`RECONNECT_BACKOFF`][] until it succeeds
/// or `cancel` fires. Returns `None` only when cancelled mid-retry.
async fn connect_with_hello(
    addr: SocketAddr,
    node: NodeId,
    incarnation: u64,
    tls: &TlsCtx,
    cancel: &CancellationToken,
) -> Option<PeerFramed> {
    const RECONNECT_BACKOFF: Duration = Duration::from_millis(500);
    loop {
        if let Ok(framed) = dial_with_hello(addr, node, incarnation, tls).await {
            return Some(framed);
        }
        tokio::select! {
            biased;
            () = cancel.cancelled() => return None,
            () = tokio::time::sleep(RECONNECT_BACKOFF) => {}
        }
    }
}

/// Accepts connections until `cancel` fires, spawning a handler task per
/// connection.
pub(super) async fn accept_loop(
    listener: TcpListener,
    inbound_tx: mpsc::Sender<InboundMsg>,
    handler: Arc<dyn RequestHandler>,
    cancel: CancellationToken,
    tls: TlsCtx,
) {
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            accepted = listener.accept() => {
                let Ok((stream, _peer_addr)) = accepted else { continue };
                tokio::spawn(handle_accepted(
                    stream,
                    inbound_tx.clone(),
                    Arc::clone(&handler),
                    tls.clone(),
                ));
            }
        }
    }
}

/// Serves one accepted connection: layers TLS on per `tls` if configured,
/// requires `Hello` first, then dispatches each subsequent message.
/// `Invalidate`/`Replicate` keep the connection open and forward to
/// `inbound_tx` (the persistent mesh-link case); a request message
/// (`StRequest`/`AeDigest`/`AePull`) is served once and the connection is
/// then closed, per plan "own streams" for request/response. A failed TLS
/// handshake (a plaintext peer dialing a TLS-configured node, or a
/// certificate that doesn't chain to the trusted root) drops the connection
/// exactly like a missing `Hello` — a loud `tracing` event, no crash.
async fn handle_accepted(
    stream: TcpStream,
    inbound_tx: mpsc::Sender<InboundMsg>,
    handler: Arc<dyn RequestHandler>,
    tls: TlsCtx,
) {
    let stream = match establish_accept(stream, &tls).await {
        Ok(stream) => stream,
        Err(error) => {
            tracing::debug!(%error, "TLS handshake failed on accept; dropping connection");
            return;
        }
    };
    let mut framed = new_framed(stream);
    let Some(Ok(Msg::Hello { node: from, .. })) = recv_msg(&mut framed).await else {
        return;
    };

    loop {
        let Some(Ok(msg)) = recv_msg(&mut framed).await else {
            return;
        };
        match msg {
            Msg::Invalidate { .. } | Msg::Replicate { .. } => {
                let _ = inbound_tx.send(InboundMsg { from, msg }).await;
            }
            Msg::StRequest { cache } => {
                serve_state_transfer(&mut framed, cache, handler.as_ref()).await;
                return;
            }
            Msg::AeDigest { cache, buckets } => {
                serve_ae_digest(&mut framed, cache, buckets, handler.as_ref()).await;
                return;
            }
            Msg::AePull { cache, keys } => {
                serve_ae_pull(&mut framed, cache, keys, handler.as_ref()).await;
                return;
            }
            // A duplicate `Hello`, or `StChunk`/`AeBucket` — the latter only
            // ever sent as replies on a connection *we* initiated as a
            // requester, never to a connection we're serving.
            Msg::Hello { .. } | Msg::StChunk { .. } | Msg::AeBucket { .. } => {}
        }
    }
}

async fn serve_state_transfer(
    framed: &mut PeerFramed,
    cache: SmolStr,
    handler: &dyn RequestHandler,
) {
    let mut chunks = handler.snapshot_chunks(cache.clone());
    while let Some(recs) = chunks.next().await {
        let msg = Msg::StChunk {
            cache: cache.clone(),
            recs,
            done: false,
        };
        if send_msg(framed, &msg).await.is_err() {
            return;
        }
    }
    let _ = send_msg(
        framed,
        &Msg::StChunk {
            cache,
            recs: Vec::new(),
            done: true,
        },
    )
    .await;
}

async fn serve_ae_digest(
    framed: &mut PeerFramed,
    cache: SmolStr,
    remote_buckets: Vec<(u16, u64)>,
    handler: &dyn RequestHandler,
) {
    let local: std::collections::HashMap<u16, u64> =
        handler.digests(cache.clone()).await.into_iter().collect();
    for (bucket, remote_digest) in remote_buckets {
        if local.get(&bucket).copied().unwrap_or(0) == remote_digest {
            continue;
        }
        let entries = handler.bucket_entries(cache.clone(), bucket).await;
        let msg = Msg::AeBucket {
            cache: cache.clone(),
            bucket,
            entries,
        };
        if send_msg(framed, &msg).await.is_err() {
            return;
        }
    }
}

async fn serve_ae_pull(
    framed: &mut PeerFramed,
    cache: SmolStr,
    keys: Vec<Bytes>,
    handler: &dyn RequestHandler,
) {
    for rec in handler.records_for(cache.clone(), keys).await {
        let msg = Msg::Replicate {
            cache: cache.clone(),
            rec,
        };
        if send_msg(framed, &msg).await.is_err() {
            return;
        }
    }
}

/// Reads `AeBucket` replies until the peer closes the connection.
pub(super) async fn collect_ae_buckets(
    mut framed: PeerFramed,
) -> Result<Vec<(u16, Vec<(Bytes, crate::hlc::Hlc)>)>, CodecError> {
    let mut result = Vec::new();
    while let Some(msg) = recv_msg(&mut framed).await {
        if let Msg::AeBucket {
            bucket, entries, ..
        } = msg?
        {
            result.push((bucket, entries));
        }
    }
    Ok(result)
}

/// Reads `Replicate` replies until the peer closes the connection.
pub(super) async fn collect_pulled_records(
    mut framed: PeerFramed,
) -> Result<Vec<crate::wire::WireRecord>, CodecError> {
    let mut result = Vec::new();
    while let Some(msg) = recv_msg(&mut framed).await {
        if let Msg::Replicate { rec, .. } = msg? {
            result.push(rec);
        }
    }
    Ok(result)
}

/// Adapts a request-state connection into a lazy stream of records, reading
/// `StChunk`s off the wire only as the consumer polls for more.
pub(super) fn state_stream(
    framed: PeerFramed,
) -> futures::stream::BoxStream<'static, Result<crate::wire::WireRecord, CodecError>> {
    use std::collections::VecDeque;

    let state = (framed, VecDeque::<crate::wire::WireRecord>::new(), false);
    Box::pin(futures::stream::unfold(
        state,
        |(mut framed, mut buf, mut done)| async move {
            loop {
                if let Some(rec) = buf.pop_front() {
                    return Some((Ok(rec), (framed, buf, done)));
                }
                if done {
                    return None;
                }
                match recv_msg(&mut framed).await {
                    Some(Ok(Msg::StChunk {
                        recs,
                        done: is_done,
                        ..
                    })) => {
                        buf = recs.into();
                        done = is_done;
                    }
                    Some(Ok(_)) => {} // unexpected message on this stream; keep reading
                    Some(Err(err)) => return Some((Err(err), (framed, buf, true))),
                    // The connection closed before a `done: true` chunk ever
                    // arrived — a donor crash/close mid-stream. Surfacing
                    // this as an error (rather than silently ending the
                    // stream, indistinguishable from a clean completion) is
                    // what lets the state-transfer retry logic tell a
                    // truncated transfer from a finished one.
                    None => {
                        let err = CodecError::Io(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "state-transfer connection closed before the final chunk",
                        ));
                        return Some((Err(err), (framed, buf, true)));
                    }
                }
            }
        },
    ))
}

#[cfg(test)]
mod tests {
    use futures::{SinkExt as _, StreamExt as _};
    // Real tokio sockets and a locally-built codec throughout, not this
    // module's own `new_framed`/`send_msg`/`recv_msg` — those are typed
    // against `net::tcp`'s seam alias (`turmoil::net::TcpStream` under the
    // `sim` feature), while this suite specifically exercises the real
    // socket stack regardless of feature flags.
    use tokio::net::{TcpListener, TcpStream};
    use tokio_util::codec::LengthDelimitedCodec;

    use crate::node::NodeId;
    use crate::wire::{self, MAX_FRAME, Msg};

    #[tokio::test]
    async fn codec_roundtrips_a_message_through_a_real_socket_pair() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("listener has a local addr");
        let codec = || {
            LengthDelimitedCodec::builder()
                .max_frame_length(MAX_FRAME)
                .new_codec()
        };

        let accept = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            tokio_util::codec::Framed::new(stream, codec())
        });
        let client = TcpStream::connect(addr).await.expect("connect");
        let mut client = tokio_util::codec::Framed::new(client, codec());
        let mut server = accept.await.expect("accept task");

        let sent = Msg::Hello {
            node: NodeId::from(7),
            incarnation: 3,
        };
        let encoded = wire::encode(&sent).expect("encodes");
        client
            .send(bytes::Bytes::from(encoded))
            .await
            .expect("send");
        let frame = server
            .next()
            .await
            .expect("frame arrives")
            .expect("no io error");
        let got = wire::decode(&frame).expect("decodes");
        assert_eq!(got, sent);
    }
}

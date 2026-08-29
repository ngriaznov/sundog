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
use crate::wire::{self, MAX_FRAME, Msg, WireRecord};

pub(super) type PeerFramed = Framed<MeshStream, LengthDelimitedCodec>;

/// Disables Nagle's algorithm on a freshly dialed/accepted socket. Every
/// wire message here is already a deliberately-sized, application-level
/// batch (`net::conn`'s own coalescing, or a lone small message that should
/// go out now) — nothing is ever gained by the kernel holding it back to
/// wait for more, and Nagle's interaction with the peer's delayed-ACK timer
/// is a classic multi-millisecond stall on exactly this small-frequent-write
/// traffic shape. Best-effort: a failure here (a socket already gone) is
/// harmless and surfaces on the next real read/write instead.
fn disable_nagle(stream: &TcpStream) {
    if let Err(error) = stream.set_nodelay(true) {
        tracing::debug!(%error, "set_nodelay failed; leaving Nagle's algorithm enabled");
    }
}

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
    let len = bytes.len();
    framed
        .send(Bytes::from(bytes))
        .await
        .map_err(CodecError::Io)?;
    super::record_frame_sent(len);
    Ok(())
}

/// Sends every message in `msgs` and flushes once at the end, rather than
/// once per message — the "drain-many-then-flush-once" half of the per-peer
/// writer's opportunistic batching (plan §6): several `Msg::Invalidate`s (or
/// the coalesced [`coalesce_replicate`] output) sharing one flush instead of
/// one syscall-worthy flush each.
async fn send_batch(framed: &mut PeerFramed, msgs: &[Msg]) -> Result<(), CodecError> {
    for msg in msgs {
        let bytes = wire::encode(msg)?;
        let len = bytes.len();
        framed
            .feed(Bytes::from(bytes))
            .await
            .map_err(CodecError::Io)?;
        super::record_frame_sent(len);
    }
    framed.flush().await.map_err(CodecError::Io)?;
    Ok(())
}

/// Byte budget for one coalesced [`Msg::ReplicateBatch`] frame — well under
/// [`MAX_FRAME`], so opportunistic coalescing never risks tripping the wire
/// frame cap even for large-valued caches.
const REPLICATE_BATCH_BUDGET: usize = 256 * 1024;

/// Count cap alongside [`REPLICATE_BATCH_BUDGET`], so a long run of tiny
/// records doesn't grow one batch frame without bound.
const REPLICATE_BATCH_COUNT: usize = 4096;

/// Flushes `pending` (if any) into `out`: a run of exactly one accumulated
/// record becomes a lone `Msg::Replicate`, more than one a
/// `Msg::ReplicateBatch` — the shared close-off step [`coalesce_replicate`]
/// calls both mid-run (a cache change, or a budget/count cap hit) and at the
/// end of `drained`.
fn flush_pending_replicate(
    pending: &mut Option<(SmolStr, Vec<WireRecord>, usize)>,
    out: &mut Vec<Msg>,
) {
    let Some((cache, mut recs, _)) = pending.take() else {
        return;
    };
    out.push(if recs.len() == 1 {
        Msg::Replicate {
            cache,
            rec: recs.pop().expect("invariant: length checked above"),
        }
    } else {
        Msg::ReplicateBatch { cache, recs }
    });
}

/// Opportunistically coalesces consecutive same-cache `Msg::Replicate`
/// entries in `drained` — messages the writer already had queued by the time
/// it drained them, never delayed to wait for more (plan §6's Aeron-style
/// smart batching: no timers, no added latency) — into
/// `Msg::ReplicateBatch` frames bounded by [`REPLICATE_BATCH_BUDGET`] and
/// [`REPLICATE_BATCH_COUNT`]. A run of exactly one message stays a lone
/// `Msg::Replicate`, so a lone queued write under light load carries no
/// wrapping overhead. Anything other than `Msg::Replicate` passes through
/// unchanged (the replicate-class outbox never actually carries anything
/// else, but this stays honest rather than assuming it).
fn coalesce_replicate(drained: Vec<Msg>) -> Vec<Msg> {
    let mut out = Vec::with_capacity(drained.len());
    let mut pending: Option<(SmolStr, Vec<WireRecord>, usize)> = None;

    for msg in drained {
        let Msg::Replicate { cache, rec } = msg else {
            flush_pending_replicate(&mut pending, &mut out);
            out.push(msg);
            continue;
        };
        let rec_size = postcard::to_stdvec(&rec).map_or(0, |bytes| bytes.len());
        let fits_pending = pending.as_ref().is_some_and(|(pending_cache, recs, size)| {
            *pending_cache == cache
                && recs.len() < REPLICATE_BATCH_COUNT
                && size + rec_size <= REPLICATE_BATCH_BUDGET
        });
        if fits_pending {
            let (_, recs, size) = pending
                .as_mut()
                .expect("invariant: fits_pending implies Some");
            recs.push(rec);
            *size += rec_size;
        } else {
            flush_pending_replicate(&mut pending, &mut out);
            pending = Some((cache, vec![rec], rec_size));
        }
    }
    flush_pending_replicate(&mut pending, &mut out);
    out
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
    disable_nagle(&stream);
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
                    let mut drained = vec![msg];
                    while let Some(msg) = invalidate.try_pop() {
                        drained.push(msg);
                    }
                    if send_batch(&mut framed, &drained).await.is_err() {
                        continue 'reconnect;
                    }
                }
                received = replicate_rx.recv() => {
                    let Some(first) = received else { return }; // sender dropped: peer was removed
                    let mut drained = vec![first];
                    while let Ok(msg) = replicate_rx.try_recv() {
                        drained.push(msg);
                    }
                    if send_batch(&mut framed, &coalesce_replicate(drained)).await.is_err() {
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
                disable_nagle(&stream);
                tokio::spawn(handle_accepted(
                    stream,
                    inbound_tx.clone(),
                    Arc::clone(&handler),
                    tls.clone(),
                    cancel.clone(),
                ));
            }
        }
    }
}

/// Serves one accepted connection: layers TLS on per `tls` if configured,
/// requires `Hello` first, then dispatches each subsequent message.
/// `Invalidate`/`Replicate`/`ReplicateBatch` keep the connection open and
/// forward to `inbound_tx` (the persistent mesh-link case); a request message
/// (`StRequest`/`AeDigest`/`AePull`) is served once and the connection is
/// then closed, per plan "own streams" for request/response. A failed TLS
/// handshake (a plaintext peer dialing a TLS-configured node, or a
/// certificate that doesn't chain to the trusted root) drops the connection
/// exactly like a missing `Hello` — a loud `tracing` event, no crash.
///
/// `cancel` is `accept_loop`'s own token: every read and write below races
/// against it, so a slow/never-ending snapshot or AE reply gets torn down
/// promptly on [`super::Mesh::shutdown`] instead of outliving it (the
/// `handler` this task holds keeps the whole shard registry alive until the
/// task actually returns).
async fn handle_accepted(
    stream: TcpStream,
    inbound_tx: mpsc::Sender<InboundMsg>,
    handler: Arc<dyn RequestHandler>,
    tls: TlsCtx,
    cancel: CancellationToken,
) {
    let stream = match establish_accept(stream, &tls).await {
        Ok(stream) => stream,
        Err(error) => {
            tracing::debug!(%error, "TLS handshake failed on accept; dropping connection");
            return;
        }
    };
    let mut framed = new_framed(stream);
    let hello = tokio::select! {
        biased;
        () = cancel.cancelled() => return,
        hello = recv_msg(&mut framed) => hello,
    };
    let Some(Ok(Msg::Hello { node: from, .. })) = hello else {
        return;
    };

    loop {
        let received = tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            received = recv_msg(&mut framed) => received,
        };
        let Some(Ok(msg)) = received else {
            return;
        };
        match msg {
            Msg::Invalidate { .. } | Msg::Replicate { .. } | Msg::ReplicateBatch { .. } => {
                let _ = inbound_tx.send(InboundMsg { from, msg }).await;
            }
            Msg::StRequest { cache } => {
                serve_state_transfer(&mut framed, cache, handler.as_ref(), &cancel).await;
                return;
            }
            Msg::AeDigest { cache, buckets } => {
                serve_ae_digest(&mut framed, cache, buckets, handler.as_ref(), &cancel).await;
                return;
            }
            Msg::AePull { cache, keys } => {
                serve_ae_pull(&mut framed, cache, keys, handler.as_ref(), &cancel).await;
                return;
            }
            // A duplicate `Hello`, or `StChunk`/`AeBucket` — the latter only
            // ever sent as replies on a connection *we* initiated as a
            // requester, never to a connection we're serving.
            Msg::Hello { .. } | Msg::StChunk { .. } | Msg::AeBucket { .. } => {}
        }
    }
}

/// Sends `msg`, racing the write against `cancel`. Returns `true` if the
/// caller should stop serving this connection (cancelled, or the send failed).
async fn send_or_cancelled(framed: &mut PeerFramed, msg: &Msg, cancel: &CancellationToken) -> bool {
    tokio::select! {
        biased;
        () = cancel.cancelled() => true,
        sent = send_msg(framed, msg) => sent.is_err(),
    }
}

async fn serve_state_transfer(
    framed: &mut PeerFramed,
    cache: SmolStr,
    handler: &dyn RequestHandler,
    cancel: &CancellationToken,
) {
    let mut chunks = handler.snapshot_chunks(cache.clone());
    loop {
        let next = tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            next = chunks.next() => next,
        };
        let Some(recs) = next else { break };
        let msg = Msg::StChunk {
            cache: cache.clone(),
            recs,
            done: false,
        };
        if send_or_cancelled(framed, &msg, cancel).await {
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
    cancel: &CancellationToken,
) {
    let local: std::collections::HashMap<u16, u64> =
        handler.digests(cache.clone()).await.into_iter().collect();
    let mismatched: Vec<u16> = remote_buckets
        .into_iter()
        .filter(|&(bucket, remote_digest)| {
            local.get(&bucket).copied().unwrap_or(0) != remote_digest
        })
        .map(|(bucket, _)| bucket)
        .collect();
    if mismatched.is_empty() {
        return;
    }
    // One shard pass for every mismatched bucket at once: a mostly-divergent
    // peer mismatches all 1,024, and per-bucket scans would be quadratic.
    for (bucket, entries) in handler.entries_for_buckets(cache.clone(), mismatched).await {
        let msg = Msg::AeBucket {
            cache: cache.clone(),
            bucket,
            entries,
        };
        if send_or_cancelled(framed, &msg, cancel).await {
            return;
        }
    }
}

async fn serve_ae_pull(
    framed: &mut PeerFramed,
    cache: SmolStr,
    keys: Vec<Bytes>,
    handler: &dyn RequestHandler,
    cancel: &CancellationToken,
) {
    for rec in handler.records_for(cache.clone(), keys).await {
        let msg = Msg::Replicate {
            cache: cache.clone(),
            rec,
        };
        if send_or_cancelled(framed, &msg, cancel).await {
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

/// Adapts a request-state connection into a lazy stream of record chunks,
/// reading `StChunk`s off the wire only as the consumer polls for more, and
/// yielding each `StChunk`'s records as one `Vec` — the donor's own chunk
/// boundaries (~500 records, plan §9) preserved so the caller can apply a
/// whole chunk through `ShardOps::apply_remote_batch` under one lock
/// acquisition instead of one per record.
pub(super) fn state_stream(
    framed: PeerFramed,
) -> futures::stream::BoxStream<'static, Result<Vec<WireRecord>, CodecError>> {
    Box::pin(futures::stream::unfold(Some(framed), |state| async move {
        let mut framed = state?;
        loop {
            match recv_msg(&mut framed).await {
                Some(Ok(Msg::StChunk { recs, done, .. })) => {
                    if recs.is_empty() {
                        // Either the trailing `recs: []` marker chunk
                        // (done) or a no-op chunk (not done): nothing to
                        // yield either way.
                        if done {
                            return None;
                        }
                        continue;
                    }
                    return Some((Ok(recs), (!done).then_some(framed)));
                }
                Some(Ok(_)) => {} // unexpected message on this stream; keep reading
                Some(Err(err)) => return Some((Err(err), None)),
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
                    return Some((Err(err), None));
                }
            }
        }
    }))
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

    // `serve_state_transfer` takes `&mut PeerFramed` (`Framed<MeshStream,
    // _>`), and `MeshStream` is a different concrete type per feature (a
    // `turmoil` socket under `sim`, `tls::MeshStream` wrapping a plain
    // `tokio` socket under `tls`) — this test exercises the real transport
    // directly, so it's skipped under `sim` the same way `net::mod`'s and
    // `state_transfer`'s own real-transport test modules are.
    #[cfg(all(feature = "tls", not(feature = "sim")))]
    fn as_mesh_stream(stream: tokio::net::TcpStream) -> super::MeshStream {
        super::MeshStream::Plain(stream)
    }
    #[cfg(all(not(feature = "tls"), not(feature = "sim")))]
    fn as_mesh_stream(stream: tokio::net::TcpStream) -> super::MeshStream {
        stream
    }

    #[cfg(not(feature = "sim"))]
    #[tokio::test]
    async fn serve_state_transfer_stops_promptly_once_cancelled() {
        use futures::future::BoxFuture;
        use futures::stream::BoxStream;
        use smol_str::SmolStr;
        use tokio_util::sync::CancellationToken;

        use crate::hlc::Hlc;

        struct NeverRespondingHandler;
        impl super::RequestHandler for NeverRespondingHandler {
            fn snapshot_chunks(
                &self,
                _cache: SmolStr,
            ) -> BoxStream<'static, Vec<wire::WireRecord>> {
                Box::pin(futures::stream::pending())
            }
            fn digests(&self, _cache: SmolStr) -> BoxFuture<'_, Vec<(u16, u64)>> {
                Box::pin(async { Vec::new() })
            }
            fn bucket_entries(
                &self,
                _cache: SmolStr,
                _bucket: u16,
            ) -> BoxFuture<'_, Vec<(bytes::Bytes, Hlc)>> {
                Box::pin(async { Vec::new() })
            }
            fn entries_for_buckets(
                &self,
                _cache: SmolStr,
                _buckets: Vec<u16>,
            ) -> BoxFuture<'_, crate::store::BucketEntries> {
                Box::pin(async { Vec::new() })
            }
            fn records_for(
                &self,
                _cache: SmolStr,
                _keys: Vec<bytes::Bytes>,
            ) -> BoxFuture<'_, Vec<wire::WireRecord>> {
                Box::pin(async { Vec::new() })
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("listener has a local addr");
        let accept = tokio::spawn(async move { listener.accept().await.expect("accept").0 });
        let _client = TcpStream::connect(addr).await.expect("connect");
        let server_stream = accept.await.expect("accept task");

        let framed = super::new_framed(as_mesh_stream(server_stream));
        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();

        let served = tokio::spawn(async move {
            let mut framed = framed;
            super::serve_state_transfer(
                &mut framed,
                SmolStr::new("c"),
                &NeverRespondingHandler,
                &cancel_for_task,
            )
            .await;
        });

        cancel.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(2), served)
            .await
            .expect(
                "an accepted-connection handler must observe cancellation instead of blocking \
                 forever on a snapshot stream that never yields — otherwise Mesh::shutdown() \
                 leaves it running with the shard registry Arc still held",
            )
            .expect("task must not panic");
    }
}

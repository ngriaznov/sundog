//! TCP framing and connection tasks: the accept-side demux (persistent mesh
//! traffic vs. pooled request/response, distinguished by the first message
//! after `Hello`) and the per-peer dial/write loop for the broadcast-class
//! outboxes.

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
use super::{InboundMsg, MeshStream, OutFrame, RequestHandler, TlsCtx};
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
    let frame = wire::encode(msg)?;
    let len = frame.len();
    framed.send(frame).await.map_err(CodecError::Io)?;
    super::record_frame_sent(len);
    Ok(())
}

/// Sends every message in `msgs` and flushes once at the end, rather than
/// once per message — the "drain-many-then-flush-once" half of the per-peer
/// writer's opportunistic batching: several messages sharing one flush
/// instead of one syscall-worthy flush each. Used for control-plane
/// replies (`serve_ae_digest`/`serve_ae_pull`) and `Hello`-plus-first-request
/// dials, which build their own `Msg`s in-process rather than reusing an
/// already-encoded [`OutFrame`] — see [`send_frames`] for the broadcast path
/// that does.
async fn send_batch(framed: &mut PeerFramed, msgs: &[Msg]) -> Result<(), CodecError> {
    for msg in msgs {
        let frame = wire::encode(msg)?;
        let len = frame.len();
        framed.feed(frame).await.map_err(CodecError::Io)?;
        super::record_frame_sent(len);
    }
    framed.flush().await.map_err(CodecError::Io)?;
    Ok(())
}

/// [`send_batch`], but for already-encoded frames: the per-peer
/// writer's broadcast-class drain feeds these straight onto the wire with no
/// `wire::encode` call of its own — the frame was built once, upstream, and
/// shared (a `Bytes` clone per peer) across every live peer's outbox.
async fn send_frames(
    framed: &mut PeerFramed,
    encoded: impl IntoIterator<Item = Bytes>,
) -> Result<(), CodecError> {
    for frame in encoded {
        let len = frame.len();
        framed.feed(frame).await.map_err(CodecError::Io)?;
        super::record_frame_sent(len);
    }
    framed.flush().await.map_err(CodecError::Io)?;
    Ok(())
}

/// Byte budget for one coalesced [`Msg::ReplicateBatch`] frame — well under
/// [`MAX_FRAME`], so opportunistic coalescing never risks tripping the wire
/// frame cap even for large-valued caches. Shared with the fan-out layer
/// (`cluster::fan_out_batch`), which pre-batches drained write bursts by the
/// same rules before they ever reach a per-peer outbox.
pub(crate) const REPLICATE_BATCH_BUDGET: usize = 256 * 1024;

/// Count cap alongside [`REPLICATE_BATCH_BUDGET`], so a long run of tiny
/// records doesn't grow one batch frame without bound.
pub(crate) const REPLICATE_BATCH_COUNT: usize = 4096;

/// A run of consecutive same-cache `Msg::Replicate`/`Msg::ReplicateBatch`
/// [`OutFrame`]s being considered for merging into one `Msg::ReplicateBatch`,
/// tracked alongside their cumulative frame byte size for the
/// [`REPLICATE_BATCH_BUDGET`] check and their cumulative *record* count for
/// the [`REPLICATE_BATCH_COUNT`] check (one already-batched item can carry
/// many records).
struct PendingRun {
    cache: SmolStr,
    items: Vec<OutFrame>,
    size: usize,
    records: usize,
}

/// Flushes `pending` (if any) into `out`: a run of exactly one accumulated
/// record reuses that record's own already-encoded frame as-is (no
/// re-encode for the uncoalesced case), a run of more than one is combined
/// into a fresh `Msg::ReplicateBatch` encode — the shared close-off step
/// [`coalesce_replicate`] calls both mid-run (a cache change, or a
/// budget/count cap hit) and at the end of `drained`. A batch that somehow
/// fails to encode (unexpected — every record in it already encoded fine on
/// its own) is dropped with a warning rather than panicking; anti-entropy
/// repairs the gap like any other lost write.
fn flush_pending_replicate(pending: &mut Option<PendingRun>, out: &mut Vec<Bytes>) {
    let Some(run) = pending.take() else {
        return;
    };
    if run.items.len() == 1 {
        out.push(
            run.items
                .into_iter()
                .next()
                .expect("invariant: length checked above")
                .frame,
        );
        return;
    }
    let mut recs = Vec::with_capacity(run.records);
    for item in run.items {
        match item.msg {
            Msg::Replicate { rec, .. } => recs.push(rec),
            Msg::ReplicateBatch { recs: batch, .. } => recs.extend(batch),
            _ => unreachable!(
                "invariant: PendingRun only ever accumulates Replicate/ReplicateBatch items"
            ),
        }
    }
    match wire::encode(&Msg::ReplicateBatch {
        cache: run.cache,
        recs,
    }) {
        Ok(frame) => out.push(frame),
        Err(error) => {
            tracing::warn!(%error, "failed to encode coalesced ReplicateBatch; dropped");
        }
    }
}

/// Opportunistically coalesces consecutive same-cache `Msg::Replicate` *and*
/// `Msg::ReplicateBatch` entries in `drained` — messages the writer already
/// had queued by the time it drained them, never delayed to wait for more
/// (Aeron-style smart batching: no timers, no added latency) — into
/// `Msg::ReplicateBatch` frames bounded by [`REPLICATE_BATCH_BUDGET`] and
/// [`REPLICATE_BATCH_COUNT`] (counting *records*, not queue items — one
/// pre-batched item from `cluster::fan_out_batch` can carry many). A run of
/// exactly one item reuses its already-encoded [`OutFrame::frame`] as-is
/// rather than re-encoding — only an actual merge (more than one item
/// combined) pays for a fresh encode. Anything else passes its frame through
/// unchanged (the replicate-class outbox never actually carries anything
/// else, but this stays honest rather than assuming it).
fn coalesce_replicate(drained: Vec<OutFrame>) -> Vec<Bytes> {
    let mut out = Vec::with_capacity(drained.len());
    let mut pending: Option<PendingRun> = None;

    for item in drained {
        let (cache, item_records) = match &item.msg {
            Msg::Replicate { cache, .. } => (cache, 1),
            Msg::ReplicateBatch { cache, recs } => (cache, recs.len()),
            _ => {
                flush_pending_replicate(&mut pending, &mut out);
                out.push(item.frame);
                continue;
            }
        };
        let rec_size = item.frame.len();
        let fits_pending = pending.as_ref().is_some_and(|run| {
            run.cache == *cache
                && run.records + item_records <= REPLICATE_BATCH_COUNT
                && run.size + rec_size <= REPLICATE_BATCH_BUDGET
        });
        if fits_pending {
            let run = pending
                .as_mut()
                .expect("invariant: fits_pending implies Some");
            run.items.push(item);
            run.size += rec_size;
            run.records += item_records;
        } else {
            flush_pending_replicate(&mut pending, &mut out);
            let cache = cache.clone();
            pending = Some(PendingRun {
                cache,
                items: vec![item],
                size: rec_size,
                records: item_records,
            });
        }
    }
    flush_pending_replicate(&mut pending, &mut out);
    out
}

/// [`send_or_cancelled`], but for a whole in-memory reply (`serve_ae_digest`/
/// `serve_ae_pull`, which already know their whole response before sending
/// anything) rather than one message at a time: feeds every message in
/// `msgs` and flushes once, racing the whole batch against `cancel`. Returns
/// `true` if the caller should stop serving this connection (cancelled, or
/// the send failed).
async fn send_batch_or_cancelled(
    framed: &mut PeerFramed,
    msgs: &[Msg],
    cancel: &CancellationToken,
) -> bool {
    tokio::select! {
        biased;
        () = cancel.cancelled() => true,
        sent = send_batch(framed, msgs) => sent.is_err(),
    }
}

/// Reads the next message off `framed`, bounded by `idle_timeout` when set —
/// used only once a connection has already served at least one
/// request/response exchange: a persistent per-peer
/// broadcast link never goes through this path (`idle_timeout` stays `None`
/// for it, matching the original unbounded wait), but a connection kept
/// alive for client-side request-pool reuse must not sit open on this side
/// forever if the peer never checks it back out. Timing out is treated
/// exactly like the peer closing the connection.
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

async fn recv_msg(framed: &mut PeerFramed) -> Option<Result<Msg, CodecError>> {
    match framed.next().await {
        Some(Ok(bytes)) => Some(wire::decode(&bytes.freeze())),
        Some(Err(source)) => Some(Err(CodecError::Io(source))),
        None => None,
    }
}

/// Max idle pooled request/response connections kept per peer: bounds
/// memory/fd use while still letting `ae_round`/`ae_pull`/`request_state`
/// skip a fresh dial (and, under `tls`, a full mutual-cert handshake) on
/// the common path of "this peer is already known reachable."
const REQ_POOL_MAX_IDLE: usize = 4;

/// Idle bound on an *accepted* request/response connection: torn down
/// after this long without a new request, so a peer that stops checking a
/// pooled connection back in doesn't hold a server-side socket open
/// forever.
const REQ_CONN_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Bounds how many requests one accepted connection serves before this side
/// closes it regardless of activity — the companion cap to
/// [`REQ_CONN_IDLE_TIMEOUT`], guarding against a connection that keeps
/// getting reused forever without ever going idle.
const REQ_CONN_MAX_REQUESTS: usize = 4096;

/// A small per-peer pool of request/response connections that have already
/// completed `Hello` (and, under `tls`, a full mutual-cert handshake),
/// reused across `Mesh::ae_round`/`ae_pull`/`request_state` calls instead of
/// dialing fresh every time. Deliberately separate from the persistent
/// per-peer writer connection (`run_peer_writer`), so a slow snapshot
/// transfer can never back up live broadcast traffic.
///
/// A connection only ever gets checked back in after a clean end-of-reply
/// (`Msg::ReqDone`, or a state-transfer stream that reached its final
/// `done: true` chunk) — one left in an unknown framing state after an
/// error, a timeout, or cancellation is dropped instead of pooled, so a
/// broken or partially-consumed connection is never handed out again.
pub(super) struct ReqPool {
    idle: std::sync::Mutex<Vec<PeerFramed>>,
}

impl ReqPool {
    pub(super) fn new() -> Self {
        Self {
            idle: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Takes one idle connection out of the pool, if any is available.
    pub(super) fn checkout(&self) -> Option<PeerFramed> {
        self.idle
            .lock()
            .expect("invariant: req pool lock is never poisoned")
            .pop()
    }

    /// Returns a connection known to be in a clean, ready-for-reuse framing
    /// state. Silently drops it (closing the socket) once the pool is
    /// already at [`REQ_POOL_MAX_IDLE`].
    pub(super) fn checkin(&self, framed: PeerFramed) {
        let mut idle = self
            .idle
            .lock()
            .expect("invariant: req pool lock is never poisoned");
        if idle.len() < REQ_POOL_MAX_IDLE {
            idle.push(framed);
        }
    }
}

/// Dials `addr` on a fresh connection, layers TLS on per `tls` if
/// configured, and completes the `Hello` handshake — for
/// [`connect_with_hello`]'s persistent per-peer writer, which has nothing
/// further to send right away. The one-shot request/response paths (state
/// transfer, anti-entropy) want [`dial_with_hello_and`] instead, so their
/// first request message shares `Hello`'s flush rather than paying for a
/// second one.
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

/// [`dial_with_hello`], but `feed()`s `first` onto the fresh connection
/// alongside `Hello` and flushes once for both — one syscall instead of two,
/// for the one-shot request/response callers (`Mesh::request_state`,
/// `Mesh::ae_round`, `Mesh::ae_pull`) that always have a request message
/// ready to send immediately after `Hello`, with no read in between and
/// Nagle already disabled.
pub(super) async fn dial_with_hello_and(
    addr: SocketAddr,
    node: NodeId,
    incarnation: u64,
    tls: &TlsCtx,
    first: Msg,
) -> Result<PeerFramed, CodecError> {
    let stream = TcpStream::connect(addr).await?;
    disable_nagle(&stream);
    let stream = establish_dial(stream, tls).await?;
    let mut framed = new_framed(stream);
    send_batch(&mut framed, &[Msg::Hello { node, incarnation }, first]).await?;
    Ok(framed)
}

/// The long-lived per-peer writer: connects (retrying with a fixed backoff),
/// sends `Hello`, then drains both broadcast-class outboxes onto the wire
/// until told to stop or the connection breaks, in which case it reconnects.
/// Never reads from the connection — broadcast traffic is one-directional by
/// design; each side dials the other independently, so two connections
/// between the same pair of peers, one per direction, is normal.
pub(super) async fn run_peer_writer(
    local_node: NodeId,
    incarnation: u64,
    addr: SocketAddr,
    invalidate: Arc<DropOldestQueue<OutFrame>>,
    mut replicate_rx: mpsc::Receiver<OutFrame>,
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
                item = invalidate.pop() => {
                    let mut drained = vec![item];
                    while let Some(item) = invalidate.try_pop() {
                        drained.push(item);
                    }
                    let encoded = drained.into_iter().map(|item| item.frame);
                    if send_frames(&mut framed, encoded).await.is_err() {
                        continue 'reconnect;
                    }
                }
                received = replicate_rx.recv() => {
                    let Some(first) = received else { return }; // sender dropped: peer was removed
                    let mut drained = vec![first];
                    while let Ok(item) = replicate_rx.try_recv() {
                        drained.push(item);
                    }
                    if send_frames(&mut framed, coalesce_replicate(drained)).await.is_err() {
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
/// forward to `inbound_tx` (the persistent mesh-link case). A request
/// message (`StRequest`/`AeDigest`/`AePull`) is served, and the connection
/// is then kept open for the requester to reuse rather than
/// closed after one exchange, subject to [`REQ_CONN_IDLE_TIMEOUT`] and
/// [`REQ_CONN_MAX_REQUESTS`] so a peer that never checks a pooled connection
/// back in doesn't hold a server-side socket (and, under `tls`, an open
/// session) forever; a failed serve (an error or this task's own
/// cancellation mid-reply, `serve_*`'s `bool` return) still closes the
/// connection immediately, since it may be mid-frame and unsafe to reuse. A
/// failed TLS handshake (a plaintext peer dialing a TLS-configured node, or
/// a certificate that doesn't chain to the trusted root) drops the
/// connection exactly like a missing `Hello` — a loud `tracing` event, no
/// crash.
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

    let mut served_requests: usize = 0;
    loop {
        // Only a connection that has already served at least one
        // request/response exchange is a pooling candidate; a persistent
        // broadcast link (which never sends a request message) keeps
        // waiting indefinitely, exactly as before this wave.
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
        );
        let stop = match msg {
            Msg::Invalidate { .. } | Msg::Replicate { .. } | Msg::ReplicateBatch { .. } => {
                let _ = inbound_tx.send(InboundMsg { from, msg }).await;
                false
            }
            Msg::StRequest { cache } => {
                serve_state_transfer(&mut framed, cache, handler.as_ref(), &cancel).await
            }
            Msg::AeDigest { cache, buckets } => {
                serve_ae_digest(&mut framed, cache, buckets, handler.as_ref(), &cancel).await
            }
            Msg::AeEntries { cache, buckets } => {
                serve_ae_entries(&mut framed, cache, buckets, handler.as_ref(), &cancel).await
            }
            Msg::AePull { cache, keys } => {
                serve_ae_pull(&mut framed, cache, keys, handler.as_ref(), &cancel).await
            }
            Msg::AePullHashes {
                cache,
                bucket,
                hashes,
            } => {
                serve_ae_pull_hashes(
                    &mut framed,
                    cache,
                    bucket,
                    hashes,
                    handler.as_ref(),
                    &cancel,
                )
                .await
            }
            // A duplicate `Hello`, or `StChunk`/`AeBucket`/`AeSketch`/
            // `ReqDone` — the latter only ever sent as replies on a
            // connection *we* initiated as a requester, never to a
            // connection we're serving.
            Msg::Hello { .. }
            | Msg::StChunk { .. }
            | Msg::AeBucket { .. }
            | Msg::AeSketch { .. }
            | Msg::ReqDone => false,
        };
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

/// Sends `msg`, racing the write against `cancel`. Returns `true` if the
/// caller should stop serving this connection (cancelled, or the send failed).
async fn send_or_cancelled(framed: &mut PeerFramed, msg: &Msg, cancel: &CancellationToken) -> bool {
    tokio::select! {
        biased;
        () = cancel.cancelled() => true,
        sent = send_msg(framed, msg) => sent.is_err(),
    }
}

/// Serves one state-transfer request. Returns `true` if the caller should
/// stop serving this connection (cancelled, or a send failed mid-stream —
/// the connection may be mid-frame and is never safe to keep open in that
/// case), `false` once the final `done: true` chunk sent cleanly and the
/// connection remains reusable for the next request.
async fn serve_state_transfer(
    framed: &mut PeerFramed,
    cache: SmolStr,
    handler: &dyn RequestHandler,
    cancel: &CancellationToken,
) -> bool {
    let mut chunks = handler.snapshot_chunks(cache.clone());
    loop {
        let next = tokio::select! {
            biased;
            () = cancel.cancelled() => return true,
            next = chunks.next() => next,
        };
        let Some(recs) = next else { break };
        let msg = Msg::StChunk {
            cache: cache.clone(),
            recs,
            done: false,
        };
        if send_or_cancelled(framed, &msg, cancel).await {
            return true;
        }
    }
    send_or_cancelled(
        framed,
        &Msg::StChunk {
            cache,
            recs: Vec::new(),
            done: true,
        },
        cancel,
    )
    .await
}

/// Serves one anti-entropy digest exchange, feeding every mismatched
/// bucket's reply plus a trailing [`Msg::ReqDone`] and flushing once,
/// rather than one flush per bucket. A mismatched bucket whose local entry
/// count exceeds `handler.ae_sketch_min_bucket()` replies with an IBLT
/// sketch ([`Msg::AeSketch`], built with `handler.ae_sketch_cells()` cells)
/// instead of its full listing (`Msg::AeBucket`) — `cluster::sketch`'s
/// module docs for why past that size a fixed-cost sketch reply is cheaper
/// on the wire than the listing regardless of the actual diff. Returns
/// `true` if the caller should stop serving this connection (cancelled, or
/// the send failed), `false` once the reply (including `ReqDone`) sent
/// cleanly and the connection remains reusable.
async fn serve_ae_digest(
    framed: &mut PeerFramed,
    cache: SmolStr,
    remote_buckets: Vec<(u16, u64)>,
    handler: &dyn RequestHandler,
    cancel: &CancellationToken,
) -> bool {
    let local: std::collections::HashMap<u16, u64> =
        handler.digests(cache.clone()).await.into_iter().collect();
    let mismatched: Vec<u16> = remote_buckets
        .into_iter()
        .filter(|&(bucket, remote_digest)| {
            local.get(&bucket).copied().unwrap_or(0) != remote_digest
        })
        .map(|(bucket, _)| bucket)
        .collect();
    // One shard pass for every mismatched bucket at once: a mostly-divergent
    // peer mismatches all 1,024, and per-bucket scans would be quadratic.
    let mut replies: Vec<Msg> = if mismatched.is_empty() {
        Vec::new()
    } else {
        let min_bucket = handler.ae_sketch_min_bucket();
        let sketch_cells = handler.ae_sketch_cells();
        handler
            .entries_for_buckets(cache.clone(), mismatched)
            .await
            .into_iter()
            .map(|(bucket, entries)| {
                if entries.len() > min_bucket {
                    let mut iblt = crate::cluster::sketch::Iblt::new(sketch_cells);
                    for (key, ver) in &entries {
                        iblt.insert(xxhash_rust::xxh3::xxh3_64(key), *ver);
                    }
                    Msg::AeSketch {
                        cache: cache.clone(),
                        bucket,
                        cells: iblt.into_cells(),
                    }
                } else {
                    Msg::AeBucket {
                        cache: cache.clone(),
                        bucket,
                        entries,
                    }
                }
            })
            .collect()
    };
    replies.push(Msg::ReqDone);
    send_batch_or_cancelled(framed, &replies, cancel).await
}

/// Serves an `AeEntries` request: the sketch fallback, full listings for the
/// named `buckets` — exactly [`serve_ae_digest`]'s own `AeBucket` reply
/// shape, requested explicitly rather than chosen by the responder.
async fn serve_ae_entries(
    framed: &mut PeerFramed,
    cache: SmolStr,
    buckets: Vec<u16>,
    handler: &dyn RequestHandler,
    cancel: &CancellationToken,
) -> bool {
    let mut replies: Vec<Msg> = if buckets.is_empty() {
        Vec::new()
    } else {
        handler
            .entries_for_buckets(cache.clone(), buckets)
            .await
            .into_iter()
            .map(|(bucket, entries)| Msg::AeBucket {
                cache: cache.clone(),
                bucket,
                entries,
            })
            .collect()
    };
    replies.push(Msg::ReqDone);
    send_batch_or_cancelled(framed, &replies, cancel).await
}

async fn serve_ae_pull(
    framed: &mut PeerFramed,
    cache: SmolStr,
    keys: Vec<Bytes>,
    handler: &dyn RequestHandler,
    cancel: &CancellationToken,
) -> bool {
    let records = handler.records_for(cache.clone(), keys).await;
    let mut replies = super::batch_replicate(&cache, records);
    replies.push(Msg::ReqDone);
    send_batch_or_cancelled(framed, &replies, cancel).await
}

/// Serves an `AePullHashes` request: full records for the entries of
/// `bucket` whose key hash is in `hashes` — the sketch-decoded counterpart
/// to [`serve_ae_pull`], answered with the same batched-records-then-`ReqDone`
/// reply shape.
async fn serve_ae_pull_hashes(
    framed: &mut PeerFramed,
    cache: SmolStr,
    bucket: u16,
    hashes: Vec<u64>,
    handler: &dyn RequestHandler,
    cancel: &CancellationToken,
) -> bool {
    let records = handler
        .records_for_hashes(cache.clone(), bucket, hashes)
        .await;
    let mut replies = super::batch_replicate(&cache, records);
    replies.push(Msg::ReqDone);
    send_batch_or_cancelled(framed, &replies, cancel).await
}

/// Reads `AeBucket` replies until [`Msg::ReqDone`] marks the reply complete
/// — not until the peer closes the connection, which it no longer does
/// after one exchange. On success, checks `framed` back into `pool` for
/// reuse; on error, the connection is dropped instead (never safe to reuse
/// mid-frame).
pub(super) async fn collect_ae_buckets(
    mut framed: PeerFramed,
    pool: &ReqPool,
) -> Result<Vec<(u16, Vec<(Bytes, crate::hlc::Hlc)>)>, CodecError> {
    let mut result = Vec::new();
    loop {
        match recv_msg(&mut framed).await {
            Some(Ok(Msg::ReqDone)) => {
                pool.checkin(framed);
                return Ok(result);
            }
            Some(Ok(Msg::AeBucket {
                bucket, entries, ..
            })) => result.push((bucket, entries)),
            Some(Ok(_)) => {} // unexpected message on this connection; keep reading
            Some(Err(err)) => return Err(err),
            None => return Err(unexpected_close("anti-entropy digest reply")),
        }
    }
}

/// Reads `AeBucket`/`AeSketch` replies until [`Msg::ReqDone`] marks the
/// reply complete — [`Mesh::ae_round`]'s collector, one [`super::AeMismatch`]
/// per mismatched bucket regardless of which shape the responder answered
/// with. See [`collect_ae_buckets`]'s docs for the same "no longer relies on
/// connection close" reasoning and pool-checkin-on-success rule.
///
/// [`Mesh::ae_round`]: super::Mesh::ae_round
pub(super) async fn collect_ae_mismatches(
    mut framed: PeerFramed,
    pool: &ReqPool,
) -> Result<Vec<super::AeMismatch>, CodecError> {
    let mut result = Vec::new();
    loop {
        match recv_msg(&mut framed).await {
            Some(Ok(Msg::ReqDone)) => {
                pool.checkin(framed);
                return Ok(result);
            }
            Some(Ok(Msg::AeBucket {
                bucket, entries, ..
            })) => result.push(super::AeMismatch::Bucket(bucket, entries)),
            Some(Ok(Msg::AeSketch { bucket, cells, .. })) => {
                result.push(super::AeMismatch::Sketch(bucket, cells));
            }
            Some(Ok(_)) => {} // unexpected message on this connection; keep reading
            Some(Err(err)) => return Err(err),
            None => return Err(unexpected_close("anti-entropy digest reply")),
        }
    }
}

/// Reads `Replicate`/`ReplicateBatch` replies until [`Msg::ReqDone`] marks the reply
/// complete — see [`collect_ae_buckets`]'s docs for the same "no longer
/// relies on connection close" reasoning and pool-checkin-on-success rule.
pub(super) async fn collect_pulled_records(
    mut framed: PeerFramed,
    pool: &ReqPool,
) -> Result<Vec<crate::wire::WireRecord>, CodecError> {
    let mut result = Vec::new();
    loop {
        match recv_msg(&mut framed).await {
            Some(Ok(Msg::ReqDone)) => {
                pool.checkin(framed);
                return Ok(result);
            }
            Some(Ok(Msg::Replicate { rec, .. })) => result.push(rec),
            Some(Ok(Msg::ReplicateBatch { recs, .. })) => result.extend(recs),
            Some(Ok(_)) => {} // unexpected message on this connection; keep reading
            Some(Err(err)) => return Err(err),
            None => return Err(unexpected_close("anti-entropy pull reply")),
        }
    }
}

fn unexpected_close(what: &str) -> CodecError {
    CodecError::Io(std::io::Error::new(
        std::io::ErrorKind::UnexpectedEof,
        format!("{what} connection closed before its terminating ReqDone"),
    ))
}

/// Adapts a request-state connection into a lazy stream of record chunks,
/// reading `StChunk`s off the wire only as the consumer polls for more, and
/// yielding each `StChunk`'s records as one `Vec` — the donor's own chunk
/// boundaries (~500 records) preserved so the caller can apply a
/// whole chunk through `ShardOps::apply_remote_batch` under one lock
/// acquisition instead of one per record. On reaching the final
/// `done: true` chunk, checks the connection back into `pool` for reuse
/// instead of dropping it; any error path (including a truncated,
/// donor-crash-mid-stream close) drops it instead.
pub(super) fn state_stream(
    framed: PeerFramed,
    pool: Arc<ReqPool>,
) -> futures::stream::BoxStream<'static, Result<Vec<WireRecord>, CodecError>> {
    Box::pin(futures::stream::unfold(Some(framed), move |state| {
        let pool = Arc::clone(&pool);
        async move {
            let mut framed = state?;
            loop {
                match recv_msg(&mut framed).await {
                    Some(Ok(Msg::StChunk { recs, done, .. })) => {
                        if recs.is_empty() {
                            // Either the trailing `recs: []` marker chunk
                            // (done) or a no-op chunk (not done): nothing to
                            // yield either way.
                            if done {
                                pool.checkin(framed);
                                return None;
                            }
                            continue;
                        }
                        if done {
                            pool.checkin(framed);
                            return Some((Ok(recs), None));
                        }
                        return Some((Ok(recs), Some(framed)));
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
        }
    }))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::{SinkExt as _, StreamExt as _};
    use smol_str::SmolStr;
    // Real tokio sockets and a locally-built codec throughout, not this
    // module's own `new_framed`/`send_msg`/`recv_msg` — those are typed
    // against `net::tcp`'s seam alias (`turmoil::net::TcpStream` under the
    // `sim` feature), while this suite specifically exercises the real
    // socket stack regardless of feature flags.
    use tokio::net::{TcpListener, TcpStream};
    use tokio_util::codec::LengthDelimitedCodec;

    use super::{OutFrame, coalesce_replicate};
    use crate::hlc::Hlc;
    use crate::node::NodeId;
    use crate::wire::{self, MAX_FRAME, Msg, WireRecord};

    fn out_frame(msg: Msg) -> OutFrame {
        OutFrame::new(msg).expect("test message encodes")
    }

    fn record(n: u8) -> WireRecord {
        WireRecord {
            key: Bytes::from(vec![n]),
            value: Some(Bytes::from(vec![n, n])),
            ver: Hlc {
                wall_ms: u64::from(n),
                logical: 0,
                node: NodeId::from(1),
            },
            expires_at_ms: None,
        }
    }

    /// A lone queued `Msg::Replicate` (nothing else in the drain to merge
    /// with) must come out of `coalesce_replicate` as exactly the frame it
    /// already carried in: no re-encode for the uncoalesced case.
    #[test]
    fn coalesce_replicate_reuses_the_lone_frame_verbatim() {
        let item = out_frame(Msg::Replicate {
            cache: SmolStr::new("users"),
            rec: record(1),
        });
        let original_frame = item.frame.clone();
        let out = coalesce_replicate(vec![item]);
        assert_eq!(out, vec![original_frame]);
    }

    /// Consecutive same-cache `Msg::Replicate` entries must merge into one
    /// `Msg::ReplicateBatch` frame that decodes back to all of them, in order.
    #[test]
    fn coalesce_replicate_merges_consecutive_same_cache_entries() {
        let drained = vec![
            out_frame(Msg::Replicate {
                cache: SmolStr::new("users"),
                rec: record(1),
            }),
            out_frame(Msg::Replicate {
                cache: SmolStr::new("users"),
                rec: record(2),
            }),
        ];
        let out = coalesce_replicate(drained);
        assert_eq!(
            out.len(),
            1,
            "two same-cache entries must merge into one frame"
        );
        let decoded = wire::decode(&out[0]).expect("decodes");
        assert_eq!(
            decoded,
            Msg::ReplicateBatch {
                cache: SmolStr::new("users"),
                recs: vec![record(1), record(2)],
            }
        );
    }

    /// A cache-name change mid-run must close off the pending merge rather
    /// than silently mixing records from two caches into one
    /// `Msg::ReplicateBatch`.
    #[test]
    fn coalesce_replicate_does_not_merge_across_a_cache_change() {
        let drained = vec![
            out_frame(Msg::Replicate {
                cache: SmolStr::new("a"),
                rec: record(1),
            }),
            out_frame(Msg::Replicate {
                cache: SmolStr::new("b"),
                rec: record(2),
            }),
        ];
        let out = coalesce_replicate(drained);
        assert_eq!(out.len(), 2, "a cache change must end the pending run");
        assert_eq!(
            wire::decode(&out[0]).expect("decodes"),
            Msg::Replicate {
                cache: SmolStr::new("a"),
                rec: record(1),
            }
        );
        assert_eq!(
            wire::decode(&out[1]).expect("decodes"),
            Msg::Replicate {
                cache: SmolStr::new("b"),
                rec: record(2),
            }
        );
    }

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
        client.send(encoded).await.expect("send");
        let frame = server
            .next()
            .await
            .expect("frame arrives")
            .expect("no io error");
        let got = wire::decode(&frame.freeze()).expect("decodes");
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

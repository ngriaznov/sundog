//! Framing and connection tasks: the accept-side demux between persistent
//! mesh traffic and pooled request/response, and the per-peer dial-and-write
//! loop over the broadcast outboxes.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{FutureExt as _, SinkExt as _, StreamExt as _};
use smol_str::SmolStr;
use tokio::sync::mpsc;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tokio_util::sync::CancellationToken;

use super::outbox::DropOldestQueue;
use super::tcp::{TcpListener, TcpStream};
use super::{InboundMsg, MeshInner, MeshStream, OutFrame, RequestHandler, TlsCtx};
use crate::error::CodecError;
use crate::node::NodeId;
use crate::wire::{self, MAX_FRAME, Msg, WireRecord};

pub(super) type PeerFramed = Framed<MeshStream, LengthDelimitedCodec>;

/// Disables Nagle's algorithm on a freshly dialed/accepted socket. Every
/// wire message here is already a deliberately-sized batch, and Nagle's
/// interaction with delayed ACKs is a classic multi-millisecond stall.
/// Best-effort: a failure here surfaces on the next real read/write instead.
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

/// Layers TLS onto a freshly dialed `stream` per `tls`, a no-op passthrough
/// whenever TLS isn't compiled in for this transport.
#[cfg(all(feature = "tls", not(feature = "sim")))]
async fn establish_dial(stream: TcpStream, tls: &TlsCtx) -> std::io::Result<MeshStream> {
    match tls {
        Some(ctx) => ctx.connect(stream).await,
        None => Ok(MeshStream::Plain(stream)),
    }
}
// A genuine `.await`, not `Ok(stream)` alone, keeps this `async fn` like
// the TLS-active branch above, so every caller writes `.await` unconditionally.
#[cfg(not(all(feature = "tls", not(feature = "sim"))))]
async fn establish_dial(stream: TcpStream, _tls: &TlsCtx) -> std::io::Result<MeshStream> {
    std::future::ready(Ok(stream)).await
}

/// Layers TLS onto a freshly accepted `stream`, the accept-side counterpart of
/// [`establish_dial`].
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

/// Sends every message in `msgs` and flushes once at the end. Used for
/// control-plane replies and `Hello`-plus-first-request dials, which build
/// their own `Msg`s rather than reusing an [`OutFrame`] like [`send_frames`].
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

/// [`send_batch`], but for already-encoded frames: the per-peer writer's
/// broadcast-class drain feeds these straight onto the wire. The frame was
/// built once, upstream, and shared as a `Bytes` clone per peer.
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

/// Byte budget for one coalesced [`Msg::ReplicateBatch`] frame, well under
/// [`MAX_FRAME`]. Shared with the fan-out layer (`cluster::fan_out_batch`),
/// which pre-batches drained write bursts by the same rules.
pub(crate) const REPLICATE_BATCH_BUDGET: usize = 256 * 1024;

/// Count cap alongside [`REPLICATE_BATCH_BUDGET`], so a long run of tiny
/// records doesn't grow one batch frame without bound.
pub(crate) const REPLICATE_BATCH_COUNT: usize = 4096;

/// A run of consecutive same-cache `Msg::Replicate`/`Msg::ReplicateBatch`
/// [`OutFrame`]s considered for merging, tracked with cumulative frame byte
/// size for [`REPLICATE_BATCH_BUDGET`] and record count for
/// [`REPLICATE_BATCH_COUNT`].
struct PendingRun {
    cache: SmolStr,
    items: Vec<OutFrame>,
    size: usize,
    records: usize,
}

/// Flushes `pending`, if any, into `out`. The close-off step
/// [`coalesce_replicate`] calls both mid-run and at the end of `drained`. A
/// batch that fails to encode is dropped with a warning rather than
/// panicking; anti-entropy repairs the gap like any other lost write.
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

/// Coalesces consecutive same-cache `Msg::Replicate`/`Msg::ReplicateBatch`
/// entries in `drained` into `Msg::ReplicateBatch` frames bounded by
/// [`REPLICATE_BATCH_BUDGET`] and [`REPLICATE_BATCH_COUNT`], counting
/// records rather than queue items. A run of exactly one item reuses its
/// already-encoded [`OutFrame::frame`] as-is; only a real merge re-encodes.
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

/// [`send_or_cancelled`], but for a whole in-memory reply rather than one
/// message at a time: feeds every message in `msgs` and flushes once,
/// racing the whole batch against `cancel`. Returns `true` when this
/// connection is done.
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

pub(super) async fn recv_msg(framed: &mut PeerFramed) -> Option<Result<Msg, CodecError>> {
    match framed.next().await {
        Some(Ok(bytes)) => Some(wire::decode(&bytes.freeze())),
        Some(Err(source)) => Some(Err(CodecError::Io(source))),
        None => None,
    }
}

/// Max idle pooled request/response connections kept per peer: bounds
/// memory/fd use while letting requests skip a fresh dial on a known peer.
const REQ_POOL_MAX_IDLE_CONNS: usize = 4;

/// Idle bound on an accepted request/response connection: torn down after
/// this long without a new request, so a peer that stops checking a pooled
/// connection back in doesn't hold a server-side socket open forever.
const REQ_CONN_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Age bound for a pooled connection at [`ReqPool::checkout`], half of
/// [`REQ_CONN_IDLE_TIMEOUT`]. The server may already have closed anything
/// older than this, so a connection past the bound is dropped instead of
/// handed back: reusing it would still write successfully into the
/// half-closed socket, only to see the first read come back `EOF`.
const REQ_POOL_MAX_IDLE: Duration = Duration::from_secs(30);

/// True when a connection checked into the pool at `checked_in` is still
/// within [`REQ_POOL_MAX_IDLE`] of `now`.
fn pooled_is_fresh(checked_in: Instant, now: Instant) -> bool {
    now.duration_since(checked_in) < REQ_POOL_MAX_IDLE
}

/// Bounds how many requests one accepted connection serves before this
/// side closes it, guarding against one reused forever without going idle.
const REQ_CONN_MAX_REQUESTS: usize = 4096;

/// A small per-peer pool of request/response connections that have already
/// completed `Hello`, reused instead of dialing fresh every time. Separate
/// from the persistent per-peer writer connection, so a slow snapshot
/// transfer never backs up live broadcast traffic.
///
/// A connection is checked back in only after a clean end-of-reply; one
/// left in an unknown framing state is dropped instead of pooled.
pub(super) struct ReqPool {
    idle: std::sync::Mutex<Vec<(PeerFramed, Instant)>>,
}

impl ReqPool {
    pub(super) fn new() -> Self {
        Self {
            idle: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Takes one idle connection out of the pool, if any, skipping and
    /// dropping every one checked in more than [`REQ_POOL_MAX_IDLE`] ago:
    /// the server may already have torn it down as idle.
    pub(super) fn checkout(&self) -> Option<PeerFramed> {
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
    pub(super) fn checkin(&self, framed: PeerFramed) {
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
pub(super) enum ReusedProbe {
    /// Nothing to read yet: the connection is alive and the reply is still
    /// in flight, unread.
    Pending,
    /// A receive-side `EOF` or a broken read: the server already closed
    /// this connection as idle, or it is otherwise unusable.
    Stale,
    /// A reply frame was already there. Handed back so the caller doesn't
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
/// [acq]: super::Mesh::acquire_conn
pub(super) fn probe_reused(framed: &mut PeerFramed) -> ReusedProbe {
    match recv_msg(framed).now_or_never() {
        None => ReusedProbe::Pending,
        Some(None | Some(Err(_))) => ReusedProbe::Stale,
        Some(Some(Ok(msg))) => ReusedProbe::Ready(msg),
    }
}

/// Dials `addr` on a fresh connection, layers TLS on per `tls`, and
/// completes the `Hello` handshake, for [`connect_with_hello`]'s persistent
/// per-peer writer. One-shot request/response paths want
/// [`dial_with_hello_and`] instead.
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
    send_msg(
        &mut framed,
        &Msg::Hello {
            node,
            incarnation,
            protocol: wire::PROTOCOL_VERSION,
        },
    )
    .await?;
    Ok(framed)
}

/// [`dial_with_hello`], but `feed()`s `first` onto the fresh connection
/// alongside `Hello` and flushes once for both, one syscall instead of two.
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
    send_batch(
        &mut framed,
        &[
            Msg::Hello {
                node,
                incarnation,
                protocol: wire::PROTOCOL_VERSION,
            },
            first,
        ],
    )
    .await?;
    Ok(framed)
}

/// The long-lived per-peer writer: connects with a fixed backoff, sends
/// `Hello`, then drains both broadcast-class outboxes until told to stop
/// or the connection breaks, in which case it reconnects. Never reads from
/// the connection; two connections per pair of peers is normal.
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

/// Accepts connections until `mesh`'s accept token fires, spawning a handler
/// per connection.
pub(super) async fn accept_loop(
    listener: TcpListener,
    inbound_tx: mpsc::Sender<InboundMsg>,
    handler: Arc<dyn RequestHandler>,
    mesh: Arc<MeshInner>,
) {
    let cancel = mesh.accept_cancel.clone();
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
                    Arc::clone(&mesh),
                ));
            }
        }
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
/// Every read and write below races against `mesh`'s accept token, tearing
/// a slow snapshot or AE reply down promptly on [`super::Mesh::shutdown`].
/// An `AeDigest` from a peer this node still has replicate frames queued
/// toward is answered empty (`MeshInner::defers_ae_digest_from`): the
/// stream delivers what a listing would, and the peer's next round catches
/// the rest.
async fn handle_accepted(
    stream: TcpStream,
    inbound_tx: mpsc::Sender<InboundMsg>,
    handler: Arc<dyn RequestHandler>,
    mesh: Arc<MeshInner>,
) {
    let cancel = mesh.accept_cancel.clone();
    let stream = match establish_accept(stream, &mesh.tls).await {
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
    let Some(Ok(Msg::Hello {
        node: from,
        protocol: peer_protocol,
        ..
    })) = hello
    else {
        return;
    };
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
#[allow(clippy::too_many_arguments)]
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
        Msg::Invalidate { .. } | Msg::Replicate { .. } | Msg::ReplicateBatch { .. } => {
            let _ = inbound_tx.send(InboundMsg { from, msg }).await;
            false
        }
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
            serve_ae_entries(framed, cache, buckets, handler, cancel).await
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
        // A duplicate `Hello`, or `StChunk`/`AeBucket`/`AeSketch`/
        // `AePartDigests`/`AePart`/`AePartSketch`/`StUnavailable`/`ReqDone` sent only as
        // replies on a connection this node initiated, never on one being
        // served here.
        Msg::Hello { .. }
        | Msg::StChunk { .. }
        | Msg::AeBucket { .. }
        | Msg::AeSketch { .. }
        | Msg::AePartDigests { .. }
        | Msg::AePart { .. }
        | Msg::AePartSketch { .. }
        | Msg::StUnavailable { .. }
        | Msg::ReqDone => false,
    }
}

/// Sends `msg`, racing the write against `cancel`. Returns `true` when this
/// connection is done.
async fn send_or_cancelled(framed: &mut PeerFramed, msg: &Msg, cancel: &CancellationToken) -> bool {
    tokio::select! {
        biased;
        () = cancel.cancelled() => true,
        sent = send_msg(framed, msg) => sent.is_err(),
    }
}

/// Serves one state-transfer request. Returns `true` when this connection
/// is done, `false` once the final `done: true` chunk
/// sent cleanly and the connection remains reusable.
async fn serve_state_transfer(
    framed: &mut PeerFramed,
    cache: SmolStr,
    handler: &dyn RequestHandler,
    cancel: &CancellationToken,
    peer_protocol: u16,
) -> bool {
    if !handler.snapshot_available(cache.clone()) {
        if wire::peer_supports(peer_protocol, wire::PROTOCOL_ST_UNAVAILABLE) {
            tracing::debug!(%cache, "state transfer requested before this node is warm; declined");
            return send_or_cancelled(framed, &Msg::StUnavailable { cache }, cancel).await;
        }
        tracing::debug!(
            %cache,
            peer_protocol,
            "state transfer requested before this node is warm by a peer that cannot be declined; serving"
        );
    }
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
/// bucket's reply plus a trailing [`Msg::ReqDone`] and flushing once. The
/// three-tier responder rule: a bucket whose local entry count exceeds
/// `handler.ae_part_min_bucket()` replies with its 64 part digests
/// ([`Msg::AePartDigests`]) without ever materializing its listing; a
/// smaller mismatched bucket falls back to the existing rule, an IBLT
/// sketch past `handler.ae_sketch_min_bucket()` entries or else the full
/// [`Msg::AeBucket`] listing. Returns `true` when this connection is done.
async fn serve_ae_digest(
    framed: &mut PeerFramed,
    cache: SmolStr,
    remote_buckets: Vec<(u16, u64)>,
    handler: &dyn RequestHandler,
    cancel: &CancellationToken,
    peer_protocol: u16,
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

    let mut replies: Vec<Msg> = Vec::new();
    if !mismatched.is_empty() {
        // A peer that predates part digests gets a listing or sketch for
        // every bucket, whatever its size.
        let part_min_bucket = if wire::peer_supports(peer_protocol, wire::PROTOCOL_PART_DIGESTS) {
            handler.ae_part_min_bucket()
        } else {
            usize::MAX
        };
        let lens: std::collections::HashMap<u16, usize> = handler
            .bucket_lens(cache.clone(), mismatched.clone())
            .await
            .into_iter()
            .collect();
        let (big, small): (Vec<u16>, Vec<u16>) = mismatched
            .into_iter()
            .partition(|bucket| lens.get(bucket).copied().unwrap_or(0) > part_min_bucket);

        if !big.is_empty() {
            replies.extend(
                handler
                    .part_digests(cache.clone(), big)
                    .await
                    .into_iter()
                    .map(|(bucket, digests)| Msg::AePartDigests {
                        cache: cache.clone(),
                        bucket,
                        digests,
                    }),
            );
        }
        if !small.is_empty() {
            // One shard pass for every small mismatched bucket at once:
            // per-bucket scans would be quadratic against a
            // mostly-divergent peer. A bucket answered above with part
            // digests never reaches this call, so its listing is never
            // materialized.
            let min_bucket = handler.ae_sketch_min_bucket();
            let sketch_cells = handler.ae_sketch_cells();
            replies.extend(
                handler
                    .entries_for_buckets(cache.clone(), small)
                    .await
                    .into_iter()
                    .map(|(bucket, entries)| {
                        match listing_or_sketch(entries, min_bucket, sketch_cells) {
                            ListingOrSketch::Sketch(cells) => Msg::AeSketch {
                                cache: cache.clone(),
                                bucket,
                                cells,
                            },
                            ListingOrSketch::Listing(entries) => Msg::AeBucket {
                                cache: cache.clone(),
                                bucket,
                                entries,
                            },
                        }
                    }),
            );
        }
    }
    replies.push(Msg::ReqDone);
    send_batch_or_cancelled(framed, &replies, cancel).await
}

/// Serves an `AeParts` request: the part-grained counterpart of
/// [`serve_ae_digest`]'s classification step, one reply per requested
/// `(bucket, part)` pair, a part past `handler.ae_sketch_min_bucket()`
/// entries answered with [`Msg::AePartSketch`], otherwise
/// [`Msg::AePart`]. Unlike `AeDigest`, never deferred by
/// `MeshInner::defers_ae_digest_from`: a part request only ever follows a
/// part-digest reply the requester already paid to compare.
async fn serve_ae_parts(
    framed: &mut PeerFramed,
    cache: SmolStr,
    parts: Vec<(u16, u8)>,
    handler: &dyn RequestHandler,
    cancel: &CancellationToken,
) -> bool {
    let mut replies: Vec<Msg> = if parts.is_empty() {
        Vec::new()
    } else {
        let min_bucket = handler.ae_sketch_min_bucket();
        let sketch_cells = handler.ae_sketch_cells();
        handler
            .entries_for_parts(cache.clone(), parts)
            .await
            .into_iter()
            .map(|((bucket, part), entries)| {
                match listing_or_sketch(entries, min_bucket, sketch_cells) {
                    ListingOrSketch::Sketch(cells) => Msg::AePartSketch {
                        cache: cache.clone(),
                        bucket,
                        part,
                        cells,
                    },
                    ListingOrSketch::Listing(entries) => Msg::AePart {
                        cache: cache.clone(),
                        bucket,
                        part,
                        entries,
                    },
                }
            })
            .collect()
    };
    replies.push(Msg::ReqDone);
    send_batch_or_cancelled(framed, &replies, cancel).await
}

/// A responder's shape for one mismatched bucket or part: its listing, or
/// an IBLT sketch once the listing would outweigh one.
#[derive(Debug, PartialEq, Eq)]
enum ListingOrSketch {
    Listing(Vec<(bytes::Bytes, crate::hlc::Hlc)>),
    Sketch(Vec<crate::wire::Cell>),
}

/// The one crossover rule for [`serve_ae_digest`] and [`serve_ae_parts`]:
/// more than `min_bucket` entries answer with a `sketch_cells`-cell sketch
/// over their `(key_hash, version)` pairs, anything up to it with the
/// listing itself.
fn listing_or_sketch(
    entries: Vec<(bytes::Bytes, crate::hlc::Hlc)>,
    min_bucket: usize,
    sketch_cells: usize,
) -> ListingOrSketch {
    if entries.len() > min_bucket {
        let mut iblt = crate::cluster::sketch::Iblt::new(sketch_cells);
        for (key, ver) in &entries {
            iblt.insert(xxhash_rust::xxh3::xxh3_64(key), *ver);
        }
        ListingOrSketch::Sketch(iblt.into_cells())
    } else {
        ListingOrSketch::Listing(entries)
    }
}

/// Serves an `AeEntries` request: the sketch fallback, full listings for the
/// named `buckets`, requested explicitly rather than chosen by the responder.
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

/// Serves an `AePullHashes` request, the sketch-decoded counterpart to
/// [`serve_ae_pull`]: records for `bucket`'s entries whose key hash is in `hashes`.
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

/// Reads `AeBucket` replies until [`Msg::ReqDone`]. On success, checks
/// `framed` back into `pool`; on error, the connection is dropped instead.
/// `first`, when set, is consumed before any further read: `acquire_conn`
/// hands one over when it already peeked a reply off a reused connection.
pub(super) async fn collect_ae_buckets(
    mut framed: PeerFramed,
    pool: &ReqPool,
    first: Option<Result<Msg, CodecError>>,
) -> Result<Vec<(u16, Vec<(Bytes, crate::hlc::Hlc)>)>, CodecError> {
    let mut result = Vec::new();
    let mut pending = first;
    loop {
        let received = match pending.take() {
            Some(msg) => Some(msg),
            None => recv_msg(&mut framed).await,
        };
        match received {
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
/// reply complete: [`Mesh::ae_round`]'s collector, one [`super::AeMismatch`]
/// per bucket regardless of shape. Same pool-checkin rules as
/// [`collect_ae_buckets`].
///
/// [`Mesh::ae_round`]: super::Mesh::ae_round
pub(super) async fn collect_ae_mismatches(
    mut framed: PeerFramed,
    pool: &ReqPool,
    first: Option<Result<Msg, CodecError>>,
) -> Result<Vec<super::AeMismatch>, CodecError> {
    let mut result = Vec::new();
    let mut pending = first;
    loop {
        let received = match pending.take() {
            Some(msg) => Some(msg),
            None => recv_msg(&mut framed).await,
        };
        match received {
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
            Some(Ok(Msg::AePartDigests {
                bucket, digests, ..
            })) => {
                result.push(super::AeMismatch::PartDigests(bucket, digests));
            }
            Some(Ok(_)) => {} // unexpected message on this connection; keep reading
            Some(Err(err)) => return Err(err),
            None => return Err(unexpected_close("anti-entropy digest reply")),
        }
    }
}

/// Reads `AePart`/`AePartSketch` replies until [`Msg::ReqDone`]:
/// [`super::Mesh::ae_parts`]'s collector, one [`super::AePartReply`] per
/// part. Same pool-checkin rules as [`collect_ae_buckets`].
pub(super) async fn collect_ae_part_replies(
    mut framed: PeerFramed,
    pool: &ReqPool,
    first: Option<Result<Msg, CodecError>>,
) -> Result<Vec<super::AePartReply>, CodecError> {
    let mut result = Vec::new();
    let mut pending = first;
    loop {
        let received = match pending.take() {
            Some(msg) => Some(msg),
            None => recv_msg(&mut framed).await,
        };
        match received {
            Some(Ok(Msg::ReqDone)) => {
                pool.checkin(framed);
                return Ok(result);
            }
            Some(Ok(Msg::AePart {
                bucket,
                part,
                entries,
                ..
            })) => result.push(super::AePartReply::Listing {
                bucket,
                part,
                entries,
            }),
            Some(Ok(Msg::AePartSketch {
                bucket,
                part,
                cells,
                ..
            })) => result.push(super::AePartReply::Sketch {
                bucket,
                part,
                cells,
            }),
            Some(Ok(_)) => {} // unexpected message on this connection; keep reading
            Some(Err(err)) => return Err(err),
            None => return Err(unexpected_close("anti-entropy part reply")),
        }
    }
}

/// Reads `Replicate`/`ReplicateBatch` replies until [`Msg::ReqDone`], per
/// [`collect_ae_buckets`].
pub(super) async fn collect_pulled_records(
    mut framed: PeerFramed,
    pool: &ReqPool,
    first: Option<Result<Msg, CodecError>>,
) -> Result<Vec<crate::wire::WireRecord>, CodecError> {
    let mut result = Vec::new();
    let mut pending = first;
    loop {
        let received = match pending.take() {
            Some(msg) => Some(msg),
            None => recv_msg(&mut framed).await,
        };
        match received {
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
/// reading `StChunk`s off the wire only as the consumer polls, yielding
/// each `StChunk`'s records as one `Vec` at the donor's own chunk
/// boundaries. On the final `done: true` chunk, checks the connection back
/// into `pool`; any error path drops it instead.
pub(super) fn state_stream(
    framed: PeerFramed,
    pool: Arc<ReqPool>,
    first: Option<Result<Msg, CodecError>>,
) -> futures::stream::BoxStream<'static, Result<Vec<WireRecord>, CodecError>> {
    Box::pin(futures::stream::unfold(
        Some((framed, first)),
        move |state| {
            let pool = Arc::clone(&pool);
            async move {
                let (mut framed, mut pending) = state?;
                loop {
                    // `Mesh::request_state` already read the first message
                    // to tell a declined request from a stream; it is
                    // consumed here before the connection is read again.
                    let received = match pending.take() {
                        Some(first) => Some(first),
                        None => recv_msg(&mut framed).await,
                    };
                    match received {
                        Some(Ok(Msg::StChunk { recs, done, .. })) => {
                            if recs.is_empty() {
                                // The trailing marker chunk, or a no-op
                                // chunk: nothing to yield either way.
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
                            return Some((Ok(recs), Some((framed, None))));
                        }
                        Some(Ok(_)) => {} // unexpected message on this stream; keep reading
                        Some(Err(err)) => return Some((Err(err), None)),
                        // Surfacing this as an error, not a silent stream
                        // end, lets the retry logic tell a truncated
                        // transfer from a finished one.
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
        },
    ))
}

#[cfg(test)]
mod tests {
    #[test]
    fn listing_or_sketch_crosses_over_past_min_bucket() {
        let entry = |n: u64| {
            (
                bytes::Bytes::from(n.to_be_bytes().to_vec()),
                crate::hlc::Hlc {
                    wall_ms: n,
                    logical: 0,
                    node: crate::node::NodeId::from(1),
                },
            )
        };
        let small: Vec<_> = (0..3).map(entry).collect();
        assert_eq!(
            super::listing_or_sketch(small.clone(), 3, 6),
            super::ListingOrSketch::Listing(small),
            "up to min_bucket entries stay a listing"
        );
        let large: Vec<_> = (0..4).map(entry).collect();
        match super::listing_or_sketch(large, 3, 6) {
            super::ListingOrSketch::Sketch(cells) => assert_eq!(cells.len(), 6),
            other @ super::ListingOrSketch::Listing(_) => {
                panic!("expected a sketch, got {other:?}")
            }
        }
    }

    /// Below [`super::REQ_POOL_MAX_IDLE`] a pooled connection stays fresh;
    /// at or past it, `ReqPool::checkout` must treat it as stale. Uses
    /// tokio's paused clock so the boundary is exact rather than
    /// timing-dependent.
    #[tokio::test(start_paused = true)]
    async fn pooled_is_fresh_expires_at_the_max_idle_boundary() {
        let checked_in = tokio::time::Instant::now().into();
        tokio::time::advance(
            super::REQ_POOL_MAX_IDLE
                .checked_sub(Duration::from_millis(1))
                .expect("invariant: REQ_POOL_MAX_IDLE exceeds one millisecond"),
        )
        .await;
        assert!(
            super::pooled_is_fresh(checked_in, tokio::time::Instant::now().into()),
            "just under REQ_POOL_MAX_IDLE must stay fresh"
        );

        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(
            !super::pooled_is_fresh(checked_in, tokio::time::Instant::now().into()),
            "at REQ_POOL_MAX_IDLE must be stale"
        );
    }

    // Only the `not(sim)` tests below dial real loopback sockets through
    // `handle_accepted`/`run_peer_writer`/the request collectors; under
    // `sim` those tests (and these imports) are compiled out entirely.
    #[cfg(not(feature = "sim"))]
    use std::net::SocketAddr;
    #[cfg(not(feature = "sim"))]
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use futures::{SinkExt as _, StreamExt as _};
    use smol_str::SmolStr;
    #[cfg(not(feature = "sim"))]
    use tokio::sync::mpsc;
    // Real tokio sockets and a locally-built codec, not this module's own
    // `new_framed`/`send_msg`/`recv_msg`: this suite exercises the real
    // socket stack regardless of feature flags.
    use tokio::net::{TcpListener, TcpStream};
    use tokio_util::codec::LengthDelimitedCodec;
    #[cfg(not(feature = "sim"))]
    use tokio_util::sync::CancellationToken;

    #[cfg(not(feature = "sim"))]
    use super::{InboundMsg, PeerFramed, ReqPool};
    use super::{OutFrame, coalesce_replicate};
    #[cfg(not(feature = "sim"))]
    use crate::error::CodecError;
    use crate::hlc::Hlc;
    #[cfg(not(feature = "sim"))]
    use crate::net::AeMismatch;
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

    /// A lone queued `Msg::Replicate` must come out of `coalesce_replicate`
    /// as exactly the frame it already carried in: no re-encode.
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

    /// Consecutive same-cache `Msg::Replicate` entries merge into one
    /// `Msg::ReplicateBatch` frame decoding back to all of them, in order.
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

    /// A cache-name change mid-run closes off the pending merge rather than
    /// mixing records from two caches into one `Msg::ReplicateBatch`.
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

    /// A `Msg::ReplicateBatch` following same-cache entries in the pending
    /// run merges into it too, not just a lone `Msg::Replicate`.
    #[test]
    fn coalesce_replicate_merges_a_replicate_batch_into_a_pending_run() {
        let drained = vec![
            out_frame(Msg::Replicate {
                cache: SmolStr::new("users"),
                rec: record(1),
            }),
            out_frame(Msg::ReplicateBatch {
                cache: SmolStr::new("users"),
                recs: vec![record(2), record(3)],
            }),
        ];
        let out = coalesce_replicate(drained);
        assert_eq!(
            out.len(),
            1,
            "a same-cache batch must merge into the pending run"
        );
        let decoded = wire::decode(&out[0]).expect("decodes");
        assert_eq!(
            decoded,
            Msg::ReplicateBatch {
                cache: SmolStr::new("users"),
                recs: vec![record(1), record(2), record(3)],
            }
        );
    }

    /// A non-`Replicate`-class frame following a pending run flushes the run
    /// first, then passes through untouched.
    #[test]
    fn coalesce_replicate_flushes_the_pending_run_before_a_non_replicate_frame() {
        let invalidate = Msg::Invalidate {
            cache: SmolStr::new("users"),
            key: Bytes::from_static(b"k"),
            ver: Hlc {
                wall_ms: 9,
                logical: 0,
                node: NodeId::from(1),
            },
        };
        let drained = vec![
            out_frame(Msg::Replicate {
                cache: SmolStr::new("users"),
                rec: record(1),
            }),
            out_frame(Msg::Replicate {
                cache: SmolStr::new("users"),
                rec: record(2),
            }),
            out_frame(invalidate.clone()),
        ];
        let out = coalesce_replicate(drained);
        assert_eq!(
            out.len(),
            2,
            "the non-replicate frame must close off the pending merge first"
        );
        assert_eq!(
            wire::decode(&out[0]).expect("decodes"),
            Msg::ReplicateBatch {
                cache: SmolStr::new("users"),
                recs: vec![record(1), record(2)],
            }
        );
        assert_eq!(wire::decode(&out[1]).expect("decodes"), invalidate);
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
        let mut server = accept.await.expect("connection accepted");

        let sent = Msg::Hello {
            node: NodeId::from(7),
            incarnation: 3,
            protocol: wire::PROTOCOL_VERSION,
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

    // `MeshStream` is a different concrete type per feature, so this test
    // exercises the real transport directly and is skipped under `sim`.
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
            fn bucket_lens(
                &self,
                _cache: SmolStr,
                _buckets: Vec<u16>,
            ) -> BoxFuture<'_, Vec<(u16, usize)>> {
                Box::pin(async { Vec::new() })
            }
            fn part_digests(
                &self,
                _cache: SmolStr,
                _buckets: Vec<u16>,
            ) -> BoxFuture<'_, Vec<(u16, Vec<u64>)>> {
                Box::pin(async { Vec::new() })
            }
            fn entries_for_parts(
                &self,
                _cache: SmolStr,
                _parts: Vec<(u16, u8)>,
            ) -> BoxFuture<'_, crate::store::PartEntries> {
                Box::pin(async { Vec::new() })
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("listener has a local addr");
        let accept = tokio::spawn(async move { listener.accept().await.expect("accept").0 });
        let _client = TcpStream::connect(addr).await.expect("connect");
        let server_stream = accept.await.expect("connection accepted");

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
                crate::wire::PROTOCOL_VERSION,
            )
            .await;
        });

        cancel.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(2), served)
            .await
            .expect(
                "an accepted-connection handler must observe cancellation instead of blocking \
                 forever on a snapshot stream that never yields; otherwise Mesh::shutdown() \
                 leaves it running with the shard registry Arc still held",
            )
            .expect("accepted-connection handler did not panic");
    }

    /// `TlsCtx` for a plain (non-TLS) accepted/dialed connection in a test,
    /// matching whichever concrete shape the active feature set gives it.
    #[cfg(all(feature = "tls", not(feature = "sim")))]
    fn no_tls() -> super::TlsCtx {
        None
    }
    #[cfg(all(not(feature = "tls"), not(feature = "sim")))]
    fn no_tls() -> super::TlsCtx {
        super::TlsCtx
    }

    /// A `RequestHandler` with nothing to serve: every lookup comes back
    /// empty, never called in the tests that use it since they exercise
    /// paths that never reach the handler.
    #[cfg(not(feature = "sim"))]
    struct EmptyHandler;
    #[cfg(not(feature = "sim"))]
    impl super::RequestHandler for EmptyHandler {
        fn snapshot_chunks(
            &self,
            _cache: SmolStr,
        ) -> futures::stream::BoxStream<'static, Vec<WireRecord>> {
            Box::pin(futures::stream::empty())
        }
        fn digests(&self, _cache: SmolStr) -> futures::future::BoxFuture<'_, Vec<(u16, u64)>> {
            Box::pin(async { Vec::new() })
        }
        fn bucket_entries(
            &self,
            _cache: SmolStr,
            _bucket: u16,
        ) -> futures::future::BoxFuture<'_, Vec<(Bytes, Hlc)>> {
            Box::pin(async { Vec::new() })
        }
        fn entries_for_buckets(
            &self,
            _cache: SmolStr,
            _buckets: Vec<u16>,
        ) -> futures::future::BoxFuture<'_, crate::store::BucketEntries> {
            Box::pin(async { Vec::new() })
        }
        fn records_for(
            &self,
            _cache: SmolStr,
            _keys: Vec<Bytes>,
        ) -> futures::future::BoxFuture<'_, Vec<WireRecord>> {
            Box::pin(async { Vec::new() })
        }
        fn bucket_lens(
            &self,
            _cache: SmolStr,
            _buckets: Vec<u16>,
        ) -> futures::future::BoxFuture<'_, Vec<(u16, usize)>> {
            Box::pin(async { Vec::new() })
        }
        fn part_digests(
            &self,
            _cache: SmolStr,
            _buckets: Vec<u16>,
        ) -> futures::future::BoxFuture<'_, Vec<(u16, Vec<u64>)>> {
            Box::pin(async { Vec::new() })
        }
        fn entries_for_parts(
            &self,
            _cache: SmolStr,
            _parts: Vec<(u16, u8)>,
        ) -> futures::future::BoxFuture<'_, crate::store::PartEntries> {
            Box::pin(async { Vec::new() })
        }
    }

    /// Dials a fresh loopback connection to a fake donor that sends every
    /// message in `prelude` and then drops the connection without a
    /// terminating `Msg::ReqDone`, for the "closed before `ReqDone`" and
    /// "unrelated frame skipped" collector tests below.
    #[cfg(not(feature = "sim"))]
    async fn dial_fake_donor(prelude: Vec<Msg>) -> PeerFramed {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("listener has a local addr");
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut framed = LengthDelimitedCodec::builder()
                .max_frame_length(MAX_FRAME)
                .new_framed(stream);
            for msg in prelude {
                let encoded = wire::encode(&msg).expect("encodes");
                framed.send(encoded).await.expect("send");
            }
            // Dropped here: the connection closes without ever sending
            // `Msg::ReqDone`.
        });
        let client = TcpStream::connect(addr).await.expect("connect");
        super::new_framed(as_mesh_stream(client))
    }

    /// Asserts `err` is the `UnexpectedEof` a collector returns when its
    /// connection closes before the terminating `Msg::ReqDone`.
    #[cfg(not(feature = "sim"))]
    fn assert_unexpected_eof(err: &CodecError) {
        assert!(
            matches!(err, CodecError::Io(io_err) if io_err.kind() == std::io::ErrorKind::UnexpectedEof),
            "expected an UnexpectedEof i/o error, got {err:?}"
        );
    }

    #[cfg(not(feature = "sim"))]
    #[tokio::test]
    async fn collect_ae_buckets_errors_on_a_connection_closed_before_req_done() {
        let framed = dial_fake_donor(Vec::new()).await;
        let pool = ReqPool::new();
        let err = super::collect_ae_buckets(framed, &pool, None)
            .await
            .expect_err("a connection closed before ReqDone must error");
        assert_unexpected_eof(&err);
    }

    #[cfg(not(feature = "sim"))]
    #[tokio::test]
    async fn collect_ae_mismatches_errors_on_a_connection_closed_before_req_done() {
        let framed = dial_fake_donor(Vec::new()).await;
        let pool = ReqPool::new();
        let err = super::collect_ae_mismatches(framed, &pool, None)
            .await
            .expect_err("a connection closed before ReqDone must error");
        assert_unexpected_eof(&err);
    }

    #[cfg(not(feature = "sim"))]
    #[tokio::test]
    async fn collect_pulled_records_errors_on_a_connection_closed_before_req_done() {
        let framed = dial_fake_donor(Vec::new()).await;
        let pool = ReqPool::new();
        let err = super::collect_pulled_records(framed, &pool, None)
            .await
            .expect_err("a connection closed before ReqDone must error");
        assert_unexpected_eof(&err);
    }

    #[cfg(not(feature = "sim"))]
    #[tokio::test]
    async fn collect_ae_buckets_skips_an_unrelated_hello_mid_reply() {
        let entries = vec![(
            Bytes::from_static(b"k1"),
            Hlc {
                wall_ms: 5,
                logical: 0,
                node: NodeId::from(1),
            },
        )];
        let framed = dial_fake_donor(vec![
            Msg::Hello {
                node: NodeId::from(9),
                incarnation: 1,
                protocol: wire::PROTOCOL_VERSION,
            },
            Msg::AeBucket {
                cache: SmolStr::new("users"),
                bucket: 3,
                entries: entries.clone(),
            },
            Msg::ReqDone,
        ])
        .await;
        let pool = ReqPool::new();
        let got = super::collect_ae_buckets(framed, &pool, None)
            .await
            .expect("the stray Hello must be skipped, not break the reply");
        assert_eq!(got, vec![(3, entries)]);
    }

    #[cfg(not(feature = "sim"))]
    #[tokio::test]
    async fn collect_ae_mismatches_skips_an_unrelated_hello_mid_reply() {
        let framed = dial_fake_donor(vec![
            Msg::Hello {
                node: NodeId::from(9),
                incarnation: 1,
                protocol: wire::PROTOCOL_VERSION,
            },
            Msg::AeSketch {
                cache: SmolStr::new("users"),
                bucket: 2,
                cells: Vec::new(),
            },
            Msg::ReqDone,
        ])
        .await;
        let pool = ReqPool::new();
        let got = super::collect_ae_mismatches(framed, &pool, None)
            .await
            .expect("the stray Hello must be skipped, not break the reply");
        assert_eq!(got, vec![AeMismatch::Sketch(2, Vec::new())]);
    }

    #[cfg(not(feature = "sim"))]
    #[tokio::test]
    async fn collect_pulled_records_skips_an_unrelated_hello_mid_reply() {
        let rec = WireRecord {
            key: Bytes::from_static(b"k1"),
            value: Some(Bytes::from_static(b"v1")),
            ver: Hlc {
                wall_ms: 5,
                logical: 0,
                node: NodeId::from(1),
            },
            expires_at_ms: None,
        };
        let framed = dial_fake_donor(vec![
            Msg::Hello {
                node: NodeId::from(9),
                incarnation: 1,
                protocol: wire::PROTOCOL_VERSION,
            },
            Msg::Replicate {
                cache: SmolStr::new("users"),
                rec: rec.clone(),
            },
            Msg::ReqDone,
        ])
        .await;
        let pool = ReqPool::new();
        let got = super::collect_pulled_records(framed, &pool, None)
            .await
            .expect("the stray Hello must be skipped, not break the reply");
        assert_eq!(got, vec![rec]);
    }

    #[cfg(not(feature = "sim"))]
    #[tokio::test]
    async fn collect_ae_mismatches_reports_part_digests_replies() {
        let digests: Vec<u64> = (0..64u64).collect();
        let framed = dial_fake_donor(vec![
            Msg::AePartDigests {
                cache: SmolStr::new("users"),
                bucket: 5,
                digests: digests.clone(),
            },
            Msg::ReqDone,
        ])
        .await;
        let pool = ReqPool::new();
        let got = super::collect_ae_mismatches(framed, &pool, None)
            .await
            .expect("collects the AePartDigests reply");
        assert_eq!(got, vec![AeMismatch::PartDigests(5, digests)]);
    }

    #[cfg(not(feature = "sim"))]
    #[tokio::test]
    async fn collect_ae_part_replies_errors_on_a_connection_closed_before_req_done() {
        let framed = dial_fake_donor(Vec::new()).await;
        let pool = ReqPool::new();
        let err = super::collect_ae_part_replies(framed, &pool, None)
            .await
            .expect_err("a connection closed before ReqDone must error");
        assert_unexpected_eof(&err);
    }

    #[cfg(not(feature = "sim"))]
    #[tokio::test]
    async fn collect_ae_part_replies_reports_listings_and_sketches() {
        let entries = vec![(
            Bytes::from_static(b"k1"),
            Hlc {
                wall_ms: 5,
                logical: 0,
                node: NodeId::from(1),
            },
        )];
        let framed = dial_fake_donor(vec![
            Msg::Hello {
                node: NodeId::from(9),
                incarnation: 1,
                protocol: wire::PROTOCOL_VERSION,
            },
            Msg::AePart {
                cache: SmolStr::new("users"),
                bucket: 3,
                part: 7,
                entries: entries.clone(),
            },
            Msg::AePartSketch {
                cache: SmolStr::new("users"),
                bucket: 3,
                part: 8,
                cells: Vec::new(),
            },
            Msg::ReqDone,
        ])
        .await;
        let pool = ReqPool::new();
        let got = super::collect_ae_part_replies(framed, &pool, None)
            .await
            .expect("the stray Hello must be skipped, not break the reply");
        assert_eq!(
            got,
            vec![
                super::super::AePartReply::Listing {
                    bucket: 3,
                    part: 7,
                    entries,
                },
                super::super::AePartReply::Sketch {
                    bucket: 3,
                    part: 8,
                    cells: Vec::new(),
                },
            ]
        );
    }

    /// `Msg::ReqDone`/`Msg::AeBucket` arriving as the first message after
    /// `Hello` fall into the request-class routing arm that treats this as
    /// a persistent broadcast-class connection: neither served nor
    /// forwarded to `inbound_tx`, but the loop keeps reading, so a genuine
    /// broadcast message afterward still comes through.
    #[cfg(not(feature = "sim"))]
    #[tokio::test]
    async fn req_done_and_ae_bucket_after_hello_are_not_served_or_forwarded() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("listener has a local addr");
        let accept = tokio::spawn(async move { listener.accept().await.expect("accept").0 });
        let client_stream = TcpStream::connect(addr).await.expect("connect");
        let server_stream = accept.await.expect("connection accepted");

        let (inbound_tx, mut inbound_rx) = mpsc::channel(8);
        let cancel = CancellationToken::new();
        let handler: Arc<dyn super::RequestHandler> = Arc::new(EmptyHandler);
        // `handle_accepted` takes the pre-TLS raw stream, not `MeshStream`;
        // it layers TLS on internally via `establish_accept`.
        let accepted_task = tokio::spawn(super::handle_accepted(
            server_stream,
            inbound_tx,
            handler,
            super::MeshInner::for_tests(no_tls(), cancel.clone()),
        ));

        let mut client = LengthDelimitedCodec::builder()
            .max_frame_length(MAX_FRAME)
            .new_framed(client_stream);
        let from = NodeId::from(9);
        for msg in [
            Msg::Hello {
                node: from,
                incarnation: 1,
                protocol: wire::PROTOCOL_VERSION,
            },
            Msg::ReqDone,
            Msg::AeBucket {
                cache: SmolStr::new("users"),
                bucket: 0,
                entries: Vec::new(),
            },
        ] {
            let encoded = wire::encode(&msg).expect("encodes");
            client.send(encoded).await.expect("send");
        }

        let nothing = tokio::time::timeout(Duration::from_millis(200), inbound_rx.recv()).await;
        assert!(
            nothing.is_err(),
            "ReqDone/AeBucket right after Hello must not reach inbound_tx"
        );

        // The accept loop must still be reading: a genuine broadcast
        // message afterward is forwarded normally.
        let invalidate = Msg::Invalidate {
            cache: SmolStr::new("users"),
            key: Bytes::from_static(b"k"),
            ver: Hlc {
                wall_ms: 1,
                logical: 0,
                node: from,
            },
        };
        let encoded = wire::encode(&invalidate).expect("encodes");
        client.send(encoded).await.expect("send");
        let got = tokio::time::timeout(Duration::from_secs(2), inbound_rx.recv())
            .await
            .expect("the connection must still be reading")
            .expect("channel open");
        assert_eq!(
            got,
            InboundMsg {
                from,
                msg: invalidate
            }
        );

        cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(2), accepted_task).await;
    }

    #[cfg(all(unix, not(feature = "sim")))]
    #[tokio::test]
    async fn disable_nagle_logs_and_does_not_panic_when_set_nodelay_fails() {
        use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("listener has a local addr");
        let stream = TcpStream::connect(addr).await.expect("connect");

        // Close the stream's underlying fd out from under it, so the
        // subsequent `set_nodelay` fails with a real OS error (EBADF)
        // instead of the happy path.
        let fd = stream.as_raw_fd();
        // SAFETY: `fd` is a valid, open fd owned by `stream`; wrapping and
        // immediately dropping it closes that fd number exactly once. The
        // stream is forgotten below so its own `Drop` never double-closes it.
        drop(unsafe { OwnedFd::from_raw_fd(fd) });

        super::disable_nagle(&stream); // must log, not panic
        std::mem::forget(stream);
    }

    #[cfg(not(feature = "sim"))]
    #[tokio::test]
    async fn run_peer_writer_exits_promptly_when_cancelled_while_retrying() {
        // Loopback port 1 is unassigned: the connection is refused
        // immediately, so the writer cycles through its reconnect backoff
        // instead of hanging on a slow connect.
        let unreachable: SocketAddr = "127.0.0.1:1".parse().expect("valid addr");
        let invalidate = Arc::new(super::DropOldestQueue::new(4));
        let (_replicate_tx, replicate_rx) = mpsc::channel(4);
        let cancel = CancellationToken::new();
        let handle = tokio::spawn(super::run_peer_writer(
            NodeId::from(1),
            1,
            unreachable,
            invalidate,
            replicate_rx,
            cancel.clone(),
            no_tls(),
        ));

        // Let it fail to connect and settle into the retry backoff.
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect(
                "run_peer_writer must exit promptly once cancelled while retrying, not wait \
                 out its reconnect backoff",
            )
            .expect("writer task did not panic");
    }
}

//! Framing and connection tasks: the accept-side demux between persistent
//! mesh traffic and pooled request/response, and the per-peer dial-and-write
//! loop over the broadcast outboxes.

mod framing;
#[cfg(test)]
mod tests;

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{FutureExt as _, SinkExt as _, StreamExt as _};
use smol_str::SmolStr;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::outbox::DropOldestQueue;
use super::tcp::{TcpListener, TcpStream};
use super::{
    AeMismatch, AePartReply, AeRoundOutcome, AeServeOutcome, BucketStreamItem, FetchOutcome,
    Greeting, InboundMsg, LoadOutcome, LoadServe, MeshInner, OutFrame, RequestHandler, TlsCtx,
};
use crate::error::CodecError;
use crate::node::NodeId;
use crate::store::{BucketDigest, BucketPart, BucketPartDigests, KeyVersion};
use crate::wire::{self, MAX_FRAME, Msg, WireRecord};
use framing::{
    PeerFramed, disable_nagle, establish_accept, establish_dial, feed_msg, key_versions_to_wire,
    new_framed, send_batch, send_batch_or_cancelled, send_frames, send_or_cancelled,
    wire_to_key_versions,
};

pub(super) use framing::{recv_msg, send_msg};

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
    greeting: Greeting,
    tls: &TlsCtx,
) -> Result<PeerFramed, CodecError> {
    let stream = TcpStream::connect(addr).await?;
    disable_nagle(&stream);
    let stream = establish_dial(stream, tls).await?;
    let mut framed = new_framed(stream);
    send_msg(&mut framed, &greeting.hello()).await?;
    Ok(framed)
}

/// [`dial_with_hello`], but `feed()`s `first` onto the fresh connection
/// alongside `Hello` and flushes once for both, one syscall instead of two.
pub(super) async fn dial_with_hello_and(
    addr: SocketAddr,
    greeting: Greeting,
    tls: &TlsCtx,
    first: Msg,
) -> Result<PeerFramed, CodecError> {
    let stream = TcpStream::connect(addr).await?;
    disable_nagle(&stream);
    let stream = establish_dial(stream, tls).await?;
    let mut framed = new_framed(stream);
    send_batch(&mut framed, &[greeting.hello(), first]).await?;
    Ok(framed)
}

/// The long-lived per-peer writer: connects with a fixed backoff, sends
/// `Hello`, then drains both broadcast-class outboxes until told to stop
/// or the connection breaks, in which case it reconnects. Never reads from
/// the connection; two connections per pair of peers is normal.
pub(super) async fn run_peer_writer(
    greeting: Greeting,
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
            connected = connect_with_hello(addr, greeting, &tls, &cancel) => match connected {
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
                    let Some(first) = received else { return }; // sender dropped: peer removed
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
    greeting: Greeting,
    tls: &TlsCtx,
    cancel: &CancellationToken,
) -> Option<PeerFramed> {
    const RECONNECT_BACKOFF: Duration = Duration::from_millis(500);
    loop {
        if let Ok(framed) = dial_with_hello(addr, greeting, tls).await {
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
        cluster: peer_cluster,
        ..
    })) = hello
    else {
        return;
    };
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

/// The mismatched-bucket reply messages for one digest exchange, given the
/// responder's own `local` digests and the requester's `remote_buckets`:
/// shared by [`serve_ae_digest`] and [`serve_ae_digest_scoped`], whose only
/// difference is where `local` comes from. The three-tier responder rule: a
/// bucket whose local entry count exceeds `handler.ae_part_min_bucket()`
/// replies with its 64 part digests ([`Msg::AePartDigests`]) without ever
/// materializing its listing; a smaller mismatched bucket falls back to the
/// existing rule, an IBLT sketch past `handler.ae_sketch_min_bucket()`
/// entries or else the full [`Msg::AeBucket`] listing. Does not append the
/// trailing [`Msg::ReqDone`]; callers add it.
async fn ae_mismatch_replies(
    cache: &SmolStr,
    local: &std::collections::HashMap<u16, u64>,
    remote_buckets: Vec<(u16, u64)>,
    handler: &dyn RequestHandler,
    peer_protocol: u16,
) -> Vec<Msg> {
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
            .map(|bl| (bl.bucket, bl.len))
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
                    .map(|bpd| Msg::AePartDigests {
                        cache: cache.clone(),
                        bucket: bpd.bucket,
                        digests: bpd.digests,
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
                                entries: key_versions_to_wire(entries),
                            },
                        }
                    }),
            );
        }
    }
    replies
}

/// Serves one anti-entropy digest exchange, feeding every mismatched
/// bucket's reply plus a trailing [`Msg::ReqDone`] and flushing once.
/// Returns `true` when this connection is done.
async fn serve_ae_digest(
    framed: &mut PeerFramed,
    cache: SmolStr,
    remote_buckets: Vec<(u16, u64)>,
    handler: &dyn RequestHandler,
    cancel: &CancellationToken,
    peer_protocol: u16,
) -> bool {
    let local: std::collections::HashMap<u16, u64> = handler
        .digests(cache.clone())
        .await
        .into_iter()
        .map(|bd| (bd.bucket, bd.digest))
        .collect();
    let mut replies =
        ae_mismatch_replies(&cache, &local, remote_buckets, handler, peer_protocol).await;
    replies.push(Msg::ReqDone);
    send_batch_or_cancelled(framed, &replies, cancel).await
}

/// Serves a `Fetch`: answers with the responder's record or a definitive
/// miss, with `StaleView` when it holds no record in a trusted part and its
/// own view differs from the requester's, or with `FetchDeclined`. Always
/// followed by [`Msg::ReqDone`], so the connection stays poolable.
async fn serve_fetch(
    framed: &mut PeerFramed,
    cache: SmolStr,
    key: Bytes,
    view_hash: u64,
    handler: &dyn RequestHandler,
    cancel: &CancellationToken,
) -> bool {
    let reply = match handler.fetch(cache.clone(), key, view_hash).await {
        super::FetchServe::Found(rec) => Msg::FetchReply { rec },
        super::FetchServe::Stale {
            responder_view_hash,
        } => Msg::StaleView {
            cache,
            responder_view_hash,
        },
        // A responder that cannot vouch for a miss declines outright, so
        // the requester tries its next owner instead of taking `None` as
        // the answer.
        super::FetchServe::Unavailable => Msg::FetchDeclined { cache },
    };
    send_batch_or_cancelled(framed, &[reply, Msg::ReqDone], cancel).await
}

/// Serves a `Load`: the handler loads the keys, and the answer goes back as
/// [`loaded_replies`] or one `LoadFailed`/`LoadDeclined`, then `ReqDone`.
async fn serve_load(
    framed: &mut PeerFramed,
    cache: SmolStr,
    keys: Vec<Bytes>,
    handler: &dyn RequestHandler,
    cancel: &CancellationToken,
) -> bool {
    let mut replies = match handler.load(cache.clone(), keys).await {
        LoadServe::Loaded { found, uncached } => {
            loaded_replies(found, uncached, LOADED_REPLY_BUDGET)
        }
        LoadServe::Failed(message) => vec![Msg::LoadFailed { cache, message }],
        LoadServe::Unavailable => vec![Msg::LoadDeclined { cache }],
    };
    replies.push(Msg::ReqDone);
    send_batch_or_cancelled(framed, &replies, cancel).await
}

/// The bytes of keys and values one [`Msg::Loaded`] carries before
/// [`loaded_replies`] starts the next: half a frame, leaving room for the
/// records' fixed fields.
const LOADED_REPLY_BUDGET: usize = MAX_FRAME / 2;

/// Splits a load's answer into [`Msg::Loaded`] messages, each holding at
/// most `budget` bytes of keys and values unless one entry alone is
/// larger; always at least one message. Pure; unit tested directly.
fn loaded_replies(
    found: Vec<WireRecord>,
    uncached: Vec<(Bytes, Bytes)>,
    budget: usize,
) -> Vec<Msg> {
    let mut replies = Vec::new();
    let mut current = (Vec::new(), Vec::new());
    let mut used = 0usize;
    let entries = found
        .into_iter()
        .map(Ok)
        .chain(uncached.into_iter().map(Err));
    for entry in entries {
        let len = match &entry {
            Ok(rec) => rec.key.len() + rec.value.as_ref().map_or(0, Bytes::len),
            Err((key, value)) => key.len() + value.len(),
        };
        if used > 0 && used + len > budget {
            let (found, uncached) = std::mem::take(&mut current);
            replies.push(Msg::Loaded { found, uncached });
            used = 0;
        }
        used += len;
        match entry {
            Ok(rec) => current.0.push(rec),
            Err(pair) => current.1.push(pair),
        }
    }
    if replies.is_empty() || !current.0.is_empty() || !current.1.is_empty() {
        let (found, uncached) = current;
        replies.push(Msg::Loaded { found, uncached });
    }
    replies
}

/// Serves an `AeDigestScoped`: an epoch check ahead of an ordinary digest
/// exchange. `handler.ae_digest_scoped` folds both together: its `Digests`
/// case is this responder's own owned-bucket digests, view hashes already
/// confirmed to match, fed into the same [`ae_mismatch_replies`] tiering
/// [`serve_ae_digest`] uses. A `Stale` or `Unavailable` answer short-circuits
/// before any bucket is ever touched.
async fn serve_ae_digest_scoped(
    framed: &mut PeerFramed,
    cache: SmolStr,
    view_hash: u64,
    remote_buckets: Vec<(u16, u64)>,
    handler: &dyn RequestHandler,
    cancel: &CancellationToken,
    peer_protocol: u16,
) -> bool {
    let remote_bucket_digests: Vec<BucketDigest> = remote_buckets
        .iter()
        .map(|&(bucket, digest)| BucketDigest { bucket, digest })
        .collect();
    let outcome = handler
        .ae_digest_scoped(cache.clone(), view_hash, remote_bucket_digests)
        .await;
    let mut replies = match outcome {
        AeServeOutcome::Digests(local) => {
            let local: std::collections::HashMap<u16, u64> =
                local.into_iter().map(|bd| (bd.bucket, bd.digest)).collect();
            ae_mismatch_replies(&cache, &local, remote_buckets, handler, peer_protocol).await
        }
        declined => declined_replies(cache, &declined),
    };
    replies.push(Msg::ReqDone);
    send_batch_or_cancelled(framed, &replies, cancel).await
}

/// Serves an `AeDigestMasked`: each bucket whose masked digest differs
/// from this responder's own is answered with its part digests, for the
/// requester to compare part by part within its mask.
async fn serve_ae_digest_masked(
    framed: &mut PeerFramed,
    cache: SmolStr,
    view_hash: u64,
    remote_buckets: Vec<(u16, u64, u64)>,
    handler: &dyn RequestHandler,
    cancel: &CancellationToken,
) -> bool {
    let outcome = handler
        .ae_digest_masked(cache.clone(), view_hash, remote_buckets)
        .await;
    let mut replies = match outcome {
        AeServeOutcome::Digests(mismatched) => mismatched
            .into_iter()
            .map(|BucketPartDigests { bucket, digests }| Msg::AePartDigests {
                cache: cache.clone(),
                bucket,
                digests,
            })
            .collect(),
        declined => declined_replies(cache, &declined),
    };
    replies.push(Msg::ReqDone);
    send_batch_or_cancelled(framed, &replies, cancel).await
}

/// What a scoped round the responder answered without digests replies
/// ahead of `ReqDone`: a `StaleView` when the views differ, else nothing.
fn declined_replies<T>(cache: SmolStr, outcome: &AeServeOutcome<T>) -> Vec<Msg> {
    match *outcome {
        AeServeOutcome::Stale {
            responder_view_hash,
        } => vec![Msg::StaleView {
            cache,
            responder_view_hash,
        }],
        AeServeOutcome::Digests(_) | AeServeOutcome::Unavailable => Vec::new(),
    }
}

/// The most [`Msg::StBucketDone`] frames [`serve_st_buckets`] sends per
/// flush.
const DONE_BATCH: usize = 1024;

/// Sends a [`Msg::StBucketDone`] per id in `ids`, [`DONE_BATCH`] at a
/// time, draining acks via [`drain_ready_acks`] between batches: a donor
/// that only writes fills both directions' socket buffers. Returns `true`
/// when the connection is done.
async fn send_done_run(
    framed: &mut PeerFramed,
    cache: &SmolStr,
    ids: Vec<u16>,
    mesh: &MeshInner,
    from: NodeId,
    cancel: &CancellationToken,
) -> bool {
    for batch in ids.chunks(DONE_BATCH) {
        let dones: Vec<Msg> = batch
            .iter()
            .map(|&bucket| Msg::StBucketDone {
                cache: cache.clone(),
                bucket,
            })
            .collect();
        if send_batch_or_cancelled(framed, &dones, cancel).await
            || drain_ready_acks(framed, cache, mesh, from)
        {
            return true;
        }
    }
    false
}

/// Records every [`Msg::StBucketAck`] for `cache` already readable on
/// `framed`, without waiting for more. Returns `true` when the connection
/// broke.
fn drain_ready_acks(
    framed: &mut PeerFramed,
    cache: &SmolStr,
    mesh: &MeshInner,
    from: NodeId,
) -> bool {
    while let Some(Some(msg)) = recv_msg(framed).now_or_never() {
        match msg {
            Ok(Msg::StBucketAck {
                cache: acked,
                bucket,
                view_hash,
            }) if acked == *cache => mesh.record_bucket_ack(acked, bucket, from, view_hash),
            Ok(_) => {}
            Err(_) => return true,
        }
    }
    false
}

/// Serves an `StBuckets` rebalance pull: streams every chunk
/// [`RequestHandler::st_bucket_chunks`] yields once
/// [`RequestHandler::st_buckets_available`] agrees, the bucket-scoped
/// counterpart of [`serve_state_transfer`]; declines with `StaleView`
/// otherwise (an unrecognized cache, one not open in distribution mode, or
/// a view hash that has diverged from the requester's). Returns `true` when
/// this connection is done.
///
/// Against a requester speaking at least
/// [`wire::PROTOCOL_ST_BUCKET_DONE_ACK`], sends [`Msg::StBucketDone`] once a
/// bucket's chunks are exhausted, including buckets left empty locally, and
/// folds any [`Msg::StBucketAck`] read meanwhile into `mesh` via
/// [`MeshInner::record_bucket_ack`]. A missing ack or an older peer is not
/// an error: release then falls back to the timer/AE-round path.
#[allow(
    clippy::too_many_arguments,
    reason = "the bucket-done/ack wiring needs the serving peer's identity and protocol alongside every parameter serve_st_buckets already took"
)]
#[expect(
    clippy::too_many_lines,
    reason = "one connection's whole chunk-send/ack-read loop plus its tail drain reads best kept together"
)]
async fn serve_st_buckets(
    framed: &mut PeerFramed,
    cache: SmolStr,
    buckets: Vec<u16>,
    view_hash: u64,
    handler: &dyn RequestHandler,
    mesh: &MeshInner,
    from: NodeId,
    peer_protocol: u16,
    cancel: &CancellationToken,
) -> bool {
    let requested = buckets.len();
    let (mut chunks, mut remaining, parts_requested) =
        match handler.st_serve(cache.clone(), buckets, view_hash).await {
            super::StServe::Stale {
                responder_view_hash,
            } => {
                tracing::debug!(
                    cache = %cache,
                    requester_view_hash = view_hash,
                    responder_view_hash,
                    "st buckets pull declined: view hash mismatch"
                );
                return send_or_cancelled(
                    framed,
                    &Msg::StaleView {
                        cache,
                        responder_view_hash,
                    },
                    cancel,
                )
                .await;
            }
            super::StServe::Cold => {
                // Owned but not yet pulled: not a source. The requester tries
                // its next donor.
                tracing::debug!(
                    cache = %cache,
                    requested,
                    "st buckets pull declined: requested buckets not yet warm locally"
                );
                return send_or_cancelled(framed, &Msg::StUnavailable { cache }, cancel).await;
            }
            super::StServe::Stream {
                chunks,
                order,
                parts,
            } => (chunks, VecDeque::from(order), parts),
        };
    let signal_done = wire::peer_supports(peer_protocol, wire::PROTOCOL_ST_BUCKET_DONE_ACK);
    let mut chunks_sent: usize = 0;
    let mut records_sent: usize = 0;
    loop {
        let next = tokio::select! {
            biased;
            () = cancel.cancelled() => return true,
            ack = recv_msg(framed), if signal_done => {
                match ack {
                    Some(Ok(Msg::StBucketAck {
                        cache: ack_cache,
                        bucket,
                        view_hash,
                    })) => {
                        if ack_cache == cache {
                            mesh.record_bucket_ack(ack_cache, bucket, from, view_hash);
                        }
                        continue;
                    }
                    // An unrecognized message here is ignored, not fatal,
                    // like the reply-only arms in `dispatch_one`.
                    Some(Ok(_)) => continue,
                    Some(Err(ref error)) => {
                        tracing::debug!(
                            cache = %cache,
                            %error,
                            chunks_sent,
                            records_sent,
                            remaining = remaining.len(),
                            "st buckets pull ended early: requester sent an error"
                        );
                        return true;
                    }
                    None => {
                        tracing::debug!(
                            cache = %cache,
                            chunks_sent,
                            records_sent,
                            remaining = remaining.len(),
                            "st buckets pull ended early: connection closed"
                        );
                        return true;
                    }
                }
            }
            next = chunks.next() => next,
        };
        let Some((bucket, recs)) = next else {
            break;
        };
        if signal_done {
            // Every id ahead of this chunk's is finished: one flush for the
            // whole run, since a part-granular pull finishes thousands of
            // empty ids back to back.
            let mut done_run: Vec<u16> = Vec::new();
            while remaining.front().is_some_and(|&b| b != bucket) {
                done_run.push(
                    remaining
                        .pop_front()
                        .expect("invariant: the while guard just checked front() is Some"),
                );
            }
            if send_done_run(framed, &cache, done_run, mesh, from, cancel).await {
                tracing::debug!(
                    cache = %cache,
                    chunks_sent,
                    records_sent,
                    remaining = remaining.len(),
                    "st buckets pull ended early: connection closed sending StBucketDone"
                );
                return true;
            }
        }
        chunks_sent += 1;
        records_sent += recs.len();
        let msg = Msg::StBucketChunk {
            cache: cache.clone(),
            recs,
            done: false,
        };
        if send_or_cancelled(framed, &msg, cancel).await {
            tracing::debug!(
                cache = %cache,
                chunks_sent,
                records_sent,
                remaining = remaining.len(),
                "st buckets pull ended early: connection closed mid-stream"
            );
            return true;
        }
    }
    if signal_done {
        let done_run: Vec<u16> = remaining.drain(..).collect();
        let draining = done_run.len();
        if send_done_run(framed, &cache, done_run, mesh, from, cancel).await {
            tracing::debug!(
                cache = %cache,
                chunks_sent,
                records_sent,
                draining,
                "st buckets pull ended early: connection closed draining StBucketDone"
            );
            return true;
        }
    }
    // The donor-side counterpart of the requester's "in" direction.
    metrics::counter!(
        "sundog_rebalance_parts_total",
        "cache" => cache.to_string(),
        "direction" => "served"
    )
    .increment(parts_requested);
    tracing::debug!(
        cache = %cache,
        ids_served = requested,
        parts_served = parts_requested,
        chunks_sent,
        records_sent,
        signal_done,
        "st buckets pull stream ended"
    );
    send_or_cancelled(
        framed,
        &Msg::StBucketChunk {
            cache,
            recs: Vec::new(),
            done: true,
        },
        cancel,
    )
    .await
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
    let parts: Vec<BucketPart> = parts
        .into_iter()
        .map(|(bucket, part)| BucketPart { bucket, part })
        .collect();
    let mut replies = part_replies(&cache, parts, handler).await;
    replies.push(Msg::ReqDone);
    send_batch_or_cancelled(framed, &replies, cancel).await
}

/// Each of `parts`' [`Msg::AePart`] listing, or [`Msg::AePartSketch`] once
/// the listing would outweigh one.
async fn part_replies(
    cache: &SmolStr,
    parts: Vec<BucketPart>,
    handler: &dyn RequestHandler,
) -> Vec<Msg> {
    if parts.is_empty() {
        return Vec::new();
    }
    let min_bucket = handler.ae_sketch_min_bucket();
    let sketch_cells = handler.ae_sketch_cells();
    handler
        .entries_for_parts(cache.clone(), parts)
        .await
        .into_iter()
        .map(|(BucketPart { bucket, part }, entries)| {
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
                    entries: key_versions_to_wire(entries),
                },
            }
        })
        .collect()
}

/// A responder's shape for one mismatched bucket or part: its listing, or
/// an IBLT sketch once the listing would outweigh one.
#[derive(Debug, PartialEq, Eq)]
enum ListingOrSketch {
    Listing(Vec<KeyVersion>),
    Sketch(Vec<crate::wire::Cell>),
}

/// The one crossover rule for [`serve_ae_digest`] and [`serve_ae_parts`]:
/// more than `min_bucket` entries answer with a `sketch_cells`-cell sketch
/// over their `(key_hash, version)` pairs, anything up to it with the
/// listing itself.
fn listing_or_sketch(
    entries: Vec<KeyVersion>,
    min_bucket: usize,
    sketch_cells: usize,
) -> ListingOrSketch {
    if entries.len() > min_bucket {
        let mut iblt = crate::cluster::sketch::Iblt::new(sketch_cells);
        for kv in &entries {
            iblt.insert(xxhash_rust::xxh3::xxh3_64(&kv.key), kv.version);
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
                entries: key_versions_to_wire(entries),
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

/// A checked-out or dialed connection: `framed`, its `pool`, and any
/// reply `acquire_conn` already peeked off a reused connection.
pub(super) type Reply = (PeerFramed, Arc<ReqPool>, Option<Result<Msg, CodecError>>);

/// Reads `reply` until [`Msg::ReqDone`], folding each message into `seed`
/// via `accumulate`. Checks in on success; drops the connection on error.
async fn collect_replies<T>(
    reply: Reply,
    what: &str,
    mut seed: T,
    mut accumulate: impl FnMut(Msg, &mut T),
) -> Result<T, CodecError> {
    let (mut framed, pool, mut first) = reply;
    loop {
        let received = match first.take() {
            Some(msg) => Some(msg),
            None => recv_msg(&mut framed).await,
        };
        match received {
            Some(Ok(Msg::ReqDone)) => {
                pool.checkin(framed);
                return Ok(seed);
            }
            Some(Ok(msg)) => accumulate(msg, &mut seed),
            Some(Err(err)) => return Err(err),
            None => return Err(unexpected_close(what)),
        }
    }
}

/// Reads `AeBucket` replies into a `(bucket, entries)` list.
pub(super) async fn collect_ae_buckets(
    reply: Reply,
) -> Result<crate::store::BucketEntries, CodecError> {
    collect_replies(reply, "digest reply", Vec::new(), |msg, out| {
        if let Msg::AeBucket {
            bucket, entries, ..
        } = msg
        {
            out.push((bucket, wire_to_key_versions(entries)));
        }
    })
    .await
}

/// Reads digest replies into one [`AeMismatch`] list, per [`push_mismatch`].
pub(super) async fn collect_ae_mismatches(reply: Reply) -> Result<Vec<AeMismatch>, CodecError> {
    collect_replies(reply, "digest reply", Vec::new(), push_mismatch).await
}

fn push_mismatch(msg: Msg, out: &mut Vec<AeMismatch>) {
    match msg {
        Msg::AeBucket {
            bucket, entries, ..
        } => out.push(AeMismatch::Bucket(bucket, wire_to_key_versions(entries))),
        Msg::AeSketch { bucket, cells, .. } => out.push(AeMismatch::Sketch(bucket, cells)),
        Msg::AePartDigests {
            bucket, digests, ..
        } => out.push(AeMismatch::PartDigests(bucket, digests)),
        _ => {}
    }
}

/// Reads part replies into one [`AePartReply`] list.
pub(super) async fn collect_ae_part_replies(reply: Reply) -> Result<Vec<AePartReply>, CodecError> {
    collect_replies(reply, "part reply", Vec::new(), |msg, out| match msg {
        Msg::AePart {
            bucket,
            part,
            entries,
            ..
        } => out.push(AePartReply::Listing {
            bucket,
            part,
            entries: wire_to_key_versions(entries),
        }),
        Msg::AePartSketch {
            bucket,
            part,
            cells,
            ..
        } => out.push(AePartReply::Sketch {
            bucket,
            part,
            cells,
        }),
        _ => {}
    })
    .await
}

/// Reads `Replicate`/`ReplicateBatch` replies into one record list.
pub(super) async fn collect_pulled_records(
    reply: Reply,
) -> Result<Vec<crate::wire::WireRecord>, CodecError> {
    collect_replies(reply, "pull reply", Vec::new(), |msg, out| match msg {
        Msg::Replicate { rec, .. } => out.push(rec),
        Msg::ReplicateBatch { recs, .. } => out.extend(recs),
        _ => {}
    })
    .await
}

/// Reads a `FetchReply`/`FetchDeclined`/`StaleView` reply; a later one replaces an earlier one, since only a misbehaving peer sends more than one.
pub(super) async fn collect_fetch_reply(reply: Reply) -> Result<FetchOutcome, CodecError> {
    let outcome = collect_replies(reply, "fetch reply", None, |msg, out| {
        *out = match msg {
            Msg::FetchReply { rec } => Some(FetchOutcome::Found(rec)),
            Msg::FetchDeclined { .. } => Some(FetchOutcome::Declined),
            Msg::StaleView {
                responder_view_hash,
                ..
            } => Some(FetchOutcome::Stale {
                responder_view_hash,
            }),
            _ => return,
        };
    })
    .await?;
    outcome.ok_or_else(|| unexpected_close("fetch reply"))
}

/// Reads a `Load`'s replies: every `Loaded` message's records gathered, or
/// the responder's `LoadFailed` or `LoadDeclined`.
pub(super) async fn collect_load_reply(reply: Reply) -> Result<LoadOutcome, CodecError> {
    let outcome = collect_replies(reply, "load reply", None, |msg, out| match msg {
        Msg::Loaded { found, uncached } => match out {
            Some(LoadOutcome::Loaded {
                found: all_found,
                uncached: all_uncached,
            }) => {
                all_found.extend(found);
                all_uncached.extend(uncached);
            }
            _ => *out = Some(LoadOutcome::Loaded { found, uncached }),
        },
        Msg::LoadFailed { message, .. } => *out = Some(LoadOutcome::Failed(message)),
        Msg::LoadDeclined { .. } => *out = Some(LoadOutcome::Declined),
        _ => {}
    })
    .await?;
    outcome.ok_or_else(|| unexpected_close("load reply"))
}

/// Reads digest replies like [`collect_ae_mismatches`], but epoch-checked: the last `StaleView` seen wins over every mismatch.
pub(super) async fn collect_ae_round_scoped(reply: Reply) -> Result<AeRoundOutcome, CodecError> {
    type Seed = (Vec<AeMismatch>, Option<u64>);
    let accumulate = |msg, (out, stale): &mut Seed| match msg {
        Msg::StaleView {
            responder_view_hash,
            ..
        } => *stale = Some(responder_view_hash),
        other => push_mismatch(other, out),
    };
    let (mismatches, stale) =
        collect_replies(reply, "scoped digest reply", Seed::default(), accumulate).await?;
    Ok(match stale {
        Some(responder_view_hash) => AeRoundOutcome::Stale {
            responder_view_hash,
        },
        None => AeRoundOutcome::Mismatches(mismatches),
    })
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

/// How many [`Msg::StBucketAck`]s [`bucket_stream`] queues before a flush.
const ACK_FLUSH: usize = 256;

/// Adapts a rebalance bucket-pull connection into a lazy stream of
/// [`BucketStreamItem`]s: a `Chunk` for every [`Msg::StBucketChunk`], plus,
/// against a donor speaking at least [`wire::PROTOCOL_ST_BUCKET_DONE_ACK`],
/// a `BucketDone` for every [`Msg::StBucketDone`]. An older donor never
/// sends `BucketDone`, so this then yields only `Chunk`s, as before.
///
/// After yielding a `BucketDone`, the next poll queues a `Msg::StBucketAck`
/// for it (gated the same way), so the ack follows whatever the caller did
/// with that item. Queued acks flush every [`ACK_FLUSH`], before the next
/// chunk, and at the end. A caller that stops polling never sends the rest,
/// which [`serve_st_buckets`] tolerates.
pub(super) fn bucket_stream(
    framed: PeerFramed,
    pool: Arc<ReqPool>,
    first: Option<Result<Msg, CodecError>>,
    cache: SmolStr,
    donor_protocol: u16,
    view_hash: u64,
) -> futures::stream::BoxStream<'static, Result<BucketStreamItem, CodecError>> {
    let ack_gated = wire::peer_supports(donor_protocol, wire::PROTOCOL_ST_BUCKET_DONE_ACK);
    Box::pin(futures::stream::unfold(
        Some((framed, first, None::<u16>, 0usize)),
        move |state| {
            let pool = Arc::clone(&pool);
            let cache = cache.clone();
            async move {
                let (mut framed, mut pending, pending_ack, mut unflushed) = state?;
                if ack_gated && let Some(bucket) = pending_ack {
                    // Best-effort: a broken send surfaces on the read below.
                    // `view_hash` is this node's view when it requested the
                    // buckets; the donor drops the ack once its own view
                    // has moved past it. Buffered, and flushed every
                    // `ACK_FLUSH` acks, at the next chunk, or at the end.
                    let _ = feed_msg(
                        &mut framed,
                        &Msg::StBucketAck {
                            cache: cache.clone(),
                            bucket,
                            view_hash,
                        },
                    )
                    .await;
                    unflushed += 1;
                    if unflushed >= ACK_FLUSH {
                        let _ = framed.flush().await;
                        unflushed = 0;
                    }
                }
                loop {
                    let received = match pending.take() {
                        Some(first) => Some(first),
                        None => recv_msg(&mut framed).await,
                    };
                    if unflushed > 0 && !matches!(received, Some(Ok(Msg::StBucketDone { .. }))) {
                        let _ = framed.flush().await;
                        unflushed = 0;
                    }
                    match received {
                        Some(Ok(Msg::StBucketChunk { recs, done, .. })) => {
                            if recs.is_empty() {
                                if done {
                                    pool.checkin(framed);
                                    return None;
                                }
                                continue;
                            }
                            if done {
                                pool.checkin(framed);
                                return Some((Ok(BucketStreamItem::Chunk(recs)), None));
                            }
                            return Some((
                                Ok(BucketStreamItem::Chunk(recs)),
                                Some((framed, None, None, 0)),
                            ));
                        }
                        Some(Ok(Msg::StBucketDone { bucket, .. })) => {
                            return Some((
                                Ok(BucketStreamItem::BucketDone(bucket)),
                                Some((framed, None, Some(bucket), unflushed)),
                            ));
                        }
                        Some(Ok(_)) => {} // unexpected message on this stream; keep reading
                        Some(Err(err)) => return Some((Err(err), None)),
                        None => {
                            let err = CodecError::Io(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "rebalance bucket-transfer connection closed before the final chunk",
                            ));
                            return Some((Err(err), None));
                        }
                    }
                }
            }
        },
    ))
}

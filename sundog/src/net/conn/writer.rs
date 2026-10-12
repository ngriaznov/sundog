//! Dial, the persistent per-peer writer and the replicate coalescer.

#[cfg(doc)]
use crate::wire::MAX_FRAME;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use smol_str::SmolStr;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::framing::{
    PeerFramed, disable_nagle, establish_dial, new_framed, send_batch, send_frames, send_msg,
};
use crate::error::CodecError;
use crate::net::outbox::DropOldestQueue;
use crate::net::tcp::TcpStream;
use crate::net::{Greeting, OutFrame, TlsCtx};
use crate::wire::{self, Msg};

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
pub(super) fn coalesce_replicate(drained: Vec<OutFrame>) -> Vec<Bytes> {
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
pub(in crate::net) async fn dial_with_hello_and(
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
pub(in crate::net) async fn run_peer_writer(
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

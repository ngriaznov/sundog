//! Dial, the persistent per-peer writer and the replicate coalescer.

#[cfg(doc)]
use crate::wire::MAX_FRAME;

use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::{Sink, Stream, StreamExt as _};
use smol_str::SmolStr;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::framing::{
    PeerFramed, establish_dial, new_framed, send_batch, send_frames, send_msg, tune_socket,
};
use crate::error::CodecError;
use crate::net::outbox::DropOldestQueue;
use crate::net::tcp::TcpStream;
use crate::net::{Greeting, OutFrame, TlsCtx};
use crate::node::NodeId;
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
/// [`coalesce_replicate_counted`] calls both mid-run and at the end of
/// `drained`. A batch that fails to encode is dropped with a warning rather
/// than panicking, and its records are added to `unencoded` so the writer
/// marks the peer for anti-entropy.
fn flush_pending_replicate(
    pending: &mut Option<PendingRun>,
    out: &mut Vec<Bytes>,
    unencoded: &mut usize,
) {
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
    let records = run.records;
    let mut recs = Vec::with_capacity(records);
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
            *unencoded += records;
        }
    }
}

/// Coalesces consecutive same-cache `Msg::Replicate`/`Msg::ReplicateBatch`
/// entries in `drained` into `Msg::ReplicateBatch` frames bounded by
/// [`REPLICATE_BATCH_BUDGET`] and [`REPLICATE_BATCH_COUNT`], counting
/// records rather than queue items. A run of exactly one item reuses its
/// already-encoded [`OutFrame::frame`] as-is; only a real merge re-encodes.
/// Also answers how many records a failed re-encode dropped.
fn coalesce_replicate_counted(drained: Vec<OutFrame>) -> (Vec<Bytes>, usize) {
    let mut out = Vec::with_capacity(drained.len());
    let mut unencoded = 0usize;
    let mut pending: Option<PendingRun> = None;

    for item in drained {
        let (cache, item_records) = match &item.msg {
            Msg::Replicate { cache, .. } => (cache, 1),
            Msg::ReplicateBatch { cache, recs } => (cache, recs.len()),
            _ => {
                flush_pending_replicate(&mut pending, &mut out, &mut unencoded);
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
            flush_pending_replicate(&mut pending, &mut out, &mut unencoded);
            let cache = cache.clone();
            pending = Some(PendingRun {
                cache,
                items: vec![item],
                size: rec_size,
                records: item_records,
            });
        }
    }
    flush_pending_replicate(&mut pending, &mut out, &mut unencoded);
    (out, unencoded)
}

/// [`coalesce_replicate_counted`] without the count of dropped records.
#[cfg(test)]
pub(super) fn coalesce_replicate(drained: Vec<OutFrame>) -> Vec<Bytes> {
    coalesce_replicate_counted(drained).0
}

/// Dials `addr` on a fresh connection, layers TLS on per `tls`, and
/// completes the `Hello` handshake, for [`connect_with_hello`]'s persistent
/// per-peer writer. One-shot request/response paths want
/// [`dial_with_hello_and`] instead.
pub(super) async fn dial_with_hello(
    addr: SocketAddr,
    greeting: Greeting,
    tls: &TlsCtx,
    keepalive: Option<Duration>,
) -> Result<PeerFramed, CodecError> {
    let stream = TcpStream::connect(addr).await?;
    tune_socket(&stream, keepalive);
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
    keepalive: Option<Duration>,
    first: Msg,
) -> Result<PeerFramed, CodecError> {
    let stream = TcpStream::connect(addr).await?;
    tune_socket(&stream, keepalive);
    let stream = establish_dial(stream, tls).await?;
    let mut framed = new_framed(stream);
    send_batch(&mut framed, &[greeting.hello(), first]).await?;
    Ok(framed)
}

/// The wait between two dials of a peer, and between two attempts of one
/// dial.
pub(super) const RECONNECT_BACKOFF: Duration = Duration::from_millis(500);

/// The least time between two dirty marks one writer sets, so a peer that
/// flaps cannot make every anti-entropy round target it.
const DIRTY_MARK_SPACING: Duration = Duration::from_secs(5);

/// How long a written burst stays in [`RecentTail`], long enough to cover a
/// write in flight when the peer's FIN or RST arrives.
const RECENT_WINDOW: Duration = Duration::from_secs(1);

/// The bytes [`RecentTail`] keeps per peer at most.
const RECENT_BYTES: usize = 1024 * 1024;

/// How long a write already under way may finish after the writer is
/// cancelled: [`crate::net::Mesh::shutdown`] cancels a moment after the last
/// frame leaves its outbox, and that frame is still being written.
const CANCEL_SEND_GRACE: Duration = Duration::from_millis(250);

/// The wait before dialing again after a connection that lived `lived`: the
/// rest of [`RECONNECT_BACKOFF`], so a peer that accepts and closes at once
/// is dialed at most once per backoff.
pub(super) fn reconnect_delay(lived: Duration) -> Duration {
    RECONNECT_BACKOFF.saturating_sub(lived)
}

/// Whether a connection that ended in error or EOF leaves the peer owed an
/// anti-entropy round: frames wait in `held_nonempty`, or the connection
/// carried a frame (`sent_any`) and outlived [`RECONNECT_BACKOFF`]. A
/// connection shorter than that is a peer that accepts and closes (a
/// rejected `Hello`, a node that crashes in a loop), and a mark for it would
/// steer every round to that one peer.
pub(super) fn owes_repair(sent_any: bool, held_nonempty: bool, lived: Duration) -> bool {
    held_nonempty || (sent_any && lived >= RECONNECT_BACKOFF)
}

/// Whether a writer whose last dirty mark was at `last_mark` sets another
/// at `now`: at least [`DIRTY_MARK_SPACING`] apart.
pub(super) fn dirty_mark_due(last_mark: Option<Instant>, now: Instant) -> bool {
    last_mark.is_none_or(|at| now.saturating_duration_since(at) >= DIRTY_MARK_SPACING)
}

/// When a writer that `owes` a dirty mark sets it: now if no mark was set
/// yet, otherwise [`DIRTY_MARK_SPACING`] after `last_mark`. `None` when no
/// mark is owed. A connection that stays up applies the mark at this
/// instant, so a rate-limited mark is delayed, never skipped.
pub(super) fn mark_deadline(
    owes: bool,
    last_mark: Option<Instant>,
    now: Instant,
) -> Option<Instant> {
    owes.then(|| last_mark.map_or(now, |at| at + DIRTY_MARK_SPACING))
}

/// The frames written within [`RECENT_WINDOW`], at most [`RECENT_BYTES`] of
/// them. A write racing the peer's FIN or RST succeeds locally and is lost,
/// so the writer sends this tail again on the next connection.
#[derive(Default)]
pub(super) struct RecentTail {
    frames: VecDeque<(Instant, Bytes)>,
    bytes: usize,
}

impl RecentTail {
    /// Remembers `frames`, written at `now`.
    pub(super) fn record(&mut self, now: Instant, frames: &[Bytes]) {
        for frame in frames {
            self.bytes += frame.len();
            self.frames.push_back((now, frame.clone()));
        }
        self.trim(now);
    }

    /// Forgets the frames older than [`RECENT_WINDOW`], then the oldest
    /// until [`RECENT_BYTES`] hold.
    fn trim(&mut self, now: Instant) {
        while let Some((at, len)) = self.frames.front().map(|(at, frame)| (*at, frame.len())) {
            if now.saturating_duration_since(at) <= RECENT_WINDOW && self.bytes <= RECENT_BYTES {
                break;
            }
            self.bytes -= len;
            self.frames.pop_front();
        }
    }

    /// Removes and returns the frames still within the window at `now`, the
    /// oldest first.
    pub(super) fn take(&mut self, now: Instant) -> Vec<Bytes> {
        self.trim(now);
        self.bytes = 0;
        self.frames.drain(..).map(|(_, frame)| frame).collect()
    }

    /// How many frames are kept.
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.frames.len()
    }
}

/// How one connection of a writer ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LinkEnd {
    /// The writer was cancelled.
    Cancelled,
    /// The replicate sender dropped: the peer was removed.
    PeerGone,
    /// The link failed, hit EOF or carried a frame it must not: dial again.
    Closed,
}

/// How a write ended.
enum SendEnd {
    Sent,
    Failed,
    Cancelled,
}

/// What a writer keeps across its connections: the burst whose send failed,
/// the tail of what it wrote, whether this connection carried a frame, and
/// the dirty mark it owes.
pub(super) struct WriterState {
    peer: NodeId,
    dirty: Arc<AtomicBool>,
    /// Frames to send before anything else on the next write: a burst whose
    /// send failed, or the recent tail a closed link may not have delivered.
    pub(super) held: Vec<Bytes>,
    pub(super) recent: RecentTail,
    /// Whether the current connection carried a frame.
    pub(super) sent_any: bool,
    /// Whether a dirty mark is owed: a connection ended with loss to repair
    /// and the mark is not set yet.
    pub(super) owes_mark: bool,
    /// When the writer last marked the peer dirty.
    pub(super) last_mark: Option<Instant>,
}

impl WriterState {
    pub(super) fn new(peer: NodeId, dirty: Arc<AtomicBool>) -> Self {
        Self {
            peer,
            dirty,
            held: Vec::new(),
            recent: RecentTail::default(),
            sent_any: false,
            owes_mark: false,
            last_mark: None,
        }
    }

    /// Records that a connection that lived `lived` ended: a dirty mark is
    /// owed when it left loss to repair (see [`owes_repair`]).
    pub(super) fn note_connection_end(&mut self, lived: Duration) {
        self.owes_mark |= owes_repair(self.sent_any, !self.held.is_empty(), lived);
    }

    /// When the owed dirty mark is due, if one is owed.
    fn mark_deadline(&self, now: Instant) -> Option<Instant> {
        mark_deadline(self.owes_mark, self.last_mark, now)
    }

    /// Sets the owed dirty mark if the spacing since the last one allows it.
    pub(super) fn apply_mark_if_due(&mut self, now: Instant) {
        if self.owes_mark && dirty_mark_due(self.last_mark, now) {
            self.set_mark(now);
        }
    }

    /// Marks the peer dirty at `now` and clears the debt.
    fn set_mark(&mut self, now: Instant) {
        self.dirty.store(true, Ordering::Relaxed);
        self.last_mark = Some(now);
        self.owes_mark = false;
    }

    /// Marks the peer dirty and counts `frames` this writer dropped before
    /// they reached a link.
    fn note_dropped(&self, frames: usize) {
        if frames == 0 {
            return;
        }
        self.dirty.store(true, Ordering::Relaxed);
        metrics::counter!("sundog_backlog_dropped_total", "peer" => self.peer.to_string())
            .increment(u64::try_from(frames).unwrap_or(u64::MAX));
    }

    /// Puts the recent tail ahead of the held frames, for a connection that
    /// closed: the tail is older, and the peer may not have received it.
    pub(super) fn requeue_recent(&mut self, now: Instant) {
        let mut tail = self.recent.take(now);
        tail.append(&mut self.held);
        self.held = tail;
    }

    /// Sends `held`, clearing it and remembering it as recent on success.
    async fn flush_held<L>(
        &mut self,
        link: &mut L,
        cancel: &CancellationToken,
    ) -> Result<(), LinkEnd>
    where
        L: Sink<Bytes, Error = io::Error> + Unpin,
    {
        match send_unless_cancelled(link, self.held.iter().cloned(), cancel).await {
            SendEnd::Sent => {
                self.recent.record(Instant::now(), &self.held);
                self.held.clear();
                self.sent_any = true;
                Ok(())
            }
            SendEnd::Failed => Err(LinkEnd::Closed),
            SendEnd::Cancelled => Err(LinkEnd::Cancelled),
        }
    }

    /// Takes `burst` into `held` and sends it; a failed send leaves it there.
    async fn send_burst<L>(
        &mut self,
        link: &mut L,
        burst: Vec<Bytes>,
        cancel: &CancellationToken,
    ) -> Result<(), LinkEnd>
    where
        L: Sink<Bytes, Error = io::Error> + Unpin,
    {
        debug_assert!(
            self.held.is_empty(),
            "held frames are flushed before a burst"
        );
        self.held = burst;
        self.flush_held(link, cancel).await
    }
}

/// Writes `frames` to `link`, racing the write against `cancel`. A write
/// under way when `cancel` fires gets [`CANCEL_SEND_GRACE`] to finish.
async fn send_unless_cancelled<L>(
    link: &mut L,
    frames: impl IntoIterator<Item = Bytes>,
    cancel: &CancellationToken,
) -> SendEnd
where
    L: Sink<Bytes, Error = io::Error> + Unpin,
{
    let writing = send_frames(link, frames);
    tokio::pin!(writing);
    tokio::select! {
        biased;
        result = &mut writing => if result.is_ok() { SendEnd::Sent } else { SendEnd::Failed },
        () = cancel.cancelled() => {
            let _ = tokio::time::timeout(CANCEL_SEND_GRACE, writing).await;
            SendEnd::Cancelled
        }
    }
}

/// Drives one connection: sends the held frames first, then drains the two
/// outboxes in bursts until the link ends.
///
/// The link is read only to notice the peer closing: an acceptor never
/// writes on a broadcast link, so EOF, an error or any frame ends the
/// connection, and it is seen before a burst is drained into it. A burst
/// whose send fails stays in `state.held` for the next connection; the
/// receiver applies a resent frame idempotently. A dirty mark that the
/// spacing deferred is set on this connection once the spacing has passed.
pub(super) async fn drive_connection<L>(
    link: &mut L,
    state: &mut WriterState,
    invalidate: &DropOldestQueue<OutFrame>,
    replicate_rx: &mut mpsc::Receiver<OutFrame>,
    cancel: &CancellationToken,
) -> LinkEnd
where
    L: Stream<Item = io::Result<BytesMut>> + Sink<Bytes, Error = io::Error> + Unpin,
{
    if !state.held.is_empty()
        && let Err(end) = state.flush_held(link, cancel).await
    {
        return end;
    }
    loop {
        let mark_at = state.mark_deadline(Instant::now());
        tokio::select! {
            biased;
            () = cancel.cancelled() => return LinkEnd::Cancelled,
            () = tokio::time::sleep_until(mark_at.unwrap_or_else(Instant::now)),
                if mark_at.is_some() =>
            {
                state.set_mark(Instant::now());
            }
            inbound = link.next() => {
                match inbound {
                    Some(Ok(_)) => tracing::warn!(
                        peer = %state.peer,
                        "peer sent a frame on a broadcast link; closing it"
                    ),
                    Some(Err(error)) => tracing::debug!(
                        peer = %state.peer, %error, "broadcast link failed"
                    ),
                    None => tracing::debug!(peer = %state.peer, "peer closed the broadcast link"),
                }
                return LinkEnd::Closed;
            }
            item = invalidate.pop() => {
                let mut burst = vec![item.frame];
                while let Some(item) = invalidate.try_pop() {
                    burst.push(item.frame);
                }
                if let Err(end) = state.send_burst(link, burst, cancel).await {
                    return end;
                }
            }
            received = replicate_rx.recv() => {
                let Some(first) = received else { return LinkEnd::PeerGone };
                let mut drained = vec![first];
                while let Ok(item) = replicate_rx.try_recv() {
                    drained.push(item);
                }
                let (burst, unencoded) = coalesce_replicate_counted(drained);
                state.note_dropped(unencoded);
                if burst.is_empty() {
                    continue;
                }
                if let Err(end) = state.send_burst(link, burst, cancel).await {
                    return end;
                }
            }
        }
    }
}

/// The peer a writer serves and where it reports.
pub(in crate::net) struct PeerTarget {
    pub(in crate::net) peer: NodeId,
    pub(in crate::net) greeting: Greeting,
    pub(in crate::net) addr: SocketAddr,
    /// The peer's dirty flag, shared with its `PeerHandle`.
    pub(in crate::net) dirty: Arc<AtomicBool>,
    pub(in crate::net) tls: TlsCtx,
    /// `ClusterConfig::mesh_keepalive`, for every connection this writer dials.
    pub(in crate::net) keepalive: Option<Duration>,
}

/// The long-lived per-peer writer: connects with a fixed backoff, sends
/// `Hello`, then drains both broadcast-class outboxes until told to stop
/// or the connection breaks, in which case it reconnects.
///
/// A burst whose send fails stays with the writer and goes out first on the
/// next connection, and so does the tail of what the last second wrote (at
/// most 1 MiB), which a peer that closed the link may not have received (see
/// [`drive_connection`]). The peer is marked dirty once the redial succeeds,
/// when a connection that carried frames ended or frames were held, so the
/// next anti-entropy round targets it. That repairs `Replicated` and
/// owner-to-owner loss. A mark the spacing defers is set on the connection
/// itself when the spacing ends, however long that connection lives. A
/// `Distributed` non-owner's forwarded write has no other copy, and a
/// partition that never delivers a FIN can still lose it. Dials are at least
/// [`RECONNECT_BACKOFF`] apart, and the dirty marks at least
/// `DIRTY_MARK_SPACING`. Two connections per pair of peers is normal.
pub(in crate::net) async fn run_peer_writer(
    target: PeerTarget,
    invalidate: Arc<DropOldestQueue<OutFrame>>,
    mut replicate_rx: mpsc::Receiver<OutFrame>,
    cancel: CancellationToken,
) {
    let PeerTarget {
        peer,
        greeting,
        addr,
        dirty,
        tls,
        keepalive,
    } = target;
    let mut state = WriterState::new(peer, Arc::clone(&dirty));
    loop {
        let mut link = tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            connected = connect_with_hello(addr, greeting, &tls, keepalive, &cancel) => match connected {
                Some(link) => link,
                None => return, // cancelled while retrying
            },
        };
        let connected_at = Instant::now();
        state.apply_mark_if_due(connected_at);
        state.sent_any = false;
        let end = drive_connection(
            &mut link,
            &mut state,
            &invalidate,
            &mut replicate_rx,
            &cancel,
        )
        .await;
        if end != LinkEnd::Closed {
            return;
        }
        let lived = connected_at.elapsed();
        state.note_connection_end(lived);
        state.requeue_recent(Instant::now());
        tracing::debug!(
            %peer,
            held = state.held.len(),
            "broadcast link closed; redialing after the backoff"
        );
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            () = tokio::time::sleep(reconnect_delay(lived)) => {}
        }
    }
}

/// Retries `connect` + `Hello` on [`RECONNECT_BACKOFF`] until it succeeds
/// or `cancel` fires. Returns `None` only when cancelled mid-retry.
async fn connect_with_hello(
    addr: SocketAddr,
    greeting: Greeting,
    tls: &TlsCtx,
    keepalive: Option<Duration>,
    cancel: &CancellationToken,
) -> Option<PeerFramed> {
    loop {
        if let Ok(framed) = dial_with_hello(addr, greeting, tls, keepalive).await {
            return Some(framed);
        }
        tokio::select! {
            biased;
            () = cancel.cancelled() => return None,
            () = tokio::time::sleep(RECONNECT_BACKOFF) => {}
        }
    }
}

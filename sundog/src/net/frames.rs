//! Outbound frames, record batching and wire counters.

#[cfg(doc)]
use super::Mesh;

use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use smol_str::SmolStr;

use super::{REPLICATE_BATCH_BUDGET, REPLICATE_BATCH_COUNT};
use crate::error::CodecError;
use crate::wire::{self, Msg, WireRecord};

/// A queued outbound message paired with its already-encoded wire frame,
/// cheaply cloned into each peer's outbox so the writer never re-encodes
/// byte-identical content. `msg` stays alongside `frame` because
/// `Replicate`-class traffic coalesces consecutive queued entries by it.
#[derive(Debug, Clone)]
pub(crate) struct OutFrame {
    pub(crate) msg: Msg,
    pub(crate) frame: Bytes,
    /// [`OutFrame::resident_bytes`], counted once when the frame is built.
    resident: usize,
}

impl OutFrame {
    pub(crate) fn new(msg: Msg) -> Result<Self, CodecError> {
        let frame = wire::encode(&msg)?;
        let resident = resident_bytes(&msg, frame.len());
        Ok(Self {
            msg,
            frame,
            resident,
        })
    }

    /// The memory a queued copy of this frame holds: its encoded bytes, its
    /// decoded message, and each record the message carries, its struct and
    /// its key and value bytes. Shared bytes count once per frame. A record
    /// this node received and sends on is detached from the frame it came in
    /// first ([`WireRecord::detached`]), so its bytes are its own and the
    /// figure bounds what the frame keeps alive. For a run of small records
    /// the decoded message outweighs the encoded bytes several times over.
    /// Counted once, when the frame is built, since every send weighs it.
    pub(crate) const fn resident_bytes(&self) -> usize {
        self.resident
    }
}

/// [`OutFrame::resident_bytes`] of `msg` encoded in `encoded` bytes. Pure;
/// unit tested through [`OutFrame::resident_bytes`].
fn resident_bytes(msg: &Msg, encoded: usize) -> usize {
    let record = |rec: &WireRecord| {
        std::mem::size_of::<WireRecord>() + rec.key.len() + rec.value.as_ref().map_or(0, Bytes::len)
    };
    let decoded = match msg {
        Msg::Replicate { rec, .. } => record(rec),
        Msg::ReplicateBatch { recs, .. } | Msg::ForwardBatch { recs, .. } => {
            recs.iter().map(record).sum()
        }
        Msg::Invalidate { key, .. } => key.len(),
        _ => 0,
    };
    std::mem::size_of::<Msg>() + encoded + decoded
}

/// Process-wide wire-frame counters, incremented at [`super::conn::send_msg`], the
/// single choke point every outbound frame passes through:
/// [`frames_sent_total`] and [`bytes_sent_total`] sum every `Mesh` in this
/// process.
static FRAMES_SENT: AtomicU64 = AtomicU64::new(0);
static BYTES_SENT: AtomicU64 = AtomicU64::new(0);

/// Records one frame of `len` bytes written to the wire: bumps the
/// process-wide [`frames_sent_total`]/[`bytes_sent_total`] counters and
/// mirrors them to `metrics` for a live Prometheus scrape.
pub(super) fn record_frame_sent(len: usize) {
    FRAMES_SENT.fetch_add(1, Ordering::Relaxed);
    BYTES_SENT.fetch_add(len as u64, Ordering::Relaxed);
    metrics::counter!("sundog_frames_sent_total").increment(1);
    metrics::counter!("sundog_bytes_sent_total").increment(len as u64);
}

/// Splits a run of records into chunks by the same budget and count cap
/// `net::conn::writer`'s coalescer enforces
/// ([`REPLICATE_BATCH_BUDGET`]/[`REPLICATE_BATCH_COUNT`]), the shared core
/// of [`batch_replicate`] and [`batch_forward`].
fn chunk_records(cache_name: &SmolStr, records: Vec<WireRecord>) -> Vec<Vec<WireRecord>> {
    let mut chunks: Vec<Vec<WireRecord>> = Vec::new();
    let mut current: Vec<WireRecord> = Vec::new();
    let mut current_bytes = 0usize;
    for rec in records {
        let rec_bytes = wire::replicate_frame_len(
            cache_name.len(),
            rec.key.len(),
            rec.value.as_ref().map_or(0, Bytes::len),
        );
        if !current.is_empty()
            && (current.len() >= REPLICATE_BATCH_COUNT
                || current_bytes + rec_bytes > REPLICATE_BATCH_BUDGET)
        {
            chunks.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current_bytes += rec_bytes;
        current.push(rec);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

/// Splits a run of records into `Msg::ReplicateBatch` chunks by the same
/// budget and count cap `net::conn::writer`'s coalescer enforces
/// ([`REPLICATE_BATCH_BUDGET`]/[`REPLICATE_BATCH_COUNT`]). A chunk of
/// exactly one record stays a plain [`Msg::Replicate`].
pub(crate) fn batch_replicate(cache_name: &SmolStr, records: Vec<WireRecord>) -> Vec<Msg> {
    chunk_records(cache_name, records)
        .into_iter()
        .map(|mut recs| {
            if recs.len() == 1 {
                Msg::Replicate {
                    cache: cache_name.clone(),
                    rec: recs.pop().expect("invariant: length checked above"),
                }
            } else {
                Msg::ReplicateBatch {
                    cache: cache_name.clone(),
                    recs,
                }
            }
        })
        .collect()
}

/// [`batch_replicate`] for a `Mode::Distributed` fan-out: every chunk is a
/// [`Msg::ForwardBatch`] stamped with the sender's `view_hash` and `hops`,
/// a lone record included, so the receiver always sees the view the batch
/// routes under.
pub(crate) fn batch_forward(
    cache_name: &SmolStr,
    view_hash: u64,
    hops: u8,
    records: Vec<WireRecord>,
) -> Vec<Msg> {
    chunk_records(cache_name, records)
        .into_iter()
        .map(|recs| Msg::ForwardBatch {
            cache: cache_name.clone(),
            view_hash,
            hops,
            recs,
        })
        .collect()
}

/// Total wire frames sent by this process since start, across every
/// [`Mesh`] it has spawned. A cheap diagnostic for benchmarking replication
/// cost.
#[must_use]
pub fn frames_sent_total() -> u64 {
    FRAMES_SENT.load(Ordering::Relaxed)
}

/// Total wire-frame bytes sent by this process since start, across every
/// [`Mesh`] it has spawned. A cheap diagnostic for benchmarking replication
/// cost.
#[must_use]
pub fn bytes_sent_total() -> u64 {
    BYTES_SENT.load(Ordering::Relaxed)
}

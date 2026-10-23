//! The frame limit on the values of a [`Shard`]: the merges it refuses for
//! passing the limit and the records it leaves out of a sync for being past it.
//!
//! A record travels as one frame, at most [`wire::MAX_FRAME`] bytes, so a
//! value must stay within one. A write checks its own contribution against
//! the shard's `max_frame` ([`Shard::with_max_frame`]), as the replicate frame
//! it travels in. A merge builds a value larger than either side, so the apply
//! checks the merged result, as the widest frame a record takes, a forward:
//! it refuses one that passes the limit and is larger than the value it
//! replaces ([`merge_fits`](super::engine::merge_fits)). A result that is the
//! contribution itself, as over a lapsed value or under a resolver that keeps
//! the contribution, is counted as the replicate frame it was checked as or
//! arrived in. A refused merge
//! counts in `sundog_merge_refused_total{cache, reason="too_large"}`. A caller
//! gets [`CacheError::ValueTooLarge`]; a merge nobody waits on, from a peer or
//! out of a coalesce window, also logs at `warn`, at most once per
//! [`OVERSIZE_WARN_SPACING_MS`].
//!
//! A value larger than [`wire::MAX_FRAME`] can be in the store, which a merge
//! never builds. It keeps serving local reads and can only shrink;
//! anti-entropy and state transfer cannot ship it, so
//! [`Shard::skip_unframeable`] leaves it out of what they send, counted in
//! `sundog_sync_skipped_total{cache, reason="too_large"}` and logged at
//! `warn` under the same spacing. A value past a lowered `max_frame` that
//! fits a wire frame syncs as any other and cannot grow.

use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;
use smol_str::SmolStr;

use super::engine::MergeCap;
use super::{Shard, now_ms};
use crate::error::CacheError;
use crate::wire::{self, WireRecord};

/// The least time between two warnings of one kind about values past the
/// frame limit, per cache.
pub(super) const OVERSIZE_WARN_SPACING_MS: u64 = 60_000;

/// Whether a warning is due at `now_ms`, given the instant of the last one of
/// its kind (`None` before the first). A clock that stepped back makes the
/// next one due. Pure; unit tested directly.
pub(super) fn oversize_warn_due(last_warn_ms: Option<u64>, now_ms: u64) -> bool {
    last_warn_ms.is_none_or(|last| now_ms < last || now_ms - last >= OVERSIZE_WARN_SPACING_MS)
}

/// Whether the caller is the one to warn at `now`: a warning is due since the
/// instant in `last` (`u64::MAX` before the first), and no other caller has
/// claimed it first. Claims the slot, so one warning goes out per spacing
/// however many callers race for it.
fn claim_warning(last: &AtomicU64, now: u64) -> bool {
    let seen = last.load(Ordering::Relaxed);
    let seen = (seen != u64::MAX).then_some(seen);
    oversize_warn_due(seen, now)
        && last
            .compare_exchange(
                seen.unwrap_or(u64::MAX),
                now,
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_ok()
}

/// Whose merge was refused, when no caller gets the error: it names the source
/// in the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Refused {
    /// A record a peer sent.
    Peer,
    /// A coalesced fold flushing on its own, whose callers returned long ago.
    Fold,
    /// A CRDT compaction pass, whose rewrite of a record would grow it.
    Compaction,
}

impl Refused {
    /// What the log line names as the merge's source.
    const fn source(self) -> &'static str {
        match self {
            Self::Peer => "a record from a peer",
            Self::Fold => "a coalesced fold",
            Self::Compaction => "a compaction pass",
        }
    }
}

/// The longest key a log line shows, in bytes.
const LOGGED_KEY_BYTES: usize = 48;

/// `key`'s bytes in hex for a log line, cut at [`LOGGED_KEY_BYTES`] with a
/// trailing `..` when it is longer. A key's postcard bytes are what the wire
/// and the digests know it by; a string or integer key is readable in them.
/// Pure; unit tested directly.
pub(super) fn key_for_log(key: &[u8]) -> String {
    use std::fmt::Write as _;
    let shown = &key[..key.len().min(LOGGED_KEY_BYTES)];
    let mut out = String::with_capacity(shown.len() * 2 + 2);
    for byte in shown {
        let _ = write!(out, "{byte:02x}");
    }
    if shown.len() < key.len() {
        out.push_str("..");
    }
    out
}

/// The records of `records` that fit one frame under a cache name of
/// `cache_len` bytes, and the frame length and key of the largest one left
/// out, if any. A record that does not fit cannot be encoded as a `Replicate`, a
/// `ReplicateBatch` or a state-transfer chunk, so a sender that included it
/// would fail the whole exchange. Order is kept. Pure; unit tested directly.
pub(super) fn split_unframeable(
    cache_len: usize,
    mut records: Vec<WireRecord>,
) -> (Vec<WireRecord>, Option<(usize, Bytes)>) {
    let mut largest: Option<(usize, Bytes)> = None;
    records.retain(|rec| {
        let len = wire::replicate_frame_len(
            cache_len,
            rec.key.len(),
            rec.value.as_ref().map_or(0, Bytes::len),
        );
        let fits = len <= wire::MAX_FRAME;
        if !fits && largest.as_ref().is_none_or(|(held, _)| len > *held) {
            largest = Some((len, rec.key.clone()));
        }
        fits
    });
    (records, largest)
}

impl<K, V> Shard<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// The frame limit a merge keeps a value within.
    pub(super) fn merge_cap(&self) -> MergeCap {
        MergeCap {
            cache_len: self.name.len(),
            limit: self.max_frame,
        }
    }

    /// The error a merge gets back when its result would be `size` value
    /// bytes, past the frame limit.
    pub(super) fn merge_too_large(&self, size: usize) -> CacheError {
        CacheError::ValueTooLarge {
            cache: self.name.clone(),
            size,
            limit: self.max_frame,
        }
    }

    /// Counts one merge refused for passing the frame limit in
    /// `sundog_merge_refused_total{cache, reason="too_large"}`.
    pub(super) fn count_merge_refused(&self) {
        metrics::counter!(
            "sundog_merge_refused_total",
            "cache" => self.name.to_string(),
            "reason" => "too_large"
        )
        .increment(1);
    }

    /// Logs a change `from` refused for leaving the value `size` bytes long,
    /// at `warn` when the last warning is old enough, else at `debug`. Nobody
    /// gets the error, so the stored value stays as it is and the change is
    /// lost.
    pub(super) fn warn_merge_refused(&self, key: &[u8], size: usize, from: Refused) {
        let limit = self.max_frame;
        let key = key_for_log(key);
        if claim_warning(&self.merge_refused_warned_ms, self.now_ms()) {
            tracing::warn!(
                cache = %self.name,
                source = from.source(),
                key,
                size,
                limit,
                "refused a change that would leave the value past the frame limit: the stored \
                 value stays as it is and the change is lost. A value that keeps growing needs \
                 a bound of its own. sundog_merge_refused_total counts every one"
            );
        } else {
            tracing::debug!(
                cache = %self.name,
                source = from.source(),
                key,
                size,
                limit,
                "refused a change that would leave the value past the frame limit"
            );
        }
    }
}

/// What a shard counts and logs the records a sync leaves out with: cheap to
/// clone, so a `'static` stream of snapshot chunks can hold one.
#[derive(Clone)]
pub(super) struct SyncSkips {
    cache: SmolStr,
    /// The instant of the last warning, `u64::MAX` before the first.
    last_warn_ms: Arc<AtomicU64>,
}

impl SyncSkips {
    pub(super) fn new(cache: SmolStr) -> Self {
        Self {
            cache,
            last_warn_ms: Arc::new(AtomicU64::new(u64::MAX)),
        }
    }

    /// Drops from `records`, a sync's outbound set, every record that cannot
    /// travel in one frame, counting each in
    /// `sundog_sync_skipped_total{cache, reason="too_large"}` and logging at
    /// `warn`, rate limited. The record stays in the store and keeps serving
    /// local reads; only the sync leaves it out, and the rest of the exchange
    /// goes on.
    pub(super) fn skip(&self, records: Vec<WireRecord>) -> Vec<WireRecord> {
        let total = records.len();
        let (kept, largest) = split_unframeable(self.cache.len(), records);
        let Some((largest, key)) = largest else {
            return kept;
        };
        let key = key_for_log(&key);
        let skipped = total - kept.len();
        metrics::counter!(
            "sundog_sync_skipped_total",
            "cache" => self.cache.to_string(),
            "reason" => "too_large"
        )
        .increment(u64::try_from(skipped).unwrap_or(u64::MAX));
        if claim_warning(&self.last_warn_ms, now_ms()) {
            tracing::warn!(
                cache = %self.cache,
                skipped,
                largest_key = key,
                largest_frame = largest,
                limit = wire::MAX_FRAME,
                "left records out of a sync because each is larger than one frame: they stay \
                 here and are not replicated. A merge never builds a value this large. Shrink \
                 or remove them. sundog_sync_skipped_total counts every one"
            );
        } else {
            tracing::debug!(
                cache = %self.cache,
                skipped,
                largest_key = key,
                largest_frame = largest,
                "left records out of a sync because each is larger than one frame"
            );
        }
        kept
    }
}

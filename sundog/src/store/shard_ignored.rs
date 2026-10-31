//! The tombstones a [`Shard`] ignores as lapsed: how they are counted and when
//! the loss of a forwarded one is logged.
//!
//! A tombstone from a peer that arrives already past its retention is stored
//! nowhere and deletes nothing (see
//! `engine::expiry::drops_lapsed_tombstone`). Each one counts in
//! `sundog_tombstones_ignored_total{cache}`. A forwarded delete that arrives
//! lapsed is the only copy of a non-owner's write, so it also logs a warning,
//! at most once per [`IGNORED_FORWARD_WARN_SPACING_MS`].

use std::hash::Hash;
use std::sync::atomic::Ordering;

use serde::Serialize;
use serde::de::DeserializeOwned;

use super::Shard;

/// The least time between two warnings about forwarded tombstones ignored as
/// lapsed.
pub(super) const IGNORED_FORWARD_WARN_SPACING_MS: u64 = 60_000;

/// Whether a warning about forwarded tombstones ignored as lapsed is due at
/// `now_ms`, given the instant of the last one (`None` before the first).
/// A clock that stepped back makes the next one due. Pure; unit tested
/// directly.
pub(super) fn ignored_forward_warn_due(last_warn_ms: Option<u64>, now_ms: u64) -> bool {
    last_warn_ms
        .is_none_or(|last| now_ms < last || now_ms - last >= IGNORED_FORWARD_WARN_SPACING_MS)
}

impl<K, V> Shard<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// Counts `ignored` tombstones one inbound batch dropped as lapsed in
    /// `sundog_tombstones_ignored_total{cache}`. When the batch came by
    /// forward (`forwarded`), the delete is lost, since every node ignores a
    /// lapsed tombstone alike and a non-owner writer kept no copy: it is also
    /// logged at `warn`, rate limited.
    pub(super) fn note_ignored_tombstones(&self, ignored: u64, forwarded: bool) {
        if ignored == 0 {
            return;
        }
        metrics::counter!(
            "sundog_tombstones_ignored_total",
            "cache" => self.name.to_string()
        )
        .increment(ignored);
        if !forwarded {
            return;
        }
        let now = self.now_ms();
        let last = self.ignored_forward_warned_ms.load(Ordering::Relaxed);
        let last = (last != u64::MAX).then_some(last);
        if ignored_forward_warn_due(last, now)
            && self
                .ignored_forward_warned_ms
                .compare_exchange(
                    last.unwrap_or(u64::MAX),
                    now,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
        {
            tracing::warn!(
                cache = %self.name,
                ignored,
                "ignored forwarded deletes that arrived past tombstone_ttl from their stamp: every \
                 node ignores them alike, so those deletes are lost. A forward waits for outbox \
                 space while its owner is live; a writer clock more than tombstone_ttl behind \
                 this node's does the same. sundog_tombstones_ignored_total counts every one"
            );
        } else {
            tracing::debug!(cache = %self.name, ignored, "ignored forwarded deletes that arrived lapsed");
        }
    }
}

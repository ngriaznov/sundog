//! The skew hold of a [`Shard`]: a forwarded write an owner refused for clock
//! skew is parked until the owner's clock is within the bound of its stamp.
//! The data structure and the rules it decides by live in `skew_hold.rs`.
//!
//! The semantics are those of the `Mode::Replicated` refusal, delayed instead
//! of repeated: delivery waits until the owner's clock is within
//! `max_clock_skew` of the stamp, a writer whose clock runs `D` ahead reaches
//! its owners after about `D - max_clock_skew`, and correcting the writer's
//! clock afterwards does not shorten the wait, since the stamp is fixed at
//! write time. While a record is held the owner's clock does not absorb its
//! stamp, so a write or remove made meanwhile, here or on a node with a
//! correct clock, stamps below the held record and loses to it when it
//! releases, as a replicated write loses to a fast writer's.

use std::future::Future;
use std::hash::Hash;
use std::sync::PoisonError;
use std::sync::atomic::Ordering;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;

use super::skew_hold::{
    HoldOutcome, SkewHold, beyond_horizon, should_hold, skew_due_ms, skew_hold_horizon_ms,
    skew_hold_wait,
};
use super::{FanOutItem, PartId, Shard};
use crate::wire::WireRecord;

/// Why a forwarded record refused for skew was not held, the `reason` label
/// of `sundog_clock_skew_hold_dropped_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DropReason {
    /// The record's wait is longer than the hold's horizon.
    Horizon,
    /// The hold is at its record or byte cap.
    Full,
}

impl DropReason {
    pub(super) const fn label(self) -> &'static str {
        match self {
            Self::Horizon => "horizon",
            Self::Full => "full",
        }
    }
}

/// `len` as a gauge value.
#[expect(
    clippy::cast_precision_loss,
    reason = "a gauge only needs f64's exact-integer range, up to 2^53, far above the hold's caps"
)]
fn gauge_value(len: usize) -> f64 {
    len as f64
}

impl<K, V> Shard<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// Whether this shard can hold a forwarded record refused for skew: it
    /// has an ownership view, so it receives forwards, and a skew bound to
    /// refuse them by. Gates `cache::tasks`' release task.
    pub(crate) fn holds_skewed_forwards(&self) -> bool {
        self.max_clock_skew_ms.is_some() && self.ownership.is_some()
    }

    /// Parks `rec`, which [`Shard::observe_remote`] refused as stamped past
    /// `max_clock_skew`, when it came in a forward from a writer that is not
    /// an owner of its part under the current view; see [`should_hold`]. A
    /// record whose wait would pass the horizon, or that the hold's caps
    /// refuse, is dropped and counted in
    /// `sundog_clock_skew_hold_dropped_total{cache,reason}`.
    pub(super) fn hold_skewed(&self, rec: WireRecord) {
        let (Some(max_skew_ms), Some(tracker)) = (self.max_clock_skew_ms, self.ownership.as_ref())
        else {
            return;
        };
        let view = tracker.current();
        if !should_hold(
            true,
            view.owners_of(PartId::of_key(rec.key.as_ref())),
            rec.ver.node,
        ) {
            return;
        }
        let writer = rec.ver.node;
        let due_ms = skew_due_ms(rec.ver.wall_ms, max_skew_ms);
        if beyond_horizon(
            due_ms,
            self.now_ms(),
            skew_hold_horizon_ms(self.tombstone_max_ttl_ms),
        ) {
            self.note_skew_drop(DropReason::Horizon, writer);
            return;
        }
        let outcome = {
            let mut hold = self
                .skew_hold
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let outcome = hold.hold(rec, max_skew_ms, self.resolver.merges());
            self.publish_skew_hold(&hold);
            outcome
        };
        match outcome {
            HoldOutcome::Held => self.skew_wake.notify_one(),
            HoldOutcome::Full => self.note_skew_drop(DropReason::Full, writer),
            HoldOutcome::Duplicate | HoldOutcome::Superseded | HoldOutcome::Closed => {
                tracing::debug!(cache = %self.name, %writer, ?outcome, "a refused forward was not held");
            }
        }
    }

    /// Counts a forwarded record dropped for `reason`, warning for the first.
    fn note_skew_drop(&self, reason: DropReason, writer: crate::node::NodeId) {
        metrics::counter!(
            "sundog_clock_skew_hold_dropped_total",
            "cache" => self.name.to_string(),
            "reason" => reason.label()
        )
        .increment(1);
        if self.skew_drop_warned.swap(true, Ordering::Relaxed) {
            tracing::debug!(cache = %self.name, %writer, reason = reason.label(), "dropped a forwarded write refused for clock skew");
        } else {
            tracing::warn!(
                cache = %self.name,
                %writer,
                reason = reason.label(),
                "dropped a forwarded write refused for clock skew instead of holding it: its stamp \
                 is past the hold's horizon, or the hold is full. The writer kept no copy, so the \
                 write is lost. sundog_clock_skew_hold_dropped_total counts every drop"
            );
        }
    }

    /// Publishes `sundog_clock_skew_held{cache}` and
    /// `sundog_clock_skew_held_bytes{cache}` from `hold`.
    fn publish_skew_hold(&self, hold: &SkewHold) {
        metrics::gauge!("sundog_clock_skew_held", "cache" => self.name.to_string())
            .set(gauge_value(hold.len()));
        metrics::gauge!("sundog_clock_skew_held_bytes", "cache" => self.name.to_string())
            .set(gauge_value(hold.bytes()));
    }

    /// Applies every held record whose clock bound has come due. A record
    /// for a part this node no longer owns goes to the fan-out as a forward,
    /// so the current owners get it; the rest go through the apply body that
    /// refused them, with the hold still on, so a clock that stepped back
    /// parks them again.
    pub(crate) async fn release_due_skewed(&self) {
        let owned = {
            let mut hold = self
                .skew_hold
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let due = hold.take_due(self.now_ms());
            if due.is_empty() {
                return;
            }
            self.publish_skew_hold(&hold);
            let Some(tracker) = self.ownership.as_ref() else {
                return;
            };
            let view = tracker.current();
            let (owned, unowned): (Vec<_>, Vec<_>) = due
                .into_iter()
                .partition(|rec| view.owns(PartId::of_key(rec.key.as_ref())));
            // Under the hold's lock, so a close that has taken the hold's
            // contents cannot interleave with this hand-off to the fan-out.
            self.redirect_to_owners(unowned, "a part this node no longer owns");
            owned
        };
        if !owned.is_empty() {
            self.apply_inbound(owned, true).await;
        }
    }

    /// How long the release task sleeps before it looks at the hold again;
    /// `None` with nothing held. See [`skew_hold_wait`].
    pub(crate) fn next_skew_wait(&self) -> Option<Duration> {
        let next_due_ms = self
            .skew_hold
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .next_due_ms();
        skew_hold_wait(next_due_ms, self.now_ms())
    }

    /// Resolves the next time a record is held, so the release task can
    /// recompute its sleep instead of parking past a due time that did not
    /// exist when it last checked.
    pub(crate) fn skew_wake_notified(&self) -> impl Future<Output = ()> + '_ {
        self.skew_wake.notified()
    }

    /// Hands every held record to the fan-out as a forward and closes the
    /// hold, for a cache that is closing: the other owners of each record's
    /// part take it over, applying it or holding it in turn. A node that is
    /// the only owner of a part has nobody to hand to, and its held records
    /// are lost, as they are in a crash. Run before the fan-out queue is
    /// sealed, so the final drain sends them.
    pub(crate) fn hand_off_skew_hold(&self) {
        let mut hold = self
            .skew_hold
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let handed = hold.close();
        self.publish_skew_hold(&hold);
        if handed.is_empty() {
            return;
        }
        tracing::debug!(cache = %self.name, handed = handed.len(), "handing held forwards to the fan-out on close");
        let _ = self
            .fan_out
            .extend(handed.into_iter().map(FanOutItem::Forward));
    }

    /// Stops accepting writes: applies every [`Shard::merge`] fold still
    /// pending in its coalescing window, hands the skew hold to the fan-out,
    /// then seals the queue for its final drain. The first step of a close or
    /// a cluster shutdown, so both share one flush-then-seal sequence. The
    /// folds apply first: a fold applied once the queue is sealed reaches no
    /// peer, and a `Mode::Distributed` fold for a key this node does not own
    /// could not be forwarded and would be lost.
    pub(crate) fn seal_for_close(&self) {
        self.flush_all_pending_merges();
        self.hand_off_skew_hold();
        self.fan_out.seal();
    }

    /// Test-only: how many records and bytes the hold keeps.
    #[cfg(test)]
    pub(crate) fn skew_held(&self) -> (usize, usize) {
        let hold = self
            .skew_hold
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        (hold.len(), hold.bytes())
    }
}

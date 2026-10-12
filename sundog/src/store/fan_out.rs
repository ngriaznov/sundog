//! The fan-out queue a shard pushes its local writes onto, and the item type
//! and pure predicates around it.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex as StdMutex, PoisonError};
use std::time::Duration;

use smol_str::SmolStr;

use super::Mode;
use crate::wire::WireRecord;

#[cfg(doc)]
use super::Shard;
#[cfg(doc)]
use crate::error::CacheError;

/// Keys this node wrote locally and has not yet fanned out to
/// `cluster::fan_out_task`. A write appends its key; a drain takes the whole
/// backlog at once, so no channel ever drops a write. Holds keys, not values:
/// `records_for_typed` re-fetches fresh wire bytes. A queue nothing drains, a
/// `Mode::Local` shard's or a closed cache's, accepts nothing.
pub(crate) struct FanOutQueue<K> {
    /// This cache's name, used as the `cache` label on the fan-out metrics.
    name: SmolStr,
    pending: StdMutex<Vec<K>>,
    notify: tokio::sync::Notify,
    /// Notified when [`FanOutQueue::drain`] empties a non-empty backlog, so
    /// [`FanOutQueue::wait_for_room`] waiters recheck instead of polling.
    drained: tokio::sync::Notify,
    /// Whether a push lands in `pending` at all: `false` for a
    /// `Mode::Local` shard, whose writes never fan out and are not errors.
    accepting: AtomicBool,
    /// Set by [`FanOutQueue::seal`] and [`FanOutQueue::close`]: the cache is
    /// closing, and a write from here on is refused with
    /// [`CacheError::Closed`] rather than accepted and never sent. Read and
    /// written under `pending`'s lock, so a push and a seal never
    /// interleave: every push that returns `true` lands before the seal's
    /// final drain.
    closed: StdMutex<bool>,
}

impl<K> FanOutQueue<K> {
    pub(super) fn new(name: SmolStr, accepting: bool) -> Self {
        Self {
            name,
            pending: StdMutex::new(Vec::new()),
            notify: tokio::sync::Notify::new(),
            drained: tokio::sync::Notify::new(),
            accepting: AtomicBool::new(accepting),
            closed: StdMutex::new(false),
        }
    }

    /// Publishes `sundog_fan_out_backlog{cache}` as `len`, the one place
    /// every mutation of `pending` reports its length through.
    fn publish_backlog(&self, len: usize) {
        metrics::gauge!("sundog_fan_out_backlog", "cache" => self.name.to_string())
            .set(fan_out_backlog_gauge_value(len));
    }

    /// Stops accepting writes but keeps the backlog for the fan-out task's
    /// final drain, and wakes that task: the first step of a close or a
    /// cluster shutdown, before the task is cancelled.
    pub(crate) fn seal(&self) {
        let _pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
        *self.closed.lock().unwrap_or_else(PoisonError::into_inner) = true;
        self.notify.notify_one();
    }

    /// Stops accepting keys and drops the backlog: nothing drains this queue
    /// any more.
    pub(crate) fn close(&self) {
        self.accepting.store(false, Ordering::Release);
        let mut pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
        *self.closed.lock().unwrap_or_else(PoisonError::into_inner) = true;
        pending.clear();
        self.publish_backlog(0);
        self.drained.notify_waiters();
    }

    /// Whether a write may still be accepted: `false` once sealed or
    /// closed.
    fn is_open(&self) -> bool {
        !*self.closed.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Queues `key`, or returns `false` once the queue is sealed or closed.
    /// A `Mode::Local` shard's queue accepts nothing and returns `true`.
    #[must_use]
    pub(super) fn push(&self, key: K) -> bool {
        let mut pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
        if !self.is_open() {
            return false;
        }
        if !self.accepting.load(Ordering::Acquire) {
            return true;
        }
        pending.push(key);
        self.publish_backlog(pending.len());
        self.notify.notify_one();
        true
    }

    /// [`FanOutQueue::push`] for many keys at once.
    #[must_use]
    pub(super) fn extend(&self, keys: impl IntoIterator<Item = K>) -> bool {
        let mut pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
        if !self.is_open() {
            return false;
        }
        if !self.accepting.load(Ordering::Acquire) {
            return true;
        }
        pending.extend(keys);
        self.publish_backlog(pending.len());
        self.notify.notify_one();
        true
    }

    /// Takes every pending key and wakes [`FanOutQueue::wait_for_room`]'s waiters.
    pub(crate) fn drain(&self) -> Vec<K> {
        let items =
            std::mem::take(&mut *self.pending.lock().unwrap_or_else(PoisonError::into_inner));
        self.publish_backlog(0);
        if !items.is_empty() {
            self.drained.notify_waiters();
        }
        items
    }

    /// Resolves once the queue holds at least one key. A push that lands before
    /// the wait stores a permit, so nothing is ever missed.
    pub(crate) async fn wait_nonempty(&self) {
        loop {
            if !self
                .pending
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_empty()
            {
                return;
            }
            self.notify.notified().await;
        }
    }

    /// Waits up to `timeout` for the backlog to fall under `capacity`, giving
    /// an async write path a way to apply its own backpressure. The write
    /// proceeds either way; on timeout,
    /// `sundog_fan_out_wait_timeouts_total{cache}` counts it. The synchronous
    /// write chain has no runtime to await on and never calls this.
    pub(crate) async fn wait_for_room(&self, capacity: usize, timeout: Duration) {
        if !fan_out_over_capacity(self.len(), capacity) {
            return;
        }
        let waited = tokio::time::timeout(timeout, async {
            loop {
                // Armed before the recheck so a `drain` landing between the
                // check and the `.await` is never missed.
                let notified = self.drained.notified();
                if !fan_out_over_capacity(self.len(), capacity) {
                    return;
                }
                notified.await;
            }
        })
        .await;
        if waited.is_err() {
            metrics::counter!(
                "sundog_fan_out_wait_timeouts_total",
                "cache" => self.name.to_string()
            )
            .increment(1);
        }
    }

    pub(super) fn len(&self) -> usize {
        self.pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}

/// Whether [`FanOutQueue::wait_for_room`] keeps waiting: `true` while
/// `pending_len` is at or above `capacity`.
pub(super) fn fan_out_over_capacity(pending_len: usize, capacity: usize) -> bool {
    pending_len >= capacity
}

/// `len` as `sundog_fan_out_backlog`'s gauge value.
#[expect(
    clippy::cast_precision_loss,
    reason = "a gauge only needs f64's exact-integer range, up to 2^53, which comfortably \
              covers any realistic fan-out backlog"
)]
fn fan_out_backlog_gauge_value(len: usize) -> f64 {
    len as f64
}

/// One entry in a shard's fan-out queue. `Applied` is every write this
/// shard holds: every mode's own local write, and an owner's write under
/// `Mode::Distributed`. `cluster::fan_out_batch` re-fetches its current
/// record via `Shard::records_for_typed`, unchanged from every other mode.
/// `Forward` is a `Mode::Distributed` non-owner write: the record is never
/// applied, so there is nothing to re-fetch, and the
/// fully built [`WireRecord`] travels with the queue entry itself. One
/// queue, one drain, for every mode: this generalizes [`FanOutQueue`]'s
/// item type rather than adding a second queue and a second task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FanOutItem<K> {
    Applied(K),
    Forward(WireRecord),
}

/// Sim-only mirror of [`FanOutItem`], reachable from `tests/sim/` under
/// the `sim` feature since `FanOutItem` itself is not; see
/// [`Shard::drain_fan_out_for_sim`].
#[cfg(feature = "sim")]
#[doc(hidden)]
pub enum SimFanOut<K> {
    /// Mirrors [`FanOutItem::Applied`].
    Applied(K),
    /// Mirrors [`FanOutItem::Forward`].
    Forward(WireRecord),
}

/// Whether a [`Shard::get_or_load`] fill goes out to peers. A fill copies
/// data from its source and changes nothing there. `Replicated` and
/// `Distributed` send it so a peer holding the key skips its own loader
/// call. `Invalidation` sends nothing: an invalidation would drop every
/// peer's copy of the same data, and two nodes reading one key would evict
/// each other on every fill. Pure; unit tested directly.
pub(super) const fn fill_fans_out(mode: Mode) -> bool {
    !matches!(mode, Mode::Invalidation)
}

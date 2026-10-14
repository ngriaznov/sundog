//! The fan-out queue a shard pushes its local writes onto, and the item type
//! and pure predicates around it.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex as StdMutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use smol_str::SmolStr;

use super::Mode;
use crate::hlc::Hlc;
use crate::wire::WireRecord;

#[cfg(doc)]
use super::Shard;
#[cfg(doc)]
use crate::error::CacheError;

/// Keys this node wrote locally, each with the version it was written at, and
/// not yet fanned out to `cluster::fan_out_task`. A write appends its key; a
/// drain takes the whole backlog at once, so no channel ever drops a write.
/// Holds keys and versions, not values: `Shard::fan_out_records` re-fetches
/// fresh wire bytes, and a write whose entry is gone by then is still
/// identified by its version. A queue nothing drains, a `Mode::Local`
/// shard's or a closed cache's, accepts nothing.
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
/// shard holds, with the version it was stored at: every mode's own local
/// write, and an owner's write under `Mode::Distributed`.
/// `cluster::fan_out_batch` re-fetches its current record through
/// `Shard::fan_out_records`, which reports a write whose entry is gone as
/// departed. `Forward` is a `Mode::Distributed` non-owner write: the record
/// is never applied, so there is nothing to re-fetch, and the fully built
/// [`WireRecord`] travels with the queue entry itself. One queue, one drain,
/// for every mode: this generalizes [`FanOutQueue`]'s item type rather than
/// adding a second queue and a second task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FanOutItem<K> {
    Applied(K, Hlc),
    Forward(WireRecord),
}

/// What a drained burst of applied writes sends: the `records` still stored
/// at (or past) the version written, and the `departed` writes whose entry
/// no longer carries that version.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct FanOutRecords {
    /// Records to replicate, forward or fold per mode.
    pub(crate) records: Vec<WireRecord>,
    /// One value-less carrier per departed write, holding the key and the
    /// version written. Only ever sent as an invalidation: never
    /// replicated, forwarded or folded.
    pub(crate) departed: Vec<WireRecord>,
}

/// `items` with one entry per key, in first-seen order, holding the highest
/// version queued for it. A burst that writes a key twice fans out its
/// newest write once. Pure; unit tested directly.
pub(crate) fn newest_per_key<K: Hash + Eq + Clone>(items: Vec<(K, Hlc)>) -> Vec<(K, Hlc)> {
    let mut slot: HashMap<K, usize> = HashMap::with_capacity(items.len());
    let mut out: Vec<(K, Hlc)> = Vec::new();
    for (key, ver) in items {
        if let Some(&at) = slot.get(&key) {
            out[at].1 = out[at].1.max(ver);
        } else {
            slot.insert(key.clone(), out.len());
            out.push((key, ver));
        }
    }
    out
}

/// Splits what the written view returned for a burst of `writes` into the
/// records to send and the writes that departed. A write departs when no
/// record came back for its key, or when the one that did is older than the
/// version written: the entry was evicted, swept, disowned or went idle, or
/// an older copy refilled the key. A departed write's older record is left
/// out of `records`, so the peers receive only the invalidation and never
/// reinstall, in either arrival order, the value it dropped. A record newer
/// than the write stays: it carries a later write's value. Pure; unit tested
/// directly.
pub(crate) fn partition_writes(writes: &[(Bytes, Hlc)], fetched: Vec<WireRecord>) -> FanOutRecords {
    let mut written: HashMap<&Bytes, Hlc> = HashMap::with_capacity(writes.len());
    for (key, ver) in writes {
        written
            .entry(key)
            .and_modify(|queued| *queued = (*queued).max(*ver))
            .or_insert(*ver);
    }
    let mut covered: HashMap<Bytes, Hlc> = HashMap::with_capacity(fetched.len());
    let mut records = Vec::with_capacity(fetched.len());
    for rec in fetched {
        if written
            .get(&rec.key)
            .is_some_and(|queued| rec.ver < *queued)
        {
            continue;
        }
        covered.insert(rec.key.clone(), rec.ver);
        records.push(rec);
    }
    let mut departed = Vec::new();
    let mut seen: HashSet<&Bytes> = HashSet::with_capacity(writes.len());
    for (key, _) in writes {
        if covered.contains_key(key) || !seen.insert(key) {
            continue;
        }
        departed.push(WireRecord {
            key: key.clone(),
            value: None,
            ver: written[key],
            expires_at_ms: None,
        });
    }
    FanOutRecords { records, departed }
}

/// Sim-only mirror of [`FanOutItem`], reachable from `tests/sim/` under
/// the `sim` feature since `FanOutItem` itself is not; see
/// [`Shard::drain_fan_out_for_sim`].
#[cfg(feature = "sim")]
#[doc(hidden)]
pub enum SimFanOut<K> {
    /// Mirrors [`FanOutItem::Applied`], without the version.
    Applied(K),
    /// Mirrors [`FanOutItem::Forward`].
    Forward(WireRecord),
}

/// Whether a [`Shard::get_or_load`] fill goes out to peers. A fill copies
/// data from its source and changes nothing there. Only a fill the shard
/// stores goes out ([`fill_route`]); in `Replicated` and `Distributed` it is
/// sent so a peer holding the key skips its own loader call. `Invalidation`
/// sends nothing: an invalidation would drop every peer's copy of the same
/// data, and two nodes reading one key would evict each other on every
/// fill. Pure; unit tested directly.
pub(super) const fn fill_fans_out(mode: Mode) -> bool {
    !matches!(mode, Mode::Invalidation)
}

/// What a read-through fill does with its value once the loader returns it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FillRoute {
    /// The shard stores the value, subject to the fill guards, and fans it
    /// out per [`fill_fans_out`].
    Install,
    /// The value answers the caller and every joined waiter and is stored
    /// nowhere; `counts_refusal` is whether the memory ceiling refused it.
    Uncached { counts_refusal: bool },
}

/// Where a fill whose loader returned goes. A shard stores only what it owns:
/// a `Mode::Distributed` node that owns nothing of the key's part keeps no
/// copy and sends nothing, since no write or removal of that key would ever
/// reach a copy it held. An owner at the memory ceiling answers uncached and
/// counts the refusal; a non-owner stores nothing, so it refuses nothing.
/// Pure; unit tested directly.
pub(super) const fn fill_route(owns: bool, at_ceiling: bool) -> FillRoute {
    if !owns {
        FillRoute::Uncached {
            counts_refusal: false,
        }
    } else if at_ceiling {
        FillRoute::Uncached {
            counts_refusal: true,
        }
    } else {
        FillRoute::Install
    }
}

/// Whether the records a drain sends carry values: every mode but an
/// `Invalidation` cache whose resolver does not merge, which sends peers a
/// bare invalidation. Pure; unit tested directly.
pub(super) const fn fan_out_ships_values(mode: Mode, merges: bool) -> bool {
    merges || !matches!(mode, Mode::Invalidation)
}

/// Whether a spilled entry in the fan-out's written view is read off disk:
/// only when the cache sends values (`ships_values`) and the entry has not
/// lapsed by `now_ms`. Any other spilled entry is reported departed from its
/// pointer alone and invalidates the peers' copy, which a value could not
/// improve on: a non-merging `Invalidation` cache sends no value, and a
/// lapsed value is dead. Pure; unit tested directly.
#[cfg(any(feature = "spill", test))]
pub(super) fn spilled_needs_read(
    ships_values: bool,
    expires_at_ms: Option<u64>,
    now_ms: u64,
) -> bool {
    ships_values && expires_at_ms.is_none_or(|expires| now_ms < expires)
}

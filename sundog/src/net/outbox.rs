//! The `Invalidate`-class per-peer outbox: a bounded FIFO that drops the
//! oldest queued entry on overflow rather than rejecting the newest. An
//! invalidation storm on a dead peer must never stall writers.
//! `Replicate`-class traffic uses a plain `tokio::sync::mpsc` channel
//! instead (`net::conn`), whose drop-the-newest overflow is what `try_send`
//! already gives.
//!
//! Generic over the queued element type `T` so this drop-policy logic stays
//! independent of what is queued.

use std::collections::VecDeque;
use std::sync::Mutex;

use tokio::sync::Notify;

/// A bounded queue that discards its oldest entry rather than rejecting a
/// push once full. `push` is synchronous and non-blocking; `pop` is the
/// async side the per-peer writer task awaits.
pub(super) struct DropOldestQueue<T> {
    inner: Mutex<VecDeque<T>>,
    notify: Notify,
    capacity: usize,
}

impl<T> DropOldestQueue<T> {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(VecDeque::with_capacity(capacity.min(1024))),
            notify: Notify::new(),
            capacity,
        }
    }

    /// Enqueues `item`, dropping the oldest entry first if already at
    /// capacity. Answers whether it dropped one.
    pub(super) fn push(&self, item: T) -> bool {
        let mut queue = self
            .inner
            .lock()
            .expect("invariant: outbox mutex is never held across a panic");
        let evicted = queue.len() >= self.capacity;
        if evicted {
            queue.pop_front();
        }
        queue.push_back(item);
        drop(queue);
        self.notify.notify_one();
        evicted
    }

    /// Removes and returns the oldest queued entry, if any, without
    /// waiting. The non-blocking counterpart to [`DropOldestQueue::pop`].
    pub(super) fn try_pop(&self) -> Option<T> {
        self.inner
            .lock()
            .expect("invariant: outbox mutex is never held across a panic")
            .pop_front()
    }

    /// The number of entries queued, which never exceeds the capacity.
    pub(super) fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("invariant: outbox mutex is never held across a panic")
            .len()
    }

    /// Waits for and removes the oldest queued entry.
    pub(super) async fn pop(&self) -> T {
        loop {
            let notified = self.notify.notified();
            {
                let mut queue = self
                    .inner
                    .lock()
                    .expect("invariant: outbox mutex is never held across a panic");
                if let Some(item) = queue.pop_front() {
                    return item;
                }
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn overflow_drops_the_oldest_entry() {
        let queue = DropOldestQueue::new(2);
        assert!(!queue.push(1u64), "room left: nothing dropped");
        assert!(!queue.push(2u64), "room left: nothing dropped");
        assert!(queue.push(3u64), "the push at capacity drops the oldest"); // drops 1, not 3

        assert_eq!(queue.pop().await, 2);
        assert_eq!(queue.pop().await, 3);
    }

    #[test]
    fn try_pop_drains_without_waiting_then_returns_none() {
        let queue = DropOldestQueue::new(4);
        assert!(queue.try_pop().is_none());
        queue.push(1u64);
        queue.push(2u64);
        assert_eq!(queue.try_pop().expect("first queued"), 1);
        assert_eq!(queue.try_pop().expect("second queued"), 2);
        assert!(queue.try_pop().is_none());
    }

    #[test]
    fn len_counts_the_queued_entries_and_stops_at_the_capacity() {
        let queue = DropOldestQueue::new(3);
        assert_eq!(queue.len(), 0, "a new queue is empty");
        queue.push(1u64);
        queue.push(2u64);
        assert_eq!(queue.len(), 2);
        queue.push(3u64);
        queue.push(4u64); // drops 1
        assert_eq!(
            queue.len(),
            3,
            "an overflowing push replaces, it does not grow"
        );
        assert_eq!(queue.try_pop(), Some(2));
        assert_eq!(queue.len(), 2, "a pop shrinks it");
        while queue.try_pop().is_some() {}
        assert_eq!(queue.len(), 0);
    }

    #[tokio::test]
    async fn pop_waits_for_a_push() {
        let queue = std::sync::Arc::new(DropOldestQueue::new(4));
        let waiter = tokio::spawn({
            let queue = queue.clone();
            async move { queue.pop().await }
        });
        tokio::task::yield_now().await;
        queue.push(42u64);
        let got = waiter.await.expect("task did not panic");
        assert_eq!(got, 42);
    }
}

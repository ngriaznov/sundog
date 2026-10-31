//! The per-peer outboxes. The `Invalidate` class's is a FIFO bounded in
//! entries and in the memory they hold that drops the oldest queued entries
//! on overflow rather than rejecting the newest: an invalidation storm on a
//! dead peer must never stall writers. The `Replicate` class's is a channel
//! bounded in frames and in the memory its queued frames hold
//! ([`replicate_outbox`]). A queued frame weighs what it keeps alive
//! ([`OutFrame::resident_bytes`]), so a peer that stops reading holds at
//! most two such bounds of this node's memory.
//!
//! [`DropOldestQueue`] is generic over the queued element type `T`, which
//! says what it weighs ([`Weighed`]), so its drop-policy logic stays
//! independent of what is queued.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{Notify, Semaphore, SemaphorePermit, TryAcquireError, mpsc};

use super::OutFrame;

/// What a queued entry weighs against its queue's byte budget.
pub(super) trait Weighed {
    /// The bytes a queued copy of this entry holds.
    fn weight(&self) -> usize;
}

impl Weighed for OutFrame {
    fn weight(&self) -> usize {
        self.resident_bytes()
    }
}

/// The entries of a [`DropOldestQueue`], each with the weight it counts for,
/// and their total.
struct Weighted<T> {
    entries: VecDeque<(T, usize)>,
    bytes: usize,
}

impl<T> Weighted<T> {
    fn pop_front(&mut self) -> Option<T> {
        let (item, weight) = self.entries.pop_front()?;
        self.bytes -= weight;
        Some(item)
    }
}

/// A queue bounded in entries and in the bytes they weigh that discards its
/// oldest entries rather than rejecting a push once full. `push` is
/// synchronous and non-blocking; `pop` is the async side the per-peer writer
/// task awaits.
pub(super) struct DropOldestQueue<T> {
    inner: Mutex<Weighted<T>>,
    notify: Notify,
    capacity: usize,
    budget: usize,
}

impl<T: Weighed> DropOldestQueue<T> {
    /// A queue of at most `capacity` entries weighing at most `budget` bytes
    /// together.
    pub(super) fn new(capacity: usize, budget: usize) -> Self {
        Self {
            inner: Mutex::new(Weighted {
                entries: VecDeque::with_capacity(capacity.min(1024)),
                bytes: 0,
            }),
            notify: Notify::new(),
            capacity,
            budget,
        }
    }

    /// Enqueues `item`, dropping the oldest entries first while the queue is
    /// at capacity or `item` would take its weight past the budget. An entry
    /// heavier than the budget weighs the budget, so it goes in alone rather
    /// than never. Answers how many entries it dropped.
    pub(super) fn push(&self, item: T) -> u64 {
        let weight = item.weight().min(self.budget);
        let mut queue = self
            .inner
            .lock()
            .expect("invariant: outbox mutex is never held across a panic");
        let mut evicted = 0u64;
        while !queue.entries.is_empty()
            && (queue.entries.len() >= self.capacity
                || queue.bytes.saturating_add(weight) > self.budget)
        {
            queue.pop_front();
            evicted += 1;
        }
        queue.entries.push_back((item, weight));
        queue.bytes += weight;
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
            .entries
            .len()
    }

    /// The bytes the queued entries weigh, which never exceed the budget.
    #[cfg(test)]
    pub(super) fn bytes(&self) -> usize {
        self.inner
            .lock()
            .expect("invariant: outbox mutex is never held across a panic")
            .bytes
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

/// A peer's `Replicate`-class outbox: at most `frames` frames and `bytes` of
/// memory queued at once, the sending half and the receiving half its writer
/// drains. A frame weighs what a queued copy of it holds
/// ([`OutFrame::resident_bytes`]), and one heavier than `bytes` weighs
/// `bytes`, so it goes in alone rather than never.
pub(super) fn replicate_outbox(frames: usize, bytes: usize) -> (OutboxTx, OutboxRx) {
    let (tx, rx) = mpsc::channel(frames);
    let budget = bytes.clamp(1, Semaphore::MAX_PERMITS);
    let bytes = Arc::new(Semaphore::new(budget));
    let taken = Arc::new(AtomicU64::new(0));
    (
        OutboxTx {
            tx,
            bytes: Arc::clone(&bytes),
            budget,
            taken: Arc::clone(&taken),
        },
        OutboxRx {
            rx,
            bytes,
            taken,
            unsettled: (0, 0),
        },
    )
}

/// A queued frame and the bytes it holds of its outbox's budget, given back
/// once the writer takes the frame ([`OutboxRx::settle`]).
struct Queued {
    frame: OutFrame,
    weight: usize,
}

/// The sending half of a [`replicate_outbox`].
#[derive(Clone)]
pub(super) struct OutboxTx {
    tx: mpsc::Sender<Queued>,
    bytes: Arc<Semaphore>,
    budget: usize,
    /// Frames the writer has taken, shared with the [`OutboxRx`].
    taken: Arc<AtomicU64>,
}

/// A frame slot and the bytes for one frame, held in a [`replicate_outbox`]
/// until [`OutboxPermit::send`] queues the frame or the permit drops.
pub(super) struct OutboxPermit<'a> {
    slot: mpsc::Permit<'a, Queued>,
    bytes: SemaphorePermit<'a>,
}

impl OutboxPermit<'_> {
    /// Queues `frame` in the slot and bytes this permit holds, which the
    /// queued frame keeps until the writer settles it.
    pub(super) fn send(self, frame: OutFrame) {
        let weight = self.bytes.num_permits();
        self.bytes.forget();
        self.slot.send(Queued { frame, weight });
    }
}

/// Why a [`replicate_outbox`] took no frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OutboxFull {
    /// Every frame slot, or the byte budget a frame needs, is taken.
    Full,
    /// The writer ended and dropped the receiving half.
    Closed,
}

impl OutboxTx {
    /// `len` bytes as a weight against the budget: the budget at most, so
    /// a heavier frame goes in alone, in the permits one acquire takes.
    fn weight(&self, len: usize) -> u32 {
        u32::try_from(len.min(self.budget)).unwrap_or(u32::MAX)
    }

    /// Takes a slot and `len` bytes, a frame's [`OutFrame::resident_bytes`],
    /// if both are free now.
    pub(super) fn try_reserve(&self, len: usize) -> Result<OutboxPermit<'_>, OutboxFull> {
        let bytes = self
            .bytes
            .try_acquire_many(self.weight(len))
            .map_err(|err| match err {
                TryAcquireError::NoPermits => OutboxFull::Full,
                TryAcquireError::Closed => OutboxFull::Closed,
            })?;
        match self.tx.try_reserve() {
            Ok(slot) => Ok(OutboxPermit { slot, bytes }),
            Err(mpsc::error::TrySendError::Full(())) => Err(OutboxFull::Full),
            Err(mpsc::error::TrySendError::Closed(())) => Err(OutboxFull::Closed),
        }
    }

    /// Waits for `len` bytes and then a slot. A dropped future holds
    /// neither. Bytes free now are taken without waiting in line, which only
    /// happens when no earlier frame waits for them.
    pub(super) async fn reserve(&self, len: usize) -> Result<OutboxPermit<'_>, OutboxFull> {
        let weight = self.weight(len);
        let bytes = match self.bytes.try_acquire_many(weight) {
            Ok(bytes) => bytes,
            Err(TryAcquireError::Closed) => return Err(OutboxFull::Closed),
            Err(TryAcquireError::NoPermits) => self
                .bytes
                .acquire_many(weight)
                .await
                .map_err(|_| OutboxFull::Closed)?,
        };
        let slot = self.tx.reserve().await.map_err(|_| OutboxFull::Closed)?;
        Ok(OutboxPermit { slot, bytes })
    }

    /// Waits for a slot and `frame`'s bytes, then queues it.
    #[cfg(test)]
    pub(super) async fn send(&self, frame: OutFrame) -> Result<(), OutboxFull> {
        self.reserve(frame.resident_bytes()).await?.send(frame);
        Ok(())
    }

    /// Queues `frame` if a slot and its bytes are free now.
    pub(super) fn try_send(&self, frame: OutFrame) -> Result<(), OutboxFull> {
        self.try_reserve(frame.resident_bytes())?.send(frame);
        Ok(())
    }

    /// How many frames the writer has taken from the outbox so far: what
    /// tells a peer that reads, however little, from one that reads nothing.
    pub(super) fn taken(&self) -> u64 {
        self.taken.load(Ordering::Relaxed)
    }

    /// Free frame slots.
    pub(super) fn capacity(&self) -> usize {
        self.tx.capacity()
    }

    /// The outbox's bound in frames.
    pub(super) fn max_capacity(&self) -> usize {
        self.tx.max_capacity()
    }

    /// Bytes of the budget queued frames or reservations hold.
    #[cfg(test)]
    pub(super) fn bytes_held(&self) -> usize {
        self.budget - self.bytes.available_permits()
    }

    /// The outbox's bound in bytes.
    #[cfg(test)]
    pub(super) fn byte_budget(&self) -> usize {
        self.budget
    }
}

/// The receiving half of a [`replicate_outbox`]: taking a frame gives its
/// bytes back and counts it taken, at once or, for the frames of a burst
/// ([`OutboxRx::take_more`]), once per burst ([`OutboxRx::settle`]).
/// Dropping it drops the frames still queued and closes the outbox.
pub(super) struct OutboxRx {
    rx: mpsc::Receiver<Queued>,
    bytes: Arc<Semaphore>,
    taken: Arc<AtomicU64>,
    /// The bytes and frames taken since the last [`OutboxRx::settle`].
    unsettled: (usize, u64),
}

impl OutboxRx {
    /// Waits for the next frame and settles it; `None` once every sender is
    /// gone.
    pub(super) async fn recv(&mut self) -> Option<OutFrame> {
        let queued = self.rx.recv().await?;
        let frame = self.take(queued);
        self.settle();
        Some(frame)
    }

    /// The next frame, if one is queued, settled.
    #[cfg(test)]
    pub(super) fn try_recv(&mut self) -> Result<OutFrame, mpsc::error::TryRecvError> {
        let frame = self.rx.try_recv().map(|queued| self.take(queued))?;
        self.settle();
        Ok(frame)
    }

    /// The next frame, if one is queued, left for [`OutboxRx::settle`]: a
    /// writer draining a burst settles its frames once, not one by one.
    pub(super) fn take_more(&mut self) -> Option<OutFrame> {
        self.rx.try_recv().ok().map(|queued| self.take(queued))
    }

    /// Gives back the bytes of the frames taken since the last settle and
    /// counts them taken.
    pub(super) fn settle(&mut self) {
        let (bytes, frames) = std::mem::take(&mut self.unsettled);
        if frames > 0 {
            self.bytes.add_permits(bytes);
            self.taken.fetch_add(frames, Ordering::Relaxed);
        }
    }

    /// Takes `queued`, its bytes and its count left for the next settle.
    fn take(&mut self, queued: Queued) -> OutFrame {
        self.unsettled.0 += queued.weight;
        self.unsettled.1 += 1;
        queued.frame
    }

    /// Frames queued.
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.rx.len()
    }
}

impl Drop for OutboxRx {
    /// Closes the byte budget, so a sender waiting for bytes learns the
    /// writer is gone.
    fn drop(&mut self) {
        self.bytes.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A frame of about `bytes` encoded bytes.
    fn frame_of(bytes: usize) -> OutFrame {
        OutFrame::new(crate::wire::Msg::Invalidate {
            cache: smol_str::SmolStr::new("c"),
            key: bytes::Bytes::from(vec![7u8; bytes]),
            ver: crate::hlc::Hlc {
                wall_ms: 1,
                logical: 0,
                node: crate::node::NodeId::from(1),
            },
        })
        .expect("encodes")
    }

    #[test]
    fn a_replicate_outbox_refuses_a_frame_past_its_byte_budget_with_slots_free() {
        let len = frame_of(1_000).resident_bytes();
        let (tx, mut rx) = replicate_outbox(100, len * 3);
        for _ in 0..3 {
            tx.try_send(frame_of(1_000))
                .expect("three frames fit the budget");
        }
        assert_eq!(tx.bytes_held(), len * 3);
        assert!(tx.capacity() > 0, "frame slots are left");
        assert_eq!(
            tx.try_send(frame_of(1_000)),
            Err(OutboxFull::Full),
            "a fourth frame passes the byte budget"
        );
        assert_eq!(tx.taken(), 0);
        rx.try_recv().expect("a queued frame");
        assert_eq!(tx.taken(), 1, "taking a frame counts it taken");
        assert_eq!(
            tx.bytes_held(),
            len * 2,
            "taking a frame gives its bytes back"
        );
        tx.try_send(frame_of(1_000))
            .expect("the freed bytes take a frame again");
    }

    #[test]
    fn a_replicate_outbox_refuses_a_frame_past_its_frame_bound_with_bytes_free() {
        let (tx, _rx) = replicate_outbox(2, 1 << 20);
        tx.try_send(frame_of(10)).expect("room");
        tx.try_send(frame_of(10)).expect("room");
        assert_eq!(tx.try_send(frame_of(10)), Err(OutboxFull::Full));
        assert_eq!(
            tx.bytes_held(),
            2 * frame_of(10).resident_bytes(),
            "the refused frame holds nothing"
        );
    }

    #[test]
    fn a_byte_budget_past_four_gibibytes_is_kept_whole() {
        let budget = 5 * 1024 * 1024 * 1024;
        let (tx, _rx) = replicate_outbox(4, budget);
        assert_eq!(tx.byte_budget(), budget, "as the invalidate queue keeps it");
        assert_eq!(tx.bytes_held(), 0);
        assert_eq!(tx.weight(3 * 1024 * 1024 * 1024), 3 * 1024 * 1024 * 1024);
        let (tx, _rx) = replicate_outbox(4, 0);
        assert_eq!(tx.byte_budget(), 1, "a zero budget still takes a frame");
    }

    #[test]
    fn a_frame_longer_than_the_budget_goes_in_alone() {
        let (tx, mut rx) = replicate_outbox(8, 100);
        tx.try_send(frame_of(1_000))
            .expect("an oversized frame takes the whole budget");
        assert_eq!(tx.bytes_held(), 100);
        assert_eq!(tx.try_send(frame_of(1)), Err(OutboxFull::Full));
        rx.try_recv().expect("the oversized frame");
        tx.try_send(frame_of(1)).expect("and gives the budget back");
    }

    #[tokio::test]
    async fn reserve_waits_for_bytes_the_writer_gives_back() {
        let len = frame_of(1_000).resident_bytes();
        let (tx, mut rx) = replicate_outbox(100, len);
        tx.try_send(frame_of(1_000)).expect("fits");
        let waiting = tx.reserve(len);
        tokio::pin!(waiting);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut waiting)
                .await
                .is_err(),
            "no bytes are free until the writer takes a frame"
        );
        rx.recv().await.expect("the queued frame");
        assert_eq!(tx.taken(), 1);
        waiting
            .await
            .expect("the bytes come back")
            .send(frame_of(1_000));
        assert_eq!(rx.len(), 1);
    }

    #[tokio::test]
    async fn dropping_the_receiver_closes_the_outbox_to_a_sender_waiting_for_bytes() {
        let len = frame_of(1_000).resident_bytes();
        let (tx, rx) = replicate_outbox(100, len);
        tx.try_send(frame_of(1_000)).expect("fits");
        let waiting = tx.reserve(len);
        tokio::pin!(waiting);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut waiting)
                .await
                .is_err(),
            "no bytes are free"
        );
        drop(rx);
        let woken = tokio::time::timeout(std::time::Duration::from_secs(5), waiting)
            .await
            .expect("the waiting sender wakes once the receiver drops");
        assert!(matches!(woken, Err(OutboxFull::Closed)));
        assert!(matches!(tx.reserve(len).await, Err(OutboxFull::Closed)));
        assert!(matches!(tx.try_reserve(len), Err(OutboxFull::Closed)));
    }

    /// The frames of a burst give their bytes back and count as taken once,
    /// when the writer settles them, and not before.
    #[test]
    fn a_burst_settles_its_frames_bytes_and_count_at_once() {
        let len = frame_of(1_000).resident_bytes();
        let (tx, mut rx) = replicate_outbox(100, len * 3);
        for _ in 0..3 {
            tx.try_send(frame_of(1_000)).expect("fits");
        }
        assert!(rx.take_more().is_some());
        assert!(rx.take_more().is_some());
        assert_eq!(tx.bytes_held(), len * 3, "nothing settled yet");
        assert_eq!(tx.taken(), 0);
        rx.settle();
        assert_eq!(tx.bytes_held(), len, "the burst's bytes come back at once");
        assert_eq!(tx.taken(), 2);
        rx.settle();
        assert_eq!(tx.taken(), 2, "a settle with nothing taken changes nothing");
        assert!(rx.take_more().is_some());
        assert!(rx.take_more().is_none(), "the outbox is empty");
    }

    /// A test entry weighs its value in bytes.
    impl Weighed for u64 {
        fn weight(&self) -> usize {
            usize::try_from(*self).expect("a test weight fits in usize")
        }
    }

    #[tokio::test]
    async fn overflow_drops_the_oldest_entry() {
        let queue = DropOldestQueue::new(2, usize::MAX);
        assert_eq!(queue.push(1u64), 0, "room left: nothing dropped");
        assert_eq!(queue.push(2u64), 0, "room left: nothing dropped");
        assert_eq!(queue.push(3u64), 1, "the push at capacity drops the oldest"); // drops 1, not 3

        assert_eq!(queue.pop().await, 2);
        assert_eq!(queue.pop().await, 3);
    }

    /// Past the byte budget, a push drops the oldest entries until the new
    /// one fits, however many entries that takes, and the pops give their
    /// weight back.
    #[tokio::test]
    async fn a_push_past_the_byte_budget_drops_the_oldest_until_it_fits() {
        let queue = DropOldestQueue::new(100, 10);
        assert_eq!(queue.push(3u64), 0);
        assert_eq!(queue.push(3u64), 0);
        assert_eq!(queue.push(4u64), 0, "exactly the budget fits");
        assert_eq!(queue.bytes(), 10);
        assert_eq!(queue.push(5u64), 2, "the two oldest make room for it");
        assert_eq!(queue.bytes(), 9);
        assert_eq!(queue.pop().await, 4);
        assert_eq!(queue.bytes(), 5, "a pop gives its weight back");
        assert_eq!(queue.try_pop(), Some(5));
        assert_eq!(queue.bytes(), 0);
    }

    /// An entry heavier than the whole budget empties the queue and goes in
    /// alone, counted as the budget, so it is not refused forever and the
    /// next push drops it.
    #[test]
    fn an_entry_heavier_than_the_budget_goes_in_alone() {
        let queue = DropOldestQueue::new(100, 10);
        assert_eq!(queue.push(2u64), 0);
        assert_eq!(queue.push(2u64), 0);
        assert_eq!(queue.push(50u64), 2, "it takes the whole queue");
        assert_eq!(queue.len(), 1);
        assert_eq!(queue.bytes(), 10, "it weighs the budget");
        assert_eq!(queue.push(1u64), 1, "the next push drops it");
        assert_eq!(queue.try_pop(), Some(1));
    }

    #[test]
    fn try_pop_drains_without_waiting_then_returns_none() {
        let queue = DropOldestQueue::new(4, usize::MAX);
        assert!(queue.try_pop().is_none());
        queue.push(1u64);
        queue.push(2u64);
        assert_eq!(queue.try_pop().expect("first queued"), 1);
        assert_eq!(queue.try_pop().expect("second queued"), 2);
        assert!(queue.try_pop().is_none());
    }

    #[test]
    fn len_counts_the_queued_entries_and_stops_at_the_capacity() {
        let queue = DropOldestQueue::new(3, usize::MAX);
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
        let queue = std::sync::Arc::new(DropOldestQueue::new(4, usize::MAX));
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

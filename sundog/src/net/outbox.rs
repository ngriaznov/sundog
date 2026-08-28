//! The `Invalidate`-class per-peer outbox: a bounded FIFO that drops the
//! *oldest* queued message on overflow (plan §6) rather than rejecting the
//! newest, because an invalidation storm on a dead peer must never stall
//! writers. `Replicate`-class traffic uses a plain `tokio::sync::mpsc`
//! channel instead (see `net::conn`), since its overflow policy — drop the
//! new message — is exactly what `try_send` already gives for free.

use std::collections::VecDeque;
use std::sync::Mutex;

use tokio::sync::Notify;

use crate::wire::Msg;

/// A bounded queue that discards its oldest entry rather than rejecting a
/// push once full. `push` is synchronous and non-blocking, matching
/// [`super::Mesh::send`]'s fire-and-forget contract; `pop` is the async side
/// the per-peer writer task awaits.
pub(super) struct DropOldestQueue {
    inner: Mutex<VecDeque<Msg>>,
    notify: Notify,
    capacity: usize,
}

impl DropOldestQueue {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(VecDeque::with_capacity(capacity.min(1024))),
            notify: Notify::new(),
            capacity,
        }
    }

    /// Enqueues `msg`, dropping the oldest queued message first if already
    /// at capacity. Never blocks.
    pub(super) fn push(&self, msg: Msg) {
        let mut queue = self
            .inner
            .lock()
            .expect("invariant: outbox mutex is never held across a panic");
        if queue.len() >= self.capacity {
            queue.pop_front();
        }
        queue.push_back(msg);
        drop(queue);
        self.notify.notify_one();
    }

    /// Waits for and removes the oldest queued message.
    pub(super) async fn pop(&self) -> Msg {
        loop {
            let notified = self.notify.notified();
            {
                let mut queue = self
                    .inner
                    .lock()
                    .expect("invariant: outbox mutex is never held across a panic");
                if let Some(msg) = queue.pop_front() {
                    return msg;
                }
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::NodeId;
    use smol_str::SmolStr;

    fn sample(n: u64) -> Msg {
        Msg::Invalidate {
            cache: SmolStr::new("users"),
            key: bytes::Bytes::from(n.to_be_bytes().to_vec()),
            ver: crate::hlc::Hlc {
                wall_ms: n,
                logical: 0,
                node: NodeId::from(n),
            },
        }
    }

    fn key_of(msg: &Msg) -> u64 {
        match msg {
            Msg::Invalidate { key, .. } => {
                u64::from_be_bytes(key.as_ref().try_into().expect("8-byte key"))
            }
            _ => unreachable!("test only pushes Invalidate messages"),
        }
    }

    #[tokio::test]
    async fn overflow_drops_the_oldest_entry() {
        let queue = DropOldestQueue::new(2);
        queue.push(sample(1));
        queue.push(sample(2));
        queue.push(sample(3)); // 1 should be dropped, not 3

        assert_eq!(key_of(&queue.pop().await), 2);
        assert_eq!(key_of(&queue.pop().await), 3);
    }

    #[tokio::test]
    async fn pop_waits_for_a_push() {
        let queue = std::sync::Arc::new(DropOldestQueue::new(4));
        let waiter = tokio::spawn({
            let queue = queue.clone();
            async move { queue.pop().await }
        });
        tokio::task::yield_now().await;
        queue.push(sample(42));
        let got = waiter.await.expect("task did not panic");
        assert_eq!(key_of(&got), 42);
    }
}

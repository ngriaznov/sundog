//! The cache names a cancelled `CacheBuilder::open` is still closing.
//!
//! An `open` of a name waits while the name is marked, so a retry that follows a
//! cancelled `open` finds the name free, with its spill tier closed, instead of
//! failing with `AlreadyOpen`.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};

use smol_str::SmolStr;
use tokio::sync::watch;

/// The marks one [`Cluster`](super::Cluster) holds, one per name at most.
#[derive(Default)]
pub(crate) struct ClosingMarks {
    state: Mutex<Marks>,
}

#[derive(Default)]
struct Marks {
    /// The id the next mark takes.
    next: u64,
    /// For each marked name, the id of its mark and the sender whose drop
    /// wakes the waiters.
    marked: HashMap<SmolStr, (u64, watch::Sender<()>)>,
}

impl ClosingMarks {
    fn state(&self) -> MutexGuard<'_, Marks> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Marks `name` and answers the mark's id. A name that is already marked
    /// takes the new mark, and the waiters on the old one wake to look again.
    pub(crate) fn mark(&self, name: &str) -> u64 {
        let mut state = self.state();
        let id = state.next;
        state.next += 1;
        let (sender, _) = watch::channel(());
        state.marked.insert(SmolStr::new(name), (id, sender));
        id
    }

    /// Clears the mark `id` set on `name` and wakes its waiters. A name
    /// since marked again keeps the newer mark.
    pub(crate) fn clear(&self, name: &str, id: u64) {
        let mut state = self.state();
        if state
            .marked
            .get(name)
            .is_some_and(|&(held, _)| clears_mark(held, id))
        {
            state.marked.remove(name);
        }
    }

    /// A receiver that reports an error once `name`'s mark is cleared or
    /// replaced; `None` when `name` is not marked.
    pub(crate) fn watch(&self, name: &str) -> Option<watch::Receiver<()>> {
        self.state()
            .marked
            .get(name)
            .map(|(_, sender)| sender.subscribe())
    }

    /// Whether `name` is marked.
    #[cfg(all(test, not(feature = "sim")))]
    pub(crate) fn is_marked(&self, name: &str) -> bool {
        self.state().marked.contains_key(name)
    }
}

/// Whether a clear by the holder of mark `id` removes the mark `held` that is
/// in place: only the mark the clearer set, never a newer one. Pure; unit
/// tested directly.
fn clears_mark(held: u64, id: u64) -> bool {
    held == id
}

/// Resolves once the mark `marked` came from is cleared or replaced.
pub(crate) async fn cleared(mut marked: watch::Receiver<()>) {
    // Nothing is ever sent on the channel, so `changed` answers an error
    // exactly when the sender drops.
    while marked.changed().await.is_ok() {}
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn a_clear_removes_only_the_mark_it_set() {
        let table = [(0, 0, true), (7, 7, true), (7, 6, false), (6, 7, false)];
        for (held, id, expected) in table {
            assert_eq!(
                clears_mark(held, id),
                expected,
                "held {held}, clearing {id}"
            );
        }
    }

    #[test]
    fn a_marked_name_reads_as_marked_until_its_mark_clears() {
        let marks = ClosingMarks::default();
        assert!(marks.watch("x").is_none(), "nothing marked yet");
        let id = marks.mark("x");
        assert!(marks.watch("x").is_some());
        assert!(marks.watch("y").is_none(), "another name is not marked");
        marks.clear("x", id);
        assert!(marks.watch("x").is_none());
    }

    #[test]
    fn a_stale_clear_leaves_the_newer_mark_in_place() {
        let marks = ClosingMarks::default();
        let old = marks.mark("x");
        let new = marks.mark("x");
        assert_ne!(old, new);
        marks.clear("x", old);
        assert!(marks.watch("x").is_some(), "the newer mark stands");
        marks.clear("x", new);
        assert!(marks.watch("x").is_none());
    }

    #[tokio::test]
    async fn a_waiter_wakes_when_the_mark_clears() {
        let marks = ClosingMarks::default();
        let id = marks.mark("x");
        let waiter = tokio::spawn(cleared(marks.watch("x").expect("marked")));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiter.is_finished(), "the mark is still set");
        marks.clear("x", id);
        tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("the waiter wakes")
            .expect("the waiter finishes");
    }

    #[tokio::test]
    async fn a_waiter_wakes_when_a_newer_mark_replaces_its_own() {
        let marks = ClosingMarks::default();
        marks.mark("x");
        let waiter = tokio::spawn(cleared(marks.watch("x").expect("marked")));
        marks.mark("x");
        tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("the waiter wakes to look again")
            .expect("the waiter finishes");
    }
}

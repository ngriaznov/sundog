//! Background write-load generator plus random-fetch sampler for the
//! preloaded key space: on a steady interval, unless paused, one randomly
//! chosen live node gets a write (insert or remove) and one gets a `fetch`,
//! with latency and outcome recorded. The write side only ever touches keys
//! it hasn't already removed, so the removed set is monotonic and the
//! surviving-key count it reports is exact — no resurrection to track.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use rand::random_range;

use crate::node::NodeSlot;
use crate::preload::{key_for, value_for};

const REMOVE_PROBABILITY: f64 = 0.15;
/// Bounds the latency sample so a long headless run doesn't grow it
/// unboundedly; large enough that percentiles stay meaningful.
const LATENCY_CAP: usize = 50_000;

/// A write tick's decision: touch a surviving key with a fresh value, or
/// remove one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Op {
    Insert,
    Remove,
}

/// Pure roll-to-decision mapping: `roll` is expected uniform in `[0, 1)`.
#[must_use]
pub(crate) fn decide_op(roll: f64, remove_probability: f64) -> Op {
    if roll < remove_probability {
        Op::Remove
    } else {
        Op::Insert
    }
}

/// The value a `pct` (0..=100) percentile reads off an ascending-sorted
/// slice of latencies. `0` for an empty slice.
#[must_use]
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
pub(crate) fn percentile(sorted_us: &[u64], pct: f64) -> u64 {
    if sorted_us.is_empty() {
        return 0;
    }
    let last = sorted_us.len() - 1;
    let idx = ((pct / 100.0) * last as f64).round() as usize;
    sorted_us[idx.min(last)]
}

/// Shared state between the load generator and whoever reports on it (the
/// TUI, the headless report): which keys survive, what the load last wrote
/// to a touched key, and fetch outcome/latency counters.
pub(crate) struct LoadState {
    keys: usize,
    removed: Vec<AtomicBool>,
    removed_count: AtomicU64,
    touched: StdMutex<HashMap<usize, String>>,
    pub(crate) writes: AtomicU64,
    pub(crate) removes: AtomicU64,
    pub(crate) fetch_hits: AtomicU64,
    pub(crate) fetch_misses: AtomicU64,
    pub(crate) fetch_errors: AtomicU64,
    latencies_us: StdMutex<VecDeque<u64>>,
}

impl LoadState {
    #[must_use]
    pub(crate) fn new(keys: usize) -> Self {
        Self {
            keys,
            removed: (0..keys).map(|_| AtomicBool::new(false)).collect(),
            removed_count: AtomicU64::new(0),
            touched: StdMutex::new(HashMap::new()),
            writes: AtomicU64::new(0),
            removes: AtomicU64::new(0),
            fetch_hits: AtomicU64::new(0),
            fetch_misses: AtomicU64::new(0),
            fetch_errors: AtomicU64::new(0),
            latencies_us: StdMutex::new(VecDeque::with_capacity(LATENCY_CAP)),
        }
    }

    #[must_use]
    pub(crate) fn is_removed(&self, index: usize) -> bool {
        self.removed[index].load(Ordering::Relaxed)
    }

    /// Marks `index` removed. Returns `true` if this call is the one that
    /// removed it (idempotent: a key already removed stays removed). Drops
    /// any recorded write for `index`, since a removed key never needs an
    /// expected value again and the map would otherwise keep every touched
    /// key's `String` alive for the life of the run.
    fn mark_removed(&self, index: usize) -> bool {
        let was_removed = self.removed[index].swap(true, Ordering::Relaxed);
        if !was_removed {
            self.removed_count.fetch_add(1, Ordering::Relaxed);
            self.touched
                .lock()
                .expect("invariant: touched map lock is never poisoned")
                .remove(&index);
        }
        !was_removed
    }

    fn touch(&self, index: usize, value: String) {
        self.touched
            .lock()
            .expect("invariant: touched map lock is never poisoned")
            .insert(index, value);
    }

    /// Keys still present: preloaded minus removed. Exact, since removal is
    /// monotonic and the load never resurrects a removed key.
    #[must_use]
    pub(crate) fn surviving_keys(&self) -> usize {
        self.keys - usize::try_from(self.removed_count.load(Ordering::Relaxed)).unwrap_or(self.keys)
    }

    /// The value `index` should hold right now: the load's last write to it,
    /// or its untouched preload value.
    #[must_use]
    pub(crate) fn expected_value(&self, index: usize) -> String {
        self.touched
            .lock()
            .expect("invariant: touched map lock is never poisoned")
            .get(&index)
            .cloned()
            .unwrap_or_else(|| value_for(index))
    }

    fn record_latency(&self, elapsed: Duration) {
        let mut latencies = self
            .latencies_us
            .lock()
            .expect("invariant: latency lock is never poisoned");
        if latencies.len() >= LATENCY_CAP {
            latencies.pop_front();
        }
        latencies.push_back(u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX));
    }

    /// A snapshot of `(p50, p99)` fetch latency in microseconds.
    #[must_use]
    pub(crate) fn latency_percentiles(&self) -> (u64, u64) {
        let mut sorted: Vec<u64> = self
            .latencies_us
            .lock()
            .expect("invariant: latency lock is never poisoned")
            .iter()
            .copied()
            .collect();
        sorted.sort_unstable();
        (percentile(&sorted, 50.0), percentile(&sorted, 99.0))
    }
}

fn pick_live_node(nodes: &[Arc<NodeSlot>]) -> Option<&Arc<NodeSlot>> {
    let live: Vec<&Arc<NodeSlot>> = nodes.iter().filter(|n| n.is_alive()).collect();
    if live.is_empty() {
        return None;
    }
    Some(live[random_range(0..live.len())])
}

/// Runs until aborted by the caller. Intended to be driven via
/// `tokio::spawn` and cancelled with `JoinHandle::abort`. Waits for
/// `preload_done` before doing anything, so it never contends with the
/// preload's own `insert_many` fan-out.
pub(crate) async fn run(
    nodes: Arc<Vec<Arc<NodeSlot>>>,
    state: Arc<LoadState>,
    interval: Duration,
    paused: Arc<AtomicBool>,
    preload_done: Arc<AtomicBool>,
) {
    while !preload_done.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let mut tick = tokio::time::interval(interval);
    loop {
        tick.tick().await;
        if paused.load(Ordering::Relaxed) {
            continue;
        }
        run_write_tick(&nodes, &state).await;
        run_fetch_tick(&nodes, &state).await;
    }
}

async fn run_write_tick(nodes: &[Arc<NodeSlot>], state: &LoadState) {
    let Some(node) = pick_live_node(nodes) else {
        return;
    };
    let Some(cache) = node.cache() else { return };
    let index = random_range(0..state.keys);
    if state.is_removed(index) {
        // Already gone; skip rather than resurrect it, keeping the removed
        // set — and the surviving-key count derived from it — exact.
        return;
    }
    match decide_op(random_range(0.0..1.0), REMOVE_PROBABILITY) {
        Op::Remove => {
            if cache.remove(&key_for(index)).await.is_ok() && state.mark_removed(index) {
                state.removes.fetch_add(1, Ordering::Relaxed);
            }
        }
        Op::Insert => {
            let value = format!("{}-by-node{}", value_for(index), node.index);
            if cache.insert(key_for(index), value.clone()).await.is_ok() {
                state.touch(index, value);
                state.writes.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

async fn run_fetch_tick(nodes: &[Arc<NodeSlot>], state: &LoadState) {
    let Some(node) = pick_live_node(nodes) else {
        return;
    };
    let Some(cache) = node.cache() else { return };
    let index = random_range(0..state.keys);
    let started = tokio::time::Instant::now();
    let outcome = cache.fetch(&key_for(index)).await;
    state.record_latency(started.elapsed());
    match outcome {
        Ok(Some(_)) => {
            state.fetch_hits.fetch_add(1, Ordering::Relaxed);
        }
        Ok(None) => {
            state.fetch_misses.fetch_add(1, Ordering::Relaxed);
        }
        Err(_) => {
            state.fetch_errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::build_slots;

    #[test]
    fn decide_op_below_the_threshold_removes() {
        assert_eq!(decide_op(0.05, 0.15), Op::Remove);
    }

    #[test]
    fn decide_op_at_or_above_the_threshold_inserts() {
        assert_eq!(decide_op(0.15, 0.15), Op::Insert);
        assert_eq!(decide_op(0.5, 0.15), Op::Insert);
    }

    #[test]
    fn percentile_of_empty_slice_is_zero() {
        assert_eq!(percentile(&[], 50.0), 0);
    }

    #[test]
    fn percentile_picks_the_middle_of_an_odd_length_slice() {
        let sorted = [10, 20, 30, 40, 50];
        assert_eq!(percentile(&sorted, 50.0), 30);
    }

    #[test]
    fn percentile_p99_is_near_the_top() {
        let sorted: Vec<u64> = (1..=100).collect();
        assert_eq!(percentile(&sorted, 99.0), 99);
    }

    #[test]
    fn percentile_p0_is_the_minimum() {
        let sorted = [5, 6, 7];
        assert_eq!(percentile(&sorted, 0.0), 5);
    }

    #[test]
    fn fresh_state_has_every_key_surviving() {
        let state = LoadState::new(100);
        assert_eq!(state.surviving_keys(), 100);
        assert!(!state.is_removed(42));
    }

    #[test]
    fn marking_removed_decrements_surviving_and_is_idempotent() {
        let state = LoadState::new(10);
        assert!(state.mark_removed(3));
        assert_eq!(state.surviving_keys(), 9);
        assert!(state.is_removed(3));
        // Removing again doesn't double-count.
        assert!(!state.mark_removed(3));
        assert_eq!(state.surviving_keys(), 9);
    }

    #[test]
    fn expected_value_falls_back_to_preload_value_until_touched() {
        let state = LoadState::new(10);
        assert_eq!(state.expected_value(7), "v7");
        state.touch(7, "v7-updated".to_owned());
        assert_eq!(state.expected_value(7), "v7-updated");
    }

    #[test]
    fn marking_removed_drops_the_touched_entry() {
        let state = LoadState::new(10);
        state.touch(4, "v4-updated".to_owned());
        assert_eq!(state.expected_value(4), "v4-updated");
        assert!(state.mark_removed(4));
        assert!(
            !state
                .touched
                .lock()
                .expect("invariant: touched map lock is never poisoned")
                .contains_key(&4)
        );
        // No entry left behind: expected_value falls back to the preload
        // value rather than keeping the stale write alive.
        assert_eq!(state.expected_value(4), "v4");
    }

    #[test]
    fn latency_percentiles_of_a_fresh_state_are_zero() {
        let state = LoadState::new(1);
        assert_eq!(state.latency_percentiles(), (0, 0));
    }

    #[test]
    fn latency_percentiles_reflect_recorded_samples() {
        let state = LoadState::new(1);
        for ms in [10, 20, 30, 40, 50] {
            state.record_latency(Duration::from_millis(ms));
        }
        let (p50, p99) = state.latency_percentiles();
        assert_eq!(p50, 30_000);
        assert_eq!(p99, 50_000);
    }

    #[test]
    fn picks_no_node_when_all_dead() {
        let slots = build_slots(3, 44_000);
        assert!(pick_live_node(&slots).is_none());
    }

    #[test]
    fn picks_only_the_one_live_node() {
        let slots = build_slots(3, 44_100);
        slots[1].status.alive.store(true, Ordering::Relaxed);
        let picked = pick_live_node(&slots).expect("one live node");
        assert_eq!(picked.index, 1);
    }
}

//! Preloads the demo's key space before the write load starts: `--keys`
//! keys, `k{i}` = `v{i}`, inserted in fixed-size batches spread round-robin
//! across the live nodes via `Cache::insert_many` so no single node's
//! fan-out queue takes the whole load. Each node's batches run in order
//! against that node, but every node's queue runs concurrently with the
//! others.

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::Context as _;

use crate::node::NodeSlot;

/// Keys per `insert_many` call. Small enough that one batch's fan-out never
/// dominates a node's outbox, large enough that two million keys don't need
/// hundreds of thousands of round trips.
pub(crate) const BATCH_SIZE: usize = 5_000;

/// Splits `total` keys into consecutive, non-overlapping `batch_size`-sized
/// ranges, the last one short if `total` doesn't divide evenly. Empty for
/// `total == 0`; a single range for `batch_size >= total`.
#[must_use]
pub(crate) fn batch_ranges(total: usize, batch_size: usize) -> Vec<Range<usize>> {
    if total == 0 || batch_size == 0 {
        return Vec::new();
    }
    let mut ranges = Vec::with_capacity(total.div_ceil(batch_size));
    let mut start = 0;
    while start < total {
        let end = (start + batch_size).min(total);
        ranges.push(start..end);
        start = end;
    }
    ranges
}

/// Which node index a batch is assigned to, round-robin: batch 0 to node 0,
/// batch 1 to node 1, wrapping once every node has one.
///
/// # Panics
///
/// Panics if `node_count` is zero.
#[must_use]
pub(crate) fn node_for_batch(batch_index: usize, node_count: usize) -> usize {
    assert!(node_count > 0, "node_for_batch needs at least one node");
    batch_index % node_count
}

/// Keys inserted per second, `0.0` for a zero or negative-length elapsed.
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub(crate) fn keys_per_sec(count: usize, elapsed: Duration) -> f64 {
    let secs = elapsed.as_secs_f64();
    if secs <= 0.0 {
        return 0.0;
    }
    count as f64 / secs
}

/// The key string for preload index `i`: `k0`, `k1`, ...
#[must_use]
pub(crate) fn key_for(i: usize) -> String {
    format!("k{i}")
}

/// The value string preload writes for index `i`: `v0`, `v1`, ... Kept short
/// so millions of entries stay cheap in memory.
#[must_use]
pub(crate) fn value_for(i: usize) -> String {
    format!("v{i}")
}

/// Outcome of one preload run: how many keys landed and how long it took.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Report {
    pub(crate) keys: usize,
    pub(crate) elapsed: Duration,
}

impl Report {
    #[must_use]
    pub(crate) fn keys_per_sec(&self) -> f64 {
        keys_per_sec(self.keys, self.elapsed)
    }
}

/// Preloads `keys` keys (`k{i}` = `v{i}`) across `nodes` in
/// [`BATCH_SIZE`]-sized batches, round-robin, one node's batches never
/// running ahead of the others by more than one in flight. `progress` is
/// bumped by each batch's size as it completes, so a caller (the TUI, or
/// the headless report) can poll it live.
///
/// # Errors
///
/// Returns an error if any node's `insert_many` call fails.
///
/// # Panics
///
/// Panics if `nodes` is empty.
pub(crate) async fn run(
    nodes: &Arc<Vec<Arc<NodeSlot>>>,
    keys: usize,
    progress: &Arc<AtomicU64>,
) -> anyhow::Result<Report> {
    assert!(!nodes.is_empty(), "preload needs at least one node");
    let started = Instant::now();
    let ranges = batch_ranges(keys, BATCH_SIZE);

    // Group batches by assigned node so each node's insert_many calls run
    // in order against it, while every node's queue runs concurrently.
    let mut per_node: Vec<Vec<Range<usize>>> = (0..nodes.len()).map(|_| Vec::new()).collect();
    for (batch_index, range) in ranges.into_iter().enumerate() {
        let node_index = node_for_batch(batch_index, nodes.len());
        per_node[node_index].push(range);
    }

    let mut tasks = Vec::with_capacity(nodes.len());
    for (node_index, batches) in per_node.into_iter().enumerate() {
        let node = Arc::clone(&nodes[node_index]);
        let progress = Arc::clone(progress);
        tasks.push(tokio::spawn(async move {
            for range in batches {
                let cache = node
                    .cache()
                    .with_context(|| format!("node{node_index}: not alive during preload"))?;
                let entries = range.clone().map(|i| (key_for(i), value_for(i)));
                cache.insert_many(entries).await.with_context(|| {
                    format!("node{node_index}: insert_many failed for {range:?}")
                })?;
                let len = u64::try_from(range.len()).unwrap_or(u64::MAX);
                progress.fetch_add(len, Ordering::Relaxed);
            }
            Ok::<(), anyhow::Error>(())
        }));
    }

    for task in tasks {
        task.await.context("preload task panicked")??;
    }

    Ok(Report {
        keys,
        elapsed: started.elapsed(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_ranges_covers_every_key_with_a_short_last_batch() {
        let ranges = batch_ranges(13, 5);
        assert_eq!(ranges, vec![0..5, 5..10, 10..13]);
    }

    #[test]
    fn batch_ranges_is_empty_for_zero_keys() {
        assert!(batch_ranges(0, 5).is_empty());
    }

    #[test]
    fn batch_ranges_is_empty_for_zero_batch_size() {
        assert!(batch_ranges(10, 0).is_empty());
    }

    #[test]
    fn batch_ranges_yields_one_batch_when_it_fits() {
        assert_eq!(batch_ranges(4, 5), vec![0..4]);
    }

    #[test]
    fn batch_ranges_covers_exact_multiples_with_no_remainder() {
        assert_eq!(batch_ranges(10, 5), vec![0..5, 5..10]);
    }

    #[test]
    fn node_for_batch_round_robins() {
        assert_eq!(node_for_batch(0, 3), 0);
        assert_eq!(node_for_batch(1, 3), 1);
        assert_eq!(node_for_batch(2, 3), 2);
        assert_eq!(node_for_batch(3, 3), 0);
        assert_eq!(node_for_batch(7, 3), 1);
    }

    #[test]
    #[should_panic(expected = "at least one node")]
    fn node_for_batch_rejects_zero_nodes() {
        let _ = node_for_batch(0, 0);
    }

    #[test]
    fn keys_per_sec_divides_count_by_elapsed_seconds() {
        assert!((keys_per_sec(1000, Duration::from_secs(2)) - 500.0).abs() < f64::EPSILON);
    }

    #[test]
    fn keys_per_sec_is_zero_for_no_elapsed_time() {
        assert!(keys_per_sec(1000, Duration::ZERO).abs() < f64::EPSILON);
    }

    #[test]
    fn key_and_value_strings_are_stable() {
        assert_eq!(key_for(42), "k42");
        assert_eq!(value_for(42), "v42");
    }
}

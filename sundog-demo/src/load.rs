//! Background write-load generator: on a steady interval, unless paused,
//! writes one randomized key to a randomly chosen *live* node — tagged by
//! origin node in the value text, on top of the origin every event already
//! carries via `sundog::Origin`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use rand::{random_bool, random_range};

use crate::node::NodeSlot;

const REMOVE_PROBABILITY: f64 = 0.15;

/// Runs until the task is aborted by the caller. Intended to be driven via
/// `tokio::spawn` and cancelled with `JoinHandle::abort`.
pub(crate) async fn run(
    nodes: Arc<Vec<Arc<NodeSlot>>>,
    key_space: usize,
    interval: Duration,
    paused: Arc<AtomicBool>,
) {
    let mut tick = tokio::time::interval(interval);
    let mut sequence: u64 = 0;
    loop {
        tick.tick().await;
        if paused.load(Ordering::Relaxed) {
            continue;
        }
        let Some(node) = pick_live_node(&nodes) else {
            continue;
        };
        let Some(cache) = node.cache() else { continue };
        let key = format!("k{}", random_range(0..key_space));
        if random_bool(REMOVE_PROBABILITY) {
            let _ = cache.remove(&key).await;
        } else {
            sequence += 1;
            let value = format!("v{sequence}-by-node{}", node.index);
            let _ = cache.insert(key, value).await;
        }
    }
}

fn pick_live_node(nodes: &[Arc<NodeSlot>]) -> Option<&Arc<NodeSlot>> {
    let live: Vec<&Arc<NodeSlot>> = nodes.iter().filter(|n| n.is_alive()).collect();
    if live.is_empty() {
        return None;
    }
    Some(live[random_range(0..live.len())])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::build_slots;

    #[test]
    fn picks_no_node_when_all_dead() {
        let slots = build_slots(3, 31_000);
        assert!(pick_live_node(&slots).is_none());
    }

    #[test]
    fn picks_only_the_one_live_node() {
        let slots = build_slots(3, 31_100);
        slots[1].status.alive.store(true, Ordering::Relaxed);
        let picked = pick_live_node(&slots).expect("one live node");
        assert_eq!(picked.index, 1);
    }
}

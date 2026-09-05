//! A spilled entry's digest fingerprint is bit-for-bit identical to a
//! resident one, so anti-entropy repairs a peer's dropped copy from a donor
//! whose own copy is, by the time the repair runs, sitting on disk rather
//! than in RAM — exercising the AE-pull-reply path's off-lock spilled-value
//! read, end to end.
//!
//! Its own test binary (a separate process from every other `tests/*.rs`
//! file), so installing the process-global Prometheus recorder here never
//! races another test for the slot.

#![cfg(all(feature = "spill", feature = "prometheus", not(feature = "sim")))]

mod common;

use std::time::Duration;

use sundog::{Cluster, Mode, SpillConfig};

/// Finds `metric{...,label="value",...} <number>` in Prometheus
/// text-exposition `body`, tolerant of label ordering and
/// integer-vs-float rendering. Mirrors `tests/prometheus_exporter.rs`'s own
/// `scraped_metric_value`, kept local since integration test binaries don't
/// share code beyond `mod common`.
fn metric_value(body: &str, metric: &str, label: (&str, &str)) -> Option<f64> {
    let wanted = format!("{}=\"{}\"", label.0, label.1);
    body.lines().find_map(|line| {
        let rest = line.strip_prefix(metric)?;
        let rest = rest.strip_prefix('{')?;
        let (labels, value) = rest.split_once('}')?;
        if !labels.split(',').any(|pair| pair == wanted) {
            return None;
        }
        value.trim().parse::<f64>().ok()
    })
}

/// A tiny `max_capacity` plus a `SpillConfig` on node `a` forces it to
/// spill some of what it inserts; node `b` stays unbounded and spill-free.
/// Live fan-out delivers every key to `b` first; wiping one of `b`'s
/// entries without a tombstone means only anti-entropy can bring it back —
/// and by the time it runs, `a`'s own copy may already be on disk, so the
/// repair only succeeds if the AE-pull-reply path's spilled-value read
/// (`ShardOps::records_for`) works. Once repaired, further
/// anti-entropy rounds with nothing new to reconcile settle the repair
/// counter at a fixed value.
#[tokio::test]
async fn replicated_two_node_spill_converges_and_settles_to_zero_repairs() {
    let handle = sundog::prometheus_handle()
        .expect("this file's own test binary is the sole claimant of the recorder slot");

    let gossip_a = common::reserve_gossip_addr().await;
    let gossip_b = common::reserve_gossip_addr().await;
    let cluster_a = Cluster::builder("it-spill-repl")
        .seeds([gossip_b])
        .config(common::fast_config().with(|c| c.gossip_bind_addr = gossip_a))
        .build()
        .await
        .expect("node a builds");
    let cluster_b = Cluster::builder("it-spill-repl")
        .seeds([gossip_a])
        .config(common::fast_config().with(|c| c.gossip_bind_addr = gossip_b))
        .build()
        .await
        .expect("node b builds");
    common::wait_for_peer_count(&cluster_a, 1, Duration::from_secs(15)).await;
    common::wait_for_peer_count(&cluster_b, 1, Duration::from_secs(15)).await;

    let dir = std::env::temp_dir().join(format!(
        "sundog-it-spill-repl-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is after the unix epoch")
            .as_nanos()
    ));
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let cache_a = cluster_a
        .cache::<u32, String>("orders")
        .mode(Mode::Replicated)
        .max_capacity(2)
        .spill(cfg)
        .open()
        .await
        .expect("a opens with spill composing with Replicated's max_capacity");
    let cache_b = cluster_b
        .cache::<u32, String>("orders")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("b opens, unbounded and spill-free");

    for k in 0..5u32 {
        cache_a
            .insert(k, format!("value-{k}"))
            .await
            .expect("insert");
    }

    // Live fan-out delivers every key to b.
    common::eventually(Duration::from_secs(10), || async {
        for k in 0..5u32 {
            if cache_b.get(&k).await.is_none() {
                return false;
            }
        }
        true
    })
    .await;

    // a's tiny max_capacity spills at least one of the five under eviction.
    common::eventually(Duration::from_secs(10), || async {
        (0..5u32).any(|k| cache_a.get_sync(&k).is_none())
    })
    .await;

    // Wipe one key on b without a tombstone: only anti-entropy repairs it,
    // and a's own copy may by now be sitting on disk rather than in RAM.
    cache_b.invalidate_local(&0).await;
    assert_eq!(cache_b.get(&0).await, None);

    common::eventually(Duration::from_secs(15), || async {
        cache_b.get(&0).await.is_some()
    })
    .await;
    assert_eq!(
        cache_b.get(&0).await,
        Some("value-0".to_string()),
        "anti-entropy repairs the dropped entry with the correct value even when the donor's \
         own copy is currently spilled"
    );

    // Steady state: with nothing left to reconcile, the repair counter
    // stops moving across further anti-entropy rounds. This is a quiescence
    // check — "nothing happens for a while" — which a bounded poll cannot
    // express (a poll returns the instant its condition first holds, so it
    // could observe `before == after` after only one round, missing a
    // repair that lands one round later); two fixed windows are the
    // deliberate exception to this file's own bounded-poll rule. `fast_
    // config`'s 150ms ae_interval means two 500ms windows span several
    // rounds each, giving the counter ample opportunity to move if it were
    // going to.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let before = metric_value(
        &handle.render(),
        "sundog_ae_repaired_total",
        ("cache", "orders"),
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    let after = metric_value(
        &handle.render(),
        "sundog_ae_repaired_total",
        ("cache", "orders"),
    );
    assert!(
        before.is_some_and(|v| v >= 1.0),
        "expected at least the one repair above to have been counted; got {before:?}"
    );
    assert_eq!(
        before, after,
        "a steady state with nothing new to write settles to zero further repairs"
    );

    cache_a.close().await;
    cache_b.close().await;
    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

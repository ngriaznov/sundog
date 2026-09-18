//! Regression test for the distributed demo's warm-reopen divergence:
//! three real, gossip-joined `Mode::Distributed` nodes, a small
//! `max_capacity` over a `SpillConfig::warm_reopen(true)` tier so most of
//! the keyspace spills, node0 killed cleanly, writes and deletes against
//! the surviving pair while it is down, then restarted on the same spill
//! directory inside the tombstone TTL gate, mirroring the demo's own
//! restart: build, then open immediately, no wait for gossip.
//!
//! Without a wait, this restart races ownership/spill attachment against
//! gossip: with zero known peers, rendezvous ranking over a single
//! candidate answers `owns(bucket) == true` for everything, so the warm
//! replay installs every bucket and the sole-owner rule marks them all
//! servable without verification, orphaning entries and serving some
//! stale. `Cache::open` waits for a first known peer before computing the
//! first ownership view, closing that race: after convergence, node0
//! holds no entry outside its current view, and every overwrite/delete
//! made while down reads correctly everywhere.
//!
//! Run with:
//! `cargo test -p sundog --features spill,prometheus --test
//! warm_reopen_cluster -- --nocapture`

#![cfg(all(feature = "spill", feature = "prometheus", not(feature = "sim")))]

mod common;

use std::collections::HashSet;
use std::net::SocketAddr;
use std::num::NonZeroU8;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

use sundog::{Cache, Cluster, ClusterConfig, Mode, NodeId, PrometheusHandle, SpillConfig};

const CLUSTER_NAME: &str = "it-warm-reopen-cluster";
const CACHE_NAME: &str = "demo";
/// Large enough (versus 1024 buckets) that an ownership-computation bug
/// shows up as a clear discrepancy, not noise.
const NUM_KEYS: u32 = 4_000;
/// Matches the demo harness's own fixed record size.
const VALUE_LEN: usize = 256;
/// Small enough that most of `NUM_KEYS` spills.
const MAX_CAPACITY: u64 = 300;

/// This file's one process-global Prometheus recorder, installed lazily
/// and shared by every test after that; each test scopes its own lookups
/// to its own cache name so sharing the recorder never mixes counts.
static METRICS: OnceLock<PrometheusHandle> = OnceLock::new();

fn metrics_handle() -> PrometheusHandle {
    METRICS
        .get_or_init(|| {
            sundog::prometheus_handle()
                .expect("this file's own test binary is the sole claimant of the recorder slot")
        })
        .clone()
}

/// Finds `metric{label1="value1",...} <number>` in Prometheus `body`.
fn scraped_metric_value(body: &str, metric: &str, labels: &[(&str, &str)]) -> Option<f64> {
    let wanted: Vec<String> = labels
        .iter()
        .map(|&(k, v)| format!("{k}=\"{v}\""))
        .collect();
    body.lines().find_map(|line| {
        let rest = line.strip_prefix(metric)?;
        let rest = rest.strip_prefix('{')?;
        let (line_labels, value) = rest.split_once('}')?;
        let line_labels: Vec<&str> = line_labels.split(',').collect();
        if !wanted
            .iter()
            .all(|w| line_labels.iter().any(|&pair| pair == w))
        {
            return None;
        }
        value.trim().parse::<f64>().ok()
    })
}

fn key_for(i: u32) -> String {
    format!("key-{i:06}")
}

/// A fixed-length, easily eyeballed value: `<tag>-xxxx...`, padded to
/// [`VALUE_LEN`] bytes.
fn tagged_value(tag: &str) -> String {
    let prefix = format!("{tag}-");
    let pad = VALUE_LEN.saturating_sub(prefix.len());
    let mut value = String::with_capacity(VALUE_LEN);
    value.push_str(&prefix);
    value.extend(std::iter::repeat_n('x', pad));
    value
}

fn cluster_config(gossip: SocketAddr) -> ClusterConfig {
    common::fast_config().with(|c| {
        c.gossip_bind_addr = gossip;
        c.ae_interval = Duration::from_millis(100);
        // Above bucket_release_window() and node0's downtime, so its
        // restart lands inside the warm-reopen TTL gate.
        c.tombstone_ttl = Duration::from_secs(4);
        // Mirrors the demo's own DISOWN_GRACE_ROUNDS.
        c.distributed_disown_grace_rounds = 2;
    })
}

fn spill_cfg(dir: &Path) -> SpillConfig {
    SpillConfig::new(dir, 8 << 20)
        .region_bytes(16 * 1024)
        .warm_reopen(true)
}

fn fresh_temp_dir() -> PathBuf {
    std::env::temp_dir().join(format!(
        "sundog-it-warm-reopen-cluster-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is after the unix epoch")
            .as_nanos()
    ))
}

async fn open_distributed(
    cluster: &Cluster,
    owners: NonZeroU8,
    spill_dir: &Path,
) -> Cache<String, String> {
    open_distributed_named(cluster, owners, spill_dir, CACHE_NAME, MAX_CAPACITY).await
}

/// [`open_distributed`], generalized over cache name and `max_capacity`.
async fn open_distributed_named(
    cluster: &Cluster,
    owners: NonZeroU8,
    spill_dir: &Path,
    cache_name: &str,
    max_capacity: u64,
) -> Cache<String, String> {
    cluster
        .cache::<String, String>(cache_name)
        .mode(Mode::Distributed { owners })
        .max_capacity(max_capacity)
        .spill(spill_cfg(spill_dir))
        .open()
        .await
        .expect("cache opens")
}

/// Indices into `node_ids` that `owners_of`'s current owner set names.
/// Valid once every node's view has converged.
fn current_owner_indices(owners_of: &[NodeId], node_ids: [NodeId; 3]) -> Vec<usize> {
    node_ids
        .iter()
        .enumerate()
        .filter(|&(_, id)| owners_of.contains(id))
        .map(|(i, _)| i)
        .collect()
}

/// Reproduces the demo's kill/restart-under-spill scenario on a real
/// three-node cluster: once `Cache::open`'s bounded membership wait has
/// run, node0's warm reopen converges cleanly, with no orphaned local
/// data and every overwrite/delete made during its downtime reading
/// correctly everywhere.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one end-to-end scenario (form a three-node cluster, preload, kill node0, mutate \
              while it is down, restart it, assert convergence) reads best kept together rather \
              than scattered across helpers that would each need most of the same state passed in"
)]
async fn warm_reopen_converges_without_orphaning_or_serving_stale_entries() {
    let metrics = metrics_handle();
    let dir = fresh_temp_dir();
    let owners = NonZeroU8::new(2).expect("nonzero");
    // Fixed across the restart, as a real deployment would.
    let node0_id = NodeId::random();

    let g0 = common::reserve_gossip_addr().await;
    let g1 = common::reserve_gossip_addr().await;
    let g2 = common::reserve_gossip_addr().await;

    // Initial three-node formation, waiting for peers before opening
    // any cache; the fix under test is specific to the restart below.
    let cluster0 = Cluster::builder(CLUSTER_NAME)
        .seeds([g1, g2])
        .config(cluster_config(g0))
        .node_id(node0_id)
        .build()
        .await
        .expect("node0 builds");
    let cluster1 = Cluster::builder(CLUSTER_NAME)
        .seeds([g0, g2])
        .config(cluster_config(g1))
        .build()
        .await
        .expect("node1 builds");
    let cluster2 = Cluster::builder(CLUSTER_NAME)
        .seeds([g0, g1])
        .config(cluster_config(g2))
        .build()
        .await
        .expect("node2 builds");

    common::wait_for_peer_count(&cluster0, 2, Duration::from_secs(15)).await;
    common::wait_for_peer_count(&cluster1, 2, Duration::from_secs(15)).await;
    common::wait_for_peer_count(&cluster2, 2, Duration::from_secs(15)).await;

    let cache0 = open_distributed(&cluster0, owners, &dir.join("node0")).await;
    let cache1 = open_distributed(&cluster1, owners, &dir.join("node1")).await;
    let cache2 = open_distributed(&cluster2, owners, &dir.join("node2")).await;

    // Gossip quiescence, so ownership reflects the three-node ring.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // ---- Preload: NUM_KEYS values through node0. ----
    let mut start = 0u32;
    while start < NUM_KEYS {
        let end = (start + 500).min(NUM_KEYS);
        cache0
            .insert_many((start..end).map(|i| (key_for(i), tagged_value(&format!("v1-{i}")))))
            .await
            .expect("bulk insert succeeds");
        start = end;
    }

    // owners: 2 gives each bucket two of three nodes, not necessarily
    // node1/node2, so `last_key` is checked against its own current view.
    let node1_id = cluster1.node_id();
    let node2_id = cluster2.node_id();
    let get_as = |owner: NodeId, key: String| {
        let (cache0, cache1, cache2) = (&cache0, &cache1, &cache2);
        async move {
            if owner == node0_id {
                cache0.get(&key).await
            } else if owner == node1_id {
                cache1.get(&key).await
            } else {
                assert_eq!(owner, node2_id, "owners_of named an unknown node");
                cache2.get(&key).await
            }
        }
    };
    let last_key = key_for(NUM_KEYS - 1);
    common::eventually(Duration::from_secs(20), || async {
        for owner in cache0.owners_of(&last_key) {
            if get_as(owner, last_key.clone()).await.is_none() {
                return false;
            }
        }
        true
    })
    .await;
    // Settle time: lets max_capacity spill most of the keyspace and
    // anti-entropy finish before node0 is killed.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let owned_by_node0_before: HashSet<String> = (0..NUM_KEYS)
        .map(key_for)
        .filter(|k| cache0.owners_of(k).contains(&node0_id))
        .collect();
    assert!(
        !owned_by_node0_before.is_empty(),
        "node0 must genuinely own some share of the keyspace before it goes down"
    );

    // ---- Kill node0 cleanly: shutdown checkpoints its spill tier,
    // mirroring the demo's own teardown. ----
    drop(cache0);
    cluster0.shutdown().await;

    // ---- While node0 is down: overwrite a known subset of existing keys
    // through node1, and delete a few through node2. ----
    let overwritten: Vec<u32> = (0..400).collect();
    for &i in &overwritten {
        cache1
            .insert(key_for(i), tagged_value(&format!("v2-{i}")))
            .await
            .expect("overwrite while node0 is down");
    }
    let deleted: Vec<u32> = (400..430).collect();
    for &i in &deleted {
        cache2
            .remove(&key_for(i))
            .await
            .expect("delete while node0 is down");
    }
    common::eventually(Duration::from_secs(10), || async {
        for &i in &overwritten {
            let want = Some(tagged_value(&format!("v2-{i}")));
            if cache1.get(&key_for(i)).await != want {
                return false;
            }
        }
        deleted
            .iter()
            .all(|&i| cache2.get_sync(&key_for(i)).is_none())
    })
    .await;

    // ---- Restart node0 on the same spill dir, inside the tombstone TTL
    // gate, mirroring the demo: build, then open immediately.
    // `Cache::open` runs the membership wait itself. ----
    let cluster0b = Cluster::builder(CLUSTER_NAME)
        .seeds([g1, g2])
        .config(cluster_config(g0))
        .node_id(node0_id)
        .build()
        .await
        .expect("node0 rebuilds");
    let cache0b = open_distributed(&cluster0b, owners, &dir.join("node0")).await;

    // Confirms warm reopen ran (not cold fallback) and replayed disk records.
    common::eventually(Duration::from_secs(5), || async {
        let body = metrics.render();
        scraped_metric_value(
            &body,
            "sundog_spill_reopen_total",
            &[("cache", CACHE_NAME), ("outcome", "warm")],
        )
        .is_some_and(|v| v >= 1.0)
            && scraped_metric_value(
                &body,
                "sundog_spill_reopen_records_total",
                &[("cache", CACHE_NAME)],
            )
            .is_some_and(|v| v > 0.0)
    })
    .await;

    // Convergence polling: peer discovery, ownership, anti-entropy.
    common::wait_for_peer_count(&cluster0b, 2, Duration::from_secs(15)).await;
    tokio::time::sleep(Duration::from_secs(4)).await;

    // ==== Convergence assertions ====

    let node_ids = [node0_id, node1_id, node2_id];
    let all_caches: [&Cache<String, String>; 3] = [&cache0b, &cache1, &cache2];
    let keys0 = cache0b.keys();

    // No local key sits in a bucket node0's current view excludes it
    // from (the bug this file regresses against).
    let orphaned: Vec<&String> = keys0
        .iter()
        .filter(|k| !cache0b.owners_of(k).contains(&node0_id))
        .collect();
    assert!(
        orphaned.is_empty(),
        "node0 holds {} entries (of {} total) for buckets its own current ownership view \
         excludes it from: {:?}",
        orphaned.len(),
        keys0.len(),
        orphaned.iter().take(5).collect::<Vec<_>>(),
    );

    // Every overwritten key reads the new value via fetch (every node)
    // and get (each current owner).
    for &i in &overwritten {
        let key = key_for(i);
        let want = Some(tagged_value(&format!("v2-{i}")));
        for &cache in &all_caches {
            assert_eq!(
                cache.fetch(&key).await.expect("fetch succeeds"),
                want,
                "fetch({key}) must read the overwrite from every node"
            );
        }
        let owners = cache1.owners_of(&key);
        for &owner_idx in &current_owner_indices(&owners, node_ids) {
            assert_eq!(
                all_caches[owner_idx].get(&key).await,
                want,
                "get({key}) on current owner index {owner_idx} must read the overwrite locally"
            );
        }
    }

    // Every deleted key is absent via fetch (every node) and get
    // (each current owner).
    for &i in &deleted {
        let key = key_for(i);
        for &cache in &all_caches {
            assert_eq!(
                cache.fetch(&key).await.expect("fetch succeeds"),
                None,
                "fetch({key}) must see the delete from every node"
            );
        }
        let owners = cache1.owners_of(&key);
        for &owner_idx in &current_owner_indices(&owners, node_ids) {
            assert_eq!(
                all_caches[owner_idx].get(&key).await,
                None,
                "get({key}) on current owner index {owner_idx} must not resurrect it locally"
            );
        }
    }

    // node0's local entry count matches its current owned share:
    // every live key its current owner set includes it in.
    let deleted_keys: HashSet<u32> = deleted.iter().copied().collect();
    let owned_by_node0_now: u64 = (0..NUM_KEYS)
        .filter(|i| !deleted_keys.contains(i))
        .map(key_for)
        .filter(|k| cache0b.owners_of(k).contains(&node0_id))
        .count() as u64;
    assert_eq!(
        cache0b.entry_count().await,
        owned_by_node0_now,
        "node0's local entry count must equal exactly the live keys it currently owns"
    );

    drop(cache0b);
    cluster0b.shutdown().await;
    drop(cache1);
    drop(cache2);
    cluster1.shutdown().await;
    cluster2.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

const CLUSTER_NAME_BULK: &str = "it-warm-reopen-cluster-bulk";
const CACHE_NAME_BULK: &str = "demo-bulk-overwrite";
/// Thousands of keys hashed across virtually all 1024 buckets, so
/// overwriting them all touches close to the whole keyspace.
const NUM_KEYS_BULK: u32 = 5_000;
/// Small enough that most of `NUM_KEYS_BULK` spills.
const MAX_CAPACITY_BULK: u64 = 500;

/// [`cluster_config`] with `ae_sketch_min_bucket`/`ae_sketch_cells` shrunk
/// so a bucket's `AeSketch` reply fails to decode deterministically from a
/// two-key divergence, forcing the `AeEntries` listing fallback this
/// file's bulk-overwrite test exercises. `ae_sketch_cells: 3` gives an
/// IBLT one cell wide per partition, so two or more differing keys always
/// leave a cell over-counted or checksum-mismatched (`Undecodable`);
/// `ae_sketch_min_bucket: 1` makes any bucket with more than one entry
/// use that sketch instead of an exact listing.
fn cluster_config_bulk_overwrite(gossip: SocketAddr) -> ClusterConfig {
    cluster_config(gossip).with(|c| {
        c.ae_sketch_min_bucket = 1;
        c.ae_sketch_cells = 3;
        // Widened past the 750ms default: this test's immediate-correctness
        // loop issues NUM_KEYS_BULK sequential fetches right after open(),
        // and machine contention shouldn't turn scheduling delay into a
        // spurious FetchUnavailable.
        c.fetch_timeout = Duration::from_secs(5);
        // Widened well past the overwrite phase's plausible duration: a
        // slow downtime under load must not flip this test from the
        // warm-reopen reconciliation path to reopen's unrelated
        // "downtime_exceeded" cold fallback.
        c.tombstone_ttl = Duration::from_secs(120);
    })
}

/// Reproduces this file's scenario at the scale the converge-before-serving
/// loop exists for: not a few hundred overwrites through one live
/// co-owner, but every key of a several-thousand-key keyspace spanning
/// virtually every bucket, overwritten while node0 is down.
/// `cluster_config_bulk_overwrite` shrinks the sketch's decodable capacity
/// so this divergence forces the fallback listing-repair path for close
/// to every bucket node0 shares with its live co-owner, the case a
/// single unchecked reconciliation round would have marked servable
/// without ever re-checking.
///
/// Asserts: every overwritten key reads the new value from node0 the
/// instant `open()` returns, never the stale warm-replayed one; genuine
/// per-bucket sketch-fallback repair activity happened; and, once
/// settled, every node's reads and node0's entry count agree with the
/// current ownership view.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one end-to-end scenario (form a three-node cluster, preload thousands of keys, \
              kill node0, overwrite the entire keyspace through its live co-owner, restart it, \
              assert immediate and eventual convergence) reads best kept together, matching this \
              file's other integration test"
)]
async fn warm_reopen_reconciles_a_mass_overwrite_across_most_buckets_without_serving_stale_reads() {
    let metrics = metrics_handle();
    let dir = fresh_temp_dir();
    let owners = NonZeroU8::new(2).expect("nonzero");
    // Fixed across the restart, same reasoning as this file's other test.
    let node0_id = NodeId::random();

    let g0 = common::reserve_gossip_addr().await;
    let g1 = common::reserve_gossip_addr().await;
    let g2 = common::reserve_gossip_addr().await;

    // ---- Initial three-node formation. ----
    let cluster0 = Cluster::builder(CLUSTER_NAME_BULK)
        .seeds([g1, g2])
        .config(cluster_config_bulk_overwrite(g0))
        .node_id(node0_id)
        .build()
        .await
        .expect("node0 builds");
    let cluster1 = Cluster::builder(CLUSTER_NAME_BULK)
        .seeds([g0, g2])
        .config(cluster_config_bulk_overwrite(g1))
        .build()
        .await
        .expect("node1 builds");
    let cluster2 = Cluster::builder(CLUSTER_NAME_BULK)
        .seeds([g0, g1])
        .config(cluster_config_bulk_overwrite(g2))
        .build()
        .await
        .expect("node2 builds");

    common::wait_for_peer_count(&cluster0, 2, Duration::from_secs(15)).await;
    common::wait_for_peer_count(&cluster1, 2, Duration::from_secs(15)).await;
    common::wait_for_peer_count(&cluster2, 2, Duration::from_secs(15)).await;

    let cache0 = open_distributed_named(
        &cluster0,
        owners,
        &dir.join("node0"),
        CACHE_NAME_BULK,
        MAX_CAPACITY_BULK,
    )
    .await;
    let cache1 = open_distributed_named(
        &cluster1,
        owners,
        &dir.join("node1"),
        CACHE_NAME_BULK,
        MAX_CAPACITY_BULK,
    )
    .await;
    let cache2 = open_distributed_named(
        &cluster2,
        owners,
        &dir.join("node2"),
        CACHE_NAME_BULK,
        MAX_CAPACITY_BULK,
    )
    .await;

    // Gossip quiescence, same reasoning as this file's other test.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // ---- Preload: NUM_KEYS_BULK keys through node0, batched. ----
    let mut start = 0u32;
    while start < NUM_KEYS_BULK {
        let end = (start + 500).min(NUM_KEYS_BULK);
        cache0
            .insert_many((start..end).map(|i| (key_for(i), tagged_value(&format!("v1-{i}")))))
            .await
            .expect("bulk insert succeeds");
        start = end;
    }

    let node1_id = cluster1.node_id();
    let node2_id = cluster2.node_id();
    let get_as = |owner: NodeId, key: String| {
        let (cache0, cache1, cache2) = (&cache0, &cache1, &cache2);
        async move {
            if owner == node0_id {
                cache0.get(&key).await
            } else if owner == node1_id {
                cache1.get(&key).await
            } else {
                assert_eq!(owner, node2_id, "owners_of named an unknown node");
                cache2.get(&key).await
            }
        }
    };
    let last_key = key_for(NUM_KEYS_BULK - 1);
    common::eventually(Duration::from_secs(20), || async {
        for owner in cache0.owners_of(&last_key) {
            if get_as(owner, last_key.clone()).await.is_none() {
                return false;
            }
        }
        true
    })
    .await;
    // Settle time: lets max_capacity spill most of the keyspace and
    // anti-entropy finish before node0 is killed.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let owned_by_node0_before: HashSet<String> = (0..NUM_KEYS_BULK)
        .map(key_for)
        .filter(|k| cache0.owners_of(k).contains(&node0_id))
        .collect();
    assert!(
        !owned_by_node0_before.is_empty(),
        "node0 must genuinely own some share of the keyspace before it goes down"
    );

    // ---- Kill node0 cleanly. ----
    drop(cache0);
    cluster0.shutdown().await;

    // ---- While node0 is down: overwrite every preloaded key through
    // its live co-owner node1, the divergence this test reconciles. ----
    let mut start = 0u32;
    while start < NUM_KEYS_BULK {
        let end = (start + 500).min(NUM_KEYS_BULK);
        cache1
            .insert_many((start..end).map(|i| (key_for(i), tagged_value(&format!("v2-{i}")))))
            .await
            .expect("overwrite while node0 is down succeeds");
        start = end;
    }

    // Convergence across the two live nodes, sampled rather than
    // exhaustively: polling all NUM_KEYS_BULK would just repeat the same
    // wait for no more signal.
    let sample: Vec<u32> = (0..NUM_KEYS_BULK).step_by(137).collect();
    common::eventually(Duration::from_secs(15), || async {
        for &i in &sample {
            let key = key_for(i);
            let want = Some(tagged_value(&format!("v2-{i}")));
            for owner in cache1.owners_of(&key) {
                // node0 is down; it cannot hold the converged answer yet, so
                // it is skipped here and checked after it restarts instead.
                let got = if owner == node1_id {
                    cache1.get(&key).await
                } else if owner == node2_id {
                    cache2.get(&key).await
                } else {
                    continue;
                };
                if got != want {
                    return false;
                }
            }
        }
        true
    })
    .await;

    // Baseline sketch-fallback count: expected zero, since nothing
    // before this point diverged a bucket by more than one key.
    let fallback_before = scraped_metric_value(
        &metrics.render(),
        "sundog_ae_sketch_total",
        &[("cache", CACHE_NAME_BULK), ("outcome", "fallback")],
    )
    .unwrap_or(0.0);

    // ---- Restart node0, mirroring the demo: build, then open
    // immediately. ----
    let cluster0b = Cluster::builder(CLUSTER_NAME_BULK)
        .seeds([g1, g2])
        .config(cluster_config_bulk_overwrite(g0))
        .node_id(node0_id)
        .build()
        .await
        .expect("node0 rebuilds");
    let cache0b = open_distributed_named(
        &cluster0b,
        owners,
        &dir.join("node0"),
        CACHE_NAME_BULK,
        MAX_CAPACITY_BULK,
    )
    .await;

    // ==== `open()` already ran the reconciliation loop to completion,
    // and fetch refuses an unverified bucket regardless, so every one of
    // these fetches must already see the overwrite. ====
    for i in 0..NUM_KEYS_BULK {
        let key = key_for(i);
        let want = Some(tagged_value(&format!("v2-{i}")));
        assert_eq!(
            cache0b.fetch(&key).await.expect("fetch succeeds"),
            want,
            "fetch({key}) from node0 must read the overwrite immediately after open(), never \
             the stale warm-replayed value"
        );
    }

    // Confirms this ran the warm-reopen path (not cold fallback) and
    // replayed records from disk; the bound is generous since this runs
    // after the exhaustive fetch loop above.
    common::eventually(Duration::from_secs(20), || async {
        let body = metrics.render();
        scraped_metric_value(
            &body,
            "sundog_spill_reopen_total",
            &[("cache", CACHE_NAME_BULK), ("outcome", "warm")],
        )
        .is_some_and(|v| v >= 1.0)
            && scraped_metric_value(
                &body,
                "sundog_spill_reopen_records_total",
                &[("cache", CACHE_NAME_BULK)],
            )
            .is_some_and(|v| v > 0.0)
    })
    .await;

    // Proves the loop repaired real per-bucket divergence rather than
    // trusting a single round's outcome: the shrunk sketch forces most
    // buckets through the fallback path, and a fallback-repaired bucket
    // only reports matched on the round after the one that repaired it,
    // so converged data here is only reachable through a confirming
    // second round, which a single-round design could not produce.
    let fallback_after = scraped_metric_value(
        &metrics.render(),
        "sundog_ae_sketch_total",
        &[("cache", CACHE_NAME_BULK), ("outcome", "fallback")],
    )
    .unwrap_or(0.0);
    assert!(
        fallback_after - fallback_before > 0.0,
        "expected real per-bucket sketch-decode failures (and so a repair, then a confirming \
         second round) for at least one of the {NUM_KEYS_BULK} overwritten keys' buckets; saw \
         none (before={fallback_before}, after={fallback_after})"
    );

    // ---- Convergence polling. ----
    common::wait_for_peer_count(&cluster0b, 2, Duration::from_secs(15)).await;
    tokio::time::sleep(Duration::from_secs(4)).await;

    // ==== Eventual-state assertions, exhaustive: local reads confirm
    // every bucket landed the right data. ====
    let node_ids = [node0_id, node1_id, node2_id];
    let all_caches: [&Cache<String, String>; 3] = [&cache0b, &cache1, &cache2];
    let keys0 = cache0b.keys();
    let orphaned: Vec<&String> = keys0
        .iter()
        .filter(|k| !cache0b.owners_of(k).contains(&node0_id))
        .collect();
    assert!(
        orphaned.is_empty(),
        "node0 holds {} entries (of {} total) for buckets its own current ownership view \
         excludes it from: {:?}",
        orphaned.len(),
        keys0.len(),
        orphaned.iter().take(5).collect::<Vec<_>>(),
    );

    for i in 0..NUM_KEYS_BULK {
        let key = key_for(i);
        let want = Some(tagged_value(&format!("v2-{i}")));
        for &cache in &all_caches {
            assert_eq!(
                cache.fetch(&key).await.expect("fetch succeeds"),
                want,
                "fetch({key}) must read the overwrite from every node once settled"
            );
        }
        let owners = cache1.owners_of(&key);
        for &owner_idx in &current_owner_indices(&owners, node_ids) {
            assert_eq!(
                all_caches[owner_idx].get(&key).await,
                want,
                "get({key}) on current owner index {owner_idx} must read the overwrite locally"
            );
        }
    }

    // node0's local entry count matches its current owned share;
    // nothing is deleted in this scenario.
    let owned_by_node0_now: u64 = (0..NUM_KEYS_BULK)
        .map(key_for)
        .filter(|k| cache0b.owners_of(k).contains(&node0_id))
        .count() as u64;
    assert_eq!(
        cache0b.entry_count().await,
        owned_by_node0_now,
        "node0's local entry count must equal exactly the live keys it currently owns"
    );

    drop(cache0b);
    cluster0b.shutdown().await;
    drop(cache1);
    drop(cache2);
    cluster1.shutdown().await;
    cluster2.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

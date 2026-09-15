//! Regression test for the distributed demo's 4M-key warm-reopen divergence
//! (workstream 3, scale-plan branch): three real, gossip-joined
//! `Mode::Distributed` nodes, a small `max_capacity` over a
//! `SpillConfig::warm_reopen(true)` tier so most of the keyspace spills,
//! node0 killed with a clean `Cluster::shutdown` (which runs
//! `ShardOps::close_spill_checkpointed`, exactly like
//! `demos/sundog-distributed-demo/src/node.rs::NodeSlot::teardown`), writes
//! and deletes against the surviving pair while node0 is down, then node0
//! restarted on the same spill directory inside the tombstone TTL gate.
//!
//! The restart mirrors `demos/sundog-distributed-demo/src/node.rs::open`
//! byte for byte: it calls `Cluster::builder(..).build().await` and
//! immediately chains `.cache(..).open().await`, with no wait of its own for
//! gossip to report any peers first. That used to race
//! `cache::attach_ownership`/`Shard::attach_spill`, both synchronous over
//! whatever `Cluster::peers()` showed at that instant, against gossip: with
//! zero known peers, `OwnershipView::compute` (rendezvous ranking over a
//! single candidate) answered `owns(bucket) == true` for every bucket, so
//! the warm replay installed every bucket in the snapshot and the sole-owner
//! rule marked them all servable without verification, orphaning entries no
//! current view ever attributed to node0 and serving some of them stale.
//!
//! `Cache::open` now closes that race itself: for `Mode::Distributed` with a
//! seeded cluster, it waits for a first known peer (bounded by
//! `min(ClusterConfig::state_transfer_budget, 5s)`) before computing the
//! first ownership view at all, so this test needs no wait of its own
//! either, exactly matching the demo's own restart path. This file's one
//! test proves the fix: after full convergence, node0 holds no entry outside
//! its own current view, every overwrite and delete made while it was down
//! reads correctly everywhere, and its local entry count matches its
//! current owned share exactly.
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
use std::time::Duration;

use sundog::{Cache, Cluster, ClusterConfig, Mode, NodeId, PrometheusHandle, SpillConfig};

const CLUSTER_NAME: &str = "it-warm-reopen-cluster";
const CACHE_NAME: &str = "demo";
/// Small enough to run fast, large enough (versus 1024 buckets) that an
/// ownership-computation bug shows up as a clear double-digit-percent
/// discrepancy rather than being lost in the noise of a handful of keys.
const NUM_KEYS: u32 = 4_000;
/// Matches the demo harness's own fixed record size.
const VALUE_LEN: usize = 256;
/// Small enough that most of `NUM_KEYS` spills under a `SpillConfig` with a
/// tiny region size.
const MAX_CAPACITY: u64 = 300;

fn metrics_handle() -> PrometheusHandle {
    sundog::prometheus_handle()
        .expect("this file's own test binary is the sole claimant of the recorder slot")
}

/// Finds `metric{label1="value1",...} <number>` in Prometheus
/// text-exposition `body`. Mirrors `tests/prometheus_exporter.rs`'s own
/// `scraped_metric_value`.
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

/// A fixed-length, easily eyeballed value: `<tag>-xxxx...`, padded out to
/// exactly [`VALUE_LEN`] bytes. Mirrors `tests/spill_replication.rs::fixed_value`.
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
        // Comfortably above `bucket_release_window()`
        // (`ae_interval * (2 * distributed_disown_grace_rounds + 2)` = 600ms
        // here) and comfortably above node0's downtime below, so its
        // restart lands inside the warm-reopen TTL gate, exactly the
        // scenario under test.
        c.tombstone_ttl = Duration::from_secs(4);
        // Mirrors the demo's own `DISOWN_GRACE_ROUNDS`.
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
    cluster
        .cache::<String, String>(CACHE_NAME)
        .mode(Mode::Distributed { owners })
        .max_capacity(MAX_CAPACITY)
        .spill(spill_cfg(spill_dir))
        .open()
        .await
        .expect("cache opens")
}

/// Every node in `nodes` that `key`'s current owner set names, as
/// `(node index, Cache)` pairs, per `owners_of`'s answer on whichever node's
/// view is asked -- valid once every node's view has converged to the same
/// `view_hash`, which every call site here only relies on after its own
/// convergence wait.
fn current_owner_indices(owners_of: &[NodeId], node_ids: [NodeId; 3]) -> Vec<usize> {
    node_ids
        .iter()
        .enumerate()
        .filter(|&(_, id)| owners_of.contains(id))
        .map(|(i, _)| i)
        .collect()
}

/// Reproduces the demo's kill/restart-under-spill scenario on a real,
/// three-node, gossip-joined `Mode::Distributed` cluster and confirms that,
/// once `Cache::open`'s own bounded membership wait has run, node0's warm
/// reopen converges cleanly: no orphaned local data, every overwrite and
/// delete made during its downtime reads correctly everywhere, and its
/// local entry count matches its current owned share exactly.
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
    // Fixed across node0's restart, exactly as a real deployment persisting
    // its node id would: isolates the membership-wait fix under test from
    // the separate, unrelated fact that `demos/sundog-distributed-demo`
    // never calls `ClusterBuilder::node_id` on restart at all. If node0's
    // identity changed on every restart, the rendezvous-computed bucket set
    // the checkpoint reflects would already be unrelated to its post-restart
    // share for reasons that have nothing to do with the race this test
    // covers.
    let node0_id = NodeId::random();

    let g0 = common::reserve_gossip_addr().await;
    let g1 = common::reserve_gossip_addr().await;
    let g2 = common::reserve_gossip_addr().await;

    // ---- Initial three-node formation: legitimate startup, waits for
    // peers before opening any cache, same as every other integration test
    // in this crate. The fix under test is specific to the *restart* path
    // below, which the demo does not wait on explicitly -- `Cache::open`
    // does it internally instead. ----
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

    // Gossip quiescence: gives every node's cache-mode advertisement time
    // to reach the other two, so every node's ownership view already
    // reflects the real three-node, `owners: 2` ring before the preload
    // below, mirroring `seed_pull_timeout_metric`'s identical wait in
    // `tests/prometheus_exporter.rs`.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // ---- Preload: a steady write load of NUM_KEYS values, through node0,
    // small enough to run fast but large enough (vs. 1024 buckets) for an
    // ownership-computation bug to show up as an unmistakable share of the
    // keyspace rather than a couple of flaky keys. ----
    let mut start = 0u32;
    while start < NUM_KEYS {
        let end = (start + 500).min(NUM_KEYS);
        cache0
            .insert_many((start..end).map(|i| (key_for(i), tagged_value(&format!("v1-{i}")))))
            .await
            .expect("bulk insert succeeds");
        start = end;
    }

    // Ownership among three live nodes at `owners: 2` gives each bucket
    // exactly two of the three nodes, not necessarily node1 and node2 --
    // so convergence for `last_key` is checked against whichever two nodes
    // its *own* current view names, not a fixed pair.
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
    // Settle time: lets the small `max_capacity` actually spill most of the
    // keyspace on every node (this scenario's whole point) and lets
    // anti-entropy finish reconciling ordinary replication before node0 is
    // killed.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let owned_by_node0_before: HashSet<String> = (0..NUM_KEYS)
        .map(key_for)
        .filter(|k| cache0.owners_of(k).contains(&node0_id))
        .collect();
    assert!(
        !owned_by_node0_before.is_empty(),
        "node0 must genuinely own some share of the keyspace before it goes down"
    );

    // ---- Kill node0 cleanly: `Cluster::shutdown` runs
    // `ShardOps::close_spill_checkpointed` for every still-open cache,
    // exactly like `demos/sundog-distributed-demo/src/node.rs`'s
    // `NodeSlot::teardown`. ----
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

    // ---- Restart node0 on the same spill dir, *inside* the tombstone TTL
    // gate, deliberately mirroring
    // `demos/sundog-distributed-demo/src/node.rs::open`: build, then
    // immediately open the cache. No wait for gossip to report any peers
    // first is performed *here*: `Cache::open` now runs that wait itself
    // for a `Mode::Distributed` cache opened on a seeded cluster, exactly
    // this scenario. ----
    let cluster0b = Cluster::builder(CLUSTER_NAME)
        .seeds([g1, g2])
        .config(cluster_config(g0))
        .node_id(node0_id)
        .build()
        .await
        .expect("node0 rebuilds");
    let cache0b = open_distributed(&cluster0b, owners, &dir.join("node0")).await;

    // Confirms the warm-reopen path actually ran (not a cold fallback) and
    // actually replayed records from disk.
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

    // ---- Convergence polling: wait for peer discovery, ownership
    // stabilization, reconciliation, and several anti-entropy rounds, the
    // same kind of wait the demo's own convergence poll performs. ----
    common::wait_for_peer_count(&cluster0b, 2, Duration::from_secs(15)).await;
    tokio::time::sleep(Duration::from_secs(4)).await;

    // ==== Convergence assertions ====

    let node_ids = [node0_id, node1_id, node2_id];
    let all_caches: [&Cache<String, String>; 3] = [&cache0b, &cache1, &cache2];
    let keys0 = cache0b.keys();

    // No local key sits in a bucket node0's own current view excludes it
    // from: every entry it physically holds has an ownership basis. Before
    // the fix, a warm reopen racing gossip convergence installed every
    // bucket under a transient "I own everything" view and the sole-owner
    // rule marked them all servable without verification, so this held
    // entries no current view ever attributed to node0.
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

    // Every overwritten key reads the new value through `Cache::fetch` on
    // every node (routes to a real owner regardless of who is asked), and
    // through `Cache::get` on each of its current owners (a local read is
    // only ever meaningful there).
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

    // Every deleted key is absent everywhere: through `Cache::fetch` on
    // every node, and through `Cache::get` on each of its current owners.
    // A resurrection would additionally violate "deleted or expired
    // entries never resurrect."
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

    // node0's local entry count matches its current owned share exactly:
    // every live (non-deleted) key whose current owner set includes node0,
    // no more (no orphans left over from the restart) and no less (nothing
    // still missing from a stalled pull).
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

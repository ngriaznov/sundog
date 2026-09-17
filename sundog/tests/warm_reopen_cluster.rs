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
use std::sync::OnceLock;
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

/// This file's one process-global Prometheus recorder, installed by
/// whichever of this file's tests calls [`metrics_handle`] first and shared
/// (via a cheap handle clone) by every test after that:
/// `sundog::prometheus_handle` installs into a single process-global slot
/// and fails a second caller in the same process, so a second `#[tokio::test]`
/// in this file calling it directly would panic whenever the test harness
/// runs both concurrently. Every test that reads it scopes its own
/// `scraped_metric_value` lookups to its own cache name, so sharing one
/// recorder across tests never mixes their counts.
static METRICS: OnceLock<PrometheusHandle> = OnceLock::new();

fn metrics_handle() -> PrometheusHandle {
    METRICS
        .get_or_init(|| {
            sundog::prometheus_handle()
                .expect("this file's own test binary is the sole claimant of the recorder slot")
        })
        .clone()
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
    open_distributed_named(cluster, owners, spill_dir, CACHE_NAME, MAX_CAPACITY).await
}

/// [`open_distributed`], generalized over the cache name and
/// `max_capacity`: lets a second scenario in this file open its own,
/// differently named and differently sized cache on the same
/// [`Cluster`]/spill-dir shape without disturbing `open_distributed`'s own
/// callers.
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

const CLUSTER_NAME_BULK: &str = "it-warm-reopen-cluster-bulk";
const CACHE_NAME_BULK: &str = "demo-bulk-overwrite";
/// Thousands of keys, hashed across virtually all 1024 buckets: expected
/// empty buckets at this count is `1024 * e^(-NUM_KEYS_BULK / 1024)` -- under
/// four, so overwriting every one of them below touches close to the whole
/// keyspace, not a favorable subset of it.
const NUM_KEYS_BULK: u32 = 5_000;
/// Small enough that most of `NUM_KEYS_BULK` spills, same reasoning as
/// [`MAX_CAPACITY`] at this file's other, smaller scale.
const MAX_CAPACITY_BULK: u64 = 500;

/// [`cluster_config`], with `ae_sketch_min_bucket`/`ae_sketch_cells` shrunk
/// so a bucket's `AeSketch` reply fails to decode deterministically from a
/// divergence as small as two keys, rather than needing every one of a
/// thousand buckets to individually clear the sketch's production
/// `RATED_CAPACITY` (~100 elements) worth of real overwrites to force the
/// same `AeEntries` fallback path this file's bulk-overwrite test exercises.
/// The same technique `cluster.rs`'s own
/// `anti_entropy_falls_back_to_the_listing_when_a_sketch_cannot_decode` unit
/// test uses (there: `ae_sketch_cells: 6` plus a deliberately induced
/// 25-element diff). Here, `ae_sketch_cells: 3` gives an IBLT exactly one
/// cell wide per partition (`Iblt::new`'s `(cells / IBLT_PARTITIONS).max(1)`),
/// so every element of a bucket's symmetric difference is forced into the
/// same three cells: a lone differing key still peels cleanly (its cell's
/// count is exactly `+-1` and its checksum matches that one key), but two or
/// more always leave every cell either over-counted or checksum-mismatched,
/// which `Iblt::peel` can only ever read as `Undecodable`.
/// `ae_sketch_min_bucket: 1` makes any bucket with more than one entry
/// answer with that sketch instead of an exact (and always-correct) full
/// listing, so the shrunk capacity above is actually reached.
fn cluster_config_bulk_overwrite(gossip: SocketAddr) -> ClusterConfig {
    cluster_config(gossip).with(|c| {
        c.ae_sketch_min_bucket = 1;
        c.ae_sketch_cells = 3;
        // This test's own immediate-correctness loop below issues
        // `NUM_KEYS_BULK` sequential `fetch`es right after `open()`, each a
        // real network round trip for any bucket that loop hasn't yet
        // marked serving; widened well past the 750ms default so a run
        // sharing the machine with this crate's other, unrelated parallel
        // test binaries does not turn ordinary scheduling delay into a
        // spurious `FetchUnavailable`.
        c.fetch_timeout = Duration::from_secs(5);
        // `cluster_config`'s own 4s default is sized for that function's
        // caller's much smaller downtime window (400 overwrites, 30
        // deletes); this test's `NUM_KEYS_BULK`-sized overwrite phase pushes
        // thousands of keys through real network round trips while node0 is
        // down, and does so under the same shrunk sketch capacity above, so
        // every one of those writes' own anti-entropy fallout is heavier
        // too. A downtime that runs long under load must not itself flip
        // this test from exercising the warm-reopen reconciliation path to
        // `reopen`'s unrelated, already-covered `"downtime_exceeded"` cold
        // fallback (`store/spill.rs`'s `now_ms - closed_at_ms >
        // tombstone_ttl_ms` gate) -- widened well past any plausible
        // overwrite-phase duration instead of merely above
        // `bucket_release_window()`.
        c.tombstone_ttl = Duration::from_secs(120);
    })
}

/// Reproduces this file's own scenario at the scale `reconcile_warm_buckets`'s
/// converge-before-serving loop (workstream 3's warm-reopen reconciliation
/// redesign, `reconcile-spec.md`) exists for: not a few hundred overwrites
/// through one live co-owner, but every key of a several-thousand-key
/// keyspace spanning virtually every bucket, overwritten while node0 is
/// down. `cluster_config_bulk_overwrite` shrinks the sketch's decodable
/// capacity so this divergence forces the responder's `AeSketch` reply to
/// fail decoding (`sundog_ae_sketch_total{outcome="fallback"}`) and fall
/// through to the exact `AeEntries` listing repair path for close to every
/// bucket node0 shares with its live co-owner -- exactly the case the
/// pre-fix `reconcile_warm_buckets` (one `run_round_against` per peer, then
/// trust `RoundOutcome::Reconciled` unconditionally, regardless of whether
/// the repair actually landed; `reconcile-spec.md` section 1) would have
/// marked servable without ever re-checking.
///
/// Asserts: every one of `NUM_KEYS_BULK` overwritten keys reads the new
/// value through `Cache::fetch` from node0 the instant `open()` returns,
/// never the stale warm-replayed one; genuine per-bucket sketch-fallback
/// repair activity actually happened (proving the loop closed a real gap,
/// not a single round's unchecked say-so); and, once the cluster settles,
/// every node's local reads and node0's entry count agree exactly with the
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
    // Settle time, same reasoning as this file's other test: lets the small
    // `max_capacity` actually spill most of the keyspace and lets ordinary
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

    // ---- While node0 is down: overwrite every preloaded key through its
    // live co-owner node1, batched exactly like the preload above -- this
    // is the "thousands of keys across most buckets" divergence the test
    // exists to reconcile. ----
    let mut start = 0u32;
    while start < NUM_KEYS_BULK {
        let end = (start + 500).min(NUM_KEYS_BULK);
        cache1
            .insert_many((start..end).map(|i| (key_for(i), tagged_value(&format!("v2-{i}")))))
            .await
            .expect("overwrite while node0 is down succeeds");
        start = end;
    }

    // Convergence across the two live nodes, on a representative sample
    // rather than exhaustively: `get` is a real per-key local read on each
    // node, and polling all `NUM_KEYS_BULK` of them here would just repeat
    // the same wait thousands of times for no more signal than a spread
    // sample already gives.
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

    // Baseline sketch-fallback count for this cache: expected zero, since
    // this cache name is unique to this test and nothing before this point
    // diverged a live co-owner's bucket by more than the one key a sketch
    // this small still peels cleanly.
    let fallback_before = scraped_metric_value(
        &metrics.render(),
        "sundog_ae_sketch_total",
        &[("cache", CACHE_NAME_BULK), ("outcome", "fallback")],
    )
    .unwrap_or(0.0);

    // ---- Restart node0, mirroring the demo's own restart path exactly as
    // this file's other test does: build, then open immediately, with no
    // wait of its own for gossip to report any peers first. ----
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

    // ==== The assertion this test exists for: `open()` already ran
    // `reconcile_warm_buckets` to completion before returning, and
    // `Cache::fetch`'s unverified-bucket gate refuses a local answer for any
    // bucket that loop could not close in time regardless -- so every one of
    // these `fetch`es, issued the instant `open()` hands back control with
    // no settle sleep first, must already see the overwrite, never the
    // stale warm-replayed value. ====
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

    // Confirms this ran the warm-reopen path (not a cold fallback) and
    // actually replayed records from disk, same check as this file's other
    // test. Both reopen metrics are recorded synchronously inside `open()`,
    // long before this line runs, so this is normally true on the very
    // first poll; the bound is generous (matching this test's other waits)
    // rather than tight, since it runs after this test's own
    // `NUM_KEYS_BULK`-sized exhaustive fetch loop, alongside whatever else
    // this crate's full test suite is running in parallel at the same time.
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

    // Proves the loop actually repaired real, per-bucket divergence rather
    // than trusting a single round's outcome unconditionally (the bug
    // `reconcile-spec.md` section 1 describes): with `ae_sketch_min_bucket`/
    // `ae_sketch_cells` shrunk (`cluster_config_bulk_overwrite`), any bucket
    // with two or more overwritten keys forces its co-owner's `AeSketch`
    // reply through the undecodable/fallback path deterministically, and
    // `NUM_KEYS_BULK` keys hashed across 1024 buckets puts the overwhelming
    // majority of buckets well past that two-key floor. A fallback-repaired
    // bucket's own digest exchange only reports it `matched` on the round
    // *after* the one that repaired it (`Cache::reconcile_warm_buckets`'s
    // own doc, and the discovery behind its
    // `reconcile_warm_buckets_loops_a_bounded_number_of_times_when_one_round_cannot_close_the_gap`
    // unit test), so this cache landing any converged, correct data at all
    // by the time `open()` returned above is only reachable through
    // `reconcile_warm_buckets`'s loop actually running a confirming second
    // round against real repaired data -- the single-round design this
    // workstream replaces never re-checked a round's outcome at all, so it
    // could not have produced this credibly.
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

    // ---- Convergence polling, mirroring this file's other test. ----
    common::wait_for_peer_count(&cluster0b, 2, Duration::from_secs(15)).await;
    tokio::time::sleep(Duration::from_secs(4)).await;

    // ==== Eventual-state assertions, exhaustive over the whole keyspace:
    // local reads (no network hop, so cheap even at this scale) confirm
    // every bucket genuinely landed the right data, not just that `fetch`
    // papered over one that never converged. ====
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

    // node0's local entry count matches its current owned share exactly:
    // every key (nothing is deleted in this scenario) whose current owner
    // set includes node0, no more and no less.
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

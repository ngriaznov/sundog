//! A value larger than one frame, which no merge builds, stays on its node and
//! out of every sync, and does not hold the other keys back; a peer's merge
//! that would push a value past the cap is refused until the value shrinks.
//!
//! The tests run on the real clock. Each waits for its cluster and its
//! convergence with a real-time bound, and returns as soon as its condition
//! holds.

use std::sync::Mutex;

use bytes::Bytes;
use futures::stream::StreamExt as _;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};

use super::*;
use crate::cache::Cache;
use crate::cluster::anti_entropy::{RoundOutcome, run_round_against};
use crate::cluster::handler::ClusterRequestHandler;
use crate::cluster::test_support::registered_shard;
use crate::hlc::Hlc;
use crate::store::crdt::{OrSet, OrSetResolver};
use crate::store::{Mode, now_ms};
use crate::wire::{self, WireRecord};

/// Reads one counter of a cache from a recorder. A snapshot resets the
/// counters it reads, so this keeps the running total.
struct Counted {
    snapshotter: Snapshotter,
    name: &'static str,
    cache: &'static str,
    total: Mutex<u64>,
}

impl Counted {
    fn new(recorder: &DebuggingRecorder, name: &'static str, cache: &'static str) -> Self {
        Self {
            snapshotter: recorder.snapshotter(),
            name,
            cache,
            total: Mutex::new(0),
        }
    }

    fn total(&self) -> u64 {
        let mut total = self.total.lock().expect("lock");
        for (key, _, _, value) in self.snapshotter.snapshot().into_vec() {
            let for_cache = key
                .key()
                .labels()
                .any(|l| l.key() == "cache" && l.value() == self.cache);
            if key.key().name() == self.name && for_cache {
                let DebugValue::Counter(n) = value else {
                    panic!("a counter, got {value:?}");
                };
                *total += n;
            }
        }
        *total
    }
}

/// The key of the record no frame holds.
const BIG_KEY: u32 = 1_000;

/// A record for [`BIG_KEY`] of a cache named `cache`, one byte past what a
/// frame holds, as a peer under a larger cap, or this node before the cap
/// applied, would store.
fn unshippable_record(cache: &SmolStr) -> WireRecord {
    let key = Bytes::from(postcard::to_stdvec(&BIG_KEY).expect("key encodes"));
    let base = wire::replicate_frame_len(cache.len(), key.len(), 0);
    let value_len = wire::MAX_FRAME - base + 1;
    // A `Vec<u8>` this long carries a four-byte length prefix.
    let value = postcard::to_stdvec(&vec![9u8; value_len - 4]).expect("value encodes");
    assert_eq!(value.len(), value_len);
    WireRecord {
        key,
        value: Some(Bytes::from(value)),
        ver: Hlc {
            wall_ms: now_ms(),
            logical: 0,
            node: NodeId::from(7_777),
        },
        expires_at_ms: None,
    }
}

const SMALL_KEYS: u32 = 20;

fn small_value(key: u32) -> Vec<u8> {
    vec![u8::try_from(key).expect("small"); 8]
}

async fn small_keys_present(cache: &Cache<u32, Vec<u8>>) -> bool {
    for key in 1..=SMALL_KEYS {
        if cache.get(&key).await != Some(small_value(key)) {
            return false;
        }
    }
    true
}

#[tokio::test]
async fn an_unshippable_record_stays_out_of_state_transfer_and_anti_entropy_and_the_other_keys_converge()
 {
    let recorder = DebuggingRecorder::new();
    let skipped = Counted::new(&recorder, "sundog_sync_skipped_total", "big");
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let cluster_a = Cluster::builder("cluster-it-oversize-sync")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node a builds");
    let cache_a = cluster_a
        .cache::<u32, Vec<u8>>("big")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("a opens");
    let name = SmolStr::new("big");
    let shard_a = registered_shard(&cluster_a, &name);
    // Stored below the public API, as a value stored before the upgrade is.
    shard_a.apply_remote(unshippable_record(&name)).await;
    for key in 1..=SMALL_KEYS {
        cache_a.insert(key, small_value(key)).await.expect("insert");
    }
    assert_eq!(
        cache_a.get(&BIG_KEY).await.map(|v| v.len()),
        Some(wire::MAX_FRAME - wire::replicate_frame_len(name.len(), 2, 0) + 1 - 4),
        "a keeps serving it"
    );

    // A joining node pulls a's state; the join must complete with every
    // other key.
    let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;
    let cluster_b = Cluster::builder("cluster-it-oversize-sync")
        .seeds([gossip_a])
        .config(loopback_config())
        .build()
        .await
        .expect("node b builds");
    wait_for_peer_count(&cluster_b, 1).await;
    let cache_b = tokio::time::timeout(
        Duration::from_secs(30),
        cluster_b
            .cache::<u32, Vec<u8>>("big")
            .mode(Mode::Replicated)
            .open(),
    )
    .await
    .expect("the join completes within the state-transfer budget")
    .expect("b opens");
    wait_until(
        Duration::from_secs(30),
        "b holds every small key",
        async || small_keys_present(&cache_b).await,
    )
    .await;
    assert_eq!(
        cache_b.get(&BIG_KEY).await,
        None,
        "the big record did not travel"
    );
    assert!(skipped.total() >= 1, "the skip is counted");

    // Anti-entropy still moves every other key in both directions, and the
    // round does not fail on the key it cannot ship.
    cache_b
        .insert(500, small_value(5))
        .await
        .expect("insert on b");
    cache_a
        .insert(501, small_value(6))
        .await
        .expect("insert on a");
    let shard_b = registered_shard(&cluster_b, &name);
    for _ in 0..3 {
        let outcome =
            run_round_against(cluster_b.mesh(), &shard_b, &name, cluster_a.node_id()).await;
        assert_eq!(
            outcome,
            RoundOutcome::Reconciled,
            "a round leaves out the key it cannot ship and goes on"
        );
    }
    wait_until(Duration::from_secs(30), "both new keys cross", async || {
        cache_a.get(&500).await == Some(small_value(5))
            && cache_b.get(&501).await == Some(small_value(6))
    })
    .await;
    assert_eq!(cache_b.get(&BIG_KEY).await, None);
    assert!(cache_a.get(&BIG_KEY).await.is_some(), "a still holds it");

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

/// A record for `key` of the small-value shape, as a peer's record, at a
/// version later than any this node has stamped.
fn peer_record(key: u32) -> WireRecord {
    WireRecord {
        key: Bytes::from(postcard::to_stdvec(&key).expect("key encodes")),
        value: Some(Bytes::from(
            postcard::to_stdvec(&small_value(key % 200)).expect("value encodes"),
        )),
        ver: Hlc {
            wall_ms: now_ms(),
            logical: 0,
            node: NodeId::from(7_777),
        },
        expires_at_ms: None,
    }
}

#[tokio::test]
async fn the_node_holding_the_unshippable_record_pushes_every_other_key_in_its_own_round() {
    let name = SmolStr::new("big");
    // A background round would push the keys before the one under test does.
    let quiet = || loopback_config().with(|c| c.ae_interval = Duration::from_secs(3_600));
    let cluster_a = Cluster::builder("cluster-it-oversize-push")
        .seeds(std::iter::empty())
        .config(quiet())
        .build()
        .await
        .expect("node a builds");
    let cache_a = cluster_a
        .cache::<u32, Vec<u8>>("big")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("a opens");
    let shard_a = registered_shard(&cluster_a, &name);
    shard_a.apply_remote(unshippable_record(&name)).await;
    let cluster_b = Cluster::builder("cluster-it-oversize-push")
        .seeds([cluster_a.inner.membership.local_peer().gossip_addr])
        .config(quiet())
        .build()
        .await
        .expect("node b builds");
    wait_for_peer_count(&cluster_b, 1).await;
    let cache_b = tokio::time::timeout(
        Duration::from_secs(30),
        cluster_b
            .cache::<u32, Vec<u8>>("big")
            .mode(Mode::Replicated)
            .open(),
    )
    .await
    .expect("the join completes within the state-transfer budget")
    .expect("b opens");
    // Arrivals from a peer are not fanned out, so b gets these from a's round alone.
    for key in 1..=SMALL_KEYS {
        shard_a.apply_remote(peer_record(key)).await;
    }
    assert_eq!(cache_b.get(&1).await, None, "b has none of them yet");

    let outcome = run_round_against(cluster_a.mesh(), &shard_a, &name, cluster_b.node_id()).await;

    assert_eq!(
        outcome,
        RoundOutcome::Reconciled,
        "a round leaves out the record it cannot push and goes on"
    );
    wait_until(
        Duration::from_secs(30),
        "b holds every small key a pushed",
        async || {
            for key in 1..=SMALL_KEYS {
                if cache_b.get(&key).await != Some(small_value(key % 200)) {
                    return false;
                }
            }
            true
        },
    )
    .await;
    assert_eq!(cache_b.get(&BIG_KEY).await, None, "the big one stays on a");
    assert!(cache_a.get(&BIG_KEY).await.is_some(), "a still holds it");

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

#[tokio::test]
async fn a_rebalance_chunk_carries_every_key_but_the_unshippable_record_and_counts_the_skip() {
    let recorder = DebuggingRecorder::new();
    let skipped = Counted::new(&recorder, "sundog_sync_skipped_total", "big");
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let name = SmolStr::new("big");
    let cluster = solo_cluster("cluster-it-oversize-rebalance").await;
    let cache = cluster
        .cache::<u32, Vec<u8>>("big")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("opens");
    let shard = registered_shard(&cluster, &name);
    shard.apply_remote(unshippable_record(&name)).await;
    for key in 1..=SMALL_KEYS {
        cache.insert(key, small_value(key)).await.expect("insert");
    }
    let handler = ClusterRequestHandler {
        shards: cluster.shards(),
        warmth: Arc::clone(&cluster.inner.warmth),
        ae_part_min_bucket: cluster.config().ae_part_min_bucket,
        ae_sketch_min_bucket: cluster.config().ae_sketch_min_bucket,
        ae_sketch_cells: cluster.config().ae_sketch_cells,
        rebalance_chunk_bytes: cluster.config().rebalance_chunk_bytes_value(),
    };

    // The chunk reader a rebalance and a leaving node's hand-off stream from.
    let bucket_count = u16::try_from(crate::store::BUCKET_COUNT).expect("fits");
    let every_bucket: Vec<u16> = (0..bucket_count).collect();
    let chunks: Vec<(u16, Vec<WireRecord>)> =
        handler.st_bucket_chunks(name, every_bucket).collect().await;

    let mut keys: Vec<u32> = chunks
        .iter()
        .flat_map(|(_, recs)| recs)
        .map(|rec| postcard::from_bytes(&rec.key).expect("u32 key"))
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        (1..=SMALL_KEYS).collect::<Vec<_>>(),
        "every small key streams and the big record does not"
    );
    assert_eq!(skipped.total(), 1, "the skip is counted");
    assert!(cache.get(&BIG_KEY).await.is_some(), "the record stays here");
    cluster.shutdown().await;
}

/// The cap of a node in [`a_merge_a_peer_sent_past_the_cap_is_refused_until_the_value_shrinks`].
const SMALL_CAP: usize = 16 * 1_024;

/// An element of a kilobyte, distinct per `n`.
fn element(n: usize) -> String {
    format!("{n:04}{}", "x".repeat(1_000))
}

#[tokio::test]
async fn a_merge_a_peer_sent_past_the_cap_is_refused_until_the_value_shrinks() {
    let recorder = DebuggingRecorder::new();
    let refused = Counted::new(&recorder, "sundog_merge_refused_total", "tags");
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let name = "cluster-it-peer-merge-cap";
    // a keeps values to 16 KiB; b, under a larger cap, grows one to 20.
    let cluster_a = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(loopback_config().with(|c| c.max_frame = SMALL_CAP))
        .build()
        .await
        .expect("node a builds");
    let open = |cluster: &Cluster| {
        cluster
            .cache::<u32, OrSet<String>>("tags")
            .mode(Mode::Replicated)
            .resolver(Arc::new(OrSetResolver::<String>::new()))
            .open()
    };
    let cache_a = open(&cluster_a).await.expect("a opens");
    let cluster_b = Cluster::builder(name)
        .seeds([cluster_a.inner.membership.local_peer().gossip_addr])
        .config(loopback_config().with(|c| c.max_frame = 64 * 1_024))
        .build()
        .await
        .expect("node b builds");
    wait_for_peer_count(&cluster_b, 1).await;
    let cache_b = tokio::time::timeout(Duration::from_secs(30), open(&cluster_b))
        .await
        .expect("b opens within the budget")
        .expect("b opens");
    cache_a
        .merge_sequenced(1, |writer, seq| OrSet::add(writer, seq, element(0)))
        .await
        .expect("a seeds the set");
    wait_until(Duration::from_secs(30), "b holds a's element", async || {
        cache_b
            .get(&1)
            .await
            .is_some_and(|set| set.contains(&element(0)))
    })
    .await;

    for n in 1..20 {
        cache_b
            .merge_sequenced(1, |writer, seq| OrSet::add(writer, seq, element(n)))
            .await
            .expect("b is under its own, larger, cap");
    }
    wait_until(
        Duration::from_secs(30),
        "a refuses what would pass its cap",
        async || refused.total() >= 1,
    )
    .await;
    // Every round of anti-entropy offers a the same record, and a refuses it.
    tokio::time::sleep(Duration::from_millis(600)).await;
    let held_by_a = cache_a
        .get(&1)
        .await
        .expect("a holds the set")
        .iter()
        .count();
    assert!(
        held_by_a <= 15,
        "a stays within what fits its cap and does not take b's twenty: {held_by_a}"
    );
    assert_eq!(
        cache_b
            .get(&1)
            .await
            .expect("b holds the set")
            .iter()
            .count(),
        20,
        "b holds all twenty"
    );

    // Shrinking the value on b lets a take it.
    for n in 0..8 {
        let observed = cache_b.get(&1).await.expect("b holds the set");
        cache_b
            .merge(1, OrSet::remove(&observed, &element(n)))
            .await
            .expect("a remove leaves the value no larger");
    }
    wait_until(
        Duration::from_secs(60),
        "a takes the smaller value",
        async || {
            let (a, b) = (cache_a.get(&1).await, cache_b.get(&1).await);
            matches!((a, b), (Some(a), Some(b))
                if a.iter().count() == 12 && a.iter().eq(b.iter()))
        },
    )
    .await;

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

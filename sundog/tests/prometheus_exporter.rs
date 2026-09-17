//! The `prometheus` feature installs a real Prometheus recorder, either
//! serving `GET /metrics` itself
//! ([`sundog::ClusterBuilder::prometheus_listen`]) or handing back a
//! [`sundog::PrometheusHandle`] for a caller's own HTTP stack
//! ([`sundog::prometheus_handle`]).
//! `metrics::set_global_recorder` is a single process-global slot: whichever
//! test in this binary installs a recorder first wins it, so the second
//! test below tolerates losing that race instead of assuming it always
//! runs first.

#![cfg(all(feature = "prometheus", not(feature = "sim")))]

mod common;

use std::net::{Ipv4Addr, SocketAddr};
use std::num::NonZeroU8;
#[cfg(feature = "spill")]
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use smol_str::SmolStr;
use sundog::crdt::{PnCounter, PnCounterResolver};
use sundog::store::Shard;
use sundog::{CacheError, Cluster, Mode, NodeId};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Reserves a loopback TCP port the way `cluster.rs`'s own
/// `reserve_data_bind_addr` does: probe-bind, read back, then drop it.
async fn reserve_tcp_addr() -> SocketAddr {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind an ephemeral loopback tcp port to reserve a metrics address");
    listener
        .local_addr()
        .expect("a freshly bound tcp listener reports a local address")
}

/// A minimal raw-socket `GET /metrics`. Returns `None` if the listener
/// is not accepting connections yet.
async fn scrape_metrics(addr: SocketAddr) -> Option<String> {
    let mut stream = TcpStream::connect(addr).await.ok()?;
    stream
        .write_all(
            format!("GET /metrics HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .ok()?;
    let mut body = Vec::new();
    stream.read_to_end(&mut body).await.ok()?;
    Some(String::from_utf8_lossy(&body).into_owned())
}

/// A minimal raw-socket `GET` against `path`, returning the status line.
/// `None` if the listener is not accepting connections yet.
async fn scrape_status(addr: SocketAddr, path: &str) -> Option<String> {
    let mut stream = TcpStream::connect(addr).await.ok()?;
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .await
        .ok()?;
    let mut body = Vec::new();
    stream.read_to_end(&mut body).await.ok()?;
    let response = String::from_utf8_lossy(&body).into_owned();
    Some(response.lines().next()?.to_string())
}

/// Finds `metric{label1="value1",label2="value2",...} <number>` in
/// Prometheus text-exposition `body`, tolerant of label ordering and
/// integer-vs-float rendering. Every pair in `labels` must match the same
/// line. A single label, `&[("cache", "x")]`, is ambiguous once more than
/// one series shares that label's value under a different label, such as
/// `sundog_spill_reads_total`'s `outcome` varying per `cache`.
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

/// Every bucket of the [`SKETCH_FILL`]-key fill holds about 20 entries, so
/// with this threshold a mismatch there reconciles through an IBLT sketch.
/// Comfortably under [`PART_MIN_BUCKET`], so this cache's buckets never take
/// the part path.
const SKETCH_MIN_BUCKET: usize = 8;
const SKETCH_FILL: u32 = 20_000;

/// Bucket size past which a mismatch answers with part digests instead of a
/// listing or sketch. Between [`SKETCH_FILL`]'s ~20-entry buckets (which
/// must stay on the bucket-level sketch path) and [`DENSE_BUCKET_COUNT`]
/// (which must exceed it).
const PART_MIN_BUCKET: usize = 100;
/// Keys concentrated into one bucket via [`keys_in_one_bucket`], dense
/// enough to clear [`PART_MIN_BUCKET`] while each of its 64 parts (~3
/// entries apiece) stays well under [`SKETCH_MIN_BUCKET`], so a part
/// mismatch there answers with a listing.
const DENSE_BUCKET_COUNT: usize = 200;

fn node_config(gossip_bind_addr: SocketAddr) -> sundog::ClusterConfig {
    common::fast_config().with(|c| {
        c.gossip_bind_addr = gossip_bind_addr;
        c.ae_sketch_min_bucket = SKETCH_MIN_BUCKET;
        c.ae_part_min_bucket = PART_MIN_BUCKET;
        // `crdt_compact_task` ticks at `(crdt_retire_after / 4).max(30s)`;
        // shrinking `crdt_retire_after` well under 120s pins that cadence to
        // its 30s floor instead of the default's six hours. No cache opened
        // in this file besides `seed_crdt_compaction_metrics`'s `counters`
        // ever merges, so this is otherwise inert.
        c.crdt_retire_after = Duration::from_secs(1);
    })
}

/// The anti-entropy bucket a `u32` key hashes into, mirroring
/// `store::stripe_index_from_hash`'s formula; `cluster.rs`'s and
/// `tests/sim.rs`'s own tests carry the identical helper for the same
/// reason.
fn bucket_of(key: u32) -> u16 {
    let bytes = postcard::to_stdvec(&key).expect("u32 key encodes");
    let bucket = xxhash_rust::xxh3::xxh3_64(&bytes) & (sundog::store::BUCKET_COUNT as u64 - 1);
    u16::try_from(bucket).expect("invariant: masked to BUCKET_COUNT - 1, always fits in u16")
}

/// `count` keys guaranteed to land in the same anti-entropy bucket, dense
/// enough to force the part path at [`PART_MIN_BUCKET`] without a
/// uniform-fill key count in the millions.
fn keys_in_one_bucket(count: usize) -> Vec<u32> {
    let target = bucket_of(0);
    (0..)
        .filter(|&k| bucket_of(k) == target)
        .take(count)
        .collect()
}

/// A known hit/miss sequence on a `Mode::Local` cache of its own, exact
/// rather than shared with `users`' traffic: 2 inserts, 3 hit gets, 2 miss
/// gets, one filling `get_or_load`, one hit `get_or_load`, two
/// `contains_key` checks, one `get_or_insert_with` miss and hit, and four
/// concurrent `get_or_load`s of one key: hits=3+1+1+3=8, misses=2+1+1+1=5.
async fn count_hits_and_misses(cluster: &Cluster) {
    let counted = cluster
        .cache::<u32, String>("counted")
        .mode(Mode::Local)
        .open()
        .await
        .expect("cache opens");
    counted.insert(1, "a".into()).await.expect("insert");
    counted.insert(2, "b".into()).await.expect("insert");
    assert_eq!(counted.get(&1).await, Some("a".to_string()));
    assert_eq!(counted.get(&2).await, Some("b".to_string()));
    assert_eq!(counted.get(&1).await, Some("a".to_string()));
    assert_eq!(counted.get(&3).await, None);
    assert_eq!(counted.get(&4).await, None);
    let filled = counted
        .get_or_load(&5, async |_key| {
            Ok::<_, std::convert::Infallible>("loaded".to_string())
        })
        .await
        .expect("loader succeeds");
    assert_eq!(filled, "loaded");
    let cached = counted
        .get_or_load(&5, async |_key| {
            Ok::<_, std::convert::Infallible>("loaded".to_string())
        })
        .await
        .expect("loader succeeds");
    assert_eq!(cached, "loaded");

    // An existence check moves neither counter.
    assert!(counted.contains_key(&1).await);
    assert!(!counted.contains_key(&9).await);
    // get_or_insert_with: one miss to fill, one hit to read back.
    let made = counted
        .get_or_insert_with(&6, async |_key| "made".to_string())
        .await
        .expect("make succeeds");
    assert_eq!(made, "made");
    let kept = counted
        .get_or_insert_with(&6, async |_key| "unused".to_string())
        .await
        .expect("make succeeds");
    assert_eq!(kept, "made");
    // Four concurrent loads of one key collapse into one loader run.
    let loads = futures::future::join_all((0..4).map(|_| {
        let counted = counted.clone();
        async move {
            counted
                .get_or_load(&7, async |_key| {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    Ok::<_, std::convert::Infallible>("joined".to_string())
                })
                .await
                .expect("loader succeeds")
        }
    }))
    .await;
    assert!(loads.iter().all(|value| value == "joined"));
}

/// Drives a genuine `sundog_fan_out_wait_timeouts_total{cache}` increment,
/// and pins `sundog_fan_out_backlog{cache}` at a real nonzero value,
/// through a bare `sundog::store::Shard` with no `Cluster`/mesh involved
/// at all: `cluster::fan_out_task`, the only thing that ever drains a
/// shard's fan-out queue, is spawned by `Cluster`/`Cache`, never by
/// `Shard` itself, so nothing here races this scenario's own two `insert`
/// calls to drain the queue first -- unlike a live cluster's real
/// background fan-out, which would need genuine network backpressure to
/// reproduce the same wait reliably (see `reserve_timeout_pin_metric`'s
/// own doc on why a purely local producer/consumer pair tends not to
/// reproduce a spill-side wait either).
///
/// A `fan_out_backlog_capacity` of 1 and a one-microsecond
/// `fan_out_wait_timeout` force the second `insert` call's wait to time
/// out deterministically, not just probably: `Notify::notified()`'s
/// first poll can never resolve synchronously, since nothing ever
/// notifies before it exists to be notified (unlike a semaphore permit
/// that might already be free), so `tokio::time::timeout` always finds it
/// genuinely pending on the very first poll, and a one-microsecond bound
/// always loses that race. The write still lands regardless, over
/// capacity: two keys queued against a capacity of one.
async fn fan_out_wait_timeout_pin_metrics() {
    let shard = Shard::<u32, String>::new(
        SmolStr::new("fan-out-timeout-pin"),
        Mode::Replicated,
        NodeId::from(1),
        10_000,
        None,
        None,
    )
    .with_fan_out_backlog_capacity(1)
    .with_fan_out_wait_timeout(Duration::from_micros(1));

    shard
        .insert(0, "a".to_string())
        .await
        .expect("first insert fills the backlog to its capacity of one");
    shard
        .insert(1, "b".to_string())
        .await
        .expect("second insert proceeds once its wait times out, over capacity");
}

/// Opens `users` on both `cluster` and `peer`, does one plain insert/remove
/// pair, then a sketch-scale fill with one key dropped on the peer, so the
/// next round finds one bucket mismatched at ~20 entries: past
/// `SKETCH_MIN_BUCKET`, so it reconciles through a decoded IBLT sketch.
async fn seed_sketch_mismatch(cluster: &Cluster, peer: &Cluster) {
    let cache = cluster
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("cache opens");
    let peer_users = peer
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("peer cache opens");
    cache.insert(1, "hello".into()).await.expect("insert");
    cache.remove(&1).await.expect("remove");

    cache
        .insert_many((100..SKETCH_FILL + 100).map(|k| (k, k.to_string())))
        .await
        .expect("bulk insert");
    common::eventually(Duration::from_secs(15), || async {
        peer_users.get(&(SKETCH_FILL + 99)).await.is_some()
    })
    .await;
    peer_users.invalidate_local(&150).await;
}

/// Opens `parts` on both `cluster` and `peer`, densely fills one anti-entropy
/// bucket past `PART_MIN_BUCKET`, then drops one key on the peer: the bucket
/// mismatch answers with part digests, and each mismatched part, far under
/// `SKETCH_MIN_BUCKET`, reconciles through a listing.
async fn seed_part_mismatch(cluster: &Cluster, peer: &Cluster) {
    let parts_cache = cluster
        .cache::<u32, String>("parts")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("parts cache opens");
    let peer_parts = peer
        .cache::<u32, String>("parts")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("peer parts cache opens");
    let dense_keys = keys_in_one_bucket(DENSE_BUCKET_COUNT);
    parts_cache
        .insert_many(dense_keys.iter().map(|&k| (k, k.to_string())))
        .await
        .expect("dense bulk insert");
    let last_dense_key = *dense_keys.last().expect("DENSE_BUCKET_COUNT is nonzero");
    common::eventually(Duration::from_secs(15), || async {
        peer_parts.get(&last_dense_key).await.is_some()
    })
    .await;
    peer_parts
        .invalidate_local(dense_keys.first().expect("DENSE_BUCKET_COUNT is nonzero"))
        .await;
}

/// Opens `delayed` as `Mode::Distributed { owners: 2 }` on a scenario-local
/// donor, joins a scenario-local victim configured with a short
/// `state_transfer_budget`, then kills the donor immediately before the
/// victim opens the same cache: the victim's ownership view still lists the
/// donor as live (gossip has not reacted yet, the same gap
/// `seed_distributed_metrics`'s outcome="error" case relies on), so every
/// bucket pull dials a closed listener, fails at once, and retries until its
/// budget runs out. After `state_transfer::MAX_WARM_UP_ATTEMPTS` such rounds
/// `rebalance::warm_up_task` gives up, opens warm with nothing pulled, and
/// increments `sundog_rebalance_pull_timeouts_total{cache="delayed"}`.
/// Independent of `peer`/`third`/`fourth`, like `seed_crdt_compaction_metrics`:
/// both scenario-local nodes are down by the time this returns.
async fn seed_pull_timeout_metric(gossip_a: SocketAddr, metrics_addr: SocketAddr) {
    let name = "delayed";
    let owners = NonZeroU8::new(2).expect("nonzero");

    let donor = Cluster::builder("it-prometheus-exporter")
        .seeds([gossip_a])
        .config(node_config(common::reserve_gossip_addr().await))
        .build()
        .await
        .expect("donor builds");
    common::wait_for_peer_count(&donor, 1, Duration::from_secs(15)).await;
    donor
        .cache::<u32, String>(name)
        .mode(Mode::Distributed { owners })
        .open()
        .await
        .expect("donor opens delayed as the sole owner, warm at once");

    let victim_config = node_config(common::reserve_gossip_addr().await)
        .with(|c| c.state_transfer_budget = Duration::from_millis(200));
    let victim = Cluster::builder("it-prometheus-exporter")
        .seeds([gossip_a])
        .config(victim_config)
        .build()
        .await
        .expect("victim builds");
    common::wait_for_peer_count(&victim, 1, Duration::from_secs(15)).await;
    // Gossip quiescence: gives the donor's `delayed` cache-mode
    // advertisement time to reach the victim, so the victim's initial
    // ownership view already lists the donor as a co-owner instead of
    // opening alone with nothing to pull.
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Closes the donor's listeners at once; the victim's ownership view
    // still lists it as live, so every pull dials it and fails fast rather
    // than ever finding a real donor to warm from.
    donor.shutdown().await;

    let _victim_cache = victim
        .cache::<u32, String>(name)
        .mode(Mode::Distributed { owners })
        .open()
        .await
        .expect("victim opens cold; a timed-out pull never fails open()");

    common::eventually(Duration::from_secs(15), || async {
        scrape_metrics(metrics_addr).await.is_some_and(|body| {
            scraped_metric_value(
                &body,
                "sundog_rebalance_pull_timeouts_total",
                &[("cache", name)],
            )
            .is_some_and(|count| count >= 1.0)
        })
    })
    .await;

    victim.shutdown().await;
}

/// Drives one real writer through CRDT retirement, rejoining
/// `second_life` under `first_life`'s identical explicit
/// [`sundog::ClusterBuilder::node_id`] with a fresh incarnation so
/// `membership::incarnation_is_dead` marks `first_life`'s writer dead, the
/// one trigger for a real `cluster::crdt_compact_task` pass reachable from
/// a black-box integration test (a graceful [`Cluster::shutdown`] never
/// enters absence tracking at all). `sundog_crdt_retired_writers_total{cache="counters"}`
/// settles at exactly `1` once stage-one retirement runs, while
/// `sundog_crdt_compactions_total` reads `1` or `2` depending on whether
/// stage two's later record rewrite has landed by the time the caller
/// scrapes.
async fn seed_crdt_compaction_metrics(
    cluster: &Cluster,
    gossip_a: SocketAddr,
    metrics_addr: SocketAddr,
) {
    let name = "counters";
    let restart_id = NodeId::random();

    let cluster_counters = cluster
        .cache::<u32, PnCounter>(name)
        .mode(Mode::Replicated)
        .resolver(Arc::new(PnCounterResolver))
        .open()
        .await
        .expect("cluster opens counters");

    let first_life = Cluster::builder("it-prometheus-exporter")
        .seeds([gossip_a])
        .config(node_config(common::reserve_gossip_addr().await))
        .node_id(restart_id)
        .build()
        .await
        .expect("first-life node builds");
    common::wait_for_peer_count(&first_life, 1, Duration::from_secs(15)).await;
    let first_life_counters = first_life
        .cache::<u32, PnCounter>(name)
        .mode(Mode::Replicated)
        .resolver(Arc::new(PnCounterResolver))
        .open()
        .await
        .expect("first-life opens counters");
    let dead_writer = first_life_counters.writer_id();
    first_life_counters
        .merge(1, PnCounter::local_delta(dead_writer, 7))
        .await
        .expect("first-life contributes under its own writer id");

    common::eventually(Duration::from_secs(15), || async {
        cluster_counters
            .get(&1)
            .await
            .is_some_and(|c| c.value() == 7)
    })
    .await;

    // A graceful departure never enters absence tracking (see this fn's own
    // doc), so the writer stays alive in `cluster`'s view until this same
    // node identity returns under a fresh incarnation below.
    first_life.shutdown().await;

    let second_life = Cluster::builder("it-prometheus-exporter")
        .seeds([gossip_a])
        .config(node_config(common::reserve_gossip_addr().await))
        .node_id(restart_id)
        .build()
        .await
        .expect("second-life node builds");
    let second_life_counters = second_life
        .cache::<u32, PnCounter>(name)
        .mode(Mode::Replicated)
        .resolver(Arc::new(PnCounterResolver))
        .open()
        .await
        .expect("second-life opens counters");
    let fresh_incarnation = second_life_counters.writer_id().incarnation();

    common::eventually(Duration::from_secs(15), || async {
        cluster
            .peers()
            .iter()
            .any(|p| p.node == restart_id && p.incarnation == fresh_incarnation)
    })
    .await;

    // `crdt_compact_task` ticks at most every 30s (its own cadence floor,
    // see `node_config`'s comment above). Give the next real tick after the
    // incarnation mismatch above becomes visible ample room to land.
    common::eventually(Duration::from_secs(120), || async {
        scrape_metrics(metrics_addr).await.is_some_and(|body| {
            scraped_metric_value(
                &body,
                "sundog_crdt_retired_writers_total",
                &[("cache", name)],
            )
            .is_some_and(|count| count >= 1.0)
        })
    })
    .await;

    assert_eq!(
        cluster_counters.get(&1).await.map(|c| c.value()),
        Some(7),
        "stage-one retirement moves a dead writer's slot without changing the merged value"
    );

    second_life.shutdown().await;
}

/// Opens `prices` as `Mode::Distributed { owners: 2 }` across `cluster`,
/// `peer`, and two more nodes joined for this scenario, driving every
/// `sundog_fetch_total` outcome, a forwarded write, and both
/// `sundog_rebalance_buckets_total` directions on `cluster` itself, the
/// only node whose metrics this test scrapes. Folded into the main test
/// rather than its own `#[tokio::test]`, for the same process-global
/// recorder reason `spill_writes_and_promotes_pin_metrics` is.
#[allow(clippy::too_many_lines, reason = "one scripted end-to-end scenario")]
async fn seed_distributed_metrics(cluster: &Cluster, peer: &Cluster, gossip_a: SocketAddr) {
    let owners = NonZeroU8::new(2).expect("nonzero");
    let name = "prices";

    // `peer` and a fresh third node stabilize a two-way distributed cache
    // (both own everything with only two eligible nodes) before `cluster`
    // ever opens it, so `cluster`'s own open() pulls a real share of the
    // 1,024 buckets from a real donor: the open()-time initial pull is
    // `sundog_rebalance_buckets_total{direction="in"}`'s natural trigger.
    let third = Cluster::builder("it-prometheus-exporter")
        .seeds([gossip_a])
        .config(node_config(common::reserve_gossip_addr().await))
        .build()
        .await
        .expect("third node builds");
    common::wait_for_peer_count(&third, 1, Duration::from_secs(15)).await;
    // Both real peers, not only third's own view of the mesh: cluster's
    // own membership must include third before it ever opens prices.
    common::wait_for_peer_count(cluster, 2, Duration::from_secs(15)).await;

    let peer_prices = peer
        .cache::<u32, String>(name)
        .mode(Mode::Distributed { owners })
        .open()
        .await
        .expect("peer opens prices");
    let third_prices = third
        .cache::<u32, String>(name)
        .mode(Mode::Distributed { owners })
        .open()
        .await
        .expect("third opens prices");
    peer_prices
        .insert_many((0..500u32).map(|k| (k, k.to_string())))
        .await
        .expect("peer fills prices before cluster ever opens it");
    common::eventually(Duration::from_secs(15), || async {
        third_prices.entry_count().await == 500
    })
    .await;

    // A short, documented quiescence window: gossip needs a couple of
    // rounds past peer liveness to also carry peer's and third's cache-mode
    // advertisements to `cluster`, so `cluster`'s very first computed view
    // already reflects both real co-owners instead of transiently
    // believing itself the sole one.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let cluster_prices = cluster
        .cache::<u32, String>(name)
        .mode(Mode::Distributed { owners })
        .open()
        .await
        .expect("cluster opens prices, pulling its share from peer/third");

    // outcome="local": a key cluster owns after its view converges.
    let owned_key = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(k) =
                (0..500u32).find(|&k| cluster_prices.owners_of(&k).contains(&cluster.node_id()))
            {
                return k;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("cluster owns at least one of the 500 keys");
    // Owning a bucket per the current view and already holding its data
    // are separate facts: the initial pull for this particular bucket may
    // still be in flight, or the round it landed on may have declined and
    // await a retry, so this polls rather than asserting the very first
    // fetch.
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Ok(Some(value)) = cluster_prices.fetch(&owned_key).await
                && value == owned_key.to_string()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("outcome=local eventually reads the owned key's value");

    // outcome="remote": cluster reads an unowned key's value back over the
    // wire from a real owner, proof the open()-time pull, or a live fetch
    // to peer/third, delivers it.
    let unowned_key = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(k) =
                (0..500u32).find(|&k| !cluster_prices.owners_of(&k).contains(&cluster.node_id()))
            {
                return k;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("cluster's view excludes it from at least one of the 500 keys");
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Ok(Some(value)) = cluster_prices.fetch(&unowned_key).await
                && value == unowned_key.to_string()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("outcome=remote reaches a real owner");

    // outcome="miss": a key nobody ever wrote, in a bucket cluster still
    // does not own.
    let miss_key = (10_000..20_000u32)
        .find(|&k| !cluster_prices.owners_of(&k).contains(&cluster.node_id()))
        .expect("some never-written key's bucket excludes cluster too");
    assert_eq!(
        cluster_prices
            .fetch(&miss_key)
            .await
            .expect("fetch succeeds"),
        None,
        "outcome=miss"
    );

    // sundog_forwarded_writes_total: a write through cluster for a key it
    // does not own forwards rather than ever applying locally.
    cluster_prices
        .insert(unowned_key, "forwarded".to_string())
        .await
        .expect("insert forwards");
    assert_eq!(
        cluster_prices.get(&unowned_key).await,
        None,
        "a forwarded write never lands locally on cluster"
    );

    // sundog_rebalance_buckets_total{direction="out"}: a fourth node joins
    // and, for at least one of cluster's owned buckets, displaces it;
    // cluster releases that bucket once the disown grace elapses.
    let owned_before: Vec<u32> = (0..500u32)
        .filter(|&k| cluster_prices.owners_of(&k).contains(&cluster.node_id()))
        .collect();
    let fourth = Cluster::builder("it-prometheus-exporter")
        .seeds([gossip_a])
        .config(node_config(common::reserve_gossip_addr().await))
        .build()
        .await
        .expect("fourth node builds");
    let _fourth_prices = fourth
        .cache::<u32, String>(name)
        .mode(Mode::Distributed { owners })
        .open()
        .await
        .expect("fourth opens prices");
    let displaced_key = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(&k) = owned_before
                .iter()
                .find(|&&k| !cluster_prices.owners_of(&k).contains(&cluster.node_id()))
            {
                return k;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("fourth's arrival displaces cluster from at least one bucket it held before");
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if cluster_prices.get(&displaced_key).await.is_none() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("cluster releases the displaced bucket once the disown grace elapses");

    // outcome="error": every real owner of some key cluster never owned
    // goes down; both dials fail before gossip has time to react.
    let error_key = (20_000..30_000u32)
        .find(|&k| !cluster_prices.owners_of(&k).contains(&cluster.node_id()))
        .expect("some key's bucket excludes cluster among four real nodes");
    let error_owners = cluster_prices.owners_of(&error_key);
    for down in [peer.clone(), third.clone(), fourth.clone()] {
        if error_owners.contains(&down.node_id()) {
            down.shutdown().await;
        }
    }
    assert!(
        matches!(
            cluster_prices.fetch(&error_key).await,
            Err(CacheError::FetchUnavailable { .. })
        ),
        "outcome=error"
    );
}

#[allow(
    clippy::too_many_lines,
    reason = "folds in the spill metrics pin behind feature = \"spill\"; see \
              spill_writes_and_promotes_pin_metrics's own doc for why it can't be a separate \
              #[tokio::test]"
)]
// Multi-threaded, matching `spill_replication.rs`'s own reservation/timeout
// scenarios: `reserve_timeout_pin_metric`'s genuine `SpillWaitTimedOut` race
// needs the flusher's dedicated OS thread and this test's own tokio tasks
// to run with real parallelism. On a single-threaded runtime, this test's
// one worker thread sharing time with every other scenario's background
// work (gossip, anti-entropy, other clusters still open from earlier in
// this same function) tends to give the flusher enough real wall-clock
// time between polls to keep `admit` refilled, so the race that scenario
// depends on rarely triggers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metrics_endpoint_serves_sundog_metrics_after_cache_ops() {
    let metrics_addr = reserve_tcp_addr().await;
    let gossip_a = common::reserve_gossip_addr().await;
    let gossip_b = common::reserve_gossip_addr().await;

    let cluster = Cluster::builder("it-prometheus-exporter")
        .seeds([gossip_b])
        .config(node_config(gossip_a))
        .prometheus_listen(metrics_addr)
        .build()
        .await
        .expect("cluster builds with a prometheus listener");
    // A peer for `users` to replicate to and reconcile against; only the
    // first node serves metrics, since the recorder is process-global.
    let peer = Cluster::builder("it-prometheus-exporter")
        .seeds([gossip_a])
        .config(node_config(gossip_b))
        .build()
        .await
        .expect("peer builds");
    common::wait_for_peer_count(&cluster, 1, Duration::from_secs(15)).await;
    common::wait_for_peer_count(&peer, 1, Duration::from_secs(15)).await;

    seed_sketch_mismatch(&cluster, &peer).await;
    seed_part_mismatch(&cluster, &peer).await;
    count_hits_and_misses(&cluster).await;
    fan_out_wait_timeout_pin_metrics().await;
    #[cfg(feature = "spill")]
    let mut spill_dirs = spill_writes_and_promotes_pin_metrics(&cluster).await;
    #[cfg(feature = "spill")]
    let (disk_error_immutable, disk_error_dropped) = {
        let (dir, immutable, observed) =
            disk_error_and_reserve_wait_pin_metrics(&cluster, metrics_addr).await;
        spill_dirs.push(dir);
        (immutable, observed)
    };
    #[cfg(feature = "spill")]
    {
        spill_dirs.push(reserve_timeout_pin_metric().await);
    }
    // Independent of `peer`/`third`/`fourth`: creates and fully retires its
    // own two scenario-local nodes before returning, so it leaves no peer
    // count `seed_distributed_metrics` below needs to account for.
    seed_crdt_compaction_metrics(&cluster, gossip_a, metrics_addr).await;
    // Also independent, for the same reason: its donor and victim are both
    // shut down before it returns.
    seed_pull_timeout_metric(gossip_a, metrics_addr).await;
    // Runs last: it shuts down two of its own scenario-local nodes once it
    // is done with them, and `peer` isn't touched by anything after it.
    seed_distributed_metrics(&cluster, &peer, gossip_a).await;

    // `sundog_open_caches` comes from a periodic background routine and the
    // sketch/parts counters from an anti-entropy round, so poll until every
    // metric checked below has been published once.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let body = loop {
        if let Some(body) = scrape_metrics(metrics_addr).await
            && body.contains("sundog_open_caches")
            && body.contains("sundog_live_peers")
            && scraped_metric_value(&body, "sundog_ae_parts_total", &[("outcome", "listing")])
                .is_some()
            && body.contains("sundog_cache_entries")
            && scraped_metric_value(&body, "sundog_ae_sketch_total", &[("outcome", "decoded")])
                .is_some()
            && (cfg!(not(feature = "spill"))
                || scraped_metric_value(
                    &body,
                    "sundog_spill_writes_total",
                    &[("cache", "spilled")],
                )
                .is_some())
        {
            break body;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "metrics endpoint never served sundog_open_caches, sundog_live_peers, \
             sundog_cache_entries, a decoded sundog_ae_sketch_total, and (feature = \"spill\") \
             sundog_spill_writes_total within the bound"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    assert_eq!(
        scraped_metric_value(&body, "sundog_cache_hits_total", &[("cache", "counted")]),
        Some(8.0),
        "expected 8 hits on the 'counted' cache; got body:\n{body}"
    );
    assert_eq!(
        scraped_metric_value(&body, "sundog_cache_misses_total", &[("cache", "counted")]),
        Some(5.0),
        "expected 5 misses on the 'counted' cache; got body:\n{body}"
    );
    assert!(
        scraped_metric_value(&body, "sundog_cache_entries", &[("cache", "counted")]).is_some(),
        "expected a sundog_cache_entries line for the 'counted' cache; got body:\n{body}"
    );

    // sundog_fan_out_wait_timeouts_total / sundog_fan_out_backlog: pinned by
    // `fan_out_wait_timeout_pin_metrics`'s bare-`Shard` scenario -- one
    // genuine, deterministic wait timeout, and two keys left sitting in the
    // backlog against a configured capacity of one, since nothing ever
    // drains a `Shard` with no `Cluster` behind it.
    assert!(
        scraped_metric_value(
            &body,
            "sundog_fan_out_wait_timeouts_total",
            &[("cache", "fan-out-timeout-pin")]
        )
        .is_some_and(|count| count >= 1.0),
        "expected at least one genuine fan-out wait timeout on the 'fan-out-timeout-pin' \
         cache; got body:\n{body}"
    );
    assert_eq!(
        scraped_metric_value(
            &body,
            "sundog_fan_out_backlog",
            &[("cache", "fan-out-timeout-pin")]
        ),
        Some(2.0),
        "both keys landed over the configured capacity of one, with nothing to drain them; \
         got body:\n{body}"
    );
    assert!(
        scraped_metric_value(&body, "sundog_ae_sketch_total", &[("cache", "users")])
            .is_some_and(|decoded| decoded >= 1.0),
        "expected at least one decoded sketch on the 'users' cache; got body:\n{body}"
    );
    assert!(
        scraped_metric_value(&body, "sundog_ae_parts_total", &[("cache", "parts")])
            .is_some_and(|listings| listings >= 1.0),
        "expected at least one part listing on the 'parts' cache; got body:\n{body}"
    );

    assert!(
        scraped_metric_value(&body, "sundog_owned_buckets", &[("cache", "prices")])
            .is_some_and(|owned| owned > 0.0),
        "expected a positive sundog_owned_buckets for the distributed 'prices' cache; got \
         body:\n{body}"
    );
    for outcome in ["local", "remote", "miss", "error"] {
        assert!(
            scraped_metric_value(
                &body,
                "sundog_fetch_total",
                &[("cache", "prices"), ("outcome", outcome)]
            )
            .is_some_and(|count| count >= 1.0),
            "expected sundog_fetch_total{{cache=\"prices\",outcome=\"{outcome}\"}} >= 1; got \
             body:\n{body}"
        );
    }
    assert!(
        scraped_metric_value(
            &body,
            "sundog_forwarded_writes_total",
            &[("cache", "prices")]
        )
        .is_some_and(|count| count >= 1.0),
        "expected at least one forwarded write on the 'prices' cache; got body:\n{body}"
    );
    for direction in ["in", "out"] {
        assert!(
            scraped_metric_value(
                &body,
                "sundog_rebalance_buckets_total",
                &[("cache", "prices"), ("direction", direction)]
            )
            .is_some_and(|count| count >= 1.0),
            "expected sundog_rebalance_buckets_total{{cache=\"prices\",direction=\"{direction}\"}} \
             >= 1; got body:\n{body}"
        );
    }

    // `seed_pull_timeout_metric`'s victim gives up on exactly one warm-up
    // pass, ever, once its ownership view moves on and its cache is warm.
    assert_eq!(
        scraped_metric_value(
            &body,
            "sundog_rebalance_pull_timeouts_total",
            &[("cache", "delayed")]
        ),
        Some(1.0),
        "expected exactly one rebalance pull timeout on the 'delayed' cache; got body:\n{body}"
    );

    // `seed_crdt_compaction_metrics` retires exactly one dead writer, ever
    // (stage-one retirement never calls `retire` on it again once moved),
    // so this pins exactly. `sundog_crdt_compactions_total` only gets a
    // lower bound: that same writer's retired slot may or may not have
    // already aged into a second, real stage-two fold by the time this
    // scrape lands (see `seed_crdt_compaction_metrics`'s own doc).
    assert_eq!(
        scraped_metric_value(
            &body,
            "sundog_crdt_retired_writers_total",
            &[("cache", "counters")]
        ),
        Some(1.0),
        "expected exactly one retired writer on the 'counters' cache; got body:\n{body}"
    );
    assert!(
        scraped_metric_value(
            &body,
            "sundog_crdt_compactions_total",
            &[("cache", "counters")]
        )
        .is_some_and(|count| count >= 1.0),
        "expected at least one crdt compaction on the 'counters' cache; got body:\n{body}"
    );

    #[cfg(feature = "spill")]
    {
        assert_eq!(
            scraped_metric_value(&body, "sundog_spill_writes_total", &[("cache", "spilled")]),
            Some(1.0),
            "expected exactly one spill install; got body:\n{body}"
        );
        assert_eq!(
            scraped_metric_value(
                &body,
                "sundog_spill_reads_total",
                &[("cache", "spilled"), ("outcome", "hit")]
            ),
            Some(1.0),
            "expected exactly one disk hit; got body:\n{body}"
        );
        assert_eq!(
            scraped_metric_value(
                &body,
                "sundog_spill_promotions_total",
                &[("cache", "spilled")]
            ),
            Some(1.0),
            "expected exactly one promotion; got body:\n{body}"
        );
        assert_eq!(
            scraped_metric_value(&body, "sundog_spill_entries", &[("cache", "spilled")]),
            Some(0.0),
            "the promoted key is resident again, so zero currently-spilled entries remain; \
             got body:\n{body}"
        );

        // An overwrite of a spilled key decrements sundog_spill_entries the
        // same way a promotion does, via apply_put rather than a disk read.
        assert_eq!(
            scraped_metric_value(
                &body,
                "sundog_spill_writes_total",
                &[("cache", "spill-overwrite")]
            ),
            Some(1.0),
            "the overwrite itself never spills anything new; got body:\n{body}"
        );
        assert_eq!(
            scraped_metric_value(
                &body,
                "sundog_spill_entries",
                &[("cache", "spill-overwrite")]
            ),
            Some(0.0),
            "the overwritten key is resident again, so zero currently-spilled entries remain; \
             got body:\n{body}"
        );

        // A remove of a spilled key decrements sundog_spill_entries via
        // apply_tombstone.
        assert_eq!(
            scraped_metric_value(
                &body,
                "sundog_spill_writes_total",
                &[("cache", "spill-remove")]
            ),
            Some(1.0),
            "the remove itself never spills anything new; got body:\n{body}"
        );
        assert_eq!(
            scraped_metric_value(&body, "sundog_spill_entries", &[("cache", "spill-remove")]),
            Some(0.0),
            "the removed key is gone, so zero currently-spilled entries remain; got body:\n{body}"
        );

        // `disk_error_and_reserve_wait_pin_metrics`'s `insert_many` call
        // threads exactly one `Reservation` through `apply_grouped` ->
        // `reserve()`, resolved against an otherwise-empty flush queue: no
        // real wait, no timeout, and the waiter gauge back at zero once
        // the call returns.
        assert_eq!(
            scraped_metric_value(
                &body,
                "sundog_spill_wait_seconds_total",
                &[("cache", "disk-error-pin")]
            ),
            Some(0.0),
            "reserve() resolved against an empty queue, so this whole-second counter has \
             nothing to accumulate yet; got body:\n{body}"
        );
        assert_eq!(
            scraped_metric_value(
                &body,
                "sundog_spill_waiters",
                &[("cache", "disk-error-pin")]
            ),
            Some(0.0),
            "the RAII guard decrements sundog_spill_waiters back to zero once reserve() \
             resolves; got body:\n{body}"
        );
        // sundog_spill_wait_timeouts_total is pinned by
        // `reserve_tracks_waiters_wait_seconds_and_wait_timeouts`
        // (spill.rs) against a genuine timeout, driven through the
        // crate-internal `pause_flusher` hook this external test cannot
        // reach: `admit`'s bytes free the instant the flusher *dequeues* a
        // job, before its write even starts (`flusher_loop`'s own
        // doc), so this particular tiny, two-entry scenario's own
        // `flush_queue_bytes` never comes close to saturating within it.
        // A `metrics::Counter` series exists only once something
        // increments it, so a scenario that never times out correctly
        // never registers this series at all -- this is that expected
        // absence for *this* scenario, not a gap in coverage:
        // `reserve_timeout_pin_metric`, below, pins a genuine nonzero
        // count on its own differently-shaped cache.
        assert_eq!(
            scraped_metric_value(
                &body,
                "sundog_spill_wait_timeouts_total",
                &[("cache", "disk-error-pin")]
            ),
            None,
            "reserve() resolved well inside its own timeout, so this series was never \
             registered; got body:\n{body}"
        );
        // `reserve_timeout_pin_metric`'s own scenario: a real donor/joiner
        // state-transfer pull against a tiny `flush_queue_bytes` and an
        // absurdly short `spill_wait_timeout` (1 microsecond) forces at
        // least one `ShardOps::apply_remote_batch` reservation or
        // `Shard::retry_reservation_deficit` retry call to lose its race
        // against a genuine wait -- a real, nonzero count pinned through
        // the actual Prometheus text-exposition path, not just an absence
        // check.
        assert!(
            scraped_metric_value(
                &body,
                "sundog_spill_wait_timeouts_total",
                &[("cache", "reserve-timeout-pin")]
            )
            .is_some_and(|count| count >= 1.0),
            "expected at least one genuine reserve() timeout on the 'reserve-timeout-pin' \
             cache; got body:\n{body}"
        );

        // disk_error: only pinned when `chattr_dir_entries` could actually
        // set up the fault (root or passwordless sudo, on an ext2/3/4
        // filesystem); elsewhere this is skipped rather than failing the
        // whole scenario, the same tolerance this file already extends to
        // losing the process-global recorder race. Once the fault *is* set
        // up, `None` is a real failure, not tolerated.
        if disk_error_immutable {
            assert!(
                disk_error_dropped.is_some_and(|count| count >= 1.0),
                "the evictions insert_many forces land in the region chattr_dir_entries made \
                 immutable, so every job in the failing segment counts as a disk_error drop; \
                 how many jobs share that segment depends on flusher timing, so at least one; \
                 got body:\n{body}"
            );
        }
    }

    // sundog_fan_out_wait_seconds_total{peer}: `net::Mesh::send_frames_awaiting`
    // only ever bumps this when a real per-peer outbox stays full past a
    // whole `FAN_OUT_SEND_DEADLINE` (2s) slice while the peer is still
    // live -- every real peer this whole scenario ever talks to (`peer`,
    // `third`, `fourth`) drains its own inbound traffic far faster than
    // that on loopback with no artificial slowdown, the same reason
    // `disk_error_and_reserve_wait_pin_metrics`'s tiny scenario never
    // registers `sundog_spill_wait_timeouts_total` either: reproducing a
    // genuine multi-second mesh stall needs a deliberately slow receiver
    // (a real network round trip, or a receiver-side admission wait), not
    // just a small buffer, so this series is correctly never registered
    // here. `send_frames_awaiting_waits_past_one_deadline_for_a_still_live_peer`
    // and `send_frames_awaiting_drops_once_the_peer_leaves_the_table_mid_wait`
    // (net/mod.rs) pin this counter's actual increment in-process instead,
    // against a mesh outbox held full by construction.
    assert!(
        !body.contains("sundog_fan_out_wait_seconds_total"),
        "no peer in this scenario ever stalls its outbox for a whole deadline slice; got \
         body:\n{body}"
    );

    // `users` warmed during `seed_sketch_mismatch` above: `is_ready()` and
    // `/readyz` on the same listener must both already agree.
    assert!(cluster.is_ready(), "the open Replicated caches are warm");
    let readyz_status = scrape_status(metrics_addr, "/readyz")
        .await
        .expect("readyz answers once the cluster is up");
    assert!(
        readyz_status.contains("200"),
        "expected /readyz to answer 200 once warm; got {readyz_status}"
    );
    let healthz_status = scrape_status(metrics_addr, "/healthz")
        .await
        .expect("healthz answers once the cluster is up");
    assert!(
        healthz_status.contains("200"),
        "expected /healthz to always answer 200 while the process serves; got {healthz_status}"
    );

    peer.shutdown().await;
    cluster.shutdown().await;
    #[cfg(feature = "spill")]
    for dir in &spill_dirs {
        let _ = std::fs::remove_dir_all(dir);
    }
}

/// [`sundog::prometheus_handle`], the no-listener install, for a caller that
/// serves `/metrics` from its own HTTP stack. Whichever test in this binary
/// installs the process-global recorder first wins it: if the recorder
/// above already claimed the slot, `prometheus_handle` still runs (and this
/// test still exercises it), it cannot hand back a usable handle, so
/// the render-based assertion below only applies when this test wins the
/// race.
#[tokio::test]
async fn prometheus_handle_exposes_cache_hits_without_a_listener() {
    let cluster = Cluster::builder("it-prometheus-handle")
        .seeds(std::iter::empty())
        .config(common::fast_config())
        .build()
        .await
        .expect("cluster builds");

    let cache = cluster
        .cache::<u32, String>("handle-metrics")
        .mode(Mode::Local)
        .open()
        .await
        .expect("cache opens");
    cache.insert(1, "a".into()).await.expect("insert");
    assert_eq!(cache.get(&1).await, Some("a".to_string()));

    if let Ok(handle) = sundog::prometheus_handle() {
        let body = handle.render();
        assert!(
            body.contains("sundog_cache_hits_total"),
            "prometheus_handle's own recorder captures cache hits; got body:\n{body}"
        );
    }
    // Else: another test in this binary installed the process-global
    // recorder first; there is no handle to render from here.

    cluster.shutdown().await;
}

/// A directory path under the OS temp dir, unique to this test process,
/// call, and `label`. Never created on disk; [`sundog::SpillConfig::new`]'s
/// `SpillTier::open` creates it.
#[cfg(feature = "spill")]
fn fresh_spill_dir(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "sundog-it-prometheus-spill-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is after the unix epoch")
            .as_nanos()
    ))
}

/// The spill metrics pin: a tiny `max_capacity` plus a `spill` tier on
/// `cluster` forces one eviction-to-spill, and a single `get` of the
/// spilled key forces one disk hit and one promotion. Two further,
/// isolated caches then pin an overwrite-after-spill and a
/// remove-after-spill, each sized so the one operation under test settles
/// without triggering a second eviction of its own. Batch eviction can
/// otherwise clear more than one unit of weight per pass, which would make
/// an exact count depend on internal batching details rather than on the
/// behavior under test.
///
/// Runs inside `metrics_endpoint_serves_sundog_metrics_after_cache_ops`
/// rather than as its own `#[tokio::test]`, for the same reason
/// `seed_sketch_mismatch`/`seed_part_mismatch`/`count_hits_and_misses`
/// above do: a cache's `hits`/`misses`-style `metrics::Counter` handles
/// bind to whichever recorder is installed at the moment
/// `Shard::new`/`Shard::attach_spill` calls `metrics::counter!`, not
/// whatever gets installed later. Only the one test in this binary that
/// reliably owns the process-global recorder from the start, via
/// `prometheus_listen` synchronously early in `Cluster::builder(..).build()`,
/// can pin exact metric values. A second, independent `#[tokio::test]`
/// racing for the same slot either is a no-op, if it loses, or breaks the
/// first test's own `build()`, if it wins.
///
/// Returns every tier's scratch directory for the caller to clean up.
#[allow(
    clippy::too_many_lines,
    reason = "three isolated cache scenarios (promote, overwrite, remove), each with its own \
              setup and bounded wait, read better inline than split across helpers that would \
              each retake the same handful of parameters"
)]
#[cfg(feature = "spill")]
async fn spill_writes_and_promotes_pin_metrics(cluster: &Cluster) -> Vec<std::path::PathBuf> {
    let mut dirs = Vec::new();

    // --- "spilled": one eviction-to-spill, one disk-read promotion. ---
    let dir = fresh_spill_dir("promote");
    let cfg = sundog::SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let cache = cluster
        .cache::<u32, String>("spilled")
        .mode(Mode::Local)
        .max_capacity(1)
        .spill(cfg)
        .open()
        .await
        .expect("cache opens");

    cache.insert(1, "one".to_string()).await.expect("insert 1");
    cache.insert(2, "two".to_string()).await.expect("insert 2");
    common::eventually(Duration::from_secs(5), || async {
        cache.get_sync(&1).is_none() || cache.get_sync(&2).is_none()
    })
    .await;
    let spilled_key = if cache.get_sync(&1).is_none() {
        1u32
    } else {
        2u32
    };

    // One promotion: a single disk read of the spilled key.
    let _ = cache.get(&spilled_key).await;
    dirs.push(dir);

    // --- "spill-overwrite": an overwrite of a currently-spilled key must
    // decrement sundog_spill_entries with no further disk write. `1`/`2`/`3`
    // fill a `max_capacity(2)` cache past its limit, spilling one of them;
    // removing one of the two others first frees the one unit of headroom
    // the overwrite below needs, so it settles at the cap instead of
    // forcing a second eviction.
    let dir = fresh_spill_dir("overwrite");
    let cfg = sundog::SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let cache = cluster
        .cache::<u32, String>("spill-overwrite")
        .mode(Mode::Local)
        .max_capacity(2)
        .spill(cfg)
        .open()
        .await
        .expect("cache opens");

    cache.insert(1, "one".to_string()).await.expect("insert 1");
    cache.insert(2, "two".to_string()).await.expect("insert 2");
    cache
        .insert(3, "three".to_string())
        .await
        .expect("insert 3");
    common::eventually(Duration::from_secs(5), || async {
        [1u32, 2, 3]
            .into_iter()
            .any(|k| cache.get_sync(&k).is_none())
    })
    .await;
    let spilled_key = [1u32, 2, 3]
        .into_iter()
        .find(|&k| cache.get_sync(&k).is_none())
        .expect("exactly one of the three keys spilled");
    let other_resident_key = [1u32, 2, 3]
        .into_iter()
        .find(|&k| k != spilled_key)
        .expect("the other two keys stay resident");
    cache
        .remove(&other_resident_key)
        .await
        .expect("free headroom for the overwrite below");
    cache
        .insert(spilled_key, "overwritten".to_string())
        .await
        .expect("overwrite the spilled key");
    dirs.push(dir);

    // --- "spill-remove": removing a currently-spilled key must decrement
    // sundog_spill_entries too, via apply_tombstone rather than apply_put.
    let dir = fresh_spill_dir("remove");
    let cfg = sundog::SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let cache = cluster
        .cache::<u32, String>("spill-remove")
        .mode(Mode::Local)
        .max_capacity(1)
        .spill(cfg)
        .open()
        .await
        .expect("cache opens");

    cache
        .insert(10, "ten".to_string())
        .await
        .expect("insert 10");
    cache
        .insert(11, "eleven".to_string())
        .await
        .expect("insert 11");
    common::eventually(Duration::from_secs(5), || async {
        cache.get_sync(&10).is_none() || cache.get_sync(&11).is_none()
    })
    .await;
    let spilled_key = if cache.get_sync(&10).is_none() {
        10u32
    } else {
        11u32
    };
    cache
        .remove(&spilled_key)
        .await
        .expect("remove the spilled key");
    dirs.push(dir);

    dirs
}

/// Best-effort: makes every entry directly under `dir` immutable
/// (`chattr +i`), or mutable again (`chattr -i`) on the reverse call. The
/// immutable attribute is checked by ext2/3/4 on every write syscall, not
/// only at `open()` the way permission bits are, so it makes a `pwrite` on
/// an already-open write handle fail with `EPERM` even though the handle
/// was opened, and the file preallocated, before this runs --
/// `SpillTier::open`'s own region files, specifically. Requires
/// `CAP_LINUX_IMMUTABLE` (root, or passwordless `sudo`) and an
/// ext2/3/4-family filesystem; returns `false` without changing anything
/// when either is unavailable, so a caller can skip whatever real
/// disk-error scenario this was meant to set up instead of failing
/// outright.
#[cfg(feature = "spill")]
fn chattr_dir_entries(dir: &std::path::Path, immutable: bool) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    let paths: Vec<_> = entries
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .collect();
    if paths.is_empty() {
        return false;
    }
    let flag = if immutable { "+i" } else { "-i" };
    let run = |program: &str, prefix: &[&str]| {
        Command::new(program)
            .args(prefix)
            .arg(flag)
            .args(&paths)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
    };
    run("chattr", &[]) || run("sudo", &["-n", "chattr"])
}

/// Exercises the `reserve()`-backed metrics this workstream adds --
/// `sundog_spill_wait_seconds_total`, `sundog_spill_waiters`,
/// `sundog_spill_wait_timeouts_total` -- via `insert_many`, the one
/// producer reachable through the public API that threads a `Reservation`
/// through `apply_grouped`; single-key `cache.insert` never calls
/// `reserve()` at all (`SpillConfig::spill_wait_timeout`'s own doc names
/// this as an explicit scope boundary). Also exercises
/// `sundog_spill_dropped_total{reason="disk_error"}` via a real,
/// deterministic write failure: [`chattr_dir_entries`] makes the tier's
/// region files immutable right after `SpillTier::open` creates them, so
/// the flusher's later `pwrite` on its already-open handle for one of them
/// fails with `EPERM` -- a real filesystem fault, not a simulated one.
///
/// Returns the scratch directory to clean up, whether
/// [`chattr_dir_entries`] actually set up the fault, and, when it did, the
/// observed `disk_error` count. The caller skips the `disk_error`
/// assertion entirely when the fault could not be set up (no
/// `CAP_LINUX_IMMUTABLE`, no `chattr` binary, or a filesystem that does
/// not support the attribute) instead of failing the whole scenario; when
/// it *was* set up, `None` for the observed count is a genuine failure to
/// report, not an environment limitation to tolerate.
#[cfg(feature = "spill")]
async fn disk_error_and_reserve_wait_pin_metrics(
    cluster: &Cluster,
    metrics_addr: SocketAddr,
) -> (std::path::PathBuf, bool, Option<f64>) {
    let dir = fresh_spill_dir("disk-error");
    let cache_name = "disk-error-pin";
    let cfg = sundog::SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let cache = cluster
        .cache::<u32, String>(cache_name)
        .mode(Mode::Local)
        .max_capacity(1)
        .spill(cfg)
        .open()
        .await
        .expect("cache opens");

    // `SpillTier::open` creates and preallocates every region file
    // synchronously before `open()` above ever returns, so the directory
    // is already fully populated here, before any insert or eviction has
    // run.
    let region_dir = dir.join(cache_name);
    let immutable = chattr_dir_entries(&region_dir, true);

    // `insert_many` threads one `Reservation` through `apply_grouped` ->
    // `reserve()` for the whole call, so this is the one producer this
    // file can reach that exercises the new wait metrics; it applies both
    // entries regardless of whether the eviction this forces later fails.
    cache
        .insert_many([(1u32, "one".to_string()), (2u32, "two".to_string())])
        .await
        .expect("insert_many applies both entries even when the eviction they force later fails");

    let observed_disk_error = if immutable {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(body) = scrape_metrics(metrics_addr).await
                && let Some(count) = scraped_metric_value(
                    &body,
                    "sundog_spill_dropped_total",
                    &[("cache", cache_name), ("reason", "disk_error")],
                )
            {
                break Some(count);
            }
            if tokio::time::Instant::now() >= deadline {
                break None;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    } else {
        None
    };

    // Restore normal permissions so the caller's own `remove_dir_all`
    // cleanup can actually delete these files.
    let _ = chattr_dir_entries(&region_dir, false);

    (dir, immutable, observed_disk_error)
}

/// Drives a genuine `sundog_spill_wait_timeouts_total` increment through
/// the public API alone: `disk_error_and_reserve_wait_pin_metrics`'s own
/// tiny, two-entry scenario never needs `reserve()` to actually suspend
/// (its default 64 MiB `flush_queue_bytes` has room for both records many
/// times over), so that series is never registered there, and pinning it
/// against a genuine wait needs a scenario shaped like this one instead.
///
/// A single local `insert_many` burst, however large, turns out not to
/// reproduce this reliably: `flusher_loop` frees `admit`'s permits the
/// instant it *dequeues* a job (`SpillTier::reserve`'s own doc), well
/// before that job's disk write even starts, so a purely local producer
/// and the flusher both racing on the same machine's CPU tends to keep
/// `admit` refilled faster than even a 1-microsecond timeout can lose
/// against. `ShardOps::apply_remote_batch`'s own `reserve()` call, driven
/// through a real state-transfer pull between two real nodes exactly as
/// `spill_replication.rs::a_too_short_spill_wait_timeout_degrades_to_a_
/// clean_retry_not_a_hang` does, is what reliably produces a genuine
/// suspension here: a real donor round trip over loopback TCP is
/// consistently slower than the flusher's own local dequeue-and-free-permit
/// step, so this joiner's own reservation and retry calls do genuinely
/// have to wait on real network I/O rather than only local disk/channel
/// throughput. `Duration::from_micros(1)` then loses that race
/// deterministically: `tokio::time::timeout` only ever returns early when
/// its wrapped future resolves on the very first poll, and a real
/// suspension never does that within one microsecond of real wall-clock
/// scheduling latency.
///
/// Two scenario-local nodes, entirely independent of the caller's own
/// `cluster`/`peer`: a donor fills `ENTRIES` records unbounded and
/// spill-free, then a joiner opens with a tiny `flush_queue_bytes`, a
/// small `max_capacity`, and the too-short timeout, pulling the whole
/// dataset from the donor at `open()` time. Both nodes are shut down
/// before this returns. Returns the scratch directory to clean up. Makes
/// no claim about `queue_full`/`deferred` drops on the joiner's cache --
/// degrading cleanly under a too-short timeout, not a zero-drop guarantee,
/// is the property this scenario exercises; `spill_backpressure.rs`'s own
/// scenarios cover the zero-drop claim with a realistic timeout.
#[cfg(feature = "spill")]
async fn reserve_timeout_pin_metric() -> std::path::PathBuf {
    const REGION_BYTES: u64 = 2 * 1024 * 1024;
    const CAPACITY_BYTES: u64 = 32 * 1024 * 1024;
    const FLUSH_QUEUE_BYTES: u64 = 64 * 1024;
    const MAX_CAPACITY: u64 = 64;
    const ENTRIES: u32 = 80_000;
    /// Far too short to ever win a race against a genuine wait; see
    /// `a_too_short_spill_wait_timeout_degrades_to_a_clean_retry_not_a_hang`'s
    /// own doc for why `Duration::ZERO` is deliberately not used instead.
    const TOO_SHORT_TIMEOUT: Duration = Duration::from_micros(1);
    let cache_name = "reserve-timeout-pin";
    let cluster_name = "it-prometheus-exporter-reserve-timeout";

    let gossip_a = common::reserve_gossip_addr().await;
    let donor = Cluster::builder(cluster_name)
        .seeds(std::iter::empty())
        .config(common::fast_config().with(|c| c.gossip_bind_addr = gossip_a))
        .build()
        .await
        .expect("donor builds");
    let donor_cache = donor
        .cache::<u32, String>(cache_name)
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("donor opens alone, unbounded and spill-free, owning everything");

    let mut start = 0u32;
    while start < ENTRIES {
        let end = (start + 500).min(ENTRIES);
        donor_cache
            .insert_many((start..end).map(|i| (i, "v".repeat(200))))
            .await
            .expect("bulk insert on the donor succeeds");
        start = end;
    }

    let dir = fresh_spill_dir("reserve-timeout");
    let cfg = sundog::SpillConfig::new(&dir, CAPACITY_BYTES)
        .region_bytes(REGION_BYTES)
        .flush_queue_bytes(FLUSH_QUEUE_BYTES)
        .spill_wait_timeout(TOO_SHORT_TIMEOUT);

    let gossip_b = common::reserve_gossip_addr().await;
    let joiner = Cluster::builder(cluster_name)
        .seeds([gossip_a])
        .config(common::fast_config().with(|c| c.gossip_bind_addr = gossip_b))
        .build()
        .await
        .expect("joiner builds");
    common::wait_for_peer_count(&joiner, 1, Duration::from_secs(15)).await;

    let started = std::time::Instant::now();
    let _joiner_cache = joiner
        .cache::<u32, String>(cache_name)
        .mode(Mode::Replicated)
        .max_capacity(MAX_CAPACITY)
        .spill(cfg)
        .open()
        .await
        .expect(
            "joiner opens, pulling the whole dataset from the donor even though its own \
             spill_wait_timeout is absurdly short",
        );
    eprintln!(
        "reserve-timeout-pin: joiner open() took {:?}",
        started.elapsed()
    );

    donor.shutdown().await;
    joiner.shutdown().await;

    dir
}

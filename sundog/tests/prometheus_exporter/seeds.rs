//! Scripted cluster traffic that drives the cluster-side metrics the exporter test scrapes.

use crate::common;
use crate::support::{
    DENSE_BUCKET_COUNT, SKETCH_FILL, keys_in_one_bucket, node_config, scrape_metrics,
    scraped_metric_value,
};
use std::net::{Ipv4Addr, SocketAddr};
use std::num::NonZeroU8;
use std::sync::Arc;
use std::time::Duration;
use sundog::crdt::{PnCounter, PnCounterResolver};
use sundog::{CacheError, Cluster, Mode, NodeId};

/// Opens `users` on both `cluster` and `peer`, does one plain insert/remove
/// pair, then a sketch-scale fill with one key dropped on the peer, so the
/// next round finds one bucket mismatched at ~20 entries: past
/// `SKETCH_MIN_BUCKET`, so it reconciles through a decoded IBLT sketch.
pub(crate) async fn seed_sketch_mismatch(cluster: &Cluster, peer: &Cluster) {
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
pub(crate) async fn seed_part_mismatch(cluster: &Cluster, peer: &Cluster) {
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

/// Opens `delayed` on a scenario-local donor, joins a scenario-local
/// victim with a short `state_transfer_budget`, then kills the donor
/// before the victim opens the same cache: the victim's view still lists
/// the donor as live, so every bucket pull dials a closed listener and
/// fails until the budget runs out, incrementing
/// `sundog_rebalance_pull_timeouts_total{cache="delayed"}`. Independent
/// of `peer`/`third`/`fourth`: both scenario-local nodes are down by the
/// time this returns.
pub(crate) async fn seed_pull_timeout_metric(gossip_a: SocketAddr, metrics_addr: SocketAddr) {
    let name = "delayed";
    let owners = NonZeroU8::new(2).expect("nonzero");

    let donor = Cluster::builder("it-prometheus-exporter")
        .seeds([gossip_a])
        .config(node_config(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))))
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

    // A failure detector slow enough that the victim's view keeps listing
    // the donor for every timed-out warm-up attempt: each attempt costs its
    // 200ms budget plus the retry interval, and the donor dropping out
    // first would end the warm-up with no co-owner instead of a timeout.
    let victim_config = node_config(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).with(|c| {
        c.state_transfer_budget = Duration::from_millis(200);
        c.phi_threshold = 1_000.0;
    });
    let victim = Cluster::builder("it-prometheus-exporter")
        .seeds([gossip_a])
        .config(victim_config)
        .build()
        .await
        .expect("victim builds");
    common::wait_for_peer_count(&victim, 1, Duration::from_secs(15)).await;
    // Gossip quiescence, so the victim's view lists the donor before it opens.
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // A crash, not a graceful leave: a leaving node announces itself and
    // drops out of every view at once, while a crashed one stays listed
    // live until the failure detector notices, so every pull dials a closed
    // listener.
    donor.crash().await;

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
pub(crate) async fn seed_crdt_compaction_metrics(cluster: &Cluster, metrics_addr: SocketAddr) {
    let name = "counters";
    let restart_id = NodeId::random();
    let gossip_a = cluster.local_gossip_addr();

    let cluster_counters = cluster
        .cache::<u32, PnCounter>(name)
        .mode(Mode::Replicated)
        .resolver(Arc::new(PnCounterResolver))
        .open()
        .await
        .expect("cluster opens counters");

    let first_life = Cluster::builder("it-prometheus-exporter")
        .seeds([gossip_a])
        .config(node_config(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))))
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
        .config(node_config(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))))
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
    // see the comment in `node_config`). Give the next real tick after the
    // incarnation mismatch above becomes visible ample room to land. The
    // first scrape that sees a retirement sees that tick's alone: the next
    // is 30s away, and counts the same writer again while its record is
    // skipped, so the exact count is pinned here, not at the final scrape.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    let first_count = loop {
        let count = scrape_metrics(metrics_addr)
            .await
            .and_then(|body| {
                scraped_metric_value(
                    &body,
                    "sundog_crdt_retired_writers_total",
                    &[("cache", name)],
                )
            })
            .filter(|&count| count >= 1.0);
        if count.is_some() {
            break count;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the dead writer retires within two minutes"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(
        first_count,
        Some(1.0),
        "the first sweep that finds the dead writer counts exactly it"
    );

    assert_eq!(
        cluster_counters.get(&1).await.map(|c| c.value()),
        Some(7),
        "stage-one retirement moves a dead writer's slot without changing the merged value"
    );

    second_life.shutdown().await;
}

/// Opens `prices` across `cluster`, `peer`, and two more nodes joined
/// for this scenario, driving every `sundog_fetch_total` outcome, a
/// forwarded write, and all three `sundog_rebalance_parts_total`
/// directions on `cluster` itself, the only node whose metrics this test
/// scrapes. Folded into the main test for the same process-global
/// recorder reason `spill_writes_and_promotes_pin_metrics` is; the shared
/// recorder also means `sundog_owned_parts{cache="prices"}`'s pin stays
/// a positivity check, and `sundog_owned_buckets` is pinned to it over 64.
#[allow(clippy::too_many_lines, reason = "one scripted end-to-end scenario")]
pub(crate) async fn seed_distributed_metrics(cluster: &Cluster, peer: &Cluster) {
    let owners = NonZeroU8::new(2).expect("nonzero");
    let name = "prices";
    let gossip_a = cluster.local_gossip_addr();

    // `peer` and a fresh third node stabilize a two-way distributed cache
    // (both own everything with only two eligible nodes) before `cluster`
    // ever opens it, so `cluster`'s own open() pulls a real share of the
    // 1,024 buckets from a real donor: the open()-time initial pull is
    // `sundog_rebalance_parts_total{direction="in"}`'s natural trigger.
    let third = Cluster::builder("it-prometheus-exporter")
        .seeds([gossip_a])
        .config(node_config(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))))
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

    // sundog_rebalance_parts_total{direction="out"}: a fourth node joins
    // and, for at least one of cluster's owned buckets, displaces it;
    // cluster releases that bucket once the disown grace elapses.
    let owned_before: Vec<u32> = (0..500u32)
        .filter(|&k| cluster_prices.owners_of(&k).contains(&cluster.node_id()))
        .collect();
    let fourth = Cluster::builder("it-prometheus-exporter")
        .seeds([gossip_a])
        .config(node_config(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))))
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
    // crashes; both dials fail before the failure detector reacts. A
    // graceful leave would announce the departure and hand the key to a
    // live owner instead.
    let error_key = (20_000..30_000u32)
        .find(|&k| !cluster_prices.owners_of(&k).contains(&cluster.node_id()))
        .expect("some key's bucket excludes cluster among four real nodes");
    let error_owners = cluster_prices.owners_of(&error_key);
    futures::future::join_all(
        [peer.clone(), third.clone(), fourth.clone()]
            .into_iter()
            .filter(|down| error_owners.contains(&down.node_id()))
            .map(Cluster::crash),
    )
    .await;
    assert!(
        matches!(
            cluster_prices.fetch(&error_key).await,
            Err(CacheError::FetchUnavailable { .. })
        ),
        "outcome=error"
    );
}

/// The `Distributed` cache [`seed_fan_out_stall_metrics`] stalls.
pub(crate) const FAN_OUT_STALL_PIN: &str = "fan-out-stall-pin";

/// Drives `sundog_fan_out_stalls_total` and
/// `sundog_fan_out_lost_writes_total{cache}`: three scenario-local nodes open
/// [`FAN_OUT_STALL_PIN`], the two owners of one key crash, and the writer,
/// whose failure detector never drops them and whose outboxes hold one frame,
/// writes the key until a write it forwards reaches neither owner. Shuts the
/// writer down before it returns, which the stalled owners do not hold up.
pub(crate) async fn seed_fan_out_stall_metrics(metrics_addr: SocketAddr) {
    let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    let mode = Mode::Distributed {
        owners: NonZeroU8::new(2).expect("nonzero"),
    };
    let writer = Cluster::builder("it-fan-out-stall")
        .seeds(std::iter::empty())
        .config(node_config(loopback).with(|c| {
            c.outbox_capacity = 1;
            c.fan_out_stall_timeout = Duration::from_millis(300);
            c.phi_threshold = 1_000.0;
        }))
        .build()
        .await
        .expect("the writer builds");
    let mut owners = Vec::new();
    for _ in 0..2 {
        owners.push(
            Cluster::builder("it-fan-out-stall")
                .seeds([writer.local_gossip_addr()])
                .config(node_config(loopback))
                .build()
                .await
                .expect("an owner builds"),
        );
    }
    common::wait_for_peer_count(&writer, 2, Duration::from_secs(15)).await;
    let writer_cache = writer
        .cache::<u32, String>(FAN_OUT_STALL_PIN)
        .mode(mode)
        .open()
        .await
        .expect("the writer opens the cache");
    let mut owner_caches = Vec::new();
    for owner in &owners {
        owner_caches.push(
            owner
                .cache::<u32, String>(FAN_OUT_STALL_PIN)
                .mode(mode)
                .open()
                .await
                .expect("an owner opens the cache"),
        );
    }
    let owner_ids: Vec<NodeId> = owners.iter().map(Cluster::node_id).collect();
    let key = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(key) = (0..10_000u32).find(|key| {
                let mut of = writer_cache.owners_of(key);
                of.sort_unstable();
                let mut wanted = owner_ids.clone();
                wanted.sort_unstable();
                of == wanted
            }) {
                return key;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the writer's view names the two owners within the bound");
    for owner in owners {
        owner.crash().await;
    }
    drop(owner_caches);

    common::eventually(Duration::from_secs(30), || async {
        let _ = writer_cache.insert(key, "stalled".to_string()).await;
        tokio::time::sleep(Duration::from_millis(400)).await;
        scrape_metrics(metrics_addr).await.is_some_and(|body| {
            scraped_metric_value(
                &body,
                "sundog_fan_out_lost_writes_total",
                &[("cache", FAN_OUT_STALL_PIN)],
            )
            .is_some_and(|lost| lost >= 1.0)
        })
    })
    .await;
    tokio::time::timeout(Duration::from_secs(60), writer.shutdown())
        .await
        .expect("the writer shuts down while its owners take nothing");
}

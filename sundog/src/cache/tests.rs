//! Unit tests for the cache: pure rules, builder validation and loopback clusters.

use std::net::{Ipv4Addr, SocketAddr};

use super::*;
use crate::cluster::Cluster;
use crate::cluster::test_support::wait_until;
use crate::explain::{
    LocalRead, LocalRecord, ProbeAnswer, ProbedRecord, Residency, ServeVerdict, Unreached,
};
use crate::store::bucket_of;
use crate::store::crdt::{PnCounter, PnCounterResolver};

fn loopback_config() -> ClusterConfig {
    let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    ClusterConfig {
        gossip_bind_addr: loopback,
        data_bind_addr: loopback,
        ae_interval: Duration::from_millis(200),
        tombstone_ttl: Duration::from_secs(2),
        state_transfer_budget: Duration::from_secs(5),
        ..ClusterConfig::default()
    }
}

async fn wait_for_peer_count(cluster: &Cluster, expected: usize) {
    wait_until(
        Duration::from_secs(15),
        "peers converge within the bound",
        async || cluster.peers().len() >= expected,
    )
    .await;
}

/// A loopback UDP address nothing listens on, for forcing a wait to time out.
async fn dead_gossip_addr() -> SocketAddr {
    let socket = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind an ephemeral loopback udp port to reserve a dead gossip address");
    socket
        .local_addr()
        .expect("a freshly bound udp socket reports a local address")
}

/// `record_reads` names what `decode_live_value` makes of a fetched
/// record: a value exactly when it decodes one, and otherwise why not.
#[test]
fn record_reads_names_what_decode_live_value_makes_of_a_record() {
    let now = now_ms();
    let value = |v: &str| Some(bytes::Bytes::from(postcard::to_stdvec(v).expect("encodes")));
    let record = |value: Option<bytes::Bytes>, expires_at_ms: Option<u64>| WireRecord {
        key: bytes::Bytes::from_static(b"k"),
        value,
        ver: crate::hlc::Hlc {
            wall_ms: 1,
            logical: 0,
            node: NodeId::from(1u64),
        },
        expires_at_ms,
    };
    let hour = 3_600_000;
    let cases = [
        (record(value("v"), None), Reads::Value),
        (record(value("v"), Some(now + hour)), Reads::Value),
        (record(value("v"), Some(now - hour)), Reads::Expired),
        (record(None, None), Reads::Deleted),
        (record(None, Some(now - hour)), Reads::Deleted),
        (
            record(Some(bytes::Bytes::from_static(&[0xff])), None),
            Reads::Undecodable,
        ),
    ];
    for (rec, reads) in cases {
        assert_eq!(record_reads::<String>(&rec, now), reads, "{rec:?}");
        assert_eq!(
            decode_live_value::<String>(&rec).is_some(),
            reads == Reads::Value,
            "{rec:?}"
        );
    }
}

/// Through `Cache`: on a capped cache with more stripes than entries,
/// every write past the cap reads back at once and the cap holds.
#[tokio::test]
async fn every_write_past_max_capacity_reads_back_through_the_cache() {
    let cluster = Cluster::builder("cache-it-write-past-cap")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node builds");
    let cache = cluster
        .cache::<u32, String>("capped")
        .mode(Mode::Local)
        .max_capacity(50)
        .open()
        .await
        .expect("opens");
    for k in 0..200u32 {
        cache.insert(k, k.to_string()).await.expect("insert");
        assert_eq!(cache.get(&k).await, Some(k.to_string()), "key {k}");
    }
    assert_eq!(cache.entry_count().await, 50);
    cluster.shutdown().await;
}

/// Through `Cache`: a value heavier than the whole cap goes, and an
/// older entry in another stripe stays.
#[tokio::test]
async fn a_value_heavier_than_max_capacity_leaves_the_older_entry() {
    let cluster = Cluster::builder("cache-it-heavier-than-cap")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node builds");
    let cache = cluster
        .cache::<u32, String>("heavy")
        .mode(Mode::Local)
        .max_capacity(5)
        .weigher(|_k: &u32, v: &String| u32::try_from(v.len()).unwrap_or(u32::MAX))
        .open()
        .await
        .expect("opens");
    let stripe = |k: u32| {
        crate::store::hash_key_bytes(&postcard::to_stdvec(&k).expect("key encodes"))
            % crate::store::BUCKET_COUNT as u64
    };
    let heavy = (2..100_000u32)
        .find(|&k| stripe(k) != stripe(1))
        .expect("another stripe");
    cache.insert(1, "x".to_string()).await.expect("insert");

    cache.insert(heavy, "x".repeat(10)).await.expect("insert");

    assert_eq!(cache.get(&heavy).await, None, "the heavy value goes");
    assert_eq!(
        cache.get(&1).await.as_deref(),
        Some("x"),
        "the older entry stays"
    );
    assert_eq!(cache.entry_count().await, 1);
    cluster.shutdown().await;
}

#[test]
fn should_await_first_peer_only_when_seeded_and_genuinely_alone() {
    assert!(
        should_await_first_peer(true, false),
        "seeds present and peers empty waits"
    );
    assert!(
        !should_await_first_peer(false, false),
        "no seeds does not wait, even with peers still empty"
    );
    assert!(
        !should_await_first_peer(true, true),
        "peers already known does not wait, even with seeds configured"
    );
    assert!(
        !should_await_first_peer(false, true),
        "no seeds and peers already known: nothing to wait for either way"
    );
}

#[test]
fn trust_sole_owner_at_open_only_distrusts_a_timed_out_warm_reopen() {
    assert!(
        trust_sole_owner_at_open(true, 0),
        "cold open, membership settled: trusted"
    );
    assert!(
        trust_sole_owner_at_open(true, 3),
        "warm reopen, membership settled: trusted"
    );
    assert!(
        trust_sole_owner_at_open(false, 0),
        "cold open, membership wait timed out: still trusted, nothing replayed to distrust"
    );
    assert!(
        !trust_sole_owner_at_open(false, 3),
        "warm reopen, membership wait timed out: not trusted, a replayed bucket found owned \
         alone could be the replay's own stale echo of ownership"
    );
}

#[tokio::test]
async fn await_initial_peers_returns_immediately_without_seeds() {
    let cluster = Cluster::builder("cache-it-await-peers-no-seeds")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("solo cluster builds");
    assert!(
        cluster.peers().is_empty(),
        "a fresh solo cluster knows no peer yet"
    );

    let start = Instant::now();
    let settled = await_initial_peers(&cluster).await;

    assert!(
        settled,
        "a seedless cluster is a legitimate one-node cluster, not a race to wait out"
    );
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "no wait should have happened at all, so this returns almost instantly"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn await_initial_peers_settles_once_a_seeded_peer_joins() {
    let b = Cluster::builder("cache-it-await-peers-settles")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("b builds");
    let a = Cluster::builder("cache-it-await-peers-settles")
        .seeds([b.local_gossip_addr()])
        .config(loopback_config())
        .build()
        .await
        .expect("a builds");

    // No prior `wait_for_peer_count`: `await_initial_peers` itself is under test.
    let settled = await_initial_peers(&a).await;

    assert!(settled, "the seeded peer shows up well within the bound");
    assert!(
        !a.peers().is_empty(),
        "a's peer list is non-empty once the wait reports settled"
    );

    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn await_initial_peers_times_out_when_seeded_but_nobody_answers() {
    let dead_seed = dead_gossip_addr().await;
    let config = ClusterConfig {
        state_transfer_budget: Duration::from_millis(200),
        ..loopback_config()
    };
    let cluster = Cluster::builder("cache-it-await-peers-timeout")
        .seeds([dead_seed])
        .config(config)
        .build()
        .await
        .expect("cluster builds even though its one seed answers nobody");

    let start = Instant::now();
    let settled = await_initial_peers(&cluster).await;
    let elapsed = start.elapsed();

    assert!(
        !settled,
        "nobody ever answers the dead seed, so the wait must time out"
    );
    assert!(cluster.peers().is_empty(), "still genuinely alone");
    assert!(
        elapsed >= Duration::from_millis(180),
        "the wait should run close to its full 200ms bound, took {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "the wait must not run anywhere near the `INITIAL_PEER_WAIT_CAP` default, took \
         {elapsed:?}"
    );

    cluster.shutdown().await;
}

/// A loopback TCP address nobody listens on, distinct from [`dead_gossip_addr`]'s UDP one.
fn dead_tcp_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .expect("bind an ephemeral loopback tcp port to reserve a dead address");
    listener
        .local_addr()
        .expect("a freshly bound tcp listener reports a local address")
}

/// Pins that a cold open trusts sole ownership outright even when its
/// only seed never answers, guarding the regression that left every
/// bucket cold behind a timed-out membership wait.
#[tokio::test]
async fn distributed_open_trusts_sole_ownership_when_its_membership_wait_times_out() {
    let dead_seed = dead_tcp_addr();
    let config = ClusterConfig {
        // Short enough that `await_initial_peers`'s wait times out quickly.
        state_transfer_budget: Duration::from_secs(1),
        ..loopback_config()
    };
    let cluster = Cluster::builder("cache-it-cold-open-sole-owner")
        .seeds([dead_seed])
        .config(config)
        .build()
        .await
        .expect("cluster builds even though its one seed answers nobody");

    let cache = tokio::time::timeout(
        Duration::from_secs(10),
        cluster
            .cache::<u32, String>("prices")
            .mode(Mode::Distributed {
                owners: NonZeroU8::new(2).expect("nonzero"),
            })
            .open(),
    )
    .await
    .expect("open completes within its own short membership wait")
    .expect("open succeeds even though the wait timed out");

    cache
        .insert(1, "one".to_string())
        .await
        .expect("insert succeeds on a node that owns every bucket alone");

    let key_bytes = encode_key(&1u32).expect("a u32 key always encodes");
    let bucket = bucket_of(&key_bytes);

    tokio::time::timeout(Duration::from_secs(5), async {
        while cache.shard.is_cold_bucket(bucket) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the key's bucket is not cold within a few seconds");

    assert!(
        !cache.shard.is_cold_bucket(bucket),
        "sole ownership on a cold open is trusted outright, not left cold behind a peer \
         that never appears"
    );
    assert_eq!(
        cache.fetch(&1).await.expect("fetch succeeds"),
        Some("one".to_string()),
        "a bucket trusted as sole-owned is served locally, not held back for a peer that \
         never appears"
    );
    assert!(
        cluster.peers().is_empty(),
        "this assertion holds before any peer ever appears, exactly the shape the timed-out \
         wait leaves behind"
    );

    cluster.shutdown().await;
}

#[test]
fn reconcile_budget_from_config_takes_its_byte_time_and_backoff_bounds_from_the_config() {
    let config = ClusterConfig {
        reconcile_byte_budget: 12_345,
        state_transfer_budget: Duration::from_millis(4_321),
        ae_interval: Duration::from_millis(765),
        ..ClusterConfig::default()
    };
    let budget = ReconcileBudget::from_config(&config);
    assert_eq!(budget.max_rounds, RECONCILE_MAX_ROUNDS);
    assert_eq!(budget.byte_budget, 12_345);
    assert_eq!(budget.time_budget, Duration::from_millis(4_321));
    assert_eq!(budget.backoff_cap, Duration::from_millis(765));
}

/// Pins that `should_keep_reconciling` stops once `elapsed` alone reaches `time_budget`.
#[test]
fn should_keep_reconciling_stops_once_the_time_budget_is_spent() {
    let budget = ReconcileBudget {
        max_rounds: 3,
        byte_budget: 1_000,
        time_budget: Duration::from_secs(1),
        backoff_cap: Duration::from_millis(200),
    };
    for (elapsed, expect_continue) in [
        (Duration::from_millis(999), true),
        (Duration::from_secs(1), false),
        (Duration::from_secs(2), false),
    ] {
        assert_eq!(
            should_keep_reconciling(0, 0, 5, elapsed, &budget),
            expect_continue,
            "elapsed {elapsed:?}"
        );
    }
}

/// Pins that `retry_delay` doubles from `RECONCILE_RETRY_BASE` per
/// failure, caps at `backoff_cap`, and stops at the time budget.
#[test]
fn retry_delay_doubles_from_the_base_caps_at_the_backoff_cap_and_stops_at_the_time_budget() {
    struct Case {
        name: &'static str,
        consecutive_failures: u32,
        elapsed: Duration,
        expect: Option<Duration>,
    }
    let budget = ReconcileBudget {
        max_rounds: 3,
        byte_budget: 1_000,
        time_budget: Duration::from_secs(1),
        backoff_cap: Duration::from_millis(500),
    };
    let cases = [
        Case {
            name: "first failure waits the base delay",
            consecutive_failures: 1,
            elapsed: Duration::ZERO,
            expect: Some(Duration::from_millis(200)),
        },
        Case {
            name: "second failure doubles it",
            consecutive_failures: 2,
            elapsed: Duration::ZERO,
            expect: Some(Duration::from_millis(400)),
        },
        Case {
            name: "third failure caps at the backoff cap",
            consecutive_failures: 3,
            elapsed: Duration::ZERO,
            expect: Some(Duration::from_millis(500)),
        },
        Case {
            name: "a failure count past the shift width still caps, never overflows",
            consecutive_failures: 40,
            elapsed: Duration::ZERO,
            expect: Some(Duration::from_millis(500)),
        },
        Case {
            name: "a zero failure count is treated as the first",
            consecutive_failures: 0,
            elapsed: Duration::ZERO,
            expect: Some(Duration::from_millis(200)),
        },
        Case {
            name: "a wait that still ends inside the time budget is taken",
            consecutive_failures: 1,
            elapsed: Duration::from_millis(799),
            expect: Some(Duration::from_millis(200)),
        },
        Case {
            name: "a wait that would land exactly on the deadline stops the loop",
            consecutive_failures: 1,
            elapsed: Duration::from_millis(800),
            expect: None,
        },
        Case {
            name: "time budget already spent",
            consecutive_failures: 1,
            elapsed: Duration::from_secs(1),
            expect: None,
        },
        Case {
            name: "time budget already exceeded",
            consecutive_failures: 2,
            elapsed: Duration::from_secs(3),
            expect: None,
        },
    ];
    for case in cases {
        assert_eq!(
            retry_delay(case.consecutive_failures, case.elapsed, &budget),
            case.expect,
            "case: {}",
            case.name
        );
    }
}

/// Pins that `should_keep_reconciling` stops at whichever bound (rounds,
/// bytes, time, or nothing left diverging) is hit first.
#[test]
fn should_keep_reconciling_stops_at_whichever_bound_is_hit_first() {
    struct Case {
        name: &'static str,
        rounds_run: u32,
        bytes_moved: u64,
        still_diverging: usize,
        elapsed: Duration,
        expect_continue: bool,
    }
    let budget = ReconcileBudget {
        max_rounds: 3,
        byte_budget: 1_000,
        time_budget: Duration::from_secs(1),
        backoff_cap: Duration::from_millis(200),
    };
    let cases = [
        Case {
            name: "rounds exhausted",
            rounds_run: 3,
            bytes_moved: 0,
            still_diverging: 5,
            elapsed: Duration::ZERO,
            expect_continue: false,
        },
        Case {
            name: "rounds past the cap",
            rounds_run: 4,
            bytes_moved: 0,
            still_diverging: 5,
            elapsed: Duration::ZERO,
            expect_continue: false,
        },
        Case {
            name: "byte budget exhausted",
            rounds_run: 0,
            bytes_moved: 1_000,
            still_diverging: 5,
            elapsed: Duration::ZERO,
            expect_continue: false,
        },
        Case {
            name: "byte budget exceeded",
            rounds_run: 0,
            bytes_moved: 1_001,
            still_diverging: 5,
            elapsed: Duration::ZERO,
            expect_continue: false,
        },
        Case {
            name: "nothing left diverging",
            rounds_run: 0,
            bytes_moved: 0,
            still_diverging: 0,
            elapsed: Duration::ZERO,
            expect_continue: false,
        },
        Case {
            name: "one round ran with zero progress, two left",
            rounds_run: 1,
            bytes_moved: 0,
            still_diverging: 5,
            elapsed: Duration::ZERO,
            expect_continue: true,
        },
        Case {
            name: "under every bound with work left and the time budget nearly spent",
            rounds_run: 1,
            bytes_moved: 10,
            still_diverging: 2,
            elapsed: Duration::from_millis(999),
            expect_continue: true,
        },
    ];
    for case in cases {
        assert_eq!(
            should_keep_reconciling(
                case.rounds_run,
                case.bytes_moved,
                case.still_diverging,
                case.elapsed,
                &budget
            ),
            case.expect_continue,
            "case: {}",
            case.name
        );
    }
}

/// Pins that `split_round_result` folds a `failed` round as every bucket
/// diverging regardless of `matched`, else splits by membership in it.
#[test]
fn split_round_result_folds_a_failed_round_and_otherwise_splits_by_matched() {
    fn parts(raw: &[u16]) -> HashSet<PartId> {
        raw.iter().map(|&r| PartId::from_raw(r)).collect()
    }
    struct Case {
        name: &'static str,
        requested: &'static [u16],
        outcome: anti_entropy::PartRoundOutcome,
        expect_converged: &'static [u16],
        expect_diverging: &'static [u16],
    }
    let cases = [
        Case {
            name: "a failed round leaves every requested bucket diverging, even one \
                   nominally in matched",
            requested: &[1, 2, 3],
            outcome: anti_entropy::PartRoundOutcome {
                matched: parts(&[1]),
                still_diverged: HashSet::new(),
                bytes_moved: 0,
                failed: true,
            },
            expect_converged: &[],
            expect_diverging: &[1, 2, 3],
        },
        Case {
            name: "a successful round splits matched from still-diverging",
            requested: &[1, 2, 3],
            outcome: anti_entropy::PartRoundOutcome {
                matched: parts(&[1, 3]),
                still_diverged: parts(&[2]),
                bytes_moved: 500,
                failed: false,
            },
            expect_converged: &[1, 3],
            expect_diverging: &[2],
        },
        Case {
            name: "everything requested matched: nothing left diverging",
            requested: &[4, 5],
            outcome: anti_entropy::PartRoundOutcome {
                matched: parts(&[4, 5]),
                still_diverged: HashSet::new(),
                bytes_moved: 0,
                failed: false,
            },
            expect_converged: &[4, 5],
            expect_diverging: &[],
        },
    ];
    for case in cases {
        let requested: Vec<PartId> = case
            .requested
            .iter()
            .map(|&r| PartId::from_raw(r))
            .collect();
        let (converged, diverging) = split_round_result(&requested, &case.outcome);
        let raw = |parts: Vec<PartId>| -> Vec<u16> { parts.into_iter().map(PartId::raw).collect() };
        assert_eq!(raw(converged), case.expect_converged, "case: {}", case.name);
        assert_eq!(raw(diverging), case.expect_diverging, "case: {}", case.name);
    }
}

/// A `Mode::Distributed` shard-and-context pair, `attach_ownership`'s
/// shape, for testing without a full `open()`.
fn distributed_context_for_test(
    cluster: &Cluster,
    name: &SmolStr,
) -> (Arc<dyn ShardOps>, DistributedContext) {
    let shard = Shard::<u32, String>::new(
        name.clone(),
        Mode::distributed(),
        cluster.node_id(),
        u64::MAX,
        None,
        None,
    );
    let (shard, distributed) = attach_ownership(shard, cluster, name, Mode::distributed());
    let distributed = distributed.expect("Mode::distributed always attaches a context");
    (Arc::new(shard), distributed)
}

#[tokio::test]
async fn reconcile_warm_buckets_leaves_a_bucket_cold_with_no_live_co_owner_to_verify_against() {
    let cluster = Cluster::builder("cache-it-reconcile-alone")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let name = SmolStr::new("scratch");
    let (shard_ops, distributed) = distributed_context_for_test(&cluster, &name);

    let warm_parts: Vec<PartId> = [0u16, 500, 1023]
        .into_iter()
        .flat_map(PartId::of_bucket)
        .collect();
    distributed.residency.mark_cold(&warm_parts);
    let reconciled = reconcile_warm_buckets(
        &cluster,
        &shard_ops,
        &name,
        &distributed.ownership,
        &distributed.residency,
        &warm_parts,
    )
    .await;

    assert!(
        reconciled.is_empty(),
        "a bucket with no live co-owner has nobody to verify its replayed state against, \
         so reconcile_warm_buckets itself leaves it cold rather than vacuously treating it \
         as reconciled -- it falls through to PullRequest::run's own \"owned alone\" case \
         next, which is the site that actually decides a co-owner-less bucket is servable"
    );
    for part in warm_parts {
        assert!(
            distributed.residency.is_cold(part),
            "part {part} stays cold with nobody for reconcile_warm_buckets itself to \
             reconcile against, falling through to the ordinary cold-pull path"
        );
    }

    cluster.shutdown().await;
}

/// A donor still pulling a part itself declines every pull as cold, yet
/// can hold writes that reached it meanwhile. A joiner whose only donor
/// stays cold converges with it by anti-entropy before serving, rather
/// than answering a miss for a key the donor holds. `ae_interval` is an
/// hour, so no periodic round repairs it instead.
#[tokio::test]
async fn a_joiner_whose_only_donor_stays_cold_converges_with_it_before_serving() {
    const TOTAL: u32 = 50;
    let name = SmolStr::new("cold-donor");
    let config = ClusterConfig {
        ae_interval: Duration::from_hours(1),
        tombstone_ttl: Duration::from_hours(720),
        ..loopback_config()
    };

    let b = Cluster::builder("cache-it-cold-donor")
        .seeds(std::iter::empty())
        .config(config.clone())
        .build()
        .await
        .expect("b builds");
    let cache_b = b
        .cache::<u32, String>(name.clone())
        .mode(Mode::distributed())
        .open()
        .await
        .expect("b opens alone, owning every part");
    for key in 0..TOTAL {
        cache_b
            .insert(key, format!("v{key}"))
            .await
            .expect("b owns every part while alone");
    }
    let c = Cluster::builder("cache-it-cold-donor")
        .seeds([b.local_gossip_addr()])
        .config(config)
        .build()
        .await
        .expect("c builds");
    wait_for_peer_count(&b, 1).await;
    wait_for_peer_count(&c, 1).await;
    // Marked once b sees c: while b owns every part alone, a republished
    // view marks what it owns alone servable.
    cache_b
        .shard
        .residency()
        .expect("b is distributed")
        .mark_cold(&PartId::all().collect::<Vec<_>>());
    let cache_c = c
        .cache::<u32, String>(name)
        .mode(Mode::distributed())
        .open()
        .await
        .expect("c opens, co-owning every part with b");

    wait_until(
        Duration::from_secs(10),
        "c's pull settles against its cold donor",
        async || cache_c.entry_count().await == u64::from(TOTAL),
    )
    .await;
    for key in 0..TOTAL {
        assert_eq!(
            cache_c.get(&key).await,
            Some(format!("v{key}")),
            "key {key} converged from the cold donor"
        );
    }

    cache_c.close().await;
    cache_b.close().await;
    c.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn reconcile_warm_buckets_never_runs_a_round_for_an_empty_bucket_list() {
    let cluster = Cluster::builder("cache-it-reconcile-empty")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let name = SmolStr::new("scratch");
    let (shard_ops, distributed) = distributed_context_for_test(&cluster, &name);

    let reconciled = reconcile_warm_buckets(
        &cluster,
        &shard_ops,
        &name,
        &distributed.ownership,
        &distributed.residency,
        &[],
    )
    .await;

    assert!(
        reconciled.is_empty(),
        "an empty warm-bucket list, the non-warm-reopen case, never runs a round"
    );

    cluster.shutdown().await;
}

/// Pins the core reconciliation mechanism against a real live co-owner:
/// `c` starts cold-seeded, pulls `b`'s real data via a genuine
/// anti-entropy round, and only then clears cold via `mark_serving`.
#[tokio::test]
async fn reconcile_warm_buckets_clears_cold_once_a_real_round_against_a_live_co_owner_reconciles() {
    const TOTAL: u32 = 200;
    let name = SmolStr::new("reconcile-real-round");

    let b = Cluster::builder("cache-it-reconcile-real-round")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("b builds");
    let cache_b = b
        .cache::<u32, String>(name.clone())
        .mode(Mode::distributed())
        .open()
        .await
        .expect("b opens alone, owning every bucket");
    for key in 0..TOTAL {
        cache_b
            .insert(key, format!("v{key}"))
            .await
            .expect("b owns every bucket while alone");
    }

    let c = Cluster::builder("cache-it-reconcile-real-round")
        .seeds([b.local_gossip_addr()])
        .config(loopback_config())
        .build()
        .await
        .expect("c builds");
    wait_for_peer_count(&b, 1).await;
    wait_for_peer_count(&c, 1).await;
    // Bypasses `open()`'s own advertise call: without it b never learns c
    // has `name` open, so every round against it answers `Stale`.
    c.advertise_cache_mode(&name, Mode::distributed());

    // c's shard for `name`, built directly rather than through a real `open()`.
    let (shard_ops_c, distributed_c) = distributed_context_for_test(&c, &name);
    let warm_parts: Vec<PartId> = distributed_c.ownership.current().owned_parts().collect();
    assert!(
        !warm_parts.is_empty(),
        "with only b and c eligible at owners=2, c owns every bucket"
    );
    distributed_c.residency.mark_cold(&warm_parts);

    // b's tracker catches up to c joining on its own refresh cadence,
    // answering every round `Stale` until then, so this polls rather
    // than assuming one round suffices.
    let mut reconciled: HashSet<PartId> = HashSet::new();
    wait_until(
        Duration::from_secs(10),
        "b's view catches up to c joining, so the round against it reconciles",
        async || {
            reconciled = reconcile_warm_buckets(
                &c,
                &shard_ops_c,
                &name,
                &distributed_c.ownership,
                &distributed_c.residency,
                &warm_parts,
            )
            .await;
            reconciled.len() == warm_parts.len()
        },
    )
    .await;

    for &bucket in &warm_parts {
        assert!(
            !distributed_c.residency.is_cold(bucket),
            "bucket {bucket} clears cold once its digest matches b's in a round against it"
        );
    }
    let recovered: usize = shard_ops_c
        .entries_for_buckets(buckets_of(&warm_parts))
        .await
        .into_iter()
        .map(|(_, entries)| entries.len())
        .sum();
    assert_eq!(
        recovered, TOTAL as usize,
        "the round actually pulled b's data in, not merely declared the buckets reconciled"
    );

    c.shutdown().await;
    b.shutdown().await;
}

/// Pins [`reconcile_warm_buckets`]'s per-peer processing: three real
/// nodes so `c`'s warm buckets split across two live co-owners (`b` and
/// `d`), and one call converges every bucket regardless of which peer
/// it's grouped under.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one end-to-end three-node scenario (settle b/c/d, split c's warm buckets \
              across b and d, reconcile, assert both peers' shares converge): splitting it \
              would only scatter state (b, c, d, shard_ops_c, distributed_c, warm_parts) \
              across helper signatures"
)]
async fn reconcile_warm_buckets_converges_buckets_split_across_two_live_co_owners() {
    use crate::cluster::test_support::registered_shard;

    let name = SmolStr::new("reconcile-two-peers");

    let b = Cluster::builder("cache-it-reconcile-two-peers")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("b builds");
    b.cache::<u32, String>(name.clone())
        .mode(Mode::distributed())
        .open()
        .await
        .expect("b opens alone, owning every bucket");

    let seed = b.local_gossip_addr();
    let c = Cluster::builder("cache-it-reconcile-two-peers")
        .seeds([seed])
        .config(loopback_config())
        .build()
        .await
        .expect("c builds");
    let d = Cluster::builder("cache-it-reconcile-two-peers")
        .seeds([seed])
        .config(loopback_config())
        .build()
        .await
        .expect("d builds");
    wait_for_peer_count(&b, 2).await;
    wait_for_peer_count(&c, 2).await;
    wait_for_peer_count(&d, 2).await;

    // Without this, neither b nor d learns c has `name` open, and every round answers Stale.
    c.advertise_cache_mode(&name, Mode::distributed());
    d.cache::<u32, String>(name.clone())
        .mode(Mode::distributed())
        .open()
        .await
        .expect("d opens alone of data, owning its own share of buckets");

    let node_b = b.node_id();
    let node_d = d.node_id();

    // Until gossip carries d's mode to c, c sees only b as a co-owner.
    // Rebuilding the context on each retry (not just the round) is what
    // waits out that convergence instead of freezing an early view.
    let mut built: Option<SplitWarmContext> = None;
    wait_until(
        Duration::from_secs(10),
        "c's own gossiped view learns d also has `name` open, so c's warm buckets split \
         across both b and d rather than naming b the only co-owner",
        async || {
            let (shard_ops, distributed) = distributed_context_for_test(&c, &name);
            let warm_parts: Vec<PartId> = distributed.ownership.current().owned_parts().collect();
            let view = distributed.ownership.current();
            let with_b = warm_parts
                .iter()
                .copied()
                .find(|&bucket| view.owners_of(bucket).contains(&node_b));
            let with_d = warm_parts
                .iter()
                .copied()
                .find(|&bucket| view.owners_of(bucket).contains(&node_d));
            match (with_b, with_d) {
                (Some(part_with_b), Some(part_with_d)) => {
                    built = Some(SplitWarmContext {
                        shard_ops,
                        distributed,
                        warm_parts,
                        part_with_b,
                        part_with_d,
                    });
                    true
                }
                _ => false,
            }
        },
    )
    .await;
    let SplitWarmContext {
        shard_ops: shard_ops_c,
        distributed: distributed_c,
        warm_parts,
        part_with_b,
        part_with_d,
    } = built.expect("wait_until only returns once the closure itself reported ready");
    distributed_c.residency.mark_cold(&warm_parts);

    // One planted key per peer, applied straight to each donor's shard,
    // proves one call pulls data from both groups, not just whichever runs first.
    let key_from_b = key_for_part(part_with_b);
    let key_from_d = key_for_part(part_with_d);
    registered_shard(&b, &name)
        .apply_remote_batch(vec![test_record(key_from_b, "from-b", node_b)])
        .await;
    registered_shard(&d, &name)
        .apply_remote_batch(vec![test_record(key_from_d, "from-d", node_d)])
        .await;

    // b's and d's trackers catch up to c joining on their own refresh
    // cadence, the same real-time delay the single-peer test polls around.
    let mut reconciled: HashSet<PartId> = HashSet::new();
    wait_until(
        Duration::from_secs(10),
        "b's and d's views catch up to c joining, so both peers' rounds reconcile",
        async || {
            reconciled = reconcile_warm_buckets(
                &c,
                &shard_ops_c,
                &name,
                &distributed_c.ownership,
                &distributed_c.residency,
                &warm_parts,
            )
            .await;
            reconciled.len() == warm_parts.len()
        },
    )
    .await;

    for &bucket in &warm_parts {
        assert!(
            !distributed_c.residency.is_cold(bucket),
            "bucket {bucket} clears cold once its digest matches its live co-owner's, \
             whether that co-owner is b or d"
        );
    }
    assert_record_value(&shard_ops_c, key_from_b, "from-b").await;
    assert_record_value(&shard_ops_c, key_from_d, "from-d").await;

    c.shutdown().await;
    d.shutdown().await;
    b.shutdown().await;
}

/// Raw-shard test context, rebuilt on each `wait_until` retry, plus
/// which of `warm_parts` landed with each of the two live co-owners.
struct SplitWarmContext {
    shard_ops: Arc<dyn ShardOps>,
    distributed: DistributedContext,
    warm_parts: Vec<PartId>,
    part_with_b: PartId,
    part_with_d: PartId,
}

/// The first `u32` key (bounded scan) whose part is `part`, for
/// planting a record at a known part.
fn key_for_part(part: PartId) -> u32 {
    (0u32..10_000_000)
        .find(|key| PartId::of_key(&encode_key(key).expect("u32 key encodes")) == part)
        .expect("some u32 key among the first ten million maps to every part")
}

/// The distinct buckets `parts` fall in, for the bucket-keyed
/// [`ShardOps`] reads.
fn buckets_of(parts: &[PartId]) -> Vec<u16> {
    let mut buckets: Vec<u16> = parts.iter().map(|part| part.bucket()).collect();
    buckets.sort_unstable();
    buckets.dedup();
    buckets
}

/// A [`WireRecord`] for `key`/`value`, versioned at `node`, for seeding a shard directly.
fn test_record(key: u32, value: &str, node: NodeId) -> WireRecord {
    WireRecord {
        key: encode_key(&key).expect("u32 encodes"),
        value: Some(bytes::Bytes::from(
            postcard::to_stdvec(&value.to_string()).expect("string encodes"),
        )),
        ver: crate::hlc::Hlc {
            wall_ms: 1,
            logical: 0,
            node,
        },
        expires_at_ms: None,
    }
}

/// Asserts `shard`'s live record for `key` decodes to `expected`.
async fn assert_record_value(shard: &Arc<dyn ShardOps>, key: u32, expected: &str) {
    let recs = shard
        .records_for(vec![encode_key(&key).expect("u32 encodes")])
        .await;
    assert_eq!(
        recs.len(),
        1,
        "key {key} must have landed on the reconciled shard"
    );
    let value: String = recs[0]
        .value
        .as_ref()
        .map(|bytes| postcard::from_bytes(bytes).expect("string decodes"))
        .expect("a live record, not a tombstone");
    assert_eq!(
        value, expected,
        "key {key} must carry the value its donor planted"
    );
}

/// Guards CLAUDE.md's "deleted or expired entries never resurrect":
/// a warm reopen's on-disk replay can carry a stale live copy of a key
/// this node itself deleted before going down. `c` stands in for such a
/// replay, seeded with a live record older than a tombstone `b` holds;
/// `reconcile_warm_buckets` must correct it to the tombstone before the
/// bucket clears cold.
#[tokio::test]
async fn reconcile_warm_buckets_corrects_a_stale_replayed_record_against_a_live_co_owners_tombstone()
 {
    let name = SmolStr::new("reconcile-resurrection-guard");
    let key = 7u32;
    // The default loopback config's short tombstone_ttl would race this
    // test's wait_until against a real GC sweep; a generous ttl avoids that.
    let config = ClusterConfig {
        tombstone_ttl: Duration::from_secs(300),
        ..loopback_config()
    };

    let b = Cluster::builder("cache-it-reconcile-resurrection")
        .seeds(std::iter::empty())
        .config(config.clone())
        .build()
        .await
        .expect("b builds");
    let cache_b = b
        .cache::<u32, String>(name.clone())
        .mode(Mode::distributed())
        .open()
        .await
        .expect("b opens alone, owning every bucket");
    cache_b
        .insert(key, "v1".to_string())
        .await
        .expect("b owns every bucket while alone");
    cache_b
        .remove(&key)
        .await
        .expect("b deletes the key, stamping a tombstone newer than the stale replay below");

    let c = Cluster::builder("cache-it-reconcile-resurrection")
        .seeds([b.local_gossip_addr()])
        .config(config)
        .build()
        .await
        .expect("c builds");
    wait_for_peer_count(&b, 1).await;
    wait_for_peer_count(&c, 1).await;
    c.advertise_cache_mode(&name, Mode::distributed());

    let (shard_ops_c, distributed_c) = distributed_context_for_test(&c, &name);
    let warm_parts: Vec<PartId> = distributed_c.ownership.current().owned_parts().collect();
    assert!(
        !warm_parts.is_empty(),
        "with only b and c eligible at owners=2, c owns every bucket"
    );

    // Stands in for what a warm reopen's replay would install: a stale
    // live copy of `key`, versioned older than b's tombstone above,
    // applied via the same versioned-apply entry point replay uses.
    let stale = WireRecord {
        key: encode_key(&key).expect("u32 encodes"),
        value: Some(bytes::Bytes::from(
            postcard::to_stdvec(&"stale-v1".to_string()).expect("string encodes"),
        )),
        ver: crate::hlc::Hlc {
            wall_ms: 1,
            logical: 0,
            node: c.node_id(),
        },
        expires_at_ms: None,
    };
    shard_ops_c.apply_remote_batch(vec![stale]).await;
    distributed_c.residency.mark_cold(&warm_parts);

    // b's tracker catches up to c joining on its own refresh cadence, as above.
    let mut reconciled: HashSet<PartId> = HashSet::new();
    wait_until(
        Duration::from_secs(10),
        "b's view catches up to c joining, so the round against it reconciles",
        async || {
            reconciled = reconcile_warm_buckets(
                &c,
                &shard_ops_c,
                &name,
                &distributed_c.ownership,
                &distributed_c.residency,
                &warm_parts,
            )
            .await;
            reconciled.len() == warm_parts.len()
        },
    )
    .await;

    for &bucket in &warm_parts {
        assert!(
            !distributed_c.residency.is_cold(bucket),
            "bucket {bucket} clears cold once its digest matches b's in a round against it"
        );
    }

    let recs = shard_ops_c
        .records_for(vec![encode_key(&key).expect("u32 encodes")])
        .await;
    assert_eq!(
        recs.len(),
        1,
        "the reconciliation round must have applied b's tombstone for the key"
    );
    assert!(
        recs[0].is_tombstone(),
        "the stale replayed value this node itself deleted before going down must never be \
         served: the converge-before-serving loop against b, who holds the tombstone, \
         corrects it before the bucket ever clears cold"
    );

    c.shutdown().await;
    b.shutdown().await;
}

/// Pins that when the only co-owner is unreachable, every round fails
/// and the warm bucket stays cold, falling through to the ordinary
/// cold-pull path.
#[tokio::test]
async fn reconcile_warm_buckets_leaves_a_bucket_cold_when_its_only_co_owners_round_fails() {
    let name = SmolStr::new("reconcile-real-round-failed");

    let b = Cluster::builder("cache-it-reconcile-real-round-failed")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("b builds");
    let _cache_b = b
        .cache::<u32, String>(name.clone())
        .mode(Mode::distributed())
        .open()
        .await
        .expect("b opens alone");

    let c = Cluster::builder("cache-it-reconcile-real-round-failed")
        .seeds([b.local_gossip_addr()])
        // Tightened so the test bounds itself instead of waiting out loopback_config's 5s.
        .config(loopback_config().with(|config| {
            config.state_transfer_budget = Duration::from_secs(1);
        }))
        .build()
        .await
        .expect("c builds");
    wait_for_peer_count(&b, 1).await;
    wait_for_peer_count(&c, 1).await;
    // Without this, b never learns c has `name` open, and every round answers Stale.
    c.advertise_cache_mode(&name, Mode::distributed());

    let (shard_ops_c, distributed_c) = distributed_context_for_test(&c, &name);
    let warm_parts: Vec<PartId> = distributed_c.ownership.current().owned_parts().collect();
    assert!(!warm_parts.is_empty(), "{warm_parts:?}");
    distributed_c.residency.mark_cold(&warm_parts);

    // Warms up to a first converged round first, so the round below
    // fails on b's unreachability specifically, not an unsettled view.
    wait_until(
        Duration::from_secs(10),
        "b's view catches up to c joining, so a warm-up round against it reconciles",
        async || {
            !reconcile_warm_buckets(
                &c,
                &shard_ops_c,
                &name,
                &distributed_c.ownership,
                &distributed_c.residency,
                &warm_parts,
            )
            .await
            .is_empty()
        },
    )
    .await;
    distributed_c.residency.mark_cold(&warm_parts);

    // c's gossip-derived live list still names b live briefly, so the
    // round below attempts and fails on the dial, not a no-co-owner skip.
    b.shutdown().await;

    let started = Instant::now();
    let reconciled = reconcile_warm_buckets(
        &c,
        &shard_ops_c,
        &name,
        &distributed_c.ownership,
        &distributed_c.residency,
        &warm_parts,
    )
    .await;

    assert!(
        reconciled.is_empty(),
        "a round against an unreachable co-owner never reconciles any bucket"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the retries stop at the 1s time budget or when b leaves the live list, never \
         running on to loopback_config's 5s: took {:?}",
        started.elapsed()
    );
    for &bucket in &warm_parts {
        assert!(
            distributed_c.residency.is_cold(bucket),
            "bucket {bucket} stays cold once its only co-owner's round fails, falling \
             through to the ordinary cold-pull path rather than serving unverified"
        );
    }

    c.shutdown().await;
}

/// Every part of a bucket keys `0..total` never touch, so both sides'
/// empty digests match once b's view stops answering `Stale`, untouched
/// by real data.
fn unused_bucket(total: u32) -> Vec<PartId> {
    let used: HashSet<u16> = (0..total)
        .map(|key| bucket_of(&encode_key(&key).expect("u32 key encodes")))
        .collect();
    let bucket = (0..=u16::MAX)
        .take(crate::store::BUCKET_COUNT)
        .find(|bucket| !used.contains(bucket))
        .expect("BUCKET_COUNT buckets is far more than a small test's `total` can fill");
    PartId::of_bucket(bucket).collect()
}

/// Polls `probe_bucket` (an [`unused_bucket`]) until a round against it
/// stops answering `Stale`: b's tracker catching up to c joining. An
/// untouched bucket avoids repairing the real data the caller tests.
async fn wait_for_live_co_owner_view_to_settle(
    c: &Cluster,
    shard_ops_c: &Arc<dyn ShardOps>,
    name: &SmolStr,
    node_b: NodeId,
    probe_bucket: &[PartId],
) {
    wait_until(
        Duration::from_secs(10),
        "b's view catches up to c joining, so a round against it stops answering Stale",
        async || {
            !anti_entropy::run_round_for_parts(c.mesh(), shard_ops_c, name, node_b, probe_bucket)
                .await
                .failed
        },
    )
    .await;
}

/// A single `reconcile_warm_buckets` call must itself loop more than one
/// round to close a real gap: a round reports a bucket `still_diverged`
/// from the mismatch list before the repair lands, so only a later
/// round's fresh digest exchange reports it `matched`.
/// [`RECONCILE_MAX_ROUNDS`] gives the loop that confirming round.
///
/// `b`'s view is warmed up first against an untouched bucket, isolating
/// the two-rounds-needed behavior from b's own refresh-cadence delay.
#[tokio::test]
async fn reconcile_warm_buckets_loops_a_bounded_number_of_times_when_one_round_cannot_close_the_gap()
 {
    const TOTAL: u32 = 50;
    let name = SmolStr::new("reconcile-needs-two-rounds");

    let b = Cluster::builder("cache-it-reconcile-needs-two-rounds")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("b builds");
    let node_b = b.node_id();
    let cache_b = b
        .cache::<u32, String>(name.clone())
        .mode(Mode::distributed())
        .open()
        .await
        .expect("b opens alone, owning every bucket");
    for key in 0..TOTAL {
        cache_b
            .insert(key, format!("v{key}"))
            .await
            .expect("b owns every bucket while alone");
    }
    let probe_bucket = unused_bucket(TOTAL);
    let divergent_parts: HashSet<PartId> = (0..TOTAL)
        .map(|key| PartId::of_key(&encode_key(&key).expect("u32 key encodes")))
        .collect();

    let c = Cluster::builder("cache-it-reconcile-needs-two-rounds")
        .seeds([b.local_gossip_addr()])
        .config(loopback_config())
        .build()
        .await
        .expect("c builds");
    wait_for_peer_count(&b, 1).await;
    wait_for_peer_count(&c, 1).await;
    c.advertise_cache_mode(&name, Mode::distributed());

    let (shard_ops_c, distributed_c) = distributed_context_for_test(&c, &name);
    let warm_parts: Vec<PartId> = distributed_c.ownership.current().owned_parts().collect();
    assert!(
        !warm_parts.is_empty(),
        "with only b and c eligible at owners=2, c owns every bucket"
    );
    distributed_c.residency.mark_cold(&warm_parts);

    wait_for_live_co_owner_view_to_settle(&c, &shard_ops_c, &name, node_b, &probe_bucket).await;

    // A single call: if it ran only one round, a repaired bucket would
    // still report `still_diverged`.
    let reconciled = reconcile_warm_buckets(
        &c,
        &shard_ops_c,
        &name,
        &distributed_c.ownership,
        &distributed_c.residency,
        &warm_parts,
    )
    .await;

    assert_eq!(
        reconciled.len(),
        warm_parts.len(),
        "a single call's own internal loop, not an external retry, closes the gap: round \
         one repairs the divergence but reports it still diverged, round two's fresh digest \
         exchange confirms the match"
    );
    for &bucket in &divergent_parts {
        assert!(
            !distributed_c.residency.is_cold(bucket),
            "bucket {bucket} clears cold once the loop's confirming round finds no mismatch"
        );
    }
    let recovered: usize = shard_ops_c
        .entries_for_buckets(buckets_of(&warm_parts))
        .await
        .into_iter()
        .map(|(_, entries)| entries.len())
        .sum();
    assert_eq!(
        recovered, TOTAL as usize,
        "the loop's first round actually pulled b's data in"
    );

    c.shutdown().await;
    b.shutdown().await;
}

/// The other side of the loop-needs-two-rounds test: a byte budget too
/// low for a second round makes [`should_keep_reconciling`] refuse it,
/// so a repaired-but-unconfirmed bucket stays cold and unverified rather
/// than clearing via `mark_serving` on an unconfirmed repair. Untouched
/// buckets in the same round still converge immediately.
#[tokio::test]
async fn reconcile_warm_buckets_stops_at_the_byte_budget_and_leaves_the_bucket_cold_and_unverified()
{
    const TOTAL: u32 = 50;
    let name = SmolStr::new("reconcile-byte-budget-stop");

    let b = Cluster::builder("cache-it-reconcile-byte-budget-stop")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("b builds");
    let node_b = b.node_id();
    let cache_b = b
        .cache::<u32, String>(name.clone())
        .mode(Mode::distributed())
        .open()
        .await
        .expect("b opens alone, owning every bucket");
    for key in 0..TOTAL {
        cache_b
            .insert(key, format!("v{key}"))
            .await
            .expect("b owns every bucket while alone");
    }
    let probe_bucket = unused_bucket(TOTAL);
    let divergent_parts: HashSet<PartId> = (0..TOTAL)
        .map(|key| PartId::of_key(&encode_key(&key).expect("u32 key encodes")))
        .collect();

    // A budget so small the first repair byte exceeds it: one round only.
    let config = ClusterConfig {
        reconcile_byte_budget: 1,
        ..loopback_config()
    };
    let c = Cluster::builder("cache-it-reconcile-byte-budget-stop")
        .seeds([b.local_gossip_addr()])
        .config(config)
        .build()
        .await
        .expect("c builds");
    wait_for_peer_count(&b, 1).await;
    wait_for_peer_count(&c, 1).await;
    c.advertise_cache_mode(&name, Mode::distributed());

    let (shard_ops_c, distributed_c) = distributed_context_for_test(&c, &name);
    let warm_parts: Vec<PartId> = distributed_c.ownership.current().owned_parts().collect();
    assert!(
        !warm_parts.is_empty(),
        "with only b and c eligible at owners=2, c owns every bucket"
    );
    distributed_c.residency.mark_cold(&warm_parts);

    wait_for_live_co_owner_view_to_settle(&c, &shard_ops_c, &name, node_b, &probe_bucket).await;

    let reconciled = reconcile_warm_buckets(
        &c,
        &shard_ops_c,
        &name,
        &distributed_c.ownership,
        &distributed_c.residency,
        &warm_parts,
    )
    .await;

    assert!(
        divergent_parts
            .iter()
            .all(|bucket| !reconciled.contains(bucket)),
        "the byte budget stops the loop after its one repairing round, before the \
         confirming round that would have reconciled the genuinely divergent buckets"
    );
    for &bucket in &divergent_parts {
        assert!(
            distributed_c.residency.is_cold(bucket),
            "bucket {bucket} stays cold: the budget cut the loop off before a confirming \
             round ever ran for it, so no ResidencySet call was ever made for it"
        );
    }
    let recovered: usize = shard_ops_c
        .entries_for_buckets(buckets_of(&warm_parts))
        .await
        .into_iter()
        .map(|(_, entries)| entries.len())
        .sum();
    assert_eq!(
        recovered, TOTAL as usize,
        "the one round the budget allowed still genuinely repaired the divergence -- the \
         bucket is left unverified, not un-repaired"
    );

    c.shutdown().await;
    b.shutdown().await;
}

/// Joins a fresh node onto `cluster_name`'s chitchat cluster (three
/// nodes total once this and the seed and a later joiner are all up, so
/// `owners = 2` leaves each bucket owned by exactly two of the three,
/// and every node fails to own a real share of buckets) and opens
/// `cache_name` as `Mode::distributed()`, two owners, on it.
async fn join_distributed(
    seed_addr: SocketAddr,
    cluster_name: &'static str,
    cache_name: &'static str,
    peer_count: usize,
) -> (Cluster, Cache<u32, String>) {
    let cluster = Cluster::builder(cluster_name)
        .seeds([seed_addr])
        .config(loopback_config())
        .build()
        .await
        .expect("node builds");
    wait_for_peer_count(&cluster, peer_count).await;
    let cache = tokio::time::timeout(
        Duration::from_secs(20),
        cluster
            .cache::<u32, String>(cache_name)
            .mode(Mode::distributed())
            .open(),
    )
    .await
    .expect("open completes within the state-transfer budget")
    .expect("open succeeds");
    (cluster, cache)
}

/// A three-node `Mode::Distributed { owners: 2 }` cluster, all open on
/// `cache_name`, plus a key whose bucket the first node does not own
/// (its live owners are exactly the second and third nodes).
async fn three_node_distributed(
    cluster_name: &'static str,
    cache_name: &'static str,
) -> (
    (Cluster, Cache<u32, String>),
    (Cluster, Cache<u32, String>),
    (Cluster, Cache<u32, String>),
    u32,
) {
    three_node_distributed_with_a(cluster_name, cache_name, loopback_config()).await
}

/// [`three_node_distributed`] with the first node built on `a_config`.
async fn three_node_distributed_with_a(
    cluster_name: &'static str,
    cache_name: &'static str,
    a_config: ClusterConfig,
) -> (
    (Cluster, Cache<u32, String>),
    (Cluster, Cache<u32, String>),
    (Cluster, Cache<u32, String>),
    u32,
) {
    let a = Cluster::builder(cluster_name)
        .seeds(std::iter::empty())
        .config(a_config)
        .build()
        .await
        .expect("node a builds");
    let cache_a = a
        .cache::<u32, String>(cache_name)
        .mode(Mode::Distributed {
            owners: NonZeroU8::new(2).expect("nonzero"),
        })
        .open()
        .await
        .expect("a opens alone, owning everything");
    let seed = a.local_gossip_addr();

    let (b, cache_b) = join_distributed(seed, cluster_name, cache_name, 1).await;
    wait_for_peer_count(&a, 1).await;
    let (c, cache_c) = join_distributed(seed, cluster_name, cache_name, 2).await;
    wait_for_peer_count(&a, 2).await;
    wait_for_peer_count(&b, 2).await;

    // A key whose bucket `a` does not own: with three eligible nodes and
    // owners = 2, roughly a third of keys land here. `a`'s ownership
    // view only recomputes once its background refresh task has
    // observed both the membership change above and the gossiped
    // cache-mode advertisement `b`/`c` sent on their own `open()`,
    // strictly a separate, slightly later event than peer liveness,
    // so this polls rather than searching the instant peers converge.
    let unowned_key = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(key) =
                (0..10_000u32).find(|&k| !cache_a.owners_of(&k).contains(&a.node_id()))
            {
                return key;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("a's ownership view converges to include b and c within the bound");

    ((a, cache_a), (b, cache_b), (c, cache_c), unowned_key)
}

/// `b` accepts any stamp; `a` bounds skew at one second. A record
/// stamped an hour ahead that `b` holds never reaches `a` through
/// anti-entropy, `a`'s clock never moves toward it, and `b`, whose
/// clock absorbed it, has its later writes refused by `a` too.
#[tokio::test]
async fn a_record_stamped_past_max_clock_skew_never_reaches_a_bounded_node() {
    let name = "cache-it-clock-skew";
    let a = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(loopback_config().with(|c| c.max_clock_skew = Some(Duration::from_secs(1))))
        .build()
        .await
        .expect("a builds");
    let b = Cluster::builder(name)
        .seeds([a.local_gossip_addr()])
        .config(loopback_config().with(|c| c.max_clock_skew = None))
        .build()
        .await
        .expect("b builds");
    wait_for_peer_count(&a, 1).await;
    let open = |cluster: &Cluster| {
        cluster
            .cache::<u32, String>("prices")
            .mode(Mode::Replicated)
            .open()
    };
    let cache_a = open(&a).await.expect("a opens");
    let cache_b = open(&b).await.expect("b opens");

    cache_b.insert(2, "before".into()).await.expect("insert");
    wait_until(
        Duration::from_secs(10),
        "b's write stamped by its own clock reaches a",
        async || cache_a.get(&2).await.is_some(),
    )
    .await;

    let ahead = crate::hlc::Hlc {
        wall_ms: crate::store::now_ms() + 3_600_000,
        logical: 0,
        node: b.node_id(),
    };
    ShardOps::apply_remote(
        cache_b.shard.as_ref(),
        crate::wire::WireRecord {
            key: bytes::Bytes::from(postcard::to_stdvec(&1u32).expect("key encodes")),
            value: Some(bytes::Bytes::from(
                postcard::to_stdvec(&"ahead".to_string()).expect("value encodes"),
            )),
            ver: ahead,
            expires_at_ms: None,
        },
    )
    .await;
    assert_eq!(cache_b.get(&1).await.as_deref(), Some("ahead"));
    cache_b.insert(4, "after".into()).await.expect("insert");

    // Ten anti-entropy intervals: every round offers a both keys.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        cache_a.get(&1).await,
        None,
        "a refuses the record stamped an hour ahead"
    );
    assert_eq!(
        cache_a.get(&4).await,
        None,
        "b's clock absorbed the hour, so its next write is refused as well"
    );

    cache_a.insert(3, "local".into()).await.expect("insert");
    let key = bytes::Bytes::from(postcard::to_stdvec(&3u32).expect("key encodes"));
    let held = ShardOps::bucket_entries(cache_a.shard.as_ref(), crate::store::bucket_of(&key))
        .await
        .into_iter()
        .find(|entry| entry.key == key)
        .expect("a holds its own write");
    assert!(
        held.version.wall_ms < ahead.wall_ms - 3_000_000,
        "a's clock never took the refused stamp"
    );

    b.shutdown().await;
    a.shutdown().await;
}

#[tokio::test]
async fn fetch_returns_local_value_without_a_network_request_when_this_node_owns_the_bucket() {
    let cluster = Cluster::builder("cache-it-fetch-local")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");
    let cache = cluster
        .cache::<u32, String>("prices")
        .mode(Mode::Distributed {
            owners: NonZeroU8::new(2).expect("nonzero"),
        })
        .open()
        .await
        .expect("a solo node opens warm, owning every bucket");
    cache.insert(1, "one".to_string()).await.expect("insert");

    assert_eq!(
        cache.fetch(&1).await.expect("fetch succeeds"),
        Some("one".to_string()),
        "a solo node owns every bucket, so fetch reads locally"
    );
    assert_eq!(
        cache.fetch(&2).await.expect("fetch succeeds"),
        None,
        "a genuine local miss is Ok(None), not an error"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn fetch_reaches_a_remote_owner_and_returns_its_value() {
    let ((a, cache_a), (b, _cache_b), (c, cache_c), unowned_key) =
        three_node_distributed("cache-it-fetch-remote", "prices").await;

    // Written through whichever of the two real owners it lands on;
    // `insert` on a non-owner forwards, so writing through `c` (an
    // owner or not) always reaches the true owners either way.
    cache_c
        .insert(unowned_key, "remote".to_string())
        .await
        .expect("insert");

    wait_until(
        Duration::from_secs(10),
        "fetch reaches a real owner and returns its value within the bound",
        async || matches!(cache_a.fetch(&unowned_key).await, Ok(Some(value)) if value == "remote"),
    )
    .await;

    assert_eq!(
        cache_a.get(&unowned_key).await,
        None,
        "fetch never promotes the value into a's own local store"
    );

    a.shutdown().await;
    b.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn fetch_asks_the_other_owner_for_a_miss_in_a_cold_bucket_and_is_unavailable_while_every_owner_is_cold()
 {
    // Two nodes, two owners per bucket: both own every bucket. Once
    // both are warm, `a` drops its copy of a key and marks the bucket
    // cold again by hand, so a fetch on `a` has to ask `b`; then `b`
    // does the same, and `a`'s fetch is `FetchUnavailable`: no warm
    // copy vouches for the miss.
    // Anti-entropy is effectively off: it would repair a dropped copy
    // from the co-owner and race every step below.
    let name = "distributed-cold-fetch";
    let mut config = loopback_config();
    config.ae_interval = Duration::from_secs(3600);
    config.tombstone_ttl = config.bucket_release_window();
    let a = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(config.clone())
        .build()
        .await
        .expect("node a builds");
    let cache_a = a
        .cache::<u32, String>(name)
        .mode(Mode::distributed())
        .open()
        .await
        .expect("a opens alone");
    let b = Cluster::builder(name)
        .seeds([a.local_gossip_addr()])
        .config(config)
        .build()
        .await
        .expect("node b builds");
    wait_for_peer_count(&b, 1).await;
    let cache_b = b
        .cache::<u32, String>(name)
        .mode(Mode::distributed())
        .open()
        .await
        .expect("b opens");
    wait_for_peer_count(&a, 1).await;
    cache_b
        .insert(7, "seven".to_string())
        .await
        .expect("insert");
    tokio::time::timeout(Duration::from_secs(10), async {
        while cache_a.get(&7).await.is_none() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the write reaches its other owner");
    // Both warm-up pulls have landed: nothing re-delivers a copy the
    // steps below drop by hand.
    tokio::time::timeout(Duration::from_secs(20), async {
        while !(a.is_warm(&SmolStr::new(name)) && b.is_warm(&SmolStr::new(name))) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("both nodes finish warming up");
    let bucket = PartId::of_key(&encode_key(&7u32).expect("u32 encodes"));
    let residency_a = cache_a.shard.residency().expect("a is distributed");
    let residency_b = cache_b.shard.residency().expect("b is distributed");
    // Both views list both nodes, or the fetch below ends `Stale`.
    tokio::time::timeout(Duration::from_secs(10), async {
        while cache_a.owners_of(&7).len() != 2 || cache_b.owners_of(&7).len() != 2 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("both views settle on two owners");

    cache_a.invalidate_local(&7).await;
    residency_a.mark_cold(&[bucket]);
    assert_eq!(
        cache_a.fetch(&7).await.expect("fetch reaches b").as_deref(),
        Some("seven"),
        "a cold local miss is answered by the other owner"
    );
    assert_eq!(cache_a.get(&7).await, None, "fetch never promotes locally");

    cache_b.invalidate_local(&7).await;
    residency_b.mark_cold(&[bucket]);
    assert!(
        matches!(
            cache_a.fetch(&7).await,
            Err(CacheError::FetchUnavailable { .. })
        ),
        "every owner cold leaves no copy to vouch for a miss"
    );

    residency_a.clear_cold(&[bucket]);
    assert_eq!(
        cache_a.fetch(&7).await.expect("local"),
        None,
        "a warm bucket answers its own miss without asking anyone"
    );

    cache_b.close().await;
    cache_a.close().await;
    b.shutdown().await;
    a.shutdown().await;
}

/// A part this node owns again after losing it holds whatever it kept
/// from before, which misses every write and delete made while another
/// node owned it. `a` holds such a stale copy of `key`, which `b`
/// deleted, and regains the part through the rebalance planner: while
/// the pull runs, `fetch` must answer from `b`, never the stale hit.
#[tokio::test]
async fn fetch_never_serves_a_stale_hit_from_a_regained_part_while_its_pull_runs() {
    let name = SmolStr::new("fetch-regained-stale");
    let (id_a, id_b) = (NodeId::from(0xa), NodeId::from(0xb));
    // The view a gave the part up under: two nodes since gone outranked
    // a for key's part.
    let without_a = OwnershipView::compute_at(
        id_a,
        vec![id_b, NodeId::from(0xc), NodeId::from(0xd)],
        std::num::NonZeroU8::new(2).expect("nonzero"),
        crate::ownership::Granularity::Part,
    );
    let key = (0..64u32)
        .find(|key| !without_a.owns(PartId::of_key(&encode_key(key).expect("u32 encodes"))))
        .expect("a owns about half the parts in a four-node view");
    let config = ClusterConfig {
        tombstone_ttl: Duration::from_secs(300),
        ..loopback_config()
    };

    let b = Cluster::builder("cache-it-fetch-regained")
        .node_id(id_b)
        .seeds(std::iter::empty())
        .config(config.clone())
        .build()
        .await
        .expect("b builds");
    let cache_b = b
        .cache::<u32, String>(name.clone())
        .mode(Mode::distributed())
        .open()
        .await
        .expect("b opens alone, owning every part");
    cache_b
        .insert(key, "v1".to_string())
        .await
        .expect("b owns every part while alone");
    cache_b
        .remove(&key)
        .await
        .expect("b deletes the key while a does not own its part");

    let a = Cluster::builder("cache-it-fetch-regained")
        .node_id(id_a)
        .seeds([b.local_gossip_addr()])
        .config(config)
        .build()
        .await
        .expect("a builds");
    wait_for_peer_count(&b, 1).await;
    wait_for_peer_count(&a, 1).await;
    a.advertise_cache_mode(&name, Mode::distributed());

    let shard = Shard::<u32, String>::new(
        name.clone(),
        Mode::distributed(),
        a.node_id(),
        u64::MAX,
        None,
        None,
    );
    let (shard, distributed) = attach_ownership(shard, &a, &name, Mode::distributed());
    let distributed = distributed.expect("Mode::distributed always attaches a context");
    let cache_a = Cache {
        shard: Arc::new(shard),
        cluster: a.clone(),
        cancel: CancellationToken::new(),
        tasks: TaskTracker::new(),
    };
    let part = PartId::of_key(&encode_key(&key).expect("u32 encodes"));
    let new = distributed.ownership.current();

    // What a kept from owning the part before: the write, not the delete.
    let shard_ops_a = Arc::clone(&cache_a.shard) as Arc<dyn ShardOps>;
    let stale = WireRecord {
        key: encode_key(&key).expect("u32 encodes"),
        value: Some(bytes::Bytes::from(
            postcard::to_stdvec(&"v1".to_string()).expect("string encodes"),
        )),
        ver: crate::hlc::Hlc {
            wall_ms: 1,
            logical: 0,
            node: b.node_id(),
        },
        expires_at_ms: None,
    };
    shard_ops_a.apply_remote_batch(vec![stale]).await;
    assert_eq!(cache_a.get(&key).await, Some("v1".to_string()));

    let held = shard_ops_a.held_parts().await;
    let plan = crate::cluster::rebalance::plan_view_change(
        &without_a,
        &without_a,
        &new,
        &held,
        &PartSet::new(),
    );
    assert!(plan.to_pull.contains(&part), "a owns key's part again");
    crate::cluster::rebalance::mark_pulling(&distributed.residency, &plan);

    assert_eq!(
        cache_a.fetch(&key).await.expect("b answers"),
        None,
        "a's copy of the regained part predates b's delete: only b's tombstone answers"
    );

    cache_b.close().await;
    cache_a.close().await;
    b.shutdown().await;
    a.shutdown().await;
}

/// Pins the unverified-bucket guard at the `Cache::fetch` layer: `a`'s
/// shard is seeded with a stale replay of `key` that `b` deleted first.
/// `fetch` must ask `b` while the bucket is unverified,
/// then answer locally once reconciliation clears the marker.
#[cfg(feature = "spill")]
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one end-to-end two-node scenario (stage a stale replay, assert the unverified \
              guard, reconcile, assert it serves locally again): splitting it would only \
              scatter state (a, b, cache_a, cache_b, the shared key/bucket) across helper \
              signatures"
)]
async fn fetch_never_returns_a_local_hit_from_an_unverified_bucket_until_reconciliation_clears_it()
{
    let name = SmolStr::new("fetch-unverified-guard");
    let key = 7u32;
    let config = ClusterConfig {
        tombstone_ttl: Duration::from_secs(300),
        ..loopback_config()
    };

    let b = Cluster::builder("cache-it-fetch-unverified")
        .seeds(std::iter::empty())
        .config(config.clone())
        .build()
        .await
        .expect("b builds");
    let cache_b = b
        .cache::<u32, String>(name.clone())
        .mode(Mode::distributed())
        .open()
        .await
        .expect("b opens alone, owning every bucket");
    cache_b
        .insert(key, "v1".to_string())
        .await
        .expect("b owns every bucket while alone");
    cache_b
        .remove(&key)
        .await
        .expect("b deletes the key: the co-owner's own delete during a's downtime");

    let a = Cluster::builder("cache-it-fetch-unverified")
        .seeds([b.local_gossip_addr()])
        .config(config)
        .build()
        .await
        .expect("a builds");
    wait_for_peer_count(&b, 1).await;
    wait_for_peer_count(&a, 1).await;
    a.advertise_cache_mode(&name, Mode::distributed());

    // Built directly via `attach_ownership`, keeping the typed `Shard`
    // so a real `Cache` can wrap it.
    let shard = Shard::<u32, String>::new(
        name.clone(),
        Mode::distributed(),
        a.node_id(),
        u64::MAX,
        None,
        None,
    );
    let (shard, distributed) = attach_ownership(shard, &a, &name, Mode::distributed());
    let distributed = distributed.expect("Mode::distributed always attaches a context");
    let cache_a = Cache {
        shard: Arc::new(shard),
        cluster: a.clone(),
        cancel: CancellationToken::new(),
        tasks: TaskTracker::new(),
    };

    let warm_parts: Vec<PartId> = distributed.ownership.current().owned_parts().collect();
    assert!(
        !warm_parts.is_empty(),
        "with only b and a eligible at owners=2, a owns every bucket"
    );
    let bucket = PartId::of_key(&encode_key(&key).expect("u32 encodes"));
    assert!(
        warm_parts.contains(&bucket),
        "with owners=2 and exactly two real nodes, a owns key's part too"
    );

    // Stands in for what a warm reopen's replay would install: a stale
    // live copy of `key`, via the same versioned-apply entry point replay uses.
    let shard_ops_a = Arc::clone(&cache_a.shard) as Arc<dyn ShardOps>;
    let stale = WireRecord {
        key: encode_key(&key).expect("u32 encodes"),
        value: Some(bytes::Bytes::from(
            postcard::to_stdvec(&"stale".to_string()).expect("string encodes"),
        )),
        ver: crate::hlc::Hlc {
            wall_ms: 1,
            logical: 0,
            node: a.node_id(),
        },
        expires_at_ms: None,
    };
    shard_ops_a.apply_remote_batch(vec![stale]).await;
    distributed.residency.mark_cold(&warm_parts);
    distributed.residency.mark_unverified(&warm_parts);

    assert_eq!(
        cache_a.fetch(&key).await.expect("b answers"),
        None,
        "the bucket is unverified: a's own stale hit is never trusted, so the fetch must \
         ask b, whose real tombstone is the correct answer"
    );
    assert_eq!(
        cache_a.get(&key).await,
        Some("stale".to_string()),
        "the stale record is still physically present locally; only the fetch path \
         refuses to trust it while unverified"
    );

    let mut reconciled: HashSet<PartId> = HashSet::new();
    wait_until(
        Duration::from_secs(10),
        "b's view catches up to a joining, so the round against it reconciles",
        async || {
            reconciled = reconcile_warm_buckets(
                &a,
                &shard_ops_a,
                &name,
                &distributed.ownership,
                &distributed.residency,
                &warm_parts,
            )
            .await;
            reconciled.len() == warm_parts.len()
        },
    )
    .await;
    assert!(
        !distributed.residency.is_unverified(bucket),
        "reconciliation clears the unverified mark alongside cold"
    );

    // b goes away entirely, so a's answer below must come from its own corrected state.
    cache_b.close().await;
    b.shutdown().await;

    assert_eq!(
        cache_a
            .fetch(&key)
            .await
            .expect("local, with no owner left to ask"),
        None,
        "reconciliation both corrected the stale copy and cleared unverified: the bucket \
         now serves its own tombstone locally, with no co-owner needed"
    );

    cache_a.close().await;
    a.shutdown().await;
}

#[tokio::test]
async fn fetch_retries_a_stale_owner_for_one_fetch_timeout_before_giving_up() {
    // Two solo nodes of one cluster with no seeds, so they never gossip,
    // `b` injected into `a`'s mesh by hand, so `b`'s view hash never
    // matches the one `a` sends: every
    // fetch to `b` is answered `Stale`. `a`'s own view lists `b` and a
    // phantom third node, so it has buckets it does not own to fetch.
    let name = "distributed-fetch-stale";
    let mut config = loopback_config();
    config.fetch_timeout = Duration::from_millis(300);
    let a = Cluster::builder("distributed-fetch-stale")
        .seeds(std::iter::empty())
        .config(config)
        .build()
        .await
        .expect("node a builds");
    let b = Cluster::builder("distributed-fetch-stale")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node b builds");
    let cache_b = b
        .cache::<u32, String>(name)
        .mode(Mode::distributed())
        .open()
        .await
        .expect("b opens alone");
    cache_b.insert(1, "one".to_string()).await.expect("insert");
    let peer_b = b.local_peer();
    a.mesh().update_peers(vec![peer_b.clone()]);

    let phantom = NodeId::from(u64::MAX);
    let owners = Mode::DEFAULT_OWNERS;
    let (tracker, tx) = crate::ownership::OwnershipTracker::seed(
        a.node_id(),
        &[],
        &std::collections::HashMap::new(),
        &SmolStr::new(name),
        owners,
    );
    let view = Arc::new(crate::ownership::OwnershipView::compute(
        a.node_id(),
        vec![a.node_id(), peer_b.node, phantom],
        owners,
    ));
    tx.send(Arc::clone(&view)).expect("receiver alive");
    let shard = Shard::<u32, String>::new(
        SmolStr::new(name),
        Mode::distributed(),
        a.node_id(),
        u64::MAX,
        None,
        None,
    )
    .with_ownership(tracker, Arc::new(crate::ownership::ResidencySet::new()));
    let cache_a = Cache {
        shard: Arc::new(shard),
        cluster: a.clone(),
        cancel: CancellationToken::new(),
        tasks: TaskTracker::new(),
    };
    // A key `b` does not hold: a held record answers a fetch whatever
    // the views say, so only a miss exercises the stale retry.
    let key = (2..1_000_000u32)
        .find(|key| {
            let bucket = PartId::of_key(&encode_key(key).expect("u32 encodes"));
            !view.owns(bucket) && view.owners_of(bucket).contains(&peer_b.node)
        })
        .expect("a key owned by b and not by a");

    // A bucket `a` owns with only the phantom as co-owner: cold, its
    // one co-owner unreachable, a miss `a` cannot vouch for; warm, its
    // own miss is the answer.
    let cold_key = (2..1_000_000u32)
        .find(|key| {
            let bucket = PartId::of_key(&encode_key(key).expect("u32 encodes"));
            view.owns(bucket) && !view.owners_of(bucket).contains(&peer_b.node)
        })
        .expect("a key owned by a and the phantom");
    let cold_bucket = PartId::of_key(&encode_key(&cold_key).expect("u32 encodes"));
    let residency = cache_a.shard.residency().expect("a is distributed");
    residency.mark_cold(&[cold_bucket]);
    assert!(
        matches!(
            cache_a.fetch(&cold_key).await,
            Err(CacheError::FetchUnavailable { .. })
        ),
        "a cold owner with its only co-owner unreachable cannot vouch for a miss"
    );
    residency.clear_cold(&[cold_bucket]);
    assert_eq!(
        cache_a.fetch(&cold_key).await.expect("local"),
        None,
        "a warm owner answers its own miss"
    );

    let started = tokio::time::Instant::now();
    let result = cache_a.fetch(&key).await;
    let elapsed = started.elapsed();
    assert!(
        matches!(result, Err(CacheError::FetchUnavailable { .. })),
        "a permanently stale owner and an unreachable one leave nothing to answer: {result:?}"
    );
    assert!(
        elapsed >= Duration::from_millis(300),
        "the stale owner is retried for a whole fetch_timeout: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "the retries are bounded by the owners' windows: {elapsed:?}"
    );

    cache_b.close().await;
    b.shutdown().await;
    a.shutdown().await;
}

/// A one-node cluster named `name` on `config`, with no seeds and no
/// peers, for a hand-built cache to read through.
async fn solo_cluster(name: &str, config: ClusterConfig) -> Cluster {
    Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(config)
        .build()
        .await
        .expect("node builds")
}

/// The part `key`'s encoding falls in.
fn part_of(key: u32) -> PartId {
    PartId::of_key(&encode_key(&key).expect("u32 encodes"))
}

/// A `Mode::Distributed` cache of `cluster`'s whose shard decides under
/// `view`, with its residency set to mark parts in and the sender its
/// tracker reads, to publish a further view through.
fn cache_publishing_views(
    cluster: &Cluster,
    name: &str,
    view: &Arc<crate::ownership::OwnershipView>,
) -> (
    Cache<u32, String>,
    Arc<crate::ownership::ResidencySet>,
    watch::Sender<Arc<crate::ownership::OwnershipView>>,
) {
    let owners = std::num::NonZeroU8::MIN;
    let (tracker, tx) = crate::ownership::OwnershipTracker::seed(
        cluster.node_id(),
        &[],
        &std::collections::HashMap::new(),
        &SmolStr::new(name),
        owners,
    );
    tx.send(Arc::clone(view)).expect("receiver alive");
    let residency = Arc::new(crate::ownership::ResidencySet::new());
    let shard = Shard::<u32, String>::new(
        SmolStr::new(name),
        Mode::distributed(),
        cluster.node_id(),
        u64::MAX,
        None,
        None,
    )
    .with_ownership(tracker, Arc::clone(&residency));
    let cache = Cache {
        shard: Arc::new(shard),
        cluster: cluster.clone(),
        cancel: CancellationToken::new(),
        tasks: TaskTracker::new(),
    };
    (cache, residency, tx)
}

/// `cache`'s explanation of `key`, and its `Mode::Distributed` half
/// split out of it.
async fn explain_distributed(
    cache: &Cache<u32, String>,
    key: u32,
) -> (ReadExplanation, DistributedRead) {
    let explanation = cache.explain(&key).await.expect("u32 encodes");
    let read = explanation
        .distributed
        .clone()
        .expect("a distributed cache explains its distribution");
    (explanation, read)
}

/// A `Mode::Distributed` cache of `cluster`'s whose shard decides under
/// `view`, with its residency set to mark parts in.
fn cache_under_view(
    cluster: &Cluster,
    name: &str,
    view: &Arc<crate::ownership::OwnershipView>,
) -> (Cache<u32, String>, Arc<crate::ownership::ResidencySet>) {
    let (cache, residency, _view_tx) = cache_publishing_views(cluster, name, view);
    (cache, residency)
}

/// `owner_answer`'s second local branch: a fetch that set out under a
/// view where `b` owns the key's part, answered `Stale` by `b`, finds
/// this node's view moved to one where it owns the part itself and
/// decides under that view by the same verdict as the first branch: a
/// warm part answers its own hit or miss, while a cold or stale one
/// asks the other owners, here none.
#[tokio::test]
async fn owner_answer_decides_locally_under_a_view_that_moved_during_the_fetch() {
    let name = "owner-answer-moved-view";
    let a = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node a builds");
    let b = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node b builds");
    let cache_b = b
        .cache::<u32, String>(name)
        .mode(Mode::distributed())
        .open()
        .await
        .expect("b opens alone");
    let peer_b = b.local_peer();
    a.mesh().update_peers(vec![peer_b.clone()]);

    let owners = std::num::NonZeroU8::MIN;
    let old = Arc::new(crate::ownership::OwnershipView::compute(
        a.node_id(),
        vec![a.node_id(), peer_b.node],
        owners,
    ));
    let fresh = Arc::new(crate::ownership::OwnershipView::compute(
        a.node_id(),
        vec![a.node_id()],
        owners,
    ));
    let keys = (2..1_000_000u32)
        .filter(|key| !old.owns(PartId::of_key(&encode_key(key).expect("u32 encodes"))));
    let [missing, held, cold, stale] = keys.take(4).collect::<Vec<_>>()[..] else {
        panic!("b owns some parts of the old view");
    };

    let (cache_a, residency) = cache_under_view(&a, name, &fresh);
    let held_bytes = encode_key(&held).expect("u32 encodes");
    ShardOps::apply_remote_batch(
        cache_a.shard.as_ref(),
        vec![WireRecord {
            key: held_bytes,
            value: Some(bytes::Bytes::from(
                postcard::to_stdvec(&"mine".to_string()).expect("string encodes"),
            )),
            ver: crate::hlc::Hlc {
                wall_ms: crate::store::now_ms(),
                logical: 0,
                node: a.node_id(),
            },
            expires_at_ms: None,
        }],
    )
    .await;
    let part_of = |key: u32| PartId::of_key(&encode_key(&key).expect("u32 encodes"));
    residency.mark_cold(&[part_of(cold)]);
    residency.mark_stale(&[part_of(stale)]);

    let answer = |key: u32| {
        let cache_a = &cache_a;
        let old = Arc::clone(&old);
        async move {
            let mut attempts = 0;
            let answer = cache_a
                .owner_answer(
                    &key,
                    &encode_key(&key).expect("u32 encodes"),
                    old,
                    &tracing::Span::none(),
                    &mut attempts,
                )
                .await;
            (answer, attempts)
        }
    };
    let (got, attempts) = answer(missing).await;
    assert!(
        matches!(got, Ok(OwnerAnswer::Local(None))),
        "a warm miss answers"
    );
    assert_eq!(attempts, 1, "b, asked once, answered stale");
    let (got, _) = answer(held).await;
    assert!(
        matches!(got, Ok(OwnerAnswer::Local(Some(ref value))) if value == "mine"),
        "a warm hit answers"
    );
    for key in [cold, stale] {
        let (got, _) = answer(key).await;
        assert!(
            matches!(got, Err(CacheError::FetchUnavailable { .. })),
            "a cold or stale part asks the other owners, of which there are none"
        );
    }

    cache_b.close().await;
    b.shutdown().await;
    a.shutdown().await;
}

/// The owners an explanation lists are the ones `Cache::owners_of`
/// returns, and its probes ask every owner but this node, in the same
/// rank order, whether this node ranks first, second or not at all
/// among the owners; no probe ever names this node. A node that owns the
/// key's part warm and holds the key reads it from its own copy.
#[tokio::test]
async fn explain_orders_owners_like_owners_of_and_never_probes_this_node() {
    let name = "explain-owner-order";
    let a = solo_cluster(name, loopback_config()).await;
    let me = a.node_id();
    // Three nodes no mesh member answers for: every probe is unreached.
    let others: Vec<NodeId> = (1..=3u64).map(|n| NodeId::from(u64::MAX - n)).collect();
    let view = Arc::new(OwnershipView::compute(me, others, Mode::DEFAULT_OWNERS));
    let (cache, _residency) = cache_under_view(&a, name, &view);

    let mut ranks = HashSet::new();
    let mut against_id_order = false;
    for key in 0..200u32 {
        let (_, read) = explain_distributed(&cache, key).await;
        let owners = cache.owners_of(&key);
        assert_eq!(read.owners, owners, "key {key}");
        let rank = owners.iter().position(|owner| *owner == me);
        let expected = match rank {
            Some(0) => vec![owners[1]],
            Some(1) => vec![owners[0]],
            None => owners.clone(),
            Some(rank) => panic!("rank {rank} among two owners"),
        };
        let probed: Vec<NodeId> = read.probes.iter().map(|probe| probe.node).collect();
        assert_eq!(probed, expected, "key {key}");
        assert!(
            read.probes
                .iter()
                .all(|probe| probe.answer == ProbeAnswer::Unreached(Unreached::NotAMember)),
            "key {key}: no mesh member answers"
        );
        ranks.insert(rank);
        against_id_order |= probed.len() == 2 && probed[0] > probed[1];
    }
    assert_eq!(
        ranks,
        HashSet::from([Some(0), Some(1), None]),
        "the keys rank this node first, second and not at all"
    );
    assert!(
        against_id_order,
        "probes follow rendezvous rank, which is not node id order"
    );

    let key = (0..200u32)
        .find(|key| cache.owners_of(key).contains(&me))
        .expect("this node owns some of the keys");
    let record = test_record(key, "held", me);
    let version = record.ver;
    ShardOps::apply_remote_batch(cache.shard.as_ref(), vec![record]).await;
    let (explanation, read) = explain_distributed(&cache, key).await;
    assert_eq!(explanation.cache.as_str(), name);
    assert_eq!(explanation.node, me);
    assert_eq!(explanation.mode, Mode::distributed());
    assert_eq!(explanation.part, part_of(key));
    assert_eq!(
        explanation.local,
        LocalRecord::Live {
            version,
            expires_at_ms: None,
            spilled: false
        }
    );
    assert!(read.residency.owns);
    assert_eq!(read.local_read, LocalRead::Hit);
    assert_eq!(explanation.source, ReadSource::Local { hit: true });
    assert_eq!(read.serves_peers, ServeVerdict::Serve);

    a.shutdown().await;
}

/// The view an explanation's probes set out under is the view it reports
/// its owners, marks and verdicts for, and `view_moved_to` names the view
/// the tracker holds once the probes return: published while a probe is
/// pending, it moves the report's last word and nothing before it. An
/// explanation taken after the move reports no move.
#[tokio::test]
async fn explain_reports_a_moved_view() {
    use crate::net::test_support::peer_at;
    let name = "explain-moved-view";
    let config = loopback_config().with(|c| c.fetch_timeout = Duration::from_millis(300));
    let a = solo_cluster(name, config).await;
    let me = a.node_id();
    // The cluster republishes its empty peer list the first time its
    // background tasks are polled, which would remove the peer below.
    tokio::task::yield_now().await;
    let silent = NodeId::from(u64::MAX);
    a.mesh()
        .update_peers(vec![peer_at(silent, silent_peer_addr().await)]);

    // Under `old` this node and the silent peer own every part; under
    // `moved` another node owns about half of them.
    let other = NodeId::from(u64::MAX - 1);
    let old = Arc::new(OwnershipView::compute(
        me,
        vec![silent],
        Mode::DEFAULT_OWNERS,
    ));
    let moved = Arc::new(OwnershipView::compute(me, vec![other], NonZeroU8::MIN));
    assert_ne!(old.view_hash(), moved.view_hash());
    let key = (0..1_000_000u32)
        .find(|key| !moved.owns(part_of(*key)))
        .expect("the other node owns some parts");
    let (cache, _residency, view_tx) = cache_publishing_views(&a, name, &old);

    let mut explain = std::pin::pin!(cache.explain(&key));
    assert!(
        futures::poll!(explain.as_mut()).is_pending(),
        "the silent owner holds the probe open"
    );
    view_tx
        .send(Arc::clone(&moved))
        .expect("the tracker holds a receiver");
    let explanation = explain.await.expect("u32 encodes");
    let read = explanation
        .distributed
        .expect("a distributed cache explains its distribution");
    assert_eq!(read.view_hash, old.view_hash());
    assert_eq!(read.view_moved_to, Some(moved.view_hash()));
    assert_eq!(read.owners, old.owners_of(part_of(key)));
    assert!(read.residency.owns, "owned under the view the probes used");
    assert_eq!(read.local_read, LocalRead::Miss);
    assert_eq!(
        read.probes,
        vec![OwnerProbe {
            node: silent,
            answer: ProbeAnswer::Unreached(Unreached::TimedOut),
        }]
    );
    assert_eq!(explanation.source, ReadSource::Local { hit: false });

    let (explanation, read) = explain_distributed(&cache, key).await;
    assert_eq!(read.view_hash, moved.view_hash());
    assert_eq!(read.view_moved_to, None);
    assert_eq!(read.owners, vec![other]);
    assert!(!read.residency.owns);
    assert_eq!(read.local_read, LocalRead::NotOwner);
    assert_eq!(explanation.source, ReadSource::Unavailable);

    a.shutdown().await;
}

/// A part this node owns since the last settled view and has not pulled
/// reads cold before rebalance marks it: `unsettled` and `owns`, so
/// `implicitly_cold`. `mark_cold` makes it `cold_marked`, which is no
/// longer implicit, and `mark_serving` clears both. A cold part answers
/// a missing key `ColdMiss`, a warm one `Miss`, and the entry it holds
/// is a `Hit` in either state.
#[tokio::test]
async fn explain_reports_implicit_then_marked_then_no_cold() {
    let name = "explain-cold-marks";
    let a = solo_cluster(name, loopback_config()).await;
    let me = a.node_id();
    // The last settled view owns about half the parts; the view the
    // cache decides under owns them all.
    let settled = OwnershipView::compute(me, vec![NodeId::from(u64::MAX)], NonZeroU8::MIN);
    let now = Arc::new(OwnershipView::compute(me, vec![me], NonZeroU8::MIN));
    let (cache, residency) = cache_under_view(&a, name, &now);
    residency.settle(&settled);

    let mut unsettled = (0..1_000_000u32).filter(|key| !settled.owns(part_of(*key)));
    let missing = unsettled.next().expect("the settled view lacks some parts");
    let held = unsettled
        .find(|key| part_of(*key) != part_of(missing))
        .expect("the settled view lacks parts of two kinds");
    ShardOps::apply_remote_batch(cache.shard.as_ref(), vec![test_record(held, "held", me)]).await;

    let implicit = Residency {
        owns: true,
        releasing_for: None,
        cold_marked: false,
        unsettled: true,
        unverified: false,
        stale: false,
    };
    let (explanation, read) = explain_distributed(&cache, missing).await;
    assert_eq!(read.residency, implicit);
    assert!(read.residency.implicitly_cold() && read.residency.cold());
    assert_eq!(read.local_read, LocalRead::ColdMiss);
    assert_eq!(
        explanation.source,
        ReadSource::Unavailable,
        "no other owner can vouch for the miss"
    );
    assert_eq!(read.serves_peers, ServeVerdict::DeclineCold);

    let parts = [part_of(missing), part_of(held)];
    residency.mark_cold(&parts);
    let (_, read) = explain_distributed(&cache, missing).await;
    assert_eq!(
        read.residency,
        Residency {
            cold_marked: true,
            ..implicit
        }
    );
    assert!(read.residency.cold() && !read.residency.implicitly_cold());
    assert_eq!(read.local_read, LocalRead::ColdMiss);
    let (explanation, read) = explain_distributed(&cache, held).await;
    assert!(read.residency.cold());
    assert_eq!(read.local_read, LocalRead::Hit);
    assert_eq!(explanation.source, ReadSource::Local { hit: true });
    assert_eq!(read.serves_peers, ServeVerdict::Serve);

    residency.mark_serving(&parts);
    let (explanation, read) = explain_distributed(&cache, missing).await;
    assert_eq!(read.residency, Residency::new(true));
    assert!(!read.residency.cold() && !read.residency.implicitly_cold());
    assert_eq!(read.local_read, LocalRead::Miss);
    assert_eq!(explanation.source, ReadSource::Local { hit: false });
    assert_eq!(read.serves_peers, ServeVerdict::Miss);
    let (explanation, read) = explain_distributed(&cache, held).await;
    assert_eq!(read.residency, Residency::new(true));
    assert_eq!(read.local_read, LocalRead::Hit);
    assert_eq!(explanation.source, ReadSource::Local { hit: true });

    a.shutdown().await;
}

/// A part this node no longer owns but holds through its disown grace
/// is `releasing_for` some time and not owned: this node's own read of
/// it is `NotOwner`, yet it serves the record it holds to a peer, which
/// it does not once the part is outside the grace.
#[tokio::test]
async fn explain_reports_a_releasing_node() {
    let name = "explain-releasing";
    let a = solo_cluster(name, loopback_config()).await;
    let me = a.node_id();
    let phantom = NodeId::from(u64::MAX);
    let owned = Arc::new(OwnershipView::compute(me, vec![me], NonZeroU8::MIN));
    let shared = Arc::new(OwnershipView::compute(me, vec![phantom], NonZeroU8::MIN));
    let (cache, residency, view_tx) = cache_publishing_views(&a, name, &owned);
    let key = (0..1_000_000u32)
        .find(|key| !shared.owns(part_of(*key)))
        .expect("the phantom owns some parts");
    // The record lands while this node owns its part, then the part moves.
    ShardOps::apply_remote_batch(cache.shard.as_ref(), vec![test_record(key, "held", me)]).await;
    view_tx
        .send(Arc::clone(&shared))
        .expect("the tracker holds a receiver");

    let (explanation, read) = explain_distributed(&cache, key).await;
    assert!(
        explanation.local.is_live(),
        "the record outlives the ownership"
    );
    assert_eq!(read.residency, Residency::new(false));
    assert_eq!(read.local_read, LocalRead::NotOwner);
    assert_eq!(
        read.serves_peers,
        ServeVerdict::Miss,
        "outside the grace the held record is not served"
    );

    residency.mark_releasing(&[part_of(key)]);
    let (explanation, read) = explain_distributed(&cache, key).await;
    assert!(read.residency.releasing_for.is_some());
    assert!(!read.residency.owns && read.residency.resident());
    assert_eq!(
        Residency {
            releasing_for: None,
            ..read.residency
        },
        Residency::new(false)
    );
    assert_eq!(read.local_read, LocalRead::NotOwner);
    assert_eq!(read.serves_peers, ServeVerdict::Serve);
    assert_eq!(read.owners, vec![phantom]);
    assert_eq!(
        read.probes,
        vec![OwnerProbe {
            node: phantom,
            answer: ProbeAnswer::Unreached(Unreached::NotAMember),
        }]
    );
    assert_eq!(explanation.source, ReadSource::Unavailable);

    a.shutdown().await;
}

/// Outside `Mode::Distributed` an explanation has no distribution. It
/// names the cache, this node, the mode, the key's part and the cache's
/// clock reading, and its source is this node's own copy: a write reads
/// `Live`, a key never written `Absent`, and a removed one a `Tombstone`
/// newer than the write, in every mode.
#[tokio::test]
async fn explain_in_local_invalidation_and_replicated_modes_has_no_distribution() {
    let cluster = solo_cluster("cache-it-explain-modes", loopback_config()).await;
    for (mode, name) in [
        (Mode::Local, "explain-mode-local"),
        (Mode::Invalidation, "explain-mode-invalidation"),
        (Mode::Replicated, "explain-mode-replicated"),
    ] {
        let cache = cluster
            .cache::<u32, String>(name)
            .mode(mode)
            .open()
            .await
            .expect("opens alone");
        cache.insert(1, "one".to_string()).await.expect("insert");

        let before = now_ms();
        let live = cache.explain(&1).await.expect("u32 encodes");
        let after = now_ms();
        assert_eq!(live.cache.as_str(), name);
        assert_eq!(live.node, cluster.node_id(), "{mode:?}");
        assert_eq!(live.mode, mode);
        assert_eq!(live.part, part_of(1), "{mode:?}");
        assert!((before..=after).contains(&live.at_ms), "{mode:?}: {live:?}");
        let LocalRecord::Live {
            version: written,
            expires_at_ms: None,
            spilled: false,
            ..
        } = live.local
        else {
            panic!("{mode:?}: an inserted key is live: {live:?}");
        };
        assert_eq!(live.source, ReadSource::Local { hit: true }, "{mode:?}");
        assert_eq!(live.distributed, None, "{mode:?}");

        let absent = cache.explain(&2).await.expect("u32 encodes");
        assert_eq!(absent.local, LocalRecord::Absent, "{mode:?}");
        assert_eq!(absent.source, ReadSource::Local { hit: false }, "{mode:?}");
        assert_eq!(absent.distributed, None, "{mode:?}");

        cache.remove(&1).await.expect("remove");
        let removed = cache.explain(&1).await.expect("u32 encodes");
        let LocalRecord::Tombstone {
            version: deleted, ..
        } = removed.local
        else {
            panic!("{mode:?}: a removed key leaves a tombstone: {removed:?}");
        };
        assert!(deleted > written, "{mode:?}");
        assert_eq!(removed.source, ReadSource::Local { hit: false }, "{mode:?}");
        assert_eq!(removed.distributed, None, "{mode:?}");

        cache.close().await;
    }
    cluster.shutdown().await;
}

/// A key type whose `Serialize` always fails, to reach `explain`'s
/// encode error.
#[derive(Clone, Hash, PartialEq, Eq, serde::Deserialize)]
struct Unencodable;

impl Serialize for Unencodable {
    fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("this key never encodes"))
    }
}

/// A key that fails to encode makes `explain` fail with a codec error,
/// in a `Mode::Distributed` cache before any owner is asked and in a
/// `Mode::Local` one alike.
#[tokio::test]
async fn explain_of_a_key_that_fails_to_encode_is_a_codec_error() {
    let cluster = solo_cluster("cache-it-explain-codec", loopback_config()).await;
    for (mode, name) in [
        (Mode::Local, "explain-codec-local"),
        (Mode::distributed(), "explain-codec-distributed"),
    ] {
        let cache = cluster
            .cache::<Unencodable, String>(name)
            .mode(mode)
            .open()
            .await
            .expect("opens alone");
        let result = cache.explain(&Unencodable).await;
        assert!(
            matches!(result, Err(CacheError::Codec(_))),
            "{mode:?}: {result:?}"
        );
        cache.close().await;
    }
    cluster.shutdown().await;
}

/// `Cache::explain`'s future is `Send`, so a caller can hold it across a
/// thread hop or spawn it: checked on a distributed cache, whose
/// explanation awaits its probes.
#[tokio::test]
async fn explain_future_is_send() {
    fn assert_send<T: Send>(_: &T) {}

    let name = "explain-send";
    let a = solo_cluster(name, loopback_config()).await;
    let phantom = NodeId::from(u64::MAX);
    let view = Arc::new(OwnershipView::compute(
        a.node_id(),
        vec![phantom],
        Mode::DEFAULT_OWNERS,
    ));
    let (cache, _residency) = cache_under_view(&a, name, &view);
    let key = 7u32;

    let future = cache.explain(&key);
    assert_send(&future);
    let direct = future.await.expect("u32 encodes");
    let spawned = tokio::spawn({
        let cache = cache.clone();
        async move { cache.explain(&key).await }
    })
    .await
    .expect("the task finishes")
    .expect("u32 encodes");
    assert_eq!(spawned.part, direct.part);
    assert_eq!(spawned.local, direct.local);

    a.shutdown().await;
}

#[tokio::test]
async fn fetch_returns_fetch_unavailable_when_every_owner_is_down() {
    // `a`'s failure detector never drops `b` or `c` here. Once it drops
    // one while the other still runs, `a` co-owns the key's part with
    // the survivor and pulls the value from it, so a fetch after the
    // second crash reads `a`'s own copy.
    let a_config = ClusterConfig {
        phi_threshold: 1_000.0,
        ..loopback_config()
    };
    let ((a, cache_a), (b, cache_b), (c, _cache_c), unowned_key) =
        three_node_distributed_with_a("cache-it-fetch-unavailable", "prices", a_config).await;
    cache_b
        .insert(unowned_key, "value".to_string())
        .await
        .expect("insert");
    wait_until(
        Duration::from_secs(10),
        "the value reaches a real owner first, proving the key is genuinely resident",
        async || matches!(cache_a.fetch(&unowned_key).await, Ok(Some(value)) if value == "value"),
    )
    .await;

    // Both of the key's real owners crash; `a` never owned it and its
    // failure detector never drops them, so its view still names them
    // and every dial fails outright.
    b.crash().await;
    c.crash().await;

    let result = cache_a.fetch(&unowned_key).await;
    assert!(
        matches!(&result, Err(CacheError::FetchUnavailable { cache }) if cache == "prices"),
        "a fetch with every owner down answers FetchUnavailable, got {result:?}; \
         a holds a local copy: {}",
        cache_a.get_sync(&unowned_key).is_some()
    );

    a.shutdown().await;
}

#[tokio::test]
async fn expire_touch_persist_and_ttl_of_on_one_node() {
    let cluster = Cluster::builder("cache-it-ttl-surface")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node builds");
    let cache = cluster
        .cache::<u32, String>("sessions")
        .mode(Mode::Local)
        .ttl(Duration::from_secs(600))
        .open()
        .await
        .expect("opens");
    cache.insert(1, "a".into()).await.expect("insert");
    let left = |cache: &Cache<u32, String>| match cache.ttl_of(&1) {
        Some(Ttl::Remaining(left)) => left,
        other => panic!("a remaining lifetime, got {other:?}"),
    };
    assert!(left(&cache) > Duration::from_secs(590));

    assert!(
        cache
            .expire(&1, Duration::from_secs(30))
            .await
            .expect("expire")
    );
    assert!(left(&cache) <= Duration::from_secs(30));
    assert!(cache.touch(&1).await.expect("touch"));
    assert!(
        left(&cache) > Duration::from_secs(590),
        "touch restarts the default"
    );
    assert!(cache.persist(&1).await.expect("persist"));
    assert_eq!(cache.ttl_of(&1), Some(Ttl::Never));
    assert_eq!(cache.get(&1).await, Some("a".to_string()));

    assert!(
        !cache
            .expire(&2, Duration::from_secs(1))
            .await
            .expect("expire")
    );
    assert_eq!(cache.ttl_of(&2), None);
    assert!(cache.expire(&1, Duration::ZERO).await.expect("expire"));
    assert_eq!(cache.get(&1).await, None);

    cluster.shutdown().await;
}

#[derive(Debug, thiserror::Error)]
#[error("source unavailable")]
struct SourceDown;

#[tokio::test]
async fn load_and_load_many_read_through_the_registered_loader() {
    let cluster = Cluster::builder("cache-it-load")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node builds");
    let calls = Arc::new(std::sync::Mutex::new(Vec::<Vec<u32>>::new()));
    let cache = cluster
        .cache::<u32, String>("prices")
        .mode(Mode::Local)
        .ttl(Duration::from_secs(600))
        .batch_loader({
            let calls = Arc::clone(&calls);
            move |keys: Vec<u32>| {
                let calls = Arc::clone(&calls);
                async move {
                    let mut sorted = keys.clone();
                    sorted.sort_unstable();
                    calls.lock().expect("calls lock").push(sorted);
                    Ok::<_, SourceDown>(
                        keys.into_iter()
                            .filter(|key| key.is_multiple_of(2))
                            .map(|key| (key, format!("v{key}")))
                            .collect::<HashMap<_, _>>(),
                    )
                }
            }
        })
        .open()
        .await
        .expect("opens");

    assert_eq!(cache.load(&2).await.expect("load"), Some("v2".to_string()));
    assert_eq!(cache.get(&2).await, Some("v2".to_string()));
    assert!(
        matches!(cache.ttl_of(&2), Some(Ttl::Remaining(left)) if left > Duration::from_secs(590)),
        "a loaded value takes the cache's default TTL"
    );
    assert_eq!(cache.load(&3).await.expect("load"), None);
    assert_eq!(
        cache.load_many([2, 3, 4, 6]).await.expect("load_many"),
        HashMap::from([
            (2, "v2".to_string()),
            (4, "v4".to_string()),
            (6, "v6".to_string())
        ])
    );
    assert_eq!(
        calls.lock().expect("calls lock").clone(),
        vec![vec![2], vec![3], vec![3, 4, 6]],
        "a hit never reaches the loader; a key the source lacks is asked again"
    );

    let single = cluster
        .cache::<u32, String>("names")
        .mode(Mode::Local)
        .loader(|key: u32| async move {
            if key == 0 {
                Err(SourceDown)
            } else {
                Ok((key < 10).then(|| format!("n{key}")))
            }
        })
        .open()
        .await
        .expect("opens");
    assert_eq!(single.load(&1).await.expect("load"), Some("n1".to_string()));
    assert_eq!(single.load(&11).await.expect("load"), None);
    assert!(matches!(single.load(&0).await, Err(CacheError::Loader(_))));

    let plain = cluster
        .cache::<u32, String>("plain")
        .mode(Mode::Local)
        .open()
        .await
        .expect("opens");
    assert!(matches!(
        plain.load(&1).await,
        Err(CacheError::NoLoader { cache }) if cache == "plain"
    ));
    assert!(matches!(
        cluster
            .cache::<u32, String>("windowed")
            .batch_window(Duration::from_millis(5), 10)
            .open()
            .await,
        Err(CacheError::InvalidLoadConfig { cache, .. }) if cache == "windowed"
    ));

    cluster.shutdown().await;
}

#[tokio::test]
async fn a_batch_window_groups_misses_from_concurrent_loads() {
    let cluster = Cluster::builder("cache-it-load-window")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node builds");
    let calls = Arc::new(std::sync::Mutex::new(Vec::<Vec<u32>>::new()));
    let cache = cluster
        .cache::<u32, String>("prices")
        .mode(Mode::Local)
        .batch_loader({
            let calls = Arc::clone(&calls);
            move |keys: Vec<u32>| {
                let calls = Arc::clone(&calls);
                async move {
                    let mut sorted = keys.clone();
                    sorted.sort_unstable();
                    calls.lock().expect("calls lock").push(sorted);
                    Ok::<_, SourceDown>(
                        keys.into_iter()
                            .map(|key| (key, format!("v{key}")))
                            .collect::<HashMap<_, _>>(),
                    )
                }
            }
        })
        .batch_window(Duration::from_millis(200), 3)
        .open()
        .await
        .expect("opens");
    let (a, b, c) = tokio::join!(cache.load(&1), cache.load(&2), cache.load(&3));
    for (answer, key) in [(a, 1), (b, 2), (c, 3)] {
        assert_eq!(answer.expect("load"), Some(format!("v{key}")));
    }
    assert_eq!(
        calls.lock().expect("calls lock").clone(),
        vec![vec![1, 2, 3]],
        "three misses fill the batch and load in one call"
    );
    cluster.shutdown().await;
}

#[tokio::test]
async fn a_loaded_value_replicates_to_every_replica() {
    let name = "cache-it-load-replicated";
    let a = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("a builds");
    let b = Cluster::builder(name)
        .seeds([a.local_gossip_addr()])
        .config(loopback_config())
        .build()
        .await
        .expect("b builds");
    wait_for_peer_count(&a, 1).await;
    wait_for_peer_count(&b, 1).await;
    let open = |cluster: &Cluster| {
        cluster
            .cache::<u32, String>("prices")
            .mode(Mode::Replicated)
            .loader(|key: u32| async move { Ok::<_, SourceDown>(Some(format!("v{key}"))) })
            .open()
    };
    let cache_a = open(&a).await.expect("a opens");
    let cache_b = open(&b).await.expect("b opens");

    assert_eq!(
        cache_a.load(&7).await.expect("load"),
        Some("v7".to_string())
    );
    wait_until(
        Duration::from_secs(10),
        "b holds the loaded value",
        async || cache_b.get(&7).await == Some("v7".to_string()),
    )
    .await;

    b.shutdown().await;
    a.shutdown().await;
}

/// Every loader call any node made: the node and the keys, sorted.
type LoadLog = Arc<std::sync::Mutex<Vec<(NodeId, Vec<u32>)>>>;

/// How long a test waits for a three-node `Mode::Distributed` cluster
/// to converge: views, loader adverts, and owners warming their parts.
/// Each node's ownership views cost seconds of CPU in a debug build, so
/// several such tests sharing a slow runner take well past ten seconds
/// to warm; a wait returns as soon as its condition holds.
const CONVERGE: Duration = Duration::from_secs(30);

/// Three nodes with `name` open under `mode`, each with a batch loader
/// that records its calls in `log`, takes 300ms, and holds `v{key}` for
/// every key; the loader on the node at index `fails` returns an error
/// instead. Returns once every node sees the other two advertise a
/// loader and, under `Mode::Distributed`, ranks all three in its
/// ownership view.
async fn three_loading_nodes(
    name: &'static str,
    mode: Mode,
    log: &LoadLog,
    fails: Option<usize>,
) -> Vec<(Cluster, Cache<u32, String>)> {
    let mut nodes: Vec<(Cluster, Cache<u32, String>)> = Vec::new();
    for index in 0..3 {
        // Every owner here stays up: a longer fetch timeout lets one
        // whose runtime is busy computing views still answer, and the
        // failure detector never marks down a node too busy to gossip,
        // which would move the view and cool the parts a test waits on.
        let builder = Cluster::builder(name).config(loopback_config().with(|c| {
            c.fetch_timeout = Duration::from_secs(5);
            c.phi_threshold = 1_000.0;
        }));
        let cluster = match nodes.first() {
            None => builder.seeds(std::iter::empty()),
            Some((seed, _)) => builder.seeds([seed.local_gossip_addr()]),
        }
        .build()
        .await
        .expect("node builds");
        wait_for_peer_count(&cluster, index).await;
        let node = cluster.node_id();
        let log = Arc::clone(log);
        let cache = cluster
            .cache::<u32, String>(name)
            .mode(mode)
            .batch_loader(move |keys: Vec<u32>| {
                let log = Arc::clone(&log);
                async move {
                    let mut sorted = keys.clone();
                    sorted.sort_unstable();
                    log.lock().expect("log lock").push((node, sorted));
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    if fails == Some(index) {
                        return Err(SourceDown);
                    }
                    Ok(keys
                        .into_iter()
                        .map(|key| (key, format!("v{key}")))
                        .collect::<HashMap<_, _>>())
                }
            })
            .open()
            .await
            .expect("opens");
        nodes.push((cluster, cache));
    }
    wait_until(
        CONVERGE,
        "every node sees the other two advertise a loader",
        async || {
            nodes
                .iter()
                .all(|(_, cache)| cache.loader_peers().len() == 2)
        },
    )
    .await;
    wait_for_full_views(&nodes).await;
    nodes
}

/// Waits until every `Mode::Distributed` node's ownership view ranks
/// all of `nodes`. Loader adverts and the cache-mode adverts a view is
/// built from are separate gossip keys, and each node rebuilds its view
/// in its own task, so seeing every loader says nothing about the view.
async fn wait_for_full_views(nodes: &[(Cluster, Cache<u32, String>)]) {
    wait_until(
        CONVERGE,
        "every node's ownership view ranks every node",
        async || {
            nodes.iter().all(|(_, cache)| {
                cache
                    .shard
                    .ownership_view()
                    .is_none_or(|view| view.eligible().len() == nodes.len())
            })
        },
    )
    .await;
}

async fn shut_down_all(nodes: Vec<(Cluster, Cache<u32, String>)>) {
    for (cluster, _) in nodes.into_iter().rev() {
        cluster.shutdown().await;
    }
}

#[tokio::test]
async fn concurrent_invalidation_loads_of_a_key_gather_into_one_loader_call() {
    let log = LoadLog::default();
    let nodes = three_loading_nodes("cache-it-load-gather", Mode::Invalidation, &log, None).await;
    let (answers, ()) = tokio::join!(
        futures::future::join_all(nodes.iter().map(|(_, cache)| cache.load(&7))),
        async {}
    );
    for answer in answers {
        assert_eq!(answer.expect("load"), Some("v7".to_string()));
    }
    assert_eq!(
        log.lock().expect("log lock").len(),
        1,
        "three nodes asking at once share one loader call: {:?}",
        log.lock().expect("log lock")
    );
    for (_, cache) in &nodes {
        assert_eq!(
            cache.get(&7).await,
            Some("v7".to_string()),
            "in Invalidation mode each node keeps what it loaded"
        );
    }
    let loads_before = log.lock().expect("log lock").len();
    let found = nodes[1].1.load_many([7, 8, 9]).await.expect("load_many");
    assert_eq!(found.len(), 3);
    assert_eq!(
        nodes[1].1.load(&7).await.expect("load"),
        Some("v7".to_string())
    );
    assert!(
        log.lock().expect("log lock").len() > loads_before,
        "the missing keys reach a loader"
    );
    shut_down_all(nodes).await;
}

/// Waits until every owner of `key` holds its part warm and verified, so
/// a fetch of it gets an answer rather than a decline.
async fn wait_for_warm_owners(nodes: &[(Cluster, Cache<u32, String>)], key: u32) {
    let part = PartId::of_key(&encode_key(&key).expect("u32 encodes"));
    let owners = nodes[0].1.owners_of(&key);
    wait_until(
        CONVERGE,
        "every owner holds the key's part warm",
        async || {
            nodes
                .iter()
                .filter(|(cluster, _)| owners.contains(&cluster.node_id()))
                .all(|(_, cache)| {
                    !cache.shard.is_cold_part(part) && !cache.shard.is_unverified_part(part)
                })
        },
    )
    .await;
}

/// One closed span's name and recorded fields.
type ClosedSpan = (&'static str, HashMap<&'static str, String>);

/// Every closed span, in close order.
#[derive(Clone, Default)]
struct FetchSpans(Arc<std::sync::Mutex<Vec<ClosedSpan>>>);

/// One span's recorded fields.
#[derive(Default)]
struct SpanFields(HashMap<&'static str, String>);

impl tracing::field::Visit for SpanFields {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name(), format!("{value:?}"));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name(), value.to_owned());
    }
}

impl<S> tracing_subscriber::Layer<S> for FetchSpans
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if let Some(span) = ctx.span(id) {
            let mut fields = SpanFields::default();
            attrs.record(&mut fields);
            span.extensions_mut().insert(fields);
        }
    }

    fn on_record(
        &self,
        id: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if let Some(span) = ctx.span(id)
            && let Some(fields) = span.extensions_mut().get_mut::<SpanFields>()
        {
            values.record(fields);
        }
    }

    fn on_close(&self, id: tracing::span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        if let Some(span) = ctx.span(&id)
            && let Some(fields) = span.extensions_mut().remove::<SpanFields>()
        {
            self.0
                .lock()
                .expect("spans lock")
                .push((span.name(), fields.0));
        }
    }
}

impl FetchSpans {
    /// The fields of the last closed span named `name`, as sorted
    /// `(field, value)` pairs; `None` when no such span closed.
    fn last_named(&self, name: &str) -> Option<Vec<(&'static str, String)>> {
        let spans = self.0.lock().expect("spans lock");
        let (_, fields) = spans.iter().rev().find(|(span, _)| *span == name)?;
        let mut fields: Vec<_> = fields
            .iter()
            .map(|(field, value)| (*field, value.clone()))
            .collect();
        fields.sort();
        Some(fields)
    }

    /// The last closed `sundog.fetch` span's fields.
    fn last(&self) -> Vec<(&'static str, String)> {
        self.last_named("sundog.fetch")
            .expect("a fetch span closed")
    }
}

#[tokio::test]
async fn a_fetch_span_names_the_owner_asked_its_outcome_and_its_attempts() {
    use tracing_subscriber::layer::SubscriberExt as _;
    let spans = FetchSpans::default();
    let _guard = crate::cluster::test_support::scoped_subscriber(
        tracing_subscriber::registry().with(spans.clone()),
    );
    let log = LoadLog::default();
    let nodes = three_loading_nodes("cache-it-fetch-span", Mode::distributed(), &log, None).await;
    let (a, cache_a) = &nodes[0];
    let not_owned: Vec<u32> = (0..10_000u32)
        .filter(|key| !cache_a.owners_of(key).contains(&a.node_id()))
        .take(2)
        .collect();
    let (present, absent) = (not_owned[0], not_owned[1]);
    wait_for_warm_owners(&nodes, present).await;
    wait_for_warm_owners(&nodes, absent).await;
    let local_key = (0..10_000u32)
        .find(|key| cache_a.owners_of(key).contains(&a.node_id()))
        .expect("a key a owns");
    let fields = |owner: NodeId, outcome: &str, attempts: u32| {
        let mut fields = vec![
            ("attempts", attempts.to_string()),
            ("cache", "cache-it-fetch-span".to_owned()),
            ("outcome", outcome.to_owned()),
            ("owner", owner.to_string()),
        ];
        fields.sort();
        fields
    };

    let first_owner = cache_a.owners_of(&present)[0];
    cache_a
        .insert(present, "here".to_owned())
        .await
        .expect("insert");
    wait_until(CONVERGE, "the first owner holds the key", async || {
        nodes
            .iter()
            .find(|(cluster, _)| cluster.node_id() == first_owner)
            .expect("the owner is a node")
            .1
            .get(&present)
            .await
            .is_some()
    })
    .await;
    assert_eq!(
        cache_a.fetch(&present).await.expect("fetch"),
        Some("here".to_owned())
    );
    assert_eq!(spans.last(), fields(first_owner, "remote", 1));

    let absent_owner = cache_a.owners_of(&absent)[0];
    assert_eq!(cache_a.fetch(&absent).await.expect("fetch"), None);
    assert_eq!(spans.last(), fields(absent_owner, "miss", 1));

    let part = PartId::of_key(&encode_key(&local_key).expect("u32 encodes"));
    wait_until(CONVERGE, "a holds the local key's part warm", async || {
        !cache_a.shard.is_cold_part(part)
    })
    .await;
    assert_eq!(cache_a.fetch(&local_key).await.expect("fetch"), None);
    assert_eq!(spans.last(), fields(a.node_id(), "local", 0));
    shut_down_all(nodes).await;
}

/// A subscriber that leaves the debug-level `sundog.fetch` span disabled
/// sees nothing of a fetch or an `expire` on the span the caller runs it
/// in, even when that span declares fields of the same names: neither
/// on a cache with no ownership view, nor through a `Mode::Distributed`
/// cache's owner requests.
#[tokio::test]
async fn a_disabled_fetch_span_records_nothing_on_the_callers_span() {
    use tracing::Instrument as _;
    use tracing_subscriber::Layer as _;
    use tracing_subscriber::layer::SubscriberExt as _;
    let spans = FetchSpans::default();
    let _guard = crate::cluster::test_support::scoped_subscriber(
        tracing_subscriber::registry().with(
            spans
                .clone()
                .with_filter(tracing::level_filters::LevelFilter::INFO),
        ),
    );
    let cluster = Cluster::builder("cache-it-fetch-span-disabled")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node builds");
    let cache = cluster
        .cache::<u32, String>("cache-it-fetch-span-disabled")
        .open()
        .await
        .expect("opens");
    let caller = tracing::info_span!(
        "caller",
        owner = tracing::field::Empty,
        outcome = tracing::field::Empty,
        attempts = tracing::field::Empty,
    );
    assert_eq!(
        cache
            .fetch(&1)
            .instrument(caller.clone())
            .await
            .expect("fetch"),
        None
    );
    drop(caller);
    assert_eq!(spans.last_named("caller"), Some(Vec::new()));
    assert_eq!(spans.last_named("sundog.fetch"), None);
    cluster.shutdown().await;

    let log = LoadLog::default();
    let nodes = three_loading_nodes(
        "cache-it-fetch-span-disabled-dist",
        Mode::distributed(),
        &log,
        None,
    )
    .await;
    let (a, cache_a) = &nodes[0];
    let remote = (0..10_000u32)
        .find(|key| !cache_a.owners_of(key).contains(&a.node_id()))
        .expect("a key a does not own");
    let local = (0..10_000u32)
        .find(|key| cache_a.owners_of(key).contains(&a.node_id()))
        .expect("a key a owns");
    // A fetch answers `FetchUnavailable` while every owner is cold.
    wait_for_warm_owners(&nodes, remote).await;
    wait_for_warm_owners(&nodes, local).await;
    cache_a
        .insert(remote, "there".to_owned())
        .await
        .expect("insert");
    let caller = tracing::info_span!(
        "caller",
        owner = tracing::field::Empty,
        outcome = tracing::field::Empty,
        attempts = tracing::field::Empty,
    );
    async {
        cache_a.fetch(&remote).await.expect("remote fetch");
        cache_a.fetch(&local).await.expect("local fetch");
        cache_a
            .expire(&remote, Duration::from_secs(60))
            .await
            .expect("expire");
    }
    .instrument(caller.clone())
    .await;
    drop(caller);
    assert_eq!(spans.last_named("caller"), Some(Vec::new()));
    assert_eq!(spans.last_named("sundog.fetch"), None);
    shut_down_all(nodes).await;
}

/// Per `(name, cache, outcome)` metric, a counter's value or a
/// histogram's sample count, drained from `snapshotter`.
fn drained(
    snapshotter: &metrics_util::debugging::Snapshotter,
) -> std::collections::BTreeMap<(String, String, String), u64> {
    use metrics_util::debugging::DebugValue;
    let mut values = std::collections::BTreeMap::new();
    for (key, _, _, value) in snapshotter.snapshot().into_vec() {
        let label = |name: &str| {
            key.key()
                .labels()
                .find(|label| label.key() == name)
                .map(|label| label.value().to_owned())
                .unwrap_or_default()
        };
        let n = match value {
            DebugValue::Counter(n) => n,
            DebugValue::Histogram(samples) => samples.len() as u64,
            DebugValue::Gauge(_) => continue,
        };
        values.insert(
            (
                key.key().name().to_owned(),
                label("cache"),
                label("outcome"),
            ),
            n,
        );
    }
    values
}

/// [`drained`]'s map: per `(name, cache, outcome)`, a counter's value or a
/// histogram's sample count.
type Series = std::collections::BTreeMap<(String, String, String), u64>;

/// The series a read moves: hit and miss counts, sampled read timings,
/// fetch counts and timings, and loader calls.
const READ_SERIES: [&str; 6] = [
    "sundog_cache_hits_total",
    "sundog_cache_misses_total",
    "sundog_read_duration_seconds",
    "sundog_fetch_total",
    "sundog_fetch_duration_seconds",
    "sundog_loader_calls_total",
];

/// [`drained`] restricted to [`READ_SERIES`]. A call drains every series,
/// so each reports what ran since the one before.
fn read_series(snapshotter: &metrics_util::debugging::Snapshotter) -> Series {
    drained(snapshotter)
        .into_iter()
        .filter(|((name, ..), _)| READ_SERIES.contains(&name.as_str()))
        .collect()
}

/// The value of the series `name` of `cache` under `outcome` in `series`,
/// 0 when it has none.
fn series_count(series: &Series, name: &str, cache: &str, outcome: &str) -> u64 {
    series
        .get(&(name.to_owned(), cache.to_owned(), outcome.to_owned()))
        .copied()
        .unwrap_or(0)
}

/// Each call under its own cache label, so the test sees which calls
/// are timed, not only how many per outcome.
#[test]
fn only_a_fetch_that_asked_an_owner_is_timed() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let calls = [
        ("local", 0),
        ("local", 2),
        ("remote", 1),
        ("miss", 1),
        ("error", 0),
        ("error", 3),
    ];
    metrics::with_local_recorder(&recorder, || {
        let started = std::time::Instant::now();
        for (index, (outcome, attempts)) in calls.into_iter().enumerate() {
            let cache = format!("call{index}");
            record_fetch_outcome(&tracing::Span::none(), &cache, outcome, attempts, started);
        }
    });
    let values = drained(&snapshotter);
    for (index, (outcome, attempts)) in calls.into_iter().enumerate() {
        let key = |name: &str| (name.to_owned(), format!("call{index}"), outcome.to_owned());
        assert_eq!(
            values.get(&key("sundog_fetch_total")),
            Some(&1),
            "{outcome} with {attempts} owner requests is counted"
        );
        assert_eq!(
            values.get(&key("sundog_fetch_duration_seconds")),
            (attempts > 0).then_some(&1),
            "{outcome} with {attempts} owner requests is timed iff it asked an owner"
        );
    }
}

/// `owner_answer`'s local branch over every residency state a read
/// tells apart, in front of owners no node can reach: it answers from
/// this node's copy iff the part is owned and trusted and the read hits
/// or the part is warm, and otherwise asks the owners and finds none.
#[tokio::test]
async fn the_owner_loop_reads_locally_by_the_read_rule_over_every_residency_state() {
    use crate::store::read_cases::{case_shard, every_read_case};
    let solo = Cluster::builder("cache-it-read-cases")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node builds");
    for case in every_read_case() {
        let built = case_shard("read-cases", &case).await;
        let cache = Cache {
            shard: Arc::new(built.shard),
            cluster: solo.clone(),
            cancel: CancellationToken::new(),
            tasks: TaskTracker::new(),
        };
        let mut attempts = 0;
        let answer = cache
            .owner_answer(
                &built.key,
                &built.key_bytes,
                Arc::clone(&built.view),
                &tracing::Span::none(),
                &mut attempts,
            )
            .await;
        let answers = case.owns && !case.distrusted() && (case.live() || !case.cold());
        match answer {
            Ok(OwnerAnswer::Local(value)) => {
                assert!(answers, "answered locally: {case:?}");
                assert_eq!(value.is_some(), case.live(), "{case:?}");
            }
            Err(CacheError::FetchUnavailable { .. }) => {
                assert!(!answers, "asked the owners: {case:?}");
            }
            Ok(OwnerAnswer::Remote(_)) | Err(_) => {
                panic!("no owner can answer: {case:?}");
            }
        }
    }
    solo.shutdown().await;
}

/// Asserts `local` is what a shard holding `held` stores for the case
/// key.
fn assert_stores(held: crate::store::read_cases::Held, local: &LocalRecord) {
    use crate::store::read_cases::Held;
    match held {
        Held::Nothing => assert_eq!(*local, LocalRecord::Absent),
        Held::Live => assert!(
            matches!(
                local,
                LocalRecord::Live {
                    expires_at_ms: None,
                    spilled: false,
                    ..
                }
            ),
            "{local:?}"
        ),
        Held::Tombstone => assert!(matches!(local, LocalRecord::Tombstone { .. }), "{local:?}"),
    }
}

/// Asserts `marks` are the residency marks `case` set, a running disown
/// grace counting as set whatever time it has run.
fn assert_marks_of_case(case: crate::store::read_cases::ReadCase, marks: Residency) {
    assert_eq!(
        Residency {
            releasing_for: None,
            ..marks
        },
        Residency {
            owns: case.owns,
            releasing_for: None,
            cold_marked: case.cold_marked,
            unsettled: case.unsettled,
            unverified: case.unverified,
            stale: case.stale,
        },
        "{case:?}"
    );
    assert_eq!(marks.releasing_for.is_some(), case.releasing, "{case:?}");
}

/// What a node in `case`'s state answers a peer's fetch sent with an
/// equal view hash: distrust declines even a held record, a held record
/// in a resident part is served, a cold part declines, and only a warm
/// part's miss is definitive.
fn serve_rule(case: crate::store::read_cases::ReadCase) -> ServeVerdict {
    if case.distrusted() {
        ServeVerdict::DeclineDistrusted
    } else if case.served() {
        ServeVerdict::Serve
    } else if case.cold() {
        ServeVerdict::DeclineCold
    } else {
        ServeVerdict::Miss
    }
}

/// `Cache::explain` against `Cache::fetch` over every residency state a
/// read tells apart, in front of an owner no node can reach. The marks
/// it reports are the ones the case set and the ones the shard reads
/// itself; its local read is the verdict those marks give and answers
/// iff the part is owned and trusted and the read hits or the part is
/// warm; it predicts what this node answers a peer; the one owner is
/// unreached; and the source it names is the one the fetch takes its
/// answer from.
#[tokio::test]
async fn explain_agrees_with_fetch_over_every_residency_state() {
    use crate::store::read_cases::{case_shard, every_read_case};
    let solo = solo_cluster("cache-it-explain-read-cases", loopback_config()).await;
    let (n1, n2) = (NodeId::from(1u64), NodeId::from(2u64));
    for case in every_read_case() {
        let built = case_shard("explain-cases", &case).await;
        let cache = Cache {
            shard: Arc::new(built.shard),
            cluster: solo.clone(),
            cancel: CancellationToken::new(),
            tasks: TaskTracker::new(),
        };
        let explanation = cache.explain(&built.key).await.expect("u32 encodes");
        let read = explanation
            .distributed
            .clone()
            .expect("a distributed cache explains its distribution");

        assert_eq!(explanation.cache.as_str(), "explain-cases");
        assert_eq!(explanation.node, solo.node_id());
        assert_eq!(
            explanation.mode,
            Mode::Distributed {
                owners: NonZeroU8::MIN
            }
        );
        assert_eq!(explanation.part, built.part);
        assert_stores(case.held, &explanation.local);

        assert_eq!(read.view_hash, built.view.view_hash(), "{case:?}");
        assert_eq!(read.view_moved_to, None, "{case:?}");
        let owner = if case.owns { n1 } else { n2 };
        assert_eq!(read.owners, vec![owner], "{case:?}");
        assert_eq!(read.owners, cache.owners_of(&built.key), "{case:?}");

        let direct = cache.shard.residency_of(&built.view, built.part);
        assert_marks_of_case(case, read.residency);
        assert_marks_of_case(case, direct);
        assert_eq!(
            Residency {
                releasing_for: None,
                ..read.residency
            },
            Residency {
                releasing_for: None,
                ..direct
            },
            "{case:?}"
        );
        assert_eq!(
            read.local_read,
            verdict::local_verdict(&direct, case.live()),
            "{case:?}"
        );
        let answers = case.owns && !case.distrusted() && (case.live() || !case.cold());
        assert_eq!(read.local_read.answers(), answers, "{case:?}");
        assert_eq!(read.serves_peers, serve_rule(case), "{case:?}");
        assert_eq!(
            read.probes,
            vec![OwnerProbe {
                node: owner,
                answer: ProbeAnswer::Unreached(Unreached::NotAMember),
            }],
            "{case:?}"
        );
        let source = if answers {
            ReadSource::Local { hit: case.live() }
        } else {
            ReadSource::Unavailable
        };
        assert_eq!(explanation.source, source, "{case:?}");

        let fetched = cache.fetch(&built.key).await;
        match (explanation.source, &fetched) {
            (ReadSource::Local { hit: true }, Ok(Some(value))) => {
                assert_eq!(value, "held", "{case:?}");
            }
            (ReadSource::Local { hit: false }, Ok(None))
            | (ReadSource::Unavailable, Err(CacheError::FetchUnavailable { .. })) => {}
            (source, fetched) => {
                panic!("{case:?}: explain says {source:?}, fetch returns {fetched:?}");
            }
        }
    }
    solo.shutdown().await;
}

/// Through `Cache::fetch`: a fetch answered without asking an owner is
/// counted and not timed, on a cache with no ownership view and on a
/// `Mode::Distributed` key this node owns warm; a fetch that asks an
/// owner is counted and timed.
#[tokio::test]
async fn a_fetch_is_timed_only_when_it_asks_an_owner() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let fetched = |cache: &str, outcome: &str| {
        let values = drained(&snapshotter);
        let get = |name: &str| {
            values
                .get(&(name.to_owned(), cache.to_owned(), outcome.to_owned()))
                .copied()
        };
        (
            get("sundog_fetch_total"),
            get("sundog_fetch_duration_seconds"),
        )
    };

    let cluster = Cluster::builder("cache-it-fetch-timed")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node builds");
    let plain = cluster
        .cache::<u32, String>("fetch-timed-plain")
        .open()
        .await
        .expect("opens");
    assert_eq!(plain.fetch(&1).await.expect("fetch"), None);
    assert_eq!(fetched("fetch-timed-plain", "local"), (Some(1), None));
    cluster.shutdown().await;

    let log = LoadLog::default();
    let nodes = three_loading_nodes("cache-it-fetch-timed", Mode::distributed(), &log, None).await;
    let (a, cache_a) = &nodes[0];
    let local = (0..10_000u32)
        .find(|key| cache_a.owners_of(key).contains(&a.node_id()))
        .expect("a key a owns");
    let part = PartId::of_key(&encode_key(&local).expect("u32 encodes"));
    wait_until(CONVERGE, "a holds the owned key's part warm", async || {
        !cache_a.shard.is_cold_part(part)
    })
    .await;
    let remote = (0..10_000u32)
        .find(|key| !cache_a.owners_of(key).contains(&a.node_id()))
        .expect("a key a does not own");
    wait_for_warm_owners(&nodes, remote).await;
    let _ = drained(&snapshotter);
    assert_eq!(cache_a.fetch(&local).await.expect("fetch"), None);
    assert_eq!(fetched("cache-it-fetch-timed", "local"), (Some(1), None));
    assert_eq!(cache_a.fetch(&remote).await.expect("fetch"), None);
    assert_eq!(fetched("cache-it-fetch-timed", "miss"), (Some(1), Some(1)));
    shut_down_all(nodes).await;
}

/// `explain` of a `Mode::Local` cache counts no read: over a live key, a
/// deleted key and a key never written it moves no hit, miss, read
/// timing, fetch or loader series and creates none, where a `get` and a
/// `fetch` of the same cache move them.
#[tokio::test]
async fn explain_does_not_move_a_read_metric_on_a_local_cache() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let name = "explain-quiet-local";
    let cluster = Cluster::builder("cache-it-explain-quiet-local")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node builds");
    let cache = cluster
        .cache::<u32, String>(name)
        .mode(Mode::Local)
        .loader(|key: u32| async move { Ok::<_, SourceDown>(Some(format!("v{key}"))) })
        .open()
        .await
        .expect("opens");
    cache.insert(1, "live".to_string()).await.expect("insert");
    cache.insert(2, "gone".to_string()).await.expect("insert");
    cache.remove(&2).await.expect("remove");

    assert_eq!(cache.get(&1).await, Some("live".to_string()));
    assert_eq!(cache.get(&3).await, None);
    assert_eq!(
        cache.fetch(&1).await.expect("fetch"),
        Some("live".to_string())
    );
    let read = read_series(&snapshotter);
    assert_eq!(series_count(&read, "sundog_cache_hits_total", name, ""), 2);
    assert_eq!(
        series_count(&read, "sundog_cache_misses_total", name, ""),
        1
    );
    assert_eq!(series_count(&read, "sundog_fetch_total", name, "local"), 1);

    let before = read_series(&snapshotter);
    for (key, returns_value) in [(1, true), (2, false), (3, false)] {
        let explanation = cache.explain(&key).await.expect("explains");
        assert_eq!(
            explanation.source.returns_value(),
            returns_value,
            "{explanation}"
        );
        assert!(explanation.distributed.is_none());
    }
    assert_eq!(
        read_series(&snapshotter),
        before,
        "explain moves no read series and creates none"
    );

    assert_eq!(cache.get(&1).await, Some("live".to_string()));
    assert_eq!(
        series_count(
            &read_series(&snapshotter),
            "sundog_cache_hits_total",
            name,
            ""
        ),
        1,
        "a read after the explanations counts as it always does"
    );
    cluster.shutdown().await;
}

/// `explain` of a `Mode::Distributed` cache asks the owners and still
/// counts no read: over a key this node owns and a key it does not, each
/// stored and never written, asked of every node, it moves no hit, miss,
/// read timing, fetch, fetch timing or loader series, where the `fetch`
/// of the same four keys moves them.
#[tokio::test]
async fn explain_does_not_move_a_read_metric_on_a_distributed_cache() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let name = "cache-it-explain-quiet";
    let log = LoadLog::default();
    let nodes = three_loading_nodes(name, Mode::distributed(), &log, None).await;
    let (a, cache_a) = &nodes[0];
    let owned: Vec<u32> = (0..10_000u32)
        .filter(|key| cache_a.owners_of(key).contains(&a.node_id()))
        .take(2)
        .collect();
    let remote: Vec<u32> = (0..10_000u32)
        .filter(|key| !cache_a.owners_of(key).contains(&a.node_id()))
        .take(2)
        .collect();
    let [owned_stored, owned_absent] = owned[..] else {
        panic!("a owns some parts of a three-node view");
    };
    let [remote_stored, remote_absent] = remote[..] else {
        panic!("a does not own some parts of a three-node view");
    };
    let keys = [owned_stored, owned_absent, remote_stored, remote_absent];
    let is_stored = |key: u32| key == owned_stored || key == remote_stored;
    for key in keys {
        wait_for_warm_owners(&nodes, key).await;
    }
    for key in [owned_stored, remote_stored] {
        cache_a
            .insert(key, format!("v{key}"))
            .await
            .expect("insert");
        let holders: Vec<&Cache<u32, String>> = cache_a
            .owners_of(&key)
            .into_iter()
            .map(|owner| cache_of(&nodes, owner))
            .collect();
        wait_for_local_record(&holders, key, LocalRecord::is_live).await;
    }

    for key in keys {
        assert_eq!(
            cache_a.fetch(&key).await.expect("fetch"),
            is_stored(key).then(|| format!("v{key}"))
        );
    }
    let fetched = read_series(&snapshotter);
    let count = |series: &str, outcome: &str| series_count(&fetched, series, name, outcome);
    assert_eq!(count("sundog_fetch_total", "remote"), 1);
    assert_eq!(count("sundog_fetch_total", "miss"), 1);
    assert_eq!(count("sundog_fetch_total", "local"), 2);
    assert_eq!(count("sundog_fetch_duration_seconds", "remote"), 1);
    assert_eq!(count("sundog_fetch_duration_seconds", "miss"), 1);
    assert_eq!(count("sundog_fetch_duration_seconds", "local"), 0);
    assert_eq!(count("sundog_cache_hits_total", ""), 1);
    assert_eq!(count("sundog_cache_misses_total", ""), 1);

    let before = read_series(&snapshotter);
    for (_, cache) in &nodes {
        for key in keys {
            let explanation = cache.explain(&key).await.expect("explains");
            assert_eq!(
                explanation.source.returns_value(),
                is_stored(key),
                "{explanation}"
            );
        }
    }
    let asked = cache_a.explain(&remote_stored).await.expect("explains");
    let read = asked
        .distributed
        .as_ref()
        .expect("a Distributed cache explains its fetch");
    assert_eq!(read.probes.len(), 2, "explain asks both owners: {asked}");
    assert_eq!(
        read_series(&snapshotter),
        before,
        "explain moves no read series and creates none"
    );
    assert!(
        log.lock().expect("log lock").is_empty(),
        "explain calls no loader: {:?}",
        log.lock().expect("log lock")
    );
    shut_down_all(nodes).await;
}

/// `explain` of a key the source holds and no cache stores calls no
/// loader and stores nothing: every node still misses the key afterwards,
/// where `Cache::load` of it calls the first owner's loader once and the
/// owners keep the value.
#[tokio::test]
async fn explain_does_not_load_or_fill_a_missing_key() {
    let log = LoadLog::default();
    let nodes =
        three_loading_nodes("cache-it-explain-no-load", Mode::distributed(), &log, None).await;
    let (a, cache_a) = &nodes[0];
    let key = (0..10_000u32)
        .find(|key| !cache_a.owners_of(key).contains(&a.node_id()))
        .expect("a key a does not own");
    let owners = cache_a.owners_of(&key);
    wait_until(
        CONVERGE,
        "every node agrees on the key's owners",
        async || {
            nodes
                .iter()
                .all(|(_, cache)| cache.owners_of(&key) == owners)
        },
    )
    .await;
    wait_for_warm_owners(&nodes, key).await;

    for (_, cache) in &nodes {
        let explanation = cache.explain(&key).await.expect("explains");
        assert_eq!(explanation.local, LocalRecord::Absent);
        assert!(
            !explanation.source.returns_value(),
            "no cache stores the key: {explanation}"
        );
    }
    assert!(
        log.lock().expect("log lock").is_empty(),
        "explain calls no loader: {:?}",
        log.lock().expect("log lock")
    );
    for (_, cache) in &nodes {
        assert_eq!(cache.get(&key).await, None, "explain stores nothing");
        assert_eq!(cache.keys(), Vec::<u32>::new(), "explain fills no key");
    }
    assert_eq!(cache_a.fetch(&key).await.expect("fetch"), None);
    assert!(
        log.lock().expect("log lock").is_empty(),
        "a fetch loads nothing either"
    );

    assert_eq!(
        cache_a.load(&key).await.expect("load"),
        Some(format!("v{key}"))
    );
    assert_eq!(
        log.lock().expect("log lock").clone(),
        vec![(owners[0], vec![key])],
        "the key's first owner runs the one load"
    );
    wait_until(CONVERGE, "both owners hold the loaded value", async || {
        let mut held = 0;
        for (cluster, cache) in &nodes {
            if owners.contains(&cluster.node_id()) && cache.get(&key).await.is_some() {
                held += 1;
            }
        }
        held == 2
    })
    .await;
    shut_down_all(nodes).await;
}

/// `explain` opens no `sundog.fetch` span: not for a key this node owns,
/// a key it asks the owners for, or a cache with no ownership view,
/// where the `fetch` of the same three keys opens one each.
#[tokio::test]
async fn explain_does_not_open_a_fetch_span() {
    use tracing_subscriber::layer::SubscriberExt as _;
    let spans = FetchSpans::default();
    let _guard = crate::cluster::test_support::scoped_subscriber(
        tracing_subscriber::registry().with(spans.clone()),
    );
    let log = LoadLog::default();
    let nodes =
        three_loading_nodes("cache-it-explain-no-span", Mode::distributed(), &log, None).await;
    let (a, cache_a) = &nodes[0];
    let plain = a
        .cache::<u32, String>("cache-it-explain-no-span-plain")
        .mode(Mode::Local)
        .open()
        .await
        .expect("opens");
    let local = (0..10_000u32)
        .find(|key| cache_a.owners_of(key).contains(&a.node_id()))
        .expect("a key a owns");
    let remote = (0..10_000u32)
        .find(|key| !cache_a.owners_of(key).contains(&a.node_id()))
        .expect("a key a does not own");
    wait_for_warm_owners(&nodes, local).await;
    wait_for_warm_owners(&nodes, remote).await;
    let fetch_spans = || {
        spans
            .0
            .lock()
            .expect("spans lock")
            .iter()
            .filter(|(span, _)| *span == "sundog.fetch")
            .count()
    };

    let before = fetch_spans();
    for key in [local, remote] {
        cache_a.explain(&key).await.expect("explains");
    }
    plain.explain(&1).await.expect("explains");
    assert_eq!(fetch_spans(), before, "explain opens no fetch span");

    for key in [local, remote] {
        cache_a.fetch(&key).await.expect("fetch");
    }
    plain.fetch(&1).await.expect("fetch");
    assert_eq!(
        fetch_spans(),
        before + 3,
        "a fetch opens one span, on this node and on its owners alike"
    );
    shut_down_all(nodes).await;
}

/// A loopback address that accepts every connection and answers none: an
/// owner that is up and silent.
async fn silent_peer_addr() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind an ephemeral loopback tcp port for a silent peer");
    let addr = listener
        .local_addr()
        .expect("a freshly bound tcp listener reports a local address");
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            held.push(stream);
        }
    });
    addr
}

/// Three `Mode::distributed()` nodes of `name` and two keys the first node
/// does not own, whose two owners hold their parts warm under one view on
/// every node. Anti-entropy is effectively off, so nothing repairs a copy
/// a test drops by hand, and the failure detector never drops a crashed
/// node from a view.
async fn explain_nodes(name: &'static str) -> (Vec<(Cluster, Cache<u32, String>)>, [u32; 2]) {
    let mut config = loopback_config().with(|c| {
        c.ae_interval = Duration::from_secs(3600);
        c.phi_threshold = 1_000.0;
    });
    config.tombstone_ttl = config.bucket_release_window();
    let (a, b, c) = three_node_distributed_with(name, config).await;
    let nodes = vec![a, b, c];
    wait_for_full_views(&nodes).await;
    let asker = nodes[0].0.node_id();
    let keys: Vec<u32> = (0..100_000u32)
        .filter(|key| !nodes[0].1.owners_of(key).contains(&asker))
        .take(2)
        .collect();
    let [first, second] = keys[..] else {
        panic!("the first node owns only some parts of a three-node view");
    };
    for key in [first, second] {
        wait_for_warm_owners(&nodes, key).await;
    }
    (nodes, [first, second])
}

/// The cache `node` runs in `nodes`.
fn cache_of(nodes: &[(Cluster, Cache<u32, String>)], node: NodeId) -> &Cache<u32, String> {
    &nodes
        .iter()
        .find(|(cluster, _)| cluster.node_id() == node)
        .expect("the node is one of the cluster's")
        .1
}

/// Waits until every cache of `caches` stores `key` as `want` says,
/// read from its engine: a tombstone is invisible to `Cache::get`.
async fn wait_for_local_record(
    caches: &[&Cache<u32, String>],
    key: u32,
    want: fn(&LocalRecord) -> bool,
) {
    let key_bytes = encode_key(&key).expect("u32 encodes");
    wait_until(
        Duration::from_secs(10),
        "every cache stores the key as wanted",
        async || {
            caches
                .iter()
                .all(|cache| want(&cache.shard.local_record(&key_bytes).1))
        },
    )
    .await;
}

/// Crashes `node`'s cluster and drops it from `nodes`.
async fn crash_node(nodes: &mut Vec<(Cluster, Cache<u32, String>)>, node: NodeId) {
    let at = nodes
        .iter()
        .position(|(cluster, _)| cluster.node_id() == node)
        .expect("the node is one of the cluster's");
    let (cluster, _) = nodes.remove(at);
    cluster.crash().await;
}

/// `explanation`'s distributed half, after checking it against `cache`:
/// its owners are the ones `Cache::owners_of` returns for `key`, and its
/// probes ask every owner but `cache`'s own node, in that order.
fn distributed_of<'a>(
    cache: &Cache<u32, String>,
    key: u32,
    explanation: &'a ReadExplanation,
) -> &'a DistributedRead {
    let read = explanation
        .distributed
        .as_ref()
        .expect("a Distributed cache explains its fetch");
    let owners = cache.owners_of(&key);
    assert_eq!(
        read.owners, owners,
        "the explanation names the owners owners_of does"
    );
    let me = cache.cluster.node_id();
    let others: Vec<NodeId> = owners.into_iter().filter(|owner| *owner != me).collect();
    let probed: Vec<NodeId> = read.probes.iter().map(|probe| probe.node).collect();
    assert_eq!(
        probed, others,
        "the probes ask every other owner, in owners order"
    );
    read
}

/// Each probe's answer, in probe order.
fn answers_of(read: &DistributedRead) -> Vec<ProbeAnswer> {
    read.probes.iter().map(|probe| probe.answer).collect()
}

/// The record `probe` reports its owner sending.
fn held_record(probe: &OwnerProbe) -> ProbedRecord {
    match probe.answer {
        ProbeAnswer::Held(record) => record,
        other => panic!("owner {} sends its record, answered {other:?}", probe.node),
    }
}

/// The record every probe of `read` reports sent, in probe order.
fn held_records(read: &DistributedRead) -> Vec<ProbedRecord> {
    read.probes.iter().map(held_record).collect()
}

/// Asserts a fetch of `key` on `cache` returns what `explanation.source`
/// predicts: a value for a source that returns one, `None` for one that
/// does not, and `FetchUnavailable` for no source.
async fn assert_fetch_returns_source(
    cache: &Cache<u32, String>,
    key: u32,
    explanation: &ReadExplanation,
) {
    let fetched = cache.fetch(&key).await;
    match (explanation.source, fetched) {
        (ReadSource::Local { hit: true } | ReadSource::Owner { hit: true, .. }, Ok(Some(_)))
        | (ReadSource::Local { hit: false } | ReadSource::Owner { hit: false, .. }, Ok(None))
        | (ReadSource::Unavailable, Err(CacheError::FetchUnavailable { .. })) => {}
        (source, fetched) => panic!("explained {source:?} for key {key}, fetched {fetched:?}"),
    }
}

/// A key a node does not own, probed through every state its two owners
/// hold it in: a live entry reads as a value on both under one version, a
/// delete reads as `Deleted` on both under a newer one, and a key never
/// written is a miss on both. The asking node holds nothing, so its own
/// copy never answers, and each source is what a fetch returns.
#[tokio::test]
async fn explain_probe_reports_a_value_a_tombstone_and_a_miss() {
    let (nodes, [key, absent]) = explain_nodes("cache-it-explain-probe-answers").await;
    let cache_a = &nodes[0].1;
    let owners = cache_a.owners_of(&key);
    let (first, second) = (cache_of(&nodes, owners[0]), cache_of(&nodes, owners[1]));

    first.insert(key, "one".to_string()).await.expect("insert");
    wait_for_local_record(&[first, second], key, LocalRecord::is_live).await;
    let explanation = cache_a.explain(&key).await.expect("explains");
    let read = distributed_of(cache_a, key, &explanation);
    assert_eq!(
        explanation.local,
        LocalRecord::Absent,
        "the asking node does not own the key's part"
    );
    assert_eq!(read.local_read, LocalRead::NotOwner);
    let live = held_records(read);
    assert!(
        live.iter()
            .all(|record| record.reads == Reads::Value && record.expires_at_ms.is_none()),
        "both owners hold a value without an expiry: {live:?}"
    );
    assert_eq!(live[0].version, live[1].version, "one write, one version");
    assert_eq!(
        explanation.source,
        ReadSource::Owner {
            node: owners[0],
            hit: true
        },
        "the first owner probed answers"
    );
    assert_fetch_returns_source(cache_a, key, &explanation).await;

    first.remove(&key).await.expect("remove");
    wait_for_local_record(&[first, second], key, |record| {
        matches!(record, LocalRecord::Tombstone { .. })
    })
    .await;
    let explanation = cache_a.explain(&key).await.expect("explains");
    let read = distributed_of(cache_a, key, &explanation);
    let dead = held_records(read);
    assert!(
        dead.iter().all(|record| record.reads == Reads::Deleted),
        "both owners send a tombstone, which reads as no value: {dead:?}"
    );
    assert_eq!(dead[0].version, dead[1].version, "one delete, one version");
    assert!(
        dead[0].version > live[0].version,
        "the delete is newer than the write it removes"
    );
    assert_eq!(
        explanation.source,
        ReadSource::Owner {
            node: owners[0],
            hit: false
        }
    );
    assert_fetch_returns_source(cache_a, key, &explanation).await;

    let explanation = cache_a.explain(&absent).await.expect("explains");
    let read = distributed_of(cache_a, absent, &explanation);
    assert_eq!(
        answers_of(read),
        [ProbeAnswer::Miss, ProbeAnswer::Miss],
        "warm owners on one view agree a key never written is a miss"
    );
    assert_eq!(
        explanation.source,
        ReadSource::Owner {
            node: read.owners[0],
            hit: false
        }
    );
    assert_fetch_returns_source(cache_a, absent, &explanation).await;

    shut_down_all(nodes).await;
}

/// An owner whose copy of the part is cold and holds no record declines,
/// and so does one that holds the record in a part it distrusts (owned
/// again after a loss): the fetch moves past the first and is
/// unavailable once both decline. Each owner's own explanation names the
/// verdict the asking node saw as its answer.
#[tokio::test]
async fn explain_probe_reports_a_cold_owner_and_a_distrusted_owner_as_declined() {
    let (nodes, [key, _]) = explain_nodes("cache-it-explain-probe-declines").await;
    let cache_a = &nodes[0].1;
    let owners = cache_a.owners_of(&key);
    let (cold, distrusted) = (cache_of(&nodes, owners[0]), cache_of(&nodes, owners[1]));
    cold.insert(key, "held".to_string()).await.expect("insert");
    wait_for_local_record(&[cold, distrusted], key, LocalRecord::is_live).await;
    let part = PartId::of_key(&encode_key(&key).expect("u32 encodes"));

    cold.invalidate_local(&key).await;
    cold.shard
        .residency()
        .expect("the owner is distributed")
        .mark_cold(&[part]);
    let explanation = cache_a.explain(&key).await.expect("explains");
    let read = distributed_of(cache_a, key, &explanation);
    assert_eq!(
        read.probes[0].answer,
        ProbeAnswer::Declined,
        "a cold part's miss says nothing"
    );
    assert_eq!(held_record(&read.probes[1]).reads, Reads::Value);
    assert_eq!(
        explanation.source,
        ReadSource::Owner {
            node: owners[1],
            hit: true
        },
        "the fetch moves past a decline to the next owner"
    );
    assert_fetch_returns_source(cache_a, key, &explanation).await;
    let own = cold.explain(&key).await.expect("explains");
    let own_read = distributed_of(cold, key, &own);
    assert!(own_read.residency.cold_marked);
    assert_eq!(own_read.local_read, LocalRead::ColdMiss);
    assert_eq!(own_read.serves_peers, ServeVerdict::DeclineCold);

    distrusted
        .shard
        .residency()
        .expect("the owner is distributed")
        .mark_stale(&[part]);
    let explanation = cache_a.explain(&key).await.expect("explains");
    let read = distributed_of(cache_a, key, &explanation);
    assert_eq!(
        answers_of(read),
        [ProbeAnswer::Declined, ProbeAnswer::Declined],
        "a distrusted part declines even the record it holds"
    );
    assert_eq!(explanation.source, ReadSource::Unavailable);
    assert_fetch_returns_source(cache_a, key, &explanation).await;
    let own = distrusted.explain(&key).await.expect("explains");
    assert!(own.local.is_live(), "the declining owner still holds it");
    let own_read = distributed_of(distrusted, key, &own);
    assert!(own_read.residency.stale);
    assert_eq!(own_read.local_read, LocalRead::Distrusted);
    assert_eq!(own_read.serves_peers, ServeVerdict::DeclineDistrusted);

    shut_down_all(nodes).await;
}

/// Whether `answer` is what a probe of a crashed owner reads: a refused
/// connection, or a timeout where the platform retries a refused
/// loopback connect for longer than `fetch_timeout`, as Windows does
/// for about two seconds.
fn crashed_owner_answer(answer: ProbeAnswer) -> bool {
    matches!(
        answer,
        ProbeAnswer::Unreached(Unreached::Io(_) | Unreached::TimedOut)
    )
}

/// An owner whose node crashed gives no answer, while the other owner
/// still answers: the view keeps naming the crashed owner, the fetch
/// moves past it, and the source is the second owner.
#[tokio::test]
async fn explain_probe_reports_a_crashed_owner_as_unreached() {
    let (mut nodes, [key, _]) = explain_nodes("cache-it-explain-probe-crashed").await;
    let owners = nodes[0].1.owners_of(&key);
    let (crashed, survivor) = (owners[0], cache_of(&nodes, owners[1]));
    survivor
        .insert(key, "value".to_string())
        .await
        .expect("insert");
    wait_for_local_record(
        &[cache_of(&nodes, crashed), survivor],
        key,
        LocalRecord::is_live,
    )
    .await;

    crash_node(&mut nodes, crashed).await;
    let cache_a = &nodes[0].1;
    let explanation = cache_a.explain(&key).await.expect("explains");
    let read = distributed_of(cache_a, key, &explanation);
    assert!(
        crashed_owner_answer(read.probes[0].answer),
        "a crashed owner gives no answer: {:?}",
        read.probes[0].answer
    );
    assert_eq!(held_record(&read.probes[1]).reads, Reads::Value);
    assert_eq!(
        explanation.source,
        ReadSource::Owner {
            node: owners[1],
            hit: true
        }
    );
    assert_fetch_returns_source(cache_a, key, &explanation).await;

    shut_down_all(nodes).await;
}

/// An owner that holds no record and runs another ownership view answers
/// `StaleView` with its own view hash, which a fetch counts as no answer,
/// while an owner that holds the record sends it whatever its view.
#[tokio::test]
async fn explain_probe_reports_an_owner_on_another_view_as_stale_view() {
    let name = "cache-it-explain-probe-stale-view";
    let mut config = loopback_config();
    config.fetch_timeout = Duration::from_millis(300);
    let a = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(config)
        .build()
        .await
        .expect("node a builds");
    let b = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node b builds");
    let cache_b = b
        .cache::<u32, String>(name)
        .mode(Mode::distributed())
        .open()
        .await
        .expect("b opens alone");
    let peer_b = b.local_peer();
    a.mesh().update_peers(vec![peer_b.clone()]);

    // a's view ranks a and b, b's ranks b alone: the hashes differ.
    let view = Arc::new(OwnershipView::compute(
        a.node_id(),
        vec![a.node_id(), peer_b.node],
        NonZeroU8::MIN,
    ));
    let part_of = |key: &u32| PartId::of_key(&encode_key(key).expect("u32 encodes"));
    let [missing, stored] = (0..1_000_000u32)
        .filter(|key| !view.owns(part_of(key)))
        .take(2)
        .collect::<Vec<_>>()[..]
    else {
        panic!("b owns some parts of the view");
    };
    cache_b
        .insert(stored, "stored".to_string())
        .await
        .expect("b owns every part while alone");
    let (cache_a, _residency) = cache_under_view(&a, name, &view);

    let explanation = cache_a.explain(&missing).await.expect("explains");
    let read = distributed_of(&cache_a, missing, &explanation);
    let ProbeAnswer::StaleView {
        responder_view_hash,
    } = read.probes[0].answer
    else {
        panic!("b runs another view, answered {:?}", read.probes[0].answer);
    };
    assert_ne!(responder_view_hash, read.view_hash);
    assert_eq!(
        Some(responder_view_hash),
        cache_b.shard.ownership_view_hash(),
        "the answer carries the owner's own view hash"
    );
    assert!(!read.probes[0].answer.answers());
    assert_eq!(read.view_moved_to, None);
    assert_eq!(
        explanation.source,
        ReadSource::Unavailable,
        "a stale view counts as no answer"
    );
    assert_fetch_returns_source(&cache_a, missing, &explanation).await;

    let explanation = cache_a.explain(&stored).await.expect("explains");
    let read = distributed_of(&cache_a, stored, &explanation);
    assert_eq!(
        held_record(&read.probes[0]).reads,
        Reads::Value,
        "a held record is sent whatever the two views say"
    );
    assert_eq!(
        explanation.source,
        ReadSource::Owner {
            node: peer_b.node,
            hit: true
        }
    );
    assert_fetch_returns_source(&cache_a, stored, &explanation).await;

    cache_b.close().await;
    b.shutdown().await;
    a.shutdown().await;
}

/// With both owners of a key crashed and the asking node's view still
/// naming them, no probe answers: the source is unavailable and a fetch
/// returns `FetchUnavailable`.
#[tokio::test]
async fn explain_predicts_unavailable_when_every_owner_is_down() {
    let (mut nodes, [key, _]) = explain_nodes("cache-it-explain-all-owners-down").await;
    let owners = nodes[0].1.owners_of(&key);
    cache_of(&nodes, owners[0])
        .insert(key, "value".to_string())
        .await
        .expect("insert");
    wait_for_local_record(
        &[cache_of(&nodes, owners[0]), cache_of(&nodes, owners[1])],
        key,
        LocalRecord::is_live,
    )
    .await;

    for owner in &owners {
        crash_node(&mut nodes, *owner).await;
    }
    let cache_a = &nodes[0].1;
    let explanation = cache_a.explain(&key).await.expect("explains");
    let read = distributed_of(cache_a, key, &explanation);
    assert!(
        read.probes
            .iter()
            .all(|probe| crashed_owner_answer(probe.answer)),
        "no crashed owner answers: {:?}",
        answers_of(read)
    );
    assert_eq!(explanation.local, LocalRecord::Absent);
    assert_eq!(explanation.source, ReadSource::Unavailable);
    assert_fetch_returns_source(cache_a, key, &explanation).await;

    shut_down_all(nodes).await;
}

/// The owners are asked at once: two owners that accept a connection and
/// never answer both read `TimedOut`, and the call returns after about
/// one `fetch_timeout`, not one per owner.
#[tokio::test]
async fn explain_probes_the_owners_at_once() {
    use crate::net::test_support::peer_at;

    let name = "cache-it-explain-probe-at-once";
    let fetch_timeout = Duration::from_millis(500);
    let a = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(ClusterConfig {
            fetch_timeout,
            ..loopback_config()
        })
        .build()
        .await
        .expect("node a builds");
    let silent = [NodeId::from(0xb), NodeId::from(0xc)];
    let addrs = [silent_peer_addr().await, silent_peer_addr().await];
    // The cluster republishes its empty peer list once, when its mesh
    // task first runs: let that happen before the peers are injected.
    tokio::task::yield_now().await;
    a.mesh().update_peers(vec![
        peer_at(silent[0], addrs[0]),
        peer_at(silent[1], addrs[1]),
    ]);
    let view = Arc::new(OwnershipView::compute(
        a.node_id(),
        vec![a.node_id(), silent[0], silent[1]],
        NonZeroU8::new(2).expect("nonzero"),
    ));
    let key = (0..1_000_000u32)
        .find(|key| !view.owns(PartId::of_key(&encode_key(key).expect("u32 encodes"))))
        .expect("a owns only some parts of a three-node view");
    let (cache_a, _residency) = cache_under_view(&a, name, &view);

    let started = Instant::now();
    let explanation = cache_a.explain(&key).await.expect("explains");
    let elapsed = started.elapsed();
    let read = distributed_of(&cache_a, key, &explanation);
    assert_eq!(
        answers_of(read),
        [ProbeAnswer::Unreached(Unreached::TimedOut); 2],
        "both owners accept the connection and never answer"
    );
    assert!(
        elapsed >= fetch_timeout,
        "each probe waits out the fetch timeout: {elapsed:?}"
    );
    assert!(
        elapsed < 2 * fetch_timeout,
        "the owners are asked at once, not one after the other: {elapsed:?}"
    );
    assert_eq!(explanation.source, ReadSource::Unavailable);
    assert_fetch_returns_source(&cache_a, key, &explanation).await;

    a.shutdown().await;
}

#[tokio::test]
async fn a_distributed_load_runs_on_the_first_owner_and_stays_off_a_non_owner() {
    let log = LoadLog::default();
    let nodes = three_loading_nodes("cache-it-load-owner", Mode::distributed(), &log, None).await;
    let (a, cache_a) = &nodes[0];
    let key = (0..10_000u32)
        .find(|key| !cache_a.owners_of(key).contains(&a.node_id()))
        .expect("a key a does not own");
    let owners = cache_a.owners_of(&key);
    wait_until(
        CONVERGE,
        "every node agrees on the key's owners",
        async || {
            nodes
                .iter()
                .all(|(_, cache)| cache.owners_of(&key) == owners)
        },
    )
    .await;
    // A cold owner declines a load, which then runs on the next owner.
    wait_for_warm_owners(&nodes, key).await;

    assert_eq!(
        cache_a.load(&key).await.expect("load"),
        Some(format!("v{key}"))
    );
    assert_eq!(
        log.lock().expect("log lock").clone(),
        vec![(owners[0], vec![key])],
        "the key's first owner runs the one load"
    );
    assert_eq!(cache_a.get(&key).await, None, "a non-owner keeps no copy");
    wait_until(CONVERGE, "both owners hold the loaded value", async || {
        let mut held = 0;
        for (cluster, cache) in &nodes {
            if owners.contains(&cluster.node_id()) && cache.get(&key).await.is_some() {
                held += 1;
            }
        }
        held == 2
    })
    .await;
    assert_eq!(
        cache_a.load(&key).await.expect("load"),
        Some(format!("v{key}"))
    );
    assert_eq!(
        log.lock().expect("log lock").len(),
        1,
        "the owner answers the second read from its copy"
    );
    shut_down_all(nodes).await;
}

#[tokio::test]
async fn a_failing_remote_loader_answers_with_a_remote_loader_error() {
    let log = LoadLog::default();
    let nodes = three_loading_nodes(
        "cache-it-load-remote-failure",
        Mode::Invalidation,
        &log,
        Some(1),
    )
    .await;
    let (a, cache_a) = &nodes[0];
    let failing = nodes[1].0.node_id();
    let capable = cache_a.loader_peers();
    let key = (0..10_000u32)
        .find(|key| {
            let part = PartId::of_key(&encode_key(key).expect("u32 encodes"));
            load_target(Mode::Invalidation, part, a.node_id(), &[], &capable)
                == LoadTarget::Peer(failing)
        })
        .expect("a key whose loads gather on the failing node");
    let err = cache_a
        .load(&key)
        .await
        .expect_err("the remote loader fails");
    let CacheError::Loader(source) = &err else {
        panic!("a loader error, got {err:?}");
    };
    let remote = std::iter::successors(Some(source.as_ref() as &dyn std::error::Error), |err| {
        err.source()
    })
    .find_map(|err| err.downcast_ref::<RemoteLoaderError>())
    .expect("the error chain names the remote loader");
    assert_eq!(remote.node, failing);
    assert_eq!(remote.message, "source unavailable");
    assert_eq!(cache_a.get(&key).await, None);
    assert_eq!(
        log.lock().expect("log lock").clone(),
        vec![(failing, vec![key])],
        "a remote failure is not retried here"
    );
    shut_down_all(nodes).await;
}

/// A 10s TTL with refresh-ahead at 0.1: a key falls due 1s after its
/// write and expires 9s after that, so a stalled runner reads a due key
/// long before it expires.
const LONG_REFRESH: (Duration, f64) = (Duration::from_secs(10), 0.1);

/// `count` nodes with `name` open under `mode` with `lifetime`'s TTL and
/// refresh-ahead fraction, each with a batch loader that records its
/// calls in `log` and holds `v{key}.{n}`, `n` counting calls across all
/// nodes. Returns once every node sees the others advertise a loader
/// and, under `Mode::Distributed`, ranks them all in its ownership view.
async fn refreshing_nodes(
    name: &'static str,
    mode: Mode,
    count: usize,
    lifetime: (Duration, f64),
    log: &LoadLog,
) -> Vec<(Cluster, Cache<u32, String>)> {
    let calls = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut nodes: Vec<(Cluster, Cache<u32, String>)> = Vec::new();
    for index in 0..count {
        let builder = Cluster::builder(name).config(loopback_config());
        let cluster = match nodes.first() {
            None => builder.seeds(std::iter::empty()),
            Some((seed, _)) => builder.seeds([seed.local_gossip_addr()]),
        }
        .build()
        .await
        .expect("node builds");
        wait_for_peer_count(&cluster, index).await;
        let node = cluster.node_id();
        let (log, calls) = (Arc::clone(log), Arc::clone(&calls));
        let cache = cluster
            .cache::<u32, String>(name)
            .mode(mode)
            .ttl(lifetime.0)
            .refresh_ahead(lifetime.1)
            .batch_loader(move |keys: Vec<u32>| {
                let (log, calls) = (Arc::clone(&log), Arc::clone(&calls));
                async move {
                    let call = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                    let mut sorted = keys.clone();
                    sorted.sort_unstable();
                    log.lock().expect("log lock").push((node, sorted));
                    Ok::<_, SourceDown>(
                        keys.into_iter()
                            .map(|key| (key, format!("v{key}.{call}")))
                            .collect::<HashMap<_, _>>(),
                    )
                }
            })
            .open()
            .await
            .expect("opens");
        nodes.push((cluster, cache));
    }
    wait_until(
        Duration::from_secs(10),
        "every node sees the others advertise a loader",
        async || {
            nodes
                .iter()
                .all(|(_, cache)| cache.loader_peers().len() == count - 1)
        },
    )
    .await;
    wait_for_full_views(&nodes).await;
    nodes
}

#[tokio::test]
async fn refresh_ahead_keeps_a_read_key_alive_past_its_expiry_on_one_node() {
    let log = LoadLog::default();
    // A 5s TTL refreshed a fifth of the way in: a key falls due 1s
    // after its write and expires 4s after that, so a runner stalled
    // for up to 4s still reads it in time, and the 7s of reads below
    // outlive the lifetime.
    let lifetime = (Duration::from_secs(5), 0.2);
    let nodes = refreshing_nodes("cache-it-refresh-local", Mode::Local, 1, lifetime, &log).await;
    let cache = &nodes[0].1;
    assert_eq!(
        cache.load(&1).await.expect("load"),
        Some("v1.1".to_string())
    );
    let loaded_at = Instant::now();
    while loaded_at.elapsed() < Duration::from_secs(7) {
        assert!(
            cache.get(&1).await.is_some(),
            "a key read through its refresh window never expires"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // A reload restarts the 5s lifetime, so the next refresh point is
    // 1s later: over 7s of reads, the load and about six reloads, fewer
    // when the runner stalls between reads.
    let loads = log.lock().expect("log lock").len();
    assert!(
        (2..=8).contains(&loads),
        "about one reload a refresh interval, not one per read: {loads} loads"
    );
    shut_down_all(nodes).await;
}

#[tokio::test]
async fn refresh_ahead_reloads_a_replicated_key_once_on_its_refresher() {
    let log = LoadLog::default();
    let nodes = refreshing_nodes(
        "cache-it-refresh-replicated",
        Mode::Replicated,
        2,
        LONG_REFRESH,
        &log,
    )
    .await;
    let (a, cache_a) = &nodes[0];
    let (b, cache_b) = &nodes[1];
    let key = 7u32;
    assert_eq!(
        cache_a.load(&key).await.expect("load"),
        Some("v7.1".to_string())
    );
    wait_until(
        Duration::from_secs(5),
        "b holds the loaded value",
        async || cache_b.get(&key).await.is_some(),
    )
    .await;
    let part = PartId::of_key(&encode_key(&key).expect("u32 encodes"));
    let refresher = match load_target(
        Mode::Replicated,
        part,
        a.node_id(),
        &[],
        &[b.node_id()].into_iter().collect(),
    ) {
        LoadTarget::Here => a.node_id(),
        LoadTarget::Peer(peer) => peer,
    };

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    for _ in 0..20 {
        assert!(cache_a.get(&key).await.is_some());
        assert!(cache_b.get(&key).await.is_some());
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    wait_until(
        Duration::from_secs(5),
        "both nodes hold the reloaded value",
        async || {
            cache_a.get(&key).await == Some("v7.2".to_string())
                && cache_b.get(&key).await == Some("v7.2".to_string())
        },
    )
    .await;
    let log = log.lock().expect("log lock").clone();
    assert_eq!(
        log.len(),
        2,
        "one load and one reload for both nodes' reads: {log:?}"
    );
    assert_eq!(log[1], (refresher, vec![key]), "the refresher reloads");
    shut_down_all(nodes).await;
}

#[tokio::test]
async fn refresh_ahead_reloads_a_distributed_key_a_non_owner_fetches_on_its_owner() {
    let log = LoadLog::default();
    let nodes = refreshing_nodes(
        "cache-it-refresh-distributed",
        Mode::distributed(),
        3,
        LONG_REFRESH,
        &log,
    )
    .await;
    let (a, cache_a) = &nodes[0];
    let key = (0..10_000u32)
        .find(|key| !cache_a.owners_of(key).contains(&a.node_id()))
        .expect("a key a does not own");
    let owners = cache_a.owners_of(&key);
    wait_until(
        CONVERGE,
        "every node agrees on the key's owners",
        async || {
            nodes
                .iter()
                .all(|(_, cache)| cache.owners_of(&key) == owners)
        },
    )
    .await;
    assert!(cache_a.load(&key).await.expect("load").is_some());
    let first = log.lock().expect("log lock").len();

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert!(cache_a.fetch(&key).await.expect("fetch").is_some());
    wait_until(
        Duration::from_secs(5),
        "the first owner reloads the key a non-owner read",
        async || log.lock().expect("log lock").len() > first,
    )
    .await;
    assert_eq!(
        log.lock().expect("log lock")[first],
        (owners[0], vec![key]),
        "the first owner reloads"
    );
    shut_down_all(nodes).await;
}

/// `explain` inside a key's refresh window is not a read: asked of every
/// node, the owners that hold the entry and the non-owner whose `fetch`
/// hints the owner included, it queues no reload and no loader runs,
/// where the `fetch` of the same key right after asks for one.
#[tokio::test]
async fn explain_does_not_start_a_refresh_inside_the_refresh_window() {
    let log = LoadLog::default();
    let nodes = refreshing_nodes(
        "cache-it-explain-refresh",
        Mode::distributed(),
        3,
        LONG_REFRESH,
        &log,
    )
    .await;
    let (a, cache_a) = &nodes[0];
    let key = (0..10_000u32)
        .find(|key| !cache_a.owners_of(key).contains(&a.node_id()))
        .expect("a key a does not own");
    let owners = cache_a.owners_of(&key);
    wait_until(
        CONVERGE,
        "every node agrees on the key's owners",
        async || {
            nodes
                .iter()
                .all(|(_, cache)| cache.owners_of(&key) == owners)
        },
    )
    .await;
    assert!(cache_a.load(&key).await.expect("load").is_some());
    let loaded = log.lock().expect("log lock").len();
    wait_for_local_record(
        &[cache_of(&nodes, owners[0]), cache_of(&nodes, owners[1])],
        key,
        LocalRecord::is_live,
    )
    .await;

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    for (_, cache) in &nodes {
        let explanation = cache.explain(&key).await.expect("explains");
        assert!(
            explanation.source.returns_value(),
            "the entry is live: {explanation}"
        );
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        log.lock().expect("log lock").len(),
        loaded,
        "explain asks for no reload"
    );

    assert!(cache_a.fetch(&key).await.expect("fetch").is_some());
    wait_until(
        Duration::from_secs(5),
        "the first owner reloads the key a non-owner read",
        async || log.lock().expect("log lock").len() > loaded,
    )
    .await;
    assert_eq!(
        log.lock().expect("log lock")[loaded],
        (owners[0], vec![key]),
        "the first owner reloads"
    );
    shut_down_all(nodes).await;
}

#[tokio::test]
async fn refresh_ahead_needs_a_loader_a_ttl_and_a_mode_that_carries_values() {
    let cluster = Cluster::builder("cache-it-refresh-config")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node builds");
    let loader = |key: u32| async move { Ok::<_, SourceDown>(Some(format!("v{key}"))) };
    let reason = |result: Result<Cache<u32, String>, CacheError>| match result {
        Err(CacheError::InvalidLoadConfig { reason, .. }) => reason,
        other => panic!("an invalid load config, got {other:?}"),
    };
    assert_eq!(
        reason(
            cluster
                .cache::<u32, String>("no-ttl")
                .loader(loader)
                .refresh_ahead(0.8)
                .open()
                .await
        ),
        "refresh_ahead needs a ttl"
    );
    assert_eq!(
        reason(
            cluster
                .cache::<u32, String>("invalidation")
                .mode(Mode::Invalidation)
                .ttl(Duration::from_secs(1))
                .loader(loader)
                .refresh_ahead(0.8)
                .open()
                .await
        ),
        "refresh_ahead does not apply to Mode::Invalidation"
    );
    assert_eq!(
        reason(
            cluster
                .cache::<u32, String>("whole")
                .mode(Mode::Local)
                .ttl(Duration::from_secs(1))
                .loader(loader)
                .refresh_ahead(1.0)
                .open()
                .await
        ),
        "refresh_ahead takes a fraction strictly between 0 and 1"
    );
    cluster.shutdown().await;
}

#[tokio::test]
async fn a_new_lifetime_replicates_to_every_replica() {
    let name = "cache-it-ttl-replicated";
    let a = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("a builds");
    let b = Cluster::builder(name)
        .seeds([a.local_gossip_addr()])
        .config(loopback_config())
        .build()
        .await
        .expect("b builds");
    wait_for_peer_count(&a, 1).await;
    wait_for_peer_count(&b, 1).await;
    let open = |cluster: &Cluster| {
        cluster
            .cache::<u32, String>("sessions")
            .mode(Mode::Replicated)
            .open()
    };
    let cache_a = open(&a).await.expect("a opens");
    let cache_b = open(&b).await.expect("b opens");

    cache_a.insert(1, "a".into()).await.expect("insert");
    wait_until(Duration::from_secs(10), "b holds the entry", async || {
        cache_b.ttl_of(&1) == Some(Ttl::Never)
    })
    .await;

    assert!(
        cache_a
            .expire(&1, Duration::from_secs(60))
            .await
            .expect("expire")
    );
    wait_until(
        Duration::from_secs(10),
        "b takes the new lifetime",
        async || {
            matches!(cache_b.ttl_of(&1), Some(Ttl::Remaining(left))
                if left <= Duration::from_secs(60) && left > Duration::from_secs(50))
        },
    )
    .await;
    assert!(cache_b.persist(&1).await.expect("persist"));
    wait_until(
        Duration::from_secs(10),
        "a takes b's persist back",
        async || cache_a.ttl_of(&1) == Some(Ttl::Never),
    )
    .await;
    assert_eq!(cache_a.get(&1).await, Some("a".to_string()));

    b.shutdown().await;
    a.shutdown().await;
}

/// `a` owns none of the key's part, so it asks an owner for the record
/// and forwards the re-arm: both owners take the lifetime and `a` still
/// holds nothing. A key no owner holds answers `false`.
#[tokio::test]
async fn expire_from_a_node_that_does_not_own_the_key_reaches_its_owners() {
    let ((a, cache_a), (b, cache_b), (c, cache_c), unowned_key) =
        three_node_distributed("cache-it-ttl-forward", "prices").await;
    cache_b
        .insert(unowned_key, "value".to_string())
        .await
        .expect("insert");
    wait_until(
        Duration::from_secs(10),
        "both owners hold the value",
        async || cache_b.ttl_of(&unowned_key).is_some() && cache_c.ttl_of(&unowned_key).is_some(),
    )
    .await;

    assert!(
        cache_a
            .expire(&unowned_key, Duration::from_secs(60))
            .await
            .expect("expire")
    );
    let rearmed = |cache: &Cache<u32, String>| {
        matches!(cache.ttl_of(&unowned_key), Some(Ttl::Remaining(left))
            if left <= Duration::from_secs(60) && left > Duration::from_secs(50))
    };
    wait_until(
        Duration::from_secs(10),
        "both owners take the new lifetime",
        async || rearmed(&cache_b) && rearmed(&cache_c),
    )
    .await;
    assert_eq!(cache_a.ttl_of(&unowned_key), None, "a still holds no copy");
    assert_eq!(
        cache_a.fetch(&unowned_key).await.expect("fetch"),
        Some("value".to_string())
    );

    let absent = (unowned_key + 1..u32::MAX)
        .find(|k| !cache_a.owners_of(k).contains(&a.node_id()))
        .expect("another key a does not own");
    assert!(
        !cache_a.persist(&absent).await.expect("the owners answer"),
        "no owner holds the key"
    );

    c.shutdown().await;
    b.shutdown().await;
    a.shutdown().await;
}

/// Unlike a crash, a graceful leave hands a key on: when both of its
/// owners shut down one after the other, the remaining node inherits
/// the key and still reads it.
#[tokio::test]
async fn both_owners_leaving_gracefully_hand_the_key_to_the_remaining_node() {
    let ((a, cache_a), (b, cache_b), (c, _cache_c), unowned_key) =
        three_node_distributed("cache-it-graceful-hand-off", "prices").await;
    cache_b
        .insert(unowned_key, "value".to_string())
        .await
        .expect("insert");
    wait_until(
        Duration::from_secs(10),
        "the value reaches its owners",
        async || matches!(cache_a.fetch(&unowned_key).await, Ok(Some(value)) if value == "value"),
    )
    .await;

    b.shutdown().await;
    c.shutdown().await;

    wait_until(
        Duration::from_secs(10),
        "a owns the key alone and holds it",
        async || cache_a.owners_of(&unowned_key) == vec![a.node_id()],
    )
    .await;
    assert_eq!(
        cache_a.get(&unowned_key).await,
        Some("value".to_string()),
        "the hand-off put the value on a before its last owner left"
    );

    a.shutdown().await;
}

#[tokio::test]
async fn owners_of_matches_the_ownership_view_for_the_keys_bucket() {
    let ((a, cache_a), (b, _cache_b), (c, _cache_c), unowned_key) =
        three_node_distributed("cache-it-owners-of", "prices").await;

    let owners = cache_a.owners_of(&unowned_key);
    assert_eq!(owners.len(), 2, "owners = 2 for this cache");
    assert!(
        !owners.contains(&a.node_id()),
        "this key was chosen specifically because a does not own it"
    );
    // Every owner reported is a real, live node in this cluster.
    for owner in &owners {
        assert!(*owner == a.node_id() || *owner == b.node_id() || *owner == c.node_id());
    }

    a.shutdown().await;
    b.shutdown().await;
    c.shutdown().await;
}

/// The end-to-end proof that the write forward, the fan-out group-by-
/// owner-set, and the fetch read path all compose: a write through a
/// node that does not own the key's bucket never becomes locally
/// visible on the writer, forwards to the real owners, and both the
/// writer and another non-owner read the value back correctly through
/// [`Cache::fetch`].
#[tokio::test]
async fn write_through_a_non_owner_composes_with_fetch_on_every_node() {
    let ((a, cache_a), (b, cache_b), (c, cache_c), unowned_key) =
        three_node_distributed("cache-it-write-through-non-owner", "prices").await;

    // `a` was chosen specifically because it does not own this key's
    // bucket: this insert never touches a's engine, only forwards.
    cache_a
        .insert(unowned_key, "through-a".to_string())
        .await
        .expect("insert forwards");
    assert_eq!(
        cache_a.get(&unowned_key).await,
        None,
        "a forwarded write never becomes locally visible on the writer"
    );

    // Every node, owner or not, reads the same value back through
    // fetch: the writer itself, and whichever of b/c is not a real
    // owner either.
    let owners = cache_a.owners_of(&unowned_key);
    for cache in [&cache_a, &cache_b, &cache_c] {
        wait_until(
            Duration::from_secs(10),
            "fetch converges to the forwarded value on every node",
            async || matches!(cache.fetch(&unowned_key).await, Ok(Some(value)) if value == "through-a"),
        )
        .await;
    }
    assert_eq!(owners.len(), 2);

    a.shutdown().await;
    b.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn close_then_reopen_the_same_name_succeeds() {
    let cluster = Cluster::builder("cache-it-close-reopen")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let cache = cluster
        .cache::<u32, String>("scratch")
        .open()
        .await
        .expect("first open succeeds");
    cache.insert(1, "a".into()).await.expect("insert");
    cache.close().await;

    let reopened = cluster
        .cache::<u32, String>("scratch")
        .open()
        .await
        .expect("closing frees the name for a fresh open");
    assert_eq!(
        reopened.get(&1).await,
        None,
        "the reopened cache starts empty, not resuming the closed shard's state"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn close_returns_only_once_every_background_task_has_stopped() {
    let cluster = Cluster::builder("cache-it-close-waits")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");
    let cache = cluster
        .cache::<u32, String>("orders")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("open succeeds");
    let tasks = cache.tasks.clone();
    assert!(
        !tasks.is_empty(),
        "a Replicated cache runs background tasks"
    );

    cache.close().await;

    assert!(tasks.is_closed() && tasks.is_empty());
    let caches = cluster.health().caches;
    assert!(caches.is_empty(), "{caches:?}");
    cluster.shutdown().await;
}

#[tokio::test]
async fn closing_one_clone_leaves_another_clone_usable() {
    let cluster = Cluster::builder("cache-it-close-clone")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let cache = cluster
        .cache::<u32, String>("shared")
        .open()
        .await
        .expect("open succeeds");
    let surviving = cache.clone();

    cache.close().await;

    surviving
        .insert(2, "still here".into())
        .await
        .expect("a surviving clone keeps writing locally after another clone closes");
    assert_eq!(
        surviving.get(&2).await,
        Some("still here".to_string()),
        "a surviving clone keeps reading locally after another clone closes"
    );
    assert!(
        surviving.shard.fan_out_queue().drain().is_empty(),
        "a detached clone queues nothing for a fan-out task that no longer runs"
    );

    cluster.shutdown().await;
}

#[test]
fn routes_loads_only_where_values_do_not_reach_every_node() {
    assert!(!routes_loads(Mode::Local));
    assert!(!routes_loads(Mode::Replicated));
    assert!(routes_loads(Mode::Invalidation));
    assert!(routes_loads(Mode::distributed()));
}

#[test]
fn load_target_in_distributed_takes_the_first_owner_that_can_load() {
    let me = NodeId::from(1);
    let (a, b) = (NodeId::from(2), NodeId::from(3));
    let part = PartId::from_raw(7);
    let both: HashSet<NodeId> = [a, b].into_iter().collect();
    let mode = Mode::distributed();
    assert_eq!(
        load_target(mode, part, me, &[a, b], &both),
        LoadTarget::Peer(a)
    );
    assert_eq!(
        load_target(mode, part, me, &[a, b], &[b].into_iter().collect()),
        LoadTarget::Peer(b),
        "an owner with no loader is skipped"
    );
    assert_eq!(
        load_target(mode, part, me, &[me, a], &both),
        LoadTarget::Here,
        "this node, as the first owner, loads its own keys"
    );
    assert_eq!(
        load_target(mode, part, me, &[a, b], &HashSet::new()),
        LoadTarget::Here,
        "with no owner able to load, this node loads"
    );
    assert_eq!(load_target(mode, part, me, &[], &both), LoadTarget::Here);
}

#[test]
fn load_target_elsewhere_is_one_rendezvous_winner_every_node_agrees_on() {
    let nodes: Vec<NodeId> = (1..=5).map(NodeId::from).collect();
    for raw in [0u16, 1, 99, 4_000, 65_535] {
        let part = PartId::from_raw(raw);
        let picks: HashSet<NodeId> = nodes
            .iter()
            .map(|&me| {
                let capable: HashSet<NodeId> = nodes.iter().copied().filter(|&n| n != me).collect();
                match load_target(Mode::Invalidation, part, me, &[], &capable) {
                    LoadTarget::Here => me,
                    LoadTarget::Peer(peer) => peer,
                }
            })
            .collect();
        assert_eq!(
            picks.len(),
            1,
            "part {raw}: every node picks the same loader"
        );
    }
    let me = NodeId::from(1);
    assert_eq!(
        load_target(
            Mode::Invalidation,
            PartId::from_raw(3),
            me,
            &[],
            &HashSet::new()
        ),
        LoadTarget::Here,
        "alone, this node loads"
    );
    let spread: HashSet<NodeId> = (0..64u16)
        .map(|raw| {
            let capable: HashSet<NodeId> = nodes[1..].iter().copied().collect();
            match load_target(Mode::Invalidation, PartId::from_raw(raw), me, &[], &capable) {
                LoadTarget::Here => me,
                LoadTarget::Peer(peer) => peer,
            }
        })
        .collect();
    assert!(spread.len() > 1, "parts spread across the loaders");
}

fn load_config(has_loader: bool, has_batch_window: bool) -> LoadConfig {
    LoadConfig {
        has_loader,
        has_batch_window,
        refresh_ahead: None,
        has_ttl: true,
        mode: Mode::Replicated,
    }
}

#[test]
fn validate_load_config_needs_a_loader_for_a_batch_window() {
    assert_eq!(validate_load_config(load_config(false, false)), Ok(None));
    assert_eq!(validate_load_config(load_config(true, false)), Ok(None));
    assert_eq!(validate_load_config(load_config(true, true)), Ok(None));
    assert_eq!(
        validate_load_config(load_config(false, true)),
        Err("batch_window needs a loader")
    );
}

#[test]
fn validate_load_config_checks_refresh_ahead() {
    let refresh = |fraction, has_loader, has_ttl, mode| {
        validate_load_config(LoadConfig {
            has_loader,
            has_batch_window: false,
            refresh_ahead: Some(fraction),
            has_ttl,
            mode,
        })
    };
    for mode in [Mode::Local, Mode::Replicated, Mode::distributed()] {
        assert_eq!(refresh(0.8, true, true, mode), Ok(Some(800)), "{mode:?}");
    }
    assert_eq!(
        refresh(0.8, false, true, Mode::Replicated),
        Err("refresh_ahead needs a loader")
    );
    assert_eq!(
        refresh(0.8, true, false, Mode::Replicated),
        Err("refresh_ahead needs a ttl")
    );
    assert_eq!(
        refresh(0.8, true, true, Mode::Invalidation),
        Err("refresh_ahead does not apply to Mode::Invalidation")
    );
    assert_eq!(
        refresh(1.0, true, true, Mode::Replicated),
        Err("refresh_ahead takes a fraction strictly between 0 and 1")
    );
}

#[test]
fn validate_merge_window_accepts_zero_regardless_of_the_resolver() {
    assert!(validate_merge_window(Duration::ZERO, false));
    assert!(validate_merge_window(Duration::ZERO, true));
}

#[test]
fn validate_merge_window_requires_a_merging_resolver_once_nonzero() {
    assert!(!validate_merge_window(Duration::from_millis(1), false));
    assert!(validate_merge_window(Duration::from_millis(1), true));
}

#[tokio::test]
async fn merge_coalesce_window_rejects_a_non_merging_resolver() {
    let cluster = Cluster::builder("cache-it-merge-window-rejects-non-merging")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let err = cluster
        .cache::<u32, u32>("counters")
        .merge_coalesce_window(Duration::from_millis(5))
        .open()
        .await
        .expect_err("the default LwwResolver does not merge, so a nonzero window is rejected");
    assert!(matches!(
        err,
        CacheError::MergeWindowRequiresMergingResolver { .. }
    ));

    cluster.shutdown().await;
}

#[tokio::test]
async fn prefold_enabled_defaults_to_true_and_the_toggle_reaches_the_shard() {
    let cluster = Cluster::builder("cache-it-prefold-enabled")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let on = cluster
        .cache::<u32, u32>("counters-on")
        .mode(Mode::Local)
        .open()
        .await
        .expect("open succeeds");
    assert!(
        on.shard.prefold_enabled(),
        "pre-fold defaults to on, matching the engine's own default"
    );

    let off = cluster
        .cache::<u32, u32>("counters-off")
        .mode(Mode::Local)
        .prefold_enabled(false)
        .open()
        .await
        .expect("open succeeds");
    assert!(
        !off.shard.prefold_enabled(),
        "CacheBuilder::prefold_enabled(false) reaches the shard's engine"
    );

    on.close().await;
    off.close().await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn cache_builder_capacity_hint_reaches_the_shard() {
    let cluster = Cluster::builder("cache-it-capacity-hint")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let hint = 3_000u64;
    let expected =
        usize::try_from(hint.div_ceil(crate::store::BUCKET_COUNT as u64)).expect("hint fits usize");
    let cache = cluster
        .cache::<u32, u32>("counters")
        .mode(Mode::Local)
        .capacity_hint(hint)
        .open()
        .await
        .expect("open succeeds");
    assert_eq!(
        cache.shard.stripe_capacities(),
        vec![expected; crate::store::BUCKET_COUNT],
        "the hint reached the shard's engine, presizing every stripe"
    );

    cache.close().await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn cache_stripe_capacities_matches_the_shards() {
    let cluster = Cluster::builder("cache-it-cache-stripe-capacities")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let hint = 500u64;
    let cache = cluster
        .cache::<u32, u32>("counters")
        .mode(Mode::Local)
        .capacity_hint(hint)
        .open()
        .await
        .expect("open succeeds");
    assert_eq!(
        cache.stripe_capacities(),
        cache.shard.stripe_capacities(),
        "Cache::stripe_capacities is a plain forward to the shard's own accessor, for a \
         crate outside sundog to reach it without seeing the private `shard` field"
    );

    cache.close().await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn capacity_hint_above_max_capacity_is_clamped_without_a_weigher() {
    let cluster = Cluster::builder("cache-it-capacity-hint-clamped")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let max_capacity = 100u64;
    let hint = 3_000u64;
    let expected = usize::try_from(max_capacity.div_ceil(crate::store::BUCKET_COUNT as u64))
        .expect("clamped hint fits usize");
    let cache = cluster
        .cache::<u32, u32>("counters")
        .mode(Mode::Local)
        .max_capacity(max_capacity)
        .capacity_hint(hint)
        .open()
        .await
        .expect("open succeeds");
    assert_eq!(
        cache.shard.stripe_capacities(),
        vec![expected; crate::store::BUCKET_COUNT],
        "with no weigher, max_capacity bounds entry count, so a hint above it is clamped"
    );

    cache.close().await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn capacity_hint_above_max_capacity_is_kept_as_is_with_a_weigher() {
    let cluster = Cluster::builder("cache-it-capacity-hint-unclamped")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let max_capacity = 100u64;
    let hint = 3_000u64;
    let expected =
        usize::try_from(hint.div_ceil(crate::store::BUCKET_COUNT as u64)).expect("hint fits usize");
    let cache = cluster
        .cache::<u32, u32>("counters")
        .mode(Mode::Local)
        .max_capacity(max_capacity)
        .weigher(|_key: &u32, _value: &u32| 1)
        .capacity_hint(hint)
        .open()
        .await
        .expect("open succeeds");
    assert_eq!(
        cache.shard.stripe_capacities(),
        vec![expected; crate::store::BUCKET_COUNT],
        "with a weigher, max_capacity bounds weight, not entry count, so the hint is kept as is"
    );

    cache.close().await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn capacity_hint_defaults_to_none_and_matches_pre_change_rss() {
    let cluster = Cluster::builder("cache-it-capacity-hint-none")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let cache = cluster
        .cache::<u32, u32>("counters")
        .mode(Mode::Local)
        .open()
        .await
        .expect("open succeeds");
    assert_eq!(
        cache.shard.stripe_capacities(),
        vec![0; crate::store::BUCKET_COUNT],
        "no hint: every stripe starts at zero allocation, matching an engine with no \
         capacity_hint at all"
    );

    cache.close().await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn writer_id_pairs_the_node_with_the_cluster_incarnation() {
    let cluster = Cluster::builder("cache-it-writer-id")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");
    let cache = cluster
        .cache::<u32, PnCounter>("counters")
        .resolver(Arc::new(PnCounterResolver))
        .open()
        .await
        .expect("open succeeds");

    let writer = cache.writer_id();
    assert_eq!(
        writer,
        WriterId::new(cluster.node_id(), cluster.local_incarnation()),
        "Cache::writer_id pairs this node's id with the cluster's own \
         membership incarnation, the same pairing WriterId::new takes"
    );

    cache.close().await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn writer_id_is_stable_across_clones_and_repeated_calls() {
    let cluster = Cluster::builder("cache-it-writer-id-stable")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");
    let cache = cluster
        .cache::<u32, PnCounter>("counters")
        .resolver(Arc::new(PnCounterResolver))
        .open()
        .await
        .expect("open succeeds");
    let clone = cache.clone();

    assert_eq!(
        cache.writer_id(),
        cache.writer_id(),
        "two calls on the same handle agree"
    );
    assert_eq!(
        cache.writer_id(),
        clone.writer_id(),
        "a clone shares the same underlying cluster membership, so it \
         reports the identical writer identity, never a fresh one"
    );

    cache.close().await;
    cluster.shutdown().await;
}

/// The end-to-end reason `Cache::writer_id` exists: a value merged under
/// it round-trips through the exact merging path CRDT compaction relies
/// on, exercised at the `Cache` layer rather than only unit-tested on
/// `WriterId` itself.
#[tokio::test]
async fn a_value_merged_under_writer_id_reads_back_correctly() {
    let cluster = Cluster::builder("cache-it-writer-id-merge")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");
    let cache = cluster
        .cache::<u32, PnCounter>("counters")
        .resolver(Arc::new(PnCounterResolver))
        .open()
        .await
        .expect("open succeeds");

    cache
        .merge(1, PnCounter::local_delta(cache.writer_id(), 5))
        .await
        .expect("merge");
    assert_eq!(cache.get(&1).await.map(|c| c.value()), Some(5));

    cache.close().await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn merge_with_a_zero_window_applies_immediately() {
    let cluster = Cluster::builder("cache-it-merge-zero-window")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");
    let cache = cluster
        .cache::<u32, PnCounter>("counters")
        .resolver(Arc::new(PnCounterResolver))
        .open()
        .await
        .expect("open succeeds");
    let writer = cache.writer_id();

    cache
        .merge(1, PnCounter::local_delta(writer, 1))
        .await
        .expect("merge");
    assert_eq!(
        cache.get(&1).await.map(|c| c.value()),
        Some(1),
        "a zero coalesce window (the default) applies every merge call at once, \
         like insert"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn merge_within_a_window_coalesces_into_one_apply_and_one_event() {
    let cluster = Cluster::builder("cache-it-merge-coalesce-window")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");
    let window = Duration::from_millis(150);
    let cache = cluster
        .cache::<u32, PnCounter>("counters")
        .resolver(Arc::new(PnCounterResolver))
        .merge_coalesce_window(window)
        .open()
        .await
        .expect("open succeeds");
    let writer = cache.writer_id();
    let mut events = cache.events();

    cache
        .merge(1, PnCounter::local_delta(writer, 1))
        .await
        .expect("merge 1");
    cache
        .merge(1, PnCounter::local_delta(writer, 2))
        .await
        .expect("merge 2");
    cache
        .merge(1, PnCounter::local_delta(writer, 3))
        .await
        .expect("merge 3");

    assert_eq!(
        cache.get(&1).await,
        None,
        "a pending coalesced fold is invisible to get until its window flushes"
    );
    assert!(
        events.try_recv().is_err(),
        "nothing applies, so nothing publishes, before the window elapses"
    );

    tokio::time::sleep(window * 3).await;

    assert_eq!(
        cache.get(&1).await.map(|c| c.value()),
        Some(3),
        "the coalesced fold of all three calls lands as one write once the window \
         elapses"
    );
    match events
        .recv()
        .await
        .expect("exactly one event for the whole window")
    {
        Event::Created { key, .. } => assert_eq!(key, 1),
        other => panic!("expected Event::Created for the window's one apply, got {other:?}"),
    }
    assert!(
        events.try_recv().is_err(),
        "three merge calls in one window publish exactly one event, not three"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn close_flushes_a_pending_coalesced_merge() {
    let cluster = Cluster::builder("cache-it-merge-close-flush")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");
    let cache = cluster
        .cache::<u32, PnCounter>("counters")
        .resolver(Arc::new(PnCounterResolver))
        .merge_coalesce_window(Duration::from_secs(60))
        .open()
        .await
        .expect("open succeeds");
    let survivor = cache.clone();
    let writer = cache.writer_id();

    cache
        .merge(1, PnCounter::local_delta(writer, 7))
        .await
        .expect("merge");
    assert_eq!(
        survivor.get(&1).await,
        None,
        "still pending: the 60s window has not elapsed"
    );

    cache.close().await;

    assert_eq!(
        survivor.get(&1).await.map(|c| c.value()),
        Some(7),
        "close flushes whatever was still pending, regardless of the window"
    );

    cluster.shutdown().await;
}

/// A free loopback UDP port: bound and released, so a node can be
/// seeded with a peer that does not exist yet.
fn free_udp_port() -> u16 {
    std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
        .expect("bind a loopback udp socket")
        .local_addr()
        .expect("bound socket has an address")
        .port()
}

#[tokio::test]
async fn distributed_cache_rejects_a_tombstone_ttl_inside_the_release_window() {
    let name = "distributed-release-window-guard";
    let config = loopback_config().with(|c| {
        c.ae_interval = Duration::from_secs(3);
        c.distributed_disown_grace_rounds = 3;
        c.tombstone_ttl = Duration::from_secs(15);
    });
    let cluster = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(config)
        .build()
        .await
        .expect("the cluster builds; the rule binds distributed caches only");

    let err = cluster
        .cache::<u32, String>(name)
        .mode(Mode::distributed())
        .open()
        .await
        .expect_err("a distributed cache under a short retention is rejected");
    assert!(
        matches!(
            &err,
            CacheError::TombstoneTtlInsideReleaseWindow { cache, tombstone_ttl, window }
                if cache == name
                    && *tombstone_ttl == Duration::from_secs(15)
                    && *window == Duration::from_secs(24)
        ),
        "{err:?}"
    );

    let replicated = cluster
        .cache::<u32, String>(name)
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("a replicated cache is not bound by the release window");
    replicated.close().await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn distributed_cache_opened_before_any_peer_is_known_pulls_its_buckets_once_one_appears() {
    // `a` is seeded with the port `b` binds later, so `b` opens its
    // cache before anyone has gossiped to it: its first view is the
    // self-only one, owning every bucket with no co-owner to pull from.
    // Anti-entropy is effectively off, so only the warm-up pull can
    // bring `a`'s entries over. A timed-out pull retries only after
    // `ae_interval`, so the budget covers a whole-space pull of all
    // 65,536 parts, about 2 s in a debug build, on a slow runner.
    let name = "distributed-late-peer";
    let mut config = loopback_config();
    config.ae_interval = Duration::from_secs(3600);
    config.state_transfer_budget = Duration::from_secs(15);
    config.tombstone_ttl = config.bucket_release_window();
    config.gossip_interval = Duration::from_millis(200);
    // The released port can be taken by a test running alongside before
    // `b` binds it; then the pair is torn down and set up on a new one.
    let (a, cache_a, b) = 'setup: {
        for _ in 0..5 {
            let port_b = free_udp_port();
            let a = Cluster::builder(name)
                .seeds([SocketAddr::from((Ipv4Addr::LOCALHOST, port_b))])
                .config(config.clone())
                .build()
                .await
                .expect("node a builds");
            let cache_a = a
                .cache::<u32, String>(name)
                .mode(Mode::distributed())
                .open()
                .await
                .expect("a opens alone");
            assert!(
                !a.is_warm(&SmolStr::new(name)),
                "a cache with no co-owner in sight is not warm yet"
            );
            for key in 0..200u32 {
                cache_a
                    .insert(key, format!("v{key}"))
                    .await
                    .expect("a owns every bucket while alone");
            }
            let mut config_b = config.clone();
            config_b.gossip_bind_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port_b));
            match Cluster::builder(name)
                .seeds(std::iter::empty())
                .config(config_b)
                .build()
                .await
            {
                Ok(b) => break 'setup (a, cache_a, b),
                Err(error) => {
                    eprintln!("port {port_b} was taken before b bound it ({error}); retrying");
                    cache_a.close().await;
                    a.shutdown().await;
                }
            }
        }
        panic!("five pre-picked ports were all taken before b could bind one");
    };
    let cache_b = b
        .cache::<u32, String>(name)
        .mode(Mode::distributed())
        .open()
        .await
        .expect("b opens before it knows a");

    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let mut held = 0;
            for key in 0..200u32 {
                if cache_b.get(&key).await.as_deref() == Some(format!("v{key}").as_str()) {
                    held += 1;
                }
            }
            if held == 200 && b.is_warm(&SmolStr::new(name)) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("b pulls every bucket it owns from a once gossip shows a, and is warm");

    cache_b.close().await;
    cache_a.close().await;
    b.shutdown().await;
    a.shutdown().await;
}

/// Polls until every key in `0..total` is held by exactly the two
/// owners every node's view agrees on, or panics after 30 seconds with
/// a histogram of keys by holder count.
async fn wait_until_settled_on_agreed_owners(nodes: &[(NodeId, &Cache<u32, String>)], total: u32) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let mut by_holders = [0u32; 4];
        let mut settled = 0u32;
        for key in 0..total {
            let expected = format!("v{key}");
            let mut owners: Vec<Vec<NodeId>> = Vec::new();
            let mut holders: Vec<NodeId> = Vec::new();
            for (node, cache) in nodes {
                let mut view_owners = cache.owners_of(&key);
                view_owners.sort_unstable();
                owners.push(view_owners);
                if cache.get(&key).await.as_deref() == Some(expected.as_str()) {
                    holders.push(*node);
                }
            }
            holders.sort_unstable();
            by_holders[holders.len()] += 1;
            if owners.iter().all(|o| *o == owners[0])
                && owners[0].len() == 2
                && holders == owners[0]
            {
                settled += 1;
            }
        }
        if settled == total {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "every key settles on exactly its two agreed owners with nothing lost; settled {settled} of {total}, keys by holder count [0, 1, 2, 3]: {by_holders:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn two_nodes_joining_at_once_lose_no_bucket_the_origin_releases() {
    // `a` holds everything alone. `b` and `c` learn of each other and of
    // `a` before either opens the cache, so both first views already
    // place about a third of the buckets on {b, c}: a pull from the
    // other joiner finds nothing there. `a`'s release hands those
    // buckets off before dropping them.
    let name = "distributed-double-join";
    let a = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node a builds");
    let cache_a = a
        .cache::<u32, String>(name)
        .mode(Mode::distributed())
        .open()
        .await
        .expect("a opens alone");
    let total = 300u32;
    for key in 0..total {
        cache_a
            .insert(key, format!("v{key}"))
            .await
            .expect("a owns every bucket while alone");
    }
    let seed = a.local_gossip_addr();
    let b = Cluster::builder(name)
        .seeds([seed])
        .config(loopback_config())
        .build()
        .await
        .expect("node b builds");
    let c = Cluster::builder(name)
        .seeds([seed])
        .config(loopback_config())
        .build()
        .await
        .expect("node c builds");
    wait_for_peer_count(&b, 2).await;
    wait_for_peer_count(&c, 2).await;
    let (cache_b, cache_c) = tokio::join!(
        b.cache::<u32, String>(name)
            .mode(Mode::distributed())
            .open(),
        c.cache::<u32, String>(name)
            .mode(Mode::distributed())
            .open(),
    );
    let cache_b = cache_b.expect("b opens");
    let cache_c = cache_c.expect("c opens");

    // Past the disown grace (three anti-entropy intervals) plus the
    // hand-off, every key is held by exactly the two owners every view
    // agrees on: nothing lost, nothing lingering on the origin.
    let nodes = [
        (a.node_id(), &cache_a),
        (b.node_id(), &cache_b),
        (c.node_id(), &cache_c),
    ];
    let caches = [&cache_a, &cache_b, &cache_c];
    wait_until_settled_on_agreed_owners(&nodes, total).await;
    for key in 0..total {
        for cache in caches {
            assert_eq!(
                cache
                    .fetch(&key)
                    .await
                    .expect("fetch reaches an owner")
                    .as_deref(),
                Some(format!("v{key}").as_str()),
                "key {key} is fetchable from every node"
            );
        }
    }

    cache_c.close().await;
    cache_b.close().await;
    cache_a.close().await;
    c.shutdown().await;
    b.shutdown().await;
    a.shutdown().await;
}

#[tokio::test]
async fn forwarded_writes_reach_their_owners_when_the_writer_shuts_down_at_once() {
    // `a` forwards thousands of writes for buckets it does not own and
    // shuts down in the same breath: the fan-out task finishes the
    // batch, the mesh flushes it, and every key lands on the survivors,
    // which own everything between them once `a` is gone.
    let ((a, cache_a), (b, cache_b), (c, cache_c), _) =
        three_node_distributed("distributed-forward-flush", "forward-flush").await;
    let keys: Vec<u32> = (0..1_000_000u32)
        .filter(|key| !cache_a.owners_of(key).contains(&a.node_id()))
        .take(3_000)
        .collect();
    cache_a
        .insert_many(keys.iter().map(|&key| (key, format!("v{key}"))))
        .await
        .expect("insert_many forwards");
    cache_a.close().await;
    a.shutdown().await;

    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let mut landed = 0usize;
            for key in &keys {
                let expected = format!("v{key}");
                if cache_b.get(key).await.as_deref() == Some(expected.as_str())
                    && cache_c.get(key).await.as_deref() == Some(expected.as_str())
                {
                    landed += 1;
                }
            }
            if landed == keys.len() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("every forwarded write lands on both survivors");

    cache_c.close().await;
    cache_b.close().await;
    c.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn a_forwarded_write_after_close_fails_with_closed_instead_of_vanishing() {
    let ((a, cache_a), (b, cache_b), (c, cache_c), key_a_does_not_own) =
        three_node_distributed("distributed-write-after-close", "write-after-close").await;
    let owned_by_a = (0..1_000_000u32)
        .find(|key| cache_a.owners_of(key).contains(&a.node_id()))
        .expect("a owns some bucket");
    let late_handle = cache_a.clone();
    cache_a.close().await;
    assert!(
        matches!(
            late_handle
                .insert(key_a_does_not_own, "late".to_string())
                .await,
            Err(CacheError::Closed { .. })
        ),
        "a forwarded write after close is refused"
    );
    assert!(matches!(
        late_handle.remove(&key_a_does_not_own).await,
        Err(CacheError::Closed { .. })
    ));
    late_handle
        .insert(owned_by_a, "detached".to_string())
        .await
        .expect("a write this node applies itself still lands, detached");
    assert_eq!(
        late_handle.get(&owned_by_a).await.as_deref(),
        Some("detached")
    );

    cache_c.close().await;
    cache_b.close().await;
    c.shutdown().await;
    b.shutdown().await;
    a.shutdown().await;
}

/// Three nodes under `config`, each with `name` open as a
/// `Mode::distributed()` cache and every peer count settled.
async fn three_node_distributed_with(
    name: &'static str,
    config: ClusterConfig,
) -> (
    (Cluster, Cache<u32, String>),
    (Cluster, Cache<u32, String>),
    (Cluster, Cache<u32, String>),
) {
    let a = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(config.clone())
        .build()
        .await
        .expect("node a builds");
    let cache_a = a
        .cache::<u32, String>(name)
        .mode(Mode::distributed())
        .open()
        .await
        .expect("a opens alone");
    let seed = a.local_gossip_addr();
    let join = |peer_count: usize| {
        let config = config.clone();
        async move {
            let cluster = Cluster::builder(name)
                .seeds([seed])
                .config(config)
                .build()
                .await
                .expect("node builds");
            wait_for_peer_count(&cluster, peer_count).await;
            let cache = cluster
                .cache::<u32, String>(name)
                .mode(Mode::distributed())
                .open()
                .await
                .expect("node opens");
            (cluster, cache)
        }
    };
    let (b, cache_b) = join(1).await;
    wait_for_peer_count(&a, 1).await;
    let (c, cache_c) = join(2).await;
    wait_for_peer_count(&a, 2).await;
    wait_for_peer_count(&b, 2).await;
    ((a, cache_a), (b, cache_b), (c, cache_c))
}

/// A `WireRecord` for `key`/`value` as `node` would write it now.
fn wire_record_from(key: u32, value: &str, node: NodeId) -> crate::wire::WireRecord {
    crate::wire::WireRecord {
        key: encode_key(&key).expect("u32 encodes"),
        value: Some(bytes::Bytes::from(
            postcard::to_stdvec(&value.to_string()).expect("string encodes"),
        )),
        ver: crate::hlc::Hlc {
            wall_ms: u64::try_from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("clock past the epoch")
                    .as_millis(),
            )
            .expect("fits"),
            logical: 0,
            node,
        },
        expires_at_ms: None,
    }
}

/// A write fanned out under a stale ownership view reaches only the
/// owners that view names. The receiver's inbound loop notices the
/// batch's view hash is not its own and re-forwards the records to
/// their owners under its view, so the owner the writer missed gets
/// the record without waiting for an anti-entropy round to pair the
/// two; a batch already re-forwarded once travels no further.
#[tokio::test]
async fn a_forward_batch_under_a_stale_view_reaches_the_owner_the_writer_missed() {
    use crate::net::OutFrame;
    use crate::wire::{Msg, WireRecord};

    // Anti-entropy is effectively off: it would repair the missing
    // copy and hide whether the re-forward did.
    let name = "distributed-stale-forward";
    let mut config = loopback_config();
    config.ae_interval = Duration::from_secs(3600);
    config.tombstone_ttl = config.bucket_release_window();
    let ((a, cache_a), (b, cache_b), (c, cache_c)) =
        three_node_distributed_with(name, config).await;

    // Two keys whose owners are exactly `b` and `c`, under every node's
    // settled view.
    let mut keys = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let found: Vec<u32> = (0..100_000u32)
                .filter(|key| {
                    let owners = cache_a.owners_of(key);
                    owners.len() == 2
                        && !owners.contains(&a.node_id())
                        && cache_b.owners_of(key) == owners
                        && cache_c.owners_of(key) == owners
                })
                .take(2)
                .collect();
            if found.len() == 2 {
                return found;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("every view settles on b and c owning some bucket");
    let hop_one_key = keys.pop().expect("two keys");
    let key = keys.pop().expect("two keys");
    let cache_name = SmolStr::new(name);
    let stale_hash = cache_b
        .shard
        .ownership_view_hash()
        .expect("b is distributed")
        ^ 1;
    let record = |key: u32, value: &str| wire_record_from(key, value, a.node_id());
    let forward = |hops: u8, rec: WireRecord| {
        let frame = OutFrame::new(Msg::ForwardBatch {
            cache: cache_name.clone(),
            view_hash: stale_hash,
            hops,
            recs: vec![rec],
        })
        .expect("encodes");
        let mesh = a.mesh().clone();
        let to = b.node_id();
        async move {
            mesh.send_frames_awaiting(to, vec![frame]).await;
        }
    };

    // As if `a` still believed `b` and a departed node owned the
    // bucket: the batch reaches `b` alone, under a hash `b` does not
    // recognise, and `b` passes it on to `c`.
    forward(0, record(key, "stale-routed")).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while cache_c.get(&key).await.as_deref() != Some("stale-routed") {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("b re-forwards the batch to c, the owner a's stale view missed");
    assert_eq!(cache_b.get(&key).await.as_deref(), Some("stale-routed"));

    // The same batch already one hop past its writer stops at `b`.
    forward(1, record(hop_one_key, "hop-one")).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while cache_b.get(&hop_one_key).await.as_deref() != Some("hop-one") {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("b applies the hop-one batch");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        cache_c.get(&hop_one_key).await,
        None,
        "a batch at the hop cap is applied where it lands and re-forwarded no further"
    );

    cache_c.close().await;
    cache_b.close().await;
    cache_a.close().await;
    c.shutdown().await;
    b.shutdown().await;
    a.shutdown().await;
}

#[tokio::test]
async fn distributed_cache_rejects_owners_below_two() {
    let cluster = Cluster::builder("cache-it-distributed-owners-below-two")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let err = cluster
        .cache::<u32, String>("scratch")
        .mode(Mode::Distributed {
            owners: std::num::NonZeroU8::new(1).expect("nonzero"),
        })
        .open()
        .await
        .expect_err("a single owner is rejected");

    assert!(
        matches!(err, CacheError::TooFewOwners { .. }),
        "expected TooFewOwners, got {err:?}"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn distributed_cache_open_attaches_an_ownership_view() {
    let cluster = Cluster::builder("cache-it-distributed-attaches-view")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let cache = cluster
        .cache::<u32, String>("scratch")
        .mode(Mode::Distributed {
            owners: std::num::NonZeroU8::new(2).expect("nonzero"),
        })
        .open()
        .await
        .expect("open succeeds");

    assert!(
        ShardOps::ownership_view(&*cache.shard).is_some(),
        "opening a Distributed cache attaches a real ownership view before it's shared"
    );

    cache.close().await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn distributed_cache_rejects_tti() {
    let cluster = Cluster::builder("cache-it-distributed-rejects-tti")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let err = cluster
        .cache::<u32, String>("scratch")
        .mode(Mode::Distributed {
            owners: std::num::NonZeroU8::new(2).expect("nonzero"),
        })
        .tti(Duration::from_secs(30))
        .open()
        .await
        .expect_err("tti is rejected for Distributed just like Replicated");

    assert!(
        matches!(err, CacheError::ReplicatedWithLocalEviction { .. }),
        "expected ReplicatedWithLocalEviction, got {err:?}"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn replicated_node_at_its_memory_ceiling_refuses_local_writes_and_takes_its_peers() {
    let name = "cache-it-memory-ceiling";
    let a = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("a builds");
    let b = Cluster::builder(name)
        .seeds([a.local_gossip_addr()])
        .config(loopback_config())
        .build()
        .await
        .expect("b builds");
    wait_for_peer_count(&a, 1).await;
    let cache_a = a
        .cache::<u32, String>("sessions")
        .mode(Mode::Replicated)
        .max_resident_bytes(1)
        .open()
        .await
        .expect("a ceiling needs no spill tier in Replicated mode");
    let cache_b = b
        .cache::<u32, String>("sessions")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("b opens");

    assert_eq!(cache_a.resident_bytes(), Some(0));
    assert_eq!(
        cache_b.resident_bytes(),
        None,
        "an uncapped cache counts nothing"
    );
    cache_a
        .insert(1, "a".into())
        .await
        .expect("under the ceiling");
    assert!(cache_a.resident_bytes() > Some(0));
    assert!(matches!(
        cache_a.insert(2, "a".into()).await,
        Err(CacheError::OverMemoryCeiling { ceiling: 1, .. })
    ));
    let loaded = cache_a
        .get_or_load(&9, async |_key: &u32| -> Result<String, std::io::Error> {
            Ok("loaded".into())
        })
        .await
        .expect("the loader succeeds");
    assert_eq!(loaded, "loaded");
    assert_eq!(
        cache_a.get(&9).await,
        None,
        "a fill at the ceiling is not stored"
    );

    cache_b.insert(3, "from b".into()).await.expect("insert");
    wait_until(
        Duration::from_secs(10),
        "b's write lands on a past a's ceiling",
        async || cache_a.get(&3).await.is_some(),
    )
    .await;
    wait_until(
        Duration::from_secs(10),
        "a's accepted write reaches b",
        async || cache_b.get(&1).await.is_some(),
    )
    .await;
    assert_eq!(
        cache_b.get(&2).await,
        None,
        "a refused write never replicates"
    );

    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn distributed_cache_opens_with_a_memory_ceiling_and_no_spill() {
    let cluster = Cluster::builder("cache-it-distributed-memory-ceiling")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");
    let cache = cluster
        .cache::<u32, String>("scratch")
        .mode(Mode::Distributed {
            owners: std::num::NonZeroU8::new(2).expect("nonzero"),
        })
        .max_resident_bytes(1)
        .open()
        .await
        .expect("a ceiling evicts nothing, so Distributed accepts it without spill");
    cache
        .insert(1, "a".into())
        .await
        .expect("under the ceiling");
    assert!(matches!(
        cache.insert(2, "b".into()).await,
        Err(CacheError::OverMemoryCeiling { .. })
    ));
    cluster.shutdown().await;
}

#[tokio::test]
async fn distributed_cache_rejects_finite_max_capacity_without_spill() {
    let cluster = Cluster::builder("cache-it-distributed-no-spill-max-capacity")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let err = cluster
        .cache::<u32, String>("scratch")
        .mode(Mode::Distributed {
            owners: std::num::NonZeroU8::new(2).expect("nonzero"),
        })
        .max_capacity(100)
        .open()
        .await
        .expect_err("Distributed + finite max_capacity + no spill is rejected");

    assert!(
        matches!(err, CacheError::ReplicatedWithLocalEviction { .. }),
        "expected ReplicatedWithLocalEviction, got {err:?}"
    );

    cluster.shutdown().await;
}

#[cfg(feature = "spill")]
mod spill_gate {
    use super::*;

    /// A directory path under the OS temp dir, unique to this test
    /// process and call. Never created on disk: config validation in
    /// `open()` is pure arithmetic and never touches the filesystem.
    fn fresh_spill_dir(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "sundog-spill-gate-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock is after the unix epoch")
                .as_nanos()
        ))
    }

    #[tokio::test]
    async fn open_returns_spill_unavailable_when_dir_is_a_regular_file() {
        let cluster = Cluster::builder("cache-it-spill-dir-is-a-file")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");

        let path = fresh_spill_dir("dir-is-a-file");
        std::fs::write(&path, b"not a directory")
            .expect("create a regular file at the spill dir path");

        let cfg = SpillConfig::new(&path, 1 << 20).region_bytes(4096);
        let err = cluster
            .cache::<u32, String>("scratch")
            .spill(cfg)
            .open()
            .await
            .expect_err("a spill dir that is actually a regular file cannot be created under");
        assert!(
            matches!(err, CacheError::SpillUnavailable { .. }),
            "expected SpillUnavailable, got {err:?}"
        );

        let _ = std::fs::remove_file(&path);
        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn open_rejects_a_spill_config_whose_capacity_is_under_two_regions() {
        let cluster = Cluster::builder("cache-it-spill-under-two-regions")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");

        // Default region_bytes is 64 MiB; one byte under two regions.
        let cfg = SpillConfig::new(fresh_spill_dir("under-two-regions"), 128 * 1024 * 1024 - 1);
        let err = cluster
            .cache::<u32, String>("scratch")
            .spill(cfg)
            .open()
            .await
            .expect_err("a capacity under two regions is rejected");

        assert!(
            matches!(err, CacheError::InvalidSpillConfig { .. }),
            "expected InvalidSpillConfig, got {err:?}"
        );

        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn open_rejects_replicated_with_max_capacity_and_no_spill() {
        let cluster = Cluster::builder("cache-it-spill-no-spill-max-capacity")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");

        let err = cluster
            .cache::<u32, String>("scratch")
            .mode(Mode::Replicated)
            .max_capacity(100)
            .open()
            .await
            .expect_err("Replicated + finite max_capacity + no spill is rejected");

        assert!(
            matches!(err, CacheError::ReplicatedWithLocalEviction { .. }),
            "expected ReplicatedWithLocalEviction, got {err:?}"
        );

        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn open_accepts_replicated_with_max_capacity_once_spill_is_configured() {
        let cluster = Cluster::builder("cache-it-spill-relaxes-gate")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");

        let cfg = SpillConfig::new(fresh_spill_dir("relaxes-gate"), 256 * 1024 * 1024);
        let cache = cluster
            .cache::<u32, String>("scratch")
            .mode(Mode::Replicated)
            .max_capacity(100)
            .spill(cfg)
            .open()
            .await
            .expect("Replicated + finite max_capacity is accepted once spill is configured");

        cache.close().await;
        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn distributed_cache_opens_with_spill_and_a_capacity() {
        let cluster = Cluster::builder("cache-it-distributed-spill-relaxes-gate")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");

        let cfg = SpillConfig::new(
            fresh_spill_dir("distributed-relaxes-gate"),
            256 * 1024 * 1024,
        );
        let cache = cluster
            .cache::<u32, String>("scratch")
            .mode(Mode::Distributed {
                owners: std::num::NonZeroU8::new(2).expect("nonzero"),
            })
            .max_capacity(100)
            .spill(cfg)
            .open()
            .await
            .expect("Distributed + finite max_capacity is accepted once spill is configured");

        cache.close().await;
        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn open_still_rejects_replicated_with_tti_even_with_spill() {
        let cluster = Cluster::builder("cache-it-spill-tti-still-rejected")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");

        let cfg = SpillConfig::new(fresh_spill_dir("tti-still-rejected"), 256 * 1024 * 1024);
        let err = cluster
            .cache::<u32, String>("scratch")
            .mode(Mode::Replicated)
            .tti(Duration::from_secs(30))
            .spill(cfg)
            .open()
            .await
            .expect_err("tti stays an unconditional error for Replicated, spill or not");

        assert!(
            matches!(err, CacheError::ReplicatedWithLocalEviction { .. }),
            "expected ReplicatedWithLocalEviction, got {err:?}"
        );

        cluster.shutdown().await;
    }

    /// Polls `cond` until it returns `true` or `timeout` elapses,
    /// returning the final result either way. Never a fixed sleep.
    async fn poll_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if cond() {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return cond();
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Opens a cache under `mode` with a tiny `max_capacity` and a real
    /// spill tier, inserts past that capacity, then waits up to a
    /// bound for eviction to spill one of the two keys. Spilling is
    /// observed via `get_sync` reading a miss, `Shard::get_sync`'s
    /// documented contract for a currently-spilled entry. It then
    /// reads the key back through `get` and asserts the disk
    /// round-trip.
    async fn spill_composes_with_max_capacity(mode: Mode, label: &str) {
        let cluster = Cluster::builder("cache-it-spill-compose")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");

        let cfg = SpillConfig::new(fresh_spill_dir(label), 1 << 20).region_bytes(4096);
        let cache = cluster
            .cache::<u32, String>("scratch")
            .mode(mode)
            .max_capacity(1)
            .spill(cfg)
            .open()
            .await
            .expect("opens with spill composing with max_capacity");

        cache.insert(1, "one".to_string()).await.expect("insert 1");
        cache.insert(2, "two".to_string()).await.expect("insert 2");

        assert!(
            poll_until(Duration::from_secs(5), || {
                cache.get_sync(&1).is_none() || cache.get_sync(&2).is_none()
            })
            .await,
            "eviction spills exactly one of the two keys under a tiny max_capacity"
        );
        let (spilled_key, expected) = if cache.get_sync(&1).is_none() {
            (1u32, "one".to_string())
        } else {
            (2u32, "two".to_string())
        };
        assert_eq!(
            cache.get(&spilled_key).await,
            Some(expected),
            "the spilled key reads back correctly through the disk tier"
        );

        cache.close().await;
        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn spill_composes_with_max_capacity_under_mode_local() {
        spill_composes_with_max_capacity(Mode::Local, "compose-local").await;
    }

    #[tokio::test]
    async fn spill_composes_with_max_capacity_under_mode_invalidation() {
        spill_composes_with_max_capacity(Mode::Invalidation, "compose-invalidation").await;
    }

    /// A `Mode::Local` cache whose flush queue is full answers a read of
    /// every spilled key from disk, with `get` or with `get_or_load`,
    /// and deletes nothing: the read's promotion finds no room, because
    /// the tier refuses every victim and a read never deletes one, so
    /// the keys stay spilled, `entry_count` is unchanged and the loader
    /// is never called.
    #[tokio::test]
    async fn a_local_cache_reads_spilled_keys_behind_a_full_queue_and_keeps_every_entry() {
        full_queue_reads_keep_every_entry("cache-it-spill-full-queue-reads", false).await;
    }

    /// [`a_local_cache_reads_spilled_keys_behind_a_full_queue_and_keeps_every_entry`]
    /// through `Cache::get_or_load`.
    #[tokio::test]
    async fn a_local_cache_get_or_load_behind_a_full_queue_keeps_every_entry_and_skips_the_loader()
    {
        full_queue_reads_keep_every_entry("cache-it-spill-full-queue-loads", true).await;
    }

    async fn full_queue_reads_keep_every_entry(name: &str, load: bool) {
        const KEYS: u32 = 24;
        const CAP: u64 = 4;
        let cluster = Cluster::builder(name)
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");
        let dir = fresh_spill_dir(name);
        let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
        let cache = cluster
            .cache::<u32, String>("scratch")
            .mode(Mode::Local)
            .max_capacity(CAP)
            .spill(cfg)
            .open()
            .await
            .expect("opens with a spill tier");
        cache
            .insert_many((0..KEYS).map(|k| (k, format!("value-{k}"))))
            .await
            .expect("insert_many");
        let resident = || (0..KEYS).filter(|k| cache.get_sync(k).is_some()).count();
        assert!(
            poll_until(Duration::from_secs(5), || resident() as u64 <= CAP).await,
            "the writes spill all but the cap"
        );
        let spilled: Vec<u32> = (0..KEYS).filter(|k| cache.get_sync(k).is_none()).collect();
        assert_eq!(spilled.len() as u64, u64::from(KEYS) - CAP);
        assert_eq!(cache.entry_count().await, u64::from(KEYS));

        let hold = cache.shard.debug_hold_flush_queue().await;
        let loads = std::sync::atomic::AtomicUsize::new(0);
        for &key in &spilled {
            let value = if load {
                cache
                    .get_or_load(&key, async |_| {
                        loads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        Ok::<_, std::io::Error>(String::new())
                    })
                    .await
                    .ok()
            } else {
                cache.get(&key).await
            };
            assert_eq!(
                value,
                Some(format!("value-{key}")),
                "key {key} answers from disk"
            );
        }
        assert_eq!(
            loads.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "no read calls the loader"
        );
        assert_eq!(
            cache.entry_count().await,
            u64::from(KEYS),
            "no read deleted an entry"
        );
        assert_eq!(
            (0..KEYS).filter(|k| cache.get_sync(k).is_none()).count(),
            spilled.len(),
            "the reads left every spilled key on disk"
        );
        drop(hold);

        cache.close().await;
        cluster.shutdown().await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `Shard::attach_spill` runs only for the `open()` that wins the
    /// registry reservation, so the losing `open()` never touches the
    /// first cache's region files.
    #[tokio::test]
    async fn reopening_the_same_spill_cache_name_returns_already_open_without_corrupting_it() {
        let cluster = Cluster::builder("cache-it-spill-reopen-guard")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");

        let dir = fresh_spill_dir("reopen-guard");
        let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
        let cache = cluster
            .cache::<u32, String>("scratch")
            .max_capacity(1)
            .spill(cfg)
            .open()
            .await
            .expect("first open succeeds");

        cache.insert(1, "one".to_string()).await.expect("insert 1");
        cache.insert(2, "two".to_string()).await.expect("insert 2");
        assert!(
            poll_until(Duration::from_secs(5), || {
                cache.get_sync(&1).is_none() || cache.get_sync(&2).is_none()
            })
            .await,
            "eviction spills exactly one of the two keys under a tiny max_capacity"
        );
        let (spilled_key, expected) = if cache.get_sync(&1).is_none() {
            (1u32, "one".to_string())
        } else {
            (2u32, "two".to_string())
        };

        // Same name and SpillConfig, same directory. The registry
        // reservation must reject this second open before
        // `Shard::attach_spill` ever wipes and preallocates the first
        // cache's region files.
        let cfg2 = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
        let err = cluster
            .cache::<u32, String>("scratch")
            .max_capacity(1)
            .spill(cfg2)
            .open()
            .await
            .expect_err("the name is already open");
        assert!(
            matches!(err, CacheError::AlreadyOpen { .. }),
            "expected AlreadyOpen, got {err:?}"
        );

        assert_eq!(
            cache.get(&spilled_key).await,
            Some(expected),
            "the first cache's spilled value must still be readable after the rejected \
             second open"
        );

        cache.close().await;
        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn close_stops_the_tier_and_a_surviving_clone_evicts_by_deleting() {
        let cluster = Cluster::builder("cache-it-spill-close-stops-tier")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");

        let cfg = SpillConfig::new(fresh_spill_dir("close-stops-tier"), 1 << 20).region_bytes(4096);
        let cache = cluster
            .cache::<u32, String>("scratch")
            .max_capacity(1)
            .spill(cfg)
            .open()
            .await
            .expect("opens with spill");
        let surviving = cache.clone();

        assert!(!surviving.shard.spill_tier_closed(), "the tier starts open");
        cache.close().await;
        assert!(
            surviving.shard.spill_tier_closed(),
            "Cache::close stops the engine's attached spill tier, visible through any \
             surviving clone since they share one Shard"
        );

        // A surviving clone keeps working locally. With the tier
        // closed, capacity eviction on it deletes rather than spills,
        // since there is no live tier to hand a victim to. A spilled
        // entry still counts as live, so `entry_count` distinguishes a
        // delete from a spill.
        surviving
            .insert(1, "one".to_string())
            .await
            .expect("insert 1");
        surviving
            .insert(2, "two".to_string())
            .await
            .expect("insert 2");
        assert_eq!(
            surviving.entry_count().await,
            1,
            "eviction deletes rather than spills once the tier is closed"
        );

        cluster.shutdown().await;
    }

    /// Cache-level counterpart to the engine- and shard-level spilled-merge
    /// tests: a counter spills between two colliding `Cache::merge` calls,
    /// and the second still folds against the spilled side's real bytes
    /// instead of dropping them.
    #[tokio::test]
    async fn merging_into_a_spilled_counter_folds_instead_of_dropping_a_side() {
        let cluster = Cluster::builder("cache-it-merge-spilled")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");

        let cfg = SpillConfig::new(fresh_spill_dir("merge-spilled"), 1 << 20).region_bytes(4096);
        let cache = cluster
            .cache::<u32, PnCounter>("counters")
            .resolver(Arc::new(PnCounterResolver))
            .max_capacity(1)
            // Always exceeds the cap of 1 on its own, so the single
            // counter key spills right after the first merge instead of
            // needing a second key to create eviction pressure.
            .weigher(|_k: &u32, _v: &PnCounter| 2)
            .spill(cfg)
            .open()
            .await
            .expect("opens with spill");

        let writer_a = WriterId::new(NodeId::from(11), 1);
        let writer_b = WriterId::new(NodeId::from(22), 1);

        cache
            .merge(1, PnCounter::local_delta(writer_a, 3))
            .await
            .expect("first increment");

        assert!(
            poll_until(Duration::from_secs(5), || cache.get_sync(&1).is_none()).await,
            "the over-weight entry spills once eviction runs"
        );

        cache
            .merge(1, PnCounter::local_delta(writer_b, 4))
            .await
            .expect("second increment, colliding with the spilled record");

        assert_eq!(
            cache.get(&1).await.map(|c| c.value()),
            Some(7),
            "merging into a spilled record folds both sides' contributions instead of \
             dropping the one that was on disk"
        );

        cache.close().await;
        cluster.shutdown().await;
    }
}

//! Tests of what `open` does before it serves: waiting for first peers, trusting a sole owner and
//! concurrent joins.

use super::*;

/// A loopback UDP address nothing listens on, for forcing a wait to time out.
pub(super) async fn dead_gossip_addr() -> SocketAddr {
    let socket = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind an ephemeral loopback udp port to reserve a dead gossip address");
    socket
        .local_addr()
        .expect("a freshly bound udp socket reports a local address")
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
/// owners every node's view agrees on, and both owners hold the key's
/// part warm and verified, or panics after 30 seconds with a histogram
/// of keys by holder count.
///
/// Holding the value is not enough for a read to be served. An owner
/// whose part is unverified declines a peer's fetch even for a record it
/// holds, and one whose part is cold declines a record it lacks, so the
/// wait covers the marks as well. Only the owners count: a node that is
/// not an owner of the part keeps whatever mark an earlier view left on it
/// (see `ResidencySet::mark_serving`), and it serves nothing of that part.
async fn wait_until_settled_on_agreed_owners(nodes: &[(NodeId, &Cache<u32, String>)], total: u32) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let mut by_holders = [0u32; 4];
        let mut settled = 0u32;
        for key in 0..total {
            let expected = format!("v{key}");
            let part = part_of(key);
            let mut owners: Vec<Vec<NodeId>> = Vec::new();
            let mut holders: Vec<NodeId> = Vec::new();
            let mut owners_warm = true;
            for (node, cache) in nodes {
                let mut view_owners = cache.owners_of(&key);
                view_owners.sort_unstable();
                if view_owners.contains(node) {
                    owners_warm &=
                        !cache.shard.is_cold_part(part) && !cache.shard.is_unverified_part(part);
                }
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
                && owners_warm
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
    // Every node stays up and answers: the three share this test's one
    // runtime, whose ownership-view and rebalance planning over 65,536
    // parts in a debug build holds it for hundreds of milliseconds at a
    // time, longer on a busy runner. A fetch then outlasts the default
    // 750ms fetch timeout, and a failure detector that drops a node too busy
    // to gossip fails a fetch to it at once ("not a known mesh member") and
    // moves the views.
    let config = || {
        loopback_config().with(|c| {
            c.fetch_timeout = Duration::from_secs(5);
            c.phi_threshold = 1_000.0;
        })
    };
    let a = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(config())
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
        .config(config())
        .build()
        .await
        .expect("node b builds");
    let c = Cluster::builder(name)
        .seeds([seed])
        .config(config())
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

//! Replication-cost baseline (plan §11 layer 3 shape: in-process, real
//! `Static`-discovery loopback nodes, public API only) — the honest "before"
//! numbers an optimization phase is judged against. Not a correctness
//! suite: nothing here asserts a performance bound, it only measures and
//! prints.
//!
//! Gated on `SUNDOG_BENCH=1` (checked first thing in every test, an
//! `eprintln!` and early return otherwise, mirroring `tests/containers.rs`)
//! rather than `#[ignore]`, so a plain `cargo test --workspace` run still
//! compiles and "passes" this binary without spending the wall-clock a
//! 100k-entry replication run costs.
//!
//! Both scenarios below share the process-wide wire counters in
//! `sundog::net` (`frames_sent_total`/`bytes_sent_total`), so run this
//! binary single-threaded or the two scenarios' deltas bleed into each
//! other:
//!
//! ```text
//! SUNDOG_BENCH=1 cargo test --release -p sundog --test replication_bench \
//!     -- --test-threads=1 --nocapture
//! ```
//!
//! Each `BENCH` line is one `key=value`-per-metric record, meant to be
//! `grep`bable across runs rather than parsed as structured output.

mod common;

use std::net::{Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};

use sundog::{Cluster, ClusterConfig, Mode};

fn bench_enabled() -> bool {
    std::env::var("SUNDOG_BENCH").as_deref() == Ok("1")
}

/// Mirrors `tests/tls.rs`'s own copy: the only way, from outside the crate,
/// to learn a concrete gossip address before the node that will bind it
/// exists — every node in a mutually-`Static`-seeded group needs the
/// others' addresses up front.
async fn reserve_gossip_addr() -> SocketAddr {
    let socket = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind an ephemeral loopback udp port to reserve a gossip address");
    socket
        .local_addr()
        .expect("a just-bound udp socket reports a local address")
}

fn node_config(gossip_bind_addr: SocketAddr) -> ClusterConfig {
    common::fast_config().with(|c| {
        c.gossip_bind_addr = gossip_bind_addr;
    })
}

/// Builds `n` real, loopback-`Static`-discovery clusters that all seed each
/// other, and waits until every one reports `n - 1` live peers.
async fn peer_group(cluster_name: &str, n: usize) -> Vec<Cluster> {
    let mut gossip_addrs = Vec::with_capacity(n);
    for _ in 0..n {
        gossip_addrs.push(reserve_gossip_addr().await);
    }

    let mut clusters = Vec::with_capacity(n);
    for (i, &addr) in gossip_addrs.iter().enumerate() {
        let seeds = gossip_addrs
            .iter()
            .enumerate()
            .filter(|&(j, _)| j != i)
            .map(|(_, &seed)| seed);
        let cluster = Cluster::builder(cluster_name)
            .seeds(seeds)
            .config(node_config(addr))
            .build()
            .await
            .unwrap_or_else(|error| panic!("node {i} builds: {error}"));
        clusters.push(cluster);
    }

    for cluster in &clusters {
        common::wait_for_peer_count(cluster, n - 1, Duration::from_secs(30)).await;
    }
    clusters
}

fn bench_value(i: u32) -> String {
    format!("value-{i:07}-the-quick-brown-fox")
}

/// Plan §4/§6 in the shape that matters for a bulk-write baseline: 100k
/// sequential local inserts on one node of a 3-node `Replicated` cluster,
/// timing (a) the insert loop itself — local apply plus a non-blocking
/// fan-out push, per `net::PeerHandle::send`'s docs — and (b) wall time
/// until the other two nodes' local copies both fully catch up, which in
/// the steady default config (`outbox_capacity` 8,192 against 100k writes)
/// leans heavily on anti-entropy repair rather than the live fan-out path,
/// exactly as the drop-policy semantics in plan §6 predict.
#[tokio::test]
async fn bulk_insert_replication() {
    const ENTRIES: u32 = 100_000;

    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }

    let clusters = peer_group("bench-bulk-insert", 3).await;
    let [cluster_a, cluster_b, cluster_c] = <[Cluster; 3]>::try_from(clusters)
        .unwrap_or_else(|_| panic!("peer_group(_, 3) returns exactly 3 clusters"));

    let (cache_a, cache_b, cache_c) = tokio::join!(
        cluster_a
            .cache::<u32, String>("bulk")
            .mode(Mode::Replicated)
            .open(),
        cluster_b
            .cache::<u32, String>("bulk")
            .mode(Mode::Replicated)
            .open(),
        cluster_c
            .cache::<u32, String>("bulk")
            .mode(Mode::Replicated)
            .open(),
    );
    let cache_a = cache_a.expect("a opens");
    let cache_b = cache_b.expect("b opens");
    let cache_c = cache_c.expect("c opens");

    let frames_before = sundog::net::frames_sent_total();
    let bytes_before = sundog::net::bytes_sent_total();

    let started = Instant::now();
    for i in 0..ENTRIES {
        cache_a
            .insert(i, bench_value(i))
            .await
            .expect("insert succeeds");
    }
    let insert_elapsed = started.elapsed();

    common::eventually(Duration::from_secs(300), || async {
        cache_b.entry_count().await == u64::from(ENTRIES)
            && cache_c.entry_count().await == u64::from(ENTRIES)
    })
    .await;
    let converge_elapsed = started.elapsed();

    let frames = sundog::net::frames_sent_total() - frames_before;
    let bytes = sundog::net::bytes_sent_total() - bytes_before;

    println!(
        "BENCH bulk_insert_replication entries={ENTRIES} insert_secs={:.3} \
         converge_secs={:.3} frames_sent_total={frames} bytes_sent_total={bytes}",
        insert_elapsed.as_secs_f64(),
        converge_elapsed.as_secs_f64(),
    );

    for cluster in [cluster_a, cluster_b, cluster_c] {
        cluster.shutdown().await;
    }
}

/// Per-write overhead signal at a scale small enough to run every commit:
/// 5,000 sequential inserts on one node of a live 2-node `Replicated`
/// cluster, wall time only — no convergence wait, since the point is the
/// caller-observed cost of `insert` itself under real (if idle) fan-out.
#[tokio::test]
async fn steady_small_writes() {
    const ENTRIES: u32 = 5_000;

    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }

    let clusters = peer_group("bench-steady-writes", 2).await;
    let [cluster_a, cluster_b] = <[Cluster; 2]>::try_from(clusters)
        .unwrap_or_else(|_| panic!("peer_group(_, 2) returns exactly 2 clusters"));

    let (cache_a, cache_b) = tokio::join!(
        cluster_a
            .cache::<u32, String>("steady")
            .mode(Mode::Replicated)
            .open(),
        cluster_b
            .cache::<u32, String>("steady")
            .mode(Mode::Replicated)
            .open(),
    );
    let cache_a = cache_a.expect("a opens");
    let _cache_b = cache_b.expect("b opens");

    let frames_before = sundog::net::frames_sent_total();
    let bytes_before = sundog::net::bytes_sent_total();

    let started = Instant::now();
    for i in 0..ENTRIES {
        cache_a
            .insert(i, bench_value(i))
            .await
            .expect("insert succeeds");
    }
    let elapsed = started.elapsed();

    let frames = sundog::net::frames_sent_total() - frames_before;
    let bytes = sundog::net::bytes_sent_total() - bytes_before;

    let per_write_micros = elapsed.as_secs_f64() * 1_000_000.0 / f64::from(ENTRIES);
    println!(
        "BENCH steady_small_writes entries={ENTRIES} wall_secs={:.3} \
         per_write_micros={per_write_micros:.1} frames_sent_total={frames} bytes_sent_total={bytes}",
        elapsed.as_secs_f64(),
    );

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

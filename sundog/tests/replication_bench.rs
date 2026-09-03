//! Replication-cost baseline — in-process, real `Static`-discovery loopback
//! nodes, public API only — the honest "before" numbers an optimization
//! phase is judged against. Not a correctness
//! suite: nothing here asserts a performance bound, it only measures and
//! prints.
//!
//! Gated on `SUNDOG_BENCH=1` (checked first thing in every test, an
//! `eprintln!` and early return otherwise, mirroring `tests/containers.rs`)
//! rather than `#[ignore]`, so a plain `cargo test --workspace` run still
//! compiles and "passes" this binary without spending the wall-clock a
//! 100k-entry replication run costs.
//!
//! All scenarios below share the process-wide wire counters in
//! `sundog::net` (`frames_sent_total`/`bytes_sent_total`), so run this
//! binary single-threaded or the scenarios' deltas bleed into each other:
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
        .expect("a freshly bound udp socket reports a local address")
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

/// The shape that matters for a bulk-write baseline: 100k sequential local
/// inserts on one node of a 3-node `Replicated` cluster,
/// timing (a) the insert loop itself — local apply plus a non-blocking
/// fan-out push, per `net::PeerHandle::send`'s docs — and (b) wall time
/// until the other two nodes' local copies both fully catch up, which in
/// the steady default config (`outbox_capacity` 8,192 against 100k writes)
/// leans heavily on anti-entropy repair rather than the live fan-out path,
/// exactly as the crate's drop-policy semantics predict.
///
/// Multi-threaded runtime: an embedded cache's host application overwhelmingly
/// runs one, and on a current-thread runtime the insert loop, both peers'
/// coalescers, the TCP writers, and any concurrent anti-entropy rounds all
/// serialize onto one core — charging the whole replication engine's CPU time
/// to the caller's insert loop and making `insert_secs` a fiction.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

/// [`bulk_insert_replication`]'s counterpart through the batch API: the same
/// 100k entries on the same 3-node cluster, but handed to `insert_many` as
/// one call — the path a bulk loader should actually use. Amortizes
/// per-call overhead and lets the store apply under far fewer lock
/// acquisitions, so the spread between this scenario's `insert_secs` and the
/// sequential loop's is the price of writing bulk data one `insert` at a
/// time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_insert_many_replication() {
    const ENTRIES: u32 = 100_000;

    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }

    let clusters = peer_group("bench-bulk-insert-many", 3).await;
    let [cluster_a, cluster_b, cluster_c] = <[Cluster; 3]>::try_from(clusters)
        .unwrap_or_else(|_| panic!("peer_group(_, 3) returns exactly 3 clusters"));

    let (cache_a, cache_b, cache_c) = tokio::join!(
        cluster_a
            .cache::<u32, String>("bulk-many")
            .mode(Mode::Replicated)
            .open(),
        cluster_b
            .cache::<u32, String>("bulk-many")
            .mode(Mode::Replicated)
            .open(),
        cluster_c
            .cache::<u32, String>("bulk-many")
            .mode(Mode::Replicated)
            .open(),
    );
    let cache_a = cache_a.expect("a opens");
    let cache_b = cache_b.expect("b opens");
    let cache_c = cache_c.expect("c opens");

    let frames_before = sundog::net::frames_sent_total();
    let bytes_before = sundog::net::bytes_sent_total();

    let started = Instant::now();
    cache_a
        .insert_many((0..ENTRIES).map(|i| (i, bench_value(i))))
        .await
        .expect("insert_many succeeds");
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
        "BENCH bulk_insert_many_replication entries={ENTRIES} insert_secs={:.3} \
         converge_secs={:.3} frames_sent_total={frames} bytes_sent_total={bytes}",
        insert_elapsed.as_secs_f64(),
        converge_elapsed.as_secs_f64(),
    );

    for cluster in [cluster_a, cluster_b, cluster_c] {
        cluster.shutdown().await;
    }
}

/// The read path, which no other scenario measures: 1M `get` calls against a
/// warm 100k-entry cache on a live 2-node `Replicated` cluster. Reads are
/// pure local lookups — no network hop, by design — so `per_read_nanos` is
/// the number that justifies "embedded": it should sit orders of magnitude
/// under any networked cache's round trip. The cost measured is the honest
/// public-API cost, value clone and `async` machinery included.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_read_latency() {
    const ENTRIES: u32 = 100_000;
    const READS: u32 = 1_000_000;

    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }

    let clusters = peer_group("bench-local-reads", 2).await;
    let [cluster_a, cluster_b] = <[Cluster; 2]>::try_from(clusters)
        .unwrap_or_else(|_| panic!("peer_group(_, 2) returns exactly 2 clusters"));

    let (cache_a, cache_b) = tokio::join!(
        cluster_a
            .cache::<u32, String>("reads")
            .mode(Mode::Replicated)
            .open(),
        cluster_b
            .cache::<u32, String>("reads")
            .mode(Mode::Replicated)
            .open(),
    );
    let cache_a = cache_a.expect("a opens");
    let _cache_b = cache_b.expect("b opens");

    cache_a
        .insert_many((0..ENTRIES).map(|i| (i, bench_value(i))))
        .await
        .expect("warm fill succeeds");

    let started = Instant::now();
    let mut hits = 0u32;
    for i in 0..READS {
        if cache_a.get(&(i % ENTRIES)).await.is_some() {
            hits += 1;
        }
    }
    let elapsed = started.elapsed();
    assert_eq!(hits, READS, "every read targets a present key");

    let per_read_nanos = elapsed.as_secs_f64() * 1_000_000_000.0 / f64::from(READS);
    println!(
        "BENCH local_read_latency entries={ENTRIES} reads={READS} wall_secs={:.3} \
         per_read_nanos={per_read_nanos:.0}",
        elapsed.as_secs_f64(),
    );

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

/// [`local_read_latency`]'s quiet-path control: the same 1M reads against
/// the same warm 100k entries, but on a [`Mode::Local`] cache — no
/// anti-entropy loop attaches to it, so nothing iterates the store behind
/// the reader's back. The spread between this number and
/// [`local_read_latency`]'s is the ambient cost of *being* a live
/// `Replicated` member (background digest scans and bucket iteration
/// sharing the machine), not of the read path itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_mode_read_latency() {
    const ENTRIES: u32 = 100_000;
    const READS: u32 = 1_000_000;

    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }

    let clusters = peer_group("bench-local-mode-reads", 2).await;
    let [cluster_a, cluster_b] = <[Cluster; 2]>::try_from(clusters)
        .unwrap_or_else(|_| panic!("peer_group(_, 2) returns exactly 2 clusters"));

    let cache_a = cluster_a
        .cache::<u32, String>("local-reads")
        .mode(Mode::Local)
        .open()
        .await
        .expect("a opens");

    cache_a
        .insert_many((0..ENTRIES).map(|i| (i, bench_value(i))))
        .await
        .expect("warm fill succeeds");

    let started = Instant::now();
    let mut hits = 0u32;
    for i in 0..READS {
        if cache_a.get(&(i % ENTRIES)).await.is_some() {
            hits += 1;
        }
    }
    let elapsed = started.elapsed();
    assert_eq!(hits, READS, "every read targets a present key");

    let per_read_nanos = elapsed.as_secs_f64() * 1_000_000_000.0 / f64::from(READS);
    println!(
        "BENCH local_mode_read_latency entries={ENTRIES} reads={READS} wall_secs={:.3} \
         per_read_nanos={per_read_nanos:.0}",
        elapsed.as_secs_f64(),
    );

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

/// Per-write overhead signal at a scale small enough to run every commit:
/// 5,000 sequential inserts on one node of a live 2-node `Replicated`
/// cluster, wall time only — no convergence wait, since the point is the
/// caller-observed cost of `insert` itself under real (if idle) fan-out.
/// Multi-threaded runtime for the same representativeness reason as
/// [`bulk_insert_replication`]: the fan-out machinery must not share the
/// insert loop's core.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

/// [`steady_small_writes`]'s single-worker baseline, but driven by
/// `WRITERS` concurrent workers writing disjoint keys to the same node's
/// cache handle at once — the shape the apply-serialization lock's
/// per-key-bucket striping exists for, which `steady_small_writes`'
/// single-worker loop can never exercise regardless of how cheap a single
/// `insert` call is. A single global apply lock would serialize every
/// concurrent writer here no matter which keys they touch; striping lets
/// writers whose keys land in different stripes proceed independently, so
/// this scenario's `per_write_micros` is the number a striped lock
/// visibly improves, unlike `steady_small_writes`' or
/// `bulk_insert_replication`'s (both single-worker, sequential inserts).
///
/// Multi-threaded runtime, unlike every other test in this binary — the
/// point is real cross-core parallelism putting actual lock contention on
/// the apply-serialization stripe(s), which a single-threaded runtime's
/// cooperative scheduling can't produce.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_small_writes() {
    const ENTRIES: u32 = 5_000;
    const WRITERS: u32 = 16;

    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }

    let clusters = peer_group("bench-concurrent-writes", 2).await;
    let [cluster_a, cluster_b] = <[Cluster; 2]>::try_from(clusters)
        .unwrap_or_else(|_| panic!("peer_group(_, 2) returns exactly 2 clusters"));

    let (cache_a, cache_b) = tokio::join!(
        cluster_a
            .cache::<u32, String>("concurrent")
            .mode(Mode::Replicated)
            .open(),
        cluster_b
            .cache::<u32, String>("concurrent")
            .mode(Mode::Replicated)
            .open(),
    );
    let cache_a = cache_a.expect("a opens");
    let _cache_b = cache_b.expect("b opens");

    let frames_before = sundog::net::frames_sent_total();
    let bytes_before = sundog::net::bytes_sent_total();

    let writers = usize::try_from(WRITERS).expect("WRITERS fits in usize");
    let started = Instant::now();
    let handles: Vec<_> = (0..WRITERS)
        .map(|writer| {
            let cache_a = cache_a.clone();
            tokio::spawn(async move {
                for i in (writer..ENTRIES).step_by(writers) {
                    cache_a
                        .insert(i, bench_value(i))
                        .await
                        .expect("insert succeeds");
                }
            })
        })
        .collect();
    for handle in handles {
        handle.await.expect("writer worker did not panic");
    }
    let elapsed = started.elapsed();

    let frames = sundog::net::frames_sent_total() - frames_before;
    let bytes = sundog::net::bytes_sent_total() - bytes_before;

    let per_write_micros = elapsed.as_secs_f64() * 1_000_000.0 / f64::from(ENTRIES);
    println!(
        "BENCH concurrent_small_writes entries={ENTRIES} writers={WRITERS} wall_secs={:.3} \
         per_write_micros={per_write_micros:.1} frames_sent_total={frames} bytes_sent_total={bytes}",
        elapsed.as_secs_f64(),
    );

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

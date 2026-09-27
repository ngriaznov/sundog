//! Reads every key from every live node, continuously, while `Mode::Distributed`
//! ownership moves under real clusters: a node joining while an owner
//! crashes mid-pull, a node joining and leaving inside the disown grace, a
//! graceful leave, and a join followed by a crash. The oracle knows each
//! key's value or its deletion. A read may answer
//! `CacheError::FetchUnavailable`, never a wrong value or a miss for a key a
//! live node holds. A deleted key's old value is stale, allowed only while
//! the live nodes' views disagree about the key's owners: a node that gave
//! the key's part up still holds its copy through the disown grace and
//! answers a reader whose view has not caught up. Once every view agrees, a
//! deleted key never reads back.
//!
//! The churn schedule's delays come from a seed, printed on failure and
//! overridden with `SUNDOG_ORACLE_SEED`; `SUNDOG_ORACLE_RUNS` runs that many
//! seeds in a row.

mod common;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rand::{RngExt as _, SeedableRng as _, rngs::StdRng};
use sundog::{Cache, CacheError, Cluster, ClusterConfig, Mode, NodeId};

use common::fast_config;

const CLUSTER: &str = "it-churn-oracle";
const CACHE: &str = "oracle";
const KEYS: u32 = 400;

/// What a read of `key` must answer once written: `None` for the keys the
/// setup deletes, every fifth.
fn expected(key: u32) -> Option<String> {
    (!key.is_multiple_of(5)).then(|| format!("v{key}"))
}

/// A disown grace far longer than the whole schedule, so a displaced owner
/// still holds what it gave up throughout: every key always has a live
/// holder, and any miss is the cluster's fault, not lost data.
fn config() -> ClusterConfig {
    fast_config().with(|c| {
        c.distributed_disown_grace_rounds = 400;
        c.tombstone_ttl = c.bucket_release_window() + Duration::from_secs(10);
    })
}

struct Member {
    name: &'static str,
    cluster: Cluster,
    cache: Cache<u32, String>,
}

async fn join(name: &'static str, seeds: &[&Member]) -> Member {
    let cluster = Cluster::builder(CLUSTER)
        .seeds(seeds.iter().map(|m| m.cluster.local_gossip_addr()))
        .config(config())
        .build()
        .await
        .unwrap_or_else(|error| panic!("{name} builds: {error}"));
    let cache = cluster
        .cache::<u32, String>(CACHE)
        .mode(Mode::distributed())
        .open()
        .await
        .unwrap_or_else(|error| panic!("{name} opens: {error}"));
    Member {
        name,
        cluster,
        cache,
    }
}

/// Everything the reader found wrong, and how many reads it made.
#[derive(Default)]
struct Findings {
    violations: Mutex<Vec<String>>,
    reads: AtomicU64,
    unavailable: AtomicU64,
    /// Deleted keys read back while the views disagreed: allowed staleness.
    stale: AtomicU64,
}

/// Each live member's view of a key's owners, sorted so two views that
/// name the same owners compare equal whatever order they rank them in.
fn owner_sets(
    members: &[(&'static str, NodeId, Cache<u32, String>)],
    key: u32,
) -> Vec<Vec<NodeId>> {
    members
        .iter()
        .map(|(_, _, cache)| {
            let mut owners = cache.owners_of(&key);
            owners.sort_unstable();
            owners
        })
        .collect()
}

/// Whether the views in `before` and `after`, taken around one read, all
/// name the same owners for its key: no node can have answered from a
/// part it gave up to a reader that still thought it owned it.
fn views_agree(before: &[Vec<NodeId>], after: &[Vec<NodeId>]) -> bool {
    before
        .iter()
        .chain(after)
        .all(|owners| before.first().is_some_and(|first| owners == first))
}

/// Checks one read of `key` from `node` against the oracle. `settled` is
/// whether every live view named the same owners around the read.
fn judge(
    findings: &Findings,
    node: &str,
    key: u32,
    read: &Result<Option<String>, CacheError>,
    settled: bool,
) {
    findings.reads.fetch_add(1, Ordering::Relaxed);
    let verdict = match (read, expected(key)) {
        (Ok(got), want) if *got == want => return,
        (Err(CacheError::FetchUnavailable { .. }), _) => {
            findings.unavailable.fetch_add(1, Ordering::Relaxed);
            return;
        }
        (Ok(Some(_)), None) if !settled => {
            findings.stale.fetch_add(1, Ordering::Relaxed);
            return;
        }
        (Ok(Some(_)), None) => "a deleted key came back once every view agreed",
        (Ok(None), Some(_)) => "a miss for a key a live node holds",
        (Ok(_), _) => "a wrong value",
        (Err(_), _) => "an unexpected error",
    };
    let mut violations = findings.violations.lock().expect("unpoisoned");
    if violations.len() < 50 {
        violations.push(format!("{node}/k{key}: {verdict}: {read:?}"));
    }
}

/// A live member as the reader and `settle` see it.
type Live = Mutex<Vec<(&'static str, NodeId, Cache<u32, String>)>>;

/// How far [`settle_to`] waits.
#[derive(Clone, Copy, PartialEq)]
enum Settled {
    /// Every read answers as the oracle expects, though an owner may still
    /// be receiving its copy and answering through another owner.
    Reads,
    /// Reads, and every current owner of a present key also holds it
    /// locally.
    Placed,
}

/// [`settle_to`] with [`Settled::Placed`].
async fn settle(live: &Live, what: &str) {
    settle_to(live, what, Settled::Placed).await;
}

/// Waits until every live member answers every key as the oracle expects
/// and, for [`Settled::Placed`], every current owner of a present key holds
/// it locally. On timeout, panics with what each member still gets wrong,
/// tallied.
async fn settle_to(live: &Live, what: &str, level: Settled) {
    let members = live.lock().expect("unpoisoned").clone();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let mut wrong: Vec<String> = Vec::new();
        for (name, node, cache) in &members {
            for key in 0..KEYS {
                if level == Settled::Placed
                    && expected(key).is_some()
                    && members[0].2.owners_of(&key).contains(node)
                    && cache.get(&key).await != expected(key)
                {
                    wrong.push(format!("{name}:owner-lacks-copy"));
                }
                let read = cache.fetch(&key).await;
                if read.as_ref().ok() != Some(&expected(key)) {
                    let kind = match &read {
                        Ok(Some(_)) => "value",
                        Ok(None) => "miss",
                        Err(CacheError::FetchUnavailable { .. }) => "unavailable",
                        Err(_) => "error",
                    };
                    wrong.push(format!("{name}:{kind}"));
                }
            }
        }
        if wrong.is_empty() {
            eprintln!("settled: {what}");
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            let mut tally: std::collections::BTreeMap<String, usize> =
                std::collections::BTreeMap::new();
            for entry in wrong {
                *tally.entry(entry).or_default() += 1;
            }
            panic!("never settled after {what}: wrong reads per node and kind {tally:?}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn run(seed: u64) {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut pause = |max_ms: u64| Duration::from_millis(rng.random_range(0..=max_ms));

    let node_a = join("a", &[]).await;
    let node_b = join("b", &[&node_a]).await;
    let node_c = join("c", &[&node_a, &node_b]).await;
    common::wait_for_peer_count(&node_c.cluster, 2, Duration::from_secs(15)).await;
    for key in 0..KEYS {
        node_a
            .cache
            .insert(key, format!("v{key}"))
            .await
            .expect("insert");
    }
    for key in (0..KEYS).filter(|k| expected(*k).is_none()) {
        node_a.cache.remove(&key).await.expect("remove");
    }

    let live = Arc::new(Mutex::new(vec![
        ("a", node_a.cluster.node_id(), node_a.cache.clone()),
        ("b", node_b.cluster.node_id(), node_b.cache.clone()),
        ("c", node_c.cluster.node_id(), node_c.cache.clone()),
    ]));
    settle(&live, "initial fill").await;

    let findings = Arc::new(Findings::default());
    let stop = Arc::new(AtomicBool::new(false));
    let reader = tokio::spawn({
        let (live, findings, stop) = (Arc::clone(&live), Arc::clone(&findings), Arc::clone(&stop));
        async move {
            while !stop.load(Ordering::Relaxed) {
                let members = live.lock().expect("unpoisoned").clone();
                for (name, _, cache) in &members {
                    for key in 0..KEYS {
                        let before = owner_sets(&members, key);
                        let read = cache.fetch(&key).await;
                        let after = owner_sets(&members, key);
                        judge(&findings, name, key, &read, views_agree(&before, &after));
                    }
                    // A fetch served from a local copy completes without
                    // awaiting anything, so once every member holds every
                    // key this loop would never hand its worker back.
                    tokio::task::yield_now().await;
                }
            }
        }
    });
    let drop_from_reader = |name: &str| {
        live.lock()
            .expect("unpoisoned")
            .retain(|(n, _, _)| *n != name);
    };
    let add_to_reader = |m: &Member| {
        live.lock()
            .expect("unpoisoned")
            .push((m.name, m.cluster.node_id(), m.cache.clone()));
    };

    // A joins-while-an-owner-crashes window: d starts pulling its share,
    // and a, which owns much of it, dies partway.
    let node_d = join("d", &[&node_b, &node_c]).await;
    add_to_reader(&node_d);
    tokio::time::sleep(pause(800)).await;
    drop_from_reader("a");
    node_a.cluster.crash().await;
    settle(&live, "d joined, a crashed").await;

    // A join and a leave inside the disown grace: the parts e took come
    // back to the nodes that gave them up.
    let node_e = join("e", &[&node_b, &node_d]).await;
    add_to_reader(&node_e);
    tokio::time::sleep(pause(1_500)).await;
    drop_from_reader("e");
    node_e.cluster.shutdown().await;
    // Only reads settle before b leaves: an owner may still be receiving its
    // copy while b answers for it, so b's leave has to hand its copies off.
    settle_to(&live, "e joined and left", Settled::Reads).await;

    // A graceful leave.
    tokio::time::sleep(pause(500)).await;
    drop_from_reader("b");
    node_b.cluster.shutdown().await;
    settle(&live, "b left").await;

    // A join, then a crash of another owner while it pulls.
    let node_f = join("f", &[&node_c, &node_d]).await;
    add_to_reader(&node_f);
    tokio::time::sleep(pause(800)).await;
    drop_from_reader("c");
    node_c.cluster.crash().await;
    settle(&live, "f joined, c crashed").await;

    stop.store(true, Ordering::Relaxed);
    reader.await.expect("reader finishes");
    let violations = findings.violations.lock().expect("unpoisoned").clone();
    eprintln!(
        "seed {seed}: {} reads, {} unavailable, {} stale while views moved, {} violations",
        findings.reads.load(Ordering::Relaxed),
        findings.unavailable.load(Ordering::Relaxed),
        findings.stale.load(Ordering::Relaxed),
        violations.len()
    );
    assert!(
        violations.is_empty(),
        "reads broke the churn contract (replay with SUNDOG_ORACLE_SEED={seed}):\n{}",
        violations.join("\n")
    );

    node_d.cluster.shutdown().await;
    node_f.cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reads_never_miss_resurrect_or_go_stale_while_distributed_ownership_moves() {
    let seed: u64 = std::env::var("SUNDOG_ORACLE_SEED").map_or(0x5EED_0AC1, |raw| {
        raw.parse().expect("SUNDOG_ORACLE_SEED is a u64")
    });
    let runs: u64 = std::env::var("SUNDOG_ORACLE_RUNS")
        .map_or(1, |raw| raw.parse().expect("SUNDOG_ORACLE_RUNS is a u64"));
    for run_index in 0..runs {
        let seed = seed.wrapping_add(run_index);
        eprintln!("churn oracle: seed {seed}");
        run(seed).await;
    }
}

#[test]
fn views_agree_only_when_every_view_names_the_same_owners_before_and_after() {
    let (a, b, c) = (NodeId::from(1), NodeId::from(2), NodeId::from(3));
    let ab = vec![a, b];
    assert!(views_agree(
        &[ab.clone(), ab.clone()],
        &[ab.clone(), ab.clone()]
    ));
    assert!(
        !views_agree(&[ab.clone(), vec![a, c]], &[ab.clone(), ab.clone()]),
        "two members disagree before the read"
    );
    assert!(
        !views_agree(&[ab.clone(), ab.clone()], &[vec![b, c], vec![b, c]]),
        "the views moved during the read, even though they agree after it"
    );
}

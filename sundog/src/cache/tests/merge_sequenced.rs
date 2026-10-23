//! `Cache::merge_sequenced`: the numbers a cache hands to `make`, and an
//! `OrSet` that many tasks add to while another task removes what it read.
//!
//! The cluster tests run on the real clock. Each waits for its cluster and its
//! convergence with a real-time bound, and returns as soon as its condition
//! holds.

use std::collections::BTreeSet;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;
use crate::store::Ttl;
use crate::store::crdt::{OrSet, OrSetResolver};

type SetCache = Cache<u32, OrSet<u32>>;
type SetNodes = Vec<(Cluster, SetCache)>;

/// The key every task of a workload adds to.
const SET_KEY: u32 = 1;

async fn solo_cluster(name: &'static str) -> Cluster {
    Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node builds")
}

#[tokio::test]
async fn the_numbers_start_at_one_rise_by_one_and_come_with_the_caches_writer() {
    let cluster = solo_cluster("cache-it-seq-numbers").await;
    let cache = cluster
        .cache::<u32, u64>("numbers")
        .mode(Mode::Local)
        .open()
        .await
        .expect("opens");
    let handed = Mutex::new(Vec::new());

    for key in [1, 2, 1, 3] {
        cache
            .merge_sequenced(key, |writer, seq| {
                handed.lock().expect("lock").push((writer, seq));
                seq
            })
            .await
            .expect("merge_sequenced");
    }

    let writer = cache.writer_id();
    assert_eq!(
        *handed.lock().expect("lock"),
        [(writer, 1), (writer, 2), (writer, 3), (writer, 4)],
        "one counter per cache, shared by its keys, from 1, beside Cache::writer_id"
    );
    assert_eq!(
        cache.get(&1).await,
        Some(3),
        "the value make built is stored"
    );
    assert_eq!(cache.get(&3).await, Some(4));
    cluster.shutdown().await;
}

#[tokio::test]
async fn a_clone_shares_the_counter_and_another_cache_of_the_cluster_has_its_own() {
    let cluster = solo_cluster("cache-it-seq-per-cache").await;
    let first = cluster
        .cache::<u32, u64>("first")
        .mode(Mode::Local)
        .open()
        .await
        .expect("opens");
    let second = cluster
        .cache::<u32, u64>("second")
        .mode(Mode::Local)
        .open()
        .await
        .expect("opens");
    let clone = first.clone();

    first
        .merge_sequenced(1, |_, seq| seq)
        .await
        .expect("original");
    clone.merge_sequenced(1, |_, seq| seq).await.expect("clone");
    second
        .merge_sequenced(1, |_, seq| seq)
        .await
        .expect("other cache");

    assert_eq!(first.get(&1).await, Some(2), "the clone drew number 2");
    assert_eq!(
        second.get(&1).await,
        Some(1),
        "the other cache of the same cluster counts from 1"
    );
    cluster.shutdown().await;
}

#[tokio::test]
async fn a_restarted_node_numbers_from_one_under_a_new_incarnation() {
    let node = NodeId::from(4_242);
    let mut seen = Vec::new();
    for _ in 0..2 {
        let cluster = Cluster::builder("cache-it-seq-restart")
            .seeds(std::iter::empty())
            .node_id(node)
            .config(loopback_config())
            .build()
            .await
            .expect("node builds");
        let cache = cluster
            .cache::<u32, u64>("numbers")
            .mode(Mode::Local)
            .open()
            .await
            .expect("opens");
        for _ in 0..3 {
            cache
                .merge_sequenced(1, |writer, seq| {
                    if seq == 1 {
                        seen.push(writer);
                    }
                    seq
                })
                .await
                .expect("merge_sequenced");
        }
        assert_eq!(cache.get(&1).await, Some(3));
        cluster.shutdown().await;
        // The incarnation is a millisecond clock reading.
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    assert_eq!(seen.len(), 2, "each run drew number 1 once");
    assert_eq!(seen[0].node(), node);
    assert_eq!(seen[1].node(), node);
    assert!(
        seen[1].incarnation() > seen[0].incarnation(),
        "the restart writes as a new writer: {seen:?}"
    );
}

#[tokio::test]
async fn a_cache_that_closes_and_opens_again_carries_on_its_count() {
    let cluster = solo_cluster("cache-it-seq-reopen-local").await;
    let open = async || {
        cluster
            .cache::<u32, u64>("numbers")
            .mode(Mode::Local)
            .open()
            .await
            .expect("opens")
    };
    let handed = Mutex::new(Vec::new());
    let draw = async |cache: &Cache<u32, u64>| {
        cache
            .merge_sequenced(1, |writer, seq| {
                handed.lock().expect("lock").push((writer, seq));
                seq
            })
            .await
            .expect("merge_sequenced");
    };

    let first = open().await;
    let stale = first.clone();
    draw(&first).await;
    draw(&first).await;
    first.close().await;
    let second = open().await;
    draw(&second).await;
    draw(&stale).await;
    draw(&second).await;

    let writer = second.writer_id();
    assert_eq!(
        *handed.lock().expect("lock"),
        [
            (writer, 1),
            (writer, 2),
            (writer, 3),
            (writer, 4),
            (writer, 5)
        ],
        "a reopened cache, and a clone kept past the close, draw on from where the first open stopped"
    );
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_add_made_after_the_cache_reopens_survives_a_remove_that_observed_the_adds_before_it() {
    let name = "cache-it-seq-reopen";
    let mut nodes = set_nodes(name, Mode::Replicated, 2, Duration::ZERO).await;
    let (cluster_b, cache_b) = nodes.pop().expect("node b");
    let (cluster_a, cache_a) = nodes.pop().expect("node a");
    for element in [10, 20] {
        cache_a
            .merge_sequenced(SET_KEY, |writer, seq| OrSet::add(writer, seq, element))
            .await
            .expect("add");
    }
    wait_until(
        Duration::from_secs(30),
        "node b holds both adds",
        async || members(&cache_b).await == Some(BTreeSet::from([10, 20])),
    )
    .await;
    let observed = cache_b.get(&SET_KEY).await.expect("node b holds the set");
    cache_b
        .merge(SET_KEY, OrSet::remove(&observed, &20))
        .await
        .expect("remove");
    wait_until(
        Duration::from_secs(30),
        "node a holds the set without the removed element",
        async || members(&cache_a).await == Some(BTreeSet::from([10])),
    )
    .await;

    cache_a.close().await;
    let reopened = cluster_a
        .cache::<u32, OrSet<u32>>(name)
        .mode(Mode::Replicated)
        .resolver(Arc::new(OrSetResolver::<u32>::new()))
        .open()
        .await
        .expect("opens again");
    wait_until(
        Duration::from_secs(30),
        "the reopened cache receives the set",
        async || members(&reopened).await == Some(BTreeSet::from([10])),
    )
    .await;
    let drawn = Mutex::new(Vec::new());
    reopened
        .merge_sequenced(SET_KEY, |writer, seq| {
            drawn.lock().expect("lock").push(seq);
            OrSet::add(writer, seq, 30)
        })
        .await
        .expect("add");

    assert_eq!(
        *drawn.lock().expect("lock"),
        [3],
        "the add takes the number after the two the first open drew"
    );
    wait_until(
        Duration::from_secs(30),
        "both nodes hold the acknowledged add",
        async || {
            members(&reopened).await == Some(BTreeSet::from([10, 30]))
                && members(&cache_b).await == Some(BTreeSet::from([10, 30]))
        },
    )
    .await;
    cluster_b.shutdown().await;
    cluster_a.shutdown().await;
}

#[tokio::test]
async fn merge_sequenced_with_ttl_carries_the_calls_lifespan() {
    let cluster = solo_cluster("cache-it-seq-ttl").await;
    let cache = cluster
        .cache::<u32, u64>("numbers")
        .mode(Mode::Local)
        .open()
        .await
        .expect("opens");

    cache
        .merge_sequenced_with_ttl(1, |_, seq| seq, Duration::from_secs(600))
        .await
        .expect("merge_sequenced_with_ttl");
    cache
        .merge_sequenced(2, |_, seq| seq)
        .await
        .expect("merge_sequenced");

    assert!(
        matches!(
            cache.ttl_of(&1),
            Some(Ttl::Remaining(left))
                if left > Duration::from_secs(590) && left <= Duration::from_secs(600)
        ),
        "the call's lifespan applies: {:?}",
        cache.ttl_of(&1)
    );
    assert_eq!(cache.ttl_of(&2), Some(Ttl::Never));
    assert_eq!(cache.get(&2).await, Some(2), "the counter runs across both");
    cluster.shutdown().await;
}

#[tokio::test]
async fn a_value_over_the_frame_cap_is_refused_and_leaves_the_key_unchanged() {
    let cluster = Cluster::builder("cache-it-seq-too-large")
        .seeds(std::iter::empty())
        .config(loopback_config().with(|config| config.max_frame = 16 * 1_024))
        .build()
        .await
        .expect("node builds");
    let cache = cluster
        .cache::<u32, String>("big")
        .mode(Mode::Local)
        .open()
        .await
        .expect("opens");
    cache
        .merge_sequenced(1, |_, seq| format!("small {seq}"))
        .await
        .expect("a small value");

    let refused = cache
        .merge_sequenced(1, |_, _| "x".repeat(64 * 1_024))
        .await
        .expect_err("a value over the cap is refused");

    assert!(matches!(refused, CacheError::ValueTooLarge { .. }));
    assert_eq!(cache.get(&1).await.as_deref(), Some("small 1"));
    cluster.shutdown().await;
}

#[tokio::test]
async fn a_set_built_with_merge_sequenced_adds_and_removes_what_a_read_observed() {
    for window in [Duration::ZERO, Duration::from_millis(20)] {
        let cluster = solo_cluster("cache-it-seq-set").await;
        let cache = cluster
            .cache::<u32, OrSet<String>>("tags")
            .mode(Mode::Local)
            .resolver(Arc::new(OrSetResolver::<String>::new()))
            .merge_coalesce_window(window)
            .open()
            .await
            .expect("opens");
        for tag in ["a", "b", "c"] {
            cache
                .merge_sequenced(1, |writer, seq| OrSet::add(writer, seq, tag.to_string()))
                .await
                .expect("add");
        }
        wait_until(Duration::from_secs(10), "the adds apply", async || {
            cache
                .get(&1)
                .await
                .is_some_and(|set| set.contains(&"c".to_string()))
        })
        .await;
        let observed = cache.get(&1).await.expect("the set is stored");
        cache
            .merge(1, OrSet::remove(&observed, &"b".to_string()))
            .await
            .expect("remove");
        cache
            .merge_sequenced(1, |writer, seq| OrSet::add(writer, seq, "d".to_string()))
            .await
            .expect("a later add");
        wait_until(
            Duration::from_secs(10),
            "the remove and add apply",
            async || {
                cache.get(&1).await.is_some_and(|set| {
                    set.contains(&"d".to_string()) && !set.contains(&"b".to_string())
                })
            },
        )
        .await;

        let members: Vec<String> = cache
            .get(&1)
            .await
            .expect("the set is stored")
            .iter()
            .cloned()
            .collect();
        assert_eq!(members, ["a", "c", "d"], "with window {window:?}");
        cluster.shutdown().await;
    }
}

/// `count` nodes with an `OrSet<u32>` cache `name` open under `mode` and
/// `window`. Returns once every node's ownership view ranks every node.
async fn set_nodes(name: &'static str, mode: Mode, count: usize, window: Duration) -> SetNodes {
    let mut nodes: SetNodes = Vec::new();
    for index in 0..count {
        // A node too busy with the workload to gossip must not be marked
        // down, which would move the views the test waits on.
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
        let cache = cluster
            .cache::<u32, OrSet<u32>>(name)
            .mode(mode)
            .resolver(Arc::new(OrSetResolver::<u32>::new()))
            .merge_coalesce_window(window)
            .open()
            .await
            .expect("opens");
        nodes.push((cluster, cache));
    }
    wait_until(
        Duration::from_secs(30),
        "every node's ownership view ranks every node",
        async || {
            nodes.iter().all(|(_, cache)| {
                cache
                    .shard
                    .ownership_view()
                    .is_none_or(|view| view.eligible().len() == count)
            })
        },
    )
    .await;
    nodes
}

/// Waits until every owner of [`SET_KEY`] holds its part warm, so a read of
/// it gets an answer rather than a decline.
async fn wait_for_warm_set_owners(nodes: &SetNodes) {
    let part = PartId::of_key(&encode_key(&SET_KEY).expect("u32 encodes"));
    let owners = nodes[0].1.owners_of(&SET_KEY);
    wait_until(
        Duration::from_secs(30),
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

async fn shut_down(nodes: SetNodes) {
    for (cluster, _) in nodes.into_iter().rev() {
        cluster.shutdown().await;
    }
}

/// The members of [`SET_KEY`] as `cache` reads them, or `None` when the read
/// fails. A [`Mode::Distributed`] node that does not own the key asks its
/// owners.
async fn members(cache: &SetCache) -> Option<BTreeSet<u32>> {
    let set = cache.fetch(&SET_KEY).await.ok()?;
    Some(set.map_or_else(BTreeSet::new, |set| set.iter().copied().collect()))
}

/// The tasks of one workload and what they did.
struct Workload {
    /// Every element an adder added; each is unique.
    added: BTreeSet<u32>,
    /// Every element a remover removed after it read the element.
    removed: BTreeSet<u32>,
}

/// How long an adder waits between two adds, and a remover between two
/// passes. A real timer lets every worker park, so a task that waits to run
/// again is woken within the interval however busy the machine is. A task
/// that only yields is woken after the driver next polls, which a worker that
/// never idles does not reach for tens of seconds.
const ADD_PACE: Duration = Duration::from_millis(1);
const REMOVE_PACE: Duration = Duration::from_millis(2);

/// Removals a remover makes at least, beyond the adders' last add if it must.
const MIN_REMOVALS: usize = 3;

/// Passes a remover makes after the adders finish while it has fewer than
/// [`MIN_REMOVALS`] removals.
const MAX_CLOSING_PASSES: u32 = 50;

/// Elements added once the removers have stopped, which no remover can have
/// read.
const CLOSING_ADDS: u32 = 5;

/// Runs `adders` tasks that each add `per_task` unique elements to
/// [`SET_KEY`] through `source.merge_sequenced`, while one remover per cache of
/// `removers` reads the set and removes the element with the highest tag it
/// observed, as long as an adder runs. The highest tag is the one a lower
/// sequence number applied after it would fall under. A remover that has
/// removed fewer than [`MIN_REMOVALS`] elements by then makes up to
/// [`MAX_CLOSING_PASSES`] more passes, so the workload removes something.
/// Once the removers have stopped, [`CLOSING_ADDS`] more elements are added,
/// which no remover observed, so the workload also always keeps something.
async fn run_workload(
    source: &SetCache,
    removers: &[SetCache],
    adders: u32,
    per_task: u32,
) -> Workload {
    let running = Arc::new(AtomicUsize::new(usize::try_from(adders).expect("small")));
    let mut adding = Vec::new();
    for task in 0..adders {
        let (cache, running) = (source.clone(), Arc::clone(&running));
        adding.push(tokio::spawn(async move {
            let mut inserted = BTreeSet::new();
            for index in 0..per_task {
                let element = task * 1_000 + index;
                cache
                    .merge_sequenced(SET_KEY, |writer, seq| OrSet::add(writer, seq, element))
                    .await
                    .expect("add");
                inserted.insert(element);
                tokio::time::sleep(ADD_PACE).await;
            }
            running.fetch_sub(1, Ordering::SeqCst);
            inserted
        }));
    }
    let mut removing = Vec::new();
    for remover in removers {
        let (cache, running) = (remover.clone(), Arc::clone(&running));
        removing.push(tokio::spawn(async move {
            let mut deleted = BTreeSet::new();
            let mut closing_passes = 0;
            loop {
                if running.load(Ordering::SeqCst) == 0 {
                    if deleted.len() >= MIN_REMOVALS || closing_passes >= MAX_CLOSING_PASSES {
                        break;
                    }
                    closing_passes += 1;
                }
                if let Ok(Some(observed)) = cache.fetch(&SET_KEY).await
                    && let Some(&element) = observed.iter().last()
                {
                    cache
                        .merge(SET_KEY, OrSet::remove(&observed, &element))
                        .await
                        .expect("remove");
                    deleted.insert(element);
                }
                tokio::time::sleep(REMOVE_PACE).await;
            }
            deleted
        }));
    }
    let mut workload = Workload {
        added: BTreeSet::new(),
        removed: BTreeSet::new(),
    };
    for task in adding {
        workload.added.extend(task.await.expect("adder task"));
    }
    for task in removing {
        workload.removed.extend(task.await.expect("remover task"));
    }
    for index in 0..CLOSING_ADDS {
        let element = 1_000_000 + index;
        source
            .merge_sequenced(SET_KEY, |writer, seq| OrSet::add(writer, seq, element))
            .await
            .expect("closing add");
        workload.added.insert(element);
    }
    workload
}

/// Panics unless every cache in `readers` ends up reading exactly the
/// elements `workload` added and no remover removed: no add that a remove did
/// not observe is lost, and no observed one comes back.
async fn assert_only_unobserved_adds_survive(readers: &[&SetCache], workload: &Workload) {
    let expected: BTreeSet<u32> = workload
        .added
        .difference(&workload.removed)
        .copied()
        .collect();
    assert!(
        !workload.removed.is_empty() && !expected.is_empty(),
        "the workload both removes and keeps elements: {} added, {} removed",
        workload.added.len(),
        workload.removed.len()
    );
    wait_until(
        Duration::from_secs(60),
        "every reader holds exactly the added elements no remove observed",
        async || {
            for reader in readers {
                if members(reader).await.as_ref() != Some(&expected) {
                    return false;
                }
            }
            true
        },
    )
    .await;
    for (index, reader) in readers.iter().enumerate() {
        let held = members(reader).await.expect("the read answers");
        assert_eq!(
            held.difference(&expected).count() + expected.difference(&held).count(),
            0,
            "reader {index}: lost {:?}, resurrected {:?}",
            expected.difference(&held).collect::<Vec<_>>(),
            held.difference(&expected).collect::<Vec<_>>()
        );
    }
}

/// Many tasks add to one key of node 0 of a two-node `Replicated` cache,
/// through `merge_sequenced`, while a remover on each node reads that node's
/// copy and removes the newest element it saw.
async fn replicated_workload(name: &'static str, window: Duration) {
    let nodes = set_nodes(name, Mode::Replicated, 2, window).await;
    let removers = [nodes[0].1.clone(), nodes[1].1.clone()];

    let workload = tokio::time::timeout(
        Duration::from_secs(120),
        run_workload(&nodes[0].1, &removers, 6, 40),
    )
    .await
    .expect("the workload finishes within the bound");

    assert_only_unobserved_adds_survive(&[&nodes[0].1, &nodes[1].1], &workload).await;
    shut_down(nodes).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_adds_made_with_merge_sequenced_survive_a_remove_that_did_not_observe_them() {
    replicated_workload("cache-it-seq-replicated", Duration::ZERO).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_adds_folded_in_a_window_survive_a_remove_that_did_not_observe_them() {
    replicated_workload("cache-it-seq-replicated-window", Duration::from_millis(5)).await;
}

/// The same workload on the one node of a three-node `Distributed` cache
/// that does not own the key: its adds and its remover's removes are forwards
/// to the owners.
async fn non_owner_workload(name: &'static str, window: Duration) {
    let nodes = set_nodes(name, Mode::distributed(), 3, window).await;
    wait_for_warm_set_owners(&nodes).await;
    let owners = nodes[0].1.owners_of(&SET_KEY);
    let writer = nodes
        .iter()
        .position(|(cluster, _)| !owners.contains(&cluster.node_id()))
        .expect("one node does not own the key");
    let non_owner = nodes[writer].1.clone();

    let workload = tokio::time::timeout(
        Duration::from_secs(120),
        run_workload(&non_owner, std::slice::from_ref(&non_owner), 6, 40),
    )
    .await
    .expect("the workload finishes within the bound");

    let readers: Vec<&SetCache> = nodes.iter().map(|(_, cache)| cache).collect();
    assert_only_unobserved_adds_survive(&readers, &workload).await;
    assert_eq!(
        non_owner.get(&SET_KEY).await,
        None,
        "the writer, not an owner, holds no copy"
    );
    shut_down(nodes).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adds_forwarded_by_a_non_owner_survive_a_remove_that_did_not_observe_them() {
    non_owner_workload("cache-it-seq-distributed", Duration::ZERO).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adds_a_non_owner_folds_in_a_window_survive_a_remove_that_did_not_observe_them() {
    non_owner_workload("cache-it-seq-distributed-window", Duration::from_millis(5)).await;
}

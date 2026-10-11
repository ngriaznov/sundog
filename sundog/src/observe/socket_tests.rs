use std::sync::Mutex as StdMutex;

use super::tests::caches;
use super::*;
use crate::cache::Cache;
use crate::cluster::Cluster;
use crate::cluster::test_support::{
    EndingDiscovery, loopback_config, registered_shard, wait_for_peer_count, wait_until,
};
use crate::store::encode_key;

/// How long a condition may take to hold. Failure detection at phi 6 and
/// a 200 ms gossip interval takes several seconds.
const BOUND: Duration = Duration::from_secs(30);

async fn eventually(what: &str, cond: impl AsyncFnMut() -> bool) {
    wait_until(BOUND, what, cond).await;
}

/// Three clusters on loopback, the second and third seeded by the first,
/// each seeing the other two.
async fn trio(name: &str) -> [Cluster; 3] {
    let a = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("a builds");
    let mut rest = Vec::new();
    for label in ["b", "c"] {
        rest.push(
            Cluster::builder(name)
                .seeds([a.local_gossip_addr()])
                .config(loopback_config())
                .build()
                .await
                .unwrap_or_else(|error| panic!("{label} builds: {error}")),
        );
    }
    let [b, c]: [Cluster; 2] = rest.try_into().expect("two clusters");
    for cluster in [&a, &b, &c] {
        wait_for_peer_count(cluster, 2).await;
    }
    [a, b, c]
}

async fn open(cluster: &Cluster, cache: &str, mode: Mode) -> Cache<u32, String> {
    tokio::time::timeout(
        Duration::from_secs(15),
        cluster.cache::<u32, String>(cache).mode(mode).open(),
    )
    .await
    .expect("the cache opens within the bound")
    .expect("the cache opens")
}

async fn observe(name: &str, seed: &Cluster) -> Observer {
    Observer::builder(name)
        .seeds([seed.local_gossip_addr()])
        .config(loopback_config())
        .build()
        .await
        .expect("the observer builds")
}

/// One live-peers receiver per member, marked seen. chitchat fires a
/// member's live-set watch exactly when the set of live gossip
/// participants changes, and the membership loop then publishes its peers
/// again, so a receiver changes once the member itself has counted a
/// participant in or out. That is the member's own verdict, where an
/// observer's failure detector holds its own arrival times.
fn member_watches(clusters: &[&Cluster]) -> Vec<watch::Receiver<Vec<Peer>>> {
    clusters
        .iter()
        .map(|cluster| {
            let mut peers = cluster.peers_watch();
            peers.borrow_and_update();
            peers
        })
        .collect()
}

/// Waits until every watched member has republished its peers since
/// `member_watches` marked them seen, and checks each value it sees holds
/// `expected` peers.
async fn every_member_republished(
    what: &str,
    watches: &mut [watch::Receiver<Vec<Peer>>],
    expected: usize,
) {
    for peers in watches {
        tokio::time::timeout(BOUND, peers.changed())
            .await
            .unwrap_or_else(|_| panic!("{what}: a member's live set did not change"))
            .unwrap_or_else(|_| panic!("{what}: a member stopped"));
        assert_eq!(peers.borrow_and_update().len(), expected, "{what}");
    }
}

fn live_members(observer: &Observer) -> usize {
    observer
        .snapshot()
        .members
        .iter()
        .filter(|member| member.status == MemberStatus::Live)
        .count()
}

fn status_of_node(snapshot: &ClusterSnapshot, node: NodeId) -> Option<MemberStatus> {
    snapshot
        .members
        .iter()
        .find(|member| member.peer.node == node)
        .map(|member| member.status)
}

/// What [`record`] collects while an observer runs.
struct Recorder {
    /// Every status each node passes through, in order.
    history: Arc<StdMutex<Vec<(NodeId, MemberStatus)>>>,
    /// The first snapshot that shows a departing member.
    departing: Arc<StdMutex<Option<Arc<ClusterSnapshot>>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Recorder {
    /// The statuses `node` passed through, in order.
    fn statuses(&self, node: NodeId) -> Vec<MemberStatus> {
        self.history
            .lock()
            .expect("history lock")
            .iter()
            .filter(|(member, _)| *member == node)
            .map(|&(_, status)| status)
            .collect()
    }
}

/// Follows `observer`'s snapshots from the current one on.
fn record(observer: &Observer) -> Recorder {
    let history: Arc<StdMutex<Vec<(NodeId, MemberStatus)>>> = Arc::default();
    let departing: Arc<StdMutex<Option<Arc<ClusterSnapshot>>>> = Arc::default();
    let mut updates = observer.subscribe();
    let task = tokio::spawn({
        let (history, departing) = (Arc::clone(&history), Arc::clone(&departing));
        async move {
            loop {
                let snapshot = Arc::clone(&updates.borrow_and_update());
                {
                    let mut history = history.lock().expect("history lock");
                    for member in &snapshot.members {
                        let last = history
                            .iter()
                            .rev()
                            .find(|(node, _)| *node == member.peer.node)
                            .map(|&(_, status)| status);
                        if last != Some(member.status) {
                            history.push((member.peer.node, member.status));
                        }
                    }
                }
                if snapshot
                    .members
                    .iter()
                    .any(|member| member.status == MemberStatus::Departing)
                {
                    departing
                        .lock()
                        .expect("departing lock")
                        .get_or_insert(snapshot);
                }
                if updates.changed().await.is_err() {
                    return;
                }
            }
        }
    });
    Recorder {
        history,
        departing,
        task,
    }
}

/// A graceful leave: Live, Departing, then Left for good, never Down.
fn assert_leave_history(history: &[MemberStatus]) {
    assert_eq!(history.first(), Some(&MemberStatus::Live));
    assert!(
        !history.contains(&MemberStatus::Down),
        "a leave is no crash: {history:?}"
    );
    let departing = history
        .iter()
        .position(|status| *status == MemberStatus::Departing)
        .expect("the departure was seen");
    let first_left = history
        .iter()
        .position(|status| *status == MemberStatus::Left)
        .expect("the leave ended Left");
    assert!(
        departing < first_left,
        "Departing comes before Left: {history:?}"
    );
    assert!(
        history[first_left..]
            .iter()
            .all(|status| *status == MemberStatus::Left),
        "Left is final: {history:?}"
    );
}

/// A crash: Live, then Down, never a departure.
fn assert_crash_history(history: &[MemberStatus]) {
    assert_eq!(history.first(), Some(&MemberStatus::Live));
    assert!(history.contains(&MemberStatus::Down), "{history:?}");
    assert!(
        !history.contains(&MemberStatus::Left) && !history.contains(&MemberStatus::Departing),
        "a crash is never a leave: {history:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_observer_sees_members_their_caches_and_ownership() {
    let name = "observe-it-sees";
    let [a, b, c] = trio(name).await;
    let owners = Mode::DEFAULT_OWNERS;
    let mut distributed_caches = Vec::new();
    for cluster in [&a, &b, &c] {
        distributed_caches.push(open(cluster, "d", Mode::distributed()).await);
    }
    let _replicated = [
        open(&a, "r", Mode::Replicated).await,
        open(&b, "r", Mode::Replicated).await,
    ];
    let observer = observe(name, &a).await;

    let expected = |with_r: bool| {
        let mut advertised = vec![("d", Mode::distributed())];
        if with_r {
            advertised.push(("r", Mode::Replicated));
        }
        caches(&advertised)
    };
    eventually(
        "the observer shows three live members and their caches",
        async || {
            let snapshot = observer.snapshot();
            snapshot.members.len() == 3
                && snapshot.members.iter().all(|member| {
                    member.status == MemberStatus::Live
                        && member.caches == expected(member.peer.node != c.node_id())
                })
        },
    )
    .await;
    let snapshot = observer.snapshot();
    assert_eq!(snapshot.cluster, name);
    assert_eq!(snapshot.anonymous, 0);
    for cluster in [&a, &b, &c] {
        for peer in cluster.peers() {
            let seen = snapshot
                .members
                .iter()
                .find(|member| member.peer.node == peer.node)
                .expect("a peer of a member is a member");
            assert_eq!(seen.peer, peer, "the record Cluster::peers reports");
        }
    }

    let shares = snapshot.ownership("d", owners).expect("three advertisers");
    assert!(shares.ranks_parts());
    assert_eq!(shares.eligible().len(), 3);
    let total: usize = shares
        .eligible()
        .iter()
        .map(|&node| shares.parts_owned_by(node))
        .sum();
    assert_eq!(total, 131_072, "two owners of each of 65,536 parts");
    eventually("every member computes the observer's view", async || {
        [&a, &b, &c].iter().all(|cluster| {
            registered_shard(cluster, &SmolStr::new("d")).ownership_view_hash()
                == Some(shares.view_hash())
        })
    })
    .await;
    for key in 0..512u32 {
        let part = PartId::of_key(&encode_key(&key).expect("a u32 encodes"));
        for cache in &distributed_caches {
            assert_eq!(
                shares.owners_of(part),
                cache.owners_of(&key).as_slice(),
                "key {key}"
            );
        }
    }

    observer.shutdown().await;
    for cluster in [a, b, c] {
        cluster.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_observer_tells_a_graceful_leave_from_a_crash() {
    let name = "observe-it-leave-crash";
    let [stays, leaves, crashes] = trio(name).await;
    for cluster in [&stays, &leaves, &crashes] {
        open(cluster, "d", Mode::distributed()).await;
    }
    let observer = observe(name, &stays).await;
    eventually("the observer shows three live members", async || {
        live_members(&observer) == 3
            && observer
                .snapshot()
                .members
                .iter()
                .all(|member| member.caches.contains_key("d"))
    })
    .await;
    let (stays_id, leaves_id, crashes_id) = (stays.node_id(), leaves.node_id(), crashes.node_id());

    let recorder = record(&observer);

    leaves.shutdown().await;
    crashes.crash().await;
    eventually(
        "the observer shows the leave as Left and the crash as Down",
        async || {
            let snapshot = observer.snapshot();
            status_of_node(&snapshot, leaves_id) == Some(MemberStatus::Left)
                && status_of_node(&snapshot, crashes_id) == Some(MemberStatus::Down)
        },
    )
    .await;
    eventually("the recorder has followed both", async || {
        recorder.statuses(leaves_id).last() == Some(&MemberStatus::Left)
            && recorder.statuses(crashes_id).last() == Some(&MemberStatus::Down)
    })
    .await;
    recorder.task.abort();

    // Properties, not exact sequences: chitchat revives a node whose
    // newer heartbeat a peer still relays, so a crash can read Down, Live,
    // Down. A departure is final, and a crash is never a departure.
    assert_leave_history(&recorder.statuses(leaves_id));
    assert_crash_history(&recorder.statuses(crashes_id));
    let stay_history = recorder.statuses(stays_id);
    assert_eq!(stay_history.first(), Some(&MemberStatus::Live));
    assert!(
        !stay_history.contains(&MemberStatus::Departing)
            && !stay_history.contains(&MemberStatus::Left),
        "a member that never stops never leaves: {stay_history:?}"
    );

    let seen_departing = recorder
        .departing
        .lock()
        .expect("departing lock")
        .clone()
        .expect("the departure was seen");
    let shares = seen_departing
        .ownership("d", Mode::DEFAULT_OWNERS)
        .expect("members still advertise");
    assert!(
        !shares.eligible().contains(&leaves_id),
        "a departing member owns nothing"
    );
    assert!(shares.eligible().contains(&stays_id));

    let shares = observer
        .snapshot()
        .ownership("d", Mode::DEFAULT_OWNERS)
        .expect("one member stays");
    assert_eq!(
        shares.eligible(),
        [stays_id],
        "neither the left nor the down member"
    );

    observer.shutdown().await;
    stays.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_observer_shows_a_joining_member_live_and_never_down() {
    let name = "observe-it-join";
    let [a, b, c] = trio(name).await;
    let observer = observe(name, &a).await;
    eventually("the observer shows three live members", async || {
        live_members(&observer) == 3
    })
    .await;
    let recorder = record(&observer);

    let joiner = Cluster::builder(name)
        .seeds([a.local_gossip_addr()])
        .config(loopback_config())
        .build()
        .await
        .expect("the joiner builds");
    let joiner_id = joiner.node_id();
    eventually("the observer shows four live members", async || {
        live_members(&observer) == 4
    })
    .await;
    // Several refresh ticks past the join, so a late verdict would show.
    tokio::time::sleep(REFRESH_INTERVAL * 3).await;
    recorder.task.abort();

    assert_eq!(
        recorder.statuses(joiner_id),
        [MemberStatus::Live],
        "chitchat holds a node dead until its second heartbeat; the observer does not report that"
    );
    for node in [a.node_id(), b.node_id(), c.node_id()] {
        assert_eq!(recorder.statuses(node), [MemberStatus::Live]);
    }

    observer.shutdown().await;
    for cluster in [a, b, c, joiner] {
        cluster.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_observer_is_invisible_to_every_member() {
    let name = "observe-it-invisible";
    let [a, b, c] = trio(name).await;
    let clusters = [&a, &b, &c];
    let mut distributed_caches = Vec::new();
    let mut replicated = Vec::new();
    for cluster in clusters {
        distributed_caches.push(open(cluster, "d", Mode::distributed()).await);
        replicated.push(open(cluster, "r", Mode::Replicated).await);
    }
    let owners_now = |caches: &[Cache<u32, String>]| -> Vec<Vec<Vec<NodeId>>> {
        caches
            .iter()
            .map(|cache| (0..256u32).map(|key| cache.owners_of(&key)).collect())
            .collect()
    };
    eventually(
        "every member has settled on one ownership view",
        async || {
            let hashes: Vec<Option<u64>> = clusters
                .iter()
                .map(|cluster| registered_shard(cluster, &SmolStr::new("d")).ownership_view_hash())
                .collect();
            hashes[0].is_some() && hashes.iter().all(|hash| *hash == hashes[0])
        },
    )
    .await;
    let before = owners_now(&distributed_caches);

    let mut watches = member_watches(&clusters);
    let observer = observe(name, &a).await;
    eventually("the observer shows three members", async || {
        live_members(&observer) == 3
    })
    .await;
    every_member_republished("the first observer joined", &mut watches, 2).await;
    for _ in 0..10 {
        for cluster in clusters {
            assert_eq!(cluster.peers().len(), 2, "the observer is no peer");
            assert_eq!(cluster.health().live_peers, 2);
        }
        assert_eq!(observer.snapshot().anonymous, 0);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let mut watches = member_watches(&clusters);
    let second = observe(name, &b).await;
    eventually("each observer counts the other as anonymous", async || {
        observer.snapshot().anonymous == 1
            && second.snapshot().anonymous == 1
            && live_members(&second) == 3
    })
    .await;
    every_member_republished("the second observer joined", &mut watches, 2).await;
    for cluster in clusters {
        assert_eq!(cluster.peers().len(), 2);
        assert_eq!(cluster.health().live_peers, 2);
    }

    for key in 0..100u32 {
        replicated[0]
            .insert(key, key.to_string())
            .await
            .expect("insert");
    }
    eventually(
        "replicated inserts converge with observers present",
        async || {
            let mut counts = Vec::new();
            for cache in &replicated {
                counts.push(cache.entry_count().await);
            }
            counts.iter().all(|&count| count == 100)
        },
    )
    .await;
    assert_eq!(owners_now(&distributed_caches), before, "observers joined");

    // Each observer's departure fires every member's live-set watch once
    // that member's own failure detector drops it, and the membership loop
    // then resends its peers. The test waits for that signal on every
    // member, one observer at a time so the first drop cannot stand in for
    // the second, and only then checks that nothing moved.
    for (leaving, label) in [
        (second, "the second observer"),
        (observer, "the first observer"),
    ] {
        let mut watches = member_watches(&clusters);
        leaving.shutdown().await;
        every_member_republished(&format!("{label} left"), &mut watches, 2).await;
    }
    for cluster in clusters {
        assert_eq!(cluster.peers().len(), 2);
        assert_eq!(cluster.health().live_peers, 2);
    }
    assert_eq!(owners_now(&distributed_caches), before, "observers left");

    for cluster in [a, b, c] {
        cluster.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observer_subscribe_wakes_on_a_change_and_not_on_heartbeats() {
    let name = "observe-it-subscribe";
    let [a, b, c] = trio(name).await;
    let observer = observe(name, &a).await;
    eventually("the observer shows three members", async || {
        live_members(&observer) == 3
    })
    .await;

    let mut updates = observer.subscribe();
    assert!(
        !updates.has_changed().expect("the observer runs"),
        "a new subscription starts with the current snapshot seen"
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        !updates.has_changed().expect("the observer runs"),
        "gossip heartbeats publish nothing"
    );

    open(&b, "fresh", Mode::Replicated).await;
    let b_id = b.node_id();
    tokio::time::timeout(BOUND, async {
        loop {
            updates
                .changed()
                .await
                .expect("the observer runs while it is awaited");
            let snapshot = Arc::clone(&updates.borrow_and_update());
            let advertises = snapshot.members.iter().any(|member| {
                member.peer.node == b_id && member.caches == caches(&[("fresh", Mode::Replicated)])
            });
            if advertises {
                return;
            }
        }
    })
    .await
    .expect("opening a cache publishes within the bound");

    observer.shutdown().await;
    for cluster in [a, b, c] {
        cluster.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observer_builder_honors_discovery_and_config() {
    let name = "observe-it-builder";
    let a = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("a builds");

    let observer = Observer::builder(name)
        .discovery(Static::new([a.local_gossip_addr()]))
        .config(loopback_config())
        .build()
        .await
        .expect("the observer builds");

    let addr = observer.local_gossip_addr();
    assert!(addr.ip().is_loopback(), "gossip_bind_addr is honored");
    assert_ne!(addr.port(), 0, "port 0 resolves to a bound port");
    let rendered = format!("{observer:?}");
    assert!(rendered.contains(name) && rendered.contains(&addr.to_string()));
    eventually("the custom discovery finds the cluster", async || {
        live_members(&observer) == 1
    })
    .await;
    assert_eq!(observer.snapshot().members[0].peer.node, a.node_id());

    let unbindable = Observer::builder(name)
        .seeds([a.local_gossip_addr()])
        .config(ClusterConfig {
            gossip_bind_addr: addr,
            ..loopback_config()
        })
        .build()
        .await;
    assert!(
        matches!(unbindable, Err(JoinError::Membership(_))),
        "a taken fixed gossip_bind_addr is a membership error, not a probe failure: {unbindable:?}"
    );

    observer.shutdown().await;
    a.shutdown().await;
}

/// Builds an observer over a seed stream that ends `lag` after its one
/// address, then checks it finds the member, still publishes a change
/// once the stream has ended, and shuts down.
async fn assert_observer_survives_ending_seeds(name: &str, lag: Duration) {
    let a = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("a builds");
    let observer = Observer::builder(name)
        .discovery(EndingDiscovery {
            addrs: vec![a.local_gossip_addr()],
            lag,
        })
        .config(loopback_config())
        .build()
        .await
        .expect("the observer builds");
    eventually("the observer finds the member", async || {
        live_members(&observer) == 1
    })
    .await;

    // Outwait the end of the seed stream, then change the cluster.
    time::sleep(lag + Duration::from_secs(1)).await;
    open(&a, "late", Mode::Replicated).await;
    eventually(
        "the observer sees a change after the stream ended",
        async || {
            observer
                .snapshot()
                .members
                .iter()
                .any(|member| member.caches.contains_key("late"))
        },
    )
    .await;

    let stopped = time::timeout(Duration::from_secs(10), observer.shutdown()).await;
    assert!(stopped.is_ok(), "shutdown completes after the stream ended");
    a.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_observer_keeps_watching_after_its_seed_stream_ends() {
    assert_observer_survives_ending_seeds("observe-it-ending-seeds", Duration::ZERO).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_observer_keeps_watching_after_its_seed_stream_ends_mid_run() {
    // Past `INITIAL_SEED_WINDOW`, so the observe loop itself sees the end.
    assert_observer_survives_ending_seeds(
        "observe-it-ending-seeds-late",
        Duration::from_millis(1_500),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observer_shutdown_returns_and_a_fresh_observer_sees_every_member() {
    let name = "observe-it-shutdown";
    let [a, b, c] = trio(name).await;
    let first = observe(name, &a).await;
    eventually("the first observer shows three members", async || {
        live_members(&first) == 3
    })
    .await;

    let clone = first.clone();
    let mut updates = clone.subscribe();
    first.shutdown().await;
    assert_eq!(
        live_members(&clone),
        3,
        "the last snapshot stays readable after the loop stops"
    );
    let closed =
        tokio::time::timeout(BOUND, async { while updates.changed().await.is_ok() {} }).await;
    assert!(
        closed.is_ok(),
        "a receiver from a surviving clone learns that the observer stopped"
    );

    let second = observe(name, &b).await;
    // The first observer stays live in the members' gossip until the
    // failure detector drops it, and counts as anonymous until then.
    eventually(
        "a fresh observer shows every member and no stale observer",
        async || live_members(&second) == 3 && second.snapshot().anonymous == 0,
    )
    .await;

    second.shutdown().await;
    for cluster in [a, b, c] {
        cluster.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clones_share_one_session_and_dropping_the_last_stops_it() {
    let name = "observe-it-drop";
    let a = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("a builds");
    let observer = observe(name, &a).await;
    let clone = observer.clone();
    assert_eq!(clone.local_gossip_addr(), observer.local_gossip_addr());
    let mut updates = observer.subscribe();

    drop(observer);
    open(&a, "late", Mode::Replicated).await;
    eventually("a surviving clone keeps watching", async || {
        clone
            .snapshot()
            .members
            .iter()
            .any(|member| member.caches.contains_key("late"))
    })
    .await;

    drop(clone);
    // The loop holds the only sender; the channel closes when it stops.
    let closed = tokio::time::timeout(Duration::from_secs(10), async {
        while updates.changed().await.is_ok() {}
    })
    .await;
    assert!(closed.is_ok(), "the loop outlived every clone");

    a.shutdown().await;
}

//! The data pipeline end to end: real in-process clusters on loopback, a
//! `Feed` joined as an observer, and a `Model` folding what the feed sends.
//!
//! The scenario is the demo's core contrast. A crash raises `DOWN` and never
//! `LEFT`; a graceful leave raises `LEAVE`, then the ownership `VIEW` that
//! removes the node, then `LEFT`.

use std::net::{Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant, SystemTime};

use sundog::observe::MemberStatus;
use sundog::store::Mode;
use sundog::{Cache, Cluster, ClusterConfig, NodeId};
use sundog_lens::model::events::{Event, EventKind};
use sundog_lens::model::lifelines::PhaseKind;
use sundog_lens::model::{Model, digest};
use sundog_lens::source::observer::forward;
use sundog_lens::source::{Feed, FeedConfig, Update};

/// The longest any wait lasts.
const BOUND: Duration = Duration::from_secs(30);

/// Parts in the key space.
const PARTS: usize = 65_536;

fn loopback_config() -> ClusterConfig {
    ClusterConfig::default().with(|config| {
        let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        config.gossip_bind_addr = loopback;
        config.data_bind_addr = loopback;
        config.ae_interval = Duration::from_millis(200);
        config.tombstone_ttl = Duration::from_secs(2);
        config.state_transfer_budget = Duration::from_secs(5);
    })
}

async fn node(name: &str, seed: Option<SocketAddr>) -> Cluster {
    Cluster::builder(name)
        .seeds(seed)
        .config(loopback_config())
        .build()
        .await
        .expect("the node builds")
}

async fn open_caches(cluster: &Cluster) -> (Cache<u32, String>, Cache<u32, String>) {
    let open = |name: &'static str, mode: Mode| async move {
        tokio::time::timeout(
            Duration::from_secs(15),
            cluster.cache::<u32, String>(name).mode(mode).open(),
        )
        .await
        .expect("the cache opens within the bound")
        .expect("the cache opens")
    };
    (
        open("it", Mode::distributed()).await,
        open("side", Mode::Replicated).await,
    )
}

/// Applies the feed's updates to `model` until `done` holds.
async fn pump(feed: &mut Feed, model: &mut Model, what: &str, done: impl Fn(&Model) -> bool) {
    let deadline = Instant::now() + BOUND;
    let mut ticker = tokio::time::interval(Duration::from_millis(100));
    while !done(model) {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {what}; events so far: {:?}",
            model
                .events()
                .iter()
                .map(|event| event.kind.tag())
                .collect::<Vec<_>>()
        );
        tokio::select! {
            update = feed.recv() => {
                let update = update.expect("the feed stays open");
                model.apply(update, Instant::now(), SystemTime::now());
            }
            _ = ticker.tick() => {
                model.tick(Instant::now());
            }
        }
    }
}

fn status_of(model: &Model, node: NodeId) -> Option<MemberStatus> {
    model
        .snapshot()?
        .members
        .iter()
        .find(|member| member.peer.node == node)
        .map(|member| member.status)
}

fn position(events: &[Event], wanted: impl Fn(&EventKind) -> bool) -> Option<usize> {
    events.iter().position(|event| wanted(&event.kind))
}

fn eligible(model: &Model) -> Vec<NodeId> {
    model
        .ownership("it")
        .map(|digest| digest.eligible.clone())
        .unwrap_or_default()
}

/// The index of the first event of `tag` about `id`.
fn about(events: &[Event], tag: &str, id: NodeId) -> Option<usize> {
    position(events, |kind| kind.tag() == tag && kind.node() == Some(id))
}

/// The index of the first `VIEW` after index `after` that takes parts from
/// `id`. Earlier views can shrink a node's share as members arrive.
fn view_dropping(events: &[Event], after: usize, id: NodeId) -> Option<usize> {
    events
        .iter()
        .enumerate()
        .skip(after + 1)
        .find(|(_, event)| {
            matches!(&event.kind, EventKind::View { deltas, .. }
                if deltas.iter().any(|&(node, delta)| node == id && delta < 0))
        })
        .map(|(index, _)| index)
}

fn log(model: &Model) -> Vec<Event> {
    model.events().iter().cloned().collect()
}

fn assert_started(model: &Model, ids: [NodeId; 3]) {
    let it = model.ownership("it").expect("`it` is ranked");
    assert_eq!(
        it.counts.iter().map(|&(_, count)| count).sum::<usize>(),
        2 * PARTS,
        "two owners per part"
    );
    assert!(
        model.ownership("side").is_none(),
        "a Replicated cache has no ownership"
    );
    // The members found at startup are the baseline: each one runs a Live
    // lifeline from the first sight.
    let snapshot = model.snapshot().expect("the observer has a snapshot");
    for id in ids {
        let member = snapshot
            .members
            .iter()
            .find(|member| member.peer.node == id)
            .unwrap_or_else(|| panic!("{id} is a member"));
        let line = model
            .lifelines()
            .node(member.peer.gossip_addr)
            .unwrap_or_else(|| panic!("{id} has a lifeline"));
        assert_eq!(line.current(), Some(PhaseKind::Live), "{id} lives");
    }
    assert_eq!(model.slots().len(), 3);
}

/// A crash is DOWN, then a VIEW that drops the node, and never LEFT.
fn assert_crash(model: &Model, crashed: NodeId) {
    let events = log(model);
    let down = about(&events, "DOWN", crashed).expect("DOWN(c)");
    assert!(
        view_dropping(&events, down, crashed).is_some(),
        "a VIEW that drops c follows DOWN"
    );
    assert!(
        about(&events, "LEFT", crashed).is_none(),
        "a crash is never a departure"
    );
}

/// A graceful leave is LEAVE, a VIEW that drops the node, then LEFT.
fn assert_leave(model: &Model, left: NodeId) {
    let events = log(model);
    let leave = about(&events, "LEAVE", left).expect("LEAVE(b)");
    let gone = about(&events, "LEFT", left).expect("LEFT(b)");
    let view = view_dropping(&events, leave, left).expect("a VIEW that drops b follows LEAVE");
    assert!(leave < gone, "LEAVE comes before LEFT");
    assert!(view < gone, "VIEW comes before LEFT");
    assert!(
        about(&events, "DOWN", left).is_none(),
        "a graceful leave is never a crash"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_pipeline_follows_a_cluster_through_a_crash_and_a_graceful_leave() {
    let name = "lens-observe-pipeline";
    let a = node(name, None).await;
    let b = node(name, Some(a.local_gossip_addr())).await;
    let c = node(name, Some(a.local_gossip_addr())).await;
    let (a_id, b_id, c_id) = (a.node_id(), b.node_id(), c.node_id());
    let _a_caches = open_caches(&a).await;
    let _b_caches = open_caches(&b).await;
    let _c_caches = open_caches(&c).await;

    let mut config = FeedConfig::new(name);
    config.bind = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    config.seeds = vec![sundog_lens::cli::Seed::Addr(a.local_gossip_addr())];
    let mut feed = Feed::spawn(config).await.expect("the feed starts");
    let mut model = Model::new();

    pump(&mut feed, &mut model, "three live members", |model| {
        digest(model).live == 3 && eligible(model).len() == 3
    })
    .await;
    assert_started(&model, [a_id, b_id, c_id]);
    pump(&mut feed, &mut model, "the first view to settle", |model| {
        model.settled("it") == Some(true)
    })
    .await;

    c.crash().await;
    pump(&mut feed, &mut model, "the crash to read Down", |model| {
        status_of(model, c_id) == Some(MemberStatus::Down)
            && eligible(model) == [a_id.min(b_id), a_id.max(b_id)]
    })
    .await;
    assert_crash(&model, c_id);
    pump(&mut feed, &mut model, "the view to settle", |model| {
        model.settled("it") == Some(true)
    })
    .await;

    b.shutdown().await;
    pump(&mut feed, &mut model, "b to be Left", |model| {
        status_of(model, b_id) == Some(MemberStatus::Left) && eligible(model) == [a_id]
    })
    .await;
    assert_leave(&model, b_id);

    // One eligible member is left, and it owns every part once.
    let last = model.ownership("it").expect("`it` is still ranked");
    assert_eq!(last.eligible, [a_id]);
    assert_eq!(last.counts, [(a_id, PARTS)]);
    assert_eq!(digest(&model).live, 1);

    feed.shutdown().await;
    a.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forward_ends_with_the_observer() {
    let name = "lens-observe-forward";
    let a = node(name, None).await;
    let observer = sundog::observe::Observer::builder(name)
        .seeds([a.local_gossip_addr()])
        .config(ClusterConfig::default().with(|config| {
            config.gossip_bind_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        }))
        .build()
        .await
        .expect("the observer starts");
    let (tx, mut updates) = tokio::sync::mpsc::channel(16);
    let task = tokio::spawn(forward(observer.subscribe(), tx, None));
    let first = tokio::time::timeout(BOUND, updates.recv())
        .await
        .expect("an update arrives")
        .expect("the stream is open");
    assert!(matches!(first, Update::Snapshot(..)));

    observer.shutdown().await;
    tokio::time::timeout(BOUND, task)
        .await
        .expect("forward ends once the observer stops")
        .expect("forward does not panic");
    a.shutdown().await;
}

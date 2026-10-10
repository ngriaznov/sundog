//! `watch --once` against real in-process clusters: the collection waits for
//! the member set to settle, and the report it builds lists every member and
//! the ownership the lens computes.

use std::net::{Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant, SystemTime};

use sundog::store::Mode;
use sundog::{Cluster, ClusterConfig};
use sundog_lens::cli::{ExplainArgs, OnceArgs, Seed};
use sundog_lens::key::KeySpec;
use sundog_lens::model::Model;
use sundog_lens::once::{Limits, build_report, collect, explain_report, render_text};
use sundog_lens::source::{Feed, FeedConfig};

fn loopback_config() -> ClusterConfig {
    ClusterConfig::default().with(|config| {
        let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        config.gossip_bind_addr = loopback;
        config.data_bind_addr = loopback;
        config.ae_interval = Duration::from_millis(200);
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

async fn open_it(cluster: &Cluster) -> sundog::Cache<u32, String> {
    tokio::time::timeout(
        Duration::from_secs(15),
        cluster
            .cache::<u32, String>("it")
            .mode(Mode::distributed())
            .open(),
    )
    .await
    .expect("the cache opens within the bound")
    .expect("the cache opens")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_report_lists_every_member_and_the_ownership_computed_for_them() {
    let name = "lens-once";
    let a = node(name, None).await;
    let b = node(name, Some(a.local_gossip_addr())).await;
    let c = node(name, Some(a.local_gossip_addr())).await;
    let _caches = (open_it(&a).await, open_it(&b).await, open_it(&c).await);

    let mut config = FeedConfig::new(name);
    config.bind = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    config.seeds = vec![Seed::Addr(a.local_gossip_addr())];
    let mut feed = Feed::spawn(config).await.expect("the feed starts");
    let mut model = Model::new();
    let once = OnceArgs {
        json: false,
        settle: Duration::from_secs(2),
        explain: None,
    };
    let started = Instant::now();
    tokio::time::timeout(
        Duration::from_secs(40),
        collect(&mut feed, &mut model, &once, false, Limits::default()),
    )
    .await
    .expect("the collection ends within the bound")
    .expect("the collection succeeds");
    assert!(
        started.elapsed() >= once.settle,
        "the run waits for the settle time"
    );

    let observer = feed.observer_addr().to_string();
    let report = build_report(&model, &observer, started.elapsed(), SystemTime::now());
    assert_eq!(report.cluster, name);
    assert_eq!(
        (report.live, report.departing, report.down, report.left),
        (3, 0, 0, 0)
    );
    assert_eq!(report.members.len(), 3);
    assert!(
        report
            .members
            .iter()
            .all(|m| m.status == "live" && m.caches["it"] == "distributed:2")
    );
    let it = report
        .caches
        .iter()
        .find(|c| c.name == "it")
        .expect("it is listed");
    let own = it.ownership.as_ref().expect("it has ownership");
    assert_eq!((own.owners, own.eligible, own.parts_total), (2, 3, 131_072));
    assert_eq!(own.shares.iter().map(|s| s.parts).sum::<usize>(), 131_072);
    assert_eq!(own.reporting, 0, "no exporter was asked");

    let text = render_text(&report);
    assert!(text.starts_with("lens-once · 3 live · protocol "), "{text}");
    assert!(text.contains("131,072 (2×65,536)"), "{text}");
    let member_rows = text
        .lines()
        .filter(|l| l.starts_with('n') && l.contains("live"))
        .count();
    assert_eq!(member_rows, 3, "{text}");
    let json = serde_json::to_value(&report).expect("the report is JSON");
    assert_eq!(json["members"].as_array().map(Vec::len), Some(3));

    feed.shutdown().await;
    a.shutdown().await;
    b.shutdown().await;
    c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_run_that_hears_no_member_gives_up_and_names_the_likely_causes() {
    let mut config = FeedConfig::new("lens-once-empty");
    config.bind = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    config.seeds = vec![Seed::Addr(SocketAddr::from((Ipv4Addr::LOCALHOST, 9)))];
    let mut feed = Feed::spawn(config).await.expect("the feed starts");
    let mut model = Model::new();
    let once = OnceArgs {
        json: false,
        settle: Duration::from_secs(1),
        explain: None,
    };
    let limits = Limits {
        first_member: Duration::from_secs(2),
        extras: Duration::from_secs(1),
    };
    let error = tokio::time::timeout(
        Duration::from_secs(20),
        collect(&mut feed, &mut model, &once, false, limits),
    )
    .await
    .expect("the run ends within the bound")
    .expect_err("no member shows up");
    let message = error.to_string();
    assert!(
        message.contains("no member of the cluster showed up in 2 s"),
        "{message}"
    );
    assert!(message.contains("cluster name and the seeds"), "{message}");
    feed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_run_stops_waiting_for_scrapes_that_never_come_after_the_extras_limit() {
    let name = "lens-once-extras";
    let a = node(name, None).await;
    let _cache = open_it(&a).await;
    let mut config = FeedConfig::new(name);
    config.bind = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    config.seeds = vec![Seed::Addr(a.local_gossip_addr())];
    let mut feed = Feed::spawn(config).await.expect("the feed starts");
    let mut model = Model::new();
    let once = OnceArgs {
        json: false,
        settle: Duration::from_secs(1),
        explain: None,
    };
    let limits = Limits {
        first_member: Duration::from_secs(10),
        extras: Duration::from_secs(1),
    };
    let started = Instant::now();
    // The run is told to expect scrapes, but nothing scrapes.
    tokio::time::timeout(
        Duration::from_secs(30),
        collect(&mut feed, &mut model, &once, true, limits),
    )
    .await
    .expect("the run ends within the bound")
    .expect("the run succeeds without the scrapes");
    assert!(
        started.elapsed() >= Duration::from_secs(2),
        "settle plus extras"
    );
    feed.shutdown().await;
    a.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_explained_run_waits_for_the_view_it_names_to_settle() {
    let name = "lens-once-explain-wait";
    let a = node(name, None).await;
    let _cache = open_it(&a).await;
    let mut config = FeedConfig::new(name);
    config.bind = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    config.seeds = vec![Seed::Addr(a.local_gossip_addr())];
    let mut feed = Feed::spawn(config).await.expect("the feed starts");
    let mut model = Model::new();
    // The members hold still after one second, but a view needs three to
    // count as settled, so the run is not done when the members are.
    let explain = ExplainArgs {
        key: KeySpec::parse("k1").expect("the key parses"),
        cache: None,
    };
    let once = OnceArgs {
        json: false,
        settle: Duration::from_secs(1),
        explain: Some(explain.clone()),
    };
    let limits = Limits {
        first_member: Duration::from_secs(10),
        extras: Duration::from_secs(30),
    };
    tokio::time::timeout(
        Duration::from_secs(60),
        collect(&mut feed, &mut model, &once, false, limits),
    )
    .await
    .expect("the run ends within the bound")
    .expect("the run succeeds");
    assert_eq!(model.settled("it"), Some(true), "the view has held");
    let located = explain_report(&model, &explain).expect("the key is located");
    assert!(located.settled && located.gossip_only);
    assert_eq!(located.eligible, 1);
    feed.shutdown().await;
    a.shutdown().await;
}

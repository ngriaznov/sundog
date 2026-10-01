//! The metrics scraper against an in-test exporter: a `TcpListener` that
//! serves a capture of a real node's `/metrics` with `Connection: close`, and
//! a `/readyz` that answers 503, then 200.
//!
//! The capture is `fixtures/metrics.prom`, taken from a running
//! `sundog-testnode` built with the `prometheus` feature.

use std::collections::VecDeque;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU16, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use sundog::observe::{ClusterSnapshot, MemberStatus};
use sundog::{Cluster, ClusterConfig};
use sundog_lens::cli::{ScrapePin, Seed};
use sundog_lens::model::testkit;
use sundog_lens::model::{EventKind, ExporterState, Model};
use sundog_lens::source::expo::{self, Sample};
use sundog_lens::source::names;
use sundog_lens::source::scrape::{
    self, REQUEST_TIMEOUT, ScrapeConfig, ScrapeError, ScrapeReport, Target,
};
use sundog_lens::source::{Feed, FeedConfig, Update};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

const FIXTURE: &str = include_str!("fixtures/metrics.prom");

/// The longest any wait lasts.
const BOUND: Duration = Duration::from_secs(30);

/// How the in-test exporter answers.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Behavior {
    /// Answer every request.
    Serve,
    /// Accept the connection and never answer.
    Hang,
}

struct State {
    metrics: Mutex<String>,
    metrics_status: AtomicU16,
    ready: Mutex<VecDeque<u16>>,
    last_ready: AtomicU16,
    metrics_requests: AtomicUsize,
}

/// An HTTP/1.1 server that closes every connection after one answer.
struct Exporter {
    addr: SocketAddr,
    state: Arc<State>,
    task: JoinHandle<()>,
}

impl Exporter {
    /// Serves `FIXTURE`; `/readyz` answers each of `ready` once, then repeats
    /// the last.
    async fn start(ready: &[u16]) -> Self {
        Self::start_as(Behavior::Serve, ready).await
    }

    async fn start_as(behavior: Behavior, ready: &[u16]) -> Self {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("the exporter binds");
        let addr = listener.local_addr().expect("a local address");
        let state = Arc::new(State {
            metrics: Mutex::new(FIXTURE.to_owned()),
            metrics_status: AtomicU16::new(200),
            ready: Mutex::new(ready.iter().copied().collect()),
            last_ready: AtomicU16::new(ready.last().copied().unwrap_or(200)),
            metrics_requests: AtomicUsize::new(0),
        });
        let served = Arc::clone(&state);
        let task = tokio::spawn(async move {
            let mut hung = Vec::new();
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                if behavior == Behavior::Hang {
                    hung.push(stream);
                    continue;
                }
                let state = Arc::clone(&served);
                tokio::spawn(async move {
                    let mut request = Vec::new();
                    let mut chunk = [0u8; 1024];
                    while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                        match stream.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(read) => request.extend_from_slice(&chunk[..read]),
                        }
                    }
                    let line = String::from_utf8_lossy(&request);
                    let path = line.split_whitespace().nth(1).unwrap_or("/").to_owned();
                    let (status, body) = state.answer(&path);
                    let reason = match status {
                        200 => "OK",
                        404 => "Not Found",
                        503 => "Service Unavailable",
                        _ => "Error",
                    };
                    let response = format!(
                        "HTTP/1.1 {status} {reason}\r\nContent-Type: text/plain; charset=utf-8\r\n\
                         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                    let _ = stream.shutdown().await;
                });
            }
        });
        Self { addr, state, task }
    }

    fn url(&self) -> String {
        format!("http://{}/metrics", self.addr)
    }

    fn metrics_requests(&self) -> usize {
        self.state.metrics_requests.load(Ordering::SeqCst)
    }

    fn set_metrics(&self, body: String) {
        *self.state.metrics.lock().unwrap() = body;
    }

    fn set_metrics_status(&self, status: u16) {
        self.state.metrics_status.store(status, Ordering::SeqCst);
    }

    /// Stops answering: later connections are refused.
    fn stop(self) {
        self.task.abort();
    }
}

impl State {
    fn answer(&self, path: &str) -> (u16, String) {
        match path {
            "/metrics" => {
                self.metrics_requests.fetch_add(1, Ordering::SeqCst);
                let status = self.metrics_status.load(Ordering::SeqCst);
                let body = if status == 200 {
                    self.metrics.lock().unwrap().clone()
                } else {
                    "error\n".to_owned()
                };
                (status, body)
            }
            "/readyz" => {
                let status = self
                    .ready
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or_else(|| self.last_ready.load(Ordering::SeqCst));
                (
                    status,
                    if status == 200 {
                        "ready\n"
                    } else {
                        "not ready\n"
                    }
                    .to_owned(),
                )
            }
            _ => (404, "not found\n".to_owned()),
        }
    }
}

/// The capture with `sundog_frames_sent_total` raised by `more`.
fn capture_with_frames_up(more: u32) -> String {
    let frames = expo::parse(FIXTURE)
        .into_iter()
        .find(|sample| sample.name == names::FRAMES_SENT)
        .expect("the capture holds the frame counter")
        .value;
    let line = |value: f64| format!("{} {value}", names::FRAMES_SENT);
    assert!(FIXTURE.contains(&line(frames)));
    FIXTURE.replace(&line(frames), &line(frames + f64::from(more)))
}

fn target(exporter: &Exporter) -> Target {
    Target {
        addr: testkit::gossip_addr(1),
        node: testkit::node_id(1, 0),
        url: exporter.url(),
    }
}

fn samples(report: &ScrapeReport) -> &[Sample] {
    report.outcome.as_ref().expect("the scrape answered")
}

#[tokio::test]
async fn a_round_returns_the_captured_samples_and_the_ready_verdict() {
    let exporter = Exporter::start(&[503, 200]).await;
    let target = target(&exporter);

    let first = scrape::scrape_round(&target, REQUEST_TIMEOUT, true).await;
    assert_eq!(samples(&first), expo::parse(FIXTURE));
    assert_eq!(first.ready, Some(false), "/readyz answers 503 first");
    assert_eq!((first.addr, first.node), (target.addr, target.node));

    let second = scrape::scrape_round(&target, REQUEST_TIMEOUT, true).await;
    assert_eq!(second.ready, Some(true), "then 200");
    assert!(second.at > first.at);

    let unprobed = scrape::scrape_round(&target, REQUEST_TIMEOUT, false).await;
    assert_eq!(
        unprobed.ready, None,
        "a round that does not probe has no verdict"
    );
    assert_eq!(exporter.metrics_requests(), 3);
}

#[tokio::test]
async fn two_rounds_give_rates_and_a_ready_event_through_the_model() {
    let exporter = Exporter::start(&[503, 200]).await;
    let target = target(&exporter);
    let mut model = Model::new();
    let snapshot = Arc::new(testkit::snapshot(1));
    let now = Instant::now();
    model.apply(Update::Snapshot(snapshot, now), now, SystemTime::now());

    let first = scrape::scrape_round(&target, REQUEST_TIMEOUT, true).await;
    exporter.set_metrics(capture_with_frames_up(1000));
    tokio::time::sleep(Duration::from_millis(150)).await;
    let second = scrape::scrape_round(&target, REQUEST_TIMEOUT, true).await;
    let elapsed = second.at.duration_since(first.at).as_secs_f64();

    let mut events = Vec::new();
    for report in [first, second] {
        let at = report.at;
        events.extend(model.apply(Update::Scrape(report), at, SystemTime::now()));
    }
    let tags: Vec<_> = events.iter().map(|event| event.kind.tag()).collect();
    assert_eq!(tags, ["EXPORTER", "READY"], "answering, then ready");

    let metrics = model.metrics(target.addr).expect("metrics were folded");
    assert_eq!(metrics.folds(), 2);
    let rate = metrics
        .rate_sum(names::FRAMES_SENT)
        .expect("two scrapes give a rate");
    assert!(
        (rate - 1000.0 / elapsed).abs() < 1e-6,
        "{rate} frames/s over {elapsed} s"
    );
    assert_eq!(metrics.owned_parts("it"), Some(43_616.0));
    assert_eq!(metrics.rate_sum(names::BYTES_SENT), Some(0.0));
    assert_eq!(model.exporter(target.addr).unwrap().ready(), Some(true));
}

#[tokio::test]
async fn an_exporter_that_never_answers_times_out_at_the_deadline() {
    let exporter = Exporter::start_as(Behavior::Hang, &[200]).await;
    let target = target(&exporter);
    let started = Instant::now();
    let report = scrape::scrape_round(&target, REQUEST_TIMEOUT, true).await;
    let took = started.elapsed();
    assert_eq!(report.outcome, Err(ScrapeError::Timeout));
    assert_eq!(report.ready, None, "a probe that times out has no verdict");
    assert!(
        took + Duration::from_millis(50) >= REQUEST_TIMEOUT,
        "the round waited for its deadline: {took:?}"
    );
    assert!(
        took < REQUEST_TIMEOUT + Duration::from_secs(2),
        "metrics and readyz wait together, not one after the other: {took:?}"
    );
}

#[tokio::test]
async fn a_refused_connection_is_a_connect_error() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let target = Target {
        addr: testkit::gossip_addr(1),
        node: testkit::node_id(1, 0),
        url: format!("http://{addr}/metrics"),
    };
    let report = scrape::scrape_round(&target, REQUEST_TIMEOUT, true).await;
    assert!(
        matches!(&report.outcome, Err(ScrapeError::Connect(message)) if message.contains("connect")),
        "{:?}",
        report.outcome
    );
    assert_eq!(report.ready, None);
}

#[tokio::test]
async fn an_answer_other_than_200_is_a_status_error() {
    let exporter = Exporter::start(&[404]).await;
    exporter.set_metrics_status(500);
    let report = scrape::scrape_round(&target(&exporter), REQUEST_TIMEOUT, true).await;
    assert_eq!(report.outcome, Err(ScrapeError::Status(500)));
    assert_eq!(report.ready, None, "a 404 from /readyz is no verdict");
}

#[tokio::test]
async fn fetching_samples_and_probing_readiness_work_on_their_own() {
    let exporter = Exporter::start(&[503, 200, 404]).await;
    let url = exporter.url();
    assert_eq!(
        scrape::fetch_samples(&url, REQUEST_TIMEOUT).await.unwrap(),
        expo::parse(FIXTURE)
    );
    assert_eq!(
        scrape::probe_ready(&url, REQUEST_TIMEOUT).await,
        Some(false)
    );
    assert_eq!(scrape::probe_ready(&url, REQUEST_TIMEOUT).await, Some(true));
    assert_eq!(scrape::probe_ready(&url, REQUEST_TIMEOUT).await, None);
    assert_eq!(
        scrape::probe_ready("https://not-http/metrics", REQUEST_TIMEOUT).await,
        None
    );
}

fn pin(addr: SocketAddr, url: &str) -> ScrapePin {
    ScrapePin {
        node: addr.to_string(),
        url: url.to_owned(),
    }
}

fn two_members(second: MemberStatus) -> Arc<ClusterSnapshot> {
    Arc::new(ClusterSnapshot::new(
        "fixture",
        vec![
            testkit::member(1, MemberStatus::Live),
            testkit::member(2, second),
        ],
        0,
    ))
}

/// The scrape reports that arrive within `window`.
async fn reports_within(
    updates: &mut mpsc::Receiver<Update>,
    window: Duration,
) -> Vec<ScrapeReport> {
    let deadline = tokio::time::Instant::now() + window;
    let mut reports = Vec::new();
    while let Ok(Some(update)) = tokio::time::timeout_at(deadline, updates.recv()).await {
        if let Update::Scrape(report) = update {
            reports.push(report);
        }
    }
    reports
}

/// Reports until one has arrived from every address in `addrs`.
async fn reports_from(updates: &mut mpsc::Receiver<Update>, addrs: &[SocketAddr]) {
    let mut missing: Vec<SocketAddr> = addrs.to_vec();
    let deadline = tokio::time::Instant::now() + BOUND;
    while !missing.is_empty() {
        let update = tokio::time::timeout_at(deadline, updates.recv())
            .await
            .expect("every address reports in time")
            .expect("the scraper stays open");
        if let Update::Scrape(report) = update {
            missing.retain(|addr| *addr != report.addr);
        }
    }
}

fn fast(config: ScrapeConfig) -> ScrapeConfig {
    ScrapeConfig {
        interval: Duration::from_millis(50),
        ..config
    }
}

#[tokio::test]
async fn the_supervisor_scrapes_each_live_member_and_stops_when_one_goes_down() {
    let (a, b) = (Exporter::start(&[200]).await, Exporter::start(&[200]).await);
    let (addr1, addr2) = (testkit::gossip_addr(1), testkit::gossip_addr(2));
    let config = fast(ScrapeConfig::new(
        Vec::new(),
        vec![pin(addr1, &a.url()), pin(addr2, &b.url())],
    ));
    let (snapshots, watched) = watch::channel(two_members(MemberStatus::Live));
    let (tx, mut updates) = mpsc::channel(256);
    let task = tokio::spawn(scrape::run(config, watched, tx));

    reports_from(&mut updates, &[addr1, addr2]).await;
    snapshots
        .send(two_members(MemberStatus::Down))
        .expect("the supervisor watches");
    // Rounds already under way can still land.
    reports_within(&mut updates, Duration::from_millis(400)).await;
    let later = reports_within(&mut updates, Duration::from_millis(400)).await;
    assert!(
        later.iter().any(|report| report.addr == addr1),
        "the live member keeps being scraped"
    );
    assert!(
        later.iter().all(|report| report.addr != addr2),
        "the down member is not scraped any more"
    );
    assert!(later.iter().all(|report| report.outcome.is_ok()));

    drop(snapshots);
    tokio::time::timeout(BOUND, task)
        .await
        .expect("the supervisor ends with the snapshot sender")
        .expect("it does not panic");
}

#[tokio::test]
async fn readiness_is_probed_once_per_period_not_once_per_round() {
    let exporter = Exporter::start(&[200]).await;
    let addr = testkit::gossip_addr(1);
    let mut config = fast(ScrapeConfig::new(
        Vec::new(),
        vec![pin(addr, &exporter.url())],
    ));
    config.ready_every = Duration::from_millis(400);
    let (snapshots, watched) = watch::channel(Arc::new(testkit::snapshot(1)));
    let (tx, mut updates) = mpsc::channel(256);
    let task = tokio::spawn(scrape::run(config, watched, tx));

    let reports = reports_within(&mut updates, Duration::from_millis(1500)).await;
    let probed = reports
        .iter()
        .filter(|report| report.ready.is_some())
        .count();
    assert!(
        reports.len() >= 8,
        "a round every 50 ms: {} reports",
        reports.len()
    );
    assert!(reports[0].ready.is_some(), "the first round probes");
    assert!(
        (2..=5).contains(&probed),
        "a probe every 400 ms, not every round: {probed} of {}",
        reports.len()
    );

    drop(snapshots);
    tokio::time::timeout(BOUND, task).await.unwrap().unwrap();
}

#[tokio::test]
async fn members_sharing_a_url_get_one_collision_report_each_and_no_scrape() {
    let shared = Exporter::start(&[200]).await;
    let (addr1, addr2) = (testkit::gossip_addr(1), testkit::gossip_addr(2));
    let config = fast(ScrapeConfig::new(
        Vec::new(),
        vec![pin(addr1, &shared.url()), pin(addr2, &shared.url())],
    ));
    let (snapshots, watched) = watch::channel(two_members(MemberStatus::Live));
    let (tx, mut updates) = mpsc::channel(256);
    let task = tokio::spawn(scrape::run(config, watched, tx));

    let reports = reports_within(&mut updates, Duration::from_millis(600)).await;
    assert_eq!(reports.len(), 2, "one report per member, not one per round");
    for report in &reports {
        assert_eq!(report.outcome, Err(ScrapeError::Collision(shared.url())));
    }
    let mut reported: Vec<_> = reports.iter().map(|report| report.addr).collect();
    reported.sort();
    assert_eq!(reported, [addr1, addr2]);
    assert_eq!(shared.metrics_requests(), 0, "neither member is scraped");

    // The second member goes away: the first has the URL to itself.
    snapshots
        .send(Arc::new(ClusterSnapshot::new(
            "fixture",
            vec![testkit::member(1, MemberStatus::Live)],
            0,
        )))
        .unwrap();
    reports_from(&mut updates, &[addr1]).await;
    assert!(shared.metrics_requests() > 0);

    drop(snapshots);
    tokio::time::timeout(BOUND, task).await.unwrap().unwrap();
}

#[tokio::test]
async fn a_template_that_does_not_expand_is_reported_once_for_the_member() {
    let config = fast(ScrapeConfig::new(
        vec![
            sundog_lens::source::targets::UrlTemplate::parse("http://{ip}:{gossip_port-9000}/m")
                .unwrap(),
        ],
        Vec::new(),
    ));
    let (snapshots, watched) = watch::channel(Arc::new(testkit::snapshot(1)));
    let (tx, mut updates) = mpsc::channel(64);
    let task = tokio::spawn(scrape::run(config, watched, tx));
    let reports = reports_within(&mut updates, Duration::from_millis(400)).await;
    let [report] = reports.as_slice() else {
        panic!("one report, got {}", reports.len());
    };
    assert!(matches!(&report.outcome, Err(ScrapeError::Template(_))));
    drop(snapshots);
    tokio::time::timeout(BOUND, task).await.unwrap().unwrap();
}

fn loopback_config() -> ClusterConfig {
    ClusterConfig::default().with(|config| {
        let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        config.gossip_bind_addr = loopback;
        config.data_bind_addr = loopback;
    })
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_feed_scrapes_the_exporter_mapped_to_a_live_member_and_notices_it_go_quiet() {
    let name = "lens-scrape-feed";
    let no_seed: Option<SocketAddr> = None;
    let node = Cluster::builder(name)
        .seeds(no_seed)
        .config(loopback_config())
        .build()
        .await
        .expect("the node builds");
    let addr = node.local_gossip_addr();
    let exporter = Exporter::start(&[200]).await;

    let mut config = FeedConfig::new(name);
    config.bind = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    config.seeds = vec![Seed::Addr(addr)];
    config.scrape = Some(fast(ScrapeConfig::new(
        Vec::new(),
        vec![pin(addr, &exporter.url())],
    )));
    let mut feed = Feed::spawn(config).await.expect("the feed starts");
    let mut model = Model::new();

    pump(&mut feed, &mut model, "three folded scrapes", |model| {
        model
            .metrics(addr)
            .is_some_and(|metrics| metrics.folds() >= 3)
            && model.exporter(addr).and_then(ExporterState::ready) == Some(true)
    })
    .await;
    let metrics = model.metrics(addr).unwrap();
    assert_eq!(metrics.node(), node.node_id());
    assert_eq!(metrics.owned_parts("it"), Some(43_616.0));
    let tags: Vec<_> = model.events().iter().map(|e| e.kind.tag()).collect();
    assert!(tags.contains(&"EXPORTER"), "{tags:?}");
    assert!(!tags.contains(&"UNREACHABLE"), "{tags:?}");

    exporter.stop();
    pump(&mut feed, &mut model, "UNREACHABLE", |model| {
        model
            .events()
            .iter()
            .any(|e| matches!(e.kind, EventKind::Unreachable { node: n } if n == node.node_id()))
    })
    .await;
    assert!(model.exporter(addr).unwrap().unreachable());

    feed.shutdown().await;
    node.shutdown().await;
}

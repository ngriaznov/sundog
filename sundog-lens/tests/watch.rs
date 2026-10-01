//! The terminal loop end to end: a real in-process cluster, the feed, the
//! model and the interface, drawn onto a test backend and driven by scripted
//! keys and director commands.

use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use sundog::store::Mode;
use sundog::{Cluster, ClusterConfig};
use sundog_lens::app::{App, AppConfig, FleetCmd, UiCommand};
use sundog_lens::cli::Seed;
use sundog_lens::model::Model;
use sundog_lens::model::digest::ModelDigest;
use sundog_lens::source::{Feed, FeedConfig};
use sundog_lens::ui::View;
use sundog_lens::watch::{Session, drive};
use tokio::sync::{mpsc, watch};

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

fn key(c: char) -> Event {
    Event::Key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE))
}

fn screen(terminal: &Terminal<TestBackend>) -> String {
    let buffer = terminal.backend().buffer();
    (0..buffer.area.height)
        .map(|y| {
            (0..buffer.area.width)
                .map(|x| buffer[(x, y)].symbol())
                .collect::<String>()
                .trim_end()
                .to_owned()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

async fn feed_for(name: &str, seed: SocketAddr) -> Feed {
    let mut config = FeedConfig::new(name);
    config.bind = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    config.seeds = vec![Seed::Addr(seed)];
    Feed::spawn(config).await.expect("the feed starts")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_loop_draws_the_cluster_follows_keys_and_commands_and_quits() {
    let name = "lens-watch-loop";
    let a = node(name, None).await;
    let b = node(name, Some(a.local_gossip_addr())).await;
    let _caches = (open_it(&a).await, open_it(&b).await);
    let feed = feed_for(name, a.local_gossip_addr()).await;

    let mut app = App::new(AppConfig {
        demo: true,
        cluster: name.to_owned(),
        ..AppConfig::default()
    });
    app.set_observer(feed.observer_addr());
    let (command_tx, command_rx) = mpsc::unbounded_channel();
    let (fleet_tx, mut fleet_rx) = mpsc::unbounded_channel();
    let (input_tx, input_rx) = mpsc::unbounded_channel();
    let session = Session {
        feed,
        model: Model::new(),
        app,
        commands: Some(command_rx),
        fleet: Some(fleet_tx),
        exit_after: Some(Duration::from_secs(60)),
        digests: None,
    };
    let mut terminal = Terminal::new(TestBackend::new(140, 40)).expect("the test terminal");
    let driver = tokio::spawn(async move {
        let outcome = drive(&mut terminal, session, input_rx, std::future::pending()).await;
        (outcome, terminal)
    });

    // Before any member shows up the splash waits for gossip.
    command_tx
        .send(UiCommand::Caption(Some("scripted caption".into())))
        .unwrap();
    // Wait for both nodes, then drive the interface.
    tokio::time::sleep(Duration::from_secs(8)).await;
    input_tx.send(key('S')).unwrap();
    command_tx.send(UiCommand::Tab(View::Timeline)).unwrap();
    tokio::time::sleep(Duration::from_millis(600)).await;
    input_tx.send(Event::Resize(140, 40)).unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    input_tx.send(key('q')).unwrap();

    let (outcome, terminal) = tokio::time::timeout(Duration::from_secs(20), driver)
        .await
        .expect("the loop ends after q")
        .expect("the loop does not panic");
    outcome.expect("the loop ends cleanly");
    assert_eq!(fleet_rx.try_recv().unwrap(), FleetCmd::Spawn);
    let last = screen(&terminal);
    assert!(last.contains("sundog lens"), "{last}");
    assert!(last.contains(name), "{last}");
    assert!(last.contains("● 2 live"), "{last}");
    assert!(
        last.contains("Lifelines · last 2 min"),
        "the director's tab: {last}"
    );
    assert!(last.contains("▶ 0:"), "{last}");
    assert!(last.contains("scripted caption"), "{last}");
    assert!(last.contains("demo: S spawn"), "{last}");
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_loop_ends_at_exit_after_and_shows_the_splash_before_a_member_appears() {
    // Nobody listens on the discard port: the splash is all there is to draw.
    let feed = feed_for(
        "lens-watch-splash",
        SocketAddr::from((Ipv4Addr::LOCALHOST, 9)),
    )
    .await;
    let mut app = App::new(AppConfig {
        cluster: "lens-watch-splash".to_owned(),
        ..AppConfig::default()
    });
    app.set_observer(feed.observer_addr());
    let (_input_tx, input_rx) = mpsc::unbounded_channel();
    let session = Session {
        feed,
        model: Model::new(),
        app,
        commands: None,
        fleet: None,
        exit_after: Some(Duration::from_secs(2)),
        digests: None,
    };
    let mut terminal = Terminal::new(TestBackend::new(100, 30)).expect("the test terminal");
    let started = std::time::Instant::now();
    tokio::time::timeout(
        Duration::from_secs(20),
        drive(&mut terminal, session, input_rx, std::future::pending()),
    )
    .await
    .expect("the loop ends at exit-after")
    .expect("the loop ends cleanly");
    assert!(started.elapsed() >= Duration::from_secs(2));
    let last = screen(&terminal);
    assert!(
        last.contains("listening for gossip from \"lens-watch-splash\""),
        "{last}"
    );
    assert!(
        last.contains("the observer opens no cache and is never a peer"),
        "{last}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_loop_shuts_the_feed_down_and_returns_when_the_process_is_asked_to_stop() {
    let feed = feed_for(
        "lens-watch-stop",
        SocketAddr::from((Ipv4Addr::LOCALHOST, 9)),
    )
    .await;
    let app = App::new(AppConfig {
        cluster: "lens-watch-stop".to_owned(),
        ..AppConfig::default()
    });
    let (_input_tx, input_rx) = mpsc::unbounded_channel();
    let session = Session {
        feed,
        model: Model::new(),
        app,
        commands: None,
        fleet: None,
        exit_after: None,
        digests: None,
    };
    let mut terminal = Terminal::new(TestBackend::new(100, 30)).expect("the test terminal");
    tokio::time::timeout(
        Duration::from_secs(20),
        drive(&mut terminal, session, input_rx, std::future::ready(())),
    )
    .await
    .expect("a resolved stop future ends the loop")
    .expect("the loop ends cleanly");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_digest_follows_the_cluster_and_a_quit_command_ends_the_loop() {
    let name = "lens-watch-digest";
    let a = node(name, None).await;
    let b = node(name, Some(a.local_gossip_addr())).await;
    let _caches = (open_it(&a).await, open_it(&b).await);
    let feed = feed_for(name, a.local_gossip_addr()).await;
    let app = App::new(AppConfig {
        demo: true,
        cluster: name.to_owned(),
        ..AppConfig::default()
    });
    let (command_tx, command_rx) = mpsc::unbounded_channel();
    let (digest_tx, mut digest_rx) = watch::channel(ModelDigest::default());
    let (_input_tx, input_rx) = mpsc::unbounded_channel();
    let session = Session {
        feed,
        model: Model::new(),
        app,
        commands: Some(command_rx),
        fleet: None,
        exit_after: Some(Duration::from_secs(60)),
        digests: Some(digest_tx),
    };
    let mut terminal = Terminal::new(TestBackend::new(140, 40)).expect("the test terminal");
    let driver = tokio::spawn(async move {
        drive(&mut terminal, session, input_rx, std::future::pending()).await
    });

    let reached = tokio::time::timeout(
        Duration::from_secs(30),
        digest_rx.wait_for(|digest| digest.live == 2 && digest.view_hash.contains_key("it")),
    )
    .await
    .expect("the digest shows both nodes and the view within the bound")
    .expect("the digest channel stays open");
    assert_eq!(reached.statuses.len(), 2);
    drop(reached);

    // Without a quit the loop keeps running.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!driver.is_finished());
    let started = std::time::Instant::now();
    command_tx.send(UiCommand::Quit).unwrap();
    tokio::time::timeout(Duration::from_secs(20), driver)
        .await
        .expect("the loop ends after the quit command")
        .expect("the loop does not panic")
        .expect("the loop ends cleanly");
    assert!(started.elapsed() < Duration::from_secs(10));
    a.shutdown().await;
    b.shutdown().await;
}

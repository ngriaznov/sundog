//! `demo`: the fleet, the director and the interface in one process.
//!
//! The demo starts the observer, a [`FleetStage`] of local test nodes and the
//! scenario [`Director`], and shows the lens while the director plays. The
//! observer binds `127.0.0.1:0`, joins through the first two slots, scrapes
//! each node's exporter and names every node by its slot.
//!
//! With `--headless` nothing is drawn: the demo prints every step and every
//! event to the standard output and exits 1 when an await times out. That run
//! is the end-to-end check of the whole stack: real processes, gossip,
//! scrape, model and director.

use std::fs::File;
use std::io::{self, Write};
use std::path::Path;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, bail};
use tokio::sync::{mpsc, watch};

use crate::app::{App, AppConfig};
use crate::cli::{DemoArgs, ScenarioSource, Seed};
use crate::fleet::proc::METRICS_PORT;
use crate::fleet::{Fleet, FleetConfig, FleetStage, MAX_SLOTS, SlotInfo, load};
use crate::model::Model;
use crate::model::digest::ModelDigest;
use crate::model::events::Event;
use crate::scenario::director::{Director, Options};
use crate::scenario::{self, Scenario};
use crate::source::scrape::ScrapeConfig;
use crate::source::targets::UrlTemplate;
use crate::source::{Feed, FeedConfig, Update};
use crate::ui::look::Look;
use crate::ui::{Ctx, LayoutKind, Scene};
use crate::watch::{Session, publish_digest, run_session};

/// How often the demo scrapes each exporter.
pub const SCRAPE_INTERVAL: Duration = Duration::from_secs(1);

/// The time between ticks of the headless loop.
const HEADLESS_TICK: Duration = Duration::from_millis(50);

/// The scrape URL template: every node serves its exporter on the same port
/// at its own address.
#[must_use]
pub fn metrics_template() -> String {
    format!("http://{{ip}}:{METRICS_PORT}/metrics")
}

/// What the observer of the demo watches: the fleet's cluster through its
/// first two slots, on a loopback port of its own, with each node's exporter
/// scraped and every slot address named by its label.
///
/// # Errors
///
/// Returns an error when the metrics template does not parse.
pub fn feed_config(config: &FleetConfig) -> anyhow::Result<FeedConfig> {
    let mut feed = FeedConfig::new(config.cluster.as_str());
    feed.bind = std::net::SocketAddr::from(([127, 0, 0, 1], 0));
    feed.seeds = config.seed_addrs().into_iter().map(Seed::Addr).collect();
    let template = UrlTemplate::parse(&metrics_template()).context("the metrics template")?;
    let mut scrape = ScrapeConfig::new(vec![template], Vec::new());
    scrape.interval = SCRAPE_INTERVAL;
    feed.scrape = Some(scrape);
    for slot in 1..=MAX_SLOTS {
        if let Some(info) = SlotInfo::new(config.base_ip, slot) {
            feed.hint(info.gossip, info.label);
        }
    }
    Ok(feed)
}

/// The scenario a `--scenario` flag names: the built-in tour or a file.
///
/// # Errors
///
/// Returns an error when the file cannot be read, or when a line is not a
/// step; the message names the file and the line.
pub fn load_scenario(source: &ScenarioSource) -> anyhow::Result<Scenario> {
    match source {
        ScenarioSource::Tour => {
            scenario::parse(scenario::TOUR).context("the built-in tour does not parse")
        }
        ScenarioSource::File(path) => {
            let text = std::fs::read_to_string(path)
                .with_context(|| format!("reading the scenario {}", path.display()))?;
            scenario::parse(&text)
                .map_err(|error| anyhow::anyhow!("scenario {}: {error}", path.display()))
        }
    }
}

/// A model that names the slots as the feed does.
fn model_for(feed: &Feed, started: SystemTime) -> Model {
    let mut model = Model::new();
    for (addr, label) in feed.hints() {
        model.set_label_hint(*addr, label.clone());
    }
    model.set_scrape_interval(SCRAPE_INTERVAL);
    model.set_started(started);
    model
}

/// Opens the `--marks` file for writing.
fn open_marks(path: Option<&Path>) -> anyhow::Result<Option<File>> {
    path.map(|path| {
        File::create(path).with_context(|| format!("creating the marks file {}", path.display()))
    })
    .transpose()
}

/// Runs `demo`: checks the machine, plays the scenario against a local fleet
/// and stops every node afterwards, whichever way the run ends.
///
/// # Errors
///
/// Returns an error when the scenario does not load, the machine cannot run
/// the fleet, a node does not start, or, with `--headless`, a step fails or
/// an await times out.
pub async fn run(args: DemoArgs) -> anyhow::Result<()> {
    // The origin of every stamp the run prints and writes, taken before the
    // checks so the marks line up with the start of a recording.
    let started = Instant::now();
    // The interface owns the terminal, so the warnings of a run (a key that
    // failed, a step that failed, an await that timed out) reach `--log`.
    crate::watch::init_logging(args.log.as_deref())?;
    let scenario = load_scenario(&args.scenario)?;
    let config = FleetConfig::from_args(&args.fleet)?;
    let feed_config = feed_config(&config)?;
    let marks = open_marks(args.marks.as_deref())?;
    let fleet = Fleet::new(config);
    // The preflight starts no process: a signal during it needs no cleanup.
    fleet.preflight().await?;
    let (load, handle) = load::Load::spawn(args.fleet.rate, args.fleet.keys);
    let stage = FleetStage::new(fleet, handle);
    // A signal at any point after that still reaches `stop_all` below.
    let outcome = tokio::select! {
        outcome = async {
            if args.headless {
                run_headless(&scenario, &stage, feed_config, marks, started).await
            } else {
                run_ui(&args, &scenario, &stage, feed_config, marks, started).await
            }
        } => outcome,
        () = crate::watch::termination() => signal_outcome(args.headless),
    };
    if outcome.is_err() {
        for failure in stage.startup_failures() {
            eprintln!("node did not start: {failure}");
        }
    }
    eprintln!("stopping the nodes");
    stage.stop_all().await;
    load.shutdown();
    outcome
}

/// The interface configuration of the demo: captions and demo keys on, and the
/// seeds the observer joins through named on the splash.
fn app_config(args: &DemoArgs, seeds: &[String]) -> AppConfig {
    AppConfig {
        look: Look::from_args(&args.display),
        forget_after: Duration::from_secs(90),
        anim: !args.display.no_anim,
        demo: true,
        captions: !args.no_captions,
        scrape_interval: Some(SCRAPE_INTERVAL),
        cluster: args.fleet.name.clone(),
        seeds: seeds.to_vec(),
        observer: None,
    }
}

/// How a run ends when the process is asked to stop: a headless run did not
/// finish its scenario and fails; an interface run is closed by the user.
fn signal_outcome(headless: bool) -> anyhow::Result<()> {
    if headless {
        bail!("stopped by a signal");
    }
    Ok(())
}

/// Plays `scenario` under the interface. The run ends when the user quits,
/// the scenario's `quit` step runs, or the process is asked to stop.
async fn run_ui(
    args: &DemoArgs,
    scenario: &Scenario,
    stage: &FleetStage,
    feed_config: FeedConfig,
    mut marks: Option<File>,
    started: Instant,
) -> anyhow::Result<()> {
    let seeds: Vec<String> = feed_config.seeds.iter().map(ToString::to_string).collect();
    let wall = SystemTime::now();
    let feed = Feed::spawn(feed_config).await?;
    let mut app = App::new(app_config(args, &seeds));
    app.set_observer(feed.observer_addr());
    let model = model_for(&feed, wall);

    let (commands_tx, commands_rx) = mpsc::unbounded_channel();
    let (fleet_tx, mut fleet_rx) = mpsc::unbounded_channel();
    let (digest_tx, digest_rx) = watch::channel(ModelDigest::default());
    let keys = {
        let stage = stage.clone();
        tokio::spawn(async move {
            while let Some(command) = fleet_rx.recv().await {
                if let Err(error) = stage.apply(command).await {
                    tracing::warn!("a demo key failed: {error:#}");
                }
            }
        })
    };
    let director = Director::new(
        stage.clone(),
        digest_rx,
        Some(commands_tx),
        Options {
            headless: false,
            captions: !args.no_captions,
        },
    );
    let session = Session {
        feed,
        model,
        app,
        commands: Some(commands_rx),
        fleet: Some(fleet_tx),
        exit_after: None,
        digests: Some(digest_tx),
    };
    let ui = run_session(session);
    tokio::pin!(ui);
    let mut log = io::sink();
    let play = director.play(
        scenario,
        tokio::time::Instant::from_std(started),
        &mut log,
        marks.as_mut().map(|file| file as &mut dyn Write),
    );
    tokio::pin!(play);
    let mut played = false;
    let result = loop {
        tokio::select! {
            result = &mut ui => break result,
            outcome = &mut play, if !played => {
                played = true;
                if let Err(error) = outcome {
                    tracing::warn!("the scenario stopped: {error}");
                }
            }
        }
    };
    keys.abort();
    result
}

/// Writes `events` to `out` as the log lines of a headless run.
///
/// # Errors
///
/// Returns the error of the first failed write, which is a closed pipe when
/// whoever reads the log has gone.
fn print_events(
    model: &Model,
    events: &[Event],
    started: Instant,
    out: &mut impl Write,
) -> io::Result<()> {
    if events.is_empty() {
        return Ok(());
    }
    let app = App::new(AppConfig::default());
    let ctx = Ctx {
        now: Instant::now(),
        wall: SystemTime::now(),
        elapsed: started.elapsed(),
    };
    let scene = Scene {
        app: &app,
        model,
        ctx: &ctx,
        look: Look::default(),
        kind: LayoutKind::Full,
    };
    for event in events {
        writeln!(out, "{}", event_line(&scene, event, started.elapsed()))?;
    }
    Ok(())
}

/// One event as a log line: the time into the run, then the event row as the
/// interface draws it, without styles.
#[must_use]
pub fn event_line(scene: &Scene<'_>, event: &Event, elapsed: Duration) -> String {
    let row = crate::ui::eventlog::line(scene, event);
    let text: String = row.spans.iter().map(|span| span.content.as_ref()).collect();
    format!("[{:7.1}s]   {}", elapsed.as_secs_f64(), text.trim_end())
}

/// Plays `scenario` with no interface, printing steps and events.
async fn run_headless(
    scenario: &Scenario,
    stage: &FleetStage,
    feed_config: FeedConfig,
    mut marks: Option<File>,
    started: Instant,
) -> anyhow::Result<()> {
    let mut feed = Feed::spawn(feed_config).await?;
    let mut model = model_for(&feed, SystemTime::now());
    let (digest_tx, digest_rx) = watch::channel(ModelDigest::default());
    let director = Director::new(
        stage.clone(),
        digest_rx,
        None,
        Options {
            headless: true,
            captions: false,
        },
    );
    let mut stdout = io::stdout();
    let play = director.play(
        scenario,
        tokio::time::Instant::from_std(started),
        &mut stdout,
        marks.as_mut().map(|file| file as &mut dyn Write),
    );
    tokio::pin!(play);
    // A signal here shuts the feed down; one before this point is caught by
    // `run`, which has nothing to shut down yet.
    let stop = crate::watch::termination();
    tokio::pin!(stop);
    let mut tick = tokio::time::interval(HEADLESS_TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut sources_open = true;
    let outcome = loop {
        tokio::select! {
            outcome = &mut play => break outcome.map_err(anyhow::Error::from),
            () = &mut stop => break Err(anyhow::anyhow!("stopped by a signal")),
            update = feed.recv(), if sources_open => {
                let Some(update) = update else {
                    sources_open = false;
                    continue;
                };
                let mut out = io::stdout();
                let mut written = fold(&mut model, update, started, &mut out);
                while written.is_ok() && let Some(update) = feed.try_recv() {
                    written = fold(&mut model, update, started, &mut out);
                }
                if let Err(error) = written {
                    break Err(log_failure(&error));
                }
                publish_digest(&model, &digest_tx);
            }
            _ = tick.tick() => {
                let events = model.tick(Instant::now());
                if let Err(error) = print_events(&model, &events, started, &mut io::stdout()) {
                    break Err(log_failure(&error));
                }
                publish_digest(&model, &digest_tx);
            }
        }
    };
    feed.shutdown().await;
    let outcome = outcome?;
    if !outcome.timeouts.is_empty() || !outcome.failures.is_empty() {
        bail!("the scenario ended with timeouts or failures");
    }
    writeln!(
        io::stdout(),
        "[{:7.1}s] headless run passed",
        started.elapsed().as_secs_f64()
    )
    .map_err(|error| log_failure(&error))
}

/// The error of a headless run whose log cannot be written.
fn log_failure(error: &io::Error) -> anyhow::Error {
    anyhow::anyhow!("writing the log: {error}")
}

/// Folds `update` into `model` and writes the events it raises to `out`.
fn fold(
    model: &mut Model,
    update: Update,
    started: Instant,
    out: &mut impl Write,
) -> io::Result<()> {
    let events = model.apply(update, Instant::now(), SystemTime::now());
    print_events(model, &events, started, out)
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;
    use std::num::NonZeroU8;
    use std::path::PathBuf;

    use super::*;
    use crate::model::testkit;

    fn config() -> FleetConfig {
        FleetConfig {
            cluster: "lens-demo".to_owned(),
            base_ip: Ipv4Addr::new(127, 0, 0, 11),
            testnode: PathBuf::from("sundog-testnode"),
            owners: NonZeroU8::new(2).unwrap(),
            logs: PathBuf::from("target/lens-demo"),
        }
    }

    #[test]
    fn the_template_reaches_each_nodes_exporter_by_its_address() {
        assert_eq!(metrics_template(), "http://{ip}:9090/metrics");
        assert!(UrlTemplate::parse(&metrics_template()).is_ok());
    }

    #[test]
    fn the_observer_joins_through_the_first_two_slots_and_scrapes_every_node() {
        let feed = feed_config(&config()).unwrap();
        assert_eq!(feed.cluster, "lens-demo");
        assert_eq!(feed.bind, "127.0.0.1:0".parse().unwrap());
        assert_eq!(
            feed.seeds,
            [
                Seed::Addr("127.0.0.11:7946".parse().unwrap()),
                Seed::Addr("127.0.0.12:7946".parse().unwrap())
            ]
        );
        let scrape = feed.scrape.expect("the demo scrapes");
        assert_eq!(scrape.templates.len(), 1);
        assert!(scrape.pins.is_empty(), "{:?}", scrape.pins);
        assert_eq!(scrape.interval, SCRAPE_INTERVAL);
    }

    fn demo_args(extra: &[&str]) -> DemoArgs {
        let mut args = vec!["demo"];
        args.extend_from_slice(extra);
        match crate::cli::parse(args).unwrap() {
            crate::cli::Command::Demo(args) => args,
            other => panic!("not a demo: {other:?}"),
        }
    }

    #[test]
    fn the_splash_names_the_seeds_the_observer_joins_through() {
        let args = demo_args(&[]);
        let feed = feed_config(&config()).unwrap();
        let seeds: Vec<String> = feed.seeds.iter().map(ToString::to_string).collect();
        let app_config = app_config(&args, &seeds);
        assert_eq!(app_config.seeds, ["127.0.0.11:7946", "127.0.0.12:7946"]);
        assert!(app_config.demo && app_config.captions);
        assert_eq!(app_config.cluster, "lens-demo");

        let app = App::new(app_config);
        let model = Model::new();
        let ctx = Ctx {
            now: Instant::now(),
            wall: SystemTime::UNIX_EPOCH,
            elapsed: Duration::ZERO,
        };
        let scene = Scene {
            app: &app,
            model: &model,
            ctx: &ctx,
            look: Look::default(),
            kind: LayoutKind::Full,
        };
        let text: String = crate::ui::splash::lines(&scene)
            .iter()
            .flat_map(|line| line.spans.iter().map(|span| span.content.as_ref()))
            .collect();
        assert!(
            text.contains("seeds 127.0.0.11:7946, 127.0.0.12:7946"),
            "{text}"
        );
        assert!(!text.contains("mDNS"), "{text}");
    }

    #[test]
    fn captions_follow_the_flag_and_a_signal_fails_only_a_headless_run() {
        assert!(!app_config(&demo_args(&["--no-captions"]), &[]).captions);
        assert!(signal_outcome(true).is_err());
        assert!(signal_outcome(false).is_ok());
    }

    #[test]
    fn every_slot_address_is_named_by_its_label() {
        let feed = feed_config(&config()).unwrap();
        assert_eq!(feed.hints.len(), MAX_SLOTS);
        assert_eq!(feed.hints[0].0, "127.0.0.11:7946".parse().unwrap());
        assert_eq!(feed.hints[0].1, "n1");
        assert_eq!(feed.hints[5].0, "127.0.0.16:7946".parse().unwrap());
        assert_eq!(feed.hints[5].1, "n6");
    }

    #[test]
    fn the_tour_loads_and_a_scenario_file_names_its_bad_line() {
        let tour = load_scenario(&ScenarioSource::Tour).unwrap();
        assert!(!tour.steps.is_empty(), "{:?}", tour.steps);

        let dir = std::env::temp_dir().join(format!("sundog-lens-demo-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let good = dir.join("good.txt");
        std::fs::write(&good, "pause 1s\nquit\n").unwrap();
        assert_eq!(
            load_scenario(&ScenarioSource::File(good))
                .unwrap()
                .steps
                .len(),
            2
        );

        let bad = dir.join("bad.txt");
        std::fs::write(&bad, "pause 1s\nfrobnicate\n").unwrap();
        let error = load_scenario(&ScenarioSource::File(bad.clone()))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("bad.txt") && error.contains("line 2"),
            "{error}"
        );

        let error = load_scenario(&ScenarioSource::File(dir.join("missing.txt")))
            .unwrap_err()
            .to_string();
        assert!(error.contains("missing.txt"), "{error}");
    }

    #[test]
    fn a_marks_path_opens_a_file_and_no_path_opens_nothing() {
        assert!(open_marks(None).unwrap().is_none());
        let dir = std::env::temp_dir().join(format!("sundog-lens-marks-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("marks.txt");
        assert!(open_marks(Some(&path)).unwrap().is_some());
        assert!(path.exists());
        assert!(open_marks(Some(&dir.join("no/such/dir/marks.txt"))).is_err());
    }

    /// A writer that fails like a closed pipe once `allow` bytes are written.
    struct Closing {
        allow: usize,
        written: Vec<u8>,
    }

    impl Write for Closing {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            if self.written.len() >= self.allow {
                return Err(io::ErrorKind::BrokenPipe.into());
            }
            self.written.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn fixture_events(model: &Model) -> Vec<Event> {
        model.events().iter().take(3).cloned().collect()
    }

    #[test]
    fn events_print_one_line_each() {
        let model = testkit::fixture_model(Instant::now());
        let events = fixture_events(&model);
        assert_eq!(events.len(), 3);
        let mut out = Vec::new();
        print_events(&model, &events, Instant::now(), &mut out).unwrap();
        let text = String::from_utf8(out).unwrap();
        assert_eq!(text.lines().count(), 3, "{text}");
        let mut none = Vec::new();
        print_events(&model, &[], Instant::now(), &mut none).unwrap();
        assert!(none.is_empty(), "{none:?}");
    }

    #[test]
    fn a_closed_log_is_an_error_instead_of_a_panic() {
        let model = testkit::fixture_model(Instant::now());
        let events = fixture_events(&model);
        let mut closed = Closing {
            allow: 0,
            written: Vec::new(),
        };
        let error = print_events(&model, &events, Instant::now(), &mut closed).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        let failure = log_failure(&error);
        assert!(
            failure.to_string().starts_with("writing the log: "),
            "{failure}"
        );
    }

    #[test]
    fn folding_an_update_prints_its_events_and_reports_a_closed_log() {
        let (model, now) = testkit::past_discovery(Instant::now());
        let update = || Update::Snapshot(std::sync::Arc::new(testkit::snapshot(2)), now);
        let mut joined = model.clone();
        let mut out = Vec::new();
        fold(&mut joined, update(), now, &mut out).unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("JOIN"), "{text}");
        let mut closed = Closing {
            allow: 0,
            written: Vec::new(),
        };
        let mut other = model;
        let error = fold(&mut other, update(), now, &mut closed).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    }

    #[test]
    fn an_event_line_has_the_time_into_the_run_and_the_row_text() {
        let model = testkit::fixture_model(Instant::now());
        let app = App::new(AppConfig::default());
        let ctx = Ctx {
            now: Instant::now(),
            wall: SystemTime::now(),
            elapsed: Duration::ZERO,
        };
        let scene = Scene {
            app: &app,
            model: &model,
            ctx: &ctx,
            look: Look::default(),
            kind: LayoutKind::Full,
        };
        let event = model
            .events()
            .iter()
            .next()
            .cloned()
            .expect("the fixture model has events");
        let line = event_line(&scene, &event, Duration::from_millis(12_340));
        assert!(line.starts_with("[   12.3s]   "), "{line}");
        assert!(
            line.contains(crate::ui::eventlog::tag(&event.kind)),
            "{line}"
        );
        assert!(!line.contains('\u{1b}'), "no escape codes: {line}");
        assert_eq!(line, line.trim_end());
    }
}

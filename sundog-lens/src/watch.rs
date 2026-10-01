//! The terminal loop shared by `watch` and `demo`.
//!
//! One task owns the model and the interface state. Keys arrive from a
//! blocking thread, updates from the [`Feed`], director commands from the
//! demo. A tick every 50 ms advances motion and the model's clock; a frame is
//! drawn at most once per tick, and only when something changed or moves.

use std::io::IsTerminal;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, bail};
use crossterm::event::{self, Event};
use ratatui::Terminal;
use ratatui::backend::Backend;
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;

use crate::app::{Action, App, AppConfig, FleetCmd, UiCommand};
use crate::cli::WatchArgs;
use crate::model::Model;
use crate::source::{Feed, FeedConfig, Update};
use crate::ui::look::Look;
use crate::ui::{self, Ctx};

/// The time between ticks, and the shortest time between frames.
pub const TICK: Duration = Duration::from_millis(50);

/// How many updates one pass of the loop folds in before it draws.
const BATCH: usize = 256;

/// Everything one run of the interface owns.
#[derive(Debug)]
pub struct Session {
    /// The sources of updates.
    pub feed: Feed,
    /// The model the updates fold into.
    pub model: Model,
    /// The interface state.
    pub app: App,
    /// Commands from the scenario director, in demo mode.
    pub commands: Option<mpsc::UnboundedReceiver<UiCommand>>,
    /// Where fleet requests from the demo keys go, in demo mode.
    pub fleet: Option<mpsc::UnboundedSender<FleetCmd>>,
    /// Quit after this long.
    pub exit_after: Option<Duration>,
}

/// The interface configuration `args` ask for.
#[must_use]
pub fn app_config(args: &WatchArgs, look: Look) -> AppConfig {
    let scraping = !args.metrics.is_empty() || !args.scrape.is_empty();
    AppConfig {
        look,
        forget_after: args.forget_after,
        anim: !args.display.no_anim,
        demo: false,
        captions: false,
        scrape_interval: scraping.then_some(args.interval),
        cluster: args.cluster.clone(),
        seeds: args.seeds.iter().map(ToString::to_string).collect(),
        observer: None,
    }
}

/// Writes tracing output to `path`. With no `--log`, tracing stays silent:
/// the terminal belongs to the interface.
///
/// # Errors
///
/// Returns an error when `path` cannot be created.
pub fn init_logging(path: Option<&std::path::Path>) -> anyhow::Result<()> {
    let Some(path) = path else { return Ok(()) };
    let file = std::fs::File::create(path)
        .with_context(|| format!("creating the log file {}", path.display()))?;
    tracing_subscriber::fmt()
        .with_writer(std::sync::Mutex::new(file))
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .try_init()
        .map_err(|error| anyhow::anyhow!("installing the log writer: {error}"))
}

/// Runs `watch`: joins the cluster and shows it until the user quits.
///
/// # Errors
///
/// Returns an error when the standard output is not a terminal, when the
/// observer cannot start, or when the terminal fails.
pub async fn run(args: WatchArgs) -> anyhow::Result<()> {
    if !std::io::stdout().is_terminal() {
        bail!("watch needs a terminal; use --once for a report");
    }
    init_logging(args.log.as_deref())?;
    let look = Look::from_args(&args.display);
    let config = FeedConfig::try_from(&args).context("reading --metrics")?;
    let feed = Feed::spawn(config).await?;
    let mut app = App::new(app_config(&args, look));
    app.set_observer(feed.observer_addr());
    run_session(Session {
        feed,
        model: Model::new(),
        app,
        commands: None,
        fleet: None,
        exit_after: args.exit_after,
    })
    .await
}

/// The next director command, or never when there is no director.
async fn next_command(
    commands: &mut Option<mpsc::UnboundedReceiver<UiCommand>>,
) -> Option<UiCommand> {
    match commands {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

/// Reads terminal events on a blocking thread until told to stop.
fn spawn_input(stop: Arc<AtomicBool>) -> mpsc::UnboundedReceiver<Event> {
    let (tx, rx) = mpsc::unbounded_channel();
    std::thread::spawn(move || {
        while !stop.load(Ordering::Relaxed) {
            match event::poll(Duration::from_millis(100)) {
                Ok(true) => match event::read() {
                    Ok(event) => {
                        if tx.send(event).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                },
                Ok(false) => {}
                Err(_) => break,
            }
        }
    });
    rx
}

/// Restores the terminal when dropped, including on a panic unwinding past
/// the loop.
struct Restore;

impl Drop for Restore {
    fn drop(&mut self) {
        ratatui::restore();
    }
}

/// What a pass of the loop decided.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Pass {
    /// Whether the screen must be drawn again.
    pub dirty: bool,
    /// Whether the user asked to quit.
    pub quit: bool,
}

/// Folds one terminal event into the interface. A key may quit, draw or ask
/// the fleet to act.
pub fn handle_event(
    event: &Event,
    app: &mut App,
    model: &Model,
    fleet: Option<&mpsc::UnboundedSender<FleetCmd>>,
) -> Pass {
    match event {
        Event::Key(key) => match app.handle_key(*key, model) {
            Action::None => Pass::default(),
            Action::Redraw => Pass {
                dirty: true,
                quit: false,
            },
            Action::Quit => Pass {
                dirty: true,
                quit: true,
            },
            Action::Fleet(command) => {
                if let Some(fleet) = fleet {
                    let _ = fleet.send(command);
                }
                Pass {
                    dirty: true,
                    quit: false,
                }
            }
        },
        Event::Resize(..) | Event::FocusGained => Pass {
            dirty: true,
            quit: false,
        },
        _ => Pass::default(),
    }
}

/// The clock a frame is drawn at: now, or the instant a frozen display stopped
/// at.
fn frame_ctx(app: &App, model: &Model, start: Instant) -> Ctx {
    let shown = app.shown(model);
    Ctx {
        now: shown.now().unwrap_or_else(Instant::now),
        wall: shown.wall().unwrap_or_else(SystemTime::now),
        elapsed: start.elapsed(),
    }
}

/// Runs the interface on the real terminal until the user quits,
/// `exit_after` passes or the sources stop. The terminal is restored on every
/// path out.
///
/// # Errors
///
/// Returns an error when the terminal cannot be set up or drawn to.
pub async fn run_session(session: Session) -> anyhow::Result<()> {
    let mut terminal = ratatui::try_init().context("setting up the terminal")?;
    let _restore = Restore;
    let stop = Arc::new(AtomicBool::new(false));
    let input = spawn_input(Arc::clone(&stop));
    let result = tokio::select! {
        result = drive(&mut terminal, session, input) => result,
        () = termination() => Ok(()),
    };
    stop.store(true, Ordering::Relaxed);
    result
}

/// Resolves when the process is asked to stop from outside: SIGTERM or SIGHUP
/// on Unix, Ctrl-C elsewhere. Ctrl-C at the keyboard is a key event in raw
/// mode, not a signal. If the handlers cannot be installed this never
/// resolves.
pub async fn termination() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let (Ok(mut terminate), Ok(mut hangup)) = (
            signal(SignalKind::terminate()),
            signal(SignalKind::hangup()),
        ) else {
            return std::future::pending().await;
        };
        tokio::select! {
            _ = terminate.recv() => {}
            _ = hangup.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        if tokio::signal::ctrl_c().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

/// The loop itself, over any backend and any source of terminal events: folds
/// updates into the model, handles events, advances motion and draws, until a
/// quit event, `exit_after` or the end of the sources. It shuts the feed down
/// before it returns.
///
/// # Errors
///
/// Returns an error when drawing to the backend fails.
pub async fn drive<B>(
    terminal: &mut Terminal<B>,
    mut session: Session,
    mut input: mpsc::UnboundedReceiver<Event>,
) -> anyhow::Result<()>
where
    B: Backend,
    B::Error: Send + Sync + 'static,
{
    let start = Instant::now();
    let mut tick = tokio::time::interval(TICK);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut dirty = true;
    let mut last_second = 0;
    let mut last_tick = start;
    let mut sources_open = true;
    let result: anyhow::Result<()> = loop {
        tokio::select! {
            Some(event) = input.recv() => {
                let pass = handle_event(
                    &event,
                    &mut session.app,
                    &session.model,
                    session.fleet.as_ref(),
                );
                dirty |= pass.dirty;
                if pass.quit {
                    break Ok(());
                }
            }
            update = session.feed.recv(), if sources_open => {
                let Some(update) = update else {
                    sources_open = false;
                    continue;
                };
                fold(&mut session, update);
                for _ in 0..BATCH {
                    let Some(update) = session.feed.try_recv() else { break };
                    fold(&mut session, update);
                }
                dirty = true;
            }
            Some(command) = next_command(&mut session.commands) => {
                session.app.apply_director(command, &session.model);
                dirty = true;
            }
            _ = tick.tick() => {
                let now = Instant::now();
                let dt = now.saturating_duration_since(last_tick);
                last_tick = now;
                if !session.model.tick(now).is_empty() {
                    dirty = true;
                }
                session.app.observe(&session.model, now);
                session.app.step(dt);
                let second = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map_or(0, |d| d.as_secs());
                if second != last_second {
                    last_second = second;
                    dirty = true;
                }
                if session.app.animating(&session.model, now, SystemTime::now()) {
                    dirty = true;
                }
                if session.app.anim && session.model.snapshot().is_none_or(|s| s.members.is_empty()) {
                    dirty = true;
                }
                if session.exit_after.is_some_and(|after| start.elapsed() >= after) {
                    break Ok(());
                }
                if dirty {
                    let ctx = frame_ctx(&session.app, &session.model, start);
                    if let Err(error) = terminal.draw(|frame| {
                        ui::draw(frame, &session.app, &session.model, &ctx);
                    }) {
                        break Err(anyhow::anyhow!("drawing the screen: {error}"));
                    }
                    dirty = false;
                }
            }
        }
    };
    session.feed.shutdown().await;
    result
}

/// Folds one update into the model and tells the interface.
fn fold(session: &mut Session, update: Update) {
    let now = Instant::now();
    session.model.apply(update, now, SystemTime::now());
    session.app.observe(&session.model, now);
}

#[cfg(test)]
mod tests {
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    use super::*;
    use crate::cli::{self, Command};

    fn args(extra: &[&str]) -> WatchArgs {
        let mut all = vec!["watch", "prod"];
        all.extend_from_slice(extra);
        let Command::Watch(args) = cli::parse(all).unwrap() else {
            panic!("a watch command");
        };
        args
    }

    fn key(c: char) -> Event {
        Event::Key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE))
    }

    #[test]
    fn the_app_config_follows_the_arguments() {
        let config = app_config(
            &args(&[
                "--seed",
                "10.0.0.1:7946",
                "--forget-after",
                "30s",
                "--no-anim",
            ]),
            Look::default(),
        );
        assert_eq!(config.cluster, "prod");
        assert_eq!(config.seeds, ["10.0.0.1:7946"]);
        assert_eq!(config.forget_after, Duration::from_secs(30));
        assert!(!config.anim);
        assert!(!config.demo);
        assert_eq!(config.scrape_interval, None);
    }

    #[test]
    fn scraping_shows_in_the_config_only_when_asked_for() {
        let with_template = app_config(
            &args(&["--metrics", "http://{ip}:9090/metrics", "--interval", "2s"]),
            Look::default(),
        );
        assert_eq!(with_template.scrape_interval, Some(Duration::from_secs(2)));
        let with_pin = app_config(&args(&["--scrape", "n1=http://h:1/m"]), Look::default());
        assert!(with_pin.scrape_interval.is_some());
    }

    #[test]
    fn keys_quit_redraw_or_do_nothing() {
        let model = Model::new();
        let mut app = App::new(AppConfig::default());
        let quit = handle_event(&key('q'), &mut app, &model, None);
        assert_eq!(
            quit,
            Pass {
                dirty: true,
                quit: true
            }
        );
        let redraw = handle_event(&key('2'), &mut app, &model, None);
        assert_eq!(
            redraw,
            Pass {
                dirty: true,
                quit: false
            }
        );
        let nothing = handle_event(&key('z'), &mut app, &model, None);
        assert_eq!(nothing, Pass::default());
    }

    #[test]
    fn a_resize_redraws_and_other_events_are_ignored() {
        let model = Model::new();
        let mut app = App::new(AppConfig::default());
        assert!(handle_event(&Event::Resize(100, 40), &mut app, &model, None).dirty);
        assert!(handle_event(&Event::FocusGained, &mut app, &model, None).dirty);
        assert_eq!(
            handle_event(&Event::FocusLost, &mut app, &model, None),
            Pass::default()
        );
    }

    #[test]
    fn fleet_keys_reach_the_fleet_channel_in_demo_mode() {
        let model = crate::model::testkit::fixture_model(Instant::now());
        let mut app = App::new(AppConfig {
            demo: true,
            ..AppConfig::default()
        });
        let (tx, mut rx) = mpsc::unbounded_channel();
        let pass = handle_event(&key('S'), &mut app, &model, Some(&tx));
        assert!(pass.dirty && !pass.quit);
        assert_eq!(rx.try_recv().unwrap(), FleetCmd::Spawn);
        // Without a channel the request is dropped, not an error.
        assert!(handle_event(&key('S'), &mut app, &model, None).dirty);
    }

    #[test]
    fn a_frozen_display_draws_at_the_instant_it_froze() {
        let model = crate::model::testkit::fixture_model(Instant::now());
        let mut app = App::new(AppConfig::default());
        let start = Instant::now();
        let live = frame_ctx(&app, &model, start);
        assert_eq!(live.now, model.now().unwrap());
        app.handle_key(
            KeyEvent::new(KeyCode::Char('p'), KeyModifiers::NONE),
            &model,
        );
        let moved_on = Model::new();
        let frozen = frame_ctx(&app, &moved_on, start);
        assert_eq!(frozen.wall, model.wall().unwrap());
    }

    #[test]
    fn logging_without_a_path_installs_nothing_and_a_bad_path_fails() {
        assert!(init_logging(None).is_ok());
        let error = init_logging(Some(std::path::Path::new("/no/such/dir/lens.log"))).unwrap_err();
        assert!(
            error.to_string().contains("/no/such/dir/lens.log"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn without_a_director_the_command_future_never_resolves() {
        let mut none: Option<mpsc::UnboundedReceiver<UiCommand>> = None;
        let waited = tokio::time::timeout(Duration::from_millis(30), next_command(&mut none)).await;
        assert!(waited.is_err());
        let (tx, rx) = mpsc::unbounded_channel();
        let mut some = Some(rx);
        tx.send(UiCommand::Help(true)).unwrap();
        assert_eq!(next_command(&mut some).await, Some(UiCommand::Help(true)));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_sigterm_to_the_process_ends_the_wait_instead_of_killing_it() {
        use rustix::process::{Signal, getpid, kill_process};
        let waiting = tokio::spawn(termination());
        // Give the handlers time to install before the signal arrives.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!waiting.is_finished());
        kill_process(getpid(), Signal::TERM).expect("the signal is sent");
        tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("the signal ends the wait")
            .expect("the wait does not panic");
    }

    #[test]
    fn the_loop_ticks_every_fifty_milliseconds() {
        assert_eq!(TICK, Duration::from_millis(50));
    }
}

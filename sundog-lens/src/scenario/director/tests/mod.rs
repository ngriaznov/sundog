//! Tests of the scenario director.

use std::sync::Mutex;
use std::time::SystemTime;

use sundog::observe::MemberStatus;

use super::*;
use crate::ask::SCRIPTED_ID;
use crate::explained::{Outcome, fixture};
use crate::model::digest::HeldShares;
use crate::model::testkit;
use crate::scenario::{AwaitedStatus, Entry, parse};

mod decisions;

const S: fn(u64) -> Duration = Duration::from_secs;

/// Held by the tests whose runs log a warning. `tracing` caches whether a
/// callsite is enabled when a thread first reaches it, so a test that
/// installs a subscriber must not race another test into the callsite.
static LOGGING: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn digest(live: usize) -> ModelDigest {
    ModelDigest {
        live,
        ..ModelDigest::default()
    }
}

fn with_cache(mut digest: ModelDigest, view: u64, settled: bool) -> ModelDigest {
    digest.view_hash.insert("it".into(), view);
    digest.settled.insert("it".into(), settled);
    digest
}

/// A stage that records what it is asked and, for a spawn, moves the
/// digest the way a real cluster would: members join, the view changes,
/// and the cache settles a moment later.
struct Fake {
    calls: Mutex<Vec<String>>,
    digests: watch::Sender<ModelDigest>,
    fail_kill: bool,
    settle_after: Duration,
    /// After a kill or crash the node shows `Down` this much later.
    down_after: Option<Duration>,
    /// What the nodes answer to `explain` once the script has run out.
    nodes: Nodes,
    /// What the nodes answer to the first asks, in order.
    script: Mutex<std::collections::VecDeque<Nodes>>,
}

/// What the fake's nodes answer to `explain`: the five live nodes of the
/// fixture model, as a converged cluster gives them, unless it says
/// otherwise.
#[derive(Clone, Copy, Debug)]
enum Nodes {
    /// Every node holds the lens's view and owners.
    Agree,
    /// Every node holds a view the lens does not.
    Moved,
    /// `n3` refuses the connection.
    OneRefuses,
    /// The answer holds no node.
    Nobody,
    /// Every node agrees, after this long.
    Late(Duration),
    /// Nobody answers.
    Never,
    /// The stage cannot ask.
    Broken,
}

impl Fake {
    fn new(digests: watch::Sender<ModelDigest>) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            digests,
            fail_kill: false,
            settle_after: Duration::from_millis(500),
            down_after: None,
            nodes: Nodes::Agree,
            script: Mutex::default(),
        }
    }

    fn note(&self, call: String) {
        self.calls.lock().unwrap().push(call);
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }

    /// The detector notices `label` `down_after` later, if set.
    fn goes_down(&self, label: &str) {
        let Some(after) = self.down_after else { return };
        let digests = self.digests.clone();
        let label = label.to_owned();
        tokio::spawn(async move {
            tokio::time::sleep(after).await;
            digests.send_modify(|d| {
                d.statuses.insert(label.as_str().into(), MemberStatus::Down);
            });
        });
    }

    /// The cluster changes: `live` members, a new view, settled later.
    fn change(&self, live: usize) {
        let view = u64::try_from(live).unwrap() * 100;
        self.digests
            .send_replace(with_cache(digest(live), view, false));
        let digests = self.digests.clone();
        let after = self.settle_after;
        tokio::spawn(async move {
            tokio::time::sleep(after).await;
            digests.send_modify(|d| {
                d.settled.insert("it".into(), true);
            });
        });
    }
}

#[expect(clippy::unused_async_trait_impl, reason = "the fake never waits")]
impl Stage for Fake {
    async fn spawn(&self, count: usize, stagger: Option<Duration>) -> anyhow::Result<()> {
        self.note(format!("spawn {count} {stagger:?}"));
        let live = self.digests.borrow().live + count;
        self.change(live);
        Ok(())
    }

    async fn fill(&self, keys: u64) -> anyhow::Result<()> {
        self.note(format!("fill {keys}"));
        Ok(())
    }

    fn load(&self, on: bool) {
        self.note(format!("load {on}"));
    }

    async fn kill(&self, label: &str) -> anyhow::Result<()> {
        self.note(format!("kill {label}"));
        if self.fail_kill {
            anyhow::bail!("{label} is not running");
        }
        self.goes_down(label);
        Ok(())
    }

    async fn leave(&self, label: &str) -> anyhow::Result<()> {
        self.note(format!("leave {label}"));
        Ok(())
    }

    async fn crash(&self, label: &str) -> anyhow::Result<()> {
        self.note(format!("crash {label}"));
        self.goes_down(label);
        Ok(())
    }

    async fn restart(&self, label: &str) -> anyhow::Result<()> {
        self.note(format!("restart {label}"));
        Ok(())
    }

    async fn explain(&self, key: &str) -> anyhow::Result<Explained> {
        self.note(format!("explain {key}"));
        let nodes = self
            .script
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(self.nodes);
        let mut answers = converged_answers(key);
        match nodes {
            Nodes::Agree => {}
            Nodes::Moved => {
                for answer in &mut answers.nodes {
                    if let Outcome::Read(reading) = &mut answer.outcome {
                        reading
                            .distributed
                            .as_mut()
                            .expect("the fixture reads are distributed")
                            .view = "00c1a2f4e8b3195d".to_owned();
                    }
                }
            }
            Nodes::OneRefuses => {
                answers.nodes[2].outcome =
                    Outcome::Failed("no answer (connection refused)".to_owned());
            }
            Nodes::Nobody => answers.nodes.clear(),
            Nodes::Late(after) => tokio::time::sleep(after).await,
            Nodes::Never => std::future::pending().await,
            Nodes::Broken => anyhow::bail!("no node is running to ask"),
        }
        Ok(answers)
    }
}

/// The model of five live nodes ranking `it`, whose placement the fake's
/// answers and the digest's shares both come from. Ranking costs a few
/// hundred thousand hashes, so every test shares one.
fn cluster() -> &'static crate::model::Model {
    static CLUSTER: std::sync::OnceLock<crate::model::Model> = std::sync::OnceLock::new();
    CLUSTER.get_or_init(|| testkit::fixture_model(std::time::Instant::now()))
}

/// What five nodes that hold the lens's view answer about `key`.
fn converged_answers(key: &str) -> Explained {
    fixture::explained(cluster(), SCRIPTED_ID, key, SystemTime::UNIX_EPOCH)
}

/// `digest` with the ownership shares of the model of [`cluster`].
fn with_cluster_shares(mut digest: ModelDigest) -> ModelDigest {
    let ranked = cluster().ownership("it").expect("the fixture ranks it");
    digest.ownership.insert(
        "it".into(),
        HeldShares(std::sync::Arc::clone(&ranked.shares)),
    );
    digest
}

struct Rig {
    director: Director<Fake>,
    ui: mpsc::UnboundedReceiver<UiCommand>,
}

fn rig(options: Options, tweak: impl FnOnce(&mut Fake)) -> Rig {
    let (tx, rx) = watch::channel(ModelDigest::default());
    let mut fake = Fake::new(tx);
    tweak(&mut fake);
    let (ui_tx, ui_rx) = mpsc::unbounded_channel();
    Rig {
        director: Director::new(fake, rx, Some(ui_tx), options),
        ui: ui_rx,
    }
}

/// A rig whose digest holds the cluster's shares and whose nodes answer
/// `nodes`.
fn explaining_rig(options: Options, nodes: Nodes) -> Rig {
    let rig = rig(options, |fake| fake.nodes = nodes);
    rig.director
        .stage
        .digests
        .send_modify(|digest| *digest = with_cluster_shares(std::mem::take(digest)));
    rig
}

const HEADLESS: Options = Options {
    headless: true,
    captions: true,
};
const SHOW: Options = Options {
    headless: false,
    captions: true,
};

#[tokio::test(start_paused = true)]
async fn a_scenario_plays_in_order_and_awaits_what_it_changed() {
    let mut rig = rig(HEADLESS, |_| {});
    let scenario = parse(
        "caption \"hello\"\n\
         pause 2s\n\
         spawn 3 stagger 1s\n\
         await members 3 within 20s\n\
         fill 100\n\
         load start\n\
         await settled it within 25s\n\
         tab caches\n\
         select n2\n\
         cache it\n\
         help on\n\
         quit\n",
    )
    .unwrap();
    let mut out = Vec::new();
    let mut marks = Vec::new();
    let outcome = rig
        .director
        .play(&scenario, Instant::now(), &mut out, Some(&mut marks))
        .await
        .unwrap();
    assert_eq!(outcome.steps, 12);
    assert!(outcome.timeouts.is_empty() && outcome.failures.is_empty());
    assert!(outcome.elapsed >= S(2));
    assert_eq!(
        rig.director.stage.calls(),
        ["spawn 3 Some(1s)", "fill 100", "load true"]
    );
    let mut commands = Vec::new();
    while let Ok(command) = rig.ui.try_recv() {
        commands.push(command);
    }
    assert_eq!(
        commands,
        [
            UiCommand::Caption(Some("hello".into())),
            UiCommand::Tab(crate::ui::View::Caches),
            UiCommand::Select("n2".into()),
            UiCommand::Cache("it".into()),
            UiCommand::Help(true),
            UiCommand::Quit,
        ]
    );
    let log = String::from_utf8(out).unwrap();
    assert!(log.contains("spawn 3 stagger 1s"), "{log}");
    assert!(log.contains("ok after"), "{log}");
    assert!(
        log.contains("scenario done: 12 steps, 0 timeouts, 0 failures"),
        "{log}"
    );
    let marks = String::from_utf8(marks).unwrap();
    assert_eq!(marks.lines().count(), 12);
    let second: Vec<&str> = marks.lines().nth(2).unwrap().splitn(2, ' ').collect();
    assert!(second[0].parse::<f64>().unwrap() >= 2.0, "{marks}");
    assert_eq!(second[1], "spawn 3 stagger 1s");
}

#[tokio::test(start_paused = true)]
async fn a_settled_await_waits_for_the_view_to_move_and_settle() {
    let rig = rig(HEADLESS, |fake| fake.settle_after = S(4));
    let scenario = parse("spawn 1\nawait settled it within 25s\nquit").unwrap();
    let mut out = Vec::new();
    let outcome = rig
        .director
        .play(&scenario, Instant::now(), &mut out, None)
        .await
        .unwrap();
    // The cache settled four seconds after the spawn, so the await took
    // that long and no less.
    assert!(outcome.elapsed >= S(4), "{:?}", outcome.elapsed);
    assert!(outcome.elapsed < S(6), "{:?}", outcome.elapsed);
}

#[tokio::test(start_paused = true)]
async fn the_baseline_is_taken_before_the_action_so_a_fast_view_change_still_counts() {
    // The view changes and settles inside the spawn itself: a baseline
    // taken after the action would equal the new view and never pass.
    let rig = rig(HEADLESS, |fake| fake.settle_after = Duration::ZERO);
    let scenario =
        parse("spawn 1\nawait settled it within 5s\nspawn 1\nawait settled it within 5s").unwrap();
    let mut out = Vec::new();
    let outcome = rig
        .director
        .play(&scenario, Instant::now(), &mut out, None)
        .await
        .unwrap();
    assert!(outcome.timeouts.is_empty(), "{:?}", outcome.timeouts);
    assert_eq!(outcome.steps, 4);
}

#[tokio::test(start_paused = true)]
async fn a_headless_run_that_times_out_is_an_error_naming_the_line() {
    let rig = rig(HEADLESS, |_| {});
    let scenario = parse("pause 1s\nawait members 9 within 2s\nquit").unwrap();
    let mut out = Vec::new();
    let error = rig
        .director
        .play(&scenario, Instant::now(), &mut out, None)
        .await
        .unwrap_err();
    match &error {
        PlayError::TimedOut(Timeout { line, step }) => {
            assert_eq!(*line, 2);
            assert_eq!(step, "await members 9 within 2s");
        }
        other => panic!("not a timeout: {other}"),
    }
    assert_eq!(
        error.to_string(),
        "line 2: `await members 9 within 2s` timed out"
    );
    assert!(
        String::from_utf8(out)
            .unwrap()
            .contains("timed out after 2.0s")
    );
}

#[tokio::test(start_paused = true)]
async fn a_run_with_an_interface_notes_a_timeout_shows_a_caption_and_goes_on() {
    let _logging = LOGGING.lock().await;
    let mut rig = rig(SHOW, |_| {});
    let scenario = parse("await members 9 within 2s\nfill 5\nquit").unwrap();
    let mut out = Vec::new();
    let outcome = rig
        .director
        .play(&scenario, Instant::now(), &mut out, None)
        .await
        .unwrap();
    assert_eq!(outcome.steps, 3);
    assert_eq!(
        outcome.timeouts,
        [Timeout {
            line: 1,
            step: "await members 9 within 2s".into()
        }]
    );
    assert_eq!(rig.director.stage.calls(), ["fill 5"]);
    assert_eq!(
        rig.ui.try_recv().unwrap(),
        UiCommand::Caption(Some(TIMED_OUT_CAPTION.into()))
    );
    assert_eq!(rig.ui.try_recv().unwrap(), UiCommand::Quit);
}

#[tokio::test(start_paused = true)]
async fn a_failed_step_ends_a_headless_run_and_is_noted_otherwise() {
    let _logging = LOGGING.lock().await;
    let headless = rig(HEADLESS, |fake| fake.fail_kill = true);
    let scenario = parse("fill 1\nkill n7\nfill 2").unwrap();
    let mut out = Vec::new();
    let error = headless
        .director
        .play(&scenario, Instant::now(), &mut out, None)
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "line 2: `kill n7` failed: n7 is not running"
    );
    assert_eq!(headless.director.stage.calls(), ["fill 1", "kill n7"]);

    let shown = rig(SHOW, |fake| fake.fail_kill = true);
    let mut out = Vec::new();
    let outcome = shown
        .director
        .play(&scenario, Instant::now(), &mut out, None)
        .await
        .unwrap();
    assert_eq!(outcome.steps, 3);
    assert_eq!(outcome.failures, [(2, "n7 is not running".to_owned())]);
    assert_eq!(
        shown.director.stage.calls(),
        ["fill 1", "kill n7", "fill 2"]
    );
    assert!(
        String::from_utf8(out)
            .unwrap()
            .contains("failed: n7 is not running")
    );
}

#[tokio::test(start_paused = true)]
async fn captions_can_be_turned_off_and_a_run_without_an_interface_is_fine() {
    let mut quiet = rig(
        Options {
            headless: true,
            captions: false,
        },
        |_| {},
    );
    let scenario = parse("caption \"hidden\"\ntab node\n").unwrap();
    let mut out = Vec::new();
    quiet
        .director
        .play(&scenario, Instant::now(), &mut out, None)
        .await
        .unwrap();
    assert_eq!(
        quiet.ui.try_recv().unwrap(),
        UiCommand::Tab(crate::ui::View::Node)
    );
    // The scenario has no `quit` step, so the director ends it.
    assert_eq!(quiet.ui.try_recv().unwrap(), UiCommand::Quit);
    assert!(quiet.ui.try_recv().is_err());

    let (tx, rx) = watch::channel(ModelDigest::default());
    let none = Director::new(Fake::new(tx), rx, None, HEADLESS);
    let mut out = Vec::new();
    let outcome = none
        .play(&scenario, Instant::now(), &mut out, None)
        .await
        .unwrap();
    assert_eq!(outcome.steps, 2);
}

#[test]
fn a_failure_to_write_the_log_is_a_play_error_that_says_so() {
    let error = PlayError::from(io::Error::other("disk full"));
    assert!(matches!(error, PlayError::Io(_)));
    assert_eq!(error.to_string(), "writing the scenario log: disk full");
}

/// A writer that fails.
struct Broken;

impl Write for Broken {
    fn write(&mut self, _: &[u8]) -> io::Result<usize> {
        Err(io::Error::other("broken pipe"))
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn a_log_that_cannot_be_written_ends_the_run_with_an_error() {
    let rig = rig(HEADLESS, |_| {});
    let scenario = parse("fill 1\nquit").unwrap();
    let error = rig
        .director
        .play(&scenario, Instant::now(), &mut Broken, None)
        .await
        .unwrap_err();
    assert!(matches!(error, PlayError::Io(_)), "{error}");
    let mut out = Vec::new();
    let error = rig
        .director
        .play(&scenario, Instant::now(), &mut out, Some(&mut Broken))
        .await
        .unwrap_err();
    assert!(matches!(error, PlayError::Io(_)), "{error}");
}

#[tokio::test(start_paused = true)]
async fn a_scenario_stops_at_quit_and_an_empty_scenario_is_done_at_once() {
    let rig = rig(HEADLESS, |_| {});
    let scenario = parse("fill 1\nquit\nfill 2").unwrap();
    let mut out = Vec::new();
    let outcome = rig
        .director
        .play(&scenario, Instant::now(), &mut out, None)
        .await
        .unwrap();
    assert_eq!(outcome.steps, 2);
    assert_eq!(rig.director.stage.calls(), ["fill 1"]);

    let empty = rig
        .director
        .play(&Scenario::default(), Instant::now(), &mut Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(empty.steps, 0);
}

/// The stamps of the `[ 12.3s]` lines of a log, in order.
fn stamps(log: &str) -> Vec<f64> {
    log.lines()
        .filter_map(|line| {
            let inner = line.strip_prefix('[')?.split_once("s]")?.0;
            inner.trim().parse().ok()
        })
        .collect()
}

#[tokio::test(start_paused = true)]
async fn every_stamp_counts_from_the_origin_the_caller_gives_and_never_goes_back() {
    let rig = rig(HEADLESS, |fake| fake.settle_after = S(2));
    let scenario = parse(
        "pause 1s\nspawn 1\nawait members 1 within 5s\n\
         await settled it within 25s\npause 1s\nquit",
    )
    .unwrap();
    // The run began 10 s before the director took over.
    let start = Instant::now() - S(10);
    let mut out = Vec::new();
    let mut marks = Vec::new();
    rig.director
        .play(&scenario, start, &mut out, Some(&mut marks))
        .await
        .unwrap();
    let log = String::from_utf8(out).unwrap();
    let stamps = stamps(&log);
    assert!(stamps.len() >= 9, "{log}");
    assert!(stamps[0] >= 10.0, "{log}");
    assert!(stamps.windows(2).all(|pair| pair[0] <= pair[1]), "{log}");
    let marks = String::from_utf8(marks).unwrap();
    let first: f64 = marks.split(' ').next().unwrap().parse().unwrap();
    assert!(first >= 10.0, "{marks}");
}

#[tokio::test(start_paused = true)]
async fn a_scenario_without_quit_tells_the_interface_to_quit_after_its_last_step() {
    let mut rig = rig(SHOW, |_| {});
    let scenario = parse("tab node\nfill 1").unwrap();
    let outcome = rig
        .director
        .play(&scenario, Instant::now(), &mut Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(outcome.steps, 2);
    assert_eq!(
        rig.ui.try_recv().unwrap(),
        UiCommand::Tab(crate::ui::View::Node)
    );
    assert_eq!(rig.ui.try_recv().unwrap(), UiCommand::Quit);
    assert!(rig.ui.try_recv().is_err());
}

/// Collects what `tracing` events write.
#[derive(Clone, Default)]
struct Captured(std::sync::Arc<Mutex<Vec<u8>>>);

impl Write for Captured {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
    type Writer = Self;

    fn make_writer(&'a self) -> Self {
        self.clone()
    }
}

impl Captured {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

/// Collects what this thread's `tracing` events write, until the guard
/// drops. Held beside [`LOGGING`].
fn capture_logs() -> (Captured, tracing::subscriber::DefaultGuard) {
    let captured = Captured::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(captured.clone())
        .with_ansi(false)
        .finish();
    let guard = tracing::subscriber::set_default(subscriber);
    // Other tests of this module hit the same callsites on their own
    // threads, with no subscriber: recompute what each callsite is
    // interested in now that this thread has one.
    tracing::callsite::rebuild_interest_cache();
    (captured, guard)
}

#[tokio::test(start_paused = true)]
async fn a_run_with_an_interface_logs_a_warning_for_a_timeout_and_a_failed_step() {
    let _logging = LOGGING.lock().await;
    let (captured, _guard) = capture_logs();
    let scenario = parse("await members 9 within 2s\nkill n7\nquit").unwrap();

    let shown = rig(SHOW, |fake| fake.fail_kill = true);
    shown
        .director
        .play(&scenario, Instant::now(), &mut Vec::new(), None)
        .await
        .unwrap();
    let text = captured.text();
    assert!(text.contains("WARN"), "{text}");
    assert!(text.contains("await timed out after 2.0s"), "{text}");
    assert!(text.contains("await members 9 within 2s"), "{text}");
    assert!(text.contains("step failed: n7 is not running"), "{text}");

    // A headless run reports through its error and stays out of the log.
    let before = captured.text();
    let headless = rig(HEADLESS, |_| {});
    let timeout = parse("await members 9 within 2s").unwrap();
    headless
        .director
        .play(&timeout, Instant::now(), &mut Vec::new(), None)
        .await
        .unwrap_err();
    assert_eq!(captured.text(), before);
}

#[test]
fn only_a_kill_and_a_crash_leave_without_a_goodbye() {
    assert_eq!(
        abrupt_exit(&Step::Kill("n3".into())),
        Some(("n3", "SIGKILL"))
    );
    assert_eq!(
        abrupt_exit(&Step::Crash("n2".into())),
        Some(("n2", "crash"))
    );
    for step in [
        Step::Leave("n1".into()),
        Step::Restart("n1".into()),
        Step::Quit,
    ] {
        assert_eq!(abrupt_exit(&step), None, "{step}");
    }
    assert_eq!(
        detected_caption("n3", "SIGKILL", Duration::from_millis(840)),
        "n3 marked down 0.8 s after the SIGKILL"
    );
}

#[tokio::test(start_paused = true)]
async fn the_caption_after_a_kill_says_how_long_the_detector_took() {
    let _logging = LOGGING.lock().await;
    let mut rig = rig(SHOW, |fake| {
        fake.down_after = Some(Duration::from_millis(800));
    });
    let scenario = parse(
        "kill n3\nawait down n3 within 5s\nkill n2\nawait down n9 within 1s\ncrash n1\nawait down n1 within 5s",
    )
    .unwrap();
    let outcome = rig
        .director
        .play(&scenario, Instant::now(), &mut Vec::new(), None)
        .await
        .unwrap();
    // `await down n9` times out: no caption about it.
    assert_eq!(outcome.timeouts.len(), 1);
    let mut captions = Vec::new();
    while let Ok(command) = rig.ui.try_recv() {
        if let UiCommand::Caption(Some(text)) = command {
            captions.push(text);
        }
    }
    assert_eq!(
        captions,
        [
            "n3 marked down 0.8 s after the SIGKILL".to_owned(),
            TIMED_OUT_CAPTION.to_owned(),
            "n1 marked down 0.8 s after the crash".to_owned(),
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn no_detection_caption_without_captions_or_without_an_interface() {
    let quiet = Options {
        headless: false,
        captions: false,
    };
    let mut rig = rig(quiet, |fake| {
        fake.down_after = Some(Duration::from_millis(300));
    });
    let scenario = parse("kill n3\nawait down n3 within 5s").unwrap();
    rig.director
        .play(&scenario, Instant::now(), &mut Vec::new(), None)
        .await
        .unwrap();
    while let Ok(command) = rig.ui.try_recv() {
        assert!(
            !matches!(command, UiCommand::Caption(_)),
            "no caption: {command:?}"
        );
    }
}

/// What reached the interface, in order.
fn drain(ui: &mut mpsc::UnboundedReceiver<UiCommand>) -> Vec<UiCommand> {
    let mut commands = Vec::new();
    while let Ok(command) = ui.try_recv() {
        commands.push(command);
    }
    commands
}

/// The overlays the commands open: the key typed, and the answers.
fn overlays(commands: &[UiCommand]) -> Vec<(&str, &Explained)> {
    commands
        .iter()
        .filter_map(|command| match command {
            UiCommand::Explain { key, answers } => Some((key.as_str(), answers.as_ref())),
            _ => None,
        })
        .collect()
}

/// The commands as one word each.
fn kinds(commands: &[UiCommand]) -> Vec<&'static str> {
    commands
        .iter()
        .map(|command| match command {
            UiCommand::Explain { .. } => "explain",
            UiCommand::CloseExplain => "close",
            UiCommand::Caption(_) => "caption",
            UiCommand::Quit => "quit",
            other => panic!("unexpected {other:?}"),
        })
        .collect()
}

#[tokio::test(start_paused = true)]
async fn an_explain_step_asks_every_node_and_hands_the_overlay_the_answers() {
    let mut rig = explaining_rig(HEADLESS, Nodes::Agree);
    let scenario = parse(
        "explain k1 within 5s\n\
         pause 1s\n\
         explain close\n\
         explain str:close\n\
         explain close\n\
         quit",
    )
    .unwrap();
    let mut out = Vec::new();
    let outcome = rig
        .director
        .play(&scenario, Instant::now(), &mut out, None)
        .await
        .unwrap();
    assert_eq!(outcome.steps, 6);
    assert!(outcome.timeouts.is_empty() && outcome.failures.is_empty());
    // The nodes are asked about the key after the overlay's prefix.
    assert_eq!(rig.director.stage.calls(), ["explain k1", "explain close"]);

    let commands = drain(&mut rig.ui);
    assert_eq!(
        kinds(&commands),
        ["explain", "close", "explain", "close", "quit"]
    );
    let shown = overlays(&commands);
    assert_eq!((shown[0].0, shown[0].1.key.as_str()), ("k1", "k1"));
    assert_eq!(
        (shown[1].0, shown[1].1.key.as_str()),
        ("str:close", "close"),
        "the overlay types the key as the scenario wrote it"
    );
    for (_, answers) in &shown {
        assert_eq!(answers.id, SCRIPTED_ID);
        let labels: Vec<&str> = answers.nodes.iter().map(|a| a.label.as_str()).collect();
        assert_eq!(labels, ["n1", "n2", "n3", "n4", "n5"]);
    }
    let log = String::from_utf8(out).unwrap();
    assert!(log.contains("] explain k1 within 5s"), "{log}");
    assert!(
        log.contains("ok after 0.0s: 5 nodes agree, the lens computes the same"),
        "{log}"
    );
}

#[tokio::test(start_paused = true)]
async fn an_explain_needs_no_interface_to_ask_and_judge() {
    let (tx, rx) = watch::channel(with_cluster_shares(ModelDigest::default()));
    let none = Director::new(Fake::new(tx), rx, None, HEADLESS);
    let scenario = parse("explain k1\nexplain close").unwrap();
    let mut out = Vec::new();
    let outcome = none
        .play(&scenario, Instant::now(), &mut out, None)
        .await
        .unwrap();
    assert_eq!(outcome.steps, 2);
    assert_eq!(none.stage.calls(), ["explain k1"]);
    let log = String::from_utf8(out).unwrap();
    assert!(
        log.contains("5 nodes agree, the lens computes the same"),
        "{log}"
    );
}

#[tokio::test(start_paused = true)]
async fn an_explain_asks_again_until_every_node_answers_and_agrees() {
    // Two nodes still opening their caches, then a view still moving,
    // then the cluster agrees.
    let mut rig = explaining_rig(HEADLESS, Nodes::Agree);
    rig.director.stage.script.lock().unwrap().extend([
        Nodes::OneRefuses,
        Nodes::OneRefuses,
        Nodes::Moved,
    ]);
    let scenario = parse("explain k1 within 5s\nquit").unwrap();
    let mut out = Vec::new();
    let outcome = rig
        .director
        .play(&scenario, Instant::now(), &mut out, None)
        .await
        .unwrap();
    assert!(outcome.timeouts.is_empty() && outcome.failures.is_empty());
    assert_eq!(rig.director.stage.calls(), ["explain k1"; 4]);
    // One look at the digest after another, a quarter second apart.
    assert!(outcome.elapsed >= ASK_AGAIN * 3, "{:?}", outcome.elapsed);
    assert!(outcome.elapsed < ASK_AGAIN * 4, "{:?}", outcome.elapsed);
    let log = String::from_utf8(out).unwrap();
    // Each new reason is logged once, and the pass ends the wait.
    assert_eq!(log.matches("not yet after").count(), 2, "{log}");
    assert!(
        log.contains(
            "not yet after 0.0s: 1 of 5 nodes gave no explanation: \
             n3: no answer (connection refused)"
        ),
        "{log}"
    );
    assert!(log.contains("not yet after 0.5s: view moved "), "{log}");
    assert!(
        log.contains("ok after 0.8s: 5 nodes agree, the lens computes the same")
            || log.contains("ok after 0.7s: 5 nodes agree, the lens computes the same"),
        "{log}"
    );
    // The overlay opens once, on the answers that passed.
    let commands = drain(&mut rig.ui);
    assert_eq!(kinds(&commands), ["explain", "quit"]);
    let shown = overlays(&commands);
    assert_eq!(shown[0].1.readings().count(), 5);
}

#[test]
fn an_explain_asks_again_every_quarter_second_until_its_deadline() {
    assert_eq!(ask_again_in(S(0), S(5)), Some(ASK_AGAIN));
    assert_eq!(ask_again_in(S(4), S(5)), Some(ASK_AGAIN));
    // Less than a quarter second left: wait out what is left.
    let near = Duration::from_millis(4900);
    assert_eq!(ask_again_in(near, S(5)), Some(Duration::from_millis(100)));
    // At the deadline and past it: no more asks.
    assert_eq!(ask_again_in(S(5), S(5)), None);
    assert_eq!(ask_again_in(S(6), S(5)), None);
    assert_eq!(ask_again_in(S(0), S(0)), None);
    // A deadline no instant can hold is a wait that does not end.
    assert_eq!(ask_again_in(S(1_000_000), Duration::MAX), Some(ASK_AGAIN));
    assert_eq!(ask_again_in(Duration::MAX, Duration::MAX), None);
}

#[tokio::test(start_paused = true)]
async fn an_explain_with_a_deadline_no_instant_can_hold_asks_until_the_answers_pass() {
    let mut rig = explaining_rig(HEADLESS, Nodes::Agree);
    rig.director
        .stage
        .script
        .lock()
        .unwrap()
        .extend([Nodes::OneRefuses, Nodes::OneRefuses]);
    let scenario = parse("explain k1 within 18446744073709551615s\nquit").unwrap();
    let mut out = Vec::new();
    let outcome = rig
        .director
        .play(&scenario, Instant::now(), &mut out, None)
        .await
        .unwrap();
    assert!(outcome.timeouts.is_empty() && outcome.failures.is_empty());
    assert_eq!(rig.director.stage.calls(), ["explain k1"; 3]);
    assert!(outcome.elapsed >= ASK_AGAIN * 2, "{:?}", outcome.elapsed);
    assert_eq!(kinds(&drain(&mut rig.ui)), ["explain", "quit"]);
    let log = String::from_utf8(out).unwrap();
    assert!(
        log.contains("5 nodes agree, the lens computes the same"),
        "{log}"
    );
}

#[tokio::test(start_paused = true)]
async fn an_explain_waits_for_slow_answers_and_gives_up_at_its_deadline() {
    let rig = explaining_rig(HEADLESS, Nodes::Late(S(3)));
    let scenario = parse("explain k1 within 5s").unwrap();
    let outcome = rig
        .director
        .play(&scenario, Instant::now(), &mut Vec::new(), None)
        .await
        .unwrap();
    assert!(outcome.elapsed >= S(3), "{:?}", outcome.elapsed);
    assert!(outcome.elapsed < S(4), "{:?}", outcome.elapsed);

    let rig = explaining_rig(HEADLESS, Nodes::Late(S(3)));
    let scenario = parse("explain k1 within 2s").unwrap();
    let error = rig
        .director
        .play(&scenario, Instant::now(), &mut Vec::new(), None)
        .await
        .unwrap_err();
    assert!(matches!(error, PlayError::TimedOut(_)), "{error}");
}

#[tokio::test(start_paused = true)]
async fn an_explain_nobody_answers_times_out_like_an_await() {
    // Headless, it is an error that names the line.
    let rig = explaining_rig(HEADLESS, Nodes::Never);
    let scenario = parse("pause 1s\nexplain k1 within 2s\nquit").unwrap();
    let mut out = Vec::new();
    let error = rig
        .director
        .play(&scenario, Instant::now(), &mut out, None)
        .await
        .unwrap_err();
    match &error {
        PlayError::TimedOut(Timeout { line, step }) => {
            assert_eq!(*line, 2);
            assert_eq!(step, "explain k1 within 2s");
        }
        other => panic!("not a timeout: {other}"),
    }
    assert_eq!(
        error.to_string(),
        "line 2: `explain k1 within 2s` timed out"
    );
    assert!(
        String::from_utf8(out)
            .unwrap()
            .contains("timed out after 2.0s")
    );

    // Without a `within` it waits the default.
    let rig = explaining_rig(HEADLESS, Nodes::Never);
    let mut out = Vec::new();
    rig.director
        .play(
            &parse("explain k1").unwrap(),
            Instant::now(),
            &mut out,
            None,
        )
        .await
        .unwrap_err();
    let log = String::from_utf8(out).unwrap();
    assert!(log.contains("timed out after 30.0s"), "{log}");

    // With an interface, the run warns, captions it and goes on.
    let _logging = LOGGING.lock().await;
    let (captured, _guard) = capture_logs();
    let mut rig = explaining_rig(SHOW, Nodes::Never);
    let scenario = parse("explain k1 within 2s\nfill 5\nquit").unwrap();
    let outcome = rig
        .director
        .play(&scenario, Instant::now(), &mut Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(outcome.steps, 3);
    assert_eq!(
        outcome.timeouts,
        [Timeout {
            line: 1,
            step: "explain k1 within 2s".into()
        }]
    );
    assert!(outcome.failures.is_empty(), "{:?}", outcome.failures);
    assert_eq!(rig.director.stage.calls(), ["explain k1", "fill 5"]);
    assert_eq!(
        drain(&mut rig.ui),
        [
            UiCommand::Caption(Some(TIMED_OUT_CAPTION.into())),
            UiCommand::Quit
        ]
    );
    let text = captured.text();
    assert!(text.contains("WARN"), "{text}");
    assert!(text.contains("await timed out after 2.0s"), "{text}");
    assert!(text.contains("explain k1 within 2s"), "{text}");
}

#[tokio::test(start_paused = true)]
async fn answers_that_disagree_with_the_lens_fail_the_step_and_the_overlay_shows_them() {
    let mut rig = explaining_rig(HEADLESS, Nodes::Moved);
    let scenario = parse("fill 1\nexplain k1 within 5s\nexplain close\nquit").unwrap();
    let error = rig
        .director
        .play(&scenario, Instant::now(), &mut Vec::new(), None)
        .await
        .unwrap_err();
    let PlayError::Step { line, step, source } = &error else {
        panic!("not a failed step: {error}");
    };
    assert_eq!((*line, step.as_str()), (2, "explain k1 within 5s"));
    let why = source.to_string();
    assert!(why.starts_with("view moved 00c1a2f4 -> "), "{why}");
    assert!(!why.contains("Enter"), "no key to press in a run: {why}");
    assert_eq!(
        error.to_string(),
        format!("line 2: `explain k1 within 5s` failed: {why}")
    );
    // The nodes were asked again until the deadline, the run stopped
    // there, and the overlay was handed the last answers once.
    let calls = rig.director.stage.calls();
    assert_eq!(calls[0], "fill 1");
    assert!(calls.len() > 3, "{calls:?}");
    assert!(
        calls[1..].iter().all(|call| call == "explain k1"),
        "{calls:?}"
    );
    let commands = drain(&mut rig.ui);
    assert_eq!(kinds(&commands), ["explain"]);

    // With an interface the run warns of the failure and goes on.
    let _logging = LOGGING.lock().await;
    let (captured, _guard) = capture_logs();
    let mut rig = explaining_rig(SHOW, Nodes::Moved);
    let scenario = parse("explain k1\nexplain close\nquit").unwrap();
    let mut out = Vec::new();
    let outcome = rig
        .director
        .play(&scenario, Instant::now(), &mut out, None)
        .await
        .unwrap();
    assert_eq!(outcome.steps, 3);
    assert!(outcome.timeouts.is_empty(), "{:?}", outcome.timeouts);
    assert_eq!(outcome.failures, [(1, why)]);
    assert_eq!(
        kinds(&drain(&mut rig.ui)),
        ["explain", "close", "quit"],
        "the overlay opens and closes as scripted"
    );
    let log = String::from_utf8(out).unwrap();
    assert!(log.contains("failed: view moved 00c1a2f4 -> "), "{log}");
    let text = captured.text();
    assert!(text.contains("WARN"), "{text}");
    assert!(
        text.contains("step failed: view moved 00c1a2f4 -> "),
        "{text}"
    );
    assert!(text.contains("explain k1"), "{text}");
}

#[tokio::test(start_paused = true)]
async fn an_explain_fails_when_a_node_gives_no_explanation_or_nobody_is_asked() {
    for (nodes, expected) in [
        (
            Nodes::OneRefuses,
            "1 of 5 nodes gave no explanation: n3: no answer (connection refused)",
        ),
        (Nodes::Nobody, "no node was asked"),
        (Nodes::Broken, "no node is running to ask"),
    ] {
        let mut rig = explaining_rig(HEADLESS, nodes);
        let error = rig
            .director
            .play(
                &parse("explain k1").unwrap(),
                Instant::now(),
                &mut Vec::new(),
                None,
            )
            .await
            .unwrap_err();
        let PlayError::Step { source, .. } = &error else {
            panic!("{nodes:?}: not a failed step: {error}");
        };
        assert_eq!(source.to_string(), expected, "{nodes:?}");
        // A stage that cannot ask has no answers for the overlay.
        let opened = overlays(&drain(&mut rig.ui)).len();
        assert_eq!(opened, usize::from(!matches!(nodes, Nodes::Broken)));
    }
}

#[tokio::test(start_paused = true)]
async fn an_explain_without_a_placement_or_a_key_to_ask_fails() {
    // The digest holds no shares of the cache the nodes explained.
    let rig = rig(HEADLESS, |_| {});
    let error = rig
        .director
        .play(
            &parse("explain k1").unwrap(),
            Instant::now(),
            &mut Vec::new(),
            None,
        )
        .await
        .unwrap_err();
    let PlayError::Step { source, .. } = &error else {
        panic!("not a failed step: {error}");
    };
    assert_eq!(
        source.to_string(),
        "the lens has no placement of the key to compare with"
    );
    let calls = rig.director.stage.calls();
    assert!(calls.len() > 3, "asked again until the deadline: {calls:?}");

    // The parser refuses a key a test node cannot be asked, so a step
    // built by hand is the director's to refuse, before it asks anyone.
    let rig = explaining_rig(HEADLESS, Nodes::Agree);
    let scenario = Scenario {
        steps: vec![Entry {
            line: 7,
            step: Step::Explain {
                key: "uint:5".into(),
                within: None,
            },
        }],
    };
    let error = rig
        .director
        .play(&scenario, Instant::now(), &mut Vec::new(), None)
        .await
        .unwrap_err();
    let PlayError::Step { line, source, .. } = &error else {
        panic!("not a failed step: {error}");
    };
    assert_eq!(*line, 7);
    assert!(source.to_string().contains("cannot be asked"), "{source}");
    let calls = rig.director.stage.calls();
    assert!(calls.is_empty(), "{calls:?}");
}

#[tokio::test(start_paused = true)]
async fn the_built_in_tour_runs_against_a_fake_cluster() {
    // A fake that follows the tour: it counts live members and moves the
    // view on every spawn, kill, leave and restart.
    struct Tour {
        digests: watch::Sender<ModelDigest>,
    }
    impl Tour {
        fn bump(&self, f: impl FnOnce(&mut ModelDigest)) {
            self.digests.send_modify(|d| {
                f(d);
                let next = d.view_hash.get("it").copied().unwrap_or(0) + 1;
                d.view_hash.insert("it".into(), next);
                d.settled.insert("it".into(), false);
            });
            let digests = self.digests.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(800)).await;
                digests.send_modify(|d| {
                    d.settled.insert("it".into(), true);
                });
            });
        }
        fn status(&self, label: &str, status: MemberStatus, live_delta: isize) {
            let label = label.to_owned();
            self.bump(|d| {
                d.statuses.insert(label.as_str().into(), status);
                d.live = d.live.saturating_add_signed(live_delta);
            });
        }
    }
    #[expect(clippy::unused_async_trait_impl, reason = "the fake never waits")]
    impl Stage for Tour {
        async fn spawn(&self, count: usize, _: Option<Duration>) -> anyhow::Result<()> {
            for _ in 0..count {
                let n = self.digests.borrow().statuses.len() + 1;
                self.status(&format!("n{n}"), MemberStatus::Live, 1);
            }
            Ok(())
        }
        async fn fill(&self, _: u64) -> anyhow::Result<()> {
            Ok(())
        }
        fn load(&self, _: bool) {}
        async fn kill(&self, label: &str) -> anyhow::Result<()> {
            self.status(label, MemberStatus::Down, -1);
            Ok(())
        }
        async fn leave(&self, label: &str) -> anyhow::Result<()> {
            self.status(label, MemberStatus::Departing, -1);
            let digests = self.digests.clone();
            let label = label.to_owned();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(3)).await;
                digests.send_modify(|d| {
                    d.statuses.insert(label.as_str().into(), MemberStatus::Left);
                });
            });
            Ok(())
        }
        async fn crash(&self, label: &str) -> anyhow::Result<()> {
            self.status(label, MemberStatus::Down, -1);
            Ok(())
        }
        async fn restart(&self, label: &str) -> anyhow::Result<()> {
            self.status(label, MemberStatus::Live, 1);
            Ok(())
        }
        async fn explain(&self, key: &str) -> anyhow::Result<Explained> {
            Ok(converged_answers(key))
        }
    }
    let (tx, rx) = watch::channel(with_cluster_shares(ModelDigest::default()));
    let director = Director::new(Tour { digests: tx }, rx, None, HEADLESS);
    let scenario = parse(crate::scenario::TOUR).unwrap();
    let mut out = Vec::new();
    let outcome = director
        .play(&scenario, Instant::now(), &mut out, None)
        .await
        .unwrap();
    assert_eq!(outcome.steps, scenario.steps.len());
    assert!(outcome.timeouts.is_empty(), "{:?}", outcome.timeouts);
    assert!(outcome.failures.is_empty(), "{:?}", outcome.failures);
    // The tour's explain scene asks and passes.
    let log = String::from_utf8(out).unwrap();
    assert!(log.contains("] explain k1 within 10s"), "{log}");
    assert!(
        log.contains("5 nodes agree, the lens computes the same"),
        "{log}"
    );
}

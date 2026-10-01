//! The director: plays a scenario against the fleet and the model.
//!
//! The director never sleeps for a change of state. A step that changes the
//! cluster (`spawn`, `kill`, `leave`, `crash`, `restart`) is followed by an
//! `await` step that waits on the [`ModelDigest`] the interface publishes:
//! members counted, a node's status, a cache's view and its settling. Only
//! `pause` waits for time, and only to give the eye a moment.
//!
//! What a step is waiting for is decided by the pure [`next_action`]. The
//! rest is plumbing: [`Director::play`] reads the digest, calls
//! [`next_action`], acts on the [`Stage`] and sends [`UiCommand`]s.
//!
//! An `await settled` must not pass on the view that held before the step
//! that caused the change. At every fleet action the director records each
//! cache's view hash as a baseline, and `await settled` completes only once
//! the view differs from it and the cache has settled.

use std::collections::BTreeMap;
use std::fmt;
use std::io::{self, Write};
use std::time::Duration;

use smol_str::SmolStr;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

use super::{Scenario, Step};
use crate::app::UiCommand;
use crate::model::digest::ModelDigest;

/// How long an `await` without `within` waits.
pub const DEFAULT_WITHIN: Duration = Duration::from_secs(30);

/// The longest the director goes without looking at the digest.
pub const POLL: Duration = Duration::from_millis(50);

/// The caption a timed-out await leaves on screen.
pub const TIMED_OUT_CAPTION: &str = "await timed out";

/// What a scenario acts on: the fleet of nodes and the load against it.
#[expect(
    async_fn_in_trait,
    reason = "the director polls these futures in its own task, so they need not be Send"
)]
pub trait Stage {
    /// Starts `count` nodes, `stagger` apart.
    async fn spawn(&self, count: usize, stagger: Option<Duration>) -> anyhow::Result<()>;
    /// Writes `keys` keys through the fleet.
    async fn fill(&self, keys: u64) -> anyhow::Result<()>;
    /// Starts or stops the load.
    fn load(&self, on: bool);
    /// SIGKILLs a node.
    async fn kill(&self, label: &str) -> anyhow::Result<()>;
    /// SIGTERMs a node: a graceful leave.
    async fn leave(&self, label: &str) -> anyhow::Result<()>;
    /// Asks a node to crash without leaving.
    async fn crash(&self, label: &str) -> anyhow::Result<()>;
    /// Starts a stopped node again at its address.
    async fn restart(&self, label: &str) -> anyhow::Result<()>;
}

/// What the director remembers between looks at the digest.
#[derive(Debug, Clone)]
pub struct DirectorState {
    /// When the current step began.
    pub step_started: Instant,
    /// Each cache's view hash when the last fleet action began.
    pub baseline: BTreeMap<SmolStr, u64>,
}

/// What to do about the current step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectorAction {
    /// Look again later.
    Wait,
    /// The step is done: go on.
    Done,
    /// An await ran out of time.
    TimedOut,
}

/// Whether `step` changes the cluster, so that the director records the
/// baseline views before it.
#[must_use]
pub const fn is_fleet_action(step: &Step) -> bool {
    matches!(
        step,
        Step::Spawn { .. } | Step::Kill(_) | Step::Leave(_) | Step::Crash(_) | Step::Restart(_)
    )
}

/// How long the await `step` waits: its `within`, or [`DEFAULT_WITHIN`]. `None`
/// for a step that is not an await.
#[must_use]
pub const fn deadline(step: &Step) -> Option<Duration> {
    match step {
        Step::AwaitMembers { within, .. }
        | Step::AwaitStatus { within, .. }
        | Step::AwaitSettled { within, .. } => Some(match within {
            Some(within) => *within,
            None => DEFAULT_WITHIN,
        }),
        _ => None,
    }
}

/// Whether the condition of the await `step` holds in `digest`. A step that
/// is not an await has no condition and holds.
#[must_use]
pub fn condition_met(step: &Step, baseline: &BTreeMap<SmolStr, u64>, digest: &ModelDigest) -> bool {
    match step {
        Step::AwaitMembers { count, .. } => digest.live == *count,
        Step::AwaitStatus { status, label, .. } => digest
            .statuses
            .get(label.as_str())
            .is_some_and(|held| *held == status.member_status()),
        Step::AwaitSettled { cache, .. } => {
            let view_moved = match (
                baseline.get(cache.as_str()),
                digest.view_hash.get(cache.as_str()),
            ) {
                (_, None) => false,
                (None, Some(_)) => true,
                (Some(before), Some(now)) => before != now,
            };
            view_moved && digest.settled.get(cache.as_str()).copied().unwrap_or(false)
        }
        _ => true,
    }
}

/// What to do about `step` given the digest at `now`. A pause is done once
/// its time has passed. An await is done once its condition holds, and timed
/// out once its deadline has passed; the condition wins a tie. Every other
/// step is done at once.
#[must_use]
pub fn next_action(
    step: &Step,
    state: &DirectorState,
    digest: &ModelDigest,
    now: Instant,
) -> DirectorAction {
    let elapsed = now.saturating_duration_since(state.step_started);
    match step {
        Step::Pause(duration) => {
            if elapsed >= *duration {
                DirectorAction::Done
            } else {
                DirectorAction::Wait
            }
        }
        Step::AwaitMembers { .. } | Step::AwaitStatus { .. } | Step::AwaitSettled { .. } => {
            if condition_met(step, &state.baseline, digest) {
                DirectorAction::Done
            } else if deadline(step).is_some_and(|limit| elapsed >= limit) {
                DirectorAction::TimedOut
            } else {
                DirectorAction::Wait
            }
        }
        _ => DirectorAction::Done,
    }
}

/// How a run behaves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Options {
    /// Headless: a timed-out await or a failed step ends the run with an
    /// error. Otherwise the run notes it and goes on.
    pub headless: bool,
    /// Whether `caption` steps reach the interface.
    pub captions: bool,
}

/// An await that ran out of time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Timeout {
    /// The scenario line.
    pub line: usize,
    /// The step, as written.
    pub step: String,
}

/// What a finished run reports.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Outcome {
    /// How many steps ran.
    pub steps: usize,
    /// The awaits that timed out. Empty in a headless run, which stops at the
    /// first.
    pub timeouts: Vec<Timeout>,
    /// The steps that failed, with the error. Empty in a headless run, which
    /// stops at the first.
    pub failures: Vec<(usize, String)>,
    /// How long the run took.
    pub elapsed: Duration,
}

/// Why a run ended before its last step.
#[derive(Debug)]
pub enum PlayError {
    /// An await ran out of time in a headless run.
    TimedOut(Timeout),
    /// A step failed in a headless run.
    Step {
        /// The scenario line.
        line: usize,
        /// The step, as written.
        step: String,
        /// What went wrong.
        source: anyhow::Error,
    },
    /// Writing the log or the marks failed.
    Io(io::Error),
}

impl fmt::Display for PlayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TimedOut(Timeout { line, step }) => {
                write!(f, "line {line}: `{step}` timed out")
            }
            Self::Step { line, step, source } => {
                write!(f, "line {line}: `{step}` failed: {source:#}")
            }
            Self::Io(error) => write!(f, "writing the scenario log: {error}"),
        }
    }
}

impl std::error::Error for PlayError {}

impl From<io::Error> for PlayError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Plays scenarios.
#[derive(Debug)]
pub struct Director<S> {
    stage: S,
    digests: watch::Receiver<ModelDigest>,
    ui: Option<mpsc::UnboundedSender<UiCommand>>,
    options: Options,
}

impl<S: Stage> Director<S> {
    /// A director that acts on `stage`, reads `digests` and, when there is an
    /// interface, sends it commands.
    #[must_use]
    pub const fn new(
        stage: S,
        digests: watch::Receiver<ModelDigest>,
        ui: Option<mpsc::UnboundedSender<UiCommand>>,
        options: Options,
    ) -> Self {
        Self {
            stage,
            digests,
            ui,
            options,
        }
    }

    fn command(&self, command: UiCommand) {
        if let Some(ui) = &self.ui {
            let _ = ui.send(command);
        }
    }

    /// Does the immediate part of `step`: interface commands and fleet
    /// actions. Waits are the caller's.
    async fn act(&self, step: &Step) -> anyhow::Result<()> {
        match step {
            Step::Caption(text) => {
                if self.options.captions {
                    self.command(UiCommand::Caption(Some(text.clone())));
                }
            }
            Step::Tab(view) => self.command(UiCommand::Tab(*view)),
            Step::Select(label) => self.command(UiCommand::Select(label.as_str().into())),
            Step::Cache(name) => self.command(UiCommand::Cache(name.as_str().into())),
            Step::Help(open) => self.command(UiCommand::Help(*open)),
            Step::Quit => self.command(UiCommand::Quit),
            Step::Spawn { count, stagger } => self.stage.spawn(*count, *stagger).await?,
            Step::Fill(keys) => self.stage.fill(*keys).await?,
            Step::Load(on) => self.stage.load(*on),
            Step::Kill(label) => self.stage.kill(label).await?,
            Step::Leave(label) => self.stage.leave(label).await?,
            Step::Crash(label) => self.stage.crash(label).await?,
            Step::Restart(label) => self.stage.restart(label).await?,
            Step::Pause(_)
            | Step::AwaitMembers { .. }
            | Step::AwaitStatus { .. }
            | Step::AwaitSettled { .. } => {}
        }
        Ok(())
    }

    /// Waits until [`next_action`] says the step is done or out of time.
    async fn wait_for(&self, step: &Step, state: &DirectorState) -> DirectorAction {
        let mut digests = self.digests.clone();
        loop {
            let digest = digests.borrow_and_update().clone();
            match next_action(step, state, &digest, Instant::now()) {
                DirectorAction::Wait => {}
                settled => return settled,
            }
            if let Ok(Err(_)) = tokio::time::timeout(POLL, digests.changed()).await {
                // The publisher is gone: the digest will not change again.
                tokio::time::sleep(POLL).await;
            }
        }
    }

    /// Plays every step of `scenario`, writing one line per step and per
    /// finished wait to `out` and, when given, one `<seconds> <step>` line
    /// per step to `marks`. A scenario that ends without a `quit` step ends
    /// as if it had one.
    ///
    /// # Errors
    ///
    /// In a headless run, returns the first await that times out or step that
    /// fails. In any run, returns a failure to write `out` or `marks`.
    pub async fn play<W: Write>(
        &self,
        scenario: &Scenario,
        out: &mut W,
        mut marks: Option<&mut dyn Write>,
    ) -> Result<Outcome, PlayError> {
        let start = Instant::now();
        let mut state = DirectorState {
            step_started: start,
            baseline: BTreeMap::new(),
        };
        let mut outcome = Outcome::default();
        for entry in &scenario.steps {
            let step = &entry.step;
            let line = entry.line;
            let text = step.to_string();
            state.step_started = Instant::now();
            let at = state.step_started.duration_since(start).as_secs_f64();
            writeln!(out, "[{at:7.1}s] {text}")?;
            if let Some(marks) = marks.as_deref_mut() {
                writeln!(marks, "{at:.3} {text}")?;
                marks.flush()?;
            }
            if is_fleet_action(step) {
                state.baseline.clone_from(&self.digests.borrow().view_hash);
            }
            if let Err(source) = self.act(step).await {
                if self.options.headless {
                    return Err(PlayError::Step {
                        line,
                        step: text,
                        source,
                    });
                }
                writeln!(out, "[{at:7.1}s]   failed: {source:#}")?;
                outcome.failures.push((line, format!("{source:#}")));
            } else if let Some(timeout) = self.finish(step, &state, line, &text, out).await? {
                if self.options.headless {
                    return Err(PlayError::TimedOut(timeout));
                }
                outcome.timeouts.push(timeout);
            }
            outcome.steps += 1;
            if matches!(step, Step::Quit) {
                break;
            }
        }
        outcome.elapsed = start.elapsed();
        writeln!(
            out,
            "[{:7.1}s] scenario done: {} steps, {} timeouts, {} failures",
            outcome.elapsed.as_secs_f64(),
            outcome.steps,
            outcome.timeouts.len(),
            outcome.failures.len()
        )?;
        Ok(outcome)
    }

    /// Waits out a pause or an await. Returns the timeout when an await ran
    /// out of time; `None` for every other end, including a step that does
    /// not wait.
    async fn finish<W: Write>(
        &self,
        step: &Step,
        state: &DirectorState,
        line: usize,
        text: &str,
        out: &mut W,
    ) -> io::Result<Option<Timeout>> {
        let pause = matches!(step, Step::Pause(_));
        if !pause && deadline(step).is_none() {
            return Ok(None);
        }
        let action = self.wait_for(step, state).await;
        if pause {
            return Ok(None);
        }
        let took = Instant::now().saturating_duration_since(state.step_started);
        if action == DirectorAction::TimedOut {
            writeln!(out, "           timed out after {:.1}s", took.as_secs_f64())?;
            self.command(UiCommand::Caption(Some(TIMED_OUT_CAPTION.to_owned())));
            return Ok(Some(Timeout {
                line,
                step: text.to_owned(),
            }));
        }
        writeln!(out, "           ok after {:.1}s", took.as_secs_f64())?;
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use sundog::observe::MemberStatus;

    use super::*;
    use crate::scenario::{AwaitedStatus, parse};

    const S: fn(u64) -> Duration = Duration::from_secs;

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

    fn state(started: Instant) -> DirectorState {
        DirectorState {
            step_started: started,
            baseline: BTreeMap::new(),
        }
    }

    fn settled_step(within: Option<Duration>) -> Step {
        Step::AwaitSettled {
            cache: "it".into(),
            within,
        }
    }

    #[test]
    fn a_pause_is_done_when_its_time_has_passed() {
        let t0 = Instant::now();
        let step = Step::Pause(S(3));
        let d = ModelDigest::default();
        assert_eq!(next_action(&step, &state(t0), &d, t0), DirectorAction::Wait);
        assert_eq!(
            next_action(&step, &state(t0), &d, t0 + Duration::from_millis(2999)),
            DirectorAction::Wait
        );
        assert_eq!(
            next_action(&step, &state(t0), &d, t0 + S(3)),
            DirectorAction::Done
        );
    }

    #[test]
    fn steps_that_do_not_wait_are_done_at_once() {
        let t0 = Instant::now();
        let d = ModelDigest::default();
        for step in [
            Step::Caption("x".into()),
            Step::Spawn {
                count: 3,
                stagger: None,
            },
            Step::Fill(10),
            Step::Load(true),
            Step::Kill("n1".into()),
            Step::Tab(crate::ui::View::Node),
            Step::Select("n1".into()),
            Step::Help(true),
            Step::Quit,
        ] {
            assert_eq!(
                next_action(&step, &state(t0), &d, t0),
                DirectorAction::Done,
                "{step}"
            );
        }
    }

    #[test]
    fn await_members_counts_live_members_exactly() {
        let t0 = Instant::now();
        let step = Step::AwaitMembers {
            count: 3,
            within: Some(S(20)),
        };
        for (live, want) in [
            (0, DirectorAction::Wait),
            (2, DirectorAction::Wait),
            (3, DirectorAction::Done),
            (4, DirectorAction::Wait),
        ] {
            assert_eq!(
                next_action(&step, &state(t0), &digest(live), t0),
                want,
                "{live}"
            );
        }
    }

    #[test]
    fn await_a_status_reads_the_slot_label_and_the_status() {
        let t0 = Instant::now();
        let mut d = digest(2);
        d.statuses.insert("n3".into(), MemberStatus::Live);
        d.statuses.insert("n2".into(), MemberStatus::Departing);
        let step = |status, label: &str| Step::AwaitStatus {
            status,
            label: label.into(),
            within: Some(S(5)),
        };
        let at = |step: &Step, d: &ModelDigest| next_action(step, &state(t0), d, t0);
        assert_eq!(
            at(&step(AwaitedStatus::Departing, "n2"), &d),
            DirectorAction::Done
        );
        assert_eq!(
            at(&step(AwaitedStatus::Left, "n2"), &d),
            DirectorAction::Wait
        );
        assert_eq!(
            at(&step(AwaitedStatus::Down, "n3"), &d),
            DirectorAction::Wait
        );
        assert_eq!(
            at(&step(AwaitedStatus::Down, "n9"), &d),
            DirectorAction::Wait
        );
        d.statuses.insert("n3".into(), MemberStatus::Down);
        assert_eq!(
            at(&step(AwaitedStatus::Down, "n3"), &d),
            DirectorAction::Done
        );
    }

    #[test]
    fn a_settled_await_does_not_pass_on_the_view_that_held_before() {
        let t0 = Instant::now();
        let step = settled_step(Some(S(25)));
        let mut st = state(t0);
        st.baseline.insert("it".into(), 7);

        // The view has not moved from the baseline, however settled it is.
        let same = with_cache(digest(3), 7, true);
        assert_eq!(next_action(&step, &st, &same, t0), DirectorAction::Wait);
        // It moved but has not settled.
        let moving = with_cache(digest(4), 8, false);
        assert_eq!(next_action(&step, &st, &moving, t0), DirectorAction::Wait);
        // It moved and settled.
        let done = with_cache(digest(4), 8, true);
        assert_eq!(next_action(&step, &st, &done, t0), DirectorAction::Done);
    }

    #[test]
    fn a_settled_await_with_no_baseline_needs_a_view_and_settling() {
        let t0 = Instant::now();
        let step = settled_step(Some(S(25)));
        let st = state(t0);
        assert_eq!(
            next_action(&step, &st, &ModelDigest::default(), t0),
            DirectorAction::Wait
        );
        let mut no_view = digest(3);
        no_view.settled.insert("it".into(), true);
        assert_eq!(next_action(&step, &st, &no_view, t0), DirectorAction::Wait);
        assert_eq!(
            next_action(&step, &st, &with_cache(digest(3), 1, false), t0),
            DirectorAction::Wait
        );
        assert_eq!(
            next_action(&step, &st, &with_cache(digest(3), 1, true), t0),
            DirectorAction::Done
        );
    }

    #[test]
    fn an_await_times_out_at_its_deadline_and_the_condition_wins_a_tie() {
        let t0 = Instant::now();
        let step = Step::AwaitMembers {
            count: 3,
            within: Some(S(20)),
        };
        let st = state(t0);
        assert_eq!(
            next_action(&step, &st, &digest(2), t0 + Duration::from_millis(19_999)),
            DirectorAction::Wait
        );
        assert_eq!(
            next_action(&step, &st, &digest(2), t0 + S(20)),
            DirectorAction::TimedOut
        );
        assert_eq!(
            next_action(&step, &st, &digest(3), t0 + S(20)),
            DirectorAction::Done
        );
    }

    #[test]
    fn an_await_without_within_waits_the_default() {
        let t0 = Instant::now();
        let step = Step::AwaitMembers {
            count: 3,
            within: None,
        };
        assert_eq!(deadline(&step), Some(DEFAULT_WITHIN));
        assert_eq!(
            next_action(&step, &state(t0), &digest(0), t0 + DEFAULT_WITHIN),
            DirectorAction::TimedOut
        );
        assert_eq!(deadline(&Step::Pause(S(1))), None);
        assert_eq!(deadline(&Step::Quit), None);
        assert_eq!(deadline(&settled_step(Some(S(7)))), Some(S(7)));
    }

    #[test]
    fn only_steps_that_change_the_cluster_are_fleet_actions() {
        for line in ["spawn 2", "kill n1", "leave n1", "crash n1", "restart n1"] {
            let step = parse(line).unwrap().steps.remove(0).step;
            assert!(is_fleet_action(&step), "{line}");
        }
        for line in [
            "fill 10",
            "load start",
            "pause 1s",
            "tab node",
            "await members 1",
            "quit",
            "caption \"x\"",
        ] {
            let step = parse(line).unwrap().steps.remove(0).step;
            assert!(!is_fleet_action(&step), "{line}");
        }
    }

    /// A stage that records what it is asked and, for a spawn, moves the
    /// digest the way a real cluster would: members join, the view changes,
    /// and the cache settles a moment later.
    struct Fake {
        calls: Mutex<Vec<String>>,
        digests: watch::Sender<ModelDigest>,
        fail_kill: bool,
        settle_after: Duration,
    }

    impl Fake {
        fn new(digests: watch::Sender<ModelDigest>) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                digests,
                fail_kill: false,
                settle_after: Duration::from_millis(500),
            }
        }

        fn note(&self, call: String) {
            self.calls.lock().unwrap().push(call);
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
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
            Ok(())
        }

        async fn leave(&self, label: &str) -> anyhow::Result<()> {
            self.note(format!("leave {label}"));
            Ok(())
        }

        async fn crash(&self, label: &str) -> anyhow::Result<()> {
            self.note(format!("crash {label}"));
            Ok(())
        }

        async fn restart(&self, label: &str) -> anyhow::Result<()> {
            self.note(format!("restart {label}"));
            Ok(())
        }
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
            .play(&scenario, &mut out, Some(&mut marks))
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
        let outcome = rig.director.play(&scenario, &mut out, None).await.unwrap();
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
            parse("spawn 1\nawait settled it within 5s\nspawn 1\nawait settled it within 5s")
                .unwrap();
        let mut out = Vec::new();
        let outcome = rig.director.play(&scenario, &mut out, None).await.unwrap();
        assert!(outcome.timeouts.is_empty());
        assert_eq!(outcome.steps, 4);
    }

    #[tokio::test(start_paused = true)]
    async fn a_headless_run_that_times_out_is_an_error_naming_the_line() {
        let rig = rig(HEADLESS, |_| {});
        let scenario = parse("pause 1s\nawait members 9 within 2s\nquit").unwrap();
        let mut out = Vec::new();
        let error = rig
            .director
            .play(&scenario, &mut out, None)
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
        let mut rig = rig(SHOW, |_| {});
        let scenario = parse("await members 9 within 2s\nfill 5\nquit").unwrap();
        let mut out = Vec::new();
        let outcome = rig.director.play(&scenario, &mut out, None).await.unwrap();
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
        let headless = rig(HEADLESS, |fake| fake.fail_kill = true);
        let scenario = parse("fill 1\nkill n7\nfill 2").unwrap();
        let mut out = Vec::new();
        let error = headless
            .director
            .play(&scenario, &mut out, None)
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
            .play(&scenario, &mut out, None)
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
            .play(&scenario, &mut out, None)
            .await
            .unwrap();
        assert_eq!(
            quiet.ui.try_recv().unwrap(),
            UiCommand::Tab(crate::ui::View::Node)
        );
        assert!(quiet.ui.try_recv().is_err());

        let (tx, rx) = watch::channel(ModelDigest::default());
        let none = Director::new(Fake::new(tx), rx, None, HEADLESS);
        let mut out = Vec::new();
        let outcome = none.play(&scenario, &mut out, None).await.unwrap();
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
            .play(&scenario, &mut Broken, None)
            .await
            .unwrap_err();
        assert!(matches!(error, PlayError::Io(_)), "{error}");
        let mut out = Vec::new();
        let error = rig
            .director
            .play(&scenario, &mut out, Some(&mut Broken))
            .await
            .unwrap_err();
        assert!(matches!(error, PlayError::Io(_)), "{error}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_scenario_stops_at_quit_and_an_empty_scenario_is_done_at_once() {
        let rig = rig(HEADLESS, |_| {});
        let scenario = parse("fill 1\nquit\nfill 2").unwrap();
        let mut out = Vec::new();
        let outcome = rig.director.play(&scenario, &mut out, None).await.unwrap();
        assert_eq!(outcome.steps, 2);
        assert_eq!(rig.director.stage.calls(), ["fill 1"]);

        let empty = rig
            .director
            .play(&Scenario::default(), &mut Vec::new(), None)
            .await
            .unwrap();
        assert_eq!(empty.steps, 0);
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
        }
        let (tx, rx) = watch::channel(ModelDigest::default());
        let director = Director::new(Tour { digests: tx }, rx, None, HEADLESS);
        let scenario = parse(crate::scenario::TOUR).unwrap();
        let mut out = Vec::new();
        let outcome = director.play(&scenario, &mut out, None).await.unwrap();
        assert_eq!(outcome.steps, scenario.steps.len());
        assert!(outcome.timeouts.is_empty());
    }
}

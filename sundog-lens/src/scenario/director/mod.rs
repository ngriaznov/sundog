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
//! An `explain` step asks the stage's test nodes about a key and waits for
//! their answers to pass, up to its deadline. The answers pass when every
//! node read the key and all agree with each other and with the placement the
//! lens computes from the ownership shares in the digest: [`placement_in`]
//! places the key and [`judge`] decides, on the [`Verdict`] the overlay draws.
//! Answers that do not pass yet are asked for again every [`ASK_AGAIN`], as an
//! `await` looks at the digest again: a node that is still opening its caches
//! refuses the connection, and a view settles in a moment. The step fails
//! with the last reason once the deadline passes. The interface gets the
//! answers that ended the wait, passing or not, so the overlay shows what the
//! nodes said.
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

use super::{AwaitedStatus, Scenario, Step, askable_key};
use crate::app::UiCommand;
use crate::explained::{self, Explained, Verdict, verdict};
use crate::key::KeySpec;
use crate::locate::Placement;
use crate::model::digest::ModelDigest;

/// How long an `await` without `within` waits.
pub const DEFAULT_WITHIN: Duration = Duration::from_secs(30);

/// The longest the director goes without looking at the digest.
pub const POLL: Duration = Duration::from_millis(50);

/// The caption a timed-out await leaves on screen.
pub const TIMED_OUT_CAPTION: &str = "await timed out";

/// The time between the asks of an `explain` step whose answers do not pass
/// yet: a node that is still opening its caches refuses the connection, and
/// the lens may not have ranked the cache.
pub const ASK_AGAIN: Duration = Duration::from_millis(250);

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
    /// Asks every running node what it makes of reading `key`, a `String`
    /// key of visible ASCII characters. The answers are in slot order; a node
    /// that cannot answer keeps its place with the reason in words.
    async fn explain(&self, key: &str) -> anyhow::Result<Explained>;
}

/// The node `step` removes without a goodbye and how the scenario names the
/// way: a `kill` is a SIGKILL, a `crash` is the node's own crash.
#[must_use]
pub fn abrupt_exit(step: &Step) -> Option<(&str, &'static str)> {
    match step {
        Step::Kill(label) => Some((label, "SIGKILL")),
        Step::Crash(label) => Some((label, "crash")),
        _ => None,
    }
}

/// The caption that makes the failure detector's delay visible: how long
/// after its abrupt exit (`how`, as [`abrupt_exit`] names it) the lens saw
/// `label` go down.
#[must_use]
pub fn detected_caption(label: &str, how: &str, after: Duration) -> String {
    format!(
        "{label} marked down {:.1} s after the {how}",
        after.as_secs_f64()
    )
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

/// How long the await or `explain` `step` waits: its `within`, or
/// [`DEFAULT_WITHIN`]. `None` for any other step.
#[must_use]
pub const fn deadline(step: &Step) -> Option<Duration> {
    match step {
        Step::AwaitMembers { within, .. }
        | Step::AwaitStatus { within, .. }
        | Step::AwaitSettled { within, .. }
        | Step::Explain { within, .. } => Some(match within {
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

/// How long an `explain` step waits before it asks again, `took` into a wait
/// of `limit`: [`ASK_AGAIN`], or what is left of `limit` when that is less.
/// `None` once `limit` has passed. The wait works on durations, so a `limit`
/// no instant can hold is a wait that does not end.
#[must_use]
pub fn ask_again_in(took: Duration, limit: Duration) -> Option<Duration> {
    let left = limit.saturating_sub(took);
    (!left.is_zero()).then(|| ASK_AGAIN.min(left))
}

/// Where the lens places `key` in the cache the nodes explained: the cache
/// the first reading names, in the ownership shares `digest` holds. `None`
/// when no node read the key or the digest holds no shares of that cache.
#[must_use]
pub fn placement_in(digest: &ModelDigest, answers: &Explained, key: &KeySpec) -> Option<Placement> {
    let (_, reading) = answers.readings().next()?;
    let held = digest.ownership.get(reading.cache.as_str())?;
    Some(Placement::of(&held.0, key))
}

/// Whether the nodes' `answers` pass: every node asked read the key and all
/// agree with each other and with `placement`, the lens's own. `Ok` holds
/// the overlay's sentence for it, `5 nodes agree, the lens computes the same`;
/// `Err` says in words who gave no answer or what differs, without the
/// overlay's hints about keys to press or blocks on screen.
///
/// # Errors
///
/// Returns why the answers do not pass: no node was asked, a node gave no
/// explanation, or the [`Verdict`] is anything but agreement among all of
/// them.
pub fn judge(answers: &Explained, placement: Option<&Placement>) -> Result<String, String> {
    let asked = answers.nodes.len();
    if asked == 0 {
        return Err("no node was asked".to_owned());
    }
    let silent: Vec<String> = answers
        .nodes
        .iter()
        .filter_map(|answer| match &answer.outcome {
            explained::Outcome::Failed(why) => Some(format!("{}: {why}", answer.label)),
            explained::Outcome::Read(_) => None,
        })
        .collect();
    if !silent.is_empty() {
        return Err(format!(
            "{} of {asked} {} gave no explanation: {}",
            silent.len(),
            if asked == 1 { "node" } else { "nodes" },
            silent.join("; ")
        ));
    }
    match verdict(answers, placement) {
        agree @ Verdict::Agree { nodes } if nodes == asked => Ok(agree.words(false)),
        Verdict::Agree { nodes } => Err(format!(
            "only {nodes} of {asked} nodes gave a Distributed reading"
        )),
        Verdict::Moved { from, to } => Err(format!("view moved {from} -> {to}")),
        Verdict::OtherCache { asked, shown } => Err(format!(
            "the nodes explained cache {asked} and the lens places the key in cache {shown}: \
             nothing to compare"
        )),
        other => Err(other.words(false)),
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

/// How a step that ran ended.
enum Ended {
    /// It did what it was to do.
    Done,
    /// It waited for something that did not happen in time.
    TimedOut(Timeout),
    /// It failed, for the reason the error gives.
    Failed(anyhow::Error),
}

/// A step that waits: where it sits in the scenario and when it began.
struct Waiting<'a> {
    /// The scenario line.
    line: usize,
    /// The step, as written.
    text: &'a str,
    /// When the step began.
    began: Instant,
    /// The origin of the run's stamps.
    start: Instant,
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
            Step::ExplainClose => self.command(UiCommand::CloseExplain),
            Step::Quit => self.command(UiCommand::Quit),
            Step::Spawn { count, stagger } => self.stage.spawn(*count, *stagger).await?,
            Step::Fill(keys) => self.stage.fill(*keys).await?,
            Step::Load(on) => self.stage.load(*on),
            Step::Kill(label) => self.stage.kill(label).await?,
            Step::Leave(label) => self.stage.leave(label).await?,
            Step::Crash(label) => self.stage.crash(label).await?,
            Step::Restart(label) => self.stage.restart(label).await?,
            Step::Pause(_)
            | Step::Explain { .. }
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
                // The publisher is gone: the digest does not change again.
                tokio::time::sleep(POLL).await;
            }
        }
    }

    /// Plays every step of `scenario`, writing one line per step to `out`
    /// when it starts and one for each wait that ends, and, when given, one
    /// `<seconds> <step>` line per step to `marks`. Every stamp counts from
    /// `start`, so the caller's own lines share the origin. A scenario that
    /// ends without a `quit` step ends as if it had one: the director tells
    /// the interface to quit.
    ///
    /// # Errors
    ///
    /// In a headless run, returns the first await that times out or step that
    /// fails. In any run, returns a failure to write `out` or `marks`.
    pub async fn play<W: Write>(
        &self,
        scenario: &Scenario,
        start: Instant,
        out: &mut W,
        mut marks: Option<&mut dyn Write>,
    ) -> Result<Outcome, PlayError> {
        let mut state = DirectorState {
            step_started: Instant::now(),
            baseline: BTreeMap::new(),
        };
        let mut outcome = Outcome::default();
        let mut quit = false;
        // The last abrupt exit: who, how, and when the stage reported it done.
        let mut exited: Option<(String, &'static str, Instant)> = None;
        for entry in &scenario.steps {
            let step = &entry.step;
            let line = entry.line;
            let text = step.to_string();
            state.step_started = Instant::now();
            let at = state
                .step_started
                .saturating_duration_since(start)
                .as_secs_f64();
            writeln!(out, "[{at:7.1}s] {text}")?;
            if let Some(marks) = marks.as_deref_mut() {
                writeln!(marks, "{at:.3} {text}")?;
                marks.flush()?;
            }
            if is_fleet_action(step) {
                state.baseline.clone_from(&self.digests.borrow().view_hash);
            }
            let ended = match self.act(step).await {
                Ok(()) => self.finish(step, &state, start, line, &text, out).await?,
                Err(source) => Ended::Failed(source),
            };
            match ended {
                Ended::Done => {
                    if let Some((label, how)) = abrupt_exit(step) {
                        exited = Some((label.to_owned(), how, Instant::now()));
                    }
                    self.show_detection(step, exited.as_ref());
                }
                Ended::TimedOut(timeout) => {
                    if self.options.headless {
                        return Err(PlayError::TimedOut(timeout));
                    }
                    outcome.timeouts.push(timeout);
                }
                Ended::Failed(source) => {
                    if self.options.headless {
                        return Err(PlayError::Step {
                            line,
                            step: text,
                            source,
                        });
                    }
                    tracing::warn!(line, step = %text, "step failed: {source:#}");
                    writeln!(
                        out,
                        "[{:7.1}s]   failed: {source:#}",
                        Instant::now()
                            .saturating_duration_since(start)
                            .as_secs_f64()
                    )?;
                    outcome.failures.push((line, format!("{source:#}")));
                }
            }
            outcome.steps += 1;
            if matches!(step, Step::Quit) {
                quit = true;
                break;
            }
        }
        if !quit {
            self.command(UiCommand::Quit);
        }
        outcome.elapsed = Instant::now().saturating_duration_since(start);
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

    /// After `await down <label>` for the node `exited` names, captions how
    /// long the failure detector took.
    fn show_detection(&self, step: &Step, exited: Option<&(String, &'static str, Instant)>) {
        if !self.options.captions {
            return;
        }
        if let Step::AwaitStatus {
            status: AwaitedStatus::Down,
            label,
            ..
        } = step
            && let Some((who, how, at)) = exited
            && who == label
        {
            let after = Instant::now().saturating_duration_since(*at);
            self.command(UiCommand::Caption(Some(detected_caption(
                label, how, after,
            ))));
        }
    }

    /// Waits out a pause, an await or an `explain`. A step that does not wait
    /// is done at once.
    async fn finish<W: Write>(
        &self,
        step: &Step,
        state: &DirectorState,
        start: Instant,
        line: usize,
        text: &str,
        out: &mut W,
    ) -> io::Result<Ended> {
        let waiting = Waiting {
            line,
            text,
            began: state.step_started,
            start,
        };
        if let Step::Explain { key, .. } = step {
            let limit = deadline(step).unwrap_or(DEFAULT_WITHIN);
            return self.await_answers(key, limit, &waiting, out).await;
        }
        let pause = matches!(step, Step::Pause(_));
        if !pause && deadline(step).is_none() {
            return Ok(Ended::Done);
        }
        let action = self.wait_for(step, state).await;
        if pause {
            return Ok(Ended::Done);
        }
        if action == DirectorAction::TimedOut {
            return self.timed_out(&waiting, out);
        }
        let (at, took) = waiting.elapsed();
        writeln!(out, "[{at:7.1}s]   ok after {took:.1}s")?;
        Ok(Ended::Done)
    }

    /// Notes that a step ran out of time: a line in the log, a warning when
    /// there is an interface, and a caption on it.
    fn timed_out<W: Write>(&self, waiting: &Waiting<'_>, out: &mut W) -> io::Result<Ended> {
        let (at, took) = waiting.elapsed();
        if !self.options.headless {
            tracing::warn!(
                line = waiting.line,
                step = %waiting.text,
                "await timed out after {took:.1}s"
            );
        }
        writeln!(out, "[{at:7.1}s]   timed out after {took:.1}s")?;
        self.command(UiCommand::Caption(Some(TIMED_OUT_CAPTION.to_owned())));
        Ok(Ended::TimedOut(Timeout {
            line: waiting.line,
            step: waiting.text.to_owned(),
        }))
    }

    /// Asks the nodes about the key an `explain` step types, `typed`, until
    /// their answers pass or `limit` has passed since the step began, asking
    /// again every [`ASK_AGAIN`]. A stage that cannot ask fails the step at
    /// once. When time runs out, the step has timed out if no ask was
    /// answered, and has failed with the last reason if any was. The
    /// interface gets the answers that ended the wait.
    async fn await_answers<W: Write>(
        &self,
        typed: &str,
        limit: Duration,
        waiting: &Waiting<'_>,
        out: &mut W,
    ) -> io::Result<Ended> {
        let mut last: Option<(Explained, String)> = None;
        loop {
            let left = limit.saturating_sub(waiting.took());
            let Ok(asked) = tokio::time::timeout(left, self.ask(typed)).await else {
                break;
            };
            let (answers, judged) = match asked {
                Ok(asked) => asked,
                Err(source) => return Ok(Ended::Failed(source)),
            };
            let why = match judged {
                Ok(words) => {
                    self.show_answers(typed, answers);
                    let (at, took) = waiting.elapsed();
                    writeln!(out, "[{at:7.1}s]   ok after {took:.1}s: {words}")?;
                    return Ok(Ended::Done);
                }
                Err(why) => why,
            };
            if last.as_ref().is_none_or(|(_, before)| *before != why) {
                let (at, took) = waiting.elapsed();
                writeln!(out, "[{at:7.1}s]   not yet after {took:.1}s: {why}")?;
            }
            last = Some((answers, why));
            let Some(wait) = ask_again_in(waiting.took(), limit) else {
                break;
            };
            tokio::time::sleep(wait).await;
        }
        match last {
            Some((answers, why)) => {
                self.show_answers(typed, answers);
                Ok(Ended::Failed(anyhow::Error::msg(why)))
            }
            None => self.timed_out(waiting, out),
        }
    }

    /// Opens the explain overlay on the key `typed` with `answers` in.
    fn show_answers(&self, typed: &str, answers: Explained) {
        self.command(UiCommand::Explain {
            key: typed.to_owned(),
            answers: Box::new(answers),
        });
    }

    /// Asks the stage's nodes about the key `typed` and judges the answers
    /// against the placement in the digest as it stands once they are in.
    async fn ask(&self, typed: &str) -> anyhow::Result<(Explained, Result<String, String>)> {
        let spec = askable_key(typed).map_err(anyhow::Error::msg)?;
        let answers = self.stage.explain(spec.text()).await?;
        let placement = placement_in(&self.digests.borrow(), &answers, &spec);
        let judged = judge(&answers, placement.as_ref());
        Ok((answers, judged))
    }
}

impl Waiting<'_> {
    /// How long the step has taken.
    fn took(&self) -> Duration {
        Instant::now().saturating_duration_since(self.began)
    }

    /// The seconds from the origin of the run's stamps to now, and the
    /// seconds the step has taken.
    fn elapsed(&self) -> (f64, f64) {
        let now = Instant::now();
        (
            now.saturating_duration_since(self.start).as_secs_f64(),
            now.saturating_duration_since(self.began).as_secs_f64(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::SystemTime;

    use sundog::observe::MemberStatus;

    use super::*;
    use crate::ask::SCRIPTED_ID;
    use crate::explained::{Outcome, fixture};
    use crate::model::digest::HeldShares;
    use crate::model::testkit;
    use crate::scenario::{AwaitedStatus, Entry, parse};

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
            Step::Explain {
                key: "k1".into(),
                within: Some(S(5)),
            },
            Step::ExplainClose,
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
    fn an_explain_waits_for_its_answers_up_to_its_within_or_the_default() {
        let explain = |within| Step::Explain {
            key: "k1".into(),
            within,
        };
        assert_eq!(deadline(&explain(Some(S(4)))), Some(S(4)));
        assert_eq!(deadline(&explain(None)), Some(DEFAULT_WITHIN));
        assert_eq!(deadline(&Step::ExplainClose), None);
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
            "explain k1",
            "explain close",
            "quit",
            "caption \"x\"",
        ] {
            let step = parse(line).unwrap().steps.remove(0).step;
            assert!(!is_fleet_action(&step), "{line}");
        }
    }

    #[test]
    fn condition_met_holds_for_each_await_and_for_non_awaits() {
        let members = Step::AwaitMembers {
            count: 3,
            within: None,
        };
        let none = BTreeMap::new();
        assert!(condition_met(&members, &none, &digest(3)));
        assert!(!condition_met(&members, &none, &digest(2)));

        let mut d = digest(2);
        d.statuses.insert("n2".into(), MemberStatus::Departing);
        let departing = Step::AwaitStatus {
            status: AwaitedStatus::Departing,
            label: "n2".into(),
            within: None,
        };
        let left = Step::AwaitStatus {
            status: AwaitedStatus::Left,
            label: "n2".into(),
            within: None,
        };
        assert!(condition_met(&departing, &none, &d));
        assert!(!condition_met(&left, &none, &d));

        let settled = settled_step(None);
        let base: BTreeMap<SmolStr, u64> = [("it".into(), 7)].into();
        // Same view as the baseline: not met, however settled.
        assert!(!condition_met(
            &settled,
            &base,
            &with_cache(digest(3), 7, true)
        ));
        // A different view, settled: met.
        assert!(condition_met(
            &settled,
            &base,
            &with_cache(digest(3), 8, true)
        ));
        assert!(!condition_met(
            &settled,
            &base,
            &with_cache(digest(3), 8, false)
        ));
        // No baseline: any view counts once it has settled.
        assert!(condition_met(
            &settled,
            &none,
            &with_cache(digest(3), 1, true)
        ));
        // No view at all.
        assert!(!condition_met(&settled, &base, &digest(3)));

        // A step that is not an await has no condition and holds.
        for step in [
            Step::Quit,
            Step::Pause(S(1)),
            Step::Fill(1),
            Step::Load(true),
            Step::ExplainClose,
            Step::Explain {
                key: "k1".into(),
                within: None,
            },
        ] {
            assert!(
                condition_met(&step, &none, &ModelDigest::default()),
                "{step}"
            );
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
            parse("spawn 1\nawait settled it within 5s\nspawn 1\nawait settled it within 5s")
                .unwrap();
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

    /// The nodes' answers about `k1`, and the lens's placement of it.
    fn k1() -> (Explained, Placement) {
        let model = cluster();
        let ranked = model.ownership("it").expect("the fixture ranks it");
        let spec = KeySpec::parse("k1").unwrap();
        (
            fixture::explained(model, SCRIPTED_ID, "k1", SystemTime::UNIX_EPOCH),
            Placement::of(&ranked.shares, &spec),
        )
    }

    /// The reading of node `at` of `answers`.
    fn reading_mut(answers: &mut Explained, at: usize) -> &mut crate::explained::Reading {
        match &mut answers.nodes[at].outcome {
            Outcome::Read(reading) => reading,
            Outcome::Failed(why) => panic!("node {at} failed: {why}"),
        }
    }

    #[test]
    fn answers_pass_only_when_every_node_read_the_key_and_all_agree_with_the_lens() {
        let (agreeing, placement) = k1();
        let judged = |answers: &Explained| judge(answers, Some(&placement));
        assert_eq!(
            judged(&agreeing),
            Ok("5 nodes agree, the lens computes the same".to_owned())
        );

        let mut nobody = agreeing.clone();
        nobody.nodes.clear();
        assert_eq!(judged(&nobody), Err("no node was asked".to_owned()));

        let mut silent = agreeing.clone();
        silent.nodes[1].outcome = Outcome::Failed("no answer within 3 s".to_owned());
        silent.nodes[3].outcome = Outcome::Failed("no answer (connection refused)".to_owned());
        assert_eq!(
            judged(&silent),
            Err(
                "2 of 5 nodes gave no explanation: n2: no answer within 3 s; \
                 n4: no answer (connection refused)"
                    .to_owned()
            )
        );

        let mut lone = agreeing.clone();
        lone.nodes.truncate(1);
        lone.nodes[0].outcome = Outcome::Failed("no answer (connection refused)".to_owned());
        assert_eq!(
            judged(&lone),
            Err("1 of 1 node gave no explanation: n1: no answer (connection refused)".to_owned())
        );

        // Every node holds a view the lens no longer has.
        let mut moved = agreeing.clone();
        for at in 0..5 {
            reading_mut(&mut moved, at)
                .distributed
                .as_mut()
                .unwrap()
                .view = "00c1a2f4e8b3195d".to_owned();
        }
        let why = judged(&moved).unwrap_err();
        assert!(why.starts_with("view moved 00c1a2f4 -> "), "{why}");
        assert!(!why.contains("Enter"), "no key to press in a run: {why}");

        // One node holds another view than the rest.
        let mut split = agreeing.clone();
        reading_mut(&mut split, 2)
            .distributed
            .as_mut()
            .unwrap()
            .view = "00c1a2f4e8b3195d".to_owned();
        let why = judged(&split).unwrap_err();
        assert!(why.starts_with("n1 n2 n4 n5 hold "), "{why}");
        assert!(why.ends_with(", n3 holds 00c1a2f4"), "{why}");

        // One node lists the owners in another order.
        let mut apart = agreeing.clone();
        reading_mut(&mut apart, 4)
            .distributed
            .as_mut()
            .unwrap()
            .owners
            .reverse();
        let why = judged(&apart).unwrap_err();
        assert!(
            why.starts_with("n5 differs from the lens, which holds view "),
            "{why}"
        );

        // One node's cache is not Distributed, so it compares nothing.
        let mut mixed = agreeing.clone();
        reading_mut(&mut mixed, 0).distributed = None;
        assert_eq!(
            judged(&mixed),
            Err("only 4 of 5 nodes gave a Distributed reading".to_owned())
        );

        // The lens has no placement, or places the key in another cache.
        assert_eq!(
            judge(&agreeing, None),
            Err("the lens has no placement of the key to compare with".to_owned())
        );
        let mut elsewhere = placement.clone();
        elsewhere.cache = "other".into();
        assert_eq!(
            judge(&agreeing, Some(&elsewhere)),
            Err(
                "the nodes explained cache it and the lens places the key in cache other: \
                 nothing to compare"
                    .to_owned()
            )
        );
    }

    #[test]
    fn a_key_is_placed_in_the_cache_the_nodes_explained() {
        let (answers, expected) = k1();
        let digest = with_cluster_shares(ModelDigest::default());
        let spec = KeySpec::parse("k1").unwrap();
        assert_eq!(
            placement_in(&digest, &answers, &spec),
            Some(expected.clone())
        );

        // Another key lands on its own part in the same view.
        let k2 = KeySpec::parse("k2").unwrap();
        let other = placement_in(&digest, &answers, &k2).expect("k2 is placed");
        assert_eq!(other.part, sundog::store::PartId::of_key(k2.bytes()));
        assert_eq!(
            (other.cache.as_str(), other.view_hash),
            ("it", expected.view_hash)
        );

        // No shares in the digest, no reading, or a cache the digest does not
        // hold: nothing to place the key in.
        assert_eq!(placement_in(&ModelDigest::default(), &answers, &spec), None);
        let mut failed = answers.clone();
        for answer in &mut failed.nodes {
            answer.outcome = Outcome::Failed("no answer".to_owned());
        }
        assert_eq!(placement_in(&digest, &failed, &spec), None);
        let mut elsewhere = answers.clone();
        for at in 0..5 {
            reading_mut(&mut elsewhere, at).cache = "other".to_owned();
        }
        assert_eq!(placement_in(&digest, &elsewhere, &spec), None);
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
}

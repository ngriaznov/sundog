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
mod tests;

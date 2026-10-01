//! The scenario language: a line-oriented script that the director plays.
//!
//! ```text
//! caption "<text>"         pause <dur>          spawn <n> [stagger <dur>]
//! fill <n>                 load start|stop      kill <label>   leave <label>   crash <label>
//! restart <label>          tab overview|caches|node|timeline
//! select <label>           cache <name>         help on|off
//! await members <n> [within <dur>]
//! await departing|left|down <label> [within <dur>]
//! await settled <cache> [within <dur>]
//! quit
//! ```
//!
//! Blank lines and lines starting with `#` are ignored. [`parse`] is pure and
//! reports the first bad line with its number.

use std::fmt;
use std::time::Duration;

use sundog::observe::MemberStatus;

use crate::cli::parse_duration;
use crate::ui::View;

pub mod director;

/// A member status a scenario can await.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AwaitedStatus {
    /// `await departing`.
    Departing,
    /// `await left`.
    Left,
    /// `await down`.
    Down,
}

/// One scenario step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// Show a caption.
    Caption(String),
    /// Wait.
    Pause(Duration),
    /// Start `count` nodes, `stagger` apart.
    Spawn {
        /// How many nodes.
        count: usize,
        /// The delay between starts.
        stagger: Option<Duration>,
    },
    /// Write `count` keys through the fleet.
    Fill(u64),
    /// Start or stop the load.
    Load(bool),
    /// SIGKILL a node.
    Kill(String),
    /// SIGTERM a node: a graceful leave.
    Leave(String),
    /// Ask a node to crash without leaving.
    Crash(String),
    /// Start a stopped node again at its address.
    Restart(String),
    /// Switch view.
    Tab(View),
    /// Select a member.
    Select(String),
    /// Select a cache.
    Cache(String),
    /// Show or hide the help overlay.
    Help(bool),
    /// Wait for `count` live members.
    AwaitMembers {
        /// The live count to reach.
        count: usize,
        /// The deadline.
        within: Option<Duration>,
    },
    /// Wait for a member to reach a status.
    AwaitStatus {
        /// The status.
        status: AwaitedStatus,
        /// The member's label.
        label: String,
        /// The deadline.
        within: Option<Duration>,
    },
    /// Wait for a cache's ownership to move and settle.
    AwaitSettled {
        /// The cache name.
        cache: String,
        /// The deadline.
        within: Option<Duration>,
    },
    /// End the scenario.
    Quit,
}

impl AwaitedStatus {
    /// The member status the await waits for.
    #[must_use]
    pub const fn member_status(self) -> MemberStatus {
        match self {
            Self::Departing => MemberStatus::Departing,
            Self::Left => MemberStatus::Left,
            Self::Down => MemberStatus::Down,
        }
    }

    /// The word a scenario writes for the status.
    #[must_use]
    pub const fn word(self) -> &'static str {
        match self {
            Self::Departing => "departing",
            Self::Left => "left",
            Self::Down => "down",
        }
    }
}

/// A duration as a scenario writes it: whole seconds as `3s`, otherwise
/// milliseconds as `500ms`.
fn write_duration(f: &mut fmt::Formatter<'_>, duration: Duration) -> fmt::Result {
    if duration.subsec_millis() == 0 {
        write!(f, "{}s", duration.as_secs())
    } else {
        write!(f, "{}ms", duration.as_millis())
    }
}

/// The ` within <dur>` tail of an await, empty without a deadline.
fn write_within(f: &mut fmt::Formatter<'_>, within: Option<Duration>) -> fmt::Result {
    if let Some(within) = within {
        f.write_str(" within ")?;
        write_duration(f, within)?;
    }
    Ok(())
}

impl fmt::Display for Step {
    /// The step as the line that parses back to it.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Caption(text) => {
                f.write_str("caption \"")?;
                for c in text.chars() {
                    if matches!(c, '"' | '\\') {
                        f.write_str("\\")?;
                    }
                    write!(f, "{c}")?;
                }
                f.write_str("\"")
            }
            Self::Pause(duration) => {
                f.write_str("pause ")?;
                write_duration(f, *duration)
            }
            Self::Spawn { count, stagger } => {
                write!(f, "spawn {count}")?;
                if let Some(stagger) = stagger {
                    f.write_str(" stagger ")?;
                    write_duration(f, *stagger)?;
                }
                Ok(())
            }
            Self::Fill(count) => write!(f, "fill {count}"),
            Self::Load(true) => f.write_str("load start"),
            Self::Load(false) => f.write_str("load stop"),
            Self::Kill(label) => write!(f, "kill {label}"),
            Self::Leave(label) => write!(f, "leave {label}"),
            Self::Crash(label) => write!(f, "crash {label}"),
            Self::Restart(label) => write!(f, "restart {label}"),
            Self::Tab(view) => write!(f, "tab {}", view.name()),
            Self::Select(label) => write!(f, "select {label}"),
            Self::Cache(name) => write!(f, "cache {name}"),
            Self::Help(true) => f.write_str("help on"),
            Self::Help(false) => f.write_str("help off"),
            Self::AwaitMembers { count, within } => {
                write!(f, "await members {count}")?;
                write_within(f, *within)
            }
            Self::AwaitStatus {
                status,
                label,
                within,
            } => {
                write!(f, "await {} {label}", status.word())?;
                write_within(f, *within)
            }
            Self::AwaitSettled { cache, within } => {
                write!(f, "await settled {cache}")?;
                write_within(f, *within)
            }
            Self::Quit => f.write_str("quit"),
        }
    }
}

/// The built-in tour, `tour.txt`.
pub const TOUR: &str = include_str!("tour.txt");

/// A step with the line it came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// The 1-based line number.
    pub line: usize,
    /// The step.
    pub step: Step,
}

/// A parsed scenario.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Scenario {
    /// The steps in order.
    pub steps: Vec<Entry>,
}

/// A bad scenario line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScenarioError {
    /// The 1-based line number.
    pub line: usize,
    /// What is wrong.
    pub msg: String,
}

impl fmt::Display for ScenarioError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "line {}: {}", self.line, self.msg)
    }
}

impl std::error::Error for ScenarioError {}

/// Parses `source` into a [`Scenario`].
///
/// # Errors
///
/// Returns the first line that is not a step, with a message.
pub fn parse(source: &str) -> Result<Scenario, ScenarioError> {
    let mut steps = Vec::new();
    for (index, raw) in source.lines().enumerate() {
        let line = index + 1;
        let text = raw.trim();
        if text.is_empty() || text.starts_with('#') {
            continue;
        }
        let step = parse_step(text).map_err(|msg| ScenarioError { line, msg })?;
        steps.push(Entry { line, step });
    }
    Ok(Scenario { steps })
}

/// Parses one non-blank, non-comment line.
fn parse_step(text: &str) -> Result<Step, String> {
    let (keyword, rest) = text.split_once(char::is_whitespace).unwrap_or((text, ""));
    let rest = rest.trim();
    if keyword == "caption" {
        return parse_quoted(rest).map(Step::Caption);
    }
    let words: Vec<&str> = rest.split_whitespace().collect();
    match (keyword, words.as_slice()) {
        ("pause", [dur]) => duration(dur).map(Step::Pause),
        ("spawn", [count]) => Ok(Step::Spawn {
            count: number(count)?,
            stagger: None,
        }),
        ("spawn", [count, "stagger", dur]) => Ok(Step::Spawn {
            count: number(count)?,
            stagger: Some(duration(dur)?),
        }),
        ("fill", [count]) => number(count).map(Step::Fill),
        ("load", ["start"]) => Ok(Step::Load(true)),
        ("load", ["stop"]) => Ok(Step::Load(false)),
        ("kill", [label]) => Ok(Step::Kill((*label).to_owned())),
        ("leave", [label]) => Ok(Step::Leave((*label).to_owned())),
        ("crash", [label]) => Ok(Step::Crash((*label).to_owned())),
        ("restart", [label]) => Ok(Step::Restart((*label).to_owned())),
        ("tab", [name]) => View::from_name(name)
            .map(Step::Tab)
            .ok_or_else(|| format!("unknown tab {name:?}: use overview, caches, node or timeline")),
        ("select", [label]) => Ok(Step::Select((*label).to_owned())),
        ("cache", [name]) => Ok(Step::Cache((*name).to_owned())),
        ("help", ["on"]) => Ok(Step::Help(true)),
        ("help", ["off"]) => Ok(Step::Help(false)),
        ("await", words) => parse_await(words),
        ("quit", []) => Ok(Step::Quit),
        (
            "pause" | "spawn" | "fill" | "load" | "kill" | "leave" | "crash" | "restart" | "tab"
            | "select" | "cache" | "help" | "quit",
            _,
        ) => Err(format!("wrong arguments for {keyword}")),
        _ => Err(format!("unknown step {keyword:?}")),
    }
}

/// Parses the words after `await`.
fn parse_await(words: &[&str]) -> Result<Step, String> {
    let (target, within) = match words {
        [target @ .., "within", dur] => (target, Some(duration(dur)?)),
        target => (target, None),
    };
    match target {
        ["members", count] => Ok(Step::AwaitMembers {
            count: number(count)?,
            within,
        }),
        [status @ ("departing" | "left" | "down"), label] => Ok(Step::AwaitStatus {
            status: match *status {
                "departing" => AwaitedStatus::Departing,
                "left" => AwaitedStatus::Left,
                _ => AwaitedStatus::Down,
            },
            label: (*label).to_owned(),
            within,
        }),
        ["settled", cache] => Ok(Step::AwaitSettled {
            cache: (*cache).to_owned(),
            within,
        }),
        _ => Err(
            "await takes members <n>, departing|left|down <label> or settled <cache>, then an \
             optional within <dur>"
                .to_owned(),
        ),
    }
}

/// Parses a `<n>` count.
fn number<T: std::str::FromStr>(text: &str) -> Result<T, String> {
    text.parse()
        .map_err(|_| format!("{text:?} is not a whole number"))
}

/// Parses a duration word.
fn duration(text: &str) -> Result<Duration, String> {
    parse_duration(text).map_err(|e| format!("{text:?}: {e}"))
}

/// Parses `"<text>"` with `\"` and `\\` escapes, and nothing after the
/// closing quote.
fn parse_quoted(text: &str) -> Result<String, String> {
    let body = text
        .strip_prefix('"')
        .ok_or_else(|| "caption needs a quoted text".to_owned())?;
    let mut out = String::new();
    let mut chars = body.char_indices();
    while let Some((i, c)) = chars.next() {
        match c {
            '"' => {
                if body[i + 1..].trim().is_empty() {
                    return Ok(out);
                }
                return Err("text after the closing quote".to_owned());
            }
            '\\' => match chars.next() {
                Some((_, escaped @ ('"' | '\\'))) => out.push(escaped),
                Some((_, other)) => {
                    out.push('\\');
                    out.push(other);
                }
                None => break,
            },
            other => out.push(other),
        }
    }
    Err("caption is missing its closing quote".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn steps(source: &str) -> Vec<Step> {
        parse(source)
            .unwrap()
            .steps
            .into_iter()
            .map(|e| e.step)
            .collect()
    }

    fn one(line: &str) -> Step {
        let mut all = steps(line);
        assert_eq!(all.len(), 1, "{line}");
        all.remove(0)
    }

    fn fails(line: &str) -> ScenarioError {
        parse(line).unwrap_err()
    }

    const S: fn(u64) -> Duration = Duration::from_secs;

    #[test]
    fn caption_pause_and_spawn() {
        assert_eq!(
            one("caption \"hello, world\""),
            Step::Caption("hello, world".into())
        );
        assert_eq!(one("pause 3s"), Step::Pause(S(3)));
        assert_eq!(one("pause 500ms"), Step::Pause(Duration::from_millis(500)));
        assert_eq!(
            one("spawn 3"),
            Step::Spawn {
                count: 3,
                stagger: None
            }
        );
        assert_eq!(
            one("spawn 3 stagger 1s"),
            Step::Spawn {
                count: 3,
                stagger: Some(S(1))
            }
        );
    }

    #[test]
    fn fill_and_load() {
        assert_eq!(one("fill 20000"), Step::Fill(20_000));
        assert_eq!(one("load start"), Step::Load(true));
        assert_eq!(one("load stop"), Step::Load(false));
    }

    #[test]
    fn fleet_actions_take_a_label() {
        assert_eq!(one("kill n3"), Step::Kill("n3".into()));
        assert_eq!(one("leave n2"), Step::Leave("n2".into()));
        assert_eq!(one("crash n1"), Step::Crash("n1".into()));
        assert_eq!(one("restart n3"), Step::Restart("n3".into()));
    }

    #[test]
    fn view_steps() {
        assert_eq!(one("tab overview"), Step::Tab(View::Overview));
        assert_eq!(one("tab caches"), Step::Tab(View::Caches));
        assert_eq!(one("tab node"), Step::Tab(View::Node));
        assert_eq!(one("tab timeline"), Step::Tab(View::Timeline));
        assert_eq!(one("select n3"), Step::Select("n3".into()));
        assert_eq!(one("cache it"), Step::Cache("it".into()));
        assert_eq!(one("help on"), Step::Help(true));
        assert_eq!(one("help off"), Step::Help(false));
        assert_eq!(one("quit"), Step::Quit);
    }

    #[test]
    fn await_members_with_and_without_a_deadline() {
        assert_eq!(
            one("await members 3"),
            Step::AwaitMembers {
                count: 3,
                within: None
            }
        );
        assert_eq!(
            one("await members 3 within 20s"),
            Step::AwaitMembers {
                count: 3,
                within: Some(S(20))
            }
        );
    }

    #[test]
    fn await_a_member_status() {
        for (word, status) in [
            ("departing", AwaitedStatus::Departing),
            ("left", AwaitedStatus::Left),
            ("down", AwaitedStatus::Down),
        ] {
            assert_eq!(
                one(&format!("await {word} n2 within 5s")),
                Step::AwaitStatus {
                    status,
                    label: "n2".into(),
                    within: Some(S(5))
                }
            );
        }
        assert_eq!(
            one("await down n3"),
            Step::AwaitStatus {
                status: AwaitedStatus::Down,
                label: "n3".into(),
                within: None
            }
        );
    }

    #[test]
    fn await_settled() {
        assert_eq!(
            one("await settled it within 25s"),
            Step::AwaitSettled {
                cache: "it".into(),
                within: Some(S(25))
            }
        );
        assert_eq!(
            one("await settled it"),
            Step::AwaitSettled {
                cache: "it".into(),
                within: None
            }
        );
    }

    #[test]
    fn blank_lines_and_comments_are_skipped_and_lines_are_numbered() {
        let scenario = parse("# a tour\n\n  pause 1s  \n  # note\nquit\n").unwrap();
        assert_eq!(scenario.steps.len(), 2);
        assert_eq!(scenario.steps[0].line, 3);
        assert_eq!(scenario.steps[1].line, 5);
        assert_eq!(parse("").unwrap(), Scenario::default());
    }

    #[test]
    fn captions_unescape_quotes_and_backslashes() {
        assert_eq!(
            one(r#"caption "say \"hi\" \\ done""#),
            Step::Caption(r#"say "hi" \ done"#.into())
        );
        assert_eq!(
            one("caption \"colons: and — dashes\""),
            Step::Caption("colons: and — dashes".into())
        );
        assert_eq!(one("caption \"\""), Step::Caption(String::new()));
        assert_eq!(one("caption \"a\"   "), Step::Caption("a".into()));
    }

    #[test]
    fn bad_captions_are_errors() {
        assert!(fails("caption hello").msg.contains("quoted"));
        assert!(fails("caption \"open").msg.contains("closing quote"));
        assert!(
            fails("caption \"a\" b")
                .msg
                .contains("after the closing quote")
        );
        assert!(fails("caption").msg.contains("quoted"));
    }

    #[test]
    fn a_bad_line_reports_its_number() {
        let error = parse("pause 1s\n\n# note\nfrobnicate now\nquit").unwrap_err();
        assert_eq!(error.line, 4);
        assert!(error.msg.contains("frobnicate"));
        assert_eq!(error.to_string(), format!("line 4: {}", error.msg));
    }

    #[test]
    fn wrong_arguments_are_errors() {
        for line in [
            "pause",
            "pause 3",
            "pause 3s 4s",
            "spawn",
            "spawn x",
            "spawn 3 stagger",
            "spawn 3 delay 1s",
            "fill",
            "fill -1",
            "load",
            "load maybe",
            "kill",
            "kill n1 n2",
            "tab",
            "tab sideways",
            "select",
            "cache",
            "help",
            "help maybe",
            "quit now",
            "await",
            "await members",
            "await members x",
            "await members 3 within",
            "await members 3 within soon",
            "await down",
            "await frozen n1",
            "await settled",
        ] {
            assert!(parse(line).is_err(), "{line}");
        }
    }

    #[test]
    fn errors_carry_a_line_number_even_on_the_first_line() {
        assert_eq!(fails("pause").line, 1);
    }

    /// A script using every step kind.
    const EVERY_STEP: &str = r#"
caption "sundog-lens joins the cluster's gossip as an observer: no cache, no data plane, never a peer"
pause 3s
spawn 3 stagger 1s
await members 3 within 20s
fill 20000
load start
spawn 1
await settled it within 25s
tab caches
select n3
kill n3
await down n3 within 20s
select n2
leave n2
await departing n2 within 5s
await left n2 within 20s
restart n3
await members 4 within 20s
tab node
help on
help off
tab timeline
load stop
cache it
crash n1
quit
"#;

    #[test]
    fn a_script_using_every_step_kind_parses_in_order() {
        let all = steps(EVERY_STEP);
        assert_eq!(all.len(), 26);
        assert!(matches!(all[0], Step::Caption(_)));
        assert!(matches!(all[2], Step::Spawn { count: 3, .. }));
        assert_eq!(all[25], Step::Quit);
    }

    #[test]
    fn every_step_prints_as_the_line_that_parses_back_to_it() {
        let scenario = parse(EVERY_STEP).unwrap();
        for entry in &scenario.steps {
            let line = entry.step.to_string();
            assert_eq!(parse(&line).unwrap().steps[0].step, entry.step, "{line}");
        }
        let printed: Vec<String> = scenario.steps.iter().map(|e| e.step.to_string()).collect();
        assert_eq!(parse(&printed.join("\n")).unwrap().steps.len(), 26);
    }

    #[test]
    fn steps_print_in_the_form_a_scenario_writes() {
        let cases = [
            (
                Step::Caption("say \"hi\" \\ done".into()),
                r#"caption "say \"hi\" \\ done""#,
            ),
            (Step::Pause(Duration::from_millis(500)), "pause 500ms"),
            (Step::Pause(S(3)), "pause 3s"),
            (Step::Pause(S(90)), "pause 90s"),
            (Step::Pause(Duration::from_millis(1500)), "pause 1500ms"),
            (
                Step::Spawn {
                    count: 3,
                    stagger: Some(S(1)),
                },
                "spawn 3 stagger 1s",
            ),
            (
                Step::Spawn {
                    count: 1,
                    stagger: None,
                },
                "spawn 1",
            ),
            (Step::Fill(20_000), "fill 20000"),
            (Step::Load(true), "load start"),
            (Step::Load(false), "load stop"),
            (Step::Kill("n3".into()), "kill n3"),
            (Step::Leave("n2".into()), "leave n2"),
            (Step::Crash("n1".into()), "crash n1"),
            (Step::Restart("n3".into()), "restart n3"),
            (Step::Tab(View::Timeline), "tab timeline"),
            (Step::Select("n3".into()), "select n3"),
            (Step::Cache("it".into()), "cache it"),
            (Step::Help(true), "help on"),
            (Step::Help(false), "help off"),
            (
                Step::AwaitMembers {
                    count: 4,
                    within: Some(S(20)),
                },
                "await members 4 within 20s",
            ),
            (
                Step::AwaitStatus {
                    status: AwaitedStatus::Down,
                    label: "n3".into(),
                    within: None,
                },
                "await down n3",
            ),
            (
                Step::AwaitSettled {
                    cache: "it".into(),
                    within: Some(S(25)),
                },
                "await settled it within 25s",
            ),
            (Step::Quit, "quit"),
        ];
        for (step, text) in cases {
            assert_eq!(step.to_string(), text);
            assert_eq!(parse(text).unwrap().steps[0].step, step, "{text}");
        }
    }

    #[test]
    fn an_awaited_status_maps_to_a_member_status_and_its_word() {
        for (status, member, word) in [
            (
                AwaitedStatus::Departing,
                MemberStatus::Departing,
                "departing",
            ),
            (AwaitedStatus::Left, MemberStatus::Left, "left"),
            (AwaitedStatus::Down, MemberStatus::Down, "down"),
        ] {
            assert_eq!(status.member_status(), member);
            assert_eq!(status.word(), word);
        }
    }

    fn tour() -> Vec<Step> {
        parse(TOUR)
            .unwrap()
            .steps
            .into_iter()
            .map(|e| e.step)
            .collect()
    }

    #[test]
    fn the_built_in_tour_tells_the_story_in_order() {
        let steps = tour();
        let actions: Vec<String> = steps
            .iter()
            .filter(|step| {
                matches!(
                    step,
                    Step::Spawn { .. }
                        | Step::Kill(_)
                        | Step::Leave(_)
                        | Step::Crash(_)
                        | Step::Restart(_)
                )
            })
            .map(ToString::to_string)
            .collect();
        assert_eq!(
            actions,
            [
                "spawn 3 stagger 1s",
                "spawn 1",
                "spawn 1",
                "kill n3",
                "leave n2",
                "restart n3"
            ]
        );
        assert_eq!(steps.last(), Some(&Step::Quit));
        // The four views and the help overlay all appear.
        for view in View::ALL {
            assert!(steps.contains(&Step::Tab(view)), "{view:?}");
        }
        assert!(steps.contains(&Step::Help(true)) && steps.contains(&Step::Help(false)));
        // A crash and a graceful leave each get their await in order.
        let at = |wanted: &str| {
            steps
                .iter()
                .position(|step| step.to_string() == wanted)
                .unwrap_or_else(|| panic!("the tour has no `{wanted}`"))
        };
        assert!(at("kill n3") < at("await down n3 within 20s"));
        assert!(at("await down n3 within 20s") < at("leave n2"));
        assert!(at("leave n2") < at("await departing n2 within 5s"));
        assert!(at("await departing n2 within 5s") < at("await left n2 within 20s"));
        assert!(at("await left n2 within 20s") < at("restart n3"));
    }

    #[test]
    fn the_built_in_tour_never_sleeps_for_a_change_of_state() {
        let steps = tour();
        for (index, step) in steps.iter().enumerate() {
            let changes_state = matches!(
                step,
                Step::Spawn { .. }
                    | Step::Kill(_)
                    | Step::Leave(_)
                    | Step::Crash(_)
                    | Step::Restart(_)
            );
            if !changes_state {
                continue;
            }
            // Past the captions and selections, an await comes before any pause.
            let next = steps[index + 1..]
                .iter()
                .find(|step| !matches!(step, Step::Caption(_) | Step::Select(_) | Step::Tab(_)))
                .expect("a step follows");
            assert!(
                matches!(
                    next,
                    Step::AwaitMembers { .. }
                        | Step::AwaitStatus { .. }
                        | Step::AwaitSettled { .. }
                ),
                "`{step}` is followed by `{next}`, not an await"
            );
        }
        // Every await has its own deadline.
        for step in &steps {
            if let Step::AwaitMembers { within, .. }
            | Step::AwaitStatus { within, .. }
            | Step::AwaitSettled { within, .. } = step
            {
                assert!(within.is_some(), "`{step}` has no deadline");
            }
        }
    }

    #[test]
    fn the_built_in_tour_has_time_to_read_and_stays_inside_the_budget() {
        let steps = tour();
        let pauses: Duration = steps
            .iter()
            .filter_map(|step| match step {
                Step::Pause(duration) => Some(*duration),
                _ => None,
            })
            .sum();
        // The pauses are the eye's time; the awaits add the cluster's. The
        // whole run targets 75 to 90 seconds and must finish within 95.
        assert!(pauses >= S(30) && pauses <= S(45), "{pauses:?}");
        let deadlines: Duration = steps
            .iter()
            .filter_map(|step| match step {
                Step::AwaitMembers { within, .. }
                | Step::AwaitStatus { within, .. }
                | Step::AwaitSettled { within, .. } => *within,
                _ => None,
            })
            .sum();
        assert!(deadlines > S(60), "the awaits are generous: {deadlines:?}");
    }
}

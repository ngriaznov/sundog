//! Lifelines: each node's life as phases and marks over time, for the
//! Timeline view, and the pure rendering of one lifeline into cells.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use smol_str::SmolStr;

use super::count_to_f64;
use super::events::EventKind;

/// How many phases and how many marks a lifeline keeps; the oldest go first.
pub const MAX_ENTRIES: usize = 256;

/// What a node is doing during a phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhaseKind {
    /// Live and serving.
    Live,
    /// Gossiping a graceful departure.
    Departing,
    /// Live in gossip while its exporter fails: a crash may be near.
    Suspect,
}

/// A stretch of time in one [`PhaseKind`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Phase {
    /// What the node is doing.
    pub kind: PhaseKind,
    /// When the phase began.
    pub from: Instant,
    /// When it ended; `None` while it lasts.
    pub until: Option<Instant>,
}

/// A point event on a lifeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkKind {
    /// A new node (`▲`).
    Join,
    /// A new identity or incarnation at the address (`↻`).
    Rejoin,
    /// The node announced a departure (`◐`).
    Leave,
    /// A departing node is gone (`○`).
    Left,
    /// The exporter failed while gossip still lists the node (`⚠`).
    Suspect,
    /// The node dropped with no departure (`✖`).
    Down,
    /// A cache's ownership view changed (`⇄`).
    View,
    /// A cache settled (`✔`).
    Settled,
}

impl MarkKind {
    /// The glyph the mark draws.
    #[must_use]
    pub const fn glyph(self) -> char {
        match self {
            Self::Join => '▲',
            Self::Rejoin => '↻',
            Self::Leave => '◐',
            Self::Left => '○',
            Self::Suspect => '⚠',
            Self::Down => '✖',
            Self::View => '⇄',
            Self::Settled => '✔',
        }
    }

    /// The tone the mark draws in.
    #[must_use]
    pub const fn tone(self) -> Tone {
        match self {
            Self::Join => Tone::Node,
            Self::Rejoin => Tone::Info,
            Self::Leave | Self::Suspect => Tone::Warn,
            Self::Left => Tone::Muted,
            Self::Down => Tone::Bad,
            Self::View => Tone::Move,
            Self::Settled => Tone::Ok,
        }
    }

    /// Which mark wins a cell that holds several: the higher.
    const fn priority(self) -> u8 {
        match self {
            Self::Settled => 1,
            Self::View | Self::Join => 2,
            Self::Rejoin => 3,
            Self::Leave => 4,
            Self::Suspect => 5,
            Self::Left => 6,
            Self::Down => 7,
        }
    }
}

/// A mark and when it happened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Mark {
    /// What happened.
    pub kind: MarkKind,
    /// When.
    pub at: Instant,
}

/// One row of the Timeline: phases and marks, oldest first.
#[derive(Debug, Clone, Default)]
pub struct Lifeline {
    phases: Vec<Phase>,
    marks: Vec<Mark>,
}

impl Lifeline {
    /// An empty lifeline.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Ends the open phase at `at` and opens one of `kind`.
    pub fn begin(&mut self, kind: PhaseKind, at: Instant) {
        self.end(at);
        self.phases.push(Phase {
            kind,
            from: at,
            until: None,
        });
        if self.phases.len() > MAX_ENTRIES {
            self.phases.remove(0);
        }
    }

    /// Ends the open phase at `at`, if any.
    pub fn end(&mut self, at: Instant) {
        if let Some(open) = self.phases.last_mut().filter(|p| p.until.is_none()) {
            open.until = Some(at);
        }
    }

    /// Adds a mark.
    pub fn mark(&mut self, kind: MarkKind, at: Instant) {
        self.marks.push(Mark { kind, at });
        if self.marks.len() > MAX_ENTRIES {
            self.marks.remove(0);
        }
    }

    /// The kind of the open phase.
    #[must_use]
    pub fn current(&self) -> Option<PhaseKind> {
        self.phases
            .last()
            .filter(|phase| phase.until.is_none())
            .map(|phase| phase.kind)
    }

    /// The phases, oldest first.
    #[must_use]
    pub fn phases(&self) -> &[Phase] {
        &self.phases
    }

    /// The marks, oldest first.
    #[must_use]
    pub fn marks(&self) -> &[Mark] {
        &self.marks
    }

    /// The earliest instant the lifeline holds: its first phase or mark.
    #[must_use]
    pub fn first_at(&self) -> Option<Instant> {
        let phase = self.phases.first().map(|phase| phase.from);
        let mark = self.marks.first().map(|mark| mark.at);
        phase.into_iter().chain(mark).min()
    }

    /// Whether the lifeline has no phase and no mark.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.phases.is_empty() && self.marks.is_empty()
    }
}

/// The lifelines of every node, by gossip address, and of every
/// `Distributed` cache, by name.
#[derive(Debug, Clone, Default)]
pub struct Lifelines {
    nodes: BTreeMap<SocketAddr, Lifeline>,
    caches: BTreeMap<SmolStr, Lifeline>,
}

impl Lifelines {
    /// No lifelines.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Draws `kind`, which happened at `at`, onto the lifelines it concerns:
    /// `JOIN` marks `▲` and goes live; `REJOIN`, `RESTART` and `UP` mark `↻`
    /// and go live; `LEAVE` marks `◐` and goes departing; `LEFT` and `DOWN`
    /// mark and end the line, and a `DOWN` of a live node with no failed
    /// scrape before it first marks `⚠`; `VIEW` and `SETTLED` mark the cache's line. A
    /// `LEAVE`, `LEFT` or `DOWN` of a superseded incarnation marks the line
    /// and leaves its phase alone, because the node now at the address lives.
    /// The other kinds draw nothing.
    pub fn record(&mut self, kind: &EventKind, at: Instant) {
        match kind {
            EventKind::Join { addr, .. } => self.begin(*addr, MarkKind::Join, PhaseKind::Live, at),
            EventKind::Rejoin { addr, .. }
            | EventKind::Restart { addr, .. }
            | EventKind::Up { addr, .. } => {
                self.begin(*addr, MarkKind::Rejoin, PhaseKind::Live, at);
            }
            EventKind::Leave {
                addr, superseded, ..
            } => {
                if *superseded {
                    self.mark_only(*addr, MarkKind::Leave, at);
                } else {
                    self.begin(*addr, MarkKind::Leave, PhaseKind::Departing, at);
                }
            }
            EventKind::Left {
                addr, superseded, ..
            } => self.finish(*addr, MarkKind::Left, *superseded, at),
            EventKind::Down {
                addr, superseded, ..
            } => self.finish(*addr, MarkKind::Down, *superseded, at),
            EventKind::View { cache, .. } => {
                self.caches
                    .entry(cache.clone())
                    .or_default()
                    .mark(MarkKind::View, at);
            }
            EventKind::Settled { cache, .. } => {
                self.caches
                    .entry(cache.clone())
                    .or_default()
                    .mark(MarkKind::Settled, at);
            }
            _ => {}
        }
    }

    /// A scrape of the node at `addr` failed at `at`: a live node turns
    /// suspect and gets a `⚠` mark. A node already suspect, departing or
    /// gone is unchanged.
    pub fn suspect(&mut self, addr: SocketAddr, at: Instant) {
        if let Some(line) = self.nodes.get_mut(&addr)
            && line.current() == Some(PhaseKind::Live)
        {
            line.mark(MarkKind::Suspect, at);
            line.begin(PhaseKind::Suspect, at);
        }
    }

    /// A scrape of the node at `addr` succeeded at `at`: a suspect node is
    /// live again.
    pub fn recovered(&mut self, addr: SocketAddr, at: Instant) {
        if let Some(line) = self.nodes.get_mut(&addr)
            && line.current() == Some(PhaseKind::Suspect)
        {
            line.begin(PhaseKind::Live, at);
        }
    }

    /// The lifeline of the node at gossip address `addr`.
    #[must_use]
    pub fn node(&self, addr: SocketAddr) -> Option<&Lifeline> {
        self.nodes.get(&addr)
    }

    /// The lifeline of `cache`, which carries only marks.
    #[must_use]
    pub fn cache(&self, cache: &str) -> Option<&Lifeline> {
        self.caches.get(cache)
    }

    /// Drops the lifeline of `cache`.
    pub fn forget_cache(&mut self, cache: &str) {
        self.caches.remove(cache);
    }

    fn begin(&mut self, addr: SocketAddr, mark: MarkKind, phase: PhaseKind, at: Instant) {
        let line = self.nodes.entry(addr).or_default();
        line.mark(mark, at);
        line.begin(phase, at);
    }

    fn finish(&mut self, addr: SocketAddr, mark: MarkKind, superseded: bool, at: Instant) {
        if superseded {
            self.mark_only(addr, mark, at);
            return;
        }
        let line = self.nodes.entry(addr).or_default();
        if mark == MarkKind::Down && line.current() == Some(PhaseKind::Live) {
            // The failure detector can mark a node down before a scrape of it
            // fails; the crash still opens with a suspect run.
            line.mark(MarkKind::Suspect, at);
            line.begin(PhaseKind::Suspect, at);
        }
        line.mark(mark, at);
        line.end(at);
    }

    fn mark_only(&mut self, addr: SocketAddr, mark: MarkKind, at: Instant) {
        self.nodes.entry(addr).or_default().mark(mark, at);
    }
}

/// How a cell is to be colored; the view maps each tone to a theme color.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tone {
    /// The node's own color.
    Node,
    /// Warning.
    Warn,
    /// Failure.
    Bad,
    /// De-emphasized.
    Muted,
    /// Rebalancing.
    Move,
    /// Success.
    Ok,
    /// Information.
    Info,
    /// Nothing drawn.
    Blank,
}

/// One character of a rendered lifeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cell {
    /// The character.
    pub glyph: char,
    /// How to color it.
    pub tone: Tone,
}

const BLANK: Cell = Cell {
    glyph: ' ',
    tone: Tone::Blank,
};

/// Renders `line` into `width` cells that cover the `window` ending at `now`,
/// oldest at the left. A cell holding a mark shows the mark that wins by
/// priority (`✖` over `○` over `⚠` over `◐` over `↻` over `▲` and `⇄` over
/// `✔`); any other cell shows the phase that covers its middle: `━` for live,
/// `┄` for departing or suspect, blank for none.
#[must_use]
pub fn lifeline_cells(line: &Lifeline, window: Duration, width: usize, now: Instant) -> Vec<Cell> {
    let mut cells = vec![BLANK; width];
    if width == 0 || window.is_zero() {
        return cells;
    }
    for (index, cell) in cells.iter_mut().enumerate() {
        let mid_age = window.mul_f64(1.0 - (count_to_f64(index) + 0.5) / count_to_f64(width));
        let Some(mid) = now.checked_sub(mid_age) else {
            continue;
        };
        let phase = line
            .phases
            .iter()
            .rev()
            .find(|p| p.from <= mid && p.until.is_none_or(|until| mid < until));
        *cell = match phase.map(|p| p.kind) {
            Some(PhaseKind::Live) => Cell {
                glyph: '━',
                tone: Tone::Node,
            },
            Some(PhaseKind::Departing | PhaseKind::Suspect) => Cell {
                glyph: '┄',
                tone: Tone::Warn,
            },
            None => BLANK,
        };
    }
    place_marks(line, window, width, now, &mut cells);
    cells
}

/// The columns of the marks of `line` that fall inside the `window`, oldest
/// mark first, as `(mark index, column)`.
fn mark_columns(
    line: &Lifeline,
    window: Duration,
    width: usize,
    now: Instant,
) -> Vec<(usize, usize)> {
    line.marks
        .iter()
        .enumerate()
        .filter_map(|(index, mark)| {
            let age = now.checked_duration_since(mark.at)?;
            if age > window {
                return None;
            }
            let from_left = 1.0 - age.as_secs_f64() / window.as_secs_f64();
            Some((index, column_of(from_left, width)))
        })
        .collect()
}

/// The index of the mark that opened the run `closing` ends: the mark of
/// `opening` kind that began the phase of `phase` kind which `closing` ended.
fn opening_of(
    line: &Lifeline,
    closing: &Mark,
    opening: MarkKind,
    phase: PhaseKind,
) -> Option<usize> {
    let run = line
        .phases
        .iter()
        .find(|p| p.kind == phase && p.until == Some(closing.at))?;
    line.marks
        .iter()
        .position(|mark| mark.kind == opening && mark.at == run.from)
}

/// The fewest cells a run from an opening mark to its closing mark spans:
/// the opening mark, one cell of dashes and the closing mark.
const MIN_RUN: usize = 3;

/// Draws the marks of `line` into `cells`. Each cell shows the mark that wins
/// by priority. A crash (`⚠` then `✖`) and a leave (`◐` then `○`) always span
/// at least [`MIN_RUN`] cells: the opening mark moves left of the closing
/// mark's column when they share one, and dashes in the warn tone join them.
fn place_marks(line: &Lifeline, window: Duration, width: usize, now: Instant, cells: &mut [Cell]) {
    let mut columns = mark_columns(line, window, width, now);
    let mut dashes: Vec<(usize, usize)> = Vec::new();
    for (closing_index, closing_column) in columns.clone() {
        let closing = &line.marks[closing_index];
        let (opening, phase) = match closing.kind {
            MarkKind::Down => (MarkKind::Suspect, PhaseKind::Suspect),
            MarkKind::Left => (MarkKind::Leave, PhaseKind::Departing),
            _ => continue,
        };
        let Some(opening_index) = opening_of(line, closing, opening, phase) else {
            continue;
        };
        let Some(entry) = columns
            .iter_mut()
            .find(|(index, _)| *index == opening_index)
        else {
            continue;
        };
        entry.1 = entry.1.min(closing_column.saturating_sub(MIN_RUN - 1));
        dashes.push((entry.1 + 1, closing_column));
    }
    let mut best: Vec<Option<MarkKind>> = vec![None; width];
    for (index, column) in columns {
        let kind = line.marks[index].kind;
        if best[column].is_none_or(|held| held.priority() < kind.priority()) {
            best[column] = Some(kind);
        }
    }
    for (from, to) in dashes {
        for column in from..to {
            if best[column].is_none() {
                cells[column] = Cell {
                    glyph: '┄',
                    tone: Tone::Warn,
                };
            }
        }
    }
    for (cell, mark) in cells.iter_mut().zip(best) {
        if let Some(mark) = mark {
            *cell = Cell {
                glyph: mark.glyph(),
                tone: mark.tone(),
            };
        }
    }
}

#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "the fraction is clamped to 0..=1 and the width is a screen width"
)]
fn column_of(fraction_from_left: f64, width: usize) -> usize {
    let column = (fraction_from_left.clamp(0.0, 1.0) * count_to_f64(width)).floor() as usize;
    column.min(width - 1)
}

#[cfg(test)]
mod tests {
    use sundog::NodeId;

    use crate::model::testkit;

    use super::*;

    const WINDOW: Duration = Duration::from_secs(40);

    fn addr() -> SocketAddr {
        testkit::gossip_addr(1)
    }

    fn join(addr: SocketAddr) -> EventKind {
        EventKind::Join {
            node: testkit::node_id(1, 0),
            addr,
            protocol: 6,
            caches: BTreeMap::new(),
        }
    }

    fn leave(node: NodeId, superseded: bool) -> EventKind {
        EventKind::Leave {
            node,
            addr: addr(),
            superseded,
        }
    }

    fn left(node: NodeId, superseded: bool) -> EventKind {
        EventKind::Left {
            node,
            addr: addr(),
            superseded,
        }
    }

    fn down(node: NodeId, superseded: bool) -> EventKind {
        EventKind::Down {
            node,
            addr: addr(),
            exporter_silent: None,
            superseded,
        }
    }

    /// The instant `age` before `now`.
    fn ago(now: Instant, age_ms: u64) -> Instant {
        now.checked_sub(Duration::from_millis(age_ms))
            .expect("the test clock starts after the age")
    }

    fn text(cells: &[Cell]) -> String {
        cells.iter().map(|c| c.glyph).collect()
    }

    fn render(lifelines: &Lifelines, now: Instant) -> String {
        text(&lifeline_cells(
            lifelines.node(addr()).unwrap(),
            WINDOW,
            40,
            now,
        ))
    }

    #[test]
    fn a_join_draws_a_triangle_and_a_live_line() {
        let now = Instant::now() + Duration::from_secs(100);
        let mut lifelines = Lifelines::new();
        lifelines.record(&join(addr()), ago(now, 9_500));
        assert_eq!(
            render(&lifelines, now),
            format!("{}▲{}", " ".repeat(30), "━".repeat(9))
        );
    }

    #[test]
    fn a_graceful_leave_draws_a_half_moon_dashes_and_a_ring() {
        let now = Instant::now() + Duration::from_secs(100);
        let mut lifelines = Lifelines::new();
        let node = testkit::node_id(1, 0);
        lifelines.record(&join(addr()), ago(now, 39_500));
        lifelines.record(&leave(node, false), ago(now, 20_500));
        lifelines.record(&left(node, false), ago(now, 14_500));
        let expected = format!("▲{}◐{}○{}", "━".repeat(18), "┄".repeat(5), " ".repeat(14));
        assert_eq!(render(&lifelines, now), expected);
    }

    #[test]
    fn a_crash_draws_a_warning_dashes_and_a_cross_then_a_rejoin_restarts_the_line() {
        let now = Instant::now() + Duration::from_secs(100);
        let mut lifelines = Lifelines::new();
        let node = testkit::node_id(1, 0);
        lifelines.record(&join(addr()), ago(now, 39_500));
        lifelines.suspect(addr(), ago(now, 20_500));
        lifelines.record(&down(node, false), ago(now, 14_500));
        let crashed = format!("▲{}⚠{}✖{}", "━".repeat(18), "┄".repeat(5), " ".repeat(14));
        assert_eq!(render(&lifelines, now), crashed);

        lifelines.record(
            &EventKind::Rejoin {
                node: testkit::node_id(1, 1),
                addr: addr(),
                previous: node,
                caches: std::collections::BTreeMap::new(),
            },
            ago(now, 5_500),
        );
        let rejoined = format!(
            "▲{}⚠{}✖{}↻{}",
            "━".repeat(18),
            "┄".repeat(5),
            " ".repeat(8),
            "━".repeat(5)
        );
        assert_eq!(render(&lifelines, now), rejoined);
    }

    #[test]
    fn a_restart_marks_like_a_rejoin() {
        let now = Instant::now() + Duration::from_secs(100);
        let mut lifelines = Lifelines::new();
        lifelines.record(
            &EventKind::Restart {
                node: testkit::node_id(1, 0),
                addr: addr(),
            },
            ago(now, 0),
        );
        assert!(
            text(&lifeline_cells(
                lifelines.node(addr()).unwrap(),
                WINDOW,
                40,
                now
            ))
            .ends_with('↻')
        );
    }

    #[test]
    fn suspicion_applies_to_live_nodes_only_and_lifts_on_recovery() {
        let now = Instant::now() + Duration::from_secs(100);
        let mut lifelines = Lifelines::new();
        lifelines.suspect(addr(), ago(now, 30_000));
        assert!(
            lifelines.node(addr()).is_none(),
            "an unknown node is not drawn"
        );

        lifelines.record(&join(addr()), ago(now, 39_500));
        lifelines.suspect(addr(), ago(now, 20_500));
        lifelines.suspect(addr(), ago(now, 19_500));
        let line = lifelines.node(addr()).unwrap();
        assert_eq!(line.current(), Some(PhaseKind::Suspect));
        assert_eq!(
            line.marks()
                .iter()
                .filter(|m| m.kind == MarkKind::Suspect)
                .count(),
            1,
            "a second failure marks nothing"
        );

        lifelines.recovered(addr(), ago(now, 10_500));
        assert_eq!(
            lifelines.node(addr()).unwrap().current(),
            Some(PhaseKind::Live)
        );
        lifelines.recovered(addr(), ago(now, 9_500));
        assert_eq!(lifelines.node(addr()).unwrap().phases().len(), 3);
    }

    #[test]
    fn a_departing_node_is_not_suspected() {
        let now = Instant::now() + Duration::from_secs(100);
        let mut lifelines = Lifelines::new();
        lifelines.record(&join(addr()), ago(now, 30_000));
        lifelines.record(&leave(testkit::node_id(1, 0), false), ago(now, 20_000));
        lifelines.suspect(addr(), ago(now, 10_000));
        assert_eq!(
            lifelines.node(addr()).unwrap().current(),
            Some(PhaseKind::Departing)
        );
    }

    #[test]
    fn a_cache_line_carries_view_and_settled_marks() {
        let now = Instant::now() + Duration::from_secs(100);
        let mut lifelines = Lifelines::new();
        lifelines.record(
            &EventKind::View {
                cache: "it".into(),
                from: None,
                to: 1,
                moved: 0,
                deltas: Vec::new(),
            },
            ago(now, 30_500),
        );
        lifelines.record(
            &EventKind::Settled {
                cache: "it".into(),
                took: Duration::from_secs(3),
            },
            ago(now, 20_500),
        );
        let cells = lifeline_cells(lifelines.cache("it").unwrap(), WINDOW, 40, now);
        assert_eq!(cells[9].glyph, '⇄');
        assert_eq!(cells[9].tone, Tone::Move);
        assert_eq!(cells[19].glyph, '✔');
        assert_eq!(cells[19].tone, Tone::Ok);
        assert_eq!(cells.iter().filter(|c| c.glyph != ' ').count(), 2);
        assert!(lifelines.cache("other").is_none());
        lifelines.forget_cache("it");
        assert!(lifelines.cache("it").is_none());
    }

    #[test]
    fn other_events_draw_nothing() {
        let mut lifelines = Lifelines::new();
        lifelines.record(
            &EventKind::Xfer {
                node: testkit::node_id(1, 0),
            },
            Instant::now(),
        );
        assert!(lifelines.node(addr()).is_none());
    }

    #[test]
    fn the_stronger_mark_wins_a_shared_cell() {
        let now = Instant::now() + Duration::from_secs(100);
        let mut line = Lifeline::new();
        line.mark(MarkKind::Suspect, ago(now, 5_400));
        line.mark(MarkKind::Down, ago(now, 5_200));
        line.mark(MarkKind::Leave, ago(now, 5_300));
        let cells = lifeline_cells(&line, WINDOW, 40, now);
        assert_eq!(cells[34].glyph, '✖');
        assert_eq!(cells.iter().filter(|c| c.glyph != ' ').count(), 1);
    }

    #[test]
    fn marks_outside_the_window_are_not_drawn() {
        let now = Instant::now() + Duration::from_secs(100);
        let mut line = Lifeline::new();
        line.mark(MarkKind::Down, ago(now, 41_000));
        line.mark(MarkKind::Down, now + Duration::from_secs(1));
        assert!(
            lifeline_cells(&line, WINDOW, 40, now)
                .iter()
                .all(|c| c.glyph == ' ')
        );
    }

    #[test]
    fn a_mark_at_now_lands_in_the_last_cell() {
        let now = Instant::now() + Duration::from_secs(100);
        let mut line = Lifeline::new();
        line.mark(MarkKind::Join, now);
        line.mark(MarkKind::Left, ago(now, 40_000));
        let cells = lifeline_cells(&line, WINDOW, 40, now);
        assert_eq!(cells[39].glyph, '▲');
        assert_eq!(cells[0].glyph, '○');
    }

    #[test]
    fn degenerate_sizes_render_blank_cells() {
        let now = Instant::now();
        let mut line = Lifeline::new();
        line.begin(PhaseKind::Live, now);
        let cells = lifeline_cells(&line, WINDOW, 0, now);
        assert!(cells.is_empty(), "{cells:?}");
        assert!(
            lifeline_cells(&line, Duration::ZERO, 5, now)
                .iter()
                .all(|c| c.glyph == ' ')
        );
        assert!(Lifeline::new().is_empty());
        assert!(!line.is_empty());
    }

    #[test]
    fn a_lifeline_keeps_only_the_newest_entries() {
        let base = Instant::now();
        let mut line = Lifeline::new();
        for index in 0..(u32::try_from(MAX_ENTRIES).unwrap() + 10) {
            let at = base + Duration::from_secs(u64::from(index));
            line.mark(MarkKind::Join, at);
            line.begin(PhaseKind::Live, at);
        }
        assert_eq!(line.marks().len(), MAX_ENTRIES);
        assert_eq!(line.phases().len(), MAX_ENTRIES);
        assert_eq!(line.marks()[0].at, base + Duration::from_secs(10));
        assert_eq!(line.phases()[0].from, base + Duration::from_secs(10));
    }

    #[test]
    fn begin_closes_the_open_phase() {
        let base = Instant::now();
        let mut line = Lifeline::new();
        line.begin(PhaseKind::Live, base);
        line.begin(PhaseKind::Departing, base + Duration::from_secs(1));
        assert_eq!(line.phases()[0].until, Some(base + Duration::from_secs(1)));
        assert_eq!(line.current(), Some(PhaseKind::Departing));
        line.end(base + Duration::from_secs(2));
        assert_eq!(line.current(), None);
        // Ending twice keeps the first end.
        line.end(base + Duration::from_secs(3));
        assert_eq!(line.phases()[1].until, Some(base + Duration::from_secs(2)));
    }

    #[test]
    fn every_mark_has_a_glyph_a_tone_and_a_distinct_priority() {
        let kinds = [
            MarkKind::Join,
            MarkKind::Rejoin,
            MarkKind::Leave,
            MarkKind::Left,
            MarkKind::Suspect,
            MarkKind::Down,
            MarkKind::View,
            MarkKind::Settled,
        ];
        let glyphs: std::collections::HashSet<_> = kinds.iter().map(|k| k.glyph()).collect();
        assert_eq!(glyphs.len(), kinds.len());
        assert_eq!(MarkKind::Down.tone(), Tone::Bad);
        assert_eq!(MarkKind::Left.tone(), Tone::Muted);
        assert_eq!(MarkKind::Rejoin.tone(), Tone::Info);
        assert_eq!(MarkKind::Join.tone(), Tone::Node);
        assert_eq!(MarkKind::Leave.tone(), Tone::Warn);
    }

    #[test]
    fn a_member_that_comes_up_again_reopens_its_line() {
        let now = Instant::now() + Duration::from_secs(100);
        let mut lifelines = Lifelines::new();
        let node = testkit::node_id(1, 0);
        lifelines.record(&join(addr()), ago(now, 39_500));
        lifelines.record(&down(node, false), ago(now, 20_500));
        assert_eq!(lifelines.node(addr()).unwrap().current(), None);
        lifelines.record(&EventKind::Up { node, addr: addr() }, ago(now, 10_500));
        let line = lifelines.node(addr()).unwrap();
        assert_eq!(line.current(), Some(PhaseKind::Live));
        let kinds: Vec<_> = line.marks().iter().map(|m| m.kind).collect();
        assert_eq!(
            kinds,
            [
                MarkKind::Join,
                MarkKind::Suspect,
                MarkKind::Down,
                MarkKind::Rejoin
            ]
        );
    }

    #[test]
    fn a_superseded_departure_or_crash_marks_the_line_and_leaves_its_phase() {
        let now = Instant::now() + Duration::from_secs(100);
        let node = testkit::node_id(1, 0);
        let restart = EventKind::Restart { node, addr: addr() };
        for (kind, mark) in [
            (down(node, true), MarkKind::Down),
            (left(node, true), MarkKind::Left),
            (leave(node, true), MarkKind::Leave),
        ] {
            let mut lifelines = Lifelines::new();
            lifelines.record(&join(addr()), ago(now, 30_500));
            lifelines.record(&restart, ago(now, 20_500));
            lifelines.record(&kind, ago(now, 10_500));
            let line = lifelines.node(addr()).unwrap();
            assert_eq!(line.current(), Some(PhaseKind::Live), "{mark:?}");
            assert_eq!(line.marks().last().map(|m| m.kind), Some(mark));
        }
    }

    #[test]
    fn a_superseded_event_for_an_unseen_address_still_marks_it() {
        let node = testkit::node_id(1, 0);
        let mut lifelines = Lifelines::new();
        lifelines.record(&down(node, true), Instant::now());
        let line = lifelines.node(addr()).unwrap();
        assert_eq!(line.current(), None);
        assert_eq!(line.marks().len(), 1);
    }

    #[test]
    fn column_of_clamps_a_fraction_to_a_column() {
        assert_eq!(column_of(0.0, 40), 0);
        assert_eq!(column_of(0.5, 40), 20);
        assert_eq!(column_of(0.999, 40), 39);
        assert_eq!(column_of(1.0, 40), 39, "the right edge is the last column");
        assert_eq!(column_of(-3.0, 40), 0);
        assert_eq!(column_of(7.5, 40), 39);
        assert_eq!(column_of(0.9, 1), 0);
    }

    /// A node that joined `join_ms` ago and goes through `open` at
    /// `open_ms` ago and `close` at `close_ms` ago.
    fn run_line(
        now: Instant,
        (open, phase): (MarkKind, PhaseKind),
        close: MarkKind,
        (open_ms, close_ms): (u64, u64),
    ) -> Lifeline {
        let mut line = Lifeline::new();
        line.mark(MarkKind::Join, ago(now, 39_000));
        line.begin(PhaseKind::Live, ago(now, 39_000));
        line.mark(open, ago(now, open_ms));
        line.begin(phase, ago(now, open_ms));
        line.mark(close, ago(now, close_ms));
        line.end(ago(now, close_ms));
        line
    }

    #[test]
    fn a_crash_and_a_leave_span_three_cells_when_their_marks_share_a_column() {
        let now = Instant::now() + Duration::from_secs(100);
        // One second a cell: 8.9 s and 8.5 s ago both land in column 31.
        let crash = run_line(
            now,
            (MarkKind::Suspect, PhaseKind::Suspect),
            MarkKind::Down,
            (8_900, 8_500),
        );
        let cells = lifeline_cells(&crash, WINDOW, 40, now);
        assert_eq!(cells[31].glyph, '✖');
        let shown = text(&cells);
        assert!(shown.contains("⚠┄✖"), "{shown}");
        assert_eq!(cells[30].tone, Tone::Warn);
        assert_eq!(cells[29].glyph, '⚠');
        let leave = run_line(
            now,
            (MarkKind::Leave, PhaseKind::Departing),
            MarkKind::Left,
            (8_900, 8_500),
        );
        let shown = text(&lifeline_cells(&leave, WINDOW, 40, now));
        assert!(shown.contains("◐┄○"), "{shown}");
    }

    #[test]
    fn a_down_straight_from_live_opens_with_a_suspect_mark() {
        let now = Instant::now() + Duration::from_secs(100);
        let mut lines = Lifelines::default();
        lines.record(&join(addr()), ago(now, 39_000));
        let down = down(testkit::node_id(1, 0), false);
        lines.record(&down, ago(now, 8_500));
        let line = lines.node(addr()).unwrap();
        let kinds: Vec<MarkKind> = line.marks().iter().map(|m| m.kind).collect();
        assert_eq!(kinds, [MarkKind::Join, MarkKind::Suspect, MarkKind::Down]);
        assert_eq!(line.current(), None);
        let shown = text(&lifeline_cells(line, WINDOW, 40, now));
        assert!(shown.contains("⚠┄✖"), "{shown}");
        // After a failed scrape the run is not opened twice.
        let mut scraped = Lifelines::default();
        scraped.record(&join(addr()), ago(now, 39_000));
        scraped.suspect(addr(), ago(now, 9_000));
        scraped.record(&down, ago(now, 8_500));
        let kinds: Vec<MarkKind> = scraped
            .node(addr())
            .unwrap()
            .marks()
            .iter()
            .map(|m| m.kind)
            .collect();
        assert_eq!(kinds, [MarkKind::Join, MarkKind::Suspect, MarkKind::Down]);
    }

    #[test]
    fn a_run_that_already_spans_three_cells_is_left_as_it_is() {
        let now = Instant::now() + Duration::from_secs(100);
        let crash = run_line(
            now,
            (MarkKind::Suspect, PhaseKind::Suspect),
            MarkKind::Down,
            (12_500, 5_500),
        );
        let cells = lifeline_cells(&crash, WINDOW, 40, now);
        assert_eq!(cells[27].glyph, '⚠');
        assert_eq!(cells[34].glyph, '✖');
        assert!(cells[28..34].iter().all(|c| c.glyph == '┄'));
    }

    #[test]
    fn a_crash_with_no_suspect_mark_and_a_mark_between_keep_their_places() {
        let now = Instant::now() + Duration::from_secs(100);
        let mut bare = Lifeline::new();
        bare.mark(MarkKind::Join, ago(now, 39_000));
        bare.begin(PhaseKind::Live, ago(now, 39_000));
        bare.mark(MarkKind::Down, ago(now, 8_500));
        bare.end(ago(now, 8_500));
        let shown = text(&lifeline_cells(&bare, WINDOW, 40, now));
        assert_eq!(shown.chars().nth(31), Some('✖'));
        assert_eq!(shown.chars().nth(30), Some('━'), "{shown}");
        // A view mark in the cell the dashes cross stays.
        let mut line = run_line(
            now,
            (MarkKind::Suspect, PhaseKind::Suspect),
            MarkKind::Down,
            (8_900, 8_500),
        );
        line.mark(MarkKind::View, ago(now, 9_500));
        let shown = text(&lifeline_cells(&line, WINDOW, 40, now));
        assert_eq!(shown.chars().nth(30), Some('⇄'), "{shown}");
        assert_eq!(shown.chars().nth(31), Some('✖'), "{shown}");
    }

    #[test]
    fn the_earliest_instant_is_the_first_phase_or_the_first_mark() {
        let now = Instant::now() + Duration::from_secs(100);
        assert_eq!(Lifeline::new().first_at(), None);
        let mut phase_first = Lifeline::new();
        phase_first.begin(PhaseKind::Live, ago(now, 9_000));
        phase_first.mark(MarkKind::Join, ago(now, 4_000));
        assert_eq!(phase_first.first_at(), Some(ago(now, 9_000)));
        let mut mark_first = Lifeline::new();
        mark_first.mark(MarkKind::Join, ago(now, 9_000));
        mark_first.begin(PhaseKind::Live, ago(now, 4_000));
        assert_eq!(mark_first.first_at(), Some(ago(now, 9_000)));
        let mut mark_only = Lifeline::new();
        mark_only.mark(MarkKind::View, ago(now, 2_000));
        assert_eq!(mark_only.first_at(), Some(ago(now, 2_000)));
    }
}

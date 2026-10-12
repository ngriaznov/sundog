//! The explain overlay: where one key lives, and what each test node says of
//! reading it.
//!
//! `e` opens a popup over the body of any view. The top is **computed** from
//! the model alone, with the vocabulary of `Cache::explain`: the part as
//! `bucket/part`, the ownership view and the part's owners in the order a
//! fetch asks them. Below it, once `Enter` has asked the test nodes over
//! their control ports, comes what each node says of the key: its own record,
//! the residency marks, what its own copy and its peers' fetches make of the
//! key, and what the other owners answer. [`lines`] holds the whole layout
//! and is pure over a [`Scene`].
//!
//! Every verdict is a word as well as a color, so the overlay reads the same
//! in mono. Text that reaches the overlay from a node or from the keyboard
//! goes through [`printable`].

use std::time::Duration;

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Clear, Paragraph, Widget};
use smol_str::SmolStr;
use sundog::observe::MemberStatus;

use super::look::Token;
use super::panel::{self, gap};
use super::table::{self, Col};
use super::theme::Rgb;
use super::widgets::keycap;
use super::{Scene, data, eventlog, text};
use crate::app::{self, ExplainState};
use crate::explained::{
    self, Answer, Distributed, Explained, Local, NodeAnswer, Outcome, Reading, Source, Verdict,
    verdict,
};
use crate::key::{self, KeyError, KeySpec, printable};
use crate::locate::{LocateError, Located, locate};
use crate::once::status_name;

/// The widest the popup grows, in columns. All seven table columns fit.
const MAX_WIDTH: u16 = 120;

/// The width of the label column in front of the computed lines.
const LABEL: usize = 7;

/// Everything the overlay works out of the scene before it lays lines out.
struct Resolved<'a> {
    /// The typed key.
    spec: Result<KeySpec, KeyError>,
    /// Where the key lives, for a key that parses.
    placement: Option<Result<Located, LocateError>>,
    /// The answers to show: the last result, while it answers this key.
    answers: Option<&'a Explained>,
}

impl Resolved<'_> {
    fn located(&self) -> Option<&Located> {
        self.placement
            .as_ref()
            .and_then(|placement| placement.as_ref().ok())
    }
}

/// Reads the typed key, places it and picks the answers to show.
fn resolve<'a>(scene: &Scene<'_>, state: &'a ExplainState) -> Resolved<'a> {
    let spec = KeySpec::parse(&state.text);
    let cache = scene.app.ownership_cache(scene.model);
    let placement = spec
        .as_ref()
        .ok()
        .map(|spec| locate(scene.model, cache.as_deref(), spec));
    Resolved {
        spec,
        placement,
        answers: app::result_for(state),
    }
}

/// Draws the overlay over `area`, the body of the screen, so the header, the
/// caption and the footer stay visible.
pub fn render(scene: &Scene<'_>, state: &ExplainState, area: Rect, buf: &mut Buffer) {
    let width = area.width.saturating_sub(4).min(MAX_WIDTH);
    let max_height = area.height.saturating_sub(2);
    if width < 12 || max_height < 3 {
        return;
    }
    let look = scene.look;
    let resolved = resolve(scene, state);
    let inner_width = usize::from(width).saturating_sub(4);
    let inner_height = usize::from(max_height).saturating_sub(2);
    let body = compose(scene, state, &resolved, inner_width, inner_height);
    let height = u16::try_from(body.len() + 2)
        .unwrap_or(max_height)
        .min(max_height);
    let popup = Rect::new(
        area.x + (area.width - width) / 2,
        area.y + (area.height - height) / 2,
        width,
        height,
    );
    Clear.render(popup, buf);
    let block = Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(look.border(true))
        .style(look.surface())
        .title_top(Line::from(title(scene, &resolved)));
    let inner = block.inner(popup);
    block.render(popup, buf);
    Paragraph::new(body).render(panel::padded(inner), buf);
}

/// The popup's title: `Explain a read · it · computed`, with `asked` once
/// the nodes have answered and `(display frozen)` while the display is
/// frozen.
fn title(scene: &Scene<'_>, resolved: &Resolved<'_>) -> Vec<Span<'static>> {
    let look = scene.look;
    let mut spans = vec![Span::raw(" ")];
    match resolved.located() {
        Some(located) => {
            spans.push(panel::title_span(
                look,
                &format!("Explain a read · {}", printable(&located.cache)),
            ));
            spans.extend(panel::tag_spans(look, "computed"));
        }
        None => spans.push(panel::title_span(look, "Explain a read")),
    }
    if resolved.answers.is_some() {
        spans.extend(panel::tag_spans(look, "asked"));
    }
    if scene.app.is_frozen() {
        spans.push(look.span(" (display frozen)", Token::Warn));
    }
    spans.push(Span::raw(" "));
    spans
}

/// The overlay's lines for `state`, at most `height` of them, each fit to
/// `width` cells. The last line is the key hints, alone when `height` is
/// under 3. When the rows do not all fit, the detail block of the selected
/// node goes first, then the node rows give way to `… +N nodes`; the rows
/// that stay include the selected node's.
#[must_use]
pub fn lines(
    scene: &Scene<'_>,
    state: &ExplainState,
    width: usize,
    height: usize,
) -> Vec<Line<'static>> {
    compose(scene, state, &resolve(scene, state), width, height)
}

fn compose(
    scene: &Scene<'_>,
    state: &ExplainState,
    resolved: &Resolved<'_>,
    width: usize,
    height: usize,
) -> Vec<Line<'static>> {
    let bottom = bottom_line(scene);
    if height < 3 {
        return vec![bottom];
    }
    // The rows left for the body once a blank line and the key hints are
    // reserved.
    let budget = height - 2;
    let mut out = head(scene, state, resolved, width);
    out.truncate(budget);
    let mut left = budget - out.len();
    let asked = asked_section(scene, state, resolved, width);
    for line in asked.intro {
        if left == 0 {
            break;
        }
        out.push(line);
        left -= 1;
    }
    if let Some((header, rows)) = asked.table
        && left >= 2
    {
        out.push(header);
        left -= 1;
        if rows.len() + asked.detail.len() <= left {
            out.extend(rows);
            out.extend(asked.detail);
        } else if rows.len() <= left {
            out.extend(rows);
        } else {
            // The rows that stay form a window that ends at the selected
            // node's row when that row lies below the first ones.
            let shown = left - 1;
            let hidden = rows.len() - shown;
            let start = asked
                .anchor
                .saturating_sub(shown.saturating_sub(1))
                .min(hidden);
            out.extend(rows.into_iter().skip(start).take(shown));
            out.push(Line::from(vec![
                gap(2),
                scene.look.span(format!("… +{hidden} nodes"), Token::Muted),
            ]));
        }
    }
    out.push(Line::default());
    out.push(bottom);
    for line in &mut out {
        line.spans = panel::clip(std::mem::take(&mut line.spans), width);
    }
    out
}

/// A computed line: a muted label, then `spans`.
fn labeled(scene: &Scene<'_>, label: &str, mut spans: Vec<Span<'static>>) -> Line<'static> {
    let mut all = vec![scene.look.span(text::pad_right(label, LABEL), Token::Muted)];
    all.append(&mut spans);
    Line::from(all)
}

/// The key line and everything computed from the key.
fn head(
    scene: &Scene<'_>,
    state: &ExplainState,
    resolved: &Resolved<'_>,
    width: usize,
) -> Vec<Line<'static>> {
    let look = scene.look;
    // The cursor stays in view: a long key shows its end.
    let room = width.saturating_sub(LABEL + 1);
    let typed = printable(&state.text);
    let count = typed.chars().count();
    let shown: String = typed.chars().skip(count.saturating_sub(room)).collect();
    let mut out = vec![labeled(
        scene,
        "key",
        vec![look.span(shown, Token::Text), look.span("▌", Token::Accent)],
    )];
    let spec = match &resolved.spec {
        Ok(spec) => spec,
        Err(error) => {
            out.push(labeled(
                scene,
                "",
                vec![look.span(error.to_string(), Token::Warn)],
            ));
            return out;
        }
    };
    let bytes = spec.bytes().len();
    out.push(labeled(
        scene,
        "",
        vec![
            look.span(spec.to_string(), Token::Text),
            look.span(
                format!(
                    " · {bytes} {} {}",
                    if bytes == 1 { "byte" } else { "bytes" },
                    spec.hex()
                ),
                Token::Muted,
            ),
        ],
    ));
    out.push(labeled(
        scene,
        "",
        vec![look.span(
            "other key types land on other parts: prefix uint: int: hex: str:",
            Token::Faint,
        )],
    ));
    match &resolved.placement {
        Some(Ok(located)) => out.extend(computed(scene, located)),
        Some(Err(error)) => {
            // The message ends in what to do, so it wraps rather than clips.
            let message = printable(&error.to_string());
            for line in wrap(&message, width.saturating_sub(LABEL)) {
                out.push(labeled(scene, "", vec![look.span(line, Token::Warn)]));
            }
        }
        None => {}
    }
    out
}

/// `text` broken at spaces into lines of at most `width` characters. A word
/// longer than `width` keeps a line of its own.
fn wrap(text: &str, width: usize) -> Vec<String> {
    let mut lines: Vec<String> = Vec::new();
    for word in text.split_whitespace() {
        match lines.last_mut() {
            Some(line) if line.chars().count() + 1 + word.chars().count() <= width => {
                line.push(' ');
                line.push_str(word);
            }
            _ => lines.push(word.to_owned()),
        }
    }
    lines
}

/// The part, the view and the owners of a located key.
fn computed(scene: &Scene<'_>, located: &Located) -> Vec<Line<'static>> {
    let look = scene.look;
    let mut out = vec![labeled(
        scene,
        "part",
        vec![
            look.span(located.part_text(), Token::Text),
            look.span(
                format!(
                    " · bucket {}, part {}",
                    located.part.bucket(),
                    located.part.part()
                ),
                Token::Muted,
            ),
        ],
    )];
    out.push(labeled(scene, "view", view_spans(scene, located)));
    if scene.model.discovering() {
        out.push(labeled(
            scene,
            "",
            vec![look.span("provisional: still discovering", Token::Warn)],
        ));
    }
    out.extend(owner_lines(scene, located));
    out
}

/// The view of a located key: its hash, owners per part, what it ranks, and
/// whether it has settled.
fn view_spans(scene: &Scene<'_>, located: &Located) -> Vec<Span<'static>> {
    let look = scene.look;
    let mut view = vec![
        look.span(eventlog::view_hash(located.view_hash), Token::Text),
        look.span(
            format!(
                " · k={} · ranks {}",
                located.owners_per_part,
                if located.ranks_parts {
                    "parts"
                } else {
                    "buckets"
                }
            ),
            Token::Muted,
        ),
        look.span(" · ", Token::Faint),
    ];
    if located.settle.settled {
        view.push(look.span("✔ settled", Token::Ok));
    } else {
        let waited = data::view_state(scene.model, &located.cache, scene.ctx.wall)
            .and_then(|state| state.since)
            .map_or(String::new(), |since| format!(" {}", text::seconds(since)));
        view.push(look.span(format!("↻ settling{waited}"), Token::Move));
    }
    if located.settle.gossip_only {
        view.push(look.span(" (gossip only)", Token::Muted));
    }
    view
}

/// One line per owner in fetch order: its rank, `◆` for the first owner, its
/// slot, short id and status.
fn owner_lines(scene: &Scene<'_>, located: &Located) -> Vec<Line<'static>> {
    let look = scene.look;
    if located.owners.is_empty() {
        return vec![labeled(
            scene,
            "owner",
            vec![look.span("none: the view ranks no member", Token::Warn)],
        )];
    }
    located
        .owners
        .iter()
        .map(|owner| {
            let tag = data::tag_of(scene.model, owner.node);
            let mut spans = vec![
                look.span(
                    format!(
                        "{} {} ",
                        owner.rank,
                        if owner.rank == 1 { "◆" } else { " " }
                    ),
                    Token::Accent,
                ),
                Span::styled(
                    tag.label.to_string(),
                    look.node(tag.color).add_modifier(Modifier::BOLD),
                ),
                gap(1),
                look.span(tag.short, Token::Muted),
                gap(2),
                look.span(
                    owner.status.map_or("not listed", status_name).to_owned(),
                    match owner.status {
                        Some(MemberStatus::Live) => Token::Ok,
                        Some(_) => Token::Warn,
                        None => Token::Muted,
                    },
                ),
            ];
            if owner.rank == 1 {
                spans.push(look.span("  first owner: a fetch asks it first", Token::Muted));
            }
            labeled(scene, if owner.rank == 1 { "owner" } else { "" }, spans)
        })
        .collect()
}

/// The part of the overlay below the computed block.
struct Asked {
    /// The lines before the table: a blank line, then the verdict, the note
    /// or what `Enter` does.
    intro: Vec<Line<'static>>,
    /// The table's header and one line per asked node.
    table: Option<(Line<'static>, Vec<Line<'static>>)>,
    /// The selected node in detail, with a blank line before it.
    detail: Vec<Line<'static>>,
    /// The index in the table's rows of the selected node, 0 when no row is
    /// selected.
    anchor: usize,
}

fn asked_section(
    scene: &Scene<'_>,
    state: &ExplainState,
    resolved: &Resolved<'_>,
    width: usize,
) -> Asked {
    let look = scene.look;
    let blank = Line::default();
    let Some(answers) = resolved.answers else {
        let (token, words) = idle_note(scene, state, resolved);
        return Asked {
            intro: vec![blank, Line::from(vec![look.span(words, token)])],
            table: None,
            detail: Vec::new(),
            anchor: 0,
        };
    };
    let verdict = verdict(answers, resolved.located().map(Located::placement).as_ref());
    // A frozen display stops the clock at the freeze. Answers asked later
    // come from the live nodes, which the frozen placement need not match.
    let after_freeze = scene.app.is_frozen() && answers.asked > scene.ctx.wall;
    let age = if after_freeze {
        " · asked after the freeze".to_owned()
    } else {
        let age = scene
            .ctx
            .wall
            .duration_since(answers.asked)
            .unwrap_or_default();
        format!(" · asked {} ago", ago(age))
    };
    let summary = Line::from(vec![
        look.span(verdict.mark(), verdict.token()),
        gap(1),
        look.span(verdict.words(after_freeze), verdict.token()),
        look.span(age, Token::Muted),
    ]);
    let dim = matches!(verdict, Verdict::Moved { .. });
    let cols = columns(answers);
    let visible = table::visible(&cols, width);
    let header = table::header(look, &cols, &visible);
    let selected = selected_label(scene)
        .filter(|label| answers.nodes.iter().any(|answer| answer.label == *label))
        .or_else(|| answers.nodes.first().map(|answer| answer.label.clone()));
    let mut rows = Vec::new();
    for answer in &answers.nodes {
        let is_selected = selected.as_ref() == Some(&answer.label);
        let mut line = row(scene, answers, answer, &cols, &visible, is_selected);
        if dim {
            dim_line(&mut line);
        }
        rows.push(line);
    }
    let anchor = answers
        .nodes
        .iter()
        .position(|answer| selected.as_ref() == Some(&answer.label))
        .unwrap_or(0);
    let mut detail = Vec::new();
    if let Some(answer) = answers
        .nodes
        .iter()
        .find(|answer| selected.as_ref() == Some(&answer.label))
    {
        detail.push(Line::default());
        detail.extend(detail_lines(scene, answers, answer));
        if dim {
            detail.iter_mut().for_each(dim_line);
        }
    }
    Asked {
        intro: vec![blank, summary],
        table: Some((header, rows)),
        detail,
        anchor,
    }
}

/// Adds the dim modifier to every span of `line`.
fn dim_line(line: &mut Line<'static>) {
    for span in &mut line.spans {
        span.style = span.style.add_modifier(Modifier::DIM);
    }
}

/// What the block says when no answers show: the note of the last `Enter`,
/// else what `Enter` does.
fn idle_note(scene: &Scene<'_>, state: &ExplainState, resolved: &Resolved<'_>) -> (Token, String) {
    if let Some(note) = &state.note {
        let token = if state.pending.is_some() {
            Token::Info
        } else {
            Token::Warn
        };
        return (token, printable(note));
    }
    let Some(template) = &scene.app.config().control else {
        return (
            Token::Muted,
            "asked: test nodes answer over their control ports, which the demo knows".to_owned(),
        );
    };
    if state.text.is_empty() {
        return (
            Token::Muted,
            "type a key: Enter asks the test nodes about it".to_owned(),
        );
    }
    match &resolved.spec {
        Ok(spec) => {
            if let Some(why) = key::not_askable(spec) {
                return (Token::Muted, why.to_owned());
            }
            // An ask goes to the live nodes, which a frozen display need not
            // list.
            if scene.app.is_frozen() {
                return (
                    Token::Muted,
                    "Enter asks the live nodes about this key".to_owned(),
                );
            }
            let nodes = data::control_targets(scene.model, template).asked.len();
            (
                Token::Muted,
                format!(
                    "Enter asks {nodes} {} about this key",
                    if nodes == 1 { "node" } else { "nodes" }
                ),
            )
        }
        Err(_) => (Token::Muted, "fix the key to ask the nodes".to_owned()),
    }
}

impl Verdict {
    /// The glyph that leads the verdict line.
    const fn mark(&self) -> &'static str {
        match self {
            Self::Agree { .. } => "✔",
            Self::OtherCache { .. } | Self::Nothing(_) => "·",
            Self::Moved { .. } | Self::Split { .. } | Self::Apart { .. } => "↻",
        }
    }

    /// The color of the verdict line.
    const fn token(&self) -> Token {
        match self {
            Self::Agree { .. } => Token::Ok,
            Self::Moved { .. } => Token::Warn,
            Self::Split { .. } | Self::Apart { .. } => Token::Move,
            Self::OtherCache { .. } | Self::Nothing(_) => Token::Muted,
        }
    }
}

/// `span` as the overlay writes how long ago the nodes were asked.
fn ago(span: Duration) -> String {
    if span.as_secs() < 100 {
        text::seconds(span)
    } else {
        text::age(span)
    }
}

/// The slot label of the selected node, when the model lists it.
fn selected_label(scene: &Scene<'_>) -> Option<SmolStr> {
    let addr = scene.selected_addr()?;
    data::row_at(scene.model, addr).map(|row| row.slot.label.clone())
}

/// The table's columns. An answer from a cache that is not `Distributed`
/// has no owner columns, so they show only when some answer has them.
fn columns(answers: &Explained) -> Vec<Col> {
    let distributed = answers
        .readings()
        .any(|(_, reading)| reading.distributed.is_some());
    let mut cols = vec![Col::new("NODE", 7, 9)];
    if distributed {
        cols.push(Col::new("OWNS", 6, 8));
    }
    cols.push(Col::new("LOCAL", 26, 7));
    if distributed {
        cols.push(Col::new("READ", 14, 3));
        cols.push(Col::new("SERVES", 23, 2));
    }
    cols.push(Col::new("SOURCE", 15, 6));
    if distributed {
        cols.push(Col::new("PROBES", 24, 1));
    }
    cols
}

/// How a node is named: its slot label in its color when the model lists it,
/// else the label the answers give it, in the muted color.
fn name_of(scene: &Scene<'_>, answers: &Explained, hex: &str) -> Span<'static> {
    let look = scene.look;
    match data::tag_of_hex(scene.model, hex) {
        Some(tag) => Span::styled(
            tag.label.to_string(),
            look.node(tag.color).add_modifier(Modifier::BOLD),
        ),
        None => look.span(printable(&answers.label_of(hex)), Token::Muted),
    }
}

/// The text of [`name_of`].
fn name_text(scene: &Scene<'_>, answers: &Explained, hex: &str) -> String {
    name_of(scene, answers, hex).content.into_owned()
}

/// The color of the node labeled `label`, when the model lists it.
fn label_color(scene: &Scene<'_>, label: &str) -> Option<Rgb> {
    data::row_labeled(scene.model, label).map(|row| row.color())
}

/// The node cell of a table row: the selection mark, then the label.
fn node_cell(scene: &Scene<'_>, answer: &NodeAnswer, selected: bool) -> Vec<Span<'static>> {
    let look = scene.look;
    let mark = if selected {
        look.span("▌ ", Token::Accent)
    } else {
        gap(2)
    };
    let label = printable(&answer.label);
    let name = match label_color(scene, &answer.label) {
        Some(color) => Span::styled(label, look.node(color).add_modifier(Modifier::BOLD)),
        None => look.span(label, Token::Muted),
    };
    vec![mark, name]
}

/// One node's table row.
fn row(
    scene: &Scene<'_>,
    answers: &Explained,
    answer: &NodeAnswer,
    cols: &[Col],
    visible: &[usize],
    selected: bool,
) -> Line<'static> {
    let look = scene.look;
    let node = node_cell(scene, answer, selected);
    let line = match &answer.outcome {
        Outcome::Failed(reason) => {
            let mut spans = panel::pad_spans(node, cols[0].width);
            spans.push(look.span(printable(reason), Token::Warn));
            Line::from(spans)
        }
        Outcome::Read(reading) => {
            let dash = || vec![look.span("—", Token::Faint)];
            let distributed = reading.distributed.as_ref();
            let cells = cols
                .iter()
                .map(|col| match col.title.as_str() {
                    "NODE" => node.clone(),
                    "OWNS" => distributed.map_or_else(dash, |dist| {
                        if dist.residency.owns {
                            vec![look.span("yes", Token::Ok)]
                        } else {
                            vec![look.span("no", Token::Muted)]
                        }
                    }),
                    "LOCAL" => vec![look.span(local_text(reading), local_token(&reading.local))],
                    "READ" => distributed.map_or_else(dash, |dist| {
                        vec![look.span(dist.local_read.describe(), Token::Text)]
                    }),
                    "SERVES" => distributed.map_or_else(dash, |dist| {
                        vec![look.span(dist.serves_peers.describe(), Token::Text)]
                    }),
                    "SOURCE" => vec![look.span(source_text(scene, answers, reading), Token::Text)],
                    _ => distributed.map_or_else(dash, |dist| probes_spans(scene, answers, dist)),
                })
                .collect();
            table::row(cols, visible, cells)
        }
    };
    if selected {
        line.style(look.selected())
    } else {
        line
    }
}

/// The color of a record in the table.
const fn local_token(local: &Local) -> Token {
    match local {
        Local::Live { .. } => Token::Text,
        Local::Absent | Local::Tombstone { .. } => Token::Muted,
        Local::Lapsed { .. } | Local::Other(_) => Token::Warn,
    }
}

/// What the node stores for the key, short enough for the table: `absent`,
/// `tombstone`, `live · expires in 4.2 s`, `spilled · never expires`,
/// `lapsed idle`.
fn local_text(reading: &Reading) -> String {
    match &reading.local {
        Local::Absent => "absent".to_owned(),
        Local::Tombstone { .. } => "tombstone".to_owned(),
        Local::Live {
            expires_at_ms,
            spilled,
            ..
        } => format!(
            "{} · {}",
            if *spilled { "spilled" } else { "live" },
            explained::expiry_text(reading.at_ms, *expires_at_ms)
        ),
        Local::Lapsed { cause, .. } => format!("lapsed {}", cause.describe()),
        Local::Other(kind) => printable(kind),
    }
}

/// Where a fetch on the node takes its answer from: `this node hit`,
/// `n3 hit`, `unavailable`.
fn source_text(scene: &Scene<'_>, answers: &Explained, reading: &Reading) -> String {
    let outcome = |hit: bool| if hit { "hit" } else { "miss" };
    match &reading.source {
        Source::Local { hit } => format!("this node {}", outcome(*hit)),
        Source::Owner { node, hit } => {
            format!("{} {}", name_text(scene, answers, node), outcome(*hit))
        }
        Source::Unavailable => "unavailable".to_owned(),
        Source::Other(kind) => printable(kind),
    }
}

/// The probes of a node in one cell: `n2 held · n4 miss`.
fn probes_spans(scene: &Scene<'_>, answers: &Explained, dist: &Distributed) -> Vec<Span<'static>> {
    let look = scene.look;
    if dist.probes.is_empty() {
        return vec![look.span("none", Token::Muted)];
    }
    let mut spans = Vec::new();
    for (index, probe) in dist.probes.iter().enumerate() {
        if index > 0 {
            spans.push(look.span(" · ", Token::Faint));
        }
        spans.push(name_of(scene, answers, &probe.node));
        spans.push(look.span(format!(" {}", probe.answer.describe()), Token::Text));
    }
    spans
}

/// A version, `wall_ms.logical@node`, with the node named by its label.
fn version_text(scene: &Scene<'_>, answers: &Explained, version: &str) -> String {
    match version.split_once('@') {
        Some((stamp, node)) => format!("{}@{}", printable(stamp), name_text(scene, answers, node)),
        None => printable(version),
    }
}

/// The selected node in detail: its record, marks, reads, source and probes.
fn detail_lines(scene: &Scene<'_>, answers: &Explained, answer: &NodeAnswer) -> Vec<Line<'static>> {
    let look = scene.look;
    let mut head = node_cell(scene, answer, false);
    head.remove(0);
    head.push(look.span(" in detail", Token::Muted));
    let mut out = vec![Line::from(head)];
    let item = |label: &str, spans: Vec<Span<'static>>| {
        let mut all = vec![gap(2), look.span(text::pad_right(label, 8), Token::Muted)];
        all.extend(spans);
        Line::from(all)
    };
    let reading = match &answer.outcome {
        Outcome::Failed(reason) => {
            out.push(item(
                "reply",
                vec![look.span(printable(reason), Token::Warn)],
            ));
            return out;
        }
        Outcome::Read(reading) => reading,
    };
    out.push(item(
        "record",
        vec![look.span(record_text(scene, answers, reading), Token::Text)],
    ));
    if let Some(dist) = &reading.distributed {
        out.push(item(
            "marks",
            vec![look.span(
                format!(
                    "{} · {}",
                    if dist.residency.owns {
                        "owns the part"
                    } else {
                        "does not own the part"
                    },
                    explained::marks_text(&dist.residency)
                ),
                Token::Text,
            )],
        ));
        out.push(item(
            "reads",
            vec![look.span(
                format!(
                    "own copy: {} · serves peers: {}",
                    dist.local_read.describe(),
                    dist.serves_peers.describe()
                ),
                Token::Text,
            )],
        ));
        let view = match &dist.view_moved_to {
            Some(moved) => format!(
                "{} · moved to {} during the call",
                printable(&dist.view),
                printable(moved)
            ),
            None => printable(&dist.view),
        };
        out.push(item("view", vec![look.span(view, Token::Text)]));
    }
    out.push(item(
        "source",
        vec![look.span(source_words(scene, answers, reading), Token::Text)],
    ));
    if let Some(dist) = &reading.distributed {
        for (index, probe) in dist.probes.iter().enumerate() {
            let mut spans = vec![name_of(scene, answers, &probe.node)];
            spans.push(look.span(format!(" {}", probe.answer.describe()), Token::Text));
            spans.push(look.span(
                probe_detail(scene, answers, reading.at_ms, &probe.answer),
                Token::Muted,
            ));
            out.push(item(if index == 0 { "probes" } else { "" }, spans));
        }
    }
    out
}

/// The node's record in words, with its version.
fn record_text(scene: &Scene<'_>, answers: &Explained, reading: &Reading) -> String {
    match &reading.local {
        Local::Absent => "absent".to_owned(),
        Local::Tombstone { version } => {
            format!("tombstone {}", version_text(scene, answers, version))
        }
        Local::Live {
            version,
            expires_at_ms,
            spilled,
        } => format!(
            "live {} · {}{}",
            version_text(scene, answers, version),
            explained::expiry_text(reading.at_ms, *expires_at_ms),
            if *spilled {
                " · in the spill tier"
            } else {
                ""
            }
        ),
        Local::Lapsed {
            version,
            expires_at_ms,
            cause,
        } => format!(
            "lapsed ({}) {} · {}",
            cause.describe(),
            version_text(scene, answers, version),
            explained::expiry_text(reading.at_ms, *expires_at_ms)
        ),
        Local::Other(kind) => printable(kind),
    }
}

/// Where a fetch on the node takes its answer, in a sentence.
fn source_words(scene: &Scene<'_>, answers: &Explained, reading: &Reading) -> String {
    let outcome = |hit: bool| {
        if hit { "a hit" } else { "a miss" }
    };
    match &reading.source {
        Source::Local { hit } => format!("this node answers its own fetch: {}", outcome(*hit)),
        Source::Owner { node, hit } => format!(
            "the first owner to answer is {}: {}",
            name_text(scene, answers, node),
            outcome(*hit)
        ),
        Source::Unavailable => "no owner answers, so the fetch fails".to_owned(),
        Source::Other(kind) => printable(kind),
    }
}

/// What an owner's answer carries beyond its name.
fn probe_detail(scene: &Scene<'_>, answers: &Explained, at_ms: u64, answer: &Answer) -> String {
    match answer {
        Answer::Held {
            version,
            expires_at_ms,
            reads,
        } => format!(
            " · {} · {} · a read gets {}",
            version_text(scene, answers, version),
            explained::expiry_text(at_ms, *expires_at_ms),
            reads.describe()
        ),
        Answer::StaleView { responder_view } => {
            format!(" · its view is {}", printable(responder_view))
        }
        _ => String::new(),
    }
}

/// The key hints at the bottom of the popup. The prompt types `q`, so the
/// hints name `Ctrl-C` as the way out of the interface.
fn bottom_line(scene: &Scene<'_>) -> Line<'static> {
    let hints: &[(&str, &str)] = if scene.app.config().control.is_some() {
        &[
            ("Enter", "ask"),
            ("↑↓", "node"),
            ("Ctrl-U", "clear"),
            ("Esc", "close"),
            ("Ctrl-C", "quit"),
        ]
    } else {
        &[("Ctrl-U", "clear"), ("Esc", "close"), ("Ctrl-C", "quit")]
    };
    Line::from(keycap::keycap_spans(hints, scene.look.mode))
}

#[cfg(test)]
mod tests;

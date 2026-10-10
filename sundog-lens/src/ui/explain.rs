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
    self, Answer, Distributed, Explained, Local, NodeAnswer, Outcome, Reading, Source,
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
/// node goes first, then the node rows give way to `… +N nodes`.
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
            let shown = left - 1;
            let hidden = rows.len() - shown;
            out.extend(rows.into_iter().take(shown));
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
        Some(Err(error)) => out.push(labeled(
            scene,
            "",
            vec![look.span(printable(&error.to_string()), Token::Warn)],
        )),
        None => {}
    }
    out
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
    if located.conflicted {
        out.push(labeled(
            scene,
            "",
            vec![look.span(
                "⚠ the nodes disagree on this cache's mode: the view is the Distributed nodes'",
                Token::Warn,
            )],
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
        };
    };
    let verdict = verdict(answers, resolved.located());
    let age = scene
        .ctx
        .wall
        .duration_since(answers.asked)
        .unwrap_or_default();
    let summary = Line::from(vec![
        look.span(verdict.mark(), verdict.token()),
        gap(1),
        look.span(verdict.words(), verdict.token()),
        look.span(format!(" · asked {} ago", ago(age)), Token::Muted),
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

/// What the answers say against each other and against the lens.
enum Verdict {
    /// Every `Distributed` answer holds the lens's view and owners.
    Agree {
        /// How many nodes answered.
        nodes: usize,
    },
    /// No answer holds the lens's view: the view moved since the nodes were
    /// asked.
    Moved {
        /// The view the answers used, eight digits.
        from: String,
        /// The lens's view, eight digits.
        to: String,
    },
    /// The nodes hold different views.
    Split {
        /// The nodes in each view, as words.
        groups: String,
    },
    /// The nodes agree on a view but not with the lens on the placement.
    Apart {
        /// The nodes that differ, as words.
        nodes: String,
        /// Whether more than one node differs.
        many: bool,
        /// The lens's view, eight digits.
        view: String,
    },
    /// Nothing to compare, for the reason in words.
    Nothing(&'static str),
}

impl Verdict {
    const fn mark(&self) -> &'static str {
        match self {
            Self::Agree { .. } => "✔",
            Self::Nothing(_) => "·",
            Self::Moved { .. } | Self::Split { .. } | Self::Apart { .. } => "↻",
        }
    }

    const fn token(&self) -> Token {
        match self {
            Self::Agree { .. } => Token::Ok,
            Self::Moved { .. } => Token::Warn,
            Self::Split { .. } | Self::Apart { .. } => Token::Move,
            Self::Nothing(_) => Token::Muted,
        }
    }

    fn words(&self) -> String {
        match self {
            Self::Agree { nodes: 1 } => "1 node agrees, the lens computes the same".to_owned(),
            Self::Agree { nodes } => {
                format!("{nodes} nodes agree, the lens computes the same")
            }
            Self::Moved { from, to } => {
                format!("view moved {from} -> {to}: Enter asks again")
            }
            Self::Split { groups } => groups.clone(),
            Self::Apart { nodes, many, view } => format!(
                "{nodes} {} from the lens, which holds view {view}",
                if *many { "differ" } else { "differs" }
            ),
            Self::Nothing(why) => (*why).to_owned(),
        }
    }
}

/// Compares `answers` with `located`, when the lens placed the key.
fn verdict(answers: &Explained, located: Option<&Located>) -> Verdict {
    let Some(located) = located else {
        return Verdict::Nothing("the lens has no placement of the key to compare with");
    };
    let agreement = explained::agreement(answers, located);
    let lens = format!("{:016x}", located.view_hash);
    let short = |view: &str| printable(view.get(..8).unwrap_or(view));
    let Some(first) = agreement.groups.first() else {
        return Verdict::Nothing("no node gave a Distributed reading to compare");
    };
    if agreement.groups.iter().all(|group| group.view != lens) {
        return Verdict::Moved {
            from: short(&first.view),
            to: short(&lens),
        };
    }
    if agreement.groups.len() > 1 {
        let groups: Vec<String> = agreement
            .groups
            .iter()
            .map(|group| {
                let nodes: Vec<String> = group.nodes.iter().map(|node| printable(node)).collect();
                format!(
                    "{} {} {}",
                    nodes.join(" "),
                    if nodes.len() == 1 { "holds" } else { "hold" },
                    short(&group.view)
                )
            })
            .collect();
        return Verdict::Split {
            groups: groups.join(", "),
        };
    }
    if agreement.apart.is_empty() {
        let nodes = agreement.groups.iter().map(|group| group.nodes.len()).sum();
        return Verdict::Agree { nodes };
    }
    let nodes: Vec<String> = agreement.apart.iter().map(|node| printable(node)).collect();
    Verdict::Apart {
        many: nodes.len() > 1,
        nodes: nodes.join(" "),
        view: short(&lens),
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

/// The key hints at the bottom of the popup.
fn bottom_line(scene: &Scene<'_>) -> Line<'static> {
    let hints: &[(&str, &str)] = if scene.app.config().control.is_some() {
        &[
            ("Enter", "ask"),
            ("↑↓", "node"),
            ("Ctrl-U", "clear"),
            ("Esc", "close"),
        ]
    } else {
        &[("Ctrl-U", "clear"), ("Esc", "close")]
    };
    Line::from(keycap::keycap_spans(hints, scene.look.mode))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant, SystemTime};

    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use ratatui::style::Color;
    use sundog::store::{Mode, PartId};

    use super::*;
    use crate::app::{Action, App, AppConfig, ExplainRequest, FleetCmd, UiCommand};
    use crate::explained::fixture;
    use crate::model::Model;
    use crate::model::testkit;
    use crate::source::Update;
    use crate::source::targets::UrlTemplate;
    use crate::ui::look::Look;
    use crate::ui::panel::row_text;
    use crate::ui::theme::{self, ColorMode};
    use crate::ui::{Ctx, LayoutKind};

    fn model() -> Model {
        testkit::fixture_model(Instant::now())
    }

    fn control() -> UrlTemplate {
        UrlTemplate::parse("{ip}:{gossip_port+134}").expect("the template parses")
    }

    fn app_in(look: Look, control: Option<UrlTemplate>) -> App {
        App::new(AppConfig {
            look,
            control,
            ..AppConfig::default()
        })
    }

    fn press(app: &mut App, model: &Model, code: KeyCode) -> Action {
        app.handle_key(KeyEvent::new(code, KeyModifiers::NONE), model)
    }

    /// An app with the overlay open on `text`, whose control ports are known.
    fn opened(model: &Model, text: &str) -> App {
        let mut app = app_in(Look::default(), Some(control()));
        typed(&mut app, model, text);
        app
    }

    /// Opens the overlay, if it is closed, and types `text` over its key.
    fn typed(app: &mut App, model: &Model, text: &str) {
        if app.explain.is_none() {
            press(app, model, KeyCode::Char('e'));
        }
        let ctrl_u = KeyEvent::new(KeyCode::Char('u'), KeyModifiers::CONTROL);
        app.handle_key(ctrl_u, model);
        for c in text.chars() {
            press(app, model, KeyCode::Char(c));
        }
    }

    fn ask(app: &mut App, model: &Model) -> ExplainRequest {
        match press(app, model, KeyCode::Enter) {
            Action::Fleet(FleetCmd::Explain(request)) => request,
            other => panic!("Enter asks the nodes, not {other:?}"),
        }
    }

    /// The nodes' answers to `request` as a converged cluster gives them,
    /// changed by `edit`, delivered to the overlay.
    fn answered(
        app: &mut App,
        model: &Model,
        request: &ExplainRequest,
        edit: impl FnOnce(&mut Explained),
    ) {
        let asked = model.wall().expect("the model has a wall clock") - Duration::from_secs(3);
        let mut answers = fixture::explained(model, request.id, &request.key, asked);
        edit(&mut answers);
        app.apply_director(UiCommand::Explained(Box::new(answers)), model);
    }

    fn ctx_of(model: &Model) -> Ctx {
        Ctx {
            now: model.now().expect("the model has a clock"),
            wall: model.wall().expect("the model has a wall clock"),
            elapsed: Duration::ZERO,
        }
    }

    fn text_of(line: &Line<'_>) -> String {
        line.spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect()
    }

    /// The overlay's lines as text.
    fn shown(app: &App, model: &Model, width: usize, height: usize) -> Vec<String> {
        let ctx = ctx_of(model);
        let scene = Scene {
            app,
            model: app.shown(model),
            ctx: &ctx,
            look: app.look(),
            kind: LayoutKind::Full,
        };
        let state = app.explain.as_ref().expect("the overlay is open");
        lines(&scene, state, width, height)
            .iter()
            .map(text_of)
            .collect()
    }

    /// The overlay drawn into a `width` by `height` body: its rows as text.
    fn drawn(app: &App, model: &Model, width: u16, height: u16) -> Vec<String> {
        let ctx = ctx_of(model);
        let scene = Scene {
            app,
            model: app.shown(model),
            ctx: &ctx,
            look: app.look(),
            kind: LayoutKind::Full,
        };
        let area = Rect::new(0, 0, width, height);
        let mut buf = Buffer::empty(area);
        render(
            &scene,
            app.explain.as_ref().expect("the overlay is open"),
            area,
            &mut buf,
        );
        (0..height).map(|y| row_text(&buf, y)).collect()
    }

    fn find<'a>(rows: &'a [String], needle: &str) -> &'a str {
        rows.iter()
            .map(String::as_str)
            .find(|row| row.contains(needle))
            .unwrap_or_else(|| panic!("no line holds {needle:?} in\n{}", rows.join("\n")))
    }

    fn has(rows: &[String], needle: &str) -> bool {
        rows.iter().any(|row| row.contains(needle))
    }

    /// A model that has seen `snapshot` and no ownership.
    fn model_of(snapshot: &sundog::observe::ClusterSnapshot) -> Model {
        let base = Instant::now();
        let mut model = Model::new();
        model.apply(
            Update::Snapshot(Arc::new(snapshot.clone()), base),
            base,
            SystemTime::UNIX_EPOCH + Duration::from_secs(100),
        );
        model
    }

    #[test]
    fn the_overlay_echoes_the_key_kind_bytes_and_part() {
        let model = model();
        let app = opened(&model, "k1");
        let rows = shown(&app, &model, 100, 30);
        assert_eq!(rows[0], "key    k1▌");
        assert_eq!(rows[1], "       \"k1\" as String · 3 bytes 02 6b 31");
        assert!(
            rows[2].contains("prefix uint: int: hex: str:"),
            "{}",
            rows[2]
        );
        let part = PartId::of_key(&[2, b'k', b'1']);
        let line = find(&rows, "part   ");
        assert_eq!(
            line,
            format!(
                "part   {}/{} · bucket {}, part {}",
                part.bucket(),
                part.part(),
                part.bucket(),
                part.part()
            )
        );
        for (typed, echo) in [
            ("uint:7", "7 as unsigned integer · 1 byte 07"),
            ("int:-1", "-1 as signed integer · 1 byte 01"),
            ("hex:ff00", "ff 00 as postcard bytes · 2 bytes ff 00"),
        ] {
            let app = opened(&model, typed);
            let rows = shown(&app, &model, 100, 30);
            assert_eq!(rows[1].trim(), echo, "{typed}");
        }
    }

    #[test]
    fn owners_are_in_fetch_order_with_slot_labels_and_the_first_owner_mark() {
        let model = model();
        let app = opened(&model, "k1");
        let rows = shown(&app, &model, 100, 30);
        let located = locate(&model, Some("it"), &KeySpec::parse("k1").unwrap()).unwrap();
        let first = &located.owners[0];
        let second = &located.owners[1];
        let tag = |owner: &crate::locate::LocatedOwner| data::tag_of(&model, owner.node);
        let first_row = find(&rows, "owner  1 ◆");
        assert!(
            first_row.contains(&format!("{} {}  live", tag(first).label, tag(first).short)),
            "{first_row}"
        );
        assert!(first_row.contains("first owner: a fetch asks it first"));
        let second_row = find(&rows, &format!("2   {}", tag(second).label));
        assert!(second_row.starts_with(NO_LABEL_TEXT), "{second_row}");
        assert!(second_row.contains("live"), "{second_row}");
        assert!(!second_row.contains('◆'), "{second_row}");
        let at = |row: &str| rows.iter().position(|r| r == row).unwrap();
        assert!(at(first_row) < at(second_row));
    }

    /// The text of a label column with no label.
    const NO_LABEL_TEXT: &str = "       ";

    #[test]
    fn the_view_line_names_the_view_the_verdict_and_what_it_rests_on() {
        let model = model();
        let app = opened(&model, "k1");
        let rows = shown(&app, &model, 100, 30);
        let digest = model.ownership("it").unwrap();
        let view = find(&rows, "view   ");
        assert!(
            view.starts_with(&format!(
                "view   {} · k=2 · ranks parts · ",
                eventlog::view_hash(digest.view_hash)
            )),
            "{view}"
        );
        let settled = model.settle("it").unwrap();
        assert_eq!(view.contains("✔ settled"), settled.settled, "{view}");
        assert_eq!(
            view.contains("(gossip only)"),
            settled.gossip_only,
            "{view}"
        );
        let unsettled = testkit::fixture_model_with_metrics(Instant::now());
        assert!(!unsettled.settle("it").unwrap().settled);
        let rows = shown(&opened(&unsettled, "k1"), &unsettled, 100, 30);
        let view = find(&rows, "view   ");
        assert!(view.contains("↻ settling"), "{view}");
        assert!(!view.contains("✔ settled"), "{view}");
    }

    #[test]
    fn a_cluster_with_no_distributed_cache_says_so() {
        let snapshot = sundog::observe::ClusterSnapshot::new(
            "fixture",
            (1..=3)
                .map(|index| {
                    testkit::member_with(
                        index,
                        0,
                        1,
                        sundog::observe::MemberStatus::Live,
                        &[("churn", Mode::Replicated)],
                    )
                })
                .collect(),
            0,
        );
        let model = model_of(&snapshot);
        let app = opened(&model, "k1");
        let rows = shown(&app, &model, 100, 30);
        assert_eq!(rows[0], "key    k1▌");
        assert!(
            has(
                &rows,
                "no Distributed cache is advertised: explain needs part ownership"
            ),
            "{rows:?}"
        );
        assert!(!has(&rows, "part   "), "{rows:?}");
        assert!(!has(&rows, "owner  "), "{rows:?}");
        let title = drawn(&app, &model, 100, 30);
        assert!(find(&title, "Explain a read").contains("╭ Explain a read ─"));
        assert!(!find(&title, "Explain a read").contains("computed"));
    }

    #[test]
    fn discovering_marks_the_block_provisional() {
        let snapshot = testkit::snapshot_with_owners(3, 2);
        let mut model = model_of(&snapshot);
        let base = Instant::now();
        let digest = testkit::ownership_digest(&snapshot, "it").unwrap();
        model.apply(
            Update::Ownership(digest),
            base,
            SystemTime::UNIX_EPOCH + Duration::from_secs(100),
        );
        assert!(model.discovering());
        let rows = shown(&opened(&model, "k1"), &model, 100, 30);
        assert!(has(&rows, "provisional: still discovering"), "{rows:?}");
        assert!(has(&rows, "part   "), "the placement still shows");
        let settled = self::model();
        assert!(!settled.discovering());
        let rows = shown(&opened(&settled, "k1"), &settled, 100, 30);
        assert!(!has(&rows, "provisional"), "{rows:?}");
    }

    #[test]
    fn a_bad_key_shows_its_error_in_place_of_the_block() {
        let model = model();
        for (typed, error) in [
            ("uint:x", "uint: takes decimal digits"),
            ("uint:-1", "uint: takes no sign"),
            ("hex:abc", "hex: takes pairs of hex digits and this has 3"),
            ("int:", "int: takes an optional minus sign"),
        ] {
            let app = opened(&model, typed);
            let rows = shown(&app, &model, 100, 30);
            assert_eq!(rows[0], format!("key    {typed}▌"));
            assert!(
                rows[1].starts_with(NO_LABEL_TEXT) && rows[1].contains(error),
                "{rows:?}"
            );
            for gone in ["part   ", "view   ", "owner", "bytes"] {
                assert!(!has(&rows, gone), "{gone} in {rows:?}");
            }
            let title = drawn(&app, &model, 100, 30);
            assert!(!has(&title, "computed"), "{title:?}");
        }
        assert!(has(
            &shown(&opened(&model, "hex:zz"), &model, 100, 30),
            "fix the key to ask the nodes"
        ));
        // The note of an Enter on a bad key names the error and asks nobody.
        let mut app = opened(&model, "uint:x");
        assert_eq!(press(&mut app, &model, KeyCode::Enter), Action::Redraw);
        let rows = shown(&app, &model, 100, 30);
        assert!(
            has(&rows, "nobody is asked: uint: takes decimal digits"),
            "{rows:?}"
        );
    }

    #[test]
    fn hostile_text_stays_in_the_allowlist() {
        let model = model();
        let mut app = opened(&model, "k\u{1b}[31m\u{202e}é\u{7}");
        let hostile = "\u{1b}[2J\u{7}boom \u{202e}é\u{85}";
        for check in [false, true] {
            let rows = shown(&app, &model, 100, 30);
            for row in &rows {
                for c in row.chars() {
                    assert!(theme::is_allowed(c) && !c.is_control(), "{c:?} in {row}");
                }
            }
            if check {
                break;
            }
            typed(&mut app, &model, "k1");
            let request = ask(&mut app, &model);
            answered(&mut app, &model, &request, |answers| {
                answers.nodes[1].outcome = Outcome::Failed(hostile.to_owned());
                answers.nodes[2].label = SmolStr::new("n\u{1b}3\u{202e}");
                if let Outcome::Read(reading) = &mut answers.nodes[3].outcome {
                    reading.local = Local::Other(hostile.to_owned());
                    reading.source = Source::Other(hostile.to_owned());
                    if let Some(dist) = &mut reading.distributed {
                        dist.probes[0].node = hostile.to_owned();
                        dist.view = hostile.to_owned();
                        dist.view_moved_to = Some(hostile.to_owned());
                    }
                }
            });
        }
        let rows = drawn(&app, &model, 140, 40);
        for row in &rows {
            for c in row.chars() {
                assert!(theme::is_allowed(c) && !c.is_control(), "{c:?} in {row}");
            }
        }
    }

    #[test]
    fn asking_names_the_nodes() {
        let model = model();
        let mut app = opened(&model, "k1");
        let rows = shown(&app, &model, 100, 30);
        assert!(has(&rows, "Enter asks 5 nodes about this key"), "{rows:?}");
        ask(&mut app, &model);
        let rows = shown(&app, &model, 100, 30);
        assert!(has(&rows, "asking n1 n2 n3 n4 n5"), "{rows:?}");
        assert!(!has(&rows, "NODE"), "no table before the answers: {rows:?}");
    }

    #[test]
    fn what_enter_does_is_stated_before_and_after_it_is_pressed() {
        let model = model();
        let mut app = opened(&model, "uint:5");
        let rows = shown(&app, &model, 100, 30);
        assert!(has(&rows, "test nodes take String keys"), "{rows:?}");
        assert_eq!(press(&mut app, &model, KeyCode::Enter), Action::Redraw);
        let rows = shown(&app, &model, 100, 30);
        assert!(
            has(&rows, "nobody is asked: test nodes take String keys"),
            "{rows:?}"
        );
        // A lens without control ports says what asks need.
        let mut plain = app_in(Look::default(), None);
        typed(&mut plain, &model, "k1");
        let rows = shown(&plain, &model, 100, 30);
        assert!(
            has(&rows, "asked: test nodes answer over their control ports"),
            "{rows:?}"
        );
        assert!(
            !has(&rows, "Enter ask"),
            "no Enter hint without ports: {rows:?}"
        );
        assert!(rows.last().unwrap().contains("Ctrl-U clear  Esc close"));
        press(&mut plain, &model, KeyCode::Enter);
        let rows = shown(&plain, &model, 100, 30);
        assert!(
            has(&rows, "nobody is asked: this lens does not know"),
            "{rows:?}"
        );
        // The key hints with ports.
        let rows = shown(&opened(&model, "k1"), &model, 100, 30);
        assert_eq!(
            rows.last().unwrap(),
            "Enter ask  ↑↓ node  Ctrl-U clear  Esc close"
        );
    }

    #[test]
    fn a_result_lists_every_node_with_owns_local_source_and_probes() {
        let model = model();
        let mut app = opened(&model, "k1");
        let request = ask(&mut app, &model);
        answered(&mut app, &model, &request, |_| {});
        let rows = shown(&app, &model, 116, 40);
        let header = find(&rows, "NODE");
        for column in ["OWNS", "LOCAL", "READ", "SERVES", "SOURCE", "PROBES"] {
            assert!(header.contains(column), "{column} in {header}");
        }
        let located = locate(&model, Some("it"), &KeySpec::parse("k1").unwrap()).unwrap();
        let owners: Vec<_> = located.owners.iter().map(|o| o.slot.to_string()).collect();
        let first = owners[0].as_str();
        let start = rows.iter().position(|row| row == header).unwrap();
        for (offset, label) in ["n1", "n2", "n3", "n4", "n5"].into_iter().enumerate() {
            let row = &rows[start + 1 + offset];
            assert!(row.contains(&format!(" {label} ")), "{row}");
            if owners.iter().any(|owner| owner == label) {
                assert!(row.contains("yes"), "{row}");
                assert!(row.contains("live · expires in 38.1 s"), "{row}");
                assert!(row.contains("this node hit"), "{row}");
                assert!(row.contains("serves"), "{row}");
                assert!(row.contains(" hit "), "{row}");
                let other = owners.iter().find(|owner| *owner != label).unwrap();
                assert!(row.contains(&format!("{other} held")), "{row}");
            } else {
                assert!(row.contains("no "), "{row}");
                assert!(row.contains("absent"), "{row}");
                assert!(row.contains("not an owner"), "{row}");
                assert!(row.contains(&format!("{first} hit")), "{row}");
                assert!(
                    row.contains(&format!("{} held · {} held", owners[0], owners[1])),
                    "{row}"
                );
            }
        }
    }

    #[test]
    fn agreeing_views_show_the_check_and_the_lens_hash() {
        let model = model();
        let mut app = opened(&model, "k1");
        let request = ask(&mut app, &model);
        answered(&mut app, &model, &request, |_| {});
        let rows = shown(&app, &model, 116, 40);
        assert!(
            has(
                &rows,
                "✔ 5 nodes agree, the lens computes the same · asked 3.0 s ago"
            ),
            "{rows:?}"
        );
        let digest = model.ownership("it").unwrap();
        assert!(has(&rows, &eventlog::view_hash(digest.view_hash)));
        let title = drawn(&app, &model, 140, 40);
        assert!(
            has(&title, "Explain a read · it · computed · asked"),
            "{title:?}"
        );
    }

    #[test]
    fn disagreeing_views_show_who_holds_which() {
        let model = model();
        let mut app = opened(&model, "k1");
        let request = ask(&mut app, &model);
        answered(&mut app, &model, &request, |answers| {
            if let Outcome::Read(reading) = &mut answers.nodes[3].outcome {
                reading.distributed.as_mut().unwrap().view = "71c0a2f4e8b3195d".to_owned();
            }
        });
        let rows = shown(&app, &model, 116, 40);
        let digest = model.ownership("it").unwrap();
        let lens = eventlog::view_hash(digest.view_hash);
        let line = find(&rows, "↻ n1");
        assert!(
            line.starts_with(&format!(
                "↻ n1 n2 n3 n5 hold {lens}, n4 holds 71c0a2f4 · asked"
            )),
            "{line}"
        );
        assert!(!has(&rows, "agree"), "{rows:?}");
        // Every node on one view that is not the lens's: the nodes differ
        // from the lens only when the placement differs; the view moved when
        // none holds the lens's.
        let mut app = opened(&model, "k1");
        let request = ask(&mut app, &model);
        answered(&mut app, &model, &request, |answers| {
            if let Outcome::Read(reading) = &mut answers.nodes[2].outcome {
                reading.distributed.as_mut().unwrap().owners.reverse();
            }
        });
        let rows = shown(&app, &model, 116, 40);
        let line = find(&rows, "differs from the lens");
        assert!(
            line.starts_with(&format!(
                "↻ n3 differs from the lens, which holds view {lens} · asked"
            )),
            "{line}"
        );
        // Two nodes that differ read in the plural.
        let mut app = opened(&model, "k1");
        let request = ask(&mut app, &model);
        answered(&mut app, &model, &request, |answers| {
            for answer in &mut answers.nodes[1..3] {
                if let Outcome::Read(reading) = &mut answer.outcome {
                    reading.distributed.as_mut().unwrap().owners.reverse();
                }
            }
        });
        let rows = shown(&app, &model, 116, 40);
        let line = find(&rows, "differ from the lens");
        assert!(
            line.starts_with(&format!(
                "↻ n2 n3 differ from the lens, which holds view {lens} · asked"
            )),
            "{line}"
        );
    }

    #[test]
    fn one_node_that_agrees_reads_in_the_singular() {
        let model = model();
        let mut app = opened(&model, "k1");
        let request = ask(&mut app, &model);
        answered(&mut app, &model, &request, |answers| {
            answers.nodes.truncate(1);
        });
        let rows = shown(&app, &model, 116, 40);
        assert!(
            has(&rows, "✔ 1 node agrees, the lens computes the same · asked"),
            "{rows:?}"
        );
        assert!(!has(&rows, "1 nodes"), "{rows:?}");
    }

    #[test]
    fn a_failed_node_keeps_its_row_with_the_reason() {
        let model = model();
        let mut app = opened(&model, "k1");
        let request = ask(&mut app, &model);
        answered(&mut app, &model, &request, |answers| {
            answers.nodes[1].outcome = Outcome::Failed("no answer (connection refused)".to_owned());
            answers.nodes[3].outcome = Outcome::Failed(crate::ask::PREDATES.to_owned());
        });
        let rows = shown(&app, &model, 116, 40);
        let header = rows.iter().position(|row| row.contains("NODE")).unwrap();
        assert!(
            rows[header + 2].starts_with("  n2")
                && rows[header + 2].contains("no answer (connection refused)"),
            "{}",
            rows[header + 2]
        );
        assert!(
            rows[header + 4].starts_with("  n4")
                && rows[header + 4].contains("rebuild sundog-testnode"),
            "{}",
            rows[header + 4]
        );
        // The three that answered still agree, and the failures take no part
        // in the comparison.
        assert!(has(&rows, "✔ 3 nodes agree"), "{rows:?}");
        assert_eq!(rows[header + 1].matches("n1").count(), 1);
    }

    #[test]
    fn the_detail_block_follows_the_selected_node() {
        let model = model();
        let mut app = opened(&model, "k1");
        let request = ask(&mut app, &model);
        answered(&mut app, &model, &request, |_| {});
        let located = locate(&model, Some("it"), &KeySpec::parse("k1").unwrap()).unwrap();
        let first = located.owners[0].slot.to_string();
        let rows = shown(&app, &model, 116, 40);
        assert!(has(&rows, "n1 in detail"), "{rows:?}");
        assert!(find(&rows, "record  ").contains("absent"));
        assert!(find(&rows, "marks   ").contains("does not own the part · none"));
        assert!(find(&rows, "reads   ").contains("own copy: not an owner · serves peers: miss"));
        assert!(
            find(&rows, "source  ")
                .contains(&format!("the first owner to answer is {first}: a hit"))
        );
        let probes = find(&rows, "probes  ");
        assert!(
            probes.contains(&format!("{first} held · 1760054300112.3@{first}")),
            "{probes}"
        );
        assert!(
            probes.contains("expires in 38.1 s · a read gets a value"),
            "{probes}"
        );
        // Down moves the one node selection; the detail follows.
        press(&mut app, &model, KeyCode::Down);
        press(&mut app, &model, KeyCode::Down);
        let rows = shown(&app, &model, 116, 40);
        assert!(has(&rows, "n3 in detail"), "{rows:?}");
        assert!(!has(&rows, "n1 in detail"));
        let selected = rows
            .iter()
            .find(|row| row.starts_with("▌ n3") || row.contains("▌ n3"));
        assert!(selected.is_some(), "{rows:?}");
        if first == "n3" {
            assert!(
                find(&rows, "record  ").contains("live 1760054300112.3@n3 · expires in 38.1 s")
            );
            assert!(find(&rows, "marks   ").contains("owns the part · none"));
        }
        // A failed node's detail is its reason.
        answered_failure(&mut app, &model);
        let rows = shown(&app, &model, 116, 40);
        assert!(has(&rows, "n3 in detail"));
        assert!(
            find(&rows, "reply   ").contains("no answer within 3 s"),
            "{rows:?}"
        );
    }

    /// Delivers a second request's answers in which n3 stayed silent.
    fn answered_failure(app: &mut App, model: &Model) {
        let request = ask(app, model);
        answered(app, model, &request, |answers| {
            answers.nodes[2].outcome = Outcome::Failed("no answer within 3 s".to_owned());
        });
    }

    /// The reading of the `at`th answer, which must have one.
    fn reading(answers: &mut Explained, at: usize) -> &mut Reading {
        match &mut answers.nodes[at].outcome {
            Outcome::Read(reading) => reading,
            Outcome::Failed(reason) => panic!("node {at} failed: {reason}"),
        }
    }

    /// What the answers of [`odd_answers`] name.
    struct Odd {
        /// The slot labels of the key's first and second owner.
        first: String,
        second: String,
        /// The lens's view, 16 digits.
        view: String,
        /// The version every record carries, with its node named by label.
        version: String,
    }

    /// An overlay whose answers hold every kind of record, source and probe
    /// the fixture cluster leaves out: n1 a tombstone with no owner to answer,
    /// an owner out of reach, an owner on another view and a view of its own
    /// that moved; n2 an entry gone idle on a part it gives up, beside owners
    /// that miss and decline; n3 a spilled entry that never expires; n4 an
    /// entry whose expiry passed.
    fn odd_answers(model: &Model) -> (App, Odd) {
        use crate::explained::{Lapse, Probe, Why};

        let located = locate(model, Some("it"), &KeySpec::parse("k1").unwrap()).unwrap();
        let (first, second) = (
            located.owners[0].node.to_string(),
            located.owners[1].node.to_string(),
        );
        let stamp = format!("1760054300112.3@{first}");
        let expired = Some(fixture::AT_MS - 4_200);
        let other_view = "71c0a2f4e8b3195d";
        let mut app = opened(model, "k1");
        let request = ask(&mut app, model);
        answered(&mut app, model, &request, |answers| {
            let n1 = reading(answers, 0);
            n1.local = Local::Tombstone {
                version: stamp.clone(),
            };
            n1.source = Source::Unavailable;
            let dist = n1.distributed.as_mut().unwrap();
            dist.view_moved_to = Some(other_view.to_owned());
            dist.probes = vec![
                Probe {
                    node: first.clone(),
                    answer: Answer::Unreached {
                        why: Why::Io("ConnectionRefused".to_owned()),
                    },
                },
                Probe {
                    node: second.clone(),
                    answer: Answer::StaleView {
                        responder_view: other_view.to_owned(),
                    },
                },
            ];
            let n2 = reading(answers, 1);
            n2.local = Local::Lapsed {
                version: stamp.clone(),
                expires_at_ms: expired,
                cause: Lapse::Idle,
            };
            let dist = n2.distributed.as_mut().unwrap();
            dist.residency.releasing_ms = Some(4_200);
            dist.residency.cold_marked = true;
            dist.probes = vec![
                Probe {
                    node: first.clone(),
                    answer: Answer::Miss,
                },
                Probe {
                    node: second.clone(),
                    answer: Answer::Declined,
                },
            ];
            reading(answers, 2).local = Local::Live {
                version: stamp.clone(),
                expires_at_ms: None,
                spilled: true,
            };
            reading(answers, 3).local = Local::Lapsed {
                version: stamp.clone(),
                expires_at_ms: expired,
                cause: Lapse::Expired,
            };
        });
        let odd = Odd {
            first: located.owners[0].slot.to_string(),
            second: located.owners[1].slot.to_string(),
            view: format!("{:016x}", located.view_hash),
            version: format!("1760054300112.3@{}", located.owners[0].slot),
        };
        (app, odd)
    }

    #[test]
    fn the_table_names_each_kind_of_record_and_source() {
        let model = model();
        let (app, _) = odd_answers(&model);
        let rows = shown(&app, &model, 116, 60);
        let table = |label: &str| {
            rows.iter()
                .find(|row| {
                    row.starts_with(&format!("  {label} "))
                        || row.starts_with(&format!("▌ {label} "))
                })
                .unwrap_or_else(|| panic!("no row for {label} in {rows:?}"))
        };
        assert!(table("n1").contains("tombstone"), "{}", table("n1"));
        assert!(table("n1").contains("unavailable"), "{}", table("n1"));
        assert!(table("n2").contains("lapsed idle"), "{}", table("n2"));
        assert!(
            table("n3").contains("spilled · never expires"),
            "{}",
            table("n3")
        );
        assert!(table("n4").contains("lapsed expired"), "{}", table("n4"));
    }

    #[test]
    fn the_detail_block_gives_each_record_mark_and_probe_its_words() {
        let model = model();
        let (mut app, odd) = odd_answers(&model);
        let Odd {
            first,
            second,
            view,
            version,
        } = odd;
        let detail = |app: &App, label: &str| {
            let rows = shown(app, &model, 116, 60);
            assert!(has(&rows, &format!("{label} in detail")), "{rows:?}");
            rows
        };
        let rows = detail(&app, "n1");
        assert!(find(&rows, "record  ").ends_with(&format!("tombstone {version}")));
        assert!(find(&rows, "source  ").ends_with("no owner answers, so the fetch fails"));
        assert!(
            find(&rows, "view    ").ends_with(&format!(
                "{view} · moved to 71c0a2f4e8b3195d during the call"
            )),
            "{rows:?}"
        );
        assert!(
            find(&rows, "probes  ")
                .ends_with(&format!("{first} unreached (io error (ConnectionRefused))")),
            "{rows:?}"
        );
        assert!(
            has(
                &rows,
                &format!("{second} stale view · its view is 71c0a2f4e8b3195d")
            ),
            "{rows:?}"
        );

        press(&mut app, &model, KeyCode::Down);
        let rows = detail(&app, "n2");
        assert!(
            find(&rows, "record  ")
                .ends_with(&format!("lapsed (idle) {version} · expired 4.2 s ago")),
            "{rows:?}"
        );
        assert!(
            find(&rows, "marks   ").ends_with("does not own the part · releasing 4.2 s, cold"),
            "{rows:?}"
        );
        assert!(
            find(&rows, "probes  ").ends_with(&format!("{first} miss")),
            "{rows:?}"
        );
        assert!(has(&rows, &format!("{second} declined")), "{rows:?}");

        press(&mut app, &model, KeyCode::Down);
        let rows = detail(&app, "n3");
        assert!(
            find(&rows, "record  ").ends_with(&format!(
                "live {version} · never expires · in the spill tier"
            )),
            "{rows:?}"
        );

        press(&mut app, &model, KeyCode::Down);
        let rows = detail(&app, "n4");
        assert!(
            find(&rows, "record  ")
                .ends_with(&format!("lapsed (expired) {version} · expired 4.2 s ago")),
            "{rows:?}"
        );
    }

    #[test]
    fn a_replicated_reading_has_no_owner_columns() {
        let model = model();
        let mut app = opened(&model, "k1");
        let request = ask(&mut app, &model);
        answered(&mut app, &model, &request, |answers| {
            for answer in &mut answers.nodes {
                if let Outcome::Read(reading) = &mut answer.outcome {
                    reading.distributed = None;
                    reading.mode = "replicated".to_owned();
                    reading.source = Source::Local { hit: true };
                }
            }
        });
        let rows = shown(&app, &model, 116, 40);
        let header = find(&rows, "NODE");
        assert!(
            header.contains("LOCAL") && header.contains("SOURCE"),
            "{header}"
        );
        for gone in ["OWNS", "READ", "SERVES", "PROBES"] {
            assert!(!header.contains(gone), "{gone} in {header}");
        }
        assert!(
            has(&rows, "no node gave a Distributed reading to compare"),
            "{rows:?}"
        );
        let detail = rows.join("\n");
        assert!(
            !detail.contains("marks   ") && !detail.contains("probes  "),
            "{detail}"
        );
    }

    #[test]
    fn columns_drop_in_order_at_100_and_80_columns() {
        let model = model();
        let mut app = opened(&model, "k1");
        let request = ask(&mut app, &model);
        answered(&mut app, &model, &request, |_| {});
        let columns = |width: usize| -> Vec<&'static str> {
            let rows = shown(&app, &model, width, 40);
            let header = find(&rows, "NODE").to_owned();
            [
                "NODE", "OWNS", "LOCAL", "READ", "SERVES", "SOURCE", "PROBES",
            ]
            .into_iter()
            .filter(|column| header.contains(column))
            .collect()
        };
        assert_eq!(
            columns(116),
            [
                "NODE", "OWNS", "LOCAL", "READ", "SERVES", "SOURCE", "PROBES"
            ]
        );
        // The 100-column popup: PROBES goes first.
        assert_eq!(
            columns(92),
            ["NODE", "OWNS", "LOCAL", "READ", "SERVES", "SOURCE"]
        );
        // The 80-column popup: SERVES goes next.
        assert_eq!(columns(72), ["NODE", "OWNS", "LOCAL", "READ", "SOURCE"]);
        assert_eq!(columns(60), ["NODE", "OWNS", "LOCAL", "SOURCE"]);
        // Drawn into the bodies of the three smaller screens.
        for (width, height, absent) in [(100, 27, "PROBES"), (80, 21, "SERVES")] {
            let rows = drawn(&app, &model, width, height);
            assert!(has(&rows, "NODE"), "{rows:?}");
            assert!(!has(&rows, absent), "{absent} in {rows:?}");
        }
    }

    #[test]
    fn rows_are_cut_with_a_count_and_the_detail_goes_first() {
        let model = model();
        let mut app = opened(&model, "k1");
        let request = ask(&mut app, &model);
        answered(&mut app, &model, &request, |_| {});
        let full = shown(&app, &model, 116, 40);
        assert!(has(&full, "in detail"));
        let head = full.iter().position(|row| row.contains("NODE")).unwrap();
        let with_rows = head + 1 + 5;
        // Room for the rows and the key hints but not the detail.
        let rows = shown(&app, &model, 116, with_rows + 2);
        assert!(!has(&rows, "in detail"), "{rows:?}");
        assert!(has(&rows, "n5") && !has(&rows, "more nodes"), "{rows:?}");
        assert_eq!(rows.len(), with_rows + 2);
        // Two rows fewer: the last rows give way to a count.
        let rows = shown(&app, &model, 116, with_rows);
        assert!(has(&rows, "… +3 nodes"), "{rows:?}");
        assert!(has(&rows, "n1") && has(&rows, "n2"), "{rows:?}");
        assert!(!rows.iter().any(|row| row.starts_with("  n3")), "{rows:?}");
        assert_eq!(rows.len(), with_rows);
        assert!(rows.last().unwrap().starts_with("Enter ask"));
        // Every height down to nothing yields at most that many lines with
        // the key hints last.
        for height in 0..full.len() + 3 {
            let rows = shown(&app, &model, 116, height);
            assert!(rows.len() <= height.max(1), "{height}: {}", rows.len());
            assert!(rows.last().unwrap().contains("Esc close"), "{height}");
        }
    }

    #[test]
    fn a_moved_view_dims_the_result_and_says_enter_asks_again() {
        let model = model();
        let mut app = opened(&model, "k1");
        let request = ask(&mut app, &model);
        answered(&mut app, &model, &request, |answers| {
            for answer in &mut answers.nodes {
                if let Outcome::Read(reading) = &mut answer.outcome {
                    reading.distributed.as_mut().unwrap().view = "00c1a2f4e8b3195d".to_owned();
                }
            }
        });
        let digest = model.ownership("it").unwrap();
        let lens = eventlog::view_hash(digest.view_hash);
        let rows = shown(&app, &model, 116, 40);
        assert!(
            has(
                &rows,
                &format!("view moved 00c1a2f4 -> {lens}: Enter asks again")
            ),
            "{rows:?}"
        );
        let ctx = ctx_of(&model);
        let scene = Scene {
            app: &app,
            model: &model,
            ctx: &ctx,
            look: app.look(),
            kind: LayoutKind::Full,
        };
        let state = app.explain.as_ref().unwrap();
        let drawn = lines(&scene, state, 116, 40);
        let dimmed = |line: &Line<'_>| {
            line.spans
                .iter()
                .filter(|span| !span.content.trim().is_empty())
                .all(|span| span.style.add_modifier.contains(Modifier::DIM))
        };
        let node_rows: Vec<_> = drawn
            .iter()
            .filter(|line| text_of(line).contains(" hit "))
            .collect();
        assert_eq!(node_rows.len(), 5);
        assert!(node_rows.iter().all(|line| dimmed(line)));
        let detail = drawn
            .iter()
            .find(|line| text_of(line).contains("record  "))
            .unwrap();
        assert!(dimmed(detail));
        let summary = drawn
            .iter()
            .find(|line| text_of(line).contains("Enter asks again"))
            .unwrap();
        assert!(!dimmed(summary), "the verdict itself stays bright");
    }

    #[test]
    fn a_frozen_display_is_named() {
        let model = model();
        let mut app = opened(&model, "k1");
        let rows = drawn(&app, &model, 140, 37);
        assert!(!has(&rows, "display frozen"), "{rows:?}");
        // `p` types while the overlay is open; the display freezes by the
        // model the overlay was drawn from.
        app.explain = None;
        press(&mut app, &model, KeyCode::Char('p'));
        press(&mut app, &model, KeyCode::Char('e'));
        assert!(app.is_frozen());
        let rows = drawn(&app, &model, 140, 37);
        assert!(
            has(&rows, "Explain a read · it · computed (display frozen)"),
            "{rows:?}"
        );
    }

    #[test]
    fn the_popup_is_a_rounded_box_centered_in_the_body() {
        let model = model();
        let app = opened(&model, "k1");
        let rows = drawn(&app, &model, 140, 37);
        let top = rows.iter().position(|row| row.contains('╭')).unwrap();
        let bottom = rows.iter().rposition(|row| row.contains('╯')).unwrap();
        assert!(top >= 1 && bottom <= 35, "{top} {bottom}");
        let chars: Vec<char> = rows[top].chars().collect();
        let left = chars.iter().position(|c| *c == '╭').unwrap();
        let right = chars.iter().rposition(|c| *c == '╮').unwrap();
        assert!(left.abs_diff(140 - 1 - right) <= 1, "{left} {right}");
        assert_eq!(
            right - left + 1,
            usize::from(MAX_WIDTH),
            "the popup is MAX_WIDTH wide"
        );
        // Nothing fits a body under 12 columns or 3 rows: nothing is drawn.
        for (w, h) in [(10, 30), (140, 4), (0, 0)] {
            assert!(
                drawn(&app, &model, w, h)
                    .iter()
                    .all(|row| row.trim().is_empty())
            );
        }
    }

    #[test]
    fn mono_mode_carries_the_verdict_in_words() {
        let model = model();
        let mono = Look::with_mode(&crate::cli::DisplayArgs::default(), ColorMode::Mono);
        let mut app = app_in(mono, Some(control()));
        typed(&mut app, &model, "k1");
        let request = ask(&mut app, &model);
        answered(&mut app, &model, &request, |answers| {
            answers.nodes[1].outcome = Outcome::Failed("no answer (connection refused)".to_owned());
        });
        let rendered = |app: &App| {
            let ctx = ctx_of(&model);
            let scene = Scene {
                app,
                model: &model,
                ctx: &ctx,
                look: app.look(),
                kind: LayoutKind::Full,
            };
            lines(&scene, app.explain.as_ref().unwrap(), 116, 40)
        };
        let lines_now = rendered(&app);
        let words: Vec<String> = lines_now.iter().map(text_of).collect();
        assert!(
            has(&words, "✔ 4 nodes agree, the lens computes the same"),
            "{words:?}"
        );
        assert!(has(&words, "no answer (connection refused)"), "{words:?}");
        assert!(has(&words, "owner  1 ◆"), "{words:?}");
        for line in &lines_now {
            for span in &line.spans {
                for color in [span.style.fg, span.style.bg].into_iter().flatten() {
                    assert_eq!(color, Color::Reset, "{span:?}");
                }
            }
        }
        // A split verdict reads the same without color.
        let request = ask(&mut app, &model);
        answered(&mut app, &model, &request, |answers| {
            if let Outcome::Read(reading) = &mut answers.nodes[0].outcome {
                reading.distributed.as_mut().unwrap().view = "71c0a2f4e8b3195d".to_owned();
            }
        });
        let words: Vec<String> = rendered(&app).iter().map(text_of).collect();
        assert!(
            has(&words, "↻ n1 holds 71c0a2f4, n2 n3 n4 n5 hold"),
            "{words:?}"
        );
    }

    #[test]
    fn the_age_of_the_answers_is_counted_from_the_frame_clock() {
        let model = model();
        let wall = model.wall().expect("the model has a wall clock");
        let asked_ago = |ago: Duration, later: bool| {
            let mut app = opened(&model, "k1");
            let request = ask(&mut app, &model);
            answered(&mut app, &model, &request, |answers| {
                answers.asked = if later { wall + ago } else { wall - ago };
            });
            find(&shown(&app, &model, 116, 40), "nodes agree").to_owned()
        };
        assert!(asked_ago(Duration::from_millis(3_400), false).ends_with("asked 3.4 s ago"));
        assert!(asked_ago(Duration::from_secs(250), false).ends_with("asked 4m ago"));
        assert!(asked_ago(Duration::from_secs(7_300), false).ends_with("asked 2h ago"));
        // A display frozen before the nodes were asked reads no negative age.
        assert!(asked_ago(Duration::from_secs(5), true).ends_with("asked 0.0 s ago"));
    }

    #[test]
    fn answers_with_no_placement_to_compare_with_are_listed_without_a_verdict() {
        // A cluster the lens has not ranked yet: the nodes still answer.
        let snapshot = testkit::snapshot_with_owners(3, 2);
        let model = model_of(&snapshot);
        let mut app = opened(&model, "k1");
        let rows = shown(&app, &model, 116, 40);
        assert!(
            has(
                &rows,
                "is Distributed but the lens has not ranked its parts yet"
            ),
            "{rows:?}"
        );
        let request = ask(&mut app, &model);
        let reading =
            crate::explained::parse_reply(include_str!("../../tests/fixtures/explain/owner.json"))
                .expect("the fixture parses");
        let answers = Explained {
            id: request.id,
            key: request.key.clone(),
            asked: SystemTime::UNIX_EPOCH,
            nodes: vec![NodeAnswer {
                label: "n1".into(),
                outcome: Outcome::Read(Box::new(reading)),
            }],
        };
        app.apply_director(UiCommand::Explained(Box::new(answers)), &model);
        let rows = shown(&app, &model, 116, 40);
        assert!(
            has(
                &rows,
                "· the lens has no placement of the key to compare with"
            ),
            "{rows:?}"
        );
        assert!(has(&rows, "n1 "), "the node's row stays");
        // Nodes that all failed leave a Distributed cache with nothing to
        // compare either.
        let model = self::model();
        let mut app = opened(&model, "k1");
        let request = ask(&mut app, &model);
        answered(&mut app, &model, &request, |answers| {
            for answer in &mut answers.nodes {
                answer.outcome = Outcome::Failed("no answer within 3 s".to_owned());
            }
        });
        let rows = shown(&app, &model, 116, 40);
        assert!(
            has(&rows, "· no node gave a Distributed reading to compare"),
            "{rows:?}"
        );
        assert_eq!(
            rows.iter()
                .filter(|r| r.contains("no answer within 3 s"))
                .count(),
            5 + 1
        );
    }

    #[test]
    fn an_old_result_gives_way_to_the_prompt_once_the_key_changes() {
        let model = model();
        let mut app = opened(&model, "k1");
        let request = ask(&mut app, &model);
        answered(&mut app, &model, &request, |_| {});
        assert!(has(&shown(&app, &model, 116, 40), "NODE"));
        press(&mut app, &model, KeyCode::Char('0'));
        let rows = shown(&app, &model, 116, 40);
        assert!(!has(&rows, "NODE"), "{rows:?}");
        assert!(has(&rows, "Enter asks 5 nodes about this key"), "{rows:?}");
        press(&mut app, &model, KeyCode::Backspace);
        assert!(
            has(&shown(&app, &model, 116, 40), "NODE"),
            "the same key shows its answers again"
        );
    }

    #[test]
    fn a_long_key_shows_its_end_beside_the_cursor() {
        let model = model();
        let text = format!("{}end", "x".repeat(100));
        let app = opened(&model, &text);
        let rows = shown(&app, &model, 60, 30);
        assert!(rows[0].ends_with("xend▌"), "{}", rows[0]);
        assert!(rows[0].chars().count() <= 60, "{}", rows[0]);
        assert!(
            rows[1].chars().count() <= 60 && rows[1].ends_with('…'),
            "{}",
            rows[1]
        );
    }
}

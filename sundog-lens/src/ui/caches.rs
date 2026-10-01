//! The Caches view: every cache on the left, the selected cache's detail on
//! the right. A `Distributed` cache shows each node's computed share against
//! what it reports and its rebalance rates; any other cache shows entries,
//! backlog and repair rates per node.

use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::widgets::Widget;
use sundog::store::Mode;

use super::data::{self, CacheRow, NodeRow};
use super::look::Token;
use super::panel::{self, gap};
use super::table::{self, Col};
use super::widgets::blocks::{BlockArea, bar_eighths};
use super::widgets::braille::BrailleArea;
use super::widgets::sharebar::{bar_spans, segments};
use super::widgets::spark;
use super::{LayoutKind, Scene, overview, text};
use crate::model::derive::{self, Agreement, PART_SPACE};
use crate::model::metrics::NodeMetrics;
use crate::model::ownership::OwnershipDigest;
use crate::model::series::Ring;
use crate::source::names;

/// Draws the Caches view into `area`.
pub fn render(scene: &Scene<'_>, area: Rect, buf: &mut Buffer) {
    let rows = data::cache_rows(scene.model);
    let list_width = if scene.kind == LayoutKind::Narrow {
        22
    } else {
        30
    };
    let columns =
        Layout::horizontal([Constraint::Length(list_width), Constraint::Min(30)]).split(area);
    let conflicts = rows.iter().filter(|row| row.is_conflicted()).count();
    let modes_height =
        (u16::try_from(data::live_count(scene.model) + conflicts).unwrap_or(u16::MAX) + 2)
            .clamp(4, 10);
    let list_height = (u16::try_from(rows.len()).unwrap_or(u16::MAX) + 2).max(4);
    let left = Layout::vertical([
        Constraint::Length(list_height),
        Constraint::Length(modes_height),
        Constraint::Min(0),
    ])
    .split(columns[0]);
    list(scene, &rows, left[0], buf);
    modes(scene, &rows, left[1], buf);
    let selected = scene.app.selected_cache(scene.model);
    let row = selected
        .as_deref()
        .and_then(|name| rows.iter().find(|row| row.name == name));
    let right = if scene.kind == LayoutKind::Compact && columns[1].height >= 20 {
        let split = Layout::vertical([Constraint::Length(9), Constraint::Min(8)]).split(columns[1]);
        overview::throughput(scene, split[0], buf);
        split[1]
    } else {
        columns[1]
    };
    if let Some(row) = row {
        detail(scene, row, right, buf);
    } else {
        let block = panel::block(scene.look, "Cache", "gossip", Vec::new(), false);
        let inner = panel::draw(block, right, buf);
        panel::centered(
            vec![Line::from(
                scene.look.span("no cache advertised", Token::Muted),
            )],
            inner,
            buf,
        );
    }
}

/// The status mark of a cache in the list.
fn list_status(scene: &Scene<'_>, row: &CacheRow) -> Vec<Span<'static>> {
    let look = scene.look;
    if row.is_conflicted() {
        return vec![look.span("⚠", Token::Bad)];
    }
    if row.is_distributed() {
        return match scene.model.settle(&row.name) {
            Some(verdict) if verdict.settled => vec![look.span("✔", Token::Ok)],
            Some(_) => vec![look.span("↻", Token::Move)],
            None => Vec::new(),
        };
    }
    match scene.model.divergence(&row.name) {
        Some(spread) if spread > 0.0 => {
            vec![look.span(format!("±{} ⚠", text::whole(spread)), Token::Warn)]
        }
        Some(_) => vec![look.span("±0", Token::Muted)],
        None => Vec::new(),
    }
}

fn mode_token(row: &CacheRow) -> Token {
    match row.mode {
        Some(Mode::Distributed { .. }) => Token::Accent,
        Some(Mode::Replicated) => Token::Info,
        Some(Mode::Invalidation) => Token::Move,
        None => Token::Bad,
        _ => Token::Muted,
    }
}

/// The list of caches, the selected one barred in amber.
fn list(scene: &Scene<'_>, rows: &[CacheRow], area: Rect, buf: &mut Buffer) {
    let look = scene.look;
    let block = panel::block(look, "Caches", "gossip", Vec::new(), true);
    let inner = panel::draw(block, area, buf);
    let selected = scene.app.selected_cache(scene.model);
    let focus = scene.app.ownership_cache(scene.model);
    let name_width = usize::from(inner.width).saturating_sub(14).clamp(4, 12);
    let mut lines = Vec::new();
    for row in rows {
        let is_selected = selected.as_deref() == Some(row.name.as_str());
        let mark = if focus.as_deref() == Some(row.name.as_str()) {
            "▸"
        } else {
            " "
        };
        let mode = row.mode.map_or_else(|| "?".to_owned(), data::mode_dotted);
        let mut spans = vec![
            if is_selected {
                look.span("▌", Token::Accent)
            } else {
                gap(1)
            },
            look.span(mark, Token::Accent),
            Span::styled(
                text::pad_right(&row.name, name_width),
                look.style(Token::Text).add_modifier(if is_selected {
                    Modifier::BOLD
                } else {
                    Modifier::empty()
                }),
            ),
            gap(1),
            look.span(text::pad_right(&mode, 5), mode_token(row)),
            gap(1),
        ];
        spans.extend(list_status(scene, row));
        let line = Line::from(spans);
        lines.push(if is_selected {
            line.style(look.selected())
        } else {
            line
        });
    }
    panel::lines(lines, inner, buf);
}

/// The Modes box: each live node's caches and modes. It turns red when the
/// nodes disagree, and names who disagrees.
fn modes(scene: &Scene<'_>, rows: &[CacheRow], area: Rect, buf: &mut Buffer) {
    let look = scene.look;
    let conflicts: Vec<&CacheRow> = rows.iter().filter(|row| row.is_conflicted()).collect();
    let mut block = panel::block(look, "Modes", "gossip", Vec::new(), false);
    if !conflicts.is_empty() {
        block = block.border_style(look.style(Token::Bad));
    }
    let inner = panel::padded(panel::draw(block, area, buf));
    let width = usize::from(inner.width);
    let mut lines = Vec::new();
    for node in scene.rows().iter().filter(|row| row.status().is_live()) {
        let mut spans = vec![look.node_span(text::pad_right(node.label(), 3), node.color())];
        let caches: Vec<String> = ordered_caches(node)
            .iter()
            .map(|(name, mode)| format!("{name} {}", data::mode_short(*mode)))
            .collect();
        let room = width.saturating_sub(3);
        spans.push(look.span(text::fit(&caches.join(" "), room), Token::Muted));
        lines.push(Line::from(spans));
    }
    for conflict in conflicts {
        let mut spans = vec![look.span(format!("⚠ {} ", conflict.name), Token::Bad)];
        for (node, mode) in &conflict.modes {
            let tag = data::tag_of(scene.model, *node);
            spans.push(look.node_span(tag.label.to_string(), tag.color));
            spans.push(look.span(format!(" {} ", data::mode_short(*mode)), Token::Muted));
        }
        lines.push(Line::from(spans));
    }
    panel::lines(lines, inner, buf);
}

/// A node's caches, `Distributed` first and then by name.
fn ordered_caches(node: &NodeRow<'_>) -> Vec<(String, Mode)> {
    let mut caches: Vec<(String, Mode)> = node
        .member
        .caches
        .iter()
        .map(|(name, &mode)| (name.to_string(), mode))
        .collect();
    caches.sort_by_key(|(_, mode)| !matches!(mode, Mode::Distributed { .. }));
    caches
}

/// A number cell: the formatted value, or a faint dash.
fn number(
    scene: &Scene<'_>,
    value: Option<f64>,
    width: usize,
    format: impl Fn(f64) -> String,
) -> Vec<Span<'static>> {
    let look = scene.look;
    match value {
        Some(value) => vec![look.span(text::pad_left(&format(value), width), Token::Text)],
        None => vec![look.span(text::pad_left("—", width), Token::Faint)],
    }
}

/// The advertisers of `cache` as node rows, in slot order.
fn advertisers<'a>(scene: &'a Scene<'_>, cache: &CacheRow) -> Vec<NodeRow<'a>> {
    scene
        .rows()
        .into_iter()
        .filter(|row| cache.advertisers.contains(&row.member.peer.node) && row.status().is_live())
        .collect()
}

/// A subheading with a rule to the right.
fn rule(scene: &Scene<'_>, title: &str, width: usize) -> Line<'static> {
    let look = scene.look;
    let head = format!(" {title} ");
    let fill = width.saturating_sub(head.chars().count() + 2);
    Line::from(vec![
        look.span("──", Token::Faint),
        look.span(head, Token::Muted),
        look.span("─".repeat(fill), Token::Faint),
    ])
}

/// The title spans of the detail panel in `width` cells: the cache, its mode,
/// who advertises it and where the numbers come from, the last two dropped
/// when they do not fit.
fn detail_title(
    scene: &Scene<'_>,
    row: &CacheRow,
    nodes: &[NodeRow<'_>],
    width: usize,
) -> Vec<Span<'static>> {
    let look = scene.look;
    let mode = row
        .mode
        .map_or_else(|| "mode conflict".to_owned(), data::mode_name);
    let head = vec![
        gap(1),
        Span::styled(
            row.name.to_string(),
            look.style(Token::Accent).add_modifier(Modifier::BOLD),
        ),
        look.span(format!(" · {mode}"), Token::Muted),
    ];
    let mut advertised = vec![look.span(" · advertised by", Token::Muted)];
    for node in nodes.iter().take(6) {
        advertised.push(gap(1));
        advertised.push(look.node_span(node.label().to_owned(), node.color()));
    }
    if nodes.len() > 6 {
        advertised.push(look.span(format!(" +{}", nodes.len() - 6), Token::Muted));
    }
    let tag = if row.is_distributed() {
        "computed+metrics"
    } else {
        "gossip+metrics"
    };
    let mut full = head.clone();
    full.extend(advertised.clone());
    full.extend(panel::tag_spans(look, tag));
    full.push(gap(1));
    let mut with_advertisers = head.clone();
    with_advertisers.extend(advertised);
    with_advertisers.push(gap(1));
    let mut plain = head;
    plain.push(gap(1));
    // Room inside the corners.
    let room = width.saturating_sub(2);
    [full, with_advertisers]
        .into_iter()
        .find(|spans| panel::width_of(spans) <= room)
        .unwrap_or(plain)
}

/// Draws the detail panel of `row`. The panel ends where its content ends:
/// the table sections, then the history charts in the rows that remain.
fn detail(scene: &Scene<'_>, row: &CacheRow, area: Rect, buf: &mut Buffer) {
    let look = scene.look;
    let nodes = advertisers(scene, row);
    let block = panel::block_with(
        look,
        detail_title(scene, row, &nodes, usize::from(area.width)),
        Vec::new(),
        false,
    );
    let block = if row.is_conflicted() {
        block.border_style(look.style(Token::Bad))
    } else {
        block
    };
    let frame = block.inner(area);
    if frame.width < 20 || frame.height < 3 {
        panel::draw(block, area, buf);
        return;
    }
    let content = Rect::new(frame.x + 1, frame.y, frame.width - 2, frame.height);
    let mut lines = match row.mode {
        Some(Mode::Distributed { .. }) => scene.model.ownership(&row.name).map_or_else(
            || vec![Line::from(look.span("computing ownership", Token::Muted))],
            |digest| distributed_lines(scene, row, digest, &nodes, content),
        ),
        Some(_) => replicated_lines(scene, row, &nodes, content),
        None => conflict_lines(scene, row),
    };
    if data::throughput(scene.model).nodes == 0 && !row.is_conflicted() {
        lines.push(Line::default());
        lines.push(Line::from(look.span(
            "no exporter mapped · pass --metrics 'http://{ip}:9090/metrics'",
            Token::Faint,
        )));
    }
    let used = u16::try_from(lines.len()).unwrap_or(u16::MAX);
    let history = (!row.is_conflicted())
        .then(|| History::plan(scene, row, &nodes, content.height.saturating_sub(used + 1)))
        .flatten();
    let rows_used = used + history.as_ref().map_or(0, |plan| plan.rows() + 1);
    let height = (rows_used + 2).min(area.height);
    let area = Rect::new(area.x, area.y, area.width, height);
    panel::draw(block, area, buf);
    let content = Rect::new(
        content.x,
        content.y,
        content.width,
        height.saturating_sub(2),
    );
    panel::lines(lines, content, buf);
    if let Some(plan) = history {
        // One blank row separates the tables from the charts.
        let top = content.y + used + 1;
        let section = Rect::new(content.x, top, content.width, plan.rows());
        plan.draw(scene, row, &nodes, section, buf);
    }
}

/// One chart column of the history section: a title and one series per node.
#[derive(Debug, Clone)]
struct HistoryCol {
    title: &'static str,
    series: Vec<Vec<f64>>,
    max: f64,
    unit: HistoryUnit,
}

/// How the latest sample of a history column reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HistoryUnit {
    Count,
    Percent,
}

/// The history charts under a cache's tables: for each node, entries and hit
/// percentage, and the fan-out backlog of a cache that keeps one.
#[derive(Debug, Clone)]
struct History {
    cols: Vec<HistoryCol>,
    chart_rows: u16,
    nodes: usize,
}

/// The most rows a node's chart takes.
const HISTORY_CHART_ROWS: u16 = 4;

impl History {
    /// The history that fits in `rows` rows under the tables, or `None` when
    /// no node has history yet or the rows cannot hold a title and a chart
    /// row for every node.
    fn plan(scene: &Scene<'_>, row: &CacheRow, nodes: &[NodeRow<'_>], rows: u16) -> Option<Self> {
        let metrics: Vec<Option<&NodeMetrics>> = nodes
            .iter()
            .map(|node| data::metrics_of(scene.model, node))
            .collect();
        let series = |pick: fn(&NodeMetrics, &str) -> Option<Vec<f64>>| -> Vec<Vec<f64>> {
            metrics
                .iter()
                .map(|m| m.and_then(|m| pick(m, &row.name)).unwrap_or_default())
                .collect()
        };
        let largest = |series: &[Vec<f64>]| {
            series
                .iter()
                .flatten()
                .copied()
                .filter(|v| v.is_finite())
                .fold(1.0, f64::max)
        };
        let entries = series(|m, cache| m.entries_history(cache).map(Ring::to_vec));
        let hit = series(|m, cache| m.hit_history(cache).map(Ring::to_vec));
        // Only a cache that fans writes out has a backlog worth charting.
        let backlog = if row.is_distributed() {
            vec![Vec::new(); nodes.len()]
        } else {
            series(|m, cache| m.backlog_history(cache).map(Ring::to_vec))
        };
        let mut cols = Vec::new();
        for (title, series, max, unit) in [
            (
                "entries",
                entries.clone(),
                largest(&entries),
                HistoryUnit::Count,
            ),
            ("hit", hit, 100.0, HistoryUnit::Percent),
            (
                "backlog",
                backlog.clone(),
                largest(&backlog),
                HistoryUnit::Count,
            ),
        ] {
            if series.iter().any(|s| !s.is_empty()) {
                cols.push(HistoryCol {
                    title,
                    series,
                    max,
                    unit,
                });
            }
        }
        let count = u16::try_from(nodes.len()).ok().filter(|count| *count > 0)?;
        // The rule takes a row; each node takes a title row and its chart.
        let per_node = rows.checked_sub(1)? / count;
        let chart_rows = per_node.checked_sub(1)?.min(HISTORY_CHART_ROWS);
        (!cols.is_empty() && chart_rows >= 1).then_some(Self {
            cols,
            chart_rows,
            nodes: nodes.len(),
        })
    }

    /// The rows the section takes, the rule included.
    fn rows(&self) -> u16 {
        1 + u16::try_from(self.nodes).unwrap_or(u16::MAX) * (self.chart_rows + 1)
    }

    fn draw(
        &self,
        scene: &Scene<'_>,
        row: &CacheRow,
        nodes: &[NodeRow<'_>],
        area: Rect,
        buf: &mut Buffer,
    ) {
        let look = scene.look;
        let title = if row.is_distributed() {
            "history · metrics · entries and hit %"
        } else {
            "history · metrics · entries, hit % and backlog"
        };
        panel::lines(
            vec![rule(scene, title, usize::from(area.width))],
            Rect::new(area.x, area.y, area.width, 1),
            buf,
        );
        let label = 5;
        let columns = u16::try_from(self.cols.len()).unwrap_or(1);
        let gaps = 2 * columns.saturating_sub(1);
        let col_width = area.width.saturating_sub(label + gaps) / columns;
        if col_width < 6 {
            return;
        }
        for (index, node) in nodes.iter().enumerate() {
            let top = area.y + 1 + u16::try_from(index).unwrap_or(0) * (self.chart_rows + 1);
            panel::lines(
                vec![Line::from(
                    look.node_span(node.label().to_owned(), node.color()),
                )],
                Rect::new(area.x, top, label, 1),
                buf,
            );
            for (position, col) in self.cols.iter().enumerate() {
                let x = area.x + label + u16::try_from(position).unwrap_or(0) * (col_width + 2);
                let samples = &col.series[index];
                // The title sits at the left and the latest value at the
                // right, above the newest end of the chart.
                let latest = samples
                    .last()
                    .map_or_else(|| "—".to_owned(), |value| col.unit.format(*value));
                let room = usize::from(col_width)
                    .saturating_sub(col.title.chars().count() + latest.chars().count());
                panel::lines(
                    vec![Line::from(vec![
                        look.span(col.title, Token::Muted),
                        gap(room.max(1)),
                        look.span(
                            latest,
                            if samples.is_empty() {
                                Token::Faint
                            } else {
                                Token::Text
                            },
                        ),
                    ])],
                    Rect::new(x, top, col_width, 1),
                    buf,
                );
                let chart = Rect::new(x, top + 1, col_width, self.chart_rows);
                if look.braille {
                    Widget::render(
                        BrailleArea::new(samples, col.max * 1.1, look.mode).tinted(node.color()),
                        chart,
                        buf,
                    );
                } else {
                    Widget::render(
                        BlockArea::new(samples, col.max * 1.1, look.mode).tinted(node.color()),
                        chart,
                        buf,
                    );
                }
            }
        }
    }
}

impl HistoryUnit {
    fn format(self, value: f64) -> String {
        match self {
            Self::Count => text::count(value),
            Self::Percent => format!("{}%", text::whole(value.round())),
        }
    }
}

fn conflict_lines(scene: &Scene<'_>, row: &CacheRow) -> Vec<Line<'static>> {
    let look = scene.look;
    let mut lines = vec![Line::from(look.span(
        "the live nodes advertise this cache under different modes:",
        Token::Bad,
    ))];
    for (node, mode) in &row.modes {
        let tag = data::tag_of(scene.model, *node);
        lines.push(Line::from(vec![
            gap(2),
            look.node_span(tag.label.to_string(), tag.color),
            gap(1),
            look.span(tag.short, Token::Muted),
            gap(2),
            look.span(data::mode_name(*mode), Token::Text),
        ]));
    }
    lines
}

/// The reported-parts cell: the number and whether it matches the computed
/// one.
fn reported_cell(scene: &Scene<'_>, reported: Option<f64>, computed: usize) -> Vec<Span<'static>> {
    let look = scene.look;
    match derive::agreement(reported, computed) {
        Agreement::Unknown => vec![look.span(text::pad_left("—", 7), Token::Faint)],
        Agreement::Match => vec![
            look.span(
                text::pad_left(&text::whole(reported.unwrap_or(0.0)), 7),
                Token::Text,
            ),
            look.span(" ✓", Token::Ok),
        ],
        Agreement::Differs => vec![
            look.span(
                text::pad_left(&text::whole(reported.unwrap_or(0.0)), 7),
                Token::Text,
            ),
            look.span(" ↻", Token::Move),
        ],
    }
}

fn last_of(
    ring: Option<&crate::model::series::Ring<{ crate::model::series::RING_LEN }>>,
) -> Option<f64> {
    ring.and_then(crate::model::series::Ring::last)
}

fn distributed_columns() -> Vec<Col> {
    vec![
        Col::new("NODE", 6, 9),
        Col::new("OWNED", 12, 8),
        Col::right("SHARE", 8, 7),
        Col::right("REPORTED", 10, 8),
        Col::right("IN/s", 7, 4),
        Col::right("OUT/s", 7, 4),
        Col::right("ENTRIES", 9, 5),
        Col::right("HIT%", 6, 3),
        Col::new("FETCH l/r/m/e", 14, 2),
        Col::right("FWD/s", 7, 2),
        Col::right("STALE/s", 9, 1),
    ]
}

fn node_cell(scene: &Scene<'_>, node: &NodeRow<'_>) -> Vec<Span<'static>> {
    vec![
        scene
            .look
            .node_span(text::pad_right(node.label(), 5), node.color()),
    ]
}

/// The coverage bar and the share strip that head a `Distributed` detail.
fn coverage_lines(
    scene: &Scene<'_>,
    row: &CacheRow,
    digest: &OwnershipDigest,
    width: usize,
) -> Vec<Line<'static>> {
    let look = scene.look;
    let total: usize = digest.counts.iter().map(|&(_, count)| count).sum();
    let owners = usize::from(digest.k.get()).min(digest.eligible.len().max(1));
    let bar_width = width.saturating_sub(54).clamp(10, 60);
    let mut coverage = vec![look.span("coverage  ", Token::Muted)];
    match scene.model.coverage(&row.name) {
        Some(fraction) => {
            coverage.extend(bar_spans(
                fraction,
                None,
                bar_width,
                crate::ui::theme::OK,
                look,
            ));
            coverage.push(look.span(format!("  {}", text::percent(fraction, 1)), Token::Text));
        }
        None => coverage.push(look.span("no node reports", Token::Faint)),
    }
    coverage.push(look.span(
        format!(
            "   Σ owned {} / {owners} × {}",
            text::thousands(u64::try_from(total).unwrap_or(0)),
            text::thousands(u64::try_from(PART_SPACE).unwrap_or(0)),
        ),
        Token::Muted,
    ));
    let counts: Vec<u64> = digest
        .counts
        .iter()
        .map(|&(_, count)| u64::try_from(count).unwrap_or(0))
        .collect();
    let mut strip = vec![look.span("share     ", Token::Muted)];
    for (index, cells) in segments(&counts, bar_width).into_iter().enumerate() {
        let tag = data::tag_of(scene.model, digest.eligible[index]);
        strip.push(Span::styled(
            "█".repeat(usize::from(cells)),
            look.node(tag.color),
        ));
    }
    vec![Line::from(coverage), Line::from(strip)]
}

/// The lines of a `Distributed` cache's detail.
fn distributed_lines(
    scene: &Scene<'_>,
    row: &CacheRow,
    digest: &OwnershipDigest,
    nodes: &[NodeRow<'_>],
    inner: Rect,
) -> Vec<Line<'static>> {
    let width = usize::from(inner.width);
    let mut lines = coverage_lines(scene, row, digest, width);
    lines.push(Line::default());
    lines.push(rule(scene, "per node · metrics", width));
    lines.extend(node_table(scene, row, digest, nodes, width));
    lines.push(Line::default());
    lines.push(rule(scene, "rebalance · metrics · parts/s", width));
    lines.extend(rebalance_lines(scene, row, nodes, width));
    lines.push(Line::default());
    lines.push(distributed_footer(scene, row, digest, nodes));
    lines
}

/// The per-node table of a `Distributed` cache.
fn node_table(
    scene: &Scene<'_>,
    row: &CacheRow,
    digest: &OwnershipDigest,
    nodes: &[NodeRow<'_>],
    width: usize,
) -> Vec<Line<'static>> {
    let look = scene.look;
    let mut lines = Vec::new();
    let cols = distributed_columns();
    let visible = table::visible(&cols, width);
    lines.push(table::header(look, &cols, &visible));
    let fair = derive::fair_share(digest.k, digest.eligible.len());
    for node in nodes {
        let id = node.member.peer.node;
        let computed = digest.parts_owned_by(id);
        let metrics = data::metrics_of(scene.model, node);
        let share = derive::share_fraction(computed);
        let animated = scene.app.share(&digest.cache, id, share);
        let mut owned = bar_spans(animated, Some(fair), 10, node.color(), look);
        owned.push(gap(1));
        let cells = vec![
            node_cell(scene, node),
            owned,
            vec![look.span(text::pad_left(&text::percent(share, 1), 7), Token::Text)],
            reported_cell(
                scene,
                metrics.and_then(|m| m.owned_parts(&row.name)),
                computed,
            ),
            number(
                scene,
                metrics.and_then(|m| last_of(m.rebalance_in(&row.name))),
                6,
                text::count,
            ),
            number(
                scene,
                metrics.and_then(|m| last_of(m.rebalance_out(&row.name))),
                6,
                text::count,
            ),
            number(
                scene,
                metrics.and_then(|m| m.entries(&row.name)),
                8,
                text::whole,
            ),
            number(
                scene,
                metrics.and_then(|m| m.hit_ratio(&row.name)),
                5,
                |v| text::percent(v, 0),
            ),
            fetch_cell(scene, metrics, &row.name),
            number(
                scene,
                metrics.and_then(|m| m.rate(names::FORWARDED_WRITES, &[("cache", &row.name)])),
                6,
                text::count,
            ),
            number(
                scene,
                metrics.and_then(|m| m.rate(names::STALE_VIEW, &[("cache", &row.name)])),
                8,
                text::count,
            ),
        ];
        lines.push(table::row(&cols, &visible, cells));
    }
    lines
}

/// One line per node of in and out sparklines.
fn rebalance_lines(
    scene: &Scene<'_>,
    row: &CacheRow,
    nodes: &[NodeRow<'_>],
    width: usize,
) -> Vec<Line<'static>> {
    let look = scene.look;
    let spark_width = width.saturating_sub(24).clamp(8, 40) / 2;
    nodes
        .iter()
        .map(|node| {
            let metrics = data::metrics_of(scene.model, node);
            let series =
                |ring: Option<&crate::model::series::Ring<{ crate::model::series::RING_LEN }>>| {
                    ring.map(crate::model::series::Ring::to_vec)
                        .unwrap_or_default()
                };
            let into = series(metrics.and_then(|m| m.rebalance_in(&row.name)));
            let out = series(metrics.and_then(|m| m.rebalance_out(&row.name)));
            Line::from(vec![
                look.node_span(text::pad_right(node.label(), 4), node.color()),
                look.span("▲ ", Token::Move),
                look.node_span(spark(&into, spark_width, look.braille), node.color()),
                look.span("  ▼ ", Token::Move),
                look.node_span(spark(&out, spark_width, look.braille), node.color()),
            ])
        })
        .collect()
}

fn fetch_cell(scene: &Scene<'_>, metrics: Option<&NodeMetrics>, cache: &str) -> Vec<Span<'static>> {
    let look = scene.look;
    match metrics.and_then(|m| m.fetch_mix(cache)) {
        Some(mix) => vec![look.span(
            format!(
                "{}/{}/{}/{}",
                text::to_u64(mix.local * 100.0),
                text::to_u64(mix.remote * 100.0),
                text::to_u64(mix.miss * 100.0),
                text::to_u64(mix.error * 100.0)
            ),
            Token::Text,
        )],
        None => vec![look.span("—", Token::Faint)],
    }
}

fn distributed_footer(
    scene: &Scene<'_>,
    row: &CacheRow,
    digest: &OwnershipDigest,
    nodes: &[NodeRow<'_>],
) -> Line<'static> {
    let look = scene.look;
    let metrics: Vec<&NodeMetrics> = nodes
        .iter()
        .filter_map(|node| data::metrics_of(scene.model, node))
        .collect();
    let timeouts: f64 = metrics
        .iter()
        .filter_map(|m| m.value(names::REBALANCE_PULL_TIMEOUTS, &[("cache", &row.name)]))
        .sum();
    let transfer: f64 = metrics
        .iter()
        .filter_map(|m| m.rate_sum_where(names::STATE_TRANSFER_RECORDS, &[("cache", &row.name)]))
        .sum();
    let mut spans = vec![
        look.span("pull timeouts ", Token::Muted),
        look.span(text::whole(timeouts), Token::Text),
        look.span(" · state transfer ", Token::Muted),
        look.span(format!("{}/s", text::count(transfer)), Token::Text),
    ];
    if let Some(keys) = data::key_estimate(scene.model, row) {
        spans.push(look.span(" · Σentries/k ≈ ", Token::Muted));
        spans.push(look.span(format!("{} unique keys", text::count(keys)), Token::Text));
    }
    spans.push(look.span(
        format!(
            " · view {} · {} view",
            super::eventlog::view_hash(digest.view_hash),
            if digest.ranks_parts { "part" } else { "bucket" }
        ),
        Token::Muted,
    ));
    Line::from(spans)
}

/// The BACKLOG cell: a spark of the fan-out backlog in the node's color and
/// the frames waiting now.
fn backlog_cell(
    scene: &Scene<'_>,
    node: &NodeRow<'_>,
    metrics: Option<&NodeMetrics>,
    cache: &str,
) -> Vec<Span<'static>> {
    let look = scene.look;
    let Some(now) = metrics.and_then(|m| m.cache_value(names::FAN_OUT_BACKLOG, cache)) else {
        return vec![look.span("—", Token::Faint)];
    };
    let history = metrics
        .and_then(|m| m.backlog_history(cache))
        .map(crate::model::series::Ring::to_vec)
        .unwrap_or_default();
    vec![
        look.node_span(spark(&history, 6, look.braille), node.color()),
        gap(1),
        look.span(text::pad_left(&text::whole(now), 5), Token::Text),
    ]
}

fn replicated_columns() -> Vec<Col> {
    vec![
        Col::new("NODE", 6, 9),
        Col::new("ENTRIES", 26, 8),
        Col::new("BACKLOG", 13, 6),
        Col::right("AE rep/s", 10, 5),
        Col::right("XFER/s", 8, 4),
        Col::right("HIT%", 6, 3),
        Col::right("FWD/s", 7, 2),
    ]
}

/// The lines of a `Replicated`, `Invalidation` or `Local` cache's detail.
fn replicated_lines(
    scene: &Scene<'_>,
    row: &CacheRow,
    nodes: &[NodeRow<'_>],
    inner: Rect,
) -> Vec<Line<'static>> {
    let look = scene.look;
    let width = usize::from(inner.width);
    let mut lines = Vec::new();
    let entries: Vec<Option<f64>> = nodes
        .iter()
        .map(|node| data::metrics_of(scene.model, node).and_then(|m| m.entries(&row.name)))
        .collect();
    let max = entries.iter().flatten().copied().fold(0.0, f64::max);
    let mut head = vec![look.span("divergence  ", Token::Muted)];
    match scene.model.divergence(&row.name) {
        Some(spread) if spread > 0.0 => {
            head.push(look.span(format!("±{} ", text::whole(spread)), Token::Warn));
            head.push(look.span("⚠", Token::Warn));
            head.push(look.span("  largest minus smallest entry count", Token::Muted));
        }
        Some(_) => {
            head.push(look.span("±0", Token::Ok));
            head.push(look.span("  every node holds the same count", Token::Muted));
        }
        None => head.push(look.span("needs two reporting nodes", Token::Faint)),
    }
    lines.push(Line::from(head));
    lines.push(Line::default());
    lines.push(rule(scene, "per node · metrics", width));
    let cols = replicated_columns();
    let visible = table::visible(&cols, width);
    lines.push(table::header(look, &cols, &visible));
    for (node, entry) in nodes.iter().zip(&entries) {
        let metrics = data::metrics_of(scene.model, node);
        let entries_cell = match entry {
            Some(value) => vec![
                look.node_span(
                    text::pad_right(
                        &bar_eighths(if max > 0.0 { value / max } else { 0.0 }, 14),
                        14,
                    ),
                    node.color(),
                ),
                look.span(text::pad_left(&text::whole(*value), 8), Token::Text),
            ],
            None => vec![look.span(text::pad_left("—", 22), Token::Faint)],
        };
        let cells = vec![
            node_cell(scene, node),
            entries_cell,
            backlog_cell(scene, node, metrics, &row.name),
            number(
                scene,
                metrics.and_then(|m| m.rate(names::AE_REPAIRED, &[("cache", &row.name)])),
                9,
                text::count,
            ),
            number(
                scene,
                metrics
                    .and_then(|m| m.rate(names::STATE_TRANSFER_RECORDS, &[("cache", &row.name)])),
                7,
                text::count,
            ),
            number(
                scene,
                metrics.and_then(|m| m.hit_ratio(&row.name)),
                5,
                |v| text::percent(v, 0),
            ),
            number(
                scene,
                metrics.and_then(|m| m.rate(names::FORWARDED_WRITES, &[("cache", &row.name)])),
                6,
                text::count,
            ),
        ];
        lines.push(table::row(&cols, &visible, cells));
    }
    lines
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;
    use crate::app::{App, AppConfig, UiCommand};
    use crate::model::Model;
    use crate::model::testkit;
    use crate::ui::Ctx;
    use crate::ui::panel::row_text;

    fn draw_with(app: &App, model: &Model, kind: LayoutKind, w: u16, h: u16) -> Vec<String> {
        let ctx = Ctx {
            now: model.now().unwrap(),
            wall: model.wall().unwrap(),
            elapsed: Duration::ZERO,
        };
        let scene = Scene {
            app,
            model,
            ctx: &ctx,
            look: app.look(),
            kind,
        };
        let area = Rect::new(0, 0, w, h);
        let mut buf = Buffer::empty(area);
        render(&scene, area, &mut buf);
        (0..h).map(|y| row_text(&buf, y)).collect()
    }

    fn draw(model: &Model, kind: LayoutKind, w: u16, h: u16) -> Vec<String> {
        let mut app = App::new(AppConfig::default());
        app.observe(model, Instant::now());
        app.snap();
        draw_with(&app, model, kind, w, h)
    }

    fn fixture() -> Model {
        testkit::fixture_model(Instant::now())
    }

    #[test]
    fn the_list_names_every_cache_with_its_mode_and_status() {
        let rows = draw(&fixture(), LayoutKind::Full, 140, 37);
        let text = rows.join("\n");
        assert!(rows[0].starts_with("╭ Caches · gossip"), "{}", rows[0]);
        assert!(text.contains("▌▸it"), "{text}");
        assert!(text.contains("D·2"), "{text}");
        assert!(text.contains("churn"), "{text}");
        assert!(text.contains("╭ Modes · gossip"), "{text}");
    }

    #[test]
    fn the_distributed_detail_shows_coverage_shares_and_the_per_node_table() {
        let rows = draw(&fixture(), LayoutKind::Full, 140, 37);
        let text = rows.join("\n");
        assert!(
            text.contains("it · distributed k=2 · advertised by n1 n2 n3 n4 n5 n6"),
            "{text}"
        );
        assert!(text.contains("computed+metrics"), "{text}");
        assert!(text.contains("coverage  no node reports"), "{text}");
        assert!(text.contains("Σ owned 131,072 / 2 × 65,536"), "{text}");
        assert!(text.contains("── per node · metrics"), "{text}");
        for title in [
            "NODE",
            "OWNED",
            "SHARE",
            "REPORTED",
            "IN/s",
            "ENTRIES",
            "HIT%",
            "FETCH l/r/m/e",
            "STALE/s",
        ] {
            assert!(text.contains(title), "{title} in\n{text}");
        }
        assert!(text.contains("── rebalance · metrics · parts/s"), "{text}");
        assert!(
            text.contains("pull timeouts 0 · state transfer 0/s"),
            "{text}"
        );
        assert!(text.contains("part view"), "{text}");
        assert!(text.contains("no exporter mapped"), "{text}");
    }

    #[test]
    fn a_replicated_cache_shows_entries_divergence_and_repairs() {
        let model = fixture();
        let mut app = App::new(AppConfig::default());
        app.apply_director(UiCommand::Cache("churn".into()), &model);
        app.observe(&model, Instant::now());
        let rows = draw_with(&app, &model, LayoutKind::Full, 140, 37);
        let text = rows.join("\n");
        assert!(
            text.contains("churn · replicated · advertised by"),
            "{text}"
        );
        assert!(text.contains("gossip+metrics"), "{text}");
        assert!(
            text.contains("divergence  needs two reporting nodes"),
            "{text}"
        );
        for title in ["ENTRIES", "BACKLOG", "AE rep/s", "XFER/s"] {
            assert!(text.contains(title), "{title} in\n{text}");
        }
        assert!(!text.contains("coverage"), "{text}");
    }

    fn draw_cache(model: &Model, cache: &str, kind: LayoutKind, w: u16, h: u16) -> Vec<String> {
        let mut app = App::new(AppConfig::default());
        app.apply_director(UiCommand::Cache(cache.into()), model);
        app.observe(model, Instant::now());
        app.snap();
        draw_with(&app, model, kind, w, h)
    }

    fn live() -> Model {
        testkit::fixture_model_with_metrics(Instant::now())
    }

    /// The index of the last row that has any text.
    fn last_row(rows: &[String]) -> usize {
        rows.iter().rposition(|row| !row.is_empty()).unwrap()
    }

    #[test]
    fn the_list_and_the_detail_end_where_their_content_ends() {
        let rows = draw(&fixture(), LayoutKind::Full, 140, 37);
        // The list holds four caches and its border: six rows, not the column.
        assert!(rows[5].starts_with("╰"), "{}", rows[5]);
        assert!(rows[6].starts_with("╭ Modes"), "{}", rows[6]);
        // Without metrics the detail has no history, so it ends with its
        // text: a short panel, not the whole screen.
        let end = last_row(&rows);
        assert!(end < 27, "the view ends at row {end}:\n{}", rows.join("\n"));
        assert!(rows[end].contains('╰'), "{}", rows[end]);
        assert!(!rows.join("\n").contains("history · metrics"));
    }

    #[test]
    fn with_metrics_the_detail_charts_each_nodes_history_in_the_rows_that_remain() {
        let model = live();
        let rows = draw(&model, LayoutKind::Full, 140, 37);
        let text = rows.join("\n");
        assert!(
            text.contains("── history · metrics · entries and hit %"),
            "{text}"
        );
        assert!(
            !text.contains("history · metrics · entries, hit %"),
            "{text}"
        );
        for node in ["n1", "n2", "n3", "n4", "n5", "n6"] {
            let title = rows
                .iter()
                .find(|r| r.contains(&format!("│ {node}   entries")))
                .unwrap_or_else(|| panic!("a {node} history row in\n{text}"));
            assert!(title.contains("hit "), "{title}");
            assert!(
                !title.contains("backlog"),
                "a Distributed cache has none: {title}"
            );
        }
        // The charts are braille below the titles and the panel runs on to
        // the rows that hold them.
        let first = rows
            .iter()
            .position(|r| r.contains("│ n1   entries"))
            .unwrap();
        assert!(
            rows[first + 1]
                .chars()
                .any(|c| ('\u{2801}'..='\u{28FF}').contains(&c))
        );
        let end = last_row(&rows);
        assert!(end >= 30 && rows[end].contains('╰'), "{end}:\n{text}");
    }

    #[test]
    fn a_replicated_cache_charts_the_backlog_and_shows_it_as_a_spark_in_the_table() {
        let model = live();
        let rows = draw_cache(&model, "churn", LayoutKind::Full, 140, 37);
        let text = rows.join("\n");
        assert!(
            text.contains("── history · metrics · entries, hit % and backlog"),
            "{text}"
        );
        let title = rows.iter().find(|r| r.contains("│ n1   entries")).unwrap();
        assert!(
            title.contains("hit ") && title.contains("backlog"),
            "{title}"
        );
        let table = rows.iter().position(|r| r.contains("BACKLOG")).unwrap();
        let n1 = &rows[table + 1];
        assert!(
            n1.chars().any(|c| ('\u{2800}'..='\u{28FF}').contains(&c)),
            "the backlog spark: {n1}"
        );
    }

    #[test]
    fn a_short_panel_drops_the_history_rather_than_squeeze_it() {
        let model = live();
        let rows = draw(&model, LayoutKind::Full, 140, 24);
        assert!(!rows.join("\n").contains("history · metrics"));
        let rows = draw(&model, LayoutKind::Full, 140, 30);
        assert!(!rows.join("\n").contains("history · metrics"));
    }

    #[test]
    fn the_tables_keep_two_cells_between_a_right_aligned_column_and_the_next() {
        let rows = draw(&live(), LayoutKind::Full, 140, 37);
        let header = rows.iter().find(|r| r.contains("REPORTED")).unwrap();
        assert!(header.contains("HIT%  FETCH l/r/m/e"), "{header}");
        let n1 = rows.iter().find(|r| r.contains("│ n1    ━")).unwrap();
        assert!(n1.contains("%  64/29/"), "{n1}");
        let rows = draw_cache(&live(), "churn", LayoutKind::Full, 140, 37);
        let header = rows.iter().find(|r| r.contains("BACKLOG")).unwrap();
        assert!(header.contains("AE rep/s"), "{header}");
    }

    #[test]
    fn the_modes_box_lists_each_live_node_and_distributed_caches_first() {
        let text = draw(&fixture(), LayoutKind::Full, 140, 37).join("\n");
        assert!(text.contains("│ n1 it D2 churn R os R pn R"), "{text}");
        assert!(text.contains("pn R │"), "padded on both sides: {text}");
    }

    #[test]
    fn a_mode_conflict_turns_the_box_red_and_names_the_nodes() {
        let mut model = Model::new();
        let now = Instant::now();
        let snapshot = sundog::observe::ClusterSnapshot::new(
            "c",
            vec![
                testkit::member_with(
                    1,
                    0,
                    1,
                    sundog::observe::MemberStatus::Live,
                    &[("x", Mode::Replicated)],
                ),
                testkit::member_with(
                    2,
                    0,
                    1,
                    sundog::observe::MemberStatus::Live,
                    &[("x", testkit::distributed(2))],
                ),
            ],
            0,
        );
        model.apply(
            crate::source::Update::Snapshot(std::sync::Arc::new(snapshot), now),
            now,
            std::time::SystemTime::UNIX_EPOCH,
        );
        let rows = draw(&model, LayoutKind::Full, 140, 37);
        let text = rows.join("\n");
        assert!(text.contains("⚠ x n1 R n2 D2"), "{text}");
        assert!(
            text.contains("the live nodes advertise this cache under different modes"),
            "{text}"
        );
        assert!(text.contains("mode conflict"), "{text}");
    }

    #[test]
    fn the_compact_view_adds_throughput_above_the_detail() {
        let text = draw(&fixture(), LayoutKind::Compact, 120, 33).join("\n");
        assert!(text.contains("Throughput · metrics"), "{text}");
        assert!(text.contains("it · distributed"), "{text}");
    }

    #[test]
    fn a_narrow_view_keeps_the_list_and_trims_the_table() {
        let text = draw(&fixture(), LayoutKind::Narrow, 80, 21).join("\n");
        assert!(text.contains("Caches · gossip"), "{text}");
        assert!(text.contains("NODE"), "{text}");
        assert!(!text.contains("STALE/s"), "{text}");
    }

    #[test]
    fn an_empty_cluster_has_no_cache_to_show() {
        let model = Model::new();
        let app = App::new(AppConfig::default());
        let ctx = Ctx {
            now: Instant::now(),
            wall: std::time::SystemTime::UNIX_EPOCH,
            elapsed: Duration::ZERO,
        };
        let scene = Scene {
            app: &app,
            model: &model,
            ctx: &ctx,
            look: app.look(),
            kind: LayoutKind::Full,
        };
        let area = Rect::new(0, 0, 100, 30);
        let mut buf = Buffer::empty(area);
        render(&scene, area, &mut buf);
        let rows: Vec<String> = (0..30).map(|y| row_text(&buf, y)).collect();
        assert!(rows.join("\n").contains("no cache advertised"), "{rows:?}");
    }

    #[test]
    fn eligibility_and_agreement_cells_read_clearly() {
        let model = fixture();
        let app = App::new(AppConfig::default());
        let ctx = Ctx {
            now: model.now().unwrap(),
            wall: model.wall().unwrap(),
            elapsed: Duration::ZERO,
        };
        let scene = Scene {
            app: &app,
            model: &model,
            ctx: &ctx,
            look: app.look(),
            kind: LayoutKind::Full,
        };
        let text = |spans: Vec<Span<'static>>| -> String {
            spans.iter().map(|s| s.content.as_ref()).collect()
        };
        assert_eq!(text(reported_cell(&scene, Some(100.0), 100)), "    100 ✓");
        assert_eq!(text(reported_cell(&scene, Some(90.0), 100)), "     90 ↻");
        assert_eq!(text(reported_cell(&scene, None, 100)), "      —");
        assert_eq!(text(number(&scene, Some(1500.0), 6, text::count)), "  1.5k");
        assert_eq!(text(number(&scene, None, 4, text::count)), "   —");
        assert_eq!(distributed_columns().len(), 11);
        assert_eq!(replicated_columns().len(), 7);
    }

    #[test]
    fn with_metrics_the_distributed_detail_fills_coverage_and_the_per_node_columns() {
        let model = testkit::fixture_model_with_metrics(Instant::now());
        let text = draw(&model, LayoutKind::Full, 140, 37).join("\n");
        // n3 reports 900 of 131,072 parts short.
        assert!(text.contains("coverage  "), "{text}");
        assert!(text.contains("99.3%"), "{text}");
        assert!(!text.contains("no node reports"), "{text}");
        let n3 = text
            .lines()
            .find(|l| l.contains("││ n3") || l.contains("│ n3 "))
            .unwrap();
        assert!(n3.contains(" ↻"), "{n3}");
        let n1 = text
            .lines()
            .find(|l| l.contains(" n1 ") && l.contains('%'))
            .unwrap();
        assert!(n1.contains(" ✓"), "{n1}");
        assert!(
            n1.contains('/'),
            "the fetch mix reads local/remote/miss/error: {n1}"
        );
        assert!(text.contains("Σentries/k ≈ "), "{text}");
        assert!(!text.contains("no exporter mapped"), "{text}");
    }

    #[test]
    fn with_metrics_a_replicated_cache_lists_its_entries_and_backlog() {
        let model = testkit::fixture_model_with_metrics(Instant::now());
        let mut app = App::new(AppConfig::default());
        app.apply_director(UiCommand::Cache("pn".into()), &model);
        app.observe(&model, Instant::now());
        app.snap();
        let text = draw_with(&app, &model, LayoutKind::Full, 140, 37).join("\n");
        assert!(text.contains("divergence  ±0"), "{text}");
        assert!(text.contains("every node holds the same count"), "{text}");
        assert!(
            text.contains('█') || text.contains('▌'),
            "entry bars: {text}"
        );
    }
}

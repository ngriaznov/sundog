//! The Node view: one node in detail, from its gossip record, its exporter
//! and its events.

use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use sundog::observe::MemberStatus;
use sundog::wire::{PROTOCOL_DISTRIBUTED, PROTOCOL_PART_OWNERSHIP, PROTOCOL_VERSION};

use super::data::{self, NodeRow};
use super::look::Token;
use super::panel::{self, gap};
use super::table::{self, Col};
use super::widgets::blocks::BlockArea;
use super::widgets::braille::BrailleArea;
use super::{LayoutKind, Scene, eventlog, overview, text};
use crate::model::derive::{self, Agreement};
use crate::model::events::Filter;
use crate::model::metrics::NodeMetrics;
use crate::source::names;

/// Draws the Node view into `area`.
pub fn render(scene: &Scene<'_>, area: Rect, buf: &mut Buffer) {
    let Some(addr) = scene.selected_addr() else {
        return;
    };
    let Some(row) = data::row_at(scene.model, addr) else {
        return;
    };
    let metrics = data::metrics_of(scene.model, &row).filter(|_| row.status().is_live());
    if scene.app.raw && scene.kind != LayoutKind::Narrow {
        let columns = Layout::horizontal([Constraint::Length(64), Constraint::Min(20)]).split(area);
        render_stack(scene, &row, metrics, columns[0], buf, false);
        raw(scene, &row, metrics, columns[1], buf);
        return;
    }
    render_stack(scene, &row, metrics, area, buf, true);
}

/// Draws the info panel, the charts (with `charts`), the cache table and the
/// events, stacked in `area`.
fn render_stack(
    scene: &Scene<'_>,
    row: &NodeRow<'_>,
    metrics: Option<&NodeMetrics>,
    area: Rect,
    buf: &mut Buffer,
    charts: bool,
) {
    let caches_rows = u16::try_from(row.member.caches.len()).unwrap_or(u16::MAX);
    let caches_height = (caches_rows + 3).clamp(4, 9);
    let parts = Layout::vertical([
        Constraint::Length(match scene.kind {
            LayoutKind::Full => 13,
            _ => 10,
        }),
        Constraint::Length(caches_height),
        Constraint::Min(4),
    ])
    .split(area);
    if !charts || scene.kind == LayoutKind::Narrow {
        info(scene, row, parts[0], buf);
    } else {
        let width = if scene.kind == LayoutKind::Full {
            64
        } else {
            56
        };
        let top =
            Layout::horizontal([Constraint::Length(width), Constraint::Min(20)]).split(parts[0]);
        info(scene, row, top[0], buf);
        if scene.kind == LayoutKind::Full && row.status().is_live() {
            let right = Layout::vertical([Constraint::Length(7), Constraint::Min(4)]).split(top[1]);
            chart(scene, row, metrics, right[0], buf);
            links(scene, row, metrics, right[1], buf);
        } else {
            chart(scene, row, metrics, top[1], buf);
        }
    }
    cache_table(scene, row, metrics, parts[1], buf);
    events(scene, row, parts[2], buf);
}

/// The host part of a `{host}-{nodeid}` name.
#[must_use]
pub fn host_of(name: &str, node: &str) -> String {
    name.strip_suffix(&format!("-{node}"))
        .unwrap_or(name)
        .to_owned()
}

/// The status word and the verb that goes with the time: `live since`.
fn status_phrase(status: MemberStatus) -> &'static str {
    match status {
        MemberStatus::Live => "live since",
        MemberStatus::Departing => "departing since",
        MemberStatus::Down => "down since",
        _ => "left since",
    }
}

/// The title of the info panel; the provenance tag is left off when it does
/// not fit in `width` cells.
fn info_title(scene: &Scene<'_>, row: &NodeRow<'_>, width: usize) -> Vec<Span<'static>> {
    let look = scene.look;
    let age = row
        .uptime(scene.ctx.wall)
        .unwrap_or_else(|| row.status_age(scene.ctx.wall));
    let since = row.started().unwrap_or(row.member.since);
    let glyph = match row.status() {
        MemberStatus::Live => "●",
        MemberStatus::Departing => "◐",
        MemberStatus::Down => "✖",
        _ => "○",
    };
    let mut spans = vec![
        gap(1),
        look.node_span(glyph, row.color()),
        gap(1),
        Span::styled(
            row.label().to_owned(),
            look.node(row.color()).add_modifier(Modifier::BOLD),
        ),
        gap(1),
        look.span(row.full_id(), Token::Muted),
        look.span(
            format!(
                " · {} {} ({})",
                status_phrase(row.status()),
                text::clock(since),
                text::age(age)
            ),
            Token::Muted,
        ),
    ];
    spans.push(gap(1));
    let tag = panel::tag_spans(look, "gossip");
    if panel::width_of(&spans) + panel::width_of(&tag) + 2 <= width {
        let last = spans.pop();
        spans.extend(tag);
        spans.extend(last);
    }
    spans
}

/// The older identity or incarnation that held this node's address.
fn previous_at<'a>(scene: &'a Scene<'_>, row: &NodeRow<'_>) -> Option<&'a sundog::observe::Member> {
    scene
        .model
        .snapshot()?
        .members
        .iter()
        .filter(|member| {
            member.peer.gossip_addr == row.member.peer.gossip_addr
                && member.peer.incarnation < row.member.peer.incarnation
        })
        .max_by_key(|member| member.peer.incarnation)
}

fn field(scene: &Scene<'_>, name: &str, value: Vec<Span<'static>>) -> Line<'static> {
    let mut spans = vec![scene.look.span(text::pad_right(name, 13), Token::Muted)];
    spans.extend(value);
    Line::from(spans)
}

/// The lines of the info panel, `width` cells wide: the data address shares
/// the gossip row when there is room for it.
fn info_lines(scene: &Scene<'_>, row: &NodeRow<'_>, width: usize) -> Vec<Line<'static>> {
    let look = scene.look;
    let peer = &row.member.peer;
    let plain = |content: String| vec![look.span(content, Token::Text)];
    let mut lines = vec![field(
        scene,
        "host",
        plain(host_of(peer.name.as_str(), &row.full_id())),
    )];
    let gossip = peer.gossip_addr.to_string();
    let data = peer.data_addr.to_string();
    if 13 + 17 + 5 + data.chars().count() <= width {
        lines.push(field(
            scene,
            "gossip",
            vec![
                look.span(text::pad_right(&gossip, 17), Token::Text),
                look.span("data ", Token::Muted),
                look.span(data, Token::Text),
            ],
        ));
    } else {
        lines.push(field(scene, "gossip", plain(gossip)));
        lines.push(field(scene, "data", plain(data)));
    }
    let mut incarnation = vec![look.span(peer.incarnation.to_string(), Token::Text)];
    if let Some(older) = previous_at(scene, row) {
        let was = older.peer.node.to_string();
        let same = older.peer.node == peer.node;
        incarnation.push(look.span(
            if same {
                " · ↻ restarted".to_owned()
            } else {
                format!(" · ↻ rejoined (was {}…)", text::short_id(&was))
            },
            Token::Info,
        ));
    }
    lines.push(field(scene, "incarnation", incarnation));
    let ownership = if peer.protocol >= PROTOCOL_PART_OWNERSHIP {
        "part ownership"
    } else if peer.protocol >= PROTOCOL_DISTRIBUTED {
        "bucket ownership"
    } else {
        "no Distributed caches"
    };
    let protocol_token = if peer.protocol == PROTOCOL_VERSION {
        Token::Text
    } else {
        Token::Warn
    };
    lines.push(field(
        scene,
        "protocol",
        vec![
            look.span(peer.protocol.to_string(), protocol_token),
            look.span(
                format!(" (lens {PROTOCOL_VERSION}) · {ownership}"),
                Token::Muted,
            ),
        ],
    ));
    lines.push(field(scene, "peers", peers_value(scene, row)));
    lines.push(field(scene, "exporter", exporter_value(scene, row)));
    let mut caches = Vec::new();
    let mut listed: Vec<_> = row.member.caches.iter().collect();
    listed.sort_by_key(|(_, mode)| !matches!(mode, sundog::store::Mode::Distributed { .. }));
    for (index, (name, mode)) in listed.into_iter().enumerate() {
        if index > 0 {
            caches.push(look.span(" · ", Token::Faint));
        }
        caches.push(look.span(name.to_string(), Token::Text));
        caches.push(look.span(format!(" {}", data::mode_dotted(*mode)), Token::Muted));
    }
    if caches.is_empty() {
        caches.push(look.span("none", Token::Faint));
    }
    lines.push(field(scene, "caches", caches));
    lines
}

fn peers_value(scene: &Scene<'_>, row: &NodeRow<'_>) -> Vec<Span<'static>> {
    let look = scene.look;
    match scene.model.peers(row.member.peer.gossip_addr) {
        Some(view) => vec![
            look.span(
                format!("{}/{}", text::whole(view.reported), view.expected),
                if view.amber { Token::Warn } else { Token::Text },
            ),
            look.span(" (sundog_live_peers vs observer)", Token::Muted),
        ],
        None => vec![look.span("— (no metrics)", Token::Faint)],
    }
}

fn exporter_value(scene: &Scene<'_>, row: &NodeRow<'_>) -> Vec<Span<'static>> {
    let look = scene.look;
    let Some(state) = scene.model.exporter(row.member.peer.gossip_addr) else {
        return vec![look.span("not mapped", Token::Faint)];
    };
    if !row.status().is_live() {
        return vec![look.span("no longer scraped", Token::Muted)];
    }
    if let Some(error) = state.mapping_error() {
        return vec![look.span(format!("not scraped: {error}"), Token::Warn)];
    }
    let mut spans = Vec::new();
    if state.unreachable() {
        spans.push(look.span("not answering", Token::Bad));
    } else if data::metrics_of(scene.model, row).is_some() {
        spans.push(look.span("answering", Token::Ok));
    } else {
        spans.push(look.span("waiting for the first answer", Token::Muted));
    }
    match state.ready() {
        Some(true) => {
            spans.push(look.span(" · ready ", Token::Muted));
            spans.push(look.span("✓", Token::Ok));
        }
        Some(false) => {
            spans.push(look.span(" · not ready ", Token::Muted));
            spans.push(look.span("…", Token::Warn));
        }
        None => {}
    }
    if state.mismatch() {
        spans.push(look.span("  ⚠ map", Token::Warn));
    }
    spans
}

fn info(scene: &Scene<'_>, row: &NodeRow<'_>, area: Rect, buf: &mut Buffer) {
    let title = info_title(scene, row, usize::from(area.width));
    let block = panel::block_with(scene.look, title, Vec::new(), true);
    let inner = panel::draw(block, area, buf);
    let inner = Rect::new(
        inner.x + 1,
        inner.y,
        inner.width.saturating_sub(2),
        inner.height,
    );
    panel::lines(info_lines(scene, row, usize::from(inner.width)), inner, buf);
}

/// What a node that is not live shows in place of its metrics.
fn gone_notice(scene: &Scene<'_>, row: &NodeRow<'_>) -> Vec<Line<'static>> {
    let look = scene.look;
    let age = text::age(row.status_age(scene.ctx.wall));
    match row.status() {
        MemberStatus::Down => vec![
            Line::from(look.span(format!("down {age} ago"), Token::Bad)),
            Line::from(look.span(
                "no departure seen: a crash, a stall or a partition",
                Token::Muted,
            )),
        ],
        MemberStatus::Left => vec![Line::from(
            look.span(format!("left {age} ago after a departure"), Token::Muted),
        )],
        _ => vec![Line::from(look.span("no metrics", Token::Muted))],
    }
}

fn chart(
    scene: &Scene<'_>,
    row: &NodeRow<'_>,
    metrics: Option<&NodeMetrics>,
    area: Rect,
    buf: &mut Buffer,
) {
    let look = scene.look;
    let block = panel::block(look, "ops/s", "metrics", Vec::new(), false);
    let inner = panel::draw(block, area, buf);
    if inner.height < 3 || inner.width < 4 {
        return;
    }
    let Some(metrics) = metrics else {
        if row.status().is_live() {
            overview::no_exporter(scene, inner, buf);
        } else {
            panel::centered(gone_notice(scene, row), inner, buf);
        }
        return;
    };
    if !data::scrape_answered(scene.model, row) {
        panel::centered(
            vec![Line::from(look.span("exporter not answering", Token::Warn))],
            inner,
            buf,
        );
        return;
    }
    let ops = metrics.ops().to_vec();
    let max = ops.iter().copied().fold(0.0, f64::max).max(1.0) * 1.1;
    let plot = Rect::new(
        inner.x + 1,
        inner.y,
        inner.width.saturating_sub(2),
        inner.height - 1,
    );
    if look.braille {
        ratatui::widgets::Widget::render(
            BrailleArea::new(&ops, max, look.mode).tinted(row.color()),
            plot,
            buf,
        );
    } else {
        ratatui::widgets::Widget::render(
            BlockArea::new(&ops, max, look.mode).tinted(row.color()),
            plot,
            buf,
        );
    }
    let mut spans = vec![
        look.span(
            format!("{} ops/s", text::count(ops.last().copied().unwrap_or(0.0))),
            Token::Accent,
        ),
        look.span(" · tx ", Token::Muted),
        look.span(
            text::byte_rate(metrics.tx_bytes().last().unwrap_or(0.0)),
            Token::Text,
        ),
        look.span(" · ", Token::Faint),
        look.span(
            format!(
                "{} frames/s",
                text::count(metrics.tx_frames().last().unwrap_or(0.0))
            ),
            Token::Text,
        ),
    ];
    if let Some(ready) = scene
        .model
        .exporter(row.member.peer.gossip_addr)
        .and_then(crate::model::ExporterState::ready)
    {
        spans.push(look.span(" · ready ", Token::Muted));
        spans.push(if ready {
            look.span("✓", Token::Ok)
        } else {
            look.span("…", Token::Warn)
        });
    }
    panel::lines(
        vec![Line::from(spans)],
        Rect::new(plot.x, plot.y + plot.height, plot.width, 1),
        buf,
    );
}

/// One peer link as the exporter reports it.
#[derive(Debug, Clone, PartialEq)]
struct Link {
    peer: String,
    dropped: f64,
    wait: f64,
}

/// The peers the node reports a drop or a wait for, by peer id.
fn link_rows(metrics: &NodeMetrics) -> Vec<Link> {
    let mut links: std::collections::BTreeMap<String, Link> = std::collections::BTreeMap::new();
    for (key, value) in metrics.samples() {
        let Some(peer) = key.label("peer") else {
            continue;
        };
        let entry = links.entry(peer.to_owned()).or_insert_with(|| Link {
            peer: peer.to_owned(),
            dropped: 0.0,
            wait: 0.0,
        });
        if key.name() == names::BACKLOG_DROPPED {
            entry.dropped = value;
        } else if key.name() == names::FAN_OUT_WAIT_SECONDS {
            entry.wait = value;
        }
    }
    links.into_values().collect()
}

fn links(
    scene: &Scene<'_>,
    row: &NodeRow<'_>,
    metrics: Option<&NodeMetrics>,
    area: Rect,
    buf: &mut Buffer,
) {
    let look = scene.look;
    let block = panel::block(look, "links", "metrics", Vec::new(), false);
    let inner = panel::draw(block, area, buf);
    let Some(metrics) = metrics else {
        if !row.status().is_live() {
            panel::centered(gone_notice(scene, row), inner, buf);
        }
        return;
    };
    let rows = link_rows(metrics);
    if rows.is_empty() {
        panel::centered(
            vec![Line::from(
                look.span("no dropped frames, no writer waits", Token::Faint),
            )],
            inner,
            buf,
        );
        return;
    }
    let inner = Rect::new(
        inner.x + 1,
        inner.y,
        inner.width.saturating_sub(2),
        inner.height,
    );
    let mut lines = vec![Line::from(look.span(
        format!(
            "{}{}{}",
            text::pad_right("PEER", 12),
            text::pad_right("DROPPED", 10),
            "WAIT s"
        ),
        Token::Muted,
    ))];
    for link in rows {
        let peer = data::tag_of_hex(scene.model, &link.peer);
        let name = peer.as_ref().map_or_else(
            || {
                look.span(
                    text::pad_right(text::short_id(&link.peer), 12),
                    Token::Muted,
                )
            },
            |tag| look.node_span(text::pad_right(&tag.label, 12), tag.color),
        );
        lines.push(Line::from(vec![
            name,
            look.span(
                text::pad_right(&text::whole(link.dropped), 10),
                if link.dropped > 0.0 {
                    Token::Bad
                } else {
                    Token::Text
                },
            ),
            look.span(
                text::whole(link.wait),
                if link.wait > 0.0 {
                    Token::Warn
                } else {
                    Token::Text
                },
            ),
        ]));
    }
    panel::lines(lines, inner, buf);
}

fn cache_columns() -> Vec<Col> {
    vec![
        Col::new("CACHE", 9, 9),
        Col::new("MODE", 6, 8),
        Col::right("ENTRIES", 9, 7),
        Col::right("BYTES", 9, 3),
        Col::right("HIT%", 7, 6),
        Col::right("BACKLOG", 9, 4),
        Col::new("OWNED computed / reported", 29, 5),
        Col::right("AE rep/s", 10, 2),
        Col::right("XFER/s", 8, 2),
        Col::right("SPILL", 10, 1),
    ]
}

fn cell(
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

fn owned_cell(
    scene: &Scene<'_>,
    row: &NodeRow<'_>,
    cache: &str,
    metrics: Option<&NodeMetrics>,
) -> Vec<Span<'static>> {
    let look = scene.look;
    let Some(digest) = scene.model.ownership(cache) else {
        return vec![look.span("—", Token::Faint)];
    };
    let computed = digest.parts_owned_by(row.member.peer.node);
    let reported = metrics.and_then(|m| m.owned_parts(cache));
    let mut spans = vec![look.span(
        text::thousands(u64::try_from(computed).unwrap_or(0)),
        Token::Text,
    )];
    spans.push(look.span(" / ", Token::Faint));
    match derive::agreement(reported, computed) {
        Agreement::Unknown => spans.push(look.span("—", Token::Faint)),
        Agreement::Match => {
            spans.push(look.span(text::whole(reported.unwrap_or(0.0)), Token::Text));
            spans.push(look.span(" ✓", Token::Ok));
        }
        Agreement::Differs => {
            spans.push(look.span(text::whole(reported.unwrap_or(0.0)), Token::Text));
            spans.push(look.span(" ↻", Token::Move));
        }
    }
    spans
}

fn cache_table(
    scene: &Scene<'_>,
    row: &NodeRow<'_>,
    metrics: Option<&NodeMetrics>,
    area: Rect,
    buf: &mut Buffer,
) {
    let look = scene.look;
    let title = vec![
        gap(1),
        panel::title_span(look, "caches on"),
        gap(1),
        look.node_span(row.label().to_owned(), row.color()),
    ];
    let mut title = title;
    title.extend(panel::tag_spans(look, "metrics"));
    title.push(gap(1));
    let block = panel::block_with(look, title, Vec::new(), false);
    let inner = panel::draw(block, area, buf);
    if inner.height < 2 {
        return;
    }
    let inner = Rect::new(
        inner.x + 1,
        inner.y,
        inner.width.saturating_sub(2),
        inner.height,
    );
    let cols = cache_columns();
    let visible = table::visible(&cols, usize::from(inner.width));
    let mut lines = vec![table::header(look, &cols, &visible)];
    let mut caches: Vec<_> = row.member.caches.iter().collect();
    caches.sort_by_key(|(_, mode)| !matches!(mode, sundog::store::Mode::Distributed { .. }));
    for (name, &mode) in caches {
        let cache = name.as_str();
        let spill = metrics.and_then(|m| {
            let entries = m.cache_value(names::SPILL_ENTRIES, cache)?;
            Some(text::count(entries))
        });
        let cells = vec![
            vec![look.span(text::pad_right(cache, 8), Token::Text)],
            vec![look.span(text::pad_right(&data::mode_dotted(mode), 5), Token::Muted)],
            cell(
                scene,
                metrics.and_then(|m| m.entries(cache)),
                8,
                text::whole,
            ),
            cell(
                scene,
                metrics.and_then(|m| m.cache_value(names::CACHE_BYTES, cache)),
                8,
                text::bytes,
            ),
            cell(scene, metrics.and_then(|m| m.hit_ratio(cache)), 6, |v| {
                text::percent(v, 1)
            }),
            cell(
                scene,
                metrics.and_then(|m| m.cache_value(names::FAN_OUT_BACKLOG, cache)),
                8,
                text::whole,
            ),
            if matches!(mode, sundog::store::Mode::Distributed { .. }) {
                owned_cell(scene, row, cache, metrics)
            } else {
                vec![look.span("—", Token::Faint)]
            },
            cell(
                scene,
                metrics.and_then(|m| m.rate(names::AE_REPAIRED, &[("cache", cache)])),
                9,
                text::count,
            ),
            cell(
                scene,
                metrics.and_then(|m| m.rate(names::STATE_TRANSFER_RECORDS, &[("cache", cache)])),
                7,
                text::count,
            ),
            match spill {
                Some(entries) => vec![look.span(text::pad_left(&entries, 9), Token::Text)],
                None => vec![look.span(text::pad_left("—", 9), Token::Faint)],
            },
        ];
        lines.push(table::row(&cols, &visible, cells));
    }
    panel::lines(lines, inner, buf);
}

fn events(scene: &Scene<'_>, row: &NodeRow<'_>, area: Rect, buf: &mut Buffer) {
    let look = scene.look;
    let node = row.member.peer.node;
    let mine: Vec<_> = scene
        .model
        .events()
        .newest_first(Filter::All)
        .filter(|event| event.kind.node() == Some(node))
        .collect();
    let mut title = vec![gap(1), panel::title_span(look, "events for"), gap(1)];
    title.push(look.node_span(row.label().to_owned(), row.color()));
    title.push(gap(1));
    let block = panel::block_with(
        look,
        title,
        vec![look.span(format!("{}", mine.len()), Token::Muted)],
        false,
    );
    let inner = panel::draw(block, area, buf);
    if mine.is_empty() {
        panel::centered(
            vec![Line::from(
                look.span("no events for this node", Token::Faint),
            )],
            inner,
            buf,
        );
        return;
    }
    eventlog::render(scene, &mine, 0, inner, buf);
}

/// The raw samples of the node's last scrape.
fn raw(
    scene: &Scene<'_>,
    row: &NodeRow<'_>,
    metrics: Option<&NodeMetrics>,
    area: Rect,
    buf: &mut Buffer,
) {
    let look = scene.look;
    let block = panel::block(look, "raw samples", "metrics", Vec::new(), true);
    let inner = panel::draw(block, area, buf);
    let Some(metrics) = metrics else {
        if row.status().is_live() {
            overview::no_exporter(scene, inner, buf);
        } else {
            panel::centered(gone_notice(scene, row), inner, buf);
        }
        return;
    };
    let lines: Vec<Line<'static>> = metrics
        .samples()
        .map(|(key, value)| {
            let labels = if key.labels().is_empty() {
                String::new()
            } else {
                let inside: Vec<String> = key
                    .labels()
                    .iter()
                    .map(|(name, value)| format!("{name}=\"{value}\""))
                    .collect();
                format!("{{{}}}", inside.join(","))
            };
            Line::from(vec![
                look.span(
                    key.name().trim_start_matches("sundog_").to_owned(),
                    Token::Text,
                ),
                look.span(labels, Token::Muted),
                look.span(format!(" {}", text::sample(value)), Token::Accent),
            ])
        })
        .collect();
    let inner = Rect::new(
        inner.x + 1,
        inner.y,
        inner.width.saturating_sub(2),
        inner.height,
    );
    let capacity = usize::from(inner.height);
    let shown: Vec<Line<'static>> = lines.iter().take(capacity).cloned().collect();
    panel::lines(shown, inner, buf);
    if lines.len() > capacity && inner.height > 0 {
        let hidden = lines.len() - capacity + 1;
        panel::lines(
            vec![Line::from(panel::pad_spans(
                vec![look.span(format!("+{hidden} more samples"), Token::Muted)],
                usize::from(inner.width),
            ))],
            Rect::new(inner.x, inner.y + inner.height - 1, inner.width, 1),
            buf,
        );
    }
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

    fn buffer_with(app: &App, model: &Model, w: u16, h: u16) -> Buffer {
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
            kind: LayoutKind::Full,
        };
        let area = Rect::new(0, 0, w, h);
        let mut buf = Buffer::empty(area);
        render(&scene, area, &mut buf);
        buf
    }

    fn app_on(label: &str, model: &Model) -> App {
        let mut app = App::new(AppConfig::default());
        app.apply_director(UiCommand::Select(label.into()), model);
        app.observe(model, Instant::now());
        app.snap();
        app
    }

    fn fixture() -> Model {
        testkit::fixture_model(Instant::now())
    }

    #[test]
    fn the_info_panel_names_the_node_its_addresses_protocol_and_caches() {
        let model = fixture();
        let rows = draw_with(&app_on("n2", &model), &model, LayoutKind::Full, 140, 37);
        let text = rows.join("\n");
        assert!(rows[0].starts_with("╭ ● n2 "), "{}", rows[0]);
        assert!(rows[0].contains("live since 00:00:00 (19s)"), "{}", rows[0]);
        assert!(text.contains("host         host"), "{text}");
        assert!(text.contains("gossip       127.0.0.12:7946"), "{text}");
        assert!(text.contains("data 127.0.0.12:39211"), "{text}");
        assert!(text.contains("incarnation  1"), "{text}");
        assert!(
            text.contains("protocol     6 (lens 6) · part ownership"),
            "{text}"
        );
        assert!(text.contains("peers        — (no metrics)"), "{text}");
        assert!(text.contains("exporter     not mapped"), "{text}");
        assert!(
            text.contains("caches       it D·2 · churn R · os R · pn R"),
            "{text}"
        );
    }

    #[test]
    fn the_ops_chart_is_shaded_in_the_nodes_own_color() {
        let model = testkit::fixture_model_with_metrics(Instant::now());
        let buf = buffer_with(&app_on("n2", &model), &model, 140, 37);
        let color = data::row_labeled(&model, "n2").unwrap().color();
        assert_eq!(color, crate::ui::theme::NODE_COLORS[1]);
        let shades: Vec<_> = (0..=12)
            .map(|step| {
                crate::ui::theme::tinted_at(color, f64::from(step) / 12.0)
                    .color(crate::ui::theme::ColorMode::Truecolor)
            })
            .collect();
        let (mut tinted, mut amber) = (0, 0);
        for y in 1..10 {
            for x in 66..138 {
                let cell = &buf[(x, y)];
                if !cell
                    .symbol()
                    .chars()
                    .all(|c| ('\u{2801}'..='\u{28FF}').contains(&c))
                {
                    continue;
                }
                if shades.contains(&cell.fg) {
                    tinted += 1;
                } else if cell.fg
                    == crate::ui::theme::GRADIENT[1].color(crate::ui::theme::ColorMode::Truecolor)
                {
                    amber += 1;
                }
            }
        }
        assert!(tinted > 20, "{tinted} tinted cells");
        assert_eq!(amber, 0, "no cell takes the amber throughput gradient");
    }

    #[test]
    fn a_node_whose_exporter_stopped_answering_says_so_in_place_of_the_chart() {
        let mut model = testkit::fixture_model_with_metrics(Instant::now());
        let row = data::row_labeled(&model, "n2").unwrap();
        let report = crate::source::ScrapeReport {
            addr: row.member.peer.gossip_addr,
            node: row.member.peer.node,
            at: model.now().unwrap(),
            outcome: Err(crate::source::scrape::ScrapeError::Timeout),
            ready: None,
        };
        let (now, wall) = (model.now().unwrap(), model.wall().unwrap());
        model.apply(crate::source::Update::Scrape(report), now, wall);
        let text = draw_with(&app_on("n2", &model), &model, LayoutKind::Full, 140, 37).join("\n");
        assert!(text.contains("exporter not answering"), "{text}");
        let healthy = testkit::fixture_model_with_metrics(Instant::now());
        let text =
            draw_with(&app_on("n2", &healthy), &healthy, LayoutKind::Full, 140, 37).join("\n");
        assert!(!text.contains("exporter not answering"), "{text}");
    }

    #[test]
    fn without_metrics_the_panels_say_how_to_get_them() {
        let model = fixture();
        let text = draw_with(&app_on("n1", &model), &model, LayoutKind::Full, 140, 37).join("\n");
        assert!(text.contains("ops/s · metrics"), "{text}");
        assert!(text.contains("no exporter mapped"), "{text}");
        assert!(text.contains("links · metrics"), "{text}");
    }

    #[test]
    fn the_cache_table_lists_each_cache_with_the_computed_share() {
        let model = fixture();
        let text = draw_with(&app_on("n1", &model), &model, LayoutKind::Full, 140, 37).join("\n");
        assert!(text.contains("caches on n1 · metrics"), "{text}");
        for title in ["CACHE", "MODE", "ENTRIES", "OWNED computed / reported"] {
            assert!(text.contains(title), "{title} in\n{text}");
        }
        let it = text
            .lines()
            .find(|l| l.contains("│it ") || l.contains("│ it "))
            .unwrap_or("");
        assert!(it.contains("D·2"), "{text}");
        assert!(it.contains(" / —"), "{it}");
    }

    #[test]
    fn the_events_panel_lists_only_this_nodes_events() {
        let model = fixture();
        let text = draw_with(&app_on("n6", &model), &model, LayoutKind::Full, 140, 37).join("\n");
        assert!(text.contains("events for n6"), "{text}");
        assert!(text.contains("LEAVE"), "{text}");
        assert!(!text.contains("DOWN"), "n7's crash is not n6's: {text}");
    }

    #[test]
    fn a_down_node_shows_its_status_in_the_title_and_no_metrics() {
        let model = fixture();
        let rows = draw_with(&app_on("n7", &model), &model, LayoutKind::Full, 140, 37);
        let text = rows.join("\n");
        assert!(rows[0].starts_with("╭ ✖ n7 "), "{}", rows[0]);
        assert!(rows[0].contains("down since"), "{}", rows[0]);
        assert!(
            text.contains("no departure seen: a crash, a stall or a partition"),
            "{text}"
        );
        assert!(text.contains("DOWN"), "{text}");
    }

    #[test]
    fn a_node_that_is_gone_gets_one_notice_in_place_of_the_charts() {
        let model = fixture();
        let text = draw_with(&app_on("n7", &model), &model, LayoutKind::Full, 140, 37).join("\n");
        assert_eq!(
            text.matches("no departure seen: a crash").count(),
            1,
            "{text}"
        );
        assert!(!text.contains("links · metrics"), "{text}");
    }

    #[test]
    fn a_left_node_says_it_left() {
        let model = fixture();
        let text = draw_with(&app_on("n8", &model), &model, LayoutKind::Full, 140, 37).join("\n");
        assert!(text.contains("left 10s ago after a departure"), "{text}");
    }

    #[test]
    fn raw_mode_replaces_the_charts_with_the_samples_panel() {
        let model = fixture();
        let mut app = app_on("n1", &model);
        app.raw = true;
        let text = draw_with(&app, &model, LayoutKind::Full, 140, 37).join("\n");
        assert!(text.contains("raw samples · metrics"), "{text}");
        assert!(text.contains("no exporter mapped"), "{text}");
        assert!(!text.contains("links · metrics"), "{text}");
        assert!(
            text.contains("caches on n1"),
            "the node stays on the left: {text}"
        );
    }

    #[test]
    fn smaller_screens_drop_panels_but_keep_the_node() {
        let model = fixture();
        let compact =
            draw_with(&app_on("n1", &model), &model, LayoutKind::Compact, 120, 33).join("\n");
        assert!(
            compact.contains("ops/s · metrics") && !compact.contains("links · metrics"),
            "{compact}"
        );
        let narrow =
            draw_with(&app_on("n1", &model), &model, LayoutKind::Narrow, 80, 21).join("\n");
        assert!(
            narrow.contains("● n1") && !narrow.contains("ops/s"),
            "{narrow}"
        );
        assert!(narrow.contains("caches on n1"), "{narrow}");
    }

    #[test]
    fn a_narrow_info_panel_puts_the_data_address_on_its_own_row_and_drops_the_tag() {
        let model = fixture();
        let wide = draw_with(&app_on("n1", &model), &model, LayoutKind::Full, 140, 37);
        assert!(wide[2].contains("data 127.0.0.11:39211"), "{}", wide[2]);
        assert!(wide[0].contains("· gossip"), "{}", wide[0]);
        let app = app_on("n1", &model);
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
            kind: LayoutKind::Compact,
        };
        let row = data::row_labeled(&model, "n1").unwrap();
        let text = |lines: Vec<Line<'static>>| -> Vec<String> {
            lines
                .iter()
                .map(|l| l.spans.iter().map(|s| s.content.as_ref()).collect())
                .collect()
        };
        let roomy = text(info_lines(&scene, &row, 60));
        assert_eq!(
            roomy[1],
            "gossip       127.0.0.11:7946  data 127.0.0.11:39211"
        );
        let tight = text(info_lines(&scene, &row, 40));
        assert_eq!(tight[1], "gossip       127.0.0.11:7946");
        assert_eq!(tight[2], "data         127.0.0.11:39211");
        let title = |width| -> String {
            info_title(&scene, &row, width)
                .iter()
                .map(|s| s.content.as_ref())
                .collect()
        };
        assert!(title(120).ends_with("· gossip "), "{}", title(120));
        assert!(!title(50).contains("gossip"), "{}", title(50));
        assert!(title(50).starts_with(" ● n1 "), "{}", title(50));
    }

    #[test]
    fn host_names_lose_their_node_id_suffix() {
        assert_eq!(
            host_of("lens-host-7f3a51d2e09bc210", "7f3a51d2e09bc210"),
            "lens-host"
        );
        assert_eq!(host_of("odd", "7f3a51d2e09bc210"), "odd");
    }

    #[test]
    fn status_phrases_and_link_rows_are_plain() {
        assert_eq!(status_phrase(MemberStatus::Live), "live since");
        assert_eq!(status_phrase(MemberStatus::Departing), "departing since");
        assert_eq!(status_phrase(MemberStatus::Down), "down since");
        assert_eq!(status_phrase(MemberStatus::Left), "left since");
        let metrics = NodeMetrics::new(sundog::NodeId::from(1));
        assert!(link_rows(&metrics).is_empty());
        assert_eq!(cache_columns().len(), 10);
    }

    #[test]
    fn a_node_that_rejoined_names_the_identity_it_replaced() {
        let mut model = Model::new();
        let now = Instant::now();
        let snapshot = sundog::observe::ClusterSnapshot::new(
            "c",
            vec![
                testkit::member_at(1, 0, 1, MemberStatus::Down),
                testkit::member_at(1, 1, 2, MemberStatus::Live),
            ],
            0,
        );
        model.apply(
            crate::source::Update::Snapshot(std::sync::Arc::new(snapshot), now),
            now,
            std::time::SystemTime::UNIX_EPOCH,
        );
        let text = draw_with(&app_on("n1", &model), &model, LayoutKind::Full, 140, 37).join("\n");
        assert!(text.contains("↻ rejoined (was "), "{text}");
    }

    fn live() -> Model {
        testkit::fixture_model_with_metrics(Instant::now())
    }

    #[test]
    fn with_metrics_the_chart_the_peers_and_the_cache_table_read_the_exporter() {
        let model = live();
        let text = draw_with(&app_on("n3", &model), &model, LayoutKind::Full, 140, 37).join("\n");
        assert!(
            text.contains("peers        5/5 (sundog_live_peers vs observer)"),
            "{text}"
        );
        assert!(text.contains("exporter     answering · ready ✓"), "{text}");
        assert!(text.contains(" ops/s · tx "), "{text}");
        assert!(text.contains(" frames/s · ready ✓"), "{text}");
        assert!(
            text.contains("no dropped frames, no writer waits"),
            "{text}"
        );
        // n3 reports 900 parts fewer than the observer computes for it.
        let it = text
            .lines()
            .find(|l| l.contains("D·2") && l.contains(" / "))
            .unwrap();
        assert!(it.contains(" ↻"), "{it}");
        assert!(it.contains("55.1%"), "the hit ratio: {it}");
        let ok = draw_with(&app_on("n1", &model), &model, LayoutKind::Full, 140, 37).join("\n");
        let it = ok
            .lines()
            .find(|l| l.contains("D·2") && l.contains(" / "))
            .unwrap();
        assert!(it.contains(" ✓"), "{it}");
    }

    #[test]
    fn raw_mode_lists_the_last_scrape_with_labels_and_counts_what_does_not_fit() {
        let model = live();
        let mut app = app_on("n1", &model);
        app.raw = true;
        let text = draw_with(&app, &model, LayoutKind::Full, 140, 37).join("\n");
        assert!(text.contains("cache_hits_total{cache=\"it\"}"), "{text}");
        assert!(text.contains("cache_entries{cache=\"it\"} 2040"), "{text}");
        let narrow = draw_with(&app, &model, LayoutKind::Full, 140, 12).join("\n");
        assert!(narrow.contains(" more samples"), "{narrow}");
    }

    #[test]
    fn link_rows_gather_drops_and_waits_per_peer() {
        use crate::source::expo::Sample;
        let sample = |name: &str, peer: &str, value: f64| Sample {
            name: name.to_owned(),
            labels: vec![("peer".to_owned(), peer.to_owned())],
            value,
        };
        let mut metrics = NodeMetrics::new(sundog::NodeId::from(1));
        metrics.fold(
            Instant::now(),
            &[
                sample(names::BACKLOG_DROPPED, "00000000000000b2", 312.0),
                sample(names::FAN_OUT_WAIT_SECONDS, "00000000000000b2", 4.0),
                sample(names::FAN_OUT_WAIT_SECONDS, "00000000000000a1", 1.0),
            ],
        );
        assert_eq!(
            link_rows(&metrics),
            [
                Link {
                    peer: "00000000000000a1".into(),
                    dropped: 0.0,
                    wait: 1.0
                },
                Link {
                    peer: "00000000000000b2".into(),
                    dropped: 312.0,
                    wait: 4.0
                },
            ]
        );
    }
}

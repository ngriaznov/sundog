//! The Members table: one row per node with its status, address, uptime,
//! protocol, caches, ownership share, peer count and operations rate.

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::widgets::Widget;
use sundog::observe::MemberStatus;
use sundog::store::Mode;

use super::data::{self, JOINED_FOR, NodeRow, REJOINED_FOR};
use super::look::Token;
use super::panel::{self, gap};
use super::theme;
use super::widgets::{sharebar, spark};
use super::{LayoutKind, Scene, anim, text};
use crate::app::{DOWN_HOLD, JOIN_PULSE};
use crate::model::derive::{self, Agreement};
use crate::model::ownership::OwnershipDigest;

/// A column of the table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Col {
    /// Label and short id.
    Name,
    /// Gossip address.
    Gossip,
    /// Time in status.
    Up,
    /// Wire protocol.
    Proto,
    /// Cache pills.
    Caches,
    /// Ownership share of the selected cache.
    Share,
    /// Peer count against the observer's.
    Peers,
    /// Operations per second.
    Ops,
}

/// The width of the bar in the SHARE column.
const BAR: usize = 8;

/// The width of the `▌● ` prefix: selection bar, status glyph, space.
const PREFIX: usize = 3;

/// The columns to draw and the widths of the variable ones.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cols {
    /// The columns, left to right.
    pub list: Vec<Col>,
    /// The width of the label part of the NAME column.
    pub label: usize,
    /// The width of the GOSSIP column, with its gap.
    pub gossip: usize,
}

impl Cols {
    /// The columns for a table `width` cells wide on a `kind` screen, over
    /// rows whose labels are up to `label` wide and whose gossip addresses
    /// are up to `gossip` characters. A narrow screen keeps NAME, SHARE and
    /// OPS; wider ones drop GOSSIP, CACHES, P, UP and PEERS, in that order,
    /// until the table fits.
    #[must_use]
    pub fn for_width(kind: LayoutKind, width: usize, label: usize, gossip: usize) -> Self {
        let mut cols = Self {
            list: vec![
                Col::Name,
                Col::Gossip,
                Col::Up,
                Col::Proto,
                Col::Caches,
                Col::Share,
                Col::Peers,
                Col::Ops,
            ],
            label: label.clamp(2, 8),
            gossip: gossip.max(15) + 1,
        };
        if kind == LayoutKind::Narrow {
            cols.list = vec![Col::Name, Col::Share, Col::Ops];
            return cols;
        }
        if kind == LayoutKind::Compact {
            cols.list.retain(|col| *col != Col::Gossip);
        }
        for dropped in [Col::Gossip, Col::Caches, Col::Proto, Col::Up, Col::Peers] {
            if cols.total() <= width {
                break;
            }
            cols.list.retain(|col| *col != dropped);
        }
        cols
    }

    /// The width of `col`, with its trailing gap.
    #[must_use]
    pub const fn width_of(&self, col: Col) -> usize {
        match col {
            Col::Name => self.label + 7,
            Col::Gossip => self.gossip,
            Col::Up | Col::Peers => 6,
            Col::Proto => 3,
            Col::Caches | Col::Ops => 9,
            Col::Share => BAR + 10,
        }
    }

    /// The width of the whole table, the prefix included.
    #[must_use]
    pub fn total(&self) -> usize {
        PREFIX
            + self
                .list
                .iter()
                .map(|&col| self.width_of(col))
                .sum::<usize>()
    }

    fn has(&self, col: Col) -> bool {
        self.list.contains(&col)
    }
}

/// The glyph, token and node-color flag of a row's status.
fn status_glyph(scene: &Scene<'_>, row: &NodeRow<'_>) -> Span<'static> {
    let look = scene.look;
    let age = row.status_age(scene.ctx.wall);
    match row.status() {
        MemberStatus::Live => {
            let glyph = if age < JOINED_FOR { '✚' } else { '●' };
            Span::styled(glyph.to_string(), look.node(row.color()))
        }
        MemberStatus::Departing => {
            let glyph = if scene.app.anim && !anim::blink(scene.ctx.elapsed) {
                '◒'
            } else {
                '◐'
            };
            look.span(glyph.to_string(), Token::Warn)
        }
        MemberStatus::Down => {
            let token = if age < DOWN_HOLD {
                Token::Bad
            } else {
                Token::Faint
            };
            look.span("✖", token)
        }
        _ => look.span("○", Token::Muted),
    }
}

/// The pills of a node's caches: `D R R R`, or `D R R +2` past four.
fn cache_pills(scene: &Scene<'_>, row: &NodeRow<'_>) -> Vec<Span<'static>> {
    let look = scene.look;
    let caches: Vec<(char, Token)> = row
        .member
        .caches
        .iter()
        .map(|(_, &mode)| {
            let (letter, _) = data::mode_letter(mode);
            let token = match mode {
                Mode::Distributed { .. } => Token::Accent,
                Mode::Replicated => Token::Info,
                Mode::Invalidation => Token::Move,
                _ => Token::Muted,
            };
            (letter, token)
        })
        .collect();
    // Distributed first, as the Caches list orders them.
    let mut caches = caches;
    caches.sort_by_key(|(letter, _)| *letter != 'D');
    let shown = if caches.len() > 4 { 3 } else { caches.len() };
    let mut spans = Vec::new();
    for (index, (letter, token)) in caches.iter().take(shown).enumerate() {
        if index > 0 {
            spans.push(gap(1));
        }
        spans.push(Span::styled(
            letter.to_string(),
            look.style(*token).add_modifier(Modifier::BOLD),
        ));
    }
    if caches.len() > shown {
        spans.push(look.span(format!(" +{}", caches.len() - shown), Token::Muted));
    }
    spans
}

/// The SHARE cell: bar, percentage and agreement mark.
fn share_cell(
    scene: &Scene<'_>,
    row: &NodeRow<'_>,
    digest: Option<&OwnershipDigest>,
) -> Vec<Span<'static>> {
    let look = scene.look;
    let blank = || vec![gap(BAR + 10)];
    let Some(digest) = digest else { return blank() };
    let node = row.member.peer.node;
    let eligible = digest.position(node).is_some();
    let departing = row.status() == MemberStatus::Departing;
    if !eligible && !departing {
        return blank();
    }
    let owned = digest.parts_owned_by(node);
    let target = derive::share_fraction(owned);
    let frac = if departing && !eligible {
        0.0
    } else {
        scene.app.share(&digest.cache, node, target)
    };
    let fair = derive::fair_share(digest.k, digest.eligible.len());
    let mut spans = sharebar::bar_spans(frac, Some(fair), BAR, row.color(), look);
    spans.push(gap(1));
    spans.push(look.span(text::pad_left(&text::percent(target, 1), 6), Token::Text));
    spans.push(gap(1));
    spans.push(agreement_mark(scene, row, digest));
    spans.push(gap(1));
    spans
}

/// `✓` when the node reports the parts the observer computes, `↻` while it
/// differs, a space without metrics.
fn agreement_mark(scene: &Scene<'_>, row: &NodeRow<'_>, digest: &OwnershipDigest) -> Span<'static> {
    let look = scene.look;
    let reported =
        data::metrics_of(scene.model, row).and_then(|metrics| metrics.owned_parts(&digest.cache));
    match derive::agreement(reported, digest.parts_owned_by(row.member.peer.node)) {
        Agreement::Match => look.span("✓", Token::Ok),
        Agreement::Differs => look.span("↻", Token::Move),
        Agreement::Unknown => gap(1),
    }
}

/// The PEERS cell: the node's own count against the observer's.
fn peers_cell(scene: &Scene<'_>, row: &NodeRow<'_>) -> Span<'static> {
    let look = scene.look;
    let cell = |content: String, token| look.span(text::pad_right(&content, 6), token);
    match scene.model.peers(row.member.peer.gossip_addr) {
        Some(view) => {
            let content = format!("{}/{}", text::whole(view.reported), view.expected);
            cell(
                content,
                if view.amber {
                    Token::Warn
                } else {
                    Token::Muted
                },
            )
        }
        None => cell("—".to_owned(), Token::Faint),
    }
}

/// The OPS/S cell: a short sparkline and the rate.
fn ops_cell(scene: &Scene<'_>, row: &NodeRow<'_>) -> Vec<Span<'static>> {
    let look = scene.look;
    let Some(metrics) = data::metrics_of(scene.model, row) else {
        return vec![look.span(text::pad_left("—", 9), Token::Faint)];
    };
    let samples = metrics.ops().to_vec();
    let rate = samples.last().copied().unwrap_or(0.0);
    vec![
        Span::styled(spark(&samples, 3, look.braille), look.node(row.color())),
        gap(1),
        look.span(text::pad_left(&text::count(rate), 5), Token::Text),
    ]
}

/// The text of a row whose node is gone.
fn gone_text(scene: &Scene<'_>, row: &NodeRow<'_>) -> Span<'static> {
    let look = scene.look;
    let age = text::age(row.status_age(scene.ctx.wall));
    match row.status() {
        MemberStatus::Down => {
            let token = if row.status_age(scene.ctx.wall) < DOWN_HOLD {
                Token::Bad
            } else {
                Token::Faint
            };
            look.span(format!("down {age} · no departure seen"), token)
        }
        _ => look.span(format!("left {age}"), Token::Muted),
    }
}

/// One row of the table.
fn row_line(
    scene: &Scene<'_>,
    row: &NodeRow<'_>,
    cols: &Cols,
    selected: bool,
    digest: Option<&OwnershipDigest>,
    label_width: usize,
) -> Line<'static> {
    let look = scene.look;
    let gone = row.is_gone();
    let faded =
        gone && (row.status() == MemberStatus::Left || row.status_age(scene.ctx.wall) >= DOWN_HOLD);
    let text_token = if faded { Token::Muted } else { Token::Text };
    let mut spans: Vec<Span<'static>> = vec![
        if selected {
            Span::styled("▌", look.style(Token::Accent))
        } else {
            gap(1)
        },
        status_glyph(scene, row),
        gap(1),
    ];
    // NAME
    let mut name_style = if gone {
        look.style(Token::Muted)
    } else {
        look.node(row.color()).add_modifier(Modifier::BOLD)
    };
    let age = row.status_age(scene.ctx.wall);
    if scene.app.anim && row.status() == MemberStatus::Live && age < JOIN_PULSE {
        let glow = anim::blend(theme::BG, row.color(), 0.4 * anim::pulse(age, JOIN_PULSE));
        name_style = name_style.patch(look.bg(glow));
    }
    spans.push(Span::styled(
        text::pad_right(row.label(), label_width),
        name_style,
    ));
    spans.push(gap(1));
    spans.push(look.span(
        row.short_id(),
        if gone { Token::Faint } else { Token::Muted },
    ));
    let badge = if row.rejoined && age < REJOINED_FOR && !gone {
        look.span("↻", Token::Info)
    } else {
        gap(1)
    };
    spans.push(gap(1));
    spans.push(badge);
    spans.push(gap(cols
        .width_of(Col::Name)
        .saturating_sub(label_width + 7)));
    if gone {
        if cols.has(Col::Gossip) {
            spans.push(look.span(
                text::pad_right(&row.member.peer.gossip_addr.to_string(), cols.gossip),
                Token::Muted,
            ));
        }
        spans.push(gone_text(scene, row));
    } else {
        for &col in &cols.list[1..] {
            match col {
                Col::Name => {}
                Col::Gossip => spans.push(look.span(
                    text::pad_right(&row.member.peer.gossip_addr.to_string(), cols.gossip),
                    Token::Muted,
                )),
                Col::Up => {
                    // A departing node's clock runs from the departure, not
                    // from the join: it reads in the warning color.
                    let token = if row.status() == MemberStatus::Departing {
                        Token::Warn
                    } else {
                        text_token
                    };
                    spans.push(look.span(
                        text::pad_right(&text::uptime(row.status_age(scene.ctx.wall)), 6),
                        token,
                    ));
                }
                Col::Proto => {
                    let token = if row.member.peer.protocol == sundog::wire::PROTOCOL_VERSION {
                        Token::Muted
                    } else {
                        Token::Warn
                    };
                    spans.push(look.span(
                        text::pad_right(&row.member.peer.protocol.to_string(), 3),
                        token,
                    ));
                }
                Col::Caches => {
                    let pills = cache_pills(scene, row);
                    spans.extend(panel::pad_spans(pills, 9));
                }
                Col::Share => spans.extend(share_cell(scene, row, digest)),
                Col::Peers => spans.push(peers_cell(scene, row)),
                Col::Ops => spans.extend(ops_cell(scene, row)),
            }
        }
    }
    let line = Line::from(spans);
    if selected {
        line.style(look.selected())
    } else {
        line
    }
}

/// The header line of the table.
fn header_line(scene: &Scene<'_>, cols: &Cols, cache: Option<&str>) -> Line<'static> {
    let look = scene.look;
    let mut spans = vec![gap(PREFIX)];
    for &col in &cols.list {
        let title = match col {
            Col::Name => "NODE".to_owned(),
            Col::Gossip => "GOSSIP".to_owned(),
            Col::Up => "UP".to_owned(),
            Col::Proto => "P".to_owned(),
            Col::Caches => "CACHES".to_owned(),
            Col::Share => cache.map_or_else(|| "SHARE".to_owned(), |c| format!("SHARE {c}")),
            Col::Peers => "PEERS".to_owned(),
            Col::Ops => "OPS/S".to_owned(),
        };
        let width = cols.width_of(col);
        let cell = if col == Col::Ops {
            text::pad_left(&title, width)
        } else {
            text::pad_right(&title, width)
        };
        spans.push(look.span(cell, Token::Muted));
    }
    Line::from(spans)
}

/// The columns for the rows on screen.
fn columns(scene: &Scene<'_>, rows: &[NodeRow<'_>], width: usize) -> (Cols, usize) {
    let label = rows
        .iter()
        .map(|row| row.label().chars().count())
        .max()
        .unwrap_or(2)
        .clamp(2, 8);
    let gossip = rows
        .iter()
        .map(|row| row.member.peer.gossip_addr.to_string().chars().count())
        .max()
        .unwrap_or(15);
    (Cols::for_width(scene.kind, width, label, gossip), label)
}

/// How many rows the table wants inside its border: the header plus the rows,
/// at least `min` and at most `max`.
#[must_use]
pub fn wanted_height(scene: &Scene<'_>, min: u16, max: u16) -> u16 {
    let rows = u16::try_from(scene.rows().len()).unwrap_or(u16::MAX);
    (rows + 1).clamp(min, max) + 2
}

/// Draws the Members panel.
pub fn render(scene: &Scene<'_>, area: Rect, buf: &mut Buffer, focused: bool) {
    let look = scene.look;
    let rows = scene.rows();
    let block = panel::block(
        look,
        "Members",
        "gossip",
        vec![look.span(format!("{} seen", rows.len()), Token::Muted)],
        focused,
    );
    let inner = panel::draw(block, area, buf);
    if inner.height == 0 || inner.width == 0 {
        return;
    }
    let selected = scene.selected_addr();
    let cache = scene.app.ownership_cache(scene.model);
    let digest = cache
        .as_deref()
        .and_then(|name| scene.model.ownership(name));
    let (cols, label) = columns(scene, &rows, usize::from(inner.width));
    let mut lines = vec![header_line(scene, &cols, cache.as_deref())];
    let capacity = usize::from(inner.height).saturating_sub(1);
    let overflow = rows.len().saturating_sub(capacity);
    let shown = if overflow > 0 {
        capacity.saturating_sub(1)
    } else {
        rows.len()
    };
    for row in rows.iter().take(shown) {
        let is_selected = selected == Some(row.member.peer.gossip_addr);
        lines.push(row_line(scene, row, &cols, is_selected, digest, label));
    }
    if overflow > 0 {
        let hidden = rows.len() - shown;
        lines.push(Line::from(vec![
            gap(PREFIX),
            look.span(format!("+{hidden} more"), Token::Muted),
        ]));
    }
    for (index, line) in lines.into_iter().enumerate() {
        let Ok(dy) = u16::try_from(index) else { break };
        line.render(Rect::new(inner.x, inner.y + dy, inner.width, 1), buf);
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;
    use crate::app::{App, AppConfig};
    use crate::model::Model;
    use crate::model::testkit;
    use crate::ui::panel::row_text;
    use crate::ui::{Ctx, LayoutKind};

    fn draw(model: &Model, kind: LayoutKind, w: u16, h: u16) -> Vec<String> {
        let mut app = App::new(AppConfig::default());
        app.observe(model, Instant::now());
        app.snap();
        draw_app(&app, model, kind, w, h)
    }

    fn draw_app(app: &App, model: &Model, kind: LayoutKind, w: u16, h: u16) -> Vec<String> {
        let ctx = Ctx {
            now: model.now().unwrap(),
            wall: model.wall().unwrap(),
            elapsed: Duration::from_millis(100),
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
        render(&scene, area, &mut buf, true);
        (0..h).map(|y| row_text(&buf, y)).collect()
    }

    fn fixture() -> Model {
        testkit::fixture_model(Instant::now())
    }

    #[test]
    fn the_table_lists_every_node_with_its_columns() {
        let rows = draw(&fixture(), LayoutKind::Full, 84, 13);
        assert!(rows[0].starts_with("╭ Members · gossip "), "{}", rows[0]);
        assert!(rows[0].ends_with("8 seen ╮"), "{}", rows[0]);
        let header = &rows[1];
        for title in [
            "NODE", "GOSSIP", "UP", "CACHES", "SHARE it", "PEERS", "OPS/S",
        ] {
            assert!(header.contains(title), "{title} in {header}");
        }
        let first = &rows[2];
        assert!(first.contains("n1"), "{first}");
        assert!(first.contains("127.0.0.11:7946"), "{first}");
        assert!(first.contains("D R R R"), "{first}");
        assert!(first.contains('%'), "{first}");
        assert!(first.contains('▌'), "the first row is selected: {first}");
    }

    #[test]
    fn departing_down_and_left_rows_read_as_the_spec_shows() {
        let rows = draw(&fixture(), LayoutKind::Full, 84, 13);
        let departing = rows.iter().find(|r| r.contains("n6")).unwrap();
        assert!(departing.contains("0.0%"), "{departing}");
        assert!(
            departing.contains('◐') || departing.contains('◒'),
            "{departing}"
        );
        let down = rows.iter().find(|r| r.contains("n7")).unwrap();
        assert!(
            down.contains("✖") && down.contains("down 10s · no departure seen"),
            "{down}"
        );
        let left = rows.iter().find(|r| r.contains("n8")).unwrap();
        assert!(left.contains("○") && left.contains("left 10s"), "{left}");
    }

    #[test]
    fn without_metrics_the_peer_and_ops_columns_are_dashes() {
        let rows = draw(&fixture(), LayoutKind::Full, 84, 13);
        assert!(rows[2].contains('—'), "{}", rows[2]);
        assert!(!rows[2].contains('✓'), "no agreement mark without metrics");
    }

    #[test]
    fn a_compact_table_drops_the_gossip_column() {
        let rows = draw(&fixture(), LayoutKind::Compact, 98, 13);
        assert!(!rows[1].contains("GOSSIP"), "{}", rows[1]);
        assert!(rows[1].contains("CACHES"), "{}", rows[1]);
    }

    #[test]
    fn a_narrow_table_keeps_only_name_share_and_ops() {
        let rows = draw(&fixture(), LayoutKind::Narrow, 78, 13);
        let header = &rows[1];
        assert!(header.contains("NODE") && header.contains("SHARE it") && header.contains("OPS/S"));
        for title in ["GOSSIP", "UP", "CACHES", "PEERS"] {
            assert!(!header.contains(title), "{title} in {header}");
        }
    }

    #[test]
    fn a_table_that_cannot_fit_every_column_drops_them_in_order() {
        let wide = Cols::for_width(LayoutKind::Full, 200, 2, 15);
        assert_eq!(wide.list.len(), 8);
        let fewer = Cols::for_width(LayoutKind::Full, 70, 2, 15);
        assert!(!fewer.list.contains(&Col::Gossip));
        assert!(fewer.total() <= 70);
        let least = Cols::for_width(LayoutKind::Full, 40, 2, 15);
        assert!(least.list.contains(&Col::Name) && least.list.contains(&Col::Share));
        assert!(!least.list.contains(&Col::Gossip));
        assert_eq!(
            Cols::for_width(LayoutKind::Narrow, 500, 2, 15).list.len(),
            3
        );
    }

    #[test]
    fn column_widths_add_up_to_the_table_width() {
        let cols = Cols::for_width(LayoutKind::Full, 200, 2, 15);
        let sum: usize = cols.list.iter().map(|&c| cols.width_of(c)).sum();
        assert_eq!(cols.total(), PREFIX + sum);
        assert!(cols.total() <= 82, "{}", cols.total());
        let long = Cols::for_width(LayoutKind::Full, 200, 20, 40);
        assert_eq!(long.label, 8);
        assert_eq!(long.gossip, 41);
    }

    #[test]
    fn a_short_panel_shows_how_many_rows_it_hides() {
        let rows = draw(&fixture(), LayoutKind::Full, 84, 7);
        assert!(rows.iter().any(|r| r.contains("more")), "{rows:?}");
        let last_row = &rows[5];
        assert!(last_row.contains("+5 more"), "{last_row}");
    }

    #[test]
    fn the_wanted_height_follows_the_row_count_within_bounds() {
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
        assert_eq!(wanted_height(&scene, 7, 14), 9 + 2);
        assert_eq!(wanted_height(&scene, 12, 14), 14);
        assert_eq!(wanted_height(&scene, 2, 5), 7);
    }

    #[test]
    fn a_joining_row_pulses_and_shows_the_joined_glyph() {
        let model = fixture();
        let app = App::new(AppConfig::default());
        let mut ctx = Ctx {
            now: model.now().unwrap(),
            wall: model.wall().unwrap(),
            elapsed: Duration::ZERO,
        };
        // n4 first showed at 5 s; 1 s later it is joining, 15 s later it is not.
        ctx.wall = std::time::UNIX_EPOCH + Duration::from_secs(6);
        let scene = Scene {
            app: &app,
            model: &model,
            ctx: &ctx,
            look: app.look(),
            kind: LayoutKind::Full,
        };
        let rows = scene.rows();
        let n4 = rows.iter().find(|r| r.label() == "n4").unwrap();
        let line = row_line(
            &scene,
            n4,
            &Cols::for_width(LayoutKind::Full, 82, 2, 15),
            false,
            None,
            2,
        );
        assert_eq!(line.spans[1].content, "✚");
        assert!(line.spans[3].style.bg.is_some(), "the name glows");
        let mut later = ctx;
        later.wall = std::time::UNIX_EPOCH + Duration::from_secs(20);
        let scene = Scene {
            ctx: &later,
            ..scene
        };
        let rows = scene.rows();
        let n4 = rows.iter().find(|r| r.label() == "n4").unwrap();
        let settled = row_line(
            &scene,
            n4,
            &Cols::for_width(LayoutKind::Full, 82, 2, 15),
            false,
            None,
            2,
        );
        assert_eq!(settled.spans[1].content, "●");
        assert!(settled.spans[3].style.bg.is_none());
    }

    #[test]
    fn with_metrics_the_rows_show_peers_agreement_and_operations() {
        let model = testkit::fixture_model_with_metrics(Instant::now());
        let rows = draw(&model, LayoutKind::Full, 84, 13);
        let n1 = rows.iter().find(|r| r.contains("n1")).unwrap();
        assert!(n1.contains("5/5"), "peers: {n1}");
        assert!(n1.contains('✓'), "{n1}");
        assert!(n1.contains('⣿') || n1.contains('⣤'), "the ops spark: {n1}");
        let n3 = rows.iter().find(|r| r.contains("n3")).unwrap();
        assert!(n3.contains('↻'), "n3 reports fewer parts: {n3}");
        assert!(!n1.contains("—"), "{n1}");
    }
}

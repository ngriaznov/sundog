//! The Ownership panel: the mosaic of who leads each bucket, a share bar per
//! eligible node and the last view change, for one `Distributed` cache.

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::widgets::Widget;
use sundog::store::Mode;

use super::data::{self, ViewState};
use super::look::Token;
use super::panel::{self, gap};
use super::theme::Rgb;
use super::widgets::mosaic::Mosaic;
use super::widgets::sharebar::{self, bar_spans};
use super::{LayoutKind, Scene, text};
use crate::model::derive::{self, Agreement, PART_SPACE};
use crate::model::ownership::OwnershipDigest;

/// The mosaic is eight cell rows tall.
const MOSAIC_ROWS: u16 = 8;

/// The width of the full mosaic.
const FULL_COLUMNS: u16 = 64;

/// The width of the compact mosaic.
const COMPACT_COLUMNS: u16 = 32;

/// The rows the panel wants inside its border at the screen class `kind`
/// when `nodes` nodes are eligible.
#[must_use]
pub fn wanted_height(kind: LayoutKind, nodes: usize) -> u16 {
    if kind == LayoutKind::Narrow {
        let rows = u16::try_from(nodes).unwrap_or(u16::MAX).clamp(1, 5);
        rows + 2 + 2
    } else {
        MOSAIC_ROWS + 2
    }
}

/// One eligible node as the panel lists it.
struct Entry {
    label: String,
    color: Rgb,
    owned: usize,
    reported: Option<f64>,
    node: sundog::NodeId,
}

/// The eligible nodes in slot order.
fn entries(scene: &Scene<'_>, digest: &OwnershipDigest) -> Vec<Entry> {
    let mut list: Vec<(usize, Entry)> = digest
        .counts
        .iter()
        .map(|&(node, owned)| {
            let tag = data::tag_of(scene.model, node);
            let slot = scene
                .model
                .snapshot()
                .and_then(|s| {
                    s.members
                        .iter()
                        .filter(|member| member.peer.node == node)
                        .max_by_key(|member| member.peer.incarnation)
                })
                .and_then(|member| scene.model.slots().get(member.peer.gossip_addr))
                .map_or(usize::MAX, |slot| slot.index);
            let reported = data::all_node_rows(scene.model)
                .iter()
                .find(|row| row.member.peer.node == node)
                .and_then(|row| data::metrics_of(scene.model, row))
                .and_then(|metrics| metrics.owned_parts(&digest.cache));
            (
                slot,
                Entry {
                    label: tag.label.to_string(),
                    color: tag.color,
                    owned,
                    reported,
                    node,
                },
            )
        })
        .collect();
    list.sort_by_key(|(slot, _)| *slot);
    list.into_iter().map(|(_, entry)| entry).collect()
}

/// How many eligible nodes report the parts the observer computes, and how
/// many report at all.
fn agreement_counts(entries: &[Entry]) -> (usize, usize) {
    let mut agree = 0;
    let mut reporting = 0;
    for entry in entries {
        if entry.reported.is_some() {
            reporting += 1;
        }
        if derive::agreement(entry.reported, entry.owned) == Agreement::Match {
            agree += 1;
        }
    }
    (agree, reporting)
}

/// The right-aligned stats of the title, the longest of three forms that
/// fits in `room` cells: the sum and the agreement, the sum alone, or the
/// eligible count.
fn stats(
    scene: &Scene<'_>,
    digest: &OwnershipDigest,
    entries: &[Entry],
    room: usize,
) -> Vec<Span<'static>> {
    let look = scene.look;
    let owners = usize::from(digest.k.get()).min(digest.eligible.len().max(1));
    let total: usize = digest.counts.iter().map(|&(_, count)| count).sum();
    let sum = format!(
        "Σ {} = {} × {owners} · {} eligible",
        text::thousands(u64::try_from(total).unwrap_or(0)),
        text::thousands(u64::try_from(PART_SPACE).unwrap_or(0)),
        digest.eligible.len()
    );
    let (agree, reporting) = agreement_counts(entries);
    let mut full = vec![look.span(format!("{sum} · "), Token::Muted)];
    if reporting == 0 {
        full.push(look.span("no node reports", Token::Faint));
    } else {
        let token = if agree == entries.len() {
            Token::Ok
        } else {
            Token::Move
        };
        full.push(look.span("reported ", Token::Muted));
        full.push(look.span(format!("✓ {agree}/{}", entries.len()), token));
    }
    let short = vec![look.span(
        format!(
            "Σ {} · {} eligible",
            text::thousands(u64::try_from(total).unwrap_or(0)),
            digest.eligible.len()
        ),
        Token::Muted,
    )];
    let shortest = vec![look.span(format!("{} eligible", digest.eligible.len()), Token::Muted)];
    [full, short]
        .into_iter()
        .find(|spans| panel::width_of(spans) <= room)
        .unwrap_or(shortest)
}

/// The title: the cache, its mode and the `computed` tag.
fn title(scene: &Scene<'_>, digest: &OwnershipDigest) -> Vec<Span<'static>> {
    let look = scene.look;
    let mode = data::mode_name(Mode::Distributed { owners: digest.k });
    let mut spans = vec![
        gap(1),
        panel::title_span(look, "Ownership"),
        look.span(" · ", Token::Faint),
        Span::styled(
            digest.cache.to_string(),
            look.style(Token::Accent).add_modifier(Modifier::BOLD),
        ),
        look.span(format!(" · {mode}"), Token::Muted),
    ];
    spans.extend(panel::tag_spans(look, "computed"));
    spans.push(gap(1));
    spans
}

/// The line of one node: glyph, label, bar, parts, percentage and whether
/// the node reports the same.
fn node_line(
    scene: &Scene<'_>,
    digest: &OwnershipDigest,
    entry: &Entry,
    label_width: usize,
    bar: usize,
) -> Line<'static> {
    let look = scene.look;
    let target = derive::share_fraction(entry.owned);
    let frac = scene.app.share(&digest.cache, entry.node, target);
    let fair = derive::fair_share(digest.k, digest.eligible.len());
    let mut spans = vec![
        look.node_span("●", entry.color),
        gap(1),
        Span::styled(
            text::pad_right(&entry.label, label_width),
            look.node(entry.color).add_modifier(Modifier::BOLD),
        ),
        gap(2),
    ];
    spans.extend(bar_spans(frac, Some(fair), bar, entry.color, look));
    spans.push(gap(2));
    spans.push(look.span(
        text::pad_left(&text::thousands(u64::try_from(entry.owned).unwrap_or(0)), 7),
        Token::Text,
    ));
    spans.push(gap(2));
    spans.push(look.span(text::pad_left(&text::percent(target, 1), 6), Token::Muted));
    spans.push(gap(2));
    match derive::agreement(entry.reported, entry.owned) {
        Agreement::Match => spans.push(look.span("✓", Token::Ok)),
        Agreement::Differs => spans.push(look.span(
            format!("↻ {}", text::whole(entry.reported.unwrap_or(0.0))),
            Token::Move,
        )),
        Agreement::Unknown => {}
    }
    Line::from(spans)
}

/// The stacked share strip and the fair-share legend.
fn strip_line(
    scene: &Scene<'_>,
    digest: &OwnershipDigest,
    entries: &[Entry],
    label_width: usize,
    bar: usize,
) -> Line<'static> {
    let look = scene.look;
    let counts: Vec<u64> = entries
        .iter()
        .map(|entry| u64::try_from(entry.owned).unwrap_or(0))
        .collect();
    let mut spans = vec![gap(label_width + 4)];
    for (index, cells) in sharebar::segments(&counts, bar).into_iter().enumerate() {
        spans.push(Span::styled(
            "█".repeat(usize::from(cells)),
            look.node(entries[index].color),
        ));
    }
    let fair = derive::fair_share(digest.k, digest.eligible.len());
    spans.push(look.span("  fair share ┊ ", Token::Muted));
    spans.push(look.span(text::percent(fair, 1), Token::Text));
    Line::from(spans)
}

/// The last view change and whether it has settled.
fn view_line(scene: &Scene<'_>, state: &ViewState) -> Line<'static> {
    let look = scene.look;
    let mut spans = vec![look.span("⇄ ", Token::Move)];
    match state.changed_at {
        Some(at) => {
            spans.push(look.span(format!("view change {}", text::clock(at)), Token::Muted));
            if let Some(moved) = state.moved.filter(|moved| *moved > 0) {
                spans.push(look.span(
                    format!(
                        " · {} parts moved",
                        text::thousands(u64::try_from(moved).unwrap_or(0))
                    ),
                    Token::Muted,
                ));
            }
        }
        None => spans.push(look.span("no view change seen yet", Token::Muted)),
    }
    if state.settled {
        spans.push(look.span(" · ", Token::Faint));
        spans.push(look.span("✔ settled", Token::Ok));
    } else {
        let waited = state
            .since
            .map_or(String::new(), |since| format!(" {}", text::seconds(since)));
        spans.push(look.span(" · ", Token::Faint));
        spans.push(look.span(format!("settling{waited}"), Token::Move));
    }
    if state.gossip_only {
        spans.push(look.span(" (gossip only)", Token::Muted));
    }
    Line::from(spans)
}

/// The lines of the right column for `height` rows.
fn column(
    scene: &Scene<'_>,
    digest: &OwnershipDigest,
    state: Option<&ViewState>,
    entries: &[Entry],
    width: usize,
    height: usize,
) -> Vec<Line<'static>> {
    let label_width = entries
        .iter()
        .map(|entry| entry.label.chars().count())
        .max()
        .unwrap_or(2)
        .clamp(2, 8);
    let fixed = label_width + 32;
    let bar = width.saturating_sub(fixed).clamp(4, 28);
    let tail = 1 + usize::from(state.is_some() && height >= 6);
    let spare = height.saturating_sub(tail);
    let (listed, more) = if entries.len() <= spare {
        (entries.len(), 0)
    } else {
        (
            spare.saturating_sub(1),
            entries.len() - spare.saturating_sub(1),
        )
    };
    let mut lines: Vec<Line<'static>> = entries
        .iter()
        .take(listed)
        .map(|entry| node_line(scene, digest, entry, label_width, bar))
        .collect();
    if more > 0 {
        lines.push(Line::from(vec![
            gap(label_width + 4),
            scene.look.span(format!("+{more} more"), Token::Muted),
        ]));
    }
    if lines.len() + tail < height {
        lines.push(Line::default());
    }
    lines.push(strip_line(scene, digest, entries, label_width, bar));
    if let Some(state) = state.filter(|_| height >= 6) {
        lines.push(view_line(scene, state));
    }
    lines
}

/// Draws the Ownership panel.
pub fn render(scene: &Scene<'_>, area: Rect, buf: &mut Buffer, focused: bool) {
    let look = scene.look;
    let cache = scene.app.ownership_cache(scene.model);
    let digest = cache
        .as_deref()
        .and_then(|name| scene.model.ownership(name));
    let Some(digest) = digest else {
        let block = panel::block(look, "Ownership", "computed", Vec::new(), focused);
        let inner = panel::draw(block, area, buf);
        let message = if cache.is_some() {
            "computing ownership"
        } else {
            "no Distributed cache advertised"
        };
        panel::centered(
            vec![Line::from(look.span(message, Token::Muted))],
            inner,
            buf,
        );
        return;
    };
    let entries = entries(scene, digest);
    let heading = title(scene, digest);
    let room = usize::from(area.width).saturating_sub(panel::width_of(&heading) + 6);
    let block = panel::block_with(look, heading, stats(scene, digest, &entries, room), focused);
    let inner = panel::draw(block, area, buf);
    if inner.width == 0 || inner.height == 0 {
        return;
    }
    let state = data::view_state(scene.model, &digest.cache, scene.ctx.wall);
    let mosaic_width = match scene.kind {
        LayoutKind::Full => FULL_COLUMNS,
        LayoutKind::Compact => COMPACT_COLUMNS,
        _ => 0,
    };
    let column_x = if mosaic_width > 0 {
        let palette: Vec<Rgb> = digest
            .eligible
            .iter()
            .map(|&node| data::tag_of(scene.model, node).color)
            .collect();
        let mosaic_area = Rect::new(
            inner.x + 1,
            inner.y,
            mosaic_width.min(inner.width),
            MOSAIC_ROWS.min(inner.height),
        );
        let mut mosaic = Mosaic::new(&digest.lead, &palette, look.mode);
        if mosaic_width == COMPACT_COLUMNS {
            mosaic = mosaic.compact(&digest.lead_compact);
        }
        if let Some((prev, intensity)) = scene.app.flash(&digest.cache, scene.ctx.now) {
            mosaic = mosaic.flash(prev, intensity);
        }
        mosaic.render(mosaic_area, buf);
        inner.x + 1 + mosaic_width + 3
    } else {
        inner.x + 1
    };
    let right = Rect::new(
        column_x,
        inner.y,
        (inner.x + inner.width).saturating_sub(column_x),
        inner.height,
    );
    if right.width < 24 {
        return;
    }
    let lines = column(
        scene,
        digest,
        state.as_ref(),
        &entries,
        usize::from(right.width),
        usize::from(right.height),
    );
    panel::lines(lines, right, buf);
}

/// The strip colors, for the Caches view.
#[must_use]
pub fn palette(scene: &Scene<'_>, digest: &OwnershipDigest) -> Vec<Rgb> {
    digest
        .eligible
        .iter()
        .map(|&node| data::tag_of(scene.model, node).color)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;
    use crate::app::{App, AppConfig};
    use crate::model::Model;
    use crate::model::testkit;
    use crate::ui::Ctx;
    use crate::ui::panel::row_text;
    use crate::ui::theme;

    fn draw(model: &Model, kind: LayoutKind, w: u16, h: u16) -> Vec<String> {
        let mut app = App::new(AppConfig::default());
        app.observe(model, Instant::now());
        app.snap();
        let ctx = Ctx {
            now: model.now().unwrap(),
            wall: model.wall().unwrap(),
            elapsed: Duration::ZERO,
        };
        let scene = Scene {
            app: &app,
            model,
            ctx: &ctx,
            look: app.look(),
            kind,
        };
        let area = Rect::new(0, 0, w, h);
        let mut buf = Buffer::empty(area);
        render(&scene, area, &mut buf, false);
        (0..h).map(|y| row_text(&buf, y)).collect()
    }

    fn fixture() -> Model {
        testkit::fixture_model(Instant::now())
    }

    #[test]
    fn the_panel_titles_the_cache_its_mode_and_the_totals() {
        let rows = draw(&fixture(), LayoutKind::Full, 140, 10);
        let top = &rows[0];
        assert!(
            top.starts_with("╭ Ownership · it · distributed k=2 · computed "),
            "{top}"
        );
        assert!(
            top.contains("Σ 131,072 = 65,536 × 2 · 5 eligible · no node reports"),
            "{top}"
        );
    }

    #[test]
    fn the_mosaic_fills_eight_rows_with_half_blocks() {
        let rows = draw(&fixture(), LayoutKind::Full, 140, 10);
        for row in &rows[1..9] {
            let blocks = row.chars().filter(|c| *c == '▀').count();
            assert_eq!(blocks, 64, "{row}");
        }
    }

    #[test]
    fn each_eligible_node_gets_a_share_line_and_the_panel_a_strip_and_a_view_line() {
        let rows = draw(&fixture(), LayoutKind::Full, 140, 10);
        let body = rows[1..9].join("\n");
        for label in ["n1", "n2", "n3", "n4", "n5"] {
            assert!(body.contains(&format!("● {label}")), "{label} in\n{body}");
        }
        assert!(!body.contains("● n6"), "a departing node is not eligible");
        assert!(body.contains("fair share ┊ 40.0%"), "{body}");
        assert!(body.contains("⇄ view change 00:00:10"), "{body}");
        assert!(body.contains("parts moved"), "{body}");
        assert!(body.contains("✔ settled"), "{body}");
        assert!(body.contains("(gossip only)"), "{body}");
    }

    #[test]
    fn the_shares_of_the_nodes_sum_to_the_part_space_times_owners() {
        let model = fixture();
        let digest = model.ownership("it").unwrap();
        let total: usize = digest.counts.iter().map(|&(_, c)| c).sum();
        assert_eq!(total, 2 * PART_SPACE);
        let rows = draw(&model, LayoutKind::Full, 140, 10);
        assert!(rows[1..9].iter().any(|r| r.contains('%')));
    }

    #[test]
    fn a_compact_panel_halves_the_mosaic() {
        let rows = draw(&fixture(), LayoutKind::Compact, 110, 10);
        for row in &rows[1..9] {
            assert_eq!(row.chars().filter(|c| *c == '▀').count(), 32, "{row}");
        }
        assert!(rows[1..9].join("\n").contains("● n1"));
    }

    #[test]
    fn a_narrow_panel_has_no_mosaic_and_lists_the_nodes() {
        let rows = draw(&fixture(), LayoutKind::Narrow, 78, 9);
        assert!(rows.iter().all(|r| !r.contains('▀')));
        assert!(rows.join("\n").contains("● n1"));
        assert_eq!(wanted_height(LayoutKind::Narrow, 3), 7);
        assert_eq!(wanted_height(LayoutKind::Narrow, 50), 9);
        assert_eq!(wanted_height(LayoutKind::Full, 3), 10);
    }

    #[test]
    fn many_nodes_collapse_into_a_more_line() {
        let lines = {
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
            let digest = model.ownership("it").unwrap();
            let list = entries(&scene, digest);
            column(&scene, digest, None, &list, 70, 4)
        };
        let text: Vec<String> = lines
            .iter()
            .map(|l| l.spans.iter().map(|s| s.content.as_ref()).collect())
            .collect();
        assert_eq!(text.len(), 4, "{text:?}");
        assert!(text[2].contains("+3 more"), "{text:?}");
        assert!(text[3].contains("fair share"), "{text:?}");
    }

    #[test]
    fn without_a_distributed_cache_the_panel_says_so() {
        let mut model = Model::new();
        let now = Instant::now();
        let snapshot = sundog::observe::ClusterSnapshot::new(
            "c",
            vec![testkit::member_with(
                1,
                0,
                1,
                sundog::observe::MemberStatus::Live,
                &[("r", sundog::store::Mode::Replicated)],
            )],
            0,
        );
        model.apply(
            crate::source::Update::Snapshot(std::sync::Arc::new(snapshot), now),
            now,
            std::time::SystemTime::UNIX_EPOCH,
        );
        let rows = draw(&model, LayoutKind::Full, 100, 10);
        assert!(rows.join("\n").contains("no Distributed cache advertised"));
    }

    #[test]
    fn a_cache_without_a_digest_yet_reads_computing() {
        let mut model = Model::new();
        let now = Instant::now();
        model.apply(
            crate::source::Update::Snapshot(std::sync::Arc::new(testkit::snapshot(3)), now),
            now,
            std::time::SystemTime::UNIX_EPOCH,
        );
        let rows = draw(&model, LayoutKind::Full, 100, 10);
        assert!(rows.join("\n").contains("computing ownership"));
    }

    #[test]
    fn the_palette_follows_the_eligible_nodes() {
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
        let digest = model.ownership("it").unwrap();
        let colors = palette(&scene, digest);
        assert_eq!(colors.len(), 5);
        assert_eq!(colors[0], theme::NODE_COLORS[0]);
    }
}

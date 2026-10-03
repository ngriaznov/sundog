//! The header and tab row: cluster name, member counters and the clock.

use std::collections::BTreeSet;
use std::time::Duration;

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Widget};
use sundog::observe::MemberStatus;
use sundog::wire::PROTOCOL_VERSION;

use super::look::Token;
use super::panel::{gap, width_of};
use super::theme::{self, Rgb};
use super::{Scene, View, anim, data, eventlog, text};

/// The frame the spinner shows with motion off.
const STILL_SPINNER: char = '⣾';

/// The logo: the sun and its two parhelia, then the name.
fn logo(scene: &Scene<'_>) -> Vec<Span<'static>> {
    let look = scene.look;
    let dim: Rgb = anim::blend(theme::ACCENT, theme::BG, 0.45);
    vec![
        Span::styled("●", look.style(Token::Accent).add_modifier(Modifier::BOLD)),
        Span::styled("••", look.fg(dim)),
        gap(1),
        Span::styled(
            "sundog lens",
            look.style(Token::Text).add_modifier(Modifier::BOLD),
        ),
    ]
}

/// The member counters, each shown only when nonzero.
fn counters(scene: &Scene<'_>) -> Vec<Span<'static>> {
    let look = scene.look;
    let mut counts = [0usize; 4];
    for row in scene.rows() {
        match row.status() {
            MemberStatus::Live => counts[0] += 1,
            MemberStatus::Departing => counts[1] += 1,
            MemberStatus::Down => counts[2] += 1,
            MemberStatus::Left => counts[3] += 1,
            _ => {}
        }
    }
    let table = [
        ('●', "live", Token::Ok),
        ('◐', "leaving", Token::Warn),
        ('✖', "down", Token::Bad),
        ('○', "left", Token::Muted),
    ];
    let mut spans = Vec::new();
    for (&count, (mark, label, token)) in counts.iter().zip(table) {
        if count == 0 {
            continue;
        }
        if !spans.is_empty() {
            spans.push(gap(2));
        }
        spans.push(look.span(mark.to_string(), token));
        spans.push(look.span(format!(" {count} {label}"), Token::Text));
    }
    spans
}

/// The protocols the live members speak: `proto 7`, or `proto 6·7 ⚠` when
/// they differ among themselves or from this build.
fn protocols(scene: &Scene<'_>) -> Vec<Span<'static>> {
    let look = scene.look;
    let seen: BTreeSet<u16> = scene
        .rows()
        .iter()
        .filter(|row| row.status().is_live())
        .map(|row| row.member.peer.protocol)
        .collect();
    if seen.is_empty() {
        return Vec::new();
    }
    let joined = seen
        .iter()
        .map(u16::to_string)
        .collect::<Vec<_>>()
        .join("·");
    if seen.len() == 1 && seen.contains(&PROTOCOL_VERSION) {
        vec![look.span(format!("proto {joined}"), Token::Muted)]
    } else {
        vec![Span::styled(
            format!("proto {joined} ⚠"),
            look.style(Token::Warn).add_modifier(Modifier::BOLD),
        )]
    }
}

/// The observer's address and the anonymous gossip participants it counts.
fn observer(scene: &Scene<'_>) -> Vec<Span<'static>> {
    let anonymous = scene.model.snapshot().map_or(0, |s| s.anonymous);
    let addr = scene
        .app
        .config()
        .observer
        .map_or_else(|| "observer".to_owned(), |addr| format!("observer {addr}"));
    vec![
        scene
            .look
            .span(format!("{addr} · {anonymous} anon"), Token::Muted),
    ]
}

/// The clock and the spinner, or the frozen chip.
fn clock(scene: &Scene<'_>) -> Vec<Span<'static>> {
    let look = scene.look;
    let spinner = if scene.app.anim {
        anim::spinner_frame(scene.ctx.elapsed)
    } else {
        STILL_SPINNER
    };
    let mut spans = Vec::new();
    if scene.app.is_frozen() {
        spans.push(Span::styled(
            "‖ frozen",
            look.style(Token::Warn).add_modifier(Modifier::BOLD),
        ));
        spans.push(gap(2));
    } else {
        spans.push(look.span(spinner.to_string(), Token::Accent));
        spans.push(gap(1));
    }
    spans.push(look.span(format!("{} UTC", text::clock(scene.ctx.wall)), Token::Text));
    spans.push(gap(1));
    spans
}

/// Draws the header row.
pub fn render(scene: &Scene<'_>, area: Rect, buf: &mut Buffer) {
    let look = scene.look;
    let right = clock(scene);
    let right_width = width_of(&right);
    let available = usize::from(area.width).saturating_sub(right_width + 1);

    let mut left = vec![gap(1)];
    left.extend(logo(scene));
    let cluster = scene.model.snapshot().map(|s| s.cluster.to_string());
    if let Some(name) = cluster.filter(|name| !name.is_empty()) {
        left.push(gap(2));
        left.push(Span::styled(
            name,
            look.style(Token::Accent).add_modifier(Modifier::BOLD),
        ));
    }
    // Optional segments, most important first; each goes in only if it fits.
    for segment in [counters(scene), protocols(scene), observer(scene)] {
        if segment.is_empty() {
            continue;
        }
        if width_of(&left) + 3 + width_of(&segment) > available {
            break;
        }
        left.push(gap(3));
        left.extend(segment);
    }
    let filler = usize::from(area.width).saturating_sub(width_of(&left) + right_width);
    left.push(gap(filler));
    left.extend(right);
    Paragraph::new(Line::from(left))
        .style(look.surface())
        .render(area, buf);
}

/// The scrape health at the right of the tab row: how many exporters answer.
fn metrics_status(scene: &Scene<'_>) -> Vec<Span<'static>> {
    let look = scene.look;
    match (
        data::exporter_summary(scene.model),
        scene.app.config().scrape_interval,
    ) {
        (Some((working, live)), interval) => {
            let every = interval.map_or(String::new(), |every| {
                format!(" · {}", interval_text(every))
            });
            let token = if working == live {
                Token::Muted
            } else {
                Token::Warn
            };
            vec![look.span(format!("metrics {working}/{live}{every}"), token)]
        }
        (None, Some(_)) => vec![look.span("metrics waiting", Token::Muted)],
        (None, None) => vec![look.span("no metrics", Token::Muted)],
    }
}

/// The ownership view of the cache the Overview shows, in the tab row.
fn view_status(scene: &Scene<'_>) -> Vec<Span<'static>> {
    let look = scene.look;
    let Some(cache) = scene.app.ownership_cache(scene.model) else {
        return Vec::new();
    };
    let Some(state) = data::view_state(scene.model, &cache, scene.ctx.wall) else {
        return Vec::new();
    };
    let mut spans = Vec::new();
    spans.push(Span::styled(
        cache.to_string(),
        look.style(Token::Text).add_modifier(Modifier::BOLD),
    ));
    let kind = if state.ranks_parts { "part" } else { "bucket" };
    spans.push(look.span(
        format!(" · {kind} view {} ", eventlog::view_hash(state.hash)),
        Token::Muted,
    ));
    if state.settled {
        spans.push(look.span("✔ settled", Token::Ok));
    } else {
        let waited = state
            .since
            .map_or(String::new(), |since| format!(" {}", text::seconds(since)));
        spans.push(look.span(format!("↻ settling{waited}"), Token::Move));
    }
    if state.gossip_only {
        spans.push(look.span(" (gossip only)", Token::Muted));
    }
    spans.push(gap(1));
    spans
}

/// The right side of the tab row: the scrape health and the ownership view
/// together, the scrape health alone, or nothing, whichever is the most that
/// fits in `room` cells.
fn tab_status(scene: &Scene<'_>, room: usize) -> Vec<Span<'static>> {
    let metrics = metrics_status(scene);
    let view = view_status(scene);
    let mut both = metrics.clone();
    if !view.is_empty() {
        both.push(gap(5));
        both.extend(view);
    }
    let mut alone = metrics;
    alone.push(gap(1));
    [both, alone]
        .into_iter()
        .find(|spans| width_of(spans) + 2 <= room)
        .unwrap_or_default()
}

/// A scrape interval as `1 s` or `500 ms`.
fn interval_text(every: Duration) -> String {
    if every.as_millis().is_multiple_of(1000) {
        format!("{} s", every.as_secs())
    } else {
        format!("{} ms", every.as_millis())
    }
}

/// Draws the tab row: the tabs, the active one as an amber pill, and the
/// scrape and ownership status at the right.
pub fn render_tabs(scene: &Scene<'_>, area: Rect, buf: &mut Buffer) {
    let look = scene.look;
    let mut left = vec![gap(1)];
    for (index, view) in View::ALL.into_iter().enumerate() {
        if index > 0 {
            left.push(gap(1));
        }
        let number = index + 1;
        if view == scene.app.view {
            left.push(Span::styled(
                format!(" {number} {} ", view.title()),
                look.pill(),
            ));
        } else {
            left.push(look.span(format!(" {number} "), Token::Muted));
            left.push(look.span(format!("{} ", view.title()), Token::Text));
        }
    }
    let width = usize::from(area.width);
    let right = tab_status(scene, width.saturating_sub(width_of(&left)));
    let used = width_of(&left) + width_of(&right);
    if used <= width {
        left.push(gap(width - used));
        left.extend(right);
    }
    Paragraph::new(Line::from(left)).render(area, buf);
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;
    use crate::app::{App, AppConfig};
    use crate::model::testkit;
    use crate::ui::panel::row_text;
    use crate::ui::{Ctx, LayoutKind, Scene};

    fn draw_with(app: &App, model: &crate::model::Model, width: u16) -> (String, String) {
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
        let area = Rect::new(0, 0, width, 2);
        let mut buf = Buffer::empty(area);
        render(&scene, Rect::new(0, 0, width, 1), &mut buf);
        render_tabs(&scene, Rect::new(0, 1, width, 1), &mut buf);
        (row_text(&buf, 0), row_text(&buf, 1))
    }

    fn fixture() -> crate::model::Model {
        testkit::fixture_model(Instant::now())
    }

    #[test]
    fn the_header_shows_logo_cluster_counters_protocol_and_clock() {
        let app = App::new(AppConfig::default());
        let (header, _) = draw_with(&app, &fixture(), 140);
        assert!(header.starts_with(" ●•• sundog lens  fixture"), "{header}");
        assert!(header.contains("● 5 live"), "{header}");
        assert!(header.contains("◐ 1 leaving"), "{header}");
        assert!(header.contains("✖ 1 down"), "{header}");
        assert!(header.contains("○ 1 left"), "{header}");
        assert!(
            header.contains(&format!("proto {PROTOCOL_VERSION}")),
            "{header}"
        );
        assert!(header.contains("observer · 0 anon"), "{header}");
        assert!(header.ends_with("00:00:20 UTC"), "{header}");
        assert!(header.contains('⣋') || header.contains('⠋'), "{header}");
    }

    #[test]
    fn a_counter_with_nothing_to_count_is_left_out() {
        let app = App::new(AppConfig::default());
        let model = {
            let mut model = crate::model::Model::new();
            let now = Instant::now();
            model.apply(
                crate::source::Update::Snapshot(std::sync::Arc::new(testkit::snapshot(2)), now),
                now,
                std::time::SystemTime::UNIX_EPOCH,
            );
            model
        };
        let (header, _) = draw_with(&app, &model, 140);
        assert!(header.contains("● 2 live"), "{header}");
        assert!(
            !header.contains("leaving") && !header.contains("down"),
            "{header}"
        );
    }

    #[test]
    fn a_mixed_protocol_cluster_warns() {
        use sundog::membership::Peer;
        let mut model = crate::model::Model::new();
        let now = Instant::now();
        let mut old = testkit::member(2, MemberStatus::Live);
        old = sundog::observe::Member::new(
            Peer {
                protocol: PROTOCOL_VERSION - 1,
                ..old.peer.clone()
            },
            old.status,
            old.since,
            old.caches.clone(),
        );
        let snapshot = sundog::observe::ClusterSnapshot::new(
            "c",
            vec![testkit::member(1, MemberStatus::Live), old],
            0,
        );
        model.apply(
            crate::source::Update::Snapshot(std::sync::Arc::new(snapshot), now),
            now,
            std::time::SystemTime::UNIX_EPOCH,
        );
        let app = App::new(AppConfig::default());
        let (header, _) = draw_with(&app, &model, 140);
        assert!(
            header.contains(&format!(
                "proto {}·{PROTOCOL_VERSION} ⚠",
                PROTOCOL_VERSION - 1
            )),
            "{header}"
        );
    }

    #[test]
    fn a_narrow_header_drops_the_optional_segments_it_cannot_fit() {
        let app = App::new(AppConfig::default());
        let (header, _) = draw_with(&app, &fixture(), 60);
        assert!(header.starts_with(" ●•• sundog lens  fixture"), "{header}");
        assert!(!header.contains("observer"), "{header}");
        assert!(header.ends_with("00:00:20 UTC"), "{header}");
        assert!(header.chars().count() <= 60, "{header}");
    }

    #[test]
    fn a_frozen_display_says_so_instead_of_spinning() {
        let model = fixture();
        let mut app = App::new(AppConfig::default());
        app.handle_key(
            crossterm::event::KeyEvent::new(
                crossterm::event::KeyCode::Char('p'),
                crossterm::event::KeyModifiers::NONE,
            ),
            &model,
        );
        let (header, _) = draw_with(&app, &model, 140);
        assert!(header.contains("‖ frozen"), "{header}");
    }

    #[test]
    fn without_motion_the_spinner_stands_still() {
        let app = App::new(AppConfig {
            anim: false,
            ..AppConfig::default()
        });
        let (header, _) = draw_with(&app, &fixture(), 140);
        assert!(header.contains('⣾'), "{header}");
    }

    #[test]
    fn the_tab_row_lists_four_tabs_and_the_ownership_status() {
        let app = App::new(AppConfig::default());
        let (_, tabs) = draw_with(&app, &fixture(), 140);
        assert!(tabs.contains(" 1 Overview "), "{tabs}");
        assert!(tabs.contains("2 Caches"), "{tabs}");
        assert!(tabs.contains("3 Node"), "{tabs}");
        assert!(tabs.contains("4 Timeline"), "{tabs}");
        assert!(tabs.contains("no metrics"), "{tabs}");
        assert!(tabs.contains("it · part view "), "{tabs}");
        assert!(tabs.contains("✔ settled"), "{tabs}");
        assert!(tabs.contains("(gossip only)"), "{tabs}");
    }

    #[test]
    fn a_narrow_tab_row_keeps_the_scrape_health_and_drops_the_view_status() {
        let app = App::new(AppConfig::default());
        let (_, tabs) = draw_with(&app, &fixture(), 100);
        assert!(tabs.contains("no metrics"), "{tabs}");
        assert!(!tabs.contains("part view"), "{tabs}");
        let (_, tiny) = draw_with(&app, &fixture(), 55);
        assert!(!tiny.contains("no metrics"), "{tiny}");
        assert!(tiny.contains("4 Timeline"), "{tiny}");
    }

    #[test]
    fn the_active_tab_is_an_amber_pill() {
        let model = fixture();
        let mut app = App::new(AppConfig::default());
        app.apply_director(crate::app::UiCommand::Tab(View::Timeline), &model);
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
        let area = Rect::new(0, 0, 140, 1);
        let mut buf = Buffer::empty(area);
        render_tabs(&scene, area, &mut buf);
        let text = row_text(&buf, 0);
        let start = text.find(" 4 Timeline ").unwrap();
        let cell = &buf[(u16::try_from(start).unwrap() + 3, 0)];
        assert_eq!(cell.bg, ratatui::style::Color::Rgb(0xF2, 0xB5, 0x44));
        let other = &buf[(2, 0)];
        assert_ne!(other.bg, ratatui::style::Color::Rgb(0xF2, 0xB5, 0x44));
    }

    #[test]
    fn scrape_intervals_read_in_seconds_or_milliseconds() {
        assert_eq!(interval_text(Duration::from_secs(1)), "1 s");
        assert_eq!(interval_text(Duration::from_millis(500)), "500 ms");
        assert_eq!(interval_text(Duration::from_secs(5)), "5 s");
    }
}

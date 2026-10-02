//! The help overlay: keys, glyphs and what the lens cannot show.

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Clear, Paragraph, Widget};

use super::look::{Look, Token};
use super::panel::{gap, width_of};
use super::{Scene, text};

/// A key and what it does.
type Key = (&'static str, &'static str);

/// The key table: each row is a left entry and a right entry.
const KEYS: [(Key, Key); 8] = [
    (("1-4 Tab ⇧Tab", "views"), ("↑↓ j k g G", "select")),
    (("⏎", "node detail"), ("Esc", "close, then back")),
    (
        ("c C", "next/prev Distributed cache"),
        ("f", "event filter"),
    ),
    (("t", "gone rows"), ("p", "freeze")),
    (("a", "animations"), ("r", "raw samples (Node)")),
    (("?", "help"), ("q Ctrl-C", "quit")),
    (("S", "spawn (demo)"), ("K", "SIGKILL (demo)")),
    (("L", "SIGTERM leave (demo)"), ("R", "restart (demo)")),
];

/// The glyph legend.
const GLYPHS: [&str; 2] = [
    "● live  ◒ warming  ◐ departing  ○ left  ✖ down  ✚ joined  ↻ rejoined",
    "✓ reported = computed  ↻ settling  ⇄ view change  ✔ settled",
];

/// What v1 cannot show.
const NOT_SHOWN: &str = "not shown: per-part pulls and repairs, slowest parts, keys and digests, per-cache warmth, CPU and RSS";

/// The popup's lines for `look`, with the observer address of the interface.
fn content(scene: &Scene<'_>, look: Look, left_width: usize) -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    for ((left_key, left_label), (right_key, right_label)) in KEYS {
        let mut spans = vec![gap(2)];
        spans.push(Span::styled(
            text::pad_right(left_key, 13),
            look.style(Token::Accent).add_modifier(Modifier::BOLD),
        ));
        spans.push(look.span(
            text::pad_right(left_label, left_width.saturating_sub(13)),
            Token::Text,
        ));
        spans.push(Span::styled(
            text::pad_right(right_key, 11),
            look.style(Token::Accent).add_modifier(Modifier::BOLD),
        ));
        spans.push(look.span(right_label.to_owned(), Token::Text));
        lines.push(Line::from(spans));
    }
    lines.push(Line::default());
    for legend in GLYPHS {
        lines.push(Line::from(vec![gap(2), look.span(legend, Token::Muted)]));
    }
    lines.push(Line::from(vec![
        gap(2),
        look.span(
            "mosaic: 1024 buckets, each in the color of the node that leads most of its 64 parts",
            Token::Muted,
        ),
    ]));
    lines.push(Line::default());
    let observer = scene
        .app
        .config()
        .observer
        .map_or_else(|| "the observer".to_owned(), |addr| addr.to_string());
    lines.push(Line::from(vec![
        gap(2),
        look.span("gossip", Token::Info),
        look.span(
            format!(" = observer {observer} (never a peer)"),
            Token::Muted,
        ),
    ]));
    lines.push(Line::from(vec![
        gap(2),
        look.span("metrics", Token::Info),
        look.span(" = each node's /metrics, mapped by template", Token::Muted),
    ]));
    lines.push(Line::from(vec![
        gap(2),
        look.span("computed", Token::Move),
        look.span(
            " = worked out here with the code the nodes run",
            Token::Muted,
        ),
    ]));
    lines.push(Line::from(vec![gap(2), look.span(NOT_SHOWN, Token::Faint)]));
    lines
}

/// The popup's area: centered, as wide as its content needs and the screen
/// allows.
#[must_use]
pub fn popup_area(area: Rect, lines: &[Line<'_>]) -> Rect {
    let wanted = lines
        .iter()
        .map(|line| width_of(&line.spans))
        .max()
        .unwrap_or(0)
        + 4;
    let width = u16::try_from(wanted)
        .unwrap_or(u16::MAX)
        .min(area.width.saturating_sub(2));
    let height = u16::try_from(lines.len() + 2)
        .unwrap_or(u16::MAX)
        .min(area.height.saturating_sub(2));
    Rect::new(
        area.x + (area.width - width) / 2,
        area.y + (area.height - height) / 2,
        width,
        height,
    )
}

/// Draws the help popup over `area`.
pub fn render(scene: &Scene<'_>, area: Rect, buf: &mut Buffer) {
    let look = scene.look;
    let lines = content(scene, look, 43);
    let popup = popup_area(area, &lines);
    Clear.render(popup, buf);
    let title = vec![
        Span::raw(" "),
        Span::styled("●", look.style(Token::Accent).add_modifier(Modifier::BOLD)),
        look.span("••", Token::Faint),
        Span::styled(
            " sundog lens · keys ",
            look.style(Token::Text).add_modifier(Modifier::BOLD),
        ),
    ];
    let block = Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(look.border(true))
        .style(look.surface())
        .title_top(Line::from(title));
    let inner = block.inner(popup);
    block.render(popup, buf);
    Paragraph::new(lines).render(inner, buf);
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;
    use crate::app::{App, AppConfig};
    use crate::model::testkit;
    use crate::ui::panel::row_text;
    use crate::ui::theme;
    use crate::ui::{Ctx, LayoutKind};

    fn rendered(area: Rect) -> Vec<String> {
        let model = testkit::fixture_model(Instant::now());
        let mut app = App::new(AppConfig {
            observer: Some("127.0.0.1:41733".parse().unwrap()),
            ..AppConfig::default()
        });
        app.help = true;
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
        let mut buf = Buffer::empty(area);
        render(&scene, area, &mut buf);
        (0..area.height).map(|y| row_text(&buf, y)).collect()
    }

    #[test]
    fn the_popup_lists_the_keys_the_legend_and_the_sources() {
        let rows = rendered(Rect::new(0, 0, 140, 40)).join("\n");
        for needle in [
            "sundog lens · keys",
            "1-4 Tab ⇧Tab",
            "next/prev Distributed cache",
            "SIGKILL (demo)",
            "● live  ◒ warming  ◐ departing",
            "mosaic: 1024 buckets",
            "gossip = observer 127.0.0.1:41733 (never a peer)",
            "metrics = each node's /metrics",
            "computed = worked out here",
            "not shown: per-part pulls",
        ] {
            assert!(rows.contains(needle), "missing {needle:?} in\n{rows}");
        }
    }

    #[test]
    fn every_glyph_the_popup_draws_is_allowed() {
        for row in rendered(Rect::new(0, 0, 140, 40)) {
            for c in row.chars() {
                assert!(theme::is_allowed(c), "{c:?} in {row}");
            }
        }
    }

    #[test]
    fn the_popup_is_centered_inside_a_rounded_border() {
        let rows = rendered(Rect::new(0, 0, 140, 40));
        let top = rows.iter().position(|row| row.contains('╭')).unwrap();
        let bottom = rows.iter().rposition(|row| row.contains('╯')).unwrap();
        assert!(top > 1 && bottom < 38, "{top} {bottom}");
        let left = rows[top].find('╭').unwrap();
        let right = rows[top].rfind('╮').unwrap();
        let margin_left = rows[top][..left].chars().count();
        let margin_right = 140 - rows[top][..right].chars().count() - 1;
        assert!(
            margin_left.abs_diff(margin_right) <= 1,
            "{margin_left} {margin_right}"
        );
    }

    #[test]
    fn a_small_screen_gets_a_clipped_popup_and_no_panic() {
        let area = Rect::new(0, 0, 80, 24);
        let rows = rendered(area);
        assert!(rows.iter().any(|row| row.contains("keys")));
        let popup = popup_area(area, &vec![Line::from("x".repeat(200)); 60]);
        assert!(popup.width <= 78 && popup.height <= 22);
    }
}

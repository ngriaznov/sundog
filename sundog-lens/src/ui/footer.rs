//! The footer: key hints for the view on screen, and the demo keys at the
//! right in demo mode. A row too narrow for every hint drops them from the
//! end of the list, ahead of `? help` and `q quit`, which always stay.

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Widget};

use super::panel::{gap, width_of};
use super::widgets::keycap;
use super::{LayoutKind, Scene, View};

/// The hints of `view`, most important first.
#[must_use]
pub fn hints(view: View) -> Vec<(&'static str, &'static str)> {
    let mut hints = match view {
        View::Overview => vec![
            ("1-4", "view"),
            ("↑↓", "select"),
            ("⏎", "node"),
            ("c", "cache"),
            ("f", "filter"),
            ("t", "gone"),
        ],
        View::Caches => vec![
            ("1-4", "view"),
            ("↑↓", "cache"),
            ("c", "ownership"),
            ("f", "filter"),
        ],
        View::Node => vec![
            ("1-4", "view"),
            ("↑↓", "node"),
            ("r", "raw"),
            ("Esc", "back"),
        ],
        View::Timeline => vec![
            ("1-4", "view"),
            ("↑↓", "scroll"),
            ("G", "live"),
            ("f", "filter"),
            ("t", "gone"),
        ],
    };
    hints.extend([
        ("e", "explain"),
        ("p", "freeze"),
        ("a", "anim"),
        ("?", "help"),
        ("q", "quit"),
    ]);
    hints
}

/// The hints at the end of the list that stay on screen whatever the width:
/// `? help` and `q quit`.
const KEPT: usize = 2;

/// The shortened hints of a narrow screen.
const NARROW: &str = "1-4 ↑↓ ⏎ e ? q";

/// The demo keys.
const DEMO: [(&str, &str); 4] = [
    ("S", "spawn"),
    ("K", "kill"),
    ("L", "leave"),
    ("R", "restart"),
];

/// Draws the footer row.
pub fn render(scene: &Scene<'_>, area: Rect, buf: &mut Buffer) {
    let look = scene.look;
    let width = usize::from(area.width);
    let mut spans: Vec<Span<'static>> = vec![gap(1)];
    if scene.kind == LayoutKind::Narrow {
        spans.push(look.span(NARROW, super::look::Token::Accent));
    } else {
        let all = hints(scene.app.view);
        let demo = scene.app.config().demo;
        let demo_width = if demo {
            keycap::keycaps_width(&DEMO) + "demo: ".len() + 2
        } else {
            0
        };
        let room = width.saturating_sub(demo_width + 2);
        spans.extend(keycap::keycap_spans(
            &keycap::fit_keeping(&all, KEPT, room),
            look.mode,
        ));
        if demo {
            let used = width_of(&spans);
            let mut tail = vec![look.span("demo: ", super::look::Token::Muted)];
            tail.extend(keycap::keycap_spans(&DEMO, look.mode));
            tail.push(gap(1));
            let tail_width = width_of(&tail);
            if used + tail_width <= width {
                spans.push(gap(width - used - tail_width));
                spans.extend(tail);
            }
        }
    }
    Paragraph::new(Line::from(spans))
        .style(look.surface())
        .render(area, buf);
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;
    use crate::app::{App, AppConfig};
    use crate::model::testkit;
    use crate::ui::panel::row_text;
    use crate::ui::{Ctx, Scene};

    fn footer(app: &App, kind: LayoutKind, width: u16) -> String {
        let model = testkit::fixture_model(Instant::now());
        let ctx = Ctx {
            now: model.now().unwrap(),
            wall: model.wall().unwrap(),
            elapsed: Duration::ZERO,
        };
        let scene = Scene {
            app,
            model: &model,
            ctx: &ctx,
            look: app.look(),
            kind,
        };
        let area = Rect::new(0, 0, width, 1);
        let mut buf = Buffer::empty(area);
        render(&scene, area, &mut buf);
        row_text(&buf, 0)
    }

    #[test]
    fn the_overview_footer_lists_the_spec_keys() {
        let app = App::new(AppConfig::default());
        let text = footer(&app, LayoutKind::Full, 140);
        assert_eq!(
            text,
            " 1-4 view  ↑↓ select  ⏎ node  c cache  f filter  t gone  e explain  p freeze  a anim  ? help  q quit"
        );
    }

    #[test]
    fn the_overview_footer_lists_explain() {
        let hints = hints(View::Overview);
        let explain = hints.iter().position(|hint| *hint == ("e", "explain"));
        let freeze = hints.iter().position(|hint| *hint == ("p", "freeze"));
        assert_eq!(explain.map(|at| at + 1), freeze, "{hints:?}");
    }

    #[test]
    fn each_view_has_its_own_hints_ending_in_the_common_keys() {
        for view in View::ALL {
            let hints = hints(view);
            assert_eq!(hints.last(), Some(&("q", "quit")), "{view:?}");
            assert!(hints.contains(&("?", "help")));
            assert!(hints.contains(&("1-4", "view")));
            assert!(hints.contains(&("e", "explain")), "{view:?}");
        }
        assert!(hints(View::Node).contains(&("r", "raw")));
        assert!(hints(View::Timeline).contains(&("G", "live")));
    }

    #[test]
    fn a_narrow_footer_shortens_to_the_bare_keys() {
        let app = App::new(AppConfig::default());
        assert_eq!(footer(&app, LayoutKind::Narrow, 80), " 1-4 ↑↓ ⏎ e ? q");
    }

    #[test]
    fn the_narrow_footer_lists_e() {
        assert!(NARROW.split(' ').any(|key| key == "e"), "{NARROW}");
    }

    #[test]
    fn hints_that_do_not_fit_are_dropped_before_the_kept_tail() {
        let app = App::new(AppConfig::default());
        let text = footer(&app, LayoutKind::Compact, 40);
        assert_eq!(text, " 1-4 view  ↑↓ select  ? help  q quit");
        assert!(text.chars().count() <= 40);
        let wide = footer(&app, LayoutKind::Compact, 60);
        assert!(wide.starts_with(" 1-4 view  ↑↓ select  ⏎ node"), "{wide}");
        assert!(wide.ends_with("  ? help  q quit"), "{wide}");
        assert!(!wide.contains("anim") && !wide.contains("freeze"), "{wide}");
    }

    #[test]
    fn quit_stays_visible_in_demo_at_140_and_at_width_100() {
        let demo = App::new(AppConfig {
            demo: true,
            ..AppConfig::default()
        });
        let text = footer(&demo, LayoutKind::Full, 140);
        assert!(
            text.contains("e explain  p freeze  ? help  q quit"),
            "{text}"
        );
        assert!(text.ends_with("demo: S spawn  K kill  L leave  R restart"));
        assert!(!text.contains("a anim"), "{text}");
        let plain = App::new(AppConfig::default());
        let text = footer(&plain, LayoutKind::Compact, 100);
        assert!(
            text.contains("e explain  p freeze  ? help  q quit"),
            "{text}"
        );
        assert!(!text.contains("a anim"), "{text}");
        for width in [100, 120, 140] {
            for app in [&demo, &plain] {
                let text = footer(app, LayoutKind::Compact, width);
                assert!(text.contains("? help  q quit"), "{width}: {text}");
            }
        }
    }

    #[test]
    fn demo_mode_adds_the_fleet_keys_at_the_right() {
        let app = App::new(AppConfig {
            demo: true,
            ..AppConfig::default()
        });
        let text = footer(&app, LayoutKind::Full, 140);
        assert!(
            text.ends_with("demo: S spawn  K kill  L leave  R restart"),
            "{text}"
        );
        assert!(text.contains("q quit"), "{text}");
        assert_eq!(text.chars().count(), 139, "the trailing space is trimmed");
    }
}

//! The splash shown until the first member appears.

use std::time::Duration;

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};

use super::look::Token;
use super::theme::{self, Rgb};
use super::{Scene, anim, panel};

/// How long the splash waits before it explains what might be wrong.
pub const HINT_AFTER: Duration = Duration::from_secs(10);

/// The splash lines for the clock of `scene`.
#[must_use]
pub fn lines(scene: &Scene<'_>) -> Vec<Line<'static>> {
    let look = scene.look;
    let config = scene.app.config();
    let spinner = if scene.app.anim {
        anim::spinner_frame(scene.ctx.elapsed)
    } else {
        '⣾'
    };
    let dim: Rgb = anim::blend(theme::ACCENT, theme::BG, 0.45);
    let seeds = if config.seeds.is_empty() {
        "seeds from SUNDOG_SEEDS or mDNS".to_owned()
    } else {
        format!("seeds {}", config.seeds.join(", "))
    };
    let observer = config
        .observer
        .map_or_else(String::new, |addr| format!(" · observer {addr}"));
    let mut lines = vec![
        Line::from(vec![
            Span::styled("●", look.style(Token::Accent).add_modifier(Modifier::BOLD)),
            Span::styled("••", look.fg(dim)),
            panel::gap(1),
            Span::styled(
                "sundog lens",
                look.style(Token::Text).add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::default(),
        Line::from(vec![
            look.span("listening for gossip from ", Token::Text),
            Span::styled(
                format!("\"{}\"", config.cluster),
                look.style(Token::Accent).add_modifier(Modifier::BOLD),
            ),
            panel::gap(1),
            look.span(spinner.to_string(), Token::Accent),
        ]),
        Line::from(look.span(format!("{seeds}{observer}"), Token::Muted)),
        Line::from(look.span(
            "the observer opens no cache and is never a peer",
            Token::Muted,
        )),
    ];
    if scene.ctx.elapsed >= HINT_AFTER {
        lines.push(Line::default());
        lines.push(Line::from(
            look.span("the cluster name must match exactly", Token::Warn),
        ));
        lines.push(Line::from(look.span(
            "seeds: --seed or SUNDOG_SEEDS · mDNS does not cross a Docker bridge",
            Token::Warn,
        )));
    }
    lines
}

/// Draws the splash, centered in `area`.
pub fn render(scene: &Scene<'_>, area: Rect, buf: &mut Buffer) {
    panel::centered(lines(scene), area, buf);
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;
    use crate::app::{App, AppConfig};
    use crate::model::Model;
    use crate::ui::panel::row_text;
    use crate::ui::{Ctx, LayoutKind};

    fn splash(elapsed: Duration, config: AppConfig) -> Vec<String> {
        let app = App::new(config);
        let model = Model::new();
        let ctx = Ctx {
            now: Instant::now(),
            wall: std::time::SystemTime::UNIX_EPOCH,
            elapsed,
        };
        let scene = Scene {
            app: &app,
            model: &model,
            ctx: &ctx,
            look: app.look(),
            kind: LayoutKind::Full,
        };
        let area = Rect::new(0, 0, 100, 12);
        let mut buf = Buffer::empty(area);
        render(&scene, area, &mut buf);
        (0..12)
            .map(|y| row_text(&buf, y))
            .filter(|row| !row.is_empty())
            .collect()
    }

    fn config() -> AppConfig {
        AppConfig {
            cluster: "lens-demo".into(),
            seeds: vec!["127.0.0.11:7946".into(), "127.0.0.12:7946".into()],
            observer: Some("127.0.0.1:41733".parse().unwrap()),
            ..AppConfig::default()
        }
    }

    #[test]
    fn the_splash_names_the_cluster_the_seeds_and_the_observer() {
        let rows: Vec<String> = splash(Duration::ZERO, config())
            .into_iter()
            .map(|row| row.trim().to_owned())
            .collect();
        assert_eq!(rows[0], "●•• sundog lens");
        assert_eq!(rows[1], "listening for gossip from \"lens-demo\" ⠋");
        assert_eq!(
            rows[2],
            "seeds 127.0.0.11:7946, 127.0.0.12:7946 · observer 127.0.0.1:41733"
        );
        assert_eq!(rows[3], "the observer opens no cache and is never a peer");
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn after_ten_seconds_the_splash_explains_what_might_be_wrong() {
        let rows = splash(Duration::from_secs(10), config());
        assert!(
            rows.iter()
                .any(|row| row.contains("the cluster name must match exactly"))
        );
        assert!(
            rows.iter()
                .any(|row| row.contains("mDNS does not cross a Docker bridge"))
        );
        assert_eq!(splash(Duration::from_secs(9), config()).len(), 4);
    }

    #[test]
    fn without_seeds_the_splash_says_where_seeds_come_from() {
        let rows = splash(
            Duration::ZERO,
            AppConfig {
                cluster: "x".into(),
                ..AppConfig::default()
            },
        );
        assert!(
            rows.iter()
                .any(|row| row.contains("seeds from SUNDOG_SEEDS or mDNS"))
        );
    }

    #[test]
    fn the_hint_comes_after_ten_seconds() {
        assert_eq!(HINT_AFTER, Duration::from_secs(10));
    }
}

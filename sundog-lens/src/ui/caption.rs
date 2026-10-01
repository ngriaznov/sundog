//! The caption row of the demo: the elapsed time and one sentence.

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::text::Line;
use ratatui::widgets::{Paragraph, Widget};

use super::look::Token;
use super::panel::gap;
use super::{Scene, text};

/// Draws the caption row. Without a caption the row stays blank.
pub fn render(scene: &Scene<'_>, area: Rect, buf: &mut Buffer) {
    let look = scene.look;
    let spans = scene.app.caption.as_ref().map_or_else(
        || vec![gap(1)],
        |caption| {
            vec![
                gap(1),
                look.span("▶", Token::Accent),
                gap(1),
                look.span(text::minutes_seconds(scene.ctx.elapsed), Token::Muted),
                gap(2),
                look.span(
                    text::fit(caption, usize::from(area.width).saturating_sub(12)),
                    Token::Text,
                ),
            ]
        },
    );
    Paragraph::new(Line::from(spans))
        .style(look.surface())
        .render(area, buf);
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;
    use crate::app::{App, AppConfig, UiCommand};
    use crate::model::testkit;
    use crate::ui::panel::row_text;
    use crate::ui::{Ctx, LayoutKind};

    fn caption_row(caption: Option<&str>, width: u16) -> String {
        let model = testkit::fixture_model(Instant::now());
        let mut app = App::new(AppConfig::default());
        app.apply_director(UiCommand::Caption(caption.map(str::to_owned)), &model);
        let ctx = Ctx {
            now: model.now().unwrap(),
            wall: model.wall().unwrap(),
            elapsed: Duration::from_secs(52),
        };
        let scene = Scene {
            app: &app,
            model: &model,
            ctx: &ctx,
            look: app.look(),
            kind: LayoutKind::Full,
        };
        let area = Rect::new(0, 0, width, 1);
        let mut buf = Buffer::empty(area);
        render(&scene, area, &mut buf);
        row_text(&buf, 0)
    }

    #[test]
    fn a_caption_shows_the_elapsed_time_and_the_text() {
        assert_eq!(
            caption_row(Some("A graceful leave"), 80),
            " ▶ 0:52  A graceful leave"
        );
    }

    #[test]
    fn a_long_caption_is_cut_to_the_row() {
        let row = caption_row(Some(&"word ".repeat(40)), 50);
        assert!(row.chars().count() <= 50, "{row}");
        assert!(row.ends_with('…'), "{row}");
    }

    #[test]
    fn without_a_caption_the_row_is_blank() {
        assert_eq!(caption_row(None, 40), "");
    }
}

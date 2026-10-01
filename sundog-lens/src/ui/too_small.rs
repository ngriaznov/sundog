//! The notice shown below the minimum screen size.

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::text::Line;

use super::look::{Look, Token};
use super::panel;

/// The smallest screen the interface draws on, in columns and rows.
pub const MINIMUM: (u16, u16) = (80, 24);

/// The notice text for a screen of `width` by `height`.
#[must_use]
pub fn notice(width: u16, height: u16) -> String {
    format!(
        "sundog-lens needs {}×{} (now {width}×{height})",
        MINIMUM.0, MINIMUM.1
    )
}

/// Draws the notice, centered, over `area`.
pub fn render(look: Look, area: Rect, buf: &mut Buffer) {
    let text = notice(area.width, area.height);
    let cut = super::text::fit(&text, usize::from(area.width));
    panel::centered(vec![Line::from(look.span(cut, Token::Warn))], area, buf);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ui::panel::row_text;

    #[test]
    fn the_notice_names_the_minimum_and_the_current_size() {
        assert_eq!(notice(72, 20), "sundog-lens needs 80×24 (now 72×20)");
    }

    #[test]
    fn the_notice_is_centered_and_cut_to_a_tiny_screen() {
        let area = Rect::new(0, 0, 72, 20);
        let mut buf = Buffer::empty(area);
        render(Look::default(), area, &mut buf);
        let row = row_text(&buf, 9);
        assert!(
            row.trim_start().starts_with("sundog-lens needs 80×24"),
            "{row}"
        );
        let tiny = Rect::new(0, 0, 10, 3);
        let mut small = Buffer::empty(tiny);
        render(Look::default(), tiny, &mut small);
        assert!(row_text(&small, 1).chars().count() <= 10);
        let none = Rect::new(0, 0, 0, 0);
        render(Look::default(), none, &mut Buffer::empty(none));
    }

    #[test]
    fn the_minimum_screen_is_eighty_by_twenty_four() {
        assert_eq!(MINIMUM, (80, 24));
    }
}

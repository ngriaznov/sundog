//! The Timeline view's lifelines widget: one row of segments per node over a
//! time window, drawn from the cells that `model::lifelines` computes.

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::widgets::Widget;

use crate::model::lifelines::{Cell, Tone};
use crate::ui::look::{Look, Token};
use crate::ui::theme::Rgb;

/// The style a cell of `tone` is drawn in; `None` for a blank cell. A mark
/// (any glyph but a phase run) is bold.
#[must_use]
pub fn tone_style(look: Look, tone: Tone, node: Rgb, glyph: char) -> Option<Style> {
    let base = match tone {
        Tone::Node => look.node(node),
        Tone::Warn => look.style(Token::Warn),
        Tone::Bad => look.style(Token::Bad),
        Tone::Muted => look.style(Token::Muted),
        Tone::Move => look.style(Token::Move),
        Tone::Ok => look.style(Token::Ok),
        Tone::Info => look.style(Token::Info),
        Tone::Blank => return None,
    };
    let run = matches!(glyph, '━' | '┄');
    Some(if run {
        base
    } else {
        base.add_modifier(Modifier::BOLD)
    })
}

/// One lifeline: its cells, left to right, in the node's color.
#[derive(Debug, Clone, Copy)]
pub struct LifelineRow<'a> {
    cells: &'a [Cell],
    node: Rgb,
    look: Look,
}

impl<'a> LifelineRow<'a> {
    /// A row of `cells`, where a `Node` tone takes `node`.
    #[must_use]
    pub const fn new(cells: &'a [Cell], node: Rgb, look: Look) -> Self {
        Self { cells, node, look }
    }
}

impl Widget for LifelineRow<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        for (offset, cell) in self.cells.iter().take(usize::from(area.width)).enumerate() {
            let Some(style) = tone_style(self.look, cell.tone, self.node, cell.glyph) else {
                continue;
            };
            let Ok(dx) = u16::try_from(offset) else { break };
            if let Some(target) = buf.cell_mut((area.x + dx, area.y)) {
                target.set_char(cell.glyph).set_style(style);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use ratatui::style::Color;

    use super::*;
    use crate::ui::theme::NODE_COLORS;

    fn cell(glyph: char, tone: Tone) -> Cell {
        Cell { glyph, tone }
    }

    #[test]
    fn runs_take_the_node_color_and_marks_are_bold() {
        let look = Look::default();
        let run = tone_style(look, Tone::Node, NODE_COLORS[1], '━').unwrap();
        assert_eq!(run.fg, Some(Color::Rgb(0xFF, 0x7E, 0xB6)));
        assert!(!run.add_modifier.contains(Modifier::BOLD));
        let mark = tone_style(look, Tone::Bad, NODE_COLORS[1], '✖').unwrap();
        assert_eq!(mark.fg, Some(Color::Rgb(0xFF, 0x6B, 0x6B)));
        assert!(mark.add_modifier.contains(Modifier::BOLD));
        assert!(tone_style(look, Tone::Blank, NODE_COLORS[0], ' ').is_none());
        for tone in [Tone::Warn, Tone::Muted, Tone::Move, Tone::Ok, Tone::Info] {
            assert!(tone_style(look, tone, NODE_COLORS[0], '┄').is_some());
        }
    }

    #[test]
    fn a_row_draws_its_glyphs_and_skips_blanks() {
        let cells = [
            cell(' ', Tone::Blank),
            cell('━', Tone::Node),
            cell('◐', Tone::Warn),
            cell('┄', Tone::Warn),
        ];
        let area = Rect::new(0, 0, 6, 1);
        let mut buf = Buffer::empty(area);
        LifelineRow::new(&cells, NODE_COLORS[0], Look::default()).render(area, &mut buf);
        assert_eq!(crate::ui::panel::row_text(&buf, 0), " ━◐┄");
        assert_eq!(buf[(1, 0)].fg, Color::Rgb(0x6C, 0xB6, 0xFF));
        assert_eq!(buf[(0, 0)].symbol(), " ");
    }

    #[test]
    fn a_row_wider_than_its_area_is_clipped() {
        let cells = vec![cell('━', Tone::Node); 10];
        let area = Rect::new(0, 0, 4, 1);
        let mut buf = Buffer::empty(area);
        LifelineRow::new(&cells, NODE_COLORS[0], Look::default()).render(area, &mut buf);
        assert_eq!(crate::ui::panel::row_text(&buf, 0), "━━━━");
    }
}

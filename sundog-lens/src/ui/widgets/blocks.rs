//! Block-character sparklines and bars: the `--no-braille` fallback and the
//! horizontal eighths bar.

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::widgets::Widget;

use super::count_f64;
use crate::ui::theme::{self, ColorMode};

/// Vertical levels 1 to 8, bottom up.
const VERTICAL: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];

/// Horizontal partial cells, 1/8 to 7/8 of a cell.
const HORIZONTAL: [char; 7] = ['▏', '▎', '▍', '▌', '▋', '▊', '▉'];

/// A sparkline `width` cells wide over the newest `width` samples, one
/// sample per cell, scaled against the largest of them. The newest sample is
/// the last cell; missing history on the left and zero samples are spaces,
/// and any positive sample draws at least `▁`.
#[must_use]
pub fn spark_blocks(values: &[f64], width: usize) -> String {
    let window = &values[values.len().saturating_sub(width)..];
    let max = window
        .iter()
        .copied()
        .filter(|v| v.is_finite())
        .fold(0.0, f64::max);
    let mut line = " ".repeat(width - window.len());
    line.extend(window.iter().map(|&value| {
        if !value.is_finite() || value <= 0.0 || max <= 0.0 {
            return ' ';
        }
        let scaled = (value / max).clamp(0.0, 1.0) * 8.0;
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "the value lies between 0 and 8"
        )]
        let level = scaled.ceil() as usize;
        VERTICAL[level.clamp(1, 8) - 1]
    }));
    line
}

/// [`spark_blocks`] with a floor: a zero sample draws `▁`, so a quiet line
/// reads as a baseline and only missing history is blank.
#[must_use]
pub fn spark_blocks_floor(values: &[f64], width: usize) -> String {
    let window = &values[values.len().saturating_sub(width)..];
    let missing = width - window.len();
    let drawn = spark_blocks(values, width);
    drawn
        .chars()
        .enumerate()
        .map(|(i, c)| {
            if c == ' ' && i >= missing {
                VERTICAL[0]
            } else {
                c
            }
        })
        .collect()
}

/// A horizontal bar `width` cells wide filled to `frac` (clamped to 0 to 1),
/// in eighths of a cell: full cells `█`, one partial cell, then spaces.
#[must_use]
pub fn bar_eighths(frac: f64, width: usize) -> String {
    let frac = if frac.is_nan() {
        0.0
    } else {
        frac.clamp(0.0, 1.0)
    };
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "the value lies between 0 and 8 times the width"
    )]
    let eighths = (frac * count_f64(width) * 8.0).round() as usize;
    let (full, part) = (eighths / 8, eighths % 8);
    let mut bar = "█".repeat(full);
    if part > 0 {
        bar.push(HORIZONTAL[part - 1]);
    }
    let used = full + usize::from(part > 0);
    bar.push_str(&" ".repeat(width.saturating_sub(used)));
    bar
}

/// A filled area chart of block characters: `h` rows of `w` columns, one
/// sample per column (the newest `w` samples), each column filled from the
/// bottom in eighths of the area's height against `max`. Missing history on
/// the left is blank. Row 0 is the top.
#[must_use]
pub fn block_area(values: &[f64], w: usize, h: usize, max: f64) -> Vec<Vec<char>> {
    let window = &values[values.len().saturating_sub(w)..];
    let missing = w - window.len();
    let mut rows = vec![vec![' '; w]; h];
    for (i, &value) in window.iter().enumerate() {
        if !value.is_finite() || value <= 0.0 || max <= 0.0 {
            continue;
        }
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "the value lies between 0 and 8 times the height"
        )]
        let eighths = ((value / max).clamp(0.0, 1.0) * count_f64(h) * 8.0).ceil() as usize;
        let eighths = eighths.clamp(1, h * 8);
        let (full, part) = (eighths / 8, eighths % 8);
        for dy in 0..full {
            rows[h - 1 - dy][missing + i] = '█';
        }
        if part > 0 && full < h {
            rows[h - 1 - full][missing + i] = VERTICAL[part - 1];
        }
    }
    rows
}

/// The `--no-braille` area chart: [`block_area`] in a gradient.
#[derive(Debug, Clone)]
pub struct BlockArea<'a> {
    values: &'a [f64],
    max: f64,
    mode: ColorMode,
}

impl<'a> BlockArea<'a> {
    /// A chart of `values` scaled against `max`.
    #[must_use]
    pub const fn new(values: &'a [f64], max: f64, mode: ColorMode) -> Self {
        Self { values, max, mode }
    }
}

impl Widget for BlockArea<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let (w, h) = (usize::from(area.width), usize::from(area.height));
        if w == 0 || h == 0 {
            return;
        }
        for (row, line) in block_area(self.values, w, h, self.max).iter().enumerate() {
            let height = if h == 1 {
                1.0
            } else {
                count_f64(h - 1 - row) / count_f64(h - 1)
            };
            let style = Style::new().fg(theme::gradient_at(height).color(self.mode));
            for (column, &glyph) in line.iter().enumerate() {
                if glyph == ' ' {
                    continue;
                }
                let (Ok(dx), Ok(dy)) = (u16::try_from(column), u16::try_from(row)) else {
                    continue;
                };
                if let Some(cell) = buf.cell_mut((area.x + dx, area.y + dy)) {
                    cell.set_char(glyph).set_style(style);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spark_blocks_maps_levels_to_the_eight_blocks() {
        let values: Vec<f64> = (1..=8).map(f64::from).collect();
        assert_eq!(spark_blocks(&values, 8), "▁▂▃▄▅▆▇█");
    }

    #[test]
    fn spark_blocks_is_width_exact_and_right_aligned() {
        for width in 0..8 {
            for len in 0..12 {
                let values: Vec<f64> = (0..len).map(f64::from).collect();
                assert_eq!(spark_blocks(&values, width).chars().count(), width);
            }
        }
        assert_eq!(spark_blocks(&[4.0], 3), "  █");
    }

    #[test]
    fn spark_blocks_blanks_zero_and_draws_any_positive() {
        assert_eq!(spark_blocks(&[0.0, 1000.0, 0.001], 3), " █▁");
        assert_eq!(spark_blocks(&[0.0, 0.0], 2), "  ");
        assert_eq!(spark_blocks(&[f64::NAN, -1.0], 2), "  ");
    }

    #[test]
    fn spark_blocks_keeps_only_the_newest_samples() {
        assert_eq!(spark_blocks(&[9.0, 9.0, 1.0, 2.0], 2), "▄█");
    }

    #[test]
    fn bar_eighths_fills_whole_and_partial_cells() {
        assert_eq!(bar_eighths(0.0, 4), "    ");
        assert_eq!(bar_eighths(1.0, 4), "████");
        assert_eq!(bar_eighths(0.5, 4), "██  ");
        assert_eq!(bar_eighths(0.375, 4), "█▌  ");
        assert_eq!(bar_eighths(0.125, 4), "▌   ");
        assert_eq!(bar_eighths(1.0 / 32.0, 4), "▏   ");
        assert_eq!(bar_eighths(7.0 / 32.0, 4), "▉   ");
    }

    #[test]
    fn bar_eighths_clamps_and_keeps_its_width() {
        assert_eq!(bar_eighths(-1.0, 3), "   ");
        assert_eq!(bar_eighths(5.0, 3), "███");
        assert_eq!(bar_eighths(f64::NAN, 3), "   ");
        for width in 0..10 {
            for step in 0..=20 {
                let bar = bar_eighths(f64::from(step) / 20.0, width);
                assert_eq!(bar.chars().count(), width);
            }
        }
    }

    #[test]
    fn a_floored_block_spark_keeps_a_baseline_where_samples_exist() {
        assert_eq!(spark_blocks_floor(&[0.0, 0.0], 4), "  ▁▁");
        assert_eq!(spark_blocks_floor(&[0.0, 4.0, 8.0], 3), "▁▄█");
        assert_eq!(spark_blocks_floor(&[], 2), "  ");
    }

    #[test]
    fn a_block_area_fills_columns_from_the_bottom_in_eighths() {
        let rows = block_area(&[0.0, 4.0, 8.0, 12.0, 16.0], 5, 2, 16.0);
        assert_eq!(rows.len(), 2);
        // 16 eighths in two rows: 0 blank, 4 is half a row, 8 a full row,
        // 12 a full row and a half, 16 two full rows.
        assert_eq!(rows[1], [' ', '▄', '█', '█', '█']);
        assert_eq!(rows[0], [' ', ' ', ' ', '▄', '█']);
    }

    #[test]
    fn a_block_area_leaves_missing_history_blank_and_ignores_bad_samples() {
        let rows = block_area(&[f64::NAN, 5.0], 4, 1, 5.0);
        assert_eq!(rows[0], [' ', ' ', ' ', '█']);
        assert_eq!(block_area(&[1.0], 2, 1, 0.0)[0], [' ', ' ']);
        assert_eq!(block_area(&[], 3, 2, 1.0).len(), 2);
        // The newest `w` samples win.
        assert_eq!(block_area(&[9.0, 9.0, 9.0, 1.0], 2, 1, 9.0)[0], ['█', '▁']);
    }

    #[test]
    fn the_block_area_widget_draws_its_glyphs_in_a_gradient() {
        let values = [1.0, 2.0, 3.0, 4.0];
        let area = Rect::new(0, 0, 4, 3);
        let mut buf = Buffer::empty(area);
        BlockArea::new(&values, 4.0, ColorMode::Truecolor).render(area, &mut buf);
        assert_eq!(buf[(3, 0)].symbol(), "█");
        assert_eq!(buf[(0, 0)].symbol(), " ");
        assert_ne!(buf[(3, 0)].fg, buf[(3, 2)].fg);
        BlockArea::new(&values, 4.0, ColorMode::Mono)
            .render(Rect::new(0, 0, 0, 0), &mut Buffer::empty(area));
    }
}

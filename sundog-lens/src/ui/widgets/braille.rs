//! Braille sparklines and area charts: each cell holds 2 columns by 4 rows of
//! dots, so a cell carries two samples at four levels each.

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::widgets::Widget;

use super::count_f64;
use crate::ui::theme::{self, ColorMode};

/// The first braille code point, the blank cell.
const BASE: u32 = 0x2800;

/// Bits for levels 1 to 4 of a cell's left column, bottom dot first.
const LEFT: [u8; 4] = [0x40, 0x04, 0x02, 0x01];

/// Bits for levels 1 to 4 of a cell's right column, bottom dot first.
const RIGHT: [u8; 4] = [0x80, 0x20, 0x10, 0x08];

/// The braille character for dot pattern `bits`.
fn glyph(bits: u8) -> char {
    char::from_u32(BASE + u32::from(bits)).unwrap_or(' ')
}

/// The bits of a column filled to `level` (0 to 4) from the bottom.
fn column_bits(column: [u8; 4], level: usize) -> u8 {
    column.iter().take(level).fold(0, |acc, bit| acc | bit)
}

/// The dot level of `value` against `max` on a scale of `levels` steps: 0 for
/// a value at or below zero (or not finite), at least 1 for any positive
/// value, `levels` at `max`.
fn level_of(value: f64, max: f64, levels: usize) -> usize {
    if !value.is_finite() || value <= 0.0 || !max.is_finite() || max <= 0.0 {
        return 0;
    }
    let scaled = (value / max).clamp(0.0, 1.0) * count_f64(levels);
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "the value lies between 0 and the level count"
    )]
    let level = scaled.ceil() as usize;
    level.clamp(1, levels)
}

/// The largest finite sample, or 0.
fn max_of(values: &[f64]) -> f64 {
    values
        .iter()
        .copied()
        .filter(|v| v.is_finite())
        .fold(0.0, f64::max)
}

/// A sparkline `width` cells wide over the newest `2 * width` samples,
/// scaled against the largest of them. The newest sample is the right half of
/// the last cell; missing history on the left is blank. A zero sample draws
/// no dots, any positive sample at least the bottom dot.
#[must_use]
pub fn spark_braille(values: &[f64], width: usize) -> String {
    let window = &values[values.len().saturating_sub(2 * width)..];
    spark_braille_scaled(values, width, max_of(window))
}

/// [`spark_braille`] against an explicit `max`, to give several lines one
/// shared scale.
#[must_use]
pub fn spark_braille_scaled(values: &[f64], width: usize, max: f64) -> String {
    spark_with_floor(values, width, max, 0)
}

/// [`spark_braille`] with a floor: every sample that exists draws at least
/// the bottom dots, so a quiet line reads as a baseline (`⣀`) and only
/// missing history is blank.
#[must_use]
pub fn spark_braille_floor(values: &[f64], width: usize) -> String {
    let window = &values[values.len().saturating_sub(2 * width)..];
    spark_with_floor(values, width, max_of(window), 1)
}

fn spark_with_floor(values: &[f64], width: usize, max: f64, floor: usize) -> String {
    let window = &values[values.len().saturating_sub(2 * width)..];
    // Right-align: the sample index of the first column.
    let missing = 2 * width - window.len();
    let level = |column: usize| -> usize {
        column
            .checked_sub(missing)
            .map_or(0, |i| level_of(window[i], max, 4).max(floor))
    };
    (0..width)
        .map(|cell| {
            glyph(column_bits(LEFT, level(2 * cell)) | column_bits(RIGHT, level(2 * cell + 1)))
        })
        .collect()
}

/// An area chart of `w` by `h` cells as braille bit patterns (add U+2800 for
/// the character), row-major from the top row. Samples are right-aligned, two
/// per cell column, each filled from the bottom to `value / max` of the full
/// `4 * h` dot height; a positive sample fills at least one dot.
#[must_use]
pub fn rasterize(values: &[f64], w: usize, h: usize, max: f64) -> Vec<u8> {
    let mut cells = vec![0u8; w * h];
    let window = &values[values.len().saturating_sub(2 * w)..];
    let missing = 2 * w - window.len();
    for (i, &value) in window.iter().enumerate() {
        let column = missing + i;
        let bits = if column.is_multiple_of(2) {
            LEFT
        } else {
            RIGHT
        };
        for dot in 0..level_of(value, max, 4 * h) {
            let row = h - 1 - dot / 4;
            cells[row * w + column / 2] |= bits[dot % 4];
        }
    }
    cells
}

/// A filled area chart over `values` in a gradient by height.
#[derive(Debug, Clone)]
pub struct BrailleArea<'a> {
    values: &'a [f64],
    max: f64,
    mode: ColorMode,
}

impl<'a> BrailleArea<'a> {
    /// An area chart of `values` against `max`.
    #[must_use]
    pub const fn new(values: &'a [f64], max: f64, mode: ColorMode) -> Self {
        Self { values, max, mode }
    }
}

impl Widget for BrailleArea<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let (cols, rows) = (usize::from(area.width), usize::from(area.height));
        if cols == 0 || rows == 0 {
            return;
        }
        let cells = rasterize(self.values, cols, rows, self.max);
        for (row, line) in cells.chunks(cols).enumerate() {
            // Row 0 is the top: the gradient rises with height.
            let height = if rows == 1 {
                1.0
            } else {
                count_f64(rows - 1 - row) / count_f64(rows - 1)
            };
            let color = theme::gradient_at(height).color(self.mode);
            for (column, &bits) in line.iter().enumerate() {
                if bits == 0 {
                    continue;
                }
                let (Ok(dx), Ok(dy)) = (u16::try_from(column), u16::try_from(row)) else {
                    continue;
                };
                if let Some(cell) = buf.cell_mut((area.x + dx, area.y + dy)) {
                    cell.set_char(glyph(bits)).set_style(Style::new().fg(color));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn codepoints(s: &str) -> Vec<u32> {
        s.chars().map(u32::from).collect()
    }

    #[test]
    fn spark_braille_emits_the_exact_codepoints() {
        // One cell: left level 1 and right level 0.
        assert_eq!(
            codepoints(&spark_braille_scaled(&[0.25, 0.0], 1, 1.0)),
            [0x2840]
        );
        // Both columns at level 1.
        assert_eq!(
            codepoints(&spark_braille_scaled(&[0.25, 0.25], 1, 1.0)),
            [0x28C0]
        );
        // Both columns full.
        assert_eq!(
            codepoints(&spark_braille_scaled(&[1.0, 1.0], 1, 1.0)),
            [0x28FF]
        );
        // Right column only.
        assert_eq!(
            codepoints(&spark_braille_scaled(&[0.0, 1.0], 1, 1.0)),
            [0x28B8]
        );
    }

    #[test]
    fn spark_braille_levels_fill_from_the_bottom() {
        let level = |v: f64| codepoints(&spark_braille_scaled(&[v, 0.0], 1, 1.0))[0];
        assert_eq!(level(0.25), 0x2840);
        assert_eq!(level(0.5), 0x2840 | 0x04);
        assert_eq!(level(0.75), 0x2840 | 0x04 | 0x02);
        assert_eq!(level(1.0), 0x2847);
    }

    #[test]
    fn a_floored_spark_draws_a_baseline_for_quiet_samples_and_blanks_for_missing_ones() {
        // Two quiet samples in a three-cell line: the first two cells are
        // missing history, the last carries the baseline.
        assert_eq!(
            codepoints(&spark_braille_floor(&[0.0, 0.0], 3)),
            [0x2800, 0x2800, 0x28C0]
        );
        assert_eq!(codepoints(&spark_braille_floor(&[0.0; 6], 3)), [0x28C0; 3]);
        // A loud sample still rises above the floor.
        assert_eq!(codepoints(&spark_braille_floor(&[0.0, 1.0], 1)), [0x28F8]);
        assert_eq!(spark_braille_floor(&[], 2), "\u{2800}\u{2800}");
    }

    #[test]
    fn spark_braille_scales_to_the_window_maximum() {
        let line = spark_braille(&[0.0, 5.0, 10.0, 10.0], 2);
        assert_eq!(codepoints(&line), [0x28A0, 0x28FF]);
    }

    #[test]
    fn spark_braille_is_width_exact_and_right_aligned() {
        for width in 0..8 {
            for len in 0..20 {
                let values: Vec<f64> = (0..len).map(f64::from).collect();
                assert_eq!(spark_braille(&values, width).chars().count(), width);
            }
        }
        // One sample lands in the right half of the last cell.
        assert_eq!(
            codepoints(&spark_braille(&[1.0], 3)),
            [0x2800, 0x2800, 0x28B8]
        );
    }

    #[test]
    fn spark_braille_draws_a_positive_sample_and_skips_a_zero_one() {
        assert_eq!(
            codepoints(&spark_braille_scaled(&[0.001, 0.0], 1, 1000.0)),
            [0x2840]
        );
        assert_eq!(codepoints(&spark_braille(&[0.0, 0.0], 1)), [0x2800]);
        assert_eq!(codepoints(&spark_braille(&[f64::NAN, -3.0], 1)), [0x2800]);
        assert_eq!(spark_braille(&[], 0), "");
    }

    #[test]
    fn spark_braille_keeps_only_the_newest_samples() {
        let line = spark_braille(&[9.0, 9.0, 0.0, 0.0], 1);
        assert_eq!(codepoints(&line), [0x2800]);
    }

    #[test]
    fn rasterize_returns_w_times_h_cells() {
        for (w, h) in [(0, 0), (1, 1), (5, 3), (10, 2), (3, 7)] {
            assert_eq!(rasterize(&[1.0, 2.0, 3.0], w, h, 3.0).len(), w * h);
        }
    }

    #[test]
    fn rasterize_fills_from_the_bottom_row() {
        // One cell high, a full left column and an empty right one.
        assert_eq!(rasterize(&[1.0, 0.0], 1, 1, 1.0), [0x47]);
        // Two cells high: half height fills the bottom cell only.
        assert_eq!(rasterize(&[0.5, 0.5], 1, 2, 1.0), [0x00, 0xFF]);
        // Full height fills both.
        assert_eq!(rasterize(&[1.0, 1.0], 1, 2, 1.0), [0xFF, 0xFF]);
        // A tiny positive sample fills the bottom dot.
        assert_eq!(rasterize(&[0.0001, 0.0], 1, 2, 1.0), [0x00, 0x40]);
    }

    #[test]
    fn rasterize_with_a_zero_max_draws_nothing() {
        assert_eq!(rasterize(&[5.0, 5.0], 1, 1, 0.0), [0]);
    }

    #[test]
    fn braille_area_renders_a_gradient_and_skips_empty_cells() {
        let values = [1.0, 1.0, 0.0, 0.0];
        let area = Rect::new(2, 1, 2, 2);
        let mut buf = Buffer::empty(Rect::new(0, 0, 6, 4));
        BrailleArea::new(&values, 1.0, ColorMode::Truecolor).render(area, &mut buf);
        let filled = buf.cell((2, 1)).unwrap();
        assert_eq!(filled.symbol(), "\u{28FF}");
        assert_eq!(buf.cell((2, 2)).unwrap().symbol(), "\u{28FF}");
        assert_eq!(buf.cell((3, 1)).unwrap().symbol(), " ");
        assert_eq!(buf.cell((3, 2)).unwrap().symbol(), " ");
        // The top row takes the top gradient stop, the bottom row the bottom.
        assert_eq!(filled.fg, theme::GRADIENT[2].color(ColorMode::Truecolor));
        assert_eq!(
            buf.cell((2, 2)).unwrap().fg,
            theme::GRADIENT[0].color(ColorMode::Truecolor)
        );
    }

    #[test]
    fn braille_area_ignores_an_empty_area() {
        let mut buf = Buffer::empty(Rect::new(0, 0, 3, 3));
        BrailleArea::new(&[1.0], 1.0, ColorMode::Mono).render(Rect::new(0, 0, 0, 0), &mut buf);
        assert_eq!(buf, Buffer::empty(Rect::new(0, 0, 3, 3)));
    }
}

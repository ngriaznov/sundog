//! The ownership mosaic: 1024 buckets as a grid of half-block cells, each
//! bucket in the color of the node that leads most of its 64 parts. The
//! compact grid draws each pair of buckets in the lead of its 128 parts.

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::widgets::Widget;

use crate::ui::anim::blend;
use crate::ui::theme::{self, ColorMode, Rgb};

pub use crate::model::ownership::{BUCKETS, COMPACT_PIXELS, NO_LEAD};

/// Bucket columns in the full grid.
pub const COLUMNS: usize = 64;

/// Pixel rows in the full grid: 1024 buckets over 64 columns.
const PIXEL_ROWS: usize = BUCKETS / COLUMNS;

/// The pixel grid: bucket `b` sits at column `b % 64` and pixel row `b / 64`
/// of `lead`. With `compact`, the grid is half as wide and pixel `p` is the
/// lead of the 128 parts of buckets `2p` and `2p + 1`. Returns the grid width
/// and the row-major pixels.
#[must_use]
pub fn pixel_grid(
    lead: &[u8; BUCKETS],
    compact: Option<&[u8; COMPACT_PIXELS]>,
) -> (usize, Vec<u8>) {
    compact.map_or_else(
        || (COLUMNS, lead.to_vec()),
        |compact| (COLUMNS / 2, compact.to_vec()),
    )
}

/// The cells of the mosaic: for each cell row, for each column, the pair of
/// leads drawn as the upper and lower half. A cell row covers two pixel rows.
#[must_use]
pub fn cell_pairs(
    lead: &[u8; BUCKETS],
    compact: Option<&[u8; COMPACT_PIXELS]>,
) -> Vec<Vec<(u8, u8)>> {
    let (width, pixels) = pixel_grid(lead, compact);
    (0..PIXEL_ROWS / 2)
        .map(|row| {
            (0..width)
                .map(|column| {
                    (
                        pixels[2 * row * width + column],
                        pixels[(2 * row + 1) * width + column],
                    )
                })
                .collect()
        })
        .collect()
}

/// Whether bucket `b` changed lead between `prev` and `lead`.
fn changed(prev: Option<&[u8; BUCKETS]>, lead: &[u8; BUCKETS], bucket: usize) -> bool {
    prev.is_some_and(|prev| prev[bucket] != lead[bucket])
}

/// The ownership mosaic widget.
#[derive(Debug, Clone)]
pub struct Mosaic<'a> {
    lead: &'a [u8; BUCKETS],
    palette: &'a [Rgb],
    mode: ColorMode,
    compact: Option<&'a [u8; COMPACT_PIXELS]>,
    flash: Option<(&'a [u8; BUCKETS], f64)>,
}

impl<'a> Mosaic<'a> {
    /// A mosaic of `lead`, where lead `i` takes `palette[i]`.
    #[must_use]
    pub const fn new(lead: &'a [u8; BUCKETS], palette: &'a [Rgb], mode: ColorMode) -> Self {
        Self {
            lead,
            palette,
            mode,
            compact: None,
            flash: None,
        }
    }

    /// Halves the width: two buckets per pixel, drawn in the leads of
    /// `compact`, which carries the lead of each pair's 128 parts.
    #[must_use]
    pub const fn compact(mut self, compact: &'a [u8; COMPACT_PIXELS]) -> Self {
        self.compact = Some(compact);
        self
    }

    /// Blends every bucket whose lead differs from `prev` toward white by
    /// `intensity` (0 to 1).
    #[must_use]
    pub const fn flash(mut self, prev: &'a [u8; BUCKETS], intensity: f64) -> Self {
        self.flash = Some((prev, intensity));
        self
    }

    /// The color of a pixel with lead `index`, flashed when `flashing`.
    fn color(&self, index: u8, flashing: bool) -> Rgb {
        let base = self
            .palette
            .get(usize::from(index))
            .copied()
            .filter(|_| index != NO_LEAD)
            .unwrap_or(theme::FAINT);
        match self.flash {
            Some((_, intensity)) if flashing => blend(base, theme::WHITE, intensity),
            _ => base,
        }
    }
}

impl Widget for Mosaic<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let prev = self.flash.map(|(prev, _)| prev);
        let (width, _) = pixel_grid(self.lead, self.compact);
        let flash_pairs = prev.map(|prev| cell_flash(self.lead, prev, self.compact.is_some()));
        for (row, pairs) in cell_pairs(self.lead, self.compact).into_iter().enumerate() {
            let Ok(y) = u16::try_from(row) else { break };
            if y >= area.height {
                break;
            }
            for (column, (upper, lower)) in pairs.into_iter().enumerate().take(width) {
                let Ok(x) = u16::try_from(column) else { break };
                if x >= area.width {
                    break;
                }
                let (flash_upper, flash_lower) = flash_pairs
                    .as_ref()
                    .map_or((false, false), |flags| flags[row][column]);
                let style = Style::new()
                    .fg(self.color(upper, flash_upper).color(self.mode))
                    .bg(self.color(lower, flash_lower).color(self.mode));
                if let Some(cell) = buf.cell_mut((area.x + x, area.y + y)) {
                    cell.set_char('▀').set_style(style);
                }
            }
        }
    }
}

/// For each cell, whether its upper and lower pixel changed lead between
/// `prev` and `lead`. A compact pixel changed when either of its buckets did.
fn cell_flash(lead: &[u8; BUCKETS], prev: &[u8; BUCKETS], compact: bool) -> Vec<Vec<(bool, bool)>> {
    let flags: [bool; BUCKETS] = std::array::from_fn(|b| changed(Some(prev), lead, b));
    let per_pixel = if compact { 2 } else { 1 };
    let width = COLUMNS / per_pixel;
    let pixel = |row: usize, column: usize| -> bool {
        (0..per_pixel).any(|i| flags[row * COLUMNS + column * per_pixel + i])
    };
    (0..PIXEL_ROWS / 2)
        .map(|row| {
            (0..width)
                .map(|column| (pixel(2 * row, column), pixel(2 * row + 1, column)))
                .collect()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lead_of(f: impl Fn(usize) -> u8) -> [u8; BUCKETS] {
        std::array::from_fn(f)
    }

    #[test]
    fn full_grid_places_bucket_b_at_column_b_mod_64_and_row_b_div_64() {
        let lead = lead_of(|b| u8::try_from(b % 251).unwrap());
        let (width, pixels) = pixel_grid(&lead, None);
        assert_eq!(width, 64);
        assert_eq!(pixels.len(), 1024);
        for b in [0usize, 1, 63, 64, 65, 500, 1023] {
            assert_eq!(pixels[(b / 64) * width + b % 64], lead[b]);
        }
    }

    #[test]
    fn cell_pairs_stack_two_pixel_rows_per_cell() {
        let lead = lead_of(|b| u8::try_from(b / 64).unwrap());
        let cells = cell_pairs(&lead, None);
        assert_eq!(cells.len(), 8);
        assert!(cells.iter().all(|row| row.len() == 64));
        for (row, line) in cells.iter().enumerate() {
            let expected = (
                u8::try_from(2 * row).unwrap(),
                u8::try_from(2 * row + 1).unwrap(),
            );
            assert!(line.iter().all(|&pair| pair == expected));
        }
    }

    #[test]
    fn compact_grid_is_half_as_wide_and_takes_the_compact_leads() {
        let lead = [0u8; BUCKETS];
        let compact: [u8; COMPACT_PIXELS] = std::array::from_fn(|p| u8::try_from(p % 7).unwrap());
        let (width, pixels) = pixel_grid(&lead, Some(&compact));
        assert_eq!(width, 32);
        assert_eq!(pixels, compact.to_vec());
        assert_eq!(cell_pairs(&lead, Some(&compact)).len(), 8);
        assert!(
            cell_pairs(&lead, Some(&compact))
                .iter()
                .all(|row| row.len() == 32)
        );
    }

    #[test]
    fn a_compact_pixel_draws_the_pair_lead_not_the_lower_index() {
        // Buckets 0 and 1 lead n5 and n1, yet the pair's 128 parts lead n5.
        let mut lead = [0u8; BUCKETS];
        lead[0] = 4;
        lead[1] = 0;
        let mut compact = [0u8; COMPACT_PIXELS];
        compact[0] = 4;
        let palette: Vec<Rgb> = (0..5).map(|i| Rgb(i * 10, 0, 0)).collect();
        let area = Rect::new(0, 0, 32, 8);
        let mut buf = Buffer::empty(area);
        Mosaic::new(&lead, &palette, ColorMode::Truecolor)
            .compact(&compact)
            .render(area, &mut buf);
        let cell = buf.cell((0, 0)).unwrap();
        assert_eq!(cell.fg, Rgb(40, 0, 0).color(ColorMode::Truecolor));
        assert_eq!(
            buf.cell((1, 0)).unwrap().fg,
            Rgb(0, 0, 0).color(ColorMode::Truecolor)
        );
    }

    #[test]
    fn a_single_node_paints_every_cell_in_its_color() {
        let lead = [0u8; BUCKETS];
        let palette = [Rgb(10, 20, 30)];
        let area = Rect::new(0, 0, 64, 8);
        let mut buf = Buffer::empty(area);
        Mosaic::new(&lead, &palette, ColorMode::Truecolor).render(area, &mut buf);
        let want = Rgb(10, 20, 30).color(ColorMode::Truecolor);
        for y in 0..8 {
            for x in 0..64 {
                let cell = buf.cell((x, y)).unwrap();
                assert_eq!(cell.symbol(), "▀");
                assert_eq!((cell.fg, cell.bg), (want, want));
            }
        }
    }

    #[test]
    fn a_bucket_with_no_lead_draws_faint() {
        let lead = [NO_LEAD; BUCKETS];
        let area = Rect::new(0, 0, 64, 8);
        let mut buf = Buffer::empty(area);
        Mosaic::new(&lead, &[Rgb(1, 1, 1)], ColorMode::Truecolor).render(area, &mut buf);
        assert_eq!(
            buf.cell((0, 0)).unwrap().fg,
            theme::FAINT.color(ColorMode::Truecolor)
        );
    }

    #[test]
    fn a_changed_bucket_flashes_toward_white_and_others_hold() {
        let prev = [0u8; BUCKETS];
        let mut lead = [0u8; BUCKETS];
        // Bucket 65 is column 1 of pixel row 1: the lower half of cell (0, 1).
        lead[65] = 1;
        let palette = [Rgb(0, 0, 0), Rgb(100, 100, 100)];
        let area = Rect::new(0, 0, 64, 8);
        let mut buf = Buffer::empty(area);
        Mosaic::new(&lead, &palette, ColorMode::Truecolor)
            .flash(&prev, 1.0)
            .render(area, &mut buf);
        let cell = buf.cell((1, 0)).unwrap();
        assert_eq!(cell.bg, theme::WHITE.color(ColorMode::Truecolor));
        assert_eq!(cell.fg, Rgb(0, 0, 0).color(ColorMode::Truecolor));
        assert_eq!(
            buf.cell((2, 0)).unwrap().bg,
            Rgb(0, 0, 0).color(ColorMode::Truecolor)
        );
    }

    #[test]
    fn a_flash_of_zero_intensity_changes_nothing() {
        let prev = [0u8; BUCKETS];
        let lead = [1u8; BUCKETS];
        let palette = [Rgb(0, 0, 0), Rgb(100, 100, 100)];
        let area = Rect::new(0, 0, 64, 8);
        let (mut plain, mut flashed) = (Buffer::empty(area), Buffer::empty(area));
        Mosaic::new(&lead, &palette, ColorMode::Truecolor).render(area, &mut plain);
        Mosaic::new(&lead, &palette, ColorMode::Truecolor)
            .flash(&prev, 0.0)
            .render(area, &mut flashed);
        assert_eq!(plain, flashed);
    }

    #[test]
    fn compact_mode_flashes_a_pair_when_either_bucket_changed() {
        let prev = [0u8; BUCKETS];
        let mut lead = [0u8; BUCKETS];
        lead[1] = 1;
        let palette = [Rgb(0, 0, 0), Rgb(100, 100, 100)];
        let area = Rect::new(0, 0, 32, 8);
        let mut buf = Buffer::empty(area);
        let compact = [0u8; COMPACT_PIXELS];
        Mosaic::new(&lead, &palette, ColorMode::Truecolor)
            .compact(&compact)
            .flash(&prev, 1.0)
            .render(area, &mut buf);
        assert_eq!(
            buf.cell((0, 0)).unwrap().fg,
            theme::WHITE.color(ColorMode::Truecolor)
        );
        assert_eq!(
            buf.cell((1, 0)).unwrap().fg,
            Rgb(0, 0, 0).color(ColorMode::Truecolor)
        );
    }

    #[test]
    fn rendering_into_a_small_area_clips() {
        let lead = [0u8; BUCKETS];
        let area = Rect::new(0, 0, 10, 3);
        let mut buf = Buffer::empty(area);
        Mosaic::new(&lead, &[Rgb(1, 2, 3)], ColorMode::Truecolor).render(area, &mut buf);
        assert_eq!(buf.cell((9, 2)).unwrap().symbol(), "▀");
    }
}

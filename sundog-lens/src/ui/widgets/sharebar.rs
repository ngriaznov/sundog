//! Ownership share bars: a stacked strip split in proportion to part counts,
//! and a single bar with a fair-share marker.

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::widgets::Widget;

use super::count_f64;
use crate::ui::theme::{self, ColorMode, Rgb};

/// Splits `width` cells in proportion to `counts` by the largest-remainder
/// method. The segments sum to `width` whenever any count is nonzero, a zero
/// count gets a zero segment, and each segment is within one cell of its
/// exact share. All counts zero gives all-zero segments.
#[must_use]
pub fn segments(counts: &[u64], width: usize) -> Vec<u16> {
    let total: u128 = counts.iter().map(|&c| u128::from(c)).sum();
    if total == 0 {
        return vec![0; counts.len()];
    }
    let width_u = width as u128;
    // Floor of each exact share, and the remainder that orders the spare cells.
    let mut cells: Vec<u128> = counts
        .iter()
        .map(|&c| u128::from(c) * width_u / total)
        .collect();
    let mut order: Vec<(u128, usize)> = counts
        .iter()
        .enumerate()
        .map(|(i, &c)| (u128::from(c) * width_u % total, i))
        .collect();
    // Largest remainder first; the lower index wins a tie.
    order.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    let spare = width - usize::try_from(cells.iter().sum::<u128>()).unwrap_or(width);
    // The remainders sum to `total * spare` and each is below `total`, so at
    // least `spare` of them are positive: the spare cells go to nonzero counts.
    for &(_, i) in order.iter().take(spare) {
        cells[i] += 1;
    }
    cells
        .iter()
        .map(|&c| u16::try_from(c).unwrap_or(u16::MAX))
        .collect()
}

/// A stacked bar of node colors, one `█` run per count.
#[derive(Debug, Clone)]
pub struct ShareStrip<'a> {
    counts: &'a [u64],
    colors: &'a [Rgb],
    mode: ColorMode,
}

impl<'a> ShareStrip<'a> {
    /// A strip of `counts`, where entry `i` takes `colors[i % colors.len()]`.
    #[must_use]
    pub const fn new(counts: &'a [u64], colors: &'a [Rgb], mode: ColorMode) -> Self {
        Self {
            counts,
            colors,
            mode,
        }
    }
}

impl Widget for ShareStrip<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        if self.colors.is_empty() {
            return;
        }
        let mut x = area.x;
        for (i, cells) in segments(self.counts, usize::from(area.width))
            .into_iter()
            .enumerate()
        {
            let style = Style::new().fg(self.colors[i % self.colors.len()].color(self.mode));
            for _ in 0..cells {
                if let Some(cell) = buf.cell_mut((x, area.y)) {
                    cell.set_char('█').set_style(style);
                }
                x += 1;
            }
        }
    }
}

/// What one cell of a [`share_bar`] holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BarCell {
    /// `━`, inside the filled share.
    Filled,
    /// `╸`, the half-filled cell at the end of the share.
    Head,
    /// `─`, beyond the share.
    Empty,
    /// `┊`, the fair-share marker.
    Marker,
}

impl BarCell {
    /// The character that draws this cell.
    #[must_use]
    pub const fn glyph(self) -> char {
        match self {
            Self::Filled => '━',
            Self::Head => '╸',
            Self::Empty => '─',
            Self::Marker => '┊',
        }
    }
}

/// The cells of a bar `width` wide filled to `frac` (clamped to 0 to 1): `━`
/// up to the share, `╸` for a final half cell, `─` beyond. `fair`, when
/// given, puts a `┊` marker on the cell holding that fraction.
#[must_use]
pub fn share_bar(frac: f64, fair: Option<f64>, width: usize) -> Vec<BarCell> {
    let unit = |x: f64| if x.is_nan() { 0.0 } else { x.clamp(0.0, 1.0) };
    let position = unit(frac) * count_f64(width);
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "the position lies between 0 and the width"
    )]
    let full = position.floor() as usize;
    let head = position - position.floor() >= 0.5;
    let mut cells: Vec<BarCell> = (0..width)
        .map(|i| {
            if i < full {
                BarCell::Filled
            } else if i == full && head {
                BarCell::Head
            } else {
                BarCell::Empty
            }
        })
        .collect();
    if let Some(fair) = fair {
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "the position lies between 0 and the width"
        )]
        let at = (unit(fair) * count_f64(width)).floor() as usize;
        if let Some(cell) = cells.get_mut(at.min(width.saturating_sub(1))) {
            *cell = BarCell::Marker;
        }
    }
    cells
}

/// A [`share_bar`] in a node's color, with the marker in the text color.
#[derive(Debug, Clone, Copy)]
pub struct ShareBar {
    frac: f64,
    fair: Option<f64>,
    color: Rgb,
    mode: ColorMode,
}

impl ShareBar {
    /// A bar filled to `frac` in `color`, with an optional fair-share mark.
    #[must_use]
    pub const fn new(frac: f64, fair: Option<f64>, color: Rgb, mode: ColorMode) -> Self {
        Self {
            frac,
            fair,
            color,
            mode,
        }
    }
}

impl Widget for ShareBar {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let fill = Style::new().fg(self.color.color(self.mode));
        let empty = Style::new().fg(theme::FAINT.color(self.mode));
        let marker = Style::new().fg(theme::TEXT.color(self.mode));
        for (i, bar_cell) in share_bar(self.frac, self.fair, usize::from(area.width))
            .into_iter()
            .enumerate()
        {
            let style = match bar_cell {
                BarCell::Filled | BarCell::Head => fill,
                BarCell::Empty => empty,
                BarCell::Marker => marker,
            };
            let Ok(offset) = u16::try_from(i) else { break };
            if let Some(cell) = buf.cell_mut((area.x + offset, area.y)) {
                cell.set_char(bar_cell.glyph()).set_style(style);
            }
        }
    }
}

/// A [`share_bar`] as styled spans, for a table row: the share in `color`,
/// the empty cells faint and the marker in the text color. Runs of one style
/// are one span.
#[must_use]
pub fn bar_spans(
    frac: f64,
    fair: Option<f64>,
    width: usize,
    color: Rgb,
    look: crate::ui::look::Look,
) -> Vec<ratatui::text::Span<'static>> {
    use crate::ui::look::Token;
    let fill = look.node(color);
    let empty = look.style(Token::Faint);
    let marker = look.style(Token::Text);
    let mut spans: Vec<ratatui::text::Span<'static>> = Vec::new();
    let mut run = String::new();
    let mut run_style = None;
    for bar_cell in share_bar(frac, fair, width) {
        let style = match bar_cell {
            BarCell::Filled | BarCell::Head => fill,
            BarCell::Empty => empty,
            BarCell::Marker => marker,
        };
        if run_style.is_some_and(|held| held != style) {
            spans.push(ratatui::text::Span::styled(
                std::mem::take(&mut run),
                run_style.unwrap_or_default(),
            ));
        }
        run_style = Some(style);
        run.push(bar_cell.glyph());
    }
    if let Some(style) = run_style {
        spans.push(ratatui::text::Span::styled(run, style));
    }
    spans
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A small deterministic generator, so the random tests repeat.
    struct Xorshift(u64);

    impl Xorshift {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
    }

    fn text(cells: &[BarCell]) -> String {
        cells.iter().map(|c| c.glyph()).collect()
    }

    #[test]
    fn segments_sum_to_the_width_over_random_inputs() {
        let mut rng = Xorshift(0x9E37_79B9_7F4A_7C15);
        for _ in 0..1000 {
            let n = usize::try_from(rng.next() % 9).unwrap() + 1;
            let counts: Vec<u64> = (0..n).map(|_| rng.next() % 70_000).collect();
            let width = usize::try_from(rng.next() % 200).unwrap();
            let segs = segments(&counts, width);
            assert_eq!(segs.len(), n);
            if counts.iter().all(|&c| c == 0) {
                assert!(segs.iter().all(|&s| s == 0));
                continue;
            }
            assert_eq!(segs.iter().map(|&s| usize::from(s)).sum::<usize>(), width);
            let total: u64 = counts.iter().sum();
            for (&count, &seg) in counts.iter().zip(&segs) {
                #[expect(clippy::cast_precision_loss, reason = "test sizes are small")]
                let exact = count as f64 / total as f64 * width as f64;
                assert!(
                    (f64::from(seg) - exact).abs() < 1.0 + 1e-9,
                    "{counts:?} {segs:?}"
                );
                if count == 0 {
                    assert_eq!(seg, 0);
                }
            }
        }
    }

    #[test]
    fn segments_split_known_shares() {
        assert_eq!(segments(&[1, 1], 10), [5, 5]);
        assert_eq!(segments(&[1, 1, 1], 10), [4, 3, 3]);
        assert_eq!(segments(&[3, 1], 8), [6, 2]);
        assert_eq!(segments(&[0, 5, 0], 7), [0, 7, 0]);
        assert_eq!(segments(&[], 7), Vec::<u16>::new());
        assert_eq!(segments(&[0, 0], 7), [0, 0]);
        assert_eq!(segments(&[5, 5], 0), [0, 0]);
    }

    #[test]
    fn share_strip_paints_each_node_run_in_its_color() {
        let colors = [Rgb(1, 2, 3), Rgb(4, 5, 6)];
        let mut buf = Buffer::empty(Rect::new(0, 0, 6, 1));
        ShareStrip::new(&[1, 2], &colors, ColorMode::Truecolor)
            .render(Rect::new(0, 0, 6, 1), &mut buf);
        let fg = |x| buf.cell((x, 0)).unwrap().fg;
        assert_eq!(fg(0), Rgb(1, 2, 3).color(ColorMode::Truecolor));
        assert_eq!(fg(1), Rgb(1, 2, 3).color(ColorMode::Truecolor));
        assert_eq!(fg(2), Rgb(4, 5, 6).color(ColorMode::Truecolor));
        assert_eq!(fg(5), Rgb(4, 5, 6).color(ColorMode::Truecolor));
        assert_eq!(buf.cell((3, 0)).unwrap().symbol(), "█");
    }

    #[test]
    fn share_strip_without_colors_draws_nothing() {
        let mut buf = Buffer::empty(Rect::new(0, 0, 4, 1));
        ShareStrip::new(&[1, 1], &[], ColorMode::Mono).render(Rect::new(0, 0, 4, 1), &mut buf);
        assert_eq!(buf, Buffer::empty(Rect::new(0, 0, 4, 1)));
    }

    #[test]
    fn share_bar_fills_to_the_share_with_a_half_cell_head() {
        assert_eq!(text(&share_bar(0.0, None, 6)), "──────");
        assert_eq!(text(&share_bar(1.0, None, 6)), "━━━━━━");
        assert_eq!(text(&share_bar(0.5, None, 6)), "━━━───");
        assert_eq!(text(&share_bar(0.25, None, 6)), "━╸────");
        assert_eq!(text(&share_bar(0.2, None, 6)), "━─────");
    }

    #[test]
    fn share_bar_puts_the_fair_marker_on_its_cell() {
        assert_eq!(text(&share_bar(0.5, Some(0.5), 6)), "━━━┊──");
        assert_eq!(text(&share_bar(0.0, Some(0.0), 6)), "┊─────");
        assert_eq!(text(&share_bar(0.5, Some(1.0), 6)), "━━━──┊");
    }

    #[test]
    fn share_bar_clamps_and_keeps_its_width() {
        assert_eq!(text(&share_bar(-2.0, None, 3)), "───");
        assert_eq!(text(&share_bar(9.0, None, 3)), "━━━");
        assert_eq!(text(&share_bar(f64::NAN, Some(f64::NAN), 3)), "┊──");
        assert!(share_bar(0.5, Some(0.5), 0).is_empty());
        for width in 0..12 {
            assert_eq!(share_bar(0.37, Some(0.5), width).len(), width);
        }
    }

    #[test]
    fn share_bar_widget_colors_fill_empty_and_marker() {
        let color = Rgb(9, 9, 9);
        let mut buf = Buffer::empty(Rect::new(0, 0, 6, 1));
        ShareBar::new(0.5, Some(0.5), color, ColorMode::Truecolor)
            .render(Rect::new(0, 0, 6, 1), &mut buf);
        let cell = |x| buf.cell((x, 0)).unwrap().clone();
        assert_eq!(cell(0).symbol(), "━");
        assert_eq!(cell(0).fg, color.color(ColorMode::Truecolor));
        assert_eq!(cell(3).symbol(), "┊");
        assert_eq!(cell(3).fg, theme::TEXT.color(ColorMode::Truecolor));
        assert_eq!(cell(5).symbol(), "─");
        assert_eq!(cell(5).fg, theme::FAINT.color(ColorMode::Truecolor));
    }

    #[test]
    fn bar_spans_group_runs_of_one_style() {
        let look = crate::ui::look::Look::default();
        let spans = bar_spans(0.5, Some(0.5), 10, theme::NODE_COLORS[0], look);
        let text: String = spans.iter().map(|s| s.content.as_ref()).collect();
        assert_eq!(text, "━━━━━┊────");
        assert_eq!(spans.len(), 3, "fill, marker, empty");
        assert_eq!(spans[0].content, "━━━━━");
        assert!(bar_spans(0.5, None, 0, theme::NODE_COLORS[0], look).is_empty());
        let plain = bar_spans(1.0, None, 4, theme::NODE_COLORS[0], look);
        assert_eq!(plain.len(), 1);
    }
}

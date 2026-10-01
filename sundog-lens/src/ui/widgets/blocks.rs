//! Block-character sparklines and bars: the `--no-braille` fallback and the
//! horizontal eighths bar.

use super::count_f64;

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
}

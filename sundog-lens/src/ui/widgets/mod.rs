//! Drawing primitives. Each widget is a thin `ratatui` wrapper around pure
//! functions that compute its cells, so the tests pin the cells without a
//! terminal.

pub mod blocks;
pub mod braille;
pub mod keycap;
pub mod lifeline;
pub mod mosaic;
pub mod sharebar;

/// A cell or sample count as a float.
#[expect(
    clippy::cast_precision_loss,
    reason = "counts of cells and samples are far below 2^52"
)]
pub(crate) fn count_f64(count: usize) -> f64 {
    count as f64
}

/// A sparkline `width` cells wide over the newest samples of `values`:
/// braille (two samples per cell) or, without braille, blocks (one per cell).
/// A quiet sample draws a baseline; only missing history is blank.
#[must_use]
pub fn spark(values: &[f64], width: usize, braille: bool) -> String {
    if braille {
        braille::spark_braille_floor(values, width)
    } else {
        blocks::spark_blocks_floor(values, width)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_spark_is_braille_or_blocks_by_choice() {
        let values = [1.0, 2.0, 4.0, 8.0];
        assert!(
            spark(&values, 2, true)
                .chars()
                .all(|c| ('\u{2800}'..='\u{28FF}').contains(&c))
        );
        assert_eq!(spark(&values, 4, false), "▁▂▄█");
        assert_eq!(spark(&[0.0; 4], 2, true), "⣀⣀");
        assert_eq!(spark(&[0.0, 0.0], 2, true), "\u{2800}⣀");
        assert_eq!(spark(&[0.0, 0.0], 2, false), "▁▁");
        assert_eq!(spark(&values, 2, true).chars().count(), 2);
    }
}

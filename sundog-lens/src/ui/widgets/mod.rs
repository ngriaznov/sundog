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

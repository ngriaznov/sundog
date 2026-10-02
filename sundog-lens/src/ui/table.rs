//! Tables of styled cells: columns with a width and a keep priority, so a
//! narrow panel drops its least important columns first.

use ratatui::text::{Line, Span};

use super::look::{Look, Token};
use super::panel::{gap, pad_spans, width_of};
use super::text;

/// One column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Col {
    /// The header text.
    pub title: String,
    /// The width of the column, its trailing gap included.
    pub width: usize,
    /// Whether the header is right-aligned over a right-aligned column.
    pub right: bool,
    /// How long the column survives a narrow panel: the column with the
    /// lowest value goes first.
    pub keep: u8,
}

impl Col {
    /// A left-aligned column.
    #[must_use]
    pub fn new(title: impl Into<String>, width: usize, keep: u8) -> Self {
        Self {
            title: title.into(),
            width,
            right: false,
            keep,
        }
    }

    /// A right-aligned column.
    #[must_use]
    pub fn right(title: impl Into<String>, width: usize, keep: u8) -> Self {
        Self {
            right: true,
            ..Self::new(title, width, keep)
        }
    }
}

/// The cells added after a right-aligned column that another column follows:
/// its numbers end at the edge of the column, so the extra cell keeps two
/// cells between them and the next column's text.
const AFTER_RIGHT: usize = 1;

/// The cells that follow a cell wider than its column.
const MIN_GAP: usize = 2;

/// The cells that follow the column at `position` among `visible`.
fn trailing(cols: &[Col], visible: &[usize], position: usize) -> usize {
    if position + 1 < visible.len() && cols[visible[position]].right {
        AFTER_RIGHT
    } else {
        0
    }
}

/// The width of the visible columns with the gaps after right-aligned ones.
#[must_use]
pub fn total_width(cols: &[Col], visible: &[usize]) -> usize {
    visible
        .iter()
        .enumerate()
        .map(|(position, &i)| cols[i].width + trailing(cols, visible, position))
        .sum()
}

/// The indices of the columns that fit in `width` cells, in their order:
/// columns go in ascending `keep` order until the rest fit. A column with the
/// highest `keep` always stays. The width of a column counts the gap after
/// it, and a right-aligned column that another follows gets one more cell
/// (see [`total_width`]).
#[must_use]
pub fn visible(cols: &[Col], width: usize) -> Vec<usize> {
    let mut kept: Vec<usize> = (0..cols.len()).collect();
    let total = |kept: &[usize]| total_width(cols, kept);
    while kept.len() > 1 && total(&kept) > width {
        let drop = kept
            .iter()
            .enumerate()
            .min_by_key(|&(position, &i)| (cols[i].keep, std::cmp::Reverse(position)))
            .map(|(position, _)| position);
        match drop {
            Some(position) => {
                kept.remove(position);
            }
            None => break,
        }
    }
    kept
}

/// The header line of the visible columns.
#[must_use]
pub fn header(look: Look, cols: &[Col], visible: &[usize]) -> Line<'static> {
    let mut spans: Vec<Span<'static>> = Vec::new();
    for (position, &i) in visible.iter().enumerate() {
        let col = &cols[i];
        let cell = if col.right {
            text::pad_left(&format!("{} ", col.title), col.width)
        } else {
            text::pad_right(&col.title, col.width)
        };
        spans.push(look.span(cell, Token::Muted));
        spans.push(gap(trailing(cols, visible, position)));
    }
    Line::from(spans)
}

/// A row from `cells`, one per column (the visible ones are picked and
/// padded to their widths).
#[must_use]
pub fn row(cols: &[Col], visible: &[usize], mut cells: Vec<Vec<Span<'static>>>) -> Line<'static> {
    let mut spans = Vec::new();
    for (position, &i) in visible.iter().enumerate() {
        let cell = std::mem::take(&mut cells[i]);
        if width_of(&cell) > cols[i].width {
            spans.extend(cell);
            spans.push(gap(MIN_GAP));
        } else {
            spans.extend(pad_spans(cell, cols[i].width));
        }
        spans.push(gap(trailing(cols, visible, position)));
    }
    Line::from(spans)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cols() -> Vec<Col> {
        vec![
            Col::new("NODE", 6, 9),
            Col::right("IN/s", 7, 5),
            Col::right("HIT%", 6, 3),
            Col::new("FETCH", 10, 1),
        ]
    }

    fn text_of(line: &Line<'_>) -> String {
        line.spans.iter().map(|s| s.content.as_ref()).collect()
    }

    #[test]
    fn every_column_shows_when_there_is_room() {
        assert_eq!(visible(&cols(), 31), [0, 1, 2, 3]);
        assert_eq!(visible(&cols(), 200), [0, 1, 2, 3]);
    }

    #[test]
    fn the_lowest_keep_goes_first_and_the_widest_priority_always_stays() {
        assert_eq!(visible(&cols(), 30), [0, 1, 2]);
        assert_eq!(visible(&cols(), 19), [0, 1]);
        assert_eq!(visible(&cols(), 12), [0]);
        assert_eq!(visible(&cols(), 0), [0]);
        let rows = visible(&[], 10);
        assert!(rows.is_empty(), "{rows:?}");
    }

    #[test]
    fn equal_priorities_drop_from_the_right() {
        let tied = vec![
            Col::new("A", 4, 1),
            Col::new("B", 4, 1),
            Col::new("C", 4, 1),
        ];
        assert_eq!(visible(&tied, 8), [0, 1]);
    }

    #[test]
    fn headers_align_with_their_columns() {
        let cols = cols();
        let line = header(Look::default(), &cols, &[0, 1, 2, 3]);
        assert_eq!(text_of(&line), "NODE    IN/s   HIT%  FETCH     ");
        assert_eq!(text_of(&line).chars().count(), 31);
    }

    #[test]
    fn rows_pad_each_cell_to_its_column_and_skip_hidden_columns() {
        let cols = cols();
        let cells = vec![
            vec![Span::raw("n1")],
            vec![Span::raw("   12")],
            vec![Span::raw("  9")],
            vec![Span::raw("61/30")],
        ];
        let line = row(&cols, &[0, 1, 3], cells);
        assert_eq!(text_of(&line), "n1       12   61/30     ");
    }

    #[test]
    fn a_cell_wider_than_its_column_is_kept_whole() {
        let cols = vec![Col::new("A", 3, 1)];
        let line = row(&cols, &[0], vec![vec![Span::raw("wide")]]);
        assert_eq!(text_of(&line), "wide  ");
    }

    #[test]
    fn two_cells_part_a_right_aligned_column_from_the_next_and_none_trail_the_last() {
        let cols = vec![Col::right("HIT%", 7, 1), Col::new("OWNED", 7, 1)];
        let cells = vec![vec![Span::raw(" 77.5%")], vec![Span::raw("65,536")]];
        let both = [0, 1];
        let row_text = text_of(&row(&cols, &both, cells));
        assert_eq!(row_text, " 77.5%  65,536 ");
        assert!(row_text.contains("%  6"), "{row_text}");
        assert_eq!(total_width(&cols, &both), 15);
        assert_eq!(total_width(&cols, &[0]), 7, "a lone column has no neighbor");
        assert_eq!(total_width(&cols, &[1]), 7);
        // A trailing right-aligned column keeps its own width only.
        let rev = vec![Col::new("A", 3, 1), Col::right("B", 4, 1)];
        assert_eq!(total_width(&rev, &[0, 1]), 7);
    }
}

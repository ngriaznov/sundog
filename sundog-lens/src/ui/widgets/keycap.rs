//! Footer key hints: a key cap followed by its label.

use ratatui::style::{Modifier, Style};
use ratatui::text::Span;

use crate::ui::theme::{self, ColorMode};

/// The gap between two hints, in cells.
const GAP: usize = 2;

/// A hint's width in cells: the key, a space and the label.
fn hint_width(key: &str, label: &str) -> usize {
    key.chars().count() + 1 + label.chars().count()
}

/// The width in cells of `hints` laid out by [`keycap_spans`], without the
/// leading space.
#[must_use]
pub fn keycaps_width(hints: &[(&str, &str)]) -> usize {
    hints
        .iter()
        .map(|(key, label)| hint_width(key, label))
        .sum::<usize>()
        + GAP * hints.len().saturating_sub(1)
}

/// The longest prefix of `hints` that fits in `width` cells.
#[must_use]
pub fn fit<'a>(hints: &'a [(&'a str, &'a str)], width: usize) -> &'a [(&'a str, &'a str)] {
    let mut used = 0;
    for (count, (key, label)) in hints.iter().enumerate() {
        let next = used + hint_width(key, label) + if count > 0 { GAP } else { 0 };
        if next > width {
            return &hints[..count];
        }
        used = next;
    }
    hints
}

/// The longest prefix of `hints` that fits in `width` cells beside the last
/// `keep` hints, which stay whatever the width: the hints that fit, then the
/// last `keep`. When every hint fits, that is all of them.
///
/// The tail is never cut, so a `width` below its own width draws it past the
/// edge of the row; the callers' rows are at least 80 cells wide.
#[must_use]
pub fn fit_keeping<'a>(
    hints: &[(&'a str, &'a str)],
    keep: usize,
    width: usize,
) -> Vec<(&'a str, &'a str)> {
    let split = hints.len().saturating_sub(keep);
    let (head, tail) = hints.split_at(split);
    if keycaps_width(hints) <= width {
        return hints.to_vec();
    }
    let room = width.saturating_sub(keycaps_width(tail) + if tail.is_empty() { 0 } else { GAP });
    let mut kept = head[..fit(head, room).len()].to_vec();
    kept.extend_from_slice(tail);
    kept
}

/// The spans for `hints`: the key bold in the accent color, the label muted,
/// two spaces between hints. In mono the key is bold and the label dim.
#[must_use]
pub fn keycap_spans(hints: &[(&str, &str)], mode: ColorMode) -> Vec<Span<'static>> {
    let key_style = Style::new()
        .fg(theme::ACCENT.color(mode))
        .add_modifier(Modifier::BOLD);
    let label_style = if mode == ColorMode::Mono {
        Style::new().add_modifier(Modifier::DIM)
    } else {
        Style::new().fg(theme::MUTED.color(mode))
    };
    let mut spans = Vec::with_capacity(hints.len() * 3);
    for (i, (key, label)) in hints.iter().enumerate() {
        if i > 0 {
            spans.push(Span::raw(" ".repeat(GAP)));
        }
        spans.push(Span::styled((*key).to_owned(), key_style));
        spans.push(Span::styled(format!(" {label}"), label_style));
    }
    spans
}

#[cfg(test)]
mod tests {
    use super::*;

    const HINTS: [(&str, &str); 3] = [("1-4", "view"), ("q", "quit"), ("?", "help")];

    fn line(spans: &[Span<'_>]) -> String {
        spans.iter().map(|s| s.content.as_ref()).collect()
    }

    #[test]
    fn spans_lay_out_key_label_and_gap() {
        let spans = keycap_spans(&HINTS, ColorMode::Truecolor);
        assert_eq!(line(&spans), "1-4 view  q quit  ? help");
    }

    #[test]
    fn width_matches_the_laid_out_line() {
        let spans = keycap_spans(&HINTS, ColorMode::Truecolor);
        assert_eq!(keycaps_width(&HINTS), line(&spans).chars().count());
        assert_eq!(keycaps_width(&[]), 0);
    }

    #[test]
    fn keys_are_bold_accent_and_labels_muted() {
        let spans = keycap_spans(&HINTS[..1], ColorMode::Truecolor);
        assert_eq!(
            spans[0].style.fg,
            Some(theme::ACCENT.color(ColorMode::Truecolor))
        );
        assert!(spans[0].style.add_modifier.contains(Modifier::BOLD));
        assert_eq!(
            spans[1].style.fg,
            Some(theme::MUTED.color(ColorMode::Truecolor))
        );
    }

    #[test]
    fn mono_uses_modifiers_only() {
        let spans = keycap_spans(&HINTS[..1], ColorMode::Mono);
        assert!(spans[0].style.add_modifier.contains(Modifier::BOLD));
        assert!(spans[1].style.add_modifier.contains(Modifier::DIM));
        assert_eq!(spans[1].style.fg, None);
    }

    #[test]
    fn fit_keeping_drops_hints_before_the_kept_tail() {
        let hints = [
            ("1-4", "view"),
            ("c", "cache"),
            ("e", "explain"),
            ("?", "help"),
            ("q", "quit"),
        ];
        let shown = |width| {
            let kept = fit_keeping(&hints, 2, width);
            line(&keycap_spans(&kept, ColorMode::Truecolor))
        };
        let all = keycaps_width(&hints);
        assert_eq!(shown(all), "1-4 view  c cache  e explain  ? help  q quit");
        assert_eq!(shown(all - 1), "1-4 view  c cache  ? help  q quit");
        assert_eq!(shown(all - 11), "1-4 view  c cache  ? help  q quit");
        assert_eq!(shown(all - 12), "1-4 view  ? help  q quit");
        // Below the tail's own width the tail stays and nothing precedes it.
        assert_eq!(shown(0), "? help  q quit");
        assert_eq!(shown(keycaps_width(&hints[3..])), "? help  q quit");
        // Without a tail it is `fit`.
        assert_eq!(fit_keeping(&hints, 0, 12), fit(&hints, 12));
        // A tail longer than the list keeps every hint.
        assert_eq!(fit_keeping(&hints, 9, 0), hints);
    }

    #[test]
    fn fit_keeps_the_longest_prefix_that_fits() {
        assert_eq!(fit(&HINTS, 100).len(), 3);
        assert_eq!(fit(&HINTS, keycaps_width(&HINTS)).len(), 3);
        assert_eq!(fit(&HINTS, keycaps_width(&HINTS) - 1).len(), 2);
        assert_eq!(fit(&HINTS, keycaps_width(&HINTS[..1])).len(), 1);
        assert_eq!(fit(&HINTS, keycaps_width(&HINTS[..1]) - 1).len(), 0);
        assert_eq!(fit(&HINTS, 0).len(), 0);
    }
}

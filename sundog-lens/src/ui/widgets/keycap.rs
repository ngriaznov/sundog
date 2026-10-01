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
    fn fit_keeps_the_longest_prefix_that_fits() {
        assert_eq!(fit(&HINTS, 100).len(), 3);
        assert_eq!(fit(&HINTS, keycaps_width(&HINTS)).len(), 3);
        assert_eq!(fit(&HINTS, keycaps_width(&HINTS) - 1).len(), 2);
        assert_eq!(fit(&HINTS, keycaps_width(&HINTS[..1])).len(), 1);
        assert_eq!(fit(&HINTS, keycaps_width(&HINTS[..1]) - 1).len(), 0);
        assert_eq!(fit(&HINTS, 0).len(), 0);
    }
}

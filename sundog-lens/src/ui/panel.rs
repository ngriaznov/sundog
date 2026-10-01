//! Panels: the rounded, titled border every view draws its content in, and the
//! small line helpers the views share.

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Paragraph, Widget};

use super::look::{Look, Token};
use super::theme::Rgb;

/// A panel's frame: a rounded border, a bold title with a provenance tag and
/// right-aligned stats. The focused panel's border is amber.
///
/// `tag` names where the content comes from: `gossip`, `metrics` or
/// `computed`.
#[must_use]
pub fn block(
    look: Look,
    title: &str,
    tag: &str,
    right: Vec<Span<'static>>,
    focused: bool,
) -> Block<'static> {
    let mut spans = vec![Span::raw(" "), title_span(look, title)];
    spans.extend(tag_spans(look, tag));
    spans.push(Span::raw(" "));
    block_with(look, spans, right, focused)
}

/// As [`block`], with a title built from spans.
#[must_use]
pub fn block_with(
    look: Look,
    title: Vec<Span<'static>>,
    right: Vec<Span<'static>>,
    focused: bool,
) -> Block<'static> {
    let mut block = Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(look.border(focused))
        .title_top(Line::from(title));
    if !right.is_empty() {
        let mut spans = vec![Span::raw(" ")];
        spans.extend(right);
        spans.push(Span::raw(" "));
        block = block.title_top(Line::from(spans).right_aligned());
    }
    block
}

/// A bold title.
#[must_use]
pub fn title_span(look: Look, title: &str) -> Span<'static> {
    Span::styled(
        title.to_owned(),
        look.style(Token::Text).add_modifier(Modifier::BOLD),
    )
}

/// The ` · tag` that follows a title; empty for no tag. A `computed` tag is
/// purple, because the lens works it out; the others are muted.
#[must_use]
pub fn tag_spans(look: Look, tag: &str) -> Vec<Span<'static>> {
    if tag.is_empty() {
        return Vec::new();
    }
    let token = if tag.starts_with("computed") {
        Token::Move
    } else {
        Token::Muted
    };
    vec![
        look.span(" · ", Token::Faint),
        look.span(tag.to_owned(), token),
    ]
}

/// Draws a panel over `area` and returns the area inside it.
pub fn draw(block: Block<'static>, area: Rect, buf: &mut Buffer) -> Rect {
    let inner = block.inner(area);
    block.render(area, buf);
    inner
}

/// Draws `lines` into `area`, one per row, clipped.
pub fn lines(lines: Vec<Line<'static>>, area: Rect, buf: &mut Buffer) {
    Paragraph::new(lines).render(area, buf);
}

/// Draws `text` centered in `area`, one line per row, vertically centered.
pub fn centered(lines_in: Vec<Line<'static>>, area: Rect, buf: &mut Buffer) {
    let count = u16::try_from(lines_in.len()).unwrap_or(u16::MAX);
    let top = area.height.saturating_sub(count) / 2;
    let target = Rect::new(
        area.x,
        area.y + top,
        area.width,
        area.height.saturating_sub(top),
    );
    Paragraph::new(lines_in.into_iter().map(Line::centered).collect::<Vec<_>>())
        .render(target, buf);
}

/// A line of `spans`.
#[must_use]
pub fn line(spans: Vec<Span<'static>>) -> Line<'static> {
    Line::from(spans)
}

/// A `width`-cell run of spaces.
#[must_use]
pub fn gap(width: usize) -> Span<'static> {
    Span::raw(" ".repeat(width))
}

/// A colored glyph in a node's color.
#[must_use]
pub fn node_glyph(look: Look, glyph: char, color: Rgb) -> Span<'static> {
    look.node_span(glyph.to_string(), color)
}

/// The width of a line in cells.
#[must_use]
pub fn width_of(spans: &[Span<'_>]) -> usize {
    spans.iter().map(|span| span.content.chars().count()).sum()
}

/// Pads `spans` with spaces to `width` cells; longer lines are returned as
/// they are.
#[must_use]
pub fn pad_spans(mut spans: Vec<Span<'static>>, width: usize) -> Vec<Span<'static>> {
    let used = width_of(&spans);
    if used < width {
        spans.push(gap(width - used));
    }
    spans
}

/// `spans` cut to `width` cells: a line that is too long ends in `…`, in the
/// style of the span it was cut in.
#[must_use]
pub fn clip(spans: Vec<Span<'static>>, width: usize) -> Vec<Span<'static>> {
    if width_of(&spans) <= width {
        return spans;
    }
    let mut kept = Vec::new();
    let mut used = 0;
    for span in spans {
        let len = span.content.chars().count();
        if used + len < width {
            used += len;
            kept.push(span);
            continue;
        }
        let room = width.saturating_sub(used + 1);
        let mut cut: String = span.content.chars().take(room).collect();
        cut.push('…');
        kept.push(Span::styled(cut, span.style));
        return kept;
    }
    kept
}

/// The text of a buffer row, trailing spaces trimmed.
#[must_use]
pub fn row_text(buf: &Buffer, y: u16) -> String {
    let area = buf.area;
    let mut row = String::new();
    for x in area.x..area.x + area.width {
        row.push_str(buf[(x, y)].symbol());
    }
    row.trim_end().to_owned()
}

#[cfg(test)]
mod tests {
    use ratatui::style::Color;

    use super::*;

    fn look() -> Look {
        Look::default()
    }

    fn render(block: Block<'static>, w: u16, h: u16) -> Buffer {
        let area = Rect::new(0, 0, w, h);
        let mut buf = Buffer::empty(area);
        block.render(area, &mut buf);
        buf
    }

    #[test]
    fn a_panel_has_a_rounded_border_a_title_a_tag_and_right_stats() {
        let block = block(
            look(),
            "Members",
            "gossip",
            vec![Span::raw("6 seen")],
            false,
        );
        let buf = render(block, 40, 3);
        let top = row_text(&buf, 0);
        assert!(top.starts_with("╭ Members · gossip "), "{top}");
        assert!(top.ends_with(" 6 seen ╮"), "{top}");
        assert_eq!(row_text(&buf, 2), format!("╰{}╯", "─".repeat(38)));
        assert_eq!(buf[(0, 1)].symbol(), "│");
    }

    #[test]
    fn a_panel_without_a_tag_or_stats_shows_only_its_title() {
        let buf = render(block(look(), "Events", "", Vec::new(), false), 20, 3);
        assert_eq!(row_text(&buf, 0), "╭ Events ──────────╮");
    }

    #[test]
    fn the_focused_border_is_amber_and_the_others_are_not() {
        let focused = render(block(look(), "A", "", Vec::new(), true), 10, 3);
        let plain = render(block(look(), "A", "", Vec::new(), false), 10, 3);
        assert_eq!(focused[(0, 0)].fg, Color::Rgb(0xF2, 0xB5, 0x44));
        assert_eq!(plain[(0, 0)].fg, Color::Rgb(0x3A, 0x36, 0x2E));
    }

    #[test]
    fn the_computed_tag_is_purple_and_the_rest_muted() {
        let computed = tag_spans(look(), "computed+metrics");
        let gossip = tag_spans(look(), "gossip");
        assert_eq!(computed[1].style.fg, Some(Color::Rgb(0xB7, 0x9C, 0xFF)));
        assert_eq!(gossip[1].style.fg, Some(Color::Rgb(0x7D, 0x77, 0x6B)));
        assert!(tag_spans(look(), "").is_empty());
    }

    #[test]
    fn drawing_returns_the_inner_area() {
        let area = Rect::new(2, 1, 20, 5);
        let mut buf = Buffer::empty(Rect::new(0, 0, 30, 8));
        let inner = draw(block(look(), "T", "", Vec::new(), false), area, &mut buf);
        assert_eq!(inner, Rect::new(3, 2, 18, 3));
    }

    #[test]
    fn centered_text_sits_in_the_middle_rows() {
        let area = Rect::new(0, 0, 11, 5);
        let mut buf = Buffer::empty(area);
        centered(vec![Line::from("abc")], area, &mut buf);
        assert_eq!(row_text(&buf, 2), "    abc");
        assert_eq!(row_text(&buf, 0), "");
    }

    #[test]
    fn a_long_line_is_cut_with_an_ellipsis_in_the_style_of_the_cut_span() {
        let bold = ratatui::style::Style::new().add_modifier(Modifier::BOLD);
        let spans = vec![
            Span::raw("abc"),
            Span::styled("defgh", bold),
            Span::raw("ij"),
        ];
        let cut = clip(spans.clone(), 6);
        let joined: String = cut.iter().map(|s| s.content.as_ref()).collect();
        assert_eq!(joined, "abcde…");
        assert_eq!(cut.last().unwrap().style, bold);
        assert_eq!(clip(spans.clone(), 10), spans);
        assert_eq!(
            clip(spans.clone(), 3)
                .iter()
                .map(|s| s.content.as_ref())
                .collect::<String>(),
            "ab…"
        );
        assert_eq!(width_of(&clip(spans, 0)), 1);
    }

    #[test]
    fn lines_draw_one_per_row_and_clip() {
        let area = Rect::new(0, 0, 4, 2);
        let mut buf = Buffer::empty(area);
        lines(
            vec![Line::from("abcdef"), Line::from("gh"), Line::from("ij")],
            area,
            &mut buf,
        );
        assert_eq!(row_text(&buf, 0), "abcd");
        assert_eq!(row_text(&buf, 1), "gh");
    }

    #[test]
    fn padding_widens_a_line_and_leaves_a_long_one() {
        let spans = vec![Span::raw("ab")];
        assert_eq!(width_of(&spans), 2);
        assert_eq!(width_of(&pad_spans(spans.clone(), 5)), 5);
        assert_eq!(width_of(&pad_spans(spans, 1)), 2);
        assert_eq!(width_of(&[gap(3)]), 3);
        assert_eq!(line(vec![Span::raw("x")]).width(), 1);
        let glyph = node_glyph(look(), '●', Rgb(1, 2, 3));
        assert_eq!(glyph.content, "●");
        assert_eq!(glyph.style.fg, Some(Color::Rgb(1, 2, 3)));
    }

    #[test]
    fn a_block_with_custom_title_spans_keeps_them_and_adds_the_right_stats() {
        let title = vec![Span::raw(" "), title_span(look(), "Custom"), Span::raw(" ")];
        let block = block_with(look(), title, vec![Span::raw("7")], true);
        let buf = render(block, 30, 3);
        let top = row_text(&buf, 0);
        assert!(top.starts_with("╭ Custom "), "{top}");
        assert!(top.ends_with(" 7 ╮"), "{top}");
        let bold = title_span(look(), "T");
        assert!(bold.style.add_modifier.contains(Modifier::BOLD));
        assert_eq!(bold.content, "T");
    }
}

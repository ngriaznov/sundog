//! The Timeline view: each node's life as a lifeline over the span the lens
//! has watched, up to two minutes, and below it the full, scrollable event
//! log.

use std::time::{Duration, Instant};

use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::widgets::Widget;

use super::look::Token;
use super::panel::{self, gap};
use super::theme;
use super::widgets::lifeline::LifelineRow;
use super::{Scene, eventlog, text};
use crate::model::events::Filter;
use crate::model::lifelines::lifeline_cells;

/// The longest time the lifelines cover.
pub const WINDOW: Duration = Duration::from_secs(120);

/// The shortest time the lifelines cover.
pub const MIN_WINDOW: Duration = Duration::from_secs(30);

/// The window grows in steps of this length.
pub const WINDOW_STEP: Duration = Duration::from_secs(15);

/// The time the lifelines cover when the oldest of them is `span` old and
/// they covered `previous` a frame ago. The window holds while the span fits
/// in it and never shrinks. A span that outgrows it opens a window one
/// [`WINDOW_STEP`] longer than the span rounded up to the next step, so the
/// next steps of growth arrive without the lines jumping; the result is at
/// least [`MIN_WINDOW`] and at most [`WINDOW`]. A short history fills the
/// width instead of huddling at the right.
#[must_use]
pub fn window_for(span: Duration, previous: Duration) -> Duration {
    let held = previous.clamp(MIN_WINDOW, WINDOW);
    if span <= held {
        return held;
    }
    let step = WINDOW_STEP.as_secs();
    let secs = span.as_secs() + u64::from(span.subsec_nanos() > 0);
    let grown = Duration::from_secs((secs.div_ceil(step) + 1) * step);
    grown.clamp(held, WINDOW)
}

/// A span as the axis writes it: whole minutes as `2m`, otherwise seconds as
/// `75s`.
fn span_label(span: Duration) -> String {
    let secs = span.as_secs();
    if secs >= 60 && secs.is_multiple_of(60) {
        format!("{}m", secs / 60)
    } else {
        format!("{secs}s")
    }
}

/// A span as the panel title writes it: `2 min` or `75 s`.
fn span_title(span: Duration) -> String {
    let secs = span.as_secs();
    if secs >= 60 && secs.is_multiple_of(60) {
        format!("{} min", secs / 60)
    } else {
        format!("{secs} s")
    }
}

/// The window the lifelines of `nodes` and `caches` are drawn over at `now`.
fn window_of(
    scene: &Scene<'_>,
    nodes: &[super::data::NodeRow<'_>],
    caches: &[smol_str::SmolStr],
    now: Instant,
) -> Duration {
    let lifelines = scene.model.lifelines();
    let oldest = nodes
        .iter()
        .filter_map(|row| lifelines.node(row.member.peer.gossip_addr))
        .chain(caches.iter().filter_map(|cache| lifelines.cache(cache)))
        .filter_map(crate::model::lifelines::Lifeline::first_at)
        .min();
    oldest.map_or(WINDOW, |oldest| {
        scene
            .app
            .lifeline_window(now.saturating_duration_since(oldest))
    })
}

/// The filters in the order the `f` key visits them.
const FILTERS: [Filter; 5] = [
    Filter::All,
    Filter::Membership,
    Filter::Ownership,
    Filter::Traffic,
    Filter::Exporter,
];

/// The time axis under the lifelines of `window`: its length at the left,
/// half of it in the middle (when the window is a whole multiple of ten
/// seconds) and `now` at the right of a rule `width` cells wide. A space
/// separates each label from the rule.
#[must_use]
pub fn axis(width: usize, window: Duration) -> String {
    let mut cells: Vec<char> = vec!['─'; width];
    let mut put = |at: usize, label: &str| {
        for (offset, c) in label.chars().enumerate() {
            if let Some(cell) = cells.get_mut(at + offset) {
                *cell = c;
            }
        }
    };
    put(0, &format!("−{} ", span_label(window)));
    if width >= 16 && window.as_secs().is_multiple_of(10) {
        put(width / 2 - 2, &format!(" −{} ", span_label(window / 2)));
    }
    if width >= 8 {
        put(width - 4, " now");
    }
    cells.into_iter().collect()
}

/// The filter bar of the log title: the active filter in brackets.
fn filter_bar(scene: &Scene<'_>) -> Vec<Span<'static>> {
    let look = scene.look;
    let mut spans = vec![
        gap(1),
        panel::title_span(look, "Events"),
        look.span(" ─ ", Token::Faint),
    ];
    for (index, filter) in FILTERS.into_iter().enumerate() {
        if index > 0 {
            spans.push(gap(1));
        }
        if filter == scene.app.filter {
            spans.push(Span::styled(
                format!("[{}]", filter.label()),
                look.style(Token::Accent).add_modifier(Modifier::BOLD),
            ));
        } else {
            spans.push(look.span(filter.label(), Token::Muted));
        }
    }
    spans.push(look.span(" (f) ", Token::Faint));
    spans
}

/// Draws the Timeline into `area`.
pub fn render(scene: &Scene<'_>, area: Rect, buf: &mut Buffer) {
    let nodes = scene.rows();
    let caches = super::data::distributed_caches(scene.model);
    let wanted = u16::try_from(nodes.len() + caches.len()).unwrap_or(u16::MAX) + 3;
    let height = wanted.clamp(5, (area.height / 2).max(5));
    let parts = Layout::vertical([Constraint::Length(height), Constraint::Min(4)]).split(area);
    lifelines(scene, &nodes, &caches, parts[0], buf);
    log(scene, parts[1], buf);
}

fn lifelines(
    scene: &Scene<'_>,
    nodes: &[super::data::NodeRow<'_>],
    caches: &[smol_str::SmolStr],
    area: Rect,
    buf: &mut Buffer,
) {
    let look = scene.look;
    let window = window_of(scene, nodes, caches, scene.ctx.now);
    let title = vec![
        gap(1),
        panel::title_span(look, "Lifelines"),
        look.span(format!(" · last {}", span_title(window)), Token::Muted),
        gap(1),
    ];
    let block = panel::block_with(look, title, Vec::new(), false);
    let inner = panel::draw(block, area, buf);
    if inner.width < 12 || inner.height < 2 {
        return;
    }
    let label_width = nodes
        .iter()
        .map(|row| row.label().chars().count())
        .chain(caches.iter().map(|c| c.chars().count()))
        .max()
        .unwrap_or(2)
        .clamp(2, 8);
    let track_x = inner.x + 1 + u16::try_from(label_width).unwrap_or(2) + 2;
    let track_width = (inner.x + inner.width).saturating_sub(track_x + 1);
    let width = usize::from(track_width);
    let selected = scene.selected_addr();
    let mut y = inner.y;
    let last = inner.y + inner.height - 1;
    for row in nodes {
        if y >= last {
            break;
        }
        let is_selected = selected == Some(row.member.peer.gossip_addr);
        let label = Span::styled(
            text::pad_right(row.label(), label_width),
            look.node(row.color()).add_modifier(if is_selected {
                Modifier::BOLD | Modifier::UNDERLINED
            } else {
                Modifier::BOLD
            }),
        );
        panel::lines(
            vec![Line::from(vec![
                if is_selected {
                    look.span("▌", Token::Accent)
                } else {
                    gap(1)
                },
                label,
            ])],
            Rect::new(inner.x, y, inner.width, 1),
            buf,
        );
        if let Some(line) = scene.model.lifelines().node(row.member.peer.gossip_addr) {
            let cells = lifeline_cells(line, window, width, scene.ctx.now);
            LifelineRow::new(&cells, row.color(), look)
                .render(Rect::new(track_x, y, track_width, 1), buf);
        }
        y += 1;
    }
    for cache in caches {
        if y >= last {
            break;
        }
        panel::lines(
            vec![Line::from(vec![
                gap(1),
                Span::styled(
                    text::pad_right(cache, label_width),
                    look.style(Token::Accent).add_modifier(Modifier::BOLD),
                ),
            ])],
            Rect::new(inner.x, y, inner.width, 1),
            buf,
        );
        if let Some(line) = scene.model.lifelines().cache(cache) {
            let cells = lifeline_cells(line, window, width, scene.ctx.now);
            LifelineRow::new(&cells, theme::ACCENT, look)
                .render(Rect::new(track_x, y, track_width, 1), buf);
        }
        y += 1;
    }
    panel::lines(
        vec![Line::from(look.span(axis(width, window), Token::Faint))],
        Rect::new(track_x, last, track_width, 1),
        buf,
    );
}

fn log(scene: &Scene<'_>, area: Rect, buf: &mut Buffer) {
    let look = scene.look;
    let filter = scene.app.filter;
    let events: Vec<_> = scene.model.events().newest_first(filter).collect();
    let mut right = Vec::new();
    if scene.app.is_pinned() {
        right.push(Span::styled(
            "‖ pinned",
            look.style(Token::Warn).add_modifier(Modifier::BOLD),
        ));
        right.push(look.span("  G live  ", Token::Muted));
    }
    right.push(look.span(format!("{} events", events.len()), Token::Muted));
    let block = panel::block_with(look, filter_bar(scene), right, true);
    let inner = panel::draw(block, area, buf);
    if events.is_empty() {
        panel::centered(
            vec![Line::from(look.span("no events yet", Token::Faint))],
            inner,
            buf,
        );
        return;
    }
    let offset = scene.app.log_offset(events.len());
    eventlog::render(scene, &events, offset, inner, buf);
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;
    use crate::app::{App, AppConfig};
    use crate::model::Model;
    use crate::model::testkit;
    use crate::ui::panel::row_text;
    use crate::ui::{Ctx, LayoutKind};

    fn draw_with(app: &App, model: &Model, now: Instant, w: u16, h: u16) -> Vec<String> {
        let ctx = Ctx {
            now,
            wall: model.wall().unwrap(),
            elapsed: Duration::ZERO,
        };
        let scene = Scene {
            app,
            model,
            ctx: &ctx,
            look: app.look(),
            kind: LayoutKind::Full,
        };
        let area = Rect::new(0, 0, w, h);
        let mut buf = Buffer::empty(area);
        render(&scene, area, &mut buf);
        (0..h).map(|y| row_text(&buf, y)).collect()
    }

    fn fixture(base: Instant) -> Model {
        testkit::fixture_model(base)
    }

    #[test]
    fn the_axis_labels_both_ends_and_the_middle() {
        let two_min = Duration::from_secs(120);
        let forty = axis(40, two_min);
        assert_eq!(forty.chars().count(), 40);
        assert!(forty.starts_with("−2m ─"), "{forty}");
        assert!(forty.ends_with("─ now"), "{forty}");
        assert_eq!(forty.chars().nth(19), Some('−'));
        assert_eq!(axis(6, two_min), "−2m ──");
        assert_eq!(axis(0, two_min), "");
        assert_eq!(axis(10, two_min).chars().count(), 10);
        assert!(axis(10, two_min).ends_with(" now"));
    }

    #[test]
    fn the_axis_is_labeled_from_its_window() {
        let at = |secs: u64| axis(40, Duration::from_secs(secs));
        assert!(at(30).starts_with("−30s "), "{}", at(30));
        assert!(at(30).contains(" −15s "), "{}", at(30));
        assert!(at(60).starts_with("−1m "), "{}", at(60));
        assert!(at(60).contains(" −30s "), "{}", at(60));
        assert!(at(90).starts_with("−90s "), "{}", at(90));
        assert!(at(90).contains(" −45s "), "{}", at(90));
        assert!(at(90).ends_with(" now"), "{}", at(90));
        // Half of 75 s is not a round number: the middle stays blank.
        let odd = at(75);
        assert!(odd.starts_with("−75s "), "{odd}");
        assert!(odd.ends_with(" now"), "{odd}");
        assert_eq!(odd.matches('−').count(), 1, "{odd}");
    }

    #[test]
    fn the_window_fits_the_history_between_thirty_seconds_and_two_minutes() {
        let secs = Duration::from_secs;
        let first = |span| window_for(span, Duration::ZERO);
        assert_eq!(first(Duration::ZERO), secs(30));
        assert_eq!(first(secs(30)), secs(30));
        assert_eq!(first(Duration::from_millis(30_001)), secs(60));
        assert_eq!(first(secs(31)), secs(60));
        assert_eq!(first(secs(46)), secs(75));
        assert_eq!(first(secs(73)), secs(90));
        assert_eq!(first(secs(75)), secs(90));
        assert_eq!(first(secs(76)), secs(105));
        assert_eq!(first(secs(105)), secs(120));
        assert_eq!(first(secs(119)), secs(120));
        assert_eq!(first(secs(900)), secs(120));
    }

    #[test]
    fn the_window_holds_while_the_history_fits_and_never_shrinks() {
        let secs = Duration::from_secs;
        assert_eq!(window_for(secs(78), secs(90)), secs(90));
        assert_eq!(window_for(secs(90), secs(90)), secs(90));
        assert_eq!(window_for(secs(91), secs(90)), secs(120));
        assert_eq!(window_for(secs(10), secs(90)), secs(90));
        assert_eq!(window_for(secs(10), secs(120)), secs(120));
        assert_eq!(window_for(secs(500), secs(120)), secs(120));
    }

    #[test]
    fn spans_are_written_in_minutes_when_whole_and_in_seconds_otherwise() {
        let secs = Duration::from_secs;
        assert_eq!(span_label(secs(120)), "2m");
        assert_eq!(span_label(secs(75)), "75s");
        assert_eq!(span_label(secs(60)), "1m");
        assert_eq!(span_title(secs(120)), "2 min");
        assert_eq!(span_title(secs(75)), "75 s");
        assert_eq!(span_title(secs(60)), "1 min");
    }

    /// The column of the first lifeline glyph in the first node row.
    fn first_glyph_column(row: &str) -> usize {
        let track_start = row.chars().position(|c| c == ' ').unwrap_or(0);
        row.chars()
            .enumerate()
            .skip(track_start + 4)
            .find(|(_, c)| matches!(c, '━' | '▲' | '↻' | '┄'))
            .map_or(usize::MAX, |(column, _)| column)
    }

    #[test]
    fn a_short_history_opens_a_window_with_room_to_grow() {
        let base = Instant::now();
        let model = fixture(base);
        let app = App::new(AppConfig::default());
        // The oldest lifeline is 75 s old: the window opens at 90 s, so the
        // first node's line starts a sixth of the way along the track.
        let rows = draw_with(&app, &model, base + Duration::from_secs(75), 140, 36);
        assert!(rows[0].contains("Lifelines · last 90 s"), "{}", rows[0]);
        let first = rows
            .iter()
            .find(|row| row.contains("n1"))
            .expect("the first node has a row");
        let label_end = first.find("n1").unwrap() + 2;
        let column = first_glyph_column(first);
        assert!(
            column > label_end + 5,
            "the line starts at column {column}, label ends at {label_end}: {first}"
        );
        let axis_row = rows
            .iter()
            .find(|row| row.contains("−90s"))
            .expect("the axis names the window");
        assert!(axis_row.contains(" now"), "{axis_row}");
    }

    #[test]
    fn the_window_holds_across_frames_until_the_view_is_left() {
        let base = Instant::now();
        let model = fixture(base);
        let mut app = App::new(AppConfig::default());
        let title = |app: &App, secs: u64| {
            draw_with(app, &model, base + Duration::from_secs(secs), 140, 36)[0].clone()
        };
        // Frames on both sides of the 75 s step all show the 90 s window.
        assert!(title(&app, 73).contains("last 90 s"));
        assert!(title(&app, 78).contains("last 90 s"));
        assert!(title(&app, 79).contains("last 90 s"));
        assert!(title(&app, 20).contains("last 90 s"), "never shrinks");
        // Leaving the view lets the window start again.
        app.apply_director(crate::app::UiCommand::Tab(crate::ui::View::Caches), &model);
        assert!(title(&app, 20).contains("last 30 s"));
    }

    #[test]
    fn lifelines_show_a_row_per_node_and_one_per_distributed_cache() {
        let base = Instant::now();
        let model = fixture(base);
        let app = App::new(AppConfig::default());
        let rows = draw_with(&app, &model, base + Duration::from_secs(20), 140, 36);
        let text = rows.join("\n");
        assert!(
            rows[0].starts_with("╭ Lifelines · last 30 s"),
            "{}",
            rows[0]
        );
        for label in ["n1", "n2", "n6", "n7", "n8"] {
            assert!(
                text.contains(&format!(" {label} ")) || text.contains(&format!("▌{label}")),
                "{label}"
            );
        }
        assert!(text.contains(" it "), "{text}");
        assert!(text.contains("−30s"), "{text}");
        assert!(text.contains("now"), "{text}");
    }

    #[test]
    fn the_lifelines_draw_runs_marks_and_the_view_and_settled_marks() {
        let base = Instant::now();
        let model = fixture(base);
        let app = App::new(AppConfig::default());
        let rows = draw_with(&app, &model, base + Duration::from_secs(20), 140, 36);
        let text = rows.join("\n");
        assert!(text.contains('━'), "live runs: {text}");
        assert!(text.contains('▲'), "join marks: {text}");
        assert!(text.contains('✖'), "the crash: {text}");
        assert!(text.contains('◐'), "the departure: {text}");
        assert!(text.contains('⇄'), "the view change: {text}");
        assert!(text.contains('✔'), "the settle: {text}");
    }

    #[test]
    fn the_log_title_marks_the_active_filter() {
        let base = Instant::now();
        let model = fixture(base);
        let mut app = App::new(AppConfig::default());
        let rows = draw_with(&app, &model, base, 140, 36).join("\n");
        assert!(
            rows.contains("Events ─ [all] membership ownership traffic exporter (f)"),
            "{rows}"
        );
        assert!(
            rows.contains(&format!("{} events", model.events().len())),
            "{rows}"
        );
        app.filter = Filter::Ownership;
        let filtered = draw_with(&app, &model, base, 140, 36).join("\n");
        assert!(
            filtered.contains("all membership [ownership] traffic exporter (f)"),
            "{filtered}"
        );
        assert!(
            filtered.contains("VIEW") && !filtered.contains("JOIN  "),
            "{filtered}"
        );
    }

    #[test]
    fn scrolling_pins_the_log_and_says_so() {
        let base = Instant::now();
        let model = fixture(base);
        let mut app = App::new(AppConfig::default());
        app.apply_director(
            crate::app::UiCommand::Tab(crate::ui::View::Timeline),
            &model,
        );
        let key = crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::Up,
            crossterm::event::KeyModifiers::NONE,
        );
        app.handle_key(key, &model);
        let rows = draw_with(&app, &model, base, 140, 36).join("\n");
        assert!(rows.contains("‖ pinned  G live"), "{rows}");
    }

    #[test]
    fn a_short_screen_gives_the_log_at_least_four_rows() {
        let base = Instant::now();
        let model = fixture(base);
        let app = App::new(AppConfig::default());
        let rows = draw_with(&app, &model, base, 100, 21);
        assert!(rows.iter().any(|r| r.contains("Events")), "{rows:?}");
        assert!(rows.last().is_some_and(|r| r.contains('╯')));
    }
}

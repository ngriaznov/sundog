//! The Overview: members, throughput, ownership, events and caches.

use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::widgets::Widget;

use super::data::{self, CacheRow};
use super::look::Token;
use super::panel::{self, gap};
use super::widgets::blocks::BlockArea;
use super::widgets::braille::BrailleArea;
use super::widgets::spark;
use super::{LayoutKind, Scene, allocate, eventlog, members, ownership, text};

/// The height the Ownership panel needs at the screen class `kind`.
fn ownership_height(scene: &Scene<'_>) -> u16 {
    let nodes = scene
        .app
        .ownership_cache(scene.model)
        .and_then(|cache| scene.model.ownership(&cache))
        .map_or(1, |digest| digest.eligible.len());
    ownership::wanted_height(scene.kind, nodes)
}

/// The most rows the bottom row (Events and Caches) takes at the full layout.
/// Every row beyond it goes to the top row, so the Throughput chart has room.
const BOTTOM_ROWS: u16 = 12;

/// The most rows the top row (Members and Throughput) takes at the full
/// layout before the bottom row takes the rest.
const TOP_ROWS: u16 = 16;

/// The heights of the Overview's three stacked rows in `total` rows. At the
/// full layout the bottom row stops at [`BOTTOM_ROWS`] and the spare rows go
/// to the top row up to [`TOP_ROWS`]; what is left beyond that returns to the
/// bottom row. The other layouts give every spare row to Events.
fn row_heights(scene: &Scene<'_>, total: u16) -> Vec<u16> {
    if scene.kind != LayoutKind::Full {
        return allocate(
            total,
            &[
                (members::wanted_height(scene, 7, 14), 5),
                (ownership_height(scene), 5),
                (u16::MAX, 4),
            ],
        );
    }
    let mut heights = allocate(
        total,
        &[
            (members::wanted_height(scene, 9, 14), 5),
            (ownership_height(scene), 5),
            (BOTTOM_ROWS, 4),
        ],
    );
    let spare = total - heights.iter().sum::<u16>();
    let to_top = spare.min(TOP_ROWS.saturating_sub(heights[0]));
    heights[0] += to_top;
    heights[2] += spare - to_top;
    heights
}

/// Draws the Overview into `area`.
pub fn render(scene: &Scene<'_>, area: Rect, buf: &mut Buffer) {
    let heights = row_heights(scene, area.height);
    let rows = Layout::vertical(heights.iter().map(|&h| Constraint::Length(h))).split(area);
    if scene.kind == LayoutKind::Full {
        let top = Layout::horizontal([Constraint::Percentage(60), Constraint::Percentage(40)])
            .split(rows[0]);
        members::render(scene, top[0], buf, true);
        throughput(scene, top[1], buf);
        ownership::render(scene, rows[1], buf, false);
        let bottom = Layout::horizontal([Constraint::Percentage(60), Constraint::Percentage(40)])
            .split(rows[2]);
        events(scene, bottom[0], buf);
        caches(scene, bottom[1], buf);
    } else {
        members::render(scene, rows[0], buf, true);
        ownership::render(scene, rows[1], buf, false);
        events(scene, rows[2], buf);
    }
}

/// The Events panel: the newest events that pass the filter.
pub fn events(scene: &Scene<'_>, area: Rect, buf: &mut Buffer) {
    let look = scene.look;
    let filter = scene.app.filter;
    let log = scene.model.events();
    let shown: Vec<_> = log.newest_first(filter).collect();
    let block = panel::block(
        look,
        "Events",
        "",
        vec![look.span(
            format!("{} · {}", filter.label(), shown.len()),
            Token::Muted,
        )],
        false,
    );
    let inner = panel::draw(block, area, buf);
    if shown.is_empty() {
        panel::centered(
            vec![Line::from(look.span("no events yet", Token::Faint))],
            inner,
            buf,
        );
        return;
    }
    eventlog::render(scene, &shown, 0, inner, buf);
}

/// The sum line under the throughput chart: reads, fetches, forwards and
/// repairs per second. A rate that no node reports is left out, and so are
/// the last segments that do not fit in `width`.
fn rates_line(scene: &Scene<'_>, total: &data::Throughput, width: usize) -> Line<'static> {
    let look = scene.look;
    let mut spans: Vec<Span<'static>> = Vec::new();
    let mut used = 0;
    for (label, value) in [
        ("reads", total.reads),
        ("fetch", total.fetch),
        ("fwd", total.forwarded),
        ("repairs", total.repairs),
    ] {
        let Some(value) = value else { continue };
        let number = text::count(value);
        let sep = if spans.is_empty() { 0 } else { 3 };
        let needed = sep + label.len() + 1 + number.chars().count();
        // Leave room for the closing `  /s`.
        if used + needed + 4 > width {
            break;
        }
        if sep > 0 {
            spans.push(look.span(" · ", Token::Faint));
        }
        spans.push(look.span(format!("{label} "), Token::Muted));
        spans.push(look.span(number, Token::Text));
        used += needed;
    }
    spans.push(look.span("  /s", Token::Muted));
    Line::from(spans)
}

/// The headline above the throughput chart.
fn throughput_headline(scene: &Scene<'_>, total: &data::Throughput) -> Line<'static> {
    let look = scene.look;
    let now = total.ops.last().copied().unwrap_or(0.0);
    let mut spans = vec![
        Span::styled(
            format!("{} ops/s", text::count(now)),
            look.style(Token::Accent).add_modifier(Modifier::BOLD),
        ),
        gap(1),
    ];
    match data::trend(&total.ops) {
        Some(change) if change >= 0.005 => {
            spans.push(look.span(format!("▲{}", text::percent(change, 0)), Token::Ok));
        }
        Some(change) if change <= -0.005 => {
            spans.push(look.span(format!("▼{}", text::percent(-change, 0)), Token::Warn));
        }
        _ => {}
    }
    if let Some(ratio) = total.hit_ratio {
        spans.push(look.span("   hit ", Token::Muted));
        spans.push(look.span(text::percent(ratio, 1), Token::Text));
    }
    if let Some(tx) = total.tx_bytes {
        spans.push(look.span("   tx ", Token::Muted));
        spans.push(look.span(text::byte_rate(tx), Token::Text));
    }
    Line::from(spans)
}

/// The no-metrics notice.
pub fn no_exporter(scene: &Scene<'_>, area: Rect, buf: &mut Buffer) {
    let look = scene.look;
    panel::centered(
        vec![
            Line::from(look.span("no exporter mapped", Token::Muted)),
            Line::from(look.span("pass --metrics 'http://{ip}:9090/metrics'", Token::Faint)),
        ],
        area,
        buf,
    );
}

/// The Throughput panel: an area chart of operations per second, and the
/// rates beneath it.
pub fn throughput(scene: &Scene<'_>, area: Rect, buf: &mut Buffer) {
    let look = scene.look;
    let block = panel::block(look, "Throughput", "metrics", Vec::new(), false);
    let inner = panel::draw(block, area, buf);
    if inner.width == 0 || inner.height < 4 {
        return;
    }
    let total = data::throughput(scene.model);
    if total.nodes == 0 {
        no_exporter(scene, inner, buf);
        return;
    }
    panel::lines(
        vec![throughput_headline(scene, &total)],
        Rect::new(inner.x + 1, inner.y, inner.width.saturating_sub(2), 1),
        buf,
    );
    let chart = Rect::new(
        inner.x + 1,
        inner.y + 1,
        inner.width.saturating_sub(2),
        inner.height - 3,
    );
    let max = total.ops.iter().copied().fold(0.0, f64::max).max(1.0) * 1.1;
    if look.braille {
        BrailleArea::new(&total.ops, max, look.mode).render(chart, buf);
    } else {
        BlockArea::new(&total.ops, max, look.mode).render(chart, buf);
    }
    let seconds = data::window_span(usize::from(chart.width), scene.model.scrape_interval());
    let axis_label = format!("−{seconds}s ");
    let fill = usize::from(chart.width).saturating_sub(axis_label.chars().count() + 4);
    panel::lines(
        vec![Line::from(vec![
            look.span(axis_label, Token::Muted),
            look.span("─".repeat(fill), Token::Faint),
            look.span(" now", Token::Muted),
        ])],
        Rect::new(chart.x, chart.y + chart.height, chart.width, 1),
        buf,
    );
    panel::lines(
        vec![rates_line(scene, &total, usize::from(chart.width))],
        Rect::new(chart.x, chart.y + chart.height + 1, chart.width, 1),
        buf,
    );
}

/// The status text of a cache row: settled or settling for a `Distributed`
/// cache, the divergence for a `Replicated` one.
fn cache_status(scene: &Scene<'_>, row: &CacheRow) -> Vec<Span<'static>> {
    let look = scene.look;
    if row.is_conflicted() {
        return vec![look.span("⚠ modes disagree", Token::Bad)];
    }
    if row.is_distributed() {
        return match scene.model.settle(&row.name) {
            Some(verdict) if verdict.settled => vec![look.span("✔ settled", Token::Ok)],
            Some(_) => vec![look.span("↻ settling", Token::Move)],
            None => vec![look.span("computing", Token::Muted)],
        };
    }
    match scene.model.divergence(&row.name) {
        Some(spread) if spread > 0.0 => vec![
            look.span(format!("±{} ", text::whole(spread)), Token::Warn),
            look.span("⚠", Token::Warn),
        ],
        Some(_) => vec![look.span("±0", Token::Muted)],
        None => Vec::new(),
    }
}

/// The Caches panel of the Overview: one line per cache, the fetch mix of the
/// ownership cache and its rebalance rates.
pub fn caches(scene: &Scene<'_>, area: Rect, buf: &mut Buffer) {
    let look = scene.look;
    let block = panel::block(look, "Caches", "gossip+metrics", Vec::new(), false);
    let inner = panel::padded(panel::draw(block, area, buf));
    if inner.width == 0 || inner.height == 0 {
        return;
    }
    let rows = data::cache_rows(scene.model);
    let focus = scene.app.ownership_cache(scene.model);
    let live = data::live_count(scene.model);
    let width = rows
        .iter()
        .map(|r| r.name.chars().count())
        .max()
        .unwrap_or(4)
        .clamp(4, 12);
    let mut lines = Vec::new();
    for row in &rows {
        let marked = focus.as_deref() == Some(row.name.as_str());
        let mode = row.mode.map_or_else(|| "?".to_owned(), data::mode_dotted);
        let keys = data::key_estimate(scene.model, row).map_or_else(
            || "—".to_owned(),
            |keys| format!("{} keys", text::count(keys)),
        );
        let mut spans = vec![
            if marked {
                look.span("▸ ", Token::Accent)
            } else {
                gap(2)
            },
            Span::styled(
                text::pad_right(&row.name, width),
                look.style(Token::Text).add_modifier(if marked {
                    Modifier::BOLD
                } else {
                    Modifier::empty()
                }),
            ),
            gap(1),
            look.span(text::pad_right(&mode, 4), mode_token(row)),
            gap(1),
            look.span(
                text::pad_right(&format!("{}/{live}", row.advertisers.len()), 6),
                Token::Muted,
            ),
            look.span(text::pad_left(&keys, 12), Token::Text),
            gap(2),
        ];
        spans.extend(cache_status(scene, row));
        lines.push(Line::from(spans));
    }
    if let Some(cache) = focus.as_deref() {
        lines.push(Line::default());
        lines.push(fetch_line(scene, cache));
        lines.push(rebalance_line(scene, cache, usize::from(inner.width)));
    }
    panel::lines(lines, inner, buf);
}

fn mode_token(row: &CacheRow) -> Token {
    match row.mode {
        Some(sundog::store::Mode::Distributed { .. }) => Token::Accent,
        Some(sundog::store::Mode::Replicated) => Token::Info,
        Some(sundog::store::Mode::Invalidation) => Token::Move,
        None => Token::Bad,
        _ => Token::Muted,
    }
}

/// `fetch mix  local 61% · remote 30% · miss 9% · err 0%`.
fn fetch_line(scene: &Scene<'_>, cache: &str) -> Line<'static> {
    let look = scene.look;
    let Some(mix) = data::cluster_fetch_mix(scene.model, cache) else {
        return Line::from(vec![
            look.span("fetch mix  ", Token::Muted),
            look.span("no traffic", Token::Faint),
        ]);
    };
    let mut spans = vec![look.span("fetch mix  ", Token::Muted)];
    for (index, (label, value, token)) in [
        ("local", mix.local, Token::Ok),
        ("remote", mix.remote, Token::Info),
        ("miss", mix.miss, Token::Warn),
        ("err", mix.error, Token::Bad),
    ]
    .into_iter()
    .enumerate()
    {
        if index > 0 {
            spans.push(look.span(" · ", Token::Faint));
        }
        spans.push(look.span(format!("{label} "), Token::Muted));
        spans.push(look.span(text::percent(value, 0), token));
    }
    Line::from(spans)
}

/// `⇄ rebal in ⣀⣠⣴  out ⣀⣀⣀`.
fn rebalance_line(scene: &Scene<'_>, cache: &str, width: usize) -> Line<'static> {
    let look = scene.look;
    let (into, out) = data::rebalance_series(scene.model, cache);
    let cells = width.saturating_sub(20) / 2;
    let cells = cells.clamp(4, 14);
    Line::from(vec![
        look.span("⇄ ", Token::Move),
        look.span("rebal in ", Token::Muted),
        look.span(spark(&into, cells, look.braille), Token::Move),
        look.span("  out ", Token::Muted),
        look.span(spark(&out, cells, look.braille), Token::Move),
    ])
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;
    use crate::app::{App, AppConfig};
    use crate::model::Model;
    use crate::model::testkit;
    use crate::ui::Ctx;
    use crate::ui::panel::row_text;

    fn draw(model: &Model, kind: LayoutKind, w: u16, h: u16) -> Vec<String> {
        let mut app = App::new(AppConfig::default());
        app.observe(model, Instant::now());
        app.snap();
        let ctx = Ctx {
            now: model.now().unwrap(),
            wall: model.wall().unwrap(),
            elapsed: Duration::ZERO,
        };
        let scene = Scene {
            app: &app,
            model,
            ctx: &ctx,
            look: app.look(),
            kind,
        };
        let area = Rect::new(0, 0, w, h);
        let mut buf = Buffer::empty(area);
        render(&scene, area, &mut buf);
        (0..h).map(|y| row_text(&buf, y)).collect()
    }

    fn fixture() -> Model {
        testkit::fixture_model(Instant::now())
    }

    #[test]
    fn the_full_overview_holds_every_panel() {
        let rows = draw(&fixture(), LayoutKind::Full, 140, 37).join("\n");
        for title in [
            "Members · gossip",
            "Throughput · metrics",
            "Ownership · it",
            "Events",
            "Caches · gossip+metrics",
        ] {
            assert!(rows.contains(title), "{title} in\n{rows}");
        }
    }

    #[test]
    fn without_metrics_the_throughput_panel_says_how_to_get_them() {
        let rows = draw(&fixture(), LayoutKind::Full, 140, 37).join("\n");
        assert!(rows.contains("no exporter mapped"), "{rows}");
        assert!(
            rows.contains("pass --metrics 'http://{ip}:9090/metrics'"),
            "{rows}"
        );
    }

    #[test]
    fn the_events_panel_lists_newest_first_with_the_filter_and_count() {
        let model = fixture();
        let rows = draw(&model, LayoutKind::Full, 140, 37);
        let count = model.events().len();
        let title = rows.iter().find(|r| r.contains("Events")).unwrap();
        assert!(title.contains(&format!("all · {count}")), "{title}");
        let first = rows
            .iter()
            .position(|r| r.contains("Events"))
            .map(|i| &rows[i + 1])
            .unwrap();
        assert!(
            first.contains("SETTLED") || first.contains("VIEW"),
            "{first}"
        );
    }

    #[test]
    fn the_caches_panel_lists_each_cache_with_its_mode_and_status() {
        let rows = draw(&fixture(), LayoutKind::Full, 140, 37).join("\n");
        assert!(rows.contains("▸ it"), "{rows}");
        assert!(rows.contains("D·2"), "{rows}");
        assert!(rows.contains("6/6"), "{rows}");
        assert!(rows.contains("✔ settled"), "{rows}");
        assert!(rows.contains("churn"), "{rows}");
        assert!(rows.contains("fetch mix  no traffic"), "{rows}");
        assert!(rows.contains("rebal in"), "{rows}");
    }

    #[test]
    fn the_caches_and_events_panels_keep_a_cell_of_padding_inside_their_borders() {
        let rows = draw(&fixture(), LayoutKind::Full, 140, 40);
        let marked = rows.iter().find(|r| r.contains("▸ it")).unwrap();
        assert!(marked.contains("│ ▸ it "), "{marked}");
        let event = rows.iter().find(|r| r.contains("SETTLED")).unwrap();
        assert!(event.starts_with("│ 00:00:"), "{event}");
        let other = rows.iter().find(|r| r.contains("churn")).unwrap();
        assert!(other.contains("│   churn"), "{other}");
    }

    #[test]
    fn compact_and_narrow_overviews_leave_out_throughput_and_caches() {
        for (kind, w, h) in [(LayoutKind::Compact, 120, 33), (LayoutKind::Narrow, 80, 21)] {
            let rows = draw(&fixture(), kind, w, h).join("\n");
            assert!(rows.contains("Members"), "{kind:?}");
            assert!(rows.contains("Ownership"), "{kind:?}");
            assert!(rows.contains("Events"), "{kind:?}");
            assert!(!rows.contains("Throughput"), "{kind:?}");
            assert!(!rows.contains("Caches · gossip"), "{kind:?}");
        }
    }

    #[test]
    fn with_metrics_the_throughput_panel_charts_and_summarizes_the_cluster() {
        let model = testkit::fixture_model_with_metrics(Instant::now());
        let rows = draw(&model, LayoutKind::Full, 140, 37);
        let text = rows.join("\n");
        assert!(text.contains(" ops/s"), "{text}");
        assert!(text.contains("hit "), "{text}");
        assert!(text.contains("tx "), "{text}");
        assert!(text.contains("−104s"), "{text}");
        assert!(text.contains("reads "), "{text}");
        assert!(text.contains("fetch "), "{text}");
        assert!(text.contains("fetch mix  local "), "{text}");
        assert!(
            text.contains('⣿') || text.contains('⣤'),
            "the area chart: {text}"
        );
    }

    /// The axis label under the throughput chart of `model` at 140x37, where
    /// the chart is 52 columns wide.
    fn axis_label(model: &Model) -> String {
        let rows = draw(model, LayoutKind::Full, 140, 37);
        let row = rows.iter().find(|r| r.contains(" now")).expect("an axis");
        let start = row.find('−').expect("a minus sign");
        row[start..].split(' ').next().unwrap().to_owned()
    }

    #[test]
    fn the_axis_label_names_the_span_of_the_chart_not_the_length_of_the_history() {
        // Ten samples fill only the right edge of a 52-column chart, which
        // spans 104 samples.
        let mut model = testkit::fixture_model_with_scrapes(Instant::now(), 10);
        assert!(model.cluster_ops().len() <= 10);
        assert_eq!(axis_label(&model), "−104s");
        // One sample per two seconds doubles the span.
        model.set_scrape_interval(Duration::from_secs(2));
        assert_eq!(axis_label(&model), "−208s");
    }

    #[test]
    fn at_140x40_the_bottom_row_stops_at_twelve_rows_and_the_top_row_takes_the_rest() {
        // The body of a 140x40 screen is 37 rows, 36 with a caption.
        for model in [
            fixture(),
            testkit::fixture_model_with_metrics(Instant::now()),
        ] {
            for height in [36, 37] {
                let rows = draw(&model, LayoutKind::Full, 140, height);
                let row_of = |title: &str| {
                    rows.iter()
                        .position(|r| r.starts_with(title))
                        .unwrap_or_else(|| panic!("{title} in\n{}", rows.join("\n")))
                };
                let events = row_of("╭ Events");
                let ownership = row_of("╭ Ownership");
                assert!(
                    rows.len() - events <= 12,
                    "bottom row {} of {height}",
                    rows.len() - events
                );
                assert!(
                    ownership >= 9,
                    "the top row has 9 rows or more: {ownership}"
                );
            }
        }
    }

    #[test]
    fn the_rates_line_drops_what_the_panel_cannot_hold() {
        let model = testkit::fixture_model_with_metrics(Instant::now());
        let app = App::new(AppConfig::default());
        let ctx = Ctx {
            now: model.now().unwrap(),
            wall: model.wall().unwrap(),
            elapsed: Duration::ZERO,
        };
        let scene = Scene {
            app: &app,
            model: &model,
            ctx: &ctx,
            look: app.look(),
            kind: LayoutKind::Full,
        };
        let total = data::throughput(&model);
        let line = |width| -> String {
            rates_line(&scene, &total, width)
                .spans
                .iter()
                .map(|s| s.content.as_ref())
                .collect()
        };
        assert!(
            line(80).starts_with("reads ") && line(80).contains("fwd "),
            "{}",
            line(80)
        );
        let short = line(26);
        assert!(
            short.starts_with("reads ") && !short.contains("fetch"),
            "{short}"
        );
        assert_eq!(line(4), "  /s");
        let quiet = data::Throughput::default();
        let spans = rates_line(&scene, &quiet, 80).spans;
        assert_eq!(spans.len(), 1, "no rate to show");
    }

    #[test]
    fn the_no_exporter_notice_names_the_flag_to_pass() {
        let model = fixture();
        let app = App::new(AppConfig::default());
        let ctx = Ctx {
            now: model.now().unwrap(),
            wall: model.wall().unwrap(),
            elapsed: Duration::ZERO,
        };
        let scene = Scene {
            app: &app,
            model: &model,
            ctx: &ctx,
            look: app.look(),
            kind: LayoutKind::Full,
        };
        let area = Rect::new(0, 0, 60, 5);
        let mut buf = Buffer::empty(area);
        no_exporter(&scene, area, &mut buf);
        let shown: Vec<String> = (0..5).map(|y| row_text(&buf, y)).collect();
        assert!(
            shown.iter().any(|r| r.trim() == "no exporter mapped"),
            "{shown:?}"
        );
        assert!(
            shown
                .iter()
                .any(|r| r.trim() == "pass --metrics 'http://{ip}:9090/metrics'"),
            "{shown:?}"
        );
    }
}

use std::sync::atomic::Ordering;

use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Cell, Gauge, List, ListItem, Paragraph, Row, Table};

use crate::app::{self, App};
use crate::convergence::Convergence;

const COLUMN_WIDTHS: [Constraint; 8] = [
    Constraint::Length(3),
    Constraint::Length(18),
    Constraint::Length(7),
    Constraint::Length(6),
    Constraint::Length(9),
    Constraint::Length(8),
    Constraint::Length(6),
    Constraint::Length(9),
];

pub(crate) fn draw(frame: &mut Frame, app: &App) {
    let area = frame.area();

    if let Some((done, total)) = app.demo.preload_progress() {
        draw_preload(frame, area, done, total);
        return;
    }

    let table_height = u16::try_from(app.demo.nodes.len())
        .unwrap_or(u16::MAX)
        .saturating_add(4)
        .min(area.height.saturating_sub(9))
        .max(5);

    let chunks = Layout::vertical([
        Constraint::Length(table_height),
        Constraint::Min(3),
        Constraint::Length(6),
    ])
    .split(area);

    draw_table(frame, chunks[0], app);
    draw_feed(frame, chunks[1], app);
    draw_status(frame, chunks[2], app);
}

#[allow(clippy::cast_precision_loss)]
fn draw_preload(frame: &mut Frame, area: Rect, done: u64, total: usize) {
    let chunks = Layout::vertical([Constraint::Length(3), Constraint::Min(1)]).split(area);
    let ratio = if total == 0 {
        0.0
    } else {
        (done as f64 / total as f64).clamp(0.0, 1.0)
    };
    let gauge = Gauge::default()
        .block(Block::bordered().title("Preloading"))
        .gauge_style(Style::new().fg(Color::Cyan))
        .ratio(ratio)
        .label(format!("{done}/{total} keys"));
    frame.render_widget(gauge, chunks[0]);
    let note = Paragraph::new("filling the key space before the load starts…")
        .block(Block::bordered().title("Status"));
    frame.render_widget(note, chunks[1]);
}

fn draw_table(frame: &mut Frame, area: Rect, app: &App) {
    let header = Row::new([
        "#", "Node", "Status", "Peers", "Entries", "Buckets", "Warm", "Restarts",
    ])
    .style(Style::new().add_modifier(Modifier::BOLD));

    let rows: Vec<Row> = app
        .demo
        .nodes
        .iter()
        .enumerate()
        .map(|(i, node)| row_for(i, node, app))
        .collect();

    let title = format!("Nodes — {}s elapsed", app.started.elapsed().as_secs());
    let table = Table::new(rows, COLUMN_WIDTHS)
        .header(header)
        .block(Block::bordered().title(title));
    frame.render_widget(table, area);
}

fn row_for(i: usize, node: &crate::node::NodeSlot, app: &App) -> Row<'static> {
    let alive = node.is_alive();
    let node_id = node.status.node_id.load(Ordering::Relaxed);
    let node_label = if node_id == 0 {
        "—".to_owned()
    } else {
        format!("{node_id:016x}")
    };
    let peers = node
        .peer_count()
        .map_or_else(|| "-".to_owned(), |p| p.to_string());
    let entries = node.status.entry_count.load(Ordering::Relaxed);
    let owned_buckets = node.status.owned_buckets.load(Ordering::Relaxed);
    let warm = node.status.warm.load(Ordering::Relaxed);
    let restarts = node.status.restarts.load(Ordering::Relaxed);

    let cells = vec![
        Cell::new((i + 1).to_string()),
        Cell::new(node_label),
        Cell::new(if alive { "alive" } else { "killed" }.to_owned()),
        Cell::new(peers),
        Cell::new(entries.to_string()),
        Cell::new(owned_buckets.to_string()),
        Cell::new(if warm { "yes" } else { "no" }.to_owned()),
        Cell::new(restarts.to_string()),
    ];

    Row::new(cells).style(row_style(i, alive, app))
}

fn row_style(i: usize, alive: bool, app: &App) -> Style {
    let base = if alive {
        Style::new()
    } else {
        Style::new().fg(Color::DarkGray)
    };
    let base = if i == app.cursor && i != app.selected {
        base.add_modifier(Modifier::UNDERLINED)
    } else {
        base
    };
    if i == app.selected {
        base.add_modifier(Modifier::BOLD | Modifier::REVERSED)
    } else {
        base
    }
}

fn draw_feed(frame: &mut Frame, area: Rect, app: &App) {
    let visible = usize::from(area.height.saturating_sub(2)).max(1);
    let start = app.feed.len().saturating_sub(visible);
    let items: Vec<ListItem> = app
        .feed
        .iter()
        .skip(start)
        .map(|line| ListItem::new(line.as_str()))
        .collect();
    let list = List::new(items).block(Block::bordered().title("Event Feed"));
    frame.render_widget(list, area);
}

fn draw_status(frame: &mut Frame, area: Rect, app: &App) {
    let convergence = app.convergence();
    let (conv_text, conv_color) = match convergence {
        Convergence::NoLiveNodes => ("no live nodes".to_owned(), Color::Red),
        Convergence::Converged { total, live } => (
            format!("CONVERGED ({live} nodes, {total} entries)"),
            Color::Green,
        ),
        Convergence::Diverged {
            total,
            expected,
            live,
        } => (
            format!("settling ({live} nodes, {total}/{expected} entries)"),
            Color::Yellow,
        ),
    };
    let paused = app.demo.paused.load(Ordering::Relaxed);

    let hits = app.demo.state.fetch_hits.load(Ordering::Relaxed);
    let misses = app.demo.state.fetch_misses.load(Ordering::Relaxed);
    let errors = app.demo.state.fetch_errors.load(Ordering::Relaxed);
    let (p50_us, p99_us) = app.demo.state.latency_percentiles();

    let lines = vec![
        Line::from(vec![
            Span::raw("convergence: "),
            Span::styled(
                conv_text,
                Style::new().fg(conv_color).add_modifier(Modifier::BOLD),
            ),
            Span::raw(format!(
                "   load: {}",
                if paused { "PAUSED" } else { "running" }
            )),
        ]),
        Line::from(format!(
            "fetch: {hits} hits / {misses} misses / {errors} errors — p50 {p50_us}us, p99 {p99_us}us"
        )),
        Line::from(format!(
            "convergence poll bound: {}s of anti-entropy settling once paused",
            app::convergence_deadline_secs()
        )),
        Line::from(
            "↑/↓ or j/k move · enter/1-9 select · K kill · R restart · P pause/resume load · q quit",
        ),
    ];
    let paragraph = Paragraph::new(lines).block(Block::bordered().title("Status"));
    frame.render_widget(paragraph, area);
}

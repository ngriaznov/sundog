use std::sync::atomic::Ordering;

use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Cell, List, ListItem, Paragraph, Row, Table};

use crate::app::App;
use crate::convergence::Convergence;

const COLUMN_WIDTHS: [Constraint; 7] = [
    Constraint::Length(3),
    Constraint::Length(18),
    Constraint::Length(7),
    Constraint::Length(6),
    Constraint::Length(8),
    Constraint::Length(8),
    Constraint::Length(9),
];

pub(crate) fn draw(frame: &mut Frame, app: &App) {
    let area = frame.area();
    let table_height = u16::try_from(app.demo.nodes.len())
        .unwrap_or(u16::MAX)
        .saturating_add(4)
        .min(area.height.saturating_sub(6))
        .max(5);

    let chunks = Layout::vertical([
        Constraint::Length(table_height),
        Constraint::Min(3),
        Constraint::Length(4),
    ])
    .split(area);

    draw_table(frame, chunks[0], app);
    draw_feed(frame, chunks[1], app);
    draw_status(frame, chunks[2], app);
}

fn draw_table(frame: &mut Frame, area: Rect, app: &App) {
    let convergence = app.convergence();
    let majority = match convergence {
        Convergence::Converged { entries, .. } => Some(entries),
        Convergence::Diverged { .. } | Convergence::NoLiveNodes => None,
    };

    let header = Row::new([
        "#", "Node", "Status", "Peers", "Entries", "Writes", "Restarts",
    ])
    .style(Style::new().add_modifier(Modifier::BOLD));

    let rows: Vec<Row> = app
        .demo
        .nodes
        .iter()
        .enumerate()
        .map(|(i, node)| row_for(i, node, majority, app))
        .collect();

    let title = format!("Nodes — {}s elapsed", app.started.elapsed().as_secs());
    let table = Table::new(rows, COLUMN_WIDTHS)
        .header(header)
        .block(Block::bordered().title(title));
    frame.render_widget(table, area);
}

fn row_for(
    i: usize,
    node: &crate::node::NodeSlot,
    majority: Option<i64>,
    app: &App,
) -> Row<'static> {
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
    let writes = node.status.writes_applied.load(Ordering::Relaxed);
    let restarts = node.status.restarts.load(Ordering::Relaxed);

    let entries_style = if alive && majority.is_some_and(|m| m != entries) {
        Style::new().fg(Color::Red)
    } else {
        Style::new()
    };

    let cells = vec![
        Cell::new((i + 1).to_string()),
        Cell::new(node_label),
        Cell::new(if alive { "alive" } else { "killed" }.to_owned()),
        Cell::new(peers),
        Cell::new(entries.to_string()).style(entries_style),
        Cell::new(writes.to_string()),
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
        Convergence::Converged { entries, live } => (
            format!("CONVERGED ({live} nodes @ {entries} entries)"),
            Color::Green,
        ),
        Convergence::Diverged { min, max, live } => (
            format!("DIVERGED ({live} nodes, {min}..={max} entries)"),
            Color::Yellow,
        ),
    };
    let paused = app.demo.paused.load(Ordering::Relaxed);

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
        Line::from(
            "↑/↓ or j/k move · enter/1-9 select · K kill · R restart · P pause/resume load · q quit",
        ),
    ];
    let paragraph = Paragraph::new(lines).block(Block::bordered().title("Status"));
    frame.render_widget(paragraph, area);
}

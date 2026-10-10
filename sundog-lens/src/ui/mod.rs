//! The interface: views, layout classes, the theme and the widgets.
//!
//! [`render`] draws one frame into a buffer from the interface state, the
//! model and an injected clock, so a test renders the same frame every time.
//! Each view reads a [`Scene`] and draws into its area; none changes state.

use std::time::{Duration, Instant, SystemTime};

use ratatui::Frame;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::widgets::{Block, Widget};

use crate::app::App;
use crate::model::Model;
use look::Look;

pub mod anim;
pub mod caches;
pub mod caption;
pub mod data;
pub mod eventlog;
pub mod explain;
pub mod footer;
pub mod header;
pub mod help;
pub mod look;
pub mod members;
pub mod node;
pub mod overview;
pub mod ownership;
pub mod panel;
pub mod splash;
pub mod table;
pub mod text;
pub mod theme;
pub mod timeline;
pub mod too_small;
pub mod widgets;

/// The four top-level views, in tab order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum View {
    /// Members, throughput, ownership, events and caches.
    #[default]
    Overview,
    /// Per-cache detail.
    Caches,
    /// One node in detail.
    Node,
    /// Lifelines and the full event log.
    Timeline,
}

impl View {
    /// Every view in tab order.
    pub const ALL: [Self; 4] = [Self::Overview, Self::Caches, Self::Node, Self::Timeline];

    /// The view a scenario `tab` step names: `overview`, `caches`, `node` or
    /// `timeline`.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "overview" => Some(Self::Overview),
            "caches" => Some(Self::Caches),
            "node" => Some(Self::Node),
            "timeline" => Some(Self::Timeline),
            _ => None,
        }
    }

    /// The word a scenario `tab` step writes for the view.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Overview => "overview",
            Self::Caches => "caches",
            Self::Node => "node",
            Self::Timeline => "timeline",
        }
    }

    /// The tab title.
    #[must_use]
    pub const fn title(self) -> &'static str {
        match self {
            Self::Overview => "Overview",
            Self::Caches => "Caches",
            Self::Node => "Node",
            Self::Timeline => "Timeline",
        }
    }

    /// The next view in tab order, wrapping.
    #[must_use]
    pub fn next(self) -> Self {
        Self::ALL[(self.position() + 1) % Self::ALL.len()]
    }

    /// The previous view in tab order, wrapping.
    #[must_use]
    pub fn prev(self) -> Self {
        Self::ALL[(self.position() + Self::ALL.len() - 1) % Self::ALL.len()]
    }

    fn position(self) -> usize {
        Self::ALL.iter().position(|&v| v == self).unwrap_or(0)
    }
}

/// How much of the interface a terminal size holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LayoutKind {
    /// 140 columns by 38 rows or more: every panel.
    Full,
    /// 100 by 30 or more: members, a half-width mosaic and events.
    Compact,
    /// 80 by 24 or more: members, share bars and events.
    Narrow,
    /// Below 80 by 24: a notice only.
    TooSmall,
}

/// Minimum columns and rows of each layout.
const FULL: (u16, u16) = (140, 38);
const COMPACT: (u16, u16) = (100, 30);
const NARROW: (u16, u16) = (80, 24);

/// The layout class for `area`. A terminal at least 80 columns by 24 rows
/// never falls below Narrow, so a wide but short terminal gets
/// Narrow rather than a notice.
#[must_use]
pub fn layout_kind(area: Rect) -> LayoutKind {
    let (w, h) = (area.width, area.height);
    if w < NARROW.0 || h < NARROW.1 {
        LayoutKind::TooSmall
    } else if w >= FULL.0 && h >= FULL.1 {
        LayoutKind::Full
    } else if w >= COMPACT.0 && h >= COMPACT.1 {
        LayoutKind::Compact
    } else {
        LayoutKind::Narrow
    }
}

/// The clock a frame is drawn at. Injected, so a test draws a fixed frame.
#[derive(Debug, Clone, Copy)]
pub struct Ctx {
    /// The monotonic time of the frame.
    pub now: Instant,
    /// The wall-clock time of the frame, shown in the header.
    pub wall: SystemTime,
    /// The time since the interface started, for the spinner and the splash.
    pub elapsed: Duration,
}

/// Where each part of the screen goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Layout {
    /// How much of the interface the screen holds.
    pub kind: LayoutKind,
    /// The header row.
    pub header: Rect,
    /// The tab row.
    pub tabs: Rect,
    /// The view's area.
    pub body: Rect,
    /// The caption row, in demo mode.
    pub caption: Option<Rect>,
    /// The footer row.
    pub footer: Rect,
}

/// The layout of `area`: a header row, a tab row, the body, an optional
/// caption row and the footer row. A screen below the minimum is one notice.
#[must_use]
pub fn layout_for(area: Rect, caption: bool) -> Layout {
    let kind = layout_kind(area);
    if kind == LayoutKind::TooSmall {
        return Layout {
            kind,
            header: area,
            tabs: Rect::default(),
            body: area,
            caption: None,
            footer: Rect::default(),
        };
    }
    let row = |y: u16| Rect::new(area.x, area.y + y, area.width, 1);
    let caption_rows = u16::from(caption);
    let footer_y = area.height - 1;
    let body_height = area.height - 3 - caption_rows;
    Layout {
        kind,
        header: row(0),
        tabs: row(1),
        body: Rect::new(area.x, area.y + 2, area.width, body_height),
        caption: caption.then(|| row(footer_y - 1)),
        footer: row(footer_y),
    }
}

/// Splits `total` rows among stacked panels, each given as `(want, min)`.
/// Every panel gets its minimum first (in order, while rows last), then the
/// panels in order grow toward what they want with the rows that remain.
#[must_use]
pub fn allocate(total: u16, wants: &[(u16, u16)]) -> Vec<u16> {
    let mut heights = Vec::with_capacity(wants.len());
    let mut left = total;
    for &(_, min) in wants {
        let given = min.min(left);
        heights.push(given);
        left -= given;
    }
    for (height, &(want, _)) in heights.iter_mut().zip(wants) {
        let grow = want.saturating_sub(*height).min(left);
        *height += grow;
        left -= grow;
    }
    heights
}

/// Everything a view reads to draw: the interface state, the model on
/// screen, the clock and the layout class.
#[derive(Debug, Clone, Copy)]
pub struct Scene<'a> {
    /// The interface state.
    pub app: &'a App,
    /// The model on screen: the frozen copy while frozen.
    pub model: &'a Model,
    /// The clock of the frame.
    pub ctx: &'a Ctx,
    /// How text is styled.
    pub look: Look,
    /// The layout class.
    pub kind: LayoutKind,
}

impl Scene<'_> {
    /// The node rows to draw.
    #[must_use]
    pub fn rows(&self) -> Vec<data::NodeRow<'_>> {
        self.app.rows(self.model, self.ctx.wall)
    }

    /// The gossip address of the selected node.
    #[must_use]
    pub fn selected_addr(&self) -> Option<std::net::SocketAddr> {
        self.app.selected_addr(self.model, self.ctx.wall)
    }
}

/// Draws one frame of `live` (or the frozen copy) into `frame`.
pub fn draw(frame: &mut Frame, app: &App, live: &Model, ctx: &Ctx) {
    let area = frame.area();
    render(frame.buffer_mut(), area, app, live, ctx);
}

/// Draws one frame of `live` (or the frozen copy) into `buf` over `area`.
pub fn render(buf: &mut Buffer, area: Rect, app: &App, live: &Model, ctx: &Ctx) {
    let look = app.look();
    Block::new().style(look.base()).render(area, buf);
    let layout = layout_for(area, app.config().demo && app.config().captions);
    app.note_too_small(layout.kind == LayoutKind::TooSmall);
    if layout.kind == LayoutKind::TooSmall {
        too_small::render(look, area, buf);
        return;
    }
    let model = app.shown(live);
    let scene = Scene {
        app,
        model,
        ctx,
        look,
        kind: layout.kind,
    };
    header::render(&scene, layout.header, buf);
    header::render_tabs(&scene, layout.tabs, buf);
    if scene.rows().is_empty() {
        splash::render(&scene, layout.body, buf);
    } else {
        match app.view {
            View::Overview => overview::render(&scene, layout.body, buf),
            View::Caches => caches::render(&scene, layout.body, buf),
            View::Node => node::render(&scene, layout.body, buf),
            View::Timeline => timeline::render(&scene, layout.body, buf),
        }
    }
    if let Some(row) = layout.caption {
        caption::render(&scene, row, buf);
    }
    footer::render(&scene, layout.footer, buf);
    if let Some(state) = &app.explain {
        explain::render(&scene, state, layout.body, buf);
    }
    if app.help {
        help::render(&scene, area, buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_go_to_minimums_first_then_to_wants_in_order() {
        assert_eq!(allocate(37, &[(9, 5), (10, 4), (u16::MAX, 4)]), [9, 10, 18]);
        assert_eq!(allocate(21, &[(11, 5), (7, 4), (u16::MAX, 4)]), [11, 6, 4]);
        assert_eq!(allocate(10, &[(11, 5), (7, 4), (u16::MAX, 4)]), [5, 4, 1]);
        assert_eq!(allocate(3, &[(11, 5), (7, 4)]), [3, 0]);
        assert_eq!(allocate(0, &[(1, 1)]), [0]);
        let slots = allocate(9, &[]);
        assert!(slots.is_empty(), "{slots:?}");
        let all = allocate(30, &[(8, 2), (8, 2), (u16::MAX, 2)]);
        assert_eq!(all.iter().sum::<u16>(), 30);
    }

    #[test]
    fn the_layout_stacks_header_tabs_body_and_footer() {
        let layout = layout_for(Rect::new(0, 0, 140, 40), false);
        assert_eq!(layout.kind, LayoutKind::Full);
        assert_eq!(layout.header, Rect::new(0, 0, 140, 1));
        assert_eq!(layout.tabs, Rect::new(0, 1, 140, 1));
        assert_eq!(layout.body, Rect::new(0, 2, 140, 37));
        assert_eq!(layout.caption, None);
        assert_eq!(layout.footer, Rect::new(0, 39, 140, 1));
    }

    #[test]
    fn a_caption_takes_the_row_above_the_footer() {
        let layout = layout_for(Rect::new(0, 0, 140, 40), true);
        assert_eq!(layout.body, Rect::new(0, 2, 140, 36));
        assert_eq!(layout.caption, Some(Rect::new(0, 38, 140, 1)));
        assert_eq!(layout.footer, Rect::new(0, 39, 140, 1));
    }

    #[test]
    fn every_class_has_a_body_and_a_too_small_screen_has_only_a_notice() {
        for (w, h, kind) in [
            (120, 36, LayoutKind::Compact),
            (100, 30, LayoutKind::Compact),
            (80, 24, LayoutKind::Narrow),
        ] {
            let layout = layout_for(Rect::new(0, 0, w, h), false);
            assert_eq!(layout.kind, kind);
            assert_eq!(layout.body.height, h - 3);
            assert_eq!(layout.body.width, w);
        }
        let tiny = layout_for(Rect::new(0, 0, 72, 20), true);
        assert_eq!(tiny.kind, LayoutKind::TooSmall);
        assert_eq!(tiny.body, Rect::new(0, 0, 72, 20));
        assert_eq!(tiny.caption, None);
    }

    #[test]
    fn the_layout_respects_an_offset_origin() {
        let layout = layout_for(Rect::new(5, 3, 100, 30), true);
        assert_eq!(layout.header, Rect::new(5, 3, 100, 1));
        assert_eq!(layout.footer, Rect::new(5, 32, 100, 1));
        assert_eq!(layout.caption, Some(Rect::new(5, 31, 100, 1)));
        assert_eq!(layout.body, Rect::new(5, 5, 100, 26));
    }

    fn kind(w: u16, h: u16) -> LayoutKind {
        layout_kind(Rect::new(0, 0, w, h))
    }

    #[test]
    fn layout_breakpoints() {
        assert_eq!(kind(140, 40), LayoutKind::Full);
        assert_eq!(kind(140, 38), LayoutKind::Full);
        assert_eq!(kind(200, 60), LayoutKind::Full);
        assert_eq!(kind(139, 40), LayoutKind::Compact);
        assert_eq!(kind(140, 37), LayoutKind::Compact);
        assert_eq!(kind(100, 30), LayoutKind::Compact);
        assert_eq!(kind(120, 36), LayoutKind::Compact);
        assert_eq!(kind(99, 40), LayoutKind::Narrow);
        assert_eq!(kind(100, 29), LayoutKind::Narrow);
        assert_eq!(kind(80, 24), LayoutKind::Narrow);
        assert_eq!(kind(79, 24), LayoutKind::TooSmall);
        assert_eq!(kind(80, 23), LayoutKind::TooSmall);
        assert_eq!(kind(0, 0), LayoutKind::TooSmall);
        assert_eq!(kind(72, 20), LayoutKind::TooSmall);
    }

    #[test]
    fn a_wide_but_short_terminal_is_narrow_not_too_small() {
        assert_eq!(kind(300, 24), LayoutKind::Narrow);
        assert_eq!(kind(300, 29), LayoutKind::Narrow);
    }

    #[test]
    fn views_cycle_in_tab_order() {
        assert_eq!(View::Overview.next(), View::Caches);
        assert_eq!(View::Timeline.next(), View::Overview);
        assert_eq!(View::Overview.prev(), View::Timeline);
        assert_eq!(View::Node.prev(), View::Caches);
        for view in View::ALL {
            assert_eq!(view.next().prev(), view);
        }
    }

    #[test]
    fn views_parse_by_name_and_have_titles() {
        for view in View::ALL {
            assert_eq!(View::from_name(&view.title().to_lowercase()), Some(view));
            assert_eq!(View::from_name(view.name()), Some(view));
            assert_eq!(view.name(), view.title().to_lowercase());
        }
        assert_eq!(View::from_name("Overview"), None);
        assert_eq!(View::from_name("nope"), None);
        assert_eq!(View::default(), View::Overview);
    }
}

//! The interface: views, layout classes, the theme and the widgets.

use ratatui::layout::Rect;

pub mod anim;
pub mod caches;
pub mod caption;
pub mod footer;
pub mod header;
pub mod help;
pub mod node;
pub mod overview;
pub mod splash;
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

#[cfg(test)]
mod tests {
    use super::*;

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
        }
        assert_eq!(View::from_name("Overview"), None);
        assert_eq!(View::from_name("nope"), None);
        assert_eq!(View::default(), View::Overview);
    }
}

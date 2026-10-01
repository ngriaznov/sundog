//! How the views style text: the palette tokens in the chosen color mode,
//! with bold, dim and reverse standing in for color in mono.

use ratatui::style::{Modifier, Style};
use ratatui::text::Span;

use super::theme::{self, ColorMode, Rgb};
use crate::cli::DisplayArgs;

/// A palette token a view asks for by role, not by color.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Token {
    /// Body text.
    Text,
    /// Secondary text.
    Muted,
    /// Ghost text and empty bars.
    Faint,
    /// The logo, the active tab and the cluster name.
    Accent,
    /// Healthy.
    Ok,
    /// Attention.
    Warn,
    /// Failure.
    Bad,
    /// Information.
    Info,
    /// Rebalance and view changes.
    Move,
}

impl Token {
    /// The palette color of the token.
    #[must_use]
    pub const fn rgb(self) -> Rgb {
        match self {
            Self::Text => theme::TEXT,
            Self::Muted => theme::MUTED,
            Self::Faint => theme::FAINT,
            Self::Accent => theme::ACCENT,
            Self::Ok => theme::OK,
            Self::Warn => theme::WARN,
            Self::Bad => theme::BAD,
            Self::Info => theme::INFO,
            Self::Move => theme::MOVE,
        }
    }
}

/// The display settings every view draws with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Look {
    /// How colors reach the terminal.
    pub mode: ColorMode,
    /// Whether to paint the background under every cell.
    pub paint_bg: bool,
    /// Whether sparklines are braille rather than blocks.
    pub braille: bool,
}

impl Default for Look {
    fn default() -> Self {
        Self {
            mode: ColorMode::Truecolor,
            paint_bg: true,
            braille: true,
        }
    }
}

impl Look {
    /// The look the display flags ask for, with `auto` resolved against the
    /// environment.
    #[must_use]
    pub fn from_args(display: &DisplayArgs) -> Self {
        Self::with_mode(display, ColorMode::from_env(display.color))
    }

    /// The look the display flags ask for in a given color mode.
    #[must_use]
    pub const fn with_mode(display: &DisplayArgs, mode: ColorMode) -> Self {
        Self {
            mode,
            paint_bg: !display.no_bg,
            braille: !display.no_braille,
        }
    }

    /// Whether the terminal shows no color.
    #[must_use]
    pub fn is_mono(self) -> bool {
        self.mode == ColorMode::Mono
    }

    /// The style of `token`: its color, or in mono a modifier that keeps the
    /// role readable.
    #[must_use]
    pub fn style(self, token: Token) -> Style {
        if self.is_mono() {
            return match token {
                Token::Muted | Token::Faint => Style::new().add_modifier(Modifier::DIM),
                Token::Accent | Token::Warn | Token::Bad => {
                    Style::new().add_modifier(Modifier::BOLD)
                }
                Token::Text | Token::Ok | Token::Info | Token::Move => Style::new(),
            };
        }
        Style::new().fg(token.rgb().color(self.mode))
    }

    /// A foreground color: `rgb` in color modes, the plain default in mono.
    #[must_use]
    pub fn fg(self, rgb: Rgb) -> Style {
        Style::new().fg(rgb.color(self.mode))
    }

    /// A node's color as a style. In mono every node reads alike; its label
    /// tells it apart.
    #[must_use]
    pub fn node(self, rgb: Rgb) -> Style {
        self.fg(rgb)
    }

    /// The style that paints the background under every cell, or none.
    #[must_use]
    pub fn base(self) -> Style {
        if self.paint_bg && !self.is_mono() {
            Style::new()
                .bg(theme::BG.color(self.mode))
                .fg(theme::TEXT.color(self.mode))
        } else {
            Style::new()
        }
    }

    /// The style of the header, footer, caption and popups.
    #[must_use]
    pub fn surface(self) -> Style {
        if self.is_mono() {
            Style::new()
        } else {
            Style::new()
                .bg(theme::SURFACE.color(self.mode))
                .fg(theme::TEXT.color(self.mode))
        }
    }

    /// The style of the selected row.
    #[must_use]
    pub fn selected(self) -> Style {
        if self.is_mono() {
            Style::new().add_modifier(Modifier::REVERSED)
        } else {
            Style::new().bg(theme::SURFACE_HI.color(self.mode))
        }
    }

    /// A background of `rgb`, or none in mono.
    #[must_use]
    pub fn bg(self, rgb: Rgb) -> Style {
        if self.is_mono() {
            Style::new()
        } else {
            Style::new().bg(rgb.color(self.mode))
        }
    }

    /// The style of a panel border; the focused panel's is amber.
    #[must_use]
    pub fn border(self, focused: bool) -> Style {
        if focused {
            if self.is_mono() {
                Style::new().add_modifier(Modifier::BOLD)
            } else {
                Style::new().fg(theme::BORDER_FOCUS.color(self.mode))
            }
        } else if self.is_mono() {
            Style::new().add_modifier(Modifier::DIM)
        } else {
            Style::new().fg(theme::BORDER.color(self.mode))
        }
    }

    /// The style of the active tab pill: the background color on amber.
    #[must_use]
    pub fn pill(self) -> Style {
        if self.is_mono() {
            Style::new().add_modifier(Modifier::REVERSED | Modifier::BOLD)
        } else {
            Style::new()
                .fg(theme::BG.color(self.mode))
                .bg(theme::ACCENT.color(self.mode))
                .add_modifier(Modifier::BOLD)
        }
    }

    /// `text` in `token`'s style.
    #[must_use]
    pub fn span(self, text: impl Into<String>, token: Token) -> Span<'static> {
        Span::styled(text.into(), self.style(token))
    }

    /// `text` in a node's color.
    #[must_use]
    pub fn node_span(self, text: impl Into<String>, rgb: Rgb) -> Span<'static> {
        Span::styled(text.into(), self.node(rgb))
    }
}

#[cfg(test)]
mod tests {
    use ratatui::style::Color;

    use super::*;

    fn look(mode: ColorMode) -> Look {
        Look {
            mode,
            ..Look::default()
        }
    }

    #[test]
    fn tokens_take_their_palette_colors() {
        let look = look(ColorMode::Truecolor);
        assert_eq!(
            look.style(Token::Accent).fg,
            Some(Color::Rgb(0xF2, 0xB5, 0x44))
        );
        assert_eq!(
            look.style(Token::Bad).fg,
            Some(Color::Rgb(0xFF, 0x6B, 0x6B))
        );
        assert_eq!(
            look.style(Token::Muted).fg,
            Some(Color::Rgb(0x7D, 0x77, 0x6B))
        );
        assert_eq!(Token::Move.rgb(), theme::MOVE);
        assert_eq!(Token::Info.rgb(), theme::INFO);
        assert_eq!(Token::Ok.rgb(), theme::OK);
        assert_eq!(Token::Warn.rgb(), theme::WARN);
        assert_eq!(Token::Faint.rgb(), theme::FAINT);
        assert_eq!(Token::Text.rgb(), theme::TEXT);
    }

    #[test]
    fn the_256_mode_picks_indexed_colors() {
        let look = look(ColorMode::Ansi256);
        assert_eq!(
            look.style(Token::Accent).fg,
            Some(Color::Indexed(theme::to_256(theme::ACCENT)))
        );
    }

    #[test]
    fn mono_uses_modifiers_and_no_color() {
        let look = look(ColorMode::Mono);
        for token in [
            Token::Text,
            Token::Muted,
            Token::Faint,
            Token::Accent,
            Token::Ok,
            Token::Warn,
            Token::Bad,
            Token::Info,
            Token::Move,
        ] {
            assert_eq!(look.style(token).fg, None, "{token:?}");
        }
        assert!(
            look.style(Token::Muted)
                .add_modifier
                .contains(Modifier::DIM)
        );
        assert!(
            look.style(Token::Accent)
                .add_modifier
                .contains(Modifier::BOLD)
        );
        assert!(look.selected().add_modifier.contains(Modifier::REVERSED));
        assert!(look.pill().add_modifier.contains(Modifier::REVERSED));
        assert_eq!(look.base(), Style::new());
        assert_eq!(look.surface(), Style::new());
        assert_eq!(look.bg(theme::BAD), Style::new());
        assert!(look.border(false).add_modifier.contains(Modifier::DIM));
        assert!(look.border(true).add_modifier.contains(Modifier::BOLD));
    }

    #[test]
    fn the_background_paints_unless_turned_off() {
        let on = look(ColorMode::Truecolor);
        assert_eq!(on.base().bg, Some(Color::Rgb(0x12, 0x11, 0x0F)));
        let off = Look {
            paint_bg: false,
            ..on
        };
        assert_eq!(off.base(), Style::new());
        assert_eq!(on.surface().bg, Some(Color::Rgb(0x1B, 0x1A, 0x17)));
        assert_eq!(on.selected().bg, Some(Color::Rgb(0x26, 0x24, 0x1F)));
    }

    #[test]
    fn borders_are_amber_only_when_focused() {
        let look = look(ColorMode::Truecolor);
        assert_eq!(look.border(true).fg, Some(Color::Rgb(0xF2, 0xB5, 0x44)));
        assert_eq!(look.border(false).fg, Some(Color::Rgb(0x3A, 0x36, 0x2E)));
        let pill = look.pill();
        assert_eq!(pill.bg, Some(Color::Rgb(0xF2, 0xB5, 0x44)));
        assert_eq!(pill.fg, Some(Color::Rgb(0x12, 0x11, 0x0F)));
    }

    #[test]
    fn the_look_follows_the_display_flags() {
        let display = DisplayArgs {
            no_bg: true,
            no_braille: true,
            ..DisplayArgs::default()
        };
        let look = Look::with_mode(&display, ColorMode::Ansi256);
        assert_eq!(look.mode, ColorMode::Ansi256);
        assert!(!look.paint_bg && !look.braille);
        let mono = Look::from_args(&DisplayArgs {
            color: crate::ui::theme::ColorChoice::Mono,
            ..DisplayArgs::default()
        });
        assert!(mono.is_mono());
        assert!(Look::default().paint_bg && Look::default().braille);
    }

    #[test]
    fn spans_carry_the_style() {
        let look = look(ColorMode::Truecolor);
        assert_eq!(look.span("x", Token::Ok).style, look.style(Token::Ok));
        assert_eq!(
            look.node_span("y", theme::NODE_COLORS[1]).style.fg,
            Some(Color::Rgb(0xFF, 0x7E, 0xB6))
        );
        assert_eq!(look.fg(theme::BAD).fg, look.style(Token::Bad).fg);
    }
}

//! The palette, the color modes and the glyph allowlist.
//!
//! The palette is the brand card: amber on near-black. A node keeps one of
//! eight colors everywhere it appears. [`ColorMode`] degrades every color to
//! the 256-color cube or to bold, dim and reverse only.

use ratatui::style::Color;

/// A 24-bit color.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rgb(pub u8, pub u8, pub u8);

impl Rgb {
    /// This color as a terminal color under `mode`: exact in truecolor, the
    /// nearest xterm-256 index in 256-color mode, and the terminal default in
    /// mono (where only modifiers distinguish cells).
    #[must_use]
    pub fn color(self, mode: ColorMode) -> Color {
        match mode {
            ColorMode::Truecolor => Color::Rgb(self.0, self.1, self.2),
            ColorMode::Ansi256 => Color::Indexed(to_256(self)),
            ColorMode::Mono => Color::Reset,
        }
    }
}

/// Painted under every cell unless `--no-bg` is given.
pub const BG: Rgb = Rgb(0x12, 0x11, 0x0F);
/// Header, footer, caption and popups.
pub const SURFACE: Rgb = Rgb(0x1B, 0x1A, 0x17);
/// The selected row.
pub const SURFACE_HI: Rgb = Rgb(0x26, 0x24, 0x1F);
/// Rounded borders.
pub const BORDER: Rgb = Rgb(0x3A, 0x36, 0x2E);
/// The focused panel and the selected-row bar.
pub const BORDER_FOCUS: Rgb = Rgb(0xF2, 0xB5, 0x44);
/// Body text.
pub const TEXT: Rgb = Rgb(0xEC, 0xE6, 0xD9);
/// Secondary text.
pub const MUTED: Rgb = Rgb(0x7D, 0x77, 0x6B);
/// Ghost rows and empty bars.
pub const FAINT: Rgb = Rgb(0x4A, 0x46, 0x3E);
/// The logo, the active tab pill and the cluster name.
pub const ACCENT: Rgb = Rgb(0xF2, 0xB5, 0x44);
/// Healthy.
pub const OK: Rgb = Rgb(0x8F, 0xD1, 0x6A);
/// Attention.
pub const WARN: Rgb = Rgb(0xF0, 0xA0, 0x4B);
/// Failure.
pub const BAD: Rgb = Rgb(0xFF, 0x6B, 0x6B);
/// Information.
pub const INFO: Rgb = Rgb(0x6C, 0xC4, 0xE8);
/// Rebalance and view changes.
pub const MOVE: Rgb = Rgb(0xB7, 0x9C, 0xFF);
/// The color a flash blends toward.
pub const WHITE: Rgb = Rgb(0xFF, 0xFF, 0xFF);

/// The eight node colors. A node takes the one at its slot index, modulo
/// eight, and keeps it in every panel.
pub const NODE_COLORS: [Rgb; 8] = [
    Rgb(0x6C, 0xB6, 0xFF),
    Rgb(0xFF, 0x7E, 0xB6),
    Rgb(0x4F, 0xD1, 0xC5),
    Rgb(0xC7, 0x92, 0xEA),
    Rgb(0xC3, 0xE8, 0x8D),
    Rgb(0xE0, 0xC9, 0xA6),
    Rgb(0x89, 0xDD, 0xFF),
    Rgb(0xA0, 0xA8, 0xFF),
];

/// The area-chart gradient from the bottom of a chart to its top.
pub const GRADIENT: [Rgb; 3] = [
    Rgb(0x5A, 0x4A, 0x1F),
    Rgb(0xF2, 0xB5, 0x44),
    Rgb(0xFF, 0xE2, 0xA0),
];

/// The node color for slot `index`.
#[must_use]
pub fn node_color(index: usize) -> Rgb {
    NODE_COLORS[index % NODE_COLORS.len()]
}

/// The gradient color at height `t`, where 0 is the bottom stop and 1 the top
/// stop. `t` outside 0 to 1 clamps.
#[must_use]
pub fn gradient_at(t: f64) -> Rgb {
    let t = if t.is_nan() { 0.0 } else { t.clamp(0.0, 1.0) };
    if t < 0.5 {
        crate::ui::anim::blend(GRADIENT[0], GRADIENT[1], t * 2.0)
    } else {
        crate::ui::anim::blend(GRADIENT[1], GRADIENT[2], (t - 0.5) * 2.0)
    }
}

/// How colors reach the terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ColorMode {
    /// 24-bit color.
    Truecolor,
    /// The xterm 256-color palette.
    Ansi256,
    /// No color: bold, dim and reverse only.
    Mono,
}

/// The `--color` choice before the environment resolves `auto`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ColorChoice {
    /// Truecolor when `COLORTERM` says so, 256 colors otherwise.
    #[default]
    Auto,
    /// Always truecolor.
    Truecolor,
    /// Always 256 colors.
    Ansi256,
    /// No color.
    Mono,
}

impl ColorChoice {
    /// The choice a `--color` value names: `auto`, `truecolor`, `256` or
    /// `mono`.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "auto" => Some(Self::Auto),
            "truecolor" => Some(Self::Truecolor),
            "256" => Some(Self::Ansi256),
            "mono" => Some(Self::Mono),
            _ => None,
        }
    }
}

impl ColorMode {
    /// Resolves `choice` against the environment: `colorterm` is the value of
    /// `COLORTERM` and `no_color` whether `NO_COLOR` is set and non-empty. A
    /// set `NO_COLOR` forces mono whatever the choice.
    #[must_use]
    pub fn resolve(choice: ColorChoice, colorterm: Option<&str>, no_color: bool) -> Self {
        if no_color {
            return Self::Mono;
        }
        match choice {
            ColorChoice::Truecolor => Self::Truecolor,
            ColorChoice::Ansi256 => Self::Ansi256,
            ColorChoice::Mono => Self::Mono,
            ColorChoice::Auto => match colorterm {
                Some("truecolor" | "24bit") => Self::Truecolor,
                _ => Self::Ansi256,
            },
        }
    }

    /// Reads `COLORTERM` and `NO_COLOR` from the process environment and
    /// resolves `choice` against them.
    #[must_use]
    pub fn from_env(choice: ColorChoice) -> Self {
        let colorterm = std::env::var("COLORTERM").ok();
        let no_color = std::env::var_os("NO_COLOR").is_some_and(|value| !value.is_empty());
        Self::resolve(choice, colorterm.as_deref(), no_color)
    }
}

/// The six levels of each channel in the xterm 6x6x6 color cube.
const CUBE_LEVELS: [u8; 6] = [0, 95, 135, 175, 215, 255];

/// The xterm-256 index nearest to `rgb`, among the color cube (16 to 231) and
/// the grayscale ramp (232 to 255). The 16 system colors are skipped: a
/// terminal theme redefines them.
#[must_use]
pub fn to_256(rgb: Rgb) -> u8 {
    let Rgb(r, g, b) = rgb;
    let cube = [r, g, b].map(nearest_cube_level);
    let cube_rgb = cube.map(|level| CUBE_LEVELS[level]);
    let cube_index = 16 + 36 * cube[0] + 6 * cube[1] + cube[2];

    let mean = (u32::from(r) + u32::from(g) + u32::from(b)) / 3;
    // Ramp step i is gray 8 + 10 i, for i in 0..24.
    let step = ((mean.saturating_sub(8) + 5) / 10).min(23);
    let gray = u8::try_from(8 + 10 * step).unwrap_or(238);

    let cube_distance = distance([r, g, b], cube_rgb);
    let gray_distance = distance([r, g, b], [gray; 3]);
    let index = if gray_distance < cube_distance {
        232 + step
    } else {
        u32::try_from(cube_index).unwrap_or(16)
    };
    u8::try_from(index).unwrap_or(231)
}

/// The cube level (0 to 5) whose channel value is nearest to `value`.
fn nearest_cube_level(value: u8) -> usize {
    (0..CUBE_LEVELS.len())
        .min_by_key(|&level| value.abs_diff(CUBE_LEVELS[level]))
        .unwrap_or(0)
}

/// Squared distance between two colors.
fn distance(a: [u8; 3], b: [u8; 3]) -> u32 {
    a.iter()
        .zip(b)
        .map(|(&x, y)| u32::from(x.abs_diff(y)).pow(2))
        .sum()
}

/// Every non-ASCII glyph the interface draws, besides the braille block
/// (see [`is_allowed`]). Each is present in `DejaVu Sans Mono`.
pub const GLYPHS: &str = "● • · ◐ ◒ ○ ✖ ✚ ↻ ⇄ ⇣ ▸ ▲ ▼ ▽ ✓ ✔ ⚠ ‖ ◆ ━ ╸ ─ ┊ ┄ │ ╭ ╮ ╰ ╯ ├ ┤ ▌ ▀ ▄ █ \
▁▂▃▄▅▆▇ ▏▎▍▋▊▉ ▓ ▒ ░ → ← ↑ ↓ ⇧ ⏎ ▶ Σ × ± − … ≈ —";

/// Glyphs the interface never draws: absent from `DejaVu Sans Mono`.
pub const BANNED: &str = "⟳⏸❶⏱⬤☼";

/// Whether `c` may appear in a rendered buffer: ASCII, the braille block
/// (U+2800 to U+28FF, drawn through the `FreeMono` fallback), or a glyph in
/// [`GLYPHS`].
#[must_use]
pub fn is_allowed(c: char) -> bool {
    c.is_ascii() || ('\u{2800}'..='\u{28FF}').contains(&c) || GLYPHS.contains(c)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_256_maps_known_colors() {
        assert_eq!(to_256(Rgb(0, 0, 0)), 16);
        assert_eq!(to_256(Rgb(255, 255, 255)), 231);
        assert_eq!(to_256(Rgb(255, 0, 0)), 196);
        assert_eq!(to_256(Rgb(0, 255, 0)), 46);
        assert_eq!(to_256(Rgb(0, 0, 255)), 21);
        // A mid gray sits on the ramp, not the cube.
        assert_eq!(to_256(Rgb(128, 128, 128)), 244);
        // The near-black background lands on the second ramp step.
        assert_eq!(to_256(BG), 233);
        // The accent amber (242, 181, 68) is nearest the cube entry (255, 175, 95).
        assert_eq!(to_256(ACCENT), 16 + 36 * 5 + 6 * 3 + 1);
    }

    #[test]
    fn to_256_stays_out_of_the_system_colors() {
        for r in (0..=255u8).step_by(15) {
            for g in (0..=255u8).step_by(15) {
                for b in (0..=255u8).step_by(15) {
                    assert!(to_256(Rgb(r, g, b)) >= 16);
                }
            }
        }
    }

    #[test]
    fn color_maps_each_mode() {
        let c = Rgb(0x12, 0x34, 0x56);
        assert_eq!(c.color(ColorMode::Truecolor), Color::Rgb(0x12, 0x34, 0x56));
        assert_eq!(c.color(ColorMode::Ansi256), Color::Indexed(to_256(c)));
        assert_eq!(c.color(ColorMode::Mono), Color::Reset);
    }

    #[test]
    fn auto_picks_truecolor_only_when_colorterm_says_so() {
        let auto = ColorChoice::Auto;
        assert_eq!(
            ColorMode::resolve(auto, Some("truecolor"), false),
            ColorMode::Truecolor
        );
        assert_eq!(
            ColorMode::resolve(auto, Some("24bit"), false),
            ColorMode::Truecolor
        );
        assert_eq!(
            ColorMode::resolve(auto, Some("yes"), false),
            ColorMode::Ansi256
        );
        assert_eq!(ColorMode::resolve(auto, None, false), ColorMode::Ansi256);
    }

    #[test]
    fn explicit_choices_ignore_colorterm_and_no_color_forces_mono() {
        assert_eq!(
            ColorMode::resolve(ColorChoice::Truecolor, None, false),
            ColorMode::Truecolor
        );
        assert_eq!(
            ColorMode::resolve(ColorChoice::Ansi256, Some("truecolor"), false),
            ColorMode::Ansi256
        );
        assert_eq!(
            ColorMode::resolve(ColorChoice::Mono, Some("truecolor"), false),
            ColorMode::Mono
        );
        for choice in [
            ColorChoice::Auto,
            ColorChoice::Truecolor,
            ColorChoice::Ansi256,
            ColorChoice::Mono,
        ] {
            assert_eq!(
                ColorMode::resolve(choice, Some("truecolor"), true),
                ColorMode::Mono
            );
        }
    }

    #[test]
    fn from_env_honors_an_explicit_mono_choice_whatever_the_environment() {
        assert_eq!(ColorMode::from_env(ColorChoice::Mono), ColorMode::Mono);
        // Any other choice resolves to some mode without panicking.
        let _ = ColorMode::from_env(ColorChoice::Auto);
    }

    #[test]
    fn color_choice_names() {
        assert_eq!(ColorChoice::from_name("auto"), Some(ColorChoice::Auto));
        assert_eq!(
            ColorChoice::from_name("truecolor"),
            Some(ColorChoice::Truecolor)
        );
        assert_eq!(ColorChoice::from_name("256"), Some(ColorChoice::Ansi256));
        assert_eq!(ColorChoice::from_name("mono"), Some(ColorChoice::Mono));
        assert_eq!(ColorChoice::from_name("24bit"), None);
    }

    #[test]
    fn node_color_wraps_after_eight() {
        assert_eq!(node_color(0), NODE_COLORS[0]);
        assert_eq!(node_color(7), NODE_COLORS[7]);
        assert_eq!(node_color(8), NODE_COLORS[0]);
        assert_eq!(node_color(19), NODE_COLORS[3]);
    }

    #[test]
    fn gradient_hits_its_three_stops_and_clamps() {
        assert_eq!(gradient_at(0.0), GRADIENT[0]);
        assert_eq!(gradient_at(0.5), GRADIENT[1]);
        assert_eq!(gradient_at(1.0), GRADIENT[2]);
        assert_eq!(gradient_at(-3.0), GRADIENT[0]);
        assert_eq!(gradient_at(9.0), GRADIENT[2]);
        assert_eq!(gradient_at(f64::NAN), GRADIENT[0]);
    }

    #[test]
    fn allowlist_admits_ascii_braille_and_listed_glyphs() {
        for c in "abc XYZ 0123 ~".chars() {
            assert!(is_allowed(c));
        }
        for c in ['\u{2800}', '\u{2840}', '\u{28FF}'] {
            assert!(is_allowed(c));
        }
        for c in GLYPHS.chars() {
            assert!(is_allowed(c));
        }
    }

    #[test]
    fn allowlist_refuses_banned_glyphs() {
        for c in BANNED.chars() {
            assert!(!is_allowed(c), "{c} is banned");
            assert!(!GLYPHS.contains(c));
        }
        assert!(!is_allowed('\u{2900}'));
        assert!(!is_allowed('é'));
    }

    #[test]
    fn allowlist_holds_the_status_and_bar_glyphs() {
        for c in "●◐○✖✚↻⇄⇣▸▲▼✓✔⚠‖━╸─┊┄▌▀▄█▁▇▏▉".chars()
        {
            assert!(is_allowed(c), "{c}");
        }
    }
}

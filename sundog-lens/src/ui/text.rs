//! Text formatting for the views: counts, rates, byte sizes, durations and
//! clock times, and fitting a string to a column.
//!
//! Every function is pure. Widths count `char`s: the glyph allowlist holds
//! only single-width glyphs.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// `value` as a short human count: `412`, `1.3k`, `4.21k`, `20.1k`, `412k`,
/// `1.2M`. A negative, `NaN` or infinite value reads `0`. Values below 10
/// keep one decimal.
#[must_use]
pub fn count(value: f64) -> String {
    if !value.is_finite() || value <= 0.0 {
        return "0".to_owned();
    }
    if value < 0.05 {
        return "0".to_owned();
    }
    if value < 10.0 {
        return trim_zeros(format!("{value:.1}"));
    }
    if value < 1_000.0 {
        return format!("{value:.0}");
    }
    let (scaled, unit) = if value < 1_000_000.0 {
        (value / 1_000.0, 'k')
    } else if value < 1_000_000_000.0 {
        (value / 1_000_000.0, 'M')
    } else {
        (value / 1_000_000_000.0, 'G')
    };
    let number = if scaled < 10.0 {
        trim_zeros(format!("{scaled:.2}"))
    } else if scaled < 100.0 {
        trim_zeros(format!("{scaled:.1}"))
    } else {
        format!("{scaled:.0}")
    };
    format!("{number}{unit}")
}

/// `text`, a decimal number, without trailing zeros or a bare point.
fn trim_zeros(text: String) -> String {
    if text.contains('.') {
        text.trim_end_matches('0').trim_end_matches('.').to_owned()
    } else {
        text
    }
}

/// A raw sample value as the exporter would print it: a whole number without
/// a point, anything else with up to three decimals, `NaN` and the
/// infinities spelled out.
#[must_use]
pub fn sample(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value.is_infinite() {
        if value > 0.0 { "+Inf" } else { "-Inf" }.to_owned()
    } else if value.fract() == 0.0 && value.abs() < 1e15 {
        format!("{value:.0}")
    } else {
        trim_zeros(format!("{value:.3}"))
    }
}

/// `value` rounded to a whole number with thousands separators: `131,072`.
#[must_use]
pub fn thousands(value: u64) -> String {
    let digits = value.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(digit);
    }
    grouped
}

/// A part or entry count read from a metric: the float rounded to a whole
/// number with thousands separators. A negative or non-finite value reads
/// `0`.
#[must_use]
pub fn whole(value: f64) -> String {
    thousands(to_u64(value))
}

/// `value` as an unsigned whole number: rounded, clamped at zero, and zero
/// for `NaN`.
#[must_use]
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "the value is rounded and clamped to the u64 range first"
)]
pub fn to_u64(value: f64) -> u64 {
    if !value.is_finite() || value <= 0.0 {
        return 0;
    }
    value.round().min(1.8e19) as u64
}

/// `bytes` as a size: `812 B`, `340 KB`, `1.1 MB`, `2.40 GB`.
#[must_use]
pub fn bytes(bytes: f64) -> String {
    const UNITS: [&str; 4] = ["B", "KB", "MB", "GB"];
    let bytes = if bytes.is_finite() {
        bytes.max(0.0)
    } else {
        0.0
    };
    let mut scaled = bytes;
    let mut unit = 0;
    while scaled >= 1000.0 && unit < UNITS.len() - 1 {
        scaled /= 1000.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{scaled:.0} {}", UNITS[unit])
    } else if scaled < 10.0 {
        format!("{scaled:.1} {}", UNITS[unit])
    } else {
        format!("{scaled:.0} {}", UNITS[unit])
    }
}

/// `bytes_per_second` as a rate: `12.6 MB/s`.
#[must_use]
pub fn byte_rate(bytes_per_second: f64) -> String {
    const UNITS: [&str; 4] = ["B/s", "KB/s", "MB/s", "GB/s"];
    let bytes_per_second = if bytes_per_second.is_finite() {
        bytes_per_second.max(0.0)
    } else {
        0.0
    };
    let mut scaled = bytes_per_second;
    let mut unit = 0;
    while scaled >= 1000.0 && unit < UNITS.len() - 1 {
        scaled /= 1000.0;
        unit += 1;
    }
    if unit == 0 || scaled >= 100.0 {
        format!("{scaled:.0} {}", UNITS[unit])
    } else {
        format!("{scaled:.1} {}", UNITS[unit])
    }
}

/// `fraction` (0 to 1) as a percentage with `decimals` decimals: `50.1%`.
#[must_use]
pub fn percent(fraction: f64, decimals: usize) -> String {
    let fraction = if fraction.is_finite() { fraction } else { 0.0 };
    let shown = format!("{:.decimals$}", fraction * 100.0);
    // A tiny negative rounds to "-0"; a percentage is never negative zero.
    let shown = if shown.starts_with('-') && shown.chars().all(|c| matches!(c, '-' | '0' | '.')) {
        shown[1..].to_owned()
    } else {
        shown
    };
    format!("{shown}%")
}

/// How long a node has been up: `01:12` under an hour, `1h02m` under a day,
/// `2d03h` beyond.
#[must_use]
pub fn uptime(span: Duration) -> String {
    let secs = span.as_secs();
    if secs < 3_600 {
        format!("{:02}:{:02}", secs / 60, secs % 60)
    } else if secs < 86_400 {
        format!("{}h{:02}m", secs / 3_600, secs % 3_600 / 60)
    } else {
        format!("{}d{:02}h", secs / 86_400, secs % 86_400 / 3_600)
    }
}

/// A short age: `7s`, `31s`, `2m`, `1h`, `3d`.
#[must_use]
pub fn age(span: Duration) -> String {
    let secs = span.as_secs();
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3_600 {
        format!("{}m", secs / 60)
    } else if secs < 86_400 {
        format!("{}h", secs / 3_600)
    } else {
        format!("{}d", secs / 86_400)
    }
}

/// A span in seconds with one decimal: `2.4 s`.
#[must_use]
pub fn seconds(span: Duration) -> String {
    format!("{:.1} s", span.as_secs_f64())
}

/// A span as minutes and seconds: `0:52`, `12:03`.
#[must_use]
pub fn minutes_seconds(span: Duration) -> String {
    let secs = span.as_secs();
    format!("{}:{:02}", secs / 60, secs % 60)
}

/// The UTC time of day of `time` as `HH:MM:SS`.
#[must_use]
pub fn clock(time: SystemTime) -> String {
    let secs = time.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
    let day = secs % 86_400;
    format!("{:02}:{:02}:{:02}", day / 3_600, day % 3_600 / 60, day % 60)
}

/// The first `width` characters of `text`, ending in `…` when it is cut.
#[must_use]
pub fn fit(text: &str, width: usize) -> String {
    let len = text.chars().count();
    if len <= width {
        return text.to_owned();
    }
    match width {
        0 => String::new(),
        1 => "…".to_owned(),
        _ => {
            let mut cut: String = text.chars().take(width - 1).collect();
            cut.push('…');
            cut
        }
    }
}

/// `text` cut to `width` characters and padded on the right with spaces.
#[must_use]
pub fn pad_right(text: &str, width: usize) -> String {
    let mut fitted = fit(text, width);
    let len = fitted.chars().count();
    fitted.push_str(&" ".repeat(width - len));
    fitted
}

/// `text` cut to `width` characters and padded on the left with spaces.
#[must_use]
pub fn pad_left(text: &str, width: usize) -> String {
    let fitted = fit(text, width);
    let len = fitted.chars().count();
    format!("{}{fitted}", " ".repeat(width - len))
}

/// The first four hex digits of a 16-digit node id.
#[must_use]
pub fn short_id(full: &str) -> &str {
    full.get(..4).unwrap_or(full)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_scale_with_a_unit_and_keep_three_figures() {
        assert_eq!(count(0.0), "0");
        assert_eq!(count(-3.0), "0");
        assert_eq!(count(f64::NAN), "0");
        assert_eq!(count(f64::INFINITY), "0");
        assert_eq!(count(0.01), "0");
        assert_eq!(count(0.4), "0.4");
        assert_eq!(count(2.0), "2");
        assert_eq!(count(2.14), "2.1");
        assert_eq!(count(12.4), "12");
        assert_eq!(count(412.0), "412");
        assert_eq!(count(999.4), "999");
        assert_eq!(count(1_300.0), "1.3k");
        assert_eq!(count(4_210.0), "4.21k");
        assert_eq!(count(20_100.0), "20.1k");
        assert_eq!(count(412_000.0), "412k");
        assert_eq!(count(1_200_000.0), "1.2M");
        assert_eq!(count(3_000_000_000.0), "3G");
    }

    #[test]
    fn thousands_groups_by_three() {
        assert_eq!(thousands(0), "0");
        assert_eq!(thousands(999), "999");
        assert_eq!(thousands(1_000), "1,000");
        assert_eq!(thousands(65_536), "65,536");
        assert_eq!(thousands(131_072), "131,072");
        assert_eq!(thousands(1_234_567), "1,234,567");
    }

    #[test]
    fn samples_print_whole_numbers_plainly_and_others_to_three_decimals() {
        assert_eq!(sample(0.0), "0");
        assert_eq!(sample(2040.0), "2040");
        assert_eq!(sample(681.5), "681.5");
        assert_eq!(sample(0.12345), "0.123");
        assert_eq!(sample(18380.925), "18380.925");
        assert_eq!(sample(f64::NAN), "NaN");
        assert_eq!(sample(f64::INFINITY), "+Inf");
        assert_eq!(sample(f64::NEG_INFINITY), "-Inf");
        assert_eq!(sample(1e20), "100000000000000000000");
    }

    #[test]
    fn whole_rounds_and_clamps() {
        assert_eq!(whole(32_840.4), "32,840");
        assert_eq!(whole(681.5), "682");
        assert_eq!(whole(-1.0), "0");
        assert_eq!(whole(f64::NAN), "0");
        assert_eq!(to_u64(1e30), 18_000_000_000_000_000_000);
    }

    #[test]
    fn sizes_and_rates_pick_a_unit() {
        assert_eq!(bytes(812.0), "812 B");
        assert_eq!(bytes(340_000.0), "340 KB");
        assert_eq!(bytes(1_100_000.0), "1.1 MB");
        assert_eq!(bytes(2_400_000_000.0), "2.4 GB");
        assert_eq!(bytes(-1.0), "0 B");
        assert_eq!(byte_rate(12_600_000.0), "12.6 MB/s");
        assert_eq!(byte_rate(900.0), "900 B/s");
        assert_eq!(byte_rate(150_000.0), "150 KB/s");
        assert_eq!(byte_rate(f64::NAN), "0 B/s");
    }

    #[test]
    fn percentages_keep_the_requested_decimals() {
        assert_eq!(percent(0.501, 1), "50.1%");
        assert_eq!(percent(1.0, 0), "100%");
        assert_eq!(percent(0.0, 1), "0.0%");
        assert_eq!(percent(f64::NAN, 1), "0.0%");
        assert_eq!(percent(-0.0, 0), "0%");
        assert_eq!(percent(-0.0001, 1), "0.0%");
        assert_eq!(percent(-0.25, 0), "-25%");
    }

    #[test]
    fn uptime_ages_and_stopwatches() {
        assert_eq!(uptime(Duration::from_secs(72)), "01:12");
        assert_eq!(uptime(Duration::from_secs(3_725)), "1h02m");
        assert_eq!(uptime(Duration::from_hours(25)), "1d01h");
        assert_eq!(age(Duration::from_secs(7)), "7s");
        assert_eq!(age(Duration::from_secs(125)), "2m");
        assert_eq!(age(Duration::from_secs(7_300)), "2h");
        assert_eq!(age(Duration::from_secs(200_000)), "2d");
        assert_eq!(seconds(Duration::from_millis(2_440)), "2.4 s");
        assert_eq!(minutes_seconds(Duration::from_secs(52)), "0:52");
        assert_eq!(minutes_seconds(Duration::from_secs(723)), "12:03");
    }

    #[test]
    fn the_clock_is_utc_time_of_day() {
        assert_eq!(clock(UNIX_EPOCH), "00:00:00");
        assert_eq!(
            clock(UNIX_EPOCH + Duration::from_secs(86_400 + 14 * 3600 + 3 * 60 + 27)),
            "14:03:27"
        );
        assert_eq!(clock(UNIX_EPOCH - Duration::from_secs(5)), "00:00:00");
    }

    #[test]
    fn fitting_cuts_with_an_ellipsis_and_pads_with_spaces() {
        assert_eq!(fit("abc", 5), "abc");
        assert_eq!(fit("abcdef", 4), "abc…");
        assert_eq!(fit("abcdef", 1), "…");
        assert_eq!(fit("abcdef", 0), "");
        assert_eq!(pad_right("ab", 5), "ab   ");
        assert_eq!(pad_right("abcdef", 4), "abc…");
        assert_eq!(pad_left("ab", 5), "   ab");
        assert_eq!(pad_left("abcdef", 4), "abc…");
        assert_eq!(pad_right("", 0), "");
    }

    #[test]
    fn short_ids_take_four_digits() {
        assert_eq!(short_id("7f3a51d2e09bc210"), "7f3a");
        assert_eq!(short_id("ab"), "ab");
    }
}

//! A parser for the Prometheus text exposition format, keeping the
//! `sundog_*` samples the lens reads.

/// One sample line: a metric name, its labels and its value.
#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    /// The metric name, such as `sundog_cache_entries`.
    pub name: String,
    /// The labels in the order they appear, values unescaped.
    pub labels: Vec<(String, String)>,
    /// The sample value. `NaN` and the infinities parse.
    pub value: f64,
}

impl Sample {
    /// The value of label `key`, if the sample has it.
    #[must_use]
    pub fn label(&self, key: &str) -> Option<&str> {
        self.labels
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.as_str())
    }
}

/// Parses `text` and returns its `sundog_*` samples in order. Comment lines
/// (`# HELP`, `# TYPE`), blank lines, samples of other metrics, histogram
/// `_bucket` lines and any line that does not parse are skipped. A trailing
/// timestamp is ignored.
#[must_use]
pub fn parse(text: &str) -> Vec<Sample> {
    text.lines().filter_map(parse_line).collect()
}

/// Parses one line into a `sundog_*` sample.
fn parse_line(line: &str) -> Option<Sample> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') || !line.starts_with("sundog_") {
        return None;
    }
    let name_end = line.find(['{', ' ', '\t']).unwrap_or(line.len());
    let name = &line[..name_end];
    if name.ends_with("_bucket") {
        return None;
    }
    let rest = &line[name_end..];
    let (labels, rest) = if rest.starts_with('{') {
        parse_labels(rest)?
    } else {
        (Vec::new(), rest)
    };
    // The value, then an optional timestamp.
    let value = parse_value(rest.split_whitespace().next()?)?;
    Some(Sample {
        name: name.to_owned(),
        labels,
        value,
    })
}

/// Parses a `{k="v",...}` block at the start of `s` and returns the labels
/// and the text after the closing brace.
fn parse_labels(s: &str) -> Option<(Vec<(String, String)>, &str)> {
    let mut labels = Vec::new();
    let mut rest = s.strip_prefix('{')?;
    loop {
        rest = rest.trim_start();
        if let Some(after) = rest.strip_prefix('}') {
            return Some((labels, after));
        }
        let eq = rest.find('=')?;
        let key = rest[..eq].trim();
        if key.is_empty() {
            return None;
        }
        rest = rest[eq + 1..].trim_start().strip_prefix('"')?;
        let (value, after) = parse_quoted(rest)?;
        labels.push((key.to_owned(), value));
        rest = after.trim_start();
        if let Some(after) = rest.strip_prefix(',') {
            rest = after;
        } else if !rest.starts_with('}') {
            return None;
        }
    }
}

/// Reads a label value up to its closing quote, resolving `\\`, `\"` and
/// `\n`. Returns the value and the text after the quote.
fn parse_quoted(s: &str) -> Option<(String, &str)> {
    let mut value = String::new();
    let mut chars = s.char_indices();
    while let Some((i, c)) = chars.next() {
        match c {
            '"' => return Some((value, &s[i + 1..])),
            '\\' => match chars.next()?.1 {
                '\\' => value.push('\\'),
                '"' => value.push('"'),
                'n' => value.push('\n'),
                other => {
                    value.push('\\');
                    value.push(other);
                }
            },
            other => value.push(other),
        }
    }
    None
}

/// Parses a sample value, including `NaN`, `+Inf` and `-Inf`.
fn parse_value(token: &str) -> Option<f64> {
    match token {
        "NaN" | "nan" => Some(f64::NAN),
        "+Inf" | "Inf" | "inf" | "+inf" => Some(f64::INFINITY),
        "-Inf" | "-inf" => Some(f64::NEG_INFINITY),
        _ => token.parse().ok(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXPOSITION: &str = r#"# HELP sundog_cache_entries live entries on this node
# TYPE sundog_cache_entries gauge
sundog_cache_entries{cache="it"} 9880
sundog_cache_entries{cache="churn"} 512

# TYPE sundog_live_peers gauge
sundog_live_peers 4
sundog_fetch_total{cache="it",outcome="local"} 61000
sundog_fetch_total{cache="it",outcome="remote"} 30000
sundog_backlog_dropped_total{peer="0b77c1d2e3f40516"} 312
process_cpu_seconds_total 12.5
go_goroutines 7
"#;

    #[test]
    fn parses_names_labels_and_values_in_order() {
        let samples = parse(EXPOSITION);
        let names: Vec<_> = samples.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "sundog_cache_entries",
                "sundog_cache_entries",
                "sundog_live_peers",
                "sundog_fetch_total",
                "sundog_fetch_total",
                "sundog_backlog_dropped_total",
            ]
        );
        assert_eq!(samples[0].label("cache"), Some("it"));
        assert!((samples[0].value - 9880.0).abs() < f64::EPSILON);
        let labels = &samples[2].labels;
        assert!(labels.is_empty(), "{labels:?}");
        assert!((samples[2].value - 4.0).abs() < f64::EPSILON);
        assert_eq!(samples[3].labels.len(), 2);
        assert_eq!(samples[4].label("outcome"), Some("remote"));
        assert_eq!(samples[5].label("peer"), Some("0b77c1d2e3f40516"));
        assert_eq!(samples[5].label("cache"), None);
    }

    #[test]
    fn skips_comments_blank_lines_and_other_metrics() {
        let help_only = parse("# HELP sundog_x help\n# TYPE sundog_x gauge\n\n   \n");
        assert!(help_only.is_empty(), "{help_only:?}");
        let foreign_only = parse("process_cpu_seconds_total 1\nhttp_requests_total{a=\"b\"} 2\n");
        assert!(foreign_only.is_empty(), "{foreign_only:?}");
        let commented = parse("  # sundog_commented 1\n");
        assert!(commented.is_empty(), "{commented:?}");
    }

    #[test]
    fn unescapes_label_values() {
        let samples =
            parse(r#"sundog_x{a="q\"uote",b="back\\slash",c="new\nline",d="tab\tkeep"} 1"#);
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].label("a"), Some("q\"uote"));
        assert_eq!(samples[0].label("b"), Some("back\\slash"));
        assert_eq!(samples[0].label("c"), Some("new\nline"));
        assert_eq!(samples[0].label("d"), Some("tab\\tkeep"));
    }

    #[test]
    fn a_label_value_may_hold_braces_commas_equals_and_spaces() {
        let samples = parse(r#"sundog_x{a="}{, =x y"} 3"#);
        assert_eq!(samples[0].label("a"), Some("}{, =x y"));
        assert!((samples[0].value - 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn parses_nan_and_infinities() {
        let samples = parse("sundog_a NaN\nsundog_b +Inf\nsundog_c -Inf\nsundog_d Inf\n");
        assert!(samples[0].value.is_nan());
        assert!(samples[1].value.is_infinite() && samples[1].value.is_sign_positive());
        assert!(samples[2].value.is_infinite() && samples[2].value.is_sign_negative());
        assert!(samples[3].value.is_infinite() && samples[3].value.is_sign_positive());
    }

    #[test]
    fn parses_exponents_and_negative_values() {
        let samples = parse("sundog_a 1.5e3\nsundog_b -2\nsundog_c 0.25\n");
        let values: Vec<f64> = samples.iter().map(|s| s.value).collect();
        assert_eq!(values, [1500.0, -2.0, 0.25]);
    }

    #[test]
    fn ignores_a_trailing_timestamp() {
        let samples = parse("sundog_a{x=\"1\"} 42 1700000000000\nsundog_b 7 1700000000000\n");
        assert_eq!(samples.len(), 2);
        assert!((samples[0].value - 42.0).abs() < f64::EPSILON);
        assert!((samples[1].value - 7.0).abs() < f64::EPSILON);
    }

    #[test]
    fn ignores_histogram_buckets_and_keeps_sum_and_count() {
        let text = "sundog_lat_bucket{le=\"0.1\"} 4\nsundog_lat_bucket{le=\"+Inf\"} 9\n\
                    sundog_lat_sum 1.5\nsundog_lat_count 9\n";
        let samples = parse(text);
        let names: Vec<_> = samples.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, ["sundog_lat_sum", "sundog_lat_count"]);
    }

    #[test]
    fn skips_malformed_lines_and_keeps_the_rest() {
        let text = "sundog_a{x=\"1\" 5\nsundog_b{x=1} 5\nsundog_c\nsundog_d notanumber\n\
                    sundog_e{=\"v\"} 1\nsundog_f{x=\"unterminated} 1\nsundog_ok 9\n";
        let samples = parse(text);
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].name, "sundog_ok");
    }

    #[test]
    fn accepts_an_empty_label_block_and_a_trailing_comma() {
        let samples = parse("sundog_a{} 1\nsundog_b{x=\"1\",} 2\n");
        assert_eq!(samples.len(), 2);
        let labels = &samples[0].labels;
        assert!(labels.is_empty(), "{labels:?}");
        assert_eq!(samples[1].label("x"), Some("1"));
    }

    #[test]
    fn accepts_crlf_line_endings_and_tab_separators() {
        let samples = parse("sundog_a 1\r\nsundog_b\t2\r\n");
        assert_eq!(samples.len(), 2);
        assert!((samples[1].value - 2.0).abs() < f64::EPSILON);
    }

    /// A capture of `/metrics` from a running `sundog-testnode` built with the
    /// `prometheus` feature.
    const CAPTURE: &str = include_str!("../../tests/fixtures/metrics.prom");

    #[test]
    fn parses_every_sample_line_of_a_real_capture() {
        let samples = parse(CAPTURE);
        let sample_lines = CAPTURE
            .lines()
            .filter(|line| line.starts_with("sundog_"))
            .count();
        assert!(sample_lines > 30, "the capture holds real samples");
        assert_eq!(samples.len(), sample_lines);
        assert!(
            samples
                .iter()
                .all(|s| s.value.is_finite() && s.value >= 0.0)
        );
        let find = |name: &str, labels: &[(&str, &str)]| {
            samples
                .iter()
                .find(|s| s.name == name && labels.iter().all(|(k, v)| s.label(k) == Some(*v)))
                .map(|s| s.value)
        };
        assert_eq!(find("sundog_live_peers", &[]), Some(2.0));
        assert_eq!(find("sundog_open_caches", &[]), Some(4.0));
        assert_eq!(
            find("sundog_owned_parts", &[("cache", "it")]),
            Some(43_616.0)
        );
        assert_eq!(
            find("sundog_owned_buckets", &[("cache", "it")]),
            Some(681.5)
        );
        assert_eq!(
            find("sundog_cache_hits_total", &[("cache", "it")]),
            Some(268.0)
        );
        assert_eq!(
            find(
                "sundog_fetch_total",
                &[("cache", "it"), ("outcome", "remote")]
            ),
            Some(39.0)
        );
        assert_eq!(
            find(
                "sundog_rebalance_parts_total",
                &[("cache", "it"), ("direction", "in")]
            ),
            Some(43_616.0)
        );
    }
}

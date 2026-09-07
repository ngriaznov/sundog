//! Resident-set-size reporting: parses `/proc/self/status` on Linux, `"n/a"`
//! everywhere else. Split so the parsing itself, the part worth getting
//! wrong, is a pure function over a string.

/// Parses the `VmRSS:` line out of the contents of `/proc/<pid>/status`,
/// returning the value in kibibytes.
///
/// `/proc/self/status` reports one `key: value unit` pair per line; `VmRSS`'s
/// unit is always `kB` (kibibytes, despite the label).
#[must_use]
pub(crate) fn parse_vmrss_kb(status: &str) -> Option<u64> {
    status.lines().find_map(|line| {
        let rest = line.strip_prefix("VmRSS:")?;
        rest.split_whitespace().next()?.parse().ok()
    })
}

/// Formats a `VmRSS` reading in kibibytes as a human-scaled string, or
/// `"n/a"` when the platform doesn't expose one.
#[must_use]
pub(crate) fn format_rss(kb: Option<u64>) -> String {
    match kb {
        Some(kb) if kb >= 1024 * 1024 => format!(
            "{:.2} GiB",
            f64::from(u32::try_from(kb).unwrap_or(u32::MAX)) / (1024.0 * 1024.0)
        ),
        Some(kb) if kb >= 1024 => format!(
            "{:.1} MiB",
            f64::from(u32::try_from(kb).unwrap_or(u32::MAX)) / 1024.0
        ),
        Some(kb) => format!("{kb} KiB"),
        None => "n/a".to_owned(),
    }
}

/// Reads this process's current `VmRSS` in kibibytes. `None` off Linux, or
/// if `/proc/self/status` couldn't be read or didn't carry the line.
#[must_use]
pub(crate) fn read_rss_kb() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        parse_vmrss_kb(&status)
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_STATUS: &str = "Name:\tsundog-distributed-demo\n\
VmPeak:\t  123456 kB\n\
VmRSS:\t   98765 kB\n\
VmData:\t   45678 kB\n";

    #[test]
    fn parses_vmrss_line_out_of_a_full_status_dump() {
        assert_eq!(parse_vmrss_kb(SAMPLE_STATUS), Some(98_765));
    }

    #[test]
    fn missing_vmrss_line_is_none() {
        assert_eq!(parse_vmrss_kb("Name:\tfoo\nVmPeak:\t1 kB\n"), None);
    }

    #[test]
    fn empty_status_is_none() {
        assert_eq!(parse_vmrss_kb(""), None);
    }

    #[test]
    fn format_rss_scales_by_magnitude() {
        assert_eq!(format_rss(None), "n/a");
        assert_eq!(format_rss(Some(512)), "512 KiB");
        assert_eq!(format_rss(Some(2048)), "2.0 MiB");
        assert_eq!(format_rss(Some(3 * 1024 * 1024)), "3.00 GiB");
    }
}

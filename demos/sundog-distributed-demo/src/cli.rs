//! Command-line arguments for the distributed demo: node count, key-space
//! size, owner count, value size, the per-node RAM cap and spill tier, the
//! in-process metrics reporter, and either the interactive TUI or a
//! fixed-duration `--headless` smoke run.

use std::num::NonZeroU8;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context as _, bail};

/// Parsed command-line configuration for one run of the demo.
#[derive(Debug, Clone)]
pub(crate) struct Args {
    pub(crate) nodes: usize,
    pub(crate) headless: Option<Duration>,
    pub(crate) cluster_name: String,
    pub(crate) keys: usize,
    pub(crate) owners: NonZeroU8,
    pub(crate) write_interval: Duration,
    pub(crate) gossip_base_port: Option<u16>,
    /// Pads every preload and load value out to this many bytes; `0`
    /// leaves the short `v{i}` values.
    pub(crate) value_bytes: usize,
    pub(crate) tuning: CacheTuning,
    /// `Some(interval)` prints an in-process metrics line every `interval`
    /// and a full `sundog_*` dump at each milestone of a headless run.
    pub(crate) metrics: Option<Duration>,
    /// `Some(path)` writes a JSON [`crate::report::Report`] to `path` at the
    /// end of a headless run; needs `metrics` on.
    pub(crate) report_json: Option<PathBuf>,
    /// `Some(path)` reads a JSON [`crate::report::Gate`] from `path` and
    /// checks the headless run's report against it, exiting nonzero on any
    /// violated threshold; needs `metrics` on.
    pub(crate) gate: Option<PathBuf>,
}

/// Per-node cache sizing: the RAM entry cap and the spill tier that catches
/// what the cap evicts.
#[derive(Debug, Clone)]
pub(crate) struct CacheTuning {
    /// `CacheBuilder::max_capacity`, in entries per node; `None` leaves the
    /// cache unbounded.
    pub(crate) max_entries: Option<u64>,
    /// Root of the spill tier; each node gets its own subdirectory.
    pub(crate) spill_dir: Option<PathBuf>,
    /// `SpillConfig::capacity_bytes` per node.
    pub(crate) spill_capacity_bytes: u64,
    /// `SpillConfig::region_bytes`; `None` keeps the library default.
    pub(crate) spill_region_bytes: Option<u64>,
    /// `SpillConfig::flush_queue_bytes`; `None` keeps the library default
    /// of one region.
    pub(crate) spill_flush_queue_bytes: Option<u64>,
}

const MIB: u64 = 1024 * 1024;

impl Default for CacheTuning {
    fn default() -> Self {
        Self {
            max_entries: None,
            spill_dir: None,
            spill_capacity_bytes: 4096 * MIB,
            spill_region_bytes: None,
            spill_flush_queue_bytes: None,
        }
    }
}

impl Default for Args {
    fn default() -> Self {
        Self {
            nodes: 5,
            headless: None,
            cluster_name: "sundog-demo-distributed".to_owned(),
            keys: 2_000_000,
            owners: NonZeroU8::new(2).expect("2 is nonzero"),
            write_interval: Duration::from_millis(50),
            gossip_base_port: None,
            value_bytes: 0,
            tuning: CacheTuning::default(),
            metrics: None,
            report_json: None,
            gate: None,
        }
    }
}

const DEFAULT_METRICS_INTERVAL: Duration = Duration::from_secs(15);

/// Parses `std::env::args()`, minus `argv[0]`, into [`Args`].
///
/// # Errors
///
/// Returns an error for an unknown flag, a missing value, or a bad value.
pub(crate) fn parse(mut args: impl Iterator<Item = String>) -> anyhow::Result<Args> {
    let mut parsed = Args::default();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--nodes" => {
                parsed.nodes = args
                    .next()
                    .context("--nodes needs a value")?
                    .parse()
                    .context("--nodes must be a positive integer")?;
                if parsed.nodes == 0 {
                    bail!("--nodes must be at least 1");
                }
            }
            "--headless" => {
                let secs: u64 = args
                    .next()
                    .context("--headless needs a value (seconds)")?
                    .parse()
                    .context("--headless must be an integer number of seconds")?;
                parsed.headless = Some(Duration::from_secs(secs));
            }
            "--cluster" => parsed.cluster_name = args.next().context("--cluster needs a value")?,
            "--keys" => {
                parsed.keys = args
                    .next()
                    .context("--keys needs a value")?
                    .parse()
                    .context("--keys must be a positive integer")?;
                if parsed.keys == 0 {
                    bail!("--keys must be at least 1");
                }
            }
            "--owners" => {
                let raw: u8 = args
                    .next()
                    .context("--owners needs a value")?
                    .parse()
                    .context("--owners must be an integer between 2 and 255")?;
                parsed.owners =
                    NonZeroU8::new(raw).with_context(|| "--owners must be at least 2")?;
                if parsed.owners.get() < 2 {
                    bail!("--owners must be at least 2");
                }
            }
            "--write-interval-ms" => {
                let ms: u64 = args
                    .next()
                    .context("--write-interval-ms needs a value")?
                    .parse()
                    .context("--write-interval-ms must be an integer")?;
                parsed.write_interval = Duration::from_millis(ms.max(1));
            }
            "--gossip-base-port" => {
                parsed.gossip_base_port = Some(
                    args.next()
                        .context("--gossip-base-port needs a value")?
                        .parse()
                        .context("--gossip-base-port must be a 16-bit port number")?,
                );
            }
            flag if parse_sizing_flag(flag, &mut args, &mut parsed)? => {}
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            other => bail!("unrecognized argument: {other} (try --help)"),
        }
    }
    if parsed.tuning.spill_dir.is_some() && !cfg!(feature = "spill") {
        bail!("--spill-dir needs the demo built with --features spill");
    }
    if parsed.tuning.spill_dir.is_none() && parsed.tuning.max_entries.is_some() {
        bail!(
            "--max-entries needs --spill-dir: a distributed cache refuses a RAM cap with no spill tier to catch the evictions"
        );
    }
    if parsed.metrics.is_some() && !cfg!(feature = "prometheus") {
        bail!("--metrics needs the demo built with --features prometheus");
    }
    if parsed.report_json.is_some() && parsed.metrics.is_none() {
        bail!("--report-json needs --metrics");
    }
    if parsed.gate.is_some() && parsed.metrics.is_none() {
        bail!("--gate needs --metrics");
    }
    Ok(parsed)
}

/// Handles the value-size, RAM-cap, spill and metrics flags; `Ok(false)`
/// for any other flag.
///
/// # Errors
///
/// Returns an error for a missing or bad value.
fn parse_sizing_flag(
    flag: &str,
    args: &mut impl Iterator<Item = String>,
    parsed: &mut Args,
) -> anyhow::Result<bool> {
    match flag {
        "--value-bytes" => {
            parsed.value_bytes = args
                .next()
                .context("--value-bytes needs a value")?
                .parse()
                .context("--value-bytes must be a non-negative integer")?;
        }
        "--max-entries" => {
            let n: u64 = args
                .next()
                .context("--max-entries needs a value")?
                .parse()
                .context("--max-entries must be a positive integer")?;
            if n == 0 {
                bail!("--max-entries must be at least 1");
            }
            parsed.tuning.max_entries = Some(n);
        }
        "--spill-dir" => {
            parsed.tuning.spill_dir = Some(PathBuf::from(
                args.next().context("--spill-dir needs a value")?,
            ));
        }
        "--spill-capacity-mb" => {
            let mb: u64 = args
                .next()
                .context("--spill-capacity-mb needs a value")?
                .parse()
                .context("--spill-capacity-mb must be a positive integer")?;
            if mb == 0 {
                bail!("--spill-capacity-mb must be at least 1");
            }
            parsed.tuning.spill_capacity_bytes = mb.saturating_mul(MIB);
        }
        "--spill-region-mb" => {
            parsed.tuning.spill_region_bytes = Some(parse_mib(args, "--spill-region-mb")?);
        }
        "--spill-flush-queue-mb" => {
            parsed.tuning.spill_flush_queue_bytes =
                Some(parse_mib(args, "--spill-flush-queue-mb")?);
        }
        "--metrics" => {
            parsed.metrics.get_or_insert(DEFAULT_METRICS_INTERVAL);
        }
        "--metrics-interval-secs" => {
            let secs: u64 = args
                .next()
                .context("--metrics-interval-secs needs a value")?
                .parse()
                .context("--metrics-interval-secs must be an integer")?;
            parsed.metrics = Some(Duration::from_secs(secs.max(1)));
        }
        "--report-json" => {
            parsed.report_json = Some(PathBuf::from(
                args.next().context("--report-json needs a value")?,
            ));
        }
        "--gate" => {
            parsed.gate = Some(PathBuf::from(args.next().context("--gate needs a value")?));
        }
        _ => return Ok(false),
    }
    Ok(true)
}

/// The next argument as a positive MiB count, in bytes.
fn parse_mib(args: &mut impl Iterator<Item = String>, flag: &str) -> anyhow::Result<u64> {
    let mb: u64 = args
        .next()
        .with_context(|| format!("{flag} needs a value"))?
        .parse()
        .with_context(|| format!("{flag} must be a positive integer"))?;
    if mb == 0 {
        bail!("{flag} must be at least 1");
    }
    Ok(mb.saturating_mul(MIB))
}

fn print_help() {
    println!(
        "sundog-distributed-demo — distribution-mode TUI for a sundog cluster\n\n\
         USAGE:\n    sundog-distributed-demo [OPTIONS]\n\n\
         OPTIONS:\n\
         \x20   --nodes <N>                 number of in-process nodes (default 5)\n\
         \x20   --keys <N>                  preloaded key-space size (default 2000000)\n\
         \x20   --owners <N>                live owners per bucket, >= 2 (default 2)\n\
         \x20   --headless <SECS>           run without a TUI for SECS seconds, killing and\n\
         \x20                               restarting one node after half the tombstone TTL at\n\
         \x20                               most, then print a convergence report and exit\n\
         \x20                               nonzero on divergence\n\
         \x20   --cluster <NAME>            cluster name (default sundog-demo-distributed)\n\
         \x20   --write-interval-ms <N>     delay between load ticks (default 50)\n\
         \x20   --gossip-base-port <PORT>   first loopback gossip port (default random)\n\
         \x20   --value-bytes <N>           pad every value out to N bytes (default 0: short v{{i}} values)\n\
         \x20   --max-entries <N>           RAM entry cap per node; needs --spill-dir (default unbounded)\n\
         \x20   --spill-dir <PATH>          spill tier root, one subdirectory per node (needs --features spill)\n\
         \x20   --spill-capacity-mb <N>     spill disk budget per node in MiB (default 4096)\n\
         \x20   --spill-region-mb <N>       spill region file size in MiB (default 64)\n\
         \x20   --spill-flush-queue-mb <N>  queued-but-unwritten spill bytes before eviction drops instead (default one region)\n\
         \x20   --metrics                   print in-process sundog_* metrics during a headless run (needs --features prometheus)\n\
         \x20   --metrics-interval-secs <N> seconds between metrics lines (default 15, implies --metrics)\n\
         \x20   --report-json <PATH>        write a JSON run report to PATH at the end of --headless (needs --metrics)\n\
         \x20   --gate <PATH>               check the run report against a JSON threshold file, exit nonzero on any violation (needs --metrics)\n\
         \x20   -h, --help                  print this help\n\n\
         KEYS (TUI): up/down or j/k move, 1-9/enter select, K kill, R restart, P pause/resume load, q quit"
    );
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[test]
    fn defaults_are_five_nodes_two_million_keys_two_owners_interactive() {
        let args = parse(std::iter::empty()).expect("empty args parse");
        assert_eq!(args.nodes, 5);
        assert_eq!(args.keys, 2_000_000);
        assert_eq!(args.owners.get(), 2);
        assert!(args.headless.is_none());
    }

    #[test]
    fn parses_nodes_keys_owners_and_headless() {
        let args = parse(
            [
                "--nodes",
                "3",
                "--keys",
                "200000",
                "--owners",
                "3",
                "--headless",
                "10",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expect("valid args parse");
        assert_eq!(args.nodes, 3);
        assert_eq!(args.keys, 200_000);
        assert_eq!(args.owners.get(), 3);
        assert_eq!(args.headless, Some(Duration::from_secs(10)));
    }

    #[test]
    fn rejects_zero_nodes() {
        assert!(parse(["--nodes", "0"].into_iter().map(str::to_owned)).is_err());
    }

    #[test]
    fn rejects_zero_keys() {
        assert!(parse(["--keys", "0"].into_iter().map(str::to_owned)).is_err());
    }

    #[test]
    fn rejects_owners_below_two() {
        assert!(parse(["--owners", "1"].into_iter().map(str::to_owned)).is_err());
        assert!(parse(["--owners", "0"].into_iter().map(str::to_owned)).is_err());
    }

    #[test]
    fn rejects_unknown_flag() {
        assert!(parse(["--bogus"].into_iter().map(str::to_owned)).is_err());
    }

    #[test]
    fn parses_value_bytes_and_metrics_interval() {
        let args = parse(
            ["--value-bytes", "256", "--metrics-interval-secs", "5"]
                .into_iter()
                .map(str::to_owned),
        );
        if cfg!(feature = "prometheus") {
            let args = args.expect("valid args parse");
            assert_eq!(args.value_bytes, 256);
            assert_eq!(args.metrics, Some(Duration::from_secs(5)));
        } else {
            assert!(args.is_err(), "--metrics needs the prometheus feature");
        }
    }

    #[test]
    fn report_json_needs_metrics() {
        let args = parse(
            ["--report-json", "/tmp/report.json"]
                .into_iter()
                .map(str::to_owned),
        );
        assert!(args.is_err(), "--report-json with no --metrics is refused");

        let args = parse(
            ["--metrics", "--report-json", "/tmp/report.json"]
                .into_iter()
                .map(str::to_owned),
        );
        if cfg!(feature = "prometheus") {
            let args = args.expect("valid args parse");
            assert_eq!(
                args.report_json.as_deref(),
                Some(Path::new("/tmp/report.json"))
            );
        } else {
            assert!(args.is_err(), "--metrics needs the prometheus feature");
        }
    }

    #[test]
    fn gate_needs_metrics() {
        let args = parse(["--gate", "/tmp/gate.json"].into_iter().map(str::to_owned));
        assert!(args.is_err(), "--gate with no --metrics is refused");

        let args = parse(
            ["--metrics", "--gate", "/tmp/gate.json"]
                .into_iter()
                .map(str::to_owned),
        );
        if cfg!(feature = "prometheus") {
            let args = args.expect("valid args parse");
            assert_eq!(args.gate.as_deref(), Some(Path::new("/tmp/gate.json")));
        } else {
            assert!(args.is_err(), "--metrics needs the prometheus feature");
        }
    }

    #[test]
    fn spill_flags_need_the_feature_and_each_other() {
        assert!(
            parse(["--max-entries", "10"].into_iter().map(str::to_owned)).is_err(),
            "a RAM cap with no spill tier is refused"
        );
        let args = parse(
            [
                "--spill-dir",
                "/tmp/spill",
                "--max-entries",
                "10",
                "--spill-capacity-mb",
                "512",
            ]
            .into_iter()
            .map(str::to_owned),
        );
        if cfg!(feature = "spill") {
            let args = args.expect("valid args parse");
            assert_eq!(args.tuning.max_entries, Some(10));
            assert_eq!(
                args.tuning.spill_dir.as_deref(),
                Some(Path::new("/tmp/spill"))
            );
            assert_eq!(args.tuning.spill_capacity_bytes, 512 * MIB);
        } else {
            assert!(args.is_err(), "--spill-dir needs the spill feature");
        }
    }
}

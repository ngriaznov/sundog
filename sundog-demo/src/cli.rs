//! Command-line arguments for the chaos demo: node count, and either the
//! interactive TUI or a fixed-duration `--headless` smoke run.

use std::time::Duration;

use anyhow::{Context as _, bail};

/// Parsed command-line configuration for one run of the demo.
#[derive(Debug, Clone)]
pub(crate) struct Args {
    pub(crate) nodes: usize,
    pub(crate) headless: Option<Duration>,
    pub(crate) cluster_name: String,
    pub(crate) key_space: usize,
    pub(crate) write_interval: Duration,
    pub(crate) gossip_base_port: Option<u16>,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            nodes: 5,
            headless: None,
            cluster_name: "sundog-demo-chaos".to_owned(),
            key_space: 64,
            write_interval: Duration::from_millis(120),
            gossip_base_port: None,
        }
    }
}

/// Parses `std::env::args()`, minus `argv[0]`, into [`Args`].
/// # Errors
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
            "--key-space" => {
                parsed.key_space = args
                    .next()
                    .context("--key-space needs a value")?
                    .parse()
                    .context("--key-space must be a positive integer")?;
                if parsed.key_space == 0 {
                    bail!("--key-space must be at least 1");
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
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            other => bail!("unrecognized argument: {other} (try --help)"),
        }
    }
    Ok(parsed)
}

fn print_help() {
    println!(
        "sundog-demo — chaos TUI for a replicated sundog cluster\n\n\
         USAGE:\n    sundog-demo [OPTIONS]\n\n\
         OPTIONS:\n\
         \x20   --nodes <N>                 number of in-process nodes (default 5)\n\
         \x20   --headless <SECS>           run without a TUI for SECS seconds, then print\n\
         \x20                               a convergence report and exit nonzero on divergence\n\
         \x20   --cluster <NAME>            cluster name (default sundog-demo-chaos)\n\
         \x20   --key-space <N>             distinct keys the write load cycles over (default 64)\n\
         \x20   --write-interval-ms <N>     delay between load writes (default 120)\n\
         \x20   --gossip-base-port <PORT>   first loopback gossip port (default random)\n\
         \x20   -h, --help                  print this help\n\n\
         KEYS (TUI): up/down or j/k move, 1-9/enter select, K kill, R restart, P pause/resume load, q quit"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_five_nodes_and_interactive() {
        let args = parse(std::iter::empty()).expect("empty args parse");
        assert_eq!(args.nodes, 5);
        assert!(args.headless.is_none());
    }

    #[test]
    fn parses_nodes_and_headless() {
        let args = parse(
            ["--nodes", "3", "--headless", "10"]
                .into_iter()
                .map(str::to_owned),
        )
        .expect("valid args parse");
        assert_eq!(args.nodes, 3);
        assert_eq!(args.headless, Some(Duration::from_secs(10)));
    }

    #[test]
    fn rejects_zero_nodes() {
        assert!(parse(["--nodes", "0"].into_iter().map(str::to_owned)).is_err());
    }

    #[test]
    fn rejects_unknown_flag() {
        assert!(parse(["--bogus"].into_iter().map(str::to_owned)).is_err());
    }
}

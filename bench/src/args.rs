//! Command-line arguments.

use std::path::PathBuf;

use anyhow::{Context as _, bail};

use crate::target::Kind;
use crate::workload::Workload;

#[derive(Debug, Clone, PartialEq)]
pub struct Args {
    pub targets: Vec<Kind>,
    pub workload: Workload,
    pub report_json: Option<PathBuf>,
    pub report_md: Option<PathBuf>,
    pub gate: Option<PathBuf>,
}

pub const USAGE: &str = "\
sundog-bench [options]

  --targets a,b,...     targets to run, default all:
                        sundog-local, sundog-replicated, sundog-distributed,
                        redis, valkey, dragonfly, olric, hazelcast
  --keys N              distinct keys, all loaded first (default 100000)
  --value-bytes N       bytes per value (default 100)
  --ops N               measured operations (default 200000)
  --warmup-ops N        unmeasured operations first (default ops / 10)
  --concurrency N       concurrent workers (default 16)
  --read-ratio F        fraction of reads, 0 to 1 (default 0.9)
  --zipf F              zipf exponent, 0 is uniform (default 0.99)
  --seed N              random seed (default 7)
  --report-json PATH    write the report as JSON
  --report-md PATH      write the report as Markdown
  --gate PATH           check the report against a JSON gate file
  -h, --help            print this text

Servers run in containers through rightsize; set RIGHTSIZE_BACKEND=docker.";

/// Whether the arguments ask for the usage text instead of a run.
#[must_use]
pub fn wants_help(args: &[String]) -> bool {
    args.iter().any(|arg| arg == "--help" || arg == "-h")
}

/// Parses the arguments after the program name.
///
/// # Errors
///
/// Returns an error for an unknown flag, a missing or malformed value, an
/// unknown target, or a workload that cannot run.
pub fn parse(args: impl IntoIterator<Item = String>) -> anyhow::Result<Args> {
    let mut out = Args {
        targets: Kind::ALL.to_vec(),
        workload: Workload::default(),
        report_json: None,
        report_md: None,
        gate: None,
    };
    let mut warmup_set = false;
    let mut args = args.into_iter();
    while let Some(flag) = args.next() {
        let mut value = || args.next().with_context(|| format!("{flag} needs a value"));
        match flag.as_str() {
            "--targets" => {
                out.targets = value()?
                    .split(',')
                    .map(|name| {
                        Kind::parse(name.trim()).with_context(|| format!("unknown target {name}"))
                    })
                    .collect::<anyhow::Result<_>>()?;
            }
            "--keys" => out.workload.keys = value()?.parse()?,
            "--value-bytes" => out.workload.value_bytes = value()?.parse()?,
            "--ops" => out.workload.ops = value()?.parse()?,
            "--warmup-ops" => {
                out.workload.warmup_ops = value()?.parse()?;
                warmup_set = true;
            }
            "--concurrency" => out.workload.concurrency = value()?.parse()?,
            "--read-ratio" => out.workload.read_ratio = value()?.parse()?,
            "--zipf" => out.workload.zipf_exponent = value()?.parse()?,
            "--seed" => out.workload.seed = value()?.parse()?,
            "--report-json" => out.report_json = Some(value()?.into()),
            "--report-md" => out.report_md = Some(value()?.into()),
            "--gate" => out.gate = Some(value()?.into()),
            other => bail!("unknown option {other}\n\n{USAGE}"),
        }
    }
    if !warmup_set {
        out.workload.warmup_ops = out.workload.ops / 10;
    }
    let w = &out.workload;
    if w.keys == 0 || w.concurrency == 0 || w.ops == 0 {
        bail!("--keys, --concurrency and --ops must be positive");
    }
    if !(0.0..=1.0).contains(&w.read_ratio) {
        bail!("--read-ratio must lie between 0 and 1");
    }
    if w.zipf_exponent < 0.0 {
        bail!("--zipf must not be negative");
    }
    if out.targets.is_empty() {
        bail!("--targets names no target");
    }
    Ok(out)
}

/// The arguments that run `kind` alone under `workload` and write its
/// report to `report_json`: how `sundog-bench` runs each target in a fresh
/// process of its own.
#[must_use]
pub fn child_args(kind: Kind, workload: &Workload, report_json: &std::path::Path) -> Vec<String> {
    vec![
        "--targets".into(),
        kind.name().into(),
        "--keys".into(),
        workload.keys.to_string(),
        "--value-bytes".into(),
        workload.value_bytes.to_string(),
        "--ops".into(),
        workload.ops.to_string(),
        "--warmup-ops".into(),
        workload.warmup_ops.to_string(),
        "--concurrency".into(),
        workload.concurrency.to_string(),
        "--read-ratio".into(),
        workload.read_ratio.to_string(),
        "--zipf".into(),
        workload.zipf_exponent.to_string(),
        "--seed".into(),
        workload.seed.to_string(),
        "--report-json".into(),
        report_json.display().to_string(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::servers::Server;
    use crate::sundog_target::SundogMode;

    fn parse_str(line: &str) -> anyhow::Result<Args> {
        parse(line.split_whitespace().map(str::to_string))
    }

    #[test]
    fn help_is_asked_for_by_either_flag() {
        let args = |line: &str| {
            line.split_whitespace()
                .map(str::to_string)
                .collect::<Vec<_>>()
        };
        assert!(wants_help(&args("--help")));
        assert!(wants_help(&args("--keys 10 -h")));
        assert!(!wants_help(&args("--keys 10")));
    }

    #[test]
    fn defaults_run_every_target() {
        let args = parse_str("").expect("defaults parse");
        assert_eq!(args.targets, Kind::ALL.to_vec());
        assert_eq!(args.workload.warmup_ops, args.workload.ops / 10);
    }

    #[test]
    fn every_flag_lands_in_its_field() {
        let args = parse_str(
            "--targets sundog-local,valkey --keys 10 --value-bytes 8 --ops 50 --warmup-ops 5 \
             --concurrency 2 --read-ratio 0.5 --zipf 1.1 --seed 9 --report-json r.json \
             --report-md r.md --gate g.json",
        )
        .expect("parses");
        assert_eq!(
            args.targets,
            vec![
                Kind::Sundog(SundogMode::Local),
                Kind::Server(Server::Valkey)
            ]
        );
        let w = &args.workload;
        assert_eq!(
            (w.keys, w.value_bytes, w.ops, w.warmup_ops, w.concurrency),
            (10, 8, 50, 5, 2)
        );
        assert!((w.read_ratio - 0.5).abs() < f64::EPSILON);
        assert!((w.zipf_exponent - 1.1).abs() < f64::EPSILON);
        assert_eq!(w.seed, 9);
        assert_eq!(args.report_json, Some("r.json".into()));
        assert_eq!(args.report_md, Some("r.md".into()));
        assert_eq!(args.gate, Some("g.json".into()));
    }

    #[test]
    fn child_args_parse_back_to_the_same_workload_and_one_target() {
        let workload = Workload {
            keys: 42,
            value_bytes: 7,
            ops: 900,
            warmup_ops: 11,
            concurrency: 3,
            read_ratio: 0.75,
            zipf_exponent: 1.25,
            seed: 99,
        };
        let kind = Kind::Server(Server::Olric);
        let args = parse(child_args(
            kind,
            &workload,
            std::path::Path::new("out.json"),
        ))
        .expect("child args parse");
        assert_eq!(args.targets, vec![kind]);
        assert_eq!(args.workload, workload);
        assert_eq!(args.report_json, Some("out.json".into()));
    }

    #[test]
    fn bad_input_is_refused() {
        assert!(parse_str("--targets memcached").is_err());
        assert!(parse_str("--keys").is_err());
        assert!(parse_str("--keys ten").is_err());
        assert!(parse_str("--read-ratio 1.5").is_err());
        assert!(parse_str("--ops 0").is_err());
        assert!(parse_str("--frobnicate 1").is_err());
    }
}

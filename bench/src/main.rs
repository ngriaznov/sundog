//! `sundog-bench`: one zipf workload against sundog and the servers it is
//! compared with, reporting read and write latency, throughput and memory
//! per entry.
//!
//! sundog runs in this process, the way a service embeds it. Each server
//! runs in its own container, started through rightsize, and is reached over
//! its published port on loopback. Every target gets the same keys, values,
//! operation mix, worker count and random seed.

mod args;
mod gate;
mod report;
mod runner;
mod servers;
mod summary;
mod sundog_target;
mod target;
mod workload;

use std::process::ExitCode;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use anyhow::Context as _;

use crate::gate::Gate;
use crate::report::{Report, TargetResult};
use crate::target::Kind;

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(code) => code,
        Err(error) => {
            eprintln!("sundog-bench: {error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> anyhow::Result<ExitCode> {
    let raw: Vec<String> = std::env::args().skip(1).collect();
    if args::wants_help(&raw) {
        println!("{}", args::USAGE);
        return Ok(ExitCode::SUCCESS);
    }
    let args = args::parse(raw)?;
    let mut report = Report {
        workload: args.workload.clone(),
        results: Vec::with_capacity(args.targets.len()),
    };
    if let [kind] = args.targets[..] {
        eprintln!("sundog-bench: running {}", kind.name());
        let result = match runner::run(kind, &args.workload).await {
            Ok(result) => result,
            Err(error) => {
                eprintln!("sundog-bench: {} failed: {error:#}", kind.name());
                TargetResult::failed(kind.name(), kind.transport(), format!("{error:#}"))
            }
        };
        report.results.push(result);
    } else {
        // Each target runs in a fresh process, so one target's leftover
        // allocations and threads never touch another's figures.
        for &kind in &args.targets {
            let workload = args.workload.clone();
            let result = tokio::task::spawn_blocking(move || run_child(kind, &workload))
                .await
                .context("the child runner panicked")?;
            report.results.push(result);
        }
    }

    let markdown = report.markdown();
    println!("{markdown}");
    if let Some(path) = &args.report_json {
        std::fs::write(path, serde_json::to_string_pretty(&report)?)
            .with_context(|| format!("write {}", path.display()))?;
    }
    if let Some(path) = &args.report_md {
        std::fs::write(path, &markdown).with_context(|| format!("write {}", path.display()))?;
    }

    let mut failed = report.results.iter().any(|r| r.error.is_some());
    if let Some(path) = &args.gate {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        let gate: Gate =
            serde_json::from_str(&text).with_context(|| format!("parse {}", path.display()))?;
        for violation in gate.violations(&report) {
            eprintln!("sundog-bench: gate: {violation}");
            failed = true;
        }
    }
    Ok(if failed {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    })
}

/// Runs `kind` alone in a child process of this binary and reads back its
/// one-row report.
fn run_child(kind: Kind, workload: &workload::Workload) -> TargetResult {
    let path = std::env::temp_dir().join(format!(
        "sundog-bench-{}-{}.json",
        std::process::id(),
        kind.name()
    ));
    let outcome = std::env::current_exe()
        .context("locate this binary")
        .and_then(|exe| {
            std::process::Command::new(exe)
                .args(args::child_args(kind, workload, &path))
                .stdout(std::process::Stdio::null())
                .status()
                .context("spawn the child run")
        })
        .and_then(|_| std::fs::read_to_string(&path).context("the child wrote no report"))
        .and_then(|text| serde_json::from_str::<Report>(&text).context("parse the child report"))
        .and_then(|report| {
            report
                .results
                .into_iter()
                .next()
                .context("the child report is empty")
        });
    let _ = std::fs::remove_file(&path);
    outcome.unwrap_or_else(|error| {
        TargetResult::failed(kind.name(), kind.transport(), format!("{error:#}"))
    })
}

//! Runs the workload against one target: load every key, warm up, then
//! measure with every worker drawing keys from the same zipf distribution.

use std::sync::Arc;
use std::time::Instant;

use anyhow::Context as _;
use hdrhistogram::Histogram;
use rand::rngs::SmallRng;
use rand::{RngExt as _, SeedableRng as _};

use crate::report::TargetResult;
use crate::summary::{self, Summary};
use crate::target::{Kind, Target};
use crate::workload::{self, Workload, Zipf};

/// What one measured phase recorded.
struct Observed {
    reads: Histogram<u64>,
    writes: Histogram<u64>,
    read_misses: u64,
    errors: u64,
    elapsed_secs: f64,
}

/// Runs `workload` against `kind` from start to shutdown.
///
/// # Errors
///
/// Returns an error if the target fails to start, load or settle.
pub async fn run(kind: Kind, workload: &Workload) -> anyhow::Result<TargetResult> {
    let target = Target::start(kind)
        .await
        .with_context(|| format!("{} starts", kind.name()))?;
    let result = measure(kind, &target, workload).await;
    target.shutdown().await;
    result
}

async fn measure(kind: Kind, target: &Target, workload: &Workload) -> anyhow::Result<TargetResult> {
    let before = target.memory_used().await?;
    load(target, workload).await?;
    target.settle(workload.keys).await?;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let after = target.memory_used().await?;
    let bytes_per_entry = bytes_per_entry(before, after, workload.keys, target.copies());

    let zipf = Arc::new(Zipf::new(workload.keys, workload.zipf_exponent));
    phase(
        target,
        workload,
        &zipf,
        workload.warmup_ops,
        workload.seed ^ 0xA5A5,
    )
    .await?;
    let observed = phase(target, workload, &zipf, workload.ops, workload.seed).await?;

    #[expect(
        clippy::cast_precision_loss,
        reason = "operation counts stay far below 2^52"
    )]
    let throughput = (observed.reads.len() + observed.writes.len()) as f64 / observed.elapsed_secs;
    Ok(TargetResult {
        target: kind.name().to_string(),
        image: target.image(),
        transport: kind.transport().to_string(),
        reads: Summary::of(&observed.reads),
        writes: Summary::of(&observed.writes),
        throughput_ops_per_s: throughput,
        read_misses: observed.read_misses,
        errors: observed.errors,
        bytes_per_entry,
        copies: target.copies(),
        error: None,
    })
}

/// Loads every key once, spread across the workers.
async fn load(target: &Target, workload: &Workload) -> anyhow::Result<()> {
    let mut tasks = tokio::task::JoinSet::new();
    for worker in 0..workload.concurrency {
        let mut client = target.client().await?;
        let (keys, value_bytes, stride) =
            (workload.keys, workload.value_bytes, workload.concurrency);
        tasks.spawn(async move {
            let mut i = worker;
            while i < keys {
                client
                    .set(workload::key(i), workload::value(i, value_bytes))
                    .await?;
                i += stride;
            }
            anyhow::Ok(())
        });
    }
    while let Some(joined) = tasks.join_next().await {
        joined.context("a load worker panicked")??;
    }
    Ok(())
}

/// Runs `ops` operations across the workers and merges what they saw.
async fn phase(
    target: &Target,
    workload: &Workload,
    zipf: &Arc<Zipf>,
    ops: usize,
    seed: u64,
) -> anyhow::Result<Observed> {
    let per_worker = ops.div_ceil(workload.concurrency.max(1));
    let mut clients = Vec::with_capacity(workload.concurrency);
    for _ in 0..workload.concurrency {
        clients.push(target.client().await?);
    }
    let phase_started = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for (worker, mut client) in clients.into_iter().enumerate() {
        let zipf = Arc::clone(zipf);
        let (read_ratio, value_bytes) = (workload.read_ratio, workload.value_bytes);
        let mut rng = SmallRng::seed_from_u64(seed.wrapping_add(worker as u64));
        tasks.spawn(async move {
            let mut reads = summary::histogram();
            let mut writes = summary::histogram();
            let (mut read_misses, mut errors) = (0u64, 0u64);
            for _ in 0..per_worker {
                let index = zipf.sample(rng.random::<f64>());
                let key = workload::key(index);
                if workload::is_read(rng.random::<f64>(), read_ratio) {
                    let started = Instant::now();
                    let outcome = client.get(&key).await;
                    summary::record(&mut reads, nanos(started));
                    match outcome {
                        Ok(true) => {}
                        Ok(false) => read_misses += 1,
                        Err(_) => errors += 1,
                    }
                } else {
                    let value = workload::value(index, value_bytes);
                    let started = Instant::now();
                    let outcome = client.set(key, value).await;
                    summary::record(&mut writes, nanos(started));
                    if outcome.is_err() {
                        errors += 1;
                    }
                }
            }
            (reads, writes, read_misses, errors)
        });
    }
    let mut observed = Observed {
        reads: summary::histogram(),
        writes: summary::histogram(),
        read_misses: 0,
        errors: 0,
        elapsed_secs: 0.0,
    };
    while let Some(joined) = tasks.join_next().await {
        let (reads, writes, read_misses, errors) = joined.context("a worker panicked")?;
        observed.reads.add(&reads).context("merge read latencies")?;
        observed
            .writes
            .add(&writes)
            .context("merge write latencies")?;
        observed.read_misses += read_misses;
        observed.errors += errors;
    }
    observed.elapsed_secs = phase_started.elapsed().as_secs_f64();
    Ok(observed)
}

fn nanos(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

/// Memory per stored copy of an entry, from the memory a target reports
/// before and after loading `keys` entries held `copies` times each.
#[must_use]
pub fn bytes_per_entry(
    before: Option<u64>,
    after: Option<u64>,
    keys: usize,
    copies: u64,
) -> Option<f64> {
    let (before, after) = (before?, after?);
    let stored = keys as u64 * copies;
    if stored == 0 || after <= before {
        return None;
    }
    #[expect(
        clippy::cast_precision_loss,
        reason = "byte counts stay far below 2^52"
    )]
    Some((after - before) as f64 / stored as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_per_entry_divides_growth_across_every_copy() {
        assert_eq!(
            bytes_per_entry(Some(1_000), Some(7_000), 10, 3),
            Some(200.0)
        );
        assert_eq!(bytes_per_entry(None, Some(7_000), 10, 3), None);
        assert_eq!(
            bytes_per_entry(Some(7_000), Some(1_000), 10, 1),
            None,
            "no growth, no figure"
        );
        assert_eq!(bytes_per_entry(Some(0), Some(10), 0, 1), None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn every_sundog_mode_runs_a_small_workload_without_errors() {
        let workload = Workload {
            keys: 500,
            value_bytes: 32,
            ops: 2_000,
            warmup_ops: 200,
            concurrency: 4,
            ..Workload::default()
        };
        for mode in [
            crate::sundog_target::SundogMode::Local,
            crate::sundog_target::SundogMode::Replicated,
            crate::sundog_target::SundogMode::Distributed,
        ] {
            let result = run(Kind::Sundog(mode), &workload)
                .await
                .expect("the run completes");
            assert_eq!(result.errors, 0, "{}", result.target);
            assert_eq!(
                result.read_misses, 0,
                "every key is loaded: {}",
                result.target
            );
            assert_eq!(
                result.reads.count + result.writes.count,
                2_000,
                "{}",
                result.target
            );
            assert!(
                result.reads.count > result.writes.count,
                "{}",
                result.target
            );
            assert!(result.throughput_ops_per_s > 0.0);
        }
    }

    /// Runs a small workload against each server in a container. Skipped
    /// unless `SUNDOG_BENCH_CONTAINERS=1`; `SUNDOG_BENCH_SERVERS=a,b` narrows
    /// it to the named servers.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn every_server_runs_a_small_workload_in_a_container() {
        if std::env::var("SUNDOG_BENCH_CONTAINERS").as_deref() != Ok("1") {
            eprintln!("skipped: set SUNDOG_BENCH_CONTAINERS=1 and RIGHTSIZE_BACKEND=docker");
            return;
        }
        let only = std::env::var("SUNDOG_BENCH_SERVERS").ok();
        let workload = Workload {
            keys: 500,
            value_bytes: 32,
            ops: 2_000,
            warmup_ops: 200,
            concurrency: 4,
            ..Workload::default()
        };
        for kind in Kind::ALL
            .into_iter()
            .filter(|k| matches!(k, Kind::Server(_)))
        {
            if only
                .as_deref()
                .is_some_and(|list| !list.split(',').any(|n| n == kind.name()))
            {
                continue;
            }
            let result = run(kind, &workload).await.expect("the run completes");
            eprintln!(
                "{}: {:?} bytes/entry {:?}",
                result.target, result.reads, result.bytes_per_entry
            );
            assert_eq!(result.errors, 0, "{}", result.target);
            assert_eq!(
                result.read_misses, 0,
                "every key is loaded: {}",
                result.target
            );
            assert_eq!(
                result.reads.count + result.writes.count,
                2_000,
                "{}",
                result.target
            );
            assert!(result.image.is_some(), "{}", result.target);
        }
    }
}

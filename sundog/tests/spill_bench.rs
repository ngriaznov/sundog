//! SSD spill tier performance baseline: real single-node, loopback
//! [`Mode::Local`] caches, public API only. Not a correctness suite; it
//! measures and prints.
//!
//! Gated on `SUNDOG_BENCH=1`, an `eprintln!` and early return otherwise, so
//! a plain `cargo test` run still compiles without the wall-clock cost. This
//! file's own binary is the sole claimant of the process-global Prometheus
//! recorder slot: [`metrics_handle`] installs it once, lazily, the first
//! time any benchmark in this binary needs it, before that benchmark opens
//! its first cache — every later benchmark in the same run then shares the
//! same handle.
//!
//! ```text
//! SUNDOG_BENCH=1 cargo test --release -p sundog --features spill,prometheus \
//!     --test spill_bench -- --test-threads=1 --nocapture
//! ```
//!
//! Each `BENCH` line is one `key=value`-per-metric record, `grep`able.

#![cfg(all(feature = "spill", feature = "prometheus"))]

mod common;

use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use sundog::{Cache, Cluster, Mode, PrometheusHandle, SpillConfig};

fn bench_enabled() -> bool {
    std::env::var("SUNDOG_BENCH").as_deref() == Ok("1")
}

/// Every value this file writes is padded to exactly this many bytes, so a
/// byte-counting weigher turns `max_capacity` into a RAM budget in bytes
/// rather than an entry count.
const VALUE_LEN: usize = 256;

/// A fixed-length, easily eyeballed value: `v0000000042-xxxx...`, padded
/// with `x` out to [`VALUE_LEN`] bytes regardless of `i`'s digit count.
fn bench_value(i: u32) -> String {
    let prefix = format!("v{i:010}-");
    let pad = VALUE_LEN.saturating_sub(prefix.len());
    let mut value = String::with_capacity(VALUE_LEN);
    value.push_str(&prefix);
    value.extend(std::iter::repeat_n('x', pad));
    value
}

/// The byte-counting weigher every benchmark cache installs: weight is the
/// value's encoded length, so `CacheBuilder::max_capacity` reads as a RAM
/// byte budget instead of an entry count. Ignores the key's own bytes,
/// which are negligible next to [`VALUE_LEN`].
#[allow(
    clippy::trivially_copy_pass_by_ref,
    clippy::ptr_arg,
    reason = "CacheBuilder::weigher requires Fn(&K, &V) -> u32; K = u32 and V = String are fixed \
              by the cache this weigher is installed on"
)]
fn byte_weigher(_key: &u32, value: &String) -> u32 {
    u32::try_from(value.len()).unwrap_or(u32::MAX)
}

/// Region size generous enough that every scenario using it never triggers
/// a region reclaim: [`spill_reclaim`] is the one benchmark that deliberately
/// undersizes this instead.
const GENEROUS_REGION_BYTES: u64 = 8 * 1024 * 1024;
/// Paired with [`GENEROUS_REGION_BYTES`]: 16 regions, comfortably more than
/// any scenario here spills.
const GENEROUS_CAPACITY_BYTES: u64 = 128 * 1024 * 1024;
/// [`SpillConfig::read_concurrency`] used wherever a benchmark does not
/// deliberately vary it.
const DEFAULT_READ_CONCURRENCY: usize = 16;

/// A directory under [`std::env::temp_dir`], unique to this process and this
/// call, never created ahead of time: `SpillTier::open` creates it. The
/// caller removes it once the benchmark is done with it.
fn fresh_temp_dir(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "sundog-spill-bench-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is after the unix epoch")
            .as_nanos()
    ))
}

/// A single-node, loopback, `Static`-discovery cluster: every benchmark's
/// cache lives on this one node, so `Mode::Local` is all that is ever
/// needed.
async fn local_cluster(name: &str) -> Cluster {
    Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(common::fast_config())
        .build()
        .await
        .expect("single-node loopback cluster builds")
}

/// Opens a `Mode::Local` cache with a spill tier: `max_capacity` bytes
/// resident, everything past that demoted to `dir` instead of discarded.
async fn open_spilling_cache(
    cluster: &Cluster,
    cache_name: &str,
    max_capacity: u64,
    dir: &Path,
    region_bytes: u64,
    capacity_bytes: u64,
    read_concurrency: usize,
) -> Cache<u32, String> {
    let cfg = SpillConfig::new(dir, capacity_bytes)
        .region_bytes(region_bytes)
        .read_concurrency(read_concurrency);
    cluster
        .cache::<u32, String>(cache_name)
        .mode(Mode::Local)
        .max_capacity(max_capacity)
        .weigher(byte_weigher)
        .spill(cfg)
        .open()
        .await
        .expect("spilling cache opens")
}

/// [`open_spilling_cache`]'s counterpart with no spill tier at all: past
/// `max_capacity`, eviction discards the coldest entries outright, the
/// baseline every spill scenario is measured against.
async fn open_plain_cache(
    cluster: &Cluster,
    cache_name: &str,
    max_capacity: u64,
) -> Cache<u32, String> {
    cluster
        .cache::<u32, String>(cache_name)
        .mode(Mode::Local)
        .max_capacity(max_capacity)
        .weigher(byte_weigher)
        .open()
        .await
        .expect("plain cache opens")
}

/// Writes `[0, entries)` through `insert_many`, `chunk` keys at a time: the
/// bulk-loader shape every benchmark here writes through.
async fn insert_chunks(cache: &Cache<u32, String>, entries: u32, chunk: u32) {
    let mut start = 0u32;
    while start < entries {
        let end = (start + chunk).min(entries);
        cache
            .insert_many((start..end).map(|i| (i, bench_value(i))))
            .await
            .expect("insert_many succeeds");
        start = end;
    }
}

/// This binary's one claim on the process-global Prometheus recorder slot,
/// installed lazily on first use and shared by every benchmark that runs
/// after it. `None` if this process somehow lost the race for the slot
/// (never expected for a single-purpose test binary, but tolerated the way
/// `tests/prometheus_exporter.rs` tolerates it): every metric read below
/// then just reads back as zero instead of panicking.
static METRICS_HANDLE: OnceLock<Option<PrometheusHandle>> = OnceLock::new();

fn metrics_handle() -> Option<&'static PrometheusHandle> {
    METRICS_HANDLE
        .get_or_init(|| sundog::prometheus_handle().ok())
        .as_ref()
}

/// Finds `metric{label1="value1",...} <number>` in Prometheus
/// text-exposition `body`, tolerant of label ordering and
/// integer-vs-float rendering. Mirrors `tests/prometheus_exporter.rs`'s own
/// `scraped_metric_value`, kept local since integration test binaries don't
/// share code beyond `mod common`.
fn scraped_metric(body: &str, metric: &str, labels: &[(&str, &str)]) -> Option<f64> {
    let wanted: Vec<String> = labels
        .iter()
        .map(|&(k, v)| format!("{k}=\"{v}\""))
        .collect();
    body.lines().find_map(|line| {
        let rest = line.strip_prefix(metric)?;
        let rest = rest.strip_prefix('{')?;
        let (line_labels, value) = rest.split_once('}')?;
        let line_labels: Vec<&str> = line_labels.split(',').collect();
        if !wanted.iter().all(|w| line_labels.contains(&w.as_str())) {
            return None;
        }
        value.trim().parse::<f64>().ok()
    })
}

/// Every `sundog_spill_*` counter and gauge this file reads is an
/// exact-integer count in practice, so reading it back through `f64` and
/// rounding to `u64` sidesteps `clippy::float_cmp` entirely: every
/// comparison and settle-poll below compares `u64`s, never `f64`s.
fn metric_count(name: &str, labels: &[(&str, &str)]) -> u64 {
    let value = metrics_handle().and_then(|h| scraped_metric(&h.render(), name, labels));
    #[allow(
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation,
        reason = "every metric read here is a nonnegative counter or gauge"
    )]
    let count = value.unwrap_or(0.0).round() as u64;
    count
}

/// Polls `sundog_spill_writes_total{cache}` until it stops changing across
/// `STABLE_ROUNDS` consecutive checks, or `timeout` elapses either way:
/// the flusher thread has caught up with everything `try_spill` queued.
/// Returns the wall time from call to settle, the "flusher drain" figure
/// each write-path benchmark reports.
async fn wait_for_flusher_drain(cache_name: &str, timeout: Duration) -> Duration {
    const STABLE_ROUNDS: u32 = 5;
    const POLL_INTERVAL: Duration = Duration::from_millis(50);

    let start = Instant::now();
    let mut last: Option<u64> = None;
    let mut stable_for = 0u32;
    loop {
        let current = metric_count("sundog_spill_writes_total", &[("cache", cache_name)]);
        if last == Some(current) {
            stable_for += 1;
            if stable_for >= STABLE_ROUNDS {
                return start.elapsed();
            }
        } else {
            stable_for = 0;
        }
        last = Some(current);
        if start.elapsed() >= timeout {
            return start.elapsed();
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// A `u64` count read back as `f64`, for a rate or ratio computation. Every
/// value this is used on, byte counts and metric readings, fits exactly in
/// `f64`'s 52-bit mantissa at this benchmark's scale.
#[allow(clippy::cast_precision_loss)]
fn count_f64(n: u64) -> f64 {
    n as f64
}

/// The `p`th percentile (0-100) of an ascending-sorted `durations`, nearest-
/// rank, reported in microseconds. `0.0` for an empty slice.
fn percentile_micros(sorted: &[Duration], p: f64) -> f64 {
    let Some(last_idx) = sorted.len().checked_sub(1) else {
        return 0.0;
    };
    #[allow(clippy::cast_precision_loss)]
    let rank = (p / 100.0) * last_idx as f64;
    #[allow(
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation,
        reason = "rank is always within [0, last_idx], both nonnegative"
    )]
    let idx = (rank.round() as usize).min(last_idx);
    sorted[idx].as_secs_f64() * 1_000_000.0
}

/// A simple xorshift64* step: cheap, seeded, and reproducible run to run.
/// Not cryptographic; a benchmark's read pattern only needs to look skewed,
/// not be unpredictable.
fn xorshift64(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

/// `2^53`, exact in `f64`: the standard "top 53 bits over `2^53`" recipe for
/// turning a `u64` into a uniform double in `[0, 1)`.
const TWO_POW_53: f64 = 9_007_199_254_740_992.0;

/// A key in `[0, n)`, biased toward the low end: draws a uniform fraction
/// from `state` and squares it before scaling by `n`, so low keys land far
/// more often than high ones. A cheap zipfian-*ish* stand-in, not an
/// attempt at a real zipfian distribution.
fn skewed_key(state: &mut u64, n: u32) -> u32 {
    let raw = xorshift64(state);
    #[allow(clippy::cast_precision_loss)]
    let uniform = (raw >> 11) as f64 / TWO_POW_53;
    let biased = uniform * uniform;
    #[allow(
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation,
        reason = "biased is in [0, 1) and n is a small positive count"
    )]
    let idx = (biased * f64::from(n)) as u32;
    idx.min(n - 1)
}

/// Runs `reads` skewed lookups (see [`skewed_key`]) against `cache`, timing
/// every call, and returns `(hit_ratio, reads_per_sec, p99_micros)`.
async fn skewed_read_bench(
    cache: &Cache<u32, String>,
    n: u32,
    reads: u32,
    seed: u64,
) -> (f64, f64, f64) {
    let mut state = seed;
    let mut durations = Vec::with_capacity(reads as usize);
    let mut hits = 0u32;
    let started = Instant::now();
    for _ in 0..reads {
        let key = skewed_key(&mut state, n);
        let t0 = Instant::now();
        let got = cache.get(&key).await;
        durations.push(t0.elapsed());
        if got.is_some() {
            hits += 1;
        }
    }
    let elapsed = started.elapsed();
    durations.sort_unstable();
    let hit_ratio = f64::from(hits) / f64::from(reads);
    let reads_per_sec = f64::from(reads) / elapsed.as_secs_f64();
    let p99 = percentile_micros(&durations, 99.0);
    (hit_ratio, reads_per_sec, p99)
}

/// Bulk-write cost with the tier on versus off: 200k `insert_many`d entries
/// against a cache whose 25%-of-total-bytes RAM budget forces most of them
/// onto disk, next to the same insert against an identically-weighed cache
/// with no spill tier at all. Reports the write path's own overhead (per-
/// insert micros, spill versus no spill) plus the flusher's own throughput
/// once the insert loop returns and the queue drains.
#[allow(
    clippy::too_many_lines,
    reason = "one self-contained scenario: two cache setups, one insert loop each, one report"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spill_write_path() {
    const ENTRIES: u32 = 200_000;
    const CHUNK: u32 = 10_000;
    const RAM_NUMERATOR: u64 = 1;
    const RAM_DENOMINATOR: u64 = 4; // 25%

    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }

    let total_bytes = u64::from(ENTRIES) * VALUE_LEN as u64;
    let max_capacity = total_bytes * RAM_NUMERATOR / RAM_DENOMINATOR;

    let cluster = local_cluster("bench-spill-write-path").await;
    let dir = fresh_temp_dir("write-path");

    let spill_cache = open_spilling_cache(
        &cluster,
        "write-spill",
        max_capacity,
        &dir,
        GENEROUS_REGION_BYTES,
        GENEROUS_CAPACITY_BYTES,
        DEFAULT_READ_CONCURRENCY,
    )
    .await;

    let started = Instant::now();
    insert_chunks(&spill_cache, ENTRIES, CHUNK).await;
    let insert_elapsed = started.elapsed();
    let per_insert_micros_spill = insert_elapsed.as_secs_f64() * 1_000_000.0 / f64::from(ENTRIES);

    let drain_elapsed = wait_for_flusher_drain("write-spill", Duration::from_secs(120)).await;

    let spilled_entries = metric_count("sundog_spill_entries", &[("cache", "write-spill")]);
    let bytes_written = metric_count("sundog_spill_bytes_used", &[("cache", "write-spill")]);
    let dropped_queue_full = metric_count(
        "sundog_spill_dropped_total",
        &[("cache", "write-spill"), ("reason", "queue_full")],
    );
    let io_errors = metric_count(
        "sundog_spill_reads_total",
        &[("cache", "write-spill"), ("outcome", "io_error")],
    );
    let drain_secs = drain_elapsed.as_secs_f64().max(0.001);
    let mb_per_sec = count_f64(bytes_written) / 1_000_000.0 / drain_secs;

    let plain_cache = open_plain_cache(&cluster, "write-nospill", max_capacity).await;
    let started = Instant::now();
    insert_chunks(&plain_cache, ENTRIES, CHUNK).await;
    let plain_elapsed = started.elapsed();
    let per_insert_micros_nospill = plain_elapsed.as_secs_f64() * 1_000_000.0 / f64::from(ENTRIES);

    println!(
        "BENCH spill_write_path entries={ENTRIES} max_capacity_bytes={max_capacity} \
         insert_secs_spill={:.3} per_insert_micros_spill={per_insert_micros_spill:.2} \
         insert_secs_nospill={:.3} per_insert_micros_nospill={per_insert_micros_nospill:.2} \
         drain_secs={:.3} spilled_entries={spilled_entries} bytes_written={bytes_written} \
         mb_per_sec={mb_per_sec:.2} dropped_queue_full={dropped_queue_full}",
        insert_elapsed.as_secs_f64(),
        plain_elapsed.as_secs_f64(),
        drain_elapsed.as_secs_f64(),
    );

    assert!(
        spilled_entries > 0,
        "a 25%-of-total RAM budget must force some spilling"
    );
    assert_eq!(io_errors, 0, "a pure write workload never reads off disk");

    cluster.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// RAM hit versus tier hit, side by side: once a 200k-entry, 25%-RAM spill
/// population settles, one `get` per key for 20k distinct spilled keys
/// (each promotes) and 20k distinct resident keys, timed individually.
/// Also times `get_sync` on spilled keys, a miss by contract since the
/// synchronous path never touches disk.
#[allow(
    clippy::too_many_lines,
    reason = "one self-contained scenario: fill, three read passes, one report"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spill_read_latency() {
    const ENTRIES: u32 = 200_000;
    const CHUNK: u32 = 10_000;
    const SAMPLE: u32 = 20_000;
    const SYNC_MISS_SAMPLE: u32 = 5_000;

    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }

    let total_bytes = u64::from(ENTRIES) * VALUE_LEN as u64;
    let max_capacity = total_bytes / 4; // 25%

    let cluster = local_cluster("bench-spill-read-latency").await;
    let dir = fresh_temp_dir("read-latency");
    let cache = open_spilling_cache(
        &cluster,
        "read-latency",
        max_capacity,
        &dir,
        GENEROUS_REGION_BYTES,
        GENEROUS_CAPACITY_BYTES,
        DEFAULT_READ_CONCURRENCY,
    )
    .await;

    insert_chunks(&cache, ENTRIES, CHUNK).await;
    wait_for_flusher_drain("read-latency", Duration::from_secs(120)).await;

    // `get_sync` never disk-reads, so it tells resident from not-resident
    // with zero I/O; `contains_key_sync` then tells a currently-spilled key
    // (still live, just not in RAM) from one queue_full/too_large dropped
    // outright during eviction, which `get_sync` alone cannot distinguish.
    let mut resident_keys = Vec::with_capacity(SAMPLE as usize);
    let mut spilled_keys = Vec::with_capacity((SAMPLE + SYNC_MISS_SAMPLE) as usize);
    for k in 0..ENTRIES {
        if resident_keys.len() >= SAMPLE as usize
            && spilled_keys.len() >= (SAMPLE + SYNC_MISS_SAMPLE) as usize
        {
            break;
        }
        if cache.get_sync(&k).is_some() {
            if resident_keys.len() < SAMPLE as usize {
                resident_keys.push(k);
            }
        } else if cache.contains_key_sync(&k)
            && spilled_keys.len() < (SAMPLE + SYNC_MISS_SAMPLE) as usize
        {
            spilled_keys.push(k);
        }
    }
    assert!(
        resident_keys.len() == SAMPLE as usize,
        "the 25% RAM budget must leave at least {SAMPLE} resident keys"
    );
    assert!(
        spilled_keys.len() == (SAMPLE + SYNC_MISS_SAMPLE) as usize,
        "the 75% overflow must spill at least {} keys",
        SAMPLE + SYNC_MISS_SAMPLE
    );

    // get_sync on a currently-spilled key: a miss by contract, timed before
    // any of these particular keys are touched through `get`.
    let mut sync_miss_durations = Vec::with_capacity(SYNC_MISS_SAMPLE as usize);
    for &k in &spilled_keys[SAMPLE as usize..] {
        let t0 = Instant::now();
        let got = cache.get_sync(&k);
        sync_miss_durations.push(t0.elapsed());
        assert!(got.is_none(), "get_sync never reads a spilled key's bytes");
    }
    let sync_miss_avg_nanos = sync_miss_durations
        .iter()
        .map(Duration::as_secs_f64)
        .sum::<f64>()
        * 1_000_000_000.0
        / f64::from(SYNC_MISS_SAMPLE);

    // `get` on a spilled key: a tier hit, and it promotes the entry.
    let mut spill_durations = Vec::with_capacity(SAMPLE as usize);
    let spill_started = Instant::now();
    for &k in &spilled_keys[..SAMPLE as usize] {
        let t0 = Instant::now();
        let got = cache.get(&k).await;
        spill_durations.push(t0.elapsed());
        assert!(
            got.is_some(),
            "a currently-spilled key is a tier hit through get"
        );
    }
    let spill_elapsed = spill_started.elapsed();
    spill_durations.sort_unstable();

    // `get` on a resident key: a plain RAM hit.
    let mut resident_durations = Vec::with_capacity(SAMPLE as usize);
    let resident_started = Instant::now();
    for &k in &resident_keys {
        let t0 = Instant::now();
        let got = cache.get(&k).await;
        resident_durations.push(t0.elapsed());
        assert!(got.is_some(), "a resident key is a RAM hit through get");
    }
    let resident_elapsed = resident_started.elapsed();
    resident_durations.sort_unstable();

    let spill_p50 = percentile_micros(&spill_durations, 50.0);
    let spill_p99 = percentile_micros(&spill_durations, 99.0);
    let spill_max = spill_durations
        .last()
        .map_or(0.0, |d| d.as_secs_f64() * 1_000_000.0);
    let spill_reads_per_sec = f64::from(SAMPLE) / spill_elapsed.as_secs_f64();

    let resident_p50 = percentile_micros(&resident_durations, 50.0);
    let resident_p99 = percentile_micros(&resident_durations, 99.0);
    let resident_max = resident_durations
        .last()
        .map_or(0.0, |d| d.as_secs_f64() * 1_000_000.0);
    let resident_reads_per_sec = f64::from(SAMPLE) / resident_elapsed.as_secs_f64();

    println!(
        "BENCH spill_read_latency entries={ENTRIES} sample={SAMPLE} \
         tier_p50_micros={spill_p50:.2} tier_p99_micros={spill_p99:.2} tier_max_micros={spill_max:.2} \
         tier_reads_per_sec={spill_reads_per_sec:.0} \
         ram_p50_micros={resident_p50:.2} ram_p99_micros={resident_p99:.2} ram_max_micros={resident_max:.2} \
         ram_reads_per_sec={resident_reads_per_sec:.0} \
         get_sync_miss_avg_nanos={sync_miss_avg_nanos:.0}"
    );

    assert_eq!(
        metric_count(
            "sundog_spill_reads_total",
            &[("cache", "read-latency"), ("outcome", "io_error")]
        ),
        0,
        "no disk read should fail on a freshly written tier"
    );

    cluster.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// Aggregate tier throughput under concurrent, non-overlapping readers: 64
/// tasks each promoting a disjoint slice of spilled keys through `get`, run
/// once at `read_concurrency(4)` and once at `read_concurrency(32)`.
async fn concurrency_read_bench(concurrency: usize) -> (f64, f64) {
    const ENTRIES: u32 = 100_000;
    const CHUNK: u32 = 10_000;
    const RAM_FRACTION_PERCENT: u64 = 10;
    const TASKS: u32 = 64;
    const PER_TASK: u32 = 500;

    let total_bytes = u64::from(ENTRIES) * VALUE_LEN as u64;
    let max_capacity = total_bytes * RAM_FRACTION_PERCENT / 100;

    let cluster = local_cluster(&format!("bench-spill-concurrency-{concurrency}")).await;
    let dir = fresh_temp_dir(&format!("concurrency-{concurrency}"));
    let cache = open_spilling_cache(
        &cluster,
        "concurrent-reads",
        max_capacity,
        &dir,
        GENEROUS_REGION_BYTES,
        GENEROUS_CAPACITY_BYTES,
        concurrency,
    )
    .await;

    insert_chunks(&cache, ENTRIES, CHUNK).await;
    wait_for_flusher_drain("concurrent-reads", Duration::from_secs(120)).await;

    let needed = (TASKS * PER_TASK) as usize;
    let mut spilled_keys = Vec::with_capacity(needed);
    for k in 0..ENTRIES {
        // `contains_key_sync` rules out a key dropped outright (queue_full
        // or too_large) during eviction, which a bare `get_sync` miss alone
        // cannot distinguish from a currently-spilled, still-live one.
        if cache.get_sync(&k).is_none() && cache.contains_key_sync(&k) {
            spilled_keys.push(k);
            if spilled_keys.len() == needed {
                break;
            }
        }
    }
    assert_eq!(
        spilled_keys.len(),
        needed,
        "a 10%-RAM, 100k-entry fill must spill at least {needed} keys"
    );

    let started = Instant::now();
    let mut handles = Vec::with_capacity(TASKS as usize);
    for task in 0..TASKS {
        let slice: Vec<u32> =
            spilled_keys[(task * PER_TASK) as usize..((task + 1) * PER_TASK) as usize].to_vec();
        let cache = cache.clone();
        handles.push(tokio::spawn(async move {
            let mut durations = Vec::with_capacity(slice.len());
            for k in slice {
                let t0 = Instant::now();
                let got = cache.get(&k).await;
                durations.push(t0.elapsed());
                assert!(
                    got.is_some(),
                    "every slice key is a spilled, currently-present key"
                );
            }
            durations
        }));
    }
    let mut all_durations = Vec::with_capacity(needed);
    for handle in handles {
        all_durations.extend(handle.await.expect("reader task did not panic"));
    }
    let elapsed = started.elapsed();
    all_durations.sort_unstable();

    let reads_per_sec = f64::from(TASKS * PER_TASK) / elapsed.as_secs_f64();
    let p99 = percentile_micros(&all_durations, 99.0);

    cluster.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);

    (reads_per_sec, p99)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn spill_read_no_promotion_concurrency() {
    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }

    let (reads_per_sec_c4, p99_c4) = concurrency_read_bench(4).await;
    let (reads_per_sec_c32, p99_c32) = concurrency_read_bench(32).await;

    println!(
        "BENCH spill_read_no_promotion_concurrency tasks=64 per_task=500 \
         concurrency4_reads_per_sec={reads_per_sec_c4:.0} concurrency4_p99_micros={p99_c4:.2} \
         concurrency32_reads_per_sec={reads_per_sec_c32:.0} concurrency32_p99_micros={p99_c32:.2}"
    );
}

/// A tier undersized on purpose: 4 regions of 4 MiB while the workload
/// spills tens of megabytes, forcing repeated region reclaim. Reports how
/// many reclaims happened, how many entries the live count lost to them,
/// and the latency of a `get` on a key known to have been reclaimed, a
/// miss with no disk touched at all since the key is no longer in `live`.
#[allow(
    clippy::too_many_lines,
    reason = "one self-contained scenario: fill, settle, one reclaimed-key read pass, one report"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spill_reclaim() {
    const ENTRIES: u32 = 200_000;
    const CHUNK: u32 = 10_000;
    const REGION_BYTES: u64 = 4 * 1024 * 1024;
    const REGIONS: u64 = 4;
    const CAPACITY_BYTES: u64 = REGION_BYTES * REGIONS;
    // A tiny RAM budget: almost every insert evicts something to disk,
    // so the tier's total spilled volume comfortably exceeds its capacity.
    const MAX_CAPACITY: u64 = 4096;
    const RECLAIMED_SAMPLE: u32 = 1_000;

    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }

    let cluster = local_cluster("bench-spill-reclaim").await;
    let dir = fresh_temp_dir("reclaim");
    let cache = open_spilling_cache(
        &cluster,
        "reclaim",
        MAX_CAPACITY,
        &dir,
        REGION_BYTES,
        CAPACITY_BYTES,
        DEFAULT_READ_CONCURRENCY,
    )
    .await;

    insert_chunks(&cache, ENTRIES, CHUNK).await;
    wait_for_flusher_drain("reclaim", Duration::from_secs(120)).await;

    let region_reclaims = metric_count(
        "sundog_spill_region_reclaims_total",
        &[("cache", "reclaim")],
    );
    let dropped_queue_full = metric_count(
        "sundog_spill_dropped_total",
        &[("cache", "reclaim"), ("reason", "queue_full")],
    );
    let live_entries = cache.entry_count().await;
    let entries_lost = u64::from(ENTRIES).saturating_sub(live_entries);

    assert!(
        region_reclaims > 0,
        "spilling far more than {CAPACITY_BYTES} bytes into a {REGIONS}-region tier must reclaim \
         at least one region"
    );

    // The earliest-written keys are the ones the FIFO ring reclaims first;
    // by the time all 200k entries have settled, they are gone entirely,
    // not merely spilled, so `get` misses without ever touching disk.
    let mut reclaimed_durations = Vec::with_capacity(RECLAIMED_SAMPLE as usize);
    for k in 0..RECLAIMED_SAMPLE {
        let t0 = Instant::now();
        let got = cache.get(&k).await;
        reclaimed_durations.push(t0.elapsed());
        assert!(
            got.is_none(),
            "an early key must have been reclaimed off the FIFO ring by now"
        );
    }
    reclaimed_durations.sort_unstable();
    let reclaimed_avg_nanos = reclaimed_durations
        .iter()
        .map(Duration::as_secs_f64)
        .sum::<f64>()
        * 1_000_000_000.0
        / f64::from(RECLAIMED_SAMPLE);
    let reclaimed_p99_nanos = percentile_micros(&reclaimed_durations, 99.0) * 1_000.0;

    println!(
        "BENCH spill_reclaim entries={ENTRIES} capacity_bytes={CAPACITY_BYTES} \
         region_bytes={REGION_BYTES} regions={REGIONS} region_reclaims={region_reclaims} \
         live_entries={live_entries} entries_lost_to_reclaim={entries_lost} \
         dropped_queue_full={dropped_queue_full} \
         reclaimed_get_avg_nanos={reclaimed_avg_nanos:.0} \
         reclaimed_get_p99_nanos={reclaimed_p99_nanos:.0}"
    );

    cluster.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// The case for spilling instead of discarding: a 100k-entry working set
/// against a 40%-of-total RAM budget, read 500k times with a skew biased
/// toward low keys (see [`skewed_key`]) — the same keys eviction-without-
/// spill drops earliest, since they were also the first written. With a
/// spill tier every one of those reads is still reachable from disk; with
/// plain eviction they are gone for good.
#[allow(
    clippy::too_many_lines,
    reason = "one self-contained scenario: two cache setups, one skewed read pass each, one report"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spill_vs_eviction_hit_ratio() {
    const ENTRIES: u32 = 100_000;
    const CHUNK: u32 = 10_000;
    const READS: u32 = 500_000;
    const RAM_FRACTION_PERCENT: u64 = 40;
    const SEED: u64 = 0x5EED_5EED;

    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }

    let total_bytes = u64::from(ENTRIES) * VALUE_LEN as u64;
    let max_capacity = total_bytes * RAM_FRACTION_PERCENT / 100;

    let cluster_spill = local_cluster("bench-hit-ratio-spill").await;
    let dir = fresh_temp_dir("hit-ratio");
    let cache_spill = open_spilling_cache(
        &cluster_spill,
        "hit-ratio-spill",
        max_capacity,
        &dir,
        GENEROUS_REGION_BYTES,
        GENEROUS_CAPACITY_BYTES,
        DEFAULT_READ_CONCURRENCY,
    )
    .await;
    insert_chunks(&cache_spill, ENTRIES, CHUNK).await;
    wait_for_flusher_drain("hit-ratio-spill", Duration::from_secs(120)).await;

    let (hit_ratio_spill, reads_per_sec_spill, p99_spill) =
        skewed_read_bench(&cache_spill, ENTRIES, READS, SEED).await;

    cluster_spill.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);

    let cluster_plain = local_cluster("bench-hit-ratio-nospill").await;
    let cache_plain = open_plain_cache(&cluster_plain, "hit-ratio-nospill", max_capacity).await;
    insert_chunks(&cache_plain, ENTRIES, CHUNK).await;

    let (hit_ratio_nospill, reads_per_sec_nospill, p99_nospill) =
        skewed_read_bench(&cache_plain, ENTRIES, READS, SEED).await;

    cluster_plain.shutdown().await;

    println!(
        "BENCH spill_vs_eviction_hit_ratio entries={ENTRIES} reads={READS} \
         max_capacity_bytes={max_capacity} \
         hit_ratio_spill={hit_ratio_spill:.4} reads_per_sec_spill={reads_per_sec_spill:.0} \
         p99_micros_spill={p99_spill:.2} \
         hit_ratio_nospill={hit_ratio_nospill:.4} reads_per_sec_nospill={reads_per_sec_nospill:.0} \
         p99_micros_nospill={p99_nospill:.2}"
    );

    assert!(
        hit_ratio_spill > hit_ratio_nospill,
        "a spill tier sized to hold the whole overflow must retain more of the working set than \
         plain eviction: spill={hit_ratio_spill:.4} nospill={hit_ratio_nospill:.4}"
    );
}

//! Entry diet acceptance bench: real, single-node, loopback [`Mode::Local`]
//! caches, public API only. Not a correctness suite; it measures, prints,
//! and asserts the numbers `Live<K, V>`'s record diet
//! (`sundog/src/store/engine.rs`) is scored against.
//!
//! Gated on `SUNDOG_BENCH=1`; a plain `cargo test` run still compiles
//! without the wall-clock cost:
//!
//! ```text
//! SUNDOG_BENCH=1 cargo test --release -p sundog --test entry_diet_bench \
//!     -- --test-threads=1 --nocapture
//! ```
//!
//! Each `BENCH` line is one `key=value`-per-metric record, `grep`able.

mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use sundog::{Cluster, Mode};

/// Counts bytes currently allocated (live heap), unlike resident set
/// which also carries retention after a free; [`entry_diet_rss_budget`]
/// prints both.
struct CountingAlloc;

static LIVE_HEAP_BYTES: AtomicUsize = AtomicUsize::new(0);

// SAFETY: every call forwards to `System` unchanged, only counting bytes.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwards the caller's contract for `alloc` as is.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            LIVE_HEAP_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE_HEAP_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        // SAFETY: forwards the caller's contract for `dealloc` as is.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: forwards the caller's contract for `realloc` as is.
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            LIVE_HEAP_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
            LIVE_HEAP_BYTES.fetch_add(new_size, Ordering::Relaxed);
        }
        new_ptr
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

/// Bytes the process holds allocated, per [`CountingAlloc`].
fn live_heap_bytes() -> u64 {
    u64::try_from(LIVE_HEAP_BYTES.load(Ordering::Relaxed)).unwrap_or(u64::MAX)
}

fn bench_enabled() -> bool {
    std::env::var("SUNDOG_BENCH").as_deref() == Ok("1")
}

/// A single-node, loopback cluster every benchmark's cache lives on.
async fn local_cluster(name: &str) -> Cluster {
    Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(common::fast_config())
        .build()
        .await
        .expect("single-node loopback cluster builds")
}

/// The `p`th percentile (0-100) of ascending-sorted `durations`,
/// nearest-rank, in microseconds. Mirrors `spill_bench.rs::percentile_micros`.
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

/// A simple xorshift64* step: cheap, seeded, reproducible run to run.
fn xorshift64(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

/// The bench profile's 7-byte key: `k` plus 6 zero-padded digits.
fn profile_key(i: u32) -> String {
    format!("k{i:06}")
}

/// The bench profile's 8-byte value: `v` plus 7 zero-padded digits.
fn profile_value(i: u32) -> String {
    format!("v{i:07}")
}

/// A 7-byte key, hex not decimal: decimal would need an 8th digit past
/// 9,999,999 and silently widen mid-run at [`ENTRIES_64M`].
fn rss_key(i: u32) -> String {
    format!("{i:07x}")
}

/// [`rss_key`]'s value counterpart: 8 ASCII bytes, stays wide enough decimal.
fn rss_value(i: u32) -> String {
    format!("{i:08}")
}

/// [`entry_diet_rss_budget_heap_shape`]'s key, 16 ASCII bytes. Paired with
/// [`heap_value`], the record is past `Record::INLINE_CAP`, so every
/// entry takes one heap allocation, matching Redis's heap-shape layout.
fn heap_key(i: u32) -> String {
    format!("{i:016}")
}

/// [`heap_key`]'s value counterpart: 100 ASCII bytes.
fn heap_value(i: u32) -> String {
    format!("{i:0100}")
}

/// `get_sync` p50 measured before the record diet landed (commit
/// `12b56ee`), at this bench's exact profile: average of four runs
/// (0.519, 0.584, 0.597, 0.536), rounded to two significant figures.
const PRE_DIET_P50_MICROS: f64 = 0.56;

/// Read p50 stays within this fraction of [`PRE_DIET_P50_MICROS`]: a
/// decode on every read costs something, but not an order of magnitude.
const P50_TOLERANCE: f64 = 0.10;

/// Read latency: 1,000,000 xorshift-skewed `get_sync` calls against a
/// warm 200,000-entry cache, asserting p50 stays within [`P50_TOLERANCE`]
/// of [`PRE_DIET_P50_MICROS`].
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn entry_diet_read_p50() {
    const ENTRIES: u32 = 200_000;
    const READS: u32 = 1_000_000;
    const SEED: u64 = 0x5EED_5EED;

    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }

    let cluster = local_cluster("bench-entry-diet-read-p50").await;
    let cache = cluster
        .cache::<String, String>("entry-diet-read-p50")
        .mode(Mode::Local)
        .open()
        .await
        .expect("cache opens");

    for i in 0..ENTRIES {
        cache
            .insert(profile_key(i), profile_value(i))
            .await
            .expect("insert succeeds");
    }

    let mut state = SEED;
    let mut durations = Vec::with_capacity(READS as usize);
    let mut hits = 0u32;
    let started = Instant::now();
    for _ in 0..READS {
        #[allow(
            clippy::cast_possible_truncation,
            reason = "xorshift64(..) % u64::from(ENTRIES) is always < ENTRIES, which fits u32"
        )]
        let i = (xorshift64(&mut state) % u64::from(ENTRIES)) as u32;
        let key = profile_key(i);
        let t0 = Instant::now();
        let got = cache.get_sync(&key);
        durations.push(t0.elapsed());
        if got.is_some() {
            hits += 1;
        }
    }
    let elapsed = started.elapsed();
    assert_eq!(hits, READS, "every read targets a present key");

    durations.sort_unstable();
    let p50 = percentile_micros(&durations, 50.0);
    let p99 = percentile_micros(&durations, 99.0);
    let reads_per_sec = f64::from(READS) / elapsed.as_secs_f64();

    println!(
        "BENCH entry_diet_read_p50 entries={ENTRIES} reads={READS} p50_micros={p50:.4} \
         p99_micros={p99:.4} reads_per_sec={reads_per_sec:.0} pre_diet_p50_micros={PRE_DIET_P50_MICROS}"
    );

    let max_allowed = PRE_DIET_P50_MICROS * (1.0 + P50_TOLERANCE);
    assert!(
        p50 <= max_allowed,
        "decode-on-read p50 {p50:.4}us exceeds {max_allowed:.4}us, {P50_TOLERANCE:.0}% over the \
         pre-diet baseline of {PRE_DIET_P50_MICROS}us"
    );

    cluster.shutdown().await;
}

/// This process's resident set size from `/proc/self/status`'s `VmRSS`
/// line, in bytes. `None` if that file is unavailable.
fn vm_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find_map(|line| {
        let rest = line.strip_prefix("VmRSS:")?;
        let kib: u64 = rest.trim().trim_end_matches(" kB").trim().parse().ok()?;
        Some(kib * 1024)
    })
}

/// `/proc/meminfo`'s `MemAvailable` line, in bytes: what a new allocation
/// can claim without swapping, unlike `MemFree` alone. `None` if
/// unavailable.
fn mem_available_bytes() -> Option<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    meminfo.lines().find_map(|line| {
        let rest = line.strip_prefix("MemAvailable:")?;
        let kib: u64 = rest.trim().trim_end_matches(" kB").trim().parse().ok()?;
        Some(kib * 1024)
    })
}

/// `MemAvailable` floor [`entry_diet_rss_budget_64m`] requires, with
/// headroom above its two ~4 GiB runs' transient peaks.
const MIN_MEM_AVAILABLE_FOR_64M_BYTES: u64 = 10 * 1024 * 1024 * 1024;

/// Whether `mem_available_bytes` meets the floor; unreadable is insufficient.
fn has_enough_ram_for_64m(mem_available_bytes: Option<u64>) -> bool {
    mem_available_bytes.is_some_and(|bytes| bytes >= MIN_MEM_AVAILABLE_FOR_64M_BYTES)
}

/// Polls [`vm_rss_bytes`] until it stops growing across `STABLE_ROUNDS`
/// checks, or `timeout` elapses, so background settling finishes first.
async fn settled_vm_rss_bytes(timeout: Duration) -> Option<u64> {
    const STABLE_ROUNDS: u32 = 5;
    const POLL_INTERVAL: Duration = Duration::from_millis(200);

    let start = Instant::now();
    let mut last: Option<u64> = None;
    let mut stable_for = 0u32;
    loop {
        let current = vm_rss_bytes()?;
        if last.is_some_and(|l| current <= l) {
            stable_for += 1;
            if stable_for >= STABLE_ROUNDS {
                return Some(current);
            }
        } else {
            stable_for = 0;
        }
        last = Some(current);
        if start.elapsed() >= timeout {
            return Some(current);
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

// `malloc_trim`/`mallopt` are glibc's API for releasing freed-but-unreturned
// memory and tuning when it does so.
#[cfg(target_os = "linux")]
unsafe extern "C" {
    fn malloc_trim(pad: usize) -> i32;
    fn mallopt(param: i32, value: i32) -> i32;
}

/// glibc's `mallopt` parameter for the mmap threshold.
#[cfg(target_os = "linux")]
const M_MMAP_THRESHOLD: i32 = -3;

/// Below this, glibc's ordinary arena stays in charge.
#[cfg(target_os = "linux")]
const MMAP_THRESHOLD_BYTES: i32 = 64 * 1024;

/// Pins glibc's mmap threshold and releases freed-but-unreturned memory,
/// so a later [`measure_rss_run`] doesn't inherit an earlier scenario's
/// unreturned arena chunks. No-op off Linux.
fn stabilize_allocator_for_next_scenario() {
    #[cfg(target_os = "linux")]
    // SAFETY: safe to call any time per glibc's contract; tunes policy
    // and frees only memory already known unused.
    unsafe {
        mallopt(M_MMAP_THRESHOLD, MMAP_THRESHOLD_BYTES);
        malloc_trim(0);
    }
}

/// One [`measure_rss_run`] result: entry count, timing, and RSS/live-heap
/// byte counts before and after.
struct RssRun {
    entries: u32,
    entry_count: u64,
    insert_secs: f64,
    baseline_rss_bytes: Option<u64>,
    rss_bytes: Option<u64>,
    baseline_live_bytes: u64,
    live_bytes: u64,
}

impl RssRun {
    #[allow(clippy::cast_precision_loss, reason = "reporting only, not compared")]
    fn rss_gib(&self) -> f64 {
        self.rss_bytes
            .map_or(0.0, |b| b as f64 / (1024.0 * 1024.0 * 1024.0))
    }

    #[allow(clippy::cast_precision_loss, reason = "reporting only, not compared")]
    fn bytes_per_entry(&self) -> f64 {
        self.rss_bytes
            .map_or(0.0, |b| b as f64 / f64::from(self.entries))
    }

    #[allow(clippy::cast_precision_loss, reason = "reporting only, not compared")]
    fn marginal_bytes_per_entry(&self) -> f64 {
        match (self.rss_bytes, self.baseline_rss_bytes) {
            (Some(rss), Some(baseline)) => {
                rss.saturating_sub(baseline) as f64 / f64::from(self.entries)
            }
            _ => 0.0,
        }
    }

    #[allow(clippy::cast_precision_loss, reason = "reporting only, not compared")]
    fn live_heap_bytes_per_entry(&self) -> f64 {
        self.live_bytes.saturating_sub(self.baseline_live_bytes) as f64 / f64::from(self.entries)
    }

    /// Prints this run's `BENCH` line, asserting every insert landed and,
    /// when `assert_budget` and RSS is readable, that settled RSS stays
    /// at or under `budget_bytes`.
    fn report(&self, label: &str, budget_bytes: u64, assert_budget: bool) {
        println!(
            "BENCH {label} entries={} entry_count={} insert_secs={:.3} baseline_rss_bytes={} \
             rss_bytes={} rss_gib={:.3} bytes_per_entry={:.1} \
             marginal_bytes_per_entry={:.1} live_heap_bytes_per_entry={:.1} \
             budget_bytes={budget_bytes}",
            self.entries,
            self.entry_count,
            self.insert_secs,
            self.baseline_rss_bytes
                .map_or("unavailable".to_string(), |b| b.to_string()),
            self.rss_bytes
                .map_or("unavailable".to_string(), |b| b.to_string()),
            self.rss_gib(),
            self.bytes_per_entry(),
            self.marginal_bytes_per_entry(),
            self.live_heap_bytes_per_entry(),
        );
        assert_eq!(
            self.entry_count,
            u64::from(self.entries),
            "{label}: every insert lands"
        );
        if !assert_budget {
            return;
        }
        if let Some(rss) = self.rss_bytes {
            assert!(
                rss <= budget_bytes,
                "{label}: steady-state RSS {rss} bytes ({:.3} GiB) exceeds the {budget_bytes}-byte \
                 budget for {} entries",
                self.rss_gib(),
                self.entries
            );
        } else {
            eprintln!(
                "{label}: skipping the RSS assertion: /proc/self/status is unavailable on this \
                 platform"
            );
        }
    }
}

/// Inserts `entries` `key_fn`/`value_fn` pairs into a fresh,
/// spill-disabled cache (with `capacity_hint` when given) and returns the
/// settled [`RssRun`]. `cluster_name`/`cache_name` must be unique per call.
async fn measure_rss_run(
    cluster_name: &str,
    cache_name: &str,
    entries: u32,
    capacity_hint: Option<u64>,
    key_fn: impl Fn(u32) -> String,
    value_fn: impl Fn(u32) -> String,
) -> RssRun {
    const CHUNK: u32 = 50_000;

    stabilize_allocator_for_next_scenario();
    let cluster = local_cluster(cluster_name).await;
    let mut builder = cluster
        .cache::<String, String>(cache_name)
        .mode(Mode::Local);
    if let Some(hint) = capacity_hint {
        builder = builder.capacity_hint(hint);
    }
    let cache = builder.open().await.expect("cache opens");
    let baseline_rss_bytes = settled_vm_rss_bytes(Duration::from_secs(10)).await;
    let baseline_live_bytes = live_heap_bytes();

    let started = Instant::now();
    let mut start = 0u32;
    while start < entries {
        let end = (start + CHUNK).min(entries);
        cache
            .insert_many((start..end).map(|i| (key_fn(i), value_fn(i))))
            .await
            .expect("insert_many succeeds");
        start = end;
    }
    let insert_secs = started.elapsed().as_secs_f64();

    let rss_bytes = settled_vm_rss_bytes(Duration::from_secs(60)).await;
    let live_bytes = live_heap_bytes();
    let entry_count = cache.entry_count().await;

    cluster.shutdown().await;

    RssRun {
        entries,
        entry_count,
        insert_secs,
        baseline_rss_bytes,
        rss_bytes,
        baseline_live_bytes,
        live_bytes,
    }
}

/// Measured settled RSS at 4,000,000 entries, unhinted: about 0.278 GiB,
/// 74.7 bytes/entry. This budget sits about 13% above that, so a
/// regression of a few bytes/entry fails the bench while run-to-run noise
/// does not.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "0.315 GiB is a small positive constant, exact in f64 up to a rounding sub-byte"
)]
const RSS_BUDGET_BYTES: u64 = (0.315 * 1024.0 * 1024.0 * 1024.0) as u64;

/// [`RSS_BUDGET_BYTES`]'s counterpart with `capacity_hint` set to the
/// exact entry count: measures a little higher, about 77.3 bytes/entry
/// (0.288 GiB), since a stripe whose real share lands over its reserved
/// capacity still pays a full doubling from that base rather than zero.
/// Set with the same ~13% headroom.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "a small positive GiB constant, exact in f64 up to a rounding sub-byte"
)]
const RSS_BUDGET_BYTES_HINTED: u64 = (0.325 * 1024.0 * 1024.0 * 1024.0) as u64;

/// The entry count [`entry_diet_rss_budget`]'s budgets are pinned to.
const RSS_BUDGET_ENTRIES: u32 = 4_000_000;

/// The entry count [`entry_diet_rss_budget`] inserts:
/// [`RSS_BUDGET_ENTRIES`], or `SUNDOG_BENCH_ENTRIES` for a density
/// measurement at another size. Budget assertions only run at
/// [`RSS_BUDGET_ENTRIES`].
fn rss_bench_entries() -> u32 {
    std::env::var("SUNDOG_BENCH_ENTRIES")
        .ok()
        .and_then(|raw| raw.parse::<u32>().ok())
        .filter(|&entries| entries > 0)
        .unwrap_or(RSS_BUDGET_ENTRIES)
}

/// RSS budget: [`rss_bench_entries`] entries inserted twice, unhinted
/// and hinted at the exact count, asserting each settled RSS stays under
/// its own budget ([`RSS_BUDGET_BYTES`]/[`RSS_BUDGET_BYTES_HINTED`]) when
/// run at [`RSS_BUDGET_ENTRIES`].
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn entry_diet_rss_budget() {
    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }
    let entries = rss_bench_entries();
    let assert_budget = entries == RSS_BUDGET_ENTRIES;

    let unhinted = measure_rss_run(
        "bench-entry-diet-rss-unhinted",
        "entry-diet-rss-unhinted",
        entries,
        None,
        rss_key,
        rss_value,
    )
    .await;
    unhinted.report(
        "entry_diet_rss_budget_unhinted",
        RSS_BUDGET_BYTES,
        assert_budget,
    );

    let hinted = measure_rss_run(
        "bench-entry-diet-rss-hinted",
        "entry-diet-rss-hinted",
        entries,
        Some(u64::from(entries)),
        rss_key,
        rss_value,
    )
    .await;
    hinted.report(
        "entry_diet_rss_budget_hinted",
        RSS_BUDGET_BYTES_HINTED,
        assert_budget,
    );
}

/// 16x [`RSS_BUDGET_ENTRIES`], confirming bytes-per-entry holds at scale.
const ENTRIES_64M: u32 = 64_000_000;

/// [`RSS_BUDGET_BYTES`]'s counterpart at [`ENTRIES_64M`], unhinted:
/// measures ~67.4 bytes/entry, lower than the 4M run's 74.7 since fixed
/// per-stripe overhead amortizes over more entries. Set with ~13%
/// headroom over the measured number.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "a small positive GiB constant, exact in f64 up to a rounding sub-byte"
)]
const RSS_BUDGET_BYTES_64M: u64 = (4.55 * 1024.0 * 1024.0 * 1024.0) as u64;

/// [`RSS_BUDGET_BYTES_HINTED`]'s counterpart at [`ENTRIES_64M`], hinted:
/// measures ~67.3 bytes/entry, essentially the same as unhinted at this
/// scale since the per-stripe hint is large enough that hash variance
/// rarely overshoots reserved capacity.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "a small positive GiB constant, exact in f64 up to a rounding sub-byte"
)]
const RSS_BUDGET_BYTES_64M_HINTED: u64 = (4.54 * 1024.0 * 1024.0 * 1024.0) as u64;

/// Scale-invariance confirmation: the same unhinted/hinted pair
/// [`entry_diet_rss_budget`] runs, at [`ENTRIES_64M`] entries instead,
/// measuring the ~constant bytes-per-entry claim directly across a 16x
/// jump rather than assuming it. Skips with a printed reason when
/// [`has_enough_ram_for_64m`] reads `MemAvailable` below
/// [`MIN_MEM_AVAILABLE_FOR_64M_BYTES`].
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn entry_diet_rss_budget_64m() {
    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }
    if !has_enough_ram_for_64m(mem_available_bytes()) {
        eprintln!(
            "skipping entry_diet_rss_budget_64m: MemAvailable is below the \
             {MIN_MEM_AVAILABLE_FOR_64M_BYTES}-byte floor this pair of {ENTRIES_64M}-entry runs \
             (~4 GiB settled each) needs"
        );
        return;
    }

    let unhinted = measure_rss_run(
        "bench-entry-diet-rss-64m-unhinted",
        "entry-diet-rss-64m-unhinted",
        ENTRIES_64M,
        None,
        rss_key,
        rss_value,
    )
    .await;
    unhinted.report(
        "entry_diet_rss_budget_64m_unhinted",
        RSS_BUDGET_BYTES_64M,
        true,
    );

    let hinted = measure_rss_run(
        "bench-entry-diet-rss-64m-hinted",
        "entry-diet-rss-64m-hinted",
        ENTRIES_64M,
        Some(u64::from(ENTRIES_64M)),
        rss_key,
        rss_value,
    )
    .await;
    hinted.report(
        "entry_diet_rss_budget_64m_hinted",
        RSS_BUDGET_BYTES_64M_HINTED,
        true,
    );
}

/// Same as [`RSS_BUDGET_ENTRIES`], so the two shapes are comparable.
const HEAP_SHAPE_RSS_BUDGET_ENTRIES: u32 = RSS_BUDGET_ENTRIES;

/// Budget for [`entry_diet_rss_budget_heap_shape`]'s run: measures
/// ~209.6 bytes/entry, inside Redis's 195-230 bytes/copy range for the
/// same shape. Set with ~13% headroom over the measured number.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "a small positive GiB constant, exact in f64 up to a rounding sub-byte"
)]
const HEAP_SHAPE_RSS_BUDGET_BYTES: u64 = (0.885 * 1024.0 * 1024.0 * 1024.0) as u64;

/// RSS budget for `Record::Heap`'s shape: a 16-byte key and 100-byte
/// value, both past `Record::INLINE_CAP`, so every entry takes one heap
/// allocation on both sundog and Redis. Otherwise identical to
/// [`entry_diet_rss_budget`]'s unhinted run, asserting settled RSS stays
/// at or under [`HEAP_SHAPE_RSS_BUDGET_BYTES`].
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn entry_diet_rss_budget_heap_shape() {
    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }

    let run = measure_rss_run(
        "bench-entry-diet-rss-heap-shape",
        "entry-diet-rss-heap-shape",
        HEAP_SHAPE_RSS_BUDGET_ENTRIES,
        None,
        heap_key,
        heap_value,
    )
    .await;
    run.report(
        "entry_diet_rss_budget_heap_shape",
        HEAP_SHAPE_RSS_BUDGET_BYTES,
        true,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn has_enough_ram_for_64m_requires_the_floor() {
        assert!(!has_enough_ram_for_64m(None));
        assert!(!has_enough_ram_for_64m(Some(
            MIN_MEM_AVAILABLE_FOR_64M_BYTES - 1
        )));
        assert!(has_enough_ram_for_64m(Some(
            MIN_MEM_AVAILABLE_FOR_64M_BYTES
        )));
        assert!(has_enough_ram_for_64m(Some(
            MIN_MEM_AVAILABLE_FOR_64M_BYTES + 1
        )));
    }

    #[test]
    fn rss_key_stays_seven_bytes_up_to_the_64m_run() {
        assert_eq!(rss_key(0).len(), 7);
        assert_eq!(rss_key(RSS_BUDGET_ENTRIES - 1).len(), 7);
        assert_eq!(rss_key(ENTRIES_64M - 1).len(), 7);
    }

    #[test]
    fn rss_value_stays_eight_bytes_up_to_the_64m_run() {
        assert_eq!(rss_value(0).len(), 8);
        assert_eq!(rss_value(RSS_BUDGET_ENTRIES - 1).len(), 8);
        assert_eq!(rss_value(ENTRIES_64M - 1).len(), 8);
    }

    #[test]
    fn heap_key_and_value_hold_their_stated_widths() {
        assert_eq!(heap_key(0).len(), 16);
        assert_eq!(heap_key(HEAP_SHAPE_RSS_BUDGET_ENTRIES - 1).len(), 16);
        assert_eq!(heap_value(0).len(), 100);
        assert_eq!(heap_value(HEAP_SHAPE_RSS_BUDGET_ENTRIES - 1).len(), 100);
    }
}

//! Entry diet acceptance bench: real, single-node, loopback [`Mode::Local`]
//! caches, public API only. Not a correctness suite; it measures and prints,
//! then asserts the two numbers this workstream's diet of `Live<K, V>`
//! (`sundog/src/store/engine.rs`) is scored against.
//!
//! Gated on `SUNDOG_BENCH=1`, an `eprintln!` and early return otherwise, so
//! a plain `cargo test` run still compiles without the wall-clock cost:
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

/// The process allocator, wrapped to count the bytes currently allocated:
/// what the engine and everything else in the process hold live, as
/// opposed to the resident set, which also carries what the allocator
/// keeps after a free. The gap between the two is allocator retention;
/// [`entry_diet_rss_budget`] prints both per entry.
struct CountingAlloc;

static LIVE_HEAP_BYTES: AtomicUsize = AtomicUsize::new(0);

// SAFETY: every call forwards to `System` unchanged; the counter is
// updated with the sizes the layout carries, never touching the memory.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract for `alloc` is forwarded as is.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            LIVE_HEAP_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE_HEAP_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        // SAFETY: the caller's contract for `dealloc` is forwarded as is.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: the caller's contract for `realloc` is forwarded as is.
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

/// Bytes the process holds allocated right now, per [`CountingAlloc`].
fn live_heap_bytes() -> u64 {
    u64::try_from(LIVE_HEAP_BYTES.load(Ordering::Relaxed)).unwrap_or(u64::MAX)
}

fn bench_enabled() -> bool {
    std::env::var("SUNDOG_BENCH").as_deref() == Ok("1")
}

/// A single-node, loopback, `Static`-discovery cluster: every benchmark's
/// cache lives on this one node.
async fn local_cluster(name: &str) -> Cluster {
    Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(common::fast_config())
        .build()
        .await
        .expect("single-node loopback cluster builds")
}

/// The `p`th percentile (0-100) of an ascending-sorted `durations`, nearest-
/// rank, reported in microseconds. Mirrors `tests/spill_bench.rs`'s own
/// `percentile_micros`; integration test binaries don't share code beyond
/// `mod common`.
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
/// Mirrors `tests/spill_bench.rs`'s own copy.
fn xorshift64(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

/// `sundog/src/store/engine.rs:6`'s stated profile: a 7-byte key,
/// `k` followed by 6 zero-padded digits.
fn profile_key(i: u32) -> String {
    format!("k{i:06}")
}

/// `sundog/src/store/engine.rs:6`'s stated profile: an 8-byte value, `v`
/// followed by 7 zero-padded digits.
fn profile_value(i: u32) -> String {
    format!("v{i:07}")
}

/// A key that stays exactly 7 ASCII bytes across the full `[0, 4_000_000)`
/// range `entry_diet_rss_budget` inserts, unlike [`profile_key`]'s `k`
/// prefix, which would grow past 7 bytes once `i` needs a 7th digit.
fn rss_key(i: u32) -> String {
    format!("{i:07}")
}

/// [`rss_key`]'s value counterpart: exactly 8 ASCII bytes across the same
/// range.
fn rss_value(i: u32) -> String {
    format!("{i:08}")
}

/// This bench's own `get_sync` p50, measured against `main` before
/// `Live<K, V>`'s record diet landed (`sundog/src/store/engine.rs`,
/// pre-diet commit `12b56ee`): 200,000 entries, 1,000,000 xorshift-driven
/// reads, `Cache<String, String>` at this file's exact key/value profile,
/// `git archive 12b56ee` extracted to a scratch checkout and built with
/// `cargo test --release`. Four back-to-back runs on the same box printed
/// `p50_micros` of 0.519, 0.584, 0.597, 0.536; this constant is their
/// average, rounded to two significant figures.
const PRE_DIET_P50_MICROS: f64 = 0.56;

/// Read p50 stays within this fraction of [`PRE_DIET_P50_MICROS`]: a decode
/// on every read costs something over the old clone, but not an order of
/// magnitude, per the risk this workstream's spec flags for the read path.
const P50_TOLERANCE: f64 = 0.10;

/// Read latency: 1,000,000 xorshift-skewed `get_sync` calls against a warm
/// 200,000-entry `Cache<String, String>` at this file's stated profile,
/// asserting the new decode-on-read p50 stays within
/// [`P50_TOLERANCE`] of [`PRE_DIET_P50_MICROS`].
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

/// Reads this process's own resident set size from `/proc/self/status`'s
/// `VmRSS` line, in bytes. `None` on a platform or sandbox without that
/// file (never expected on the Linux CI runners this bench targets, but
/// tolerated rather than panicking).
fn vm_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find_map(|line| {
        let rest = line.strip_prefix("VmRSS:")?;
        let kib: u64 = rest.trim().trim_end_matches(" kB").trim().parse().ok()?;
        Some(kib * 1024)
    })
}

/// Polls [`vm_rss_bytes`] until it stops growing across `STABLE_ROUNDS`
/// consecutive checks, or `timeout` elapses either way: lets the allocator
/// and any background threads settle before the RSS reading is taken.
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

/// The settled RSS the diet measures at 4,000,000 entries on a 4-core
/// Linux box is 0.66 GiB, 177 bytes per entry, the same under glibc and
/// jemalloc: a 72-byte `Live` in hashbrown tables that size each stripe
/// to a power of two, and no heap allocation for this profile's 18-byte
/// inline records. This budget sits 13% above that reading, so a
/// regression of a few bytes per entry fails the bench while allocator
/// and kernel noise between runs does not.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "0.75 GiB is a small positive constant, exact in f64 up to a rounding sub-byte"
)]
const RSS_BUDGET_BYTES: u64 = (0.75 * 1024.0 * 1024.0 * 1024.0) as u64;

/// The entry count [`entry_diet_rss_budget`] inserts and pins its budget
/// against.
const RSS_BUDGET_ENTRIES: u32 = 4_000_000;

/// The entry count [`entry_diet_rss_budget`] inserts:
/// [`RSS_BUDGET_ENTRIES`], or `SUNDOG_BENCH_ENTRIES` when set to a
/// positive integer, for a density measurement at another size. The
/// budget assertion only runs at [`RSS_BUDGET_ENTRIES`], the size the
/// budget is stated for.
fn rss_bench_entries() -> u32 {
    std::env::var("SUNDOG_BENCH_ENTRIES")
        .ok()
        .and_then(|raw| raw.parse::<u32>().ok())
        .filter(|&entries| entries > 0)
        .unwrap_or(RSS_BUDGET_ENTRIES)
}

/// RSS budget: [`RSS_BUDGET_ENTRIES`] entries inserted into a
/// spill-disabled `Cache<String, String>` at this file's stated profile,
/// asserting the settled resident set stays at or under
/// [`RSS_BUDGET_BYTES`]. Prints the settled resident set before the first
/// insert too, so the per-entry cost the line reports comes in two forms:
/// the whole process divided by the count, and the growth alone divided by
/// the count, which is the engine's own marginal cost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn entry_diet_rss_budget() {
    const CHUNK: u32 = 50_000;

    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }
    let entries = rss_bench_entries();

    let cluster = local_cluster("bench-entry-diet-rss").await;
    let cache = cluster
        .cache::<String, String>("entry-diet-rss")
        .mode(Mode::Local)
        .open()
        .await
        .expect("cache opens");
    let baseline = settled_vm_rss_bytes(Duration::from_secs(10)).await;
    let baseline_live = live_heap_bytes();

    let started = Instant::now();
    let mut start = 0u32;
    while start < entries {
        let end = (start + CHUNK).min(entries);
        cache
            .insert_many((start..end).map(|i| (rss_key(i), rss_value(i))))
            .await
            .expect("insert_many succeeds");
        start = end;
    }
    let insert_elapsed = started.elapsed();

    let rss = settled_vm_rss_bytes(Duration::from_secs(60)).await;
    let live = live_heap_bytes();
    let entry_count = cache.entry_count().await;

    #[allow(clippy::cast_precision_loss, reason = "reporting only, not compared")]
    let rss_gib = rss.map_or(0.0, |b| b as f64 / (1024.0 * 1024.0 * 1024.0));
    #[allow(clippy::cast_precision_loss, reason = "reporting only, not compared")]
    let bytes_per_entry = rss.map_or(0.0, |b| b as f64 / f64::from(entries));
    #[allow(clippy::cast_precision_loss, reason = "reporting only, not compared")]
    let marginal_bytes_per_entry = match (rss, baseline) {
        (Some(rss), Some(baseline)) => rss.saturating_sub(baseline) as f64 / f64::from(entries),
        _ => 0.0,
    };
    #[allow(clippy::cast_precision_loss, reason = "reporting only, not compared")]
    let live_heap_bytes_per_entry = live.saturating_sub(baseline_live) as f64 / f64::from(entries);

    println!(
        "BENCH entry_diet_rss_budget entries={entries} entry_count={entry_count} \
         insert_secs={:.3} baseline_rss_bytes={} rss_bytes={} rss_gib={rss_gib:.3} \
         bytes_per_entry={bytes_per_entry:.1} \
         marginal_bytes_per_entry={marginal_bytes_per_entry:.1} \
         live_heap_bytes_per_entry={live_heap_bytes_per_entry:.1} budget_bytes={RSS_BUDGET_BYTES}",
        insert_elapsed.as_secs_f64(),
        baseline.map_or("unavailable".to_string(), |b| b.to_string()),
        rss.map_or("unavailable".to_string(), |b| b.to_string()),
    );

    assert_eq!(entry_count, u64::from(entries), "every insert lands");
    if let (Some(rss), true) = (rss, entries == RSS_BUDGET_ENTRIES) {
        assert!(
            rss <= RSS_BUDGET_BYTES,
            "steady-state RSS {rss} bytes ({rss_gib:.3} GiB) exceeds the {RSS_BUDGET_BYTES}-byte \
             budget for {entries} entries"
        );
    } else {
        eprintln!("skipping the RSS assertion: /proc/self/status is unavailable on this platform");
    }

    cluster.shutdown().await;
}

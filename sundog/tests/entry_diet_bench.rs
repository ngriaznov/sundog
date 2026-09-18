//! Entry diet acceptance bench: real, single-node, loopback [`Mode::Local`]
//! caches, public API only. Not a correctness suite; it measures and prints,
//! then asserts the numbers this workstream's diet of `Live<K, V>`
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

/// A key that stays exactly 7 ASCII bytes across every range this file's
/// RSS runs insert, up to and including [`ENTRIES_64M`]: an index's
/// lowercase hex digits, zero-padded to 7, stay exactly 7 bytes up to
/// `16^7` (about 268 million), unlike a 7-digit decimal counterpart, which
/// needs an 8th digit past 9,999,999 and would silently widen the key
/// (and this profile's record) partway through a 64,000,000-entry run.
fn rss_key(i: u32) -> String {
    format!("{i:07x}")
}

/// [`rss_key`]'s value counterpart: exactly 8 ASCII bytes. A decimal
/// counter already stays within 8 digits up to 99,999,999, past
/// [`ENTRIES_64M`], so it needs no hex switch the way [`rss_key`] does.
fn rss_value(i: u32) -> String {
    format!("{i:08}")
}

/// [`entry_diet_rss_budget_heap_shape`]'s key: a decimal index zero-padded
/// to 16 ASCII bytes. Framed with [`heap_value`], the postcard record is
/// well past `Record::INLINE_CAP`, so every entry takes `Record::Heap`'s
/// one allocation, matching Redis's own heap-shape `robj`+`sds` pair for
/// a fair comparison.
fn heap_key(i: u32) -> String {
    format!("{i:016}")
}

/// [`heap_key`]'s value counterpart: exactly 100 ASCII bytes.
fn heap_value(i: u32) -> String {
    format!("{i:0100}")
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

/// Reads `/proc/meminfo`'s `MemAvailable` line, in bytes: the kernel's own
/// estimate of what a new allocation can claim without swapping, unlike
/// `MemFree` alone, which excludes page cache the kernel would reclaim on
/// demand. `None` on a platform or sandbox without that file.
fn mem_available_bytes() -> Option<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    meminfo.lines().find_map(|line| {
        let rest = line.strip_prefix("MemAvailable:")?;
        let kib: u64 = rest.trim().trim_end_matches(" kB").trim().parse().ok()?;
        Some(kib * 1024)
    })
}

/// The `MemAvailable` floor [`entry_diet_rss_budget_64m`] requires before it
/// starts: its unhinted and hinted runs each settle around 4 GiB, run one
/// after the other rather than concurrently, and each transiently holds
/// more than its settled reading while the insert loop and the allocator's
/// own growth churn are still in flight. 10 GiB leaves headroom for that
/// transient peak, the rest of the test binary, its allocator overhead,
/// and whatever else shares the box.
const MIN_MEM_AVAILABLE_FOR_64M_BYTES: u64 = 10 * 1024 * 1024 * 1024;

/// Pure decision behind [`entry_diet_rss_budget_64m`]'s early skip:
/// `mem_available_bytes` must both be readable and meet
/// [`MIN_MEM_AVAILABLE_FOR_64M_BYTES`] — an unreadable `MemAvailable` (
/// `None`) gives no basis for judging headroom, so it counts as
/// insufficient rather than being assumed fine.
fn has_enough_ram_for_64m(mem_available_bytes: Option<u64>) -> bool {
    mem_available_bytes.is_some_and(|bytes| bytes >= MIN_MEM_AVAILABLE_FOR_64M_BYTES)
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

// SAFETY-relevant note: `malloc_trim`/`mallopt` are glibc's own API for
// asking the allocator to release memory it has freed but not yet returned
// to the OS, and for tuning when it does so.
#[cfg(target_os = "linux")]
unsafe extern "C" {
    fn malloc_trim(pad: usize) -> i32;
    fn mallopt(param: i32, value: i32) -> i32;
}

/// glibc's `mallopt` parameter selecting the mmap threshold, from
/// `malloc.h`: allocations at or above it go straight to `mmap` instead of
/// the growable `brk` arena.
#[cfg(target_os = "linux")]
const M_MMAP_THRESHOLD: i32 = -3;

/// Below this size, `force_mmap_for_large_allocations` leaves glibc's
/// ordinary arena in charge: small enough that it never touches a stripe's
/// own arena/index allocations (each at least in the tens of KiB at this
/// file's entry counts) or a fresh, empty per-stripe `HashMap`.
#[cfg(target_os = "linux")]
const MMAP_THRESHOLD_BYTES: i32 = 64 * 1024;

/// Pins glibc's mmap threshold at [`MMAP_THRESHOLD_BYTES`] and disables its
/// own dynamic adjustment of that threshold (`mallopt`'s documented side
/// effect of setting it explicitly), then releases whatever the allocator
/// has already freed but not yet returned to the OS. Both matter for this
/// file's multi-scenario tests (measuring more than one
/// [`measure_rss_run`] scenario per process): every stripe's arena/index
/// allocation here is well past this threshold, so pinning it forces each
/// one through `mmap`, which the kernel reclaims in full and immediately
/// on `Drop`, rather than through the arena, where a previous scenario's
/// same-sized-but-not-identical freed chunks can sit unreturned (`free()`
/// alone never shrinks `VmRSS`) and inflate a later scenario's own
/// reading by tens of bytes per entry — an artifact of two scenarios
/// sharing a process, not a real cost either one pays standalone. A no-op
/// on a non-Linux target, where this file's other `/proc`-based readings
/// already return `None` regardless.
fn stabilize_allocator_for_next_scenario() {
    #[cfg(target_os = "linux")]
    // SAFETY: `mallopt` and `malloc_trim(0)` are safe to call at any time
    // per glibc's documented contract; they only tune the allocator's own
    // policy and free memory it already knows is unused, never touching
    // anything still live.
    unsafe {
        mallopt(M_MMAP_THRESHOLD, MMAP_THRESHOLD_BYTES);
        malloc_trim(0);
    }
}

/// One RSS run's raw measurements: entry count, timing, and the RSS/live-
/// heap byte counts read before and after the insert loop. Shared by every
/// scenario this file measures (hinted/unhinted, 4M/64M, the default and
/// heap-shape profiles) so the insert-and-settle path is exercised
/// identically everywhere, via [`measure_rss_run`].
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

    /// Prints this run's `BENCH` line under `label`, always asserting every
    /// insert landed, and additionally asserting the settled RSS is at or
    /// under `budget_bytes` when `assert_budget` is set and RSS was
    /// readable on this platform at all. `assert_budget` is `false` for an
    /// ad hoc `SUNDOG_BENCH_ENTRIES` override, whose budget only applies at
    /// the size it was measured for.
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

/// Inserts `entries` `key_fn`/`value_fn` pairs into a fresh, spill-disabled
/// `Cache<String, String>` (opened with `capacity_hint` when given), and
/// returns the settled [`RssRun`]. `cluster_name`/`cache_name` must be
/// unique per call within one test binary process, since every call opens
/// its own loopback cluster.
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

/// The settled RSS the diet measures at 4,000,000 entries on a 4-core
/// Linux box, with `Stripe::live` (`sundog/src/store/engine.rs`) a `Slab`
/// (a dense `Vec<Live<K, V>>` arena plus a `u32`-keyed `HashTable` index)
/// over Phase A's packed 56-byte `Live`, is about 0.278 GiB, 74.7 bytes
/// per entry, 69.2 of it live heap: the arena's own power-of-two growth
/// plus the index's roughly 5 bytes per bucket at this profile's load
/// factor, and no heap allocation for this profile's 18-byte inline
/// records. This budget sits at 0.315 GiB, about 13% above that reading,
/// so a regression of a few bytes per entry fails the bench while
/// allocator and kernel noise between runs does not.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "0.315 GiB is a small positive constant, exact in f64 up to a rounding sub-byte"
)]
const RSS_BUDGET_BYTES: u64 = (0.315 * 1024.0 * 1024.0 * 1024.0) as u64;

/// [`RSS_BUDGET_BYTES`]'s counterpart for the same 4,000,000-entry profile
/// opened with `CacheBuilder::capacity_hint(RSS_BUDGET_ENTRIES)`: every
/// stripe's `Slab` presizes for its share of the hint at `open()` instead
/// of growing one insert at a time. Measures a little higher than
/// [`RSS_BUDGET_BYTES`]'s unhinted reading at this exact profile, about
/// 77.3 bytes per entry (0.288 GiB): hinting the exact expected count
/// means any stripe whose real share lands even one key over its
/// reserved capacity, which about half of 1024 stripes do at this
/// entry count under ordinary hash variance, still pays a full doubling
/// growth from that reserved base rather than from zero, and that
/// doubling outweighs what presizing saves the stripes that stay at or
/// under their share. Set from the measured hinted number with about 13%
/// headroom, the same way as [`RSS_BUDGET_BYTES`].
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "a small positive GiB constant, exact in f64 up to a rounding sub-byte"
)]
const RSS_BUDGET_BYTES_HINTED: u64 = (0.325 * 1024.0 * 1024.0 * 1024.0) as u64;

/// The entry count [`entry_diet_rss_budget`] inserts and pins its budgets
/// against.
const RSS_BUDGET_ENTRIES: u32 = 4_000_000;

/// The entry count [`entry_diet_rss_budget`] inserts:
/// [`RSS_BUDGET_ENTRIES`], or `SUNDOG_BENCH_ENTRIES` when set to a
/// positive integer, for a density measurement at another size. The
/// budget assertions only run at [`RSS_BUDGET_ENTRIES`], the size the
/// budgets are stated for.
fn rss_bench_entries() -> u32 {
    std::env::var("SUNDOG_BENCH_ENTRIES")
        .ok()
        .and_then(|raw| raw.parse::<u32>().ok())
        .filter(|&entries| entries > 0)
        .unwrap_or(RSS_BUDGET_ENTRIES)
}

/// RSS budget: [`rss_bench_entries`] entries (normally
/// [`RSS_BUDGET_ENTRIES`]) inserted twice into a spill-disabled
/// `Cache<String, String>` at this file's stated profile, once with no
/// `capacity_hint` and once hinted at the exact entry count, asserting
/// each settled resident set stays at or under its own budget
/// ([`RSS_BUDGET_BYTES`] unhinted, [`RSS_BUDGET_BYTES_HINTED`] hinted)
/// when run at [`RSS_BUDGET_ENTRIES`]. Prints the settled resident set
/// before the first insert too, so the per-entry cost each `BENCH` line
/// reports comes in two forms: the whole process divided by the count,
/// and the growth alone divided by the count, which is the engine's own
/// marginal cost.
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

/// The entry count [`entry_diet_rss_budget_64m`] inserts: a second point,
/// 16x [`RSS_BUDGET_ENTRIES`]'s 4,000,000, confirming by direct
/// measurement (rather than by extrapolation) that the bytes-per-entry
/// figure holds, or improves, across a large entry-count jump.
const ENTRIES_64M: u32 = 64_000_000;

/// [`RSS_BUDGET_BYTES`]'s counterpart at [`ENTRIES_64M`] entries, unhinted:
/// measures at about 4.017 GiB, 67.4 bytes per entry, lower than
/// [`RSS_BUDGET_ENTRIES`]'s 74.7: each stripe's fixed overhead (glibc
/// allocator retention and chunk bookkeeping, not the arena or index data
/// itself) amortizes over more entries as the count grows, so it costs
/// less per entry, never more. Set from the measured number with about
/// 13% headroom, never from the ≤90-bytes-per-entry target.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "a small positive GiB constant, exact in f64 up to a rounding sub-byte"
)]
const RSS_BUDGET_BYTES_64M: u64 = (4.55 * 1024.0 * 1024.0 * 1024.0) as u64;

/// [`RSS_BUDGET_BYTES_HINTED`]'s counterpart at [`ENTRIES_64M`] entries,
/// hinted at the exact entry count: measures at about 4.010 GiB, 67.3
/// bytes per entry, essentially the same as the unhinted reading at this
/// scale (unlike at [`RSS_BUDGET_ENTRIES`], where hinting the exact count
/// costs a little more) — at 64,000,000 entries the per-stripe hint is
/// large enough that ordinary hash variance rarely pushes a stripe's real
/// share past its reserved capacity. Set the same way as
/// [`RSS_BUDGET_BYTES_64M`].
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "a small positive GiB constant, exact in f64 up to a rounding sub-byte"
)]
const RSS_BUDGET_BYTES_64M_HINTED: u64 = (4.54 * 1024.0 * 1024.0 * 1024.0) as u64;

/// Scale-invariance confirmation: the same unhinted/hinted pair
/// [`entry_diet_rss_budget`] runs at [`RSS_BUDGET_ENTRIES`], run once
/// more at [`ENTRIES_64M`] entries (about 4 GiB settled each) so the
/// ~constant bytes-per-entry claim across a 16x entry-count jump is
/// measured directly rather than assumed. Measures lower per entry than
/// [`RSS_BUDGET_ENTRIES`]'s own pair (67.4 unhinted, 67.3 hinted, against
/// 74.7 and 77.3): each stripe's fixed overhead amortizes over more
/// entries as the count grows, so it costs less per entry, never more.
/// Skips with a printed reason, rather than risking this box (or
/// whatever else shares it) running out of memory, when
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

/// The entry count [`entry_diet_rss_budget_heap_shape`] inserts: the same
/// [`RSS_BUDGET_ENTRIES`] as the default profile, so the two shapes'
/// `BENCH` lines are directly comparable at the same scale.
const HEAP_SHAPE_RSS_BUDGET_ENTRIES: u32 = RSS_BUDGET_ENTRIES;

/// Budget for [`entry_diet_rss_budget_heap_shape`]'s 16-byte-key/100-byte-
/// value run, unhinted: measures at about 0.781 GiB, 209.6 bytes per
/// entry, inside Redis's own 195-230 bytes/copy practical range for the
/// same shape (184 bytes/copy computed from Redis's own `dictEntry`, key
/// and value `sds` layout, and bucket-slot accounting for this shape).
/// Sundog's target for this shape is parity with Redis, not another win,
/// stated as such rather than pinned to a sundog-favoring number. Set
/// from the measured number with about 13% headroom.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "a small positive GiB constant, exact in f64 up to a rounding sub-byte"
)]
const HEAP_SHAPE_RSS_BUDGET_BYTES: u64 = (0.885 * 1024.0 * 1024.0 * 1024.0) as u64;

/// RSS budget for `Record::Heap`'s shape: a 16-byte key and a 100-byte
/// value, both past `Record::INLINE_CAP`, so every entry takes one heap
/// allocation on both sundog and Redis (past sundog's `Record::Inline`
/// cap and Redis's `embstr` threshold alike). Otherwise identical in
/// structure to [`entry_diet_rss_budget`]'s unhinted run:
/// [`HEAP_SHAPE_RSS_BUDGET_ENTRIES`] entries inserted into a fresh,
/// spill-disabled `Cache<String, String>`, asserting the settled resident
/// set stays at or under [`HEAP_SHAPE_RSS_BUDGET_BYTES`].
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

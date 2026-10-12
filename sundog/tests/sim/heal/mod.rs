//! The partition-heal harness: variants, configuration and metrics.

// ---------------------------------------------------------------------
// Partition-heal family: three nodes, a two-way partition (node `a` split
// from `b`/`c`, which stay mutually connected) during which side a and
// side b each increment a share of `keys` counters, then a heal. Run in
// two variants over the same shape, one merged `PnCounter` key per counter
// under `PnCounterResolver`, and per-writer keys decomposed under the
// default `LwwResolver`, measuring anti-entropy rounds, virtual time,
// frames, bytes, records, engine applies, resolver folds, and redundant
// pulls spent reconciling after the heal, against the exact expected
// total. `SUNDOG_SIM_FULL` controls the grid's size: the fast default
// (2,000 keys, conflict fraction 0.0/1.0, one seed) or the full sweep
// (2,000 and 20,000 keys, the whole 0.0/0.1/0.5/1.0 conflict-fraction
// range, three seeds).
// ---------------------------------------------------------------------

mod counting;
mod driver;
mod scenarios;

/// Which side of the fair comparison a partition-heal run exercises: the
/// same content, keyed either per-writer under [`LwwResolver`] (so no key is
/// ever really contended) or on one shared key under [`PnCounterResolver`]
/// (so every overlapping counter is a real, folded merge). This is the
/// fair A/B comparison [`partition_heal_comparison`] measures across every
/// configured key count, conflict fraction, and seed.
///
/// [`LwwResolver`]: sundog::LwwResolver
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Variant {
    Decomposed,
    Merged,
}

impl Variant {
    const fn tag(self) -> &'static str {
        match self {
            Self::Decomposed => "decomposed",
            Self::Merged => "merged",
        }
    }
}

const HEAL_DEFAULT_KEYS: u32 = 2_000;
const HEAL_DEFAULT_CONFLICT_FRACTIONS: [f64; 2] = [0.0, 1.0];
const HEAL_FULL_KEYS: [u32; 2] = [2_000, 20_000];
const HEAL_FULL_CONFLICT_FRACTIONS: [f64; 4] = [0.0, 0.1, 0.5, 1.0];
const HEAL_FULL_SEEDS: [u64; 3] = [0xC0DE_7001, 0xC0DE_7002, 0xC0DE_7003];
const HEAL_INCREMENTS_PER_SIDE: u32 = 3;
const HEAL_PARTITION_MS: u64 = 500;
/// Deliberately generous relative to a single round's own cost, even at
/// this family's largest key count: a repair large enough to need several
/// `REPAIR_BATCH` chunks spans several `sim.step()`s within one
/// `run_round_against` call, and node b's and node c's own independent
/// tick (never partitioned from node a, so *also* a candidate to relay
/// node a's content, and equally capable of fully repairing a pair on its
/// own as node a's own round against that same pair is) can fall on any
/// step in between. A short tick period lets that race resolve differently
/// depending on how many chunks a given key count happens to need -- the
/// scale-sensitive non-determinism the "Findings" section's own
/// non-monotonic figures came from. A period long enough that every node's
/// own round, at every scale this grid drives, always finishes well before
/// the next tick could possibly fire removes the race by removing the
/// overlap: the ordering the two paths land in step order stays the same
/// regardless of key count, since neither path is still running when
/// the next tick becomes possible.
///
/// This is a deliberate trade-off, not an oversight: production's own
/// default `ae_interval` is 200ms (`ClusterConfig::default`,
/// `sundog/src/cluster.rs`), 5x shorter than this constant, specifically
/// because a shorter tick can let node b/c's own independent round overlap
/// the multi-step repair this family drives -- the exact race this constant
/// is chosen to avoid. Every assertion in this family (determinism,
/// monotonicity, exact convergence) therefore measures the resolver and
/// anti-entropy logic in isolation from that scheduling race, not the
/// bidirectional exchange's round count at production's actual tick
/// cadence: a merging resolver's bidirectional exchange trades a round
/// for an extra push/pull pair per divergent key, a real slowdown at
/// production's actual 200ms cadence. This generous margin only shows
/// that nothing here gets slower still; it does not bound how much slower
/// the bidirectional exchange itself is at that cadence;
/// [`partition_heal_at_the_default_ae_interval`] runs the same grid at
/// 200ms to measure that directly.
const HEAL_AE_INTERVAL_MS: u64 = 1000;
/// Every anti-entropy round in this family runs against at most two peers
/// on a 1s tick; a generous multiple of the handful of rounds convergence
/// needs at every scale this grid drives, so a run spending more than
/// this many rounds signals a genuine slowdown rather than ordinary
/// scheduling noise.
const HEAL_MAX_AE_ROUNDS: u64 = 60;
/// `40_000` sits past the `20_000` scale where the Findings section's own
/// non-monotonic round counts appear, giving this pin headroom beyond
/// that exact scale, not merely up to it.
const HEAL_MONOTONIC_KEYS: [u32; 5] = [2_000, 8_000, 16_000, 20_000, 40_000];

/// `SUNDOG_SIM_FULL=1` switches [`partition_heal_comparison`] from the fast
/// default grid to the full one: more key counts, the whole conflict-fraction
/// range, and three seeds instead of one.
fn heal_sim_full() -> bool {
    std::env::var("SUNDOG_SIM_FULL").is_ok_and(|v| v == "1")
}

/// One partition-heal scenario's full parameterization; see [`run_partition_heal`] for what each field drives.
#[derive(Debug, Clone, Copy, PartialEq)]
struct HealConfig {
    seed: u64,
    variant: Variant,
    keys: u32,
    conflict_fraction: f64,
    increments_per_side: u32,
    partition_ms: u64,
    ae_interval_ms: u64,
}

impl HealConfig {
    fn new(seed: u64, variant: Variant, keys: u32, conflict_fraction: f64) -> Self {
        Self {
            seed,
            variant,
            keys,
            conflict_fraction,
            increments_per_side: HEAL_INCREMENTS_PER_SIDE,
            partition_ms: HEAL_PARTITION_MS,
            ae_interval_ms: HEAL_AE_INTERVAL_MS,
        }
    }
}

/// One [`run_partition_heal`] call's reported metrics, from the heal onward.
/// `records` is every record either direction applied (or attempted to
/// apply), `applies` is the subset that changed the touched key's stored
/// `(version, bytes)`, `folds` is how many times the resolver's `merge`
/// itself returned `Some`, and `redundant_pulls` is the pull-direction
/// subset of `records - applies` -- the "Findings" section's own
/// redundant-pull cost of the bidirectional exchange, isolated from the
/// push direction.
#[derive(Debug, Clone, PartialEq)]
struct HealMetrics {
    variant: &'static str,
    keys: u32,
    conflict_fraction: f64,
    seed: u64,
    ae_rounds: u64,
    virtual_ms: u64,
    frames: u64,
    bytes: u64,
    records: u64,
    applies: u64,
    folds: u64,
    redundant_pulls: u64,
    expected_total: i128,
    actual_total: i128,
    lost_updates: i128,
}

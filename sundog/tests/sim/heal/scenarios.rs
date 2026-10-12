//! The partition-heal scenarios.

use super::driver::{print_heal_comparison, run_partition_heal};
use super::{
    HEAL_DEFAULT_CONFLICT_FRACTIONS, HEAL_DEFAULT_KEYS, HEAL_FULL_CONFLICT_FRACTIONS,
    HEAL_FULL_KEYS, HEAL_FULL_SEEDS, HEAL_MAX_AE_ROUNDS, HEAL_MONOTONIC_KEYS, HealConfig, Variant,
    heal_sim_full,
};
use crate::support::sim_seed;

/// Correctness only, at the default grid: every combination of variant and
/// conflict fraction converges to the exact expected total. The convergence
/// loop inside `run_partition_heal` already gates on this (and on all three
/// nodes' fingerprints agreeing) before it ever returns, so a `HealMetrics`
/// this test receives at all already reflects a converged, correct run;
/// `lost_updates == 0` restates that guarantee explicitly rather than
/// relying on it being implicit.
#[test]
fn partition_heal_variant_reaches_exact_totals() {
    let seed = sim_seed(0xC0DE_7100);
    for variant in [Variant::Decomposed, Variant::Merged] {
        for &conflict_fraction in &HEAL_DEFAULT_CONFLICT_FRACTIONS {
            let metrics = run_partition_heal(HealConfig::new(
                seed,
                variant,
                HEAL_DEFAULT_KEYS,
                conflict_fraction,
            ));
            assert_eq!(
                metrics.lost_updates,
                0,
                "{}: keys={} conflict_fraction={conflict_fraction} did not converge to the exact total",
                variant.tag(),
                metrics.keys
            );
        }
    }
}

/// The fair decomposed-vs-merged comparison: for every config in the grid,
/// runs decomposed then merged with the same seed, prints one `SIM` line
/// per run and one `SIM partition_heal_pair` line per pair, and asserts only
/// that both sides converged correctly and within
/// [`HEAL_MAX_AE_ROUNDS`] -- never a specific ratio between them, which the
/// printed `SIM`/`SIM partition_heal_pair` lines are there to read, not a
/// property to pin.
/// `SUNDOG_SIM_FULL=1` swaps the fast default grid (2,000 keys, conflict
/// fraction 0.0/1.0, one seed) for the full one (2,000 and 20,000 keys, the
/// whole 0.0/0.1/0.5/1.0 conflict-fraction range, three seeds).
#[test]
fn partition_heal_comparison() {
    let full = heal_sim_full();
    let keys_grid: &[u32] = if full {
        &HEAL_FULL_KEYS
    } else {
        &[HEAL_DEFAULT_KEYS]
    };
    let fractions: &[f64] = if full {
        &HEAL_FULL_CONFLICT_FRACTIONS
    } else {
        &HEAL_DEFAULT_CONFLICT_FRACTIONS
    };
    let default_seed = [sim_seed(0xC0DE_7200)];
    let seeds: &[u64] = if full {
        &HEAL_FULL_SEEDS
    } else {
        &default_seed
    };

    for &keys in keys_grid {
        for &conflict_fraction in fractions {
            for &seed in seeds {
                let decomposed = run_partition_heal(HealConfig::new(
                    seed,
                    Variant::Decomposed,
                    keys,
                    conflict_fraction,
                ));
                let merged = run_partition_heal(HealConfig::new(
                    seed,
                    Variant::Merged,
                    keys,
                    conflict_fraction,
                ));

                for metrics in [&decomposed, &merged] {
                    assert_eq!(
                        metrics.lost_updates, 0,
                        "{}: keys={keys} conflict_fraction={conflict_fraction} seed={seed:#x} \
                         did not converge to the exact total",
                        metrics.variant
                    );
                    assert!(
                        metrics.ae_rounds <= HEAL_MAX_AE_ROUNDS,
                        "{}: keys={keys} conflict_fraction={conflict_fraction} seed={seed:#x} \
                         spent {} anti-entropy rounds, over the {HEAL_MAX_AE_ROUNDS} sanity bound",
                        metrics.variant,
                        metrics.ae_rounds
                    );
                }
                print_heal_comparison(&decomposed, &merged);
            }
        }
    }
}

/// `ClusterConfig::default`'s `ae_interval`: the tick
/// [`partition_heal_at_the_default_ae_interval`] runs at.
const HEAL_DEFAULT_AE_INTERVAL_MS: u64 = 200;

/// [`partition_heal_comparison`] at production's default 200 ms
/// anti-entropy tick instead of [`HEAL_AE_INTERVAL_MS`]'s race-free one
/// second. At this cadence a node's next round can start while a peer's
/// multi-batch repair is still landing, so the rounds interleave differently
/// from run to run; the test asserts only what holds under any
/// interleaving: both variants converge to the exact total within
/// [`HEAL_MAX_AE_ROUNDS`]. It prints the same `SIM` lines as
/// [`partition_heal_comparison`] for reading the round and byte ratios at
/// this cadence, and `SUNDOG_SIM_FULL=1` widens the grid the same way.
#[test]
fn partition_heal_at_the_default_ae_interval() {
    let full = heal_sim_full();
    let keys_grid: &[u32] = if full {
        &HEAL_FULL_KEYS
    } else {
        &[HEAL_DEFAULT_KEYS]
    };
    let fractions: &[f64] = if full {
        &HEAL_FULL_CONFLICT_FRACTIONS
    } else {
        &HEAL_DEFAULT_CONFLICT_FRACTIONS
    };
    let default_seed = [sim_seed(0xC0DE_7400)];
    let seeds: &[u64] = if full {
        &HEAL_FULL_SEEDS
    } else {
        &default_seed
    };
    let at_default_tick = |seed, variant, keys, conflict_fraction| HealConfig {
        ae_interval_ms: HEAL_DEFAULT_AE_INTERVAL_MS,
        ..HealConfig::new(seed, variant, keys, conflict_fraction)
    };

    for &keys in keys_grid {
        for &conflict_fraction in fractions {
            for &seed in seeds {
                let decomposed = run_partition_heal(at_default_tick(
                    seed,
                    Variant::Decomposed,
                    keys,
                    conflict_fraction,
                ));
                let merged = run_partition_heal(at_default_tick(
                    seed,
                    Variant::Merged,
                    keys,
                    conflict_fraction,
                ));
                for metrics in [&decomposed, &merged] {
                    assert_eq!(
                        metrics.lost_updates, 0,
                        "{}: keys={keys} conflict_fraction={conflict_fraction} seed={seed:#x} \
                         did not converge to the exact total at a {HEAL_DEFAULT_AE_INTERVAL_MS} ms tick",
                        metrics.variant
                    );
                    assert!(
                        metrics.ae_rounds <= HEAL_MAX_AE_ROUNDS,
                        "{}: keys={keys} conflict_fraction={conflict_fraction} seed={seed:#x} \
                         spent {} anti-entropy rounds at a {HEAL_DEFAULT_AE_INTERVAL_MS} ms tick, \
                         over the {HEAL_MAX_AE_ROUNDS} sanity bound",
                        metrics.variant,
                        metrics.ae_rounds
                    );
                }
                print_heal_comparison(&decomposed, &merged);
            }
        }
    }
}

/// The same config, run twice in one process, must yield identical metrics
/// on every field `run_partition_heal`'s own simulation determines:
/// `run_partition_heal`'s per-run namespacing (see [`heal_run_id`]) is what
/// makes two calls in one process share nothing, so nothing about the
/// second run's `Sim`, shards, or counters can be influenced by the
/// first's. `frames`/`bytes` are the one deliberate exception: they read
/// `sundog::net`'s process-wide wire counters (see
/// [`HEAL_WIRE_METRICS_LOCK`]'s own doc), so under `cargo test`'s default
/// parallelism a wholly unrelated sim test running concurrently on another
/// thread can add to those same two counters mid-window -- a real source of
/// noise this test's own two `run_partition_heal` calls cannot serialize
/// against, since it comes from other tests entirely. Every field this
/// harness itself controls is still asserted equal.
#[test]
fn partition_heal_is_deterministic_for_a_fixed_config() {
    let cfg = HealConfig::new(
        sim_seed(0xC0DE_7300),
        Variant::Merged,
        HEAL_DEFAULT_KEYS,
        1.0,
    );
    let first = run_partition_heal(cfg);
    let second = run_partition_heal(cfg);
    let msg = "the same partition-heal config run twice in one process must yield identical \
               metrics on every field this harness itself controls (frames/bytes excepted -- \
               see this test's own doc)";
    assert_eq!(
        (
            first.variant,
            first.keys,
            first.conflict_fraction,
            first.seed
        ),
        (
            second.variant,
            second.keys,
            second.conflict_fraction,
            second.seed
        ),
        "{msg}"
    );
    assert_eq!(
        (
            first.ae_rounds,
            first.virtual_ms,
            first.records,
            first.applies
        ),
        (
            second.ae_rounds,
            second.virtual_ms,
            second.records,
            second.applies
        ),
        "{msg}"
    );
    assert_eq!(
        (
            first.folds,
            first.redundant_pulls,
            first.expected_total,
            first.actual_total,
            first.lost_updates,
        ),
        (
            second.folds,
            second.redundant_pulls,
            second.expected_total,
            second.actual_total,
            second.lost_updates,
        ),
        "{msg}"
    );
}

/// Pins the "Findings" section's own non-monotonic round-count signature:
/// the harness artifact that reimplemented anti-entropy produced
/// non-monotonic round counts as key count grew (3 rounds at 8,000 keys,
/// 19 at 16,000, 5 at 20,000). Driving rounds through the production
/// `run_round_against` entry point instead makes rounds monotone
/// non-decreasing in key count, at fixed full conflict, for both variants.
#[test]
fn partition_heal_rounds_are_monotone_in_key_count() {
    let seed = sim_seed(0xC0DE_7400);
    for variant in [Variant::Decomposed, Variant::Merged] {
        let mut previous = 0u64;
        for &keys in &HEAL_MONOTONIC_KEYS {
            let metrics = run_partition_heal(HealConfig::new(seed, variant, keys, 1.0));
            assert_eq!(
                metrics.lost_updates,
                0,
                "{}: keys={keys} did not converge to the exact total",
                variant.tag()
            );
            assert!(
                metrics.ae_rounds >= previous,
                "{}: ae_rounds regressed from {previous} at a smaller key count to {} at \
                 keys={keys} -- rounds must be monotone non-decreasing in key count",
                variant.tag(),
                metrics.ae_rounds
            );
            previous = metrics.ae_rounds;
        }
    }
}

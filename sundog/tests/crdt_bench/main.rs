//! CRDT merge resolver benchmark: real, in-process, loopback `Cluster`s,
//! public API only. Not a correctness suite; it measures and prints.
//!
//! Gated on `SUNDOG_BENCH=1`, an `eprintln!` and early return otherwise, so
//! a plain `cargo test` run still compiles without the wall-clock cost.
//! Every scenario shares the process-wide wire counters in `sundog::net`,
//! so run this binary single-threaded:
//!
//! ```text
//! SUNDOG_BENCH=1 cargo test --release -p sundog --test crdt_bench \
//!     -- --test-threads=1 --nocapture
//! ```
//!
//! With `--features prometheus`, every `BENCH` line also carries a
//! `sundog_ae_repaired_total` scrape for its cache(s):
//!
//! ```text
//! SUNDOG_BENCH=1 cargo test --release -p sundog --features prometheus \
//!     --test crdt_bench -- --test-threads=1 --nocapture
//! ```
//!
//! `WRITERS` (`SUNDOG_BENCH_WRITERS`, default 8), `ITERS`
//! (`SUNDOG_BENCH_ITERS`, default 200) and the repetition count
//! (`SUNDOG_BENCH_REPS`, default 3) are all env-overridable; the defaults
//! finish every scenario in well under a minute. Scenarios 7, 8, and 9
//! scale a third axis instead, an entity or batch count `N`
//! (`SUNDOG_BENCH_KEYS`); scenarios 7 and 8 fix the writer count at 3 (one
//! per warm-cluster node) and each writer's contribution to each entity at
//! a single increment, so `N` alone governs their cost, and scenario 9
//! reuses the same knob for its own batch size, no network or writer count
//! involved. Their defaults are set low (documented at each default's
//! definition) to keep every variant inside a 3-minute budget on a 4-core
//! box. A smoke run:
//!
//! ```text
//! SUNDOG_BENCH=1 SUNDOG_BENCH_WRITERS=2 SUNDOG_BENCH_ITERS=50 \
//!     cargo test --release -p sundog --features prometheus \
//!     --test crdt_bench -- --test-threads=1 --nocapture
//! ```
//!
//! Each `BENCH` line is one `key=value`-per-metric record, `grep`able.
//! Scenarios 1-4 and 6 build a fresh 3-node `Replicated` cluster per
//! repetition (identical `fast_config()` topology throughout: 150ms AE
//! interval, 2s tombstone TTL); every writer targets a single cache handle
//! (`cache_a`), matching `replication_bench.rs`'s own concurrent-writer
//! shape. Scenarios 5/5a/5b build a single-node `Mode::Local` cluster with
//! no peers, isolating per-apply CPU cost from all network/AE noise. No
//! key ever expires or is removed in any scenario, since TTL is
//! irrelevant here. Every numeric field is the median of at least
//! [`support::repetitions`] independent runs, so a single noisy run never skews a
//! reported number.
//!
//! Scenarios 9-12 isolate a merging resolver's two write-path levers
//! through the real `CacheBuilder::prefold_enabled` and
//! `merge_coalesce_window` toggles rather than approximating either: Lever
//! A (`Engine::apply_many`'s batch pre-fold, `sundog/src/store/engine.rs`)
//! in scenarios 9 and 10, Lever B (`Cache::merge`'s coalescing window) in
//! scenarios 11 and 12. All four report "engine applies" as
//! `Cache::events()`'s own count: one event per non-no-op apply, the only
//! public-API signal for how many times the engine applied, per
//! `Cache::merge`'s and the resolver contract's own docs.
//!
//! Scenarios 7-14 each carry their full rationale on their own `run_*` or
//! `print_*` function; this is only an index:
//!
//! - Scenario 7, `cold_join_initial_replication`: times a fourth node's
//!   join against an already-converged warm 3-node trio.
//! - Scenario 8, `large_entity_convergence`: times convergence of `N`
//!   concurrently written entities under live anti-entropy.
//! - Scenario 9, `apply_many_prefold`: Lever A on the local apply path,
//!   isolated from the network on a single-node cluster.
//! - Scenario 10, `replicated_hot_counter_receive`: Lever A on the
//!   replication receive path instead of the local apply path.
//! - Scenario 11, `merged_counter_coalesced`: Lever B across three
//!   coalescing window sizes.
//! - Scenario 12, `large_entity_convergence_coalesced`: scenario 8's
//!   `merged` variant with Lever B's coalescing window replacing `insert`.
//! - Scenario 13, `sketch_path_convergence`: forces a deliberate bucket
//!   mismatch onto the IBLT sketch path (`sundog_ae_sketch_total`).
//! - Scenario 14, `crdt_compaction_writer_churn`: churns one writer
//!   role's `NodeId` incarnation and times the compaction sweep's
//!   retirement of the dead incarnations.

#[path = "../common/mod.rs"]
mod common;

fn bench_enabled() -> bool {
    std::env::var("SUNDOG_BENCH").as_deref() == Ok("1")
}

/// Returns from the calling scenario fn unless `SUNDOG_BENCH=1` is set.
macro_rules! require_bench {
    () => {
        if !$crate::bench_enabled() {
            eprintln!("skipping: SUNDOG_BENCH=1 not set");
            return;
        }
    };
}

mod churn;
mod coalesce;
mod prefold;
mod scale;
mod scrape;
mod sketch_path;
mod support;
mod write_path;

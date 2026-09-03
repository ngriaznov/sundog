//! Coverage-guided, sequence-driven fuzzing of everything after a
//! successful wire decode: the versioned apply, digest bookkeeping,
//! tombstone retention, expiry, and the resolver. An `Arbitrary`-generated
//! `Vec<Op>` (capped at 256 ops) is replayed through `store::model::run`
//! against a fresh shard and its paired reference model side by side; `run`
//! panics on the first divergence.

#![no_main]

use libfuzzer_sys::fuzz_target;
use sundog::store::model::{self, Op};

/// Matches [`model::run`]'s own docs on why a stateful sequence is capped —
/// a realistic bound on one fuzz iteration's work, not a correctness limit.
const MAX_OPS: usize = 256;

fuzz_target!(|ops: Vec<Op>| {
    let ops = &ops[..ops.len().min(MAX_OPS)];
    let (shard, mut model) = model::new_shard_and_model("fuzz-apply-model", 1);
    model::run(ops, &shard, &mut model);
});

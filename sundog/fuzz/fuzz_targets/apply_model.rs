//! Coverage-guided, sequence-driven fuzzing of the versioned apply, digest
//! bookkeeping, tombstone retention, expiry, and the resolver. An
//! `Arbitrary`-generated `Vec<Op>` replays through `store::model::run`.

#![no_main]

use libfuzzer_sys::fuzz_target;
use sundog::store::model::{self, Op};

/// A realistic bound on one fuzz iteration's work, not a correctness limit.
const MAX_OPS: usize = 256;

fuzz_target!(|ops: Vec<Op>| {
    let ops = &ops[..ops.len().min(MAX_OPS)];
    let (shard, mut model) = model::new_shard_and_model("fuzz-apply-model", 1);
    model::run(ops, &shard, &mut model);
});

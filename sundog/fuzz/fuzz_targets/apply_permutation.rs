//! The permutation-convergence invariant under coverage guidance, rather
//! than proptest's random sampling: an `Arbitrary` set of remote records,
//! stamped once into `WireRecord`s, is duplicated and shuffled two
//! different ways (`seed_a`, `seed_b`) and fed to two fresh shards through
//! `ShardOps::apply_remote_batch`. Whatever order and duplication either
//! shard saw, both must converge to identical digests and an identical
//! `entries_for_buckets` set.
//!
//! The convergence guarantee assumes what `ConflictResolver`'s docs state:
//! a given `(wall_ms, logical, node)` triple is produced by at most one
//! write ever, so an equal-version pair is always a duplicate of the exact
//! same record, never two different ones racing. A real `HlcClock` upholds
//! that; `RemoteRecord`'s directly-sampled `wall_ms_offset`/`logical`/`node`
//! fields don't — two distinct `Arbitrary`-generated records can land on
//! the identical `Hlc` by chance, and *then* which one a permutation
//! applies last (winning the tie) legitimately differs by order, which
//! isn't the bug this target hunts for. [`unique_wire_record`] closes that
//! gap by folding each record's position in the base list into `logical`,
//! so only true duplicates (the exact same struct, cloned by
//! [`shuffled_with_duplicates`]) can ever share a version.

#![no_main]

use std::collections::HashSet;

use arbitrary::Arbitrary;
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use sundog::hlc::Hlc;
use sundog::store::model::{self, RemoteRecord};
use sundog::store::{BUCKET_COUNT, Shard, ShardOps};
use sundog::wire::WireRecord;

/// Matches `apply_model`'s op cap: a realistic bound on one iteration's
/// work, not a correctness limit.
const MAX_RECORDS: usize = 128;
/// A single fixed clock reading every record in this target stamps
/// relative to — permutation convergence doesn't need clock movement, only
/// [`model::RemoteRecord`]'s offsets need *some* baseline.
const NOW_MS: u64 = model::START_CLOCK_MS;

#[derive(Debug, Arbitrary)]
struct Input {
    records: Vec<RemoteRecord>,
    seed_a: u64,
    seed_b: u64,
}

/// xorshift64* — a tiny, allocation-free, deterministic generator (the same
/// algorithm `engine::Engine`'s own eviction sampling uses), so this target
/// needs no RNG dependency just to permute and duplicate a short record
/// list.
fn next_u64(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

/// Duplicates every record 1-3 times and shuffles the result, deterministic
/// given `seed` — the same shape as the in-crate `permutation_convergence`
/// property test's `shuffled_with_duplicates`, minus its dependency on
/// `rand`.
fn shuffled_with_duplicates(records: &[WireRecord], seed: u64) -> Vec<WireRecord> {
    let mut state = seed | 1; // xorshift64* never recovers from a zero state
    let mut expanded = Vec::with_capacity(records.len() * 2);
    for rec in records {
        let copies = 1 + next_u64(&mut state) % 3;
        for _ in 0..copies {
            expanded.push(rec.clone());
        }
    }
    let len = expanded.len();
    for i in (1..len).rev() {
        let bound = i as u64 + 1;
        let j = usize::try_from(next_u64(&mut state) % bound).expect("bound fits usize");
        expanded.swap(i, j);
    }
    expanded
}

/// Builds the [`WireRecord`] for `record` at `index` in the base list,
/// folding `index` into [`Hlc::logical`] — see this module's docs on why
/// that's required for the permutation-convergence check to be sound
/// against arbitrary, not-necessarily-`HlcClock`-produced input.
fn unique_wire_record(index: usize, record: &RemoteRecord, now_ms: u64) -> WireRecord {
    let mut rec = model::remote_wire_record(record, now_ms);
    rec.ver.logical = (rec.ver.logical << 8) | (index as u32 & 0xFF);
    rec
}

fn digests(shard: &Shard<u8, u8>) -> Vec<u64> {
    futures::executor::block_on(ShardOps::digests(shard))
        .into_iter()
        .map(|(_, digest)| digest)
        .collect()
}

fn entry_set(shard: &Shard<u8, u8>) -> HashSet<(Bytes, Hlc)> {
    let all_buckets: Vec<u16> = (0..u16::try_from(BUCKET_COUNT).expect("fits")).collect();
    futures::executor::block_on(ShardOps::entries_for_buckets(shard, all_buckets))
        .into_iter()
        .flat_map(|(_, entries)| entries)
        .collect()
}

fuzz_target!(|input: Input| {
    let records = &input.records[..input.records.len().min(MAX_RECORDS)];
    if records.is_empty() {
        return;
    }
    let base: Vec<WireRecord> = records
        .iter()
        .enumerate()
        .map(|(index, r)| unique_wire_record(index, r, NOW_MS))
        .collect();

    let (shard_a, _) = model::new_shard_and_model("fuzz-apply-permutation-a", 1);
    let (shard_b, _) = model::new_shard_and_model("fuzz-apply-permutation-b", 2);

    let order_a = shuffled_with_duplicates(&base, input.seed_a);
    let order_b = shuffled_with_duplicates(&base, input.seed_b);

    futures::executor::block_on(ShardOps::apply_remote_batch(&shard_a, order_a));
    futures::executor::block_on(ShardOps::apply_remote_batch(&shard_b, order_b));

    assert_eq!(
        digests(&shard_a),
        digests(&shard_b),
        "digests diverged across permutation/duplication of the same record multiset"
    );
    assert_eq!(
        entry_set(&shard_a),
        entry_set(&shard_b),
        "entries_for_buckets diverged across permutation/duplication of the same record multiset"
    );
});

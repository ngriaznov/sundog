//! Regression inputs of the weekly-fuzz `apply_permutation` target, replayed
//! deterministically: the same records, stamped and shuffled as the target
//! does, applied to two fresh `Mode::Replicated` shards from
//! [`model::new_shard_and_model`], which end with equal digests and equal
//! `entries_for_buckets` sets.

use std::collections::HashSet;

use bytes::Bytes;

use super::model::{self, RemoteRecord};
use super::{BUCKET_COUNT, Shard, ShardOps, WireRecord};
use crate::hlc::Hlc;

/// The clock reading every record stamps against, as in the target.
const NOW_MS: u64 = model::START_CLOCK_MS;

/// xorshift64*, as in the target.
fn next_u64(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

/// Duplicates every record 1-3 times and shuffles the result under `seed`,
/// as the target does.
fn shuffled_with_duplicates(records: &[WireRecord], seed: u64) -> Vec<WireRecord> {
    let mut state = seed | 1;
    let mut expanded = Vec::with_capacity(records.len() * 2);
    for rec in records {
        let copies = 1 + next_u64(&mut state) % 3;
        for _ in 0..copies {
            expanded.push(rec.clone());
        }
    }
    for i in (1..expanded.len()).rev() {
        let bound = i as u64 + 1;
        let j = usize::try_from(next_u64(&mut state) % bound).expect("bound fits usize");
        expanded.swap(i, j);
    }
    expanded
}

/// The [`WireRecord`] for `record` at `index`, with `index` folded into the
/// version's logical counter so only true duplicates share a version.
fn unique_wire_record(index: usize, record: &RemoteRecord) -> WireRecord {
    let mut rec = model::remote_wire_record(record, NOW_MS);
    let position = u32::try_from(index & 0xFF).expect("a byte fits u32");
    rec.ver.logical = (rec.ver.logical << 8) | position;
    rec
}

fn digests(shard: &Shard<u8, u8>) -> Vec<u64> {
    futures::executor::block_on(ShardOps::digests(shard))
        .into_iter()
        .map(|bd| bd.digest)
        .collect()
}

fn entry_set(shard: &Shard<u8, u8>) -> HashSet<(Bytes, Hlc)> {
    let all_buckets: Vec<u16> = (0..u16::try_from(BUCKET_COUNT).expect("fits")).collect();
    futures::executor::block_on(ShardOps::entries_for_buckets(shard, all_buckets))
        .into_iter()
        .flat_map(|(_, entries)| entries)
        .map(|kv| (kv.key, kv.version))
        .collect()
}

/// The target's two fresh shards, fed `records` duplicated and shuffled under
/// `seed_a` and `seed_b`, and its assertions on them. Returns the shards.
fn converge(records: &[RemoteRecord], seed_a: u64, seed_b: u64) -> (Shard<u8, u8>, Shard<u8, u8>) {
    let base: Vec<WireRecord> = records
        .iter()
        .enumerate()
        .map(|(index, r)| unique_wire_record(index, r))
        .collect();
    let (shard_a, _) = model::new_shard_and_model("regression-apply-permutation-a", 1);
    let (shard_b, _) = model::new_shard_and_model("regression-apply-permutation-b", 2);
    futures::executor::block_on(ShardOps::apply_remote_batch(
        &shard_a,
        shuffled_with_duplicates(&base, seed_a),
    ));
    futures::executor::block_on(ShardOps::apply_remote_batch(
        &shard_b,
        shuffled_with_duplicates(&base, seed_b),
    ));
    assert!(
        digests(&shard_a) == digests(&shard_b),
        "digests diverged across permutation/duplication of the same record multiset"
    );
    assert!(
        entry_set(&shard_a) == entry_set(&shard_b),
        "entries_for_buckets diverged across permutation/duplication of the same record multiset"
    );
    (shard_a, shard_b)
}

/// The tombstone of key 255's entry, which expired `expired_ago_ms` before
/// now, and an older value for the same key.
fn expiry_tombstone_and_an_older_value(expired_ago_ms: i16) -> Vec<RemoteRecord> {
    vec![
        RemoteRecord {
            key: 255,
            value: None,
            wall_ms_offset: -2087,
            logical: 255,
            node: 217,
            expires_offset_ms: Some(-expired_ago_ms),
        },
        RemoteRecord {
            key: 255,
            value: Some(255),
            wall_ms_offset: -9745,
            logical: 1,
            node: 0,
            expires_offset_ms: None,
        },
    ]
}

/// The fuzz crash: the tombstone's retention (its `ttl` and its `max`
/// deadline both counted from an expiry 2,253 ms ago) has lapsed before it
/// arrives, so it is dropped in either order and the older value stays.
#[test]
fn a_lapsed_expiry_tombstone_and_an_older_value_converge_in_any_order() {
    let records = expiry_tombstone_and_an_older_value(2_253);
    let (a, b) = converge(&records, 17, 0);
    for shard in [&a, &b] {
        assert_eq!(
            futures::executor::block_on(shard.get(&255)),
            Some(255),
            "a lapsed expiry tombstone is a no-op whatever the key holds"
        );
        assert_eq!(shard.tombstone_count(), 0);
    }
}

/// An expiry tombstone past its `ttl` deadline but within its `max` one is
/// kept by a new shard, which defers collection until a collection says
/// otherwise: it deletes the older value in either order.
#[test]
fn an_expiry_tombstone_within_a_new_shards_deferral_is_kept_in_any_order() {
    let records = expiry_tombstone_and_an_older_value(1_000);
    let (a, b) = converge(&records, 17, 0);
    for shard in [&a, &b] {
        assert_eq!(
            futures::executor::block_on(shard.get(&255)),
            None,
            "the tombstone is newer than the value and deletes it"
        );
        assert_eq!(shard.tombstone_count(), 1);
    }
}

/// Every order of the pair, not just the target's two shuffles, converges
/// for each retention state: lapsed past `max`, past `ttl` only, and within
/// `ttl`.
#[test]
fn the_pair_converges_under_every_shuffle_for_each_retention_state() {
    for expired_ago_ms in [2_253, 1_000, 100] {
        let records = expiry_tombstone_and_an_older_value(expired_ago_ms);
        for seed_a in 0..16 {
            for seed_b in 0..16 {
                let _ = converge(&records, seed_a, seed_b);
            }
        }
    }
}

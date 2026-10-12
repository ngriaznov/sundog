//! Tests for `Engine::compact`: cursor rotation, lock budget and spilled entries.

use super::*;
use crate::store::crdt::{PnCounter, PnCounterResolver, WriterId};

/// The smallest key whose hash lands in stripe `target`. Exists for
/// every `target < BUCKET_COUNT`: `stripe_index_from_hash` hashes
/// over the full `u32` key space, uniformly enough that some small
/// key lands in every stripe.
fn key_for_stripe(target: usize) -> u32 {
    (0u32..1 << 20)
        .find(|&k| stripe_index_from_hash(hash_key_bytes(key_bytes(k).as_ref())) == target)
        .expect("some u32 key well within range hashes into every stripe")
}

fn writer(id: u64) -> WriterId {
    WriterId::new(NodeId::from(id), 1)
}

/// `count` distinct keys, all landing in stripe `target`.
fn same_stripe_keys(target: usize, count: usize) -> Vec<u32> {
    (0u32..1 << 20)
        .filter(|&k| stripe_index_from_hash(hash_key_bytes(key_bytes(k).as_ref())) == target)
        .take(count)
        .collect()
}

fn seed_counter(
    engine: &Engine<u32, PnCounter>,
    resolver: PnCounterResolver,
    key: u32,
    writer_id: WriterId,
) {
    let kb = key_bytes(key);
    let _ = put_with_resolver(
        engine,
        key,
        kb,
        PnCounter::local_delta(writer_id, 1),
        hlc(1, 1),
        None,
        0,
        &resolver,
    );
}

/// Four resident counters, each alone in its own stripe, each
/// carrying one retirement-eligible writer. One entry per
/// [`Engine::compact`] call, cursor pinned to `0` beforehand: each
/// call must return exactly the one entry sitting between the
/// cursor and the next seeded stripe, in ascending stripe order,
/// and leave the cursor immediately past the stripe it visited,
/// proving rotation carries forward across calls instead of
/// restarting at stripe `0` (which would instead return the same
/// first entry every time) or skipping ahead arbitrarily.
#[test]
fn compact_rotates_the_cursor_across_stripes_between_calls() {
    let engine = Engine::<u32, PnCounter>::new(u64::MAX, None, None, None);
    let resolver = PnCounterResolver;
    let targets = [3usize, 200, 500, 900];
    let keys: Vec<u32> = targets.iter().map(|&t| key_for_stripe(t)).collect();
    for (i, &key) in keys.iter().enumerate() {
        seed_counter(&engine, resolver, key, writer(u64::try_from(i).unwrap()));
    }

    engine.debug_set_compact_cursor(0);
    let retire_everyone = |_: WriterId| true;
    let mut seen = Vec::new();
    for &target in &targets {
        let out = engine
            .compact(
                &resolver,
                1_000,
                &retire_everyone,
                Quiescence::Churning,
                crate::store::CompactionBounds::three_bounds(0),
                1,
            )
            .candidates;
        assert_eq!(
            out.len(),
            1,
            "exactly one seeded entry sits between the cursor and the next target"
        );
        seen.push(out[0].key);
        assert_eq!(
            engine.debug_compact_cursor(),
            u64::try_from((target + 1) % BUCKET_COUNT).unwrap(),
            "the cursor advances to just past the stripe it last visited"
        );
    }
    assert_eq!(
        seen, keys,
        "four calls visit the four seeded stripes in rotation, none repeated, none \
         skipped"
    );
}

/// A single entry seeded in stripe `5`, cursor pinned to stripe
/// `0`: one [`Engine::compact`] call with `max_entries: 1` must
/// stop the instant its budget is met, locking stripes `0..=5`
/// (six stripes) and no more, never the whole 1024-stripe engine,
/// and never fewer than it needed to find the one entry.
/// Each stripe's lock is taken and dropped before the next is
/// touched (`Engine::compact`'s own structure never calls
/// `self.stripes[idx].read()` a second time before the previous
/// guard is dropped), so this count is exactly the number of
/// distinct stripes visited, one lock each.
#[test]
fn compact_locks_only_as_many_stripes_as_its_budget_needs() {
    let engine = Engine::<u32, PnCounter>::new(u64::MAX, None, None, None);
    let resolver = PnCounterResolver;
    let key = key_for_stripe(5);
    seed_counter(&engine, resolver, key, writer(1));

    engine.debug_set_compact_cursor(0);
    let retire_everyone = |_: WriterId| true;
    let before = engine.debug_compact_lock_acquisitions();
    let out = engine
        .compact(
            &resolver,
            1_000,
            &retire_everyone,
            Quiescence::Churning,
            crate::store::CompactionBounds::three_bounds(0),
            1,
        )
        .candidates;
    let after = engine.debug_compact_lock_acquisitions();

    assert_eq!(out.len(), 1);
    assert_eq!(
        after - before,
        6,
        "stripes 0..=5 are visited (the entry sits in stripe 5) — not the whole engine"
    );
}

/// One entry seeded into a stripe an inflated [`Engine::new`]
/// `capacity_hint` presized far past what it ever held:
/// [`Engine::compact`]'s visit to that stripe must call
/// [`should_shrink_stripe`] and, finding `1 <= 100 / 8`, shrink the
/// slab back down toward its one live entry instead of leaving it
/// at its original oversized allocation.
#[test]
fn compact_shrinks_an_overallocated_stripe_toward_its_len() {
    let target = 7usize;
    let per_stripe_capacity = 100usize;
    let hint = per_stripe_capacity as u64 * BUCKET_COUNT as u64;
    let engine = Engine::<u32, PnCounter>::new(u64::MAX, None, None, Some(hint));
    assert_eq!(
        engine.stripe_capacities()[target],
        per_stripe_capacity,
        "the hint presizes every stripe, including this one, before any write"
    );

    let resolver = PnCounterResolver;
    let key = key_for_stripe(target);
    seed_counter(&engine, resolver, key, writer(1));

    engine.debug_set_compact_cursor(target as u64);
    let retire_everyone = |_: WriterId| true;
    let _ = engine.compact(
        &resolver,
        1_000,
        &retire_everyone,
        Quiescence::Churning,
        crate::store::CompactionBounds::three_bounds(0),
        1,
    );

    let shrunk = engine.stripe_capacities()[target];
    assert!(
        shrunk < per_stripe_capacity,
        "one entry in a 100-capacity stripe sits well under the eighth-capacity \
         threshold, so the visit shrinks it: got {shrunk}"
    );
    assert!(
        shrunk <= 8,
        "shrink_to_fit brings the arena back down toward its one live entry, not only \
         partway: got {shrunk}"
    );
}

/// Four entries seeded into an 8-capacity stripe: `4 > 8 / 8`, so
/// [`should_shrink_stripe`] says no.
#[test]
fn compact_leaves_a_well_utilized_stripe_alone() {
    let target = 9usize;
    let per_stripe_capacity = 8usize;
    let hint = per_stripe_capacity as u64 * BUCKET_COUNT as u64;
    let engine = Engine::<u32, PnCounter>::new(u64::MAX, None, None, Some(hint));

    let resolver = PnCounterResolver;
    let keys = same_stripe_keys(target, 4);
    assert_eq!(keys.len(), 4, "four distinct keys land in this stripe");
    for (i, &k) in keys.iter().enumerate() {
        seed_counter(&engine, resolver, k, writer(u64::try_from(i).unwrap()));
    }
    assert_eq!(engine.stripe_capacities()[target], per_stripe_capacity);

    engine.debug_set_compact_cursor(target as u64);
    let retire_everyone = |_: WriterId| true;
    let _ = engine.compact(
        &resolver,
        1_000,
        &retire_everyone,
        Quiescence::Churning,
        crate::store::CompactionBounds::three_bounds(0),
        4,
    );

    assert_eq!(
        engine.stripe_capacities()[target],
        per_stripe_capacity,
        "4 of 8 slots filled sits above the eighth-capacity shrink threshold, so the \
         visit leaves this stripe's allocation untouched"
    );
}

/// A resident entry and a spilled one, both carrying a
/// retirement-eligible writer: only the resident entry is ever
/// handed to [`ConflictResolver::compact`]. A spilled payload has
/// no bytes in RAM for a resolver to read without a disk read this
/// sweep does not pay for; it must be silently skipped, not
/// errored or panicked on, and must never suppress the resident
/// candidate sharing the pass.
#[cfg(feature = "spill")]
#[test]
fn compact_never_hands_a_spilled_payload_to_the_resolver() {
    let engine = Engine::<u32, PnCounter>::new(u64::MAX, None, None, None);
    let resolver = PnCounterResolver;

    let spilled_key = 1u32;
    let spilled_kb = key_bytes(spilled_key);
    engine.debug_insert_spilled(
        &spilled_kb,
        hlc(1, 1),
        None,
        SpillLoc {
            region: 0,
            offset: 0,
            len: 4,
            generation: 0,
        },
        0,
    );

    let resident_key = 2u32;
    seed_counter(&engine, resolver, resident_key, writer(1));

    engine.debug_set_compact_cursor(0);
    let retire_everyone = |_: WriterId| true;
    let out = engine
        .compact(
            &resolver,
            1_000,
            &retire_everyone,
            Quiescence::Churning,
            crate::store::CompactionBounds::three_bounds(0),
            usize::MAX,
        )
        .candidates;

    assert_eq!(
        out.len(),
        1,
        "the spilled entry is skipped; only the resident one is a candidate"
    );
    assert_eq!(out[0].key, resident_key);
}

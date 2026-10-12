//! Unit tests for `Slab`, its entries and the arena growth rule.

use super::expiry::NEVER_EXPIRES;
use super::*;

fn key_bytes(key: u32) -> Bytes {
    Bytes::from(postcard::to_stdvec(&key).expect("u32 encodes"))
}

fn hash_of(key: u32) -> u64 {
    hash_key_bytes(key_bytes(key).as_ref())
}

fn eq_for(key: u32) -> impl FnMut(&Live<u32, Vec<u8>>) -> bool {
    move |l: &Live<u32, Vec<u8>>| record_key(&l.record) == key_bytes(key).as_ref()
}

fn live_for(key: u32, value: &[u8]) -> Live<u32, Vec<u8>> {
    Live::new(
        build_resident_record(key_bytes(key).as_ref(), value),
        Hlc {
            wall_ms: 1,
            logical: 0,
            node: NodeId::from(1),
        },
        NEVER_EXPIRES,
        1,
        0,
        #[cfg(feature = "spill")]
        EntryState::Resident,
    )
}

#[test]
fn slab_find_locates_inserted_key() {
    let mut slab: Slab<u32, Vec<u8>> = Slab::new();
    let hash = hash_of(7);
    slab.insert_unique(hash, live_for(7, b"v7"), hasher_for);
    let found = slab
        .find(hash, eq_for(7))
        .expect("just-inserted key is findable");
    assert_eq!(record_value(&found.record), b"v7");
}

#[test]
fn arena_growth_adds_a_quarter_and_at_least_the_minimum() {
    assert_eq!(arena_growth(0), ARENA_MIN_GROWTH);
    assert_eq!(arena_growth(8), ARENA_MIN_GROWTH);
    assert_eq!(arena_growth(16), 4);
    assert_eq!(arena_growth(100), 25);
    assert_eq!(arena_growth(1_000_000), 250_000);
}

#[test]
fn slab_arena_stays_four_fifths_full_as_it_grows() {
    let mut slab: Slab<u32, Vec<u8>> = Slab::new();
    for key in 0..5_000u32 {
        slab.insert_unique(hash_of(key), live_for(key, b"v"), hasher_for);
        let len = slab.len();
        assert!(
            slab.capacity() <= len + arena_growth(len.saturating_sub(1)),
            "at {len} entries the arena holds {} slots",
            slab.capacity()
        );
        if len >= 20 {
            assert!(
                slab.capacity() * 4 <= len * 5,
                "at {len} entries the arena holds {} slots, more than a quarter spare",
                slab.capacity()
            );
        }
    }
    for key in 0..5_000u32 {
        assert!(slab.find(hash_of(key), eq_for(key)).is_some());
    }
}

#[test]
fn slab_find_returns_none_for_absent_key() {
    let mut slab: Slab<u32, Vec<u8>> = Slab::new();
    slab.insert_unique(hash_of(1), live_for(1, b"v1"), hasher_for);
    assert!(slab.find(hash_of(2), eq_for(2)).is_none());
}

#[test]
fn slab_insert_grows_entries_by_one_and_is_findable() {
    let mut slab: Slab<u32, Vec<u8>> = Slab::new();
    assert_eq!(slab.len(), 0);
    let hash = hash_of(3);
    let slot = slab.insert_unique(hash, live_for(3, b"v3"), hasher_for);
    assert_eq!(slot, 0);
    assert_eq!(slab.len(), 1);
    assert!(slab.find(hash, eq_for(3)).is_some());
    let live = slab
        .find_mut(hash, eq_for(3))
        .expect("just-inserted key is mutably findable");
    live.weight = 42;
    assert_eq!(slab.find(hash, eq_for(3)).unwrap().weight, 42);
}

#[test]
fn slab_occupied_entry_get_mut_mutates_the_slot_visibly_through_find() {
    let mut slab: Slab<u32, Vec<u8>> = Slab::new();
    let hash = hash_of(9);
    slab.insert_unique(hash, live_for(9, b"v9"), hasher_for);
    let SlabEntry::Occupied(mut occ) = slab.entry(hash, eq_for(9), hasher_for) else {
        panic!("key 9 is present");
    };
    occ.get_mut().weight = 77;
    assert_eq!(
        slab.find(hash, eq_for(9)).unwrap().weight,
        77,
        "a mutation through get_mut is visible on a later find"
    );
}

#[test]
fn slab_vacant_entry_insert_makes_the_value_findable() {
    let mut slab: Slab<u32, Vec<u8>> = Slab::new();
    let hash = hash_of(11);
    let SlabEntry::Vacant(vacant) = slab.entry(hash, eq_for(11), hasher_for) else {
        panic!("key 11 is absent from an empty slab");
    };
    let slot = vacant.insert(live_for(11, b"v11"));
    assert_eq!(slot, 0, "the first insert lands in arena slot 0");
    assert_eq!(slab.len(), 1);
    let found = slab
        .find(hash, eq_for(11))
        .expect("the value inserted through a vacant entry is findable");
    assert_eq!(record_value(&found.record), b"v11");
}

#[test]
fn slab_remove_swaps_last_entry_into_freed_slot_and_fixes_its_index() {
    let mut slab: Slab<u32, Vec<u8>> = Slab::new();
    let (ha, hb, hc) = (hash_of(10), hash_of(20), hash_of(30));
    slab.insert_unique(ha, live_for(10, b"a"), hasher_for);
    slab.insert_unique(hb, live_for(20, b"b"), hasher_for);
    slab.insert_unique(hc, live_for(30, b"c"), hasher_for);
    assert_eq!(slab.len(), 3);

    // Removing slot 0 swap-moves key 30 into it; its index row must
    // repoint.
    let SlabEntry::Occupied(occ) = slab.entry(ha, eq_for(10), hasher_for) else {
        panic!("key 10 is present");
    };
    assert_eq!(record_value(&occ.get().record), b"a");
    let (removed, _vacant) = occ.remove();
    assert_eq!(record_value(&removed.record), b"a");

    assert_eq!(slab.len(), 2);
    let moved = slab
        .find(hc, eq_for(30))
        .expect("the swapped-in entry keeps its own findability at its own hash");
    assert_eq!(record_value(&moved.record), b"c");
    assert!(slab.find(hb, eq_for(20)).is_some());
    assert!(slab.find(ha, eq_for(10)).is_none());

    // Every remaining entry is still reachable from its own hash.
    let keys: Vec<u32> = slab
        .iter()
        .map(|live| decode_key(record_key(&live.record)))
        .collect();
    for key in keys {
        assert!(slab.find(hash_of(key), eq_for(key)).is_some());
    }
}

#[test]
fn slab_remove_of_the_last_slot_needs_no_fixup() {
    let mut slab: Slab<u32, Vec<u8>> = Slab::new();
    let (ha, hb) = (hash_of(1), hash_of(2));
    slab.insert_unique(ha, live_for(1, b"a"), hasher_for);
    slab.insert_unique(hb, live_for(2, b"b"), hasher_for);

    let SlabEntry::Occupied(occ) = slab.entry(hb, eq_for(2), hasher_for) else {
        panic!("key 2 is present");
    };
    let (removed, _vacant) = occ.remove();
    assert_eq!(record_value(&removed.record), b"b");
    assert_eq!(slab.len(), 1);
    assert!(slab.find(ha, eq_for(1)).is_some());
    assert!(slab.find(hb, eq_for(2)).is_none());
}

#[test]
fn slab_remove_of_absent_key_returns_none() {
    let mut slab: Slab<u32, Vec<u8>> = Slab::new();
    slab.insert_unique(hash_of(1), live_for(1, b"a"), hasher_for);
    assert!(slab.remove(hash_of(99), eq_for(99), hasher_for).is_none());
    assert_eq!(slab.len(), 1);
}

#[test]
fn slab_retain_keeps_dense_and_visits_every_removed_entry_once() {
    let mut slab: Slab<u32, Vec<u8>> = Slab::new();
    for key in 0..10u32 {
        slab.insert_unique(hash_of(key), live_for(key, b"v"), hasher_for);
    }
    let mut visited: Vec<u32> = Vec::new();
    slab.retain(hasher_for, |live| {
        let k: u32 = decode_key(record_key(&live.record));
        assert!(!visited.contains(&k), "key {k} visited more than once");
        visited.push(k);
        k.is_multiple_of(2)
    });
    visited.sort_unstable();
    assert_eq!(
        visited,
        (0..10).collect::<Vec<_>>(),
        "retain visits every entry exactly once"
    );
    assert_eq!(slab.len(), 5);
    for key in (0..10u32).step_by(2) {
        assert!(slab.find(hash_of(key), eq_for(key)).is_some());
    }
    for key in (1..10u32).step_by(2) {
        assert!(slab.find(hash_of(key), eq_for(key)).is_none());
    }
}

#[test]
fn slab_iter_visits_every_live_entry_exactly_once_in_some_order() {
    let mut slab: Slab<u32, Vec<u8>> = Slab::new();
    let keys: Vec<u32> = (0..7).collect();
    for &key in &keys {
        slab.insert_unique(hash_of(key), live_for(key, b"v"), hasher_for);
    }
    let mut seen: Vec<u32> = slab
        .iter()
        .map(|live| decode_key(record_key(&live.record)))
        .collect();
    seen.sort_unstable();
    assert_eq!(seen, keys);
}

#[test]
fn slab_drain_empties_and_returns_every_entry() {
    let mut slab: Slab<u32, Vec<u8>> = Slab::new();
    let keys: Vec<u32> = (0..5).collect();
    for &key in &keys {
        slab.insert_unique(hash_of(key), live_for(key, b"v"), hasher_for);
    }
    let mut drained: Vec<u32> = slab
        .drain()
        .map(|live| decode_key(record_key(&live.record)))
        .collect();
    drained.sort_unstable();
    assert_eq!(drained, keys);
    assert_eq!(slab.len(), 0);
    assert!(slab.find(hash_of(0), eq_for(0)).is_none());
}

#[test]
fn slab_with_capacity_presizes_entries_and_index_with_no_growth_up_to_hint() {
    let mut slab: Slab<u32, Vec<u8>> = Slab::with_capacity(64);
    assert!(slab.entries.capacity() >= 64);
    let entries_cap_before = slab.entries.capacity();
    for key in 0..64u32 {
        slab.insert_unique(hash_of(key), live_for(key, b"v"), hasher_for);
    }
    assert_eq!(
        slab.entries.capacity(),
        entries_cap_before,
        "no growth up to the hint"
    );
    assert_eq!(slab.len(), 64);
    for key in 0..64u32 {
        assert!(slab.find(hash_of(key), eq_for(key)).is_some());
    }

    // After removing all but one entry, shrink drops the oversized
    // allocation.
    for key in 1..64u32 {
        slab.remove(hash_of(key), eq_for(key), hasher_for);
    }
    assert_eq!(slab.len(), 1);
    slab.shrink_to_fit(hasher_for);
    assert!(slab.entries.capacity() < entries_cap_before);
    assert!(slab.find(hash_of(0), eq_for(0)).is_some());
}

#[test]
fn slab_random_permutation_insert_remove_matches_a_hashmap_mirror() {
    use rand::{RngExt as _, SeedableRng as _, rngs::StdRng};

    let mut slab: Slab<u32, Vec<u8>> = Slab::new();
    let mut mirror: HashMap<u32, u8> = HashMap::new();
    let mut rng = StdRng::seed_from_u64(0xDEAD_BEEF);

    for step in 0..2000u32 {
        let key = rng.random_range(0..64u32);
        let hash = hash_of(key);
        let value_byte = (step % 251) as u8;
        let was_present = mirror.contains_key(&key);
        if was_present {
            if rng.random_bool(0.5) {
                let removed = slab.remove(hash, eq_for(key), hasher_for);
                assert!(
                    removed.is_some(),
                    "step {step}: key {key} is in the mirror but not the slab"
                );
                slab.insert_unique(hash, live_for(key, &[value_byte]), hasher_for);
                mirror.insert(key, value_byte);
            } else {
                let removed = slab.remove(hash, eq_for(key), hasher_for);
                assert!(
                    removed.is_some(),
                    "step {step}: key {key} is in the mirror but not the slab"
                );
                mirror.remove(&key);
            }
        } else {
            assert!(
                slab.remove(hash, eq_for(key), hasher_for).is_none(),
                "step {step}: key {key} is absent from the mirror but the slab found it"
            );
            slab.insert_unique(hash, live_for(key, &[value_byte]), hasher_for);
            mirror.insert(key, value_byte);
        }

        assert_eq!(
            slab.len(),
            mirror.len(),
            "step {step}: length diverged from the mirror"
        );
        for (&k, &v) in &mirror {
            let found = slab.find(hash_of(k), eq_for(k)).unwrap_or_else(|| {
                panic!("step {step}: key {k} is in the mirror but not the slab")
            });
            assert_eq!(
                record_value(&found.record),
                [v],
                "step {step}: key {k}'s value diverged"
            );
        }
    }

    // shrink_to_fit at the end must not disturb any surviving entry.
    slab.shrink_to_fit(hasher_for);
    for (&k, &v) in &mirror {
        let found = slab
            .find(hash_of(k), eq_for(k))
            .expect("shrink_to_fit must not drop or misplace a surviving entry");
        assert_eq!(record_value(&found.record), [v]);
    }
}

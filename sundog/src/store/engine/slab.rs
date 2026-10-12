//! The per-stripe `Slab`: a dense arena of `Live` entries behind a hash index, with its entry handles and growth rule.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use hashbrown::HashTable;

use super::Live;
#[cfg(doc)]
use super::keys::hasher_for;
use super::record::entry_bytes;
#[cfg(doc)]
use super::record::record_key;
#[cfg(doc)]
use super::{Engine, Stripe};

/// How many slots a full arena of `len` entries grows by: a quarter of
/// `len`, and never fewer than [`ARENA_MIN_GROWTH`]. `Vec`'s own doubling
/// leaves a stripe that just outgrew its arena half empty, and a
/// [`Live`] slot is the largest per-entry cost after the record itself;
/// growing by a quarter keeps the arena at least four fifths full while
/// each entry is still copied only a bounded number of times on average.
pub(super) fn arena_growth(len: usize) -> usize {
    (len / 4).max(ARENA_MIN_GROWTH)
}

/// The fewest slots [`arena_growth`] adds, so a small stripe does not
/// reallocate on every insert.
pub(super) const ARENA_MIN_GROWTH: usize = 4;

/// One stripe's live entries: a dense arena plus a hash index mapping each
/// live key's hash to its arena slot. No holes, no free list:
/// `entries.len()` is always the exact live count, so a full-stripe walker
/// iterates `entries` directly with no liveness check. [`Stripe::live`]'s
/// storage.
pub(super) struct Slab<K, V> {
    /// The arena: one slot per live entry, indexed by `u32`.
    pub(super) entries: Vec<Live<K, V>>,
    /// `u32` slot indices, hashed and compared through [`hasher_for`]/
    /// [`record_key`] on the slot's own record.
    index: HashTable<u32>,
    /// The engine's resident byte total, shared by every stripe's slab when
    /// [`Engine::count_resident_bytes`] turns counting on: every entry this
    /// slab gains or loses moves it by [`entry_bytes`]. `None` counts
    /// nothing.
    pub(super) resident_bytes: Option<Arc<AtomicU64>>,
}

impl<K, V> Slab<K, V> {
    /// An empty slab: no allocation until the first insert, matching
    /// `HashTable::new()`'s own zero-allocation behavior.
    pub(super) fn new() -> Self {
        Self {
            entries: Vec::new(),
            index: HashTable::new(),
            resident_bytes: None,
        }
    }

    /// A slab presized for `capacity` entries: the arena and index each
    /// allocate once, up front, reached through [`Stripe::with_capacity`].
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
            index: HashTable::with_capacity(capacity),
            resident_bytes: None,
        }
    }

    /// This slab's live entry count: `entries.len()`, always exact since
    /// the arena carries no holes or tombstoned slots.
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    /// This slab's reserved arena capacity: `entries.capacity()`.
    pub(super) fn capacity(&self) -> usize {
        self.entries.capacity()
    }

    /// The live entry at `hash` for which `eq` holds, if any.
    pub(super) fn find(
        &self,
        hash: u64,
        mut eq: impl FnMut(&Live<K, V>) -> bool,
    ) -> Option<&Live<K, V>> {
        let entries = &self.entries;
        let &slot = self.index.find(hash, |&i| eq(&entries[i as usize]))?;
        Some(&self.entries[slot as usize])
    }

    /// [`Slab::find`]'s mutable counterpart.
    pub(super) fn find_mut(
        &mut self,
        hash: u64,
        mut eq: impl FnMut(&Live<K, V>) -> bool,
    ) -> Option<&mut Live<K, V>> {
        let entries = &self.entries;
        let slot = *self.index.find(hash, |&i| eq(&entries[i as usize]))?;
        Some(&mut self.entries[slot as usize])
    }

    /// Looks `hash` up under `eq`: [`SlabEntry::Occupied`] naming the slot
    /// when found, [`SlabEntry::Vacant`] carrying `hash`/`hasher` otherwise.
    pub(super) fn entry<F>(
        &mut self,
        hash: u64,
        mut eq: impl FnMut(&Live<K, V>) -> bool,
        hasher: F,
    ) -> SlabEntry<'_, K, V, F>
    where
        F: Fn(&Live<K, V>) -> u64,
    {
        let found = {
            let entries = &self.entries;
            self.index
                .find(hash, |&i| eq(&entries[i as usize]))
                .copied()
        };
        match found {
            Some(slot) => SlabEntry::Occupied(SlabOccupiedEntry {
                slab: self,
                hash,
                slot: slot as usize,
                hasher,
            }),
            None => SlabEntry::Vacant(SlabVacantEntry {
                slab: self,
                hash,
                hasher,
            }),
        }
    }

    /// Pushes `value` onto the arena and indexes it at `hash`, returning
    /// the slot. No duplicate-key check.
    pub(super) fn insert_unique(
        &mut self,
        hash: u64,
        value: Live<K, V>,
        hasher: impl Fn(&Live<K, V>) -> u64,
    ) -> u32 {
        let slot = u32::try_from(self.entries.len())
            .expect("a stripe never holds anywhere near u32::MAX live entries");
        if self.entries.len() == self.entries.capacity() {
            self.entries.reserve_exact(arena_growth(self.entries.len()));
        }
        if let Some(total) = &self.resident_bytes {
            total.fetch_add(entry_bytes(&value), Ordering::Relaxed);
        }
        self.entries.push(value);
        let entries = &self.entries;
        self.index
            .insert_unique(hash, slot, |&i| hasher(&entries[i as usize]));
        slot
    }

    /// Removes the live entry at `hash` for which `eq` holds, if any. No
    /// production call site uses this; only tests do.
    #[allow(
        dead_code,
        reason = "no production call site removes through this convenience wrapper yet; \
                  exercised directly by slab_remove_of_absent_key_returns_none and \
                  slab_random_permutation_insert_remove_matches_a_hashmap_mirror"
    )]
    pub(super) fn remove(
        &mut self,
        hash: u64,
        eq: impl FnMut(&Live<K, V>) -> bool,
        hasher: impl Fn(&Live<K, V>) -> u64,
    ) -> Option<Live<K, V>> {
        match self.entry(hash, eq, hasher) {
            SlabEntry::Occupied(occ) => Some(occ.remove().0),
            SlabEntry::Vacant(_) => None,
        }
    }

    /// Swap-removes arena slot `slot` (index row hashing to `hash`),
    /// repointing the moved entry's index row so `entries` stays dense.
    fn remove_at(
        &mut self,
        hash: u64,
        slot: usize,
        hasher: &impl Fn(&Live<K, V>) -> u64,
    ) -> Live<K, V> {
        let slot_u32 = u32::try_from(slot).expect("slot is a valid arena index, so it fits a u32");
        let index_row = self
            .index
            .find_entry(hash, |&i| i == slot_u32)
            .expect("remove_at is only called for a slot this slab's index still names");
        index_row.remove();
        let last = self.entries.len() - 1;
        let removed = self.entries.swap_remove(slot);
        if let Some(total) = &self.resident_bytes {
            total.fetch_sub(entry_bytes(&removed), Ordering::Relaxed);
        }
        if slot != last {
            let moved_hash = hasher(&self.entries[slot]);
            let last_u32 =
                u32::try_from(last).expect("last is a valid arena index, so it fits a u32");
            let moved_row = self
                .index
                .find_mut(moved_hash, |&i| i == last_u32)
                .expect("the entry swapped into `slot` still has its own index row at `last`");
            *moved_row = slot_u32;
        }
        removed
    }

    /// Keeps only entries for which `f` returns `true`, dense afterwards: a
    /// kept entry's slot never moves; a dropped one is swap-removed.
    pub(super) fn retain(
        &mut self,
        hasher: impl Fn(&Live<K, V>) -> u64,
        mut f: impl FnMut(&Live<K, V>) -> bool,
    ) {
        let mut i = 0;
        while i < self.entries.len() {
            if f(&self.entries[i]) {
                i += 1;
            } else {
                let hash = hasher(&self.entries[i]);
                self.remove_at(hash, i, &hasher);
            }
        }
    }

    /// Every live entry, in arena order (no caller depends on the order).
    pub(super) fn iter(&self) -> std::slice::Iter<'_, Live<K, V>> {
        self.entries.iter()
    }

    /// Drains and returns every live entry, emptying both the arena and the
    /// index.
    pub(super) fn drain(&mut self) -> std::vec::Drain<'_, Live<K, V>> {
        if let Some(total) = &self.resident_bytes {
            let drained: u64 = self.entries.iter().map(entry_bytes).sum();
            total.fetch_sub(drained, Ordering::Relaxed);
        }
        self.index.clear();
        self.entries.drain(..)
    }

    /// Shrinks the arena and index to fit, rehashing every entry's index
    /// row. Called by [`Engine::compact`]'s paced shrink pass.
    pub(super) fn shrink_to_fit(&mut self, hasher: impl Fn(&Live<K, V>) -> u64) {
        self.entries.shrink_to_fit();
        let entries = &self.entries;
        self.index.shrink_to_fit(|&i| hasher(&entries[i as usize]));
    }
}

/// [`Slab::entry`]'s result: an occupied arena slot or a vacant hash+hasher
/// pair ready for [`SlabVacantEntry::insert`].
pub(super) enum SlabEntry<'a, K, V, F> {
    Occupied(SlabOccupiedEntry<'a, K, V, F>),
    Vacant(SlabVacantEntry<'a, K, V, F>),
}

/// An occupied [`Slab`] arena slot found by [`Slab::entry`].
pub(super) struct SlabOccupiedEntry<'a, K, V, F> {
    slab: &'a mut Slab<K, V>,
    hash: u64,
    slot: usize,
    hasher: F,
}

impl<'a, K, V, F> SlabOccupiedEntry<'a, K, V, F>
where
    F: Fn(&Live<K, V>) -> u64,
{
    /// A reference to this slot's entry.
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "every production read site goes through Slab::find; kept for symmetry \
                      with SlabOccupiedEntry::get_mut and hashbrown's own OccupiedEntry API"
        )
    )]
    pub(super) fn get(&self) -> &Live<K, V> {
        &self.slab.entries[self.slot]
    }

    /// A mutable reference to this slot's entry. No production site uses
    /// this; kept for symmetry with hashbrown's own `OccupiedEntry` API.
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "every production mutation site uses Slab::find_mut directly instead; \
                      kept for symmetry with SlabOccupiedEntry::get and hashbrown's own \
                      OccupiedEntry API"
        )
    )]
    pub(super) fn get_mut(&mut self) -> &mut Live<K, V> {
        &mut self.slab.entries[self.slot]
    }

    /// Takes this slot's entry out via [`Slab::remove_at`], returning it
    /// with a [`SlabVacantEntry`] for the same hash.
    pub(super) fn remove(self) -> (Live<K, V>, SlabVacantEntry<'a, K, V, F>) {
        let removed = self.slab.remove_at(self.hash, self.slot, &self.hasher);
        (
            removed,
            SlabVacantEntry {
                slab: self.slab,
                hash: self.hash,
                hasher: self.hasher,
            },
        )
    }
}

/// A vacant [`Slab`] hash slot found by [`Slab::entry`], or handed back by
/// [`SlabOccupiedEntry::remove`].
pub(super) struct SlabVacantEntry<'a, K, V, F> {
    slab: &'a mut Slab<K, V>,
    hash: u64,
    hasher: F,
}

impl<K, V, F> SlabVacantEntry<'_, K, V, F>
where
    F: Fn(&Live<K, V>) -> u64,
{
    /// Inserts `value` at this vacant hash via [`Slab::insert_unique`]. No
    /// production path uses this; kept for symmetry with hashbrown's API.
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "every production write path calls Slab::insert_unique directly instead; \
                      kept for a caller that only has a SlabEntry::Vacant in hand, and for \
                      symmetry with hashbrown's own VacantEntry API"
        )
    )]
    pub(super) fn insert(self, value: Live<K, V>) -> u32 {
        self.slab.insert_unique(self.hash, value, self.hasher)
    }
}

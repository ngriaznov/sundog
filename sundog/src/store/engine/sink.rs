//! The engine side of the spill tier's flusher callbacks: installing a flushed record, abandoning a pending one and reclaiming a rotated-out region; and dropping a spilled entry a read found unreadable.

use std::hash::Hash;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::hlc::Hlc;
use crate::store::entry_fingerprint;
#[cfg(doc)]
use crate::store::spill::SpillTier;
use crate::store::spill::{ReclaimRef, SpillLoc, SpillSink, spilled_is_current};

use super::expiry::encode_expiry;
use super::keys::{digest_slot, hasher_for, part_index_from_hash, stripe_index_from_hash};
use super::record::{build_key_only_record, record_key, set_record};
use super::{Engine, EntryState, Live, SlabEntry};

/// The engine-side callback surface [`SpillTier`]'s flusher drives, so
/// installing a flushed record and reclaiming a rotated-out region both flip
/// existing `live` entries in place rather than routing back through
/// `apply_locked`'s fan-out/event machinery: neither changes a key's
/// version, value, or expiry, so nothing about it differs from the
/// cluster's point of view.
#[cfg(feature = "spill")]
impl<K, V> SpillSink for Engine<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// Resolves the job either way: `weight` leaves `pending_spill_weight`
    /// whether the flip happens or the key moved on, so a write, tombstone,
    /// or expiry that displaced a pending key never strands its share.
    fn install(
        &self,
        stripe_idx: usize,
        key_bytes: &Bytes,
        hash: u64,
        ver: Hlc,
        loc: SpillLoc,
        weight: u32,
    ) -> bool {
        let flipped = {
            let mut stripe = self.stripes[stripe_idx].write();
            let stored_tombstone_ver = stripe.tombstones.get(key_bytes.as_ref()).map(|t| t.ver);
            match stripe
                .live
                .find_mut(hash, |l| record_key(&l.record) == key_bytes.as_ref())
            {
                Some(live)
                    if spilled_is_current(stored_tombstone_ver, Some(live.ver()), ver)
                        && live.weight == 0
                        && matches!(live.state, EntryState::Resident) =>
                {
                    // A fresh copy of the key half, never a slice of the
                    // record (which would keep its value bytes resident).
                    let key_only = build_key_only_record(record_key(&live.record));
                    set_record(live, key_only, self.resident_bytes.as_deref());
                    live.state = EntryState::Spilled(loc);
                    true
                }
                _ => false,
            }
        };
        self.pending_spill_weight
            .fetch_sub(u64::from(weight), Ordering::Relaxed);
        if flipped {
            self.note_spill_arrival();
        }
        flipped
    }

    /// Originates a fresh entry straight from an on-disk [`SpillLoc`], with
    /// no value bytes ever read into RAM: the warm tier-reopen replay
    /// primitive, distinct from `install`, which only flips an
    /// already-present entry. `record` holds only the key half
    /// ([`build_key_only_record`]); the entry starts at weight `0` like a
    /// freshly installed one (a later [`Engine::promote_locked`] recomputes
    /// the real weight). `weight` mirrors `install`'s parameter for
    /// signature symmetry and is never folded into `total_weight`.
    ///
    /// Never overwrites a key already present, live or tombstoned: `false`
    /// and a no-op either way, so this can never resurrect a deleted key or
    /// clobber a fresher write. Folds the fresh entry's fingerprint into
    /// the bucket's digest like every other insert path.
    #[allow(clippy::too_many_arguments)]
    fn install_new(
        &self,
        stripe_idx: usize,
        key_bytes: &Bytes,
        hash: u64,
        ver: Hlc,
        expires_at_ms: Option<u64>,
        loc: SpillLoc,
        _weight: u32,
    ) -> bool {
        let part = part_index_from_hash(hash);
        let inserted = {
            let mut stripe = self.stripes[stripe_idx].write();
            if stripe.tombstones.contains_key(key_bytes.as_ref())
                || stripe
                    .live
                    .find(hash, |l| record_key(&l.record) == key_bytes.as_ref())
                    .is_some()
            {
                false
            } else {
                let (expiry, long_ttl_value) = encode_expiry(ver.wall_ms, expires_at_ms);
                if let Some(deadline) = long_ttl_value {
                    stripe.long_ttl.insert(key_bytes.clone(), deadline);
                }
                stripe.live.insert_unique(
                    hash,
                    Live::new(
                        build_key_only_record(key_bytes.as_ref()),
                        ver,
                        expiry,
                        0,
                        ver.wall_ms,
                        EntryState::Spilled(loc),
                    ),
                    hasher_for,
                );
                if let Some(exp) = expires_at_ms {
                    stripe.next_expiry_ms = stripe.next_expiry_ms.min(exp);
                }
                true
            }
        };
        if inserted {
            self.digest[digest_slot(stripe_idx, part)].fetch_xor(
                entry_fingerprint(key_bytes.as_ref(), ver),
                Ordering::Relaxed,
            );
            self.live_count.fetch_add(1, Ordering::Relaxed);
            self.note_spill_arrival();
        }
        inserted
    }

    /// Resolves a job the tier could not write: `weight` leaves
    /// `pending_spill_weight` unconditionally, and goes back onto the entry
    /// and `total_weight` only while the entry is still the pending one.
    fn abandon(&self, stripe_idx: usize, key_bytes: &Bytes, hash: u64, ver: Hlc, weight: u32) {
        let restored = {
            let mut stripe = self.stripes[stripe_idx].write();
            match stripe
                .live
                .find_mut(hash, |l| record_key(&l.record) == key_bytes.as_ref())
            {
                Some(live)
                    if live.ver() == ver
                        && live.weight == 0
                        && matches!(live.state, EntryState::Resident) =>
                {
                    live.weight = weight;
                    true
                }
                _ => false,
            }
        };
        self.pending_spill_weight
            .fetch_sub(u64::from(weight), Ordering::Relaxed);
        if restored {
            self.total_weight
                .fetch_add(u64::from(weight), Ordering::Relaxed);
        }
    }

    fn reclaim(&self, region: u32, generation: u32, rows: &[ReclaimRef]) -> usize {
        let mut removed = 0usize;
        for row in rows {
            // Location identifies the entry: one record occupies one offset
            // of one region generation, so no key compare is needed, and an
            // entry since overwritten, promoted or spilled again elsewhere no
            // longer matches.
            let removed_this = self.remove_spilled_where(usize::from(row.stripe), row.hash, |l| {
                matches!(
                    &l.state,
                    EntryState::Spilled(loc) if loc.region == region
                        && loc.generation == generation
                        && loc.offset == row.offset
                )
            });
            if removed_this {
                removed += 1;
            }
        }
        self.note_spill_departures(removed);
        removed
    }
}

#[cfg(feature = "spill")]
impl<K, V> Engine<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// Drops `key_bytes`'s entry, found by `hash`, if it is still spilled at
    /// `loc`, the pointer a read found unreadable: its record fails its
    /// checksum or no longer decodes. The entry leaves the digests, the live
    /// count and the spill gauge as a reclaimed one does, and no tombstone is
    /// written, so a peer that holds the key can supply it again. An entry
    /// since overwritten, promoted, removed or spilled again elsewhere no
    /// longer matches and is kept. Returns whether it dropped the entry.
    pub(crate) fn drop_unreadable_spilled(
        &self,
        key_bytes: &[u8],
        hash: u64,
        loc: SpillLoc,
    ) -> bool {
        let dropped = self.remove_spilled_where(stripe_index_from_hash(hash), hash, |l| {
            matches!(&l.state, EntryState::Spilled(at) if *at == loc)
                && record_key(&l.record) == key_bytes
        });
        if dropped {
            self.note_spill_departures(1);
        }
        dropped
    }

    /// Removes the spilled entry in stripe `bucket` that `hash` and `is_it`
    /// pick out, under the stripe write lock, together with its long-TTL row,
    /// its digest fingerprint and its share of the live count. Writes no
    /// tombstone and leaves the spill gauge to the caller, which counts its
    /// departures once. Returns whether it removed one.
    fn remove_spilled_where(
        &self,
        bucket: usize,
        hash: u64,
        is_it: impl Fn(&Live<K, V>) -> bool,
    ) -> bool {
        {
            let mut stripe = self.stripes[bucket].write();
            let SlabEntry::Occupied(occ) = stripe.live.entry(hash, is_it, hasher_for) else {
                return false;
            };
            let (removed_entry, _vacant) = occ.remove();
            let key_bytes = record_key(&removed_entry.record);
            stripe.long_ttl.remove(key_bytes);
            let part = part_index_from_hash(hash);
            self.digest[digest_slot(bucket, part)].fetch_xor(
                entry_fingerprint(key_bytes, removed_entry.ver()),
                Ordering::Relaxed,
            );
        }
        self.live_count.fetch_sub(1, Ordering::Relaxed);
        true
    }
}

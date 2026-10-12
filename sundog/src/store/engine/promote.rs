//! Promotion on `Engine`: reading a spilled entry's pointer and flipping it back to resident.

use std::hash::Hash;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::hlc::Hlc;
use crate::store::spill::{SpillLoc, spilled_is_current};

use super::keys::stripe_index_from_hash;
use super::record::{build_resident_record, decode_key, record_key, set_record};
use super::{Engine, EntryState, OnRefusal, Stripe, Written};

impl<K, V> Engine<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// Snapshots a currently-[`EntryState::Spilled`] entry's pointer under the
    /// stripe read lock: `None` for a resident entry, an absent key, or one
    /// a read at `now_ms` would see as expired. Touches `last_access_ms`.
    /// The entry's `ver` travels alongside the pointer so a later
    /// promotion can re-verify it is still current via
    /// [`spilled_is_current`]. The caller drops the read lock, reads the
    /// pointer's bytes off-lock, via `spawn_blocking` behind the tier's
    /// read semaphore, then reacquires the stripe write lock for
    /// [`Engine::promote_locked`].
    #[cfg(feature = "spill")]
    pub(crate) fn spilled_loc(
        &self,
        key_bytes: &[u8],
        hash: u64,
        now_ms: u64,
    ) -> Option<(Hlc, SpillLoc)> {
        let stripe = self.stripes[stripe_index_from_hash(hash)].read();
        let live = stripe
            .live
            .find(hash, |l| record_key(&l.record) == key_bytes)?;
        if self.is_absent(live, key_bytes, &stripe.long_ttl, now_ms) {
            return None;
        }
        match &live.state {
            EntryState::Spilled(loc) => {
                let loc = *loc;
                let ver = live.ver();
                self.touch(live, now_ms);
                Some((ver, loc))
            }
            EntryState::Resident => None,
        }
    }

    /// Flips a currently-[`EntryState::Spilled`] entry back to
    /// [`EntryState::Resident`] in place, iff [`spilled_is_current`] still
    /// holds for `read_ver` against what is currently stored. A tombstone
    /// or a version change since the disk read started means the
    /// promotion is a silent no-op. The caller's read already succeeded
    /// independently of this; only the RAM reinstall is skipped. Decodes
    /// `key` from `key_bytes` for the one weigher call this makes.
    ///
    /// Makes room first: while the entry is still spilled, and so never an
    /// eviction candidate, [`Engine::evict_down_to`] evicts down to
    /// `max_capacity` less the entry's weight, under
    /// [`OnRefusal::KeepResident`] in every mode: a victim the tier refuses,
    /// most often for a full flush queue, stays resident, never deleted,
    /// and is recorded under `sundog_spill_dropped_total{reason="deferred"}`.
    /// The entry's weight then goes back to `total_weight` only if it fits
    /// within the cap ([`claimed_total`]), claimed atomically so concurrent
    /// promotions never take the same room twice. Without room, the read
    /// stands and the entry stays on disk: an entry heavier than the whole
    /// cap ([`promotion_fits`]), a tier refusing every victim, or a
    /// concurrent promotion that claimed the room first. A promotion
    /// therefore never takes `total_weight` past the cap, never evicts the
    /// entry it reads, and never deletes another entry, whatever the
    /// cache's mode; a write's eviction keeps the tier's own policy.
    ///
    /// **Never touches the digest or `live_count`**: same key, same `ver`,
    /// same fingerprint, so nothing about the entry's replicated identity
    /// changes. Also decrements `sundog_spill_entries{cache}`, the mirror of
    /// `SpillSink::install`'s increment, since a resident entry is not
    /// counted among currently-spilled entries. Returns whether it
    /// promoted.
    #[cfg(feature = "spill")]
    pub(crate) fn promote_locked(
        &self,
        key_bytes: &[u8],
        hash: u64,
        read_ver: Hlc,
        value: &V,
        encoded: &Bytes,
        now_ms: u64,
    ) -> bool {
        let bucket = stripe_index_from_hash(hash);
        if !spilled_and_current(&self.stripes[bucket].read(), key_bytes, hash, read_ver) {
            return false;
        }
        let key: K = decode_key(key_bytes);
        let weight = self.weigher.as_ref().map_or(1, |w| w(&key, value));
        if !promotion_fits(weight, self.max_capacity) {
            return false;
        }
        let _ = self.evict_down_to(
            self.max_capacity.saturating_sub(u64::from(weight)),
            bucket,
            None,
            &mut Written::default(),
            OnRefusal::KeepResident,
            now_ms,
        );
        if !self.claim_weight(weight) {
            return false;
        }
        self.reinstall_claimed(key_bytes, hash, read_ver, weight, encoded)
    }

    /// [`Engine::promote_locked`]'s last step, once `weight` is claimed in
    /// `total_weight`: rechecks under the write lock that the key is still
    /// spilled at `read_ver`, since a write, removal or racing promotion
    /// may have landed while the room was made, and reinstalls it resident
    /// at `weight`. A failed recheck gives the claim back and returns
    /// `false`.
    #[cfg(feature = "spill")]
    pub(super) fn reinstall_claimed(
        &self,
        key_bytes: &[u8],
        hash: u64,
        read_ver: Hlc,
        weight: u32,
        encoded: &Bytes,
    ) -> bool {
        let bucket = stripe_index_from_hash(hash);
        let mut stripe = self.stripes[bucket].write();
        if !spilled_and_current(&stripe, key_bytes, hash, read_ver) {
            drop(stripe);
            self.total_weight
                .fetch_sub(u64::from(weight), Ordering::Relaxed);
            return false;
        }
        let live = stripe
            .live
            .find_mut(hash, |l| record_key(&l.record) == key_bytes)
            .expect("invariant: spilled_and_current found it under this same lock");
        set_record(
            live,
            build_resident_record(key_bytes, encoded.as_ref()),
            self.resident_bytes.as_deref(),
        );
        live.state = EntryState::Resident;
        live.weight = weight;
        drop(stripe);
        self.note_spill_departure(true);
        true
    }

    /// Adds `weight` to `total_weight` iff the sum stays within
    /// `max_capacity` ([`claimed_total`]), as one atomic step, so
    /// concurrent [`Engine::promote_locked`] calls never claim the same
    /// room twice. Returns whether it claimed.
    #[cfg(feature = "spill")]
    pub(super) fn claim_weight(&self, weight: u32) -> bool {
        let mut total = self.total_weight.load(Ordering::Relaxed);
        loop {
            let Some(claimed) = claimed_total(total, weight, self.max_capacity) else {
                return false;
            };
            match self.total_weight.compare_exchange_weak(
                total,
                claimed,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => total = actual,
            }
        }
    }
}

/// Whether `stripe` holds `key_bytes` spilled at `read_ver` with no
/// tombstone for the key: the entry [`Engine::promote_locked`] may still
/// reinstall.
#[cfg(feature = "spill")]
fn spilled_and_current<K, V>(
    stripe: &Stripe<K, V>,
    key_bytes: &[u8],
    hash: u64,
    read_ver: Hlc,
) -> bool {
    let stored_tombstone_ver = stripe.tombstones.get(key_bytes).map(|t| t.ver);
    stripe
        .live
        .find(hash, |l| record_key(&l.record) == key_bytes)
        .is_some_and(|live| {
            spilled_is_current(stored_tombstone_ver, Some(live.ver()), read_ver)
                && matches!(live.state, EntryState::Spilled(_))
        })
}

/// Whether an entry of `weight` may come back into RAM under
/// `max_capacity`: never one heavier than the whole cap, which no eviction
/// makes room for. Pure; unit tested directly.
#[cfg(feature = "spill")]
pub(super) const fn promotion_fits(weight: u32, max_capacity: u64) -> bool {
    (weight as u64) <= max_capacity
}

/// `total` with a promoted entry's `weight` added, iff the sum stays
/// within `max_capacity`: [`Engine::promote_locked`]'s claim on the room it
/// made. Pure; unit tested directly.
#[cfg(feature = "spill")]
pub(super) fn claimed_total(total: u64, weight: u32, max_capacity: u64) -> Option<u64> {
    total
        .checked_add(u64::from(weight))
        .filter(|&sum| sum <= max_capacity)
}

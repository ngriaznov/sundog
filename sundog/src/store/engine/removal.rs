//! Removal and ownership on `Engine`: peer invalidation and the release of buckets and parts.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::Ordering;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::hlc::Hlc;
use crate::store::part::PartSet;
use crate::store::{BUCKET_COUNT, PART_COUNT, PartId};

use super::keys::{
    digest_slot, hash_key_bytes, hasher_for, part_index_from_hash, stripe_index_from_hash,
};
use super::record::record_key;
use super::{Engine, Live, is_spilled, remove_live};

impl<K, V> Engine<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// Applies an inbound [`super::super::ShardOps::invalidate`]: drops the live entry
    /// at `key_bytes` iff `ver` is newer than whatever version is stored,
    /// writing no tombstone of its own, and marks a load in flight for the
    /// key so its fill stores nothing. Returns the departing version on an
    /// actual removal, `None` otherwise.
    pub(crate) fn invalidate(&self, key_bytes: &[u8], hash: u64, ver: Hlc) -> Option<Hlc> {
        let bucket = stripe_index_from_hash(hash);
        let mut stripe = self.stripes[bucket].write();
        let prior_tombstone = stripe.tombstones.get(key_bytes).copied();
        let stored_ver = match prior_tombstone {
            Some(t) => Some(t.ver),
            None => stripe
                .live
                .find(hash, |l| record_key(&l.record) == key_bytes)
                .map(Live::ver),
        };
        if stored_ver.is_some_and(|sv| ver <= sv) {
            return None;
        }
        if let Some(inflight) = stripe.inflight.get(key_bytes) {
            inflight.invalidated.store(true, Ordering::Relaxed);
        }
        let had_live = prior_tombstone.is_none() && stored_ver.is_some();
        if !had_live {
            return None;
        }
        let removed = remove_live(&mut stripe, hash, key_bytes)?;
        drop(stripe);
        let ver_out = removed.ver;
        let part = part_index_from_hash(hash);
        self.account_removed(bucket, part, key_bytes, &removed, false);
        Some(ver_out)
    }

    /// Drops the local live entry at `key_bytes` unconditionally, no version
    /// check, no tombstone, for [`super::super::Shard::invalidate_local`]'s
    /// cache-busting escape hatch, and marks a load in flight for the key so
    /// its fill stores nothing.
    pub(crate) fn invalidate_local(&self, key_bytes: &[u8], hash: u64) {
        let bucket = stripe_index_from_hash(hash);
        let mut stripe = self.stripes[bucket].write();
        if let Some(inflight) = stripe.inflight.get(key_bytes) {
            inflight.invalidated.store(true, Ordering::Relaxed);
        }
        if let Some(removed) = remove_live(&mut stripe, hash, key_bytes) {
            drop(stripe);
            let part = part_index_from_hash(hash);
            self.account_removed(bucket, part, key_bytes, &removed, false);
        }
    }

    /// Removes every locally held entry, live or tombstone, in each of
    /// `buckets`: one write-lock hold per bucket, resetting that stripe's
    /// digest atomics, [`Engine::live_count`], [`Engine::total_weight`], and
    /// spill bookkeeping to match, the same accounting a per-key removal
    /// keeps, batched here per stripe instead of per key. Writes no
    /// tombstone of its own, so a released bucket carries nothing for
    /// anti-entropy or replication to fan back out; a peer later applying a
    /// fresh write for a key in a released bucket lands it under the
    /// ordinary ownership-gated apply path, never a resurrection of the
    /// bucket's prior contents. A bucket at or past [`BUCKET_COUNT`], which
    /// only a misbehaving caller names, is skipped. Returns the total number of
    /// entries removed, live and tombstoned together, across every bucket
    /// given.
    pub(crate) fn release_buckets(&self, buckets: &[u16]) -> u64 {
        let mut removed_total = 0u64;
        for &bucket in buckets {
            if usize::from(bucket) >= BUCKET_COUNT {
                continue;
            }
            let bucket_idx = usize::from(bucket);
            let (removed_weight, removed_live, removed_tombstones, removed_spilled) = {
                let mut stripe = self.stripes[bucket_idx].write();
                let mut removed_weight = 0u64;
                let mut removed_live = 0u64;
                let mut removed_spilled = 0usize;
                for live in stripe.live.drain() {
                    removed_weight += u64::from(live.weight);
                    removed_live += 1;
                    if is_spilled(&live) {
                        removed_spilled += 1;
                    }
                }
                let removed_tombstones = stripe.tombstones.len();
                stripe.tombstones.clear();
                stripe.long_ttl.clear();
                stripe.next_expiry_ms = u64::MAX;
                (
                    removed_weight,
                    removed_live,
                    removed_tombstones,
                    removed_spilled,
                )
            };
            for part in 0..PART_COUNT {
                self.digest[digest_slot(bucket_idx, part)].store(0, Ordering::Relaxed);
            }
            if removed_weight > 0 {
                self.total_weight
                    .fetch_sub(removed_weight, Ordering::Relaxed);
            }
            if removed_live > 0 {
                self.live_count.fetch_sub(removed_live, Ordering::Relaxed);
            }
            self.note_spill_departures(removed_spilled);
            removed_total = removed_total
                .saturating_add(removed_live)
                .saturating_add(u64::try_from(removed_tombstones).unwrap_or(u64::MAX));
        }
        removed_total
    }

    /// [`Engine::release_buckets`] for single parts: drops every live entry
    /// and tombstone in `parts`, zeroes their part digests, and leaves the
    /// rest of each bucket untouched. A bucket named by all
    /// [`PART_COUNT`] of its parts takes the whole-bucket path. Returns the
    /// total entries removed, live and tombstoned together.
    pub(crate) fn release_parts(&self, parts: &[PartId]) -> u64 {
        let mut masks: HashMap<u16, u64> = HashMap::new();
        for part in parts {
            *masks.entry(part.bucket()).or_default() |= 1u64 << part.part();
        }
        let mut removed_total = 0u64;
        for (bucket, mask) in masks {
            if mask == u64::MAX {
                removed_total = removed_total.saturating_add(self.release_buckets(&[bucket]));
                continue;
            }
            let released =
                |key: &[u8]| mask & (1u64 << part_index_from_hash(hash_key_bytes(key))) != 0;
            let bucket_idx = usize::from(bucket);
            let (removed_weight, removed_live, removed_tombstones, removed_spilled) = {
                let mut stripe = self.stripes[bucket_idx].write();
                let mut removed_weight = 0u64;
                let mut removed_live = 0u64;
                let mut removed_spilled = 0usize;
                stripe.live.retain(hasher_for, |live| {
                    if released(record_key(&live.record)) {
                        removed_weight += u64::from(live.weight);
                        removed_live += 1;
                        if is_spilled(live) {
                            removed_spilled += 1;
                        }
                        false
                    } else {
                        true
                    }
                });
                let before = stripe.tombstones.len();
                stripe.tombstones.retain(|key, _| !released(key));
                let removed_tombstones = before - stripe.tombstones.len();
                stripe.long_ttl.retain(|key, _| !released(key));
                (
                    removed_weight,
                    removed_live,
                    removed_tombstones,
                    removed_spilled,
                )
            };
            for part in 0..PART_COUNT {
                if mask & (1u64 << part) != 0 {
                    self.digest[digest_slot(bucket_idx, part)].store(0, Ordering::Relaxed);
                }
            }
            if removed_weight > 0 {
                self.total_weight
                    .fetch_sub(removed_weight, Ordering::Relaxed);
            }
            if removed_live > 0 {
                self.live_count.fetch_sub(removed_live, Ordering::Relaxed);
            }
            self.note_spill_departures(removed_spilled);
            removed_total = removed_total
                .saturating_add(removed_live)
                .saturating_add(u64::try_from(removed_tombstones).unwrap_or(u64::MAX));
        }
        removed_total
    }

    /// Every part holding at least one live entry or un-GC'd tombstone,
    /// ascending: one stripe read lock at a time, skipping empty stripes
    /// without touching an entry.
    pub(crate) fn held_parts(&self) -> Vec<PartId> {
        let mut held = PartSet::new();
        for stripe in &*self.stripes {
            let stripe = stripe.read();
            if stripe.live.len() == 0 && stripe.tombstones.is_empty() {
                continue;
            }
            for live in stripe.live.iter() {
                held.insert(PartId::from_hash(hash_key_bytes(record_key(&live.record))));
            }
            for key in stripe.tombstones.keys() {
                held.insert(PartId::from_hash(hash_key_bytes(key)));
            }
        }
        held.iter().collect()
    }
}

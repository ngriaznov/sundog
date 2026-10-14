//! Checkpointing on `Engine`: the resident snapshot, the finalize pass and the spilled-aware record reads.

use std::hash::Hash;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::hlc::Hlc;
#[cfg(doc)]
use crate::store::spill::SpillTier;
use crate::wire::WireRecord;

#[cfg(feature = "spill")]
use super::Lapsed;
use super::expiry::decode_expiry;
use super::keys::{hash_key_bytes, stripe_index_from_hash};
use super::record::{
    build_key_only_record, record_key, record_key_bytes, record_value_bytes, set_record,
};
use super::{Engine, EntryState, SpilledPointer, Stripe, is_resident};

/// [`Engine::finalize_checkpoint_writes`]'s outcome for one entry.
#[cfg(feature = "spill")]
enum FinalizeWriteOutcome {
    /// Still `Resident` at its captured version and not expired: flipped
    /// to `Spilled` and kept.
    Kept(u32),
    /// The key gained a tombstone between capture and this call.
    DroppedTombstoned,
    /// No longer present (removed, or its tombstone already collected).
    DroppedGone,
    /// The version no longer matches what was captured, or it's no longer
    /// `Resident`.
    DroppedVersionChanged,
    /// The live entry is now past its expiry.
    DroppedExpired,
}

/// Per-reason breakdown [`Engine::finalize_checkpoint_writes`] returns
/// alongside its survivors.
#[cfg(feature = "spill")]
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct CheckpointFinalizeCounts {
    pub(crate) kept: u64,
    pub(crate) dropped_tombstoned: u64,
    pub(crate) dropped_gone: u64,
    pub(crate) dropped_version_changed: u64,
    pub(crate) dropped_expired: u64,
}

impl<K, V> Engine<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// [`Engine::snapshot_records`]'s sibling: every currently-spilled live
    /// entry's pointer, `(key_bytes, ver, expires_at_ms, loc)`, snapshotted
    /// under each stripe's read lock alongside `snapshot_records`' pass. A
    /// spill-aware caller reads these off-lock, via `spawn_blocking` behind
    /// the tier's read semaphore, and folds the results into the snapshot,
    /// dropping any whose read comes back `None`.
    #[cfg(feature = "spill")]
    pub(crate) fn snapshot_spilled(&self, now_ms: u64) -> Vec<SpilledPointer> {
        let mut out = Vec::new();
        for stripe_lock in &self.stripes {
            let stripe = stripe_lock.read();
            out.extend(
                stripe
                    .live
                    .iter()
                    .filter(|live| {
                        !self.is_absent(live, record_key(&live.record), &stripe.long_ttl, now_ms)
                    })
                    .filter_map(|live| match &live.state {
                        EntryState::Spilled(loc) => Some((
                            record_key_bytes(&live.record),
                            live.ver(),
                            decode_expiry(
                                live.wall_ms,
                                live.expiry,
                                record_key(&live.record),
                                &stripe.long_ttl,
                            ),
                            *loc,
                        )),
                        EntryState::Resident => None,
                    }),
            );
        }
        out
    }

    /// A warm-reopen checkpoint's other half: every currently-resident live
    /// entry's key, version, expiry, and encoded value bytes, snapshotted
    /// under each stripe's read lock like [`Engine::snapshot_spilled`], but
    /// for the entries that method skips. `Shard::close_spill_checkpointed`
    /// hands this to `SpillTier::checkpoint_flush`, so a clean close can
    /// warm-reopen every live entry, not only ones already spilled.
    #[cfg(feature = "spill")]
    pub(crate) fn checkpoint_resident_entries(
        &self,
        now_ms: u64,
    ) -> Vec<(Bytes, Hlc, Option<u64>, Bytes)> {
        let mut out = Vec::new();
        for stripe_lock in &self.stripes {
            let stripe = stripe_lock.read();
            out.extend(
                stripe
                    .live
                    .iter()
                    .filter(|live| {
                        !self.is_absent(live, record_key(&live.record), &stripe.long_ttl, now_ms)
                    })
                    .filter(|live| is_resident(live))
                    .map(|live| {
                        (
                            record_key_bytes(&live.record),
                            live.ver(),
                            decode_expiry(
                                live.wall_ms,
                                live.expiry,
                                record_key(&live.record),
                                &stripe.long_ttl,
                            ),
                            record_value_bytes(&live.record),
                        )
                    }),
            );
        }
        out
    }

    /// Re-validates every entry [`SpillTier::checkpoint_flush`] just wrote
    /// to disk against the live engine state, under each key's own stripe
    /// write lock, before `Shard::close_spill_checkpointed` folds the
    /// survivors into the final checkpoint snapshot.
    ///
    /// `checkpoint_flush` writes with no version check, since these entries
    /// never went through the ordinary pending-spill hand-off, leaving a
    /// window for a racing `Cache` clone to remove, tombstone, or overwrite
    /// the key before this call. Keeps only entries still `Resident` at
    /// exactly the captured version with no tombstone, flips each survivor
    /// to [`EntryState::Spilled`] in the same lock (dropping `total_weight`
    /// by its weight, zeroing the entry's own weight), and leaves
    /// `pending_spill_weight` untouched since these writes never entered
    /// that accounting. `now_ms` is the timestamp already captured before
    /// this call, so an entry that expired during the write is excluded.
    ///
    /// Returns the survivors with a [`CheckpointFinalizeCounts`] breakdown
    /// by drop reason, for `Shard::close_spill_checkpointed` to log and
    /// meter.
    #[cfg(feature = "spill")]
    pub(crate) fn finalize_checkpoint_writes(
        &self,
        entries: Vec<SpilledPointer>,
        now_ms: u64,
    ) -> (Vec<SpilledPointer>, CheckpointFinalizeCounts) {
        let mut survivors = Vec::with_capacity(entries.len());
        let mut counts = CheckpointFinalizeCounts::default();
        for (key_bytes, ver, expires_at_ms, loc) in entries {
            let hash = hash_key_bytes(key_bytes.as_ref());
            let stripe_idx = stripe_index_from_hash(hash);
            let outcome = {
                let mut guard = self.stripes[stripe_idx].write();
                // One explicit reborrow: see `sweep`'s identical comment.
                let stripe: &mut Stripe<K, V> = &mut guard;
                if stripe.tombstones.contains_key(key_bytes.as_ref()) {
                    FinalizeWriteOutcome::DroppedTombstoned
                } else if let Some(live) = stripe
                    .live
                    .find_mut(hash, |l| record_key(&l.record) == key_bytes.as_ref())
                {
                    if live.ver() != ver || !matches!(live.state, EntryState::Resident) {
                        FinalizeWriteOutcome::DroppedVersionChanged
                    } else if self.is_absent(live, key_bytes.as_ref(), &stripe.long_ttl, now_ms) {
                        FinalizeWriteOutcome::DroppedExpired
                    } else {
                        let weight = live.weight;
                        let key_only = build_key_only_record(record_key(&live.record));
                        set_record(live, key_only, self.resident_bytes.as_deref());
                        live.state = EntryState::Spilled(loc);
                        live.weight = 0;
                        FinalizeWriteOutcome::Kept(weight)
                    }
                } else {
                    FinalizeWriteOutcome::DroppedGone
                }
            };
            match outcome {
                FinalizeWriteOutcome::Kept(weight) => {
                    self.total_weight
                        .fetch_sub(u64::from(weight), Ordering::Relaxed);
                    self.note_spill_arrival();
                    counts.kept += 1;
                    survivors.push((key_bytes, ver, expires_at_ms, loc));
                }
                FinalizeWriteOutcome::DroppedTombstoned => counts.dropped_tombstoned += 1,
                FinalizeWriteOutcome::DroppedGone => counts.dropped_gone += 1,
                FinalizeWriteOutcome::DroppedVersionChanged => counts.dropped_version_changed += 1,
                FinalizeWriteOutcome::DroppedExpired => counts.dropped_expired += 1,
            }
        }
        (survivors, counts)
    }

    /// [`Engine::record_for`] for many keys in one pass, but reporting a
    /// currently-spilled entry's pointer instead of treating it as absent.
    /// The AE-pull-reply path, `ShardOps::records_for`, reads the spilled
    /// half off-lock, via `spawn_blocking` behind the tier's read
    /// semaphore, and folds any successful read back in as a `WireRecord`,
    /// dropping the rest. Nothing here promotes; a served-from-disk record
    /// leaves `record` unchanged.
    #[cfg(feature = "spill")]
    pub(crate) fn records_for_or_spilled(
        &self,
        keys: &[Bytes],
        now_ms: u64,
    ) -> (Vec<WireRecord>, Vec<SpilledPointer>) {
        self.records_scoped_or_spilled(keys, now_ms, Lapsed::Hidden)
    }

    /// [`Engine::records_for_or_spilled`] on the written view the fan-out
    /// reads (`Engine::written_record_for`): an entry past its expiry is
    /// reported, resident as a record and spilled as a pointer, both with
    /// the past deadline.
    #[cfg(feature = "spill")]
    pub(crate) fn written_records_or_spilled(
        &self,
        keys: &[Bytes],
        now_ms: u64,
    ) -> (Vec<WireRecord>, Vec<SpilledPointer>) {
        self.records_scoped_or_spilled(keys, now_ms, Lapsed::Shown)
    }

    #[cfg(feature = "spill")]
    fn records_scoped_or_spilled(
        &self,
        keys: &[Bytes],
        now_ms: u64,
        lapsed: Lapsed,
    ) -> (Vec<WireRecord>, Vec<SpilledPointer>) {
        let mut records = Vec::new();
        let mut spilled = Vec::new();
        for key_bytes in keys {
            let hash = hash_key_bytes(key_bytes.as_ref());
            let stripe = self.stripes[stripe_index_from_hash(hash)].read();
            if let Some(t) = stripe.tombstones.get(key_bytes.as_ref()) {
                records.push(WireRecord {
                    key: key_bytes.clone(),
                    value: None,
                    ver: t.ver,
                    expires_at_ms: t.expired_at_ms,
                });
                continue;
            }
            let Some(live) = stripe
                .live
                .find(hash, |l| record_key(&l.record) == key_bytes.as_ref())
            else {
                continue;
            };
            if self.is_absent_under(live, key_bytes.as_ref(), &stripe.long_ttl, now_ms, lapsed) {
                continue;
            }
            match &live.state {
                EntryState::Resident => records.push(WireRecord {
                    key: key_bytes.clone(),
                    value: Some(record_value_bytes(&live.record)),
                    ver: live.ver(),
                    expires_at_ms: decode_expiry(
                        live.wall_ms,
                        live.expiry,
                        key_bytes.as_ref(),
                        &stripe.long_ttl,
                    ),
                }),
                EntryState::Spilled(loc) => {
                    spilled.push((
                        key_bytes.clone(),
                        live.ver(),
                        decode_expiry(
                            live.wall_ms,
                            live.expiry,
                            key_bytes.as_ref(),
                            &stripe.long_ttl,
                        ),
                        *loc,
                    ));
                }
            }
        }
        (records, spilled)
    }
}

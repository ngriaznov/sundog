//! Loads on `Engine`: the in-flight map, the fill outcomes and the load completion paths.

use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::watch;

use crate::hlc::Hlc;
use crate::store::entry_fingerprint;
#[cfg(feature = "spill")]
use crate::store::spill::spilled_is_current;

#[cfg(feature = "spill")]
use super::EntryState;
use super::expiry::encode_expiry;
use super::keys::{digest_slot, hasher_for, part_index_from_hash, stripe_index_from_hash};
use super::record::{build_resident_record, decode_value, record_key, record_value};
use super::{Engine, Inflight, Live, Written, is_resident, remove_live};

/// What [`Engine::complete_fresh_load`] did with a fill.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FillOutcome {
    /// The loaded value is installed; `had_live` says whether it replaced a
    /// live entry.
    Installed { had_live: bool },
    /// A write or removal at least as new as the fill landed while the
    /// loader ran, and stays in place of the loaded value.
    Superseded,
    /// An invalidation landed while the loader ran, so the loaded value may
    /// predate the change it announces: nothing is stored, and joined
    /// waiters answer with the value.
    Invalidated,
}

/// Whether a fill stamped `fill` replaces what its key holds, `existing`
/// being the version of the tombstone or live entry there, if any. A fill
/// is stamped before its loader runs, so a write or removal that lands
/// during the load is newer and wins, the same versioned-apply rule every
/// other write follows. Pure; unit tested directly.
pub(super) fn fill_supersedes(existing: Option<Hlc>, fill: Hlc) -> bool {
    existing.is_none_or(|current| current < fill)
}

/// A successful [`super::super::Shard::get_or_load`] fill's write, for [`Engine::complete_fresh_load`].
pub(crate) struct FreshLoad<'a, K, V> {
    pub(crate) key: &'a K,
    pub(crate) key_bytes: &'a Bytes,
    pub(crate) hash: u64,
    pub(crate) ver: Hlc,
    pub(crate) value: V,
    pub(crate) encoded: Bytes,
    pub(crate) expires_at_ms: Option<u64>,
}

/// The outcome of [`Engine::miss_or_join`]: a fast-path re-check hit, joining
/// an already in-flight load, or this call becoming the one that runs the
/// loader.
pub(crate) enum JoinOutcome<V> {
    Hit(V),
    /// An in-flight load to wait on, with a receiver subscribed under the
    /// stripe lock: `changed()` resolves once the owner finishes, or
    /// immediately if that already happened.
    Join(Arc<Inflight<V>>, watch::Receiver<bool>),
    Owner(Arc<Inflight<V>>),
}

/// A drop guard that frees a cancelled [`Engine::get_or_load`] fill: if the
/// caller's future drops before [`InflightGuard::complete`] runs, the in-flight
/// entry is removed and waiters are notified so one of them takes over.
pub(crate) struct InflightGuard<'a, K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    engine: &'a Engine<K, V>,
    key_bytes: Bytes,
    hash: u64,
    inflight: Arc<Inflight<V>>,
    completed: bool,
}

impl<K, V> InflightGuard<'_, K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// Marks the fill as finished, so the drop path becomes a no-op.
    pub(crate) fn complete(mut self) {
        self.completed = true;
    }
}

impl<K, V> Drop for InflightGuard<'_, K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    fn drop(&mut self) {
        if !self.completed {
            self.engine.finish_inflight(&self.key_bytes, self.hash);
            self.inflight.finish();
        }
    }
}

impl<K, V> Engine<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// Registers a reload of a key that may still hold a live entry, for
    /// refresh-ahead: readers keep answering from the entry while the
    /// reload runs, and [`Engine::miss_or_join`] joins it only once the
    /// entry is gone. `None` when a load for the key is already in flight.
    /// The reload ends like any other load, through
    /// [`Engine::complete_fresh_load`] or [`Engine::abandon_inflight`].
    pub(crate) fn begin_refresh(&self, key_bytes: &Bytes, hash: u64) -> Option<Arc<Inflight<V>>> {
        let mut stripe = self.stripes[stripe_index_from_hash(hash)].write();
        if stripe.inflight.contains_key(key_bytes.as_ref()) {
            return None;
        }
        let inflight = Arc::new(Inflight::new());
        stripe
            .inflight
            .insert(key_bytes.clone(), Arc::clone(&inflight));
        Some(inflight)
    }

    /// The lock-protected first half of [`super::super::Shard::get_or_load`]: a
    /// fast-path re-check, then either joining an already in-flight load or
    /// registering as the new owner.
    ///
    /// A currently-spilled entry never takes the `Hit` branch. There is no
    /// value here to clone. A spill-aware caller checks
    /// [`Engine::spilled_loc`] before ever reaching this call, so it only
    /// falls through to here once it already knows the entry, if any, is
    /// resident or absent.
    pub(crate) fn miss_or_join(&self, key_bytes: &Bytes, hash: u64, now_ms: u64) -> JoinOutcome<V> {
        let bucket = stripe_index_from_hash(hash);
        let mut stripe = self.stripes[bucket].write();
        if let Some(live) = stripe
            .live
            .find(hash, |l| record_key(&l.record) == key_bytes.as_ref())
            && !self.is_absent(live, key_bytes.as_ref(), &stripe.long_ttl, now_ms)
            && is_resident(live)
        {
            let value = decode_value(record_value(live.record.as_slice()));
            self.touch(live, now_ms);
            return JoinOutcome::Hit(value);
        }
        if let Some(existing) = stripe.inflight.get(key_bytes.as_ref()) {
            return JoinOutcome::Join(Arc::clone(existing), existing.done.subscribe());
        }
        let inflight = Arc::new(Inflight::new());
        stripe
            .inflight
            .insert(key_bytes.clone(), Arc::clone(&inflight));
        JoinOutcome::Owner(inflight)
    }

    /// Builds the [`InflightGuard`] that frees `inflight` if its loader future
    /// drops early.
    pub(crate) fn guard_inflight(
        &self,
        key_bytes: Bytes,
        hash: u64,
        inflight: Arc<Inflight<V>>,
    ) -> InflightGuard<'_, K, V> {
        InflightGuard {
            engine: self,
            key_bytes,
            hash,
            inflight,
            completed: false,
        }
    }

    /// Removes `key_bytes`'s in-flight entry from its stripe. Setting an error,
    /// on failure, is the caller's job against the `Inflight` handle it
    /// already holds.
    fn finish_inflight(&self, key_bytes: &Bytes, hash: u64) {
        let bucket = stripe_index_from_hash(hash);
        let mut stripe = self.stripes[bucket].write();
        stripe.inflight.remove(key_bytes.as_ref());
    }

    /// Applies a successful [`super::super::Shard::get_or_load`] fill under one
    /// stripe write-lock acquisition: removes the `inflight` entry, stores
    /// nothing if an invalidation landed during the load, and,
    /// when [`fill_supersedes`] allows it, replaces any prior tombstone or
    /// live entry for `write.key` with the loader's value and corrects
    /// `live_count` for the net change. A tombstone or live entry at least
    /// as new as the fill, a removal or write that landed while the loader
    /// ran, stays, and the fill is dropped.
    pub(crate) fn complete_fresh_load(
        &self,
        write: FreshLoad<'_, K, V>,
        now_ms: u64,
        inflight: &Inflight<V>,
    ) -> FillOutcome {
        let FreshLoad {
            key,
            key_bytes,
            hash,
            ver,
            value,
            encoded,
            expires_at_ms,
        } = write;
        let bucket = stripe_index_from_hash(hash);
        let part = part_index_from_hash(hash);
        let outcome = {
            let mut stripe = self.stripes[bucket].write();
            stripe.inflight.remove(key_bytes.as_ref());
            if inflight.invalidated.load(Ordering::Relaxed) {
                drop(stripe);
                let _ = inflight.value.set(value);
                inflight.finish();
                return FillOutcome::Invalidated;
            }
            let existing = stripe
                .tombstones
                .get(key_bytes.as_ref())
                .map(|t| t.ver)
                .or_else(|| {
                    stripe
                        .live
                        .find(hash, |l| record_key(&l.record) == key_bytes.as_ref())
                        .map(Live::ver)
                });
            if !fill_supersedes(existing, ver) {
                drop(stripe);
                inflight.finish();
                return FillOutcome::Superseded;
            }
            let mut had_live = false;
            if let Some(t) = stripe.tombstones.remove(key_bytes.as_ref()) {
                self.digest[digest_slot(bucket, part)].fetch_xor(
                    entry_fingerprint(key_bytes.as_ref(), t.ver),
                    Ordering::Relaxed,
                );
            } else if let Some(removed) = remove_live(&mut stripe, hash, key_bytes.as_ref()) {
                had_live = true;
                // A replacement is about to land below, so `live_count` nets
                // to no change: `keep_count = true`.
                self.account_removed(bucket, part, key_bytes.as_ref(), &removed, true);
            }
            if !had_live {
                self.live_count.fetch_add(1, Ordering::Relaxed);
            }
            self.digest[digest_slot(bucket, part)].fetch_xor(
                entry_fingerprint(key_bytes.as_ref(), ver),
                Ordering::Relaxed,
            );
            let weight = self.weigher.as_ref().map_or(1, |w| w(key, &value));
            // `remove_live` already cleared any stale `long_ttl` row above.
            let (expiry, long_ttl_value) = encode_expiry(ver.wall_ms, expires_at_ms);
            if let Some(deadline) = long_ttl_value {
                stripe.long_ttl.insert(key_bytes.clone(), deadline);
            }
            stripe.live.insert_unique(
                hash,
                Live::new(
                    build_resident_record(key_bytes.as_ref(), encoded.as_ref()),
                    ver,
                    expiry,
                    weight,
                    now_ms,
                    #[cfg(feature = "spill")]
                    EntryState::Resident,
                ),
                hasher_for,
            );
            if let Some(exp) = expires_at_ms {
                stripe.next_expiry_ms = stripe.next_expiry_ms.min(exp);
            }
            self.total_weight
                .fetch_add(u64::from(weight), Ordering::Relaxed);
            FillOutcome::Installed { had_live }
        };
        inflight.finish();
        self.enforce_capacity(bucket, &mut Written::of(hash), now_ms);
        outcome
    }

    /// Records a failed loader run: removes the `inflight` entry and stores
    /// `error` so every joined waiter returns the same
    /// [`crate::error::CacheError::Loader`].
    pub(crate) fn fail_inflight(
        &self,
        key_bytes: &Bytes,
        hash: u64,
        inflight: &Inflight<V>,
        error: Arc<dyn std::error::Error + Send + Sync>,
    ) {
        let _ = inflight.error.set(error);
        self.finish_inflight(key_bytes, hash);
        inflight.finish();
    }

    /// Records a loader run that found nothing for the key: removes the
    /// `inflight` entry, and every joined waiter answers `None`.
    pub(crate) fn finish_absent(&self, key_bytes: &Bytes, hash: u64, inflight: &Inflight<V>) {
        inflight.absent.store(true, Ordering::Relaxed);
        self.finish_inflight(key_bytes, hash);
        inflight.finish();
    }

    /// Ends `inflight` with no outcome, as a dropped [`InflightGuard`]
    /// does: every joined waiter wakes, re-reads and takes the load over.
    pub(crate) fn abandon_inflight(&self, key_bytes: &Bytes, hash: u64, inflight: &Inflight<V>) {
        self.finish_inflight(key_bytes, hash);
        inflight.finish();
    }

    /// Ends `inflight` once its owner read the key's spilled value, `value`
    /// at `read_ver`, off disk: under one stripe write lock, hands `value`
    /// to every joined waiter iff the key still holds `read_ver`, spilled
    /// or promoted back, with no tombstone and unexpired at `now_ms`, and
    /// removes the in-flight entry. A removal, expiry or newer write since
    /// the read leaves the waiters to read again, so a call that joins
    /// after it never answers the old value: the check and the removal
    /// share the lock every join takes.
    #[cfg(feature = "spill")]
    pub(crate) fn finish_spilled_read(
        &self,
        key_bytes: &Bytes,
        hash: u64,
        inflight: &Inflight<V>,
        read_ver: Hlc,
        value: V,
        now_ms: u64,
    ) {
        let bucket = stripe_index_from_hash(hash);
        let mut stripe = self.stripes[bucket].write();
        let stored_tombstone_ver = stripe.tombstones.get(key_bytes.as_ref()).map(|t| t.ver);
        let current = stripe
            .live
            .find(hash, |l| record_key(&l.record) == key_bytes.as_ref())
            .is_some_and(|live| {
                spilled_is_current(stored_tombstone_ver, Some(live.ver()), read_ver)
                    && !self.is_absent(live, key_bytes.as_ref(), &stripe.long_ttl, now_ms)
            });
        if current {
            let _ = inflight.value.set(value);
        }
        stripe.inflight.remove(key_bytes.as_ref());
        drop(stripe);
        inflight.finish();
    }

    /// Records a loader run whose value is not stored: removes the
    /// `inflight` entry and hands `value` to every joined waiter.
    pub(crate) fn finish_uncached(
        &self,
        key_bytes: &Bytes,
        hash: u64,
        inflight: &Inflight<V>,
        value: V,
    ) {
        let _ = inflight.value.set(value);
        self.finish_inflight(key_bytes, hash);
        inflight.finish();
    }
}

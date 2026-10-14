//! The merge path of a [`Shard`]: stored merges, the coalesce window and the flush of pending
//! merges, with the pure deadline helpers they decide by.

use std::collections::hash_map::{Entry, OccupiedEntry};
use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::hash::Hash;
#[cfg(test)]
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::{Mutex as StdMutex, PoisonError, RwLockReadGuard};
use std::time::Duration;

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::engine::{Fold, fold_sides};
use super::{
    Incoming, Merged, PendingMerge, PendingMergeStripe, RecordView, Shard, Winner, duration_ms,
    encode_key, engine,
};
use crate::error::{CacheError, CodecError};
use crate::hlc::Hlc;
use crate::wire;

#[cfg(doc)]
use super::{BUCKET_COUNT, ConflictResolver, Event};

/// The epoch millisecond at which a [`Shard::merge`] coalescing window
/// opened at `started_at_ms` must flush. Pure so the scheduling math behind
/// `Shard::merge`'s pending map has a unit test independent of a live clock.
pub(super) fn merge_deadline_ms(started_at_ms: u64, window: Duration) -> u64 {
    started_at_ms.saturating_add(duration_ms(window))
}

/// Whether a pending coalesced merge whose window closes at `deadline_ms`
/// must flush by `now_ms`. The one real decision
/// [`Shard::flush_due_pending_merges`]'s background sweep makes, split out
/// so it has its own unit test that needs neither a live `Shard` nor
/// `tokio::time`.
pub(super) fn merge_window_elapsed(now_ms: u64, deadline_ms: u64) -> bool {
    now_ms >= deadline_ms
}

/// Whether a tombstone at `tombstone_ver` discards a pending fold whose last
/// call was stamped `pending_ver`: only a tombstone newer than the call does.
/// A fold stamped at or after the tombstone would have applied over it with
/// no window, so it stays. Pure, so the ordering has a unit test of its own.
pub(super) fn pending_merge_superseded(pending_ver: Hlc, tombstone_ver: Hlc) -> bool {
    pending_ver < tombstone_ver
}

/// Splits the pending keys of a [`Shard::clear`] by whether this node owns
/// them: the first list is the keys the clear tombstones, the owned ones, the
/// second the keys whose fold it applies instead (which forwards it to the
/// owners), the ones this node does not own. Only a `Mode::Distributed` shard
/// has a key it does not own. Pure, so the rule has a unit test of its own.
pub(super) fn split_pending_for_clear<K>(
    pending: impl IntoIterator<Item = (K, bool)>,
) -> (Vec<K>, Vec<K>) {
    let mut owned = Vec::new();
    let mut foreign = Vec::new();
    for (key, is_owned) in pending {
        if is_owned {
            owned.push(key);
        } else {
            foreign.push(key);
        }
    }
    (owned, foreign)
}

/// The version a pending fold carries once a call stamped `call_ver` folds
/// into it: the newer of the two. Two `merge` calls stamp before they take
/// the stripe lock, so the later stamp can fold first; keeping the maximum
/// stops the earlier one from lowering the version the flush applies at.
pub(super) fn folded_pending_ver(pending_ver: Hlc, call_ver: Hlc) -> Hlc {
    pending_ver.max(call_ver)
}

/// Pops every `(deadline_ms, seq) -> key` entry from `by_deadline` whose
/// deadline has passed by `now_ms`, in ascending deadline order, stopping at
/// the first entry not yet due. `BTreeMap` keeps `by_deadline` ordered by
/// its `(deadline_ms, seq)` key, so this touches only the entries due,
/// never scanning the whole stripe, keeping
/// [`Shard::flush_due_pending_merges`]'s sweep cheap at high key counts.
/// Pure over the ordering structure alone (no `V`, no live `Shard`), split
/// out for its own unit test the same way [`merge_window_elapsed`] is.
pub(super) fn drain_due_deadlines<K>(
    by_deadline: &mut BTreeMap<(u64, u64), K>,
    now_ms: u64,
) -> Vec<K> {
    let mut due = Vec::new();
    while let Some(&(deadline_ms, _)) = by_deadline.keys().next() {
        if !merge_window_elapsed(now_ms, deadline_ms) {
            break;
        }
        let (_, key) = by_deadline
            .pop_first()
            .expect("just peeked the smallest key");
        due.push(key);
    }
    due
}

#[allow(
    clippy::unused_async,
    clippy::unused_async_trait_impl,
    reason = "most methods here keep async fn even though this backend never awaits"
)]
impl<K, V> Shard<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// Folds `value` into `key` through the configured [`ConflictResolver`]
    /// without a read, applying through the same versioned-apply path as
    /// [`Shard::insert`]. The folded record carries the shard's default TTL,
    /// if any; a merging resolver keeps the later deadline of the two sides.
    /// With [`Shard::with_merge_coalesce_window`] left at its default zero,
    /// every call applies at once. With a non-zero window, consecutive calls to the same key fold into an in-memory
    /// pending value instead, applying once when the window that opened at
    /// the first call elapses (`crate::cache::tasks::merge_coalesce_task`,
    /// `Cache::close`, and this shard's `Drop` all guarantee a flush);
    /// replication and every [`Event`] then see exactly one record, the
    /// fold, not each call.
    ///
    /// [`Shard::get`]/[`Shard::get_sync`] never consult a pending fold: it
    /// stays invisible to reads for up to one whole window.
    ///
    /// A pending fold orders against other writes as its last call did. The
    /// flush applies it at the version of that call, so an insert after the
    /// call outranks it as it would with no window. A
    /// [`Shard::remove`], [`Shard::remove_many`] or [`Shard::clear`] of the
    /// key discards the fold, and a merge after the remove opens a fresh
    /// one. A tombstone from a peer newer than the last call discards it
    /// too; a peer's invalidation newer than the call applies the fold first
    /// and then drops the copy. A tombstone older than the last call leaves
    /// the fold whole, including contributions folded in before the
    /// tombstone's version.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::ValueTooLarge`] if `value`'s wire frame exceeds
    /// the configured frame cap. See [`Shard::with_max_frame`]. Returns
    /// [`CacheError::OverMemoryCeiling`] as [`Shard::insert`] does; a call
    /// folded into a pending value within the window is checked when it is
    /// made, not when the fold flushes.
    pub async fn merge(&self, key: K, value: V) -> Result<(), CacheError> {
        self.merge_expiring(key, value, None).await
    }

    /// [`Shard::merge`] with a lifespan for this contribution alone,
    /// overriding the shard's default TTL. The folded record's deadline is
    /// the resolver's merge of the two sides' deadlines: the shipped
    /// resolvers keep the later one, so a stored value that outlives `ttl`
    /// keeps its own deadline. Coalesces and replicates exactly as
    /// [`Shard::merge`] does.
    ///
    /// # Errors
    ///
    /// As [`Shard::merge`].
    pub async fn merge_with_ttl(&self, key: K, value: V, ttl: Duration) -> Result<(), CacheError> {
        self.merge_expiring(key, value, Some(ttl)).await
    }

    /// The path [`Shard::merge`] and [`Shard::merge_with_ttl`] share; `ttl`
    /// overrides the shard's default when given.
    async fn merge_expiring(
        &self,
        key: K,
        value: V,
        ttl: Option<Duration>,
    ) -> Result<(), CacheError> {
        self.check_ceiling()?;
        let key_bytes = encode_key(&key)?;
        let encoded = Bytes::from(postcard::to_stdvec(&value).map_err(CodecError::from)?);
        let wire_size = wire::replicate_frame_len(self.name.len(), key_bytes.len(), encoded.len());
        if wire_size > self.max_frame {
            return Err(CacheError::ValueTooLarge {
                cache: self.name.clone(),
                size: encoded.len(),
                limit: self.max_frame,
            });
        }
        let ver = self.stamp_local();
        if self.merge_window.is_zero() {
            self.wait_for_fan_out_room().await;
            let expires_at_ms = self.expiry_for(ver.wall_ms, ttl);
            return self.apply_or_forward(
                key,
                key_bytes,
                ver,
                Incoming::Put {
                    value,
                    expires_at_ms,
                    encoded,
                },
            );
        }
        let expires_at_ms = self.expiry_for(ver.wall_ms, ttl);
        let hash = engine::hash_key_bytes(key_bytes.as_ref());
        let mut stripe = self.pending_merges[engine::stripe_index_from_hash(hash)]
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let PendingMergeStripe {
            entries,
            by_deadline,
            next_seq,
        } = &mut *stripe;
        match entries.entry(key) {
            Entry::Occupied(slot) => {
                self.fold_into_occupied_merge(
                    slot,
                    &key_bytes,
                    encoded,
                    value,
                    ver,
                    expires_at_ms,
                )?;
            }
            Entry::Vacant(slot) => {
                let deadline_ms = merge_deadline_ms(self.now_ms(), self.merge_window);
                let seq = *next_seq;
                *next_seq += 1;
                by_deadline.insert((deadline_ms, seq), slot.key().clone());
                slot.insert(PendingMerge {
                    key_bytes,
                    value,
                    encoded,
                    expires_at_ms,
                    ver,
                    deadline_key: (deadline_ms, seq),
                });
                self.pending_merge_count.fetch_add(1, Ordering::AcqRel);
                drop(stripe);
                self.merge_wake.notify_one();
            }
        }
        Ok(())
    }

    /// [`Shard::merge`]'s occupied-slot branch: folds `value`, stamped `ver`
    /// and expiring at `expires_at_ms`, into whatever this stripe already
    /// holds pending. For a merging resolver, [`fold_sides`] decides which
    /// content survives, as it does against a stored record: both sides
    /// fold through the resolver's `merge`, a lapsed pending entry is
    /// replaced by the new call, a lapsed new call is dropped, and two
    /// lapsed sides fall to the resolver's `winner`, which a non-merging
    /// resolver always uses.
    fn fold_into_occupied_merge(
        &self,
        slot: OccupiedEntry<'_, K, PendingMerge<V>>,
        key_bytes: &Bytes,
        encoded: Bytes,
        value: V,
        ver: Hlc,
        expires_at_ms: Option<u64>,
    ) -> Result<(), CacheError> {
        let fold = if self.resolver.merges() {
            let existing = slot.get();
            fold_sides(
                false,
                existing.ver,
                existing.expires_at_ms,
                ver,
                expires_at_ms,
            )
        } else {
            Fold::Winner
        };
        let (a, b) = {
            let existing = slot.get();
            (
                RecordView {
                    value: Some(existing.encoded.as_ref()),
                    ver: existing.ver,
                    expires_at_ms: existing.expires_at_ms,
                },
                RecordView {
                    value: Some(encoded.as_ref()),
                    ver,
                    expires_at_ms,
                },
            )
        };
        let merged = matches!(fold, Fold::Both)
            .then(|| self.resolver.merge(key_bytes.as_ref(), a, b))
            .flatten();
        match (fold, merged) {
            (
                _,
                Some(Merged {
                    value: merged_bytes,
                    expires_at_ms: merged_expires_at_ms,
                }),
            ) => {
                let merged_value: V =
                    postcard::from_bytes(&merged_bytes).map_err(CodecError::from)?;
                let entry = slot.into_mut();
                entry.value = merged_value;
                entry.encoded = merged_bytes;
                entry.expires_at_ms = merged_expires_at_ms;
                entry.ver = folded_pending_ver(entry.ver, ver);
            }
            // The pending entry is all that is alive: the new call adds
            // nothing.
            (Fold::Stored, None) => {}
            (Fold::Incoming, None) => {
                let entry = slot.into_mut();
                entry.value = value;
                entry.encoded = encoded;
                entry.expires_at_ms = expires_at_ms;
                entry.ver = folded_pending_ver(entry.ver, ver);
            }
            (Fold::Both | Fold::Winner, None) => {
                match self.resolver.winner(key_bytes.as_ref(), a, b) {
                    Winner::B => {
                        let entry = slot.into_mut();
                        entry.value = value;
                        entry.encoded = encoded;
                        entry.expires_at_ms = expires_at_ms;
                        entry.ver = folded_pending_ver(entry.ver, ver);
                    }
                    Winner::A => {
                        // The incoming call lost outright: nothing about the pending entry
                        // changes, mirroring `resolve_and_rebind`'s `IncomingLoses => None`.
                    }
                }
            }
        }
        Ok(())
    }

    /// Applies one flushed [`PendingMerge`] through
    /// [`Shard::apply_or_forward`], exactly as if its folded value were
    /// [`Shard::insert`]ed by the last call it folds: at that call's own
    /// version, so replication and events see one record that orders against
    /// every remove, clear and write issued after the call as the call itself
    /// would have, and a forwarded fold carries the call's version to the
    /// owners. Pushes onto the fan-out queue without awaiting room first,
    /// unlike `Shard::merge`'s immediate-apply branch: this runs from the
    /// merge-coalesce sweep and from `Drop`, where no async runtime may be
    /// left to await on.
    pub(super) fn flush_one_pending_merge(&self, key: K, pending: PendingMerge<V>) {
        let incoming = Incoming::Put {
            value: pending.value,
            expires_at_ms: pending.expires_at_ms,
            encoded: pending.encoded,
        };
        // Best-effort: a forward failure here means the cache is closing,
        // and nothing calling this (the background sweep, `close`, or
        // `Drop`) has anywhere to hand an error to, the same fate an
        // ordinary local write racing a close already gets, documented on
        // `Shard::insert`.
        let _ = self.apply_or_forward(key, pending.key_bytes, pending.ver, incoming);
    }

    /// Whether any fold is pending. One load: the tombstone paths check it
    /// before they encode or lock anything.
    fn has_pending_merges(&self) -> bool {
        self.pending_merge_count.load(Ordering::Acquire) != 0
    }

    /// Test-only: sets `hook` to run once, in the first flush that has taken
    /// a fold, between its take and its apply.
    #[cfg(test)]
    pub(super) fn set_after_take_hook(&self, hook: impl Fn() + Send + Sync + 'static) {
        *self
            .after_take_hook
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(Arc::new(hook));
    }

    #[cfg(test)]
    fn run_after_take_hook(&self) {
        let hook = self
            .after_take_hook
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(hook) = hook {
            hook();
        }
    }

    /// The number of pending folds.
    #[cfg(test)]
    pub(super) fn pending_merge_count(&self) -> usize {
        self.pending_merge_count.load(Ordering::Acquire)
    }

    /// Takes `key`'s pending fold out when `superseded` says so of its
    /// version. `hash` is `key`'s encoded-bytes hash, which picks the stripe.
    fn take_pending_merge_if(
        &self,
        key: &K,
        hash: u64,
        superseded: impl FnOnce(Hlc) -> bool,
    ) -> Option<PendingMerge<V>> {
        let mut stripe = self.pending_merges[engine::stripe_index_from_hash(hash)]
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if !superseded(stripe.entries.get(key)?.ver) {
            return None;
        }
        let taken = stripe.remove(key);
        if taken.is_some() {
            self.pending_merge_count.fetch_sub(1, Ordering::AcqRel);
        }
        taken
    }

    /// Drops `key`'s pending fold, whatever its version. A local remove or
    /// clear calls this before it stamps its tombstone, so a `merge` stamped
    /// after the tombstone folds into nothing and opens a fresh fold, and one
    /// stamped before it that lands late is applied at its own older version
    /// and loses.
    pub(super) fn discard_pending_merge(&self, key: &K, key_bytes: &[u8]) {
        if !self.has_pending_merges() {
            return;
        }
        let hash = engine::hash_key_bytes(key_bytes);
        drop(self.take_pending_merge_if(key, hash, |_| true));
    }

    /// Drops `key`'s pending fold when a tombstone at `tombstone_ver` is newer
    /// than the fold's last call ([`pending_merge_superseded`]). The path for
    /// a tombstone this node did not stamp.
    pub(super) fn discard_pending_merge_before(&self, key: &K, tombstone_ver: Hlc) {
        if !self.has_pending_merges() {
            return;
        }
        let Ok(key_bytes) = encode_key(key) else {
            return;
        };
        let hash = engine::hash_key_bytes(key_bytes.as_ref());
        drop(self.take_pending_merge_if(key, hash, |pending_ver| {
            pending_merge_superseded(pending_ver, tombstone_ver)
        }));
    }

    /// Applies `key`'s pending fold now when `ver` is newer than its last
    /// call, so a peer's invalidation of the key lands after the merge it
    /// follows. A bare invalidation cannot say whether the peer removed the
    /// key or wrote it, and the fold's contribution has replicated nowhere
    /// yet, so dropping it would lose it from the cluster; applying it
    /// first, then invalidating, replays the order a zero window gives.
    pub(super) fn flush_pending_merge_before(&self, key: &K, hash: u64, ver: Hlc) {
        if !self.has_pending_merges() {
            return;
        }
        let _flushing = self.begin_flush();
        if let Some(pending) = self.take_pending_merge_if(key, hash, |pending_ver| {
            pending_merge_superseded(pending_ver, ver)
        }) {
            #[cfg(test)]
            self.run_after_take_hook();
            self.flush_one_pending_merge(key.clone(), pending);
        }
    }

    /// Marks a flush in flight until the returned guard drops. Every path that
    /// takes a fold out of its stripe holds it from the take through the last
    /// apply of what it took, so a key is always either pending or stored,
    /// never between the two, to [`Shard::clear`]'s snapshot, which waits for
    /// the guards to drop. A guard is never held across an await.
    fn begin_flush(&self) -> RwLockReadGuard<'_, ()> {
        self.merge_flush_gate
            .read()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Takes `select`'s pick out of one stripe and applies it, holding
    /// [`Shard::begin_flush`]'s guard across both.
    fn flush_stripe(
        &self,
        stripe_lock: &StdMutex<PendingMergeStripe<K, V>>,
        select: impl FnOnce(&mut PendingMergeStripe<K, V>) -> Vec<(K, PendingMerge<V>)>,
    ) {
        let _flushing = self.begin_flush();
        let due = {
            let mut stripe = stripe_lock.lock().unwrap_or_else(PoisonError::into_inner);
            let due = select(&mut stripe);
            self.pending_merge_count
                .fetch_sub(due.len(), Ordering::AcqRel);
            due
        };
        #[cfg(test)]
        if !due.is_empty() {
            self.run_after_take_hook();
        }
        for (key, entry) in due {
            self.flush_one_pending_merge(key, entry);
        }
    }

    /// The keys [`Shard::clear`] tombstones, and the pending folds it settles
    /// on the way. Waits for every flush in flight, so each pending key is
    /// still pending and each flushed one is stored, and snapshots both under
    /// that one wait. A fold for a key this node owns is discarded by the
    /// tombstone and listed with the stored keys. A fold for a key this node
    /// does not own, which only a `Mode::Distributed` shard has, is applied as
    /// a zero window would have, which forwards it to the owners, and is not
    /// listed: this node holds nothing of that key for a tombstone to remove,
    /// and a tombstone forwarded for it would delete the owners' copy.
    pub(super) fn keys_to_clear(&self) -> Vec<K> {
        let _clearing = self
            .merge_flush_gate
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        let mut keys = Vec::new();
        let mut forwarded = Vec::new();
        if self.has_pending_merges() {
            for stripe_lock in &*self.pending_merges {
                let mut stripe = stripe_lock.lock().unwrap_or_else(PoisonError::into_inner);
                let (kept, flush) =
                    split_pending_for_clear(stripe.entries.iter().map(|(key, entry)| {
                        (key.clone(), self.owns_key(entry.key_bytes.as_ref()))
                    }));
                keys.extend(kept);
                for key in flush {
                    if let Some(entry) = stripe.remove(&key) {
                        self.pending_merge_count.fetch_sub(1, Ordering::AcqRel);
                        forwarded.push((key, entry));
                    }
                }
            }
        }
        for (key, entry) in forwarded {
            self.flush_one_pending_merge(key, entry);
        }
        let pending: HashSet<K> = keys.iter().cloned().collect();
        keys.extend(self.keys().into_iter().filter(|key| !pending.contains(key)));
        keys
    }

    /// Flushes every key whose [`Shard::merge`] coalescing window has
    /// elapsed by `now_ms`. Visits [`Shard::pending_merges`]'s
    /// [`BUCKET_COUNT`] stripes one at a time, locking each only long
    /// enough for [`drain_due_deadlines`] to pop that stripe's due entries
    /// out of its deadline-ordered index, never one lock (or one linear
    /// scan) for the whole shard's pending state, so many keys coalescing
    /// at once don't serialize this sweep on each other. The background
    /// sweep (`crate::cache::tasks::merge_coalesce_task`) is the only production
    /// caller; `pub(crate)` so `crate::cache` can drive it without owning
    /// the pending map itself.
    pub(crate) fn flush_due_pending_merges(&self, now_ms: u64) {
        for stripe_lock in &*self.pending_merges {
            self.flush_stripe(stripe_lock, |stripe| {
                drain_due_deadlines(&mut stripe.by_deadline, now_ms)
                    .into_iter()
                    .filter_map(|key| stripe.entries.remove(&key).map(|entry| (key, entry)))
                    .collect()
            });
        }
    }

    /// Flushes every currently pending coalesced merge, regardless of
    /// whether its window has elapsed: `crate::cache::Cache::close` and
    /// this shard's own `Drop` call this so nothing folded in stays
    /// invisible past the cache's own lifetime, closing the one gap
    /// [`Shard::flush_due_pending_merges`]'s window-bound leaves. Drains
    /// each stripe in turn, the same one-stripe-at-a-time locking
    /// [`Shard::flush_due_pending_merges`] uses.
    pub(crate) fn flush_all_pending_merges(&self) {
        for stripe_lock in &*self.pending_merges {
            self.flush_stripe(stripe_lock, |stripe| {
                stripe.by_deadline.clear();
                std::mem::take(&mut stripe.entries).into_iter().collect()
            });
        }
    }

    /// The earliest deadline among every currently pending coalesced merge
    /// across every stripe, or `None` when nothing is pending anywhere.
    /// `crate::cache::tasks::merge_coalesce_task` sleeps until this to flush right
    /// at the window's edge instead of polling. Each stripe's own earliest
    /// deadline is its `by_deadline` index's first key, no scan of that
    /// stripe's entries needed, so this costs one brief lock per stripe,
    /// not one lock over the whole shard's pending state.
    pub(crate) fn next_pending_merge_deadline_ms(&self) -> Option<u64> {
        self.pending_merges
            .iter()
            .filter_map(|stripe_lock| {
                stripe_lock
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .by_deadline
                    .keys()
                    .next()
                    .map(|&(deadline_ms, _)| deadline_ms)
            })
            .min()
    }

    /// Resolves the next time a [`Shard::merge`] call opens a fresh
    /// coalescing window (a key with nothing already pending), so
    /// `crate::cache::tasks::merge_coalesce_task` can recompute its sleep-until
    /// instead of parking past a deadline that did not exist yet when it
    /// last checked.
    pub(crate) fn merge_wake_notified(&self) -> impl Future<Output = ()> + '_ {
        self.merge_wake.notified()
    }

    /// Test-only: `(version, value)` of `key`'s currently pending
    /// [`Shard::merge`] fold, if any, without the caller needing to know
    /// which of [`Self::pending_merges`]'s stripes it hashes into.
    #[cfg(test)]
    pub(super) fn pending_merge_snapshot(&self, key: &K) -> Option<(Hlc, V)> {
        let key_bytes = encode_key(key).ok()?;
        let hash = engine::hash_key_bytes(key_bytes.as_ref());
        self.pending_merges[engine::stripe_index_from_hash(hash)]
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entries
            .get(key)
            .map(|entry| (entry.ver, entry.value.clone()))
    }
}

impl<K, V> Drop for Shard<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// Flushes every pending coalesced [`Shard::merge`] fold before this
    /// shard's last reference goes away, so a fold nothing ever explicitly
    /// closed still lands rather than vanishing silently. A no-op whenever
    /// nothing is pending, which is every shard
    /// [`Shard::with_merge_coalesce_window`] never touched.
    fn drop(&mut self) {
        self.flush_all_pending_merges();
    }
}

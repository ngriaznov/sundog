//! The merge path of a [`Shard`]: stored merges, the coalesce window and the flush of pending
//! merges, with the pure deadline helpers they decide by.

use std::collections::BTreeMap;
use std::collections::hash_map::{Entry, OccupiedEntry};
use std::future::Future;
use std::hash::Hash;
use std::sync::PoisonError;
use std::time::Duration;

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

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
    /// [`Shard::insert`]. With [`Shard::with_merge_coalesce_window`] left
    /// at its default zero, every call applies at once. With a non-zero
    /// window, consecutive calls to the same key fold into an in-memory
    /// pending value instead, applying once when the window that opened at
    /// the first call elapses (`crate::cache::merge_coalesce_task`,
    /// `Cache::close`, and this shard's `Drop` all guarantee a flush);
    /// replication and every [`Event`] then see exactly one record, the
    /// fold, not each call.
    ///
    /// [`Shard::get`]/[`Shard::get_sync`] never consult a pending fold: it
    /// stays invisible to reads for up to one whole window.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::ValueTooLarge`] if `value`'s wire frame exceeds
    /// the configured frame cap. See [`Shard::with_max_frame`]. Returns
    /// [`CacheError::OverMemoryCeiling`] as [`Shard::insert`] does; a call
    /// folded into a pending value within the window is checked when it is
    /// made, not when the fold flushes.
    pub async fn merge(&self, key: K, value: V) -> Result<(), CacheError> {
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
            let expires_at_ms = self.expiry_for(None);
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
                self.fold_into_occupied_merge(slot, &key_bytes, encoded, value, ver)?;
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
                    expires_at_ms: self.expiry_for(None),
                    ver,
                });
                drop(stripe);
                self.merge_wake.notify_one();
            }
        }
        Ok(())
    }

    /// [`Shard::merge`]'s occupied-slot branch: folds `value` into whatever
    /// this stripe already holds pending, via the resolver's `merge` or `winner` call.
    fn fold_into_occupied_merge(
        &self,
        slot: OccupiedEntry<'_, K, PendingMerge<V>>,
        key_bytes: &Bytes,
        encoded: Bytes,
        value: V,
        ver: Hlc,
    ) -> Result<(), CacheError> {
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
                    expires_at_ms: self.expiry_for(None),
                },
            )
        };
        let merged = self
            .resolver
            .merges()
            .then(|| self.resolver.merge(key_bytes.as_ref(), a, b))
            .flatten();
        match merged {
            Some(Merged {
                value: merged_bytes,
                expires_at_ms,
            }) => {
                let merged_value: V =
                    postcard::from_bytes(&merged_bytes).map_err(CodecError::from)?;
                let entry = slot.into_mut();
                entry.value = merged_value;
                entry.encoded = merged_bytes;
                entry.expires_at_ms = expires_at_ms;
                entry.ver = ver;
            }
            None => match self.resolver.winner(key_bytes.as_ref(), a, b) {
                Winner::B => {
                    let entry = slot.into_mut();
                    entry.value = value;
                    entry.encoded = encoded;
                    entry.expires_at_ms = self.expiry_for(None);
                    entry.ver = ver;
                }
                Winner::A => {
                    // The incoming call lost outright: nothing about the pending entry
                    // changes, mirroring `resolve_and_rebind`'s `IncomingLoses => None`.
                }
            },
        }
        Ok(())
    }

    /// Applies one flushed [`PendingMerge`] through
    /// [`Shard::apply_or_forward`], exactly as if its folded value were
    /// freshly [`Shard::insert`]ed: a fresh version stamped now, so
    /// replication and events see the fold as one write happening at flush
    /// time, not backdated to whichever `merge` call opened the window.
    /// Pushes onto the fan-out queue without awaiting room first, unlike
    /// `Shard::merge`'s immediate-apply branch: this runs from the
    /// merge-coalesce sweep and from `Drop`, where no async runtime may be
    /// left to await on.
    fn flush_one_pending_merge(&self, key: K, pending: PendingMerge<V>) {
        let ver = self.stamp_local();
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
        let _ = self.apply_or_forward(key, pending.key_bytes, ver, incoming);
    }

    /// Flushes every key whose [`Shard::merge`] coalescing window has
    /// elapsed by `now_ms`. Visits [`Shard::pending_merges`]'s
    /// [`BUCKET_COUNT`] stripes one at a time, locking each only long
    /// enough for [`drain_due_deadlines`] to pop that stripe's due entries
    /// out of its deadline-ordered index, never one lock (or one linear
    /// scan) for the whole shard's pending state, so many keys coalescing
    /// at once don't serialize this sweep on each other. The background
    /// sweep (`crate::cache::merge_coalesce_task`) is the only production
    /// caller; `pub(crate)` so `crate::cache` can drive it without owning
    /// the pending map itself.
    pub(crate) fn flush_due_pending_merges(&self, now_ms: u64) {
        for stripe_lock in &*self.pending_merges {
            let due: Vec<(K, PendingMerge<V>)> = {
                let mut stripe = stripe_lock.lock().unwrap_or_else(PoisonError::into_inner);
                drain_due_deadlines(&mut stripe.by_deadline, now_ms)
                    .into_iter()
                    .filter_map(|key| stripe.entries.remove(&key).map(|entry| (key, entry)))
                    .collect()
            };
            for (key, entry) in due {
                self.flush_one_pending_merge(key, entry);
            }
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
            let due: Vec<(K, PendingMerge<V>)> = {
                let mut stripe = stripe_lock.lock().unwrap_or_else(PoisonError::into_inner);
                stripe.by_deadline.clear();
                std::mem::take(&mut stripe.entries).into_iter().collect()
            };
            for (key, entry) in due {
                self.flush_one_pending_merge(key, entry);
            }
        }
    }

    /// The earliest deadline among every currently pending coalesced merge
    /// across every stripe, or `None` when nothing is pending anywhere.
    /// `crate::cache::merge_coalesce_task` sleeps until this to flush right
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
    /// `crate::cache::merge_coalesce_task` can recompute its sleep-until
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

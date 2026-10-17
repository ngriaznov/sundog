//! Housekeeping on `Engine`: tombstone collection, the expiry sweep and the compaction scan and replace.

use std::hash::Hash;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::hlc::Hlc;
use crate::store::{
    BUCKET_COUNT, CompactionBounds, ConflictResolver, Quiescence, Tombstone, crdt,
    entry_fingerprint,
};

#[cfg(all(doc, feature = "spill"))]
use super::EntryState;
use super::expiry::{
    LONG_TTL, SweepFate, TombstoneRetention, decode_expiry, encode_expiry, gc_defer_decision,
    pause_hold_after, paused_since, sweep_fate, swept_tombstone_deadlines,
};
use super::keys::{
    digest_slot, hash_key_bytes, hasher_for, part_index_from_hash, stripe_index_from_hash,
};
use super::record::{
    build_resident_record, decode_key, record_key, record_key_bytes, record_value_bytes, set_record,
};
#[cfg(doc)]
use super::slab::Slab;
use super::{Engine, Stripe, is_resident, is_spilled};

/// One record [`Engine::compact`]'s scan found a compacted form for.
pub(crate) struct CompactCandidate<K> {
    pub(crate) key: K,
    pub(crate) key_bytes: Bytes,
    pub(crate) stale_ver: Hlc,
    pub(crate) new_encoded: Bytes,
}

/// One [`Engine::compact`] call's result: the records whose resolver
/// produced a compacted form, each with the version it is read at, and
/// how many stripes the call walked before its entry budget ran out. A
/// caller that wants the whole keyspace examined keeps calling until the
/// visited stripes add up to [`BUCKET_COUNT`](crate::store::BUCKET_COUNT).
pub(crate) struct CompactScan<K> {
    pub(crate) candidates: Vec<CompactCandidate<K>>,
    pub(crate) stripes_visited: usize,
}

/// [`Engine::compact`]'s paced shrink-pass rule: `true` once a stripe's
/// live count `len` sits at or below an eighth of its slab `capacity`,
/// the point past which the arena wastes at least seven times what it
/// holds. Well under the range an unhinted `Vec`'s own amortized growth
/// settles into, so a naturally growing stripe is never shrunk out from
/// under itself; this only fires for an oversized `capacity_hint` or a
/// stripe drained by ownership rebalancing. Pure; unit tested directly.
pub(super) fn should_shrink_stripe(len: usize, capacity: usize) -> bool {
    capacity > 0 && len <= capacity / 8
}

/// One [`Engine::compact_replace_if_current`] call's write.
pub(crate) struct CompactReplace<'a, K, V> {
    pub(crate) key: &'a K,
    pub(crate) key_bytes: &'a [u8],
    pub(crate) hash: u64,
    pub(crate) expected_ver: Hlc,
    pub(crate) new_ver: Hlc,
    pub(crate) value: V,
    pub(crate) encoded: Bytes,
}

impl<K, V> Engine<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// Drops tombstones past `tombstone_ttl_ms`, or, while collection defers,
    /// past their cap, correcting the digest: `tombstone_max_ttl` for a
    /// delete, the ttl deadline itself for the tombstone of an expired entry.
    ///
    /// Collection defers when the caller says so (`defer`), and, with the pause
    /// guard on ([`Engine::guarding_pauses`]), when the wall-clock gap since
    /// the last call exceeds [`expiry::pause_gap_ms`] and for
    /// `tombstone_ttl_ms` after that: a node that did not run for that long
    /// may hold values whose deletes it never saw, and its peers hold those
    /// tombstones only so long. A later pause extends the hold.
    ///
    /// Remembers the decision for [`Engine::apply_many`]: while collection is
    /// deferred, a delete a peer sends that arrives lapsed past its
    /// `ttl_deadline_ms` but not its `max_deadline_ms` is still worth
    /// keeping, and one that arrives past its `max_deadline_ms` is ignored.
    /// An expiry tombstone's two deadlines are equal, so it is never kept
    /// past its ttl.
    ///
    /// Returns the gap in milliseconds when this call starts a pause hold,
    /// for the caller to report, and `None` otherwise.
    pub(crate) fn gc_tombstones(
        &self,
        defer: bool,
        now_ms: u64,
        tombstone_ttl_ms: u64,
    ) -> Option<u64> {
        let (defer, entered_hold_gap) = if self.pause_guard {
            let prior = self.last_gc_ms.load(Ordering::Relaxed);
            let paused = paused_since(prior, now_ms, tombstone_ttl_ms);
            let hold = self.pause_hold_until_ms.load(Ordering::Relaxed);
            let (effective, entered) = gc_defer_decision(defer, paused, hold, now_ms);
            self.pause_hold_until_ms.store(
                pause_hold_after(hold, paused, now_ms, tombstone_ttl_ms),
                Ordering::Release,
            );
            self.gc_deferring.store(effective, Ordering::Release);
            // The collection time goes last: an arrival that reads it with
            // `Acquire` (`Engine::deferring_at`) sees the hold and the
            // deferral that belong to it, so the gap it reads as closed is
            // never closed without the node deferring.
            self.last_gc_ms.store(now_ms.max(1), Ordering::Release);
            (effective, entered.then(|| now_ms.saturating_sub(prior)))
        } else {
            self.gc_deferring.store(defer, Ordering::Relaxed);
            (defer, None)
        };
        for (idx, stripe_lock) in self.stripes.iter().enumerate() {
            let mut stripe = stripe_lock.write();
            stripe.tombstones.retain(|key_bytes, t| {
                let past_ttl = now_ms >= t.ttl_deadline_ms;
                let past_max = now_ms >= t.max_deadline_ms;
                let collect = past_ttl && (!defer || past_max);
                if collect {
                    let part = part_index_from_hash(hash_key_bytes(key_bytes));
                    self.digest[digest_slot(idx, part)]
                        .fetch_xor(entry_fingerprint(key_bytes, t.ver), Ordering::Relaxed);
                }
                !collect
            });
        }
        entered_hold_gap
    }

    /// The engine's only free-running housekeeping: visits every stripe whose
    /// `next_expiry_ms` is due, or every stripe if TTI is configured,
    /// removes expired/idle live entries, corrects the digest and total
    /// weight, and recomputes `next_expiry_ms` exactly.
    ///
    /// With `expiry_tombstones` set, an entry removed for having passed its
    /// expiry leaves a tombstone at its own version, anchored on that
    /// expiry (see [`sweep_fate`]): a replica that missed the entry's
    /// overwrite cannot bring the overwritten value back after the sweep
    /// any more than before it. The tombstone has the live entry's
    /// fingerprint, so the digest does not change here and
    /// [`Engine::gc_tombstones`] drops it from the digest when it collects.
    /// Without it, and for an entry that went idle, the removal leaves no
    /// trace.
    pub(crate) fn sweep(&self, now_ms: u64, expiry_tombstones: Option<TombstoneRetention>) {
        for (idx, stripe_lock) in self.stripes.iter().enumerate() {
            let due = stripe_lock.read().next_expiry_ms <= now_ms;
            if !due && self.tti_ms.is_none() {
                continue;
            }
            let mut guard = stripe_lock.write();
            // One explicit reborrow, so stripe.live/stripe.long_ttl below
            // are disjoint field projections the borrow checker can split.
            let stripe: &mut Stripe<K, V> = &mut guard;
            let mut removed_weight = 0u64;
            let mut removed_count = 0u64;
            let mut removed_spilled = 0usize;
            let mut new_next = u64::MAX;
            let mut cleared_long_ttl: Vec<Bytes> = Vec::new();
            let mut expired: Vec<(Bytes, Hlc, u64)> = Vec::new();
            stripe.live.retain(hasher_for, |live| {
                let key_bytes = record_key(&live.record);
                let expires_at_ms =
                    decode_expiry(live.wall_ms, live.expiry, key_bytes, &stripe.long_ttl);
                let absent = self.is_absent(live, key_bytes, &stripe.long_ttl, now_ms);
                let fate = sweep_fate(absent, expires_at_ms, now_ms, expiry_tombstones.is_some());
                if fate == SweepFate::Keep {
                    if let Some(exp) = expires_at_ms {
                        new_next = new_next.min(exp);
                    }
                    return true;
                }
                if let (SweepFate::Tombstone, Some(lapsed_at)) = (fate, expires_at_ms) {
                    // The tombstone carries the fingerprint on: no digest
                    // change here.
                    expired.push((record_key_bytes(&live.record), live.ver(), lapsed_at));
                } else {
                    let part = part_index_from_hash(hash_key_bytes(key_bytes));
                    self.digest[digest_slot(idx, part)]
                        .fetch_xor(entry_fingerprint(key_bytes, live.ver()), Ordering::Relaxed);
                }
                removed_weight += u64::from(live.weight);
                removed_count += 1;
                if is_spilled(live) {
                    removed_spilled += 1;
                }
                if live.expiry == LONG_TTL {
                    cleared_long_ttl.push(Bytes::copy_from_slice(key_bytes));
                }
                false
            });
            for key_bytes in &cleared_long_ttl {
                stripe.long_ttl.remove(key_bytes.as_ref());
            }
            if let Some(retention) = expiry_tombstones {
                for (key_bytes, ver, lapsed_at) in expired {
                    let (ttl_deadline_ms, max_deadline_ms) =
                        swept_tombstone_deadlines(lapsed_at, now_ms, retention);
                    let prior = stripe.tombstones.insert(
                        key_bytes,
                        Tombstone {
                            ver,
                            ttl_deadline_ms,
                            max_deadline_ms,
                            expired_at_ms: Some(lapsed_at),
                        },
                    );
                    debug_assert!(
                        prior.is_none(),
                        "a key is never live and tombstoned at once, so the digest would count it twice"
                    );
                }
            }
            stripe.next_expiry_ms = new_next;
            drop(guard);
            if removed_weight > 0 {
                self.total_weight
                    .fetch_sub(removed_weight, Ordering::Relaxed);
            }
            if removed_count > 0 {
                self.live_count.fetch_sub(removed_count, Ordering::Relaxed);
            }
            self.note_spill_departures(removed_spilled);
        }
    }

    /// One rate-limited pass of the CRDT writer-retirement sweep: visits
    /// stripes from [`Engine::compact_cursor`] under a read lock each,
    /// calling `resolver.compact(key_bytes, encoded, now_ms, retire, quiet,
    /// bounds)` on every resident live entry and collecting the `Some`
    /// results; never mutates a stripe's live entries, versions, or keys
    /// this way, and [`super::super::ShardOps::compact_pass`] applies each
    /// candidate separately, via [`Engine::compact_replace_if_current`]. A
    /// [`EntryState::Spilled`] entry is skipped until promoted back to
    /// `Resident`.
    ///
    /// Once a visited stripe's scan finishes, [`should_shrink_stripe`]
    /// decides whether its `live` slab is worth reclaiming (an oversized
    /// `capacity_hint` or a stripe drained by rebalancing): if so, this
    /// pass takes a separate write lock and calls [`Slab::shrink_to_fit`].
    ///
    /// `max_entries` bounds entries examined, not entries eligible; a
    /// stripe already in progress always finishes. The cursor advances past
    /// every visited stripe, wrapping at `BUCKET_COUNT`; `max_entries == 0`
    /// visits nothing. Returns the candidates and how many stripes were
    /// walked.
    pub(crate) fn compact(
        &self,
        resolver: &dyn ConflictResolver,
        now_ms: u64,
        retire: &dyn Fn(crdt::WriterId) -> bool,
        quiet: Quiescence,
        bounds: CompactionBounds,
        max_entries: usize,
    ) -> CompactScan<K> {
        if max_entries == 0 {
            return CompactScan {
                candidates: Vec::new(),
                stripes_visited: 0,
            };
        }
        let start = stripe_index_from_hash(self.compact_cursor.load(Ordering::Relaxed));
        let mut out = Vec::new();
        let mut examined = 0usize;
        let mut visited = 0usize;
        let mut next_start = start;
        while visited < BUCKET_COUNT {
            let idx = (start + visited) % BUCKET_COUNT;
            visited += 1;
            next_start = (idx + 1) % BUCKET_COUNT;
            let stripe = self.stripes[idx].read();
            self.note_compact_lock_acquisition();
            for live in stripe.live.iter() {
                // Never compacted off-lock: reading its bytes back in would
                // cost a disk read this sweep does not pay for.
                if !is_resident(live) {
                    continue;
                }
                let encoded = record_value_bytes(&live.record);
                examined += 1;
                if let Some(new_encoded) = resolver.compact(
                    record_key(&live.record),
                    encoded.as_ref(),
                    now_ms,
                    retire,
                    quiet,
                    bounds,
                ) {
                    out.push(CompactCandidate {
                        key: decode_key(record_key(&live.record)),
                        key_bytes: record_key_bytes(&live.record),
                        stale_ver: live.ver(),
                        new_encoded,
                    });
                }
            }
            let shrink = should_shrink_stripe(stripe.live.len(), stripe.live.capacity());
            drop(stripe);
            // A separate write lock: reclaiming is rare enough that a
            // second lock here beats holding one for the whole scan.
            if shrink {
                self.stripes[idx].write().live.shrink_to_fit(hasher_for);
            }
            if examined >= max_entries {
                break;
            }
        }
        self.compact_cursor
            .store(next_start as u64, Ordering::Relaxed);
        CompactScan {
            candidates: out,
            stripes_visited: visited,
        }
    }

    /// Replaces a live, resident entry's payload and version with
    /// `replace.value`/`replace.encoded`/`replace.new_ver` iff it is still
    /// exactly at `replace.expected_ver`: the version-gated, no-merge write
    /// [`super::super::ShardOps::compact_pass`] uses for every [`Self::compact`]
    /// candidate, in place of the ordinary merge-based apply path. A
    /// merge-based apply would silently restore the receipt a compaction
    /// pass pruned, since [`ConflictResolver::merge`] only ever unions
    /// state, so this replaces directly instead. Mints a fresh version from
    /// the compacted bytes ([`super::super::compacted_version`]) rather than
    /// reusing `expected_ver`, since two different contents must never
    /// share one version; see
    /// `pn_counter_compaction_under_gossip_keeps_the_exact_total` in
    /// `store::prop_tests` for why this still converges. Returns `false`, a
    /// no-op, once the entry has moved on: a different version, not live,
    /// or currently spilled.
    pub(crate) fn compact_replace_if_current(&self, replace: CompactReplace<'_, K, V>) -> bool {
        let CompactReplace {
            key,
            key_bytes,
            hash,
            expected_ver,
            new_ver,
            value,
            encoded,
        } = replace;
        let bucket = stripe_index_from_hash(hash);
        let part = part_index_from_hash(hash);
        let (old_weight, new_weight) = {
            let mut guard = self.stripes[bucket].write();
            // One explicit reborrow, as in `sweep`, so the fields below
            // borrow disjointly.
            let stripe: &mut Stripe<K, V> = &mut guard;
            let Some(live) = stripe
                .live
                .find_mut(hash, |l| record_key(&l.record) == key_bytes)
            else {
                return false;
            };
            if live.ver() != expected_ver || !is_resident(live) {
                return false;
            }
            let new_weight = self.weigher.as_ref().map_or(1, |w| w(key, &value));
            let old_weight = live.weight;
            let old_wall_ms = live.wall_ms;
            let old_expiry = live.expiry;
            let record = build_resident_record(record_key(&live.record), encoded.as_ref());
            set_record(live, record, self.resident_bytes.as_deref());
            live.weight = new_weight;
            live.set_ver(new_ver);
            // new_ver carries a new wall_ms; re-deriving expiry from the
            // real absolute deadline keeps it exact across the version bump.
            let abs_expiry = decode_expiry(old_wall_ms, old_expiry, key_bytes, &stripe.long_ttl);
            let (expiry, long_ttl_value) = encode_expiry(new_ver.wall_ms, abs_expiry);
            live.expiry = expiry;
            match long_ttl_value {
                Some(deadline) => {
                    stripe
                        .long_ttl
                        .insert(Bytes::copy_from_slice(key_bytes), deadline);
                }
                None => {
                    stripe.long_ttl.remove(key_bytes);
                }
            }
            (old_weight, new_weight)
        };
        self.digest[digest_slot(bucket, part)].fetch_xor(
            entry_fingerprint(key_bytes, expected_ver) ^ entry_fingerprint(key_bytes, new_ver),
            Ordering::Relaxed,
        );
        self.total_weight
            .fetch_add(u64::from(new_weight), Ordering::Relaxed);
        self.total_weight
            .fetch_sub(u64::from(old_weight), Ordering::Relaxed);
        true
    }

    #[cfg_attr(not(test), allow(clippy::unused_self))]
    fn note_compact_lock_acquisition(&self) {
        #[cfg(test)]
        self.compact_lock_acquisitions
            .fetch_add(1, Ordering::Relaxed);
    }
}

//! The versioned write path: `apply_locked` and its put and tombstone halves, `Engine::apply_many` with its pre-fold and the spilled conflict-bytes prefetch.

#[cfg(feature = "spill")]
use std::collections::HashMap;
use std::hash::Hash;
#[cfg(feature = "spill")]
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
#[cfg(feature = "spill")]
use parking_lot::RwLock;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::hlc::Hlc;
#[cfg(feature = "spill")]
use crate::store::spill::{SpillLoc, SpillTier};
use crate::store::{ConflictResolver, Incoming, Tombstone, Weigher, entry_fingerprint};

#[cfg(feature = "spill")]
use super::EntryState;
use super::expiry::{absent_at, decode_expiry, encode_expiry};
use super::keys::{digest_slot, hasher_for, part_index_from_hash, stripe_index_from_hash};
use super::merge::resolve_and_rebind;
#[cfg(doc)]
use super::merge::{merge_version, resolve_conflict};
#[cfg(doc)]
use super::prefold::peek_stored_seed;
use super::prefold::{BatchEntry, group_prefold_runs, peek_prefold_seeds, prefold_batch};
use super::record::{build_resident_record, record_key, record_value_bytes};
use super::{ApplyOutcome, Engine, Live, Reservation, Stripe, Written, remove_live};

/// Reads a currently-[`EntryState::Spilled`] stored record's value bytes off
/// disk, so [`resolve_conflict`]'s collision decision can fold a
/// value-aware resolver against them instead of degrading to its
/// value-less guard. Called off-lock by [`prefetch_spilled_conflict_bytes`]
/// for the overwhelming majority of calls, and under the stripe write lock
/// by [`apply_locked`] itself on the rare fallback where prefetch missed
/// this pointer, since losing a real value to a stale prefetch would
/// violate [`resolve_conflict`]'s never-drop-a-real-value guarantee.
///
/// `None` on anything [`SpillTier::read_at`] treats as an ordinary outcome
/// (a torn write, a failed checksum, a rotated-past generation, a genuine
/// I/O error): degrades like a value-less stored side, never panics.
#[cfg(feature = "spill")]
pub(super) fn read_spilled_for_conflict(tier: &SpillTier, loc: SpillLoc) -> Option<Bytes> {
    tier.read_at(loc)
        .ok()
        .flatten()
        .map(|spilled| spilled.encoded)
}

/// Reads back, off any stripe lock, the value bytes of every currently-
/// [`EntryState::Spilled`] stored record among `keys` a merge might need,
/// feeding both [`peek_stored_seed`]'s pre-fold seeding and
/// [`apply_locked`]'s stored-side lookup; see [`read_spilled_for_conflict`]
/// for why a fallback under the write lock still exists. A brief stripe
/// read lock finds which keys are spilled, then every read happens off-lock,
/// keyed by [`SpillLoc`] so a moved entry misses rather than serving stale
/// content. A merging `resolver` never folds a record that reads as absent
/// at `now_ms` (expired, or idle for `tti_ms`), so that record is not read;
/// any other resolver sees every spilled record's bytes. Skips the read lock
/// entirely when `resolver` never inspects stored bytes
/// ([`ConflictResolver::needs_value_bytes`] is `false`) or no tier is
/// attached.
#[cfg(feature = "spill")]
pub(super) fn prefetch_spilled_conflict_bytes<'a, K, V>(
    stripe_lock: &RwLock<Stripe<K, V>>,
    keys: impl IntoIterator<Item = (u64, &'a Bytes)>,
    resolver: &dyn ConflictResolver,
    spill: Option<&SpillTier>,
    tti_ms: Option<u64>,
    now_ms: u64,
) -> HashMap<SpillLoc, Bytes> {
    let Some(tier) = spill.filter(|_| resolver.needs_value_bytes()) else {
        return HashMap::new();
    };
    let merges = resolver.merges();
    let mut locs: std::collections::HashSet<SpillLoc> = std::collections::HashSet::new();
    {
        let stripe = stripe_lock.read();
        for (hash, key_bytes) in keys {
            if stripe.tombstones.contains_key(key_bytes.as_ref()) {
                continue;
            }
            if let Some(live) = stripe
                .live
                .find(hash, |l| record_key(&l.record) == key_bytes.as_ref())
                && let EntryState::Spilled(loc) = &live.state
                && !(merges
                    && absent_at(live, key_bytes.as_ref(), &stripe.long_ttl, tti_ms, now_ms))
            {
                locs.insert(*loc);
            }
        }
    }
    locs.into_iter()
        .filter_map(|loc| read_spilled_for_conflict(tier, loc).map(|bytes| (loc, bytes)))
        .collect()
}

/// Which writes a batch apply ([`Engine::apply_many_in`]) stores.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ApplyScope {
    /// Every write the resolver picks.
    Any,
    /// A write only as a fold into a key held visible; every other write is
    /// rejected.
    HeldOnly,
}

/// One entry's identity for the versioned-apply path, in place of three positional parameters.
pub(crate) struct EntryKey<K> {
    pub(super) hash: u64,
    pub(super) key: K,
    pub(super) key_bytes: Bytes,
}

/// An [`Incoming::Put`]'s three fields, bundled for [`apply_put`].
pub(crate) struct PutWrite<V> {
    value: V,
    expires_at_ms: Option<u64>,
    encoded: Bytes,
}

/// Borrowed engine-wide state one versioned-apply call needs, in place of a dozen positional parameters.
pub(crate) struct ApplyCtx<'a, K, V> {
    /// Already resolved for this entry's part; a batch apply builds a fresh one per entry.
    pub(super) digest_bucket: &'a AtomicU64,
    pub(super) total_weight: &'a AtomicU64,
    pub(super) live_count: &'a AtomicU64,
    pub(super) weigher: Option<&'a Weigher<K, V>>,
    pub(super) tti_ms: Option<u64>,
    pub(super) resolver: &'a dyn ConflictResolver,
    #[cfg(feature = "spill")]
    pub(super) prefetched_spilled: &'a HashMap<SpillLoc, Bytes>,
    #[cfg(feature = "spill")]
    pub(super) spill: Option<&'a SpillTier>,
    pub(super) tombstone_ttl_ms: u64,
    pub(super) tombstone_max_ttl_ms: u64,
    pub(super) now_ms: u64,
    /// Applies a write only as a fold into a visible live entry; see
    /// [`ApplyScope::HeldOnly`].
    pub(super) held_only: bool,
}

/// The live entry [`apply_locked`] finds for a key, as the collision
/// decision reads it.
pub(super) struct StoredLive {
    pub(super) ver: Hlc,
    /// The value bytes: `None` for a spilled entry that is not read, or
    /// whose read failed.
    pub(super) encoded: Option<Bytes>,
    pub(super) expires_at_ms: Option<u64>,
    /// What a read at `now_ms` would see: an expired or idle entry still
    /// takes part in version ordering and still gets displaced, and a write
    /// over it counts as a creation, the same as after a sweep, but it never
    /// folds into a merge.
    pub(super) visible: bool,
}

/// `key_bytes`' live entry in `stripe`, if any. A currently-spilled entry
/// has no value bytes resident in RAM; see [`apply_locked`]'s doc for
/// `prefetched_spilled` and the off-lock/under-lock read split. A spilled
/// entry that is not visible is not read when the resolver merges, since a
/// dead side never folds; any other resolver sees its bytes as it sees a
/// resident entry's.
pub(super) fn read_stored_live<K, V>(
    stripe: &Stripe<K, V>,
    ctx: &ApplyCtx<'_, K, V>,
    hash: u64,
    key_bytes: &[u8],
) -> Option<StoredLive> {
    let live = stripe
        .live
        .find(hash, |l| record_key(&l.record) == key_bytes)?;
    let visible = !absent_at(live, key_bytes, &stripe.long_ttl, ctx.tti_ms, ctx.now_ms);
    #[cfg(feature = "spill")]
    let encoded = match &live.state {
        EntryState::Resident => Some(record_value_bytes(&live.record)),
        EntryState::Spilled(_) if !visible && ctx.resolver.merges() => None,
        EntryState::Spilled(loc) => ctx.prefetched_spilled.get(loc).cloned().or_else(|| {
            ctx.spill
                .filter(|_| ctx.resolver.needs_value_bytes())
                .and_then(|tier| read_spilled_for_conflict(tier, *loc))
        }),
    };
    #[cfg(not(feature = "spill"))]
    let encoded = Some(record_value_bytes(&live.record));
    Some(StoredLive {
        ver: live.ver(),
        encoded,
        expires_at_ms: decode_expiry(live.wall_ms, live.expiry, key_bytes, &stripe.long_ttl),
        visible,
    })
}

/// The versioned-apply core: applies `incoming` at `ver` for `key` iff
/// `resolver` picks it over whatever `stripe` holds, updating
/// `digest_bucket`, `total_weight`, and `live_count` to match. A collision
/// may fold both sides via [`ConflictResolver::merge`], rebinding
/// `ver`/`incoming` to the merged version ([`merge_version`]) and bytes
/// before anything downstream runs, so a merge stores and fingerprints like
/// any other `Put`. A stored entry that reads as absent at `now_ms` never
/// folds ([`resolve_conflict`] treats it as absent), nor does a side that
/// lapsed before the other side's write; under a merging resolver a spilled
/// entry that reads as absent is not read from disk, by this call or by
/// [`prefetch_spilled_conflict_bytes`].
/// With [`ApplyCtx::held_only`] set, a write that does not fold into a
/// visible live entry is [`ApplyOutcome::Rejected`] and stores nothing; see
/// [`reject_unheld`]. Synchronous under the caller's write lock on `stripe`;
/// no disk I/O of its own. `prefetched_spilled` (`spill` builds) is
/// [`prefetch_spilled_conflict_bytes`]'s off-lock read of the stored side's
/// bytes; a lookup miss falls back to [`read_spilled_for_conflict`] under
/// this lock rather than treating the side as value-less. Returns whether
/// this displaced a [`EntryState::Spilled`] entry (`false` for `Rejected`);
/// see [`Engine::note_spill_departure`].
pub(crate) fn apply_locked<K, V>(
    stripe: &mut Stripe<K, V>,
    ctx: &ApplyCtx<'_, K, V>,
    entry: EntryKey<K>,
    mut ver: Hlc,
    mut incoming: Incoming<V>,
) -> (ApplyOutcome<K, V>, bool)
where
    K: Hash + Eq + Clone,
    V: Clone + DeserializeOwned,
{
    let EntryKey {
        hash,
        key,
        key_bytes,
    } = entry;
    let prior_tombstone = stripe.tombstones.get(key_bytes.as_ref()).copied();
    let stored_live = if prior_tombstone.is_none() {
        read_stored_live(stripe, ctx, hash, key_bytes.as_ref())
    } else {
        None
    };
    let stored_ver = prior_tombstone
        .map(|t| t.ver)
        .or_else(|| stored_live.as_ref().map(|s| s.ver));

    if ctx.held_only {
        let holds = stored_live.as_ref().is_some_and(|s| s.visible);
        if reject_unheld(
            stripe,
            key_bytes.as_ref(),
            stored_ver,
            ver,
            &incoming,
            holds,
        ) {
            return (ApplyOutcome::Rejected, false);
        }
    }

    if let Some(sv) = stored_ver {
        let stored_encoded = stored_live.as_ref().and_then(|s| s.encoded.as_deref());
        let stored_expires_at_ms = stored_live.as_ref().and_then(|s| s.expires_at_ms);
        let stored_absent = stored_live.as_ref().is_some_and(|s| !s.visible);
        match resolve_and_rebind(
            ctx.resolver,
            key_bytes.as_ref(),
            sv,
            stored_encoded,
            stored_expires_at_ms,
            stored_absent,
            prior_tombstone.is_some(),
            ver,
            incoming,
        ) {
            None => return (ApplyOutcome::Rejected, false),
            Some((new_ver, new_incoming)) => {
                ver = new_ver;
                incoming = new_incoming;
            }
        }
    }

    let had_live = prior_tombstone.is_none() && stored_ver.is_some();
    let was_visible = stored_live.as_ref().is_some_and(|s| s.visible);
    let new_fp = entry_fingerprint(key_bytes.as_ref(), ver);

    // Subtracts whatever this write displaces before adding the new fingerprint
    // in.
    if let Some(t) = prior_tombstone {
        ctx.digest_bucket.fetch_xor(
            entry_fingerprint(key_bytes.as_ref(), t.ver),
            Ordering::Relaxed,
        );
        stripe.tombstones.remove(key_bytes.as_ref());
    } else if let Some(sv) = stored_ver {
        ctx.digest_bucket
            .fetch_xor(entry_fingerprint(key_bytes.as_ref(), sv), Ordering::Relaxed);
    }
    ctx.digest_bucket.fetch_xor(new_fp, Ordering::Relaxed);

    let entry = EntryKey {
        hash,
        key,
        key_bytes,
    };
    match incoming {
        Incoming::Put {
            value,
            expires_at_ms,
            encoded,
        } => apply_put(
            stripe,
            ctx,
            entry,
            ver,
            PutWrite {
                value,
                expires_at_ms,
                encoded,
            },
            had_live,
            was_visible,
        ),
        Incoming::Tombstone => apply_tombstone(stripe, ctx, entry, ver, had_live),
    }
}

/// The held-only gate of [`apply_locked`]: whether the write at `ver` is
/// refused because it cannot fold into a visible live entry (`holds`). It
/// folds only an [`Incoming::Put`] into such an entry; a tombstone reaches
/// the engine through [`Engine::invalidate`] instead. A refused write
/// newer than whatever is stored for the key (a tombstone, a live entry
/// that reads as absent, or nothing) marks a load in flight for the key
/// invalidated, exactly as [`Engine::invalidate`] does, so a fill racing it
/// stores nothing stale. Stores nothing itself.
fn reject_unheld<K, V>(
    stripe: &Stripe<K, V>,
    key_bytes: &[u8],
    stored_ver: Option<Hlc>,
    ver: Hlc,
    incoming: &Incoming<V>,
    holds: bool,
) -> bool {
    if holds && matches!(incoming, Incoming::Put { .. }) {
        return false;
    }
    if stored_ver.is_none_or(|sv| ver > sv)
        && let Some(inflight) = stripe.inflight.get(key_bytes)
    {
        inflight.invalidated.store(true, Ordering::Relaxed);
    }
    true
}

/// The `Incoming::Put` half of [`apply_locked`]'s write: installs the new
/// value, corrects total weight for whatever it displaced (`had_live`), bumps
/// `live_count` iff nothing physically live occupied the key before, and
/// reports `created` unless a readable entry (`was_visible`) is replaced.
/// The returned `bool` is whether the displaced entry, if any, held a
/// spilled payload. An overwrite of a spilled key always installs
/// fresh as resident, so this is the only place such an entry departs
/// without a matching promotion.
fn apply_put<K, V>(
    stripe: &mut Stripe<K, V>,
    ctx: &ApplyCtx<'_, K, V>,
    entry: EntryKey<K>,
    ver: Hlc,
    put: PutWrite<V>,
    had_live: bool,
    was_visible: bool,
) -> (ApplyOutcome<K, V>, bool)
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    let EntryKey {
        hash,
        key,
        key_bytes,
    } = entry;
    let PutWrite {
        value,
        expires_at_ms,
        encoded,
    } = put;
    let weight = ctx.weigher.map_or(1, |w| w(&key, &value));
    let removed = if had_live {
        remove_live(stripe, hash, key_bytes.as_ref())
    } else {
        None
    };
    let displaced_spilled = removed.as_ref().is_some_and(|r| r.was_spilled);
    let old_weight = removed.map(|r| r.weight);
    // `remove_live` cleared any stale `long_ttl` row this write displaces;
    // only a TTL past the delta's range needs a fresh one.
    let (expiry, long_ttl_value) = encode_expiry(ver.wall_ms, expires_at_ms);
    if let Some(deadline) = long_ttl_value {
        stripe.long_ttl.insert(key_bytes.clone(), deadline);
    }
    // A fresh write always installs resident: spilling only ever happens
    // through sampled eviction, never directly on a write.
    stripe.live.insert_unique(
        hash,
        Live::new(
            build_resident_record(key_bytes.as_ref(), encoded.as_ref()),
            ver,
            expiry,
            weight,
            ctx.now_ms,
            #[cfg(feature = "spill")]
            EntryState::Resident,
        ),
        hasher_for,
    );
    if let Some(exp) = expires_at_ms {
        stripe.next_expiry_ms = stripe.next_expiry_ms.min(exp);
    }
    ctx.total_weight
        .fetch_add(u64::from(weight), Ordering::Relaxed);
    if let Some(ow) = old_weight {
        ctx.total_weight.fetch_sub(u64::from(ow), Ordering::Relaxed);
    } else {
        ctx.live_count.fetch_add(1, Ordering::Relaxed);
    }
    (
        ApplyOutcome::Put {
            key,
            value,
            created: !was_visible,
        },
        displaced_spilled,
    )
}

/// The `Incoming::Tombstone` half of [`apply_locked`]'s write: removes the
/// displaced live entry from total weight and `live_count`, then records the
/// tombstone with its two GC deadlines. The returned `bool` is whether the
/// displaced entry, if any, held a spilled payload.
fn apply_tombstone<K, V>(
    stripe: &mut Stripe<K, V>,
    ctx: &ApplyCtx<'_, K, V>,
    entry: EntryKey<K>,
    ver: Hlc,
    had_live: bool,
) -> (ApplyOutcome<K, V>, bool)
where
    K: Hash + Eq,
{
    let EntryKey {
        hash,
        key,
        key_bytes,
    } = entry;
    let mut displaced_spilled = false;
    if had_live && let Some(removed) = remove_live(stripe, hash, key_bytes.as_ref()) {
        ctx.total_weight
            .fetch_sub(u64::from(removed.weight), Ordering::Relaxed);
        ctx.live_count.fetch_sub(1, Ordering::Relaxed);
        displaced_spilled = removed.was_spilled;
    }
    stripe.tombstones.insert(
        key_bytes,
        Tombstone {
            ver,
            ttl_deadline_ms: ctx.now_ms.saturating_add(ctx.tombstone_ttl_ms),
            max_deadline_ms: ctx.now_ms.saturating_add(ctx.tombstone_max_ttl_ms),
        },
    );
    (ApplyOutcome::Tombstoned { key }, displaced_spilled)
}

impl<K, V> Engine<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// Flips [`Engine::apply_many`]'s pre-fold on or off, `true` by default.
    /// Only a benchmark or test ever needs it off, to compare pre-fold's
    /// effect on throughput against the unfolded per-record path it
    /// otherwise always takes; nothing on a real write path ever calls it.
    /// [`super::super::Shard::with_prefold_enabled`] is the `#[doc(hidden)]` seam
    /// that reaches this from `crate::cache::CacheBuilder::prefold_enabled`,
    /// in turn reachable from an integration-test binary outside this
    /// crate, so this stays outside any `#[cfg(test)]` gate.
    pub(crate) fn set_prefold_enabled(&self, enabled: bool) {
        self.prefold_enabled.store(enabled, Ordering::Relaxed);
    }

    /// The current value of [`Engine::set_prefold_enabled`]'s flag, which a
    /// shard's engine rebuild carries forward onto the new engine.
    pub(crate) fn prefold_enabled(&self) -> bool {
        self.prefold_enabled.load(Ordering::Relaxed)
    }

    /// Applies a batch of versioned writes that all hash into `bucket`,
    /// under one write-lock acquisition for the whole group. Runs
    /// [`Self::enforce_capacity`] once afterward, outside the write lock,
    /// iff the batch put anything.
    ///
    /// When `resolver` merges and pre-folding is enabled
    /// (`prefold_enabled`, toggled only via [`Engine::set_prefold_enabled`]),
    /// `entries` are grouped into runs and folded through [`prefold_batch`]
    /// before any [`apply_locked`] call, collapsing repeated keys to one
    /// call each with every other index reported [`ApplyOutcome::Rejected`];
    /// see [`prefold_batch`]'s doc for why the result matches sequential
    /// application exactly, version and all.
    ///
    /// [`prefetch_spilled_conflict_bytes`] reads back any currently-spilled
    /// stored sides off-lock first, before the write lock this call holds
    /// for the whole batch; see [`apply_locked`]'s doc for the under-lock
    /// fallback on a miss.
    ///
    /// A thin wrapper, `reservation: None`, over
    /// [`Engine::apply_many_with_reservation`].
    pub(crate) fn apply_many(
        &self,
        bucket: usize,
        entries: Vec<BatchEntry<K, V>>,
        resolver: &dyn ConflictResolver,
        tombstone_ttl_ms: u64,
        tombstone_max_ttl_ms: u64,
        now_ms: u64,
    ) -> Vec<ApplyOutcome<K, V>> {
        self.apply_many_with_reservation(
            bucket,
            entries,
            resolver,
            tombstone_ttl_ms,
            tombstone_max_ttl_ms,
            now_ms,
            None,
            &mut Written::default(),
        )
        .0
    }

    /// [`Engine::apply_many`], additionally threading `reservation` into
    /// [`Engine::enforce_capacity_with_reservation`] when the batch wrote
    /// anything, spending the pre-lock budget one caller reserved before
    /// its first stripe lock. The tuple's second element is that method's
    /// own deficit, `0` when nothing was written or every victim resolved.
    /// Each value the batch stores joins `written`, which that eviction
    /// passes over until nothing else is left; a caller applying one batch
    /// across several stripes passes the same `written` to every call.
    #[allow(
        clippy::too_many_arguments,
        reason = "one caller per bucket, from an already-grouped batch; splitting these into a struct would not make either call site clearer"
    )]
    pub(crate) fn apply_many_with_reservation(
        &self,
        bucket: usize,
        entries: Vec<BatchEntry<K, V>>,
        resolver: &dyn ConflictResolver,
        tombstone_ttl_ms: u64,
        tombstone_max_ttl_ms: u64,
        now_ms: u64,
        reservation: Option<&mut Reservation<'_>>,
        written: &mut Written,
    ) -> (Vec<ApplyOutcome<K, V>>, u64) {
        self.apply_many_in(
            ApplyScope::Any,
            bucket,
            entries,
            resolver,
            tombstone_ttl_ms,
            tombstone_max_ttl_ms,
            now_ms,
            reservation,
            written,
        )
    }

    /// [`Engine::apply_many_with_reservation`] with the batch's
    /// [`ApplyScope`] given. [`ApplyScope::Any`] is that method;
    /// [`ApplyScope::HeldOnly`] applies each entry only as a fold into a key
    /// this engine holds visible, for an `Invalidation`-mode cache receiving
    /// a peer's merged record. No live entry, a tombstone, an expired or
    /// idle entry, or a tombstone record is [`ApplyOutcome::Rejected`] and
    /// stores nothing, and a load in flight for the key is marked
    /// invalidated when the rejected write is newer than what is stored
    /// ([`reject_unheld`]). The held check and the apply share the stripe
    /// write lock, so no removal lands between them. Every entry applies on
    /// its own, with no pre-fold, so each one's effect on an in-flight load
    /// is its own.
    #[allow(
        clippy::too_many_arguments,
        reason = "the same call shape as `apply_many_with_reservation`, plus the scope"
    )]
    pub(crate) fn apply_many_in(
        &self,
        scope: ApplyScope,
        bucket: usize,
        entries: Vec<BatchEntry<K, V>>,
        resolver: &dyn ConflictResolver,
        tombstone_ttl_ms: u64,
        tombstone_max_ttl_ms: u64,
        now_ms: u64,
        reservation: Option<&mut Reservation<'_>>,
        written: &mut Written,
    ) -> (Vec<ApplyOutcome<K, V>>, u64) {
        let held_only = scope == ApplyScope::HeldOnly;
        let capped = self.max_capacity != u64::MAX;
        let prefold =
            resolver.merges() && !held_only && self.prefold_enabled.load(Ordering::Relaxed);
        #[cfg(feature = "spill")]
        let prefetched_spilled = prefetch_spilled_conflict_bytes(
            &self.stripes[bucket],
            entries
                .iter()
                .map(|(hash, _, key_bytes, ..)| (*hash, key_bytes)),
            resolver,
            self.spill().map(Arc::as_ref),
            self.tti_ms,
            now_ms,
        );
        let entries: Vec<Option<BatchEntry<K, V>>> = if prefold {
            let runs = group_prefold_runs(&entries);
            let stored_seeds = {
                let stripe = self.stripes[bucket].read();
                peek_prefold_seeds(
                    &stripe,
                    &entries,
                    &runs,
                    self.tti_ms,
                    now_ms,
                    #[cfg(feature = "spill")]
                    &prefetched_spilled,
                )
            };
            prefold_batch(entries, runs, resolver, &stored_seeds, now_ms)
        } else {
            entries.into_iter().map(Some).collect()
        };
        let mut outcomes = Vec::with_capacity(entries.len());
        let mut wrote = false;
        {
            let mut stripe = self.stripes[bucket].write();
            for entry in entries {
                let Some((hash, key, key_bytes, ver, incoming)) = entry else {
                    // Absorbed into its run's survivor by `prefold_batch`;
                    // that survivor's own `apply_locked` call, elsewhere in
                    // this same loop, is this entry's only real contribution.
                    outcomes.push(ApplyOutcome::Rejected);
                    continue;
                };
                let part = part_index_from_hash(hash);
                let ctx = ApplyCtx {
                    digest_bucket: &self.digest[digest_slot(bucket, part)],
                    total_weight: &self.total_weight,
                    live_count: &self.live_count,
                    weigher: self.weigher.as_ref(),
                    tti_ms: self.tti_ms,
                    resolver,
                    #[cfg(feature = "spill")]
                    prefetched_spilled: &prefetched_spilled,
                    #[cfg(feature = "spill")]
                    spill: self.spill().map(Arc::as_ref),
                    tombstone_ttl_ms,
                    tombstone_max_ttl_ms,
                    now_ms,
                    held_only,
                };
                let (outcome, displaced_spilled) = apply_locked(
                    &mut stripe,
                    &ctx,
                    EntryKey {
                        hash,
                        key,
                        key_bytes,
                    },
                    ver,
                    incoming,
                );
                self.note_spill_departure(displaced_spilled);
                if matches!(outcome, ApplyOutcome::Put { .. }) {
                    wrote = true;
                    if capped {
                        written.insert(hash);
                    }
                }
                outcomes.push(outcome);
            }
        }
        let deficit = if wrote {
            self.enforce_capacity_with_reservation(bucket, reservation, written, now_ms)
        } else {
            0
        };
        (outcomes, deficit)
    }

    /// [`Engine::apply_many`] for one entry, applied only while the key's
    /// live entry is still at exactly `expected`, resident or spilled:
    /// `None`, changing nothing, once it has moved on to another version, a
    /// tombstone, or expiry. The check and the apply share one stripe write
    /// lock, so no write lands between them.
    pub(crate) fn apply_if_current(
        &self,
        entry: BatchEntry<K, V>,
        expected: Hlc,
        resolver: &dyn ConflictResolver,
        tombstone_ttl_ms: u64,
        tombstone_max_ttl_ms: u64,
        now_ms: u64,
    ) -> Option<ApplyOutcome<K, V>> {
        let (hash, key, key_bytes, ver, incoming) = entry;
        let bucket = stripe_index_from_hash(hash);
        #[cfg(feature = "spill")]
        let prefetched_spilled = prefetch_spilled_conflict_bytes(
            &self.stripes[bucket],
            [(hash, &key_bytes)],
            resolver,
            self.spill().map(Arc::as_ref),
            self.tti_ms,
            now_ms,
        );
        let outcome = {
            let mut stripe = self.stripes[bucket].write();
            let live = stripe
                .live
                .find(hash, |l| record_key(&l.record) == key_bytes.as_ref())?;
            if live.ver() != expected
                || self.is_absent(live, key_bytes.as_ref(), &stripe.long_ttl, now_ms)
            {
                return None;
            }
            let ctx = ApplyCtx {
                digest_bucket: &self.digest[digest_slot(bucket, part_index_from_hash(hash))],
                total_weight: &self.total_weight,
                live_count: &self.live_count,
                weigher: self.weigher.as_ref(),
                tti_ms: self.tti_ms,
                resolver,
                #[cfg(feature = "spill")]
                prefetched_spilled: &prefetched_spilled,
                #[cfg(feature = "spill")]
                spill: self.spill().map(Arc::as_ref),
                tombstone_ttl_ms,
                tombstone_max_ttl_ms,
                now_ms,
                held_only: false,
            };
            let (outcome, displaced_spilled) = apply_locked(
                &mut stripe,
                &ctx,
                EntryKey {
                    hash,
                    key,
                    key_bytes,
                },
                ver,
                incoming,
            );
            self.note_spill_departure(displaced_spilled);
            outcome
        };
        if matches!(outcome, ApplyOutcome::Put { .. }) {
            self.enforce_capacity(bucket, &mut Written::of(hash), now_ms);
        }
        Some(outcome)
    }
}

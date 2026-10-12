//! Capacity eviction on `Engine`: sampled victim selection, spill hand-off and the enforcement loops.

use std::hash::Hash;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

#[cfg(all(doc, feature = "spill"))]
use crate::store::spill::SpillTier;
#[cfg(feature = "spill")]
use crate::store::spill::{
    Admission, SpillJob, SpillSink, keeps_refused_victim_resident, spill_record_len,
};
use crate::store::{BUCKET_COUNT, entry_fingerprint};

#[cfg(feature = "spill")]
use super::EntryState;
#[cfg(feature = "spill")]
use super::expiry::decode_expiry;
use super::expiry::idle_elapsed_ms;
use super::keys::{
    digest_slot, hash_key_bytes, hasher_for, part_index_from_hash, stripe_index_from_hash,
};
#[cfg(feature = "spill")]
use super::record::record_value_bytes;
use super::record::{record_key, record_key_bytes};
use super::slab::SlabEntry;
use super::{Engine, Live, OnRefusal, Reservation, Stripe, Written, is_spill_candidate};

/// How many live entries one capacity-eviction pass weighs before evicting the
/// least recently read of them.
pub(super) const EVICTION_SAMPLE: usize = 8;

/// How many entries one [`Engine::enforce_capacity`] lock hold samples, and
/// at most how many of the coldest it evicts.
pub(super) const EVICTION_BATCH_SAMPLE: usize = 32;

pub(super) const EVICTION_BATCH: usize = 8;

/// [`Engine::enforce_capacity`]'s stop rule once one pass evicted nothing:
/// whether to return immediately, rather than pay for
/// [`Engine::evict_one_scanning`]'s scan across stripes, and leave the rest
/// to the flusher and later passes. Only ever `true` while some hand-off is
/// in flight, and then in either of two cases: the tier `refused` the
/// pass's victim, most often because the flush queue is full, which
/// refuses every other stripe's victim alike until the flusher drains it;
/// or `total_weight`, the resident weight no hand-off covers, is already
/// within `limit`, so the installs alone bring the cache within it. A
/// refusal particular to its victim, a record larger than a region or than
/// the queue's free bytes, stops the pass as well. `false` otherwise, this
/// loop's cue to fall back to the scan, spill tier configured or not: a
/// cache still over `limit` without its hand-offs stays over it after they
/// install. Pure; unit tested directly.
pub(super) fn defer_to_flusher(
    refused: bool,
    total_weight: u64,
    pending_spill_weight: u64,
    limit: u64,
) -> bool {
    pending_spill_weight > 0 && (refused || total_weight <= limit)
}

/// [`Engine::evict_down_to`]'s stop rule after a batch reports a nonzero
/// [`EvictOutcome::deficit`]: `0` once `current` (weight read fresh after
/// the batch) has already dropped to `limit` or under, `deficit` unchanged
/// otherwise. A concurrent flusher install or another victim's success can
/// already satisfy `current` even though `deficit` alone still looks owed.
/// Pure; unit tested directly.
pub(super) fn deficit_once_still_over_capacity(current: u64, limit: u64, deficit: u64) -> u64 {
    if current <= limit { 0 } else { deficit }
}

/// How many of `sampled_weights` (coldest first) one lock hold evicts: the
/// fewest that clear `over_by`, at most [`EVICTION_BATCH`], and never more
/// than the colder half of the sample, so recency still decides under a
/// burst.
pub(super) fn eviction_batch_size(over_by: u64, sampled_weights: &[u32]) -> usize {
    let cap = EVICTION_BATCH.min(sampled_weights.len().div_ceil(2));
    let mut cleared = 0u64;
    for (evicted, &weight) in sampled_weights.iter().take(cap).enumerate() {
        if cleared >= over_by {
            return evicted;
        }
        cleared += u64::from(weight);
    }
    cap
}

/// What one [`Engine::evict_one_sampled`]/[`Engine::evict_batch_sampled`]
/// lock hold accomplished, for [`Engine::enforce_capacity`]'s loop. A
/// spill hand-off frees its victim's weight at the same moment a physical
/// removal does, [`Engine::evict_victim_locked`]'s doc has the details, so
/// this carries only the one number either kind of victim contributes to;
/// nothing here needs to tell them apart.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct EvictOutcome {
    /// Weight freed this pass, physically removed or handed to a spill
    /// tier; `0` when the stripe this pass sampled held no victim, an
    /// empty stripe among them.
    pub(super) removed_weight: u64,
    /// Sum of every [`VictimOutcome::PendingReservation`] this pass
    /// reported: bytes both the reservation and `admit`'s global headroom
    /// refused. Always `0` when `reservation` was `None`. A nonzero value
    /// tells [`Engine::enforce_capacity_with_reservation`] to stop rather
    /// than spin retrying an exhausted budget.
    pub(super) deficit: u64,
    /// Whether this pass left a victim resident because the spill tier
    /// refused its hand-off ([`VictimOutcome::Deferred`]): a full flush
    /// queue, a record larger than a region or than the queue's free bytes,
    /// or a closed tier.
    pub(super) refused: bool,
}

impl EvictOutcome {
    /// Whether this pass accomplished nothing at all.
    pub(super) fn made_no_progress(self) -> bool {
        self.removed_weight == 0
    }
}

/// What handing one victim to [`Engine::evict_victim_locked`] accomplished.
pub(super) enum VictimOutcome {
    /// Physically removed from `live`: frees `weight` and one live entry,
    /// both the caller's job to fold into `total_weight`/`live_count`.
    Removed(u32),
    /// Committed to a configured spill tier: still `Resident` in `live`, at
    /// weight `0`, the entry's fate already decided under the stripe lock
    /// via [`SpillTier::would_accept_under`], but the channel send itself,
    /// [`SpillTier::enqueue`], deliberately deferred until the lock is
    /// released; see [`Engine::try_spill_victim`]. `weight` is what the
    /// entry carried right before hand-off, already zeroed on the entry
    /// itself, so it is the caller's to fold into `total_weight` exactly
    /// like `Removed`'s; only `live_count` differs between the two. The
    /// caller finishes the hand-off with [`Engine::finish_spill_handoff`]
    /// once the lock is dropped: on a full queue that restores the weight
    /// through [`SpillSink::abandon`] exactly as a downstream write or
    /// install failure would, never a physical removal. Only ever
    /// constructed under `feature = "spill"`, the only build where anything
    /// can be handed off in the first place.
    #[cfg(feature = "spill")]
    PendingSpill(u32, SpillJob),
    /// A configured spill tier refused this victim's hand-off
    /// ([`SpillTier::would_accept_under`] declining: too large, closed, or the
    /// flush queue is full), and `keep_resident` says to leave it
    /// resident rather than delete it: the `Mode::Replicated`/
    /// `Mode::Distributed` policy `Shard::attach_spill` sets on the tier, or
    /// [`OnRefusal::KeepResident`], which a promotion's eviction passes in
    /// every mode.
    /// `stripe.live` is untouched: the entry keeps its full weight and
    /// stays a spill candidate for a later eviction pass to sample and
    /// retry, once the tier has room again.
    /// Reported to the caller exactly like [`VictimOutcome::Vanished`], no
    /// weight or count to fold in, since nothing here changed. Only ever
    /// constructed under `feature = "spill"`.
    #[cfg(feature = "spill")]
    Deferred,
    /// [`SpillTier::would_accept_under`] returned [`Admission::RefusedPending`];
    /// `stripe.live` is untouched, like [`VictimOutcome::Deferred`].
    #[cfg(feature = "spill")]
    PendingReservation(u32),
    /// Vanished between sampling and this call, a race with another
    /// writer on the same stripe; nothing to do.
    Vanished,
}

/// What [`Engine::try_spill_victim`] found for a resident victim.
#[cfg(feature = "spill")]
enum SpillAttempt {
    /// The tier commits to taking the victim; see
    /// [`Engine::try_spill_victim`]'s docs for what the two fields mean.
    Committed(u32, SpillJob),
    /// [`SpillTier::would_accept_under`] returned [`Admission::RefusedFinal`].
    /// `keep_resident` is [`keeps_refused_victim_resident`] of the pass's
    /// [`OnRefusal`] and the tier's policy at refusal time.
    Refused { keep_resident: bool },
    /// [`SpillTier::would_accept_under`] returned [`Admission::RefusedPending`].
    /// `deficit` is how many more reserved bytes would have covered it.
    Pending { deficit: u32 },
    /// No tier configured, or the victim raced away, removed or not
    /// resident anymore, between sampling and this call. The ordinary
    /// remove-and-XOR path runs exactly as without this policy.
    NotApplicable,
}

impl<K, V> Engine<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// xorshift64: fast and allocation-free for choosing which stripe to look
    /// at next and where to start sampling. No correctness property depends
    /// on its output.
    fn next_random(&self) -> u64 {
        let mut x = self.evict_cursor.load(Ordering::Relaxed);
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.evict_cursor.store(x, Ordering::Relaxed);
        x
    }

    fn next_pseudo_random_bucket(&self) -> usize {
        stripe_index_from_hash(self.next_random())
    }

    /// Where in a stripe of `len` entries the next sample starts. Rotating the
    /// start keeps every entry reachable, instead of always weighing the
    /// table's first slots.
    pub(super) fn sample_offset(&self, len: usize) -> usize {
        if len == 0 {
            0
        } else {
            usize::try_from(self.next_random() % len as u64).unwrap_or(0)
        }
    }

    #[cfg_attr(not(test), allow(clippy::unused_self))]
    fn note_eviction_lock_acquisition(&self) {
        #[cfg(test)]
        self.eviction_lock_acquisitions
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Whether a configured spill tier commits to taking `victim_bytes`,
    /// found at `hash` in `bucket` with `stripe` already write-locked, in
    /// place of physically removing it. [`SpillAttempt::NotApplicable`] when
    /// no tier is configured or the victim is not [`EntryState::Resident`]
    /// anymore; [`SpillAttempt::Refused`] when
    /// [`SpillTier::would_accept_under`] declines (too large, closed, flush queue
    /// full), leaving [`Engine::evict_victim_locked`] to decide the
    /// victim's fate from `keep_resident`; `on_refusal` is the pass's
    /// policy, which also picks the reason [`SpillTier::would_accept_under`]
    /// records the refusal under. On [`SpillAttempt::Committed`], the
    /// weight returned is already zeroed on the entry in place, and the job
    /// is the caller's to hand to [`Engine::finish_spill_handoff`] once the
    /// stripe lock is released; [`SpillTier::enqueue`] itself runs later,
    /// off this lock, and a full queue found there is handled like any
    /// downstream write failure via [`SpillSink::abandon`].
    #[cfg(feature = "spill")]
    fn try_spill_victim(
        &self,
        stripe: &mut Stripe<K, V>,
        bucket: usize,
        hash: u64,
        victim_bytes: &Bytes,
        reservation: Option<&mut Reservation<'_>>,
        on_refusal: OnRefusal,
    ) -> SpillAttempt {
        let Some(tier) = self.spill() else {
            return SpillAttempt::NotApplicable;
        };
        let Some(live) = stripe
            .live
            .find_mut(hash, |l| record_key(&l.record) == victim_bytes.as_ref())
        else {
            return SpillAttempt::NotApplicable;
        };
        if !matches!(live.state, EntryState::Resident) {
            return SpillAttempt::NotApplicable;
        }
        let encoded = record_value_bytes(&live.record);
        match tier.would_accept_under(on_refusal, reservation, victim_bytes.len(), encoded.len()) {
            Admission::RefusedFinal => {
                return SpillAttempt::Refused {
                    keep_resident: keeps_refused_victim_resident(
                        on_refusal,
                        tier.keep_resident_when_refused(),
                    ),
                };
            }
            Admission::RefusedPending => {
                return SpillAttempt::Pending {
                    deficit: spill_record_len(victim_bytes.len(), encoded.len()),
                };
            }
            Admission::Accepted => {}
        }
        let weight = live.weight;
        let admitted_bytes = spill_record_len(victim_bytes.len(), encoded.len());
        let job = SpillJob {
            stripe_idx: bucket,
            hash,
            key_bytes: record_key_bytes(&live.record),
            ver: live.ver(),
            expires_at_ms: decode_expiry(
                live.wall_ms,
                live.expiry,
                victim_bytes.as_ref(),
                &stripe.long_ttl,
            ),
            encoded,
            weight,
            admitted_bytes,
        };
        live.weight = 0;
        SpillAttempt::Committed(weight, job)
    }

    /// Finishes a hand-off [`Engine::evict_victim_locked`] committed to via
    /// [`Engine::try_spill_victim`], after the stripe lock is released:
    /// runs the one part of the hand-off that touches the flusher's
    /// channel, [`SpillTier::enqueue`]. A full queue gets `job` back with
    /// its key bytes, and [`SpillSink::abandon`] restores the victim's
    /// weight exactly as it would for a write or install failure
    /// discovered later downstream. `on_refusal` is the pass's policy, which
    /// picks the reason [`SpillTier::enqueue`] records that failure under. No
    /// tier configured is unreachable here, but still handled rather than
    /// assumed.
    #[cfg(feature = "spill")]
    fn finish_spill_handoff(&self, on_refusal: OnRefusal, job: SpillJob) {
        let Some(tier) = self.spill() else { return };
        let stripe_idx = job.stripe_idx;
        let hash = job.hash;
        let ver = job.ver;
        let weight = job.weight;
        let admitted_bytes = job.admitted_bytes;
        if let Err(job) = tier.enqueue(on_refusal, job) {
            // A job that never reaches the flusher must not permanently
            // shrink `admit`'s total, so release its bytes before abandon.
            tier.release(admitted_bytes);
            let job = *job;
            self.abandon(stripe_idx, &job.key_bytes, hash, ver, weight);
        }
    }

    /// Removes or spills `victim_bytes`, found at `hash` in `bucket` with
    /// `stripe` already write-locked. Hands a [`EntryState::Resident`] victim
    /// to a configured spill tier when [`Engine::try_spill_victim`] commits
    /// to it: weight zeroed in place, reported as
    /// [`VictimOutcome::PendingSpill`], while `stripe.live`, the digest,
    /// and `live_count` stay untouched until the flusher installs it. On a
    /// refusal, `keep_resident` (`on_refusal` combined with the tier's own
    /// policy, see [`keeps_refused_victim_resident`]) picks between
    /// [`VictimOutcome::Deferred`] (victim stays resident, untouched
    /// weight) and the ordinary remove-and-XOR path,
    /// [`VictimOutcome::Removed`]; a vanished entry reports
    /// [`VictimOutcome::Vanished`]. Total weight and `live_count` stay the
    /// caller's job; a [`VictimOutcome::PendingSpill`] job still needs
    /// [`Engine::finish_spill_handoff`] once `stripe` is dropped.
    /// `reservation`, forwarded to [`Engine::try_spill_victim`], is the
    /// pre-lock budget a caller carried in; unused once `spill` is off, as
    /// is `on_refusal`.
    #[cfg_attr(
        not(feature = "spill"),
        allow(
            unused_variables,
            clippy::needless_pass_by_value,
            reason = "`reservation` and `on_refusal` are never read once `try_spill_victim` \
                      compiles out; kept as real parameters so every caller keeps one \
                      signature across both builds"
        )
    )]
    fn evict_victim_locked(
        &self,
        stripe: &mut Stripe<K, V>,
        bucket: usize,
        victim_bytes: &Bytes,
        reservation: Option<&mut Reservation<'_>>,
        on_refusal: OnRefusal,
    ) -> VictimOutcome {
        let hash = hash_key_bytes(victim_bytes.as_ref());
        #[cfg(feature = "spill")]
        match self.try_spill_victim(stripe, bucket, hash, victim_bytes, reservation, on_refusal) {
            SpillAttempt::Committed(weight, job) => {
                return VictimOutcome::PendingSpill(weight, job);
            }
            SpillAttempt::Refused { keep_resident } => {
                if keep_resident {
                    return VictimOutcome::Deferred;
                }
            }
            SpillAttempt::Pending { deficit } => {
                return VictimOutcome::PendingReservation(deficit);
            }
            SpillAttempt::NotApplicable => {}
        }
        let SlabEntry::Occupied(occ) = stripe.live.entry(
            hash,
            |l| record_key(&l.record) == victim_bytes.as_ref(),
            hasher_for,
        ) else {
            return VictimOutcome::Vanished;
        };
        let (removed, _vacant) = occ.remove();
        stripe.long_ttl.remove(victim_bytes.as_ref());
        let part = part_index_from_hash(hash);
        self.digest[digest_slot(bucket, part)].fetch_xor(
            entry_fingerprint(record_key(&removed.record), removed.ver()),
            Ordering::Relaxed,
        );
        VictimOutcome::Removed(removed.weight)
    }

    /// The sampling window [`Engine::evict_one_sampled`] and [`Engine::evict_batch_sampled`] both draw from.
    /// Walks `stripe.live` in arena order, so a tie on [`idle_elapsed_ms`]
    /// resolves to whichever comes last: an undocumented tie-break. The
    /// candidates `written` holds are never in the window, except one
    /// heavier than the whole cap, which never fits; only a candidate's key
    /// is hashed to check.
    pub(super) fn sample_candidates<'s>(
        &self,
        stripe: &'s Stripe<K, V>,
        sample_size: usize,
        written: &'s Written,
    ) -> impl Iterator<Item = &'s Live<K, V>> {
        let offset = self.sample_offset(stripe.live.len());
        stripe
            .live
            .iter()
            .skip(offset)
            .chain(stripe.live.iter().take(offset))
            .filter(|live| {
                !is_spill_candidate(live)
                    || u64::from(live.weight) > self.max_capacity
                    || !written.holds(live)
            })
            .take(sample_size)
            .filter(|live| is_spill_candidate(live))
    }

    /// Evicts the coldest of up to [`EVICTION_SAMPLE`] resident entries in
    /// `bucket`. Returns what happened: nothing to evict, a physical
    /// removal, or a hand-off to the spill tier. A thin wrapper,
    /// `reservation: None`, over
    /// [`Engine::evict_one_sampled_with_reservation`]; production code
    /// reaches eviction only through [`Engine::enforce_capacity`], calling
    /// the `_with_reservation` twin directly, so this wrapper is test-only.
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "kept as the stable, reservation-less entry point every existing direct \
                      test call site already uses"
        )
    )]
    pub(super) fn evict_one_sampled(&self, bucket: usize, now_ms: u64) -> EvictOutcome {
        self.evict_one_sampled_with_reservation(
            bucket,
            None,
            &Written::default(),
            OnRefusal::TierPolicy,
            now_ms,
        )
    }

    /// [`Engine::evict_one_sampled`], threading `reservation` and
    /// `on_refusal` through to [`Engine::evict_victim_locked`] and passing
    /// over the entries `written` holds.
    pub(super) fn evict_one_sampled_with_reservation(
        &self,
        bucket: usize,
        reservation: Option<&mut Reservation<'_>>,
        written: &Written,
        on_refusal: OnRefusal,
        now_ms: u64,
    ) -> EvictOutcome {
        self.note_eviction_lock_acquisition();
        let mut stripe = self.stripes[bucket].write();
        let Some(victim_bytes) = self
            .sample_candidates(&stripe, EVICTION_SAMPLE, written)
            .max_by_key(|live| idle_elapsed_ms(now_ms, live.last_access_ms.load(Ordering::Relaxed)))
            .map(|live| record_key_bytes(&live.record))
        else {
            return EvictOutcome::default();
        };
        let outcome =
            self.evict_victim_locked(&mut stripe, bucket, &victim_bytes, reservation, on_refusal);
        drop(stripe);
        match outcome {
            VictimOutcome::Removed(weight) => {
                self.total_weight
                    .fetch_sub(u64::from(weight), Ordering::Relaxed);
                self.live_count.fetch_sub(1, Ordering::Relaxed);
                EvictOutcome {
                    removed_weight: u64::from(weight),
                    deficit: 0,
                    refused: false,
                }
            }
            #[cfg(feature = "spill")]
            VictimOutcome::PendingSpill(weight, job) => {
                self.total_weight
                    .fetch_sub(u64::from(weight), Ordering::Relaxed);
                self.pending_spill_weight
                    .fetch_add(u64::from(weight), Ordering::Relaxed);
                self.finish_spill_handoff(on_refusal, job);
                EvictOutcome {
                    removed_weight: u64::from(weight),
                    deficit: 0,
                    refused: false,
                }
            }
            #[cfg(feature = "spill")]
            VictimOutcome::Deferred => EvictOutcome {
                refused: true,
                ..EvictOutcome::default()
            },
            #[cfg(feature = "spill")]
            VictimOutcome::PendingReservation(deficit) => EvictOutcome {
                removed_weight: 0,
                deficit: u64::from(deficit),
                refused: false,
            },
            VictimOutcome::Vanished => EvictOutcome::default(),
        }
    }

    /// Evicts one entry from the first non-empty stripe at or after `bucket`,
    /// wrapping around once. Returns `None` only when every stripe holds
    /// nothing to evict. A thin wrapper, `reservation: None`, over
    /// [`Engine::evict_one_scanning_with_reservation`]; test-only.
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "kept as the stable, reservation-less entry point every existing direct \
                      test call site already uses"
        )
    )]
    pub(super) fn evict_one_scanning(&self, bucket: usize, now_ms: u64) -> Option<EvictOutcome> {
        self.evict_one_scanning_with_reservation(
            bucket,
            None,
            &Written::default(),
            OnRefusal::TierPolicy,
            now_ms,
        )
        .map(|(_, outcome)| outcome)
    }

    /// [`Engine::evict_one_scanning`], reborrowing `reservation` into each
    /// candidate stripe's scan, so the same budget carries across every
    /// stripe visited. Stops early, `Some`, the first time a candidate
    /// reports [`EvictOutcome::deficit`]: one reservation covers every
    /// stripe, so a deficit in one is a deficit in all. Stops early too,
    /// `Some` with no progress, at a victim refused while a hand-off is in
    /// flight, the case [`defer_to_flusher`] leaves to the flusher. Passes
    /// over the entries `written` holds, so `None` means every stripe holds
    /// nothing else to evict. `Some` carries the stripe it stopped at.
    /// `on_refusal` applies to every victim the scan tries.
    pub(super) fn evict_one_scanning_with_reservation(
        &self,
        bucket: usize,
        mut reservation: Option<&mut Reservation<'_>>,
        written: &Written,
        on_refusal: OnRefusal,
        now_ms: u64,
    ) -> Option<(usize, EvictOutcome)> {
        (0..BUCKET_COUNT)
            .map(|step| (bucket + step) % BUCKET_COUNT)
            .find_map(|candidate| {
                let outcome = self.evict_one_sampled_with_reservation(
                    candidate,
                    reservation.as_deref_mut(),
                    written,
                    on_refusal,
                    now_ms,
                );
                let refused_while_busy = outcome.refused && self.pending_spill_weight_or_zero() > 0;
                (!outcome.made_no_progress() || outcome.deficit > 0 || refused_while_busy)
                    .then_some((candidate, outcome))
            })
    }

    /// Evicts the coldest of up to [`EVICTION_BATCH_SAMPLE`] resident
    /// entries in `bucket` under one lock hold, as many as
    /// [`eviction_batch_size`] allows for `over_by`. Each victim is removed
    /// or, with a spill tier configured and room in its queue, handed off
    /// instead; see [`Engine::evict_victim_locked`]. A thin wrapper,
    /// `reservation: None`, over
    /// [`Engine::evict_batch_sampled_with_reservation`].
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "kept as the stable, reservation-less entry point every existing direct \
                      test call site already uses"
        )
    )]
    pub(super) fn evict_batch_sampled(
        &self,
        bucket: usize,
        over_by: u64,
        now_ms: u64,
    ) -> EvictOutcome {
        self.evict_batch_sampled_with_reservation(
            bucket,
            over_by,
            None,
            &Written::default(),
            false,
            OnRefusal::TierPolicy,
            now_ms,
        )
    }

    /// [`Engine::evict_batch_sampled`], reborrowing `reservation` into each
    /// victim's own [`Engine::evict_victim_locked`] call, so the same
    /// pre-lock budget carries across every victim this lock hold evicts.
    /// Victims are ranked coldest-first by [`idle_elapsed_ms`] as of
    /// `now_ms`, correct across a `last_access_ms` rollover. With
    /// `only_if_dense`, a sample of fewer than [`EVICTION_SAMPLE`]
    /// candidates gives up nothing, so a sparse stripe is never drained.
    /// `on_refusal` applies to every victim of the batch.
    #[allow(
        clippy::too_many_arguments,
        reason = "one caller per eviction pass; the policy and the density flag are separate \
                  per-pass decisions, and a struct would not make either call site clearer"
    )]
    pub(super) fn evict_batch_sampled_with_reservation(
        &self,
        bucket: usize,
        over_by: u64,
        mut reservation: Option<&mut Reservation<'_>>,
        written: &Written,
        only_if_dense: bool,
        on_refusal: OnRefusal,
        now_ms: u64,
    ) -> EvictOutcome {
        self.note_eviction_lock_acquisition();
        let mut stripe = self.stripes[bucket].write();
        let mut sampled: Vec<(Bytes, u64, u32)> = self
            .sample_candidates(&stripe, EVICTION_BATCH_SAMPLE, written)
            .map(|live| {
                (
                    record_key_bytes(&live.record),
                    idle_elapsed_ms(now_ms, live.last_access_ms.load(Ordering::Relaxed)),
                    live.weight,
                )
            })
            .collect();
        if sampled.is_empty() || (only_if_dense && sampled.len() < EVICTION_SAMPLE) {
            return EvictOutcome::default();
        }
        sampled.sort_unstable_by_key(|&(_, idle, _)| std::cmp::Reverse(idle));
        let weights: Vec<u32> = sampled.iter().map(|&(_, _, w)| w).collect();
        let victims = eviction_batch_size(over_by, &weights);

        let mut removed_weight = 0u64;
        let mut removed_count = 0u64;
        #[cfg_attr(
            not(feature = "spill"),
            allow(
                unused_mut,
                reason = "only VictimOutcome::PendingReservation, spill-only, ever mutates this"
            )
        )]
        let mut deficit = 0u64;
        #[cfg_attr(
            not(feature = "spill"),
            allow(
                unused_mut,
                reason = "only VictimOutcome::Deferred, spill-only, ever sets this"
            )
        )]
        let mut refused = false;
        #[cfg(feature = "spill")]
        let mut pending_spills: Vec<SpillJob> = Vec::new();
        #[cfg(feature = "spill")]
        let mut pending_weight_added = 0u64;
        for (key_bytes, _, _) in sampled.into_iter().take(victims) {
            match self.evict_victim_locked(
                &mut stripe,
                bucket,
                &key_bytes,
                reservation.as_deref_mut(),
                on_refusal,
            ) {
                VictimOutcome::Removed(weight) => {
                    removed_weight += u64::from(weight);
                    removed_count += 1;
                }
                #[cfg(feature = "spill")]
                VictimOutcome::PendingSpill(weight, job) => {
                    removed_weight += u64::from(weight);
                    pending_weight_added += u64::from(weight);
                    pending_spills.push(job);
                }
                #[cfg(feature = "spill")]
                VictimOutcome::Deferred => refused = true,
                #[cfg(feature = "spill")]
                VictimOutcome::PendingReservation(bytes) => {
                    deficit += u64::from(bytes);
                }
                VictimOutcome::Vanished => {}
            }
        }
        drop(stripe);
        #[cfg(feature = "spill")]
        if pending_weight_added > 0 {
            self.pending_spill_weight
                .fetch_add(pending_weight_added, Ordering::Relaxed);
        }
        // Every victim's channel send waits until here, past the stripe
        // lock this whole batch shared: cloning a victim's bytes and
        // deciding its fate needs that lock, but handing the job to the
        // flusher's channel does not, so this is the one point per batch
        // where that per-victim cost, rather than per-eviction-pass, comes
        // off the lock.
        #[cfg(feature = "spill")]
        for job in pending_spills {
            self.finish_spill_handoff(on_refusal, job);
        }
        if removed_weight > 0 {
            self.total_weight
                .fetch_sub(removed_weight, Ordering::Relaxed);
        }
        if removed_count > 0 {
            self.live_count.fetch_sub(removed_count, Ordering::Relaxed);
        }
        EvictOutcome {
            removed_weight,
            deficit,
            refused,
        }
    }

    /// After a write to `start_bucket` may have pushed total weight over
    /// `max_capacity`, evicts sampled-cold entries, starting at
    /// `start_bucket` then pseudo-random stripes, until it is back under
    /// the cap, up to [`EVICTION_BATCH`] entries per lock hold. A random
    /// probe that lands on an empty stripe falls back to a scan for the next
    /// non-empty one. When the stripe the scan found holds at least
    /// [`EVICTION_SAMPLE`] candidates, the next pass stays there and evicts
    /// the colder half of its sample, so draining a dense stripe in an
    /// otherwise sparse cache pays for one scan per batch rather than one
    /// per victim; a sparser stripe is left to the random probes. The loop
    /// ends under the cap, with nothing left to evict, or with the rest
    /// left to the flusher, as below. Never holds
    /// two stripe locks at once; a no-op when `max_capacity` is
    /// [`u64::MAX`].
    ///
    /// The over-cap check weighs `total_weight` plus `pending_spill_weight`:
    /// a spill hand-off zeroes and frees its victim's weight from
    /// `total_weight` the instant it commits, but the victim stays fully
    /// resident until the flusher's install resolves it, so
    /// `pending_spill_weight` is what keeps that still-resident RAM
    /// counting against the cap in the meantime. When a pass evicts nothing
    /// while some hand-off is still pending, [`defer_to_flusher`] has this
    /// return rather than pay for [`Engine::evict_one_scanning`]'s scan
    /// across stripes, if the tier refused the pass's victim, most often
    /// for a full flush queue, or `total_weight` alone is within the cap.
    /// Otherwise the scan runs, spill tier configured or not, and stops
    /// early at a victim refused while a hand-off is in flight.
    ///
    /// The entries `written` holds, the ones the triggering write just
    /// stored, are passed over until nothing else is left to evict, and
    /// evicted only then, so the cap holds even for a write heavier than
    /// the whole cap. A thin wrapper, `reservation: None`, over
    /// [`Engine::enforce_capacity_with_reservation`].
    pub(crate) fn enforce_capacity(&self, start_bucket: usize, written: &mut Written, now_ms: u64) {
        let _ = self.enforce_capacity_with_reservation(start_bucket, None, written, now_ms);
    }

    /// [`Engine::enforce_capacity`], reborrowing `reservation` into every
    /// batch and scan call this loop makes, so one caller-supplied budget
    /// carries across as many lock holds as it takes to fall back under
    /// the cap.
    ///
    /// Returns the sum of [`VictimOutcome::PendingReservation`] bytes this
    /// call's eviction passes hit, where both the reservation's remaining
    /// budget and `admit`'s global headroom refused the same victim. `0`
    /// means every victim fit, was evicted, or was refused for a reason no
    /// retry changes. A nonzero return stops the loop immediately, leaving
    /// each stopped victim as [`VictimOutcome::Deferred`] does (fully
    /// resident, untouched weight) for a later pass to resolve; the caller
    /// (`Shard::retry_reservation_deficit`) pays it down with a fresh
    /// reservation.
    pub(crate) fn enforce_capacity_with_reservation(
        &self,
        start_bucket: usize,
        reservation: Option<&mut Reservation<'_>>,
        written: &mut Written,
        now_ms: u64,
    ) -> u64 {
        self.evict_down_to(
            self.max_capacity,
            start_bucket,
            reservation,
            written,
            OnRefusal::TierPolicy,
            now_ms,
        )
    }

    /// [`Engine::enforce_capacity_with_reservation`]'s loop, evicting until
    /// `total_weight` plus `pending_spill_weight` is within `limit`, at
    /// most `max_capacity`. [`Engine::promote_locked`] passes the cap less
    /// the weight it is about to reinstall, to make that room first. A
    /// no-op when `max_capacity` is [`u64::MAX`]. Passes over the entries
    /// `written` holds until a scan finds nothing else to evict, then
    /// clears `written` and goes back to `start_bucket`, where the write
    /// that triggered this call landed.
    ///
    /// `on_refusal` is what every victim the tier refuses meets. Under
    /// [`OnRefusal::KeepResident`] a refused victim is never deleted, so the
    /// loop can end over `limit`: it returns once a pass defers to the
    /// flusher or a scan of every stripe finds nothing to evict, and the
    /// caller sees the weight still over the limit.
    pub(super) fn evict_down_to(
        &self,
        limit: u64,
        start_bucket: usize,
        mut reservation: Option<&mut Reservation<'_>>,
        written: &mut Written,
        on_refusal: OnRefusal,
        now_ms: u64,
    ) -> u64 {
        if self.max_capacity == u64::MAX {
            return 0;
        }
        let mut bucket = start_bucket;
        // Whether a scan just found a victim in `bucket`: the one pass that
        // follows samples that stripe, and evicts there only from a full
        // sample.
        let mut after_scan = false;
        loop {
            let total = self.total_weight.load(Ordering::Relaxed);
            let pending = self.pending_spill_weight_or_zero();
            let current = total.saturating_add(pending);
            if current <= limit {
                return 0;
            }
            let over_by = current - limit;
            let batch_outcome = self.evict_batch_sampled_with_reservation(
                bucket,
                over_by,
                reservation.as_deref_mut(),
                written,
                after_scan,
                on_refusal,
                now_ms,
            );
            if batch_outcome.deficit > 0 {
                let total = self.total_weight.load(Ordering::Relaxed);
                let pending = self.pending_spill_weight_or_zero();
                let current = total.saturating_add(pending);
                return deficit_once_still_over_capacity(current, limit, batch_outcome.deficit);
            }
            if batch_outcome.made_no_progress() && !after_scan {
                if defer_to_flusher(
                    batch_outcome.refused,
                    self.total_weight.load(Ordering::Relaxed),
                    self.pending_spill_weight_or_zero(),
                    limit,
                ) {
                    return 0;
                }
                match self.evict_one_scanning_with_reservation(
                    bucket,
                    reservation.as_deref_mut(),
                    written,
                    on_refusal,
                    now_ms,
                ) {
                    // Only the writes' own entries are left: they go too,
                    // so the cap holds.
                    None if !written.is_empty() => {
                        written.clear();
                        bucket = start_bucket;
                        continue;
                    }
                    None => return 0,
                    Some((_, outcome)) if outcome.deficit > 0 => return outcome.deficit,
                    // The scan stopped at a victim refused while a hand-off
                    // is in flight.
                    Some((_, outcome)) if outcome.made_no_progress() => return 0,
                    Some((found, _)) => {
                        bucket = found;
                        after_scan = true;
                        continue;
                    }
                }
            }
            after_scan = false;
            bucket = self.next_pseudo_random_bucket();
        }
    }
}

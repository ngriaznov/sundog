//! Admission and refusal policy of the spill tier: reservations, queue bounds and the enqueue path.

use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use crate::store::engine::OnRefusal;

#[cfg(doc)]
use super::SpillConfig;
#[cfg(doc)]
use super::flusher::flusher_loop;
use super::rules::{
    keeps_refused_victim_resident, record_too_large, refusal_drop_reason, spill_record_len,
};
use super::{Admission, HEADER_LEN, Reservation, SpillJob, SpillTier, SpillWaitTimedOut};

impl SpillTier {
    /// Sets this tier's refusal policy: whether a resident victim whose
    /// hand-off [`SpillTier::would_accept_under`] or [`SpillTier::enqueue`]
    /// declines is left resident, to be retried on a later eviction pass,
    /// rather than the caller falling back to a delete. `Shard::attach_spill`
    /// calls this once, right after [`SpillTier::open`], from the shard's
    /// `Mode`: `Mode::Replicated` and `Mode::Distributed` pass `true`, since
    /// every peer or co-owner still holds the entry and a local delete would
    /// have anti-entropy repair it back in; the other modes pass `false`, the
    /// delete fallback that keeps RAM bounded with no repair loop to guard
    /// against. A promotion's eviction overrides this flag with
    /// [`OnRefusal::KeepResident`]: a spilled-key read never deletes an entry
    /// in any mode. Also steers which reason
    /// [`SpillTier::would_accept_under`]/[`SpillTier::enqueue`] record a
    /// refusal under: `"deferred"` in place of `"too_large"`/`"closed"`/
    /// `"queue_full"` once this is `true`, so an operator sees backpressure
    /// rather than a delete reason that fails to describe what happened.
    pub(crate) fn set_keep_resident_when_refused(&self, keep: bool) {
        self.inner
            .keep_resident_when_refused
            .store(keep, Ordering::Release);
    }

    /// Whether [`SpillTier::set_keep_resident_when_refused`] set this
    /// tier's policy to leave a refused victim resident. Read by
    /// `engine::Engine::try_spill_victim` to decide a refused victim's
    /// fate, through [`keeps_refused_victim_resident`], and by
    /// [`SpillTier::would_accept_under`] and [`SpillTier::enqueue`] for
    /// their refusal-reason bookkeeping, through [`refusal_drop_reason`].
    pub(crate) fn keep_resident_when_refused(&self) -> bool {
        self.inner
            .keep_resident_when_refused
            .load(Ordering::Acquire)
    }

    /// The reason a refusal of `specific` records under for a pass running
    /// `on_refusal`: [`refusal_drop_reason`] of
    /// [`keeps_refused_victim_resident`] and this tier's own policy.
    fn refusal_reason(&self, on_refusal: OnRefusal, specific: &'static str) -> &'static str {
        refusal_drop_reason(
            keeps_refused_victim_resident(on_refusal, self.keep_resident_when_refused()),
            specific,
        )
    }

    /// Non-blocking, best-effort: `false` means the record can never fit any
    /// region, `reason = "too_large"`, or [`SpillTier::close`] has run,
    /// `reason = "closed"`, or the flusher's queue has no room, or none is
    /// attached, `reason = "queue_full"` either way. The caller
    /// must fall back to an unconditional delete. Never touches disk on
    /// this call, so it is safe to call while holding a stripe write
    /// lock.
    ///
    /// Built from [`SpillTier::would_accept`] and [`SpillTier::enqueue`],
    /// which `Engine::try_spill_victim` calls separately instead: the first,
    /// cheap and lock-hold-safe, decides eviction's fallback right there
    /// under the stripe lock, while the second, the only part that touches
    /// the channel, runs after the lock is released. See the `spill` module docs.
    /// Test-only: production has exactly one caller of either half, and it
    /// needs them split.
    #[cfg(all(test, not(feature = "sim")))]
    pub(crate) fn try_spill(&self, job: SpillJob) -> bool {
        if self.would_accept(None, job.key_bytes.len(), job.encoded.len()) != Admission::Accepted {
            return false;
        }
        self.enqueue(OnRefusal::TierPolicy, job).is_ok()
    }

    /// [`SpillTier::would_accept_under`] with [`OnRefusal::TierPolicy`]:
    /// the tier's own flag decides which reason a refusal records.
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "the tier-policy entry point the direct tests call; production calls \
                      `would_accept_under`"
        )
    )]
    pub(crate) fn would_accept(
        &self,
        reservation: Option<&mut Reservation<'_>>,
        key_len: usize,
        value_len: usize,
    ) -> Admission {
        self.would_accept_under(OnRefusal::TierPolicy, reservation, key_len, value_len)
    }

    /// Whether a record built from `key_len` and `value_len` bytes could be
    /// admitted right now: not closed, small enough for a region, and
    /// within `flush_queue_bytes`, counted at [`SpillTier::admit_charge`].
    /// The charge keeps the byte budget from admitting more jobs than the
    /// channel has slots, so a send after an admission finds room unless
    /// the tier closed or the flusher is gone. `reservation`, when given, is spent
    /// first via [`Reservation::spend`]; otherwise this falls back to
    /// `admit.try_acquire_many`. Never awaits, so, unlike
    /// [`SpillTier::enqueue`], this is cheap enough to call under a stripe
    /// write lock. [`Admission::RefusedFinal`] records a drop under
    /// `sundog_spill_dropped_total` (`"too_large"`, `"closed"`,
    /// `"queue_full"`, or `"deferred"` when the caller leaves the victim
    /// resident: [`keeps_refused_victim_resident`] of `on_refusal` and the
    /// tier's [`SpillTier::set_keep_resident_when_refused`] flag) -- always
    /// for `closed`/`too_large`, and for `queue_full` when `reservation` is
    /// `None`. [`Admission::RefusedPending`] is reachable only with an
    /// insufficient `reservation` and a drained `admit`; no drop is
    /// recorded, and the caller retries or falls back later.
    pub(crate) fn would_accept_under(
        &self,
        on_refusal: OnRefusal,
        reservation: Option<&mut Reservation<'_>>,
        key_len: usize,
        value_len: usize,
    ) -> Admission {
        let refusal_reason = |specific: &'static str| self.refusal_reason(on_refusal, specific);
        if self.inner.closed.load(Ordering::Acquire) {
            self.inner.record_dropped(refusal_reason("closed"));
            return Admission::RefusedFinal;
        }
        let record_len_u64 = HEADER_LEN as u64 + key_len as u64 + value_len as u64;
        if record_too_large(record_len_u64, u64::from(self.inner.region_bytes)) {
            self.inner.record_dropped(refusal_reason("too_large"));
            return Admission::RefusedFinal;
        }
        let charge = self.admit_charge(key_len, value_len);
        let reservation_in_play = reservation.is_some();
        if let Some(reservation) = reservation
            && reservation.spend(charge)
        {
            return Admission::Accepted;
        }
        if let Ok(permit) = self.inner.admit.try_acquire_many(charge) {
            permit.forget();
            Admission::Accepted
        } else if reservation_in_play {
            Admission::RefusedPending
        } else {
            self.inner.record_dropped(refusal_reason("queue_full"));
            Admission::RefusedFinal
        }
    }

    /// The admission bytes one hand-off of a `key_len`-byte key and a
    /// `value_len`-byte value costs: its record length, or its share of a
    /// channel slot when that is larger. [`SpillTier::would_accept_under`]
    /// spends it, and every caller sizing a [`Reservation`], a deficit or a
    /// job's `admitted_bytes` uses it too.
    pub(crate) fn admit_charge(&self, key_len: usize, value_len: usize) -> u32 {
        self.inner
            .admit_charge(spill_record_len(key_len, value_len))
    }

    /// This tier's configured [`SpillConfig::spill_wait_timeout`], exposed
    /// here since `SpillConfig` isn't retained past [`SpillTier::open`].
    pub(crate) fn spill_wait_timeout_value(&self) -> Duration {
        self.inner.spill_wait_timeout
    }

    /// Waits, at most `timeout`, for `bytes` of flush-queue admission to
    /// become free, then commits them to the returned [`Reservation`]: the
    /// acquired permits are `.forget()`-ed on arrival, so they stop
    /// counting as available before anything is queued. `bytes` is clamped
    /// to this tier's total permit count so a request larger than the tier
    /// could ever hold does not await forever. Called once per apply-batch
    /// call, strictly before any stripe lock.
    /// `Duration::ZERO` degenerates to a single non-blocking check,
    /// reproducing today's instant-refuse behavior.
    ///
    /// # Errors
    ///
    /// Returns [`SpillWaitTimedOut`] once `timeout` elapses with the
    /// requested bytes still unavailable; the caller falls back to
    /// `reservation: None` for the rest of that call.
    pub(crate) async fn reserve(
        &self,
        bytes: u32,
        timeout: Duration,
    ) -> Result<Reservation<'_>, SpillWaitTimedOut> {
        let clamped = bytes.min(self.whole_queue_permits());
        let started = Instant::now();
        // Covers every exit, including a dropped future, since
        // `_waiter`'s Drop always runs.
        let _waiter = self.inner.record_waiter_delta();
        // Not the owned variant: nothing here needs to outlive this call.
        let outcome = tokio::time::timeout(timeout, self.inner.admit.acquire_many(clamped)).await;
        self.inner.record_wait_seconds(started.elapsed());
        match outcome {
            Ok(Ok(permit)) => {
                permit.forget();
                Ok(Reservation {
                    admit: &self.inner.admit,
                    remaining: clamped,
                })
            }
            // Unreachable, since `admit` is never closed; treated as a
            // timeout since the fallback is identical, but not counted
            // under `sundog_spill_wait_timeouts_total`.
            Ok(Err(_)) => Err(SpillWaitTimedOut),
            Err(_) => {
                self.inner.record_wait_timeout();
                Err(SpillWaitTimedOut)
            }
        }
    }

    /// The most [`SpillTier::reserve`] ever grants: one permit per byte of
    /// `flush_queue_bytes`, saturating at `u32::MAX`.
    pub(crate) fn whole_queue_permits(&self) -> u32 {
        u32::try_from(self.inner.flush_queue_bytes).unwrap_or(u32::MAX)
    }

    /// Returns `bytes` of admitted-but-never-queued capacity to `admit`.
    /// Called by `finish_spill_handoff`'s `Err` branch before `abandon`
    /// restores the victim's weight, so an unqueued job doesn't
    /// permanently shrink the admission budget.
    pub(crate) fn release(&self, bytes: u32) {
        self.inner.admit.add_permits(bytes as usize);
    }

    /// Enqueues `job` onto the flusher's channel. A job
    /// [`SpillTier::would_accept_under`] admitted always finds a slot, so a
    /// caller that checked it gets `job` back in `Err` only when no flusher
    /// is attached or it has gone; a caller with no further use for the job on failure
    /// can discard it, exactly like `try_spill`'s plain `bool`. `reason =
    /// "queue_full"` covers both a full queue and one with none
    /// attached, or `reason = "deferred"` in either case when the caller
    /// leaves the victim resident: `on_refusal` is
    /// [`OnRefusal::KeepResident`], or the tier's
    /// [`SpillTier::set_keep_resident_when_refused`] policy is set. Never
    /// touches disk, but does take the channel's own lock, so, unlike
    /// [`SpillTier::would_accept_under`], this is not meant to run while
    /// holding a stripe write lock. The permits `would_accept_under`
    /// admitted for `job` are already the caller's; this call touches no
    /// counter of its own, since only [`flusher_loop`] returns them via
    /// `admit.add_permits` once the job is pulled off the channel.
    pub(crate) fn enqueue(
        &self,
        on_refusal: OnRefusal,
        job: SpillJob,
    ) -> Result<(), Box<SpillJob>> {
        let sent = {
            let sender = self.sender.lock();
            let Some(tx) = sender.as_ref() else {
                self.inner
                    .record_dropped(self.refusal_reason(on_refusal, "queue_full"));
                return Err(Box::new(job));
            };
            tx.try_send(job)
        };
        match sent {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Full(job) | mpsc::TrySendError::Disconnected(job)) => {
                self.inner
                    .record_dropped(self.refusal_reason(on_refusal, "queue_full"));
                Err(Box::new(job))
            }
        }
    }

    /// Admission bytes currently sitting in the flusher's channel, queued but
    /// not yet taken off it, each job counted at its [`SpillTier::admit_charge`]:
    /// recomputed from `admit`'s available permits. Test-facing.
    #[cfg(all(test, not(feature = "sim")))]
    pub(crate) fn queued_bytes(&self) -> u64 {
        let available = u64::try_from(self.inner.admit.available_permits()).unwrap_or(u64::MAX);
        self.inner.flush_queue_bytes.saturating_sub(available)
    }

    /// Test-only: parks [`flusher_loop`] at the top of its loop, before its
    /// next receive, so a job's bytes stay counted in `queued_bytes` (and,
    /// upstream, a hand-off's weight stays in
    /// `crate::store::engine::Engine::pending_spill_weight`) for as long as
    /// the test needs. A flusher already blocked in the receive takes the job
    /// that wakes it, flushes it, and parks after.
    /// [`SpillTier::resume_flusher`] undoes this.
    #[cfg(all(test, not(feature = "sim")))]
    pub(crate) fn pause_flusher(&self) {
        self.inner.flusher_paused.store(true, Ordering::Release);
    }

    /// Undoes [`SpillTier::pause_flusher`].
    #[cfg(all(test, not(feature = "sim")))]
    pub(crate) fn resume_flusher(&self) {
        self.inner.flusher_paused.store(false, Ordering::Release);
    }
}

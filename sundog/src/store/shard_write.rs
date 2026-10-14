//! The write path of a [`Shard`]: single and bulk inserts, removes, `clear`, the apply and
//! forward helpers they share, and the fan-out backpressure and memory-ceiling checks.

use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::broadcast;

use super::engine::{ApplyOutcome, Reservation};
use super::{
    BUCKET_COUNT, Event, FanOutItem, FanOutQueue, Incoming, Origin, PartId, Shard, encode_key,
    engine,
};
use crate::error::{CacheError, CodecError};
use crate::hlc::Hlc;
use crate::net::REPLICATE_BATCH_COUNT;
use crate::wire::{self, WireRecord};

#[cfg(doc)]
use super::{ConflictResolver, Mode, ShardOps};
#[cfg(doc)]
use crate::ownership::OwnershipTracker;
#[cfg(doc)]
use crate::wire::MAX_FRAME;

/// Whether a [`Shard::retry_reservation_deficit`] round proves further
/// rounds futile: it was `granted` the whole flush queue (`whole_queue`
/// permits), spent none of it (`unspent` is still `granted`), and still
/// owes a `deficit`. The victim it stopped at then needs more than the
/// whole queue, which no reservation can ever cover. A round granted less
/// than the whole queue proves nothing: its victim may only need more than
/// that round asked for, and the next round asks for its whole deficit.
/// Pure; unit tested directly.
#[cfg(feature = "spill")]
pub(super) const fn retry_round_is_futile(
    granted: u32,
    whole_queue: u32,
    unspent: u32,
    deficit: u64,
) -> bool {
    deficit > 0 && granted == whole_queue && unspent == granted
}

/// Builds an [`Incoming::Put`], for the bulk-insert closures that reassemble one from a prepared tuple.
fn put_incoming<V>(value: V, expires_at_ms: Option<u64>, encoded: Bytes) -> Incoming<V> {
    Incoming::Put {
        value,
        expires_at_ms,
        encoded,
    }
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
    /// Bytes this shard's live entries hold in memory, counted only under
    /// a [`Shard::with_max_resident_bytes`] ceiling and `None` without one:
    /// a fixed per-entry overhead for the entry slot and its index row,
    /// plus each record's heap-held key and value bytes. A spilled entry
    /// counts only its key. A lower bound on the process memory the entries
    /// occupy, since allocator rounding and spare arena capacity are not
    /// counted.
    #[must_use]
    pub fn resident_bytes(&self) -> Option<u64> {
        self.engine.resident_bytes()
    }

    /// [`Shard::resident_bytes`] once it has reached the ceiling set by
    /// [`Shard::with_max_resident_bytes`], `None` below it or with no
    /// ceiling.
    pub(super) fn at_ceiling(&self) -> Option<u64> {
        let resident_bytes = self.engine.resident_bytes()?;
        (resident_bytes >= self.max_resident_bytes).then_some(resident_bytes)
    }

    /// `Err(CacheError::OverMemoryCeiling)`, counted, when this shard is at
    /// its ceiling: the gate every local write passes first.
    pub(super) fn check_ceiling(&self) -> Result<(), CacheError> {
        match self.at_ceiling() {
            None => Ok(()),
            Some(resident_bytes) => {
                if let Some(refusals) = &self.ceiling_refusals {
                    refusals.writes.increment(1);
                }
                Err(CacheError::OverMemoryCeiling {
                    cache: self.name.clone(),
                    resident_bytes,
                    ceiling: self.max_resident_bytes,
                })
            }
        }
    }

    /// Handles the outcome of one versioned apply: a local write's fan-out and
    /// event. A `Rejected` outcome, where the incoming record lost, is a
    /// silent no-op.
    ///
    /// `notify_fan_out: false` is how [`Shard::insert_many`] and
    /// [`Shard::remove_many`] opt out of the per-write queue push in favor
    /// of [`Shard::hand_off_bulk`].
    pub(super) fn handle_apply_outcome(
        &self,
        outcome: ApplyOutcome<K, V>,
        origin: Origin,
        notify_fan_out: bool,
    ) {
        match outcome {
            ApplyOutcome::Rejected => {}
            ApplyOutcome::Put {
                key,
                value,
                created,
                version,
            } => {
                // A refusal means the cache is closing: the write landed
                // here, as a detached local write, and fans out nowhere.
                if notify_fan_out && matches!(origin, Origin::Local) {
                    let _ = self.fan_out.push(FanOutItem::Applied(key.clone(), version));
                }
                if self.events.receiver_count() > 0 {
                    let event = if created {
                        Event::Created { key, value, origin }
                    } else {
                        Event::Updated { key, value, origin }
                    };
                    let _ = self.events.send(event);
                }
            }
            ApplyOutcome::Tombstoned { key, version } => {
                // A refusal means the cache is closing: the write landed
                // here, as a detached local write, and fans out nowhere.
                if notify_fan_out && matches!(origin, Origin::Local) {
                    let _ = self.fan_out.push(FanOutItem::Applied(key.clone(), version));
                }
                if self.events.receiver_count() > 0 {
                    let _ = self.events.send(Event::Removed { key, origin });
                }
            }
        }
    }

    /// The versioned-apply core: applies `incoming` at `ver` for `key` iff the
    /// configured [`ConflictResolver`] picks it over whatever this shard
    /// currently holds, publishing the resulting [`Event`] on success.
    /// Idempotent and commutative: the single path shared by single writes,
    /// replication, state transfer, and anti-entropy repair. Bulk writes and
    /// replicated batches go through `engine::Engine::apply_many` directly
    /// instead, holding one stripe lock across a whole group.
    ///
    /// Equal versions are always a no-op: a given `(wall_ms, logical, node)`
    /// triple comes from at most one write ever, so an equal-version
    /// incoming record is already the one stored.
    fn apply(&self, key: K, key_bytes: Bytes, ver: Hlc, incoming: Incoming<V>, origin: Origin) {
        let hash = engine::hash_key_bytes(key_bytes.as_ref());
        let bucket = engine::stripe_index_from_hash(hash);
        let outcome = self
            .engine
            .apply_many(
                bucket,
                vec![(hash, key, key_bytes, ver, incoming)],
                self.resolver.as_ref(),
                self.tombstone_ttl_ms,
                self.tombstone_max_ttl_ms,
                self.now_ms(),
            )
            .into_iter()
            .next()
            .expect("invariant: apply_many returns exactly one outcome per entry given");
        self.handle_apply_outcome(outcome, origin, true);
    }

    /// Whether this shard's own current ownership view owns `key_bytes`'
    /// part. `true` unconditionally for every mode but `Mode::Distributed`,
    /// which has no [`OwnershipTracker`] attached at all: every other mode
    /// owns everything it holds.
    pub(super) fn owns_key(&self, key_bytes: &[u8]) -> bool {
        self.ownership
            .as_ref()
            .is_none_or(|tracker| tracker.current().owns(PartId::of_key(key_bytes)))
    }

    /// Non-owner write path: builds the [`WireRecord`] a
    /// `Mode::Distributed` write for a bucket this node does not own fans
    /// out, without ever touching `engine`; see the store module's
    /// write-path docs for the no-window proof this relies on. Emits
    /// [`Event::Created`]/[`Event::Removed`] directly from the
    /// caller-supplied value: a non-owner holds no prior copy to tell a
    /// create from an update, so a forwarded write never emits
    /// [`Event::Updated`], a documented, deliberate difference from every
    /// other write path here. The caller pushes the returned record onto
    /// [`Shard::fan_out`] as [`FanOutItem::Forward`] and counts
    /// `sundog_forwarded_writes_total{cache}`; this method only builds the
    /// record and emits the event.
    fn forward_write(
        &self,
        key: K,
        key_bytes: Bytes,
        incoming: Incoming<V>,
        ver: Hlc,
    ) -> WireRecord {
        match incoming {
            Incoming::Put {
                value,
                expires_at_ms,
                encoded,
            } => {
                let rec = WireRecord {
                    key: key_bytes,
                    value: Some(encoded),
                    ver,
                    expires_at_ms,
                };
                if self.events.receiver_count() > 0 {
                    let _ = self.events.send(Event::Created {
                        key,
                        value,
                        origin: Origin::Local,
                    });
                }
                rec
            }
            Incoming::Tombstone => {
                let rec = WireRecord {
                    key: key_bytes,
                    value: None,
                    ver,
                    expires_at_ms: None,
                };
                if self.events.receiver_count() > 0 {
                    let _ = self.events.send(Event::Removed {
                        key,
                        origin: Origin::Local,
                    });
                }
                rec
            }
        }
    }

    /// Splits prepared entries into owned and forwarded halves by current part ownership.
    fn partition_owned<T>(&self, prepared: Vec<T>, hash: impl Fn(&T) -> u64) -> (Vec<T>, Vec<T>) {
        match &self.ownership {
            Some(tracker) => {
                let view = tracker.current();
                prepared
                    .into_iter()
                    .partition(|entry| view.owns(PartId::from_hash(hash(entry))))
            }
            None => (prepared, Vec::new()),
        }
    }

    /// [`Shard::insert_many`]'s and [`Shard::remove_many`]'s shared fan-out step, chunked into [`REPLICATE_BATCH_COUNT`] batches.
    fn forward_prepared<T>(
        &self,
        prepared: Vec<T>,
        to_forward: impl Fn(T) -> (K, Bytes, Hlc, Incoming<V>),
    ) -> bool {
        if prepared.is_empty() {
            return true;
        }
        let total = u64::try_from(prepared.len()).unwrap_or(u64::MAX);
        let mut landed = Vec::new();
        let flush = |landed: &mut Vec<_>| {
            self.fan_out
                .extend(landed.drain(..).map(FanOutItem::Forward))
        };
        for entry in prepared {
            let (key, key_bytes, ver, incoming) = to_forward(entry);
            landed.push(self.forward_write(key, key_bytes, incoming, ver));
            if landed.len() >= REPLICATE_BATCH_COUNT && !flush(&mut landed) {
                return false;
            }
        }
        if !flush(&mut landed) {
            return false;
        }
        metrics::counter!("sundog_forwarded_writes_total", "cache" => self.name.to_string())
            .increment(total);
        true
    }

    /// Stamps and applies a local write, then fans it out per [`Mode`]:
    /// `Invalidate` for `Mode::Invalidation` (the merged record as
    /// `Replicate` when the resolver merges and the peer speaks
    /// [`crate::wire::PROTOCOL_INVALIDATION_FOLD`]), `Replicate` for
    /// `Mode::Replicated`, nothing for `Mode::Local`, through the
    /// composition layer's subscription to [`Shard::events`]. Under
    /// `Mode::Distributed` a write for a bucket this node does not own is
    /// never applied here: it is built as a record and forwarded to the
    /// bucket's owners through the same queue.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::ValueTooLarge`] if the wire frame this write would
    /// replicate as exceeds the configured frame cap. See
    /// [`Shard::with_max_frame`], default [`MAX_FRAME`]. Returns
    /// [`CacheError::OverMemoryCeiling`] if this shard is at the ceiling set
    /// by [`Shard::with_max_resident_bytes`].
    pub async fn insert(&self, key: K, value: V) -> Result<(), CacheError> {
        self.wait_for_fan_out_room().await;
        self.insert_sync(key, value)
    }

    /// [`Shard::insert`] without an async runtime: same fan-out and events.
    ///
    /// # Errors
    ///
    /// As [`Shard::insert`].
    pub fn insert_sync(&self, key: K, value: V) -> Result<(), CacheError> {
        self.insert_expiring(key, value, None)
    }

    /// [`Shard::insert`] with a lifespan for this entry alone, overriding the
    /// shard's default TTL. Replicates exactly as a default-TTL stamp does.
    ///
    /// # Errors
    ///
    /// As [`Shard::insert`].
    pub async fn insert_with_ttl(&self, key: K, value: V, ttl: Duration) -> Result<(), CacheError> {
        self.wait_for_fan_out_room().await;
        self.insert_expiring(key, value, Some(ttl))
    }

    fn insert_expiring(&self, key: K, value: V, ttl: Option<Duration>) -> Result<(), CacheError> {
        self.check_ceiling()?;
        let key_bytes = encode_key(&key)?;
        let encoded = Bytes::from(postcard::to_stdvec(&value).map_err(CodecError::from)?);
        let ver = self.stamp_local();
        let expires_at_ms = self.expiry_for(ver.wall_ms, ttl);
        let wire_size = wire::replicate_frame_len(self.name.len(), key_bytes.len(), encoded.len());
        if wire_size > self.max_frame {
            return Err(CacheError::ValueTooLarge {
                cache: self.name.clone(),
                size: encoded.len(),
                limit: self.max_frame,
            });
        }
        let incoming = Incoming::Put {
            value,
            expires_at_ms,
            encoded,
        };
        self.apply_or_forward(key, key_bytes, ver, incoming)
    }

    /// Waits for the fan-out backlog to fall under capacity via
    /// [`FanOutQueue::wait_for_room`]. Every async write path calls this; the
    /// synchronous chain never does.
    pub(super) async fn wait_for_fan_out_room(&self) {
        self.fan_out
            .wait_for_room(self.fan_out_backlog_capacity, self.fan_out_wait_timeout)
            .await;
    }

    /// Applies a versioned write locally if this shard owns `key_bytes`'
    /// bucket, or builds and forwards it to the bucket's current owners
    /// otherwise: the tail every local-write entry point shares,
    /// [`Shard::insert_expiring`] and a coalesced [`Shard::merge`] fold's
    /// flush alike.
    pub(super) fn apply_or_forward(
        &self,
        key: K,
        key_bytes: Bytes,
        ver: Hlc,
        incoming: Incoming<V>,
    ) -> Result<(), CacheError> {
        if !self.owns_key(key_bytes.as_ref()) {
            let rec = self.forward_write(key, key_bytes, incoming, ver);
            if !self.fan_out.push(FanOutItem::Forward(rec)) {
                return Err(self.closed());
            }
            metrics::counter!("sundog_forwarded_writes_total", "cache" => self.name.to_string())
                .increment(1);
            return Ok(());
        }
        self.apply(key, key_bytes, ver, incoming, Origin::Local);
        Ok(())
    }

    /// [`Shard::insert`] for many entries, grouped by key stripe and applied
    /// under one lock acquisition per touched stripe rather than one per
    /// entry, so unrelated writers to other stripes are never blocked for
    /// the whole batch. Not a transaction: entries validated
    /// before an oversized one apply regardless of the error this returns.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::ValueTooLarge`] if any entry's wire frame exceeds
    /// the configured frame cap (see [`Shard::insert`]). Returns
    /// [`CacheError::OverMemoryCeiling`], applying nothing, if this shard is
    /// at its ceiling when the call begins; a batch that begins under the
    /// ceiling applies whole.
    pub async fn insert_many(
        &self,
        entries: impl IntoIterator<Item = (K, V)>,
    ) -> Result<(), CacheError> {
        self.insert_many_expiring(entries, None).await
    }

    /// [`Shard::insert_many`] with one lifespan applied to every entry in the
    /// batch, overriding the shard's default TTL. See
    /// [`Shard::insert_with_ttl`].
    ///
    /// # Errors
    ///
    /// As [`Shard::insert_many`].
    pub async fn insert_many_with_ttl(
        &self,
        entries: impl IntoIterator<Item = (K, V)>,
        ttl: Duration,
    ) -> Result<(), CacheError> {
        self.insert_many_expiring(entries, Some(ttl)).await
    }

    async fn insert_many_expiring(
        &self,
        entries: impl IntoIterator<Item = (K, V)>,
        ttl: Option<Duration>,
    ) -> Result<(), CacheError> {
        self.check_ceiling()?;
        // Stops preparing entries at the first one that fails to encode or
        // exceeds `max_frame`, but keeps everything prepared before it so
        // the apply loop below still applies those, per this method's
        // not-a-transaction contract.
        let mut prepared = Vec::new();
        let mut failure: Option<CacheError> = None;
        for (key, value) in entries {
            let key_bytes = match encode_key(&key) {
                Ok(key_bytes) => key_bytes,
                Err(err) => {
                    failure = Some(err.into());
                    break;
                }
            };
            let encoded = match postcard::to_stdvec(&value).map_err(CodecError::from) {
                Ok(bytes) => Bytes::from(bytes),
                Err(err) => {
                    failure = Some(err.into());
                    break;
                }
            };
            let ver = self.stamp_local();
            let expires_at_ms = self.expiry_for(ver.wall_ms, ttl);
            let wire_size =
                wire::replicate_frame_len(self.name.len(), key_bytes.len(), encoded.len());
            if wire_size > self.max_frame {
                failure = Some(CacheError::ValueTooLarge {
                    cache: self.name.clone(),
                    size: encoded.len(),
                    limit: self.max_frame,
                });
                break;
            }
            let hash = engine::hash_key_bytes(key_bytes.as_ref());
            prepared.push((hash, key, key_bytes, ver, value, expires_at_ms, encoded));
        }

        // A `Mode::Distributed` shard splits off every entry whose bucket
        // this node does not own before the owned group's ordinary
        // by-stripe apply below: a forwarded entry never reaches `engine`,
        // matching `Shard::insert`'s single-write guard.
        let (owned_prepared, forwarded_prepared) = self.partition_owned(prepared, |e| e.0);
        self.wait_for_fan_out_room().await;
        if !self.forward_prepared(forwarded_prepared, |(_, key, key_bytes, ver, v, e, enc)| {
            (key, key_bytes, ver, put_incoming(v, e, enc))
        }) {
            return Err(self.closed());
        }

        self.apply_grouped(owned_prepared, |(hash, key, key_bytes, ver, v, e, enc)| {
            (hash, key, key_bytes, ver, put_incoming(v, e, enc))
        })
        .await;
        match failure {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// Reserves flush-queue admission for one whole
    /// [`Shard::apply_grouped`]/[`ShardOps::apply_remote_batch`] call, before
    /// any stripe lock: sums `SpillTier::admit_charge` over every
    /// `Incoming::Put`, then makes one `SpillTier::reserve` call bounded by
    /// the tier's `spill_wait_timeout`. Also returns that wait's deadline so
    /// [`Shard::retry_reservation_deficit`] shares the same budget instead of
    /// starting a fresh one. `None` for both when no spill tier is attached
    /// or the reserve times out; the caller then falls back to
    /// [`engine::Engine::apply_many_with_reservation`]'s non-blocking path.
    #[cfg_attr(
        not(feature = "spill"),
        allow(
            clippy::unused_async,
            clippy::unused_self,
            reason = "the tier accessor and `reserve` only exist under feature = \"spill\""
        )
    )]
    pub(super) async fn reserve_for_batch<'e>(
        &self,
        entries: impl Iterator<Item = (&'e Bytes, &'e Incoming<V>)>,
    ) -> (Option<Reservation<'_>>, Option<std::time::Instant>)
    where
        V: 'e,
    {
        #[cfg(feature = "spill")]
        {
            let Some(tier) = self.engine.spill() else {
                return (None, None);
            };
            let reserved: u64 = entries
                .map(|(key_bytes, incoming)| match incoming {
                    Incoming::Put { encoded, .. } => {
                        u64::from(tier.admit_charge(key_bytes.len(), encoded.len()))
                    }
                    Incoming::Tombstone => 0,
                })
                .sum();
            let reserved = u32::try_from(reserved).unwrap_or(u32::MAX);
            let budget = tier.spill_wait_timeout_value();
            let deadline = std::time::Instant::now() + budget;
            let reservation = tier.reserve(reserved, budget).await.ok();
            (reservation, Some(deadline))
        }
        #[cfg(not(feature = "spill"))]
        {
            let _ = entries;
            (None, None)
        }
    }

    /// Pays down `deficit` bytes
    /// [`engine::Engine::apply_many_with_reservation`]'s per-bucket loop left
    /// resident (never dropped) when the call's one `Reservation` ran out.
    /// Retries with a fresh [`SpillTier::reserve`] for the remainder, then
    /// another `enforce_capacity_with_reservation` pass, until the deficit
    /// clears or `deadline` passes. `deadline` is the same one
    /// [`Shard::reserve_for_batch`] measured, not a fresh budget, so the
    /// whole call stays bounded by one `spill_wait_timeout` rather than `2x`
    /// it. A no-op when `deficit` is `0`. Once the budget runs out with a
    /// deficit still outstanding, this makes one closing
    /// [`engine::Engine::enforce_capacity`] call (`reservation: None`) so the
    /// deficit is resolved before this call returns, not deferred to
    /// whatever write happens to touch this shard next. A round that held
    /// the whole flush queue and spent none of it ends the retries at once
    /// (see [`retry_round_is_futile`]): the victim it stopped at needs more
    /// than the whole queue, so no later reservation could cover it either.
    /// Every pass leaves the entries `written` holds, the batch's own, for
    /// last.
    #[cfg_attr(
        not(feature = "spill"),
        allow(
            clippy::unused_async,
            clippy::unused_self,
            reason = "the tier accessor and `reserve` only exist under feature = \"spill\""
        )
    )]
    pub(super) async fn retry_reservation_deficit(
        &self,
        start_bucket: usize,
        deficit: u64,
        deadline: Option<std::time::Instant>,
        written: &mut engine::Written,
    ) {
        #[cfg(feature = "spill")]
        {
            if deficit == 0 {
                return;
            }
            let Some(tier) = self.engine.spill() else {
                return;
            };
            let Some(deadline) = deadline else {
                self.engine
                    .enforce_capacity(start_bucket, written, self.now_ms());
                return;
            };
            let mut deficit = deficit;
            while deficit > 0 {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    // Budget already spent (initial wait or an earlier
                    // iteration): skip straight to the closing fallback.
                    break;
                }
                let bytes = u32::try_from(deficit).unwrap_or(u32::MAX);
                match tier.reserve(bytes, remaining).await {
                    Ok(mut reservation) => {
                        let granted = reservation.remaining();
                        deficit = self.engine.enforce_capacity_with_reservation(
                            start_bucket,
                            Some(&mut reservation),
                            written,
                            self.now_ms(),
                        );
                        if retry_round_is_futile(
                            granted,
                            tier.whole_queue_permits(),
                            reservation.remaining(),
                            deficit,
                        ) {
                            break;
                        }
                    }
                    // `reserve` already recorded
                    // `sundog_spill_wait_timeouts_total`; the fallback below resolves the rest.
                    Err(_) => break,
                }
            }
            if deficit > 0 {
                self.engine
                    .enforce_capacity(start_bucket, written, self.now_ms());
            }
        }
        #[cfg(not(feature = "spill"))]
        {
            let _ = (start_bucket, deficit, deadline, written);
        }
    }

    /// [`Shard::insert_many_expiring`]'s and [`Shard::remove_many`]'s shared apply step: groups by stripe, applies each under one lock.
    ///
    /// Reserves flush-queue admission once for the whole call via
    /// [`Shard::reserve_for_batch`], threading the `Reservation` through
    /// every bucket's [`engine::Engine::apply_many_with_reservation`] call.
    /// [`Shard::retry_reservation_deficit`] then pays down any deficit,
    /// sharing the same deadline rather than a fresh budget.
    async fn apply_grouped<T>(
        &self,
        prepared: Vec<T>,
        to_batch_entry: impl Fn(T) -> (u64, K, Bytes, Hlc, Incoming<V>),
    ) {
        let batch_entries: Vec<(u64, K, Bytes, Hlc, Incoming<V>)> =
            prepared.into_iter().map(to_batch_entry).collect();

        let (reservation, deadline) = self
            .reserve_for_batch(
                batch_entries
                    .iter()
                    .map(|(_, _, key_bytes, _, incoming)| (key_bytes, incoming)),
            )
            .await;

        let mut by_stripe: Vec<Vec<_>> = (0..BUCKET_COUNT).map(|_| Vec::new()).collect();
        for batch_entry in batch_entries {
            by_stripe[engine::stripe_index_from_hash(batch_entry.0)].push(batch_entry);
        }
        let now = self.now_ms();
        let mut applied_keys: Vec<(K, Hlc)> = Vec::new();
        let mut written = engine::Written::default();
        // The reservation lives in this block only, so its unspent budget
        // is back with the tier before `retry_reservation_deficit` asks for
        // a fresh one: a retry clamped to the whole flush queue can never be
        // granted while this call still holds part of it.
        let deficit = {
            let mut reservation = reservation;
            let mut deficit = 0u64;
            for (bucket, group) in by_stripe.into_iter().enumerate() {
                if group.is_empty() {
                    continue;
                }
                let (outcomes, bucket_deficit) = self.engine.apply_many_with_reservation(
                    bucket,
                    group,
                    self.resolver.as_ref(),
                    self.tombstone_ttl_ms,
                    self.tombstone_max_ttl_ms,
                    now,
                    reservation.as_mut(),
                    &mut written,
                );
                deficit += bucket_deficit;
                for outcome in outcomes {
                    applied_keys.extend(outcome.key().cloned().zip(outcome.version()));
                    self.handle_apply_outcome(outcome, Origin::Local, false);
                }
                self.hand_off_bulk(&mut applied_keys, false);
            }
            deficit
        };
        self.hand_off_bulk(&mut applied_keys, true);
        self.retry_reservation_deficit(0, deficit, deadline, &mut written)
            .await;
    }

    /// Stamps and applies a local tombstone, then fans it out per [`Mode`], as
    /// [`Shard::insert`] does.
    ///
    /// # Errors
    ///
    /// Returns a [`CacheError`] if the key cannot be encoded for the wire.
    pub async fn remove(&self, key: &K) -> Result<(), CacheError> {
        self.wait_for_fan_out_room().await;
        self.remove_sync(key)
    }

    /// [`Shard::remove`] without an async runtime: same fan-out and events.
    ///
    /// # Errors
    ///
    /// As [`Shard::remove`].
    pub fn remove_sync(&self, key: &K) -> Result<(), CacheError> {
        let key_bytes = encode_key(key)?;
        let ver = self.stamp_local();
        if !self.owns_key(key_bytes.as_ref()) {
            let rec = self.forward_write(key.clone(), key_bytes, Incoming::Tombstone, ver);
            if !self.fan_out.push(FanOutItem::Forward(rec)) {
                return Err(self.closed());
            }
            metrics::counter!("sundog_forwarded_writes_total", "cache" => self.name.to_string())
                .increment(1);
            return Ok(());
        }
        self.apply(
            key.clone(),
            key_bytes,
            ver,
            Incoming::Tombstone,
            Origin::Local,
        );
        Ok(())
    }

    /// [`Shard::remove`] for many keys at once, the tombstone counterpart of
    /// [`Shard::insert_many`]. **Not a transaction**: if a key partway through
    /// fails to encode, the keys before it are still tombstoned.
    ///
    /// # Errors
    ///
    /// Returns a [`CacheError`] if any key fails to encode for the wire.
    pub async fn remove_many(&self, keys: impl IntoIterator<Item = K>) -> Result<(), CacheError> {
        let mut prepared = Vec::new();
        for key in keys {
            let key_bytes = encode_key(&key)?;
            let ver = self.stamp_local();
            let hash = engine::hash_key_bytes(key_bytes.as_ref());
            prepared.push((hash, key, key_bytes, ver));
        }

        // Same owner-vs-forward split as `Shard::insert_many_expiring`: a
        // forwarded tombstone never reaches `engine`.
        let (owned_prepared, forwarded_prepared) = self.partition_owned(prepared, |e| e.0);
        self.wait_for_fan_out_room().await;
        if !self.forward_prepared(forwarded_prepared, |(_, key, key_bytes, ver)| {
            (key, key_bytes, ver, Incoming::Tombstone)
        }) {
            return Err(self.closed());
        }

        self.apply_grouped(owned_prepared, |(hash, key, key_bytes, ver)| {
            (hash, key, key_bytes, ver, Incoming::Tombstone)
        })
        .await;
        Ok(())
    }

    /// Hands a bulk write's landed keys to the fan-out queue one full
    /// replicate batch ([`REPLICATE_BATCH_COUNT`] keys) at a time, and the
    /// remainder when `flush` is set, so replication streams full frames
    /// while the rest of the write is still applying and a fill costs a
    /// bounded number of frames per peer whatever the machine's speed.
    pub(super) fn hand_off_bulk(&self, landed: &mut Vec<(K, Hlc)>, flush: bool) {
        if landed.is_empty() || (!flush && landed.len() < REPLICATE_BATCH_COUNT) {
            return;
        }
        // A refusal means the cache is closing: these writes landed here,
        // as detached local writes, and fan out nowhere.
        let _ = self.fan_out.extend(
            landed
                .drain(..)
                .map(|(key, ver)| FanOutItem::Applied(key, ver)),
        );
    }

    /// The error a forwarded write gets once the cache is closing: it
    /// lands nowhere locally, so refusing it is the only honest answer. A
    /// write this node applies itself still succeeds, as a detached local
    /// write.
    fn closed(&self) -> CacheError {
        CacheError::Closed {
            cache: self.name.clone(),
        }
    }

    /// Tombstones every key this node currently holds, via
    /// [`Shard::remove_many`] over a snapshot of [`Shard::keys`], cost
    /// O(entries). An entry a peer holds that never reached
    /// this node survives untouched.
    ///
    /// # Errors
    ///
    /// As [`Shard::remove_many`].
    pub async fn clear(&self) -> Result<(), CacheError> {
        self.remove_many(self.keys()).await
    }

    /// Drops the local copy of `key` without writing a tombstone or fanning
    /// out. An escape hatch for tests and manual cache-busting; the entry
    /// may reappear on the next replicated write or anti-entropy round.
    pub async fn invalidate_local(&self, key: &K) {
        let Ok(key_bytes) = postcard::to_stdvec(key) else {
            return;
        };
        let hash = engine::hash_key_bytes(&key_bytes);
        self.engine.invalidate_local(&key_bytes, hash);
    }

    /// Subscribes to this shard's change events.
    #[must_use]
    pub fn events(&self) -> broadcast::Receiver<Event<K, V>> {
        self.events.subscribe()
    }

    /// The queue of locally written keys, each with the version it was
    /// written at, awaiting fan-out. `fan_out_task`'s cheaper alternative to
    /// subscribing on [`Shard::events`], since it never reads `Event`'s
    /// `value`. Carries local writes and forwarded
    /// non-owner writes; see [`FanOutQueue`] and [`FanOutItem`].
    pub(crate) fn fan_out_queue(&self) -> Arc<FanOutQueue<FanOutItem<K>>> {
        Arc::clone(&self.fan_out)
    }
}

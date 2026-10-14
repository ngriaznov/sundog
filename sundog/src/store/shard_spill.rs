//! The spill tier of a [`Shard`]: attaching a tier and replaying it, reading spilled entries
//! back, and closing the tier with or without a checkpoint.

#[cfg(feature = "spill")]
use std::collections::HashSet;
use std::hash::Hash;
#[cfg(feature = "spill")]
use std::sync::Arc;
#[cfg(all(feature = "spill", not(feature = "sim"), test))]
use std::time::Duration;

#[cfg(feature = "spill")]
use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::Shard;
#[cfg(feature = "spill")]
use super::{Mode, PartId, SpillRead, encode_key, engine, now_ms, spill};
#[cfg(feature = "spill")]
use crate::hlc::Hlc;
#[cfg(feature = "spill")]
use crate::ownership::OwnershipTracker;

#[cfg(doc)]
use super::ShardOps;

/// [`Shard::attach_spill`]'s result: the parts the reopen replayed at
/// least one record for. Empty covers both a cold fallback and
/// a clean reopen with nothing to replay.
#[cfg(feature = "spill")]
pub(crate) struct AttachSpillOutcome {
    pub(crate) warm_parts: HashSet<PartId>,
}

/// Increments `sundog_spill_checkpoint_entries_total{cache,stage}` by
/// `count` for one checkpoint phase, matching
/// `Shard::close_spill_checkpointed`'s `info` logs.
#[cfg(feature = "spill")]
fn record_checkpoint_stage(cache: &str, stage: &'static str, count: u64) {
    metrics::counter!(
        "sundog_spill_checkpoint_entries_total",
        "cache" => cache.to_string(),
        "stage" => stage,
    )
    .increment(count);
}

/// Increments `sundog_spill_reopen_entries_total{cache,stage}` by `count`
/// for one reopen phase, matching [`Shard::attach_spill`]'s `info` logs.
#[cfg(feature = "spill")]
fn record_reopen_stage(cache: &str, stage: &'static str, count: u64) {
    metrics::counter!(
        "sundog_spill_reopen_entries_total",
        "cache" => cache.to_string(),
        "stage" => stage,
    )
    .increment(count);
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
    /// Opens a local SSD/NVMe spill tier at `cfg` and attaches it to this
    /// shard's engine: once `max_capacity` is exceeded, eviction demotes the
    /// coldest entries onto disk instead of discarding them. Call this last
    /// in the builder chain, right after [`Shard::with_weigher`] if both are
    /// used. [`Shard::with_weigher`] rebuilds the engine from scratch, and
    /// the tier attaches to whichever engine is current when this runs. A
    /// write's eviction that the tier refuses leaves the victim resident, to
    /// be retried by a later eviction pass, for a `Mode::Replicated` or
    /// `Mode::Distributed` shard, and deletes it for
    /// `Mode::Local`/`Mode::Invalidation`; a promotion's eviction leaves a
    /// refused victim resident in every mode.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`std::io::Error`] if the tier's directory or
    /// a region file cannot be created; see `spill::SpillTier::open`.
    ///
    /// # Panics
    ///
    /// Panics if `Shard::attach_spill` has already run on this shard; this
    /// method calls it.
    #[cfg(feature = "spill")]
    pub fn with_spill(self, cfg: &spill::SpillConfig) -> Result<Self, std::io::Error> {
        self.attach_spill(cfg)?;
        Ok(self)
    }

    /// The core of [`Shard::with_spill`], through `&self` rather than
    /// consuming `self`: opens the tier, attaches it to this shard's engine,
    /// and creates the spilled-key read-path metric handles.
    /// `Engine::spill`/`Engine::set_spill` and this shard's own `spill_read`
    /// are `OnceLock`s rather than plain fields behind `&mut self`, so this
    /// can run once the shard is already `Arc`-shared.
    /// [`crate::cache::CacheBuilder::open`] relies on that. It reserves
    /// this shard's name in the cluster's shard registry first, then calls
    /// this only for the `open()` that wins that reservation, so a losing,
    /// already-open, `open()` never wipes or preallocates another cache's
    /// region files. [`Shard::with_spill`] stays the ordinary
    /// builder-chain entry point for a caller that already owns an
    /// unshared `Shard`.
    ///
    ///
    /// Prefers `spill::SpillTier::reopen` over a cold open when
    /// `SpillConfig::warm_reopen` is on and a checkpoint is still inside
    /// `ClusterConfig::tombstone_ttl`, replaying it instead of wiping the
    /// region files, filtered to [`Shard::with_ownership`]'s owned parts.
    /// Returns [`AttachSpillOutcome::warm_parts`], the subset it replayed.
    /// A warm-reloaded part still counts as cold per
    /// [`ShardOps::is_cold_part`] until the caller clears it.
    ///
    /// A warm reopen advances this shard's clock past the greatest version it
    /// replayed, so a later local write or remove stamps after every
    /// replayed entry, even when the system clock restarted behind those
    /// stamps. Nothing stamps on the shard before this returns: the cache is
    /// not advertised and no handle to it exists yet.
    /// # Errors
    ///
    /// Returns the underlying [`std::io::Error`] if the tier's directory or
    /// a region file cannot be created; see `spill::SpillTier::open`.
    ///
    /// # Panics
    ///
    /// Panics if called more than once on the same shard.
    #[cfg(feature = "spill")]
    pub(crate) fn attach_spill(
        &self,
        cfg: &spill::SpillConfig,
    ) -> Result<AttachSpillOutcome, std::io::Error> {
        let sink = Arc::clone(&self.engine) as Arc<dyn spill::SpillSink>;
        let view = self.ownership.as_ref().map(OwnershipTracker::current);
        let owned = move |part: PartId| view.as_ref().is_none_or(|v| v.owns(part));
        let outcome = spill::SpillTier::reopen(
            cfg,
            &self.name,
            sink.as_ref(),
            now_ms(),
            self.tombstone_ttl_ms,
            owned,
        )?;
        self.seed_clock_from_replay(outcome.max_ver);
        if outcome.warm {
            tracing::info!(
                cache = %self.name,
                records = outcome.records_installed,
                "sundog spill: warm reopen replayed the tier from disk",
            );
            metrics::counter!(
                "sundog_spill_reopen_total",
                "cache" => self.name.to_string(),
                "outcome" => "warm",
                "reason" => "",
            )
            .increment(1);
        } else {
            let reason = outcome.reason.unwrap_or("unknown");
            tracing::info!(
                cache = %self.name,
                reason,
                "sundog spill: cold fallback",
            );
            metrics::counter!(
                "sundog_spill_reopen_total",
                "cache" => self.name.to_string(),
                "outcome" => "cold_fallback",
                "reason" => reason,
            )
            .increment(1);
        }
        metrics::counter!(
            "sundog_spill_reopen_records_total",
            "cache" => self.name.to_string(),
        )
        .increment(outcome.records_installed);
        tracing::info!(
            cache = %self.name,
            read = outcome.entries_read,
            dropped_unowned = outcome.dropped_unowned,
            dropped_expired = outcome.dropped_expired,
            dropped_bad = outcome.dropped_bad,
            installed = outcome.records_installed,
            refused_present = outcome.refused_present,
            "sundog spill: reopen entry accounting",
        );
        record_reopen_stage(&self.name, "read", outcome.entries_read);
        record_reopen_stage(&self.name, "dropped_unowned", outcome.dropped_unowned);
        record_reopen_stage(&self.name, "dropped_expired", outcome.dropped_expired);
        record_reopen_stage(&self.name, "dropped_bad", outcome.dropped_bad);
        record_reopen_stage(&self.name, "installed", outcome.records_installed);
        record_reopen_stage(&self.name, "refused_present", outcome.refused_present);
        let tier = Arc::new(outcome.tier);
        tier.set_keep_resident_when_refused(matches!(
            self.mode,
            Mode::Replicated | Mode::Distributed { .. }
        ));
        self.engine.set_spill(Arc::clone(&tier));
        tier.attach(Arc::downgrade(&sink));
        self.spill_read
            .set(SpillRead {
                semaphore: Arc::new(tokio::sync::Semaphore::new(cfg.read_concurrency_value())),
                reads_hit: metrics::counter!(
                    "sundog_spill_reads_total",
                    "cache" => self.name.to_string(), "outcome" => "hit",
                ),
                reads_stale: metrics::counter!(
                    "sundog_spill_reads_total",
                    "cache" => self.name.to_string(), "outcome" => "stale",
                ),
                reads_io_error: metrics::counter!(
                    "sundog_spill_reads_total",
                    "cache" => self.name.to_string(), "outcome" => "io_error",
                ),
                promotions: metrics::counter!(
                    "sundog_spill_promotions_total",
                    "cache" => self.name.to_string(),
                ),
                read_duration: metrics::histogram!(
                    "sundog_spill_read_duration_seconds",
                    "cache" => self.name.to_string(),
                ),
            })
            .unwrap_or_else(|_| panic!("invariant: attach_spill runs at most once per shard"));
        Ok(AttachSpillOutcome {
            warm_parts: outcome.warm_parts,
        })
    }

    /// After a RAM miss, checks whether `key` is currently spilled and, if
    /// so, reads its bytes back off disk and promotes it to residency.
    /// Returns `None` for a resident or absent entry, a stale generation
    /// where the region rotated out from under the pointer, a checksum
    /// mismatch, or a postcard decode failure. Each of the latter three
    /// counts `sundog_spill_reads_total{cache,outcome}` accordingly. A hit
    /// counts `outcome = "hit"` and, iff it flips the entry back to
    /// residency, `sundog_spill_promotions_total{cache}`. The promotion
    /// evicts colder entries to make room within `max_capacity` first, and
    /// never deletes one the spill tier refuses, in any mode: a refused
    /// victim stays resident, so a full flush queue leaves the promotion
    /// without room. The promotion does not happen without room: a
    /// tombstone or a newer write racing the disk read, a tier that refuses
    /// every victim, or an entry heavier than the whole cap leaves the
    /// entry on disk, and the read still answers its value (see
    /// [`engine::Engine::promote_locked`]). No lock or permit is ever held
    /// across more than one `.await` point at a time, and no stripe lock is
    /// ever held across the disk read.
    #[cfg(feature = "spill")]
    pub(super) async fn get_spilled(&self, key: &K) -> Option<V> {
        let key_bytes = encode_key(key).ok()?;
        let hash = engine::hash_key_bytes(key_bytes.as_ref());
        self.get_spilled_by_bytes(key_bytes.as_ref(), hash)
            .await
            .map(|(_, value)| value)
    }

    /// [`Shard::get_spilled`] for a caller that already has `key_bytes` and
    /// its hash, such as [`Shard::get_or_load`]'s owner arm, with the
    /// version the value was spilled at.
    #[cfg(feature = "spill")]
    pub(super) async fn get_spilled_by_bytes(
        &self,
        key_bytes: &[u8],
        hash: u64,
    ) -> Option<(Hlc, V)> {
        let (ver, value, encoded) = self.read_spilled_by_bytes(key_bytes, hash).await?;
        if self
            .engine
            .promote_locked(key_bytes, hash, ver, &value, &encoded, self.now_ms())
            && let Some(spill_read) = self.spill_read.get()
        {
            spill_read.promotions.increment(1);
        }
        Some((ver, value))
    }

    /// Reads the key's currently spilled value off disk, without promoting
    /// it: the version it was spilled at, the value, and its encoded bytes.
    /// `None` for a key not spilled here, and for a read that fails or finds
    /// the record rotated out, each counted in the spill read metrics.
    #[cfg(feature = "spill")]
    pub(super) async fn read_spilled_by_bytes(
        &self,
        key_bytes: &[u8],
        hash: u64,
    ) -> Option<(Hlc, V, Bytes)> {
        let spill_read = self.spill_read.get()?;
        let tier = Arc::clone(self.engine.spill()?);
        let (ver, loc) = self.engine.spilled_loc(key_bytes, hash, self.now_ms())?;
        let started = std::time::Instant::now();
        let permit = Arc::clone(&spill_read.semaphore)
            .acquire_owned()
            .await
            .ok()?;
        let read = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            tier.read_at(loc)
        })
        .await;
        let bytes = match read {
            Ok(Ok(Some(bytes))) => bytes,
            Ok(Ok(None)) => {
                spill_read.finish(&spill_read.reads_stale, started);
                return None;
            }
            Ok(Err(err)) => {
                tracing::warn!(
                    cache = %self.name,
                    error = %err,
                    "sundog spill: positional read failed"
                );
                spill_read.finish(&spill_read.reads_io_error, started);
                return None;
            }
            Err(_join_err) => {
                spill_read.finish(&spill_read.reads_io_error, started);
                return None;
            }
        };
        let Ok(value) = postcard::from_bytes::<V>(&bytes.encoded) else {
            spill_read.finish(&spill_read.reads_io_error, started);
            return None;
        };
        spill_read.finish(&spill_read.reads_hit, started);
        Some((ver, value, bytes.encoded))
    }

    /// Closes this shard's attached spill tier, if `Shard::attach_spill`
    /// ever ran: stops accepting new spills and drops the flusher thread's
    /// channel sender, so its loop drains whatever is queued and exits on
    /// its own, writing no checkpoint. The next `attach_spill` here opens
    /// cold even with `SpillConfig::warm_reopen` on; see
    /// [`Shard::close_spill_checkpointed`] for a warm reopen. A no-op
    /// otherwise, and always a no-op in a non-`spill` build.
    #[cfg_attr(
        not(feature = "spill"),
        allow(
            clippy::unused_self,
            reason = "the tier accessor only exists under feature = \"spill\""
        )
    )]
    pub(crate) fn close_spill(&self) {
        #[cfg(feature = "spill")]
        if let Some(tier) = self.engine.spill() {
            tracing::debug!(
                cache = %self.name,
                bytes_used = tier.bytes_used(),
                "sundog spill: closing tier"
            );
            tier.close();
        }
    }

    /// [`Shard::close_spill`], plus, with `SpillConfig::warm_reopen` on, a
    /// checkpoint of every resident entry to disk before closing, each step a
    /// separate blocking task. The disk write is unconditional, racy against
    /// a concurrent `Cache` clone's remove/overwrite;
    /// `Engine::finalize_checkpoint_writes` re-validates under the stripe
    /// lock afterward and keeps only entries still live at the captured
    /// version, so a losing key is dropped, never resurrected. Without
    /// `warm_reopen`, this is [`Shard::close_spill`] alone: no checkpoint.
    ///
    /// A no-op otherwise, and always a no-op in a non-`spill` build. Called
    /// by `Cache::close` and, for a cache still registered at cluster
    /// shutdown, by `Cluster::shutdown`'s sweep.
    ///
    /// Runs once per shard. A call that finds the close under way waits for
    /// it, and one that finds it done returns at once, so a second clone's
    /// close, or one after the sweep, cannot checkpoint a closed tier into
    /// files a reopened cache has since taken over, which could bring back
    /// an entry the reopened cache deleted. A call dropped before it
    /// finishes leaves the close undone, and the next call runs it.
    pub(crate) async fn close_spill_checkpointed(&self) {
        self.spill_close
            .get_or_init(|| self.checkpoint_and_close_spill())
            .await;
    }

    /// The body of [`Shard::close_spill_checkpointed`].
    #[cfg_attr(
        not(feature = "spill"),
        allow(
            clippy::unused_self,
            clippy::unused_async,
            reason = "the tier accessor, and everything this awaits, only exist under \
                      feature = \"spill\""
        )
    )]
    #[allow(
        clippy::too_many_lines,
        reason = "one scripted checkpoint sequence, logging and metering each stage in order"
    )]
    async fn checkpoint_and_close_spill(&self) {
        #[cfg(feature = "spill")]
        if let Some(tier) = self.engine.spill().cloned() {
            tracing::debug!(
                cache = %self.name,
                bytes_used = tier.bytes_used(),
                "sundog spill: closing tier"
            );
            if tier.warm_reopen_value() {
                let now = self.now_ms();
                let engine = Arc::clone(&self.engine);
                let resident =
                    tokio::task::spawn_blocking(move || engine.checkpoint_resident_entries(now))
                        .await
                        .unwrap_or_else(|err| {
                            tracing::warn!(
                                cache = %self.name,
                                error = %err,
                                "sundog spill: resident-entry scan task panicked; the \
                                 checkpoint below covers only already-spilled entries",
                            );
                            Vec::new()
                        });
                tracing::info!(
                    cache = %self.name,
                    count = resident.len(),
                    "sundog spill: checkpoint captured resident entries",
                );
                record_checkpoint_stage(&self.name, "captured", resident.len() as u64);

                let sink = Arc::clone(&self.engine) as Arc<dyn spill::SpillSink>;
                let checkpoint_tier = Arc::clone(&tier);
                let flush_outcome = tokio::task::spawn_blocking(move || {
                    checkpoint_tier.checkpoint_flush(sink.as_ref(), resident)
                })
                .await
                .unwrap_or_else(|err| {
                    tracing::warn!(
                        cache = %self.name,
                        error = %err,
                        "sundog spill: checkpoint flush task panicked; the snapshot below \
                         still covers every already-spilled entry",
                    );
                    spill::CheckpointFlushOutcome::default()
                });
                let newly_written = flush_outcome.written;
                tracing::info!(
                    cache = %self.name,
                    count = newly_written.len(),
                    "sundog spill: checkpoint wrote records",
                );
                record_checkpoint_stage(&self.name, "written", newly_written.len() as u64);
                tracing::info!(
                    cache = %self.name,
                    jobs_drained = flush_outcome.jobs_drained,
                    jobs_abandoned = flush_outcome.jobs_abandoned,
                    "sundog spill: checkpoint drained the flush queue",
                );
                record_checkpoint_stage(&self.name, "drained", flush_outcome.jobs_drained);
                record_checkpoint_stage(&self.name, "abandoned", flush_outcome.jobs_abandoned);

                // Re-validates under the stripe lock: a concurrent write may have
                // changed the key since capture, so a losing entry is dropped here.
                let engine = Arc::clone(&self.engine);
                let (_survivors, finalize_counts) = tokio::task::spawn_blocking(move || {
                    engine.finalize_checkpoint_writes(newly_written, now)
                })
                .await
                .unwrap_or_else(|err| {
                    tracing::warn!(
                        cache = %self.name,
                        error = %err,
                        "sundog spill: checkpoint finalize task panicked; the snapshot below \
                         drops every entry this checkpoint just wrote",
                    );
                    (Vec::new(), engine::CheckpointFinalizeCounts::default())
                });
                tracing::info!(
                    cache = %self.name,
                    kept = finalize_counts.kept,
                    dropped_version_changed = finalize_counts.dropped_version_changed,
                    dropped_tombstoned = finalize_counts.dropped_tombstoned,
                    dropped_gone = finalize_counts.dropped_gone,
                    dropped_expired = finalize_counts.dropped_expired,
                    "sundog spill: checkpoint finalized writes",
                );
                record_checkpoint_stage(&self.name, "kept", finalize_counts.kept);
                record_checkpoint_stage(
                    &self.name,
                    "dropped_version_changed",
                    finalize_counts.dropped_version_changed,
                );
                record_checkpoint_stage(
                    &self.name,
                    "dropped_tombstoned",
                    finalize_counts.dropped_tombstoned,
                );
                record_checkpoint_stage(&self.name, "dropped_gone", finalize_counts.dropped_gone);
                record_checkpoint_stage(
                    &self.name,
                    "dropped_expired",
                    finalize_counts.dropped_expired,
                );

                let engine = Arc::clone(&self.engine);
                let entries = tokio::task::spawn_blocking(move || engine.snapshot_spilled(now))
                    .await
                    .unwrap_or_else(|err| {
                        tracing::warn!(
                            cache = %self.name,
                            error = %err,
                            "sundog spill: spilled-pointer scan task panicked; the \
                             checkpoint below covers only entries this checkpoint just wrote",
                        );
                        Vec::new()
                    });
                tracing::info!(
                    cache = %self.name,
                    count = entries.len(),
                    "sundog spill: checkpoint listed already-spilled entries",
                );
                record_checkpoint_stage(&self.name, "listed", entries.len() as u64);

                // `entries` already includes every survivor:
                // `finalize_checkpoint_writes` flipped each to `Spilled` before this
                // scan ran, so extending again would double-count.
                tracing::info!(
                    cache = %self.name,
                    count = entries.len(),
                    "sundog spill: checkpoint snapshot total",
                );
                record_checkpoint_stage(&self.name, "snapshot", entries.len() as u64);

                let snapshot_tier = Arc::clone(&tier);
                let closed = tokio::task::spawn_blocking(move || {
                    snapshot_tier.write_checkpoint_snapshot(&entries, now);
                    snapshot_tier.close();
                })
                .await;
                if let Err(err) = closed {
                    tracing::warn!(
                        cache = %self.name,
                        error = %err,
                        "sundog spill: checkpoint snapshot task panicked",
                    );
                }
            } else {
                tier.close();
            }
        }
    }

    /// Test-only: takes every byte of the spill tier's flush-queue
    /// admission, waiting up to five seconds for the flusher to hand back
    /// what in-flight jobs hold, so every eviction hand-off is refused for a
    /// full queue until the returned [`spill::Reservation`] drops. Nothing
    /// is queued and nothing is written, so no entry is affected and no drop
    /// is counted.
    ///
    /// # Panics
    ///
    /// Panics without a spill tier, or if the flusher does not drain in
    /// time.
    #[cfg(all(feature = "spill", test, not(feature = "sim")))]
    pub(crate) async fn debug_hold_flush_queue(&self) -> spill::Reservation<'_> {
        let tier = self.engine.spill().expect("a spill tier is attached");
        tier.reserve(tier.whole_queue_permits(), Duration::from_secs(5))
            .await
            .expect("the flusher drains its queue")
    }

    /// Whether this shard's attached spill tier is closed by
    /// Whether this shard's spill tier is closed (by either close path) or
    /// none is attached. Test-only: lets a test check closure directly.
    #[cfg(all(feature = "spill", test, not(feature = "sim")))]
    pub(crate) fn spill_tier_closed(&self) -> bool {
        self.engine.spill().is_none_or(|tier| tier.is_closed())
    }
}

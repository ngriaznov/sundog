//! The read path of a [`Shard`]: point reads, key listings, read-through fills and the
//! residency views a read decides under.

use std::hash::Hash;
use std::sync::Arc;

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::engine::JoinOutcome;
use super::fan_out::fill_fans_out;
use super::{
    Event, FanOutItem, Origin, PartId, Shard, SharedLoaderFailure, encode_key, engine, read_timing,
};
use crate::error::CacheError;
use crate::explain::{LocalRecord, Residency};
use crate::hlc::Hlc;
use crate::ownership::OwnershipView;

// Every method here is fully synchronous, with no `.await`, except
// `Shard::get`/`Shard::get_or_load`'s spilled-key path under `feature =
// "spill"`: a disk read runs behind `spawn_blocking` and a semaphore
// permit there, the one place this backend awaits. `async fn` stays the
// signature everywhere in this block regardless, so callers and a later
// backend swap need no signature change.
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
    /// Ends a loader run that returned `value` for `key`, the load stamped
    /// `ver` before it started, through [`Shard::store_loaded`], and counts
    /// the run as a miss.
    pub(super) fn finish_fill(
        &self,
        key: &K,
        key_bytes: &Bytes,
        hash: u64,
        ver: Hlc,
        inflight: &engine::Inflight<V>,
        value: V,
    ) {
        self.store_loaded(key, key_bytes, hash, ver, inflight, value);
        self.misses.increment(1);
    }

    /// Stores a loaded `value` for `key` through
    /// [`engine::Engine::complete_fresh_load`], the load stamped `ver`
    /// before it started, or hands it to joined waiters uncached at the
    /// ceiling. A stored value fans out per [`fill_fans_out`] and emits
    /// [`Event::Created`], or [`Event::Updated`] over a live entry; one a
    /// write, removal or invalidation overtook stores and sends nothing.
    /// Returns whether the value was stored.
    pub(super) fn store_loaded(
        &self,
        key: &K,
        key_bytes: &Bytes,
        hash: u64,
        ver: Hlc,
        inflight: &engine::Inflight<V>,
        value: V,
    ) -> bool {
        if self.at_ceiling().is_some() {
            self.engine
                .finish_uncached(key_bytes, hash, inflight, value);
            if let Some(refusals) = &self.ceiling_refusals {
                refusals.fills.increment(1);
            }
            return false;
        }
        let encoded = Bytes::from(
            postcard::to_stdvec(&value)
                .expect("invariant: a value returned by the loader postcard-encodes"),
        );
        let expires_at_ms = self.expiry_for(ver.wall_ms, None);
        let outcome = self.engine.complete_fresh_load(
            engine::FreshLoad {
                key,
                key_bytes,
                hash,
                ver,
                value: value.clone(),
                encoded,
                expires_at_ms,
            },
            self.now_ms(),
            inflight,
        );
        let engine::FillOutcome::Installed { had_live } = outcome else {
            return false;
        };
        // A refusal means the cache is closing; the loaded value still
        // answers the read.
        if fill_fans_out(self.mode) {
            let _ = self.fan_out.push(FanOutItem::Applied(key.clone(), ver));
        }
        if self.events.receiver_count() > 0 {
            let key = key.clone();
            let origin = Origin::Local;
            let _ = self.events.send(if had_live {
                Event::Updated { key, value, origin }
            } else {
                Event::Created { key, value, origin }
            });
        }
        true
    }

    /// This shard's residency marks for `part`, with ownership taken from
    /// `view`, the view a read decides under. A shard with no residency
    /// set, one not in `Mode::Distributed`, reports ownership alone.
    pub(crate) fn residency_of(&self, view: &OwnershipView, part: PartId) -> Residency {
        let owns = view.owns(part);
        self.residency
            .as_ref()
            .map_or(Residency::new(owns), |residency| {
                residency.residency_of(part, owns, std::time::Instant::now())
            })
    }

    /// What this shard stores for `key_bytes`, judged at this shard's clock,
    /// with that clock reading: no recency touch, no disk read, no
    /// promotion, no load and no metric. Not gated on ownership.
    pub(crate) fn local_record(&self, key_bytes: &[u8]) -> (u64, LocalRecord) {
        let at_ms = self.now_ms();
        (at_ms, self.engine.inspect(key_bytes, at_ms))
    }

    /// [`Shard::residency_of`] under this shard's current view. A shard not
    /// in `Mode::Distributed` owns every part.
    pub(crate) fn current_residency(&self, part: PartId) -> Residency {
        self.ownership
            .as_ref()
            .map_or(Residency::new(true), |tracker| {
                self.residency_of(&tracker.current(), part)
            })
    }

    /// Reads `key`, without triggering read-through. A deleted key is never
    /// present, since a tombstone leaves no live entry to find.
    ///
    /// Counts `sundog_cache_hits_total{cache}` on `Some`,
    /// `sundog_cache_misses_total{cache}` on `None`. On a `feature = "spill"`
    /// build, a RAM miss that turns out to be a currently-spilled entry is
    /// read back off disk behind the tier's read semaphore, via
    /// `spawn_blocking`, and promoted back to residency under a fresh
    /// stripe write lock when room within `max_capacity` can be made;
    /// either way it still counts as a hit. See
    /// the store module's docs and [`Shard::get_sync`], which never does
    /// this. About one read in 256 on each thread is timed into
    /// `sundog_read_duration_seconds{cache,outcome}`, at a stride drawn
    /// uniformly from 128 to 384 reads.
    pub async fn get(&self, key: &K) -> Option<V> {
        let started = read_timing::start_read();
        if let Some(value) = self.read_resident_key(key, self.now_ms()) {
            self.hits.increment(1);
            self.read_durations.record(started, true);
            return Some(value);
        }
        #[cfg(feature = "spill")]
        if let Some(value) = self.get_spilled(key).await {
            self.hits.increment(1);
            self.read_durations.record(started, true);
            return Some(value);
        }
        self.misses.increment(1);
        self.read_durations.record(started, false);
        None
    }

    /// [`Shard::get`] without an async runtime: same hit and miss counting,
    /// for every entry except a currently-spilled one. On `feature =
    /// "spill"`, a spilled entry reads as a miss here. This is the
    /// documented behavioral difference between the sync and async twins:
    /// the RAM-only synchronous path never touches disk, so it cannot read a
    /// value that has moved there. Use [`Shard::get`] to read a spilled
    /// value; it also promotes the entry back to residency when there is
    /// room. Timed
    /// into `sundog_read_duration_seconds` on the same per-thread sample.
    #[must_use]
    pub fn get_sync(&self, key: &K) -> Option<V> {
        let started = read_timing::start_read();
        if let Some(value) = self.read_resident_key(key, self.now_ms()) {
            self.hits.increment(1);
            self.read_durations.record(started, true);
            Some(value)
        } else {
            self.misses.increment(1);
            self.read_durations.record(started, false);
            None
        }
    }

    /// Reads whether `key` has a live entry, honoring expiry, without cloning
    /// the stored value. This asks about the entry as written, not against
    /// some other deadline; no read method here takes a TTL argument. An
    /// existence check, not a read: it moves neither
    /// `sundog_cache_hits_total` nor `sundog_cache_misses_total`, though it
    /// restarts the entry's `tti` idle clock as a read does. A
    /// currently-spilled entry counts as present with zero disk reads:
    /// existence doesn't need the value bytes.
    pub async fn contains_key(&self, key: &K) -> bool {
        self.contains_key_sync(key)
    }

    /// [`Shard::contains_key`] without an async runtime. Answers `true` for
    /// a currently-spilled entry as [`Shard::contains_key`] does, with zero
    /// disk reads: existence doesn't need the value bytes.
    #[must_use]
    pub fn contains_key_sync(&self, key: &K) -> bool {
        self.engine.contains_key(key, self.now_ms())
    }

    /// The number of live entries this node currently holds. Runs the engine's
    /// sweep first, so completed TTL/TTI expiries are reflected rather than
    /// estimated. In a `Replicated` or `Distributed` shard whose resolver
    /// does not merge, that sweep turns an entry that expired into a
    /// tombstone at its version, so an older copy of the key cannot come back
    /// after it; an entry that went idle, and every sweep in another mode or
    /// under a merging resolver, leaves nothing. Sampled periodically by
    /// `cluster::cache_entries_gauge_task` to publish
    /// `sundog_cache_entries{cache}`.
    pub async fn entry_count(&self) -> u64 {
        let now = self.now_ms();
        self.engine.sweep(now, self.expiry_tombstones());
        self.engine.live_entry_count()
    }

    /// The number of tombstones this node holds: the deletes it keeps for
    /// `tombstone_ttl`, and in a `Replicated` or `Distributed` cache with a
    /// resolver that picks a side, the expired entries its sweep turned into
    /// tombstones. Sampled periodically by `cluster::cache_entries_gauge_task`
    /// to publish `sundog_tombstones{cache}`. A full pass over the stripes.
    #[must_use]
    pub(crate) fn tombstone_count(&self) -> u64 {
        self.engine.tombstone_count()
    }

    /// A weakly consistent, point-in-time snapshot of this node's local live
    /// keys. Not a cluster view, and gives no guarantee about a key
    /// inserted concurrently with the scan. Cost is O(entries).
    #[must_use]
    pub fn keys(&self) -> Vec<K> {
        self.engine.keys(self.now_ms())
    }

    /// [`Shard::keys`] as a visitor: `f` runs once per local live key, never
    /// under a stripe lock, and no `Vec` of every key is built.
    pub fn for_each_key(&self, f: impl FnMut(K)) {
        self.engine.for_each_key(self.now_ms(), f);
    }

    /// Reads `key`, invoking `loader` on a miss. Concurrent callers racing on
    /// the same missing key collapse into one `loader` call.
    ///
    /// Counts `sundog_cache_misses_total{cache}` exactly once per `loader`
    /// execution, from the fill's owner. Every other call, a cache hit or a
    /// collapsed caller, counts `sundog_cache_hits_total{cache}` instead.
    ///
    /// On a `feature = "spill"` build, the collapse extends to a spilled
    /// key's disk read too: only the call that wins ownership of the
    /// missing key checks whether it is currently spilled and, if so, reads
    /// it back before ever calling `loader`. Every other concurrent caller
    /// joins and waits as it already does for an in-flight `loader` run,
    /// so a burst of concurrent misses on one spilled key reads it off
    /// disk once.
    ///
    /// A fill that finishes with the shard at the ceiling set by
    /// [`Shard::with_max_resident_bytes`] returns its value to the caller
    /// and every joined waiter without storing it, and counts
    /// `sundog_ceiling_refusals_total{cache, kind="fill"}`.
    ///
    /// An invalidation for `key` that lands while `loader` runs, from a peer
    /// or [`Shard::invalidate_local`], means the loader may have read the
    /// source before the change it announces: the fill returns its value to
    /// the caller and every joined waiter without storing it, and the next
    /// read loads afresh.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::Loader`] if `loader` fails.
    ///
    /// # Panics
    ///
    /// Panics if a value the loader returned fails to postcard-encode, since
    /// `Shard::insert` relies on the same bound to encode any `V`.
    pub async fn get_or_load<F, E>(&self, key: &K, loader: F) -> Result<V, CacheError>
    where
        F: AsyncFnOnce(&K) -> Result<V, E>,
        E: std::error::Error + Send + Sync + 'static,
    {
        let key_bytes = encode_key(key)?;
        let hash = engine::hash_key_bytes(key_bytes.as_ref());
        loop {
            if let Some(value) = self
                .engine
                .get_by_bytes(key_bytes.as_ref(), hash, self.now_ms())
            {
                self.hits.increment(1);
                return Ok(value);
            }
            match self.engine.miss_or_join(&key_bytes, hash, self.now_ms()) {
                JoinOutcome::Hit(value) => {
                    self.hits.increment(1);
                    return Ok(value);
                }
                JoinOutcome::Join(inflight, mut done) => {
                    // `Err` means the owner's `Inflight` dropped without
                    // finishing; the next iteration takes
                    // over the load.
                    let _ = done.changed().await;
                    if let Some(err) = inflight.error.get() {
                        return Err(CacheError::Loader(Box::new(SharedLoaderFailure(
                            Arc::clone(err),
                        ))));
                    }
                    if let Some(value) = inflight.value.get() {
                        self.hits.increment(1);
                        return Ok(value.clone());
                    }
                    // The fast-path read above picks up the fresh value, or its
                    // absence.
                }
                JoinOutcome::Owner(inflight) => {
                    let guard =
                        self.engine
                            .guard_inflight(key_bytes.clone(), hash, Arc::clone(&inflight));
                    // The sole owner of this key's fill checks the disk
                    // before ever calling `loader`: a burst of concurrent
                    // misses collapses to at most one spilled-key read, the
                    // same way it already collapses to at most one `loader`
                    // call. Ending the in-flight entry hands the value to
                    // every joined waiter, promoted back to RAM or not, iff
                    // the key still holds it; a removal, expiry or newer
                    // write since the read leaves them to read again.
                    #[cfg(feature = "spill")]
                    if let Some((ver, value)) =
                        self.get_spilled_by_bytes(key_bytes.as_ref(), hash).await
                    {
                        self.hits.increment(1);
                        self.engine.finish_spilled_read(
                            &key_bytes,
                            hash,
                            &inflight,
                            ver,
                            value.clone(),
                            self.now_ms(),
                        );
                        guard.complete();
                        return Ok(value);
                    }
                    // Stamped before the loader runs, so a write or removal
                    // that lands during the load is newer than the fill and
                    // keeps its place.
                    let ver = self.stamp_local();
                    return match loader(key).await {
                        Ok(value) => {
                            self.finish_fill(key, &key_bytes, hash, ver, &inflight, value.clone());
                            guard.complete();
                            Ok(value)
                        }
                        Err(err) => {
                            let err: Arc<dyn std::error::Error + Send + Sync> = Arc::new(err);
                            self.engine.fail_inflight(
                                &key_bytes,
                                hash,
                                &inflight,
                                Arc::clone(&err),
                            );
                            guard.complete();
                            Err(CacheError::Loader(Box::new(SharedLoaderFailure(err))))
                        }
                    };
                }
            }
        }
    }

    /// [`Shard::get_or_load`] for a loader that never fails: same stampede
    /// collapse on concurrent misses, same fan-out of the fill. The
    /// `Result` remains only for [`CacheError::Codec`]; `make` itself
    /// cannot fail.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::Codec`] if `key` fails to postcard-encode.
    pub async fn get_or_insert_with<F>(&self, key: &K, make: F) -> Result<V, CacheError>
    where
        F: AsyncFnOnce(&K) -> V,
    {
        self.get_or_load(key, async move |k| {
            Ok::<V, std::convert::Infallible>(make(k).await)
        })
        .await
    }
}

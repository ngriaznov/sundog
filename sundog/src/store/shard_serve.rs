//! The serving side of a [`Shard`]: the inbound-apply guard, the residency views outbound
//! serving decides under, and the [`ShardOps`] implementation the cluster drives.

use std::collections::{BTreeMap, HashSet};
use std::hash::Hash;
use std::sync::{Arc, Mutex as StdMutex, PoisonError};

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::{self, BoxStream, StreamExt};
use serde::Serialize;
use serde::de::DeserializeOwned;

#[cfg(feature = "spill")]
use super::SpillRead;
use super::shard_ops::MaskedDigest;
use super::{
    BUCKET_COUNT, BucketDigest, BucketEntries, BucketLen, BucketPart, BucketPartDigests,
    CompactPassOutcome, CompactionBounds, Event, FanOutItem, Incoming, KeyVersion, Mode, Origin,
    PartEntries, PartId, PartMask, Quiescence, Scope, ScopedDigests, Shard, ShardOps,
    chunk_records_for_snapshot, compacted_version, crdt, encode_key, engine,
};
use crate::hlc::Hlc;
use crate::node::NodeId;
use crate::ownership::{Granularity, OwnershipTracker, OwnershipView, ResidencySet};
use crate::verdict;
use crate::wire::WireRecord;

/// One inbound [`WireRecord`], decoded down to what
/// [`ShardOps::apply_remote_batch`]'s per-stripe grouping needs: its
/// precomputed hash, typed key, raw key bytes, version, and the value or
/// tombstone it carries, plus which peer (or local call) it came from.
type RemoteEntry<K, V> = (u64, K, Bytes, Hlc, Incoming<V>, Origin);

/// Which inbound records a shard in `mode` stores. An `Invalidation` cache
/// holds only what it read or wrote itself, so a peer's merged record folds
/// into a key it holds and is ignored for any other; every other mode stores
/// what the resolver picks. Pure; unit tested directly.
pub(super) const fn inbound_scope(mode: Mode) -> engine::ApplyScope {
    match mode {
        Mode::Invalidation => engine::ApplyScope::HeldOnly,
        Mode::Local | Mode::Replicated | Mode::Distributed { .. } => engine::ApplyScope::Any,
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
    /// The inbound-apply guard, applied to a batch before decoding: keeps
    /// the records this node's current view owns, forwards the ones for a
    /// bucket mid disown-grace on to its current owners, and drops the rest.
    fn guard_inbound(&self, recs: Vec<WireRecord>) -> Vec<WireRecord> {
        match self.ownership.as_ref().map(OwnershipTracker::current) {
            Some(view) => {
                let (owned, unowned): (Vec<_>, Vec<_>) = recs
                    .into_iter()
                    .partition(|rec| view.owns(PartId::of_key(rec.key.as_ref())));
                let (redirected, dropped): (Vec<_>, Vec<_>) =
                    unowned.into_iter().partition(|rec| {
                        self.residency
                            .as_ref()
                            .is_some_and(|r| r.is_releasing(PartId::of_key(rec.key.as_ref())))
                    });
                if !redirected.is_empty() {
                    metrics::counter!(
                        "sundog_forwarded_writes_total",
                        "cache" => self.name.to_string()
                    )
                    .increment(u64::try_from(redirected.len()).unwrap_or(u64::MAX));
                    tracing::debug!(
                        cache = %self.name,
                        redirected = redirected.len(),
                        "apply_remote_batch: forwarded records for a releasing bucket on \
                         to its current owners"
                    );
                    // A refusal means the cache is closing: nothing
                    // forwards from a node on its way out.
                    let _ = self
                        .fan_out
                        .extend(redirected.into_iter().map(FanOutItem::Forward));
                }
                if !dropped.is_empty() {
                    metrics::counter!(
                        "sundog_unowned_inbound_dropped_total",
                        "cache" => self.name.to_string()
                    )
                    .increment(u64::try_from(dropped.len()).unwrap_or(u64::MAX));
                    tracing::debug!(
                        cache = %self.name,
                        dropped = dropped.len(),
                        "apply_remote_batch: dropped records for a bucket this node does \
                         not own"
                    );
                }
                owned
            }
            None => recs,
        }
    }

    /// This shard's current ownership view and residency set together, for
    /// the outbound-serving guard and `ae_peer_filter`: `None` for every
    /// mode but `Mode::Distributed`. Both are cheap `Arc` clones, so a
    /// caller in an async block that must own its captures (`snapshot_chunks`)
    /// and one that only borrows for the call's duration use the same
    /// accessor.
    fn residency_check(&self) -> Option<(Arc<OwnershipView>, Arc<ResidencySet>)> {
        match (&self.ownership, &self.residency) {
            (Some(tracker), Some(residency)) => Some((tracker.current(), Arc::clone(residency))),
            _ => None,
        }
    }

    /// Whether `part` is currently resident for outbound anti-entropy or
    /// snapshot serving: owned, or mid disown-grace. `is_resident = owns ||
    /// residency.is_releasing`, used only by the outbound-serving guard and
    /// the donor-serving exception, never by the inbound-apply guard,
    /// which stays strict current-view ownership.
    pub(super) fn is_resident_part(
        residency: &(Arc<OwnershipView>, Arc<ResidencySet>),
        part: PartId,
    ) -> bool {
        let (view, res) = residency;
        verdict::resident(view.owns(part), || res.is_releasing(part))
    }

    /// Whether any part of `bucket` is resident; see
    /// [`Shard::is_resident_part`].
    fn is_resident_bucket(
        residency: &(Arc<OwnershipView>, Arc<ResidencySet>),
        bucket: u16,
    ) -> bool {
        let (view, res) = residency;
        view.owns_any_in_bucket(bucket) || res.releasing_in_bucket(bucket)
    }

    /// [`Shard::is_resident_bucket`] for every bucket at once, indexed by
    /// bucket: one pass over the owned and releasing parts instead of one
    /// lookup per part.
    fn resident_bucket_mask(residency: &(Arc<OwnershipView>, Arc<ResidencySet>)) -> Vec<bool> {
        let (view, res) = residency;
        let mut mask = view.owned_bucket_mask().to_vec();
        for part in res.releasing_parts() {
            mask[usize::from(part.bucket())] = true;
        }
        mask
    }

    /// The parts one anti-entropy round with `peer` covers: every part
    /// both self and `peer` own under the current view, plus every part
    /// mid disown-grace here that `peer` owns, the backstop for a lost
    /// rebalance pull. Ascending, each part once.
    fn peer_scope(
        residency: &(Arc<OwnershipView>, Arc<ResidencySet>),
        peer: NodeId,
    ) -> Vec<PartId> {
        let (view, releasing) = residency;
        let mut scope = crate::ownership::shared_owned_parts(view, peer);
        scope.extend(
            releasing
                .releasing_parts()
                .into_iter()
                .filter(|&part| view.owners_of(part).contains(&peer)),
        );
        scope.sort_unstable();
        scope.dedup();
        scope
    }

    /// The bucket digest of every bucket [`Shard::peer_scope`] touches.
    fn bucket_digests_for(
        &self,
        residency: &(Arc<OwnershipView>, Arc<ResidencySet>),
        peer: NodeId,
    ) -> Vec<BucketDigest> {
        let scope: HashSet<u16> = Self::peer_scope(residency, peer)
            .into_iter()
            .map(PartId::bucket)
            .collect();
        self.engine
            .digests()
            .into_iter()
            .filter(|(bucket, _)| scope.contains(bucket))
            .map(|(bucket, digest)| BucketDigest { bucket, digest })
            .collect()
    }

    /// `parts` gathered by bucket, ascending, each bucket's parts as a mask
    /// and their digests folded: one engine read per bucket.
    fn masked_digests(&self, parts: &[PartId]) -> Vec<MaskedDigest> {
        let mut masks: BTreeMap<u16, PartMask> = BTreeMap::new();
        for part in parts {
            masks.entry(part.bucket()).or_default().insert(part.part());
        }
        masks
            .into_iter()
            .map(|(bucket, mask)| MaskedDigest {
                bucket,
                mask,
                digest: mask.fold(&self.engine.part_digests(bucket)),
            })
            .collect()
    }

    /// Dedups and sorts a bulk request list, ascending.
    fn dedup_sorted<T: Ord>(mut items: Vec<T>) -> Vec<T> {
        items.sort_unstable();
        items.dedup();
        items
    }

    /// Runs `compute` for a resident `bucket`, else substitutes `absent`
    /// unrun. `mask` is [`Shard::resident_bucket_mask`]'s, `None` for a
    /// shard with no residency guard.
    fn gated<R>(
        mask: Option<&[bool]>,
        bucket: u16,
        compute: impl FnOnce() -> R,
        absent: R,
    ) -> (u16, R) {
        let resident =
            mask.is_none_or(|mask| mask.get(usize::from(bucket)).copied().unwrap_or(false));
        let value = if resident { compute() } else { absent };
        (bucket, value)
    }

    /// Dedups and sorts `buckets`, then [`Self::gated`]-maps each one; the shared shape of `bucket_lens` and `part_digests`.
    fn gated_map<R: Clone>(
        &self,
        buckets: Vec<u16>,
        compute: impl Fn(u16) -> R,
        absent: R,
    ) -> Vec<(u16, R)> {
        let mask = self
            .residency_check()
            .map(|residency| Self::resident_bucket_mask(&residency));
        Self::dedup_sorted(buckets)
            .into_iter()
            .map(|b| Self::gated(mask.as_deref(), b, || compute(b), absent.clone()))
            .collect()
    }

    /// The residency-gated backfill shared by `entries_for_buckets` and `entries_for_parts`.
    fn resident_backfill<T: Ord + Copy>(
        &self,
        wanted: Vec<T>,
        is_resident: impl Fn(&(Arc<OwnershipView>, Arc<ResidencySet>), T) -> bool,
        query: impl FnOnce(Vec<T>) -> Vec<(T, Vec<KeyVersion>)>,
    ) -> Vec<(T, Vec<KeyVersion>)> {
        let wanted = Self::dedup_sorted(wanted);
        match self.residency_check() {
            Some(residency) => {
                let (resident, unresident): (Vec<T>, Vec<T>) = wanted
                    .into_iter()
                    .partition(|&t| is_resident(&residency, t));
                let mut entries = query(resident);
                entries.extend(unresident.into_iter().map(|t| (t, Vec::new())));
                entries.sort_unstable_by_key(|&(t, _)| t);
                entries
            }
            None => query(wanted),
        }
    }

    /// [`ShardOps::records_for`] for callers that already hold typed `K`s, such
    /// as `cluster::fan_out_batch` re-fetching for keys read off its own
    /// `Event<K, V>`s. Encodes each key straight to the bytes
    /// `engine::Engine::record_for` looks entries up by, with no
    /// decode step on either end.
    pub(crate) async fn records_for_typed(&self, keys: &[K]) -> Vec<WireRecord> {
        let now = self.now_ms();
        keys.iter()
            .filter_map(|key| {
                let key_bytes = encode_key(key).ok()?;
                self.engine.record_for(key_bytes.as_ref(), now)
            })
            .collect()
    }
}

/// Reads a batch of currently-spilled pointers off-lock, behind a single
/// semaphore permit for the whole batch rather than one per key.
/// [`ShardOps::records_for`], the AE-pull-reply path, and
/// [`ShardOps::snapshot_chunks`] both fold spilled entries in this way. A
/// pointer read that fails, whether from a stale generation, a checksum
/// mismatch, or a genuine I/O error, is dropped like an absent key;
/// nothing here promotes. Returns `Vec::new()` with no read attempted if
/// this shard never attached a tier.
#[cfg(feature = "spill")]
pub(super) async fn read_spilled_batch<K, V>(
    engine: &engine::Engine<K, V>,
    spill_read: Option<&SpillRead>,
    cache_name: &str,
    spilled: Vec<engine::SpilledPointer>,
) -> Vec<WireRecord>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let Some(spill_read) = spill_read else {
        return Vec::new();
    };
    let Some(tier) = engine.spill() else {
        return Vec::new();
    };
    let tier = Arc::clone(tier);
    let Ok(permit) = Arc::clone(&spill_read.semaphore).acquire_owned().await else {
        return Vec::new();
    };
    let spill_read = spill_read.clone();
    let cache_name = cache_name.to_string();
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let mut out = Vec::with_capacity(spilled.len());
        for (key_bytes, ver, expires_at_ms, loc) in spilled {
            let started = std::time::Instant::now();
            match tier.read_at(loc) {
                Ok(Some(bytes)) => {
                    spill_read.finish(&spill_read.reads_hit, started);
                    out.push(WireRecord {
                        key: key_bytes,
                        value: Some(bytes.encoded),
                        ver,
                        expires_at_ms,
                    });
                }
                Ok(None) => {
                    spill_read.finish(&spill_read.reads_stale, started);
                }
                Err(err) => {
                    tracing::warn!(
                        cache = %cache_name,
                        error = %err,
                        "sundog spill: anti-entropy/snapshot read failed"
                    );
                    spill_read.finish(&spill_read.reads_io_error, started);
                }
            }
        }
        out
    })
    .await
    .unwrap_or_default()
}

impl<K, V> ShardOps for Shard<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    fn apply_remote(&self, rec: WireRecord) -> BoxFuture<'_, ()> {
        Box::pin(async move { self.apply_remote_batch(vec![rec]).await })
    }

    fn apply_remote_batch(&self, recs: Vec<WireRecord>) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            // The inbound-apply guard: a `Mode::Distributed` shard never
            // applies a record for a bucket its own *current* view does not
            // own: strict current-view ownership, before decoding, is the
            // single choke point every inbound record path (live
            // replication, an anti-entropy pull reply, a rebalance transfer
            // chunk) funnels through. A record for a bucket this node is
            // mid disown-grace on came from a sender whose view still lists
            // this node as an owner; this node's view is the newer one, so
            // the record is forwarded on to the bucket's current owners,
            // never applied here and never dropped. A record for a bucket
            // this node never held is dropped and counted.
            let recs = self.guard_inbound(recs);
            // An `Invalidation` cache takes a tombstone record as the
            // invalidation it stands for: the copy goes, and a load in
            // flight for the key stores nothing.
            let scope = inbound_scope(self.mode);
            let (recs, removals) = match scope {
                engine::ApplyScope::HeldOnly => {
                    recs.into_iter().partition(|rec| !rec.is_tombstone())
                }
                engine::ApplyScope::Any => (recs, Vec::new()),
            };
            for rec in removals {
                self.invalidate(rec.key, rec.ver).await;
            }
            let mut decoded: Vec<RemoteEntry<K, V>> = Vec::with_capacity(recs.len());
            for rec in recs {
                if !self.observe_remote(rec.ver) {
                    continue;
                }
                let hash = engine::hash_key_bytes(rec.key.as_ref());
                let Ok(key) = postcard::from_bytes::<K>(&rec.key) else {
                    tracing::warn!(cache = %self.name, "apply_remote_batch: undecodable key bytes");
                    continue;
                };
                let origin = Origin::Remote(rec.ver.node);
                let incoming = match rec.value {
                    Some(value_bytes) => {
                        let Ok(value) = postcard::from_bytes::<V>(&value_bytes) else {
                            tracing::warn!(
                                cache = %self.name,
                                "apply_remote_batch: undecodable value bytes"
                            );
                            continue;
                        };
                        Incoming::Put {
                            value,
                            expires_at_ms: rec.expires_at_ms,
                            // The verbatim wire bytes, decoded above only for the resolver.
                            encoded: value_bytes,
                        }
                    }
                    None => Incoming::Tombstone,
                };
                decoded.push((hash, key, rec.key, rec.ver, incoming, origin));
            }

            // One reservation for the whole decoded batch (see
            // `Shard::reserve_for_batch`) throttles replication, anti-entropy
            // repair, and state-transfer apply alike through this one method.
            // A held-only batch stores a record only as a fold into a key
            // already held, and most of its records store nothing, so it
            // reserves nothing and takes the apply's non-blocking path.
            let (reservation, deadline) = match scope {
                engine::ApplyScope::HeldOnly => (None, None),
                engine::ApplyScope::Any => {
                    self.reserve_for_batch(
                        decoded
                            .iter()
                            .map(|(_, _, key_bytes, _, incoming, _)| (key_bytes, incoming)),
                    )
                    .await
                }
            };

            // Groups by raw key-byte stripe, no decode needed; order within
            // a stripe is preserved.
            let mut by_stripe: Vec<Vec<RemoteEntry<K, V>>> =
                (0..BUCKET_COUNT).map(|_| Vec::new()).collect();
            for entry in decoded {
                by_stripe[engine::stripe_index_from_hash(entry.0)].push(entry);
            }
            let now = self.now_ms();
            let mut written = engine::Written::default();
            // Scoped as in `Shard::apply_grouped`: the reservation is back
            // with the tier before the retry asks for a fresh one.
            let deficit = {
                let mut reservation = reservation;
                let mut deficit = 0u64;
                for (bucket, group) in by_stripe.into_iter().enumerate() {
                    if group.is_empty() {
                        continue;
                    }
                    let mut origins = Vec::with_capacity(group.len());
                    let entries: Vec<_> = group
                        .into_iter()
                        .map(|(hash, key, key_bytes, ver, incoming, origin)| {
                            origins.push(origin);
                            (hash, key, key_bytes, ver, incoming)
                        })
                        .collect();
                    let (outcomes, bucket_deficit) = self.engine.apply_many_in(
                        scope,
                        bucket,
                        entries,
                        self.resolver.as_ref(),
                        self.tombstone_ttl_ms,
                        self.tombstone_max_ttl_ms,
                        now,
                        reservation.as_mut(),
                        &mut written,
                    );
                    deficit += bucket_deficit;
                    for (outcome, origin) in outcomes.into_iter().zip(origins) {
                        self.handle_apply_outcome(outcome, origin, true);
                    }
                }
                deficit
            };
            self.retry_reservation_deficit(0, deficit, deadline, &mut written)
                .await;
        })
    }

    fn invalidate(&self, key: Bytes, ver: Hlc) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            if !self.observe_remote(ver) {
                return;
            }
            let Ok(decoded_key) = postcard::from_bytes::<K>(&key) else {
                tracing::warn!(cache = %self.name, "invalidate: undecodable key bytes");
                return;
            };
            let hash = engine::hash_key_bytes(key.as_ref());
            if self.engine.invalidate(key.as_ref(), hash, ver).is_some() {
                let _ = self.events.send(Event::Removed {
                    key: decoded_key,
                    origin: Origin::Remote(ver.node),
                });
            }
        })
    }

    #[allow(private_interfaces, reason = "see ShardOps::serve_load")]
    fn serve_load(&self, keys: Vec<Bytes>) -> BoxFuture<'_, crate::net::LoadServe> {
        Box::pin(self.serve_load_encoded(keys))
    }

    fn refresh_hint(&self, key: Bytes, ver: Hlc) -> BoxFuture<'_, ()> {
        self.take_refresh_hint(&key, ver);
        Box::pin(async {})
    }

    fn digests(&self) -> BoxFuture<'_, Vec<BucketDigest>> {
        let digests = self.engine.digests();
        // The outbound-serving guard: a `Mode::Distributed` shard never
        // reports a bucket it neither owns nor is releasing.
        let digests = match self.residency_check() {
            Some(residency) => {
                let mask = Self::resident_bucket_mask(&residency);
                digests
                    .into_iter()
                    .filter(|&(bucket, _)| mask[usize::from(bucket) % BUCKET_COUNT])
                    .collect()
            }
            None => digests,
        };
        let digests = digests
            .into_iter()
            .map(|(bucket, digest)| BucketDigest { bucket, digest })
            .collect();
        Box::pin(async move { digests })
    }

    fn ae_digests_for(&self, peer: NodeId) -> BoxFuture<'_, Vec<BucketDigest>> {
        let Some(residency) = self.residency_check() else {
            return self.digests();
        };
        let digests = self.bucket_digests_for(&residency, peer);
        Box::pin(async move { digests })
    }

    #[allow(private_interfaces, reason = "see the trait method's own allow")]
    fn ae_scoped_digests_for(&self, peer: NodeId) -> BoxFuture<'_, Option<ScopedDigests>> {
        let scoped = self.residency_check().map(|residency| {
            let scope = match residency.0.granularity() {
                Granularity::Bucket => Scope::Buckets(
                    self.bucket_digests_for(&residency, peer)
                        .into_iter()
                        .map(|bd| (bd.bucket, bd.digest))
                        .collect(),
                ),
                Granularity::Part => {
                    Scope::Parts(self.masked_digests(&Self::peer_scope(&residency, peer)))
                }
            };
            ScopedDigests {
                view_hash: residency.0.view_hash(),
                scope,
            }
        });
        Box::pin(async move { scoped })
    }

    fn bucket_entries(&self, bucket: u16) -> BoxFuture<'_, Vec<KeyVersion>> {
        if let Some(residency) = self.residency_check()
            && !Self::is_resident_bucket(&residency, bucket)
        {
            return Box::pin(async { Vec::new() });
        }
        let entries = self
            .engine
            .collect_buckets(&[bucket], self.now_ms())
            .pop()
            .map(|(_, entries)| entries)
            .unwrap_or_default();
        Box::pin(async move { entries })
    }

    fn entries_for_buckets(&self, buckets: Vec<u16>) -> BoxFuture<'_, BucketEntries> {
        // Every requested bucket exactly once, ascending, so the initiator
        // learns to push even for buckets this peer holds nothing in.
        let entries = self.resident_backfill(
            buckets,
            |residency, bucket| Self::is_resident_bucket(residency, bucket),
            |resident| self.engine.collect_buckets(&resident, self.now_ms()),
        );
        Box::pin(async move { entries })
    }

    fn bucket_lens(&self, buckets: Vec<u16>) -> BoxFuture<'_, Vec<BucketLen>> {
        let out = self.gated_map(buckets, |b| self.engine.bucket_len(b), 0);
        let out = out
            .into_iter()
            .map(|(bucket, len)| BucketLen { bucket, len })
            .collect();
        Box::pin(async move { out })
    }

    fn held_buckets(&self) -> BoxFuture<'_, Vec<u16>> {
        let held: Vec<u16> = (0..BUCKET_COUNT)
            .map(|b| u16::try_from(b).expect("invariant: BUCKET_COUNT fits u16"))
            .filter(|&b| self.engine.bucket_len(b) > 0)
            .collect();
        Box::pin(async move { held })
    }

    fn held_parts(&self) -> BoxFuture<'_, Vec<PartId>> {
        let held = self.engine.held_parts();
        Box::pin(async move { held })
    }

    fn part_digests(&self, buckets: Vec<u16>) -> BoxFuture<'_, Vec<BucketPartDigests>> {
        let out = self.gated_map(buckets, |b| self.engine.part_digests(b), Vec::new());
        let out = out
            .into_iter()
            .map(|(bucket, digests)| BucketPartDigests { bucket, digests })
            .collect();
        Box::pin(async move { out })
    }

    fn entries_for_parts(&self, parts: Vec<BucketPart>) -> BoxFuture<'_, PartEntries> {
        let entries = self.resident_backfill(
            parts,
            |residency, part: BucketPart| Self::is_resident_part(residency, PartId::from(part)),
            |resident| self.engine.collect_parts(&resident, self.now_ms()),
        );
        Box::pin(async move { entries })
    }

    /// A spilled entry's value is read off-lock behind the tier's read
    /// semaphore, batched into one `spawn_blocking` call, and dropped if the
    /// read fails. It is never promoted: an anti-entropy pull reply covers
    /// many keys, and reinstalling every one of them on every round would
    /// repopulate RAM as fast as eviction drains it.
    fn records_for(&self, keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>> {
        let now = self.now_ms();
        // The outbound-serving guard, at the key level: a key whose bucket
        // this shard neither owns nor is releasing is dropped from the
        // request before it ever reaches `engine`.
        let keys = match self.residency_check() {
            Some(residency) => keys
                .into_iter()
                .filter(|k| Self::is_resident_part(&residency, PartId::of_key(k.as_ref())))
                .collect(),
            None => keys,
        };
        #[cfg(feature = "spill")]
        {
            let (records, spilled) = self.engine.records_for_or_spilled(&keys, now);
            Box::pin(async move {
                let mut records = records;
                if !spilled.is_empty() {
                    records.extend(
                        read_spilled_batch(
                            self.engine.as_ref(),
                            self.spill_read.get(),
                            &self.name,
                            spilled,
                        )
                        .await,
                    );
                }
                records
            })
        }
        #[cfg(not(feature = "spill"))]
        {
            let recs = keys
                .into_iter()
                .filter_map(|key_bytes| self.engine.record_for(key_bytes.as_ref(), now))
                .collect();
            Box::pin(async move { recs })
        }
    }

    /// [`ShardOps::records_for`]'s note applies here too: a spilled entry is
    /// read off-lock and folded in, never promoted.
    fn snapshot_chunks(&self) -> BoxStream<'static, Vec<WireRecord>> {
        let engine = Arc::clone(&self.engine);
        let now = self.now_ms();
        let residency = self.residency_check();
        #[cfg(feature = "spill")]
        let spill_read = self.spill_read.get().cloned();
        #[cfg(feature = "spill")]
        let name = self.name.to_string();
        let fut = async move {
            let records = engine.snapshot_records(now);
            #[cfg(feature = "spill")]
            let records = {
                let spilled = engine.snapshot_spilled(now);
                let mut records = records;
                if !spilled.is_empty() {
                    records.extend(
                        read_spilled_batch(engine.as_ref(), spill_read.as_ref(), &name, spilled)
                            .await,
                    );
                }
                records
            };
            // The outbound-serving guard, over a full snapshot's records.
            let records = match residency {
                Some(residency) => records
                    .into_iter()
                    .filter(|rec| {
                        Self::is_resident_part(&residency, PartId::of_key(rec.key.as_ref()))
                    })
                    .collect(),
                None => records,
            };
            chunk_records_for_snapshot(records)
        };
        Box::pin(stream::once(fut).flat_map(stream::iter))
    }

    fn gc_tombstones(&self, any_member_absent: bool) -> BoxFuture<'_, ()> {
        let now = self.now_ms();
        self.engine.gc_tombstones(any_member_absent, now);
        Box::pin(async move {})
    }

    fn run_pending_tasks(&self) -> BoxFuture<'_, ()> {
        let now = self.now_ms();
        self.engine.sweep(now);
        Box::pin(async move {})
    }

    /// All the work here is synchronous: `Engine::compact` never awaits,
    /// nor does re-applying its candidates, so, like
    /// [`ShardOps::gc_tombstones`]/[`ShardOps::run_pending_tasks`] above,
    /// it all runs before the trivial future is built rather than inside
    /// an `async move` block: `retire`'s borrow only needs to outlive this
    /// call, not whatever lifetime the returned `BoxFuture<'_, _>` (tied
    /// to `&self`, per this trait method's elided signature) would force
    /// on anything captured into it.
    fn compact_pass(
        &self,
        now_ms: u64,
        retire: &dyn Fn(crdt::WriterId) -> bool,
        quiet: Quiescence,
        bounds: CompactionBounds,
        max_entries: usize,
    ) -> BoxFuture<'_, CompactPassOutcome> {
        if !self.resolver.merges() {
            // `ConflictResolver::compact`'s own contract: "only ever
            // called while `merges()` is `true`". Enforced here rather
            // than left to `Engine::compact`'s caller to remember, so a
            // non-merging shard (`LwwResolver` and every other
            // pick-a-side resolver, the common case) never pays for a
            // full-engine scan whose every call would return `None`
            // anyway, and the contract holds regardless of what a future
            // caller of this method does or doesn't check first.
            return Box::pin(async {
                CompactPassOutcome {
                    retired: Vec::new(),
                    compacted: 0,
                    stripes_visited: BUCKET_COUNT,
                }
            });
        }
        // `Engine::compact` takes `retire` as `&dyn Fn`, not `FnMut`, so
        // recording which writers it accepted (as opposed to every writer
        // merely passed to it) goes through interior mutability
        // rather than a captured `&mut`. A resolver's own `compact` only
        // ever calls `retire` on a writer identity the record still holds
        // a *live* slot for (never one already folded away by a prior
        // pass), and a `true` answer always turns that record into a
        // candidate (`newly` becomes non-empty), so every writer captured
        // here belongs to some candidate this scan found, deduplicated by
        // the `HashSet`, since the same eligible writer can surface across
        // more than one candidate record in a single pass.
        //
        // This is scoped to the whole scan, not to the handful of
        // candidates that go on to apply below: a candidate for a bucket
        // this shard's *current* view does not own anymore is skipped,
        // never applied, yet the writer(s) it would have retired are
        // still reported here. This is deliberate, not an oversight: the
        // alternative (only ever reporting a writer once every one of its
        // live records anywhere in the cluster is compacted) is
        // unobtainable at this layer, since nothing this generic-over-`V`
        // method sees distinguishes "a writer's slot" from arbitrary
        // bytes. Reporting it anyway is safe in the direction that
        // matters: the caller (`crdt_compact_tick`) only counts it in
        // `sundog_crdt_retired_writers_total`, never deletes data on the
        // strength of it, and the member stays tracked gone until it
        // returns, so a skipped record is retried on the next pass: the
        // skipped bucket's own stale copy is either physically dropped by
        // `ShardOps::release_buckets` once its disown-grace period ends
        // (the common case: ownership rarely moves back), or, if this
        // shard reacquires the bucket first, recompacted on this shard's
        // own next pass. The metric therefore counts writers found
        // eligible in the scan, which can exceed the records rewritten.
        let newly_retired: StdMutex<HashSet<crdt::WriterId>> = StdMutex::new(HashSet::new());
        let tracking_retire = |writer: crdt::WriterId| {
            let accepted = retire(writer);
            if accepted {
                newly_retired
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(writer);
            }
            accepted
        };
        let engine::CompactScan {
            candidates,
            stripes_visited,
        } = self.engine.compact(
            self.resolver.as_ref(),
            now_ms,
            &tracking_retire,
            quiet,
            bounds,
            max_entries,
        );
        let mut compacted = 0usize;
        for engine::CompactCandidate {
            key,
            key_bytes,
            stale_ver,
            new_encoded,
        } in candidates
        {
            // Mirrors `guard_inbound`'s strict current-view rule: a
            // `Mode::Distributed` shard mid disown-grace never re-applies
            // a compacted record for a bucket its *current* view does not
            // own anymore. Skipped, never forwarded: unlike an ordinary
            // write, a compaction candidate has no caller waiting on it,
            // and the bucket's new owner will compact its own copy on its
            // own schedule once it, too, decides the same writers are
            // dead.
            if !self.owns_key(key_bytes.as_ref()) {
                continue;
            }
            let Ok(value) = postcard::from_bytes::<V>(&new_encoded) else {
                tracing::warn!(
                    cache = %self.name,
                    "compact_pass: a resolver's own compacted bytes failed to decode as V"
                );
                continue;
            };
            // A version-gated direct replace, never the ordinary
            // merge-based apply path: see `Engine::compact_replace_if_current`'s
            // own doc for why a merge-based apply here would silently
            // undo stage three's own pruning on every single pass. `false`
            // means something else touched this entry since the scan
            // above read it (or it is not live anymore, or not
            // resident anymore): skipped, exactly like an unowned bucket
            // above, and recomputed fresh on the next pass rather than
            // counted here.
            let hash = engine::hash_key_bytes(key_bytes.as_ref());
            let new_ver = compacted_version(stale_ver, new_encoded.as_ref());
            self.observe_derived(new_ver);
            if self
                .engine
                .compact_replace_if_current(engine::CompactReplace {
                    key: &key,
                    key_bytes: key_bytes.as_ref(),
                    hash,
                    expected_ver: stale_ver,
                    new_ver,
                    value,
                    encoded: new_encoded,
                })
            {
                compacted += 1;
            }
        }
        let retired: Vec<crdt::WriterId> = newly_retired
            .into_inner()
            .unwrap_or_else(PoisonError::into_inner)
            .into_iter()
            .collect();
        Box::pin(async move {
            CompactPassOutcome {
                retired,
                compacted,
                stripes_visited,
            }
        })
    }

    fn close_spill(&self) {
        Shard::close_spill(self);
    }

    fn close_spill_checkpointed(&self) -> BoxFuture<'_, ()> {
        Box::pin(Shard::close_spill_checkpointed(self))
    }

    fn seal_fan_out(&self) {
        self.fan_out.seal();
    }

    #[allow(
        private_interfaces,
        reason = "OwnershipView has no public fields or methods; only this crate can act on the returned value regardless of this trait's own visibility"
    )]
    fn ownership_view(&self) -> Option<Arc<OwnershipView>> {
        self.ownership.as_ref().map(OwnershipTracker::current)
    }

    fn ownership_view_hash(&self) -> Option<u64> {
        self.ownership
            .as_ref()
            .map(|tracker| tracker.current().view_hash())
    }

    fn release_buckets(&self, buckets: &[u16]) -> BoxFuture<'_, u64> {
        let removed = self.engine.release_buckets(buckets);
        Box::pin(async move { removed })
    }

    fn release_parts(&self, parts: &[PartId]) -> BoxFuture<'_, u64> {
        let removed = self.engine.release_parts(parts);
        Box::pin(async move { removed })
    }

    // `Residency::cold` is the same rule over one read of every mark; this
    // reads only the marks it needs, for the per-part loops that call it.
    fn is_cold_part(&self, part: PartId) -> bool {
        self.residency.as_ref().is_some_and(|residency| {
            residency.is_cold(part)
                || (residency.is_unsettled(part)
                    && self
                        .ownership
                        .as_ref()
                        .is_some_and(|tracker| tracker.current().owns(part)))
        })
    }

    // `Residency::distrusted` is the same rule over one read of every mark.
    fn is_unverified_part(&self, part: PartId) -> bool {
        self.residency
            .as_ref()
            .is_some_and(|residency| residency.is_unverified(part))
    }

    fn is_cold_bucket(&self, bucket: u16) -> bool {
        PartId::of_bucket(bucket).any(|part| self.is_cold_part(part))
    }

    fn is_unverified_bucket(&self, bucket: u16) -> bool {
        PartId::of_bucket(bucket).any(|part| self.is_unverified_part(part))
    }

    fn ae_peer_filter(&self, dirty: Vec<NodeId>, live: Vec<NodeId>) -> (Vec<NodeId>, Vec<NodeId>) {
        let (Some(tracker), Some(residency)) = (self.ownership.as_ref(), self.residency.as_ref())
        else {
            return (dirty, live);
        };
        let view = tracker.current();
        let mut cohort: HashSet<NodeId> = view.co_owners().iter().copied().collect();
        for part in residency.releasing_parts() {
            cohort.extend(view.owners_of(part).iter().copied());
        }
        let filter = |nodes: Vec<NodeId>| -> Vec<NodeId> {
            nodes.into_iter().filter(|n| cohort.contains(n)).collect()
        };
        (filter(dirty), filter(live))
    }

    fn merges(&self) -> bool {
        self.resolver.merges()
    }
}

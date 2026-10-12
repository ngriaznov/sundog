//! The `RequestHandler` a cluster's mesh answers state-transfer, anti-entropy, fetch and load requests through, over the cluster's shard registry.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::{self, BoxStream, StreamExt as _};
use smol_str::SmolStr;

use super::{ShardRegistry, ShardRegistryExt, Warmth};
use crate::explain::ServeVerdict;
use crate::net::{AeServeOutcome, FetchServe, LoadServe, RequestHandler, StServe};
use crate::ownership::{Granularity, parts_of_wire_id};
use crate::store::{
    BucketDigest, BucketLen, BucketPart, BucketPartDigests, KeyVersion, PartId, PartMask, ShardOps,
    chunk_records_for_snapshot,
};
use crate::verdict;
use crate::wire::{self, WireRecord};

/// Answers inbound state-transfer/anti-entropy requests over this cluster's
/// shard registry, for whichever cache a peer names. An unregistered cache
/// name degrades to an empty result rather than an error.
pub(super) struct ClusterRequestHandler {
    pub(super) shards: ShardRegistry,
    pub(super) warmth: Arc<Warmth>,
    pub(super) ae_part_min_bucket: usize,
    pub(super) ae_sketch_min_bucket: usize,
    pub(super) ae_sketch_cells: usize,
    /// [`crate::config::ClusterConfig::rebalance_chunk_bytes_value`],
    /// resolved once at construction.
    pub(super) rebalance_chunk_bytes: u64,
}

impl ClusterRequestHandler {
    fn lookup(&self, cache: &SmolStr) -> Option<Arc<dyn ShardOps>> {
        self.shards.read_shards().get(cache).cloned()
    }

    /// `cache`'s shard when its view's hash is `view_hash`, otherwise the
    /// decline a scoped anti-entropy round answers with.
    fn matching_view<T>(
        &self,
        cache: &SmolStr,
        view_hash: u64,
    ) -> Result<Arc<dyn ShardOps>, AeServeOutcome<T>> {
        let shard = self.lookup(cache).ok_or(AeServeOutcome::Unavailable)?;
        let view = shard.ownership_view().ok_or(AeServeOutcome::Unavailable)?;
        if view.view_hash() == view_hash {
            Ok(shard)
        } else {
            Err(AeServeOutcome::Stale {
                responder_view_hash: view.view_hash(),
            })
        }
    }
}

/// The reply [`ClusterRequestHandler::fetch`] sends for `verdict`: `rec`,
/// the record it holds for the key, when serving it, and `local_hash`, its
/// own view hash, when the asker's view is stale. Pure; unit tested
/// directly.
pub(super) fn serve_reply(
    verdict: ServeVerdict,
    rec: Option<WireRecord>,
    local_hash: u64,
) -> FetchServe {
    match verdict {
        ServeVerdict::Serve => FetchServe::Found(rec),
        ServeVerdict::Miss => FetchServe::Found(None),
        ServeVerdict::Stale => FetchServe::Stale {
            responder_view_hash: local_hash,
        },
        ServeVerdict::DeclineDistrusted | ServeVerdict::DeclineCold => FetchServe::Unavailable,
    }
}

impl RequestHandler for ClusterRequestHandler {
    fn snapshot_available(&self, cache: SmolStr) -> bool {
        self.warmth.is_warm(&cache)
    }

    fn snapshot_chunks(&self, cache: SmolStr) -> BoxStream<'static, Vec<WireRecord>> {
        match self.lookup(&cache) {
            Some(shard) => shard.snapshot_chunks(),
            None => stream::empty().boxed(),
        }
    }

    fn digests(&self, cache: SmolStr) -> BoxFuture<'_, Vec<BucketDigest>> {
        Box::pin(async move {
            match self.lookup(&cache) {
                Some(shard) => shard.digests().await,
                None => Vec::new(),
            }
        })
    }

    fn bucket_entries(&self, cache: SmolStr, bucket: u16) -> BoxFuture<'_, Vec<KeyVersion>> {
        Box::pin(async move {
            match self.lookup(&cache) {
                Some(shard) => shard.bucket_entries(bucket).await,
                None => Vec::new(),
            }
        })
    }

    fn entries_for_buckets(
        &self,
        cache: SmolStr,
        buckets: Vec<u16>,
    ) -> BoxFuture<'_, crate::store::BucketEntries> {
        Box::pin(async move {
            match self.lookup(&cache) {
                Some(shard) => shard.entries_for_buckets(buckets).await,
                None => Vec::new(),
            }
        })
    }

    fn records_for(&self, cache: SmolStr, keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>> {
        Box::pin(async move {
            match self.lookup(&cache) {
                Some(shard) => shard.records_for(keys).await,
                None => Vec::new(),
            }
        })
    }

    fn bucket_lens(&self, cache: SmolStr, buckets: Vec<u16>) -> BoxFuture<'_, Vec<BucketLen>> {
        Box::pin(async move {
            match self.lookup(&cache) {
                Some(shard) => shard.bucket_lens(buckets).await,
                None => Vec::new(),
            }
        })
    }

    fn part_digests(
        &self,
        cache: SmolStr,
        buckets: Vec<u16>,
    ) -> BoxFuture<'_, Vec<BucketPartDigests>> {
        Box::pin(async move {
            match self.lookup(&cache) {
                Some(shard) => shard.part_digests(buckets).await,
                None => Vec::new(),
            }
        })
    }

    fn entries_for_parts(
        &self,
        cache: SmolStr,
        parts: Vec<BucketPart>,
    ) -> BoxFuture<'_, crate::store::PartEntries> {
        Box::pin(async move {
            match self.lookup(&cache) {
                Some(shard) => shard.entries_for_parts(parts).await,
                None => Vec::new(),
            }
        })
    }

    fn ae_part_min_bucket(&self) -> usize {
        self.ae_part_min_bucket
    }

    fn ae_sketch_min_bucket(&self) -> usize {
        self.ae_sketch_min_bucket
    }

    fn ae_sketch_cells(&self) -> usize {
        self.ae_sketch_cells
    }

    // Distribution-mode serving: every method here answers `Unavailable`
    // (or its shape's equivalent) for a cache that isn't registered, or one
    // registered but not a `Mode::Distributed` cache (`ownership_view_hash`
    // is `None` either way). Same "unknown cache degrades gracefully"
    // convention every other method on this trait already has.
    fn ownership_view_hash(&self, cache: SmolStr) -> Option<u64> {
        self.lookup(&cache)
            .and_then(|shard| shard.ownership_view_hash())
    }

    fn fetch(&self, cache: SmolStr, key: Bytes, view_hash: u64) -> BoxFuture<'_, FetchServe> {
        Box::pin(async move {
            let Some(shard) = self.lookup(&cache) else {
                return FetchServe::Unavailable;
            };
            let Some(local_hash) = shard.ownership_view_hash() else {
                return FetchServe::Unavailable;
            };
            let part = PartId::of_key(&key);
            // Both marks are read before the record, so a pull landing
            // during the read cannot clear a cold mark this answer then
            // trusts. A distrusted part reads nothing: it declines even a
            // hit.
            let distrusted = shard.is_unverified_part(part);
            let cold = shard.is_cold_part(part);
            let rec = if distrusted {
                None
            } else {
                shard.records_for(vec![key]).await.into_iter().next()
            };
            let verdict = verdict::fetch_serve_verdict(verdict::ServeInputs {
                distrusted,
                served: rec.is_some(),
                views_match: local_hash == view_hash,
                cold,
            });
            serve_reply(verdict, rec, local_hash)
        })
    }

    fn load(&self, cache: SmolStr, keys: Vec<Bytes>) -> BoxFuture<'_, LoadServe> {
        Box::pin(async move {
            match self.lookup(&cache) {
                Some(shard) => shard.serve_load(keys).await,
                None => LoadServe::Unavailable,
            }
        })
    }

    fn ae_digest_scoped(
        &self,
        cache: SmolStr,
        view_hash: u64,
        _buckets: Vec<BucketDigest>,
    ) -> BoxFuture<'_, AeServeOutcome<Vec<BucketDigest>>> {
        // The requester's buckets only matter once its epoch is confirmed
        // current; `conn::serve_ae_digest_scoped` classifies them.
        Box::pin(async move {
            match self.matching_view(&cache, view_hash) {
                Ok(shard) => AeServeOutcome::Digests(shard.digests().await),
                Err(declined) => declined,
            }
        })
    }

    fn ae_digest_masked(
        &self,
        cache: SmolStr,
        view_hash: u64,
        buckets: Vec<(u16, u64, u64)>,
    ) -> BoxFuture<'_, AeServeOutcome<Vec<BucketPartDigests>>> {
        Box::pin(async move {
            let shard = match self.matching_view(&cache, view_hash) {
                Ok(shard) => shard,
                Err(declined) => return declined,
            };
            let remote: HashMap<u16, (PartMask, u64)> = buckets
                .into_iter()
                .map(|(bucket, mask, digest)| (bucket, (PartMask::from_bits(mask), digest)))
                .collect();
            let local = shard.part_digests(remote.keys().copied().collect()).await;
            AeServeOutcome::Digests(
                local
                    .into_iter()
                    .filter(|bpd| {
                        remote
                            .get(&bpd.bucket)
                            .is_some_and(|&(mask, digest)| mask.fold(&bpd.digests) != digest)
                    })
                    .collect(),
            )
        })
    }

    fn st_serve(&self, cache: SmolStr, ids: Vec<u16>, view_hash: u64) -> BoxFuture<'_, StServe> {
        Box::pin(async move {
            let serving = self
                .lookup(&cache)
                .and_then(|shard| shard.ownership_view().map(|view| (shard, view)));
            let Some((shard, view)) = serving else {
                return StServe::Stale {
                    responder_view_hash: 0,
                };
            };
            if view.view_hash() != view_hash {
                return StServe::Stale {
                    responder_view_hash: view.view_hash(),
                };
            }
            let granularity = view.granularity();
            let order = serve_order(granularity, ids);
            let parts = || {
                order
                    .iter()
                    .flat_map(|&id| parts_of_wire_id(granularity, id))
            };
            if parts().any(|part| shard.is_cold_part(part)) {
                return StServe::Cold;
            }
            let parts = parts().count() as u64;
            StServe::Stream {
                chunks: id_chunks(shard, granularity, &order, self.rebalance_chunk_bytes),
                order,
                parts,
            }
        })
    }

    fn st_buckets_available(&self, cache: SmolStr, view_hash: u64) -> BoxFuture<'_, bool> {
        Box::pin(async move {
            self.lookup(&cache)
                .and_then(|shard| shard.ownership_view_hash())
                .is_some_and(|local_hash| local_hash == view_hash)
        })
    }

    fn st_buckets_cold(&self, cache: SmolStr, buckets: Vec<u16>) -> BoxFuture<'_, bool> {
        Box::pin(async move {
            self.lookup(&cache).is_some_and(|shard| {
                let granularity = current_granularity(shard.as_ref());
                buckets
                    .iter()
                    .flat_map(|&id| parts_of_wire_id(granularity, id))
                    .any(|part| shard.is_cold_part(part))
            })
        })
    }

    fn st_bucket_chunks(
        &self,
        cache: SmolStr,
        buckets: Vec<u16>,
    ) -> BoxStream<'static, (u16, Vec<WireRecord>)> {
        let Some(shard) = self.lookup(&cache) else {
            return stream::empty().boxed();
        };
        let granularity = current_granularity(shard.as_ref());
        let order = serve_order(granularity, buckets);
        id_chunks(shard, granularity, &order, self.rebalance_chunk_bytes)
    }
}

/// The order a donor streams `ids` in: bucket by bucket, each bucket's parts
/// together, so one entries read per bucket serves every requested part of
/// it. Bucket ids already are that order once sorted.
pub(super) fn serve_order(granularity: Granularity, mut ids: Vec<u16>) -> Vec<u16> {
    match granularity {
        Granularity::Bucket => ids.sort_unstable(),
        Granularity::Part => ids.sort_unstable_by_key(|&id| {
            let part = PartId::from_raw(id);
            (part.bucket(), part.part())
        }),
    }
    ids.dedup();
    ids
}

/// The granularity `shard`'s current view reads wire ids at: `Bucket` for a
/// shard with no view.
fn current_granularity(shard: &dyn ShardOps) -> Granularity {
    shard
        .ownership_view()
        .map_or(Granularity::Bucket, |view| view.granularity())
}

/// Splits `ids`, already in [`serve_order`], into one run per bucket: a
/// bucket id alone, or every requested part of one bucket.
pub(super) fn bucket_runs(granularity: Granularity, ids: &[u16]) -> impl Iterator<Item = &[u16]> {
    ids.chunk_by(move |&a, &b| {
        granularity == Granularity::Part
            && PartId::from_raw(a).bucket() == PartId::from_raw(b).bucket()
    })
}

/// Streams the records of `ids`, in [`serve_order`], as frames tagged with
/// their id: one entries read per bucket run, then `chunk_bytes`-sized key
/// batches read one at a time, each split on the frame cap. An id holding
/// nothing yields no frame.
fn id_chunks(
    shard: Arc<dyn ShardOps>,
    granularity: Granularity,
    ids: &[u16],
    chunk_bytes: u64,
) -> BoxStream<'static, (u16, Vec<WireRecord>)> {
    let runs: Vec<Vec<u16>> = bucket_runs(granularity, ids).map(<[u16]>::to_vec).collect();
    let keys_shard = Arc::clone(&shard);
    stream::iter(runs)
        .then(move |run| id_keys(Arc::clone(&keys_shard), granularity, run))
        .flat_map(move |keyed| {
            stream::iter(keyed.into_iter().flat_map(move |(id, keys)| {
                st_bucket_key_sub_batches(keys, chunk_bytes)
                    .into_iter()
                    .map(move |batch| (id, batch))
            }))
        })
        .then(move |(id, batch)| {
            let shard = Arc::clone(&shard);
            async move { (id, shard.records_for(batch).await) }
        })
        .flat_map(|(id, recs)| {
            stream::iter(
                chunk_records_for_snapshot(recs)
                    .into_iter()
                    .map(move |chunk| (id, chunk)),
            )
        })
        .boxed()
}

/// The keys each id of one bucket run holds, in one entries read; an id
/// holding nothing is left out.
async fn id_keys(
    shard: Arc<dyn ShardOps>,
    granularity: Granularity,
    run: Vec<u16>,
) -> Vec<(u16, Vec<Bytes>)> {
    let listed: Vec<(u16, Vec<KeyVersion>)> = match granularity {
        Granularity::Bucket => shard.entries_for_buckets(run).await,
        Granularity::Part => shard
            .entries_for_parts(
                run.iter()
                    .map(|&id| PartId::from_raw(id).bucket_part())
                    .collect(),
            )
            .await
            .into_iter()
            .map(|(bp, entries)| (PartId::from(bp).raw(), entries))
            .collect(),
    };
    listed
        .into_iter()
        .filter(|(_, entries)| !entries.is_empty())
        .map(|(id, entries)| (id, entries.into_iter().map(|kv| kv.key).collect()))
        .collect()
}

/// Splits one bucket's `keys` into groups whose estimated encoded size
/// (`wire::RECORD_HEADER_LEN + key.len()` per key, the only size known
/// before `records_for` reads each value) is `<= chunk_bytes`. A key whose
/// own estimate already meets or exceeds `chunk_bytes` still gets a
/// one-key group of its own rather than being dropped.
pub(super) fn st_bucket_key_sub_batches(keys: Vec<Bytes>, chunk_bytes: u64) -> Vec<Vec<Bytes>> {
    let mut sub_batches = Vec::new();
    let mut current = Vec::new();
    let mut current_size: u64 = 0;
    for key in keys {
        let key_size = (wire::RECORD_HEADER_LEN + key.len()) as u64;
        let over_budget = current_size.saturating_add(key_size) > chunk_bytes;
        if over_budget && !current.is_empty() {
            sub_batches.push(std::mem::take(&mut current));
            current_size = 0;
        }
        current_size += key_size;
        current.push(key);
    }
    if !current.is_empty() {
        sub_batches.push(current);
    }
    sub_batches
}

#[cfg(kani)]
mod kani_proofs {
    use super::*;

    /// Bucket runs split a serve order where the bucket changes: every run
    /// is one bucket, runs ascend by bucket, and together the runs are the
    /// order. Ids span four buckets of four parts, where raw id order and
    /// bucket order disagree.
    #[kani::proof]
    #[kani::unwind(8)]
    fn bucket_runs_split_the_serve_order_at_each_bucket() {
        let granularity = if kani::any() {
            Granularity::Part
        } else {
            Granularity::Bucket
        };
        let key = |id: u16| match granularity {
            Granularity::Bucket => (id, 0),
            Granularity::Part => (PartId::from_raw(id).bucket(), PartId::from_raw(id).part()),
        };
        let order: [u16; 3] = std::array::from_fn(|_| {
            let (bucket, part): (u8, u8) = (kani::any(), kani::any());
            kani::assume(bucket < 4 && part < 4);
            match granularity {
                Granularity::Bucket => u16::from(bucket),
                Granularity::Part => PartId::new(u16::from(bucket), part).raw(),
            }
        });
        kani::assume(order.windows(2).all(|pair| key(pair[0]) < key(pair[1])));
        let mut next = 0;
        let mut last_bucket = None;
        for run in bucket_runs(granularity, &order) {
            let bucket = key(run[0]).0;
            assert!(last_bucket < Some(bucket));
            last_bucket = Some(bucket);
            for &id in run {
                assert_eq!((id, key(id).0), (order[next], bucket));
                next += 1;
            }
            if granularity == Granularity::Bucket {
                assert_eq!(run.len(), 1);
            }
        }
        assert_eq!(next, order.len());
    }
}

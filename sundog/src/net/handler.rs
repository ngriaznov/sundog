//! What the net layer needs from the local shard registry to answer a peer.

#[cfg(doc)]
use crate::wire::Msg;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use smol_str::SmolStr;
use xxhash_rust::xxh3::xxh3_64;

use super::{AeServeOutcome, FetchServe, LoadServe, StServe};
use crate::store::{BucketDigest, BucketLen, BucketPart, BucketPartDigests, KeyVersion};
use crate::wire::WireRecord;

/// What the net layer needs from the local shard registry to answer
/// another node's state-transfer or anti-entropy request. An unknown cache
/// degrades to an empty result rather than an error: a normal race, not a
/// fault.
pub trait RequestHandler: Send + Sync + 'static {
    /// Whether this node can donate a snapshot of `cache` right now. A node
    /// still warming `cache` from its own state transfer, or one that never
    /// opened it, answers `false` and the requester moves on to another
    /// donor; defaults to `true`.
    fn snapshot_available(&self, cache: SmolStr) -> bool {
        let _ = cache;
        true
    }
    /// Streams a full snapshot of `cache` in write-sized chunks, for state
    /// transfer on join.
    fn snapshot_chunks(&self, cache: SmolStr) -> BoxStream<'static, Vec<WireRecord>>;
    /// Returns `cache`'s current per-bucket digest array.
    fn digests(&self, cache: SmolStr) -> BoxFuture<'_, Vec<BucketDigest>>;
    /// Returns the live key/version listing for one bucket of `cache`.
    fn bucket_entries(&self, cache: SmolStr, bucket: u16) -> BoxFuture<'_, Vec<KeyVersion>>;
    /// [`RequestHandler::bucket_entries`] for many buckets in one shard pass.
    fn entries_for_buckets(
        &self,
        cache: SmolStr,
        buckets: Vec<u16>,
    ) -> BoxFuture<'_, crate::store::BucketEntries>;
    /// Returns full records for `keys` in `cache` that this node holds.
    fn records_for(&self, cache: SmolStr, keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>>;

    /// Returns the entry count of each of `buckets` in `cache`, without
    /// materializing their contents: the cheap check `serve_ae_digest` makes
    /// before deciding whether a mismatched bucket is answered with part
    /// digests instead of a listing or sketch.
    fn bucket_lens(&self, cache: SmolStr, buckets: Vec<u16>) -> BoxFuture<'_, Vec<BucketLen>>;

    /// Returns `cache`'s part digests for each of `buckets`, the responder's
    /// step-2 reply for a bucket past
    /// [`RequestHandler::ae_part_min_bucket`] entries.
    fn part_digests(
        &self,
        cache: SmolStr,
        buckets: Vec<u16>,
    ) -> BoxFuture<'_, Vec<BucketPartDigests>>;

    /// [`RequestHandler::entries_for_buckets`] at part granularity: a
    /// [`KeyVersion`] for every live entry and un-GC'd tombstone in each
    /// requested [`BucketPart`] of `cache`.
    fn entries_for_parts(
        &self,
        cache: SmolStr,
        parts: Vec<BucketPart>,
    ) -> BoxFuture<'_, crate::store::PartEntries>;

    /// Returns full records for the entries of `bucket` in `cache` whose
    /// key hash is in `hashes`: the sketch-decoded counterpart to
    /// [`RequestHandler::records_for`]'s direct-key form.
    ///
    /// The default composes [`RequestHandler::bucket_entries`] and
    /// [`RequestHandler::records_for`], paying for two lookups; override
    /// it when one lookup serves both.
    fn records_for_hashes(
        &self,
        cache: SmolStr,
        bucket: u16,
        hashes: Vec<u64>,
    ) -> BoxFuture<'_, Vec<WireRecord>> {
        Box::pin(async move {
            let wanted: std::collections::HashSet<u64> = hashes.into_iter().collect();
            let entries = self.bucket_entries(cache.clone(), bucket).await;
            let keys: Vec<Bytes> = entries
                .into_iter()
                .filter(|kv| wanted.contains(&xxh3_64(&kv.key)))
                .map(|kv| kv.key)
                .collect();
            self.records_for(cache, keys).await
        })
    }

    /// Bucket size above which the responder answers a mismatch with its 64
    /// part digests instead of a listing or sketch. Mirrors
    /// [`crate::config::ClusterConfig::ae_part_min_bucket`].
    fn ae_part_min_bucket(&self) -> usize {
        crate::config::ClusterConfig::default().ae_part_min_bucket
    }

    /// Bucket size above which the responder answers with an IBLT sketch
    /// instead of its full listing; a listing that would pass half a frame
    /// goes out as a sketch whatever the size. Mirrors
    /// [`crate::config::ClusterConfig::ae_sketch_min_bucket`].
    fn ae_sketch_min_bucket(&self) -> usize {
        crate::config::ClusterConfig::default().ae_sketch_min_bucket
    }

    /// Sketch size, in cells, the responder builds an `Msg::AeSketch` reply
    /// with. Mirrors [`crate::config::ClusterConfig::ae_sketch_cells`]'s
    /// default.
    fn ae_sketch_cells(&self) -> usize {
        crate::config::ClusterConfig::default().ae_sketch_cells
    }

    /// The requester's `view_hash` for `cache`, when this node has one:
    /// `cache` is a distribution-mode cache this node has opened. Default
    /// `None`: every existing implementor keeps compiling and never answers
    /// a distribution-mode epoch check, correct since none of them open a
    /// distribution-mode cache.
    fn ownership_view_hash(&self, cache: SmolStr) -> Option<u64> {
        let _ = cache;
        None
    }

    /// Serves a [`crate::wire::Msg::Fetch`]. Default: `FetchServe::Unavailable`,
    /// the same "unknown cache degrades gracefully" shape every existing
    /// method here already has.
    ///
    /// `FetchServe` is `pub(crate)`, deliberately: distribution mode is
    /// served only by this crate's own `ClusterRequestHandler` for now, so
    /// an external implementor overrides every other method on this trait
    /// but leaves this one at its default.
    #[allow(private_interfaces, reason = "see the doc comment above")]
    fn fetch(&self, cache: SmolStr, key: Bytes, view_hash: u64) -> BoxFuture<'_, FetchServe> {
        let _ = (cache, key, view_hash);
        Box::pin(async { FetchServe::Unavailable })
    }

    /// Serves a [`crate::wire::Msg::Load`]. Default:
    /// `LoadServe::Unavailable`; `LoadServe` is `pub(crate)` for the same
    /// reason `FetchServe` is, see [`RequestHandler::fetch`].
    #[allow(private_interfaces, reason = "see RequestHandler::fetch's doc comment")]
    fn load(&self, cache: SmolStr, keys: Vec<Bytes>) -> BoxFuture<'_, LoadServe> {
        let _ = (cache, keys);
        Box::pin(async { LoadServe::Unavailable })
    }

    /// Serves a [`crate::wire::Msg::AeDigestScoped`]. Default:
    /// `AeServeOutcome::Unavailable`. `AeServeOutcome` is `pub(crate)` for
    /// the same reason `FetchServe` is; see [`RequestHandler::fetch`].
    #[allow(private_interfaces, reason = "see RequestHandler::fetch's doc comment")]
    fn ae_digest_scoped(
        &self,
        cache: SmolStr,
        view_hash: u64,
        buckets: Vec<BucketDigest>,
    ) -> BoxFuture<'_, AeServeOutcome<Vec<BucketDigest>>> {
        let _ = (cache, view_hash, buckets);
        Box::pin(async { AeServeOutcome::Unavailable })
    }

    /// Serves a [`crate::wire::Msg::AeDigestMasked`]: the part digests of
    /// each `(bucket, mask, digest)` whose masked fold here differs.
    /// Default: `AeServeOutcome::Unavailable`; see
    /// [`RequestHandler::ae_digest_scoped`].
    #[allow(private_interfaces, reason = "see RequestHandler::fetch's doc comment")]
    fn ae_digest_masked(
        &self,
        cache: SmolStr,
        view_hash: u64,
        buckets: Vec<(u16, u64, u64)>,
    ) -> BoxFuture<'_, AeServeOutcome<Vec<BucketPartDigests>>> {
        let _ = (cache, view_hash, buckets);
        Box::pin(async { AeServeOutcome::Unavailable })
    }

    /// Whether this node owns any of `buckets` without having pulled it
    /// from a co-owner yet, so its copy is not a source to transfer from:
    /// the responder declines with [`Msg::StUnavailable`] and the requester
    /// tries its next donor. Default: `false`.
    fn st_buckets_cold(&self, cache: SmolStr, buckets: Vec<u16>) -> BoxFuture<'_, bool> {
        let _ = (cache, buckets);
        Box::pin(async { false })
    }

    /// Whether this node can donate `cache`'s named `buckets` right now, at
    /// `view_hash`: the [`crate::wire::Msg::StBuckets`] availability check.
    /// Default `false`.
    fn st_buckets_available(&self, cache: SmolStr, view_hash: u64) -> BoxFuture<'_, bool> {
        let _ = (cache, view_hash);
        Box::pin(async { false })
    }

    /// Decides a [`crate::wire::Msg::StBuckets`] pull in one step: the view
    /// check, cold check, and chunk stream all read the same view, so ids
    /// are read at the granularity the requester sent them at. The default
    /// composes [`RequestHandler::st_buckets_available`],
    /// [`RequestHandler::st_buckets_cold`], and
    /// [`RequestHandler::st_bucket_chunks`], treating each id as a whole
    /// bucket.
    #[allow(private_interfaces, reason = "see RequestHandler::fetch's doc comment")]
    fn st_serve(&self, cache: SmolStr, ids: Vec<u16>, view_hash: u64) -> BoxFuture<'_, StServe> {
        Box::pin(async move {
            if !self.st_buckets_available(cache.clone(), view_hash).await {
                return StServe::Stale {
                    responder_view_hash: self.ownership_view_hash(cache).unwrap_or(0),
                };
            }
            if self.st_buckets_cold(cache.clone(), ids.clone()).await {
                return StServe::Cold;
            }
            let parts = (ids.len() as u64).saturating_mul(crate::store::PART_COUNT as u64);
            StServe::Stream {
                chunks: self.st_bucket_chunks(cache, ids.clone()),
                order: ids,
                parts,
            }
        })
    }

    /// Streams the reply chunks once
    /// [`RequestHandler::st_buckets_available`] said yes. Default: an
    /// immediately-empty stream, never polled unless the caller ignores the
    /// availability check.
    ///
    /// Each item is tagged with its bucket, since a donor may split one
    /// bucket's records into several chunks
    /// ([`crate::config::ClusterConfig::rebalance_chunk_bytes`]).
    fn st_bucket_chunks(
        &self,
        cache: SmolStr,
        buckets: Vec<u16>,
    ) -> BoxStream<'static, (u16, Vec<WireRecord>)> {
        let _ = (cache, buckets);
        Box::pin(futures::stream::empty())
    }
}

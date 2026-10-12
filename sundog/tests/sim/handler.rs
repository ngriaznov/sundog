//! The `ShardHandler` request handler and the inbound dispatch helpers.

use crate::support::{cache_name, key_bytes};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use smol_str::SmolStr;
use std::sync::Arc;
use sundog::config::ClusterConfig;
use sundog::net::{Mesh, MsgClass, RequestHandler};
use sundog::node::NodeId;
use sundog::store::{BucketDigest, BucketLen, BucketPart, BucketPartDigests, KeyVersion, ShardOps};
use sundog::wire::{Msg, WireRecord};

pub(crate) async fn dispatch_inbound<S: ShardOps>(shard: &S, msg: Msg) {
    match msg {
        Msg::Invalidate { key, ver, .. } => ShardOps::invalidate(shard, key, ver).await,
        Msg::Replicate { rec, .. } => ShardOps::apply_remote(shard, rec).await,
        Msg::ReplicateBatch { recs, .. } | Msg::ForwardBatch { recs, .. } => {
            ShardOps::apply_remote_batch(shard, recs).await;
        }
        // `Hello`, request/response messages, and `ReqDone` are a no-op here.
        _ => {}
    }
}

/// Wraps a shared `Shard` as the `net::RequestHandler` a `Mesh` answers
/// inbound requests through, standing in for `cluster.rs`'s handler.
/// `ae_sketch_min_bucket`/`ae_part_min_bucket` mirror the fields of the same
/// name on `ClusterConfig`, letting a scenario force a low threshold so a
/// mismatched bucket answers with `AeMismatch::Sketch`/`PartDigests` instead
/// of a full listing.
pub(crate) struct ShardHandler<S: ShardOps + 'static> {
    shard: Arc<S>,
    ae_part_min_bucket: usize,
    ae_sketch_min_bucket: usize,
}

impl<S: ShardOps + 'static> ShardHandler<S> {
    /// Both thresholds at [`ClusterConfig::default`]'s values: every
    /// scenario but the sketch/part ones want this, since their buckets
    /// never grow past either.
    pub(crate) fn new(shard: Arc<S>) -> Self {
        let defaults = ClusterConfig::default();
        Self::with_min_buckets(
            shard,
            defaults.ae_part_min_bucket,
            defaults.ae_sketch_min_bucket,
        )
    }

    pub(crate) fn with_min_buckets(
        shard: Arc<S>,
        ae_part_min_bucket: usize,
        ae_sketch_min_bucket: usize,
    ) -> Self {
        Self {
            shard,
            ae_part_min_bucket,
            ae_sketch_min_bucket,
        }
    }
}

impl<S: ShardOps + 'static> RequestHandler for ShardHandler<S> {
    fn snapshot_chunks(&self, _cache: SmolStr) -> BoxStream<'static, Vec<WireRecord>> {
        ShardOps::snapshot_chunks(self.shard.as_ref())
    }

    fn digests(&self, _cache: SmolStr) -> BoxFuture<'_, Vec<BucketDigest>> {
        let shard = Arc::clone(&self.shard);
        Box::pin(async move { ShardOps::digests(shard.as_ref()).await })
    }

    fn bucket_entries(&self, _cache: SmolStr, bucket: u16) -> BoxFuture<'_, Vec<KeyVersion>> {
        let shard = Arc::clone(&self.shard);
        Box::pin(async move { ShardOps::bucket_entries(shard.as_ref(), bucket).await })
    }

    fn entries_for_buckets(
        &self,
        _cache: SmolStr,
        buckets: Vec<u16>,
    ) -> BoxFuture<'_, sundog::store::BucketEntries> {
        let shard = Arc::clone(&self.shard);
        Box::pin(async move { ShardOps::entries_for_buckets(shard.as_ref(), buckets).await })
    }

    fn records_for(&self, _cache: SmolStr, keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>> {
        let shard = Arc::clone(&self.shard);
        Box::pin(async move { ShardOps::records_for(shard.as_ref(), keys).await })
    }

    fn bucket_lens(&self, _cache: SmolStr, buckets: Vec<u16>) -> BoxFuture<'_, Vec<BucketLen>> {
        let shard = Arc::clone(&self.shard);
        Box::pin(async move { ShardOps::bucket_lens(shard.as_ref(), buckets).await })
    }

    fn part_digests(
        &self,
        _cache: SmolStr,
        buckets: Vec<u16>,
    ) -> BoxFuture<'_, Vec<BucketPartDigests>> {
        let shard = Arc::clone(&self.shard);
        Box::pin(async move { ShardOps::part_digests(shard.as_ref(), buckets).await })
    }

    fn entries_for_parts(
        &self,
        _cache: SmolStr,
        parts: Vec<BucketPart>,
    ) -> BoxFuture<'_, sundog::store::PartEntries> {
        let shard = Arc::clone(&self.shard);
        Box::pin(async move { ShardOps::entries_for_parts(shard.as_ref(), parts).await })
    }

    fn ae_part_min_bucket(&self) -> usize {
        self.ae_part_min_bucket
    }

    fn ae_sketch_min_bucket(&self) -> usize {
        self.ae_sketch_min_bucket
    }
}

/// Fans `key`'s current record out to `peers`, `dup_factor` times each,
/// standing in for `cluster::fan_out_one`'s `Mode::Replicated` arm.
pub(crate) async fn fan_out<S: ShardOps>(
    shard: &S,
    mesh: &Mesh,
    peers: &[NodeId],
    key: u32,
    dup_factor: usize,
) {
    let Some(rec) = ShardOps::records_for(shard, vec![key_bytes(key)])
        .await
        .into_iter()
        .next()
    else {
        return;
    };
    for &peer in peers {
        for _ in 0..dup_factor.max(1) {
            mesh.send(
                peer,
                MsgClass::Replicate,
                Msg::Replicate {
                    cache: cache_name(),
                    rec: rec.clone(),
                },
            );
        }
    }
}

//! The resolver and shard wrappers that count folds, records and redundant pulls.

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use sundog::hlc::Hlc;
use sundog::node::NodeId;
use sundog::store::{BucketDigest, BucketLen, BucketPart, BucketPartDigests, KeyVersion, ShardOps};
use sundog::wire::WireRecord;
use sundog::{ConflictResolver, Merged, RecordView, Winner};

/// Wraps a [`ConflictResolver`] to count every `Some` its [`ConflictResolver::merge`]
/// returns: the "resolver folds" metric, a test-visible counter on the
/// resolver itself rather than anything the production `ConflictResolver`
/// contract exposes.
pub(super) struct CountingResolver<R> {
    inner: R,
    folds: AtomicU64,
}

impl<R> CountingResolver<R> {
    pub(super) fn new(inner: R) -> Self {
        Self {
            inner,
            folds: AtomicU64::new(0),
        }
    }

    pub(super) fn folds(&self) -> u64 {
        self.folds.load(Ordering::Relaxed)
    }
}

impl<R: ConflictResolver> ConflictResolver for CountingResolver<R> {
    fn winner(&self, key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        self.inner.winner(key, a, b)
    }

    fn needs_value_bytes(&self) -> bool {
        self.inner.needs_value_bytes()
    }

    fn merges(&self) -> bool {
        self.inner.merges()
    }

    fn merge(&self, key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Option<Merged> {
        let merged = self.inner.merge(key, a, b);
        if merged.is_some() {
            self.folds.fetch_add(1, Ordering::Relaxed);
        }
        merged
    }
}

/// Wraps a shard's `ShardOps` surface to count, on THIS wrapper instance
/// specifically, every record passed to `apply_remote`/`apply_remote_batch`
/// and how many of them left the touched key's stored `(version, bytes)` --
/// its exact `WireRecord` -- unchanged: a genuine no-op fold, by the same
/// byte-exact criterion `resolve_and_rebind` itself uses to skip
/// re-publishing and re-replicating a redelivered merge. A node keeps two
/// independently-counting instances wrapping the very same inner shard: one
/// given to `dispatch_inbound` for the push path, one given to
/// `run_round_against` for the pull path -- `ShardOps::apply_remote_batch`'s
/// own signature carries no tag saying which direction a call arrived
/// through, so which wrapper instance a call lands on is the only thing
/// that tells push apart from pull. [`HealNode`] pairs the two up around one
/// real shard.
pub(super) struct CountingShard<S> {
    inner: Arc<S>,
    records: AtomicU64,
    redundant: AtomicU64,
}

impl<S> CountingShard<S> {
    pub(super) fn new(inner: Arc<S>) -> Self {
        Self {
            inner,
            records: AtomicU64::new(0),
            redundant: AtomicU64::new(0),
        }
    }

    pub(super) fn records(&self) -> u64 {
        self.records.load(Ordering::Relaxed)
    }

    pub(super) fn redundant(&self) -> u64 {
        self.redundant.load(Ordering::Relaxed)
    }
}

impl<S: ShardOps + 'static> CountingShard<S> {
    /// Applies `recs` through the inner shard (`forwarded` picks its
    /// forwarded entry point) and counts each record and each one that left
    /// its key's stored record unchanged.
    fn counted(&self, recs: Vec<WireRecord>, forwarded: bool) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            if recs.is_empty() {
                return;
            }
            self.records.fetch_add(recs.len() as u64, Ordering::Relaxed);
            let keys: Vec<Bytes> = recs.iter().map(|rec| rec.key.clone()).collect();
            let before: HashMap<Bytes, WireRecord> = self
                .inner
                .records_for(keys.clone())
                .await
                .into_iter()
                .map(|rec| (rec.key.clone(), rec))
                .collect();
            if forwarded {
                self.inner.apply_forwarded_batch(recs).await;
            } else {
                self.inner.apply_remote_batch(recs).await;
            }
            let after = self.inner.records_for(keys).await;
            let redundant = after
                .into_iter()
                .filter(|rec| before.get(&rec.key).is_some_and(|prior| prior == rec))
                .count();
            if redundant > 0 {
                self.redundant
                    .fetch_add(redundant as u64, Ordering::Relaxed);
            }
        })
    }
}

impl<S: ShardOps + 'static> ShardOps for CountingShard<S> {
    fn apply_remote(&self, rec: WireRecord) -> BoxFuture<'_, ()> {
        self.apply_remote_batch(vec![rec])
    }

    fn apply_remote_batch(&self, recs: Vec<WireRecord>) -> BoxFuture<'_, ()> {
        self.counted(recs, false)
    }

    /// Counts like `apply_remote_batch` and delegates to the inner shard's
    /// forwarded apply, so its skew hold still works under the wrapper.
    fn apply_forwarded_batch(&self, recs: Vec<WireRecord>) -> BoxFuture<'_, ()> {
        self.counted(recs, true)
    }

    fn invalidate(&self, key: Bytes, ver: Hlc) -> BoxFuture<'_, ()> {
        self.inner.invalidate(key, ver)
    }

    fn digests(&self) -> BoxFuture<'_, Vec<BucketDigest>> {
        self.inner.digests()
    }

    fn ae_digests_for(&self, peer: NodeId) -> BoxFuture<'_, Vec<BucketDigest>> {
        self.inner.ae_digests_for(peer)
    }

    fn bucket_entries(&self, bucket: u16) -> BoxFuture<'_, Vec<KeyVersion>> {
        self.inner.bucket_entries(bucket)
    }

    fn entries_for_buckets(
        &self,
        buckets: Vec<u16>,
    ) -> BoxFuture<'_, sundog::store::BucketEntries> {
        self.inner.entries_for_buckets(buckets)
    }

    fn bucket_lens(&self, buckets: Vec<u16>) -> BoxFuture<'_, Vec<BucketLen>> {
        self.inner.bucket_lens(buckets)
    }

    fn part_digests(&self, buckets: Vec<u16>) -> BoxFuture<'_, Vec<BucketPartDigests>> {
        self.inner.part_digests(buckets)
    }

    fn entries_for_parts(
        &self,
        parts: Vec<BucketPart>,
    ) -> BoxFuture<'_, sundog::store::PartEntries> {
        self.inner.entries_for_parts(parts)
    }

    fn records_for(&self, keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>> {
        self.inner.records_for(keys)
    }

    fn snapshot_chunks(&self) -> BoxStream<'static, Vec<WireRecord>> {
        self.inner.snapshot_chunks()
    }

    fn gc_tombstones(&self, any_member_absent: bool) -> BoxFuture<'_, ()> {
        self.inner.gc_tombstones(any_member_absent)
    }

    fn run_pending_tasks(&self) -> BoxFuture<'_, ()> {
        self.inner.run_pending_tasks()
    }

    fn merges(&self) -> bool {
        self.inner.merges()
    }

    fn ae_exchanges_both(&self) -> bool {
        self.inner.ae_exchanges_both()
    }
}

//! The type-erased `ShardOps` surface the network layer drives a shard
//! through, and the scoped digests its anti-entropy answers carry.

use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::BoxStream;

use super::{
    BUCKET_COUNT, BucketDigest, BucketEntries, BucketLen, BucketPart, BucketPartDigests,
    CompactPassOutcome, CompactionBounds, KeyVersion, PartEntries, PartId, PartMask, Quiescence,
    crdt,
};
use crate::hlc::Hlc;
use crate::node::NodeId;
use crate::ownership::OwnershipView;
use crate::wire::WireRecord;

#[cfg(doc)]
use super::{ConflictResolver, LwwResolver};
#[cfg(doc)]
use crate::error::CacheError;

/// [`ShardOps::ae_scoped_digests_for`]'s answer: one view's hash and what
/// a round under it compares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScopedDigests {
    pub(crate) view_hash: u64,
    pub(crate) scope: Scope,
}

/// What one scoped anti-entropy round compares, at its view's granularity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Scope {
    /// Under a bucket view: each bucket in scope and its digest.
    Buckets(Vec<(u16, u64)>),
    /// Under a part view: each bucket holding a part in scope, those parts,
    /// and their folded digest.
    Parts(Vec<MaskedDigest>),
}

/// One bucket of a [`Scope::Parts`]: the parts in scope and the XOR of
/// their part digests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MaskedDigest {
    pub(crate) bucket: u16,
    pub(crate) mask: PartMask,
    pub(crate) digest: u64,
}

impl Scope {
    /// Each bucket the round compares and the parts of it that count.
    pub(crate) fn coverage(&self) -> Vec<(u16, PartMask)> {
        match self {
            Self::Buckets(ids) => ids
                .iter()
                .map(|&(bucket, _)| (bucket, PartMask::ALL))
                .collect(),
            Self::Parts(masked) => masked.iter().map(|m| (m.bucket, m.mask)).collect(),
        }
    }

    /// Keeps the buckets whose parts in scope `keep` accepts.
    pub(crate) fn retain(&mut self, mut keep: impl FnMut(u16, PartMask) -> bool) {
        match self {
            Self::Buckets(ids) => ids.retain(|&(bucket, _)| keep(bucket, PartMask::ALL)),
            Self::Parts(masked) => masked.retain(|m| keep(m.bucket, m.mask)),
        }
    }
}

/// The type-erased surface the network layer drives a shard through, wire bytes
/// in and out: the boundary where postcard (de)serialization happens for the
/// wire. A local read decodes its own stored record under the stripe's read
/// lock. Implemented by `Shard<K, V>` for any `K`, `V` meeting its bounds, and
/// held as `Arc<dyn ShardOps>` in the cluster's cache registry.
///
/// Async methods return `BoxFuture` rather than `async fn` so `dyn ShardOps`
/// stays usable from a `HashMap<SmolStr, Arc<dyn ShardOps>>`.
pub trait ShardOps: Send + Sync {
    /// Applies an inbound replicated record iff its version is newer than
    /// what's stored, the versioned-apply rule that makes replication
    /// commutative.
    fn apply_remote(&self, rec: WireRecord) -> BoxFuture<'_, ()>;

    /// [`ShardOps::apply_remote`] for a whole batch, one lock acquisition per
    /// touched stripe rather than one per record. A `Mode::Invalidation`
    /// shard folds a live record only into a key it holds, ignores one for
    /// any other key, and treats a tombstone record as [`ShardOps::invalidate`].
    fn apply_remote_batch(&self, recs: Vec<WireRecord>) -> BoxFuture<'_, ()>;

    /// [`ShardOps::apply_remote_batch`] for the records of a
    /// [`crate::wire::Msg::ForwardBatch`]. A `Mode::Distributed` writer that is
    /// not an owner of a record's part keeps no copy of the write it forwards,
    /// so a record this shard refuses for a stamp further ahead of its clock
    /// than `max_clock_skew` is held in memory and applied once the clock is
    /// within the bound, where `apply_remote_batch` drops it (its sender still
    /// holds the record and offers it again). The hold is bounded and lost on
    /// a crash; a graceful close hands it to the other owners. Default:
    /// `apply_remote_batch`, for a shard that holds nothing.
    fn apply_forwarded_batch(&self, recs: Vec<WireRecord>) -> BoxFuture<'_, ()> {
        self.apply_remote_batch(recs)
    }

    /// Drops the local copy of `key` iff `ver` is newer than the locally stored
    /// version. Not routed through [`ConflictResolver`]: an invalidation
    /// carries no value, so `Hlc` order is the only signal.
    fn invalidate(&self, key: Bytes, ver: Hlc) -> BoxFuture<'_, ()>;

    /// Serves a peer's [`crate::wire::Msg::Load`] for `keys`, encoded:
    /// loads them through this shard's loader, joining loads already in
    /// flight, and answers with the record stored for each key the cache
    /// or its source holds. Default: unavailable, for a shard with no
    /// loader.
    #[allow(
        private_interfaces,
        reason = "LoadServe is crate-private like the rest of the load path; \
                  see crate::net::RequestHandler::fetch"
    )]
    fn serve_load(&self, keys: Vec<Bytes>) -> BoxFuture<'_, crate::net::LoadServe> {
        let _ = keys;
        Box::pin(async { crate::net::LoadServe::Unavailable })
    }

    /// Takes a peer's [`crate::wire::Msg::RefreshHint`]: the peer read
    /// `key` at version `ver` past its refresh point. Default: ignores it.
    fn refresh_hint(&self, key: Bytes, ver: Hlc) -> BoxFuture<'_, ()> {
        let _ = (key, ver);
        Box::pin(async {})
    }

    /// This shard's current per-bucket XOR digests, `(bucket, digest)` for all
    /// [`BUCKET_COUNT`] buckets. The first step of an anti-entropy round.
    fn digests(&self) -> BoxFuture<'_, Vec<BucketDigest>>;

    /// [`ShardOps::digests`] narrowed to what one anti-entropy round with
    /// `peer` has to reconcile. The full list for every mode but
    /// `Mode::Distributed`, whose override keeps only the buckets `peer`
    /// owns in this shard's current view and this shard holds (owned or
    /// mid disown-grace): a bucket the peer does not own would be reported
    /// as a mismatch every round and its entries pushed only to be dropped
    /// by the peer's inbound guard.
    fn ae_digests_for(&self, peer: NodeId) -> BoxFuture<'_, Vec<BucketDigest>> {
        let _ = peer;
        self.digests()
    }

    /// A `Mode::Distributed` shard's scoped anti-entropy digests for `peer`,
    /// read under one ownership view: its hash and a `Scope` at its
    /// granularity. `None` for every other mode. The default reads
    /// [`ShardOps::ownership_view_hash`] and [`ShardOps::ae_digests_for`]
    /// as a bucket scope.
    #[allow(
        private_interfaces,
        reason = "ScopedDigests is only built and read inside this crate; see ShardOps::ownership_view"
    )]
    fn ae_scoped_digests_for(&self, peer: NodeId) -> BoxFuture<'_, Option<ScopedDigests>> {
        Box::pin(async move {
            let view_hash = self.ownership_view_hash()?;
            let ids = self
                .ae_digests_for(peer)
                .await
                .into_iter()
                .map(|bd| (bd.bucket, bd.digest))
                .collect();
            Some(ScopedDigests {
                view_hash,
                scope: Scope::Buckets(ids),
            })
        })
    }

    /// A [`KeyVersion`] for every live entry and un-GC'd tombstone in
    /// `bucket`, for a peer that reported a digest mismatch there.
    fn bucket_entries(&self, bucket: u16) -> BoxFuture<'_, Vec<KeyVersion>>;

    /// [`ShardOps::bucket_entries`] for many buckets in one pass, so an
    /// anti-entropy round stays linear in shard size instead of quadratic.
    fn entries_for_buckets(&self, buckets: Vec<u16>) -> BoxFuture<'_, BucketEntries>;

    /// The number of live entries plus un-GC'd tombstones in each of
    /// `buckets`, without materializing any of them: an anti-entropy
    /// responder's cheap check for whether a mismatched bucket passes
    /// [`crate::config::ClusterConfig::ae_part_min_bucket`] before it pays to
    /// build a listing or sketch. A bucket at or past [`BUCKET_COUNT`]
    /// answers `0`.
    fn bucket_lens(&self, buckets: Vec<u16>) -> BoxFuture<'_, Vec<BucketLen>>;

    /// Every bucket this shard holds at least one live entry or un-GC'd
    /// tombstone in, whether or not its current view owns the bucket:
    /// unlike [`ShardOps::bucket_lens`], residency never gates this list.
    /// `rebalance_task` folds the held buckets its new view does not own
    /// into the ones it releases, so a bucket owned only under a view it
    /// never observed still hands its data off. The default answers
    /// nothing, for a stub with no store behind it.
    fn held_buckets(&self) -> BoxFuture<'_, Vec<u16>> {
        Box::pin(async { Vec::new() })
    }

    /// [`ShardOps::held_buckets`] at part granularity: every part holding at
    /// least one live entry or un-GC'd tombstone, whether or not the current
    /// view owns it. The default names every part of each held bucket.
    fn held_parts(&self) -> BoxFuture<'_, Vec<PartId>> {
        Box::pin(async move {
            self.held_buckets()
                .await
                .into_iter()
                .flat_map(PartId::of_bucket)
                .collect()
        })
    }

    /// This shard's part digests for each of `buckets`, the second-level
    /// reply for a bucket whose digest mismatched and whose entry count
    /// passed [`crate::config::ClusterConfig::ae_part_min_bucket`].
    fn part_digests(&self, buckets: Vec<u16>) -> BoxFuture<'_, Vec<BucketPartDigests>>;

    /// [`ShardOps::entries_for_buckets`] at part granularity: a
    /// [`KeyVersion`] for every live entry and un-GC'd tombstone in each
    /// requested [`BucketPart`].
    fn entries_for_parts(&self, parts: Vec<BucketPart>) -> BoxFuture<'_, PartEntries>;

    /// The full [`WireRecord`] for each of `keys` this shard holds, present
    /// entries and tombstones alike, answering an `AePull`.
    fn records_for(&self, keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>>;

    /// Streams the shard's full contents in ~500-record chunks for state
    /// transfer to a joining node, applied on arrival through the same
    /// versioned [`ShardOps::apply_remote`] path as live traffic.
    fn snapshot_chunks(&self) -> BoxStream<'static, Vec<WireRecord>>;

    /// Garbage-collects tombstones past `tombstone_ttl`, counted from an
    /// instant all replicas agree on: a delete's version stamp, an expired
    /// entry's expiry. While `defer`, a delete past `tombstone_ttl` but not
    /// `tombstone_max_ttl` is left in place instead; the tombstone of an
    /// expired entry has its cap at its ttl deadline, so no deferral keeps it.
    /// On a `Replicated` or
    /// `Distributed` shard a call more than twice the gc period (a quarter of
    /// `tombstone_ttl`, at least a second) after the previous one defers too,
    /// as does every later call for `tombstone_ttl`: the shard did not run,
    /// and may hold values whose deletes it never saw. A tombstone that
    /// arrives from a peer already past the retention this shard applies is
    /// dropped on arrival, whatever the key holds.
    fn gc_tombstones(&self, defer: bool) -> BoxFuture<'_, ()>;

    /// Runs `engine::Engine::sweep`, since the engine has no free-running sweep
    /// of its own. Called periodically by `tombstone_gc_task`, independent
    /// of read/write traffic. In a `Replicated` or `Distributed` shard whose
    /// resolver does not merge, an entry the sweep removes for having expired
    /// leaves a tombstone at its version.
    fn run_pending_tasks(&self) -> BoxFuture<'_, ()>;

    /// Runs one rate-limited pass of the CRDT writer-retirement sweep over
    /// this shard's merging caches: reads up to `max_entries` live,
    /// resident candidates, asks the resolver to fold writers out of each
    /// via [`ConflictResolver::compact`], then applies every changed
    /// record with a version-gated direct replace, never the ordinary
    /// merge-based apply path (see
    /// `engine::Engine::compact_replace_if_current`'s doc for why).
    /// `now_ms`, `retire`, `quiet`, and `bounds` are exactly
    /// [`ConflictResolver::compact`]'s own parameters, threaded straight
    /// through; this method's job is the shard-level plumbing, never the
    /// retirement decision, which stays the resolver's call. A shard in
    /// [`crate::store::Mode::Invalidation`] retires a dead writer but never folds it,
    /// whatever `quiet` says: its nodes hold different writers, which a
    /// fold's merged total cannot add up.
    ///
    /// Returns every writer the scan found eligible for retirement,
    /// deduplicated across the batch, and how many records were
    /// compacted; the writer count can exceed the record count, since a
    /// bucket this shard doesn't own anymore is skipped without applying
    /// but its writer(s) are still reported. A no-op for a shard whose
    /// resolver never compacts (the common case for [`LwwResolver`]).
    /// Called periodically by `crdt_compact_task`.
    fn compact_pass(
        &self,
        now_ms: u64,
        retire: &dyn Fn(crdt::WriterId) -> bool,
        quiet: Quiescence,
        bounds: CompactionBounds,
        max_entries: usize,
    ) -> BoxFuture<'_, CompactPassOutcome> {
        let _ = (now_ms, retire, quiet, bounds, max_entries);
        Box::pin(async {
            CompactPassOutcome {
                retired: Vec::new(),
                compacted: 0,
                stripes_visited: BUCKET_COUNT,
            }
        })
    }

    /// Closes this shard's spill tier, if the `spill` feature is compiled in
    /// and one was attached via `CacheBuilder::spill`: stops accepting
    /// new spills and drops the flusher thread's channel sender. A no-op
    /// otherwise. This close writes no checkpoint, so the next open is cold
    /// regardless of `SpillConfig::warm_reopen`; use
    /// [`ShardOps::close_spill_checkpointed`] for a warm reopen.
    fn close_spill(&self) {}

    /// [`ShardOps::close_spill`], plus, with `SpillConfig::warm_reopen` on, a
    /// checkpoint of every live entry and a snapshot a later warm reopen
    /// replays. The default just calls `close_spill`, so an implementor with
    /// no spill tier keeps working, cold on the next open. Called by
    /// `crate::cache::Cache::close` and cluster shutdown. The `Shard` one runs
    /// once: a later or concurrent call waits for the first and does nothing
    /// more.
    fn close_spill_checkpointed(&self) -> BoxFuture<'_, ()> {
        self.close_spill();
        Box::pin(async {})
    }

    /// Stops accepting writes while keeping the fan-out backlog for the
    /// fan-out task's final drain: the first step of a cluster shutdown,
    /// before its tasks are cancelled. The folds still pending in a
    /// merge-coalesce window apply first, so they join that backlog. A write
    /// from here on fails with [`CacheError::Closed`] instead of being
    /// accepted and never sent.
    fn seal_fan_out(&self) {}

    /// This shard's current bucket-ownership view, for a `Mode::Distributed`
    /// cache that has one attached. `None` for every other mode, and for a
    /// `Distributed` cache before its tracker is attached.
    #[allow(
        private_interfaces,
        reason = "OwnershipView has no public fields or methods; only this crate can act on the returned value regardless of this trait's own visibility"
    )]
    fn ownership_view(&self) -> Option<Arc<OwnershipView>> {
        None
    }

    /// This shard's current bucket-ownership `view_hash`, for a
    /// `Mode::Distributed` cache that has one attached. `None` for every
    /// other mode, and for a `Distributed` cache before its tracker is
    /// attached. A thin projection of [`ShardOps::ownership_view`] for a
    /// caller that only needs the epoch check, not the whole view.
    fn ownership_view_hash(&self) -> Option<u64> {
        None
    }

    /// Removes every locally held entry, live or tombstone, in each of
    /// `buckets`, for a `Mode::Distributed` cache whose disown-grace has
    /// elapsed for them. A no-op returning `0` for every other mode, which
    /// never disowns a bucket in the first place. See
    /// `engine::Engine::release_buckets` for the accounting this resets.
    fn release_buckets(&self, buckets: &[u16]) -> BoxFuture<'_, u64> {
        let _ = buckets;
        Box::pin(async { 0 })
    }

    /// [`ShardOps::release_buckets`] for single parts: removes every locally
    /// held entry, live or tombstone, in each of `parts`, leaving the rest
    /// of each bucket untouched. A no-op returning `0` for a stub.
    fn release_parts(&self, parts: &[PartId]) -> BoxFuture<'_, u64> {
        let _ = parts;
        Box::pin(async { 0 })
    }

    /// Whether `part` is owned here but not yet pulled from a co-owner; see
    /// [`ShardOps::is_cold_bucket`]. `false` for a stub.
    fn is_cold_part(&self, part: PartId) -> bool {
        let _ = part;
        false
    }

    /// Whether `part` was warm-reloaded and not yet verified; see
    /// [`ShardOps::is_unverified_bucket`]. `false` for a stub.
    fn is_unverified_part(&self, part: PartId) -> bool {
        let _ = part;
        false
    }

    /// Whether `bucket` is owned by this `Mode::Distributed` shard but not
    /// yet pulled from a co-owner, so a local miss there is not an answer.
    /// `false` for every other mode.
    fn is_cold_bucket(&self, bucket: u16) -> bool {
        let _ = bucket;
        false
    }

    /// Whether `bucket` was warm-reloaded from spill and not yet verified
    /// against a co-owner: a local hit there is not trustworthy, since a
    /// co-owner may have deleted the record during this node's downtime.
    /// `false` for every other mode and for a bucket never warm-reloaded.
    fn is_unverified_bucket(&self, bucket: u16) -> bool {
        let _ = bucket;
        false
    }

    /// Narrows anti-entropy's `dirty`/`live` peer candidates to this
    /// shard's current cohort before `pick_peer`'s random choice runs.
    /// Identity for every mode but `Mode::Distributed`, whose override
    /// narrows both lists to peers sharing at least one bucket this shard
    /// currently owns or is mid disown-grace on: a peer sharing nothing
    /// with this shard has nothing to reconcile.
    fn ae_peer_filter(&self, dirty: Vec<NodeId>, live: Vec<NodeId>) -> (Vec<NodeId>, Vec<NodeId>) {
        (dirty, live)
    }

    /// Whether this shard's [`ConflictResolver`] merges, forwarded straight
    /// from [`ConflictResolver::merges`]. Compaction and the `Invalidation`
    /// wire read it; [`ShardOps::ae_exchanges_both`] carries the answer
    /// anti-entropy reads. `false` for every shard without a resolver set,
    /// since [`LwwResolver::merges`] is `false`.
    fn merges(&self) -> bool {
        false
    }

    /// Whether anti-entropy sends both records of a key present on both
    /// sides under different versions, as `exchanges_both_records` decides
    /// from the shard's [`ConflictResolver`]: it is `true` when the resolver
    /// merges or reads values (`needs_value_bytes`), the only resolvers that
    /// can prefer the lower version. `cluster::anti_entropy` reads this once
    /// per round and, when `true`, has `diff_bucket`/`diff_decoded` queue
    /// such a key for both push and pull instead of only pushing the greater
    /// side to the lesser one, so a replica holding the winner at the lower
    /// version sends it, and two replicas each holding half of a merge
    /// exchange records in the same round. The pulls run first, and a key
    /// that lands at exactly the peer's version is not pushed back. `false`
    /// for every shard without a resolver set. A wrapper around a
    /// `ShardOps` forwards this beside [`ShardOps::merges`]; the default
    /// would silently make the wrapped shard repair one way.
    fn ae_exchanges_both(&self) -> bool {
        false
    }
}

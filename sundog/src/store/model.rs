//! The reference model for stateful fuzzing of the apply path (see
//! `super`'s docs on this module). [`Model`] is a sequential,
//! single-threaded last-writer-wins map keyed by `u8` — a small key space
//! forces version conflicts between concurrent origins — that reimplements
//! [`Shard::apply`]'s versioned-write rule, [`ShardOps::gc_tombstones`]'s
//! retention rule, and TTL expiry from scratch, independently of
//! `engine::Engine`, so a divergence between the two is a real bug rather
//! than the model quietly agreeing with its own implementation.
//!
//! [`Op`] is the `Arbitrary` operation vocabulary both the fuzz targets and
//! the in-crate property test generate sequences of; [`run`] is the one
//! driver that applies a sequence to a live [`Shard`] and its paired
//! [`Model`] side by side, asserting after every op that they agree.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use smol_str::SmolStr;

use crate::hlc::{Hlc, HlcClock};
use crate::node::NodeId;
use crate::wire::WireRecord;

use super::engine::{hash_key_bytes, stripe_index_from_hash};
use super::{BUCKET_COUNT, Mode, Shard, ShardOps, entry_fingerprint};

/// Tombstone retention configured on every [`new_shard_and_model`] pair —
/// short enough that [`Op::AdvanceClock`]'s `u16`-millisecond range
/// routinely crosses both deadlines within one generated op sequence.
pub const TOMBSTONE_TTL_MS: u64 = 200;
/// Hard cap on tombstone retention configured on every [`new_shard_and_model`]
/// pair — see [`TOMBSTONE_TTL_MS`].
pub const TOMBSTONE_MAX_TTL_MS: u64 = 2_000;
/// The manual clock's starting value: an arbitrary epoch-ms-shaped baseline,
/// matching the rest of the store's clock-driven tests.
pub const START_CLOCK_MS: u64 = 1_000_000;

/// One entry the model holds for a key: either a live value or a tombstone,
/// each carrying the same bookkeeping [`super::Stored`]/[`super::Tombstone`]
/// do — see [`super::ShardOps::gc_tombstones`]'s docs for the two tombstone
/// deadlines' meaning.
#[derive(Debug, Clone)]
enum ModelEntry {
    Live {
        ver: Hlc,
        value: u8,
        expires_at_ms: Option<u64>,
    },
    Tombstone {
        ver: Hlc,
        ttl_deadline_ms: u64,
        max_deadline_ms: u64,
    },
}

/// The reference model itself: see this module's docs.
pub struct Model {
    entries: HashMap<u8, ModelEntry>,
    /// Shared with the paired [`Shard`] via [`Model::clock_fn`] — advancing
    /// this is advancing the shard's own notion of time, with no separate
    /// synchronization step needed.
    clock: Arc<AtomicU64>,
    /// Mirrors the paired [`Shard`]'s own internal `HlcClock` exactly: every
    /// event that touches the shard's clock (a local stamp, an observed
    /// remote version) touches this one identically, in the same order, so
    /// [`Model::stamp_local`] always predicts the version the shard is
    /// about to stamp rather than needing to read it back afterward — see
    /// [`Op::LocalInsert`]'s docs for why prediction, not read-back, is the
    /// only sound option for a write whose TTL makes it dead on arrival.
    hlc_clock: HlcClock,
    tombstone_ttl_ms: u64,
    tombstone_max_ttl_ms: u64,
}

impl Model {
    /// Builds an empty model, its clock starting at [`START_CLOCK_MS`] and
    /// its mirrored [`HlcClock`] seeded with the same `node` the paired
    /// [`Shard`] stamps local writes with.
    #[must_use]
    pub fn new(node: NodeId, tombstone_ttl_ms: u64, tombstone_max_ttl_ms: u64) -> Self {
        Self {
            entries: HashMap::new(),
            clock: Arc::new(AtomicU64::new(START_CLOCK_MS)),
            hlc_clock: HlcClock::new(node),
            tombstone_ttl_ms,
            tombstone_max_ttl_ms,
        }
    }

    /// The clock-reading closure to install via [`Shard::with_clock`] so the
    /// paired shard and this model always agree on "now" — [`Model`] is the
    /// clock's sole owner; [`Model::advance_clock`] is the only way to move
    /// it forward.
    #[must_use]
    pub fn clock_fn(&self) -> Arc<dyn Fn() -> u64 + Send + Sync> {
        let clock = Arc::clone(&self.clock);
        Arc::new(move || clock.load(Ordering::Relaxed))
    }

    /// The model's current clock reading.
    #[must_use]
    pub fn now_ms(&self) -> u64 {
        self.clock.load(Ordering::Relaxed)
    }

    /// Moves the shared clock forward by `ms`.
    pub fn advance_clock(&mut self, ms: u64) {
        self.clock.fetch_add(ms, Ordering::Relaxed);
    }

    /// Predicts the [`Hlc`] the paired shard's own clock is about to stamp
    /// a local write with — the standard HLC "send" rule
    /// ([`HlcClock::now`]), run against [`Model::hlc_clock`] instead of the
    /// shard's, at the same clock reading. Sound only as long as every
    /// event that reaches the shard's clock also reaches this one, in the
    /// same order — [`run`]'s driver's whole job.
    fn stamp_local(&mut self) -> Hlc {
        let now = self.now_ms();
        self.hlc_clock.now(now)
    }

    /// Mirrors the paired shard's `observe_remote`: folds an inbound
    /// remote version into [`Model::hlc_clock`] so a later
    /// [`Model::stamp_local`] stays causally after it, exactly as the
    /// shard's own local stamps do.
    fn observe_remote(&mut self, remote: Hlc) {
        let now = self.now_ms();
        self.hlc_clock.observe(now, remote);
    }

    /// The versioned-apply core: the same "equal version is a no-op,
    /// otherwise the newer [`Hlc`] wins" rule as [`Shard::apply`] with the
    /// default resolver. `expires_at_ms` is the record's own absolute
    /// deadline, exactly as carried on the wire.
    pub fn apply(&mut self, key: u8, ver: Hlc, value: u8, expires_at_ms: Option<u64>) {
        if self.incoming_loses(key, ver) {
            return;
        }
        self.entries.insert(
            key,
            ModelEntry::Live {
                ver,
                value,
                expires_at_ms,
            },
        );
    }

    /// A versioned tombstone write — the deletion counterpart of
    /// [`Model::apply`], under the identical version rule. Its two GC
    /// deadlines are stamped from the model's current clock, exactly as
    /// `engine::apply_tombstone` stamps them from the engine's.
    pub fn remove(&mut self, key: u8, ver: Hlc) {
        if self.incoming_loses(key, ver) {
            return;
        }
        let now = self.now_ms();
        self.entries.insert(
            key,
            ModelEntry::Tombstone {
                ver,
                ttl_deadline_ms: now.saturating_add(self.tombstone_ttl_ms),
                max_deadline_ms: now.saturating_add(self.tombstone_max_ttl_ms),
            },
        );
    }

    /// Whether an incoming write at `ver` loses to whatever is currently
    /// stored at `key` — equal versions always lose (a no-op), matching
    /// [`Shard::apply`]'s tie rule; otherwise the higher [`Hlc`] wins.
    fn incoming_loses(&self, key: u8, ver: Hlc) -> bool {
        self.stored_ver(key).is_some_and(|sv| sv >= ver)
    }

    fn stored_ver(&self, key: u8) -> Option<Hlc> {
        match self.entries.get(&key)? {
            ModelEntry::Live { ver, .. } | ModelEntry::Tombstone { ver, .. } => Some(*ver),
        }
    }

    /// [`super::engine::Engine::invalidate`]'s rule: drops a *live* entry
    /// iff `ver` is newer than it, writing no tombstone. A no-op against a
    /// tombstone or an absent key, regardless of `ver` — an invalidation
    /// carries no value to arbitrate with what a tombstone already recorded.
    pub fn invalidate(&mut self, key: u8, ver: Hlc) {
        let should_drop = matches!(
            self.entries.get(&key),
            Some(ModelEntry::Live { ver: sv, .. }) if ver > *sv
        );
        if should_drop {
            self.entries.remove(&key);
        }
    }

    /// [`super::ShardOps::gc_tombstones`]'s rule: drops tombstones past
    /// `tombstone_ttl` and, while `any_member_absent`, already past
    /// `tombstone_max_ttl` too.
    pub fn gc(&mut self, any_member_absent: bool) {
        let now = self.now_ms();
        self.entries.retain(|_, entry| match entry {
            ModelEntry::Tombstone {
                ttl_deadline_ms,
                max_deadline_ms,
                ..
            } => {
                let past_ttl = now >= *ttl_deadline_ms;
                let past_max = now >= *max_deadline_ms;
                !(past_ttl && (!any_member_absent || past_max))
            }
            ModelEntry::Live { .. } => true,
        });
    }

    /// [`super::engine::Engine::sweep`]'s rule for live entries: a live
    /// entry past its own `expires_at_ms` is dropped outright, keeping
    /// neither its version nor a tombstone in its place.
    pub fn sweep(&mut self) {
        let now = self.now_ms();
        self.entries.retain(|_, entry| match entry {
            ModelEntry::Live { expires_at_ms, .. } => expires_at_ms.is_none_or(|exp| now < exp),
            ModelEntry::Tombstone { .. } => true,
        });
    }

    /// What a read of `key` should see right now: `None` for an absent key,
    /// a tombstone, or a live entry already past its own deadline — even
    /// before the next [`Model::sweep`] — exactly as
    /// `engine::Engine::is_absent` hides an expired-but-unswept entry from
    /// [`Shard::get`].
    #[must_use]
    pub fn visible(&self, key: u8) -> Option<u8> {
        match self.entries.get(&key)? {
            ModelEntry::Live {
                value,
                expires_at_ms,
                ..
            } if expires_at_ms.is_none_or(|exp| self.now_ms() < exp) => Some(*value),
            _ => None,
        }
    }

    /// The `(key_bytes, ver)` set the digest should cover: every tombstone
    /// still tracked, plus every live entry not currently expired — the same
    /// filter [`super::engine::Engine::collect_buckets`] applies.
    #[must_use]
    pub fn entries(&self) -> Vec<(Bytes, Hlc)> {
        let now = self.now_ms();
        self.entries
            .iter()
            .filter_map(|(&key, entry)| match entry {
                ModelEntry::Live {
                    ver, expires_at_ms, ..
                } => expires_at_ms
                    .is_none_or(|exp| now < exp)
                    .then(|| (key_bytes(key), *ver)),
                ModelEntry::Tombstone { ver, .. } => Some((key_bytes(key), *ver)),
            })
            .collect()
    }
}

/// Postcard-encodes a `u8` key — the same bytes [`Shard`]'s own
/// `encode_key` produces for a `u8`.
fn key_bytes(key: u8) -> Bytes {
    Bytes::from(postcard::to_stdvec(&key).expect("invariant: u8 always postcard-encodes"))
}

fn value_bytes(value: u8) -> Bytes {
    Bytes::from(postcard::to_stdvec(&value).expect("invariant: u8 always postcard-encodes"))
}

/// Clamps a fuzz-generated byte to node ids `1..=4` — `RemoteApply`'s and
/// `Invalidate`'s `node` field never stamps node `0`, kept free so it can't
/// collide with a real cluster member id in a caller that composes this
/// model with other fixtures.
fn clamp_node(node: u8) -> NodeId {
    NodeId::from(u64::from(node % 4) + 1)
}

/// Builds the [`Hlc`] a `RemoteApply`/`RemoteBatch`/`Invalidate` op stamps,
/// relative to `now_ms` — see [`Op::RemoteApply`]'s docs for why offsets are
/// relative rather than absolute.
fn remote_hlc(now_ms: u64, wall_ms_offset: i16, logical: u8, node: u8) -> Hlc {
    Hlc {
        wall_ms: now_ms.saturating_add_signed(i64::from(wall_ms_offset)),
        logical: u32::from(logical),
        node: clamp_node(node),
    }
}

fn remote_expiry(now_ms: u64, expires_offset_ms: Option<i16>) -> Option<u64> {
    expires_offset_ms.map(|offset| now_ms.saturating_add_signed(i64::from(offset)))
}

/// One `RemoteApply`-shaped record inside an [`Op::RemoteBatch`] — same
/// fields as [`Op::RemoteApply`], factored into its own type only because a
/// struct-variant's fields can't be reused as a standalone type for `Vec<_>`.
#[derive(Debug, Clone, arbitrary::Arbitrary)]
pub struct RemoteRecord {
    /// The key this record targets.
    pub key: u8,
    /// The record's value, or `None` for a tombstone.
    pub value: Option<u8>,
    /// Offset from the current clock reading, for [`Hlc::wall_ms`] — signed,
    /// so a generated batch lands records both before and after "now".
    pub wall_ms_offset: i16,
    /// The record's [`Hlc::logical`] tiebreaker.
    pub logical: u8,
    /// The stamping node, clamped to `1..=4` by [`clamp_node`].
    pub node: u8,
    /// Offset from the current clock reading for `expires_at_ms`, or `None`
    /// for no TTL.
    pub expires_offset_ms: Option<i16>,
}

/// The `Arbitrary`, coverage-guided operation vocabulary a stateful apply-path
/// fuzz run is a sequence of: local writes stamped by the shard's own HLC,
/// remote applies and batches carrying their own version and expiry relative
/// to "now", invalidations, tombstone GC, a sweep, and a manual clock
/// advance.
#[derive(Debug, Clone, arbitrary::Arbitrary)]
pub enum Op {
    /// [`Shard::insert`] or [`Shard::insert_with_ttl`] — stamped by the
    /// shard's own [`crate::hlc::HlcClock`]. [`run`]'s driver predicts the
    /// stamp via [`Model::stamp_local`] rather than reading it back
    /// afterward: [`ShardOps::records_for`] filters an entry that's already
    /// past its own `expires_at_ms` (`engine::Engine::is_absent`, the same
    /// filter [`Shard::get`] applies), so a write whose `ttl_ms` makes it
    /// dead on arrival — a real, reachable case, not an edge case reads
    /// alone would ever surface — would be invisible to a read-back
    /// immediately after writing it, silently leaving the model on stale
    /// data for that key.
    LocalInsert {
        /// The key to write.
        key: u8,
        /// The value to write.
        value: u8,
        /// A per-write TTL in milliseconds, or `None` for the shard's
        /// default (none — [`new_shard_and_model`] configures no default
        /// TTL).
        ttl_ms: Option<u16>,
    },
    /// [`Shard::remove`] — a local tombstone write, its version likewise
    /// predicted via [`Model::stamp_local`].
    LocalRemove {
        /// The key to tombstone.
        key: u8,
    },
    /// [`ShardOps::apply_remote`]: one record from a simulated peer, its
    /// version and expiry offset from the current clock so generated
    /// records land near, before, and after "now".
    RemoteApply {
        /// The key this record targets.
        key: u8,
        /// The record's value, or `None` for a tombstone.
        value: Option<u8>,
        /// Offset from the current clock reading, for [`Hlc::wall_ms`].
        wall_ms_offset: i16,
        /// The record's [`Hlc::logical`] tiebreaker.
        logical: u8,
        /// The stamping node, clamped to `1..=4`.
        node: u8,
        /// Offset from the current clock reading for `expires_at_ms`, or
        /// `None` for no TTL.
        expires_offset_ms: Option<i16>,
    },
    /// [`ShardOps::apply_remote_batch`]: many records applied under one
    /// lock acquisition per touched stripe — capped to the first 16 of
    /// whatever `arbitrary` generates, matching a realistic coalesced batch
    /// size rather than an unbounded one.
    RemoteBatch(Vec<RemoteRecord>),
    /// [`ShardOps::invalidate`]: an invalidation with no value of its own.
    Invalidate {
        /// The key to invalidate.
        key: u8,
        /// Offset from the current clock reading, for [`Hlc::wall_ms`].
        wall_ms_offset: i16,
        /// The invalidation's [`Hlc::logical`] tiebreaker.
        logical: u8,
        /// The stamping node, clamped to `1..=4`.
        node: u8,
    },
    /// [`ShardOps::gc_tombstones`].
    Gc {
        /// Whether to defer past-`tombstone_ttl` (but not yet
        /// past-`tombstone_max_ttl`) tombstones.
        any_member_absent: bool,
    },
    /// [`ShardOps::run_pending_tasks`] — the engine's explicit sweep.
    Sweep,
    /// Moves the shared manual clock forward.
    AdvanceClock {
        /// Milliseconds to advance by.
        ms: u16,
    },
}

/// Caps a generated batch to its first 16 entries, matching the shard-side
/// coalescing this stands in for — see [`Op::RemoteBatch`]'s docs.
const REMOTE_BATCH_CAP: usize = 16;

/// Builds a fresh `Shard<u8, u8>` and its paired [`Model`], wired to the
/// same manual clock via [`Shard::with_clock`] and the same tombstone
/// retention — the harness every fuzz target and the in-crate property test
/// share. Capacity is unbounded: sampled-LRU eviction is outside what this
/// model reimplements.
#[must_use]
pub fn new_shard_and_model(name: &str, node: u64) -> (Shard<u8, u8>, Model) {
    let node = NodeId::from(node);
    let model = Model::new(node, TOMBSTONE_TTL_MS, TOMBSTONE_MAX_TTL_MS);
    let shard = Shard::<u8, u8>::new(
        SmolStr::new(name),
        Mode::Replicated,
        node,
        u64::MAX,
        None,
        None,
    )
    .with_tombstone_ttl(Duration::from_millis(TOMBSTONE_TTL_MS))
    .with_tombstone_max_ttl(Duration::from_millis(TOMBSTONE_MAX_TTL_MS))
    .with_clock(model.clock_fn());
    (shard, model)
}

/// Builds the [`WireRecord`] a `RemoteRecord` stamps at `now_ms` — the same
/// conversion [`run`]'s driver applies for [`Op::RemoteApply`] and
/// [`Op::RemoteBatch`], exposed so `sundog-fuzz`'s `apply_permutation`
/// target (which builds and applies [`WireRecord`]s directly, outside
/// [`run`]'s driver) matches it exactly.
#[must_use]
pub fn remote_wire_record(record: &RemoteRecord, now_ms: u64) -> WireRecord {
    WireRecord {
        key: key_bytes(record.key),
        value: record.value.map(value_bytes),
        ver: remote_hlc(now_ms, record.wall_ms_offset, record.logical, record.node),
        expires_at_ms: remote_expiry(now_ms, record.expires_offset_ms),
    }
}

/// Mirrors one remote record's effect on [`Model::hlc_clock`]
/// ([`Model::observe_remote`]) and then applies it exactly as
/// [`Shard::apply`]'s default resolver would.
fn apply_remote_to_model(model: &mut Model, record: &RemoteRecord, now_ms: u64) {
    let ver = remote_hlc(now_ms, record.wall_ms_offset, record.logical, record.node);
    model.observe_remote(ver);
    match record.value {
        Some(v) => model.apply(
            record.key,
            ver,
            v,
            remote_expiry(now_ms, record.expires_offset_ms),
        ),
        None => model.remove(record.key, ver),
    }
}

/// Applies one [`Op`] to both `shard` and `model`.
fn apply_op(op: &Op, shard: &Shard<u8, u8>, model: &mut Model) {
    match op {
        Op::LocalInsert { key, value, ttl_ms } => {
            let now = model.now_ms();
            let expires_at_ms = ttl_ms.map(|ms| now.saturating_add(u64::from(ms)));
            let ver = model.stamp_local();
            let _ = match ttl_ms {
                Some(ms) => futures::executor::block_on(shard.insert_with_ttl(
                    *key,
                    *value,
                    Duration::from_millis(u64::from(*ms)),
                )),
                None => futures::executor::block_on(shard.insert(*key, *value)),
            };
            model.apply(*key, ver, *value, expires_at_ms);
        }
        Op::LocalRemove { key } => {
            let ver = model.stamp_local();
            let _ = futures::executor::block_on(shard.remove(key));
            model.remove(*key, ver);
        }
        Op::RemoteApply {
            key,
            value,
            wall_ms_offset,
            logical,
            node,
            expires_offset_ms,
        } => {
            let now = model.now_ms();
            let record = RemoteRecord {
                key: *key,
                value: *value,
                wall_ms_offset: *wall_ms_offset,
                logical: *logical,
                node: *node,
                expires_offset_ms: *expires_offset_ms,
            };
            let rec = remote_wire_record(&record, now);
            futures::executor::block_on(ShardOps::apply_remote(shard, rec));
            apply_remote_to_model(model, &record, now);
        }
        Op::RemoteBatch(records) => {
            let now = model.now_ms();
            let capped = &records[..records.len().min(REMOTE_BATCH_CAP)];
            let wire_records: Vec<WireRecord> =
                capped.iter().map(|r| remote_wire_record(r, now)).collect();
            futures::executor::block_on(ShardOps::apply_remote_batch(shard, wire_records));
            for r in capped {
                apply_remote_to_model(model, r, now);
            }
        }
        Op::Invalidate {
            key,
            wall_ms_offset,
            logical,
            node,
        } => {
            let now = model.now_ms();
            let ver = remote_hlc(now, *wall_ms_offset, *logical, *node);
            futures::executor::block_on(ShardOps::invalidate(shard, key_bytes(*key), ver));
            model.observe_remote(ver);
            model.invalidate(*key, ver);
        }
        Op::Gc { any_member_absent } => {
            futures::executor::block_on(ShardOps::gc_tombstones(shard, *any_member_absent));
            model.gc(*any_member_absent);
        }
        Op::Sweep => {
            futures::executor::block_on(ShardOps::run_pending_tasks(shard));
            model.sweep();
        }
        Op::AdvanceClock { ms } => {
            model.advance_clock(u64::from(*ms));
        }
    }
}

/// Invariants (a) and (c): every key's read matches [`Model::visible`], and
/// no key that reads as present carries a deadline at or before the clock.
fn assert_reads_match_model(shard: &Shard<u8, u8>, model: &Model) {
    let now = model.now_ms();
    for key in 0u8..=255 {
        let got = futures::executor::block_on(shard.get(&key));
        assert_eq!(
            got,
            model.visible(key),
            "shard.get({key}) = {got:?} diverged from model.visible({key})"
        );
        if got.is_none() {
            continue;
        }
        let recs = futures::executor::block_on(ShardOps::records_for(shard, vec![key_bytes(key)]));
        if let Some(deadline) = recs.first().and_then(|rec| rec.expires_at_ms) {
            assert!(
                deadline > now,
                "key {key} reads as present but its record's deadline {deadline} is not \
                 after now ({now})"
            );
        }
    }
}

/// Invariant (b), checked after every [`Op::Sweep`]: [`ShardOps::digests`]
/// equals the XOR of [`entry_fingerprint`] over [`Model::entries`], and
/// [`ShardOps::entries_for_buckets`] over all [`BUCKET_COUNT`] buckets
/// equals [`Model::entries`] as sets.
fn assert_digest_and_entries_match_model(shard: &Shard<u8, u8>, model: &Model) {
    let model_entries: HashSet<(Bytes, Hlc)> = model.entries().into_iter().collect();

    let mut expected_digest = vec![0u64; BUCKET_COUNT];
    for (key_bytes, ver) in &model_entries {
        let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes));
        expected_digest[bucket] ^= entry_fingerprint(key_bytes, *ver);
    }
    let actual_digest: Vec<u64> = futures::executor::block_on(ShardOps::digests(shard))
        .into_iter()
        .map(|(_, digest)| digest)
        .collect();
    assert_eq!(
        actual_digest, expected_digest,
        "digest diverged from the XOR of entry_fingerprint over the model's entry set"
    );

    let all_buckets: Vec<u16> = (0..u16::try_from(BUCKET_COUNT).expect("fits")).collect();
    let shard_entries: HashSet<(Bytes, Hlc)> =
        futures::executor::block_on(ShardOps::entries_for_buckets(shard, all_buckets))
            .into_iter()
            .flat_map(|(_, entries)| entries)
            .collect();
    assert_eq!(
        shard_entries, model_entries,
        "entries_for_buckets(all buckets) diverged from the model's entry set"
    );
}

/// Applies `ops` to `shard` and `model` side by side, one op at a time,
/// asserting after every op that the shard's observable state matches the
/// model's — the driver every stateful apply-path fuzz target and the
/// in-crate property test share.
///
/// # Panics
///
/// Panics on the first divergence between `shard` and `model` — that
/// divergence is exactly what this function exists to catch.
pub fn run(ops: &[Op], shard: &Shard<u8, u8>, model: &mut Model) {
    for op in ops {
        apply_op(op, shard, model);
        assert_reads_match_model(shard, model);
        if matches!(op, Op::Sweep) {
            assert_digest_and_entries_match_model(shard, model);
        }
    }
}

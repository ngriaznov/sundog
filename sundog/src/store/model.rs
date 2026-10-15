//! `Model` is a sequential last-writer-wins map over `u8` keys that
//! reimplements `Shard::apply`'s versioned-write rule,
//! `ShardOps::gc_tombstones`'s retention rule, and TTL expiry without
//! touching `engine::Engine`. A divergence between the two is a bug.
//!
//! `Op` is the `Arbitrary` vocabulary `run` applies to a shard and its
//! model side by side, asserting agreement after every step.

use std::collections::{HashMap, HashSet};
use std::num::NonZeroU8;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use smol_str::SmolStr;

use crate::hlc::{Hlc, HlcClock};
use crate::node::NodeId;
use crate::ownership::{OwnershipTracker, OwnershipView, ResidencySet};
use crate::wire::WireRecord;

use super::engine::{hash_key_bytes, stripe_index_from_hash};
use super::{BUCKET_COUNT, Mode, PartId, Shard, ShardOps, entry_fingerprint};

/// The anti-entropy bucket a model key hashes into, mirroring `Shard`'s own
/// key encoding. `pub` so the out-of-workspace fuzz crate can group
/// generated keys by bucket without reaching into `store::engine`.
#[must_use]
pub fn bucket_of(key: u8) -> u16 {
    u16::try_from(stripe_index_from_hash(hash_key_bytes(
        key_bytes(key).as_ref(),
    )))
    .expect("invariant: BUCKET_COUNT fits u16")
}

/// The ownership part a model key hashes into.
#[must_use]
pub fn part_of(key: u8) -> PartId {
    PartId::from_hash(hash_key_bytes(key_bytes(key).as_ref()))
}

/// Decodes a model key back from the wire bytes [`ShardOps::entries_for_buckets`]
/// et al. report it as, `None` on anything that isn't one of this model's own
/// `key_bytes(k)` encodings. `pub` so the out-of-workspace fuzz crate can
/// decode an observed key without a direct `postcard` dependency of its own.
#[must_use]
pub fn key_from_bytes(bytes: &[u8]) -> Option<u8> {
    postcard::from_bytes(bytes).ok()
}

/// Tombstone retention for every [`new_shard_and_model`] pair. Short
/// enough that [`Op::AdvanceClock`] routinely crosses both deadlines.
pub const TOMBSTONE_TTL_MS: u64 = 200;
/// Hard cap on tombstone retention for every [`new_shard_and_model`] pair.
pub const TOMBSTONE_MAX_TTL_MS: u64 = 2_000;
/// The clock-skew bound for every [`new_shard_and_model`] pair: inside the
/// roughly 32-second reach of a remote op's `i16` wall-clock offset, so a
/// fuzz run sends records both inside and past it.
pub const MAX_CLOCK_SKEW_MS: u64 = 20_000;
/// The manual clock's starting value, an arbitrary epoch-ms baseline.
pub const START_CLOCK_MS: u64 = 1_000_000;

/// One entry the model holds for a key: a live value or a tombstone,
/// carrying the same bookkeeping the engine's own `Live` and
/// [`super::Tombstone`] carry.
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

/// The reference model: entries keyed by `u8`, mirroring one [`Shard`].
pub struct Model {
    entries: HashMap<u8, ModelEntry>,
    /// Shared with the paired [`Shard`] via [`Model::clock_fn`]; advancing
    /// it advances the shard's clock too.
    clock: Arc<AtomicU64>,
    /// Mirrors the paired [`Shard`]'s internal `HlcClock`. Every clock event
    /// that touches the shard's clock touches this one identically, so
    /// [`Model::stamp_local`] predicts the shard's next stamp exactly.
    hlc_clock: HlcClock,
    tombstone_ttl_ms: u64,
    tombstone_max_ttl_ms: u64,
    /// What the last [`Model::gc`] was told about whether collection
    /// defers; `true` until the first one, as a new `Replicated` or
    /// `Distributed` shard starts.
    gc_deferring: bool,
    /// `Some` only for [`new_shard_and_model_with_ownership`]'s pair: the
    /// same view attached to the paired `Shard`, so [`Model::owns`] answers
    /// exactly what that shard's own inbound/write guards would. `None`
    /// (every other constructor) means "every node owns everything," the
    /// default that keeps every fuzz target and property test written
    /// before this axis existed behaving byte-for-byte the same.
    ownership: Option<Arc<OwnershipView>>,
}

impl Model {
    /// Builds an empty model with its clock at [`START_CLOCK_MS`] and its
    /// [`HlcClock`] seeded with `node`, the same node the shard stamps with.
    /// The model starts with collection deferring, as the `Replicated` and
    /// `Distributed` shards it pairs with do, until the first [`Model::gc`].
    #[must_use]
    pub fn new(node: NodeId, tombstone_ttl_ms: u64, tombstone_max_ttl_ms: u64) -> Self {
        Self {
            entries: HashMap::new(),
            clock: Arc::new(AtomicU64::new(START_CLOCK_MS)),
            hlc_clock: HlcClock::new(node),
            tombstone_ttl_ms,
            tombstone_max_ttl_ms,
            gc_deferring: true,
            ownership: None,
        }
    }

    /// Whether this model's attached ownership view (if any) owns `key`'s
    /// part. `true` unconditionally when no view is attached, the default
    /// for every constructor but [`new_shard_and_model_with_ownership`].
    #[must_use]
    pub fn owns(&self, key: u8) -> bool {
        self.ownership
            .as_ref()
            .is_none_or(|view| view.owns(part_of(key)))
    }

    /// The clock-reading closure to install via [`Shard::with_clock`], so
    /// the paired shard and this model always agree on "now".
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

    /// Predicts the [`Hlc`] the paired shard's clock is about to stamp a
    /// local write with, run against [`Model::hlc_clock`] at the same
    /// reading. Sound only as long as every clock event reaches both
    /// clocks in the same order.
    fn stamp_local(&mut self) -> Hlc {
        let now = self.now_ms();
        self.hlc_clock.now(now)
    }

    /// Mirrors the paired shard's `observe_remote`: folds a remote version
    /// into [`Model::hlc_clock`] so a later stamp stays causally after it,
    /// or refuses one stamped more than [`MAX_CLOCK_SKEW_MS`] ahead and
    /// returns `false`, leaving the clock alone, as the shard does.
    fn observe_remote(&mut self, remote: Hlc) -> bool {
        let now = self.now_ms();
        self.hlc_clock
            .observe_bounded(now, remote, MAX_CLOCK_SKEW_MS)
            .is_some()
    }

    /// The versioned-apply core: the newer [`Hlc`] wins, an equal version
    /// is a no-op, matching [`Shard::apply`]'s default resolver.
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

    /// A versioned tombstone write under the same version rule as
    /// [`Model::apply`], its two GC deadlines stamped from the clock. A
    /// delete counts them from now. The tombstone of an expired entry
    /// (`expired_at_ms` `Some`) counts them from that expiry, capped at now,
    /// and is dropped on arrival, whatever the key holds, when its retention
    /// has already lapsed: the `ttl` deadline, or the `max` one while the
    /// last [`Model::gc`] deferred.
    pub fn remove(&mut self, key: u8, ver: Hlc, expired_at_ms: Option<u64>) {
        if self.incoming_loses(key, ver) {
            return;
        }
        let now = self.now_ms();
        let anchor = expired_at_ms.map_or(now, |expired| expired.min(now));
        let ttl_deadline_ms = anchor.saturating_add(self.tombstone_ttl_ms);
        let max_deadline_ms = anchor.saturating_add(self.tombstone_max_ttl_ms);
        let horizon = if self.gc_deferring {
            max_deadline_ms
        } else {
            ttl_deadline_ms
        };
        if expired_at_ms.is_some() && now >= horizon {
            return;
        }
        self.entries.insert(
            key,
            ModelEntry::Tombstone {
                ver,
                ttl_deadline_ms,
                max_deadline_ms,
            },
        );
    }

    /// Whether an incoming write at `ver` loses to what is stored at `key`.
    /// Equal versions always lose, matching [`Shard::apply`]'s tie rule.
    fn incoming_loses(&self, key: u8, ver: Hlc) -> bool {
        self.stored_ver(key).is_some_and(|sv| sv >= ver)
    }

    fn stored_ver(&self, key: u8) -> Option<Hlc> {
        match self.entries.get(&key)? {
            ModelEntry::Live { ver, .. } | ModelEntry::Tombstone { ver, .. } => Some(*ver),
        }
    }

    /// [`super::engine::Engine::invalidate`]'s rule: drops a live entry only
    /// if `ver` is newer, writing no tombstone. A no-op against a tombstone.
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
    /// `tombstone_ttl`, and, while `defer`, past `tombstone_max_ttl` instead.
    pub fn gc(&mut self, defer: bool) {
        self.gc_deferring = defer;
        let now = self.now_ms();
        self.entries.retain(|_, entry| match entry {
            ModelEntry::Tombstone {
                ttl_deadline_ms,
                max_deadline_ms,
                ..
            } => {
                let past_ttl = now >= *ttl_deadline_ms;
                let past_max = now >= *max_deadline_ms;
                !(past_ttl && (!defer || past_max))
            }
            ModelEntry::Live { .. } => true,
        });
    }

    /// [`super::engine::Engine::sweep`]'s rule: a live entry past its own
    /// `expires_at_ms` is replaced by a tombstone at its version, anchored
    /// on that expiry: its deadlines count a retention from the expiry and
    /// are never earlier than now.
    pub fn sweep(&mut self) {
        let now = self.now_ms();
        let (ttl, max) = (self.tombstone_ttl_ms, self.tombstone_max_ttl_ms);
        for entry in self.entries.values_mut() {
            if let ModelEntry::Live {
                ver,
                expires_at_ms: Some(expired_at_ms),
                ..
            } = *entry
                && now >= expired_at_ms
            {
                let anchor = expired_at_ms.min(now);
                *entry = ModelEntry::Tombstone {
                    ver,
                    ttl_deadline_ms: anchor.saturating_add(ttl).max(now),
                    max_deadline_ms: anchor.saturating_add(max).max(now),
                };
            }
        }
    }

    /// What a read of `key` sees now: `None` for an absent key, a
    /// tombstone, or a live entry already past its deadline.
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

    /// The `(key_bytes, ver)` set the digest covers: every tombstone plus
    /// every unexpired live entry, the same filter `collect_buckets` applies.
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

/// Postcard-encodes a `u8` key, matching [`Shard`]'s own `encode_key`.
fn key_bytes(key: u8) -> Bytes {
    Bytes::from(postcard::to_stdvec(&key).expect("invariant: u8 always postcard-encodes"))
}

fn value_bytes(value: u8) -> Bytes {
    Bytes::from(postcard::to_stdvec(&value).expect("invariant: u8 always postcard-encodes"))
}

/// Clamps a fuzz-generated byte to node ids `1..=4`, kept free of node `0`
/// so it never collides with a real cluster member id.
fn clamp_node(node: u8) -> NodeId {
    NodeId::from(u64::from(node % 4) + 1)
}

/// Builds the [`Hlc`] a remote op stamps, relative to `now_ms`.
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

/// One `RemoteApply`-shaped record inside an [`Op::RemoteBatch`], factored
/// out because a struct variant's fields cannot be reused as a `Vec` item.
#[derive(Debug, Clone, arbitrary::Arbitrary)]
pub struct RemoteRecord {
    pub key: u8,
    pub value: Option<u8>,
    /// Signed, so a generated batch lands records both before and after "now".
    pub wall_ms_offset: i16,
    pub logical: u8,
    pub node: u8,
    pub expires_offset_ms: Option<i16>,
}

/// The `Arbitrary`, coverage-guided operation vocabulary a stateful apply-path
/// fuzz run is a sequence of.
#[derive(Debug, Clone, arbitrary::Arbitrary)]
pub enum Op {
    /// [`Shard::insert`] or [`Shard::insert_with_ttl`], stamped by the
    /// shard's own [`crate::hlc::HlcClock`]. [`run`]'s driver predicts the
    /// stamp via `Model::stamp_local` rather than reading it back, since
    /// an already-expired write is invisible to [`ShardOps::records_for`].
    LocalInsert {
        key: u8,
        value: u8,
        /// `None` for the shard's default, which is no TTL.
        ttl_ms: Option<u16>,
    },
    /// [`Shard::remove`], predicted the same way as [`Op::LocalInsert`].
    LocalRemove { key: u8 },
    /// [`ShardOps::apply_remote`]: one record from a simulated peer, its
    /// version and expiry offset from the current clock.
    RemoteApply {
        key: u8,
        /// `None` for a tombstone.
        value: Option<u8>,
        wall_ms_offset: i16,
        logical: u8,
        /// Clamped to `1..=4`.
        node: u8,
        /// `None` for no TTL.
        expires_offset_ms: Option<i16>,
    },
    /// [`ShardOps::apply_remote_batch`], capped to the first 16 generated
    /// records to match a realistic coalesced batch size.
    RemoteBatch(Vec<RemoteRecord>),
    /// [`ShardOps::invalidate`], an invalidation carrying no value.
    Invalidate {
        key: u8,
        wall_ms_offset: i16,
        logical: u8,
        /// Clamped to `1..=4`.
        node: u8,
    },
    /// [`ShardOps::gc_tombstones`].
    Gc {
        /// Defers a past-`tombstone_ttl` tombstone until it is also
        /// past-`tombstone_max_ttl`.
        any_member_absent: bool,
    },
    /// [`ShardOps::run_pending_tasks`], the engine's explicit sweep.
    Sweep,
    /// Moves the shared manual clock forward by `ms` milliseconds.
    AdvanceClock { ms: u16 },
}

/// Caps a generated batch to its first 16 entries.
const REMOTE_BATCH_CAP: usize = 16;

/// Builds a fresh `Shard<u8, u8>` and its paired [`Model`], wired to the
/// same manual clock and tombstone retention. Capacity is unbounded;
/// sampled-LRU eviction is outside what this model reimplements.
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
    .with_max_clock_skew(Some(Duration::from_millis(MAX_CLOCK_SKEW_MS)))
    .with_clock(model.clock_fn());
    (shard, model)
}

/// Like [`new_shard_and_model`], but for a `Mode::Distributed` cache: a
/// bucket-ownership view computed directly from `eligible` (`node` folded
/// in automatically, matching [`OwnershipView::compute`]'s own contract)
/// and `owners` is attached to both the returned shard and the returned
/// model, so a fuzz target can assert convergence never smuggles an entry
/// into a bucket outside that view via [`Model::owns`]. Every other
/// constructor here leaves both at "every node owns everything."
#[must_use]
pub fn new_shard_and_model_with_ownership(
    name: &str,
    node: u64,
    eligible: Vec<NodeId>,
    owners: NonZeroU8,
) -> (Shard<u8, u8>, Model) {
    let node = NodeId::from(node);
    let view = Arc::new(OwnershipView::compute(node, eligible, owners));

    let mut model = Model::new(node, TOMBSTONE_TTL_MS, TOMBSTONE_MAX_TTL_MS);
    model.ownership = Some(Arc::clone(&view));

    let (tracker, tx) =
        OwnershipTracker::seed(node, &[], &HashMap::new(), &SmolStr::new(name), owners);
    tx.send(Arc::clone(&view)).expect("receiver still alive");
    let residency = Arc::new(ResidencySet::new());

    let shard = Shard::<u8, u8>::new(
        SmolStr::new(name),
        Mode::Distributed { owners },
        node,
        u64::MAX,
        None,
        None,
    )
    .with_tombstone_ttl(Duration::from_millis(TOMBSTONE_TTL_MS))
    .with_tombstone_max_ttl(Duration::from_millis(TOMBSTONE_MAX_TTL_MS))
    .with_max_clock_skew(Some(Duration::from_millis(MAX_CLOCK_SKEW_MS)))
    .with_clock(model.clock_fn())
    .with_ownership(tracker, residency);
    (shard, model)
}

/// Builds the [`WireRecord`] a `RemoteRecord` stamps at `now_ms`, matching
/// the conversion [`run`]'s driver applies for remote ops.
#[must_use]
pub fn remote_wire_record(record: &RemoteRecord, now_ms: u64) -> WireRecord {
    WireRecord {
        key: key_bytes(record.key),
        value: record.value.map(value_bytes),
        ver: remote_hlc(now_ms, record.wall_ms_offset, record.logical, record.node),
        expires_at_ms: remote_expiry(now_ms, record.expires_offset_ms),
    }
}

/// Mirrors one remote record's effect on [`Model::hlc_clock`], then applies
/// it exactly as [`Shard::apply`]'s default resolver would.
fn apply_remote_to_model(model: &mut Model, record: &RemoteRecord, now_ms: u64) {
    let ver = remote_hlc(now_ms, record.wall_ms_offset, record.logical, record.node);
    if !model.observe_remote(ver) {
        return;
    }
    match record.value {
        Some(v) => model.apply(
            record.key,
            ver,
            v,
            remote_expiry(now_ms, record.expires_offset_ms),
        ),
        None => model.remove(
            record.key,
            ver,
            remote_expiry(now_ms, record.expires_offset_ms),
        ),
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
            model.remove(*key, ver, None);
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
            if model.observe_remote(ver) {
                model.invalidate(*key, ver);
            }
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

/// Every key's read matches [`Model::visible`], with no deadline at or
/// before the clock on a key that reads as present.
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

/// [`ShardOps::digests`] equals the XOR of [`entry_fingerprint`] over
/// [`Model::entries`], and [`ShardOps::entries_for_buckets`] matches it as
/// sets.
fn assert_digest_and_entries_match_model(shard: &Shard<u8, u8>, model: &Model) {
    let model_entries: HashSet<(Bytes, Hlc)> = model.entries().into_iter().collect();

    let mut expected_digest = vec![0u64; BUCKET_COUNT];
    for (key_bytes, ver) in &model_entries {
        let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes));
        expected_digest[bucket] ^= entry_fingerprint(key_bytes, *ver);
    }
    let actual_digest: Vec<u64> = futures::executor::block_on(ShardOps::digests(shard))
        .into_iter()
        .map(|bd| bd.digest)
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
            .map(|kv| (kv.key, kv.version))
            .collect();
    assert_eq!(
        shard_entries, model_entries,
        "entries_for_buckets(all buckets) diverged from the model's entry set"
    );
}

/// Applies `ops` to `shard` and `model` side by side, asserting after every
/// op that the shard's observable state matches the model's.
///
/// # Panics
///
/// Panics on the first divergence between `shard` and `model`.
pub fn run(ops: &[Op], shard: &Shard<u8, u8>, model: &mut Model) {
    for op in ops {
        apply_op(op, shard, model);
        assert_reads_match_model(shard, model);
        if matches!(op, Op::Sweep) {
            assert_digest_and_entries_match_model(shard, model);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn model() -> Model {
        Model::new(NodeId::from(1), 1_000, 10_000)
    }

    fn ver(wall_ms: u64) -> Hlc {
        Hlc {
            wall_ms,
            logical: 0,
            node: NodeId::from(2),
        }
    }

    fn held(model: &Model, key: u8) -> Option<Hlc> {
        model
            .entries()
            .into_iter()
            .find_map(|(bytes, v)| (bytes == key_bytes(key)).then_some(v))
    }

    #[test]
    fn sweep_replaces_an_expired_entry_with_a_tombstone_that_keeps_its_version() {
        let mut model = model();
        model.apply(1, ver(5), 7, Some(START_CLOCK_MS + 100));
        model.advance_clock(200);
        model.sweep();

        assert_eq!(model.visible(1), None);
        assert_eq!(
            held(&model, 1),
            Some(ver(5)),
            "listed at the entry's version"
        );
        model.apply(1, ver(4), 9, None);
        assert_eq!(model.visible(1), None, "an older write stays out");
        model.apply(1, ver(6), 9, None);
        assert_eq!(model.visible(1), Some(9), "a newer one lands");
    }

    #[test]
    fn a_swept_tombstone_is_collected_a_retention_after_the_expiry() {
        let mut model = model();
        model.apply(1, ver(5), 7, Some(START_CLOCK_MS + 100));
        model.advance_clock(200);
        model.sweep();

        model.advance_clock(899);
        model.gc(false);
        assert!(held(&model, 1).is_some(), "kept until the retention ends");
        model.advance_clock(1);
        model.gc(true);
        assert!(held(&model, 1).is_some(), "an absent member defers it");
        model.advance_clock(9_000);
        model.gc(true);
        assert_eq!(held(&model, 1), None, "the cap collects it");
    }

    #[test]
    fn a_late_sweep_keeps_the_tombstone_to_the_sweep() {
        let mut model = model();
        model.apply(1, ver(5), 7, Some(START_CLOCK_MS + 100));
        model.advance_clock(50_000);
        model.sweep();
        assert!(held(&model, 1).is_some());
        model.gc(false);
        assert_eq!(held(&model, 1), None, "collectible at the sweep instant");
    }

    #[test]
    fn a_lapsed_expiry_tombstone_stores_nothing_whatever_the_key_holds() {
        let mut model = model();
        model.gc(false);
        let long_ago = Some(START_CLOCK_MS - 5_000);
        model.remove(1, ver(5), long_ago);
        assert_eq!(held(&model, 1), None, "nothing held, already lapsed");

        model.apply(2, ver(4), 1, None);
        model.remove(2, ver(5), long_ago);
        assert_eq!(model.visible(2), Some(1), "the older copy stays");
        assert_eq!(held(&model, 2), Some(ver(4)), "and nothing replaces it");

        model.remove(3, ver(3), None);
        model.remove(3, ver(5), long_ago);
        assert_eq!(held(&model, 3), Some(ver(3)), "an older tombstone stays");

        model.remove(4, ver(5), None);
        assert_eq!(held(&model, 4), Some(ver(5)), "a delete is always stored");
    }

    #[test]
    fn a_deferring_collection_keeps_a_lapsed_expiry_tombstone_to_its_cap() {
        let mut model = model();
        model.gc(true);
        model.remove(1, ver(5), Some(START_CLOCK_MS - 20_000));
        assert_eq!(held(&model, 1), None, "past its cap too");
        model.remove(1, ver(5), Some(START_CLOCK_MS - 2_000));
        assert_eq!(
            held(&model, 1),
            Some(ver(5)),
            "within its cap, past its ttl"
        );

        model.apply(2, ver(4), 1, None);
        model.remove(2, ver(5), Some(START_CLOCK_MS - 2_000));
        assert_eq!(model.visible(2), None, "it deletes the older copy");
        assert_eq!(held(&model, 2), Some(ver(5)));
    }

    #[test]
    fn a_new_model_defers_collection_until_its_first_gc() {
        let mut model = model();
        let past_its_ttl = Some(START_CLOCK_MS - 2_000);
        model.apply(1, ver(4), 1, None);
        model.remove(1, ver(5), past_its_ttl);
        assert_eq!(
            model.visible(1),
            None,
            "a new model keeps it, as a new shard"
        );
        assert_eq!(held(&model, 1), Some(ver(5)));

        model.gc(false);
        assert_eq!(held(&model, 1), None, "the collection ends the deferral");
        model.apply(2, ver(4), 1, None);
        model.remove(2, ver(5), past_its_ttl);
        assert_eq!(model.visible(2), Some(1), "and the next one is dropped");
    }

    #[test]
    fn part_of_is_the_part_a_model_key_hashes_into_inside_its_bucket() {
        for key in [0u8, 1, 7, 200, 255] {
            let part = part_of(key);
            assert_eq!(part, PartId::of_key(key_bytes(key).as_ref()));
            assert_eq!(part.bucket(), bucket_of(key));
        }
    }
}

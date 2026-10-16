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
/// enough that [`Op::AdvanceClock`] routinely crosses both deadlines. The
/// gc period is the one-second floor, so the pause gap is 2,000 ms.
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
    /// The clock reading of the last [`Model::gc`], at least `1`; `None`
    /// before the first. Mirrors the engine's `last_gc_ms`, whose `0` is the
    /// sentinel for no collection yet.
    last_gc_ms: Option<u64>,
    /// The instant the hold a pause started lasts to; `0` when none.
    pause_hold_until_ms: u64,
    /// How many collections after the first came after a pause and how many
    /// after a normal gap.
    paused_gcs: u64,
    steady_gcs: u64,
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
            last_gc_ms: None,
            pause_hold_until_ms: 0,
            paused_gcs: 0,
            steady_gcs: 0,
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
    /// [`Model::apply`], its two GC deadlines stamped from the clock. Every
    /// tombstone counts them from an instant all replicas agree on, capped
    /// at now: the version's `wall_ms` for a delete, the expiry
    /// (`expired_at_ms` `Some`) for the tombstone of an expired entry. One
    /// that comes from a peer (`from_peer`) is dropped on arrival, whatever
    /// the key holds, when its retention has already lapsed: the `ttl`
    /// deadline, or the `max` one while the last [`Model::gc`] deferred. A
    /// local delete is always stored.
    pub fn remove(&mut self, key: u8, ver: Hlc, expired_at_ms: Option<u64>, from_peer: bool) {
        let now = self.now_ms();
        let anchor = expired_at_ms.unwrap_or(ver.wall_ms).min(now);
        let ttl_deadline_ms = anchor.saturating_add(self.tombstone_ttl_ms);
        let max_deadline_ms = anchor.saturating_add(self.tombstone_max_ttl_ms);
        let horizon = if self.deferring_at(now) {
            max_deadline_ms
        } else {
            ttl_deadline_ms
        };
        if from_peer && now >= horizon {
            return;
        }
        if self.incoming_loses(key, ver) {
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

    /// The pause gap, restated here rather than imported so the model stays
    /// an independent oracle: twice the gc period, a quarter of the
    /// retention with a one-second floor.
    fn pause_gap_ms(&self) -> u64 {
        2 * (self.tombstone_ttl_ms / 4).max(1_000)
    }

    /// Whether the clock at `now` shows more than the pause gap since the
    /// last collection. Strict; a clock that stepped back is not a pause.
    fn paused_at(&self, now: u64) -> bool {
        self.last_gc_ms
            .is_some_and(|last| now.saturating_sub(last) > self.pause_gap_ms())
    }

    /// Whether an arrival at `now` is judged against the cap deadline: the
    /// last collection deferred, or a pause is showing, or its hold lasts.
    fn deferring_at(&self, now: u64) -> bool {
        self.gc_deferring || self.paused_at(now) || now < self.pause_hold_until_ms
    }

    /// How many collections after the first came after a pause and how many
    /// after a normal gap, as `(paused, steady)`. A fuzz run that never
    /// reaches one of them leaves that side of the pause guard untested.
    #[must_use]
    pub fn gc_branch_counts(&self) -> (u64, u64) {
        (self.paused_gcs, self.steady_gcs)
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
    /// A collection more than the pause gap after the previous one also
    /// defers, and so does every later one for a `tombstone_ttl` after it: a
    /// later pause extends that hold. The first collection is a baseline.
    pub fn gc(&mut self, defer: bool) {
        let now = self.now_ms();
        let paused = self.paused_at(now);
        let holding = now < self.pause_hold_until_ms;
        if self.last_gc_ms.is_some() {
            if paused {
                self.paused_gcs += 1;
            } else {
                self.steady_gcs += 1;
            }
        }
        self.last_gc_ms = Some(now.max(1));
        if paused {
            self.pause_hold_until_ms = self
                .pause_hold_until_ms
                .max(now.saturating_add(self.tombstone_ttl_ms));
        }
        let defer = defer || paused || holding;
        self.gc_deferring = defer;
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
    /// Moves the shared manual clock forward by a small step, `ms` modulo
    /// 700 milliseconds: past `tombstone_ttl` in a few steps and, summed
    /// over several, past the pause gap, though seldom between two
    /// collections.
    AdvanceClock { ms: u16 },
    /// Moves the shared manual clock forward by a long jump of 2,000 to
    /// 7,999 milliseconds, `2_000 + ms % 6_000`: at least the pause gap, so
    /// a collection right after it sees a pause unless it lands exactly on
    /// the gap.
    JumpClock { ms: u16 },
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
            true,
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
            model.remove(*key, ver, None, false);
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
            model.advance_clock(u64::from(*ms) % 700);
        }
        Op::JumpClock { ms } => {
            model.advance_clock(2_000 + u64::from(*ms) % 6_000);
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
        model.remove(1, ver(5), long_ago, true);
        assert_eq!(held(&model, 1), None, "nothing held, already lapsed");

        model.apply(2, ver(4), 1, None);
        model.remove(2, ver(5), long_ago, true);
        assert_eq!(model.visible(2), Some(1), "the older copy stays");
        assert_eq!(held(&model, 2), Some(ver(4)), "and nothing replaces it");

        model.remove(3, ver(3), None, false);
        model.remove(3, ver(5), long_ago, true);
        assert_eq!(held(&model, 3), Some(ver(3)), "an older tombstone stays");

        model.remove(4, ver(5), None, false);
        assert_eq!(
            held(&model, 4),
            Some(ver(5)),
            "a local delete is always stored"
        );
    }

    #[test]
    fn a_lapsed_peer_delete_stores_nothing_whatever_the_key_holds() {
        let mut model = model();
        model.gc(false);
        let lapsed = ver(START_CLOCK_MS - 5_000);
        model.remove(1, lapsed, None, true);
        assert_eq!(held(&model, 1), None, "nothing held, already lapsed");

        model.apply(2, ver(4), 1, None);
        model.remove(2, lapsed, None, true);
        assert_eq!(model.visible(2), Some(1), "the older copy stays");
        assert_eq!(held(&model, 2), Some(ver(4)), "and nothing replaces it");

        model.remove(3, ver(3), None, false);
        model.remove(3, lapsed, None, true);
        assert_eq!(held(&model, 3), Some(ver(3)), "an older tombstone stays");

        let fresh = ver(START_CLOCK_MS - 100);
        model.apply(4, ver(4), 1, None);
        model.remove(4, fresh, None, true);
        assert_eq!(model.visible(4), None, "a delete within its ttl lands");
        assert_eq!(held(&model, 4), Some(fresh));
    }

    #[test]
    fn a_local_remove_is_always_stored() {
        let mut model = model();
        model.gc(false);
        let long_ago = START_CLOCK_MS - 5_000;
        model.remove(1, ver(long_ago), None, false);
        assert_eq!(
            held(&model, 1),
            Some(ver(long_ago)),
            "a local delete past its retention is stored"
        );
        model.apply(2, ver(4), 1, None);
        model.remove(2, ver(5), Some(long_ago), false);
        assert_eq!(model.visible(2), None, "and deletes the older copy");
    }

    #[test]
    fn a_delete_counts_its_retention_from_its_stamp_capped_at_now() {
        let mut model = model();
        model.gc(false);
        model.remove(1, ver(START_CLOCK_MS - 999), None, true);
        model.gc(false);
        assert!(held(&model, 1).is_some(), "kept until stamp + ttl");
        model.advance_clock(1);
        model.gc(false);
        assert_eq!(held(&model, 1), None, "collected at stamp + ttl");

        model.remove(2, ver(model.now_ms() + 50_000), None, true);
        model.advance_clock(999);
        model.gc(false);
        assert!(
            held(&model, 2).is_some(),
            "a stamp ahead counts from the receipt"
        );
        model.advance_clock(1);
        model.gc(false);
        assert_eq!(held(&model, 2), None);
    }

    #[test]
    fn a_deferring_collection_keeps_a_lapsed_expiry_tombstone_to_its_cap() {
        let mut model = model();
        model.gc(true);
        model.remove(1, ver(5), Some(START_CLOCK_MS - 20_000), true);
        assert_eq!(held(&model, 1), None, "past its cap too");
        model.remove(1, ver(5), Some(START_CLOCK_MS - 2_000), true);
        assert_eq!(
            held(&model, 1),
            Some(ver(5)),
            "within its cap, past its ttl"
        );

        model.apply(2, ver(4), 1, None);
        model.remove(2, ver(5), Some(START_CLOCK_MS - 2_000), true);
        assert_eq!(model.visible(2), None, "it deletes the older copy");
        assert_eq!(held(&model, 2), Some(ver(5)));
    }

    #[test]
    fn a_deferring_collection_keeps_a_lapsed_peer_delete_to_its_cap() {
        let mut model = model();
        model.gc(true);
        model.remove(1, ver(START_CLOCK_MS - 20_000), None, true);
        assert_eq!(held(&model, 1), None, "past its cap");
        let within_cap = ver(START_CLOCK_MS - 2_000);
        model.remove(2, within_cap, None, true);
        assert_eq!(held(&model, 2), Some(within_cap), "past its ttl only");
        model.gc(false);
        assert_eq!(held(&model, 2), None, "collected once collection resumes");
    }

    #[test]
    fn a_new_model_defers_collection_until_its_first_gc() {
        let mut model = model();
        let past_its_ttl = Some(START_CLOCK_MS - 2_000);
        model.apply(1, ver(4), 1, None);
        model.remove(1, ver(5), past_its_ttl, true);
        assert_eq!(
            model.visible(1),
            None,
            "a new model keeps it, as a new shard"
        );
        assert_eq!(held(&model, 1), Some(ver(5)));

        model.gc(false);
        assert_eq!(held(&model, 1), None, "the collection ends the deferral");
        model.apply(2, ver(4), 1, None);
        model.remove(2, ver(5), past_its_ttl, true);
        assert_eq!(model.visible(2), Some(1), "and the next one is dropped");
    }

    /// Gap of this module's model: twice the one-second floor of the period.
    const GAP: u64 = 2_000;

    #[test]
    fn a_pause_longer_than_the_gap_defers_collection_for_a_ttl() {
        // A retention of 8 s: a 2 s gc period and a 4 s gap.
        let mut model = Model::new(NodeId::from(1), 8_000, 80_000);
        model.gc(false);
        model.remove(1, ver(START_CLOCK_MS - 7_999), None, true);
        assert!(
            held(&model, 1).is_some(),
            "stored, a millisecond from its ttl"
        );

        // The clock jumps past the gap: the tombstone is past its ttl, and
        // the collection that sees the pause keeps it.
        model.advance_clock(4_001);
        model.gc(false);
        assert!(held(&model, 1).is_some(), "past its ttl, held by the pause");
        let hold_ends = model.now_ms() + 8_000;
        assert_eq!(model.pause_hold_until_ms, hold_ends);

        // Ticks one gap apart through the hold keep it; the one at its end
        // collects.
        model.advance_clock(4_000);
        model.gc(false);
        assert!(held(&model, 1).is_some(), "inside the hold");
        model.advance_clock(hold_ends - 1 - model.now_ms());
        model.gc(false);
        assert!(held(&model, 1).is_some(), "a millisecond before it ends");
        model.advance_clock(1);
        model.gc(false);
        assert_eq!(held(&model, 1), None, "collected once the hold ends");
    }

    #[test]
    fn the_gap_is_strict_and_the_first_collection_is_a_baseline() {
        let mut model = model();
        assert!(!model.paused_at(u64::MAX), "no collection yet");
        model.gc(false);
        assert_eq!(model.gc_branch_counts(), (0, 0), "the first is neither");
        model.advance_clock(GAP);
        assert!(!model.paused_at(model.now_ms()));
        model.advance_clock(1);
        assert!(model.paused_at(model.now_ms()));
        model.gc(false);
        assert_eq!(model.gc_branch_counts(), (1, 0));
        model.advance_clock(GAP);
        model.gc(false);
        assert_eq!(model.gc_branch_counts(), (1, 1));
    }

    #[test]
    fn a_collection_at_clock_zero_is_a_collection() {
        let mut model = model();
        model.clock.store(0, Ordering::Relaxed);
        model.gc(false);
        assert_eq!(model.last_gc_ms, Some(1), "stored as 1, as the engine does");
        model.advance_clock(GAP + 1);
        assert!(
            !model.paused_at(model.now_ms()),
            "1 + 2000 + 1 - 1 is the gap"
        );
        model.advance_clock(1);
        assert!(model.paused_at(model.now_ms()));
    }

    #[test]
    fn an_arrival_after_a_resume_is_judged_against_the_cap_before_any_collection() {
        let mut model = model();
        model.gc(false);
        let lapsed = ver(model.now_ms() - 2_000);
        model.apply(1, ver(4), 1, None);
        model.remove(1, lapsed, None, true);
        assert_eq!(model.visible(1), Some(1), "ignored on a normal gap");

        model.advance_clock(GAP + 1);
        model.remove(1, ver(model.now_ms() - 2_000), None, true);
        assert_eq!(model.visible(1), None, "stored: the pause defers at once");
    }

    #[test]
    fn a_clock_that_steps_back_is_not_a_pause() {
        let mut model = model();
        model.gc(false);
        model
            .clock
            .store(START_CLOCK_MS - 10 * GAP, Ordering::Relaxed);
        assert!(!model.paused_at(model.now_ms()));
        model.gc(false);
        assert_eq!(model.pause_hold_until_ms, 0);
        assert_eq!(model.gc_branch_counts(), (0, 1));
    }

    /// Op sequences drawn the way the fuzz targets and the model property
    /// test draw them reach collections after a pause and collections after
    /// a normal gap often, so neither side of the guard goes untested.
    #[test]
    fn generated_op_sequences_reach_both_sides_of_the_pause_guard() {
        use arbitrary::Arbitrary as _;
        use rand::{RngExt as _, SeedableRng as _, rngs::StdRng};

        let mut rng = StdRng::seed_from_u64(0x5EED_7157);
        let (mut paused, mut steady) = (0u64, 0u64);
        for run_index in 0..60 {
            let bytes: Vec<u8> = (0..2_048).map(|_| rng.random()).collect();
            let mut unstructured = arbitrary::Unstructured::new(&bytes);
            let mut ops = Vec::new();
            while ops.len() < 128 && !unstructured.is_empty() {
                ops.push(Op::arbitrary(&mut unstructured).expect("random bytes make an op"));
            }
            let (shard, mut model) = new_shard_and_model(&format!("coverage-{run_index}"), 1);
            run(&ops, &shard, &mut model);
            let (p, s) = model.gc_branch_counts();
            paused += p;
            steady += s;
        }
        let total = paused + steady;
        assert!(total >= 200, "enough collections to judge: {total}");
        assert!(
            paused * 3 >= total,
            "at least a third after a pause: {paused} of {total}"
        );
        assert!(
            steady * 3 >= total,
            "at least a third after a normal gap: {steady} of {total}"
        );
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

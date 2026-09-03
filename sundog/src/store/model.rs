//! [`Model`] is a sequential last-writer-wins map over `u8` keys that
//! reimplements [`Shard::apply`]'s versioned-write rule,
//! [`ShardOps::gc_tombstones`]'s retention rule, and TTL expiry without
//! touching `engine::Engine`. A divergence between the two is a bug.
//!
//! [`Op`] is the `Arbitrary` vocabulary [`run`] applies to a shard and its
//! model side by side, asserting agreement after every step.

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

/// Tombstone retention for every [`new_shard_and_model`] pair. Short
/// enough that [`Op::AdvanceClock`] routinely crosses both deadlines.
pub const TOMBSTONE_TTL_MS: u64 = 200;
/// Hard cap on tombstone retention for every [`new_shard_and_model`] pair.
pub const TOMBSTONE_MAX_TTL_MS: u64 = 2_000;
/// The manual clock's starting value, an arbitrary epoch-ms baseline.
pub const START_CLOCK_MS: u64 = 1_000_000;

/// One entry the model holds for a key: a live value or a tombstone,
/// carrying the same bookkeeping [`super::Stored`] and [`super::Tombstone`]
/// carry.
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
}

impl Model {
    /// Builds an empty model with its clock at [`START_CLOCK_MS`] and its
    /// [`HlcClock`] seeded with `node`, the same node the shard stamps with.
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
    /// into [`Model::hlc_clock`] so a later stamp stays causally after it.
    fn observe_remote(&mut self, remote: Hlc) {
        let now = self.now_ms();
        self.hlc_clock.observe(now, remote);
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
    /// [`Model::apply`], its two GC deadlines stamped from the clock.
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
    /// `tombstone_ttl`, and past `tombstone_max_ttl` while any member is absent.
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

    /// [`super::engine::Engine::sweep`]'s rule: a live entry past its own
    /// `expires_at_ms` is dropped outright, with no tombstone left behind.
    pub fn sweep(&mut self) {
        let now = self.now_ms();
        self.entries.retain(|_, entry| match entry {
            ModelEntry::Live { expires_at_ms, .. } => expires_at_ms.is_none_or(|exp| now < exp),
            ModelEntry::Tombstone { .. } => true,
        });
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
    /// stamp via [`Model::stamp_local`] rather than reading it back, since
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
    .with_clock(model.clock_fn());
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
/// [`Model::entries`], and [`ShardOps::entries_for_buckets`] matches it as sets.
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

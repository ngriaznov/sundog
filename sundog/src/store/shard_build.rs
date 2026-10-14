//! Construction of a [`Shard`]: the builder methods, the clock and the stamping helpers that
//! derive a record's version and expiry.

use std::hash::Hash;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;
use smol_str::SmolStr;
use tokio::sync::{Notify, broadcast};

use super::engine::Engine;
use super::skew_hold::{SKEW_HOLD_MAX_BYTES, SKEW_HOLD_MAX_RECORDS, SkewHold};
use super::{
    BUCKET_COUNT, CeilingRefusals, CompactionBounds, ConflictResolver, FanOutQueue, LwwResolver,
    Merged, Mode, PendingMergeStripe, Quiescence, RecordView, Shard, SharedWeigher, Weigher,
    Winner, crdt, duration_ms, load, now_ms, read_timing,
};
use crate::config::ClusterConfig;
use crate::hlc::{Hlc, HlcClock};
use crate::node::NodeId;
use crate::ownership::{OwnershipTracker, ResidencySet};
use crate::wire::MAX_FRAME;

#[cfg(doc)]
use super::{Event, ShardOps};
#[cfg(feature = "sim")]
use super::{FanOutItem, SimFanOut};
#[cfg(doc)]
use crate::error::CacheError;
#[cfg(feature = "spill")]
use std::sync::OnceLock;

/// Boxes a remembered [`SharedWeigher`] into a fresh [`Weigher`] for
/// `engine::Engine::new`, keeping the `Arc` alive for the next rebuild.
fn boxed_weigher<K, V>(weigher: SharedWeigher<K, V>) -> Weigher<K, V>
where
    K: 'static,
    V: 'static,
{
    Box::new(move |k: &K, v: &V| weigher(k, v))
}

/// Whether a clock whose last stamp runs `lead_ms` ahead of the system
/// clock has fallen behind by more than `max_clock_skew_ms` allows. `None`
/// never flags.
pub(super) fn clock_jumped(lead_ms: u64, max_clock_skew_ms: Option<u64>) -> bool {
    max_clock_skew_ms.is_some_and(|max| lead_ms > max)
}

/// Capacity of each shard's [`Event`] broadcast channel. A subscriber that
/// falls this far behind misses events (`broadcast::error::RecvError::Lagged`)
/// instead of applying backpressure to writers.
const EVENTS_CAPACITY: usize = 1024;

/// The resolver a merging shard runs: `inner` for every decision,
/// plus [`ConflictResolver::settle`] applied to each merge result with the
/// shard's clock and its [`CompactionBounds`], so aged fold receipts are
/// dropped on the apply path and not only on the compaction sweep.
pub(super) struct SettlingResolver {
    inner: Arc<dyn ConflictResolver>,
    bounds: CompactionBounds,
    clock_fn: Arc<dyn Fn() -> u64 + Send + Sync>,
}

impl ConflictResolver for SettlingResolver {
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
        let merged = self.inner.merge(key, a, b)?;
        let now_ms = (self.clock_fn)();
        match self.inner.settle(
            key,
            merged.value.as_ref(),
            now_ms,
            self.bounds.receipt_ttl_ms,
        ) {
            Some(value) => Some(Merged {
                value,
                expires_at_ms: merged.expires_at_ms,
            }),
            None => Some(merged),
        }
    }

    fn compact(
        &self,
        key: &[u8],
        value: &[u8],
        now_ms: u64,
        retire: &dyn Fn(crdt::WriterId) -> bool,
        quiet: Quiescence,
        bounds: CompactionBounds,
    ) -> Option<Bytes> {
        self.inner
            .compact(key, value, now_ms, retire, quiet, bounds)
    }

    fn settle(&self, key: &[u8], value: &[u8], now_ms: u64, receipt_ttl_ms: u64) -> Option<Bytes> {
        self.inner.settle(key, value, now_ms, receipt_ttl_ms)
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
    /// Builds a new shard. `node` stamps this shard's local writes.
    ///
    /// Tombstone GC uses [`ClusterConfig::default`]'s `tombstone_ttl` until
    /// overridden via [`Shard::with_tombstone_ttl`]; `Shard::new` takes no
    /// `ClusterConfig` itself.
    #[must_use]
    pub fn new(
        name: SmolStr,
        mode: Mode,
        node: NodeId,
        max_capacity: u64,
        ttl: Option<Duration>,
        tti: Option<Duration>,
    ) -> Self {
        let engine = Arc::new(Engine::new(max_capacity, tti, None, None));
        let hits = metrics::counter!("sundog_cache_hits_total", "cache" => name.to_string());
        let misses = metrics::counter!("sundog_cache_misses_total", "cache" => name.to_string());
        let read_durations = read_timing::ReadDurations::new(&name);
        let skew_rejected =
            metrics::counter!("sundog_clock_skew_rejected_total", "cache" => name.to_string());
        let fan_out = Arc::new(FanOutQueue::new(name.clone(), !matches!(mode, Mode::Local)));

        Self {
            name,
            mode,
            engine,
            events: broadcast::channel(EVENTS_CAPACITY).0,
            fan_out,
            clock: StdMutex::new(HlcClock::new(node)),
            clock_fn: Arc::new(now_ms),
            ttl,
            tombstone_ttl_ms: duration_ms(ClusterConfig::default().tombstone_ttl),
            tombstone_max_ttl_ms: duration_ms(ClusterConfig::default().tombstone_max_ttl),
            max_clock_skew_ms: ClusterConfig::default().max_clock_skew.map(duration_ms),
            skew_rejected,
            skew_warned: AtomicBool::new(false),
            clock_jump_warned: AtomicBool::new(false),
            skew_hold: StdMutex::new(SkewHold::new(SKEW_HOLD_MAX_RECORDS, SKEW_HOLD_MAX_BYTES)),
            skew_wake: Notify::new(),
            skew_drop_warned: AtomicBool::new(false),
            spill_close: tokio::sync::OnceCell::new(),
            resolver: Arc::new(LwwResolver),
            raw_resolver: Arc::new(LwwResolver),
            crdt_bounds: ClusterConfig::default().crdt_compaction_bounds(),
            max_frame: MAX_FRAME,
            max_resident_bytes: u64::MAX,
            ceiling_refusals: None,
            fan_out_backlog_capacity: ClusterConfig::default().fan_out_backlog_capacity,
            fan_out_wait_timeout: ClusterConfig::default().fan_out_wait_timeout,
            merge_window: Duration::ZERO,
            pending_merges: (0..BUCKET_COUNT)
                .map(|_| StdMutex::new(PendingMergeStripe::new()))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            pending_merge_count: AtomicUsize::new(0),
            merge_flush_gate: std::sync::RwLock::new(()),
            #[cfg(test)]
            after_take_hook: std::sync::Mutex::new(None),
            merge_wake: Notify::new(),
            max_capacity,
            tti,
            capacity_hint: None,
            weigher: None,
            hits,
            misses,
            read_durations,
            loader: None,
            loader_calls: None,
            load_queue: load::LoadQueue::new(),
            refresh: None,
            ownership: None,
            residency: None,
            #[cfg(feature = "spill")]
            spill_read: OnceLock::new(),
        }
    }

    /// Overrides the tombstone retention period used by
    /// [`ShardOps::gc_tombstones`], defaulting
    /// to [`ClusterConfig::default`]'s value. Own-and-return, for the
    /// composition layer to thread a live cluster's configured
    /// `tombstone_ttl` through right after construction.
    #[must_use]
    pub fn with_tombstone_ttl(mut self, tombstone_ttl: Duration) -> Self {
        self.tombstone_ttl_ms = duration_ms(tombstone_ttl);
        self
    }

    /// Overrides the hard cap on tombstone retention used by
    /// [`ShardOps::gc_tombstones`].
    #[must_use]
    pub fn with_tombstone_max_ttl(mut self, tombstone_max_ttl: Duration) -> Self {
        self.tombstone_max_ttl_ms = duration_ms(tombstone_max_ttl);
        self
    }

    /// What a sweep of this shard leaves behind for an entry that expired:
    /// the retention of its tombstone, or `None` for nothing. See
    /// [`expiry_tombstone_retention`]. Read at call time, so a retention
    /// set after construction applies.
    pub(super) fn expiry_tombstones(&self) -> Option<super::engine::TombstoneRetention> {
        super::engine::expiry_tombstone_retention(
            self.mode,
            self.resolver.merges(),
            self.tombstone_ttl_ms,
            self.tombstone_max_ttl_ms,
        )
    }

    /// Overrides how far ahead of this node's clock a remote stamp may be
    /// ([`ClusterConfig::max_clock_skew`]); `None` accepts any stamp.
    #[must_use]
    pub fn with_max_clock_skew(mut self, max_clock_skew: Option<Duration>) -> Self {
        self.max_clock_skew_ms = max_clock_skew.map(duration_ms);
        self
    }

    /// Overrides the [`ConflictResolver`] `Shard::apply` consults whenever an
    /// incoming record's version differs from what's stored. Defaults to
    /// [`LwwResolver`].
    #[must_use]
    pub fn with_resolver(mut self, resolver: Arc<dyn ConflictResolver>) -> Self {
        self.raw_resolver = resolver;
        self.rebuild_resolver();
        self
    }

    /// Overrides the bounds the resolver's [`ConflictResolver::settle`]
    /// step prunes fold receipts against on every merge apply; the
    /// compaction sweep passes the same bounds per pass. Defaults to
    /// [`ClusterConfig::default`]'s.
    #[must_use]
    pub fn with_crdt_bounds(mut self, bounds: CompactionBounds) -> Self {
        self.crdt_bounds = bounds;
        self.rebuild_resolver();
        self
    }

    /// Applies everything a live cluster's configuration decides for a
    /// shard: tombstone bounds, the clock-skew bound, CRDT bounds, frame
    /// cap, fan-out capacity, and its wait timeout.
    #[must_use]
    pub fn with_cluster_config(self, config: &ClusterConfig) -> Self {
        self.with_tombstone_ttl(config.tombstone_ttl)
            .with_tombstone_max_ttl(config.tombstone_max_ttl)
            .with_max_clock_skew(config.max_clock_skew)
            .with_crdt_bounds(config.crdt_compaction_bounds())
            .with_max_frame(config.max_frame)
            .with_fan_out_backlog_capacity(config.fan_out_backlog_capacity)
            .with_fan_out_wait_timeout(config.fan_out_wait_timeout)
    }

    /// Rewraps `raw_resolver` with the current bound and clock. A
    /// non-merging resolver is used bare: it never merges, so it never
    /// settles either, and the wrapper would only add a hop.
    fn rebuild_resolver(&mut self) {
        self.resolver = if self.raw_resolver.merges() {
            Arc::new(SettlingResolver {
                inner: Arc::clone(&self.raw_resolver),
                bounds: self.crdt_bounds,
                clock_fn: Arc::clone(&self.clock_fn),
            })
        } else {
            Arc::clone(&self.raw_resolver)
        };
    }

    /// Overrides the hard cap [`Shard::insert`] enforces before writing a
    /// value, threaded from a live cluster's configured
    /// `ClusterConfig::max_frame`. Defaults to [`MAX_FRAME`].
    #[must_use]
    pub fn with_max_frame(mut self, max_frame: usize) -> Self {
        self.max_frame = max_frame;
        self
    }

    /// Sets a soft ceiling on [`Shard::resident_bytes`]. A local write
    /// ([`Shard::insert`], [`Shard::insert_with_ttl`], [`Shard::merge`],
    /// [`Shard::insert_many`]) that finds the ceiling reached fails with
    /// [`CacheError::OverMemoryCeiling`], and a read-through fill returns its
    /// value without storing it. Removals and writes arriving from peers are
    /// always accepted, so every replica keeps the same entries and resident
    /// bytes can pass the ceiling by what peers send. Defaults to `u64::MAX`,
    /// no ceiling.
    ///
    /// A shard with no ceiling counts no bytes: [`Shard::resident_bytes`]
    /// is `None` and the write paths skip the check. Setting one rebuilds
    /// the engine with counting on, carrying forward any
    /// [`Shard::with_weigher`] and [`Shard::with_capacity_hint`], so call it
    /// right after [`Shard::new`], before any reads or writes reach this
    /// shard. `u64::MAX` removes the ceiling.
    #[must_use]
    pub fn with_max_resident_bytes(mut self, ceiling: u64) -> Self {
        self.max_resident_bytes = ceiling;
        self.ceiling_refusals = (ceiling != u64::MAX).then(|| CeilingRefusals::new(&self.name));
        if self.ceiling_refusals.is_some() != self.engine.resident_bytes().is_some() {
            self.engine = self.build_engine();
        }
        self
    }

    /// Overrides the fan-out backlog capacity an async write waits against
    /// before queuing. Defaults to [`ClusterConfig::default`]'s.
    #[must_use]
    pub fn with_fan_out_backlog_capacity(mut self, capacity: usize) -> Self {
        self.fan_out_backlog_capacity = capacity;
        self
    }

    /// Overrides the bound on that fan-out backlog wait. Defaults to
    /// [`ClusterConfig::default`]'s.
    #[must_use]
    pub fn with_fan_out_wait_timeout(mut self, timeout: Duration) -> Self {
        self.fan_out_wait_timeout = timeout;
        self
    }

    /// Sets the window [`Shard::merge`] and [`Shard::merge_with_ttl`]
    /// coalesce consecutive calls to one key within: they fold in memory instead of each applying on its own,
    /// and the fold applies once, when the window elapses. Zero, the
    /// default, applies every `merge` call at once. A raw setter that
    /// trusts the caller to have already checked the resolver merges;
    /// [`crate::cache::CacheBuilder::merge_coalesce_window`] is the
    /// validated entry point.
    #[must_use]
    pub fn with_merge_coalesce_window(mut self, window: Duration) -> Self {
        self.merge_window = window;
        self
    }

    /// This shard's configured [`Shard::with_merge_coalesce_window`], for
    /// `crate::cache`'s background sweep to decide whether to spawn at all.
    pub(crate) fn merge_window(&self) -> Duration {
        self.merge_window
    }

    /// Flips [`Engine::apply_many`]'s pre-fold on or off for this shard's
    /// engine, `true` (pre-folding on) by default. `#[doc(hidden)]`:
    /// reachable from `crate::cache::CacheBuilder::prefold_enabled`, in turn
    /// reachable from an integration-test binary outside this crate; a
    /// benchmark measuring pre-fold's own effect is the only caller that
    /// ever needs it off, to compare against the unfolded per-record path
    /// [`Engine::apply_many`] otherwise always takes. Never call this
    /// outside a benchmark or test.
    #[doc(hidden)]
    #[must_use]
    pub fn with_prefold_enabled(self, enabled: bool) -> Self {
        self.engine.set_prefold_enabled(enabled);
        self
    }

    /// This shard's engine's current [`Shard::with_prefold_enabled`] flag,
    /// for a test to confirm the toggle reached the engine.
    #[cfg(test)]
    pub(crate) fn prefold_enabled(&self) -> bool {
        self.engine.prefold_enabled()
    }

    /// Attaches this shard's `Mode::Distributed` bucket-ownership tracker
    /// and its disown-grace residency set. Call this once, right after
    /// construction and before the shard is ever shared, so
    /// [`ShardOps::ownership_view`] never observes anything but a real,
    /// already-computed view. Every other mode leaves both unset.
    #[must_use]
    pub(crate) fn with_ownership(
        mut self,
        tracker: OwnershipTracker,
        residency: Arc<ResidencySet>,
    ) -> Self {
        self.ownership = Some(tracker);
        self.residency = Some(residency);
        self
    }

    /// The residency set [`Shard::with_ownership`] attached, for a test
    /// that marks a bucket cold or releasing by hand.
    #[cfg(all(test, not(feature = "sim")))]
    pub(crate) fn residency(&self) -> Option<&Arc<ResidencySet>> {
        self.residency.as_ref()
    }

    /// Test-only mirror of [`Shard::with_ownership`], reachable from
    /// `tests/sim/`, which lives outside this crate and cannot otherwise
    /// reach a `pub(crate)` builder method. Never call this outside the sim
    /// harness.
    #[cfg(feature = "sim")]
    #[doc(hidden)]
    #[must_use]
    pub fn with_ownership_for_sim(
        self,
        tracker: OwnershipTracker,
        residency: Arc<ResidencySet>,
    ) -> Self {
        self.with_ownership(tracker, residency)
    }

    /// Test-only mirror of [`Shard::fan_out_queue`]'s drain, translating
    /// each [`FanOutItem`] into the sim-visible [`SimFanOut`] shape so the
    /// harness never needs `FanOutItem`/`FanOutQueue` themselves made
    /// reachable. Reachable from `tests/sim/` for the same reason as
    /// [`Shard::with_ownership_for_sim`].
    #[cfg(feature = "sim")]
    #[doc(hidden)]
    pub fn drain_fan_out_for_sim(&self) -> Vec<SimFanOut<K>> {
        self.fan_out_queue()
            .drain()
            .into_iter()
            .map(|item| match item {
                FanOutItem::Applied(key, _) => SimFanOut::Applied(key),
                FanOutItem::Forward(rec) => SimFanOut::Forward(rec),
            })
            .collect()
    }

    /// Installs a custom per-entry weigher for size-bounded eviction, in place
    /// of the default of one weight unit per entry. Rebuilds
    /// `engine::Engine` from scratch, carrying forward any
    /// [`Shard::with_capacity_hint`] already set. Call this right after
    /// [`Shard::new`], before any reads or writes reach this shard; the two
    /// builders can run in either order.
    #[must_use]
    pub fn with_weigher<W>(mut self, weigher: W) -> Self
    where
        W: Fn(&K, &V) -> u32 + Send + Sync + 'static,
    {
        self.weigher = Some(Arc::new(weigher));
        self.engine = self.build_engine();
        self
    }

    /// Hints this shard's expected local entry count, presizing every
    /// stripe up front; see [`crate::cache::CacheBuilder::capacity_hint`]
    /// for the contract. Rebuilds the engine, carrying forward any
    /// [`Shard::with_weigher`] already set, in either order. Call right
    /// after [`Shard::new`], before any reads or writes reach this shard.
    #[must_use]
    pub fn with_capacity_hint(mut self, hint: u64) -> Self {
        self.capacity_hint = Some(hint);
        self.engine = self.build_engine();
        self
    }

    /// A fresh engine from this shard's capacity, TTI, weigher, capacity
    /// hint, ceiling and the current engine's pre-fold flag: the rebuild
    /// every engine-shaping builder shares, so each carries the others'
    /// settings forward. Counts resident bytes only under a ceiling.
    fn build_engine(&self) -> Arc<Engine<K, V>> {
        let engine = Engine::new(
            self.max_capacity,
            self.tti,
            self.weigher.clone().map(boxed_weigher),
            self.capacity_hint,
        );
        engine.set_prefold_enabled(self.engine.prefold_enabled());
        Arc::new(if self.max_resident_bytes == u64::MAX {
            engine
        } else {
            engine.count_resident_bytes()
        })
    }

    /// Per-stripe `live` arena capacities, in stripe order. `#[doc(hidden)]`
    /// test/benchmark accessor for an integration-test binary outside this crate.
    #[doc(hidden)]
    pub fn stripe_capacities(&self) -> Vec<usize> {
        self.engine.stripe_capacities()
    }

    /// Overrides the clock every timestamp this shard stamps reads from, in
    /// place of the system clock. Reserved for a deterministic-clock fuzz
    /// harness driving this shard's notion of time.
    #[doc(hidden)]
    #[must_use]
    pub fn with_clock(mut self, now_ms: Arc<dyn Fn() -> u64 + Send + Sync>) -> Self {
        self.clock_fn = now_ms;
        self.rebuild_resolver();
        self
    }

    /// This shard's cache name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// This shard's clustering mode.
    #[must_use]
    pub const fn mode(&self) -> Mode {
        self.mode
    }

    /// Whether [`Shard::with_ownership`] attached a residency set to this
    /// shard. Test-only: production code reads `residency` directly once
    /// anti-entropy's cohort widening consumes it.
    #[cfg(test)]
    pub(super) fn has_residency(&self) -> bool {
        self.residency.is_some()
    }

    /// The timestamp, in epoch milliseconds, this shard stamps its writes,
    /// deadlines, and sweeps with. The system clock, unless overridden by
    /// [`Shard::with_clock`].
    pub(super) fn now_ms(&self) -> u64 {
        (self.clock_fn)()
    }

    pub(super) fn stamp_local(&self) -> Hlc {
        let now = self.now_ms();
        let (stamp, lead_ms) = {
            let mut clock = self.clock.lock().unwrap_or_else(PoisonError::into_inner);
            let lead_ms = clock.lead_ms(now);
            (clock.now(now), lead_ms)
        };
        if clock_jumped(lead_ms, self.max_clock_skew_ms)
            && !self.clock_jump_warned.swap(true, Ordering::Relaxed)
        {
            tracing::warn!(
                cache = %self.name,
                behind_ms = lead_ms,
                "this node's clock is further behind its own last write stamp than max_clock_skew: \
                 the system clock stepped back, or it was set ahead and corrected; writes stay \
                 ordered at the last stamp until the clock catches up"
            );
        }
        stamp
    }

    /// Merges `remote` into this shard's clock and returns `true`, or
    /// refuses it and returns `false` when it is stamped more than
    /// `max_clock_skew_ms` ahead of this node's clock: a refused record is
    /// not applied, and counts in `sundog_clock_skew_rejected_total{cache}`.
    /// This method only refuses: whether the record is lost is the caller's
    /// to decide. A record from a replication, anti-entropy or transfer is
    /// offered again by its sender, while a forward is the only copy and the
    /// caller holds it (see `Shard::hold_skewed`).
    pub(super) fn observe_remote(&self, remote: Hlc) -> bool {
        let now = self.now_ms();
        let accepted = {
            let mut clock = self.clock.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(max_skew_ms) = self.max_clock_skew_ms {
                clock.observe_bounded(now, remote, max_skew_ms).is_some()
            } else {
                clock.observe(now, remote);
                true
            }
        };
        if !accepted {
            self.skew_rejected.increment(1);
            let ahead_ms = remote.wall_ms.saturating_sub(now);
            if self.skew_warned.swap(true, Ordering::Relaxed) {
                tracing::debug!(cache = %self.name, writer = %remote.node, ahead_ms, "refused a record stamped past max_clock_skew");
            } else {
                tracing::warn!(
                    cache = %self.name,
                    writer = %remote.node,
                    ahead_ms,
                    "refused a record stamped further ahead of this node's clock than \
                     max_clock_skew; the writer's clock runs fast, or this one runs slow. \
                     sundog_clock_skew_rejected_total counts every refusal"
                );
            }
        }
        accepted
    }

    /// Merges a stamp this shard derived from one it already holds, such
    /// as a compacted record's, with no skew bound: the stamp it came from
    /// passed [`Shard::observe_remote`] or was stamped here.
    pub(super) fn observe_derived(&self, derived: Hlc) {
        self.clock
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .observe(self.now_ms(), derived);
    }

    /// Folds the greatest version a warm reopen replayed into this shard's
    /// clock with no skew bound, so every later local stamp exceeds it: the
    /// stamps were written here, and a bound would re-create the lost
    /// write. Returns whether the clock now runs ahead of the system clock by
    /// more than `max_clock_skew`, which it warns about once: the node's
    /// writes stay ordered at the replayed stamp, and peers may refuse them
    /// until the system clock catches up.
    #[cfg(feature = "spill")]
    pub(super) fn seed_clock_from_replay(&self, max_ver: Option<Hlc>) -> bool {
        let Some(ver) = max_ver else {
            return false;
        };
        self.observe_derived(ver);
        let lead_ms = self
            .clock
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .lead_ms(self.now_ms());
        let jumped = clock_jumped(lead_ms, self.max_clock_skew_ms);
        if jumped {
            tracing::warn!(
                cache = %self.name,
                ahead_ms = lead_ms,
                "a warm reopen replayed a stamp further ahead of this node's clock than \
                 max_clock_skew: the system clock restarted behind it. Writes stay ordered \
                 at the replayed stamp until the clock catches up, and peers may refuse them \
                 as stamped ahead until then"
            );
        }
        jumped
    }

    /// The absolute expiry a write stamped `stamp_wall_ms` carries: the
    /// per-write `ttl` if given, else the shard's configured default, else
    /// none. Only this stamp is per-cache; everything downstream reads each
    /// record's own `expires_at_ms`.
    ///
    /// A merging resolver decides whether a side lapsed by comparing its
    /// deadline with the other write's stamp, and a stamp runs ahead of the
    /// physical clock by as much as the clock has absorbed from a fast peer.
    /// The deadline of a merging shard therefore counts from the later of the
    /// physical clock and the stamp, so it lies past the stamp of the write
    /// that set it. Every other resolver counts from the physical clock.
    pub(super) fn expiry_for(&self, stamp_wall_ms: u64, ttl: Option<Duration>) -> Option<u64> {
        let now = self.now_ms();
        let base = if self.resolver.merges() {
            now.max(stamp_wall_ms)
        } else {
            now
        };
        ttl.or(self.ttl)
            .map(|d| base.saturating_add(duration_ms(d)))
    }
}

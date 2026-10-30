//! Cluster-wide tunables: the values and their defaults, consumed by the
//! `Cluster` builder.

use crate::store::CompactionBounds;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

#[cfg(feature = "tls")]
use std::sync::Arc;

#[cfg(feature = "tls")]
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use crate::wire::MAX_FRAME;

/// Mutual-TLS material for the data-plane mesh (feature `tls`). Set
/// [`ClusterConfig::tls`] to wrap every data-plane connection in TLS;
/// client certificates are verified against `root_ca_certs` too.
///
/// A node with `tls: None` and one with `tls: Some(_)` cannot join the same
/// mesh: every connection between them fails outright rather than silently
/// downgrading. Every certificate must carry `net::MESH_SERVER_NAME` as a
/// DNS SAN. A `sim`-feature build stays plaintext regardless of this
/// field.
#[cfg(feature = "tls")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsConfig {
    /// This node's certificate chain, leaf first, DER-encoded.
    pub cert_chain: Vec<CertificateDer<'static>>,
    /// This node's private key, matching `cert_chain`'s leaf certificate.
    /// `Arc`-wrapped so `TlsConfig` stays cheaply `Clone`.
    pub private_key: Arc<PrivateKeyDer<'static>>,
    /// DER-encoded root CA certificate(s) trusted on both sides of the
    /// handshake.
    pub root_ca_certs: Vec<CertificateDer<'static>>,
}

/// Tunable knobs for a running cluster. Every field has a sane zeroconf
/// default; `Cluster::builder` exposes setters only for the ones worth
/// overriding day to day.
///
/// `#[non_exhaustive]`: a field added here in a future release must not
/// break code that only overrides a few knobs. Use [`ClusterConfig::default`]
/// and [`ClusterConfig::with`] to change a subset of fields.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ClusterConfig {
    /// Base interval between a node's anti-entropy rounds. The actual delay
    /// is jittered around this value to avoid thundering-herd rounds.
    pub ae_interval: Duration,
    /// How long a tombstone is retained before garbage collection. Must be
    /// at least `3 * ae_interval` so a lagging peer gets a few anti-entropy
    /// rounds to observe the deletion first, and at least
    /// `ae_interval * (2 * distributed_disown_grace_rounds + 2)`, the bucket
    /// release window: a `Mode::Distributed` node that lost a bucket keeps a
    /// copy that no removal reaches until its hand-off round, up to twice
    /// the grace later, and that round pushes whatever the owners lack. With
    /// the tombstone still there the stale copy loses; without it a removed
    /// key comes back. Opening a `Mode::Distributed` cache rejects a shorter
    /// retention with `CacheError::TombstoneTtlInsideReleaseWindow`.
    ///
    /// While a member is absent, a cache defers collection past this point,
    /// up to [`tombstone_max_ttl`](Self::tombstone_max_ttl), and for another
    /// `tombstone_ttl` once the member is back, so that member can't
    /// resurrect the entry via anti-entropy on return. A `Mode::Distributed`
    /// cache counts only a member that shares a part with this node once it
    /// is eligible again. A `Mode::Replicated` or `Mode::Distributed` cache
    /// also defers collection for the first `tombstone_ttl` after it opens,
    /// whoever is absent, since a node that just opened can hold a copy a
    /// peer's tombstone has to delete, and for another `tombstone_ttl` after
    /// a pause: a wall-clock gap of more than twice the collection period,
    /// a quarter of this retention and at least a second, between two of its
    /// tombstone collections. A stopped or suspended process leaves such a
    /// gap, and misses the deletes its peers make meanwhile. The gap is half
    /// of this retention from 4 seconds up.
    ///
    /// Every tombstone counts this retention from an instant all replicas
    /// agree on, capped at the receiving node's clock: the delete's version
    /// stamp for a delete, the expiry for the tombstone of an expired entry.
    /// A tombstone a peer sends that arrives already past its retention is
    /// ignored and counted in `sundog_tombstones_ignored_total{cache}`; a
    /// node's own delete is always stored. Keep node clocks well within
    /// this retention of each other.
    ///
    /// In a `Mode::Replicated` or `Mode::Distributed` cache whose resolver
    /// does not merge, the sweep also leaves a tombstone for an entry it
    /// removes for having expired, kept for this retention counted from the
    /// entry's expiry and never deferred past it, as Cassandra keeps a
    /// tombstone for `gc_grace_seconds`. `sundog_tombstones{cache}` counts the
    /// tombstones a node holds.
    pub tombstone_ttl: Duration,
    /// Hard cap on tombstone retention regardless of any member's absence.
    /// Bounds [`tombstone_ttl`](Self::tombstone_ttl)'s deferral against a
    /// member that never comes back. A node that defers, for an absent
    /// member, after opening or after a pause, counts this cap from a
    /// delete's version stamp, as `tombstone_ttl`: it keeps its deletes to
    /// it, and ignores a delete a peer sends after it. The tombstone of an
    /// expired entry is not deferred and has no cap past `tombstone_ttl`. While a member is absent, a cache holds one tombstone
    /// per delete made in the last `tombstone_max_ttl`.
    pub tombstone_max_ttl: Duration,
    /// How far ahead of this node's clock a remote write's version stamp
    /// may be. A replicated write, invalidation, anti-entropy repair or
    /// transferred record stamped further ahead is refused: not applied,
    /// its stamp not merged into this node's clock, and counted in
    /// `sundog_clock_skew_rejected_total{cache}`. Once this node's clock
    /// comes within the bound, anti-entropy delivers the record like any
    /// other. `None` accepts any stamp, and then one node whose clock runs
    /// an hour fast wins every conflicting write and pulls every other
    /// node's clock an hour forward.
    ///
    /// The bound caps how far a writer's clock can lead a receiver's, which
    /// delete retention relies on. It does not bound a receiver running ahead
    /// of a writer or a writer running behind. A tombstone counts
    /// [`tombstone_ttl`](Self::tombstone_ttl) from its version stamp, capped
    /// at the receiver's clock, so a peer can take a delete stamped ahead of a
    /// receiver's clock again until that clock passes the stamp. The
    /// tombstone lives until its stamp plus `tombstone_ttl`, one collection
    /// period and the stamp's lead over the slowest clock; this bound caps the
    /// lead. With `None` the lead is unbounded, and so is the lifetime.
    ///
    /// A `Mode::Distributed` write that a node forwards without owning its
    /// part leaves no copy behind for anti-entropy to offer, so an owner that
    /// refuses its stamp holds the record in memory and applies it once its
    /// clock is within the bound, after about the stamp's lead minus this
    /// bound. The hold takes 16,384 records and 32 MiB per cache, and a
    /// record whose wait would pass the shorter of
    /// [`tombstone_max_ttl`](Self::tombstone_max_ttl) and a day, a stamp ahead
    /// by more than this bound plus that limit, is not held. A record past
    /// those limits is dropped and counted in
    /// `sundog_clock_skew_hold_dropped_total{cache, reason}`; a held record
    /// is never evicted to make room, so a merging resolver loses the
    /// contribution that arrives at a full hold. A crash loses
    /// the hold; a closing cache hands it to the other owners. While a record
    /// is held the owner's clock has not absorbed its stamp, so a write made
    /// meanwhile stamps below it and loses to it once it applies.
    ///
    /// The same bound flags this node's own clock: a node whose system
    /// clock falls further than this behind its own last stamp logs one
    /// warning per cache. Its writes stay ordered, held at that last stamp
    /// until the clock catches up.
    pub max_clock_skew: Option<Duration>,
    /// How long a gone, non-local writer incarnation must stay gone,
    /// whether it crashed or left through `Cluster::shutdown` (or be
    /// superseded by a live incarnation of the same node), before its
    /// `PnCounter`/`OrSet` slot becomes eligible for the CRDT compaction
    /// sweep's stage-one retirement. Defaults to
    /// [`tombstone_max_ttl`](Self::tombstone_max_ttl), the same bound this
    /// crate already documents as "a member gone longer than this may
    /// resurrect data" if it returns, and the trust boundary CRDT
    /// compaction's writer-retirement rule relies on for the same reason.
    ///
    /// A writer whose node this node never saw as a member counts as gone
    /// from the later of this node's start and the sweep's first sight of
    /// the writer, so the slots earlier deploys left retire once this node
    /// has run for this long, and values stay bounded once any node runs
    /// that long. A fleet that replaces every node faster than this lowers
    /// it, or merged values keep a slot for every deploy.
    ///
    /// A `Replicated` or `Distributed` cache folds a retirement into a
    /// bounded total once it has aged past twice this bound. An
    /// `Invalidation` cache retires but never folds, since its nodes hold
    /// different writers, so there a value keeps one small retired entry for
    /// every dead writer.
    pub crdt_retire_after: Duration,
    /// How many resident CRDT records one `compact_pass` call examines
    /// before yielding to the executor. A tick keeps calling until the
    /// whole keyspace has been examined, so this bounds the stall per call,
    /// not the work per tick.
    pub crdt_compact_batch: usize,
    /// How often the CRDT compaction sweep runs. `None`, the default, is a
    /// quarter of [`crdt_retire_after`](Self::crdt_retire_after) floored at
    /// 30 seconds ([`crdt_sweep_period`](Self::crdt_sweep_period)); a test
    /// cluster with a bound of seconds sets this to something shorter,
    /// which also shortens the fold receipt lifetime, counted from the fold
    /// ([`crdt_compaction_bounds`](Self::crdt_compaction_bounds)) the
    /// sweep period feeds.
    pub crdt_sweep_interval: Option<Duration>,
    /// Bounded capacity of each per-peer outbox (`mpsc`) on the data plane.
    pub outbox_capacity: usize,
    /// How much memory each of a peer's two outboxes holds, its replicate
    /// outbox and its invalidate queue, alongside
    /// [`outbox_capacity`](Self::outbox_capacity)'s bound in frames: what bounds
    /// the memory one peer's queued records take while it reads slower than
    /// this node writes, or not at all. A queued frame counts its encoded bytes
    /// and its decoded records, each record's struct and its key and value
    /// bytes. A `Replicated` write that finds the replicate outbox full is
    /// dropped and left to anti-entropy, as one that finds every frame slot
    /// taken is; a `Distributed` fan-out waits for room, up to
    /// [`fan_out_stall_timeout`](Self::fan_out_stall_timeout). An invalidation,
    /// or an `Invalidation` cache's merged records, that finds the invalidate
    /// queue full drops its oldest frames instead. A frame that weighs more than
    /// this goes in alone. Must be nonzero: `ClusterBuilder::build` rejects
    /// zero. Default: 64 MiB.
    pub outbox_bytes: usize,
    /// How long a data-plane connection may stay silent before the kernel
    /// probes the peer: TCP keepalive on every connection this node dials or
    /// accepts. Probes then repeat every third of this, three times, where
    /// the platform lets a socket set the interval and count; elsewhere the
    /// system's own apply. A silent connection to a host that vanished
    /// without closing it, through a power loss, a lost VM or a partition,
    /// errors and is released after about twice this. A connection that only
    /// receives would otherwise be held until the process exits. One with
    /// data the peer has not acknowledged is not probed: the kernel's
    /// retransmission timeout, about 15 minutes on Linux, releases it
    /// instead. Must be between 1 second and 32,767 seconds, the whole
    /// seconds a kernel accepts: `ClusterBuilder::build` rejects any other
    /// value. `None` leaves keepalive off. Defaults to 30 seconds.
    pub mesh_keepalive: Option<Duration>,
    /// Hard cap on a single wire frame, in bytes. Must not exceed
    /// [`crate::wire::MAX_FRAME`]; `build()` rejects a higher value with
    /// [`crate::error::JoinError::InvalidConfig`]. A write whose record
    /// would not fit one frame fails with
    /// [`CacheError::ValueTooLarge`](crate::error::CacheError::ValueTooLarge),
    /// and so does a merge that would leave a value past the cap and larger
    /// than it was; a merge from a peer that would is refused and counted in
    /// `sundog_merge_refused_total`. Keep it equal on every node: a node under
    /// a smaller cap refuses a merged value a node under a larger one
    /// builds.
    pub max_frame: usize,
    /// Bind address for the gossip (membership) UDP socket. Port `0` picks the
    /// zeroconf default.
    pub gossip_bind_addr: SocketAddr,
    /// Bind address for the data-plane TCP listener. Port `0` picks the
    /// zeroconf default.
    pub data_bind_addr: SocketAddr,
    /// Cadence of chitchat's own SWIM gossip rounds, distinct from
    /// [`ae_interval`](Self::ae_interval). Chosen for sub-5s failure
    /// detection on a LAN.
    pub gossip_interval: Duration,
    /// Phi-accrual failure-detector suspicion threshold: a peer is flagged
    /// faulty once its accrual value crosses this. Higher tolerates more
    /// jitter at the cost of slower detection.
    pub phi_threshold: f64,
    /// Sample window size behind the phi-accrual calculation.
    pub phi_sampling_window_size: usize,
    /// Upper bound on the failure detector's inter-heartbeat interval; wider
    /// gaps are dropped from the sample window.
    pub phi_max_interval: Duration,
    /// Initial assumed heartbeat interval, used before the failure detector
    /// has enough samples of its own to adapt.
    pub phi_initial_interval: Duration,
    /// How long a dead node's chitchat state is retained before this node
    /// forgets it. Bounded well below chitchat's own 24h default.
    pub dead_node_grace_period: Duration,
    /// Grace period for tombstoned chitchat key-values, distinct from this
    /// crate's own cache tombstones, before chitchat garbage-collects them.
    pub kv_tombstone_grace_period: Duration,
    /// Wall-clock budget for the state transfer a [`Mode::Replicated`] cache
    /// runs inside `open()`: how long a joining node pulls a snapshot from
    /// donors before giving up and proceeding with whatever it has. A
    /// startup-latency bound, not a correctness one: anti-entropy repairs
    /// whatever the cut-off transfer didn't deliver. A fifth of it is the
    /// grace a node with no peer in sight waits for gossip to show one
    /// before it opens as the cluster's origin. A cache whose transfer times
    /// out opens cold, declining to donate, and keeps pulling in the
    /// background every `ae_interval`; after three timed-out pulls it opens
    /// warm with what landed. Zero is honored: `open()` skips the transfer
    /// entirely and the cache is warm with what it has. A [`Mode::Distributed`]
    /// part this node gains after open is pulled under the same budget; a pull
    /// that times out is retried every `ae_interval` until it lands, and the
    /// part reads as cold, never from a possibly incomplete copy, until then.
    /// A warm reopen's
    /// reconciliation loop (see [`reconcile_byte_budget`](Self::reconcile_byte_budget))
    /// also runs on this bound, and so does the wait of a
    /// [`Mode::Distributed`] part this node owns alone because the failure
    /// detector dropped its other owners: the part stays cold until they
    /// return or this long has passed, then serves what this node holds.
    /// A graceful [`Cluster::shutdown`](crate::Cluster::shutdown) of a node
    /// with [`Mode::Distributed`] caches also runs on it: the node
    /// reconciles its parts with each live co-owner first, every co-owner of
    /// every cache gets an equal share of the time left, and a round that
    /// fails is retried within the share.
    ///
    /// [`Mode::Replicated`]: crate::Mode::Replicated
    /// [`Mode::Distributed`]: crate::Mode::Distributed
    pub state_transfer_budget: Duration,
    /// Bucket size past which an anti-entropy responder answers a mismatch
    /// with its 64 part digests instead of a listing or sketch, narrowing
    /// the mismatch to whichever parts differ before either side sends
    /// anything at bucket scale. Each mismatched part is then answered
    /// by the same rule [`ae_sketch_min_bucket`](Self::ae_sketch_min_bucket)
    /// applies at bucket scale: a sketch past that many entries or when its
    /// listing would pass half a frame, a listing otherwise. Effectively a
    /// three-tier responder rule: part digests, then sketch, then listing.
    pub ae_part_min_bucket: usize,
    /// Bucket size past which an anti-entropy responder answers a mismatch
    /// with an IBLT sketch instead of the bucket's full listing; the same
    /// rule applies to a mismatched part once
    /// [`ae_part_min_bucket`](Self::ae_part_min_bucket) has narrowed a
    /// bucket to it. A sketch costs a fixed ~9 KB at the default
    /// `ae_sketch_cells`; a listing costs ~23 bytes per entry, so 384 is the
    /// crossover for small keys. A listing is one frame, so a listing that
    /// would pass 2 MiB, half of the largest wire frame, goes out as a sketch
    /// whatever the bucket's size: a bucket of long keys answers with a
    /// sketch below this threshold.
    pub ae_sketch_min_bucket: usize,
    /// Cell count of the IBLT sketch built for a bucket past
    /// [`ae_sketch_min_bucket`](Self::ae_sketch_min_bucket), and for a bucket
    /// or part whose listing would pass half a frame. The default of
    /// 240 decodes a difference of up to 100 elements in at least 99% of
    /// cases; an undecodable one falls back to a full listing, which the
    /// responder streams in frames of about 2 MiB. A count whose
    /// sketch cannot fit in one [`max_frame`](Self::max_frame) frame fails
    /// [`ClusterBuilder::build`](crate::ClusterBuilder::build).
    pub ae_sketch_cells: usize,
    /// The IP this node advertises for both gossip and the data plane,
    /// overriding the automatic outbound-interface probe. `None` (the
    /// default) resolves it automatically: a UDP-connect probe toward a
    /// public address, falling back to the first non-loopback,
    /// non-link-local local interface address, and finally to loopback.
    ///
    /// Set this behind NAT or a container port mapping, where the interface
    /// address a probe or `if-addrs` finds is not the address peers must
    /// dial: a cloud instance's public IP while the process binds its
    /// private one, a container's externally published port, or a fixed
    /// address under host networking. When set, no probe runs at all.
    pub advertise_ip: Option<IpAddr>,
    /// Mutual-TLS material for the data-plane mesh; `None` (the default)
    /// means the mesh runs plaintext. See [`TlsConfig`]. A call to
    /// [`ClusterBuilder::tls`](crate::ClusterBuilder::tls) takes precedence
    /// over this field.
    #[cfg(feature = "tls")]
    pub tls: Option<TlsConfig>,
    /// How many anti-entropy intervals a `Mode::Distributed` node keeps a
    /// disowned bucket's data resident before physically releasing it, so a
    /// new owner's rebalance pull has time to land against it as donor
    /// first. Default: 3. Bounded above by
    /// [`tombstone_ttl`](Self::tombstone_ttl), which must cover
    /// [`bucket_release_window`](Self::bucket_release_window),
    /// `ae_interval * (2 * rounds + 2)`.
    pub distributed_disown_grace_rounds: u32,
    /// Per-owner-attempt timeout for a `Mode::Distributed` cache's read
    /// against a remote owner. Deliberately shorter than a general request
    /// timeout: a read tries owners in sequence, so each attempt should
    /// fail fast. Default: 750ms.
    pub fetch_timeout: Duration,
    /// Maximum simultaneous bucket-transfer streams one rebalance pass
    /// opens for a `Mode::Distributed` cache, so a mass membership change
    /// can't spawn dozens of concurrent transfers at once. Default: 4.
    pub rebalance_concurrency: usize,
    /// Target size in bytes for `st_bucket_chunks` sub-batches of a
    /// bucket's key list. Default: 1 MiB.
    /// [`rebalance_chunk_bytes_value`](Self::rebalance_chunk_bytes_value)
    /// clamps this to `MAX_FRAME - SNAPSHOT_CHUNK_ENVELOPE_HEADROOM` so a
    /// `Msg::StBucketChunk` frame can't exceed the wire cap.
    pub rebalance_chunk_bytes: u64,
    /// How long a `Msg::StBucketAck` is trusted as proof a bucket is
    /// reconciled, before `buckets_to_release` falls back to the
    /// timer-and-anti-entropy path. Default: 60s, twice
    /// [`ae_interval`](Self::ae_interval); an ack predating the current
    /// `OwnershipView` is never trusted.
    /// [`rebalance_ack_window_value`](Self::rebalance_ack_window_value)
    /// bounds this by `distributed_disown_grace_rounds * ae_interval` -- an
    /// ack only speeds up confirmation, never bypasses the grace-round
    /// floor. A node drops an ack once it is older than the window, and the
    /// acks of a peer that leaves with it.
    pub rebalance_ack_window: Duration,
    /// Backlog capacity, in keys, for a cache's fan-out queue. An async
    /// write (`Cache::insert` and friends) awaits room under this cap, up
    /// to [`fan_out_wait_timeout`](Self::fan_out_wait_timeout), then
    /// proceeds anyway over capacity -- the write always lands, and
    /// overflow is counted in `sundog_fan_out_wait_timeouts_total{cache}`.
    /// The synchronous push path (`Cache::insert_sync`, `remove_sync`,
    /// `Shard::apply`) never waits on this. Must be nonzero:
    /// `ClusterBuilder::build` rejects zero. Default: 262,144.
    pub fan_out_backlog_capacity: usize,
    /// How long an async write waits for room under
    /// [`fan_out_backlog_capacity`](Self::fan_out_backlog_capacity) before
    /// proceeding regardless. Default: 30s.
    pub fan_out_wait_timeout: Duration,
    /// How long a `Mode::Distributed` fan-out batch waits on an owner whose
    /// outbox takes nothing. A batch sends to every owner at once, and waits
    /// on each for as long as its outbox keeps taking frames, which its
    /// writer takes at most 1 MiB at a time as the owner reads; an owner that
    /// takes none for this long while gossip still has it live has the rest
    /// of its frames dropped and is marked dirty for anti-entropy, counted
    /// once per stall in `sundog_fan_out_stalls_total`, and later batches
    /// send it what fits without waiting until it reads a frame again. A
    /// forwarded
    /// write that no owner took, and a departed write's invalidation an owner
    /// missed, are lost, counted in `sundog_fan_out_lost_writes_total{cache}`.
    /// `Duration::MAX` waits for as long as the owner stays live. Must be
    /// nonzero: `ClusterBuilder::build` rejects zero. Default: 10s.
    pub fan_out_stall_timeout: Duration,
    /// Per-peer wire-byte budget for a warm reopen's reconciliation loop
    /// (`Cache::reconcile_warm_buckets`). A peer's still-diverging buckets
    /// fall through to the cold-pull path once its rounds push and pull
    /// this many bytes, paired with the round cap in
    /// [`state_transfer_budget`](Self::state_transfer_budget). Sized so a
    /// bucket's full listing fits in a handful of rounds without letting a
    /// re-diverging peer stall `open()` indefinitely. Default: 32 MiB.
    pub reconcile_byte_budget: u64,
}

impl ClusterConfig {
    /// Returns `true` if `tombstone_ttl` satisfies the `>= 3 * ae_interval`
    /// rule.
    #[must_use]
    pub fn tombstone_ttl_is_safe(&self) -> bool {
        self.tombstone_ttl >= self.ae_interval.saturating_mul(3)
    }

    /// The longest a `Mode::Distributed` node holds a bucket it no longer
    /// owns before its hand-off round pushes that copy to the owners:
    /// `ae_interval * (2 * distributed_disown_grace_rounds + 2)`, twice the
    /// disown grace plus one interval for the round and one of slack.
    /// [`tombstone_ttl`](Self::tombstone_ttl) must cover it.
    #[must_use]
    pub fn bucket_release_window(&self) -> Duration {
        self.ae_interval.saturating_mul(
            self.distributed_disown_grace_rounds
                .saturating_mul(2)
                .saturating_add(2),
        )
    }

    /// How often the CRDT compaction sweep runs:
    /// [`crdt_sweep_interval`](Self::crdt_sweep_interval) if set, else a
    /// quarter of [`crdt_retire_after`](Self::crdt_retire_after) floored at
    /// 30 seconds, since retirement eligibility rather than traffic is what
    /// changes over time, and a full pass examines every resident record.
    #[must_use]
    pub fn crdt_sweep_period(&self) -> Duration {
        self.crdt_sweep_interval
            .unwrap_or_else(|| (self.crdt_retire_after / 4).max(Duration::from_secs(30)))
    }

    /// The [`CompactionBounds`] the sweep and the merge apply path share
    /// under this configuration: `crdt_retire_after` with a fold receipt
    /// living the longer of three bounds and two bounds plus two sweep
    /// periods past the fold, so it outlives every reachable replica's own
    /// fold of the same writer.
    #[must_use]
    pub fn crdt_compaction_bounds(&self) -> CompactionBounds {
        CompactionBounds::new(self.crdt_retire_after, self.crdt_sweep_period())
    }

    /// [`rebalance_chunk_bytes`](Self::rebalance_chunk_bytes) clamped to
    /// `MAX_FRAME - SNAPSHOT_CHUNK_ENVELOPE_HEADROOM`.
    #[must_use]
    pub fn rebalance_chunk_bytes_value(&self) -> u64 {
        let cap = (MAX_FRAME - crate::store::SNAPSHOT_CHUNK_ENVELOPE_HEADROOM) as u64;
        self.rebalance_chunk_bytes.min(cap)
    }

    /// How long a `Mode::Distributed` node keeps serving a part it no longer
    /// owns: `distributed_disown_grace_rounds * ae_interval`, saturating.
    #[must_use]
    pub(crate) fn disown_grace(&self) -> Duration {
        self.ae_interval
            .saturating_mul(self.distributed_disown_grace_rounds)
    }

    /// Upper bound for [`rebalance_ack_window`](Self::rebalance_ack_window):
    /// `distributed_disown_grace_rounds * ae_interval`.
    #[must_use]
    pub fn rebalance_ack_window_max(&self) -> Duration {
        self.disown_grace()
    }

    /// [`rebalance_ack_window`](Self::rebalance_ack_window) clamped to
    /// [`rebalance_ack_window_max`](Self::rebalance_ack_window_max).
    #[must_use]
    pub fn rebalance_ack_window_value(&self) -> Duration {
        self.rebalance_ack_window
            .min(self.rebalance_ack_window_max())
    }

    /// Applies `f` to a mutable borrow of `self` and returns it: how code
    /// outside this crate overrides a subset of fields on
    /// [`ClusterConfig::default`] without breaking when a field is added.
    #[must_use]
    pub fn with(mut self, f: impl FnOnce(&mut Self)) -> Self {
        f(&mut self);
        self
    }
}

/// The shortest [`ClusterConfig::mesh_keepalive`] `ClusterBuilder::build`
/// accepts.
pub(crate) const MESH_KEEPALIVE_MIN: Duration = Duration::from_secs(1);
/// The longest [`ClusterConfig::mesh_keepalive`] `ClusterBuilder::build`
/// accepts: Linux's `MAX_TCP_KEEPIDLE`.
pub(crate) const MESH_KEEPALIVE_MAX: Duration = Duration::from_secs(32_767);

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            ae_interval: Duration::from_secs(30),
            tombstone_ttl: Duration::from_mins(10),
            tombstone_max_ttl: Duration::from_hours(24),
            max_clock_skew: Some(Duration::from_mins(1)),
            crdt_retire_after: Duration::from_hours(24),
            crdt_compact_batch: 4_096,
            crdt_sweep_interval: None,
            outbox_capacity: 8_192,
            outbox_bytes: 64 * 1024 * 1024,
            mesh_keepalive: Some(Duration::from_secs(30)),
            max_frame: MAX_FRAME,
            gossip_bind_addr: SocketAddr::from(([0, 0, 0, 0], 0)),
            data_bind_addr: SocketAddr::from(([0, 0, 0, 0], 0)),
            gossip_interval: Duration::from_millis(200),
            phi_threshold: 6.0,
            phi_sampling_window_size: 1_000,
            phi_max_interval: Duration::from_secs(5),
            phi_initial_interval: Duration::from_millis(500),
            dead_node_grace_period: Duration::from_secs(600),
            kv_tombstone_grace_period: Duration::from_mins(15),
            state_transfer_budget: Duration::from_secs(20),
            ae_part_min_bucket: 4_096,
            ae_sketch_min_bucket: 384,
            ae_sketch_cells: 240,
            advertise_ip: None,
            #[cfg(feature = "tls")]
            tls: None,
            distributed_disown_grace_rounds: 3,
            fetch_timeout: Duration::from_millis(750),
            rebalance_concurrency: 4,
            rebalance_chunk_bytes: 1024 * 1024,
            rebalance_ack_window: Duration::from_secs(60),
            fan_out_backlog_capacity: 262_144,
            fan_out_wait_timeout: Duration::from_secs(30),
            fan_out_stall_timeout: Duration::from_secs(10),
            reconcile_byte_budget: 32 * 1024 * 1024,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_satisfy_the_tombstone_ttl_rule() {
        assert!(ClusterConfig::default().tombstone_ttl_is_safe());
    }

    #[test]
    fn bucket_release_window_spans_two_graces_plus_two_intervals() {
        let config = ClusterConfig::default().with(|c| {
            c.ae_interval = Duration::from_secs(3);
            c.distributed_disown_grace_rounds = 3;
        });
        assert_eq!(config.bucket_release_window(), Duration::from_secs(24));
        assert!(
            ClusterConfig::default().tombstone_ttl
                >= ClusterConfig::default().bucket_release_window()
        );
    }

    #[test]
    fn default_tombstone_max_ttl_bounds_the_default_tombstone_ttl() {
        let config = ClusterConfig::default();
        assert!(config.tombstone_max_ttl > config.tombstone_ttl);
    }

    #[test]
    fn with_overrides_only_the_touched_fields() {
        let config = ClusterConfig::default().with(|c| {
            c.ae_interval = Duration::from_millis(1);
        });
        assert_eq!(config.ae_interval, Duration::from_millis(1));
        assert_eq!(
            config.outbox_capacity,
            ClusterConfig::default().outbox_capacity
        );
    }

    #[test]
    fn default_ae_sketch_knobs_match_the_rated_sketch_shape() {
        let config = ClusterConfig::default();
        assert_eq!(config.ae_sketch_min_bucket, 384);
        assert_eq!(config.ae_sketch_cells, 240);
    }

    #[test]
    fn default_ae_part_min_bucket_is_four_thousand_ninety_six() {
        assert_eq!(ClusterConfig::default().ae_part_min_bucket, 4_096);
    }

    #[test]
    fn default_state_transfer_budget_is_twenty_seconds() {
        assert_eq!(
            ClusterConfig::default().state_transfer_budget,
            Duration::from_secs(20)
        );
    }

    #[test]
    fn defaults_bind_ephemeral_on_any_interface() {
        let config = ClusterConfig::default();
        assert_eq!(config.gossip_bind_addr.port(), 0);
        assert_eq!(config.data_bind_addr.port(), 0);
    }

    #[test]
    fn default_advertise_ip_is_none() {
        assert_eq!(ClusterConfig::default().advertise_ip, None);
    }

    #[test]
    fn default_mesh_keepalive_is_thirty_seconds() {
        assert_eq!(
            ClusterConfig::default().mesh_keepalive,
            Some(Duration::from_secs(30))
        );
    }

    #[test]
    fn default_max_clock_skew_is_one_minute() {
        assert_eq!(
            ClusterConfig::default().max_clock_skew,
            Some(Duration::from_mins(1))
        );
    }

    #[test]
    fn disown_grace_is_rounds_times_the_interval_and_saturates() {
        let config = ClusterConfig::default();
        assert_eq!(config.disown_grace(), config.ae_interval * 3);
        assert_eq!(config.rebalance_ack_window_max(), config.disown_grace());
        let huge = ClusterConfig {
            ae_interval: Duration::MAX,
            ..ClusterConfig::default()
        };
        assert_eq!(huge.disown_grace(), Duration::MAX);
    }

    #[test]
    fn default_distributed_disown_grace_rounds_is_three() {
        assert_eq!(ClusterConfig::default().distributed_disown_grace_rounds, 3);
    }

    #[test]
    fn default_fetch_timeout_is_750_ms() {
        assert_eq!(
            ClusterConfig::default().fetch_timeout,
            Duration::from_millis(750)
        );
    }

    #[test]
    fn default_rebalance_concurrency_is_four() {
        assert_eq!(ClusterConfig::default().rebalance_concurrency, 4);
    }

    #[test]
    fn default_rebalance_chunk_bytes_is_one_mebibyte_and_under_max_frame() {
        let config = ClusterConfig::default();
        assert_eq!(config.rebalance_chunk_bytes, 1024 * 1024);
        assert!(config.rebalance_chunk_bytes_value() < MAX_FRAME as u64);
        assert_eq!(
            config.rebalance_chunk_bytes_value(),
            config.rebalance_chunk_bytes
        );
    }

    #[test]
    fn rebalance_chunk_bytes_value_clamps_a_value_over_the_wire_cap() {
        let config = ClusterConfig::default().with(|c| {
            c.rebalance_chunk_bytes = MAX_FRAME as u64 * 4;
        });
        assert!(config.rebalance_chunk_bytes_value() < MAX_FRAME as u64);
        assert!(config.rebalance_chunk_bytes_value() < config.rebalance_chunk_bytes);
    }

    #[test]
    fn default_rebalance_ack_window_is_bounded_by_disown_grace() {
        let config = ClusterConfig::default();
        assert_eq!(config.rebalance_ack_window, Duration::from_secs(60));
        assert_eq!(
            config.rebalance_ack_window,
            config.ae_interval * 2,
            "documented default is twice ae_interval"
        );
        assert!(config.rebalance_ack_window_value() <= config.rebalance_ack_window_max());
        assert_eq!(
            config.rebalance_ack_window_value(),
            config.rebalance_ack_window
        );
    }

    #[test]
    fn rebalance_ack_window_value_is_bounded_by_disown_grace_rounds_times_ae_interval() {
        let config = ClusterConfig::default().with(|c| {
            c.rebalance_ack_window = Duration::from_secs(3600);
        });
        assert_eq!(
            config.rebalance_ack_window_max(),
            config.ae_interval * config.distributed_disown_grace_rounds
        );
        assert_eq!(
            config.rebalance_ack_window_value(),
            config.rebalance_ack_window_max()
        );
        assert!(config.rebalance_ack_window_value() < config.rebalance_ack_window);
    }

    #[test]
    fn default_crdt_retire_after_matches_tombstone_max_ttl() {
        let config = ClusterConfig::default();
        assert_eq!(config.crdt_retire_after, config.tombstone_max_ttl);
    }

    #[test]
    fn default_crdt_compact_batch_is_four_thousand_ninety_six() {
        assert_eq!(ClusterConfig::default().crdt_compact_batch, 4_096);
    }

    #[test]
    fn crdt_sweep_period_is_a_quarter_of_the_bound_floored_at_thirty_seconds() {
        let config = ClusterConfig::default();
        assert_eq!(config.crdt_sweep_period(), Duration::from_hours(6));
        let short = config
            .clone()
            .with(|c| c.crdt_retire_after = Duration::from_secs(60));
        assert_eq!(short.crdt_sweep_period(), Duration::from_secs(30));
        let overridden = short.with(|c| c.crdt_sweep_interval = Some(Duration::from_millis(250)));
        assert_eq!(overridden.crdt_sweep_period(), Duration::from_millis(250));
    }

    #[test]
    fn crdt_compaction_bounds_receipt_outlives_two_sweep_periods_at_any_bound() {
        for secs in [1, 2, 5, 60, 119, 120, 3_600, 86_400] {
            let config =
                ClusterConfig::default().with(|c| c.crdt_retire_after = Duration::from_secs(secs));
            let bounds = config.crdt_compaction_bounds();
            let period_ms = u64::try_from(config.crdt_sweep_period().as_millis()).expect("fits");
            assert!(
                bounds.receipt_ttl_ms >= 2 * period_ms,
                "bound {secs}s: receipt ttl {} ms must cover two sweep periods of {period_ms} ms, \
                 so a member that left between ticks is seen gone before the tick forgets it",
                bounds.receipt_ttl_ms
            );
        }
    }

    #[test]
    fn crdt_compaction_bounds_receipt_lives_the_longer_of_three_bounds_and_two_plus_two_periods() {
        let default = ClusterConfig::default().crdt_compaction_bounds();
        let day_ms = 24 * 60 * 60 * 1_000;
        assert_eq!(default.retire_after_ms, day_ms);
        assert_eq!(
            default.receipt_ttl_ms,
            3 * day_ms,
            "three bounds win over 48h + 12h"
        );

        let short = ClusterConfig::default()
            .with(|c| c.crdt_retire_after = Duration::from_secs(2))
            .crdt_compaction_bounds();
        assert_eq!(short.retire_after_ms, 2_000);
        assert_eq!(
            short.receipt_ttl_ms,
            2 * 2_000 + 2 * 30_000,
            "two bounds plus two 30s sweep periods win over a 6s triple bound"
        );
    }

    #[test]
    fn with_overrides_crdt_retire_after_and_batch_independently() {
        let config = ClusterConfig::default().with(|c| {
            c.crdt_retire_after = Duration::from_secs(1);
            c.crdt_compact_batch = 10;
        });
        assert_eq!(config.crdt_retire_after, Duration::from_secs(1));
        assert_eq!(config.crdt_compact_batch, 10);
        assert_eq!(
            config.tombstone_max_ttl,
            ClusterConfig::default().tombstone_max_ttl,
            "overriding the new knobs leaves unrelated fields at their defaults"
        );
    }

    #[test]
    fn defaults_disable_tls() {
        #[cfg(feature = "tls")]
        assert!(ClusterConfig::default().tls.is_none());
    }

    #[cfg(feature = "tls")]
    #[test]
    fn tls_config_is_cheaply_cloneable_and_comparable() {
        use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};

        let cert = CertificateDer::from(vec![1, 2, 3]);
        let key: PrivateKeyDer<'static> = PrivatePkcs8KeyDer::from(vec![4, 5, 6]).into();
        let tls = TlsConfig {
            cert_chain: vec![cert.clone()],
            private_key: Arc::new(key),
            root_ca_certs: vec![cert],
        };
        assert_eq!(tls.clone(), tls);
    }

    #[test]
    fn default_fan_out_backlog_capacity_is_262_144() {
        assert_eq!(ClusterConfig::default().fan_out_backlog_capacity, 262_144);
    }

    #[test]
    fn default_fan_out_wait_timeout_is_thirty_seconds() {
        assert_eq!(
            ClusterConfig::default().fan_out_wait_timeout,
            Duration::from_secs(30)
        );
    }

    #[test]
    fn default_outbox_bytes_is_64_mebibytes() {
        assert_eq!(ClusterConfig::default().outbox_bytes, 64 * 1024 * 1024);
    }

    #[test]
    fn default_fan_out_stall_timeout_is_ten_seconds() {
        assert_eq!(
            ClusterConfig::default().fan_out_stall_timeout,
            Duration::from_secs(10)
        );
    }

    #[test]
    fn default_reconcile_byte_budget_is_32_mebibytes() {
        assert_eq!(
            ClusterConfig::default().reconcile_byte_budget,
            32 * 1024 * 1024
        );
    }

    #[test]
    fn with_overrides_fan_out_backlog_capacity_independently() {
        let config = ClusterConfig::default().with(|c| {
            c.fan_out_backlog_capacity = 10;
        });
        assert_eq!(config.fan_out_backlog_capacity, 10);
        assert_eq!(
            config.fan_out_wait_timeout,
            ClusterConfig::default().fan_out_wait_timeout,
            "overriding the new capacity knob leaves the wait timeout at its default"
        );
    }
}

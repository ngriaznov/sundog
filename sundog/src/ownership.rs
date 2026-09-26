//! Ownership for a `Mode::Distributed` cache: which live nodes own each of a
//! cache's 65,536 parts, computed by rendezvous (highest-random-weight)
//! hashing over the cache's live, protocol-compatible peers. A view has a
//! [`Granularity`]: `Part` ranks every part on its own, keeping each node's
//! share within a few percent even at a hundred nodes; `Bucket` gives every
//! part of a bucket the bucket's owners, what a peer on an older protocol
//! computes. A node always counts itself eligible, so a lone node owns every
//! part: a valid, under-replicated cluster.
//!
//! [`OwnershipView`] is the immutable, point-in-time answer; [`OwnershipTracker`]
//! is the live-updating handle every reader shares. [`ResidencySet`] is a
//! separate, time-based layer tracking parts a node has stopped owning but
//! keeps serving for a grace period, so a new owner's transfer has time to
//! land.

use std::collections::{BTreeSet, HashMap};
use std::num::NonZeroU8;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use smol_str::SmolStr;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use xxhash_rust::xxh3::xxh3_64;

use crate::cluster::Cluster;
use crate::membership::{CacheModes, Peer};
use crate::node::NodeId;
use crate::store::part::{PART_SPACE, PartSet};
use crate::store::{BUCKET_COUNT, Mode, PART_COUNT, PartId};
use crate::wire;

/// The rendezvous (highest-random-weight) score of `node` for `bucket`:
/// `xxh3_64(node ++ bucket)`. Pure, total, and deterministic across
/// processes and releases.
pub(crate) fn rendezvous_score(node: NodeId, bucket: u16) -> u64 {
    let mut buf = [0u8; 10];
    buf[..8].copy_from_slice(&node.as_u64().to_le_bytes());
    buf[8..].copy_from_slice(&bucket.to_le_bytes());
    xxh3_64(&buf)
}

/// Rendezvous rank order: higher score first, ties to the lower [`NodeId`].
fn rank_order(a: &(u64, NodeId), b: &(u64, NodeId)) -> std::cmp::Ordering {
    b.0.cmp(&a.0).then(a.1.cmp(&b.1))
}

/// The first `k` of `scored` in [`rank_order`], by a full sort: the
/// reference for [`select_top`], on scores a test picks.
#[cfg(test)]
fn rank_by_score(mut scored: Vec<(u64, NodeId)>, k: u8) -> Vec<NodeId> {
    scored.sort_by(rank_order);
    scored
        .into_iter()
        .take(usize::from(k))
        .map(|(_, node)| node)
        .collect()
}

/// The owners of `bucket` among `eligible` by [`rank_by_score`]: the
/// reference a view's rankings are tested against.
#[cfg(test)]
fn owners_of_bucket(eligible: &[NodeId], bucket: u16, k: u8) -> Vec<NodeId> {
    let scored = eligible
        .iter()
        .map(|&node| (rendezvous_score(node, bucket), node))
        .collect();
    rank_by_score(scored, k)
}

/// `xxh3_64` over `eligible`, sorted and deduplicated first, so two nodes
/// that assembled the same live set in a different gossip-arrival order
/// still agree. Given equal `k`, equal `view_hash` implies identical
/// per-bucket ownership on both sides.
pub(crate) fn view_hash(eligible: &[NodeId]) -> u64 {
    let mut sorted: Vec<NodeId> = eligible.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    let mut buf = Vec::with_capacity(sorted.len() * 8);
    for node in &sorted {
        buf.extend_from_slice(&node.as_u64().to_le_bytes());
    }
    xxh3_64(&buf)
}

/// [`view_hash`] for a view at `granularity`: `Bucket` hashes exactly as
/// [`view_hash`] does, so a node agrees with a peer on an older protocol;
/// `Part` folds a marker in, so a part view never compares equal to a
/// bucket view over the same nodes.
pub(crate) fn view_hash_at(eligible: &[NodeId], granularity: Granularity) -> u64 {
    match granularity {
        Granularity::Bucket => view_hash(eligible),
        Granularity::Part => {
            let mut sorted: Vec<NodeId> = eligible.to_vec();
            sorted.sort_unstable();
            sorted.dedup();
            let mut buf = Vec::with_capacity(sorted.len() * 8 + PART_VIEW_MARKER.len());
            for node in &sorted {
                buf.extend_from_slice(&node.as_u64().to_le_bytes());
            }
            buf.extend_from_slice(PART_VIEW_MARKER);
            xxh3_64(&buf)
        }
    }
}

/// Appended to a part view's hash input; see [`view_hash_at`].
const PART_VIEW_MARKER: &[u8] = b"part-ownership";

/// How finely a view divides ownership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Granularity {
    /// Every part of a bucket has the bucket's owners: what a peer on a
    /// protocol before [`wire::PROTOCOL_PART_OWNERSHIP`] computes.
    Bucket,
    /// Every part is ranked on its own.
    Part,
}

/// `Part` when this node and every eligible peer other than `self_node`
/// speak [`wire::PROTOCOL_PART_OWNERSHIP`], `Bucket` otherwise, so a cluster
/// mid rolling upgrade keeps the ownership its older members compute and
/// switches once the last of them is gone.
#[must_use]
pub fn ownership_granularity(
    self_node: NodeId,
    peers: &[Peer],
    eligible: &[NodeId],
) -> Granularity {
    Granularity::for_protocols(
        eligible
            .iter()
            .filter(|&&node| node != self_node)
            .map(|&node| {
                peers
                    .iter()
                    .find(|peer| peer.node == node)
                    .map_or(0, |peer| peer.protocol)
            }),
    )
}

impl Granularity {
    /// `Part` when this build and every one of `peer_protocols` speak
    /// [`wire::PROTOCOL_PART_OWNERSHIP`], `Bucket` otherwise.
    fn for_protocols(peer_protocols: impl IntoIterator<Item = u16>) -> Self {
        let speaks = |protocol| wire::peer_supports(protocol, wire::PROTOCOL_PART_OWNERSHIP);
        if speaks(wire::PROTOCOL_VERSION) && peer_protocols.into_iter().all(speaks) {
            Self::Part
        } else {
            Self::Bucket
        }
    }
}

/// The nodes eligible to own a part of `cache`: live peers speaking at
/// least [`wire::PROTOCOL_DISTRIBUTED`] that currently advertise `cache` under
/// `Mode::Distributed` with this same `k`, plus `self_node` unconditionally,
/// since a node is always eligible for its own view regardless of what it
/// advertises about itself. A peer that's live but hasn't opened `cache`
/// yet, or opened it with a different `owners` count, is excluded: routing
/// a forwarded write or a rebalance pull to a node that can't decode it, or
/// has no shard to apply it to, is worse than never trying.
#[must_use]
pub fn eligible_owners(
    self_node: NodeId,
    peers: &[Peer],
    modes: &CacheModes,
    cache: &SmolStr,
    k: NonZeroU8,
) -> Vec<NodeId> {
    let mut eligible: Vec<NodeId> = peers
        .iter()
        .filter(|peer| peer.protocol >= wire::PROTOCOL_DISTRIBUTED)
        .filter(|peer| {
            modes
                .get(&peer.node)
                .and_then(|caches| caches.get(cache))
                .is_some_and(|mode| matches!(mode, Mode::Distributed { owners } if *owners == k))
        })
        .map(|peer| peer.node)
        .collect();
    if !eligible.contains(&self_node) {
        eligible.push(self_node);
    }
    eligible
}

/// The parts `view` owns that `peer` also owns: strict-ownership input for
/// anti-entropy's pairing predicate. A residency-widened cohort is a
/// separate, time-based layer on top of this, never a change to this
/// function.
pub(crate) fn shared_owned_parts(view: &OwnershipView, peer: NodeId) -> Vec<PartId> {
    view.owned_parts()
        .filter(|&part| view.owners_of(part).contains(&peer))
        .collect()
}

/// The parts gained and lost between two successive [`OwnershipView`]s for
/// the same cache: a pure set difference over each view's owned-part set,
/// whatever either view's granularity. `cluster::rebalance::rebalance_task`'s
/// trigger input.
#[must_use]
pub fn ownership_diff(old: &OwnershipView, new: &OwnershipView) -> (Vec<PartId>, Vec<PartId>) {
    let gained: Vec<PartId> = new.owned_parts().filter(|&p| !old.owns(p)).collect();
    let lost: Vec<PartId> = old.owned_parts().filter(|&p| !new.owns(p)).collect();
    (gained, lost)
}

/// One cache's computed ownership, current as of the eligible-node set that
/// built it. Immutable once built: a membership or cache-mode change produces
/// a whole new view via [`OwnershipView::compute_at`] or
/// [`OwnershipView::successor`], never a mutation, so a reader holding an
/// `Arc<OwnershipView>` sees a consistent snapshot for the whole of one
/// operation.
#[derive(Debug)]
pub struct OwnershipView {
    view_hash: u64,
    self_node: NodeId,
    /// The ranked nodes, `self_node` among them, ascending.
    eligible: Vec<NodeId>,
    k: NonZeroU8,
    granularity: Granularity,
    /// Every unit's owners, highest rendezvous score first, `stride` apiece:
    /// one unit per bucket at `Bucket`, one per part at `Part`.
    owners: Vec<NodeId>,
    stride: usize,
    owned: PartSet,
    /// Per bucket, whether `owned` holds any of its parts.
    owns_in_bucket: Vec<bool>,
    /// Every other node that owns some part `self_node` owns, ascending.
    co_owners: Vec<NodeId>,
}

impl OwnershipView {
    /// [`OwnershipView::compute_at`] at [`Granularity::Bucket`].
    #[must_use]
    pub fn compute(self_node: NodeId, eligible: Vec<NodeId>, k: NonZeroU8) -> Self {
        Self::compute_at(self_node, eligible, k, Granularity::Bucket)
    }

    /// Builds the view for `self_node` from `eligible` at `granularity`, as
    /// [`eligible_owners`] and [`ownership_granularity`] pick them. Folds
    /// `self_node` into `eligible` and dedups first, so a caller may omit it.
    #[must_use]
    pub fn compute_at(
        self_node: NodeId,
        eligible: Vec<NodeId>,
        k: NonZeroU8,
        granularity: Granularity,
    ) -> Self {
        let eligible = with_self(self_node, eligible);
        let stride = usize::from(k.get()).min(eligible.len());
        let mut owners = Vec::with_capacity(unit_count(granularity) * stride);
        let mut top = Vec::with_capacity(stride + 1);
        for unit in units(granularity) {
            top_by_score(&eligible, unit, stride, &mut top);
            owners.extend(top.iter().map(|&(_, node)| node));
        }
        Self::from_owners(self_node, eligible, k, granularity, owners)
    }

    /// The view `self_node` moves to when the eligible set becomes
    /// `eligible`: equal to [`OwnershipView::compute_at`]'s, built from this
    /// view's ranking. A unit whose owners all stay eligible ranks only
    /// them and the nodes that joined; a unit that lost an owner ranks
    /// every node again. A change of granularity, `k` or owners per unit
    /// builds from scratch.
    #[must_use]
    pub fn successor(&self, eligible: Vec<NodeId>, k: NonZeroU8, granularity: Granularity) -> Self {
        let eligible = with_self(self.self_node, eligible);
        let stride = usize::from(k.get()).min(eligible.len());
        if (k, granularity, stride) != (self.k, self.granularity, self.stride) {
            return Self::compute_at(self.self_node, eligible, k, granularity);
        }
        let joined: Vec<NodeId> = eligible
            .iter()
            .copied()
            .filter(|node| self.eligible.binary_search(node).is_err())
            .collect();
        let stays = |node: NodeId| eligible.binary_search(&node).is_ok();
        let mut owners = Vec::with_capacity(self.owners.len());
        let mut candidates = Vec::with_capacity(stride + joined.len());
        let mut top = Vec::with_capacity(stride + 1);
        for (unit, previous) in units(granularity).zip(self.owners.chunks_exact(stride)) {
            if joined.is_empty() && previous.iter().all(|&node| stays(node)) {
                owners.extend_from_slice(previous);
                continue;
            }
            candidates.clear();
            candidates.extend(
                previous
                    .iter()
                    .chain(&joined)
                    .map(|&node| (rendezvous_score(node, unit), node)),
            );
            let (kept, newcomers) = candidates.split_at(stride);
            if !carried_top(
                kept,
                newcomers,
                |&(_, node)| stays(node),
                stride,
                &mut top,
                rank_order,
            ) {
                top_by_score(&eligible, unit, stride, &mut top);
            }
            owners.extend(top.iter().map(|&(_, node)| node));
        }
        Self::from_owners(self.self_node, eligible, k, granularity, owners)
    }

    /// The view `owners` describes: every unit's owners, `min(k, eligible)`
    /// apiece, in [`units`] order.
    fn from_owners(
        self_node: NodeId,
        eligible: Vec<NodeId>,
        k: NonZeroU8,
        granularity: Granularity,
        owners: Vec<NodeId>,
    ) -> Self {
        let stride = usize::from(k.get()).min(eligible.len());
        let mut owned = PartSet::new();
        let mut owns_in_bucket = vec![false; BUCKET_COUNT];
        let mut co_owners = BTreeSet::new();
        for (unit, ranked) in units(granularity).zip(owners.chunks_exact(stride)) {
            if ranked.contains(&self_node) {
                for part in parts_of_wire_id(granularity, unit) {
                    owned.insert(part);
                    owns_in_bucket[usize::from(part.bucket())] = true;
                }
                co_owners.extend(ranked.iter().copied().filter(|&node| node != self_node));
            }
        }
        Self {
            view_hash: view_hash_at(&eligible, granularity),
            self_node,
            eligible,
            k,
            granularity,
            owners,
            stride,
            owned,
            owns_in_bucket,
            co_owners: co_owners.into_iter().collect(),
        }
    }

    /// This view's identity: two views with equal `view_hash` (and equal
    /// `k`, which is gossip-validated before either view is built) hold
    /// identical per-part ownership.
    #[must_use]
    pub const fn view_hash(&self) -> u64 {
        self.view_hash
    }

    /// Whether this view ranks whole buckets or single parts.
    #[must_use]
    pub const fn granularity(&self) -> Granularity {
        self.granularity
    }

    /// Whether `self_node` owns `part`.
    #[must_use]
    pub fn owns(&self, part: PartId) -> bool {
        self.owned.contains(part)
    }

    /// Every other node that owns some part `self_node` owns, ascending:
    /// the peers an anti-entropy round has something to compare with.
    #[must_use]
    pub fn co_owners(&self) -> &[NodeId] {
        &self.co_owners
    }

    /// Whether `self_node` owns any part of `bucket`.
    #[must_use]
    pub fn owns_any_in_bucket(&self, bucket: u16) -> bool {
        self.owned_bucket_mask()
            .get(usize::from(bucket))
            .is_some_and(|&owns| owns)
    }

    /// Whether `self_node` owns any part of each bucket, indexed by bucket.
    #[must_use]
    pub(crate) fn owned_bucket_mask(&self) -> &[bool] {
        &self.owns_in_bucket
    }

    /// `part`'s live owners, highest rendezvous score first.
    #[must_use]
    pub fn owners_of(&self, part: PartId) -> &[NodeId] {
        let unit = match self.granularity {
            Granularity::Bucket => usize::from(part.bucket()),
            Granularity::Part => part.index(),
        };
        let start = unit * self.stride;
        self.owners
            .get(start..start + self.stride)
            .unwrap_or_default()
    }

    /// Every part `self_node` owns, ascending by [`PartId::index`].
    pub fn owned_parts(&self) -> impl Iterator<Item = PartId> + '_ {
        self.owned.iter()
    }

    /// How many parts `self_node` owns.
    #[must_use]
    pub fn owned_part_count(&self) -> usize {
        self.owned.len()
    }

    /// The wire ids naming `parts` under this view; see [`wire_ids`].
    #[must_use]
    pub(crate) fn wire_ids(&self, parts: &[PartId]) -> Vec<u16> {
        wire_ids(self.granularity, parts)
    }
}

/// The `u16` ids that name `parts` on the wire at `granularity`: at
/// `Bucket` each distinct bucket once, standing for all of its parts, at
/// `Part` each part's raw id. Ascending and deduplicated either way.
#[must_use]
pub(crate) fn wire_ids(granularity: Granularity, parts: &[PartId]) -> Vec<u16> {
    let mut ids: Vec<u16> = parts
        .iter()
        .map(|part| match granularity {
            Granularity::Bucket => part.bucket(),
            Granularity::Part => part.raw(),
        })
        .collect();
    ids.sort_unstable();
    ids.dedup();
    ids
}

/// The parts one wire `id` names at `granularity`: all [`PART_COUNT`] parts
/// of bucket `id` at `Bucket`, the single part `id` at `Part`.
pub(crate) fn parts_of_wire_id(
    granularity: Granularity,
    id: u16,
) -> impl Iterator<Item = PartId> + use<> {
    let (bucket, single) = match granularity {
        Granularity::Bucket => (Some(id), None),
        Granularity::Part => (None, Some(PartId::from_raw(id))),
    };
    bucket.into_iter().flat_map(PartId::of_bucket).chain(single)
}

/// Fills `top` with the `k` highest-scoring nodes of `eligible` for the unit
/// `raw`, in [`rank_order`], without sorting every node for each of 65,536
/// parts.
fn top_by_score(eligible: &[NodeId], raw: u16, k: usize, top: &mut Vec<(u64, NodeId)>) {
    select_top(
        eligible
            .iter()
            .map(|&node| (rendezvous_score(node, raw), node)),
        k,
        top,
        rank_order,
    );
}

/// Leaves the first `k` of `items` in `top`, in `order`: a bounded
/// insertion sort that sifts each item toward the front and never keeps
/// more than `k`.
fn select_top<T>(
    items: impl IntoIterator<Item = T>,
    k: usize,
    top: &mut Vec<T>,
    order: impl Fn(&T, &T) -> std::cmp::Ordering,
) {
    top.clear();
    for item in items {
        top.push(item);
        for at in (1..top.len()).rev() {
            if order(&top[at - 1], &top[at]).is_le() {
                break;
            }
            top.swap(at - 1, at);
        }
        top.truncate(k);
    }
}

/// Refreshes one unit's first `k` when its candidates change: `kept` was the
/// first `k` of the old candidates in `order`, `joined` are the new ones,
/// and `stays` says which old ones remain. Fills `top` and returns `true`
/// when every one of `kept` stays, since then no candidate outside `kept`
/// can outrank them; returns `false`, leaving the unit to a full ranking,
/// when one left, since a candidate `kept` crowded out may rise.
fn carried_top<T: Copy>(
    kept: &[T],
    joined: &[T],
    stays: impl Fn(&T) -> bool,
    k: usize,
    top: &mut Vec<T>,
    order: impl Fn(&T, &T) -> std::cmp::Ordering,
) -> bool {
    let carried = kept.iter().all(stays);
    if carried {
        select_top(kept.iter().chain(joined).copied(), k, top, order);
    }
    carried
}

/// `eligible` with `self_node` in it, ascending and deduplicated.
fn with_self(self_node: NodeId, mut eligible: Vec<NodeId>) -> Vec<NodeId> {
    eligible.push(self_node);
    eligible.sort_unstable();
    eligible.dedup();
    eligible
}

/// How many units a view at `granularity` ranks.
const fn unit_count(granularity: Granularity) -> usize {
    match granularity {
        Granularity::Bucket => BUCKET_COUNT,
        Granularity::Part => PART_SPACE,
    }
}

/// Every unit a view at `granularity` ranks, by wire id: a bucket at
/// `Bucket`, a part at `Part`.
fn units(granularity: Granularity) -> impl Iterator<Item = u16> {
    (0..=u16::MAX).take(unit_count(granularity))
}

/// A live-updating, cheap-to-clone handle onto one cache's current
/// [`OwnershipView`]. Every reader shares the same watch channel: one
/// recomputation per relevant membership or cache-mode change, never one per
/// reader, and every reader agrees with every other reader on the current
/// view at all times.
#[derive(Debug, Clone)]
pub struct OwnershipTracker {
    view: watch::Receiver<Arc<OwnershipView>>,
    /// This tracker's first computed view, kept distinct from the live
    /// [`OwnershipTracker::current`] channel; see [`OwnershipTracker::baseline`].
    baseline: Arc<OwnershipView>,
}

impl OwnershipTracker {
    /// Computes the first view synchronously, no task and no placeholder,
    /// from a `(peers, modes)` snapshot the caller already has to hand.
    /// Returns the tracker plus the `watch::Sender` half a later refresh
    /// loop publishes new views through as membership and cache modes
    /// change.
    #[must_use]
    pub fn seed(
        self_node: NodeId,
        peers: &[Peer],
        modes: &CacheModes,
        cache: &SmolStr,
        k: NonZeroU8,
    ) -> (Self, watch::Sender<Arc<OwnershipView>>) {
        let view = Arc::new(compute_view(self_node, peers, modes, cache, k));
        // Published here since a cache whose membership never changes again
        // never gets a refresh_task publish to do it.
        publish_owned_parts(cache, &view, "ownership view seeded");
        let baseline = Arc::clone(&view);
        let (tx, rx) = watch::channel(view);
        (Self { view: rx, baseline }, tx)
    }

    /// The current view: a `watch::Receiver::borrow()` plus one cheap `Arc`
    /// clone. Synchronous, so it's safe to call from a write path with no
    /// lock over shard or network state.
    #[must_use]
    pub fn current(&self) -> Arc<OwnershipView> {
        Arc::clone(&self.view.borrow())
    }

    /// This tracker's original seeded view, pinned for its whole lifetime.
    /// `rebalance_task` diffs its first lost-part check against this
    /// instead of a live re-borrow of the view channel, which could already
    /// show a view `refresh_task` corrected before `rebalance_task` started
    /// watching, missing a part only the seed view ever called owned.
    #[must_use]
    pub fn baseline(&self) -> Arc<OwnershipView> {
        Arc::clone(&self.baseline)
    }

    /// A fresh subscription for a caller that awaits the next change, such
    /// as a rebalance trigger.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<Arc<OwnershipView>> {
        self.view.clone()
    }
}

/// Parts this node recently lost ownership of, per the latest
/// [`OwnershipView`], but keeps resident until a grace period elapses, so a
/// new owner's pull, or an ordinary anti-entropy round pairing this node
/// with the new owner as a self-healing backstop, has time to land before
/// the data disappears. Read by anti-entropy's cohort widening and the
/// donor-serving exception; never read by the inbound-apply guard, which
/// stays strict current-view ownership.
pub struct ResidencySet {
    releasing: RwLock<HashMap<PartId, Instant>>,
    /// Parts this node owns but has not yet pulled from a co-owner: a local
    /// miss there says nothing about the key, so a fetch asks the other
    /// owners before answering `None`, and this node declines to answer a
    /// remote fetch's miss for them. Cleared as each pull lands, or
    /// wholesale when the warm-up gives up.
    cold: RwLock<PartSet>,
    /// Parts a warm spill-tier reopen replayed from disk but not yet
    /// verified against a live co-owner. Unlike `cold`, a part here may
    /// hold a record a co-owner deleted during this node's downtime, so a
    /// local hit isn't trusted until verification lands or every donor
    /// turns out cold or unreachable, at which point this clears anyway.
    unverified: RwLock<PartSet>,
}

impl Default for ResidencySet {
    fn default() -> Self {
        Self::new()
    }
}

impl ResidencySet {
    #[must_use]
    pub fn new() -> Self {
        Self {
            releasing: RwLock::new(HashMap::new()),
            cold: RwLock::new(PartSet::new()),
            unverified: RwLock::new(PartSet::new()),
        }
    }

    /// Marks each of `parts` cold: owned here, not yet pulled.
    pub(crate) fn mark_cold(&self, parts: &[PartId]) {
        self.cold.write().extend(parts.iter().copied());
    }

    /// Clears the cold mark from each of `parts`: their pull landed.
    /// Production call sites use [`ResidencySet::mark_serving`] instead,
    /// which clears this and `unverified` together; kept separate since the
    /// two marks are independent state.
    pub(crate) fn clear_cold(&self, parts: &[PartId]) {
        let mut cold = self.cold.write();
        for &part in parts {
            cold.remove(part);
        }
    }

    /// Clears every cold mark: the warm-up gave up, so what is here is what
    /// there is. [`ResidencySet::mark_all_serving`] clears this and every
    /// unverified mark together in production.
    pub(crate) fn clear_all_cold(&self) {
        self.cold.write().clear();
    }

    /// Clears the cold and unverified marks from `parts` together: the
    /// single point where this node starts trusting both a local miss and a
    /// local hit in these parts. Called from every site that decides a part
    /// is ready to serve -- a donor pull landing, an eager verification
    /// round, or every donor turning out cold or unreachable.
    pub(crate) fn mark_serving(&self, parts: &[PartId]) {
        self.clear_cold(parts);
        self.clear_unverified(parts);
    }

    /// Wholesale analogue of [`ResidencySet::mark_serving`]: after
    /// `warm_up_task`'s `WarmAnyway` give-up, every part still marked cold
    /// or unverified is declared servable at once.
    pub(crate) fn mark_all_serving(&self) {
        self.clear_all_cold();
        self.unverified.write().clear();
    }

    /// Whether `part` is owned here but not yet pulled.
    pub(crate) fn is_cold(&self, part: PartId) -> bool {
        self.cold.read().contains(part)
    }

    /// Marks each of `parts` unverified: a warm spill-tier reopen just
    /// replayed them from disk with no live co-owner check yet. `spill`-gated
    /// since only `attach_spill_and_record_warm` calls it; `is_unverified`
    /// and `clear_unverified` stay unconditional, since an always-empty set
    /// already behaves correctly without the feature.
    #[cfg(feature = "spill")]
    pub(crate) fn mark_unverified(&self, parts: &[PartId]) {
        self.unverified.write().extend(parts.iter().copied());
    }

    /// Clears the unverified mark from `parts`: either an eager
    /// `reconcile_warm_buckets` round vouched for them, or the cold-pull
    /// path landed fresh data.
    pub(crate) fn clear_unverified(&self, parts: &[PartId]) {
        let mut unverified = self.unverified.write();
        for &part in parts {
            unverified.remove(part);
        }
    }

    /// Whether `part` was warm-reloaded and not yet verified against a live
    /// co-owner; see [`ResidencySet::unverified`].
    pub(crate) fn is_unverified(&self, part: PartId) -> bool {
        self.unverified.read().contains(part)
    }

    /// Marks each of `parts` as releasing, starting its grace clock. A part
    /// already releasing keeps its original clock: only a call to
    /// [`ResidencySet::unmark`] in between resets it.
    pub fn mark_releasing(&self, parts: &[PartId]) {
        let now = Instant::now();
        let mut releasing = self.releasing.write();
        for &part in parts {
            releasing.entry(part).or_insert(now);
        }
    }

    /// Clears the release clock for each of `parts`: for one that regains
    /// ownership before its grace elapsed, so a flap never accumulates
    /// toward release.
    pub fn unmark(&self, parts: &[PartId]) {
        let mut releasing = self.releasing.write();
        for part in parts {
            releasing.remove(part);
        }
    }

    /// Whether `part` is currently mid disown-grace.
    pub fn is_releasing(&self, part: PartId) -> bool {
        self.releasing.read().contains_key(&part)
    }

    /// Every part currently mid disown-grace, in no particular order: one
    /// lock for a caller that checks many parts.
    #[must_use]
    pub fn releasing_parts(&self) -> Vec<PartId> {
        self.releasing.read().keys().copied().collect()
    }

    /// Whether any part of `bucket` is mid disown-grace.
    #[must_use]
    pub fn releasing_in_bucket(&self, bucket: u16) -> bool {
        let releasing = self.releasing.read();
        if releasing.len() < PART_COUNT {
            releasing.keys().any(|part| part.bucket() == bucket)
        } else {
            PartId::of_bucket(bucket).any(|part| releasing.contains_key(&part))
        }
    }

    /// Parts whose grace has elapsed: due for physical release.
    pub fn expired(&self, grace: Duration) -> Vec<PartId> {
        let now = Instant::now();
        self.releasing
            .read()
            .iter()
            .filter(|&(_, since)| now.saturating_duration_since(*since) >= grace)
            .map(|(&part, _)| part)
            .collect()
    }
}

/// The view `self_node` computes for `cache` from one `(peers, modes)`
/// snapshot: [`eligible_owners`] at [`ownership_granularity`].
fn compute_view(
    self_node: NodeId,
    peers: &[Peer],
    modes: &CacheModes,
    cache: &SmolStr,
    k: NonZeroU8,
) -> OwnershipView {
    let eligible = eligible_owners(self_node, peers, modes, cache, k);
    let granularity = ownership_granularity(self_node, peers, &eligible);
    OwnershipView::compute_at(self_node, eligible, k, granularity)
}

/// The owned share `sundog_owned_buckets` reports for `owned_parts`: whole
/// buckets' worth, so the gauge summed across a cluster is `1024 × owners`
/// at either granularity.
fn owned_bucket_equivalent(owned_parts: usize) -> f64 {
    #[allow(
        clippy::cast_precision_loss,
        reason = "at most 65,536 parts, exact in f64"
    )]
    let owned = owned_parts as f64;
    #[allow(clippy::cast_precision_loss, reason = "64, exact in f64")]
    let per_bucket = PART_COUNT as f64;
    owned / per_bucket
}

/// Sets `cache`'s `sundog_owned_parts` and `sundog_owned_buckets` gauges
/// from `view` and logs them under `message`; used by both
/// `OwnershipTracker::seed` and [`refresh_task`].
fn publish_owned_parts(cache: &SmolStr, view: &OwnershipView, message: &'static str) {
    let owned = view.owned_part_count();
    #[allow(
        clippy::cast_precision_loss,
        reason = "at most 65,536 parts, exact in f64"
    )]
    metrics::gauge!("sundog_owned_parts", "cache" => cache.to_string()).set(owned as f64);
    metrics::gauge!("sundog_owned_buckets", "cache" => cache.to_string())
        .set(owned_bucket_equivalent(owned));
    tracing::debug!(
        cache = %cache,
        self_node = %view.self_node,
        view_hash = view.view_hash(),
        granularity = ?view.granularity(),
        owned_parts = owned,
        "{message}"
    );
}

/// Recomputes and republishes `cache`'s view on every change to `cluster`'s
/// live peer set or its advertised cache modes, for as long as `cancel`
/// stays live. Spawned once [`Shard::with_ownership`](crate::store::Shard::with_ownership)
/// has already installed `tx`'s receiver half, so this task's first publish
/// is already the second view a reader could ever see, never the first:
/// [`OwnershipTracker::seed`] publishes the first view's gauge itself.
/// Publishes with [`watch::Sender::send_if_modified`], keyed on
/// `view_hash`, so a peer's unrelated gossip key changing never ripples
/// through this cache: the `sundog_owned_parts` and `sundog_owned_buckets`
/// gauges only move on a real ownership change. The hash is known before
/// the view is built, so an unchanged view costs no ranking at all.
pub(crate) async fn refresh_task(
    cluster: Cluster,
    cache: SmolStr,
    k: NonZeroU8,
    tx: watch::Sender<Arc<OwnershipView>>,
    cancel: CancellationToken,
) {
    let self_node = cluster.node_id();
    let mut peers = cluster.peers_watch();
    let mut modes = cluster.cache_modes_watch();
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            changed = peers.changed() => {
                if changed.is_err() {
                    return; // membership shut down
                }
            }
            changed = modes.changed() => {
                if changed.is_err() {
                    return; // membership shut down
                }
            }
        }
        let peers_snapshot = peers.borrow_and_update().clone();
        let modes_snapshot = modes.borrow_and_update().clone();
        // The hash alone says whether anything changed; the view itself is
        // built only when it did, from the current one, and at part
        // granularity off the async workers.
        let eligible = eligible_owners(self_node, &peers_snapshot, &modes_snapshot, &cache, k);
        let granularity = ownership_granularity(self_node, &peers_snapshot, &eligible);
        let new_hash = view_hash_at(&eligible, granularity);
        if tx.borrow().view_hash() == new_hash {
            continue;
        }
        let current = Arc::clone(&tx.borrow());
        let view = match granularity {
            Granularity::Bucket => current.successor(eligible, k, granularity),
            Granularity::Part => {
                match tokio::task::spawn_blocking(move || {
                    current.successor(eligible, k, granularity)
                })
                .await
                {
                    Ok(view) => view,
                    Err(_) => return, // the runtime is shutting down
                }
            }
        };
        let view = Arc::new(view);
        let published = tx.send_if_modified(|current| {
            if current.view_hash() == new_hash {
                false
            } else {
                *current = Arc::clone(&view);
                true
            }
        });
        if published {
            publish_owned_parts(&cache, &view, "ownership view republished");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use proptest::prelude::*;
    use rand::seq::SliceRandom as _;
    use rand::{SeedableRng as _, rngs::StdRng};

    use super::*;
    use crate::node::NodeName;

    fn peer(node: u64, protocol: u16) -> Peer {
        Peer {
            node: NodeId::from(node),
            name: NodeName::new("host", NodeId::from(node)),
            gossip_addr: "127.0.0.1:0".parse::<SocketAddr>().expect("valid addr"),
            data_addr: "127.0.0.1:0".parse::<SocketAddr>().expect("valid addr"),
            incarnation: 0,
            protocol,
        }
    }

    fn modes_with(entries: &[(NodeId, &str, Mode)]) -> CacheModes {
        let mut modes: CacheModes = HashMap::new();
        for &(node, cache, mode) in entries {
            modes
                .entry(node)
                .or_default()
                .insert(SmolStr::new(cache), mode);
        }
        modes
    }

    #[test]
    fn rendezvous_score_is_deterministic_across_repeated_calls() {
        let node = NodeId::from(42);
        assert_eq!(rendezvous_score(node, 7), rendezvous_score(node, 7));
    }

    #[test]
    fn rendezvous_score_varies_with_bucket() {
        let node = NodeId::from(42);
        assert_ne!(rendezvous_score(node, 1), rendezvous_score(node, 2));
    }

    #[test]
    fn owners_of_bucket_returns_top_k_by_descending_score() {
        let eligible: Vec<NodeId> = (1..=20u64).map(NodeId::from).collect();
        let bucket = 5;
        let mut expected: Vec<(u64, NodeId)> = eligible
            .iter()
            .map(|&n| (rendezvous_score(n, bucket), n))
            .collect();
        expected.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
        let expected_top3: Vec<NodeId> = expected.into_iter().take(3).map(|(_, n)| n).collect();

        assert_eq!(owners_of_bucket(&eligible, bucket, 3), expected_top3);
    }

    #[test]
    fn owners_of_bucket_breaks_ties_by_ascending_node_id() {
        let tied = vec![
            (100, NodeId::from(9)),
            (100, NodeId::from(3)),
            (100, NodeId::from(7)),
        ];
        assert_eq!(
            rank_by_score(tied, 2),
            vec![NodeId::from(3), NodeId::from(7)],
            "equal scores break ties by ascending NodeId"
        );
    }

    #[test]
    fn owners_of_bucket_degenerates_to_every_eligible_node_when_k_exceeds_eligible_count() {
        let eligible = vec![NodeId::from(1), NodeId::from(2)];
        let result = owners_of_bucket(&eligible, 42, 5);
        assert_eq!(result.len(), 2);
        assert!(eligible.iter().all(|n| result.contains(n)));
    }

    /// Holds `successor` equal to a fresh build of the same inputs, field by
    /// field.
    fn assert_same_view(successor: &OwnershipView, fresh: &OwnershipView) {
        assert_eq!(successor.view_hash, fresh.view_hash);
        assert_eq!(successor.eligible, fresh.eligible);
        assert_eq!(successor.stride, fresh.stride);
        assert_eq!(successor.owners, fresh.owners, "every unit's ranked owners");
        assert!(successor.owned_parts().eq(fresh.owned_parts()));
        assert_eq!(successor.owns_in_bucket, fresh.owns_in_bucket);
        assert_eq!(successor.co_owners, fresh.co_owners);
    }

    /// The nodes `1..=n` minus `left`, plus `joined`.
    fn moved(n: u64, left: &[u64], joined: &[u64]) -> Vec<NodeId> {
        (1..=n)
            .filter(|node| !left.contains(node))
            .chain(joined.iter().copied())
            .map(NodeId::from)
            .collect()
    }

    proptest! {
        #[test]
        fn a_successor_view_equals_a_fresh_build(
            n in 1u64..24,
            k in 1u8..4,
            left in proptest::collection::vec(2u64..24, 0..4),
            joined in proptest::collection::vec(100u64..110, 0..4),
        ) {
            let self_node = NodeId::from(1);
            let k = NonZeroU8::new(k).expect("nonzero");
            let before = OwnershipView::compute_at(self_node, moved(n, &[], &[]), k, Granularity::Bucket);
            let eligible = moved(n, &left, &joined);
            let successor = before.successor(eligible.clone(), k, Granularity::Bucket);
            assert_same_view(
                &successor,
                &OwnershipView::compute_at(self_node, eligible, k, Granularity::Bucket),
            );
        }

        #[test]
        fn owners_of_bucket_is_invariant_to_input_order(seed in any::<u64>(), n in 1u64..30) {
            let mut nodes: Vec<NodeId> = (1..=n).map(NodeId::from).collect();
            let baseline = owners_of_bucket(&nodes, 11, 3);
            let mut rng = StdRng::seed_from_u64(seed);
            nodes.shuffle(&mut rng);
            let shuffled = owners_of_bucket(&nodes, 11, 3);
            prop_assert_eq!(baseline, shuffled);
        }
    }

    #[test]
    fn view_hash_is_invariant_to_input_order_and_duplicates() {
        let a = vec![NodeId::from(1), NodeId::from(2), NodeId::from(3)];
        let b = vec![
            NodeId::from(3),
            NodeId::from(1),
            NodeId::from(2),
            NodeId::from(1),
        ];
        assert_eq!(view_hash(&a), view_hash(&b));
    }

    #[test]
    fn view_hash_differs_for_different_live_sets() {
        let a = vec![NodeId::from(1), NodeId::from(2)];
        let b = vec![NodeId::from(1), NodeId::from(3)];
        assert_ne!(view_hash(&a), view_hash(&b));
    }

    #[test]
    fn eligible_owners_excludes_a_protocol_two_peer() {
        let self_node = NodeId::from(1);
        let other = NodeId::from(2);
        let k = NonZeroU8::new(2).expect("nonzero");
        let peers = vec![peer(2, 2)];
        let modes = modes_with(&[(other, "cache", Mode::Distributed { owners: k })]);
        let cache = SmolStr::new("cache");

        assert_eq!(
            eligible_owners(self_node, &peers, &modes, &cache, k),
            vec![self_node]
        );
    }

    #[test]
    fn eligible_owners_excludes_a_peer_that_has_not_opened_this_cache() {
        let self_node = NodeId::from(1);
        let k = NonZeroU8::new(2).expect("nonzero");
        let peers = vec![peer(2, 3)];
        let modes: CacheModes = HashMap::new();
        let cache = SmolStr::new("cache");

        assert_eq!(
            eligible_owners(self_node, &peers, &modes, &cache, k),
            vec![self_node]
        );
    }

    #[test]
    fn eligible_owners_excludes_a_peer_advertising_a_different_owners_count() {
        let self_node = NodeId::from(1);
        let other = NodeId::from(2);
        let k = NonZeroU8::new(3).expect("nonzero");
        let peers = vec![peer(2, 3)];
        let modes = modes_with(&[(
            other,
            "cache",
            Mode::Distributed {
                owners: NonZeroU8::new(2).expect("nonzero"),
            },
        )]);
        let cache = SmolStr::new("cache");

        assert_eq!(
            eligible_owners(self_node, &peers, &modes, &cache, k),
            vec![self_node]
        );
    }

    #[test]
    fn eligible_owners_always_includes_self_regardless_of_self_advertisement() {
        let self_node = NodeId::from(1);
        let k = NonZeroU8::new(2).expect("nonzero");
        // Self, but speaking a protocol too old to be eligible and
        // advertising nothing about this cache.
        let peers = vec![peer(1, 2)];
        let modes: CacheModes = HashMap::new();
        let cache = SmolStr::new("cache");

        assert!(eligible_owners(self_node, &peers, &modes, &cache, k).contains(&self_node));
    }

    #[test]
    fn shared_owned_parts_is_empty_for_a_peer_owning_nothing_in_common() {
        let self_node = NodeId::from(1);
        let k = NonZeroU8::new(1).expect("nonzero");
        let view = OwnershipView::compute(self_node, vec![self_node], k);
        let stranger = NodeId::from(99);

        assert!(shared_owned_parts(&view, stranger).is_empty());
    }

    #[test]
    fn shared_owned_parts_lists_exactly_the_parts_both_nodes_own() {
        let self_node = NodeId::from(1);
        let peer_node = NodeId::from(2);
        let eligible: Vec<NodeId> = (1..=5u64).map(NodeId::from).collect();
        let k = NonZeroU8::new(2).expect("nonzero");
        for granularity in [Granularity::Bucket, Granularity::Part] {
            let view = OwnershipView::compute_at(self_node, eligible.clone(), k, granularity);
            let shared = shared_owned_parts(&view, peer_node);
            assert!(!shared.is_empty());
            for part in PartId::all() {
                let both = view.owns(part) && view.owners_of(part).contains(&peer_node);
                assert_eq!(shared.contains(&part), both, "{granularity:?} {part:?}");
            }
        }
    }

    #[test]
    fn ownership_diff_reports_disjoint_gained_and_lost_sets() {
        let self_node = NodeId::from(1);
        let old_eligible: Vec<NodeId> = (1..=10u64).map(NodeId::from).collect();
        let new_eligible: Vec<NodeId> = (1..=10u64)
            .filter(|&n| n != 1)
            .chain(101..=110u64)
            .map(NodeId::from)
            .collect();
        let k = NonZeroU8::new(3).expect("nonzero");

        let old = OwnershipView::compute(self_node, old_eligible, k);
        let new = OwnershipView::compute(self_node, new_eligible, k);
        let (gained, lost) = ownership_diff(&old, &new);

        for &b in &gained {
            assert!(!old.owns(b) && new.owns(b));
        }
        for &b in &lost {
            assert!(old.owns(b) && !new.owns(b));
        }
        assert!(gained.iter().all(|b| !lost.contains(b)));
        assert!(
            !gained.is_empty() || !lost.is_empty(),
            "replacing most of the eligible set changes at least one bucket's ownership"
        );
    }

    #[test]
    fn ownership_view_compute_always_places_self_somewhere_even_when_omitted_from_the_input() {
        let self_node = NodeId::from(7);
        let eligible = vec![NodeId::from(1), NodeId::from(2)];
        let k = NonZeroU8::new(1).expect("nonzero");

        let view = OwnershipView::compute(self_node, eligible, k);

        assert!(
            view.owned_parts().next().is_some(),
            "self is folded into eligible and owns at least one part somewhere"
        );
    }

    #[test]
    fn ownership_view_owns_and_owners_of_and_owned_parts_agree_at_both_granularities() {
        let self_node = NodeId::from(1);
        let eligible = vec![self_node, NodeId::from(2), NodeId::from(3)];
        let k = NonZeroU8::new(2).expect("nonzero");
        for granularity in [Granularity::Bucket, Granularity::Part] {
            let view = OwnershipView::compute_at(self_node, eligible.clone(), k, granularity);
            assert_eq!(view.granularity(), granularity);
            assert_eq!(view.owned_parts().count(), view.owned_part_count());
            for part in PartId::all() {
                assert_eq!(view.owners_of(part).len(), 2);
                assert_eq!(
                    view.owns(part),
                    view.owners_of(part).contains(&self_node),
                    "{granularity:?} {part:?}"
                );
            }
        }
    }

    #[test]
    fn a_bucket_view_gives_every_part_of_a_bucket_the_bucket_s_owners() {
        let self_node = NodeId::from(1);
        let eligible: Vec<NodeId> = (1..=7u64).map(NodeId::from).collect();
        let view = OwnershipView::compute(
            self_node,
            eligible.clone(),
            NonZeroU8::new(3).expect("nonzero"),
        );
        for bucket in [0u16, 5, 1023] {
            let expected = owners_of_bucket(&eligible, bucket, 3);
            for part in PartId::of_bucket(bucket) {
                assert_eq!(view.owners_of(part), expected.as_slice());
            }
        }
    }

    #[test]
    fn a_part_view_ranks_each_part_by_its_own_score() {
        let self_node = NodeId::from(1);
        let eligible: Vec<NodeId> = (1..=9u64).map(NodeId::from).collect();
        let k = NonZeroU8::new(2).expect("nonzero");
        let view = OwnershipView::compute_at(self_node, eligible.clone(), k, Granularity::Part);
        for part in [PartId::new(0, 0), PartId::new(7, 33), PartId::new(1023, 63)] {
            let expected = rank_by_score(
                eligible
                    .iter()
                    .map(|&n| (rendezvous_score(n, part.raw()), n))
                    .collect(),
                2,
            );
            assert_eq!(view.owners_of(part), expected.as_slice());
        }
        let per_bucket: std::collections::HashSet<Vec<NodeId>> = PartId::of_bucket(3)
            .map(|part| view.owners_of(part).to_vec())
            .collect();
        assert!(
            per_bucket.len() > 1,
            "parts of one bucket land on different owners"
        );
    }

    #[test]
    fn a_part_view_spreads_ownership_far_more_evenly_than_a_bucket_view_at_a_hundred_nodes() {
        let eligible: Vec<NodeId> = (1..=100u64).map(NodeId::from).collect();
        let k = NonZeroU8::new(2).expect("nonzero");
        let spread = |granularity: Granularity| -> (f64, f64) {
            let view = OwnershipView::compute_at(eligible[0], eligible.clone(), k, granularity);
            let mut load: HashMap<NodeId, usize> = HashMap::new();
            for part in PartId::all() {
                for &owner in view.owners_of(part) {
                    *load.entry(owner).or_default() += 1;
                }
            }
            #[allow(clippy::cast_precision_loss, reason = "small exact counts")]
            let fair = (PART_SPACE * 2) as f64 / 100.0;
            #[allow(clippy::cast_precision_loss, reason = "small exact counts")]
            let ratio = |n: usize| n as f64 / fair;
            let max = load.values().copied().max().unwrap_or(0);
            let min = eligible
                .iter()
                .map(|n| load.get(n).copied().unwrap_or(0))
                .min()
                .unwrap_or(0);
            (ratio(max), ratio(min))
        };
        let (part_max, part_min) = spread(Granularity::Part);
        let (bucket_max, bucket_min) = spread(Granularity::Bucket);
        assert!(
            part_max < 1.15 && part_min > 0.85,
            "part view: {part_min:.2}..{part_max:.2}"
        );
        assert!(
            bucket_max > 1.3 || bucket_min < 0.7,
            "a bucket view at a hundred nodes is visibly lumpier: {bucket_min:.2}..{bucket_max:.2}"
        );
    }

    #[test]
    fn owns_any_in_bucket_is_whether_any_owned_part_lies_in_the_bucket() {
        let self_node = NodeId::from(1);
        let eligible: Vec<NodeId> = (1..=6u64).map(NodeId::from).collect();
        let k = NonZeroU8::new(2).expect("nonzero");
        for granularity in [Granularity::Bucket, Granularity::Part] {
            let view = OwnershipView::compute_at(self_node, eligible.clone(), k, granularity);
            for bucket in 0..u16::try_from(BUCKET_COUNT).expect("fits") {
                assert_eq!(
                    view.owns_any_in_bucket(bucket),
                    PartId::of_bucket(bucket).any(|part| view.owns(part)),
                    "bucket {bucket} at {granularity:?}"
                );
            }
        }
        let alone = OwnershipView::compute(self_node, vec![self_node], k);
        assert!(alone.owns_any_in_bucket(0));
        assert!(
            !alone.owns_any_in_bucket(u16::MAX),
            "a bucket past the space is never owned"
        );
    }

    #[test]
    fn co_owners_are_every_other_owner_of_an_owned_part() {
        let self_node = NodeId::from(1);
        let k = NonZeroU8::new(2).expect("nonzero");
        for granularity in [Granularity::Bucket, Granularity::Part] {
            let eligible: Vec<NodeId> = (1..=8u64).map(NodeId::from).collect();
            let view = OwnershipView::compute_at(self_node, eligible, k, granularity);
            let mut expected: Vec<NodeId> = view
                .owned_parts()
                .flat_map(|part| view.owners_of(part).to_vec())
                .filter(|&node| node != self_node)
                .collect();
            expected.sort_unstable();
            expected.dedup();
            assert_eq!(view.co_owners(), expected.as_slice(), "at {granularity:?}");
            assert!(!view.co_owners().contains(&self_node));
        }
        let alone = OwnershipView::compute(self_node, vec![self_node], k);
        assert!(alone.co_owners().is_empty(), "a lone node has no co-owner");
    }

    #[test]
    fn a_part_view_successor_equals_a_fresh_build_across_joins_leaves_and_switches() {
        let self_node = NodeId::from(1);
        let two = NonZeroU8::new(2).expect("nonzero");
        let before =
            OwnershipView::compute_at(self_node, moved(20, &[], &[]), two, Granularity::Part);
        let cases = [
            ("a join", moved(20, &[], &[21]), two, Granularity::Part),
            ("a leave", moved(20, &[7], &[]), two, Granularity::Part),
            (
                "a join and a leave",
                moved(20, &[7, 9], &[21]),
                two,
                Granularity::Part,
            ),
            ("no change", moved(20, &[], &[]), two, Granularity::Part),
            (
                "another k",
                moved(20, &[], &[]),
                NonZeroU8::new(3).expect("nonzero"),
                Granularity::Part,
            ),
            (
                "bucket ranking",
                moved(20, &[], &[]),
                two,
                Granularity::Bucket,
            ),
            ("a lone node", vec![self_node], two, Granularity::Part),
        ];
        for (what, eligible, k, granularity) in cases {
            let successor = before.successor(eligible.clone(), k, granularity);
            let fresh = OwnershipView::compute_at(self_node, eligible, k, granularity);
            assert_eq!(successor.view_hash(), fresh.view_hash(), "{what}");
            assert_same_view(&successor, &fresh);
        }
        let alone = OwnershipView::compute_at(self_node, vec![self_node], two, Granularity::Part);
        assert_same_view(
            &alone.successor(moved(20, &[], &[]), two, Granularity::Part),
            &before,
        );
    }

    #[test]
    fn carried_top_ranks_kept_and_joined_or_hands_back_a_unit_that_lost_a_member() {
        let mut top = Vec::new();
        assert!(carried_top(
            &[9, 7],
            &[8, 3],
            |_| true,
            2,
            &mut top,
            |a: &u8, b| b.cmp(a)
        ));
        assert_eq!(top, vec![9, 8]);
        assert!(!carried_top(
            &[9, 7],
            &[8],
            |&x| x != 7,
            2,
            &mut top,
            |a: &u8, b| b.cmp(a)
        ));
    }

    #[test]
    fn units_name_every_bucket_or_every_part() {
        assert_eq!(units(Granularity::Bucket).count(), BUCKET_COUNT);
        assert_eq!(units(Granularity::Part).count(), PART_SPACE);
        assert_eq!(units(Granularity::Part).last(), Some(u16::MAX));
        assert_eq!(
            with_self(
                NodeId::from(2),
                vec![NodeId::from(3), NodeId::from(2), NodeId::from(3)]
            ),
            vec![NodeId::from(2), NodeId::from(3)]
        );
    }

    #[test]
    fn top_by_score_matches_a_full_sort() {
        let eligible: Vec<NodeId> = (1..=40u64).map(NodeId::from).collect();
        let mut top = Vec::new();
        for raw in [0u16, 1, 999, 65_535] {
            for k in 1..=4usize {
                top_by_score(&eligible, raw, k, &mut top);
                let expected = rank_by_score(
                    eligible
                        .iter()
                        .map(|&n| (rendezvous_score(n, raw), n))
                        .collect(),
                    u8::try_from(k).expect("small"),
                );
                let got: Vec<NodeId> = top.iter().map(|&(_, n)| n).collect();
                assert_eq!(got, expected, "raw {raw} k {k}");
            }
        }
        top_by_score(&[NodeId::from(1)], 5, 3, &mut top);
        assert_eq!(top.len(), 1, "fewer nodes than k keeps every node");
    }

    #[test]
    fn a_bucket_view_keeps_the_legacy_hash_and_a_part_view_does_not() {
        let self_node = NodeId::from(1);
        let eligible = vec![self_node, NodeId::from(2)];
        let k = NonZeroU8::new(1).expect("nonzero");
        let bucket = OwnershipView::compute_at(self_node, eligible.clone(), k, Granularity::Bucket);
        let part = OwnershipView::compute_at(self_node, eligible.clone(), k, Granularity::Part);
        assert_eq!(bucket.view_hash(), view_hash(&eligible));
        assert_ne!(part.view_hash(), bucket.view_hash());
        assert_eq!(part.view_hash(), view_hash_at(&eligible, Granularity::Part));
        let reordered = vec![NodeId::from(2), self_node, self_node];
        assert_eq!(
            view_hash_at(&reordered, Granularity::Part),
            part.view_hash(),
            "order and duplicates never change a part view's hash either"
        );
    }

    #[test]
    fn ownership_granularity_is_part_only_when_every_eligible_peer_speaks_it() {
        let self_node = NodeId::from(1);
        let eligible = vec![self_node, NodeId::from(2), NodeId::from(3)];
        let current = wire::PROTOCOL_PART_OWNERSHIP;
        let everyone = vec![peer(2, current), peer(3, current)];
        let one_older = vec![peer(2, current), peer(3, current - 1)];
        let expected_when_all = if wire::PROTOCOL_VERSION >= current {
            Granularity::Part
        } else {
            Granularity::Bucket
        };
        assert_eq!(
            ownership_granularity(self_node, &everyone, &eligible),
            expected_when_all
        );
        assert_eq!(
            ownership_granularity(self_node, &one_older, &eligible),
            Granularity::Bucket,
            "one peer on an older protocol keeps the whole cluster on buckets"
        );
        assert_eq!(
            ownership_granularity(self_node, &[], &[self_node]),
            expected_when_all,
            "a lone node has no older peer to wait for"
        );
    }

    /// `refresh_task` decides whether a view changed from
    /// [`view_hash_at`] over [`eligible_owners`] at
    /// [`ownership_granularity`], before building anything: that hash is
    /// the built view's, at either granularity.
    #[test]
    fn a_view_hash_is_known_before_the_view_is_built() {
        let self_node = NodeId::from(1);
        let k = NonZeroU8::new(2).expect("nonzero");
        let cache = SmolStr::new("prices");
        let mode = Mode::Distributed { owners: k };
        let modes = modes_with(&[
            (NodeId::from(2), "prices", mode),
            (NodeId::from(3), "prices", mode),
        ]);
        let current = wire::PROTOCOL_VERSION;
        for peers in [
            vec![peer(2, current), peer(3, current)],
            vec![peer(2, current), peer(3, wire::PROTOCOL_DISTRIBUTED)],
        ] {
            let eligible = eligible_owners(self_node, &peers, &modes, &cache, k);
            let granularity = ownership_granularity(self_node, &peers, &eligible);
            let built = compute_view(self_node, &peers, &modes, &cache, k);
            assert_eq!(built.granularity(), granularity);
            assert_eq!(view_hash_at(&eligible, granularity), built.view_hash());
        }
    }

    #[test]
    fn wire_ids_and_parts_of_wire_id_round_trip_at_each_granularity() {
        let parts = vec![PartId::new(4, 0), PartId::new(4, 9), PartId::new(900, 63)];
        assert_eq!(wire_ids(Granularity::Bucket, &parts), vec![4, 900]);
        assert_eq!(wire_ids(Granularity::Part, &parts), {
            let mut raw: Vec<u16> = parts.iter().map(|p| p.raw()).collect();
            raw.sort_unstable();
            raw
        });
        let whole: Vec<PartId> = parts_of_wire_id(Granularity::Bucket, 4).collect();
        assert_eq!(whole, PartId::of_bucket(4).collect::<Vec<_>>());
        let one: Vec<PartId> = parts_of_wire_id(Granularity::Part, parts[1].raw()).collect();
        assert_eq!(one, vec![parts[1]]);
    }

    #[test]
    fn owned_bucket_equivalent_sums_to_a_whole_bucket_per_sixty_four_parts() {
        assert!((owned_bucket_equivalent(0) - 0.0).abs() < f64::EPSILON);
        assert!((owned_bucket_equivalent(64) - 1.0).abs() < f64::EPSILON);
        assert!((owned_bucket_equivalent(PART_SPACE) - 1024.0).abs() < f64::EPSILON);
        assert!((owned_bucket_equivalent(96) - 1.5).abs() < f64::EPSILON);
    }

    #[test]
    fn ownership_view_view_hash_matches_the_free_function() {
        let self_node = NodeId::from(1);
        let eligible = vec![self_node, NodeId::from(2)];
        let k = NonZeroU8::new(1).expect("nonzero");
        let view = OwnershipView::compute(self_node, eligible.clone(), k);

        assert_eq!(view.view_hash(), view_hash(&eligible));
    }

    #[test]
    fn ownership_tracker_seed_never_returns_a_default_or_empty_view() {
        let self_node = NodeId::from(1);
        let k = NonZeroU8::new(2).expect("nonzero");
        let cache = SmolStr::new("cache");

        let (tracker, _tx) = OwnershipTracker::seed(self_node, &[], &HashMap::new(), &cache, k);
        let view = tracker.current();

        assert_eq!(
            view.owned_part_count(),
            PART_SPACE,
            "the sole eligible node owns every part, never zero of them"
        );
    }

    #[test]
    fn ownership_tracker_current_reflects_the_seeded_view() {
        let self_node = NodeId::from(1);
        let k = NonZeroU8::new(1).expect("nonzero");
        let cache = SmolStr::new("cache");
        let eligible_peers = vec![peer(2, 3)];
        let modes = modes_with(&[(NodeId::from(2), "cache", Mode::Distributed { owners: k })]);

        let (tracker, _tx) = OwnershipTracker::seed(self_node, &eligible_peers, &modes, &cache, k);
        let expected = eligible_owners(self_node, &eligible_peers, &modes, &cache, k);

        assert_eq!(tracker.current().view_hash(), view_hash(&expected));
    }

    #[test]
    fn ownership_tracker_subscribe_returns_a_receiver_that_sees_future_publishes() {
        let self_node = NodeId::from(1);
        let k = NonZeroU8::new(1).expect("nonzero");
        let cache = SmolStr::new("cache");
        let (tracker, tx) = OwnershipTracker::seed(self_node, &[], &HashMap::new(), &cache, k);
        let sub = tracker.subscribe();

        let new_view = Arc::new(OwnershipView::compute(
            self_node,
            vec![self_node, NodeId::from(2)],
            k,
        ));
        let expected_hash = new_view.view_hash();
        tx.send(new_view).expect("receiver still alive");

        assert_eq!(sub.borrow().view_hash(), expected_hash);
    }

    #[test]
    fn ownership_tracker_baseline_stays_the_seeded_view_even_after_a_later_publish() {
        let self_node = NodeId::from(1);
        let k = NonZeroU8::new(2).expect("nonzero");
        let cache = SmolStr::new("cache");
        // The sole eligible node owns every bucket: the transient view
        // open() computes while racing gossip convergence.
        let (tracker, tx) = OwnershipTracker::seed(self_node, &[], &HashMap::new(), &cache, k);
        let baseline_hash = tracker.baseline().view_hash();
        assert_eq!(
            tracker.baseline().owned_part_count(),
            PART_SPACE,
            "the seeded, lone-node baseline owns every part"
        );

        // Simulates refresh_task correcting the view before a reader ever
        // re-derives its own starting point from the channel.
        let corrected = Arc::new(OwnershipView::compute(
            self_node,
            vec![self_node, NodeId::from(2)],
            k,
        ));
        let corrected_hash = corrected.view_hash();
        tx.send(Arc::clone(&corrected))
            .expect("receiver still alive");

        assert_eq!(
            tracker.current().view_hash(),
            corrected_hash,
            "current tracks the latest publish"
        );
        assert_eq!(
            tracker.baseline().view_hash(),
            baseline_hash,
            "baseline stays pinned to the tracker's original seeded view regardless of later \
             publishes"
        );
    }

    /// A `metrics::Recorder` that records every `sundog_owned_buckets` and
    /// `sundog_owned_parts` set for one cache, so a test can observe a gauge
    /// publish without the process-global recorder slot.
    struct OwnedGaugeRecorder {
        cache: String,
        sets: Arc<parking_lot::Mutex<Vec<f64>>>,
        part_sets: Arc<parking_lot::Mutex<Vec<f64>>>,
    }

    struct OwnedGauge(Arc<parking_lot::Mutex<Vec<f64>>>);

    impl metrics::GaugeFn for OwnedGauge {
        fn increment(&self, _value: f64) {}
        fn decrement(&self, _value: f64) {}
        fn set(&self, value: f64) {
            self.0.lock().push(value);
        }
    }

    impl metrics::Recorder for OwnedGaugeRecorder {
        fn describe_counter(
            &self,
            _key: metrics::KeyName,
            _unit: Option<metrics::Unit>,
            _description: metrics::SharedString,
        ) {
        }

        fn describe_gauge(
            &self,
            _key: metrics::KeyName,
            _unit: Option<metrics::Unit>,
            _description: metrics::SharedString,
        ) {
        }

        fn describe_histogram(
            &self,
            _key: metrics::KeyName,
            _unit: Option<metrics::Unit>,
            _description: metrics::SharedString,
        ) {
        }

        fn register_counter(
            &self,
            _key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> metrics::Counter {
            metrics::Counter::noop()
        }

        fn register_gauge(
            &self,
            key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> metrics::Gauge {
            let this_cache = key
                .labels()
                .any(|l| l.key() == "cache" && l.value() == self.cache);
            if !this_cache {
                return metrics::Gauge::noop();
            }
            let sets = match key.name() {
                "sundog_owned_buckets" => &self.sets,
                "sundog_owned_parts" => &self.part_sets,
                _ => return metrics::Gauge::noop(),
            };
            metrics::Gauge::from_arc(Arc::new(OwnedGauge(Arc::clone(sets))))
        }

        fn register_histogram(
            &self,
            _key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> metrics::Histogram {
            metrics::Histogram::noop()
        }
    }

    #[test]
    fn ownership_tracker_seed_publishes_the_owned_gauges_for_its_first_view() {
        let self_node = NodeId::from(1);
        let other = NodeId::from(2);
        let k = NonZeroU8::new(1).expect("nonzero");
        let cache = SmolStr::new("seeded");
        let sets = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let part_sets = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let recorder = OwnedGaugeRecorder {
            cache: cache.to_string(),
            sets: Arc::clone(&sets),
            part_sets: Arc::clone(&part_sets),
        };
        let modes = modes_with(&[(other, "seeded", Mode::Distributed { owners: k })]);
        let peers = vec![peer(2, wire::PROTOCOL_DISTRIBUTED)];
        // The view once open()'s membership wait lands a peer; membership
        // won't change again so this seed is the only publish.
        let (tracker, _tx) = metrics::with_local_recorder(&recorder, || {
            OwnershipTracker::seed(self_node, &peers, &modes, &cache, k)
        });
        let parts = tracker.current().owned_part_count();
        let owned = owned_bucket_equivalent(parts);
        assert_eq!(
            sets.lock().as_slice(),
            &[owned],
            "the seed sets the gauge once, to the seeded view's owned share in buckets"
        );
        assert_eq!(
            part_sets.lock().as_slice(),
            &[f64::from(u32::try_from(parts).expect("fits u32"))],
            "and the part gauge once, to its owned-part count"
        );
        let total = f64::from(u32::try_from(BUCKET_COUNT).expect("fits u32"));
        assert!(
            owned > 0.0 && owned < total,
            "one owner over two nodes is a real split of the bucket space, not all or nothing: \
             {owned} of {total}"
        );
    }

    /// A part by raw id, for residency tests that only need distinct parts.
    fn p(raw: u16) -> PartId {
        PartId::from_raw(raw)
    }

    #[test]
    fn residency_set_is_releasing_tracks_marked_parts() {
        let set = ResidencySet::new();
        assert!(!set.is_releasing(p(3)));

        set.mark_releasing(&[p(3), p(4)]);
        assert!(set.is_releasing(p(3)));
        assert!(set.is_releasing(p(4)));

        set.unmark(&[p(3)]);
        assert!(!set.is_releasing(p(3)));
        assert!(set.is_releasing(p(4)));
    }

    #[test]
    fn residency_set_cold_marks_clear_per_part_or_all_at_once() {
        let set = ResidencySet::new();
        assert!(!set.is_cold(p(1)));
        set.mark_cold(&[p(1), p(2), p(3)]);
        assert!(set.is_cold(p(1)) && set.is_cold(p(2)) && set.is_cold(p(3)));
        assert!(
            !set.is_releasing(p(1)),
            "cold and releasing are separate marks"
        );
        set.clear_cold(&[p(2)]);
        assert!(set.is_cold(p(1)) && !set.is_cold(p(2)) && set.is_cold(p(3)));
        set.clear_all_cold();
        assert!(!set.is_cold(p(1)) && !set.is_cold(p(3)));
    }

    #[cfg(feature = "spill")]
    #[test]
    fn residency_set_unverified_marks_clear_per_part_and_are_distinct_from_cold() {
        let set = ResidencySet::new();
        assert!(!set.is_unverified(p(1)));

        set.mark_unverified(&[p(1), p(2), p(3)]);
        assert!(set.is_unverified(p(1)) && set.is_unverified(p(2)) && set.is_unverified(p(3)));
        assert!(
            !set.is_cold(p(1)),
            "unverified and cold are separate marks: marking one never marks the other"
        );

        set.clear_unverified(&[p(2)]);
        assert!(set.is_unverified(p(1)) && !set.is_unverified(p(2)) && set.is_unverified(p(3)));
    }

    #[cfg(feature = "spill")]
    #[test]
    fn residency_set_cold_and_unverified_clear_independently() {
        let set = ResidencySet::new();
        set.mark_cold(&[p(1)]);
        set.mark_unverified(&[p(1)]);
        assert!(set.is_cold(p(1)) && set.is_unverified(p(1)));

        // cold and unverified clear independently; only mark_serving
        // clears both together.
        set.clear_cold(&[p(1)]);
        assert!(!set.is_cold(p(1)) && set.is_unverified(p(1)));

        set.mark_cold(&[p(1)]);
        set.clear_unverified(&[p(1)]);
        assert!(set.is_cold(p(1)) && !set.is_unverified(p(1)));
    }

    #[cfg(feature = "spill")]
    #[test]
    fn residency_set_mark_serving_clears_both_cold_and_unverified_together() {
        let set = ResidencySet::new();
        set.mark_cold(&[p(1), p(2)]);
        set.mark_unverified(&[p(1)]);
        assert!(set.is_cold(p(1)) && set.is_cold(p(2)) && set.is_unverified(p(1)));

        // mark_serving clears whichever marks a part carries.
        set.mark_serving(&[p(1), p(2)]);
        assert!(!set.is_cold(p(1)) && !set.is_cold(p(2)) && !set.is_unverified(p(1)));
    }

    #[cfg(feature = "spill")]
    #[test]
    fn residency_set_mark_all_serving_clears_every_cold_and_unverified_mark() {
        let set = ResidencySet::new();
        set.mark_cold(&[p(1), p(2), p(3)]);
        set.mark_unverified(&[p(2), p(3)]);
        set.mark_releasing(&[p(9)]);

        set.mark_all_serving();

        assert!(!set.is_cold(p(1)) && !set.is_cold(p(2)) && !set.is_cold(p(3)));
        assert!(!set.is_unverified(p(2)) && !set.is_unverified(p(3)));
        assert!(
            set.is_releasing(p(9)),
            "mark_all_serving touches only the cold and unverified marks"
        );
    }

    #[test]
    fn residency_set_releasing_parts_and_releasing_in_bucket_read_the_marks_in_bulk() {
        let set = ResidencySet::new();
        assert!(set.releasing_parts().is_empty());
        assert!(!set.releasing_in_bucket(7));
        set.mark_releasing(&[PartId::new(7, 3), PartId::new(9, 0)]);
        let mut releasing = set.releasing_parts();
        releasing.sort_unstable();
        assert_eq!(releasing, {
            let mut expected = vec![PartId::new(7, 3), PartId::new(9, 0)];
            expected.sort_unstable();
            expected
        });
        assert!(set.releasing_in_bucket(7));
        assert!(set.releasing_in_bucket(9));
        assert!(!set.releasing_in_bucket(8));
        // Past `PART_COUNT` marks the per-bucket check reads the bucket's
        // own parts instead of scanning every mark; same answers.
        set.mark_releasing(&PartId::of_bucket(100).collect::<Vec<_>>());
        assert!(set.releasing_in_bucket(7));
        assert!(set.releasing_in_bucket(100));
        assert!(!set.releasing_in_bucket(8));
    }

    #[test]
    fn residency_set_expired_returns_only_parts_past_grace() {
        let set = ResidencySet::new();
        set.mark_releasing(&[p(1), p(2)]);

        assert!(
            set.expired(Duration::from_secs(3600)).is_empty(),
            "nothing is past a huge grace yet"
        );
        let mut due = set.expired(Duration::ZERO);
        due.sort_unstable();
        assert_eq!(due, vec![p(1), p(2)], "everything is past a zero grace");
    }

    #[test]
    fn residency_set_flap_resets_the_release_clock_instead_of_accumulating() {
        let set = ResidencySet::new();
        set.mark_releasing(&[p(5)]);
        std::thread::sleep(Duration::from_millis(400));
        // The part regains ownership before its grace elapses.
        set.unmark(&[p(5)]);
        // It's lost again immediately: the clock must restart from here,
        // not continue running from the first mark 400ms ago. A 200ms
        // grace leaves scheduler jitter a wide margin either way.
        set.mark_releasing(&[p(5)]);

        assert!(
            !set.expired(Duration::from_millis(200)).contains(&p(5)),
            "a flap resets the release clock rather than letting it accumulate across the gap"
        );
    }
}

#[cfg(kani)]
mod kani_proofs {
    use super::*;

    /// The bounded selection is the first `k` of any strict order, for
    /// every `k` through the input's length: `k` items, strictly ascending,
    /// each drawn from the input, and every item left out ordered after
    /// every one kept. It compares items and nothing else, so four distinct
    /// bytes cover every shape of input.
    #[kani::proof]
    #[kani::unwind(6)]
    fn select_top_keeps_the_first_k_in_order() {
        let items: [u8; 4] = kani::any();
        for i in 0..items.len() {
            for j in 0..i {
                kani::assume(items[i] != items[j]);
            }
        }
        let mut top = Vec::with_capacity(items.len() + 1);
        for k in 0..=items.len() {
            select_top(items, k, &mut top, u8::cmp);
            let held = |item: &u8| top.iter().any(|kept| kept == item);
            assert_eq!(top.len(), k);
            assert!(top.windows(2).all(|pair| pair[0] < pair[1]));
            assert!(top.iter().all(|kept| items.iter().any(|item| item == kept)));
            assert!(
                items
                    .iter()
                    .all(|item| held(item) || top.iter().all(|kept| kept < item))
            );
        }
    }

    /// Four distinct candidates, in any order.
    fn distinct_candidates() -> [u8; 4] {
        let [a, b, c, d]: [u8; 4] = kani::any();
        kani::assume(a != b && a != c && a != d && b != c && b != d && c != d);
        [a, b, c, d]
    }

    /// Carrying the first `k` of `old` to `new`, where `joined` are the
    /// newcomers, gives the first `k` of `new` whenever it does not hand
    /// the unit back, for every `k` through four.
    fn assert_carried_top_is_full(old: &[u8], new: &[u8], joined: &[u8]) {
        let (mut kept, mut carried, mut full) = (
            Vec::with_capacity(5),
            Vec::with_capacity(5),
            Vec::with_capacity(5),
        );
        for k in 0..=4 {
            select_top(old.iter().copied(), k, &mut kept, u8::cmp);
            select_top(new.iter().copied(), k, &mut full, u8::cmp);
            let stays = |item: &u8| new.iter().any(|stayed| stayed == item);
            if carried_top(&kept, joined, stays, k, &mut carried, u8::cmp) {
                assert!(carried.iter().eq(full.iter()));
            }
        }
    }

    /// A leave carries a unit's first `k` whether the leaver was kept or
    /// crowded out.
    #[kani::proof]
    #[kani::unwind(7)]
    fn a_carried_top_survives_a_leave() {
        let [a, b, c, _] = distinct_candidates();
        assert_carried_top_is_full(&[a, b, c], &[a, b], &[]);
    }

    /// A join carries a unit's first `k` wherever the newcomer ranks.
    #[kani::proof]
    #[kani::unwind(7)]
    fn a_carried_top_survives_a_join() {
        let [a, b, c, d] = distinct_candidates();
        assert_carried_top_is_full(&[a, b, c], &[a, b, c, d], &[d]);
    }

    /// A leave and a join at once carry a unit's first `k`.
    #[kani::proof]
    #[kani::unwind(7)]
    fn a_carried_top_survives_a_leave_and_a_join() {
        let [a, b, c, d] = distinct_candidates();
        assert_carried_top_is_full(&[a, b, c], &[a, b, d], &[d]);
    }

    /// A part's wire id names it back at either granularity, and every
    /// part that id names lies where the id says.
    #[kani::proof]
    #[kani::unwind(66)]
    fn a_part_s_wire_id_names_it_back() {
        let part = PartId::from_raw(kani::any());
        let granularity = if kani::any() {
            Granularity::Part
        } else {
            Granularity::Bucket
        };
        let ids = wire_ids(granularity, &[part]);
        assert_eq!(ids.len(), 1);
        let id = ids[0];
        assert!(parts_of_wire_id(granularity, id).any(|named| named == part));
        match granularity {
            Granularity::Bucket => {
                assert_eq!(id, part.bucket());
                assert!(parts_of_wire_id(granularity, id).all(|named| named.bucket() == id));
            }
            Granularity::Part => assert_eq!(id, part.raw()),
        }
    }

    /// Parts only when this build and every peer speak part ownership.
    #[kani::proof]
    fn parts_only_when_every_peer_speaks_them() {
        let protocols: [u16; 3] = kani::any();
        let everyone = protocols
            .iter()
            .all(|&protocol| protocol >= wire::PROTOCOL_PART_OWNERSHIP);
        let expected = if wire::PROTOCOL_VERSION >= wire::PROTOCOL_PART_OWNERSHIP && everyone {
            Granularity::Part
        } else {
            Granularity::Bucket
        };
        assert_eq!(Granularity::for_protocols(protocols), expected);
    }

    /// The owned-buckets gauge is the part count over 64, exactly, for any
    /// share a node can own.
    #[kani::proof]
    fn the_bucket_gauge_is_the_part_count_over_sixty_four() {
        let owned: u32 = kani::any();
        kani::assume(owned as usize <= PART_SPACE);
        #[allow(clippy::cast_precision_loss, reason = "at most 65,536, exact in f64")]
        let parts = f64::from(owned);
        assert_eq!(owned_bucket_equivalent(owned as usize) * 64.0, parts);
    }
}

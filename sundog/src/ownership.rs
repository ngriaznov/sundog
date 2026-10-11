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
use crate::explain::Residency;
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
        if wire::peer_supports(wire::PROTOCOL_VERSION, wire::PROTOCOL_PART_OWNERSHIP) {
            Self::for_members(peer_protocols)
        } else {
            Self::Bucket
        }
    }

    /// `Part` when every one of `protocols` speaks
    /// [`wire::PROTOCOL_PART_OWNERSHIP`], `Bucket` otherwise, with no term
    /// for this build. A member's own granularity combines its build with
    /// its peers' protocols, which equals this over every eligible member's
    /// protocol, so a caller built from another release computes the
    /// members' granularity.
    pub(crate) fn for_members(protocols: impl IntoIterator<Item = u16>) -> Self {
        if protocols
            .into_iter()
            .all(|protocol| wire::peer_supports(protocol, wire::PROTOCOL_PART_OWNERSHIP))
        {
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
    let mut eligible = eligible_peers(peers, modes, cache, k);
    if !eligible.contains(&self_node) {
        eligible.push(self_node);
    }
    eligible
}

/// The members of `peers` eligible to own a part of `cache`: those speaking
/// at least [`wire::PROTOCOL_DISTRIBUTED`] that advertise `cache` under
/// `Mode::Distributed` with this same `k`. [`eligible_owners`] adds the
/// calling node's own seat to this; a caller outside the cluster adds none.
pub(crate) fn eligible_peers(
    peers: &[Peer],
    modes: &CacheModes,
    cache: &SmolStr,
    k: NonZeroU8,
) -> Vec<NodeId> {
    peers
        .iter()
        .filter(|peer| peer.protocol >= wire::PROTOCOL_DISTRIBUTED)
        .filter(|peer| {
            modes
                .get(&peer.node)
                .and_then(|caches| caches.get(cache))
                .is_some_and(|mode| matches!(mode, Mode::Distributed { owners } if *owners == k))
        })
        .map(|peer| peer.node)
        .collect()
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

    /// The ranked nodes, `self_node` among them, ascending.
    #[must_use]
    pub(crate) fn eligible(&self) -> &[NodeId] {
        &self.eligible
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

    /// The co-owners `self_node` has once every node in `returning` is
    /// eligible again alongside this view's own: a node this view dropped
    /// shares a part with `self_node` there when it ranks among that part's
    /// owners with everyone back. Nodes already eligible change nothing.
    #[must_use]
    pub(crate) fn co_owners_with(&self, returning: &[NodeId]) -> Vec<NodeId> {
        self.widened(returning)
            .map_or_else(|| self.co_owners.clone(), |view| view.co_owners)
    }

    /// This view with every node in `returning` eligible again alongside its
    /// own, or `None` when each of them already is.
    #[must_use]
    pub(crate) fn widened(&self, returning: &[NodeId]) -> Option<Self> {
        let mut eligible = self.eligible.clone();
        eligible.extend_from_slice(returning);
        eligible.sort_unstable();
        eligible.dedup();
        (eligible.len() != self.eligible.len())
            .then(|| self.successor(eligible, self.k, self.granularity))
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
    /// Parts this node owns again while it still holds its copy from
    /// owning them before, which lacks every write and delete made while
    /// it was not an owner. A local hit isn't trusted until the pull lands;
    /// unlike `unverified`, a part here serves once every donor turns out
    /// cold and has converged with this node, since no copy anywhere is
    /// more complete.
    stale: RwLock<PartSet>,
    /// Which owned parts hold everything written to them; see
    /// [`Settlement`].
    settlement: RwLock<Settlement>,
}

/// The parts whose local copy [`ResidencySet`] vouches for, kept current
/// from both ends: `refresh_task` drops a part the moment a published view
/// stops owning it, and `rebalance_task` settles a view once its pull
/// completes. A part outside it counts as cold even before
/// `rebalance_task` marks it so, since the tracker publishes a view first
/// and a local miss in between says nothing about the key.
#[derive(Default)]
struct Settlement {
    /// Parts owned without a break since the last settled view, or `None`
    /// before one is recorded.
    settled: Option<PartSet>,
    /// Parts a pull or verification served since then, so a part gained
    /// afterward serves as soon as its own pull lands.
    served: PartSet,
    /// Parts a published view dropped since `rebalance_task` last took
    /// them: writes to them since went to other owners, so owning one again
    /// means pulling it again, even when `rebalance_task` never saw the
    /// view that dropped it.
    lost: PartSet,
    /// Parts the latest published view owns, or `None` before `refresh_task`
    /// publishes one. A pull planned under an earlier view can finish after
    /// a newer one drops its parts: the inbound guard forwards those records
    /// instead of applying them, so the part is marked nothing.
    owned: Option<PartSet>,
    /// Each open [`PullTicket`]'s held parts: those every published view
    /// has owned since the ticket opened. A pull that ran while its part
    /// was not owned here, from the start or for a stretch in between,
    /// had the records for it forwarded to the other owners, so it never
    /// vouches for that part.
    pulls: HashMap<u64, PartSet>,
    /// The id the next [`PullTicket`] takes.
    next_pull: u64,
}

impl Settlement {
    /// Whether the latest published view owns `part`; `true` before any
    /// view is published.
    fn owns(&self, part: PartId) -> bool {
        self.owned.as_ref().is_none_or(|owned| owned.contains(part))
    }

    /// Whether `part` is outside the last settled view and not served by a
    /// pull or verification since; `false` before any view is settled.
    fn is_unsettled(&self, part: PartId) -> bool {
        self.settled
            .as_ref()
            .is_some_and(|settled| !settled.contains(part))
            && !self.served.contains(part)
    }
}

/// One in-flight pull's hold on a [`ResidencySet`], from
/// [`ResidencySet::open_pull`]: its [`PullTicket::mark_serving`] vouches
/// only for parts every published view has owned since the ticket opened.
/// Dropping the ticket, including when its pull is cancelled, releases the
/// hold.
pub(crate) struct PullTicket {
    residency: Arc<ResidencySet>,
    id: u64,
}

impl PullTicket {
    /// The residency set this ticket marks.
    pub(crate) fn residency(&self) -> &ResidencySet {
        &self.residency
    }

    /// [`ResidencySet::mark_serving`] for the parts of `parts` every
    /// published view has owned since this ticket opened.
    pub(crate) fn mark_serving(&self, parts: &[PartId]) {
        self.residency.mark_serving_for(Some(self.id), parts);
    }
}

impl Drop for PullTicket {
    fn drop(&mut self) {
        self.residency.settlement.write().pulls.remove(&self.id);
    }
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
            stale: RwLock::new(PartSet::new()),
            settlement: RwLock::new(Settlement::default()),
        }
    }

    /// Records `view` as the last view `rebalance_task` finished with: every
    /// part it owns is now either pulled or marked cold on its own, except a
    /// part a later published view dropped, which stays unsettled.
    pub(crate) fn settle(&self, view: &OwnershipView) {
        let mut settlement = self.settlement.write();
        let settled: PartSet = view
            .owned_parts()
            .filter(|&part| !settlement.lost.contains(part))
            .collect();
        settlement.settled = Some(settled);
        settlement.served.clear();
    }

    /// Records that `refresh_task` published `new` over `old`: every part
    /// `old` owned and `new` does not is unsettled from here on and joins
    /// the lost parts [`ResidencySet::take_lost`] hands `rebalance_task`.
    pub(crate) fn record_published(&self, old: &OwnershipView, new: &OwnershipView) {
        let mut settlement = self.settlement.write();
        let Settlement {
            settled,
            served,
            lost,
            owned,
            pulls,
            ..
        } = &mut *settlement;
        for part in old.owned_parts().filter(|&part| !new.owns(part)) {
            if let Some(settled) = settled.as_mut() {
                settled.remove(part);
            }
            served.remove(part);
            lost.insert(part);
            for held in pulls.values_mut() {
                held.remove(part);
            }
        }
        *owned = Some(new.owned_parts().collect());
    }

    /// Opens a [`PullTicket`] for a pull about to start.
    pub(crate) fn open_pull(self: &Arc<Self>) -> PullTicket {
        let mut settlement = self.settlement.write();
        let id = settlement.next_pull;
        settlement.next_pull += 1;
        let held = settlement
            .owned
            .clone()
            .unwrap_or_else(|| PartId::all().collect());
        settlement.pulls.insert(id, held);
        PullTicket {
            residency: Arc::clone(self),
            id,
        }
    }

    /// Drains the parts published views dropped since the last call, for
    /// `rebalance_task` to pull again any it owns once more.
    pub(crate) fn take_lost(&self) -> PartSet {
        std::mem::take(&mut self.settlement.write().lost)
    }

    /// Whether `part`, owned under the current view, is not yet settled:
    /// not owned without a break since the last settled view, and not
    /// served by a pull or verification since. `false` before any view is
    /// settled.
    pub(crate) fn is_unsettled(&self, part: PartId) -> bool {
        self.settlement.read().is_unsettled(part)
    }

    /// Every mark on `part`, with `owns` from the ownership view the
    /// caller decides under and a disown grace measured to `now`. Each mark
    /// is read under its own lock, released before the next, in the order
    /// writers nest them: settlement, cold, unverified, stale, releasing.
    /// A writer moving several marks at once can show some from before the
    /// move and some from after; each mark a reader sees is one a writer
    /// set.
    pub(crate) fn residency_of(&self, part: PartId, owns: bool, now: Instant) -> Residency {
        let unsettled = self.is_unsettled(part);
        let cold_marked = self.cold.read().contains(part);
        let unverified = self.unverified.read().contains(part);
        let stale = self.stale.read().contains(part);
        let releasing_for = self
            .releasing
            .read()
            .get(&part)
            .map(|since| now.saturating_duration_since(*since));
        Residency {
            owns,
            releasing_for,
            cold_marked,
            unsettled,
            unverified,
            stale,
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

    /// Clears the cold and unverified marks from `parts` together: the
    /// single point where this node starts trusting both a local miss and a
    /// local hit in these parts. Called from every site that decides a part
    /// is ready to serve -- a donor pull landing, an eager verification
    /// round, or every donor turning out cold or unreachable. A part the
    /// latest published view does not own keeps its marks: whatever landed
    /// for it went through the inbound guard to its current owners, and a
    /// view that owns it again pulls it again.
    pub(crate) fn mark_serving(&self, parts: &[PartId]) {
        self.mark_serving_for(None, parts);
    }

    /// [`ResidencySet::mark_serving`], restricted as well to the parts
    /// `pull`'s ticket still holds.
    fn mark_serving_for(&self, pull: Option<u64>, parts: &[PartId]) {
        let mut settlement = self.settlement.write();
        let held = pull.and_then(|id| settlement.pulls.get(&id));
        let owned: Vec<PartId> = parts
            .iter()
            .copied()
            .filter(|&part| settlement.owns(part) && held.is_none_or(|held| held.contains(part)))
            .collect();
        self.clear_cold(&owned);
        self.clear_unverified(&owned);
        settlement.served.extend(owned);
    }

    /// Wholesale analogue of [`ResidencySet::mark_serving`]: after
    /// `warm_up_task`'s `WarmAnyway` give-up, every part the latest
    /// published view owns and still marks cold or unverified is declared
    /// servable at once.
    pub(crate) fn mark_all_serving(&self) {
        let mut settlement = self.settlement.write();
        let owned: Vec<PartId> = PartId::all()
            .filter(|&part| settlement.owns(part))
            .collect();
        self.clear_cold(&owned);
        self.clear_unverified(&owned);
        let Settlement {
            settled, served, ..
        } = &mut *settlement;
        if let Some(settled) = settled.as_ref() {
            served.extend(owned.into_iter().filter(|&part| !settled.contains(part)));
        }
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

    /// Marks each of `parts` stale: `rebalance_task` is pulling a part it
    /// already holds a copy of from owning it before; see
    /// [`ResidencySet::stale`].
    pub(crate) fn mark_stale(&self, parts: &[PartId]) {
        self.stale.write().extend(parts.iter().copied());
    }

    /// Clears the unverified and stale marks from `parts`: either an eager
    /// `reconcile_warm_buckets` round vouched for them, or the cold-pull
    /// path landed fresh data.
    pub(crate) fn clear_unverified(&self, parts: &[PartId]) {
        let mut unverified = self.unverified.write();
        let mut stale = self.stale.write();
        for &part in parts {
            unverified.remove(part);
            stale.remove(part);
        }
    }

    /// Whether a local hit in `part` is not yet trusted: warm-reloaded or
    /// stale, and not yet verified against a live co-owner; see
    /// [`ResidencySet::unverified`] and [`ResidencySet::stale`].
    pub(crate) fn is_unverified(&self, part: PartId) -> bool {
        self.unverified.read().contains(part) || self.stale.read().contains(part)
    }

    /// Whether `part` was warm-reloaded and not yet verified: the one mark
    /// that holds a part back even once every donor turns out cold.
    pub(crate) fn is_replayed(&self, part: PartId) -> bool {
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
    residency: Arc<ResidencySet>,
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
                // Inside the send, so no reader sees `view` before the
                // parts it drops are unsettled.
                residency.record_published(current, &view);
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
mod tests;

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

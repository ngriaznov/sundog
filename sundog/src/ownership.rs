//! Bucket ownership for a `Mode::Distributed` cache: which live nodes own
//! each of a cache's buckets, computed by rendezvous (highest-random-weight)
//! hashing over the cache's live, protocol-compatible peers. A node always
//! owns its own view's computation regardless of what else is live: the
//! degenerate one-node case is a valid, if under-replicated, cluster.
//!
//! [`OwnershipView`] is the immutable, point-in-time answer; [`OwnershipTracker`]
//! is the live-updating handle every reader shares. [`ResidencySet`] is a
//! separate, time-based layer tracking buckets a node has stopped owning but
//! keeps serving for a grace period, so a new owner's transfer has time to
//! land.

use std::collections::{HashMap, HashSet};
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
use crate::store::{BUCKET_COUNT, Mode};
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

/// Ranks `scored` by descending score, ties broken by ascending [`NodeId`],
/// and truncates to `k`. Factored out of [`owners_of_bucket`] so the
/// tie-break and truncation logic is testable against synthetic scores
/// without needing an actual rendezvous-score collision.
fn rank_by_score(mut scored: Vec<(u64, NodeId)>, k: u8) -> Vec<NodeId> {
    scored.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    scored
        .into_iter()
        .take(usize::from(k))
        .map(|(_, node)| node)
        .collect()
}

/// The owners of `bucket` among `eligible`: the top `k` by descending
/// [`rendezvous_score`], ties broken by ascending [`NodeId`].
/// `eligible.len() <= k` degenerates to "every eligible node owns every
/// bucket," returning every entry of `eligible`. Never inspects whether
/// `eligible` contains any particular node; that a view always includes
/// itself is [`OwnershipView::compute`]'s contract, not this function's.
pub(crate) fn owners_of_bucket(eligible: &[NodeId], bucket: u16, k: u8) -> Vec<NodeId> {
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

/// The nodes eligible to own a bucket for `cache`: live peers speaking at
/// least [`wire::PROTOCOL_DISTRIBUTED`] that currently advertise `cache` under
/// `Mode::Distributed` with this same `k`, plus `self_node` unconditionally
/// — a node is always eligible for its own view regardless of what it
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

/// The buckets `view` owns that `peer` also owns: strict-ownership input for
/// anti-entropy's pairing predicate. A residency-widened cohort is a
/// separate, time-based layer on top of this, never a change to this
/// function.
pub(crate) fn shared_owned_buckets(view: &OwnershipView, peer: NodeId) -> Vec<u16> {
    view.owned_buckets()
        .filter(|&bucket| view.owners_of(bucket).contains(&peer))
        .collect()
}

/// The buckets gained and lost between two successive [`OwnershipView`]s for
/// the same cache: a pure set difference over each view's owned-bucket set.
/// `cluster::rebalance::rebalance_task`'s trigger input.
#[must_use]
pub fn ownership_diff(old: &OwnershipView, new: &OwnershipView) -> (Vec<u16>, Vec<u16>) {
    let gained: Vec<u16> = new.owned_buckets().filter(|&b| !old.owns(b)).collect();
    let lost: Vec<u16> = old.owned_buckets().filter(|&b| !new.owns(b)).collect();
    (gained, lost)
}

/// One bucket's live owners, highest rendezvous score first. `k` is a `u8`,
/// so owner sets are tiny.
pub(crate) type OwnerSet = Vec<NodeId>;

/// One cache's computed bucket ownership, current as of the eligible-node
/// set it was built from. Immutable once built: a membership or cache-mode
/// change produces a whole new view via [`OwnershipView::compute`], never a
/// mutation, so a reader holding an `Arc<OwnershipView>` sees a consistent
/// snapshot for the whole of one operation.
#[derive(Debug)]
pub struct OwnershipView {
    view_hash: u64,
    self_node: NodeId,
    owners: Vec<OwnerSet>,
    owned: [u64; BUCKET_COUNT / 64],
}

impl OwnershipView {
    /// Builds the view for `self_node` from `eligible` (typically
    /// [`eligible_owners`]'s output). Folds `self_node` into `eligible` and
    /// dedups before ranking, so this is correct even when called directly
    /// with `self_node` omitted from `eligible`, such as from a test.
    ///
    /// # Panics
    ///
    /// Panics only on the platform-impossible case of [`BUCKET_COUNT`] not
    /// fitting a `u16`.
    #[must_use]
    pub fn compute(self_node: NodeId, eligible: Vec<NodeId>, k: NonZeroU8) -> Self {
        let mut eligible = eligible;
        if !eligible.contains(&self_node) {
            eligible.push(self_node);
        }
        eligible.sort_unstable();
        eligible.dedup();

        let view_hash = view_hash(&eligible);
        let k = k.get();
        let mut owners = Vec::with_capacity(BUCKET_COUNT);
        let mut owned = [0u64; BUCKET_COUNT / 64];
        for bucket in 0..BUCKET_COUNT {
            let bucket = u16::try_from(bucket).expect("invariant: BUCKET_COUNT fits u16");
            let set = owners_of_bucket(&eligible, bucket, k);
            if set.contains(&self_node) {
                let idx = usize::from(bucket);
                owned[idx / 64] |= 1u64 << (idx % 64);
            }
            owners.push(set);
        }
        Self {
            view_hash,
            self_node,
            owners,
            owned,
        }
    }

    /// This view's identity: two views with equal `view_hash` (and equal
    /// `k`, which is gossip-validated before either view is built) hold
    /// identical per-bucket ownership.
    #[must_use]
    pub const fn view_hash(&self) -> u64 {
        self.view_hash
    }

    /// Whether `self_node` owns `bucket`. `false` for a bucket at or past
    /// [`BUCKET_COUNT`].
    #[must_use]
    pub fn owns(&self, bucket: u16) -> bool {
        let idx = usize::from(bucket);
        idx < BUCKET_COUNT && self.owned[idx / 64] & (1u64 << (idx % 64)) != 0
    }

    /// `bucket`'s live owners, highest rendezvous score first. Empty for a
    /// bucket at or past [`BUCKET_COUNT`].
    #[must_use]
    pub fn owners_of(&self, bucket: u16) -> &[NodeId] {
        self.owners
            .get(usize::from(bucket))
            .map_or(&[], Vec::as_slice)
    }

    /// Every bucket `self_node` owns, ascending.
    ///
    /// # Panics
    ///
    /// Panics only on the platform-impossible case of [`BUCKET_COUNT`] not
    /// fitting a `u16`.
    pub fn owned_buckets(&self) -> impl Iterator<Item = u16> + '_ {
        (0..BUCKET_COUNT).filter_map(move |i| {
            let bucket = u16::try_from(i).expect("invariant: BUCKET_COUNT fits u16");
            self.owns(bucket).then_some(bucket)
        })
    }
}

/// A live-updating, cheap-to-clone handle onto one cache's current
/// [`OwnershipView`]. Every reader shares the same watch channel: one
/// recomputation per relevant membership or cache-mode change, never one per
/// reader, and every reader agrees with every other reader on the current
/// view at all times.
#[derive(Debug, Clone)]
pub struct OwnershipTracker {
    view: watch::Receiver<Arc<OwnershipView>>,
}

impl OwnershipTracker {
    /// Computes the first view synchronously — no task, no placeholder —
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
        let eligible = eligible_owners(self_node, peers, modes, cache, k);
        let view = Arc::new(OwnershipView::compute(self_node, eligible, k));
        let (tx, rx) = watch::channel(view);
        (Self { view: rx }, tx)
    }

    /// The current view: a `watch::Receiver::borrow()` plus one cheap `Arc`
    /// clone. Synchronous, so it's safe to call from a write path with no
    /// lock over shard or network state.
    #[must_use]
    pub fn current(&self) -> Arc<OwnershipView> {
        Arc::clone(&self.view.borrow())
    }

    /// A fresh subscription for a caller that awaits the next change, such
    /// as a rebalance trigger.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<Arc<OwnershipView>> {
        self.view.clone()
    }
}

/// Buckets this node has just lost ownership of, per the latest
/// [`OwnershipView`], but keeps resident until a grace period elapses, so a
/// new owner's bucket pull — or, as a self-healing backstop, an ordinary
/// anti-entropy round pairing this node with the new owner — has time to
/// land before the data disappears. Read by anti-entropy's cohort widening
/// and the donor-serving exception; never read by the inbound-apply guard,
/// which stays strict current-view ownership.
pub struct ResidencySet {
    releasing: RwLock<HashMap<u16, Instant>>,
    /// Buckets this node owns but has not yet pulled from a co-owner: a
    /// local miss there says nothing about the key, so a fetch asks the
    /// other owners before answering `None`, and this node declines to
    /// answer a remote fetch's miss for them. Cleared bucket by bucket as
    /// each pull lands, or wholesale when the warm-up gives up.
    cold: RwLock<HashSet<u16>>,
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
            cold: RwLock::new(HashSet::new()),
        }
    }

    /// Marks each of `buckets` cold: owned here, not yet pulled.
    pub(crate) fn mark_cold(&self, buckets: &[u16]) {
        self.cold.write().extend(buckets.iter().copied());
    }

    /// Clears the cold mark from each of `buckets`: their pull landed.
    pub(crate) fn clear_cold(&self, buckets: &[u16]) {
        let mut cold = self.cold.write();
        for bucket in buckets {
            cold.remove(bucket);
        }
    }

    /// Clears every cold mark: the warm-up gave up, so what is here is what
    /// there is.
    pub(crate) fn clear_all_cold(&self) {
        self.cold.write().clear();
    }

    /// Whether `bucket` is owned here but not yet pulled.
    pub(crate) fn is_cold(&self, bucket: u16) -> bool {
        self.cold.read().contains(&bucket)
    }

    /// Marks each of `buckets` as releasing, starting its grace clock. A
    /// bucket already releasing keeps its original clock: only a call to
    /// [`ResidencySet::unmark`] in between resets it.
    pub fn mark_releasing(&self, buckets: &[u16]) {
        let now = Instant::now();
        let mut releasing = self.releasing.write();
        for &bucket in buckets {
            releasing.entry(bucket).or_insert(now);
        }
    }

    /// Clears the release clock for each of `buckets`: for one that regains
    /// ownership before its grace elapsed, so a flap never accumulates
    /// toward release.
    pub fn unmark(&self, buckets: &[u16]) {
        let mut releasing = self.releasing.write();
        for bucket in buckets {
            releasing.remove(bucket);
        }
    }

    /// Whether `bucket` is currently mid disown-grace.
    pub fn is_releasing(&self, bucket: u16) -> bool {
        self.releasing.read().contains_key(&bucket)
    }

    /// Buckets whose grace has elapsed: due for physical release.
    pub fn expired(&self, grace: Duration) -> Vec<u16> {
        let now = Instant::now();
        self.releasing
            .read()
            .iter()
            .filter(|&(_, since)| now.saturating_duration_since(*since) >= grace)
            .map(|(&bucket, _)| bucket)
            .collect()
    }
}

/// Recomputes and republishes `cache`'s view on every change to `cluster`'s
/// live peer set or its advertised cache modes, for as long as `cancel`
/// stays live. Spawned once [`Shard::with_ownership`](crate::store::Shard::with_ownership)
/// has already installed `tx`'s receiver half, so this task's first publish
/// is already the second view a reader could ever see, never the first.
/// Publishes with [`watch::Sender::send_if_modified`], keyed on
/// `view_hash`, so a peer's unrelated gossip key changing never ripples
/// through this cache: the `sundog_owned_buckets` gauge only moves on a
/// real ownership change.
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
        let eligible = eligible_owners(self_node, &peers_snapshot, &modes_snapshot, &cache, k);
        let view = Arc::new(OwnershipView::compute(self_node, eligible, k));
        let new_hash = view.view_hash();
        let published = tx.send_if_modified(|current| {
            if current.view_hash() == new_hash {
                false
            } else {
                *current = Arc::clone(&view);
                true
            }
        });
        if published {
            let owned = u32::try_from(view.owned_buckets().count()).unwrap_or(u32::MAX);
            metrics::gauge!("sundog_owned_buckets", "cache" => cache.to_string())
                .set(f64::from(owned));
            tracing::debug!(
                cache = %cache,
                self_node = %view.self_node,
                view_hash = new_hash,
                owned_buckets = owned,
                "ownership view republished"
            );
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

    proptest! {
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
    fn shared_owned_buckets_is_empty_for_a_peer_owning_nothing_in_common() {
        let self_node = NodeId::from(1);
        let k = NonZeroU8::new(1).expect("nonzero");
        let view = OwnershipView::compute(self_node, vec![self_node], k);
        let stranger = NodeId::from(99);

        assert!(shared_owned_buckets(&view, stranger).is_empty());
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
            view.owned_buckets().next().is_some(),
            "self is folded into eligible and owns at least one bucket somewhere"
        );
    }

    #[test]
    fn ownership_view_owns_and_owners_of_and_owned_buckets_agree() {
        let self_node = NodeId::from(1);
        let eligible = vec![self_node, NodeId::from(2), NodeId::from(3)];
        let k = NonZeroU8::new(2).expect("nonzero");
        let view = OwnershipView::compute(self_node, eligible, k);

        for bucket in view.owned_buckets() {
            assert!(view.owns(bucket));
            assert!(view.owners_of(bucket).contains(&self_node));
        }
        let total = u16::try_from(BUCKET_COUNT).expect("fits");
        if let Some(unowned) = (0..total).find(|&b| !view.owns(b)) {
            assert!(!view.owners_of(unowned).contains(&self_node));
        }
    }

    #[test]
    fn ownership_view_owns_and_owners_of_answer_out_of_range_buckets_safely() {
        let self_node = NodeId::from(1);
        let view = OwnershipView::compute(self_node, vec![], NonZeroU8::new(1).expect("nonzero"));
        let past_range = u16::try_from(BUCKET_COUNT).expect("fits");

        assert!(!view.owns(past_range));
        assert!(view.owners_of(past_range).is_empty());
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
            view.owned_buckets().count(),
            BUCKET_COUNT,
            "the sole eligible node owns every bucket, never zero of them"
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
    fn residency_set_is_releasing_tracks_marked_buckets() {
        let set = ResidencySet::new();
        assert!(!set.is_releasing(3));

        set.mark_releasing(&[3, 4]);
        assert!(set.is_releasing(3));
        assert!(set.is_releasing(4));

        set.unmark(&[3]);
        assert!(!set.is_releasing(3));
        assert!(set.is_releasing(4));
    }

    #[test]
    fn residency_set_cold_marks_clear_per_bucket_or_all_at_once() {
        let set = ResidencySet::new();
        assert!(!set.is_cold(1));
        set.mark_cold(&[1, 2, 3]);
        assert!(set.is_cold(1) && set.is_cold(2) && set.is_cold(3));
        assert!(
            !set.is_releasing(1),
            "cold and releasing are separate marks"
        );
        set.clear_cold(&[2]);
        assert!(set.is_cold(1) && !set.is_cold(2) && set.is_cold(3));
        set.clear_all_cold();
        assert!(!set.is_cold(1) && !set.is_cold(3));
    }

    #[test]
    fn residency_set_expired_returns_only_buckets_past_grace() {
        let set = ResidencySet::new();
        set.mark_releasing(&[1, 2]);

        assert!(
            set.expired(Duration::from_secs(3600)).is_empty(),
            "nothing is past a huge grace yet"
        );
        let mut due = set.expired(Duration::ZERO);
        due.sort_unstable();
        assert_eq!(due, vec![1, 2], "everything is past a zero grace");
    }

    #[test]
    fn residency_set_flap_resets_the_release_clock_instead_of_accumulating() {
        let set = ResidencySet::new();
        set.mark_releasing(&[5]);
        std::thread::sleep(Duration::from_millis(400));
        // The bucket regains ownership before its grace elapses.
        set.unmark(&[5]);
        // It's lost again immediately: the clock must restart from here,
        // not continue running from the first mark 400ms ago. A 200ms
        // grace leaves scheduler jitter a wide margin either way.
        set.mark_releasing(&[5]);

        assert!(
            !set.expired(Duration::from_millis(200)).contains(&5),
            "a flap resets the release clock rather than letting it accumulate across the gap"
        );
    }
}

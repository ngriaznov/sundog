//! Ownership as the model holds it: an [`OwnershipDigest`] per `Distributed`
//! cache, and the pure functions that summarize and compare
//! [`OwnershipShares`].

use std::num::NonZeroU8;
use std::sync::Arc;

use smol_str::SmolStr;
use sundog::NodeId;
use sundog::observe::OwnershipShares;
use sundog::store::PartId;

/// Buckets in the key space.
pub const BUCKETS: usize = 1024;

/// Parts in each bucket.
pub const PARTS_PER_BUCKET: u8 = 64;

/// The lead value of a bucket whose lead is not an index below 255.
pub const NO_LEAD: u8 = u8::MAX;

/// Every part in the key space.
const PART_SPACE: usize = BUCKETS * PARTS_PER_BUCKET as usize;

/// One `Distributed` cache's computed ownership at one moment.
#[derive(Debug, Clone)]
pub struct OwnershipDigest {
    /// The cache name.
    pub cache: SmolStr,
    /// Owners per part.
    pub k: NonZeroU8,
    /// The view hash every eligible member's own view carries.
    pub view_hash: u64,
    /// Whether the view ranks single parts rather than whole buckets.
    pub ranks_parts: bool,
    /// The eligible members, ascending.
    pub eligible: Vec<NodeId>,
    /// Parts owned at any rank, one entry per eligible member, in the order
    /// of `eligible`.
    pub counts: Vec<(NodeId, usize)>,
    /// For each bucket, the index into `eligible` of the node that is first
    /// owner of most of its 64 parts; [`NO_LEAD`] when that index is 255 or
    /// more.
    pub lead: Box<[u8; BUCKETS]>,
    /// The view hash of the digest this one replaced, if any.
    pub previous_view: Option<u64>,
    /// Owner slots that changed hands since the digest this one replaced; 0
    /// without one. See [`moved_parts`].
    pub moved: usize,
    /// The full per-part ownership.
    pub shares: Arc<OwnershipShares>,
}

impl OwnershipDigest {
    /// The digest of `shares`, compared against `previous` (the digest of the
    /// same cache that `shares` replaces) for [`Self::moved`] and
    /// [`Self::previous_view`].
    #[must_use]
    pub fn from_shares(shares: Arc<OwnershipShares>, previous: Option<&Self>) -> Self {
        let eligible = shares.eligible().to_vec();
        let counts = eligible
            .iter()
            .map(|&node| (node, shares.parts_owned_by(node)))
            .collect();
        Self {
            cache: shares.cache().into(),
            k: shares.owners(),
            view_hash: shares.view_hash(),
            ranks_parts: shares.ranks_parts(),
            lead: Box::new(lead_owners(&shares)),
            previous_view: previous.map(|p| p.view_hash),
            moved: previous.map_or(0, |p| moved_parts(&p.shares, &shares)),
            eligible,
            counts,
            shares,
        }
    }

    /// Parts `node` owns at any rank; 0 off the eligible set.
    #[must_use]
    pub fn parts_owned_by(&self, node: NodeId) -> usize {
        self.shares.parts_owned_by(node)
    }

    /// The index of `node` in [`Self::eligible`].
    #[must_use]
    pub fn position(&self, node: NodeId) -> Option<usize> {
        self.eligible.binary_search(&node).ok()
    }
}

/// For each of the 1024 buckets, the index into `shares.eligible()` of the
/// node that is first owner of the most parts of that bucket, the lower index
/// winning a tie. An index of 255 or more is [`NO_LEAD`].
#[must_use]
pub fn lead_owners(shares: &OwnershipShares) -> [u8; BUCKETS] {
    let eligible = shares.eligible();
    let mut lead = [NO_LEAD; BUCKETS];
    let mut tally = vec![0u8; eligible.len()];
    for (bucket, slot) in lead.iter_mut().enumerate() {
        tally.fill(0);
        let bucket = u16::try_from(bucket).unwrap_or(0);
        for part in 0..PARTS_PER_BUCKET {
            let first = shares.owners_of(PartId::new(bucket, part)).first();
            if let Some(index) = first.and_then(|node| eligible.binary_search(node).ok()) {
                tally[index] += 1;
            }
        }
        let best = tally
            .iter()
            .enumerate()
            .max_by_key(|&(index, &count)| (count, std::cmp::Reverse(index)));
        if let Some((index, &count)) = best
            && count > 0
        {
            *slot = u8::try_from(index)
                .ok()
                .filter(|&index| index != NO_LEAD)
                .unwrap_or(NO_LEAD);
        }
    }
    lead
}

/// How many owner slots change hands from `a` to `b`: the number of
/// (part, node) pairs where the node owns the part in `b` and did not in `a`.
/// Adding a node to a view of `n` nodes moves about `1 / (n + 1)` of the
/// `k × 65,536` slots.
#[must_use]
pub fn moved_parts(a: &OwnershipShares, b: &OwnershipShares) -> usize {
    (0..PART_SPACE)
        .map(|index| {
            let part = PartId::from_index(index);
            let before = a.owners_of(part);
            b.owners_of(part)
                .iter()
                .filter(|node| !before.contains(node))
                .count()
        })
        .sum()
}

/// Each node's change in parts owned from `a` to `b`, ascending by node over
/// the union of both eligible sets, zero deltas included. The deltas sum to
/// 0 when `min(k, eligible)` is the same in both views.
#[must_use]
pub fn share_deltas(a: &OwnershipShares, b: &OwnershipShares) -> Vec<(NodeId, i64)> {
    let mut nodes: Vec<NodeId> = a.eligible().iter().chain(b.eligible()).copied().collect();
    nodes.sort_unstable();
    nodes.dedup();
    let owned = |shares: &OwnershipShares, node| {
        i64::try_from(shares.parts_owned_by(node)).unwrap_or(i64::MAX)
    };
    nodes
        .into_iter()
        .map(|node| (node, owned(b, node) - owned(a, node)))
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::model::testkit;

    use super::*;

    fn shares(nodes: u8) -> Arc<OwnershipShares> {
        Arc::new(
            testkit::snapshot(nodes)
                .ownership("it", NonZeroU8::new(2).unwrap())
                .expect("live advertisers are eligible"),
        )
    }

    #[test]
    fn a_one_node_view_paints_every_bucket_with_that_node() {
        let one = shares(1);
        let lead = lead_owners(&one);
        assert!(lead.iter().all(|&index| index == 0));
    }

    #[test]
    fn three_nodes_each_lead_about_a_third_of_the_buckets() {
        let three = shares(3);
        let lead = lead_owners(&three);
        let mut leads = [0usize; 3];
        for &index in &lead {
            leads[usize::from(index)] += 1;
        }
        for led in leads {
            assert!((250..=430).contains(&led), "{leads:?}");
        }
    }

    #[test]
    fn lead_indexes_point_into_the_eligible_list() {
        let five = shares(5);
        assert!(
            lead_owners(&five)
                .iter()
                .all(|&index| usize::from(index) < 5)
        );
    }

    #[test]
    fn a_view_moves_nothing_against_itself() {
        let three = shares(3);
        assert_eq!(moved_parts(&three, &three), 0);
    }

    #[test]
    fn adding_a_fourth_node_moves_about_a_quarter_of_the_owner_slots() {
        let (three, four) = (shares(3), shares(4));
        let slots = 2 * PART_SPACE;
        #[expect(clippy::cast_precision_loss, reason = "slot counts are small")]
        let fraction = moved_parts(&three, &four) as f64 / slots as f64;
        assert!((fraction - 0.25).abs() < 0.03, "{fraction}");
    }

    #[test]
    fn share_deltas_sum_to_zero_and_favor_the_new_node() {
        let (three, four) = (shares(3), shares(4));
        let deltas = share_deltas(&three, &four);
        assert_eq!(deltas.len(), 4);
        assert_eq!(deltas.iter().map(|&(_, d)| d).sum::<i64>(), 0);
        let newest = *four
            .eligible()
            .iter()
            .find(|n| !three.eligible().contains(n))
            .unwrap();
        for (node, delta) in deltas {
            if node == newest {
                assert!(delta > 0);
            } else {
                assert!(delta < 0, "{node}: {delta}");
            }
        }
    }

    #[test]
    fn share_deltas_cover_a_departed_node() {
        let (four, three) = (shares(4), shares(3));
        let deltas = share_deltas(&four, &three);
        let gone = *four
            .eligible()
            .iter()
            .find(|n| !three.eligible().contains(n))
            .unwrap();
        let delta = deltas.iter().find(|&&(n, _)| n == gone).unwrap().1;
        assert_eq!(delta, -i64::try_from(four.parts_owned_by(gone)).unwrap());
    }

    #[test]
    fn a_digest_summarizes_its_shares() {
        let three = shares(3);
        let digest = OwnershipDigest::from_shares(three.clone(), None);
        assert_eq!(digest.cache, "it");
        assert_eq!(digest.k.get(), 2);
        assert_eq!(digest.view_hash, three.view_hash());
        assert_eq!(digest.ranks_parts, three.ranks_parts());
        assert_eq!(digest.eligible, three.eligible());
        assert_eq!(digest.counts.len(), 3);
        assert_eq!(
            digest.counts.iter().map(|&(_, c)| c).sum::<usize>(),
            2 * PART_SPACE
        );
        assert_eq!(digest.previous_view, None);
        assert_eq!(digest.moved, 0);
        assert_eq!(*digest.lead, lead_owners(&three));
        for (index, &node) in digest.eligible.iter().enumerate() {
            assert_eq!(digest.position(node), Some(index));
            assert_eq!(digest.parts_owned_by(node), digest.counts[index].1);
        }
        assert_eq!(digest.position(NodeId::from(u64::MAX >> 1)), None);
    }

    #[test]
    fn a_digest_records_what_it_replaced() {
        let (three, four) = (shares(3), shares(4));
        let before = OwnershipDigest::from_shares(three.clone(), None);
        let after = OwnershipDigest::from_shares(four.clone(), Some(&before));
        assert_eq!(after.previous_view, Some(three.view_hash()));
        assert_eq!(after.moved, moved_parts(&three, &four));
        assert!(after.moved > 0);
        assert_ne!(after.view_hash, before.view_hash);
    }
}

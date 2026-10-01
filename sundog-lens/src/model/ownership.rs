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

/// Pixels in the compact mosaic: two buckets share each.
pub const COMPACT_PIXELS: usize = BUCKETS / 2;

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
    /// For each pair of adjacent buckets, the index into `eligible` of the node
    /// that is first owner of most of the pair's 128 parts; [`NO_LEAD`] when
    /// that index is 255 or more.
    pub lead_compact: Box<[u8; COMPACT_PIXELS]>,
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
            lead_compact: Box::new(lead_owners_compact(&shares)),
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
    let mut lead = [NO_LEAD; BUCKETS];
    lead.copy_from_slice(&lead_owners_by(shares, usize::from(PARTS_PER_BUCKET)));
    lead
}

/// For each of the 512 pairs of adjacent buckets, the lead over the pair's 128
/// parts, as [`lead_owners_by`] with 128 parts per pixel computes it.
#[must_use]
pub fn lead_owners_compact(shares: &OwnershipShares) -> [u8; COMPACT_PIXELS] {
    let mut lead = [NO_LEAD; COMPACT_PIXELS];
    lead.copy_from_slice(&lead_owners_by(shares, 2 * usize::from(PARTS_PER_BUCKET)));
    lead
}

/// For each run of `parts_per_pixel` consecutive parts, in part order, the
/// index into `shares.eligible()` of the node that is first owner of the most
/// parts of the run, the lower index winning a tie. An index of 255 or more,
/// and a run no eligible node leads, is [`NO_LEAD`]. `parts_per_pixel` of 0 is
/// 1; a final short run counts as a pixel.
#[must_use]
pub fn lead_owners_by(shares: &OwnershipShares, parts_per_pixel: usize) -> Vec<u8> {
    let eligible = shares.eligible();
    let run = parts_per_pixel.max(1);
    let mut tally = vec![0usize; eligible.len()];
    (0..PART_SPACE)
        .step_by(run)
        .map(|start| {
            tally.fill(0);
            for index in start..(start + run).min(PART_SPACE) {
                let first = shares.owners_of(PartId::from_index(index)).first();
                if let Some(slot) = first.and_then(|node| eligible.binary_search(node).ok()) {
                    tally[slot] += 1;
                }
            }
            tally
                .iter()
                .enumerate()
                .max_by_key(|&(index, &count)| (count, std::cmp::Reverse(index)))
                .filter(|&(_, &count)| count > 0)
                .and_then(|(index, _)| u8::try_from(index).ok())
                .filter(|&index| index != NO_LEAD)
                .unwrap_or(NO_LEAD)
        })
        .collect()
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

    /// The first-owner tallies of parts `start..start + len`, per eligible index.
    fn tally(shares: &OwnershipShares, start: usize, len: usize) -> Vec<usize> {
        let mut counts = vec![0usize; shares.eligible().len()];
        for index in start..start + len {
            let owners = shares.owners_of(PartId::from_index(index));
            let at = shares.eligible().binary_search(&owners[0]).unwrap();
            counts[at] += 1;
        }
        counts
    }

    #[test]
    fn a_bucket_lead_is_the_plurality_of_its_64_parts() {
        let five = shares(5);
        let lead = lead_owners(&five);
        for bucket in [0usize, 1, 2, 511, 512, 1023] {
            let counts = tally(&five, bucket * 64, 64);
            let most = *counts.iter().max().unwrap();
            assert_eq!(counts[usize::from(lead[bucket])], most, "bucket {bucket}");
        }
    }

    #[test]
    fn a_compact_lead_is_the_plurality_of_the_pairs_128_parts() {
        let five = shares(5);
        let compact = lead_owners_compact(&five);
        let lead = lead_owners(&five);
        for pair in 0..COMPACT_PIXELS {
            let counts = tally(&five, pair * 128, 128);
            let most = *counts.iter().max().unwrap();
            assert_eq!(counts[usize::from(compact[pair])], most, "pair {pair}");
        }
        // Not the lower of the two bucket leads: some pair has a higher lead.
        assert!(
            (0..COMPACT_PIXELS).any(|p| compact[p] > lead[2 * p].min(lead[2 * p + 1])),
            "every pair drew its lower bucket lead"
        );
    }

    #[test]
    fn lead_owners_by_counts_runs_of_the_given_length() {
        let three = shares(3);
        assert_eq!(lead_owners_by(&three, 64).len(), BUCKETS);
        assert_eq!(lead_owners_by(&three, 128).len(), COMPACT_PIXELS);
        assert_eq!(lead_owners_by(&three, 0).len(), PART_SPACE);
        assert_eq!(
            lead_owners_by(&three, PART_SPACE),
            lead_owners_by(&three, PART_SPACE + 1)
        );
        assert_eq!(lead_owners_by(&three, PART_SPACE).len(), 1);
        // A run of one part is that part's first owner.
        let single = lead_owners_by(&three, 1);
        for index in [0usize, 1, 777, PART_SPACE - 1] {
            let owner = three.owners_of(PartId::from_index(index))[0];
            assert_eq!(
                usize::from(single[index]),
                three.eligible().binary_search(&owner).unwrap()
            );
        }
    }

    #[test]
    fn a_one_node_view_paints_every_compact_pixel_with_that_node() {
        let one = shares(1);
        assert!(lead_owners_compact(&one).iter().all(|&index| index == 0));
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
        assert_eq!(*digest.lead_compact, lead_owners_compact(&three));
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

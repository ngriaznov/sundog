//! Pure derived values: fair share, agreement, settling and traffic ratios.

use std::num::NonZeroU8;
use std::time::Duration;

use super::ownership::{BUCKETS, PARTS_PER_BUCKET};
use super::{GOSSIP_SETTLE, count_to_f64};

/// Parts in the key space.
pub const PART_SPACE: usize = BUCKETS * PARTS_PER_BUCKET as usize;

/// Consecutive scrapes with a zero `rebalance_parts_total{direction="in"}`
/// rate that a node needs before it counts as settled.
pub const QUIET_SCRAPES: u32 = 2;

/// The fraction of the key space each of `n` eligible nodes owns at any rank
/// when every part has `k` owners: `min(k, n) / n`. 0 for no node.
#[must_use]
pub fn fair_share(k: NonZeroU8, n: usize) -> f64 {
    if n == 0 {
        return 0.0;
    }
    let owners = usize::from(k.get()).min(n);
    ratio(owners, n)
}

/// The fraction of the key space a node owns at any rank, from the parts it
/// owns.
#[must_use]
pub fn share_fraction(parts_owned: usize) -> f64 {
    ratio(parts_owned, PART_SPACE)
}

/// How a node's reported part count compares with the computed one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Agreement {
    /// The node reports exactly the computed count (`✓`).
    Match,
    /// The node reports another count; it is still settling (`↻`).
    Differs,
    /// The node reports nothing.
    Unknown,
}

/// Compares the `sundog_owned_parts` a node reports for a cache with the parts
/// the observer computes it owns.
#[must_use]
pub fn agreement(reported: Option<f64>, computed: usize) -> Agreement {
    match reported {
        None => Agreement::Unknown,
        Some(value) if (value - count_to_f64(computed)).abs() < 0.5 => Agreement::Match,
        Some(_) => Agreement::Differs,
    }
}

/// What the settling test knows about one eligible node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeProgress {
    /// Whether its reported parts match the computed ones; `None` without
    /// metrics.
    pub agrees: Option<bool>,
    /// Consecutive scrapes in which its parts-in rate was zero.
    pub quiet_scrapes: u32,
}

/// Whether a cache has settled, and what the verdict rests on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Settle {
    /// Whether the cache has settled.
    pub settled: bool,
    /// Whether the verdict rests on gossip alone: no eligible node reports
    /// metrics, so the view only has to hold for [`GOSSIP_SETTLE`].
    pub gossip_only: bool,
}

/// Whether a cache has settled: every eligible node that reports metrics
/// agrees with the computed ownership and has been quiet for
/// [`QUIET_SCRAPES`] scrapes. With no node reporting metrics the cache is
/// settled once its view has held for [`GOSSIP_SETTLE`], and the verdict is
/// marked gossip-only.
#[must_use]
pub fn settled(view_held: Duration, nodes: &[NodeProgress]) -> Settle {
    let mut reporting = nodes.iter().filter(|node| node.agrees.is_some()).peekable();
    if reporting.peek().is_none() {
        return Settle {
            settled: view_held >= GOSSIP_SETTLE,
            gossip_only: true,
        };
    }
    Settle {
        settled: reporting
            .all(|node| node.agrees == Some(true) && node.quiet_scrapes >= QUIET_SCRAPES),
        gossip_only: false,
    }
}

/// The fraction of the owner slots the nodes report holding:
/// `Σ reported / (min(k, eligible) × 65,536)`. `None` without an eligible node.
#[must_use]
pub fn coverage(reported_sum: f64, k: NonZeroU8, eligible: usize) -> Option<f64> {
    let owners = usize::from(k.get()).min(eligible);
    (owners > 0).then(|| reported_sum / count_to_f64(owners * PART_SPACE))
}

/// The spread of a Replicated cache's entry counts across its advertisers:
/// largest minus smallest. `None` for fewer than two counts.
#[must_use]
pub fn divergence(entries: &[f64]) -> Option<f64> {
    if entries.len() < 2 {
        return None;
    }
    let max = entries.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let min = entries.iter().copied().fold(f64::INFINITY, f64::min);
    Some(max - min)
}

/// `hits / (hits + misses)` from two rates; `None` at zero traffic.
#[must_use]
pub fn hit_ratio(hit_rate: f64, miss_rate: f64) -> Option<f64> {
    let total = hit_rate + miss_rate;
    (total > 0.0).then(|| hit_rate / total)
}

/// The share of `fetch` calls by outcome, each in `0.0..=1.0`, summing to 1.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FetchMix {
    /// Served from the local copy.
    pub local: f64,
    /// Fetched from a remote owner.
    pub remote: f64,
    /// Found nowhere.
    pub miss: f64,
    /// Failed.
    pub error: f64,
}

/// The mix of `fetch` outcomes from four rates; `None` at zero traffic.
#[must_use]
pub fn fetch_mix(local: f64, remote: f64, miss: f64, error: f64) -> Option<FetchMix> {
    let total = local + remote + miss + error;
    (total > 0.0).then(|| FetchMix {
        local: local / total,
        remote: remote / total,
        miss: miss / total,
        error: error / total,
    })
}

/// Whether a node's reported `sundog_live_peers` matches the observer's count
/// of live members (which includes the node itself); `None` without metrics.
#[must_use]
pub fn peers_agree(reported: Option<f64>, live_members: usize) -> Option<bool> {
    let expected = count_to_f64(live_members.saturating_sub(1));
    reported.map(|value| (value - expected).abs() < 0.5)
}

fn ratio(numerator: usize, denominator: usize) -> f64 {
    count_to_f64(numerator) / count_to_f64(denominator)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(owners: u8) -> NonZeroU8 {
        NonZeroU8::new(owners).unwrap()
    }

    fn progress(agrees: Option<bool>, quiet_scrapes: u32) -> NodeProgress {
        NodeProgress {
            agrees,
            quiet_scrapes,
        }
    }

    #[test]
    fn fair_share_is_min_k_n_over_n() {
        assert!((fair_share(k(2), 4) - 0.5).abs() < 1e-12);
        assert!((fair_share(k(2), 3) - 2.0 / 3.0).abs() < 1e-12);
        assert!((fair_share(k(1), 4) - 0.25).abs() < 1e-12);
    }

    #[test]
    fn fair_share_caps_at_every_node_owning_everything() {
        assert!((fair_share(k(3), 2) - 1.0).abs() < 1e-12);
        assert!((fair_share(k(2), 1) - 1.0).abs() < 1e-12);
        assert!(fair_share(k(2), 0).abs() < 1e-12);
    }

    #[test]
    fn a_share_fraction_is_parts_over_the_part_space() {
        assert!((share_fraction(PART_SPACE) - 1.0).abs() < 1e-12);
        assert!((share_fraction(PART_SPACE / 4) - 0.25).abs() < 1e-12);
        assert!(share_fraction(0).abs() < 1e-12);
    }

    #[test]
    fn agreement_compares_the_reported_count_with_the_computed_one() {
        assert_eq!(agreement(Some(32_768.0), 32_768), Agreement::Match);
        assert_eq!(agreement(Some(30_112.0), 32_768), Agreement::Differs);
        assert_eq!(agreement(Some(32_769.0), 32_768), Agreement::Differs);
        assert_eq!(agreement(None, 32_768), Agreement::Unknown);
        assert_eq!(agreement(Some(0.0), 0), Agreement::Match);
    }

    #[test]
    fn without_metrics_a_cache_settles_after_the_gossip_hold() {
        let none = [progress(None, 0), progress(None, 0)];
        let waiting = settled(Duration::from_millis(2_999), &none);
        assert_eq!(
            waiting,
            Settle {
                settled: false,
                gossip_only: true
            }
        );
        let held = settled(GOSSIP_SETTLE, &none);
        assert_eq!(
            held,
            Settle {
                settled: true,
                gossip_only: true
            }
        );
        assert!(settled(GOSSIP_SETTLE, &[]).gossip_only);
    }

    #[test]
    fn with_metrics_every_reporting_node_must_agree_and_be_quiet() {
        let long = Duration::from_secs(60);
        let two = QUIET_SCRAPES;
        let settled_all = settled(long, &[progress(Some(true), two), progress(Some(true), 9)]);
        assert_eq!(
            settled_all,
            Settle {
                settled: true,
                gossip_only: false
            }
        );
        // One quiet scrape is not enough: it needs two.
        assert!(!settled(long, &[progress(Some(true), two - 1)]).settled);
        // One node still differs.
        assert!(
            !settled(
                long,
                &[progress(Some(true), two), progress(Some(false), two)]
            )
            .settled
        );
        // The view's age does not matter once metrics report.
        assert!(settled(Duration::ZERO, &[progress(Some(true), two)]).settled);
    }

    #[test]
    fn a_node_without_metrics_does_not_block_the_others() {
        let nodes = [progress(Some(true), QUIET_SCRAPES), progress(None, 0)];
        let verdict = settled(Duration::ZERO, &nodes);
        assert!(verdict.settled && !verdict.gossip_only);
    }

    #[test]
    fn coverage_is_reported_slots_over_owned_slots() {
        let full = coverage(131_072.0, k(2), 4).unwrap();
        assert!((full - 1.0).abs() < 1e-12);
        let half = coverage(65_536.0, k(2), 4).unwrap();
        assert!((half - 0.5).abs() < 1e-12);
        // k above the node count counts every node once.
        let few = coverage(65_536.0, k(3), 1).unwrap();
        assert!((few - 1.0).abs() < 1e-12);
        assert_eq!(coverage(1.0, k(2), 0), None);
    }

    #[test]
    fn divergence_is_the_spread_of_entry_counts() {
        assert_eq!(divergence(&[10.0, 12.0, 11.0]), Some(2.0));
        assert_eq!(divergence(&[5.0, 5.0]), Some(0.0));
        assert_eq!(divergence(&[5.0]), None);
        assert_eq!(divergence(&[]), None);
    }

    #[test]
    fn a_hit_ratio_needs_traffic() {
        assert_eq!(hit_ratio(0.0, 0.0), None);
        assert!((hit_ratio(93.0, 7.0).unwrap() - 0.93).abs() < 1e-12);
        assert!(hit_ratio(0.0, 5.0).unwrap().abs() < 1e-12);
    }

    #[test]
    fn a_fetch_mix_sums_to_one_and_needs_traffic() {
        assert_eq!(fetch_mix(0.0, 0.0, 0.0, 0.0), None);
        let mix = fetch_mix(61.0, 30.0, 9.0, 0.0).unwrap();
        assert!((mix.local - 0.61).abs() < 1e-12);
        assert!((mix.remote - 0.30).abs() < 1e-12);
        assert!((mix.miss - 0.09).abs() < 1e-12);
        assert!(mix.error.abs() < 1e-12);
        let sum = mix.local + mix.remote + mix.miss + mix.error;
        assert!((sum - 1.0).abs() < 1e-12);
    }

    #[test]
    fn peers_agree_when_the_node_counts_every_other_live_member() {
        assert_eq!(peers_agree(Some(3.0), 4), Some(true));
        assert_eq!(peers_agree(Some(4.0), 4), Some(false));
        assert_eq!(peers_agree(None, 4), None);
        assert_eq!(peers_agree(Some(0.0), 0), Some(true));
    }
}

//! Owned-bucket estimation for the TUI: `Cache::owners_of` is exact per key
//! but there is no public way to enumerate the 1,024 anti-entropy buckets
//! directly, so a node's share is estimated by probing a fixed set of keys
//! and scaling the hit rate up to the real bucket count.

use sundog::NodeId;

/// The number of anti-entropy buckets a distributed cache splits into.
/// Matches `sundog::store::BUCKET_COUNT`, which isn't public.
pub(crate) const BUCKET_COUNT: usize = 1024;

/// How many probe keys the sample set carries.
pub(crate) const PROBE_COUNT: usize = 4096;

/// The fixed probe key set: `"probe0".."probe4095"`, distinct from the
/// preload key space (`"k0"..`) so probing never touches real demo data.
#[must_use]
pub(crate) fn probe_keys() -> Vec<String> {
    (0..PROBE_COUNT).map(|i| format!("probe{i}")).collect()
}

/// Scales a hit count over `probe_count` probes up to an estimated bucket
/// count out of `bucket_count` total buckets, rounding to the nearest
/// bucket. `0` for zero probes.
#[must_use]
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
pub(crate) fn estimate_owned_buckets(
    hits: usize,
    probe_count: usize,
    bucket_count: usize,
) -> usize {
    if probe_count == 0 {
        return 0;
    }
    let fraction = hits as f64 / probe_count as f64;
    (fraction * bucket_count as f64).round() as usize
}

/// Counts, over `owners_by_probe` (one owners list per probe key, in the
/// same order [`probe_keys`] produced them), how many list `node`.
#[must_use]
pub(crate) fn count_hits(owners_by_probe: &[Vec<NodeId>], node: NodeId) -> usize {
    owners_by_probe
        .iter()
        .filter(|owners| owners.contains(&node))
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_keys_are_distinct_and_disjoint_from_the_preload_prefix() {
        let keys = probe_keys();
        assert_eq!(keys.len(), PROBE_COUNT);
        let unique: std::collections::HashSet<_> = keys.iter().collect();
        assert_eq!(unique.len(), PROBE_COUNT);
        assert!(keys.iter().all(|k| k.starts_with("probe")));
    }

    #[test]
    fn estimate_owned_buckets_scales_hit_rate_to_bucket_count() {
        assert_eq!(estimate_owned_buckets(2048, 4096, 1024), 512);
        assert_eq!(estimate_owned_buckets(4096, 4096, 1024), 1024);
        assert_eq!(estimate_owned_buckets(0, 4096, 1024), 0);
    }

    #[test]
    fn estimate_owned_buckets_is_zero_for_zero_probes() {
        assert_eq!(estimate_owned_buckets(0, 0, 1024), 0);
    }

    #[test]
    fn count_hits_counts_only_lists_containing_the_node() {
        let a = NodeId::from(1);
        let b = NodeId::from(2);
        let owners_by_probe = vec![vec![a, b], vec![b], vec![a]];
        assert_eq!(count_hits(&owners_by_probe, a), 2);
        assert_eq!(count_hits(&owners_by_probe, b), 2);
        assert_eq!(count_hits(&owners_by_probe, NodeId::from(3)), 0);
    }
}

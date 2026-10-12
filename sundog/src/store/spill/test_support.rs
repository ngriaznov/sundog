//! Test-only helpers for reading the spill tier's metrics.

/// Test-only: the `sundog_spill_dropped_total{cache, reason}` increments
/// `snapshotter` saw for `cache` since its previous snapshot, by reason,
/// zero counts left out: a snapshot drains the counters.
#[cfg(all(test, not(feature = "sim")))]
pub(crate) fn dropped_since_last_snapshot(
    snapshotter: &metrics_util::debugging::Snapshotter,
    cache: &str,
) -> std::collections::BTreeMap<String, u64> {
    let mut by_reason = std::collections::BTreeMap::new();
    for (key, _, _, value) in snapshotter.snapshot().into_vec() {
        let label = |name: &str| {
            key.key()
                .labels()
                .find(|l| l.key() == name)
                .map(|l| l.value().to_string())
        };
        if key.key().name() != "sundog_spill_dropped_total"
            || label("cache").as_deref() != Some(cache)
        {
            continue;
        }
        if let (Some(reason), metrics_util::debugging::DebugValue::Counter(n)) =
            (label("reason"), value)
            && n > 0
        {
            *by_reason.entry(reason).or_insert(0) += n;
        }
    }
    by_reason
}

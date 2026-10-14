//! Anti-entropy between two real nodes after one swept an expired overwrite.

use super::ae_repair::ResolverPair;
use super::*;
use crate::cluster::anti_entropy::RoundOutcome;
use crate::explain::LocalRecord;
use crate::hlc::Hlc;
use crate::store::LwwResolver;
use crate::wire::WireRecord;
use std::time::Duration;

/// Key 1's overwrite as a peer sends it: `value` at `wall_ms`, expiring at `expires_at_ms`.
fn overwrite(
    value: &str,
    wall_ms: u64,
    expires_at_ms: u64,
    node: crate::node::NodeId,
) -> WireRecord {
    WireRecord {
        expires_at_ms: Some(expires_at_ms),
        ..super::ae_repair::test_record(1, Some(value), wall_ms, node)
    }
}

/// `a` swept an overwrite that expired and `b` missed it, still holding the
/// value it replaced. A round started from either side leaves neither node
/// holding that value, where without the tombstone `a` took it back from `b`
/// and handed it on.
#[tokio::test]
async fn a_round_does_not_bring_back_a_value_overwritten_by_a_swept_expired_entry() {
    for (label, from_a) in [("a-initiates", true), ("b-initiates", false)] {
        let pair =
            ResolverPair::open_with(&format!("cluster-it-ae-swept-expiry-{label}"), LwwResolver)
                .await;
        pair.apply(false, 1, Some("old"), 100).await;
        let expires_at = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("after the epoch")
                .as_millis(),
        )
        .expect("fits")
            + 400;
        pair.shard_a
            .apply_remote(overwrite("new", 200, expires_at, pair.cluster_a.node_id()))
            .await;
        assert_eq!(pair.cache_a.get(&1).await.as_deref(), Some("new"));
        assert_eq!(pair.cache_b.get(&1).await.as_deref(), Some("old"));

        wait_until(Duration::from_secs(10), label, async || {
            pair.cache_a.get(&1).await.is_none()
        })
        .await;
        pair.shard_a.run_pending_tasks().await;
        assert!(
            matches!(
                pair.cache_a.explain(&1).await.expect("explains").local,
                LocalRecord::Tombstone { .. }
            ),
            "{label}: a swept the overwrite and kept its version"
        );

        assert_eq!(
            pair.round(from_a).await,
            RoundOutcome::Reconciled,
            "{label}"
        );
        pair.wait_converged(label).await;
        assert_eq!(pair.cache_a.get(&1).await, None, "{label}: a");
        assert_eq!(
            pair.cache_b.get(&1).await,
            None,
            "{label}: b's stale copy is removed"
        );
        let held: Vec<Hlc> = pair
            .shard_b
            .records_for(vec![
                super::ae_repair::test_record(1, None, 0, pair.cluster_a.node_id()).key,
            ])
            .await
            .into_iter()
            .map(|rec| rec.ver)
            .collect();
        assert_eq!(
            held.len(),
            1,
            "{label}: b holds the tombstone it pulled: {held:?}"
        );
        pair.shutdown().await;
    }
}

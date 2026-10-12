//! Tests for the cluster background tasks: inbound activity, mode-conflict reporting and re-forward grouping.

use super::*;
use crate::hlc::Hlc;
use crate::wire::WireRecord;
use bytes::Bytes;

#[test]
fn inbound_activity_is_recent_only_inside_the_window() {
    let activity = InboundActivity::default();
    let peer = NodeId::from(5);
    assert!(
        !activity.recent(peer, Duration::from_secs(10)),
        "never seen"
    );
    activity.note(peer);
    assert!(activity.recent(peer, Duration::from_secs(10)));
    std::thread::sleep(Duration::from_millis(15));
    assert!(
        !activity.recent(peer, Duration::from_millis(5)),
        "a stamp older than the window does not count"
    );
    assert!(!activity.recent(NodeId::from(6), Duration::from_secs(10)));
}

#[test]
fn mode_conflicts_are_reported_once_per_peer_and_cache() {
    let peer_a = NodeId::from(1);
    let peer_b = NodeId::from(2);
    let local: HashMap<SmolStr, Mode> = [
        (SmolStr::new("users"), Mode::Replicated),
        (SmolStr::new("orders"), Mode::Invalidation),
    ]
    .into_iter()
    .collect();
    let mut advertised: CacheModes = HashMap::new();
    advertised.insert(
        peer_a,
        [
            (SmolStr::new("users"), Mode::Invalidation),
            (SmolStr::new("orders"), Mode::Invalidation),
            (SmolStr::new("unrelated"), Mode::Replicated),
        ]
        .into_iter()
        .collect(),
    );
    advertised.insert(
        peer_b,
        [(SmolStr::new("users"), Mode::Replicated)]
            .into_iter()
            .collect(),
    );
    let mut warned = HashSet::new();

    assert_eq!(
        new_mode_conflicts(&advertised, &local, &mut warned),
        vec![ModeConflict {
            peer: peer_a,
            cache: SmolStr::new("users"),
            local: Mode::Replicated,
            remote: Mode::Invalidation,
        }],
        "only the pair whose modes differ counts; matching and unknown caches are ignored"
    );
    assert!(
        new_mode_conflicts(&advertised, &local, &mut warned).is_empty(),
        "the same view again reports nothing new"
    );

    advertised
        .get_mut(&peer_b)
        .expect("peer b advertised")
        .insert(SmolStr::new("users"), Mode::Invalidation);
    assert_eq!(
        new_mode_conflicts(&advertised, &local, &mut warned),
        vec![ModeConflict {
            peer: peer_b,
            cache: SmolStr::new("users"),
            local: Mode::Replicated,
            remote: Mode::Invalidation,
        }],
        "a peer that flips later is reported once, without repeating peer a"
    );
    let conflicts = new_mode_conflicts(&advertised, &local, &mut warned);
    assert!(conflicts.is_empty(), "{conflicts:?}");
}

/// A `WireRecord` for `key`, its own postcard-encoded bytes as `key`.
fn wire_record_for_u32(key: u32) -> WireRecord {
    WireRecord {
        key: postcard::to_stdvec(&key)
            .expect("a u32 key always postcard-encodes")
            .into(),
        value: Some(Bytes::from_static(b"\x01v")),
        ver: Hlc {
            wall_ms: 1,
            logical: 0,
            node: NodeId::from(1),
        },
        expires_at_ms: None,
    }
}

/// A batch routed under another view hash at hop zero re-forwards to
/// every current owner but this node and its sender; the same batch at
/// the hop cap, or under this node's own hash, re-forwards to nobody.
#[test]
fn reforward_groups_exclude_self_and_sender_and_stop_at_the_hop_cap() {
    let self_node = NodeId::from(1);
    let sender = NodeId::from(2);
    let third = NodeId::from(3);
    let eligible = vec![self_node, sender, third];
    let k = std::num::NonZeroU8::new(2).expect("nonzero");
    let view = OwnershipView::compute(self_node, eligible, k);
    let stale_hash = view.view_hash() ^ 1;

    let records: Vec<WireRecord> = (0u32..200).map(wire_record_for_u32).collect();
    let groups = reforward_groups(&view, self_node, sender, stale_hash, 0, &records)
        .expect("a hop-zero batch under another view re-forwards");
    assert!(
        !groups.is_empty(),
        "some bucket has an owner besides self and the sender"
    );
    for group in &groups {
        assert_eq!(
            group.owners.as_slice(),
            [third],
            "with three eligible nodes the only other target is the third node"
        );
        for rec in &group.records {
            assert!(
                view.owners_of(crate::store::PartId::of_key(rec.key.as_ref()))
                    .contains(&third),
                "a record only re-forwards to a node that owns its bucket"
            );
        }
    }
    let re_forwarded: usize = groups.iter().map(|g| g.records.len()).sum();
    let expected = records
        .iter()
        .filter(|rec| {
            view.owners_of(crate::store::PartId::of_key(rec.key.as_ref()))
                .contains(&third)
        })
        .count();
    assert_eq!(
        re_forwarded, expected,
        "every record the third node owns, and no other"
    );

    assert!(
        reforward_groups(
            &view,
            self_node,
            sender,
            stale_hash,
            MAX_FORWARD_HOPS,
            &records
        )
        .is_none(),
        "a batch at the hop cap never re-forwards"
    );
    assert!(
        reforward_groups(&view, self_node, sender, view.view_hash(), 0, &records).is_none(),
        "a batch routed under this node's own view needs no re-forward"
    );
}

#[tokio::test]
async fn report_mode_conflicts_logs_a_late_conflict_against_a_live_peer() {
    let cluster = solo_cluster("cluster-it-mode-conflict-log").await;
    let _cache = cluster
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("open succeeds");

    // Hand-built rather than gossiped: `report_mode_conflicts` only
    // reads `cluster`'s own advertised mode, so no live peer is needed
    // to exercise its late-conflict log line deterministically.
    let peer = NodeId::from(99);
    let mut advertised: CacheModes = HashMap::new();
    advertised.insert(
        peer,
        [(SmolStr::new("users"), Mode::Invalidation)]
            .into_iter()
            .collect(),
    );
    let mut warned = HashSet::new();

    report_mode_conflicts(&advertised, &cluster, &mut warned);
    assert!(
        warned.contains(&(peer, SmolStr::new("users"))),
        "the conflict is recorded so a repeated view doesn't re-warn"
    );

    cluster.shutdown().await;
}

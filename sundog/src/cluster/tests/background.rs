//! Tests for the cluster background tasks: inbound activity, mode-conflict reporting and re-forward grouping.

use super::*;
use crate::cluster::inbound::{MAX_FORWARD_HOPS, reforward_groups};
use crate::cluster::test_support::wait_for_no_peers;
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

#[test]
fn a_mode_conflict_with_a_peer_that_left_is_released() {
    let peer = NodeId::from(1);
    let local: HashMap<SmolStr, Mode> = [(SmolStr::new("users"), Mode::Replicated)]
        .into_iter()
        .collect();
    let mut advertised: CacheModes = HashMap::new();
    advertised.insert(
        peer,
        [(SmolStr::new("users"), Mode::Invalidation)]
            .into_iter()
            .collect(),
    );
    let mut warned = HashSet::new();
    assert_eq!(
        new_mode_conflicts(&advertised, &local, &mut warned).len(),
        1
    );

    // The peer leaves the membership view: nothing is advertised any more.
    let gone: CacheModes = HashMap::new();
    let conflicts = new_mode_conflicts(&gone, &local, &mut warned);
    assert!(conflicts.is_empty(), "{conflicts:?}");
    assert!(
        warned.is_empty(),
        "state keyed by a departed peer is released, but warned still holds {warned:?}"
    );
}

/// One cache `"users"` advertised under `mode` by each of `peers`.
fn advertised_users(peers: &[NodeId], mode: Mode) -> CacheModes {
    peers
        .iter()
        .map(|&peer| (peer, HashMap::from([(SmolStr::new("users"), mode)])))
        .collect()
}

#[test]
fn a_mode_conflict_with_a_peer_that_stays_is_not_reported_again() {
    let (leaving, staying) = (NodeId::from(1), NodeId::from(2));
    let local = HashMap::from([(SmolStr::new("users"), Mode::Replicated)]);
    let mut warned = HashSet::new();
    let both = advertised_users(&[leaving, staying], Mode::Invalidation);
    assert_eq!(new_mode_conflicts(&both, &local, &mut warned).len(), 2);

    let only_staying = advertised_users(&[staying], Mode::Invalidation);
    assert!(
        new_mode_conflicts(&only_staying, &local, &mut warned).is_empty(),
        "the peer that stayed is not reported a second time"
    );
    assert_eq!(
        warned,
        HashSet::from([(staying, SmolStr::new("users"))]),
        "the warning of the departed peer goes and the live peer's stays"
    );
}

#[test]
fn a_peer_that_returns_with_its_conflict_is_reported_again() {
    let peer = NodeId::from(1);
    let local = HashMap::from([(SmolStr::new("users"), Mode::Replicated)]);
    let present = advertised_users(&[peer], Mode::Invalidation);
    let mut warned = HashSet::new();
    assert_eq!(new_mode_conflicts(&present, &local, &mut warned).len(), 1);
    let conflicts = new_mode_conflicts(&present, &local, &mut warned);
    assert!(conflicts.is_empty(), "{conflicts:?}");

    let conflicts = new_mode_conflicts(&CacheModes::new(), &local, &mut warned);
    assert!(conflicts.is_empty(), "{conflicts:?}");
    assert_eq!(
        new_mode_conflicts(&present, &local, &mut warned),
        vec![ModeConflict {
            peer,
            cache: SmolStr::new("users"),
            local: Mode::Replicated,
            remote: Mode::Invalidation,
        }],
        "the same peer id appearing with the conflict again is a new report"
    );
}

#[test]
fn forget_departed_warnings_keeps_exactly_the_pairs_of_advertised_peers() {
    let (live, departed) = (NodeId::from(1), NodeId::from(2));
    let pair = |peer, cache: &str| (peer, SmolStr::new(cache));
    let mut warned = HashSet::from([
        pair(live, "users"),
        pair(live, "orders"),
        pair(departed, "users"),
        pair(departed, "orders"),
    ]);
    // A live peer advertising no cache at all is still live.
    let advertised: CacheModes = HashMap::from([(live, HashMap::new())]);
    forget_departed_warnings(&mut warned, &advertised);
    assert_eq!(
        warned,
        HashSet::from([pair(live, "users"), pair(live, "orders")])
    );

    forget_departed_warnings(&mut warned, &advertised);
    assert_eq!(warned.len(), 2, "a second pass changes nothing");
    forget_departed_warnings(&mut warned, &CacheModes::new());
    assert!(warned.is_empty(), "an empty view forgets every pair");
}

#[test]
fn retain_live_stamps_keeps_exactly_the_stamps_of_live_peers() {
    let (live, departed) = (NodeId::from(1), NodeId::from(2));
    let mut seen = HashMap::from([(live, 10_u64), (departed, 20)]);
    retain_live_stamps(&mut seen, &HashSet::from([live, NodeId::from(3)]));
    assert_eq!(
        seen,
        HashMap::from([(live, 10)]),
        "the live peer keeps its stamp, the departed one loses it, and a live peer never stamped is not added"
    );
    retain_live_stamps(&mut seen, &HashSet::new());
    assert!(seen.is_empty());
}

#[test]
fn inbound_activity_forgets_a_peer_that_left_the_view_and_keeps_one_that_stayed() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let (live, departed) = (NodeId::from(1), NodeId::from(2));
    let activity = InboundActivity::default();
    activity.note(live);
    activity.note(departed);
    let window = Duration::from_secs(3600);

    activity.retain_live(&[crate::net::test_support::peer_at(live, addr)]);
    assert!(activity.recent(live, window));
    assert!(!activity.recent(departed, window));
    assert_eq!(activity.seen().len(), 1);

    activity.retain_live(&[]);
    assert!(!activity.recent(live, window));
    assert!(activity.seen().is_empty());
}

/// Replicate traffic from a peer stamps it in the inbound activity; once the
/// peer leaves the view, the membership-to-mesh task forgets the stamp.
#[tokio::test]
async fn a_peer_that_leaves_is_forgotten_by_the_inbound_activity() {
    let cluster_a = Cluster::builder("cluster-it-activity-forgets-departed")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node a builds");
    let cache_a = cluster_a
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("node a opens the cache");
    let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;
    let cluster_b = Cluster::builder("cluster-it-activity-forgets-departed")
        .seeds([gossip_a])
        .config(loopback_config())
        .build()
        .await
        .expect("node b builds");
    let node_b = cluster_b.node_id();
    let cache_b = cluster_b
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("node b opens the cache");
    wait_for_peer_count(&cluster_a, 1).await;

    let window = Duration::from_secs(3600);
    cache_b.insert(1, "one".to_string()).await.expect("insert");
    wait_until(
        Duration::from_secs(20),
        "node a stamps the peer that streamed to it",
        async || {
            cache_a.get(&1).await.is_some()
                && cluster_a.inner.inbound_activity.recent(node_b, window)
        },
    )
    .await;

    cluster_b.shutdown().await;
    wait_for_no_peers(&cluster_a).await;
    wait_until(
        Duration::from_secs(20),
        "node a forgets the stamp of the peer that left",
        async || !cluster_a.inner.inbound_activity.recent(node_b, window),
    )
    .await;

    cluster_a.shutdown().await;
}

/// A `WireRecord` for `key`, its own postcard-encoded bytes as `key`.
pub(super) fn wire_record_for_u32(key: u32) -> WireRecord {
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

/// A re-forwarded record is detached from the frame it arrived in, so the
/// frames it travels on next hold its own bytes, not the inbound buffer.
#[test]
fn reforward_groups_detach_their_records_from_the_inbound_buffer() {
    let self_node = NodeId::from(1);
    let view = OwnershipView::compute(
        self_node,
        (1..=3u64).map(NodeId::from).collect(),
        std::num::NonZeroU8::new(2).expect("nonzero"),
    );
    let records: Vec<WireRecord> = (0u32..50).map(wire_record_for_u32).collect();
    let mut buffer = Vec::new();
    for rec in &records {
        buffer.extend_from_slice(&rec.key);
        buffer.extend_from_slice(rec.value.as_deref().expect("a value"));
    }
    let buffer = Bytes::from(buffer);
    let mut at = 0;
    let sliced: Vec<WireRecord> = records
        .iter()
        .map(|rec| {
            let (key_len, value_len) = (rec.key.len(), rec.value.as_ref().map_or(0, Bytes::len));
            let sliced = WireRecord {
                key: buffer.slice(at..at + key_len),
                value: Some(buffer.slice(at + key_len..at + key_len + value_len)),
                ..rec.clone()
            };
            at += key_len + value_len;
            sliced
        })
        .collect();

    let groups = reforward_groups(
        &view,
        self_node,
        NodeId::from(2),
        view.view_hash() ^ 1,
        0,
        &sliced,
    )
    .expect("a hop-zero batch under another view re-forwards");
    let within = |bytes: &Bytes| buffer.as_ptr_range().contains(&bytes.as_ptr());
    let mut seen = 0;
    for rec in groups.iter().flat_map(|group| &group.records) {
        assert!(!within(&rec.key) && !within(rec.value.as_ref().expect("a value")));
        assert!(records.contains(rec), "the same record, bytes and all");
        seen += 1;
    }
    assert!(seen > 0, "some record re-forwards");
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

#[test]
fn the_gc_grid_tick_is_the_next_multiple_of_the_period() {
    let period = Duration::from_secs(150);
    assert_eq!(
        until_next_grid_tick(1, period),
        Duration::from_millis(149_999)
    );
    assert_eq!(
        until_next_grid_tick(149_999, period),
        Duration::from_millis(1),
        "one millisecond short of a multiple"
    );
    assert_eq!(
        until_next_grid_tick(150_000, period),
        period,
        "a tick on the grid waits a whole period for the next"
    );
    assert_eq!(
        until_next_grid_tick(1_700_000_123_456, period),
        Duration::from_millis(150_000 - 1_700_000_123_456 % 150_000)
    );
}

#[test]
fn every_node_reaches_the_same_gc_instant_from_any_start() {
    let period = Duration::from_secs(150);
    for start in [0u64, 1, 74_999, 75_000, 149_999, 1_700_000_000_123] {
        let wait = u64::try_from(until_next_grid_tick(start, period).as_millis())
            .expect("a wait of at most one period fits");
        let instant = start + wait;
        assert_eq!(instant % 150_000, 0, "start {start}");
        assert!(
            instant > start && instant - start <= 150_000,
            "start {start}"
        );
        let after = until_next_grid_tick(instant, period);
        assert_eq!(after, period, "the following tick is one period on");
    }
}

#[test]
fn a_zero_gc_period_waits_a_millisecond() {
    assert_eq!(
        until_next_grid_tick(5, Duration::ZERO),
        Duration::from_millis(1)
    );
}

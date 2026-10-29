//! Tests for donor grouping, credit, view-change planning and the `rebalance_task` loop over published views.

use super::*;

#[test]
fn group_parts_by_donor_set_groups_parts_sharing_the_same_donors() {
    let self_node = NodeId::from(1);
    let eligible: Vec<NodeId> = (1..=5u64).map(NodeId::from).collect();
    let view = view(self_node, eligible, 2);
    let parts: Vec<PartId> = PartId::all().step_by(97).take(64).collect();

    let groups = group_parts_by_donor_set(&view, self_node, parts.clone());

    // Every part appears in exactly one group, and each group's donor
    // set is precisely that part's owners minus self.
    let mut regrouped: Vec<PartId> = groups.iter().flat_map(|(_, ps)| ps.clone()).collect();
    regrouped.sort_unstable();
    assert_eq!(regrouped, parts);
    for (donors, group_parts) in &groups {
        for &part in group_parts {
            let expected: Vec<NodeId> = view
                .owners_of(part)
                .iter()
                .copied()
                .filter(|&n| n != self_node)
                .collect();
            assert_eq!(donors, &expected);
        }
    }
}

#[test]
fn group_parts_by_donor_set_merges_rankings_that_name_the_same_donors() {
    // Two eligible nodes at k=2: every part's owners are self and the
    // other node, in either order, so every part has one donor set.
    let self_node = NodeId::from(1);
    let other = NodeId::from(2);
    let view = OwnershipView::compute_at(
        self_node,
        vec![self_node, other],
        NonZeroU8::new(2).expect("nonzero"),
        Granularity::Part,
    );
    let orders: HashSet<Vec<NodeId>> = PartId::all()
        .map(|part| view.owners_of(part).to_vec())
        .collect();
    assert_eq!(
        orders.len(),
        2,
        "the fixture ranks self first for some parts and second for others"
    );
    let groups = group_parts_by_donor_set(&view, self_node, PartId::all().collect());
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].0, vec![other]);
    assert_eq!(groups[0].1.len(), crate::store::part::PART_SPACE);
}

#[test]
fn group_parts_by_donor_set_excludes_self_from_every_donor_list() {
    let self_node = NodeId::from(1);
    // Solo eligible set: self is every part's only owner, so every
    // donor list is empty once self is excluded.
    let view = view(self_node, vec![self_node], 2);
    let groups = group_parts_by_donor_set(&view, self_node, PartId::all().take(3).collect());

    assert_eq!(groups.len(), 1);
    let first_donors = &groups[0].0;
    assert!(first_donors.is_empty(), "{first_donors:?}");
}

#[test]
fn parts_pending_group_credit_excludes_parts_already_credited_live() {
    let mut already_credited = HashSet::new();
    already_credited.insert(PartId::from_raw(3));
    already_credited.insert(PartId::from_raw(7));
    assert_eq!(
        parts_pending_group_credit(5, &already_credited),
        3,
        "5 parts minus the 2 already credited live leaves 3 for the group-level fallback"
    );
}

#[test]
fn parts_pending_group_credit_is_the_full_group_when_nothing_landed_live() {
    assert_eq!(
        parts_pending_group_credit(4, &HashSet::new()),
        4,
        "a protocol-3 donor never fires a live BucketDone, so every part in the group is \
         credited at group completion instead"
    );
}

#[test]
fn plan_view_change_pulls_a_part_regained_after_a_loss_its_last_completed_pull_never_saw() {
    let self_node = NodeId::from(1);
    let k = 2;
    // Pulled under two nodes (self owns everything), then a third node
    // joined and took some parts while that change's pull was
    // superseded, then it left again and self regained them.
    let pulled = view(self_node, (1..=2u64).map(NodeId::from).collect(), k);
    let prev = view(self_node, (1..=3u64).map(NodeId::from).collect(), k);
    let new = view(self_node, (1..=2u64).map(NodeId::from).collect(), k);
    let regained: Vec<PartId> = new.owned_parts().filter(|&p| !prev.owns(p)).collect();
    assert!(!regained.is_empty(), "the fixture regains parts");

    let plan = plan_view_change(&prev, &pulled, &new, &[], &PartSet::new());
    let to_pull: HashSet<PartId> = plan.to_pull.iter().copied().collect();
    for part in &regained {
        assert!(
            to_pull.contains(part),
            "part {part} missed writes while node 3 owned it, so it is pulled again"
        );
    }
    assert_eq!(
        to_pull.len(),
        plan.to_pull.len(),
        "each part is planned once"
    );
}

#[test]
fn sole_owned_keeps_a_part_alone_only_when_no_absent_member_would_co_own_it() {
    let me = NodeId::from(1);
    let absent = NodeId::from(2);
    let one = NonZeroU8::new(1).expect("nonzero");
    let two = NonZeroU8::new(2).expect("nonzero");
    let alone = OwnershipView::compute_at(me, vec![me], one, Granularity::Part);
    let all = || alone.owned_parts();

    assert_eq!(
        sole_owned(&alone, all(), &[]),
        all().collect::<Vec<_>>(),
        "with nobody absent, every part owned alone serves"
    );

    // One owner per part: once `absent` is eligible again, each part
    // goes to exactly one of the two, and only this node's stay.
    let widened = OwnershipView::compute_at(me, vec![me, absent], one, Granularity::Part);
    let kept = sole_owned(&alone, all(), &[absent]);
    assert_eq!(kept, widened.owned_parts().collect::<Vec<_>>());
    assert!(!kept.is_empty() && kept.len() < alone.owned_part_count());

    // Two owners per part: `absent` would co-own every one of them.
    let alone_two = OwnershipView::compute_at(me, vec![me], two, Granularity::Part);
    assert!(
        sole_owned(&alone_two, alone_two.owned_parts(), &[absent]).is_empty(),
        "a part an absent member would co-own stays cold"
    );
    assert!(
        sole_owned(&alone_two, alone_two.owned_parts(), &[me]).len()
            == alone_two.owned_part_count(),
        "this node itself is never absent from its own view"
    );
}

#[test]
fn plan_view_change_marks_a_pulled_part_stale_only_where_a_copy_is_already_held() {
    let self_node = NodeId::from(1);
    let k = 2;
    // Two nodes (self owns everything), a third joins and takes some
    // parts, then leaves: self owns them again, and still holds its
    // copy of one of them from before.
    let two = view(self_node, (1..=2u64).map(NodeId::from).collect(), k);
    let three = view(self_node, (1..=3u64).map(NodeId::from).collect(), k);
    let regained: Vec<PartId> = two.owned_parts().filter(|&p| !three.owns(p)).collect();
    assert!(regained.len() >= 2, "the fixture regains parts");
    let kept = regained[0];
    let steady = two
        .owned_parts()
        .find(|&p| three.owns(p))
        .expect("some part stays owned throughout");

    let plan = plan_view_change(&three, &three, &two, &[kept, steady], &PartSet::new());

    assert!(plan.to_pull.contains(&kept) && plan.to_pull.contains(&regained[1]));
    assert_eq!(
        plan.stale,
        vec![kept],
        "only the regained part with a copy already here can hold a record another \
         owner overwrote or deleted meanwhile; the steady part is not pulled at all"
    );

    let residency = ResidencySet::new();
    mark_pulling(&residency, &plan);
    assert!(residency.is_cold(kept) && residency.is_unverified(kept));
    assert!(residency.is_cold(regained[1]) && !residency.is_unverified(regained[1]));
}

#[test]
fn plan_view_change_pulls_a_part_a_view_it_never_saw_dropped() {
    let self_node = NodeId::from(1);
    let k = 2;
    // Two nodes, then a third joined and left again before this task
    // looked: it sees the same view twice, but the tracker recorded the
    // parts the intermediate view dropped.
    let two = view(self_node, (1..=2u64).map(NodeId::from).collect(), k);
    let three = view(self_node, (1..=3u64).map(NodeId::from).collect(), k);
    let dropped: PartSet = two.owned_parts().filter(|&p| !three.owns(p)).collect();
    assert!(!dropped.is_empty(), "the fixture drops parts");

    let blind = plan_view_change(&two, &two, &two, &[], &PartSet::new());
    assert!(blind.to_pull.is_empty(), "the views alone show no change");
    let plan = plan_view_change(&two, &two, &two, &[], &dropped);
    let to_pull: PartSet = plan.to_pull.iter().copied().collect();
    assert_eq!(to_pull.len(), dropped.len());
    assert!(dropped.iter().all(|part| to_pull.contains(part)));
}

#[test]
fn plan_view_change_marks_a_bucket_lost_since_the_previous_view_even_when_its_pull_was_superseded()
{
    let self_node = NodeId::from(1);
    let k = 2;
    // v0: three nodes. v1: node 3 died, so self gained its share. v2:
    // node 4 joined, taking some of those newly gained buckets away.
    let v0 = view(self_node, (1..=3u64).map(NodeId::from).collect(), k);
    let v1 = view(self_node, (1..=2u64).map(NodeId::from).collect(), k);
    let v2 = view(self_node, [1u64, 2, 4].map(NodeId::from).to_vec(), k);
    let owned = |v: &OwnershipView| v.owned_parts().collect::<HashSet<PartId>>();
    let (o0, o1, o2) = (owned(&v0), owned(&v1), owned(&v2));

    // v0 -> v1 pulled cleanly: prev and pulled agree.
    let plan = plan_view_change(&v0, &v0, &v1, &[], &PartSet::new());
    assert_eq!(
        plan.to_pull.iter().copied().collect::<HashSet<_>>(),
        plan.regained.iter().copied().collect::<HashSet<_>>()
    );
    for &b in &plan.lost {
        assert!(o0.contains(&b) && !o1.contains(&b));
    }

    // v1's pull gets superseded by v2: lost is measured from v1, so a
    // bucket gained under v1 and gone in v2 starts its grace, while
    // the pull covers everything v2 owns that v0 did not.
    let plan = plan_view_change(&v1, &v0, &v2, &[], &PartSet::new());
    let gained_then_lost: Vec<PartId> = o1
        .iter()
        .copied()
        .filter(|b| !o0.contains(b) && !o2.contains(b))
        .collect();
    assert!(
        !gained_then_lost.is_empty(),
        "the fixture has a bucket gained under v1 and lost in v2"
    );
    for b in &gained_then_lost {
        assert!(plan.lost.contains(b), "bucket {b} starts its disown grace");
    }
    for &b in &plan.to_pull {
        assert!(o2.contains(&b) && !o0.contains(&b));
    }
    for b in o2.iter().filter(|b| !o0.contains(b)) {
        assert!(plan.to_pull.contains(b), "bucket {b} is pulled under v2");
    }
}

#[tokio::test]
async fn rebalance_task_schedules_release_for_a_bucket_only_the_baseline_view_ever_owned() {
    let cluster = solo_cluster("rebalance-unit-test-baseline-release").await;
    let name = SmolStr::new("prices");
    let k = NonZeroU8::new(2).expect("nonzero");
    let (tracker, tx) = OwnershipTracker::seed(
        cluster.node_id(),
        &cluster.peers(),
        &cluster.advertised_cache_modes(),
        &name,
        k,
    );
    // The seeded, lone-node view: the transient "owns everything" state.
    let baseline = tracker.baseline();
    assert_eq!(baseline.owned_part_count(), crate::store::part::PART_SPACE);

    // Published before rebalance_task is spawned: as if refresh_task
    // had already raced ahead, the very race baseline() exists to
    // make irrelevant.
    let other_a = NodeId::from(u64::MAX);
    let other_b = NodeId::from(u64::MAX - 1);
    let corrected = Arc::new(view(
        cluster.node_id(),
        vec![cluster.node_id(), other_a, other_b],
        2,
    ));
    let bucket_only_in_baseline = baseline
        .owned_parts()
        .find(|&b| !corrected.owns(b))
        .expect("a lone-node view owns strictly more than a three-node, k=2 split");
    tx.send(Arc::clone(&corrected))
        .expect("the tracker's own receiver keeps the channel open");

    let residency = Arc::new(ResidencySet::new());
    let cancel = CancellationToken::new();
    let _task = tokio::spawn(rebalance_task(
        cluster.clone(),
        empty_shard(),
        tracker,
        Arc::clone(&residency),
        name.clone(),
        Duration::from_secs(3600),
        4,
        cancel.clone(),
    ));

    // Gives rebalance_task time to subscribe before this test publishes.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // A raw resend, a genuine new channel version, so view_rx.changed()
    // fires without needing a third distinct membership change.
    tx.send(Arc::clone(&corrected))
        .expect("the tracker's own receiver keeps the channel open");

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        residency.is_releasing(bucket_only_in_baseline),
        "a bucket only the tracker's seeded baseline view ever owned, already gone by the \
         very first published correction, must still start its disown grace: this is \
         `rebalance_task`'s normal lost-bucket path (`plan_view_change`) reacting off \
         `OwnershipTracker::baseline`, not a live re-borrow of the channel a race could \
         already have moved past"
    );

    cancel.cancel();
    cluster.shutdown().await;
}

#[test]
fn plan_view_change_marks_a_held_bucket_lost_when_the_view_that_owned_it_was_never_observed() {
    let self_node = NodeId::from(1);
    let k = 2;
    // v0: three nodes. v1: node 3 died. v2: node 4 joined. The task
    // observed v0 and v2 only: v1 came and went while it was busy.
    let v0 = view(self_node, (1..=3u64).map(NodeId::from).collect(), k);
    let v1 = view(self_node, (1..=2u64).map(NodeId::from).collect(), k);
    let v2 = view(self_node, [1u64, 2, 4].map(NodeId::from).to_vec(), k);
    let owned = |v: &OwnershipView| v.owned_parts().collect::<HashSet<PartId>>();
    let (o0, o1, o2) = (owned(&v0), owned(&v1), owned(&v2));
    let only_in_v1: Vec<PartId> = o1
        .iter()
        .copied()
        .filter(|b| !o0.contains(b) && !o2.contains(b))
        .collect();
    assert!(
        !only_in_v1.is_empty(),
        "the fixture has a bucket owned under v1 alone"
    );
    let kept = *o2
        .iter()
        .find(|b| o0.contains(b))
        .expect("the fixture has a bucket owned throughout");

    // Without the held fold, the unobserved view leaves no trace.
    let blind = plan_view_change(&v0, &v0, &v2, &[], &PartSet::new());
    for b in &only_in_v1 {
        assert!(
            !blind.lost.contains(b),
            "bucket {b} is in neither prev nor new, so the plain diff misses it"
        );
    }

    // The inbound guard applied entries to those buckets under v1:
    // the shard holds them, and the fold releases every one.
    let mut held = only_in_v1.clone();
    held.push(kept);
    let plan = plan_view_change(&v0, &v0, &v2, &held, &PartSet::new());
    for b in &only_in_v1 {
        assert!(
            plan.lost.contains(b),
            "held bucket {b} starts its disown grace"
        );
    }
    assert!(
        !plan.lost.contains(&kept),
        "a held bucket the new view owns is not lost"
    );
    let distinct: HashSet<PartId> = plan.lost.iter().copied().collect();
    assert_eq!(
        distinct.len(),
        plan.lost.len(),
        "lost lists each bucket once"
    );
    for b in o0.iter().filter(|b| !o2.contains(b)) {
        assert!(
            plan.lost.contains(b),
            "the plain prev-to-new loss {b} stays"
        );
    }
}

#[tokio::test]
async fn rebalance_task_releases_a_bucket_held_under_a_view_it_never_observed() {
    let cluster = solo_cluster("rebalance-unit-test-held-release").await;
    let name = SmolStr::new("prices");
    let k = NonZeroU8::new(2).expect("nonzero");
    let (tracker, tx) = OwnershipTracker::seed(
        cluster.node_id(),
        &cluster.peers(),
        &cluster.advertised_cache_modes(),
        &name,
        k,
    );
    let other_a = NodeId::from(u64::MAX);
    let other_b = NodeId::from(u64::MAX - 1);
    let settled = Arc::new(view(
        cluster.node_id(),
        vec![cluster.node_id(), other_a, other_b],
        2,
    ));
    let bucket = PartId::all()
        .find(|&p| !settled.owns(p))
        .expect("a three-node, k=2 split leaves some bucket unowned here");
    let shard = empty_shard();
    let residency = Arc::new(ResidencySet::new());
    let cancel = CancellationToken::new();
    let _task = tokio::spawn(rebalance_task(
        cluster.clone(),
        Arc::clone(&shard),
        tracker,
        Arc::clone(&residency),
        name.clone(),
        Duration::from_secs(3600),
        4,
        cancel.clone(),
    ));
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The lone-node baseline owned everything, so the first observed
    // view loses `bucket` through the plain diff; its release then
    // completes, modelled by the same unmark the release path calls.
    tx.send(Arc::clone(&settled))
        .expect("the tracker's own receiver keeps the channel open");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(residency.is_releasing(bucket), "lost from the baseline");
    residency.unmark(&[bucket]);

    // A change that moves nothing, with nothing held: no grace starts.
    tx.send(Arc::clone(&settled))
        .expect("the tracker's own receiver keeps the channel open");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !residency.is_releasing(bucket),
        "an empty unowned bucket has nothing to release"
    );

    // An entry lands in the bucket, as the inbound guard applies one
    // under a view that owned it and was superseded before this task
    // observed it. The next observed change, whose prev and new both
    // disown the bucket, still starts its grace.
    let key = (0..1_000_000u32)
        .find(|key| PartId::of_key(&postcard::to_stdvec(key).expect("encodes")) == bucket)
        .expect("some key hashes into the bucket");
    shard
        .apply_remote_batch(vec![crate::wire::WireRecord {
            key: bytes::Bytes::from(postcard::to_stdvec(&key).expect("encodes")),
            value: Some(bytes::Bytes::from(
                postcard::to_stdvec(&7u32).expect("encodes"),
            )),
            ver: crate::hlc::Hlc {
                wall_ms: 1,
                logical: 0,
                node: NodeId::from(9),
            },
            expires_at_ms: None,
        }])
        .await;
    assert_eq!(shard.held_parts().await, vec![bucket]);
    tx.send(Arc::clone(&settled))
        .expect("the tracker's own receiver keeps the channel open");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        residency.is_releasing(bucket),
        "a bucket the shard holds without owning starts its disown grace on the next \
         observed view change, whichever views the task saw in between"
    );

    cancel.cancel();
    cluster.shutdown().await;
}

/// Every part of a two-node, two-owner view is owned here with a co-owner.
fn co_owned_view() -> OwnershipView {
    view(NodeId::from(1), vec![NodeId::from(1), NodeId::from(2)], 2)
}

fn unfinished_of(parts: &[PartId]) -> PartSet {
    parts.iter().copied().collect()
}

#[test]
fn retry_parts_covers_only_cold_parts_the_view_owns_with_a_co_owner() {
    let view = co_owned_view();
    let parts: Vec<PartId> = PartId::all().take(6).collect();
    let cold: HashSet<PartId> = parts[..4].iter().copied().collect();

    let retry = retry_parts(
        &unfinished_of(&parts),
        &view,
        Duration::from_secs(20),
        |part| cold.contains(&part),
    );

    assert_eq!(
        retry,
        parts[..4].to_vec(),
        "the parts that landed are not retried"
    );
}

#[test]
fn retry_parts_leaves_a_part_owned_alone_to_sole_owned() {
    let alone = view(NodeId::from(1), vec![NodeId::from(1)], 2);
    let parts: Vec<PartId> = PartId::all().take(3).collect();
    assert!(
        parts.iter().all(|&part| alone.owners_of(part).len() == 1),
        "a lone node owns each part alone"
    );

    assert_eq!(
        retry_parts(
            &unfinished_of(&parts),
            &alone,
            Duration::from_secs(20),
            |_| true
        ),
        Vec::<PartId>::new()
    );
}

#[test]
fn retry_parts_leaves_a_part_the_view_no_longer_owns_to_the_next_view_change() {
    let view = view(NodeId::from(1), (1..=9u64).map(NodeId::from).collect(), 2);
    let unowned: Vec<PartId> = PartId::all()
        .filter(|&part| !view.owns(part))
        .take(4)
        .collect();
    let owned: Vec<PartId> = PartId::all()
        .filter(|&part| view.owns(part))
        .take(4)
        .collect();
    let all: Vec<PartId> = unowned.iter().chain(&owned).copied().collect();

    assert_eq!(
        retry_parts(&unfinished_of(&all), &view, Duration::from_secs(20), |_| {
            true
        }),
        owned
    );
}

#[test]
fn retry_parts_is_empty_for_nothing_unfinished_and_for_a_zero_budget() {
    let view = co_owned_view();
    assert_eq!(
        retry_parts(&PartSet::new(), &view, Duration::from_secs(20), |_| true),
        Vec::<PartId>::new()
    );
    let parts: Vec<PartId> = PartId::all().take(3).collect();
    assert_eq!(
        retry_parts(&unfinished_of(&parts), &view, Duration::ZERO, |_| true),
        Vec::<PartId>::new(),
        "a zero budget runs no pull, so there is nothing to retry"
    );
}

#[test]
fn retry_parts_lists_parts_in_ascending_order() {
    let view = co_owned_view();
    let mut parts: Vec<PartId> = PartId::all().step_by(131).take(20).collect();
    parts.reverse();

    let retry = retry_parts(
        &unfinished_of(&parts),
        &view,
        Duration::from_secs(20),
        |_| true,
    );

    let mut sorted = parts.clone();
    sorted.sort_unstable();
    assert_eq!(retry, sorted);
}

#[test]
fn still_cold_drops_the_parts_that_landed() {
    let parts: Vec<PartId> = PartId::all().take(5).collect();
    let landed: HashSet<PartId> = parts[1..3].iter().copied().collect();

    let kept = still_cold(parts.iter().copied(), |part| !landed.contains(&part));

    assert_eq!(
        kept.iter().collect::<Vec<_>>(),
        vec![parts[0], parts[3], parts[4]]
    );
    assert!(still_cold(PartSet::new().iter(), |_| true).is_empty());
}

#[test]
fn a_retry_counts_a_timeout_and_restarts_the_tick_only_for_a_pull_that_ran() {
    let effects = |end| {
        let effects = retry_effects(end);
        (effects.count_timeout, effects.reset_ticker)
    };
    assert_eq!(effects(RetryEnd::Ran(Outcome::TimedOut)), (true, true));
    for outcome in [
        Outcome::Completed,
        Outcome::Superseded,
        Outcome::Unverified,
        Outcome::NoDonor,
    ] {
        assert_eq!(
            effects(RetryEnd::Ran(outcome)),
            (false, true),
            "{outcome:?} is no timeout, and the pull used its time"
        );
    }
    assert_eq!(
        effects(RetryEnd::ViewChanged),
        (false, false),
        "a view change cuts the retry short; the next turn handles it"
    );
    assert_eq!(effects(RetryEnd::Cancelled), (false, false));
}

/// A peer node on its own runtime and its own thread, so a test can freeze
/// the whole node, as a SIGSTOP would, and thaw it later.
struct FreezablePeer {
    node: NodeId,
    commands: tokio::sync::mpsc::UnboundedSender<PeerCmd>,
    thread: std::thread::JoinHandle<()>,
}

enum PeerCmd {
    Freeze(std::sync::mpsc::Receiver<()>),
    Stop,
}

impl FreezablePeer {
    /// Builds a member of cluster `name` seeded from `seed`, and waits until
    /// `observer` sees it live.
    async fn join(
        name: &'static str,
        seed: std::net::SocketAddr,
        config: crate::config::ClusterConfig,
        observer: &crate::cluster::Cluster,
    ) -> Self {
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<NodeId>();
        let (commands, mut cmd_rx) = tokio::sync::mpsc::unbounded_channel::<PeerCmd>();
        let thread = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            rt.block_on(async move {
                let peer = crate::cluster::Cluster::builder(name)
                    .seeds([seed])
                    .config(config)
                    .build()
                    .await
                    .expect("the peer builds");
                ready_tx.send(peer.node_id()).expect("test alive");
                while let Some(cmd) = cmd_rx.recv().await {
                    match cmd {
                        // Blocks this node's only runtime thread.
                        PeerCmd::Freeze(thaw) => drop(thaw.recv()),
                        PeerCmd::Stop => break,
                    }
                }
                peer.shutdown().await;
            });
        });
        let node = ready_rx.await.expect("the peer is up");
        tokio::time::timeout(Duration::from_secs(15), async {
            while !observer.peers().iter().any(|peer| peer.node == node) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the peer is live in gossip");
        Self {
            node,
            commands,
            thread,
        }
    }

    /// Freezes the peer until the returned sender sends or drops.
    fn freeze(&self) -> std::sync::mpsc::Sender<()> {
        let (thaw_tx, thaw_rx) = std::sync::mpsc::channel::<()>();
        self.commands
            .send(PeerCmd::Freeze(thaw_rx))
            .expect("the peer is alive");
        thaw_tx
    }

    fn stop(self) {
        self.commands
            .send(PeerCmd::Stop)
            .expect("the peer is alive");
        self.thread.join().expect("the peer's thread ends");
    }
}

/// Stores one entry in `part` on `shard`, so a wipe of the part shows.
async fn hold_an_entry_in(shard: &Arc<dyn ShardOps>, part: PartId) {
    let key = (0..1_000_000u32)
        .find(|key| PartId::of_key(&postcard::to_stdvec(key).expect("encodes")) == part)
        .expect("some key hashes into the part");
    shard
        .apply_remote_batch(vec![crate::wire::WireRecord {
            key: bytes::Bytes::from(postcard::to_stdvec(&key).expect("encodes")),
            value: Some(bytes::Bytes::from(
                postcard::to_stdvec(&7u32).expect("encodes"),
            )),
            ver: crate::hlc::Hlc {
                wall_ms: 1,
                logical: 0,
                node: NodeId::from(9),
            },
            expires_at_ms: None,
        }])
        .await;
    assert_eq!(shard.held_parts().await, vec![part]);
}

/// Polls `cond` every 10 ms until it holds, failing the test after 15 s.
async fn poll_until(what: &str, mut cond: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(15), async {
        while !cond() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what} within 15 s"));
}

/// A part A lost to B comes due while B is frozen, so the tick's hand-off
/// round to B stalls; a view published meanwhile gives the part back to A
/// alone. Once B thaws and the round ends, the release planned under the
/// earlier view must not delete A's copy.
#[tokio::test]
async fn a_part_a_view_returns_during_the_hand_off_rounds_is_not_released() {
    const CLUSTER: &str = "regained-during-hand-off";
    let config = crate::config::ClusterConfig {
        gossip_bind_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
        data_bind_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
        ae_interval: Duration::from_millis(50),
        ..crate::config::ClusterConfig::default()
    };
    let cluster = crate::cluster::Cluster::builder(CLUSTER)
        .seeds(std::iter::empty())
        .config(config.clone())
        .build()
        .await
        .expect("a builds");
    let b = FreezablePeer::join(CLUSTER, cluster.local_gossip_addr(), config, &cluster).await;

    let name = SmolStr::new("prices");
    let k = NonZeroU8::new(2).expect("nonzero");
    let (tracker, tx) = OwnershipTracker::seed(
        cluster.node_id(),
        &cluster.peers(),
        &cluster.advertised_cache_modes(),
        &name,
        k,
    );
    let shard = empty_shard();
    let residency = Arc::new(ResidencySet::new());
    let cancel = CancellationToken::new();
    let task = tokio::spawn(rebalance_task(
        cluster.clone(),
        Arc::clone(&shard),
        tracker,
        Arc::clone(&residency),
        name.clone(),
        Duration::from_millis(400),
        4,
        cancel.clone(),
    ));
    tokio::time::sleep(Duration::from_millis(100)).await;

    // B stops answering; A then loses part X to it.
    let thaw = b.freeze();
    let v1 = Arc::new(view(cluster.node_id(), vec![cluster.node_id(), b.node], 1));
    let part = PartId::all()
        .find(|&p| !v1.owns(p))
        .expect("a two-node, k=1 split leaves parts unowned here");
    tx.send(Arc::clone(&v1)).expect("tracker keeps the channel");
    poll_until("A marks the part releasing", || {
        residency.is_releasing(part)
    })
    .await;
    hold_an_entry_in(&shard, part).await;

    // The grace passes and the tick's hand-off round to the frozen B
    // starts; meanwhile A regains X as its sole owner.
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(
        shard.held_parts().await,
        vec![part],
        "not released before V2"
    );
    let v2 = Arc::new(view(cluster.node_id(), vec![cluster.node_id()], 2));
    assert!(v2.owns(part));
    tx.send(v2).expect("tracker keeps the channel");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(shard.held_parts().await, vec![part], "round still stalled");
    thaw.send(()).expect("b is frozen");
    // The view arm unmarks the regained part only once the tick that held
    // it returns, whichever way that tick decided.
    poll_until("the tick returns and V2 is planned", || {
        !residency.is_releasing(part)
    })
    .await;

    let held = shard.held_parts().await;
    cancel.cancel();
    let _ = task.await;
    b.stop();
    cluster.shutdown().await;
    assert_eq!(
        held,
        vec![part],
        "A owns X alone again; a release planned under the earlier view must not wipe it"
    );
}

/// A part this node lost is held past the hand-off cutoff, a tombstone
/// retention less one interval, while its new owner never answers: the tick
/// drops it without a round, long before its disown grace would.
#[tokio::test]
async fn a_lost_part_held_past_the_hand_off_cutoff_is_released_without_a_round() {
    let config = crate::config::ClusterConfig {
        gossip_bind_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
        data_bind_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
        ae_interval: Duration::from_millis(50),
        tombstone_ttl: Duration::from_millis(150),
        ..crate::config::ClusterConfig::default()
    };
    let cutoff = hand_off_cutoff(config.tombstone_ttl, config.ae_interval);
    let grace = Duration::from_secs(10);
    assert!(
        cutoff < grace,
        "only the cutoff can release within the test"
    );
    let cluster = crate::cluster::Cluster::builder("released-past-the-cutoff")
        .seeds(std::iter::empty())
        .config(config)
        .build()
        .await
        .expect("a builds");

    let name = SmolStr::new("prices");
    let k = NonZeroU8::new(1).expect("nonzero");
    let (tracker, tx) = OwnershipTracker::seed(
        cluster.node_id(),
        &cluster.peers(),
        &cluster.advertised_cache_modes(),
        &name,
        k,
    );
    let shard = empty_shard();
    let residency = Arc::new(ResidencySet::new());
    let cancel = CancellationToken::new();
    let task = tokio::spawn(rebalance_task(
        cluster.clone(),
        Arc::clone(&shard),
        tracker,
        Arc::clone(&residency),
        name.clone(),
        grace,
        4,
        cancel.clone(),
    ));

    // A node gossip never saw takes part of the space: it answers nothing.
    let ghost = NodeId::from(0xD0D0);
    let lost = Arc::new(view(cluster.node_id(), vec![cluster.node_id(), ghost], 1));
    let part = PartId::all()
        .find(|&p| !lost.owns(p))
        .expect("a two-node, k=1 split gives the ghost parts");
    // The entry lands while this node still owns everything, so the release
    // cannot run before it.
    hold_an_entry_in(&shard, part).await;
    tx.send(lost).expect("tracker keeps the channel");

    // With the owner unreachable, a hand-off release waits for twice the
    // grace; the cutoff comes far sooner.
    let dropped = tokio::time::timeout(Duration::from_secs(5), async {
        while !shard.held_parts().await.is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok();
    let releasing = residency.is_releasing(part);
    cancel.cancel();
    let _ = task.await;
    cluster.shutdown().await;
    assert!(
        dropped,
        "the copy past the cutoff is dropped without a round"
    );
    assert!(!releasing, "and the part leaves residency");
}

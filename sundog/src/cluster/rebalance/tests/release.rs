//! Tests for what a node pushes, releases and confirms when a view change takes parts away.

use super::*;

#[test]
fn unpullable_losses_names_every_bucket_a_lone_owner_loses() {
    let self_node = NodeId::from(1);
    let alone = view(self_node, vec![self_node], 2);
    let three = view(self_node, (1..=3u64).map(NodeId::from).collect(), 2);
    let (_, lost) = ownership_diff(&alone, &three);
    assert!(!lost.is_empty(), "a three-node split takes buckets away");
    assert_eq!(
        unpullable_losses(&alone, &three, self_node, &lost),
        lost,
        "no other node owned any bucket before, so none can hand one over"
    );
}

#[test]
fn unpullable_losses_leaves_a_bucket_with_a_surviving_co_owner_to_the_pull() {
    let self_node = NodeId::from(1);
    let before = view(self_node, (1..=3u64).map(NodeId::from).collect(), 2);
    let after = view(self_node, (1..=4u64).map(NodeId::from).collect(), 2);
    let (_, lost) = ownership_diff(&before, &after);
    assert!(!lost.is_empty(), "the joiner displaces this node somewhere");
    assert!(
        unpullable_losses(&before, &after, self_node, &lost).is_empty(),
        "a joiner only displaces one owner per bucket, so the other still holds it"
    );

    // Both of a bucket's other owners change: nobody left holds it.
    let swapped = view(self_node, [1u64, 5, 6].map(NodeId::from).to_vec(), 2);
    let (_, lost) = ownership_diff(&before, &swapped);
    let orphaned: Vec<PartId> = lost
        .iter()
        .copied()
        .filter(|&bucket| {
            !swapped
                .owners_of(bucket)
                .iter()
                .any(|owner| before.owners_of(bucket).contains(owner))
        })
        .collect();
    assert!(
        !orphaned.is_empty(),
        "the fixture has a bucket with no survivor"
    );
    assert_eq!(
        unpullable_losses(&before, &swapped, self_node, &lost),
        orphaned
    );
}

#[test]
fn push_targets_groups_parts_by_owner_and_skips_only_self() {
    let self_node = NodeId::from(1);
    let eligible: Vec<NodeId> = (1..=4u64).map(NodeId::from).collect();
    let view = view(self_node, eligible.clone(), 2);
    let buckets: Vec<PartId> = PartId::all().take(64).collect();
    let targets = push_targets(&view, self_node, &buckets);
    let owners: Vec<NodeId> = targets.iter().map(|(owner, _)| *owner).collect();
    assert!(!owners.contains(&self_node), "self is never a push target");
    let distinct: HashSet<NodeId> = owners.iter().copied().collect();
    assert_eq!(distinct.len(), owners.len(), "each owner appears once");
    for &bucket in &buckets {
        for owner in view.owners_of(bucket) {
            let listed = targets
                .iter()
                .any(|(node, owned)| node == owner && owned.contains(&bucket));
            assert_eq!(
                listed,
                *owner != self_node,
                "bucket {bucket} goes to each owner other than self, live or not"
            );
        }
    }
    for (owner, owned) in &targets {
        for bucket in owned {
            assert!(view.owners_of(*bucket).contains(owner));
        }
    }
}

#[test]
fn split_reachable_holds_back_each_owner_gossip_has_dropped() {
    let parts = |n: u16| vec![PartId::all().nth(usize::from(n)).expect("in range")];
    let targets = vec![
        (NodeId::from(2), parts(0)),
        (NodeId::from(3), parts(1)),
        (NodeId::from(4), parts(2)),
    ];
    let live: HashSet<NodeId> = [2u64, 4].map(NodeId::from).into_iter().collect();
    let (reachable, waiting) = split_reachable(targets.clone(), &live);
    assert_eq!(reachable, vec![targets[0].clone(), targets[2].clone()]);
    assert_eq!(waiting, vec![targets[1].clone()]);

    let (reachable, waiting) = split_reachable(targets.clone(), &HashSet::new());
    let empty: PushTargets = Vec::new();
    assert_eq!(
        reachable, empty,
        "with every peer dropped, nothing is reachable"
    );
    assert_eq!(
        waiting, targets,
        "and every target waits for the next attempt"
    );
}

#[test]
fn hand_off_owners_lists_each_live_current_owner_once_and_never_self() {
    let self_node = NodeId::from(1);
    let eligible: Vec<NodeId> = (1..=5u64).map(NodeId::from).collect();
    let view = view(self_node, eligible.clone(), 2);
    let due: Vec<PartId> = PartId::all().take(256).collect();
    let live: HashSet<NodeId> = eligible
        .iter()
        .copied()
        .filter(|n| *n != self_node)
        .collect();

    let owners = hand_off_owners(&view, self_node, &due, &live);
    let distinct: HashSet<NodeId> = owners.iter().copied().collect();
    assert_eq!(owners.len(), distinct.len(), "each owner listed once");
    assert!(
        !owners.contains(&self_node),
        "self is never a hand-off target"
    );
    for owner in &owners {
        assert!(
            due.iter().any(|&b| view.owners_of(b).contains(owner)),
            "every listed owner owns a due bucket: {owner:?}"
        );
    }
    for &bucket in &due {
        for owner in view.owners_of(bucket) {
            if *owner != self_node {
                assert!(
                    owners.contains(owner),
                    "every other owner of a due bucket is listed"
                );
            }
        }
    }

    let dead = eligible[1];
    let mut live_minus_one = live.clone();
    live_minus_one.remove(&dead);
    assert!(
        !hand_off_owners(&view, self_node, &due, &live_minus_one).contains(&dead),
        "a dead owner is skipped"
    );
}

#[test]
fn hand_off_cutoff_is_one_interval_inside_the_tombstone_retention() {
    assert_eq!(
        hand_off_cutoff(Duration::from_secs(60), Duration::from_secs(3)),
        Duration::from_secs(57)
    );
    assert_eq!(
        hand_off_cutoff(Duration::from_secs(2), Duration::from_secs(3)),
        Duration::from_secs(3),
        "never shorter than one interval"
    );
}

#[test]
fn out_of_time_gives_up_only_on_a_retry_past_a_representable_deadline() {
    let now = tokio::time::Instant::now();
    let retry = Duration::from_secs(1);
    assert!(!out_of_time(now, retry, None));
    assert!(!out_of_time(now, Duration::MAX, None));
    assert!(!out_of_time(now, retry, Some(now + Duration::from_secs(5))));
    assert!(out_of_time(
        now,
        retry,
        Some(now + Duration::from_millis(500))
    ));
    assert!(out_of_time(
        now,
        Duration::MAX,
        Some(now + Duration::from_secs(5))
    ));
    assert!(now.checked_add(Duration::MAX).is_none());
}

#[test]
fn parts_to_release_holds_a_bucket_until_every_owner_answered_or_it_is_overdue() {
    let self_node = NodeId::from(1);
    let eligible: Vec<NodeId> = (1..=5u64).map(NodeId::from).collect();
    let view = view(self_node, eligible, 2);
    let due: Vec<PartId> = PartId::all().take(256).collect();

    let none: HashSet<NodeId> = HashSet::new();
    let no_acked: HashMap<PartId, HashSet<NodeId>> = HashMap::new();
    let held = parts_to_release(
        &view,
        self_node,
        &due,
        &PartSet::new(),
        &none,
        &no_acked,
        &none,
    );
    for &bucket in &held {
        assert!(
            view.owners_of(bucket).iter().all(|o| *o == self_node),
            "with no owner answered, only a bucket owned by self alone is released: {bucket}"
        );
    }

    let answered: HashSet<NodeId> = [NodeId::from(2)].into_iter().collect();
    let released = parts_to_release(
        &view,
        self_node,
        &due,
        &PartSet::new(),
        &answered,
        &no_acked,
        &none,
    );
    for &bucket in &due {
        let all_answered = view
            .owners_of(bucket)
            .iter()
            .all(|o| *o == self_node || answered.contains(o));
        assert_eq!(
            released.contains(&bucket),
            all_answered,
            "bucket {bucket} is released exactly when every other owner answered"
        );
    }
    assert!(
        released.len() < due.len(),
        "some bucket waits on an owner that did not answer"
    );

    // Overdue: a stale owner still holds the bucket, an unreachable
    // one does not.
    let overdue: PartSet = due.iter().copied().collect();
    let still_held = parts_to_release(&view, self_node, &due, &overdue, &none, &no_acked, &none);
    assert_eq!(
        still_held, held,
        "an overdue bucket whose owners are merely stale stays resident"
    );
    let everyone: HashSet<NodeId> = (2..=5u64).map(NodeId::from).collect();
    let dropped = parts_to_release(
        &view, self_node, &due, &overdue, &none, &no_acked, &everyone,
    );
    assert_eq!(
        dropped, due,
        "an overdue bucket whose owners are all unreachable is released"
    );
    let mixed = parts_to_release(
        &view, self_node, &due, &overdue, &answered, &no_acked, &everyone,
    );
    assert_eq!(
        mixed, due,
        "reconciled and unreachable owners together release"
    );
}

#[test]
fn parts_to_release_accepts_an_acked_owner_without_a_confirming_ae_round() {
    // An acked owner releases like a reconciled one; other current
    // owners must still be covered.
    let self_node = NodeId::from(1);
    let eligible: Vec<NodeId> = (1..=3u64).map(NodeId::from).collect();
    let view = view(self_node, eligible, 2);
    let due: Vec<PartId> = PartId::all().take(256).collect();
    let bucket = due
        .iter()
        .copied()
        .find(|&b| view.owners_of(b).contains(&self_node) && view.owners_of(b).len() == 2)
        .expect("some bucket is co-owned by exactly one other node at k=2");
    let other = *view
        .owners_of(bucket)
        .iter()
        .find(|&&o| o != self_node)
        .expect("a co-owned bucket has another owner");

    let none: HashSet<NodeId> = HashSet::new();
    let no_acked: HashMap<PartId, HashSet<NodeId>> = HashMap::new();
    assert!(
        !parts_to_release(
            &view,
            self_node,
            &[bucket],
            &PartSet::new(),
            &none,
            &no_acked,
            &none
        )
        .contains(&bucket),
        "with no ack and no round, the bucket stays resident"
    );

    let acked: HashMap<PartId, HashSet<NodeId>> = HashMap::from([(bucket, HashSet::from([other]))]);
    assert!(
        parts_to_release(
            &view,
            self_node,
            &[bucket],
            &PartSet::new(),
            &none,
            &acked,
            &none
        )
        .contains(&bucket),
        "an owner who acked this bucket releases it without a confirming AE round against it"
    );
}

#[test]
fn parts_to_release_never_releases_a_co_owned_bucket_whose_owner_only_acked_a_different_bucket() {
    // Regression: two due buckets share an owner who acked only one;
    // the un-acked bucket must stay resident.
    let self_node = NodeId::from(1);
    let eligible: Vec<NodeId> = (1..=3u64).map(NodeId::from).collect();
    let view = view(self_node, eligible, 2);
    let due: Vec<PartId> = PartId::all()
        .take(256)
        .filter(|&b| view.owners_of(b).contains(&self_node) && view.owners_of(b).len() == 2)
        .collect();
    let other = *view
        .owners_of(due[0])
        .iter()
        .find(|&&o| o != self_node)
        .expect("a co-owned bucket has another owner");
    // Two distinct due buckets `other` co-owns alongside self.
    let owner_due: Vec<PartId> = due
        .iter()
        .copied()
        .filter(|&b| view.owners_of(b).contains(&other))
        .take(2)
        .collect();
    assert_eq!(
        owner_due.len(),
        2,
        "`other` co-owns at least two due buckets at this eligible set/k"
    );
    let (bucket_acked, bucket_unacked) = (owner_due[0], owner_due[1]);

    let none: HashSet<NodeId> = HashSet::new();
    // `other` acked `bucket_acked` only.
    let acked: HashMap<PartId, HashSet<NodeId>> =
        HashMap::from([(bucket_acked, HashSet::from([other]))]);
    let released = parts_to_release(
        &view,
        self_node,
        &[bucket_acked, bucket_unacked],
        &PartSet::new(),
        &none,
        &acked,
        &none,
    );
    assert!(
        released.contains(&bucket_acked),
        "the bucket `other` actually acked releases"
    );
    assert!(
        !released.contains(&bucket_unacked),
        "a different due bucket the same owner co-owns, which it never acked, must not \
         release on the strength of an ack for a different bucket"
    );
}

#[test]
fn owners_needing_confirmation_skips_an_already_reconciled_owner() {
    let self_node = NodeId::from(1);
    let eligible: Vec<NodeId> = (1..=3u64).map(NodeId::from).collect();
    let view = view(self_node, eligible, 2);
    let owners = vec![NodeId::from(2), NodeId::from(3)];
    // The full bucket range: every real caller's owners co-own at
    // least one bucket in `due`, so an empty range here would
    // vacuously look reconciled.
    let due: Vec<PartId> = PartId::all().take(256).collect();
    let reconciled: HashSet<NodeId> = [NodeId::from(2)].into_iter().collect();
    let no_acked: HashMap<PartId, HashSet<NodeId>> = HashMap::new();
    assert_eq!(
        owners_needing_confirmation(&view, &owners, &due, &reconciled, &no_acked),
        vec![NodeId::from(3)],
        "an owner already reconciled, e.g. via a round covering every bucket it co-owns, \
         needs no round"
    );
    assert_eq!(
        owners_needing_confirmation(&view, &owners, &due, &HashSet::new(), &no_acked),
        owners,
        "with nobody reconciled yet and no acks, every owner still needs a round"
    );
}

#[test]
fn owners_needing_confirmation_still_runs_a_round_for_an_owner_who_only_partly_acked() {
    // Same partial-ack scenario from the confirming-round side: an
    // owner who acked only one of several due buckets still needs a round.
    let self_node = NodeId::from(1);
    let eligible: Vec<NodeId> = (1..=3u64).map(NodeId::from).collect();
    let view = view(self_node, eligible, 2);
    let due: Vec<PartId> = PartId::all()
        .take(256)
        .filter(|&b| view.owners_of(b).contains(&self_node) && view.owners_of(b).len() == 2)
        .collect();
    let other = *view
        .owners_of(due[0])
        .iter()
        .find(|&&o| o != self_node)
        .expect("a co-owned bucket has another owner");
    let owner_due: Vec<PartId> = due
        .iter()
        .copied()
        .filter(|&b| view.owners_of(b).contains(&other))
        .collect();
    assert!(
        owner_due.len() >= 2,
        "`other` co-owns at least two due buckets at this eligible set/k"
    );

    let reconciled: HashSet<NodeId> = HashSet::new();
    // Acked only the first of the buckets `other` co-owns.
    let acked: HashMap<PartId, HashSet<NodeId>> =
        HashMap::from([(owner_due[0], HashSet::from([other]))]);
    assert_eq!(
        owners_needing_confirmation(&view, &[other], &due, &reconciled, &acked),
        vec![other],
        "a partial ack never lets an owner skip its confirming round"
    );

    // Acking every due bucket `other` co-owns does let it skip.
    let full_acked: HashMap<PartId, HashSet<NodeId>> = owner_due
        .iter()
        .map(|&b| (b, HashSet::from([other])))
        .collect();
    assert!(
        owners_needing_confirmation(&view, &[other], &due, &reconciled, &full_acked).is_empty(),
        "an owner who acked every due bucket it co-owns needs no confirming round"
    );
}

#[test]
fn acked_by_part_rekeys_each_id_by_the_parts_it_names() {
    let a = NodeId::from(1);
    let b = NodeId::from(2);
    let by_bucket = acked_by_part(
        Granularity::Bucket,
        HashMap::from([(3u16, HashSet::from([a]))]),
    );
    assert_eq!(by_bucket.len(), crate::store::PART_COUNT);
    assert!(
        by_bucket
            .iter()
            .all(|(part, owners)| part.bucket() == 3 && owners == &HashSet::from([a]))
    );

    let part = PartId::new(3, 5);
    let by_part = acked_by_part(
        Granularity::Part,
        HashMap::from([(part.raw(), HashSet::from([a, b]))]),
    );
    assert_eq!(by_part, HashMap::from([(part, HashSet::from([a, b]))]));
}

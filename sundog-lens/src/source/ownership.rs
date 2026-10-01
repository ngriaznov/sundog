//! The ownership worker: computes each `Distributed` cache's ownership off the
//! async workers and publishes [`Update::Ownership`], and
//! [`Update::OwnershipGone`] once a cache has none.
//!
//! Ranking a cache costs about 65,536 hashes per eligible node, so the worker
//! computes it on a blocking thread, only when the members that decide the
//! ownership change, and only for the newest snapshot. The worker is the one
//! authority on the ownership the model holds: it sends a digest when a view
//! changes and a retraction when the cache has none.

use std::collections::BTreeMap;
use std::num::NonZeroU8;
use std::sync::Arc;

use smol_str::SmolStr;
use sundog::NodeId;
use sundog::observe::{ClusterSnapshot, Member, MemberStatus};
use sundog::store::Mode;
use tokio::sync::{mpsc, watch};

use super::Update;
use crate::model::ownership::OwnershipDigest;

/// One member's part in a cache's ownership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Voter {
    /// The member's node id.
    pub node: NodeId,
    /// The member's incarnation.
    pub incarnation: u64,
    /// The member's lifecycle status.
    pub status: MemberStatus,
    /// The member's wire protocol.
    pub protocol: u16,
    /// The mode the member advertises for the cache.
    pub mode: Option<Mode>,
}

/// Everything that decides one cache's ownership: two snapshots with equal
/// keys rank the cache the same way. The key lists the members that are live or
/// departing and either advertise the cache or share a node id with another
/// such member, because only those records can change who is eligible or at
/// what granularity the cache is ranked. A member that does not advertise the
/// cache and holds its node id alone is not in the key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoKey {
    /// The cache name.
    pub cache: SmolStr,
    /// Owners per part.
    pub k: NonZeroU8,
    /// The members, in snapshot order.
    pub voters: Vec<Voter>,
}

/// The owner count to rank `cache` with: the count most live members
/// advertise, the lower on a tie. `None` when no live member advertises the
/// cache as `Distributed`.
fn leading_k(snapshot: &ClusterSnapshot, cache: &str) -> Option<NonZeroU8> {
    let mut votes: BTreeMap<NonZeroU8, usize> = BTreeMap::new();
    for member in &snapshot.members {
        if member.status != MemberStatus::Live {
            continue;
        }
        if let Some(Mode::Distributed { owners }) = member.caches.get(cache) {
            *votes.entry(*owners).or_default() += 1;
        }
    }
    votes
        .into_iter()
        .max_by_key(|&(owners, count)| (count, std::cmp::Reverse(owners)))
        .map(|(owners, _)| owners)
}

/// Whether another live or departing member holds `member`'s node id under a
/// different incarnation. The ownership reads such a node's caches from the
/// later record and its protocol from the earlier, so each decides the result
/// whether or not it advertises a given cache.
fn shares_node(snapshot: &ClusterSnapshot, member: &Member) -> bool {
    snapshot.members.iter().any(|other| {
        other.status.is_live()
            && other.peer.node == member.peer.node
            && other.peer.incarnation != member.peer.incarnation
    })
}

/// The memo key of every `Distributed` cache a live member advertises,
/// ascending by cache name. Where live members disagree on the owner count,
/// the cache is ranked with the count most of them advertise.
#[must_use]
pub fn targets(snapshot: &ClusterSnapshot) -> Vec<MemoKey> {
    let mut caches: Vec<&SmolStr> = snapshot
        .members
        .iter()
        .filter(|member| member.status == MemberStatus::Live)
        .flat_map(|member| &member.caches)
        .filter(|(_, mode)| matches!(mode, Mode::Distributed { .. }))
        .map(|(cache, _)| cache)
        .collect();
    caches.sort_unstable();
    caches.dedup();
    caches
        .into_iter()
        .filter_map(|cache| {
            let k = leading_k(snapshot, cache)?;
            let voters = snapshot
                .members
                .iter()
                .filter(|member| member.status.is_live())
                .filter(|member| member.caches.contains_key(cache) || shares_node(snapshot, member))
                .map(|member| Voter {
                    node: member.peer.node,
                    incarnation: member.peer.incarnation,
                    status: member.status,
                    protocol: member.peer.protocol,
                    mode: member.caches.get(cache).copied(),
                })
                .collect();
            Some(MemoKey {
                cache: cache.clone(),
                k,
                voters,
            })
        })
        .collect()
}

/// The keys in `wanted` whose cache `held` has not ranked under that same
/// key: the caches to compute.
#[must_use]
pub fn to_compute<'a>(
    held: &BTreeMap<SmolStr, MemoKey>,
    wanted: &'a [MemoKey],
) -> Vec<&'a MemoKey> {
    wanted
        .iter()
        .filter(|key| held.get(&key.cache) != Some(*key))
        .collect()
}

/// What the worker has done for one cache.
struct Done {
    key: MemoKey,
    /// The digest the model holds for the cache: `Some` once the worker has
    /// sent one and until it retracts it.
    digest: Option<OwnershipDigest>,
}

/// Ranks the cache of `key` in `snapshot`, comparing with `previous`.
///
/// `None` when no member is eligible.
fn compute(
    snapshot: &ClusterSnapshot,
    key: &MemoKey,
    previous: Option<&OwnershipDigest>,
) -> Option<OwnershipDigest> {
    let shares = snapshot.ownership(&key.cache, key.k)?;
    Some(OwnershipDigest::from_shares(Arc::new(shares), previous))
}

/// What the worker tells the model about a cache after a recompute.
#[derive(Debug, Clone)]
enum Step {
    /// Send this digest: the view is the first or a new one.
    Publish(OwnershipDigest),
    /// Send a retraction: the model holds a digest and no member is eligible.
    Withdraw,
    /// Send nothing: the model already shows this view, or shows none.
    Skip,
}

/// The step to take and the digest to keep after ranking a cache whose model
/// holds `previous` and whose new ranking is `computed`.
///
/// A ranking whose view hash equals the held one sends nothing and keeps the
/// held digest, so its `moved` and `previous_view` stay those of the change
/// that produced it. A ranking with no eligible member retracts a held digest.
fn outcome(
    previous: Option<OwnershipDigest>,
    computed: Option<OwnershipDigest>,
) -> (Step, Option<OwnershipDigest>) {
    match (previous, computed) {
        (None, None) => (Step::Skip, None),
        (Some(_), None) => (Step::Withdraw, None),
        (Some(old), Some(new)) if old.view_hash == new.view_hash => (Step::Skip, Some(old)),
        (_, Some(new)) => (Step::Publish(new.clone()), Some(new)),
    }
}

/// The caches `held` has a key for that `wanted` no longer names, ascending.
fn departed(held: &BTreeMap<SmolStr, MemoKey>, wanted: &[MemoKey]) -> Vec<SmolStr> {
    held.keys()
        .filter(|cache| !wanted.iter().any(|key| key.cache == **cache))
        .cloned()
        .collect()
}

/// Computes the ownership of every `Distributed` cache in each snapshot the
/// receiver shows and sends a changed one as [`Update::Ownership`]. A cache
/// whose members and owner count did not change is not computed again, and a
/// computed ranking with the view hash already published is not sent again. A
/// cache that stops being wanted, or has no eligible member, is retracted with
/// [`Update::OwnershipGone`] if the model holds a digest for it. Returns when
/// the observer stops or `updates` closes.
pub async fn run(
    mut snapshots: watch::Receiver<Arc<ClusterSnapshot>>,
    updates: mpsc::Sender<Update>,
) {
    let mut done: BTreeMap<SmolStr, Done> = BTreeMap::new();
    loop {
        let snapshot = Arc::clone(&snapshots.borrow_and_update());
        let wanted = targets(&snapshot);
        let mut held = held_keys(&done);
        for cache in departed(&held, &wanted) {
            held.remove(&cache);
            if let Some(gone) = done.remove(&cache)
                && gone.digest.is_some()
                && updates.send(Update::OwnershipGone(cache)).await.is_err()
            {
                return;
            }
        }
        for key in to_compute(&held, &wanted).into_iter().cloned() {
            let previous = done.get(&key.cache).and_then(|done| done.digest.clone());
            let (snapshot, work_key) = (Arc::clone(&snapshot), key.clone());
            let work_previous = previous.clone();
            let Ok(computed) = tokio::task::spawn_blocking(move || {
                compute(&snapshot, &work_key, work_previous.as_ref())
            })
            .await
            else {
                return;
            };
            let (step, kept) = outcome(previous, computed);
            let update = match step {
                Step::Publish(digest) => Some(Update::Ownership(digest)),
                Step::Withdraw => Some(Update::OwnershipGone(key.cache.clone())),
                Step::Skip => None,
            };
            if let Some(update) = update
                && updates.send(update).await.is_err()
            {
                return;
            }
            done.insert(key.cache.clone(), Done { key, digest: kept });
        }
        if snapshots.changed().await.is_err() {
            return;
        }
    }
}

/// The key each cache in `done` was last ranked under.
fn held_keys(done: &BTreeMap<SmolStr, Done>) -> BTreeMap<SmolStr, MemoKey> {
    done.iter()
        .map(|(cache, done)| (cache.clone(), done.key.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use sundog::observe::Member;

    use super::*;
    use crate::model::testkit::{self, distributed, member_with};

    fn k(owners: u8) -> NonZeroU8 {
        NonZeroU8::new(owners).unwrap()
    }

    fn snapshot_of(members: Vec<Member>) -> ClusterSnapshot {
        ClusterSnapshot::new("c", members, 0)
    }

    #[test]
    fn a_target_exists_for_each_distributed_cache_a_live_member_advertises() {
        let snapshot = snapshot_of(vec![
            testkit::full_member(1, MemberStatus::Live),
            testkit::full_member(2, MemberStatus::Live),
            member_with(3, 0, 1, MemberStatus::Live, &[("other", distributed(3))]),
        ]);
        let keys = targets(&snapshot);
        let names: Vec<&str> = keys.iter().map(|key| key.cache.as_str()).collect();
        assert_eq!(names, ["it", "other"]);
        assert_eq!(keys[0].k, k(2));
        assert_eq!(keys[1].k, k(3));
        assert_eq!(keys[0].voters.len(), 2, "the members that advertise `it`");
        assert_eq!(
            keys[1].voters.len(),
            1,
            "the member that advertises `other`"
        );
    }

    #[test]
    fn replicated_caches_and_non_live_advertisers_make_no_target() {
        let snapshot = snapshot_of(vec![
            member_with(1, 0, 1, MemberStatus::Live, &[("r", Mode::Replicated)]),
            testkit::member(2, MemberStatus::Departing),
            testkit::member(3, MemberStatus::Down),
            testkit::member(4, MemberStatus::Left),
        ]);
        assert!(targets(&snapshot).is_empty());
        assert!(targets(&snapshot_of(Vec::new())).is_empty());
    }

    #[test]
    fn the_voters_are_the_live_and_departing_members_only() {
        let snapshot = snapshot_of(vec![
            testkit::member(1, MemberStatus::Live),
            testkit::member(2, MemberStatus::Departing),
            testkit::member(3, MemberStatus::Down),
            testkit::member(4, MemberStatus::Left),
        ]);
        let key = &targets(&snapshot)[0];
        let nodes: Vec<_> = key.voters.iter().map(|v| v.node).collect();
        assert_eq!(nodes, [testkit::node_id(1, 0), testkit::node_id(2, 0)]);
        assert_eq!(key.voters[1].status, MemberStatus::Departing);
        assert_eq!(key.voters[0].mode, Some(distributed(2)));
    }

    #[test]
    fn live_members_that_disagree_on_owners_are_ranked_with_the_majority() {
        let snapshot = snapshot_of(vec![
            member_with(1, 0, 1, MemberStatus::Live, &[("it", distributed(2))]),
            member_with(2, 0, 1, MemberStatus::Live, &[("it", distributed(3))]),
            member_with(3, 0, 1, MemberStatus::Live, &[("it", distributed(3))]),
        ]);
        assert_eq!(targets(&snapshot)[0].k, k(3));
    }

    #[test]
    fn a_tie_on_owners_goes_to_the_lower_count() {
        let snapshot = snapshot_of(vec![
            member_with(1, 0, 1, MemberStatus::Live, &[("it", distributed(3))]),
            member_with(2, 0, 1, MemberStatus::Live, &[("it", distributed(2))]),
        ]);
        assert_eq!(targets(&snapshot)[0].k, k(2));
    }

    #[test]
    fn an_unchanged_snapshot_asks_for_nothing() {
        let snapshot = testkit::snapshot(3);
        let wanted = targets(&snapshot);
        let held: BTreeMap<_, _> = wanted
            .iter()
            .map(|key| (key.cache.clone(), key.clone()))
            .collect();
        assert!(to_compute(&held, &wanted).is_empty());
        assert_eq!(to_compute(&BTreeMap::new(), &wanted).len(), 1);
    }

    #[test]
    fn a_changed_eligible_set_protocol_or_status_asks_for_a_recompute() {
        let before = targets(&testkit::snapshot(3));
        let held: BTreeMap<_, _> = before
            .iter()
            .map(|key| (key.cache.clone(), key.clone()))
            .collect();

        let joined = targets(&testkit::snapshot(4));
        assert_eq!(to_compute(&held, &joined).len(), 1);

        let mut members = testkit::snapshot(3).members;
        members[1].status = MemberStatus::Departing;
        assert_eq!(to_compute(&held, &targets(&snapshot_of(members))).len(), 1);

        let mut members = testkit::snapshot(3).members;
        members[2].peer.protocol -= 1;
        assert_eq!(to_compute(&held, &targets(&snapshot_of(members))).len(), 1);

        let mut members = testkit::snapshot(3).members;
        members[0].peer.incarnation += 1;
        assert_eq!(to_compute(&held, &targets(&snapshot_of(members))).len(), 1);
    }

    fn held_of(snapshot: &ClusterSnapshot) -> BTreeMap<SmolStr, MemoKey> {
        targets(snapshot)
            .into_iter()
            .map(|key| (key.cache.clone(), key))
            .collect()
    }

    #[test]
    fn a_member_first_seen_down_or_left_does_not_ask_for_a_recompute() {
        let held = held_of(&testkit::snapshot(3));
        let mut members = testkit::snapshot(3).members;
        members.push(testkit::member(4, MemberStatus::Down));
        members.push(testkit::member(5, MemberStatus::Left));
        assert!(to_compute(&held, &targets(&snapshot_of(members))).is_empty());
    }

    #[test]
    fn a_live_member_that_goes_down_or_left_asks_for_a_recompute() {
        let held = held_of(&testkit::snapshot(3));
        for status in [MemberStatus::Down, MemberStatus::Left] {
            let mut members = testkit::snapshot(3).members;
            members[1].status = status;
            let wanted = targets(&snapshot_of(members));
            assert_eq!(to_compute(&held, &wanted).len(), 1, "{status:?}");
        }
    }

    #[test]
    fn a_new_live_member_that_does_not_advertise_the_cache_asks_for_no_recompute() {
        let held = held_of(&testkit::snapshot(3));
        let mut members = testkit::snapshot(3).members;
        members.push(member_with(
            4,
            0,
            1,
            MemberStatus::Live,
            &[("r", Mode::Replicated)],
        ));
        members.push(member_with(5, 0, 1, MemberStatus::Live, &[]));
        let wanted = targets(&snapshot_of(members));
        assert!(to_compute(&held, &wanted).is_empty());
        assert_eq!(wanted[0].voters.len(), 3, "only the advertisers vote");
    }

    #[test]
    fn a_live_member_that_stops_advertising_the_cache_asks_for_a_recompute() {
        let held = held_of(&testkit::snapshot(3));
        let mut members = testkit::snapshot(3).members;
        members[2].caches.clear();
        members.push(testkit::member(4, MemberStatus::Live));
        // Node 3 left the voters and node 4 joined them: the key differs.
        assert_eq!(
            to_compute(&held, &targets(&snapshot_of(members.clone()))).len(),
            1
        );
        members.pop();
        assert_eq!(to_compute(&held, &targets(&snapshot_of(members))).len(), 1);
    }

    #[test]
    fn a_second_incarnation_of_a_node_votes_even_when_it_advertises_nothing() {
        let held = held_of(&testkit::snapshot(2));
        // The later incarnation of node 1 advertises no cache, so the node is
        // no longer eligible although the record is not an advertiser.
        let mut members = testkit::snapshot(2).members;
        members.push(member_with(1, 0, 2, MemberStatus::Live, &[]));
        let wanted = targets(&snapshot_of(members));
        assert_eq!(to_compute(&held, &wanted).len(), 1);
        assert_eq!(wanted[0].voters.len(), 3);
    }

    #[test]
    fn leading_k_takes_the_majority_and_the_lower_count_on_a_tie() {
        let live = |index, owners| {
            member_with(
                index,
                0,
                1,
                MemberStatus::Live,
                &[("it", distributed(owners))],
            )
        };
        let majority = snapshot_of(vec![live(1, 2), live(2, 3), live(3, 3)]);
        assert_eq!(leading_k(&majority, "it"), Some(k(3)));
        let tie = snapshot_of(vec![live(1, 3), live(2, 2)]);
        assert_eq!(leading_k(&tie, "it"), Some(k(2)));
        let single = snapshot_of(vec![live(1, 4)]);
        assert_eq!(leading_k(&single, "it"), Some(k(4)));
    }

    #[test]
    fn leading_k_counts_live_distributed_advertisers_only() {
        assert_eq!(leading_k(&snapshot_of(Vec::new()), "it"), None);
        let others = snapshot_of(vec![
            member_with(1, 0, 1, MemberStatus::Live, &[("it", Mode::Replicated)]),
            member_with(2, 0, 1, MemberStatus::Live, &[("other", distributed(2))]),
            member_with(3, 0, 1, MemberStatus::Departing, &[("it", distributed(2))]),
            member_with(4, 0, 1, MemberStatus::Down, &[("it", distributed(2))]),
            member_with(5, 0, 1, MemberStatus::Left, &[("it", distributed(2))]),
        ]);
        assert_eq!(leading_k(&others, "it"), None);
        // Departing members do not outvote a live one.
        let outvoted = snapshot_of(vec![
            member_with(1, 0, 1, MemberStatus::Live, &[("it", distributed(2))]),
            member_with(2, 0, 1, MemberStatus::Departing, &[("it", distributed(3))]),
            member_with(3, 0, 1, MemberStatus::Departing, &[("it", distributed(3))]),
        ]);
        assert_eq!(leading_k(&outvoted, "it"), Some(k(2)));
    }

    #[test]
    fn compute_ranks_the_key_and_is_none_without_an_eligible_member() {
        let snapshot = testkit::snapshot(3);
        let key = targets(&snapshot).remove(0);
        let first = compute(&snapshot, &key, None).expect("three eligible members");
        assert_eq!(first.cache, "it");
        assert_eq!(first.eligible.len(), 3);
        assert_eq!(first.previous_view, None);

        let four = testkit::snapshot(4);
        let key = targets(&four).remove(0);
        let next = compute(&four, &key, Some(&first)).expect("four eligible members");
        assert_eq!(next.previous_view, Some(first.view_hash));
        assert!(next.moved > 0);

        let mut members = testkit::snapshot(2).members;
        for member in &mut members {
            member.peer.protocol = sundog::wire::PROTOCOL_DISTRIBUTED - 1;
        }
        let old = snapshot_of(members);
        let key = targets(&old).remove(0);
        assert!(compute(&old, &key, None).is_none());
    }

    fn digest_of(live: u8) -> OwnershipDigest {
        testkit::ownership_digest(&testkit::snapshot(live), "it").unwrap()
    }

    #[test]
    fn a_first_digest_is_published_and_kept() {
        let (step, kept) = outcome(None, Some(digest_of(3)));
        let Step::Publish(sent) = step else {
            panic!("a first digest is published, not {step:?}");
        };
        assert_eq!(sent.view_hash, digest_of(3).view_hash);
        assert_eq!(kept.unwrap().view_hash, sent.view_hash);
    }

    #[test]
    fn a_ranking_with_the_published_view_is_skipped_and_the_held_digest_kept() {
        let three = digest_of(3);
        let held_hash = three.view_hash;
        let held_eligible = three.eligible.clone();
        // A recompute under a new incarnation ranks the same nodes.
        let (step, kept) = outcome(Some(three), Some(digest_of(3)));
        assert!(matches!(step, Step::Skip), "{step:?}");
        let kept = kept.expect("the held digest is kept");
        assert_eq!(kept.view_hash, held_hash);
        assert_eq!(kept.eligible, held_eligible);
    }

    #[test]
    fn a_new_view_is_published_and_kept() {
        let (step, kept) = outcome(Some(digest_of(3)), Some(digest_of(4)));
        let Step::Publish(sent) = step else {
            panic!("a new view is published, not {step:?}");
        };
        assert_eq!(sent.eligible.len(), 4);
        assert_eq!(kept.unwrap().view_hash, sent.view_hash);
    }

    #[test]
    fn no_ranking_sends_nothing_before_a_digest_and_retracts_one_after() {
        let (step, kept) = outcome(None, None);
        assert!(matches!(step, Step::Skip), "{step:?}");
        assert!(kept.is_none());
        let (step, kept) = outcome(Some(digest_of(3)), None);
        assert!(matches!(step, Step::Withdraw), "{step:?}");
        assert!(kept.is_none());
    }

    #[test]
    fn departed_names_the_held_caches_no_key_wants() {
        let held = held_of(&snapshot_of(vec![member_with(
            1,
            0,
            1,
            MemberStatus::Live,
            &[
                ("a", distributed(2)),
                ("b", distributed(2)),
                ("c", distributed(2)),
            ],
        )]));
        let wanted = targets(&snapshot_of(vec![member_with(
            1,
            0,
            1,
            MemberStatus::Live,
            &[("b", distributed(2))],
        )]));
        assert_eq!(departed(&held, &wanted), ["a", "c"]);
        assert_eq!(departed(&held, &targets(&snapshot_of(Vec::new()))).len(), 3);
        assert!(departed(&BTreeMap::new(), &wanted).is_empty());
        assert!(departed(&held, &held.values().cloned().collect::<Vec<_>>()).is_empty());
    }

    async fn next_update(updates: &mut mpsc::Receiver<Update>) -> Update {
        tokio::time::timeout(Duration::from_secs(60), updates.recv())
            .await
            .expect("the worker publishes within the bound")
            .expect("the worker is running")
    }

    /// Asserts the worker sends nothing for a while. A worker that sends
    /// wrongly fails; a slow one only weakens the check, because the decision
    /// itself is pinned by the unit tests of [`outcome`] and [`to_compute`].
    async fn assert_quiet(updates: &mut mpsc::Receiver<Update>) {
        let heard = tokio::time::timeout(Duration::from_millis(500), updates.recv()).await;
        assert!(heard.is_err(), "unexpected update: {heard:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_worker_publishes_a_digest_per_view() {
        let (tx, rx) = watch::channel(Arc::new(testkit::snapshot(3)));
        let (updates_tx, mut updates) = mpsc::channel(8);
        let task = tokio::spawn(run(rx, updates_tx));

        let Update::Ownership(first) = next_update(&mut updates).await else {
            panic!("an ownership update");
        };
        assert_eq!(first.cache, "it");
        assert_eq!(first.eligible.len(), 3);
        assert_eq!(first.previous_view, None);
        assert_eq!(first.moved, 0);

        // A fourth live member changes the view.
        tx.send(Arc::new(testkit::snapshot(4))).unwrap();
        let Update::Ownership(second) = next_update(&mut updates).await else {
            panic!("an ownership update");
        };
        assert_eq!(second.eligible.len(), 4);
        assert_eq!(second.previous_view, Some(first.view_hash));
        assert!(second.moved > 0);
        assert_ne!(second.view_hash, first.view_hash);

        drop(tx);
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("the worker ends with the observer")
            .unwrap();
        assert!(updates.recv().await.is_none(), "nothing else was published");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_worker_sends_nothing_for_snapshots_that_leave_the_view_alone() {
        let (tx, rx) = watch::channel(Arc::new(testkit::snapshot(2)));
        let (updates_tx, mut updates) = mpsc::channel(8);
        let task = tokio::spawn(run(rx, updates_tx));
        next_update(&mut updates).await;

        // A member first seen down does not change the key.
        let mut members = testkit::snapshot(2).members;
        members.push(testkit::member(4, MemberStatus::Down));
        tx.send(Arc::new(snapshot_of(members))).unwrap();
        assert_quiet(&mut updates).await;

        // A new incarnation changes the key but not the ranking.
        let mut members = testkit::snapshot(2).members;
        members[0].peer.incarnation += 1;
        tx.send(Arc::new(snapshot_of(members))).unwrap();
        assert_quiet(&mut updates).await;

        tx.send(Arc::new(testkit::snapshot(3))).unwrap();
        let Update::Ownership(next) = next_update(&mut updates).await else {
            panic!("an ownership update");
        };
        assert_eq!(next.eligible.len(), 3);

        drop(tx);
        task.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_worker_retracts_a_cache_no_live_member_advertises() {
        let (tx, rx) = watch::channel(Arc::new(testkit::snapshot(2)));
        let (updates_tx, mut updates) = mpsc::channel(8);
        let task = tokio::spawn(run(rx, updates_tx));
        let Update::Ownership(first) = next_update(&mut updates).await else {
            panic!("an ownership update");
        };

        let mut members = testkit::snapshot(2).members;
        members[0].status = MemberStatus::Down;
        members[1].status = MemberStatus::Left;
        tx.send(Arc::new(snapshot_of(members))).unwrap();
        let Update::OwnershipGone(cache) = next_update(&mut updates).await else {
            panic!("a retraction");
        };
        assert_eq!(cache, "it");

        // The cache returns as a first view; the retraction is sent once.
        tx.send(Arc::new(testkit::snapshot(2))).unwrap();
        let Update::Ownership(again) = next_update(&mut updates).await else {
            panic!("an ownership update");
        };
        assert_eq!(again.previous_view, None);
        assert_eq!(again.view_hash, first.view_hash);

        drop(tx);
        task.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_worker_retracts_a_digest_when_no_member_is_eligible_any_more() {
        let (tx, rx) = watch::channel(Arc::new(testkit::snapshot(2)));
        let (updates_tx, mut updates) = mpsc::channel(8);
        let task = tokio::spawn(run(rx, updates_tx));
        next_update(&mut updates).await;

        // Both advertisers are live, but on a protocol that predates
        // `Distributed`: the cache is wanted and nobody is eligible.
        let mut members = testkit::snapshot(2).members;
        for member in &mut members {
            member.peer.protocol = sundog::wire::PROTOCOL_DISTRIBUTED - 1;
        }
        tx.send(Arc::new(snapshot_of(members))).unwrap();
        let Update::OwnershipGone(cache) = next_update(&mut updates).await else {
            panic!("a retraction");
        };
        assert_eq!(cache, "it");

        drop(tx);
        task.await.unwrap();
        assert!(updates.recv().await.is_none(), "retracted once");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_worker_ends_when_nobody_listens() {
        let (tx, rx) = watch::channel(Arc::new(testkit::snapshot(2)));
        let (updates_tx, updates) = mpsc::channel(1);
        drop(updates);
        tokio::time::timeout(Duration::from_secs(60), run(rx, updates_tx))
            .await
            .expect("the worker ends once a send fails");
        drop(tx);
    }
}

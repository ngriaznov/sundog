//! The ownership worker: computes each `Distributed` cache's ownership off the
//! async workers and publishes [`Update::Ownership`].
//!
//! Ranking a cache costs about 65,536 hashes per eligible node, so the worker
//! computes it on a blocking thread, only when the members that decide the
//! ownership change, and only for the newest snapshot.

use std::collections::BTreeMap;
use std::num::NonZeroU8;
use std::sync::Arc;

use smol_str::SmolStr;
use sundog::NodeId;
use sundog::observe::{ClusterSnapshot, MemberStatus};
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
/// keys rank the cache the same way. The key lists every member that is live
/// or departing, because those are the members whose records the ownership
/// reads.
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

/// Computes the ownership of every `Distributed` cache in each snapshot the
/// receiver shows and sends a changed one as [`Update::Ownership`]. A cache
/// whose members and owner count did not change is not computed again, and a
/// computed ranking with the view hash already published is not sent again.
/// Returns when the observer stops or `updates` closes.
pub async fn run(
    mut snapshots: watch::Receiver<Arc<ClusterSnapshot>>,
    updates: mpsc::Sender<Update>,
) {
    let mut done: BTreeMap<SmolStr, Done> = BTreeMap::new();
    loop {
        let snapshot = Arc::clone(&snapshots.borrow_and_update());
        let wanted = targets(&snapshot);
        done.retain(|cache, _| wanted.iter().any(|key| key.cache == *cache));
        let held: BTreeMap<SmolStr, MemoKey> = done
            .iter()
            .map(|(cache, done)| (cache.clone(), done.key.clone()))
            .collect();
        for key in to_compute(&held, &wanted).into_iter().cloned() {
            let previous = done.get(&key.cache).and_then(|done| done.digest.clone());
            let (snapshot, work_key) = (Arc::clone(&snapshot), key.clone());
            let work_previous = previous.clone();
            let Ok(digest) = tokio::task::spawn_blocking(move || {
                compute(&snapshot, &work_key, work_previous.as_ref())
            })
            .await
            else {
                return;
            };
            let changed = digest.as_ref().is_some_and(|new| {
                previous
                    .as_ref()
                    .is_none_or(|old| old.view_hash != new.view_hash)
            });
            if changed
                && let Some(new) = &digest
                && updates.send(Update::Ownership(new.clone())).await.is_err()
            {
                return;
            }
            let kept = if digest.is_some() && !changed {
                previous
            } else {
                digest
            };
            done.insert(key.cache.clone(), Done { key, digest: kept });
        }
        if snapshots.changed().await.is_err() {
            return;
        }
    }
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
        assert_eq!(keys[0].voters.len(), 3);
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

    #[test]
    fn a_member_going_down_or_left_does_not_ask_for_a_recompute() {
        let before = targets(&testkit::snapshot(3));
        let held: BTreeMap<_, _> = before
            .iter()
            .map(|key| (key.cache.clone(), key.clone()))
            .collect();
        let mut members = testkit::snapshot(3).members;
        members.push(testkit::member(4, MemberStatus::Down));
        members.push(testkit::member(5, MemberStatus::Left));
        assert!(to_compute(&held, &targets(&snapshot_of(members))).is_empty());
    }

    async fn next_update(updates: &mut mpsc::Receiver<Update>) -> Update {
        tokio::time::timeout(Duration::from_secs(60), updates.recv())
            .await
            .expect("the worker publishes within the bound")
            .expect("the worker is running")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_worker_publishes_a_digest_per_view_and_none_for_an_unchanged_one() {
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

        // A snapshot that changes nothing the ownership reads is not computed again.
        let mut members = testkit::snapshot(3).members;
        members.push(testkit::member(4, MemberStatus::Down));
        tx.send(Arc::new(snapshot_of(members))).unwrap();
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
    async fn the_worker_skips_a_recompute_that_yields_the_published_view() {
        let (tx, rx) = watch::channel(Arc::new(testkit::snapshot(2)));
        let (updates_tx, mut updates) = mpsc::channel(8);
        let task = tokio::spawn(run(rx, updates_tx));
        next_update(&mut updates).await;

        // A new incarnation changes the memo key but not the ranking.
        let mut members = testkit::snapshot(2).members;
        members[0].peer.incarnation += 1;
        tx.send(Arc::new(snapshot_of(members))).unwrap();
        tx.send(Arc::new(testkit::snapshot(3))).unwrap();
        let Update::Ownership(next) = next_update(&mut updates).await else {
            panic!("an ownership update");
        };
        assert_eq!(
            next.eligible.len(),
            3,
            "the unchanged ranking was not resent"
        );

        drop(tx);
        task.await.unwrap();
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

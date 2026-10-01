//! The model's summary that the scenario director awaits on.

use std::collections::BTreeMap;

use smol_str::SmolStr;
use sundog::observe::MemberStatus;

use super::Model;

/// What a scenario step can await: counts and states, keyed by slot label and
/// cache name so the director never touches node ids.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ModelDigest {
    /// Members in the `Live` status.
    pub live: usize,
    /// The status of the newest incarnation at each slot's address, by label.
    pub statuses: BTreeMap<SmolStr, MemberStatus>,
    /// The ownership view hash of each `Distributed` cache.
    pub view_hash: BTreeMap<SmolStr, u64>,
    /// Whether each `Distributed` cache has settled.
    pub settled: BTreeMap<SmolStr, bool>,
}

/// The digest of `model`.
#[must_use]
pub fn digest(model: &Model) -> ModelDigest {
    let mut digest = ModelDigest::default();
    if let Some(snapshot) = model.snapshot() {
        digest.live = snapshot
            .members
            .iter()
            .filter(|member| member.status == MemberStatus::Live)
            .count();
        // The newest incarnation at an address stands for its slot.
        let mut newest: BTreeMap<SmolStr, (u64, MemberStatus)> = BTreeMap::new();
        for member in &snapshot.members {
            let Some(slot) = model.slots().get(member.peer.gossip_addr) else {
                continue;
            };
            let entry = (member.peer.incarnation, member.status);
            newest
                .entry(slot.label.clone())
                .and_modify(|held| {
                    if entry.0 >= held.0 {
                        *held = entry;
                    }
                })
                .or_insert(entry);
        }
        digest.statuses = newest
            .into_iter()
            .map(|(label, (_, status))| (label, status))
            .collect();
    }
    for ownership in model.ownership_digests() {
        digest
            .view_hash
            .insert(ownership.cache.clone(), ownership.view_hash);
        digest.settled.insert(
            ownership.cache.clone(),
            model.settled(&ownership.cache).unwrap_or(false),
        );
    }
    digest
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant, SystemTime};

    use sundog::observe::ClusterSnapshot;

    use crate::model::testkit;
    use crate::source::Update;

    use super::*;

    fn feed(model: &mut Model, snapshot: ClusterSnapshot, now: Instant) {
        let digest = testkit::ownership_digest(&snapshot, "it");
        model.apply(
            Update::Snapshot(Arc::new(snapshot), now),
            now,
            SystemTime::UNIX_EPOCH,
        );
        if let Some(digest) = digest {
            model.apply(Update::Ownership(digest), now, SystemTime::UNIX_EPOCH);
        }
    }

    #[test]
    fn an_empty_model_digests_to_nothing() {
        assert_eq!(digest(&Model::new()), ModelDigest::default());
    }

    #[test]
    fn the_digest_counts_live_members_and_labels_statuses() {
        let mut model = Model::new();
        let snapshot = ClusterSnapshot::new(
            "c",
            vec![
                testkit::member(1, MemberStatus::Live),
                testkit::member(2, MemberStatus::Departing),
                testkit::member(3, MemberStatus::Down),
                testkit::member(4, MemberStatus::Left),
            ],
            0,
        );
        feed(&mut model, snapshot, Instant::now());
        let digest = digest(&model);
        assert_eq!(digest.live, 1);
        assert_eq!(digest.statuses["n1"], MemberStatus::Live);
        assert_eq!(digest.statuses["n2"], MemberStatus::Departing);
        assert_eq!(digest.statuses["n3"], MemberStatus::Down);
        assert_eq!(digest.statuses["n4"], MemberStatus::Left);
    }

    #[test]
    fn the_newest_incarnation_at_an_address_stands_for_its_slot() {
        let mut model = Model::new();
        let old = testkit::member_at(1, 7, 1, MemberStatus::Left);
        let new = testkit::member_at(1, 8, 2, MemberStatus::Live);
        let snapshot = ClusterSnapshot::new("c", vec![old, new], 0);
        feed(&mut model, snapshot, Instant::now());
        assert_eq!(digest(&model).statuses["n1"], MemberStatus::Live);
        assert_eq!(digest(&model).live, 1);
    }

    #[test]
    fn the_digest_carries_view_hashes_and_settled_flags() {
        let mut model = Model::new();
        let start = Instant::now();
        feed(&mut model, testkit::snapshot(3), start);
        let first = digest(&model);
        assert_eq!(first.view_hash.len(), 1);
        assert!(!first.settled["it"]);

        model.tick(start + Duration::from_secs(4));
        let later = digest(&model);
        assert_eq!(later.view_hash, first.view_hash);
        assert!(later.settled["it"]);

        feed(
            &mut model,
            testkit::snapshot(4),
            start + Duration::from_secs(5),
        );
        let moved = digest(&model);
        assert_ne!(moved.view_hash["it"], first.view_hash["it"]);
        assert!(!moved.settled["it"]);
        assert_eq!(moved.live, 4);
    }
}

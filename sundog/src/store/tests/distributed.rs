//! A `Mode::Distributed` shard: forwarding, owner gating and the serving methods under an ownership view.

use super::*;

/// `bucket`'s first part, which a bucket-granularity view owns exactly
/// when it owns the bucket.
fn first_part(bucket: u16) -> PartId {
    PartId::new(bucket, 0)
}

#[tokio::test]
async fn insert_on_a_non_owner_never_becomes_visible_to_local_get() {
    let self_node = NodeId::from(1);
    let eligible = (1..=5u64).map(NodeId::from).collect::<Vec<_>>();
    let residency = Arc::new(ResidencySet::new());
    let (s, view, _tx) = distributed_shard::<u32, String>(self_node, eligible, 2, residency);

    let unowned_keys: Vec<u32> = (0..5_000u32)
        .filter(|&key| !view.owns(PartId::of_key(&key_bytes(&key))))
        .take(200)
        .collect();
    assert!(
        !unowned_keys.is_empty(),
        "some unowned key is found within a 5,000-key scan"
    );

    for &key in &unowned_keys {
        s.insert(key, "v".to_string())
            .await
            .expect("a non-owner's insert never errors");
        assert!(
            s.get(&key).await.is_none(),
            "a non-owner's write for key {key} must never become locally visible"
        );
    }
}

#[tokio::test]
async fn clear_on_a_distributed_cache_only_tombstones_owned_buckets() {
    let self_node = NodeId::from(1);
    let eligible = (1..=5u64).map(NodeId::from).collect::<Vec<_>>();
    let residency = Arc::new(ResidencySet::new());
    let (s, view, _tx) = distributed_shard::<u32, String>(self_node, eligible, 2, residency);

    let owned_key = find_key_by_ownership(&view, true);
    let unowned_key = find_key_by_ownership(&view, false);
    s.insert(owned_key, "owned".to_string())
        .await
        .expect("owner's insert lands");
    s.insert(unowned_key, "forwarded".to_string())
        .await
        .expect("a non-owner's insert never errors");

    assert_eq!(s.get(&owned_key).await, Some("owned".to_string()));
    assert!(
        s.get(&unowned_key).await.is_none(),
        "the forwarded write never became locally visible in the first place"
    );

    s.clear().await.expect("clear");

    assert!(
        s.get(&owned_key).await.is_none(),
        "clear tombstones the owned key"
    );
    assert!(
        s.get(&unowned_key).await.is_none(),
        "still absent: nothing was ever held for the unowned bucket to begin with"
    );
}

#[tokio::test]
async fn insert_many_on_a_distributed_cache_forwards_unowned_entries_and_applies_owned_ones() {
    let self_node = NodeId::from(1);
    let eligible = (1..=5u64).map(NodeId::from).collect::<Vec<_>>();
    let residency = Arc::new(ResidencySet::new());
    let (s, view, _tx) = distributed_shard::<u32, String>(self_node, eligible, 2, residency);

    let owned_key = find_key_by_ownership(&view, true);
    let unowned_key = find_key_by_ownership(&view, false);

    s.insert_many([
        (owned_key, "owned".to_string()),
        (unowned_key, "forwarded".to_string()),
    ])
    .await
    .expect("bulk insert");

    assert_eq!(s.get(&owned_key).await, Some("owned".to_string()));
    assert!(
        s.get(&unowned_key).await.is_none(),
        "a bulk write forwarded for an unowned bucket never becomes locally visible"
    );
    let forwarded = s.fan_out_queue().drain();
    assert!(
        forwarded.iter().any(|item| matches!(
            item,
            FanOutItem::Forward(rec) if rec.key.as_ref() == key_bytes(&unowned_key).as_ref()
        )),
        "the unowned key lands on the fan-out queue as a Forward item: {forwarded:?}"
    );
}

#[tokio::test]
async fn remove_many_on_a_distributed_cache_forwards_unowned_tombstones_and_applies_owned_ones() {
    let self_node = NodeId::from(1);
    let eligible = (1..=5u64).map(NodeId::from).collect::<Vec<_>>();
    let residency = Arc::new(ResidencySet::new());
    let (s, view, _tx) = distributed_shard::<u32, String>(self_node, eligible, 2, residency);

    let owned_key = find_key_by_ownership(&view, true);
    let unowned_key = find_key_by_ownership(&view, false);
    s.insert(owned_key, "owned".to_string())
        .await
        .expect("owner's insert lands");
    let _ = s.fan_out_queue().drain();

    s.remove_many([owned_key, unowned_key])
        .await
        .expect("bulk remove");

    assert!(
        s.get(&owned_key).await.is_none(),
        "the owned key is tombstoned"
    );
    assert!(
        s.get(&unowned_key).await.is_none(),
        "still absent: it was never held to begin with"
    );
    let forwarded = s.fan_out_queue().drain();
    assert!(
        forwarded.iter().any(|item| matches!(
            item,
            FanOutItem::Forward(rec) if rec.key.as_ref() == key_bytes(&unowned_key).as_ref()
        )),
        "the unowned tombstone lands on the fan-out queue as a Forward item: {forwarded:?}"
    );
}

#[tokio::test]
async fn apply_remote_batch_drops_records_for_an_unowned_bucket_and_counts_them() {
    let self_node = NodeId::from(1);
    let eligible = (1..=5u64).map(NodeId::from).collect::<Vec<_>>();
    let residency = Arc::new(ResidencySet::new());
    let (s, view, _tx) = distributed_shard::<u32, String>(self_node, eligible, 2, residency);

    let owned_key = find_key_by_ownership(&view, true);
    let unowned_key = find_key_by_ownership(&view, false);
    let owned_rec = wire_record(owned_key, "owned", hlc(1, 9));
    let unowned_rec = wire_record(unowned_key, "dropped", hlc(1, 9));

    ShardOps::apply_remote_batch(&s, vec![owned_rec, unowned_rec]).await;

    assert_eq!(
        s.get(&owned_key).await,
        Some("owned".to_string()),
        "a record for a bucket this node owns still applies"
    );
    assert!(
        s.get(&unowned_key).await.is_none(),
        "a record for a bucket this node does not own is dropped, never applied"
    );
}

#[tokio::test]
async fn held_buckets_lists_a_bucket_with_an_entry_the_current_view_no_longer_owns() {
    let self_node = NodeId::from(1);
    let eligible = (1..=5u64).map(NodeId::from).collect::<Vec<_>>();
    let residency = Arc::new(ResidencySet::new());
    let (s, view, tx) = distributed_shard::<u32, String>(self_node, eligible, 2, residency);

    let key = find_key_by_ownership(&view, true);
    let bucket = bucket_of(&key_bytes(&key));
    ShardOps::apply_remote_batch(&s, vec![wire_record(key, "held", hlc(1, 9))]).await;
    assert_eq!(
        ShardOps::held_buckets(&s).await,
        vec![bucket],
        "the one bucket with an entry is held; every empty bucket is not"
    );

    // A view that grew past this node's share of `bucket`, as a
    // membership change publishes while rebalance_task is busy.
    let moved = (6..=64u64)
        .map(|extra| {
            let eligible = (1..=extra).map(NodeId::from).collect::<Vec<_>>();
            OwnershipView::compute(self_node, eligible, NonZeroU8::new(2).expect("nonzero"))
        })
        .find(|v| !v.owns(first_part(bucket)))
        .expect("some larger eligible set takes the bucket away from self");
    tx.send(Arc::new(moved)).expect("receiver still alive");

    assert_eq!(
        ShardOps::held_buckets(&s).await,
        vec![bucket],
        "held_buckets is ungated: the entry is still resident under the new view"
    );
    assert_eq!(
        ShardOps::bucket_lens(&s, vec![bucket]).await,
        vec![BucketLen { bucket, len: 0 }],
        "bucket_lens stays residency-gated: the same bucket reads as empty"
    );
}

#[tokio::test]
async fn apply_remote_batch_forwards_a_record_for_a_releasing_bucket_on_instead_of_applying_it() {
    let self_node = NodeId::from(1);
    let eligible = (1..=5u64).map(NodeId::from).collect::<Vec<_>>();
    let residency = Arc::new(ResidencySet::new());
    let (s, view, _tx) =
        distributed_shard::<u32, String>(self_node, eligible, 2, Arc::clone(&residency));

    let unowned_key = find_key_by_ownership(&view, false);
    let unowned_bucket = bucket_of(&key_bytes(&unowned_key));
    // Mid disown-grace: outbound `is_resident` (owns || releasing) says
    // yes for this bucket, but the inbound-apply guard stays strict
    // current-view ownership: the record goes on to the current owners
    // through the fan-out queue and never lands here.
    residency.mark_releasing(&whole(unowned_bucket));

    let rec = wire_record(unowned_key, "redirected", hlc(1, 9));
    ShardOps::apply_remote_batch(&s, vec![rec.clone()]).await;

    assert!(
        s.get(&unowned_key).await.is_none(),
        "residency never widens what the inbound-apply guard applies"
    );
    assert_eq!(
        s.fan_out_queue().drain(),
        vec![FanOutItem::Forward(rec)],
        "the record is queued for the bucket's current owners"
    );

    // A bucket this node never held is still dropped outright.
    residency.unmark(&whole(unowned_bucket));
    let rec = wire_record(unowned_key, "dropped", hlc(2, 9));
    ShardOps::apply_remote_batch(&s, vec![rec]).await;
    assert!(s.get(&unowned_key).await.is_none());
    assert!(
        s.fan_out_queue().drain().is_empty(),
        "nothing is queued for a dropped record"
    );
}

#[tokio::test]
async fn digests_never_reports_an_unowned_bucket() {
    let self_node = NodeId::from(1);
    let eligible = (1..=5u64).map(NodeId::from).collect::<Vec<_>>();
    let residency = Arc::new(ResidencySet::new());
    let (s, view, _tx) = distributed_shard::<u32, String>(self_node, eligible, 2, residency);

    let reported: HashSet<u16> = ShardOps::digests(&s)
        .await
        .into_iter()
        .map(|bd| bd.bucket)
        .collect();
    for &bucket in &reported {
        assert!(
            view.owns(first_part(bucket)),
            "digests reported unowned bucket {bucket}"
        );
    }
    assert!(
        reported.len() < BUCKET_COUNT,
        "a 5-eligible/k=2 view owns a strict subset of buckets, so at least one is never \
         reported"
    );
}

#[tokio::test]
async fn outbound_guard_returns_nothing_for_an_unowned_bucket_on_every_serving_method() {
    let self_node = NodeId::from(1);
    let eligible = (1..=5u64).map(NodeId::from).collect::<Vec<_>>();
    let residency = Arc::new(ResidencySet::new());
    let (s, view, _tx) = distributed_shard::<u32, String>(self_node, eligible, 2, residency);

    let unowned_key = find_key_by_ownership(&view, false);
    let unowned_bucket = bucket_of(&key_bytes(&unowned_key));

    assert_eq!(
        ShardOps::bucket_lens(&s, vec![unowned_bucket]).await,
        vec![BucketLen {
            bucket: unowned_bucket,
            len: 0
        }],
        "bucket_lens reports zero for an unowned bucket"
    );
    assert_eq!(
        ShardOps::part_digests(&s, vec![unowned_bucket]).await,
        vec![BucketPartDigests {
            bucket: unowned_bucket,
            digests: Vec::new()
        }],
        "part_digests reports nothing for an unowned bucket"
    );
    let part = BucketPart {
        bucket: unowned_bucket,
        part: 0u8,
    };
    assert_eq!(
        ShardOps::entries_for_parts(&s, vec![part]).await,
        vec![(part, Vec::new())],
        "entries_for_parts reports nothing for an unowned bucket's part"
    );
    assert!(
        ShardOps::records_for(&s, vec![key_bytes(&unowned_key)])
            .await
            .is_empty(),
        "records_for never answers a key in an unowned bucket"
    );
    let snapshot: Vec<WireRecord> = ShardOps::snapshot_chunks(&s)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .flatten()
        .collect();
    assert!(
        snapshot
            .iter()
            .all(|rec| bucket_of(rec.key.as_ref()) != unowned_bucket),
        "snapshot_chunks never includes a record from an unowned bucket"
    );
}

#[tokio::test]
async fn a_releasing_bucket_still_answers_digests_and_entries() {
    let self_node = NodeId::from(1);
    // Self starts as the sole eligible node: it owns every bucket, so an
    // ordinary insert lands for real.
    let residency = Arc::new(ResidencySet::new());
    let (s, initial_view, tx) =
        distributed_shard::<u32, String>(self_node, vec![self_node], 1, Arc::clone(&residency));

    let new_view = OwnershipView::compute(
        self_node,
        vec![self_node, NodeId::from(2)],
        NonZeroU8::new(1).expect("nonzero"),
    );
    let key = (0..1_000_000u32)
        .find(|&k| !new_view.owns(PartId::of_key(&key_bytes(&k))))
        .expect("some bucket flips ownership once a second node joins");
    let part = PartId::of_key(&key_bytes(&key));
    assert!(initial_view.owns(part));

    s.insert(key, "still here".to_string())
        .await
        .expect("owner's insert lands");

    // Membership widens and this bucket moves to the other node,
    // exactly what `rebalance_task` reacts to by starting this bucket's
    // disown grace.
    tx.send(Arc::new(new_view)).expect("receiver still alive");
    residency.mark_releasing(&[part]);
    assert!(
        !ShardOps::ownership_view(&s).expect("attached").owns(part),
        "sanity: this node no longer owns the bucket under the new view"
    );

    let digests: HashSet<u16> = ShardOps::digests(&s)
        .await
        .into_iter()
        .map(|bd| bd.bucket)
        .collect();
    assert!(
        digests.contains(&part.bucket()),
        "a releasing bucket still answers digests"
    );

    let entries = ShardOps::entries_for_buckets(&s, vec![part.bucket()]).await;
    assert_eq!(entries.len(), 1);
    assert!(
        entries[0]
            .1
            .iter()
            .any(|kv| kv.key.as_ref() == key_bytes(&key).as_ref()),
        "the pre-existing entry in a releasing bucket is still served"
    );
}

#[test]
fn fan_out_item_applied_and_forward_both_drain_through_one_queue() {
    let queue = FanOutQueue::<FanOutItem<u32>>::new(SmolStr::new("test"), true);
    let rec = wire_record(7, "v", hlc(1, 1));
    let _ = queue.push(FanOutItem::Applied(1, hlc(2, 1)));
    let _ = queue.push(FanOutItem::Forward(rec.clone()));

    assert_eq!(
        queue.drain(),
        vec![FanOutItem::Applied(1, hlc(2, 1)), FanOutItem::Forward(rec)],
        "one queue drains both applied and forwarded items, in push order"
    );
}

#[tokio::test]
async fn forwarded_writes_emit_created_and_removed_events_and_never_updated() {
    let self_node = NodeId::from(1);
    let eligible = (1..=5u64).map(NodeId::from).collect::<Vec<_>>();
    let residency = Arc::new(ResidencySet::new());
    let (s, view, _tx) = distributed_shard::<u32, String>(self_node, eligible, 2, residency);
    let mut events = s.events();

    let key = find_key_by_ownership(&view, false);
    s.insert(key, "first".to_string())
        .await
        .expect("forwarded insert");
    match events.recv().await.expect("an event was published") {
        Event::Created {
            key: k,
            value,
            origin,
        } => {
            assert_eq!(k, key);
            assert_eq!(value, "first");
            assert_eq!(origin, Origin::Local);
        }
        other => panic!("expected Event::Created, got {other:?}"),
    }

    // A second forwarded insert for the same key: a non-owner holds no
    // prior copy, so it still cannot distinguish create from update:
    // this stays Created, never Updated, the documented difference from
    // every other write path.
    s.insert(key, "second".to_string())
        .await
        .expect("forwarded insert");
    match events.recv().await.expect("an event was published") {
        Event::Created { key: k, .. } => assert_eq!(k, key),
        other => panic!("a forwarded write never emits Event::Updated, got {other:?}"),
    }

    s.remove(&key).await.expect("forwarded remove");
    match events.recv().await.expect("an event was published") {
        Event::Removed { key: k, origin } => {
            assert_eq!(k, key);
            assert_eq!(origin, Origin::Local);
        }
        other => panic!("expected Event::Removed, got {other:?}"),
    }
}

#[tokio::test]
async fn release_buckets_removes_every_entry_and_tombstone_and_corrects_the_digest() {
    let s = shard::<u32, String>(1);
    let mut by_bucket: HashMap<u16, Vec<u32>> = HashMap::new();
    let mut pair: Vec<u32> = Vec::new();
    for k in 0..100_000u32 {
        let bucket = bucket_of(&key_bytes(&k));
        let group = by_bucket.entry(bucket).or_default();
        group.push(k);
        if group.len() == 2 {
            pair = group.clone();
            break;
        }
    }
    assert_eq!(
        pair.len(),
        2,
        "1024 buckets; a same-bucket pair is found quickly"
    );
    let released_bucket = bucket_of(&key_bytes(&pair[0]));

    s.insert(pair[0], "live".to_string()).await.expect("insert");
    s.insert(pair[1], "gone".to_string()).await.expect("insert");
    s.remove(&pair[1]).await.expect("remove");

    let other_key = (0..100_000u32)
        .find(|&k| bucket_of(&key_bytes(&k)) != released_bucket)
        .expect("a distinct bucket is found quickly");
    s.insert(other_key, "untouched".to_string())
        .await
        .expect("insert");

    let removed = ShardOps::release_buckets(&s, &[released_bucket]).await;

    assert_eq!(removed, 2, "one live entry and one tombstone removed");
    assert!(s.get(&pair[0]).await.is_none());
    assert_eq!(s.get(&other_key).await, Some("untouched".to_string()));
    let digest_for_released = ShardOps::digests(&s)
        .await
        .into_iter()
        .find(|bd| bd.bucket == released_bucket)
        .map(|bd| bd.digest);
    assert_eq!(
        digest_for_released,
        Some(0),
        "the released bucket's digest resets to zero"
    );
}

#[test]
fn is_cold_bucket_follows_the_residency_set_and_is_false_elsewhere() {
    let plain = shard::<u32, String>(1);
    assert!(!ShardOps::is_cold_bucket(&plain, 7));

    let residency = Arc::new(ResidencySet::new());
    let (s, _view, _tx) = distributed_shard::<u32, String>(
        NodeId::from(1),
        (1..=3u64).map(NodeId::from).collect(),
        2,
        Arc::clone(&residency),
    );
    assert!(!ShardOps::is_cold_bucket(&s, 7));
    residency.mark_cold(&whole(7));
    assert!(ShardOps::is_cold_bucket(&s, 7));
    residency.clear_cold(&whole(7));
    assert!(!ShardOps::is_cold_bucket(&s, 7));
}

#[cfg(feature = "spill")]
#[test]
fn is_unverified_bucket_follows_the_residency_set_and_is_false_elsewhere() {
    let plain = shard::<u32, String>(1);
    assert!(!ShardOps::is_unverified_bucket(&plain, 7));

    let residency = Arc::new(ResidencySet::new());
    let (s, _view, _tx) = distributed_shard::<u32, String>(
        NodeId::from(1),
        (1..=3u64).map(NodeId::from).collect(),
        2,
        Arc::clone(&residency),
    );
    assert!(!ShardOps::is_unverified_bucket(&s, 7));
    residency.mark_unverified(&whole(7));
    assert!(ShardOps::is_unverified_bucket(&s, 7));
    // Distinct from cold: marking one leaves the other alone.
    assert!(!ShardOps::is_cold_bucket(&s, 7));
    residency.clear_unverified(&whole(7));
    assert!(!ShardOps::is_unverified_bucket(&s, 7));
}

#[test]
fn ae_peer_filter_is_identity_for_a_non_distributed_shard() {
    let s = shard::<u32, String>(1);
    let peers = vec![NodeId::from(2), NodeId::from(3)];
    assert_eq!(
        ShardOps::ae_peer_filter(&s, peers.clone(), peers.clone()),
        (peers.clone(), peers)
    );
}

#[tokio::test]
async fn ae_digests_for_is_the_full_digest_list_for_a_non_distributed_shard() {
    let s = shard::<u32, String>(1);
    s.insert(7, "v".to_string()).await.expect("insert");
    let all = ShardOps::digests(&s).await;
    assert_eq!(all.len(), BUCKET_COUNT);
    assert_eq!(ShardOps::ae_digests_for(&s, NodeId::from(2)).await, all);
}

#[tokio::test]
async fn ae_digests_for_keeps_only_buckets_the_peer_co_owns_or_this_shard_is_releasing_to() {
    let self_node = NodeId::from(1);
    let eligible = (1..=5u64).map(NodeId::from).collect::<Vec<_>>();
    let residency = Arc::new(ResidencySet::new());
    let (s, view, _tx) =
        distributed_shard::<u32, String>(self_node, eligible, 2, Arc::clone(&residency));
    let peer = NodeId::from(2);
    let bucket_at = |i: usize| u16::try_from(i).expect("invariant: BUCKET_COUNT fits u16");
    let shared_owned = (0..BUCKET_COUNT)
        .map(bucket_at)
        .find(|&b| view.owns(first_part(b)) && view.owners_of(first_part(b)).contains(&peer))
        .expect("some bucket is owned by both self and the peer");
    let owned_not_shared = (0..BUCKET_COUNT)
        .map(bucket_at)
        .find(|&b| view.owns(first_part(b)) && !view.owners_of(first_part(b)).contains(&peer))
        .expect("some bucket is owned by self but not the peer");
    let peer_only = (0..BUCKET_COUNT)
        .map(bucket_at)
        .find(|&b| !view.owns(first_part(b)) && view.owners_of(first_part(b)).contains(&peer))
        .expect("some bucket is owned by the peer but not self");
    let neither = (0..BUCKET_COUNT)
        .map(bucket_at)
        .find(|&b| !view.owns(first_part(b)) && !view.owners_of(first_part(b)).contains(&peer))
        .expect("some bucket is owned by neither self nor the peer");
    // A bucket mid disown-grace that the peer now owns stays in the
    // round: the anti-entropy backstop for a lost rebalance pull.
    residency.mark_releasing(&[whole(peer_only), whole(neither)].concat());

    let listed: HashSet<u16> = ShardOps::ae_digests_for(&s, peer)
        .await
        .into_iter()
        .map(|bd| bd.bucket)
        .collect();
    assert!(
        listed.contains(&shared_owned),
        "a co-owned bucket is listed"
    );
    assert!(
        listed.contains(&peer_only),
        "a releasing bucket the peer owns is listed"
    );
    assert!(
        !listed.contains(&owned_not_shared),
        "an owned bucket the peer does not own is not listed"
    );
    assert!(
        !listed.contains(&neither),
        "a releasing bucket the peer does not own is not listed"
    );
    for bucket in &listed {
        assert!(
            view.owners_of(first_part(*bucket)).contains(&peer),
            "every listed bucket is owned by the peer: {bucket}"
        );
        assert!(
            view.owns(first_part(*bucket)) || residency.is_releasing(first_part(*bucket)),
            "every listed bucket is resident here: {bucket}"
        );
    }
    let full = ShardOps::digests(&s).await;
    assert!(
        listed.len() < full.len(),
        "the peer-scoped list is a strict subset"
    );
}

#[test]
fn ae_peer_filter_narrows_to_the_current_cohort() {
    let self_node = NodeId::from(1);
    let eligible = (1..=5u64).map(NodeId::from).collect::<Vec<_>>();
    let residency = Arc::new(ResidencySet::new());
    let (s, view, _tx) =
        distributed_shard::<u32, String>(self_node, eligible.clone(), 2, residency);

    let mut cohort: HashSet<NodeId> = HashSet::new();
    for i in 0..BUCKET_COUNT {
        let bucket = u16::try_from(i).expect("invariant: BUCKET_COUNT fits u16");
        if view.owns(first_part(bucket)) {
            cohort.extend(view.owners_of(first_part(bucket)).iter().copied());
        }
    }
    cohort.remove(&self_node);
    assert!(!cohort.is_empty(), "some peer co-owns a bucket with self");

    let candidates: Vec<NodeId> = eligible
        .iter()
        .copied()
        .filter(|&n| n != self_node)
        .collect();
    let stranger = candidates.iter().copied().find(|n| !cohort.contains(n));

    let (dirty, live) = ShardOps::ae_peer_filter(&s, candidates.clone(), candidates.clone());

    for peer in dirty.iter().chain(live.iter()) {
        assert!(
            cohort.contains(peer),
            "ae_peer_filter kept a peer sharing no owned bucket with this shard: {peer:?}"
        );
    }
    if let Some(stranger) = stranger {
        assert!(
            !dirty.contains(&stranger) && !live.contains(&stranger),
            "a peer sharing nothing with this shard is filtered out"
        );
    }
    for peer in candidates.iter().filter(|n| cohort.contains(n)) {
        assert!(
            dirty.contains(peer) && live.contains(peer),
            "a co-owning peer stays in the cohort"
        );
    }
}

//! Shard-level `compact_pass` plumbing tests.

use std::sync::atomic::AtomicUsize;

use super::*;
use crate::store::crdt::{OrSet, OrSetResolver, PnCounter, PnCounterResolver};

fn writer(id: u64) -> crdt::WriterId {
    crdt::WriterId::new(NodeId::from(id), 1)
}

#[tokio::test]
async fn folds_a_dead_writer_in_place_with_no_event_or_version_change() {
    let s = shard::<u32, PnCounter>(1).with_resolver(Arc::new(PnCounterResolver));
    let dead = writer(11);
    let alive = writer(22);
    s.merge(1, PnCounter::local_delta(dead, 3))
        .await
        .expect("first writer's contribution");
    s.merge(1, PnCounter::local_delta(alive, 4))
        .await
        .expect("second writer's contribution");
    assert_eq!(s.get(&1).await.map(|c| c.value()), Some(7));
    let before = ShardOps::records_for(&s, vec![key_bytes(&1u32)]).await;
    assert_eq!(before.len(), 1);

    let mut events = s.events();
    let retire = move |w: crdt::WriterId| w == dead;
    // A `bound_ms` far larger than any `now_ms` gap this test ever
    // spans: stage two only folds a retired entry once it has aged
    // past `2 * bound_ms`, and this test means to isolate stage
    // one's own idempotency, not stage two's separate threshold.
    let CompactPassOutcome {
        retired, compacted, ..
    } = ShardOps::compact_pass(
        &s,
        1_000,
        &retire,
        Quiescence::Settled,
        CompactionBounds::three_bounds(1_000_000_000),
        100,
    )
    .await;

    assert_eq!(
        retired,
        vec![dead],
        "exactly the one dead writer is reported"
    );
    assert_eq!(compacted, 1, "exactly the one touched record is counted");
    assert_eq!(
        s.get(&1).await.map(|c| c.value()),
        Some(7),
        "folding a writer's slot into its retired total never changes the value"
    );
    assert!(
        matches!(
            events.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ),
        "compaction applies through `Engine::compact_replace_if_current`, a direct \
         in-place replace, never `Shard::apply` — no `Event` fires for it, unlike \
         every ordinary write"
    );
    let after = ShardOps::records_for(&s, vec![key_bytes(&1u32)]).await;
    assert_eq!(after.len(), 1);
    assert!(
        after[0].ver > before[0].ver,
        "the version-gated direct replace still mints a fresh, strictly greater \
         version — never leaving the old one in place — so a version alone still \
         uniquely names this content, exactly as `resolve_and_rebind`'s own \
         equal-version no-op rule requires"
    );
    assert_ne!(
        after[0].value, before[0].value,
        "the resident bytes themselves change shape once a writer is retired"
    );

    // Idempotent: the record now holds nothing live for that writer,
    // so a second pass with the identical retire predicate finds
    // nothing left to do.
    let CompactPassOutcome {
        retired: retired_again,
        compacted: compacted_again,
        ..
    } = ShardOps::compact_pass(
        &s,
        2_000,
        &retire,
        Quiescence::Settled,
        CompactionBounds::three_bounds(1_000_000_000),
        100,
    )
    .await;
    assert!(retired_again.is_empty(), "{retired_again:?}");
    assert_eq!(compacted_again, 0);
}

#[tokio::test]
async fn never_calls_a_non_merging_resolvers_compact() {
    struct SpyNeverMerges {
        calls: Arc<AtomicUsize>,
    }
    impl ConflictResolver for SpyNeverMerges {
        fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
            if a.ver >= b.ver { Winner::A } else { Winner::B }
        }
        // `merges()` is left at its `false` default: this resolver
        // never claims to merge.
        fn compact(
            &self,
            _key: &[u8],
            _value: &[u8],
            _now_ms: u64,
            _retire: &dyn Fn(crdt::WriterId) -> bool,
            _quiet: Quiescence,
            _bounds: CompactionBounds,
        ) -> Option<Bytes> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Some(Bytes::from_static(b"should never be reached"))
        }
    }

    let calls = Arc::new(AtomicUsize::new(0));
    let s = shard::<u32, String>(1).with_resolver(Arc::new(SpyNeverMerges {
        calls: Arc::clone(&calls),
    }));
    s.insert(1, "v".to_string()).await.expect("insert");

    let retire_everyone = |_: crdt::WriterId| true;
    let CompactPassOutcome {
        retired, compacted, ..
    } = ShardOps::compact_pass(
        &s,
        0,
        &retire_everyone,
        Quiescence::Settled,
        CompactionBounds::three_bounds(0),
        100,
    )
    .await;

    assert!(retired.is_empty(), "{retired:?}");
    assert_eq!(compacted, 0);
    assert_eq!(
        calls.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "ConflictResolver::compact's own contract (\"only ever called while merges() \
         is true\") is enforced here, not merely assumed: an override that would \
         otherwise happily compact is never even invoked for a non-merging resolver"
    );
}

#[tokio::test]
async fn skips_a_bucket_the_current_view_no_longer_owns() {
    let self_node = NodeId::from(1);
    let residency = Arc::new(ResidencySet::new());
    let (s, initial_view, tx) =
        distributed_shard::<u32, PnCounter>(self_node, vec![self_node], 1, Arc::clone(&residency));
    let s = s.with_resolver(Arc::new(PnCounterResolver));

    let new_view = OwnershipView::compute(
        self_node,
        vec![self_node, NodeId::from(2)],
        NonZeroU8::new(1).expect("nonzero"),
    );
    let key = (0..1_000_000u32)
        .find(|&k| !new_view.owns(PartId::of_key(&key_bytes(&k))))
        .expect("some bucket flips ownership once a second node joins");
    let bucket = PartId::of_key(&key_bytes(&key));
    assert!(initial_view.owns(bucket));

    let dead = writer(11);
    s.merge(key, PnCounter::local_delta(dead, 3))
        .await
        .expect("owner's merge lands");
    let before = ShardOps::records_for(&s, vec![key_bytes(&key)]).await;
    assert_eq!(before.len(), 1);
    // Drains the setup write's own `FanOutItem::Applied` entry so
    // the check below only ever sees what `compact_pass` itself
    // did to the queue, never the ordinary write that seeded it.
    let _ = s.fan_out.drain();

    // Membership widens and this bucket moves to the other node; the
    // shard's own physical copy is still sitting right there, mid
    // disown-grace, exactly as `a_releasing_bucket_still_answers_
    // digests_and_entries` sets up for the ordinary write path.
    // `mark_releasing` keeps the bucket servable by `records_for`'s
    // own outbound-serving guard below, so the *before*/*after*
    // comparison actually reads the resident record rather than
    // the guard's "not resident at all" empty answer either time.
    tx.send(Arc::new(new_view)).expect("receiver still alive");
    residency.mark_releasing(&[bucket]);
    assert!(
        !ShardOps::ownership_view(&s).expect("attached").owns(bucket),
        "sanity: this node no longer owns the bucket under the new view"
    );

    let retire = move |w: crdt::WriterId| w == dead;
    let CompactPassOutcome {
        retired: _retired,
        compacted,
        ..
    } = ShardOps::compact_pass(
        &s,
        1_000,
        &retire,
        Quiescence::Settled,
        CompactionBounds::three_bounds(1),
        100,
    )
    .await;

    assert_eq!(
        compacted, 0,
        "a compaction candidate for a bucket this shard's current view no longer \
         owns is never applied"
    );
    assert!(
        s.fan_out.drain().is_empty(),
        "unlike an ordinary write, a skipped compaction candidate is never forwarded \
         to the bucket's new owners either"
    );
    let after = ShardOps::records_for(&s, vec![key_bytes(&key)]).await;
    assert_eq!(
        before, after,
        "the unowned bucket's resident record is left completely untouched"
    );
}

#[tokio::test]
async fn processes_the_whole_shard_over_several_rate_limited_calls() {
    const KEYS: u32 = 12;
    let s = shard::<u32, PnCounter>(1).with_resolver(Arc::new(PnCounterResolver));
    let dead = writer(9);
    let alive = writer(99);
    for k in 0..KEYS {
        s.merge(k, PnCounter::local_delta(dead, 1))
            .await
            .expect("merge");
        s.merge(k, PnCounter::local_delta(alive, 1))
            .await
            .expect("merge");
    }

    let retire = move |w: crdt::WriterId| w == dead;
    let mut total_compacted = 0usize;
    let mut all_retired: HashSet<crdt::WriterId> = HashSet::new();
    for _ in 0..BUCKET_COUNT {
        let CompactPassOutcome {
            retired, compacted, ..
        } = ShardOps::compact_pass(
            &s,
            1_000,
            &retire,
            Quiescence::Settled,
            CompactionBounds::three_bounds(1),
            3,
        )
        .await;
        total_compacted += compacted;
        all_retired.extend(retired);
        if total_compacted >= KEYS as usize {
            break;
        }
    }

    assert_eq!(
        total_compacted, KEYS as usize,
        "a small per-tick budget still processes every candidate, given enough \
         rate-limited calls — the returned counts sum to exactly the work actually \
         done, never more and never less"
    );
    assert_eq!(
        all_retired,
        HashSet::from([dead]),
        "the same writer, dead in every one of the twelve records, is reported \
         exactly once across the whole run, not once per record"
    );
    for k in 0..KEYS {
        assert_eq!(
            s.get(&k).await.map(|c| c.value()),
            Some(2),
            "key {k}'s value is unaffected by compaction"
        );
    }
}

#[cfg(feature = "spill")]
#[tokio::test]
async fn never_hands_a_spilled_entry_to_the_resolver() {
    use crate::store::spill::SpillLoc;

    let s = shard::<u32, PnCounter>(1).with_resolver(Arc::new(PnCounterResolver));
    let dead = writer(9);

    // An ordinary resident entry.
    s.merge(2, PnCounter::local_delta(dead, 4))
        .await
        .expect("merge");

    // A fabricated spilled entry for the very same dead writer,
    // bypassing any real spill tier: `Engine::compact` (and
    // therefore `Shard::compact_pass`, which never reads a stripe
    // directly) must skip it regardless of what its bytes would
    // decode to.
    s.engine.debug_insert_spilled(
        &key_bytes(&1u32),
        hlc(1, 1),
        None,
        SpillLoc {
            region: 0,
            offset: 0,
            len: 4,
            generation: 0,
        },
        0,
    );

    let retire = move |w: crdt::WriterId| w == dead;
    let CompactPassOutcome {
        retired, compacted, ..
    } = ShardOps::compact_pass(
        &s,
        1_000,
        &retire,
        Quiescence::Settled,
        CompactionBounds::three_bounds(1),
        100,
    )
    .await;

    assert_eq!(
        compacted, 1,
        "the spilled entry is skipped; only the resident one is compacted"
    );
    assert_eq!(retired, vec![dead]);
    assert_eq!(
        s.get(&2).await.map(|c| c.value()),
        Some(4),
        "the resident counter's own value is unchanged by compaction"
    );
}

/// Two replicas, holding the same pre-compaction state,
/// independently decide to retire a *different* writer each,
/// through this task's own real `Shard::compact_pass` plumbing, not
/// `PnCounter::compact` called directly. Cross-applying each
/// shard's resulting record through the other's ordinary
/// replicated-apply path converges both to the exact combined
/// total, and to bit-identical resident state, not merely an equal
/// `.value()`.
#[tokio::test]
/// Two replicas compacting byte-identical records at the same
/// version mint the same version, so their digests keep agreeing
/// and anti-entropy has nothing to exchange after a pass.
async fn two_replicas_compacting_identical_records_mint_identical_versions() {
    let a = shard::<u32, PnCounter>(1).with_resolver(Arc::new(PnCounterResolver));
    let b = shard::<u32, PnCounter>(2).with_resolver(Arc::new(PnCounterResolver));
    let dead = writer(9);
    let alive = writer(99);
    for k in 0..5u32 {
        a.merge(k, PnCounter::local_delta(dead, 3))
            .await
            .expect("merge");
        a.merge(k, PnCounter::local_delta(alive, 4))
            .await
            .expect("merge");
    }
    let keys: Vec<Bytes> = (0..5u32).map(|k| key_bytes(&k)).collect();
    for rec in ShardOps::records_for(&a, keys).await {
        ShardOps::apply_remote(&b, rec).await;
    }
    assert_eq!(
        ShardOps::digests(&a).await,
        ShardOps::digests(&b).await,
        "b warmed from a's records, versions included"
    );

    let retire = move |w: crdt::WriterId| w == dead;
    let bounds = CompactionBounds::three_bounds(1);
    let outcome_a =
        ShardOps::compact_pass(&a, 1_000, &retire, Quiescence::Settled, bounds, 100).await;
    let outcome_b =
        ShardOps::compact_pass(&b, 1_000, &retire, Quiescence::Settled, bounds, 100).await;
    assert_eq!((outcome_a.compacted, outcome_b.compacted), (5, 5));
    assert_eq!(
        ShardOps::digests(&a).await,
        ShardOps::digests(&b).await,
        "the same compaction on the same records mints the same versions on both"
    );
}

#[tokio::test]
/// A merge apply drops a fold receipt older than the receipt ttl even
/// when a peer's copy carries it back in, so two replicas whose sweeps
/// prune at different moments still converge on the receipt-free
/// bytes instead of re-importing each other's receipts forever.
async fn merge_apply_settles_an_aged_receipt_a_peer_carries_back_in() {
    let bounds = CompactionBounds::three_bounds(1_000);
    let now = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let clock = {
        let now = Arc::clone(&now);
        Arc::new(move || now.load(std::sync::atomic::Ordering::Relaxed))
            as Arc<dyn Fn() -> u64 + Send + Sync>
    };
    let s = shard::<u32, PnCounter>(1)
        .with_resolver(Arc::new(PnCounterResolver))
        .with_crdt_bounds(bounds)
        .with_clock(clock);
    let dead = writer(9);
    let alive = writer(99);
    s.merge(1, PnCounter::local_delta(dead, 3))
        .await
        .expect("merge");
    s.merge(1, PnCounter::local_delta(alive, 4))
        .await
        .expect("merge");

    let retire = move |w: crdt::WriterId| w == dead;
    // Stage one at 1_000, stage two past two bounds: a receipt stamped
    // with the fold instant, 3_001.
    ShardOps::compact_pass(&s, 1_000, &retire, Quiescence::Settled, bounds, 100).await;
    let folded = ShardOps::compact_pass(&s, 3_001, &retire, Quiescence::Settled, bounds, 100).await;
    assert_eq!(folded.compacted, 1, "folded with a receipt");
    let with_receipt = ShardOps::records_for(&s, vec![key_bytes(&1u32)])
        .await
        .into_iter()
        .next()
        .expect("resident");
    // Stage three past three bounds of the fold: the receipt is gone locally.
    let pruned = ShardOps::compact_pass(&s, 6_002, &retire, Quiescence::Settled, bounds, 100).await;
    assert_eq!(pruned.compacted, 1, "receipt pruned");
    let pruned_bytes = s.get(&1).await.expect("resident").encode().expect("encode");

    // A peer's copy still carrying the receipt merges in at a time
    // past the ttl: the merge unions the receipt, settle drops it.
    now.store(6_002, std::sync::atomic::Ordering::Relaxed);
    ShardOps::apply_remote(&s, with_receipt).await;
    let after = s.get(&1).await.expect("resident");
    assert_eq!(after.value(), 7, "the exact total is untouched");
    assert_eq!(
        after.encode().expect("encode"),
        pruned_bytes,
        "the receipt the peer carried back in is settled away on apply"
    );
}

#[tokio::test]
/// A record that another write touched between the scan and the
/// version-gated apply is skipped that pass and rewritten on the
/// next: the writer stays eligible, so nothing is left behind.
async fn a_record_touched_between_scan_and_apply_is_retried_on_the_next_pass() {
    let s = shard::<u32, PnCounter>(1).with_resolver(Arc::new(PnCounterResolver));
    let dead = writer(9);
    let alive = writer(99);
    s.merge(1, PnCounter::local_delta(dead, 5))
        .await
        .expect("merge");
    s.merge(1, PnCounter::local_delta(alive, 1))
        .await
        .expect("merge");
    let retire = move |w: crdt::WriterId| w == dead;
    let bounds = CompactionBounds::three_bounds(1);

    let scan = s.engine.compact(
        s.resolver.as_ref(),
        1_000,
        &retire,
        Quiescence::Settled,
        bounds,
        100,
    );
    let engine::CompactCandidate {
        key,
        key_bytes,
        stale_ver,
        new_encoded,
    } = scan
        .candidates
        .into_iter()
        .next()
        .expect("the dead writer's record is a candidate");
    // A concurrent write lands before the apply.
    s.merge(1, PnCounter::local_delta(alive, 2))
        .await
        .expect("merge");
    let value = PnCounter::decode(&new_encoded).expect("decodes");
    let applied = s.engine.compact_replace_if_current(engine::CompactReplace {
        key: &key,
        key_bytes: key_bytes.as_ref(),
        hash: engine::hash_key_bytes(key_bytes.as_ref()),
        expected_ver: stale_ver,
        new_ver: compacted_version(stale_ver, new_encoded.as_ref()),
        value,
        encoded: new_encoded,
    });
    assert!(!applied, "the stale candidate is refused");
    assert_eq!(s.get(&1).await.map(|c| c.value()), Some(7));

    let next = ShardOps::compact_pass(&s, 1_001, &retire, Quiescence::Settled, bounds, 100).await;
    assert_eq!(next.retired, vec![dead], "still eligible on the next pass");
    assert_eq!(
        next.compacted, 1,
        "rewritten with the concurrent write folded in"
    );
    assert_eq!(s.get(&1).await.map(|c| c.value()), Some(7));
}

#[tokio::test]
async fn two_shards_compacting_independently_converge() {
    let a = shard::<u32, PnCounter>(1).with_resolver(Arc::new(PnCounterResolver));
    let b = shard::<u32, PnCounter>(2).with_resolver(Arc::new(PnCounterResolver));
    let w1 = writer(101);
    let w2 = writer(202);

    for shard in [&a, &b] {
        shard
            .merge(1, PnCounter::local_delta(w1, 10))
            .await
            .expect("w1's contribution");
        shard
            .merge(1, PnCounter::local_delta(w2, 50))
            .await
            .expect("w2's contribution");
    }
    assert_eq!(a.get(&1).await.map(|c| c.value()), Some(60));
    assert_eq!(b.get(&1).await.map(|c| c.value()), Some(60));

    // Independent decisions: `a` alone judges `w1` dead, `b` alone
    // judges `w2` dead, with the same tick timestamp on both sides
    // so the two sides' `retired` entries land at the identical
    // `since_ms`.
    let retire_w1 = move |w: crdt::WriterId| w == w1;
    let retire_w2 = move |w: crdt::WriterId| w == w2;
    let CompactPassOutcome {
        retired: retired_a,
        compacted: compacted_a,
        ..
    } = ShardOps::compact_pass(
        &a,
        5_000,
        &retire_w1,
        Quiescence::Settled,
        CompactionBounds::three_bounds(1),
        100,
    )
    .await;
    let CompactPassOutcome {
        retired: retired_b,
        compacted: compacted_b,
        ..
    } = ShardOps::compact_pass(
        &b,
        5_000,
        &retire_w2,
        Quiescence::Settled,
        CompactionBounds::three_bounds(1),
        100,
    )
    .await;
    assert_eq!(retired_a, vec![w1]);
    assert_eq!(compacted_a, 1);
    assert_eq!(retired_b, vec![w2]);
    assert_eq!(compacted_b, 1);

    // Each side still reads its own exact total after compacting
    // only its own writer.
    assert_eq!(a.get(&1).await.map(|c| c.value()), Some(60));
    assert_eq!(b.get(&1).await.map(|c| c.value()), Some(60));

    let rec_a = ShardOps::records_for(&a, vec![key_bytes(&1u32)])
        .await
        .into_iter()
        .next()
        .expect("a holds the key");
    let rec_b = ShardOps::records_for(&b, vec![key_bytes(&1u32)])
        .await
        .into_iter()
        .next()
        .expect("b holds the key");

    ShardOps::apply_remote(&a, rec_b).await;
    ShardOps::apply_remote(&b, rec_a).await;

    assert_eq!(
        a.get(&1).await.map(|c| c.value()),
        Some(60),
        "cross-applying the other side's independently-compacted record recovers \
         the exact combined total on a"
    );
    assert_eq!(
        b.get(&1).await.map(|c| c.value()),
        Some(60),
        "cross-applying the other side's independently-compacted record recovers \
         the exact combined total on b"
    );
    assert_eq!(
        a.get(&1).await,
        b.get(&1).await,
        "both replicas converge to bit-identical state, not merely an equal value: \
         each now carries both writers' slots folded into its own retired map"
    );
}

/// A clock the test moves by hand, for the shards whose merge apply settles
/// receipts against it.
fn manual_clock() -> (
    Arc<std::sync::atomic::AtomicU64>,
    Arc<dyn Fn() -> u64 + Send + Sync>,
) {
    let now = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let clock = {
        let now = Arc::clone(&now);
        Arc::new(move || now.load(std::sync::atomic::Ordering::Relaxed))
            as Arc<dyn Fn() -> u64 + Send + Sync>
    };
    (now, clock)
}

/// A fold that membership churn delays past the receipt lifetime leaves a
/// receipt the merge apply keeps: replicas that retired the writer but did
/// not fold it, and merge in the folded record one after the other, count
/// the writer once.
#[tokio::test]
async fn a_late_fold_leaves_a_receipt_the_merge_apply_keeps() {
    let bounds = CompactionBounds::three_bounds(1_000);
    let (now, clock) = manual_clock();
    let open = |node: u64| {
        shard::<u32, PnCounter>(node)
            .with_resolver(Arc::new(PnCounterResolver))
            .with_crdt_bounds(bounds)
            .with_clock(Arc::clone(&clock))
    };
    let (s, first_peer, second_peer) = (open(1), open(2), open(3));
    let dead = writer(9);
    let alive = writer(99);
    let retire = move |w: crdt::WriterId| w == dead;
    let mut retired_records = Vec::new();
    for shard in [&s, &first_peer, &second_peer] {
        shard
            .merge(1, PnCounter::local_delta(dead, 3))
            .await
            .expect("merge");
        shard
            .merge(1, PnCounter::local_delta(alive, 4))
            .await
            .expect("merge");
        ShardOps::compact_pass(shard, 1_000, &retire, Quiescence::Churning, bounds, 100).await;
    }
    for peer in [&first_peer, &second_peer] {
        retired_records.push(
            ShardOps::records_for(peer, vec![key_bytes(&1u32)])
                .await
                .into_iter()
                .next()
                .expect("the peer holds the key"),
        );
    }

    // The fold waits for the cache to be quiet, far past three bounds of
    // the retirement.
    let folded =
        ShardOps::compact_pass(&s, 20_000, &retire, Quiescence::Settled, bounds, 100).await;
    assert_eq!(folded.compacted, 1, "folded with a receipt");
    let folded_bytes = s.get(&1).await.expect("resident").encode().expect("encode");

    now.store(20_001, std::sync::atomic::Ordering::Relaxed);
    for record in retired_records {
        ShardOps::apply_remote(&s, record).await;
        let after = s.get(&1).await.expect("resident");
        assert_eq!(
            after.value(),
            7,
            "the writer a peer still carries as retired is counted once"
        );
        assert_eq!(
            after.encode().expect("encode"),
            folded_bytes,
            "the receipt survived the merge apply and absorbed the peer's retired entry"
        );
    }
}

/// Two shards that fold one writer at different instants carry different
/// receipt stamps; one exchange both ways leaves them identical, bytes and
/// versions, with the exact total.
#[tokio::test]
async fn two_shards_folding_at_different_instants_converge_in_one_exchange() {
    let bounds = CompactionBounds::three_bounds(1_000);
    let (now, clock) = manual_clock();
    let a = shard::<u32, PnCounter>(1)
        .with_resolver(Arc::new(PnCounterResolver))
        .with_crdt_bounds(bounds)
        .with_clock(Arc::clone(&clock));
    let b = shard::<u32, PnCounter>(2)
        .with_resolver(Arc::new(PnCounterResolver))
        .with_crdt_bounds(bounds)
        .with_clock(clock);
    let dead = writer(9);
    let alive = writer(99);
    for shard in [&a, &b] {
        shard
            .merge(1, PnCounter::local_delta(dead, 3))
            .await
            .expect("merge");
        shard
            .merge(1, PnCounter::local_delta(alive, 4))
            .await
            .expect("merge");
    }
    let retire = move |w: crdt::WriterId| w == dead;
    for shard in [&a, &b] {
        ShardOps::compact_pass(shard, 1_000, &retire, Quiescence::Churning, bounds, 100).await;
    }
    ShardOps::compact_pass(&a, 3_001, &retire, Quiescence::Settled, bounds, 100).await;
    ShardOps::compact_pass(&b, 3_500, &retire, Quiescence::Settled, bounds, 100).await;
    assert_ne!(
        a.get(&1).await.expect("resident").encode().expect("encode"),
        b.get(&1).await.expect("resident").encode().expect("encode"),
        "the receipts carry the two fold instants"
    );

    now.store(3_600, std::sync::atomic::Ordering::Relaxed);
    let rec_a = ShardOps::records_for(&a, vec![key_bytes(&1u32)])
        .await
        .into_iter()
        .next()
        .expect("a holds the key");
    let rec_b = ShardOps::records_for(&b, vec![key_bytes(&1u32)])
        .await
        .into_iter()
        .next()
        .expect("b holds the key");
    ShardOps::apply_remote(&a, rec_b).await;
    ShardOps::apply_remote(&b, rec_a).await;

    assert_eq!(a.get(&1).await.map(|c| c.value()), Some(7));
    assert_eq!(a.get(&1).await, b.get(&1).await, "identical bytes");
    assert_eq!(
        ShardOps::digests(&a).await,
        ShardOps::digests(&b).await,
        "identical versions: nothing is left for anti-entropy to exchange"
    );
}

/// An `apply_many` batch that carries two receipts for one writer, stamped
/// by two folds, merges to the same record whatever the order.
#[tokio::test]
async fn a_batch_with_two_fold_stamps_for_one_writer_applies_in_either_order() {
    let bounds = CompactionBounds::three_bounds(1_000);
    let dead = writer(9);
    let alive = writer(99);
    let retire = move |w: crdt::WriterId| w == dead;
    let mut records = Vec::new();
    for (node, fold_at) in [(1u64, 3_001u64), (2, 3_500)] {
        let (_, source_clock) = manual_clock();
        let source = shard::<u32, PnCounter>(node)
            .with_resolver(Arc::new(PnCounterResolver))
            .with_clock(source_clock);
        source
            .merge(1, PnCounter::local_delta(dead, 3))
            .await
            .expect("merge");
        source
            .merge(1, PnCounter::local_delta(alive, 4))
            .await
            .expect("merge");
        ShardOps::compact_pass(&source, 1_000, &retire, Quiescence::Churning, bounds, 100).await;
        ShardOps::compact_pass(&source, fold_at, &retire, Quiescence::Settled, bounds, 100).await;
        records.push(
            ShardOps::records_for(&source, vec![key_bytes(&1u32)])
                .await
                .into_iter()
                .next()
                .expect("the source holds the key"),
        );
    }
    let mut results = Vec::new();
    for batch in [
        records.clone(),
        records.into_iter().rev().collect::<Vec<_>>(),
    ] {
        let (now, clock) = manual_clock();
        now.store(3_600, std::sync::atomic::Ordering::Relaxed);
        let target = shard::<u32, PnCounter>(3)
            .with_resolver(Arc::new(PnCounterResolver))
            .with_crdt_bounds(bounds)
            .with_clock(clock);
        ShardOps::apply_remote_batch(&target, batch).await;
        results.push(target.get(&1).await.expect("resident"));
    }
    assert_eq!(results[0].value(), 7);
    assert_eq!(results[0], results[1], "the batch order does not matter");
}

/// The `OrSet` counterpart: a fold that membership churn delays past the
/// receipt lifetime leaves a receipt the merge apply keeps, so stale
/// replicas that still hold a removed element, and retired the writer
/// without folding it, do not bring the element back one after the other.
#[tokio::test]
async fn an_or_set_late_fold_leaves_a_receipt_the_merge_apply_keeps() {
    let bounds = CompactionBounds::three_bounds(1_000);
    let (now, clock) = manual_clock();
    let open = |node: u64| {
        shard::<u32, OrSet<String>>(node)
            .with_resolver(Arc::new(OrSetResolver::<String>::new()))
            .with_crdt_bounds(bounds)
            .with_clock(Arc::clone(&clock))
    };
    let (s, first_stale, second_stale) = (open(1), open(2), open(3));
    let dead = writer(9);
    let x = "x".to_string();
    for shard in [&s, &first_stale, &second_stale] {
        shard
            .merge(1, OrSet::add(dead, 0, x.clone()))
            .await
            .expect("add");
    }
    // Only `s` sees the remove.
    let observed = s.get(&1).await.expect("resident");
    s.merge(1, OrSet::remove(&observed, &x))
        .await
        .expect("remove");
    assert!(!s.get(&1).await.expect("resident").contains(&x));

    let retire = move |w: crdt::WriterId| w == dead;
    let mut stale_records = Vec::new();
    for shard in [&s, &first_stale, &second_stale] {
        ShardOps::compact_pass(shard, 1_000, &retire, Quiescence::Churning, bounds, 100).await;
    }
    for stale in [&first_stale, &second_stale] {
        assert!(stale.get(&1).await.expect("resident").contains(&x));
        stale_records.push(
            ShardOps::records_for(stale, vec![key_bytes(&1u32)])
                .await
                .into_iter()
                .next()
                .expect("the stale replica holds the key"),
        );
    }

    let folded =
        ShardOps::compact_pass(&s, 20_000, &retire, Quiescence::Settled, bounds, 100).await;
    assert_eq!(folded.compacted, 1, "folded with a receipt");

    now.store(20_001, std::sync::atomic::Ordering::Relaxed);
    for record in stale_records {
        ShardOps::apply_remote(&s, record).await;
        assert!(
            !s.get(&1).await.expect("resident").contains(&x),
            "the removed element stays absent"
        );
    }
}

/// Only `Invalidation` declines to fold: nodes there hold different
/// writers, so two nodes' folds can be of disjoint writer sets, which the
/// folded totals' merge cannot add up.
#[test]
fn only_an_invalidation_cache_declines_to_fold_retired_writers() {
    assert!(Mode::Local.folds_retired_writers());
    assert!(!Mode::Invalidation.folds_retired_writers());
    assert!(Mode::Replicated.folds_retired_writers());
    assert!(Mode::distributed().folds_retired_writers());
}

fn shard_in<V>(mode: Mode, node: u64) -> Shard<u32, V>
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    Shard::new(
        SmolStr::new("test"),
        mode,
        NodeId::from(node),
        10_000,
        None,
        None,
    )
}

/// The sweep retires a dead writer in every mode, and folds it into the
/// bounded total in every mode but `Invalidation`, whatever `quiet` the
/// caller passes: the retired entry stays, exact per writer, past twice the
/// bound.
#[tokio::test]
async fn an_invalidation_counter_keeps_a_retired_entry_unfolded_past_twice_the_bound() {
    let bounds = CompactionBounds::three_bounds(1_000);
    let dead = writer(9);
    let alive = writer(99);
    let retire = move |w: crdt::WriterId| w == dead;
    for (mode, folds) in [
        (Mode::Invalidation, false),
        (Mode::Replicated, true),
        (Mode::distributed(), true),
        (Mode::Local, true),
    ] {
        let s = shard_in::<PnCounter>(mode, 1).with_resolver(Arc::new(PnCounterResolver));
        s.merge(1, PnCounter::local_delta(dead, 3))
            .await
            .expect("merge");
        s.merge(1, PnCounter::local_delta(alive, 4))
            .await
            .expect("merge");
        let retired =
            ShardOps::compact_pass(&s, 1_000, &retire, Quiescence::Settled, bounds, 100).await;
        assert_eq!(
            retired.retired,
            vec![dead],
            "{mode:?} retires the dead writer"
        );
        assert_eq!(retired.compacted, 1, "{mode:?}");
        let retired_bytes = s.get(&1).await.expect("resident").encode().expect("encode");

        // Far past twice the bound, with every peer quiet.
        let later =
            ShardOps::compact_pass(&s, 100_000, &retire, Quiescence::Settled, bounds, 100).await;
        let bytes = s.get(&1).await.expect("resident").encode().expect("encode");
        assert_eq!(s.get(&1).await.map(|c| c.value()), Some(7), "{mode:?}");
        if folds {
            assert_eq!(later.compacted, 1, "{mode:?} folds the retired writer");
            assert_ne!(bytes, retired_bytes, "{mode:?}");
        } else {
            assert_eq!(
                later.compacted, 0,
                "{mode:?} leaves the retired entry alone"
            );
            assert_eq!(bytes, retired_bytes, "{mode:?}");
        }
    }
}

/// The same for `OrSet`: an `Invalidation` cache keeps the retirement.
#[tokio::test]
async fn an_invalidation_or_set_keeps_a_retirement_unfolded_past_twice_the_bound() {
    let bounds = CompactionBounds::three_bounds(1_000);
    let dead = writer(9);
    let retire = move |w: crdt::WriterId| w == dead;
    for (mode, folds) in [(Mode::Invalidation, false), (Mode::Replicated, true)] {
        let s = shard_in::<OrSet<String>>(mode, 1)
            .with_resolver(Arc::new(OrSetResolver::<String>::new()));
        s.merge(1, OrSet::add(dead, 0, "x".to_string()))
            .await
            .expect("add");
        let retired =
            ShardOps::compact_pass(&s, 1_000, &retire, Quiescence::Settled, bounds, 100).await;
        assert_eq!(retired.retired, vec![dead], "{mode:?}");
        let retired_bytes = s.get(&1).await.expect("resident").encode().expect("encode");

        let later =
            ShardOps::compact_pass(&s, 100_000, &retire, Quiescence::Settled, bounds, 100).await;
        let bytes = s.get(&1).await.expect("resident").encode().expect("encode");
        assert!(
            s.get(&1)
                .await
                .expect("resident")
                .contains(&"x".to_string())
        );
        assert_eq!(later.compacted, usize::from(folds), "{mode:?}");
        assert_eq!(bytes == retired_bytes, !folds, "{mode:?}");
    }
}

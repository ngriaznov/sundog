//! Shard-level `compact_pass` plumbing tests.

use std::sync::atomic::AtomicUsize;

use super::*;
use crate::store::crdt::{PnCounter, PnCounterResolver};

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
    // Stage one at 1_000, stage two past two bounds: a receipt at 1_000.
    ShardOps::compact_pass(&s, 1_000, &retire, Quiescence::Settled, bounds, 100).await;
    let folded = ShardOps::compact_pass(&s, 3_001, &retire, Quiescence::Settled, bounds, 100).await;
    assert_eq!(folded.compacted, 1, "folded with a receipt");
    let with_receipt = ShardOps::records_for(&s, vec![key_bytes(&1u32)])
        .await
        .into_iter()
        .next()
        .expect("resident");
    // Stage three past three bounds: the receipt is gone locally.
    let pruned = ShardOps::compact_pass(&s, 4_001, &retire, Quiescence::Settled, bounds, 100).await;
    assert_eq!(pruned.compacted, 1, "receipt pruned");
    let pruned_bytes = s.get(&1).await.expect("resident").encode().expect("encode");

    // A peer's copy still carrying the receipt merges in at a time
    // past the ttl: the merge unions the receipt, settle drops it.
    now.store(4_001, std::sync::atomic::Ordering::Relaxed);
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

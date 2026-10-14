//! The fan-out's written view: what a drain reads back for the writes it queued, and which of them it reports departed.

use std::sync::atomic::{AtomicU64, Ordering};

use super::*;
use crate::store::crdt::{PnCounter, PnCounterResolver, WriterId};
use crate::store::fan_out::{fan_out_ships_values, partition_writes, spilled_needs_read};

const T0: u64 = 1_000_000;

/// A shard in `mode` on a clock the test sets.
fn clocked(
    mode: Mode,
    max_capacity: u64,
    tti: Option<Duration>,
    clock: &Arc<AtomicU64>,
) -> Shard<u32, String> {
    let reader = Arc::clone(clock);
    Shard::<u32, String>::new(
        SmolStr::new("test"),
        mode,
        NodeId::from(1),
        max_capacity,
        None,
        tti,
    )
    .with_clock(Arc::new(move || reader.load(Ordering::SeqCst)))
}

fn replicated(clock: &Arc<AtomicU64>) -> Shard<u32, String> {
    clocked(Mode::Replicated, u64::MAX, None, clock)
}

fn clock_at(ms: u64) -> Arc<AtomicU64> {
    Arc::new(AtomicU64::new(ms))
}

/// The drained queue as `fan_out_batch` folds it: one newest write per key.
fn drained(s: &Shard<u32, String>) -> Vec<(u32, Hlc)> {
    newest_per_key(applied_writes(s.fan_out_queue().drain()))
}

fn key_of(rec: &WireRecord) -> u32 {
    postcard::from_bytes(&rec.key).expect("test key decodes")
}

fn wire(key: u32, ver: Hlc) -> WireRecord {
    WireRecord {
        key: key_bytes(&key),
        value: Some(Bytes::from_static(b"\x01v")),
        ver,
        expires_at_ms: None,
    }
}

fn write(key: u32, ver: Hlc) -> (Bytes, Hlc) {
    (key_bytes(&key), ver)
}

// -- pure: newest_per_key -----------------------------------------------------

#[test]
fn newest_per_key_keeps_the_highest_version_in_first_seen_order() {
    let items = vec![
        (2u32, hlc(5, 1)),
        (1, hlc(3, 1)),
        (2, hlc(9, 1)),
        (3, hlc(1, 1)),
        (1, hlc(2, 1)),
        (2, hlc(7, 1)),
    ];
    assert_eq!(
        newest_per_key(items),
        vec![(2, hlc(9, 1)), (1, hlc(3, 1)), (3, hlc(1, 1))]
    );
}

#[test]
fn newest_per_key_leaves_distinct_keys_and_empty_input_alone() {
    assert_eq!(newest_per_key(Vec::<(u32, Hlc)>::new()), Vec::new());
    let distinct = vec![(1u32, hlc(1, 1)), (2, hlc(1, 1)), (3, hlc(1, 1))];
    assert_eq!(newest_per_key(distinct.clone()), distinct);
}

// -- pure: partition_writes ---------------------------------------------------

#[test]
fn a_write_with_no_fetched_record_is_departed_at_its_version() {
    let split = partition_writes(&[write(1, hlc(4, 1))], Vec::new());
    assert!(split.records.is_empty(), "nothing expected");
    assert_eq!(split.departed.len(), 1);
    let gone = &split.departed[0];
    assert_eq!(gone.key, key_bytes(&1u32));
    assert_eq!(gone.ver, hlc(4, 1));
    assert!(gone.value.is_none(), "a departed carrier holds no value");
    assert_eq!(gone.expires_at_ms, None);
}

#[test]
fn a_fetched_record_at_or_past_the_written_version_is_not_departed() {
    let equal = wire(1, hlc(4, 1));
    let newer = wire(2, hlc(9, 1));
    let split = partition_writes(
        &[write(1, hlc(4, 1)), write(2, hlc(4, 1))],
        vec![equal.clone(), newer.clone()],
    );
    assert_eq!(split.records, vec![equal, newer]);
    assert!(split.departed.is_empty(), "nothing expected");
}

#[test]
fn a_fetched_record_older_than_the_write_is_departed_and_never_sent() {
    let older = wire(1, hlc(2, 1));
    let current = wire(2, hlc(4, 1));
    let split = partition_writes(
        &[write(1, hlc(4, 1)), write(2, hlc(4, 1))],
        vec![older, current.clone()],
    );
    assert_eq!(
        split.records,
        vec![current],
        "the older copy would reinstall, on a peer, the value the invalidation dropped"
    );
    assert_eq!(split.departed.len(), 1);
    assert_eq!(split.departed[0].key, key_bytes(&1u32));
    assert_eq!(split.departed[0].ver, hlc(4, 1));
}

#[test]
fn a_tombstone_at_a_newer_version_covers_the_write() {
    let tombstone = WireRecord {
        value: None,
        ..wire(1, hlc(8, 1))
    };
    let split = partition_writes(&[write(1, hlc(4, 1))], vec![tombstone.clone()]);
    assert_eq!(split.records, vec![tombstone]);
    assert!(split.departed.is_empty(), "nothing expected");
}

#[test]
fn a_key_queued_twice_is_departed_once_at_its_highest_version() {
    let split = partition_writes(
        &[
            write(1, hlc(4, 1)),
            write(1, hlc(6, 1)),
            write(1, hlc(5, 1)),
        ],
        Vec::new(),
    );
    assert_eq!(split.departed.len(), 1);
    assert_eq!(split.departed[0].ver, hlc(6, 1));
}

#[test]
fn partitioning_nothing_yields_nothing() {
    let split = partition_writes(&[], Vec::new());
    assert!(
        split.records.is_empty() && split.departed.is_empty(),
        "nothing expected"
    );
}

// -- pure: what a drain ships ---------------------------------------------------

#[test]
fn only_a_non_merging_invalidation_cache_ships_no_values() {
    let owners = NonZeroU8::new(2).expect("nonzero");
    for merges in [false, true] {
        assert_eq!(
            fan_out_ships_values(Mode::Invalidation, merges),
            merges,
            "Invalidation ships values only when the resolver merges"
        );
        assert!(fan_out_ships_values(Mode::Replicated, merges));
        assert!(fan_out_ships_values(Mode::Distributed { owners }, merges));
        assert!(fan_out_ships_values(Mode::Local, merges));
    }
}

#[test]
fn a_spilled_entry_is_read_only_when_a_value_would_be_sent_and_has_not_lapsed() {
    assert!(spilled_needs_read(true, None, 100));
    assert!(spilled_needs_read(true, Some(101), 100));
    assert!(
        !spilled_needs_read(true, Some(100), 100),
        "an entry at its deadline has lapsed"
    );
    assert!(!spilled_needs_read(true, Some(50), 100));
    assert!(!spilled_needs_read(false, None, 100));
    assert!(!spilled_needs_read(false, Some(101), 100));
}

// -- Shard: the queue carries the stored version --------------------------------

#[tokio::test]
async fn every_local_write_queues_the_version_it_is_stored_at() {
    let clock = clock_at(T0);
    let s = replicated(&clock);

    s.insert(1, "a".into()).await.expect("insert");
    let stored = ShardOps::records_for(&s, vec![key_bytes(&1u32)]).await;
    assert_eq!(
        applied_writes(s.fan_out_queue().drain()),
        vec![(1, stored[0].ver)],
        "insert"
    );

    s.insert_many([(2, "b".to_string()), (3, "c".to_string())])
        .await
        .expect("insert_many");
    let stored = ShardOps::records_for(&s, vec![key_bytes(&2u32), key_bytes(&3u32)]).await;
    let mut queued = applied_writes(s.fan_out_queue().drain());
    queued.sort_unstable_by_key(|&(key, _)| key);
    assert_eq!(
        queued,
        vec![(2, stored[0].ver), (3, stored[1].ver)],
        "insert_many"
    );

    s.remove(&1).await.expect("remove");
    let tombstone = ShardOps::records_for(&s, vec![key_bytes(&1u32)]).await;
    assert_eq!(
        applied_writes(s.fan_out_queue().drain()),
        vec![(1, tombstone[0].ver)],
        "remove queues the tombstone's version"
    );

    s.remove_many([2, 3]).await.expect("remove_many");
    let tombstones = ShardOps::records_for(&s, vec![key_bytes(&2u32), key_bytes(&3u32)]).await;
    let mut queued = applied_writes(s.fan_out_queue().drain());
    queued.sort_unstable_by_key(|&(key, _)| key);
    assert_eq!(
        queued,
        vec![(2, tombstones[0].ver), (3, tombstones[1].ver)],
        "remove_many"
    );
}

#[tokio::test]
async fn expire_touch_and_persist_queue_the_rearmed_version() {
    let clock = clock_at(T0);
    let s = replicated(&clock);
    s.insert(1, "a".into()).await.expect("insert");
    let _ = s.fan_out_queue().drain();

    let rearmed = s.expire(&1, Duration::from_secs(30)).await;
    assert_queued_at_stored_version(&s, "expire", rearmed).await;
    let rearmed = s.touch(&1).await;
    assert_queued_at_stored_version(&s, "touch", rearmed).await;
    let rearmed = s.persist(&1).await;
    assert_queued_at_stored_version(&s, "persist", rearmed).await;
}

async fn assert_queued_at_stored_version(
    s: &Shard<u32, String>,
    label: &str,
    rearmed: Result<bool, crate::error::CacheError>,
) {
    assert!(rearmed.expect(label), "{label} finds the key");
    let stored = ShardOps::records_for(s, vec![key_bytes(&1u32)]).await;
    assert_eq!(
        applied_writes(s.fan_out_queue().drain()),
        vec![(1, stored[0].ver)],
        "{label}"
    );
}

#[tokio::test]
async fn a_replicated_fill_queues_the_version_it_is_stored_at() {
    let s = shard::<u32, String>(1);
    s.get_or_load(
        &7,
        async |_key: &u32| -> Result<String, std::convert::Infallible> { Ok("loaded".into()) },
    )
    .await
    .expect("load");
    let stored = ShardOps::records_for(&s, vec![key_bytes(&7u32)]).await;
    assert_eq!(
        applied_writes(s.fan_out_queue().drain()),
        vec![(7, stored[0].ver)]
    );
}

// -- Shard::fan_out_records ---------------------------------------------------

#[tokio::test]
async fn an_expire_to_zero_still_fans_out_its_lapsed_record() {
    let clock = clock_at(T0);
    let a = replicated(&clock);
    a.insert(1, "a".into()).await.expect("insert");
    assert!(a.expire(&1, Duration::ZERO).await.expect("expire"));
    let writes = drained(&a);

    let fan = a.fan_out_records(&writes).await;

    assert!(fan.departed.is_empty(), "nothing expected");
    assert_eq!(fan.records.len(), 1, "the lapsed write travels as a record");
    let rec = &fan.records[0];
    assert_eq!(key_of(rec), 1);
    assert!(rec.value.is_some(), "the record keeps its value");
    assert_eq!(
        rec.expires_at_ms,
        Some(T0),
        "it carries its (already past) deadline"
    );
    assert_eq!(rec.ver, writes[0].1, "at the version the write stored");
    assert!(
        a.records_for_typed(&[1u32]).await.is_empty(),
        "the readable view still hides the lapsed entry"
    );
}

#[tokio::test]
async fn an_insert_with_a_zero_ttl_still_fans_out_its_record() {
    let clock = clock_at(T0);
    let a = replicated(&clock);
    a.insert_with_ttl(1, "a".into(), Duration::ZERO)
        .await
        .expect("insert");
    let writes = drained(&a);

    let fan = a.fan_out_records(&writes).await;

    assert!(fan.departed.is_empty(), "nothing expected");
    assert_eq!(fan.records.len(), 1);
    assert_eq!(fan.records[0].expires_at_ms, Some(T0));
    assert_eq!(fan.records[0].ver, writes[0].1);
}

#[tokio::test]
async fn a_lapsed_record_replicated_to_a_peer_leaves_it_with_no_value() {
    let clock = clock_at(T0);
    let a = replicated(&clock);
    let b = replicated(&clock);
    a.insert(1, "old".into()).await.expect("insert");
    ShardOps::apply_remote_batch(&b, ShardOps::records_for(&a, vec![key_bytes(&1u32)]).await).await;
    assert_eq!(b.get(&1).await, Some("old".to_string()));
    let _ = a.fan_out_queue().drain();

    a.insert_with_ttl(1, "new".into(), Duration::ZERO)
        .await
        .expect("insert");
    let fan = a.fan_out_records(&drained(&a)).await;
    ShardOps::apply_remote_batch(&b, fan.records).await;

    assert_eq!(
        b.get(&1).await,
        None,
        "the peer drops the older value and installs the write already lapsed"
    );
}

#[tokio::test]
async fn a_lapsed_record_replicated_to_a_peer_leaves_the_digests_equal_through_both_sweeps() {
    let clock = clock_at(T0);
    let a = replicated(&clock);
    let b = replicated(&clock);
    a.insert(1, "old".into()).await.expect("insert");
    ShardOps::apply_remote_batch(&b, ShardOps::records_for(&a, vec![key_bytes(&1u32)]).await).await;
    let _ = a.fan_out_queue().drain();
    a.insert_with_ttl(1, "new".into(), Duration::ZERO)
        .await
        .expect("insert");
    let fan = a.fan_out_records(&drained(&a)).await;
    ShardOps::apply_remote_batch(&b, fan.records).await;

    assert_eq!(
        ShardOps::digests(&a).await,
        ShardOps::digests(&b).await,
        "writer and peer agree on the lapsed entry, so anti-entropy has nothing to repair"
    );

    clock.store(T0 + 1, Ordering::SeqCst);
    ShardOps::run_pending_tasks(&a).await;
    assert_eq!(
        ShardOps::digests(&a).await,
        ShardOps::digests(&b).await,
        "the writer's sweep leaves the digest alone while the peer has not swept"
    );
    ShardOps::run_pending_tasks(&b).await;
    assert_eq!(
        ShardOps::digests(&a).await,
        ShardOps::digests(&b).await,
        "both have swept"
    );
    assert_digest_matches_full_recompute(&a).await;
    assert_digest_matches_full_recompute(&b).await;
}

#[tokio::test]
async fn a_merging_zero_ttl_write_ships_what_the_writer_stores() {
    let clock = clock_at(T0);
    let reader = Arc::clone(&clock);
    let counter = |node: u64| {
        let reader = Arc::clone(&reader);
        Shard::<u32, PnCounter>::new(
            SmolStr::new("test"),
            Mode::Replicated,
            NodeId::from(node),
            u64::MAX,
            None,
            None,
        )
        .with_resolver(Arc::new(PnCounterResolver))
        .with_clock(Arc::new(move || reader.load(Ordering::SeqCst)))
    };
    let (a, b) = (counter(1), counter(2));
    let delta = |by: u64| PnCounter::local_delta(WriterId::new(NodeId::from(1), 1), by);
    a.merge(1, delta(5)).await.expect("merge");
    ShardOps::apply_remote_batch(&b, ShardOps::records_for(&a, vec![key_bytes(&1u32)]).await).await;
    let _ = a.fan_out_queue().drain();

    a.merge_with_ttl(1, delta(1), Duration::ZERO)
        .await
        .expect("merge");
    let writes = newest_per_key(applied_writes(a.fan_out_queue().drain()));
    let fan = a.fan_out_records(&writes).await;
    assert!(fan.departed.is_empty(), "nothing expected");
    ShardOps::apply_remote_batch(&b, fan.records).await;

    assert_eq!(
        b.get_sync(&1).map(|c| c.value()),
        a.get_sync(&1).map(|c| c.value()),
        "the peer reads what the writer reads, lapsed or not"
    );
}

#[tokio::test]
async fn an_evicted_write_is_departed_with_its_queued_version() {
    let s = Shard::<u32, String>::new(
        SmolStr::new("inv"),
        Mode::Invalidation,
        NodeId::from(1),
        1,
        None,
        None,
    );
    s.insert(1, "a".into()).await.expect("insert");
    s.insert(2, "b".into()).await.expect("insert");
    assert_eq!(s.entry_count().await, 1, "one entry fits");
    let writes = applied_writes(s.fan_out_queue().drain());
    assert_eq!(writes.len(), 2);

    let fan = s.fan_out_records(&writes).await;

    assert_eq!(fan.records.len(), 1, "the resident write");
    assert_eq!(key_of(&fan.records[0]), 2);
    assert_eq!(fan.departed.len(), 1, "the evicted write");
    assert_eq!(key_of(&fan.departed[0]), 1);
    assert_eq!(fan.departed[0].ver, writes[0].1, "at its queued version");
    assert!(fan.departed[0].value.is_none());
}

#[tokio::test]
async fn a_lapsed_write_swept_before_the_drain_ships_its_tombstone() {
    let clock = clock_at(T0);
    let a = replicated(&clock);
    a.insert_with_ttl(1, "a".into(), Duration::from_millis(50))
        .await
        .expect("insert");
    let writes = drained(&a);
    clock.store(T0 + 100, Ordering::SeqCst);
    ShardOps::run_pending_tasks(&a).await;

    let fan = a.fan_out_records(&writes).await;

    assert!(
        fan.departed.is_empty(),
        "a replicated sweep keeps the write's version as a tombstone, which ships"
    );
    assert_eq!(fan.records.len(), 1);
    assert_eq!(key_of(&fan.records[0]), 1);
    assert!(
        fan.records[0].value.is_none(),
        "a tombstone carries no value"
    );
    assert_eq!(fan.records[0].ver, writes[0].1);
    assert_eq!(
        fan.records[0].expires_at_ms,
        Some(T0 + 50),
        "it carries the expiry its retention counts from"
    );
}

#[tokio::test]
async fn a_lapsed_write_swept_before_the_drain_is_departed_where_a_sweep_keeps_nothing() {
    let clock = clock_at(T0);
    let a = clocked(Mode::Invalidation, u64::MAX, None, &clock);
    a.insert_with_ttl(1, "a".into(), Duration::from_millis(50))
        .await
        .expect("insert");
    let writes = drained(&a);
    clock.store(T0 + 100, Ordering::SeqCst);
    ShardOps::run_pending_tasks(&a).await;

    let fan = a.fan_out_records(&writes).await;

    assert!(fan.records.is_empty(), "nothing expected");
    assert_eq!(
        fan.departed.len(),
        1,
        "the swept write invalidates its peers"
    );
    assert_eq!(key_of(&fan.departed[0]), 1);
    assert_eq!(fan.departed[0].ver, writes[0].1);
}

#[tokio::test]
async fn an_idle_write_is_departed_not_shipped() {
    let clock = clock_at(T0);
    let a = clocked(
        Mode::Replicated,
        u64::MAX,
        Some(Duration::from_millis(1_000)),
        &clock,
    );
    a.insert(1, "a".into()).await.expect("insert");
    let writes = drained(&a);

    let fresh = a.fan_out_records(&writes).await;
    assert_eq!(fresh.records.len(), 1, "within its idle time it ships");
    assert!(fresh.departed.is_empty(), "nothing expected");

    clock.store(T0 + 2_000, Ordering::SeqCst);
    let idle = a.fan_out_records(&writes).await;
    assert!(
        idle.records.is_empty(),
        "idleness is local state that does not travel with a record"
    );
    assert_eq!(idle.departed.len(), 1);
    assert_eq!(idle.departed[0].ver, writes[0].1);
}

#[tokio::test]
async fn a_removed_key_ships_its_tombstone_and_is_not_departed() {
    let clock = clock_at(T0);
    let a = replicated(&clock);
    a.insert(1, "a".into()).await.expect("insert");
    a.remove(&1).await.expect("remove");
    let writes = drained(&a);

    let fan = a.fan_out_records(&writes).await;

    assert!(fan.departed.is_empty(), "nothing expected");
    assert_eq!(fan.records.len(), 1);
    assert!(fan.records[0].is_tombstone());
    assert_eq!(fan.records[0].ver, writes[0].1);
}

#[tokio::test]
async fn a_key_the_shard_never_held_is_departed() {
    let a = shard::<u32, String>(1);
    let fan = a.fan_out_records(&[(9, hlc(3, 1))]).await;
    assert!(fan.records.is_empty(), "nothing expected");
    assert_eq!(fan.departed.len(), 1);
    assert_eq!(fan.departed[0].ver, hlc(3, 1));
}

#[tokio::test]
async fn a_stored_record_older_than_the_write_is_departed_and_not_shipped() {
    let clock = clock_at(T0);
    let a = replicated(&clock);
    a.insert(1, "older".into()).await.expect("insert");
    let queued = hlc(T0 + 500, 1);

    let fan = a.fan_out_records(&[(1, queued)]).await;

    assert!(
        fan.records.is_empty(),
        "the older copy is left out beside the invalidation"
    );
    assert_eq!(fan.departed.len(), 1);
    assert_eq!(fan.departed[0].ver, queued);
}

#[tokio::test]
async fn a_stored_record_newer_than_the_write_ships_and_is_not_departed() {
    let clock = clock_at(T0);
    let a = replicated(&clock);
    a.insert(1, "first".into()).await.expect("insert");
    let first = drained(&a);
    a.insert(1, "second".into()).await.expect("insert");

    let fan = a.fan_out_records(&first).await;

    assert!(fan.departed.is_empty(), "nothing expected");
    assert_eq!(fan.records.len(), 1);
    let second = ShardOps::records_for(&a, vec![key_bytes(&1u32)]).await;
    assert_eq!(fan.records[0].ver, second[0].ver);
}

#[tokio::test]
async fn an_evicted_fill_in_a_replicated_shard_is_departed() {
    let clock = clock_at(T0);
    let s = clocked(Mode::Replicated, 1, None, &clock);
    for key in [1u32, 2] {
        s.get_or_load(
            &key,
            async |k: &u32| -> Result<String, std::convert::Infallible> { Ok(k.to_string()) },
        )
        .await
        .expect("load");
    }
    assert_eq!(s.entry_count().await, 1);
    let writes = applied_writes(s.fan_out_queue().drain());
    assert_eq!(writes.len(), 2);

    let fan = s.fan_out_records(&writes).await;

    assert_eq!(fan.records.len(), 1);
    assert_eq!(fan.departed.len(), 1);
    assert_eq!(key_of(&fan.departed[0]), 1, "the evicted fill");
    assert_eq!(fan.departed[0].ver, writes[0].1);
}

#[tokio::test]
async fn a_write_in_a_part_the_node_has_since_released_is_departed() {
    let self_node = NodeId::from(1);
    let eligible = vec![self_node, NodeId::from(2)];
    let (s, view, _tx) =
        distributed_shard::<u32, String>(self_node, eligible, 2, Arc::new(ResidencySet::new()));
    let key = find_key_by_ownership(&view, true);
    s.insert(key, "v".into()).await.expect("insert");
    let writes = applied_writes(s.fan_out_queue().drain());
    let bucket = bucket_of(&key_bytes(&key));

    ShardOps::release_buckets(&s, &[bucket]).await;
    let fan = s.fan_out_records(&writes).await;

    assert!(fan.records.is_empty(), "nothing expected");
    assert_eq!(fan.departed.len(), 1);
    assert_eq!(fan.departed[0].ver, writes[0].1);
}

#[tokio::test]
async fn invalidating_at_an_equal_version_keeps_a_new_owners_copy() {
    let s = shard::<u32, String>(1);
    s.insert(1, "kept".into()).await.expect("insert");
    let stored = ShardOps::records_for(&s, vec![key_bytes(&1u32)]).await;

    ShardOps::invalidate(&s, key_bytes(&1u32), stored[0].ver).await;

    assert_eq!(
        s.get(&1).await,
        Some("kept".to_string()),
        "a departed write's invalidation does not drop a copy already at that version"
    );
}

#[tokio::test]
async fn the_readable_record_view_still_hides_a_lapsed_entry() {
    let clock = clock_at(T0);
    let a = replicated(&clock);
    a.insert_with_ttl(1, "a".into(), Duration::from_millis(10))
        .await
        .expect("insert");
    assert_eq!(a.records_for_typed(&[1u32]).await.len(), 1);

    clock.store(T0 + 10, Ordering::SeqCst);

    assert!(
        a.records_for_typed(&[1u32]).await.is_empty(),
        "nothing expected"
    );
    assert!(
        ShardOps::records_for(&a, vec![key_bytes(&1u32)])
            .await
            .is_empty(),
        "an anti-entropy reply never serves a lapsed entry"
    );
}

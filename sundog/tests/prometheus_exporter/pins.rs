//! Scripted cache traffic whose exact metric values the exporter test pins.

use smol_str::SmolStr;
use std::net::{Ipv4Addr, SocketAddr};
use std::num::NonZeroU8;
use std::sync::Arc;
use std::time::Duration;
use sundog::store::{Shard, ShardOps};
use sundog::{
    CacheError, Cluster, ConflictResolver, Merged, Mode, NodeId, RecordView, Winner, later_deadline,
};

use crate::support::{node_config, reserve_tcp_addr};

/// A known hit/miss sequence on a `Mode::Local` cache of its own, exact
/// rather than shared with `users`' traffic: 2 inserts, 3 hit gets, 2 miss
/// gets, one filling `get_or_load`, one hit `get_or_load`, two
/// `contains_key` checks, one `get_or_insert_with` miss and hit, and four
/// concurrent `get_or_load`s of one key: hits=3+1+1+3=8, misses=2+1+1+1=5.
/// It ends with `explain` of a live, a missing and a filled key, which moves
/// neither total and counts no fetch.
pub(crate) async fn count_hits_and_misses(cluster: &Cluster) {
    let counted = cluster
        .cache::<u32, String>("counted")
        .mode(Mode::Local)
        .open()
        .await
        .expect("cache opens");
    counted.insert(1, "a".into()).await.expect("insert");
    counted.insert(2, "b".into()).await.expect("insert");
    assert_eq!(counted.get(&1).await, Some("a".to_string()));
    assert_eq!(counted.get(&2).await, Some("b".to_string()));
    assert_eq!(counted.get(&1).await, Some("a".to_string()));
    assert_eq!(counted.get(&3).await, None);
    assert_eq!(counted.get(&4).await, None);
    let filled = counted
        .get_or_load(&5, async |_key| {
            Ok::<_, std::convert::Infallible>("loaded".to_string())
        })
        .await
        .expect("loader succeeds");
    assert_eq!(filled, "loaded");
    let cached = counted
        .get_or_load(&5, async |_key| {
            Ok::<_, std::convert::Infallible>("loaded".to_string())
        })
        .await
        .expect("loader succeeds");
    assert_eq!(cached, "loaded");

    // An existence check moves neither counter.
    assert!(counted.contains_key(&1).await);
    assert!(!counted.contains_key(&9).await);
    // get_or_insert_with: one miss to fill, one hit to read back.
    let made = counted
        .get_or_insert_with(&6, async |_key| "made".to_string())
        .await
        .expect("make succeeds");
    assert_eq!(made, "made");
    let kept = counted
        .get_or_insert_with(&6, async |_key| "unused".to_string())
        .await
        .expect("make succeeds");
    assert_eq!(kept, "made");
    // Four concurrent loads of one key collapse into one loader run.
    let loads = futures::future::join_all((0..4).map(|_| {
        let counted = counted.clone();
        async move {
            counted
                .get_or_load(&7, async |_key| {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    Ok::<_, std::convert::Infallible>("joined".to_string())
                })
                .await
                .expect("loader succeeds")
        }
    }))
    .await;
    assert!(loads.iter().all(|value| value == "joined"));

    // `explain` is not a read: it counts no hit, miss or fetch, so the totals
    // pinned in the scrape stay exact and `counted` exports no fetch series.
    for (key, returns_value) in [(1, true), (3, false), (5, true)] {
        let explanation = counted.explain(&key).await.expect("explains");
        assert_eq!(
            explanation.source.returns_value(),
            returns_value,
            "{explanation}"
        );
    }
}

/// Drives `sundog_read_duration_seconds{cache="timed-reads",outcome}`: on a
/// fresh thread, 768 hits and then 768 misses. Timed reads fall 128 to 384
/// reads apart, so each run of 768 times between two and six reads.
pub(crate) async fn read_durations_pin_metrics(cluster: &Cluster) {
    let timed = cluster
        .cache::<u32, String>("timed-reads")
        .mode(Mode::Local)
        .open()
        .await
        .expect("cache opens");
    timed.insert(1, "a".into()).await.expect("insert");
    std::thread::spawn(move || {
        for _ in 0..768 {
            assert_eq!(timed.get_sync(&1), Some("a".to_string()));
        }
        for _ in 0..768 {
            assert_eq!(timed.get_sync(&2), None);
        }
    })
    .join()
    .expect("the reads finish");
}

/// Drives `sundog_ceiling_refusals_total{cache, kind}` and
/// `sundog_cache_bytes{cache}` on a `Mode::Local` cache whose memory ceiling
/// is one byte: one accepted insert, two refused writes (`kind="write"`),
/// and one fill returned uncached (`kind="fill"`). Returns the cache's
/// resident bytes, the value its gauge settles on.
pub(crate) async fn memory_ceiling_pin_metrics(cluster: &Cluster) -> u64 {
    let capped = cluster
        .cache::<u32, String>("ceiling-pin")
        .mode(Mode::Local)
        .max_resident_bytes(1)
        .open()
        .await
        .expect("cache opens");
    capped
        .insert(1, "a".into())
        .await
        .expect("under the ceiling");
    assert!(capped.insert(2, "b".into()).await.is_err());
    assert!(capped.insert_many([(3, "c".into())]).await.is_err());
    let loaded = capped
        .get_or_load(&4, async |_key| {
            Ok::<_, std::convert::Infallible>("uncached".to_string())
        })
        .await
        .expect("loader succeeds");
    assert_eq!(loaded, "uncached");
    assert_eq!(capped.get(&4).await, None);
    capped
        .resident_bytes()
        .expect("a capped cache counts its bytes")
}

/// How many tombstones [`tombstone_pin_metrics`] leaves: two deletes and one
/// swept expired entry.
pub(crate) const TOMBSTONE_PIN_COUNT: f64 = 3.0;

/// Drives `sundog_tombstones{cache}` on a `Mode::Replicated` cache of a node
/// of its own, which keeps tombstones for an hour where the shared nodes'
/// two seconds would collect them mid-test: two removes and one entry that
/// expires and is swept leave three tombstones, and the live entries beside
/// them leave none. Returns the node, which must stay up until the gauge is
/// scraped.
pub(crate) async fn tombstone_pin_metrics() -> Cluster {
    let node = Cluster::builder("it-prometheus-tombstones")
        .seeds(std::iter::empty())
        .config(
            node_config(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).with(|c| {
                c.tombstone_ttl = Duration::from_hours(1);
            }),
        )
        .build()
        .await
        .expect("node builds");
    let cache = node
        .cache::<u32, String>("tombstone-pin")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("cache opens");
    for key in 1..=4 {
        cache.insert(key, "v".into()).await.expect("insert");
    }
    cache.remove(&1).await.expect("remove");
    cache.remove(&2).await.expect("remove");
    cache
        .insert_with_ttl(5, "short".into(), Duration::from_millis(30))
        .await
        .expect("insert with ttl");
    tokio::time::sleep(Duration::from_millis(200)).await;
    // The count sweeps first, so the expired entry becomes a tombstone.
    assert_eq!(cache.entry_count().await, 2);
    node
}

#[derive(Debug, thiserror::Error)]
#[error("source unavailable")]
struct SourceDown;

/// Drives `sundog_loader_calls_total{cache}` on a `Mode::Local` cache: one
/// loader call for three missed keys, none for a hit, and one for a key the
/// source lacks. A cache with no loader exports no series.
pub(crate) async fn loader_calls_pin_metric(cluster: &Cluster) {
    let cache = cluster
        .cache::<u32, String>("load-pin")
        .mode(Mode::Local)
        .batch_loader(|keys: Vec<u32>| async move {
            Ok::<_, SourceDown>(
                keys.into_iter()
                    .filter(|key| *key < 10)
                    .map(|key| (key, format!("v{key}")))
                    .collect::<std::collections::HashMap<_, _>>(),
            )
        })
        .open()
        .await
        .expect("cache opens");
    assert_eq!(
        cache.load_many([1, 2, 3]).await.expect("load_many").len(),
        3
    );
    assert_eq!(cache.load(&1).await.expect("load"), Some("v1".to_string()));
    assert_eq!(cache.load(&11).await.expect("load"), None);
    // `explain` loads nothing: a held key and a key the source lacks leave
    // the two loader calls the scrape pins.
    for key in [1, 11] {
        cache.explain(&key).await.expect("explains");
    }
}

/// Drives `sundog_refreshes_total{cache, outcome}` on a `Mode::Local` cache
/// with a 1s TTL and refresh-ahead at 0.5: three keys load together, then,
/// once due, each is read in turn and reloads alone, one into a new value
/// (`loaded`), one the source no longer holds (`absent`), and one whose
/// reload fails (`failed`). Every loader call shows in
/// `sundog_loader_calls_total{cache}`: four.
pub(crate) async fn refresh_outcomes_pin_metrics(cluster: &Cluster) {
    let calls = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let cache = cluster
        .cache::<u32, String>("refresh-pin")
        .mode(Mode::Local)
        .ttl(Duration::from_secs(1))
        .refresh_ahead(0.5)
        .batch_loader({
            let calls = Arc::clone(&calls);
            move |keys: Vec<u32>| {
                let first = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0;
                async move {
                    if !first && keys.contains(&5) {
                        return Err(SourceDown);
                    }
                    Ok(keys
                        .into_iter()
                        .filter(|key| first || *key == 2)
                        .map(|key| (key, if first { "old" } else { "new" }.to_string()))
                        .collect::<std::collections::HashMap<_, _>>())
                }
            }
        })
        .open()
        .await
        .expect("cache opens");
    assert_eq!(
        cache.load_many([2, 3, 5]).await.expect("load_many").len(),
        3
    );
    tokio::time::sleep(Duration::from_millis(550)).await;
    // The keys are past their refresh point: `explain` of each asks for no
    // reload, so the call and outcome counts the scrape pins hold and the
    // reads below stay the only ones that ask.
    for key in [2, 3, 5] {
        cache.explain(&key).await.expect("explains");
    }
    for (expected_calls, key) in [(2, 2u32), (3, 3), (4, 5)] {
        assert!(cache.get(&key).await.is_some(), "key {key} is still live");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while calls.load(std::sync::atomic::Ordering::SeqCst) < expected_calls {
            assert!(
                tokio::time::Instant::now() < deadline,
                "key {key}'s reload never ran"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while cache.get(&2).await.as_deref() != Some("new") {
        assert!(
            tokio::time::Instant::now() < deadline,
            "key 2 never reloaded"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Drives `sundog_refresh_hints_sent_total{cache}` across `cluster` and
/// `peer`, both holding a `Mode::Replicated` cache with a 2s TTL and
/// refresh-ahead at 0.5: 16 keys load on `cluster` and replicate, and once
/// due both nodes read every key. Each key reloads on its one refresher,
/// so the other node sends exactly one hint for it: 16 across both nodes,
/// whichever of the two each key's refresher is.
pub(crate) async fn refresh_hints_pin_metric(cluster: &Cluster, peer: &Cluster) {
    let calls = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let open = |node: &Cluster| {
        let calls = Arc::clone(&calls);
        node.cache::<u32, String>("refresh-hint-pin")
            .mode(Mode::Replicated)
            .ttl(Duration::from_secs(2))
            .refresh_ahead(0.5)
            .batch_loader(move |keys: Vec<u32>| {
                let call = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                async move {
                    Ok::<_, SourceDown>(
                        keys.into_iter()
                            .map(|key| (key, format!("v{key}.{call}")))
                            .collect::<std::collections::HashMap<_, _>>(),
                    )
                }
            })
            .open()
    };
    let here = open(cluster).await.expect("cache opens");
    let there = open(peer).await.expect("peer cache opens");
    let keys: Vec<u32> = (0..16).collect();
    assert_eq!(
        here.load_many(keys.clone()).await.expect("load_many").len(),
        16
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    for key in &keys {
        while there.get(key).await.is_none() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "key {key} never replicated"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    for key in &keys {
        assert!(here.get(key).await.is_some());
        assert!(there.get(key).await.is_some());
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    for key in &keys {
        let first = format!("v{key}.1");
        while here.get(key).await.as_ref() == Some(&first)
            || there.get(key).await.as_ref() == Some(&first)
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "key {key} never reloaded on both nodes"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// Drives a genuine `sundog_fan_out_wait_timeouts_total{cache}` increment
/// and pins `sundog_fan_out_backlog{cache}` nonzero through a bare
/// `Shard`, with no `Cluster`/mesh to drain the fan-out queue. A
/// `fan_out_backlog_capacity` of 1 and a one-microsecond
/// `fan_out_wait_timeout` force the second `insert`'s wait to time out
/// deterministically; the write still lands, over capacity.
pub(crate) async fn fan_out_wait_timeout_pin_metrics() {
    let shard = Shard::<u32, String>::new(
        SmolStr::new("fan-out-timeout-pin"),
        Mode::Replicated,
        NodeId::from(1),
        10_000,
        None,
        None,
    )
    .with_fan_out_backlog_capacity(1)
    .with_fan_out_wait_timeout(Duration::from_micros(1));

    shard
        .insert(0, "a".to_string())
        .await
        .expect("first insert fills the backlog to its capacity of one");
    shard
        .insert(1, "b".to_string())
        .await
        .expect("second insert proceeds once its wait times out, over capacity");
}

/// Drives `sundog_clock_skew_rejected_total{cache}` through a bare `Shard`
/// bounding skew at one minute: one record stamped an hour ahead is
/// refused, one stamped a second ahead is applied, so the counter reads
/// exactly one.
pub(crate) async fn clock_skew_pin_metrics() {
    let shard = Shard::<u32, String>::new(
        SmolStr::new("clock-skew-pin"),
        Mode::Replicated,
        NodeId::from(1),
        10_000,
        None,
        None,
    )
    .with_max_clock_skew(Some(Duration::from_secs(60)));
    let now_ms = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the clock is past the epoch")
            .as_millis(),
    )
    .expect("epoch milliseconds fit in u64");
    let record = |key: u32, ahead_ms: u64| sundog::wire::WireRecord {
        key: bytes::Bytes::from(postcard::to_stdvec(&key).expect("key encodes")),
        value: Some(bytes::Bytes::from(
            postcard::to_stdvec(&"v".to_string()).expect("value encodes"),
        )),
        ver: sundog::Hlc {
            wall_ms: now_ms + ahead_ms,
            logical: 0,
            node: NodeId::from(2),
        },
        expires_at_ms: None,
    };
    shard
        .apply_remote_batch(vec![record(1, 3_600_000), record(2, 1_000)])
        .await;
    assert_eq!(shard.get(&1).await, None);
    assert_eq!(shard.get(&2).await.as_deref(), Some("v"));
}

/// Drives `sundog_tombstones_ignored_total{cache}` through a bare `Shard`
/// that keeps tombstones for a minute and has ended its start deferral:
/// three deletes stamped an hour ago arrive lapsed and are ignored, one
/// stamped a second ago is stored, so the counter reads exactly three.
pub(crate) async fn tombstone_ignored_pin_metrics() {
    let shard = Shard::<u32, String>::new(
        SmolStr::new("tombstone-ignored-pin"),
        Mode::Replicated,
        NodeId::from(1),
        10_000,
        None,
        None,
    )
    .with_tombstone_ttl(Duration::from_secs(60));
    shard.gc_tombstones(false).await;
    let now_ms = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the clock is past the epoch")
            .as_millis(),
    )
    .expect("epoch milliseconds fit in u64");
    let delete = |key: u32, ago_ms: u64| sundog::wire::WireRecord {
        key: bytes::Bytes::from(postcard::to_stdvec(&key).expect("key encodes")),
        value: None,
        ver: sundog::Hlc {
            wall_ms: now_ms - ago_ms,
            logical: 0,
            node: NodeId::from(2),
        },
        expires_at_ms: None,
    };
    shard
        .apply_remote_batch(vec![
            delete(1, 3_600_000),
            delete(2, 3_600_000),
            delete(3, 3_600_000),
            delete(4, 1_000),
        ])
        .await;
    let held = |key: u32| {
        shard.records_for(vec![bytes::Bytes::from(
            postcard::to_stdvec(&key).expect("key encodes"),
        )])
    };
    assert_eq!(held(4).await.len(), 1, "the fresh delete is kept");
    assert!(held(1).await.is_empty(), "a lapsed one is not");
}

/// A resolver over byte vectors whose merge appends the incoming vector to
/// the stored one, and clears the value when the incoming one is empty.
struct ConcatResolver;

impl ConflictResolver for ConcatResolver {
    fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        if a.ver >= b.ver { Winner::A } else { Winner::B }
    }

    fn merges(&self) -> bool {
        true
    }

    fn merge(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Option<Merged> {
        let mut left: Vec<u8> = postcard::from_bytes(a.value?).ok()?;
        let right: Vec<u8> = postcard::from_bytes(b.value?).ok()?;
        if right.is_empty() {
            left.clear();
        } else {
            left.extend(right);
        }
        Some(Merged {
            value: bytes::Bytes::from(postcard::to_stdvec(&left).ok()?),
            expires_at_ms: later_deadline(a.expires_at_ms, b.expires_at_ms),
        })
    }
}

/// Drives `sundog_merge_refused_total{cache, reason="too_large"}` through a
/// bare `Shard` that keeps frames to a kilobyte: a merge of 600 bytes
/// stores, a second 600 bytes would make 1,200 and is refused, an insert that
/// merges the same way is refused, and a merge that clears the value
/// shrinks it and is accepted, so the counter reads exactly two.
pub(crate) async fn merge_refused_pin_metric() {
    let shard = Shard::<u32, Vec<u8>>::new(
        SmolStr::new("merge-refused-pin"),
        Mode::Local,
        NodeId::from(1),
        10_000,
        None,
        None,
    )
    .with_resolver(Arc::new(ConcatResolver))
    .with_max_frame(1_024);
    shard.merge(1, vec![1; 600]).await.expect("600 bytes fit");
    assert!(matches!(
        shard.merge(1, vec![2; 600]).await,
        Err(CacheError::ValueTooLarge { .. })
    ));
    assert!(matches!(
        shard.insert(1, vec![3; 600]).await,
        Err(CacheError::ValueTooLarge { .. })
    ));
    shard.merge(1, Vec::new()).await.expect("a clear shrinks");
    assert_eq!(shard.get(&1).await, Some(Vec::new()));
}

/// Drives `sundog_sync_skipped_total{cache, reason="too_large"}` through a
/// bare `Shard` that holds a record larger than any frame, stored below the
/// public API as one stored before the frame cap applied would be: it stays
/// readable, and the sync leaves it out twice, once from a pull reply's
/// records and once from a state-transfer snapshot, so the counter reads
/// exactly two.
pub(crate) async fn sync_skipped_pin_metric() {
    use futures::StreamExt as _;
    let shard = Shard::<u32, Vec<u8>>::new(
        SmolStr::new("sync-skipped-pin"),
        Mode::Replicated,
        NodeId::from(1),
        10_000,
        None,
        None,
    );
    let key = bytes::Bytes::from(postcard::to_stdvec(&1u32).expect("key encodes"));
    let big = sundog::wire::WireRecord {
        key: key.clone(),
        value: Some(bytes::Bytes::from(
            postcard::to_stdvec(&vec![0u8; sundog::wire::MAX_FRAME]).expect("value encodes"),
        )),
        ver: sundog::Hlc {
            wall_ms: u64::try_from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("the clock is past the epoch")
                    .as_millis(),
            )
            .expect("epoch milliseconds fit in u64"),
            logical: 0,
            node: NodeId::from(2),
        },
        expires_at_ms: None,
    };
    shard.apply_remote(big).await;
    assert_eq!(
        shard.get(&1).await.map(|v| v.len()),
        Some(sundog::wire::MAX_FRAME)
    );
    let records = shard.records_for(vec![key]).await;
    assert_eq!(records.len(), 1, "a read answers it");
    assert!(
        shard.skip_unframeable(records).is_empty(),
        "a sync does not"
    );
    let snapshot: Vec<_> = shard.snapshot_chunks().collect().await;
    assert!(snapshot.iter().all(Vec::is_empty), "nor does a snapshot");
}

/// The records the skew hold keeps per cache: the cap
/// `ShardOps::apply_forwarded_batch` holds forwards up to, documented with
/// `sundog_clock_skew_held` in operations.md.
pub(crate) const SKEW_HOLD_PIN_CAPACITY: u32 = 16_384;

/// Bytes the hold charges a record beyond its key and value.
const SKEW_HOLD_RECORD_OVERHEAD: usize = 64;

/// What `sundog_clock_skew_held_bytes{cache="skew-hold-pin"}` reads after
/// [`skew_hold_pin_metrics`]: every held record's key and value bytes plus
/// the fixed overhead.
pub(crate) fn skew_hold_pin_bytes() -> f64 {
    let held: usize = (0..SKEW_HOLD_PIN_CAPACITY)
        .map(|key| {
            postcard::to_stdvec(&key).expect("key encodes").len()
                + postcard::to_stdvec(&"v".to_string())
                    .expect("value encodes")
                    .len()
                + SKEW_HOLD_RECORD_OVERHEAD
        })
        .sum();
    f64::from(u32::try_from(held).expect("the pinned hold fits in u32"))
}

/// A node whose data-plane port holds two connections that never send a
/// hello: one silent, one that writes two bytes of a length prefix. Both
/// close at the hello deadline, so `sundog_accept_dropped_total
/// {reason="hello_timeout"}` reaches 2, the only connections in this binary
/// that wait out the deadline. Returns the node and the two sockets, which
/// stay open until the metrics are scraped.
pub(crate) async fn accept_drop_pin_metrics()
-> (Cluster, tokio::net::TcpStream, tokio::net::TcpStream) {
    use tokio::io::AsyncWriteExt;

    let data_addr = reserve_tcp_addr().await;
    let node = Cluster::builder("it-prometheus-accept-drop")
        .seeds(std::iter::empty())
        .config(
            node_config(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
                .with(|c| c.data_bind_addr = data_addr),
        )
        .build()
        .await
        .expect("node builds");
    let silent = tokio::net::TcpStream::connect(data_addr)
        .await
        .expect("the node's data port accepts");
    let mut partial = tokio::net::TcpStream::connect(data_addr)
        .await
        .expect("the node's data port accepts");
    partial
        .write_all(&[0, 0])
        .await
        .expect("two bytes of a length prefix");
    (node, silent, partial)
}

/// Drives the skew hold's metrics on a `Mode::Distributed` cache that owns
/// every part, with a raw data-plane peer that is no owner forwarding it
/// records stamped an hour ahead of the default one-minute skew bound: the
/// first [`SKEW_HOLD_PIN_CAPACITY`] distinct keys are held, one more finds
/// the hold full, and one stamped three days ahead is past its horizon.
/// Pins `sundog_clock_skew_held`, `sundog_clock_skew_held_bytes`,
/// `sundog_clock_skew_hold_dropped_total{reason}` and, one refusal per
/// record, `sundog_clock_skew_rejected_total`. Returns the node and the
/// peer's socket, which stay up until the metrics are scraped.
pub(crate) async fn skew_hold_pin_metrics() -> (Cluster, tokio::net::TcpStream) {
    use sundog::wire::{self, Msg, WireRecord};
    use tokio::io::AsyncWriteExt;

    let data_addr = reserve_tcp_addr().await;
    let node = Cluster::builder("it-prometheus-skew-hold")
        .seeds(std::iter::empty())
        .config(
            node_config(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).with(|c| {
                c.data_bind_addr = data_addr;
                c.max_clock_skew = Some(Duration::from_mins(1));
            }),
        )
        .build()
        .await
        .expect("node builds");
    let _cache = node
        .cache::<u32, String>("skew-hold-pin")
        .mode(Mode::Distributed {
            owners: NonZeroU8::new(2).expect("nonzero"),
        })
        .open()
        .await
        .expect("cache opens");
    // The cache stays open until the node shuts down: dropping a handle
    // does not close it.

    let writer = NodeId::from(0x5eed_0001);
    let mut stream = tokio::net::TcpStream::connect(data_addr)
        .await
        .expect("the node's data port accepts");
    let send = async |stream: &mut tokio::net::TcpStream, msg: &Msg| {
        let frame = wire::encode(msg).expect("encodes");
        let len = u32::try_from(frame.len()).expect("a frame fits u32");
        stream
            .write_all(&len.to_be_bytes())
            .await
            .expect("length sent");
        stream.write_all(&frame).await.expect("frame sent");
    };
    send(
        &mut stream,
        &Msg::Hello {
            node: writer,
            incarnation: 1,
            protocol: wire::PROTOCOL_VERSION,
            cluster: wire::UNNAMED_CLUSTER,
        },
    )
    .await;

    let now_ms = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the clock is past the epoch")
            .as_millis(),
    )
    .expect("epoch milliseconds fit in u64");
    let record = |key: u32, ahead_ms: u64| WireRecord {
        key: bytes::Bytes::from(postcard::to_stdvec(&key).expect("key encodes")),
        value: Some(bytes::Bytes::from(
            postcard::to_stdvec(&"v".to_string()).expect("value encodes"),
        )),
        ver: sundog::Hlc {
            wall_ms: now_ms + ahead_ms,
            logical: 0,
            node: writer,
        },
        expires_at_ms: None,
    };
    let hour = 3_600_000;
    let keys: Vec<u32> = (0..=SKEW_HOLD_PIN_CAPACITY).collect();
    for chunk in keys.chunks(2_000) {
        send(
            &mut stream,
            &Msg::ForwardBatch {
                cache: SmolStr::new("skew-hold-pin"),
                view_hash: 0,
                hops: 0,
                recs: chunk.iter().map(|&key| record(key, hour)).collect(),
            },
        )
        .await;
    }
    send(
        &mut stream,
        &Msg::ForwardBatch {
            cache: SmolStr::new("skew-hold-pin"),
            view_hash: 0,
            hops: 0,
            recs: vec![record(u32::MAX, 72 * hour)],
        },
    )
    .await;
    (node, stream)
}

//! Tests of the owner probes behind `Cache::explain`: values, declines, crashes and stale views.

use super::*;
use crate::explain::ProbedRecord;

/// Three `Mode::distributed()` nodes of `name` and two keys the first node
/// does not own, whose two owners hold their parts warm under one view on
/// every node. Anti-entropy is effectively off, so nothing repairs a copy
/// a test drops by hand, and the failure detector never drops a crashed
/// node from a view.
async fn explain_nodes(name: &'static str) -> (Vec<(Cluster, Cache<u32, String>)>, [u32; 2]) {
    let mut config = loopback_config().with(|c| {
        c.ae_interval = Duration::from_secs(3600);
        c.phi_threshold = 1_000.0;
    });
    config.tombstone_ttl = config.bucket_release_window();
    let (a, b, c) = three_node_distributed_with(name, config).await;
    let nodes = vec![a, b, c];
    wait_for_full_views(&nodes).await;
    let asker = nodes[0].0.node_id();
    let keys: Vec<u32> = (0..100_000u32)
        .filter(|key| !nodes[0].1.owners_of(key).contains(&asker))
        .take(2)
        .collect();
    let [first, second] = keys[..] else {
        panic!("the first node owns only some parts of a three-node view");
    };
    for key in [first, second] {
        wait_for_warm_owners(&nodes, key).await;
    }
    (nodes, [first, second])
}

/// Crashes `node`'s cluster and drops it from `nodes`.
async fn crash_node(nodes: &mut Vec<(Cluster, Cache<u32, String>)>, node: NodeId) {
    let at = nodes
        .iter()
        .position(|(cluster, _)| cluster.node_id() == node)
        .expect("the node is one of the cluster's");
    let (cluster, _) = nodes.remove(at);
    cluster.crash().await;
}

/// `explanation`'s distributed half, after checking it against `cache`:
/// its owners are the ones `Cache::owners_of` returns for `key`, and its
/// probes ask every owner but `cache`'s own node, in that order.
fn distributed_of<'a>(
    cache: &Cache<u32, String>,
    key: u32,
    explanation: &'a ReadExplanation,
) -> &'a DistributedRead {
    let read = explanation
        .distributed
        .as_ref()
        .expect("a Distributed cache explains its fetch");
    let owners = cache.owners_of(&key);
    assert_eq!(
        read.owners, owners,
        "the explanation names the owners owners_of does"
    );
    let me = cache.cluster.node_id();
    let others: Vec<NodeId> = owners.into_iter().filter(|owner| *owner != me).collect();
    let probed: Vec<NodeId> = read.probes.iter().map(|probe| probe.node).collect();
    assert_eq!(
        probed, others,
        "the probes ask every other owner, in owners order"
    );
    read
}

/// Each probe's answer, in probe order.
fn answers_of(read: &DistributedRead) -> Vec<ProbeAnswer> {
    read.probes.iter().map(|probe| probe.answer).collect()
}

/// The record `probe` reports its owner sending.
fn held_record(probe: &OwnerProbe) -> ProbedRecord {
    match probe.answer {
        ProbeAnswer::Held(record) => record,
        other => panic!("owner {} sends its record, answered {other:?}", probe.node),
    }
}

/// The record every probe of `read` reports sent, in probe order.
fn held_records(read: &DistributedRead) -> Vec<ProbedRecord> {
    read.probes.iter().map(held_record).collect()
}

/// Asserts a fetch of `key` on `cache` returns what `explanation.source`
/// predicts: a value for a source that returns one, `None` for one that
/// does not, and `FetchUnavailable` for no source.
async fn assert_fetch_returns_source(
    cache: &Cache<u32, String>,
    key: u32,
    explanation: &ReadExplanation,
) {
    let fetched = cache.fetch(&key).await;
    match (explanation.source, fetched) {
        (ReadSource::Local { hit: true } | ReadSource::Owner { hit: true, .. }, Ok(Some(_)))
        | (ReadSource::Local { hit: false } | ReadSource::Owner { hit: false, .. }, Ok(None))
        | (ReadSource::Unavailable, Err(CacheError::FetchUnavailable { .. })) => {}
        (source, fetched) => panic!("explained {source:?} for key {key}, fetched {fetched:?}"),
    }
}

/// A key a node does not own, probed through every state its two owners
/// hold it in: a live entry reads as a value on both under one version, a
/// delete reads as `Deleted` on both under a newer one, and a key never
/// written is a miss on both. The asking node holds nothing, so its own
/// copy never answers, and each source is what a fetch returns.
#[tokio::test]
async fn explain_probe_reports_a_value_a_tombstone_and_a_miss() {
    let (nodes, [key, absent]) = explain_nodes("cache-it-explain-probe-answers").await;
    let cache_a = &nodes[0].1;
    let owners = cache_a.owners_of(&key);
    let (first, second) = (cache_of(&nodes, owners[0]), cache_of(&nodes, owners[1]));

    first.insert(key, "one".to_string()).await.expect("insert");
    wait_for_local_record(&[first, second], key, LocalRecord::is_live).await;
    let explanation = cache_a.explain(&key).await.expect("explains");
    let read = distributed_of(cache_a, key, &explanation);
    assert_eq!(
        explanation.local,
        LocalRecord::Absent,
        "the asking node does not own the key's part"
    );
    assert_eq!(read.local_read, LocalRead::NotOwner);
    let live = held_records(read);
    assert!(
        live.iter()
            .all(|record| record.reads == Reads::Value && record.expires_at_ms.is_none()),
        "both owners hold a value without an expiry: {live:?}"
    );
    assert_eq!(live[0].version, live[1].version, "one write, one version");
    assert_eq!(
        explanation.source,
        ReadSource::Owner {
            node: owners[0],
            hit: true
        },
        "the first owner probed answers"
    );
    assert_fetch_returns_source(cache_a, key, &explanation).await;

    first.remove(&key).await.expect("remove");
    wait_for_local_record(&[first, second], key, |record| {
        matches!(record, LocalRecord::Tombstone { .. })
    })
    .await;
    let explanation = cache_a.explain(&key).await.expect("explains");
    let read = distributed_of(cache_a, key, &explanation);
    let dead = held_records(read);
    assert!(
        dead.iter().all(|record| record.reads == Reads::Deleted),
        "both owners send a tombstone, which reads as no value: {dead:?}"
    );
    assert_eq!(dead[0].version, dead[1].version, "one delete, one version");
    assert!(
        dead[0].version > live[0].version,
        "the delete is newer than the write it removes"
    );
    assert_eq!(
        explanation.source,
        ReadSource::Owner {
            node: owners[0],
            hit: false
        }
    );
    assert_fetch_returns_source(cache_a, key, &explanation).await;

    let explanation = cache_a.explain(&absent).await.expect("explains");
    let read = distributed_of(cache_a, absent, &explanation);
    assert_eq!(
        answers_of(read),
        [ProbeAnswer::Miss, ProbeAnswer::Miss],
        "warm owners on one view agree a key never written is a miss"
    );
    assert_eq!(
        explanation.source,
        ReadSource::Owner {
            node: read.owners[0],
            hit: false
        }
    );
    assert_fetch_returns_source(cache_a, absent, &explanation).await;

    shut_down_all(nodes).await;
}

/// An owner whose copy of the part is cold and holds no record declines,
/// and so does one that holds the record in a part it distrusts (owned
/// again after a loss): the fetch moves past the first and is
/// unavailable once both decline. Each owner's own explanation names the
/// verdict the asking node saw as its answer.
#[tokio::test]
async fn explain_probe_reports_a_cold_owner_and_a_distrusted_owner_as_declined() {
    let (nodes, [key, _]) = explain_nodes("cache-it-explain-probe-declines").await;
    let cache_a = &nodes[0].1;
    let owners = cache_a.owners_of(&key);
    let (cold, distrusted) = (cache_of(&nodes, owners[0]), cache_of(&nodes, owners[1]));
    cold.insert(key, "held".to_string()).await.expect("insert");
    wait_for_local_record(&[cold, distrusted], key, LocalRecord::is_live).await;
    let part = PartId::of_key(&encode_key(&key).expect("u32 encodes"));

    cold.invalidate_local(&key).await;
    cold.shard
        .residency()
        .expect("the owner is distributed")
        .mark_cold(&[part]);
    let explanation = cache_a.explain(&key).await.expect("explains");
    let read = distributed_of(cache_a, key, &explanation);
    assert_eq!(
        read.probes[0].answer,
        ProbeAnswer::Declined,
        "a cold part's miss says nothing"
    );
    assert_eq!(held_record(&read.probes[1]).reads, Reads::Value);
    assert_eq!(
        explanation.source,
        ReadSource::Owner {
            node: owners[1],
            hit: true
        },
        "the fetch moves past a decline to the next owner"
    );
    assert_fetch_returns_source(cache_a, key, &explanation).await;
    let own = cold.explain(&key).await.expect("explains");
    let own_read = distributed_of(cold, key, &own);
    assert!(own_read.residency.cold_marked);
    assert_eq!(own_read.local_read, LocalRead::ColdMiss);
    assert_eq!(own_read.serves_peers, ServeVerdict::DeclineCold);

    distrusted
        .shard
        .residency()
        .expect("the owner is distributed")
        .mark_stale(&[part]);
    let explanation = cache_a.explain(&key).await.expect("explains");
    let read = distributed_of(cache_a, key, &explanation);
    assert_eq!(
        answers_of(read),
        [ProbeAnswer::Declined, ProbeAnswer::Declined],
        "a distrusted part declines even the record it holds"
    );
    assert_eq!(explanation.source, ReadSource::Unavailable);
    assert_fetch_returns_source(cache_a, key, &explanation).await;
    let own = distrusted.explain(&key).await.expect("explains");
    assert!(own.local.is_live(), "the declining owner still holds it");
    let own_read = distributed_of(distrusted, key, &own);
    assert!(own_read.residency.stale);
    assert_eq!(own_read.local_read, LocalRead::Distrusted);
    assert_eq!(own_read.serves_peers, ServeVerdict::DeclineDistrusted);

    shut_down_all(nodes).await;
}

/// Whether `answer` is what a probe of a crashed owner reads: a refused
/// connection, or a timeout where the platform retries a refused
/// loopback connect for longer than `fetch_timeout`, as Windows does
/// for about two seconds.
fn crashed_owner_answer(answer: ProbeAnswer) -> bool {
    matches!(
        answer,
        ProbeAnswer::Unreached(Unreached::Io(_) | Unreached::TimedOut)
    )
}

/// An owner whose node crashed gives no answer, while the other owner
/// still answers: the view keeps naming the crashed owner, the fetch
/// moves past it, and the source is the second owner.
#[tokio::test]
async fn explain_probe_reports_a_crashed_owner_as_unreached() {
    let (mut nodes, [key, _]) = explain_nodes("cache-it-explain-probe-crashed").await;
    let owners = nodes[0].1.owners_of(&key);
    let (crashed, survivor) = (owners[0], cache_of(&nodes, owners[1]));
    survivor
        .insert(key, "value".to_string())
        .await
        .expect("insert");
    wait_for_local_record(
        &[cache_of(&nodes, crashed), survivor],
        key,
        LocalRecord::is_live,
    )
    .await;

    crash_node(&mut nodes, crashed).await;
    let cache_a = &nodes[0].1;
    let explanation = cache_a.explain(&key).await.expect("explains");
    let read = distributed_of(cache_a, key, &explanation);
    assert!(
        crashed_owner_answer(read.probes[0].answer),
        "a crashed owner gives no answer: {:?}",
        read.probes[0].answer
    );
    assert_eq!(held_record(&read.probes[1]).reads, Reads::Value);
    assert_eq!(
        explanation.source,
        ReadSource::Owner {
            node: owners[1],
            hit: true
        }
    );
    assert_fetch_returns_source(cache_a, key, &explanation).await;

    shut_down_all(nodes).await;
}

/// An owner that holds no record and runs another ownership view answers
/// `StaleView` with its own view hash, which a fetch counts as no answer,
/// while an owner that holds the record sends it whatever its view.
#[tokio::test]
async fn explain_probe_reports_an_owner_on_another_view_as_stale_view() {
    let name = "cache-it-explain-probe-stale-view";
    let mut config = loopback_config();
    config.fetch_timeout = Duration::from_millis(300);
    let a = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(config)
        .build()
        .await
        .expect("node a builds");
    let b = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node b builds");
    let cache_b = b
        .cache::<u32, String>(name)
        .mode(Mode::distributed())
        .open()
        .await
        .expect("b opens alone");
    let peer_b = b.local_peer();
    a.mesh().update_peers(vec![peer_b.clone()]);

    // a's view ranks a and b, b's ranks b alone: the hashes differ.
    let view = Arc::new(OwnershipView::compute(
        a.node_id(),
        vec![a.node_id(), peer_b.node],
        NonZeroU8::MIN,
    ));
    let part_of = |key: &u32| PartId::of_key(&encode_key(key).expect("u32 encodes"));
    let [missing, stored] = (0..1_000_000u32)
        .filter(|key| !view.owns(part_of(key)))
        .take(2)
        .collect::<Vec<_>>()[..]
    else {
        panic!("b owns some parts of the view");
    };
    cache_b
        .insert(stored, "stored".to_string())
        .await
        .expect("b owns every part while alone");
    let (cache_a, _residency) = cache_under_view(&a, name, &view);

    let explanation = cache_a.explain(&missing).await.expect("explains");
    let read = distributed_of(&cache_a, missing, &explanation);
    let ProbeAnswer::StaleView {
        responder_view_hash,
    } = read.probes[0].answer
    else {
        panic!("b runs another view, answered {:?}", read.probes[0].answer);
    };
    assert_ne!(responder_view_hash, read.view_hash);
    assert_eq!(
        Some(responder_view_hash),
        cache_b.shard.ownership_view_hash(),
        "the answer carries the owner's own view hash"
    );
    assert!(!read.probes[0].answer.answers());
    assert_eq!(read.view_moved_to, None);
    assert_eq!(
        explanation.source,
        ReadSource::Unavailable,
        "a stale view counts as no answer"
    );
    assert_fetch_returns_source(&cache_a, missing, &explanation).await;

    let explanation = cache_a.explain(&stored).await.expect("explains");
    let read = distributed_of(&cache_a, stored, &explanation);
    assert_eq!(
        held_record(&read.probes[0]).reads,
        Reads::Value,
        "a held record is sent whatever the two views say"
    );
    assert_eq!(
        explanation.source,
        ReadSource::Owner {
            node: peer_b.node,
            hit: true
        }
    );
    assert_fetch_returns_source(&cache_a, stored, &explanation).await;

    cache_b.close().await;
    b.shutdown().await;
    a.shutdown().await;
}

/// With both owners of a key crashed and the asking node's view still
/// naming them, no probe answers: the source is unavailable and a fetch
/// returns `FetchUnavailable`.
#[tokio::test]
async fn explain_predicts_unavailable_when_every_owner_is_down() {
    let (mut nodes, [key, _]) = explain_nodes("cache-it-explain-all-owners-down").await;
    let owners = nodes[0].1.owners_of(&key);
    cache_of(&nodes, owners[0])
        .insert(key, "value".to_string())
        .await
        .expect("insert");
    wait_for_local_record(
        &[cache_of(&nodes, owners[0]), cache_of(&nodes, owners[1])],
        key,
        LocalRecord::is_live,
    )
    .await;

    for owner in &owners {
        crash_node(&mut nodes, *owner).await;
    }
    let cache_a = &nodes[0].1;
    let explanation = cache_a.explain(&key).await.expect("explains");
    let read = distributed_of(cache_a, key, &explanation);
    assert!(
        read.probes
            .iter()
            .all(|probe| crashed_owner_answer(probe.answer)),
        "no crashed owner answers: {:?}",
        answers_of(read)
    );
    assert_eq!(explanation.local, LocalRecord::Absent);
    assert_eq!(explanation.source, ReadSource::Unavailable);
    assert_fetch_returns_source(cache_a, key, &explanation).await;

    shut_down_all(nodes).await;
}

/// The owners are asked at once: two owners that accept a connection and
/// never answer both read `TimedOut`, and the call returns after about
/// one `fetch_timeout`, not one per owner.
#[tokio::test]
async fn explain_probes_the_owners_at_once() {
    use crate::net::test_support::peer_at;

    let name = "cache-it-explain-probe-at-once";
    let fetch_timeout = Duration::from_millis(500);
    let a = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(ClusterConfig {
            fetch_timeout,
            ..loopback_config()
        })
        .build()
        .await
        .expect("node a builds");
    let silent = [NodeId::from(0xb), NodeId::from(0xc)];
    let addrs = [silent_peer_addr().await, silent_peer_addr().await];
    // The cluster republishes its empty peer list once, when its mesh
    // task first runs: let that happen before the peers are injected.
    tokio::task::yield_now().await;
    a.mesh().update_peers(vec![
        peer_at(silent[0], addrs[0]),
        peer_at(silent[1], addrs[1]),
    ]);
    let view = Arc::new(OwnershipView::compute(
        a.node_id(),
        vec![a.node_id(), silent[0], silent[1]],
        NonZeroU8::new(2).expect("nonzero"),
    ));
    let key = (0..1_000_000u32)
        .find(|key| !view.owns(PartId::of_key(&encode_key(key).expect("u32 encodes"))))
        .expect("a owns only some parts of a three-node view");
    let (cache_a, _residency) = cache_under_view(&a, name, &view);

    let started = Instant::now();
    let explanation = cache_a.explain(&key).await.expect("explains");
    let elapsed = started.elapsed();
    let read = distributed_of(&cache_a, key, &explanation);
    assert_eq!(
        answers_of(read),
        [ProbeAnswer::Unreached(Unreached::TimedOut); 2],
        "both owners accept the connection and never answer"
    );
    assert!(
        elapsed >= fetch_timeout,
        "each probe waits out the fetch timeout: {elapsed:?}"
    );
    assert!(
        elapsed < 2 * fetch_timeout,
        "the owners are asked at once, not one after the other: {elapsed:?}"
    );
    assert_eq!(explanation.source, ReadSource::Unavailable);
    assert_fetch_returns_source(&cache_a, key, &explanation).await;

    a.shutdown().await;
}

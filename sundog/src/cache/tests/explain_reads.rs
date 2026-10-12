//! Tests of `Cache::explain` over residency states, views and modes, and its agreement with `fetch`.

use super::*;

/// A one-node cluster named `name` on `config`, with no seeds and no
/// peers, for a hand-built cache to read through.
async fn solo_cluster(name: &str, config: ClusterConfig) -> Cluster {
    Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(config)
        .build()
        .await
        .expect("node builds")
}

/// `cache`'s explanation of `key`, and its `Mode::Distributed` half
/// split out of it.
async fn explain_distributed(
    cache: &Cache<u32, String>,
    key: u32,
) -> (ReadExplanation, DistributedRead) {
    let explanation = cache.explain(&key).await.expect("u32 encodes");
    let read = explanation
        .distributed
        .clone()
        .expect("a distributed cache explains its distribution");
    (explanation, read)
}

/// The owners an explanation lists are the ones `Cache::owners_of`
/// returns, and its probes ask every owner but this node, in the same
/// rank order, whether this node ranks first, second or not at all
/// among the owners; no probe ever names this node. A node that owns the
/// key's part warm and holds the key reads it from its own copy.
#[tokio::test]
async fn explain_orders_owners_like_owners_of_and_never_probes_this_node() {
    let name = "explain-owner-order";
    let a = solo_cluster(name, loopback_config()).await;
    let me = a.node_id();
    // Three nodes no mesh member answers for: every probe is unreached.
    let others: Vec<NodeId> = (1..=3u64).map(|n| NodeId::from(u64::MAX - n)).collect();
    let view = Arc::new(OwnershipView::compute(me, others, Mode::DEFAULT_OWNERS));
    let (cache, _residency) = cache_under_view(&a, name, &view);

    let mut ranks = HashSet::new();
    let mut against_id_order = false;
    for key in 0..200u32 {
        let (_, read) = explain_distributed(&cache, key).await;
        let owners = cache.owners_of(&key);
        assert_eq!(read.owners, owners, "key {key}");
        let rank = owners.iter().position(|owner| *owner == me);
        let expected = match rank {
            Some(0) => vec![owners[1]],
            Some(1) => vec![owners[0]],
            None => owners.clone(),
            Some(rank) => panic!("rank {rank} among two owners"),
        };
        let probed: Vec<NodeId> = read.probes.iter().map(|probe| probe.node).collect();
        assert_eq!(probed, expected, "key {key}");
        assert!(
            read.probes
                .iter()
                .all(|probe| probe.answer == ProbeAnswer::Unreached(Unreached::NotAMember)),
            "key {key}: no mesh member answers"
        );
        ranks.insert(rank);
        against_id_order |= probed.len() == 2 && probed[0] > probed[1];
    }
    assert_eq!(
        ranks,
        HashSet::from([Some(0), Some(1), None]),
        "the keys rank this node first, second and not at all"
    );
    assert!(
        against_id_order,
        "probes follow rendezvous rank, which is not node id order"
    );

    let key = (0..200u32)
        .find(|key| cache.owners_of(key).contains(&me))
        .expect("this node owns some of the keys");
    let record = test_record(key, "held", me);
    let version = record.ver;
    ShardOps::apply_remote_batch(cache.shard.as_ref(), vec![record]).await;
    let (explanation, read) = explain_distributed(&cache, key).await;
    assert_eq!(explanation.cache.as_str(), name);
    assert_eq!(explanation.node, me);
    assert_eq!(explanation.mode, Mode::distributed());
    assert_eq!(explanation.part, part_of(key));
    assert_eq!(
        explanation.local,
        LocalRecord::Live {
            version,
            expires_at_ms: None,
            spilled: false
        }
    );
    assert!(read.residency.owns);
    assert_eq!(read.local_read, LocalRead::Hit);
    assert_eq!(explanation.source, ReadSource::Local { hit: true });
    assert_eq!(read.serves_peers, ServeVerdict::Serve);

    a.shutdown().await;
}

/// The view an explanation's probes set out under is the view it reports
/// its owners, marks and verdicts for, and `view_moved_to` names the view
/// the tracker holds once the probes return: published while a probe is
/// pending, it moves the report's last word and nothing before it. An
/// explanation taken after the move reports no move.
#[tokio::test]
async fn explain_reports_a_moved_view() {
    use crate::net::test_support::peer_at;
    let name = "explain-moved-view";
    let config = loopback_config().with(|c| c.fetch_timeout = Duration::from_millis(300));
    let a = solo_cluster(name, config).await;
    let me = a.node_id();
    // The cluster republishes its empty peer list the first time its
    // background tasks are polled, which would remove the peer below.
    tokio::task::yield_now().await;
    let silent = NodeId::from(u64::MAX);
    a.mesh()
        .update_peers(vec![peer_at(silent, silent_peer_addr().await)]);

    // Under `old` this node and the silent peer own every part; under
    // `moved` another node owns about half of them.
    let other = NodeId::from(u64::MAX - 1);
    let old = Arc::new(OwnershipView::compute(
        me,
        vec![silent],
        Mode::DEFAULT_OWNERS,
    ));
    let moved = Arc::new(OwnershipView::compute(me, vec![other], NonZeroU8::MIN));
    assert_ne!(old.view_hash(), moved.view_hash());
    let key = (0..1_000_000u32)
        .find(|key| !moved.owns(part_of(*key)))
        .expect("the other node owns some parts");
    let (cache, _residency, view_tx) = cache_publishing_views(&a, name, &old);

    let mut explain = std::pin::pin!(cache.explain(&key));
    assert!(
        futures::poll!(explain.as_mut()).is_pending(),
        "the silent owner holds the probe open"
    );
    view_tx
        .send(Arc::clone(&moved))
        .expect("the tracker holds a receiver");
    let explanation = explain.await.expect("u32 encodes");
    let read = explanation
        .distributed
        .expect("a distributed cache explains its distribution");
    assert_eq!(read.view_hash, old.view_hash());
    assert_eq!(read.view_moved_to, Some(moved.view_hash()));
    assert_eq!(read.owners, old.owners_of(part_of(key)));
    assert!(read.residency.owns, "owned under the view the probes used");
    assert_eq!(read.local_read, LocalRead::Miss);
    assert_eq!(
        read.probes,
        vec![OwnerProbe {
            node: silent,
            answer: ProbeAnswer::Unreached(Unreached::TimedOut),
        }]
    );
    assert_eq!(explanation.source, ReadSource::Local { hit: false });

    let (explanation, read) = explain_distributed(&cache, key).await;
    assert_eq!(read.view_hash, moved.view_hash());
    assert_eq!(read.view_moved_to, None);
    assert_eq!(read.owners, vec![other]);
    assert!(!read.residency.owns);
    assert_eq!(read.local_read, LocalRead::NotOwner);
    assert_eq!(explanation.source, ReadSource::Unavailable);

    a.shutdown().await;
}

/// A part this node owns since the last settled view and has not pulled
/// reads cold before rebalance marks it: `unsettled` and `owns`, so
/// `implicitly_cold`. `mark_cold` makes it `cold_marked`, which is no
/// longer implicit, and `mark_serving` clears both. A cold part answers
/// a missing key `ColdMiss`, a warm one `Miss`, and the entry it holds
/// is a `Hit` in either state.
#[tokio::test]
async fn explain_reports_implicit_then_marked_then_no_cold() {
    let name = "explain-cold-marks";
    let a = solo_cluster(name, loopback_config()).await;
    let me = a.node_id();
    // The last settled view owns about half the parts; the view the
    // cache decides under owns them all.
    let settled = OwnershipView::compute(me, vec![NodeId::from(u64::MAX)], NonZeroU8::MIN);
    let now = Arc::new(OwnershipView::compute(me, vec![me], NonZeroU8::MIN));
    let (cache, residency) = cache_under_view(&a, name, &now);
    residency.settle(&settled);

    let mut unsettled = (0..1_000_000u32).filter(|key| !settled.owns(part_of(*key)));
    let missing = unsettled.next().expect("the settled view lacks some parts");
    let held = unsettled
        .find(|key| part_of(*key) != part_of(missing))
        .expect("the settled view lacks parts of two kinds");
    ShardOps::apply_remote_batch(cache.shard.as_ref(), vec![test_record(held, "held", me)]).await;

    let implicit = Residency {
        owns: true,
        releasing_for: None,
        cold_marked: false,
        unsettled: true,
        unverified: false,
        stale: false,
    };
    let (explanation, read) = explain_distributed(&cache, missing).await;
    assert_eq!(read.residency, implicit);
    assert!(read.residency.implicitly_cold() && read.residency.cold());
    assert_eq!(read.local_read, LocalRead::ColdMiss);
    assert_eq!(
        explanation.source,
        ReadSource::Unavailable,
        "no other owner can vouch for the miss"
    );
    assert_eq!(read.serves_peers, ServeVerdict::DeclineCold);

    let parts = [part_of(missing), part_of(held)];
    residency.mark_cold(&parts);
    let (_, read) = explain_distributed(&cache, missing).await;
    assert_eq!(
        read.residency,
        Residency {
            cold_marked: true,
            ..implicit
        }
    );
    assert!(read.residency.cold() && !read.residency.implicitly_cold());
    assert_eq!(read.local_read, LocalRead::ColdMiss);
    let (explanation, read) = explain_distributed(&cache, held).await;
    assert!(read.residency.cold());
    assert_eq!(read.local_read, LocalRead::Hit);
    assert_eq!(explanation.source, ReadSource::Local { hit: true });
    assert_eq!(read.serves_peers, ServeVerdict::Serve);

    residency.mark_serving(&parts);
    let (explanation, read) = explain_distributed(&cache, missing).await;
    assert_eq!(read.residency, Residency::new(true));
    assert!(!read.residency.cold() && !read.residency.implicitly_cold());
    assert_eq!(read.local_read, LocalRead::Miss);
    assert_eq!(explanation.source, ReadSource::Local { hit: false });
    assert_eq!(read.serves_peers, ServeVerdict::Miss);
    let (explanation, read) = explain_distributed(&cache, held).await;
    assert_eq!(read.residency, Residency::new(true));
    assert_eq!(read.local_read, LocalRead::Hit);
    assert_eq!(explanation.source, ReadSource::Local { hit: true });

    a.shutdown().await;
}

/// A part this node no longer owns but holds through its disown grace
/// is `releasing_for` some time and not owned: this node's own read of
/// it is `NotOwner`, yet it serves the record it holds to a peer, which
/// it does not once the part is outside the grace.
#[tokio::test]
async fn explain_reports_a_releasing_node() {
    let name = "explain-releasing";
    let a = solo_cluster(name, loopback_config()).await;
    let me = a.node_id();
    let phantom = NodeId::from(u64::MAX);
    let owned = Arc::new(OwnershipView::compute(me, vec![me], NonZeroU8::MIN));
    let shared = Arc::new(OwnershipView::compute(me, vec![phantom], NonZeroU8::MIN));
    let (cache, residency, view_tx) = cache_publishing_views(&a, name, &owned);
    let key = (0..1_000_000u32)
        .find(|key| !shared.owns(part_of(*key)))
        .expect("the phantom owns some parts");
    // The record lands while this node owns its part, then the part moves.
    ShardOps::apply_remote_batch(cache.shard.as_ref(), vec![test_record(key, "held", me)]).await;
    view_tx
        .send(Arc::clone(&shared))
        .expect("the tracker holds a receiver");

    let (explanation, read) = explain_distributed(&cache, key).await;
    assert!(
        explanation.local.is_live(),
        "the record outlives the ownership"
    );
    assert_eq!(read.residency, Residency::new(false));
    assert_eq!(read.local_read, LocalRead::NotOwner);
    assert_eq!(
        read.serves_peers,
        ServeVerdict::Miss,
        "outside the grace the held record is not served"
    );

    residency.mark_releasing(&[part_of(key)]);
    let (explanation, read) = explain_distributed(&cache, key).await;
    assert!(read.residency.releasing_for.is_some());
    assert!(!read.residency.owns && read.residency.resident());
    assert_eq!(
        Residency {
            releasing_for: None,
            ..read.residency
        },
        Residency::new(false)
    );
    assert_eq!(read.local_read, LocalRead::NotOwner);
    assert_eq!(read.serves_peers, ServeVerdict::Serve);
    assert_eq!(read.owners, vec![phantom]);
    assert_eq!(
        read.probes,
        vec![OwnerProbe {
            node: phantom,
            answer: ProbeAnswer::Unreached(Unreached::NotAMember),
        }]
    );
    assert_eq!(explanation.source, ReadSource::Unavailable);

    a.shutdown().await;
}

/// Outside `Mode::Distributed` an explanation has no distribution. It
/// names the cache, this node, the mode, the key's part and the cache's
/// clock reading, and its source is this node's own copy: a write reads
/// `Live`, a key never written `Absent`, and a removed one a `Tombstone`
/// newer than the write, in every mode.
#[tokio::test]
async fn explain_in_local_invalidation_and_replicated_modes_has_no_distribution() {
    let cluster = solo_cluster("cache-it-explain-modes", loopback_config()).await;
    for (mode, name) in [
        (Mode::Local, "explain-mode-local"),
        (Mode::Invalidation, "explain-mode-invalidation"),
        (Mode::Replicated, "explain-mode-replicated"),
    ] {
        let cache = cluster
            .cache::<u32, String>(name)
            .mode(mode)
            .open()
            .await
            .expect("opens alone");
        cache.insert(1, "one".to_string()).await.expect("insert");

        let before = now_ms();
        let live = cache.explain(&1).await.expect("u32 encodes");
        let after = now_ms();
        assert_eq!(live.cache.as_str(), name);
        assert_eq!(live.node, cluster.node_id(), "{mode:?}");
        assert_eq!(live.mode, mode);
        assert_eq!(live.part, part_of(1), "{mode:?}");
        assert!((before..=after).contains(&live.at_ms), "{mode:?}: {live:?}");
        let LocalRecord::Live {
            version: written,
            expires_at_ms: None,
            spilled: false,
            ..
        } = live.local
        else {
            panic!("{mode:?}: an inserted key is live: {live:?}");
        };
        assert_eq!(live.source, ReadSource::Local { hit: true }, "{mode:?}");
        assert_eq!(live.distributed, None, "{mode:?}");

        let absent = cache.explain(&2).await.expect("u32 encodes");
        assert_eq!(absent.local, LocalRecord::Absent, "{mode:?}");
        assert_eq!(absent.source, ReadSource::Local { hit: false }, "{mode:?}");
        assert_eq!(absent.distributed, None, "{mode:?}");

        cache.remove(&1).await.expect("remove");
        let removed = cache.explain(&1).await.expect("u32 encodes");
        let LocalRecord::Tombstone {
            version: deleted, ..
        } = removed.local
        else {
            panic!("{mode:?}: a removed key leaves a tombstone: {removed:?}");
        };
        assert!(deleted > written, "{mode:?}");
        assert_eq!(removed.source, ReadSource::Local { hit: false }, "{mode:?}");
        assert_eq!(removed.distributed, None, "{mode:?}");

        cache.close().await;
    }
    cluster.shutdown().await;
}

/// A key type whose `Serialize` always fails, to reach `explain`'s
/// encode error.
#[derive(Clone, Hash, PartialEq, Eq, serde::Deserialize)]
struct Unencodable;

impl Serialize for Unencodable {
    fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("this key never encodes"))
    }
}

/// A key that fails to encode makes `explain` fail with a codec error,
/// in a `Mode::Distributed` cache before any owner is asked and in a
/// `Mode::Local` one alike.
#[tokio::test]
async fn explain_of_a_key_that_fails_to_encode_is_a_codec_error() {
    let cluster = solo_cluster("cache-it-explain-codec", loopback_config()).await;
    for (mode, name) in [
        (Mode::Local, "explain-codec-local"),
        (Mode::distributed(), "explain-codec-distributed"),
    ] {
        let cache = cluster
            .cache::<Unencodable, String>(name)
            .mode(mode)
            .open()
            .await
            .expect("opens alone");
        let result = cache.explain(&Unencodable).await;
        assert!(
            matches!(result, Err(CacheError::Codec(_))),
            "{mode:?}: {result:?}"
        );
        cache.close().await;
    }
    cluster.shutdown().await;
}

/// `Cache::explain`'s future is `Send`, so a caller can hold it across a
/// thread hop or spawn it: checked on a distributed cache, whose
/// explanation awaits its probes.
#[tokio::test]
async fn explain_future_is_send() {
    fn assert_send<T: Send>(_: &T) {}

    let name = "explain-send";
    let a = solo_cluster(name, loopback_config()).await;
    let phantom = NodeId::from(u64::MAX);
    let view = Arc::new(OwnershipView::compute(
        a.node_id(),
        vec![phantom],
        Mode::DEFAULT_OWNERS,
    ));
    let (cache, _residency) = cache_under_view(&a, name, &view);
    let key = 7u32;

    let future = cache.explain(&key);
    assert_send(&future);
    let direct = future.await.expect("u32 encodes");
    let spawned = tokio::spawn({
        let cache = cache.clone();
        async move { cache.explain(&key).await }
    })
    .await
    .expect("the task finishes")
    .expect("u32 encodes");
    assert_eq!(spawned.part, direct.part);
    assert_eq!(spawned.local, direct.local);

    a.shutdown().await;
}

/// `owner_answer`'s local branch over every residency state a read
/// tells apart, in front of owners no node can reach: it answers from
/// this node's copy iff the part is owned and trusted and the read hits
/// or the part is warm, and otherwise asks the owners and finds none.
#[tokio::test]
async fn the_owner_loop_reads_locally_by_the_read_rule_over_every_residency_state() {
    use crate::store::read_cases::{case_shard, every_read_case};
    let solo = Cluster::builder("cache-it-read-cases")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node builds");
    for case in every_read_case() {
        let built = case_shard("read-cases", &case).await;
        let cache = Cache {
            shard: Arc::new(built.shard),
            cluster: solo.clone(),
            cancel: CancellationToken::new(),
            tasks: TaskTracker::new(),
        };
        let mut attempts = 0;
        let answer = cache
            .owner_answer(
                &built.key,
                &built.key_bytes,
                Arc::clone(&built.view),
                &tracing::Span::none(),
                &mut attempts,
            )
            .await;
        let answers = case.owns && !case.distrusted() && (case.live() || !case.cold());
        match answer {
            Ok(OwnerAnswer::Local(value)) => {
                assert!(answers, "answered locally: {case:?}");
                assert_eq!(value.is_some(), case.live(), "{case:?}");
            }
            Err(CacheError::FetchUnavailable { .. }) => {
                assert!(!answers, "asked the owners: {case:?}");
            }
            Ok(OwnerAnswer::Remote(_)) | Err(_) => {
                panic!("no owner can answer: {case:?}");
            }
        }
    }
    solo.shutdown().await;
}

/// Asserts `local` is what a shard holding `held` stores for the case
/// key.
fn assert_stores(held: crate::store::read_cases::Held, local: &LocalRecord) {
    use crate::store::read_cases::Held;
    match held {
        Held::Nothing => assert_eq!(*local, LocalRecord::Absent),
        Held::Live => assert!(
            matches!(
                local,
                LocalRecord::Live {
                    expires_at_ms: None,
                    spilled: false,
                    ..
                }
            ),
            "{local:?}"
        ),
        Held::Tombstone => assert!(matches!(local, LocalRecord::Tombstone { .. }), "{local:?}"),
    }
}

/// Asserts `marks` are the residency marks `case` set, a running disown
/// grace counting as set whatever time it has run.
fn assert_marks_of_case(case: crate::store::read_cases::ReadCase, marks: Residency) {
    assert_eq!(
        Residency {
            releasing_for: None,
            ..marks
        },
        Residency {
            owns: case.owns,
            releasing_for: None,
            cold_marked: case.cold_marked,
            unsettled: case.unsettled,
            unverified: case.unverified,
            stale: case.stale,
        },
        "{case:?}"
    );
    assert_eq!(marks.releasing_for.is_some(), case.releasing, "{case:?}");
}

/// What a node in `case`'s state answers a peer's fetch sent with an
/// equal view hash: distrust declines even a held record, a held record
/// in a resident part is served, a cold part declines, and only a warm
/// part's miss is definitive.
fn serve_rule(case: crate::store::read_cases::ReadCase) -> ServeVerdict {
    if case.distrusted() {
        ServeVerdict::DeclineDistrusted
    } else if case.served() {
        ServeVerdict::Serve
    } else if case.cold() {
        ServeVerdict::DeclineCold
    } else {
        ServeVerdict::Miss
    }
}

/// `Cache::explain` against `Cache::fetch` over every residency state a
/// read tells apart, in front of an owner no node can reach. The marks
/// it reports are the ones the case set and the ones the shard reads
/// itself; its local read is the verdict those marks give and answers
/// iff the part is owned and trusted and the read hits or the part is
/// warm; it predicts what this node answers a peer; the one owner is
/// unreached; and the source it names is the one the fetch takes its
/// answer from.
#[tokio::test]
async fn explain_agrees_with_fetch_over_every_residency_state() {
    use crate::store::read_cases::{case_shard, every_read_case};
    let solo = solo_cluster("cache-it-explain-read-cases", loopback_config()).await;
    let (n1, n2) = (NodeId::from(1u64), NodeId::from(2u64));
    for case in every_read_case() {
        let built = case_shard("explain-cases", &case).await;
        let cache = Cache {
            shard: Arc::new(built.shard),
            cluster: solo.clone(),
            cancel: CancellationToken::new(),
            tasks: TaskTracker::new(),
        };
        let explanation = cache.explain(&built.key).await.expect("u32 encodes");
        let read = explanation
            .distributed
            .clone()
            .expect("a distributed cache explains its distribution");

        assert_eq!(explanation.cache.as_str(), "explain-cases");
        assert_eq!(explanation.node, solo.node_id());
        assert_eq!(
            explanation.mode,
            Mode::Distributed {
                owners: NonZeroU8::MIN
            }
        );
        assert_eq!(explanation.part, built.part);
        assert_stores(case.held, &explanation.local);

        assert_eq!(read.view_hash, built.view.view_hash(), "{case:?}");
        assert_eq!(read.view_moved_to, None, "{case:?}");
        let owner = if case.owns { n1 } else { n2 };
        assert_eq!(read.owners, vec![owner], "{case:?}");
        assert_eq!(read.owners, cache.owners_of(&built.key), "{case:?}");

        let direct = cache.shard.residency_of(&built.view, built.part);
        assert_marks_of_case(case, read.residency);
        assert_marks_of_case(case, direct);
        assert_eq!(
            Residency {
                releasing_for: None,
                ..read.residency
            },
            Residency {
                releasing_for: None,
                ..direct
            },
            "{case:?}"
        );
        assert_eq!(
            read.local_read,
            verdict::local_verdict(&direct, case.live()),
            "{case:?}"
        );
        let answers = case.owns && !case.distrusted() && (case.live() || !case.cold());
        assert_eq!(read.local_read.answers(), answers, "{case:?}");
        assert_eq!(read.serves_peers, serve_rule(case), "{case:?}");
        assert_eq!(
            read.probes,
            vec![OwnerProbe {
                node: owner,
                answer: ProbeAnswer::Unreached(Unreached::NotAMember),
            }],
            "{case:?}"
        );
        let source = if answers {
            ReadSource::Local { hit: case.live() }
        } else {
            ReadSource::Unavailable
        };
        assert_eq!(explanation.source, source, "{case:?}");

        let fetched = cache.fetch(&built.key).await;
        match (explanation.source, &fetched) {
            (ReadSource::Local { hit: true }, Ok(Some(value))) => {
                assert_eq!(value, "held", "{case:?}");
            }
            (ReadSource::Local { hit: false }, Ok(None))
            | (ReadSource::Unavailable, Err(CacheError::FetchUnavailable { .. })) => {}
            (source, fetched) => {
                panic!("{case:?}: explain says {source:?}, fetch returns {fetched:?}");
            }
        }
    }
    solo.shutdown().await;
}

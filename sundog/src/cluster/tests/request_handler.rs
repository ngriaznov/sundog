//! Tests for the cluster request handler: fetch, state-transfer and anti-entropy serving.

use super::*;
use crate::explain::ServeVerdict;
use crate::hlc::Hlc;
use crate::net::{AeServeOutcome, FetchServe, StServe};
use crate::ownership::Granularity;
use crate::store::{BucketDigest, BucketLen, BucketPart, BucketPartDigests, KeyVersion, PartMask};
use crate::wire::WireRecord;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::BoxStream;

#[tokio::test]
async fn records_for_hashes_answers_exactly_the_records_whose_hash_matches() {
    let cluster = solo_cluster("cluster-it-records-for-hashes").await;
    let cache = cluster
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("open succeeds");
    cache
        .insert_many([
            (1, "one".to_string()),
            (2, "two".into()),
            (3, "three".into()),
        ])
        .await
        .expect("insert");

    // Constructed the way `ClusterBuilder::build` constructs the handler
    // it hands to `Mesh::spawn`, against this single-node cluster's own
    // registry.
    let handler = ClusterRequestHandler {
        shards: cluster.shards(),
        warmth: Arc::clone(&cluster.inner.warmth),
        ae_part_min_bucket: cluster.config().ae_part_min_bucket,
        ae_sketch_min_bucket: cluster.config().ae_sketch_min_bucket,
        ae_sketch_cells: cluster.config().ae_sketch_cells,
        rebalance_chunk_bytes: cluster.config().rebalance_chunk_bytes_value(),
    };
    let name = SmolStr::new("users");
    let key_one = Bytes::from(postcard::to_stdvec(&1u32).expect("test key encodes"));
    let bucket = bucket_of_u32(1);
    let hash_one = xxhash_rust::xxh3::xxh3_64(&key_one);

    let records = handler
        .records_for_hashes(name.clone(), bucket, vec![hash_one])
        .await;
    assert_eq!(records.len(), 1, "exactly the one matching hash comes back");
    assert_eq!(records[0].key, key_one);

    let none = handler
        .records_for_hashes(name, bucket, vec![hash_one.wrapping_add(1)])
        .await;
    assert!(none.is_empty(), "a hash matching nothing yields nothing");

    cluster.shutdown().await;
}

#[tokio::test]
async fn cluster_request_handler_degrades_gracefully_for_an_unregistered_cache() {
    // An unregistered cache name answers every distribution-mode method
    // with its `Unavailable` shape, the same "unknown cache degrades
    // gracefully" convention every other method on this trait already
    // has.
    let handler = ClusterRequestHandler {
        shards: Arc::new(RwLock::new(HashMap::new())),
        warmth: Arc::new(Warmth::default()),
        ae_part_min_bucket: ClusterConfig::default().ae_part_min_bucket,
        ae_sketch_min_bucket: ClusterConfig::default().ae_sketch_min_bucket,
        ae_sketch_cells: ClusterConfig::default().ae_sketch_cells,
        rebalance_chunk_bytes: ClusterConfig::default().rebalance_chunk_bytes_value(),
    };
    let name = SmolStr::new("users");

    assert_eq!(handler.ownership_view_hash(name.clone()), None);
    assert!(matches!(
        handler
            .fetch(name.clone(), Bytes::from_static(b"k1"), 1)
            .await,
        FetchServe::Unavailable
    ));
    assert!(matches!(
        handler.ae_digest_scoped(name.clone(), 1, Vec::new()).await,
        AeServeOutcome::Unavailable
    ));
    assert!(matches!(
        handler.ae_digest_masked(name.clone(), 1, Vec::new()).await,
        AeServeOutcome::Unavailable
    ));
    assert!(!handler.st_buckets_available(name.clone(), 1).await);
    let mut chunks = handler.st_bucket_chunks(name, vec![0]);
    assert!(
        chunks.next().await.is_none(),
        "an unregistered cache's chunk stream is immediately empty"
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines, reason = "one handler walk-through per RPC")]
async fn cluster_request_handler_serves_a_distributed_cache_by_view_hash() {
    let cluster = solo_cluster("cluster-it-request-handler-distributed").await;
    let cache = cluster
        .cache::<u32, String>("prices")
        .mode(Mode::Distributed {
            owners: std::num::NonZeroU8::new(2).expect("nonzero"),
        })
        .open()
        .await
        .expect("a solo node opens a distributed cache, owning every bucket");
    cache.insert(1, "one".to_string()).await.expect("insert");
    let other_key = (2..1_000u32)
        .find(|k| bucket_of_u32(*k) != bucket_of_u32(1))
        .expect("a key in another bucket is found within a generous scan range");
    let part_one = part_of_u32(1);
    let part_other = part_of_u32(other_key);
    cache
        .insert(other_key, "other".to_string())
        .await
        .expect("insert");

    let handler = ClusterRequestHandler {
        shards: cluster.shards(),
        warmth: Arc::clone(&cluster.inner.warmth),
        ae_part_min_bucket: cluster.config().ae_part_min_bucket,
        ae_sketch_min_bucket: cluster.config().ae_sketch_min_bucket,
        ae_sketch_cells: cluster.config().ae_sketch_cells,
        rebalance_chunk_bytes: cluster.config().rebalance_chunk_bytes_value(),
    };
    let name = SmolStr::new("prices");
    let view_hash = handler
        .ownership_view_hash(name.clone())
        .expect("a distributed cache reports a view hash");
    let key_one = Bytes::from(postcard::to_stdvec(&1u32).expect("test key encodes"));

    let FetchServe::Found(Some(rec)) = handler
        .fetch(name.clone(), key_one.clone(), view_hash)
        .await
    else {
        panic!("expected a found record for a key this sole owner holds");
    };
    assert_eq!(rec.key, key_one);
    // A held record answers even against a mismatched view hash; only
    // a miss is declined as stale.
    assert!(matches!(
        handler
            .fetch(name.clone(), key_one.clone(), view_hash.wrapping_add(1))
            .await,
        FetchServe::Found(Some(rec)) if rec.key == key_one
    ));
    let key_absent = Bytes::from(postcard::to_stdvec(&999_999u32).expect("test key encodes"));
    assert!(matches!(
        handler
            .fetch(name.clone(), key_absent.clone(), view_hash.wrapping_add(1))
            .await,
        FetchServe::Stale { responder_view_hash } if responder_view_hash == view_hash
    ));
    assert!(matches!(
        handler.fetch(name.clone(), key_absent, view_hash).await,
        FetchServe::Found(None)
    ));

    // Every eligible node speaks part ownership, so the view is
    // part-granular and rounds arrive masked.
    let masked = |part: PartId, digest| {
        let mask: PartMask = [part.part()].into_iter().collect();
        (part.bucket(), mask.bits(), digest)
    };
    let AeServeOutcome::Digests(mismatched) = handler
        .ae_digest_masked(
            name.clone(),
            view_hash,
            vec![masked(part_one, 0), masked(part_other, 0)],
        )
        .await
    else {
        panic!("expected the owner's part digests when the view hash matches");
    };
    let by_bucket: HashMap<u16, Vec<u64>> = mismatched
        .into_iter()
        .map(|bpd| (bpd.bucket, bpd.digests))
        .collect();
    for part in [part_one, part_other] {
        let digest = by_bucket[&part.bucket()][usize::from(part.part())];
        assert_ne!(digest, 0, "each requested part holds a record");
        let AeServeOutcome::Digests(settled) = handler
            .ae_digest_masked(name.clone(), view_hash, vec![masked(part, digest)])
            .await
        else {
            panic!("the view hash still matches");
        };
        assert!(settled.is_empty(), "a matching fold draws no reply");
    }
    assert!(matches!(
        handler
            .ae_digest_masked(name.clone(), view_hash.wrapping_add(1), Vec::new())
            .await,
        AeServeOutcome::Stale { responder_view_hash } if responder_view_hash == view_hash
    ));
    assert!(matches!(
        handler
            .ae_digest_scoped(name.clone(), view_hash.wrapping_add(1), Vec::new())
            .await,
        AeServeOutcome::Stale { responder_view_hash } if responder_view_hash == view_hash
    ));

    assert!(handler.st_buckets_available(name.clone(), view_hash).await);
    assert!(
        !handler
            .st_buckets_cold(name.clone(), vec![part_one.raw()])
            .await,
        "a warm owner is a source for its part"
    );
    assert!(matches!(
        handler
            .st_serve(name.clone(), vec![part_one.raw()], view_hash.wrapping_add(1))
            .await,
        StServe::Stale { responder_view_hash } if responder_view_hash == view_hash
    ));
    let StServe::Stream {
        mut chunks, parts, ..
    } = handler
        .st_serve(
            name.clone(),
            vec![part_one.raw(), part_other.raw()],
            view_hash,
        )
        .await
    else {
        panic!("a warm owner at the requester's view hash donates");
    };
    assert_eq!(parts, 2, "two part ids cover two parts");
    let mut served: Vec<u16> = Vec::new();
    while let Some((id, _)) = chunks.next().await {
        served.push(id);
    }
    assert_eq!(served, vec![part_one.raw(), part_other.raw()]);
    assert!(
        !handler
            .st_buckets_available(name.clone(), view_hash.wrapping_add(1))
            .await
    );

    let owned_part = part_one.raw();
    let mut chunks = handler.st_bucket_chunks(name.clone(), vec![owned_part]);
    let mut got_key = false;
    while let Some((id, chunk)) = chunks.next().await {
        assert_eq!(
            id, owned_part,
            "every chunk is tagged with the requested part"
        );
        if chunk.iter().any(|r| r.key == key_one) {
            got_key = true;
        }
    }
    assert!(
        got_key,
        "bucket_chunks streams the requested owned part's contents"
    );

    // A multi-part pull streams one part at a time, bucket by bucket
    // whatever the request order, each chunk tagged with its part.
    let key_other = Bytes::from(postcard::to_stdvec(&other_key).expect("test key encodes"));
    let mut chunks = handler.st_bucket_chunks(name, vec![part_other.raw(), owned_part]);
    let mut order: Vec<Bytes> = Vec::new();
    while let Some((id, chunk)) = chunks.next().await {
        assert!(
            chunk.iter().all(|r| PartId::of_key(&r.key).raw() == id),
            "a chunk never spans two parts, and its tag matches its records"
        );
        order.extend(chunk.into_iter().map(|r| r.key));
    }
    let expected = if part_one.bucket() < part_other.bucket() {
        vec![key_one, key_other]
    } else {
        vec![key_other, key_one]
    };
    assert_eq!(
        order, expected,
        "parts stream in bucket order, one at a time"
    );

    cluster.shutdown().await;
}

#[test]
fn serve_reply_maps_each_verdict() {
    let rec = WireRecord {
        key: Bytes::from_static(b"k"),
        value: None,
        ver: Hlc {
            wall_ms: 1,
            logical: 0,
            node: NodeId::from(1u64),
        },
        expires_at_ms: None,
    };
    assert!(matches!(
        serve_reply(ServeVerdict::Serve, Some(rec.clone()), 7),
        FetchServe::Found(Some(served)) if served == rec
    ));
    assert!(matches!(
        serve_reply(ServeVerdict::Miss, None, 7),
        FetchServe::Found(None)
    ));
    assert!(matches!(
        serve_reply(ServeVerdict::Stale, None, 7),
        FetchServe::Stale {
            responder_view_hash: 7
        }
    ));
    for declined in [ServeVerdict::DeclineDistrusted, ServeVerdict::DeclineCold] {
        assert!(matches!(
            serve_reply(declined, Some(rec.clone()), 7),
            FetchServe::Unavailable
        ));
    }
}

/// A responder with `shard` registered under `name`, and nothing else.
fn handler_for(name: &SmolStr, shard: Arc<dyn ShardOps>) -> ClusterRequestHandler {
    let shards: ShardRegistry = Arc::new(RwLock::new(HashMap::from([(name.clone(), shard)])));
    ClusterRequestHandler {
        shards,
        warmth: Arc::new(Warmth::default()),
        ae_part_min_bucket: ClusterConfig::default().ae_part_min_bucket,
        ae_sketch_min_bucket: ClusterConfig::default().ae_sketch_min_bucket,
        ae_sketch_cells: ClusterConfig::default().ae_sketch_cells,
        rebalance_chunk_bytes: ClusterConfig::default().rebalance_chunk_bytes_value(),
    }
}

/// The responder over every residency state a read tells apart, on an
/// equal and an unequal view hash, against its serve rule written out
/// longhand: distrust declines even a held record, a held
/// record in a resident part is served whatever the views say, a view
/// mismatch then answers stale, a cold part declines, and only a warm
/// part's miss is definitive.
#[tokio::test]
async fn the_fetch_responder_follows_its_serve_rule_over_every_residency_state() {
    use crate::store::read_cases::{case_shard, every_read_case};
    let name = SmolStr::new("serve-cases");
    for case in every_read_case() {
        let built = case_shard(&name, &case).await;
        let local_hash = built.view.view_hash();
        let handler = handler_for(&name, Arc::new(built.shard));
        for views_match in [true, false] {
            let asked_hash = if views_match {
                local_hash
            } else {
                local_hash ^ 1
            };
            let reply = handler
                .fetch(name.clone(), built.key_bytes.clone(), asked_hash)
                .await;
            let expected = if case.distrusted() {
                "unavailable"
            } else if case.served() {
                "found"
            } else if !views_match {
                "stale"
            } else if case.cold() {
                "unavailable"
            } else {
                "miss"
            };
            let got = match &reply {
                FetchServe::Unavailable => "unavailable",
                FetchServe::Found(Some(rec)) => {
                    assert_eq!(rec.key, built.key_bytes, "{case:?}");
                    assert_eq!(
                        rec.value.is_some(),
                        case.live(),
                        "the held record is served as held: {case:?}"
                    );
                    "found"
                }
                FetchServe::Found(None) => "miss",
                FetchServe::Stale {
                    responder_view_hash,
                } => {
                    assert_eq!(*responder_view_hash, local_hash, "{case:?}");
                    "stale"
                }
            };
            assert_eq!(got, expected, "{case:?} views_match={views_match}");
        }
    }
}

/// A shard whose `records_for` lets a pull land right after its read:
/// once the inner shard's records are read, the pull's record is
/// applied and `part` marked serving, as a pull applies its records and
/// then clears the part's cold mark. Every other method the fetch
/// responder does not call panics.
struct PullDuringRead {
    inner: Arc<Shard<u32, String>>,
    residency: Arc<crate::ownership::ResidencySet>,
    part: PartId,
    pulled: WireRecord,
}

impl ShardOps for PullDuringRead {
    fn apply_remote(&self, _rec: WireRecord) -> BoxFuture<'_, ()> {
        unimplemented!("the fetch responder never applies")
    }

    fn apply_remote_batch(&self, _recs: Vec<WireRecord>) -> BoxFuture<'_, ()> {
        unimplemented!("the fetch responder never applies")
    }

    fn invalidate(&self, _key: Bytes, _ver: Hlc) -> BoxFuture<'_, ()> {
        unimplemented!("the fetch responder never invalidates")
    }

    fn digests(&self) -> BoxFuture<'_, Vec<BucketDigest>> {
        unimplemented!("the fetch responder never reads digests")
    }

    fn bucket_entries(&self, _bucket: u16) -> BoxFuture<'_, Vec<KeyVersion>> {
        unimplemented!("the fetch responder never lists a bucket")
    }

    fn entries_for_buckets(
        &self,
        _buckets: Vec<u16>,
    ) -> BoxFuture<'_, crate::store::BucketEntries> {
        unimplemented!("the fetch responder never lists a bucket")
    }

    fn bucket_lens(&self, _buckets: Vec<u16>) -> BoxFuture<'_, Vec<BucketLen>> {
        unimplemented!("the fetch responder never counts a bucket")
    }

    fn part_digests(&self, _buckets: Vec<u16>) -> BoxFuture<'_, Vec<BucketPartDigests>> {
        unimplemented!("the fetch responder never reads digests")
    }

    fn entries_for_parts(
        &self,
        _parts: Vec<BucketPart>,
    ) -> BoxFuture<'_, crate::store::PartEntries> {
        unimplemented!("the fetch responder never lists a part")
    }

    fn records_for(&self, keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>> {
        Box::pin(async move {
            let read = ShardOps::records_for(self.inner.as_ref(), keys).await;
            ShardOps::apply_remote_batch(self.inner.as_ref(), vec![self.pulled.clone()]).await;
            self.residency.mark_serving(&[self.part]);
            read
        })
    }

    fn snapshot_chunks(&self) -> BoxStream<'static, Vec<WireRecord>> {
        unimplemented!("the fetch responder never snapshots")
    }

    fn gc_tombstones(&self, _any_member_absent: bool) -> BoxFuture<'_, ()> {
        unimplemented!("the fetch responder never collects tombstones")
    }

    fn run_pending_tasks(&self) -> BoxFuture<'_, ()> {
        unimplemented!("the fetch responder never sweeps")
    }

    fn ownership_view_hash(&self) -> Option<u64> {
        ShardOps::ownership_view_hash(self.inner.as_ref())
    }

    fn is_cold_part(&self, part: PartId) -> bool {
        ShardOps::is_cold_part(self.inner.as_ref(), part)
    }

    fn is_unverified_part(&self, part: PartId) -> bool {
        ShardOps::is_unverified_part(self.inner.as_ref(), part)
    }
}

/// The responder reads a part's marks before its record: a pull that
/// lands its record and clears the part's cold mark after the
/// responder read a miss leaves that miss undefinitive, so the responder
/// declines, and a fetch after the pull is served the pulled record.
#[tokio::test]
async fn the_fetch_responder_reads_the_cold_mark_before_the_record() {
    use crate::store::read_cases::{Held, ReadCase, case_shard};
    let name = SmolStr::new("serve-pull-during-read");
    let built = case_shard(
        &name,
        &ReadCase {
            owns: true,
            cold_marked: true,
            unsettled: false,
            stale: false,
            unverified: false,
            releasing: false,
            held: Held::Nothing,
        },
    )
    .await;
    let pulled = WireRecord {
        key: built.key_bytes.clone(),
        value: Some(Bytes::from(
            postcard::to_stdvec(&"pulled".to_string()).expect("a string encodes"),
        )),
        ver: Hlc {
            wall_ms: crate::store::now_ms(),
            logical: 1,
            node: NodeId::from(2u64),
        },
        expires_at_ms: None,
    };
    let inner = Arc::new(built.shard);
    let handler = handler_for(
        &name,
        Arc::new(PullDuringRead {
            inner: Arc::clone(&inner),
            residency: Arc::clone(&built.residency),
            part: built.part,
            pulled: pulled.clone(),
        }),
    );
    let hash = built.view.view_hash();

    let reply = handler
        .fetch(name.clone(), built.key_bytes.clone(), hash)
        .await;
    assert!(
        matches!(reply, FetchServe::Unavailable),
        "a miss read while the part was cold is no answer"
    );
    assert!(!ShardOps::is_cold_part(inner.as_ref(), built.part));

    let reply = handler_for(&name, inner)
        .fetch(name.clone(), built.key_bytes.clone(), hash)
        .await;
    assert!(
        matches!(reply, FetchServe::Found(Some(ref rec)) if *rec == pulled),
        "the pulled record is served once it landed"
    );
}

/// `verdict::serves_peers`, what `Cache::explain` reports this node
/// answers a peer, over every residency state a read tells apart: the
/// responder's reply to a fetch sent with an equal view hash.
#[tokio::test]
async fn serves_peers_is_the_responders_answer_on_an_equal_view_hash() {
    use crate::explain::ServeVerdict;
    use crate::store::read_cases::{case_shard, every_read_case};
    let name = SmolStr::new("serves-peers-cases");
    for case in every_read_case() {
        let built = case_shard(&name, &case).await;
        let shard = Arc::new(built.shard);
        let predicted = verdict::serves_peers(
            &shard.residency_of(&built.view, built.part),
            &shard.local_record(&built.key_bytes).1,
        );
        let handler = handler_for(&name, Arc::clone(&shard) as Arc<dyn ShardOps>);
        let reply = handler
            .fetch(
                name.clone(),
                built.key_bytes.clone(),
                built.view.view_hash(),
            )
            .await;
        let answered = match reply {
            FetchServe::Found(Some(_)) => ServeVerdict::Serve,
            FetchServe::Found(None) => ServeVerdict::Miss,
            FetchServe::Stale { .. } => ServeVerdict::Stale,
            FetchServe::Unavailable => match predicted {
                ServeVerdict::DeclineDistrusted | ServeVerdict::DeclineCold => predicted,
                _ => panic!("declined where explain predicts {predicted:?}: {case:?}"),
            },
        };
        assert_eq!(answered, predicted, "{case:?}");
    }
}

/// A hit in an unverified bucket answers `Unavailable`, never the
/// record itself, as if this responder held nothing at all.
#[cfg(feature = "spill")]
#[tokio::test]
async fn cluster_request_handler_answers_unavailable_for_a_hit_in_an_unverified_bucket() {
    let name = SmolStr::new("prices");
    let owners = std::num::NonZeroU8::new(2).expect("nonzero");
    let node = NodeId::from(1u64);
    let (tracker, tx) =
        crate::ownership::OwnershipTracker::seed(node, &[], &HashMap::new(), &name, owners);
    let view = Arc::new(OwnershipView::compute(node, vec![node], owners));
    tx.send(Arc::clone(&view)).expect("receiver alive");
    let residency = Arc::new(crate::ownership::ResidencySet::new());
    let shard = Shard::<u32, String>::new(
        name.clone(),
        Mode::Distributed { owners },
        node,
        u64::MAX,
        None,
        None,
    )
    .with_ownership(tracker, Arc::clone(&residency));
    let shard_ops: Arc<dyn ShardOps> = Arc::new(shard);

    let key = 1u32;
    let key_bytes = Bytes::from(postcard::to_stdvec(&key).expect("test key encodes"));
    let record = WireRecord {
        key: key_bytes.clone(),
        value: Some(Bytes::from(
            postcard::to_stdvec(&"one".to_string()).expect("test value encodes"),
        )),
        ver: Hlc {
            wall_ms: 1,
            logical: 0,
            node,
        },
        expires_at_ms: None,
    };
    shard_ops.apply_remote_batch(vec![record]).await;

    let shards: ShardRegistry = Arc::new(RwLock::new(HashMap::from([(
        name.clone(),
        Arc::clone(&shard_ops),
    )])));
    let handler = ClusterRequestHandler {
        shards,
        warmth: Arc::new(Warmth::default()),
        ae_part_min_bucket: ClusterConfig::default().ae_part_min_bucket,
        ae_sketch_min_bucket: ClusterConfig::default().ae_sketch_min_bucket,
        ae_sketch_cells: ClusterConfig::default().ae_sketch_cells,
        rebalance_chunk_bytes: ClusterConfig::default().rebalance_chunk_bytes_value(),
    };
    let view_hash = handler
        .ownership_view_hash(name.clone())
        .expect("a registered distributed cache reports a view hash");

    let part = PartId::of_key(&key_bytes);
    residency.mark_cold(&[part]);
    residency.mark_unverified(&[part]);

    assert!(
        matches!(
            handler
                .fetch(name.clone(), key_bytes.clone(), view_hash)
                .await,
            FetchServe::Unavailable
        ),
        "a hit in an unverified bucket answers Unavailable, never the record itself"
    );

    residency.clear_cold(&[part]);
    residency.clear_unverified(&[part]);
    let FetchServe::Found(Some(rec)) = handler
        .fetch(name.clone(), key_bytes.clone(), view_hash)
        .await
    else {
        panic!("expected the record again once the bucket is no longer unverified");
    };
    assert_eq!(rec.key, key_bytes);
}

/// `st_serve` reads the requester's ids at the matching view's
/// granularity for the cold check too: a cold part declines the pull,
/// a warm one in the same bucket does not.
#[tokio::test]
async fn cluster_request_handler_st_serve_declines_a_pull_naming_a_cold_part() {
    let name = SmolStr::new("prices");
    let owners = std::num::NonZeroU8::new(2).expect("nonzero");
    let node = NodeId::from(1u64);
    let (tracker, _tx) =
        crate::ownership::OwnershipTracker::seed(node, &[], &HashMap::new(), &name, owners);
    let view = tracker.current();
    assert_eq!(
        view.granularity(),
        Granularity::Part,
        "a lone current node ranks parts"
    );
    let residency = Arc::new(crate::ownership::ResidencySet::new());
    let shard_ops: Arc<dyn ShardOps> = Arc::new(
        Shard::<u32, String>::new(
            name.clone(),
            Mode::Distributed { owners },
            node,
            u64::MAX,
            None,
            None,
        )
        .with_ownership(tracker, Arc::clone(&residency)),
    );
    let shards: ShardRegistry = Arc::new(RwLock::new(HashMap::from([(name.clone(), shard_ops)])));
    let handler = ClusterRequestHandler {
        shards,
        warmth: Arc::new(Warmth::default()),
        ae_part_min_bucket: ClusterConfig::default().ae_part_min_bucket,
        ae_sketch_min_bucket: ClusterConfig::default().ae_sketch_min_bucket,
        ae_sketch_cells: ClusterConfig::default().ae_sketch_cells,
        rebalance_chunk_bytes: ClusterConfig::default().rebalance_chunk_bytes_value(),
    };
    let cold = PartId::new(9, 1);
    let warm = PartId::new(9, 2);
    residency.mark_cold(&[cold]);
    assert!(matches!(
        handler
            .st_serve(name.clone(), vec![warm.raw(), cold.raw()], view.view_hash())
            .await,
        StServe::Cold
    ));
    let StServe::Stream { order, parts, .. } = handler
        .st_serve(name.clone(), vec![warm.raw()], view.view_hash())
        .await
    else {
        panic!("a warm part donates");
    };
    assert_eq!((order, parts), (vec![warm.raw()], 1));
    assert!(matches!(
        handler
            .st_serve(SmolStr::new("unknown"), vec![warm.raw()], view.view_hash())
            .await,
        StServe::Stale {
            responder_view_hash: 0
        }
    ));
}

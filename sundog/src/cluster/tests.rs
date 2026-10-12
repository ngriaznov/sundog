//! Shared helpers and the topic modules of the cluster unit tests.

use super::test_support::{loopback_config, wait_for_peer_count, wait_until};
use super::*;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

mod ae_repair;
mod background;
mod builder;
mod distributed;
mod modes;
mod resolvers;
mod solo;
mod warmth;

/// A single-node cluster over loopback, the common case for a test that
/// needs no peers at all.
async fn solo_cluster(name: &str) -> Cluster {
    Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds")
}

async fn two_node_cluster(cluster_name: &str) -> (Cluster, Cluster) {
    two_node_cluster_with_config(cluster_name, loopback_config()).await
}

/// A fresh two-node loopback pair on `config`, joined and converged
/// before this returns: `b` seeds off `a`'s gossip address, then both
/// see one live peer.
async fn two_node_cluster_with_config(
    cluster_name: &str,
    config: ClusterConfig,
) -> (Cluster, Cluster) {
    let cluster_a = Cluster::builder(cluster_name)
        .seeds(std::iter::empty())
        .config(config.clone())
        .build()
        .await
        .expect("node a builds");
    let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;

    let cluster_b = Cluster::builder(cluster_name)
        .seeds([gossip_a])
        .config(config)
        .build()
        .await
        .expect("node b builds");

    wait_for_peer_count(&cluster_a, 1).await;
    wait_for_peer_count(&cluster_b, 1).await;
    (cluster_a, cluster_b)
}

/// A fresh 3-node cluster, all-to-all: every node sees the other two
/// before this returns.
async fn three_node_cluster(cluster_name: &str) -> (Cluster, Cluster, Cluster) {
    let (cluster_a, cluster_b) = two_node_cluster(cluster_name).await;
    let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;
    let gossip_b = cluster_b.inner.membership.local_peer().gossip_addr;
    let cluster_c = Cluster::builder(cluster_name)
        .seeds([gossip_a, gossip_b])
        .config(loopback_config())
        .build()
        .await
        .expect("node c builds");
    wait_for_peer_count(&cluster_a, 2).await;
    wait_for_peer_count(&cluster_b, 2).await;
    wait_for_peer_count(&cluster_c, 2).await;
    (cluster_a, cluster_b, cluster_c)
}

/// Polls until `cluster` sees a live peer advertising `cache` under some
/// [`Mode`]. Needed before a second `open()`, or it could race the first.
async fn wait_for_cache_advertised(cluster: &Cluster, cache: &str) {
    wait_until(
        Duration::from_secs(15),
        "peer's cache-mode advertisement converges within the bound",
        async || {
            cluster
                .advertised_cache_modes()
                .values()
                .any(|caches| caches.contains_key(cache))
        },
    )
    .await;
}

/// `key`'s part, from its postcard-encoded bytes.
fn part_of_u32(key: u32) -> crate::store::PartId {
    crate::store::PartId::of_key(
        &postcard::to_stdvec(&key).expect("a u32 key always postcard-encodes"),
    )
}

/// Mirrors `store::bucket_of`'s formula so a test can compute which
/// anti-entropy bucket a key lands in without that private function.
fn bucket_of_u32(key: u32) -> u16 {
    let bytes = postcard::to_stdvec(&key).expect("a u32 key always postcard-encodes");
    let bucket = xxhash_rust::xxh3::xxh3_64(&bytes) & (crate::store::BUCKET_COUNT as u64 - 1);
    u16::try_from(bucket).expect("masked to BUCKET_COUNT - 1, always fits in u16")
}

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

#[test]
fn group_in_order_keeps_first_seen_key_order_and_input_value_order() {
    let groups = group_in_order([('b', [1]), ('a', [2]), ('b', [3])]);
    assert_eq!(groups, vec![('b', vec![1, 3]), ('a', vec![2])]);
    let merged = group_in_order([("k", vec![1, 2]), ("k", vec![]), ("k", vec![3])]);
    assert_eq!(merged, vec![("k", vec![1, 2, 3])]);
    let groups = group_in_order(Vec::<(u8, [u8; 0])>::new());
    assert!(groups.is_empty(), "{groups:?}");
}

#[test]
fn serve_order_sorts_bucket_ids_and_groups_part_ids_by_bucket() {
    assert_eq!(serve_order(Granularity::Bucket, vec![9, 3, 9]), vec![3, 9]);
    let (a, b, c) = (PartId::new(5, 2), PartId::new(3, 9), PartId::new(5, 0));
    assert_eq!(
        serve_order(Granularity::Part, vec![a.raw(), b.raw(), c.raw(), a.raw()]),
        vec![b.raw(), c.raw(), a.raw()],
        "bucket 3 first, then bucket 5's parts in part order, each once"
    );
}

#[test]
fn bucket_runs_groups_consecutive_part_ids_of_one_bucket() {
    let runs = |granularity, ids: &[u16]| -> Vec<Vec<u16>> {
        bucket_runs(granularity, ids).map(<[u16]>::to_vec).collect()
    };
    assert_eq!(
        runs(Granularity::Bucket, &[3, 9]),
        vec![vec![3], vec![9]],
        "a bucket id is a run of its own"
    );
    let ids = serve_order(
        Granularity::Part,
        vec![
            PartId::new(5, 2).raw(),
            PartId::new(3, 9).raw(),
            PartId::new(5, 0).raw(),
        ],
    );
    assert_eq!(
        runs(Granularity::Part, &ids),
        vec![
            vec![PartId::new(3, 9).raw()],
            vec![PartId::new(5, 0).raw(), PartId::new(5, 2).raw()],
        ]
    );
    let no_runs = runs(Granularity::Part, &[]);
    assert!(no_runs.is_empty(), "{no_runs:?}");
}

#[test]
fn st_bucket_key_sub_batches_of_no_keys_is_empty() {
    assert_eq!(
        st_bucket_key_sub_batches(Vec::new(), 1024),
        Vec::<Vec<Bytes>>::new()
    );
}

#[test]
fn st_bucket_key_sub_batches_splits_by_estimated_size() {
    let keys: Vec<Bytes> = (0u8..5).map(|n| Bytes::from(vec![n])).collect();
    let per_key = (wire::RECORD_HEADER_LEN + 1) as u64;
    let sub_batches = st_bucket_key_sub_batches(keys.clone(), per_key * 2);
    assert_eq!(
        sub_batches,
        vec![
            vec![keys[0].clone(), keys[1].clone()],
            vec![keys[2].clone(), keys[3].clone()],
            vec![keys[4].clone()],
        ],
        "two one-byte keys fit the two-key budget exactly before a group closes"
    );
}

#[test]
fn st_bucket_key_sub_batches_gives_an_oversized_key_its_own_batch() {
    let small = Bytes::from_static(b"a");
    let big = Bytes::from(vec![0u8; 4096]);
    // Fits `small`; nowhere near enough for `big`.
    let budget = (wire::RECORD_HEADER_LEN + 1) as u64;
    let sub_batches = st_bucket_key_sub_batches(vec![small.clone(), big.clone()], budget);
    assert_eq!(
        sub_batches,
        vec![vec![small], vec![big]],
        "a key whose own estimate exceeds the budget still gets a batch of its own"
    );
}

/// A [`ShardOps`] test double recording every `records_for` call's key
/// list. Every method but `entries_for_buckets`, `records_for`, and
/// `close_spill` panics if called, since that would be a test bug.
struct RecordingShard {
    entries: Vec<(u16, Vec<KeyVersion>)>,
    held: Vec<u16>,
    records: HashMap<Bytes, WireRecord>,
    records_for_calls: Mutex<Vec<Vec<Bytes>>>,
    close_spill_calls: AtomicUsize,
}

impl ShardOps for RecordingShard {
    fn apply_remote(&self, _rec: WireRecord) -> BoxFuture<'_, ()> {
        unimplemented!("st_bucket_chunks never calls apply_remote")
    }

    fn apply_remote_batch(&self, _recs: Vec<WireRecord>) -> BoxFuture<'_, ()> {
        unimplemented!("st_bucket_chunks never calls apply_remote_batch")
    }

    fn invalidate(&self, _key: Bytes, _ver: Hlc) -> BoxFuture<'_, ()> {
        unimplemented!("st_bucket_chunks never calls invalidate")
    }

    fn digests(&self) -> BoxFuture<'_, Vec<BucketDigest>> {
        unimplemented!("st_bucket_chunks never calls digests")
    }

    fn bucket_entries(&self, _bucket: u16) -> BoxFuture<'_, Vec<KeyVersion>> {
        unimplemented!("st_bucket_chunks calls entries_for_buckets, not bucket_entries")
    }

    fn entries_for_buckets(&self, buckets: Vec<u16>) -> BoxFuture<'_, crate::store::BucketEntries> {
        let entries = self.entries.clone();
        Box::pin(async move {
            entries
                .into_iter()
                .filter(|(bucket, _)| buckets.contains(bucket))
                .collect()
        })
    }

    fn bucket_lens(&self, _buckets: Vec<u16>) -> BoxFuture<'_, Vec<BucketLen>> {
        unimplemented!("st_bucket_chunks never calls bucket_lens")
    }

    fn part_digests(&self, _buckets: Vec<u16>) -> BoxFuture<'_, Vec<BucketPartDigests>> {
        unimplemented!("st_bucket_chunks never calls part_digests")
    }

    fn entries_for_parts(
        &self,
        _parts: Vec<BucketPart>,
    ) -> BoxFuture<'_, crate::store::PartEntries> {
        unimplemented!("st_bucket_chunks never calls entries_for_parts")
    }

    fn records_for(&self, keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>> {
        self.records_for_calls
            .lock()
            .expect("invariant: fixture mutex is never poisoned")
            .push(keys.clone());
        Box::pin(async move {
            keys.into_iter()
                .filter_map(|k| self.records.get(&k).cloned())
                .collect()
        })
    }

    fn snapshot_chunks(&self) -> BoxStream<'static, Vec<WireRecord>> {
        unimplemented!("st_bucket_chunks never calls snapshot_chunks")
    }

    fn gc_tombstones(&self, _any_member_absent: bool) -> BoxFuture<'_, ()> {
        unimplemented!("st_bucket_chunks never calls gc_tombstones")
    }

    fn run_pending_tasks(&self) -> BoxFuture<'_, ()> {
        unimplemented!("st_bucket_chunks never calls run_pending_tasks")
    }

    fn close_spill(&self) {
        self.close_spill_calls.fetch_add(1, Ordering::Relaxed);
    }

    fn held_buckets(&self) -> BoxFuture<'_, Vec<u16>> {
        let held = self.held.clone();
        Box::pin(async move { held })
    }
}

/// `ShardOps`' part-granular defaults, on a stub that implements only
/// the bucket forms: held parts are every part of each held bucket,
/// nothing releases or reads cold or unverified, and a stub with no
/// view has no scoped digests.
#[tokio::test]
async fn shard_ops_part_defaults_follow_the_bucket_forms() {
    let stub = RecordingShard {
        entries: Vec::new(),
        held: vec![3, 900],
        records: HashMap::new(),
        records_for_calls: Mutex::new(Vec::new()),
        close_spill_calls: AtomicUsize::new(0),
    };
    let held = stub.held_parts().await;
    assert_eq!(held.len(), 2 * crate::store::PART_COUNT);
    assert!(held.iter().all(|part| [3, 900].contains(&part.bucket())));
    assert_eq!(stub.release_parts(&held).await, 0);
    assert!(!stub.is_cold_part(held[0]));
    assert!(!stub.is_unverified_part(held[0]));
    assert!(stub.ae_scoped_digests_for(NodeId::from(2)).await.is_none());
}

#[tokio::test]
async fn st_bucket_chunks_never_materializes_more_than_rebalance_chunk_bytes_for_a_bucket_larger_than_the_budget()
 {
    let bucket: u16 = 7;
    let key_count = 10u8;
    let keys: Vec<Bytes> = (0..key_count).map(|n| Bytes::from(vec![n])).collect();
    let ver = Hlc {
        wall_ms: 1,
        logical: 0,
        node: NodeId::from(1),
    };
    let entries: Vec<KeyVersion> = keys
        .iter()
        .map(|key| KeyVersion {
            key: key.clone(),
            version: ver,
        })
        .collect();
    let records: HashMap<Bytes, WireRecord> = keys
        .iter()
        .map(|key| {
            (
                key.clone(),
                WireRecord {
                    key: key.clone(),
                    value: Some(Bytes::from_static(b"v")),
                    ver,
                    expires_at_ms: None,
                },
            )
        })
        .collect();

    // A budget of three keys' worth forces the ten-key bucket into
    // more than one `records_for` call.
    let per_key_size = (wire::RECORD_HEADER_LEN + 1) as u64;
    let chunk_bytes = per_key_size * 3;

    let recording = Arc::new(RecordingShard {
        entries: vec![(bucket, entries)],
        held: Vec::new(),
        records,
        records_for_calls: Mutex::new(Vec::new()),
        close_spill_calls: AtomicUsize::new(0),
    });
    let shard: Arc<dyn ShardOps> = recording.clone();
    let mut registry = HashMap::new();
    registry.insert(SmolStr::new("cache"), shard);
    let handler = ClusterRequestHandler {
        shards: Arc::new(RwLock::new(registry)),
        warmth: Arc::new(Warmth::default()),
        ae_part_min_bucket: ClusterConfig::default().ae_part_min_bucket,
        ae_sketch_min_bucket: ClusterConfig::default().ae_sketch_min_bucket,
        ae_sketch_cells: ClusterConfig::default().ae_sketch_cells,
        rebalance_chunk_bytes: chunk_bytes,
    };

    let mut got_keys: Vec<Bytes> = Vec::new();
    let mut chunks = handler.st_bucket_chunks(SmolStr::new("cache"), vec![bucket]);
    while let Some((got_bucket, chunk)) = chunks.next().await {
        assert_eq!(
            got_bucket, bucket,
            "every yielded chunk is tagged with the bucket"
        );
        got_keys.extend(chunk.into_iter().map(|r| r.key));
    }
    got_keys.sort_unstable_by(|a, b| a.as_ref().cmp(b.as_ref()));
    let mut expected_keys = keys.clone();
    expected_keys.sort_unstable_by(|a, b| a.as_ref().cmp(b.as_ref()));
    assert_eq!(
        got_keys, expected_keys,
        "every key's record is served exactly once, regardless of sub-batching"
    );

    let calls = recording
        .records_for_calls
        .lock()
        .expect("invariant: fixture mutex is never poisoned");
    assert!(
        calls.len() > 1,
        "a bucket larger than the budget is fetched over more than one records_for call, not all at once"
    );
    for call in calls.iter() {
        let call_size: u64 = call
            .iter()
            .map(|k| (wire::RECORD_HEADER_LEN + k.len()) as u64)
            .sum();
        assert!(
            call_size <= chunk_bytes,
            "one records_for call never exceeds the rebalance_chunk_bytes estimate: {call_size} > {chunk_bytes}"
        );
    }
    let total_keys: usize = calls.iter().map(Vec::len).sum();
    assert_eq!(
        total_keys, key_count as usize,
        "every key is fetched exactly once across sub-batches"
    );
}

/// [`ShardOps::close_spill_checkpointed`]'s default, for an implementor
/// that never overrides it, calls the sync [`ShardOps::close_spill`]
/// once.
#[tokio::test]
async fn close_spill_checkpointed_default_resolves_and_calls_the_sync_close() {
    let recording = Arc::new(RecordingShard {
        entries: Vec::new(),
        held: Vec::new(),
        records: HashMap::new(),
        records_for_calls: Mutex::new(Vec::new()),
        close_spill_calls: AtomicUsize::new(0),
    });
    let shard: Arc<dyn ShardOps> = recording.clone();

    shard.close_spill_checkpointed().await;

    assert_eq!(
        recording.close_spill_calls.load(Ordering::Relaxed),
        1,
        "the default close_spill_checkpointed calls the sync close_spill exactly once"
    );
}

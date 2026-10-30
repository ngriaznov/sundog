//! A `Mode::Distributed` write a non-owner forwards and the owners refuse for clock skew.

use super::*;

/// How far the writer's clock runs ahead of the owners'. The window an owner
/// refuses a stamp in, `LEAD_MS - SKEW_MS`, is ten seconds, so a forward that
/// a loaded runner delivers seconds late still arrives inside it and is held
/// rather than applied at once.
const LEAD_MS: u64 = 11_000;

/// The owners' `max_clock_skew`: a stamp `LEAD_MS` ahead is `LEAD_MS - SKEW_MS`
/// past the bound.
const SKEW_MS: u64 = 1_000;

/// How long a test waits for the owners to apply a write they refused at
/// first: past the refusal window, with room for a late delivery.
const CONVERGE_BUDGET: Duration = Duration::from_secs(30);

/// Three `Mode::Distributed` nodes whose `max_clock_skew` is `SKEW_MS`, the
/// third with a clock `LEAD_MS` ahead of the other two.
struct SkewRig {
    nodes: [Cluster; 3],
    /// The first two nodes' caches, the owners of every key `writer` does not own.
    owners: [Cache<u32, String>; 2],
    /// The third node's cache, the writer with the fast clock.
    writer: Cache<u32, String>,
}

impl SkewRig {
    async fn open(name: &'static str) -> Self {
        let config =
            || loopback_config().with(|c| c.max_clock_skew = Some(Duration::from_millis(SKEW_MS)));
        let a = Cluster::builder(name)
            .seeds(std::iter::empty())
            .config(config())
            .build()
            .await
            .expect("a builds");
        let cache_a = a
            .cache::<u32, String>(name)
            .mode(Mode::distributed())
            .open()
            .await
            .expect("a opens");
        let b = Cluster::builder(name)
            .seeds([a.local_gossip_addr()])
            .config(config())
            .build()
            .await
            .expect("b builds");
        wait_for_peer_count(&b, 1).await;
        let cache_b = b
            .cache::<u32, String>(name)
            .mode(Mode::distributed())
            .open()
            .await
            .expect("b opens");
        let c = Cluster::builder(name)
            .seeds([a.local_gossip_addr()])
            .config(config())
            .build()
            .await
            .expect("c builds");
        wait_for_peer_count(&c, 2).await;
        let cache_c = c
            .cache::<u32, String>(name)
            .mode(Mode::distributed())
            .clock(Arc::new(|| now_ms() + LEAD_MS))
            .open()
            .await
            .expect("c opens");
        wait_for_peer_count(&a, 2).await;
        wait_for_peer_count(&b, 2).await;
        wait_until(
            Duration::from_secs(30),
            "every node's ownership view ranks all three nodes",
            async || {
                [&cache_a, &cache_b, &cache_c].iter().all(|cache| {
                    cache
                        .shard
                        .ownership_view()
                        .is_none_or(|view| view.eligible().len() == 3)
                })
            },
        )
        .await;
        Self {
            nodes: [a, b, c],
            owners: [cache_a, cache_b],
            writer: cache_c,
        }
    }

    /// `count` keys the writer does not own, each owned by the other two nodes.
    fn keys_the_writer_does_not_own(&self, count: usize) -> Vec<u32> {
        let writer = self.nodes[2].node_id();
        let keys: Vec<u32> = (0..10_000u32)
            .filter(|key| !self.writer.owners_of(key).contains(&writer))
            .take(count)
            .collect();
        assert_eq!(keys.len(), count, "enough keys the writer does not own");
        keys
    }

    async fn owners_read(&self, key: u32) -> [Option<String>; 2] {
        [
            self.owners[0].get(&key).await,
            self.owners[1].get(&key).await,
        ]
    }

    /// Waits until both owners read `expected` for `key`, a wait that may
    /// span the whole refusal window.
    async fn owners_converge_on(&self, key: u32, expected: Option<&str>) {
        wait_until(
            CONVERGE_BUDGET,
            "both owners read the forwarded write once their clocks are within the bound",
            async || {
                self.owners_read(key)
                    .await
                    .iter()
                    .all(|read| read.as_deref() == expected)
            },
        )
        .await;
    }

    async fn shutdown(self) {
        let Self {
            nodes: [a, b, c],
            owners: [cache_a, cache_b],
            writer,
        } = self;
        writer.close().await;
        cache_b.close().await;
        cache_a.close().await;
        c.shutdown().await;
        b.shutdown().await;
        a.shutdown().await;
    }
}

/// The writer keeps no copy of a forward, so an owner that refuses its stamp
/// for skew holds the record and applies it once its clock is within the
/// bound, about `LEAD_MS - SKEW_MS` after the write.
#[tokio::test]
async fn a_forwarded_write_refused_for_clock_skew_reaches_its_owners_once_their_clocks_come_within_the_bound()
 {
    let rig = SkewRig::open("cache-it-forward-skew-insert").await;
    let key = rig.keys_the_writer_does_not_own(1)[0];

    rig.writer
        .insert(key, "skewed".into())
        .await
        .expect("insert returns Ok");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        rig.owners_read(key).await,
        [None, None],
        "the owners refuse the stamp while it is past their bound"
    );
    rig.owners_converge_on(key, Some("skewed")).await;
    rig.shutdown().await;
}

/// A remove the owners refuse for skew is held and applied: the key stays
/// readable until the owners' clocks come within the bound, then goes, and
/// does not come back.
#[tokio::test]
async fn a_forwarded_remove_refused_for_clock_skew_still_deletes_on_the_owners() {
    let rig = SkewRig::open("cache-it-forward-skew-remove").await;
    let key = rig.keys_the_writer_does_not_own(1)[0];
    rig.owners[0]
        .insert(key, "seeded".into())
        .await
        .expect("an owner inserts");
    rig.owners_converge_on(key, Some("seeded")).await;

    rig.writer.remove(&key).await.expect("remove returns Ok");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        rig.owners_read(key).await,
        [Some("seeded".into()), Some("seeded".into())],
        "the delete waits for the owners' clocks"
    );
    rig.owners_converge_on(key, None).await;
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    assert_eq!(
        rig.owners_read(key).await,
        [None, None],
        "the deleted key stays deleted"
    );
    rig.shutdown().await;
}

/// The batch paths forward through `forward_prepared`; every key of an
/// `insert_many`, then of a `remove_many`, lands on both owners.
#[tokio::test]
async fn forwarded_batches_refused_for_clock_skew_reach_their_owners() {
    let rig = SkewRig::open("cache-it-forward-skew-batch").await;
    let keys = rig.keys_the_writer_does_not_own(20);

    rig.writer
        .insert_many(keys.iter().map(|&key| (key, format!("v{key}"))))
        .await
        .expect("insert_many returns Ok");
    for &key in &keys {
        rig.owners_converge_on(key, Some(&format!("v{key}"))).await;
    }

    // The batch's tombstones are stamped after the inserts, so they win.
    rig.writer
        .remove_many(keys.iter().copied())
        .await
        .expect("remove_many returns Ok");
    for &key in &keys {
        rig.owners_converge_on(key, None).await;
    }
    rig.shutdown().await;
}

/// A forward queued before this node owned its part, and drained after, is
/// applied here by the fan-out. Its stamp is the writer's, so this node's
/// clock can refuse it: it is held, like one that arrives in a frame.
#[tokio::test]
async fn a_forward_drained_by_its_new_owner_is_held_when_its_stamp_is_refused() {
    use crate::cluster::fan_out::fan_out_batch;
    use crate::ownership::{OwnershipTracker, ResidencySet};
    use crate::store::FanOutItem;

    let name = "cache-it-forward-skew-drain";
    let cluster = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("cluster builds");
    let (tracker, _tx) = OwnershipTracker::seed(
        cluster.node_id(),
        &[],
        &std::collections::HashMap::new(),
        &SmolStr::new(name),
        NonZeroU8::new(2).expect("nonzero"),
    );
    let shard = Shard::<u32, String>::new(
        SmolStr::new(name),
        Mode::distributed(),
        cluster.node_id(),
        u64::MAX,
        None,
        None,
    )
    .with_max_clock_skew(Some(Duration::from_millis(SKEW_MS)))
    .with_ownership(tracker, Arc::new(ResidencySet::new()));
    let mut rec = test_record(1, "drained", NodeId::from(77));
    rec.ver.wall_ms = now_ms() + 3_600_000;

    fan_out_batch(
        &shard,
        &cluster,
        &SmolStr::new(name),
        Mode::distributed(),
        vec![FanOutItem::Forward(rec)],
    )
    .await;

    assert_eq!(shard.get(&1).await, None, "the stamp is past the bound");
    assert_eq!(shard.skew_held().0, 1, "held, not dropped");
    cluster.shutdown().await;
}

/// An owner's own write reaches its co-owners as a `ForwardBatch` too. A
/// co-owner that refuses its stamp for skew does not hold it: the owner kept
/// the record, and anti-entropy offers it again until the clocks agree.
#[tokio::test]
async fn an_owners_write_refused_for_clock_skew_is_not_held_and_anti_entropy_delivers_it() {
    let rig = SkewRig::open("cache-it-forward-skew-owner-write").await;
    let writer = rig.nodes[2].node_id();
    let key = (0..10_000u32)
        .find(|key| rig.writer.owners_of(key).contains(&writer))
        .expect("some key is owned by the fast node");
    let co_owners: Vec<&Cache<u32, String>> = [0, 1]
        .into_iter()
        .filter(|&n| rig.writer.owners_of(&key).contains(&rig.nodes[n].node_id()))
        .map(|n| &rig.owners[n])
        .collect();
    assert!(!co_owners.is_empty(), "the fast node has a co-owner");

    rig.writer
        .insert(key, "owner-write".into())
        .await
        .expect("the fast owner inserts");
    tokio::time::sleep(Duration::from_millis(500)).await;
    for cache in &co_owners {
        assert_eq!(cache.get(&key).await, None, "refused for skew");
        assert_eq!(
            cache.shard.skew_held(),
            (0, 0),
            "the writer is an owner, so nothing is held for it"
        );
    }
    wait_until(
        CONVERGE_BUDGET,
        "anti-entropy delivers the owner's write once the clocks are within the bound",
        async || {
            let mut all = true;
            for cache in &co_owners {
                all &= cache.get(&key).await.as_deref() == Some("owner-write");
            }
            all
        },
    )
    .await;
    rig.shutdown().await;
}

impl SkewRig {
    /// Sends the writer's `ForwardBatch` of one skewed record for `key` to
    /// owner `to` (0 or 1), under `view_hash`, at `hops`.
    async fn send_skewed_forward(
        &self,
        to: usize,
        key: u32,
        value: &str,
        view_hash: u64,
        hops: u8,
    ) {
        use crate::net::OutFrame;
        use crate::wire::Msg;

        let writer = self.nodes[2].node_id();
        let mut rec = test_record(key, value, writer);
        rec.ver.wall_ms = now_ms() + LEAD_MS;
        let frame = OutFrame::new(Msg::ForwardBatch {
            cache: SmolStr::new(self.writer.shard.name()),
            view_hash,
            hops,
            recs: vec![rec],
        })
        .expect("encodes");
        self.nodes[2]
            .mesh()
            .clone()
            .send_frames_awaiting(self.nodes[to].node_id(), vec![frame])
            .await;
    }

    async fn wait_held(&self, owner: usize, held: usize) {
        wait_until(
            Duration::from_secs(10),
            "the owner holds the skewed forward",
            async || self.owners[owner].shard.skew_held().0 == held,
        )
        .await;
    }
}

/// Closing an owner hands what it holds to the other owner, which holds it
/// in turn and applies it when its own clock allows. The record reached only
/// the closing owner, so without the hand-off it is gone.
#[tokio::test]
async fn closing_an_owner_hands_its_held_forwards_to_the_other_owner() {
    let rig = SkewRig::open("cache-it-forward-skew-close").await;
    let key = rig.keys_the_writer_does_not_own(1)[0];
    let hash = rig.owners[0]
        .shard
        .ownership_view_hash()
        .expect("distributed");
    rig.send_skewed_forward(0, key, "handed", hash, 0).await;
    rig.wait_held(0, 1).await;
    assert_eq!(rig.owners[1].shard.skew_held(), (0, 0), "b never saw it");

    let SkewRig {
        nodes: [a, b, c],
        owners: [cache_a, cache_b],
        writer,
    } = rig;
    cache_a.close().await;
    wait_until(
        Duration::from_secs(10),
        "the other owner takes the hand-off",
        async || cache_b.shard.skew_held().0 == 1 || cache_b.get(&key).await.is_some(),
    )
    .await;
    wait_until(
        CONVERGE_BUDGET,
        "the other owner applies it once its clock is within the bound",
        async || cache_b.get(&key).await.as_deref() == Some("handed"),
    )
    .await;
    writer.close().await;
    cache_b.close().await;
    c.shutdown().await;
    b.shutdown().await;
    a.shutdown().await;
}

/// A skewed forward routed under a stale view reaches the owner its writer
/// missed through the first receiver's re-forward, and that last hop holds
/// it too: the stamp's writer is still the non-owner after the relay.
#[tokio::test]
async fn a_skewed_forward_re_forwarded_under_a_stale_view_is_held_at_the_final_owner() {
    let rig = SkewRig::open("cache-it-forward-skew-relay").await;
    let key = rig.keys_the_writer_does_not_own(1)[0];
    let stale = rig.owners[1]
        .shard
        .ownership_view_hash()
        .expect("distributed")
        ^ 1;
    rig.send_skewed_forward(1, key, "relayed", stale, 0).await;

    rig.wait_held(1, 1).await;
    rig.wait_held(0, 1).await;
    rig.owners_converge_on(key, Some("relayed")).await;
    rig.shutdown().await;
}

/// The release task applies a held record when it comes due and ends when
/// the cache's token is cancelled, with a record still held.
#[tokio::test]
async fn the_release_task_applies_a_record_when_it_comes_due_and_ends_on_cancel() {
    use crate::cache::tasks::skew_hold_task;
    use crate::ownership::{OwnershipTracker, ResidencySet};

    let name = "cache-it-forward-skew-task";
    let (tracker, _tx) = OwnershipTracker::seed(
        NodeId::from(1),
        &[],
        &std::collections::HashMap::new(),
        &SmolStr::new(name),
        NonZeroU8::new(2).expect("nonzero"),
    );
    let shard = Arc::new(
        Shard::<u32, String>::new(
            SmolStr::new(name),
            Mode::distributed(),
            NodeId::from(1),
            u64::MAX,
            None,
            None,
        )
        .with_max_clock_skew(Some(Duration::from_millis(SKEW_MS)))
        .with_ownership(tracker, Arc::new(ResidencySet::new())),
    );
    assert!(shard.holds_skewed_forwards());
    let cancel = CancellationToken::new();
    let task = tokio::spawn(skew_hold_task(Arc::clone(&shard), cancel.clone()));

    // Due once the refusal window passes; a second record due in an hour
    // stays held.
    let mut soon = test_record(1, "soon", NodeId::from(77));
    soon.ver.wall_ms = now_ms() + LEAD_MS;
    let mut later = test_record(2, "later", NodeId::from(77));
    later.ver.wall_ms = now_ms() + SKEW_MS + 3_600_000;
    ShardOps::apply_forwarded_batch(shard.as_ref(), vec![soon, later]).await;
    assert_eq!(shard.get(&1).await, None);

    wait_until(
        CONVERGE_BUDGET,
        "the task applies the record once its clock is within the bound",
        async || shard.get(&1).await.as_deref() == Some("soon"),
    )
    .await;
    assert_eq!(
        shard.skew_held().0,
        1,
        "the record due in an hour stays held"
    );

    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("the task ends on cancel with a record held")
        .expect("the task does not panic");
}

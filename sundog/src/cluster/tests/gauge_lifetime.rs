//! The gauges of a cache or a cluster read 0 once it is gone: the task that
//! publishes a gauge sets it to 0 as its last act.

use std::net::SocketAddr;

use super::*;
use crate::store::test_support::Gauges;

/// Waits until the gauge reads `want`, panicking if it does not within five
/// seconds.
async fn settle(snapshotter: &Gauges, name: &str, cache: Option<&str>, want: f64) {
    wait_until(
        Duration::from_secs(5),
        &format!("{name} reads {want}"),
        async || snapshotter.get(name, cache) == Some(want),
    )
    .await;
}

fn bare_shard(name: &str) -> Arc<Shard<u32, String>> {
    Arc::new(
        Shard::<u32, String>::new(
            name.into(),
            Mode::Replicated,
            NodeId::from(1),
            100,
            None,
            None,
        )
        .with_max_resident_bytes(1 << 30),
    )
}

#[tokio::test]
async fn a_closed_cache_reports_no_entries_tombstones_or_bytes() {
    let recorder = Gauges::default();
    let snapshotter = recorder.clone();
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let shard = bare_shard("gauge-close");
    shard.insert(1, "a".into()).await.expect("insert");
    shard.insert(2, "b".into()).await.expect("insert");
    shard.remove(&2).await.expect("remove");

    let cancel = CancellationToken::new();
    let task = tokio::spawn(cache_entries_gauge_task(
        Arc::clone(&shard),
        "gauge-close".into(),
        cancel.clone(),
    ));
    let cache = Some("gauge-close");
    settle(&snapshotter, "sundog_cache_entries", cache, 1.0).await;
    settle(&snapshotter, "sundog_tombstones", cache, 1.0).await;
    assert_eq!(snapshotter.get("sundog_cache_entries", cache), Some(1.0));
    assert_eq!(snapshotter.get("sundog_tombstones", cache), Some(1.0));
    assert!(
        snapshotter
            .get("sundog_cache_bytes", cache)
            .is_some_and(|bytes| bytes > 0.0)
    );

    cancel.cancel();
    task.await.expect("the task ends");
    for name in [
        "sundog_cache_entries",
        "sundog_tombstones",
        "sundog_cache_bytes",
    ] {
        assert_eq!(snapshotter.get(name, cache), Some(0.0), "{name}");
    }
}

#[tokio::test]
async fn an_uncapped_cache_publishes_no_byte_gauge_even_when_it_closes() {
    let recorder = Gauges::default();
    let snapshotter = recorder.clone();
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let shard = Arc::new(Shard::<u32, String>::new(
        "gauge-uncapped".into(),
        Mode::Replicated,
        NodeId::from(1),
        100,
        None,
        None,
    ));
    let cancel = CancellationToken::new();
    let task = tokio::spawn(cache_entries_gauge_task(
        shard,
        "gauge-uncapped".into(),
        cancel.clone(),
    ));
    settle(
        &snapshotter,
        "sundog_cache_entries",
        Some("gauge-uncapped"),
        0.0,
    )
    .await;
    cancel.cancel();
    task.await.expect("the task ends");
    assert_eq!(
        snapshotter.get("sundog_cache_entries", Some("gauge-uncapped")),
        Some(0.0)
    );
    assert_eq!(
        snapshotter.get("sundog_cache_bytes", Some("gauge-uncapped")),
        None
    );
}

#[tokio::test]
async fn a_cache_reopened_under_the_name_reports_its_own_entries() {
    let recorder = Gauges::default();
    let snapshotter = recorder.clone();
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let cache = Some("gauge-reopen");
    let run = |shard: Arc<Shard<u32, String>>| {
        let cancel = CancellationToken::new();
        let task = tokio::spawn(cache_entries_gauge_task(
            shard,
            "gauge-reopen".into(),
            cancel.clone(),
        ));
        (cancel, task)
    };

    let first = bare_shard("gauge-reopen");
    first.insert(1, "a".into()).await.expect("insert");
    let (cancel, task) = run(first);
    settle(&snapshotter, "sundog_cache_entries", cache, 1.0).await;
    cancel.cancel();
    task.await.expect("the first task ends");
    assert_eq!(snapshotter.get("sundog_cache_entries", cache), Some(0.0));

    let second = bare_shard("gauge-reopen");
    for key in 0..3 {
        second.insert(key, "v".into()).await.expect("insert");
    }
    let (cancel, task) = run(second);
    settle(&snapshotter, "sundog_cache_entries", cache, 3.0).await;
    cancel.cancel();
    task.await.expect("the second task ends");
}

#[tokio::test]
async fn a_cluster_with_open_caches_reports_none_once_its_gauge_task_ends() {
    let recorder = Gauges::default();
    let snapshotter = recorder.clone();
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let shards: ShardRegistry = Arc::new(RwLock::new(HashMap::new()));
    shards.write_shards().insert(
        "gauge-open".into(),
        bare_shard("gauge-open") as Arc<dyn ShardOps>,
    );
    let cancel = CancellationToken::new();
    let task = tokio::spawn(open_cache_gauge_task(shards, cancel.clone()));
    settle(&snapshotter, "sundog_open_caches", None, 1.0).await;

    cancel.cancel();
    task.await.expect("the task ends");
    assert_eq!(snapshotter.get("sundog_open_caches", None), Some(0.0));
}

#[tokio::test]
async fn a_distributed_cache_reports_no_owned_parts_once_its_refresh_task_ends() {
    let recorder = Gauges::default();
    let snapshotter = recorder.clone();
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let cluster = solo_cluster("cluster-it-owned-gauge").await;
    let name = SmolStr::new("gauge-owned");
    let k = std::num::NonZeroU8::new(1).expect("nonzero");
    let cache = Some("gauge-owned");

    let start = |name: &SmolStr| {
        let (_tracker, tx) = crate::ownership::OwnershipTracker::seed(
            cluster.node_id(),
            &cluster.peers(),
            &cluster.advertised_cache_modes(),
            name,
            k,
        );
        let cancel = CancellationToken::new();
        let task = tokio::spawn(crate::ownership::refresh_task(
            cluster.clone(),
            name.clone(),
            k,
            tx,
            Arc::new(crate::ownership::ResidencySet::new()),
            cancel.clone(),
        ));
        (cancel, task)
    };
    let (cancel, task) = start(&name);
    let owned = snapshotter
        .get("sundog_owned_parts", cache)
        .expect("seed publishes");
    assert!(owned > 0.0, "a sole node owns every part");
    cancel.cancel();
    task.await.expect("the task ends");
    assert_eq!(snapshotter.get("sundog_owned_parts", cache), Some(0.0));
    assert_eq!(snapshotter.get("sundog_owned_buckets", cache), Some(0.0));

    // A reopen under the same name publishes its own view again.
    let (cancel, task) = start(&name);
    assert_eq!(snapshotter.get("sundog_owned_parts", cache), Some(owned));
    cancel.cancel();
    task.await.expect("the task ends");
    cluster.shutdown().await;
}

/// A node on a runtime of its own thread, whose gauge writes never reach
/// this test's thread-local recorder. Stops when the returned sender is
/// dropped or sent to.
fn spawn_peer_on_another_thread(
    name: &'static str,
    seed: SocketAddr,
) -> (
    std::thread::JoinHandle<()>,
    tokio::sync::oneshot::Sender<()>,
) {
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let thread = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime builds");
        runtime.block_on(async move {
            let peer = Cluster::builder(name)
                .seeds([seed])
                .config(loopback_config())
                .build()
                .await
                .expect("peer builds");
            let _ = stop_rx.await;
            peer.shutdown().await;
        });
    });
    (thread, stop_tx)
}

#[tokio::test]
async fn a_node_that_shuts_down_reports_no_live_peers() {
    let recorder = Gauges::default();
    let snapshotter = recorder.clone();
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let name = "cluster-it-live-peers-gauge";
    let a = solo_cluster(name).await;
    let (thread, stop) = spawn_peer_on_another_thread(name, a.local_gossip_addr());
    wait_until(
        Duration::from_secs(15),
        "the live peers gauge reads 1",
        async || snapshotter.get("sundog_live_peers", None) == Some(1.0),
    )
    .await;

    a.shutdown().await;
    assert_eq!(snapshotter.get("sundog_live_peers", None), Some(0.0));
    let _ = stop.send(());
    tokio::task::spawn_blocking(move || thread.join().expect("the peer thread ends"))
        .await
        .expect("join runs");
}

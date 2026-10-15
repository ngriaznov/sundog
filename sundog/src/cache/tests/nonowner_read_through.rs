//! A `Mode::Distributed` node that does not own a key reads through the key's owners before it runs the caller's loader, and still keeps no copy.
//!
//! Every test runs on three nodes of one cache: the first never owns the key,
//! the other two own it. The nodes tolerate a runtime stalled for seconds
//! (`fetch_timeout` 5 s, `phi_threshold` 1000, as `three_loading_nodes`
//! does), and each test starts once every node's view ranks all three nodes
//! and agrees on the key's owners and both owners hold its part warm and
//! verified, so an owner answers a fetch and never declines.

use std::sync::atomic::{AtomicUsize, Ordering};

use super::observability::{read_series, series_count};
use super::*;

/// Three settled `Mode::distributed()` nodes of one cache and a key the
/// first never owns.
pub(super) struct Trio {
    pub(super) name: &'static str,
    pub(super) nodes: Vec<(Cluster, Cache<u32, String>)>,
    pub(super) key: u32,
}

impl Trio {
    /// The first node's cache: it does not own `key`.
    pub(super) fn non_owner(&self) -> &Cache<u32, String> {
        &self.nodes[0].1
    }

    /// The caches of the two owners of `key`.
    pub(super) fn owners(&self) -> Vec<&Cache<u32, String>> {
        self.non_owner()
            .owners_of(&self.key)
            .into_iter()
            .map(|owner| cache_of(&self.nodes, owner))
            .collect()
    }

    /// Writes `value` for `key` on an owner and waits until both owners store it.
    pub(super) async fn hold_at_the_owners(&self, value: &str) {
        let owners = self.owners();
        owners[0]
            .insert(self.key, value.to_string())
            .await
            .expect("an owner writes");
        wait_for_local_record(&owners, self.key, LocalRecord::is_live).await;
    }

    /// The `sundog_fetch_total` fetches counted since the last read of `series`.
    pub(super) fn fetches(&self, series: &super::observability::Series) -> u64 {
        ["local", "remote", "miss", "error"]
            .into_iter()
            .map(|outcome| series_count(series, "sundog_fetch_total", self.name, outcome))
            .sum()
    }

    pub(super) async fn shut_down(self) {
        shut_down_all(self.nodes).await;
    }
}

/// A settled [`Trio`] of `name`.
async fn settled_trio(name: &'static str) -> Trio {
    settled_trio_with(name, loopback_config()).await
}

/// A settled [`Trio`] of `name` whose nodes run `config`, with the fetch
/// timeout and failure detector the settled nodes need.
pub(super) async fn settled_trio_with(name: &'static str, config: ClusterConfig) -> Trio {
    let config = config.with(|c| {
        c.fetch_timeout = Duration::from_secs(5);
        c.phi_threshold = 1_000.0;
    });
    let (a, b, c) = three_node_distributed_with(name, config).await;
    let nodes = vec![a, b, c];
    wait_for_full_views(&nodes).await;
    let first = &nodes[0];
    let key = tokio::time::timeout(CONVERGE, async {
        loop {
            if let Some(key) =
                (0..10_000u32).find(|key| !first.1.owners_of(key).contains(&first.0.node_id()))
            {
                return key;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the first node's view has a key it does not own");
    let owners = first.1.owners_of(&key);
    wait_until(
        CONVERGE,
        "every node agrees on the key's owners",
        async || {
            nodes
                .iter()
                .all(|(_, cache)| cache.owners_of(&key) == owners)
        },
    )
    .await;
    wait_for_warm_owners(&nodes, key).await;
    Trio { name, nodes, key }
}

/// Installs a recorder for the test's thread; the guard keeps it.
fn debugging_recorder() -> (
    metrics_util::debugging::DebuggingRecorder,
    metrics_util::debugging::Snapshotter,
) {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    (recorder, snapshotter)
}

#[tokio::test]
async fn get_or_load_on_a_non_owner_answers_with_an_owners_value_and_runs_no_loader() {
    let (recorder, snapshotter) = debugging_recorder();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let trio = settled_trio("nonowner-rt-owner-holds").await;
    let (key, cache_a) = (trio.key, trio.non_owner());
    trio.hold_at_the_owners("owned").await;
    let loads = AtomicUsize::new(0);
    let _ = read_series(&snapshotter);

    let got = cache_a
        .get_or_load(&key, async |_: &u32| {
            loads.fetch_add(1, Ordering::SeqCst);
            Ok::<_, SourceDown>("loaded".to_string())
        })
        .await
        .expect("get_or_load");

    assert_eq!(got, "owned", "the owner's value answers");
    assert_eq!(loads.load(Ordering::SeqCst), 0, "no loader runs");
    let series = read_series(&snapshotter);
    assert_eq!(
        series_count(&series, "sundog_fetch_total", trio.name, "remote"),
        1
    );
    assert_eq!(trio.fetches(&series), 1, "one owner fetch: {series:?}");
    assert_eq!(
        series_count(
            &series,
            "sundog_fetch_duration_seconds",
            trio.name,
            "remote"
        ),
        1
    );
    assert_eq!(
        series_count(&series, "sundog_cache_misses_total", trio.name, ""),
        1,
        "the local miss counts once"
    );
    assert_eq!(
        series_count(&series, "sundog_cache_hits_total", trio.name, ""),
        0
    );
    assert_eq!(cache_a.get(&key).await, None, "the non-owner keeps no copy");
    assert!(cache_a.keys().is_empty(), "no key is held");
    trio.shut_down().await;
}

#[tokio::test]
async fn get_or_load_on_a_non_owner_loads_on_every_call_for_a_key_no_owner_holds() {
    let (recorder, snapshotter) = debugging_recorder();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let trio = settled_trio("nonowner-rt-owners-miss").await;
    let (key, cache_a) = (trio.key, trio.non_owner());
    let loads = AtomicUsize::new(0);
    let _ = read_series(&snapshotter);

    for call in 1..=2usize {
        let got = cache_a
            .get_or_load(&key, async |_: &u32| {
                loads.fetch_add(1, Ordering::SeqCst);
                Ok::<_, SourceDown>("loaded".to_string())
            })
            .await
            .expect("get_or_load");
        assert_eq!(got, "loaded");
        assert_eq!(loads.load(Ordering::SeqCst), call, "each call loads");
    }

    let series = read_series(&snapshotter);
    assert_eq!(
        series_count(&series, "sundog_fetch_total", trio.name, "miss"),
        2
    );
    assert_eq!(trio.fetches(&series), 2, "each call asks the owners once");
    assert_eq!(
        series_count(&series, "sundog_cache_misses_total", trio.name, ""),
        2,
        "each fill counts a miss"
    );
    assert_eq!(cache_a.get(&key).await, None, "the non-owner keeps no copy");
    for owner in trio.owners() {
        assert_eq!(owner.get(&key).await, None, "the loaded value goes nowhere");
    }
    trio.shut_down().await;
}

/// `calls` concurrent `get_or_load`s of the trio's key on the non-owner,
/// each counting its loader in `loads`, and their answers.
async fn concurrent_get_or_loads(trio: &Trio, calls: usize, loads: &AtomicUsize) -> Vec<String> {
    let cache_a = trio.non_owner();
    futures::future::join_all((0..calls).map(|_| {
        cache_a.get_or_load(&trio.key, async |_: &u32| {
            loads.fetch_add(1, Ordering::SeqCst);
            Ok::<_, SourceDown>("loaded".to_string())
        })
    }))
    .await
    .into_iter()
    .map(|got| got.expect("get_or_load"))
    .collect()
}

#[tokio::test]
async fn concurrent_get_or_loads_on_a_non_owner_make_one_owner_fetch_and_no_loader_call() {
    let (recorder, snapshotter) = debugging_recorder();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let trio = settled_trio("nonowner-rt-concurrent-held").await;
    trio.hold_at_the_owners("owned").await;
    let loads = AtomicUsize::new(0);
    let _ = read_series(&snapshotter);

    let answers = concurrent_get_or_loads(&trio, 8, &loads).await;

    assert_eq!(answers, vec!["owned".to_string(); 8]);
    assert_eq!(loads.load(Ordering::SeqCst), 0);
    let series = read_series(&snapshotter);
    assert_eq!(
        series_count(&series, "sundog_fetch_total", trio.name, "remote"),
        1
    );
    assert_eq!(
        trio.fetches(&series),
        1,
        "one fetch for eight calls: {series:?}"
    );
    assert_eq!(
        series_count(&series, "sundog_cache_misses_total", trio.name, ""),
        1,
        "the fill owner counts the one miss"
    );
    assert_eq!(
        series_count(&series, "sundog_cache_hits_total", trio.name, ""),
        7,
        "every joined call counts a hit"
    );
    assert_eq!(trio.non_owner().get(&trio.key).await, None);
    trio.shut_down().await;
}

#[tokio::test]
async fn concurrent_get_or_loads_on_a_non_owner_make_one_owner_fetch_and_one_loader_call() {
    let (recorder, snapshotter) = debugging_recorder();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let trio = settled_trio("nonowner-rt-concurrent-miss").await;
    let loads = AtomicUsize::new(0);
    let _ = read_series(&snapshotter);

    let answers = concurrent_get_or_loads(&trio, 8, &loads).await;

    assert_eq!(answers, vec!["loaded".to_string(); 8]);
    assert_eq!(loads.load(Ordering::SeqCst), 1, "one loader call");
    let series = read_series(&snapshotter);
    assert_eq!(
        series_count(&series, "sundog_fetch_total", trio.name, "miss"),
        1
    );
    assert_eq!(
        trio.fetches(&series),
        1,
        "one fetch for eight calls: {series:?}"
    );
    assert_eq!(
        series_count(&series, "sundog_cache_misses_total", trio.name, ""),
        1
    );
    assert_eq!(
        series_count(&series, "sundog_cache_hits_total", trio.name, ""),
        7
    );
    assert_eq!(trio.non_owner().get(&trio.key).await, None);
    trio.shut_down().await;
}

#[tokio::test]
async fn get_or_load_on_a_non_owner_loads_when_every_owner_is_down() {
    let (recorder, snapshotter) = debugging_recorder();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let Trio {
        name,
        mut nodes,
        key,
    } = settled_trio("nonowner-rt-owners-down").await;
    // `a`'s failure detector never drops the owners, so its view still
    // names them as the key's and every dial fails outright.
    let (c, _) = nodes.pop().expect("third node");
    let (b, _) = nodes.pop().expect("second node");
    b.crash().await;
    c.crash().await;
    let cache_a = &nodes[0].1;
    assert!(
        !cache_a.owners_of(&key).contains(&nodes[0].0.node_id()),
        "the view still names the crashed owners"
    );
    let loads = AtomicUsize::new(0);
    let _ = read_series(&snapshotter);

    let got = cache_a
        .get_or_load(&key, async |_: &u32| {
            loads.fetch_add(1, Ordering::SeqCst);
            Ok::<_, SourceDown>("loaded".to_string())
        })
        .await
        .expect("get_or_load answers through the loader");

    assert_eq!(got, "loaded");
    assert_eq!(loads.load(Ordering::SeqCst), 1);
    assert_eq!(cache_a.get(&key).await, None, "the value is stored nowhere");
    let series = read_series(&snapshotter);
    assert_eq!(
        series_count(&series, "sundog_fetch_total", name, "error"),
        1
    );
    shut_down_all(nodes).await;
}

#[tokio::test]
async fn get_or_load_on_a_non_owner_fails_with_the_loader_error_when_the_owners_miss() {
    let trio = settled_trio("nonowner-rt-loader-error").await;
    let (key, cache_a) = (trio.key, trio.non_owner());

    let err = cache_a
        .get_or_load(&key, async |_: &u32| Err::<String, _>(SourceDown))
        .await
        .expect_err("the loader fails");

    assert!(
        matches!(err, CacheError::Loader(_)),
        "a loader failure after the owners missed is the loader's: {err:?}"
    );
    trio.shut_down().await;
}

#[tokio::test]
async fn get_or_insert_with_on_a_non_owner_answers_with_an_owners_value_and_runs_no_maker() {
    let trio = settled_trio("nonowner-rt-insert-held").await;
    let (key, cache_a) = (trio.key, trio.non_owner());
    trio.hold_at_the_owners("owned").await;
    let makes = AtomicUsize::new(0);

    let got = cache_a
        .get_or_insert_with(&key, async |_: &u32| {
            makes.fetch_add(1, Ordering::SeqCst);
            "made".to_string()
        })
        .await
        .expect("get_or_insert_with");

    assert_eq!(got, "owned");
    assert_eq!(makes.load(Ordering::SeqCst), 0);
    assert_eq!(cache_a.get(&key).await, None, "the non-owner keeps no copy");
    trio.shut_down().await;
}

#[tokio::test]
async fn get_or_insert_with_on_a_non_owner_makes_the_value_when_the_owners_miss() {
    let trio = settled_trio("nonowner-rt-insert-miss").await;
    let (key, cache_a) = (trio.key, trio.non_owner());
    let makes = AtomicUsize::new(0);

    for call in 1..=2usize {
        let got = cache_a
            .get_or_insert_with(&key, async |_: &u32| {
                makes.fetch_add(1, Ordering::SeqCst);
                "made".to_string()
            })
            .await
            .expect("get_or_insert_with");
        assert_eq!(got, "made");
        assert_eq!(makes.load(Ordering::SeqCst), call, "each call makes");
    }

    assert_eq!(cache_a.get(&key).await, None, "the non-owner keeps no copy");
    for owner in trio.owners() {
        assert_eq!(owner.get(&key).await, None, "the owners still lack the key");
    }
    trio.shut_down().await;
}

/// A node whose view moved a part to a peer it cannot reach keeps the
/// record it held, and a read-through answers from it before any owner is asked.
#[tokio::test]
async fn a_non_owner_that_holds_a_copy_answers_from_it_without_asking_the_owners() {
    let (recorder, snapshotter) = debugging_recorder();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let name = "nonowner-rt-local-copy";
    let a = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node builds");
    let me = a.node_id();
    let phantom = NodeId::from(u64::MAX);
    let owned = Arc::new(OwnershipView::compute(me, vec![me], NonZeroU8::MIN));
    let shared = Arc::new(OwnershipView::compute(me, vec![phantom], NonZeroU8::MIN));
    let (cache, _residency, view_tx) = cache_publishing_views(&a, name, &owned);
    let (held, absent) = {
        let mut keys = (0..1_000_000u32).filter(|key| !shared.owns(part_of(*key)));
        (
            keys.next().expect("the phantom owns some parts"),
            keys.next().expect("the phantom owns many parts"),
        )
    };
    // The record lands while this node owns its part, then the part moves.
    ShardOps::apply_remote_batch(cache.shard.as_ref(), vec![test_record(held, "kept", me)]).await;
    view_tx
        .send(Arc::clone(&shared))
        .expect("the tracker holds a receiver");
    let loads = AtomicUsize::new(0);
    let _ = read_series(&snapshotter);

    let got = cache
        .get_or_load(&held, async |_: &u32| {
            loads.fetch_add(1, Ordering::SeqCst);
            Ok::<_, SourceDown>("loaded".to_string())
        })
        .await
        .expect("get_or_load");

    assert_eq!(got, "kept", "the local copy answers");
    assert_eq!(loads.load(Ordering::SeqCst), 0);
    let series = read_series(&snapshotter);
    for outcome in ["local", "remote", "miss", "error"] {
        assert_eq!(
            series_count(&series, "sundog_fetch_total", name, outcome),
            0,
            "no owner is asked: {series:?}"
        );
    }

    // A key it holds no copy of asks the owners; the phantom answers
    // nothing, so the loader runs.
    let got = cache
        .get_or_load(&absent, async |_: &u32| {
            loads.fetch_add(1, Ordering::SeqCst);
            Ok::<_, SourceDown>("loaded".to_string())
        })
        .await
        .expect("get_or_load");
    assert_eq!(got, "loaded");
    assert_eq!(loads.load(Ordering::SeqCst), 1);
    let series = read_series(&snapshotter);
    assert_eq!(
        series_count(&series, "sundog_fetch_total", name, "error"),
        1,
        "the owners are asked and unreachable: {series:?}"
    );
    assert_eq!(
        cache.get(&absent).await,
        None,
        "the loaded value is stored nowhere"
    );
    a.shutdown().await;
}

#[tokio::test]
async fn get_or_load_on_an_owner_loads_stores_and_fans_out_without_asking_the_owners() {
    let (recorder, snapshotter) = debugging_recorder();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let trio = settled_trio("nonowner-rt-owner-loads").await;
    let key = trio.key;
    let owners = trio.owners();
    let loads = AtomicUsize::new(0);
    let _ = read_series(&snapshotter);

    for _ in 0..2 {
        let got = owners[0]
            .get_or_load(&key, async |_: &u32| {
                loads.fetch_add(1, Ordering::SeqCst);
                Ok::<_, SourceDown>("loaded".to_string())
            })
            .await
            .expect("get_or_load");
        assert_eq!(got, "loaded");
    }

    assert_eq!(loads.load(Ordering::SeqCst), 1, "the second call hits");
    assert_eq!(owners[0].get(&key).await.as_deref(), Some("loaded"));
    wait_for_local_record(&owners, key, LocalRecord::is_live).await;
    assert_eq!(
        trio.non_owner().get(&key).await,
        None,
        "the fill goes to the owners alone"
    );
    let series = read_series(&snapshotter);
    assert_eq!(trio.fetches(&series), 0, "an owner asks nobody: {series:?}");
    trio.shut_down().await;
}

#[test]
fn owners_answer_runs_the_loader_on_a_miss_and_on_no_answer_and_passes_any_other_error_on() {
    use crate::cache::fetch::owners_answer;

    assert_eq!(
        owners_answer(Ok(Some("owned".to_string()))).expect("a value answers"),
        Some("owned".to_string())
    );
    assert_eq!(
        owners_answer::<String>(Ok(None)).expect("a miss leaves the loader to run"),
        None
    );
    assert_eq!(
        owners_answer::<String>(Err(CacheError::FetchUnavailable {
            cache: SmolStr::new("prices")
        }))
        .expect("owners that cannot answer leave the loader to run"),
        None
    );
    assert!(matches!(
        owners_answer::<String>(Err(CacheError::NoLoader {
            cache: SmolStr::new("prices")
        })),
        Err(CacheError::NoLoader { .. })
    ));
}

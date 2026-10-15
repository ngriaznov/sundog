//! A `Mode::Distributed` read-through fill on a node that does not own the key answers the caller and is stored nowhere.

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize};

use tokio::sync::Notify;

use super::*;
use crate::net::LoadServe;
use crate::store::Loader;
use crate::store::fan_out::{FillRoute, ReadThroughRoute, fill_route, read_through_route};
use crate::store::refresh::RefreshRequest;

/// What publishes a further view to a test shard.
type ViewSender = watch::Sender<Arc<OwnershipView>>;

/// A five-node `Mode::Distributed` shard of node 1 with two owners per part,
/// a loader that holds `v{key}` for every key and counts its calls in the
/// returned counter, the view it decides under, and the sender to publish a
/// further view through.
fn loading_shard() -> (
    Shard<u32, String>,
    Arc<OwnershipView>,
    ViewSender,
    Arc<AtomicUsize>,
) {
    let eligible = (1..=5u64).map(NodeId::from).collect::<Vec<_>>();
    let (s, view, tx) = distributed_shard::<u32, String>(
        NodeId::from(1),
        eligible,
        2,
        Arc::new(ResidencySet::new()),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&calls);
    let s = s.with_loader(Loader::batch(move |keys: Vec<u32>| {
        counted.fetch_add(1, Ordering::SeqCst);
        async move {
            Ok::<_, Infallible>(
                keys.into_iter()
                    .map(|key| (key, format!("v{key}")))
                    .collect::<HashMap<_, _>>(),
            )
        }
    }));
    (s, view, tx, calls)
}

/// A view over forty nodes, in which node 1 owns far fewer parts than in
/// [`loading_shard`]'s five-node view.
pub(super) fn crowded_view() -> Arc<OwnershipView> {
    Arc::new(OwnershipView::compute(
        NodeId::from(1),
        (1..=40u64).map(NodeId::from).collect(),
        NonZeroU8::new(2).expect("nonzero"),
    ))
}

/// A key `before` owns and `after` does not.
pub(super) fn key_moved_away(before: &OwnershipView, after: &OwnershipView) -> u32 {
    (0..1_000_000u32)
        .find(|key| {
            let part = PartId::of_key(&key_bytes(key));
            before.owns(part) && !after.owns(part)
        })
        .expect("a key moves away from this node in a crowded cluster")
}

fn filled(value: &str) -> impl AsyncFnOnce(&u32) -> Result<String, Infallible> {
    let value = value.to_string();
    async move |_key: &u32| Ok(value)
}

#[tokio::test]
async fn get_or_load_on_a_non_owner_answers_the_caller_and_stores_nothing() {
    let (s, view, _tx, _calls) = loading_shard();
    let key = find_key_by_ownership(&view, false);
    let mut events = s.events();

    let loaded = s
        .get_or_load(&key, filled("filled"))
        .await
        .expect("the loader succeeds");

    assert_eq!(loaded, "filled", "the caller gets the loaded value");
    assert_eq!(s.get(&key).await, None, "a non-owner keeps no copy");
    assert!(!s.contains_key_sync(&key));
    assert!(s.keys().is_empty(), "no key is held");
    assert_eq!(s.entry_count().await, 0);
    assert!(
        s.fan_out_queue().drain().is_empty(),
        "nothing is sent to the owners"
    );
    assert!(
        matches!(
            events.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ),
        "a fill that stores nothing emits no event"
    );
}

#[tokio::test]
async fn get_or_load_on_an_owner_stores_the_fill_and_fans_it_out() {
    let (s, view, _tx, _calls) = loading_shard();
    let key = find_key_by_ownership(&view, true);

    let loaded = s
        .get_or_load(&key, filled("filled"))
        .await
        .expect("the loader succeeds");

    assert_eq!(loaded, "filled");
    assert_eq!(s.get(&key).await, Some("filled".to_string()));
    assert_eq!(applied_keys(s.fan_out_queue().drain()), vec![key]);
}

#[tokio::test]
async fn every_get_or_load_on_a_non_owner_runs_the_loader() {
    let (s, view, _tx, _calls) = loading_shard();
    let key = find_key_by_ownership(&view, false);
    let ran = AtomicUsize::new(0);
    for _ in 0..3 {
        s.get_or_load(&key, async |_: &u32| {
            ran.fetch_add(1, Ordering::SeqCst);
            Ok::<_, Infallible>("filled".to_string())
        })
        .await
        .expect("the loader succeeds");
    }
    assert_eq!(
        ran.load(Ordering::SeqCst),
        3,
        "a non-owner has no copy to hit"
    );
}

#[tokio::test]
async fn get_or_insert_with_on_a_non_owner_answers_and_stores_nothing() {
    let (s, view, _tx, _calls) = loading_shard();
    let key = find_key_by_ownership(&view, false);
    let made = s
        .get_or_insert_with(&key, async |_: &u32| "made".to_string())
        .await
        .expect("encodes");
    assert_eq!(made, "made");
    assert_eq!(s.get(&key).await, None);
    assert!(s.fan_out_queue().drain().is_empty(), "nothing is queued");
}

#[tokio::test]
async fn a_stampede_on_a_non_owner_calls_the_loader_once_and_every_waiter_gets_the_value() {
    let (s, view, _tx, _calls) = loading_shard();
    let key = find_key_by_ownership(&view, false);
    let ran = AtomicUsize::new(0);
    let release = Notify::new();
    let first = s.get_or_load(&key, async |_: &u32| {
        ran.fetch_add(1, Ordering::SeqCst);
        release.notified().await;
        Ok::<_, Infallible>("filled".to_string())
    });
    let joiners = async {
        // Joins the load in flight rather than starting its own.
        let joined = s.get_or_load(&key, async |_: &u32| {
            ran.fetch_add(1, Ordering::SeqCst);
            Ok::<_, Infallible>("second".to_string())
        });
        let release_later = async {
            tokio::task::yield_now().await;
            release.notify_one();
        };
        tokio::join!(joined, release_later).0
    };
    let (first, joined) = tokio::join!(first, joiners);
    assert_eq!(first.expect("loads"), "filled");
    assert_eq!(
        joined.expect("joins"),
        "filled",
        "a joined waiter gets the value the loader returned"
    );
    assert_eq!(ran.load(Ordering::SeqCst), 1, "one loader run");
    assert_eq!(s.get(&key).await, None);
}

#[tokio::test]
async fn a_dropped_get_or_load_on_a_non_owner_frees_the_in_flight_load() {
    let (s, view, _tx, _calls) = loading_shard();
    let key = find_key_by_ownership(&view, false);
    tokio::select! {
        biased;
        _ = s.get_or_load(&key, async |_: &u32| {
            std::future::pending::<()>().await;
            Ok::<_, Infallible>("never".to_string())
        }) => unreachable!("the loader never returns"),
        () = tokio::task::yield_now() => {}
    }
    let loaded = tokio::time::timeout(Duration::from_secs(5), s.get_or_load(&key, filled("after")))
        .await
        .expect("the dropped load left nothing in flight")
        .expect("loads");
    assert_eq!(loaded, "after");
}

#[tokio::test]
async fn a_remove_forwarded_during_a_non_owner_fill_is_the_only_thing_queued() {
    let (s, view, _tx, _calls) = loading_shard();
    let key = find_key_by_ownership(&view, false);
    let started = Notify::new();
    let release = Notify::new();
    let load = s.get_or_load(&key, async |_: &u32| {
        started.notify_one();
        release.notified().await;
        Ok::<_, Infallible>("stale".to_string())
    });
    let race = async {
        started.notified().await;
        s.remove(&key).await.expect("remove");
        release.notify_one();
    };
    let (loaded, ()) = tokio::join!(load, race);
    assert_eq!(loaded.expect("loads"), "stale", "the read still answers");
    assert_eq!(s.get(&key).await, None);
    let queued = s.fan_out_queue().drain();
    assert!(
        matches!(
            &queued[..],
            [FanOutItem::Forward(rec)] if rec.is_tombstone()
        ),
        "only the remove goes to the owners: {queued:?}"
    );
}

#[tokio::test]
async fn a_fill_that_completes_after_the_view_moved_away_stores_nothing() {
    let (s, view, tx, _calls) = loading_shard();
    let crowded = crowded_view();
    let key = key_moved_away(&view, &crowded);
    let started = Notify::new();
    let release = Notify::new();
    let load = s.get_or_load(&key, async |_: &u32| {
        started.notify_one();
        release.notified().await;
        Ok::<_, Infallible>("filled".to_string())
    });
    let race = async {
        started.notified().await;
        tx.send(crowded).expect("receiver alive");
        release.notify_one();
    };
    let (loaded, ()) = tokio::join!(load, race);
    assert_eq!(loaded.expect("loads"), "filled");
    assert!(
        s.keys().is_empty(),
        "ownership is judged when the fill completes"
    );
    assert!(s.fan_out_queue().drain().is_empty(), "nothing is queued");
}

#[tokio::test]
async fn load_and_load_many_on_a_non_owner_answer_and_store_nothing() {
    let (s, view, _tx, calls) = loading_shard();
    let key = find_key_by_ownership(&view, false);
    let other = (key + 1..1_000_000)
        .find(|candidate| !view.owns(PartId::of_key(&key_bytes(candidate))))
        .expect("a second unowned key");
    let mut events = s.events();

    assert_eq!(
        s.load(&key).await.expect("load"),
        Some(format!("v{key}")),
        "the caller gets the loaded value"
    );
    let found = s.load_many([key, other]).await.expect("load_many");
    assert_eq!(
        found,
        HashMap::from([(key, format!("v{key}")), (other, format!("v{other}"))])
    );
    assert!(s.keys().is_empty(), "nothing is stored");
    assert!(s.fan_out_queue().drain().is_empty(), "nothing is sent");
    assert!(
        matches!(
            events.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ),
        "no event"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "each call asks the source again: a non-owner has no copy to hit"
    );
}

#[tokio::test]
async fn load_and_load_many_on_an_owner_store_and_fan_out() {
    let (s, view, _tx, _calls) = loading_shard();
    let key = find_key_by_ownership(&view, true);
    assert_eq!(s.load(&key).await.expect("load"), Some(format!("v{key}")));
    assert_eq!(s.get(&key).await, Some(format!("v{key}")));
    assert_eq!(applied_keys(s.fan_out_queue().drain()), vec![key]);
}

#[tokio::test]
async fn a_load_served_for_a_part_this_node_does_not_own_reports_the_value_uncached() {
    let (s, view, _tx, _calls) = loading_shard();
    let unowned = find_key_by_ownership(&view, false);
    let owned = find_key_by_ownership(&view, true);
    let LoadServe::Loaded { found, uncached } = s
        .serve_load_encoded(vec![key_bytes(&unowned), key_bytes(&owned)])
        .await
    else {
        panic!("a warm shard with a loader serves a load");
    };
    assert_eq!(
        found.iter().map(|rec| rec.key.clone()).collect::<Vec<_>>(),
        vec![key_bytes(&owned)],
        "the stored fill of an owned key is a record"
    );
    assert_eq!(
        uncached
            .iter()
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>(),
        vec![key_bytes(&unowned)],
        "a fill this node does not own is only answered"
    );
}

#[tokio::test]
async fn a_non_owner_fill_at_the_memory_ceiling_counts_no_refusal_and_an_owner_fill_counts_one() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let eligible = (1..=5u64).map(NodeId::from).collect::<Vec<_>>();
    let (s, view, _tx) = distributed_shard::<u32, String>(
        NodeId::from(1),
        eligible,
        2,
        Arc::new(ResidencySet::new()),
    );
    let s = s.with_max_resident_bytes(1);
    let refusals = || -> u64 {
        snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(key, ..)| {
                key.key().name() == "sundog_ceiling_refusals_total"
                    && key
                        .key()
                        .labels()
                        .any(|label| label.key() == "kind" && label.value() == "fill")
            })
            .map(|(_, _, _, value)| match value {
                metrics_util::debugging::DebugValue::Counter(n) => n,
                _ => 0,
            })
            .sum()
    };
    let owned = find_key_by_ownership(&view, true);
    let unowned = find_key_by_ownership(&view, false);
    s.insert(owned, "resident".to_string()).await.expect("fits");
    assert!(s.at_ceiling().is_some(), "one byte of room is gone");

    assert_eq!(
        s.get_or_load(&unowned, filled("a")).await.expect("loads"),
        "a"
    );
    assert_eq!(
        refusals(),
        0,
        "a non-owner stores nothing, so refuses nothing"
    );
    let second_owned = (owned + 1..1_000_000)
        .find(|key| view.owns(PartId::of_key(&key_bytes(key))))
        .expect("a second owned key");
    assert_eq!(
        s.get_or_load(&second_owned, filled("b"))
            .await
            .expect("loads"),
        "b"
    );
    assert_eq!(refusals(), 1, "an owner's refused fill is counted");
}

/// A Distributed shard with a 10s TTL on a manual clock, refresh-ahead at
/// 0.8 and a loader counting its calls.
fn refreshing_distributed_shard(
    clock: &Arc<AtomicU64>,
    calls: &Arc<AtomicUsize>,
) -> (Shard<u32, String>, Arc<OwnershipView>, ViewSender) {
    let self_node = NodeId::from(1);
    let owners = NonZeroU8::new(2).expect("nonzero");
    let (tracker, tx) = OwnershipTracker::seed(
        self_node,
        &[],
        &HashMap::new(),
        &SmolStr::new("test"),
        owners,
    );
    let view = Arc::new(OwnershipView::compute(
        self_node,
        (1..=5u64).map(NodeId::from).collect(),
        owners,
    ));
    tx.send(Arc::clone(&view)).expect("receiver alive");
    let now = Arc::clone(clock);
    let counted = Arc::clone(calls);
    let s = Shard::<u32, String>::new(
        SmolStr::new("test"),
        Mode::Distributed { owners },
        self_node,
        u64::MAX,
        Some(Duration::from_secs(10)),
        None,
    )
    .with_ownership(tracker, Arc::new(ResidencySet::new()))
    .with_clock(Arc::new(move || now.load(Ordering::SeqCst)))
    .with_loader(Loader::batch(move |keys: Vec<u32>| {
        counted.fetch_add(1, Ordering::SeqCst);
        async move {
            Ok::<_, Infallible>(
                keys.into_iter()
                    .map(|key| (key, format!("reloaded{key}")))
                    .collect::<HashMap<_, _>>(),
            )
        }
    }))
    .with_refresh_ahead(800);
    (s, view, tx)
}

#[tokio::test]
async fn a_refresh_of_an_entry_held_through_disown_grace_calls_no_loader_and_changes_nothing() {
    const T0: u64 = 1_000_000;
    let clock = Arc::new(AtomicU64::new(T0));
    let calls = Arc::new(AtomicUsize::new(0));
    let (s, view, tx) = refreshing_distributed_shard(&clock, &calls);
    let crowded = crowded_view();
    let key = key_moved_away(&view, &crowded);
    s.insert(key, "held".to_string())
        .await
        .expect("owner insert");
    let held = key_bytes(&key);
    let LocalRecord::Live { version: ver, .. } = s.local_record(&held).1 else {
        panic!("the owner holds the entry");
    };
    clock.store(T0 + 9_000, Ordering::SeqCst);
    tx.send(crowded).expect("receiver alive");
    let _ = s.fan_out_queue().drain();

    let tally = s
        .refresh(vec![RefreshRequest {
            key,
            key_bytes: held.clone(),
            ver,
            hinted: false,
        }])
        .await;

    assert_eq!(tally, crate::store::refresh::RefreshTally::default());
    assert_eq!(calls.load(Ordering::SeqCst), 0, "no loader call");
    assert!(
        matches!(s.local_record(&held).1, LocalRecord::Live { version: now, .. } if now == ver),
        "the entry is as it was"
    );
    assert!(s.fan_out_queue().drain().is_empty(), "nothing is queued");
}

#[tokio::test]
async fn a_read_asks_for_a_refresh_again_after_the_node_regains_the_part_a_refresh_skipped() {
    const T0: u64 = 1_000_000;
    let clock = Arc::new(AtomicU64::new(T0));
    let calls = Arc::new(AtomicUsize::new(0));
    let (s, view, tx) = refreshing_distributed_shard(&clock, &calls);
    let mut requests = s.take_refresh_requests().expect("refresh is on");
    let crowded = crowded_view();
    let key = key_moved_away(&view, &crowded);
    s.insert(key, "held".to_string())
        .await
        .expect("owner insert");
    let held = key_bytes(&key);
    let hash = crate::store::engine::hash_key_bytes(held.as_ref());
    let LocalRecord::Live { version: ver, .. } = s.local_record(&held).1 else {
        panic!("the owner holds the entry");
    };
    let expires_at_ms = Some(T0 + 10_000);
    let now = T0 + 9_000;
    clock.store(now, Ordering::SeqCst);
    tx.send(crowded).expect("receiver alive");

    // A read of the disown-grace copy asks, and the refresh skips it.
    s.note_read(&key, held.as_ref(), hash, ver, expires_at_ms, now);
    let request = requests.try_recv().expect("the read asks for a reload");
    assert_eq!(
        s.refresh(vec![request]).await,
        crate::store::refresh::RefreshTally::default()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0, "no loader call");

    // The node regains the part: the entry is owned again at the same version.
    tx.send(view).expect("receiver alive");
    s.note_read(&key, held.as_ref(), hash, ver, expires_at_ms, now);
    assert!(
        requests.try_recv().is_ok(),
        "a read after the part returns asks again"
    );
}

#[tokio::test]
async fn a_refresh_of_an_owned_entry_still_reloads_it() {
    const T0: u64 = 1_000_000;
    let clock = Arc::new(AtomicU64::new(T0));
    let calls = Arc::new(AtomicUsize::new(0));
    let (s, view, _tx) = refreshing_distributed_shard(&clock, &calls);
    let key = find_key_by_ownership(&view, true);
    s.insert(key, "held".to_string())
        .await
        .expect("owner insert");
    let held = key_bytes(&key);
    let LocalRecord::Live { version: ver, .. } = s.local_record(&held).1 else {
        panic!("the owner holds the entry");
    };
    clock.store(T0 + 9_000, Ordering::SeqCst);

    let tally = s
        .refresh(vec![RefreshRequest {
            key,
            key_bytes: held,
            ver,
            hinted: false,
        }])
        .await;

    assert_eq!(tally.loaded, 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(s.get(&key).await, Some(format!("reloaded{key}")));
}

#[test]
fn fill_route_installs_only_an_owned_fill_under_the_ceiling() {
    assert_eq!(fill_route(true, false), FillRoute::Install);
    assert_eq!(
        fill_route(true, true),
        FillRoute::Uncached {
            counts_refusal: true
        },
        "an owner's fill the ceiling refuses is a counted refusal"
    );
    for at_ceiling in [false, true] {
        assert_eq!(
            fill_route(false, at_ceiling),
            FillRoute::Uncached {
                counts_refusal: false
            },
            "a non-owner stores nothing, at the ceiling or not, and refuses nothing"
        );
    }
}

#[test]
fn read_through_route_asks_the_owners_only_for_a_distributed_non_owner() {
    let modes = [
        Mode::Local,
        Mode::Invalidation,
        Mode::Replicated,
        Mode::distributed(),
    ];
    for mode in modes {
        for owns in [false, true] {
            let want = if matches!(mode, Mode::Distributed { .. }) && !owns {
                ReadThroughRoute::OwnersFirst
            } else {
                ReadThroughRoute::Local
            };
            assert_eq!(read_through_route(mode, owns), want, "{mode:?} owns={owns}");
        }
    }
}

#[tokio::test]
async fn the_read_through_route_follows_the_shards_current_view() {
    let (s, view, tx, _calls) = loading_shard();
    let owned = find_key_by_ownership(&view, true);
    let foreign = find_key_by_ownership(&view, false);
    assert_eq!(
        s.read_through_route_of(&owned).expect("encodes"),
        ReadThroughRoute::Local
    );
    assert_eq!(
        s.read_through_route_of(&foreign).expect("encodes"),
        ReadThroughRoute::OwnersFirst
    );

    let crowded = crowded_view();
    let moved = key_moved_away(&view, &crowded);
    tx.send(crowded).expect("the tracker holds a receiver");
    assert_eq!(
        s.read_through_route_of(&moved).expect("encodes"),
        ReadThroughRoute::OwnersFirst,
        "a part the view moved away asks the owners"
    );

    let replicated = shard::<u32, String>(1);
    assert_eq!(
        replicated.read_through_route_of(&7).expect("encodes"),
        ReadThroughRoute::Local,
        "a shard with no ownership view loads on a miss"
    );
}

#[tokio::test]
async fn get_or_load_via_on_a_non_owner_answers_with_the_asked_value_and_stores_nothing() {
    let (s, view, _tx, _calls) = loading_shard();
    let key = find_key_by_ownership(&view, false);
    let mut events = s.events();
    let ran = AtomicUsize::new(0);

    let got = s
        .get_or_load_via(
            &key,
            async |_: &u32| Ok(Some("asked".to_string())),
            async |_: &u32| {
                ran.fetch_add(1, Ordering::SeqCst);
                Ok::<_, Infallible>("loaded".to_string())
            },
        )
        .await
        .expect("answers");

    assert_eq!(got, "asked");
    assert_eq!(ran.load(Ordering::SeqCst), 0, "the loader does not run");
    assert_eq!(s.get(&key).await, None, "a non-owner keeps no copy");
    assert!(s.fan_out_queue().drain().is_empty(), "nothing is sent");
    assert!(
        matches!(
            events.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ),
        "a value stored nowhere emits no event"
    );
}

#[tokio::test]
async fn get_or_load_via_on_an_owner_stores_the_asked_value_and_fans_it_out() {
    let (s, view, _tx, _calls) = loading_shard();
    let key = find_key_by_ownership(&view, true);
    let ran = AtomicUsize::new(0);

    let got = s
        .get_or_load_via(
            &key,
            async |_: &u32| Ok(Some("asked".to_string())),
            async |_: &u32| {
                ran.fetch_add(1, Ordering::SeqCst);
                Ok::<_, Infallible>("loaded".to_string())
            },
        )
        .await
        .expect("answers");

    assert_eq!(got, "asked");
    assert_eq!(ran.load(Ordering::SeqCst), 0);
    assert_eq!(s.get(&key).await, Some("asked".to_string()));
    assert_eq!(applied_keys(s.fan_out_queue().drain()), vec![key]);
}

#[tokio::test]
async fn get_or_load_via_runs_the_loader_when_the_ask_has_no_value() {
    let (s, view, _tx, _calls) = loading_shard();
    let key = find_key_by_ownership(&view, false);

    let got = s
        .get_or_load_via(&key, async |_: &u32| Ok(None), filled("loaded"))
        .await
        .expect("answers");

    assert_eq!(got, "loaded");
    assert_eq!(s.get(&key).await, None);
}

#[tokio::test]
async fn get_or_load_via_returns_the_asks_error_and_a_joined_call_takes_the_fill_over() {
    let (s, view, _tx, _calls) = loading_shard();
    let key = find_key_by_ownership(&view, false);
    let release = Notify::new();
    let first = s.get_or_load_via(
        &key,
        async |_: &u32| {
            release.notified().await;
            Err(CacheError::NoLoader {
                cache: SmolStr::new("asked"),
            })
        },
        filled("first"),
    );
    let waiter = async {
        let joined = s.get_or_load_via(
            &key,
            async |_: &u32| Ok(Some("second".to_string())),
            filled("loaded"),
        );
        let release_later = async {
            tokio::task::yield_now().await;
            release.notify_one();
        };
        tokio::join!(joined, release_later).0
    };
    let (first, joined) = tokio::join!(first, waiter);

    assert!(
        matches!(first, Err(CacheError::NoLoader { .. })),
        "the ask's own error answers the call that ran it: {first:?}"
    );
    assert_eq!(
        joined.expect("the joined call answers"),
        "second",
        "it takes the fill over and asks for itself"
    );
    assert_eq!(s.get(&key).await, None);
}

#[tokio::test]
async fn concurrent_get_or_load_via_calls_share_one_ask() {
    let (s, view, _tx, _calls) = loading_shard();
    let key = find_key_by_ownership(&view, false);
    let asks = AtomicUsize::new(0);
    let loads = AtomicUsize::new(0);
    let release = Notify::new();
    let calls = (0..8).map(|_| {
        s.get_or_load_via(
            &key,
            async |_: &u32| {
                asks.fetch_add(1, Ordering::SeqCst);
                release.notified().await;
                Ok(None)
            },
            async |_: &u32| {
                loads.fetch_add(1, Ordering::SeqCst);
                Ok::<_, Infallible>("loaded".to_string())
            },
        )
    });
    let release_later = async {
        tokio::task::yield_now().await;
        release.notify_one();
    };
    let (answers, ()) = tokio::join!(futures::future::join_all(calls), release_later);

    for answer in answers {
        assert_eq!(answer.expect("answers"), "loaded");
    }
    assert_eq!(asks.load(Ordering::SeqCst), 1);
    assert_eq!(loads.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_dropped_ask_frees_the_in_flight_fill() {
    let (s, view, _tx, _calls) = loading_shard();
    let key = find_key_by_ownership(&view, false);
    tokio::select! {
        biased;
        _ = s.get_or_load_via(
            &key,
            async |_: &u32| {
                std::future::pending::<()>().await;
                Ok(None)
            },
            filled("never"),
        ) => unreachable!("the ask never returns"),
        () = tokio::task::yield_now() => {}
    }
    let loaded = tokio::time::timeout(Duration::from_secs(5), s.get_or_load(&key, filled("after")))
        .await
        .expect("the dropped ask left nothing in flight")
        .expect("loads");
    assert_eq!(loaded, "after");
}

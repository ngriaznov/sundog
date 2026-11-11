//! Read-through fills: `get_or_load`, `get_or_insert_with` and how a fill interacts with concurrent writes.

use super::*;

#[tokio::test]
async fn an_invalidation_fill_queues_no_fan_out_and_a_replicated_fill_does() {
    let invalidation = Shard::<u32, String>::new(
        SmolStr::new("test"),
        Mode::Invalidation,
        NodeId::from(1),
        10_000,
        None,
        None,
    );
    invalidation
        .get_or_load(
            &7,
            async |_key: &u32| -> Result<String, std::convert::Infallible> {
                Ok("loaded".to_string())
            },
        )
        .await
        .expect("load succeeds");
    assert!(
        invalidation.fan_out.drain().is_empty(),
        "an Invalidation fill sends no invalidation to peers"
    );

    let replicated = shard::<u32, String>(1);
    replicated
        .get_or_load(
            &7,
            async |_key: &u32| -> Result<String, std::convert::Infallible> {
                Ok("loaded".to_string())
            },
        )
        .await
        .expect("load succeeds");
    assert_eq!(applied_keys(replicated.fan_out.drain()), vec![7]);
}

#[tokio::test]
async fn get_or_load_keeps_a_remove_that_lands_while_the_loader_runs() {
    let s = shard::<u32, String>(1);
    let started = tokio::sync::Notify::new();
    let release = tokio::sync::Notify::new();
    let load = s.get_or_load(
        &7,
        async |_key: &u32| -> Result<String, std::convert::Infallible> {
            started.notify_one();
            release.notified().await;
            Ok("stale".to_string())
        },
    );
    let race = async {
        started.notified().await;
        s.remove(&7).await.expect("remove");
        release.notify_one();
    };
    let (loaded, ()) = tokio::join!(load, race);
    assert_eq!(
        loaded.expect("load succeeds"),
        "stale",
        "the read that raced the removal answers with what it loaded"
    );
    assert_eq!(s.get(&7).await, None, "the removal stays removed");
    let _ = s.fan_out.drain();
    assert_eq!(
        s.get_or_load(
            &7,
            async |_key: &u32| -> Result<String, std::convert::Infallible> {
                Ok("reloaded".to_string())
            }
        )
        .await
        .expect("load succeeds"),
        "reloaded",
        "the next read loads afresh over the removal"
    );
}

#[tokio::test]
async fn get_or_load_keeps_an_insert_that_lands_while_the_loader_runs() {
    let s = shard::<u32, String>(1);
    let started = tokio::sync::Notify::new();
    let release = tokio::sync::Notify::new();
    let load = s.get_or_load(
        &7,
        async |_key: &u32| -> Result<String, std::convert::Infallible> {
            started.notify_one();
            release.notified().await;
            Ok("stale".to_string())
        },
    );
    let race = async {
        started.notified().await;
        s.insert(7, "fresh".to_string()).await.expect("insert");
        release.notify_one();
    };
    let (loaded, ()) = tokio::join!(load, race);
    assert_eq!(loaded.expect("load succeeds"), "stale");
    assert_eq!(
        s.get(&7).await,
        Some("fresh".to_string()),
        "the write that landed during the load keeps its place"
    );
    assert_eq!(
        applied_keys(s.fan_out.drain()),
        vec![7],
        "only the insert fans out; the superseded fill sends nothing"
    );
}

#[tokio::test]
async fn get_or_load_drops_a_fill_an_invalidation_lands_during() {
    let s = Shard::<u32, String>::new(
        SmolStr::new("test"),
        Mode::Invalidation,
        NodeId::from(1),
        10_000,
        None,
        None,
    );
    let started = tokio::sync::Notify::new();
    let release = tokio::sync::Notify::new();
    let load = s.get_or_load(
        &7,
        async |_key: &u32| -> Result<String, std::convert::Infallible> {
            started.notify_one();
            release.notified().await;
            Ok("stale".to_string())
        },
    );
    let race = async {
        started.notified().await;
        ShardOps::invalidate(&s, key_bytes(&7u32), hlc(now_ms(), 2)).await;
        release.notify_one();
    };
    let (loaded, ()) = tokio::join!(load, race);
    assert_eq!(
        loaded.expect("load succeeds"),
        "stale",
        "the read that raced the invalidation answers with what it loaded"
    );
    assert_eq!(
        s.get(&7).await,
        None,
        "a value read before the key changed elsewhere is not cached"
    );
    assert_eq!(
        s.get_or_load(
            &7,
            async |_key: &u32| -> Result<String, std::convert::Infallible> {
                Ok("fresh".to_string())
            }
        )
        .await
        .expect("load succeeds"),
        "fresh",
        "the next read loads afresh"
    );
    assert_eq!(s.get(&7).await, Some("fresh".to_string()));
}

#[tokio::test]
async fn get_or_load_drops_a_fill_a_local_invalidation_lands_during() {
    let s = shard::<u32, String>(1);
    let started = tokio::sync::Notify::new();
    let release = tokio::sync::Notify::new();
    let load = s.get_or_load(
        &7,
        async |_key: &u32| -> Result<String, std::convert::Infallible> {
            started.notify_one();
            release.notified().await;
            Ok("stale".to_string())
        },
    );
    let race = async {
        started.notified().await;
        s.invalidate_local(&7).await;
        release.notify_one();
    };
    let (loaded, ()) = tokio::join!(load, race);
    assert_eq!(loaded.expect("load succeeds"), "stale");
    assert_eq!(s.get(&7).await, None, "the busted key stays uncached");
}

#[tokio::test]
async fn get_or_load_waiters_share_a_dropped_fill_without_loading_again() {
    let s = Shard::<u32, String>::new(
        SmolStr::new("test"),
        Mode::Invalidation,
        NodeId::from(1),
        10_000,
        None,
        None,
    );
    let calls = std::sync::atomic::AtomicUsize::new(0);
    let started = tokio::sync::Notify::new();
    let release = tokio::sync::Notify::new();
    let first = s.get_or_load(
        &7,
        async |_key: &u32| -> Result<String, std::convert::Infallible> {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            started.notify_one();
            release.notified().await;
            Ok("stale".to_string())
        },
    );
    let rest = async {
        started.notified().await;
        let joined = s.get_or_load(
            &7,
            async |_key: &u32| -> Result<String, std::convert::Infallible> {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok("second".to_string())
            },
        );
        let race = async {
            tokio::task::yield_now().await;
            ShardOps::invalidate(&s, key_bytes(&7u32), hlc(now_ms(), 2)).await;
            release.notify_one();
        };
        let (joined, ()) = tokio::join!(joined, race);
        joined
    };
    let (first, joined) = tokio::join!(first, rest);
    assert_eq!(first.expect("load succeeds"), "stale");
    assert_eq!(joined.expect("load succeeds"), "stale");
    assert_eq!(
        calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the joined read answers with the dropped fill's value"
    );
    assert_eq!(s.get(&7).await, None);
}

#[tokio::test]
async fn get_or_load_fills_once_and_emits_created() {
    let s = shard::<u32, String>(1);
    let mut events = s.events();
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let c = std::sync::Arc::clone(&calls);
    let loaded = s
        .get_or_load(
            &7,
            async move |_key: &u32| -> Result<String, std::convert::Infallible> {
                c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok("loaded".to_string())
            },
        )
        .await
        .expect("load succeeds");
    assert_eq!(loaded, "loaded");
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    match events.recv().await.expect("created") {
        Event::Created {
            key: 7,
            value,
            origin: Origin::Local,
        } => assert_eq!(value, "loaded"),
        other => panic!("unexpected event: {other:?}"),
    }

    // A hit does not call the loader again.
    let c2 = std::sync::Arc::clone(&calls);
    let hit = s
        .get_or_load(
            &7,
            async move |_key: &u32| -> Result<String, std::convert::Infallible> {
                c2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok("should-not-run".to_string())
            },
        )
        .await
        .expect("hit succeeds");
    assert_eq!(hit, "loaded");
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn get_or_load_propagates_loader_error() {
    let s = shard::<u32, String>(1);
    let err = s
        .get_or_load(&1, async move |_key: &u32| -> Result<String, BoomError> {
            Err(BoomError)
        })
        .await
        .expect_err("loader failed");
    assert!(matches!(err, CacheError::Loader(_)));
}

#[tokio::test]
async fn get_or_load_stampede_collapses_to_one_loader_call() {
    const CONCURRENCY: usize = 64;
    let s = Arc::new(shard::<u32, String>(1));
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..CONCURRENCY {
        let s = Arc::clone(&s);
        let calls = std::sync::Arc::clone(&calls);
        tasks.spawn(async move {
            s.get_or_load(&42, async move |_key: &u32| -> Result<String, BoomError> {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                // Widens the race window so every caller waits on this one
                // load.
                tokio::time::sleep(Duration::from_millis(30)).await;
                Ok("loaded-once".to_string())
            })
            .await
            .expect("loader succeeds")
        });
    }

    let mut results = Vec::with_capacity(CONCURRENCY);
    while let Some(result) = tasks.join_next().await {
        results.push(result.expect("stampede caller does not panic"));
    }

    assert_eq!(
        calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the loader runs exactly once under a stampede of {CONCURRENCY} concurrent misses"
    );
    assert!(results.iter().all(|value| value == "loaded-once"));
    assert_eq!(results.len(), CONCURRENCY);
}

#[tokio::test]
async fn get_or_load_stampede_with_a_failing_loader_shares_one_error_with_every_waiter() {
    const CONCURRENCY: usize = 8;
    let s = Arc::new(shard::<u32, String>(1));
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..CONCURRENCY {
        let s = Arc::clone(&s);
        let calls = std::sync::Arc::clone(&calls);
        tasks.spawn(async move {
            s.get_or_load(&99, async move |_key: &u32| -> Result<String, BoomError> {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                // Widens the race window so every caller joins this one
                // load before it fails.
                tokio::time::sleep(Duration::from_millis(30)).await;
                Err(BoomError)
            })
            .await
        });
    }

    let mut results = Vec::with_capacity(CONCURRENCY);
    while let Some(result) = tasks.join_next().await {
        results.push(result.expect("stampede caller does not panic"));
    }

    assert_eq!(
        calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the loader runs exactly once under a stampede of {CONCURRENCY} concurrent misses"
    );
    assert_eq!(results.len(), CONCURRENCY);
    assert!(
        results
            .iter()
            .all(|r| matches!(r, Err(CacheError::Loader(_)))),
        "every joined waiter returns the same Loader failure"
    );
}

/// Deterministically lands a write in the narrow gap between
/// `get_or_load`'s fast-path miss and `Engine::miss_or_join`'s locked
/// re-check, by giving the shard a clock whose second call (the one
/// feeding `miss_or_join`) performs the write itself before returning a
/// timestamp. No real concurrency, and no flakiness: the write always
/// lands on the second clock read, deterministically.
#[tokio::test]
async fn get_or_load_hits_when_a_write_lands_between_the_fast_path_miss_and_the_locked_recheck() {
    let s0 = shard::<u32, String>(1);
    let engine_for_clock = Arc::clone(&s0.engine);
    let resolver_for_clock = Arc::clone(&s0.resolver);
    let tombstone_ttl_ms = s0.tombstone_ttl_ms;
    let tombstone_max_ttl_ms = s0.tombstone_max_ttl_ms;

    let call_idx = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let call_idx_for_clock = Arc::clone(&call_idx);
    let clock: Arc<dyn Fn() -> u64 + Send + Sync> = Arc::new(move || {
        let n = call_idx_for_clock.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if n == 1 {
            let kb = key_bytes(&7u32);
            let hash = engine::hash_key_bytes(kb.as_ref());
            let bucket = engine::stripe_index_from_hash(hash);
            let value = "landed".to_string();
            let encoded = Bytes::from(postcard::to_stdvec(&value).expect("encode"));
            let _ = engine_for_clock.apply_many(
                engine::Arrival::Peer,
                bucket,
                vec![(
                    hash,
                    7u32,
                    kb,
                    hlc(1, 9),
                    Incoming::Put {
                        value,
                        expires_at_ms: None,
                        encoded,
                    },
                )],
                resolver_for_clock.as_ref(),
                tombstone_ttl_ms,
                tombstone_max_ttl_ms,
                0,
            );
        }
        0
    });
    let s = s0.with_clock(clock);

    let loaded = s
        .get_or_load(&7, async move |_key: &u32| -> Result<String, BoomError> {
            panic!(
                "the value already landed before this call reaches the loader; \
                 get_or_load must resolve via the locked re-check's Hit instead"
            );
        })
        .await
        .expect("resolves via the Hit path, never reaching the loader");
    assert_eq!(loaded, "landed");
    assert_eq!(
        call_idx.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "exactly two clock reads: the fast-path miss, then the locked re-check that hits"
    );
}

#[tokio::test]
async fn get_or_insert_with_fills_on_miss_and_skips_make_on_hit() {
    let s = shard::<u32, String>(1);
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let c1 = std::sync::Arc::clone(&calls);
    let filled = s
        .get_or_insert_with(&1, async move |_key: &u32| {
            c1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            "loaded".to_string()
        })
        .await
        .expect("fill succeeds");
    assert_eq!(filled, "loaded");
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);

    let c2 = std::sync::Arc::clone(&calls);
    let hit = s
        .get_or_insert_with(&1, async move |_key: &u32| {
            c2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            "should-not-run".to_string()
        })
        .await
        .expect("hit succeeds");
    assert_eq!(hit, "loaded");
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}

/// Misses counted by `sundog_cache_misses_total` in `snapshotter`.
fn counted_misses(snapshotter: &metrics_util::debugging::Snapshotter) -> u64 {
    use metrics_util::debugging::DebugValue;
    let mut misses = 0;
    for (key, _, _, value) in snapshotter.snapshot().into_vec() {
        if let ("sundog_cache_misses_total", DebugValue::Counter(n)) = (key.key().name(), value) {
            misses += n;
        }
    }
    misses
}

#[tokio::test]
async fn a_failed_get_or_load_run_counts_one_miss() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let s = shard::<u32, String>(1);
    for key in [1u32, 2] {
        let err = s
            .get_or_load(&key, async move |_key: &u32| -> Result<String, BoomError> {
                Err(BoomError)
            })
            .await
            .expect_err("loader failed");
        assert!(matches!(err, CacheError::Loader(_)));
    }
    assert_eq!(
        counted_misses(&snapshotter),
        2,
        "each failed loader execution counts one miss"
    );
}

#[tokio::test]
async fn a_failed_run_counts_one_miss_for_its_owner_and_none_for_joined_callers() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let s = Arc::new(shard::<u32, String>(1));
    let gate = Arc::new(tokio::sync::Notify::new());
    let started = Arc::new(tokio::sync::Notify::new());
    let owner = tokio::spawn({
        let s = Arc::clone(&s);
        let gate = Arc::clone(&gate);
        let started = Arc::clone(&started);
        async move {
            s.get_or_load(&1, async move |_key: &u32| -> Result<String, BoomError> {
                started.notify_one();
                gate.notified().await;
                Err(BoomError)
            })
            .await
        }
    });
    // The owner holds the in-flight entry before the joiners ask.
    started.notified().await;
    let joiners: Vec<_> = (0..3)
        .map(|_| {
            let s = Arc::clone(&s);
            tokio::spawn(async move {
                s.get_or_load(&1, async |_key: &u32| -> Result<String, BoomError> {
                    panic!("a joined call never runs its loader")
                })
                .await
            })
        })
        .collect();
    // The joiners subscribe to the in-flight entry before it fails.
    tokio::time::sleep(Duration::from_millis(50)).await;
    gate.notify_one();
    owner.await.expect("owner").expect_err("loader failed");
    for joiner in joiners {
        joiner
            .await
            .expect("joiner")
            .expect_err("shares the failure");
    }
    assert_eq!(counted_misses(&snapshotter), 1);
}

#[tokio::test]
async fn a_failed_load_many_run_counts_one_miss_per_key() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let s = shard::<u32, String>(1).with_loader(Loader::batch(|_keys: Vec<u32>| async move {
        Err::<HashMap<u32, String>, _>(BoomError)
    }));
    s.load_many([1u32, 2, 3]).await.expect_err("source down");
    assert_eq!(
        counted_misses(&snapshotter),
        3,
        "each key of a failed batch run counts one miss"
    );
}

#[tokio::test]
async fn a_successful_run_counts_one_miss() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let s = shard::<u32, String>(1);
    s.get_or_load(&1, async move |_key: &u32| -> Result<String, BoomError> {
        Ok("x".into())
    })
    .await
    .expect("ok");
    assert_eq!(counted_misses(&snapshotter), 1);
}

/// A value whose encoding fails when it holds `true`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
struct Flaky(bool);

impl Serialize for Flaky {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.0 {
            Err(serde::ser::Error::custom("refuses to encode"))
        } else {
            serializer.serialize_bool(false)
        }
    }
}

#[tokio::test]
async fn a_loaded_value_that_fails_to_encode_fails_get_or_load_with_codec_and_frees_the_load() {
    let s = shard::<u32, Flaky>(1);
    let err = s
        .get_or_load(&1, async |_key: &u32| Ok::<_, BoomError>(Flaky(true)))
        .await
        .expect_err("the value does not encode");
    assert!(matches!(err, CacheError::Codec(_)), "got {err:?}");
    assert_eq!(s.get(&1).await, None, "nothing is stored");
    let again = s
        .get_or_load(&1, async |_key: &u32| Ok::<_, BoomError>(Flaky(false)))
        .await
        .expect("the in-flight entry was released");
    assert_eq!(again, Flaky(false));
}

#[tokio::test]
async fn joined_waiters_get_the_codec_error_of_a_value_that_fails_to_encode() {
    let s = Arc::new(shard::<u32, Flaky>(1));
    let gate = Arc::new(tokio::sync::Notify::new());
    let started = Arc::new(tokio::sync::Notify::new());
    let owner = tokio::spawn({
        let s = Arc::clone(&s);
        let gate = Arc::clone(&gate);
        let started = Arc::clone(&started);
        async move {
            s.get_or_load(&1, async move |_key: &u32| {
                started.notify_one();
                gate.notified().await;
                Ok::<_, BoomError>(Flaky(true))
            })
            .await
        }
    });
    started.notified().await;
    let joiners: Vec<_> = (0..3)
        .map(|_| {
            let s = Arc::clone(&s);
            tokio::spawn(async move {
                s.get_or_load(&1, async |_key: &u32| Ok::<_, BoomError>(Flaky(false)))
                    .await
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(50)).await;
    gate.notify_one();
    let err = owner.await.expect("owner").expect_err("does not encode");
    assert!(matches!(err, CacheError::Codec(_)), "owner got {err:?}");
    for joiner in joiners {
        let err = joiner.await.expect("joiner").expect_err("shares the error");
        assert!(matches!(err, CacheError::Codec(_)), "joiner got {err:?}");
    }
}

#[tokio::test]
async fn load_and_load_many_fail_with_codec_for_a_value_that_fails_to_encode() {
    let s = shard::<u32, Flaky>(1).with_loader(Loader::batch(|keys: Vec<u32>| async move {
        Ok::<_, BoomError>(
            keys.into_iter()
                .map(|key| (key, Flaky(key == 2)))
                .collect::<HashMap<_, _>>(),
        )
    }));
    let err = s.load(&2).await.expect_err("does not encode");
    assert!(matches!(err, CacheError::Codec(_)), "got {err:?}");
    let err = s.load_many([1, 2, 3]).await.expect_err("does not encode");
    assert!(matches!(err, CacheError::Codec(_)), "got {err:?}");
    // The in-flight entries are free: the good keys load, the bad one fails
    // again rather than hanging.
    assert_eq!(s.load(&1).await.expect("loads"), Some(Flaky(false)));
    let err = s.load(&2).await.expect_err("does not encode");
    assert!(matches!(err, CacheError::Codec(_)), "got {err:?}");
}

#[tokio::test]
async fn serving_a_load_whose_value_fails_to_encode_replies_failed() {
    let loader = || {
        Loader::batch(|keys: Vec<u32>| async move {
            Ok::<_, BoomError>(
                keys.into_iter()
                    .map(|key| (key, Flaky(true)))
                    .collect::<HashMap<_, _>>(),
            )
        })
    };
    // A stored fill fails at the fill.
    let s = shard::<u32, Flaky>(1).with_loader(loader());
    assert!(matches!(
        s.serve_load_encoded(vec![key_bytes(&1u32)]).await,
        LoadServe::Failed(_)
    ));
    // A value answered without being stored fails at the reply's encoding.
    let ceiling = shard::<u32, Flaky>(1)
        .with_loader(loader())
        .with_max_resident_bytes(1);
    ceiling.insert(9, Flaky(false)).await.expect("insert");
    assert!(matches!(
        ceiling.serve_load_encoded(vec![key_bytes(&1u32)]).await,
        LoadServe::Failed(_)
    ));
}

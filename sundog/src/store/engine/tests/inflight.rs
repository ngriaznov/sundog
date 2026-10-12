//! Tests for single-flight loads: joining a load, completing or abandoning it, and the fill guards.

use std::sync::atomic::AtomicUsize;

use super::*;

#[tokio::test]
async fn stampede_collapses_to_one_loader_n_minus_one_hits() {
    const CONCURRENCY: usize = 32;
    let engine = Arc::new(engine_u32_string(u64::MAX, None));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..CONCURRENCY {
        let engine = Arc::clone(&engine);
        let calls = Arc::clone(&calls);
        tasks.spawn(async move {
            let key = 7u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            loop {
                if let Some(v) = engine.get(&key, 0) {
                    return v;
                }
                match engine.miss_or_join(&kb, hash, 0) {
                    JoinOutcome::Hit(v) => return v,
                    JoinOutcome::Join(inflight, mut done) => {
                        let _ = done.changed().await;
                        if let Some(e) = inflight.error.get() {
                            panic!("unexpected loader failure: {e}");
                        }
                    }
                    JoinOutcome::Owner(inflight) => {
                        let guard = engine.guard_inflight(kb.clone(), hash, Arc::clone(&inflight));
                        calls.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        let value = "loaded-once".to_string();
                        let encoded = Bytes::from(postcard::to_stdvec(&value).expect("encode"));
                        let outcome = engine.complete_fresh_load(
                            FreshLoad {
                                key: &key,
                                key_bytes: &kb,
                                hash,
                                ver: hlc(1, 1),
                                value: value.clone(),
                                encoded,
                                expires_at_ms: None,
                            },
                            0,
                            &inflight,
                        );
                        assert_eq!(
                            outcome,
                            FillOutcome::Installed { had_live: false },
                            "a genuine miss sees no prior live entry"
                        );
                        guard.complete();
                        return value;
                    }
                }
            }
        });
    }
    let mut results = Vec::with_capacity(CONCURRENCY);
    while let Some(r) = tasks.join_next().await {
        results.push(r.expect("spawned call does not panic"));
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "exactly one loader call under a stampede"
    );
    assert!(results.iter().all(|v| v == "loaded-once"));
}

#[tokio::test]
async fn loader_error_is_shared_by_joined_waiters() {
    let engine = Arc::new(engine_u32_string(u64::MAX, None));
    let key = 9u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());

    let JoinOutcome::Owner(inflight) = engine.miss_or_join(&kb, hash, 0) else {
        panic!("first caller becomes the owner");
    };
    let guard = engine.guard_inflight(kb.clone(), hash, Arc::clone(&inflight));

    // The receiver subscribes under the stripe lock, so a late waiter still
    // sees the failure.
    let JoinOutcome::Join(joined, mut done) = engine.miss_or_join(&kb, hash, 0) else {
        panic!("second caller joins the existing load");
    };

    let boom: Arc<dyn std::error::Error + Send + Sync> = Arc::new(std::io::Error::other("boom"));
    engine.fail_inflight(&kb, hash, &inflight, boom);
    guard.complete();

    done.changed().await.expect("the owner finished the fill");
    assert!(
        joined.error.get().is_some(),
        "the joined waiter sees the error"
    );
    assert_eq!(
        engine.get(&key, 0),
        None,
        "a failed load never installs a value"
    );
}

#[tokio::test]
async fn a_cancelled_loader_lets_a_waiter_take_over() {
    let engine = Arc::new(engine_u32_string(u64::MAX, None));
    let key = 11u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());

    let JoinOutcome::Owner(inflight) = engine.miss_or_join(&kb, hash, 0) else {
        panic!("first caller becomes the owner");
    };
    {
        // Dropped without `complete()`, simulating a cancelled loader
        // future.
        let _guard = engine.guard_inflight(kb.clone(), hash, Arc::clone(&inflight));
    }

    assert!(
        !engine
            .stripe_lock(stripe_index_from_hash(hash))
            .read()
            .inflight
            .contains_key(kb.as_ref())
    );
    // A fresh caller becomes the new owner instead of joining a dead entry.
    match engine.miss_or_join(&kb, hash, 0) {
        JoinOutcome::Owner(_) => {}
        _ => panic!("a cancelled load's key is free for a new owner"),
    }
}

#[tokio::test]
async fn a_waiter_that_starts_waiting_after_the_owner_finished_still_wakes() {
    let engine = Arc::new(engine_u32_string(u64::MAX, None));
    let key = 13u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());

    let JoinOutcome::Owner(inflight) = engine.miss_or_join(&kb, hash, 0) else {
        panic!("first caller becomes the owner");
    };
    let JoinOutcome::Join(_, mut done) = engine.miss_or_join(&kb, hash, 0) else {
        panic!("second caller joins the existing load");
    };
    // The owner completes before the waiter ever polls its receiver.
    let encoded = Bytes::from(postcard::to_stdvec("late").expect("encode"));
    engine.complete_fresh_load(
        FreshLoad {
            key: &key,
            key_bytes: &kb,
            hash,
            ver: hlc(1, 1),
            value: "late".to_string(),
            encoded,
            expires_at_ms: None,
        },
        0,
        &inflight,
    );
    tokio::time::timeout(std::time::Duration::from_secs(1), done.changed())
        .await
        .expect("a completion that preceded the wait is not lost")
        .expect("the owner finished the fill");
    assert_eq!(engine.get(&key, 0), Some("late".to_string()));
}

#[test]
fn miss_or_join_hits_on_the_locked_recheck_after_a_concurrent_insert() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 5u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());

    assert_eq!(engine.get(&key, 0), None, "the fast-path read misses first");
    // A write lands on the key between the caller's fast-path miss and its
    // call to `miss_or_join`, e.g. a concurrent `get_or_load` owner or a
    // plain remote write.
    let _ = put(&engine, key, kb.clone(), "late".into(), hlc(1, 1), None, 0);

    match engine.miss_or_join(&kb, hash, 0) {
        JoinOutcome::Hit(v) => assert_eq!(v, "late"),
        _ => panic!("the locked re-check finds the entry that landed after the fast-path miss"),
    }
}

#[test]
fn complete_fresh_load_replaces_a_tombstone_and_keeps_digest_and_weight_correct() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 21u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());

    let _ = put(&engine, key, kb.clone(), "old".into(), hlc(1, 1), None, 0);
    tombstone(&engine, key, kb.clone(), hlc(2, 1), 0);
    assert_eq!(
        engine.get(&key, 0),
        None,
        "a tombstone sits where the load will land"
    );

    let JoinOutcome::Owner(inflight) = engine.miss_or_join(&kb, hash, 0) else {
        panic!("no live entry: this caller becomes the owner");
    };
    let encoded = Bytes::from(postcard::to_stdvec("fresh").expect("encode"));
    let outcome = engine.complete_fresh_load(
        FreshLoad {
            key: &key,
            key_bytes: &kb,
            hash,
            ver: hlc(3, 1),
            value: "fresh".to_string(),
            encoded,
            expires_at_ms: None,
        },
        0,
        &inflight,
    );
    assert_eq!(
        outcome,
        FillOutcome::Installed { had_live: false },
        "an older tombstone gives way, and it is not a live entry"
    );
    assert_eq!(engine.get(&key, 0), Some("fresh".to_string()));
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
    let (entries, weight) = engine.debug_totals();
    assert_eq!(entries, 1);
    assert_eq!(weight, 1);
}

#[test]
fn fill_supersedes_only_an_absent_or_older_entry() {
    assert!(fill_supersedes(None, hlc(1, 1)), "nothing to displace");
    assert!(
        fill_supersedes(Some(hlc(1, 1)), hlc(2, 1)),
        "an older entry gives way"
    );
    assert!(
        !fill_supersedes(Some(hlc(3, 1)), hlc(2, 1)),
        "a newer entry that landed during the load stays"
    );
    assert!(
        !fill_supersedes(Some(hlc(2, 1)), hlc(2, 1)),
        "the same version is not newer"
    );
}

#[test]
fn complete_fresh_load_keeps_a_newer_write_that_landed_during_the_load() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 22u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());

    assert_eq!(engine.get(&key, 0), None);
    let JoinOutcome::Owner(inflight) = engine.miss_or_join(&kb, hash, 0) else {
        panic!("first caller becomes the owner");
    };

    // A write lands on the same key while the load is in flight, e.g. a
    // replicated write racing the local loader, stamped after the fill.
    let _ = put(
        &engine,
        key,
        kb.clone(),
        "raced-in".into(),
        hlc(5, 2),
        None,
        0,
    );

    let encoded = Bytes::from(postcard::to_stdvec("loaded").expect("encode"));
    let outcome = engine.complete_fresh_load(
        FreshLoad {
            key: &key,
            key_bytes: &kb,
            hash,
            ver: hlc(1, 1),
            value: "loaded".to_string(),
            encoded,
            expires_at_ms: None,
        },
        0,
        &inflight,
    );
    assert_eq!(outcome, FillOutcome::Superseded);
    assert_eq!(
        engine.get(&key, 0),
        Some("raced-in".to_string()),
        "the newer write keeps its place over the older fill"
    );
    assert!(
        matches!(engine.miss_or_join(&kb, hash, 0), JoinOutcome::Hit(_)),
        "the superseded fill left no in-flight entry behind"
    );
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
    let (entries, weight) = engine.debug_totals();
    assert_eq!(entries, 1);
    assert_eq!(weight, 1);
}

#[test]
fn complete_fresh_load_keeps_a_newer_tombstone_that_landed_during_the_load() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 23u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());

    let JoinOutcome::Owner(inflight) = engine.miss_or_join(&kb, hash, 0) else {
        panic!("first caller becomes the owner");
    };
    // A removal lands while the loader still reads the old row.
    tombstone(&engine, key, kb.clone(), hlc(4, 1), 0);

    let encoded = Bytes::from(postcard::to_stdvec("stale").expect("encode"));
    let outcome = engine.complete_fresh_load(
        FreshLoad {
            key: &key,
            key_bytes: &kb,
            hash,
            ver: hlc(2, 1),
            value: "stale".to_string(),
            encoded,
            expires_at_ms: None,
        },
        0,
        &inflight,
    );
    assert_eq!(outcome, FillOutcome::Superseded);
    assert_eq!(
        engine.get(&key, 0),
        None,
        "the removal stays removed; the stale fill never resurrects the key"
    );
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
    let (entries, weight) = engine.debug_totals();
    assert_eq!(entries, 0);
    assert_eq!(weight, 0);
}

#[test]
fn complete_fresh_load_stores_nothing_after_an_invalidation_during_the_load() {
    for stamp in [hlc(2, 1), hlc(9, 1)] {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 23u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let JoinOutcome::Owner(inflight) = engine.miss_or_join(&kb, hash, 0) else {
            panic!("first caller becomes the owner");
        };
        // The key changes elsewhere while the loader still reads the old
        // row. The invalidation's version does not matter: the loader's
        // read and the change have no order the versions could show.
        assert_eq!(engine.invalidate(kb.as_ref(), hash, hlc(4, 2)), None);
        let encoded = Bytes::from(postcard::to_stdvec("stale").expect("encode"));
        let outcome = engine.complete_fresh_load(
            FreshLoad {
                key: &key,
                key_bytes: &kb,
                hash,
                ver: stamp,
                value: "stale".to_string(),
                encoded,
                expires_at_ms: None,
            },
            0,
            &inflight,
        );
        assert_eq!(outcome, FillOutcome::Invalidated, "fill stamped {stamp:?}");
        assert_eq!(engine.get(&key, 0), None);
        assert_eq!(
            inflight.value.get().map(String::as_str),
            Some("stale"),
            "joined waiters answer with the loaded value"
        );
        assert_eq!(engine.digests(), engine.recompute_digests_paired());
        assert_eq!(engine.debug_totals(), (0, 0));
        assert!(
            matches!(engine.miss_or_join(&kb, hash, 0), JoinOutcome::Owner(_)),
            "the in-flight entry is gone, so the next read loads afresh"
        );
    }
}

#[test]
fn an_invalidation_before_the_load_starts_leaves_the_next_fill_alone() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 23u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    assert_eq!(engine.invalidate(kb.as_ref(), hash, hlc(4, 2)), None);
    let JoinOutcome::Owner(inflight) = engine.miss_or_join(&kb, hash, 0) else {
        panic!("first caller becomes the owner");
    };
    let encoded = Bytes::from(postcard::to_stdvec("fresh").expect("encode"));
    let outcome = engine.complete_fresh_load(
        FreshLoad {
            key: &key,
            key_bytes: &kb,
            hash,
            ver: hlc(9, 1),
            value: "fresh".to_string(),
            encoded,
            expires_at_ms: None,
        },
        0,
        &inflight,
    );
    assert_eq!(outcome, FillOutcome::Installed { had_live: false });
    assert_eq!(engine.get(&key, 0), Some("fresh".to_string()));
}

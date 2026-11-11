//! A `load_many` dropped while it claims keys frees the loads it registered.

use super::*;
use crate::store::load::Loader;

/// A caller dropped at a later key's spilled read, with an earlier key's load
/// registered and unstarted, leaves that key free: a fresh load of it
/// completes.
#[tokio::test]
async fn a_load_many_dropped_at_a_spilled_read_frees_the_loads_it_registered() {
    let dir = temp_dir("claim-cancel");
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let shard = Shard::<u32, String>::new(
        SmolStr::new("claim-cancel"),
        Mode::Local,
        NodeId::from(1u64),
        10,
        None,
        None,
    )
    .with_weigher(|_key: &u32, value: &String| u32::try_from(value.len()).unwrap_or(u32::MAX))
    .with_loader(Loader::batch(|keys: Vec<u32>| async move {
        Ok::<_, std::io::Error>(
            keys.into_iter()
                .filter(|key| *key != 1)
                .map(|key| (key, format!("v{key}")))
                .collect::<HashMap<_, _>>(),
        )
    }))
    .with_spill(&cfg)
    .expect("tier opens");
    shard.insert(1, "x".repeat(64)).await.expect("insert");
    assert!(
        poll_until(POLL_TIMEOUT, || shard.get_sync(&1).is_none()).await,
        "the write spills the entry heavier than the cap"
    );

    let held = hold_spill_reads(&shard).await;
    // Key 2 registers its load, then key 1's spilled read waits for a permit.
    let mut claiming = Box::pin(shard.load_many([2u32, 1]));
    assert!(futures::poll!(claiming.as_mut()).is_pending());
    drop(claiming);
    drop(held);

    let loaded = tokio::time::timeout(Duration::from_secs(5), shard.load(&2))
        .await
        .expect("the load of key 2 hangs on a stale in-flight entry")
        .expect("load");
    assert_eq!(loaded, Some("v2".to_string()));
    std::fs::remove_dir_all(&dir).ok();
}

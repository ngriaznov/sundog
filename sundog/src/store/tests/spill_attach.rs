//! A shard that attaches its spill tier after it is registered: inbound batches wait for the
//! tier, and the tier is published only once its flusher runs.

use super::*;

fn spill_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "sundog-shard-spill-attach-{tag}-{}-{:?}",
        std::process::id(),
        std::thread::current().id(),
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn replicated(name: &str, capacity: u64) -> Shard<u32, String> {
    Shard::<u32, String>::new(
        SmolStr::new(name),
        Mode::Replicated,
        NodeId::from(1),
        capacity,
        None,
        None,
    )
}

fn puts(keys: std::ops::Range<u32>, value: &str, ver: Hlc) -> Vec<WireRecord> {
    keys.map(|key| WireRecord {
        key: key_bytes(&key),
        value: Some(Bytes::from(
            postcard::to_stdvec(&value.to_string()).expect("value encodes"),
        )),
        ver,
        expires_at_ms: None,
    })
    .collect()
}

#[tokio::test]
async fn a_shard_applies_inbound_batches_unless_its_open_holds_them() {
    let s = replicated("test-spill-attach-unheld", 64);
    assert!(!s.inbound_held(), "a shard starts unheld");

    ShardOps::apply_remote_batch(&s, puts(0..4, "v", hlc(10, 2))).await;

    assert_eq!(s.get(&3).await.as_deref(), Some("v"));
}

#[tokio::test]
async fn a_shard_held_for_its_spill_attach_drops_inbound_batches_until_the_tier_runs() {
    let dir = spill_dir("held");
    let cfg = spill::SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let s = replicated("test-spill-attach-held", 4);
    s.hold_inbound_until_attached();
    assert!(s.inbound_held());

    ShardOps::apply_remote_batch(&s, puts(0..16, "early", hlc(10, 2))).await;
    assert_eq!(
        s.entry_count().await,
        0,
        "a held shard applies nothing, so nothing evicts before the tier runs"
    );

    s.attach_spill(&cfg).expect("the tier attaches");
    assert!(!s.inbound_held(), "the hold ends once the tier runs");
    ShardOps::apply_remote_batch(&s, puts(0..16, "late", hlc(20, 2))).await;

    assert_eq!(
        s.entry_count().await,
        16,
        "past the capacity of 4, the batch evicts into the tier"
    );
    assert_eq!(s.get(&0).await.as_deref(), Some("late"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_published_spill_tier_already_has_its_flusher() {
    let dir = spill_dir("publish-order");
    let cfg = spill::SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let s = Arc::new(replicated("test-spill-attach-order", 4));
    let attaching = {
        let s = Arc::clone(&s);
        std::thread::spawn(move || {
            // Stretches the step between spawning the flusher and the tier
            // holding its sender, so the watcher below sees any publication
            // made inside it.
            spill::ATTACH_PAUSE.with(|pause| pause.set(Some(Duration::from_millis(300))));
            s.attach_spill(&cfg).expect("the tier attaches");
        })
    };

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let tier = loop {
        if let Some(tier) = s.engine.spill() {
            break Arc::clone(tier);
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the attach publishes the tier"
        );
        std::hint::spin_loop();
    };

    assert!(
        tier.has_flusher(),
        "an eviction that finds the published tier finds a flusher to take its job"
    );
    attaching.join().expect("the attach does not panic");
    let _ = std::fs::remove_dir_all(&dir);
}

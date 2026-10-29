//! A spilled entry whose record a read cannot use comes back from a peer that holds the key.

use super::*;

#[tokio::test]
async fn a_spilled_entry_whose_record_fails_its_checksum_returns_from_a_peer() {
    let dir = std::env::temp_dir().join(format!(
        "sundog-cache-spill-unreadable-{}-{}",
        std::process::id(),
        now_ms()
    ));
    let a = Cluster::builder("cache-it-spill-unreadable")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("a builds");
    let b = Cluster::builder("cache-it-spill-unreadable")
        .seeds([a.local_gossip_addr()])
        .config(loopback_config())
        .build()
        .await
        .expect("b builds");
    wait_for_peer_count(&a, 1).await;
    wait_for_peer_count(&b, 1).await;
    let cache_a = a
        .cache::<u32, String>("sessions")
        .mode(Mode::Replicated)
        .max_capacity(1)
        .spill(SpillConfig::new(&dir, 1 << 20).region_bytes(4096))
        .open()
        .await
        .expect("a opens");
    let cache_b = b
        .cache::<u32, String>("sessions")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("b opens");

    let value = |key: u32| format!("the value written for key {key}");
    for key in [1u32, 2] {
        cache_a.insert(key, value(key)).await.expect("insert");
    }
    wait_until(Duration::from_secs(10), "b holds both keys", async || {
        cache_b.get(&1).await.is_some() && cache_b.get(&2).await.is_some()
    })
    .await;
    // A one-entry cap spills one key; `get_sync` never reads the disk.
    let mut spilled = None;
    wait_until(Duration::from_secs(10), "a spills one key", async || {
        spilled = [1u32, 2]
            .into_iter()
            .find(|key| cache_a.get_sync(key).is_none());
        spilled.is_some()
    })
    .await;
    let key = spilled.expect("a spilled one key");

    // One byte of the spilled value flipped in its region file: the
    // record still frames, and fails its checksum.
    let needle = value(key).into_bytes();
    let mut flipped = 0;
    let mut stack = vec![dir.clone()];
    while let Some(path) = stack.pop() {
        for entry in std::fs::read_dir(&path).expect("read dir").flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
                continue;
            }
            let mut bytes = std::fs::read(&p).expect("read file");
            if let Some(at) = bytes
                .windows(needle.len())
                .position(|w| w == needle.as_slice())
            {
                bytes[at] ^= 0x01;
                std::fs::write(&p, &bytes).expect("write file");
                flipped += 1;
            }
        }
    }
    assert_eq!(
        flipped, 1,
        "exactly one region file holds the spilled value"
    );

    assert_eq!(cache_a.get(&key).await, None, "a's own copy cannot be read");
    // The drop takes the key out of a's digests, so anti-entropy pulls b's
    // copy back where it once compared equal and never repaired.
    wait_until(
        Duration::from_secs(10),
        "b's copy returns to a",
        async || cache_a.get(&key).await == Some(value(key)),
    )
    .await;

    cache_a.close().await;
    b.shutdown().await;
    a.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

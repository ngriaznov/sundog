//! The `prometheus` feature installs a real Prometheus recorder, either
//! serving `GET /metrics` itself
//! ([`sundog::ClusterBuilder::prometheus_listen`]) or handing back a
//! [`sundog::PrometheusHandle`] for a caller's own HTTP stack
//! ([`sundog::prometheus_handle`]).
//! `metrics::set_global_recorder` is a single process-global slot: whichever
//! test in this binary installs a recorder first wins it, so the second
//! test below tolerates losing that race instead of assuming it always
//! runs first.

#![cfg(all(feature = "prometheus", not(feature = "sim")))]

mod common;

use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use sundog::{Cluster, Mode};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Reserves a loopback TCP port the way `cluster.rs`'s own
/// `reserve_data_bind_addr` does: probe-bind, read back, then drop it.
async fn reserve_tcp_addr() -> SocketAddr {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind an ephemeral loopback tcp port to reserve a metrics address");
    listener
        .local_addr()
        .expect("a freshly bound tcp listener reports a local address")
}

/// A minimal raw-socket `GET /metrics`. Returns `None` if the listener
/// is not accepting connections yet.
async fn scrape_metrics(addr: SocketAddr) -> Option<String> {
    let mut stream = TcpStream::connect(addr).await.ok()?;
    stream
        .write_all(
            format!("GET /metrics HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .ok()?;
    let mut body = Vec::new();
    stream.read_to_end(&mut body).await.ok()?;
    Some(String::from_utf8_lossy(&body).into_owned())
}

/// A minimal raw-socket `GET` against `path`, returning the status line.
/// `None` if the listener is not accepting connections yet.
async fn scrape_status(addr: SocketAddr, path: &str) -> Option<String> {
    let mut stream = TcpStream::connect(addr).await.ok()?;
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .await
        .ok()?;
    let mut body = Vec::new();
    stream.read_to_end(&mut body).await.ok()?;
    let response = String::from_utf8_lossy(&body).into_owned();
    Some(response.lines().next()?.to_string())
}

/// Finds `metric{label1="value1",label2="value2",...} <number>` in
/// Prometheus text-exposition `body`, tolerant of label ordering and
/// integer-vs-float rendering. Every pair in `labels` must match the same
/// line. A single label, `&[("cache", "x")]`, is ambiguous once more than
/// one series shares that label's value under a different label, such as
/// `sundog_spill_reads_total`'s `outcome` varying per `cache`.
fn scraped_metric_value(body: &str, metric: &str, labels: &[(&str, &str)]) -> Option<f64> {
    let wanted: Vec<String> = labels
        .iter()
        .map(|&(k, v)| format!("{k}=\"{v}\""))
        .collect();
    body.lines().find_map(|line| {
        let rest = line.strip_prefix(metric)?;
        let rest = rest.strip_prefix('{')?;
        let (line_labels, value) = rest.split_once('}')?;
        let line_labels: Vec<&str> = line_labels.split(',').collect();
        if !wanted
            .iter()
            .all(|w| line_labels.iter().any(|&pair| pair == w))
        {
            return None;
        }
        value.trim().parse::<f64>().ok()
    })
}

/// Every bucket of the [`SKETCH_FILL`]-key fill holds about 20 entries, so
/// with this threshold a mismatch there reconciles through an IBLT sketch.
/// Comfortably under [`PART_MIN_BUCKET`], so this cache's buckets never take
/// the part path.
const SKETCH_MIN_BUCKET: usize = 8;
const SKETCH_FILL: u32 = 20_000;

/// Bucket size past which a mismatch answers with part digests instead of a
/// listing or sketch. Between [`SKETCH_FILL`]'s ~20-entry buckets (which
/// must stay on the bucket-level sketch path) and [`DENSE_BUCKET_COUNT`]
/// (which must exceed it).
const PART_MIN_BUCKET: usize = 100;
/// Keys concentrated into one bucket via [`keys_in_one_bucket`], dense
/// enough to clear [`PART_MIN_BUCKET`] while each of its 64 parts (~3
/// entries apiece) stays well under [`SKETCH_MIN_BUCKET`], so a part
/// mismatch there answers with a listing.
const DENSE_BUCKET_COUNT: usize = 200;

fn node_config(gossip_bind_addr: SocketAddr) -> sundog::ClusterConfig {
    common::fast_config().with(|c| {
        c.gossip_bind_addr = gossip_bind_addr;
        c.ae_sketch_min_bucket = SKETCH_MIN_BUCKET;
        c.ae_part_min_bucket = PART_MIN_BUCKET;
    })
}

/// The anti-entropy bucket a `u32` key hashes into, mirroring
/// `store::stripe_index_from_hash`'s formula; `cluster.rs`'s and
/// `tests/sim.rs`'s own tests carry the identical helper for the same
/// reason.
fn bucket_of(key: u32) -> u16 {
    let bytes = postcard::to_stdvec(&key).expect("u32 key encodes");
    let bucket = xxhash_rust::xxh3::xxh3_64(&bytes) & (sundog::store::BUCKET_COUNT as u64 - 1);
    u16::try_from(bucket).expect("invariant: masked to BUCKET_COUNT - 1, always fits in u16")
}

/// `count` keys guaranteed to land in the same anti-entropy bucket, dense
/// enough to force the part path at [`PART_MIN_BUCKET`] without a
/// uniform-fill key count in the millions.
fn keys_in_one_bucket(count: usize) -> Vec<u32> {
    let target = bucket_of(0);
    (0..)
        .filter(|&k| bucket_of(k) == target)
        .take(count)
        .collect()
}

/// A known hit/miss sequence on a `Mode::Local` cache of its own, exact
/// rather than shared with `users`' traffic: 2 inserts, 3 hit gets, 2 miss
/// gets, one filling `get_or_load`, one hit `get_or_load`, two
/// `contains_key` checks, one `get_or_insert_with` miss and hit, and four
/// concurrent `get_or_load`s of one key: hits=3+1+1+3=8, misses=2+1+1+1=5.
async fn count_hits_and_misses(cluster: &Cluster) {
    let counted = cluster
        .cache::<u32, String>("counted")
        .mode(Mode::Local)
        .open()
        .await
        .expect("cache opens");
    counted.insert(1, "a".into()).await.expect("insert");
    counted.insert(2, "b".into()).await.expect("insert");
    assert_eq!(counted.get(&1).await, Some("a".to_string()));
    assert_eq!(counted.get(&2).await, Some("b".to_string()));
    assert_eq!(counted.get(&1).await, Some("a".to_string()));
    assert_eq!(counted.get(&3).await, None);
    assert_eq!(counted.get(&4).await, None);
    let filled = counted
        .get_or_load(&5, async |_key| {
            Ok::<_, std::convert::Infallible>("loaded".to_string())
        })
        .await
        .expect("loader succeeds");
    assert_eq!(filled, "loaded");
    let cached = counted
        .get_or_load(&5, async |_key| {
            Ok::<_, std::convert::Infallible>("loaded".to_string())
        })
        .await
        .expect("loader succeeds");
    assert_eq!(cached, "loaded");

    // An existence check moves neither counter.
    assert!(counted.contains_key(&1).await);
    assert!(!counted.contains_key(&9).await);
    // get_or_insert_with: one miss to fill, one hit to read back.
    let made = counted
        .get_or_insert_with(&6, async |_key| "made".to_string())
        .await
        .expect("make succeeds");
    assert_eq!(made, "made");
    let kept = counted
        .get_or_insert_with(&6, async |_key| "unused".to_string())
        .await
        .expect("make succeeds");
    assert_eq!(kept, "made");
    // Four concurrent loads of one key collapse into one loader run.
    let loads = futures::future::join_all((0..4).map(|_| {
        let counted = counted.clone();
        async move {
            counted
                .get_or_load(&7, async |_key| {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    Ok::<_, std::convert::Infallible>("joined".to_string())
                })
                .await
                .expect("loader succeeds")
        }
    }))
    .await;
    assert!(loads.iter().all(|value| value == "joined"));
}

/// Opens `users` on both `cluster` and `peer`, does one plain insert/remove
/// pair, then a sketch-scale fill with one key dropped on the peer, so the
/// next round finds one bucket mismatched at ~20 entries: past
/// `SKETCH_MIN_BUCKET`, so it reconciles through a decoded IBLT sketch.
async fn seed_sketch_mismatch(cluster: &Cluster, peer: &Cluster) {
    let cache = cluster
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("cache opens");
    let peer_users = peer
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("peer cache opens");
    cache.insert(1, "hello".into()).await.expect("insert");
    cache.remove(&1).await.expect("remove");

    cache
        .insert_many((100..SKETCH_FILL + 100).map(|k| (k, k.to_string())))
        .await
        .expect("bulk insert");
    common::eventually(Duration::from_secs(15), || async {
        peer_users.get(&(SKETCH_FILL + 99)).await.is_some()
    })
    .await;
    peer_users.invalidate_local(&150).await;
}

/// Opens `parts` on both `cluster` and `peer`, densely fills one anti-entropy
/// bucket past `PART_MIN_BUCKET`, then drops one key on the peer: the bucket
/// mismatch answers with part digests, and each mismatched part, far under
/// `SKETCH_MIN_BUCKET`, reconciles through a listing.
async fn seed_part_mismatch(cluster: &Cluster, peer: &Cluster) {
    let parts_cache = cluster
        .cache::<u32, String>("parts")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("parts cache opens");
    let peer_parts = peer
        .cache::<u32, String>("parts")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("peer parts cache opens");
    let dense_keys = keys_in_one_bucket(DENSE_BUCKET_COUNT);
    parts_cache
        .insert_many(dense_keys.iter().map(|&k| (k, k.to_string())))
        .await
        .expect("dense bulk insert");
    let last_dense_key = *dense_keys.last().expect("DENSE_BUCKET_COUNT is nonzero");
    common::eventually(Duration::from_secs(15), || async {
        peer_parts.get(&last_dense_key).await.is_some()
    })
    .await;
    peer_parts
        .invalidate_local(dense_keys.first().expect("DENSE_BUCKET_COUNT is nonzero"))
        .await;
}

#[allow(
    clippy::too_many_lines,
    reason = "folds in the spill metrics pin behind feature = \"spill\"; see \
              spill_writes_and_promotes_pin_metrics's own doc for why it can't be a separate \
              #[tokio::test]"
)]
#[tokio::test]
async fn metrics_endpoint_serves_sundog_metrics_after_cache_ops() {
    let metrics_addr = reserve_tcp_addr().await;
    let gossip_a = common::reserve_gossip_addr().await;
    let gossip_b = common::reserve_gossip_addr().await;

    let cluster = Cluster::builder("it-prometheus-exporter")
        .seeds([gossip_b])
        .config(node_config(gossip_a))
        .prometheus_listen(metrics_addr)
        .build()
        .await
        .expect("cluster builds with a prometheus listener");
    // A peer for `users` to replicate to and reconcile against; only the
    // first node serves metrics, since the recorder is process-global.
    let peer = Cluster::builder("it-prometheus-exporter")
        .seeds([gossip_a])
        .config(node_config(gossip_b))
        .build()
        .await
        .expect("peer builds");
    common::wait_for_peer_count(&cluster, 1, Duration::from_secs(15)).await;
    common::wait_for_peer_count(&peer, 1, Duration::from_secs(15)).await;

    seed_sketch_mismatch(&cluster, &peer).await;
    seed_part_mismatch(&cluster, &peer).await;
    count_hits_and_misses(&cluster).await;
    #[cfg(feature = "spill")]
    let spill_dirs = spill_writes_and_promotes_pin_metrics(&cluster).await;

    // `sundog_open_caches` comes from a periodic background routine and the
    // sketch/parts counters from an anti-entropy round, so poll until every
    // metric checked below has been published once.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let body = loop {
        if let Some(body) = scrape_metrics(metrics_addr).await
            && body.contains("sundog_open_caches")
            && body.contains("sundog_live_peers")
            && scraped_metric_value(&body, "sundog_ae_parts_total", &[("outcome", "listing")])
                .is_some()
            && body.contains("sundog_cache_entries")
            && scraped_metric_value(&body, "sundog_ae_sketch_total", &[("outcome", "decoded")])
                .is_some()
            && (cfg!(not(feature = "spill"))
                || scraped_metric_value(
                    &body,
                    "sundog_spill_writes_total",
                    &[("cache", "spilled")],
                )
                .is_some())
        {
            break body;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "metrics endpoint never served sundog_open_caches, sundog_live_peers, \
             sundog_cache_entries, a decoded sundog_ae_sketch_total, and (feature = \"spill\") \
             sundog_spill_writes_total within the bound"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    assert_eq!(
        scraped_metric_value(&body, "sundog_cache_hits_total", &[("cache", "counted")]),
        Some(8.0),
        "expected 8 hits on the 'counted' cache; got body:\n{body}"
    );
    assert_eq!(
        scraped_metric_value(&body, "sundog_cache_misses_total", &[("cache", "counted")]),
        Some(5.0),
        "expected 5 misses on the 'counted' cache; got body:\n{body}"
    );
    assert!(
        scraped_metric_value(&body, "sundog_cache_entries", &[("cache", "counted")]).is_some(),
        "expected a sundog_cache_entries line for the 'counted' cache; got body:\n{body}"
    );
    assert!(
        scraped_metric_value(&body, "sundog_ae_sketch_total", &[("cache", "users")])
            .is_some_and(|decoded| decoded >= 1.0),
        "expected at least one decoded sketch on the 'users' cache; got body:\n{body}"
    );
    assert!(
        scraped_metric_value(&body, "sundog_ae_parts_total", &[("cache", "parts")])
            .is_some_and(|listings| listings >= 1.0),
        "expected at least one part listing on the 'parts' cache; got body:\n{body}"
    );

    #[cfg(feature = "spill")]
    {
        assert_eq!(
            scraped_metric_value(&body, "sundog_spill_writes_total", &[("cache", "spilled")]),
            Some(1.0),
            "expected exactly one spill install; got body:\n{body}"
        );
        assert_eq!(
            scraped_metric_value(
                &body,
                "sundog_spill_reads_total",
                &[("cache", "spilled"), ("outcome", "hit")]
            ),
            Some(1.0),
            "expected exactly one disk hit; got body:\n{body}"
        );
        assert_eq!(
            scraped_metric_value(
                &body,
                "sundog_spill_promotions_total",
                &[("cache", "spilled")]
            ),
            Some(1.0),
            "expected exactly one promotion; got body:\n{body}"
        );
        assert_eq!(
            scraped_metric_value(&body, "sundog_spill_entries", &[("cache", "spilled")]),
            Some(0.0),
            "the promoted key is resident again, so zero currently-spilled entries remain; \
             got body:\n{body}"
        );

        // An overwrite of a spilled key decrements sundog_spill_entries the
        // same way a promotion does, via apply_put rather than a disk read.
        assert_eq!(
            scraped_metric_value(
                &body,
                "sundog_spill_writes_total",
                &[("cache", "spill-overwrite")]
            ),
            Some(1.0),
            "the overwrite itself never spills anything new; got body:\n{body}"
        );
        assert_eq!(
            scraped_metric_value(
                &body,
                "sundog_spill_entries",
                &[("cache", "spill-overwrite")]
            ),
            Some(0.0),
            "the overwritten key is resident again, so zero currently-spilled entries remain; \
             got body:\n{body}"
        );

        // A remove of a spilled key decrements sundog_spill_entries via
        // apply_tombstone.
        assert_eq!(
            scraped_metric_value(
                &body,
                "sundog_spill_writes_total",
                &[("cache", "spill-remove")]
            ),
            Some(1.0),
            "the remove itself never spills anything new; got body:\n{body}"
        );
        assert_eq!(
            scraped_metric_value(&body, "sundog_spill_entries", &[("cache", "spill-remove")]),
            Some(0.0),
            "the removed key is gone, so zero currently-spilled entries remain; got body:\n{body}"
        );
    }

    // `users` warmed during `seed_sketch_mismatch` above: `is_ready()` and
    // `/readyz` on the same listener must both already agree.
    assert!(cluster.is_ready(), "the open Replicated caches are warm");
    let readyz_status = scrape_status(metrics_addr, "/readyz")
        .await
        .expect("readyz answers once the cluster is up");
    assert!(
        readyz_status.contains("200"),
        "expected /readyz to answer 200 once warm; got {readyz_status}"
    );
    let healthz_status = scrape_status(metrics_addr, "/healthz")
        .await
        .expect("healthz answers once the cluster is up");
    assert!(
        healthz_status.contains("200"),
        "expected /healthz to always answer 200 while the process serves; got {healthz_status}"
    );

    peer.shutdown().await;
    cluster.shutdown().await;
    #[cfg(feature = "spill")]
    for dir in &spill_dirs {
        let _ = std::fs::remove_dir_all(dir);
    }
}

/// [`sundog::prometheus_handle`], the no-listener install, for a caller that
/// serves `/metrics` from its own HTTP stack. Whichever test in this binary
/// installs the process-global recorder first wins it: if the recorder
/// above already claimed the slot, `prometheus_handle` still runs (and this
/// test still exercises it), it just cannot hand back a usable handle, so
/// the render-based assertion below only applies when this test wins the
/// race.
#[tokio::test]
async fn prometheus_handle_exposes_cache_hits_without_a_listener() {
    let cluster = Cluster::builder("it-prometheus-handle")
        .seeds(std::iter::empty())
        .config(common::fast_config())
        .build()
        .await
        .expect("cluster builds");

    let cache = cluster
        .cache::<u32, String>("handle-metrics")
        .mode(Mode::Local)
        .open()
        .await
        .expect("cache opens");
    cache.insert(1, "a".into()).await.expect("insert");
    assert_eq!(cache.get(&1).await, Some("a".to_string()));

    if let Ok(handle) = sundog::prometheus_handle() {
        let body = handle.render();
        assert!(
            body.contains("sundog_cache_hits_total"),
            "prometheus_handle's own recorder captures cache hits; got body:\n{body}"
        );
    }
    // Else: another test in this binary installed the process-global
    // recorder first; there is no handle to render from here.

    cluster.shutdown().await;
}

/// A directory path under the OS temp dir, unique to this test process,
/// call, and `label`. Never created on disk; [`sundog::SpillConfig::new`]'s
/// `SpillTier::open` creates it.
#[cfg(feature = "spill")]
fn fresh_spill_dir(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "sundog-it-prometheus-spill-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is after the unix epoch")
            .as_nanos()
    ))
}

/// The spill metrics pin: a tiny `max_capacity` plus a `spill` tier on
/// `cluster` forces one eviction-to-spill, and a single `get` of the
/// spilled key forces one disk hit and one promotion. Two further,
/// isolated caches then pin an overwrite-after-spill and a
/// remove-after-spill, each sized so the one operation under test settles
/// without triggering a second eviction of its own. Batch eviction can
/// otherwise clear more than one unit of weight per pass, which would make
/// an exact count depend on internal batching details rather than on the
/// behavior under test.
///
/// Runs inside `metrics_endpoint_serves_sundog_metrics_after_cache_ops`
/// rather than as its own `#[tokio::test]`, for the same reason
/// `seed_sketch_mismatch`/`seed_part_mismatch`/`count_hits_and_misses`
/// above do: a cache's `hits`/`misses`-style `metrics::Counter` handles
/// bind to whichever recorder is installed at the moment
/// `Shard::new`/`Shard::attach_spill` calls `metrics::counter!`, not
/// whatever gets installed later. Only the one test in this binary that
/// reliably owns the process-global recorder from the start, via
/// `prometheus_listen` synchronously early in `Cluster::builder(..).build()`,
/// can pin exact metric values. A second, independent `#[tokio::test]`
/// racing for the same slot either is a no-op, if it loses, or breaks the
/// first test's own `build()`, if it wins.
///
/// Returns every tier's scratch directory for the caller to clean up.
#[allow(
    clippy::too_many_lines,
    reason = "three isolated cache scenarios (promote, overwrite, remove), each with its own \
              setup and bounded wait, read better inline than split across helpers that would \
              each retake the same handful of parameters"
)]
#[cfg(feature = "spill")]
async fn spill_writes_and_promotes_pin_metrics(cluster: &Cluster) -> Vec<std::path::PathBuf> {
    let mut dirs = Vec::new();

    // --- "spilled": one eviction-to-spill, one disk-read promotion. ---
    let dir = fresh_spill_dir("promote");
    let cfg = sundog::SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let cache = cluster
        .cache::<u32, String>("spilled")
        .mode(Mode::Local)
        .max_capacity(1)
        .spill(cfg)
        .open()
        .await
        .expect("cache opens");

    cache.insert(1, "one".to_string()).await.expect("insert 1");
    cache.insert(2, "two".to_string()).await.expect("insert 2");
    common::eventually(Duration::from_secs(5), || async {
        cache.get_sync(&1).is_none() || cache.get_sync(&2).is_none()
    })
    .await;
    let spilled_key = if cache.get_sync(&1).is_none() {
        1u32
    } else {
        2u32
    };

    // One promotion: a single disk read of the spilled key.
    let _ = cache.get(&spilled_key).await;
    dirs.push(dir);

    // --- "spill-overwrite": an overwrite of a currently-spilled key must
    // decrement sundog_spill_entries with no further disk write. `1`/`2`/`3`
    // fill a `max_capacity(2)` cache past its limit, spilling one of them;
    // removing one of the two others first frees the one unit of headroom
    // the overwrite below needs, so it settles at the cap instead of
    // forcing a second eviction.
    let dir = fresh_spill_dir("overwrite");
    let cfg = sundog::SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let cache = cluster
        .cache::<u32, String>("spill-overwrite")
        .mode(Mode::Local)
        .max_capacity(2)
        .spill(cfg)
        .open()
        .await
        .expect("cache opens");

    cache.insert(1, "one".to_string()).await.expect("insert 1");
    cache.insert(2, "two".to_string()).await.expect("insert 2");
    cache
        .insert(3, "three".to_string())
        .await
        .expect("insert 3");
    common::eventually(Duration::from_secs(5), || async {
        [1u32, 2, 3]
            .into_iter()
            .any(|k| cache.get_sync(&k).is_none())
    })
    .await;
    let spilled_key = [1u32, 2, 3]
        .into_iter()
        .find(|&k| cache.get_sync(&k).is_none())
        .expect("exactly one of the three keys spilled");
    let other_resident_key = [1u32, 2, 3]
        .into_iter()
        .find(|&k| k != spilled_key)
        .expect("the other two keys stay resident");
    cache
        .remove(&other_resident_key)
        .await
        .expect("free headroom for the overwrite below");
    cache
        .insert(spilled_key, "overwritten".to_string())
        .await
        .expect("overwrite the spilled key");
    dirs.push(dir);

    // --- "spill-remove": removing a currently-spilled key must decrement
    // sundog_spill_entries too, via apply_tombstone rather than apply_put.
    let dir = fresh_spill_dir("remove");
    let cfg = sundog::SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let cache = cluster
        .cache::<u32, String>("spill-remove")
        .mode(Mode::Local)
        .max_capacity(1)
        .spill(cfg)
        .open()
        .await
        .expect("cache opens");

    cache
        .insert(10, "ten".to_string())
        .await
        .expect("insert 10");
    cache
        .insert(11, "eleven".to_string())
        .await
        .expect("insert 11");
    common::eventually(Duration::from_secs(5), || async {
        cache.get_sync(&10).is_none() || cache.get_sync(&11).is_none()
    })
    .await;
    let spilled_key = if cache.get_sync(&10).is_none() {
        10u32
    } else {
        11u32
    };
    cache
        .remove(&spilled_key)
        .await
        .expect("remove the spilled key");
    dirs.push(dir);

    dirs
}

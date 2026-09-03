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

/// Finds `metric{label="value"} <number>` in Prometheus text-exposition
/// `body`, tolerant of label ordering and integer-vs-float rendering.
fn scraped_metric_value(body: &str, metric: &str, label: (&str, &str)) -> Option<f64> {
    let wanted = format!("{}=\"{}\"", label.0, label.1);
    body.lines().find_map(|line| {
        let rest = line.strip_prefix(metric)?;
        let rest = rest.strip_prefix('{')?;
        let (labels, value) = rest.split_once('}')?;
        if !labels.split(',').any(|pair| pair == wanted) {
            return None;
        }
        value.trim().parse::<f64>().ok()
    })
}

/// Every bucket of the [`SKETCH_FILL`]-key fill holds about 20 entries, so
/// with this threshold a mismatch there reconciles through an IBLT sketch.
const SKETCH_MIN_BUCKET: usize = 8;
const SKETCH_FILL: u32 = 20_000;

fn node_config(gossip_bind_addr: SocketAddr) -> sundog::ClusterConfig {
    common::fast_config().with(|c| {
        c.gossip_bind_addr = gossip_bind_addr;
        c.ae_sketch_min_bucket = SKETCH_MIN_BUCKET;
    })
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

    // A sketch-scale fill, replicated to the peer, then one key dropped
    // there: the next anti-entropy round finds that bucket mismatched, and
    // at ~20 entries it reconciles through a sketch, which decodes.
    cache
        .insert_many((100..SKETCH_FILL + 100).map(|k| (k, k.to_string())))
        .await
        .expect("bulk insert");
    common::eventually(Duration::from_secs(15), || async {
        peer_users.get(&(SKETCH_FILL + 99)).await.is_some()
    })
    .await;
    peer_users.invalidate_local(&150).await;

    count_hits_and_misses(&cluster).await;

    // `sundog_open_caches` comes from a periodic background routine and the
    // sketch counter from an anti-entropy round, so poll until every metric
    // checked below has been published once.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let body = loop {
        if let Some(body) = scrape_metrics(metrics_addr).await
            && body.contains("sundog_open_caches")
            && body.contains("sundog_live_peers")
            && body.contains("sundog_cache_entries")
            && scraped_metric_value(&body, "sundog_ae_sketch_total", ("outcome", "decoded"))
                .is_some()
        {
            break body;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "metrics endpoint never served sundog_open_caches, sundog_live_peers, \
             sundog_cache_entries, and a decoded sundog_ae_sketch_total within the bound"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    assert_eq!(
        scraped_metric_value(&body, "sundog_cache_hits_total", ("cache", "counted")),
        Some(8.0),
        "expected 8 hits on the 'counted' cache; got body:\n{body}"
    );
    assert_eq!(
        scraped_metric_value(&body, "sundog_cache_misses_total", ("cache", "counted")),
        Some(5.0),
        "expected 5 misses on the 'counted' cache; got body:\n{body}"
    );
    assert!(
        scraped_metric_value(&body, "sundog_cache_entries", ("cache", "counted")).is_some(),
        "expected a sundog_cache_entries line for the 'counted' cache; got body:\n{body}"
    );
    assert!(
        scraped_metric_value(&body, "sundog_ae_sketch_total", ("cache", "users"))
            .is_some_and(|decoded| decoded >= 1.0),
        "expected at least one decoded sketch on the 'users' cache; got body:\n{body}"
    );

    peer.shutdown().await;
    cluster.shutdown().await;
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

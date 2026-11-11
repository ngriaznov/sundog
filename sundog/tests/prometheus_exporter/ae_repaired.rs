//! `sundog_ae_repaired_total` counts what landed: a record a node refuses for
//! its stamp is not a repair, however many rounds offer it.

use crate::support::{node_config, reserve_tcp_addr, scrape_metrics, scraped_metric_value};
use smol_str::SmolStr;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;
use sundog::wire::{self, Msg, WireRecord};
use sundog::{Cluster, Mode, NodeId};

const CACHE: &str = "ae-repaired-pin";

/// Two nodes share `ae-repaired-pin`; the offering node holds two records a
/// raw writer handed it, which only anti-entropy carries to the other: one
/// stamped now, and one stamped an hour ahead. The pulling node refuses
/// stamps more than a minute ahead and its rounds run every 150 ms, the
/// offering node's once an hour. The record that landed counts, and the
/// counter then stays where it is while `sundog_clock_skew_rejected_total`
/// proves further rounds offered the refused one.
pub(crate) async fn ae_repaired_pin_metrics(metrics_addr: SocketAddr) {
    let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    let offering_data = reserve_tcp_addr().await;
    let offering_config = node_config(loopback).with(|c| {
        c.data_bind_addr = offering_data;
        c.max_clock_skew = None;
        c.ae_interval = Duration::from_hours(1);
        c.tombstone_ttl = Duration::from_hours(4);
    });
    let offering = Cluster::builder("it-ae-repaired")
        .seeds(std::iter::empty())
        .config(offering_config)
        .build()
        .await
        .expect("the offering node builds");
    let _offering_cache = offering
        .cache::<u32, String>(CACHE)
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("the offering node opens the cache");
    let pulling_config =
        node_config(loopback).with(|c| c.max_clock_skew = Some(Duration::from_mins(1)));
    let pulling = Cluster::builder("it-ae-repaired")
        .seeds([offering.local_gossip_addr()])
        .config(pulling_config)
        .build()
        .await
        .expect("the pulling node builds");
    crate::common::wait_for_peer_count(&offering, 1, Duration::from_secs(15)).await;
    crate::common::wait_for_peer_count(&pulling, 1, Duration::from_secs(15)).await;
    let pulling_cache = pulling
        .cache::<u32, String>(CACHE)
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("the pulling node opens the cache");

    send_records(offering_data).await;

    crate::common::eventually(Duration::from_secs(30), || async {
        pulling_cache.get(&1).await.is_some()
    })
    .await;
    let rejected = async || {
        let body = scrape_metrics(metrics_addr).await.unwrap_or_default();
        scraped_metric_value(
            &body,
            "sundog_clock_skew_rejected_total",
            &[("cache", CACHE)],
        )
        .unwrap_or(0.0)
    };
    crate::common::eventually(Duration::from_secs(30), || async {
        rejected().await >= 2.0
    })
    .await;

    let repaired = async || {
        let body = scrape_metrics(metrics_addr).await.unwrap_or_default();
        scraped_metric_value(&body, "sundog_ae_repaired_total", &[("cache", CACHE)])
    };
    let settled = repaired().await;
    assert!(
        settled.is_some_and(|landed| landed >= 1.0),
        "the record that landed counts: {settled:?}"
    );
    // Three more rounds offer the refused record and refuse it again.
    let refused_so_far = rejected().await;
    crate::common::eventually(Duration::from_secs(30), || async {
        rejected().await >= refused_so_far + 3.0
    })
    .await;
    assert_eq!(
        repaired().await,
        settled,
        "a record the pulling node refuses is no repair, in any round"
    );
    assert_eq!(
        pulling_cache.get(&2).await,
        None,
        "the refused record is not stored"
    );
    pulling.shutdown().await;
    offering.shutdown().await;
}

/// Hands the node at `data_addr`, as a writer in no member's view, a record
/// for key 1 stamped now and one for key 2 stamped an hour ahead.
async fn send_records(data_addr: SocketAddr) {
    use tokio::io::AsyncWriteExt as _;

    let writer = NodeId::from(0x5eed_0003);
    let now_ms = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the clock is past the epoch")
            .as_millis(),
    )
    .expect("epoch milliseconds fit in u64");
    let record = |key: u32, ahead_ms: u64| WireRecord {
        key: bytes::Bytes::from(postcard::to_stdvec(&key).expect("key encodes")),
        value: Some(bytes::Bytes::from(
            postcard::to_stdvec(&"v".to_string()).expect("value encodes"),
        )),
        ver: sundog::Hlc {
            wall_ms: now_ms + ahead_ms,
            logical: 0,
            node: writer,
        },
        expires_at_ms: None,
    };
    let mut stream = tokio::net::TcpStream::connect(data_addr)
        .await
        .expect("the node's data port accepts");
    let messages = [
        Msg::Hello {
            node: writer,
            incarnation: 1,
            protocol: wire::PROTOCOL_VERSION,
            cluster: wire::UNNAMED_CLUSTER,
        },
        Msg::Replicate {
            cache: SmolStr::new(CACHE),
            rec: record(1, 0),
        },
        Msg::Replicate {
            cache: SmolStr::new(CACHE),
            rec: record(2, 3_600_000),
        },
    ];
    for message in &messages {
        let frame = wire::encode(message).expect("encodes");
        let len = u32::try_from(frame.len()).expect("a frame fits u32");
        stream
            .write_all(&len.to_be_bytes())
            .await
            .expect("length sent");
        stream.write_all(&frame).await.expect("frame sent");
    }
    stream.flush().await.expect("flushed");
}

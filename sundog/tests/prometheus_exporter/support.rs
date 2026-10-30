//! Loopback scrape helpers and the anti-entropy node config the exporter tests share.

use crate::common;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// The exporter files and `tests/tls.rs` reserve a port through one helper.
pub(crate) use crate::common::reserve_tcp_addr;

/// A minimal raw-socket `GET /metrics`. Returns `None` if the listener
/// is not accepting connections yet.
pub(crate) async fn scrape_metrics(addr: SocketAddr) -> Option<String> {
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
pub(crate) async fn scrape_status(addr: SocketAddr, path: &str) -> Option<String> {
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

/// A minimal raw-socket request with `method` against `path`, returning the
/// whole response, headers and body. `None` if the listener is not accepting
/// connections yet.
pub(crate) async fn raw_request(addr: SocketAddr, method: &str, path: &str) -> Option<String> {
    let mut stream = TcpStream::connect(addr).await.ok()?;
    stream
        .write_all(
            format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .ok()?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.ok()?;
    Some(String::from_utf8_lossy(&response).into_owned())
}

/// Finds `metric{label1="value1",label2="value2",...} <number>` in
/// Prometheus text-exposition `body`, tolerant of label ordering and
/// integer-vs-float rendering. Every pair in `labels` must match the same
/// line. A single label, `&[("cache", "x")]`, is ambiguous once more than
/// one series shares that label's value under a different label, such as
/// `sundog_spill_reads_total`'s `outcome` varying per `cache`.
pub(crate) fn scraped_metric_value(
    body: &str,
    metric: &str,
    labels: &[(&str, &str)],
) -> Option<f64> {
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

/// The value of the unlabelled series `metric` in a scraped `body`.
pub(crate) fn scraped_unlabelled_value(body: &str, metric: &str) -> Option<f64> {
    body.lines().find_map(|line| {
        line.strip_prefix(metric)?
            .strip_prefix(' ')?
            .trim()
            .parse::<f64>()
            .ok()
    })
}

/// Every bucket of the [`SKETCH_FILL`]-key fill holds about 20 entries, so
/// with this threshold a mismatch there reconciles through an IBLT sketch.
/// Comfortably under [`PART_MIN_BUCKET`], so this cache's buckets never take
/// the part path.
const SKETCH_MIN_BUCKET: usize = 8;
pub(crate) const SKETCH_FILL: u32 = 20_000;

/// Bucket size past which a mismatch answers with part digests instead of a
/// listing or sketch. Between [`SKETCH_FILL`]'s ~20-entry buckets (which
/// must stay on the bucket-level sketch path) and [`DENSE_BUCKET_COUNT`]
/// (which must exceed it).
const PART_MIN_BUCKET: usize = 100;
/// Keys concentrated into one bucket via [`keys_in_one_bucket`], dense
/// enough to clear [`PART_MIN_BUCKET`] while each of its 64 parts (~3
/// entries apiece) stays well under [`SKETCH_MIN_BUCKET`], so a part
/// mismatch there answers with a listing.
pub(crate) const DENSE_BUCKET_COUNT: usize = 200;

pub(crate) fn node_config(gossip_bind_addr: SocketAddr) -> sundog::ClusterConfig {
    common::fast_config().with(|c| {
        c.gossip_bind_addr = gossip_bind_addr;
        c.ae_sketch_min_bucket = SKETCH_MIN_BUCKET;
        c.ae_part_min_bucket = PART_MIN_BUCKET;
        // `crdt_compact_task` ticks at `(crdt_retire_after / 4).max(30s)`;
        // shrinking `crdt_retire_after` well under 120s pins that cadence to
        // its 30s floor instead of the default's six hours. No cache opened
        // in this binary besides `seed_crdt_compaction_metrics`'s `counters`
        // ever merges, so this is otherwise inert.
        c.crdt_retire_after = Duration::from_secs(1);
    })
}

/// The anti-entropy bucket a `u32` key hashes into, mirroring
/// `store::stripe_index_from_hash`'s formula; `cluster.rs`'s and
/// the `tests/sim/` suite's own tests carry the identical helper for the same
/// reason.
fn bucket_of(key: u32) -> u16 {
    let bytes = postcard::to_stdvec(&key).expect("u32 key encodes");
    let bucket = xxhash_rust::xxh3::xxh3_64(&bytes) & (sundog::store::BUCKET_COUNT as u64 - 1);
    u16::try_from(bucket).expect("invariant: masked to BUCKET_COUNT - 1, always fits in u16")
}

/// `count` keys guaranteed to land in the same anti-entropy bucket, dense
/// enough to force the part path at [`PART_MIN_BUCKET`] without a
/// uniform-fill key count in the millions.
pub(crate) fn keys_in_one_bucket(count: usize) -> Vec<u32> {
    let target = bucket_of(0);
    (0..)
        .filter(|&k| bucket_of(k) == target)
        .take(count)
        .collect()
}

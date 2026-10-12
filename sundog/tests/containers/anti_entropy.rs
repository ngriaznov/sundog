//! The anti-entropy repair and bulk-fill scenarios.

use crate::container_util::{Node, eventually, seed, spawn_trio, wait_for_peers};
use crate::support::{CONVERGE_WAIT, scrape_metric};
use rightsize::Network;
use std::sync::Arc;
use std::time::Duration;

/// `sundog-testnode`'s control protocol has no way to make a live member
/// miss one fan-out message, so stopping and restarting under the same
/// alias is the closest reachable equivalent: the restart's `open()` runs
/// state transfer and one anti-entropy round before `testnode-ready`
/// prints, exercising the same repair path anti-entropy exists for.
#[tokio::test]
async fn anti_entropy_repairs_a_gap_after_a_member_returns() {
    require_containers!();

    let net = Arc::new(Network::new_network());
    let (n1, n2, n3) = spawn_trio(&net, "ae-cluster").await;

    n1.put("steady", "v0").await.expect("n1 puts");
    eventually(CONVERGE_WAIT, || async {
        n2.get("steady").await == Ok(Some("v0".to_string()))
            && n3.get("steady").await == Ok(Some("v0".to_string()))
    })
    .await;

    n3.stop().await.expect("n3 stops");
    wait_for_peers(&[&n1, &n2], 1).await;

    n1.put("gap", "v1").await.expect("n1 puts while n3 is down");
    eventually(CONVERGE_WAIT, || async {
        n2.get("gap").await == Ok(Some("v1".to_string()))
    })
    .await;

    let n3 = Node::spawn(&net, "ae-cluster", "n3", &[&seed("n1"), &seed("n2")]).await;
    eventually(Duration::from_secs(15), || async {
        n3.get("steady").await == Ok(Some("v0".to_string()))
            && n3.get("gap").await == Ok(Some("v1".to_string()))
    })
    .await;

    n1.stop().await.expect("n1 stops");
    n2.stop().await.expect("n2 stops");
    n3.stop().await.expect("n3 stops");
    net.close().await.expect("network closes");
}

/// Sent bytes per 1s sample at or under which a node counts as quiet for
/// [`wait_for_quiescent_netstats`]. Two in-sync replicas never stop
/// talking: every anti-entropy round costs a bucket digest of about 3 KB
/// for a populated cache and a couple of bytes for an empty one, and with
/// four caches each side runs a jittered round every 1 to 3 s per cache, so
/// a second with no frame at all is a matter of jitter alignment rather
/// than a state the node reaches. A state-transfer chunk alone runs about
/// 26 KB and a draining stream carries dozens per second, so this ceiling
/// admits the digest chatter and excludes the stream.
const QUIET_BYTES_PER_SAMPLE: u64 = 32 * 1024;

/// Waits until `node`'s cumulative sent-byte counter moves by at most
/// [`QUIET_BYTES_PER_SAMPLE`] across each of two consecutive 1s samples, so
/// a `netstats` snapshot taken right after this returns isolates whatever
/// traffic comes next from any trailing activity (a state-transfer
/// stream's last chunks, or the first post-join anti-entropy round) still
/// draining at the moment a peer's local count first matches the expected
/// total. Two samples in a row keep a one-second stall inside a stream on
/// a busy runner from passing as the stream's end.
/// # Panics
///
/// Panics if the counter is still moving faster than that once `timeout`
/// elapses.
async fn wait_for_quiescent_netstats(node: &Node, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    let (_, mut last_bytes) = node.netstats().await.expect("netstats");
    let mut quiet_samples = 0;
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let (_, bytes) = node.netstats().await.expect("netstats");
        if bytes - last_bytes <= QUIET_BYTES_PER_SAMPLE {
            quiet_samples += 1;
            if quiet_samples == 2 {
                return;
            }
        } else {
            quiet_samples = 0;
        }
        last_bytes = bytes;
        assert!(
            tokio::time::Instant::now() < deadline,
            "{}'s netstats never settled within {timeout:?}",
            node.name()
        );
    }
}

/// At 500k entries, `sundog-testnode`'s 1,024 buckets hold about 488 entries
/// apiece, past `ClusterConfig::default`'s `ae_sketch_min_bucket` of 384:
/// the repair below runs through the IBLT sketch path, not a full listing.
/// n2 cold-joins and warms first, so both replicas start byte-identical;
/// dropping one key locally on n2 then leaves exactly one bucket mismatched
/// for anti-entropy to close. The drop waits for n1's sent bytes to go
/// quiet ([`wait_for_quiescent_netstats`]): n2's local count reaches ENTRIES
/// slightly before its state-transfer stream and the first post-join
/// anti-entropy round finish draining, and a record of that tail delivered
/// after the drop puts the dropped key straight back.
#[tokio::test]
async fn anti_entropy_repairs_a_dropped_key_at_sketch_scale() {
    const ENTRIES: u32 = 500_000;
    const TARGET_KEY: &str = "k123456";
    const TARGET_VALUE: &str = "v123456";

    require_containers!();

    let net = Arc::new(Network::new_network());
    let n1 = Node::spawn(&net, "ae-sketch-cluster", "n1", &[]).await;
    n1.fill(ENTRIES).await.expect("bulk fill succeeds");
    assert_eq!(n1.count().await, Ok(ENTRIES as usize));

    let n2 = Node::spawn(&net, "ae-sketch-cluster", "n2", &[&seed("n1")]).await;
    eventually(Duration::from_secs(200), || async {
        n2.count().await == Ok(ENTRIES as usize)
    })
    .await;
    assert_eq!(n2.get(TARGET_KEY).await, Ok(Some(TARGET_VALUE.to_string())));
    wait_for_quiescent_netstats(&n1, Duration::from_secs(90)).await;

    n2.drop_key(TARGET_KEY)
        .await
        .expect("drop succeeds, standing in for a lost Replicate");
    assert_eq!(
        n2.get(TARGET_KEY).await,
        Ok(None),
        "n2's copy is gone locally right after the drop"
    );

    // sundog-testnode sets `ae_interval` to 2s; generous past that plus the
    // digest pass and sketch build/peel/pull round trip over 500k entries.
    eventually(Duration::from_secs(60), || async {
        n2.get(TARGET_KEY).await == Ok(Some(TARGET_VALUE.to_string()))
    })
    .await;

    n1.stop().await.expect("n1 stops");
    n2.stop().await.expect("n2 stops");
    net.close().await.expect("network closes");
}

/// At a 1,000,000-entry fill, `sundog-testnode`'s 1,024 buckets hold about
/// 976 entries apiece. With `SUNDOG_TESTNODE_AE_PART_MIN_BUCKET` lowered to
/// [`PART_MIN_BUCKET`], well past that, the mismatched bucket below is
/// answered with 64 part digests instead of a ~976-entry listing; only the
/// one part the dropped key falls into then answers with its own small
/// listing. n2 cold-joins and warms first, so both replicas start
/// byte-identical; dropping one key locally on n2 then leaves exactly one
/// bucket, and within it one part, mismatched. The repair's shape is read
/// off both nodes' `sundog_ae_parts_total` outcomes summed, since each node
/// runs its own rounds and the initiator counts a listing: one `listing`
/// from whichever node's round finds the gap first, and a second when the
/// other node's round also compares before the first round's repair lands,
/// with no `sketch` or `fallback` and no bucket-level sketch either: n1's
/// wire
/// bytes over the same window also carry its own rounds' 3 KB digests,
/// so they are printed for reference, not asserted on. The baseline is
/// read after [`wait_for_quiescent_netstats`]: n2's local count reaches
/// ENTRIES slightly before its state-transfer stream and the first
/// post-join anti-entropy round finish draining, and that round's own
/// outcomes must not count as the repair's.
#[tokio::test]
async fn anti_entropy_repairs_a_dropped_key_through_part_digests() {
    const ENTRIES: u32 = 1_000_000;
    const TARGET_KEY: &str = "k123456";
    const TARGET_VALUE: &str = "v123456";
    const PART_MIN_BUCKET: &str = "512";

    require_containers!();

    let net = Arc::new(Network::new_network());
    let env = [("SUNDOG_TESTNODE_AE_PART_MIN_BUCKET", PART_MIN_BUCKET)];
    let n1 = Node::spawn_with_env(&net, "ae-parts-cluster", "n1", &[], &env).await;
    n1.fill(ENTRIES).await.expect("bulk fill succeeds");
    assert_eq!(n1.count().await, Ok(ENTRIES as usize));

    let n2 = Node::spawn_with_env(&net, "ae-parts-cluster", "n2", &[&seed("n1")], &env).await;
    eventually(Duration::from_secs(200), || async {
        n2.count().await == Ok(ENTRIES as usize)
    })
    .await;
    assert_eq!(n2.get(TARGET_KEY).await, Ok(Some(TARGET_VALUE.to_string())));

    // n2's local count reaches ENTRIES slightly before its state-transfer
    // stream and the first post-join anti-entropy round finish draining;
    // waiting for n1's own sent-byte counter to go quiet isolates the
    // repair's cost below from that unrelated tail.
    // A shared CI runner can keep n1's counters moving well past 30 s while
    // the million-entry transfer and fan-out drain.
    wait_for_quiescent_netstats(&n1, Duration::from_secs(90)).await;

    let (frames_before, bytes_before) = n1.netstats().await.expect("netstats before the drop");
    let outcomes_before = ae_outcomes_on_both(&n1, &n2).await;

    n2.drop_key(TARGET_KEY)
        .await
        .expect("drop succeeds, standing in for a lost Replicate");
    assert_eq!(
        n2.get(TARGET_KEY).await,
        Ok(None),
        "n2's copy is gone locally right after the drop"
    );

    // sundog-testnode sets `ae_interval` to 2s; generous past that plus the
    // digest pass, part-digest comparison, and pull round trip.
    eventually(Duration::from_secs(60), || async {
        n2.get(TARGET_KEY).await == Ok(Some(TARGET_VALUE.to_string()))
    })
    .await;

    let (frames_after, bytes_after) = n1.netstats().await.expect("netstats after the repair");
    let frames_for_repair = frames_after - frames_before;
    let bytes_for_repair = bytes_after - bytes_before;
    println!("part-digest repair: n1 sent {frames_for_repair} frames / {bytes_for_repair} bytes");
    let outcomes_after = ae_outcomes_on_both(&n1, &n2).await;
    let repair = outcomes_after.zip_with(outcomes_before, |after, before| after - before);
    assert_eq!(
        AeOutcomes {
            listing: 0,
            ..repair
        },
        AeOutcomes {
            listing: 0,
            sketch: 0,
            fallback: 0,
            bucket_sketches: 0
        },
        "the mismatched bucket answers with part digests, never a bucket sketch or a full \
         listing: {repair:?}"
    );
    assert!(
        (1..=2).contains(&repair.listing),
        "each node's round that finds the gap before the repair lands lists the one part the \
         dropped key falls into, once: {repair:?}"
    );
    assert_eq!(n2.get(TARGET_KEY).await, Ok(Some(TARGET_VALUE.to_string())));

    n1.stop().await.expect("n1 stops");
    n2.stop().await.expect("n2 stops");
    net.close().await.expect("network closes");
}

/// `wire::RecordHeader`'s exact fixed width per record ahead of its key and
/// value bytes: `wall_ms` (8) + `logical` (4) + `node` (8) + `expires_at_ms`
/// (8) + `key_len` (4) + `value_len` (4) + `flags` (1). Not reachable from
/// here (`wire::RECORD_HEADER_LEN` is `pub(crate)`), so restated as the
/// documented layout rather than a bare magic number.
const RECORD_HEADER_BYTES: u64 = 8 + 4 + 8 + 8 + 4 + 4 + 1;

/// One wire-sized copy of `fill`'s deterministic `k{i}`/`v{i}` entries: each
/// record's key/value bytes plus its fixed [`RECORD_HEADER_BYTES`] header,
/// the dominant cost at these key/value sizes. Omits the handful of
/// `RawFrameHeader`/cache-name/length-delimiter bytes shared across a whole
/// batch, negligible once amortized over thousands of records per batch.
/// [`bulk_fill_replicates_without_anti_entropy_duplicating_it`] checks its
/// measured bytes against a multiple of this.
fn fill_payload_bytes(count: u32) -> u64 {
    (0..count)
        .map(|i| RECORD_HEADER_BYTES + (format!("k{i}").len() + format!("v{i}").len()) as u64)
        .sum()
}

/// Pins the fan-out queue and the anti-entropy streaming skip together: a
/// bulk fill on a live three-node cluster must replicate as a handful of
/// batched frames fanned out once per peer, not one frame per record and not
/// a second copy from anti-entropy racing in behind it. Anti-entropy skips a
/// round while replicate traffic moves toward a peer, so a fill outlasting
/// `MAX_STREAMING_SKIPS` rounds may see a legitimate repair; the byte budget
/// below allows for that record's wire cost twice over, not as a duplicate.
#[tokio::test]
async fn bulk_fill_replicates_without_anti_entropy_duplicating_it() {
    const ENTRIES: u32 = 100_000;

    require_containers!();

    let net = Arc::new(Network::new_network());
    let (n1, n2, n3) = spawn_trio(&net, "fanout-cluster").await;

    let (frames_before, bytes_before) = n1.netstats().await.expect("netstats before the fill");

    n1.fill(ENTRIES).await.expect("bulk fill succeeds");
    assert_eq!(n1.count().await, Ok(ENTRIES as usize));
    eventually(Duration::from_secs(120), || async {
        n2.count().await == Ok(ENTRIES as usize) && n3.count().await == Ok(ENTRIES as usize)
    })
    .await;

    let (frames_after, bytes_after) = n1.netstats().await.expect("netstats after the fill");
    let frames_for_fill = frames_after - frames_before;
    let bytes_for_fill = bytes_after - bytes_before;

    let payload_estimate = fill_payload_bytes(ENTRIES);
    // Two peers receive every record, so one frame per record is
    // 2 * ENTRIES frames. The batched fan-out queue coalesces at least ten
    // records per frame however slowly the fill runs on a loaded machine;
    // an idle 4-core box sends a few hundred frames in total.
    let record_sends = u64::from(ENTRIES) * 2;
    eprintln!("bulk fill: n1 sent {frames_for_fill} frames for {record_sends} record sends");
    assert!(
        frames_for_fill < record_sends / 10,
        "n1 sent {frames_for_fill} frames for a {ENTRIES}-entry fill to 2 peers; the batched \
         fan-out queue should coalesce at least ten records per frame, not one frame per record"
    );
    // Anti-entropy repairs applied cluster-wide during the fill: zero on an
    // idle box, a legitimate repair count when the fill outlasts the streaming skip.
    let mut repaired: u64 = 0;
    for node in [&n1, &n2, &n3] {
        repaired += scrape_metric(node, "sundog_ae_repaired_total", ("cache", "it")).await;
    }
    let per_record = payload_estimate.div_ceil(u64::from(ENTRIES));
    let repair_allowance = repaired * per_record * 2;
    eprintln!(
        "bulk fill: n1 sent {bytes_for_fill} bytes against a {payload_estimate}-byte single copy; \
         anti-entropy repaired {repaired} records"
    );
    assert!(
        bytes_for_fill < payload_estimate * 3 + repair_allowance,
        "n1 sent {bytes_for_fill} bytes for a {ENTRIES}-entry fill against an estimated \
         single-copy wire payload of {payload_estimate} bytes (each entry's `k{{i}}`/`v{{i}}` \
         bytes plus its fixed {RECORD_HEADER_BYTES}-byte record header); a normal 2-peer fan-out \
         costs about 2x that, 3x leaves headroom for batch/frame overhead, and the \
         {repaired} records anti-entropy repaired in flight are granted {repair_allowance} \
         bytes on top; anything past that is a copy nothing accounts for"
    );

    n1.stop().await.expect("n1 stops");
    n2.stop().await.expect("n2 stops");
    n3.stop().await.expect("n3 stops");
    net.close().await.expect("network closes");
}

/// Scrapes `node`'s `/metrics` and reads one `metric{label}` value, `0` if
/// the scrape fails or the series has never been touched (a metric with no
/// writes yet is absent from the exposition, not printed as zero). Every
/// `sundog_spill_*`/`sundog_ae_repaired_total` series this file reads is an
/// exact-integer count, so rounding it to `u64` here sidesteps
/// `clippy::float_cmp` entirely: every comparison below compares `u64`s,
/// never `f64`s, mirroring `tests/spill_bench.rs`'s `metric_count`.
/// The anti-entropy outcome counters two nodes report between them: part
/// outcomes by kind plus bucket-level sketches, each summed over both nodes
/// since the node whose round finds a gap is the one that counts it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AeOutcomes {
    listing: u64,
    sketch: u64,
    fallback: u64,
    bucket_sketches: u64,
}

impl AeOutcomes {
    fn zip_with(self, other: Self, f: impl Fn(u64, u64) -> u64) -> Self {
        Self {
            listing: f(self.listing, other.listing),
            sketch: f(self.sketch, other.sketch),
            fallback: f(self.fallback, other.fallback),
            bucket_sketches: f(self.bucket_sketches, other.bucket_sketches),
        }
    }
}

async fn ae_outcomes_on_both(a: &Node, b: &Node) -> AeOutcomes {
    let mut total = AeOutcomes {
        listing: 0,
        sketch: 0,
        fallback: 0,
        bucket_sketches: 0,
    };
    for node in [a, b] {
        let one = AeOutcomes {
            listing: scrape_metric(node, "sundog_ae_parts_total", ("outcome", "listing")).await,
            sketch: scrape_metric(node, "sundog_ae_parts_total", ("outcome", "sketch")).await,
            fallback: scrape_metric(node, "sundog_ae_parts_total", ("outcome", "fallback")).await,
            bucket_sketches: scrape_metric(node, "sundog_ae_sketch_total", ("cache", "it")).await,
        };
        total = total.zip_with(one, |x, y| x + y);
    }
    total
}

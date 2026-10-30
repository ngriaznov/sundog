//! Unit tests for the framing, request pool, reply readers and responder handlers.

fn rec(key: &'static [u8], value: &'static [u8]) -> WireRecord {
    WireRecord {
        key: Bytes::from_static(key),
        value: Some(Bytes::from_static(value)),
        ver: Hlc {
            wall_ms: 1,
            logical: 0,
            node: NodeId::from(1),
        },
        expires_at_ms: None,
    }
}

/// Each reply's key and value byte count.
fn reply_sizes(replies: &[Msg]) -> Vec<usize> {
    replies
        .iter()
        .map(|msg| match msg {
            Msg::Loaded { found, uncached } => {
                found
                    .iter()
                    .map(|rec| rec.key.len() + rec.value.as_ref().map_or(0, Bytes::len))
                    .sum::<usize>()
                    + uncached
                        .iter()
                        .map(|(key, value)| key.len() + value.len())
                        .sum::<usize>()
            }
            other => panic!("only Loaded replies, got {other:?}"),
        })
        .collect()
}

#[test]
fn loaded_replies_split_past_the_budget_and_keep_every_entry() {
    let found = vec![
        rec(b"k1", b"aaaa"),
        rec(b"k2", b"bbbb"),
        rec(b"k3", b"cccc"),
    ];
    let uncached = vec![(Bytes::from_static(b"k4"), Bytes::from_static(b"dddd"))];
    let replies = super::serve::loaded_replies(found.clone(), uncached.clone(), 12);
    assert_eq!(
        reply_sizes(&replies),
        vec![12, 12],
        "6 bytes an entry, two a reply"
    );
    let (mut all_found, mut all_uncached) = (Vec::new(), Vec::new());
    for reply in replies {
        if let Msg::Loaded { found, uncached } = reply {
            all_found.extend(found);
            all_uncached.extend(uncached);
        }
    }
    assert_eq!(all_found, found);
    assert_eq!(all_uncached, uncached);
}

#[test]
fn loaded_replies_send_one_reply_for_nothing_and_an_oversized_entry_alone() {
    assert_eq!(
        super::serve::loaded_replies(Vec::new(), Vec::new(), 12),
        vec![Msg::Loaded {
            found: Vec::new(),
            uncached: Vec::new()
        }]
    );
    let replies = super::serve::loaded_replies(
        vec![rec(b"k1", b"aaaaaaaaaaaaaaaa"), rec(b"k2", b"bb")],
        Vec::new(),
        8,
    );
    assert_eq!(reply_sizes(&replies), vec![18, 4]);
}

#[test]
fn listing_or_sketch_crosses_over_past_min_bucket() {
    let entry = |n: u64| crate::store::KeyVersion {
        key: bytes::Bytes::from(n.to_be_bytes().to_vec()),
        version: crate::hlc::Hlc {
            wall_ms: n,
            logical: 0,
            node: crate::node::NodeId::from(1),
        },
    };
    let small: Vec<_> = (0..3).map(entry).collect();
    assert_eq!(
        super::serve::listing_or_sketch(small.clone(), 3, 6),
        super::serve::ListingOrSketch::Listing(small),
        "up to min_bucket entries stay a listing"
    );
    let large: Vec<_> = (0..4).map(entry).collect();
    match super::serve::listing_or_sketch(large, 3, 6) {
        super::serve::ListingOrSketch::Sketch(cells) => assert_eq!(cells.len(), 6),
        other @ super::serve::ListingOrSketch::Listing(_) => {
            panic!("expected a sketch, got {other:?}")
        }
    }
}

/// A `(key, version)` entry whose key is `key_len` bytes holding `n`.
fn listing_entry(n: u64, key_len: usize) -> crate::store::KeyVersion {
    let mut key = vec![0u8; key_len.max(8)];
    key[..8].copy_from_slice(&n.to_be_bytes());
    key.truncate(key_len.max(8));
    crate::store::KeyVersion {
        key: Bytes::from(key),
        version: Hlc {
            wall_ms: n,
            logical: 0,
            node: NodeId::from(1),
        },
    }
}

/// The estimate [`super::serve::chunk_ae_listing`] charges a slice.
fn listing_cost(slice: &[crate::store::KeyVersion]) -> usize {
    slice
        .iter()
        .map(|kv| kv.key.len() + wire::AE_ENTRY_MAX_WIRE_LEN)
        .sum()
}

/// `count` entries whose keys are `key_len` bytes each.
fn long_listing(count: u64, key_len: usize) -> Vec<crate::store::KeyVersion> {
    (0..count).map(|n| listing_entry(n, key_len)).collect()
}

#[test]
fn listing_fits_counts_each_key_plus_the_entry_overhead() {
    let entries = long_listing(4, 100);
    let exact = 4 * (100 + wire::AE_ENTRY_MAX_WIRE_LEN);
    for (budget, fits) in [
        (usize::MAX, true),
        (exact + 1, true),
        (exact, true),
        (exact - 1, false),
        (3 * (100 + wire::AE_ENTRY_MAX_WIRE_LEN), false),
        (0, false),
    ] {
        assert_eq!(
            super::serve::listing_fits(&entries, budget),
            fits,
            "budget {budget}"
        );
    }
}

#[test]
fn listing_fits_takes_an_empty_listing_at_any_budget() {
    assert!(super::serve::listing_fits(&[], 0));
    assert!(super::serve::listing_fits(
        &[],
        super::serve::AE_LISTING_BUDGET
    ));
}

#[test]
fn listing_fits_rejects_one_key_longer_than_the_budget() {
    let one = long_listing(1, 1_000);
    assert!(super::serve::listing_fits(
        &one,
        1_000 + wire::AE_ENTRY_MAX_WIRE_LEN
    ));
    assert!(!super::serve::listing_fits(
        &one,
        999 + wire::AE_ENTRY_MAX_WIRE_LEN
    ));
}

#[test]
fn listing_fits_weighs_key_length_not_entry_count() {
    let many_short = long_listing(100, 8);
    let few_long = long_listing(3, 2_000);
    let budget = 100 * (8 + wire::AE_ENTRY_MAX_WIRE_LEN);
    assert!(super::serve::listing_fits(&many_short, budget));
    assert!(
        !super::serve::listing_fits(&few_long, budget),
        "three 2000-byte keys outweigh a hundred 8-byte ones"
    );
}

#[test]
fn listing_fits_agrees_with_chunk_ae_listing_on_what_is_one_slice() {
    let entries: Vec<_> = (0..20)
        .map(|n| listing_entry(n, 10 + 7 * usize::try_from(n).expect("small")))
        .collect();
    let total = listing_cost(&entries);
    for budget in [0, 1, total / 3, total - 1, total, total + 1] {
        let one_slice = super::serve::chunk_ae_listing(entries.clone(), budget).len() == 1;
        assert_eq!(
            super::serve::listing_fits(&entries, budget),
            one_slice,
            "budget {budget} of {total}"
        );
    }
}

#[test]
fn listing_or_sketch_answers_a_small_bucket_of_long_keys_with_a_sketch() {
    // Three keys of a sixth of a frame each: a bucket far under min_bucket
    // whose listing passes the half frame one reply may hold.
    let long = long_listing(3, MAX_FRAME / 6);
    assert!(!super::serve::listing_fits(
        &long,
        super::serve::AE_LISTING_BUDGET
    ));
    let super::serve::ListingOrSketch::Sketch(cells) =
        super::serve::listing_or_sketch(long.clone(), 384, 6)
    else {
        panic!("a listing past half a frame goes out as a sketch");
    };
    let mut expected = crate::cluster::sketch::Iblt::new(6);
    for kv in &long {
        expected.insert(xxhash_rust::xxh3::xxh3_64(&kv.key), kv.version);
    }
    assert_eq!(
        cells,
        expected.into_cells(),
        "the sketch covers every key's hash and version"
    );
}

#[test]
fn listing_or_sketch_keeps_a_listing_that_fills_the_budget_exactly() {
    let half = super::serve::AE_LISTING_BUDGET / 2 - wire::AE_ENTRY_MAX_WIRE_LEN;
    let exact = long_listing(2, half);
    assert!(super::serve::listing_fits(
        &exact,
        super::serve::AE_LISTING_BUDGET
    ));
    assert_eq!(
        super::serve::listing_or_sketch(exact.clone(), 384, 6),
        super::serve::ListingOrSketch::Listing(exact),
        "a listing of exactly half a frame stays a listing"
    );
    let over = long_listing(2, half + 1);
    assert!(matches!(
        super::serve::listing_or_sketch(over, 384, 6),
        super::serve::ListingOrSketch::Sketch(_)
    ));
}

#[test]
fn listing_or_sketch_still_sketches_a_bucket_of_short_keys_past_min_bucket() {
    let short = long_listing(5, 8);
    assert!(super::serve::listing_fits(
        &short,
        super::serve::AE_LISTING_BUDGET
    ));
    assert!(matches!(
        super::serve::listing_or_sketch(short.clone(), 4, 6),
        super::serve::ListingOrSketch::Sketch(_)
    ));
    assert_eq!(
        super::serve::listing_or_sketch(short.clone(), 5, 6),
        super::serve::ListingOrSketch::Listing(short),
        "short keys within min_bucket stay a listing"
    );
}

#[test]
fn chunk_ae_listing_keeps_every_entry_in_order_across_chunks() {
    let entries: Vec<_> = (0..50).map(|n| listing_entry(n, 16)).collect();
    let budget = 10 * (16 + wire::AE_ENTRY_MAX_WIRE_LEN);
    let chunks = super::serve::chunk_ae_listing(entries.clone(), budget);
    assert_eq!(chunks.len(), 5, "fifty equal entries, ten per slice");
    for chunk in &chunks {
        assert!(listing_cost(chunk) <= budget, "{}", listing_cost(chunk));
    }
    let joined: Vec<_> = chunks.into_iter().flatten().collect();
    assert_eq!(joined, entries, "the slices concatenate to the listing");
}

#[test]
fn chunk_ae_listing_does_not_split_a_listing_that_fits() {
    let entries: Vec<_> = (0..4).map(|n| listing_entry(n, 16)).collect();
    let exact = listing_cost(&entries);
    assert_eq!(
        super::serve::chunk_ae_listing(entries.clone(), exact),
        vec![entries.clone()],
        "a listing that fills the budget exactly is one slice"
    );
    assert_eq!(
        super::serve::chunk_ae_listing(entries.clone(), exact - 1).len(),
        2,
        "one byte under splits it"
    );
}

#[test]
fn chunk_ae_listing_returns_one_empty_chunk_for_an_empty_bucket() {
    assert_eq!(
        super::serve::chunk_ae_listing(Vec::new(), 100),
        vec![Vec::new()]
    );
    assert_eq!(
        super::serve::chunk_ae_listing(Vec::new(), 0),
        vec![Vec::new()],
        "a zero budget still answers an empty bucket"
    );
}

#[test]
fn chunk_ae_listing_puts_an_oversized_entry_alone() {
    let small = listing_entry(1, 8);
    let big = listing_entry(2, 1_000);
    let chunks = super::serve::chunk_ae_listing(
        vec![small.clone(), big.clone(), small.clone()],
        100 + wire::AE_ENTRY_MAX_WIRE_LEN,
    );
    assert_eq!(chunks, vec![vec![small.clone()], vec![big], vec![small]]);
}

#[test]
fn every_chunk_of_a_listing_encodes_under_max_frame_whatever_the_key_length() {
    // A 255-byte cache name is the widest envelope; keys straddle the
    // varint width boundaries of the key length and the entry count.
    let cache = SmolStr::new("c".repeat(255));
    for key_len in [8usize, 33, 127, 128, 200, 4_096, 16_384, 70_000] {
        let per_entry = key_len + wire::AE_ENTRY_MAX_WIRE_LEN;
        let budget = MAX_FRAME / 2;
        for count in [
            budget / per_entry - 1,
            budget / per_entry,
            budget / per_entry + 1,
            2 * budget / per_entry + 3,
        ] {
            let entries: Vec<_> = (0..count as u64)
                .map(|n| listing_entry(n, key_len))
                .collect();
            let replies =
                super::serve::ae_entries_replies(&cache, vec![(7, entries.clone())], true, budget);
            let mut joined = Vec::new();
            for reply in replies {
                let encoded = wire::encode(&reply)
                    .unwrap_or_else(|error| panic!("key {key_len}, {count} entries: {error}"));
                assert!(encoded.len() <= MAX_FRAME);
                let Msg::AeBucket { entries, .. } = reply else {
                    panic!("only AeBucket replies");
                };
                joined.extend(entries);
            }
            assert_eq!(
                joined.len(),
                entries.len(),
                "key {key_len}, {count} entries"
            );
        }
    }
}

#[test]
fn ae_entries_replies_send_one_message_per_bucket_for_an_unchunked_peer() {
    let cache = SmolStr::new("users");
    let entries: Vec<_> = (0..100).map(|n| listing_entry(n, 32)).collect();
    let replies = super::serve::ae_entries_replies(
        &cache,
        vec![(3, entries.clone()), (7, Vec::new())],
        false,
        64,
    );
    assert_eq!(
        replies,
        vec![
            Msg::AeBucket {
                cache: cache.clone(),
                bucket: 3,
                entries: super::framing::key_versions_to_wire(entries),
            },
            Msg::AeBucket {
                cache,
                bucket: 7,
                entries: Vec::new(),
            },
        ],
        "a peer below the chunking protocol reads each message as a whole bucket"
    );
}

#[test]
fn ae_entries_replies_split_a_chunked_bucket_and_keep_an_empty_one() {
    let cache = SmolStr::new("users");
    let entries: Vec<_> = (0..100).map(|n| listing_entry(n, 32)).collect();
    let budget = 30 * (32 + wire::AE_ENTRY_MAX_WIRE_LEN);
    let replies = super::serve::ae_entries_replies(
        &cache,
        vec![(3, entries.clone()), (7, Vec::new())],
        true,
        budget,
    );
    let buckets: Vec<(u16, usize)> = replies
        .iter()
        .map(|msg| match msg {
            Msg::AeBucket {
                bucket, entries, ..
            } => (*bucket, entries.len()),
            other => panic!("only AeBucket replies, got {other:?}"),
        })
        .collect();
    assert_eq!(buckets, vec![(3, 30), (3, 30), (3, 30), (3, 10), (7, 0)]);
}

#[test]
fn ae_entries_replies_encode_under_max_frame_when_chunked_at_the_real_budget() {
    let cache = SmolStr::new("users");
    let entries: Vec<_> = (0..300_000).map(|n| listing_entry(n, 32)).collect();
    let replies = super::serve::ae_entries_replies(
        &cache,
        vec![(3, entries)],
        true,
        super::serve::AE_LISTING_BUDGET,
    );
    assert!(replies.len() > 1, "{} replies", replies.len());
    for reply in &replies {
        wire::encode(reply).expect("every reply fits one frame");
    }
}

#[test]
fn merge_bucket_chunk_concatenates_chunks_of_one_bucket() {
    let mut out = Vec::new();
    super::reply::merge_bucket_chunk(&mut out, 3, vec![listing_entry(1, 8)]);
    super::reply::merge_bucket_chunk(&mut out, 3, vec![listing_entry(2, 8)]);
    assert_eq!(
        out,
        vec![(3, vec![listing_entry(1, 8), listing_entry(2, 8)])]
    );
}

#[test]
fn merge_bucket_chunk_keeps_first_appearance_order_across_buckets() {
    let mut out = Vec::new();
    super::reply::merge_bucket_chunk(&mut out, 7, vec![listing_entry(1, 8)]);
    super::reply::merge_bucket_chunk(&mut out, 3, vec![listing_entry(2, 8)]);
    super::reply::merge_bucket_chunk(&mut out, 9, Vec::new());
    assert_eq!(
        out,
        vec![
            (7, vec![listing_entry(1, 8)]),
            (3, vec![listing_entry(2, 8)]),
            (9, Vec::new()),
        ]
    );
}

#[test]
fn merge_bucket_chunk_merges_interleaved_chunks_of_one_bucket() {
    let mut out = Vec::new();
    super::reply::merge_bucket_chunk(&mut out, 3, vec![listing_entry(1, 8)]);
    super::reply::merge_bucket_chunk(&mut out, 7, vec![listing_entry(2, 8)]);
    super::reply::merge_bucket_chunk(&mut out, 3, vec![listing_entry(3, 8)]);
    assert_eq!(
        out,
        vec![
            (3, vec![listing_entry(1, 8), listing_entry(3, 8)]),
            (7, vec![listing_entry(2, 8)]),
        ]
    );
}

/// Below [`super::pool::REQ_POOL_MAX_IDLE`] a pooled connection stays fresh;
/// at or past it, `ReqPool::checkout` must treat it as stale. Uses
/// tokio's paused clock so the boundary is exact rather than
/// timing-dependent.
#[tokio::test(start_paused = true)]
async fn pooled_is_fresh_expires_at_the_max_idle_boundary() {
    let checked_in = tokio::time::Instant::now().into();
    tokio::time::advance(
        super::pool::REQ_POOL_MAX_IDLE
            .checked_sub(Duration::from_millis(1))
            .expect("invariant: REQ_POOL_MAX_IDLE exceeds one millisecond"),
    )
    .await;
    assert!(
        super::pool::pooled_is_fresh(checked_in, tokio::time::Instant::now().into()),
        "narrowly under REQ_POOL_MAX_IDLE must stay fresh"
    );

    tokio::time::advance(Duration::from_millis(1)).await;
    assert!(
        !super::pool::pooled_is_fresh(checked_in, tokio::time::Instant::now().into()),
        "at REQ_POOL_MAX_IDLE must be stale"
    );
}

// Only the `not(sim)` tests below dial real loopback sockets through
// `handle_accepted`/`run_peer_writer`/the request collectors; under
// `sim` those tests (and these imports) are compiled out entirely.
#[cfg(not(feature = "sim"))]
use std::net::SocketAddr;
#[cfg(not(feature = "sim"))]
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt as _, StreamExt as _};
use smol_str::SmolStr;
#[cfg(not(feature = "sim"))]
use tokio::sync::mpsc;
// Real tokio sockets and a locally-built codec, not this module's own
// `new_framed`/`send_msg`/`recv_msg`: this suite exercises the real
// socket stack regardless of feature flags.
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::LengthDelimitedCodec;
#[cfg(not(feature = "sim"))]
use tokio_util::sync::CancellationToken;

#[cfg(not(feature = "sim"))]
use super::framing::PeerFramed;
#[cfg(not(feature = "sim"))]
use super::pool::ReqPool;
#[cfg(not(feature = "sim"))]
use super::reply::Reply;
use super::writer::coalesce_replicate;
#[cfg(not(feature = "sim"))]
use crate::error::CodecError;
use crate::hlc::Hlc;
use crate::net::OutFrame;
#[cfg(not(feature = "sim"))]
use crate::net::{AeMismatch, AePartReply, BucketStreamItem, InboundMsg};
use crate::node::NodeId;
use crate::wire::{self, MAX_FRAME, Msg, WireRecord};

pub(super) fn out_frame(msg: Msg) -> OutFrame {
    OutFrame::new(msg).expect("test message encodes")
}

pub(super) fn record(n: u8) -> WireRecord {
    WireRecord {
        key: Bytes::from(vec![n]),
        value: Some(Bytes::from(vec![n, n])),
        ver: Hlc {
            wall_ms: u64::from(n),
            logical: 0,
            node: NodeId::from(1),
        },
        expires_at_ms: None,
    }
}

/// A lone queued `Msg::Replicate` must come out of `coalesce_replicate`
/// as exactly the frame it already carried in: no re-encode.
#[test]
fn coalesce_replicate_reuses_the_lone_frame_verbatim() {
    let item = out_frame(Msg::Replicate {
        cache: SmolStr::new("users"),
        rec: record(1),
    });
    let original_frame = item.frame.clone();
    let out = coalesce_replicate(vec![item]);
    assert_eq!(out, vec![original_frame]);
}

/// Consecutive same-cache `Msg::Replicate` entries merge into one
/// `Msg::ReplicateBatch` frame decoding back to all of them, in order.
#[test]
fn coalesce_replicate_merges_consecutive_same_cache_entries() {
    let drained = vec![
        out_frame(Msg::Replicate {
            cache: SmolStr::new("users"),
            rec: record(1),
        }),
        out_frame(Msg::Replicate {
            cache: SmolStr::new("users"),
            rec: record(2),
        }),
    ];
    let out = coalesce_replicate(drained);
    assert_eq!(
        out.len(),
        1,
        "two same-cache entries must merge into one frame"
    );
    let decoded = wire::decode(&out[0]).expect("decodes");
    assert_eq!(
        decoded,
        Msg::ReplicateBatch {
            cache: SmolStr::new("users"),
            recs: vec![record(1), record(2)],
        }
    );
}

/// A cache-name change mid-run closes off the pending merge rather than
/// mixing records from two caches into one `Msg::ReplicateBatch`.
#[test]
fn coalesce_replicate_does_not_merge_across_a_cache_change() {
    let drained = vec![
        out_frame(Msg::Replicate {
            cache: SmolStr::new("a"),
            rec: record(1),
        }),
        out_frame(Msg::Replicate {
            cache: SmolStr::new("b"),
            rec: record(2),
        }),
    ];
    let out = coalesce_replicate(drained);
    assert_eq!(out.len(), 2, "a cache change must end the pending run");
    assert_eq!(
        wire::decode(&out[0]).expect("decodes"),
        Msg::Replicate {
            cache: SmolStr::new("a"),
            rec: record(1),
        }
    );
    assert_eq!(
        wire::decode(&out[1]).expect("decodes"),
        Msg::Replicate {
            cache: SmolStr::new("b"),
            rec: record(2),
        }
    );
}

/// A `Msg::ReplicateBatch` following same-cache entries in the pending
/// run merges into it too, not only a lone `Msg::Replicate`.
#[test]
fn coalesce_replicate_merges_a_replicate_batch_into_a_pending_run() {
    let drained = vec![
        out_frame(Msg::Replicate {
            cache: SmolStr::new("users"),
            rec: record(1),
        }),
        out_frame(Msg::ReplicateBatch {
            cache: SmolStr::new("users"),
            recs: vec![record(2), record(3)],
        }),
    ];
    let out = coalesce_replicate(drained);
    assert_eq!(
        out.len(),
        1,
        "a same-cache batch must merge into the pending run"
    );
    let decoded = wire::decode(&out[0]).expect("decodes");
    assert_eq!(
        decoded,
        Msg::ReplicateBatch {
            cache: SmolStr::new("users"),
            recs: vec![record(1), record(2), record(3)],
        }
    );
}

/// A non-`Replicate`-class frame following a pending run flushes the run
/// first, then passes through untouched.
#[test]
fn coalesce_replicate_flushes_the_pending_run_before_a_non_replicate_frame() {
    let invalidate = Msg::Invalidate {
        cache: SmolStr::new("users"),
        key: Bytes::from_static(b"k"),
        ver: Hlc {
            wall_ms: 9,
            logical: 0,
            node: NodeId::from(1),
        },
    };
    let drained = vec![
        out_frame(Msg::Replicate {
            cache: SmolStr::new("users"),
            rec: record(1),
        }),
        out_frame(Msg::Replicate {
            cache: SmolStr::new("users"),
            rec: record(2),
        }),
        out_frame(invalidate.clone()),
    ];
    let out = coalesce_replicate(drained);
    assert_eq!(
        out.len(),
        2,
        "the non-replicate frame must close off the pending merge first"
    );
    assert_eq!(
        wire::decode(&out[0]).expect("decodes"),
        Msg::ReplicateBatch {
            cache: SmolStr::new("users"),
            recs: vec![record(1), record(2)],
        }
    );
    assert_eq!(wire::decode(&out[1]).expect("decodes"), invalidate);
}

#[tokio::test]
async fn codec_roundtrips_a_message_through_a_real_socket_pair() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("listener has a local addr");
    let codec = || {
        LengthDelimitedCodec::builder()
            .max_frame_length(MAX_FRAME)
            .new_codec()
    };

    let accept = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        tokio_util::codec::Framed::new(stream, codec())
    });
    let client = TcpStream::connect(addr).await.expect("connect");
    let mut client = tokio_util::codec::Framed::new(client, codec());
    let mut server = accept.await.expect("connection accepted");

    let sent = Msg::Hello {
        node: NodeId::from(7),
        incarnation: 3,
        protocol: wire::PROTOCOL_VERSION,
        cluster: wire::UNNAMED_CLUSTER,
    };
    let encoded = wire::encode(&sent).expect("encodes");
    client.send(encoded).await.expect("send");
    let frame = server
        .next()
        .await
        .expect("frame arrives")
        .expect("no io error");
    let got = wire::decode(&frame.freeze()).expect("decodes");
    assert_eq!(got, sent);
}

// `MeshStream` is a different concrete type per feature, so this test
// exercises the real transport directly and is skipped under `sim`.
#[cfg(all(feature = "tls", not(feature = "sim")))]
fn as_mesh_stream(stream: tokio::net::TcpStream) -> crate::net::MeshStream {
    crate::net::MeshStream::Plain(stream)
}
#[cfg(all(not(feature = "tls"), not(feature = "sim")))]
fn as_mesh_stream(stream: tokio::net::TcpStream) -> crate::net::MeshStream {
    stream
}

#[cfg(not(feature = "sim"))]
#[tokio::test]
async fn serve_state_transfer_stops_promptly_once_cancelled() {
    use futures::future::BoxFuture;
    use futures::stream::BoxStream;
    use smol_str::SmolStr;
    use tokio_util::sync::CancellationToken;

    struct NeverRespondingHandler;
    impl crate::net::RequestHandler for NeverRespondingHandler {
        fn snapshot_chunks(&self, _cache: SmolStr) -> BoxStream<'static, Vec<wire::WireRecord>> {
            Box::pin(futures::stream::pending())
        }
        fn digests(&self, _cache: SmolStr) -> BoxFuture<'_, Vec<crate::store::BucketDigest>> {
            Box::pin(async { Vec::new() })
        }
        fn bucket_entries(
            &self,
            _cache: SmolStr,
            _bucket: u16,
        ) -> BoxFuture<'_, Vec<crate::store::KeyVersion>> {
            Box::pin(async { Vec::new() })
        }
        fn entries_for_buckets(
            &self,
            _cache: SmolStr,
            _buckets: Vec<u16>,
        ) -> BoxFuture<'_, crate::store::BucketEntries> {
            Box::pin(async { Vec::new() })
        }
        fn records_for(
            &self,
            _cache: SmolStr,
            _keys: Vec<bytes::Bytes>,
        ) -> BoxFuture<'_, Vec<wire::WireRecord>> {
            Box::pin(async { Vec::new() })
        }
        fn bucket_lens(
            &self,
            _cache: SmolStr,
            _buckets: Vec<u16>,
        ) -> BoxFuture<'_, Vec<crate::store::BucketLen>> {
            Box::pin(async { Vec::new() })
        }
        fn part_digests(
            &self,
            _cache: SmolStr,
            _buckets: Vec<u16>,
        ) -> BoxFuture<'_, Vec<crate::store::BucketPartDigests>> {
            Box::pin(async { Vec::new() })
        }
        fn entries_for_parts(
            &self,
            _cache: SmolStr,
            _parts: Vec<crate::store::BucketPart>,
        ) -> BoxFuture<'_, crate::store::PartEntries> {
            Box::pin(async { Vec::new() })
        }
    }

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("listener has a local addr");
    let accept = tokio::spawn(async move { listener.accept().await.expect("accept").0 });
    let _client = TcpStream::connect(addr).await.expect("connect");
    let server_stream = accept.await.expect("connection accepted");

    let framed = super::framing::new_framed(as_mesh_stream(server_stream));
    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();

    let served = tokio::spawn(async move {
        let mut framed = framed;
        super::serve::serve_state_transfer(
            &mut framed,
            SmolStr::new("c"),
            &NeverRespondingHandler,
            &cancel_for_task,
            crate::wire::PROTOCOL_VERSION,
        )
        .await;
    });

    cancel.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(2), served)
        .await
        .expect(
            "an accepted-connection handler must observe cancellation instead of blocking \
             forever on a snapshot stream that never yields; otherwise Mesh::shutdown() \
             leaves it running with the shard registry Arc still held",
        )
        .expect("accepted-connection handler did not panic");
}

/// `TlsCtx` for a plain (non-TLS) accepted/dialed connection in a test,
/// matching whichever concrete shape the active feature set gives it.
#[cfg(all(feature = "tls", not(feature = "sim")))]
pub(super) fn no_tls() -> crate::net::TlsCtx {
    None
}
#[cfg(all(not(feature = "tls"), not(feature = "sim")))]
pub(super) fn no_tls() -> crate::net::TlsCtx {
    crate::net::TlsCtx
}

/// A `RequestHandler` with nothing to serve: every lookup comes back
/// empty, never called in the tests that use it since they exercise
/// paths that never reach the handler.
#[cfg(not(feature = "sim"))]
pub(super) struct EmptyHandler;
#[cfg(not(feature = "sim"))]
impl crate::net::RequestHandler for EmptyHandler {
    fn snapshot_chunks(
        &self,
        _cache: SmolStr,
    ) -> futures::stream::BoxStream<'static, Vec<WireRecord>> {
        Box::pin(futures::stream::empty())
    }
    fn digests(
        &self,
        _cache: SmolStr,
    ) -> futures::future::BoxFuture<'_, Vec<crate::store::BucketDigest>> {
        Box::pin(async { Vec::new() })
    }
    fn bucket_entries(
        &self,
        _cache: SmolStr,
        _bucket: u16,
    ) -> futures::future::BoxFuture<'_, Vec<crate::store::KeyVersion>> {
        Box::pin(async { Vec::new() })
    }
    fn entries_for_buckets(
        &self,
        _cache: SmolStr,
        _buckets: Vec<u16>,
    ) -> futures::future::BoxFuture<'_, crate::store::BucketEntries> {
        Box::pin(async { Vec::new() })
    }
    fn records_for(
        &self,
        _cache: SmolStr,
        _keys: Vec<Bytes>,
    ) -> futures::future::BoxFuture<'_, Vec<WireRecord>> {
        Box::pin(async { Vec::new() })
    }
    fn bucket_lens(
        &self,
        _cache: SmolStr,
        _buckets: Vec<u16>,
    ) -> futures::future::BoxFuture<'_, Vec<crate::store::BucketLen>> {
        Box::pin(async { Vec::new() })
    }
    fn part_digests(
        &self,
        _cache: SmolStr,
        _buckets: Vec<u16>,
    ) -> futures::future::BoxFuture<'_, Vec<crate::store::BucketPartDigests>> {
        Box::pin(async { Vec::new() })
    }
    fn entries_for_parts(
        &self,
        _cache: SmolStr,
        _parts: Vec<crate::store::BucketPart>,
    ) -> futures::future::BoxFuture<'_, crate::store::PartEntries> {
        Box::pin(async { Vec::new() })
    }
}

/// Dials a fresh loopback connection to a fake donor that sends every
/// message in `prelude` and then drops the connection without a
/// terminating `Msg::ReqDone`, for the "closed before `ReqDone`" and
/// "unrelated frame skipped" collector tests below.
#[cfg(not(feature = "sim"))]
async fn dial_fake_donor(prelude: Vec<Msg>) -> PeerFramed {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("listener has a local addr");
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut framed = LengthDelimitedCodec::builder()
            .max_frame_length(MAX_FRAME)
            .new_framed(stream);
        for msg in prelude {
            let encoded = wire::encode(&msg).expect("encodes");
            framed.send(encoded).await.expect("send");
        }
        // Dropped here: the connection closes without ever sending
        // `Msg::ReqDone`.
    });
    let client = TcpStream::connect(addr).await.expect("connect");
    super::framing::new_framed(as_mesh_stream(client))
}

/// Polls [`super::pool::probe_reused`] until it stops reporting `Pending`, for
/// a connection whose peer has already acted.
#[cfg(not(feature = "sim"))]
async fn probe_settled(framed: &mut PeerFramed) -> super::pool::ReusedProbe {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match super::pool::probe_reused(framed) {
                super::pool::ReusedProbe::Pending => {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                settled => return settled,
            }
        }
    })
    .await
    .expect("the peer's action reaches the probe within the bound")
}

#[cfg(not(feature = "sim"))]
#[tokio::test]
async fn probe_reused_tells_a_waiting_reply_from_a_closed_peer_and_a_quiet_one() {
    let mut replied = dial_fake_donor(vec![Msg::ReqDone]).await;
    assert!(matches!(
        probe_settled(&mut replied).await,
        super::pool::ReusedProbe::Ready(Msg::ReqDone)
    ));

    let mut closed = dial_fake_donor(Vec::new()).await;
    assert!(matches!(
        probe_settled(&mut closed).await,
        super::pool::ReusedProbe::Stale
    ));

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("listener has a local addr");
    let _quiet_server = tokio::spawn(async move {
        let (_stream, _) = listener.accept().await.expect("accept");
        std::future::pending::<()>().await;
    });
    let client = TcpStream::connect(addr).await.expect("connect");
    let mut quiet = super::framing::new_framed(as_mesh_stream(client));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(matches!(
        super::pool::probe_reused(&mut quiet),
        super::pool::ReusedProbe::Pending
    ));
}

/// A [`Reply`] over `framed`, backed by a fresh, empty pool: the
/// collector tests below never expect a checked-in connection to matter.
#[cfg(not(feature = "sim"))]
fn reply(framed: PeerFramed) -> Reply {
    (framed, Arc::new(ReqPool::new()), None)
}

/// Asserts `err` is the `UnexpectedEof` a collector returns when its
/// connection closes before the terminating `Msg::ReqDone`.
#[cfg(not(feature = "sim"))]
fn assert_unexpected_eof(err: &CodecError) {
    assert!(
        matches!(err, CodecError::Io(io_err) if io_err.kind() == std::io::ErrorKind::UnexpectedEof),
        "expected an UnexpectedEof i/o error, got {err:?}"
    );
}

#[cfg(not(feature = "sim"))]
#[tokio::test]
async fn collect_ae_buckets_errors_on_a_connection_closed_before_req_done() {
    let framed = dial_fake_donor(Vec::new()).await;
    let err = super::reply::collect_ae_buckets(reply(framed))
        .await
        .expect_err("a connection closed before ReqDone must error");
    assert_unexpected_eof(&err);
}

#[cfg(not(feature = "sim"))]
#[tokio::test]
async fn collect_ae_mismatches_errors_on_a_connection_closed_before_req_done() {
    let framed = dial_fake_donor(Vec::new()).await;
    let err = super::reply::collect_ae_mismatches(reply(framed))
        .await
        .expect_err("a connection closed before ReqDone must error");
    assert_unexpected_eof(&err);
}

#[cfg(not(feature = "sim"))]
#[tokio::test]
async fn collect_pulled_records_errors_on_a_connection_closed_before_req_done() {
    let framed = dial_fake_donor(Vec::new()).await;
    let err = super::reply::collect_pulled_records(reply(framed))
        .await
        .expect_err("a connection closed before ReqDone must error");
    assert_unexpected_eof(&err);
}

#[cfg(not(feature = "sim"))]
#[tokio::test]
async fn collect_ae_buckets_skips_an_unrelated_hello_mid_reply() {
    let entries = vec![(
        Bytes::from_static(b"k1"),
        Hlc {
            wall_ms: 5,
            logical: 0,
            node: NodeId::from(1),
        },
    )];
    let framed = dial_fake_donor(vec![
        Msg::Hello {
            node: NodeId::from(9),
            incarnation: 1,
            protocol: wire::PROTOCOL_VERSION,
            cluster: wire::UNNAMED_CLUSTER,
        },
        Msg::AeBucket {
            cache: SmolStr::new("users"),
            bucket: 3,
            entries: entries.clone(),
        },
        Msg::ReqDone,
    ])
    .await;
    let got = super::reply::collect_ae_buckets(reply(framed))
        .await
        .expect("the stray Hello must be skipped, not break the reply");
    assert_eq!(
        got,
        vec![(3, super::framing::wire_to_key_versions(entries))]
    );
}

#[test]
fn a_declined_scoped_round_replies_stale_view_or_nothing() {
    let cache = SmolStr::new("users");
    assert_eq!(
        super::serve::declined_replies(
            cache.clone(),
            &crate::net::AeServeOutcome::<()>::Stale {
                responder_view_hash: 7
            }
        ),
        vec![Msg::StaleView {
            cache: cache.clone(),
            responder_view_hash: 7
        }]
    );
    let unavailable_replies = super::serve::declined_replies(
        cache.clone(),
        &crate::net::AeServeOutcome::<()>::Unavailable,
    );
    assert!(unavailable_replies.is_empty(), "{unavailable_replies:?}");
    let digests_replies =
        super::serve::declined_replies(cache, &crate::net::AeServeOutcome::Digests(()));
    assert!(digests_replies.is_empty(), "{digests_replies:?}");
}

#[cfg(not(feature = "sim"))]
#[tokio::test]
async fn collect_ae_mismatches_skips_an_unrelated_hello_mid_reply() {
    let framed = dial_fake_donor(vec![
        Msg::Hello {
            node: NodeId::from(9),
            incarnation: 1,
            protocol: wire::PROTOCOL_VERSION,
            cluster: wire::UNNAMED_CLUSTER,
        },
        Msg::AeSketch {
            cache: SmolStr::new("users"),
            bucket: 2,
            cells: Vec::new(),
        },
        Msg::ReqDone,
    ])
    .await;
    let got = super::reply::collect_ae_mismatches(reply(framed))
        .await
        .expect("the stray Hello must be skipped, not break the reply");
    assert_eq!(got, vec![AeMismatch::Sketch(2, Vec::new())]);
}

#[cfg(not(feature = "sim"))]
#[tokio::test]
async fn collect_pulled_records_skips_an_unrelated_hello_mid_reply() {
    let rec = WireRecord {
        key: Bytes::from_static(b"k1"),
        value: Some(Bytes::from_static(b"v1")),
        ver: Hlc {
            wall_ms: 5,
            logical: 0,
            node: NodeId::from(1),
        },
        expires_at_ms: None,
    };
    let framed = dial_fake_donor(vec![
        Msg::Hello {
            node: NodeId::from(9),
            incarnation: 1,
            protocol: wire::PROTOCOL_VERSION,
            cluster: wire::UNNAMED_CLUSTER,
        },
        Msg::Replicate {
            cache: SmolStr::new("users"),
            rec: rec.clone(),
        },
        Msg::ReqDone,
    ])
    .await;
    let got = super::reply::collect_pulled_records(reply(framed))
        .await
        .expect("the stray Hello must be skipped, not break the reply");
    assert_eq!(got, vec![rec]);
}

#[cfg(not(feature = "sim"))]
#[tokio::test]
async fn collect_ae_mismatches_reports_part_digests_replies() {
    let digests: Vec<u64> = (0..64u64).collect();
    let framed = dial_fake_donor(vec![
        Msg::AePartDigests {
            cache: SmolStr::new("users"),
            bucket: 5,
            digests: digests.clone(),
        },
        Msg::ReqDone,
    ])
    .await;
    let got = super::reply::collect_ae_mismatches(reply(framed))
        .await
        .expect("collects the AePartDigests reply");
    assert_eq!(got, vec![AeMismatch::PartDigests(5, digests)]);
}

#[cfg(not(feature = "sim"))]
#[tokio::test]
async fn collect_ae_part_replies_errors_on_a_connection_closed_before_req_done() {
    let framed = dial_fake_donor(Vec::new()).await;
    let err = super::reply::collect_ae_part_replies(reply(framed))
        .await
        .expect_err("a connection closed before ReqDone must error");
    assert_unexpected_eof(&err);
}

#[cfg(not(feature = "sim"))]
#[tokio::test]
async fn collect_ae_part_replies_reports_listings_and_sketches() {
    let entries = vec![(
        Bytes::from_static(b"k1"),
        Hlc {
            wall_ms: 5,
            logical: 0,
            node: NodeId::from(1),
        },
    )];
    let framed = dial_fake_donor(vec![
        Msg::Hello {
            node: NodeId::from(9),
            incarnation: 1,
            protocol: wire::PROTOCOL_VERSION,
            cluster: wire::UNNAMED_CLUSTER,
        },
        Msg::AePart {
            cache: SmolStr::new("users"),
            bucket: 3,
            part: 7,
            entries: entries.clone(),
        },
        Msg::AePartSketch {
            cache: SmolStr::new("users"),
            bucket: 3,
            part: 8,
            cells: Vec::new(),
        },
        Msg::ReqDone,
    ])
    .await;
    let got = super::reply::collect_ae_part_replies(reply(framed))
        .await
        .expect("the stray Hello must be skipped, not break the reply");
    assert_eq!(
        got,
        vec![
            AePartReply::Listing {
                bucket: 3,
                part: 7,
                entries: super::framing::wire_to_key_versions(entries),
            },
            AePartReply::Sketch {
                bucket: 3,
                part: 8,
                cells: Vec::new(),
            },
        ]
    );
}

/// `Msg::ReqDone`/`Msg::AeBucket` arriving as the first message after
/// `Hello` fall into the request-class routing arm that treats this as
/// a persistent broadcast-class connection: neither served nor
/// forwarded to `inbound_tx`, but the loop keeps reading, so a genuine
/// broadcast message afterward still comes through.
#[cfg(not(feature = "sim"))]
#[tokio::test]
async fn req_done_and_ae_bucket_after_hello_are_not_served_or_forwarded() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("listener has a local addr");
    let accept = tokio::spawn(async move { listener.accept().await.expect("accept").0 });
    let client_stream = TcpStream::connect(addr).await.expect("connect");
    let server_stream = accept.await.expect("connection accepted");

    let (inbound_tx, mut inbound_rx) = mpsc::channel(8);
    let cancel = CancellationToken::new();
    let handler: Arc<dyn crate::net::RequestHandler> = Arc::new(EmptyHandler);
    // `handle_accepted` takes the pre-TLS raw stream, not `MeshStream`;
    // it layers TLS on internally via `establish_accept`.
    let accepted_task = tokio::spawn(super::accept::handle_accepted(
        server_stream,
        inbound_tx,
        handler,
        crate::net::MeshInner::for_tests(no_tls(), cancel.clone()),
        None,
    ));

    let mut client = LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME)
        .new_framed(client_stream);
    let from = NodeId::from(9);
    for msg in [
        Msg::Hello {
            node: from,
            incarnation: 1,
            protocol: wire::PROTOCOL_VERSION,
            cluster: wire::UNNAMED_CLUSTER,
        },
        Msg::ReqDone,
        Msg::AeBucket {
            cache: SmolStr::new("users"),
            bucket: 0,
            entries: Vec::new(),
        },
    ] {
        let encoded = wire::encode(&msg).expect("encodes");
        client.send(encoded).await.expect("send");
    }

    let nothing = tokio::time::timeout(Duration::from_millis(200), inbound_rx.recv()).await;
    assert!(
        nothing.is_err(),
        "ReqDone/AeBucket right after Hello must not reach inbound_tx"
    );

    // The accept loop must still be reading: a genuine broadcast
    // message afterward is forwarded normally.
    let invalidate = Msg::Invalidate {
        cache: SmolStr::new("users"),
        key: Bytes::from_static(b"k"),
        ver: Hlc {
            wall_ms: 1,
            logical: 0,
            node: from,
        },
    };
    let encoded = wire::encode(&invalidate).expect("encodes");
    client.send(encoded).await.expect("send");
    let got = tokio::time::timeout(Duration::from_secs(2), inbound_rx.recv())
        .await
        .expect("the connection must still be reading")
        .expect("channel open");
    assert_eq!(
        got,
        InboundMsg {
            from,
            msg: invalidate
        }
    );

    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(2), accepted_task).await;
}

#[cfg(all(unix, not(feature = "sim")))]
#[tokio::test]
async fn tune_socket_logs_and_does_not_panic_when_the_socket_is_gone() {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("listener has a local addr");
    let stream = TcpStream::connect(addr).await.expect("connect");

    // Close the stream's underlying fd out from under it, so the
    // subsequent `set_nodelay` fails with a real OS error (EBADF)
    // instead of the happy path.
    let fd = stream.as_raw_fd();
    // SAFETY: `fd` is a valid, open fd owned by `stream`; wrapping and
    // immediately dropping it closes that fd number exactly once. The
    // stream is forgotten below so its own `Drop` never double-closes it.
    drop(unsafe { OwnedFd::from_raw_fd(fd) });

    super::framing::tune_socket(&stream, Some(Duration::from_secs(30))); // must log, not panic
    std::mem::forget(stream);
}

#[cfg(not(feature = "sim"))]
#[test]
fn keepalive_timing_probes_a_third_of_the_idle_apart_three_times() {
    use super::framing::{KeepaliveTiming, keepalive_timing};

    assert_eq!(
        keepalive_timing(Duration::from_secs(30)),
        KeepaliveTiming {
            idle: Duration::from_secs(30),
            interval: Duration::from_secs(10),
            retries: 3,
        }
    );
    assert_eq!(
        keepalive_timing(Duration::from_secs(2)).interval,
        Duration::from_secs(1),
        "probes are at least a second apart"
    );
    assert_eq!(
        keepalive_timing(Duration::from_millis(500)),
        KeepaliveTiming {
            idle: Duration::from_secs(1),
            interval: Duration::from_secs(1),
            retries: 3,
        },
        "no kernel takes a sub-second idle or interval"
    );
    assert_eq!(
        keepalive_timing(Duration::MAX),
        KeepaliveTiming {
            idle: Duration::from_secs(32_767),
            interval: Duration::from_secs(32_767) / 3,
            retries: 3,
        },
        "nor one past 32,767 seconds"
    );
}

/// A connected pair of loopback sockets: the dialed end and the accepted end.
#[cfg(not(feature = "sim"))]
async fn loopback_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("listener has a local addr");
    let (dialed, accepted) = tokio::join!(TcpStream::connect(addr), listener.accept());
    (dialed.expect("connect"), accepted.expect("accept").0)
}

#[cfg(all(target_os = "linux", not(feature = "sim")))]
#[tokio::test]
async fn a_request_connection_runs_keepalive_at_the_configured_idle() {
    use std::os::fd::AsRawFd as _;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("listener has a local addr");
    let greeting = crate::net::Greeting {
        node: NodeId::from(1),
        incarnation: 1,
        cluster: wire::UNNAMED_CLUSTER,
    };
    let tls = no_tls();
    let (dialed, accepted) = tokio::join!(
        super::writer::dial_with_hello_and(
            addr,
            greeting,
            &tls,
            Some(Duration::from_secs(9)),
            Msg::ReqDone,
        ),
        listener.accept(),
    );
    let framed = dialed.expect("dials and sends its first message");
    let _accepted = accepted.expect("accept");
    #[cfg(feature = "tls")]
    let crate::net::MeshStream::Plain(stream) = framed.get_ref() else {
        panic!("no TLS context, so a plain stream");
    };
    #[cfg(not(feature = "tls"))]
    let stream = framed.get_ref();
    assert_eq!(
        crate::net::tests::keepalive::keepalive_settings(stream.as_raw_fd()),
        Some((9, 3, 3))
    );
}

#[cfg(not(feature = "sim"))]
#[tokio::test]
async fn tune_socket_turns_keepalive_on_at_the_configured_idle_and_none_leaves_it_off() {
    let (with, without) = loopback_pair().await;
    super::framing::tune_socket(&with, Some(Duration::from_secs(45)));
    super::framing::tune_socket(&without, None);

    assert!(
        socket2::SockRef::from(&with)
            .keepalive()
            .expect("reads SO_KEEPALIVE")
    );
    assert!(with.nodelay().expect("reads TCP_NODELAY"));
    assert!(
        !socket2::SockRef::from(&without)
            .keepalive()
            .expect("reads SO_KEEPALIVE")
    );
    assert!(without.nodelay().expect("reads TCP_NODELAY"));
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd as _;
        assert_eq!(
            crate::net::tests::keepalive::keepalive_settings(with.as_raw_fd()),
            Some((45, 15, 3)),
            "idle, interval and probe count in seconds"
        );
    }
}

#[cfg(not(feature = "sim"))]
#[tokio::test]
async fn run_peer_writer_exits_promptly_when_cancelled_while_retrying() {
    // Loopback port 1 is unassigned: the connection is refused
    // immediately, so the writer cycles through its reconnect backoff
    // instead of hanging on a slow connect.
    let unreachable: SocketAddr = "127.0.0.1:1".parse().expect("valid addr");
    let invalidate = Arc::new(crate::net::outbox::DropOldestQueue::new(4, usize::MAX));
    let (_replicate_tx, replicate_rx) = crate::net::replicate_outbox(4, 1 << 20);
    let cancel = CancellationToken::new();
    let greeting = crate::net::Greeting {
        node: NodeId::from(1),
        incarnation: 1,
        cluster: wire::UNNAMED_CLUSTER,
    };
    let handle = tokio::spawn(super::writer::run_peer_writer(
        super::PeerTarget {
            peer: NodeId::from(2),
            greeting,
            addr: unreachable,
            dirty: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            tls: no_tls(),
            keepalive: None,
        },
        invalidate,
        replicate_rx,
        cancel.clone(),
    ));

    // Let it fail to connect and settle into the retry backoff.
    tokio::time::sleep(Duration::from_millis(50)).await;
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect(
            "run_peer_writer must exit promptly once cancelled while retrying, not wait \
             out its reconnect backoff",
        )
        .expect("writer task did not panic");
}

/// A distinct `WireRecord` for the bucket-done/ack tests below.
#[cfg(not(feature = "sim"))]
fn bucket_wire_record(n: u8) -> WireRecord {
    WireRecord {
        key: Bytes::from(vec![n]),
        value: Some(Bytes::from(vec![n, n])),
        ver: Hlc {
            wall_ms: u64::from(n),
            logical: 0,
            node: NodeId::from(1),
        },
        expires_at_ms: None,
    }
}

#[cfg(not(feature = "sim"))]
#[tokio::test]
async fn bucket_stream_yields_bucket_done_between_and_after_chunks_when_the_peer_supports_it() {
    let cache = SmolStr::new("users");
    let rec1 = bucket_wire_record(1);
    let rec2 = bucket_wire_record(2);
    let framed = dial_fake_donor(vec![
        Msg::StBucketChunk {
            cache: cache.clone(),
            recs: vec![rec1.clone()],
            done: false,
        },
        Msg::StBucketDone {
            cache: cache.clone(),
            bucket: 0,
        },
        Msg::StBucketChunk {
            cache: cache.clone(),
            recs: vec![rec2.clone()],
            done: false,
        },
        Msg::StBucketDone {
            cache: cache.clone(),
            bucket: 1,
        },
        Msg::StBucketChunk {
            cache: cache.clone(),
            recs: Vec::new(),
            done: true,
        },
    ])
    .await;

    let mut stream = super::reply::bucket_stream(
        framed,
        Arc::new(ReqPool::new()),
        None,
        cache,
        wire::PROTOCOL_VERSION,
        42,
    );
    let mut got = Vec::new();
    while let Some(item) = stream.next().await {
        got.push(item.expect("item decodes"));
    }
    assert_eq!(got.len(), 4, "two chunks and two bucket-done signals");
    assert!(matches!(&got[0], BucketStreamItem::Chunk(chunk) if chunk == &vec![rec1]));
    assert!(matches!(got[1], BucketStreamItem::BucketDone(0)));
    assert!(matches!(&got[2], BucketStreamItem::Chunk(chunk) if chunk == &vec![rec2]));
    assert!(matches!(got[3], BucketStreamItem::BucketDone(1)));
}

#[cfg(not(feature = "sim"))]
#[tokio::test]
async fn bucket_stream_yields_nothing_new_when_the_peer_does_not_support_bucket_done() {
    // An older donor never sends StBucketDone, so only Chunks result.
    let cache = SmolStr::new("users");
    let rec1 = bucket_wire_record(1);
    let rec2 = bucket_wire_record(2);
    let framed = dial_fake_donor(vec![
        Msg::StBucketChunk {
            cache: cache.clone(),
            recs: vec![rec1.clone()],
            done: false,
        },
        Msg::StBucketChunk {
            cache: cache.clone(),
            recs: vec![rec2.clone()],
            done: true,
        },
    ])
    .await;

    let mut stream = super::reply::bucket_stream(
        framed,
        Arc::new(ReqPool::new()),
        None,
        cache,
        wire::PROTOCOL_DISTRIBUTED,
        42,
    );
    let mut got = Vec::new();
    while let Some(item) = stream.next().await {
        got.push(item.expect("item decodes"));
    }
    assert_eq!(got.len(), 2);
    assert!(matches!(&got[0], BucketStreamItem::Chunk(chunk) if chunk == &vec![rec1]));
    assert!(matches!(&got[1], BucketStreamItem::Chunk(chunk) if chunk == &vec![rec2]));
}

/// Serves two one-chunk buckets, for the bucket-done/ack test below.
#[cfg(not(feature = "sim"))]
struct TwoBucketHandler;
#[cfg(not(feature = "sim"))]
impl crate::net::RequestHandler for TwoBucketHandler {
    fn snapshot_chunks(
        &self,
        _cache: SmolStr,
    ) -> futures::stream::BoxStream<'static, Vec<WireRecord>> {
        Box::pin(futures::stream::empty())
    }
    fn digests(
        &self,
        _cache: SmolStr,
    ) -> futures::future::BoxFuture<'_, Vec<crate::store::BucketDigest>> {
        Box::pin(async { Vec::new() })
    }
    fn bucket_entries(
        &self,
        _cache: SmolStr,
        _bucket: u16,
    ) -> futures::future::BoxFuture<'_, Vec<crate::store::KeyVersion>> {
        Box::pin(async { Vec::new() })
    }
    fn entries_for_buckets(
        &self,
        _cache: SmolStr,
        _buckets: Vec<u16>,
    ) -> futures::future::BoxFuture<'_, crate::store::BucketEntries> {
        Box::pin(async { Vec::new() })
    }
    fn records_for(
        &self,
        _cache: SmolStr,
        _keys: Vec<Bytes>,
    ) -> futures::future::BoxFuture<'_, Vec<WireRecord>> {
        Box::pin(async { Vec::new() })
    }
    fn bucket_lens(
        &self,
        _cache: SmolStr,
        _buckets: Vec<u16>,
    ) -> futures::future::BoxFuture<'_, Vec<crate::store::BucketLen>> {
        Box::pin(async { Vec::new() })
    }
    fn part_digests(
        &self,
        _cache: SmolStr,
        _buckets: Vec<u16>,
    ) -> futures::future::BoxFuture<'_, Vec<crate::store::BucketPartDigests>> {
        Box::pin(async { Vec::new() })
    }
    fn entries_for_parts(
        &self,
        _cache: SmolStr,
        _parts: Vec<crate::store::BucketPart>,
    ) -> futures::future::BoxFuture<'_, crate::store::PartEntries> {
        Box::pin(async { Vec::new() })
    }
    fn st_buckets_available(
        &self,
        _cache: SmolStr,
        _view_hash: u64,
    ) -> futures::future::BoxFuture<'_, bool> {
        Box::pin(async { true })
    }
    fn st_bucket_chunks(
        &self,
        _cache: SmolStr,
        _buckets: Vec<u16>,
    ) -> futures::stream::BoxStream<'static, (u16, Vec<WireRecord>)> {
        // Keeps the select loop polling for the ack instead of exiting
        // as soon as the chunk stream ends.
        let padding = futures::stream::once(tokio::time::sleep(Duration::from_millis(200)))
            .filter_map(|()| async { None });
        Box::pin(
            futures::stream::iter(vec![
                (0u16, vec![bucket_wire_record(1)]),
                (1u16, vec![bucket_wire_record(2)]),
            ])
            .chain(padding),
        )
    }
}

#[cfg(not(feature = "sim"))]
#[tokio::test]
async fn serve_st_buckets_sends_bucket_done_after_the_last_chunk_and_reads_a_mid_stream_ack() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("listener has a local addr");
    let accept = tokio::spawn(async move { listener.accept().await.expect("accept").0 });
    let client_stream = TcpStream::connect(addr).await.expect("connect");
    let server_stream = accept.await.expect("connection accepted");
    let mut client_framed = super::framing::new_framed(as_mesh_stream(client_stream));

    let mesh = crate::net::MeshInner::for_tests(no_tls(), CancellationToken::new());
    let mesh_for_task = Arc::clone(&mesh);
    let cache = SmolStr::new("users");
    let cache_for_task = cache.clone();
    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let served = tokio::spawn(async move {
        let mut server_framed = super::framing::new_framed(as_mesh_stream(server_stream));
        super::serve::serve_st_buckets(
            &mut server_framed,
            cache_for_task,
            vec![0, 1],
            42,
            &TwoBucketHandler,
            mesh_for_task.as_ref(),
            NodeId::from(9),
            wire::PROTOCOL_VERSION,
            &cancel_for_task,
        )
        .await
    });

    let first = super::framing::recv_msg(&mut client_framed)
        .await
        .expect("frame arrives")
        .expect("decodes");
    assert!(
        matches!(&first, Msg::StBucketChunk{ recs, done: false, .. } if recs.len() == 1),
        "bucket 0's chunk comes first: {first:?}"
    );
    let second = super::framing::recv_msg(&mut client_framed)
        .await
        .expect("frame arrives")
        .expect("decodes");
    let Msg::StBucketDone {
        cache: ack_cache,
        bucket,
    } = second
    else {
        panic!("expected Msg::StBucketDone for bucket 0, got {second:?}");
    };
    assert_eq!(bucket, 0);
    // Ack it now, mid-stream, before bucket 1's own chunk/done arrive.
    super::framing::send_msg(
        &mut client_framed,
        &Msg::StBucketAck {
            cache: ack_cache,
            bucket,
            view_hash: 42,
        },
    )
    .await
    .expect("send ack");

    let third = super::framing::recv_msg(&mut client_framed)
        .await
        .expect("frame arrives")
        .expect("decodes");
    assert!(
        matches!(&third, Msg::StBucketChunk{ recs, done: false, .. } if recs.len() == 1),
        "bucket 1's chunk follows: {third:?}"
    );
    let fourth = super::framing::recv_msg(&mut client_framed)
        .await
        .expect("frame arrives")
        .expect("decodes");
    assert!(
        matches!(fourth, Msg::StBucketDone { bucket: 1, .. }),
        "bucket 1's done follows its chunk: {fourth:?}"
    );
    let fifth = super::framing::recv_msg(&mut client_framed)
        .await
        .expect("frame arrives")
        .expect("decodes");
    assert!(
        matches!(fifth, Msg::StBucketChunk { done: true, .. }),
        "the group's own trailing marker chunk closes the reply: {fifth:?}"
    );

    tokio::time::timeout(Duration::from_secs(5), served)
        .await
        .expect("serve_st_buckets ends once its reply is fully drained")
        .expect("serve_st_buckets does not panic");
    assert!(
        mesh.acked_buckets
            .read()
            .expect("lock is never poisoned")
            .contains_key(&(cache, 0, NodeId::from(9))),
        "the mid-stream ack for bucket 0 is folded into the acked-owners table"
    );
}

/// Pins that an ack arriving after `serve_st_buckets` has already
/// returned is still caught, by `handle_accepted`'s own `dispatch_one`
/// arm for `Msg::StBucketAck`.
#[cfg(not(feature = "sim"))]
#[tokio::test]
async fn handle_accepted_records_a_bucket_ack_that_arrives_after_any_serve_st_buckets_call_would_have_returned()
 {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("listener has a local addr");
    let accept = tokio::spawn(async move { listener.accept().await.expect("accept").0 });
    let client_stream = TcpStream::connect(addr).await.expect("connect");
    let server_stream = accept.await.expect("connection accepted");

    let (inbound_tx, _inbound_rx) = mpsc::channel(8);
    let cancel = CancellationToken::new();
    let handler: Arc<dyn crate::net::RequestHandler> = Arc::new(EmptyHandler);
    let mesh = crate::net::MeshInner::for_tests(no_tls(), cancel.clone());
    let accepted_task = tokio::spawn(super::accept::handle_accepted(
        server_stream,
        inbound_tx,
        handler,
        Arc::clone(&mesh),
        None,
    ));

    let mut client = LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME)
        .new_framed(client_stream);
    let from = NodeId::from(9);
    let cache = SmolStr::new("users");
    for msg in [
        Msg::Hello {
            node: from,
            incarnation: 1,
            protocol: wire::PROTOCOL_VERSION,
            cluster: wire::UNNAMED_CLUSTER,
        },
        Msg::StBucketAck {
            cache: cache.clone(),
            bucket: 1,
            view_hash: 42,
        },
    ] {
        let encoded = wire::encode(&msg).expect("encodes");
        client.send(encoded).await.expect("send");
    }

    tokio::time::timeout(Duration::from_secs(2), async {
        while !mesh
            .acked_buckets
            .read()
            .expect("lock is never poisoned")
            .contains_key(&(cache.clone(), 1, from))
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect(
        "an ack arriving as an ordinary frame, outside serve_st_buckets's own mid-stream \
         read, is still recorded",
    );

    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(2), accepted_task).await;
}

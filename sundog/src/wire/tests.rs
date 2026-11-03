//! Unit tests for the wire format: each message's round trip, the hello's
//! protocol gates, and the frame length bounds and malformed-frame errors.

use super::*;

fn sample_hlc() -> Hlc {
    Hlc {
        wall_ms: 1_700_000_000_000,
        logical: 7,
        node: NodeId::from(42),
    }
}

fn sample_record(value: Option<&str>) -> WireRecord {
    WireRecord {
        key: Bytes::from_static(b"user:1"),
        value: value.map(|v| Bytes::copy_from_slice(v.as_bytes())),
        ver: sample_hlc(),
        expires_at_ms: Some(1_700_000_600_000),
    }
}

fn roundtrip(msg: &Msg) {
    let encoded = encode(msg).expect("encodes");
    let decoded = decode(&encoded).expect("decodes");
    assert_eq!(*msg, decoded);
}

#[test]
fn tombstone_has_no_value() {
    assert!(sample_record(None).is_tombstone());
    assert!(!sample_record(Some("x")).is_tombstone());
}

#[test]
fn roundtrip_hello() {
    roundtrip(&Msg::Hello {
        node: NodeId::from(1),
        incarnation: 3,
        protocol: PROTOCOL_VERSION,
        cluster: cluster_id("prices"),
    });
}

#[test]
fn a_hello_without_a_cluster_field_decodes_as_unnamed() {
    let unnamed = UnnamedHello::Hello {
        node: NodeId::from(1),
        incarnation: 3,
        protocol: PROTOCOL_PART_OWNERSHIP,
    };
    let mut frame = vec![FRAME_KIND_POSTCARD];
    frame.extend(postcard::to_stdvec(&unnamed).expect("encodes"));
    assert_eq!(
        decode(&Bytes::from(frame)).expect("a protocol-5 hello decodes"),
        Msg::Hello {
            node: NodeId::from(1),
            incarnation: 3,
            protocol: PROTOCOL_PART_OWNERSHIP,
            cluster: UNNAMED_CLUSTER,
        }
    );
}

#[test]
fn cluster_ids_match_only_the_same_name_or_an_unnamed_side() {
    let (a, b) = (cluster_id("prices"), cluster_id("sessions"));
    assert_ne!(a, UNNAMED_CLUSTER);
    assert_eq!(
        a,
        cluster_id("prices"),
        "the id is a stable hash of the name"
    );
    assert!(same_cluster(a, a));
    assert!(!same_cluster(a, b), "two named clusters never talk");
    assert!(
        same_cluster(a, UNNAMED_CLUSTER),
        "an older peer is accepted"
    );
    assert!(
        same_cluster(UNNAMED_CLUSTER, b),
        "a bare mesh accepts anyone"
    );
}

#[test]
fn a_hello_without_a_protocol_field_decodes_as_protocol_one() {
    let legacy = LegacyMsg::Hello {
        node: NodeId::from(1),
        incarnation: 3,
    };
    let mut frame = vec![FRAME_KIND_POSTCARD];
    frame.extend(postcard::to_stdvec(&legacy).expect("encodes"));
    assert_eq!(
        decode(&Bytes::from(frame)).expect("a 0.3 hello decodes"),
        Msg::Hello {
            node: NodeId::from(1),
            incarnation: 3,
            protocol: 1,
            cluster: UNNAMED_CLUSTER,
        }
    );
}

#[test]
fn a_hello_with_trailing_fields_from_a_newer_protocol_decodes() {
    let mut frame = encode(&Msg::Hello {
        node: NodeId::from(1),
        incarnation: 3,
        protocol: PROTOCOL_VERSION + 1,
        cluster: cluster_id("prices"),
    })
    .expect("encodes")
    .to_vec();
    frame.extend_from_slice(&[0x07, 0x09, 0x11]);
    assert_eq!(
        decode(&Bytes::from(frame)).expect("trailing fields are ignored"),
        Msg::Hello {
            node: NodeId::from(1),
            incarnation: 3,
            protocol: PROTOCOL_VERSION + 1,
            cluster: cluster_id("prices"),
        }
    );
}

#[test]
fn a_truncated_non_hello_frame_still_fails_to_decode() {
    let frame = encode(&Msg::ReqDone).expect("encodes");
    let mut body = frame.to_vec();
    body.push(0xFF);
    body.push(0xFF);
    // A bare variant index past the enum's range is neither a message
    // nor a legacy hello.
    let bogus = Bytes::from(vec![FRAME_KIND_POSTCARD, 0x7F]);
    assert!(decode(&bogus).is_err());
    assert!(
        decode(&Bytes::from(body)).is_ok(),
        "trailing bytes after ReqDone are ignored"
    );
}

#[test]
fn peer_supports_compares_against_the_introducing_protocol() {
    assert!(peer_supports(2, PROTOCOL_PART_DIGESTS));
    assert!(peer_supports(3, PROTOCOL_ST_UNAVAILABLE));
    assert!(!peer_supports(1, PROTOCOL_PART_DIGESTS));
    assert!(!peer_supports(1, PROTOCOL_ST_UNAVAILABLE));
}

#[test]
fn a_protocol_two_peer_never_supports_distribution_mode() {
    assert!(!peer_supports(2, PROTOCOL_DISTRIBUTED));
    assert!(!peer_supports(1, PROTOCOL_DISTRIBUTED));
    assert!(peer_supports(3, PROTOCOL_DISTRIBUTED));
}

#[test]
fn roundtrip_invalidate() {
    roundtrip(&Msg::Invalidate {
        cache: SmolStr::new("users"),
        key: Bytes::from_static(b"k1"),
        ver: sample_hlc(),
    });
}

#[test]
fn roundtrip_replicate() {
    roundtrip(&Msg::Replicate {
        cache: SmolStr::new("users"),
        rec: sample_record(Some("v1")),
    });
}

#[test]
fn roundtrip_replicate_tombstone() {
    roundtrip(&Msg::Replicate {
        cache: SmolStr::new("users"),
        rec: sample_record(None),
    });
}

#[test]
fn roundtrip_replicate_no_expiry() {
    roundtrip(&Msg::Replicate {
        cache: SmolStr::new("users"),
        rec: WireRecord {
            expires_at_ms: None,
            ..sample_record(Some("v1"))
        },
    });
}

/// `u64::MAX` must round-trip as a real expiry, not `None`, since
/// `RECORD_FLAG_HAS_EXPIRY` is an explicit flag, not a sentinel value.
#[test]
fn roundtrip_replicate_expiry_at_u64_max() {
    roundtrip(&Msg::Replicate {
        cache: SmolStr::new("users"),
        rec: WireRecord {
            expires_at_ms: Some(u64::MAX),
            ..sample_record(Some("v1"))
        },
    });
}

#[test]
fn roundtrip_replicate_empty_key_and_value() {
    roundtrip(&Msg::Replicate {
        cache: SmolStr::new("users"),
        rec: WireRecord {
            key: Bytes::new(),
            value: Some(Bytes::new()),
            ver: sample_hlc(),
            expires_at_ms: None,
        },
    });
}

#[test]
fn roundtrip_replicate_empty_cache_name() {
    roundtrip(&Msg::Replicate {
        cache: SmolStr::new(""),
        rec: sample_record(Some("v1")),
    });
}

#[test]
fn roundtrip_st_request() {
    roundtrip(&Msg::StRequest {
        cache: SmolStr::new("users"),
    });
}

#[test]
fn roundtrip_st_chunk() {
    roundtrip(&Msg::StChunk {
        cache: SmolStr::new("users"),
        recs: vec![sample_record(Some("v1")), sample_record(None)],
        done: true,
    });
}

#[test]
fn roundtrip_st_chunk_empty() {
    roundtrip(&Msg::StChunk {
        cache: SmolStr::new("users"),
        recs: Vec::new(),
        done: true,
    });
}

#[test]
fn roundtrip_ae_digest() {
    roundtrip(&Msg::AeDigest {
        cache: SmolStr::new("users"),
        buckets: vec![(0, 111), (1023, 222)],
    });
}

#[test]
fn roundtrip_ae_bucket() {
    roundtrip(&Msg::AeBucket {
        cache: SmolStr::new("users"),
        bucket: 42,
        entries: vec![(Bytes::from_static(b"k1"), sample_hlc())],
    });
}

#[test]
fn roundtrip_ae_pull() {
    roundtrip(&Msg::AePull {
        cache: SmolStr::new("users"),
        keys: vec![Bytes::from_static(b"k1"), Bytes::from_static(b"k2")],
    });
}

/// `Cell`'s fields are private; this builds one through `Iblt`'s
/// `pub(crate)` API instead of a struct literal.
fn sample_cells() -> Vec<Cell> {
    let mut iblt = crate::sketch::Iblt::new(6);
    iblt.insert(1, sample_hlc());
    iblt.into_cells()
}

#[test]
fn roundtrip_ae_sketch() {
    roundtrip(&Msg::AeSketch {
        cache: SmolStr::new("users"),
        bucket: 42,
        cells: sample_cells(),
    });
}

#[test]
fn roundtrip_ae_sketch_empty_cells() {
    roundtrip(&Msg::AeSketch {
        cache: SmolStr::new("users"),
        bucket: 0,
        cells: Vec::new(),
    });
}

#[test]
fn roundtrip_ae_entries() {
    roundtrip(&Msg::AeEntries {
        cache: SmolStr::new("users"),
        buckets: vec![0, 512, 1023],
    });
}

#[test]
fn roundtrip_ae_pull_hashes() {
    roundtrip(&Msg::AePullHashes {
        cache: SmolStr::new("users"),
        bucket: 7,
        hashes: vec![1, 2, u64::MAX],
    });
}

#[test]
fn roundtrip_ae_part_digests() {
    roundtrip(&Msg::AePartDigests {
        cache: SmolStr::new("users"),
        bucket: 42,
        digests: (0..64u64).collect(),
    });
}

#[test]
fn roundtrip_ae_parts() {
    roundtrip(&Msg::AeParts {
        cache: SmolStr::new("users"),
        parts: vec![(0, 0), (42, 7), (1023, 63)],
    });
}

#[test]
fn roundtrip_ae_part() {
    roundtrip(&Msg::AePart {
        cache: SmolStr::new("users"),
        bucket: 42,
        part: 7,
        entries: vec![(Bytes::from_static(b"k1"), sample_hlc())],
    });
}

#[test]
fn roundtrip_ae_part_sketch() {
    roundtrip(&Msg::AePartSketch {
        cache: SmolStr::new("users"),
        bucket: 42,
        part: 7,
        cells: sample_cells(),
    });
}

#[test]
fn roundtrip_st_unavailable() {
    roundtrip(&Msg::StUnavailable {
        cache: SmolStr::new("users"),
    });
}

#[test]
fn roundtrip_fetch() {
    roundtrip(&Msg::Fetch {
        cache: SmolStr::new("users"),
        key: Bytes::from_static(b"k1"),
        view_hash: 12345,
    });
}

#[test]
fn roundtrip_fetch_reply_found() {
    roundtrip(&Msg::FetchReply {
        rec: Some(sample_record(Some("v1"))),
    });
}

#[test]
fn roundtrip_fetch_reply_miss() {
    roundtrip(&Msg::FetchReply { rec: None });
    roundtrip(&Msg::FetchDeclined {
        cache: SmolStr::new("prices"),
    });
}

#[test]
fn roundtrip_load_and_its_replies() {
    roundtrip(&Msg::Load {
        cache: SmolStr::new("prices"),
        keys: vec![Bytes::from_static(b"k1"), Bytes::from_static(b"k2")],
    });
    roundtrip(&Msg::Loaded {
        found: vec![sample_record(Some("v1"))],
        uncached: vec![(Bytes::from_static(b"k2"), Bytes::from_static(b"v2"))],
    });
    roundtrip(&Msg::Loaded {
        found: Vec::new(),
        uncached: Vec::new(),
    });
    roundtrip(&Msg::LoadFailed {
        cache: SmolStr::new("prices"),
        message: "source unavailable".to_string(),
    });
    roundtrip(&Msg::LoadDeclined {
        cache: SmolStr::new("prices"),
    });
}

#[test]
fn roundtrip_refresh_hint() {
    roundtrip(&Msg::RefreshHint {
        cache: SmolStr::new("prices"),
        key: Bytes::from_static(b"k1"),
        ver: sample_record(Some("v1")).ver,
    });
}

#[test]
fn a_protocol_6_peer_supports_no_load_message() {
    assert!(!peer_supports(6, PROTOCOL_LOAD));
    assert!(peer_supports(PROTOCOL_VERSION, PROTOCOL_LOAD));
}

#[test]
fn a_protocol_8_peer_supports_no_chunked_ae_listing() {
    const { assert!(PROTOCOL_VERSION >= PROTOCOL_AE_LISTING_CHUNKS) };
    assert!(!peer_supports(7, PROTOCOL_AE_LISTING_CHUNKS));
    assert!(!peer_supports(8, PROTOCOL_AE_LISTING_CHUNKS));
    assert!(peer_supports(9, PROTOCOL_AE_LISTING_CHUNKS));
    assert!(peer_supports(PROTOCOL_VERSION, PROTOCOL_AE_LISTING_CHUNKS));
}

/// The protocol numbers are an interop contract with released nodes.
#[test]
fn the_ae_listing_chunks_protocol_is_9() {
    assert_eq!(PROTOCOL_AE_LISTING_CHUNKS, 9);
}

/// An entry's non-key bytes never exceed [`AE_ENTRY_MAX_WIRE_LEN`], even
/// with every varint at its widest and a key whose length varint crosses
/// a width boundary.
#[test]
fn ae_entry_max_wire_len_bounds_a_real_encode_at_the_widest_varints() {
    let widest = Hlc {
        wall_ms: u64::MAX,
        logical: u32::MAX,
        node: NodeId::from(u64::MAX),
    };
    let cache = SmolStr::new("c");
    let empty = encode(&Msg::AeBucket {
        cache: cache.clone(),
        bucket: 0,
        entries: Vec::new(),
    })
    .expect("encodes")
    .len();
    for key_len in [0usize, 1, 127, 128, 16_383, 16_384, 70_000] {
        let encoded = encode(&Msg::AeBucket {
            cache: cache.clone(),
            bucket: 0,
            entries: vec![(Bytes::from(vec![7u8; key_len]), widest)],
        })
        .expect("encodes")
        .len();
        let growth = encoded - empty;
        assert!(
            growth <= key_len + AE_ENTRY_MAX_WIRE_LEN,
            "key of {key_len} bytes grows the frame by {growth}"
        );
    }
    // The version's three varints take their full width.
    let one = encode(&Msg::AeBucket {
        cache,
        bucket: 0,
        entries: vec![(Bytes::new(), widest)],
    })
    .expect("encodes")
    .len();
    assert_eq!(one - empty, 1 + 10 + 5 + 10, "length byte plus the version");
}

/// An empty listing's frame with the longest cache name leaves almost
/// all of the frame for entries.
#[test]
fn an_ae_bucket_frame_envelope_stays_under_three_hundred_bytes() {
    let envelope = encode(&Msg::AeBucket {
        cache: SmolStr::new("c".repeat(255)),
        bucket: u16::MAX,
        entries: Vec::new(),
    })
    .expect("encodes")
    .len();
    assert!(envelope < 300, "{envelope}");
}

#[test]
fn a_protocol_7_peer_supports_no_merged_invalidation_record() {
    const { assert!(PROTOCOL_VERSION >= PROTOCOL_INVALIDATION_FOLD) };
    assert!(!peer_supports(7, PROTOCOL_INVALIDATION_FOLD));
    assert!(!peer_supports(PROTOCOL_LOAD, PROTOCOL_INVALIDATION_FOLD));
    assert!(peer_supports(8, PROTOCOL_INVALIDATION_FOLD));
    assert!(peer_supports(PROTOCOL_VERSION, PROTOCOL_INVALIDATION_FOLD));
}

/// The protocol numbers are an interop contract with released nodes: a
/// peer on protocol 7 gets no merged record, and this build speaks the
/// fold protocol.
#[test]
fn the_invalidation_fold_protocol_is_8_and_this_build_speaks_it() {
    assert_eq!(PROTOCOL_INVALIDATION_FOLD, 8);
    const { assert!(PROTOCOL_VERSION >= PROTOCOL_INVALIDATION_FOLD) };
}

#[test]
fn roundtrip_ae_digest_scoped() {
    roundtrip(&Msg::AeDigestScoped {
        cache: SmolStr::new("users"),
        view_hash: 999,
        buckets: vec![(0, 111), (1023, 222)],
    });
}

#[test]
fn roundtrip_ae_digest_scoped_empty_buckets() {
    roundtrip(&Msg::AeDigestScoped {
        cache: SmolStr::new("users"),
        view_hash: 0,
        buckets: Vec::new(),
    });
}

#[test]
fn roundtrip_st_buckets() {
    roundtrip(&Msg::StBuckets {
        cache: SmolStr::new("users"),
        buckets: vec![0, 42, 1023],
        view_hash: 777,
    });
}

#[test]
fn roundtrip_st_bucket_chunk() {
    roundtrip(&Msg::StBucketChunk {
        cache: SmolStr::new("users"),
        recs: vec![sample_record(Some("v1")), sample_record(None)],
        done: true,
    });
}

#[test]
fn roundtrip_st_bucket_chunk_empty() {
    roundtrip(&Msg::StBucketChunk {
        cache: SmolStr::new("users"),
        recs: Vec::new(),
        done: false,
    });
}

#[test]
fn st_bucket_done_ack_roundtrips() {
    roundtrip(&Msg::StBucketDone {
        cache: SmolStr::new("users"),
        bucket: 42,
    });
}

#[test]
fn ae_digest_masked_roundtrips() {
    roundtrip(&Msg::AeDigestMasked {
        cache: SmolStr::new("users"),
        view_hash: 999,
        buckets: vec![(0, u64::MAX, 111), (1023, 1 << 63 | 1, 222)],
    });
}

#[test]
fn st_bucket_ack_roundtrips() {
    roundtrip(&Msg::StBucketAck {
        cache: SmolStr::new("users"),
        bucket: 1023,
        view_hash: 0xDEAD_BEEF,
    });
}

#[test]
fn a_protocol_three_peer_never_supports_st_bucket_done_ack() {
    assert!(!peer_supports(3, PROTOCOL_ST_BUCKET_DONE_ACK));
    assert!(!peer_supports(2, PROTOCOL_ST_BUCKET_DONE_ACK));
    assert!(!peer_supports(1, PROTOCOL_ST_BUCKET_DONE_ACK));
    assert!(peer_supports(4, PROTOCOL_ST_BUCKET_DONE_ACK));
}

#[test]
fn part_ownership_needs_protocol_5() {
    const { assert!(PROTOCOL_VERSION >= PROTOCOL_PART_OWNERSHIP) };
    assert!(peer_supports(5, PROTOCOL_PART_OWNERSHIP));
    assert!(!peer_supports(4, PROTOCOL_PART_OWNERSHIP));
    assert!(!peer_supports(3, PROTOCOL_PART_OWNERSHIP));
}

#[test]
fn roundtrip_stale_view() {
    roundtrip(&Msg::StaleView {
        cache: SmolStr::new("users"),
        responder_view_hash: 42,
    });
}

#[test]
fn roundtrip_forward_batch() {
    roundtrip(&Msg::ForwardBatch {
        cache: SmolStr::new("users"),
        view_hash: 0xDEAD_BEEF_CAFE_F00D,
        hops: 1,
        recs: vec![sample_record(Some("v1")), sample_record(None)],
    });
}

#[test]
fn roundtrip_forward_batch_empty() {
    roundtrip(&Msg::ForwardBatch {
        cache: SmolStr::new("users"),
        view_hash: 0,
        hops: 0,
        recs: Vec::new(),
    });
}

/// A [`Msg::ForwardBatch`] frame is the raw-record layout plus its
/// forward header, never postcard, and its `msg_kind` keeps it apart
/// from a [`Msg::ReplicateBatch`] carrying the same records.
#[test]
fn forward_batch_is_a_raw_record_frame_of_its_own_kind() {
    let recs = vec![sample_record(Some("v1"))];
    let forward = encode(&Msg::ForwardBatch {
        cache: SmolStr::new("users"),
        view_hash: 7,
        hops: 0,
        recs: recs.clone(),
    })
    .expect("encodes");
    assert_eq!(forward[0], FRAME_KIND_RAW_RECORD);
    let replicate = encode(&Msg::ReplicateBatch {
        cache: SmolStr::new("users"),
        recs,
    })
    .expect("encodes");
    assert_eq!(
        forward.len(),
        replicate.len() + size_of::<ForwardHeader>(),
        "the forward header is the only extra byte cost"
    );
    assert!(matches!(
        decode(&forward).expect("decodes"),
        Msg::ForwardBatch {
            view_hash: 7,
            hops: 0,
            ..
        }
    ));
    assert!(matches!(
        decode(&replicate).expect("decodes"),
        Msg::ReplicateBatch { .. }
    ));
}

/// A [`Msg::ForwardBatch`] frame cut off inside its forward header is
/// a malformed frame, not a panic.
#[test]
fn forward_batch_truncated_inside_the_forward_header_is_malformed() {
    let frame = encode(&Msg::ForwardBatch {
        cache: SmolStr::new("users"),
        view_hash: 7,
        hops: 0,
        recs: Vec::new(),
    })
    .expect("encodes");
    let truncated = frame.slice(..frame.len() - 1);
    assert!(matches!(
        decode(&truncated),
        Err(CodecError::MalformedFrame("forward header truncated"))
    ));
}

/// [`Msg::StBucketChunk`] shares the raw-record layout with
/// [`Msg::StChunk`] but is its own `msg_kind`, so a chunk of one never
/// decodes as the other.
#[test]
fn st_bucket_chunk_and_st_chunk_do_not_cross_decode() {
    let bucket_chunk = encode(&Msg::StBucketChunk {
        cache: SmolStr::new("users"),
        recs: vec![sample_record(Some("v1"))],
        done: true,
    })
    .expect("encodes");
    assert!(matches!(
        decode(&bucket_chunk).expect("decodes"),
        Msg::StBucketChunk { .. }
    ));

    let st_chunk = encode(&Msg::StChunk {
        cache: SmolStr::new("users"),
        recs: vec![sample_record(Some("v1"))],
        done: true,
    })
    .expect("encodes");
    assert!(matches!(
        decode(&st_chunk).expect("decodes"),
        Msg::StChunk { .. }
    ));
}

/// A [`Msg::StBucketChunk`] frame's raw-record payload decodes a
/// key/value `Bytes` sharing the frame's backing storage, the same
/// zero-copy guarantee [`Msg::Replicate`] already has.
#[test]
fn st_bucket_chunk_record_bytes_are_zero_copy_slices_of_the_frame() {
    let cache = SmolStr::new("users");
    let rec = sample_record(Some("v1"));
    let frame = encode(&Msg::StBucketChunk {
        cache: cache.clone(),
        recs: vec![rec.clone()],
        done: true,
    })
    .expect("encodes");
    let key_offset = 1 + size_of::<RawFrameHeader>() + cache.len() + RECORD_HEADER_LEN;
    let expected_key_ptr = frame.as_ptr().wrapping_add(key_offset);

    let Msg::StBucketChunk { mut recs, .. } = decode(&frame).expect("decodes") else {
        panic!("decoded back to something other than StBucketChunk");
    };
    assert_eq!(recs.len(), 1);
    let decoded_rec = recs.remove(0);
    assert_eq!(
        decoded_rec.key.as_ptr(),
        expected_key_ptr,
        "decoded key must be a zero-copy slice into the received frame"
    );
}

#[test]
fn roundtrip_ae_part_sketch_empty_cells() {
    roundtrip(&Msg::AePartSketch {
        cache: SmolStr::new("users"),
        bucket: 0,
        part: 0,
        cells: Vec::new(),
    });
}

#[test]
fn roundtrip_replicate_batch() {
    roundtrip(&Msg::ReplicateBatch {
        cache: SmolStr::new("users"),
        recs: vec![sample_record(Some("v1")), sample_record(None)],
    });
}

#[test]
fn roundtrip_req_done() {
    roundtrip(&Msg::ReqDone);
}

#[test]
fn oversized_frame_is_rejected_on_decode() {
    let oversized = Bytes::from(vec![0u8; MAX_FRAME + 1]);
    assert!(matches!(
        decode(&oversized),
        Err(CodecError::FrameTooLarge { .. })
    ));
}

#[test]
fn empty_frame_is_rejected_on_decode() {
    assert!(matches!(
        decode(&Bytes::new()),
        Err(CodecError::MalformedFrame(_))
    ));
}

#[test]
fn unknown_frame_discriminant_is_rejected() {
    assert!(matches!(
        decode(&Bytes::from_static(&[0xff])),
        Err(CodecError::MalformedFrame(_))
    ));
}

#[test]
fn truncated_raw_record_frame_is_rejected_not_panicking() {
    let full = encode(&Msg::Replicate {
        cache: SmolStr::new("users"),
        rec: sample_record(Some("v1")),
    })
    .expect("encodes");
    for cut in 1..full.len() {
        let truncated = full.slice(0..cut);
        assert!(
            matches!(decode(&truncated), Err(CodecError::MalformedFrame(_))),
            "truncating a raw-record frame to {cut} bytes must error, never panic"
        );
    }
}

/// A [`Msg::Replicate`] frame's raw-record payload decodes a key/value
/// `Bytes` sharing the frame's backing storage at its exact expected
/// offset, never copying it.
#[test]
fn decoded_record_bytes_are_zero_copy_slices_of_the_frame() {
    let cache = SmolStr::new("users");
    let rec = sample_record(Some("v1"));
    let key_len = rec.key.len();
    let frame = encode(&Msg::Replicate {
        cache: cache.clone(),
        rec,
    })
    .expect("encodes");
    let key_offset = 1 + size_of::<RawFrameHeader>() + cache.len() + RECORD_HEADER_LEN;
    let expected_key_ptr = frame.as_ptr().wrapping_add(key_offset);
    let expected_value_ptr = frame.as_ptr().wrapping_add(key_offset + key_len);

    let Msg::Replicate { rec, .. } = decode(&frame).expect("decodes") else {
        panic!("decoded back to something other than Replicate");
    };
    assert_eq!(
        rec.key.as_ptr(),
        expected_key_ptr,
        "decoded key must be a zero-copy slice into the received frame"
    );
    assert_eq!(
        rec.value.expect("value present").as_ptr(),
        expected_value_ptr,
        "decoded value must be a zero-copy slice into the received frame"
    );
}

#[test]
fn ae_sketch_frame_max_len_bounds_a_real_encode_from_above_and_tightly() {
    // Every cell XOR-accumulates well-spread elements, so each field
    // encodes as a varint of a random full-width integer.
    let spread = |i: u64| i.wrapping_add(1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let mut iblt = crate::sketch::Iblt::new(240);
    for i in 0..2_000u64 {
        iblt.insert(
            spread(i),
            Hlc {
                wall_ms: spread(i ^ 0x55),
                logical: u32::try_from(spread(i ^ 0xAA) >> 32).expect("fits"),
                node: NodeId::from(spread(i ^ 0xFF)),
            },
        );
    }
    let cache = SmolStr::new("users");
    let actual = encode(&Msg::AeSketch {
        cache: cache.clone(),
        bucket: u16::MAX,
        cells: iblt.into_cells(),
    })
    .expect("encodes")
    .len();
    let bound = ae_sketch_frame_max_len(cache.len(), 240);
    assert!(actual <= bound, "{actual} bytes on the wire, bound {bound}");
    assert!(
        actual * 10 >= bound * 8,
        "the bound stays within a quarter of the real size: {actual} vs {bound}"
    );
    assert_eq!(ae_sketch_frame_max_len(255, usize::MAX), usize::MAX);
}

#[test]
fn replicate_frame_len_matches_a_real_encode() {
    let rec = sample_record(Some("v1"));
    let cache = SmolStr::new("users");
    let predicted = replicate_frame_len(
        cache.len(),
        rec.key.len(),
        rec.value.as_ref().map_or(0, Bytes::len),
    );
    let actual = encode(&Msg::Replicate { cache, rec })
        .expect("encodes")
        .len();
    assert_eq!(predicted, actual);
}

#[test]
fn forward_frame_len_matches_a_real_encode_and_is_one_header_wider_than_a_replicate() {
    let rec = sample_record(Some("v1"));
    let cache = SmolStr::new("users");
    let (key_len, value_len) = (rec.key.len(), rec.value.as_ref().map_or(0, Bytes::len));
    let predicted = forward_frame_len(cache.len(), key_len, value_len);
    let actual = encode(&Msg::ForwardBatch {
        cache: cache.clone(),
        view_hash: 7,
        hops: 1,
        recs: vec![rec],
    })
    .expect("encodes")
    .len();
    assert_eq!(predicted, actual);
    assert_eq!(
        predicted - replicate_frame_len(cache.len(), key_len, value_len),
        9,
        "a view hash of eight bytes and a hop count of one"
    );
}

#[test]
fn a_record_within_nine_bytes_of_the_limit_fits_a_replicate_and_not_a_forward() {
    let cache = SmolStr::new("users");
    let key = Bytes::from_static(b"k");
    let fixed = replicate_frame_len(cache.len(), key.len(), 0);
    let rec = |value_len: usize| WireRecord {
        key: key.clone(),
        value: Some(Bytes::from(vec![0u8; value_len])),
        ver: sample_hlc(),
        expires_at_ms: None,
    };
    let forward = |value_len: usize| Msg::ForwardBatch {
        cache: cache.clone(),
        view_hash: 7,
        hops: 1,
        recs: vec![rec(value_len)],
    };
    let widest = MAX_FRAME - fixed;

    assert_eq!(
        encode(&Msg::Replicate {
            cache: cache.clone(),
            rec: rec(widest),
        })
        .expect("a replicate of the widest value")
        .len(),
        MAX_FRAME
    );
    assert!(matches!(
        encode(&forward(widest)),
        Err(CodecError::FrameTooLarge { .. })
    ));
    assert_eq!(
        encode(&forward(widest - 9))
            .expect("a forward of the widest value that fits")
            .len(),
        MAX_FRAME
    );
    assert_eq!(
        forward_frame_len(cache.len(), key.len(), widest - 9),
        MAX_FRAME
    );
}

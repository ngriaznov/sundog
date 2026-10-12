//! State-transfer and bucket-stream request tests.

use super::*;

#[tokio::test]
async fn state_transfer_roundtrip() {
    let records = vec![sample_record(1), sample_record(2)];
    let handler = Arc::new(FixtureHandler {
        records: records.clone(),
        digests: Vec::new(),
        bucket_entries: Vec::new(),
        pulled: Mutex::new(Vec::new()),
        ..Default::default()
    });
    let (donor, _donor_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (requester, _req_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    requester.update_peers(vec![peer_at(NodeId::from(1), donor.local_addr())]);

    let mut stream = requester
        .request_state(NodeId::from(1), SmolStr::new("users"))
        .await
        .expect("request accepted")
        .expect("a warm donor streams");
    let mut got = Vec::new();
    while let Some(chunk) = stream.next().await {
        got.extend(chunk.expect("chunk decodes"));
    }
    assert_eq!(got, records);
}

#[tokio::test]
async fn request_state_reports_a_donor_that_declines_and_keeps_its_connection() {
    let handler = Arc::new(FixtureHandler {
        records: vec![sample_record(1)],
        snapshot_unavailable: true,
        ..Default::default()
    });
    let (donor, _donor_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (requester, _req_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    requester.update_peers(vec![peer_at(NodeId::from(1), donor.local_addr())]);

    for attempt in 0..2 {
        let declined = requester
            .request_state(NodeId::from(1), SmolStr::new("users"))
            .await
            .expect("request accepted");
        assert!(
            declined.is_none(),
            "attempt {attempt}: a donor that is not warm declines instead of streaming"
        );
    }
    // The declined connection went back to the pool: an unrelated
    // request on the same peer still works.
    let digests = requester
        .ae_round(NodeId::from(1), SmolStr::new("users"), Vec::new())
        .await
        .expect("the pooled connection serves the next request");
    assert!(digests.is_empty(), "{digests:?}");
}

#[tokio::test]
async fn state_transfer_stream_reports_an_error_on_truncated_connection() {
    // A hand-rolled "donor" that drops the connection mid-stream,
    // without a `done: true` chunk: the requester must see an error.
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("listener has a local addr");

    let donor = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut framed = LengthDelimitedCodec::builder()
            .max_frame_length(MAX_FRAME)
            .new_framed(stream);
        let _hello = framed.next().await.expect("hello arrives");
        let _request = framed.next().await.expect("st request arrives");
        let chunk = Msg::StChunk {
            cache: SmolStr::new("users"),
            recs: vec![sample_record(1)],
            done: false,
        };
        let encoded = wire::encode(&chunk).expect("encodes");
        futures::SinkExt::send(&mut framed, encoded)
            .await
            .expect("send partial chunk");
        drop(framed); // connection closes: no `done: true` chunk ever sent
    });

    let (requester, _req_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    requester.update_peers(vec![peer_at(NodeId::from(1), addr)]);

    let mut stream = requester
        .request_state(NodeId::from(1), SmolStr::new("users"))
        .await
        .expect("request accepted")
        .expect("a warm donor streams");
    let first = stream.next().await.expect("first chunk arrives");
    assert_eq!(first.expect("decodes"), vec![sample_record(1)]);
    let second = stream
        .next()
        .await
        .expect("stream ends with an error, not silently");
    assert!(second.is_err(), "truncated stream must surface as an error");

    donor.await.expect("donor did not panic");
}

#[tokio::test]
async fn request_buckets_streams_multiple_chunks_to_done() {
    let chunk_a = vec![sample_record(1)];
    let chunk_b = vec![sample_record(2), sample_record(3)];
    let handler = Arc::new(FixtureHandler {
        buckets_available: true,
        bucket_chunks: vec![chunk_a.clone(), chunk_b.clone()],
        ..Default::default()
    });
    let (donor, _donor_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (requester, _req_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    requester.update_peers(vec![peer_at(NodeId::from(1), donor.local_addr())]);

    let BucketPull::Stream(mut stream) = requester
        .request_buckets(NodeId::from(1), SmolStr::new("users"), vec![0, 1], 42)
        .await
        .expect("request accepted")
    else {
        panic!("an available donor streams");
    };
    let mut got = Vec::new();
    while let Some(item) = stream.next().await {
        if let BucketStreamItem::Chunk(recs) = item.expect("item decodes") {
            got.extend(recs);
        }
    }
    let mut expected = chunk_a;
    expected.extend(chunk_b);
    assert_eq!(got, expected);
}

#[tokio::test]
async fn request_buckets_yields_bucket_done_once_per_bucket_before_the_stream_ends() {
    let handler = Arc::new(FixtureHandler {
        buckets_available: true,
        bucket_chunks: vec![vec![sample_record(1)], vec![sample_record(2)]],
        bucket_chunk_tags: vec![0, 1],
        ..Default::default()
    });
    let (donor, _donor_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (requester, _req_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    requester.update_peers(vec![peer_at(NodeId::from(1), donor.local_addr())]);

    let BucketPull::Stream(mut stream) = requester
        .request_buckets(NodeId::from(1), SmolStr::new("users"), vec![0, 1], 42)
        .await
        .expect("request accepted")
    else {
        panic!("an available donor streams");
    };
    let mut dones = Vec::new();
    while let Some(item) = stream.next().await {
        if let BucketStreamItem::BucketDone(bucket) = item.expect("item decodes") {
            dones.push(bucket);
        }
    }
    assert_eq!(
        dones,
        vec![0, 1],
        "each bucket's done arrives once, in request order, ahead of the stream's own end"
    );
}

#[tokio::test]
async fn request_buckets_declines_when_the_donor_is_unavailable() {
    let handler = Arc::new(FixtureHandler {
        buckets_available: false,
        ownership_view_hash: Some(123),
        ..Default::default()
    });
    let (donor, _donor_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (requester, _req_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    requester.update_peers(vec![peer_at(NodeId::from(1), donor.local_addr())]);

    let declined = requester
        .request_buckets(NodeId::from(1), SmolStr::new("users"), vec![0], 999)
        .await
        .expect("request accepted");
    assert!(
        matches!(declined, BucketPull::Stale),
        "a donor whose view has diverged declines with StaleView"
    );
    // The declined connection went back to the pool: an unrelated
    // request on the same peer still works.
    let digests = requester
        .ae_round(NodeId::from(1), SmolStr::new("users"), Vec::new())
        .await
        .expect("the pooled connection serves the next request");
    assert!(digests.is_empty(), "{digests:?}");
}

#[tokio::test]
async fn request_buckets_declines_cold_when_the_donor_has_not_pulled_the_buckets() {
    let handler = Arc::new(FixtureHandler {
        buckets_available: true,
        buckets_cold: true,
        ownership_view_hash: Some(42),
        ..Default::default()
    });
    let (donor, _donor_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (requester, _req_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    requester.update_peers(vec![peer_at(NodeId::from(1), donor.local_addr())]);

    let declined = requester
        .request_buckets(NodeId::from(1), SmolStr::new("users"), vec![0], 42)
        .await
        .expect("request accepted");
    assert!(
        matches!(declined, BucketPull::Cold),
        "a donor that has not pulled a requested bucket declines as cold, not stale"
    );
    let digests = requester
        .ae_round(NodeId::from(1), SmolStr::new("users"), Vec::new())
        .await
        .expect("the pooled connection serves the next request");
    assert!(digests.is_empty(), "{digests:?}");
}

#[tokio::test]
async fn request_buckets_declines_against_the_default_handler_body() {
    let (donor, _donor_inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    let (requester, _req_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    requester.update_peers(vec![peer_at(NodeId::from(1), donor.local_addr())]);

    let declined = requester
        .request_buckets(NodeId::from(1), SmolStr::new("users"), vec![0], 1)
        .await
        .expect("request accepted even against a handler that never overrode it");
    assert!(
        matches!(declined, BucketPull::Stale),
        "the default RequestHandler::st_buckets_available body degrades to false"
    );
}

#[tokio::test]
async fn request_buckets_refuses_to_dial_a_peer_that_does_not_speak_distribution_mode() {
    let (donor, _donor_inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    let (requester, _req_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    requester.update_peers(vec![peer_at_protocol(
        NodeId::from(1),
        donor.local_addr(),
        wire::PROTOCOL_DISTRIBUTED - 1,
    )]);

    let result = requester
        .request_buckets(NodeId::from(1), SmolStr::new("users"), vec![0], 1)
        .await;
    assert!(
        result.is_err(),
        "a peer that does not speak PROTOCOL_DISTRIBUTED must never be dialed for a bucket pull"
    );
}

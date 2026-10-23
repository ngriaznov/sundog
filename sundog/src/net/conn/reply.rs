//! Requester-side reply readers and lazy streams.

#[cfg(doc)]
use super::serve::serve_st_buckets;

use std::sync::Arc;

use futures::SinkExt as _;
use smol_str::SmolStr;

use super::framing::{PeerFramed, feed_msg, recv_msg, wire_to_key_versions};
use super::pool::ReqPool;
use crate::error::CodecError;
use crate::net::{
    AeMismatch, AePartReply, AeRoundOutcome, BucketStreamItem, FetchOutcome, LoadOutcome,
};
use crate::store::KeyVersion;
use crate::wire::{self, Msg, WireRecord};

/// A checked-out or dialed connection: `framed`, its `pool`, and any
/// reply `acquire_conn` already peeked off a reused connection.
pub(in crate::net) type Reply = (PeerFramed, Arc<ReqPool>, Option<Result<Msg, CodecError>>);

/// Reads `reply` until [`Msg::ReqDone`], folding each message into `seed`
/// via `accumulate`. Checks in on success; drops the connection on error.
async fn collect_replies<T>(
    reply: Reply,
    what: &str,
    mut seed: T,
    mut accumulate: impl FnMut(Msg, &mut T),
) -> Result<T, CodecError> {
    let (mut framed, pool, mut first) = reply;
    loop {
        let received = match first.take() {
            Some(msg) => Some(msg),
            None => recv_msg(&mut framed).await,
        };
        match received {
            Some(Ok(Msg::ReqDone)) => {
                pool.checkin(framed);
                return Ok(seed);
            }
            Some(Ok(msg)) => accumulate(msg, &mut seed),
            Some(Err(err)) => return Err(err),
            None => return Err(unexpected_close(what)),
        }
    }
}

/// Reads `AeBucket` replies into a `(bucket, entries)` list, one element per
/// bucket in order of first appearance: a responder on
/// [`crate::wire::PROTOCOL_AE_LISTING_CHUNKS`] sends a large bucket as several
/// messages, which this concatenates in arrival order.
pub(in crate::net) async fn collect_ae_buckets(
    reply: Reply,
) -> Result<crate::store::BucketEntries, CodecError> {
    collect_replies(reply, "digest reply", Vec::new(), |msg, out| {
        if let Msg::AeBucket {
            bucket, entries, ..
        } = msg
        {
            merge_bucket_chunk(out, bucket, wire_to_key_versions(entries));
        }
    })
    .await
}

/// Appends `entries` to `out`'s element for `bucket`, or pushes a new element
/// when `out` holds none. Looks the bucket up rather than checking only the
/// last element, so interleaved chunks of two buckets never split one.
pub(super) fn merge_bucket_chunk(
    out: &mut crate::store::BucketEntries,
    bucket: u16,
    entries: Vec<KeyVersion>,
) {
    match out.iter_mut().find(|(id, _)| *id == bucket) {
        Some((_, existing)) => existing.extend(entries),
        None => out.push((bucket, entries)),
    }
}

/// Reads digest replies into one [`AeMismatch`] list, per [`push_mismatch`].
pub(in crate::net) async fn collect_ae_mismatches(
    reply: Reply,
) -> Result<Vec<AeMismatch>, CodecError> {
    collect_replies(reply, "digest reply", Vec::new(), push_mismatch).await
}

/// Folds one reply message into `out`. The `AeBucket` messages of one bucket
/// join into one [`AeMismatch::Bucket`] in arrival order, whatever else
/// arrives between them: a responder on
/// [`crate::wire::PROTOCOL_AE_LISTING_CHUNKS`] sends a bucket of large keys as
/// several.
pub(super) fn push_mismatch(msg: Msg, out: &mut Vec<AeMismatch>) {
    match msg {
        Msg::AeBucket {
            bucket, entries, ..
        } => push_bucket_listing(out, bucket, wire_to_key_versions(entries)),
        Msg::AeSketch { bucket, cells, .. } => out.push(AeMismatch::Sketch(bucket, cells)),
        Msg::AePartDigests {
            bucket, digests, ..
        } => out.push(AeMismatch::PartDigests(bucket, digests)),
        _ => {}
    }
}

/// Appends `entries` to the [`AeMismatch::Bucket`] `out` holds for `bucket`,
/// or pushes a new one when it holds none. Looks the bucket up rather than
/// checking only the last element, as [`merge_bucket_chunk`] does.
pub(super) fn push_bucket_listing(
    out: &mut Vec<AeMismatch>,
    bucket: u16,
    entries: Vec<KeyVersion>,
) {
    let held = out.iter_mut().find_map(|mismatch| match mismatch {
        AeMismatch::Bucket(id, held) if *id == bucket => Some(held),
        _ => None,
    });
    match held {
        Some(held) => held.extend(entries),
        None => out.push(AeMismatch::Bucket(bucket, entries)),
    }
}

/// Reads part replies into one [`AePartReply`] list.
pub(in crate::net) async fn collect_ae_part_replies(
    reply: Reply,
) -> Result<Vec<AePartReply>, CodecError> {
    collect_replies(reply, "part reply", Vec::new(), |msg, out| match msg {
        Msg::AePart {
            bucket,
            part,
            entries,
            ..
        } => out.push(AePartReply::Listing {
            bucket,
            part,
            entries: wire_to_key_versions(entries),
        }),
        Msg::AePartSketch {
            bucket,
            part,
            cells,
            ..
        } => out.push(AePartReply::Sketch {
            bucket,
            part,
            cells,
        }),
        _ => {}
    })
    .await
}

/// Reads `Replicate`/`ReplicateBatch` replies into one record list.
pub(in crate::net) async fn collect_pulled_records(
    reply: Reply,
) -> Result<Vec<crate::wire::WireRecord>, CodecError> {
    collect_replies(reply, "pull reply", Vec::new(), |msg, out| match msg {
        Msg::Replicate { rec, .. } => out.push(rec),
        Msg::ReplicateBatch { recs, .. } => out.extend(recs),
        _ => {}
    })
    .await
}

/// Reads a `FetchReply`/`FetchDeclined`/`StaleView` reply; a later one replaces an earlier one, since only a misbehaving peer sends more than one.
pub(in crate::net) async fn collect_fetch_reply(reply: Reply) -> Result<FetchOutcome, CodecError> {
    let outcome = collect_replies(reply, "fetch reply", None, |msg, out| {
        *out = match msg {
            Msg::FetchReply { rec } => Some(FetchOutcome::Found(rec)),
            Msg::FetchDeclined { .. } => Some(FetchOutcome::Declined),
            Msg::StaleView {
                responder_view_hash,
                ..
            } => Some(FetchOutcome::Stale {
                responder_view_hash,
            }),
            _ => return,
        };
    })
    .await?;
    outcome.ok_or_else(|| unexpected_close("fetch reply"))
}

/// Reads a `Load`'s replies: every `Loaded` message's records gathered, or
/// the responder's `LoadFailed` or `LoadDeclined`.
pub(in crate::net) async fn collect_load_reply(reply: Reply) -> Result<LoadOutcome, CodecError> {
    let outcome = collect_replies(reply, "load reply", None, |msg, out| match msg {
        Msg::Loaded { found, uncached } => match out {
            Some(LoadOutcome::Loaded {
                found: all_found,
                uncached: all_uncached,
            }) => {
                all_found.extend(found);
                all_uncached.extend(uncached);
            }
            _ => *out = Some(LoadOutcome::Loaded { found, uncached }),
        },
        Msg::LoadFailed { message, .. } => *out = Some(LoadOutcome::Failed(message)),
        Msg::LoadDeclined { .. } => *out = Some(LoadOutcome::Declined),
        _ => {}
    })
    .await?;
    outcome.ok_or_else(|| unexpected_close("load reply"))
}

/// Reads digest replies like [`collect_ae_mismatches`], but epoch-checked: the last `StaleView` seen wins over every mismatch.
pub(in crate::net) async fn collect_ae_round_scoped(
    reply: Reply,
) -> Result<AeRoundOutcome, CodecError> {
    type Seed = (Vec<AeMismatch>, Option<u64>);
    let accumulate = |msg, (out, stale): &mut Seed| match msg {
        Msg::StaleView {
            responder_view_hash,
            ..
        } => *stale = Some(responder_view_hash),
        other => push_mismatch(other, out),
    };
    let (mismatches, stale) =
        collect_replies(reply, "scoped digest reply", Seed::default(), accumulate).await?;
    Ok(match stale {
        Some(responder_view_hash) => AeRoundOutcome::Stale {
            responder_view_hash,
        },
        None => AeRoundOutcome::Mismatches(mismatches),
    })
}

fn unexpected_close(what: &str) -> CodecError {
    CodecError::Io(std::io::Error::new(
        std::io::ErrorKind::UnexpectedEof,
        format!("{what} connection closed before its terminating ReqDone"),
    ))
}

/// Adapts a request-state connection into a lazy stream of record chunks,
/// reading `StChunk`s off the wire only as the consumer polls, yielding
/// each `StChunk`'s records as one `Vec` at the donor's own chunk
/// boundaries. On the final `done: true` chunk, checks the connection back
/// into `pool`; any error path drops it instead.
pub(in crate::net) fn state_stream(
    framed: PeerFramed,
    pool: Arc<ReqPool>,
    first: Option<Result<Msg, CodecError>>,
) -> futures::stream::BoxStream<'static, Result<Vec<WireRecord>, CodecError>> {
    Box::pin(futures::stream::unfold(
        Some((framed, first)),
        move |state| {
            let pool = Arc::clone(&pool);
            async move {
                let (mut framed, mut pending) = state?;
                loop {
                    // `Mesh::request_state` already read the first message
                    // to tell a declined request from a stream; it is
                    // consumed here before the connection is read again.
                    let received = match pending.take() {
                        Some(first) => Some(first),
                        None => recv_msg(&mut framed).await,
                    };
                    match received {
                        Some(Ok(Msg::StChunk { recs, done, .. })) => {
                            if recs.is_empty() {
                                // The trailing marker chunk, or a no-op
                                // chunk: nothing to yield either way.
                                if done {
                                    pool.checkin(framed);
                                    return None;
                                }
                                continue;
                            }
                            if done {
                                pool.checkin(framed);
                                return Some((Ok(recs), None));
                            }
                            return Some((Ok(recs), Some((framed, None))));
                        }
                        Some(Ok(_)) => {} // unexpected message on this stream; keep reading
                        Some(Err(err)) => return Some((Err(err), None)),
                        // Surfacing this as an error, not a silent stream
                        // end, lets the retry logic tell a truncated
                        // transfer from a finished one.
                        None => {
                            let err = CodecError::Io(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "state-transfer connection closed before the final chunk",
                            ));
                            return Some((Err(err), None));
                        }
                    }
                }
            }
        },
    ))
}

/// How many [`Msg::StBucketAck`]s [`bucket_stream`] queues before a flush.
const ACK_FLUSH: usize = 256;

/// Adapts a rebalance bucket-pull connection into a lazy stream of
/// [`BucketStreamItem`]s: a `Chunk` for every [`Msg::StBucketChunk`], plus,
/// against a donor speaking at least [`wire::PROTOCOL_ST_BUCKET_DONE_ACK`],
/// a `BucketDone` for every [`Msg::StBucketDone`]. An older donor never
/// sends `BucketDone`, so this then yields only `Chunk`s, as before.
///
/// After yielding a `BucketDone`, the next poll queues a `Msg::StBucketAck`
/// for it (gated the same way), so the ack follows whatever the caller did
/// with that item. Queued acks flush every [`ACK_FLUSH`], before the next
/// chunk, and at the end. A caller that stops polling never sends the rest,
/// which [`serve_st_buckets`] tolerates.
pub(in crate::net) fn bucket_stream(
    framed: PeerFramed,
    pool: Arc<ReqPool>,
    first: Option<Result<Msg, CodecError>>,
    cache: SmolStr,
    donor_protocol: u16,
    view_hash: u64,
) -> futures::stream::BoxStream<'static, Result<BucketStreamItem, CodecError>> {
    let ack_gated = wire::peer_supports(donor_protocol, wire::PROTOCOL_ST_BUCKET_DONE_ACK);
    Box::pin(futures::stream::unfold(
        Some((framed, first, None::<u16>, 0usize)),
        move |state| {
            let pool = Arc::clone(&pool);
            let cache = cache.clone();
            async move {
                let (mut framed, mut pending, pending_ack, mut unflushed) = state?;
                if ack_gated && let Some(bucket) = pending_ack {
                    // Best-effort: a broken send surfaces on the read below.
                    // `view_hash` is this node's view when it requested the
                    // buckets; the donor drops the ack once its own view
                    // has moved past it. Buffered, and flushed every
                    // `ACK_FLUSH` acks, at the next chunk, or at the end.
                    let _ = feed_msg(
                        &mut framed,
                        &Msg::StBucketAck {
                            cache: cache.clone(),
                            bucket,
                            view_hash,
                        },
                    )
                    .await;
                    unflushed += 1;
                    if unflushed >= ACK_FLUSH {
                        let _ = framed.flush().await;
                        unflushed = 0;
                    }
                }
                loop {
                    let received = match pending.take() {
                        Some(first) => Some(first),
                        None => recv_msg(&mut framed).await,
                    };
                    if unflushed > 0 && !matches!(received, Some(Ok(Msg::StBucketDone { .. }))) {
                        let _ = framed.flush().await;
                        unflushed = 0;
                    }
                    match received {
                        Some(Ok(Msg::StBucketChunk { recs, done, .. })) => {
                            if recs.is_empty() {
                                if done {
                                    pool.checkin(framed);
                                    return None;
                                }
                                continue;
                            }
                            if done {
                                pool.checkin(framed);
                                return Some((Ok(BucketStreamItem::Chunk(recs)), None));
                            }
                            return Some((
                                Ok(BucketStreamItem::Chunk(recs)),
                                Some((framed, None, None, 0)),
                            ));
                        }
                        Some(Ok(Msg::StBucketDone { bucket, .. })) => {
                            return Some((
                                Ok(BucketStreamItem::BucketDone(bucket)),
                                Some((framed, None, Some(bucket), unflushed)),
                            ));
                        }
                        Some(Ok(_)) => {} // unexpected message on this stream; keep reading
                        Some(Err(err)) => return Some((Err(err), None)),
                        None => {
                            let err = CodecError::Io(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "rebalance bucket-transfer connection closed before the final chunk",
                            ));
                            return Some((Err(err), None));
                        }
                    }
                }
            }
        },
    ))
}

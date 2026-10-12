//! Responder-side handlers.

use std::collections::VecDeque;

use bytes::Bytes;
use futures::{FutureExt as _, StreamExt as _};
use smol_str::SmolStr;
use tokio_util::sync::CancellationToken;

use super::framing::{
    PeerFramed, key_versions_to_wire, recv_msg, send_batch_or_cancelled, send_or_cancelled,
};
use crate::net::{AeServeOutcome, LoadServe, MeshInner, RequestHandler};
use crate::node::NodeId;
use crate::store::{BucketDigest, BucketPart, BucketPartDigests, KeyVersion};
use crate::wire::{self, MAX_FRAME, Msg, WireRecord};

/// Serves one state-transfer request. Returns `true` when this connection
/// is done, `false` once the final `done: true` chunk
/// sent cleanly and the connection remains reusable.
pub(super) async fn serve_state_transfer(
    framed: &mut PeerFramed,
    cache: SmolStr,
    handler: &dyn RequestHandler,
    cancel: &CancellationToken,
    peer_protocol: u16,
) -> bool {
    if !handler.snapshot_available(cache.clone()) {
        if wire::peer_supports(peer_protocol, wire::PROTOCOL_ST_UNAVAILABLE) {
            tracing::debug!(%cache, "state transfer requested before this node is warm; declined");
            return send_or_cancelled(framed, &Msg::StUnavailable { cache }, cancel).await;
        }
        tracing::debug!(
            %cache,
            peer_protocol,
            "state transfer requested before this node is warm by a peer that cannot be declined; serving"
        );
    }
    let mut chunks = handler.snapshot_chunks(cache.clone());
    loop {
        let next = tokio::select! {
            biased;
            () = cancel.cancelled() => return true,
            next = chunks.next() => next,
        };
        let Some(recs) = next else { break };
        let msg = Msg::StChunk {
            cache: cache.clone(),
            recs,
            done: false,
        };
        if send_or_cancelled(framed, &msg, cancel).await {
            return true;
        }
    }
    send_or_cancelled(
        framed,
        &Msg::StChunk {
            cache,
            recs: Vec::new(),
            done: true,
        },
        cancel,
    )
    .await
}

/// The mismatched-bucket reply messages for one digest exchange, given the
/// responder's own `local` digests and the requester's `remote_buckets`:
/// shared by [`serve_ae_digest`] and [`serve_ae_digest_scoped`], whose only
/// difference is where `local` comes from. The three-tier responder rule: a
/// bucket whose local entry count exceeds `handler.ae_part_min_bucket()`
/// replies with its 64 part digests ([`Msg::AePartDigests`]) without ever
/// materializing its listing; a smaller mismatched bucket falls back to the
/// existing rule, an IBLT sketch past `handler.ae_sketch_min_bucket()`
/// entries or else the full [`Msg::AeBucket`] listing. Does not append the
/// trailing [`Msg::ReqDone`]; callers add it.
async fn ae_mismatch_replies(
    cache: &SmolStr,
    local: &std::collections::HashMap<u16, u64>,
    remote_buckets: Vec<(u16, u64)>,
    handler: &dyn RequestHandler,
    peer_protocol: u16,
) -> Vec<Msg> {
    let mismatched: Vec<u16> = remote_buckets
        .into_iter()
        .filter(|&(bucket, remote_digest)| {
            local.get(&bucket).copied().unwrap_or(0) != remote_digest
        })
        .map(|(bucket, _)| bucket)
        .collect();

    let mut replies: Vec<Msg> = Vec::new();
    if !mismatched.is_empty() {
        // A peer that predates part digests gets a listing or sketch for
        // every bucket, whatever its size.
        let part_min_bucket = if wire::peer_supports(peer_protocol, wire::PROTOCOL_PART_DIGESTS) {
            handler.ae_part_min_bucket()
        } else {
            usize::MAX
        };
        let lens: std::collections::HashMap<u16, usize> = handler
            .bucket_lens(cache.clone(), mismatched.clone())
            .await
            .into_iter()
            .map(|bl| (bl.bucket, bl.len))
            .collect();
        let (big, small): (Vec<u16>, Vec<u16>) = mismatched
            .into_iter()
            .partition(|bucket| lens.get(bucket).copied().unwrap_or(0) > part_min_bucket);

        if !big.is_empty() {
            replies.extend(
                handler
                    .part_digests(cache.clone(), big)
                    .await
                    .into_iter()
                    .map(|bpd| Msg::AePartDigests {
                        cache: cache.clone(),
                        bucket: bpd.bucket,
                        digests: bpd.digests,
                    }),
            );
        }
        if !small.is_empty() {
            // One shard pass for every small mismatched bucket at once:
            // per-bucket scans would be quadratic against a
            // mostly-divergent peer. A bucket answered above with part
            // digests never reaches this call, so its listing is never
            // materialized.
            let min_bucket = handler.ae_sketch_min_bucket();
            let sketch_cells = handler.ae_sketch_cells();
            replies.extend(
                handler
                    .entries_for_buckets(cache.clone(), small)
                    .await
                    .into_iter()
                    .map(|(bucket, entries)| {
                        match listing_or_sketch(entries, min_bucket, sketch_cells) {
                            ListingOrSketch::Sketch(cells) => Msg::AeSketch {
                                cache: cache.clone(),
                                bucket,
                                cells,
                            },
                            ListingOrSketch::Listing(entries) => Msg::AeBucket {
                                cache: cache.clone(),
                                bucket,
                                entries: key_versions_to_wire(entries),
                            },
                        }
                    }),
            );
        }
    }
    replies
}

/// Serves one anti-entropy digest exchange, feeding every mismatched
/// bucket's reply plus a trailing [`Msg::ReqDone`] and flushing once.
/// Returns `true` when this connection is done.
pub(super) async fn serve_ae_digest(
    framed: &mut PeerFramed,
    cache: SmolStr,
    remote_buckets: Vec<(u16, u64)>,
    handler: &dyn RequestHandler,
    cancel: &CancellationToken,
    peer_protocol: u16,
) -> bool {
    let local: std::collections::HashMap<u16, u64> = handler
        .digests(cache.clone())
        .await
        .into_iter()
        .map(|bd| (bd.bucket, bd.digest))
        .collect();
    let mut replies =
        ae_mismatch_replies(&cache, &local, remote_buckets, handler, peer_protocol).await;
    replies.push(Msg::ReqDone);
    send_batch_or_cancelled(framed, &replies, cancel).await
}

/// Serves a `Fetch`: answers with the responder's record or a definitive
/// miss, with `StaleView` when it holds no record in a trusted part and its
/// own view differs from the requester's, or with `FetchDeclined`. Always
/// followed by [`Msg::ReqDone`], so the connection stays poolable.
pub(super) async fn serve_fetch(
    framed: &mut PeerFramed,
    cache: SmolStr,
    key: Bytes,
    view_hash: u64,
    handler: &dyn RequestHandler,
    cancel: &CancellationToken,
) -> bool {
    let reply = match handler.fetch(cache.clone(), key, view_hash).await {
        crate::net::FetchServe::Found(rec) => Msg::FetchReply { rec },
        crate::net::FetchServe::Stale {
            responder_view_hash,
        } => Msg::StaleView {
            cache,
            responder_view_hash,
        },
        // A responder that cannot vouch for a miss declines outright, so
        // the requester tries its next owner instead of taking `None` as
        // the answer.
        crate::net::FetchServe::Unavailable => Msg::FetchDeclined { cache },
    };
    send_batch_or_cancelled(framed, &[reply, Msg::ReqDone], cancel).await
}

/// Serves a `Load`: the handler loads the keys, and the answer goes back as
/// [`loaded_replies`] or one `LoadFailed`/`LoadDeclined`, then `ReqDone`.
pub(super) async fn serve_load(
    framed: &mut PeerFramed,
    cache: SmolStr,
    keys: Vec<Bytes>,
    handler: &dyn RequestHandler,
    cancel: &CancellationToken,
) -> bool {
    let mut replies = match handler.load(cache.clone(), keys).await {
        LoadServe::Loaded { found, uncached } => {
            loaded_replies(found, uncached, LOADED_REPLY_BUDGET)
        }
        LoadServe::Failed(message) => vec![Msg::LoadFailed { cache, message }],
        LoadServe::Unavailable => vec![Msg::LoadDeclined { cache }],
    };
    replies.push(Msg::ReqDone);
    send_batch_or_cancelled(framed, &replies, cancel).await
}

/// The bytes of keys and values one [`Msg::Loaded`] carries before
/// [`loaded_replies`] starts the next: half a frame, leaving room for the
/// records' fixed fields.
const LOADED_REPLY_BUDGET: usize = MAX_FRAME / 2;

/// Splits a load's answer into [`Msg::Loaded`] messages, each holding at
/// most `budget` bytes of keys and values unless one entry alone is
/// larger; always at least one message. Pure; unit tested directly.
pub(super) fn loaded_replies(
    found: Vec<WireRecord>,
    uncached: Vec<(Bytes, Bytes)>,
    budget: usize,
) -> Vec<Msg> {
    let mut replies = Vec::new();
    let mut current = (Vec::new(), Vec::new());
    let mut used = 0usize;
    let entries = found
        .into_iter()
        .map(Ok)
        .chain(uncached.into_iter().map(Err));
    for entry in entries {
        let len = match &entry {
            Ok(rec) => rec.key.len() + rec.value.as_ref().map_or(0, Bytes::len),
            Err((key, value)) => key.len() + value.len(),
        };
        if used > 0 && used + len > budget {
            let (found, uncached) = std::mem::take(&mut current);
            replies.push(Msg::Loaded { found, uncached });
            used = 0;
        }
        used += len;
        match entry {
            Ok(rec) => current.0.push(rec),
            Err(pair) => current.1.push(pair),
        }
    }
    if replies.is_empty() || !current.0.is_empty() || !current.1.is_empty() {
        let (found, uncached) = current;
        replies.push(Msg::Loaded { found, uncached });
    }
    replies
}

/// Serves an `AeDigestScoped`: an epoch check ahead of an ordinary digest
/// exchange. `handler.ae_digest_scoped` folds both together: its `Digests`
/// case is this responder's own owned-bucket digests, view hashes already
/// confirmed to match, fed into the same [`ae_mismatch_replies`] tiering
/// [`serve_ae_digest`] uses. A `Stale` or `Unavailable` answer short-circuits
/// before any bucket is ever touched.
pub(super) async fn serve_ae_digest_scoped(
    framed: &mut PeerFramed,
    cache: SmolStr,
    view_hash: u64,
    remote_buckets: Vec<(u16, u64)>,
    handler: &dyn RequestHandler,
    cancel: &CancellationToken,
    peer_protocol: u16,
) -> bool {
    let remote_bucket_digests: Vec<BucketDigest> = remote_buckets
        .iter()
        .map(|&(bucket, digest)| BucketDigest { bucket, digest })
        .collect();
    let outcome = handler
        .ae_digest_scoped(cache.clone(), view_hash, remote_bucket_digests)
        .await;
    let mut replies = match outcome {
        AeServeOutcome::Digests(local) => {
            let local: std::collections::HashMap<u16, u64> =
                local.into_iter().map(|bd| (bd.bucket, bd.digest)).collect();
            ae_mismatch_replies(&cache, &local, remote_buckets, handler, peer_protocol).await
        }
        declined => declined_replies(cache, &declined),
    };
    replies.push(Msg::ReqDone);
    send_batch_or_cancelled(framed, &replies, cancel).await
}

/// Serves an `AeDigestMasked`: each bucket whose masked digest differs
/// from this responder's own is answered with its part digests, for the
/// requester to compare part by part within its mask.
pub(super) async fn serve_ae_digest_masked(
    framed: &mut PeerFramed,
    cache: SmolStr,
    view_hash: u64,
    remote_buckets: Vec<(u16, u64, u64)>,
    handler: &dyn RequestHandler,
    cancel: &CancellationToken,
) -> bool {
    let outcome = handler
        .ae_digest_masked(cache.clone(), view_hash, remote_buckets)
        .await;
    let mut replies = match outcome {
        AeServeOutcome::Digests(mismatched) => mismatched
            .into_iter()
            .map(|BucketPartDigests { bucket, digests }| Msg::AePartDigests {
                cache: cache.clone(),
                bucket,
                digests,
            })
            .collect(),
        declined => declined_replies(cache, &declined),
    };
    replies.push(Msg::ReqDone);
    send_batch_or_cancelled(framed, &replies, cancel).await
}

/// What a scoped round the responder answered without digests replies
/// ahead of `ReqDone`: a `StaleView` when the views differ, else nothing.
pub(super) fn declined_replies<T>(cache: SmolStr, outcome: &AeServeOutcome<T>) -> Vec<Msg> {
    match *outcome {
        AeServeOutcome::Stale {
            responder_view_hash,
        } => vec![Msg::StaleView {
            cache,
            responder_view_hash,
        }],
        AeServeOutcome::Digests(_) | AeServeOutcome::Unavailable => Vec::new(),
    }
}

/// The most [`Msg::StBucketDone`] frames [`serve_st_buckets`] sends per
/// flush.
const DONE_BATCH: usize = 1024;

/// Sends a [`Msg::StBucketDone`] per id in `ids`, [`DONE_BATCH`] at a
/// time, draining acks via [`drain_ready_acks`] between batches: a donor
/// that only writes fills both directions' socket buffers. Returns `true`
/// when the connection is done.
async fn send_done_run(
    framed: &mut PeerFramed,
    cache: &SmolStr,
    ids: Vec<u16>,
    mesh: &MeshInner,
    from: NodeId,
    cancel: &CancellationToken,
) -> bool {
    for batch in ids.chunks(DONE_BATCH) {
        let dones: Vec<Msg> = batch
            .iter()
            .map(|&bucket| Msg::StBucketDone {
                cache: cache.clone(),
                bucket,
            })
            .collect();
        if send_batch_or_cancelled(framed, &dones, cancel).await
            || drain_ready_acks(framed, cache, mesh, from)
        {
            return true;
        }
    }
    false
}

/// Records every [`Msg::StBucketAck`] for `cache` already readable on
/// `framed`, without waiting for more. Returns `true` when the connection
/// broke.
fn drain_ready_acks(
    framed: &mut PeerFramed,
    cache: &SmolStr,
    mesh: &MeshInner,
    from: NodeId,
) -> bool {
    while let Some(Some(msg)) = recv_msg(framed).now_or_never() {
        match msg {
            Ok(Msg::StBucketAck {
                cache: acked,
                bucket,
                view_hash,
            }) if acked == *cache => mesh.record_bucket_ack(acked, bucket, from, view_hash),
            Ok(_) => {}
            Err(_) => return true,
        }
    }
    false
}

/// Serves an `StBuckets` rebalance pull: streams every chunk
/// [`RequestHandler::st_bucket_chunks`] yields once
/// [`RequestHandler::st_buckets_available`] agrees, the bucket-scoped
/// counterpart of [`serve_state_transfer`]; declines with `StaleView`
/// otherwise (an unrecognized cache, one not open in distribution mode, or
/// a view hash that has diverged from the requester's). Returns `true` when
/// this connection is done.
///
/// Against a requester speaking at least
/// [`wire::PROTOCOL_ST_BUCKET_DONE_ACK`], sends [`Msg::StBucketDone`] once a
/// bucket's chunks are exhausted, including buckets left empty locally, and
/// folds any [`Msg::StBucketAck`] read meanwhile into `mesh` via
/// [`MeshInner::record_bucket_ack`]. A missing ack or an older peer is not
/// an error: release then falls back to the timer/AE-round path.
#[allow(
    clippy::too_many_arguments,
    reason = "the bucket-done/ack wiring needs the serving peer's identity and protocol alongside every parameter serve_st_buckets already took"
)]
#[expect(
    clippy::too_many_lines,
    reason = "one connection's whole chunk-send/ack-read loop plus its tail drain reads best kept together"
)]
pub(super) async fn serve_st_buckets(
    framed: &mut PeerFramed,
    cache: SmolStr,
    buckets: Vec<u16>,
    view_hash: u64,
    handler: &dyn RequestHandler,
    mesh: &MeshInner,
    from: NodeId,
    peer_protocol: u16,
    cancel: &CancellationToken,
) -> bool {
    let requested = buckets.len();
    let (mut chunks, mut remaining, parts_requested) =
        match handler.st_serve(cache.clone(), buckets, view_hash).await {
            crate::net::StServe::Stale {
                responder_view_hash,
            } => {
                tracing::debug!(
                    cache = %cache,
                    requester_view_hash = view_hash,
                    responder_view_hash,
                    "st buckets pull declined: view hash mismatch"
                );
                return send_or_cancelled(
                    framed,
                    &Msg::StaleView {
                        cache,
                        responder_view_hash,
                    },
                    cancel,
                )
                .await;
            }
            crate::net::StServe::Cold => {
                // Owned but not yet pulled: not a source. The requester tries
                // its next donor.
                tracing::debug!(
                    cache = %cache,
                    requested,
                    "st buckets pull declined: requested buckets not yet warm locally"
                );
                return send_or_cancelled(framed, &Msg::StUnavailable { cache }, cancel).await;
            }
            crate::net::StServe::Stream {
                chunks,
                order,
                parts,
            } => (chunks, VecDeque::from(order), parts),
        };
    let signal_done = wire::peer_supports(peer_protocol, wire::PROTOCOL_ST_BUCKET_DONE_ACK);
    let mut chunks_sent: usize = 0;
    let mut records_sent: usize = 0;
    loop {
        let next = tokio::select! {
            biased;
            () = cancel.cancelled() => return true,
            ack = recv_msg(framed), if signal_done => {
                match ack {
                    Some(Ok(Msg::StBucketAck {
                        cache: ack_cache,
                        bucket,
                        view_hash,
                    })) => {
                        if ack_cache == cache {
                            mesh.record_bucket_ack(ack_cache, bucket, from, view_hash);
                        }
                        continue;
                    }
                    // An unrecognized message here is ignored, not fatal,
                    // like the reply-only arms in `dispatch_one`.
                    Some(Ok(_)) => continue,
                    Some(Err(ref error)) => {
                        tracing::debug!(
                            cache = %cache,
                            %error,
                            chunks_sent,
                            records_sent,
                            remaining = remaining.len(),
                            "st buckets pull ended early: requester sent an error"
                        );
                        return true;
                    }
                    None => {
                        tracing::debug!(
                            cache = %cache,
                            chunks_sent,
                            records_sent,
                            remaining = remaining.len(),
                            "st buckets pull ended early: connection closed"
                        );
                        return true;
                    }
                }
            }
            next = chunks.next() => next,
        };
        let Some((bucket, recs)) = next else {
            break;
        };
        if signal_done {
            // Every id ahead of this chunk's is finished: one flush for the
            // whole run, since a part-granular pull finishes thousands of
            // empty ids back to back.
            let mut done_run: Vec<u16> = Vec::new();
            while remaining.front().is_some_and(|&b| b != bucket) {
                done_run.push(
                    remaining
                        .pop_front()
                        .expect("invariant: the while guard just checked front() is Some"),
                );
            }
            if send_done_run(framed, &cache, done_run, mesh, from, cancel).await {
                tracing::debug!(
                    cache = %cache,
                    chunks_sent,
                    records_sent,
                    remaining = remaining.len(),
                    "st buckets pull ended early: connection closed sending StBucketDone"
                );
                return true;
            }
        }
        chunks_sent += 1;
        records_sent += recs.len();
        let msg = Msg::StBucketChunk {
            cache: cache.clone(),
            recs,
            done: false,
        };
        if send_or_cancelled(framed, &msg, cancel).await {
            tracing::debug!(
                cache = %cache,
                chunks_sent,
                records_sent,
                remaining = remaining.len(),
                "st buckets pull ended early: connection closed mid-stream"
            );
            return true;
        }
    }
    if signal_done {
        let done_run: Vec<u16> = remaining.drain(..).collect();
        let draining = done_run.len();
        if send_done_run(framed, &cache, done_run, mesh, from, cancel).await {
            tracing::debug!(
                cache = %cache,
                chunks_sent,
                records_sent,
                draining,
                "st buckets pull ended early: connection closed draining StBucketDone"
            );
            return true;
        }
    }
    // The donor-side counterpart of the requester's "in" direction.
    metrics::counter!(
        "sundog_rebalance_parts_total",
        "cache" => cache.to_string(),
        "direction" => "served"
    )
    .increment(parts_requested);
    tracing::debug!(
        cache = %cache,
        ids_served = requested,
        parts_served = parts_requested,
        chunks_sent,
        records_sent,
        signal_done,
        "st buckets pull stream ended"
    );
    send_or_cancelled(
        framed,
        &Msg::StBucketChunk {
            cache,
            recs: Vec::new(),
            done: true,
        },
        cancel,
    )
    .await
}

/// Serves an `AeParts` request: the part-grained counterpart of
/// [`serve_ae_digest`]'s classification step, one reply per requested
/// `(bucket, part)` pair, a part past `handler.ae_sketch_min_bucket()`
/// entries answered with [`Msg::AePartSketch`], otherwise
/// [`Msg::AePart`]. Unlike `AeDigest`, never deferred by
/// `MeshInner::defers_ae_digest_from`: a part request only ever follows a
/// part-digest reply the requester already paid to compare.
pub(super) async fn serve_ae_parts(
    framed: &mut PeerFramed,
    cache: SmolStr,
    parts: Vec<(u16, u8)>,
    handler: &dyn RequestHandler,
    cancel: &CancellationToken,
) -> bool {
    let parts: Vec<BucketPart> = parts
        .into_iter()
        .map(|(bucket, part)| BucketPart { bucket, part })
        .collect();
    let mut replies = part_replies(&cache, parts, handler).await;
    replies.push(Msg::ReqDone);
    send_batch_or_cancelled(framed, &replies, cancel).await
}

/// Each of `parts`' [`Msg::AePart`] listing, or [`Msg::AePartSketch`] once
/// the listing would outweigh one.
async fn part_replies(
    cache: &SmolStr,
    parts: Vec<BucketPart>,
    handler: &dyn RequestHandler,
) -> Vec<Msg> {
    if parts.is_empty() {
        return Vec::new();
    }
    let min_bucket = handler.ae_sketch_min_bucket();
    let sketch_cells = handler.ae_sketch_cells();
    handler
        .entries_for_parts(cache.clone(), parts)
        .await
        .into_iter()
        .map(|(BucketPart { bucket, part }, entries)| {
            match listing_or_sketch(entries, min_bucket, sketch_cells) {
                ListingOrSketch::Sketch(cells) => Msg::AePartSketch {
                    cache: cache.clone(),
                    bucket,
                    part,
                    cells,
                },
                ListingOrSketch::Listing(entries) => Msg::AePart {
                    cache: cache.clone(),
                    bucket,
                    part,
                    entries: key_versions_to_wire(entries),
                },
            }
        })
        .collect()
}

/// A responder's shape for one mismatched bucket or part: its listing, or
/// an IBLT sketch once the listing would outweigh one.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum ListingOrSketch {
    Listing(Vec<KeyVersion>),
    Sketch(Vec<crate::wire::Cell>),
}

/// The one crossover rule for [`serve_ae_digest`] and [`serve_ae_parts`]:
/// more than `min_bucket` entries answer with a `sketch_cells`-cell sketch
/// over their `(key_hash, version)` pairs, anything up to it with the
/// listing itself.
pub(super) fn listing_or_sketch(
    entries: Vec<KeyVersion>,
    min_bucket: usize,
    sketch_cells: usize,
) -> ListingOrSketch {
    if entries.len() > min_bucket {
        let mut iblt = crate::cluster::sketch::Iblt::new(sketch_cells);
        for kv in &entries {
            iblt.insert(xxhash_rust::xxh3::xxh3_64(&kv.key), kv.version);
        }
        ListingOrSketch::Sketch(iblt.into_cells())
    } else {
        ListingOrSketch::Listing(entries)
    }
}

/// Serves an `AeEntries` request: the sketch fallback, full listings for the
/// named `buckets`, requested explicitly rather than chosen by the responder.
pub(super) async fn serve_ae_entries(
    framed: &mut PeerFramed,
    cache: SmolStr,
    buckets: Vec<u16>,
    handler: &dyn RequestHandler,
    cancel: &CancellationToken,
) -> bool {
    let mut replies: Vec<Msg> = if buckets.is_empty() {
        Vec::new()
    } else {
        handler
            .entries_for_buckets(cache.clone(), buckets)
            .await
            .into_iter()
            .map(|(bucket, entries)| Msg::AeBucket {
                cache: cache.clone(),
                bucket,
                entries: key_versions_to_wire(entries),
            })
            .collect()
    };
    replies.push(Msg::ReqDone);
    send_batch_or_cancelled(framed, &replies, cancel).await
}

pub(super) async fn serve_ae_pull(
    framed: &mut PeerFramed,
    cache: SmolStr,
    keys: Vec<Bytes>,
    handler: &dyn RequestHandler,
    cancel: &CancellationToken,
) -> bool {
    let records = handler.records_for(cache.clone(), keys).await;
    let mut replies = crate::net::batch_replicate(&cache, records);
    replies.push(Msg::ReqDone);
    send_batch_or_cancelled(framed, &replies, cancel).await
}

/// Serves an `AePullHashes` request, the sketch-decoded counterpart to
/// [`serve_ae_pull`]: records for `bucket`'s entries whose key hash is in `hashes`.
pub(super) async fn serve_ae_pull_hashes(
    framed: &mut PeerFramed,
    cache: SmolStr,
    bucket: u16,
    hashes: Vec<u64>,
    handler: &dyn RequestHandler,
    cancel: &CancellationToken,
) -> bool {
    let records = handler
        .records_for_hashes(cache.clone(), bucket, hashes)
        .await;
    let mut replies = crate::net::batch_replicate(&cache, records);
    replies.push(Msg::ReqDone);
    send_batch_or_cancelled(framed, &replies, cancel).await
}

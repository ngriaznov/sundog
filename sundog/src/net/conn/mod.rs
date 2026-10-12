//! Framing and connection tasks: the accept-side demux between persistent
//! mesh traffic and pooled request/response, and the per-peer dial-and-write
//! loop over the broadcast outboxes.

mod framing;
mod pool;
mod reply;
#[cfg(test)]
mod tests;
mod writer;

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{FutureExt as _, StreamExt as _};
use smol_str::SmolStr;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::tcp::{TcpListener, TcpStream};
use super::{AeServeOutcome, InboundMsg, LoadServe, MeshInner, RequestHandler};
use crate::error::CodecError;
use crate::node::NodeId;
use crate::store::{BucketDigest, BucketPart, BucketPartDigests, KeyVersion};
use crate::wire::{self, MAX_FRAME, Msg, WireRecord};
use framing::{
    PeerFramed, disable_nagle, establish_accept, key_versions_to_wire, new_framed,
    send_batch_or_cancelled, send_or_cancelled,
};
use pool::{REQ_CONN_IDLE_TIMEOUT, REQ_CONN_MAX_REQUESTS};

pub(super) use framing::{recv_msg, send_msg};
pub(super) use pool::{ReqPool, ReusedProbe, probe_reused};
pub(super) use reply::{
    Reply, bucket_stream, collect_ae_buckets, collect_ae_mismatches, collect_ae_part_replies,
    collect_ae_round_scoped, collect_fetch_reply, collect_load_reply, collect_pulled_records,
    state_stream,
};
pub(crate) use writer::{REPLICATE_BATCH_BUDGET, REPLICATE_BATCH_COUNT};
pub(super) use writer::{dial_with_hello_and, run_peer_writer};

/// Reads the next message off `framed`, bounded by `idle_timeout` when set.
/// Used only once a connection has served at least one request/response
/// exchange; a persistent broadcast link never goes through this path.
/// Timing out is treated like the peer closing the connection.
async fn recv_msg_or_idle_timeout(
    framed: &mut PeerFramed,
    idle_timeout: Option<Duration>,
) -> Option<Result<Msg, CodecError>> {
    match idle_timeout {
        Some(timeout) => tokio::time::timeout(timeout, recv_msg(framed))
            .await
            .unwrap_or(None),
        None => recv_msg(framed).await,
    }
}

/// Accepts connections until `mesh`'s accept token fires, spawning a handler
/// per connection.
pub(super) async fn accept_loop(
    listener: TcpListener,
    inbound_tx: mpsc::Sender<InboundMsg>,
    handler: Arc<dyn RequestHandler>,
    mesh: Arc<MeshInner>,
) {
    let cancel = mesh.accept_cancel.clone();
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            accepted = listener.accept() => {
                let Ok((stream, _peer_addr)) = accepted else { continue };
                disable_nagle(&stream);
                tokio::spawn(handle_accepted(
                    stream,
                    inbound_tx.clone(),
                    Arc::clone(&handler),
                    Arc::clone(&mesh),
                ));
            }
        }
    }
}

/// Serves one accepted connection: layers TLS on per `tls`, requires
/// `Hello` first, then dispatches each subsequent message.
/// `Invalidate`/`Replicate`/`ReplicateBatch` keep the connection open and
/// forward to `inbound_tx`. A request message is served and the connection
/// stays open for reuse, subject to [`REQ_CONN_IDLE_TIMEOUT`] and
/// [`REQ_CONN_MAX_REQUESTS`]. A failed serve or TLS handshake closes the
/// connection immediately, since it may be mid-frame and unsafe to reuse.
///
/// Every read and write below races against `mesh`'s accept token, tearing
/// a slow snapshot or AE reply down promptly on [`super::Mesh::shutdown`].
/// An `AeDigest` from a peer this node still has replicate frames queued
/// toward is answered empty (`MeshInner::defers_ae_digest_from`): the
/// stream delivers what a listing would, and the peer's next round catches
/// the rest.
async fn handle_accepted(
    stream: TcpStream,
    inbound_tx: mpsc::Sender<InboundMsg>,
    handler: Arc<dyn RequestHandler>,
    mesh: Arc<MeshInner>,
) {
    let cancel = mesh.accept_cancel.clone();
    let stream = match establish_accept(stream, &mesh.tls).await {
        Ok(stream) => stream,
        Err(error) => {
            tracing::debug!(%error, "TLS handshake failed on accept; dropping connection");
            return;
        }
    };
    let mut framed = new_framed(stream);
    let hello = tokio::select! {
        biased;
        () = cancel.cancelled() => return,
        hello = recv_msg(&mut framed) => hello,
    };
    let Some(Ok(Msg::Hello {
        node: from,
        protocol: peer_protocol,
        cluster: peer_cluster,
        ..
    })) = hello
    else {
        return;
    };
    // A peer of another cluster dialed an address this node now holds,
    // left over from a node of its own that held it before: nothing it
    // sends belongs in this cluster's caches.
    if !wire::same_cluster(mesh.cluster, peer_cluster) {
        tracing::warn!(
            peer = %from,
            "dropping a data-plane connection from another cluster's node"
        );
        return;
    }
    if peer_protocol > wire::PROTOCOL_VERSION {
        tracing::debug!(
            peer = %from,
            peer_protocol,
            "peer speaks a newer protocol; it limits itself to what this node understands"
        );
    }

    let mut served_requests: usize = 0;
    loop {
        // Only a connection that has served a request/response exchange is
        // a pooling candidate; a persistent broadcast link waits indefinitely.
        let idle_timeout = (served_requests > 0).then_some(REQ_CONN_IDLE_TIMEOUT);
        let received = tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            received = recv_msg_or_idle_timeout(&mut framed, idle_timeout) => received,
        };
        let Some(Ok(msg)) = received else {
            return;
        };
        let is_request = matches!(
            msg,
            Msg::StRequest { .. }
                | Msg::AeDigest { .. }
                | Msg::AePull { .. }
                | Msg::AeEntries { .. }
                | Msg::AePullHashes { .. }
                | Msg::AeParts { .. }
                | Msg::Fetch { .. }
                | Msg::AeDigestScoped { .. }
                | Msg::AeDigestMasked { .. }
                | Msg::StBuckets { .. }
                | Msg::Load { .. }
        );
        let stop = dispatch_one(
            msg,
            from,
            peer_protocol,
            &mut framed,
            &inbound_tx,
            handler.as_ref(),
            mesh.as_ref(),
            &cancel,
        )
        .await;
        if stop {
            return;
        }
        if is_request {
            served_requests += 1;
        }
        if served_requests >= REQ_CONN_MAX_REQUESTS {
            return;
        }
    }
}

/// Dispatches one message off an accepted connection: forwards a broadcast
/// message to `inbound_tx`, serves a request inline, or, for a message only
/// ever sent as a reply on a connection this node initiated, does nothing.
/// Returns `true` when this connection is done.
#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "one match arm per RPC kind; splitting it up would obscure the dispatch table"
)]
async fn dispatch_one(
    msg: Msg,
    from: NodeId,
    peer_protocol: u16,
    framed: &mut PeerFramed,
    inbound_tx: &mpsc::Sender<InboundMsg>,
    handler: &dyn RequestHandler,
    mesh: &MeshInner,
    cancel: &CancellationToken,
) -> bool {
    match msg {
        Msg::Invalidate { .. }
        | Msg::Replicate { .. }
        | Msg::ReplicateBatch { .. }
        | Msg::ForwardBatch { .. }
        | Msg::RefreshHint { .. } => {
            let _ = inbound_tx.send(InboundMsg { from, msg }).await;
            false
        }
        Msg::Load { cache, keys } => serve_load(framed, cache, keys, handler, cancel).await,
        Msg::StRequest { cache } => {
            serve_state_transfer(framed, cache, handler, cancel, peer_protocol).await
        }
        Msg::AeDigest { cache, buckets } => {
            if mesh.defers_ae_digest_from(from) {
                tracing::debug!(
                    peer = %from,
                    %cache,
                    "anti-entropy digest from a peer with replicate frames queued toward it; answered empty"
                );
                send_batch_or_cancelled(framed, &[Msg::ReqDone], cancel).await
            } else {
                serve_ae_digest(framed, cache, buckets, handler, cancel, peer_protocol).await
            }
        }
        Msg::AeEntries { cache, buckets } => {
            serve_ae_entries(framed, cache, buckets, handler, cancel).await
        }
        Msg::AePull { cache, keys } => serve_ae_pull(framed, cache, keys, handler, cancel).await,
        Msg::AePullHashes {
            cache,
            bucket,
            hashes,
        } => serve_ae_pull_hashes(framed, cache, bucket, hashes, handler, cancel).await,
        Msg::AeParts { cache, parts } => {
            serve_ae_parts(framed, cache, parts, handler, cancel).await
        }
        Msg::Fetch {
            cache,
            key,
            view_hash,
        } => serve_fetch(framed, cache, key, view_hash, handler, cancel).await,
        Msg::AeDigestScoped {
            cache,
            view_hash,
            buckets,
        } => {
            serve_ae_digest_scoped(
                framed,
                cache,
                view_hash,
                buckets,
                handler,
                cancel,
                peer_protocol,
            )
            .await
        }
        Msg::AeDigestMasked {
            cache,
            view_hash,
            buckets,
        } => serve_ae_digest_masked(framed, cache, view_hash, buckets, handler, cancel).await,
        Msg::StBuckets {
            cache,
            buckets,
            view_hash,
        } => {
            serve_st_buckets(
                framed,
                cache,
                buckets,
                view_hash,
                handler,
                mesh,
                from,
                peer_protocol,
                cancel,
            )
            .await
        }
        // The last bucket's `StBucketDone` is sent after `serve_st_buckets`
        // returns, so its ack lands here instead; recording it catches that
        // case. A mid-stream ack it already recorded just re-inserts.
        Msg::StBucketAck {
            cache,
            bucket,
            view_hash,
        } => {
            mesh.record_bucket_ack(cache, bucket, from, view_hash);
            false
        }
        // A duplicate `Hello`, or `StChunk`/`AeBucket`/`AeSketch`/
        // `AePartDigests`/`AePart`/`AePartSketch`/`StUnavailable`/
        // `FetchReply`/`StBucketChunk`/`StaleView`/`Loaded`/`LoadFailed`/
        // `LoadDeclined`/`ReqDone` sent only as replies on a connection this
        // node initiated, never on one being served here. `StBucketDone` is
        // likewise never dispatched here: only `serve_st_buckets` sends it,
        // to a requester.
        Msg::Hello { .. }
        | Msg::StChunk { .. }
        | Msg::AeBucket { .. }
        | Msg::AeSketch { .. }
        | Msg::AePartDigests { .. }
        | Msg::AePart { .. }
        | Msg::AePartSketch { .. }
        | Msg::StUnavailable { .. }
        | Msg::FetchReply { .. }
        | Msg::FetchDeclined { .. }
        | Msg::StBucketChunk { .. }
        | Msg::StaleView { .. }
        | Msg::StBucketDone { .. }
        | Msg::Loaded { .. }
        | Msg::LoadFailed { .. }
        | Msg::LoadDeclined { .. }
        | Msg::ReqDone => false,
    }
}

/// Serves one state-transfer request. Returns `true` when this connection
/// is done, `false` once the final `done: true` chunk
/// sent cleanly and the connection remains reusable.
async fn serve_state_transfer(
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
async fn serve_ae_digest(
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
async fn serve_fetch(
    framed: &mut PeerFramed,
    cache: SmolStr,
    key: Bytes,
    view_hash: u64,
    handler: &dyn RequestHandler,
    cancel: &CancellationToken,
) -> bool {
    let reply = match handler.fetch(cache.clone(), key, view_hash).await {
        super::FetchServe::Found(rec) => Msg::FetchReply { rec },
        super::FetchServe::Stale {
            responder_view_hash,
        } => Msg::StaleView {
            cache,
            responder_view_hash,
        },
        // A responder that cannot vouch for a miss declines outright, so
        // the requester tries its next owner instead of taking `None` as
        // the answer.
        super::FetchServe::Unavailable => Msg::FetchDeclined { cache },
    };
    send_batch_or_cancelled(framed, &[reply, Msg::ReqDone], cancel).await
}

/// Serves a `Load`: the handler loads the keys, and the answer goes back as
/// [`loaded_replies`] or one `LoadFailed`/`LoadDeclined`, then `ReqDone`.
async fn serve_load(
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
fn loaded_replies(
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
async fn serve_ae_digest_scoped(
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
async fn serve_ae_digest_masked(
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
fn declined_replies<T>(cache: SmolStr, outcome: &AeServeOutcome<T>) -> Vec<Msg> {
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
async fn serve_st_buckets(
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
            super::StServe::Stale {
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
            super::StServe::Cold => {
                // Owned but not yet pulled: not a source. The requester tries
                // its next donor.
                tracing::debug!(
                    cache = %cache,
                    requested,
                    "st buckets pull declined: requested buckets not yet warm locally"
                );
                return send_or_cancelled(framed, &Msg::StUnavailable { cache }, cancel).await;
            }
            super::StServe::Stream {
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
async fn serve_ae_parts(
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
enum ListingOrSketch {
    Listing(Vec<KeyVersion>),
    Sketch(Vec<crate::wire::Cell>),
}

/// The one crossover rule for [`serve_ae_digest`] and [`serve_ae_parts`]:
/// more than `min_bucket` entries answer with a `sketch_cells`-cell sketch
/// over their `(key_hash, version)` pairs, anything up to it with the
/// listing itself.
fn listing_or_sketch(
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
async fn serve_ae_entries(
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

async fn serve_ae_pull(
    framed: &mut PeerFramed,
    cache: SmolStr,
    keys: Vec<Bytes>,
    handler: &dyn RequestHandler,
    cancel: &CancellationToken,
) -> bool {
    let records = handler.records_for(cache.clone(), keys).await;
    let mut replies = super::batch_replicate(&cache, records);
    replies.push(Msg::ReqDone);
    send_batch_or_cancelled(framed, &replies, cancel).await
}

/// Serves an `AePullHashes` request, the sketch-decoded counterpart to
/// [`serve_ae_pull`]: records for `bucket`'s entries whose key hash is in `hashes`.
async fn serve_ae_pull_hashes(
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
    let mut replies = super::batch_replicate(&cache, records);
    replies.push(Msg::ReqDone);
    send_batch_or_cancelled(framed, &replies, cancel).await
}

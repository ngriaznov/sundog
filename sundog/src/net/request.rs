//! The request/response side of [`Mesh`].

#[cfg(doc)]
use super::MeshInner;

use std::collections::{HashMap, HashSet};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use futures::stream::BoxStream;
use smol_str::SmolStr;

use super::{
    AeMismatch, AePartReply, AeRoundOutcome, BucketPull, FetchOutcome, LoadOutcome, Mesh, MsgClass,
    conn,
};
use crate::error::CodecError;
use crate::hlc::Hlc;
use crate::node::NodeId;
use crate::wire::{self, Msg, WireRecord};

/// Bound on one request/response exchange over a fresh connection, end to
/// end. Without this, a peer that accepts and then stalls blocks the
/// caller forever; AP semantics mean a stuck peer must never hang this node.
pub(super) const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

// The override seam below backs only the `not(sim)` test module's
// `ae_entries_times_out_promptly_under_a_short_override`; gated the same
// way that module is, so it isn't dead code under `sim`.
#[cfg(all(test, not(feature = "sim")))]
thread_local! {
    /// Per-thread override of [`REQUEST_TIMEOUT`], set only by
    /// [`with_request_timeout`] in tests. Production code never touches
    /// this; [`request_timeout`] always returns the real constant outside
    /// `#[cfg(test)]`.
    static REQUEST_TIMEOUT_OVERRIDE: std::cell::Cell<Option<Duration>> =
        const { std::cell::Cell::new(None) };
}

/// The request timeout every `ae_*`/`request_state` call races against:
/// [`REQUEST_TIMEOUT`], unless a test installed a shorter override on this
/// thread via [`with_request_timeout`]. Anti-entropy reads it to tell a quick
/// pull answer from a slow one against the timeout the pull raced.
pub(crate) fn request_timeout() -> Duration {
    #[cfg(all(test, not(feature = "sim")))]
    {
        if let Some(over) = REQUEST_TIMEOUT_OVERRIDE.with(std::cell::Cell::get) {
            return over;
        }
    }
    REQUEST_TIMEOUT
}

/// Test-only seam for [`request_timeout`]: polls `fut` with `duration` in
/// effect on this thread for [`request_timeout`] to read, restoring the
/// real [`REQUEST_TIMEOUT`] once `fut` resolves. `duration` is set before
/// `fut` is ever polled and cleared only after, so it's in effect for
/// `fut`'s whole lifetime, not only the moment this function is called.
/// Doesn't change production behavior, which never reads the override.
#[cfg(all(test, not(feature = "sim")))]
pub(crate) async fn with_request_timeout<T>(
    duration: Duration,
    fut: impl std::future::Future<Output = T>,
) -> T {
    REQUEST_TIMEOUT_OVERRIDE.with(|cell| cell.set(Some(duration)));
    let result = fut.await;
    REQUEST_TIMEOUT_OVERRIDE.with(|cell| cell.set(None));
    result
}

/// Wraps a timed-out request/response exchange as a [`CodecError::Io`], the
/// same shape a genuine connection failure produces.
fn request_timeout_error(what: &str, timeout: Duration) -> CodecError {
    CodecError::Io(io::Error::new(
        io::ErrorKind::TimedOut,
        format!("{what} did not complete within {timeout:?}"),
    ))
}

/// Bounds `fut` by [`request_timeout`], mapping a timeout to
/// [`request_timeout_error`] naming `what`: the one expression every
/// `ae_*`/`fetch` request method reduces to.
async fn timed<T>(
    what: &str,
    fut: impl std::future::Future<Output = Result<T, CodecError>>,
) -> Result<T, CodecError> {
    let timeout = request_timeout();
    tokio::time::timeout(timeout, fut)
        .await
        .unwrap_or_else(|_| Err(request_timeout_error(what, timeout)))
}

impl Mesh {
    fn peer_req_pool(&self, peer: NodeId) -> Result<(SocketAddr, Arc<conn::ReqPool>), CodecError> {
        self.inner
            .peers
            .read()
            .expect("invariant: peers lock is never poisoned")
            .get(&peer)
            .map(|handle| (handle.data_addr, Arc::clone(&handle.req_pool)))
            .ok_or_else(|| {
                CodecError::Io(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("peer {peer} is not a known mesh member"),
                ))
            })
    }

    /// Checks `peer`'s last-known gossiped protocol against `since` before
    /// a distribution-mode send ever dials: the hard wire-layer guarantee
    /// that a peer speaking less than [`wire::PROTOCOL_DISTRIBUTED`] never
    /// receives one of these messages, independent of whether the caller's
    /// own eligibility filtering is correct.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::Io`] if `peer` is unknown, or if it is known
    /// but speaks a protocol older than `since`.
    fn require_peer_protocol(
        &self,
        peer: NodeId,
        since: u16,
        what: &str,
    ) -> Result<(), CodecError> {
        let protocol = self
            .inner
            .peers
            .read()
            .expect("invariant: peers lock is never poisoned")
            .get(&peer)
            .map(|handle| handle.protocol.load(Ordering::Relaxed))
            .ok_or_else(|| {
                CodecError::Io(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("peer {peer} is not a known mesh member"),
                ))
            })?;
        if wire::peer_supports(protocol, since) {
            Ok(())
        } else {
            Err(CodecError::Io(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("peer {peer} speaks protocol {protocol}, too old for {what}"),
            )))
        }
    }

    /// Checks a pooled, already-`Hello`'d connection out of `peer`'s pool
    /// and sends `first` on it, or dials a fresh one, coalescing `Hello`
    /// and `first` into one flush; the returned [`conn::Reply`] carries any
    /// reply already peeked off a reused connection. A reused connection
    /// whose write succeeds but whose receive side is already at `EOF` is
    /// the server's idle timeout having closed it first: retried here on
    /// the next pooled connection or a fresh dial.
    async fn acquire_conn(&self, peer: NodeId, first: Msg) -> Result<conn::Reply, CodecError> {
        let (addr, pool) = self.peer_req_pool(peer)?;
        while let Some(mut framed) = pool.checkout() {
            // A fresh dial has a real yield point; a reused connection has
            // none. Without this yield, an anti-entropy loop that skips
            // dialing can hog a single-threaded runtime turn after turn.
            tokio::task::yield_now().await;
            if conn::send_msg(&mut framed, &first).await.is_err() {
                // A stale or broken pooled connection: try the next one, or
                // fall through to a fresh dial if none are left.
                continue;
            }
            match conn::probe_reused(&mut framed) {
                conn::ReusedProbe::Pending => return Ok((framed, pool, None)),
                conn::ReusedProbe::Ready(msg) => return Ok((framed, pool, Some(Ok(msg)))),
                // Stale: the server already closed this one; try the next
                // pooled connection, or fall through to a fresh dial.
                conn::ReusedProbe::Stale => {}
            }
        }
        let framed = conn::dial_with_hello_and(
            addr,
            self.inner.greeting(),
            &self.inner.tls,
            self.inner.keepalive,
            first,
        )
        .await?;
        Ok((framed, pool, None))
    }

    /// [`acquire_conn`], then reads the responder's own first reply if it
    /// didn't already peek one off a reused connection. Bounds both steps
    /// by [`request_timeout`], mapping a timeout to
    /// `request_timeout_error(what, ..)`.
    async fn acquire_conn_and_first(
        &self,
        peer: NodeId,
        first: Msg,
        what: &str,
    ) -> Result<conn::Reply, CodecError> {
        let timeout = request_timeout();
        let (mut framed, pool, pre_read) =
            tokio::time::timeout(timeout, self.acquire_conn(peer, first))
                .await
                .unwrap_or_else(|_| Err(request_timeout_error(what, timeout)))?;
        let first = match pre_read {
            Some(result) => Some(result),
            None => tokio::time::timeout(timeout, conn::recv_msg(&mut framed))
                .await
                .map_err(|_| request_timeout_error(what, timeout))?,
        };
        Ok((framed, pool, first))
    }

    /// Requests a full snapshot of `cache` from `donor`, for state transfer
    /// on join, and returns a stream of its `StChunk`s. Reads lazily off a
    /// fresh connection as the caller polls.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] if `donor` is unknown or the request cannot be
    /// sent.
    pub async fn request_state(
        &self,
        donor: NodeId,
        cache: SmolStr,
    ) -> Result<Option<BoxStream<'static, Result<Vec<WireRecord>, CodecError>>>, CodecError> {
        // Only the checkout-or-dial step and the donor's first reply are
        // bounded here; `try_donor`'s own per-donor budget governs the full
        // snapshot stream instead.
        let (framed, pool, first) = self
            .acquire_conn_and_first(donor, Msg::StRequest { cache }, "state transfer request")
            .await?;
        if let Some(Ok(Msg::StUnavailable { .. })) = first {
            pool.checkin(framed);
            return Ok(None);
        }
        Ok(Some(conn::state_stream(framed, pool, first)))
    }

    /// Runs one anti-entropy digest exchange against `peer`, returning `local_buckets`'s mismatches.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] if `peer` is unknown or the exchange fails.
    pub async fn ae_round(
        &self,
        peer: NodeId,
        cache: SmolStr,
        local_buckets: Vec<(u16, u64)>,
    ) -> Result<Vec<AeMismatch>, CodecError> {
        timed("anti-entropy digest exchange", async {
            let msg = Msg::AeDigest {
                cache,
                buckets: local_buckets,
            };
            conn::collect_ae_mismatches(self.acquire_conn(peer, msg).await?).await
        })
        .await
    }

    /// The `AeSketch` fallback: full listings for `buckets`, answered like [`Mesh::ae_round`].
    /// A listing too large for one frame arrives in several, which this
    /// joins: the result holds one element per bucket, its entries in the
    /// responder's order. The whole reply, every frame of it, completes
    /// within one request timeout.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] if `peer` is unknown or the exchange fails.
    pub async fn ae_entries(
        &self,
        peer: NodeId,
        cache: SmolStr,
        buckets: Vec<u16>,
    ) -> Result<crate::store::BucketEntries, CodecError> {
        timed("anti-entropy sketch-fallback listing", async {
            let msg = Msg::AeEntries { cache, buckets };
            conn::collect_ae_buckets(self.acquire_conn(peer, msg).await?).await
        })
        .await
    }

    /// Requests the mismatched `(bucket, part)` pairs named by `peer`'s [`AeMismatch::PartDigests`] reply.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] if `peer` is unknown or the exchange fails.
    pub async fn ae_parts(
        &self,
        peer: NodeId,
        cache: SmolStr,
        parts: Vec<(u16, u8)>,
    ) -> Result<Vec<AePartReply>, CodecError> {
        timed("anti-entropy part exchange", async {
            let msg = Msg::AeParts { cache, parts };
            conn::collect_ae_part_replies(self.acquire_conn(peer, msg).await?).await
        })
        .await
    }

    /// Pulls full records for `keys` from `peer`, the `AePull` step.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] if `peer` is unknown or the exchange fails.
    pub async fn ae_pull(
        &self,
        peer: NodeId,
        cache: SmolStr,
        keys: Vec<Bytes>,
    ) -> Result<Vec<WireRecord>, CodecError> {
        timed("anti-entropy pull", async {
            let msg = Msg::AePull { cache, keys };
            conn::collect_pulled_records(self.acquire_conn(peer, msg).await?).await
        })
        .await
    }

    /// Pulls records from `peer` for `bucket`'s entries whose key hash is in `hashes`.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] if `peer` is unknown or the exchange fails.
    pub async fn ae_pull_hashes(
        &self,
        peer: NodeId,
        cache: SmolStr,
        bucket: u16,
        hashes: Vec<u64>,
    ) -> Result<Vec<WireRecord>, CodecError> {
        timed("anti-entropy pull-by-hash", async {
            let msg = Msg::AePullHashes {
                cache,
                bucket,
                hashes,
            };
            conn::collect_pulled_records(self.acquire_conn(peer, msg).await?).await
        })
        .await
    }

    /// Distribution-mode read: requests `key`'s record from `owner`, at this node's `view_hash`.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] if `owner` is unknown, speaks a protocol
    /// older than [`wire::PROTOCOL_DISTRIBUTED`], or the exchange fails.
    pub(crate) async fn fetch(
        &self,
        owner: NodeId,
        cache: SmolStr,
        key: Bytes,
        view_hash: u64,
    ) -> Result<FetchOutcome, CodecError> {
        self.require_peer_protocol(
            owner,
            wire::PROTOCOL_DISTRIBUTED,
            "a distribution-mode fetch",
        )?;
        timed("distribution-mode fetch", async {
            let msg = Msg::Fetch {
                cache,
                key,
                view_hash,
            };
            conn::collect_fetch_reply(self.acquire_conn(owner, msg).await?).await
        })
        .await
    }

    /// Asks `peer` to load `keys` of `cache` through its loader.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] if `peer` is unknown, speaks a protocol
    /// older than [`wire::PROTOCOL_LOAD`], or the exchange fails.
    pub(crate) async fn load(
        &self,
        peer: NodeId,
        cache: SmolStr,
        keys: Vec<Bytes>,
    ) -> Result<LoadOutcome, CodecError> {
        self.require_peer_protocol(peer, wire::PROTOCOL_LOAD, "a load request")?;
        timed("load request", async {
            let msg = Msg::Load { cache, keys };
            conn::collect_load_reply(self.acquire_conn(peer, msg).await?).await
        })
        .await
    }

    /// Tells `peer`, the node that refreshes `key`, that a read here found
    /// the entry at `ver` past its refresh point. One-way and best effort:
    /// queued like an invalidation, and not sent to a peer older than
    /// [`wire::PROTOCOL_LOAD`]. Returns whether it was queued.
    pub(crate) fn send_refresh_hint(
        &self,
        peer: NodeId,
        cache: SmolStr,
        key: Bytes,
        ver: Hlc,
    ) -> bool {
        if self
            .require_peer_protocol(peer, wire::PROTOCOL_LOAD, "a refresh hint")
            .is_err()
        {
            return false;
        }
        self.send(
            peer,
            MsgClass::Invalidate,
            Msg::RefreshHint { cache, key, ver },
        );
        true
    }

    /// Distribution-mode anti-entropy round, step 1: like [`Mesh::ae_round`] but scoped to `local_buckets` at `view_hash`.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] if `peer` is unknown, speaks a protocol older
    /// than [`wire::PROTOCOL_DISTRIBUTED`], or the exchange fails.
    pub(crate) async fn ae_round_scoped(
        &self,
        peer: NodeId,
        cache: SmolStr,
        view_hash: u64,
        local_buckets: Vec<(u16, u64)>,
    ) -> Result<AeRoundOutcome, CodecError> {
        self.require_peer_protocol(
            peer,
            wire::PROTOCOL_DISTRIBUTED,
            "a scoped anti-entropy digest exchange",
        )?;
        timed("scoped anti-entropy digest exchange", async {
            let msg = Msg::AeDigestScoped {
                cache,
                view_hash,
                buckets: local_buckets,
            };
            conn::collect_ae_round_scoped(self.acquire_conn(peer, msg).await?).await
        })
        .await
    }

    /// [`Mesh::ae_round_scoped`] under a part-granular view: each of
    /// `local_buckets` is `(bucket, mask, digest)`, the digest folded over
    /// the parts the mask names, and a mismatch answers with the bucket's
    /// part digests.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] if `peer` is unknown, speaks a protocol older
    /// than [`wire::PROTOCOL_PART_OWNERSHIP`], or the exchange fails.
    pub(crate) async fn ae_round_masked(
        &self,
        peer: NodeId,
        cache: SmolStr,
        view_hash: u64,
        local_buckets: Vec<(u16, u64, u64)>,
    ) -> Result<AeRoundOutcome, CodecError> {
        self.require_peer_protocol(
            peer,
            wire::PROTOCOL_PART_OWNERSHIP,
            "a masked anti-entropy digest exchange",
        )?;
        timed("masked anti-entropy digest exchange", async {
            let msg = Msg::AeDigestMasked {
                cache,
                view_hash,
                buckets: local_buckets,
            };
            conn::collect_ae_round_scoped(self.acquire_conn(peer, msg).await?).await
        })
        .await
    }

    /// Rebalance: requests everything `donor` holds for `buckets`, the
    /// bucket-scoped counterpart of [`Mesh::request_state`], carrying this
    /// node's `view_hash` so a donor whose own view has since diverged can
    /// decline instead of transferring against a stale owner set. `Ok(None)`
    /// is a decline (the donor is not a valid source right now, or the view
    /// hashes disagree); the caller moves on to its next candidate donor.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] if `donor` is unknown, speaks a protocol
    /// older than [`wire::PROTOCOL_DISTRIBUTED`], or the request cannot be
    /// sent.
    pub(crate) async fn request_buckets(
        &self,
        donor: NodeId,
        cache: SmolStr,
        buckets: Vec<u16>,
        view_hash: u64,
    ) -> Result<BucketPull, CodecError> {
        self.require_peer_protocol(donor, wire::PROTOCOL_DISTRIBUTED, "a rebalance bucket pull")?;
        // The caller's own transfer budget governs the full chunk stream,
        // exactly as `request_state` does; only the checkout-or-dial step
        // and the donor's first reply are bounded here.
        let stream_cache = cache.clone();
        let msg = Msg::StBuckets {
            cache,
            buckets,
            view_hash,
        };
        let (framed, pool, first) = self
            .acquire_conn_and_first(donor, msg, "rebalance bucket request")
            .await?;
        if let Some(Ok(Msg::StaleView { .. })) = first {
            pool.checkin(framed);
            return Ok(BucketPull::Stale);
        }
        if let Some(Ok(Msg::StUnavailable { .. })) = first {
            pool.checkin(framed);
            return Ok(BucketPull::Cold);
        }
        // The donor's gossiped protocol, so `bucket_stream` gates its ack
        // send the same way `serve_st_buckets` gates `StBucketDone`.
        let donor_protocol = self.inner.peer_protocol(donor);
        Ok(BucketPull::Stream(conn::bucket_stream(
            framed,
            pool,
            first,
            stream_cache,
            donor_protocol,
            view_hash,
        )))
    }

    /// See [`MeshInner::acked_owners`]. Lets `rebalance_task` skip a
    /// redundant anti-entropy round against an owner that already acked a
    /// bucket.
    pub(crate) fn acked_owners(
        &self,
        cache: &SmolStr,
        due: &[u16],
        window: Duration,
        view_hash: u64,
    ) -> HashMap<u16, HashSet<NodeId>> {
        self.inner.acked_owners(cache, due, window, view_hash)
    }
}

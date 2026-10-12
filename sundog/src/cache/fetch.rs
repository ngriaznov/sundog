//! The owner read path: asks an owner for a key, tells apart a hit, a miss and an unreachable
//! owner, and records how each read ended.

use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use rand::RngExt as _;
use serde::Serialize;
use serde::de::DeserializeOwned;
use smol_str::SmolStr;

use super::Cache;
use crate::error::CacheError;
use crate::explain::{OwnerProbe, Reads};
use crate::net::FetchOutcome;
use crate::node::NodeId;
use crate::ownership::OwnershipView;
use crate::store::{PartId, ShardOps, encode_key, now_ms};
use crate::wire::WireRecord;
use crate::{explain, verdict};

/// Where a `Mode::Distributed` read's answer came from.
pub(super) enum OwnerAnswer<V> {
    /// This node owns the key's part warm: its own copy.
    Local(Option<V>),
    /// An owner's record, a tombstone or expired record included, or
    /// `None` for a key the owner never held.
    Remote(Option<WireRecord>),
}

/// `part`'s owners under `view` other than `self_node`: the candidates a
/// [`Cache::fetch`] asks, in rendezvous order.
pub(super) fn other_owners(view: &OwnershipView, part: PartId, self_node: NodeId) -> Vec<NodeId> {
    view.owners_of(part)
        .iter()
        .copied()
        .filter(|owner| *owner != self_node)
        .collect()
}

/// A small jittered backoff between retrying the same [`Cache::fetch`]
/// owner after it reports its view as stale but this node's own view has
/// not itself changed: uniformly in `[10, 50)` ms. The retries against one
/// owner span at most `ClusterConfig::fetch_timeout`, the same bound one
/// request gets, before the next owner is tried.
fn fetch_retry_backoff() -> Duration {
    Duration::from_millis(rand::rng().random_range(10..50))
}

/// Counts one [`Cache::fetch`], started at `started`, in
/// `sundog_fetch_total{cache,outcome}`, records `outcome` on its `span` and,
/// when it made at least one owner request (`attempts`), records its
/// duration in `sundog_fetch_duration_seconds{cache,outcome}`. A fetch that
/// asked no owner is not timed, whatever its outcome.
pub(super) fn record_fetch_outcome(
    span: &tracing::Span,
    cache: &str,
    outcome: &'static str,
    attempts: u32,
    started: std::time::Instant,
) {
    metrics::counter!(
        "sundog_fetch_total",
        "cache" => cache.to_string(),
        "outcome" => outcome
    )
    .increment(1);
    span.record("outcome", outcome);
    if attempts > 0 {
        metrics::histogram!(
            "sundog_fetch_duration_seconds",
            "cache" => cache.to_string(),
            "outcome" => outcome
        )
        .record(started.elapsed());
    }
}

/// Decodes a [`Cache::fetch`] reply's record into its value, re-checking
/// expiry client-side (defense in depth against the responder's own expiry
/// sweep lagging) and treating a tombstone or undecodable value as a miss.
pub(super) fn decode_live_value<V: DeserializeOwned>(rec: &WireRecord) -> Option<V> {
    if explain::dead_record(rec, now_ms()).is_some() {
        return None;
    }
    rec.value
        .as_deref()
        .and_then(|bytes| postcard::from_bytes::<V>(bytes).ok())
}

/// What a [`Cache::fetch`] makes of an owner's record at `now_ms`, the
/// judgement [`decode_live_value`] makes, named.
pub(super) fn record_reads<V: DeserializeOwned>(rec: &WireRecord, now_ms: u64) -> Reads {
    explain::dead_record(rec, now_ms).unwrap_or_else(|| {
        if rec
            .value
            .as_deref()
            .is_some_and(|bytes| postcard::from_bytes::<V>(bytes).is_ok())
        {
            Reads::Value
        } else {
            Reads::Undecodable
        }
    })
}

impl<K, V> Cache<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// [`Cache::fetch`] inside its `sundog.fetch` span. Every field is
    /// recorded on `span` itself, so a span the subscriber disables records
    /// nothing and never writes to the caller's span.
    pub(super) async fn fetch_in(
        &self,
        key: &K,
        span: &tracing::Span,
    ) -> Result<Option<V>, CacheError> {
        let started = std::time::Instant::now();
        let Some(view) = self.shard.ownership_view() else {
            let value = self.shard.get(key).await;
            span.record("owner", tracing::field::display(self.cluster.node_id()));
            record_fetch_outcome(span, self.shard.name(), "local", 0, started);
            return Ok(value);
        };
        let key_bytes = encode_key(key)?;
        let mut attempts = 0u32;
        let answer = self
            .owner_answer(key, &key_bytes, view, span, &mut attempts)
            .await;
        let (outcome, fetched) = match answer {
            Ok(OwnerAnswer::Local(value)) => ("local", Ok(value)),
            Ok(OwnerAnswer::Remote(rec)) => {
                if let Some(rec) = &rec
                    && rec.value.is_some()
                {
                    self.shard.note_read(
                        key,
                        &rec.key,
                        crate::store::hash_key_bytes(&rec.key),
                        rec.ver,
                        rec.expires_at_ms,
                        now_ms(),
                    );
                }
                let value = rec.and_then(|rec| decode_live_value::<V>(&rec));
                (if value.is_some() { "remote" } else { "miss" }, Ok(value))
            }
            Err(err) => ("error", Err(err)),
        };
        // One call for every answer, with the owner requests this fetch
        // made, so a local answer reached after asking an owner is timed
        // exactly like a remote one.
        record_fetch_outcome(span, self.shard.name(), outcome, attempts, started);
        fetched
    }

    /// [`Cache::fetch`]'s answer for a `Mode::Distributed` key under
    /// `view`: this node's own copy when it owns the key's part warm, or
    /// else the record the first owner to answer holds, tried in rendezvous
    /// order with the stale-view retry `fetch` documents. Counts each owner
    /// request in `attempts` and records it, and the owner asked, on
    /// `span`.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::FetchUnavailable`] when no owner answers.
    pub(super) async fn owner_answer(
        &self,
        key: &K,
        key_bytes: &bytes::Bytes,
        view: Arc<OwnershipView>,
        span: &tracing::Span,
        attempts: &mut u32,
    ) -> Result<OwnerAnswer<V>, CacheError> {
        let part = PartId::of_key(key_bytes);
        let self_node = self.cluster.node_id();
        if let Some(value) = self.local_answer(key, &view, part).await {
            span.record("owner", tracing::field::display(self_node));
            return Ok(OwnerAnswer::Local(value));
        }

        let cache_name = SmolStr::new(self.shard.name());
        let mesh = self.cluster.mesh();
        let attempt_timeout = self.cluster.config().fetch_timeout;

        let mut view = view;
        let mut owners = other_owners(&view, part, self_node);
        let mut owner_idx = 0usize;
        // When the current owner first answered `Stale` with this node's
        // view unchanged: its retry window runs from here.
        let mut stale_since: Option<tokio::time::Instant> = None;

        while owner_idx < owners.len() {
            let owner = owners[owner_idx];
            *attempts += 1;
            span.record("owner", tracing::field::display(owner));
            span.record("attempts", *attempts);
            let outcome = tokio::time::timeout(
                attempt_timeout,
                mesh.fetch(
                    owner,
                    cache_name.clone(),
                    key_bytes.clone(),
                    view.view_hash(),
                ),
            )
            .await;
            match outcome {
                Ok(Ok(FetchOutcome::Found(rec))) => return Ok(OwnerAnswer::Remote(rec)),
                Ok(Ok(FetchOutcome::Stale { .. })) => {
                    if let Some(fresh) = self.shard.ownership_view()
                        && fresh.view_hash() != view.view_hash()
                    {
                        view = fresh;
                        if let Some(value) = self.local_answer(key, &view, part).await {
                            span.record("owner", tracing::field::display(self_node));
                            return Ok(OwnerAnswer::Local(value));
                        }
                        owners = other_owners(&view, part, self_node);
                        owner_idx = 0;
                        stale_since = None;
                        continue;
                    }
                    let since = *stale_since.get_or_insert_with(tokio::time::Instant::now);
                    if since.elapsed() >= attempt_timeout {
                        owner_idx += 1;
                        stale_since = None;
                        continue;
                    }
                    tokio::time::sleep(fetch_retry_backoff()).await;
                }
                // A decline is as good as no answer: that owner holds no
                // warm copy to vouch for the key.
                Ok(Ok(FetchOutcome::Declined) | Err(_)) | Err(_) => {
                    owner_idx += 1;
                    stale_since = None;
                }
            }
        }
        Err(CacheError::FetchUnavailable { cache: cache_name })
    }

    /// This node's own answer to a fetch of `key` in `part` under `view`:
    /// `Some` with the value, or with `None` for a warm miss, when this
    /// node's copy answers, by [`verdict::local_verdict`]; `None` when only
    /// another owner's copy does. A distrusted part (warm-reloaded or
    /// regained, not yet checked against a live co-owner) can hold a record
    /// a co-owner deleted while this node was down or not an owner, and a
    /// cold part's miss says nothing while its previous owner can still
    /// hold the record through the disown grace. The part's marks are read
    /// before the record, so a pull landing during the read cannot clear a
    /// cold mark the decision then trusts.
    async fn local_answer(&self, key: &K, view: &OwnershipView, part: PartId) -> Option<Option<V>> {
        let residency = self.shard.residency_of(view, part);
        if !verdict::reads_local(&residency) {
            return None;
        }
        let value = self.shard.get(key).await;
        verdict::local_verdict(&residency, value.is_some())
            .answers()
            .then_some(value)
    }

    /// `owner`'s answer to the fetch [`Cache::fetch`] sends it for
    /// `key_bytes` under `view_hash`, bounded by `fetch_timeout`, with its
    /// record judged at `at_ms`.
    pub(super) async fn probe_owner(
        &self,
        owner: NodeId,
        key_bytes: &bytes::Bytes,
        view_hash: u64,
        at_ms: u64,
    ) -> OwnerProbe {
        let reply = tokio::time::timeout(
            self.cluster.config().fetch_timeout,
            self.cluster.mesh().fetch(
                owner,
                SmolStr::new(self.shard.name()),
                key_bytes.clone(),
                view_hash,
            ),
        )
        .await
        .ok();
        OwnerProbe {
            node: owner,
            answer: explain::classify_probe(reply, |rec| record_reads::<V>(rec, at_ms)),
        }
    }
}

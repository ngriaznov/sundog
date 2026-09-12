//! Deterministic simulation suite: drives `net::Mesh` and
//! `store::Shard`/`ShardOps` directly inside a `turmoil` simulation, against
//! a hand-scripted membership feed built from `Peer` values, with no real
//! UDP or TCP. `cluster.rs`'s composition is `pub(crate)` and unusable from
//! here, so this file reimplements the relevant slice against the same
//! public `Mesh`/`ShardOps` surface `cluster.rs` itself drives.
//!
//! `turmoil`'s simulated TCP objects must be created and driven from within
//! the owning host's own future; a `Mesh` cannot be shared across hosts. A
//! `Shard` has no such constraint, so each scenario builds it up front and
//! shares an `Arc` clone with both the host future and the test itself.

#![cfg(feature = "sim")]

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::num::NonZeroU8;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt as _;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use rand::{RngExt as _, SeedableRng as _, rngs::StdRng};
use smol_str::SmolStr;
use sundog::config::ClusterConfig;
use sundog::crdt::{PnCounter, PnCounterResolver, WriterId};
use sundog::hlc::Hlc;
use sundog::membership::Peer;
use sundog::net::{AeMismatch, AePartReply, InboundMsg, Mesh, MsgClass, RequestHandler};
use sundog::node::{NodeId, NodeName};
use sundog::store::{Mode, Shard, ShardOps, SimFanOut};
use sundog::wire::{Msg, WireRecord};
use sundog::{
    ConflictResolver, Merged, OwnershipTracker, OwnershipView, RecordView, ResidencySet, Winner,
    ownership_diff,
};
use tokio::sync::watch;
use turmoil::{Builder, Sim};
use xxhash_rust::xxh3::xxh3_64;

type TestShard = Shard<u32, String>;
type SimResult = turmoil::Result;

/// Deterministic by default. The scheduled fresh-seed CI job overrides via
/// `SUNDOG_SIM_SEED`, echoing the seed so a red run can replay it.
fn sim_seed(default: u64) -> u64 {
    std::env::var("SUNDOG_SIM_SEED").map_or(default, |raw| {
        let seed: u64 = raw.parse().expect("SUNDOG_SIM_SEED is a u64 turmoil seed");
        eprintln!("sim seed override: replay with SUNDOG_SIM_SEED={seed}");
        seed
    })
}

const CACHE: &str = "sim-users";
/// Simulated time per `Sim::step()`, small relative to every interval below.
const TICK: Duration = Duration::from_millis(5);
/// Bounds every request/response network call in this harness. Turmoil's
/// `fail_rate` drops a message outright with no retransmission, so a
/// stalled read needs its own timeout; real `net::Mesh` relies on TCP's own.
const NET_TIMEOUT: Duration = Duration::from_millis(500);

fn cache_name() -> SmolStr {
    SmolStr::new(CACHE)
}

fn key_bytes(key: u32) -> Bytes {
    Bytes::from(postcard::to_stdvec(&key).expect("u32 always postcard-encodes"))
}

/// Runs a `ShardOps` future to completion with no ambient runtime, valid
/// since `Shard`'s async methods only await plain `tokio::sync` primitives.
fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    futures::executor::block_on(fut)
}

fn digests_of(shard: &TestShard) -> Vec<(u16, u64)> {
    block_on(ShardOps::digests(shard))
}

fn value_of(shard: &TestShard, key: u32) -> Option<String> {
    block_on(shard.get(&key))
}

fn new_shard(node: NodeId) -> TestShard {
    Shard::new(cache_name(), Mode::Replicated, node, 100_000, None, None)
}

/// Builds a shard pre-populated with `keys`, for state-transfer donors.
fn seed_shard(node: NodeId, keys: impl IntoIterator<Item = u32>) -> TestShard {
    let shard = new_shard(node);
    block_on(async {
        for key in keys {
            shard
                .insert(key, format!("seed:{key}"))
                .await
                .expect("small values never exceed the frame cap");
        }
    });
    shard
}

fn peer_list_of(peers: &[(NodeId, &str, u16)]) -> Vec<Peer> {
    peers
        .iter()
        .map(|&(node, host, port)| Peer {
            node,
            name: NodeName::new(host, node),
            gossip_addr: SocketAddr::from(([0, 0, 0, 0], 0)),
            data_addr: SocketAddr::new(turmoil::lookup(host), port),
            incarnation: 1,
            protocol: sundog::wire::PROTOCOL_VERSION,
        })
        .collect()
}

async fn dispatch_inbound<S: ShardOps>(shard: &S, msg: Msg) {
    match msg {
        Msg::Invalidate { key, ver, .. } => ShardOps::invalidate(shard, key, ver).await,
        Msg::Replicate { rec, .. } => ShardOps::apply_remote(shard, rec).await,
        Msg::ReplicateBatch { recs, .. } | Msg::ForwardBatch { recs, .. } => {
            ShardOps::apply_remote_batch(shard, recs).await;
        }
        // `Hello`, request/response messages, and `ReqDone` are a no-op here.
        _ => {}
    }
}

/// Wraps a shared `Shard` as the `net::RequestHandler` a `Mesh` answers
/// inbound requests through, standing in for `cluster.rs`'s handler.
/// `ae_sketch_min_bucket`/`ae_part_min_bucket` mirror the fields of the same
/// name on `ClusterConfig`, letting a scenario force a low threshold so a
/// mismatched bucket answers with `AeMismatch::Sketch`/`PartDigests` instead
/// of a full listing.
struct ShardHandler<S: ShardOps + 'static> {
    shard: Arc<S>,
    ae_part_min_bucket: usize,
    ae_sketch_min_bucket: usize,
}

impl<S: ShardOps + 'static> ShardHandler<S> {
    /// Both thresholds at [`ClusterConfig::default`]'s values: every
    /// scenario but the sketch/part ones want this, since their buckets
    /// never grow past either.
    fn new(shard: Arc<S>) -> Self {
        let defaults = ClusterConfig::default();
        Self::with_min_buckets(
            shard,
            defaults.ae_part_min_bucket,
            defaults.ae_sketch_min_bucket,
        )
    }

    fn with_min_buckets(
        shard: Arc<S>,
        ae_part_min_bucket: usize,
        ae_sketch_min_bucket: usize,
    ) -> Self {
        Self {
            shard,
            ae_part_min_bucket,
            ae_sketch_min_bucket,
        }
    }
}

impl<S: ShardOps + 'static> RequestHandler for ShardHandler<S> {
    fn snapshot_chunks(&self, _cache: SmolStr) -> BoxStream<'static, Vec<WireRecord>> {
        ShardOps::snapshot_chunks(self.shard.as_ref())
    }

    fn digests(&self, _cache: SmolStr) -> BoxFuture<'_, Vec<(u16, u64)>> {
        let shard = Arc::clone(&self.shard);
        Box::pin(async move { ShardOps::digests(shard.as_ref()).await })
    }

    fn bucket_entries(&self, _cache: SmolStr, bucket: u16) -> BoxFuture<'_, Vec<(Bytes, Hlc)>> {
        let shard = Arc::clone(&self.shard);
        Box::pin(async move { ShardOps::bucket_entries(shard.as_ref(), bucket).await })
    }

    fn entries_for_buckets(
        &self,
        _cache: SmolStr,
        buckets: Vec<u16>,
    ) -> BoxFuture<'_, sundog::store::BucketEntries> {
        let shard = Arc::clone(&self.shard);
        Box::pin(async move { ShardOps::entries_for_buckets(shard.as_ref(), buckets).await })
    }

    fn records_for(&self, _cache: SmolStr, keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>> {
        let shard = Arc::clone(&self.shard);
        Box::pin(async move { ShardOps::records_for(shard.as_ref(), keys).await })
    }

    fn bucket_lens(&self, _cache: SmolStr, buckets: Vec<u16>) -> BoxFuture<'_, Vec<(u16, usize)>> {
        let shard = Arc::clone(&self.shard);
        Box::pin(async move { ShardOps::bucket_lens(shard.as_ref(), buckets).await })
    }

    fn part_digests(
        &self,
        _cache: SmolStr,
        buckets: Vec<u16>,
    ) -> BoxFuture<'_, Vec<(u16, Vec<u64>)>> {
        let shard = Arc::clone(&self.shard);
        Box::pin(async move { ShardOps::part_digests(shard.as_ref(), buckets).await })
    }

    fn entries_for_parts(
        &self,
        _cache: SmolStr,
        parts: Vec<(u16, u8)>,
    ) -> BoxFuture<'_, sundog::store::PartEntries> {
        let shard = Arc::clone(&self.shard);
        Box::pin(async move { ShardOps::entries_for_parts(shard.as_ref(), parts).await })
    }

    fn ae_part_min_bucket(&self) -> usize {
        self.ae_part_min_bucket
    }

    fn ae_sketch_min_bucket(&self) -> usize {
        self.ae_sketch_min_bucket
    }
}

/// Fans `key`'s current record out to `peers`, `dup_factor` times each,
/// standing in for `cluster::fan_out_one`'s `Mode::Replicated` arm.
async fn fan_out<S: ShardOps>(
    shard: &S,
    mesh: &Mesh,
    peers: &[NodeId],
    key: u32,
    dup_factor: usize,
) {
    let Some(rec) = ShardOps::records_for(shard, vec![key_bytes(key)])
        .await
        .into_iter()
        .next()
    else {
        return;
    };
    for &peer in peers {
        for _ in 0..dup_factor.max(1) {
            mesh.send(
                peer,
                MsgClass::Replicate,
                Msg::Replicate {
                    cache: cache_name(),
                    rec: rec.clone(),
                },
            );
        }
    }
}

/// Decodes one sketch (bucket-scoped or part-scoped alike) against
/// `local_entries`: on success, classifies the peel via
/// [`sundog::diff_decoded`] into `push_keys`/`pull_hashes`; on failure,
/// queues `bucket` for the `Mesh::ae_entries` listing fallback. Shared by
/// [`classify_ae_mismatches`]'s `AeMismatch::Sketch` arm and
/// [`resolve_wanted_parts`]'s `AePartReply::Sketch` arm.
fn decode_sketch_into(
    bucket: u16,
    cells: Vec<sundog::wire::Cell>,
    local_entries: &[(Bytes, Hlc)],
    push_keys: &mut Vec<Bytes>,
    pull_hashes: &mut Vec<(u16, Vec<u64>)>,
    undecodable_buckets: &mut Vec<u16>,
    merging: bool,
) {
    let mut local_sketch = sundog::Iblt::new(cells.len());
    for (key, ver) in local_entries {
        local_sketch.insert(xxh3_64(key), *ver);
    }
    match local_sketch
        .subtract(&sundog::Iblt::from_cells(cells))
        .and_then(sundog::Iblt::peel)
    {
        Ok(decoded) => {
            let mut hashes = Vec::new();
            sundog::diff_decoded(local_entries, &decoded, push_keys, &mut hashes, merging);
            if !hashes.is_empty() {
                pull_hashes.push((bucket, hashes));
            }
        }
        Err(_) => undecodable_buckets.push(bucket),
    }
}

/// Classifies one round's `AeMismatch` replies into push/pull keys, hash
/// pulls, undecodable buckets, and wanted `(bucket, part)` pairs: the match
/// [`ae_round_with_sketch`] runs over `mismatched`, split out to keep that
/// function's own line count down. `bucket_listings` counts every
/// `AeMismatch::Bucket` reply seen.
#[allow(clippy::too_many_arguments)]
async fn classify_ae_mismatches<S: ShardOps>(
    shard: &S,
    mismatched: Vec<AeMismatch>,
    bucket_listings: Option<&AtomicUsize>,
    push_keys: &mut Vec<Bytes>,
    pull_keys: &mut Vec<Bytes>,
    pull_hashes: &mut Vec<(u16, Vec<u64>)>,
    undecodable_buckets: &mut Vec<u16>,
    wanted_parts: &mut Vec<(u16, u8)>,
    merging: bool,
) {
    for mismatch in mismatched {
        match mismatch {
            AeMismatch::Bucket(bucket, peer_entries) => {
                if let Some(counter) = bucket_listings {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
                diff_bucket(shard, bucket, &peer_entries, push_keys, pull_keys, merging).await;
            }
            AeMismatch::Sketch(bucket, cells) => {
                let local_entries = ShardOps::bucket_entries(shard, bucket).await;
                decode_sketch_into(
                    bucket,
                    cells,
                    &local_entries,
                    push_keys,
                    pull_hashes,
                    undecodable_buckets,
                    merging,
                );
            }
            AeMismatch::PartDigests(bucket, remote_parts) => {
                let local_parts = ShardOps::part_digests(shard, vec![bucket])
                    .await
                    .into_iter()
                    .find(|(b, _)| *b == bucket)
                    .map_or_else(Vec::new, |(_, d)| d);
                for part in sundog::mismatched_parts(&local_parts, &remote_parts) {
                    wanted_parts.push((bucket, part));
                }
            }
            // `AeMismatch` is `#[non_exhaustive]`: a reply shape this
            // scenario does not model is left to the full listing.
            other => undecodable_buckets.push(other.bucket()),
        }
    }
}

/// Requests every `(bucket, part)` pair in `wanted_parts` via
/// `Mesh::ae_parts` and classifies each reply, the part-path counterpart of
/// [`classify_ae_mismatches`]'s bucket-scoped arms, split out for the same
/// reason. Returns `false` on a failed exchange, mirroring
/// `run_round_against`'s own give-up-this-round handling.
#[allow(clippy::too_many_arguments)]
async fn resolve_wanted_parts<S: ShardOps>(
    mesh: &Mesh,
    shard: &S,
    peer: NodeId,
    wanted_parts: Vec<(u16, u8)>,
    push_keys: &mut Vec<Bytes>,
    pull_keys: &mut Vec<Bytes>,
    pull_hashes: &mut Vec<(u16, Vec<u64>)>,
    undecodable_buckets: &mut Vec<u16>,
    merging: bool,
) -> bool {
    match tokio::time::timeout(
        NET_TIMEOUT,
        mesh.ae_parts(peer, cache_name(), wanted_parts.clone()),
    )
    .await
    {
        Ok(Ok(replies)) => {
            let local_part_entries = ShardOps::entries_for_parts(shard, wanted_parts).await;
            let local_by_part: HashMap<(u16, u8), Vec<(Bytes, Hlc)>> =
                local_part_entries.into_iter().collect();
            for reply in replies {
                match reply {
                    AePartReply::Listing {
                        bucket,
                        part,
                        entries: peer_entries,
                    } => {
                        let local_entries = local_by_part
                            .get(&(bucket, part))
                            .cloned()
                            .unwrap_or_default();
                        diff_part(&local_entries, &peer_entries, push_keys, pull_keys, merging);
                    }
                    AePartReply::Sketch {
                        bucket,
                        part,
                        cells,
                    } => {
                        let local_entries = local_by_part
                            .get(&(bucket, part))
                            .cloned()
                            .unwrap_or_default();
                        decode_sketch_into(
                            bucket,
                            cells,
                            &local_entries,
                            push_keys,
                            pull_hashes,
                            undecodable_buckets,
                            merging,
                        );
                    }
                    // `AePartReply` is `#[non_exhaustive]`: an unmodelled
                    // reply shape is left to the whole-bucket fallback.
                    other => undecodable_buckets.push(other.bucket()),
                }
            }
            true
        }
        Ok(Err(_)) | Err(_) => false,
    }
}

/// One anti-entropy round against `peer`, reimplementing
/// `run_round_against`'s digest-exchange logic over the same public calls,
/// including the sketch path: a bucket answered as `AeMismatch::Sketch`
/// gets a local comparison sketch built from `bucket_entries`, subtracted
/// against the received one, and peeled via [`sundog::Iblt`]; a decode
/// classifies into pushes and hash-pulls via [`sundog::diff_decoded`], the
/// same function `cluster::anti_entropy::handle_sketch_mismatch` calls, and
/// an `Undecodable` one queues for the `Mesh::ae_entries` listing fallback;
/// and the part path, a bucket answered as `AeMismatch::PartDigests` is
/// compared against this node's own `ShardOps::part_digests` for the same
/// bucket via [`sundog::mismatched_parts`], the differing `(bucket, part)`
/// pairs requested in one `Mesh::ae_parts` call, and each reply classified
/// the same way as the bucket path above but scoped to
/// `ShardOps::entries_for_parts`. `bucket_listings` counts every
/// `AeMismatch::Bucket`/`Msg::AeBucket`-shaped reply this round receives, so
/// a scenario can assert the part path never carries one. Exactly mirrors
/// `run_round_against`'s own shape.
#[allow(
    clippy::too_many_lines,
    reason = "one round's whole digest-exchange-through-repair sequence reads best kept together, \
              mirroring cluster::anti_entropy::run_round_against's own allow"
)]
async fn ae_round_with_sketch<S: ShardOps>(
    mesh: &Mesh,
    shard: &S,
    peer: NodeId,
    bucket_listings: Option<&AtomicUsize>,
) -> bool {
    // Read once per round, exactly as `run_round_against` reads
    // `shard.merges()`: a merging resolver has both sides exchange a
    // mismatched key in the same round instead of only the greater version
    // pushing to the lesser side.
    let merging = ShardOps::merges(shard);
    let local_buckets = ShardOps::digests(shard).await;
    let Ok(Ok(mismatched)) = tokio::time::timeout(
        NET_TIMEOUT,
        mesh.ae_round(peer, cache_name(), local_buckets),
    )
    .await
    else {
        return false;
    };

    let mut push_keys = Vec::new();
    let mut pull_keys = Vec::new();
    let mut pull_hashes: Vec<(u16, Vec<u64>)> = Vec::new();
    let mut undecodable_buckets = Vec::new();
    let mut wanted_parts: Vec<(u16, u8)> = Vec::new();

    classify_ae_mismatches(
        shard,
        mismatched,
        bucket_listings,
        &mut push_keys,
        &mut pull_keys,
        &mut pull_hashes,
        &mut undecodable_buckets,
        &mut wanted_parts,
        merging,
    )
    .await;

    if !wanted_parts.is_empty()
        && !resolve_wanted_parts(
            mesh,
            shard,
            peer,
            wanted_parts,
            &mut push_keys,
            &mut pull_keys,
            &mut pull_hashes,
            &mut undecodable_buckets,
            merging,
        )
        .await
    {
        return false;
    }

    if !undecodable_buckets.is_empty() {
        match tokio::time::timeout(
            NET_TIMEOUT,
            mesh.ae_entries(peer, cache_name(), undecodable_buckets),
        )
        .await
        {
            Ok(Ok(fallback)) => {
                for (bucket, peer_entries) in fallback {
                    diff_bucket(
                        shard,
                        bucket,
                        &peer_entries,
                        &mut push_keys,
                        &mut pull_keys,
                        merging,
                    )
                    .await;
                }
            }
            Ok(Err(_)) | Err(_) => return false,
        }
    }

    if !push_keys.is_empty() {
        for rec in ShardOps::records_for(shard, push_keys).await {
            mesh.send(
                peer,
                MsgClass::Replicate,
                Msg::Replicate {
                    cache: cache_name(),
                    rec,
                },
            );
        }
    }
    let mut ok = true;
    if !pull_keys.is_empty() {
        match tokio::time::timeout(NET_TIMEOUT, mesh.ae_pull(peer, cache_name(), pull_keys)).await {
            Ok(Ok(records)) => {
                for rec in records {
                    ShardOps::apply_remote(shard, rec).await;
                }
            }
            Ok(Err(_)) | Err(_) => ok = false,
        }
    }
    for (bucket, hashes) in pull_hashes {
        match tokio::time::timeout(
            NET_TIMEOUT,
            mesh.ae_pull_hashes(peer, cache_name(), bucket, hashes),
        )
        .await
        {
            Ok(Ok(records)) => {
                for rec in records {
                    ShardOps::apply_remote(shard, rec).await;
                }
            }
            Ok(Err(_)) | Err(_) => ok = false,
        }
    }
    ok
}

/// Mirrors `cluster::anti_entropy::diff_bucket`: `merging` is
/// `ShardOps::merges(shard)`, read once per round by [`ae_round_with_sketch`]
/// and threaded through every classification call in this file the same way
/// `run_round_against` threads it through its own. `false` keeps the
/// greater-version-only push/pull rule; `true` additionally queues the
/// other direction too for a key present on both sides under different
/// versions, so a merging resolver's two replicas exchange records in one
/// round instead of needing a second round to carry a minted result back.
async fn diff_bucket<S: ShardOps>(
    shard: &S,
    bucket: u16,
    peer_entries: &[(Bytes, Hlc)],
    push_keys: &mut Vec<Bytes>,
    pull_keys: &mut Vec<Bytes>,
    merging: bool,
) {
    let peer_by_key: HashMap<Bytes, Hlc> = peer_entries.iter().cloned().collect();
    let mut local_keys = HashSet::with_capacity(peer_by_key.len());

    for (key, local_ver) in ShardOps::bucket_entries(shard, bucket).await {
        local_keys.insert(key.clone());
        match peer_by_key.get(&key) {
            Some(&peer_ver) if local_ver != peer_ver => {
                if local_ver > peer_ver || merging {
                    push_keys.push(key.clone());
                }
                if local_ver < peer_ver || merging {
                    pull_keys.push(key.clone());
                }
            }
            Some(_) => {}
            None => push_keys.push(key),
        }
    }
    for (key, _) in peer_entries {
        if !local_keys.contains(key) {
            pull_keys.push(key.clone());
        }
    }
}

/// [`diff_bucket`]'s comparison, but over an already-fetched local entry
/// list rather than a fresh `ShardOps::bucket_entries` call: the part path's
/// counterpart, since `AePartReply::Listing`'s local side comes from one
/// batched `ShardOps::entries_for_parts` call up front. `merging` is
/// [`diff_bucket`]'s same flag.
fn diff_part(
    local_entries: &[(Bytes, Hlc)],
    peer_entries: &[(Bytes, Hlc)],
    push_keys: &mut Vec<Bytes>,
    pull_keys: &mut Vec<Bytes>,
    merging: bool,
) {
    let peer_by_key: HashMap<&Bytes, Hlc> = peer_entries.iter().map(|(k, v)| (k, *v)).collect();
    let mut local_keys = HashSet::with_capacity(local_entries.len());

    for (key, local_ver) in local_entries {
        local_keys.insert(key);
        match peer_by_key.get(key) {
            Some(&peer_ver) if *local_ver != peer_ver => {
                if *local_ver > peer_ver || merging {
                    push_keys.push(key.clone());
                }
                if *local_ver < peer_ver || merging {
                    pull_keys.push(key.clone());
                }
            }
            Some(_) => {}
            None => push_keys.push(key.clone()),
        }
    }
    for (key, _) in peer_entries {
        if !local_keys.contains(key) {
            pull_keys.push(key.clone());
        }
    }
}

/// One symmetric peer's whole role in scenarios 1 and 2: write its own key
/// range on a timer, fan each write out, run anti-entropy on a separate
/// timer, and dispatch inbound traffic. `remove_on_repeat` turns a key's
/// second occurrence into a remove; `ops_issued` counts every issued op.
#[derive(Clone)]
struct NodeParams {
    node: NodeId,
    label: &'static str,
    port: u16,
    peers: Vec<(NodeId, &'static str, u16)>,
    keys: Vec<u32>,
    write_period: Duration,
    ae_period: Duration,
    dup_factor: usize,
    ae_failures: Option<Arc<AtomicUsize>>,
    remove_on_repeat: bool,
    ops_issued: Option<Arc<AtomicUsize>>,
    /// Overrides the responder's `ae_sketch_min_bucket`; `None` keeps
    /// [`ClusterConfig::default`]'s value, past what any scenario but the
    /// sketch one ever populates a bucket to.
    ae_sketch_min_bucket: Option<usize>,
    /// Overrides the responder's `ae_part_min_bucket`; `None` keeps
    /// [`ClusterConfig::default`]'s value, past what any scenario but the
    /// part one ever populates a bucket to.
    ae_part_min_bucket: Option<usize>,
    /// Counts every `AeMismatch::Bucket` reply this node's rounds receive:
    /// a full bucket listing, the cost the part path exists to avoid. `None`
    /// for scenarios that don't care.
    bucket_listings: Option<Arc<AtomicUsize>>,
}

async fn node_loop(params: NodeParams, shard: Arc<TestShard>) -> SimResult {
    let defaults = ClusterConfig::default();
    let handler: Arc<dyn RequestHandler> = Arc::new(ShardHandler::with_min_buckets(
        Arc::clone(&shard),
        params
            .ae_part_min_bucket
            .unwrap_or(defaults.ae_part_min_bucket),
        params
            .ae_sketch_min_bucket
            .unwrap_or(defaults.ae_sketch_min_bucket),
    ));
    let bind_addr = SocketAddr::from(([0, 0, 0, 0], params.port));
    let (mesh, mut inbound) = Mesh::spawn(
        bind_addr,
        params.node,
        1,
        &ClusterConfig::default(),
        handler,
    )
    .await?;

    let peer_list = peer_list_of(&params.peers);
    mesh.update_peers(peer_list.clone());
    let peer_ids: Vec<NodeId> = peer_list.iter().map(|peer| peer.node).collect();

    let mut keys = params.keys.into_iter();
    let mut seen: HashSet<u32> = HashSet::new();
    let mut write_tick = tokio::time::interval(params.write_period);
    write_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut ae_tick = tokio::time::interval(params.ae_period);
    ae_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            biased;
            Some(InboundMsg { msg, .. }) = inbound.recv() => {
                dispatch_inbound(shard.as_ref(), msg).await;
            }
            _ = write_tick.tick() => {
                if let Some(key) = keys.next() {
                    if params.remove_on_repeat && !seen.insert(key) {
                        let _ = shard.remove(&key).await;
                    } else {
                        let value = format!("{}:{key}", params.label);
                        let _ = shard.insert(key, value).await;
                    }
                    fan_out(shard.as_ref(), &mesh, &peer_ids, key, params.dup_factor).await;
                    if let Some(counter) = params.ops_issued.as_ref() {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            _ = ae_tick.tick() => {
                for &peer in &peer_ids {
                    if !ae_round_with_sketch(
                        &mesh,
                        shard.as_ref(),
                        peer,
                        params.bucket_listings.as_deref(),
                    )
                    .await
                        && let Some(counter) = params.ae_failures.as_ref()
                    {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }
}

/// A pair of symmetric peers' fixed setup: hosts, ids, key ranges, timing.
struct PairSpec {
    host_a: &'static str,
    host_b: &'static str,
    node_a: NodeId,
    node_b: NodeId,
    port: u16,
    keys_a: Vec<u32>,
    keys_b: Vec<u32>,
    write_period: Duration,
    ae_period: Duration,
    dup_factor: usize,
}

fn spawn_symmetric_pair(
    sim: &mut Sim<'_>,
    spec: PairSpec,
    shard_a: Arc<TestShard>,
    shard_b: Arc<TestShard>,
    ae_failures: Option<Arc<AtomicUsize>>,
) {
    let params_a = NodeParams {
        node: spec.node_a,
        label: "a",
        port: spec.port,
        peers: vec![(spec.node_b, spec.host_b, spec.port)],
        keys: spec.keys_a,
        write_period: spec.write_period,
        ae_period: spec.ae_period,
        dup_factor: spec.dup_factor,
        ae_failures: ae_failures.clone(),
        remove_on_repeat: false,
        ops_issued: None,
        ae_sketch_min_bucket: None,
        ae_part_min_bucket: None,
        bucket_listings: None,
    };
    sim.host(spec.host_a, move || {
        let shard = Arc::clone(&shard_a);
        let params = params_a.clone();
        async move { node_loop(params, shard).await }
    });

    let params_b = NodeParams {
        node: spec.node_b,
        label: "b",
        port: spec.port,
        peers: vec![(spec.node_a, spec.host_a, spec.port)],
        keys: spec.keys_b,
        write_period: spec.write_period,
        ae_period: spec.ae_period,
        dup_factor: spec.dup_factor,
        ae_failures,
        remove_on_repeat: false,
        ops_issued: None,
        ae_sketch_min_bucket: None,
        ae_part_min_bucket: None,
        bucket_listings: None,
    };
    sim.host(spec.host_b, move || {
        let shard = Arc::clone(&shard_b);
        let params = params_b.clone();
        async move { node_loop(params, shard).await }
    });
}

fn run_steps(sim: &mut Sim<'_>, count: usize) {
    for _ in 0..count {
        sim.step().expect("turmoil step succeeds");
    }
}

fn steps_for(duration: Duration) -> usize {
    let ticks = duration.as_millis() / TICK.as_millis();
    usize::try_from(ticks)
        .expect("test-scale durations fit in a usize step count")
        .max(1)
}

/// Steps `sim` until `converged` reports success or `max_steps` is spent,
/// returning the step count on success.
fn run_until(
    sim: &mut Sim<'_>,
    max_steps: usize,
    mut converged: impl FnMut() -> bool,
) -> Option<usize> {
    for step in 1..=max_steps {
        sim.step().expect("turmoil step succeeds");
        if converged() {
            return Some(step);
        }
    }
    None
}

#[test]
fn partition_during_writes_converges_within_five_ae_rounds() {
    let node_a = NodeId::from(9201);
    let node_b = NodeId::from(9202);
    let ae_period = Duration::from_millis(250);
    let keys_a: Vec<u32> = (0..5).collect();
    let keys_b: Vec<u32> = (100..105).collect();

    let shard_a = Arc::new(new_shard(node_a));
    let shard_b = Arc::new(new_shard(node_b));

    let mut sim = Builder::new()
        .rng_seed(sim_seed(0xA11C_E001))
        .tick_duration(TICK)
        .max_message_latency(Duration::from_millis(20))
        .build();

    spawn_symmetric_pair(
        &mut sim,
        PairSpec {
            host_a: "node-a",
            host_b: "node-b",
            node_a,
            node_b,
            port: 4000,
            keys_a: keys_a.clone(),
            keys_b: keys_b.clone(),
            write_period: Duration::from_millis(50),
            ae_period,
            dup_factor: 1,
        },
        Arc::clone(&shard_a),
        Arc::clone(&shard_b),
        None,
    );

    // Partition before the write burst, then heal and bound convergence.
    sim.partition("node-a", "node-b");
    run_steps(&mut sim, steps_for(Duration::from_millis(750)));
    sim.repair("node-a", "node-b");

    let budget = steps_for(ae_period * 5 + Duration::from_millis(500));
    let converged = run_until(&mut sim, budget, || {
        digests_of(&shard_a) == digests_of(&shard_b)
    });
    assert!(
        converged.is_some(),
        "digests converge within five AE-round intervals of healing"
    );

    for &key in keys_a.iter().chain(keys_b.iter()) {
        assert!(
            value_of(&shard_a, key).is_some(),
            "node-a missing key {key}"
        );
        assert!(
            value_of(&shard_b, key).is_some(),
            "node-b missing key {key}"
        );
    }
    assert_eq!(value_of(&shard_a, 100).as_deref(), Some("b:100"));
    assert_eq!(value_of(&shard_b, 0).as_deref(), Some("a:0"));
}

struct StormStats {
    steps_to_converge: usize,
    ae_failures: usize,
    keys_present_a: usize,
    keys_present_b: usize,
}

/// Message loss, latency spread, and a duplicate storm running concurrently
/// with live writes; `fail_rate` alone breaks and heals links throughout.
fn run_storm_scenario(seed: u64) -> StormStats {
    let node_a = NodeId::from(11);
    let node_b = NodeId::from(12);
    let keys_a: Vec<u32> = (0..8).collect();
    let keys_b: Vec<u32> = (200..208).collect();
    let total_keys = keys_a.len() + keys_b.len();

    let shard_a = Arc::new(new_shard(node_a));
    let shard_b = Arc::new(new_shard(node_b));
    let ae_failures = Arc::new(AtomicUsize::new(0));

    let write_period = Duration::from_millis(30);
    let mut sim = Builder::new()
        .rng_seed(seed)
        .tick_duration(TICK)
        // Turmoil's `fail_rate` has no TCP-segment retransmission, so a
        // dropped chunk can break a whole in-flight connection; kept low
        // enough that AE rounds still routinely succeed within a few retries.
        .fail_rate(0.03)
        .repair_rate(0.75)
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(60))
        .build();

    spawn_symmetric_pair(
        &mut sim,
        PairSpec {
            host_a: "node-a",
            host_b: "node-b",
            node_a,
            node_b,
            port: 4100,
            keys_a: keys_a.clone(),
            keys_b: keys_b.clone(),
            write_period,
            ae_period: Duration::from_millis(150),
            dup_factor: 4,
        },
        Arc::clone(&shard_a),
        Arc::clone(&shard_b),
        Some(Arc::clone(&ae_failures)),
    );

    // Both sides write concurrently with AE, so digest equality can hold
    // trivially early. Wait explicitly for every key to exist on its own
    // origin shard before treating digest equality as real convergence.
    let own_writes_issued =
        |shard: &TestShard, keys: &[u32]| keys.iter().all(|&key| value_of(shard, key).is_some());
    run_until(&mut sim, steps_for(Duration::from_secs(10)), || {
        own_writes_issued(&shard_a, &keys_a) && own_writes_issued(&shard_b, &keys_b)
    })
    .expect("both sides finish issuing their own writes within the budget");

    let budget = steps_for(Duration::from_secs(20));
    let steps_to_converge = run_until(&mut sim, budget, || {
        digests_of(&shard_a) == digests_of(&shard_b)
    })
    .expect("anti-entropy converges despite loss/reorder/duplication within the budget");

    let present = |shard: &TestShard| {
        keys_a
            .iter()
            .chain(keys_b.iter())
            .filter(|&&key| value_of(shard, key).is_some())
            .count()
    };
    let keys_present_a = present(&shard_a);
    let keys_present_b = present(&shard_b);
    assert_eq!(
        keys_present_a, total_keys,
        "node-a holds every key once converged"
    );
    assert_eq!(
        keys_present_b, total_keys,
        "node-b holds every key once converged"
    );

    StormStats {
        steps_to_converge,
        ae_failures: ae_failures.load(Ordering::Relaxed),
        keys_present_a,
        keys_present_b,
    }
}

#[test]
fn loss_reorder_duplicate_storm_still_converges() {
    run_storm_scenario(sim_seed(0x5707_2201));
}

#[test]
fn simulation_is_reproducible_for_a_fixed_seed() {
    // Digest values embed each write's `Hlc`, stamped from real wall-clock
    // time, so they are not reproducible across runs. What is a pure
    // function of the seed is turmoil's own network schedule.
    let run1 = run_storm_scenario(0x5EED_0042);
    let run2 = run_storm_scenario(0x5EED_0042);

    assert_eq!(
        run1.steps_to_converge, run2.steps_to_converge,
        "the same seed converges at the same simulated step"
    );
    assert_eq!(
        run1.ae_failures, run2.ae_failures,
        "the same seed reproduces the same count of failed AE rounds"
    );
    assert_eq!(run1.keys_present_a, run2.keys_present_a);
    assert_eq!(run1.keys_present_b, run2.keys_present_b);
}

fn spawn_donor(
    sim: &mut Sim<'_>,
    host: &'static str,
    node: NodeId,
    port: u16,
    shard: Arc<TestShard>,
    peers: Vec<(NodeId, &'static str, u16)>,
) {
    sim.host(host, move || {
        let shard = Arc::clone(&shard);
        let peers = peers.clone();
        async move { donor_software(node, port, peers, shard).await }
    });
}

async fn donor_software(
    node: NodeId,
    port: u16,
    peers: Vec<(NodeId, &'static str, u16)>,
    shard: Arc<TestShard>,
) -> SimResult {
    let handler: Arc<dyn RequestHandler> = Arc::new(ShardHandler::new(Arc::clone(&shard)));
    let bind_addr = SocketAddr::from(([0, 0, 0, 0], port));
    let (mesh, mut inbound) =
        Mesh::spawn(bind_addr, node, 1, &ClusterConfig::default(), handler).await?;
    mesh.update_peers(peer_list_of(&peers));
    loop {
        let Some(InboundMsg { msg, .. }) = inbound.recv().await else {
            return Ok(());
        };
        dispatch_inbound(shard.as_ref(), msg).await;
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_receiver(
    sim: &mut Sim<'_>,
    host: &'static str,
    node: NodeId,
    port: u16,
    donors: Vec<(NodeId, &'static str, u16)>,
    shard: Arc<TestShard>,
    applied: Arc<AtomicUsize>,
    done: Arc<AtomicBool>,
) {
    sim.host(host, move || {
        let shard = Arc::clone(&shard);
        let donors = donors.clone();
        let applied = Arc::clone(&applied);
        let done = Arc::clone(&done);
        async move { receiver_software(node, port, donors, shard, applied, done).await }
    });
}

async fn receiver_software(
    node: NodeId,
    port: u16,
    donors: Vec<(NodeId, &'static str, u16)>,
    shard: Arc<TestShard>,
    applied: Arc<AtomicUsize>,
    done: Arc<AtomicBool>,
) -> SimResult {
    let handler: Arc<dyn RequestHandler> = Arc::new(ShardHandler::new(Arc::clone(&shard)));
    let bind_addr = SocketAddr::from(([0, 0, 0, 0], port));
    let (mesh, mut inbound) =
        Mesh::spawn(bind_addr, node, 1, &ClusterConfig::default(), handler).await?;
    mesh.update_peers(peer_list_of(&donors));
    let donor_ids: Vec<NodeId> = donors.iter().map(|&(id, _, _)| id).collect();

    let ok = warm_up(&mesh, shard.as_ref(), &donor_ids, &applied).await;
    done.store(ok, Ordering::Relaxed);

    loop {
        let Some(InboundMsg { msg, .. }) = inbound.recv().await else {
            return Ok(());
        };
        dispatch_inbound(shard.as_ref(), msg).await;
    }
}

/// A simplified stand-in for `cluster::state_transfer::run`: tries each
/// donor in order, applying records as they stream in, and falls through
/// to the next donor the moment the stream reports an error.
async fn warm_up(mesh: &Mesh, shard: &TestShard, donors: &[NodeId], applied: &AtomicUsize) -> bool {
    for &donor in donors {
        // A donor that declines, `Ok(None)`, is skipped like an unreachable one.
        let Ok(Ok(Some(mut stream))) =
            tokio::time::timeout(NET_TIMEOUT, mesh.request_state(donor, cache_name())).await
        else {
            continue;
        };
        let mut broke = false;
        loop {
            match tokio::time::timeout(NET_TIMEOUT, stream.next()).await {
                Ok(Some(Ok(chunk))) => {
                    applied.fetch_add(chunk.len(), Ordering::Relaxed);
                    ShardOps::apply_remote_batch(shard, chunk).await;
                }
                Ok(Some(Err(_))) | Err(_) => {
                    broke = true;
                    break;
                }
                Ok(None) => break,
            }
        }
        if !broke {
            return true;
        }
    }
    false
}

fn new_shard_with_tombstone_ttl(
    node: NodeId,
    tombstone_ttl: Duration,
    tombstone_max_ttl: Duration,
) -> TestShard {
    Shard::new(cache_name(), Mode::Replicated, node, 100_000, None, None)
        .with_tombstone_ttl(tombstone_ttl)
        .with_tombstone_max_ttl(tombstone_max_ttl)
}

/// Runs the scenario that motivates partition-aware tombstone retention: two
/// nodes converged on a key, a partition, the survivor deleting the key,
/// real time passing past `tombstone_ttl` while still partitioned, a heal,
/// then anti-entropy. Returns each side's value once digests converge, or
/// the budget runs out, plus whether they did.
///
/// `defer_while_absent` stands in for `should_defer_gc`'s decision, handed
/// straight to the real [`ShardOps::gc_tombstones`]. `true` defers
/// collecting node-b's tombstone while node-a stays absent; `false`
/// collects it unconditionally.
fn run_partition_delete_scenario(
    seed: u64,
    port: u16,
    defer_while_absent: bool,
) -> (Option<String>, Option<String>, bool) {
    let node_a = NodeId::from(u64::from(port) * 10 + 1);
    let node_b = NodeId::from(u64::from(port) * 10 + 2);
    let key = 555u32;
    let tombstone_ttl = Duration::from_millis(30);
    let tombstone_max_ttl = Duration::from_secs(60);

    let shard_a = Arc::new(new_shard_with_tombstone_ttl(
        node_a,
        tombstone_ttl,
        tombstone_max_ttl,
    ));
    let shard_b = Arc::new(new_shard_with_tombstone_ttl(
        node_b,
        tombstone_ttl,
        tombstone_max_ttl,
    ));

    block_on(shard_a.insert(key, "original".to_string())).expect("insert");
    let rec = block_on(ShardOps::records_for(
        shard_a.as_ref(),
        vec![key_bytes(key)],
    ))
    .into_iter()
    .next()
    .expect("the freshly inserted key has a record");
    block_on(ShardOps::apply_remote(shard_b.as_ref(), rec));
    assert_eq!(value_of(&shard_a, key), Some("original".to_string()));
    assert_eq!(value_of(&shard_b, key), Some("original".to_string()));
    assert_eq!(
        digests_of(&shard_a),
        digests_of(&shard_b),
        "both sides start converged"
    );

    let mut sim = Builder::new()
        .rng_seed(seed)
        .tick_duration(TICK)
        .max_message_latency(Duration::from_millis(20))
        .build();

    let ae_period = Duration::from_millis(100);
    spawn_symmetric_pair(
        &mut sim,
        PairSpec {
            host_a: "resurrect-a",
            host_b: "resurrect-b",
            node_a,
            node_b,
            port,
            keys_a: vec![],
            keys_b: vec![],
            // No automatic writes here; the delete below is applied directly.
            write_period: Duration::from_secs(3600),
            ae_period,
            dup_factor: 1,
        },
        Arc::clone(&shard_a),
        Arc::clone(&shard_b),
        None,
    );

    sim.partition("resurrect-a", "resurrect-b");
    run_steps(&mut sim, steps_for(Duration::from_millis(200)));

    block_on(shard_b.remove(&key)).expect("remove creates a tombstone");
    assert_eq!(
        value_of(&shard_b, key),
        None,
        "the survivor's own read reflects its delete immediately"
    );

    // Tombstone deadlines are stamped from real `SystemTime`, not turmoil's
    // virtual clock, so real time must pass past `tombstone_ttl`.
    std::thread::sleep(tombstone_ttl * 10);

    block_on(ShardOps::gc_tombstones(
        shard_b.as_ref(),
        defer_while_absent,
    ));

    sim.repair("resurrect-a", "resurrect-b");
    let budget = steps_for(ae_period * 10);
    let converged = run_until(&mut sim, budget, || {
        digests_of(&shard_a) == digests_of(&shard_b)
    })
    .is_some();

    (value_of(&shard_a, key), value_of(&shard_b, key), converged)
}

/// Proves the semantic goal directly: a member absent past
/// `tombstone_ttl` must not resurrect a manually deleted entry on heal.
/// Deferral keeps node-b's tombstone alive until node-a is reachable again.
#[test]
fn partition_survivor_delete_does_not_resurrect_after_heal() {
    let (value_a, value_b, converged) =
        run_partition_delete_scenario(sim_seed(0x2E1E_7A01), 4400, true);
    assert!(
        converged,
        "digests converge within the AE-round budget after healing"
    );
    assert_eq!(
        value_a, None,
        "node-a does not resurrect the deleted key after heal + AE"
    );
    assert_eq!(value_b, None, "node-b keeps the key deleted");
}

/// The counter-case, proving deferral is load-bearing: the same scenario
/// with unconditional GC (`defer_while_absent: false`) lets node-b forget
/// the tombstone while node-a is still absent, so anti-entropy pulls the
/// stale value back once healed.
#[test]
fn tombstone_deferral_is_load_bearing_against_resurrection() {
    let (_, value_b_unconditional, converged_unconditional) =
        run_partition_delete_scenario(sim_seed(0x2E1E_7A02), 4410, false);
    assert!(
        converged_unconditional,
        "digests converge within the budget, onto the wrong, resurrected state"
    );
    assert_eq!(
        value_b_unconditional,
        Some("original".to_string()),
        "counter-case: collecting the tombstone unconditionally while node-a is still absent \
         lets anti-entropy resurrect the deleted key on node-b once healed"
    );

    let (deferred_value_a, deferred_value_b, converged_deferred) =
        run_partition_delete_scenario(sim_seed(0x2E1E_7A03), 4420, true);
    assert!(converged_deferred, "digests converge under deferral too");
    assert_eq!(
        deferred_value_a, None,
        "same scenario, deferred: node-a stays deleted"
    );
    assert_eq!(
        deferred_value_b, None,
        "same scenario, deferred: node-b stays deleted, closing exactly the gap \
         the unconditional case above demonstrated"
    );
}

/// A link that flaps: six partition/heal cycles in quick succession, each
/// shorter than an AE interval, with both sides writing throughout. Once
/// flapping stops, convergence completes within the usual five-round bound.
#[test]
fn link_flapping_under_writes_converges_after_final_heal() {
    let node_a = NodeId::from(31);
    let node_b = NodeId::from(32);
    let ae_period = Duration::from_millis(250);
    let keys_a: Vec<u32> = (0..12).collect();
    let keys_b: Vec<u32> = (300..312).collect();

    let shard_a = Arc::new(new_shard(node_a));
    let shard_b = Arc::new(new_shard(node_b));

    let mut sim = Builder::new()
        .rng_seed(sim_seed(0xF1A9_9001))
        .tick_duration(TICK)
        .max_message_latency(Duration::from_millis(20))
        .build();

    spawn_symmetric_pair(
        &mut sim,
        PairSpec {
            host_a: "flap-a",
            host_b: "flap-b",
            node_a,
            node_b,
            port: 4500,
            keys_a: keys_a.clone(),
            keys_b: keys_b.clone(),
            write_period: Duration::from_millis(40),
            ae_period,
            dup_factor: 1,
        },
        Arc::clone(&shard_a),
        Arc::clone(&shard_b),
        None,
    );

    // 6 x (300ms down + 200ms up); both write sequences finish mid-flap.
    for _ in 0..6 {
        sim.partition("flap-a", "flap-b");
        run_steps(&mut sim, steps_for(Duration::from_millis(300)));
        sim.repair("flap-a", "flap-b");
        run_steps(&mut sim, steps_for(Duration::from_millis(200)));
    }

    let budget = steps_for(ae_period * 5 + Duration::from_millis(500));
    let converged = run_until(&mut sim, budget, || {
        digests_of(&shard_a) == digests_of(&shard_b)
    });
    assert!(
        converged.is_some(),
        "digests converge within five AE-round intervals of the final heal"
    );
    for &key in keys_a.iter().chain(keys_b.iter()) {
        assert!(
            value_of(&shard_a, key).is_some() && value_of(&shard_b, key).is_some(),
            "both sides hold key {key} after the flapping stops"
        );
    }
}

/// An asymmetric fault: `partition_oneway` drops everything node-a sends to
/// node-b while node-b's path to node-a keeps delivering. The healthy
/// direction must keep replicating during the fault, and the broken
/// direction's backlog must repair once the link heals.
#[test]
fn one_way_partition_delivers_the_healthy_direction_and_heals() {
    let node_a = NodeId::from(41);
    let node_b = NodeId::from(42);
    let ae_period = Duration::from_millis(200);
    let keys_a: Vec<u32> = (0..8).collect();
    let keys_b: Vec<u32> = (400..408).collect();

    let shard_a = Arc::new(new_shard(node_a));
    let shard_b = Arc::new(new_shard(node_b));

    let mut sim = Builder::new()
        .rng_seed(sim_seed(0x0E1A_A701))
        .tick_duration(TICK)
        .max_message_latency(Duration::from_millis(20))
        .build();

    spawn_symmetric_pair(
        &mut sim,
        PairSpec {
            host_a: "oneway-a",
            host_b: "oneway-b",
            node_a,
            node_b,
            port: 4600,
            keys_a: keys_a.clone(),
            keys_b: keys_b.clone(),
            write_period: Duration::from_millis(100),
            ae_period,
            dup_factor: 1,
        },
        Arc::clone(&shard_a),
        Arc::clone(&shard_b),
        None,
    );

    // Let connections establish and a few writes cross, then break the
    // a-to-b direction only; write sequences keep issuing past this point.
    run_steps(&mut sim, steps_for(Duration::from_millis(300)));
    sim.partition_oneway("oneway-a", "oneway-b");

    // The healthy direction keeps working regardless of the fault.
    let fault_budget = steps_for(Duration::from_secs(10));
    run_until(&mut sim, fault_budget, || {
        keys_b.iter().all(|&key| value_of(&shard_a, key).is_some())
    })
    .expect("node-b's writes keep replicating to node-a during the one-way fault");

    // The broken direction stays broken: the two sides still disagree.
    assert_ne!(
        digests_of(&shard_a),
        digests_of(&shard_b),
        "node-b is missing node-a's post-fault writes while a→b is down"
    );

    sim.repair_oneway("oneway-a", "oneway-b");
    let budget = steps_for(ae_period * 10 + Duration::from_millis(500));
    let converged = run_until(&mut sim, budget, || {
        digests_of(&shard_a) == digests_of(&shard_b)
    });
    assert!(
        converged.is_some(),
        "digests converge within ten AE-round intervals of repairing a→b"
    );
    for &key in keys_a.iter().chain(keys_b.iter()) {
        assert!(
            value_of(&shard_a, key).is_some() && value_of(&shard_b, key).is_some(),
            "both sides hold key {key} after the one-way fault heals"
        );
    }
}

/// A permanently slow link, an order of magnitude above the other
/// scenarios, with live writes on both sides. Nothing is lost, only late:
/// replication and anti-entropy must still converge within a bounded budget.
#[test]
fn sustained_high_latency_still_converges() {
    let node_a = NodeId::from(51);
    let node_b = NodeId::from(52);
    let ae_period = Duration::from_millis(400);
    let keys_a: Vec<u32> = (0..8).collect();
    let keys_b: Vec<u32> = (500..508).collect();

    let shard_a = Arc::new(new_shard(node_a));
    let shard_b = Arc::new(new_shard(node_b));

    let mut sim = Builder::new()
        .rng_seed(sim_seed(0x51_0111))
        .tick_duration(TICK)
        .min_message_latency(Duration::from_millis(50))
        .max_message_latency(Duration::from_millis(150))
        .build();

    spawn_symmetric_pair(
        &mut sim,
        PairSpec {
            host_a: "slow-a",
            host_b: "slow-b",
            node_a,
            node_b,
            port: 4700,
            keys_a: keys_a.clone(),
            keys_b: keys_b.clone(),
            write_period: Duration::from_millis(50),
            ae_period,
            dup_factor: 1,
        },
        Arc::clone(&shard_a),
        Arc::clone(&shard_b),
        None,
    );

    let budget = steps_for(Duration::from_secs(30));
    let converged = run_until(&mut sim, budget, || {
        digests_of(&shard_a) == digests_of(&shard_b)
            && keys_a
                .iter()
                .chain(keys_b.iter())
                .all(|&key| value_of(&shard_a, key).is_some())
    });
    assert!(
        converged.is_some(),
        "a slow-but-lossless link still converges within the budget"
    );
    for &key in keys_a.iter().chain(keys_b.iter()) {
        assert_eq!(
            value_of(&shard_a, key),
            value_of(&shard_b, key),
            "both sides agree on key {key} under sustained high latency"
        );
    }
}

/// High-frequency entry lifecycle under loss: both nodes run overlapping
/// insert-then-remove schedules over a shared key range on a lossy,
/// reordering link. Every even key ends removed, every odd key's last
/// operation is an insert, so the converged state must be the correct one.
#[test]
fn add_remove_churn_under_loss_converges_to_the_correct_state() {
    let node_a = NodeId::from(61);
    let node_b = NodeId::from(62);
    let port = 4800;
    // First pass inserts the range, second pass removes its even keys.
    let plan_a: Vec<u32> = (0..16).chain((0..16).step_by(2)).collect();
    let plan_b: Vec<u32> = (8..24).chain((8..24).step_by(2)).collect();

    let shard_a = Arc::new(new_shard(node_a));
    let shard_b = Arc::new(new_shard(node_b));
    let ops_a = Arc::new(AtomicUsize::new(0));
    let ops_b = Arc::new(AtomicUsize::new(0));

    let mut sim = Builder::new()
        .rng_seed(sim_seed(0xC4B4_A901))
        .tick_duration(TICK)
        // Same loss rate as the storm scenario: AE rounds routinely fail
        // and retry rather than packets occasionally vanishing.
        .fail_rate(0.03)
        .repair_rate(0.75)
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(60))
        .build();

    let params_a = NodeParams {
        node: node_a,
        label: "a",
        port,
        peers: vec![(node_b, "churn-b", port)],
        keys: plan_a.clone(),
        write_period: Duration::from_millis(25),
        ae_period: Duration::from_millis(150),
        dup_factor: 2,
        ae_failures: None,
        remove_on_repeat: true,
        ops_issued: Some(Arc::clone(&ops_a)),
        ae_sketch_min_bucket: None,
        ae_part_min_bucket: None,
        bucket_listings: None,
    };
    let shard = Arc::clone(&shard_a);
    sim.host("churn-a", move || {
        let shard = Arc::clone(&shard);
        let params = params_a.clone();
        async move { node_loop(params, shard).await }
    });

    let params_b = NodeParams {
        node: node_b,
        label: "b",
        port,
        peers: vec![(node_a, "churn-a", port)],
        keys: plan_b.clone(),
        write_period: Duration::from_millis(25),
        ae_period: Duration::from_millis(150),
        dup_factor: 2,
        ae_failures: None,
        remove_on_repeat: true,
        ops_issued: Some(Arc::clone(&ops_b)),
        ae_sketch_min_bucket: None,
        ae_part_min_bucket: None,
        bucket_listings: None,
    };
    let shard = Arc::clone(&shard_b);
    sim.host("churn-b", move || {
        let shard = Arc::clone(&shard);
        let params = params_b.clone();
        async move { node_loop(params, shard).await }
    });

    run_until(&mut sim, steps_for(Duration::from_secs(15)), || {
        ops_a.load(Ordering::Relaxed) >= plan_a.len()
            && ops_b.load(Ordering::Relaxed) >= plan_b.len()
    })
    .expect("both churn sequences finish issuing within the budget");

    run_until(&mut sim, steps_for(Duration::from_secs(20)), || {
        digests_of(&shard_a) == digests_of(&shard_b)
    })
    .expect("churned shards converge despite loss within the budget");

    for key in (0..24u32).step_by(2) {
        assert_eq!(
            value_of(&shard_a, key),
            None,
            "removed key {key} stays removed on node-a"
        );
        assert_eq!(
            value_of(&shard_b, key),
            None,
            "removed key {key} stays removed on node-b"
        );
    }
    for key in (1..24u32).step_by(2) {
        let (on_a, on_b) = (value_of(&shard_a, key), value_of(&shard_b, key));
        assert!(
            on_a.is_some(),
            "surviving key {key} is present once converged"
        );
        assert_eq!(on_a, on_b, "both sides agree on surviving key {key}");
    }
}

#[test]
fn donor_crash_mid_state_transfer_repicks_and_completes() {
    let donor1 = NodeId::from(21);
    let donor2 = NodeId::from(22);
    let receiver = NodeId::from(23);
    let total_keys = 1_200u32;
    let port = 4200;

    let shard_d1 = Arc::new(seed_shard(donor1, 0..total_keys));
    let shard_d2 = Arc::new(seed_shard(donor2, 0..total_keys));
    let shard_r = Arc::new(new_shard(receiver));

    let applied = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicBool::new(false));

    let mut sim = Builder::new()
        .rng_seed(sim_seed(0xD0A0_5501))
        .tick_duration(TICK)
        .min_message_latency(Duration::from_millis(2))
        .max_message_latency(Duration::from_millis(30))
        .build();

    spawn_donor(
        &mut sim,
        "donor-1",
        donor1,
        port,
        Arc::clone(&shard_d1),
        vec![(receiver, "receiver", port)],
    );
    spawn_donor(
        &mut sim,
        "donor-2",
        donor2,
        port,
        Arc::clone(&shard_d2),
        vec![(receiver, "receiver", port)],
    );
    spawn_receiver(
        &mut sim,
        "receiver",
        receiver,
        port,
        vec![(donor1, "donor-1", port), (donor2, "donor-2", port)],
        Arc::clone(&shard_r),
        Arc::clone(&applied),
        Arc::clone(&done),
    );

    let mut crashed = false;
    let max_steps = steps_for(Duration::from_secs(20));
    for _ in 0..max_steps {
        sim.step().expect("turmoil step succeeds");
        if !crashed && applied.load(Ordering::Relaxed) >= 200 {
            sim.crash("donor-1");
            crashed = true;
        }
        if done.load(Ordering::Relaxed) {
            break;
        }
    }

    assert!(
        crashed,
        "test setup sanity: donor-1 is crashed mid-transfer"
    );
    assert!(
        done.load(Ordering::Relaxed),
        "receiver re-picks the surviving donor and completes warm-up within the step budget"
    );
    for key in [0u32, 599, total_keys - 1] {
        assert_eq!(
            value_of(&shard_r, key),
            value_of(&shard_d2, key),
            "receiver's warmed copy matches the surviving donor for key {key}"
        );
    }
}

/// Mirrors `store::bucket_of`'s formula so a fixed key range can be
/// searched for a dense bucket without that private function; `cluster.rs`'s
/// own tests carry the identical helper for the same reason.
fn bucket_of_bytes(key_bytes: &[u8]) -> u16 {
    let bucket = xxh3_64(key_bytes) & (sundog::store::BUCKET_COUNT as u64 - 1);
    u16::try_from(bucket).expect("invariant: masked to BUCKET_COUNT - 1, always fits in u16")
}

fn bucket_of_u32(key: u32) -> u16 {
    bucket_of_bytes(&key_bytes(key))
}

/// Among `0..n`, every key in a bucket holding more than `min_count` of
/// them. Deterministic given a fixed key range.
fn dense_bucket_keys(n: u32, min_count: usize) -> Vec<u32> {
    let mut by_bucket: HashMap<u16, Vec<u32>> = HashMap::new();
    for key in 0..n {
        by_bucket.entry(bucket_of_u32(key)).or_default().push(key);
    }
    by_bucket
        .into_values()
        .find(|keys| keys.len() > min_count)
        .expect("at least one bucket exceeds min_count among this many keys")
}

/// Two shards start byte-identical across 4,096 keys, dense enough that a
/// forced `ae_sketch_min_bucket` of 4 puts several hundred entries in some
/// buckets; the responder answers a mismatch there with `AeMismatch::Sketch`
/// rather than a listing. One key is then dropped locally on node-b, as if
/// its `Replicate` never arrived, and the two nodes run `ae_round_with_sketch`
/// against each other on a timer under turmoil packet loss and reordering.
/// Recovery goes through the full sketch machinery: `sundog::Iblt` built and
/// peeled locally, `sundog::diff_decoded`'s classification, and
/// `Mesh::ae_pull_hashes` for the pull, with `Mesh::ae_entries` available as
/// the undecodable fallback though this scenario's single-key diff always
/// peels clean.
#[test]
fn sketch_reconciliation_under_loss_converges() {
    const N: u32 = 4096;
    const MIN_BUCKET: usize = 4;
    let node_a = NodeId::from(71);
    let node_b = NodeId::from(72);
    let port = 4900;

    let shard_a = Arc::new(seed_shard(node_a, 0..N));
    let shard_b = Arc::new(new_shard(node_b));
    block_on(async {
        let mut chunks = ShardOps::snapshot_chunks(shard_a.as_ref());
        while let Some(chunk) = chunks.next().await {
            ShardOps::apply_remote_batch(shard_b.as_ref(), chunk).await;
        }
    });
    assert_eq!(
        digests_of(&shard_a),
        digests_of(&shard_b),
        "both sides start converged before the drop"
    );

    // A bucket dense enough that MIN_BUCKET's threshold answers it as an
    // IBLT sketch instead of a full listing.
    let bucket_keys = dense_bucket_keys(N, MIN_BUCKET + 1);
    let target_key = bucket_keys[0];
    block_on(shard_b.invalidate_local(&target_key));
    assert_eq!(
        value_of(&shard_b, target_key),
        None,
        "node-b's copy is dropped, as if a Replicate never arrived"
    );
    assert_ne!(
        digests_of(&shard_a),
        digests_of(&shard_b),
        "test setup sanity: the drop makes one bucket mismatch"
    );

    let mut sim = Builder::new()
        .rng_seed(sim_seed(0x5CE7_C401))
        .tick_duration(TICK)
        // Same loss/reorder shape as the storm scenario: AE rounds routinely
        // fail and retry rather than never losing a packet.
        .fail_rate(0.03)
        .repair_rate(0.75)
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(60))
        .build();

    let ae_period = Duration::from_millis(150);
    let base_params = NodeParams {
        node: node_a,
        label: "a",
        port,
        peers: vec![(node_b, "sketch-b", port)],
        keys: vec![],
        // No writes in this scenario: the fixed dataset plus the one drop
        // is the whole story.
        write_period: Duration::from_secs(3600),
        ae_period,
        dup_factor: 1,
        ae_failures: None,
        remove_on_repeat: false,
        ops_issued: None,
        ae_sketch_min_bucket: Some(MIN_BUCKET),
        ae_part_min_bucket: None,
        bucket_listings: None,
    };

    let params_a = base_params.clone();
    let shard = Arc::clone(&shard_a);
    sim.host("sketch-a", move || {
        let shard = Arc::clone(&shard);
        let params = params_a.clone();
        async move { node_loop(params, shard).await }
    });

    let params_b = NodeParams {
        node: node_b,
        label: "b",
        peers: vec![(node_a, "sketch-a", port)],
        ..base_params
    };
    let shard = Arc::clone(&shard_b);
    sim.host("sketch-b", move || {
        let shard = Arc::clone(&shard);
        let params = params_b.clone();
        async move { node_loop(params, shard).await }
    });

    let budget = steps_for(ae_period * 20 + Duration::from_secs(5));
    let converged = run_until(&mut sim, budget, || {
        digests_of(&shard_a) == digests_of(&shard_b)
    });
    assert!(
        converged.is_some(),
        "sketch-decoded anti-entropy converges within a bounded number of rounds despite loss"
    );
    assert_eq!(
        value_of(&shard_b, target_key),
        value_of(&shard_a, target_key),
        "the dropped key is repaired via the sketch decode / pull-by-hash path"
    );
}

/// Two shards start byte-identical across 4,096 keys, dense enough that a
/// forced `ae_part_min_bucket` of 2 puts several entries in some buckets;
/// the responder answers a mismatch there with `AeMismatch::PartDigests`
/// rather than a full listing or sketch. One key is then dropped locally on
/// node-b, and the two nodes run `ae_round_with_sketch` against each other
/// on a timer under turmoil packet loss and reordering. Recovery goes
/// through the part machinery: `ShardOps::part_digests` compared via
/// `sundog::mismatched_parts`, `Mesh::ae_parts` for the differing parts, and
/// `diff_part`/`ShardOps::entries_for_parts` for the pull, with the
/// `bucket_listings` counter proving no `AeMismatch::Bucket`/`Msg::AeBucket`
/// listing ever carried the repair.
#[test]
fn part_reconciliation_repairs_one_key_under_loss() {
    const N: u32 = 4096;
    const MIN_PART_BUCKET: usize = 2;
    let node_a = NodeId::from(81);
    let node_b = NodeId::from(82);
    let port = 5000;

    let shard_a = Arc::new(seed_shard(node_a, 0..N));
    let shard_b = Arc::new(new_shard(node_b));
    block_on(async {
        let mut chunks = ShardOps::snapshot_chunks(shard_a.as_ref());
        while let Some(chunk) = chunks.next().await {
            ShardOps::apply_remote_batch(shard_b.as_ref(), chunk).await;
        }
    });
    assert_eq!(
        digests_of(&shard_a),
        digests_of(&shard_b),
        "both sides start converged before the drop"
    );

    // A bucket dense enough that MIN_PART_BUCKET's threshold answers it with
    // part digests instead of a full listing or sketch.
    let bucket_keys = dense_bucket_keys(N, MIN_PART_BUCKET + 1);
    let target_key = bucket_keys[0];
    block_on(shard_b.invalidate_local(&target_key));
    assert_eq!(
        value_of(&shard_b, target_key),
        None,
        "node-b's copy is dropped, as if a Replicate never arrived"
    );
    assert_ne!(
        digests_of(&shard_a),
        digests_of(&shard_b),
        "test setup sanity: the drop makes one bucket mismatch"
    );

    let mut sim = Builder::new()
        .rng_seed(sim_seed(0x9A27_7501))
        .tick_duration(TICK)
        // Same loss/reorder shape as the storm scenario: AE rounds routinely
        // fail and retry rather than never losing a packet.
        .fail_rate(0.03)
        .repair_rate(0.75)
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(60))
        .build();

    let ae_period = Duration::from_millis(150);
    let bucket_listings_a = Arc::new(AtomicUsize::new(0));
    let bucket_listings_b = Arc::new(AtomicUsize::new(0));
    let base_params = NodeParams {
        node: node_a,
        label: "a",
        port,
        peers: vec![(node_b, "part-b", port)],
        keys: vec![],
        // No writes in this scenario: the fixed dataset plus the one drop
        // is the whole story.
        write_period: Duration::from_secs(3600),
        ae_period,
        dup_factor: 1,
        ae_failures: None,
        remove_on_repeat: false,
        ops_issued: None,
        ae_sketch_min_bucket: None,
        ae_part_min_bucket: Some(MIN_PART_BUCKET),
        bucket_listings: Some(Arc::clone(&bucket_listings_a)),
    };

    let params_a = base_params.clone();
    let shard = Arc::clone(&shard_a);
    sim.host("part-a", move || {
        let shard = Arc::clone(&shard);
        let params = params_a.clone();
        async move { node_loop(params, shard).await }
    });

    let params_b = NodeParams {
        node: node_b,
        label: "b",
        peers: vec![(node_a, "part-a", port)],
        bucket_listings: Some(Arc::clone(&bucket_listings_b)),
        ..base_params
    };
    let shard = Arc::clone(&shard_b);
    sim.host("part-b", move || {
        let shard = Arc::clone(&shard);
        let params = params_b.clone();
        async move { node_loop(params, shard).await }
    });

    let budget = steps_for(ae_period * 20 + Duration::from_secs(5));
    let converged = run_until(&mut sim, budget, || {
        digests_of(&shard_a) == digests_of(&shard_b)
    });
    assert!(
        converged.is_some(),
        "part-digest anti-entropy converges within a bounded number of rounds despite loss"
    );
    assert_eq!(
        value_of(&shard_b, target_key),
        value_of(&shard_a, target_key),
        "the dropped key is repaired via the part-digest / part-listing path"
    );
    for (label, counter) in [("a", &bucket_listings_a), ("b", &bucket_listings_b)] {
        assert_eq!(
            counter.load(Ordering::Relaxed),
            0,
            "node-{label}'s rounds never carried a full bucket listing"
        );
    }
}

// ---------------------------------------------------------------------
// Distribution mode (`Mode::Distributed`): deterministic turmoil
// simulations of ownership under churn, partition, owner loss, the
// non-owner write guard, and residency's disown-grace window.
//
// `OwnershipTracker`, `OwnershipView`, `ResidencySet`,
// `ownership::ownership_diff`, `Shard::with_ownership`, and a shard's
// fan-out queue are all `pub(crate)`; the pieces this file needs are
// re-exported under the `sim` feature the same way `cluster::anti_entropy`'s
// pieces are above: `sundog::{OwnershipTracker, OwnershipView,
// ResidencySet, ownership_diff}`, `Shard::with_ownership_for_sim`,
// `Shard::drain_fan_out_for_sim`, and `store::SimFanOut`.
//
// Ownership itself is driven the same way membership already is in this
// file: hand-scripted from the test, via each node's own
// `watch::Sender<Arc<OwnershipView>>`, rather than through a live
// `refresh_task`/gossip loop this harness has no chitchat layer to run.
// `republish_view` mirrors `cluster::rebalance::rebalance_task`'s own
// reaction to a view change — mark newly lost buckets releasing, unmark
// newly regained ones — so residency behaves exactly as it does in
// production. Bucket transfer to a newly owning node goes through the same
// self-healing anti-entropy backstop `cluster::rebalance`'s own doc calls
// out — `ae_round_with_sketch` above — rather than an eager
// `Mesh::request_buckets` pull (`pub(crate)`, out of reach here): once a
// gained bucket starts appearing in the new owner's own `ShardOps::digests`,
// the previous owner's own round shows a mismatch and the ordinary push
// path lands it.

/// A distribution-mode scenario's per-node handle to what the test driver
/// keeps outside any node's own async loop: the shard, its ownership
/// view's publish side, and its residency set. A real cluster keeps all
/// three behind `cluster::rebalance`'s task; here the test drives them
/// directly.
struct DistNode {
    node: NodeId,
    host: &'static str,
    port: u16,
    shard: Arc<TestShard>,
    tx: watch::Sender<Arc<OwnershipView>>,
    residency: Arc<ResidencySet>,
}

/// Builds one `Mode::Distributed` node: a fresh shard with an ownership
/// tracker and residency set attached via `Shard::with_ownership_for_sim`,
/// seeded to the solo view `OwnershipTracker::seed` always starts from —
/// see the `distributed_shard` fixture in `store/mod.rs`'s own tests for
/// the identical construction. The caller republishes the scenario's real
/// starting view immediately after, via `republish_view`/`republish_all`.
fn new_dist_node(node: NodeId, host: &'static str, port: u16, owners: u8) -> DistNode {
    let k = NonZeroU8::new(owners).expect("owners is nonzero");
    let (tracker, tx) = OwnershipTracker::seed(
        node,
        &[],
        &HashMap::<NodeId, HashMap<SmolStr, Mode>>::new(),
        &cache_name(),
        k,
    );
    let residency = Arc::new(ResidencySet::new());
    let shard = Arc::new(
        Shard::new(
            cache_name(),
            Mode::Distributed { owners: k },
            node,
            100_000,
            None,
            None,
        )
        .with_ownership_for_sim(tracker, Arc::clone(&residency)),
    );
    DistNode {
        node,
        host,
        port,
        shard,
        tx,
        residency,
    }
}

/// The `(NodeId, host, port)` entries of `roster` other than `self_node`,
/// for wiring one distribution-mode node's own peer list.
fn peers_excluding(
    roster: &[(NodeId, &'static str, u16)],
    self_node: NodeId,
) -> Vec<(NodeId, &'static str, u16)> {
    roster
        .iter()
        .copied()
        .filter(|&(node, _, _)| node != self_node)
        .collect()
}

/// The `Arc<TestShard>` for the node named `id` in `nodes`.
fn shard_of(nodes: &[DistNode], id: NodeId) -> &Arc<TestShard> {
    &nodes
        .iter()
        .find(|n| n.node == id)
        .expect("known node id")
        .shard
}

/// Recomputes and republishes one node's ownership view over `eligible`,
/// updating its residency set exactly as
/// `cluster::rebalance::rebalance_task` reacts to a real view change: a
/// newly lost bucket starts its disown-grace clock, a newly regained one
/// clears it, so a flap never accumulates toward release.
fn republish_view(
    self_node: NodeId,
    tx: &watch::Sender<Arc<OwnershipView>>,
    residency: &ResidencySet,
    eligible: Vec<NodeId>,
    k: NonZeroU8,
) {
    let new_view = Arc::new(OwnershipView::compute(self_node, eligible, k));
    tx.send_if_modified(|current| {
        if current.view_hash() == new_view.view_hash() {
            return false;
        }
        let (gained, lost) = ownership_diff(current, &new_view);
        if !lost.is_empty() {
            residency.mark_releasing(&lost);
        }
        if !gained.is_empty() {
            residency.unmark(&gained);
        }
        *current = Arc::clone(&new_view);
        true
    });
}

/// [`republish_view`] for every node in `nodes` whose id is in `live`, all
/// against the view computed over exactly `live`: the hand-scripted
/// membership feed's counterpart to a real gossip round converging on a
/// new peer set. A node not in `live` is left untouched — it may be
/// crashed, in which case nothing reads its tracker until it bounces back
/// and this is called again with it included.
fn republish_all(nodes: &[DistNode], live: &[NodeId], k: NonZeroU8) {
    for node in nodes {
        if live.contains(&node.node) {
            republish_view(node.node, &node.tx, &node.residency, live.to_vec(), k);
        }
    }
}

/// One `Mode::Distributed` write's fan-out: drains the shard's fan-out
/// queue and sends each item to its bucket's current owners (`self_node`
/// excluded), the per-write counterpart of
/// `cluster::group_by_owner_set`/`fan_out_by_owner_set`'s owner-set
/// grouping, without that function's batching since this harness drains at
/// most a handful of items per tick. An `Applied` item re-fetches its
/// current record via `ShardOps::records_for`, exactly as `Replicated`
/// mode's own [`fan_out`] helper above does; a `Forward` item already
/// carries its record, since a non-owner write is never applied to
/// `engine` in the first place.
///
/// A `Forward` item's send is duplicated a few times: the forwarding node
/// keeps no local copy once it has forwarded, so a lost send has no
/// anti-entropy backstop the way a lost `Applied` send does (the owner
/// that issued it still holds a copy anti-entropy can push again next
/// round), matching this file's existing `dup_factor` pattern for
/// `Replicated` mode's own lossy scenarios above.
fn send_to_owners(mesh: &Mesh, view: &OwnershipView, self_node: NodeId, rec: &WireRecord) {
    let bucket = bucket_of_bytes(rec.key.as_ref());
    for &owner in view.owners_of(bucket) {
        if owner != self_node {
            mesh.send(
                owner,
                MsgClass::Replicate,
                Msg::Replicate {
                    cache: cache_name(),
                    rec: rec.clone(),
                },
            );
        }
    }
}

/// Drains the shard's fan-out queue: an `Applied` item re-fetches its
/// current record via `ShardOps::records_for`, exactly as `Replicated`
/// mode's own [`fan_out`] helper above does, and is sent once (a lost send
/// still self-heals via anti-entropy, since the writing owner keeps its
/// own copy to push again next round); a `Forward` item already carries
/// its record, since a non-owner write is never applied to `engine` in the
/// first place, and is returned to the caller to retry across several
/// ticks via [`dist_node_loop`]'s own retry pool — a forwarding node keeps
/// no local copy once it has forwarded, so a lost send has no
/// anti-entropy backstop the way a lost `Applied` send does.
async fn fan_out_owned(
    shard: &TestShard,
    mesh: &Mesh,
    view: &OwnershipView,
    self_node: NodeId,
) -> Vec<WireRecord> {
    let mut forwards = Vec::new();
    for item in shard.drain_fan_out_for_sim() {
        match item {
            SimFanOut::Applied(key) => {
                if let Some(rec) = ShardOps::records_for(shard, vec![key_bytes(key)])
                    .await
                    .into_iter()
                    .next()
                {
                    send_to_owners(mesh, view, self_node, &rec);
                }
            }
            SimFanOut::Forward(rec) => forwards.push(rec),
        }
    }
    forwards
}

/// One scripted write for a distribution-mode node's own op list; see
/// [`DistNodeParams::ops`].
#[derive(Clone, Copy)]
enum DistOp {
    Insert(u32),
    Remove(u32),
}

/// One distribution-mode node's role: like [`NodeParams`] for
/// `Mode::Replicated`, but fanning writes out to a key's current owners
/// (via [`fan_out_owned`]) instead of broadcasting to every peer, gating
/// anti-entropy's peer choice through `ShardOps::ae_peer_filter`, and
/// releasing buckets whose disown grace has elapsed on its own tick —
/// mirroring `cluster::rebalance::rebalance_task`'s release half. The
/// gained half is left to anti-entropy's self-healing backstop; see this
/// section's own doc.
#[derive(Clone)]
struct DistNodeParams {
    node: NodeId,
    label: &'static str,
    port: u16,
    peers: Vec<(NodeId, &'static str, u16)>,
    /// May be empty: a scenario that drives every write itself, directly on
    /// a node's `Arc<TestShard>` from outside this loop, still needs the
    /// loop running so its own `fan_out_tick`/`ae_tick`/`rebalance_tick`
    /// pick the write up. Shared, not owned outright: `sim.host`'s closure
    /// re-clones `DistNodeParams` on every restart (a bounce included), and
    /// an owned `Vec` would replay every already-issued op from scratch on
    /// each one, re-inserting an already-removed key with a fresh,
    /// LWW-winning `Hlc` — a real bug this harness hit once already. A
    /// shared queue keeps the harness's own restart honest: a bounced node
    /// resumes issuing its remaining ops, the way a real node resumes
    /// serving already-accepted API calls rather than an external caller
    /// replaying them.
    ops: Arc<StdMutex<VecDeque<DistOp>>>,
    write_period: Duration,
    /// Drains and dispatches the fan-out queue, decoupled from
    /// `write_period` so an externally issued write (never produced by this
    /// node's own `ops`) still gets picked up promptly.
    fan_out_period: Duration,
    ae_period: Duration,
    rebalance_period: Duration,
    disown_grace: Duration,
    ops_issued: Option<Arc<AtomicUsize>>,
}

/// How many `ae_tick`s a still-undelivered `Forward` item is retried for,
/// spread one attempt per tick rather than several copies back to back in
/// one tick, so each attempt gets an independent chance against a single
/// bad connection state instead of risking every copy failing together;
/// see [`fan_out_owned`]'s own doc for why a `Forward` item needs this and
/// an `Applied` one does not.
const FORWARD_RETRIES: u8 = 15;

async fn dist_node_loop(
    params: DistNodeParams,
    shard: Arc<TestShard>,
    residency: Arc<ResidencySet>,
) -> SimResult {
    let handler: Arc<dyn RequestHandler> = Arc::new(ShardHandler::new(Arc::clone(&shard)));
    let bind_addr = SocketAddr::from(([0, 0, 0, 0], params.port));
    let (mesh, mut inbound) = Mesh::spawn(
        bind_addr,
        params.node,
        1,
        &ClusterConfig::default(),
        handler,
    )
    .await?;

    let peer_list = peer_list_of(&params.peers);
    mesh.update_peers(peer_list.clone());
    let peer_ids: Vec<NodeId> = peer_list.iter().map(|peer| peer.node).collect();

    // Forwarded writes not yet confirmed delivered, retried once per
    // `ae_tick` for `FORWARD_RETRIES` ticks each: see `fan_out_owned`'s own
    // doc for why a `Forward` item needs this and an `Applied` one does
    // not.
    let mut pending_forwards: Vec<(WireRecord, u8)> = Vec::new();

    let ops = params.ops;
    let mut write_tick = tokio::time::interval(params.write_period);
    write_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut fan_out_tick = tokio::time::interval(params.fan_out_period);
    fan_out_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut ae_tick = tokio::time::interval(params.ae_period);
    ae_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut rebalance_tick = tokio::time::interval(params.rebalance_period);
    rebalance_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            biased;
            Some(InboundMsg { msg, .. }) = inbound.recv() => {
                dispatch_inbound(shard.as_ref(), msg).await;
            }
            _ = write_tick.tick() => {
                let next_op = ops
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .pop_front();
                if let Some(op) = next_op {
                    match op {
                        DistOp::Insert(key) => {
                            let _ = shard.insert(key, format!("{}:{key}", params.label)).await;
                        }
                        DistOp::Remove(key) => {
                            let _ = shard.remove(&key).await;
                        }
                    }
                    if let Some(counter) = params.ops_issued.as_ref() {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            _ = fan_out_tick.tick() => {
                if let Some(view) = ShardOps::ownership_view(shard.as_ref()) {
                    let new_forwards = fan_out_owned(shard.as_ref(), &mesh, &view, params.node).await;
                    for rec in &new_forwards {
                        send_to_owners(&mesh, &view, params.node, rec);
                    }
                    pending_forwards.extend(new_forwards.into_iter().map(|rec| (rec, FORWARD_RETRIES)));
                }
            }
            _ = ae_tick.tick() => {
                let (_, cohort) =
                    ShardOps::ae_peer_filter(shard.as_ref(), peer_ids.clone(), peer_ids.clone());
                for peer in cohort {
                    ae_round_with_sketch(&mesh, shard.as_ref(), peer, None).await;
                }
                // Retries for still-pending forwards, spaced at the AE
                // interval rather than the much tighter `fan_out_period`:
                // turmoil's `fail_rate` breaks the whole underlying
                // connection rather than dropping one frame with
                // retransmission, so several attempts fired close together
                // land on the same broken connection and all fail
                // together; spacing them out gives each one an independent
                // chance once the connection has had time to reconnect.
                if let Some(view) = ShardOps::ownership_view(shard.as_ref()) {
                    pending_forwards.retain_mut(|(rec, attempts_left)| {
                        send_to_owners(&mesh, &view, params.node, rec);
                        *attempts_left -= 1;
                        *attempts_left > 0
                    });
                }
            }
            _ = rebalance_tick.tick() => {
                let due = residency.expired(params.disown_grace);
                if !due.is_empty() {
                    ShardOps::release_buckets(shard.as_ref(), &due).await;
                    residency.unmark(&due);
                }
            }
        }
    }
}

/// Whether every live node's local content is contained in its own current
/// owned (and, when `allow_releasing`, also releasing) bucket set. Shared by
/// the churn and non-owner-property scenarios below.
fn assert_holds_only_owned_or_releasing(
    label: &str,
    nodes: &[DistNode],
    key_space: u32,
    allow_releasing: bool,
) {
    for n in nodes {
        let Some(view) = ShardOps::ownership_view(n.shard.as_ref()) else {
            continue;
        };
        for key in 0..key_space {
            if value_of(&n.shard, key).is_some() {
                let bucket = bucket_of_u32(key);
                let ok = view.owns(bucket) || (allow_releasing && n.residency.is_releasing(bucket));
                assert!(
                    ok,
                    "{label}: node {:?} holds key {key} (bucket {bucket}) it neither owns nor is releasing (is_releasing={})",
                    n.node,
                    n.residency.is_releasing(bucket)
                );
            }
        }
    }
}

/// Whether every key in `expected` is present on at least one node that
/// currently owns its bucket: the data-safety convergence signal churn
/// scenarios wait on, since distinct nodes legitimately hold different
/// buckets and a plain digest-equality check (as the replicated-mode
/// scenarios above use) does not apply.
fn dist_data_settled(nodes: &[DistNode], expected: &HashSet<u32>) -> bool {
    expected.iter().all(|&key| {
        let bucket = bucket_of_u32(key);
        nodes.iter().any(|n| {
            ShardOps::ownership_view(n.shard.as_ref()).is_some_and(|view| view.owns(bucket))
                && value_of(&n.shard, key).is_some()
        })
    })
}

/// [`dist_data_settled`]'s counterpart for a key removed rather than
/// surviving: every current owner of the key's bucket has actually
/// forgotten it. A removed key's own writer applies its tombstone
/// instantly, but the co-owner only learns of it via the ordinary
/// `Applied`-item fan-out (or, if that one send is lost, the next
/// anti-entropy round) — [`dist_data_settled`] never looks at a removed
/// key at all, so this is the churn scenario's own explicit wait for that
/// second hop to land before treating the pre-churn baseline as settled.
fn dist_removals_settled(nodes: &[DistNode], removed: &HashSet<u32>) -> bool {
    removed.iter().all(|&key| {
        let bucket = bucket_of_u32(key);
        nodes.iter().all(|n| {
            !ShardOps::ownership_view(n.shard.as_ref()).is_some_and(|view| view.owns(bucket))
                || value_of(&n.shard, key).is_none()
        })
    })
}

/// Spawns `nodes[i]`'s `dist_node_loop` as a turmoil host for every `i`,
/// using the same `params` for each but for `node`/`port`/`peers`, which
/// come from the node itself.
fn spawn_dist_nodes(
    sim: &mut Sim<'_>,
    nodes: &[DistNode],
    mut params_for: impl FnMut(&DistNode) -> DistNodeParams,
) {
    for node in nodes {
        let params = params_for(node);
        let shard = Arc::clone(&node.shard);
        let residency = Arc::clone(&node.residency);
        sim.host(node.host, move || {
            let params = params.clone();
            let shard = Arc::clone(&shard);
            let residency = Arc::clone(&residency);
            async move { dist_node_loop(params, shard, residency).await }
        });
    }
}

/// Nodes join and leave a distributed cache (`owners=2`) repeatedly under
/// message loss and reordering. Each of four nodes writes a disjoint key
/// range, then removes every fourth key of its own range, so the surviving
/// set is deliberately not "every key ever inserted." One node at a time is
/// crashed and bounced back (never taking the live count below three, so
/// every bucket always keeps a live owner), with the self-healing
/// anti-entropy backstop given time to rebalance between each crash and the
/// next. Once churn stops and every disown grace has elapsed, the union of
/// every live node's content equals exactly the surviving set, and every
/// live node holds only buckets it currently owns.
#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one scenario's full setup, churn schedule, and assertions read best kept together"
)]
fn distributed_rebalance_under_churn() {
    const OWNERS: u8 = 2;
    let k = NonZeroU8::new(OWNERS).expect("nonzero");
    let port = 5100;
    let roster: Vec<(NodeId, &'static str, u16)> = vec![
        (NodeId::from(1001), "churn-node-a", port),
        (NodeId::from(1002), "churn-node-b", port),
        (NodeId::from(1003), "churn-node-c", port),
        (NodeId::from(1004), "churn-node-d", port),
    ];
    let node_ids: Vec<NodeId> = roster.iter().map(|&(id, _, _)| id).collect();

    let nodes: Vec<DistNode> = roster
        .iter()
        .map(|&(id, host, port)| new_dist_node(id, host, port, OWNERS))
        .collect();
    republish_all(&nodes, &node_ids, k);

    // Message loss turns on only once the initial write/remove plans have
    // settled (below), so that churn — the thing this scenario actually
    // tests — runs under real loss and reordering without also fighting
    // the transport's own worst case for a one-shot forward: turmoil's
    // `fail_rate` breaks a link outright until *some* further traffic on
    // it happens to trigger a repair check, which a forward that only ever
    // needs to reach its bucket's two owners once may never do on its own.
    // Reordering (the latency spread below) is in effect throughout.
    let mut sim = Builder::new()
        .rng_seed(sim_seed(0xD157_C001))
        .tick_duration(TICK)
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(60))
        .build();

    let ranges: [std::ops::Range<u32>; 4] = [0..20, 20..40, 40..60, 60..80];
    let labels = ["a", "b", "c", "d"];
    let mut expected: HashSet<u32> = HashSet::new();
    let mut removed_keys: HashSet<u32> = HashSet::new();
    let ops_counters: Vec<Arc<AtomicUsize>> =
        (0..4).map(|_| Arc::new(AtomicUsize::new(0))).collect();
    let mut op_lens = vec![0usize; 4];
    let mut plans: Vec<Vec<DistOp>> = Vec::new();
    for (i, range) in ranges.iter().enumerate() {
        let removed: Vec<u32> = range.clone().step_by(4).collect();
        for key in range.clone() {
            if removed.contains(&key) {
                removed_keys.insert(key);
            } else {
                expected.insert(key);
            }
        }
        let mut ops: Vec<DistOp> = range.clone().map(DistOp::Insert).collect();
        ops.extend(removed.into_iter().map(DistOp::Remove));
        op_lens[i] = ops.len();
        plans.push(ops);
    }

    spawn_dist_nodes(&mut sim, &nodes, {
        let roster = roster.clone();
        let counters = ops_counters.clone();
        let mut plans = plans.into_iter();
        let mut idx = 0usize;
        move |node| {
            let i = idx;
            idx += 1;
            DistNodeParams {
                node: node.node,
                label: labels[i],
                port: node.port,
                peers: peers_excluding(&roster, node.node),
                ops: Arc::new(StdMutex::new(VecDeque::from(
                    plans.next().expect("one plan per node"),
                ))),
                write_period: Duration::from_millis(20),
                // Deliberately not equal to `write_period`: both this and
                // `write_tick`'s own handler body complete synchronously
                // (no network await), so an identical period keeps them
                // perpetually in lock step and `biased`'s deterministic
                // tie-break would starve this tick forever.
                fan_out_period: Duration::from_millis(13),
                ae_period: Duration::from_millis(150),
                rebalance_period: Duration::from_millis(150),
                disown_grace: Duration::from_millis(300),
                ops_issued: Some(Arc::clone(&counters[i])),
            }
        }
    });

    // Phase 1: every node finishes issuing its own plan, then replication
    // settles onto every current owner before churn starts.
    run_until(&mut sim, steps_for(Duration::from_secs(15)), || {
        ops_counters
            .iter()
            .zip(&op_lens)
            .all(|(c, &len)| c.load(Ordering::Relaxed) >= len)
    })
    .expect("every node finishes its write/remove plan within the budget");
    run_until(&mut sim, steps_for(Duration::from_secs(10)), || {
        dist_data_settled(&nodes, &expected) && dist_removals_settled(&nodes, &removed_keys)
    })
    .expect("every surviving key lands on at least one owner and every removal has landed before churn starts");

    // Churn itself now runs under message loss, on top of the reordering
    // already in effect: see this test's own setup comment for why loss is
    // held off until the plans above have cleanly settled. `repair_rate`
    // keeps `Builder`'s own default (1.0): any further traffic on a
    // link heals it on the very next attempt, so churn's own repeated
    // anti-entropy rounds are always enough to recover, without needing a
    // one-shot forward to itself get as lucky as the win it was denied.
    sim.set_fail_rate(0.03);

    // Phase 2: churn. One node down at a time, never below three live, each
    // window long enough for the self-healing AE backstop to rebalance
    // before the next crash.
    let mut live: Vec<NodeId> = node_ids.clone();
    let settle = Duration::from_secs(6);
    for &idx in &[2usize, 3usize] {
        let victim = node_ids[idx];
        live.retain(|&n| n != victim);
        republish_all(&nodes, &live, k);
        sim.crash(nodes[idx].host);
        run_until(&mut sim, steps_for(settle), || {
            dist_data_settled(&nodes, &expected) && dist_removals_settled(&nodes, &removed_keys)
        })
        .expect("data safety holds immediately after a node leaves");

        sim.bounce(nodes[idx].host);
        live.push(victim);
        republish_all(&nodes, &live, k);
        run_until(&mut sim, steps_for(settle), || {
            dist_data_settled(&nodes, &expected) && dist_removals_settled(&nodes, &removed_keys)
        })
        .expect("data safety holds once the node rejoins");
    }

    // Phase 3: let every disown grace fully elapse. `ResidencySet`'s grace
    // clock is stamped from real `Instant::now()`, not turmoil's virtual
    // clock (matching tombstone retention's own real-time deadline
    // elsewhere in this file), so real time — not simulated steps — must
    // actually pass before a rebalance tick has anything to release; the
    // ticks themselves then need simulated time. Both are repeated until
    // no node has a bucket left mid grace, bounded, since a bounced
    // node's loop may need more than one window to reach its tick.
    let grace = Duration::from_millis(300);
    for _ in 0..10 {
        std::thread::sleep(grace);
        run_steps(&mut sim, steps_for(grace * 3));
        if nodes
            .iter()
            .all(|n| n.residency.expired(Duration::ZERO).is_empty())
        {
            break;
        }
    }

    let mut union: HashSet<u32> = HashSet::new();
    for key in 0..80u32 {
        if nodes.iter().any(|n| value_of(&n.shard, key).is_some()) {
            union.insert(key);
        }
    }
    assert_eq!(
        union, expected,
        "the union of every live node's content is exactly the surviving set"
    );
    assert_holds_only_owned_or_releasing("after churn settles", &nodes, 80, false);
}

/// Splits the cluster into two halves, writes on both sides — including a
/// conflicting write to the same key, side two's strictly later so its
/// `Hlc` wins — then heals. `owners=2` over a two-node eligible set
/// degenerates to both sides owning everything while split (see
/// `owners_of_bucket`'s doc), so each side's writes apply locally without
/// forwarding. After healing, every node's independently computed view has
/// the same `view_hash`, and every write survives by version: the
/// conflicting key resolves to side two's value everywhere it lands, and
/// each side's private write survives on every node that ends up owning it.
#[test]
fn distributed_partition_then_heal_reconciles_ownership() {
    const OWNERS: u8 = 2;
    let k = NonZeroU8::new(OWNERS).expect("nonzero");
    let port = 5200;
    let roster: Vec<(NodeId, &'static str, u16)> = vec![
        (NodeId::from(2001), "part-node-a", port),
        (NodeId::from(2002), "part-node-b", port),
        (NodeId::from(2003), "part-node-c", port),
        (NodeId::from(2004), "part-node-d", port),
    ];
    let node_ids: Vec<NodeId> = roster.iter().map(|&(id, _, _)| id).collect();
    let side1 = [node_ids[0], node_ids[1]];
    let side2 = [node_ids[2], node_ids[3]];

    let nodes: Vec<DistNode> = roster
        .iter()
        .map(|&(id, host, port)| new_dist_node(id, host, port, OWNERS))
        .collect();
    republish_all(&nodes, &node_ids, k);

    let mut sim = Builder::new()
        .rng_seed(sim_seed(0x9A27_1102))
        .tick_duration(TICK)
        .max_message_latency(Duration::from_millis(20))
        .build();

    spawn_dist_nodes(&mut sim, &nodes, {
        let roster = roster.clone();
        move |node| DistNodeParams {
            node: node.node,
            label: "n",
            port: node.port,
            peers: peers_excluding(&roster, node.node),
            ops: Arc::new(StdMutex::new(VecDeque::new())),
            write_period: Duration::from_secs(3600),
            fan_out_period: Duration::from_millis(20),
            ae_period: Duration::from_millis(100),
            rebalance_period: Duration::from_millis(100),
            disown_grace: Duration::from_millis(300),
            ops_issued: None,
        }
    });

    // Everyone converges on the single four-node view before the split.
    run_steps(&mut sim, steps_for(Duration::from_millis(200)));

    for &a in &side1 {
        for &b in &side2 {
            let host_a = nodes.iter().find(|n| n.node == a).unwrap().host;
            let host_b = nodes.iter().find(|n| n.node == b).unwrap().host;
            sim.partition(host_a, host_b);
        }
    }
    republish_all(&nodes, &side1, k);
    republish_all(&nodes, &side2, k);
    run_steps(&mut sim, steps_for(Duration::from_millis(100)));

    let shared_key = 999u32;
    block_on(shard_of(&nodes, side1[0]).insert(shared_key, "side1".to_string())).expect("insert");
    // Real time passing, not turmoil's virtual clock, so side two's Hlc is
    // strictly later.
    std::thread::sleep(Duration::from_millis(5));
    block_on(shard_of(&nodes, side2[0]).insert(shared_key, "side2".to_string())).expect("insert");
    block_on(shard_of(&nodes, side1[0]).insert(111u32, "only-side1".to_string())).expect("insert");
    block_on(shard_of(&nodes, side2[0]).insert(222u32, "only-side2".to_string())).expect("insert");

    run_until(&mut sim, steps_for(Duration::from_secs(5)), || {
        value_of(shard_of(&nodes, side1[1]), shared_key).is_some()
            && value_of(shard_of(&nodes, side2[1]), shared_key).is_some()
            && value_of(shard_of(&nodes, side1[1]), 111).is_some()
            && value_of(shard_of(&nodes, side2[1]), 222).is_some()
    })
    .expect("each side replicates its own writes to its other member before healing");

    for &a in &side1 {
        for &b in &side2 {
            let host_a = nodes.iter().find(|n| n.node == a).unwrap().host;
            let host_b = nodes.iter().find(|n| n.node == b).unwrap().host;
            sim.repair(host_a, host_b);
        }
    }
    republish_all(&nodes, &node_ids, k);

    // Every node that currently owns a given key's bucket holds the
    // correct, fully reconciled value for it; a node that does not own it
    // is free to have already released it (residency's disown-grace is
    // shorter than this budget) but must not still be holding the losing
    // side's stale value while it waits to either self-correct or release.
    run_until(&mut sim, steps_for(Duration::from_secs(10)), || {
        nodes.iter().all(|n| {
            let view =
                ShardOps::ownership_view(n.shard.as_ref()).expect("distributed shard has a view");
            value_of(&n.shard, shared_key).as_deref() != Some("side1")
                && (!view.owns(bucket_of_u32(shared_key))
                    || value_of(&n.shard, shared_key).as_deref() == Some("side2"))
                && (!view.owns(bucket_of_u32(111))
                    || value_of(&n.shard, 111).as_deref() == Some("only-side1"))
                && (!view.owns(bucket_of_u32(222))
                    || value_of(&n.shard, 222).as_deref() == Some("only-side2"))
        })
    })
    .expect("reconciliation lands the higher-version write and both private writes on every owner");

    let hashes: HashSet<u64> = nodes
        .iter()
        .map(|n| {
            ShardOps::ownership_view_hash(n.shard.as_ref())
                .expect("distributed shard has a view hash")
        })
        .collect();
    assert_eq!(hashes.len(), 1, "every node's view_hash agrees once healed");

    for n in &nodes {
        if let Some(value) = value_of(&n.shard, shared_key) {
            assert_eq!(
                value, "side2",
                "the conflicting write survives by version: the strictly later write wins"
            );
        }
    }
}

/// With `owners=2`, kills one of a chosen bucket's two owners. The
/// surviving owner keeps answering for every one of the bucket's keys
/// throughout the outage — checked repeatedly while the cluster
/// rebalances, not only once at the end — and once the vacated slot's new
/// third owner has pulled the bucket via the anti-entropy self-healing
/// backstop, its content for those keys matches the survivor's exactly.
#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one scenario's full setup, kill, and rebalance-matching checks read best kept together"
)]
fn distributed_owner_loss_k_two_loses_nothing() {
    const OWNERS: u8 = 2;
    let k = NonZeroU8::new(OWNERS).expect("nonzero");
    let port = 5300;
    let roster: Vec<(NodeId, &'static str, u16)> = vec![
        (NodeId::from(3001), "loss-node-a", port),
        (NodeId::from(3002), "loss-node-b", port),
        (NodeId::from(3003), "loss-node-c", port),
        (NodeId::from(3004), "loss-node-d", port),
    ];
    let node_ids: Vec<NodeId> = roster.iter().map(|&(id, _, _)| id).collect();
    let target_bucket = 0u16;

    let initial_owners = OwnershipView::compute(node_ids[0], node_ids.clone(), k)
        .owners_of(target_bucket)
        .to_vec();
    assert_eq!(
        initial_owners.len(),
        2,
        "k=2 over four eligible nodes always gives two owners"
    );
    let leaving = initial_owners[0];
    let staying = initial_owners[1];

    let bucket_keys: Vec<u32> = (0..20_000)
        .filter(|&key| bucket_of_u32(key) == target_bucket)
        .take(5)
        .collect();
    assert!(
        !bucket_keys.is_empty(),
        "test setup sanity: some key maps to the target bucket"
    );

    let nodes: Vec<DistNode> = roster
        .iter()
        .map(|&(id, host, port)| new_dist_node(id, host, port, OWNERS))
        .collect();
    republish_all(&nodes, &node_ids, k);

    let mut sim = Builder::new()
        .rng_seed(sim_seed(0x1055_3001))
        .tick_duration(TICK)
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(30))
        .build();

    spawn_dist_nodes(&mut sim, &nodes, {
        let roster = roster.clone();
        move |node| DistNodeParams {
            node: node.node,
            label: "n",
            port: node.port,
            peers: peers_excluding(&roster, node.node),
            ops: Arc::new(StdMutex::new(VecDeque::new())),
            write_period: Duration::from_secs(3600),
            fan_out_period: Duration::from_millis(15),
            ae_period: Duration::from_millis(80),
            rebalance_period: Duration::from_millis(80),
            disown_grace: Duration::from_millis(250),
            ops_issued: None,
        }
    });

    for &key in &bucket_keys {
        block_on(shard_of(&nodes, leaving).insert(key, format!("v:{key}"))).expect("insert");
    }
    run_until(&mut sim, steps_for(Duration::from_secs(5)), || {
        bucket_keys
            .iter()
            .all(|&key| value_of(shard_of(&nodes, staying), key).is_some())
    })
    .expect("the surviving owner has every key before the kill");

    sim.crash(nodes.iter().find(|n| n.node == leaving).unwrap().host);
    let live: Vec<NodeId> = node_ids.iter().copied().filter(|&n| n != leaving).collect();
    republish_all(&nodes, &live, k);

    let new_owners = OwnershipView::compute(staying, live, k)
        .owners_of(target_bucket)
        .to_vec();
    assert!(
        new_owners.contains(&staying),
        "the surviving original owner keeps the bucket"
    );
    assert!(
        !new_owners.contains(&leaving),
        "the killed node is no longer eligible"
    );
    let new_third = *new_owners
        .iter()
        .find(|&&n| n != staying)
        .expect("k=2 gives a second owner");

    for _ in 0..5 {
        run_steps(&mut sim, steps_for(Duration::from_millis(200)));
        for &key in &bucket_keys {
            assert!(
                value_of(shard_of(&nodes, staying), key).is_some(),
                "the surviving owner still answers for key {key} while rebalancing"
            );
        }
    }

    run_until(&mut sim, steps_for(Duration::from_secs(10)), || {
        bucket_keys.iter().all(|&key| {
            value_of(shard_of(&nodes, new_third), key) == value_of(shard_of(&nodes, staying), key)
        })
    })
    .expect("the new owner's content matches the survivor's within the budget");

    for &key in &bucket_keys {
        assert_eq!(
            value_of(shard_of(&nodes, new_third), key),
            value_of(shard_of(&nodes, staying), key),
            "the new owner's content matches the survivor's exactly for key {key}"
        );
    }
}

/// For a seeded, randomized sequence of membership toggles (crash/bounce,
/// never below two live nodes) and writes/removes on random live nodes,
/// checks after every step that every node's local content is contained in
/// its own current owned-or-releasing bucket set — equivalently, that a
/// node holding neither ownership nor a residency grace on a bucket holds
/// nothing in it.
#[test]
fn distributed_non_owner_never_applies_as_a_property() {
    const OWNERS: u8 = 2;
    const KEY_SPACE: u32 = 40;
    let k = NonZeroU8::new(OWNERS).expect("nonzero");
    let port = 5400;
    let roster: Vec<(NodeId, &'static str, u16)> = vec![
        (NodeId::from(4001), "prop-node-a", port),
        (NodeId::from(4002), "prop-node-b", port),
        (NodeId::from(4003), "prop-node-c", port),
        (NodeId::from(4004), "prop-node-d", port),
    ];
    let node_ids: Vec<NodeId> = roster.iter().map(|&(id, _, _)| id).collect();

    let nodes: Vec<DistNode> = roster
        .iter()
        .map(|&(id, host, port)| new_dist_node(id, host, port, OWNERS))
        .collect();
    republish_all(&nodes, &node_ids, k);

    let mut sim = Builder::new()
        .rng_seed(sim_seed(0x9A2C_0104))
        .tick_duration(TICK)
        .fail_rate(0.02)
        .repair_rate(0.8)
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(40))
        .build();

    spawn_dist_nodes(&mut sim, &nodes, {
        let roster = roster.clone();
        move |node| DistNodeParams {
            node: node.node,
            label: "n",
            port: node.port,
            peers: peers_excluding(&roster, node.node),
            ops: Arc::new(StdMutex::new(VecDeque::new())),
            write_period: Duration::from_secs(3600),
            fan_out_period: Duration::from_millis(15),
            ae_period: Duration::from_millis(80),
            rebalance_period: Duration::from_millis(80),
            disown_grace: Duration::from_millis(150),
            ops_issued: None,
        }
    });

    let mut rng = StdRng::seed_from_u64(sim_seed(0xC0DE_5555));
    let mut live: Vec<NodeId> = node_ids.clone();

    for step in 0..40 {
        if rng.random_range(0..3u32) == 0 {
            let idx = rng.random_range(0..node_ids.len());
            let target = node_ids[idx];
            let host = nodes[idx].host;
            if live.contains(&target) {
                if live.len() > 2 {
                    live.retain(|&n| n != target);
                    republish_all(&nodes, &live, k);
                    sim.crash(host);
                }
            } else {
                sim.bounce(host);
                live.push(target);
                republish_all(&nodes, &live, k);
            }
        } else {
            let writer = live[rng.random_range(0..live.len())];
            let key = rng.random_range(0..KEY_SPACE);
            let shard = shard_of(&nodes, writer);
            if rng.random_range(0..4u32) == 0 {
                let _ = block_on(shard.remove(&key));
            } else {
                let _ = block_on(shard.insert(key, format!("v{step}:{key}")));
            }
        }
        run_steps(&mut sim, steps_for(Duration::from_millis(30)));
        assert_holds_only_owned_or_releasing(
            &format!("checkpoint {step}"),
            &nodes,
            KEY_SPACE,
            true,
        );
    }

    for &id in &node_ids {
        if !live.contains(&id) {
            sim.bounce(nodes.iter().find(|n| n.node == id).unwrap().host);
            live.push(id);
        }
    }
    republish_all(&nodes, &node_ids, k);
    run_steps(&mut sim, steps_for(Duration::from_secs(2)));
    assert_holds_only_owned_or_releasing("final", &nodes, KEY_SPACE, true);
}

/// Displaces one of a bucket's two owners with a phantom fourth node —
/// advertised into the eligible set but never run as a real host, since
/// only the rendezvous computation, not its reachability, matters here —
/// so the departed owner enters disown-grace while its former co-owner
/// keeps the bucket. A one-shot prober dials the departed owner directly,
/// well within the grace window, proving it still answers a digest
/// mismatch (`Mesh::ae_round`) and an explicit entries listing
/// (`Mesh::ae_entries`) with real data, then sends it a fresh `Replicate`
/// for a new key in the same bucket, proving the inbound-apply guard drops
/// it. The bucket's pre-existing data survives untouched through the grace
/// window, then is actually released once the grace period elapses.
#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one scenario's setup, prober, and phased assertions read best kept together"
)]
fn distributed_releasing_bucket_still_answers_anti_entropy_but_never_accepts_a_fresh_apply() {
    const OWNERS: u8 = 2;
    let k = NonZeroU8::new(OWNERS).expect("nonzero");
    let port = 5500;
    let roster: Vec<(NodeId, &'static str, u16)> = vec![
        (NodeId::from(5001), "grace-node-a", port),
        (NodeId::from(5002), "grace-node-b", port),
        (NodeId::from(5003), "grace-node-c", port),
    ];
    let node_ids: Vec<NodeId> = roster.iter().map(|&(id, _, _)| id).collect();
    let target_bucket = 0u16;

    let initial_owners = OwnershipView::compute(node_ids[0], node_ids.clone(), k)
        .owners_of(target_bucket)
        .to_vec();
    assert_eq!(initial_owners.len(), 2);
    // `owners_of` is ordered by descending rendezvous score: index 0 is the
    // strictly higher-scoring owner. Adding one more eligible node can only
    // ever displace the *weaker* of the two from the top-2 — the stronger
    // owner would have to be outscored by the newcomer to fall out, which
    // would put the newcomer in its place instead, not remove it outright —
    // so `leaving` must be the weaker owner for the phantom search below to
    // have a solution.
    let staying = initial_owners[0];
    let leaving = initial_owners[1];

    // A phantom fourth node: never run as a real host, chosen purely so
    // the rendezvous computation displaces `leaving` from `target_bucket`
    // without displacing `staying`.
    let phantom = (9_000_000u64..9_001_000)
        .map(NodeId::from)
        .find(|&candidate| {
            let mut eligible = node_ids.clone();
            eligible.push(candidate);
            let owners = OwnershipView::compute(candidate, eligible, k)
                .owners_of(target_bucket)
                .to_vec();
            owners.contains(&staying) && !owners.contains(&leaving)
        })
        .expect("some candidate id displaces the leaving owner alone");

    let all_bucket_keys: Vec<u32> = (0..20_000)
        .filter(|&key| bucket_of_u32(key) == target_bucket)
        .take(4)
        .collect();
    assert_eq!(
        all_bucket_keys.len(),
        4,
        "test setup sanity: enough keys map to the target bucket"
    );
    let bucket_keys = &all_bucket_keys[0..3];
    let fresh_key = all_bucket_keys[3];

    let nodes: Vec<DistNode> = roster
        .iter()
        .map(|&(id, host, port)| new_dist_node(id, host, port, OWNERS))
        .collect();
    republish_all(&nodes, &node_ids, k);

    let mut sim = Builder::new()
        .rng_seed(sim_seed(0x9BAC_E505))
        .tick_duration(TICK)
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(20))
        .build();

    let disown_grace = Duration::from_millis(400);
    spawn_dist_nodes(&mut sim, &nodes, {
        let roster = roster.clone();
        move |node| DistNodeParams {
            node: node.node,
            label: "n",
            port: node.port,
            peers: peers_excluding(&roster, node.node),
            ops: Arc::new(StdMutex::new(VecDeque::new())),
            write_period: Duration::from_secs(3600),
            fan_out_period: Duration::from_millis(15),
            ae_period: Duration::from_millis(60),
            rebalance_period: Duration::from_millis(60),
            disown_grace,
            ops_issued: None,
        }
    });

    for &key in bucket_keys {
        block_on(shard_of(&nodes, leaving).insert(key, format!("v:{key}"))).expect("insert");
    }
    run_until(&mut sim, steps_for(Duration::from_secs(5)), || {
        bucket_keys
            .iter()
            .all(|&key| value_of(shard_of(&nodes, staying), key).is_some())
    })
    .expect("both original owners hold the bucket's keys before the membership change");

    // Advertise the phantom fourth node: `leaving` loses `target_bucket`
    // and starts its disown-grace clock; `staying` keeps it.
    let mut eligible = node_ids.clone();
    eligible.push(phantom);
    republish_all(&nodes, &eligible, k);
    assert!(
        nodes
            .iter()
            .find(|n| n.node == leaving)
            .unwrap()
            .residency
            .is_releasing(target_bucket),
        "the departed owner marks the bucket releasing immediately on the view change"
    );

    // A legitimate record for a brand-new key in the same bucket, built
    // from the current owner's own applied copy rather than hand-rolling a
    // wire frame.
    block_on(shard_of(&nodes, staying).insert(fresh_key, "fresh".to_string())).expect("insert");
    let fresh_rec = block_on(ShardOps::records_for(
        shard_of(&nodes, staying).as_ref(),
        vec![key_bytes(fresh_key)],
    ))
    .into_iter()
    .next()
    .expect("just inserted");

    let leaving_host = nodes.iter().find(|n| n.node == leaving).unwrap().host;
    let leaving_port = nodes.iter().find(|n| n.node == leaving).unwrap().port;
    let prober_id = NodeId::from(5999);
    let ae_mismatch_count = Arc::new(AtomicUsize::new(0));
    let entries_count = Arc::new(AtomicUsize::new(0));
    let probe_done = Arc::new(AtomicBool::new(false));
    let ae_mismatch_count_probe = Arc::clone(&ae_mismatch_count);
    let entries_count_probe = Arc::clone(&entries_count);
    let probe_done_probe = Arc::clone(&probe_done);
    sim.client("grace-prober", async move {
        let handler: Arc<dyn RequestHandler> =
            Arc::new(ShardHandler::new(Arc::new(new_shard(prober_id))));
        let bind_addr = SocketAddr::from(([0, 0, 0, 0], 5599));
        let (mesh, _inbound) =
            Mesh::spawn(bind_addr, prober_id, 1, &ClusterConfig::default(), handler).await?;
        mesh.update_peers(peer_list_of(&[(leaving, leaving_host, leaving_port)]));

        // A deliberately wrong digest forces a mismatch reply carrying
        // real entries, proving the responder still answers.
        if let Ok(mismatched) = mesh
            .ae_round(leaving, cache_name(), vec![(target_bucket, 0)])
            .await
        {
            ae_mismatch_count_probe.fetch_add(mismatched.len(), Ordering::Relaxed);
        }
        if let Ok(listing) = mesh
            .ae_entries(leaving, cache_name(), vec![target_bucket])
            .await
        {
            let count: usize = listing.into_iter().map(|(_, entries)| entries.len()).sum();
            entries_count_probe.fetch_add(count, Ordering::Relaxed);
        }

        // A fresh key in the same bucket, sent as a live `Replicate`:
        // the inbound-apply guard must drop it.
        mesh.send(
            leaving,
            MsgClass::Replicate,
            Msg::Replicate {
                cache: cache_name(),
                rec: fresh_rec,
            },
        );
        probe_done_probe.store(true, Ordering::Relaxed);
        Ok(())
    });

    run_until(&mut sim, steps_for(Duration::from_secs(3)), || {
        probe_done.load(Ordering::Relaxed)
    })
    .expect("the prober completes within the budget");
    run_steps(&mut sim, steps_for(Duration::from_millis(100)));

    assert!(
        ae_mismatch_count.load(Ordering::Relaxed) > 0,
        "the releasing node still answers a digest mismatch with real entries"
    );
    assert!(
        entries_count.load(Ordering::Relaxed) > 0,
        "the releasing node still answers an explicit entries listing"
    );
    assert!(
        value_of(shard_of(&nodes, leaving), fresh_key).is_none(),
        "the releasing node drops a fresh inbound apply for its released bucket"
    );
    for &key in bucket_keys {
        assert!(
            value_of(shard_of(&nodes, leaving), key).is_some(),
            "existing data for a releasing bucket is not wiped before grace elapses"
        );
    }

    // Once the grace period fully elapses, the bucket is actually released.
    // `ResidencySet`'s grace clock is stamped from real `Instant::now()`,
    // not turmoil's virtual clock, so real time must actually pass — see
    // `distributed_rebalance_under_churn`'s identical comment.
    std::thread::sleep(disown_grace * 3);
    run_steps(&mut sim, steps_for(disown_grace * 3));
    for &key in bucket_keys {
        assert!(
            value_of(shard_of(&nodes, leaving), key).is_none(),
            "the bucket's content is dropped once disown grace elapses"
        );
    }
}

// ---------------------------------------------------------------------
// Partition-heal family: three nodes, a two-way partition (node `a` split
// from `b`/`c`, which stay mutually connected) during which side a and
// side b each increment a share of `keys` counters, then a heal. Run in
// two variants over the same shape — one merged `PnCounter` key per counter
// under `PnCounterResolver`, and per-writer keys decomposed under the
// default `LwwResolver` — measuring anti-entropy rounds, virtual time,
// frames, bytes, records, engine applies, resolver folds, and redundant
// pulls spent reconciling after the heal, against the exact expected
// total. `SUNDOG_SIM_FULL` controls the grid's size: the fast default
// (2,000 keys, conflict fraction 0.0/1.0, one seed) or the full sweep
// (2,000 and 20,000 keys, the whole 0.0/0.1/0.5/1.0 conflict-fraction
// range, three seeds).
// ---------------------------------------------------------------------

/// Which side of the fair comparison a partition-heal run exercises: the
/// same content, keyed either per-writer under [`LwwResolver`] (so no key is
/// ever really contended) or on one shared key under [`PnCounterResolver`]
/// (so every overlapping counter is a real, folded merge). This is the
/// fair A/B comparison [`partition_heal_comparison`] measures across every
/// configured key count, conflict fraction, and seed.
///
/// [`LwwResolver`]: sundog::LwwResolver
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Variant {
    Decomposed,
    Merged,
}

impl Variant {
    const fn tag(self) -> &'static str {
        match self {
            Self::Decomposed => "decomposed",
            Self::Merged => "merged",
        }
    }
}

const HEAL_DEFAULT_KEYS: u32 = 2_000;
const HEAL_DEFAULT_CONFLICT_FRACTIONS: [f64; 2] = [0.0, 1.0];
const HEAL_FULL_KEYS: [u32; 2] = [2_000, 20_000];
const HEAL_FULL_CONFLICT_FRACTIONS: [f64; 4] = [0.0, 0.1, 0.5, 1.0];
const HEAL_FULL_SEEDS: [u64; 3] = [0xC0DE_7001, 0xC0DE_7002, 0xC0DE_7003];
const HEAL_INCREMENTS_PER_SIDE: u32 = 3;
const HEAL_PARTITION_MS: u64 = 500;
/// Deliberately generous relative to a single round's own cost, even at
/// this family's largest key count: a repair large enough to need several
/// `REPAIR_BATCH` chunks spans several `sim.step()`s within one
/// `run_round_against` call, and node b's and node c's own independent
/// tick (never partitioned from node a, so *also* a candidate to relay
/// node a's content, and just as capable of fully repairing a pair on its
/// own as node a's own round against that same pair is) can fall on any
/// step in between. A short tick period lets that race resolve differently
/// depending on how many chunks a given key count happens to need -- the
/// scale-sensitive non-determinism the "Findings" section's own
/// non-monotonic figures came from. A period long enough that every node's
/// own round, at every scale this grid drives, always finishes well before
/// the next tick could possibly fire removes the race by removing the
/// overlap: the ordering the two paths land in step order stays the same
/// regardless of key count, since neither path is still running when
/// the next tick becomes possible.
///
/// This is a deliberate trade-off, not an oversight: production's own
/// default `ae_interval` is 200ms (`ClusterConfig::default`,
/// `sundog/src/cluster.rs`), 5x shorter than this constant, specifically
/// because a shorter tick can let node b/c's own independent round overlap
/// the multi-step repair this family drives -- the exact race this constant
/// is chosen to avoid. Every assertion in this family (determinism,
/// monotonicity, exact convergence) therefore measures the resolver and
/// anti-entropy logic in isolation from that scheduling race, not the
/// bidirectional exchange's round count at production's actual tick
/// cadence: a merging resolver's bidirectional exchange trades a round
/// for an extra push/pull pair per divergent key, a real regression at
/// production's actual 200ms cadence; this generous margin can only show
/// that nothing here regresses further, never confirm that regression is
/// fixed. A
/// production-cadence variant, run at (or near) 200ms and tolerant of the
/// resulting scheduling noise, would be needed to confirm that separately.
const HEAL_AE_INTERVAL_MS: u64 = 1000;
/// Every anti-entropy round in this family runs against at most two peers
/// on a 1s tick; a generous multiple of the handful of rounds convergence
/// actually needs at every scale this grid drives, so a run spending more
/// than this many rounds signals a regression rather than ordinary
/// scheduling noise.
const HEAL_MAX_AE_ROUNDS: u64 = 60;
/// `40_000` sits past the `20_000` scale the original round-count
/// regression was measured at, so this pin has headroom beyond the exact
/// point that finding was made at, not just up to it.
const HEAL_MONOTONIC_KEYS: [u32; 5] = [2_000, 8_000, 16_000, 20_000, 40_000];

/// `SUNDOG_SIM_FULL=1` switches [`partition_heal_comparison`] from the fast
/// default grid to the full one: more key counts, the whole conflict-fraction
/// range, and three seeds instead of one.
fn heal_sim_full() -> bool {
    std::env::var("SUNDOG_SIM_FULL").is_ok_and(|v| v == "1")
}

/// One partition-heal scenario's full parameterization; see the module doc
/// comment above [`run_partition_heal`] for what each field drives.
#[derive(Debug, Clone, Copy, PartialEq)]
struct HealConfig {
    seed: u64,
    variant: Variant,
    keys: u32,
    conflict_fraction: f64,
    increments_per_side: u32,
    partition_ms: u64,
    ae_interval_ms: u64,
}

impl HealConfig {
    fn new(seed: u64, variant: Variant, keys: u32, conflict_fraction: f64) -> Self {
        Self {
            seed,
            variant,
            keys,
            conflict_fraction,
            increments_per_side: HEAL_INCREMENTS_PER_SIDE,
            partition_ms: HEAL_PARTITION_MS,
            ae_interval_ms: HEAL_AE_INTERVAL_MS,
        }
    }
}

/// One [`run_partition_heal`] call's reported metrics, from the heal onward.
/// `records`/`applies`/`folds`/`redundant_pulls` are this redesign's own
/// additions over the harness's earlier `PartitionHealMetrics`: `records` is
/// every record either direction actually applied (or attempted to apply),
/// `applies` is the subset that changed the touched key's stored
/// `(version, bytes)`, `folds` is how many times the resolver's `merge`
/// itself returned `Some`, and `redundant_pulls` is the pull-direction
/// subset of `records - applies` -- the "Findings" section's own
/// redundant-pull cost of the bidirectional exchange, isolated from the
/// push direction.
#[derive(Debug, Clone, PartialEq)]
struct HealMetrics {
    variant: &'static str,
    keys: u32,
    conflict_fraction: f64,
    seed: u64,
    ae_rounds: u64,
    virtual_ms: u64,
    frames: u64,
    bytes: u64,
    records: u64,
    applies: u64,
    folds: u64,
    redundant_pulls: u64,
    expected_total: i128,
    actual_total: i128,
    lost_updates: i128,
}

/// Wraps a [`ConflictResolver`] to count every `Some` its [`ConflictResolver::merge`]
/// returns: the "resolver folds" metric, a test-visible counter on the
/// resolver itself rather than anything the production `ConflictResolver`
/// contract exposes.
struct CountingResolver<R> {
    inner: R,
    folds: AtomicU64,
}

impl<R> CountingResolver<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            folds: AtomicU64::new(0),
        }
    }

    fn folds(&self) -> u64 {
        self.folds.load(Ordering::Relaxed)
    }
}

impl<R: ConflictResolver> ConflictResolver for CountingResolver<R> {
    fn winner(&self, key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        self.inner.winner(key, a, b)
    }

    fn needs_value_bytes(&self) -> bool {
        self.inner.needs_value_bytes()
    }

    fn merges(&self) -> bool {
        self.inner.merges()
    }

    fn merge(&self, key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Option<Merged> {
        let merged = self.inner.merge(key, a, b);
        if merged.is_some() {
            self.folds.fetch_add(1, Ordering::Relaxed);
        }
        merged
    }
}

/// Wraps a shard's `ShardOps` surface to count, on THIS wrapper instance
/// specifically, every record passed to `apply_remote`/`apply_remote_batch`
/// and how many of them left the touched key's stored `(version, bytes)` --
/// its exact `WireRecord` -- unchanged: a genuine no-op fold, by the same
/// byte-exact criterion `resolve_and_rebind` itself uses to skip
/// re-publishing and re-replicating a redelivered merge. A node keeps two
/// independently-counting instances wrapping the very same inner shard: one
/// given to `dispatch_inbound` for the push path, one given to
/// `run_round_against` for the pull path -- `ShardOps::apply_remote_batch`'s
/// own signature carries no tag saying which direction a call arrived
/// through, so which wrapper instance a call lands on is the only thing
/// that tells push apart from pull. [`HealNode`] pairs the two up around one
/// real shard.
struct CountingShard<S> {
    inner: Arc<S>,
    records: AtomicU64,
    redundant: AtomicU64,
}

impl<S> CountingShard<S> {
    fn new(inner: Arc<S>) -> Self {
        Self {
            inner,
            records: AtomicU64::new(0),
            redundant: AtomicU64::new(0),
        }
    }

    fn records(&self) -> u64 {
        self.records.load(Ordering::Relaxed)
    }

    fn redundant(&self) -> u64 {
        self.redundant.load(Ordering::Relaxed)
    }
}

impl<S: ShardOps + 'static> ShardOps for CountingShard<S> {
    fn apply_remote(&self, rec: WireRecord) -> BoxFuture<'_, ()> {
        self.apply_remote_batch(vec![rec])
    }

    fn apply_remote_batch(&self, recs: Vec<WireRecord>) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            if recs.is_empty() {
                return;
            }
            self.records.fetch_add(recs.len() as u64, Ordering::Relaxed);
            let keys: Vec<Bytes> = recs.iter().map(|rec| rec.key.clone()).collect();
            let before: HashMap<Bytes, WireRecord> = self
                .inner
                .records_for(keys.clone())
                .await
                .into_iter()
                .map(|rec| (rec.key.clone(), rec))
                .collect();
            self.inner.apply_remote_batch(recs).await;
            let after = self.inner.records_for(keys).await;
            let redundant = after
                .into_iter()
                .filter(|rec| before.get(&rec.key).is_some_and(|prior| prior == rec))
                .count();
            if redundant > 0 {
                self.redundant
                    .fetch_add(redundant as u64, Ordering::Relaxed);
            }
        })
    }

    fn invalidate(&self, key: Bytes, ver: Hlc) -> BoxFuture<'_, ()> {
        self.inner.invalidate(key, ver)
    }

    fn digests(&self) -> BoxFuture<'_, Vec<(u16, u64)>> {
        self.inner.digests()
    }

    fn ae_digests_for(&self, peer: NodeId) -> BoxFuture<'_, Vec<(u16, u64)>> {
        self.inner.ae_digests_for(peer)
    }

    fn bucket_entries(&self, bucket: u16) -> BoxFuture<'_, Vec<(Bytes, Hlc)>> {
        self.inner.bucket_entries(bucket)
    }

    fn entries_for_buckets(
        &self,
        buckets: Vec<u16>,
    ) -> BoxFuture<'_, sundog::store::BucketEntries> {
        self.inner.entries_for_buckets(buckets)
    }

    fn bucket_lens(&self, buckets: Vec<u16>) -> BoxFuture<'_, Vec<(u16, usize)>> {
        self.inner.bucket_lens(buckets)
    }

    fn part_digests(&self, buckets: Vec<u16>) -> BoxFuture<'_, Vec<(u16, Vec<u64>)>> {
        self.inner.part_digests(buckets)
    }

    fn entries_for_parts(
        &self,
        parts: Vec<(u16, u8)>,
    ) -> BoxFuture<'_, sundog::store::PartEntries> {
        self.inner.entries_for_parts(parts)
    }

    fn records_for(&self, keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>> {
        self.inner.records_for(keys)
    }

    fn snapshot_chunks(&self) -> BoxStream<'static, Vec<WireRecord>> {
        self.inner.snapshot_chunks()
    }

    fn gc_tombstones(&self, any_member_absent: bool) -> BoxFuture<'_, ()> {
        self.inner.gc_tombstones(any_member_absent)
    }

    fn run_pending_tasks(&self) -> BoxFuture<'_, ()> {
        self.inner.run_pending_tasks()
    }

    fn merges(&self) -> bool {
        self.inner.merges()
    }
}

/// One node's three handles onto the same real shard: `real` for direct
/// local writes and reads from the test driver, `push` wrapping it for the
/// inbound-dispatch path, `pull`/`pull_dyn` wrapping it (the same instance,
/// two views) for the anti-entropy-initiator path through
/// `sundog::run_round_against`. See [`CountingShard`]'s own doc for why push
/// and pull need separate wrapper instances at all.
struct HealNode {
    real: Arc<Shard<String, PnCounter>>,
    push: Arc<CountingShard<Shard<String, PnCounter>>>,
    pull: Arc<CountingShard<Shard<String, PnCounter>>>,
    pull_dyn: Arc<dyn ShardOps>,
}

impl HealNode {
    fn new(real: Arc<Shard<String, PnCounter>>) -> Self {
        let push = Arc::new(CountingShard::new(Arc::clone(&real)));
        let pull = Arc::new(CountingShard::new(Arc::clone(&real)));
        let pull_dyn: Arc<dyn ShardOps> = Arc::clone(&pull) as Arc<dyn ShardOps>;
        Self {
            real,
            push,
            pull,
            pull_dyn,
        }
    }

    fn records(&self) -> u64 {
        self.push.records() + self.pull.records()
    }

    fn redundant(&self) -> u64 {
        self.push.redundant() + self.pull.redundant()
    }

    fn redundant_pulls(&self) -> u64 {
        self.pull.redundant()
    }
}

fn merged_key(i: u32) -> String {
    format!("counter:{i}")
}

fn decomposed_key(i: u32, side: &str) -> String {
    format!("counter:{i}:{side}")
}

/// How many of `keys` counters land in the both-sides-written overlap:
/// `conflict_fraction * keys`, rounded and clamped into `0..=keys`.
fn overlap_count(keys: u32, conflict_fraction: f64) -> u32 {
    let scaled = (f64::from(keys) * conflict_fraction).round();
    if scaled <= 0.0 {
        return 0;
    }
    if scaled >= f64::from(keys) {
        return keys;
    }
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "scaled is already clamped into (0.0, f64::from(keys)) by the checks above, so \
                  this truncating, sign-losing cast can never actually truncate or flip sign"
    )]
    let overlap = scaled as u32;
    overlap
}

/// The exact grand total every counter converges to: side A contributes
/// `increments_per_side` to each of the `overlap_count` counters it writes,
/// side B contributes `increments_per_side` to every one of `keys` counters
/// (its write set is the overlap plus the rest), and `PnCounter::local_delta`
/// is cumulative, so only each writer's final round survives the merge.
fn expected_total(keys: u32, conflict_fraction: f64, increments_per_side: u32) -> i128 {
    let overlap = overlap_count(keys, conflict_fraction);
    let inc = i128::from(increments_per_side);
    (i128::from(keys) + i128::from(overlap)) * inc
}

/// Writes `increments_per_side` cumulative rounds to every counter in
/// `range` from `writer`, under `side`'s label -- `"a"` or `"b"` -- keyed per
/// [`Variant`]: merged writes the shared `counter:{i}` key every writer
/// folds into; decomposed writes this side's own `counter:{i}:{side}` key,
/// never contended since no other writer ever touches it.
fn write_side(
    shard: &Shard<String, PnCounter>,
    writer: WriterId,
    side: &'static str,
    range: std::ops::Range<u32>,
    variant: Variant,
    increments_per_side: u32,
) {
    block_on(async {
        for round in 1..=increments_per_side {
            for i in range.clone() {
                let key = match variant {
                    Variant::Merged => merged_key(i),
                    Variant::Decomposed => decomposed_key(i, side),
                };
                let _ = shard
                    .insert(key, PnCounter::local_delta(writer, u64::from(round)))
                    .await;
            }
        }
    });
}

/// Reads a shard's converged grand total back out, matching [`write_side`]'s
/// own key scheme per [`Variant`]: one shared key per counter for merged, two
/// per-side keys per counter for decomposed.
fn heal_digests_of(shard: &Shard<String, PnCounter>) -> Vec<(u16, u64)> {
    block_on(ShardOps::digests(shard))
}

fn counter_total(shard: &Shard<String, PnCounter>, keys: u32, variant: Variant) -> i128 {
    block_on(async {
        let mut total = 0i128;
        for i in 0..keys {
            match variant {
                Variant::Merged => {
                    if let Some(counter) = shard.get(&merged_key(i)).await {
                        total += counter.value();
                    }
                }
                Variant::Decomposed => {
                    for side in ["a", "b"] {
                        if let Some(counter) = shard.get(&decomposed_key(i, side)).await {
                            total += counter.value();
                        }
                    }
                }
            }
        }
        total
    })
}

/// A namespace unique to `cfg`: folded into every host name, the cache name,
/// and every node id this run builds, so two `run_partition_heal` calls in
/// one process -- sequential or, under `cargo test`'s default parallelism,
/// concurrent -- never share a host, a cache-labeled metric, or a node id,
/// regardless of how many fields they otherwise have in common.
fn heal_run_id(cfg: &HealConfig) -> String {
    let tag = cfg.variant.tag();
    let hash = xxh3_64(
        format!(
            "{tag}-{}-{}-{}-{}-{}",
            cfg.seed, cfg.keys, cfg.conflict_fraction, cfg.increments_per_side, cfg.ae_interval_ms
        )
        .as_bytes(),
    );
    format!("heal-{tag}-{hash:016x}")
}

fn heal_node_id(run_id: &str, role: &str) -> NodeId {
    NodeId::from(xxh3_64(format!("{run_id}-{role}").as_bytes()))
}

/// A partition-heal node's whole role: dispatch inbound traffic through its
/// `push` [`CountingShard`], and run `sundog::run_round_against` -- the
/// production anti-entropy round, not a reimplementation -- against every
/// peer on `peers` on a timer through its `pull` one. Writes are applied
/// directly to `real` from outside this loop, the same direct-write pattern
/// `run_partition_delete_scenario` uses, so this loop only has to keep the
/// mesh alive and reconcile.
///
/// `ae_rounds` only counts a round against a peer in `counted_peers`: node
/// b and node c are never partitioned from each other, so their own mutual
/// rounds settle before the measurement window opens and stay a routine,
/// always-no-op background rate the same as with any other steady pair
/// (see `run_partition_heal`'s own pre-heal settle step) -- counting them
/// alongside the rounds that actually repair the a/b-c split would mix an
/// irrelevant, scale-independent tick rate into a metric meant to measure
/// the repair itself.
#[derive(Clone)]
struct HealNodeParams {
    node: NodeId,
    port: u16,
    peers: Vec<(NodeId, String, u16)>,
    counted_peers: Vec<NodeId>,
    ae_period: Duration,
    ae_rounds: Arc<AtomicU64>,
    cache: SmolStr,
    outbox_capacity: usize,
}

async fn heal_node_loop(
    params: HealNodeParams,
    real: Arc<Shard<String, PnCounter>>,
    push: Arc<CountingShard<Shard<String, PnCounter>>>,
    pull: Arc<dyn ShardOps>,
) -> SimResult {
    let handler: Arc<dyn RequestHandler> = Arc::new(ShardHandler::new(real));
    let bind_addr = SocketAddr::from(([0, 0, 0, 0], params.port));
    let config = ClusterConfig::default().with(|c| c.outbox_capacity = params.outbox_capacity);
    let (mesh, mut inbound) = Mesh::spawn(bind_addr, params.node, 1, &config, handler).await?;

    let borrowed: Vec<(NodeId, &str, u16)> = params
        .peers
        .iter()
        .map(|(node, host, port)| (*node, host.as_str(), *port))
        .collect();
    let peer_list = peer_list_of(&borrowed);
    mesh.update_peers(peer_list.clone());
    let peer_ids: Vec<NodeId> = peer_list.iter().map(|peer| peer.node).collect();

    let mut ae_tick = tokio::time::interval(params.ae_period);
    ae_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            biased;
            Some(InboundMsg { msg, .. }) = inbound.recv() => {
                dispatch_inbound(push.as_ref(), msg).await;
            }
            _ = ae_tick.tick() => {
                for &peer in &peer_ids {
                    sundog::run_round_against(&mesh, &pull, &params.cache, peer).await;
                    if params.counted_peers.contains(&peer) {
                        params.ae_rounds.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "one node's registration: host, identity, its peer roster, the shared per-run \
              counters, and its own counting-shard handles all belong together at one call site"
)]
fn spawn_heal_host(
    sim: &mut Sim<'_>,
    host: &str,
    node: NodeId,
    port: u16,
    peers: Vec<(NodeId, String, u16)>,
    counted_peers: Vec<NodeId>,
    ae_period: Duration,
    ae_rounds: Arc<AtomicU64>,
    cache: SmolStr,
    outbox_capacity: usize,
    heal_node: &HealNode,
) {
    let params = HealNodeParams {
        node,
        port,
        peers,
        counted_peers,
        ae_period,
        ae_rounds,
        cache,
        outbox_capacity,
    };
    let real = Arc::clone(&heal_node.real);
    let push = Arc::clone(&heal_node.push);
    let pull = Arc::clone(&heal_node.pull_dyn);
    sim.host(host, move || {
        let params = params.clone();
        let real = Arc::clone(&real);
        let push = Arc::clone(&push);
        let pull = Arc::clone(&pull);
        async move { heal_node_loop(params, real, push, pull).await }
    });
}

/// [`run_partition_heal`]'s wire-metrics measurement window, guarded the
/// same way [`HEAL_WIRE_METRICS_LOCK`]'s predecessor was: `frames_sent_total`/
/// `bytes_sent_total` are process-wide, shared with every other test in this
/// binary, so only one partition-heal run's window is open at a time.
static HEAL_WIRE_METRICS_LOCK: StdMutex<()> = StdMutex::new(());

/// Runs one partition-heal scenario end to end and returns its metrics from
/// the heal onward: a fresh `turmoil::Sim`, three nodes with node `a` split
/// from `b`/`c` (who stay mutually connected), the write schedule
/// `cfg.conflict_fraction` and `cfg.variant` select, a heal, then anti-entropy
/// -- run through the production `sundog::run_round_against` entry point,
/// never a reimplementation of it -- until every node's fingerprint agrees
/// and every counter reads the exact expected total.
#[allow(
    clippy::too_many_lines,
    reason = "one scenario driver's full setup, write/heal schedule, and convergence check read \
              best kept together"
)]
fn run_partition_heal(cfg: HealConfig) -> HealMetrics {
    const PORT: u16 = 4900;

    let tag = cfg.variant.tag();
    let run_id = heal_run_id(&cfg);
    let cache = SmolStr::new(run_id.clone());
    let node_a = heal_node_id(&run_id, "a");
    let node_b = heal_node_id(&run_id, "b");
    let node_c = heal_node_id(&run_id, "c");
    let host_a = format!("{run_id}-a");
    let host_b = format!("{run_id}-b");
    let host_c = format!("{run_id}-c");

    let counting_resolver = match cfg.variant {
        Variant::Merged => Some(Arc::new(CountingResolver::new(PnCounterResolver))),
        Variant::Decomposed => None,
    };
    let dyn_resolver: Option<Arc<dyn ConflictResolver>> = counting_resolver
        .as_ref()
        .map(|r| Arc::clone(r) as Arc<dyn ConflictResolver>);

    let build_shard = |node: NodeId| -> Shard<String, PnCounter> {
        let shard = Shard::new(cache.clone(), Mode::Replicated, node, 200_000, None, None);
        match &dyn_resolver {
            Some(r) => shard.with_resolver(Arc::clone(r)),
            None => shard,
        }
    };

    let a = HealNode::new(Arc::new(build_shard(node_a)));
    let b = HealNode::new(Arc::new(build_shard(node_b)));
    let c = HealNode::new(Arc::new(build_shard(node_c)));

    let ae_period = Duration::from_millis(cfg.ae_interval_ms);
    let outbox_capacity = (cfg.keys as usize)
        .saturating_mul(4)
        .max(ClusterConfig::default().outbox_capacity);
    let ae_rounds = Arc::new(AtomicU64::new(0));

    // Zero message latency, unlike every other scenario in this file: with
    // it at the usual few milliseconds, the exact number of rounds counted
    // before convergence is detected depends on how many random-latency
    // draws a run's own frame count consumes -- itself a function of key
    // count -- so the round count picks up scale-dependent scheduling noise
    // having nothing to do with the anti-entropy protocol's own cost, the
    // harness-artifact failure mode this redesign exists to remove. At zero
    // latency every delivery lands in the same tick every run, so a
    // config's round count depends only on the protocol and the data, not
    // on how many bytes happened to shift a message's place in the RNG
    // stream.
    let mut sim = Builder::new()
        .rng_seed(cfg.seed)
        .tick_duration(TICK)
        .max_message_latency(Duration::ZERO)
        .build();

    spawn_heal_host(
        &mut sim,
        &host_a,
        node_a,
        PORT,
        vec![
            (node_b, host_b.clone(), PORT),
            (node_c, host_c.clone(), PORT),
        ],
        vec![node_b, node_c],
        ae_period,
        Arc::clone(&ae_rounds),
        cache.clone(),
        outbox_capacity,
        &a,
    );
    spawn_heal_host(
        &mut sim,
        &host_b,
        node_b,
        PORT,
        vec![
            (node_a, host_a.clone(), PORT),
            (node_c, host_c.clone(), PORT),
        ],
        vec![node_a],
        ae_period,
        Arc::clone(&ae_rounds),
        cache.clone(),
        outbox_capacity,
        &b,
    );
    spawn_heal_host(
        &mut sim,
        &host_c,
        node_c,
        PORT,
        vec![
            (node_a, host_a.clone(), PORT),
            (node_b, host_b.clone(), PORT),
        ],
        vec![node_a],
        ae_period,
        Arc::clone(&ae_rounds),
        cache.clone(),
        outbox_capacity,
        &c,
    );

    // Two-way split: node-a alone on one side, node-b/node-c together on the
    // other. Applied before any step runs, so node-a's mesh never gets the
    // chance to dial node-b/node-c at all -- see the predecessor of this
    // driver's own long-standing note on why that ordering matters for
    // determinism (`ReqPool`'s freshness window is real wall-clock time,
    // never virtualized).
    sim.partition(host_a.as_str(), host_b.as_str());
    sim.partition(host_a.as_str(), host_c.as_str());

    // Let the mesh's connections settle (node-b/node-c's with each other;
    // node-a's dial attempts toward either fail fast and back off, already
    // partitioned).
    run_steps(&mut sim, steps_for(Duration::from_millis(100)));

    let overlap = overlap_count(cfg.keys, cfg.conflict_fraction);
    write_side(
        a.real.as_ref(),
        WriterId::new(node_a, 1),
        "a",
        0..overlap,
        cfg.variant,
        cfg.increments_per_side,
    );
    write_side(
        b.real.as_ref(),
        WriterId::new(node_b, 1),
        "b",
        0..cfg.keys,
        cfg.variant,
        cfg.increments_per_side,
    );

    // Give node-b/node-c, never partitioned from each other, time to settle
    // between themselves before the measurement window starts, so the
    // reported cost is anti-entropy repairing node-a's side of the split
    // rather than ordinary in-partition b/c traffic.
    run_steps(&mut sim, steps_for(Duration::from_millis(cfg.partition_ms)));

    let wire_guard = HEAL_WIRE_METRICS_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let frames_before = sundog::net::frames_sent_total();
    let bytes_before = sundog::net::bytes_sent_total();
    let ae_rounds_before = ae_rounds.load(Ordering::Relaxed);
    let records_before = a.records() + b.records() + c.records();
    let redundant_before = a.redundant() + b.redundant() + c.redundant();
    let redundant_pulls_before = a.redundant_pulls() + b.redundant_pulls() + c.redundant_pulls();
    let folds_before = counting_resolver.as_ref().map_or(0, |r| r.folds());

    sim.repair(host_a.as_str(), host_b.as_str());
    sim.repair(host_a.as_str(), host_c.as_str());

    let expected = expected_total(cfg.keys, cfg.conflict_fraction, cfg.increments_per_side);
    let budget = steps_for(ae_period * 40 + Duration::from_secs(2));
    let steps_to_converge = run_until(&mut sim, budget, || {
        let da = heal_digests_of(a.real.as_ref());
        let db = heal_digests_of(b.real.as_ref());
        let dc = heal_digests_of(c.real.as_ref());
        da == db
            && db == dc
            && counter_total(a.real.as_ref(), cfg.keys, cfg.variant) == expected
            && counter_total(b.real.as_ref(), cfg.keys, cfg.variant) == expected
            && counter_total(c.real.as_ref(), cfg.keys, cfg.variant) == expected
    })
    .unwrap_or_else(|| {
        panic!(
            "{tag}: partition-heal did not converge within the budget (keys={}, conflict_fraction={})",
            cfg.keys, cfg.conflict_fraction
        )
    });

    let frames = sundog::net::frames_sent_total() - frames_before;
    let bytes = sundog::net::bytes_sent_total() - bytes_before;
    let ae_rounds_spent = ae_rounds.load(Ordering::Relaxed) - ae_rounds_before;
    let records = a.records() + b.records() + c.records() - records_before;
    let redundant = a.redundant() + b.redundant() + c.redundant() - redundant_before;
    let redundant_pulls =
        a.redundant_pulls() + b.redundant_pulls() + c.redundant_pulls() - redundant_pulls_before;
    let folds = counting_resolver.as_ref().map_or(0, |r| r.folds()) - folds_before;
    drop(wire_guard);

    let actual_total = counter_total(a.real.as_ref(), cfg.keys, cfg.variant);

    let metrics = HealMetrics {
        variant: tag,
        keys: cfg.keys,
        conflict_fraction: cfg.conflict_fraction,
        seed: cfg.seed,
        ae_rounds: ae_rounds_spent,
        virtual_ms: steps_to_ms(steps_to_converge),
        frames,
        bytes,
        records,
        applies: records.saturating_sub(redundant),
        folds,
        redundant_pulls,
        expected_total: expected,
        actual_total,
        lost_updates: expected - actual_total,
    };
    print_heal_metrics(&metrics);
    metrics
}

fn steps_to_ms(steps: usize) -> u64 {
    u64::try_from(steps)
        .expect("test-scale step counts fit in a u64")
        .saturating_mul(u64::try_from(TICK.as_millis()).expect("TICK's millis count fits in a u64"))
}

fn print_heal_metrics(m: &HealMetrics) {
    eprintln!(
        "SIM partition_heal variant={} keys={} conflict_fraction={} seed={:#x} ae_rounds={} \
         virtual_ms={} frames={} bytes={} records={} applies={} folds={} redundant_pulls={} \
         expected_total={} actual_total={} lost_updates={}",
        m.variant,
        m.keys,
        m.conflict_fraction,
        m.seed,
        m.ae_rounds,
        m.virtual_ms,
        m.frames,
        m.bytes,
        m.records,
        m.applies,
        m.folds,
        m.redundant_pulls,
        m.expected_total,
        m.actual_total,
        m.lost_updates
    );
}

/// A ratio only needs `f64`'s exact-integer range, up to 2^53, comfortably
/// covering this family's counters at every scale this grid drives; mirrors
/// `engine::count_f64` and `spill::bytes_used_f64`'s own precedent for the
/// same allow.
#[allow(clippy::cast_precision_loss)]
fn heal_ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        f64::NAN
    } else {
        numerator as f64 / denominator as f64
    }
}

fn print_heal_comparison(decomposed: &HealMetrics, merged: &HealMetrics) {
    eprintln!(
        "SIM partition_heal_pair keys={} conflict_fraction={} seed={:#x} ae_rounds_ratio={:.3} \
         virtual_ms_ratio={:.3} bytes_ratio={:.3}",
        merged.keys,
        merged.conflict_fraction,
        merged.seed,
        heal_ratio(merged.ae_rounds, decomposed.ae_rounds),
        heal_ratio(merged.virtual_ms, decomposed.virtual_ms),
        heal_ratio(merged.bytes, decomposed.bytes),
    );
}

/// Correctness only, at the default grid: every combination of variant and
/// conflict fraction converges to the exact expected total. The convergence
/// loop inside `run_partition_heal` already gates on this (and on all three
/// nodes' fingerprints agreeing) before it ever returns, so a `HealMetrics`
/// this test receives at all already reflects a converged, correct run;
/// `lost_updates == 0` restates that guarantee explicitly rather than
/// relying on it being implicit.
#[test]
fn partition_heal_variant_reaches_exact_totals() {
    let seed = sim_seed(0xC0DE_7100);
    for variant in [Variant::Decomposed, Variant::Merged] {
        for &conflict_fraction in &HEAL_DEFAULT_CONFLICT_FRACTIONS {
            let metrics = run_partition_heal(HealConfig::new(
                seed,
                variant,
                HEAL_DEFAULT_KEYS,
                conflict_fraction,
            ));
            assert_eq!(
                metrics.lost_updates,
                0,
                "{}: keys={} conflict_fraction={conflict_fraction} did not converge to the exact total",
                variant.tag(),
                metrics.keys
            );
        }
    }
}

/// The fair decomposed-vs-merged comparison: for every config in the grid,
/// runs decomposed then merged with the same seed, prints one `SIM` line
/// per run and one `SIM partition_heal_pair` line per pair, and asserts only
/// that both sides converged correctly and within
/// [`HEAL_MAX_AE_ROUNDS`] -- never a specific ratio between them, which the
/// printed `SIM`/`SIM partition_heal_pair` lines are there to read, not a
/// property to pin.
/// `SUNDOG_SIM_FULL=1` swaps the fast default grid (2,000 keys, conflict
/// fraction 0.0/1.0, one seed) for the full one (2,000 and 20,000 keys, the
/// whole 0.0/0.1/0.5/1.0 conflict-fraction range, three seeds).
#[test]
fn partition_heal_comparison() {
    let full = heal_sim_full();
    let keys_grid: &[u32] = if full {
        &HEAL_FULL_KEYS
    } else {
        &[HEAL_DEFAULT_KEYS]
    };
    let fractions: &[f64] = if full {
        &HEAL_FULL_CONFLICT_FRACTIONS
    } else {
        &HEAL_DEFAULT_CONFLICT_FRACTIONS
    };
    let default_seed = [sim_seed(0xC0DE_7200)];
    let seeds: &[u64] = if full {
        &HEAL_FULL_SEEDS
    } else {
        &default_seed
    };

    for &keys in keys_grid {
        for &conflict_fraction in fractions {
            for &seed in seeds {
                let decomposed = run_partition_heal(HealConfig::new(
                    seed,
                    Variant::Decomposed,
                    keys,
                    conflict_fraction,
                ));
                let merged = run_partition_heal(HealConfig::new(
                    seed,
                    Variant::Merged,
                    keys,
                    conflict_fraction,
                ));

                for metrics in [&decomposed, &merged] {
                    assert_eq!(
                        metrics.lost_updates, 0,
                        "{}: keys={keys} conflict_fraction={conflict_fraction} seed={seed:#x} \
                         did not converge to the exact total",
                        metrics.variant
                    );
                    assert!(
                        metrics.ae_rounds <= HEAL_MAX_AE_ROUNDS,
                        "{}: keys={keys} conflict_fraction={conflict_fraction} seed={seed:#x} \
                         spent {} anti-entropy rounds, over the {HEAL_MAX_AE_ROUNDS} sanity bound",
                        metrics.variant,
                        metrics.ae_rounds
                    );
                }
                print_heal_comparison(&decomposed, &merged);
            }
        }
    }
}

/// The same config, run twice in one process, must yield identical metrics
/// on every field `run_partition_heal`'s own simulation determines:
/// `run_partition_heal`'s per-run namespacing (see [`heal_run_id`]) is what
/// makes two calls in one process share nothing, so nothing about the
/// second run's `Sim`, shards, or counters can be influenced by the
/// first's. `frames`/`bytes` are the one deliberate exception: they read
/// `sundog::net`'s process-wide wire counters (see
/// [`HEAL_WIRE_METRICS_LOCK`]'s own doc), so under `cargo test`'s default
/// parallelism a wholly unrelated sim test running concurrently on another
/// thread can add to those same two counters mid-window -- a real source of
/// noise this test's own two `run_partition_heal` calls cannot serialize
/// against, since it comes from other tests entirely. Every field this
/// harness itself controls is still asserted equal.
#[test]
fn partition_heal_is_deterministic_for_a_fixed_config() {
    let cfg = HealConfig::new(
        sim_seed(0xC0DE_7300),
        Variant::Merged,
        HEAL_DEFAULT_KEYS,
        1.0,
    );
    let first = run_partition_heal(cfg);
    let second = run_partition_heal(cfg);
    let msg = "the same partition-heal config run twice in one process must yield identical \
               metrics on every field this harness itself controls (frames/bytes excepted -- \
               see this test's own doc)";
    assert_eq!(
        (
            first.variant,
            first.keys,
            first.conflict_fraction,
            first.seed
        ),
        (
            second.variant,
            second.keys,
            second.conflict_fraction,
            second.seed
        ),
        "{msg}"
    );
    assert_eq!(
        (
            first.ae_rounds,
            first.virtual_ms,
            first.records,
            first.applies
        ),
        (
            second.ae_rounds,
            second.virtual_ms,
            second.records,
            second.applies
        ),
        "{msg}"
    );
    assert_eq!(
        (
            first.folds,
            first.redundant_pulls,
            first.expected_total,
            first.actual_total,
            first.lost_updates,
        ),
        (
            second.folds,
            second.redundant_pulls,
            second.expected_total,
            second.actual_total,
            second.lost_updates,
        ),
        "{msg}"
    );
}

/// Pins the "Findings" section's own regression signature: the harness
/// artifact that reimplemented anti-entropy produced non-monotonic round
/// counts as key count grew (3 rounds at 8,000 keys, 19 at 16,000, 5 at
/// 20,000). Driving rounds through the production `run_round_against` entry
/// point instead should make rounds monotone non-decreasing in key count, at
/// fixed full conflict, for both variants.
#[test]
fn partition_heal_rounds_are_monotone_in_key_count() {
    let seed = sim_seed(0xC0DE_7400);
    for variant in [Variant::Decomposed, Variant::Merged] {
        let mut previous = 0u64;
        for &keys in &HEAL_MONOTONIC_KEYS {
            let metrics = run_partition_heal(HealConfig::new(seed, variant, keys, 1.0));
            assert_eq!(
                metrics.lost_updates,
                0,
                "{}: keys={keys} did not converge to the exact total",
                variant.tag()
            );
            assert!(
                metrics.ae_rounds >= previous,
                "{}: ae_rounds regressed from {previous} at a smaller key count to {} at \
                 keys={keys} -- rounds must be monotone non-decreasing in key count",
                variant.tag(),
                metrics.ae_rounds
            );
            previous = metrics.ae_rounds;
        }
    }
}

// ---------------------------------------------------------------------------
// CRDT writer retirement: churn + restart scenario. `ConflictResolver::compact`/
// `ShardOps::compact_pass` (`sundog::store`) and `PnCounter::compact`'s
// effects (`sundog::crdt`) are reachable through the public API; the
// retirement-eligibility and cache-quiet predicates that decide *when* a
// real cluster calls them
// (`cluster.rs::crdt_writer_is_retirement_eligible`/`crdt_cache_is_quiet`)
// are `pub(crate)` and unreachable from an integration test, so -- exactly
// as this file already reimplements `cluster::rebalance`'s reaction to a
// membership change in `republish_view` above -- this section reimplements
// that eligibility rule directly against a hand-scripted membership
// timeline. `now_ms` is read from [`turmoil::Sim::elapsed`], so a "short
// `crdt_retire_after`" means short virtual time, not real wall-clock time.

/// One member's retirement-relevant state as this harness tracks it,
/// mirroring `cluster.rs::MemberView` (`pub(crate)`, unreachable from here).
#[derive(Debug, Clone, Copy)]
enum CrdtPresence {
    /// Continuously live under `incarnation` since `since_ms`.
    Present { since_ms: u64, incarnation: u64 },
    /// Continuously absent (dropped from the live view without a graceful
    /// departure) since `since_ms`.
    Absent { since_ms: u64 },
}

type CrdtMembership = HashMap<NodeId, CrdtPresence>;

/// Whether writer `w` is dead: its node has been continuously absent for
/// at least `bound_ms`, or is live under any *other* incarnation than
/// `w`'s own. No ordering is required, so a clock stepping backward across
/// a restart can never pin an old incarnation forever.
fn crdt_writer_is_dead(w: WriterId, presence: CrdtPresence, now_ms: u64, bound_ms: u64) -> bool {
    match presence {
        CrdtPresence::Absent { since_ms } => now_ms.saturating_sub(since_ms) >= bound_ms,
        CrdtPresence::Present { incarnation, .. } => incarnation != w.incarnation(),
    }
}

/// Whether `presence` has held unchanged, continuously present or
/// continuously absent alike, for at least `bound_ms`: the per-member
/// half of the cache-wide quiet rule.
fn crdt_member_is_settled(presence: CrdtPresence, now_ms: u64, bound_ms: u64) -> bool {
    let since_ms = match presence {
        CrdtPresence::Present { since_ms, .. } | CrdtPresence::Absent { since_ms } => since_ms,
    };
    now_ms.saturating_sub(since_ms) >= bound_ms
}

/// The cache-wide quiet rule: every member other than `local` is
/// settled.
fn crdt_cache_is_quiet(
    members: &CrdtMembership,
    local: NodeId,
    now_ms: u64,
    bound_ms: u64,
) -> bool {
    members
        .iter()
        .filter(|&(&node, _)| node != local)
        .all(|(_, &presence)| crdt_member_is_settled(presence, now_ms, bound_ms))
}

/// This simulation's virtual time so far, in milliseconds.
fn elapsed_ms(sim: &Sim<'_>) -> u64 {
    u64::try_from(sim.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// One compaction sweep tick, reimplementing `cluster.rs::crdt_compact_task`'s
/// own per-tick logic against this harness's hand-scripted `members`:
/// builds the `retire` closure from [`crdt_writer_is_dead`] (excluding
/// `local`, which never retires its own slot) and `quiet` from
/// [`crdt_cache_is_quiet`], then drives [`ShardOps::compact_pass`] directly
/// from the test thread -- the same "drive the production task's own logic
/// by hand" idiom [`republish_view`] already uses for rebalance above.
fn run_compact_tick<S: ShardOps>(
    shard: &S,
    local: NodeId,
    members: &CrdtMembership,
    now_ms: u64,
    bound_ms: u64,
    batch: usize,
) -> (Vec<WriterId>, usize) {
    let quiet = crdt_cache_is_quiet(members, local, now_ms, bound_ms);
    let members_for_retire = members.clone();
    let retire = move |w: WriterId| {
        w.node() != local
            && members_for_retire
                .get(&w.node())
                .is_some_and(|&presence| crdt_writer_is_dead(w, presence, now_ms, bound_ms))
    };
    block_on(ShardOps::compact_pass(
        shard, now_ms, &retire, quiet, bound_ms, batch,
    ))
}

/// A minimal per-node loop for the CRDT-compaction scenarios: inbound
/// dispatch plus periodic anti-entropy only, no write tick and no fan-out
/// -- every write and every compaction sweep in these scenarios is driven
/// directly from the test thread against the very same `Arc<Shard<..>>`
/// this loop shares, exactly as [`run_partition_heal`]'s own `write_side`
/// does against [`HealNode`] above. `incarnation` is read once per
/// (re)spawn, so [`Sim::bounce`] re-running this host's closure after the
/// test driver bumps it is what gives a restarted node a fresh membership
/// incarnation, mirroring how, in production, `Cluster::local_incarnation`
/// and `Mesh::spawn`'s own incarnation are the very same number.
async fn crdt_ae_only_loop<S: ShardOps + 'static>(
    shard: Arc<S>,
    bind_port: u16,
    node: NodeId,
    incarnation: u64,
    peer_list: Vec<Peer>,
    ae_period: Duration,
) -> SimResult {
    let handler: Arc<dyn RequestHandler> = Arc::new(ShardHandler::new(Arc::clone(&shard)));
    let bind_addr = SocketAddr::from(([0, 0, 0, 0], bind_port));
    let (mesh, mut inbound) = Mesh::spawn(
        bind_addr,
        node,
        incarnation,
        &ClusterConfig::default(),
        handler,
    )
    .await?;
    mesh.update_peers(peer_list.clone());
    let peer_ids: Vec<NodeId> = peer_list.iter().map(|peer| peer.node).collect();
    let mut ae_tick = tokio::time::interval(ae_period);
    ae_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            Some(InboundMsg { msg, .. }) = inbound.recv() => {
                dispatch_inbound(shard.as_ref(), msg).await;
            }
            _ = ae_tick.tick() => {
                for &peer in &peer_ids {
                    let _ = ae_round_with_sketch(&mesh, shard.as_ref(), peer, None).await;
                }
            }
        }
    }
}

/// One [`spawn_crdt_host`] call's fixed identity: everything about a node
/// that doesn't change across a [`Sim::bounce`] restart. Grouped into one
/// struct purely to keep `spawn_crdt_host` under clippy's argument-count
/// threshold -- each field is otherwise independent.
struct CrdtHostSpec {
    host: &'static str,
    node: NodeId,
    incarnation: Arc<AtomicU64>,
    port: u16,
    peers: Vec<(NodeId, &'static str, u16)>,
}

/// Spawns [`crdt_ae_only_loop`] as a turmoil host, reading `spec.incarnation`
/// fresh on every (re)spawn so [`Sim::bounce`] gives a restarted node a new
/// membership incarnation.
fn spawn_crdt_host<S>(sim: &mut Sim<'_>, spec: CrdtHostSpec, shard: Arc<S>, ae_period: Duration)
where
    S: ShardOps + 'static,
{
    let CrdtHostSpec {
        host,
        node,
        incarnation,
        port,
        peers,
    } = spec;
    sim.host(host, move || {
        let shard = Arc::clone(&shard);
        let peer_list = peer_list_of(&peers);
        let inc = incarnation.load(Ordering::SeqCst);
        async move { crdt_ae_only_loop(shard, port, node, inc, peer_list, ae_period).await }
    });
}

/// Churn and restart under [`PnCounterResolver`]:
/// a three-node replicated [`PnCounter`] cache with a short, virtual-time
/// `crdt_retire_after` (`BOUND_MS`). Node C is crashed and quickly bounced
/// back under a fresh incarnation (absence under the bound: retirement
/// defers), then crashed again for longer than the bound (absence over the
/// bound: retirement fires once every member has settled). Exact totals
/// are checked on every node at every phase. Stage one has two distinct
/// triggers with different timing: a writer dead by *supersession* (a new
/// incarnation of its own node is now live) is retired on the very next
/// tick, with no quiet wait at all, while a writer dead by *absence*
/// (its node has dropped out of view) still needs `crdt_retire_after` to
/// elapse. Stage two folds a retirement's per-writer metadata into the
/// bounded scalar accumulator only once quiet AND aged past
/// `2 * BOUND_MS`, even with a second writer retired in the meantime, so
/// the record does not keep growing with churn.
#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one scenario's full churn schedule and assertions read best kept together, \
              mirroring distributed_rebalance_under_churn's own allow above"
)]
fn crdt_compaction_pncounter_churn_and_restart_retires_and_folds_exactly() {
    const PORT: u16 = 5210;
    const BOUND_MS: u64 = 2_000;
    const BATCH: usize = 64;
    let key = "ctr".to_string();

    let node_a = NodeId::from(9201);
    let node_b = NodeId::from(9202);
    let node_c = NodeId::from(9203);
    let (host_a, host_b, host_c) = ("crdt-pn-a", "crdt-pn-b", "crdt-pn-c");

    let make_shard = |node: NodeId| -> Arc<Shard<String, PnCounter>> {
        Arc::new(
            Shard::new(cache_name(), Mode::Replicated, node, 10_000, None, None)
                .with_resolver(Arc::new(PnCounterResolver) as Arc<dyn ConflictResolver>),
        )
    };
    let shard_a = make_shard(node_a);
    let shard_b = make_shard(node_b);
    let shard_c = make_shard(node_c);

    let inc_c = Arc::new(AtomicU64::new(1));
    let ae_period = Duration::from_millis(50);

    let mut sim = Builder::new()
        .rng_seed(sim_seed(0xC8D7_1001))
        .tick_duration(TICK)
        .build();

    spawn_crdt_host(
        &mut sim,
        CrdtHostSpec {
            host: host_a,
            node: node_a,
            incarnation: Arc::new(AtomicU64::new(1)),
            port: PORT,
            peers: vec![(node_b, host_b, PORT), (node_c, host_c, PORT)],
        },
        Arc::clone(&shard_a),
        ae_period,
    );
    spawn_crdt_host(
        &mut sim,
        CrdtHostSpec {
            host: host_b,
            node: node_b,
            incarnation: Arc::new(AtomicU64::new(1)),
            port: PORT,
            peers: vec![(node_a, host_a, PORT), (node_c, host_c, PORT)],
        },
        Arc::clone(&shard_b),
        ae_period,
    );
    spawn_crdt_host(
        &mut sim,
        CrdtHostSpec {
            host: host_c,
            node: node_c,
            incarnation: Arc::clone(&inc_c),
            port: PORT,
            peers: vec![(node_a, host_a, PORT), (node_b, host_b, PORT)],
        },
        Arc::clone(&shard_c),
        ae_period,
    );

    let writer_alpha = WriterId::new(node_a, 1);
    let writer_beta = WriterId::new(node_b, 1);
    let writer_gamma1 = WriterId::new(node_c, 1);

    block_on(shard_a.insert(key.clone(), PnCounter::local_delta(writer_alpha, 10))).unwrap();
    block_on(shard_b.insert(key.clone(), PnCounter::local_delta(writer_beta, 20))).unwrap();
    block_on(shard_c.insert(key.clone(), PnCounter::local_delta(writer_gamma1, 30))).unwrap();

    let value_on = |shard: &Arc<Shard<String, PnCounter>>| -> Option<i128> {
        block_on(shard.get(&key)).map(|c| c.value())
    };

    run_until(&mut sim, steps_for(Duration::from_secs(2)), || {
        value_on(&shard_a) == Some(60)
            && value_on(&shard_b) == Some(60)
            && value_on(&shard_c) == Some(60)
    })
    .expect("the three writers' initial deltas converge to the exact total on every node");

    let mut members: CrdtMembership = HashMap::new();
    for &(node, inc) in &[(node_a, 1u64), (node_b, 1u64), (node_c, 1u64)] {
        members.insert(
            node,
            CrdtPresence::Present {
                since_ms: 0,
                incarnation: inc,
            },
        );
    }

    // -- Phase 1: crash C; less than the bound must defer retirement
    // everywhere, and no data is lost while it defers. --
    sim.crash(host_c);
    members.insert(
        node_c,
        CrdtPresence::Absent {
            since_ms: elapsed_ms(&sim),
        },
    );
    run_steps(&mut sim, steps_for(Duration::from_millis(BOUND_MS / 2)));
    let now = elapsed_ms(&sim);
    let (retired_a, _) = run_compact_tick(shard_a.as_ref(), node_a, &members, now, BOUND_MS, BATCH);
    let (retired_b, _) = run_compact_tick(shard_b.as_ref(), node_b, &members, now, BOUND_MS, BATCH);
    assert!(
        retired_a.is_empty() && retired_b.is_empty(),
        "C absent for only half the bound: retirement must defer on every node, got a={retired_a:?} b={retired_b:?}"
    );
    assert_eq!(
        value_on(&shard_a),
        Some(60),
        "no data is lost while retirement is deferred"
    );
    assert_eq!(value_on(&shard_b), Some(60));

    // -- Phase 2: bounce C under a fresh incarnation. Restart never loses
    // C's pre-restart total, and C resumes contributing under its new slot
    // immediately. --
    inc_c.store(2, Ordering::SeqCst);
    sim.bounce(host_c);
    members.insert(
        node_c,
        CrdtPresence::Present {
            since_ms: elapsed_ms(&sim),
            incarnation: 2,
        },
    );
    let writer_gamma2 = WriterId::new(node_c, 2);
    block_on(shard_c.insert(key.clone(), PnCounter::local_delta(writer_gamma2, 5))).unwrap();

    run_until(&mut sim, steps_for(Duration::from_secs(2)), || {
        value_on(&shard_a) == Some(65)
            && value_on(&shard_b) == Some(65)
            && value_on(&shard_c) == Some(65)
    })
    .expect("C's post-restart write converges on top of its untouched pre-restart total");

    // -- Phase 3: stage one runs on `retire` alone, with no quiet
    // requirement (`crdt_writer_is_retirement_eligible`'s doc: "stage one
    // ... runs unconditionally on `retire` alone"). The moment C's restart
    // makes incarnation 2 live, writer_gamma1 (incarnation 1) is
    // unambiguously dead by supersession -- no absence timer involved, so
    // there is nothing for a quiet wait to bound -- and the very next
    // compaction tick on any node retires it, independently, stage one,
    // exact, value unchanged. --
    run_steps(&mut sim, steps_for(Duration::from_millis(BOUND_MS / 2)));
    let stage_one_now = elapsed_ms(&sim);
    let (retired_a, compacted_a) = run_compact_tick(
        shard_a.as_ref(),
        node_a,
        &members,
        stage_one_now,
        BOUND_MS,
        BATCH,
    );
    assert_eq!(
        retired_a,
        vec![writer_gamma1],
        "C's pre-restart incarnation is dead by supersession the instant the restart is \
         observed, independent of any quiet wait: it is retired on the very next tick, got \
         {retired_a:?}"
    );
    assert_eq!(
        compacted_a, 1,
        "exactly the one touched record was compacted"
    );
    assert_eq!(
        value_on(&shard_a),
        Some(65),
        "stage one never changes the counter's value"
    );

    let (retired_b, _) = run_compact_tick(
        shard_b.as_ref(),
        node_b,
        &members,
        stage_one_now,
        BOUND_MS,
        BATCH,
    );
    assert_eq!(
        retired_b,
        vec![writer_gamma1],
        "B independently reaches the same retirement decision"
    );

    // Let A's and B's stage-one-compacted records replicate around, and
    // confirm the total is still exact everywhere, C included, after a
    // real anti-entropy round carried the compacted bytes.
    run_until(&mut sim, steps_for(Duration::from_secs(2)), || {
        value_on(&shard_a) == Some(65)
            && value_on(&shard_b) == Some(65)
            && value_on(&shard_c) == Some(65)
    })
    .expect("the compacted record still carries the exact total once it replicates");

    // -- Phase 4: crash C for longer than the bound this time (its own
    // absence, not merely superseded by a restart), and confirm its live
    // (incarnation-2) writer is retired the same way once quiet again. --
    sim.crash(host_c);
    members.insert(
        node_c,
        CrdtPresence::Absent {
            since_ms: elapsed_ms(&sim),
        },
    );
    run_steps(&mut sim, steps_for(Duration::from_millis(BOUND_MS - 200)));
    let now = elapsed_ms(&sim);
    let (retired_a, _) = run_compact_tick(shard_a.as_ref(), node_a, &members, now, BOUND_MS, BATCH);
    assert!(
        retired_a.is_empty(),
        "C has been absent for less than the bound: still deferred, got {retired_a:?}"
    );

    run_steps(&mut sim, steps_for(Duration::from_millis(400)));
    let stage_one_now_2 = elapsed_ms(&sim);
    let (retired_a, _) = run_compact_tick(
        shard_a.as_ref(),
        node_a,
        &members,
        stage_one_now_2,
        BOUND_MS,
        BATCH,
    );
    assert_eq!(
        retired_a,
        vec![writer_gamma2],
        "C's second, longer absence retires its (still-live-writer) incarnation-2 slot"
    );
    assert_eq!(
        value_on(&shard_a),
        Some(65),
        "the total is still exact with both of C's incarnations retired"
    );

    // -- Stage two: once writer_gamma1's retirement has aged past `2 *
    // BOUND_MS` and the cache is still quiet, its per-writer metadata
    // folds into the bounded scalar accumulator and the record shrinks,
    // even though a *second* writer (writer_gamma2) has meanwhile also been
    // retired -- the record's metadata does not keep growing with churn.
    let size_with_two_retired = block_on(shard_a.get(&key)).unwrap().encode().unwrap().len();

    run_steps(&mut sim, steps_for(Duration::from_millis(2 * BOUND_MS)));
    let stage_two_now = elapsed_ms(&sim);
    let (retired_again, compacted_again) = run_compact_tick(
        shard_a.as_ref(),
        node_a,
        &members,
        stage_two_now,
        BOUND_MS,
        BATCH,
    );
    assert!(
        retired_again.is_empty(),
        "stage two folds an already-retired writer further; it never re-retires it as new, got {retired_again:?}"
    );
    assert!(
        compacted_again > 0,
        "stage two must actually change the resident record"
    );
    let size_after_stage_two = block_on(shard_a.get(&key)).unwrap().encode().unwrap().len();
    assert!(
        size_after_stage_two < size_with_two_retired,
        "stage two drops writer_gamma1's whole per-writer retired entry in favor of the two-scalar \
         accumulator, so the record must shrink even with a second writer (writer_gamma2) also \
         retired in the meantime: with_two_retired={size_with_two_retired} \
         after_stage_two={size_after_stage_two}"
    );
    assert_eq!(
        value_on(&shard_a),
        Some(65),
        "stage two never changes the counter's value"
    );

    run_until(&mut sim, steps_for(Duration::from_secs(2)), || {
        value_on(&shard_a) == Some(65) && value_on(&shard_b) == Some(65)
    })
    .expect("the stage-two-folded record still carries the exact total once replicated");
}

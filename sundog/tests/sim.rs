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

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt as _;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use smol_str::SmolStr;
use sundog::config::ClusterConfig;
use sundog::hlc::Hlc;
use sundog::membership::Peer;
use sundog::net::{AeMismatch, AePartReply, InboundMsg, Mesh, MsgClass, RequestHandler};
use sundog::node::{NodeId, NodeName};
use sundog::store::{Mode, Shard, ShardOps};
use sundog::wire::{Msg, WireRecord};
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

fn peer_list_of(peers: &[(NodeId, &'static str, u16)]) -> Vec<Peer> {
    peers
        .iter()
        .map(|&(node, host, port)| Peer {
            node,
            name: NodeName::new(host, node),
            gossip_addr: SocketAddr::from(([0, 0, 0, 0], 0)),
            data_addr: SocketAddr::new(turmoil::lookup(host), port),
            incarnation: 1,
        })
        .collect()
}

async fn dispatch_inbound(shard: &TestShard, msg: Msg) {
    match msg {
        Msg::Invalidate { key, ver, .. } => ShardOps::invalidate(shard, key, ver).await,
        Msg::Replicate { rec, .. } => ShardOps::apply_remote(shard, rec).await,
        Msg::ReplicateBatch { recs, .. } => ShardOps::apply_remote_batch(shard, recs).await,
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
struct ShardHandler {
    shard: Arc<TestShard>,
    ae_part_min_bucket: usize,
    ae_sketch_min_bucket: usize,
}

impl ShardHandler {
    /// Both thresholds at [`ClusterConfig::default`]'s values: every
    /// scenario but the sketch/part ones want this, since their buckets
    /// never grow past either.
    fn new(shard: Arc<TestShard>) -> Self {
        let defaults = ClusterConfig::default();
        Self::with_min_buckets(
            shard,
            defaults.ae_part_min_bucket,
            defaults.ae_sketch_min_bucket,
        )
    }

    fn with_min_buckets(
        shard: Arc<TestShard>,
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

impl RequestHandler for ShardHandler {
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
async fn fan_out(shard: &TestShard, mesh: &Mesh, peers: &[NodeId], key: u32, dup_factor: usize) {
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
async fn ae_round_with_sketch(
    mesh: &Mesh,
    shard: &TestShard,
    peer: NodeId,
    bucket_listings: Option<&AtomicUsize>,
) -> bool {
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

    for mismatch in mismatched {
        match mismatch {
            AeMismatch::Bucket(bucket, peer_entries) => {
                if let Some(counter) = bucket_listings {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
                diff_bucket(shard, bucket, &peer_entries, &mut push_keys, &mut pull_keys).await;
            }
            AeMismatch::Sketch(bucket, cells) => {
                let local_entries = ShardOps::bucket_entries(shard, bucket).await;
                let mut local_sketch = sundog::Iblt::new(cells.len());
                for (key, ver) in &local_entries {
                    local_sketch.insert(xxh3_64(key), *ver);
                }
                match local_sketch
                    .subtract(&sundog::Iblt::from_cells(cells))
                    .and_then(sundog::Iblt::peel)
                {
                    Ok(decoded) => {
                        let mut hashes = Vec::new();
                        sundog::diff_decoded(&local_entries, &decoded, &mut push_keys, &mut hashes);
                        if !hashes.is_empty() {
                            pull_hashes.push((bucket, hashes));
                        }
                    }
                    Err(_) => undecodable_buckets.push(bucket),
                }
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
        }
    }

    if !wanted_parts.is_empty() {
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
                            diff_part(
                                &local_entries,
                                &peer_entries,
                                &mut push_keys,
                                &mut pull_keys,
                            );
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
                            let mut local_sketch = sundog::Iblt::new(cells.len());
                            for (key, ver) in &local_entries {
                                local_sketch.insert(xxh3_64(key), *ver);
                            }
                            match local_sketch
                                .subtract(&sundog::Iblt::from_cells(cells))
                                .and_then(sundog::Iblt::peel)
                            {
                                Ok(decoded) => {
                                    let mut hashes = Vec::new();
                                    sundog::diff_decoded(
                                        &local_entries,
                                        &decoded,
                                        &mut push_keys,
                                        &mut hashes,
                                    );
                                    if !hashes.is_empty() {
                                        pull_hashes.push((bucket, hashes));
                                    }
                                }
                                Err(_) => undecodable_buckets.push(bucket),
                            }
                        }
                    }
                }
            }
            Ok(Err(_)) | Err(_) => return false,
        }
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
                    diff_bucket(shard, bucket, &peer_entries, &mut push_keys, &mut pull_keys).await;
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

async fn diff_bucket(
    shard: &TestShard,
    bucket: u16,
    peer_entries: &[(Bytes, Hlc)],
    push_keys: &mut Vec<Bytes>,
    pull_keys: &mut Vec<Bytes>,
) {
    let peer_by_key: HashMap<Bytes, Hlc> = peer_entries.iter().cloned().collect();
    let mut local_keys = HashSet::with_capacity(peer_by_key.len());

    for (key, local_ver) in ShardOps::bucket_entries(shard, bucket).await {
        local_keys.insert(key.clone());
        match peer_by_key.get(&key) {
            Some(&peer_ver) if local_ver > peer_ver => push_keys.push(key),
            Some(&peer_ver) if local_ver < peer_ver => pull_keys.push(key),
            Some(_) => {}
            None => push_keys.push(key),
        }
    }
    for key in peer_by_key.into_keys() {
        if !local_keys.contains(&key) {
            pull_keys.push(key);
        }
    }
}

/// [`diff_bucket`]'s comparison, but over an already-fetched local entry
/// list rather than a fresh `ShardOps::bucket_entries` call: the part path's
/// counterpart, since `AePartReply::Listing`'s local side comes from one
/// batched `ShardOps::entries_for_parts` call up front.
fn diff_part(
    local_entries: &[(Bytes, Hlc)],
    peer_entries: &[(Bytes, Hlc)],
    push_keys: &mut Vec<Bytes>,
    pull_keys: &mut Vec<Bytes>,
) {
    let peer_by_key: HashMap<&Bytes, Hlc> = peer_entries.iter().map(|(k, v)| (k, *v)).collect();
    let mut local_keys = HashSet::with_capacity(local_entries.len());

    for (key, local_ver) in local_entries {
        local_keys.insert(key);
        match peer_by_key.get(key) {
            Some(&peer_ver) if *local_ver > peer_ver => push_keys.push(key.clone()),
            Some(&peer_ver) if *local_ver < peer_ver => pull_keys.push(key.clone()),
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
    let node_a = NodeId::from(1);
    let node_b = NodeId::from(2);
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
        let Ok(Ok(mut stream)) =
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
fn bucket_of_u32(key: u32) -> u16 {
    let bucket = xxh3_64(&key_bytes(key)) & (sundog::store::BUCKET_COUNT as u64 - 1);
    u16::try_from(bucket).expect("invariant: masked to BUCKET_COUNT - 1, always fits in u16")
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
    assert_eq!(
        bucket_listings_a.load(Ordering::Relaxed),
        0,
        "node-a's rounds never carried a full bucket listing"
    );
    assert_eq!(
        bucket_listings_b.load(Ordering::Relaxed),
        0,
        "node-b's rounds never carried a full bucket listing"
    );
}

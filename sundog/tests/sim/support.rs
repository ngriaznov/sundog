//! Shared simulation fixtures: the seed, the test shard builders and the step runners.

use bytes::Bytes;
use smol_str::SmolStr;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use sundog::hlc::Hlc;
use sundog::membership::Peer;
use sundog::node::{NodeId, NodeName};
use sundog::store::{KeyVersion, Mode, PartId, Shard, ShardOps};
use turmoil::Sim;
use xxhash_rust::xxh3::xxh3_64;

pub(crate) type TestShard = Shard<u32, String>;
pub(crate) type SimResult = turmoil::Result;

/// Deterministic by default. The scheduled fresh-seed CI job overrides via
/// `SUNDOG_SIM_SEED`, echoing the seed so a red run can replay it.
pub(crate) fn sim_seed(default: u64) -> u64 {
    std::env::var("SUNDOG_SIM_SEED").map_or(default, |raw| {
        let seed: u64 = raw.parse().expect("SUNDOG_SIM_SEED is a u64 turmoil seed");
        eprintln!("sim seed override: replay with SUNDOG_SIM_SEED={seed}");
        seed
    })
}

const CACHE: &str = "sim-users";
/// Simulated time per `Sim::step()`, small relative to every interval in the suite.
pub(crate) const TICK: Duration = Duration::from_millis(5);
/// Bounds every request/response network call in this harness. Turmoil's
/// `fail_rate` drops a message outright with no retransmission, so a
/// stalled read needs its own timeout; real `net::Mesh` relies on TCP's own.
pub(crate) const NET_TIMEOUT: Duration = Duration::from_millis(500);

pub(crate) fn cache_name() -> SmolStr {
    SmolStr::new(CACHE)
}

pub(crate) fn key_bytes(key: u32) -> Bytes {
    Bytes::from(postcard::to_stdvec(&key).expect("u32 always postcard-encodes"))
}

/// Runs a `ShardOps` future to completion with no ambient runtime, valid
/// since `Shard`'s async methods only await plain `tokio::sync` primitives.
pub(crate) fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    futures::executor::block_on(fut)
}

/// [`KeyVersion`]s to this suite's own `(key, version)` tuple shape, at the
/// boundary where a [`ShardOps`]/[`AeMismatch`] result meets this suite's
/// tuple-based classification helpers, reimplementing
/// `cluster::anti_entropy`'s own such boundary.
pub(crate) fn kv_to_tuples(entries: Vec<KeyVersion>) -> Vec<(Bytes, Hlc)> {
    entries.into_iter().map(|kv| (kv.key, kv.version)).collect()
}

pub(crate) fn digests_of(shard: &TestShard) -> Vec<(u16, u64)> {
    block_on(ShardOps::digests(shard))
        .into_iter()
        .map(|bd| (bd.bucket, bd.digest))
        .collect()
}

pub(crate) fn value_of(shard: &TestShard, key: u32) -> Option<String> {
    block_on(shard.get(&key))
}

pub(crate) fn new_shard(node: NodeId) -> TestShard {
    Shard::new(cache_name(), Mode::Replicated, node, 100_000, None, None)
}

/// Builds a shard pre-populated with `keys`, for state-transfer donors.
pub(crate) fn seed_shard(node: NodeId, keys: impl IntoIterator<Item = u32>) -> TestShard {
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

pub(crate) fn peer_list_of(peers: &[(NodeId, &str, u16)]) -> Vec<Peer> {
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

pub(crate) fn run_steps(sim: &mut Sim<'_>, count: usize) {
    for _ in 0..count {
        sim.step().expect("turmoil step succeeds");
    }
}

pub(crate) fn steps_for(duration: Duration) -> usize {
    let ticks = duration.as_millis() / TICK.as_millis();
    usize::try_from(ticks)
        .expect("test-scale durations fit in a usize step count")
        .max(1)
}

/// Steps `sim` until `converged` reports success or `max_steps` is spent,
/// returning the step count on success.
pub(crate) fn run_until(
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

/// The ownership part `key` hashes into.
pub(crate) fn part_of_u32(key: u32) -> PartId {
    PartId::of_key(&key_bytes(key))
}

/// Among `0..n`, every key in a bucket holding more than `min_count` of
/// them. Deterministic given a fixed key range.
pub(crate) fn dense_bucket_keys(n: u32, min_count: usize) -> Vec<u32> {
    let mut by_bucket: HashMap<u16, Vec<u32>> = HashMap::new();
    for key in 0..n {
        by_bucket.entry(bucket_of_u32(key)).or_default().push(key);
    }
    by_bucket
        .into_values()
        .find(|keys| keys.len() > min_count)
        .expect("at least one bucket exceeds min_count among this many keys")
}

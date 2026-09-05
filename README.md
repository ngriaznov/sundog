[//]: # (Badges 404 until the crate's first publish; the URLs are the real post-publish targets.)

[![CI](https://github.com/ngriaznov/sundog/actions/workflows/ci.yml/badge.svg)](https://github.com/ngriaznov/sundog/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/sundog.svg)](https://crates.io/crates/sundog)
[![docs.rs](https://img.shields.io/docsrs/sundog)](https://docs.rs/sundog)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

# sundog

sundog is an embedded, replicated cache for Rust services. Every instance on the
network finds the others, forms a cluster over gossip, and keeps named caches
coherent between them. No cache server, no coordinator, no config beyond a
cluster name. Caches run in one of three modes: invalidation, full replication,
or local-only.

It's named for the [parhelion](https://en.wikipedia.org/wiki/Sun_dog), the
optical effect where ice crystals render extra copies of the sun next to the
real one. A replicated cache, drawn by the atmosphere.

Consistency is best-effort on purpose. Gossip membership and last-write-wins
skip the cost of a consensus protocol for cache data, and anti-entropy repairs
whatever gossip's fire-and-forget delivery drops.

## Getting it

```sh
cargo add sundog
```

or in `Cargo.toml`:

```toml
[dependencies]
sundog = "0.4"
```

sundog is async, on [tokio](https://tokio.rs); the examples below assume a tokio
runtime. [Feature flags](#feature-flags) are additive: `cargo add sundog
--features tls,prometheus`.

## Quick start

```rust
use std::time::Duration;

use sundog::{Cluster, Mode};

let cluster = Cluster::builder("demo")
    .build() // mDNS discovery, ephemeral ports, sane defaults
    .await?;

let users = cluster
    .cache::<UserId, Profile>("users")
    .mode(Mode::Replicated) // or Mode::Invalidation, the default, or Mode::Local
    .max_capacity(200_000)
    .ttl(Duration::from_secs(600))
    .open()
    .await?; // triggers state transfer if the cache exists cluster-wide

users.insert(id.clone(), Profile).await?; // stamp HLC -> local apply -> fan out
let profile = users.get_or_load(&id, async |id| load_profile(id).await).await?;
users.remove(&id).await?; // tombstone write

// A cache is typed at open, so sessions get one of their own.
let sessions = cluster
    .cache::<Token, Session>("sessions")
    .mode(Mode::Replicated)
    .open()
    .await?;
sessions.insert_with_ttl(token, Session, Duration::from_secs(30)).await?; // this entry's own TTL

let mut events = users.events();
while let Ok(ev) = events.recv().await {
    // handle Event::{Created, Updated, Removed}, each tagged with its Origin
}

cluster.shutdown().await; // graceful leave
```

That's the whole API surface for the common case.
`Cluster::builder(name).build()` with nothing else chained works on a LAN; the
doctest in `sundog/src/lib.rs` runs it as the project's acceptance test. For
bulk fills, `users.insert_many(entries).await?` gives every entry its own HLC
stamp and event under one lock acquisition instead of one per entry. The rest of
the surface: `contains_key`; `keys`, a local snapshot, and `for_each_key`, the
same scan as a visitor that never materializes it all at once;
`get_or_insert_with`, an infallible `get_or_load`; `remove_many`; and `clear`.
`clear` tombstones and fans out every key this node holds. In `Replicated` mode
that empties the whole cluster once the tombstones land. `get_sync`,
`contains_key_sync`, `insert_sync`, and `remove_sync` are the same operations
without an async runtime. `users.close().await` stops its background tasks and
frees the name for a fresh `open()`; a clone kept past `close()` keeps working
as a local, detached cache.

## Should you use this?

sundog is a cache, not a database. If you need durability or strong consistency,
this isn't that tool.

Writes are last-write-wins on a hybrid logical clock. If two nodes write the
same key at nearly the same time, one write silently loses: no conflict error,
no merge, the loser vanishes.

Deletes and expiries differ. A TTL-expired entry never returns. Every record
carries its own absolute `expires_at_ms`, and once a key is past it no peer
accepts a stale copy back, partition or not. `.ttl(..)` sets a cache's default
lifespan. `insert_with_ttl` and `insert_many_with_ttl` override it per entry or
batch, and the override replicates like the default. Reads never touch expiry.

A manually removed key becomes a tombstone. It skips the `tombstone_ttl` GC
schedule while any recently known member is absent. A partitioned node can't
return with pre-delete data and resurrect the key elsewhere. That deferral caps
at `tombstone_max_ttl`, 24 hours by default. Past it the tombstone is collected
regardless of who's missing. A member gone longer can resurrect the key on
return, bounded only by its stale copy's own `expires_at_ms`, and 24+ hours
already outlives most cache TTLs. Set a TTL and deletes stay deleted; without
one, raise `tombstone_max_ttl` or treat sundog as the wrong layer.

Good fits: read-through caching in front of a slower store, session or profile
data that's fine being eventually consistent, or any per-instance cache whose
instances agree without standing up Redis. It targets small clusters of 2-30
nodes on a LAN, with no consistent-hashing or partitioning; every replicated
node holds every entry.

The store is an in-crate engine with 1,024 lock-striped tables, one per
anti-entropy bucket. A read takes one read lock and one lookup with no
allocation. A write takes one guard with nothing awaited. Bucket enumeration
time is proportional to the bucket, not the cache. 447 ns for a local read, 1.0
µs for a replicated write, on one machine.

A burst of writes, `insert_many` or back-to-back `insert` calls, fans out as
coalesced `Replicate` batches; replication throughput scales with the burst, not
per-message overhead. Frames encode and decode without copying key/value bytes.
Writes to different keys apply concurrently; same-key writes still serialize.
Anti-entropy and state-transfer requests reuse pooled connections instead of
dialing fresh every round.

Each bucket also keeps 64 part digests, the next 6 hash bits below the bucket's
own 10. A round exchanges the 1,024 bucket digests first; a mismatched bucket
answers with a full `(key, version)` listing, or, past `ae_sketch_min_bucket`
entries, an IBLT sketch decoding up to ~100 differing elements, or, past the
larger `ae_part_min_bucket`, its 64 part digests instead of either, without ever
building the listing. A mismatched part then follows the same
listing-or-sketch rule at part scale. That third tier is what keeps repairing
one changed key in a 100M-entry cache cheap: a bucket-level listing there costs
megabytes, a part digest exchange costs a few hundred bytes.

## The three modes

| Mode | Each node stores | On write | On read | Pick this when |
|---|---|---|---|---|
| `Local` | its own data, nothing shared | nothing sent | local only | you want a fast in-process cache with TTL and bounded size, and no cluster traffic at all |
| `Invalidation` (default) | its own working set | broadcasts "this key changed" | local, may be momentarily stale | the dataset is big or expensive to hold everywhere, and each node mostly cares about its own hot keys |
| `Replicated` | a full copy of everything | broadcasts the value | always local, never waits on the network | the dataset is small enough to duplicate, and you want reads to never touch the network |

`Invalidation` never sends values between nodes: a write on A tells B "your copy
of this key is stale," and B drops it or reloads it on next access. `Replicated`
alone runs state transfer on join, a new node pulling a full snapshot from an
existing peer that has finished its own, then reconciling with every other
peer once. It also keeps a background anti-entropy loop running while the
cache is open.

Every node gossips the mode of each cache it has open; opening a name under a
mode that conflicts with a live peer fails with `CacheError::ModeMismatch`. TTL
and capacity are local knobs, free to differ.

## Rolling upgrades

Every node states its wire protocol version, `sundog::wire::PROTOCOL_VERSION`,
in the hello that opens each connection and in its gossip state. A node
answers a peer only with what that peer's version understands: an older peer
never receives a message kind its release cannot decode, and a newer peer
limits itself the same way. One release step interoperates, so a cluster
upgrades one node at a time with replication and repair running throughout.
0.4 speaks protocol 2 and serves 0.3's protocol 1; a container test runs the
previous release's node against the current one in both roles.

## How nodes find each other

| Mechanism | Default? | What it does | Use it for |
|---|---|---|---|
| `Mdns` | yes | registers `_sundog._udp.local.` and browses for it continuously, via `mdns-sd` | a real LAN, office network, or anywhere multicast works |
| `Static` | no, but wins over `Mdns` if either `.seeds(..)` is called or `SUNDOG_SEEDS=host:port,host:port` is set with no other discovery configured | a fixed seed list, re-resolved periodically | tests, and anywhere mDNS can't reach |
| `DnsSrv` | no, `.discovery(DnsSrv::new(..))` | polls SRV records for a service name, falls back to A/AAAA | Kubernetes: point it at a headless service and you're done |

Discovery keeps running after startup: if the whole cluster reboots at once and
nobody remembers anybody, continuous mDNS browsing lets it find itself again. A
node with no peers isn't broken; a single-node "cluster" is a normal, healthy
state.

**The Docker gotcha:** mDNS doesn't cross the default Docker bridge network, and
usually not AP-isolated Wi-Fi either, since multicast doesn't route there. If
you're demoing this with `docker compose`, use `Static` seeds; save `Mdns` for
host networking or bare-metal LANs.

**Behind NAT or a container port mapping**, the interface address a node finds
on its own — via its outbound-interface probe, or the `if-addrs` fallback
behind it — is not always the address peers must dial: a cloud instance's
public IP while the process binds its private one, or a container's
externally published port. Set `ClusterConfig::advertise_ip` to the address
peers should use; it covers both the gossip and data-plane addresses, and no
probe runs. Under Kubernetes host networking, or any setup where the bind
address is already correct, leave it unset.

## Feature flags

| Flag | Default | What it adds |
|---|---|---|
| `tls` | off | mutual TLS on the data-plane mesh (`rustls`); set `ClusterConfig::tls` / `ClusterBuilder::tls` and every connection, including state-transfer and anti-entropy, gets wrapped |
| `prometheus` | off | a Prometheus exporter; `ClusterBuilder::prometheus_listen` serves `GET /metrics` directly, or grab a recorder via `telemetry::prometheus_handle` and mount it in your own server |
| `sim` | off | swaps the data-plane transport for `turmoil`'s, so the net layer can run inside a deterministic simulation; test-only, never enable it in a real deployment |
| `fuzzing` | off | exposes the reference model the apply-path fuzz targets drive against a real shard (`sundog::store::model`); changes no behavior |
| `spill` | off | a local SSD/NVMe spill tier; `CacheBuilder::spill(SpillConfig::new(dir, capacity_bytes))` lets eviction demote cold entries to disk instead of discarding them |

With `spill` configured, eviction writes cold entries to a FIFO ring of region
files on local disk instead of discarding them, so a cache's effective size
extends past its RAM budget; a later read promotes a spilled entry back into
RAM. Three knobs: `capacity_bytes`, the disk budget the tier stays within;
`region_bytes`, the size of each region file in the ring (64 MiB default); and
`read_concurrency`, how many spilled-value reads run at once (16 default).

sundog emits these metrics regardless of features:
`sundog_cache_hits_total{cache}`, `sundog_cache_misses_total{cache}`,
`sundog_cache_entries{cache}`, `sundog_backlog_dropped_total{peer}`,
`sundog_live_peers`, `sundog_open_caches`, `sundog_ae_sketch_total{cache,
outcome}`, and `sundog_ae_parts_total{cache, outcome}`. The first of that pair
tags anti-entropy's IBLT-sketch reconciliation on large buckets; `outcome` is
`decoded` or `fallback`. The second tags the part-digest path's per-part
reconciliation; `outcome` is `listing`, `sketch`, or `fallback`. Without
`prometheus` they fall into the `metrics` crate's no-op default recorder. A
ready-made Grafana dashboard lives at
[`ops/grafana-dashboard.json`](ops/grafana-dashboard.json).

`Cluster::is_ready()` and `Cluster::health()` report whether every open
`Mode::Replicated` cache has finished its state transfer; a `Local` or
`Invalidation` cache is warm from the moment it opens, so it never holds
readiness back. With the `prometheus` feature, the same listener that serves
`GET /metrics` also serves `GET /readyz` (200 once ready, 503 otherwise) and
`GET /healthz` (200 for as long as the process serves), for a container
orchestrator's readiness and liveness probes.

## Testing

Five layers, cheapest and highest-signal first:

1. **Property tests** run via `proptest` in `hlc`, `wire`, and `store` under
   `sundog/src`. The one that matters most, `store`'s permutation-convergence
   property, applies a random batch of writes and removes in every sampled
   order, with drops and duplicates. Every run lands on the same final state,
   the property this loss-tolerant design rests on.
2. **Deterministic simulation** runs via `turmoil` in `sundog/tests/sim.rs`,
   behind the `sim` feature. It drives the real net layer and store against a
   scripted membership feed with no sockets involved. Scenarios: partition under
   load, heal, check convergence within a bounded number of rounds; message
   loss, reordering, duplication; a donor dying mid-state-transfer; a forced
   low `ae_sketch_min_bucket` driving the IBLT sketch path itself, and a forced
   low `ae_part_min_bucket` driving the part-digest path, both under the same
   loss and reordering.
3. **Container integration** runs via
   [`rightsize`](https://crates.io/crates/rightsize) in
   `sundog/tests/containers.rs`, no Docker CLI, no `bollard`. Multi-node
   scenarios run as separate processes on a real virtual network. They cover
   three-node convergence, tombstones reaching every node, and cold joins up to
   a million entries. They also cover a killed node's gap repaired by
   anti-entropy, a dropped key repaired the same way at 500k-entry sketch
   scale, the same repair again at 1M-entry part-digest scale with its wire
   cost pinned via `netstats` under a lowered `ae_part_min_bucket`, a bulk
   fill's wire cost pinned via `netstats` against the fan-out queue
   duplicating it, high-churn add/remove/TTL workloads draining to zero, and
   64 KiB values verified byte-for-byte. Each node is `sundog-testnode`, a tiny
   static/musl binary driven over a line-based control protocol. It sits behind
   an env var, so plain `cargo test --workspace` still compiles without a
   container backend:

   ```sh
   SUNDOG_CONTAINER_TESTS=1 cargo test --release -p sundog --test containers -- --test-threads=1
   ```

   `RIGHTSIZE_BACKEND=docker` is required: sundog's gossip is UDP, and only
   rightsize's Docker backend carries it, not its lighter TCP-only microVM
   emulation. CI pulls a real base image over KVM and Docker. Locally, point
   `SUNDOG_TEST_BASE_IMAGE` at a pre-seeded image if registry pulls aren't
   available.

   - **Chaos lane**: `chaos_crashes_churn_and_drops_still_converge` runs a
     seeded random mix of crashes, churn, dropped keys, refills, and put
     bursts against a four-node cluster for `SUNDOG_CHAOS_SECS` seconds, then
     checks that every node converges to the same content. `SUNDOG_CHAOS_SEED`
     pins a run for replay; `nightly-chaos.yml` runs it for ten minutes with a
     fresh seed, logged so a red night replays with
     `SUNDOG_CONTAINER_TESTS=1 SUNDOG_CHAOS_SEED=<seed> SUNDOG_CHAOS_SECS=<secs>
     RIGHTSIZE_BACKEND=docker cargo test --release -p sundog --test containers
     -- --test-threads=1 chaos_`.

   Scenarios needing only one node, or two on loopback with real UDP membership,
   run as ordinary `#[cfg(test)]` unit tests beside the code they exercise:
   `sundog::store`'s stampede-collapse and TTL tests, `sundog::cluster`'s
   two-node replication, invalidation, state-transfer, anti-entropy, and
   local-mode tests.
4. **Coverage-guided fuzzing** runs via `sundog/fuzz`, a `cargo-fuzz` crate
   outside the workspace, nightly-only via `.github/workflows/nightly-fuzz.yml`.
   `decode_never_panics` and `decode_encode_roundtrip` throw arbitrary bytes at
   the wire decoder: it must never panic, and any frame it accepts must
   re-encode to a fixed point. `apply_model` and `apply_permutation` cover
   everything after a successful decode. `apply_model` replays generated writes,
   remote applies, invalidations, tombstone GC, and clock advances against a
   real `Shard` and its reference model, the same property
   `shard_matches_the_reference_model_under_arbitrary_op_sequences` checks
   in-crate under libFuzzer instead of proptest. `apply_permutation` runs the
   same permutation-convergence property: a duplicated, twice-shuffled record
   set must converge two shards to identical digests and entry sets.
5. **Chaos demo** runs `sundog-demo` in headless mode; see below.

Two benchmark suites sit outside these five layers, each gated on
`SUNDOG_BENCH=1` so a plain `cargo test` never pays their wall-clock cost:
`sundog/tests/replication_bench.rs` (bulk write and read latency across a live
cluster) and `sundog/tests/spill_bench.rs` (the optional SSD spill tier's
write path, RAM-hit versus tier-hit read latency, concurrent tier reads,
region reclaim, and its hit-ratio case against plain eviction). Run the spill
suite with:

```sh
SUNDOG_BENCH=1 cargo test --release -p sundog --features spill,prometheus \
    --test spill_bench -- --test-threads=1 --nocapture
```

Plain `cargo test --workspace` runs everything except the `sim` and container
suites, which need their feature/env var explicitly:

```sh
cargo test --workspace
cargo test -p sundog --features sim --test 'sim*'
SUNDOG_CONTAINER_TESTS=1 RIGHTSIZE_BACKEND=docker \
    cargo test --release -p sundog --test containers -- --test-threads=1
```

`cargo clippy --workspace --all-targets -- -D warnings -W clippy::pedantic` and
`cargo fmt --all --check` are both enforced in CI.

## Chaos demo

`sundog-demo` spins up N in-process nodes on loopback and runs a background
write load against a shared key space. It lets you kill and restart nodes
interactively, watching replication and anti-entropy repair the damage in a
`ratatui` TUI:

```sh
cargo run -p sundog-demo -- --nodes 5
```

Flags: `--cluster <NAME>`, `--key-space <N>`, `--write-interval-ms <N>`,
`--gossip-base-port <PORT>`; `--help` lists everything. In the TUI: arrow keys
or `j`/`k` to move, `1`-`9`/Enter to pick a node, `K` to kill it, `R` to restart
it, `P` to pause the write load, `q` to quit.

`--headless <SECS>` runs the same thing with no terminal, for a fixed duration,
then prints a convergence report and exits nonzero if anything diverged. That's
the soak-test rig: run it for 24h and memory stays flat. It doubles as a
CI-friendly smoke check.

## MSRV

Rust edition 2024, `rust-version = "1.97"`, resolver `3`.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT
license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this project by you, as defined in the Apache-2.0 license,
shall be dual licensed as above, without any additional terms or conditions.

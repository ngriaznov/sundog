[//]: # (Badges 404 until the crate's first publish; the URLs are the real post-publish targets.)

[![CI](https://github.com/ngriaznov/sundog/actions/workflows/ci.yml/badge.svg)](https://github.com/ngriaznov/sundog/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/sundog.svg)](https://crates.io/crates/sundog)
[![docs.rs](https://img.shields.io/docsrs/sundog)](https://docs.rs/sundog)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

# sundog

sundog is an embedded, replicated cache for Rust services. Every process on the
network finds the others, forms a cluster over gossip, and keeps named caches
coherent between them. No cache server, no coordinator, no config beyond a
cluster name. Caches run in one of four modes: invalidation, full replication,
distributed, or local-only.

It's named for the [parhelion](https://en.wikipedia.org/wiki/Sun_dog), the
optical effect where ice crystals produce extra copies of the sun next to the
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
sundog = "0.6"
```

sundog is async, on [tokio](https://tokio.rs). The examples below assume a tokio
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
`Cluster::builder(name).build()` with nothing else chained works on a LAN. The
doctest in `sundog/src/lib.rs` runs it as the project's acceptance test. For
bulk fills, `users.insert_many(entries).await?` gives every entry its own HLC
stamp and event under one lock acquisition instead of one per entry. The rest of
the surface:

- `contains_key`.
- `keys`, a local snapshot.
- `for_each_key`, the same scan as a visitor that never holds it all in
  memory at once.
- `get_or_insert_with`, an infallible `get_or_load`.
- `remove_many`.
- `clear`, which tombstones and fans out every key this node holds. In
  `Replicated` mode, that empties the whole cluster once the tombstones land.

`get_sync`, `contains_key_sync`, `insert_sync`, and `remove_sync` are the same
operations without an async runtime. `users.close().await` stops its
background tasks and frees the name for a fresh `open()`. A clone kept past
`close()` keeps working as a local, detached cache.

## Should you use this?

sundog is a cache, not a database. If you need durability or strong consistency,
this isn't that tool.

Writes are last-write-wins on a hybrid logical clock. If two nodes write the
same key at nearly the same time, one write silently loses: no conflict error,
no merge, the loser vanishes.

A merge resolver changes that for values that combine. A `ConflictResolver`
can implement `merge`, folding the stored and incoming records into a third
value, and every node converges on the same result whatever order the writes
arrive in. `sundog::crdt` ships `PnCounter` and `OrSet` with their resolvers.
`Cache::merge` writes through one without a read, and
`CacheBuilder::merge_coalesce_window` batches a writer's merges to one
applied record per key per window. A background sweep keeps their metadata
from growing forever under writer churn: a writer gone (crashed or shut
down, or superseded by a restart) longer than `ClusterConfig::crdt_retire_after` (default:
`tombstone_max_ttl`, 24h) gets retired into bounded per-writer state, and
folded away once that retirement itself has aged past a second
`crdt_retire_after` with every peer quiet. That is the same trust boundary
`tombstone_max_ttl` already accepts for a member gone that long, applied
here to a writer's own contribution rather than a whole entry.

Deletes and expiries differ. A TTL-expired entry never returns. Every record
carries its own absolute `expires_at_ms`, and once a key is past it no peer
accepts a stale copy back, partition or not. `.ttl(..)` sets a cache's default
lifespan. `insert_with_ttl` and `insert_many_with_ttl` override it per entry or
batch, and the override replicates like the default. Reads never touch expiry.

A manually removed key becomes a tombstone that skips the `tombstone_ttl` GC
schedule while any member gossip still remembers is absent. A partitioned node can't
return with pre-delete data and resurrect the key elsewhere. That deferral caps
at `tombstone_max_ttl`, 24 hours unless raised. Past it the tombstone is collected
regardless of who's missing. A member gone longer can resurrect the key on
return, bounded only by its stale copy's own `expires_at_ms`, and 24+ hours
already outlives most cache TTLs. Set a TTL and deletes stay deleted. Without
one, raise `tombstone_max_ttl` or treat sundog as the wrong layer.

Good fits: read-through caching in front of a slower store, session or profile
data that's fine being eventually consistent, or any per-instance cache whose
instances agree without standing up Redis. It targets small clusters of 2-30
nodes on a LAN, with no consistent-hashing or partitioning. Every replicated
node holds every entry.

The store is an in-crate engine with 1,024 lock-striped tables, one per
anti-entropy bucket. A read takes one read lock and one lookup with no
allocation. A write takes one guard with nothing awaited. Bucket enumeration
time is proportional to the bucket, not the cache. 447 ns for a local read, 1.0
µs for a replicated write, on one machine.

A burst of writes, `insert_many` or back-to-back `insert` calls, fans out as
coalesced `Replicate` batches. Replication throughput scales with the burst, not
per-message overhead. Frames encode and decode without copying key/value bytes.
Writes to different keys apply concurrently. Same-key writes still serialize.
Anti-entropy and state-transfer requests reuse pooled connections instead of
dialing fresh every round.

Each bucket also keeps 64 part digests, the next 6 hash bits below the bucket's
own 10. A round exchanges the 1,024 bucket digests first. A mismatched bucket
answers with a full `(key, version)` listing, or, past `ae_sketch_min_bucket`
entries, an IBLT sketch decoding up to ~100 differing elements, or, past the
larger `ae_part_min_bucket`, its 64 part digests instead of either, without ever
building the listing. A mismatched part then follows the same
listing-or-sketch rule at part scale. That third tier is what keeps repairing
one changed key in a 100M-entry cache cheap: a bucket-level listing there costs
megabytes, a part digest exchange costs a few hundred bytes.

## The four modes

| Mode | Each node stores | On write | On read | Pick this when |
|---|---|---|---|---|
| `Local` | its own data, nothing shared | nothing sent | local only | you want a fast in-process cache with TTL and bounded size, and no cluster traffic at all |
| `Invalidation` (default) | its own working set | broadcasts "this key changed" | local, may be momentarily stale | the dataset is big or expensive to hold everywhere, and each node mostly cares about its own hot keys |
| `Replicated` | a full copy of everything | broadcasts the value | always local, never waits on the network | the dataset is small enough to duplicate, and you want reads to never touch the network |
| `Distributed` | the buckets it owns, `k` live owners per key (2 by default, via `Mode::distributed()`) | forwarded to the key's owners, applied only there | `get` is local-only; `fetch` asks an owner | the dataset is too big to hold on every node, but must survive a node loss |

`Invalidation` never sends values between nodes: a write on A tells B "your copy
of this key is stale," and B drops it or reloads it on next access. `Replicated`
alone runs state transfer on join, a new node pulling a full snapshot from an
existing peer that has finished its own, then reconciling with every other
peer once. It also keeps a background anti-entropy loop running while the
cache is open.

`Distributed` splits a cache into 1,024 anti-entropy buckets
(`xxh3(key) & 1023`), each assigned to its `k` live owners by rendezvous
hashing over the peers that advertise the same cache under the same mode and
owner count. The view recomputes from gossip membership, so ownership
converges a few gossip intervals after a join or leave, not instantly. A node
that gains a bucket pulls it from the previous owners. One that loses a bucket
keeps serving it for `distributed_disown_grace_rounds` anti-entropy intervals,
then hands it to each new owner in one anti-entropy round and drops it only
once every owner has answered. Until a gained bucket's pull lands, a `fetch`
that misses it locally asks the other owners first. A write for a
bucket this node doesn't own is forwarded to that bucket's owners and never
applied locally, so nothing external needs to route it, though a local `get`
right after a forwarded write still misses, since only the owners hold it.
Every forwarded batch carries the writer's view hash. An owner whose own
view differs passes the batch on once more to the owners it knows, so a
write routed under a view that has since changed still lands on every
current owner.
`get` stays local-only everywhere, returning `None` off a non-owner. `fetch`
is the network-aware read, trying live owners in rendezvous order and
returning `Ok(None)` for a genuine miss or `CacheError::FetchUnavailable` once
every owner has timed out inside `fetch_timeout`. `owners_of` reports a key's
current owners in that same order. `owners` must be 2 or more
(`CacheError::TooFewOwners` otherwise), a finite `max_capacity` needs a
`spill` tier the same way `Replicated` does, `tti` is rejected outright, and
two peers disagreeing on `owners` for the same cache name hit
`CacheError::ModeMismatch` like any other mode conflict.

```rust
let prices = cluster
    .cache::<Sku, Price>("prices")
    .mode(Mode::distributed()) // k = 2 owners per bucket
    .open()
    .await?;

prices.insert(sku.clone(), Price(999)).await?; // forwarded if this node isn't an owner
match prices.fetch(&sku).await? {
    Some(price) => { /* found: local, or read from an owner */ }
    None => { /* a genuine miss */ }
}
```

Every node gossips the mode of each cache it has open. Opening a name under a
mode that conflicts with a live peer fails with `CacheError::ModeMismatch`. TTL
and capacity are local knobs, free to differ.

## Rolling upgrades

Every node states its wire protocol version, `sundog::wire::PROTOCOL_VERSION`,
in the hello that opens each connection and in its gossip state. A node
answers a peer only with what that peer's version understands: an older peer
never receives a message kind its release cannot decode, and a newer peer
limits itself the same way. One release step interoperates, so a cluster
upgrades one node at a time with replication and repair running throughout.
The current release speaks protocol 3 and serves protocol 2, the release
before it. A container test runs the previous release's node against the
current one in both roles. Distribution mode's message kinds (`Fetch`,
`FetchReply`, `FetchDeclined`, `AeDigestScoped`, `StBuckets`,
`StBucketChunk`, `ForwardBatch`, and `StaleView`) are gated on protocol 3: a distributed cache forms only among protocol-3
peers advertising it, and a protocol-2 peer mid-rollout is never eligible to
own a bucket and never receives one of these messages at all.

## How nodes find each other

| Mechanism | Default? | What it does | Use it for |
|---|---|---|---|
| `Mdns` | yes | registers `_sundog._udp.local.` and browses for it continuously, via `mdns-sd` | a real LAN, office network, or anywhere multicast works |
| `Static` | no, but wins over `Mdns` if either `.seeds(..)` is called or `SUNDOG_SEEDS=host:port,host:port` is set with no other discovery configured | a fixed seed list, re-resolved periodically | tests, and anywhere mDNS can't reach |
| `DnsSrv` | no, `.discovery(DnsSrv::new(..))` | polls SRV records for a service name, falls back to A/AAAA | Kubernetes: point it at a headless service and you're done |

Discovery keeps running after startup: if the whole cluster reboots at once and
nobody remembers anybody, continuous mDNS browsing lets it find itself again. A
node with no peers isn't broken. A single-node "cluster" is a normal, healthy
state.

**The Docker gotcha:** mDNS doesn't cross the default Docker bridge network,
and on most Wi-Fi networks AP isolation blocks it too, since multicast
doesn't route there. If
you're demoing this with `docker compose`, use `Static` seeds. Save `Mdns` for
host networking or bare-metal LANs.

**Behind NAT or a container port mapping**, the interface address a node finds
on its own (via its outbound-interface probe, or the `if-addrs` fallback
behind it) is not always the address peers must dial: a cloud instance's
public IP while the process binds its private one, or a container's
externally published port. Set `ClusterConfig::advertise_ip` to the address
peers should use. It covers both the gossip and data-plane addresses, and no
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

With `spill` in use, eviction writes cold entries to a FIFO ring of region
files on local disk instead of discarding them, so a cache's effective size
extends past its RAM budget. A later read promotes a spilled entry back into
RAM.

- `capacity_bytes`, the disk budget bounding the tier.
- `region_bytes`, the size of each region file in the ring (64 MiB default).
- `read_concurrency`, how many spilled-value reads run at once (16 default).
- `flush_queue_bytes`, the cap on how many queued-but-unwritten bytes a
  lagging flusher may hold in RAM before eviction falls back to an ordinary
  delete instead (one region's worth unless raised), which keeps a slow disk a
  plain-eviction problem rather than an unbounded-RSS one.

A `Replicated` cache keeps a refused victim resident, at its full weight, for
a later eviction pass to retry instead, since every peer still holds the
entry and a local delete would only have anti-entropy repair it back in. A
`Local` or `Invalidation` cache evicts it as described above.

sundog emits these metrics regardless of features:
`sundog_cache_hits_total{cache}`, `sundog_cache_misses_total{cache}`,
`sundog_cache_entries{cache}`, `sundog_backlog_dropped_total{peer}`,
`sundog_live_peers`, `sundog_open_caches`, `sundog_ae_sketch_total{cache,
outcome}`, and `sundog_ae_parts_total{cache, outcome}`. The first of that pair
tags anti-entropy's IBLT-sketch reconciliation on large buckets, where
`outcome` is `decoded` or `fallback`. The second tags the part-digest path's
per-part reconciliation, where `outcome` is `listing`, `sketch`, or
`fallback`. Without
`prometheus` they fall into the `metrics` crate's no-op default recorder.
Install the recorder before opening a cache: a cache binds its per-cache
handles when it opens. A ready-made Grafana dashboard lives at
[`ops/grafana-dashboard.json`](ops/grafana-dashboard.json).

A `Mode::Distributed` cache adds six more:

- `sundog_owned_buckets{cache}`, this node's current bucket count.
- `sundog_rebalance_buckets_total{cache, direction}`, buckets rebalance
  pulled in or released out.
- `sundog_fetch_total{cache, outcome}`, each `Cache::fetch` call's outcome
  (`local`, `remote`, `miss`, or `error`).
- `sundog_forwarded_writes_total{cache}`, writes this node forwarded to a
  bucket's owners instead of applying, or passed on because they arrived
  under another node's view.
- `sundog_stale_view_total{cache}`, anti-entropy rounds a peer declined over
  a mismatched ownership view.
- `sundog_unowned_inbound_dropped_total{cache}`, inbound records dropped for
  a bucket this node neither owns nor is mid disown-grace on.

`Cluster::is_ready()` and `Cluster::health()` report whether every open
`Mode::Replicated` cache has finished its state transfer. A `Local` or
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
   scripted membership feed with no sockets involved. Scenarios:
   - Partition under load, heal, and check convergence inside a bounded
     number of rounds.
   - Message loss, reordering, and duplication.
   - A donor dying mid-state-transfer.
   - A forced low `ae_sketch_min_bucket` driving the IBLT sketch path
     itself, and a forced low `ae_part_min_bucket` driving the part-digest
     path, both under the same loss and reordering.
   - A `Mode::Distributed` cluster churning membership under loss and
     reordering, checking that every bucket's data converges across its
     current owners with no non-owner ever holding one.
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
   64 KiB values verified byte-for-byte, and, for `Mode::Distributed`, a
   five-node fill landing every key on two owners, one owner crashing
   with every key still fetchable and then re-owned, and a fourth node
   joining a filled cluster and taking its share. Each node is
   `sundog-testnode`, a tiny
   static/musl binary driven over a line-based control protocol, and reads
   `SUNDOG_TESTNODE_MODE=distributed` (with `SUNDOG_TESTNODE_OWNERS` to pick
   `owners`, default 2) to open `"it"` as a distributed cache instead of
   `Mode::Replicated`. It sits behind an env var, so plain `cargo test
   --workspace` still compiles without a container backend:

   ```sh
   SUNDOG_CONTAINER_TESTS=1 cargo test --release -p sundog --test containers -- --test-threads=1
   ```

   Set `RIGHTSIZE_BACKEND=docker`: sundog's gossip is UDP, and only
   rightsize's Docker backend carries it, not its lighter TCP-only microVM
   emulation. CI pulls a real base image over KVM and Docker. Locally, point
   `SUNDOG_TEST_BASE_IMAGE` at a pre-seeded image if registry pulls aren't
   available.

   - **Chaos lane**: `chaos_crashes_churn_and_drops_still_converge` runs a
     seeded random mix of crashes, churn, dropped keys, refills, and put
     bursts against a four-node cluster for `SUNDOG_CHAOS_SECS` seconds, then
     checks that every node converges to the same content.
     `chaos_distributed_crashes_churn_and_drops_still_converge` runs the same
     mix against a `Mode::Distributed` cluster instead, checking that every
     bucket converges across its current owners alone. `SUNDOG_CHAOS_SEED`
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
5. **Chaos demo** runs `sundog-demo` in headless mode, described in the Chaos demo section.

Three benchmark suites sit outside these five layers, each gated on
`SUNDOG_BENCH=1` so a plain `cargo test` never pays their wall-clock cost:
`sundog/tests/replication_bench.rs` (bulk write and read latency across a live
cluster), `sundog/tests/spill_bench.rs` (the optional SSD spill tier's
write path, RAM-hit versus tier-hit read latency, concurrent tier reads,
region reclaim, and its hit-ratio case against plain eviction), and
`sundog/tests/crdt_bench.rs` (merge resolvers against last-write-wins:
concurrent counters, per-apply cost, cold join, large-entity convergence,
and the sketch path, with `SUNDOG_BENCH_KEYS` scaling the entity count). Run
the spill suite with:

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
`cargo fmt --all --check` are both enforced in CI, and every CI lane builds with
`RUSTFLAGS="-D warnings"` and `RUSTDOCFLAGS="-D warnings"`: a compiler or
rustdoc warning fails the build.

## Chaos demo

`demos/sundog-demo` spins up N in-process nodes on loopback and runs a background
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

## Distributed demo

`demos/sundog-distributed-demo` is the same shape of demo built around
`Mode::Distributed` instead: it preloads a large key set across N in-process
nodes, then runs a steady write load and random `fetch` sampling against it
while you kill and restart nodes and watch buckets move:

```sh
cargo run --release -p sundog-distributed-demo -- --nodes 5 --keys 2000000
```

It preloads `--keys` keys (`k{i}` = `v{i}`, two million unless overridden) in
batches spread round-robin across the live nodes via `Cache::insert_many`
before the write load starts, printing preload throughput and this
process's RSS when the preload finishes. Flags: `--owners <N>` (live owners per bucket,
default 2), `--cluster <NAME>`, `--write-interval-ms <N>`,
`--gossip-base-port <PORT>`; `--help` lists everything. The TUI shows a
progress bar during preload, then per node: entry count, an estimated
owned-bucket share, warmth, and restarts, plus cluster-wide fetch hit/miss/
error counts and latency. Same keys as the chaos demo: arrow keys or `j`/`k`
to move, `1`-`9`/Enter to pick a node, `K` to kill it, `R` to restart it, `P`
to pause the load, `q` to quit.

Watch entries per node settle around `owners / N` of the key count; kill a
node and watch the survivors' owned-bucket counts and entry counts climb as
they pull its buckets; restart it and watch it take its share back.

`--headless <SECS>` preloads, runs the load for `SECS` seconds (killing one
node at the midpoint and restarting it three-quarters through, to exercise a
real rebalance), then pauses it, polls the sum of live nodes' entry counts
against `owners * surviving keys` under a bound wide enough for
`distributed_disown_grace_rounds` to run out, verifies a random sample of
surviving keys against their expected value, and prints one report line
each for the preload, the fetch counters, the sample check, and
convergence, exiting nonzero on either a divergence or a failed sample.

## MSRV

Rust edition 2024, `rust-version = "1.97"`, resolver `3`.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT
license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this project by you, as defined in the Apache-2.0 license,
shall be dual licensed as above, without any additional terms or conditions.

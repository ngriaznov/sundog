<p align="center">
  <img src="https://raw.githubusercontent.com/ngriaznov/sundog/main/assets/social-card.png" alt="sundog: an embedded, replicated cache for Rust" width="720">
</p>

[![CI](https://github.com/ngriaznov/sundog/actions/workflows/ci.yml/badge.svg)](https://github.com/ngriaznov/sundog/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/sundog.svg)](https://crates.io/crates/sundog)
[![docs.rs](https://img.shields.io/docsrs/sundog)](https://docs.rs/sundog)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

# sundog

Run several copies of a service and each one keeps its own cache: one copy
updates a row, the others keep serving the old one. The usual fix is a cache
server, one more thing to deploy, size and keep alive, plus a network round
trip on every read.

sundog is an embedded, replicated cache for Rust that runs inside your
process. The instances of your service find each other over gossip, form a
cluster from a name, and keep named caches coherent. There is no cache server,
no coordinator and, on a LAN, no configuration beyond that name. A read comes
from the process's own memory, in under a microsecond.

It fits read-through caching in front of a slower store, session and profile
data that tolerates eventual consistency, and per-instance caches that should
agree without standing up Redis. It is a cache, not a database: concurrent
writes to one key settle last-write-wins, and there is no durability or strong
consistency.

Named for the [parhelion](https://en.wikipedia.org/wiki/Sun_dog), the optical
effect that puts extra copies of the sun beside the real one.

## See it run

<p align="center">
  <img src="https://raw.githubusercontent.com/ngriaznov/sundog/main/assets/sundog-lens.gif" width="720" alt="The sundog-lens terminal UI during its scripted tour: three nodes join, a fourth and fifth scale the cluster out, an explain overlay shows where one key lives and what each of the five nodes says of reading it, then one node crashes, one leaves gracefully and one restarts while the part-ownership mosaic, member table, throughput chart and event log update">
</p>

`sundog-lens` is a terminal UI that watches a running cluster from outside. It
joins gossip as an observer that is never a peer, and shows each node's status,
who owns which part of a `Distributed` cache and, given each node's Prometheus
exporter, the rates it exports. A crash and a graceful leave look different on
it, and `e` explains where a key lives and what each node says of reading it.

The tour above starts a local fleet of test nodes. It runs on Linux and macOS:

```sh
git clone https://github.com/ngriaznov/sundog && cd sundog
cargo build --release -p sundog-testnode --features prometheus
cargo run --release -p sundog-lens -- demo    # the tour

# a real cluster: one member's gossip address, and each node's exporter
cargo run --release -p sundog-lens -- watch <cluster> --seed host:7946 \
    --metrics 'http://{ip}:9090/metrics'
```

`asciinema play assets/sundog-lens.cast` replays the recording. [`demos/`](https://github.com/ngriaznov/sundog/tree/main/demos)
holds two more terminal demos that kill and restart in-process nodes under write
load: `cargo run -p sundog-demo -- --nodes 5` and `cargo run --release -p
sundog-distributed-demo -- --nodes 5` (a `Distributed` cache).

## Features

- **Zeroconf clustering.** The cluster is a name. Nodes find each other over
  mDNS on a LAN, static seeds in Docker or a VPC, and DNS SRV on Kubernetes.
- **Four modes, per cache**, [below](#modes). A `get` never waits on the network.
- **Read-through loading.** `get_or_load` runs one loader call for concurrent
  misses on a key. A [registered batch loader][loading] gathers misses into
  one query against the source, and refresh-ahead reloads a hot key before it
  expires.
- **TTLs and bounds.** A TTL per cache or per entry, `expire`, `touch` and
  `persist` on live entries, a capacity limit, a memory ceiling and an optional
  SSD spill tier (`spill` feature) for cold entries.
- **Deletes and expiry stay put.** A node returning from a partition does not
  bring back a deleted or expired key, for deletes up to a 24-hour absence by
  default ([guarantees][guarantees]).
- **Merges, not lost writes.** Counters and sets merge through `sundog::crdt`
  or your own `ConflictResolver` instead of overwriting each other.
- **Change events and explain.** `Cache::events` streams every created,
  updated and removed key with its origin. `Cache::explain` says why a read of
  a key answers what it does.
- **Production hooks.** Mutual TLS, Prometheus metrics with health probes, a
  [Grafana dashboard](https://github.com/ngriaznov/sundog/blob/main/ops/grafana-dashboard.json) and rolling upgrades.
- **Tested against divergence.** Property tests, deterministic network
  simulation, container tests, fuzzing, model checking and a weekly chaos run,
  described in [Verification](https://ngriaznov.github.io/sundog/verification.html).

## Modes

Every cache picks one of four modes, and one cluster can mix them. [Choosing a
mode][modes] has the detail.

| Mode | Each node stores | A read | Pick it when |
|---|---|---|---|
| `Local` | its own entries; nothing is sent | local | you want an in-process cache with TTL and a size bound, and no cluster traffic |
| `Invalidation` (default) | its own working set; a write drops the key on every other node | local | the dataset is too large to hold everywhere and each node mostly reads its own hot keys |
| `Replicated` | every entry | local, never waits on the network | the dataset fits on one node and reads must never touch the network |
| `Distributed` | the parts it owns, 2 owners per key by default | `get` is local, `fetch` asks an owner | the dataset is too large for one node and must survive a node loss |

## Benchmark

sundog answers a read from the calling process's memory, where a cache server
answers across a socket. On one machine, a `Local` or `Replicated` read is more
than 500 times faster than a Redis, Valkey or Dragonfly read, a write more than
100 times faster, and throughput more than 50 times higher.

| Target | Read p50 | Read p99 | Write p50 | Throughput |
|---|---:|---:|---:|---:|
| sundog, `Local` | 0.25 µs | 0.80 µs | 1.02 µs | 6.52M ops/s |
| sundog, `Replicated`, 3 nodes | 0.26 µs | 0.93 µs | 1.18 µs | 5.13M ops/s |
| sundog, `Distributed`, 3 nodes | 0.58 µs | 257 µs | 1.23 µs | 415K ops/s |
| Redis 8 | 143 µs | 527 µs | 144 µs | 96.5K ops/s |
| Valkey 8 | 137 µs | 494 µs | 136 µs | 100K ops/s |
| Dragonfly | 223 µs | 596 µs | 225 µs | 65.3K ops/s |

One run on a 4-core GitHub Actions runner: 100,000 keys with 100-byte values,
90% reads on a zipf distribution, 16 concurrent workers. sundog is called
in-process, so the servers' figures include a loopback TCP round trip that
grows across a real network; a `Distributed` node reads the keys it does not
own from an owner, which sets its p99. The servers hold each entry in less
memory: about 130 to 170 bytes against sundog's 205 to 209.

The [benchmarks page](https://ngriaznov.github.io/sundog/benchmarks.html) has
the method and the Olric and Hazelcast rows. The sundog rows need no container
backend:

```sh
cargo run --release -p sundog-bench -- --targets sundog-local,sundog-replicated,sundog-distributed
```

## Quick start

```sh
cargo add sundog && cargo add tokio --features full
```

sundog runs on [tokio](https://tokio.rs) and needs Rust 1.97 or newer. Keys and
values are any `serde` types that are `Clone`; keys are also `Hash + Eq`.

```rust,no_run
use sundog::{Cluster, Mode};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cluster = Cluster::builder("my-service").build().await?; // joins by name, over mDNS
    let users = cluster
        .cache::<u64, String>("users") // user id -> name
        .mode(Mode::Replicated)
        .open()
        .await?;

    users.insert(1, "Ada".into()).await?; // applied here, sent to every peer
    assert_eq!(users.get(&1).await.as_deref(), Some("Ada")); // read from local memory

    // On a miss, load from your database (a stub here); concurrent misses share one load.
    let name = users
        .get_or_load(&2, async |id| Ok::<_, std::io::Error>(format!("user {id}")))
        .await?;
    assert_eq!(name, "user 2");

    cluster.shutdown().await; // peers see a departure, not a failure
    Ok(())
}
```

## Learn more

The [sundog book](https://ngriaznov.github.io/sundog/) holds the detail:

- [Getting started](https://ngriaznov.github.io/sundog/getting-started.html),
  [choosing a mode][modes] and the [guarantees][guarantees] of each.
- Running it: [deployment](https://ngriaznov.github.io/sundog/deployment.html)
  (a LAN, Docker, a VPC, Kubernetes, TLS, rolling upgrades),
  [sizing](https://ngriaznov.github.io/sundog/sizing.html),
  [tuning](https://ngriaznov.github.io/sundog/tuning.html),
  [feature flags](https://ngriaznov.github.io/sundog/features.html) and the
  [operations runbook](https://ngriaznov.github.io/sundog/operations.html).
- Recipes: [an axum HTTP cache](https://ngriaznov.github.io/sundog/recipes/http-cache.html),
  [read-through over sqlx][loading], with `get_or_load` and registered loaders,
  and [a session store](https://ngriaznov.github.io/sundog/recipes/sessions.html).
- The [API reference](https://docs.rs/sundog) on docs.rs covers every type and
  method.

## License

Licensed under either of [Apache License, Version 2.0](https://github.com/ngriaznov/sundog/blob/main/LICENSE-APACHE) or [MIT
license](https://github.com/ngriaznov/sundog/blob/main/LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this project by you, as defined in the Apache-2.0 license,
shall be dual licensed as above, without any additional terms or conditions.

[modes]: https://ngriaznov.github.io/sundog/modes.html
[guarantees]: https://ngriaznov.github.io/sundog/guarantees.html
[loading]: https://ngriaznov.github.io/sundog/recipes/read-through.html

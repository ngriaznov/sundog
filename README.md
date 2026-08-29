[//]: # (Badges 404 until the crate's first publish; the URLs are the real post-publish targets.)

[![CI](https://github.com/ngriaznov/sundog/actions/workflows/ci.yml/badge.svg)](https://github.com/ngriaznov/sundog/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/sundog.svg)](https://crates.io/crates/sundog)
[![docs.rs](https://img.shields.io/docsrs/sundog)](https://docs.rs/sundog)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

# sundog

sundog is an embedded, replicated cache for Rust services. Drop it into a
service, and every instance of that service on the network finds the others,
forms a cluster over gossip, and keeps named caches coherent between them —
no separate cache server, no config beyond a cluster name. It's modeled on
[Infinispan](https://infinispan.org/)'s embedded mode: same two clustered
cache shapes (invalidation and full replication), same "every node is a
peer, nobody's special" architecture, just built for Rust instead of the
JVM.

It's named for the [parhelion](https://en.wikipedia.org/wiki/Sun_dog) — the
optical effect where ice crystals in the sky render extra copies of the sun
next to the real one. A replicated cache, drawn by the atmosphere.

For the full design rationale — why gossip instead of a consensus protocol,
why LWW, why anti-entropy exists — see [`docs/BUILD_PLAN.md`](docs/BUILD_PLAN.md).
Things we deliberately didn't build, and what would make us reconsider, are
in [`ROADMAP.md`](ROADMAP.md).

## Quick start

```rust
use std::time::Duration;

use sundog::{Cluster, Mode};

let cluster = Cluster::builder("demo")
    .build() // mDNS discovery, ephemeral ports, sane defaults
    .await?;

let users = cluster
    .cache::<UserId, Profile>("users")
    .mode(Mode::Replicated) // or Mode::Invalidation (default), Mode::Local
    .max_capacity(200_000)
    .ttl(Duration::from_secs(600))
    .open()
    .await?; // triggers state transfer if the cache exists cluster-wide

users.insert(id.clone(), Profile).await?; // stamp HLC -> local apply -> fan out
let profile = users.get_or_load(&id, async |id| load_profile(id).await).await?;
users.remove(&id).await?; // tombstone write

let mut events = users.events();
while let Ok(ev) = events.recv().await {
    // handle Event::{Created, Updated, Removed}, each tagged with its Origin
}

cluster.shutdown().await; // graceful leave (chitchat departs politely)
```

That's the whole API surface for the common case. `Cluster::builder(name).build()`
with nothing else chained is supposed to just work on a LAN — it's the
project's actual acceptance test (see the doctest in `sundog/src/lib.rs`,
which `cargo test --doc` runs for real). Filling a cache in bulk — a cold
load from a backing store, say — can use `users.insert_many(entries).await?`
instead of a loop of `insert` calls: every entry still gets its own HLC
stamp and its own event, just applied under one acquisition of the store's
lock rather than one per entry.

## Should you use this?

sundog is a cache, not a database. If you need durability, strong
consistency, or a guarantee that a deleted key stays deleted, this isn't
that tool.

Concretely: writes are last-write-wins on a hybrid logical clock, so if two
nodes write the same key at close to the same time, one write silently
loses — there's no conflict error, no merge, the loser just vanishes.
Deletes are tombstones, and tombstones expire (`tombstone_ttl`, 10 minutes
by default). A node that's been partitioned away longer than that can come
back with a stale copy of a key everyone else deleted, and that key comes
back to life. For a cache — where the entry re-expires or gets overwritten
on the next real write — that's a shrug. If a deleted key resurrecting is
something your application can't tolerate, sundog is the wrong layer for
that data.

Where it's a good fit: read-through caching in front of a slower backing
store, session or profile data that's fine being eventually consistent,
anywhere you're currently running a per-instance in-memory cache and wish
the instances agreed with each other without standing up Redis. It targets
small clusters (2–30 nodes) and LAN latencies — it is not trying to be a
distributed database, and there's no consistent-hashing / partitioning mode
(every replicated node holds every entry; see `ROADMAP.md` for why, and when
that might change).

A burst of writes — `insert_many`, or plain back-to-back `insert` calls —
fans out over the wire as coalesced `Replicate` batches rather than one
frame per key, so replication throughput scales with the burst instead of
the per-message overhead.

## The three modes

| Mode | Each node stores | On write | On read | Pick this when |
|---|---|---|---|---|
| `Local` | its own data, nothing shared | nothing sent | local only | you just want `moka` with less setup — no cluster traffic at all |
| `Invalidation` (default) | its own working set | broadcasts "this key changed" | local, may be momentarily stale | the dataset is big or expensive to hold everywhere, and each node mostly cares about its own hot keys |
| `Replicated` | a full copy of everything | broadcasts the value | always local, never waits on the network | the dataset is small enough to duplicate, and you want reads to never touch the network |

`Invalidation` never sends values between nodes — a write on A tells B "your
copy of this key is stale," and B either drops it or reloads it on next
access; B never gets A's value directly. `Replicated` is the only mode that
runs state transfer on join (a new node pulls a full snapshot from an
existing peer) and keeps a background anti-entropy loop running for as long
as the cache is open.

## How nodes find each other

| Mechanism | Default? | What it does | Use it for |
|---|---|---|---|
| `Mdns` | yes | registers `_sundog._udp.local.` and browses for it continuously, via `mdns-sd` | a real LAN, office network, or anywhere multicast works |
| `Static` | no — `.seeds(..)` or `SUNDOG_SEEDS=host:port,host:port` | a fixed seed list, re-resolved periodically | tests, and anywhere mDNS can't reach |
| `DnsSrv` | no — `.discovery(DnsSrv::new(..))` | polls SRV records for a service name, falls back to A/AAAA | Kubernetes — point it at a headless service and you're done |

Discovery keeps running after startup, not just once — if the whole cluster
reboots at the same time and nobody remembers anybody, continuous mDNS
browsing is what lets it find itself again. A node that finds no peers isn't
broken; a single-node "cluster" is a normal, healthy state.

**The Docker gotcha:** mDNS doesn't cross the default Docker bridge network
(and usually not AP-isolated Wi-Fi either — multicast just doesn't route
there). If you're demoing this with `docker compose`, use `Static` seeds;
save `Mdns` for host networking or bare-metal LANs.

## Feature flags

| Flag | Default | What it adds |
|---|---|---|
| `tls` | off | mutual TLS on the data-plane mesh (`rustls`) — set `ClusterConfig::tls` / `ClusterBuilder::tls` and every connection, including state-transfer and anti-entropy, gets wrapped |
| `prometheus` | off | a Prometheus exporter — `ClusterBuilder::prometheus_listen` serves `GET /metrics` directly, or grab a recorder via `telemetry::prometheus_handle` and mount it in your own server |
| `sim` | off | swaps the data-plane transport for `turmoil`'s, so the net layer can run inside a deterministic simulation — this is a test-only knob, never turn it on in a real deployment |

Metrics (`sundog_backlog_dropped_total{peer}`, `sundog_live_peers`,
`sundog_open_caches`, and a few more) are emitted unconditionally regardless
of features — without `prometheus`, they just fall into the `metrics`
crate's no-op default recorder instead of going anywhere. A ready-made
Grafana dashboard for them lives at
[`ops/grafana-dashboard.json`](ops/grafana-dashboard.json).

## Testing

Four layers, cheapest and highest-signal first:

1. **Property tests** (`proptest`, alongside `hlc`, `wire`, and `store` in
   `sundog/src`). The one that matters most is `store`'s
   permutation-convergence property: take a random batch of writes and
   removes, apply them in every sampled order, with drops and duplicates
   thrown in, and check every run lands on the same final state. That
   property is the whole reason it's safe for this design to drop messages
   and repair later instead of guaranteeing delivery.
2. **Deterministic simulation** (`turmoil`, `sundog/tests/sim.rs`, behind
   the `sim` feature). Drives the real net layer and store against a
   scripted membership feed with no actual sockets involved — partition
   under load, heal, check convergence within a bounded number of rounds;
   message loss, reordering, duplication; a donor dying mid-state-transfer.
3. **Container integration** (`sundog/tests/containers.rs`, via
   [`rightsize`](https://crates.io/crates/rightsize) — no Docker CLI, no
   `bollard`, that's a hard rule in this repo). This is where multi-node
   scenarios run as actual separate processes on a real virtual network:
   three-node convergence, tombstones reaching every node, a cold node
   warm-joining a populated cluster via state transfer, killing a node and
   watching anti-entropy repair the gap when it comes back. Each node is a
   tiny binary, `sundog-testnode`, built static/musl and driven over a
   line-based control protocol. It's gated behind an env var rather than
   `#[ignore]`, so a plain `cargo test --workspace` still compiles and
   passes this file trivially without a container backend:

   ```sh
   SUNDOG_CONTAINER_TESTS=1 cargo test --release -p sundog --test containers -- --test-threads=1
   ```

   You'll also need `RIGHTSIZE_BACKEND=docker` — sundog's gossip is UDP, and
   rightsize's lightweight microVM network emulation only relays TCP, so the
   Docker backend is the one that actually carries chitchat's traffic. CI
   has both KVM and Docker and pulls a real base image; locally, if registry
   pulls aren't available, point `SUNDOG_TEST_BASE_IMAGE` at whatever
   minimal image you've got pre-seeded.

   Scenarios that genuinely only need one node, or two nodes on loopback
   with real UDP membership, don't live in this file — they're ordinary
   `#[cfg(test)]` unit tests next to the code they exercise (see
   `sundog::store`'s stampede-collapse and TTL tests, `sundog::cluster`'s
   two-node replication/invalidation/state-transfer/anti-entropy/local-mode
   tests). Container tests are reserved for what actually requires separate
   processes and a real network.
4. **Chaos demo** (`sundog-demo`, headless mode) — see below.

Plain `cargo test --workspace` runs everything except the `sim` and
container suites, which need their feature/env var explicitly:

```sh
cargo test --workspace
cargo test -p sundog --features sim --test 'sim*'
SUNDOG_CONTAINER_TESTS=1 RIGHTSIZE_BACKEND=docker \
    cargo test --release -p sundog --test containers -- --test-threads=1
```

`cargo clippy --workspace --all-targets -- -D warnings -W clippy::pedantic`
and `cargo fmt --all --check` are both enforced in CI.

## Chaos demo

`sundog-demo` spins up N in-process nodes on loopback, runs a background
write load against a shared key space, and lets you kill/restart nodes
interactively while watching replication and anti-entropy repair the
damage, live, in a `ratatui` TUI:

```sh
cargo run -p sundog-demo -- --nodes 5
```

Flags: `--cluster <NAME>`, `--key-space <N>`, `--write-interval-ms <N>`,
`--gossip-base-port <PORT>`; `--help` lists everything. In the TUI: arrow
keys or `j`/`k` to move, `1`–`9`/Enter to pick a node, `K` to kill it, `R` to
restart it, `P` to pause the write load, `q` to quit.

`--headless <SECS>` runs the same thing with no terminal, for a fixed
duration, then prints a convergence report and exits nonzero if anything
diverged — that's the soak-test rig (run it for 24h, memory should stay
flat) and it doubles as a CI-friendly smoke check.

## MSRV

Rust edition 2024, `rust-version = "1.97"`, resolver `3`.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in this project by you, as defined in the
Apache-2.0 license, shall be dual licensed as above, without any additional
terms or conditions.

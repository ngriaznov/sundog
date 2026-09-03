[//]: # (Badges 404 until the crate's first publish; the URLs are the real post-publish targets.)

[![CI](https://github.com/ngriaznov/sundog/actions/workflows/ci.yml/badge.svg)](https://github.com/ngriaznov/sundog/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/sundog.svg)](https://crates.io/crates/sundog)
[![docs.rs](https://img.shields.io/docsrs/sundog)](https://docs.rs/sundog)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

# sundog

sundog is an embedded, replicated cache for Rust services. Drop it into a
service, and every instance on the network finds the others, forms a
cluster over gossip, and keeps named caches coherent between them — no
cache server, no coordinator, no config beyond a cluster name. Caches run
in one of three modes: invalidation, full replication, or local-only.

It's named for the [parhelion](https://en.wikipedia.org/wiki/Sun_dog) — the
optical effect where ice crystals render extra copies of the sun next to
the real one. A replicated cache, drawn by the atmosphere.

Consistency is best-effort on purpose: gossip membership and last-write-wins
skip the cost of running a consensus protocol for cache data, and
anti-entropy repairs whatever gossip's fire-and-forget delivery drops.

## Getting it

```sh
cargo add sundog
```

or in `Cargo.toml`:

```toml
[dependencies]
sundog = "0.2"
```

sundog is async and runs on [tokio](https://tokio.rs) — the examples below
assume a tokio runtime. The optional [feature flags](#feature-flags) are
additive: `cargo add sundog --features tls,prometheus`.

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
users.insert_with_ttl(token, Session, Duration::from_secs(30)).await?; // this entry's own TTL
let profile = users.get_or_load(&id, async |id| load_profile(id).await).await?;
users.remove(&id).await?; // tombstone write

let mut events = users.events();
while let Ok(ev) = events.recv().await {
    // handle Event::{Created, Updated, Removed}, each tagged with its Origin
}

cluster.shutdown().await; // graceful leave (chitchat departs politely)
```

That's the whole API surface for the common case.
`Cluster::builder(name).build()` with nothing else chained works on a LAN —
that's the project's acceptance test, run for real by the doctest in
`sundog/src/lib.rs`. For bulk fills — a cold load from a backing store —
use `users.insert_many(entries).await?`: every entry still gets its own HLC
stamp and its own event, applied under one lock acquisition instead of one
per entry. The rest of the surface: `contains_key`, `keys` (a local
snapshot), `get_or_insert_with` (an infallible `get_or_load`),
`remove_many`, and `clear` — which tombstones every key this node holds
and fans the tombstones out, so in `Replicated` mode it empties the whole
cluster once they land.

## Should you use this?

sundog is a cache, not a database. If you need durability or strong
consistency, this isn't that tool.

Writes are last-write-wins on a hybrid logical clock: if two nodes write
the same key at nearly the same time, one write silently loses — no
conflict error, no merge, the loser vanishes.

Deletes and expiries behave differently. A TTL-expired entry can never
reappear anywhere: every record carries its own absolute `expires_at_ms`,
so once a key is past that timestamp no peer will accept a stale copy of it
back in, partition or not. The cache's `.ttl(..)` is the default lifespan;
`insert_with_ttl` and `insert_many_with_ttl` give one entry (or one batch)
its own, longer or shorter, and work on a cache with no default at all —
the per-entry deadline replicates exactly like the default one. Reads never
touch expiry. A manually
removed key is a tombstone, and tombstones are kept — not GC'd on the usual
`tombstone_ttl` schedule — for as long as any recently known member is
absent, so a partitioned node can't come back with a pre-delete copy and
resurrect the key on the nodes that stayed up. The deferral is bounded:
past `tombstone_max_ttl` (24 hours by default) the tombstone is collected
regardless of who's still missing. That leaves one hole: a member gone
longer than `tombstone_max_ttl` can return carrying pre-delete data and
resurrect the key. Even then a cache TTL caps the damage — the stale copy
keeps its original `expires_at_ms` and can only reappear inside its own
lifetime, and a node gone 24+ hours has outlived any typical cache TTL many
times over. In practice: set a TTL and deleted keys stay deleted; without
one, raise `tombstone_max_ttl` or treat sundog as the wrong layer for that
key.

Where it's a good fit: read-through caching in front of a slower backing
store, session or profile data that's fine being eventually consistent,
anywhere you're currently running a per-instance in-memory cache and wish
the instances agreed with each other without standing up Redis. It targets
small clusters (2–30 nodes) and LAN latencies. There's no
consistent-hashing / partitioning mode — every replicated node holds every
entry.

A burst of writes — `insert_many` or back-to-back `insert` calls — fans out
as coalesced `Replicate` batches rather than one frame per key, so
replication throughput scales with the burst instead of the per-message
overhead. Record-carrying frames encode and decode without copying
key/value bytes, writes to different keys apply concurrently (only same-key
writes serialize against each other), and anti-entropy/state-transfer
requests reuse pooled connections instead of dialing fresh every round.

## The three modes

| Mode | Each node stores | On write | On read | Pick this when |
|---|---|---|---|---|
| `Local` | its own data, nothing shared | nothing sent | local only | you want a fast in-process cache with TTL and bounded size, and no cluster traffic at all |
| `Invalidation` (default) | its own working set | broadcasts "this key changed" | local, may be momentarily stale | the dataset is big or expensive to hold everywhere, and each node mostly cares about its own hot keys |
| `Replicated` | a full copy of everything | broadcasts the value | always local, never waits on the network | the dataset is small enough to duplicate, and you want reads to never touch the network |

`Invalidation` never sends values between nodes — a write on A tells B "your
copy of this key is stale," and B either drops it or reloads it on next
access. `Replicated` is the only mode that runs state transfer on join (a
new node pulls a full snapshot from an existing peer) and keeps a
background anti-entropy loop running for as long as the cache is open.

Every node gossips the mode of each cache it has open. Opening a name
under a different mode than a live peer already runs it in fails with
`CacheError::ModeMismatch` — two nodes can't quietly disagree about
whether `"users"` is replicated or invalidated. TTL and capacity are local
knobs and are free to differ.

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
(and usually not AP-isolated Wi-Fi either — multicast doesn't route
there). If you're demoing this with `docker compose`, use `Static` seeds;
save `Mdns` for host networking or bare-metal LANs.

## Feature flags

| Flag | Default | What it adds |
|---|---|---|
| `tls` | off | mutual TLS on the data-plane mesh (`rustls`) — set `ClusterConfig::tls` / `ClusterBuilder::tls` and every connection, including state-transfer and anti-entropy, gets wrapped |
| `prometheus` | off | a Prometheus exporter — `ClusterBuilder::prometheus_listen` serves `GET /metrics` directly, or grab a recorder via `telemetry::prometheus_handle` and mount it in your own server |
| `sim` | off | swaps the data-plane transport for `turmoil`'s, so the net layer can run inside a deterministic simulation — test-only, never enable it in a real deployment |

Metrics (`sundog_cache_hits_total{cache}` / `sundog_cache_misses_total{cache}`,
`sundog_cache_entries{cache}`, `sundog_backlog_dropped_total{peer}`,
`sundog_live_peers`, `sundog_open_caches`, `sundog_ae_sketch_total{cache,
outcome}` — anti-entropy's IBLT-sketch reconciliation for large buckets,
`outcome` either `decoded` or `fallback` — and a few more) are emitted
regardless of features —
without `prometheus`, they fall into the `metrics` crate's no-op default
recorder instead of going anywhere. A ready-made Grafana dashboard for them
lives at [`ops/grafana-dashboard.json`](ops/grafana-dashboard.json).

## Testing

Five layers, cheapest and highest-signal first:

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
   `bollard`). This is where multi-node scenarios run as separate processes
   on a real virtual network: three-node convergence, tombstones reaching
   every node, cold joins into populated clusters at up to a million
   entries, killing a node and watching anti-entropy repair the gap when it
   comes back, high-churn add/remove/TTL workloads that must drain to zero,
   and 64 KiB values verified byte-for-byte on arrival. Each node is a tiny
   binary, `sundog-testnode`, built static/musl and driven over a
   line-based control protocol. It's gated behind an env var rather than
   `#[ignore]`, so a plain `cargo test --workspace` still compiles and
   passes this file trivially without a container backend:

   ```sh
   SUNDOG_CONTAINER_TESTS=1 cargo test --release -p sundog --test containers -- --test-threads=1
   ```

   You'll also need `RIGHTSIZE_BACKEND=docker` — sundog's gossip is UDP,
   rightsize's lightweight microVM network emulation only relays TCP, and
   only the Docker backend carries chitchat's traffic. CI
   has both KVM and Docker and pulls a real base image; locally, if registry
   pulls aren't available, point `SUNDOG_TEST_BASE_IMAGE` at whatever
   minimal image you've got pre-seeded.

   Scenarios that only need one node, or two nodes on loopback
   with real UDP membership, are ordinary `#[cfg(test)]` unit tests next to
   the code they exercise — `sundog::store`'s stampede-collapse and TTL
   tests, `sundog::cluster`'s two-node
   replication/invalidation/state-transfer/anti-entropy/local-mode tests.
4. **Coverage-guided fuzzing** (`sundog/fuzz`, a `cargo-fuzz` crate outside
   the workspace; nightly-only, run in CI by `.github/workflows/nightly-fuzz.yml`).
   `decode_never_panics` and `decode_encode_roundtrip` throw arbitrary bytes
   at the wire decoder — it must never panic, and any frame it accepts must
   re-encode to a fixed point. `apply_model` and `apply_permutation` cover
   what those two leave untouched: everything *after* a successful decode.
   `apply_model` replays an `Arbitrary`-generated sequence of local writes,
   remote applies and batches, invalidations, tombstone GC, and clock
   advances against a real `Shard` and a from-scratch reference model
   (`sundog::store::model`, `#[doc(hidden)]`) side by side, asserting after
   every op that reads, digests, and the entry set agree — the same
   semantics the in-crate `shard_matches_the_reference_model_under_arbitrary_op_sequences`
   property test checks, sampled by libFuzzer's coverage guidance instead
   of proptest. `apply_permutation` is the permutation-convergence property
   (`store`'s property tests, above) under that same coverage guidance:
   an arbitrary record set, duplicated and shuffled two different ways,
   must converge two independent shards to identical digests and entry
   sets.
5. **Chaos demo** (`sundog-demo`, headless mode) — see below.

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

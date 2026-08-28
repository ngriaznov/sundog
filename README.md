[//]: # (Badges 404 until the crate's first publish; the URLs are the real post-publish targets.)

[![CI](https://github.com/ngriaznov/sundog/actions/workflows/ci.yml/badge.svg)](https://github.com/ngriaznov/sundog/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/sundog.svg)](https://crates.io/crates/sundog)
[![docs.rs](https://img.shields.io/docsrs/sundog)](https://docs.rs/sundog)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

# sundog

sundog is an embedded, replicated, zeroconf cache for Rust, modeled on
[Infinispan](https://infinispan.org/)'s embedded mode. Instances of a service
on the same network discover each other, form a cluster over gossip
membership ([chitchat](https://github.com/quickwit-oss/chitchat)), and keep
named caches coherent across nodes — either invalidation (every node caches
independently; writes broadcast an invalidate) or full replication (every
node holds every entry; writes broadcast the value). Consistency is AP:
last-write-wins on a hybrid logical clock, healed by anti-entropy, with no
consensus and no operator action on join, leave, crash, or partition. It is
named for the [parhelion](https://en.wikipedia.org/wiki/Sun_dog) — the sky
rendering simultaneous copies of the sun in ice crystals, a replicated cache
drawn by the atmosphere.

Design rationale and every implementation decision are in
[`docs/BUILD_PLAN.md`](docs/BUILD_PLAN.md); binding coding conventions are in
[`docs/HOUSE_RULES.md`](docs/HOUSE_RULES.md). What v1 deliberately excludes,
and when to revisit each cut, is in [`ROADMAP.md`](ROADMAP.md).

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

`Cluster::builder(name).build()` with no further calls must form a working
LAN cluster — this snippet, verbatim, is the project's acceptance test (see
the crate-level doc example in `sundog/src/lib.rs`, which is compiled by
`cargo test --doc`).

## Modes

| Mode | Local storage | On write | On read | Use it for |
|---|---|---|---|---|
| `Local` | yes | nothing broadcast | local only | a plain per-node cache with no cluster traffic |
| `Invalidation` (default) | independent per node | broadcasts `Invalidate { key, ver }` | local; may be stale until invalidated | large or expensive-to-hold datasets where each node only needs its own working set kept honest |
| `Replicated` | full copy per node | broadcasts the value (`Replicate { rec }`) | always local, never blocks on the network | smaller datasets where every node should answer reads without a network hop; the only mode that runs state transfer and anti-entropy |

`Invalidation` mode never replicates values between nodes — each node warms
its own copy via `get_or_load`, and other nodes' writes only ever evict, never
populate, a peer's local entry. `Replicated` mode is the only one where
`open()` runs state transfer against an existing donor and then keeps a
background anti-entropy scheduler running for the cache's lifetime.

## Consistency model

sundog is **AP, always** — both sides of a partition keep reading and
writing; there is no quorum, no leader, no write path that blocks on
membership. Correctness of concurrent, out-of-order, and re-delivered writes
comes from one invariant: every write is stamped once, at its origin, with a
hybrid logical clock version `(wall_ms, logical, node_id)`, and applied to
the store iff that version is greater than what is already stored. That
single rule makes applying any set of records — local, replicated,
state-transfer chunk, or anti-entropy repair — in any order, any number of
times, converge to the same state, which is what lets the data plane drop
messages under load and rely on **anti-entropy** (a periodic, jittered,
digest-diff round against a random peer) to repair the gap instead of
guaranteeing delivery.

Deletes are writes too: removing a key stores a tombstone rather than erasing
it, so a stale replicated `insert` can't resurrect something that was
deleted more recently. Tombstones are garbage-collected after
`tombstone_ttl` (default 10 minutes, enforced to be at least
3 × `ae_interval` so a lagging peer gets a few anti-entropy rounds to see the
deletion first). **The dragon:** a node partitioned away for longer than
`tombstone_ttl` can resurrect an entry it holds that was deleted everywhere
else while it was gone. For a cache this is accepted rather than solved — the
entry re-expires or gets re-invalidated on the next real write — but if a key
must never come back from the dead, it does not belong in a cache.

Reads can observe values that are stale by up to one replication hop in the
normal case, or up to a partition's duration plus one anti-entropy round in
the worst case. TTL (`.ttl(..)`) is replicated as an absolute
`expires_at_ms` computed at the origin node, so **expiry trusts wall-clock
sync across the cluster to within a few seconds (NTP)** — HLC's logical
component absorbs clock skew for write *ordering*, but not for this. TTI
(`.tti(..)`, max-idle) is deliberately local-only and never replicated —
Infinispan's cluster-wide max-idle needs touch-propagation chatter and still
has documented anomalies; sundog skips it on purpose (see `ROADMAP.md`).

## Discovery

| Mechanism | Default? | How it works | Use it when |
|---|---|---|---|
| `Mdns` | yes | registers `_sundog._udp.local.` (cluster name as a TXT property, instance = node id) and browses continuously via `mdns-sd` | a real LAN, office network, or edge deployment with multicast |
| `Static` | no — `.seeds(..)` or `SUNDOG_SEEDS=host:port,host:port` | a fixed, periodically re-resolved seed list; each entry may be a hostname | tests, and anywhere mDNS can't reach |
| `DnsSrv` | no — `.discovery(DnsSrv::new(..))` | polls SRV records for a headless service name on an interval, falling back to A/AAAA | Kubernetes: point it at a headless service, "zeroconf" there is the one line of config you already have |

Discovery is a continuous stream, not a one-shot lookup at boot, by design:
a fully restarted cluster (nobody remembers anybody) still re-forms because
every node keeps browsing/polling. mDNS not finding anybody — a container
with no multicast, or a LAN of exactly one node — is a healthy single-node
cluster, not an error.

**Docker caveat:** mDNS does not cross the default Docker bridge network (nor
most AP-isolated Wi-Fi). Compose-based demos and CI use `Static`;
host-network deployments can use the zeroconf `Mdns` default.

## Feature flags

| Flag | Default | Adds | Status |
|---|---|---|---|
| `prometheus` | off | a Prometheus exporter (`metrics-exporter-prometheus`): `ClusterBuilder::prometheus_listen` serves `GET /metrics`, or `telemetry::prometheus_handle` installs a recorder into an HTTP server you already run | implemented |
| `sim` | off | swaps the data plane's transport seam (`net::tcp`) from `tokio::net` to `turmoil::net`, so `net::Mesh` can be driven inside a deterministic `turmoil` simulation | implemented; used by `tests/sim.rs` only, never enable it in a real deployment |
| `tls` | off | pulls in `rustls`/`tokio-rustls` as dependencies for a future mTLS data plane (pre-shared cert config) | **declared, not yet wired** — no code path in `net` currently uses it; see `ROADMAP.md` |

Metric *emission* (`sundog_backlog_dropped_total{peer}`, `sundog_live_peers`,
`sundog_open_caches`, and friends) happens unconditionally throughout the
crate regardless of features — without `prometheus`, those calls simply fall
through to the `metrics` crate's own no-op default recorder.

## Chaos demo

`sundog-demo` spawns N in-process nodes over static loopback seeds, drives a
background write load across a shared key space, and lets you kill/restart
individual nodes interactively to watch replication and anti-entropy repair
the resulting divergence live, in a `ratatui` TUI:

```sh
cargo run -p sundog-demo -- --nodes 5
```

Useful flags: `--cluster <NAME>`, `--key-space <N>`, `--write-interval-ms
<N>`, `--gossip-base-port <PORT>`; `--help` lists all of them. In the TUI:
arrow keys or `j`/`k` to move, `1`–`9`/Enter to select a node, `K` to kill
it, `R` to restart it, `P` to pause/resume the write load, `q` to quit.

`--headless <SECS>` runs the same load with no terminal for a fixed
duration, then prints a convergence report and exits nonzero on divergence —
this is the manual soak-test rig (run it for 24h; memory should stay flat
and digests should converge) and doubles as a CI-friendly smoke check.

## Test suite

Four layers, cheapest and highest-value first (`docs/BUILD_PLAN.md` §11):

1. **Property tests** (`proptest`, in-module `prop_tests` submodules under
   `hlc`, `wire`, `store`) on the pure core. The highest-value single test in
   the project is `store`'s permutation-convergence property: apply the same
   multiset of versioned writes/removes, in every sampled order, with
   duplication and drops, and assert the final state is byte-identical —
   that property is the license for the whole loss-tolerant design.
2. **Deterministic simulation** (`turmoil`, `sundog/tests/sim.rs`, behind the
   `sim` feature) drives the real `net::Mesh` and `store::Shard` against a
   scripted membership feed with no real UDP/TCP: partition-during-load then
   heal and assert bounded-round convergence, message loss/reorder/dup
   storms, donor crash mid-state-transfer.
3. **In-process integration** (`sundog/tests/*.rs`, plain `#[tokio::test]`):
   real `Cluster`s wired together with `Static` discovery over loopback,
   chitchat running real UDP — invalidation/replication visibility,
   state-transfer warm join, anti-entropy repair, TTL expiry, kill/restart,
   `get_or_load` stampede collapse, local-mode isolation.
4. **Chaos demo** (`sundog-demo`, headless mode) as a soak/smoke rig — see
   above.

Run everything with `cargo test --workspace`; the simulation suite needs the
feature explicitly: `cargo test -p sundog --features sim --test 'sim*'`
(this is what CI's second test job runs). `cargo clippy --workspace
--all-targets -- -D warnings -W clippy::pedantic` is clean and enforced in
CI alongside `cargo fmt --all --check`.

## MSRV

Rust edition 2024, `rust-version = "1.97"`, resolver `3`.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in this project by you, as defined in the
Apache-2.0 license, shall be dual licensed as above, without any additional
terms or conditions.

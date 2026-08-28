# Changelog

All notable changes to this project are documented in this file. Format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this
project has not yet made a versioned release, so everything below lives
under `[0.1.0] – Unreleased` and will be split out once tags start.

## [0.1.0] – Unreleased

Initial buildout of the core library described in `docs/BUILD_PLAN.md`.
Nothing has been published to crates.io yet — `sundog 0.1.0` is a workspace
name reservation, not a release.

### Added

- **Discovery** (`sundog::discovery`): `Mdns` (zeroconf default, `mdns-sd`),
  `Static` (fixed/env-var seed list, re-resolved on an interval), and
  `DnsSrv` (SRV-record polling for Kubernetes headless services, with an
  A/AAAA fallback). All three implement continuous, never-terminating
  candidate streams so a full-cluster cold restart still re-converges.
- **Membership** (`sundog::membership`): gossip membership on `chitchat`,
  advertising each node's data-plane address and incarnation; a `watch`
  stream of the live peer set drives everything downstream.
- **Data plane** (`sundog::net`): a lazy TCP mesh, one connection per live
  peer, `LengthDelimitedCodec`-framed with a 4 MiB frame cap; per-class
  bounded outboxes with the documented drop policy (`Invalidate` drops
  oldest, `Replicate` drops newest and marks the peer dirty for anti-entropy
  priority); `StRequest`/`AeDigest`/`AePull` request/response paths kept off
  the broadcast channel entirely.
- **Store** (`sundog::store`): `moka`-backed typed shards with a hybrid
  logical clock (`Hlc`/`HlcClock`) whose stamps encode deterministically and
  order lexicographically, versioned apply as the single path every write —
  local, replicated, state-transfer, anti-entropy — funnels through,
  tombstones with independent TTL, and a 1,024-bucket
  incrementally-maintained XOR digest array for anti-entropy.
- **Pluggable conflict resolution**: `ConflictResolver` trait, default
  `LwwResolver` (last-write-wins by `Hlc`).
- **`tls` feature** (off by default): mutual TLS on the data-plane mesh
  (`rustls`) — `ClusterConfig::tls`/`ClusterBuilder::tls` wraps every dialed
  and accepted connection, including the short-lived state-transfer/
  anti-entropy ones; client certificates are verified too (mutual auth).
- **Cluster/cache public API** (`sundog::cluster`, `sundog::cache`):
  `Cluster::builder(name).build()` as the zero-config zeroconf happy path;
  `Cluster::cache::<K, V>(name)` builder with `.mode()`, `.max_capacity()`,
  `.ttl()`, `.tti()`, `.resolver()`, `.weigher()`; `Cache<K, V>` with `get`,
  `get_or_load` (stampede-collapsing read-through), `insert`, `remove`,
  `entry_count` (live local count, housekeeping flushed),
  `invalidate_local`, and an `events()` broadcast stream of
  `Created`/`Updated`/`Removed`, each tagged with its `Origin`.
- **Three cache modes**: `Local` (no cluster traffic), `Invalidation`
  (default — independent local copies, writes broadcast an invalidate), and
  `Replicated` (full copy per node, writes broadcast the value).
- **State transfer**: a newly opened `Replicated` cache pulls a full
  snapshot from the lowest-node-id live donor before `open()` returns, then
  runs one immediate anti-entropy round against that donor as a
  belt-and-braces sweep; donor death mid-stream is recovered by re-picking
  and re-requesting, made free by idempotent apply.
- **Anti-entropy**: a jittered background scheduler per `Replicated` cache,
  targeting dirty (backlog-dropped) peers first, reconciling via digest
  compare → bucket pull → push/pull the actual diff.
- **Tombstone GC**: a periodic per-shard sweep at a quarter of
  `tombstone_ttl`, keeping the documented `tombstone_ttl >= 3 * ae_interval`
  safety margin (`ClusterConfig::tombstone_ttl_is_safe`).
- **`tracing` instrumentation** at membership changes, state transfer,
  anti-entropy rounds, and drops; `metrics` counters/gauges
  (`sundog_backlog_dropped_total{peer}`, `sundog_live_peers`,
  `sundog_open_caches`) emitted unconditionally, independent of the
  `prometheus` feature.
- **`prometheus` feature** (off by default): `metrics-exporter-prometheus`
  wired two ways — `ClusterBuilder::prometheus_listen(addr)` serves
  `GET /metrics` directly, and `telemetry::prometheus_handle()` installs a
  recorder for embedding into a caller-owned HTTP server.
- **`sim` feature** (off by default): swaps the data plane's transport seam
  (`net::tcp`) to `turmoil::net`, enabling `tests/sim.rs`'s deterministic
  simulation suite (partition/heal convergence, loss/reorder/dup storms,
  donor crash mid-state-transfer) with no real UDP/TCP involved.
- **`sundog-demo`**: a `ratatui` chaos-testing TUI — N in-process nodes over
  static loopback seeds, a background write load, interactive kill/restart
  per node, live replication/anti-entropy visibility; plus a `--headless
  <SECS>` mode for CI smoke checks and manual soak runs.
- **Test suite**: property tests (`proptest`) on HLC, wire encoding, and the
  store — including the permutation-convergence property that is the
  correctness license for the whole loss-tolerant design; the `turmoil`
  simulation suite; a container-backed multi-node integration suite (see
  next bullet); two-node loopback integration tests living as ordinary unit
  tests next to the code they exercise (`sundog::cluster`'s
  replication/invalidation/state-transfer/anti-entropy/local-mode tests,
  `sundog::store`'s read-through stampede-collapse and TTL-expiry tests);
  Prometheus-exporter and TLS integration tests; unit tests in every module.
- **`sundog-testnode`** (new workspace member): a tiny static-musl binary
  that opens one `Mode::Replicated` `sundog` cache and exposes it over a
  line-based control protocol (`put`/`get`/`del`/`count`/`peers`/`quit`), so
  external test harnesses can drive a real cluster member as a real, separate
  process.
- **Container-backed integration suite** (`sundog/tests/containers.rs`,
  `sundog/tests/container_smoke.rs`, harness in `sundog/tests/container_util`):
  multi-node scenarios run as real, separate `sundog-testnode` processes on
  a real virtual network, exclusively through the
  [`rightsize`](https://crates.io/crates/rightsize) crate — no Docker CLI,
  no `bollard`. Covers three-node
  convergence across distinct writers, tombstones reaching every node, a
  warm join via state transfer into a populated cluster, and a killed node
  catching back up via anti-entropy after restart under the same alias.
  Gated on `SUNDOG_CONTAINER_TESTS=1` (checked first in every test, not
  `#[ignore]`) so a plain `cargo test --workspace` still compiles and passes
  the file trivially without a container backend or the musl target
  installed; needs `RIGHTSIZE_BACKEND=docker` because sundog's gossip is UDP
  and rightsize's microsandbox network emulation relays TCP only.
- CI: the main `ci` job runs, in order, `cargo fmt --check`, `cargo clippy
  --workspace --all-targets -D warnings -W clippy::pedantic` (plus a
  `sim`-feature pass and a `tls,prometheus`-features pass), `cargo test
  --workspace`, then the `turmoil` simulation suite and the `tls,prometheus`
  feature tests as further steps in that same job. The container suite runs
  as its own separate job (musl target + `musl-tools` installed,
  `RIGHTSIZE_BACKEND=docker` and `SUNDOG_CONTAINER_TESTS=1` set, default base
  image pulls fine on a hosted runner). A nightly `nightly-sim` workflow runs
  the simulation suite under a fresh random seed (`SUNDOG_SIM_SEED`), logging
  the seed for local replay.
- **Grafana dashboard** (`ops/grafana-dashboard.json`): panels for live
  peers, open caches, per-peer backlog drops, anti-entropy repair rate, and
  state-transfer throughput.

### Known gaps (tracked, not bugs)

- `CacheError::ModeMismatch` exists as a reserved error variant but nothing
  yet detects a real mode disagreement between nodes for the same cache name
  — see `ROADMAP.md`'s cache-config fingerprint gossip sketch.

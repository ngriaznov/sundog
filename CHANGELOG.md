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
- **Store** (`sundog::store`): `moka`-backed typed shards with a hand-rolled
  HLC (`Hlc`/`HlcClock`, deviating from the plan's `uhlc` in favor of exact,
  deterministically-encoded, property-testable semantics), versioned apply
  as the single path every write — local, replicated, state-transfer,
  anti-entropy — funnels through, tombstones with independent TTL, and a
  1,024-bucket incrementally-maintained XOR digest array for anti-entropy.
- **Pluggable conflict resolution**: `ConflictResolver` trait, default
  `LwwResolver` (last-write-wins by `Hlc`, bit-for-bit what the store always
  did before the trait existed) — pulled forward from the plan's future-work
  list into v1 per `docs/HOUSE_RULES.md`.
- **`tls` feature** (off by default): mutual TLS on the data-plane mesh
  (`rustls`) — `ClusterConfig::tls`/`ClusterBuilder::tls` wraps every dialed
  and accepted connection, including the short-lived state-transfer/
  anti-entropy ones; client certificates are verified too (mutual auth) —
  pulled forward from the plan's future-work list into v1 per
  `docs/HOUSE_RULES.md`.
- **Cluster/cache public API** (`sundog::cluster`, `sundog::cache`):
  `Cluster::builder(name).build()` as the zero-config zeroconf happy path;
  `Cluster::cache::<K, V>(name)` builder with `.mode()`, `.max_capacity()`,
  `.ttl()`, `.tti()`, `.resolver()`, `.weigher()`; `Cache<K, V>` with `get`,
  `get_or_load` (stampede-collapsing read-through), `insert`, `remove`,
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
  simulation suite; in-process multi-node integration tests over real
  `Cluster`s (invalidation/replication visibility, state-transfer warm join,
  anti-entropy repair, TTL expiry, kill/restart, read-through stampede
  collapse, local-mode isolation, Prometheus exporter); unit tests in every
  module.
- CI: `cargo fmt --check`, `cargo clippy --workspace --all-targets -D
  warnings -W clippy::pedantic` (plus a `sim`-feature clippy pass), `cargo
  test --workspace`, and a dedicated job for the `turmoil` simulation suite.

### Known gaps (tracked, not bugs)

- `CacheError::ModeMismatch` exists as a reserved error variant but nothing
  yet detects a real mode disagreement between nodes for the same cache name
  — see `ROADMAP.md`'s cache-config fingerprint gossip sketch.

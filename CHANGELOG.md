# Changelog

All notable changes to this project are documented in this file. Format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.0] – 2026-09-01

The first release: the full buildout of the core library.

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
  and re-requesting, made free by idempotent apply. The time `open()` spends
  on this is bounded by `ClusterConfig::state_transfer_budget` (default 20s)
  — a startup-latency knob, not a correctness one, since anti-entropy tops
  up whatever a cut-off transfer didn't deliver.
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
  that opens `Mode::Replicated` `sundog` caches and exposes them over a
  line-based control protocol — `put`/`get`/`del`/`count`/`peers`/`quit`,
  plus bulk-fill, high-frequency churn, and large-value content-check
  commands — so external test harnesses can drive a real cluster member as
  a real, separate process.
- **Container-backed integration suite** (`sundog/tests/containers.rs`,
  `sundog/tests/container_smoke.rs`, harness in `sundog/tests/container_util`):
  multi-node scenarios run as real, separate `sundog-testnode` processes on
  a real virtual network, exclusively through the
  [`rightsize`](https://crates.io/crates/rightsize) crate — no Docker CLI,
  no `bollard`. Covers three-node
  convergence across distinct writers, tombstones reaching every node, a
  warm join via state transfer into a populated cluster, a killed node
  catching back up via anti-entropy after restart under the same alias,
  cold joins at 100k- and million-entry scale, a three-writer high-churn
  add/remove/TTL scenario that must drain to zero and stay there, and
  realistic 64 KiB values with in-node content verification plus both sides
  of the frame-cap boundary.
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
- **`Cache::insert_many`/`Shard::insert_many`**: bulk local writes applied
  under one acquisition of the store's apply lock rather than one per entry;
  each entry still gets its own `Hlc` stamp and its own `Event`. Fan-out
  notifications for a bulk fill travel as per-stripe key batches on the
  internal channel (`store::FanOutNotice::Many`) rather than one notice per
  entry, so an arbitrarily large fill can never lag the fan-out channel and
  silently degrade its replication to anti-entropy repair — a 100k-entry
  fill fully converges on live peers a fraction of a second after the call
  returns, in a few hundred wire frames.
- **Batched replication on the wire**: the fan-out layer pre-batches each
  drained burst of local writes into `Msg::ReplicateBatch` frames (budget-
  and count-capped), so a bulk burst occupies outbox slots per *batch*
  rather than per record; `net::conn`'s per-peer writer opportunistically
  coalesces whatever is queued — single `Replicate`s and pre-built batches
  alike — into fuller frames (no added latency: only what's already queued
  by the time the writer drains). Anti-entropy repair pushes travel through
  the same budgeted batching instead of one `Replicate` message per repaired
  record. `ShardOps::apply_remote_batch` applies a whole batch — a
  coalesced wire frame, a state-transfer chunk, or an
  anti-entropy pull — under one lock acquisition; the permutation-convergence
  property test mixes single and batch applies as part of its coverage.
  `TCP_NODELAY` is set on every mesh connection: every wire message is
  already a deliberately-sized application-level batch, so nothing is
  gained by leaving Nagle's algorithm to hold small frames back.
- **Replication-cost benchmark suite** (`sundog/tests/replication_bench.rs`,
  gated on `SUNDOG_BENCH=1`): 100k bulk scenarios through both the
  sequential `insert` loop and `insert_many`, a 5k steady-write scenario, a
  5k-write/16-concurrent-writer scenario, and 1M-read latency scenarios
  against both a live `Replicated` member and a quiet `Mode::Local` control
  — all on multi-threaded runtimes against real loopback clusters, printing
  wall time and the process-wide
  `sundog::net::frames_sent_total`/`bytes_sent_total` wire-frame counters.
- **Zero-copy record frames**: `Msg::Replicate`, `Msg::ReplicateBatch`, and
  `Msg::StChunk` — the wire messages that carry actual key/value bytes —
  use a fixed-width layout (`zerocopy`'s safe views, no `unsafe`) instead of
  postcard. Encoding writes straight from already-owned key/value `Bytes`
  with no intermediate buffer; decoding slices `Bytes` views directly out of
  the received frame with no payload copy. A stored record keeps its
  encoded wire bytes (`store::Stored::encoded`) alongside its typed value,
  so answering a replication or anti-entropy request clones an existing
  `Bytes` handle rather than re-serializing. Control messages (`Hello`,
  `StRequest`, `AeDigest`, `AeBucket`, `AePull`, `ReqDone`) still encode as
  postcard.
- **Connection reuse for anti-entropy and state transfer**: `Mesh::ae_round`,
  `ae_pull`, and `request_state` check out an idle, already-`Hello`'d
  connection from a small per-peer pool instead of dialing fresh (and, under
  `tls`, completing a fresh mutual-cert handshake) on every call, falling
  back to a fresh dial when the pool is empty or a pooled connection turns
  out dead. The accept side serves multiple requests per connection instead
  of exactly one, torn down after an idle timeout or a request-count cap.
  This pool is separate from the persistent per-peer broadcast connection,
  so a slow snapshot transfer can't back up live replication traffic. A
  connection is only ever returned to the pool after a clean end-of-reply —
  one left in an unknown framing state after an error, timeout, or
  cancellation is dropped instead of reused.
- **Striped apply lock**: each shard's tombstone map and write-serialization
  lock is split into 64 independent key-hash stripes instead of one lock
  per shard. Writes to keys in different stripes apply fully concurrently;
  writes to the same key stay serialized against each other exactly as a
  single shard-wide lock would. Remote batch applies and local bulk inserts
  group their entries by stripe and apply each stripe's sub-batch under one
  acquisition of that stripe's lock.
- **Lean fan-out**: local writes notify the peer fan-out path over an
  internal keys-only channel (`store::FanOutNotice` — single keys for
  ordinary writes, per-stripe key batches for bulk fills; remote applies
  never notify), separate from the public `Cache::events()` broadcast
  channel. The app-facing `Event` — which owns a clone of the value — is
  only built when `events()` has a subscriber, so a cache with nothing
  subscribed to `events()` pays no per-write value clone for replication or
  invalidation fan-out.
- **Partition-aware tombstone retention**: `ClusterConfig::tombstone_max_ttl`
  (24 hours by default) bounds a new deferral in the tombstone GC sweep — a
  tombstone past `tombstone_ttl` is kept, not collected, while any recently
  known cluster member is currently absent, up to the hard cap. This closes
  the resurrection window where a member absent longer than `tombstone_ttl`
  could bring a manually deleted key back to life on the nodes that stayed
  up; a member gone longer than `tombstone_max_ttl` is the one case that can
  still resurrect a key. Deferred tombstones stay counted in the anti-entropy
  digest until they're actually collected, so digests and the tombstone set
  never drift out of sync.

### Known gaps (tracked, not bugs)

- `CacheError::ModeMismatch` exists as a reserved error variant but nothing
  yet detects a real mode disagreement between nodes for the same cache name
  — see `ROADMAP.md`'s cache-config fingerprint gossip sketch.

# Changelog

All notable changes to this project are documented in this file. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.4.0] – 2026-09-04

### Fixed

- A node declines to donate a state-transfer snapshot of a cache until its
  own transfer of that cache has completed, answering `Msg::StUnavailable`;
  a joiner tries every live peer in turn and, once its snapshot lands, runs
  one anti-entropy round against every live peer, not only its donor. A
  node with no peer in sight at `open()` waits a fifth of
  `state_transfer_budget`, 4 s by default, for gossip to show one before it
  opens as the origin. A node that came up before gossip found any peer
  could donate an empty or half-warm copy to the next joiner, and a chain of
  such joins with the last complete holder crashing lost live entries
  cluster-wide.

### Added

- **Hierarchical anti-entropy digests**: each of the 1,024 anti-entropy
  buckets now also keeps 64 part digests, the next 6 hash bits below the
  bucket's own 10. A mismatched bucket past
  `ClusterConfig::ae_part_min_bucket` entries, default 4,096, answers with its
  64 part digests instead of a full listing or an IBLT sketch, without ever
  materializing the listing; a mismatched part then follows the existing
  listing-or-sketch rule at part scale. This narrows a mismatch 64x before any
  listing or sketch is sent, so repairing one changed key in a 100M-entry
  cache costs a few hundred bytes of digests plus a small listing, instead of
  a multi-megabyte bucket listing. New wire messages `Msg::AePartDigests`,
  `Msg::AeParts`, `Msg::AePart`, and `Msg::AePartSketch` carry the exchange.
  New metric `sundog_ae_parts_total{cache, outcome}` counts `listing`,
  `sketch`, and `fallback` outcomes, one increment per part reply. A 0.3 peer
  is answered with listings and sketches only, so a mixed 0.3/0.4 cluster
  keeps repairing.
- **Protocol versioning**: `wire::PROTOCOL_VERSION`, 2 for this release,
  travels in every `Msg::Hello` and in gossip as `Peer::protocol`. A hello
  from a 0.3 node, which has no such field, decodes as protocol 1, and a
  hello from a newer node with fields this release does not know decodes
  with them ignored. A node serves a peer only with what that peer's
  protocol understands: no part-digest replies and no `Msg::StUnavailable`
  to a protocol-1 peer. One release step interoperates, so a cluster upgrades
  one node at a time; a container test runs the 0.3.1 node against this one
  in both directions.
- **Breaking**, in the `sim`-feature test seams and the wire types only:
  `net::AeMismatch` gains the `PartDigests` variant and is
  `#[non_exhaustive]` from here on, as is the new `net::AePartReply`;
  `net::RequestHandler` and `store::ShardOps` gain the required methods
  `bucket_lens`, `part_digests`, and `entries_for_parts`;
  `wire::Msg::Hello` gains the `protocol` field and `membership::Peer` the
  `protocol` field. `cargo semver-checks --release-type minor` against
  0.3.1 reports exactly these; every other check passes.
- **Chaos lane** for the container suite: `chaos_crashes_churn_and_drops_still_converge`
  drives a four-node cluster through a seeded random mix of crashes, churn,
  dropped keys, refills, and put bursts for a bounded time, then checks every
  node converges to the same content. Gated on `SUNDOG_CONTAINER_TESTS=1` and
  `SUNDOG_CHAOS_SECS`; `SUNDOG_CHAOS_SEED` replays a specific run. Runs for 45s
  in every CI pass and for ten minutes nightly with a fresh seed
  (`nightly-chaos.yml`).
- `sundog-testnode` control commands `digest` (an order-independent xxh3
  digest of the `"it"` cache's live content) and `crash` (exits with status 3
  after replying, with no graceful cluster leave), backing the chaos lane's
  convergence check and node kills.

## [0.3.1] – 2026-09-03

### Fixed

- An anti-entropy responder answers a digest from a peer it still has
  replicate frames queued toward with an empty round, at most three rounds
  running, on top of the initiator-side skip. A bulk fill's own fan-out and
  its repair no longer ship the same records twice when the peer's round
  lands mid-stream.
- The README quick start opens a second, session-typed cache for its
  per-entry TTL example; a cache is typed once at open.

## [0.3.0] – 2026-09-03

### Added

- **Bespoke store engine**: live entries and tombstones share 1,024 lock-striped
  tables, one per anti-entropy bucket, each a `parking_lot::RwLock` over a
  `hashbrown` table keyed by the postcard-encoded key. A read takes one read
  lock and one lookup with no allocation. A versioned apply runs under one write
  guard. Bucket enumeration for anti-entropy is O(bucket), not O(cache). Expiry
  is checked on read and reclaimed by a sweep that visits only stripes with
  something due. Measured against 0.2.0 on one machine: a local read goes 1,083
  ns to 447 ns, a steady replicated write 4.0 µs to 1.0 µs, a 100k-entry
  `insert_many` convergence 0.63 s to 0.20 s.
- **Sketch-based anti-entropy for large buckets**: a mismatched bucket past
  `ClusterConfig::ae_sketch_min_bucket` entries, default 384, answers with an
  invertible Bloom lookup table, `ClusterConfig::ae_sketch_cells`, default 240
  cells and about 9 KB on the wire, instead of a full `(key, version)` listing.
  Wire cost is fixed regardless of bucket size; it decodes symmetric differences
  up to 100 elements in at least 99% of cases and falls back to the full listing
  otherwise. New wire messages `Msg::AeSketch`, `Msg::AeEntries`, and
  `Msg::AePullHashes` carry the exchange and its fallbacks. New metric
  `sundog_ae_sketch_total{cache, outcome}` counts `decoded` vs `fallback`
  outcomes.
- **Stateful fuzzing of the apply path**: two `cargo-fuzz` targets,
  `apply_model` and `apply_permutation` (`sundog/fuzz`), drive coverage-guided,
  sequence-generated local writes, remote applies and batches, invalidations,
  tombstone GC, and clock advances through a real `Shard` alongside a reference
  model of the same semantics, checking the permutation-convergence invariant
  under libFuzzer's own mutation instead of proptest's sampling. The model,
  `sundog::store::model`, `#[doc(hidden)]` behind the `fuzzing` feature or
  `cfg(test)`, is shared with the in-crate property test
  `shard_matches_the_reference_model_under_arbitrary_op_sequences`.

### Changed

- `moka` is no longer a dependency. Size-bounded eviction (`max_capacity`,
  `weigher`) is sampled LRU: a write that pushes total weight past the cap
  evicts the least recently read of eight entries sampled from a rotating offset
  in one stripe, repeating until the cap holds. TTI stays a local per-entry idle
  deadline. Neither is available in `Replicated` mode.
- The hand-off from a local write to the fan-out routine is a lossless queue of
  pending keys drained whole, replacing a bounded broadcast channel that lagged
  under a burst of single inserts and left the gap to anti-entropy. A burst of
  any size costs one drain. `insert_many` and `remove_many` hand their keys
  off one full replicate batch at a time, so a fill costs a bounded number
  of frames per peer whatever the machine's speed.
- Anti-entropy pull replies (`AePull`, `AePullHashes`) travel as
  `ReplicateBatch` frames under the same byte and count budget as the live
  fan-out, replacing one `Replicate` frame per record; a 100k-record repair is a
  few dozen frames.
- Anti-entropy skips a peer while replicate traffic with it is still in motion,
  frames queued or recently sent toward it, or a batch recently received from
  it, judged over one `ae_interval`, for at most three rounds running. This
  closes the double-delivery race between a bulk fill's own fan-out and its
  repair, without letting a steady write trickle starve the repair.
- `Cluster::build` returns `JoinError::InvalidConfig` for an `ae_sketch_cells`
  whose sketch cannot fit in one `max_frame` frame, the same way it already
  rejects a `max_frame` above the wire codec's cap.
- Mixed 0.2/0.3 clusters are not supported: a 0.3 node's anti-entropy round can
  send `AeSketch`, `AeEntries`, and `AePullHashes`, which a 0.2 node cannot
  decode. Upgrade every node.
- **Breaking**: `wire::Msg` is `#[non_exhaustive]`. A downstream `match` on
  `Msg` without a wildcard arm needs one added, a one-time cost that lets future
  wire message kinds, like this release's own
  `AeSketch`/`AeEntries`/`AePullHashes`, ship without another breaking release
  apiece. `cargo semver-checks --release-type minor` reports exactly this one
  intentional break; every other check passes.

### Fixed

- `insert_many` and `insert_many_with_ttl` apply the entries before an
  oversized value and then return `ValueTooLarge`, as their docs state.
  Every entry was rejected before.
- `DnsSrv` discovery uses the configured fallback port for an SRV record
  whose port is zero instead of dialing port zero.

## [0.2.0] – 2026-09-02

### Added

- **Cache-config fingerprint gossip**: every node advertises the mode of each
  open cache in its membership state. `open()` on a name a live peer already
  runs under a different `Mode` fails with `CacheError::ModeMismatch { cache,
  local, remote }`; `Mode::Local` counts too, since a private cache and a
  replicated one can't share a name. A conflict that slips past the open-time
  check, two nodes opening at the same instant, is reported loudly when the
  peer's advertisement arrives. TTL and capacity stay local knobs.
- **API surface**: `Cache::contains_key`, expiry-aware and not counted as a
  read; `Cache::keys`, a point-in-time local snapshot; `Cache::remove_many`, the
  tombstone counterpart of `insert_many`, one lock acquisition per stripe and
  one `Removed` event per key, batched fan-out; `Cache::clear`, which tombstones
  every key this node holds and fans them out at O(entries), and in `Replicated`
  mode empties the cluster once tombstones land; and
  `Cache::get_or_insert_with`, an infallible-loader `get_or_load` with the same
  stampede collapse. The `Shard` API gains the same methods.
- **Per-cache metrics**: `sundog_cache_hits_total{cache}` and
  `sundog_cache_misses_total{cache}`. A miss is one loader execution or one
  empty `get`; collapsed waiters count as hits; `contains_key` counts as
  neither. `sundog_cache_entries{cache}` is a gauge refreshed every five seconds
  per open cache. Counter handles are created once per shard, so the read path
  pays an atomic increment, not label resolution. Two matching Grafana panels
  track hit ratio and entries per cache.

### Removed

- `Cache::get_or_load_with_ttl` and `Shard::get_or_load_with_ttl`, present in
  0.1.2. Reads are TTL-blind: `get_or_load` fills take the cache default, and
  only writes (`insert_with_ttl`, `insert_many_with_ttl`) carry a per-entry
  lifespan. Every other 0.1.x API is unchanged in 0.2.0.

## [0.1.2] – 2026-09-01

The crate published under this version carries `Cache::get_or_load_with_ttl` and
`Shard::get_or_load_with_ttl`, a read-side TTL parameter at odds with the design
below: reads never touch expiry. 0.2.0 removes it and keeps everything else from
0.1.2.

### Added

- **Per-entry TTL**: `Cache::insert_with_ttl` and `Cache::insert_many_with_ttl`
  give one write, or one batch, its own lifespan, overriding the cache's
  `.ttl(..)` default in either direction and working on a cache with no default.
  The per-entry deadline is stamped as the record's absolute `expires_at_ms` and
  replicates exactly as a default-TTL stamp, so the entry expires at the same
  instant on every node with the same can't-resurrect guarantee. Reads stay out
  of it: `get_or_load` fills take the cache default. The `Shard` API gains the
  same two methods.

## [0.1.1] – 2026-09-01

### Fixed

- The crate's packaged README omits the `ROADMAP.md` link: the file isn't part
  of the package, so the link 404s on crates.io. The repository README links to
  `ROADMAP.md` instead.

## [0.1.0] – 2026-09-01

The first release: the full core library.

### Added

- **Discovery** (`sundog::discovery`): `Mdns`, zeroconf default via `mdns-sd`;
  `Static`, fixed or env-var seed list re-resolved on an interval; and `DnsSrv`,
  SRV-record polling for Kubernetes headless services with an A/AAAA fallback.
  All three stream candidates continuously, so a full-cluster cold restart still
  re-converges.
- **Membership** (`sundog::membership`): gossip membership on `chitchat`,
  advertising each node's data-plane address and incarnation; a `watch` stream
  of the live peer set drives everything downstream.
- **Data plane** (`sundog::net`): a lazy TCP mesh, one connection per live peer,
  `LengthDelimitedCodec`-framed with a 4 MiB frame cap; per-class bounded
  outboxes with a documented drop policy, `Invalidate` drops oldest, `Replicate`
  drops newest and marks the peer dirty for anti-entropy priority;
  `StRequest`/`AeDigest`/`AePull` request-response paths off the broadcast
  channel entirely.
- **Store** (`sundog::store`): typed shards on a hybrid logical clock
  (`Hlc`/`HlcClock`) whose stamps encode deterministically and order
  lexicographically, versioned apply as the single path every write, local,
  replicated, state-transfer, anti-entropy, funnels through, tombstones with
  independent TTL, and a 1,024-bucket incrementally-maintained XOR digest array
  for anti-entropy.
- **Pluggable conflict resolution**: `ConflictResolver` trait, default
  `LwwResolver`, last-write-wins by `Hlc`.
- **`tls` feature**, off by default: mutual TLS on the data-plane mesh
  (`rustls`). `ClusterConfig::tls`/`ClusterBuilder::tls` wraps every dialed and
  accepted connection, including the short-lived state-transfer and anti-entropy
  ones; client certificates are verified too, mutual auth.
- **Cluster/cache public API** (`sundog::cluster`, `sundog::cache`):
  `Cluster::builder(name).build()` as the zero-config zeroconf happy path;
  `Cluster::cache::<K, V>(name)` builder with `.mode()`, `.max_capacity()`,
  `.ttl()`, `.tti()`, `.resolver()`, `.weigher()`; `Cache<K, V>` with `get`,
  `get_or_load`, stampede-collapsing read-through, `insert`, `remove`,
  `entry_count`, live local count and housekeeping flushed, `invalidate_local`,
  and an `events()` broadcast stream of `Created`/`Updated`/`Removed`, each
  tagged with its `Origin`.
- **Three cache modes**: `Local`, no cluster traffic; `Invalidation`, the
  default, independent local copies with writes broadcasting an invalidate; and
  `Replicated`, full copy per node, writes broadcast the value.
- **State transfer**: a newly opened `Replicated` cache pulls a full snapshot
  from the lowest-node-id live donor before `open()` returns, then runs one
  immediate anti-entropy round against that donor as a safety sweep; donor death
  mid-stream is recovered by re-picking and re-requesting, made free by
  idempotent apply. Time `open()` spends on this is bounded by
  `ClusterConfig::state_transfer_budget`, default 20s, a startup-latency knob,
  not a correctness one, since anti-entropy tops up whatever a cut-off transfer
  didn't deliver.
- **Anti-entropy**: a jittered background scheduler per `Replicated` cache,
  targeting dirty, backlog-dropped, peers first, reconciling via digest compare,
  bucket pull, push or pull the actual diff.
- **Tombstone GC**: a periodic per-shard sweep at a quarter of `tombstone_ttl`,
  keeping the documented `tombstone_ttl >= 3 * ae_interval` safety margin
  (`ClusterConfig::tombstone_ttl_is_safe`).
- **`tracing` instrumentation** at membership changes, state transfer,
  anti-entropy rounds, and drops; `metrics` counters and gauges
  (`sundog_backlog_dropped_total{peer}`, `sundog_live_peers`,
  `sundog_open_caches`) emitted unconditionally, independent of the `prometheus`
  feature.
- **`prometheus` feature**, off by default: `metrics-exporter-prometheus` wired
  two ways, `ClusterBuilder::prometheus_listen(addr)` serves `GET /metrics`
  directly, and `telemetry::prometheus_handle()` installs a recorder for
  embedding into a caller-owned HTTP server.
- **`sim` feature**, off by default: swaps the data plane's transport seam
  (`net::tcp`) to `turmoil::net`, enabling `tests/sim.rs`'s deterministic
  simulation suite, partition/heal convergence, loss/reorder/dup storms, donor
  crash mid-state-transfer, with no real UDP/TCP involved.
- **`sundog-demo`**: a `ratatui` chaos-testing TUI, N in-process nodes over
  static loopback seeds, a background write load, interactive kill/restart per
  node, live replication/anti-entropy visibility; plus a `--headless <SECS>`
  mode for CI smoke checks and manual soak runs.
- **Test suite**: property tests (`proptest`) on HLC, wire encoding, and the
  store, including the permutation-convergence property, the correctness
  argument for the whole loss-tolerant design; the `turmoil` simulation suite; a
  container-backed multi-node integration suite, see next bullet; two-node
  loopback integration tests living as ordinary unit tests next to the code they
  exercise (`sundog::cluster`'s
  replication/invalidation/state-transfer/anti-entropy/ local-mode tests,
  `sundog::store`'s read-through stampede-collapse and TTL-expiry tests);
  Prometheus-exporter and TLS integration tests; unit tests in every module.
- **`sundog-testnode`** (new workspace member): a tiny static-musl binary that
  opens `Mode::Replicated` `sundog` caches and exposes them over a line-based
  control protocol, `put`/`get`/`del`/`count`/`peers`/`quit`, plus bulk-fill,
  high-frequency churn, and large-value content-check commands, so external test
  harnesses can drive a real cluster member as a separate process.
- **Container-backed integration suite** (`sundog/tests/containers.rs`,
  `sundog/tests/container_smoke.rs`, harness in `sundog/tests/container_util`):
  multi-node scenarios run as separate `sundog-testnode` processes on a real
  virtual network, exclusively through the
  [`rightsize`](https://crates.io/crates/rightsize) crate, no Docker CLI, no
  `bollard`. Covers three-node convergence across distinct writers, tombstones
  reaching every node, a warm join via state transfer into a populated cluster,
  a killed node catching back up via anti-entropy after restart under the same
  alias, cold joins at 100k- and million-entry scale, a three-writer high-churn
  add/remove/TTL scenario that must drain to zero and stay there, and realistic
  64 KiB values with in-node content verification plus both sides of the
  frame-cap boundary. Gated on `SUNDOG_CONTAINER_TESTS=1`, checked first in
  every test, not `#[ignore]`, so a plain `cargo test --workspace` still
  compiles and passes the file without a container backend or the musl target
  installed; needs `RIGHTSIZE_BACKEND=docker` because sundog's gossip is UDP and
  rightsize's microsandbox network emulation relays TCP only.
- CI: the main `ci` job runs, in order, `cargo fmt --check`, `cargo clippy
  --workspace --all-targets -D warnings -W clippy::pedantic`, plus a
  `sim`-feature pass and a `tls,prometheus`-features pass, `cargo test
  --workspace`, then the `turmoil` simulation suite and the `tls,prometheus`
  feature tests as further steps in that same job. The container suite runs as
  its own separate job, musl target and `musl-tools` installed,
  `RIGHTSIZE_BACKEND=docker` and `SUNDOG_CONTAINER_TESTS=1` set, default base
  image pulls fine on a hosted runner. A nightly `nightly-sim` workflow runs the
  simulation suite under a fresh random seed (`SUNDOG_SIM_SEED`), logging the
  seed for local replay.
- **Grafana dashboard** (`ops/grafana-dashboard.json`): panels for live peers,
  open caches, per-peer backlog drops, anti-entropy repair rate, and
  state-transfer throughput.
- **`Cache::insert_many`/`Shard::insert_many`**: bulk local writes applied under
  one acquisition of the store's apply lock rather than one per entry; each
  entry still gets its own `Hlc` stamp and its own `Event`. Fan-out
  notifications for a bulk fill travel as per-stripe key batches on the internal
  channel (`store::FanOutNotice::Many`) rather than one notice per entry, so an
  arbitrarily large fill can never lag the fan-out channel and degrade its
  replication to anti-entropy repair. A 100k-entry fill fully converges on live
  peers a fraction of a second after the call returns, in a few hundred wire
  frames.
- **Batched replication on the wire**: the fan-out layer pre-batches each
  drained burst of local writes into `Msg::ReplicateBatch` frames, budget- and
  count-capped, so a bulk burst occupies outbox slots per *batch* rather than
  per record; `net::conn`'s per-peer writer opportunistically coalesces whatever
  is queued, single `Replicate`s and pre-built batches alike, into fuller frames
  with no added latency, only what's already queued by the time the writer
  drains. Anti-entropy repair pushes travel through the same budgeted batching
  instead of one `Replicate` message per repaired record.
  `ShardOps::apply_remote_batch` applies a whole batch, a coalesced wire frame,
  a state-transfer chunk, or an anti-entropy pull, under one lock acquisition;
  the permutation-convergence property test mixes single and batch applies as
  part of its coverage. `TCP_NODELAY` is set on every mesh connection: every
  wire message is already a deliberately-sized application-level batch, so
  nothing is gained by leaving Nagle's algorithm to hold small frames back.
- **Replication-cost benchmark suite** (`sundog/tests/replication_bench.rs`,
  gated on `SUNDOG_BENCH=1`): 100k bulk scenarios through both the sequential
  `insert` loop and `insert_many`, a 5k steady-write scenario, a
  5k-write/16-concurrent-writer scenario, and 1M-read latency scenarios against
  both a live `Replicated` member and a quiet `Mode::Local` control, all on
  multi-threaded runtimes against real loopback clusters, printing wall time and
  the process-wide `sundog::net::frames_sent_total`/`bytes_sent_total`
  wire-frame counters.
- **Zero-copy record frames**: `Msg::Replicate`, `Msg::ReplicateBatch`, and
  `Msg::StChunk`, the wire messages that carry actual key/value bytes, use a
  fixed-width layout (`zerocopy`'s safe views, no `unsafe`) instead of postcard.
  Encoding writes straight from already-owned key/value `Bytes` with no
  intermediate buffer; decoding slices `Bytes` views directly out of the
  received frame with no payload copy. A stored record keeps its encoded wire
  bytes (`store::Stored::encoded`) alongside its typed value, so answering a
  replication or anti-entropy request clones an existing `Bytes` handle rather
  than re-serializing. Control messages (`Hello`, `StRequest`, `AeDigest`,
  `AeBucket`, `AePull`, `ReqDone`) still encode as postcard.
- **Connection reuse for anti-entropy and state transfer**: `Mesh::ae_round`,
  `ae_pull`, and `request_state` check out an idle, already-`Hello`'d connection
  from a small per-peer pool instead of dialing fresh, and under `tls`,
  completing a fresh mutual-cert handshake, on every call, falling back to a
  fresh dial when the pool is empty or a pooled connection turns out dead. The
  accept side serves multiple requests per connection instead of exactly one,
  torn down after an idle timeout or a request-count cap. This pool is separate
  from the persistent per-peer broadcast connection, so a slow snapshot transfer
  can't back up live replication traffic. A connection is only ever returned to
  the pool after a clean end-of-reply; one left in an unknown framing state
  after an error, timeout, or cancellation is dropped instead of reused.
- **Striped apply lock**: each shard's tombstone map and write-serialization
  lock is split into 64 independent key-hash stripes instead of one lock per
  shard. Writes to keys in different stripes apply fully concurrently; writes to
  the same key stay serialized against each other exactly as a single shard-wide
  lock would. Remote batch applies and local bulk inserts group their entries by
  stripe and apply each stripe's sub-batch under one acquisition of that
  stripe's lock.
- **Lean fan-out**: local writes notify the peer fan-out path over an internal
  keys-only channel (`store::FanOutNotice`, single keys for ordinary writes,
  per-stripe key batches for bulk fills; remote applies never notify), separate
  from the public `Cache::events()` broadcast channel. The app-facing `Event`,
  which owns a clone of the value, is only built when `events()` has a
  subscriber, so a cache with nothing subscribed to `events()` pays no per-write
  value clone for replication or invalidation fan-out.
- **Partition-aware tombstone retention**: `ClusterConfig::tombstone_max_ttl`,
  24 hours by default, bounds a new deferral in the tombstone GC sweep: a
  tombstone past `tombstone_ttl` is kept, not collected, while any recently
  known cluster member is currently absent, up to the hard cap. This closes the
  resurrection window where a member absent longer than `tombstone_ttl` could
  bring a manually deleted key back to life on the nodes that stayed up; a
  member gone longer than `tombstone_max_ttl` is the one case that can still
  resurrect a key. Deferred tombstones stay counted in the anti-entropy digest
  until they're actually collected, so digests and the tombstone set never drift
  out of sync.

### Known gaps (tracked, not bugs)

- `CacheError::ModeMismatch` exists as a reserved error variant but nothing
  detects a real mode disagreement between nodes for the same cache name. Closed
  in 0.2.0 by cache-config fingerprint gossip.

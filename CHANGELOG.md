# Changelog

All notable changes to this project are documented in this file. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- **Kani proofs**: `#[kani::proof]` harnesses under each module's
  `kani_proofs` prove, over every input, that expiry packing and the
  touch stamp round-trip, a hash lands inside the bucket and part tables,
  the compaction shrink rule fires only at an eighth of an allocation, a
  reconciliation retry never outruns its cap or time budget and the loop
  stops at every bound, the gossip bind retry moves only a port-0
  request within its cap, the anti-entropy skip rule keeps its bound, the
  hybrid logical clock advances past both the last and
  the observed stamp, a replicate frame length never overflows, and spill
  sizing stays within its clamps. `weekly-kani.yml` runs them weekly and
  the release gate requires a green run.
- **Entry diet**: a live entry stores one encoded record (key length, key,
  and value in the postcard form the wire already uses) instead of a typed
  key and value plus a separate encoded copy, and that record is an enum,
  inline for records up to 30 bytes and boxed beyond, so a short key and
  value need no heap allocation at all. The version's three fields sit on
  the entry itself, with the logical counter packed beside the weight, and
  `Live::ver` reassembles an `Hlc` for every comparison; `Live` is 72 bytes
  without spill. Measured on a 4-core box: 4M entries in one local cache
  settle at 177 bytes per entry against 522 before, the 3-node 4M-key
  harness settles at 3.24 GiB against 5.26 GiB before under glibc, and
  local read latency drops from 0.538 to 0.296 microseconds. The entry diet
  bench (`SUNDOG_BENCH=1 cargo test --release -p sundog --test
  entry_diet_bench`) pins both the size and the latency.
- **Entry diet, phase A, a packed `Live`**: a live entry's record inlines up
  to 22 bytes instead of 30, folding the enum discriminant into a 24-byte
  `Record` with no separate tag; the expiry packs into a `u32` delta above
  the entry's own `wall_ms`, with a per-stripe side table holding the exact
  absolute deadline for the rare TTL past `u32::MAX` milliseconds (about
  49.71 days); the last-access timestamp packs into a `u32` touch stamp,
  read back only through a wrapping-subtraction helper that stays correct
  across a stamp rollover, including in sampled eviction's coldest-entry
  pick. `Live` sits at 56 bytes without spill, 80 with it. `wall_ms`,
  `node` (the full 64-bit `NodeId`), and `logical` keep their full width:
  `node` must stay bit-identical across independently computed replica
  merges, and `logical` mints past `u32::MAX` on every merge-driven
  compaction with no periodic reset. Every TTL keeps millisecond
  precision, on both sides of the inline delta's own ~49.71-day ceiling,
  and reads stay TTL-blind. No wire change: `wire::PROTOCOL_VERSION` and
  `WireRecord` stay exactly as they are; only the in-RAM entry encoding
  changes.
- **Entry diet, phase B, a `Slab` replaces `Stripe::live`'s hash table**:
  `Stripe::live` (`sundog/src/store/engine.rs`) is a `Slab<K, V>`, a dense
  `Vec<Live<K, V>>` arena plus a `HashTable<u32>` index mapping each live
  key's hash to its arena slot, in place of a `HashTable<Live<K, V>>`
  holding entries directly. `Slab::remove` swap-removes an entry and fixes
  the moved entry's index row in the same call, under the stripe's own
  lock, so the arena stays dense with no holes and `entries.len()` is
  always the exact live count; every full-stripe walker (digest XOR,
  snapshot and state-transfer, sampled eviction, sweep's retain,
  `release_buckets`' drain) iterates the arena directly with no liveness
  check. An idle index bucket costs about 5 bytes against the roughly 73
  bytes an idle `Live`-holding bucket costs at the same load factor, since
  the index stores a 4-byte slot number instead of a 56-byte `Live`. On
  the entry diet bench's profile (`Cache<String, String>`, 8-byte keys and
  values, 4,000,000 entries, one node, `Mode::Local`, glibc, a 4-core
  box), live heap drops to 69.4 bytes per entry (119.5 after phase A
  alone) and the settled resident set drops to 0.305 GiB, about 81 bytes
  per entry; read p50 stays at 0.24-0.28 microseconds, inside the
  existing 0.616-microsecond ceiling, with no measurable cost from the
  slab's extra index-then-arena dereference on this profile.
  `entry_diet_rss_budget`'s `RSS_BUDGET_BYTES` tightens from 0.6 GiB to
  0.35 GiB to match. No public API change, no wire change, no
  `BUCKET_COUNT` change: this is an intra-stripe storage change the
  mixed-version container test needs no new gate for.
- **`CacheBuilder::capacity_hint`**: hints this shard's expected local
  entry count so each of `BUCKET_COUNT` stripes' `Slab` (arena and index)
  preallocates up front instead of growing one insert at a time. `None`,
  the default, keeps today's zero-allocation-until-first-write behavior
  exactly. `open()` clamps the hint to `max_capacity` unless a weigher is
  configured, since a weigher turns `max_capacity` into a weight budget
  rather than an entry count. An oversized hint costs memory rather than
  correctness: `Engine::compact`'s existing paced pass reclaims a stripe
  left far below its own reserved capacity, from an inflated hint or from
  ownership rebalancing draining it, via a new `should_shrink_stripe`
  check and a `Slab::shrink_to_fit` call. `demos/sundog-distributed-demo`
  and `sundog-testnode` each pass `capacity_hint` the same per-node figure
  they already pass `max_capacity`. The entry diet bench's
  `entry_diet_rss_budget` runs a hinted 4,000,000-entry pass beside the
  unhinted one (74.7 unhinted, 77.3 hinted bytes per entry on a 4-core
  glibc box, both under the 90-byte target) and a 64,000,000-entry variant
  of both (67.4 unhinted, 67.3 hinted, confirming the byte cost holds or
  improves at scale rather than climbing), and a new
  `entry_diet_rss_budget_heap_shape` measures a 16-byte-key/100-byte-value
  shape at 209.6 bytes per entry, inside Redis 7's own 195-230
  bytes/copy practical range for the same shape. The README's new Memory
  per entry section lays out both shapes against Redis 7's computed and
  practical figures, with the exact commands to reproduce either side.
- **Converge-before-serving reconciliation for a warm spill reopen**:
  `reconcile_warm_buckets` groups every warm-reloaded bucket by its live
  co-owners and, per co-owner, loops a bucket-scoped anti-entropy round
  (`anti_entropy::run_round_for_buckets`) over that peer's still-diverging
  buckets until the peer's digest exchange itself reports no mismatch for
  a bucket, up to three rounds that ran, `ClusterConfig::reconcile_byte_budget`
  (default 32 MiB) wire bytes pushed and pulled, or
  `ClusterConfig::state_transfer_budget` of wall time, whichever the
  peer's loop hits first. A round the peer fails or answers stale, as every
  co-owner does at a restart until its ownership view catches up to the
  node rejoining, never counts as a round: the loop retries it after a
  backoff from 200 ms doubling per consecutive failure up to `ae_interval`,
  while the peer stays live and the wait ends inside the time budget.
  On the 3-node 4M-key harness the restarted node's first round against each
  co-owner comes back stale, and two rounds after a 200 ms backoff mark all
  672 replayed buckets serving with none left unverified, the gate
  satisfied.
  Different peers' loops
  run concurrently, so the step's worst case stays bounded by one peer's
  budget however many live co-owners a node's warm buckets span. A bucket
  clears cold and unverified via `ResidencySet::mark_serving` the instant
  its digest matches a live co-owner's; a bucket whose loop exhausts the
  budget against every live co-owner, or that has no live co-owner at all,
  keeps both marks exactly as `attach_ownership` set them and falls through
  to the ordinary cold-pull path, which alone decides whether to trust and
  serve it, and a failed round never reports a bucket converged. A round
  classifies its bucket and part-digest mismatches in chunks of 64 buckets,
  so no round materializes more than one chunk's listings at once. The
  warm-reopen cluster suite drives a mass overwrite across most buckets
  while a node is down and reads back only current values after its
  reopen.
- **jemalloc in the demo and the test node**: `demos/sundog-distributed-demo`
  and `sundog-testnode` both set `tikv_jemallocator::Jemalloc` as their
  global allocator on every target but MSVC Windows; the `sundog` library
  itself sets none, leaving the choice to whatever embeds it. glibc's
  arenas keep the preload's and anti-entropy's transient buffers resident
  past the point they're needed; jemalloc returns that memory. On the
  3-node 4M-key harness the demo settles at 1.70 GiB against 3.24 GiB under
  glibc, and preload throughput rises from 1.49M to 1.66M keys/s.
- **Distributed demo sizing, metrics, report and gate flags**: `--value-bytes
  <N>` pads every preload and load value out to N bytes. `--max-entries
  <N>` caps live entries per node, and needs `--spill-dir <PATH>` (with
  `--spill-capacity-mb`, `--spill-region-mb`, and `--spill-flush-queue-mb`,
  all in mebibytes) to size the spill tier that catches what the cap
  evicts; both need the demo built with `--features spill`. `--metrics`
  (or `--metrics-interval-secs <N>`, which implies it) installs the
  process-wide Prometheus recorder before any node opens and prints an
  in-process `sundog_*` status line every interval during a `--headless`
  run, needing `--features prometheus`. `--report-json <PATH>` writes a
  JSON summary of a headless run (RSS, fetch latency, the sample check,
  convergence, and the summed `sundog_*` totals) at the end of the run.
  `--gate <PATH>` reads a JSON threshold file of the same shape, checks the
  run's report against it, and exits nonzero listing every violated
  threshold. Both need `--metrics`.
- **Test node sizing knobs**: `sundog-testnode` reads
  `SUNDOG_TESTNODE_MAX_ENTRIES` as an entry-count cap on `"it"`, alongside
  the existing byte-denominated `SUNDOG_TESTNODE_MAX_CAPACITY_BYTES` (which
  wins when both are set), and `SUNDOG_TESTNODE_SPILL_CAPACITY_MB`,
  `SUNDOG_TESTNODE_SPILL_REGION_MB`, and
  `SUNDOG_TESTNODE_SPILL_FLUSH_QUEUE_MB` as mebibyte-denominated
  counterparts of the existing `_BYTES` spill sizing variables (each byte
  variable still wins when both are set), mirroring the distributed demo's
  own `--max-entries`/`--spill-capacity-mb`/`--spill-region-mb`/
  `--spill-flush-queue-mb` flags.
- `sundog_rebalance_pull_timeouts_total{cache}`: a `Mode::Distributed`
  cache's seventh Prometheus metric, counting warm-ups that gave up on a
  bucket pull timing out repeatedly and opened warm with whatever landed,
  leaving the rest to anti-entropy.
- **Spill flush-queue admission control**: a semaphore, sized from
  `SpillConfig::flush_queue_bytes_value()`, tracks the flush queue's byte
  budget in place of a plain counter. `Shard::insert_many`/`remove_many`
  and `ShardOps::apply_remote_batch` (live replication, anti-entropy pull
  repair, and every rebalance/state-transfer pull) reserve one batch's
  worth of flush-queue room, once per call, before taking any stripe
  lock, and wait up to `SpillConfig::spill_wait_timeout` (a new builder
  method and accessor, `Duration::from_secs(2)` by default,
  `Duration::ZERO` a documented opt-out) for room to free up instead of
  refusing the eviction outright. That one reservation is sized only from
  the call's own new bytes, so it can still run short against a real
  backlog a lagging flusher left behind from earlier calls; when it does,
  the call retries with a fresh reservation for exactly the shortfall,
  against the same call's overall `spill_wait_timeout` budget. Every
  entry this covers stays resident while the retry is still within
  budget, never dropped for a shortfall the retry could still pay down;
  once the budget runs out, the shortfall resolves through the same
  ordinary, non-blocking refusal any unreserved write already uses, so
  `SpillTier::set_keep_resident_when_refused`'s policy decides the
  victim's fate exactly as it always has rather than leaving it resident
  regardless of that policy. **This is a real behavior change for
  every existing spill deployment**: `SpillConfig::new(...)` with no
  override moves from always refusing an eviction instantly under
  backpressure to waiting up to two seconds for room first; a cache
  opened with no `SpillConfig` sees no change at all. The flush channel's
  slot count scales with `flush_queue_bytes_value()` too (clamped to a
  floor at the existing fixed 8192 slots and a ceiling that bounds a
  pathological config), rather than staying fixed regardless of
  configuration, since a fixed slot count fills, for small records, long
  before the byte budget does. Three new metrics cover the wait itself:
  `sundog_spill_wait_seconds_total{cache}` (a counter, whole seconds),
  `sundog_spill_waiters{cache}` (a gauge), and
  `sundog_spill_wait_timeouts_total{cache}` (a counter, incremented only
  when the wait itself times out). `sundog_spill_dropped_total` gains a
  `reason="disk_error"` value, incremented for every job in a segment
  whose write fails; the victim stays resident on this path exactly as
  every other refusal reason leaves it, so this counter is the only
  visible sign of the failure.
- **Fan-out backpressure, on both the send and the write side**: a live
  peer's replicate frame is never dropped. `net::Mesh::send_frames_awaiting`
  now waits for outbox room in `FAN_OUT_SEND_DEADLINE` (2s) slices for as
  long as the target peer stays live in the mesh's peer table, logging each
  timed-out slice at debug and counting it, in whole seconds, in the new
  `sundog_fan_out_wait_seconds_total{peer}`, instead of giving up; only a
  peer that has actually left the table (or whose outbox channel has
  already closed) falls back to today's drop-and-count-and-warn path
  against `sundog_backlog_dropped_total`. On the write side, `ClusterConfig`
  gains `fan_out_backlog_capacity` (a new `usize` field, 262,144 keys
  default, validated nonzero; `ClusterBuilder::build` rejects zero) and
  `fan_out_wait_timeout` (a new `Duration` field, 30s default): every async
  write (`insert`, `insert_with_ttl`, `insert_many`, `insert_many_with_ttl`,
  `remove`, `remove_many`, and `merge`'s immediate-apply path) awaits room
  in the shard's fan-out queue below that capacity, up to the timeout,
  before pushing, so a stalled fan-out grows in memory instead of without
  bound. The write always lands regardless: past the timeout it proceeds
  anyway, over capacity, counted in the new
  `sundog_fan_out_wait_timeouts_total{cache}`. A new gauge,
  `sundog_fan_out_backlog{cache}`, exposes the queue's current length. The
  synchronous write paths (`insert_sync`, `remove_sync`, `Shard::apply`)
  keep pushing without waiting, as before; a default-configured caller who
  never hits either new limit sees no behavior change. The distributed
  demo's `--report-json` gains `backlog_dropped` (`sundog_backlog_dropped_total`
  summed across peers) and `fan_out_wait_timeouts`
  (`sundog_fan_out_wait_timeouts_total` summed across caches), and its
  `--gate` file gains `max_backlog_dropped` (optional, an older gate file
  with the field absent skips the check exactly as before);
  `ops/scale-gate.json` sets it to 0.
- **Scale workflow**: `.github/workflows/scale.yml` runs the distributed
  demo headless overnight (and on demand via `workflow_dispatch`, with
  `keys`, `duration_secs`, `max_entries`, and `runner` inputs) at 4M keys,
  three nodes, two owners, 256-byte values, an 800k-entry RAM cap per node
  over a spill tier, and checks the resulting report against
  `ops/scale-gate.json`'s thresholds: steady RSS at most 4.5 GiB, peak RSS
  at most 5.6 GiB, at most 100,000 deferred spill drops, zero pull
  timeouts, fetch p99 at most 100 milliseconds, full convergence, and a
  fully passing sample check. It uploads the run's `scale-report.json` and
  log as workflow artifacts either way.
- **Chunked bucket pull with independent per-bucket release**: a rebalance
  donor sub-batches each bucket's key list by `rebalance_chunk_bytes` (a new
  `ClusterConfig` field, 1 MiB default, clamped below `MAX_FRAME`) instead
  of materializing a whole bucket's records at once, bounding donor RAM to
  that budget times `rebalance_concurrency` regardless of bucket size.
  `wire::PROTOCOL_VERSION` bumps to 4, adding `Msg::StBucketDone` and
  `Msg::StBucketAck`, gated on `wire::PROTOCOL_ST_BUCKET_DONE_ACK`; a
  protocol-3 peer on either side of a connection sees byte-for-byte today's
  traffic. The donor signals a bucket's completion the instant its own last
  chunk goes out, and the receiver clears that bucket's cold mark and
  serves it immediately rather than waiting on the rest of its transfer
  group. The receiver's `Msg::StBucketAck`, trusted for a new bounded
  `rebalance_ack_window` (default `2 * ae_interval`), lets the donor skip a
  redundant confirming anti-entropy round before release; `disown_grace`
  stays the hard floor underneath either way, so a bucket is never
  released before it.
- **Fast spill reopen from a checkpoint snapshot**: with the new
  `SpillConfig::warm_reopen(true)` (default `false`, so a default tier's
  open and close cost are unaffected), a clean `SpillTier::close` first
  writes every currently-resident live record into the region ring
  alongside every already-spilled one, then writes a snapshot next to the
  region files listing every live entry's key, version, expiry, and
  on-disk location; tombstones and expired entries never appear in it, and
  it is written to a temporary name and renamed into place, so a crash
  mid-write leaves no snapshot. A restart against the same spill directory
  replays only that snapshot (`SpillTier::reopen`), never scanning a
  region file for records it does not already know to look for: it
  validates the snapshot's magic, format version, and sizing against the
  current `SpillConfig`, drops (and counts) an entry whose location fails
  to decode, falling the whole reopen back cold if any entry is bad,
  filters to buckets the fresh ownership view says this node still owns
  (for `Mode::Distributed` on a cluster built with seeds, `CacheBuilder::open`
  now waits briefly, bounded by `min(ClusterConfig::state_transfer_budget,
  5s)`, for a first known peer before computing that view at all, so a
  restart racing gossip convergence never treats a warm reopen's ownership
  filter as "this node owns everything" for want of a peer having reported
  in yet), drops anything already expired, and installs every survivor via the new
  `SpillSink::install_new` with no value bytes read into RAM. A crash
  before a clean close, or a close with `warm_reopen` off, leaves no
  snapshot, so the next open is cold; a successful warm reopen deletes the
  snapshot it just replayed, so a second open with no intervening clean
  close is cold too. A node down longer than `tombstone_ttl` (10 minutes
  by default) also always falls back to the ordinary cold path, since no
  live peer is still guaranteed to hold the tombstones that would out-vote
  a resurrected stale record. A `Mode::Distributed` cache never serves a
  warm-reloaded bucket straight off replay: it starts both cold and
  unverified, a marker distinct from cold precisely because a hit in a
  warm-reloaded bucket is not automatically trustworthy the way a hit in
  an ordinary cold bucket always was (pulled from a donor or replicated in
  live), since a co-owner may have deleted the record during this node's
  downtime; both `Cache::fetch` and a peer's request for the bucket treat
  a local hit there the same as a miss while unverified is set. Clearing
  unverified is always the same decision, and the same call
  (`ResidencySet::mark_serving`/`mark_all_serving`), that clears cold for
  the bucket, never a separate one: an eager anti-entropy round against
  every live co-owner confirming the replayed data, or the ordinary
  cold-pull machinery landing fresh data for the bucket, verifies it and
  clears both together, the same events that already cleared cold for any
  other reconciled or pulled bucket. When there is no co-owner left to
  verify against instead -- a bucket found to have no co-owner at all, or
  one whose only co-owners never answer before the warm-up's attempts run
  out -- both marks clear on that decision too: the replayed data, already
  bounded by the `tombstone_ttl` downtime gate above, is the best available
  answer, and refusing local hits forever in a bucket whose local misses
  are already trusted is incoherent, not extra safety.
- `sundog_spill_reopen_total{cache, outcome, reason}` and
  `sundog_spill_reopen_records_total{cache}`: a `spill` cache's two new
  Prometheus metrics, the first incremented once per cache open naming
  whether it reopened warm or fell back cold and why (`reason` one of
  `disabled` for `SpillConfig::warm_reopen` off, `no_snapshot`,
  `stale_snapshot`, `config_mismatch`, `downtime_exceeded`, or
  `bad_region`; empty for `warm`), the second counting how many records a
  warm reopen actually replayed.

- `sundog_rebalance_buckets_total{cache, direction="served"}`: a donor
  credits every bucket of a rebalance pull stream that runs to its end,
  whatever the requester's protocol, so a node that serves no metrics of
  its own still leaves its landed pull visible on the node that donated.

### Changed

- The sim, fuzz, chaos and scale runs are weekly instead of nightly:
  `weekly-sim.yml`, `weekly-fuzz.yml`, `weekly-chaos.yml` and `scale.yml`
  run early Sunday UTC and on demand, and `release.yml` requires a green
  run of each weekly workflow and of `weekly-kani.yml` on the release
  commit.
- The distributed demo's restart reopens a node under the id it had, the
  way a deployment persists its node id, so ownership stays where it was
  and a warm spill reopen replays into buckets the node still owns. Its
  report carries `backlog_dropped_other_peers`, the replicate frames
  dropped toward any peer other than the node the run kills, and the scale
  gate bounds that at zero through `max_backlog_dropped_other_peers`; the
  frames the killed node's departure drops are counted in
  `backlog_dropped` and left to its restart to pull or reconcile.
- `sundog_rebalance_buckets_total{cache, direction="in"}` is now credited
  per bucket, the moment its own pull lands (via the new
  `Msg::StBucketDone`/`Msg::StBucketAck` signaling or, against an older
  peer, its transfer group's completion), instead of once for a whole
  multi-bucket transfer only after every bucket in it has landed.
- `sundog-testnode` shuts its cluster down and exits 0 on SIGTERM, which is
  what a container stop sends, so a spill tier opened with warm reopen on
  writes its checkpoint before the process ends and a restart against a
  preserved spill dir reopens warm. A shutdown that outlasts the container
  stop's grace is killed, and the next open falls back cold. `quit` and
  `crash` exit without leaving.

### Fixed

- A node whose `gossip_bind_addr` asks for port 0 probes a free port,
  releases it and lets chitchat bind it; when another socket takes the
  port in between, chitchat's address-in-use answer sends membership
  back to probe a fresh port, up to five times, instead of failing the
  join. A fixed port never moves: one another socket holds fails the
  join with that address in the error.
- `sundog_owned_buckets` is set for a distributed cache's first ownership
  view, at open, not only when a later membership change republishes the
  view. With the bounded membership wait at open, a node joining a settled
  cluster computes its final view first and may never republish, which
  left the gauge absent for that node.

## [0.6.1] – 2026-09-12

### Added

- **`crdt` module**: reference CRDT value types and the resolvers that merge
  them through `ConflictResolver::merge`, reachable as `sundog::crdt::{PnCounter,
  PnCounterResolver, OrSet, OrSetResolver}`. `PnCounter` is a per-node
  increment/decrement counter that merges by taking the componentwise
  maximum of each node's cumulative counts. `OrSet` is an observed-remove
  set whose `remove` tombstones only the add-tags it has seen,
  so a concurrent add of the same element survives a concurrent remove.
  Both encode canonically over `BTreeMap`/`BTreeSet` state, so two logically
  equal values always produce identical bytes, and both merge commutatively,
  associatively, and idempotently under any delivery order. `PnCounterResolver`
  and `OrSetResolver` merge two decodable values (a spilled stored side's
  real bytes included, read back off disk before the fold rather than
  treated as value-less) and fall back to plain `Hlc` order only against a
  tombstone, a spilled side whose bytes genuinely can't be read (no tier
  attached, or the read fails), or a decode failure.
- `ConflictResolver::merge` and `Merged { value, expires_at_ms }`: an
  additive pair on top of the existing `winner`-only contract, both
  defaulted (`merge` to `None`, `merges` to `false`), so no existing
  `ConflictResolver` implementation or match on `Winner` changes. A
  resolver's `merge` can fold the stored and incoming records into a third
  value instead of `winner` picking one of the two. `None` falls back to
  `winner`. The engine computes the merged record's version from the merged
  bytes themselves: a merge that reduces to one side outright adopts that
  side's own `(version, bytes)` pair verbatim (or is a no-op, if that side is
  already what's stored), and a merge that produces new content
  mints a version strictly ahead of both inputs under a `node` id computed
  from a hash of the merged bytes (`NodeId::merge_derived`), never a real
  node's id, so a minted version never collides with a real single-writer
  stamp, and two nodes minting for the same merged bytes always land on the
  same version regardless of fold order. `merge` is consulted only when both
  the stored and incoming records carry a value. Against a tombstone it is
  never consulted, and `winner` decides instead. A spilled stored side is
  not value-less on that account: the engine reads its bytes back off disk
  (via a prefetch pass keyed by spill location, ahead of the stripe lock on
  the common path) so a value-aware resolver merges against a spilled
  record's real content the same way it would a resident one. Only a side
  whose bytes genuinely can't be produced still falls back to `winner`. A
  redelivered record whose merge result reproduces the stored bytes and
  version is a no-op: nothing is re-applied, no event is published,
  and nothing is re-replicated.
- `ConflictResolver::merges` and `ShardOps::merges` (the latter forwarding a
  shard's own resolver's answer): whether a resolver's `merge` can ever
  return `Some`, `false` unless overridden and `true` on `PnCounterResolver` and
  `OrSetResolver`. `cluster::anti_entropy` reads it once per round and, when
  `true`, exchanges a version-mismatched key in both directions instead of
  only pushing the greater side to the lesser one, so two replicas each
  holding half of a merge converge in that one round rather than needing a
  second round to carry a minted result back to whichever side mints first.
  The partition-heal sim, rebuilt to drive `run_round_against` (sundog's
  real anti-entropy round, re-exported under `feature = "sim"` for this
  purpose) instead of a reimplementation, repairs a fully-conflicting partition
  split in 3 anti-entropy rounds against a per-writer-key `LwwResolver`
  control's 4, at both 2,000 and 20,000 keys, with round counts pinned
  non-decreasing in key count from 2,000 through 40,000. A partial conflict
  mix can still cost more total bytes than the control despite an equal
  round count. See `ROADMAP.md`'s "Merge resolvers" section.
- **`Engine::apply_many` pre-folds a batch that repeats a key.** When the
  resolver's `ConflictResolver::merges` is `true`, a batch is grouped by key
  into maximal runs of consecutive puts and each run long enough to fold
  collapses to one survivor via the resolver, outside the stripe
  lock, before applying through the ordinary per-entry path. Several
  entries for the same key now cost one real `apply_locked` call instead of
  one per entry. The survivor is seeded with the key's own real stored
  record when one exists, ahead of the run's own entries, so the batch's
  stored `(version, bytes)` comes out identical, `Hlc` included, to applying
  every entry one at a time. A peer's replicated fan-out batch and
  `Cache::insert_many` are where a real run appears. A singleton `insert`
  never contains one, and a non-merging resolver never triggers the fold.
- **`Cache::merge` and `CacheBuilder::merge_coalesce_window`.** `Cache::merge(key,
  value)` folds `value` into the resolver without a read. Left at
  the default zero window, every call applies (and replicates) at once,
  equivalent to `insert` under a merging resolver. With
  `merge_coalesce_window(Duration)` set to a nonzero window, consecutive
  `merge` calls to one key fold in memory and apply once, when the
  window that opened at the first of those calls elapses, so replication and
  every `Event` this key gets during the window see one record, not one per
  call. `Cache::get` never consults a pending fold: a value folded in but
  not yet flushed is invisible to a read for as long as it stays pending, up
  to one whole window. `Cache::close`, and dropping a cache's last handle,
  both flush whatever is still pending regardless of the window. The flush
  sweep itself is sharded the same way `engine::Engine`'s own stripes are:
  one independently locked `pending_merges` stripe per bucket, each with its
  own deadline-ordered index, rather than one shard-wide mutex a sweep had
  to scan in full on every tick, so a flush locks and pops only the entries
  due, in one stripe at a time, no matter how many keys are
  coalescing at once.
  `CacheBuilder::merge_coalesce_window` rejects a nonzero window on a cache
  whose resolver does not merge (`CacheError::MergeWindowRequiresMergingResolver`).
- **CRDT writer retirement.** `crdt::WriterId` pairs a node with the
  membership incarnation it wrote under, replacing the bare `NodeId`
  keys `PnCounter`'s `p`/`n` and `OrSet`'s tags used in 0.6.1's first
  `crdt` module: a restarted node's fresh incarnation gets its own slot
  rather than resuming or corrupting its pre-restart one.
  `PnCounter::compact`/`OrSetResolver::compact` (backing the new
  `ConflictResolver::compact`, defaulted to a no-op) retire a dead writer
  in two stages: moving its live state into a per-writer retired entry,
  then, once the cache is quiet and the retirement has aged past twice
  `CompactionBounds::retire_after_ms`, folding it away (into a bounded
  scalar for `PnCounter`, dropped outright for `OrSet`, whose `adds` a
  retired writer's own never-removed elements stay in indefinitely) and
  leaving a per-writer receipt in `folded_at`: the folded writer's own
  `since_ms`, recorded under its own key, so `merge` can tell a side that
  has folded writer `w` from a side that has merely folded some other
  writer whose retirement time happens to be later.
  **`ConflictResolver::settle`**, a new defaulted trait hook (`None` by
  default), drops that receipt once it is older than
  `CompactionBounds::receipt_ttl_ms`. A shard's `SettlingResolver` wrapper
  calls it on every merge apply, right after `ConflictResolver::merge`, so
  a receipt one replica's sweep has already pruned is dropped again the
  moment a peer's still-carrying copy merges it back in, rather than the
  two sides re-importing each other's receipt through anti-entropy
  forever.
  **`CompactionBounds { retire_after_ms, receipt_ttl_ms }`** replaces the
  bare `bound_ms` that `ConflictResolver::compact`, `ShardOps::compact_pass`,
  `PnCounter::compact`, and `OrSet::compact` took before: `receipt_ttl_ms`
  is the longer of three bounds and two bounds plus two sweep periods, so
  a receipt always outlives every reachable replica's own fold of the
  same writer plus one anti-entropy exchange. `ClusterConfig::crdt_compaction_bounds()`
  derives it from `crdt_retire_after` and the new `crdt_sweep_period()`
  (in turn the new **`crdt_sweep_interval: Option<Duration>`** field if
  set, else a quarter of `crdt_retire_after` floored at 30s). A compacted
  record's version is now its stale version's successor under a
  content-hash-derived node tiebreaker, the same scheme `merge`'s own
  minted versions use: two replicas compacting identical bytes mint
  identical versions, so anti-entropy has nothing left to exchange for
  that key, while two that compact to different bytes (one has judged a
  writer dead the other has not yet) mint different versions and do
  exchange, converging through the ordinary `merge`-plus-`settle` path
  like any other divergent write; compaction is not invisible to
  anti-entropy, only usually redundant with it. One tick's sweep
  (`crdt_compact_task`, a plain interval with an immediate first tick;
  replicas need no alignment across nodes, since `settle` and the
  content-derived version make eventual agreement enough on their own)
  now examines the whole keyspace, calling `ShardOps::compact_pass`
  repeatedly at `crdt_compact_batch` records per call and yielding between
  calls, so `crdt_compact_batch` bounds the stall per call rather than the
  work a whole tick does.
  `PnCounter`'s `{p, n, retired, folded_p, folded_n, folded_at}` and
  `OrSet`'s `{adds, seen, retired, folded_at}` are each one plain
  `#[derive(Serialize, Deserialize)]` layout, encoded and decoded by a
  thin postcard wrapper. These are this crate's own record layouts, and
  no released node predates them, so there is nothing for a
  wire-compatible encoding to protect and no protocol gate on when the
  sweep may run: the CRDT compaction sweep runs for a cache whenever its
  resolver merges at all (`ConflictResolver::merges`), gated only by the
  per-writer dead/quiet predicates below. A future change to either
  layout bumps `wire::PROTOCOL_VERSION` and is versioned then, the same as
  any other wire change. `ClusterConfig::crdt_retire_after` (default:
  `tombstone_max_ttl`, 24h) and `crdt_compact_batch` (default 4,096) size
  the retirement window and the sweep's per-call record budget.
  `sundog_crdt_retired_writers_total{cache}` counts writers the sweep's
  scan found eligible for retirement, which can run ahead of
  `sundog_crdt_compactions_total{cache}`'s count of records actually
  rewritten: a writer counts the moment the scan judges it eligible, even
  for a record the pass skips without rewriting (an unowned bucket, or
  one that changed underneath the scan). The sweep runs at
  `crdt_sweep_period()` cadence, a quarter of `crdt_retire_after` floored
  at 30s by default. Release 0.6.1 interoperates with 0.6.0 on every cache
  except the two new ones (`PnCounter`/`OrSet` themselves are new in
  0.6.1, so a 0.6.0 peer never opens one).

### Changed

- `PnCounter` and `OrSet` key every writer's slot by `crdt::WriterId {
  node, incarnation }` instead of a bare `NodeId`, and `PnCounter::local_delta`/
  `PnCounter::local_decrement`/`OrSet::add` take a `WriterId` where they
  previously took a `NodeId`. Neither type nor these signatures had
  shipped in a release before this one, so this is not a breaking change.
  It exists so a restarted node's fresh membership incarnation gets a slot
  of its own rather than resuming, or corrupting, the one its pre-restart
  process wrote to.
- **The retirement contract, stated plainly.** A writer is retirement-
  eligible on a node once it is confirmed dead there (gone longer than
  `ClusterConfig::crdt_retire_after`, whether it crashed or left through
  `Cluster::shutdown`, or superseded by a live incarnation of the same
  node) and the cache is quiet: `membership::member_is_quiet` counts
  every other member sharing it as settled once it has been continuously
  present for `crdt_retire_after`, continuously gone for
  `crdt_retire_after`, or merely known for twice `crdt_retire_after`
  however often it has flapped between the two in that time, safe
  because any copy such a flapping member can still bring back is at most
  one bound old, and reconciles through the retired entry the fold
  replaced or the fold receipt it left behind. A graceful leaver still
  never holds tombstone collection back; only writer retirement treats it
  like a crash. A gone member stays known to the sweep until it returns
  or `AbsenceTracker::prune_gone_older_than` forgets it, run at the
  start of every tick, past the fold receipt lifetime, the same age past
  which no receipt reconciles a straggling copy of that member's writer
  either, so retiring it any later could only double count; the lifetime
  is at least two sweep periods, so a member that left between ticks is
  seen gone by the next tick before it is forgotten; this is what
  bounds the tracker under sustained restarts, where every crashed process
  leaves behind a node id (random per process) that never comes back. The
  sweep works one stripe per tick, and a writer retired in one record must
  retire in every other. Stage one moves the writer's contribution into
  per-writer retired state at that point, exact regardless of how stale
  any replica's view of any other replica is. Stage two only runs once
  that retirement has itself aged past a second `crdt_retire_after`
  (`2 * crdt_retire_after` total) with the cache still quiet, and it is
  exact only up to the receipt lifetime `CompactionBounds` derives: a
  replica isolated for longer than that, still holding a live slot for a
  writer every other replica has already folded away, double-counts that
  writer's contribution once it reconnects (an `OrSet` writer's
  already-removed elements resurrect the same way). That is the same
  trust boundary `tombstone_max_ttl` already accepts for a member gone
  that long, not a new one.

### Fixed

- A removed key could come back in a `Mode::Distributed` cache after a
  bucket moved under load. A node that lost a bucket keeps a copy that no
  removal reaches until its hand-off round, up to twice the disown grace
  later, and that round pushes whatever the owners lack; once the owners'
  tombstone was collected, the stale copy resurrected the key on both.
  Opening a `Mode::Distributed` cache now rejects a `tombstone_ttl` shorter
  than `ClusterConfig::bucket_release_window`,
  `ae_interval * (2 * distributed_disown_grace_rounds + 2)`, with
  `CacheError::TombstoneTtlInsideReleaseWindow`, and a released bucket
  still resident when the retention runs out is dropped without a
  hand-off, since its copy can no longer be trusted not to resurrect a
  removal. The library defaults already satisfy the rule; the distributed
  demo's 15-second retention did not and is 60 seconds, and the test node's
  10-second retention is 20 seconds.
- The distributed demo expected `owners` copies of every key even with
  fewer live nodes than owners, so a one-node run never converged; the
  expectation is `min(owners, live)` copies. Its status line says when the
  load is still running, since the sum settles only once the load pauses,
  and a diverged headless report lists per node the keys held in buckets
  it does not own, how many nodes hold each key, and removed keys some
  node still holds.

## [0.6.0] – 2026-09-07

### Added

- **Distribution mode**: `Mode::Distributed { owners }` and
  `Mode::distributed()` / `Mode::DEFAULT_OWNERS`. Each cache key lives on
  `owners` live nodes, chosen by rendezvous hashing over the cache's
  live, protocol-3 peers advertising it under the same mode and owner count.
  `Cache::fetch` reads a key from an owner (local if this node owns its
  bucket, otherwise the network), returning `Ok(None)` for a genuine miss.
  `Cache::owners_of` reports a key's current owners in rendezvous order. A
  write for a bucket this node doesn't own is forwarded to that bucket's
  owners and never applied locally.
- `ClusterConfig::distributed_disown_grace_rounds` (default 3): anti-entropy
  intervals a node keeps a disowned bucket's data resident before handing it
  to each new owner in one anti-entropy round and releasing it, so the new
  owner's rebalance pull, or that hand-off, has landed before the data
  goes. `ClusterConfig::fetch_timeout` (default 750ms): per-owner-attempt
  timeout for `Cache::fetch`. `ClusterConfig::rebalance_concurrency`
  (default 4): maximum simultaneous bucket-transfer streams one rebalance
  pass opens.
- `CacheError::TooFewOwners`: `CacheBuilder::open` rejects a `Mode::Distributed`
  cache with `owners` under 2. `CacheError::FetchUnavailable`: every owner of
  a `Cache::fetch`'s key was unreachable or timed out.
- Six new metrics: `sundog_owned_buckets{cache}`, `sundog_rebalance_buckets_total{
  cache, direction}` (`in`/`out`), `sundog_fetch_total{cache, outcome}`
  (`local`/`remote`/`miss`/`error`), `sundog_forwarded_writes_total{cache}`,
  `sundog_stale_view_total{cache}`, and `sundog_unowned_inbound_dropped_total{
  cache}`.
- Wire messages `Fetch`, `FetchReply`, `FetchDeclined`, `AeDigestScoped`,
  `StBuckets`, `StBucketChunk`, `ForwardBatch`, and `StaleView`, all gated on
  the peer's protocol from its hello: a peer speaking less than
  `wire::PROTOCOL_DISTRIBUTED` never receives one. A distributed fan-out
  travels as `ForwardBatch`, stamped with the writer's ownership view hash.
  A receiver whose own view differs re-forwards the batch once more to the
  records' owners under its view, so a write routed under a stale view (a
  delete issued before the writer saw a replacement owner join, say) still
  reaches every current owner instead of waiting on an anti-entropy round
  to pair the two, and never resurfaces after the tombstone is collected.
- `sundog-testnode` reads `SUNDOG_TESTNODE_MODE=distributed` (with
  `SUNDOG_TESTNODE_OWNERS` to pick `owners`) to open `"it"` as a distributed
  cache, and serves the routes the new container scenarios drive it through.
- A cache close or cluster shutdown seals every fan-out queue first, finishes
  the fan-out batch in flight, drains the backlog, and flushes queued frames
  to their peers before cancelling the writers, so a write accepted before
  shutdown still reaches its owners. A forwarded write arriving after that
  fails with the new `CacheError::Closed` instead of being accepted and never
  sent, while a write the node applies itself still lands as a detached local
  write. A `Mode::Distributed` fan-out waits for outbox space,
  bounded, instead of dropping a forwarded write on overflow.
- Five `Mode::Distributed` scenarios in the deterministic simulation suite:
  rebalance under membership churn with message loss and reordering, a
  partition healing back to one ownership view with every write kept, one
  of a bucket's two owners lost with nothing lost, a property check that
  no node ever holds a bucket outside its own owned-or-releasing set, and a
  releasing bucket still answering anti-entropy while refusing a fresh
  apply. New
  container scenarios: a five-node fill landing every key on two
  owners, one owner crashing with every key still fetchable and then
  re-owned, and a fourth node joining a filled cluster and taking its share,
  plus the chaos lane's `chaos_distributed_crashes_churn_and_drops_still_converge`.
- `demos/sundog-demo` moved out of the repository root into `demos/`, next
  to the new distributed demo below.
- **`sundog-distributed-demo`**: a `ratatui` demo for `Mode::Distributed`, a
  sibling of `sundog-demo`. Preloads a large key set (two million `k{i}` =
  `v{i}` pairs unless overridden) across N in-process nodes in batches spread
  round-robin via `Cache::insert_many`, then runs a steady write load plus
  random `fetch` sampling against it. The TUI shows a preload progress bar,
  each node's entry count and estimated owned-bucket share, and
  cluster-wide fetch hit/miss/error counts and latency, with the same
  interactive kill/restart/pause controls as the chaos demo. Its
  `--headless <SECS>` mode kills and restarts a node mid-run to exercise
  rebalance, then checks the sum of live nodes' entry counts against
  `owners * surviving keys` and a random sample of surviving keys against
  their expected value.

### Changed

- **Breaking**: `Mode` is `#[non_exhaustive]` and gains the struct variant
  `Distributed`. A downstream `match` without a wildcard arm needs one added,
  and a numeric cast of a `Mode` (`as isize`) no longer compiles.
- `wire::Msg` gains the six variants above, already `#[non_exhaustive]` since
  0.3.0, so no downstream `match` needs a change for them.
- `wire::PROTOCOL_VERSION` is 3. The current release interoperates with
  protocol 2.

## [0.5.0] – 2026-09-06

### Added

- `spill` feature: a local SSD/NVMe spill tier. `CacheBuilder::spill(SpillConfig::new(dir,
  capacity_bytes))` lets eviction demote cold entries onto a FIFO ring of
  region files on disk instead of discarding them, extending a cache's
  effective size past its RAM budget. A later read promotes a spilled entry
  back into RAM. `SpillConfig::region_bytes` and `SpillConfig::read_concurrency`
  tune the region file size (64 MiB default) and how many spilled-value reads
  run at once (16 default). Inactive unless a cache opts in, with no effect on
  a non-`spill` build. With `spill` set up, `Mode::Replicated` accepts a finite `max_capacity`.
  Eviction demotes rather than deletes, so anti-entropy does not need to
  silently re-pull evicted entries back. `tti` stays rejected for `Replicated`
  regardless, since it is local-only by design.
- **Container coverage for the spill tier**: `sundog-testnode` is now built with
  the `spill` and `prometheus` features in every container run, reads
  `SUNDOG_TESTNODE_SPILL_DIR`/`SUNDOG_TESTNODE_SPILL_CAPACITY_BYTES`/
  `SUNDOG_TESTNODE_SPILL_REGION_BYTES`/`SUNDOG_TESTNODE_MAX_CAPACITY_BYTES` to
  open `"it"` with a spill tier and a byte-counting weigher, and serves
  `GET /metrics`. Two new scenarios in `tests/containers.rs`:
  `replicated_cluster_serves_spilled_entries_and_settles_without_repair_loops`
  drives a three-node `Mode::Replicated` cluster through a tiny RAM budget on
  one node and checks every key still reads correctly, resident or spilled,
  and that anti-entropy settles with no repair loop. `spilling_node_survives_
  a_restart_and_rewarms_from_peers` restarts the spilling node and confirms
  its tier starts empty (discarded, not resumed) before rewarming from its
  peers and resuming disk-backed reads.

### Changed

- **Breaking**: `store::Stored` is gone. Its fields moved into the engine's
  own entry representation.
- **Breaking**: `CacheError` is `#[non_exhaustive]`, so a downstream `match`
  without a wildcard arm needs one added.

## [0.4.1] – 2026-09-05

### Added

- `ClusterConfig::advertise_ip`: the address a node advertises for gossip
  and the data plane, for NAT and container port mappings. Unset, the
  outbound-interface probe now falls back to the first routable interface
  address, then loopback, instead of failing `ClusterBuilder::build`.
- `ClusterBuilder::node_id` and `NodeId: FromStr`: a persisted identity, so a
  restarted process rejoins as the same member. `Cluster::shutdown` gossips a
  graceful departure first, and a member that leaves this way no longer holds
  tombstone garbage collection back for `tombstone_max_ttl`. Only a crash
  does.
- `Cluster::is_ready` and `Cluster::health`: ready once every open
  `Mode::Replicated` cache has finished warming. With the `prometheus`
  feature the metrics listener also serves `GET /readyz` (200 or 503) and
  `GET /healthz`.
- `Cache::get_sync`, `contains_key_sync`, `insert_sync`, and `remove_sync`,
  and the same on `Shard`: the operations without an async runtime, with the
  same hit and miss counting, fan-out, and events as the async ones.
- `Cache::for_each_key` and `Shard::for_each_key`: a visitor over this node's
  live keys that never holds every key in one `Vec`.
- `Cache::close`: stops the cache's background tasks, drops it from the
  registry, and erases its gossiped mode, so the name opens again at once. A
  clone kept past `close` keeps working as a local, detached cache.

### Fixed

- A `Mode::Local` cache no longer queues every written key for a fan-out task
  it never runs. That queue grew without bound with the write count.
- `SUNDOG_SEEDS` selects static discovery on its own. Before, a build without
  `.seeds()` ignored it and browsed mDNS.
- A pooled request connection idle for more than 30 s is dropped instead of
  reused, and one the peer has already closed is retried on a fresh dial
  rather than failing the anti-entropy round.

### Changed

- The live-entry gauge reads one atomic counter instead of taking every
  stripe lock on each tick, and capacity eviction removes up to eight cold
  entries per stripe lock instead of one.
- `serde`'s `rc` feature is on, so `Arc` and `Rc` values serialize.

## [0.4.0] – 2026-09-04

### Fixed

- A node declines to donate a state-transfer snapshot of a cache until its
  own transfer of that cache has completed, answering `Msg::StUnavailable`.
  A joiner tries every live peer in turn and, once its snapshot lands, runs
  one anti-entropy round against every live peer, not only its donor. A
  node with no peer in sight at `open()` waits a fifth of
  `state_transfer_budget`, 4 s unless overridden, for gossip to show one before it
  opens as the origin. A node that came up before gossip found any peer
  could donate an empty or half-warm copy to the next joiner, and a chain of
  such joins with the last complete holder crashing lost live entries
  cluster-wide.

### Added

- **Hierarchical anti-entropy digests**: each of the 1,024 anti-entropy
  buckets now also keeps 64 part digests, the next 6 hash bits below the
  bucket's own 10. A mismatched bucket past
  `ClusterConfig::ae_part_min_bucket` entries, default 4,096, answers with its
  64 part digests instead of a full listing or an IBLT sketch, without
  building the listing. A mismatched part then follows the existing
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
  one node at a time. A container test runs the 0.3.1 node against this one
  in both directions.
- **Breaking**, in the `sim`-feature test seams and the wire types only:
  `net::AeMismatch` gains the `PartDigests` variant and is
  `#[non_exhaustive]` from here on, as is the new `net::AePartReply`.
  `net::RequestHandler` and `store::ShardOps` gain the required methods
  `bucket_lens`, `part_digests`, and `entries_for_parts`.
  `wire::Msg::Hello` gains the `protocol` field and `membership::Peer` the
  `protocol` field. `cargo semver-checks --release-type minor` against
  0.3.1 reports these findings. Every other check passes.
- **Chaos lane** for the container suite: `chaos_crashes_churn_and_drops_still_converge`
  drives a four-node cluster through a seeded random mix of crashes, churn,
  dropped keys, refills, and put bursts for a bounded time, then checks every
  node converges to the same content. Gated on `SUNDOG_CONTAINER_TESTS=1` and
  `SUNDOG_CHAOS_SECS`. `SUNDOG_CHAOS_SEED` replays a specific run. Runs for 45s
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
  per-entry TTL example. A cache is typed once at open.

## [0.3.0] – 2026-09-03

### Added

- **A store engine built for sundog**: live entries and tombstones share 1,024 lock-striped
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
  Wire cost is fixed regardless of bucket size. It decodes symmetric differences
  up to 100 elements successfully in 99 of 100 cases and falls back to the full listing
  otherwise. New wire messages `Msg::AeSketch`, `Msg::AeEntries`, and
  `Msg::AePullHashes` carry the exchange and its fallbacks. New metric
  `sundog_ae_sketch_total{cache, outcome}` counts `decoded` vs `fallback`
  outcomes.
- **Stateful fuzzing of the apply path**: two `cargo-fuzz` targets,
  `apply_model` and `apply_permutation` (`sundog/fuzz`), drive coverage-guided,
  sequence-generated local writes, remote applies and batches, invalidations,
  tombstone GC, and clock advances through a real `Shard`, mirrored against a
  reference model of the same semantics, checking the permutation-convergence invariant
  under libFuzzer's own mutation instead of proptest's sampling. The model,
  `sundog::store::model`, `#[doc(hidden)]` behind the `fuzzing` feature or
  `cfg(test)`, is shared with the in-crate property test
  `shard_matches_the_reference_model_under_arbitrary_op_sequences`.

### Changed

- `moka` is no longer a dependency. Size-bounded eviction (`max_capacity`,
  `weigher`) is sampled LRU: a write that pushes total weight past the cap
  evicts whichever of eight entries sampled from a rotating offset in one
  stripe was read longest ago, repeating until the cap holds. TTI stays a local per-entry idle
  deadline. Neither is available in `Replicated` mode.
- The hand-off from a local write to the fan-out routine is a lossless queue of
  pending keys drained whole, replacing a bounded broadcast channel that lagged
  under a burst of single inserts and left the gap to anti-entropy. A burst of
  any size costs one drain. `insert_many` and `remove_many` hand their keys
  off one full replicate batch at a time, so a fill costs a bounded number
  of frames per peer whatever the machine's speed.
- Anti-entropy pull replies (`AePull`, `AePullHashes`) travel as
  `ReplicateBatch` frames under the same byte and count budget as the live
  fan-out, replacing one `Replicate` frame per record. A 100k-record repair is a
  few dozen frames.
- Anti-entropy skips a peer while replicate traffic with it is still in motion:
  frames queued or sent toward it in the last `ae_interval`, or a batch
  received from it in that same window, for at most three rounds running. This
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
  apiece. `cargo semver-checks --release-type minor` reports this one
  intentional break. Every other check passes.

### Fixed

- `insert_many` and `insert_many_with_ttl` apply the entries before an
  oversized value and then return `ValueTooLarge`, as their docs state.
  Every entry was rejected before.
- `DnsSrv` discovery uses its fallback port for an SRV record
  whose port is zero instead of dialing port zero.

## [0.2.0] – 2026-09-02

### Added

- **Cache-config fingerprint gossip**: every node advertises the mode of each
  open cache in its membership state. `open()` on a name a live peer already
  runs under a different `Mode` fails with `CacheError::ModeMismatch { cache,
  local, remote }`. `Mode::Local` counts too, since a private cache and a
  replicated one can't share a name. A conflict that slips past the open-time
  check, two nodes opening at the same instant, is reported loudly when the
  peer's advertisement arrives. TTL and capacity stay local knobs.
- **API surface**:
  - `Cache::contains_key`, expiry-aware and not counted as a read.
  - `Cache::keys`, a point-in-time local snapshot.
  - `Cache::remove_many`, the tombstone counterpart of `insert_many`, one lock
    acquisition per stripe and one `Removed` event per key, batched fan-out.
  - `Cache::clear`, which tombstones every key this node holds and fans them
    out at O(entries), and in `Replicated` mode empties the cluster once
    tombstones land.
  - `Cache::get_or_insert_with`, an infallible-loader `get_or_load` with the
    same stampede collapse.

  The `Shard` API gains the same methods.
- **Per-cache metrics**: `sundog_cache_hits_total{cache}` and
  `sundog_cache_misses_total{cache}`. A miss is one loader execution or one
  empty `get`. Collapsed waiters count as hits. `contains_key` counts as
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
  replicates the same way a default-TTL stamp does, so the entry expires at the same
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

- **Discovery** (`sundog::discovery`):
  - `Mdns`, zeroconf default via `mdns-sd`.
  - `Static`, fixed or env-var seed list re-resolved on an interval.
  - `DnsSrv`, SRV-record polling for Kubernetes headless services with an
    A/AAAA fallback.

  All three stream candidates continuously, so a full-cluster cold restart still
  re-converges.
- **Membership** (`sundog::membership`): gossip membership on `chitchat`,
  advertising each node's data-plane address and incarnation. A `watch` stream
  of the live peer set drives everything downstream.
- **Data plane** (`sundog::net`):
  - A lazy TCP mesh, one connection per live peer, `LengthDelimitedCodec`-framed
    with a 4 MiB frame cap.
  - Per-class bounded outboxes with a documented drop policy: `Invalidate` drops
    oldest, `Replicate` drops newest and marks the peer dirty for anti-entropy
    priority.
  - `StRequest`/`AeDigest`/`AePull` request-response paths off the broadcast
    channel.
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
  ones. Client certificates are verified too, mutual auth.
- **Cluster/cache public API** (`sundog::cluster`, `sundog::cache`):
  - `Cluster::builder(name).build()` as the zero-config zeroconf happy path.
  - `Cluster::cache::<K, V>(name)` builder with `.mode()`, `.max_capacity()`,
    `.ttl()`, `.tti()`, `.resolver()`, `.weigher()`.
  - `Cache<K, V>` with `get`, `get_or_load`, stampede-collapsing read-through,
    `insert`, `remove`, `entry_count`, live local count and housekeeping
    flushed, `invalidate_local`, and an `events()` broadcast stream of
    `Created`/`Updated`/`Removed`, each tagged with its `Origin`.
- **Three cache modes**:
  - `Local`, no cluster traffic.
  - `Invalidation`, the default, independent local copies with writes
    broadcasting an invalidate.
  - `Replicated`, full copy per node, writes broadcast the value.
- **State transfer**: a newly opened `Replicated` cache pulls a full snapshot
  from the lowest-node-id live donor before `open()` returns, then runs one
  immediate anti-entropy round against that donor as a safety sweep. Donor death
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
  anti-entropy rounds, and drops. `metrics` counters and gauges
  (`sundog_backlog_dropped_total{peer}`, `sundog_live_peers`,
  `sundog_open_caches`) emitted unconditionally, independent of the `prometheus`
  feature.
- **`prometheus` feature**, off by default: `metrics-exporter-prometheus` wired
  two ways, `ClusterBuilder::prometheus_listen(addr)` serves `GET /metrics`
  itself, and `telemetry::prometheus_handle()` installs a recorder for
  embedding into a caller-owned HTTP server.
- **`sim` feature**, off by default: swaps the data plane's transport seam
  (`net::tcp`) to `turmoil::net`, enabling `tests/sim.rs`'s deterministic
  simulation suite, partition/heal convergence, loss/reorder/dup storms, donor
  crash mid-state-transfer, with no real UDP/TCP involved.
- **`sundog-demo`**: a `ratatui` chaos-testing TUI, N in-process nodes over
  static loopback seeds, a background write load, interactive kill/restart per
  node, live replication/anti-entropy visibility, plus a `--headless <SECS>`
  mode for CI smoke checks and manual soak runs.
- **Test suite**:
  - Property tests (`proptest`) on HLC, wire encoding, and the store, including
    the permutation-convergence property, the correctness argument for the
    whole loss-tolerant design.
  - The `turmoil` simulation suite.
  - A container-backed multi-node integration suite, see next bullet.
  - Two-node loopback integration tests living as ordinary unit tests next to
    the code they exercise (`sundog::cluster`'s
    replication/invalidation/state-transfer/anti-entropy/local-mode tests,
    `sundog::store`'s read-through stampede-collapse and TTL-expiry tests).
  - Prometheus-exporter and TLS integration tests.
  - Unit tests in every module.
- **`sundog-testnode`** (new workspace member): a tiny static-musl binary that
  opens `Mode::Replicated` `sundog` caches and exposes them over a line-based
  control protocol, `put`/`get`/`del`/`count`/`peers`/`quit`, plus bulk-fill,
  high-frequency churn, and large-value content-check commands, so a test
  process outside the crate can control a real cluster member as a separate process.
- **Container-backed integration suite** (`sundog/tests/containers.rs`,
  `sundog/tests/container_smoke.rs`, driver code in `sundog/tests/container_util`):
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
  installed. It needs `RIGHTSIZE_BACKEND=docker` because sundog's gossip is UDP and
  rightsize's microsandbox network emulation relays TCP only.
- CI: the main `ci` job runs `cargo fmt --check`, then `cargo clippy
  --workspace --all-targets -D warnings -W clippy::pedantic`, plus a
  `sim`-feature pass and a `tls,prometheus`-features pass, `cargo test
  --workspace`, then the `turmoil` simulation suite and the `tls,prometheus`
  feature tests as later steps in that same job. The container suite runs as
  its own separate job, musl target and `musl-tools` installed,
  `RIGHTSIZE_BACKEND=docker` and `SUNDOG_CONTAINER_TESTS=1` set, default base
  image pulls fine on a hosted runner. A nightly `nightly-sim` workflow runs the
  simulation suite under a fresh random seed (`SUNDOG_SIM_SEED`), logging the
  seed for local replay.
- **Grafana dashboard** (`ops/grafana-dashboard.json`): panels for live peers,
  open caches, per-peer backlog drops, anti-entropy repair rate, and
  state-transfer throughput.
- **`Cache::insert_many`/`Shard::insert_many`**: bulk local writes applied under
  one acquisition of the store's apply lock rather than one per entry. Each
  entry still gets its own `Hlc` stamp and its own `Event`. Fan-out
  notifications for a bulk fill travel as per-stripe key batches on the internal
  channel (`store::FanOutNotice::Many`) rather than one notice per entry, so an
  arbitrarily large fill can never lag the fan-out channel and degrade its
  replication to anti-entropy repair. A 100k-entry fill converges on live
  peers a fraction of a second after the call returns, in a few hundred wire
  frames.
- **Batched replication on the wire**: the fan-out layer pre-batches each
  drained burst of local writes into `Msg::ReplicateBatch` frames, budget- and
  count-capped, so a bulk burst occupies outbox slots per *batch* rather than
  per record. `net::conn`'s per-peer writer opportunistically coalesces whatever
  is queued, single `Replicate`s and pre-built batches alike, into fuller frames
  with no added latency, only what's already queued by the time the writer
  drains. Anti-entropy repair pushes travel through the same budgeted batching
  instead of one `Replicate` message per repaired record.
  `ShardOps::apply_remote_batch` applies a whole batch, a coalesced wire frame,
  a state-transfer chunk, or an anti-entropy pull, under one lock acquisition.
  The permutation-convergence property test mixes single and batch applies as
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
  intermediate buffer. Decoding slices `Bytes` views out of the
  received frame with no payload copy. A stored record keeps its encoded wire
  bytes (`store::Stored::encoded`) next to its typed value, so answering a
  replication or anti-entropy request clones an existing `Bytes` handle rather
  than re-serializing. Control messages (`Hello`, `StRequest`, `AeDigest`,
  `AeBucket`, `AePull`, `ReqDone`) still encode as postcard.
- **Connection reuse for anti-entropy and state transfer**: `Mesh::ae_round`,
  `ae_pull`, and `request_state` check out an idle, already-`Hello`'d connection
  from a small per-peer pool instead of dialing fresh, and under `tls`,
  completing a fresh mutual-cert handshake, on every call, falling back to a
  fresh dial when the pool is empty or a pooled connection turns out dead. The
  accept side serves multiple requests per connection instead of one,
  torn down after an idle timeout or a request-count cap. This pool is separate
  from the persistent per-peer broadcast connection, so a slow snapshot transfer
  can't back up live replication traffic. A connection is only ever returned to
  the pool once a reply completes without error. One left in an unknown framing state
  after an error, timeout, or cancellation is dropped instead of reused.
- **Striped apply lock**: each shard's tombstone map and write-serialization
  lock is split into 64 independent key-hash stripes instead of one lock per
  shard. Writes to keys in different stripes apply concurrently. Writes to
  the same key stay serialized against each other the same way a single shard-wide
  lock would. Remote batch applies and local bulk inserts group their entries by
  stripe and apply each stripe's sub-batch under one acquisition of that
  stripe's lock.
- **Lean fan-out**: local writes post to the peer fan-out path over an internal
  keys-only channel (`store::FanOutNotice`, single keys for ordinary writes,
  per-stripe key batches for bulk fills, and no message at all for remote
  applies), separate from the public `Cache::events()` broadcast channel. The app-facing `Event`,
  which owns a clone of the value, is only built when `events()` has a
  subscriber, so a cache with nothing subscribed to `events()` pays no per-write
  value clone for replication or invalidation fan-out.
- **Partition-aware tombstone retention**: `ClusterConfig::tombstone_max_ttl`,
  24 hours unless overridden, bounds a new deferral in the tombstone GC sweep: a
  tombstone past `tombstone_ttl` is kept, not collected, while any member
  seen inside that window is currently absent, up to the hard cap. This closes the
  resurrection window where a member absent longer than `tombstone_ttl` could
  bring a manually deleted key back to life on the nodes that stayed up. A
  member gone longer than `tombstone_max_ttl` is the one case that can still
  resurrect a key. Deferred tombstones stay counted in the anti-entropy digest
  until they're collected, so digests and the tombstone set never drift
  out of sync.

### Known gaps (tracked, not bugs)

- `CacheError::ModeMismatch` exists as a reserved error variant but nothing
  detects a real mode disagreement between nodes for the same cache name. Closed
  in 0.2.0 by cache-config fingerprint gossip.

# CRDT merge resolvers

`ConflictResolver::winner` no longer only picks one of the stored or incoming
record: `Winner::Merged { value, expires_at_ms }` lets a resolver fold both
into a third value, so two concurrent writes to the same key combine instead
of one silently overwriting the other. `sundog::crdt` ships two resolvers
built on this — `PnCounter`/`PnCounterResolver` for an increment/decrement
counter and `OrSet`/`OrSetResolver` for an observed-remove set — as reference
implementations of the contract, not the only merge types a cache can use.
`ROADMAP.md`'s "Merge resolvers" section under Next covers the full design,
including the open provenance limitation this document's benchmark run
exercises directly.

## API and engine change

`Winner` gains a `#[non_exhaustive]` `Merged` variant carrying the merged
`Bytes` and the resolver's own choice of TTL — the engine never infers a TTL
from either input. `ConflictResolver`'s doc contract states the join-semilattice
obligation on a `Merged`-capable resolver: `merge` must be commutative,
associative under arbitrary pairwise fold order, and idempotent, since
`apply_locked` only ever folds one collision at a time and any two replicas
can fold the same concurrent writes in different orders.

The engine enforces the one invariant a resolver's convention alone can't
guarantee: `Merged` is only honored when both the stored and incoming records
carry a value. Against a tombstone or a spill-degraded side, `resolve_conflict`
degrades to keeping the existing record — a resolver that misbehaves can never
resurrect a deleted key or fabricate content against a spilled record. The
version an accepted merge lands under is computed inside the engine, never by
the resolver: `merge_version` takes the componentwise maximum of the stored
and incoming `Hlc`'s `wall_ms` and `logical` fields under a reserved sentinel
node id (`NodeId::MERGE_SENTINEL`), so two nodes that fold the same two inputs
land on byte-identical stamps regardless of arrival order, and a merge result
never collides with a real single-writer stamp. `resolve_and_rebind` treats a
redelivered merge as a true no-op — nothing re-applied, no event published,
nothing re-replicated — whenever the merge reproduces the stored bytes under
the stored version exactly; a resolver-driven merge whose computed version
happens to collapse onto the stored version but whose bytes differ still
stores the new content, since rejecting it would silently drop a legitimate
concurrent write.

## Convergence argument

Componentwise max is commutative and associative field-by-field, independent
of grouping or fold order, so any two replicas folding the same concurrent
writes pairwise — in any order, with any duplication — reach the identical
stamped version. The sentinel node id guarantees a merge result never equals
a real write's stamp (equality on `Hlc` requires all three fields to match),
and it makes a merge result strictly dominate both of its inputs under `Hlc`'s
existing `Ord`, so anti-entropy's push/pull routing always treats a merge as
the newer side to fetch. Folding a merge result back in with one of its own
inputs reproduces the same stamp exactly, which is what stops a redelivered,
already-absorbed record from re-publishing forever.

The one gap this argument leaves open, stated plainly rather than glossed
over: `merge_version` depends only on the two inputs' versions, never on the
merged content. Componentwise max is not injective — it keeps the field-wise
maxima and discards which input contributed them — so two different pairs of
concurrent writes can fold to the identical `(wall_ms, logical)` once the
sentinel erases the node field that would otherwise disambiguate them. When a
merge's computed version collapses onto the version already stored, the
engine still stores the correct merged bytes locally, but `entry_fingerprint`
and every anti-entropy digest are functions of the version alone, never the
value — so a peer whose digest already matches sees nothing to fetch and never
receives content that changed underneath an unchanged version. This is a
known, documented gap in the current scheme (`ROADMAP.md`), not an
implementation bug, and the benchmark run below reproduces it directly under
real anti-entropy rather than only arguing it in the abstract.

## Property suite

Four tiers, mirroring the levels the crate already tests resolvers at:

- **Value algebra**, no `Shard`: `PnCounter` and `OrSet<String>` each carry
  `proptest` coverage for `merge` commutativity, idempotence (byte-for-byte),
  and three-way associativity across all six foldings, plus targeted tests
  for the per-node max semantics (`local_delta` overwrites, it doesn't sum,
  which is why a writer must track its own cumulative total) and the
  observed-remove scope rule (a concurrent add survives a concurrent remove
  of the same element).
- **`merge_version` combinator**, in `engine.rs`: commutativity, three-way
  associativity across all six orderings, that a merge result never carries
  a real node id and strictly dominates both inputs under `Hlc`'s `Ord`, and
  the fixed-point property a redelivery no-op depends on — folding a merge
  result back in with one of its own inputs reproduces it exactly.
- **Resolver and engine-guard level**: both `PnCounterResolver` and
  `OrSetResolver` merge regardless of argument order and fall back to plain
  `Hlc` order against a tombstone, a spilled side, or a decode failure;
  `resolve_conflict`'s tombstone/spill guard is exercised directly, along
  with a decode-failure path that rejects rather than panics.
- **Shard and cluster integration**: a property test replays an arbitrary
  multiset of per-origin `PnCounter` increments to one key under permutation,
  duplication, and mixed single/batch/concurrent apply order across several
  independent shards, and asserts every shard converges to byte-identical
  state and to the exact sum of every increment — a stronger oracle than
  digest equality alone, since last-write-wins can converge every replica to
  the same wrong value. A targeted regression pins the redelivery no-op. At
  the `Cluster` level, a three-node integration test drives concurrent blind
  increments from all three real nodes and asserts convergence to the exact
  total; its writers are deliberately paced on staggered, coprime-ish
  intervals to keep three independent HLC clocks from landing on the same
  `(wall_ms, logical)` pair, an explicit acknowledgment, in the test itself,
  of the same collapse this document's benchmark run triggers under
  unpaced, high-throughput concurrent writes.

Every property test above passes; they establish that the merge algebra and
the version combinator each satisfy their stated laws given the full set of
records. None of them constructs an actual anti-entropy digest exchange
between real peers under unpaced concurrent load, which is why the collapse
case survives the property suite and shows up empirically in the benchmark.

## Benchmark

`sundog/tests/crdt_bench.rs`, `SUNDOG_BENCH=1 cargo test --release -p sundog
--features prometheus --test crdt_bench -- --test-threads=1 --nocapture`, run
three times at the default `WRITERS=8`/`ITERS=200` (each run took 2m33s–2m52s,
under the 10-minute budget, so `ITERS` was left at its default). Six of the
eight scenarios succeed on every run and each printed line is already the
median of 3 internal repetitions; the number reported per scenario below is
the median of those three already-medianed runs.

The remaining two scenarios, `merged_counter_blind` and `merged_counter_rmw`,
did not complete on any of the three full runs: every run hit the version-
collapse gap described above during at least one of its three internal
repetitions and panicked on the 30-second convergence wait rather than
printing a `BENCH` line. Isolated single-repetition retries at the same
`WRITERS=8`/`ITERS=200` scale converged within 30 seconds on 3 of 11 attempts
for `merged_counter_blind` and 3 of 6 for `merged_counter_rmw`; a longer
30-to-180-second wait on the failing attempts showed the stuck replicas never
catching up at all, plateaued below the expected total, confirming this is
permanent non-convergence at that version, not merely slow anti-entropy. The
figures below for these two scenarios are the median of the three converged
single-repetition attempts each did produce.

### Machine

4 vCPUs (`nproc`), `cargo 1.98.0 (797e8a9bc 2026-08-05)`, `rustc 1.98.0
(88d9e12ae 2026-08-18)`.

### Results

**Write throughput and latency** (`WRITERS=8`, `ITERS=200`, 1600 total writes
per scenario):

| Scenario | keys | writes/sec | p50 write | p99 write | lost updates | converge |
|---|---|---:|---:|---:|---:|---:|
| `naive_lww_counter` (control) | 1 | 607,975 | 2.4 µs | 118.2 µs | 574 | 0.021 s |
| `decomposed_counter` | 8 | 1,159,820 | 0.9 µs | 47.8 µs | 0 | 0.000 s |
| `merged_counter_blind` | 1 | 109,381 | 17.9 µs | 684.3 µs | 0 | 0.021 s |
| `merged_counter_rmw` (control) | 1 | 163,441 | 14.6 µs | 321.9 µs | 0 | 0.021 s |

**Wire cost**, same runs:

| Scenario | frames sent | bytes sent | AE repairs |
|---|---:|---:|---:|
| `naive_lww_counter` | 7 | 1,969 | 0 |
| `decomposed_counter` | 6 | 4,426 | 0 |
| `merged_counter_blind` | 2 | 180 | 0 |
| `merged_counter_rmw` | 2 | 304 | 0 |

**Per-apply CPU cost**, no network, 20,000 always-colliding applies against
one pre-populated key:

| Scenario | ns/apply | p50 | p99 |
|---|---:|---:|---:|
| `apply_ns_lww` | 559.1 | 488.0 ns | 817.0 ns |
| `apply_ns_lww_forced_bytes` | 568.6 | 498.0 ns | 724.0 ns |
| `apply_ns_merge` | 1,181.3 | 918.0 ns | 1,987.0 ns |

**Resident state at rest**, after convergence, no further writes:

| Axis | decomposed | merged |
|---|---:|---:|
| Resident keys | 8 | 1 |
| AE repairs at rest | 0 | 0 |

## Where merge wins

**No lost updates, ever, when it converges.** `naive_lww_counter` drops
roughly a third of 1,600 concurrent blind increments (median 574 lost) since
last-write-wins keeps only one side of every colliding pair. Both merge
scenarios and the decomposed-key workaround lose none — merge's value algebra
is exact whenever the anti-entropy path actually delivers a merge, which the
property suite proves happens under any record delivery order.

**One resident key instead of `WRITERS`.** `resident_keys_at_rest` is the
clearest, most durable axis: the merged workload holds its whole concurrent
counter in a single key, one digest slot, one fingerprint — the decomposed
workaround needs one key per writer and that count grows with writer count.
Wire cost per write is comparable between the two (both send roughly one
frame per insert), so this is a fixed-slot win that compounds as the writer
count grows, not a per-write one.

**No read round trip.** `merged_counter_blind` (blind writes, no `get`
before `insert`) reaches roughly two thirds of `merged_counter_rmw`'s
throughput in this run, and both comfortably beat `naive_lww_counter`'s
correctness. The gap between blind and read-modify-write within the merge
scenarios is the read round trip's cost, isolated from the resolver.

## Where merge loses

**Permanent non-convergence at benchmark-realistic write rates, not just
lost updates.** This is the load-bearing finding of this run. At default
scale, both merge scenarios failed to converge within 30 seconds on every
one of three full three-repetition runs, and isolated single-repetition
retries converged only 27–50% of the time; the failing attempts plateau
below the expected total and stay there rather than eventually catching up.
The mechanism is the version-collapse gap in the convergence argument above:
`merge_version` depends only on the two input versions, so a version-collision
between two different concurrent merges freezes the anti-entropy digest at
its old value while the underlying content has genuinely changed, and a peer
comparing digests sees nothing to fetch. The `Cluster`-level integration test
for this same resolver avoids the collision by pacing its three writers on
staggered intervals specifically to keep independent clocks from landing on
the same version — an unpaced, thousand-writes-per-second workload like this
benchmark's has no such pacing, and hits the gap routinely.

**Slower per-apply CPU.** `apply_ns_merge` costs roughly double
`apply_ns_lww`'s ns/apply (1,181.3 ns vs. 559.1 ns) and both its p50 and p99
run higher. `PnCounterResolver::needs_value_bytes()` is `true`, forcing a
decode of both sides and a re-encode on every collision, against LWW's
version-only comparison; `apply_ns_lww_forced_bytes` (LWW forced through the
same byte-materialization path) lands close to plain LWW, confirming the gap
is the merge logic itself, not byte materialization.

**Single shared-key stripe-lock contention.** `merged_counter_blind`'s median
throughput (109,381 writes/sec) trails `decomposed_counter`'s (1,159,820
writes/sec) by roughly 10x in this run — both funnel eight writers through
one lock per stripe-holding key, but merge concentrates every writer onto
one stripe while decomposition spreads them across up to eight. This
benchmark's writer count is small enough that other effects (the value-merge
CPU cost above, and the version-collapse failures competing for the same
CPU budget) are entangled with pure lock contention in this particular
number; the direction is consistent with a stripe-lock contention argument, but
the magnitude here should not be read as a clean measurement of contention
alone.

## What a production version still needs

- **Version provenance for merges.** Closing the collapse gap needs the
  merged version to depend on the merged content itself, not only on the two
  input versions — either an `Hlc`-like scheme that retains provenance across
  repeated merges, or a byte-aware tie-break when the fast path's versions
  already match. Until this lands, a resolver capable of `Merged` should be
  treated as eventually consistent only under write rates low enough, or
  paced enough, to keep concurrent version collisions rare — not as a
  drop-in replacement for last-write-wins under arbitrary load.
- **`OrSet` compaction.** Adds and tombstones never shrink; a long-lived
  `OrSet` key's metadata grows without bound, and the crate's weigher and
  capacity accounting don't yet account for that growth.
- **A generic typed merge adapter.** `PnCounterResolver`/`OrSetResolver` each
  hand-decode a fixed concrete type; a `MergeResolver<V, F>` convenience over
  an arbitrary join-semilattice `V` would let a caller write a merge resolver
  without the boilerplate both reference resolvers duplicate.
- **A merge decode-failure metric.** A resolver's `Merged` reply with bytes
  that fail to decode is rejected silently today, matching existing
  precedent elsewhere in the engine; a dedicated counter would make that
  failure mode observable rather than only inferable from an absent write.
- **Batch pre-folding ahead of the stripe lock.** The one lever that could
  make merge's raw throughput competitive with key decomposition rather than
  only equal to it: folding multiple concurrent writers' records for one key
  before the stripe lock is acquired, rather than one collision at a time.
- **Turmoil coverage under partition and reorder.** The property suite's
  in-process permutation/duplication/concurrency harness covers ordering
  independence; it does not exercise merge under real network partition,
  packet loss, or reordering the way `sim.rs` does for the rest of the
  engine.

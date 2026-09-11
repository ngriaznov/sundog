# CRDT merge resolvers

`ConflictResolver::winner` no longer only picks one of the stored or incoming
record: `Winner::Merged { value, expires_at_ms }` lets a resolver fold both
into a third value, so two concurrent writes to the same key combine instead
of one silently overwriting the other. `sundog::crdt` ships two resolvers
built on this — `PnCounter`/`PnCounterResolver` for an increment/decrement
counter and `OrSet`/`OrSetResolver` for an observed-remove set — as reference
implementations of the contract, not the only merge types a cache can use.
`ROADMAP.md`'s "Merge resolvers" section under Next covers the full design,
including what a production version of the mechanism still needs.

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
resurrect a deleted key or fabricate content against a spilled record.

The version an accepted merge lands under is computed inside the engine,
never by the resolver, and — unlike a scheme that stamps a merge from the two
input versions alone — it depends on the merged content itself. `merge_version`
picks among four outcomes by comparing the merged bytes against each side's
own bytes: when nothing about the value changed (`merged == stored ==
incoming`), it adopts whichever input `Hlc` is greater, or does nothing if
the stored one already is; when the merge reduces to the incoming side and
that side's own clock really is newer, it adopts the incoming `(version,
bytes)` pair verbatim; when the merge reduces to the stored side and that
side's clock really is newer, it does nothing — the incoming write is
already fully absorbed; otherwise — the merged bytes are genuinely new
relative to at least one side, or the real-clock order disagrees with which
side the content-level merge favors — it mints a version: `wall_ms` is the
max of both inputs' and `logical` is that max plus one (carrying into
`wall_ms` on a `logical` overflow), under a `node` id built from a hash of
the merged bytes themselves (`NodeId::merge_derived`). `resolve_and_rebind`
treats a redelivered merge as a true no-op — nothing re-applied, no event
published, nothing re-replicated — whenever the merge reproduces the stored
bytes under the stored version exactly.

A real node id and a merge-derived id can never collide: `NodeId` reserves
its top bit as a private `MERGE_BIT`, `NodeId::random` clears it
unconditionally, the explicit-id builder path (`ClusterBuilder::node_id`)
rejects any id with the bit set, and only `NodeId::merge_derived` sets it.
`HlcClock::observe` is unaffected by any of this — it only ever copies
`wall_ms`/`logical` from a remote stamp and stamps its own real node id, so a
live clock's `logical` counter grows by one per real tick, never by a hash.
On the wire, `Origin::Remote(node)` for a record a merge produced now names
no member of the cluster; this is documented on `Origin::Remote` itself.

## Convergence argument

This is `merge_version`'s own doc comment, restated here rather than
paraphrased. Take two replicas X and Y holding `(vx, Cx)` and `(vy, Cy)` for
the same key, `vx > vy`, any content. Anti-entropy pushes X's record to Y,
which folds `C = Cx ⊔ Cy` (the resolver's join):

- `C == Cx`: the merge reduces to the incoming side and `vx > vy`, so Y
  adopts `(vx, Cx)` verbatim. Converged.
- `C == Cy != Cx`: the merge reduces to Y's own stored side, but `vy` is
  *not* greater than `vx` — the real-clock order disagrees with which side
  the content-level merge favors — so this falls to the mint arm: Y stores a
  freshly minted `v' > vx` for `Cy`. The next round carries `(v', Cy)` to X,
  whose own merge of `Cy` against its stored `Cx` reduces to the incoming
  side with `v' > vx`, and X adopts it. Converged.
- `C` differs from both `Cx` and `Cy`: the mint arm fires on Y, minting
  `v' > vx` for `C`. X receives `(v', C)`; its own merge of `C` against its
  stored `Cx` reduces to the incoming side (the resolver's join is
  idempotent, so folding `C`'s superset back in reproduces `C`) with
  `v' > vx`, and X adopts it. Converged.

Every mint strictly grows the version and either grows content on some
replica or is immediately followed by verbatim adoption on the peer, and
content itself is a join over a finite set of writes, so repeating this
exchange terminates at one shared `(version, bytes)` pair. A redelivery of an
input already folded into what's stored lands on the no-op arm (or the first
arm with the stored side already the greater version) rather than
re-minting, so it never re-triggers this growth.

The mint arm's dominance over both inputs holds unconditionally: `wall_ms` is
at least either input's, and on a `wall_ms` tie `logical` exceeds either
input's because it is a real input's max *plus one*, not the max itself —
the same `+ 1` that keeps a merge folding in more content than the last one
strictly ahead of it even when `wall_ms` doesn't move. Its `node` being a
function of the merged content is what makes the second bullet above safe:
two nodes minting for the same bytes mint the same id and so the identical
version, and a merge-derived id can never equal a real node's id, so a
minted version can never be short-circuited by the engine's `sv == ver` fast
path against a genuine single-writer stamp.

## Property suite

Four tiers, mirroring the levels the crate already tests resolvers at:

- **Value algebra**, no `Shard`: `PnCounter` and `OrSet<String>` each carry
  `proptest` coverage for `merge` commutativity, idempotence (byte-for-byte),
  and three-way associativity across all six foldings, plus targeted tests
  for the per-node max semantics (`local_delta` overwrites, it doesn't sum,
  which is why a writer must track its own cumulative total) and the
  observed-remove scope rule (a concurrent add survives a concurrent remove
  of the same element).
- **`merge_version` combinator**, in `engine.rs`: that the mint arm strictly
  dominates both inputs under `Hlc`'s `Ord` and is always recognizable as
  merge-derived; that two mints for different merged bytes mint different
  `node` components; that each of the other three arms returns exactly what
  the rule says; and a targeted corner case where a `logical` overflow at a
  `wall_ms` tie still carries into `wall_ms` correctly.
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
  the same wrong value. A second property test drives a gossip emulation:
  three shards, each starting from only its own origin's (possibly
  redelivered, reordered) write, are driven to a fixed point by repeatedly
  applying, for every ordered pair, the record with the greater version to
  the lesser side — exactly `diff_bucket`'s anti-entropy direction rule —
  and every shard must land on the identical `(version, bytes)` pair and the
  exact expected total within a bounded number of rounds. A targeted
  regression pins the redelivery no-op. At the `Cluster` level, a three-node
  integration test drives concurrent, unpaced blind increments from all
  three real nodes and asserts convergence to the exact total, with no
  staggering needed to keep independent HLC clocks from colliding.

Every test above passes; together they establish that the merge algebra, the
version combinator in isolation, and repeated pairwise folding under
anti-entropy's own push-direction rule all converge to the exact expected
state — not merely to some shared state every replica happens to agree on.

## Benchmark

`sundog/tests/crdt_bench.rs`, `SUNDOG_BENCH=1 cargo test --release -p sundog
--features prometheus --test crdt_bench -- --test-threads=1 --nocapture`, run
three times at the default `WRITERS=8`/`ITERS=200`. Each run finished in
217.7–218.1 seconds, comfortably under the 15-minute budget, so `SUNDOG_BENCH_KEYS`
stayed at its scenario defaults throughout. All 12 tests passed on all three
runs — `merged_counter_blind` and `merged_counter_rmw` included, both
converging inside their 30-second wait on every one of the 9 internal
repetitions (3 runs × 3 reps each) — with no timeout and no plateau. Each
printed line is already the median of 3 internal repetitions; the figures
below are the median of those three already-medianed runs, taken per field.

### Machine

4 vCPUs (`nproc`), `cargo 1.98.0 (797e8a9bc 2026-08-05)`, `rustc 1.98.0
(88d9e12ae 2026-08-18)`.

### Results

**Write throughput and latency** (`WRITERS=8`, `ITERS=200`, 1600 total writes
per scenario):

| Scenario | keys | writes/sec | p50 write | p99 write | lost updates | converge |
|---|---|---:|---:|---:|---:|---:|
| `naive_lww_counter` (control) | 1 | 625,203 | 2.7 µs | 105.8 µs | 592 | 0.021 s |
| `decomposed_counter` | 8 | 1,397,419 | 1.0 µs | 25.9 µs | 0 | 0.021 s |
| `merged_counter_blind` | 1 | 110,185 | 17.2 µs | 663.3 µs | 0 | 0.021 s |
| `merged_counter_rmw` (control) | 1 | 143,718 | 17.9 µs | 393.6 µs | 0 | 0.021 s |

**Wire cost**, same runs:

| Scenario | frames sent | bytes sent | AE repairs |
|---|---:|---:|---:|
| `naive_lww_counter` | 10 | 2,567 | 0 |
| `decomposed_counter` | 7 | 3,864 | 0 |
| `merged_counter_blind` | 7 | 1,578 | 0 |
| `merged_counter_rmw` | 8 | 2,648 | 0 |

**Per-apply CPU cost**, no network, 20,000 always-colliding applies against
one pre-populated key:

| Scenario | ns/apply | p50 | p99 |
|---|---:|---:|---:|
| `apply_ns_lww` | 583.8 | 493.0 ns | 685.0 ns |
| `apply_ns_lww_forced_bytes` | 562.5 | 493.0 ns | 763.0 ns |
| `apply_ns_merge` | 1,034.5 | 915.0 ns | 1,702.0 ns |

**Resident state at rest**, after convergence, no further writes:

| Axis | decomposed | merged |
|---|---:|---:|
| Resident keys | 8 | 1 |
| AE repairs at rest | 0 | 0 |

**Cold-join initial replication** (a warm 3-node trio, then a fourth node
joins and pulls the resulting state; `keys=2000`, `3 × keys` logical
increments either way):

| Variant | resident-side keys | join time | frames | bytes | entries received |
|---|---:|---:|---:|---:|---:|
| `decomposed` (`3N` per-writer keys, `LwwResolver`) | 6,000 | 0.614 s | 46 | 466,516 | 6,000 |
| `merged` (`N` keys, `PnCounterResolver`) | 2,000 | 0.609 s | 38 | 230,069 | 2,000 |

**Large-entity convergence** (a warm 3-node trio, `N=4,000` entities written
concurrently from all three nodes with anti-entropy live throughout,
`converge_secs` timed from the last write; `lost_updates` is a snapshot taken
immediately after the writers finish and *before* the convergence wait — the
transient post-write, pre-convergence gap, not a permanent loss: both
variants reach the exact expected total by the time `converge_secs` elapses):

| Variant | resident keys | converge time | frames | bytes | pre-convergence gap |
|---|---:|---:|---:|---:|---:|
| `decomposed` (`3N` per-writer keys) | 12,000 | 0.038 s | 6 | 1,265,562 | 8,000 |
| `merged` (`N` keys) | 4,000 | 0.049 s | 6 | 1,031,430 | 8,000 |

**Partition-heal** (`sim` suite, one run: three simulated nodes, node `a`
split from `b`/`c`, 2,000 counters each incremented 3 times per side while
partitioned, then healed; metrics cover anti-entropy from the heal onward):

| Variant | AE rounds | steps to converge | frames | bytes | expected total | actual total | lost updates |
|---|---:|---:|---:|---:|---:|---:|---:|
| `lww_decomposed` (per-writer keys) | 11 | 18 | 6,797 | 1,356,029 | 18,000 | 18,000 | 0 |
| `pn_counter` (merged) | 17 | 36 | 9,456 | 1,563,004 | 18,000 | 18,000 | 0 |

## Where merge wins

**No lost updates, ever, when it converges — and it converges.** Both
`naive_lww_counter`'s companion `decomposed_counter` control and the merged
scenarios lose nothing; `naive_lww_counter` itself drops roughly 37% of
1,600 concurrent blind increments (median 592 lost) since last-write-wins
keeps only one side of every colliding pair. `merged_counter_blind` and
`merged_counter_rmw` converged inside their 30-second wait on all 9 internal
repetitions across the three full runs, and the sim partition-heal run
confirms the same result under an actual network partition and a real
anti-entropy repair: `pn_counter` lands on the exact expected total with
zero lost updates, matching the `lww_decomposed` control exactly.

**One resident key instead of `WRITERS`, and it holds under a cold join.**
`resident_keys_at_rest` is the clearest, most durable axis: the merged
workload holds its whole concurrent counter in a single key, one digest
slot, one fingerprint, where the decomposed workaround needs one key per
writer. Cold-join initial replication shows the same shape scaling with
entity count rather than writer count: at `N=2,000` entities, the merged
variant transfers 2,000 records over 38 frames and 230 KB, while the
decomposed variant carries 6,000 records (`3N`, one per writer per entity)
over 46 frames and 467 KB — roughly double the bytes for triple the
resident-side keys. This is a fixed-slot win that compounds as either the
writer count or the entity count grows, not a per-write one.

**No read round trip.** `merged_counter_blind` (blind writes, no `get`
before `insert`) reaches about 77% of `merged_counter_rmw`'s throughput in
this run, and both comfortably beat `naive_lww_counter`'s correctness. The
gap between blind and read-modify-write within the merge scenarios is the
read round trip's cost, isolated from the resolver.

## Where merge loses

**Slower per-apply CPU.** `apply_ns_merge` costs roughly 1.8x
`apply_ns_lww`'s ns/apply (1,034.5 ns vs. 583.8 ns) and both its p50 and p99
run higher. `PnCounterResolver::needs_value_bytes()` is `true`, forcing a
decode of both sides and a re-encode on every collision, against LWW's
version-only comparison; `apply_ns_lww_forced_bytes` (LWW forced through the
same byte-materialization path) lands close to plain LWW (562.5 ns), confirming
the gap is the merge logic itself, not byte materialization.

**Single shared-key stripe-lock contention.** `merged_counter_blind`'s median
throughput (110,185 writes/sec) trails `decomposed_counter`'s (1,397,419
writes/sec) by roughly 12.7x — both funnel eight writers through one lock per
stripe-holding key, but merge concentrates every writer onto one stripe
while decomposition spreads them across up to eight. The apply-CPU
measurement above accounts for less than a 2x share of that gap, so most of
it is genuine lock contention on the single hot key rather than the
per-collision decode/re-encode cost; this benchmark's writer count (8) is
still small enough that the two effects aren't cleanly separable, so the
factor here should be read as directionally consistent with stripe
contention, not as an exact decomposition of it.

**Slower anti-entropy repair under partition.** In the sim partition-heal
run, the merged variant took more anti-entropy rounds and steps to converge
than the decomposed control (17 rounds / 36 steps vs. 11 / 18) and moved
more bytes (1.56 MB vs. 1.36 MB) to repair the same 2,000-counter split,
even though both reached the identical exact total with zero lost updates.
Every counter's key was touched by both partition sides here, so every one
of the 2,000 keys needs an actual merge (decode both sides, re-encode, mint
or adopt a version) on repair, where the decomposed variant's per-writer
keys have at most one real writer each and mostly resolve as plain
last-write-wins adoptions — the same per-collision CPU and version-minting
cost the throughput benchmarks show, now paid during the repair path
instead of the write path.

## What a production version still needs

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
- **No resolver for a map or a register**, only a counter and a set; a user
  needing either writes their own `ConflictResolver` against the same
  `Merged` contract in the meantime.

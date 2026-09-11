# CRDT merge resolvers

`ConflictResolver::winner` no longer only picks one of the stored or incoming
record: `Winner::Merged { value, expires_at_ms }` lets a resolver fold both
into a third value, so two concurrent writes to the same key combine instead
of one silently overwriting the other. `sundog::crdt` ships two resolvers
built on this — `PnCounter`/`PnCounterResolver` for an increment/decrement
counter and `OrSet`/`OrSetResolver` for an observed-remove set — as reference
implementations of the contract, not the only merge types a cache can use.
Two write-path levers sit on top of the same contract: `Engine::apply_many`
pre-folds a batch that repeats a key before the stripe lock, and
`Cache::merge` with `CacheBuilder::merge_coalesce_window` coalesces a run of
client-side `merge` calls to one key into a single applied record.
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

`ConflictResolver::merges` — `false` by default, `true` on
`PnCounterResolver` and `OrSetResolver` — reports whether a resolver can ever
return `Winner::Merged`; `ShardOps::merges` forwards a shard's configured
resolver's answer, and both write-path levers below and `cluster::anti_entropy`
read it to decide whether their own extra work (grouping a batch, coalescing
a window, exchanging both directions of a mismatch) is worth doing at all.

### Write-path lever A: pre-folding a batch before the stripe lock

`Engine::apply_many` applies a batch of versioned writes that all hash into
one bucket under a single write-lock acquisition. Before pre-folding
existed, a batch holding several entries for the same key — a peer's
fan-out batch carrying many increments to one key, or a local
`Cache::insert_many` call — ran one full decode/fold/encode/mint cycle
through `apply_locked` per entry, even though every entry after the first
only ever folds into what the entries before it already produced.

`apply_many` now checks `resolver.merges()` (and a `#[cfg(test)]`-only
`prefold_enabled` flag the benchmark and property tests use to force it
off) and, when both hold, groups the batch by key with `group_prefold_runs`
into maximal runs of consecutive `Incoming::Put` entries — a run never
crosses an `Incoming::Tombstone`, since `Winner::Merged` is never legal
against a tombstone's value-less side and pre-folding takes the same stance
rather than relying on that guard alone. For every run long enough to
actually fold (two or more entries), `peek_prefold_seeds` looks up that
key's real stored record under a brief stripe *read* lock, dropped again
before any decode or fold work runs. `prefold_batch` then folds each run
down to one survivor entirely outside any lock, with `fold_run` folding the
key's real stored record in *first* when one exists, ahead of the run's own
entries in original order — `P ⊔ e0 ⊔ e1 ⊔ ... ⊔ eN`, the same left-to-right
order sequential per-entry application folds them in against real stored
state one call at a time. That seeding is what makes the survivor's minted
`Hlc`, not only its bytes, come out identical to sequential application's:
`merge_version`'s content join is fold-order independent by the resolver's
own contract, but its mint arm's `wall_ms.max(..)`/`logical.max(..) + 1` is a
running max, not a fixed function of the unordered input set, so folding the
stored record in last instead of first can mint a different — still
correct, still byte-identical in content — version than sequential
application does. Every run's survivor then applies through the ordinary
per-entry path exactly once, under the batch's single write-lock
acquisition; every other index the run absorbed is reported
`ApplyOutcome::Rejected` rather than skipped, so `apply_many` always returns
exactly one outcome per entry, pre-fold or not. `apply_locked`'s own call
against real stored state, once the survivor reaches it, still reconciles
the rare case where a concurrent writer moved the stored record in between
the read lock the seed came from and the write lock the survivor applies
under — `resolve_and_rebind`'s contract makes that reconciliation a correct,
idempotent no-op or verbatim adoption when nothing moved, and a normal merge
when something did.

A non-merging resolver, or a batch with no repeated key, pays only the
grouping pass (and, on a repeated key, the read-lock seed peek) and
otherwise applies exactly as it did before pre-folding existed. `Cache::insert`
is always a singleton `apply_many` call and so can never contain a same-key
run to fold; `insert_many` and a peer's replicated fan-out batch
(`apply_remote_batch`) are where a real run appears.

### Write-path lever B: local delta coalescing

`Cache::merge(key, value)` folds `value` into the configured resolver
without a read, calling `ConflictResolver::winner` directly against the
locally stamped incoming record and either the key's existing pending fold
or (with the coalescing window at its default zero) applying at once
through the ordinary write path — `Cache::merge` at a zero window is
`Cache::insert` under a merging resolver, nothing more.
`CacheBuilder::merge_coalesce_window(Duration)` sets a nonzero window: the
shard keeps `pending_merges`, `BUCKET_COUNT` independently locked stripes —
one `PendingMergeStripe` per `engine::stripe_index_from_hash` bucket, the
same split `engine::Engine`'s own stripes use — and a `merge` call to a key
already pending folds the new value into that pending entry via the same
`resolver.winner` call `apply_locked` itself uses, rather than opening a
second pending entry or applying immediately. The first `merge` call to open
a key's pending entry sets a deadline `merge_deadline_ms(now, window)` ahead
and records `(deadline_ms, seq) -> key` in its stripe's own `by_deadline:
BTreeMap`, `seq` a per-stripe counter breaking a same-deadline tie without
requiring the key type to be `Ord`; a background sweep
(`flush_due_pending_merges`) visits each stripe in turn, locks it only long
enough for `drain_due_deadlines` to pop `by_deadline`'s smallest entries
while their deadline has passed, and applies each through the ordinary
`Shard::apply` path, so replication and every `Event` this key gets during
the window see one record, not one per call. Locking and popping one stripe
at a time, from a deadline-ordered index rather than a linear scan, is what
lets this sweep touch only the entries actually due even when many keys are
coalescing at once — see "Coalescing turns `writers × iters` calls into one
applied record per window" under "Where merge wins" for the effect at scale.
`Cache::close`, and dropping this cache's last handle, both flush whatever is
still pending (`flush_all_pending_merges`) regardless of the window, so no
fold is ever lost to a shutdown.

`Cache::get`/`Shard::get_sync` never consult the pending map: a value folded
in but not yet flushed is invisible to a read for as long as it stays
pending — up to one whole window from the call that opened it. This is
`Cache::merge`'s documented staleness bound, and it is the only place this
lever trades anything away — the resolver still folds every call exactly
once, in the same order it arrived, before any of it reaches the engine.

`CacheBuilder::merge_coalesce_window` rejects a nonzero window on a cache
whose resolver's `merges()` is `false` (`CacheError::MergeWindowRequiresMergingResolver`):
coalescing multiple `merge` calls into one record only preserves every fold
when the resolver can actually fold two values together rather than just
pick a side, so a plain `LwwResolver` cache can only ever run this lever at
the always-immediate zero window.

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

### Bidirectional exchange erases the second round

The argument above still needs two anti-entropy rounds by default: X and Y
each merge only their own stored record against what the other side pushes
or pulls, so whichever replica merges first mints ahead of the other, and
that mint needs a second round trip before the other replica ever sees it.
This comes entirely from anti-entropy's ordinary direction rule, which
pushes only the greater of two versions to the lesser side.

`ConflictResolver::merges` — `false` by default, `true` on
`PnCounterResolver` and `OrSetResolver` — reports whether a resolver can
ever return `Winner::Merged`. `ShardOps::merges` forwards a shard's answer
straight from its configured resolver, and `cluster::anti_entropy`'s
`run_round_against` reads it once per round: when `true`,
`diff_bucket`/`diff_decoded` queue a key present on both sides under a
version mismatch for both push *and* pull, instead of only pushing the
greater side to the lesser one. `true` is always correct to return, even
for a resolver that never actually returns `Merged`; it only ever costs an
extra exchange, never a correctness issue.

With the exchange bidirectional, X and Y each fold the other's *pre-round*
record into their own stored one in the same round — X and Y each call
`merge_version` once, with `(sv, ver)` equal to `(vx, vy)` on one side and
`(vy, vx)` on the other, over the identical unordered pair of records. This
converges in one round on every arm above, and the mint arm does so by
producing byte-for-byte, `Hlc`-for-`Hlc` identical output on both sides, not
merely two outputs that happen to agree once compared:

- The content merge itself is symmetric — `winner`'s commutativity contract
  requires `merge(a, b) == merge(b, a)` byte-for-byte — so both sides mint
  from the identical `merged` bytes, which alone fixes `node`
  (`NodeId::merge_derived` of the same `xxh3_64`) equal on both sides.
- `wall_ms`/`logical` are each a `max` over the *same* two inputs (`{sv,
  ver} == {vx, vy}` on both sides, only the `sv`/`ver` labels swap), and
  `max` does not care which argument carries which label, so the minted
  `(wall_ms, logical)` comes out identical too.

A mint is therefore symmetric in its two input stamps in exactly the sense
that matters here: it is a function of the *unordered pair* of `(version,
bytes)` inputs, not of which one arrived as `sv` and which as `ver`. The two
adopt-verbatim arms converge in one round by the same swapped-argument
symmetry, when they fire at all: if the merge reduces to one side's exact
content and that same side's real clock is the greater of the two, that
side's own exchange call lands on the no-op arm (content matches `stored`,
`sv` the greater) while the other side's call lands on the adopt-verbatim
arm (content matches `incoming`, `ver` the greater) over the same swapped
`(sv, ver)` labels, so both land on the no-op side's exact `(version,
bytes)` in this one round, no mint needed. The remaining case — the merge
reduces to neither side's content, or it reduces to one side's but that
side's real clock is the lesser of the two — is exactly what routes *both*
calls to the mint arm instead, which the paragraph above already covers:
both sides mint the identical result. Every case therefore converges in
this one round, never needing a second — a bound on rounds *per divergent
key*, not on how many rounds a whole partition of many divergent keys takes
to drain; the benchmark's own partition-heal results below measure that
larger, batch-level effect separately, and it does not always move the same
direction this per-key bound does.

## Property suite

Four tiers, mirroring the levels the crate already tests resolvers at, plus
the two write-path levers' own coverage below them.

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
- **Pre-folding (lever A)**: a property test replays a `setup` batch and
  then an arbitrary `main` batch of `UnionSetResolver` entries, small keys so
  several entries collide within a batch, through two fresh engines — one
  pre-folding (the default), one with `Engine::set_prefold_enabled(false)` —
  and asserts every touched key's stored `(version, bytes)` and the set of
  keys reported a real outcome are identical either way; running `setup`
  first through the same on/off split means every key `main` touches already
  has a real stored record behind it before the comparison, the case
  `prefold_batch`'s seeding exists for. Targeted unit tests pin that a
  non-merging resolver never pre-folds, and that a run never folds across a
  tombstone even when the surrounding entries share a key. The toggle itself
  — `CacheBuilder::prefold_enabled`, `Shard::with_prefold_enabled`,
  `Engine::set_prefold_enabled`/`Engine::prefold_enabled` — has its own test
  at each layer: `Engine`'s setter/getter pair round-trips; `Shard`'s
  builder method reaches the engine's flag; `Cache`'s builder method reaches
  the shard's.
- **Coalescing (lever B)**, at both `Shard` and `Cache`: a zero window
  applies immediately, byte-for-byte like `insert`; a nonzero window folds
  consecutive calls to one key in memory and applies exactly once, at the
  deadline, with `get` proven blind to the pending value until then;
  `Cache::close` and a drop both flush every pending fold; the builder
  rejects a nonzero window on a resolver whose `merges()` is `false`;
  a losing call's own version never overwrites the pending entry's version,
  mirroring `resolve_and_rebind`'s own losing-write rule; and a fold against
  a resolver that falls back to plain `Hlc` order behaves exactly as
  `Shard::insert` would. `drain_due_deadlines`, the pure function behind the
  flush sweep's per-stripe deadline index, has its own unit tests
  independent of a live `Shard`: it pops exactly the due entries in deadline
  order (a same-deadline tie broken by insertion order), and leaves a
  not-yet-due entry untouched.

Every test above passes; together they establish that the merge algebra, the
version combinator in isolation, repeated pairwise folding under
anti-entropy's own push-direction rule, and both write-path levers all
converge to the exact expected state — not merely to some shared state every
replica happens to agree on — and that neither lever changes what gets
stored, only how many times the engine does the work of storing it.

## Writer-slot growth

`PnCounter` carries one `p` slot and one `n` slot per distinct writer node,
each a `(NodeId, u64)` pair — `merge`'s pointwise maximum never removes a
slot, only grows or holds one, so a counter's encoded size tracks how many
distinct nodes have ever written to it, never how many times any one of them
has. `pn_counter::tests::encoded_size_at_3_10_and_100_distinct_writers`
(`sundog/src/store/crdt/pn_counter.rs`) measures this directly: a counter
built from `writers` distinct nodes each incrementing once, postcard-encoded,
at `writers = 3, 10, 100`:

| Distinct writers | Encoded size (bytes) |
|---:|---:|
| 3 | 8 |
| 10 | 22 |
| 100 | 202 |

Growth is sublinear-looking here only because a fresh counter's `p` slots
start at postcard's cheapest varint encoding (a one-byte `NodeId` and a
one-byte cumulative total for `NodeId::from(0..9)` incrementing by 1) — the
`n` side stays empty throughout, since every writer in this measurement only
increments. A counter with `NodeId`s or cumulative totals large enough to
need more LEB128 bytes, or with both `p` and `n` populated (mixed
increment/decrement writers), grows faster per slot than this floor shows;
the measurement pins the shape (one slot per distinct writer, no slot ever
shrinks) rather than a byte-per-writer constant.

The size no test here pins because no bound on it exists in the crate today:
**writer-slot compaction for a departed member.** A node that stops writing
to a counter leaves its slot behind forever — `merge`'s pointwise maximum
has no notion of "this writer is gone," only ever-growing per-slot totals —
so a `PnCounter` touched by a large, churning set of writer nodes over a
long enough lifetime grows without bound the same way `OrSet`'s add/remove
metadata does (see "What a production version still needs" below). Removing
a departed writer's slot safely needs membership-wide agreement that no
replica still holds a lower cumulative total for that writer's slot than the
one about to be dropped — dropping it while even one replica has yet to fold
in that writer's true final value would drop real, un-merged content, not
compact it — and sundog's gossip layer has no primitive for that agreement
today: membership tracks who is live, not which value every replica has
folded for every slot of every key.

## Benchmark

`sundog/tests/crdt_bench.rs`, `SUNDOG_BENCH=1 cargo test --release -p sundog
--features prometheus --test crdt_bench -- --test-threads=1 --nocapture`, run
three times at the default `WRITERS=8`/`ITERS=200`. Each run finished in
332.30–334.58 seconds, comfortably under the 15-minute budget. All 16 tests
passed on all three runs — `merged_counter_blind` and `merged_counter_rmw`
included, both converging inside their 30-second wait on every one of the 9
internal repetitions (3 runs × 3 reps each), and `merged_counter_coalesced`'s
three windows converging inside the same wait — with no timeout and no
plateau. Each printed line is already the median of 3 internal repetitions;
the figures below are the median of those three already-medianed runs,
taken per field.

`SUNDOG_BENCH_KEYS` drives four scale scenarios past their defaults —
`cold_join_initial_replication_*` (`keys=2,000`), `large_entity_convergence_*`
and `large_entity_convergence_coalesced` (`N=4,000`), and
`apply_many_prefold` (`batch_size=1,000`) — filtered to run only the
scenario under test (`cargo test ... -- --nocapture <name filter>`) so a
scale run's cost isolates to the scenarios that knob actually changes: one
run each at `keys=50,000` for cold join and `N=100,000` for large-entity
(both the `decomposed`/`merged` pair and the coalesced variant), finishing in
56.77 s and 77.80 s respectively.

### Machine

4 vCPUs (`nproc`), `cargo 1.98.0 (797e8a9bc 2026-08-05)`, `rustc 1.98.0
(88d9e12ae 2026-08-18)`.

### Results

**Write throughput and latency** (`WRITERS=8`, `ITERS=200`, 1600 total writes
per scenario):

| Scenario | keys | writes/sec | p50 write | p99 write | lost updates | converge |
|---|---|---:|---:|---:|---:|---:|
| `naive_lww_counter` (control) | 1 | 451,780 | 3.0 µs | 150.6 µs | 635 | 0.021 s |
| `decomposed_counter` | 8 | 1,512,150 | 1.1 µs | 10.0 µs | 0 | 0.000 s |
| `merged_counter_blind` | 1 | 214,309 | 5.4 µs | 296.0 µs | 0 | 0.021 s |
| `merged_counter_rmw` (control) | 1 | 182,069 | 7.6 µs | 304.5 µs | 0 | 0.021 s |

**Wire cost**, same runs:

| Scenario | frames sent | bytes sent | AE repairs |
|---|---:|---:|---:|
| `naive_lww_counter` | 7 | 1,573 | 0 |
| `decomposed_counter` | 5 | 2,528 | 0 |
| `merged_counter_blind` | 7 | 1,750 | 0 |
| `merged_counter_rmw` | 4 | 480 | 0 |

**Per-apply CPU cost**, no network, 20,000 always-colliding applies against
one pre-populated key:

| Scenario | ns/apply | p50 | p99 |
|---|---:|---:|---:|
| `apply_ns_lww` | 612.4 | 515.0 ns | 1,038.0 ns |
| `apply_ns_lww_forced_bytes` | 585.4 | 522.0 ns | 802.0 ns |
| `apply_ns_merge` | 1,235.0 | 1,144.0 ns | 2,189.0 ns |

**Pre-fold (lever A) on/off**, `apply_many_prefold`, `batch_size=1,000`, a
single-node cache: the identical `insert_many` batch against two caches that
differ only in `CacheBuilder::prefold_enabled` — the real, `#[doc(hidden)]`
engine-level toggle, reachable from this integration-test binary through
`Shard::with_prefold_enabled`/`Engine::set_prefold_enabled` — "on" its
default `true`, "off" `.prefold_enabled(false)`. Both passes pay the
identical single stripe-lock acquisition and the identical per-call fan-out
push; only whether `apply_many` folds a same-key run before applying it
differs, so the `many_keys` shape's on/off pair is expected to land close
together for both resolvers — there is nothing to fold either way — while
`one_key`'s pair isolates pre-fold's own effect directly, no proxy
subtraction needed:

| Shape | Resolver | record ns on | batch ns on | record ns off | batch ns off |
|---|---|---:|---:|---:|---:|
| one key | `LwwResolver` | 741.5 | 741,494 | 718.9 | 718,869 |
| one key | `PnCounterResolver` | 891.9 | 891,893 | 1,117.9 | 1,117,905 |
| many keys | `LwwResolver` | 665.5 | 665,544 | 631.8 | 631,825 |
| many keys | `PnCounterResolver` | 990.3 | 990,257 | 787.3 | 787,283 |

`LwwResolver`'s on/off gap (`batch_ns_off - batch_ns_on`) is a sanity check
on the toggle rather than a correction to subtract: `merges()` is `false`,
so pre-fold never engages for it regardless of the flag, and its gap should
track noise alone. `PnCounterResolver`'s gap is pre-fold's own effect:

| Shape | `LwwResolver` gap (ns) | `PnCounterResolver` gap (ns) |
|---|---:|---:|
| one key | -22,625 | 226,012 |
| many keys | -33,719 | -202,974 |

`one_key` shows pre-fold's real win — `PnCounterResolver`'s 1,000-entry run
folds to one survivor and applies roughly 226 µs faster than sequential
per-entry application, against `LwwResolver`'s near-zero, noise-level gap on
the identical toggle. `many_keys` inverts for `PnCounterResolver` (off
faster than on) rather than landing at zero: with no repeated key in the
batch there is nothing to fold either way, so both resolvers' gaps here are
noise around zero, at this shape's own scale, not a real pre-fold cost —
`LwwResolver`'s gap over the same shape is the same order of magnitude and
sign-unstable across runs, confirming it.

**Receive-side applies, pre-fold on/off**, `replicated_hot_counter_receive`
(the eight-writer single-`PnCounter`-key shape, instrumented on the two
nodes that only ever receive the resulting fan-out/anti-entropy batches).
"on" opens both receiving nodes with `CacheBuilder::prefold_enabled`'s
default `true`; "off" opens them with `.prefold_enabled(false)` — a
receiving node's batch shape still comes from the sender's own fan-out, but
the toggle reaches that node's engine the same way regardless of who
assembled the batch it applies, so this is a real off variant, not a proxy:

| Metric | on | off |
|---|---:|---:|
| Applies on node b | 2 | 9 |
| Applies on node c | 2 | 9 |
| Converge time | 0.021 s | 0.021 s |
| Frames sent | 5 | 7 |
| Bytes sent | 677 | 1,511 |
| Lost updates | 0 | 0 |

With pre-fold off, each receiving node applies every record in a fan-out
batch individually rather than folding a same-key run to one survivor
first — `applies_b`/`applies_c` roughly quadruple (2 → 9) for the identical
eight-writer, 200-iteration workload, `Cache::events()`'s own count making
pre-fold's effect on the receive path directly visible for the first time,
rather than only inferred from the send-side proxy scenario 9 used before
this toggle existed. Frames and bytes shift too, since more individual
applies mean more individual `Event`s fanning back out; converge time itself
is unaffected at this scale — either way converges inside the same
sub-second window.

**Coalescing (lever B)**, `merged_counter_coalesced`, the same eight-writer
single-counter shape through `Cache::merge` instead of `insert`. `engine
applies` is `Cache::events()`'s own count — how many times a coalesced fold
actually reached the engine, against `writers × iters = 1,600` client-side
`merge` calls. `lost updates` here is a snapshot taken immediately after the
writers finish and before the convergence wait, so for a nonzero window it
also counts every fold still sitting in the pending map, not yet visible to
`get` — `Cache::merge`'s own documented staleness bound, not a real loss:
every window converges to the exact total inside the same 30-second wait the
blind/RMW scenarios use:

| Window | merges/sec | p50 merge | p99 merge | engine applies | frames sent | bytes sent | converge time | lost updates (transient) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 ms | 175,695 | 9.0 µs | 322.3 µs | 1,600 | 10 | 2,588 | 0.021 s | 0 |
| 1 ms | 190,045 | 2.7 µs | 225.8 µs | 1 | 2 | 196 | 0.021 s | 1,600 |
| 10 ms | 163,062 | 2.8 µs | 228.9 µs | 1 | 2 | 198 | 0.021 s | 1,600 |

A zero window's per-call latency is roughly 3x either nonzero window's,
since every call pays the full engine round trip; both nonzero windows fold
every one of the 1,600 calls into a single applied record (`engine
applies=1`), the fold this lever exists for.

**Resident state at rest**, after convergence, no further writes:

| Axis | decomposed | merged |
|---|---:|---:|
| Resident keys | 8 | 1 |
| AE repairs at rest | 0 | 0 |

**Cold-join initial replication** (a warm 3-node trio, then a fourth node
joins and pulls the resulting state; `3 × keys` logical increments either
way). Default `keys=2,000` is the median of the same three runs; `keys=50,000`
is one filtered run (56.77 s total for both variants):

| Variant | keys | resident-side keys | join time | frames | bytes | entries received |
|---|---:|---:|---:|---:|---:|---:|
| `decomposed` (`3N` per-writer keys, `LwwResolver`) | 2,000 | 6,000 | 0.614 s | 42 | 455,276 | 6,000 |
| `merged` (`N` keys, `PnCounterResolver`) | 2,000 | 2,000 | 0.609 s | 38 | 230,147 | 2,000 |
| `decomposed` (`3N` per-writer keys, `LwwResolver`) | 50,000 | 150,000 | 0.912 s | 1,234 | 11,044,130 | 150,000 |
| `merged` (`N` keys, `PnCounterResolver`) | 50,000 | 50,000 | 0.706 s | 139 | 2,561,448 | 50,000 |

**Large-entity convergence** (a warm 3-node trio, `N` entities written
concurrently from all three nodes with anti-entropy live throughout,
`converge_secs` timed from the last write; `lost_updates` is a snapshot taken
immediately after the writers finish and *before* the convergence wait —
the transient post-write, pre-convergence gap, not a permanent loss: every
variant at every `N` below reaches the exact expected total by the time
`converge_secs` elapses). `coalesced` uses `Cache::merge` with a 1 ms window
in place of a direct `insert`. Default `N=4,000` is the median of the same
three runs; `N=100,000` is one filtered run (77.80 s total for all three
variants, measured against `Shard::flush_due_pending_merges`'s current,
stripe-sharded, deadline-ordered sweep — see "Where merge loses" and "What
a production version still needs" for what that replaced):

| Variant | N | resident keys | converge time | frames | bytes | pre-convergence gap | AE repairs |
|---|---:|---:|---:|---:|---:|---:|---:|
| `decomposed` (`3N` per-writer keys) | 4,000 | 12,000 | 0.039 s | 6 | 1,265,562 | 8,000 | 0 |
| `merged` (`N` keys) | 4,000 | 4,000 | 0.049 s | 6 | 1,031,430 | 8,000 | 0 |
| `coalesced` (`N` keys, 1 ms window) | 4,000 | 4,000 | 0.050 s | 6 | 1,031,448 | 12,000 | 0 |
| `decomposed` (`3N` per-writer keys) | 100,000 | 300,000 | 1.011 s | 3,718 | 83,117,682 | 186,073 | 599,452 |
| `merged` (`N` keys) | 100,000 | 100,000 | 1.240 s | 7,065 | 77,906,929 | 200,000 | 767,294 |
| `coalesced` (`N` keys, 1 ms window) | 100,000 | 100,000 | 1.018 s | 2,793 | 52,429,486 | 290,727 | 434,266 |

**Partition-heal** (`sim` suite, `cargo test -p sundog --features sim --test
sim partition_heal -- --nocapture`: three simulated nodes, node `a` split
from `b`/`c`, counters each incremented 3 times per side while partitioned,
then healed; metrics cover anti-entropy from the heal onward). The sim
harness itself was hardened for determinism since the historical `before`
figures below were measured (the partition is now applied before the
mesh's first connection-settling step, and the anti-entropy outbox is sized
past this family's worst-case repair burst), which shifts every run's raw
round/step/byte counts independently of the resolver logic — the `before`/
`after` comparison at default scale still isolates the bidirectional
exchange's own effect, since both rows on each side of it share one harness:

| Variant | AE rounds | steps to converge | frames | bytes | expected total | actual total | lost updates |
|---|---:|---:|---:|---:|---:|---:|---:|
| `lww_decomposed`, before bidirectional exchange | 11 | 18 | 6,797 | 1,356,029 | 18,000 | 18,000 | 0 |
| `pn_counter`, before bidirectional exchange | 17 | 36 | 9,456 | 1,563,004 | 18,000 | 18,000 | 0 |
| `lww_decomposed`, current harness, 2,000 counters | 4 | 10 | 3,088 | 559,822 | 18,000 | 18,000 | 0 |
| `pn_counter`, current harness, 2,000 counters | 3 | 9 | 2,663 | 486,500 | 18,000 | 18,000 | 0 |
| `lww_decomposed`, current harness, 20,000 counters | 3 | 12–13 | 3,109–3,110 | 4,853,231–4,857,178 | 180,000 | 180,000 | 0 |
| `pn_counter`, current harness, 20,000 counters | **5** | 14 | 3,107 | 4,095,689–4,095,780 | 180,000 | 180,000 | 0 |

At 20,000 counters `pn_counter` takes *more* anti-entropy rounds than
`lww_decomposed` (5 vs. 3) — the reverse of the default-scale result and a
direct violation of the sim test's own assertion
(`partition_heal_pn_counter_converges_to_exact_totals`, `sundog/tests/sim.rs:3562`),
which requires the bidirectional exchange to match or beat the decomposed
control. Both totals still converge exactly with zero lost updates either
way; only the round count regresses. Reproduced identically across two
independent runs at the same seed (`ae_rounds=5`/`3`, `steps=14`/`12` then
`14`/`13`), so this is a real, scale-dependent regression in the round
count, not run-to-run noise — see "Where merge loses" and "What a production
version still needs" below.

## Where merge wins

**No lost updates, ever, when it converges — and it converges.** Both
`naive_lww_counter`'s companion `decomposed_counter` control and every
merged scenario — blind, read-modify-write, and every coalescing window —
lose nothing; `naive_lww_counter` itself drops roughly 40% of 1,600
concurrent blind increments (median 635 lost) since last-write-wins keeps
only one side of every colliding pair. The sim partition-heal run confirms
the same result under an actual network partition and a real anti-entropy
repair at default scale: `pn_counter` lands on the exact expected total with
zero lost updates, matching the `lww_decomposed` control exactly, and does
so in fewer anti-entropy rounds (3 vs. 4) — the bidirectional exchange means
a divergent key's repair no longer costs a second round to carry a minted
merge back to whichever side mints first. That advantage does not hold at
every scale tested; see "Where merge loses" below.

**One resident key instead of `WRITERS`, and it holds under a cold join, at
any scale tested.** `resident_keys_at_rest` is the clearest, most durable
axis: the merged workload holds its whole concurrent counter in a single
key, one digest slot, one fingerprint, where the decomposed workaround needs
one key per writer. Cold-join initial replication shows the same shape
scaling with entity count rather than writer count, and the bytes advantage
compounds rather than staying fixed: at `N=2,000` entities the merged variant
transfers 2,000 records over 38 frames and 230 KB against the decomposed
variant's 6,000 records (`3N`) over 42 frames and 455 KB — roughly 2.0x the
bytes for 3x the resident-side keys — and at `N=50,000` that gap widens to
roughly 4.3x the bytes (11.0 MB vs. 2.6 MB) for the same fixed 3x key ratio.
This is a fixed-slot win that compounds as either the writer count or the
entity count grows, not a per-write one.

**Pre-folding turns a batch's per-collision cost into roughly one entry's.**
`apply_many_prefold`'s `one_key` gap (the identical `insert_many` batch
against the real `prefold_enabled` toggle) shows `PnCounterResolver` saving
on the order of 226 µs across a 1,000-entry same-key batch — the
decode/fold/encode/mint work `apply_locked` used to pay once per entry now
happens once per run instead, with every other entry in that run absorbed
for the cost of a group-by pass alone. `LwwResolver`'s gap over the same
shape and toggle is noise-level (-22,625 ns): it never engages pre-fold
regardless of the flag, confirming the `one_key` gap above is pre-fold's own
effect, not batching or fan-out noise the old insert-loop proxy could not
rule out. `replicated_hot_counter_receive`'s on/off comparison shows the
same effect on the receive path directly: a receiving node's applies per
key roughly quadruple with the toggle off (2 → 9) for the identical
eight-writer workload.

**Coalescing turns `writers × iters` calls into one applied record per
window, and now scales the flush that does it.** `merged_counter_coalesced`'s
`engine applies` column drops from 1,600 (every call applies) at a zero
window to 1 at either a 1 ms or 10 ms window — every one of the 1,600
client-side `merge` calls across all eight writers folds into the single
record the window flushes once. Both nonzero windows' per-call `merge`
latency runs at roughly a third of the zero window's (no per-call engine
round trip to wait on), and both still converge to the exact expected total
inside the same 30-second wait the always-apply scenarios use — the
staleness this lever trades away is bounded and never costs correctness.
`Shard::flush_due_pending_merges`'s sweep now mirrors `engine::Engine`'s own
`BUCKET_COUNT`-stripe split: `Shard::pending_merges` is
`BUCKET_COUNT` independently locked stripes, one per `stripe_index_from_hash`
bucket, each with its own `by_deadline: BTreeMap<(u64, u64), K>` index, so a
flush locks and pops only the entries actually due in one stripe at a time
rather than one shard-wide mutex guarding a single `HashMap` the sweep had
to scan in full on every tick. `large_entity_convergence_coalesced` at
`N=100,000` shows the result directly: 1.018 s, essentially tied with
`decomposed`'s 1.011 s and faster than `merged`'s 1.240 s, rather than the
scenario's slowest variant by a wide margin — see "What a production
version still needs" for what this replaced.

## Where merge loses

**Slower per-apply CPU.** `apply_ns_merge` costs roughly 2x
`apply_ns_lww`'s ns/apply (1,235.0 ns vs. 612.4 ns) and both its p50 and p99
run higher. `PnCounterResolver::needs_value_bytes()` is `true`, forcing a
decode of both sides and a re-encode on every collision, against LWW's
version-only comparison; `apply_ns_lww_forced_bytes` (LWW forced through the
same byte-materialization path) lands close to plain LWW (585.4 ns),
confirming the gap is the merge logic itself, not byte materialization.

**Single shared-key stripe-lock contention, pre-folding included.**
`merged_counter_blind`'s median throughput (214,309 writes/sec) still trails
`decomposed_counter`'s (1,512,150 writes/sec) by roughly 7x — both funnel
eight writers through one lock per stripe-holding key, but merge
concentrates every writer onto one stripe while decomposition spreads them
across up to eight. Pre-folding does not close this gap: at `WRITERS=8`
each writer's own `insert` is still a batch of one, and pre-folding only
collapses entries that already arrive together in the *same* `apply_many`
call — a single writer's own singleton calls have nothing to fold against
each other, so the remaining gap here is the per-apply CPU cost above and
genuine lock contention on the one hot key, not anything this batch-level
lever reaches.

**More frames at scale, even as bytes fall.** `large_entity_convergence`'s
frame count flips the same way its bytes don't: at `N=4,000` `merged` and
`decomposed` send the same 6 frames, but at `N=100,000` `merged` sends 7,065
against `decomposed`'s 3,718 — while still moving fewer total bytes (77.9 MB
vs. 83.1 MB). `PnCounterResolver::merges()` being `true` makes every
version-mismatched key exchange in both directions every round instead of
only the greater side pushing, so a key that keeps mismatching across
several live anti-entropy rounds under concurrent write pressure costs more
round-trip messages even though each message carries less redundant data
than decomposition's non-overlapping per-writer keys would.

**The bidirectional exchange's round advantage does not hold at every
scale.** At the default 2,000 counters, `pn_counter` repairs a partition
split in fewer anti-entropy rounds than the `lww_decomposed` control (3 vs.
4), consistent with the bidirectional exchange's own per-key, one-round
convergence argument above. At 20,000 counters that inverts: `pn_counter`
needs 5 rounds against `lww_decomposed`'s 3, reproducibly, and fails the sim
test's own assertion that bidirectional merging should match or beat
decomposition. The per-key convergence argument bounds how many rounds *one*
divergent key's own repair costs, not how many rounds it takes a whole
partition's worth of simultaneously divergent keys to drain — every one of
20,000 counters here was touched by both partition sides, so every one
needs a real merge (decode both sides, re-encode, mint or adopt) on every
round it is still exchanged in both directions, and at this key count that
per-round cost evidently outweighs the exchange's round-count saving badly
enough to flip which control wins. This is an open regression, not a
documented trade-off; see "What a production version still needs" below.

## What a production version still needs

- **The bidirectional exchange's round-count regression at scale.**
  `partition_heal_pn_counter_converges_to_exact_totals` fails its own
  assertion at `SUNDOG_SIM_KEYS=20000` (`pn_counter` spends 5 anti-entropy
  rounds against `lww_decomposed`'s 3), reproducibly. The per-key
  convergence argument this feature ships with only bounds one key's own
  repair; it says nothing about how the bidirectional exchange's doubled
  per-round cost interacts with a partition where every key needs a real
  merge, and this is now measured evidence that the interaction can go the
  wrong way. Needs root-causing before the exchange can be trusted at this
  scale.
- **`PnCounter` writer-slot compaction.** A departed writer's `p`/`n` slots
  never shrink — see "Writer-slot growth" above for the measured per-writer
  size and the constraint a safe compaction needs: membership-wide agreement
  that every replica has already folded that writer's final value, which
  sundog's gossip does not provide today.
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
- **No metric for a coalesced fold.** `Cache::merge`'s pending-fold count and
  flush cadence are observable today only through `Cache::events()`'s own
  count (as the benchmark above does) and `entry_count`; a dedicated counter
  for calls folded versus calls actually applied would make lever B's own
  effect visible outside a benchmark.
- **No resolver for a map or a register**, only a counter and a set; a user
  needing either writes their own `ConflictResolver` against the same
  `Merged` contract in the meantime.

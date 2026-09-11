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

## API

`Winner` gains a `#[non_exhaustive]` `Merged` variant carrying the merged
`Bytes` and the resolver's own choice of TTL — the engine never infers a TTL
from either input. `ConflictResolver`'s doc contract states the join-semilattice
obligation on a `Merged`-capable resolver: `merge` must be commutative,
associative under arbitrary pairwise fold order, and idempotent, since
`apply_locked` only ever folds one collision at a time and any two replicas
can fold the same concurrent writes in different orders.

`ConflictResolver::merges` — `false` by default, `true` on
`PnCounterResolver` and `OrSetResolver` — reports whether a resolver can ever
return `Winner::Merged`; `ShardOps::merges` forwards a shard's configured
resolver's answer, and pre-folding, coalescing, and `cluster::anti_entropy`
each read it to decide whether their own extra work (grouping a batch,
coalescing a window, exchanging both directions of a mismatch) is worth
doing at all. `Cache::merge(key, value)` folds `value` into the configured
resolver without a read, and `CacheBuilder::merge_coalesce_window(Duration)`
sets how long a run of `merge` calls to one key coalesces before applying —
see "Coalescing" below. `CacheBuilder::prefold_enabled` is `#[doc(hidden)]`:
a real toggle for `Engine::apply_many`'s batch pre-fold, reachable from an
ordinary downstream crate through
`Shard::with_prefold_enabled`/`Engine::set_prefold_enabled`, that this
benchmark uses to measure pre-fold's own effect directly rather than only
inferring it.

## Engine change

The engine enforces the one invariant a resolver's convention alone can't
guarantee: `Merged` is only honored when both the stored and incoming records
carry a value. Against a tombstone, `resolve_conflict` degrades to keeping the
existing record — a resolver that misbehaves can never resurrect a deleted
key. (A spilled stored side is a separate case: see "Merge against spilled
records" below.)

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

## Version rule and convergence argument

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
pushes only the greater of two versions to the lesser side — and that rule
cannot simply be skipped by comparing versions more cleverly: the greater
side's version says nothing about whether it has already seen the lesser
side's content. A merge needs *both* sides' bytes, not just the winning
clock, so the lesser side's own unique content has to reach the greater
side somehow, and version order alone, pushed in one direction only, never
carries it there.

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
direction this per-key bound does. Redundant pulls at a partial conflict
fraction are exactly this batch-level effect, distinct from the per-key
round bound above: see "Partition-heal" and "Where merge loses" below.

## Pre-folding

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

## Coalescing

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

## Merge against spilled records

`apply_locked` never treats a spilled stored side as automatically
value-less. Before the stripe's write lock is even taken, a prefetch pass
(`prefetch_spilled_conflict_bytes`, keyed by `SpillLoc` — `region`/`offset`/
`length`/`generation`, now `Hash` so it can key that lookup map) reads a
spilled stored record's bytes back off disk for every collision on the
common path; only on the rare race where a concurrent flush moved the entry
between that prefetch and the write lock does `apply_locked` fall back to
reading it under the lock instead. Either way, a value-aware resolver merges
against a spilled record's real content exactly as it would a resident
one — `needs_value_bytes()` returning `true` no longer means "skip this side
if it's on disk." Only a spilled side whose bytes genuinely can't be
produced — no spill tier attached, or the read itself fails — still degrades
to `resolve_conflict`'s tombstone treatment (real-clock order only, no
`Merged` outcome honored), so a resolver can never fabricate content against
a record it cannot actually read. The property suite pins both paths
directly at the `Engine`, `Shard`, and `Cache` layers: a readable spilled
side folds like a resident one, and an unreadable one falls back to plain
`Hlc` order the same as a tombstone or a decode failure would.

## The sketch path

A bucket's ordinary anti-entropy mismatch answers with a full key listing;
past `ClusterConfig::ae_sketch_min_bucket` entries, it answers with an IBLT
sketch instead (`cluster::sketch::Iblt`), and `handle_sketch_mismatch` peels
the local/remote symmetric difference rather than ever materializing every
key. The bidirectional-exchange rule from anti-entropy's ordinary,
non-sketch path carries through unchanged: `peel_sketch_into` passes
`merging` (`ShardOps::merges`'s answer for the round) straight into
`diff_decoded`, so a peeled key hash present on both sides under different
versions queues for both push and pull exactly like a listed mismatch does,
rather than only the greater version's side. A sketch that fails to peel
(the symmetric difference exceeds the sketch's rated capacity) falls back to
a full listing for that bucket, at which point the same `diff_bucket`
push/pull rule applies again over the actual keys. Either path, merging or
not, emits `sundog_ae_sketch_total{cache, outcome}`'s existing `decoded`/
`fallback` counters — no separate metric for the merging case, since a
resolver's `merges()` answer changes only the push/pull direction the peeled
(or listed) result is classified into, not whether the peel itself succeeds.
`a_merging_bucket_above_the_threshold_converges_through_the_sketch_path`
pins this directly at unit scale: a merging bucket's mismatch peels with no
fallback and every one of its keys queues for both push and pull in the one
round.

## Property suite

Four tiers, mirroring the levels the crate already tests resolvers at, plus
the two write-path levers' own coverage below them, plus the partition-heal
sim's own determinism and monotonicity pins.

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
  `Hlc` order against a tombstone, an unreadable spilled side (no tier
  attached, or the read itself fails), or a decode failure — but fold a
  readable spilled side's real content exactly like a resident one, pinned
  directly at the `Engine`, `Shard`, and `Cache` layers plus a batch/prefold
  variant that seeds the fold with the real (spilled) stored record instead
  of dropping it; `resolve_conflict`'s tombstone guard is exercised directly,
  along with a decode-failure path that rejects rather than panics.
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
  staggering needed to keep independent HLC clocks from colliding. A unit
  test at the sketch path pins the same bidirectional rule one level down:
  `a_merging_bucket_above_the_threshold_converges_through_the_sketch_path`
  peels a merging bucket's mismatch with no fallback and confirms every one
  of its keys queues for both push and pull in the one round.
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
- **Partition-heal sim, determinism and monotonicity**: the sim harness
  itself is pinned, not only the resolver it drives.
  `partition_heal_is_deterministic_for_a_fixed_config` runs the identical
  `HealConfig` twice in one process and asserts every field the harness
  itself controls — `ae_rounds`, `records`, `applies`, `folds`,
  `redundant_pulls`, `expected_total`/`actual_total` — comes out identical
  both times (frames/bytes are the one deliberate exception, since they read
  `sundog::net`'s process-wide wire counters and so can pick up a wholly
  unrelated concurrent test's traffic).
  `partition_heal_rounds_are_monotone_in_key_count` drives both variants at
  full conflict across 2,000 through 40,000 keys and asserts `ae_rounds`
  never regresses as key count grows — the harness's own regression
  signature check: an earlier, since-replaced version of this sim once
  produced non-monotonic round counts (3 at 8,000 keys, 19 at 16,000, 5 at
  20,000) from a reimplementation of anti-entropy; driving the production
  `run_round_against` entry point instead removes that source of
  non-determinism.

Every test above passes; together they establish that the merge algebra, the
version combinator in isolation, repeated pairwise folding under
anti-entropy's own push-direction rule, both write-path levers, and the
partition-heal harness itself all converge to the exact expected state — not
merely to some shared state every replica happens to agree on — and that
neither lever changes what gets stored, only how many times the engine does
the work of storing it.

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
metadata does (see "Scale: what was run, what remains" below). Removing
a departed writer's slot safely needs membership-wide agreement that no
replica still holds a lower cumulative total for that writer's slot than the
one about to be dropped — dropping it while even one replica has yet to fold
in that writer's true final value would drop real, un-merged content, not
compact it — and sundog's gossip layer has no primitive for that agreement
today: membership tracks who is live, not which value every replica has
folded for every slot of every key.

## Benchmark method

`sundog/tests/crdt_bench.rs`, `SUNDOG_BENCH=1 cargo test --release -p sundog
--features prometheus --test crdt_bench -- --test-threads=1 --nocapture`, run
three times at the default `WRITERS=8`/`ITERS=200`. Each run finished in
396.03–398.59 seconds, comfortably under the 15-minute budget. All 18 tests
passed on all three runs — `merged_counter_blind` and `merged_counter_rmw`
included, both converging inside their 30-second wait on every one of the 9
internal repetitions (3 runs × 3 reps each), `merged_counter_coalesced`'s
three windows converging inside the same wait, and both
`sketch_path_convergence` scenarios peeling their seeded sketch mismatch and
reconverging within their own 60-second wait — with no timeout and no
plateau anywhere. Each printed line is already the median of 3 internal
repetitions; the figures below are the median of those three already-medianed
runs, taken per field.

`SUNDOG_BENCH_KEYS` drives five scale scenarios past their defaults —
`cold_join_initial_replication_*` (`keys=2,000`), `large_entity_convergence_*`
and `large_entity_convergence_coalesced` (`N=4,000`), `apply_many_prefold`
(`batch_size=1,000`), and `sketch_path_convergence_*` (the dense bucket size,
default 200) — filtered to run only the scenario under test (`cargo test ...
-- --nocapture <name filter>`) so a scale run's cost isolates to the
scenarios that knob actually changes: one run each at `keys=50,000` for cold
join and `N=100,000` for large-entity (both the `decomposed`/`merged` pair
and the coalesced variant) and for the sketch path (both resolvers),
finishing in 54.81 s, 79.02 s, and
73.43 s respectively.

### Machine

4 vCPUs (`nproc`, Intel(R) Xeon(R) Processor @ 2.80GHz), `cargo 1.98.0
(797e8a9bc 2026-08-05)`, `rustc 1.98.0 (88d9e12ae 2026-08-18)`.

### Fairness controls

Every scenario shares the process-wide wire counters in `sundog::net`, which
is why the whole binary runs `--test-threads=1`: two scenarios racing
concurrently would double-count each other's frames and bytes. Scenarios
1-4 and 6 build a fresh 3-node `Replicated` cluster per repetition, identical
`fast_config()` topology throughout (150ms AE interval, 2s tombstone TTL);
scenarios 5/5a/5b build a single-node `Mode::Local` cluster with no peers,
isolating per-apply CPU cost from all network/AE noise. No key ever expires
or is removed in any scenario — TTL is irrelevant here, stated to rule out a
confound rather than leave it implicit. Every numeric field is the median of
at least 3 independent runs (`repetitions()`), so a single noisy run never
skews a reported number. The two write-path-lever scenarios (9-12) and the
sketch-path scenario (13) measure their on/off or peel/fallback comparison
through the real public toggle or the real `sundog_ae_sketch_total` counter
rather than a proxy — see "Pre-folding", "Coalescing", and "The sketch path"
above for what each reaches.

### Results

**Write throughput and latency** (`WRITERS=8`, `ITERS=200`, 1600 total writes
per scenario):

| Scenario | keys | writes/sec | p50 write | p99 write | lost updates | converge |
|---|---|---:|---:|---:|---:|---:|
| `naive_lww_counter` (control) | 1 | 461,934 | 3.0 µs | 149.9 µs | 626 | 0.021 s |
| `decomposed_counter` | 8 | 1,411,967 | 1.0 µs | 25.7 µs | 0 | 0.000 s |
| `merged_counter_blind` | 1 | 217,407 | 5.2 µs | 292.9 µs | 0 | 0.020 s |
| `merged_counter_rmw` (control) | 1 | 167,039 | 7.1 µs | 312.2 µs | 0 | 0.021 s |

**Per-apply CPU cost**, no network, 20,000 always-colliding applies against
one pre-populated key:

| Scenario | ns/apply | p50 | p99 |
|---|---:|---:|---:|
| `apply_ns_lww` | 597.0 | 530.0 ns | 825.0 ns |
| `apply_ns_lww_forced_bytes` | 599.4 | 533.0 ns | 892.0 ns |
| `apply_ns_merge` | 1,195.3 | 1,092.0 ns | 1,944.0 ns |

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
| one key | `LwwResolver` | 777.0 | 776,952 | 699.3 | 699,290 |
| one key | `PnCounterResolver` | 1,032.6 | 1,032,579 | 1,080.0 | 1,080,050 |
| many keys | `LwwResolver` | 687.4 | 687,372 | 597.6 | 597,630 |
| many keys | `PnCounterResolver` | 971.8 | 971,793 | 763.8 | 763,751 |

`LwwResolver`'s on/off gap (`batch_ns_off - batch_ns_on`) is a sanity check
on the toggle rather than a correction to subtract: `merges()` is `false`,
so pre-fold never engages for it regardless of the flag, and its gap should
track noise alone. `PnCounterResolver`'s gap is pre-fold's own effect:

| Shape | `LwwResolver` gap (ns) | `PnCounterResolver` gap (ns) |
|---|---:|---:|
| one key | -77,662 | 47,471 |
| many keys | -89,742 | -208,042 |

`one_key` shows pre-fold's real win — `PnCounterResolver`'s 1,000-entry run
folds to one survivor and applies roughly 47 µs faster than sequential
per-entry application, against `LwwResolver`'s noise-level gap of the same
order of magnitude (both well under a microsecond per entry) on the
identical toggle. `many_keys` inverts for `PnCounterResolver` (off faster
than on) rather than landing at zero: with no repeated key in the batch
there is nothing to fold either way, so both resolvers' gaps here are noise
around zero, at this shape's own scale, not a real pre-fold cost —
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
| Applies on node b | 3 | 4 |
| Applies on node c | 3 | 4 |
| Converge time | 0.021 s | 0.021 s |
| Frames sent | 6 | 5 |
| Bytes sent | 578 | 772 |
| Lost updates | 0 | 0 |

With pre-fold off, each receiving node applies every record in a fan-out
batch individually rather than folding a same-key run to one survivor
first — `applies_b`/`applies_c` go from 3 to 4 for the identical
eight-writer, 200-iteration workload, `Cache::events()`'s own count making
pre-fold's effect on the receive path directly visible, rather than only
inferred from the send-side proxy scenario 9 used before this toggle
existed. This shape's own fan-out batch is small and per-run variance is
high (individual runs ranged 1-5 applies per node either way) at
`WRITERS=8`/`ITERS=200`'s default scale, so the receive-side gap here reads
as directional rather than a clean multiple — `apply_many_prefold`'s
1,000-entry `one_key` gap above isolates the same mechanism at a scale
large enough for the effect to dominate the noise. Converge time is
unaffected at this scale either way.

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
| 0 ms | 188,918 | 6.8 µs | 260.8 µs | 1,600 | 13 | 2,490 | 0.021 s | 0 |
| 1 ms | 172,021 | 2.7 µs | 227.0 µs | 2 | 4 | 392 | 0.021 s | 224 |
| 10 ms | 184,484 | 2.7 µs | 244.8 µs | 1 | 2 | 198 | 0.022 s | 1,600 |

A zero window's per-call latency is roughly 2.5x either nonzero window's,
since every call pays the full engine round trip; both nonzero windows fold
nearly every one of the 1,600 calls into one or two applied records
(`engine applies` of 1-2, against 1,600 at the zero window), the fold this
lever exists for.

**Wire cost**, the write-throughput runs above:

| Scenario | frames sent | bytes sent | AE repairs |
|---|---:|---:|---:|
| `naive_lww_counter` | 7 | 1,855 | 0 |
| `decomposed_counter` | 6 | 3,752 | 0 |
| `merged_counter_blind` | 10 | 3,718 | 0 |
| `merged_counter_rmw` | 6 | 840 | 0 |

**Resident state at rest**, after convergence, no further writes:

| Axis | decomposed | merged |
|---|---:|---:|
| Resident keys | 8 | 1 |
| AE repairs at rest | 0 | 0 |

**Cold-join initial replication** (a warm 3-node trio, then a fourth node
joins and pulls the resulting state; `3 × keys` logical increments either
way). Default `keys=2,000` is the median of the same three runs; `keys=50,000`
is one filtered run (54.81 s total for both variants):

| Variant | keys | resident-side keys | join time | frames | bytes | entries received |
|---|---:|---:|---:|---:|---:|---:|
| `decomposed` (`3N` per-writer keys, `LwwResolver`) | 2,000 | 6,000 | 0.613 s | 44 | 466,503 | 6,000 |
| `merged` (`N` keys, `PnCounterResolver`) | 2,000 | 2,000 | 0.609 s | 38 | 230,030 | 2,000 |
| `decomposed` (`3N` per-writer keys, `LwwResolver`) | 50,000 | 150,000 | 0.906 s | 680 | 10,791,609 | 150,000 |
| `merged` (`N` keys, `PnCounterResolver`) | 50,000 | 50,000 | 0.723 s | 145 | 2,585,995 | 50,000 |

**Large-entity convergence** (a warm 3-node trio, `N` entities written
concurrently from all three nodes with anti-entropy live throughout,
`converge_secs` timed from the last write; `lost_updates` is a snapshot taken
immediately after the writers finish and *before* the convergence wait —
the transient post-write, pre-convergence gap, not a permanent loss: every
variant at every `N` below reaches the exact expected total by the time
`converge_secs` elapses). `coalesced` uses `Cache::merge` with a 1 ms window
in place of a direct `insert`. Default `N=4,000` is the median of the same
three runs; `N=100,000` is one filtered run (79.02 s total for all three
variants, measured against `Shard::flush_due_pending_merges`'s current,
stripe-sharded, deadline-ordered sweep — see "Where merge loses" and "Scale:
what was run, what remains" for what that replaced):

| Variant | N | resident keys | converge time | frames | bytes | pre-convergence gap | AE repairs |
|---|---:|---:|---:|---:|---:|---:|---:|
| `decomposed` (`3N` per-writer keys) | 4,000 | 12,000 | 0.038 s | 6 | 1,265,562 | 8,000 | 0 |
| `merged` (`N` keys) | 4,000 | 4,000 | 0.049 s | 6 | 1,031,430 | 8,000 | 0 |
| `coalesced` (`N` keys, 1 ms window) | 4,000 | 4,000 | 0.049 s | 6 | 1,031,448 | 12,000 | 0 |
| `decomposed` (`3N` per-writer keys) | 100,000 | 300,000 | 1.177 s | 3,702 | 84,762,074 | 200,000 | 586,049 |
| `merged` (`N` keys) | 100,000 | 100,000 | 1.359 s | 8,263 | 81,231,679 | 200,000 | 824,218 |
| `coalesced` (`N` keys, 1 ms window) | 100,000 | 100,000 | 1.249 s | 3,475 | 57,811,395 | 296,492 | 533,516 |

**Sketch path** (`sketch_path_convergence_*`: `ClusterConfig::ae_sketch_min_bucket`
lowered to 4, `keys` filler entries forced into one anti-entropy bucket,
then a quarter of them invalidated on one node so the next mismatch answers
with an IBLT sketch rather than a listing; `sketch_rounds` is
`sketch_peeled + sketch_fallback`, both read straight off
`sundog_ae_sketch_total`). Default `keys=200` is the median of the same
three runs; `keys=100,000` is one filtered run (73.43 s total for
both resolvers):

| Resolver | keys | converge time | frames | bytes | sketch peeled | sketch fallback | AE repairs |
|---|---:|---:|---:|---:|---:|---:|---:|
| `lww` (non-merging) | 200 | 0.128 s | 6 | 12,288 | 1 | 0 | 50 |
| `pn_counter` (merging) | 200 | 0.085 s | 6 | 12,475 | 1 | 0 | 50 |
| `lww` (non-merging) | 100,000 | 0.160 s | 162 | 6,332,829 | 0 | 0 | 106,139 |
| `pn_counter` (merging) | 100,000 | 0.201 s | 207 | 8,788,271 | 0 | 0 | 121,218 |

At `keys=200` the seeded 50-element mismatch peels cleanly for both
resolvers (`sketch_fallback=0`): the bidirectional rule from "The sketch
path" applies identically whether the peeled result came from a merging or
non-merging resolver, so this axis isolates the peel-vs-fallback mechanism
itself rather than a merge-specific cost — merge's usual per-apply and
lock-contention costs from the other tables still apply on top of whichever
path the sketch takes.

At `keys=100,000` both resolvers still repair correctly (`ae_repaired_total`
is nonzero, and every invalidated key reconverges inside the wait), but
`sketch peeled`/`sketch fallback` both read zero rather than showing
fallbacks as the scale knob's own doc comment anticipated: a 100,000-entry
dense bucket exceeds `ClusterConfig::ae_part_min_bucket`'s default (4,096)
well before it exceeds the sketch's own rated capacity, so `net::conn`'s
big/small split routes this bucket to `AeMismatch::PartDigests` instead of
`AeMismatch::Bucket`/`Sketch` — the bucket-level sketch mechanism this
scenario instruments never engages at this scale, and the repair instead
goes through the part-digest tier's own per-part listing-or-sketch decision,
counted under `sundog_ae_parts_total`, a metric this scenario does not read.
This is a real finding about the two thresholds' relative scale, not a
converted or missing measurement: raising `SUNDOG_BENCH_KEYS` past
`ae_part_min_bucket` moves the mismatch to a different tier entirely rather
than stressing the sketch's own fallback path, which would need
`ae_part_min_bucket` raised alongside `ae_sketch_min_bucket` to actually
reach.

**Partition-heal** (`sim` suite, `cargo test -p sundog --features sim --test
sim partition_heal -- --nocapture`, and `SUNDOG_SIM_FULL=1` for the full
grid: three simulated nodes, node `a` split from `b`/`c`, counters each
incremented 3 times per side while partitioned, then healed; metrics cover
anti-entropy from the heal onward). The harness drives `sundog`'s real
`run_round_against` entry point under a deterministic, zero-latency
schedule (`HealConfig`/`Variant`) across a `conflict_fraction` sweep — the
share of keys touched by both partition sides, from `0.0` (every key
diverges on exactly one side) to `1.0` (every key is a genuine two-sided
conflict) — reporting `ae_rounds`, `records`/`applies` moved, `folds`
performed, and `redundant_pulls` (a key pulled again after it already
converged) alongside the exact-convergence check the sim already made.
`partition_heal_rounds_are_monotone_in_key_count` pins that
neither variant's round count regresses as key count grows, from 2,000
through 40,000 keys, at full conflict:

Every field below is the median across the full grid's 3 seeds
(`0xC0DE_7001`/`7002`/`7003`). Lost updates are 0 at every row in both
tables — omitted as a column since it never varies — and every field except
`bytes` (`ae_rounds`, `records`, `folds`, `redundant_pulls`,
`ae_rounds_ratio`) lands on the exact same value across all 3 seeds too;
`bytes` and `bytes_ratio` are the only fields with meaningful seed-to-seed
spread (`NodeId::merge_derived`'s hash-dependent varint size), shown as
`median (min–max)` where the two differ and as a single value where they
don't.

**`keys=2,000`**, columns grouped by variant, `redundant pulls` and the two
ratio columns (`merged` over `decomposed`, from `SIM partition_heal_pair`)
last:

| conflict_fraction | decomposed AE rounds | decomposed bytes | decomposed redundant pulls | merged AE rounds | merged bytes | merged redundant pulls | ae_rounds_ratio | bytes_ratio |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.0 | 3 | 867,522 (867,468–867,534) | 0 | 3 | 823,301 (823,280–823,340) | 0 | 1.000 | 0.949 |
| 0.1 | 4 | 1,112,339 (1,110,808–1,126,378) | 0 | 4 | 1,011,114 (1,010,206–1,011,126) | 0 | 1.000 | 0.897–0.910 |
| 0.5 | 4 | 1,450,786 (1,436,772–1,450,792) | 0 | 4 | 1,118,882 (1,118,788–1,118,920) | 0 | 1.000 | 0.771–0.779 |
| 1.0 | 4 | 1,844,429 (1,829,369–1,844,454) | 0 | 3 | 1,046,169 (1,036,197–1,056,184) | 0 | 0.750 | 0.562–0.573 |

**`keys=20,000`**:

| conflict_fraction | decomposed AE rounds | decomposed bytes | decomposed redundant pulls | merged AE rounds | merged bytes | merged redundant pulls | ae_rounds_ratio | bytes_ratio |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.0 | 3 | 7,312,295 (7,312,223–7,312,341) | 0 | 3 | 7,095,429 (6,975,483–7,095,430) | 0 | 1.000 | 0.954–0.970 |
| 0.1 | 4 | 8,431,348 (8,307,628–8,445,566) | 0 | 3 | 8,253,856 (8,253,779–8,376,012) | 2,000 | 0.750 | 0.977–0.994 |
| 0.5 | 4 | 12,059,758 (11,983,809–12,059,761) | 0 | 4 | 13,333,209 (13,333,176–13,457,021) | 15,879 | 1.000 | 1.106–1.116 |
| 1.0 | 4 | 16,627,076 (16,467,070–16,766,966) | 0 | 3 | 9,681,956 (9,681,949–9,681,962) | 0 | 0.750 | 0.577–0.588 |

`folds` and `records` (omitted above for width) follow the same pattern the
per-variant numbers in prose below cite directly where they matter: at
`keys=20,000`/`conflict_fraction=0.5`, `merged` folds 73,844 times over
147,687 records against `decomposed`'s 0 folds over 121,808 records — the
row `redundant_pulls` singles out as the byte-cost exception.

At every `(keys, conflict_fraction)` this grid drives, `merged`'s round
count is never worse than `decomposed`'s (`ae_rounds_ratio` never exceeds
1.000) — 3 vs. 4 at full conflict, at both 2,000 and 20,000 keys, matching
at every fraction where an ordinary anti-entropy digest already repairs the
whole partition without a second round. `partition_heal_rounds_are_monotone_in_key_count`
extends the round-count check across 2,000 through 40,000 keys at full
conflict for both variants and finds no regression at any point in between.
Both variants converge to the exact expected total with zero lost updates
at every row measured. This harness therefore does not reproduce the
round-count inversion an earlier, since-replaced version of this sim once
measured at 20,000 keys; see "Where merge loses" and "Scale: what was run,
what remains" below for why that is not the same as confirming the
underlying regression is fixed rather than no longer observable under this
harness's timing margin. The bytes ratio is where the full fraction sweep
earns its keep over the fast default grid's `0.0`/`1.0` pair alone:
`conflict_fraction=0.5` at 20,000 keys is the one row where `merged` moves
*more* bytes than `decomposed` (ratio 1.106–1.116) despite an equal round
count, and it is also the only row with a nonzero `redundant_pulls` at that
scale (15,879) — see "Where merge loses" for the mechanism.

**Million-counter scale.** Two runs past every default above, one in-process
and one in containers, both with three writers incrementing every counter
once and every variant reaching the exact total on every node.

`large_entity_convergence` at `SUNDOG_BENCH_KEYS=1000000` (one run, 321 s
for all three variants; `converge_secs` timed from the last write, the
pre-convergence gap omitted since every variant closes it):

| Variant | N | resident keys | converge time | frames | bytes | AE repairs |
|---|---:|---:|---:|---:|---:|---:|
| `decomposed` (`3N` per-writer keys) | 1,000,000 | 3,000,000 | 15.644 s | 27,256 | 1,359,939,450 | 8,623,241 |
| `merged` (`N` keys) | 1,000,000 | 1,000,000 | 13.751 s | 29,378 | 939,941,852 | 9,319,484 |
| `coalesced` (`N` keys, 1 ms window) | 1,000,000 | 1,000,000 | 19.682 s | 18,186 | 860,172,618 | 9,459,659 |

`cold_join_warms_a_million_counter_cluster_with_exact_totals` in the
container suite (`SUNDOG_CONTAINER_TESTS=1`, `RIGHTSIZE_BACKEND=docker`,
`--test-threads=1`; three `sundog-testnode` containers on a `pn` cache under
`PnCounterResolver`, each incrementing every one of a million counters once,
then a cold fourth node), against `cold_join_warms_a_million_entry_cluster`,
the same suite's single-writer last-write-wins bar:

| Scenario | settle to exact totals | cold join (incl. container boot) | donor frames | donor bytes | joiner resident entries |
|---|---:|---:|---:|---:|---:|
| million single-writer entries, `LwwResolver` | n/a, one writer | 5.33 s | — | — | 1,000,000 |
| million counters, three writers, `PnCounterResolver` | 61.1 s | 6.61 s | 2,817 | 208,486,980 | 1,000,000 |

At a million counters merge converges faster than decomposition and moves
31% fewer bytes; the resident-entry ratio is the writer count, as at every
smaller size. The container join of a million merged counters costs 1.3 s
more than a million single-writer entries, the price of a per-writer map in
every record. The 61 s settle is three million conflicting folds across
three containers on a four-core box, every one paying the per-apply cost
above.

## Where merge wins

**No lost updates, ever, when it converges — and it converges.** Both
`naive_lww_counter`'s companion `decomposed_counter` control and every
merged scenario — blind, read-modify-write, and every coalescing window —
lose nothing; `naive_lww_counter` itself drops roughly 40% of 1,600
concurrent blind increments (median 626 lost) since last-write-wins keeps
only one side of every colliding pair. The sim partition-heal run confirms
the same result under an actual network partition and a real anti-entropy
repair at default scale: `merged` lands on the exact expected total with
zero lost updates, matching the `decomposed` control exactly, and at full
conflict does so in fewer anti-entropy rounds (3 vs. 4) — the bidirectional exchange means
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
variant's 6,000 records (`3N`) over 44 frames and 467 KB — roughly 2.0x the
bytes for 3x the resident-side keys — and at `N=50,000` that gap widens to
roughly 4.2x the bytes (10.8 MB vs. 2.6 MB) for the same fixed 3x key ratio.
This is a fixed-slot win that compounds as either the writer count or the
entity count grows, not a per-write one.

**Pre-folding turns a batch's per-collision cost into roughly one entry's,
though this run's own `WRITERS=8`/`ITERS=200` scale is small enough that the
saving competes with real noise.** `apply_ns_merge` vs. `apply_ns_lww`
(1,195.3 ns vs. 597.0 ns, "Per-apply CPU cost" above) is the clean measure
of what one fold actually costs; pre-folding's own job is collapsing many of
those costs into one, which only shows cleanly when a batch's same-key run
is long enough to dominate per-run noise. At this run's 1,000-entry
`apply_many_prefold` scale, `PnCounterResolver`'s `one_key` gap (47,471 ns)
and `LwwResolver`'s same-shape gap (-77,662 ns, `merges()` is `false` so it
never engages pre-fold) sit close enough in magnitude on this run's own
4-vCPU, shared machine that neither isolates the effect cleanly on its own;
the per-apply CPU numbers above are the more reliable read on the cost this
lever actually removes per collision.
`replicated_hot_counter_receive`'s on/off comparison shows the same
mechanism on the receive path directly, and at this shape's small scale
also reads as directional (3 → 4 applies per node) rather than a clean
multiple — see the table above.

**Coalescing turns `writers × iters` calls into one applied record per
window, and now scales the flush that does it.** `merged_counter_coalesced`'s
`engine applies` column drops from 1,600 (every call applies) at a zero
window to 1-2 at either a 1 ms or 10 ms window — nearly every one of the
1,600 client-side `merge` calls across all eight writers folds into the
handful of records the window flushes. Both nonzero windows' per-call
`merge` latency runs faster than the zero window's (no per-call engine round
trip to wait on), and both still converge to the exact expected total
inside the same 30-second wait the always-apply scenarios use — the
staleness this lever trades away is bounded and never costs correctness.
`Shard::flush_due_pending_merges`'s sweep now mirrors `engine::Engine`'s own
`BUCKET_COUNT`-stripe split: `Shard::pending_merges` is
`BUCKET_COUNT` independently locked stripes, one per `stripe_index_from_hash`
bucket, each with its own `by_deadline: BTreeMap<(u64, u64), K>` index, so a
flush locks and pops only the entries actually due in one stripe at a time
rather than one shard-wide mutex guarding a single `HashMap` the sweep had
to scan in full on every tick. `large_entity_convergence_coalesced` at
`N=100,000` shows the result directly: 1.249 s, between `decomposed`'s
1.177 s and `merged`'s 1.359 s rather than the scenario's slowest variant by
a wide margin, and it moves the fewest bytes of the three (57.8 MB against
decomposed's 84.8 MB and merged's 81.2 MB) — see "Scale: what was run, what
remains" for what this replaced.

## Where merge loses

**Slower per-apply CPU.** `apply_ns_merge` costs roughly 2x
`apply_ns_lww`'s ns/apply (1,195.3 ns vs. 597.0 ns) and both its p50 and p99
run higher. `PnCounterResolver::needs_value_bytes()` is `true`, forcing a
decode of both sides and a re-encode on every collision, against LWW's
version-only comparison; `apply_ns_lww_forced_bytes` (LWW forced through the
same byte-materialization path) lands close to plain LWW (599.4 ns),
confirming the gap is the merge logic itself, not byte materialization.

**Single shared-key stripe-lock contention, pre-folding included.**
`merged_counter_blind`'s median throughput (217,407 writes/sec) still trails
`decomposed_counter`'s (1,411,967 writes/sec) by roughly 6.5x — both funnel
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
`decomposed` send the same 6 frames, but at `N=100,000` `merged` sends 8,263
against `decomposed`'s 3,702 — while still moving fewer total bytes (81.2 MB
vs. 84.8 MB). `PnCounterResolver::merges()` being `true` makes every
version-mismatched key exchange in both directions every round instead of
only the greater side pushing, so a key that keeps mismatching across
several live anti-entropy rounds under concurrent write pressure costs more
round-trip messages even though each message carries less redundant data
than decomposition's non-overlapping per-writer keys would.

**Redundant pulls can cost more bytes than the round-count win saves, at a
partial conflict fraction.** At `conflict_fraction=1.0` (every key a
genuine two-sided conflict) `merged` matches or beats `decomposed`'s round
count at every key count this grid drives (see the Partition-heal table
above) — the reverse of an earlier, since-replaced version of this sim,
which once measured `pn_counter` needing 5 anti-entropy rounds against
`lww_decomposed`'s 3 at 20,000 keys under a shorter, race-prone
anti-entropy tick (see `HEAL_AE_INTERVAL_MS`'s own doc in
`sundog/tests/sim.rs` for that race). At `conflict_fraction=0.5` and 20,000
keys, though, `merged` moves *more* bytes than `decomposed` despite an
equal round count (13.3 MB vs. 12.1 MB, `bytes_ratio` 1.11-1.12 across three
seeds) — `redundant_pulls` (15,879) shows why: a key one side pulled and
merged can still mismatch the *other* side's sketch on a later round within
the same repair, costing a second pull for content anti-entropy had already
converged on. The per-key convergence argument bounds how many rounds *one*
divergent key's own repair costs, not how many bytes a whole partition's
mix of converged and still-diverging keys costs to drain when only some of
them are two-sided conflicts. This isn't every partial fraction, though: at
`conflict_fraction=0.1` and 20,000 keys `merged` has a nonzero but far
smaller `redundant_pulls` (2,000) and still moves *fewer* bytes than
`decomposed` (`bytes_ratio` 0.977-0.994) — the cost is real but does not
show up as a byte-count loss at every conflict mix below full, only at the
one row measured here. Both variants still converge to the exact expected
total with zero lost updates in every case; only the byte cost shifts. See
"Scale: what was run, what remains" below.

## Scale: what was run, what remains

What was run: the default-scale benchmark
(`SUNDOG_BENCH=1 cargo test --release -p sundog --features prometheus --test
crdt_bench -- --test-threads=1 --nocapture`) three times at `WRITERS=8`/
`ITERS=200`, all 18 tests passing on all three runs; four scenarios
(`cold_join_initial_replication`, `large_entity_convergence` plain and
coalesced, `sketch_path_convergence`) rerun once each past their defaults via
`SUNDOG_BENCH_KEYS` — 50,000 for cold join, 100,000 for large-entity and the
sketch path, and 1,000,000 for large-entity; the million-counter cold join in
the container suite; and the partition-heal sim's fast default grid (folded into the
crdt_bench runs above) plus its full `SUNDOG_SIM_FULL=1` sweep — 2,000 and
20,000 keys, the whole 0.0/0.1/0.5/1.0 conflict-fraction range, three seeds,
all four sim tests passing. What remains, past what this run covers:

- **Redundant pulls at a partial conflict fraction.** A key already
  converged through one side's pull can still be re-pulled by the other
  side's own sketch mismatch later in the same repair (`redundant_pulls` in
  `sundog/tests/sim.rs`'s partition-heal metrics) — measured costing
  `merged` more total bytes than `decomposed` at `conflict_fraction=0.5`
  and 20,000 keys, despite an equal round count. Needs root-causing before
  the exchange's byte cost can be trusted at every conflict mix, not only a
  fully two-sided one. Separately, `HEAL_AE_INTERVAL_MS`'s own doc in
  `sundog/tests/sim.rs` explains why this whole sim family runs at a longer,
  race-free anti-entropy tick than production's 200ms default — a
  production-cadence variant is still needed to confirm the round-count
  regression an earlier version of this sim once measured is fixed, rather
  than merely no longer observable under this harness's generous timing
  margin.
- **`PnCounter` writer-slot compaction.** A departed writer's `p`/`n` slots
  never shrink — `pn_counter::tests::encoded_size_at_3_10_and_100_distinct_writers`
  measures a fresh, increment-only counter's postcard-encoded size at 8, 22,
  and 202 bytes for 3, 10, and 100 distinct writers respectively (see
  "Writer-slot growth" above for the full table and why this floor
  understates a counter with mixed increment/decrement writers or larger
  `NodeId`s). Safe compaction needs membership-wide agreement that every
  replica has already folded that writer's final value, which sundog's
  gossip does not provide today.
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

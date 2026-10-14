# Choosing a resolver

A cache's resolver decides what happens when two writes to the same key
meet. `CacheBuilder::resolver` sets it once per cache, and the cache keeps
that resolver for its lifetime. The default, `LwwResolver`, keeps the write
with the later version and drops the other. A merging resolver folds both
writes into one value instead: the value is a CRDT, a conflict-free
replicated data type, and every node that receives the same writes reaches
the same value in any order, as long as no deadline lapses between them
([Expiry and deletes](#expiry-and-deletes)). `sundog::crdt` ships two:

- `PnCounter` with `PnCounterResolver`, a counter that goes up and down.
- `OrSet<T>` with `OrSetResolver::<T>::new()`, a set of members where an
  add wins over a concurrent remove.

## Keep the default when

- **The cache holds a copy of something that lives elsewhere**: a database
  row, an API response, a rendered page. The source decides the value, a
  later write replaces an earlier one, and a miss loads it again. See
  [Read-through over sqlx](recipes/read-through.md).
- **A value is replaced as a whole**: a session, a profile, a settings
  document. Two concurrent writes of a whole value have no meaningful
  combination, so the later one wins.
- **Each key has one writer at a time.** With no concurrent writes to a
  key, last-write-wins loses nothing.

## Use a CRDT when all of these hold

1. Several nodes update the same key at the same time, and every update
   has to count, not only the latest.
2. The value is an aggregate the cache itself owns, a count or a set built
   from many small updates, rather than a copy read from a source.
3. A value that converges is enough. A merge cannot enforce a limit such as
   "never below zero" or "at most one holder".
4. Each contribution stands until a writer takes it back on purpose. A
   writer's increments and adds stay in the value after its node crashes,
   so a count of something tied to a running process, such as open
   connections or users online, drifts upward with every crash.

| Value | Resolver |
|---|---|
| views, requests or events counted on every node | `PnCounter` |
| a score that goes up and down, such as votes | `PnCounter`, with `local_decrement` |
| tags or labels on an item, added and removed from any node | `OrSet` |
| a cached database row or API response | the default, with a loader |
| a session, a profile, a document | the default |
| who is online, open connections | the default, one key per member with a TTL its heartbeat renews |
| a stock level or quota that must never go negative | the database, in a transaction |
| a lock, a lease or a leader | a coordination service |

## Every mode merges

| Mode | A merge |
|---|---|
| `Local` | folds this node's writes; an atomic in the process is simpler unless the value needs a TTL, eviction or events |
| `Invalidation` | sends the writer's merged value to every node; a node holding the key folds it in, a node without it ignores it |
| `Replicated` | sends the merged value to every node; anti-entropy repairs a lost message |
| `Distributed` | goes to the key's owners, which merge it; a non-owner reads with `fetch` |

In `Invalidation`, a node's value holds every write made on that node and
every write it received while it held the key. No anti-entropy runs, but
each write carries the writer's whole value, so the writer's next write
repairs a message a peer missed.

## Writing a counter

Each writer sends its own cumulative total, never a one-off increment:
`PnCounter::local_delta(cache.writer_id(), total)`, where `total` is this
process's running count for the key. Two writes of `local_delta(w, 1)`
merge to 1, not 2, so a retried write never counts twice. Decrements carry
a separate cumulative total, `PnCounter::local_decrement(writer, total)`.

Every task in one process shares one writer id, `Cache::writer_id`, so the
tasks share one running total per key, an `AtomicU64` for example. The
total starts at zero in each run of the process: a restart writes under a
new writer id, in a slot of its own.

`Cache::merge` writes without a read. `Cache::insert` merges the same way:
under a merging resolver no write overwrites a value. `get` returns the
merged value, and `PnCounter::value` reads the count. A
`Distributed` non-owner reads with `fetch`.

```rust
{{#include ../cookbook/src/counters.rs:counter}}
```

## Writing a set

`OrSet::add(writer, seq, element)` adds `element` under a sequence number
unique to the writer, a running counter in the process. To remove an
element, read the set, then write `OrSet::remove(&observed, &element)`
with `Cache::merge`. The remove takes out the adds the read observed; an add
of the same element made concurrently elsewhere survives. Never use
`Cache::remove` for one element: it deletes the whole set. Every add
creates a tag that stays until a remove observes it, so re-adding an
element that is already present, on every heartbeat for example, grows the
value with each write. `OrSet::contains` and `OrSet::iter` read the members.

## Expiry and deletes

`CacheBuilder::ttl`, `Cache::merge_with_ttl` and `Cache::insert_with_ttl`
give merged values a lifetime as they give any other value one:

- **A merge keeps the later deadline.** Folding two values keeps the later
  of their two deadlines, and a value with no deadline takes the other
  side's. A key expires once no write has extended it past its deadline.
- **`expire` extends and never shortens.** Every node keeps the later of
  the deadlines it sees, so a shorter one cannot win anywhere, and
  `persist` leaves a merged value's deadline as it is. `touch` moves it to
  the cache's TTL from now when that is later.
- **An expired value stays expired.** A write made after a value's deadline
  starts a new value; the expired value never folds into it. On the node
  applying a write, a value that reads as absent there, expired or idle
  past `tti`, never folds either.
- **Nodes agree while no deadline lapses between the writes.** Dropping an
  expired value is not a merge, so once a deadline falls between writes the
  order they arrive in can change the result. A merged value keeps the
  longest lifetime among its parts, so a longer-lived record can carry a
  contribution back to a node that had already dropped it as expired.
  `merge_coalesce_window` changes which contributions survive for the same
  reason. Anti-entropy settles replicas that hold the key; `Invalidation`
  runs none, so there the difference stays until the next write or the
  expiry.
- **Expiry ends a value, not a writer's running total.** A writer that
  keeps counting after the deadline sends its whole total again, and the
  new value starts from that total. To count per period, write one key per
  period, such as one per day, with a TTL a little longer than the period,
  and start a fresh running total for each key.
- **A delete is a tombstone.** `Cache::remove` writes a tombstone, and a
  tombstone and a value settle by version, as under every resolver.
  A writer's next write after the remove sends its whole running total
  again, so a remove does not reset a count; a new key does.

## What a CRDT costs

- **Every write ships the whole value.** Replication is state-based: a
  write sends the merged value, not the change, and anti-entropy exchanges
  both records of a mismatched key instead of one. A custom resolver that
  reads values pays the same, and `needs_value_bytes() == false` avoids it.
- **Every write that meets a stored value decodes both, merges them and
  encodes the result.** `CacheBuilder::merge_coalesce_window` folds a key's
  consecutive `Cache::merge` and `Cache::merge_with_ttl` calls in memory and
  applies them once per window. A read does not see a fold still waiting in
  its window, and a crash loses it. `insert` never waits in a window, and
  `open` refuses a nonzero window for a resolver that does not merge.
- **The value grows with its writers.** Each writer, one run of one node,
  owns a slot, so every restart adds one. The slots of writers whose node is
  gone longer than `ClusterConfig::crdt_retire_after`, 24 hours by default,
  or has restarted, retire and later fold into one total. A replica cut off
  while that happens can count a folded writer twice;
  [Guarantees](guarantees.md#values-that-merge) has the bound. Folding an
  `OrSet` drops only its bookkeeping, never an add. An `OrSet` writer, once
  retired, adds nothing new; its node adds again after a restart, under a
  new writer id.

## Every node needs the same resolver

Nodes compare a cache's mode when it opens, but not its resolver or its
value type. Open a cache with the same resolver and value type on every
node, including through a rolling upgrade. To change either, open a cache
under a new name.

## Writing your own

Implement `ConflictResolver`:

- `winner` is deterministic and antisymmetric and puts every distinct
  version of a key in one order. It may prefer a record with a lower
  version. Anti-entropy exchanges both records of a mismatched key for every
  resolver that reads values, so a replica holding the winner at the lower
  version sends it, each side's `winner` decides what it keeps, and the
  adopting replica stores the winner under its own, lower, version.
- A tombstone and a value settle by version, whatever `winner` prefers: the
  higher version wins and a tie keeps the stored side. A resolver cannot
  revive a deleted key or bury a newer value under an older tombstone.
- `needs_value_bytes` returns `false` only for a resolver that compares
  `ver` alone and prefers the greater version. It skips encoding the values
  on every apply and keeps anti-entropy's one-way repair, the greater record
  travelling to the replica that holds the lesser. A resolver that returns
  `false` yet prefers a lower version leaves replicas apart after a lost
  replicate.
- `merges` returns `true`, and `merge` is commutative, associative and
  idempotent over a canonical encoding, so every node reaches the same
  bytes in any order. The `crdt` types build on `BTreeMap` and `BTreeSet`
  for that reason. `needs_value_bytes` stays `true`.
- `merge` sets the merged value's expiry with a rule that is itself
  commutative, associative and idempotent; `later_deadline` is the rule the
  shipped resolvers use. The engine never hands `merge` a side that expired
  before the other side's write.
- A value that keeps per-writer state implements `compact` and `settle` to
  bound it.

The trait's
[API documentation](https://docs.rs/sundog/latest/sundog/store/trait.ConflictResolver.html)
states the full contract.

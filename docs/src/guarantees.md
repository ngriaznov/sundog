# Guarantees

sundog is eventually consistent. Every node that stays connected converges
on the same content for a shared cache. There is no consensus round and no
leader, so a write returns once it applies locally, and peers learn of it
a moment later.

## Conflicting writes

Every write carries a hybrid logical clock stamp: wall-clock milliseconds,
a logical counter, and the writing node's id as a final tiebreak. By
default the higher stamp wins, everywhere, regardless of the order writes
arrive in. Two nodes writing one key at nearly the same instant both
succeed locally, and the older write disappears once the newer one
reaches it. No error reports the loser.

Keep node clocks synchronized with NTP or your platform's time service. A
node whose clock runs ahead stamps its writes later than its peers' and
wins their conflicts.

### Values that merge

A value that combines, a counter or a set, does not have to lose writes. A
`ConflictResolver` that implements `merge` folds the stored and incoming
records into a third value, and every node reaches the same result in any
order. `sundog::crdt` ships two: `PnCounter` with `PnCounterResolver` and
`OrSet` with `OrSetResolver`.

```rust
{{#include ../cookbook/src/counters.rs:counter}}
```

`Cache::merge` writes through the resolver without a read.
`CacheBuilder::merge_coalesce_window` folds a writer's consecutive merges
to one key into one applied record per window, and a read does not see a
fold still pending inside its window.

Each writer owns a slot in a merged value. A background sweep retires the
slot of a writer gone longer than `ClusterConfig::crdt_retire_after`, 24
hours by default, and folds it away once the retirement itself has aged
past a second bound with every peer quiet. A replica isolated for longer
than that, still holding a slot every other replica has folded, counts
that writer's contribution twice when it reconnects. The limit matches the
one `tombstone_max_ttl` sets for deletes, below.

## Deletes

`remove` writes a tombstone that replicates like a value and outvotes
every older copy of the key. Tombstones stay for `tombstone_ttl`, 10
minutes by default. While any member that gossip remembers is absent, a
`Replicated` cache keeps its tombstones longer, up to `tombstone_max_ttl`,
24 hours by default, so a node returning from a partition cannot bring a
deleted key back.

A member gone longer than `tombstone_max_ttl` returns after the tombstone
is collected, and its stale copy can come back, limited by that copy's own
expiry. Give every entry a TTL and deletes stay deleted whatever the
downtime. Without TTLs, raise `tombstone_max_ttl` above the longest outage
you tolerate.

## Expiry

A TTL-expired entry never returns. Every record carries its absolute
expiry, so it expires at the same instant on every node, and no node
accepts a copy past its expiry from a peer. `CacheBuilder::ttl` sets a
cache's default lifetime, and `insert_with_ttl` and `insert_many_with_ttl`
set one entry's or one batch's. Reads never extend or accept a TTL.

`CacheBuilder::tti`, an idle timeout, is local to one node and applies only
to `Local` and `Invalidation` caches.

## Per mode

| Mode | A reader sees | After a node's partition heals |
|---|---|---|
| `Local` | its own writes | nothing to converge |
| `Invalidation` | its own copy, dropped when any node writes the key | copies it missed invalidations for stay until their TTL |
| `Replicated` | its own copy of every entry, a moment behind a write elsewhere | anti-entropy reconciles every entry by version |
| `Distributed` | via `fetch`, the owners' copy | owners reconcile by version; each side of the partition accepted writes for its own view |

An `Invalidation` cache sends values nowhere and runs no anti-entropy, so a
TTL is its bound on staleness. Set one on every `Invalidation` cache.

A `Distributed` cache has no quorum. During a partition each side computes
its own owners and accepts writes, and the two sides settle by version
when it heals. With two owners, both owners of one bucket failing inside
one rebalance window lose that bucket; set `owners` to 3 or more where
that matters.

## What sundog does not promise

- **Durability.** A cache that loses every node loses its data. The spill
  tier's warm reopen keeps data across a clean restart of one node, not a
  crash.
- **Read-your-writes across nodes.** A write on one node reaches the others
  shortly after, not before the call returns.
- **Mutual exclusion.** Gossip membership has no quorum, so a lock or a
  lease built on sundog can have two holders. Use a consensus system for
  those.

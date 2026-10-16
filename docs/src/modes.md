# Choosing a mode

| Mode | Each node stores | A write | A read | Pick it when |
|---|---|---|---|---|
| `Local` | its own entries | stays on this node | local | you want an in-process cache with TTL and a size bound, and no cluster traffic |
| `Invalidation` (default) | its own working set | drops the key on every other node, or under a merging resolver folds into each copy | local | the dataset is too large to hold everywhere and each node serves mostly its own hot keys |
| `Replicated` | every entry | sends the value to every node | local, never waits on the network | the dataset fits on one node and reads must never touch the network |
| `Distributed` | the parts it owns | goes to the key's owners | `get` is local; `fetch` asks an owner | the dataset is too large for one node and must survive a node loss |

Every node gossips the mode of each cache it has open. Opening a name
under a mode that conflicts with a live peer's fails with
`CacheError::ModeMismatch`. TTL and capacity are local settings and may
differ between nodes.

## Local

No messages leave the node. Use it for a process-local cache that still
wants sundog's expiry, eviction and read-through.

## Invalidation

A write on one node sends the key and its new version to every peer, and
each peer drops its older copy. Every node fills its own copy from its
loader or its own writes.

A write drops the peers' older copy even when this node's entry is gone
before the write is sent. An entry that `max_capacity` evicted or the spill
tier moved to disk, or one that expired, still sends its invalidation, so a
peer never serves a value this node already replaced. A merging resolver is
the exception: its peers keep the merged copy they hold, since an
invalidation cannot say which contributions this node's gone copy lacked.

Values cross the network only when the cache's resolver merges, as
`PnCounterResolver` does. The write then ships the writer's merged record
instead of the bare invalidation. A peer that holds the key folds the record
into its copy, so no node loses the contribution only it held. A peer that
does not hold the key ignores the record and stores nothing, which keeps
each node's cache to its own working set. A removal drops the copy on
every peer, and a peer on a release before protocol 8 gets the bare
invalidation for every write. A merged value keeps one retired entry for
every writer that has died, since nodes here hold different writers and the
sweep does not fold them. [Choosing a resolver](resolvers.md) has the
detail.

This is the mode for a large or expensive dataset: each node holds only
what it reads, bounded by `max_capacity`.

```rust
{{#include ../cookbook/src/deploy.rs:bounded}}
```

## Replicated

Every node holds every entry. A write sends the value to every peer, and a
read answers from local memory.

A node that opens a `Replicated` cache pulls a full snapshot from a peer
that already holds it before `open` returns, then runs one anti-entropy
round against that peer to catch writes made during the pull. A background anti-entropy loop keeps the copies equal
while the cache is open.

A `Replicated` cache takes no `max_capacity` or `tti` without a spill tier:
evicting an entry on one node would only have anti-entropy pull it back
from the others. Bound it with TTLs, or attach the [spill tier](features.md#spill)
to move cold entries to disk. `open` returns
`CacheError::ReplicatedWithLocalEviction` for a capacity without a tier.
A [memory ceiling](sizing.md#memory-ceiling) evicts nothing, so it needs no
tier.

A delete reaches every node as a tombstone that counts its retention from the
delete's stamp. Every node collects it at the same tick, and a node that is
absent, freshly opened or paused defers (see
[guarantees](guarantees.md#deletes)). All nodes see the same absent member, so
they defer together.

## Distributed

A `Distributed` cache splits its keys into 65,536 parts, 64 in each of the
1,024 anti-entropy buckets, and assigns each part to `owners` live nodes by
rendezvous hashing. `Mode::distributed()` uses two owners, and `owners`
must be at least 2. At 100 nodes and two owners the busiest node holds
about 8% more than an even share, and a join moves only the parts the
joiner takes. Anti-entropy between two owners compares one digest per
bucket, folded over the parts they share, so an idle round costs the same
at any cluster size.

A write for a part this node does not own goes to that part's owners. The
forwarded batch carries the writer's ownership view, and an owner whose view
differs passes it on once more, so a write routed under a view that has since
changed still reaches every current owner. A forwarded write is held by no
replica until an owner applies it, so anti-entropy cannot repair it. A
forwarded delete counts its retention from its stamp at the owner, so one that
waits in the send queue for more than `tombstone_ttl` is ignored there (see
[guarantees](guarantees.md#deletes)). An owner
whose clock refuses the write's stamp for clock skew holds it in memory until
its clock is within `max_clock_skew` of the stamp (see
[guarantees](guarantees.md#conflicting-writes)). The sender's writer holds a
batch whose send failed and sends it again on its next connection, along with
what it wrote in the last second, at most 1 MiB of it, and it notices a peer
that closed the connection before it writes. A partition that never delivers a
close can still lose the batches in flight. A node passes a re-forwarded batch
to a lane for each owner, so one owner that stops reading holds back only its
own lane. A lane holds 64 MiB per peer; past that, batches for that peer are
dropped and counted in `sundog_backlog_dropped_total`. When the node is not an
owner of those records, its pass-through copy was the only one, and
anti-entropy cannot repair it. `get` reads this node's copy and returns `None`
off a non-owner, and a read-through fill does not change that: `get_or_load`
and `load` on a non-owner answer and keep no copy. `get_or_load` and
`get_or_insert_with` there ask the owners as `fetch` does and run their loader
only when the owners hold no value or none can answer.
`fetch` is the network-aware read: it answers locally when this node owns the
part and otherwise asks the owners in order, returning `Ok(None)` for a real
miss or `CacheError::FetchUnavailable` when no owner answers within
`fetch_timeout`, or while the part is still cold on every owner that answers.
A part is cold on its new owner until the owner has pulled it, a few seconds
after a membership change. `owners_of` reports a key's owners, and `explain`
shows how a read of a key decides on this node and what each owner answers.

```rust
{{#include ../cookbook/src/deploy.rs:distributed}}
```

A `Distributed` cache opened on a fixed seed list with no known peer waits
up to 5 seconds, or `state_transfer_budget` if shorter, for a first peer
before it computes ownership, so `open` can take that long on the first node
of a seeded cluster. The wait keeps a node from taking itself for the sole
owner of every part just because no peer has reported in yet.

When a node joins or leaves, ownership follows gossip within a few gossip
intervals. A node that gains a part pulls it from the previous owners, and
a node that regains a part it lost pulls it again, since writes went to
other owners meanwhile. A node that shuts down gracefully first reconciles
its parts with their other owners, then announces its departure, and
ownership moves at the announcement; a crashed node keeps its parts until
the failure detector drops it. A
node that loses one keeps serving it for `distributed_disown_grace_rounds`
anti-entropy intervals, hands it to each new owner, and drops it once every
owner confirms. A lost part that no other previous owner still owns, as
when a node opened the cache before its peers and owned every part alone,
goes to its new owners as soon as the views agree, not at the end of the
grace.

A `Distributed` node defers tombstone collection when it shares a part with an
absent member, for every part it owns, so a co-owner that does not defer
ignores the tombstones it keeps offering until it collects them (see
[sizing](sizing.md#tombstones)).

A finite `max_capacity` needs a spill tier here too, and `tti` is refused.
A [memory ceiling](sizing.md#memory-ceiling) needs no tier.
Two nodes that disagree on `owners` for one cache name get
`CacheError::ModeMismatch`.

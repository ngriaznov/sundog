# Choosing a mode

| Mode | Each node stores | A write | A read | Pick it when |
|---|---|---|---|---|
| `Local` | its own entries | stays on this node | local | you want an in-process cache with TTL and a size bound, and no cluster traffic |
| `Invalidation` (default) | its own working set | drops the key on every other node | local | the dataset is too large to hold everywhere and each node serves mostly its own hot keys |
| `Replicated` | every entry | sends the value to every node | local, never waits on the network | the dataset fits on one node and reads must never touch the network |
| `Distributed` | the buckets it owns | goes to the key's owners | `get` is local; `fetch` asks an owner | the dataset is too large for one node and must survive a node loss |

Every node gossips the mode of each cache it has open. Opening a name
under a mode that conflicts with a live peer's fails with
`CacheError::ModeMismatch`. TTL and capacity are local settings and may
differ between nodes.

## Local

No messages leave the node. Use it for a process-local cache that still
wants sundog's expiry, eviction and read-through.

## Invalidation

A write on one node sends the key and its new version to every peer, and
each peer drops its older copy. Values never cross the network. Every node
fills its own copy from its loader or its own writes.

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

## Distributed

A `Distributed` cache splits its keys into 1,024 buckets and assigns each
bucket to `owners` live nodes by rendezvous hashing. `Mode::distributed()`
uses two owners, and `owners` must be at least 2.

A write for a bucket this node does not own goes to that bucket's owners.
`get` reads this node's copy and returns `None` off a non-owner. `fetch` is
the network-aware read: it answers locally when this node owns the bucket
and otherwise asks the owners in order, returning `Ok(None)` for a real
miss or `CacheError::FetchUnavailable` when no owner answers within
`fetch_timeout`. `owners_of` reports a key's owners.

```rust
{{#include ../cookbook/src/deploy.rs:distributed}}
```

When a node joins or leaves, ownership follows gossip within a few gossip
intervals. A node that gains a bucket pulls it from the previous owners. A
node that loses one keeps serving it for `distributed_disown_grace_rounds`
anti-entropy intervals, hands it to each new owner, and drops it once every
owner confirms. A lost bucket that no other previous owner still owns, as
when a node opened the cache before its peers and owned every bucket alone,
goes to its new owners as soon as the views agree, not at the end of the
grace.

A finite `max_capacity` needs a spill tier here too, and `tti` is refused.
Two nodes that disagree on `owners` for one cache name get
`CacheError::ModeMismatch`.

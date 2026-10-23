# Read-through over sqlx

A repository that reads through a cache in front of a database. A read
fills the cache from the database on a miss, and a write to the database
drops the cached copy on every node.

```rust
{{#include ../../cookbook/src/read_through.rs:types}}
```

## The repository

```rust
{{#include ../../cookbook/src/read_through.rs:repo}}
```

- **`Invalidation` mode with `max_capacity`** keeps each node's hot rows,
  bounded, with no values crossing the network.
- **`get_or_load`** runs the query only on a miss, and concurrent misses on
  one id share a single query.
- **`Option<Product>` as the value** caches a missing row too, so repeated
  reads of an id that does not exist stop reaching the database.
- **`remove` after the update** drops the old row on every node. The next
  read on any node loads the new one.

## Writes that bypass the repository

A row changed by another service, a migration or a manual query stays
cached until its TTL expires. The TTL is the bound on how long such a
change stays invisible, so choose it as the staleness you accept. When
another system can emit change events, call `remove` from that consumer
instead.

## Order of operations

Update the database first, then remove the cached entry, so the next read
on any node loads the committed row.

## Loading through a registered loader

`get_or_load` takes its loader per call, so only the calling node can run it.
A cache can instead hold one loader that every node runs the same way, and
read through it with `load` and `load_many`:

```rust
{{#include ../../cookbook/src/read_through.rs:loader}}
```

- **`batch_loader`** reads many keys in one call. A key missing from the
  returned map is one the source lacks. `loader` takes a one-key loader that
  answers `Option<V>` instead.
- **A hit never reaches the loader**, and a key the source lacks is not
  cached, so the next read asks again. Concurrent misses on one key share one
  load.
- **`batch_window`** gathers the misses of every call within the window into
  one loader call of at most `max_keys` keys. Without it, the keys one call
  misses go to the loader together.
- **`refresh_ahead(0.8)`** reloads a key that is read in the last fifth of its
  lifetime. The read answers with the old value, and the new one replaces it
  with a full lifetime, replicated or forwarded like any write. A hot key
  reloads about once per lifetime across the cluster and never misses. A
  reload the source fails keeps the entry until it expires, and one that finds
  the key gone leaves it to expire. It needs a loader and a `ttl`, and applies
  to every mode but `Invalidation`, whose values do not travel between nodes.

### Where a miss loads

In `Invalidation` and `Distributed` mode a missed key loads on one node for
the whole cluster. In `Distributed` that is the first of the key's owners that
has a loader, or this node when none has. An owner that declines, as one does
while it is still pulling a part it gained, or that cannot be reached, passes
the key to the next owner with a loader, and only a key no owner takes loads
on this node. In `Invalidation` it is the rendezvous winner for the key's part
among the nodes that have a loader, and this node loads a key that winner
declines or cannot take. Every node missing the key at the same moment asks
that node, which joins them to its own load, so the source sees one query
however many nodes miss.

In `Invalidation` each node keeps the value it read. In `Distributed` the
owners keep it, and a node that owns nothing of the key keeps no copy, as with
`fetch`. That holds for every load that runs on a non-owner: a `get_or_load`
there, and a `load` that runs on the node itself because no owner has a loader
or none took the key. The node answers with the value, stores it nowhere and
sends it to no one, and a later `get` there is `None`. A non-owner's
`get_or_load` and `get_or_insert_with` first ask the key's owners, as `fetch`
does, and run the loader only when the owners hold no value or none can
answer. Concurrent calls on the node share one owner fetch and at most one
loader call. A key no owner stores runs the loader on every non-owner call
until an owner stores it, through its own read-through, a `load` or a write,
and `load` gathers the key's loads on an owner and queries the source once. A
non-owner that still holds the key through the grace after it lost the part
answers from that copy and asks no one. An owner that loads a
key stores it and sends it to the other owners. An owner that declines runs no
loader, so the key loads once. A request that fails after the owner received
it can have run the owner's loader, so a key can then load on that owner and
again on the next node. A node whose loader fails fails the read with a
`CacheError::Loader` carrying a `RemoteLoaderError` that names it. `Local` and
`Replicated` caches load on the node that misses, and a `Replicated` value
reaches every node once loaded.

In `Invalidation`, a value copied from the node the key's load gathered on
keeps that node's expiry. The copy ends at the instant the source entry does,
whether the source holds the cache's `ttl` counted from its own load or the
lifetime of an `insert_with_ttl`, and a source entry with no expiry leaves the
copy with none. A source entry already past its expiry when the copy arrives
answers the read and is not stored.

A load that an invalidation overtakes, a peer's write announced while the
loader still read the old row, answers its callers and stores nothing.
[Operations](../operations.md#registered-loaders) lists the metrics that count
loader calls and reloads.

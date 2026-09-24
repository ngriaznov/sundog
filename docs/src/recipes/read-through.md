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

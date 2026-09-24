# Getting started

Add the crate:

```sh
cargo add sundog
```

sundog runs on [tokio](https://tokio.rs). Keys and values are any types
that implement `serde`'s `Serialize` and `Deserialize`, plus `Clone`; keys
also implement `Hash` and `Eq`:

```rust
{{#include ../cookbook/src/deploy.rs:types}}
```

## Build a node

A node with every default discovers its peers over mDNS and binds
ephemeral ports. On a LAN that is all the configuration a cluster needs:

```rust
{{#include ../cookbook/src/deploy.rs:first_cluster}}
```

Every process that builds a node under the same cluster name joins the
same cluster. A node with no peers is a healthy one-node cluster, and
peers that start later find it.

mDNS does not cross a Docker bridge network or a cloud VPC. For those,
[Deployment](deployment.md) shows static seeds and DNS discovery.

## Open a cache

A cache is typed and named. Every node that opens the same name under the
same mode shares it:

```rust
{{#include ../cookbook/src/deploy.rs:first_cache}}
```

`insert` stamps the write with a hybrid logical clock, applies it locally
and sends it to the peers. `get` reads this node's copy and never waits on
the network. `remove` writes a tombstone, so the key stays deleted on every
node.

## The rest of the surface

- `get_or_load` reads through a loader on a miss, and concurrent misses on
  one key run the loader once.
- `insert_with_ttl` sets one entry's lifetime; `ttl` on the builder sets
  the default.
- `insert_many` and `remove_many` apply a batch under one lock per stripe.
- `contains_key`, `keys` and `for_each_key` inspect this node's copy.
- `events` streams every `Created`, `Updated` and `Removed` change, tagged
  with whether it came from this node or a peer.
- `get_sync`, `insert_sync`, `remove_sync` and `contains_key_sync` work
  without an async runtime.
- `fetch` reads a `Distributed` cache from a key's owner when this node does
  not hold it.

`cluster.shutdown().await` leaves the cluster gracefully: peers learn the
node left on purpose instead of failing.

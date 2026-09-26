# sundog

sundog is an embedded, replicated cache for Rust services. It runs inside
your process. Every instance of your service finds the others, forms a
cluster over gossip, and keeps named caches coherent between them. There is
no cache server to deploy, no coordinator to keep alive, and no
configuration beyond a cluster name on a LAN.

A cache runs in one of four modes:

- **Local**: an in-process cache with TTL and a size bound, and no cluster
  traffic.
- **Invalidation**: each node caches its own working set, and a write on one
  node drops the key everywhere else.
- **Replicated**: every node holds every entry, and a read never touches the
  network.
- **Distributed**: each key lives on a fixed number of owners, for a dataset
  too large to hold on every node.

Writes are last-write-wins on a hybrid logical clock. Counters, sets and
other values that combine merge through a conflict resolver instead.
Anti-entropy repairs whatever the network drops. Deleted and expired
entries never come back.

## What it is for

- Read-through caching in front of a slower store.
- Session and profile data that tolerates eventual consistency.
- Any per-instance cache whose instances should agree without standing up
  Redis.

## What it is not

sundog is a cache, not a database. Nothing reaches disk unless you
configure the spill tier, and a cache that loses every node loses its
data. There is no consensus, so two writes to one key at the same moment
settle by version, not by a lock. Locks and leases belong in etcd or a
database row.

## Where to go next

- [Getting started](getting-started.md) builds a first cluster.
- [Choosing a mode](modes.md) and [Guarantees](guarantees.md) explain what
  each mode promises.
- [Deployment](deployment.md) covers LANs, containers, VPCs and
  Kubernetes.
- The [API reference](https://docs.rs/sundog) on docs.rs lists every type
  and method.

# Sizing

## Entries per node

How many entries a node holds depends on the mode:

| Mode | Entries on each node |
|---|---|
| `Local`, `Invalidation` | what that node reads or writes, up to `max_capacity` |
| `Replicated` | every entry in the cache |
| `Distributed` | about `owners / nodes` of the entries |

A `Distributed` cache of 30 million entries on six nodes with two owners
holds about 10 million entries per node. After a node leaves, the
survivors pull its buckets, so size each node for the share it holds with
one node gone: 12 million in that example.

## Bytes per entry

A live entry is a 56-byte slot, 80 with the `spill` feature, plus about 5
bytes of hash index. The slot holds the version, the expiry, a last-access
stamp and the encoded key and value together. A key and value that encode
to 22 bytes or less between them live inside the slot with no heap
allocation. A longer pair takes one allocation for the encoded bytes.

Measured on a 4-core Linux machine under glibc, one node, settled resident
memory divided by entry count:

| Shape | Entries | Bytes per entry |
|---|---:|---:|
| 7-byte key, 8-byte value | 4,000,000 | 76.3 |
| 7-byte key, 8-byte value | 64,000,000 | 67.3 |
| 16-byte key, 100-byte value | 4,000,000 | 212.2 |

For comparison, Redis 7 uses 85 to 100 bytes per key for the first shape
and 195 to 230 for the second, from `used_memory` over `DBSIZE`. The README
has the commands to reproduce both sides.

For a shape of your own, estimate 60 bytes per entry plus the encoded key
and value rounded up to the allocator's next size class, then confirm with
the entry diet bench:

```sh
SUNDOG_BENCH=1 cargo test --release -p sundog --test entry_diet_bench \
    -- --test-threads=1 --nocapture
```

`CacheBuilder::capacity_hint` preallocates for an expected entry count at
`open`, which avoids growing each table as it fills. Pass this
node's own share, not the cluster total.

## Tombstones

A removed key keeps a tombstone for `tombstone_ttl`, 10 minutes by default,
and in a `Replicated` cache for up to `tombstone_max_ttl` while a member is
absent. A workload that removes many keys holds their tombstones for that
long. Budget for the removal rate times the retention.

## Allocator

sundog sets no global allocator; your service chooses. Under glibc's
default arenas, bulk loads and anti-entropy leave transient buffers
resident. The demos and the test node use jemalloc, and on a three-node
run of 4 million keys the demo settles at 1.7 GiB under jemalloc against
3.2 GiB under glibc, with bulk ingest about twice as fast.

## The spill tier

With the `spill` feature, a cache's `max_capacity` bounds RAM and entries
past it move to a ring of region files on local disk. Only the value moves.
A spilled entry keeps its slot and index entry in RAM, and its key twice:
once in the slot and once in the index of the region that holds its
value. A read of a spilled entry reads the value back and promotes the
entry into RAM.

`SpillConfig::new(dir, capacity_bytes)` bounds the disk, and
`capacity_bytes` must hold at least two regions of `region_bytes`, 64 MiB
by default. When the ring wraps, the oldest region is reclaimed, and the
entries whose values still live in it leave the cache. Every spilled read
is one random read, so local NVMe serves the tier best.

## Container memory limits

Set a container's memory limit above the resident set the tables above
predict, plus the fan-out backlog: each cache queues up to
`fan_out_backlog_capacity` keys, 262,144 by default, while peers are slow.

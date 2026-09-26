# Tuning

Every setting lives on `ClusterConfig`, and every default suits a LAN
cluster of a few dozen nodes. `ClusterConfig` is non-exhaustive, so change
fields through `with`:

```rust
{{#include ../cookbook/src/deploy.rs:fixed_ports}}
```

Cache-level settings, `mode`, `max_capacity`, `max_resident_bytes`, `ttl`,
`tti`, `resolver`, `weigher` and `spill`, live on the cache builder instead.

## Network

| Field | Default | Change it when |
|---|---|---|
| `gossip_bind_addr` | `0.0.0.0:0` | a firewall needs a fixed gossip (UDP) port |
| `data_bind_addr` | `0.0.0.0:0` | a firewall needs a fixed data-plane (TCP) port |
| `advertise_ip` | probed | peers must dial an address other than the node's own interface |
| `max_frame` | 4 MiB | never raise it past `wire::MAX_FRAME`; lower it to cap single messages |
| `outbox_capacity` | 8,192 frames | a peer's link is slow and bursts need more room per peer |

## Failure detection

| Field | Default | Change it when |
|---|---|---|
| `gossip_interval` | 200 ms | the network's round trip is tens of milliseconds, across regions |
| `phi_threshold` | 6.0 | peers flap between live and dead under jitter; raise it to detect failures later and more surely |
| `phi_initial_interval` | 500 ms | the first heartbeats arrive slower than this |
| `phi_max_interval` | 5 s | never, unless heartbeats legitimately pause longer |
| `dead_node_grace_period` | 10 min | a departed node's id should be forgotten sooner or later |

## Replication and repair

| Field | Default | Change it when |
|---|---|---|
| `ae_interval` | 30 s | repairs after message loss must land faster; each round costs one digest exchange per open cache |
| `max_clock_skew` | 1 min | node clocks drift further apart than this in normal running; `None` accepts any stamp, and one fast clock then wins every conflict and pulls every other clock forward |
| `state_transfer_budget` | 20 s | a joining node's snapshot takes longer to pull; `open` waits this long before opening with a partial copy that anti-entropy completes |
| `fan_out_backlog_capacity` | 262,144 keys | write bursts outrun the network for longer than the backlog absorbs |
| `fan_out_wait_timeout` | 30 s | a writer waiting for backlog room should give up sooner and write over capacity |
| `ae_sketch_min_bucket` | 384 entries | a bucket past this answers a mismatch with a fixed-size sketch instead of a listing |
| `ae_part_min_bucket` | 4,096 entries | a bucket past this narrows a mismatch to 64 part digests first |
| `ae_sketch_cells` | 240 | differences per round regularly exceed the 100 elements a default sketch decodes |

## Deletes

| Field | Default | Change it when |
|---|---|---|
| `tombstone_ttl` | 10 min | tombstones cost too much memory, or a lagging peer needs longer to see a delete; keep it at least 3 × `ae_interval` |
| `tombstone_max_ttl` | 24 h | entries have no TTL and a member can be absent longer than this |

## Distributed caches

| Field | Default | Change it when |
|---|---|---|
| `fetch_timeout` | 750 ms | owners sit across a slower link; a read tries owners in turn, each for this long |
| `distributed_disown_grace_rounds` | 3 | a new owner's pull needs more anti-entropy intervals before the old owner releases |
| `rebalance_concurrency` | 4 | many nodes join at once and transfers should run wider or narrower |
| `rebalance_chunk_bytes` | 1 MiB | transfer chunks should be smaller on a constrained link |
| `rebalance_ack_window` | 60 s | never, in normal operation |

`tombstone_ttl` must cover the bucket release window,
`ae_interval × (2 × distributed_disown_grace_rounds + 2)`, 4 minutes at
the defaults. `open` refuses a `Distributed` cache otherwise, with
`CacheError::TombstoneTtlInsideReleaseWindow`, since a shorter retention
would let a released bucket's stale copy bring a removed key back.

## Merge resolvers

| Field | Default | Change it when |
|---|---|---|
| `crdt_retire_after` | 24 h | writers churn fast and merged values carry too many retired slots |
| `crdt_sweep_interval` | a quarter of `crdt_retire_after`, at least 30 s | tests need retirement within seconds |
| `crdt_compact_batch` | 4,096 records | one sweep step stalls the runtime too long |

## A cluster across regions

At a round trip of tens of milliseconds, raise `gossip_interval`,
`phi_threshold` and `fetch_timeout` together, then watch
`sundog_live_peers` for false failure detections before and after.

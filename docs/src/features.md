# Feature flags

Every feature is off by default and additive:

```sh
cargo add sundog --features tls,prometheus,spill
```

| Flag | What it adds |
|---|---|
| `tls` | mutual TLS on every data-plane connection, through `rustls` |
| `prometheus` | a Prometheus recorder, served by `ClusterBuilder::prometheus_listen` or rendered through `telemetry::prometheus_handle` |
| `spill` | a local SSD or NVMe tier that holds evicted values on disk |
| `sim` | swaps the network for `turmoil`'s deterministic simulation; for tests only |
| `fuzzing` | exposes the store's reference model to the fuzz targets; changes no behavior |

## tls

Set `ClusterBuilder::tls` with a `TlsConfig`: the node's certificate chain,
its private key and the root CAs to trust, all DER-encoded. Both sides of
every connection verify the other against those roots, and every
certificate carries `sundog-mesh.internal` as a DNS subject alternative
name. [Deployment](deployment.md#mutual-tls) covers what TLS protects.

## prometheus

`ClusterBuilder::prometheus_listen(addr)` installs the recorder and serves
`/metrics`, `/readyz` and `/healthz` on `addr`. A service that already runs
an HTTP server calls `telemetry::prometheus_handle()` instead, serves
`PrometheusHandle::render` from its own route, and calls
`PrometheusHandle::run_upkeep` on an interval. Either way, install the
recorder before opening caches. [Operations](operations.md) lists every
metric.

## spill

`CacheBuilder::spill(SpillConfig::new(dir, capacity_bytes))` gives a cache
a disk tier. Once the cache passes `max_capacity`, eviction writes the
coldest values to a ring of region files instead of discarding them, and a
later read promotes an entry back into RAM. A `Replicated` or `Distributed`
cache needs a tier to take a `max_capacity` at all.

| Setting | Default | What it bounds |
|---|---|---|
| `capacity_bytes` | required | disk used by the tier; at least two regions |
| `region_bytes` | 64 MiB | each region file in the ring |
| `read_concurrency` | 16 | spilled reads in flight at once |
| `flush_queue_bytes` | one region | values queued for the flusher but not yet written |
| `spill_wait_timeout` | 2 s | how long eviction waits for queue room |
| `warm_reopen` | off | whether a clean close checkpoints the tier for a fast restart |

When the flusher falls behind, a batch write's eviction waits up to
`spill_wait_timeout` for queue room; a single write's or a read's takes
room only when it is free. A write's eviction then deletes the victim in a
`Local` or `Invalidation` cache. A `Replicated` or `Distributed` cache
keeps it resident for a later pass instead, since deleting it locally
would only have anti-entropy pull it back. A read's eviction keeps its
victim in every mode, so a read of a spilled key behind a full queue finds
no room, answers from disk and leaves its own entry spilled.

A queued value is charged at least its share of the flush channel's 262,144
slots, the budget divided by 262,144, which is 256 bytes at the default 64
MiB. Many tiny values therefore fill the byte bound before the channel, and
the refusal follows the policy above, never a stalled writer. A budget of
12 MiB or less charges every value its length. A hand-off the channel cannot
take because the tier closed or its flusher stopped counts as no eviction
progress: the victim stays resident and the pass ends.

### Warm reopen

With `SpillConfig::warm_reopen(true)`, a clean `close` or `shutdown` writes
every resident entry to the tier and a snapshot of every live entry's key,
version, expiry and disk location beside the region files. The next open
against the same directory replays the snapshot instead of starting empty,
reading no values into RAM. A replayed bucket answers `fetch` only after a
live co-owner confirms it, or after the warm-up finds no co-owner to ask.

A cache checkpoints once. A second `close` from another clone, or a `close`
after `shutdown`, waits for the first to finish and writes nothing, so it
cannot overwrite the snapshot of a cache that has reopened the directory
since. The checkpoint runs on its own task: a `close` dropped part-way, such
as one cut short by a timeout, leaves it running, and the next `close` waits
for it.

A warm reopen advances the node's clock past the greatest version it
replays, so a write or delete made after the restart stamps after every
replayed entry and wins over it, even when the system clock restarted behind
those stamps. If a replayed stamp leads the system clock by more than
`max_clock_skew`, the node logs a warning once.

The snapshot holds no tombstones and no expired entries. A crash leaves no
snapshot, a successful warm reopen deletes the one it replayed, so a second
open with no clean close between is cold, and a node down longer than
`tombstone_ttl` opens cold regardless, since a peer may already have
collected a tombstone that would outvote a stale replayed entry.
`sundog_spill_reopen_total{outcome, reason}` records which path each open
took.

## sim and fuzzing

Both exist for sundog's own test suites. `sim` replaces the network with
`turmoil` so the net layer runs inside a deterministic simulation. Never
enable it in a deployment.

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

When the flusher falls behind, eviction waits up to `spill_wait_timeout`
for queue room. A `Local` or `Invalidation` cache then evicts the entry
outright. A `Replicated` cache keeps it resident for a later pass instead,
since deleting it locally would only have anti-entropy pull it back.

### Warm reopen

With `SpillConfig::warm_reopen(true)`, a clean `close` or `shutdown` writes
every resident entry to the tier and a snapshot of every live entry's key,
version, expiry and disk location beside the region files. The next open
against the same directory replays the snapshot instead of starting empty,
reading no values into RAM. A replayed bucket answers `fetch` only after a
live co-owner confirms it, or after the warm-up finds no co-owner to ask.

A crash leaves no snapshot, and a node down longer than `tombstone_ttl`
opens cold regardless, since a peer may already have collected a tombstone
that would outvote a stale replayed entry.
`sundog_spill_reopen_total{outcome, reason}` records which path each open
took.

## sim and fuzzing

Both exist for sundog's own test suites. `sim` replaces the network with
`turmoil` so the net layer runs inside a deterministic simulation. Never
enable it in a deployment.

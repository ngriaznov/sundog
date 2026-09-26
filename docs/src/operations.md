# Operations runbook

## Signals

sundog records metrics through the `metrics` crate, and your service's
recorder receives them. With the `prometheus` feature,
`ClusterBuilder::prometheus_listen(addr)` installs a recorder and serves
`GET /metrics`, `GET /readyz` and `GET /healthz` on `addr`, and
`telemetry::prometheus_handle()` installs one for a service that serves
`/metrics` from its own HTTP stack. Install the recorder before opening
caches: a cache binds its metric handles when it opens.

A ready-made Grafana dashboard ships in the repository at
`ops/grafana-dashboard.json`.

`Cluster::health()` returns the same picture in code: readiness, the live
peer count, and each open cache's mode and warmth. `Cluster::peers()` lists
every live peer with its addresses and protocol version.

Logs go through `tracing`. State transfer and anti-entropy rounds run
inside spans, and a node logs its advertised gossip and data-plane
addresses once it forms, under `cluster formed`.

## Metrics

### Every cache

| Metric | Meaning |
|---|---|
| `sundog_cache_hits_total{cache}` | reads answered from this node's copy |
| `sundog_cache_misses_total{cache}` | reads that found no entry on this node, one per loader run for `get_or_load` |
| `sundog_cache_entries{cache}` | live entries on this node |
| `sundog_cache_bytes{cache}` | a cache with a `max_resident_bytes` ceiling: resident bytes of this node's live entries, as `Cache::resident_bytes` counts them |
| `sundog_ceiling_refusals_total{cache, kind}` | a cache with a `max_resident_bytes` ceiling: local writes refused (`write`) and fills returned uncached (`fill`) |
| `sundog_fan_out_backlog{cache}` | written keys not yet sent to peers |
| `sundog_fan_out_wait_timeouts_total{cache}` | writes that waited out `fan_out_wait_timeout` for backlog room and proceeded over capacity |
| `sundog_ae_repaired_total{cache}` | entries anti-entropy repaired |
| `sundog_ae_sketch_total{cache, outcome}` | sketch reconciliations of large buckets, `decoded` or `fallback` |
| `sundog_ae_parts_total{cache, outcome}` | part-level reconciliations, `listing`, `sketch` or `fallback` |
| `sundog_state_transfer_records_total{cache}` | records a joining node pulled from its donor |
| `sundog_clock_skew_rejected_total{cache}` | records refused for a stamp further ahead of this node's clock than `max_clock_skew` |

### The cluster

| Metric | Meaning |
|---|---|
| `sundog_live_peers` | peers this node sees alive |
| `sundog_open_caches` | caches open on this node |
| `sundog_frames_sent_total` | data-plane frames sent |
| `sundog_bytes_sent_total` | data-plane bytes sent |
| `sundog_fan_out_wait_seconds_total{peer}` | whole seconds writers spent waiting for a live peer's full outbox |
| `sundog_backlog_dropped_total{peer}` | frames dropped because the peer left the mesh |

### Distributed caches

| Metric | Meaning |
|---|---|
| `sundog_owned_parts{cache}` | parts this node owns, of 65,536 |
| `sundog_owned_buckets{cache}` | the same share in buckets: owned parts over 64 |
| `sundog_rebalance_parts_total{cache, direction}` | parts pulled `in`, released `out`, or `served` to another node's pull |
| `sundog_rebalance_pull_timeouts_total{cache}` | part pulls that timed out repeatedly and left the rest to anti-entropy |
| `sundog_fetch_total{cache, outcome}` | `fetch` calls by outcome: `local`, `remote`, `miss` or `error` |
| `sundog_forwarded_writes_total{cache}` | writes sent on to a part's owners |
| `sundog_stale_view_total{cache}` | anti-entropy rounds a peer declined over a different ownership view |
| `sundog_unowned_inbound_dropped_total{cache}` | inbound records dropped for a part this node does not own |

### Merge resolvers

| Metric | Meaning |
|---|---|
| `sundog_crdt_retired_writers_total{cache}` | writer slots the compaction sweep found eligible for retirement |
| `sundog_crdt_compactions_total{cache}` | records the sweep rewrote in compacted form |

### The spill tier

| Metric | Meaning |
|---|---|
| `sundog_spill_entries{cache}` | entries whose values live on disk |
| `sundog_spill_bytes_used{cache}` | bytes the region files hold |
| `sundog_spill_writes_total{cache}` | values written to disk |
| `sundog_spill_reads_total{cache, outcome}` | disk reads: `hit`, `stale` or `io_error` |
| `sundog_spill_promotions_total{cache}` | spilled entries promoted back to RAM |
| `sundog_spill_region_reclaims_total{cache}` | regions reclaimed as the ring wrapped |
| `sundog_spill_dropped_total{cache, reason}` | evictions that did not reach disk, by reason |
| `sundog_spill_waiters{cache}` | writers waiting for the flusher |
| `sundog_spill_wait_seconds_total{cache}` | whole seconds writers waited for the flusher |
| `sundog_spill_wait_timeouts_total{cache}` | waits that ran out |
| `sundog_spill_reopen_total{cache, outcome, reason}` | cache opens, `warm` from a checkpoint or `cold_fallback` with a reason |
| `sundog_spill_reopen_records_total{cache}` | records a warm reopen replayed |
| `sundog_spill_checkpoint_entries_total{cache}` | entries written by a closing checkpoint |
| `sundog_spill_reopen_entries_total{cache}` | entries restored by a warm reopen |

## Symptoms

### A node does not join

`sundog_live_peers` stays at 0 and `Cluster::peers()` is empty.

1. Check that every node uses the same cluster name.
2. On Docker, in a VPC or in Kubernetes, mDNS finds nobody. Configure
   seeds or `DnsSrv`, per [Deployment](deployment.md).
3. Check that both ports are fixed and open between nodes: gossip over
   UDP, the data plane over TCP.
4. Check the advertised address in the `cluster formed` log line. A NAT or
   port mapping needs `advertise_ip`.
5. A TLS node and a plaintext node never connect. Give every node the same
   TLS setting.

### Opening a cache fails with `ModeMismatch`

A live peer has the same cache name open under another mode, or with
another `owners` count. Deploy the same cache configuration everywhere, or
give the new configuration a new cache name.

### Readiness stays at 503

A `Replicated` cache has not finished pulling its snapshot. Check
`sundog_state_transfer_records_total` for progress and the logs for the
chosen donor. A cache whose transfer exceeds `state_transfer_budget` opens
cold and keeps pulling in the background, then opens warm after three
timed-out pulls with what arrived; raise the budget for large caches.

### Writes slow down

`sundog_fan_out_backlog` climbs and `sundog_fan_out_wait_seconds_total`
grows for one peer: that peer reads its outbox slower than this node
writes. sundog applies backpressure instead of dropping frames for a live
peer, so writers wait. Find the slow peer by its label and check its CPU
and network. `sundog_fan_out_wait_timeouts_total` counts writes that
waited the whole `fan_out_wait_timeout` and went ahead anyway.

### Repairs climb steadily

A steady rise in `sundog_ae_repaired_total` means replication messages are
not arriving and anti-entropy is doing replication's job. Look for
`sundog_backlog_dropped_total` on the sending side and for peers flapping
in `sundog_live_peers`. Flapping under jitter calls for a higher
`phi_threshold`.

### A `Distributed` cache reports fetch errors

`sundog_fetch_total{outcome="error"}` rises and callers see
`CacheError::FetchUnavailable`: no owner of a key answered within
`fetch_timeout`, or every owner that answered still had the key's part
cold. A short burst right after a join or a leave is the second case. Check whether owners are overloaded or unreachable, and
whether a node's `sundog_owned_parts` dropped to 0 after a restart.
`sundog_stale_view_total` rising briefly during a membership change is
normal; rising steadily means gossip is not converging.

### Records are refused for clock skew

`sundog_clock_skew_rejected_total` rises, and the node logs one warning
naming the writer and how far ahead its stamp was. One node's clock runs
fast, or this node's runs slow: compare each host's time against NTP. A
refused write stays on the node that made it until the other clocks reach
its stamp. A node that logs that its clock is behind its own last write
stamp had its system clock stepped back; its writes keep their order.

### A spill tier drops evictions

`sundog_spill_dropped_total` grows: the flusher falls behind eviction, or
the disk fails writes. The `reason` label says which. Check disk latency,
raise `SpillConfig::flush_queue_bytes`, or raise `max_capacity` to spill
less.

### A restart comes back cold

`sundog_spill_reopen_total{outcome="cold_fallback"}` names why in
`reason`: `disabled` when `warm_reopen` is off, `no_snapshot` after a
crash, `downtime_exceeded` when the node was down longer than
`tombstone_ttl`, and `stale_snapshot`, `config_mismatch` or `bad_region`
when the files on disk do not match the configuration.

## Restarting and upgrading

Call `Cluster::shutdown` on the way out. It gossips that the node is
leaving on purpose, so `Replicated` caches on the other nodes do not hold
tombstones back waiting for it to return, and a spill tier writes its
checkpoint. Roll a cluster one node at a time; [Deployment](deployment.md#rolling-upgrades)
has the order.

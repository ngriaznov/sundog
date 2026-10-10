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
| `sundog_read_duration_seconds{cache, outcome}` | histogram of `get` and `get_sync` durations, `hit` or `miss`, about one read in 256 per thread |
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
| `sundog_fetch_duration_seconds{cache, outcome}` | histogram of every `fetch` that asked an owner, under its outcome: `remote`, `miss`, `error`, or `local` after a view change |
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
| `sundog_spill_read_duration_seconds{cache}` | histogram of the duration of every disk read `sundog_spill_reads_total` counts |
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

### Explaining one read

`Cache::explain(&key)` says why a read of one key answers what it answers
on this node. It is not a read: it counts no hit, miss, fetch or loader
metric, opens no span, touches no idle timer on this node, starts no
refresh or load, and promotes no spilled entry.
Its `Display` form is a few lines to paste into an incident note.

Every explanation names the cache, this node, the mode, the key's part and
what this node stores for the key (`local`): `Absent`, a `Tombstone`, a
`Live` entry (`spilled` when its value is on disk), or a `Lapsed` entry a
read no longer returns because it `Expired` or went `Idle`, not yet swept.
`source` predicts where a `fetch` takes its answer. In a `Distributed`
cache, `distributed` adds:

- `owners`: the key's owners in the order `fetch` asks them, as
  `owners_of` returns them, under the view `view_hash` names.
  `view_moved_to` is set when this node's view changed while the owners
  were asked.
- `residency`: this node's marks for the part. `cold_marked`, and
  `unsettled` on an owned part (outside the last settled view and not
  pulled since), make a local miss say nothing; every part this node does
  not own reads `unsettled` once a view has settled. `unverified` (replayed from disk at a warm
  reopen) and `stale` (regained with a copy from owning it before) make a
  local hit untrusted. `releasing_for` is how long this node has kept a
  part it no longer owns.
- `local_read`: what this node's copy makes of a fetch. `Hit` and `Miss`
  answer here; `NotOwner`, `Distrusted` and `ColdMiss` ask the owners.
- `serves_peers`: what this node answers another node's fetch.
- `probes`: each other owner's answer to the fetch a read sends:

| Answer | Meaning |
|---|---|
| `Held` | The owner sends its record. `reads` says what a fetch makes of it: `Value`, or a miss for `Deleted`, `Expired` or `Undecodable`. A held record is sent whatever the views say, so it does not show the owner's part is warm. |
| `Miss` | The owner holds nothing, on an equal view, in a trusted warm part: a definitive miss. |
| `StaleView` | The owner holds nothing in a part it trusts and its view differs. A fetch retries it; the explanation counts it as no answer. |
| `Declined` | The owner cannot vouch for the key: the cache is not open there, its copy of the part is unverified or stale whatever the views say, or the part is cold with no record on an equal view. |
| `Unreached` | No answer: `NotAMember` of the mesh, `ProtocolTooOld`, `TimedOut` within `fetch_timeout`, an `Io` error, or a `Codec` error. A crashed owner reads as a refused connection, an `Io` error, on Linux and macOS, and as `TimedOut` on Windows, which retries a refused connect for about two seconds. |

`source` is `Local` when `local_read` answers, else the first owner whose
probe is `Held` or `Miss`, else `Unavailable`, which a fetch reports as
`CacheError::FetchUnavailable`.

The owners are asked at once, each bounded by `fetch_timeout` and never
retried, so the call returns within about one `fetch_timeout`. Each probe
is an ordinary fetch request: it may dial a connection, it counts in
`sundog_frames_sent_total` and `sundog_bytes_sent_total` on both nodes,
and an owner holding the key spilled reads it from disk to answer and
counts that read in `sundog_spill_reads_total`. `explain` is for an
operator's question, not a request path.

What it cannot say: why an owner declined, which pull or anti-entropy
round last touched the part, or whether a `Held` owner's copy is current.
The marks and records are read one after another, not atomically, so a
membership change during the call can show a state from either side of
it.

`sundog-lens` names the same placement for any key without asking a node.
Its `e` key and `watch --once --explain KEY` show the key's `part` as
`bucket/part`, the ownership `view` and the `owners` in the order `fetch`
asks them, computed from gossip with the code the nodes run (see
[Explaining a key](#explaining-a-key)). The lens does not call `explain`; a
node does. A node's `local` record, `residency`, `local_read`,
`serves_peers` and `probes` come from `Cache::explain` on that node, and
the lens shows them for the demo's test nodes only, which answer over a
control port. A node whose `view` differs from the lens's has not
converged to the cluster's view, or the view moved after the node
answered.

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

## Watching a cluster: sundog-lens

`sundog-lens` is a terminal UI that watches a running cluster from outside.
It is a workspace crate and not published to crates.io; build it with
`cargo build --release -p sundog-lens`.

### What the observer is

The lens runs a `sundog::observe::Observer`. The observer joins the
cluster's gossip under the cluster name and reads every member's state, but
it opens no cache, runs no data plane and carries no node id or data
address. No member counts it as a peer, so it is never dialed, sent writes,
asked for state or made an owner, and starting or stopping one never moves a
part. Members keep its dead chitchat entry for `dead_node_grace_period`
(600 s by default) after it stops, then drop it.

From gossip alone the lens knows each member's status (`Live`, `Departing`,
`Left` or `Down`, which tells a graceful leave from a crash), its protocol,
the caches it advertises with their modes, and, for every `Distributed`
cache, which node owns which part. It computes the ownership with the code
the nodes run. With the `prometheus` feature on the nodes it also scrapes
each node's exporter for rates, hit ratios and the node's own count of owned
parts.

### Running it

```sh
sundog-lens watch mycluster --seed 10.0.0.5:7946 \
    --metrics 'http://{ip}:9090/metrics'
```

`--seed` is any member's gossip address and repeats. Without it the lens
reads `SUNDOG_SEEDS`, then falls back to mDNS. `--metrics` is a URL template
with `{ip}`, `{gossip_port}`, `{data_port}` and `{node_id}`;
`{gossip_port+N}` and `{gossip_port-N}` shift the gossip port. `--scrape
NODE=URL` pins one node's URL. A node with no exporter shows membership,
modes, status and computed ownership only. `1`-`4` switch between Overview,
Caches, Node and Timeline, `e` explains a key, `?` lists every key, and `q`
quits.

The lens speaks plain UDP for gossip even when the nodes run with the `tls`
feature, so it needs no certificates, and exporters are plain HTTP, so
`--metrics` takes an `http://` URL. Run it where the nodes' gossip and
exporter ports are reachable.

`--once` prints one report and exits, after the member set holds still for
`--settle` (3 s by default), and `--once --json` prints the same report as
JSON for scripts:

```sh
sundog-lens watch mycluster --seed 10.0.0.5:7946 --once --json
```

`--once --explain KEY` adds the placement of one key to the report; see
[Explaining a key](#explaining-a-key).

### Reading the screen

Each member carries a status glyph.

| Glyph | Meaning |
|---|---|
| `●` | live |
| `◒` | live and warming: its exporter reports it not ready |
| `◐` | departing: it announced a graceful leave and owns no part; the glyph blinks in color, never in shape |
| `○` | left after a departure |
| `✖` | down: gossip dropped it with no departure (crash, stall or partition) |
| `✚` | joined |
| `↻` | rejoined at the same address with a new identity |

A `✓` after a node's share means the parts it reports in
`sundog_owned_parts` equal the parts the lens computes for it; `↻` means they
differ and the node is still settling. A cache is `✔ settled` once every node
that reports metrics reports the parts the lens computes for it and has pulled
no part in its last two scrapes; without exporters it settles after the view
has held for three seconds.
The PEERS column compares `sundog_live_peers` on the node with the lens's
own count of live members and turns amber when the two disagree for more
than three seconds. A view change appears as `VIEW` in the event log with
the number of parts that moved, and the ownership mosaic flashes.

The Timeline view draws one lifeline per node. A crash ends in `✖`, and a
graceful leave runs `◐` to `○`, so the two read differently at a glance.

### Explaining a key

`e` opens "Explain a read" over any view, in every mode. It says where one
key lives in the vocabulary of `Cache::explain` and, in the demo, what each
test node says of reading it. The prompt is always focused: every printable
key types into the key, including `q`, `?` and `c`, Backspace deletes,
Ctrl-U clears, Up and Down move the lens's node selection, Esc closes the
overlay and Ctrl-C quits. The text stays between openings, and the demo's
first opening shows `k1`, a key its fleet holds.

A cache routes a key by the hash of its postcard encoding and gossip
carries no key type, so the text picks the encoding:

| Text | Key |
|---|---|
| `k17` | A `String`: a varint length, then UTF-8. |
| `uint:N` | A `u16`, `u32` or `u64`: one LEB128 varint. |
| `int:N` | An `i16`, `i32` or `i64`: one zigzag varint. |
| `hex:036b` | The postcard bytes `03 6b` verbatim, for any other key type: a `u8`, a tuple, a newtype, an enum. |
| `str:uint:1` | The `String` `uint:1`. `str:` escapes a prefix. |

A key holds at most 256 characters. A key typed as the wrong type reads as
a confident answer for another part, so the overlay echoes the kind and the
bytes it hashed; `hex:` states the bytes you mean.

The top of the overlay is **computed** from gossip and works against any
cluster. It shows the part as `bucket/part`, the ownership view with its
hash, owners per part, whether it ranks parts or buckets, and `✔ settled` or
`↻ settling`, and the part's owners in fetch order with their slot, short
id and status. The first owner, marked `◆`, is the one a fetch asks first.
The key is placed in the `Distributed` cache the Ownership panel shows, and
`c` picks it before `e` opens the overlay. `provisional: still discovering`
marks the first seconds of a run, until the live members have held still for
2 s. A bad key shows its error in place of the block, and a cluster with no
`Distributed` cache says that `explain` needs part ownership. No node is
asked for any of it.

In the demo, `Enter` adds the **asked** block. The lens sends `explain KEY`
to the control port of every live test node at once, the channel the fleet
uses for `fill` and `crash`, and each node answers one line of JSON, the
report of `Cache::explain` on its cache `it`. The lens waits 3 s for a node.
A line above the table says whether the nodes agree with each other and
with the lens (`✔ 5 nodes agree, the lens computes the same`), which nodes
hold which view, which differ from the lens's placement, or that the view
moved since they were asked, which dims the rows until `Enter` asks again.
The table has one row per node, in slot order:

| Column | Shows |
|---|---|
| `NODE` | The node's slot label. |
| `OWNS` | Whether the node owns the key's part. |
| `LOCAL` | What the node stores for the key: `absent`, `live` with its expiry, `spilled`, `tombstone` or `lapsed`. |
| `READ` | What the node's own copy makes of a fetch. |
| `SERVES` | What the node answers another node's fetch. |
| `SOURCE` | Where a fetch on the node takes its answer: `this node hit`, an owner and `hit` or `miss`, or `unavailable`. |
| `PROBES` | Each other owner's answer to the fetch a read sends. |

Up and Down select a node, and a detail block under the table shows its
record with the version, its residency marks, its local read, where a fetch
on it takes its answer, and every probe with the version it returned and
what a read makes of it. As the popup narrows, `PROBES` drops first, then
`SERVES` and `READ`. As it shortens, the detail block goes first, then rows
give way to a `… +N nodes` line. A node that does not answer keeps its row
with the reason in words: `no answer (connection refused)`, `no answer
within 3 s`, or, for a `sundog-testnode` built before `explain`, a note to
rebuild it. Every verdict is a word as well as a color, so mono reads the
same.

A test node holds `String` keys and reads the line as one word, so the
lens asks only about a `String` key of visible ASCII characters with no
space. A key of another type is computed and never asked: the line
`explain 5` explains the `String` `5`, a different key from the integer. A
lens with no control ports, which is every `watch`, asks nobody and says
so. A result shows while the text names the key it answered; edit the text
and the block offers to ask again.

Each asked node probes the key's other owners with the fetch a read sends,
so one `Enter` shows in the frame and byte counters on both ends of every
probe, and an owner that holds the key spilled reads it from disk and counts
that in `sundog_spill_reads_total`. No hit, miss or fetch counter moves.

`--once --explain KEY` prints the computed block without a terminal and
asks no node:

```sh
sundog-lens watch mycluster --seed 10.0.0.5:7946 --once --explain k17 --cache it
```

```text
explain it · key "k17" as String (4 bytes 03 6b 31 37) · computed
part   305/17 · ranked per part
view   5d69e3dbbfa02966 · settled (gossip only) · 5 eligible · 2 owners per part
owner  1  n2  204f5d2e9b010000  127.0.0.12:7946  data 127.0.0.12:39211
owner  2  n5  294f5d2e9b010000  127.0.0.15:7946  data 127.0.0.15:39211
note   no node was asked; a node's record, residency marks and probes come from Cache::explain on that node
```

The block follows the caches table. `--cache NAME` names the `Distributed`
cache and is needed when several are ranked. The run waits up to 8 s after
the members settle for the key's view to settle, and `settled` says whether
it did. With `--json`, the report gains an `explain` object, and a run
without `--explain` has none:

```json
"explain": {
  "cache": "it",
  "key": { "kind": "str", "text": "k17", "hex": "036b3137" },
  "part": { "bucket": 305, "part": 17 },
  "view": "5d69e3dbbfa02966",
  "owners_per_part": 2,
  "ranks_parts": true,
  "eligible": 5,
  "settled": true,
  "gossip_only": true,
  "conflicted": false,
  "owners": [
    { "rank": 1, "slot": "n2", "node": "204f5d2e9b010000", "status": "live",
      "gossip": "127.0.0.12:7946", "data": "127.0.0.12:39211" },
    { "rank": 2, "slot": "n5", "node": "294f5d2e9b010000", "status": "live",
      "gossip": "127.0.0.15:7946", "data": "127.0.0.15:39211" }
  ]
}
```

`key.kind` is `str`, `uint`, `int` or `hex`, and `key.hex` holds the
postcard bytes, which `hex:` takes back. `view` is the 16 hex digits of the
view hash, as `Cache::explain` reports `view_hash`. `owners` runs in fetch
order. `status` is `live`, `departing`, `left` or `down`, and an owner the
snapshot no longer lists has slot `??` and `null` for `status`, `gossip` and
`data`. `settled` is the lens's verdict for the view and `gossip_only` says
it rests on gossip alone. `conflicted` is true when the members that
advertise the cache disagree on its mode.

A bad key, `--explain` without `--once` and `--cache` without `--explain`
exit 2 with a message naming the flag. A key the report cannot place
prints nothing on stdout and one line on stderr that names the remedy, and
exits 1: no `Distributed` cache is advertised, `--cache` names a cache no
node advertises or one that is not `Distributed`, the lens has not ranked
the cache yet, or several caches are ranked and `--cache` is missing.

### What it cannot show

| Wanted | Status |
|---|---|
| Which node owns which part | Shown, computed with the code the nodes run. |
| Pulls and repairs in flight per part; cold, unverified or releasing parts | Not shown. Only rates from the rebalance, anti-entropy and state-transfer counters. |
| The slowest or most frequent parts | Not shown. No per-part metric or latency histogram exists. |
| Keys, digests, content convergence | Not shown. Entry-count divergence across `Replicated` nodes and the entry total divided by `k` for `Distributed` stand in. |
| One key's placement and a node's own read | Placement for any key you type: `e` and `--once --explain`, computed from gossip. A node's record, residency marks and probes are shown for test nodes only, in the demo. On a production node they come from `Cache::explain`. |
| Per-cache warmth on a node | Not shown. Only the node's `/readyz` bit. |
| A node's own membership view | The PEERS column compares its live peer count with the lens's. |
| A node without an exporter | Membership, modes, status and computed ownership only. |
| Partition versus crash | `Down` is the lens's own vantage point; the text says "crash, stall or partition". |
| CPU and RSS | sundog does not export them. |

### The demo

```sh
sundog-lens demo --scenario tour
```

The demo starts a fleet of `sundog-testnode` processes under load, plays a
scripted tour of joins, a crash, a graceful leave and a restart, and shows
the lens while it runs. It needs a built `sundog-testnode` (`cargo build
--release -p sundog-testnode --features prometheus`); `--testnode PATH` names
it. `--headless` prints every step and event instead of drawing and exits 1 when
a step times out, which makes the tour a smoke test. `sundog-lens cluster`
starts the same fleet with no scenario, and `S`, `K`, `L` and `R` in the demo
spawn a node, kill one, make one leave and restart one.
`sundog-lens/demo/record.sh` and `render.sh` produce the recording and GIF in
`assets/`.

`e` in the demo asks every test node over its control port, the channel the
fleet uses for `fill` and `crash`. The test node accepts `explain KEY` and
answers one line of JSON, the report of `Cache::explain` on its cache `it`;
see [Explaining a key](#explaining-a-key). The lens opens no connection to
a data port. A `sundog-testnode` built before `explain` answers `unknown
command`, which the overlay shows as a row that says to rebuild it. The
`watch` command that `cluster` prints knows no control ports, so `e` there
computes the placement and asks nobody.

The fleet addresses its nodes in one of two layouts. In the per-address
layout node `n1` binds `127.0.0.11`, `n2` binds `127.0.0.12` and so on, each
on the fixed ports: gossip 7946, control 8080 and exporter 9090. Linux
answers on every address of `127.0.0.0/8`, so the layout needs no setup
there and is the default. In the shared layout every node binds `127.0.0.1`
and node *i* takes the fixed ports plus *i* − 1: `n1` gossips on 7946,
`n2` on 7947, and `n2`'s exporter listens on 9091. The fleet passes each
node its ports in `SUNDOG_TESTNODE_GOSSIP_PORT`,
`SUNDOG_TESTNODE_CONTROL_PORT` and `SUNDOG_TESTNODE_METRICS_PORT`. The shared
layout is the default on macOS and every other system, and needs no
`ifconfig` alias and no `sudo`. `--base-ip IP` always picks the per-address
layout, with `n1` at `IP`; on macOS each node's address except `127.0.0.1`
then needs a `sudo ifconfig lo0 alias`. Before it starts a node, the fleet
checks that the ports of its layout are free and names the first one that is
not.

The exporter port lies 1144 above the gossip port in both layouts, so the
demo scrapes through one template, `http://{ip}:{gossip_port+1144}/metrics`,
and `cluster` prints the `watch` command that goes with its layout. The
control port lies 134 above the gossip port, so `e` reaches every node
through `{ip}:{gossip_port+134}`. The event log shows a node's gossip address
with its port, so nodes that share an address stay apart.

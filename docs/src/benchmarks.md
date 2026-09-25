# Benchmarks

`sundog-bench`, the `bench` crate in the repository, runs one workload
against sundog and the systems it is compared with: Redis, Valkey,
Dragonfly, Olric and Hazelcast. Each target gets the same keys, values,
operation mix, worker count and random seed. The report gives read and
write latency at p50 and p99, throughput, and memory per entry.

## The workload

- **Keys**: 100,000 fixed-width keys, all loaded before measuring.
- **Values**: 100 bytes, different for every key, so no server can share
  one value across keys.
- **Mix**: 90% reads and 10% writes.
- **Key choice**: a zipf distribution with exponent 0.99, so a few keys
  take most of the traffic, as in a real cache.
- **Workers**: 16, each with its own client.
- **Operations**: 20,000 unmeasured warm-up operations, then 200,000
  measured ones.

Every value is a flag on the command line, listed by `sundog-bench --help`.

## The targets

| Target | What runs | How a worker reaches it |
|---|---|---|
| `sundog-local` | one node, `Mode::Local` | in-process |
| `sundog-replicated` | three nodes, `Mode::Replicated` | in-process; reads never leave the process |
| `sundog-distributed` | three nodes, `Mode::distributed()` with two owners | in-process; `fetch` of a key this node does not own is one loopback round trip |
| `redis` | `redis:8` | loopback TCP, RESP `GET` and `SET` |
| `valkey` | `valkey/valkey:8` | loopback TCP, RESP `GET` and `SET` |
| `dragonfly` | the Dragonfly image | loopback TCP, RESP `GET` and `SET` |
| `olric` | `olricio/olricd` | loopback TCP, RESP `DM.GET` and `DM.PUT` |
| `hazelcast` | `hazelcast/hazelcast:5.5` | loopback TCP, the memcache text protocol |

sundog runs inside the benchmark process, the way a service embeds it,
and every worker uses node 0's cache handle. The three sundog nodes of a
cluster run in the same process and talk over loopback. A
`sundog-distributed` read goes through `fetch`, which serves a key this
node owns locally and asks an owner for the rest.

Each server runs in its own container, started through
[`rightsize`](https://crates.io/crates/rightsize), and the benchmark reaches
it over the container's published port. The comparison is an embedded
cache against a networked one: a server read costs a round trip that an
embedded read does not. The report says how each target is reached, next to
its figures.

`SUNDOG_BENCH_<SERVER>_IMAGE` replaces a server's image, for example
`SUNDOG_BENCH_REDIS_IMAGE=redis:7.4`.

## Memory per entry

Memory is read before the load and again after it, and the difference is
divided by the key count times the copies held:

- **sundog**: bytes in use and not yet freed: jemalloc's allocated
  count less the freed memory its thread caches keep for reuse. It is the
  same kind of figure Redis reports as `used_memory`.
  A `sundog-replicated` cluster holds three copies of each entry and a
  `sundog-distributed` one holds two, so both report bytes per copy.
- **Redis, Valkey and Dragonfly**: `used_memory` from `INFO memory`.
- **Olric and Hazelcast**: not reported. Hazelcast's memcache protocol has
  no memory figure, and Olric's Go heap still holds garbage its collector
  has not reclaimed.

Every target runs in a fresh child process, so memory one target freed
never makes the next one look smaller.

## The gate

`ops/bench-gate.json` sets limits the run must meet: a read and write p99
under one millisecond for `sundog-local` and `sundog-replicated`, and no
errors from any sundog mode. A target the gate names that fails or is
missing from the report also fails the gate. The Scale workflow keeps its
own 100 ms fetch p99 limit, for a 4-million-key cluster whose fetches cross
the network and the SSD.

## Running it

```sh
RIGHTSIZE_BACKEND=docker cargo run --release -p sundog-bench -- \
  --report-md bench-report.md --gate ops/bench-gate.json
```

`--targets` picks a subset. sundog alone needs no container backend:

```sh
cargo run --release -p sundog-bench -- \
  --targets sundog-local,sundog-replicated,sundog-distributed
```

`.github/workflows/weekly-bench.yml` runs the full set every week and posts
the report on the run's summary page. A manual dispatch takes a different
target list, key count, operation count or runner.

Latency depends on the machine, and a shared CI runner is noisy. Compare
targets from the same run, not across runs.

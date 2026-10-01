# Fixtures

`metrics.prom` is the body of `GET /metrics` from a running `sundog-testnode`
built with `--features prometheus`: node n1 of a three-node cluster on
`127.0.0.11` to `127.0.0.13` with `it` Distributed (2 owners) and the Replicated
side caches `churn`, `pn` and `os`, after a bulk fill and a few hundred reads,
fetches and writes. The file is the exporter's output byte for byte.

To retake it, start three nodes the way the demo fleet does (the environment of
`sundog-lens cluster`: `SUNDOG_TESTNODE_BIND_IP=127.0.0.1N`,
`SUNDOG_SEEDS=127.0.0.11:7946,127.0.0.12:7946`, `SUNDOG_TESTNODE_MODE=distributed`,
`SUNDOG_TESTNODE_OWNERS=2`, `SUNDOG_TESTNODE_SIDE_CACHES=on`), drive some load
over the control port, and run `curl 127.0.0.11:9090/metrics`. The names test in
`src/source/names.rs` fails when the capture lacks a metric the lens reads.

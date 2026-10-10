# Fixtures

`metrics.prom` is the body of `GET /metrics` from a running `sundog-testnode`
built with `--features prometheus`: node n1 of a three-node cluster on
`127.0.0.11` to `127.0.0.13` with `it` Distributed (2 owners) and the Replicated
side caches `churn`, `pn` and `os`, after a bulk fill and a few hundred reads,
fetches and writes. The file is the exporter's output byte for byte.

To retake it, start three nodes, one per `i` in `1..=3`:

```sh
SUNDOG_TESTNODE_BIND_IP=127.0.0.1$i SUNDOG_SEEDS=127.0.0.11:7946,127.0.0.12:7946 \
SUNDOG_TESTNODE_MODE=distributed SUNDOG_TESTNODE_OWNERS=2 \
SUNDOG_TESTNODE_SIDE_CACHES=on \
  cargo run -p sundog-testnode --features prometheus -- <cluster>
```

Drive some load over the control port 8080, then run
`curl 127.0.0.11:9090/metrics`. The names test in `src/source/names.rs` fails
when the capture lacks a metric the lens reads.

`explain/owner.json`, `explain/non_owner.json` and `explain/crashed_owner.json`
are replies to the `explain <key>` control line of `sundog-testnode`, one line
of JSON each with a trailing newline. They are the output of the test node's
encoder, `to_line` in `sundog-testnode/src/explain_reply.rs`, for the reply
values its `the_encoder_writes_the_fixture_replies_byte_for_byte` test builds:
node `6f3ac1e29d54b807`, a first owner that holds the key `k17`; node
`1b88e5d0a7c64f92`, a non-owner that reads it from the first owner; and the
same node asking the owners of `k42` after the first owner crashed. The test
fails when the encoder's bytes differ from a file. To retake them, edit the
literals in that test and the files together, then run
`cargo test -p sundog-testnode explain_reply`.

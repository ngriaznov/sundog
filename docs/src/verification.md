# Verification

A cache that silently diverges is worse than no cache. sundog's test
suites exist to catch divergence before a release does. The first five
layers below run in CI, on every change or weekly. The release workflow refuses to publish a
commit unless CI, the weekly simulation, fuzzing, chaos and model-checking
runs have each passed on that exact commit.

## Six layers

1. **Property tests** with `proptest` in the clock, wire, store and
   rebalance modules. The central one applies a random batch of writes and
   removals in every sampled order, with drops and duplicates, and requires
   every order to reach the same final state. Loss-tolerant replication
   rests on that property. Another drives the `Distributed` rebalance
   planner through random view sequences, with views coalesced and pulls
   superseded, against a model of which parts hold every write: a part
   missing writes must read as cold, and one completed pull must fill every
   owned part.
2. **Deterministic simulation** with `turmoil`, behind the `sim` feature.
   It drives the real network layer and store with no real sockets, and
   replays partitions under load, message loss, reordering and duplication,
   a donor dying mid-transfer, flapping links, one-way partitions, high
   latency, and a `Distributed` cluster churning membership. Each scenario
   checks convergence within a bounded number of rounds. `Distributed`
   scenarios here script ownership views by hand and move data by
   anti-entropy; the rebalance and pull path runs in the churn oracle below.
   A weekly run sweeps five fresh seeds.
   The **churn oracle** (`tests/churn_oracle.rs`) runs real clusters in one
   process through a join while an owner crashes mid-pull, a join and leave
   inside the disown grace, a graceful leave, and a join followed by a
   crash. A reader fetches every key from every live node throughout, and
   any miss for a key a live owner holds and any wrong value fails the
   run, as does a deleted value while every live view agrees on the key's
   owners; one while the views disagree is counted as allowed staleness.
   Between steps every owner must hold its copies. CI
   runs one seed, the weekly run thirty fresh ones.
3. **Container integration** through [`rightsize`](https://crates.io/crates/rightsize).
   Separate processes run on a real virtual network: three-node
   convergence, tombstones reaching every node, cold joins of up to a
   million entries, repairs after a killed node and after dropped keys at
   sketch and part-digest scale with their wire cost pinned, high-churn
   workloads draining to zero, 64 KiB values checked byte for byte, and a
   five-node `Distributed` cluster losing an owner, including while a new
   node is still pulling its share. A mixed-version test runs the previous
   release's node against the current one in both directions. A chaos run
   crashes, restarts, churns and drops keys on a seeded schedule for ten
   minutes every week and requires every node to converge; the same run
   repeats the owner-crash scenarios fifteen times at random crash timings.
   Every node's output lands in `target/container-logs/`, and CI keeps it
   when a container job fails.
4. **Coverage-guided fuzzing** with `cargo-fuzz`, weekly. Two targets throw
   arbitrary bytes at the wire decoder, which must never panic and must
   re-encode every frame it accepts to a fixed point. Two more drive a real
   shard against a reference model through generated sequences of writes,
   remote applies, invalidations, tombstone collection and clock jumps.
5. **Bounded model checking** with [Kani](https://github.com/model-checking/kani),
   weekly. Proof harnesses exhaust every input of the arithmetic the rest
   rests on: expiry packing, hash-to-bucket indexing, the hybrid logical
   clock, frame length arithmetic, retry bounds and spill sizing. Each proof
   covers every input, where a property test samples.
6. **The chaos demo**, `sundog-demo --headless`, is a soak rig run by
   hand. It runs nodes in one process for a fixed time, killing and
   restarting them under write load, and exits nonzero on any divergence.
   Over a 24-hour run its memory stays flat.

## Scale

A weekly workflow runs the distributed demo at 4 million keys across three
nodes over a spill tier, kills and restarts a node midway, and checks the
result against thresholds in `ops/scale-gate.json`: steady and peak
memory, dropped spill writes, pull timeouts, dropped replication backlog,
fetch latency, warm reopens, convergence and a sampled value check.

## Warnings are errors

Every CI lane builds with `RUSTFLAGS="-D warnings"` and
`RUSTDOCFLAGS="-D warnings"`, runs `clippy` in pedantic mode across five
feature combinations, and checks formatting. The fuzz and Kani workflows
deny warnings the same way. The code in this book compiles in that same
lane and each recipe runs as a test against a live node, so an example
that stops working fails CI.

## Running it yourself

```sh
cargo test --workspace
cargo test -p sundog --features sim --test 'sim*'
SUNDOG_CONTAINER_TESTS=1 RIGHTSIZE_BACKEND=docker \
    cargo test --release -p sundog --test containers -- --test-threads=1
```

The container suite needs Docker. The README lists the bench suites and the
flags that pin a chaos seed for replay.

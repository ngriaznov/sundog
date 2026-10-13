//! Container-backed multi-node scenarios for everything that needs real,
//! separate processes on a real network: membership convergence,
//! replication, tombstones, state transfer, and anti-entropy, exclusively
//! through the `rightsize` crate. See `tests/container_util` for the
//! harness and `sundog-testnode` for the control protocol.
//!
//! Gated on `SUNDOG_CONTAINER_TESTS=1`, an `eprintln!` and early return
//! otherwise, so a plain `cargo test --workspace` run still compiles
//! without a container backend or the musl target. Run:
//!
//! ```text
//! SUNDOG_CONTAINER_TESTS=1 SUNDOG_TEST_BASE_IMAGE=rz-base:local RIGHTSIZE_BACKEND=docker \
//!     cargo test --release -p sundog --test containers -- --test-threads=1
//! ```

#[path = "../container_util/mod.rs"]
mod container_util;

/// Returns from the calling test fn unless `SUNDOG_CONTAINER_TESTS=1` is
/// set.
macro_rules! require_containers {
    () => {
        if !$crate::container_util::container_tests_enabled() {
            eprintln!("skipping: SUNDOG_CONTAINER_TESTS=1 not set");
            return;
        }
    };
}

mod anti_entropy;
mod chaos;
mod distributed;
mod explain;
mod invalidation_fold;
mod loads;
mod previous_release;
mod replication;
mod scale;
mod spill;
mod support;

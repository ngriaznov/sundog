# House rules — sundog implementation

Binding for all code in this repo. Read alongside `docs/BUILD_PLAN.md`.

## Toolchain & quality gates

- Rust edition 2024, `rust-version = "1.97"`, resolver 3. Local toolchain: 1.98 stable.
- Must pass, in order: `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -W clippy::pedantic`, `cargo test --workspace`.
- No `unsafe`. No `unwrap()`/`expect()` in library paths except with an `expect("invariant: …")` stating why it cannot fail. Tests may unwrap freely.

## Style

- **Minimal comments.** Comment only invariants and constraints the code cannot express (e.g. "moka iteration is weakly consistent — safe because apply is idempotent"). No narration, no section banners, no "what the next line does".
- Expression-oriented: `let x = if/match/loop … `; `let … else` for early exits; let chains where they read well; `matches!` for boolean checks.
- Errors: `thiserror` enums per domain with `#[from]`; `?` everywhere.
- Builders: own-and-return, `#[must_use]`. Handles: cheap `Clone + Send + Sync`.
- Doc comments (`///`) on all public items; `# Errors` / `# Panics` where applicable. Doc comments are documentation, not comments — do not minimize those.
- `tracing` spans/events at membership changes, state transfer, anti-entropy rounds, drops.

## Design decisions fixed for this build (deviations from the plan noted)

- **HLC is hand-rolled**, not `uhlc`: `Hlc { wall_ms: u64, logical: u32, node: NodeId }`, derived lexicographic `Ord`, plus an `HlcClock` implementing the standard HLC send/receive update rules. Rationale: exact plan semantics, deterministic postcard encoding, trivially property-testable. (Plan §4 lists uhlc; this is the recorded deviation.)
- **NodeId** is a compact `u64` generated per process incarnation (random), displayed as hex; the chitchat node id string is `{hostname}-{node_id_hex}`.
- **All-in v1 scope**: all four testing layers from plan §11 are in scope, including **turmoil deterministic simulation** (the net layer gains a transport seam so turmoil can host the data plane; sim tests cover partition/heal convergence, loss/reorder/dup storms, donor crash mid-state-transfer).
- **Demo bin** is the full chaos TUI from plan §11.4 (ratatui): N in-process nodes, cluster view, injectable faults (kill node, pause node, partition via a toggleable transport filter).
- `metrics` counters as named in the plan (`sundog_backlog_dropped_total{peer}` etc.); **Prometheus exporter implemented** behind a `prometheus` feature flag (metrics-exporter-prometheus), off by default.
- **Future plans pulled into v1** (from plan §14): a pluggable `ConflictResolver` trait (default LWW, resolver consulted on concurrent-version conflicts); TLS on the data plane behind a `tls` feature (rustls, pre-shared cert config). The rest of §14 (distribution mode, QUIC, IBLT set reconciliation, remote thin clients) ships as `ROADMAP.md` with design sketches, not code.
- Dual license MIT OR Apache-2.0.

## Rules for build agents

- Own your assigned files only. Never edit `Cargo.toml`, `src/lib.rs`, or another module's files — if you need a dependency or a cross-module interface change, note it in your returned report instead.
- Before using a third-party crate API (chitchat, mdns-sd, moka, hickory-resolver, postcard), read its real source/docs in `~/.cargo/registry/src/` — do not code against a remembered API.
- Every module ships with its own unit tests in-file (`#[cfg(test)]`).
- Leave the tree compiling: `cargo check --workspace` green before you finish (todo-stubs elsewhere are fine; your own code must be complete).

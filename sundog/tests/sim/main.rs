//! Deterministic simulation suite: drives `net::Mesh` and
//! `store::Shard`/`ShardOps` directly inside a `turmoil` simulation, against
//! a hand-scripted membership feed built from `Peer` values, with no real
//! UDP or TCP. `cluster.rs`'s composition is `pub(crate)` and unusable from
//! here, so this file reimplements the relevant slice against the same
//! public `Mesh`/`ShardOps` surface `cluster.rs` itself drives.
//!
//! `turmoil`'s simulated TCP objects must be created and driven from within
//! the owning host's own future; a `Mesh` cannot be shared across hosts. A
//! `Shard` has no such constraint, so each scenario builds it up front and
//! shares an `Arc` clone with both the host future and the test itself.

#![cfg(feature = "sim")]

mod ae_round;
mod convergence;
mod crdt_compact;
mod deletes;
mod dist;
mod handler;
mod heal;
mod pair;
mod sketch;
mod state_transfer;
mod support;

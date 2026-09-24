//! The sundog book's code, compiled and tested. Every Rust block under
//! `docs/src` includes a marked region of one of these files, so a snippet
//! that stops compiling fails `cargo clippy --workspace` and a recipe that
//! stops working fails `cargo test --workspace`. Each recipe takes a
//! [`sundog::Cluster`] its caller built; the tests build a one-node cluster
//! on loopback and drive the recipe through its public functions.

pub mod counters;
pub mod deploy;
pub mod http_cache;
pub mod read_through;
pub mod sessions;

#[cfg(test)]
mod test_support;

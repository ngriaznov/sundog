//! `sundog-lens`: a terminal interface that watches a live sundog cluster
//! from outside.
//!
//! The lens joins the cluster's gossip as a `sundog::observe::Observer`,
//! which opens no cache, speaks no data plane and is never a peer. It reads
//! each member's lifecycle, caches and modes from gossip, computes every
//! `Distributed` cache's part ownership with the code the nodes run, and
//! scrapes each node's Prometheus exporter for rates and for the ownership
//! the node reports.
//!
//! The crate is a library so its integration tests can import every layer:
//!
//! - [`cli`] parses the command line.
//! - [`source`] turns the observer, the ownership worker and the scraper into
//!   one stream of [`source::Update`]s.
//! - [`model`] folds the updates into the state the interface draws.
//! - [`ui`] draws the model; [`app`] holds the interface state and handles
//!   keys; [`watch`] runs the terminal loop; [`once`] prints one report.
//! - [`key`] parses a key typed as text into the postcard bytes a cache hashes,
//!   and [`locate`] names the part and the owners of that key from the model.
//! - [`scenario`] parses and plays the scripted demo.
//! - [`fleet`] and [`demo`] start local test nodes (Unix only).

pub mod app;
pub mod cli;
#[cfg(unix)]
pub mod demo;
#[cfg(unix)]
pub mod fleet;
pub mod key;
pub mod locate;
pub mod model;
pub mod once;
pub mod scenario;
pub mod source;
pub mod ui;
pub mod watch;

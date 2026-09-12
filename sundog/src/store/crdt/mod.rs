//! Reference CRDT value types and the [`super::ConflictResolver`]s that merge
//! them through [`super::ConflictResolver::merge`].
//!
//! Every type here is built on `BTreeMap`/`BTreeSet` rather than a
//! hash-based collection, so its postcard encoding is canonical: two
//! logically equal values always encode to identical bytes. That property is
//! what lets a resolver's merge be checked for idempotence and associativity
//! at the byte level.

pub mod or_set;
mod pn_counter;
mod writer;

pub use or_set::{OrSet, OrSetResolver};
pub use pn_counter::{PnCounter, PnCounterResolver};
pub use writer::WriterId;

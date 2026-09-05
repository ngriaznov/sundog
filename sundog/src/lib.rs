//! sundog is an embedded, replicated, zeroconf cache for Rust. It runs inside
//! your process; there is no cache server to deploy.
//!
//! Instances of a service on one network find each other, form a cluster over
//! gossip, and keep named caches coherent by invalidation or full replication.
//! Writes are last-writer-wins on a hybrid logical clock. Anti-entropy heals
//! whatever the network drops. There is no consensus and no operator action on
//! join, leave, crash, or partition.
//!
//! # Example
//!
//! The zeroconf path, also the project's acceptance test:
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use sundog::{Cluster, Mode};
//!
//! # #[derive(Clone, serde::Serialize, serde::Deserialize, Hash, PartialEq, Eq)]
//! # struct UserId(u64);
//! # #[derive(Clone, serde::Serialize, serde::Deserialize)]
//! # struct Profile;
//! # #[derive(Clone, serde::Serialize, serde::Deserialize, Hash, PartialEq, Eq)]
//! # struct Token(String);
//! # #[derive(Clone, serde::Serialize, serde::Deserialize)]
//! # struct Session;
//! # async fn load_profile(_id: &UserId) -> Result<Profile, std::io::Error> { unimplemented!() }
//! # async fn run(id: UserId, token: Token) -> anyhow::Result<()> {
//! let cluster = Cluster::builder("demo")
//!     .build() // mDNS discovery, ephemeral ports, sane defaults
//!     .await?;
//!
//! let users = cluster
//!     .cache::<UserId, Profile>("users")
//!     .mode(Mode::Replicated) // or Mode::Invalidation (default), Mode::Local
//!     .max_capacity(200_000)
//!     .ttl(Duration::from_secs(600))
//!     .open()
//!     .await?; // triggers state transfer if the cache exists cluster-wide
//!
//! users.insert(id.clone(), Profile).await?; // stamp HLC -> local apply -> fan out
//! let profile = users.get_or_load(&id, async |id| load_profile(id).await).await?;
//! users.remove(&id).await?; // tombstone write
//!
//! // A cache is typed at open, so sessions get one of their own.
//! let sessions = cluster
//!     .cache::<Token, Session>("sessions")
//!     .mode(Mode::Replicated)
//!     .open()
//!     .await?;
//! sessions.insert_with_ttl(token, Session, Duration::from_secs(30)).await?; // this entry's own TTL
//!
//! let mut events = users.events();
//! while let Ok(ev) = events.recv().await {
//!     // handle Event::{Created, Updated, Removed}
//!     # let _ = ev;
//!     # break;
//! }
//!
//! cluster.shutdown().await; // graceful leave (chitchat departs politely)
//! # Ok(())
//! # }
//! ```

pub mod cache;
pub mod cluster;
pub mod config;
pub mod discovery;
pub mod error;
pub mod hlc;
pub mod membership;
pub mod net;
pub mod node;
pub mod store;
#[cfg(feature = "prometheus")]
pub mod telemetry;
pub mod wire;

pub use cache::{Cache, CacheBuilder};
pub use cluster::{CacheHealth, Cluster, ClusterBuilder, Health};
pub use config::ClusterConfig;
#[cfg(feature = "tls")]
pub use config::TlsConfig;
pub use discovery::Discovery;
pub use error::{CacheError, CodecError, JoinError};
pub use hlc::{Hlc, HlcClock};
pub use node::{NodeId, NodeName};
pub use store::{ConflictResolver, Event, LwwResolver, Mode, Origin, RecordView, Winner};
#[cfg(feature = "prometheus")]
pub use telemetry::{BuildError, PrometheusHandle, prometheus_handle};

/// `cluster::sketch` and `cluster::anti_entropy` are `pub(crate)`: nothing
/// outside `cluster.rs`'s own composition normally names an IBLT or its
/// diffing directly. `tests/sim.rs` is the one exception, driving `net::Mesh`
/// itself rather than `Cluster`, so it needs these to reconcile a sketch
/// reply the same way `cluster::anti_entropy::run_round_against` does.
/// `#[doc(hidden)]` and gated on `feature = "sim"` so this never appears in
/// the crate's normal public API, matching [`wire::Cell`]'s narrower
/// precedent for the same module.
#[cfg(feature = "sim")]
#[doc(hidden)]
pub use cluster::anti_entropy::{diff_decoded, mismatched_parts};
#[cfg(feature = "sim")]
#[doc(hidden)]
pub use cluster::sketch::{Decoded, Elem, Iblt, Undecodable};

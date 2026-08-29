//! sundog: an embedded, replicated, zeroconf cache for Rust. It runs inside
//! your process — no separate cache server to deploy or operate.
//!
//! Instances of a service on the same network discover each other, form a
//! cluster over gossip membership, and keep named caches coherent across
//! nodes — invalidation or full replication, last-write-wins on a hybrid
//! logical clock, healed by anti-entropy. No consensus, no operator action on
//! join, leave, crash, or partition.
//!
//! # Examples
//!
//! The zeroconf happy path — this exact snippet is the project's acceptance
//! test:
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
//! # async fn load_profile(_id: &UserId) -> Result<Profile, std::io::Error> { unimplemented!() }
//! # async fn run(id: UserId) -> anyhow::Result<()> {
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
pub use cluster::{Cluster, ClusterBuilder};
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

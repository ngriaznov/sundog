//! Every system `sundog-bench` measures, behind one interface.

use crate::servers::{Server, ServerClient, ServerTarget};
use crate::sundog_target::{SundogClient, SundogMode, SundogTarget};

/// A system `sundog-bench` measures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Sundog(SundogMode),
    Server(Server),
}

impl Kind {
    /// Every target in report order.
    pub const ALL: [Kind; 8] = [
        Kind::Sundog(SundogMode::Local),
        Kind::Sundog(SundogMode::Replicated),
        Kind::Sundog(SundogMode::Distributed),
        Kind::Server(Server::Redis),
        Kind::Server(Server::Valkey),
        Kind::Server(Server::Dragonfly),
        Kind::Server(Server::Olric),
        Kind::Server(Server::Hazelcast),
    ];

    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Kind::Sundog(SundogMode::Local) => "sundog-local",
            Kind::Sundog(SundogMode::Replicated) => "sundog-replicated",
            Kind::Sundog(SundogMode::Distributed) => "sundog-distributed",
            Kind::Server(Server::Redis) => "redis",
            Kind::Server(Server::Valkey) => "valkey",
            Kind::Server(Server::Dragonfly) => "dragonfly",
            Kind::Server(Server::Olric) => "olric",
            Kind::Server(Server::Hazelcast) => "hazelcast",
        }
    }

    #[must_use]
    pub fn parse(name: &str) -> Option<Kind> {
        Kind::ALL.into_iter().find(|kind| kind.name() == name)
    }

    /// How a worker reaches this target, for the report.
    #[must_use]
    pub fn transport(self) -> &'static str {
        match self {
            Kind::Sundog(SundogMode::Local | SundogMode::Replicated) => {
                "in-process; reads never leave the process"
            }
            Kind::Sundog(SundogMode::Distributed) => {
                "in-process; a read of a key this node does not own is one loopback round trip"
            }
            Kind::Server(_) => "loopback TCP to the container's published port",
        }
    }
}

pub enum Target {
    Sundog(SundogTarget),
    Server(Box<ServerTarget>),
}

pub enum Client {
    Sundog(SundogClient),
    Server(ServerClient),
}

impl Target {
    /// Starts `kind`.
    ///
    /// # Errors
    ///
    /// Returns an error if the target fails to start.
    pub async fn start(kind: Kind) -> anyhow::Result<Target> {
        Ok(match kind {
            Kind::Sundog(mode) => Target::Sundog(SundogTarget::start(mode).await?),
            Kind::Server(server) => Target::Server(Box::new(ServerTarget::start(server).await?)),
        })
    }

    /// One client for one worker.
    ///
    /// # Errors
    ///
    /// Returns an error if a server connection fails.
    pub async fn client(&self) -> anyhow::Result<Client> {
        Ok(match self {
            Target::Sundog(target) => Client::Sundog(target.client()),
            Target::Server(target) => Client::Server(target.client().await?),
        })
    }

    /// Bytes attributed to this target's data right now, where it reports
    /// any.
    ///
    /// # Errors
    ///
    /// Returns an error if a server fails the memory query.
    pub async fn memory_used(&self) -> anyhow::Result<Option<u64>> {
        match self {
            // sundog's memory is what this process holds from jemalloc.
            Target::Sundog(_) => Ok(crate::sundog_target::allocated_bytes()),
            Target::Server(target) => target.memory_used().await,
        }
    }

    /// How many copies of each entry the memory figure covers.
    #[must_use]
    pub fn copies(&self) -> u64 {
        match self {
            Target::Sundog(target) => target.copies(),
            Target::Server(_) => 1,
        }
    }

    /// Waits until a load of `keys` entries has landed.
    ///
    /// # Errors
    ///
    /// Returns an error if a sundog cluster does not settle.
    pub async fn settle(&self, keys: usize) -> anyhow::Result<()> {
        match self {
            Target::Sundog(target) => target.settle(keys).await,
            Target::Server(_) => Ok(()),
        }
    }

    #[must_use]
    pub fn image(&self) -> Option<String> {
        match self {
            Target::Sundog(_) => None,
            Target::Server(target) => Some(target.image().to_string()),
        }
    }

    pub async fn shutdown(self) {
        match self {
            Target::Sundog(target) => target.shutdown().await,
            Target::Server(target) => (*target).shutdown().await,
        }
    }
}

impl Client {
    /// Reads `key`, returning whether it was a hit.
    ///
    /// # Errors
    ///
    /// Returns an error if the target fails the read.
    pub async fn get(&mut self, key: &String) -> anyhow::Result<bool> {
        match self {
            Client::Sundog(client) => client.get(key).await,
            Client::Server(client) => client.get(key).await,
        }
    }

    /// Writes `key`.
    ///
    /// # Errors
    ///
    /// Returns an error if the target fails the write.
    pub async fn set(&mut self, key: String, value: Vec<u8>) -> anyhow::Result<()> {
        match self {
            Client::Sundog(client) => client.set(key, value).await,
            Client::Server(client) => client.set(&key, &value).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_kind_round_trips_through_its_name() {
        for kind in Kind::ALL {
            assert_eq!(Kind::parse(kind.name()), Some(kind));
            assert!(!kind.transport().is_empty());
        }
        assert_eq!(Kind::parse("memcached"), None);
    }
}

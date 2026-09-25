//! Cache servers as benchmark targets, each started in a container through
//! rightsize and reached over its published port on loopback.
//!
//! Redis, Valkey and Dragonfly speak RESP with `GET`/`SET`. Olric speaks
//! RESP with its own `DM.GET`/`DM.PUT`. Hazelcast speaks the memcache text
//! protocol once it is switched on.

use std::time::{Duration, Instant};

use anyhow::{Context as _, bail};
use rightsize::{Container, ContainerGuard, Wait, WaitStrategy};
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::TcpStream;

/// The map every Olric operation targets.
const OLRIC_DMAP: &str = "bench";

/// A server `sundog-bench` starts and talks to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Server {
    Redis,
    Valkey,
    Dragonfly,
    Olric,
    Hazelcast,
}

impl Server {
    /// The image a run uses, overridable per server through
    /// `SUNDOG_BENCH_<SERVER>_IMAGE`.
    #[must_use]
    pub fn image(self) -> String {
        let (var, default) = match self {
            Self::Redis => ("SUNDOG_BENCH_REDIS_IMAGE", "redis:8"),
            Self::Valkey => ("SUNDOG_BENCH_VALKEY_IMAGE", "valkey/valkey:8"),
            Self::Dragonfly => (
                "SUNDOG_BENCH_DRAGONFLY_IMAGE",
                "docker.dragonflydb.io/dragonflydb/dragonfly:latest",
            ),
            Self::Olric => ("SUNDOG_BENCH_OLRIC_IMAGE", "olricio/olricd:latest"),
            Self::Hazelcast => ("SUNDOG_BENCH_HAZELCAST_IMAGE", "hazelcast/hazelcast:5.5"),
        };
        std::env::var(var).unwrap_or_else(|_| default.to_string())
    }

    fn port(self) -> u16 {
        match self {
            Self::Redis | Self::Valkey | Self::Dragonfly => 6379,
            Self::Olric => 3320,
            Self::Hazelcast => 5701,
        }
    }

    fn wait(self) -> Box<dyn WaitStrategy> {
        match self {
            Self::Redis | Self::Valkey => {
                Wait::for_log_message(".*Ready to accept connections.*", 1)
            }
            Self::Hazelcast => Wait::for_log_message(".*is STARTED.*", 1),
            Self::Dragonfly | Self::Olric => Wait::for_listening_port(),
        }
    }
}

pub struct ServerTarget {
    server: Server,
    image: String,
    port: u16,
    guard: ContainerGuard,
}

pub enum ServerClient {
    Resp {
        server: Server,
        conn: redis::aio::MultiplexedConnection,
    },
    Memcache(BufReader<TcpStream>),
}

impl ServerTarget {
    /// Starts `server`'s container and waits until a client connects.
    ///
    /// # Errors
    ///
    /// Returns an error if the container fails to start or the server does
    /// not answer in 60 seconds.
    pub async fn start(server: Server) -> anyhow::Result<Self> {
        rightsize_modules::register_default_backends();
        let image = server.image();
        let mut container = Container::new(&image).with_exposed_ports(&[server.port()]);
        if server == Server::Hazelcast {
            container = container.with_env("HZ_NETWORK_MEMCACHEPROTOCOL_ENABLED", "true");
        }
        let guard = container
            .waiting_for(server.wait())
            .start()
            .await
            .with_context(|| format!("{image} starts"))?;
        let port = guard
            .get_mapped_port(server.port())
            .context("the server port is published")?;
        let target = Self {
            server,
            image,
            port,
            guard,
        };
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Ok(mut client) = target.client().await
                && client.ping().await.is_ok()
            {
                return Ok(target);
            }
            if Instant::now() > deadline {
                bail!("{} never answered", target.image);
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    #[must_use]
    pub fn image(&self) -> &str {
        &self.image
    }

    /// Opens one connection for one worker.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection fails.
    pub async fn client(&self) -> anyhow::Result<ServerClient> {
        match self.server {
            Server::Hazelcast => {
                let stream = TcpStream::connect(("127.0.0.1", self.port)).await?;
                stream.set_nodelay(true)?;
                Ok(ServerClient::Memcache(BufReader::new(stream)))
            }
            server => {
                let client = redis::Client::open(format!("redis://127.0.0.1:{}/", self.port))?;
                Ok(ServerClient::Resp {
                    server,
                    conn: client.get_multiplexed_async_connection().await?,
                })
            }
        }
    }

    /// Memory the server reports for its data, where it reports a usable
    /// figure: `used_memory` for Redis, Valkey and Dragonfly.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn memory_used(&self) -> anyhow::Result<Option<u64>> {
        match self.server {
            // Hazelcast's memcache protocol reports no memory figure, and
            // Olric's Go heap still holds garbage the collector has not
            // reclaimed, which no RESP command can force first.
            Server::Hazelcast | Server::Olric => Ok(None),
            Server::Redis | Server::Valkey | Server::Dragonfly => {
                let ServerClient::Resp { mut conn, .. } = self.client().await? else {
                    return Ok(None);
                };
                let info: String = redis::cmd("INFO")
                    .arg("memory")
                    .query_async(&mut conn)
                    .await?;
                Ok(parse_used_memory(&info))
            }
        }
    }

    pub async fn shutdown(self) {
        let _ = self.guard.stop().await;
    }
}

impl ServerClient {
    async fn ping(&mut self) -> anyhow::Result<()> {
        match self {
            Self::Resp { conn, .. } => {
                let _: String = redis::cmd("PING").query_async(conn).await?;
                Ok(())
            }
            Self::Memcache(stream) => {
                stream.get_mut().write_all(b"version\r\n").await?;
                let mut line = String::new();
                stream.read_line(&mut line).await?;
                if line.starts_with("VERSION") {
                    Ok(())
                } else {
                    bail!("unexpected memcache reply {line:?}")
                }
            }
        }
    }

    /// Reads `key`, returning whether it was a hit.
    ///
    /// # Errors
    ///
    /// Returns an error if the server fails the request.
    pub async fn get(&mut self, key: &str) -> anyhow::Result<bool> {
        match self {
            Self::Resp {
                server: Server::Olric,
                conn,
            } => {
                let reply: redis::RedisResult<Option<Vec<u8>>> = redis::cmd("DM.GET")
                    .arg(OLRIC_DMAP)
                    .arg(key)
                    .query_async(conn)
                    .await;
                match reply {
                    Ok(value) => Ok(value.is_some()),
                    Err(error) if is_olric_miss(&error.to_string()) => Ok(false),
                    Err(error) => Err(error.into()),
                }
            }
            Self::Resp { conn, .. } => {
                let value: Option<Vec<u8>> = redis::cmd("GET").arg(key).query_async(conn).await?;
                Ok(value.is_some())
            }
            Self::Memcache(stream) => memcache_get(stream, key).await,
        }
    }

    /// Writes `key`.
    ///
    /// # Errors
    ///
    /// Returns an error if the server fails the request.
    pub async fn set(&mut self, key: &str, value: &[u8]) -> anyhow::Result<()> {
        match self {
            Self::Resp {
                server: Server::Olric,
                conn,
            } => {
                let () = redis::cmd("DM.PUT")
                    .arg(OLRIC_DMAP)
                    .arg(key)
                    .arg(value)
                    .query_async(conn)
                    .await?;
                Ok(())
            }
            Self::Resp { conn, .. } => {
                let () = redis::cmd("SET")
                    .arg(key)
                    .arg(value)
                    .query_async(conn)
                    .await?;
                Ok(())
            }
            Self::Memcache(stream) => memcache_set(stream, key, value).await,
        }
    }
}

async fn memcache_set(
    stream: &mut BufReader<TcpStream>,
    key: &str,
    value: &[u8],
) -> anyhow::Result<()> {
    let mut request = format!("set {key} 0 0 {}\r\n", value.len()).into_bytes();
    request.extend_from_slice(value);
    request.extend_from_slice(b"\r\n");
    stream.get_mut().write_all(&request).await?;
    let mut line = String::new();
    stream.read_line(&mut line).await?;
    if line.trim_end() == "STORED" {
        Ok(())
    } else {
        bail!("memcache set answered {line:?}")
    }
}

async fn memcache_get(stream: &mut BufReader<TcpStream>, key: &str) -> anyhow::Result<bool> {
    stream
        .get_mut()
        .write_all(format!("get {key}\r\n").as_bytes())
        .await?;
    let mut line = String::new();
    stream.read_line(&mut line).await?;
    match parse_memcache_header(&line)? {
        MemcacheHeader::End => Ok(false),
        MemcacheHeader::Value(len) => {
            let mut body = vec![0; len + 2];
            stream.read_exact(&mut body).await?;
            line.clear();
            stream.read_line(&mut line).await?;
            if line.trim_end() == "END" {
                Ok(true)
            } else {
                bail!("memcache get ended with {line:?}")
            }
        }
    }
}

/// The first line of a memcache `get` reply.
#[derive(Debug, PartialEq, Eq)]
pub enum MemcacheHeader {
    /// `END`: a miss.
    End,
    /// `VALUE <key> <flags> <bytes>`: a hit of this many bytes.
    Value(usize),
}

/// Parses the first line of a memcache `get` reply.
///
/// # Errors
///
/// Returns an error for any other line.
pub fn parse_memcache_header(line: &str) -> anyhow::Result<MemcacheHeader> {
    let line = line.trim_end();
    if line == "END" {
        return Ok(MemcacheHeader::End);
    }
    let mut parts = line.split_whitespace();
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some("VALUE"), Some(_key), Some(_flags), Some(bytes)) => {
            Ok(MemcacheHeader::Value(bytes.parse()?))
        }
        _ => bail!("unexpected memcache reply {line:?}"),
    }
}

/// The `used_memory` figure from a RESP `INFO memory` reply.
#[must_use]
pub fn parse_used_memory(info: &str) -> Option<u64> {
    info.lines()
        .find_map(|line| line.trim_end().strip_prefix("used_memory:"))
        .and_then(|value| value.parse().ok())
}

/// Whether an Olric error reply is its miss, a key the map does not hold.
#[must_use]
pub fn is_olric_miss(message: &str) -> bool {
    message.to_ascii_lowercase().contains("key not found")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memcache_headers_parse_hits_misses_and_garbage() {
        assert_eq!(
            parse_memcache_header("END\r\n").unwrap(),
            MemcacheHeader::End
        );
        assert_eq!(
            parse_memcache_header("VALUE key:0000000001 0 100\r\n").unwrap(),
            MemcacheHeader::Value(100)
        );
        assert!(parse_memcache_header("SERVER_ERROR out of memory\r\n").is_err());
    }

    #[test]
    fn used_memory_comes_from_the_info_reply() {
        let info = "# Memory\r\nused_memory:1234567\r\nused_memory_human:1.18M\r\n";
        assert_eq!(parse_used_memory(info), Some(1_234_567));
        assert_eq!(parse_used_memory("# Memory\r\n"), None);
    }

    #[test]
    fn olric_misses_are_recognized() {
        assert!(is_olric_miss("ERR key not found"));
        assert!(!is_olric_miss("ERR connection refused"));
    }

    #[test]
    fn every_server_has_a_default_image_and_port() {
        for server in [
            Server::Redis,
            Server::Valkey,
            Server::Dragonfly,
            Server::Olric,
            Server::Hazelcast,
        ] {
            assert!(!server.image().is_empty());
            assert!(server.port() > 0);
        }
    }
}

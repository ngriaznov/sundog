//! The client of a test node's line-based control protocol.
//!
//! The node reads one command per line on its control port and writes one
//! line per command, in order. [`request`] runs one command on a fresh
//! connection; [`Pipeline`] keeps one connection open and lets a window of
//! commands wait for their replies at once, which is how the load driver
//! keeps a node busy.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::{Semaphore, TryAcquireError};
use tokio::task::JoinHandle;

/// How many commands a [`Pipeline`] has in flight at once.
pub const WINDOW: usize = 16;

/// How long [`Pipeline::connect`] waits for the connection.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// Whether a reply line reports a failure: the node answers `err <why>`.
#[must_use]
pub fn is_error_reply(reply: &str) -> bool {
    reply == "err" || reply.starts_with("err ")
}

/// Runs one command on a fresh connection and returns its reply line.
///
/// # Errors
///
/// Returns an error when the connection fails, when the node closes it before
/// it replies, when `deadline` passes, or when the node replies with an
/// `err` line.
pub async fn request(addr: SocketAddr, line: &str, deadline: Duration) -> io::Result<String> {
    let exchange = async {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        let (reader, mut writer) = stream.into_split();
        writer.write_all(format!("{line}\n").as_bytes()).await?;
        let mut lines = BufReader::new(reader).lines();
        lines.next_line().await?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "the node closed the connection without a reply",
            )
        })
    };
    let reply = tokio::time::timeout(deadline, exchange)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, format!("`{line}` timed out")))??;
    if is_error_reply(&reply) {
        return Err(io::Error::other(format!("`{line}`: {reply}")));
    }
    Ok(reply)
}

/// What the reader half of a [`Pipeline`] has seen.
#[derive(Debug, Default)]
struct Tally {
    replies: AtomicU64,
    errors: AtomicU64,
    closed: AtomicBool,
}

/// One open control connection with up to [`WINDOW`] commands awaiting their
/// replies. A reply frees a place in the window; a closed connection fails
/// every later send.
#[derive(Debug)]
pub struct Pipeline {
    writer: OwnedWriteHalf,
    window: Arc<Semaphore>,
    tally: Arc<Tally>,
    reader: JoinHandle<()>,
}

impl Pipeline {
    /// Connects to `addr`.
    ///
    /// # Errors
    ///
    /// Returns an error when the connection fails or takes longer than
    /// [`CONNECT_TIMEOUT`].
    pub async fn connect(addr: SocketAddr) -> io::Result<Self> {
        let stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connecting timed out"))??;
        stream.set_nodelay(true)?;
        let (reader, writer) = stream.into_split();
        let window = Arc::new(Semaphore::new(WINDOW));
        let tally = Arc::new(Tally::default());
        let reader = tokio::spawn(read_replies(
            BufReader::new(reader),
            Arc::clone(&window),
            Arc::clone(&tally),
        ));
        Ok(Self {
            writer,
            window,
            tally,
            reader,
        })
    }

    /// Sends every command in `lines`, waiting for a place in the window
    /// whenever it is full. The commands go out in as few writes as the
    /// window allows.
    ///
    /// # Errors
    ///
    /// Returns an error when the connection has closed or a write fails.
    pub async fn send_all<I>(&mut self, lines: I) -> io::Result<()>
    where
        I: IntoIterator<Item = String>,
    {
        let mut batch = String::new();
        for line in lines {
            let acquired = self
                .window
                .try_acquire()
                .map(tokio::sync::SemaphorePermit::forget);
            match acquired {
                Ok(()) => {}
                Err(TryAcquireError::NoPermits) => {
                    self.flush(&mut batch).await?;
                    self.window.acquire().await.map_err(|_| closed())?.forget();
                }
                Err(TryAcquireError::Closed) => return Err(closed()),
            }
            batch.push_str(&line);
            batch.push('\n');
        }
        self.flush(&mut batch).await
    }

    async fn flush(&mut self, batch: &mut String) -> io::Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let result = self.writer.write_all(batch.as_bytes()).await;
        batch.clear();
        result
    }

    /// How many replies have arrived.
    #[must_use]
    pub fn replies(&self) -> u64 {
        self.tally.replies.load(Ordering::Relaxed)
    }

    /// How many replies were `err` lines.
    #[must_use]
    pub fn errors(&self) -> u64 {
        self.tally.errors.load(Ordering::Relaxed)
    }

    /// Whether the node closed the connection.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.tally.closed.load(Ordering::Relaxed)
    }
}

impl Drop for Pipeline {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

fn closed() -> io::Error {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        "the control connection is closed",
    )
}

/// Counts the replies, frees a window place for each and, when the stream
/// ends, closes the window so a blocked sender wakes with an error.
async fn read_replies(
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    window: Arc<Semaphore>,
    tally: Arc<Tally>,
) {
    let mut lines = reader.lines();
    while let Ok(Some(line)) = lines.next_line().await {
        tally.replies.fetch_add(1, Ordering::Relaxed);
        if is_error_reply(&line) {
            tally.errors.fetch_add(1, Ordering::Relaxed);
        }
        window.add_permits(1);
    }
    tally.closed.store(true, Ordering::Relaxed);
    window.close();
}

#[cfg(test)]
mod tests {
    use tokio::net::TcpListener;

    use super::*;

    /// A node that answers every line with `reply(line)` until the client
    /// goes away.
    async fn node(reply: fn(&str) -> Option<String>) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let (reader, mut writer) = socket.into_split();
                    let mut lines = BufReader::new(reader).lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        match reply(&line) {
                            Some(text) => {
                                if writer
                                    .write_all(format!("{text}\n").as_bytes())
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                            None => return,
                        }
                    }
                });
            }
        });
        addr
    }

    fn echo(line: &str) -> Option<String> {
        Some(match line {
            "bad" => "err no such command".to_owned(),
            "hang-up" => return None,
            other => format!("val {other}"),
        })
    }

    #[test]
    fn only_err_lines_are_errors() {
        assert!(is_error_reply("err"));
        assert!(is_error_reply("err fill needs a count"));
        assert!(!is_error_reply("ok"));
        assert!(!is_error_reply("val err"));
        assert!(!is_error_reply("error"));
        assert!(!is_error_reply(""));
    }

    #[tokio::test]
    async fn a_request_returns_the_reply_line() {
        let addr = node(echo).await;
        let reply = request(addr, "count", Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(reply, "val count");
    }

    #[tokio::test]
    async fn an_err_reply_is_an_error_naming_the_command() {
        let addr = node(echo).await;
        let error = request(addr, "bad", Duration::from_secs(5))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("`bad`: err no such command"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn a_node_that_hangs_up_or_never_answers_is_an_error() {
        let addr = node(echo).await;
        let hung = request(addr, "hang-up", Duration::from_secs(5))
            .await
            .unwrap_err();
        assert_eq!(hung.kind(), io::ErrorKind::UnexpectedEof);

        let silent = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let silent_addr = silent.local_addr().unwrap();
        tokio::spawn(async move {
            let _held = silent.accept().await;
            std::future::pending::<()>().await;
        });
        let slow = request(silent_addr, "count", Duration::from_millis(200))
            .await
            .unwrap_err();
        assert_eq!(slow.kind(), io::ErrorKind::TimedOut);

        // Nothing listens on a port that was just freed.
        let freed = {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap()
        };
        assert!(
            request(freed, "count", Duration::from_secs(2))
                .await
                .is_err()
        );
    }

    async fn eventually(mut done: impl FnMut() -> bool) {
        for _ in 0..200 {
            if done() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("the condition did not hold within 5 s");
    }

    #[tokio::test]
    async fn a_pipeline_sends_more_commands_than_its_window_and_counts_every_reply() {
        let addr = node(echo).await;
        let mut pipe = Pipeline::connect(addr).await.unwrap();
        let lines = (0..100).map(|i| {
            if i % 10 == 0 {
                "bad".to_owned()
            } else {
                format!("get k{i}")
            }
        });
        pipe.send_all(lines).await.unwrap();
        eventually(|| pipe.replies() == 100).await;
        assert_eq!(pipe.errors(), 10);
        assert!(!pipe.is_closed());
        pipe.send_all(std::iter::empty()).await.unwrap();
        assert_eq!(pipe.replies(), 100);
    }

    #[tokio::test]
    async fn a_pipeline_whose_node_goes_away_closes_and_refuses_more() {
        let addr = node(echo).await;
        let mut pipe = Pipeline::connect(addr).await.unwrap();
        pipe.send_all(["hang-up".to_owned()]).await.unwrap();
        eventually(|| pipe.is_closed()).await;
        // The window is closed, so even a send that must wait for a place
        // fails at once instead of blocking.
        let flood = (0..(WINDOW * 4)).map(|i| format!("get k{i}"));
        let sent = tokio::time::timeout(Duration::from_secs(5), pipe.send_all(flood))
            .await
            .expect("a closed pipeline never blocks");
        assert!(sent.is_err());
    }

    #[tokio::test]
    async fn a_pipeline_cannot_connect_to_a_closed_port() {
        let freed = {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap()
        };
        assert!(Pipeline::connect(freed).await.is_err());
    }
}

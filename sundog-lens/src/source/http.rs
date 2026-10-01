//! A minimal HTTP/1.1 `GET` over a Tokio TCP stream: enough to read a node's
//! `/metrics` and `/readyz` from the exporter, which answers with a
//! `Content-Length` and closes the connection.

use std::fmt;
use std::time::Duration;

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;

/// A response larger than this is refused.
pub const MAX_RESPONSE: usize = 8 * 1024 * 1024;

/// Why a request or a response failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HttpError {
    /// The URL is not `http://host[:port]/path`.
    BadUrl(String),
    /// The response has no blank line ending its headers.
    NoHeaderEnd,
    /// The status line is not `HTTP/1.x <code> ...`.
    BadStatusLine,
    /// The body is not UTF-8.
    BadBody,
    /// The response exceeds [`MAX_RESPONSE`].
    TooLarge,
    /// The request took longer than its deadline.
    Timeout,
    /// The connection or the transfer failed.
    Io(String),
}

impl fmt::Display for HttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadUrl(url) => write!(f, "not an http URL: {url}"),
            Self::NoHeaderEnd => f.write_str("response has no header end"),
            Self::BadStatusLine => f.write_str("response has a malformed status line"),
            Self::BadBody => f.write_str("response body is not UTF-8"),
            Self::TooLarge => write!(f, "response is larger than {MAX_RESPONSE} bytes"),
            Self::Timeout => f.write_str("request timed out"),
            Self::Io(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for HttpError {}

/// Splits a raw response into its status code and body.
///
/// # Errors
///
/// Returns [`HttpError::NoHeaderEnd`] when the headers never end,
/// [`HttpError::BadStatusLine`] when the first line is not an HTTP status
/// line, and [`HttpError::BadBody`] when the body is not UTF-8.
pub fn split_response(raw: &[u8]) -> Result<(u16, &str), HttpError> {
    let end = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or(HttpError::NoHeaderEnd)?;
    let head = std::str::from_utf8(&raw[..end]).map_err(|_| HttpError::BadStatusLine)?;
    let status_line = head.lines().next().ok_or(HttpError::BadStatusLine)?;
    let mut parts = status_line.split_whitespace();
    if !parts
        .next()
        .is_some_and(|version| version.starts_with("HTTP/1."))
    {
        return Err(HttpError::BadStatusLine);
    }
    let status = parts
        .next()
        .and_then(|code| code.parse().ok())
        .ok_or(HttpError::BadStatusLine)?;
    let body = std::str::from_utf8(&raw[end + 4..]).map_err(|_| HttpError::BadBody)?;
    Ok((status, body))
}

/// An `http://` URL split for a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Url {
    /// The `host:port` to connect to; the port defaults to 80.
    pub connect: String,
    /// The `Host` header value: the authority as written.
    pub host: String,
    /// The request path; `/` when the URL has none.
    pub path: String,
}

/// Splits `http://host[:port]/path` into the address to connect to, the `Host`
/// header value and the request path.
///
/// # Errors
///
/// Returns [`HttpError::BadUrl`] for another scheme, an empty host or a bad
/// port.
pub fn split_url(url: &str) -> Result<Url, HttpError> {
    let bad = || HttpError::BadUrl(url.to_owned());
    let rest = url.strip_prefix("http://").ok_or_else(bad)?;
    let (authority, path) = rest.find('/').map_or((rest, "/"), |i| rest.split_at(i));
    if authority.is_empty() || authority.contains('@') {
        return Err(bad());
    }
    let host_port = if authority_has_port(authority) {
        let port = authority.rsplit(':').next().ok_or_else(bad)?;
        port.parse::<u16>().map_err(|_| bad())?;
        authority.to_owned()
    } else {
        format!("{authority}:80")
    };
    Ok(Url {
        connect: host_port,
        host: authority.to_owned(),
        path: path.to_owned(),
    })
}

/// Whether `authority` ends in `:port`. An IPv6 literal in brackets has a
/// port only after the closing bracket.
fn authority_has_port(authority: &str) -> bool {
    match authority.rfind(']') {
        Some(close) => authority[close..].contains(':'),
        None => authority.contains(':'),
    }
}

/// Fetches `url` with one `GET` and returns the status and the body, all
/// within `deadline`.
///
/// # Errors
///
/// Returns [`HttpError::Timeout`] past the deadline, [`HttpError::Io`] when
/// the connection or transfer fails, [`HttpError::TooLarge`] past
/// [`MAX_RESPONSE`], and the errors of [`split_url`] and [`split_response`].
pub async fn get(url: &str, deadline: Duration) -> Result<(u16, String), HttpError> {
    let Url {
        connect: connect_to,
        host,
        path,
    } = split_url(url)?;
    let exchange = async {
        let mut stream = TcpStream::connect(&connect_to)
            .await
            .map_err(|e| HttpError::Io(format!("connect {connect_to}: {e}")))?;
        let request = format!(
            "GET {path} HTTP/1.1\r\nHost: {host}\r\nAccept: */*\r\nUser-Agent: sundog-lens\r\n\
             Connection: close\r\n\r\n"
        );
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|e| HttpError::Io(format!("write: {e}")))?;
        let mut raw = Vec::new();
        let mut chunk = vec![0u8; 16 * 1024];
        loop {
            let read = stream
                .read(&mut chunk)
                .await
                .map_err(|e| HttpError::Io(format!("read: {e}")))?;
            if read == 0 {
                break;
            }
            raw.extend_from_slice(&chunk[..read]);
            if raw.len() > MAX_RESPONSE {
                return Err(HttpError::TooLarge);
            }
        }
        let (status, body) = split_response(&raw)?;
        Ok((status, body.to_owned()))
    };
    tokio::time::timeout(deadline, exchange)
        .await
        .map_err(|_| HttpError::Timeout)?
}

#[cfg(test)]
mod tests {
    use tokio::net::TcpListener;

    use super::*;

    #[test]
    fn splits_a_200_into_status_and_body() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nready\n";
        assert_eq!(split_response(raw), Ok((200, "ready\n")));
    }

    #[test]
    fn splits_a_503_and_a_404() {
        let raw = b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 10\r\n\r\nnot ready\n";
        assert_eq!(split_response(raw), Ok((503, "not ready\n")));
        let raw = b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
        assert_eq!(split_response(raw), Ok((404, "")));
    }

    #[test]
    fn a_response_without_a_header_end_is_an_error() {
        assert_eq!(
            split_response(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n"),
            Err(HttpError::NoHeaderEnd)
        );
        assert_eq!(split_response(b""), Err(HttpError::NoHeaderEnd));
    }

    #[test]
    fn a_bad_status_line_or_body_is_an_error() {
        assert_eq!(
            split_response(b"garbage\r\n\r\n"),
            Err(HttpError::BadStatusLine)
        );
        assert_eq!(
            split_response(b"HTTP/1.1 abc OK\r\n\r\n"),
            Err(HttpError::BadStatusLine)
        );
        assert_eq!(
            split_response(b"HTTP/1.1 200 OK\r\n\r\n\xff\xfe"),
            Err(HttpError::BadBody)
        );
    }

    #[test]
    fn a_body_may_hold_a_blank_line() {
        let raw = b"HTTP/1.1 200 OK\r\n\r\na\r\n\r\nb";
        assert_eq!(split_response(raw), Ok((200, "a\r\n\r\nb")));
    }

    fn url(connect: &str, host: &str, path: &str) -> Url {
        Url {
            connect: connect.into(),
            host: host.into(),
            path: path.into(),
        }
    }

    #[test]
    fn split_url_defaults_the_port_and_path() {
        assert_eq!(
            split_url("http://127.0.0.11:9090/metrics"),
            Ok(url("127.0.0.11:9090", "127.0.0.11:9090", "/metrics"))
        );
        assert_eq!(
            split_url("http://example.test"),
            Ok(url("example.test:80", "example.test", "/"))
        );
        assert_eq!(
            split_url("http://[::1]:9090/readyz"),
            Ok(url("[::1]:9090", "[::1]:9090", "/readyz"))
        );
        assert_eq!(
            split_url("http://[::1]/x"),
            Ok(url("[::1]:80", "[::1]", "/x"))
        );
    }

    #[test]
    fn split_url_refuses_other_schemes_and_bad_authorities() {
        for url in [
            "https://h/x",
            "h:9090/x",
            "http://",
            "http:///x",
            "http://h:notaport/x",
            "http://h:99999/x",
            "http://user@h/x",
        ] {
            assert_eq!(
                split_url(url),
                Err(HttpError::BadUrl(url.to_owned())),
                "{url}"
            );
        }
    }

    /// Serves `response` once on a loopback port and returns the URL.
    async fn serve_once(response: &'static [u8]) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf).await.unwrap();
            stream.write_all(response).await.unwrap();
            stream.shutdown().await.unwrap();
        });
        format!("http://{addr}/metrics")
    }

    #[tokio::test]
    async fn get_returns_the_status_and_body() {
        let url = serve_once(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok").await;
        let result = get(&url, Duration::from_secs(5)).await;
        assert_eq!(result, Ok((200, "ok".to_owned())));
    }

    #[tokio::test]
    async fn get_times_out_on_a_silent_server() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let _held = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(30)).await;
        });
        let result = get(&url, Duration::from_millis(100)).await;
        assert_eq!(result, Err(HttpError::Timeout));
    }

    #[tokio::test]
    async fn get_reports_a_refused_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        drop(listener);
        let result = get(&url, Duration::from_secs(5)).await;
        assert!(matches!(result, Err(HttpError::Io(_))), "{result:?}");
    }

    #[tokio::test]
    async fn get_rejects_a_truncated_response() {
        let url = serve_once(b"HTTP/1.1 200 OK\r\nContent-Le").await;
        let result = get(&url, Duration::from_secs(5)).await;
        assert_eq!(result, Err(HttpError::NoHeaderEnd));
    }

    #[tokio::test]
    async fn get_refuses_a_response_past_the_size_cap() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/metrics", listener.local_addr().unwrap());
        let header: &[u8] = b"HTTP/1.1 200 OK\r\n\r\n";
        let body = vec![b'x'; MAX_RESPONSE + 1 - header.len()];
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf).await.unwrap();
            // The client stops reading at the cap, so a late write may fail.
            let _ = stream.write_all(header).await;
            let _ = stream.write_all(&body).await;
            let _ = stream.shutdown().await;
        });
        let result = get(&url, Duration::from_secs(5)).await;
        assert_eq!(result, Err(HttpError::TooLarge));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn get_accepts_a_response_of_exactly_the_size_cap() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/metrics", listener.local_addr().unwrap());
        let header: &[u8] = b"HTTP/1.1 200 OK\r\n\r\n";
        let body = vec![b'x'; MAX_RESPONSE - header.len()];
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf).await.unwrap();
            stream.write_all(header).await.unwrap();
            stream.write_all(&body).await.unwrap();
            stream.shutdown().await.unwrap();
        });
        let (status, text) = get(&url, Duration::from_secs(5)).await.unwrap();
        assert_eq!((status, text.len()), (200, MAX_RESPONSE - header.len()));
    }

    #[test]
    fn errors_display_a_reason() {
        assert!(HttpError::Timeout.to_string().contains("timed out"));
        assert!(HttpError::BadUrl("x".into()).to_string().contains('x'));
        assert!(HttpError::TooLarge.to_string().contains("larger"));
    }
}

//! What one scrape of a node's exporter reports.

use std::fmt;
use std::net::SocketAddr;
use std::time::Instant;

use sundog::NodeId;

use super::expo::Sample;
use super::http::HttpError;

/// Why a scrape failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScrapeError {
    /// The exporter did not answer within the deadline.
    Timeout,
    /// The connection failed.
    Connect(String),
    /// The exporter answered `/metrics` with a status other than 200.
    Status(u16),
    /// The answer is not a well-formed HTTP response.
    Malformed(String),
}

impl From<HttpError> for ScrapeError {
    fn from(error: HttpError) -> Self {
        match error {
            HttpError::Timeout => Self::Timeout,
            HttpError::Io(message) => Self::Connect(message),
            other => Self::Malformed(other.to_string()),
        }
    }
}

impl fmt::Display for ScrapeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout => f.write_str("timed out"),
            Self::Connect(message) | Self::Malformed(message) => f.write_str(message),
            Self::Status(status) => write!(f, "HTTP {status}"),
        }
    }
}

impl std::error::Error for ScrapeError {}

/// The outcome of one scrape round of one node.
#[derive(Debug, Clone, PartialEq)]
pub struct ScrapeReport {
    /// The gossip address of the member the exporter is mapped to.
    pub addr: SocketAddr,
    /// The node id of that member when the scrape started.
    pub node: NodeId,
    /// When the round finished.
    pub at: Instant,
    /// The `sundog_*` samples of `/metrics`, or why there are none.
    pub outcome: Result<Vec<Sample>, ScrapeError>,
    /// The `/readyz` verdict: `Some(true)` for 200, `Some(false)` for 503,
    /// `None` when not probed in this round or the probe failed.
    pub ready: Option<bool>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_errors_map_to_scrape_errors() {
        assert_eq!(ScrapeError::from(HttpError::Timeout), ScrapeError::Timeout);
        assert_eq!(
            ScrapeError::from(HttpError::Io("refused".into())),
            ScrapeError::Connect("refused".into())
        );
        assert!(matches!(
            ScrapeError::from(HttpError::NoHeaderEnd),
            ScrapeError::Malformed(_)
        ));
        assert!(matches!(
            ScrapeError::from(HttpError::BadUrl("x".into())),
            ScrapeError::Malformed(_)
        ));
    }

    #[test]
    fn scrape_errors_display_a_reason() {
        assert_eq!(ScrapeError::Timeout.to_string(), "timed out");
        assert_eq!(ScrapeError::Status(503).to_string(), "HTTP 503");
        assert_eq!(
            ScrapeError::Connect("refused".into()).to_string(),
            "refused"
        );
        assert_eq!(ScrapeError::Malformed("bad".into()).to_string(), "bad");
    }
}

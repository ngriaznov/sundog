//! Node identity: a per-process-incarnation random id and the derived name used
//! as chitchat's cluster-membership identifier.

use std::fmt;

use rand::RngExt as _;
use serde::{Deserialize, Serialize};

/// A compact, random identifier for one running instance of the process.
///
/// Generated fresh per incarnation (not persisted): a restarted process is, by
/// design, a new node as far as membership and HLC tie-breaking are concerned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(u64);

impl NodeId {
    /// Generates a new random node id.
    #[must_use]
    pub fn random() -> Self {
        Self(rand::rng().random())
    }

    /// Returns the raw numeric value.
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl From<u64> for NodeId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl fmt::Display for NodeId {
    /// Renders as lowercase hex, e.g. `a1b2c3d4e5f60718`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// The human-readable, cluster-unique name derived from a node's hostname and
/// [`NodeId`]: `{hostname}-{nodeid-hex}`. This is the string chitchat uses as
/// its node id.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeName(String);

impl NodeName {
    /// Builds the canonical `{hostname}-{nodeid-hex}` name.
    #[must_use]
    pub fn new(hostname: &str, node_id: NodeId) -> Self {
        Self(format!("{hostname}-{node_id}"))
    }

    /// Returns the name as a plain string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for NodeName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn random_ids_differ() {
        assert_ne!(NodeId::random(), NodeId::random());
    }

    #[test]
    fn display_is_lowercase_hex_16_chars() {
        let id = NodeId::from(0xdead_beef_cafe_babe);
        let text = id.to_string();
        assert_eq!(text, "deadbeefcafebabe");
        assert_eq!(text.len(), 16);
        assert!(
            text.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
    }

    #[test]
    fn node_name_format() {
        let id = NodeId::from(1);
        let name = NodeName::new("host1", id);
        assert_eq!(name.as_str(), "host1-0000000000000001");
    }

    #[test]
    fn ordering_matches_numeric_value() {
        let a = NodeId::from(1);
        let b = NodeId::from(2);
        assert!(a < b);
    }

    #[test]
    fn roundtrips_through_postcard() {
        let id = NodeId::random();
        let bytes = postcard::to_stdvec(&id).expect("invariant: NodeId always encodes");
        let decoded: NodeId =
            postcard::from_bytes(&bytes).expect("invariant: freshly encoded bytes decode");
        assert_eq!(id, decoded);
    }
}

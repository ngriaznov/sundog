//! Scrape target templates: a URL with placeholders that each member's
//! addresses fill in.
//!
//! | Placeholder | Filled with |
//! |---|---|
//! | `{ip}` | the member's gossip IP (an IPv6 address in brackets) |
//! | `{gossip_port}` | its gossip port |
//! | `{data_port}` | its data port |
//! | `{node_id}` | its node id, 16 lowercase hex digits |
//! | `{gossip_port+N}`, `{gossip_port-N}` | its gossip port shifted by `N` |

use std::fmt;
use std::net::IpAddr;

use sundog::observe::Member;

/// Why a template failed to parse or expand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TemplateError {
    /// A `{` has no closing `}`, or a `}` has no opening `{`.
    UnmatchedBrace,
    /// A placeholder the template language does not define.
    UnknownPlaceholder(String),
    /// A `{gossip_port±N}` offset that is not a number.
    BadOffset(String),
    /// A shifted port fell below 0.
    PortUnderflow {
        /// The member's port.
        port: u16,
        /// The offset applied.
        offset: i32,
    },
    /// A shifted port rose above 65,535.
    PortOverflow {
        /// The member's port.
        port: u16,
        /// The offset applied.
        offset: i32,
    },
}

impl fmt::Display for TemplateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnmatchedBrace => f.write_str("unmatched brace in template"),
            Self::UnknownPlaceholder(name) => write!(f, "unknown placeholder {{{name}}}"),
            Self::BadOffset(text) => write!(f, "bad port offset in {{{text}}}"),
            Self::PortUnderflow { port, offset } => {
                write!(f, "gossip port {port} {offset:+} is below 0")
            }
            Self::PortOverflow { port, offset } => {
                write!(f, "gossip port {port} {offset:+} is above 65535")
            }
        }
    }
}

impl std::error::Error for TemplateError {}

/// One piece of a parsed template.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Segment {
    Literal(String),
    Ip,
    GossipPort(i32),
    DataPort,
    NodeId,
}

/// A parsed `--metrics` template.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UrlTemplate {
    segments: Vec<Segment>,
}

impl UrlTemplate {
    /// Parses `template`.
    ///
    /// # Errors
    ///
    /// Returns [`TemplateError::UnmatchedBrace`] for a stray brace,
    /// [`TemplateError::UnknownPlaceholder`] for a name outside the table in
    /// the module docs, and [`TemplateError::BadOffset`] for a shift that is
    /// not a whole number.
    pub fn parse(template: &str) -> Result<Self, TemplateError> {
        let mut segments = Vec::new();
        let mut literal = String::new();
        let mut rest = template;
        while let Some(i) = rest.find(['{', '}']) {
            literal.push_str(&rest[..i]);
            if rest.as_bytes()[i] == b'}' {
                return Err(TemplateError::UnmatchedBrace);
            }
            let after = &rest[i + 1..];
            let close = after
                .find(['{', '}'])
                .ok_or(TemplateError::UnmatchedBrace)?;
            if after.as_bytes()[close] == b'{' {
                return Err(TemplateError::UnmatchedBrace);
            }
            if !literal.is_empty() {
                segments.push(Segment::Literal(std::mem::take(&mut literal)));
            }
            segments.push(parse_placeholder(&after[..close])?);
            rest = &after[close + 1..];
        }
        literal.push_str(rest);
        if !literal.is_empty() {
            segments.push(Segment::Literal(literal));
        }
        Ok(Self { segments })
    }

    /// The URL for `member`.
    ///
    /// # Errors
    ///
    /// Returns [`TemplateError::PortUnderflow`] or
    /// [`TemplateError::PortOverflow`] when a `{gossip_port±N}` shift leaves
    /// the port range.
    pub fn expand(&self, member: &Member) -> Result<String, TemplateError> {
        let peer = &member.peer;
        let mut url = String::new();
        for segment in &self.segments {
            match segment {
                Segment::Literal(text) => url.push_str(text),
                Segment::Ip => match peer.gossip_addr.ip() {
                    IpAddr::V4(ip) => url.push_str(&ip.to_string()),
                    IpAddr::V6(ip) => {
                        url.push('[');
                        url.push_str(&ip.to_string());
                        url.push(']');
                    }
                },
                Segment::GossipPort(offset) => {
                    let port = peer.gossip_addr.port();
                    let shifted = i32::from(port) + offset;
                    let shifted = u16::try_from(shifted).map_err(|_| {
                        if shifted < 0 {
                            TemplateError::PortUnderflow {
                                port,
                                offset: *offset,
                            }
                        } else {
                            TemplateError::PortOverflow {
                                port,
                                offset: *offset,
                            }
                        }
                    })?;
                    url.push_str(&shifted.to_string());
                }
                Segment::DataPort => url.push_str(&peer.data_addr.port().to_string()),
                Segment::NodeId => url.push_str(&peer.node.to_string()),
            }
        }
        Ok(url)
    }
}

/// Parses the text between a pair of braces.
fn parse_placeholder(name: &str) -> Result<Segment, TemplateError> {
    match name {
        "ip" => Ok(Segment::Ip),
        "gossip_port" => Ok(Segment::GossipPort(0)),
        "data_port" => Ok(Segment::DataPort),
        "node_id" => Ok(Segment::NodeId),
        _ => {
            let Some(shift) = name.strip_prefix("gossip_port") else {
                return Err(TemplateError::UnknownPlaceholder(name.to_owned()));
            };
            let sign = match shift.as_bytes().first() {
                Some(b'+') => 1,
                Some(b'-') => -1,
                _ => return Err(TemplateError::UnknownPlaceholder(name.to_owned())),
            };
            let digits = &shift[1..];
            if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
                return Err(TemplateError::BadOffset(name.to_owned()));
            }
            let magnitude: i32 = digits
                .parse()
                .map_err(|_| TemplateError::BadOffset(name.to_owned()))?;
            Ok(Segment::GossipPort(sign * magnitude))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::time::SystemTime;

    use sundog::membership::Peer;
    use sundog::node::{NodeId, NodeName};
    use sundog::observe::MemberStatus;

    use super::*;

    fn member(gossip: &str, data: &str, node: u64) -> Member {
        let node = NodeId::from(node);
        Member::new(
            Peer {
                node,
                name: NodeName::new("host", node),
                gossip_addr: gossip.parse::<SocketAddr>().unwrap(),
                data_addr: data.parse::<SocketAddr>().unwrap(),
                incarnation: 1,
                protocol: 6,
            },
            MemberStatus::Live,
            SystemTime::UNIX_EPOCH,
            BTreeMap::new(),
        )
    }

    fn expand(template: &str, gossip: &str) -> Result<String, TemplateError> {
        UrlTemplate::parse(template)?.expand(&member(
            gossip,
            "127.0.0.11:39211",
            0x7f3a_51d2_e09b_c210,
        ))
    }

    #[test]
    fn a_literal_url_expands_to_itself() {
        assert_eq!(
            expand("http://example.test:9090/metrics", "127.0.0.11:7946").unwrap(),
            "http://example.test:9090/metrics"
        );
        assert_eq!(expand("", "127.0.0.11:7946").unwrap(), "");
    }

    #[test]
    fn ip_and_ports_fill_in() {
        assert_eq!(
            expand("http://{ip}:9090/metrics", "127.0.0.11:7946").unwrap(),
            "http://127.0.0.11:9090/metrics"
        );
        assert_eq!(
            expand("http://{ip}:{gossip_port}/", "10.1.2.3:7946").unwrap(),
            "http://10.1.2.3:7946/"
        );
        assert_eq!(expand("{data_port}", "10.1.2.3:7946").unwrap(), "39211");
    }

    #[test]
    fn node_id_is_sixteen_hex_digits() {
        assert_eq!(
            expand("/{node_id}", "10.1.2.3:7946").unwrap(),
            "/7f3a51d2e09bc210"
        );
        let small = UrlTemplate::parse("{node_id}")
            .unwrap()
            .expand(&member("10.1.2.3:7946", "10.1.2.3:1", 0xab))
            .unwrap();
        assert_eq!(small, "00000000000000ab");
    }

    #[test]
    fn gossip_port_offsets_add_and_subtract() {
        assert_eq!(
            expand("http://{ip}:{gossip_port+200}/", "10.0.0.1:7946").unwrap(),
            "http://10.0.0.1:8146/"
        );
        assert_eq!(expand("{gossip_port-1}", "10.0.0.1:7946").unwrap(), "7945");
        assert_eq!(expand("{gossip_port+0}", "10.0.0.1:7946").unwrap(), "7946");
    }

    #[test]
    fn a_shifted_port_that_leaves_the_range_is_an_error() {
        assert_eq!(
            expand("{gossip_port-7947}", "10.0.0.1:7946"),
            Err(TemplateError::PortUnderflow {
                port: 7946,
                offset: -7947
            })
        );
        assert_eq!(
            expand("{gossip_port+60000}", "10.0.0.1:7946"),
            Err(TemplateError::PortOverflow {
                port: 7946,
                offset: 60000
            })
        );
        assert_eq!(expand("{gossip_port-7946}", "10.0.0.1:7946").unwrap(), "0");
        assert_eq!(
            expand("{gossip_port+57589}", "10.0.0.1:7946").unwrap(),
            "65535"
        );
        assert!(expand("{gossip_port+57590}", "10.0.0.1:7946").is_err());
    }

    #[test]
    fn an_ipv6_address_is_bracketed() {
        assert_eq!(
            expand("http://{ip}:9090/", "[::1]:7946").unwrap(),
            "http://[::1]:9090/"
        );
    }

    #[test]
    fn an_unknown_placeholder_is_an_error() {
        assert_eq!(
            UrlTemplate::parse("http://{host}/"),
            Err(TemplateError::UnknownPlaceholder("host".into()))
        );
        assert_eq!(
            UrlTemplate::parse("{gossip_portal}"),
            Err(TemplateError::UnknownPlaceholder("gossip_portal".into()))
        );
        assert_eq!(
            UrlTemplate::parse("{}"),
            Err(TemplateError::UnknownPlaceholder(String::new()))
        );
    }

    #[test]
    fn a_bad_offset_is_an_error() {
        for text in [
            "gossip_port+",
            "gossip_port+x",
            "gossip_port-1.5",
            "gossip_port+99999999999",
        ] {
            assert_eq!(
                UrlTemplate::parse(&format!("{{{text}}}")),
                Err(TemplateError::BadOffset(text.into())),
                "{text}"
            );
        }
    }

    #[test]
    fn stray_braces_are_errors() {
        for template in ["http://{ip", "ip}", "{ip{gossip_port}}", "{{ip}}", "a}b"] {
            assert_eq!(
                UrlTemplate::parse(template),
                Err(TemplateError::UnmatchedBrace),
                "{template}"
            );
        }
    }

    #[test]
    fn two_members_expand_to_their_own_urls() {
        let template = UrlTemplate::parse("http://{ip}:9090/metrics").unwrap();
        let a = template
            .expand(&member("127.0.0.11:7946", "127.0.0.11:1", 1))
            .unwrap();
        let b = template
            .expand(&member("127.0.0.12:7946", "127.0.0.12:1", 2))
            .unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn errors_display_a_reason() {
        assert!(TemplateError::UnmatchedBrace.to_string().contains("brace"));
        assert!(
            TemplateError::UnknownPlaceholder("x".into())
                .to_string()
                .contains("{x}")
        );
        assert!(
            TemplateError::PortUnderflow {
                port: 5,
                offset: -9
            }
            .to_string()
            .contains("-9")
        );
        assert!(
            TemplateError::PortOverflow { port: 5, offset: 9 }
                .to_string()
                .contains("+9")
        );
        assert!(
            TemplateError::BadOffset("gossip_port+".into())
                .to_string()
                .contains("offset")
        );
    }
}

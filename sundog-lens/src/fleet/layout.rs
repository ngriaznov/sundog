//! How a fleet addresses its nodes.
//!
//! A test node serves gossip, control and the exporter on ports that are
//! fixed unless the environment overrides them. A [`Layout`] decides who gets
//! which address:
//!
//! - [`Layout::PerAddress`] gives slot `i` the `i`th loopback address from a
//!   base (`127.0.0.11`, `127.0.0.12`, ...) and the fixed ports on it. Linux
//!   answers on all of `127.0.0.0/8`, so the addresses need no setup there;
//!   macOS answers on `127.0.0.1` alone unless the user adds aliases.
//! - [`Layout::Shared`] puts every slot on `127.0.0.1` and gives slot `i` the
//!   fixed ports plus `i - 1`, through the node's port overrides. It works on
//!   every platform without setup.
//!
//! Every function here is pure, so both layouts are tested on every
//! platform.

use std::net::{Ipv4Addr, SocketAddr};

use super::proc::{CONTROL_PORT, GOSSIP_PORT, METRICS_PORT, slot_ip};

/// The ports of one slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ports {
    /// The gossip port.
    pub gossip: u16,
    /// The control port.
    pub control: u16,
    /// The exporter port.
    pub metrics: u16,
}

impl Ports {
    /// The ports of a test node that is not overridden.
    pub const FIXED: Self = Self {
        gossip: GOSSIP_PORT,
        control: CONTROL_PORT,
        metrics: METRICS_PORT,
    };

    /// The fixed ports, each shifted up by `offset`. `None` when a port
    /// leaves the port range.
    #[must_use]
    pub fn shifted(offset: u16) -> Option<Self> {
        Some(Self {
            gossip: GOSSIP_PORT.checked_add(offset)?,
            control: CONTROL_PORT.checked_add(offset)?,
            metrics: METRICS_PORT.checked_add(offset)?,
        })
    }
}

/// How the slots of a fleet share the loopback addresses and the ports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layout {
    /// Slot 1 at the given address, slot `i` at the `i`th address from it;
    /// every slot on the fixed ports.
    PerAddress(Ipv4Addr),
    /// Every slot at `127.0.0.1`; slot `i` on the fixed ports plus `i - 1`.
    Shared,
}

impl Layout {
    /// Slot 1's address when a per-address layout is chosen by default.
    pub const DEFAULT_BASE: Ipv4Addr = Ipv4Addr::new(127, 0, 0, 11);

    /// The layout a fleet gets. An explicit `base_ip` always means
    /// [`Layout::PerAddress`] at that address. Without one, a Linux host
    /// (`linux` is true) gets [`Layout::PerAddress`] at [`Layout::DEFAULT_BASE`]
    /// and any other host gets [`Layout::Shared`].
    #[must_use]
    pub const fn choose(base_ip: Option<Ipv4Addr>, linux: bool) -> Self {
        match base_ip {
            Some(base) => Self::PerAddress(base),
            None if linux => Self::PerAddress(Self::DEFAULT_BASE),
            None => Self::Shared,
        }
    }

    /// The layout a fleet gets on this host: [`Layout::choose`] with the
    /// platform.
    #[must_use]
    pub const fn for_host(base_ip: Option<Ipv4Addr>) -> Self {
        Self::choose(base_ip, cfg!(target_os = "linux"))
    }

    /// Whether the slots share one address.
    #[must_use]
    pub const fn is_shared(self) -> bool {
        matches!(self, Self::Shared)
    }

    /// The address of slot `slot` (1-based). `None` for slot 0 and past the
    /// end of the IPv4 space.
    #[must_use]
    pub fn ip(self, slot: usize) -> Option<Ipv4Addr> {
        match self {
            Self::PerAddress(base) => slot_ip(base, slot),
            Self::Shared => (slot >= 1).then_some(Ipv4Addr::LOCALHOST),
        }
    }

    /// The ports of slot `slot` (1-based). `None` for slot 0 and when a
    /// shifted port leaves the port range.
    #[must_use]
    pub fn ports(self, slot: usize) -> Option<Ports> {
        let index = slot.checked_sub(1)?;
        match self {
            Self::PerAddress(_) => Some(Ports::FIXED),
            Self::Shared => Ports::shifted(u16::try_from(index).ok()?),
        }
    }

    /// The gossip address of slot `slot` (1-based).
    #[must_use]
    pub fn gossip_addr(self, slot: usize) -> Option<SocketAddr> {
        Some(SocketAddr::from((self.ip(slot)?, self.ports(slot)?.gossip)))
    }

    /// The gossip address of slot 1, which always exists.
    #[must_use]
    pub const fn first_gossip(self) -> SocketAddr {
        let ip = match self {
            Self::PerAddress(base) => base,
            Self::Shared => Ipv4Addr::LOCALHOST,
        };
        SocketAddr::new(std::net::IpAddr::V4(ip), GOSSIP_PORT)
    }

    /// The gossip addresses a node or an observer joins through: those of
    /// the first two slots that exist.
    #[must_use]
    pub fn seeds(self) -> Vec<SocketAddr> {
        (1..=2).filter_map(|slot| self.gossip_addr(slot)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: Ipv4Addr = Ipv4Addr::new(127, 0, 0, 11);

    #[test]
    fn an_explicit_base_address_is_always_per_address() {
        let base = Ipv4Addr::new(127, 0, 0, 21);
        for linux in [true, false] {
            assert_eq!(
                Layout::choose(Some(base), linux),
                Layout::PerAddress(base),
                "linux={linux}"
            );
        }
    }

    #[test]
    fn without_a_base_address_linux_is_per_address_and_other_hosts_share_one() {
        assert_eq!(Layout::choose(None, true), Layout::PerAddress(BASE));
        assert_eq!(Layout::choose(None, false), Layout::Shared);
        assert_eq!(Layout::DEFAULT_BASE, BASE);
    }

    #[test]
    fn the_host_default_follows_the_platform() {
        let expected = if cfg!(target_os = "linux") {
            Layout::PerAddress(BASE)
        } else {
            Layout::Shared
        };
        assert_eq!(Layout::for_host(None), expected);
        assert_eq!(
            Layout::for_host(Some(Ipv4Addr::LOCALHOST)),
            Layout::PerAddress(Ipv4Addr::LOCALHOST)
        );
    }

    #[test]
    fn only_the_shared_layout_is_shared() {
        assert!(Layout::Shared.is_shared());
        assert!(!Layout::PerAddress(BASE).is_shared());
    }

    #[test]
    fn per_address_slots_count_up_the_addresses_on_the_fixed_ports() {
        let layout = Layout::PerAddress(BASE);
        assert_eq!(layout.ip(1), Some(BASE));
        assert_eq!(layout.ip(3), Some(Ipv4Addr::new(127, 0, 0, 13)));
        assert_eq!(layout.ip(0), None);
        assert_eq!(Layout::PerAddress(Ipv4Addr::BROADCAST).ip(2), None);
        for slot in [1, 2, 6] {
            assert_eq!(layout.ports(slot), Some(Ports::FIXED), "slot {slot}");
        }
        assert_eq!(layout.ports(0), None);
        assert_eq!(
            layout.gossip_addr(2),
            Some("127.0.0.12:7946".parse().unwrap())
        );
        assert_eq!(layout.gossip_addr(0), None);
    }

    #[test]
    fn shared_slots_keep_one_address_and_count_up_the_ports() {
        let layout = Layout::Shared;
        for slot in [1, 2, 6] {
            assert_eq!(layout.ip(slot), Some(Ipv4Addr::LOCALHOST), "slot {slot}");
        }
        assert_eq!(layout.ip(0), None);
        assert_eq!(
            layout.ports(1),
            Some(Ports {
                gossip: 7946,
                control: 8080,
                metrics: 9090
            })
        );
        assert_eq!(
            layout.ports(3),
            Some(Ports {
                gossip: 7948,
                control: 8082,
                metrics: 9092
            })
        );
        assert_eq!(layout.ports(0), None);
        assert_eq!(
            layout.gossip_addr(2),
            Some("127.0.0.1:7947".parse().unwrap())
        );
        assert_eq!(layout.gossip_addr(0), None);
    }

    #[test]
    fn the_seeds_are_the_gossip_addresses_of_the_first_two_slots() {
        assert_eq!(
            Layout::PerAddress(BASE).seeds(),
            [
                "127.0.0.11:7946".parse().unwrap(),
                "127.0.0.12:7946".parse().unwrap()
            ]
        );
        assert_eq!(
            Layout::Shared.seeds(),
            [
                "127.0.0.1:7946".parse().unwrap(),
                "127.0.0.1:7947".parse().unwrap()
            ]
        );
        assert_eq!(Layout::PerAddress(Ipv4Addr::BROADCAST).seeds().len(), 1);
    }

    #[test]
    fn the_first_gossip_address_is_slot_ones() {
        for layout in [Layout::PerAddress(BASE), Layout::Shared] {
            assert_eq!(Some(layout.first_gossip()), layout.gossip_addr(1));
            assert_eq!(Some(layout.first_gossip()), layout.seeds().first().copied());
        }
        assert_eq!(
            Layout::PerAddress(BASE).first_gossip(),
            "127.0.0.11:7946".parse().unwrap()
        );
        assert_eq!(
            Layout::Shared.first_gossip(),
            "127.0.0.1:7946".parse().unwrap()
        );
    }

    #[test]
    fn a_shifted_port_that_leaves_the_range_is_none() {
        assert_eq!(
            Ports::shifted(0),
            Some(Ports::FIXED),
            "no shift is the fixed ports"
        );
        assert_eq!(
            Ports::shifted(u16::MAX - 9090),
            Some(Ports {
                gossip: 7946 + (u16::MAX - 9090),
                control: 8080 + (u16::MAX - 9090),
                metrics: u16::MAX,
            })
        );
        assert_eq!(Ports::shifted(u16::MAX - 9089), None);
        assert_eq!(Layout::Shared.ports(usize::MAX), None);
        assert_eq!(Layout::Shared.ports(70_000), None);
    }
}

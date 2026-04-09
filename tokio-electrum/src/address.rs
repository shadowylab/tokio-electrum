//! Electrum server address

use std::fmt;
use std::net::IpAddr;
use std::num::ParseIntError;
use std::str::FromStr;

/// Electrum address parse error
#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    /// Parse int error
    ParseInt(ParseIntError),
    /// Invalid format
    InvalidFormat,
    /// Missing scheme
    MissingScheme,
    /// Invalid address scheme
    InvalidScheme,
    /// Missing host
    MissingHost,
    /// Empty host
    EmptyHost,
    /// Missing port
    MissingPort,
}

impl core::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ParseInt(e) => e.fmt(f),
            Self::InvalidFormat => write!(f, "invalid format"),
            Self::MissingScheme => write!(f, "missing scheme"),
            Self::InvalidScheme => write!(f, "invalid scheme"),
            Self::MissingHost => write!(f, "missing host"),
            Self::EmptyHost => write!(f, "empty host"),
            Self::MissingPort => write!(f, "missing port"),
        }
    }
}

impl From<ParseIntError> for Error {
    fn from(e: ParseIntError) -> Self {
        Self::ParseInt(e)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) enum Scheme {
    Tcp,
    Ssl,
}

impl fmt::Display for Scheme {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Scheme {
    fn parse(scheme: &str) -> Result<Self, Error> {
        match scheme {
            "tcp" => Ok(Self::Tcp),
            "ssl" => Ok(Self::Ssl),
            _ => Err(Error::InvalidScheme),
        }
    }

    fn as_str(&self) -> &str {
        match self {
            Self::Tcp => "tcp",
            Self::Ssl => "ssl",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) enum Host {
    Ip(IpAddr),
    Domain(String),
}

impl fmt::Display for Host {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ip(ip) => match ip {
                IpAddr::V4(ip) => write!(f, "{ip}"),
                IpAddr::V6(ip) => write!(f, "[{ip}]"),
            },
            Self::Domain(domain) => write!(f, "{domain}"),
        }
    }
}

impl Host {
    fn parse(host: &str) -> Result<Self, Error> {
        if host.is_empty() {
            return Err(Error::EmptyHost);
        }

        match IpAddr::from_str(host) {
            Ok(ip) => Ok(Self::Ip(ip)),
            Err(_) => Ok(Self::Domain(host.to_string())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct HostAndPort {
    pub host: Host,
    pub port: u16,
}

impl fmt::Display for HostAndPort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

impl HostAndPort {
    fn parse(host_and_port: &str) -> Result<Self, Error> {
        // Handle IPv6 addresses wrapped in brackets
        let (host_str, port_str) = if host_and_port.starts_with('[') {
            // IPv6 format: [ipv6]:port
            let bracket_end: usize = host_and_port.find(']').ok_or(Error::InvalidFormat)?;
            let ipv6_str: &str = host_and_port
                .get(1..bracket_end)
                .ok_or(Error::InvalidFormat)?; // Remove brackets

            // Check for port after closing bracket
            let remaining: &str = host_and_port
                .get(bracket_end + 1..)
                .ok_or(Error::InvalidFormat)?;
            if !remaining.starts_with(':') {
                return Err(Error::InvalidFormat);
            }
            let port_str: &str = remaining.get(1..).ok_or(Error::MissingPort)?; // Remove the colon

            (ipv6_str, port_str)
        } else {
            // IPv4 or domain format: host:port
            // Split host and port by the last ':'
            let colon_pos: usize = host_and_port.rfind(':').ok_or(Error::MissingPort)?;
            let host_str: &str = host_and_port.get(..colon_pos).ok_or(Error::MissingHost)?;
            let port_str: &str = host_and_port
                .get(colon_pos + 1..)
                .ok_or(Error::MissingPort)?;

            (host_str, port_str)
        };

        // Parse host
        let host: Host = Host::parse(host_str)?;

        // Parse port
        let port: u16 = u16::from_str(port_str)?;

        Ok(Self { host, port })
    }
}

/// Electrum server address
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ElectrumServerAddress {
    scheme: Scheme,
    addr: HostAndPort,
}

impl fmt::Display for ElectrumServerAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}://{}", self.scheme, self.addr)
    }
}

impl ElectrumServerAddress {
    /// Parse an electrum server address
    ///
    /// Allowed formats:
    /// - `tcp://<ip-or-domain>:<port>`
    /// - `ssl://<ip-or-domain>:<port>`
    pub fn parse(addr: &str) -> Result<Self, Error> {
        // Split by "://"
        let (scheme, host_and_port): (&str, &str) =
            addr.split_once("://").ok_or(Error::MissingScheme)?;

        // Parse scheme
        let scheme: Scheme = Scheme::parse(scheme)?;

        // Parse host and port
        let addr: HostAndPort = HostAndPort::parse(host_and_port)?;

        // Construct address
        Ok(Self { scheme, addr })
    }

    pub(super) fn scheme(&self) -> &Scheme {
        &self.scheme
    }

    pub(super) fn addr(&self) -> &HostAndPort {
        &self.addr
    }
}

impl FromStr for ElectrumServerAddress {
    type Err = Error;

    #[inline]
    fn from_str(addr: &str) -> Result<Self, Self::Err> {
        Self::parse(addr)
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr};

    use super::*;

    #[test]
    fn test_parse_tcp_with_domain() {
        let addr_str = "tcp://electrum.example.com:50001";
        let addr = ElectrumServerAddress::parse(addr_str).unwrap();
        assert_eq!(addr.scheme, Scheme::Tcp);
        assert_eq!(
            addr.addr.host,
            Host::Domain(String::from("electrum.example.com"))
        );
        assert_eq!(addr.addr.port, 50001);
        assert_eq!(addr_str, addr.to_string());
    }

    #[test]
    fn test_parse_ssl_with_domain() {
        let addr_str = "ssl://electrum.example.com:50002";
        let addr: ElectrumServerAddress = ElectrumServerAddress::parse(addr_str).unwrap();
        assert_eq!(addr.scheme, Scheme::Ssl);
        assert_eq!(
            addr.addr.host,
            Host::Domain(String::from("electrum.example.com"))
        );
        assert_eq!(addr.addr.port, 50002);
        assert_eq!(addr_str, addr.to_string());
    }

    #[test]
    fn test_parse_tcp_with_ipv4() {
        let addr_str = "tcp://127.0.0.1:50001";
        let addr = ElectrumServerAddress::parse(addr_str).unwrap();
        assert_eq!(addr.scheme, Scheme::Tcp);
        assert_eq!(addr.addr.host, Host::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)));
        assert_eq!(addr.addr.port, 50001);
        assert_eq!(addr_str, addr.to_string());
    }

    #[test]
    fn test_parse_ssl_with_ipv4() {
        let addr_str = "ssl://127.0.0.1:50002";
        let addr = ElectrumServerAddress::parse(addr_str).unwrap();
        assert_eq!(addr.scheme, Scheme::Ssl);
        assert_eq!(addr.addr.host, Host::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)));
        assert_eq!(addr.addr.port, 50002);
        assert_eq!(addr_str, addr.to_string());
    }

    #[test]
    fn test_parse_tcp_with_ipv6() {
        let addr_str = "tcp://[::1]:50001";
        let addr = ElectrumServerAddress::parse(addr_str).unwrap();
        assert_eq!(addr.scheme, Scheme::Tcp);
        assert_eq!(addr.addr.host, Host::Ip(IpAddr::V6(Ipv6Addr::LOCALHOST)));
        assert_eq!(addr.addr.port, 50001);
        assert_eq!(addr_str, addr.to_string());
    }

    #[test]
    fn test_parse_ssl_with_ipv6() {
        let addr_str = "ssl://[2001:db8::1]:50002";
        let addr: ElectrumServerAddress = ElectrumServerAddress::parse(addr_str).unwrap();
        assert_eq!(addr.scheme, Scheme::Ssl);
        assert_eq!(
            addr.addr.host,
            Host::Ip(IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)))
        );
        assert_eq!(addr.addr.port, 50002);
        assert_eq!(addr_str, addr.to_string());
    }

    #[test]
    fn test_parse_with_invalid_scheme() {
        let result = ElectrumServerAddress::parse("http://example.com:80");
        assert_eq!(result.unwrap_err(), Error::InvalidScheme);
    }

    #[test]
    fn test_parse_with_empty_host() {
        let result = ElectrumServerAddress::parse("tcp://:50001");
        assert_eq!(result.unwrap_err(), Error::EmptyHost);
    }

    #[test]
    fn test_parse_without_scheme() {
        let result = ElectrumServerAddress::parse("electrum.example.com:50001");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_without_port() {
        let result = ElectrumServerAddress::parse("ssl://electrum.example.com");
        assert_eq!(result.unwrap_err(), Error::MissingPort);
    }
}

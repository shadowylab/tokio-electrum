//! Electrum client configs

#[cfg(feature = "socks")]
use std::net::SocketAddr;
use std::time::Duration;

use bitcoin::Network;

use super::constant::{
    DEFAULT_COMMAND_CHANNEL_SIZE, DEFAULT_CONNECTION_TIMEOUT,
    DEFAULT_MAX_CONSECUTIVE_PING_TIMEOUTS, DEFAULT_NOTIFICATION_CHANNEL_SIZE, DEFAULT_PING_TIMEOUT,
    DEFAULT_RECONNECT_DELAY_INITIAL, DEFAULT_RECONNECT_DELAY_MAX, DEFAULT_REQUEST_TIMEOUT,
};
use crate::address::ElectrumServerAddress;
use crate::client::ElectrumClient;

/// Electrum connection mode
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ElectrumConnectionMode {
    /// Direct
    #[default]
    Direct,
    /// Custom proxy
    #[cfg(feature = "socks")]
    Proxy(SocketAddr),
}

/// Electrum client configs
#[derive(Debug, Clone)]
pub struct ElectrumClientBuilder {
    /// Electrum server address
    pub addr: ElectrumServerAddress,
    /// Connection mode
    pub connection_mode: ElectrumConnectionMode,
    /// Connection timeout
    pub connection_timeout: Duration,
    /// Request timeout
    pub request_timeout: Duration,
    /// Ping timeout
    pub ping_timeout: Duration,
    /// Initial reconnect delay
    pub reconnect_delay_initial: Duration,
    /// Maximum reconnect delay
    pub reconnect_delay_max: Duration,
    /// Max consecutive ping timeouts before forcing disconnect
    pub max_consecutive_ping_timeouts: u8,
    /// Command channel size
    pub command_channel_size: usize,
    /// Notification channel size
    pub notification_channel_size: usize,
    /// Expected Bitcoin network
    ///
    /// When specified, the client will verify that the server operates on the same network during connection.
    pub expected_network: Option<Network>,
}

impl ElectrumClientBuilder {
    /// New electrum client builder
    #[inline]
    pub fn new(addr: ElectrumServerAddress) -> Self {
        Self {
            addr,
            connection_mode: ElectrumConnectionMode::default(),
            connection_timeout: DEFAULT_CONNECTION_TIMEOUT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            ping_timeout: DEFAULT_PING_TIMEOUT,
            reconnect_delay_initial: DEFAULT_RECONNECT_DELAY_INITIAL,
            reconnect_delay_max: DEFAULT_RECONNECT_DELAY_MAX,
            max_consecutive_ping_timeouts: DEFAULT_MAX_CONSECUTIVE_PING_TIMEOUTS,
            command_channel_size: DEFAULT_COMMAND_CHANNEL_SIZE,
            notification_channel_size: DEFAULT_NOTIFICATION_CHANNEL_SIZE,
            expected_network: None,
        }
    }

    /// Set a connection mode
    #[inline]
    pub fn connection_mode(mut self, mode: ElectrumConnectionMode) -> Self {
        self.connection_mode = mode;
        self
    }

    /// Set a custom connection timeout (default: 60 secs)
    #[inline]
    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = timeout;
        self
    }

    /// Set a custom request timeout (default: 60 secs)
    #[inline]
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Set a custom ping timeout (default: 15 secs)
    #[inline]
    pub fn ping_timeout(mut self, timeout: Duration) -> Self {
        self.ping_timeout = timeout;
        self
    }

    /// Set a custom initial reconnect delay (default: 2 secs)
    #[inline]
    pub fn reconnect_delay_initial(mut self, delay: Duration) -> Self {
        self.reconnect_delay_initial = delay;
        self
    }

    /// Set a custom maximum reconnect delay (default: 30 secs)
    #[inline]
    pub fn reconnect_delay_max(mut self, delay: Duration) -> Self {
        self.reconnect_delay_max = delay;
        self
    }

    /// Set max consecutive ping timeouts before forcing reconnect (default: 3)
    #[inline]
    pub fn max_consecutive_ping_timeouts(mut self, max: u8) -> Self {
        self.max_consecutive_ping_timeouts = max;
        self
    }

    /// Set a custom command channel size (default: 4096)
    #[inline]
    pub fn command_channel_size(mut self, size: usize) -> Self {
        self.command_channel_size = size;
        self
    }

    /// Set a custom notification channel size (default: 4096)
    #[inline]
    pub fn notification_channel_size(mut self, size: usize) -> Self {
        self.notification_channel_size = size;
        self
    }

    /// Expected Bitcoin network
    ///
    /// When specified, the client will verify that the server operates on the same network during connection.
    pub fn expected_network(mut self, network: Network) -> Self {
        self.expected_network = Some(network);
        self
    }

    /// Build client
    #[inline]
    pub fn build(self) -> ElectrumClient {
        ElectrumClient::from_builder(self)
    }
}

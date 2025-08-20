//! Electrum client configs

#[cfg(feature = "socks")]
use std::net::SocketAddr;
use std::time::Duration;

use super::constant::{
    DEFAULT_CONNECTION_TIMEOUT, DEFAULT_NOTIFICATION_CHANNEL_SIZE, DEFAULT_REQUEST_TIMEOUT,
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
    /// Notification channel size
    pub notification_channel_size: usize,
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
            notification_channel_size: DEFAULT_NOTIFICATION_CHANNEL_SIZE,
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

    /// Set a custom notification channel size (default: 4096)
    #[inline]
    pub fn notification_channel_size(mut self, size: usize) -> Self {
        self.notification_channel_size = size;
        self
    }

    /// Build client
    #[inline]
    pub fn build(self) -> ElectrumClient {
        ElectrumClient::from_builder(self)
    }
}

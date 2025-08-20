//! Electrum client configs

#[cfg(feature = "socks")]
use std::net::SocketAddr;
use std::time::Duration;

use super::constant::{DEFAULT_CONNECTION_TIMEOUT, DEFAULT_NOTIFICATION_CHANNEL_SIZE};

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
pub struct ElectrumConfig {
    pub(crate) connection_timeout: Duration,
    pub(crate) connection_mode: ElectrumConnectionMode,
    pub(crate) notification_channel_size: usize,
}

impl Default for ElectrumConfig {
    fn default() -> Self {
        Self {
            connection_timeout: DEFAULT_CONNECTION_TIMEOUT,
            connection_mode: ElectrumConnectionMode::default(),
            notification_channel_size: DEFAULT_NOTIFICATION_CHANNEL_SIZE,
        }
    }
}

impl ElectrumConfig {
    /// New default configs
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set a custom connection timeout (default: 60 secs)
    #[inline]
    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = timeout;
        self
    }

    /// Set a connection mode
    #[inline]
    pub fn connection_mode(mut self, mode: ElectrumConnectionMode) -> Self {
        self.connection_mode = mode;
        self
    }

    /// Set a custom notification channel size (default: 4096)
    #[inline]
    pub fn notification_channel_size(mut self, size: usize) -> Self {
        self.notification_channel_size = size;
        self
    }
}

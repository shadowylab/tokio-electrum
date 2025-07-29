use std::time::Duration;

use super::constant::{DEFAULT_CONNECTION_TIMEOUT, DEFAULT_NOTIFICATION_CHANNEL_SIZE};

/// Electrum client configs
#[derive(Debug, Clone)]
pub struct ElectrumConfig {
    pub(crate) connection_timeout: Duration,
    pub(crate) notification_channel_size: usize,
}

impl Default for ElectrumConfig {
    fn default() -> Self {
        Self {
            connection_timeout: DEFAULT_CONNECTION_TIMEOUT,
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

    /// Set a custom notification channel size (default: 4096)
    #[inline]
    pub fn notification_channel_size(mut self, size: usize) -> Self {
        self.notification_channel_size = size;
        self
    }
}

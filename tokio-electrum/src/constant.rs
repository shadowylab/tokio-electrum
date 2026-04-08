use std::time::Duration;

pub(super) const DEFAULT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(60);
pub(super) const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
pub(super) const DEFAULT_PING_TIMEOUT: Duration = Duration::from_secs(15);

// Keep the ping interval below 30 sec.
// Some servers, like ssl://blockstream.info:700, close the connection if no ping is done every <30 sec.
pub(super) const PING_INTERVAL: Duration = Duration::from_secs(20);

/// Default notification channel size
pub(super) const DEFAULT_NOTIFICATION_CHANNEL_SIZE: usize = 4096;

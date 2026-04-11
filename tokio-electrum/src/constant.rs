use std::num::NonZeroUsize;
use std::time::Duration;

pub(super) const DEFAULT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(60);
pub(super) const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
pub(super) const DEFAULT_PING_TIMEOUT: Duration = Duration::from_secs(15);
pub(super) const DEFAULT_RECONNECT_DELAY_INITIAL: Duration = Duration::from_secs(2);
pub(super) const DEFAULT_RECONNECT_DELAY_MAX: Duration = Duration::from_secs(30);
pub(super) const DEFAULT_MAX_CONSECUTIVE_PING_TIMEOUTS: u8 = 3;
pub(super) const DEFAULT_COMMAND_CHANNEL_SIZE: NonZeroUsize = NonZeroUsize::new(4096).unwrap();

// Keep the ping interval below 30 sec.
// Some servers, like ssl://blockstream.info:700, close the connection if no ping is done every <30 sec.
pub(super) const PING_INTERVAL: Duration = Duration::from_secs(20);

/// Default notification channel size
pub(super) const DEFAULT_NOTIFICATION_CHANNEL_SIZE: NonZeroUsize = NonZeroUsize::new(4096).unwrap();

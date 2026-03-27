//! Electrum connection status

use std::fmt;
use std::sync::atomic::{AtomicU8, Ordering};

#[derive(Debug)]
pub(super) struct AtomicElectrumConnectionStatus {
    value: AtomicU8,
}

impl Default for AtomicElectrumConnectionStatus {
    fn default() -> Self {
        Self::new(ElectrumConnectionStatus::Initialized)
    }
}

impl AtomicElectrumConnectionStatus {
    #[inline]
    pub(super) fn new(status: ElectrumConnectionStatus) -> Self {
        Self {
            value: AtomicU8::new(status as u8),
        }
    }

    #[inline]
    pub fn set(&self, status: ElectrumConnectionStatus) {
        self.value.store(status as u8, Ordering::SeqCst);
    }

    pub(super) fn load(&self) -> ElectrumConnectionStatus {
        let val: u8 = self.value.load(Ordering::SeqCst);
        match val {
            0 => ElectrumConnectionStatus::Initialized,
            1 => ElectrumConnectionStatus::Pending,
            2 => ElectrumConnectionStatus::Connecting,
            3 => ElectrumConnectionStatus::Connected,
            4 => ElectrumConnectionStatus::Disconnected,
            5 => ElectrumConnectionStatus::Terminated,
            _ => unreachable!(),
        }
    }
}

/// Electrum connection status
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ElectrumConnectionStatus {
    /// The client has just been created.
    Initialized = 0,
    /// The client will try to connect shortly.
    Pending = 1,
    /// Trying to connecting.
    Connecting = 2,
    /// Connected.
    Connected = 3,
    /// The connection failed, but another attempt will occur soon.
    Disconnected = 4,
    /// The connection has been terminated and no retry will occur.
    Terminated = 5,
}

impl fmt::Display for ElectrumConnectionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Initialized => write!(f, "Initialized"),
            Self::Pending => write!(f, "Pending"),
            Self::Connecting => write!(f, "Connecting"),
            Self::Connected => write!(f, "Connected"),
            Self::Disconnected => write!(f, "Disconnected"),
            Self::Terminated => write!(f, "Terminated"),
        }
    }
}

impl ElectrumConnectionStatus {
    // #[inline]
    // pub(crate) fn is_initialized(&self) -> bool {
    //     matches!(self, Self::Initialized)
    // }

    /// Check if it's connected.
    #[inline]
    pub fn is_connected(&self) -> bool {
        matches!(self, Self::Connected)
    }

    /// Check if is `disconnected` or `terminated`.
    #[inline]
    pub(crate) fn is_disconnected(&self) -> bool {
        matches!(self, Self::Disconnected | Self::Terminated)
    }

    /// Check if is [`RelayStatus::Terminated`]
    pub(crate) fn is_terminated(&self) -> bool {
        matches!(self, Self::Terminated)
    }

    /// Check if relay can start a connection (status is `initialized` or `terminated`)
    #[inline]
    pub(crate) fn can_connect(&self) -> bool {
        matches!(self, Self::Initialized | Self::Terminated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_set() {
        let relay = AtomicElectrumConnectionStatus::default();
        relay.set(ElectrumConnectionStatus::Connected);
        assert_eq!(relay.load(), ElectrumConnectionStatus::Connected);
    }

    #[test]
    fn test_status_initialized() {
        let status = ElectrumConnectionStatus::Initialized;
        // assert!(status.is_initialized());
        // assert!(!status.is_connected());
        assert!(!status.is_disconnected());
        assert!(!status.is_terminated());
        assert!(status.can_connect());
        let relay = AtomicElectrumConnectionStatus::new(status);
        assert_eq!(relay.load(), ElectrumConnectionStatus::Initialized);
    }

    #[test]
    fn test_status_pending() {
        let status = ElectrumConnectionStatus::Pending;
        // assert!(!status.is_initialized());
        // assert!(!status.is_connected());
        assert!(!status.is_disconnected());
        assert!(!status.is_terminated());
        assert!(!status.can_connect());
        let relay = AtomicElectrumConnectionStatus::new(status);
        assert_eq!(relay.load(), ElectrumConnectionStatus::Pending);
    }

    #[test]
    fn test_status_connecting() {
        let status = ElectrumConnectionStatus::Connecting;
        // assert!(!status.is_initialized());
        // assert!(!status.is_connected());
        assert!(!status.is_disconnected());
        assert!(!status.is_terminated());
        assert!(!status.can_connect());
        let relay = AtomicElectrumConnectionStatus::new(status);
        assert_eq!(relay.load(), ElectrumConnectionStatus::Connecting);
    }

    #[test]
    fn test_status_connected() {
        let status = ElectrumConnectionStatus::Connected;
        // assert!(!status.is_initialized());
        // assert!(status.is_connected());
        assert!(!status.is_disconnected());
        assert!(!status.is_terminated());
        assert!(!status.can_connect());
        let relay = AtomicElectrumConnectionStatus::new(status);
        assert_eq!(relay.load(), ElectrumConnectionStatus::Connected);
    }

    #[test]
    fn test_status_disconnected() {
        let status = ElectrumConnectionStatus::Disconnected;
        // assert!(!status.is_initialized());
        // assert!(!status.is_connected());
        assert!(status.is_disconnected());
        assert!(!status.is_terminated());
        assert!(!status.can_connect());
        let relay = AtomicElectrumConnectionStatus::new(status);
        assert_eq!(relay.load(), ElectrumConnectionStatus::Disconnected);
    }

    #[test]
    fn test_status_terminated() {
        let status = ElectrumConnectionStatus::Terminated;
        // assert!(!status.is_initialized());
        // assert!(!status.is_connected());
        assert!(status.is_disconnected());
        assert!(status.is_terminated());
        assert!(status.can_connect());
        let relay = AtomicElectrumConnectionStatus::new(status);
        assert_eq!(relay.load(), ElectrumConnectionStatus::Terminated);
    }
}

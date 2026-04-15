//! Electrum connection status

use std::fmt;
use std::sync::atomic::{AtomicU8, Ordering};

#[derive(Debug)]
pub(super) struct AtomicElectrumConnectionStatus {
    value: AtomicU8,
}

impl Default for AtomicElectrumConnectionStatus {
    fn default() -> Self {
        Self::new(InternalElectrumConnectionStatus::Initialized)
    }
}

impl AtomicElectrumConnectionStatus {
    #[inline]
    pub(super) fn new(status: InternalElectrumConnectionStatus) -> Self {
        Self {
            value: AtomicU8::new(status as u8),
        }
    }

    #[inline]
    pub fn set(&self, status: InternalElectrumConnectionStatus) {
        self.value.store(status as u8, Ordering::SeqCst);
    }

    pub(super) fn load(&self) -> InternalElectrumConnectionStatus {
        let val: u8 = self.value.load(Ordering::SeqCst);
        match val {
            0 => InternalElectrumConnectionStatus::Initialized,
            1 => InternalElectrumConnectionStatus::Pending,
            2 => InternalElectrumConnectionStatus::Connecting,
            3 => InternalElectrumConnectionStatus::Connected,
            4 => InternalElectrumConnectionStatus::Disconnected,
            5 => InternalElectrumConnectionStatus::Terminated,
            6 => InternalElectrumConnectionStatus::Shutdown,
            _ => unreachable!(),
        }
    }
}

/// Electrum connection status
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum InternalElectrumConnectionStatus {
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
    /// Shutdown
    Shutdown = 6,
}

impl InternalElectrumConnectionStatus {
    /// Check if is `disconnected`, `terminated` or `shutdown.
    #[inline]
    pub(crate) fn is_disconnected_terminated_or_shutdown(&self) -> bool {
        matches!(self, Self::Disconnected | Self::Terminated | Self::Shutdown)
    }

    /// Check if is [`RelayStatus::Terminated`]
    pub(crate) fn is_terminated(&self) -> bool {
        matches!(self, Self::Terminated)
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        matches!(self, Self::Shutdown)
    }

    /// Check if relay can start a connection (status is `initialized` or `terminated`)
    #[inline]
    pub(crate) fn can_connect(&self) -> bool {
        matches!(self, Self::Initialized | Self::Terminated)
    }
}

impl From<InternalElectrumConnectionStatus> for ElectrumConnectionStatus {
    fn from(status: InternalElectrumConnectionStatus) -> Self {
        match status {
            InternalElectrumConnectionStatus::Pending
            | InternalElectrumConnectionStatus::Connecting => ElectrumConnectionStatus::Connecting,
            InternalElectrumConnectionStatus::Connected => ElectrumConnectionStatus::Connected,
            InternalElectrumConnectionStatus::Initialized
            | InternalElectrumConnectionStatus::Disconnected
            | InternalElectrumConnectionStatus::Terminated
            | InternalElectrumConnectionStatus::Shutdown => ElectrumConnectionStatus::Disconnected,
        }
    }
}

/// Electrum connection status
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ElectrumConnectionStatus {
    /// Trying to connecting.
    Connecting = 2,
    /// Connected.
    Connected = 3,
    /// The client is disconnected
    Disconnected = 4,
}

impl fmt::Display for ElectrumConnectionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connecting => write!(f, "Connecting"),
            Self::Connected => write!(f, "Connected"),
            Self::Disconnected => write!(f, "Disconnected"),
        }
    }
}

impl ElectrumConnectionStatus {
    /// Check if it's connecting.
    #[inline]
    pub fn is_connecting(&self) -> bool {
        matches!(self, Self::Connecting)
    }

    /// Check if it's connected.
    #[inline]
    pub fn is_connected(&self) -> bool {
        matches!(self, Self::Connected)
    }

    /// Check if it's disconnected.
    #[inline]
    pub fn is_disconnected(&self) -> bool {
        matches!(self, Self::Disconnected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_set() {
        let relay = AtomicElectrumConnectionStatus::default();
        relay.set(InternalElectrumConnectionStatus::Connected);
        assert_eq!(relay.load(), InternalElectrumConnectionStatus::Connected);
    }

    #[test]
    fn test_status_initialized() {
        let status = InternalElectrumConnectionStatus::Initialized;
        // assert!(status.is_initialized());
        // assert!(!status.is_connected());
        assert!(!status.is_disconnected_terminated_or_shutdown());
        assert!(!status.is_terminated());
        assert!(status.can_connect());
        let relay = AtomicElectrumConnectionStatus::new(status);
        assert_eq!(relay.load(), InternalElectrumConnectionStatus::Initialized);
    }

    #[test]
    fn test_status_pending() {
        let status = InternalElectrumConnectionStatus::Pending;
        // assert!(!status.is_initialized());
        // assert!(!status.is_connected());
        assert!(!status.is_disconnected_terminated_or_shutdown());
        assert!(!status.is_terminated());
        assert!(!status.can_connect());
        let relay = AtomicElectrumConnectionStatus::new(status);
        assert_eq!(relay.load(), InternalElectrumConnectionStatus::Pending);
    }

    #[test]
    fn test_status_connecting() {
        let status = InternalElectrumConnectionStatus::Connecting;
        // assert!(!status.is_initialized());
        // assert!(!status.is_connected());
        assert!(!status.is_disconnected_terminated_or_shutdown());
        assert!(!status.is_terminated());
        assert!(!status.can_connect());
        let relay = AtomicElectrumConnectionStatus::new(status);
        assert_eq!(relay.load(), InternalElectrumConnectionStatus::Connecting);
    }

    #[test]
    fn test_status_connected() {
        let status = InternalElectrumConnectionStatus::Connected;
        // assert!(!status.is_initialized());
        // assert!(status.is_connected());
        assert!(!status.is_disconnected_terminated_or_shutdown());
        assert!(!status.is_terminated());
        assert!(!status.can_connect());
        let relay = AtomicElectrumConnectionStatus::new(status);
        assert_eq!(relay.load(), InternalElectrumConnectionStatus::Connected);
    }

    #[test]
    fn test_status_disconnected() {
        let status = InternalElectrumConnectionStatus::Disconnected;
        // assert!(!status.is_initialized());
        // assert!(!status.is_connected());
        assert!(status.is_disconnected_terminated_or_shutdown());
        assert!(!status.is_terminated());
        assert!(!status.can_connect());
        let relay = AtomicElectrumConnectionStatus::new(status);
        assert_eq!(relay.load(), InternalElectrumConnectionStatus::Disconnected);
    }

    #[test]
    fn test_status_terminated() {
        let status = InternalElectrumConnectionStatus::Terminated;
        // assert!(!status.is_initialized());
        // assert!(!status.is_connected());
        assert!(status.is_disconnected_terminated_or_shutdown());
        assert!(status.is_terminated());
        assert!(status.can_connect());
        let relay = AtomicElectrumConnectionStatus::new(status);
        assert_eq!(relay.load(), InternalElectrumConnectionStatus::Terminated);
    }
}

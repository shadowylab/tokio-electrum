use bitcoin::block::Header;

use crate::hash::{ElectrumScriptHash, ElectrumScriptStatus};
use crate::status::ElectrumConnectionStatus;

/// Electrum notification
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ElectrumNotification {
    /// Connection status changed
    ConnectionStatusChanged(ElectrumConnectionStatus),
    /// Received a new block header
    BlockHeader {
        /// Timechain block height
        height: u32,
        /// Block header
        header: Header,
    },
    /// Received a new script hash status update
    ScriptHash {
        /// Electrum script hash
        hash: ElectrumScriptHash,
        /// Status
        status: Option<ElectrumScriptStatus>,
    },
    /// The client has been shutdown
    Shutdown,
}

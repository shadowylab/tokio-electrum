//! Electrum types

use bitcoin::absolute::Height;
use bitcoin::block::Header;
use bitcoin::hashes::sha256d::Hash as Sha256d;
use electrum_streaming_client::response::{HeadersResp, HeadersSubscribeResp, TxMerkle};
pub use electrum_streaming_client::response::{ServerFeatures, Tx};
pub use electrum_streaming_client::{ElectrumScriptHash, ElectrumScriptStatus};

/// Block header with height.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlockHeader {
    /// Block header.
    pub header: Header,
    /// The height of the block in the header.
    pub height: u32,
}

impl From<HeadersSubscribeResp> for BlockHeader {
    fn from(headers_subscribe_resp: HeadersSubscribeResp) -> Self {
        Self {
            height: headers_subscribe_resp.height,
            header: headers_subscribe_resp.header,
        }
    }
}

/// Block headers.
#[derive(Debug, Clone)]
pub struct BlockHeaders {
    /// The number of headers returned.
    pub count: usize,
    /// The deserialized headers returned by the server.
    pub headers: Vec<Header>,
    /// The server’s maximum allowed headers per request.
    pub max: usize,
}

impl From<HeadersResp> for BlockHeaders {
    fn from(headers_resp: HeadersResp) -> Self {
        Self {
            count: headers_resp.count,
            headers: headers_resp.headers,
            max: headers_resp.max,
        }
    }
}

/// Transaction Merkle.
pub struct TransactionMerkel {
    /// The height of the block containing the transaction.
    pub block_height: Height,
    /// The Merkle branch connecting the transaction to the block root.
    pub merkle: Vec<Sha256d>,
    /// The transaction's position in the block's Merkle tree.
    pub pos: usize,
}

impl From<TxMerkle> for TransactionMerkel {
    fn from(tx_merkle: TxMerkle) -> Self {
        Self {
            block_height: tx_merkle.block_height,
            merkle: tx_merkle.merkle,
            pos: tx_merkle.pos,
        }
    }
}

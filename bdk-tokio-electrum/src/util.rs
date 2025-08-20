use bdk_core::bitcoin::Txid;
use bdk_core::bitcoin::hash_types::TxMerkleNode;
use bdk_core::bitcoin::hashes::sha256d::Hash as Sha256d;
use bdk_core::bitcoin::hashes::{Hash, HashEngine};
use tokio_electrum::prelude::*;

/// Verifies a Merkle inclusion proof as retrieved via [`transaction_get_merkle`] for a transaction with the
/// given `txid` and `merkle_root` as included in the [`BlockHeader`].
///
/// Returns `true` if the transaction is included in the corresponding block, and `false`
/// otherwise.
///
/// [`transaction_get_merkle`]: crate::ElectrumApi::transaction_get_merkle
/// [`BlockHeader`]: bitcoin::BlockHeader
pub fn validate_merkle_proof(
    txid: &Txid,
    merkle_root: &TxMerkleNode,
    merkle_res: &TransactionMerkel,
) -> bool {
    let mut index = merkle_res.pos;
    let mut cur = txid.to_raw_hash();
    for hash in merkle_res.merkle.iter() {
        cur = Sha256d::from_engine({
            let mut engine = Sha256d::engine();
            if index % 2 == 0 {
                engine.input(cur.as_ref());
                engine.input(hash.as_ref());
            } else {
                engine.input(hash.as_ref());
                engine.input(cur.as_ref());
            };
            engine
        });
        index /= 2;
    }

    cur == merkle_root.to_raw_hash()
}

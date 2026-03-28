use std::collections::BTreeSet;
use std::fmt::{Display, Write};

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
pub(crate) fn validate_merkle_proof(
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

#[inline]
pub(crate) fn log_scan_range<K>(wallet_label: &str, keychain: &K, start_index: u32, end_index: u32)
where
    K: Display,
{
    let mut range = String::new();

    push_range(&mut range, start_index, end_index);

    tracing::info!(
        wallet = %wallet_label,
        keychain = %keychain,
        "Finding transactions [{range}]"
    );
}

pub(crate) fn log_loading_indexes<K>(wallet_label: &str, keychain: &K, indexes: BTreeSet<u32>)
where
    K: Display,
{
    if let Some(ranges) = format_index_ranges(indexes) {
        tracing::info!(
            wallet = %wallet_label,
            keychain = %keychain,
            "Loading transactions [{ranges}]"
        );
    }
}

fn format_index_ranges(indexes: BTreeSet<u32>) -> Option<String> {
    if indexes.is_empty() {
        return None;
    }

    let mut ranges = String::new();
    let mut start = indexes.first().copied()?;
    let mut end = start;

    for index in indexes.iter().copied().skip(1) {
        if index == end.saturating_add(1) {
            end = index;
            continue;
        }

        push_range(&mut ranges, start, end);
        start = index;
        end = index;
    }
    push_range(&mut ranges, start, end);

    Some(ranges)
}

fn push_range(output: &mut String, start: u32, end: u32) {
    if !output.is_empty() {
        output.push_str(", ");
    }

    if start == end {
        let _ = write!(output, "{start}");
    } else {
        let _ = write!(output, "{start}-{end}");
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::format_index_ranges;

    #[test]
    fn format_index_ranges_returns_none_for_empty_input() {
        let indexes = BTreeSet::new();
        assert_eq!(format_index_ranges(indexes), None);
    }

    #[test]
    fn format_index_ranges_dedups_sorts_and_compacts() {
        let indexes = BTreeSet::from([11, 10, 8, 7, 5, 4, 4, 12]);
        assert_eq!(
            format_index_ranges(indexes),
            Some("4-5, 7-8, 10-12".to_string())
        );
    }

    #[test]
    fn format_index_ranges_handles_singletons() {
        let indexes = BTreeSet::from([9, 2, 14]);
        assert_eq!(format_index_ranges(indexes), Some("2, 9, 14".to_string()));
    }
}

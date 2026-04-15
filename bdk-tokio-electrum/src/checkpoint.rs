use std::collections::{BTreeMap, HashMap};

use bdk_core::CheckPoint;
use bdk_core::bitcoin::Txid;
use tokio_electrum::types::{ElectrumScriptHash, ElectrumScriptStatus};

/// Per-script checkpoint data used for incremental resume.
#[derive(Debug, Clone, PartialEq)]
pub struct ScriptSyncCheckpoint<K> {
    /// Keychain this script belongs to.
    pub keychain: K,
    /// Derivation index within keychain.
    pub index: u32,
    /// Last known Electrum status hash.
    pub last_status: Option<ElectrumScriptStatus>,
    /// Last known electrum heights keyed by txid.
    pub expected_tx_heights: HashMap<Txid, i64>,
}

/// Minimal persistent state required to resume incrementally without a full scan.
#[derive(Debug, Clone, PartialEq)]
pub struct SyncCheckpoint<K> {
    /// Last known local chain tip.
    pub chain_tip: Option<CheckPoint>,
    /// Last active derivation index per keychain.
    pub last_active_indices: BTreeMap<K, u32>,
    /// Highest subscribed derivation index per keychain.
    pub max_subscribed_indices: BTreeMap<K, u32>,
    /// Per-script checkpoints keyed by script hash.
    pub scripts: HashMap<ElectrumScriptHash, ScriptSyncCheckpoint<K>>,
}

/// Delta between two [`SyncCheckpoint`] snapshots.
///
/// The delta can be applied to a previous snapshot to reconstruct the new one.
#[derive(Debug, Clone, PartialEq)]
pub struct SyncCheckpointDelta<K> {
    /// Chain tip update.
    ///
    /// `None` means unchanged.
    /// `Some(None)` means clear chain tip.
    /// `Some(Some(tip))` means set chain tip.
    pub chain_tip: Option<Option<CheckPoint>>,
    /// Upserts for last active indices.
    pub last_active_upserts: BTreeMap<K, u32>,
    /// Removals for last active indices.
    pub last_active_removals: Vec<K>,
    /// Upserts for max subscribed indices.
    pub max_subscribed_upserts: BTreeMap<K, u32>,
    /// Removals for max subscribed indices.
    pub max_subscribed_removals: Vec<K>,
    /// Upserts for per-script checkpoints.
    pub script_upserts: HashMap<ElectrumScriptHash, ScriptSyncCheckpoint<K>>,
    /// Removals for per-script checkpoints.
    pub script_removals: Vec<ElectrumScriptHash>,
}

impl<K> Default for SyncCheckpointDelta<K> {
    fn default() -> Self {
        Self {
            chain_tip: None,
            last_active_upserts: BTreeMap::new(),
            last_active_removals: Vec::new(),
            max_subscribed_upserts: BTreeMap::new(),
            max_subscribed_removals: Vec::new(),
            script_upserts: HashMap::new(),
            script_removals: Vec::new(),
        }
    }
}

impl<K> SyncCheckpointDelta<K> {
    /// Returns `true` if delta does not change any field.
    pub fn is_empty(&self) -> bool {
        self.chain_tip.is_none()
            && self.last_active_upserts.is_empty()
            && self.last_active_removals.is_empty()
            && self.max_subscribed_upserts.is_empty()
            && self.max_subscribed_removals.is_empty()
            && self.script_upserts.is_empty()
            && self.script_removals.is_empty()
    }
}

impl<K> SyncCheckpoint<K>
where
    K: Ord + Clone,
{
    /// Build a delta from `previous` to `current`.
    pub fn diff(previous: &Self, current: &Self) -> SyncCheckpointDelta<K> {
        let mut delta = SyncCheckpointDelta {
            chain_tip: (previous.chain_tip != current.chain_tip).then(|| current.chain_tip.clone()),
            ..SyncCheckpointDelta::default()
        };

        for (keychain, index) in &current.last_active_indices {
            if previous.last_active_indices.get(keychain) != Some(index) {
                delta.last_active_upserts.insert(keychain.clone(), *index);
            }
        }
        for keychain in previous.last_active_indices.keys() {
            if !current.last_active_indices.contains_key(keychain) {
                delta.last_active_removals.push(keychain.clone());
            }
        }

        for (keychain, index) in &current.max_subscribed_indices {
            if previous.max_subscribed_indices.get(keychain) != Some(index) {
                delta
                    .max_subscribed_upserts
                    .insert(keychain.clone(), *index);
            }
        }
        for keychain in previous.max_subscribed_indices.keys() {
            if !current.max_subscribed_indices.contains_key(keychain) {
                delta.max_subscribed_removals.push(keychain.clone());
            }
        }

        for (hash, script) in &current.scripts {
            if previous.scripts.get(hash) != Some(script) {
                delta.script_upserts.insert(*hash, script.clone());
            }
        }
        for hash in previous.scripts.keys() {
            if !current.scripts.contains_key(hash) {
                delta.script_removals.push(*hash);
            }
        }

        delta
    }

    /// Apply a delta to this checkpoint in place.
    pub fn apply_delta(&mut self, delta: SyncCheckpointDelta<K>) {
        if let Some(chain_tip) = delta.chain_tip {
            self.chain_tip = chain_tip;
        }

        for keychain in delta.last_active_removals {
            self.last_active_indices.remove(&keychain);
        }
        for (keychain, index) in delta.last_active_upserts {
            self.last_active_indices.insert(keychain, index);
        }

        for keychain in delta.max_subscribed_removals {
            self.max_subscribed_indices.remove(&keychain);
        }
        for (keychain, index) in delta.max_subscribed_upserts {
            self.max_subscribed_indices.insert(keychain, index);
        }

        for hash in delta.script_removals {
            self.scripts.remove(&hash);
        }
        for (hash, script) in delta.script_upserts {
            self.scripts.insert(hash, script);
        }
    }
}

#[cfg(test)]
mod tests {
    use bdk_core::BlockId;
    use bdk_core::bitcoin::hashes::Hash;
    use bdk_core::bitcoin::{BlockHash, ScriptBuf, Txid};

    use super::*;

    fn hash(byte: u8) -> BlockHash {
        BlockHash::from_byte_array([byte; 32])
    }

    fn txid(byte: u8) -> Txid {
        Txid::from_byte_array([byte; 32])
    }

    fn script_hash(byte: u8) -> ElectrumScriptHash {
        ElectrumScriptHash::new(&ScriptBuf::from_bytes(vec![byte]))
    }

    #[test]
    fn checkpoint_diff_apply_roundtrip() {
        let keychain_external = "external".to_string();
        let keychain_internal = "internal".to_string();
        let script_a = script_hash(0x11);
        let script_b = script_hash(0x12);
        let script_removed = script_hash(0x13);

        let previous = SyncCheckpoint {
            chain_tip: Some(CheckPoint::new(BlockId {
                height: 0,
                hash: hash(0x01),
            })),
            last_active_indices: BTreeMap::from([(keychain_external.clone(), 1_u32)]),
            max_subscribed_indices: BTreeMap::from([(keychain_external.clone(), 2_u32)]),
            scripts: HashMap::from([
                (
                    script_a,
                    ScriptSyncCheckpoint {
                        keychain: keychain_external.clone(),
                        index: 2,
                        last_status: None,
                        expected_tx_heights: HashMap::from([(txid(0x21), 10_i64)]),
                    },
                ),
                (
                    script_removed,
                    ScriptSyncCheckpoint {
                        keychain: keychain_external.clone(),
                        index: 1,
                        last_status: None,
                        expected_tx_heights: HashMap::new(),
                    },
                ),
            ]),
        };

        let current = SyncCheckpoint {
            chain_tip: Some(CheckPoint::new(BlockId {
                height: 1,
                hash: hash(0x02),
            })),
            last_active_indices: BTreeMap::from([
                (keychain_external.clone(), 3_u32),
                (keychain_internal.clone(), 0_u32),
            ]),
            max_subscribed_indices: BTreeMap::from([
                (keychain_external.clone(), 4_u32),
                (keychain_internal.clone(), 1_u32),
            ]),
            scripts: HashMap::from([
                (
                    script_a,
                    ScriptSyncCheckpoint {
                        keychain: keychain_external.clone(),
                        index: 4,
                        last_status: None,
                        expected_tx_heights: HashMap::from([(txid(0x21), 15_i64)]),
                    },
                ),
                (
                    script_b,
                    ScriptSyncCheckpoint {
                        keychain: keychain_internal.clone(),
                        index: 1,
                        last_status: None,
                        expected_tx_heights: HashMap::from([(txid(0x22), -1_i64)]),
                    },
                ),
            ]),
        };

        let delta = SyncCheckpoint::diff(&previous, &current);
        assert!(!delta.is_empty());

        let mut reconstructed = previous;
        reconstructed.apply_delta(delta);
        assert_eq!(reconstructed, current);
    }

    #[test]
    fn checkpoint_diff_can_clear_chain_tip() {
        let previous = SyncCheckpoint::<String> {
            chain_tip: Some(CheckPoint::new(BlockId {
                height: 0,
                hash: hash(0x03),
            })),
            last_active_indices: BTreeMap::new(),
            max_subscribed_indices: BTreeMap::new(),
            scripts: HashMap::new(),
        };
        let current = SyncCheckpoint::<String> {
            chain_tip: None,
            last_active_indices: BTreeMap::new(),
            max_subscribed_indices: BTreeMap::new(),
            scripts: HashMap::new(),
        };

        let delta = SyncCheckpoint::diff(&previous, &current);
        assert_eq!(delta.chain_tip, Some(None));

        let mut reconstructed = previous;
        reconstructed.apply_delta(delta);
        assert_eq!(reconstructed, current);
    }
}

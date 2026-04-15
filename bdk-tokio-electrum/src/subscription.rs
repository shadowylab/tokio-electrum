use std::collections::{BTreeMap, HashMap, HashSet};

use bdk_core::CheckPoint;
use bdk_core::bitcoin::Txid;
use bdk_core::spk_client::FullScanRequest;
use tokio::sync::Mutex;
use tokio_electrum::prelude::{ElectrumScriptHash, ElectrumScriptStatus};

pub(crate) struct SubscriptionInit<K> {
    pub(crate) subscribed_scripts: HashSet<ElectrumScriptHash>,
    pub(crate) script_subscriptions: HashMap<ElectrumScriptHash, ScriptSubscription>,
    pub(crate) script_to_keychain_index: HashMap<ElectrumScriptHash, (K, u32)>,
    pub(crate) max_subscribed_indices: BTreeMap<K, u32>,
}

/// Information about a subscribed script
#[derive(Debug, Clone)]
pub(crate) struct ScriptSubscription {
    /// The expected transaction status keyed by txid.
    ///
    /// Value is the Electrum height encoding (`>0` confirmed, `0/-1` mempool states).
    pub(crate) expected_tx_heights: HashMap<Txid, i64>,
    /// Last known Electrum status hash for this script.
    pub(crate) last_status: Option<ElectrumScriptStatus>,
}

impl ScriptSubscription {
    #[inline]
    pub(crate) fn new(expected_tx_heights: HashMap<Txid, i64>) -> Self {
        Self {
            expected_tx_heights,
            last_status: None,
        }
    }

    #[inline]
    pub(crate) fn with_status(
        expected_tx_heights: HashMap<Txid, i64>,
        last_status: Option<ElectrumScriptStatus>,
    ) -> Self {
        Self {
            expected_tx_heights,
            last_status,
        }
    }
}

#[derive(Default)]
pub(crate) struct SubscriptionState<K> {
    pub(crate) subscribed_scripts: HashSet<ElectrumScriptHash>,
    pub(crate) script_subscriptions: HashMap<ElectrumScriptHash, ScriptSubscription>,
    pub(crate) script_to_keychain_index: HashMap<ElectrumScriptHash, (K, u32)>,
    pub(crate) last_active_indices: BTreeMap<K, u32>,
    pub(crate) max_subscribed_indices: BTreeMap<K, u32>,
}

pub(crate) struct SubscriptionCtx<K> {
    pub(crate) wallet_label: String,
    pub(crate) request: Mutex<FullScanRequest<K>>,
    pub(crate) state: Mutex<SubscriptionState<K>>,
    pub(crate) chain_tip: Mutex<Option<CheckPoint>>,
}

impl<K> SubscriptionCtx<K>
where
    K: Ord + Clone,
{
    pub(crate) async fn has_script(&self, hash: &ElectrumScriptHash) -> bool {
        let state = self.state.lock().await;
        state.subscribed_scripts.contains(hash)
    }

    #[cfg(test)]
    pub(crate) async fn keychain_index(&self, hash: &ElectrumScriptHash) -> Option<(K, u32)> {
        let state = self.state.lock().await;
        state.script_to_keychain_index.get(hash).cloned()
    }

    pub(crate) async fn extension_target(&self, hash: &ElectrumScriptHash) -> Option<(K, u32)> {
        let state = self.state.lock().await;

        let (keychain, current_index): (K, u32) = state.script_to_keychain_index.get(hash)?.clone();

        // Extend only when this script index moved beyond the known last-active index.
        let last: u32 = state.last_active_indices.get(&keychain).copied()?;

        if current_index > last {
            Some((keychain, current_index))
        } else {
            None
        }
    }

    pub(crate) async fn tracked_script_hashes(&self) -> Vec<ElectrumScriptHash> {
        let state = self.state.lock().await;
        state.subscribed_scripts.iter().copied().collect()
    }

    pub(crate) async fn script_hashes_with_unconfirmed_txs(&self) -> Vec<ElectrumScriptHash> {
        let state = self.state.lock().await;
        state
            .script_subscriptions
            .iter()
            .filter_map(|(hash, sub)| {
                sub.expected_tx_heights
                    .values()
                    .any(|height| *height <= 0)
                    .then_some(*hash)
            })
            .collect()
    }

    pub(crate) async fn script_hashes_with_status_changes(
        &self,
        script_statuses: &HashMap<ElectrumScriptHash, Option<ElectrumScriptStatus>>,
    ) -> Vec<ElectrumScriptHash> {
        let state = self.state.lock().await;
        script_statuses
            .iter()
            .filter_map(|(hash, next_status)| {
                state
                    .script_subscriptions
                    .get(hash)
                    .is_some_and(|subscription| subscription.last_status != *next_status)
                    .then_some(*hash)
            })
            .collect()
    }

    pub(crate) async fn bump_last_active(&self, keychain: K, new_active_index: u32) {
        let mut state = self.state.lock().await;
        state
            .last_active_indices
            .entry(keychain)
            .and_modify(|last| *last = (*last).max(new_active_index))
            .or_insert(new_active_index);
    }

    pub(crate) async fn apply_incremental_subscription(
        &self,
        keychain: K,
        new_active_index: u32,
        new_max_subscribed: u32,
        script_hashes: Vec<ElectrumScriptHash>,
        scripts_to_subscribe: &[(ElectrumScriptHash, u32)],
    ) {
        let mut state = self.state.lock().await;
        state
            .last_active_indices
            .entry(keychain.clone())
            .and_modify(|last| *last = (*last).max(new_active_index))
            .or_insert(new_active_index);
        for (hash, index) in scripts_to_subscribe {
            state
                .script_subscriptions
                .entry(*hash)
                .or_insert_with(|| ScriptSubscription::new(HashMap::new()));
            state
                .script_to_keychain_index
                .insert(*hash, (keychain.clone(), *index));
        }
        state.subscribed_scripts.extend(script_hashes);
        state
            .max_subscribed_indices
            .insert(keychain, new_max_subscribed);
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use bdk_core::bitcoin::ScriptBuf;

    use super::*;

    fn script(byte: u8) -> ScriptBuf {
        ScriptBuf::from_bytes(vec![byte])
    }

    fn status(hex: &str) -> ElectrumScriptStatus {
        ElectrumScriptStatus::from_str(hex).unwrap()
    }

    fn test_ctx() -> SubscriptionCtx<String> {
        SubscriptionCtx {
            wallet_label: String::from("test-wallet"),
            request: Mutex::new(FullScanRequest::<String>::builder_at(0).build()),
            state: Mutex::new(SubscriptionState::default()),
            chain_tip: Mutex::new(None),
        }
    }

    #[tokio::test]
    async fn script_hashes_with_status_changes_returns_only_tracked_diffs() {
        let ctx = test_ctx();
        let unchanged = ElectrumScriptHash::new(&script(0x11));
        let changed = ElectrumScriptHash::new(&script(0x12));
        let none_to_some = ElectrumScriptHash::new(&script(0x13));
        let untracked = ElectrumScriptHash::new(&script(0x14));
        let status_a = status("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let status_b = status("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

        {
            let mut state = ctx.state.lock().await;
            state.script_subscriptions.insert(
                unchanged,
                ScriptSubscription::with_status(HashMap::new(), Some(status_a)),
            );
            state.script_subscriptions.insert(
                changed,
                ScriptSubscription::with_status(HashMap::new(), Some(status_a)),
            );
            state
                .script_subscriptions
                .insert(none_to_some, ScriptSubscription::new(HashMap::new()));
        }

        let mut incoming = HashMap::new();
        incoming.insert(unchanged, Some(status_a));
        incoming.insert(changed, Some(status_b));
        incoming.insert(none_to_some, Some(status_a));
        incoming.insert(untracked, Some(status_b));

        let changed_hashes: HashSet<ElectrumScriptHash> = ctx
            .script_hashes_with_status_changes(&incoming)
            .await
            .into_iter()
            .collect();

        assert_eq!(
            changed_hashes,
            HashSet::from([changed, none_to_some]),
            "only tracked script hashes with status deltas should be processed"
        );
    }
}

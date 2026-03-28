use std::collections::{BTreeMap, HashMap, HashSet};

use tokio_electrum::prelude::ElectrumScriptHash;

use crate::client::SpkScanResult;
use crate::subscription::{ScriptSubscription, SubscriptionInit};

#[derive(Default)]
pub(crate) struct FullScanAccumulator<K> {
    last_active_indices: BTreeMap<K, u32>,
    subscribed_scripts: HashSet<ElectrumScriptHash>,
    script_subscriptions: HashMap<ElectrumScriptHash, ScriptSubscription>,
    script_to_keychain_index: HashMap<ElectrumScriptHash, (K, u32)>,
    max_subscribed_indices: BTreeMap<K, u32>,
}

impl<K> FullScanAccumulator<K>
where
    K: Ord + Clone,
{
    pub(crate) fn new() -> Self {
        Self {
            last_active_indices: BTreeMap::new(),
            subscribed_scripts: HashSet::new(),
            script_subscriptions: HashMap::new(),
            script_to_keychain_index: HashMap::new(),
            max_subscribed_indices: BTreeMap::new(),
        }
    }

    pub(crate) fn absorb_keychain_scan(&mut self, keychain: K, spk_scan: SpkScanResult) {
        if let Some(last_active_index) = spk_scan.last_active_index {
            self.last_active_indices
                .insert(keychain.clone(), last_active_index);
        }

        self.subscribed_scripts.extend(spk_scan.subscribed_hashes);
        self.script_subscriptions
            .extend(spk_scan.subscribed_script_subscriptions);

        for (hash, index) in spk_scan.subscribed_script_to_index {
            self.script_to_keychain_index
                .insert(hash, (keychain.clone(), index));
        }
        if let Some(max_subscribed_index) = spk_scan.max_subscribed_index {
            self.max_subscribed_indices
                .insert(keychain, max_subscribed_index);
        }
    }

    pub(crate) fn into_subscription_init(self) -> (BTreeMap<K, u32>, SubscriptionInit<K>) {
        (
            self.last_active_indices,
            SubscriptionInit {
                subscribed_scripts: self.subscribed_scripts,
                script_subscriptions: self.script_subscriptions,
                script_to_keychain_index: self.script_to_keychain_index,
                max_subscribed_indices: self.max_subscribed_indices,
            },
        )
    }
}

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::{Debug, Display};
use std::ops::Deref;
use std::sync::Arc;

use bdk_core::bitcoin::block::Header;
use bdk_core::bitcoin::{BlockHash, Transaction, Txid};
use bdk_core::spk_client::{FullScanRequest, FullScanResponse, SpkWithExpectedTxids, SyncResponse};
use bdk_core::{BlockId, CheckPoint, ConfirmationBlockTime, TxUpdate};
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use tokio::sync::{Mutex, RwLock, broadcast};
pub use tokio_electrum::client::{ElectrumClient, Error};
use tokio_electrum::notification::ElectrumNotification;
use tokio_electrum::types::{BlockHeader, ElectrumScriptHash};

use crate::util;

/// We include a chain suffix of a certain length for the purpose of robustness.
const CHAIN_SUFFIX_LENGTH: u32 = 8;

/// Information about a subscribed script
#[derive(Debug, Clone)]
struct ScriptSubscription {
    /// The set of transaction IDs we've seen for this script
    expected_txids: HashSet<Txid>,
}

impl ScriptSubscription {
    #[inline]
    fn new(expected_txids: HashSet<Txid>) -> Self {
        Self { expected_txids }
    }
}

/// Tracks script subscriptions with their expected txids for diffing updates
#[derive(Debug, Clone, Default)]
struct SubscriptionTracker {
    /// Maps script hash to subscription information
    scripts: Arc<Mutex<HashMap<ElectrumScriptHash, ScriptSubscription>>>,
}

struct SubscriptionCtx<K> {
    request: Mutex<FullScanRequest<K>>,
    subscribed_scripts: Mutex<HashSet<ElectrumScriptHash>>,
    script_to_keychain_index: Mutex<HashMap<ElectrumScriptHash, (K, u32)>>,
    last_active_indices: Mutex<BTreeMap<K, u32>>,
    chain_tip: Mutex<Option<CheckPoint>>,
}

impl<K> SubscriptionCtx<K>
where
    K: Ord + Clone,
{
    async fn has_script(&self, hash: &ElectrumScriptHash) -> bool {
        let scripts_guard = self.subscribed_scripts.lock().await;
        scripts_guard.contains(hash)
    }

    async fn keychain_index(&self, hash: &ElectrumScriptHash) -> Option<(K, u32)> {
        let lookup = self.script_to_keychain_index.lock().await;
        lookup.get(hash).cloned()
    }

    async fn needs_extension(&self, hash: &ElectrumScriptHash) -> Option<(K, u32)> {
        let (keychain, current_index) = self.keychain_index(hash).await?;

        // Check if we need to extend subscriptions for this keychain
        let last_active = self.last_active_indices.lock().await;
        let last: u32 = last_active.get(&keychain).copied().unwrap_or(0);

        if current_index > last {
            Some((keychain, current_index))
        } else {
            None
        }
    }
}

/// A stream that yields real-time updates from Electrum subscriptions.
pub type SubscriptionStream =
    BoxStream<'static, Result<SyncResponse<ConfirmationBlockTime>, Error>>;

/// Event yielded by [`BdkElectrumClient::subscribe`].
#[derive(Debug)]
pub enum SubscribeEvent<K> {
    /// Initial full scan response emitted once at stream start.
    Initial(FullScanResponse<K>),
    /// Incremental update emitted for script hash and header notifications.
    Update(SyncResponse<ConfirmationBlockTime>),
    /// Connection has been disconnected and the stream will terminate.
    Disconnected,
}

/// A stream that yields initial full-scan data and then real-time updates.
pub type SubscribeStream<K> = BoxStream<'static, Result<SubscribeEvent<K>, Error>>;

/// Wrapper around [`ElectrumClient`] which includes an internal in-memory
/// transaction cache to avoid re-fetching already downloaded transactions.
#[derive(Debug, Clone)]
pub struct BdkElectrumClient {
    /// The underlying electrum client.
    inner: ElectrumClient,
    /// The transaction cache
    tx_cache: Arc<RwLock<HashMap<Txid, Arc<Transaction>>>>,
    /// The header cache
    block_header_cache: Arc<Mutex<HashMap<u32, Header>>>,
    /// Subscription tracker for real-time sync
    subscription_tracker: SubscriptionTracker,
}

impl Deref for BdkElectrumClient {
    type Target = ElectrumClient;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl BdkElectrumClient {
    /// Creates a new bdk client from a [`electrum_client::ElectrumApi`]
    pub fn new(client: ElectrumClient) -> Self {
        Self {
            inner: client,
            tx_cache: Default::default(),
            block_header_cache: Default::default(),
            subscription_tracker: Default::default(),
        }
    }

    /// Inserts transactions into the transaction cache so that the client will not fetch these
    /// transactions.
    pub async fn populate_tx_cache<I, T>(&self, txs: I)
    where
        I: IntoIterator<Item = T>,
        T: Into<Arc<Transaction>>,
    {
        let mut tx_cache = self.tx_cache.write().await;

        for tx in txs.into_iter() {
            let tx: Arc<Transaction> = tx.into();
            let txid: Txid = tx.compute_txid();
            tx_cache.insert(txid, tx);
        }
    }

    /// Fetch transaction of given `txid`.
    ///
    /// If it hits the cache it will return the cached version and avoid making the request.
    async fn fetch_tx(&self, txid: Txid) -> Result<Arc<Transaction>, Error> {
        {
            let tx_cache = self.tx_cache.read().await;

            if let Some(tx) = tx_cache.get(&txid) {
                return Ok(Arc::clone(tx));
            }
        }

        let tx: Transaction = self.inner.transaction_get(txid).await?;
        let tx: Arc<Transaction> = Arc::new(tx);

        let mut tx_cache = self.tx_cache.write().await;
        tx_cache.insert(txid, tx.clone());

        Ok(tx)
    }

    /// Fetch block header of given `height`.
    ///
    /// If it hits the cache it will return the cached version and avoid making the request.
    async fn fetch_header(&self, height: u32) -> Result<Header, Error> {
        let block_header_cache = self.block_header_cache.lock().await;

        if let Some(header) = block_header_cache.get(&height) {
            return Ok(*header);
        }

        drop(block_header_cache);

        self.update_header(height).await
    }

    /// Update a block header at given `height`. Returns the updated header.
    async fn update_header(&self, height: u32) -> Result<Header, Error> {
        let header = self.inner.block_header(height).await?;

        let mut block_header_cache = self.block_header_cache.lock().await;
        block_header_cache.insert(height, header);

        Ok(header)
    }

    /// Internal implementation of full scan that optionally subscribes to scripts.
    ///
    /// When `subscribe` is true, it subscribes to all scripts with history and returns their hashes.
    async fn internal_full_scan<K>(
        &self,
        request: &mut FullScanRequest<K>,
        stop_gap: usize,
        batch_size: usize,
        fetch_prev_txouts: bool,
    ) -> Result<(FullScanResponse<K>, HashSet<ElectrumScriptHash>), Error>
    where
        K: Ord + Clone + Display,
    {
        let start_time = request.start_time();

        let tip_and_latest_blocks = match request.chain_tip() {
            Some(chain_tip) => Some(fetch_tip_and_latest_blocks(&self.inner, chain_tip).await?),
            None => None,
        };

        let mut tx_update: TxUpdate<ConfirmationBlockTime> = TxUpdate::default();
        let mut last_active_indices: BTreeMap<K, u32> = BTreeMap::default();
        let mut all_subscribed_scripts: HashSet<ElectrumScriptHash> = HashSet::new();

        for keychain in request.keychains() {
            let spks = request
                .iter_spks(keychain.clone())
                .map(|(spk_i, spk)| (spk_i, SpkWithExpectedTxids::from(spk)));

            let (last_active_index, subscribed_hashes) = self
                .populate_with_spks(
                    start_time,
                    &mut tx_update,
                    spks,
                    stop_gap,
                    batch_size,
                    &keychain,
                )
                .await?;

            if let Some(last_active_index) = last_active_index {
                last_active_indices.insert(keychain, last_active_index);
            }

            // Collect all subscribed script hashes
            all_subscribed_scripts.extend(subscribed_hashes);
        }

        // Fetch previous `TxOut`s for fee calculation if flag is enabled.
        if fetch_prev_txouts {
            self.fetch_prev_txout(&mut tx_update).await?;
        }

        let chain_update = match tip_and_latest_blocks {
            Some((chain_tip, latest_blocks)) => Some(chain_update(
                chain_tip,
                &latest_blocks,
                tx_update.anchors.iter().cloned(),
            )?),
            _ => None,
        };

        let response = FullScanResponse {
            tx_update,
            chain_update,
            last_active_indices,
        };

        tracing::info!("Finished loading.");

        Ok((response, all_subscribed_scripts))
    }

    /// Subscribe using a full scan request.
    ///
    /// The returned stream first yields [`SubscribeEvent::Initial`] with the complete
    /// [`FullScanResponse`], then yields [`SubscribeEvent::Update`] values for real-time changes.
    /// On disconnection, it yields [`SubscribeEvent::Disconnected`] and then terminates.
    pub async fn sync<K, R>(
        &self,
        request: R,
        stop_gap: usize,
        batch_size: usize,
        fetch_prev_txouts: bool,
    ) -> Result<SubscribeStream<K>, Error>
    where
        R: Into<FullScanRequest<K>>,
        K: Ord + Clone + Display + Send + 'static,
    {
        let mut request: FullScanRequest<K> = request.into();

        let start_time: u64 = request.start_time();
        let notification_rx = self.inner.notifications();

        let (response, all_subscribed_scripts) = self
            .internal_full_scan(&mut request, stop_gap, batch_size, fetch_prev_txouts)
            .await?;

        let live_stream = self.create_live_subscription_stream(
            start_time,
            all_subscribed_scripts,
            fetch_prev_txouts,
            request,
            &response,
            stop_gap,
            notification_rx,
        );

        let initial_event = stream::once(async move { Ok(SubscribeEvent::Initial(response)) });

        Ok(Box::pin(initial_event.chain(live_stream)))
    }

    /// Creates a stream that processes script hash and block header notifications.
    #[allow(clippy::too_many_arguments)]
    fn create_live_subscription_stream<K>(
        &self,
        start_time: u64,
        subscribed_scripts: HashSet<ElectrumScriptHash>,
        fetch_prev_txouts: bool,
        mut request: FullScanRequest<K>,
        response: &FullScanResponse<K>,
        stop_gap: usize,
        notification_rx: broadcast::Receiver<ElectrumNotification>,
    ) -> SubscribeStream<K>
    where
        K: Ord + Clone + Display + Send + 'static,
    {
        let client = self.clone();

        // Build a reverse lookup map
        let script_to_keychain_index = build_reverse_lookup_map(&mut request, response, stop_gap);

        let ctx = Arc::new(SubscriptionCtx {
            request: Mutex::new(request),
            subscribed_scripts: Mutex::new(subscribed_scripts),
            script_to_keychain_index: Mutex::new(script_to_keychain_index),
            last_active_indices: Mutex::new(response.last_active_indices.clone()),
            chain_tip: Mutex::new(response.chain_update.clone()),
        });

        let stream = stream::unfold(
            (client, ctx, notification_rx, false),
            move |(client, ctx, mut notification_rx, done)| async move {
                if done {
                    return None;
                }

                loop {
                    match notification_rx.recv().await {
                        Ok(ElectrumNotification::ScriptHash { hash, .. }) => {
                            if let Some(update) = client
                                .handle_script_hash_notification(
                                    hash,
                                    start_time,
                                    fetch_prev_txouts,
                                    stop_gap,
                                    &ctx,
                                )
                                .await
                            {
                                return Some((
                                    update.map(SubscribeEvent::Update),
                                    (client, ctx, notification_rx, false),
                                ));
                            }
                        }
                        Ok(ElectrumNotification::BlockHeader { height, header }) => {
                            if let Some(update) =
                                handle_block_header_notification(height, header, &ctx).await
                            {
                                return Some((
                                    update.map(SubscribeEvent::Update),
                                    (client, ctx, notification_rx, false),
                                ));
                            }
                        }
                        Ok(ElectrumNotification::ConnectionStatusChanged(status))
                            if !status.is_connected() =>
                        {
                            // Terminate if the client is no longer connected
                            return Some((
                                Ok(SubscribeEvent::Disconnected),
                                (client, ctx, notification_rx, true),
                            ));
                        }
                        Ok(_) => {}
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!(
                                "Subscription stream lagged behind by {} messages - some updates may have been missed",
                                n
                            );
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            return Some((
                                Ok(SubscribeEvent::Disconnected),
                                (client, ctx, notification_rx, true),
                            ));
                        }
                    }
                }
            },
        );

        Box::pin(stream)
    }

    /// Handle script hash notification and check if we need to subscribe to new addresses
    async fn handle_script_hash_notification<K>(
        &self,
        hash: ElectrumScriptHash,
        start_time: u64,
        fetch_prev_txouts: bool,
        stop_gap: usize,
        ctx: &SubscriptionCtx<K>,
    ) -> Option<Result<SyncResponse<ConfirmationBlockTime>, Error>>
    where
        K: Ord + Clone + Display,
    {
        // Check if this script hash belongs to this wallet
        let has_script: bool = ctx.has_script(&hash).await;

        if !has_script {
            return None;
        }

        if let Some((keychain, current_index)) = ctx.needs_extension(&hash).await {
            // Subscribe to new addresses incrementally
            if let Err(e) = self
                .subscribe_incremental(keychain, current_index, stop_gap, ctx)
                .await
            {
                tracing::error!("Failed to subscribe incrementally: {}", e);
            }
        }

        // Process this script hash update
        match self
            .process_script_hash_update(hash, start_time, fetch_prev_txouts)
            .await
        {
            Ok(Some(update)) => Some(Ok(update)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }

    /// Subscribe incrementally to new addresses when a transaction is detected on a higher index.
    ///
    /// This maintains the stop_gap invariant by subscribing to addresses from the new
    /// last_active_index up to last_active_index + stop_gap.
    async fn subscribe_incremental<K>(
        &self,
        keychain: K,
        new_active_index: u32,
        stop_gap: usize,
        ctx: &SubscriptionCtx<K>,
    ) -> Result<(), Error>
    where
        K: Ord + Clone + Display,
    {
        // Derive scripts to subscribe from new_active_index + 1 to new_active_index + stop_gap
        let scripts_to_subscribe: Vec<(ElectrumScriptHash, ScriptSubscription, u32)> = {
            let mut request_guard = ctx.request.lock().await;
            request_guard
                .iter_spks(keychain.clone())
                .skip((new_active_index + 1) as usize)
                .take(stop_gap)
                .map(|(index, script)| {
                    let script_hash = ElectrumScriptHash::new(&script);
                    let subscription = ScriptSubscription::new(HashSet::new());
                    (script_hash, subscription, index)
                })
                .collect()
        };

        if scripts_to_subscribe.is_empty() {
            return Ok(());
        }

        if let (Some((_, _, start_index)), Some((_, _, end_index))) =
            (scripts_to_subscribe.first(), scripts_to_subscribe.last())
        {
            util::log_scan_range(&keychain, *start_index, *end_index);
        }

        // Collect script hashes for subscription
        let script_hashes: Vec<ElectrumScriptHash> = scripts_to_subscribe
            .iter()
            .map(|(hash, _, _)| *hash)
            .collect();

        // Subscribe via Electrum first (this can fail)
        self.inner
            .batch_script_hash_subscribe(script_hashes.iter().copied())?;

        // Only update state if subscription succeeded
        // Update last_active_index
        {
            let mut last_active = ctx.last_active_indices.lock().await;
            last_active.insert(keychain.clone(), new_active_index);
        }

        // Update tracker
        {
            let mut tracker = self.subscription_tracker.scripts.lock().await;
            for (hash, subscription, _) in &scripts_to_subscribe {
                tracker.insert(*hash, subscription.clone());
            }
        }

        // Update the stream's subscribed scripts and lookup in a single batch
        {
            let mut scripts = ctx.subscribed_scripts.lock().await;
            let mut lookup = ctx.script_to_keychain_index.lock().await;

            scripts.extend(script_hashes);
            for (hash, _, index) in &scripts_to_subscribe {
                lookup.insert(*hash, (keychain.clone(), *index));
            }
        }

        tracing::info!("Finished loading.");

        Ok(())
    }

    /// Process a script hash notification and return a TxUpdate if there are changes
    async fn process_script_hash_update(
        &self,
        script_hash: ElectrumScriptHash,
        start_time: u64,
        fetch_prev_txouts: bool,
    ) -> Result<Option<SyncResponse<ConfirmationBlockTime>>, Error> {
        // Look up the script from our tracker
        let tracker = self.subscription_tracker.scripts.lock().await;
        let Some(subscription) = tracker.get(&script_hash).cloned() else {
            // Not tracking this script, ignore
            return Ok(None);
        };
        drop(tracker);

        let expected_txids: HashSet<Txid> = subscription.expected_txids;

        // Fetch current history for this script
        let history = self.inner.script_get_history(script_hash).await?;
        let current_txids: HashSet<Txid> = history.iter().map(|tx| tx.txid()).collect();

        // Check if there are any changes
        if current_txids == expected_txids {
            return Ok(None);
        }

        // Build a TxUpdate with the changes
        let mut tx_update: TxUpdate<ConfirmationBlockTime> = TxUpdate::default();

        // Mark evicted transactions
        for evicted_txid in expected_txids.difference(&current_txids) {
            tx_update.evicted_ats.insert((*evicted_txid, start_time));
        }

        // Fetch new transactions
        for tx_res in history {
            if !expected_txids.contains(&tx_res.txid()) {
                let tx: Arc<Transaction> = self.fetch_tx(tx_res.txid()).await?;
                tx_update.txs.push(tx);

                match tx_res.electrum_height().try_into() {
                    Ok(height) if height > 0 => {
                        self.validate_merkle_for_anchor(&mut tx_update, tx_res.txid(), height)
                            .await?;
                    }
                    _ => {
                        tx_update.seen_ats.insert((tx_res.txid(), start_time));
                    }
                }
            }
        }

        // Fetch previous `TxOut`s for fee calculation if flag is enabled.
        if fetch_prev_txouts {
            self.fetch_prev_txout(&mut tx_update).await?;
        }

        // Update our tracker with the new expected txids
        let mut tracker = self.subscription_tracker.scripts.lock().await;
        if let Some(subscription) = tracker.get_mut(&script_hash) {
            subscription.expected_txids = current_txids;
        }

        Ok(Some(SyncResponse {
            tx_update,
            chain_update: None,
        }))
    }

    /// Populate the `tx_update` with transactions/anchors associated with the given `spks`.
    ///
    /// Transactions that contains an output with requested spk, or spends form an output with
    /// requested spk will be added to `tx_update`. Anchors of the aforementioned transactions are
    /// also included.
    ///
    /// If `subscribe` is true, also subscribes to scripts with history for real-time updates
    /// and returns the set of subscribed script hashes.
    #[allow(clippy::too_many_arguments)]
    async fn populate_with_spks<K>(
        &self,
        start_time: u64,
        tx_update: &mut TxUpdate<ConfirmationBlockTime>,
        mut spks_with_expected_txids: impl Iterator<Item = (u32, SpkWithExpectedTxids)>,
        stop_gap: usize,
        batch_size: usize,
        keychain: &K,
    ) -> Result<(Option<u32>, HashSet<ElectrumScriptHash>), Error>
    where
        K: Display,
    {
        let mut unused_spk_count = 0_usize;
        let mut last_active_index = Option::<u32>::None;
        let mut subscribed_hashes = HashSet::new();

        loop {
            let spks = (0..batch_size)
                .map_while(|_| spks_with_expected_txids.next())
                .collect::<Vec<_>>();

            if spks.is_empty() {
                return Ok((last_active_index, subscribed_hashes));
            }

            if let (Some((start_index, _)), Some((end_index, _))) = (spks.first(), spks.last()) {
                util::log_scan_range(&keychain, *start_index, *end_index);
            }

            let spk_histories = self
                .inner
                .batch_script_get_history(spks.iter().map(|(_, s)| s.spk.as_script()))
                .await?;

            // Collect scripts to subscribe (if subscribe=true)
            let mut scripts_to_subscribe: Vec<(ElectrumScriptHash, ScriptSubscription)> =
                Vec::new();

            for ((spk_index, spk), spk_history) in spks.into_iter().zip(spk_histories) {
                if spk_history.is_empty() {
                    unused_spk_count = unused_spk_count.saturating_add(1);
                    if unused_spk_count >= stop_gap {
                        // Subscribe to collected scripts before returning
                        if !scripts_to_subscribe.is_empty() {
                            self.subscribe_scripts(&scripts_to_subscribe, &mut subscribed_hashes)
                                .await;
                        }
                        return Ok((last_active_index, subscribed_hashes));
                    }
                } else {
                    last_active_index = Some(spk_index);
                    unused_spk_count = 0;

                    // Collect for subscription
                    let script_hash = ElectrumScriptHash::new(&spk.spk);
                    let spk_history_set: HashSet<Txid> =
                        spk_history.iter().map(|res| res.txid()).collect();
                    let subscription = ScriptSubscription::new(spk_history_set.clone());
                    scripts_to_subscribe.push((script_hash, subscription));
                }

                let spk_history_set = spk_history
                    .iter()
                    .map(|res| res.txid())
                    .collect::<HashSet<_>>();

                tx_update.evicted_ats.extend(
                    spk.expected_txids
                        .difference(&spk_history_set)
                        .map(|&txid| (txid, start_time)),
                );

                for tx_res in spk_history {
                    let tx = self.fetch_tx(tx_res.txid()).await?;
                    tx_update.txs.push(tx);
                    match tx_res.electrum_height().try_into() {
                        // Returned heights 0 & -1 are reserved for unconfirmed txs.
                        Ok(height) if height > 0 => {
                            self.validate_merkle_for_anchor(tx_update, tx_res.txid(), height)
                                .await?;
                        }
                        _ => {
                            tx_update.seen_ats.insert((tx_res.txid(), start_time));
                        }
                    }
                }
            }

            // Subscribe to all scripts with history in this batch
            if !scripts_to_subscribe.is_empty() {
                self.subscribe_scripts(&scripts_to_subscribe, &mut subscribed_hashes)
                    .await;
            }
        }
    }

    /// Subscribe to a batch of scripts via Electrum and update the subscription tracker.
    ///
    /// This method updates the internal subscription tracker with the provided scripts,
    /// adds their hashes to the provided set, and then subscribes to them via Electrum.
    /// If the Electrum subscription fails, it logs an error but doesn't propagate it.
    async fn subscribe_scripts(
        &self,
        scripts: &[(ElectrumScriptHash, ScriptSubscription)],
        subscribed_hashes: &mut HashSet<ElectrumScriptHash>,
    ) {
        // Update subscription tracker
        {
            let mut tracker = self.subscription_tracker.scripts.lock().await;
            for (hash, subscription) in scripts {
                tracker.insert(*hash, subscription.clone());
                subscribed_hashes.insert(*hash);
            }
        }

        // Subscribe via Electrum
        if let Err(e) = self
            .inner
            .batch_script_hash_subscribe(scripts.iter().map(|(hash, _)| *hash))
        {
            tracing::error!("Failed to subscribe scripts: {}", e);
        }
    }

    /// Validate a transaction's merkle proof and add an anchor if confirmed.
    ///
    /// This method fetches the merkle proof for the transaction, validates it against
    /// the block header's merkle root, and adds a confirmation anchor to the tx_update
    /// if the transaction is confirmed. If the cached header is outdated, it will fetch
    /// a fresh header and retry validation.
    async fn validate_merkle_for_anchor(
        &self,
        tx_update: &mut TxUpdate<ConfirmationBlockTime>,
        txid: Txid,
        confirmation_height: u32,
    ) -> Result<(), Error> {
        if let Ok(merkle_res) = self
            .inner
            .transaction_get_merkle(txid, confirmation_height)
            .await
        {
            let mut header = self
                .fetch_header(merkle_res.block_height.to_consensus_u32())
                .await?;
            let mut is_confirmed_tx =
                util::validate_merkle_proof(&txid, &header.merkle_root, &merkle_res);

            // Merkle validation will fail if the header in `block_header_cache` is outdated, so we
            // want to check if there is a new header and validate against the new one.
            if !is_confirmed_tx {
                header = self
                    .update_header(merkle_res.block_height.to_consensus_u32())
                    .await?;
                is_confirmed_tx =
                    util::validate_merkle_proof(&txid, &header.merkle_root, &merkle_res);
            }

            if is_confirmed_tx {
                tx_update.anchors.insert((
                    ConfirmationBlockTime {
                        confirmation_time: header.time as u64,
                        block_id: BlockId {
                            height: merkle_res.block_height.to_consensus_u32(),
                            hash: header.block_hash(),
                        },
                    },
                    txid,
                ));
            }
        }
        Ok(())
    }

    /// Fetch previous transaction outputs for fee calculation.
    ///
    /// This method fetches the `TxOut`s of the previous transactions for all inputs
    /// of relevant transactions in the tx_update. This data is needed to calculate
    /// transaction fees. Coinbase transactions are skipped, and duplicate fetches
    /// are avoided using a deduplication set.
    async fn fetch_prev_txout(
        &self,
        tx_update: &mut TxUpdate<ConfirmationBlockTime>,
    ) -> Result<(), Error> {
        let mut no_dup = HashSet::<Txid>::new();
        for tx in &tx_update.txs {
            if !tx.is_coinbase() && no_dup.insert(tx.compute_txid()) {
                for vin in &tx.input {
                    let outpoint = vin.previous_output;
                    let vout = outpoint.vout;
                    let prev_tx = self.fetch_tx(outpoint.txid).await?;
                    let txout = prev_tx.output[vout as usize].clone();
                    let _ = tx_update.txouts.insert(outpoint, txout);
                }
            }
        }
        Ok(())
    }
}

/// Return a [`CheckPoint`] of the latest tip, that connects with `prev_tip`. The latest blocks are
/// fetched to construct checkpoint updates with the proper [`BlockHash`] in case of re-org.
async fn fetch_tip_and_latest_blocks(
    client: &ElectrumClient,
    prev_tip: CheckPoint,
) -> Result<(CheckPoint, BTreeMap<u32, BlockHash>), Error> {
    let BlockHeader {
        height: new_tip_height,
        ..
    } = client.get_tip().await?;

    // If electrum returns a tip height that is lower than our previous tip, then checkpoints do
    // not need updating. We just return the previous tip and use that as the point of agreement.
    if new_tip_height < prev_tip.height() {
        return Ok((prev_tip, BTreeMap::new()));
    }

    // Atomically fetch the latest `CHAIN_SUFFIX_LENGTH` count of blocks from Electrum. We use this
    // to construct our checkpoint update.
    let mut new_blocks = {
        let start_height = new_tip_height.saturating_sub(CHAIN_SUFFIX_LENGTH - 1);
        let hashes = client
            .block_headers(start_height as _, CHAIN_SUFFIX_LENGTH as _)
            .await?
            .headers
            .into_iter()
            .map(|h| h.block_hash());
        (start_height..).zip(hashes).collect::<BTreeMap<u32, _>>()
    };

    // Find the "point of agreement" (if any).
    let agreement_cp = {
        let mut agreement_cp = Option::<CheckPoint>::None;
        for cp in prev_tip.iter() {
            let cp_block = cp.block_id();
            let hash = match new_blocks.get(&cp_block.height) {
                Some(&hash) => hash,
                None => {
                    assert!(
                        new_tip_height >= cp_block.height,
                        "already checked that electrum's tip cannot be smaller"
                    );
                    let hash = client
                        .block_header(cp_block.height as _)
                        .await?
                        .block_hash();
                    new_blocks.insert(cp_block.height, hash);
                    hash
                }
            };
            if hash == cp_block.hash {
                agreement_cp = Some(cp);
                break;
            }
        }
        agreement_cp
    };

    let agreement_height = agreement_cp.as_ref().map(CheckPoint::height);

    let new_tip = new_blocks
        .iter()
        // Prune `new_blocks` to only include blocks that are actually new.
        .filter(|(height, _)| Some(**height) > agreement_height)
        .map(|(height, hash)| BlockId {
            height: *height,
            hash: *hash,
        })
        .fold(agreement_cp, |prev_cp, block| {
            Some(match prev_cp {
                Some(cp) => cp.push(block).expect("must extend checkpoint"),
                None => CheckPoint::new(block),
            })
        })
        .expect("must have at least one checkpoint");

    Ok((new_tip, new_blocks))
}

/// Handle block header notification and update the chain tip
async fn handle_block_header_notification<K>(
    height: u32,
    header: Header,
    ctx: &SubscriptionCtx<K>,
) -> Option<Result<SyncResponse<ConfirmationBlockTime>, Error>>
where
    K: Ord + Clone,
{
    let mut tip_guard = ctx.chain_tip.lock().await;

    match tip_guard.take() {
        Some(tip) => {
            let block_id = BlockId {
                hash: header.block_hash(),
                height,
            };
            let new_tip = tip.insert(block_id);

            // Store the updated tip
            *tip_guard = Some(new_tip.clone());

            // Emit chain_update only (no tx_update)
            Some(Ok(SyncResponse {
                tx_update: TxUpdate::default(),
                chain_update: Some(new_tip),
            }))
        }
        None => None,
    }
}

// Add a corresponding checkpoint per anchor height if it does not yet exist. Checkpoints should not
// surpass `latest_blocks`.
fn chain_update(
    mut tip: CheckPoint,
    latest_blocks: &BTreeMap<u32, BlockHash>,
    anchors: impl Iterator<Item = (ConfirmationBlockTime, Txid)>,
) -> Result<CheckPoint, Error> {
    for (anchor, _txid) in anchors {
        let height = anchor.block_id.height;

        // Checkpoint uses the `BlockHash` from `latest_blocks` so that the hash will be consistent
        // in case of a re-org.
        if tip.get(height).is_none() && height <= tip.height() {
            let hash = match latest_blocks.get(&height) {
                Some(&hash) => hash,
                None => anchor.block_id.hash,
            };
            tip = tip.insert(BlockId { hash, height });
        }
    }
    Ok(tip)
}

/// Build a reverse lookup map: script_hash -> (keychain, index)
///
/// This allows us to efficiently determine which keychain a transaction belongs to
fn build_reverse_lookup_map<K>(
    request: &mut FullScanRequest<K>,
    response: &FullScanResponse<K>,
    stop_gap: usize,
) -> HashMap<ElectrumScriptHash, (K, u32)>
where
    K: Ord + Clone,
{
    let mut script_to_keychain_index: HashMap<ElectrumScriptHash, (K, u32)> = HashMap::new();

    for keychain in request.keychains() {
        for (index, script) in request.iter_spks(keychain.clone()) {
            let script_hash = ElectrumScriptHash::new(script);
            script_to_keychain_index.insert(script_hash, (keychain.clone(), index));

            // Only iterate up to last_active_index + stop_gap to avoid billions of iterations
            if let Some(&last_active) = response.last_active_indices.get(&keychain) {
                if index > last_active + stop_gap as u32 {
                    break;
                }
            }
        }
    }

    script_to_keychain_index
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Arc;
    use std::time::Duration;

    use bdk_core::bitcoin::absolute::LockTime;
    use bdk_core::bitcoin::constants::genesis_block;
    use bdk_core::bitcoin::transaction::Version;
    use bdk_core::bitcoin::{
        Amount, Network, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Witness,
    };
    use bdk_core::spk_client::FullScanRequest;
    use bdk_core::{BlockId, CheckPoint};
    use futures::StreamExt;
    use testenv::TestEnv;
    use tokio::time::{sleep, timeout};
    use tokio_electrum::address::ElectrumServerAddress;
    use tokio_electrum::client::ElectrumClient;

    use super::*;

    fn hash(hex: &str) -> BlockHash {
        BlockHash::from_str(hex).unwrap()
    }

    fn txid(hex: &str) -> Txid {
        Txid::from_str(hex).unwrap()
    }

    fn script(byte: u8) -> ScriptBuf {
        ScriptBuf::from_bytes(vec![byte])
    }

    fn dummy_tx(tag: u8) -> Transaction {
        Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1_000 + tag as u64),
                script_pubkey: script(tag),
            }],
        }
    }

    fn test_bdk_client() -> BdkElectrumClient {
        let addr = ElectrumServerAddress::parse("tcp://127.0.0.1:50001").unwrap();
        BdkElectrumClient::new(ElectrumClient::new(addr))
    }

    fn ctx_for_request(request: FullScanRequest<String>) -> SubscriptionCtx<String> {
        SubscriptionCtx {
            request: Mutex::new(request),
            subscribed_scripts: Mutex::new(HashSet::new()),
            script_to_keychain_index: Mutex::new(HashMap::new()),
            last_active_indices: Mutex::new(BTreeMap::new()),
            chain_tip: Mutex::new(None),
        }
    }

    async fn wait_connected(client: &ElectrumClient) {
        let connected = timeout(Duration::from_secs(20), async {
            loop {
                if client.status().is_connected() {
                    return;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
        assert!(
            connected.is_ok(),
            "timed out waiting for electrum connection"
        );
    }

    #[test]
    fn chain_update_inserts_missing_anchor_checkpoint_from_latest_blocks() {
        let tip = CheckPoint::new(BlockId {
            height: 0,
            hash: hash("0000000000000000000000000000000000000000000000000000000000000000"),
        })
        .insert(BlockId {
            height: 5,
            hash: hash("0000000000000000000000000000000000000000000000000000000000000005"),
        });
        let latest_blocks = BTreeMap::from([(
            4_u32,
            hash("0000000000000000000000000000000000000000000000000000000000000004"),
        )]);
        let anchors = vec![
            (
                ConfirmationBlockTime {
                    confirmation_time: 0,
                    block_id: BlockId {
                        height: 4,
                        hash: hash(
                            "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
                        ),
                    },
                },
                txid("0101010101010101010101010101010101010101010101010101010101010101"),
            ),
            (
                ConfirmationBlockTime {
                    confirmation_time: 0,
                    block_id: BlockId {
                        height: 6,
                        hash: hash(
                            "abababababababababababababababababababababababababababababababab",
                        ),
                    },
                },
                txid("0202020202020202020202020202020202020202020202020202020202020202"),
            ),
        ];

        let updated = chain_update(tip, &latest_blocks, anchors.into_iter()).unwrap();
        let cp4 = updated
            .get(4)
            .expect("checkpoint at anchor height should be inserted");
        assert_eq!(cp4.block_id().hash, latest_blocks[&4]);
        assert!(
            updated.get(6).is_none(),
            "height beyond tip must be ignored"
        );
    }

    #[tokio::test]
    async fn handle_block_header_notification_returns_none_without_tip() {
        let request = FullScanRequest::<String>::builder_at(0).build();
        let ctx = SubscriptionCtx {
            request: Mutex::new(request),
            subscribed_scripts: Mutex::new(HashSet::new()),
            script_to_keychain_index: Mutex::new(HashMap::new()),
            last_active_indices: Mutex::new(BTreeMap::new()),
            chain_tip: Mutex::new(None),
        };
        let header = genesis_block(Network::Regtest).header;

        let update = handle_block_header_notification(1, header, &ctx).await;
        assert!(update.is_none());
    }

    #[tokio::test]
    async fn handle_block_header_notification_updates_chain_tip() {
        let genesis = genesis_block(Network::Regtest);
        let request = FullScanRequest::<String>::builder_at(0).build();
        let ctx = SubscriptionCtx {
            request: Mutex::new(request),
            subscribed_scripts: Mutex::new(HashSet::new()),
            script_to_keychain_index: Mutex::new(HashMap::new()),
            last_active_indices: Mutex::new(BTreeMap::new()),
            chain_tip: Mutex::new(Some(CheckPoint::new(BlockId {
                height: 0,
                hash: genesis.block_hash(),
            }))),
        };

        let update = handle_block_header_notification(1, genesis.header, &ctx)
            .await
            .expect("expected update")
            .unwrap();
        let new_tip = update.chain_update.expect("missing chain update");
        assert_eq!(new_tip.height(), 1);
    }

    #[test]
    fn build_reverse_lookup_map_respects_last_active_plus_stop_gap() {
        let mut request = FullScanRequest::<String>::builder_at(0)
            .spks_for_keychain(
                "external".to_string(),
                vec![
                    (0, script(0x51)),
                    (1, script(0x52)),
                    (2, script(0x53)),
                    (3, script(0x54)),
                    (4, script(0x55)),
                ],
            )
            .build();
        let response = FullScanResponse {
            tx_update: TxUpdate::default(),
            chain_update: None,
            last_active_indices: BTreeMap::from([("external".to_string(), 1)]),
        };

        let lookup = build_reverse_lookup_map(&mut request, &response, 1);
        let max_index = lookup.values().map(|(_, index)| *index).max().unwrap();
        assert_eq!(max_index, 3);
        assert!(
            !lookup.values().any(|(_, index)| *index == 4),
            "lookup should stop after the first index greater than last_active + stop_gap"
        );
    }

    #[tokio::test]
    async fn populate_tx_cache_and_fetch_tx_cache_hit() {
        let client = test_bdk_client();
        let tx = Arc::new(dummy_tx(0x77));
        let txid = tx.compute_txid();

        client.populate_tx_cache(vec![tx.clone()]).await;
        let fetched = client.fetch_tx(txid).await.unwrap();

        assert_eq!(fetched.compute_txid(), txid);
        assert!(Arc::ptr_eq(&fetched, &tx));
    }

    #[tokio::test]
    async fn fetch_header_uses_cache_when_present() {
        let client = test_bdk_client();
        let header = genesis_block(Network::Regtest).header;

        {
            let mut cache = client.block_header_cache.lock().await;
            cache.insert(42, header);
        }

        let fetched = client.fetch_header(42).await.unwrap();
        assert_eq!(fetched, header);
    }

    #[tokio::test]
    async fn subscription_ctx_lookup_and_extension_logic() {
        let request = FullScanRequest::<String>::builder_at(0)
            .spks_for_keychain(
                "external".to_string(),
                vec![
                    (0, script(0x11)),
                    (1, script(0x12)),
                    (2, script(0x13)),
                    (3, script(0x14)),
                ],
            )
            .build();
        let ctx = ctx_for_request(request);

        let hash1 = ElectrumScriptHash::new(&script(0x12));
        let hash3 = ElectrumScriptHash::new(&script(0x14));

        {
            let mut scripts = ctx.subscribed_scripts.lock().await;
            scripts.insert(hash1);
            scripts.insert(hash3);
        }
        {
            let mut lookup = ctx.script_to_keychain_index.lock().await;
            lookup.insert(hash1, ("external".to_string(), 1));
            lookup.insert(hash3, ("external".to_string(), 3));
        }
        {
            let mut last_active = ctx.last_active_indices.lock().await;
            last_active.insert("external".to_string(), 1);
        }

        assert!(ctx.has_script(&hash1).await);
        assert_eq!(
            ctx.keychain_index(&hash1).await,
            Some(("external".to_string(), 1))
        );
        assert_eq!(ctx.needs_extension(&hash1).await, None);
        assert_eq!(
            ctx.needs_extension(&hash3).await,
            Some(("external".to_string(), 3))
        );
    }

    #[tokio::test]
    async fn subscribe_incremental_updates_state() {
        let client = test_bdk_client();
        let request = FullScanRequest::<String>::builder_at(0)
            .spks_for_keychain(
                "external".to_string(),
                vec![
                    (0, script(0x21)),
                    (1, script(0x22)),
                    (2, script(0x23)),
                    (3, script(0x24)),
                ],
            )
            .build();
        let ctx = ctx_for_request(request);

        client
            .subscribe_incremental("external".to_string(), 0, 2, &ctx)
            .await
            .unwrap();

        let expected_hashes = vec![
            ElectrumScriptHash::new(&script(0x22)),
            ElectrumScriptHash::new(&script(0x23)),
        ];
        let scripts = ctx.subscribed_scripts.lock().await;
        for hash in &expected_hashes {
            assert!(scripts.contains(hash));
        }
        drop(scripts);

        let lookup = ctx.script_to_keychain_index.lock().await;
        assert_eq!(
            lookup.get(&expected_hashes[0]),
            Some(&("external".to_string(), 1))
        );
        assert_eq!(
            lookup.get(&expected_hashes[1]),
            Some(&("external".to_string(), 2))
        );
        drop(lookup);

        let last_active = ctx.last_active_indices.lock().await;
        assert_eq!(last_active.get("external"), Some(&0));
    }

    #[tokio::test]
    async fn subscribe_incremental_noop_when_no_more_scripts() {
        let client = test_bdk_client();
        let request = FullScanRequest::<String>::builder_at(0)
            .spks_for_keychain(
                "external".to_string(),
                vec![(0, script(0x31)), (1, script(0x32))],
            )
            .build();
        let ctx = ctx_for_request(request);

        client
            .subscribe_incremental("external".to_string(), 10, 3, &ctx)
            .await
            .unwrap();

        assert!(ctx.subscribed_scripts.lock().await.is_empty());
    }

    #[tokio::test]
    async fn handle_script_hash_notification_ignores_unsubscribed_hash() {
        let client = test_bdk_client();
        let request = FullScanRequest::<String>::builder_at(0).build();
        let ctx = ctx_for_request(request);
        let hash = ElectrumScriptHash::new(&script(0x41));

        let update = client
            .handle_script_hash_notification(hash, 0, false, 2, &ctx)
            .await;
        assert!(update.is_none());
    }

    #[tokio::test]
    async fn handle_script_hash_notification_can_extend_without_update() {
        let client = test_bdk_client();
        let request = FullScanRequest::<String>::builder_at(0)
            .spks_for_keychain(
                "external".to_string(),
                vec![
                    (0, script(0x51)),
                    (1, script(0x52)),
                    (2, script(0x53)),
                    (3, script(0x54)),
                    (4, script(0x55)),
                ],
            )
            .build();
        let ctx = ctx_for_request(request);
        let trigger_hash = ElectrumScriptHash::new(&script(0x53));

        {
            let mut scripts = ctx.subscribed_scripts.lock().await;
            scripts.insert(trigger_hash);
        }
        {
            let mut lookup = ctx.script_to_keychain_index.lock().await;
            lookup.insert(trigger_hash, ("external".to_string(), 2));
        }
        {
            let mut last_active = ctx.last_active_indices.lock().await;
            last_active.insert("external".to_string(), 1);
        }

        let update = client
            .handle_script_hash_notification(trigger_hash, 0, false, 2, &ctx)
            .await;
        assert!(
            update.is_none(),
            "no tx tracker entry should yield no update"
        );

        let last_active = ctx.last_active_indices.lock().await;
        assert_eq!(last_active.get("external"), Some(&2));
        drop(last_active);

        let scripts = ctx.subscribed_scripts.lock().await;
        assert!(scripts.contains(&ElectrumScriptHash::new(&script(0x54))));
        assert!(scripts.contains(&ElectrumScriptHash::new(&script(0x55))));
    }

    #[tokio::test]
    async fn process_script_hash_update_ignores_untracked_hash() {
        let client = test_bdk_client();
        let hash = ElectrumScriptHash::new(&script(0x61));

        let update = client
            .process_script_hash_update(hash, 0, false)
            .await
            .unwrap();
        assert!(update.is_none());
    }

    #[test]
    fn chain_update_uses_anchor_hash_when_latest_block_is_missing() {
        let tip = CheckPoint::new(BlockId {
            height: 0,
            hash: hash("0000000000000000000000000000000000000000000000000000000000000000"),
        })
        .insert(BlockId {
            height: 5,
            hash: hash("0000000000000000000000000000000000000000000000000000000000000005"),
        });
        let anchor_hash = hash("1111111111111111111111111111111111111111111111111111111111111111");
        let anchors = vec![(
            ConfirmationBlockTime {
                confirmation_time: 123,
                block_id: BlockId {
                    height: 4,
                    hash: anchor_hash,
                },
            },
            txid("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        )];

        let updated = chain_update(tip, &BTreeMap::new(), anchors.into_iter()).unwrap();
        assert_eq!(updated.get(4).unwrap().block_id().hash, anchor_hash);
    }

    #[tokio::test]
    async fn fetch_tip_and_latest_blocks_returns_prev_tip_if_server_tip_is_lower() {
        let env = TestEnv::new();
        let addr =
            ElectrumServerAddress::parse(&format!("tcp://{}", env.electrsd.electrum_url)).unwrap();
        let client = ElectrumClient::new(addr);
        client.connect();
        wait_connected(&client).await;

        let prev_tip = CheckPoint::new(BlockId {
            height: 1_000,
            hash: hash("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        });
        let (tip, latest) = fetch_tip_and_latest_blocks(&client, prev_tip.clone())
            .await
            .unwrap();

        assert_eq!(tip, prev_tip);
        assert!(latest.is_empty());
        client.disconnect();
    }

    #[tokio::test]
    async fn fetch_tip_and_latest_blocks_handles_disagreeing_prev_tip() {
        let env = TestEnv::new();
        let mine_to = env.bitcoind.client.new_address().unwrap();
        env.bitcoind
            .client
            .generate_to_address(3, &mine_to)
            .unwrap();
        let indexed_height = env.bitcoind.client.get_blockchain_info().unwrap().blocks as usize;
        env.electrsd.wait_height(indexed_height);

        let addr =
            ElectrumServerAddress::parse(&format!("tcp://{}", env.electrsd.electrum_url)).unwrap();
        let client = ElectrumClient::new(addr);
        client.connect();
        wait_connected(&client).await;

        let wrong_prev_tip = CheckPoint::new(BlockId {
            height: 0,
            hash: hash("abababababababababababababababababababababababababababababababab"),
        });

        let (tip, latest) = fetch_tip_and_latest_blocks(&client, wrong_prev_tip)
            .await
            .unwrap();

        assert!(tip.height() > 0, "expected reconstructed tip from electrum");
        assert!(
            !latest.is_empty(),
            "expected latest block map to be populated"
        );
        client.disconnect();
    }

    async fn connected_bdk_client(env: &TestEnv) -> BdkElectrumClient {
        let addr =
            ElectrumServerAddress::parse(&format!("tcp://{}", env.electrsd.electrum_url)).unwrap();
        let inner = ElectrumClient::new(addr);
        let client = BdkElectrumClient::new(inner);
        client.connect();
        wait_connected(&client).await;
        client
    }

    fn current_tip_checkpoint(env: &TestEnv) -> CheckPoint {
        let height: u32 = env
            .bitcoind
            .client
            .get_blockchain_info()
            .unwrap()
            .blocks
            .try_into()
            .unwrap();
        let hash = env
            .bitcoind
            .client
            .get_block_hash(height as u64)
            .unwrap()
            .block_hash()
            .unwrap();
        CheckPoint::new(BlockId { height, hash })
    }

    fn ensure_funded_wallet(env: &TestEnv) {
        let reward_addr = env.bitcoind.client.new_address().unwrap();
        env.bitcoind
            .client
            .generate_to_address(101, &reward_addr)
            .unwrap();
        let tip_height = env.bitcoind.client.get_blockchain_info().unwrap().blocks as usize;
        env.electrsd.wait_height(tip_height);
    }

    #[tokio::test]
    async fn sync_stream_initial_empty_for_unused_spk() {
        let env = TestEnv::new();
        let client = connected_bdk_client(&env).await;

        let unused_address = env.bitcoind.client.new_address().unwrap();
        let request = FullScanRequest::<String>::builder_at(0)
            .spks_for_keychain(
                "external".to_string(),
                vec![(0, unused_address.script_pubkey())],
            )
            .build();

        let mut stream = client.sync(request, 20, 20, false).await.unwrap();
        let first = stream
            .next()
            .await
            .expect("expected initial event")
            .expect("initial event should not error");
        match first {
            SubscribeEvent::Initial(initial) => assert!(initial.is_empty()),
            other => panic!("expected initial event first, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn sync_stream_initial_contains_chain_update_when_chain_tip_present() {
        let env = TestEnv::new();
        let client = connected_bdk_client(&env).await;

        let unused_address = env.bitcoind.client.new_address().unwrap();
        let request = FullScanRequest::<String>::builder_at(0)
            .chain_tip(current_tip_checkpoint(&env))
            .spks_for_keychain(
                "external".to_string(),
                vec![(0, unused_address.script_pubkey())],
            )
            .build();

        let mut stream = client.sync(request, 20, 20, false).await.unwrap();
        let first = stream
            .next()
            .await
            .expect("expected initial event")
            .expect("initial event should not error");
        match first {
            SubscribeEvent::Initial(initial) => {
                assert!(initial.chain_update.is_some(), "expected chain update");
            }
            other => panic!("expected initial event first, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn sync_stream_initial_populates_prev_txouts_when_requested() {
        let env = TestEnv::new();
        ensure_funded_wallet(&env);
        let client = connected_bdk_client(&env).await;

        let tracked_address = env.bitcoind.client.new_address().unwrap();
        let txid = env
            .bitcoind
            .client
            .send_to_address(&tracked_address, Amount::from_sat(50_000))
            .unwrap()
            .txid()
            .unwrap();
        env.electrsd.wait_tx(&txid);

        let request = FullScanRequest::<String>::builder_at(0)
            .spks_for_keychain(
                "external".to_string(),
                vec![(0, tracked_address.script_pubkey())],
            )
            .build();

        let mut stream = client.sync(request, 20, 20, true).await.unwrap();
        let first = stream
            .next()
            .await
            .expect("expected initial event")
            .expect("initial event should not error");

        match first {
            SubscribeEvent::Initial(initial) => {
                assert!(
                    !initial.tx_update.txs.is_empty(),
                    "expected transaction data"
                );
                assert!(
                    !initial.tx_update.txouts.is_empty(),
                    "expected previous txouts to be fetched"
                );
            }
            other => panic!("expected initial event first, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn sync_stream_emits_update_after_new_block() {
        let env = TestEnv::new();
        let client = connected_bdk_client(&env).await;

        let tracked_address = env.bitcoind.client.new_address().unwrap();
        let request = FullScanRequest::<String>::builder_at(0)
            .chain_tip(current_tip_checkpoint(&env))
            .spks_for_keychain(
                "external".to_string(),
                vec![(0, tracked_address.script_pubkey())],
            )
            .build();

        let mut stream = client.sync(request, 20, 20, false).await.unwrap();
        let _ = stream
            .next()
            .await
            .expect("expected initial event")
            .expect("initial event should not error");

        env.bitcoind
            .client
            .generate_to_address(1, &env.bitcoind.client.new_address().unwrap())
            .unwrap();
        let new_height = env.bitcoind.client.get_blockchain_info().unwrap().blocks as usize;
        env.electrsd.wait_height(new_height);

        let saw_chain_update = timeout(Duration::from_secs(20), async {
            while let Some(event) = stream.next().await {
                match event {
                    Ok(SubscribeEvent::Update(update)) if update.chain_update.is_some() => {
                        return true;
                    }
                    Ok(SubscribeEvent::Disconnected) => return false,
                    Ok(_) => {}
                    Err(_) => return false,
                }
            }
            false
        })
        .await
        .unwrap_or(false);

        assert!(saw_chain_update, "expected chain update after new block");
    }

    #[tokio::test]
    async fn sync_stream_emits_disconnected_event_on_disconnect() {
        let env = TestEnv::new();
        let client = connected_bdk_client(&env).await;

        let tracked_address = env.bitcoind.client.new_address().unwrap();
        env.bitcoind
            .client
            .generate_to_address(1, &tracked_address)
            .unwrap();
        let indexed_height = env.bitcoind.client.get_blockchain_info().unwrap().blocks as usize;
        env.electrsd.wait_height(indexed_height);

        let request = FullScanRequest::<String>::builder_at(0)
            .spks_for_keychain(
                "external".to_string(),
                vec![(0, tracked_address.script_pubkey())],
            )
            .build();
        let mut stream = client.sync(request, 20, 20, false).await.unwrap();

        let _ = timeout(Duration::from_secs(10), stream.next())
            .await
            .expect("stream timed out waiting for initial event")
            .expect("stream terminated before initial event")
            .expect("initial event should not be an error");

        client.disconnect();

        let disconnected = timeout(Duration::from_secs(20), async {
            while let Some(event) = stream.next().await {
                if matches!(event, Ok(SubscribeEvent::Disconnected)) {
                    return true;
                }
            }
            false
        })
        .await
        .unwrap_or(false);

        assert!(disconnected, "expected disconnected event after disconnect");
    }

    #[tokio::test]
    async fn sync_stream_ignores_untracked_script_notifications() {
        let env = TestEnv::new();
        ensure_funded_wallet(&env);
        let client = connected_bdk_client(&env).await;

        let tracked_address = env.bitcoind.client.new_address().unwrap();
        let other_address = env.bitcoind.client.new_address().unwrap();
        let other_hash = ElectrumScriptHash::new(&other_address.script_pubkey());

        let request = FullScanRequest::<String>::builder_at(0)
            .spks_for_keychain(
                "external".to_string(),
                vec![(0, tracked_address.script_pubkey())],
            )
            .build();
        let mut stream = client.sync(request, 20, 20, false).await.unwrap();
        let _ = stream
            .next()
            .await
            .expect("expected initial event")
            .expect("initial event should not error");

        // Subscribe directly on the underlying client to force unrelated script notifications.
        client.script_hash_subscribe(other_hash).unwrap();
        let txid = env
            .bitcoind
            .client
            .send_to_address(&other_address, Amount::from_sat(12_000))
            .unwrap()
            .txid()
            .unwrap();
        env.electrsd.wait_tx(&txid);

        let maybe_event = timeout(Duration::from_secs(3), stream.next()).await;
        assert!(
            maybe_event.is_err(),
            "did not expect bdk stream event for unrelated script hash notifications"
        );
    }

    #[tokio::test]
    async fn sync_stream_emits_script_tx_updates_for_tracked_hash() {
        let env = TestEnv::new();
        ensure_funded_wallet(&env);
        let client = connected_bdk_client(&env).await;

        let tracked_address = env.bitcoind.client.new_address().unwrap();
        let initial_txid = env
            .bitcoind
            .client
            .send_to_address(&tracked_address, Amount::from_sat(20_000))
            .unwrap()
            .txid()
            .unwrap();
        env.electrsd.wait_tx(&initial_txid);

        let request = FullScanRequest::<String>::builder_at(0)
            .spks_for_keychain(
                "external".to_string(),
                vec![(0, tracked_address.script_pubkey())],
            )
            .build();

        let mut stream = client.sync(request, 20, 20, true).await.unwrap();
        let _ = stream
            .next()
            .await
            .expect("expected initial event")
            .expect("initial event should not error");

        let next_txid = env
            .bitcoind
            .client
            .send_to_address(&tracked_address, Amount::from_sat(21_000))
            .unwrap()
            .txid()
            .unwrap();
        env.electrsd.wait_tx(&next_txid);

        let saw_tx_update = timeout(Duration::from_secs(20), async {
            while let Some(event) = stream.next().await {
                match event {
                    Ok(SubscribeEvent::Update(update))
                        if update
                            .tx_update
                            .txs
                            .iter()
                            .any(|tx| tx.compute_txid() == next_txid) =>
                    {
                        return !update.tx_update.txouts.is_empty();
                    }
                    Ok(SubscribeEvent::Disconnected) => return false,
                    Ok(_) => {}
                    Err(_) => return false,
                }
            }
            false
        })
        .await
        .unwrap_or(false);

        assert!(
            saw_tx_update,
            "expected script-hash update with transaction and prevouts"
        );
    }
}

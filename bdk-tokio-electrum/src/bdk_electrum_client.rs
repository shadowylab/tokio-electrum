use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::{Debug, Display};
use std::ops::Deref;
use std::sync::Arc;

use bdk_core::bitcoin::block::Header;
use bdk_core::bitcoin::{BlockHash, Transaction, Txid};
use bdk_core::spk_client::{FullScanRequest, FullScanResponse, SpkWithExpectedTxids, SyncResponse};
use bdk_core::{BlockId, CheckPoint, ConfirmationBlockTime, TxUpdate};
use futures::StreamExt;
use futures::stream::BoxStream;
use tokio::sync::{Mutex, RwLock, oneshot};
pub use tokio_electrum::client::{ElectrumClient, Error};
use tokio_electrum::notification::ElectrumNotification;
use tokio_electrum::types::{BlockHeader, ElectrumScriptHash};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

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
        subscribe: bool,
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
                    subscribe,
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

    /// Full scan the keychain scripts specified with the blockchain (via an Electrum client) and
    /// returns updates for [`bdk_chain`] data structures.
    ///
    /// - `request`: struct with data required to perform a spk-based blockchain client full scan,
    ///   see [`FullScanRequest`].
    /// - `stop_gap`: the full scan for each keychain stops after a gap of script pubkeys with no
    ///   associated transactions.
    /// - `batch_size`: specifies the max number of script pubkeys to request for in a single batch
    ///   request.
    /// - `fetch_prev_txouts`: specifies whether we want previous `TxOut`s for fee calculation. Note
    ///   that this requires additional calls to the Electrum server, but is necessary for
    ///   calculating the fee on a transaction if your wallet does not own the inputs. Methods like
    ///   [`Wallet.calculate_fee`] and [`Wallet.calculate_fee_rate`] will return a
    ///   [`CalculateFeeError::MissingTxOut`] error if those `TxOut`s are not present in the
    ///   transaction graph.
    ///
    /// [`bdk_chain`]: ../bdk_chain/index.html
    /// [`CalculateFeeError::MissingTxOut`]: ../bdk_chain/tx_graph/enum.CalculateFeeError.html#variant.MissingTxOut
    /// [`Wallet.calculate_fee`]: ../bdk_wallet/struct.Wallet.html#method.calculate_fee
    /// [`Wallet.calculate_fee_rate`]: ../bdk_wallet/struct.Wallet.html#method.calculate_fee_rate
    pub async fn full_scan<K>(
        &self,
        request: impl Into<FullScanRequest<K>>,
        stop_gap: usize,
        batch_size: usize,
        fetch_prev_txouts: bool,
    ) -> Result<FullScanResponse<K>, Error>
    where
        K: Ord + Clone + Display,
    {
        let mut request: FullScanRequest<K> = request.into();
        let (response, _) = self
            .internal_full_scan(&mut request, stop_gap, batch_size, fetch_prev_txouts, false)
            .await?;
        Ok(response)
    }

    /// Full scan the keychain scripts with real-time subscription support.
    ///
    /// This method performs the same initial scan as [`full_scan`](Self::full_scan), but additionally
    /// subscribes to each script pubkey that has transaction history. It returns both the initial
    /// scan results and a stream of real-time updates.
    pub async fn full_scan_and_subscribe<K>(
        &self,
        request: impl Into<FullScanRequest<K>>,
        stop_gap: usize,
        batch_size: usize,
        fetch_prev_txouts: bool,
    ) -> Result<(FullScanResponse<K>, SubscriptionStream), Error>
    where
        K: Ord + Clone + Display + Send + 'static,
    {
        let mut request = request.into();

        let (response, all_subscribed_scripts) = self
            .internal_full_scan(&mut request, stop_gap, batch_size, fetch_prev_txouts, true)
            .await?;

        // Create the update stream with only the subscribed scripts
        let stream = self.create_subscription_stream(
            request.start_time(),
            all_subscribed_scripts,
            fetch_prev_txouts,
            request,
            &response,
            stop_gap,
        );

        Ok((response, stream))
    }

    /// Creates a stream that processes script hash and block header notifications.
    fn create_subscription_stream<K>(
        &self,
        start_time: u64,
        subscribed_scripts: HashSet<ElectrumScriptHash>,
        fetch_prev_txouts: bool,
        mut request: FullScanRequest<K>,
        response: &FullScanResponse<K>,
        stop_gap: usize,
    ) -> SubscriptionStream
    where
        K: Ord + Clone + Display + Send + 'static,
    {
        let client = self.clone();
        let notification_rx = self.inner.notifications();

        // Build a reverse lookup map
        let script_to_keychain_index = build_reverse_lookup_map(&mut request, response, stop_gap);

        let ctx = Arc::new(SubscriptionCtx {
            request: Mutex::new(request),
            subscribed_scripts: Mutex::new(subscribed_scripts),
            script_to_keychain_index: Mutex::new(script_to_keychain_index),
            last_active_indices: Mutex::new(response.last_active_indices.clone()),
            chain_tip: Mutex::new(response.chain_update.clone()),
        });

        // Create a oneshot channel
        let (tx, rx_done) = oneshot::channel();
        let mut tx: Option<oneshot::Sender<()>> = Some(tx);

        let stream = BroadcastStream::new(notification_rx).inspect(move |notification| {
            if let Ok(ElectrumNotification::ConnectionStatusChanged(status)) = notification {
                if status.is_disconnected() {
                    // Take the sender and send the oneshot notification
                    if let Some(tx) = tx.take() {
                        let _ = tx.send(());
                    }
                }
            }
        }).filter_map(move |result| {
            let client: BdkElectrumClient = client.clone();
            let ctx: Arc<SubscriptionCtx<K>> = ctx.clone();

            async move {
                match result {
                    Ok(ElectrumNotification::ScriptHash { hash, .. }) => {
                        client
                            .handle_script_hash_notification(
                                hash,
                                start_time,
                                fetch_prev_txouts,
                                stop_gap,
                                &ctx,
                            )
                            .await
                    }
                    Ok(ElectrumNotification::BlockHeader { height, header }) => {
                        handle_block_header_notification(height, header, &ctx).await
                    }
                    Ok(_) => None,  // Ignore other notifications
                    Err(BroadcastStreamRecvError::Lagged(n)) => {
                        tracing::warn!("Subscription stream lagged behind by {} messages - some updates may have been missed", n);
                        None
                    }
                }
            }
        }).take_until(rx_done);

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
        subscribe: bool,
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
                        if subscribe && !scripts_to_subscribe.is_empty() {
                            self.subscribe_scripts(&scripts_to_subscribe, &mut subscribed_hashes)
                                .await;
                        }
                        return Ok((last_active_index, subscribed_hashes));
                    }
                } else {
                    last_active_index = Some(spk_index);
                    unused_spk_count = 0;

                    // Collect for subscription if enabled
                    if subscribe {
                        let script_hash = ElectrumScriptHash::new(&spk.spk);
                        let spk_history_set: HashSet<Txid> =
                            spk_history.iter().map(|res| res.txid()).collect();
                        let subscription = ScriptSubscription::new(spk_history_set.clone());
                        scripts_to_subscribe.push((script_hash, subscription));
                    }
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
            if subscribe && !scripts_to_subscribe.is_empty() {
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

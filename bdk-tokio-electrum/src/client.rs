use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::{Debug, Display};
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use bdk_core::bitcoin::block::Header;
use bdk_core::bitcoin::{BlockHash, Transaction, Txid};
use bdk_core::spk_client::{FullScanRequest, FullScanResponse, SpkWithExpectedTxids, SyncResponse};
use bdk_core::{BlockId, CheckPoint, ConfirmationBlockTime, TxUpdate};
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::{self, BoxStream};
use tokio::sync::{Mutex, RwLock, broadcast};
use tokio::time::{Instant, timeout_at};
pub use tokio_electrum::client::{ElectrumClient, Error};
use tokio_electrum::notification::ElectrumNotification;
use tokio_electrum::types::{BlockHeader, ElectrumScriptHash};

use crate::util;

/// We include a chain suffix of a certain length for the purpose of robustness.
const CHAIN_SUFFIX_LENGTH: u32 = 8;
const DEFAULT_STOP_GAP: usize = 50;
const DEFAULT_BATCH_SIZE: usize = 20;
const DEFAULT_BATCH_WINDOW: Duration = Duration::from_millis(1500);
const DEFAULT_WALLET_LABEL: &str = "unlabeled";

struct SubscriptionCtx<K> {
    wallet_label: String,
    request: Mutex<FullScanRequest<K>>,
    subscribed_scripts: Mutex<HashSet<ElectrumScriptHash>>,
    script_subscriptions: Mutex<HashMap<ElectrumScriptHash, ScriptSubscription>>,
    script_to_keychain_index: Mutex<HashMap<ElectrumScriptHash, (K, u32)>>,
    last_active_indices: Mutex<BTreeMap<K, u32>>,
    max_subscribed_indices: Mutex<BTreeMap<K, u32>>,
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

    async fn extension_target(&self, hash: &ElectrumScriptHash) -> Option<(K, u32)> {
        let (keychain, current_index) = self.keychain_index(hash).await?;

        // Extend only when this script index moved beyond the known last-active index.
        let last_active = self.last_active_indices.lock().await;
        let last: u32 = last_active.get(&keychain).copied()?;

        if current_index > last {
            Some((keychain, current_index))
        } else {
            None
        }
    }

    async fn tracked_script_hashes(&self) -> Vec<ElectrumScriptHash> {
        let scripts = self.subscribed_scripts.lock().await;
        scripts.iter().copied().collect()
    }

    async fn script_hashes_with_unconfirmed_txs(&self) -> Vec<ElectrumScriptHash> {
        let subscriptions = self.script_subscriptions.lock().await;
        subscriptions
            .iter()
            .filter_map(|(hash, sub)| {
                sub.expected_tx_heights
                    .values()
                    .any(|height| *height <= 0)
                    .then_some(*hash)
            })
            .collect()
    }
}

#[derive(Default)]
struct PendingBatch {
    dirty_scripts: HashSet<ElectrumScriptHash>,
    latest_header: Option<(u32, Header)>,
    lagged: bool,
    disconnected: bool,
}

impl PendingBatch {
    fn has_work(&self) -> bool {
        self.lagged || !self.dirty_scripts.is_empty() || self.latest_header.is_some()
    }
}

struct SubscriptionInit<K> {
    subscribed_scripts: HashSet<ElectrumScriptHash>,
    script_subscriptions: HashMap<ElectrumScriptHash, ScriptSubscription>,
    script_to_keychain_index: HashMap<ElectrumScriptHash, (K, u32)>,
    max_subscribed_indices: BTreeMap<K, u32>,
}

struct SpkScanResult {
    last_active_index: Option<u32>,
    subscribed_hashes: HashSet<ElectrumScriptHash>,
    subscribed_script_subscriptions: HashMap<ElectrumScriptHash, ScriptSubscription>,
    subscribed_script_to_index: HashMap<ElectrumScriptHash, u32>,
    max_subscribed_index: Option<u32>,
}

struct LiveStreamState<K> {
    client: BdkElectrumClient,
    ctx: Arc<SubscriptionCtx<K>>,
    notification_rx: broadcast::Receiver<ElectrumNotification>,
    pending_disconnect: bool,
    done: bool,
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

/// Sync wallet API
#[must_use = "Does nothing unless you await!"]
pub struct SyncWallet<'client, K> {
    client: &'client BdkElectrumClient,
    request: FullScanRequest<K>,
    stop_gap: usize,
    batch_size: usize,
    fetch_prev_txouts: bool,
    batch_window: Duration,
    label: String,
}

impl<'client, K> SyncWallet<'client, K>
where
    K: Ord + Clone + Display + Send + 'static,
{
    pub(crate) fn new(client: &'client BdkElectrumClient, request: FullScanRequest<K>) -> Self {
        Self {
            client,
            request,
            stop_gap: DEFAULT_STOP_GAP,
            batch_size: DEFAULT_BATCH_SIZE,
            fetch_prev_txouts: false,
            batch_window: DEFAULT_BATCH_WINDOW,
            label: DEFAULT_WALLET_LABEL.to_string(),
        }
    }

    /// Set a stop gap (default: 50)
    #[inline]
    pub fn stop_gap(mut self, stop_gap: usize) -> Self {
        self.stop_gap = stop_gap;
        self
    }

    /// Set a batch size (default: 20)
    #[inline]
    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Enable fetching of previous transaction outputs (default: false)
    #[inline]
    pub fn fetch_prev_txouts(mut self, fetch_prev_txouts: bool) -> Self {
        self.fetch_prev_txouts = fetch_prev_txouts;
        self
    }

    /// Set a batch window (default: 250ms)
    #[inline]
    pub fn batch_window(mut self, batch_window: Duration) -> Self {
        self.batch_window = batch_window;
        self
    }

    /// Set a wallet label for logs (default: unlabeled)
    #[inline]
    pub fn label<T>(mut self, label: T) -> Self
    where
        T: Into<String>,
    {
        self.label = label.into();
        self
    }
}

impl<'client, K> IntoFuture for SyncWallet<'client, K>
where
    K: Ord + Clone + Display + Send + Sync + 'static,
{
    type Output = Result<SubscribeStream<K>, Error>;
    type IntoFuture = BoxFuture<'client, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            self.client
                .run_sync(
                    self.request,
                    self.stop_gap,
                    self.batch_size,
                    self.fetch_prev_txouts,
                    self.batch_window,
                    self.label,
                )
                .await
        })
    }
}

/// Information about a subscribed script
#[derive(Debug, Clone)]
struct ScriptSubscription {
    /// The expected transaction status keyed by txid.
    ///
    /// Value is the Electrum height encoding (`>0` confirmed, `0/-1` mempool states).
    expected_tx_heights: HashMap<Txid, i64>,
}

impl ScriptSubscription {
    #[inline]
    fn new(expected_tx_heights: HashMap<Txid, i64>) -> Self {
        Self {
            expected_tx_heights,
        }
    }
}

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

    /// Run initial full scan and build subscription bootstrap state for live updates.
    async fn internal_full_scan<K>(
        &self,
        wallet_label: &str,
        request: &mut FullScanRequest<K>,
        stop_gap: usize,
        batch_size: usize,
        fetch_prev_txouts: bool,
    ) -> Result<(FullScanResponse<K>, SubscriptionInit<K>), Error>
    where
        K: Ord + Clone + Display,
    {
        let start_time = request.start_time();

        let tip_and_latest_blocks = match request.chain_tip() {
            Some(chain_tip) => {
                Some(fetch_tip_and_latest_blocks(wallet_label, &self.inner, chain_tip).await?)
            }
            None => None,
        };

        let mut tx_update: TxUpdate<ConfirmationBlockTime> = TxUpdate::default();
        let mut last_active_indices: BTreeMap<K, u32> = BTreeMap::default();
        let mut all_subscribed_scripts: HashSet<ElectrumScriptHash> = HashSet::new();
        let mut all_script_subscriptions: HashMap<ElectrumScriptHash, ScriptSubscription> =
            HashMap::new();
        let mut all_script_to_keychain_index: HashMap<ElectrumScriptHash, (K, u32)> =
            HashMap::new();
        let mut max_subscribed_indices: BTreeMap<K, u32> = BTreeMap::new();

        for keychain in request.keychains() {
            let spks = request
                .iter_spks(keychain.clone())
                .map(|(spk_i, spk)| (spk_i, SpkWithExpectedTxids::from(spk)));

            let spk_scan = self
                .populate_with_spks(
                    wallet_label,
                    start_time,
                    &mut tx_update,
                    spks,
                    stop_gap,
                    batch_size,
                    &keychain,
                )
                .await?;

            if let Some(last_active_index) = spk_scan.last_active_index {
                last_active_indices.insert(keychain.clone(), last_active_index);
            }

            // Collect all subscribed script hashes
            all_subscribed_scripts.extend(spk_scan.subscribed_hashes);
            all_script_subscriptions.extend(spk_scan.subscribed_script_subscriptions);

            for (hash, index) in spk_scan.subscribed_script_to_index {
                all_script_to_keychain_index.insert(hash, (keychain.clone(), index));
            }
            if let Some(max_subscribed_index) = spk_scan.max_subscribed_index {
                max_subscribed_indices.insert(keychain.clone(), max_subscribed_index);
            }
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

        tracing::info!(wallet = %wallet_label, "Finished loading.");

        Ok((
            response,
            SubscriptionInit {
                subscribed_scripts: all_subscribed_scripts,
                script_subscriptions: all_script_subscriptions,
                script_to_keychain_index: all_script_to_keychain_index,
                max_subscribed_indices,
            },
        ))
    }

    /// Subscribe using a full scan request.
    ///
    /// The returned stream first yields [`SubscribeEvent::Initial`] with the complete
    /// [`FullScanResponse`], then yields batched [`SubscribeEvent::Update`] values.
    /// On disconnection, it yields [`SubscribeEvent::Disconnected`] and then terminates.
    pub fn sync<K, R>(&self, request: R) -> SyncWallet<'_, K>
    where
        R: Into<FullScanRequest<K>>,
        K: Ord + Clone + Display + Send + 'static,
    {
        SyncWallet::new(self, request.into())
    }

    async fn run_sync<K>(
        &self,
        mut request: FullScanRequest<K>,
        stop_gap: usize,
        batch_size: usize,
        fetch_prev_txouts: bool,
        batch_window: Duration,
        wallet_label: String,
    ) -> Result<SubscribeStream<K>, Error>
    where
        K: Ord + Clone + Display + Send + 'static,
    {
        let start_time: u64 = request.start_time();
        let notification_rx = self.inner.notifications();

        tracing::info!(wallet = %wallet_label, "Wallet loading history.");

        let (response, subscription_init) = self
            .internal_full_scan(
                &wallet_label,
                &mut request,
                stop_gap,
                batch_size,
                fetch_prev_txouts,
            )
            .await?;

        let live_stream = self.create_live_subscription_stream(
            wallet_label,
            start_time,
            subscription_init,
            fetch_prev_txouts,
            request,
            &response,
            stop_gap,
            notification_rx,
            batch_window,
        );

        let initial_event = stream::once(async move { Ok(SubscribeEvent::Initial(response)) });
        Ok(Box::pin(initial_event.chain(live_stream)))
    }

    /// Creates a stream that batches script hash and block header notifications.
    #[allow(clippy::too_many_arguments)]
    fn create_live_subscription_stream<K>(
        &self,
        wallet_label: String,
        start_time: u64,
        subscription_init: SubscriptionInit<K>,
        fetch_prev_txouts: bool,
        request: FullScanRequest<K>,
        response: &FullScanResponse<K>,
        stop_gap: usize,
        notification_rx: broadcast::Receiver<ElectrumNotification>,
        batch_window: Duration,
    ) -> SubscribeStream<K>
    where
        K: Ord + Clone + Display + Send + 'static,
    {
        let client = self.clone();

        let ctx = Arc::new(SubscriptionCtx {
            wallet_label,
            request: Mutex::new(request),
            subscribed_scripts: Mutex::new(subscription_init.subscribed_scripts),
            script_subscriptions: Mutex::new(subscription_init.script_subscriptions),
            script_to_keychain_index: Mutex::new(subscription_init.script_to_keychain_index),
            last_active_indices: Mutex::new(response.last_active_indices.clone()),
            max_subscribed_indices: Mutex::new(subscription_init.max_subscribed_indices),
            chain_tip: Mutex::new(response.chain_update.clone()),
        });

        let initial_state = LiveStreamState {
            client,
            ctx,
            notification_rx,
            pending_disconnect: false,
            done: false,
        };
        let stream = stream::unfold(initial_state, move |mut state| async move {
            if state.done {
                return None;
            }

            if state.pending_disconnect {
                state.pending_disconnect = false;
                state.done = true;
                return Some((Ok(SubscribeEvent::Disconnected), state));
            }

            loop {
                let mut batch = PendingBatch::default();

                loop {
                    let received = state.notification_rx.recv().await;
                    state
                        .client
                        .collect_batch_notification(&state.ctx, received, &mut batch)
                        .await;
                    if batch.disconnected || batch.has_work() {
                        break;
                    }
                }

                if !batch.disconnected {
                    let deadline: Instant = Instant::now() + batch_window;

                    #[allow(clippy::while_let_loop)]
                    loop {
                        match timeout_at(deadline, state.notification_rx.recv()).await {
                            Ok(received) => {
                                state
                                    .client
                                    .collect_batch_notification(&state.ctx, received, &mut batch)
                                    .await;
                                if batch.disconnected {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                }

                match state
                    .client
                    .build_sync_update_from_batch(
                        start_time,
                        fetch_prev_txouts,
                        stop_gap,
                        &state.ctx,
                        &mut batch,
                    )
                    .await
                {
                    Ok(Some(update)) => {
                        state.pending_disconnect = batch.disconnected;
                        return Some((Ok(SubscribeEvent::Update(update)), state));
                    }
                    Ok(None) if batch.disconnected => {
                        state.done = true;
                        return Some((Ok(SubscribeEvent::Disconnected), state));
                    }
                    Ok(None) => {}
                    Err(e) => {
                        state.done = true;
                        return Some((Err(e), state));
                    }
                }
            }
        });

        Box::pin(stream)
    }

    async fn collect_batch_notification<K>(
        &self,
        ctx: &SubscriptionCtx<K>,
        received: Result<ElectrumNotification, broadcast::error::RecvError>,
        batch: &mut PendingBatch,
    ) where
        K: Ord + Clone,
    {
        match received {
            Ok(ElectrumNotification::ScriptHash { hash, .. }) => {
                if ctx.has_script(&hash).await {
                    batch.dirty_scripts.insert(hash);
                }
            }
            Ok(ElectrumNotification::BlockHeader { height, header }) => {
                batch.latest_header = Some((height, header));
            }
            Ok(ElectrumNotification::ConnectionStatusChanged(status)) if !status.is_connected() => {
                batch.disconnected = true;
            }
            Ok(ElectrumNotification::Shutdown) => {
                batch.disconnected = true;
            }
            Ok(_) => {}
            Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!(
                    wallet = %ctx.wallet_label,
                    "Subscription stream lagged by {} notifications, triggering catch-up batch",
                    n
                );
                batch.lagged = true;
            }
            Err(broadcast::error::RecvError::Closed) => {
                batch.disconnected = true;
            }
        }
    }

    async fn build_sync_update_from_batch<K>(
        &self,
        start_time: u64,
        fetch_prev_txouts: bool,
        stop_gap: usize,
        ctx: &SubscriptionCtx<K>,
        batch: &mut PendingBatch,
    ) -> Result<Option<SyncResponse<ConfirmationBlockTime>>, Error>
    where
        K: Ord + Clone + Display,
    {
        let mut tx_update = TxUpdate::<ConfirmationBlockTime>::default();
        let mut chain_update = None;

        if let Some((height, header)) = batch.latest_header.take() {
            match handle_block_header_notification(height, header, ctx).await {
                Some(Ok(update)) => {
                    chain_update = update.chain_update;
                }
                Some(Err(e)) if e.is_disconnected_like() => {
                    batch.disconnected = true;
                }
                Some(Err(e)) => return Err(e),
                None => {}
            }
        }

        let mut scripts_to_process: Vec<ElectrumScriptHash> = if batch.lagged {
            ctx.tracked_script_hashes().await
        } else {
            batch.dirty_scripts.drain().collect()
        };
        if chain_update.is_some() {
            scripts_to_process.extend(ctx.script_hashes_with_unconfirmed_txs().await);
            let mut dedup = HashSet::with_capacity(scripts_to_process.len());
            scripts_to_process.retain(|hash| dedup.insert(*hash));
        }
        batch.lagged = false;

        for hash in scripts_to_process {
            match self
                .handle_script_hash_notification(hash, start_time, fetch_prev_txouts, stop_gap, ctx)
                .await
            {
                Some(Ok(update)) => {
                    tx_update.extend(update.tx_update);
                }
                Some(Err(e)) if e.is_disconnected_like() => {
                    batch.disconnected = true;
                    break;
                }
                Some(Err(e)) => return Err(e),
                None => {}
            }
        }

        if tx_update.is_empty() && chain_update.is_none() {
            return Ok(None);
        }

        tracing::info!(
            wallet = %ctx.wallet_label,
            tx_count = tx_update.txs.len(),
            anchor_count = tx_update.anchors.len(),
            seen_count = tx_update.seen_ats.len(),
            evicted_count = tx_update.evicted_ats.len(),
            has_chain_update = chain_update.is_some(),
            "Emitting realtime sync batch."
        );

        Ok(Some(SyncResponse {
            tx_update,
            chain_update,
        }))
    }

    /// Handle script hash notification and extend subscriptions when a higher index turns active.
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
        if !ctx.has_script(&hash).await {
            return None;
        }

        let update = match self
            .process_script_hash_update(hash, start_time, fetch_prev_txouts, ctx)
            .await
        {
            Ok(Some(update)) => update,
            Ok(None) => return None,
            Err(e) => return Some(Err(e)),
        };

        if let Err(e) = self
            .maybe_extend_after_script_update(hash, stop_gap, ctx, &update)
            .await
        {
            return Some(Err(e));
        }

        Some(Ok(update))
    }

    async fn maybe_extend_after_script_update<K>(
        &self,
        hash: ElectrumScriptHash,
        stop_gap: usize,
        ctx: &SubscriptionCtx<K>,
        update: &SyncResponse<ConfirmationBlockTime>,
    ) -> Result<(), Error>
    where
        K: Ord + Clone + Display,
    {
        // Do not extend on status churn; extend only for a real script update.
        if update.tx_update.txs.is_empty() {
            return Ok(());
        }

        if let Some((keychain, current_index)) = ctx.extension_target(&hash).await {
            self.subscribe_incremental(keychain, current_index, stop_gap, ctx)
                .await?;
        }

        Ok(())
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
        let (current_max, target_index) = {
            let max_subscribed = ctx.max_subscribed_indices.lock().await;
            let current_max = max_subscribed
                .get(&keychain)
                .copied()
                .unwrap_or(new_active_index);
            (
                current_max,
                new_active_index.saturating_add(stop_gap as u32),
            )
        };

        if current_max >= target_index {
            let mut last_active = ctx.last_active_indices.lock().await;
            last_active
                .entry(keychain)
                .and_modify(|last| *last = (*last).max(new_active_index))
                .or_insert(new_active_index);
            return Ok(());
        }

        let missing = (target_index - current_max) as usize;
        let scripts_to_subscribe: Vec<(ElectrumScriptHash, u32)> = {
            let mut request_guard = ctx.request.lock().await;
            let mut scripts = Vec::with_capacity(missing);
            for _ in 0..missing {
                match request_guard.next_spk(keychain.clone()) {
                    Some((index, script)) => {
                        let script_hash = ElectrumScriptHash::new(&script);
                        scripts.push((script_hash, index));
                    }
                    None => break,
                }
            }
            scripts
        };

        if scripts_to_subscribe.is_empty() {
            let mut last_active = ctx.last_active_indices.lock().await;
            last_active
                .entry(keychain)
                .and_modify(|last| *last = (*last).max(new_active_index))
                .or_insert(new_active_index);
            return Ok(());
        }

        if let (Some((_, start_index)), Some((_, end_index))) =
            (scripts_to_subscribe.first(), scripts_to_subscribe.last())
        {
            util::log_scan_range(&ctx.wallet_label, &keychain, *start_index, *end_index);
        }

        // Collect script hashes for subscription
        let script_hashes: Vec<ElectrumScriptHash> =
            scripts_to_subscribe.iter().map(|(hash, _)| *hash).collect();

        // Subscribe via Electrum first (this can fail)
        self.inner
            .batch_script_hash_subscribe(script_hashes.iter().copied())?;

        {
            let mut last_active = ctx.last_active_indices.lock().await;
            last_active
                .entry(keychain.clone())
                .and_modify(|last| *last = (*last).max(new_active_index))
                .or_insert(new_active_index);
        }

        {
            let mut scripts = ctx.subscribed_scripts.lock().await;
            let mut lookup = ctx.script_to_keychain_index.lock().await;

            scripts.extend(script_hashes);

            for (hash, index) in &scripts_to_subscribe {
                lookup.insert(*hash, (keychain.clone(), *index));
            }
        }

        {
            let mut subscriptions = ctx.script_subscriptions.lock().await;
            for (hash, _) in &scripts_to_subscribe {
                subscriptions
                    .entry(*hash)
                    .or_insert_with(|| ScriptSubscription::new(HashMap::new()));
            }
        }

        let new_max_subscribed = scripts_to_subscribe
            .iter()
            .map(|(_, index)| *index)
            .max()
            .unwrap_or(current_max);
        {
            let mut max_subscribed = ctx.max_subscribed_indices.lock().await;
            max_subscribed.insert(keychain, new_max_subscribed);
        }

        Ok(())
    }

    /// Process a script hash notification and return a TxUpdate if there are changes
    async fn process_script_hash_update<K>(
        &self,
        script_hash: ElectrumScriptHash,
        start_time: u64,
        fetch_prev_txouts: bool,
        ctx: &SubscriptionCtx<K>,
    ) -> Result<Option<SyncResponse<ConfirmationBlockTime>>, Error>
    where
        K: Ord + Clone,
    {
        // Look up the script from our tracker
        let subscriptions = ctx.script_subscriptions.lock().await;
        let Some(subscription) = subscriptions.get(&script_hash).cloned() else {
            // Not tracking this script, ignore
            return Ok(None);
        };
        drop(subscriptions);

        let expected_tx_heights: HashMap<Txid, i64> = subscription.expected_tx_heights;

        // Fetch current history for this script
        let history = self.inner.script_get_history(script_hash).await?;
        let current_tx_heights: HashMap<Txid, i64> = history
            .iter()
            .map(|tx| (tx.txid(), tx.electrum_height()))
            .collect();

        // Check if there are any changes (new/evicted txs or status transitions).
        if current_tx_heights == expected_tx_heights {
            return Ok(None);
        }

        let expected_txids: HashSet<Txid> = expected_tx_heights.keys().copied().collect();
        let current_txids: HashSet<Txid> = current_tx_heights.keys().copied().collect();
        let mut next_expected_tx_heights = current_tx_heights.clone();

        // Build a TxUpdate with the changes
        let mut tx_update: TxUpdate<ConfirmationBlockTime> = TxUpdate::default();

        // Mark evicted transactions
        for evicted_txid in expected_txids.difference(&current_txids) {
            tx_update.evicted_ats.insert((*evicted_txid, start_time));
        }

        // Fetch new transactions
        for tx_res in history {
            let txid = tx_res.txid();
            let electrum_height = tx_res.electrum_height();
            let previous_height = expected_tx_heights.get(&txid).copied();
            let is_new_tx = previous_height.is_none();
            let status_changed = previous_height.is_some_and(|h| h != electrum_height);

            if is_new_tx {
                let tx: Arc<Transaction> = self.fetch_tx(txid).await?;
                tx_update.txs.push(tx);
            }

            if is_new_tx || status_changed {
                let anchored = match electrum_height.try_into() {
                    Ok(height) if height > 0 => {
                        self.validate_merkle_for_anchor(&mut tx_update, txid, height)
                            .await?
                    }
                    _ => false,
                };
                apply_confirmation_tracking(
                    txid,
                    start_time,
                    electrum_height,
                    anchored,
                    &mut tx_update,
                    &mut next_expected_tx_heights,
                );
            }
        }

        // Fetch previous `TxOut`s for fee calculation if flag is enabled.
        if fetch_prev_txouts {
            self.fetch_prev_txout(&mut tx_update).await?;
        }

        // Update our tracker with the new expected txids
        let mut subscriptions = ctx.script_subscriptions.lock().await;
        if let Some(subscription) = subscriptions.get_mut(&script_hash) {
            subscription.expected_tx_heights = next_expected_tx_heights;
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
    /// All scanned script hashes are subscribed (including lookahead gap scripts) and returned.
    #[allow(clippy::too_many_arguments)]
    async fn populate_with_spks<K>(
        &self,
        wallet_label: &str,
        start_time: u64,
        tx_update: &mut TxUpdate<ConfirmationBlockTime>,
        mut spks_with_expected_txids: impl Iterator<Item = (u32, SpkWithExpectedTxids)>,
        stop_gap: usize,
        batch_size: usize,
        keychain: &K,
    ) -> Result<SpkScanResult, Error>
    where
        K: Display,
    {
        let mut unused_spk_count = 0_usize;
        let mut result = SpkScanResult {
            last_active_index: None,
            subscribed_hashes: HashSet::new(),
            subscribed_script_subscriptions: HashMap::new(),
            subscribed_script_to_index: HashMap::new(),
            max_subscribed_index: None,
        };

        loop {
            let spks = (0..batch_size)
                .map_while(|_| spks_with_expected_txids.next())
                .collect::<Vec<_>>();

            if spks.is_empty() {
                return Ok(result);
            }

            if let (Some((start_index, _)), Some((end_index, _))) = (spks.first(), spks.last()) {
                util::log_scan_range(wallet_label, keychain, *start_index, *end_index);
            }

            let spk_histories = self
                .inner
                .batch_script_get_history(spks.iter().map(|(_, s)| s.spk.as_script()))
                .await?;

            let mut scripts_to_subscribe: Vec<(ElectrumScriptHash, u32, ScriptSubscription)> =
                Vec::new();
            let mut should_stop = false;
            let mut batch_loaded_indexes: BTreeSet<u32> = BTreeSet::new();

            for ((spk_index, spk), spk_history) in spks.into_iter().zip(spk_histories) {
                let mut spk_history_heights: HashMap<Txid, i64> = spk_history
                    .iter()
                    .map(|res| (res.txid(), res.electrum_height()))
                    .collect();
                let spk_history_txids: HashSet<Txid> =
                    spk_history_heights.keys().copied().collect();
                let script_hash = ElectrumScriptHash::new(&spk.spk);
                result.max_subscribed_index = Some(
                    result
                        .max_subscribed_index
                        .map_or(spk_index, |max| max.max(spk_index)),
                );

                if spk_history.is_empty() {
                    unused_spk_count = unused_spk_count.saturating_add(1);
                    if unused_spk_count >= stop_gap {
                        should_stop = true;
                    }
                } else {
                    result.last_active_index = Some(spk_index);
                    unused_spk_count = 0;
                    batch_loaded_indexes.insert(spk_index);
                }

                tx_update.evicted_ats.extend(
                    spk.expected_txids
                        .difference(&spk_history_txids)
                        .map(|&txid| (txid, start_time)),
                );

                for tx_res in spk_history {
                    let txid = tx_res.txid();
                    let electrum_height = tx_res.electrum_height();
                    let tx = self.fetch_tx(txid).await?;
                    tx_update.txs.push(tx);
                    let anchored = match electrum_height.try_into() {
                        // Returned heights 0 & -1 are reserved for unconfirmed txs.
                        Ok(height) if height > 0 => {
                            self.validate_merkle_for_anchor(tx_update, txid, height)
                                .await?
                        }
                        _ => false,
                    };
                    apply_confirmation_tracking(
                        txid,
                        start_time,
                        electrum_height,
                        anchored,
                        tx_update,
                        &mut spk_history_heights,
                    );
                }

                scripts_to_subscribe.push((
                    script_hash,
                    spk_index,
                    ScriptSubscription::new(spk_history_heights),
                ));
            }

            util::log_loading_indexes(wallet_label, keychain, batch_loaded_indexes);

            if !scripts_to_subscribe.is_empty() {
                self.subscribe_scripts(
                    &scripts_to_subscribe,
                    &mut result.subscribed_hashes,
                    &mut result.subscribed_script_subscriptions,
                )
                .await?;
                for (hash, index, _) in &scripts_to_subscribe {
                    result.subscribed_script_to_index.insert(*hash, *index);
                }
            }

            if should_stop {
                return Ok(result);
            }
        }
    }

    /// Subscribe to a batch of scripts via Electrum and update the subscription tracker.
    ///
    /// This method subscribes via Electrum first and only updates local state after success.
    async fn subscribe_scripts(
        &self,
        scripts: &[(ElectrumScriptHash, u32, ScriptSubscription)],
        subscribed_hashes: &mut HashSet<ElectrumScriptHash>,
        script_subscriptions: &mut HashMap<ElectrumScriptHash, ScriptSubscription>,
    ) -> Result<(), Error> {
        self.inner
            .batch_script_hash_subscribe(scripts.iter().map(|(hash, _, _)| *hash))?;

        for (hash, _, subscription) in scripts {
            script_subscriptions.insert(*hash, subscription.clone());
            subscribed_hashes.insert(*hash);
        }

        Ok(())
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
    ) -> Result<bool, Error> {
        let merkle_res = match self
            .inner
            .transaction_get_merkle(txid, confirmation_height)
            .await
        {
            Ok(merkle) => merkle,
            Err(e) => {
                tracing::debug!(
                    txid = %txid,
                    confirmation_height,
                    error = %e,
                    "Could not fetch merkle proof for confirmed tx yet."
                );
                return Ok(false);
            }
        };

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
            is_confirmed_tx = util::validate_merkle_proof(&txid, &header.merkle_root, &merkle_res);
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
            return Ok(true);
        }

        tracing::debug!(
            txid = %txid,
            confirmation_height,
            "Merkle proof validation failed for confirmed tx."
        );
        Ok(false)
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
    wallet_label: &str,
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
        tracing::info!(
            wallet = %wallet_label,
            "Retrieving block headers.",
        );
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

fn apply_confirmation_tracking(
    txid: Txid,
    start_time: u64,
    electrum_height: i64,
    anchored: bool,
    tx_update: &mut TxUpdate<ConfirmationBlockTime>,
    next_expected_tx_heights: &mut HashMap<Txid, i64>,
) {
    if electrum_height > 0 {
        if !anchored {
            tx_update.seen_ats.insert((txid, start_time));
            next_expected_tx_heights.insert(txid, 0);
        }
    } else {
        tx_update.seen_ats.insert((txid, start_time));
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
/// This allows us to efficiently determine which keychain a transaction belongs to.
/// The map is built only for scripts that were actually subscribed.
#[cfg(test)]
fn build_reverse_lookup_map<K>(
    request: &mut FullScanRequest<K>,
    subscribed_scripts: &HashSet<ElectrumScriptHash>,
) -> HashMap<ElectrumScriptHash, (K, u32)>
where
    K: Ord + Clone,
{
    let mut script_to_keychain_index: HashMap<ElectrumScriptHash, (K, u32)> = HashMap::new();
    if subscribed_scripts.is_empty() {
        return script_to_keychain_index;
    }

    let mut remaining = subscribed_scripts.len();

    for keychain in request.keychains() {
        for (index, script) in request.iter_spks(keychain.clone()) {
            let script_hash = ElectrumScriptHash::new(script);
            if subscribed_scripts.contains(&script_hash)
                && !script_to_keychain_index.contains_key(&script_hash)
            {
                script_to_keychain_index.insert(script_hash, (keychain.clone(), index));
                remaining = remaining.saturating_sub(1);
                if remaining == 0 {
                    return script_to_keychain_index;
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
    use tokio_electrum::builder::ElectrumClientBuilder;
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
            wallet_label: String::from("test-wallet"),
            request: Mutex::new(request),
            subscribed_scripts: Mutex::new(HashSet::new()),
            script_subscriptions: Mutex::new(HashMap::new()),
            script_to_keychain_index: Mutex::new(HashMap::new()),
            last_active_indices: Mutex::new(BTreeMap::new()),
            max_subscribed_indices: Mutex::new(BTreeMap::new()),
            chain_tip: Mutex::new(None),
        }
    }

    async fn advance_request_cursor(
        ctx: &SubscriptionCtx<String>,
        keychain: &str,
        consumed: usize,
    ) {
        let mut request = ctx.request.lock().await;
        for _ in 0..consumed {
            let _ = request.next_spk(keychain.to_string());
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

    #[test]
    fn apply_confirmation_tracking_keeps_pending_when_anchor_missing() {
        let txid = txid("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let start_time = 42;
        let mut tx_update = TxUpdate::<ConfirmationBlockTime>::default();
        let mut next_expected = HashMap::from([(txid, 120_i64)]);

        apply_confirmation_tracking(
            txid,
            start_time,
            120,
            false,
            &mut tx_update,
            &mut next_expected,
        );

        assert!(tx_update.seen_ats.contains(&(txid, start_time)));
        assert_eq!(next_expected.get(&txid), Some(&0));
    }

    #[test]
    fn apply_confirmation_tracking_keeps_confirmed_when_anchor_is_present() {
        let txid = txid("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let start_time = 77;
        let mut tx_update = TxUpdate::<ConfirmationBlockTime>::default();
        let mut next_expected = HashMap::from([(txid, 200_i64)]);

        apply_confirmation_tracking(
            txid,
            start_time,
            200,
            true,
            &mut tx_update,
            &mut next_expected,
        );

        assert!(!tx_update.seen_ats.contains(&(txid, start_time)));
        assert_eq!(next_expected.get(&txid), Some(&200));
    }

    #[test]
    fn apply_confirmation_tracking_marks_unconfirmed_as_seen() {
        let txid = txid("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc");
        let start_time = 11;
        let mut tx_update = TxUpdate::<ConfirmationBlockTime>::default();
        let mut next_expected = HashMap::from([(txid, -1_i64)]);

        apply_confirmation_tracking(
            txid,
            start_time,
            -1,
            false,
            &mut tx_update,
            &mut next_expected,
        );

        assert!(tx_update.seen_ats.contains(&(txid, start_time)));
        assert_eq!(next_expected.get(&txid), Some(&-1));
    }

    #[tokio::test]
    async fn script_hashes_with_unconfirmed_txs_returns_only_pending_scripts() {
        let request = FullScanRequest::<String>::builder_at(0).build();
        let ctx = ctx_for_request(request);
        let hash_confirmed = ElectrumScriptHash::new(&script(0x31));
        let hash_pending_a = ElectrumScriptHash::new(&script(0x32));
        let hash_pending_b = ElectrumScriptHash::new(&script(0x33));

        let mut subscriptions = ctx.script_subscriptions.lock().await;
        subscriptions.insert(
            hash_confirmed,
            ScriptSubscription::new(HashMap::from([(
                txid("dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"),
                500_i64,
            )])),
        );
        subscriptions.insert(
            hash_pending_a,
            ScriptSubscription::new(HashMap::from([(
                txid("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
                0_i64,
            )])),
        );
        subscriptions.insert(
            hash_pending_b,
            ScriptSubscription::new(HashMap::from([(
                txid("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
                -1_i64,
            )])),
        );
        drop(subscriptions);

        let pending = ctx.script_hashes_with_unconfirmed_txs().await;
        let pending: HashSet<ElectrumScriptHash> = pending.into_iter().collect();
        assert!(pending.contains(&hash_pending_a));
        assert!(pending.contains(&hash_pending_b));
        assert!(!pending.contains(&hash_confirmed));
    }

    #[tokio::test]
    async fn handle_block_header_notification_returns_none_without_tip() {
        let request = FullScanRequest::<String>::builder_at(0).build();
        let ctx = SubscriptionCtx {
            wallet_label: String::from("test-wallet"),
            request: Mutex::new(request),
            subscribed_scripts: Mutex::new(HashSet::new()),
            script_subscriptions: Mutex::new(HashMap::new()),
            script_to_keychain_index: Mutex::new(HashMap::new()),
            last_active_indices: Mutex::new(BTreeMap::new()),
            max_subscribed_indices: Mutex::new(BTreeMap::new()),
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
            wallet_label: String::from("test-wallet"),
            request: Mutex::new(request),
            subscribed_scripts: Mutex::new(HashSet::new()),
            script_subscriptions: Mutex::new(HashMap::new()),
            script_to_keychain_index: Mutex::new(HashMap::new()),
            last_active_indices: Mutex::new(BTreeMap::new()),
            max_subscribed_indices: Mutex::new(BTreeMap::new()),
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
    fn build_reverse_lookup_map_tracks_only_subscribed_hashes() {
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
        let tracked = HashSet::from([
            ElectrumScriptHash::new(&script(0x52)),
            ElectrumScriptHash::new(&script(0x54)),
        ]);

        let lookup = build_reverse_lookup_map(&mut request, &tracked);
        assert_eq!(lookup.len(), tracked.len());
        assert_eq!(
            lookup.get(&ElectrumScriptHash::new(&script(0x52))),
            Some(&("external".to_string(), 1))
        );
        assert_eq!(
            lookup.get(&ElectrumScriptHash::new(&script(0x54))),
            Some(&("external".to_string(), 3))
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
        assert_eq!(ctx.extension_target(&hash1).await, None);
        assert_eq!(
            ctx.extension_target(&hash3).await,
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
        {
            let mut max_subscribed = ctx.max_subscribed_indices.lock().await;
            max_subscribed.insert("external".to_string(), 0);
        }
        advance_request_cursor(&ctx, "external", 1).await;

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
        {
            let mut max_subscribed = ctx.max_subscribed_indices.lock().await;
            max_subscribed.insert("external".to_string(), 1);
        }
        advance_request_cursor(&ctx, "external", 2).await;

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
    async fn handle_script_hash_notification_does_not_extend_without_update() {
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
        {
            let mut max_subscribed = ctx.max_subscribed_indices.lock().await;
            max_subscribed.insert("external".to_string(), 2);
        }
        advance_request_cursor(&ctx, "external", 3).await;

        let update = client
            .handle_script_hash_notification(trigger_hash, 0, false, 2, &ctx)
            .await;
        assert!(
            update.is_none(),
            "no tx tracker entry should yield no update"
        );

        let last_active = ctx.last_active_indices.lock().await;
        assert_eq!(last_active.get("external"), Some(&1));
        drop(last_active);

        let scripts = ctx.subscribed_scripts.lock().await;
        assert!(
            !scripts.contains(&ElectrumScriptHash::new(&script(0x54))),
            "stop-gap must not extend on unchanged script status"
        );
        assert!(
            !scripts.contains(&ElectrumScriptHash::new(&script(0x55))),
            "stop-gap must not extend on unchanged script status"
        );
    }

    #[tokio::test]
    async fn maybe_extend_after_script_update_extends_when_update_has_txs() {
        let client = test_bdk_client();
        let request = FullScanRequest::<String>::builder_at(0)
            .spks_for_keychain(
                "external".to_string(),
                vec![
                    (0, script(0x61)),
                    (1, script(0x62)),
                    (2, script(0x63)),
                    (3, script(0x64)),
                    (4, script(0x65)),
                ],
            )
            .build();
        let ctx = ctx_for_request(request);
        let trigger_hash = ElectrumScriptHash::new(&script(0x63));

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
        {
            let mut max_subscribed = ctx.max_subscribed_indices.lock().await;
            max_subscribed.insert("external".to_string(), 2);
        }
        advance_request_cursor(&ctx, "external", 3).await;

        let mut tx_update = TxUpdate::<ConfirmationBlockTime>::default();
        tx_update.txs.push(Arc::new(dummy_tx(0xAA)));
        let update = SyncResponse {
            tx_update,
            chain_update: None,
        };

        client
            .maybe_extend_after_script_update(trigger_hash, 2, &ctx, &update)
            .await
            .unwrap();

        let last_active = ctx.last_active_indices.lock().await;
        assert_eq!(last_active.get("external"), Some(&2));
        drop(last_active);

        let scripts = ctx.subscribed_scripts.lock().await;
        assert!(scripts.contains(&ElectrumScriptHash::new(&script(0x64))));
        assert!(scripts.contains(&ElectrumScriptHash::new(&script(0x65))));
    }

    #[tokio::test]
    async fn maybe_extend_after_script_update_does_not_extend_without_txs() {
        let client = test_bdk_client();
        let request = FullScanRequest::<String>::builder_at(0)
            .spks_for_keychain(
                "external".to_string(),
                vec![
                    (0, script(0x71)),
                    (1, script(0x72)),
                    (2, script(0x73)),
                    (3, script(0x74)),
                    (4, script(0x75)),
                ],
            )
            .build();
        let ctx = ctx_for_request(request);
        let trigger_hash = ElectrumScriptHash::new(&script(0x73));

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
        {
            let mut max_subscribed = ctx.max_subscribed_indices.lock().await;
            max_subscribed.insert("external".to_string(), 2);
        }
        advance_request_cursor(&ctx, "external", 3).await;

        let update = SyncResponse {
            tx_update: TxUpdate::<ConfirmationBlockTime>::default(),
            chain_update: Some(CheckPoint::new(BlockId {
                height: 1,
                hash: hash("0000000000000000000000000000000000000000000000000000000000000001"),
            })),
        };

        client
            .maybe_extend_after_script_update(trigger_hash, 2, &ctx, &update)
            .await
            .unwrap();

        let last_active = ctx.last_active_indices.lock().await;
        assert_eq!(
            last_active.get("external"),
            Some(&1),
            "last active index should remain unchanged when no txs were added"
        );
        drop(last_active);

        let scripts = ctx.subscribed_scripts.lock().await;
        assert!(
            !scripts.contains(&ElectrumScriptHash::new(&script(0x74))),
            "stop-gap must not extend when tx_update.txs is empty"
        );
        assert!(
            !scripts.contains(&ElectrumScriptHash::new(&script(0x75))),
            "stop-gap must not extend when tx_update.txs is empty"
        );
    }

    #[tokio::test]
    async fn process_script_hash_update_ignores_untracked_hash() {
        let client = test_bdk_client();
        let hash = ElectrumScriptHash::new(&script(0x61));
        let ctx = ctx_for_request(FullScanRequest::<String>::builder_at(0).build());

        let update = client
            .process_script_hash_update(hash, 0, false, &ctx)
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
        let (tip, latest) = fetch_tip_and_latest_blocks("test-wallet", &client, prev_tip.clone())
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

        let (tip, latest) = fetch_tip_and_latest_blocks("test-wallet", &client, wrong_prev_tip)
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

    async fn connected_bdk_client_with_notification_channel(
        env: &TestEnv,
        notification_channel_size: usize,
    ) -> BdkElectrumClient {
        let addr =
            ElectrumServerAddress::parse(&format!("tcp://{}", env.electrsd.electrum_url)).unwrap();
        let inner = ElectrumClientBuilder::new(addr)
            .notification_channel_size(notification_channel_size)
            .build();
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

        let mut stream = client.sync(request).await.unwrap();
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
    async fn sync_stream_catches_first_tx_for_address_without_history_at_start() {
        let env = TestEnv::new();
        ensure_funded_wallet(&env);
        let client = connected_bdk_client(&env).await;

        let unused_0 = env.bitcoind.client.new_address().unwrap();
        let unused_1 = env.bitcoind.client.new_address().unwrap();
        let request = FullScanRequest::<String>::builder_at(0)
            .spks_for_keychain(
                "external".to_string(),
                vec![(0, unused_0.script_pubkey()), (1, unused_1.script_pubkey())],
            )
            .build();

        let mut stream = client.sync(request).fetch_prev_txouts(true).await.unwrap();
        let first = stream
            .next()
            .await
            .expect("expected initial event")
            .expect("initial event should not error");
        match first {
            SubscribeEvent::Initial(initial) => {
                assert!(
                    initial.tx_update.txs.is_empty(),
                    "both scripts start unused in this test"
                );
            }
            other => panic!("expected initial event first, got: {:?}", other),
        }

        let new_txid = env
            .bitcoind
            .client
            .send_to_address(&unused_1, Amount::from_sat(25_000))
            .unwrap()
            .txid()
            .unwrap();
        env.electrsd.wait_tx(&new_txid);

        let saw_tx_update = timeout(Duration::from_secs(20), async {
            while let Some(event) = stream.next().await {
                match event {
                    Ok(SubscribeEvent::Update(update))
                        if update
                            .tx_update
                            .txs
                            .iter()
                            .any(|tx| tx.compute_txid() == new_txid) =>
                    {
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

        assert!(
            saw_tx_update,
            "expected first tx to previously-unused scanned script to be streamed"
        );
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

        let mut stream = client.sync(request).await.unwrap();
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

        let mut stream = client.sync(request).fetch_prev_txouts(true).await.unwrap();
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

        let mut stream = client.sync(request).await.unwrap();
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
    async fn sync_stream_marks_pending_tx_confirmed_after_new_block() {
        let env = TestEnv::new();
        ensure_funded_wallet(&env);
        let client = connected_bdk_client(&env).await;

        let tracked_address = env.bitcoind.client.new_address().unwrap();
        let request = FullScanRequest::<String>::builder_at(0)
            .chain_tip(current_tip_checkpoint(&env))
            .spks_for_keychain(
                "external".to_string(),
                vec![(0, tracked_address.script_pubkey())],
            )
            .build();

        let mut stream = client.sync(request).await.unwrap();
        let _ = stream
            .next()
            .await
            .expect("expected initial event")
            .expect("initial event should not error");

        let txid = env
            .bitcoind
            .client
            .send_to_address(&tracked_address, Amount::from_sat(22_000))
            .unwrap()
            .txid()
            .unwrap();
        env.electrsd.wait_tx(&txid);

        let saw_pending = timeout(Duration::from_secs(20), async {
            while let Some(event) = stream.next().await {
                match event {
                    Ok(SubscribeEvent::Update(update))
                        if update
                            .tx_update
                            .seen_ats
                            .iter()
                            .any(|(seen_txid, _)| *seen_txid == txid) =>
                    {
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
        assert!(
            saw_pending,
            "expected pending update for mempool transaction"
        );

        env.bitcoind
            .client
            .generate_to_address(1, &env.bitcoind.client.new_address().unwrap())
            .unwrap();
        let new_height = env.bitcoind.client.get_blockchain_info().unwrap().blocks as usize;
        env.electrsd.wait_height(new_height);

        let saw_confirmation_anchor = timeout(Duration::from_secs(20), async {
            while let Some(event) = stream.next().await {
                match event {
                    Ok(SubscribeEvent::Update(update))
                        if update
                            .tx_update
                            .anchors
                            .iter()
                            .any(|(_, anchor_txid)| *anchor_txid == txid) =>
                    {
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
        assert!(
            saw_confirmation_anchor,
            "expected confirmation anchor after transaction is mined"
        );
    }

    #[tokio::test]
    async fn sync_streams_isolate_non_overlapping_wallet_scripts() {
        let env = TestEnv::new();
        ensure_funded_wallet(&env);
        let client = connected_bdk_client(&env).await;

        let wallet_a_address = env.bitcoind.client.new_address().unwrap();
        let wallet_b_address = env.bitcoind.client.new_address().unwrap();

        let request_a = FullScanRequest::<String>::builder_at(0)
            .spks_for_keychain(
                "wallet-a".to_string(),
                vec![(0, wallet_a_address.script_pubkey())],
            )
            .build();
        let request_b = FullScanRequest::<String>::builder_at(0)
            .spks_for_keychain(
                "wallet-b".to_string(),
                vec![(0, wallet_b_address.script_pubkey())],
            )
            .build();

        let mut stream_a = client.sync(request_a).await.unwrap();
        let mut stream_b = client.sync(request_b).await.unwrap();

        let _ = stream_a
            .next()
            .await
            .expect("expected initial event for wallet A")
            .expect("initial event for wallet A should not error");
        let _ = stream_b
            .next()
            .await
            .expect("expected initial event for wallet B")
            .expect("initial event for wallet B should not error");

        let txid_a = env
            .bitcoind
            .client
            .send_to_address(&wallet_a_address, Amount::from_sat(30_000))
            .unwrap()
            .txid()
            .unwrap();
        env.electrsd.wait_tx(&txid_a);

        let wallet_a_saw_its_tx = timeout(Duration::from_secs(20), async {
            while let Some(event) = stream_a.next().await {
                match event {
                    Ok(SubscribeEvent::Update(update))
                        if update
                            .tx_update
                            .txs
                            .iter()
                            .any(|tx| tx.compute_txid() == txid_a) =>
                    {
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
        assert!(
            wallet_a_saw_its_tx,
            "wallet A should receive update for wallet A script"
        );

        let wallet_b_saw_wallet_a_tx = timeout(Duration::from_secs(3), async {
            while let Some(event) = stream_b.next().await {
                match event {
                    Ok(SubscribeEvent::Update(update))
                        if update
                            .tx_update
                            .txs
                            .iter()
                            .any(|tx| tx.compute_txid() == txid_a) =>
                    {
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
        assert!(
            !wallet_b_saw_wallet_a_tx,
            "wallet B must not receive wallet A transaction updates"
        );
    }

    #[tokio::test]
    async fn sync_streams_with_overlapping_scripts_both_receive_tx_update() {
        let env = TestEnv::new();
        ensure_funded_wallet(&env);
        let client = connected_bdk_client(&env).await;

        let shared_address = env.bitcoind.client.new_address().unwrap();

        let request_a = FullScanRequest::<String>::builder_at(0)
            .spks_for_keychain(
                "wallet-a".to_string(),
                vec![(0, shared_address.script_pubkey())],
            )
            .build();
        let request_b = FullScanRequest::<String>::builder_at(0)
            .spks_for_keychain(
                "wallet-b".to_string(),
                vec![(0, shared_address.script_pubkey())],
            )
            .build();

        let mut stream_a = client.sync(request_a).await.unwrap();
        let mut stream_b = client.sync(request_b).await.unwrap();

        let _ = stream_a
            .next()
            .await
            .expect("expected initial event for wallet A")
            .expect("initial event for wallet A should not error");
        let _ = stream_b
            .next()
            .await
            .expect("expected initial event for wallet B")
            .expect("initial event for wallet B should not error");

        let txid = env
            .bitcoind
            .client
            .send_to_address(&shared_address, Amount::from_sat(31_000))
            .unwrap()
            .txid()
            .unwrap();
        env.electrsd.wait_tx(&txid);

        let wallet_a_saw = timeout(Duration::from_secs(20), async {
            while let Some(event) = stream_a.next().await {
                match event {
                    Ok(SubscribeEvent::Update(update))
                        if update
                            .tx_update
                            .txs
                            .iter()
                            .any(|tx| tx.compute_txid() == txid) =>
                    {
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
        assert!(
            wallet_a_saw,
            "wallet A should receive update for shared script"
        );

        let wallet_b_saw = timeout(Duration::from_secs(20), async {
            while let Some(event) = stream_b.next().await {
                match event {
                    Ok(SubscribeEvent::Update(update))
                        if update
                            .tx_update
                            .txs
                            .iter()
                            .any(|tx| tx.compute_txid() == txid) =>
                    {
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
        assert!(
            wallet_b_saw,
            "wallet B should receive update for shared script"
        );
    }

    #[tokio::test]
    async fn sync_stream_handles_reorg_confirmed_pending_reconfirmed() {
        let env = TestEnv::new();
        ensure_funded_wallet(&env);
        let client = connected_bdk_client(&env).await;

        let tracked_address = env.bitcoind.client.new_address().unwrap();
        let request = FullScanRequest::<String>::builder_at(0)
            .chain_tip(current_tip_checkpoint(&env))
            .spks_for_keychain(
                "external".to_string(),
                vec![(0, tracked_address.script_pubkey())],
            )
            .build();

        let mut stream = client.sync(request).await.unwrap();
        let _ = stream
            .next()
            .await
            .expect("expected initial event")
            .expect("initial event should not error");

        let txid = env
            .bitcoind
            .client
            .send_to_address(&tracked_address, Amount::from_sat(32_000))
            .unwrap()
            .txid()
            .unwrap();
        env.electrsd.wait_tx(&txid);

        let saw_pending = timeout(Duration::from_secs(20), async {
            while let Some(event) = stream.next().await {
                match event {
                    Ok(SubscribeEvent::Update(update))
                        if update
                            .tx_update
                            .seen_ats
                            .iter()
                            .any(|(seen_txid, _)| *seen_txid == txid) =>
                    {
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
        assert!(
            saw_pending,
            "expected pending update before first confirmation"
        );

        env.bitcoind
            .client
            .generate_to_address(1, &env.bitcoind.client.new_address().unwrap())
            .unwrap();
        let first_confirmed_height =
            env.bitcoind.client.get_blockchain_info().unwrap().blocks as usize;
        env.electrsd.wait_height(first_confirmed_height);

        let first_anchor = timeout(Duration::from_secs(20), async {
            while let Some(event) = stream.next().await {
                match event {
                    Ok(SubscribeEvent::Update(update)) => {
                        if let Some((anchor, _)) = update
                            .tx_update
                            .anchors
                            .iter()
                            .find(|(_, anchor_txid)| *anchor_txid == txid)
                        {
                            return Some(anchor.block_id);
                        }
                    }
                    Ok(SubscribeEvent::Disconnected) => return None,
                    Err(_) => return None,
                    Ok(_) => {}
                }
            }
            None
        })
        .await
        .unwrap_or(None)
        .expect("expected first confirmation anchor");

        env.bitcoind
            .client
            .invalidate_block(first_anchor.hash)
            .expect("failed to invalidate tip block for reorg simulation");
        let reorg_height = env.bitcoind.client.get_blockchain_info().unwrap().blocks as usize;
        env.electrsd.wait_height(reorg_height);

        let saw_reorg_pending = timeout(Duration::from_secs(20), async {
            while let Some(event) = stream.next().await {
                match event {
                    Ok(SubscribeEvent::Update(update))
                        if update
                            .tx_update
                            .seen_ats
                            .iter()
                            .any(|(seen_txid, _)| *seen_txid == txid) =>
                    {
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
        assert!(
            saw_reorg_pending,
            "expected pending update after reorg invalidates prior confirmation"
        );

        env.bitcoind
            .client
            .generate_to_address(2, &env.bitcoind.client.new_address().unwrap())
            .unwrap();
        let reconfirmed_height = env.bitcoind.client.get_blockchain_info().unwrap().blocks as usize;
        env.electrsd.wait_height(reconfirmed_height);

        let second_anchor = timeout(Duration::from_secs(20), async {
            while let Some(event) = stream.next().await {
                match event {
                    Ok(SubscribeEvent::Update(update)) => {
                        if let Some((anchor, _)) = update
                            .tx_update
                            .anchors
                            .iter()
                            .find(|(_, anchor_txid)| *anchor_txid == txid)
                        {
                            return Some(anchor.block_id);
                        }
                    }
                    Ok(SubscribeEvent::Disconnected) => return None,
                    Err(_) => return None,
                    Ok(_) => {}
                }
            }
            None
        })
        .await
        .unwrap_or(None)
        .expect("expected second confirmation anchor");

        assert_ne!(
            first_anchor.hash, second_anchor.hash,
            "expected reconfirmed anchor to point at a different block hash after reorg"
        );
    }

    #[tokio::test]
    async fn sync_stream_reconnect_picks_up_confirmation_transition() {
        let env = TestEnv::new();
        ensure_funded_wallet(&env);
        let client = connected_bdk_client(&env).await;

        let tracked_address = env.bitcoind.client.new_address().unwrap();
        let request = FullScanRequest::<String>::builder_at(0)
            .chain_tip(current_tip_checkpoint(&env))
            .spks_for_keychain(
                "external".to_string(),
                vec![(0, tracked_address.script_pubkey())],
            )
            .build();

        let mut stream = client.sync(request).await.unwrap();
        let _ = stream
            .next()
            .await
            .expect("expected initial event")
            .expect("initial event should not error");

        let txid = env
            .bitcoind
            .client
            .send_to_address(&tracked_address, Amount::from_sat(33_000))
            .unwrap()
            .txid()
            .unwrap();
        env.electrsd.wait_tx(&txid);

        let saw_pending = timeout(Duration::from_secs(20), async {
            while let Some(event) = stream.next().await {
                match event {
                    Ok(SubscribeEvent::Update(update))
                        if update
                            .tx_update
                            .seen_ats
                            .iter()
                            .any(|(seen_txid, _)| *seen_txid == txid) =>
                    {
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
        assert!(saw_pending, "expected pending update before disconnect");

        client.disconnect();

        env.bitcoind
            .client
            .generate_to_address(1, &env.bitcoind.client.new_address().unwrap())
            .unwrap();
        let confirmed_height = env.bitcoind.client.get_blockchain_info().unwrap().blocks as usize;
        env.electrsd.wait_height(confirmed_height);

        let reconnected_client = connected_bdk_client(&env).await;
        let reconnect_request = FullScanRequest::<String>::builder_at(0)
            .chain_tip(current_tip_checkpoint(&env))
            .spks_for_keychain(
                "external".to_string(),
                vec![(0, tracked_address.script_pubkey())],
            )
            .build();
        let mut reconnect_stream = reconnected_client.sync(reconnect_request).await.unwrap();

        let initial = reconnect_stream
            .next()
            .await
            .expect("expected initial event after reconnect")
            .expect("initial event after reconnect should not error");
        match initial {
            SubscribeEvent::Initial(initial) => {
                assert!(
                    initial
                        .tx_update
                        .anchors
                        .iter()
                        .any(|(_, anchor_txid)| *anchor_txid == txid),
                    "expected reconnected scan to include confirmed anchor"
                );
            }
            other => panic!("expected initial event first, got: {:?}", other),
        }
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
        let mut stream = client.sync(request).await.unwrap();

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
        let mut stream = client.sync(request).await.unwrap();
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

        let mut stream = client.sync(request).fetch_prev_txouts(true).await.unwrap();
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

    #[tokio::test]
    async fn subscribe_scripts_does_not_mutate_state_on_subscription_failure() {
        let client = test_bdk_client();
        let mut subscribed_hashes = HashSet::new();
        let mut script_subscriptions = HashMap::new();
        let hash = ElectrumScriptHash::new(&script(0x71));
        let scripts = vec![(hash, 0, ScriptSubscription::new(HashMap::new()))];

        let mut saturated = false;
        for i in 0..10_000_u32 {
            let queued_hash = ElectrumScriptHash::new(&script((i % 251) as u8));
            if client.script_hash_subscribe(queued_hash).is_err() {
                saturated = true;
                break;
            }
        }
        assert!(
            saturated,
            "expected command queue saturation to force subscribe failure"
        );

        assert!(
            client
                .subscribe_scripts(&scripts, &mut subscribed_hashes, &mut script_subscriptions)
                .await
                .is_err()
        );
        assert!(
            subscribed_hashes.is_empty(),
            "local subscribed hash set must remain unchanged on failure"
        );
        assert!(
            script_subscriptions.is_empty(),
            "local script subscriptions must remain unchanged on failure"
        );
    }

    #[tokio::test]
    async fn sync_stream_batches_multiple_script_updates_into_single_event() {
        let env = TestEnv::new();
        ensure_funded_wallet(&env);
        let client = connected_bdk_client(&env).await;

        let addr_a = env.bitcoind.client.new_address().unwrap();
        let addr_b = env.bitcoind.client.new_address().unwrap();
        let request = FullScanRequest::<String>::builder_at(0)
            .spks_for_keychain(
                "external".to_string(),
                vec![(0, addr_a.script_pubkey()), (1, addr_b.script_pubkey())],
            )
            .build();
        let mut stream = client
            .sync(request)
            .batch_window(Duration::from_secs(4))
            .await
            .unwrap();

        let _ = stream
            .next()
            .await
            .expect("expected initial event")
            .expect("initial event should not error");

        let txid_a = env
            .bitcoind
            .client
            .send_to_address(&addr_a, Amount::from_sat(11_000))
            .unwrap()
            .txid()
            .unwrap();
        let txid_b = env
            .bitcoind
            .client
            .send_to_address(&addr_b, Amount::from_sat(12_000))
            .unwrap()
            .txid()
            .unwrap();
        env.electrsd.wait_tx(&txid_a);
        env.electrsd.wait_tx(&txid_b);

        let combined = timeout(Duration::from_secs(40), async {
            while let Some(event) = stream.next().await {
                match event {
                    Ok(SubscribeEvent::Update(update)) => {
                        let txids = update
                            .tx_update
                            .txs
                            .iter()
                            .map(|tx| tx.compute_txid())
                            .collect::<HashSet<_>>();
                        if txids.contains(&txid_a) || txids.contains(&txid_b) {
                            return txids.contains(&txid_a) && txids.contains(&txid_b);
                        }
                    }
                    Ok(SubscribeEvent::Disconnected) => return false,
                    Ok(SubscribeEvent::Initial(_)) => {}
                    Err(_) => return false,
                }
            }
            false
        })
        .await
        .unwrap_or(false);

        assert!(
            combined,
            "expected a single batched tx update containing both transactions"
        );
    }

    #[tokio::test]
    async fn sync_stream_batches_script_and_header_into_single_event() {
        let env = TestEnv::new();
        ensure_funded_wallet(&env);
        let client = connected_bdk_client(&env).await;

        let tracked_address = env.bitcoind.client.new_address().unwrap();
        let request = FullScanRequest::<String>::builder_at(0)
            .chain_tip(current_tip_checkpoint(&env))
            .spks_for_keychain(
                "external".to_string(),
                vec![(0, tracked_address.script_pubkey())],
            )
            .build();
        let mut stream = client
            .sync(request)
            .fetch_prev_txouts(true)
            .batch_window(Duration::from_secs(5))
            .await
            .unwrap();

        let _ = stream
            .next()
            .await
            .expect("expected initial event")
            .expect("initial event should not error");

        let txid = env
            .bitcoind
            .client
            .send_to_address(&tracked_address, Amount::from_sat(30_000))
            .unwrap()
            .txid()
            .unwrap();
        env.bitcoind
            .client
            .generate_to_address(1, &env.bitcoind.client.new_address().unwrap())
            .unwrap();
        env.electrsd.wait_tx(&txid);
        let new_height = env.bitcoind.client.get_blockchain_info().unwrap().blocks as usize;
        env.electrsd.wait_height(new_height);

        let mixed_update = timeout(Duration::from_secs(45), async {
            while let Some(event) = stream.next().await {
                match event {
                    Ok(SubscribeEvent::Update(update)) => {
                        let has_tx = update
                            .tx_update
                            .txs
                            .iter()
                            .any(|tx| tx.compute_txid() == txid);
                        if has_tx {
                            return update.chain_update.is_some();
                        }
                    }
                    Ok(SubscribeEvent::Disconnected) => return false,
                    Ok(SubscribeEvent::Initial(_)) => {}
                    Err(_) => return false,
                }
            }
            false
        })
        .await
        .unwrap_or(false);

        assert!(
            mixed_update,
            "expected script tx and chain tip update to be coalesced in one batch"
        );
    }

    #[tokio::test]
    async fn sync_stream_lag_recovery_catches_up_tracked_scripts() {
        let env = TestEnv::new();
        ensure_funded_wallet(&env);
        let client = connected_bdk_client_with_notification_channel(&env, 4).await;

        let tracked_address = env.bitcoind.client.new_address().unwrap();
        let request = FullScanRequest::<String>::builder_at(0)
            .chain_tip(current_tip_checkpoint(&env))
            .spks_for_keychain(
                "external".to_string(),
                vec![(0, tracked_address.script_pubkey())],
            )
            .build();
        let mut stream = client
            .sync(request)
            .batch_window(Duration::from_millis(300))
            .await
            .unwrap();

        let _ = stream
            .next()
            .await
            .expect("expected initial event")
            .expect("initial event should not error");

        let txid = env
            .bitcoind
            .client
            .send_to_address(&tracked_address, Amount::from_sat(22_000))
            .unwrap()
            .txid()
            .unwrap();
        env.bitcoind
            .client
            .generate_to_address(12, &env.bitcoind.client.new_address().unwrap())
            .unwrap();
        env.electrsd.wait_tx(&txid);
        let new_height = env.bitcoind.client.get_blockchain_info().unwrap().blocks as usize;
        env.electrsd.wait_height(new_height);

        let caught_up = timeout(Duration::from_secs(45), async {
            while let Some(event) = stream.next().await {
                match event {
                    Ok(SubscribeEvent::Update(update))
                        if update
                            .tx_update
                            .txs
                            .iter()
                            .any(|tx| tx.compute_txid() == txid) =>
                    {
                        return true;
                    }
                    Ok(SubscribeEvent::Update(_)) => {}
                    Ok(SubscribeEvent::Disconnected) => return false,
                    Ok(SubscribeEvent::Initial(_)) => {}
                    Err(_) => return false,
                }
            }
            false
        })
        .await
        .unwrap_or(false);

        assert!(
            caught_up,
            "expected lagged receiver to recover and include tracked tx via catch-up scan"
        );
    }

    #[tokio::test]
    async fn sync_stream_emits_pending_batch_before_disconnected() {
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
        let mut stream = client
            .sync(request)
            .batch_window(Duration::from_secs(2))
            .await
            .unwrap();

        let _ = stream
            .next()
            .await
            .expect("expected initial event")
            .expect("initial event should not error");
        let mut probe_notifications = client.notifications();

        env.bitcoind
            .client
            .generate_to_address(1, &env.bitcoind.client.new_address().unwrap())
            .unwrap();
        let new_height = env.bitcoind.client.get_blockchain_info().unwrap().blocks as usize;
        env.electrsd.wait_height(new_height);
        let saw_header_notification = timeout(Duration::from_secs(20), async {
            loop {
                match probe_notifications.recv().await {
                    Ok(ElectrumNotification::BlockHeader { .. }) => return true,
                    Ok(_) => {}
                    Err(_) => return false,
                }
            }
        })
        .await
        .unwrap_or(false);
        assert!(
            saw_header_notification,
            "expected to observe header notification before disconnect"
        );
        sleep(Duration::from_millis(100)).await;

        client.disconnect();

        let first = timeout(Duration::from_secs(20), stream.next())
            .await
            .expect("timed out waiting for first post-disconnect event")
            .expect("stream ended before first post-disconnect event")
            .expect("first post-disconnect event should not error");
        let second = timeout(Duration::from_secs(20), stream.next())
            .await
            .expect("timed out waiting for second post-disconnect event")
            .expect("stream ended before second post-disconnect event")
            .expect("second post-disconnect event should not error");

        assert!(
            matches!(first, SubscribeEvent::Update(ref u) if u.chain_update.is_some()),
            "expected pending chain update before disconnect marker"
        );
        assert!(
            matches!(second, SubscribeEvent::Disconnected),
            "expected disconnected marker after pending update"
        );
    }

    #[tokio::test]
    async fn subscribe_incremental_maintains_contiguous_stop_gap_window() {
        let client = test_bdk_client();

        let request = FullScanRequest::<String>::builder_at(0)
            .spks_for_keychain(
                "external".to_string(),
                vec![
                    (0, script(0x81)),
                    (1, script(0x82)),
                    (2, script(0x83)),
                    (3, script(0x84)),
                    (4, script(0x85)),
                    (5, script(0x86)),
                    (6, script(0x87)),
                    (7, script(0x88)),
                ],
            )
            .build();
        let ctx = ctx_for_request(request);

        {
            let mut scripts = ctx.subscribed_scripts.lock().await;
            let mut script_subscriptions = ctx.script_subscriptions.lock().await;
            let mut lookup = ctx.script_to_keychain_index.lock().await;
            for (index, tag) in [(0_u32, 0x81_u8), (1, 0x82), (2, 0x83)] {
                let hash = ElectrumScriptHash::new(&script(tag));
                scripts.insert(hash);
                lookup.insert(hash, ("external".to_string(), index));
                script_subscriptions.insert(hash, ScriptSubscription::new(HashMap::new()));
            }
        }
        {
            let mut last_active = ctx.last_active_indices.lock().await;
            last_active.insert("external".to_string(), 0);
        }
        {
            let mut max_subscribed = ctx.max_subscribed_indices.lock().await;
            max_subscribed.insert("external".to_string(), 2);
        }
        advance_request_cursor(&ctx, "external", 3).await;

        client
            .subscribe_incremental("external".to_string(), 3, 2, &ctx)
            .await
            .unwrap();
        client
            .subscribe_incremental("external".to_string(), 5, 2, &ctx)
            .await
            .unwrap();

        let lookup = ctx.script_to_keychain_index.lock().await;
        let mut seen = lookup
            .values()
            .filter_map(|(k, i)| (k == "external").then_some(*i))
            .collect::<Vec<_>>();
        seen.sort_unstable();
        assert_eq!(
            seen,
            vec![0, 1, 2, 3, 4, 5, 6, 7],
            "subscribed index coverage should stay contiguous with no holes"
        );
        drop(lookup);

        let max_subscribed = ctx.max_subscribed_indices.lock().await;
        assert_eq!(max_subscribed.get("external"), Some(&7));
    }
}

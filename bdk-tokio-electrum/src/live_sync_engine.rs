use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use bdk_core::bitcoin::block::Header;
use bdk_core::spk_client::{FullScanRequest, FullScanResponse, SyncResponse};
use bdk_core::{BlockId, CheckPoint, ConfirmationBlockTime, TxUpdate};
use futures_util::stream;
use tokio::sync::{Mutex, broadcast};
use tokio::time::{Instant, timeout_at};
use tokio_electrum::client::Error;
use tokio_electrum::notification::ElectrumNotification;
use tokio_electrum::prelude::{ElectrumScriptHash, ElectrumScriptStatus};

use crate::checkpoint::{ScriptSyncCheckpoint, SyncCheckpoint};
use crate::client::{BdkElectrumClient, SubscribeEvent, SubscribeStream};
use crate::constant::LIVE_SYNC_HISTORY_BATCH_SIZE;
use crate::subscription::{SubscriptionCtx, SubscriptionInit, SubscriptionState};
use crate::util::dedup_tx_update_txs;

#[derive(Default)]
struct PendingBatch {
    script_statuses: HashMap<ElectrumScriptHash, Option<ElectrumScriptStatus>>,
    latest_header: Option<(u32, Header)>,
    lagged: bool,
    reconnected: bool,
    terminal_disconnect: bool,
}

impl PendingBatch {
    fn has_work(&self) -> bool {
        self.lagged
            || self.reconnected
            || self.terminal_disconnect
            || !self.script_statuses.is_empty()
            || self.latest_header.is_some()
    }
}

struct LiveStreamState<K> {
    engine: LiveSyncEngine<K>,
    notification_rx: broadcast::Receiver<ElectrumNotification>,
    is_connected: bool,
    pending_checkpoint: Option<SyncCheckpoint<K>>,
    pending_terminal_disconnect: bool,
    done: bool,
}

#[derive(Clone)]
pub(crate) struct LiveSyncEngine<K> {
    pub(crate) client: BdkElectrumClient,
    pub(crate) ctx: Arc<SubscriptionCtx<K>>,
    pub(crate) start_time: u64,
    pub(crate) fetch_prev_txouts: bool,
    pub(crate) stop_gap: NonZeroU32,
    pub(crate) batch_window: Duration,
}

impl<K> LiveSyncEngine<K>
where
    K: Ord + Clone + Display + Send + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        client: BdkElectrumClient,
        wallet_label: String,
        start_time: u64,
        subscription_init: SubscriptionInit<K>,
        fetch_prev_txouts: bool,
        request: FullScanRequest<K>,
        response: &FullScanResponse<K>,
        stop_gap: NonZeroU32,
        batch_window: Duration,
    ) -> Self {
        let ctx = Arc::new(SubscriptionCtx {
            wallet_label,
            request: Mutex::new(request),
            state: Mutex::new(SubscriptionState {
                subscribed_scripts: subscription_init.subscribed_scripts,
                script_subscriptions: subscription_init.script_subscriptions,
                script_to_keychain_index: subscription_init.script_to_keychain_index,
                last_active_indices: response.last_active_indices.clone(),
                max_subscribed_indices: subscription_init.max_subscribed_indices,
            }),
            chain_tip: Mutex::new(response.chain_update.clone()),
        });

        Self {
            client,
            ctx,
            start_time,
            fetch_prev_txouts,
            stop_gap,
            batch_window,
        }
    }

    pub(crate) fn into_stream(
        self,
        notification_rx: broadcast::Receiver<ElectrumNotification>,
    ) -> SubscribeStream<K> {
        let initial_state = LiveStreamState {
            is_connected: self.client.status().is_connected(),
            engine: self,
            notification_rx,
            pending_checkpoint: None,
            pending_terminal_disconnect: false,
            done: false,
        };

        let stream = stream::unfold(initial_state, move |mut state| async move {
            if state.done {
                return None;
            }

            if let Some(checkpoint) = state.pending_checkpoint.take() {
                return Some((Ok(SubscribeEvent::Checkpoint(checkpoint)), state));
            }

            if state.pending_terminal_disconnect {
                state.pending_terminal_disconnect = false;
                state.done = true;
                return Some((Ok(SubscribeEvent::Disconnected), state));
            }

            loop {
                let mut batch = state
                    .engine
                    .next_pending_batch(&mut state.notification_rx, &mut state.is_connected)
                    .await;
                match state.engine.build_sync_update_from_batch(&mut batch).await {
                    Ok(Some((update, checkpoint))) => {
                        state.pending_checkpoint = Some(checkpoint);
                        state.pending_terminal_disconnect = batch.terminal_disconnect;
                        return Some((Ok(SubscribeEvent::Update(update)), state));
                    }
                    Ok(None) if batch.terminal_disconnect => {
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

    async fn next_pending_batch(
        &self,
        notification_rx: &mut broadcast::Receiver<ElectrumNotification>,
        is_connected: &mut bool,
    ) -> PendingBatch {
        let mut batch = PendingBatch::default();

        loop {
            let received = notification_rx.recv().await;
            self.collect_batch_notification(received, &mut batch, is_connected)
                .await;
            if batch.has_work() {
                break;
            }
        }

        if batch.terminal_disconnect || batch.reconnected {
            return batch;
        }

        let deadline: Instant = Instant::now() + self.batch_window;
        while let Ok(received) = timeout_at(deadline, notification_rx.recv()).await {
            self.collect_batch_notification(received, &mut batch, is_connected)
                .await;
            if batch.terminal_disconnect || batch.reconnected {
                break;
            }
        }

        batch
    }

    async fn collect_batch_notification(
        &self,
        received: Result<ElectrumNotification, broadcast::error::RecvError>,
        batch: &mut PendingBatch,
        is_connected: &mut bool,
    ) {
        match received {
            Ok(ElectrumNotification::ScriptHash { hash, status }) => {
                if self.ctx.has_script(&hash).await {
                    batch.script_statuses.insert(hash, status);
                }
            }
            Ok(ElectrumNotification::BlockHeader { height, header }) => {
                batch.latest_header = Some((height, header));
            }
            Ok(ElectrumNotification::ConnectionStatusChanged(status)) => {
                if status.is_connected() {
                    if !*is_connected {
                        tracing::info!(
                            wallet = %self.ctx.wallet_label,
                            "Electrum reconnected, starting incremental catch-up."
                        );
                        batch.reconnected = true;
                    }
                    *is_connected = true;
                } else {
                    *is_connected = false;
                }
            }
            Ok(ElectrumNotification::Shutdown) => {
                batch.terminal_disconnect = true;
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!(
                    wallet = %self.ctx.wallet_label,
                    "Subscription stream lagged by {} notifications, triggering catch-up batch",
                    n
                );
                batch.lagged = true;
            }
            Err(broadcast::error::RecvError::Closed) => {
                batch.terminal_disconnect = true;
            }
        }
    }

    async fn build_sync_update_from_batch(
        &self,
        batch: &mut PendingBatch,
    ) -> Result<Option<(SyncResponse<ConfirmationBlockTime>, SyncCheckpoint<K>)>, Error> {
        self.reconcile_after_reconnect(batch).await?;

        let chain_update = self.apply_chain_tip_update_from_batch(batch).await;
        let scripts_to_process = self
            .scripts_to_process_for_batch(batch, chain_update.is_some())
            .await;
        let tx_update = self
            .apply_script_updates_for_batch(scripts_to_process)
            .await?;

        if tx_update.is_empty() && chain_update.is_none() {
            return Ok(None);
        }

        tracing::info!(
            wallet = %self.ctx.wallet_label,
            tx_count = tx_update.txs.len(),
            anchor_count = tx_update.anchors.len(),
            seen_count = tx_update.seen_ats.len(),
            evicted_count = tx_update.evicted_ats.len(),
            has_chain_update = chain_update.is_some(),
            "Emitting realtime sync batch."
        );

        let checkpoint = self.current_checkpoint_snapshot().await;
        Ok(Some((
            SyncResponse {
                tx_update,
                chain_update,
            },
            checkpoint,
        )))
    }

    async fn reconcile_after_reconnect(&self, batch: &mut PendingBatch) -> Result<(), Error> {
        if !batch.reconnected {
            return Ok(());
        }

        match self.client.get_tip().await {
            Ok(tip) => {
                batch.latest_header = Some((tip.height, tip.header));
            }
            Err(e) if e.is_disconnected_like() => return Ok(()),
            Err(e) => return Err(e),
        }

        let tracked_hashes = self.ctx.tracked_script_hashes().await;
        if tracked_hashes.is_empty() {
            return Ok(());
        }

        match self
            .client
            .batch_script_hash_subscribe(tracked_hashes.iter().copied())
            .await
        {
            Ok(()) => Ok(()),
            Err(e) if e.is_disconnected_like() => Ok(()),
            Err(e) => Err(e),
        }
    }

    async fn apply_chain_tip_update_from_batch(
        &self,
        batch: &mut PendingBatch,
    ) -> Option<CheckPoint> {
        match batch.latest_header.take() {
            Some((height, header)) => {
                handle_block_header_notification(height, header, &self.ctx).await
            }
            None => None,
        }
    }

    async fn scripts_to_process_for_batch(
        &self,
        batch: &mut PendingBatch,
        include_pending_for_chain_tip: bool,
    ) -> Vec<ElectrumScriptHash> {
        let mut scripts_to_process: Vec<ElectrumScriptHash> = if batch.lagged {
            self.ctx.tracked_script_hashes().await
        } else {
            self.ctx
                .script_hashes_with_status_changes(&batch.script_statuses)
                .await
        };
        if include_pending_for_chain_tip {
            scripts_to_process.extend(self.ctx.script_hashes_with_unconfirmed_txs().await);
            let mut dedup = HashSet::with_capacity(scripts_to_process.len());
            scripts_to_process.retain(|hash| dedup.insert(*hash));
        }
        batch.lagged = false;
        scripts_to_process
    }

    async fn apply_script_updates_for_batch(
        &self,
        scripts_to_process: Vec<ElectrumScriptHash>,
    ) -> Result<TxUpdate<ConfirmationBlockTime>, Error> {
        let mut tx_update = TxUpdate::<ConfirmationBlockTime>::default();

        for chunk in scripts_to_process.chunks(LIVE_SYNC_HISTORY_BATCH_SIZE) {
            match self
                .client
                .process_script_hash_updates_batch(
                    chunk,
                    self.start_time,
                    self.fetch_prev_txouts,
                    &self.ctx,
                )
                .await
            {
                Ok(processed) => {
                    tx_update.extend(processed.tx_update);
                    for hash in processed.hashes_with_new_txs {
                        self.client
                            .maybe_extend_after_activity(hash, self.stop_gap, &self.ctx)
                            .await?;
                    }
                }
                Err(e) if e.is_disconnected_like() => {
                    break;
                }
                Err(e) => return Err(e),
            }
        }

        dedup_tx_update_txs(&mut tx_update);

        Ok(tx_update)
    }

    async fn current_checkpoint_snapshot(&self) -> SyncCheckpoint<K> {
        let chain_tip = self.ctx.chain_tip.lock().await.clone();
        let state = self.ctx.state.lock().await;
        let mut scripts: HashMap<ElectrumScriptHash, ScriptSyncCheckpoint<K>> =
            HashMap::with_capacity(state.script_subscriptions.len());

        for (hash, subscription) in &state.script_subscriptions {
            let Some((keychain, index)) = state.script_to_keychain_index.get(hash) else {
                continue;
            };

            scripts.insert(
                *hash,
                ScriptSyncCheckpoint {
                    keychain: keychain.clone(),
                    index: *index,
                    last_status: subscription.last_status,
                    expected_tx_heights: subscription.expected_tx_heights.clone(),
                },
            );
        }

        SyncCheckpoint {
            chain_tip,
            last_active_indices: state.last_active_indices.clone(),
            max_subscribed_indices: state.max_subscribed_indices.clone(),
            scripts,
        }
    }
}

/// Handle block header notification and update the chain tip
async fn handle_block_header_notification<K>(
    height: u32,
    header: Header,
    ctx: &SubscriptionCtx<K>,
) -> Option<CheckPoint>
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

            Some(new_tip)
        }
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use std::time::Duration;

    use bdk_core::bitcoin::Network;
    use bdk_core::bitcoin::constants::genesis_block;
    use bdk_core::spk_client::FullScanRequest;
    use tokio::sync::Mutex;
    use tokio_electrum::address::ElectrumServerAddress;
    use tokio_electrum::client::ElectrumClient;
    use tokio_electrum::status::ElectrumConnectionStatus;

    use super::*;
    use crate::subscription::SubscriptionState;

    fn make_ctx(chain_tip: Option<CheckPoint>) -> SubscriptionCtx<String> {
        let request: FullScanRequest<String> = FullScanRequest::builder_at(0).build();
        SubscriptionCtx {
            wallet_label: String::from("test-wallet"),
            request: Mutex::new(request),
            state: Mutex::new(SubscriptionState::default()),
            chain_tip: Mutex::new(chain_tip),
        }
    }

    fn make_engine(ctx: SubscriptionCtx<String>) -> LiveSyncEngine<String> {
        let addr = ElectrumServerAddress::parse("tcp://127.0.0.1:50001").unwrap();
        let client = BdkElectrumClient::new(Arc::new(ElectrumClient::builder(addr).build()));
        LiveSyncEngine {
            client,
            ctx: Arc::new(ctx),
            start_time: 0,
            fetch_prev_txouts: false,
            stop_gap: NonZeroU32::new(20).unwrap(),
            batch_window: Duration::from_millis(250),
        }
    }

    #[tokio::test]
    async fn handle_block_header_notification_returns_none_without_tip() {
        let genesis = genesis_block(Network::Regtest);
        let ctx = make_ctx(None);

        let update = handle_block_header_notification(1, genesis.header, &ctx).await;
        assert!(update.is_none());
    }

    #[tokio::test]
    async fn handle_block_header_notification_updates_chain_tip() {
        let genesis = genesis_block(Network::Regtest);
        let tip = CheckPoint::new(BlockId {
            height: 0,
            hash: genesis.block_hash(),
        });
        let ctx = make_ctx(Some(tip));

        let update = handle_block_header_notification(1, genesis.header, &ctx)
            .await
            .expect("expected update");
        assert_eq!(update.height(), 1);
    }

    #[tokio::test]
    async fn collect_batch_notification_marks_reconnect_only_on_transition() {
        let engine = make_engine(make_ctx(None));
        let mut batch = PendingBatch::default();
        let mut is_connected = false;

        engine
            .collect_batch_notification(
                Ok(ElectrumNotification::ConnectionStatusChanged(
                    ElectrumConnectionStatus::Connected,
                )),
                &mut batch,
                &mut is_connected,
            )
            .await;

        assert!(is_connected);
        assert!(batch.reconnected);
        assert!(!batch.terminal_disconnect);
    }

    #[tokio::test]
    async fn collect_batch_notification_ignores_transient_disconnect_for_stream_shutdown() {
        let engine = make_engine(make_ctx(None));
        let mut batch = PendingBatch::default();
        let mut is_connected = true;

        engine
            .collect_batch_notification(
                Ok(ElectrumNotification::ConnectionStatusChanged(
                    ElectrumConnectionStatus::Disconnected,
                )),
                &mut batch,
                &mut is_connected,
            )
            .await;

        assert!(!is_connected);
        assert!(!batch.reconnected);
        assert!(!batch.terminal_disconnect);
        assert!(!batch.has_work());
    }
}

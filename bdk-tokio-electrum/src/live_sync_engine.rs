use std::collections::HashSet;
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
use tokio_electrum::prelude::ElectrumScriptHash;

use crate::client::{BdkElectrumClient, SubscribeEvent, SubscribeStream};
use crate::constant::LIVE_SYNC_HISTORY_BATCH_SIZE;
use crate::subscription::{SubscriptionCtx, SubscriptionInit, SubscriptionState};
use crate::util::dedup_tx_update_txs;

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

struct LiveStreamState<K> {
    engine: LiveSyncEngine<K>,
    notification_rx: broadcast::Receiver<ElectrumNotification>,
    pending_disconnect: bool,
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
            engine: self,
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
                let mut batch = state
                    .engine
                    .next_pending_batch(&mut state.notification_rx)
                    .await;
                match state.engine.build_sync_update_from_batch(&mut batch).await {
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

    async fn next_pending_batch(
        &self,
        notification_rx: &mut broadcast::Receiver<ElectrumNotification>,
    ) -> PendingBatch {
        let mut batch = PendingBatch::default();

        loop {
            let received = notification_rx.recv().await;
            self.collect_batch_notification(received, &mut batch).await;
            if batch.disconnected || batch.has_work() {
                break;
            }
        }

        if batch.disconnected {
            return batch;
        }

        let deadline: Instant = Instant::now() + self.batch_window;
        while let Ok(received) = timeout_at(deadline, notification_rx.recv()).await {
            self.collect_batch_notification(received, &mut batch).await;
            if batch.disconnected {
                break;
            }
        }

        batch
    }

    async fn collect_batch_notification(
        &self,
        received: Result<ElectrumNotification, broadcast::error::RecvError>,
        batch: &mut PendingBatch,
    ) {
        match received {
            Ok(ElectrumNotification::ScriptHash { hash, .. }) => {
                if self.ctx.has_script(&hash).await {
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
                    wallet = %self.ctx.wallet_label,
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

    async fn build_sync_update_from_batch(
        &self,
        batch: &mut PendingBatch,
    ) -> Result<Option<SyncResponse<ConfirmationBlockTime>>, Error> {
        let chain_update = self.apply_chain_tip_update_from_batch(batch).await;
        let scripts_to_process = self
            .scripts_to_process_for_batch(batch, chain_update.is_some())
            .await;
        let tx_update = self
            .apply_script_updates_for_batch(scripts_to_process, batch)
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

        Ok(Some(SyncResponse {
            tx_update,
            chain_update,
        }))
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
            batch.dirty_scripts.drain().collect()
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
        batch: &mut PendingBatch,
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
                    batch.disconnected = true;
                    break;
                }
                Err(e) => return Err(e),
            }
        }

        dedup_tx_update_txs(&mut tx_update);

        Ok(tx_update)
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
    use bdk_core::bitcoin::Network;
    use bdk_core::bitcoin::constants::genesis_block;
    use bdk_core::spk_client::FullScanRequest;
    use tokio::sync::Mutex;

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
}

use std::collections::{BTreeMap, HashMap, HashSet};
use std::num::NonZeroUsize;
use std::ops::Deref;
use std::sync::Arc;

use bdk_core::bitcoin::block::Header;
use bdk_core::bitcoin::{BlockHash, OutPoint, Transaction, Txid};
use bdk_core::spk_client::{
    FullScanRequest, FullScanResponse, SpkWithExpectedTxids, SyncRequest, SyncResponse,
};
use bdk_core::{BlockId, CheckPoint, ConfirmationBlockTime, TxUpdate};
use tokio::sync::{Mutex, RwLock};
pub use tokio_electrum::client::{ElectrumClient, Error};
use tokio_electrum::types::{BlockHeader, ElectrumScriptHash, TransactionMerkel};

use crate::util;

/// We include a chain suffix of a certain length for robustness.
const CHAIN_SUFFIX_LENGTH: u32 = 8;

/// Wrapper around [`ElectrumClient`] with an internal in-memory cache.
#[derive(Debug, Clone)]
pub struct BdkElectrumClient {
    inner: Arc<ElectrumClient>,
    tx_cache: Arc<RwLock<HashMap<Txid, Arc<Transaction>>>>,
    block_header_cache: Arc<Mutex<HashMap<u32, Header>>>,
    anchor_cache: Arc<Mutex<HashMap<(Txid, BlockHash), ConfirmationBlockTime>>>,
}

impl Deref for BdkElectrumClient {
    type Target = ElectrumClient;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl BdkElectrumClient {
    /// Creates a new BDK electrum client.
    pub fn new(client: Arc<ElectrumClient>) -> Self {
        Self {
            inner: client,
            tx_cache: Default::default(),
            block_header_cache: Default::default(),
            anchor_cache: Default::default(),
        }
    }

    /// Insert anchors into the anchor cache so that the client will not re-fetch them.
    ///
    /// Typically used to pre-populate the cache from an existing `TxGraph`.
    pub async fn populate_anchor_cache(
        &self,
        tx_anchors: impl IntoIterator<Item = (Txid, impl IntoIterator<Item = ConfirmationBlockTime>)>,
    ) {
        let mut cache = self.anchor_cache.lock().await;
        for (txid, anchors) in tx_anchors {
            for anchor in anchors {
                cache.insert((txid, anchor.block_id.hash), anchor);
            }
        }
    }

    /// Inserts transactions into the cache so they do not need to be fetched again.
    pub async fn populate_tx_cache<I, T>(&self, txs: I)
    where
        I: IntoIterator<Item = T>,
        T: Into<Arc<Transaction>>,
    {
        let mut tx_cache = self.tx_cache.write().await;

        for tx in txs {
            let tx: Arc<Transaction> = tx.into();
            tx_cache.insert(tx.compute_txid(), tx);
        }
    }

    /// Fetches a transaction by id, using the local cache first.
    pub async fn fetch_tx(&self, txid: Txid) -> Result<Arc<Transaction>, Error> {
        {
            let tx_cache = self.tx_cache.read().await;
            if let Some(tx) = tx_cache.get(&txid) {
                return Ok(Arc::clone(tx));
            }
        }

        let tx = Arc::new(self.inner.transaction_get(txid).await?);
        let mut tx_cache = self.tx_cache.write().await;
        tx_cache.insert(txid, tx.clone());
        Ok(tx)
    }

    /// Full-scans keychain scripts against Electrum.
    pub async fn full_scan<K: Ord + Clone>(
        &self,
        request: impl Into<FullScanRequest<K>>,
        stop_gap: NonZeroUsize,
        batch_size: NonZeroUsize,
        fetch_prev_txouts: bool,
    ) -> Result<FullScanResponse<K>, Error> {
        let mut request: FullScanRequest<K> = request.into();
        let start_time = request.start_time();

        let tip_and_latest_blocks = match request.chain_tip() {
            Some(chain_tip) => Some(fetch_tip_and_latest_blocks(&self.inner, chain_tip).await?),
            None => None,
        };

        let mut tx_update = TxUpdate::<ConfirmationBlockTime>::default();
        let mut last_active_indices = BTreeMap::<K, u32>::default();
        let mut pending_anchors = Vec::new();

        for keychain in request.keychains() {
            let spks = request
                .iter_spks(keychain.clone())
                .map(|(spk_i, spk)| (spk_i, SpkWithExpectedTxids::from(spk)));

            let last_active_index: Option<u32> = self
                .populate_with_spks(
                    start_time,
                    &mut tx_update,
                    spks,
                    stop_gap,
                    batch_size,
                    &mut pending_anchors,
                )
                .await?;

            if let Some(last_active_index) = last_active_index {
                last_active_indices.insert(keychain.clone(), last_active_index);
            }
        }

        // Fetch previous `TxOut`s for fee calculation if flag is enabled.
        if fetch_prev_txouts {
            self.fetch_prev_txout(&mut tx_update).await?;
        }

        if !pending_anchors.is_empty() {
            let anchors = self
                .batch_fetch_anchors(&pending_anchors, batch_size)
                .await?;

            for (txid, anchor) in anchors {
                tx_update.anchors.insert((anchor, txid));
            }
        }

        let chain_update: Option<CheckPoint> =
            tip_and_latest_blocks.map(|(chain_tip, latest_blocks)| {
                chain_update(chain_tip, &latest_blocks, tx_update.anchors.iter().cloned())
            });

        Ok(FullScanResponse {
            tx_update,
            chain_update,
            last_active_indices,
        })
    }

    /// Poll-syncs known scripts/txids/outpoints against Electrum.
    pub async fn sync<I>(
        &self,
        request: impl Into<SyncRequest<I>>,
        batch_size: NonZeroUsize,
        fetch_prev_txouts: bool,
    ) -> Result<SyncResponse<ConfirmationBlockTime>, Error> {
        let mut request: SyncRequest<I> = request.into();
        let start_time = request.start_time();

        let tip_and_latest_blocks = match request.chain_tip() {
            Some(chain_tip) => Some(fetch_tip_and_latest_blocks(&self.inner, chain_tip).await?),
            None => None,
        };

        let mut tx_update = TxUpdate::<ConfirmationBlockTime>::default();
        let mut pending_anchors = Vec::new();
        self.populate_with_spks(
            start_time,
            &mut tx_update,
            request
                .iter_spks_with_expected_txids()
                .enumerate()
                .map(|(i, spk)| (i as u32, spk)),
            unsafe { NonZeroUsize::new_unchecked(usize::MAX) },
            batch_size,
            &mut pending_anchors,
        )
        .await?;
        self.populate_with_txids(
            start_time,
            &mut tx_update,
            request.iter_txids(),
            &mut pending_anchors,
        )
        .await?;
        self.populate_with_outpoints(
            start_time,
            &mut tx_update,
            request.iter_outpoints(),
            &mut pending_anchors,
        )
        .await?;

        // Fetch previous `TxOut`s for fee calculation if flag is enabled.
        if fetch_prev_txouts {
            self.fetch_prev_txout(&mut tx_update).await?;
        }

        if !pending_anchors.is_empty() {
            let anchors = self
                .batch_fetch_anchors(&pending_anchors, batch_size)
                .await?;

            for (txid, anchor) in anchors {
                tx_update.anchors.insert((anchor, txid));
            }
        }

        let chain_update: Option<CheckPoint> =
            tip_and_latest_blocks.map(|(chain_tip, latest_blocks)| {
                chain_update(chain_tip, &latest_blocks, tx_update.anchors.iter().cloned())
            });

        Ok(SyncResponse {
            tx_update,
            chain_update,
        })
    }

    async fn populate_with_spks(
        &self,
        start_time: u64,
        tx_update: &mut TxUpdate<ConfirmationBlockTime>,
        mut spks_with_expected_txids: impl Iterator<Item = (u32, SpkWithExpectedTxids)>,
        stop_gap: NonZeroUsize,
        batch_size: NonZeroUsize,
        pending_anchors: &mut Vec<(Txid, u32)>,
    ) -> Result<Option<u32>, Error> {
        let stop_gap: usize = stop_gap.get();
        let batch_size: usize = batch_size.get();

        let mut last_active_index: Option<u32> = None;
        let mut unused_spk_count: usize = 0;

        loop {
            let spks = (0..batch_size)
                .map_while(|_| spks_with_expected_txids.next())
                .collect::<Vec<_>>();

            if spks.is_empty() {
                return Ok(last_active_index);
            }

            let script_hashes = spks
                .iter()
                .map(|(_, spk)| ElectrumScriptHash::new(&spk.spk))
                .collect::<Vec<_>>();
            let spk_histories = self.inner.batch_script_get_history(script_hashes).await?;

            for ((spk_index, spk), spk_history) in spks.into_iter().zip(spk_histories) {
                if spk_history.is_empty() {
                    match unused_spk_count.checked_add(1) {
                        Some(i) if i < stop_gap => unused_spk_count = i,
                        _ => return Ok(last_active_index),
                    };
                } else {
                    last_active_index = Some(spk_index);
                    unused_spk_count = 0;
                }

                let spk_history_set: HashSet<Txid> =
                    spk_history.iter().map(|res| res.txid()).collect();

                tx_update.evicted_ats.extend(
                    spk.expected_txids
                        .difference(&spk_history_set)
                        .map(|&txid| (txid, start_time)),
                );

                for tx_entry in spk_history {
                    let txid = tx_entry.txid();
                    tx_update.txs.push(self.fetch_tx(txid).await?);

                    self.apply_history_height(
                        tx_update,
                        pending_anchors,
                        tx_entry.txid(),
                        tx_entry.electrum_height(),
                        start_time,
                    );
                }
            }
        }
    }

    async fn populate_with_outpoints(
        &self,
        start_time: u64,
        tx_update: &mut TxUpdate<ConfirmationBlockTime>,
        outpoints: impl IntoIterator<Item = OutPoint>,
        pending_anchors: &mut Vec<(Txid, u32)>,
    ) -> Result<(), Error> {
        // Collect valid outpoints with their corresponding `spk` and `tx`.
        let mut ops_spks_txs = Vec::new();

        for op in outpoints {
            if let Ok(tx) = self.fetch_tx(op.txid).await {
                if let Some(txout) = tx.output.get(op.vout as usize) {
                    ops_spks_txs.push((op, txout.script_pubkey.clone(), tx));
                }
            }
        }

        // Dedup `spk`s, batch-fetch all histories in one call, and store them in a map.
        let unique_spks: Vec<_> = ops_spks_txs
            .iter()
            .map(|(_, spk, _)| spk.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let histories = self
            .inner
            .batch_script_get_history(unique_spks.iter().map(|spk| spk.as_script()))
            .await?;
        let mut spk_map = HashMap::new();
        for (spk, history) in unique_spks.into_iter().zip(histories.into_iter()) {
            spk_map.insert(spk, history);
        }

        for (outpoint, spk, tx) in ops_spks_txs {
            if let Some(spk_history) = spk_map.get(&spk) {
                let mut has_residing = false; // tx in which the outpoint resides
                let mut has_spending = false; // tx that spends the outpoint

                for res in spk_history {
                    if has_residing && has_spending {
                        break;
                    }

                    if !has_residing && res.txid() == outpoint.txid {
                        has_residing = true;
                        tx_update.txs.push(Arc::clone(&tx));

                        self.apply_history_height(
                            tx_update,
                            pending_anchors,
                            res.txid(),
                            res.electrum_height(),
                            start_time,
                        );
                    }

                    if !has_spending && res.txid() != outpoint.txid {
                        let res_tx = self.fetch_tx(res.txid()).await?;
                        // we exclude txs/anchors that do not spend our specified outpoint(s)
                        has_spending = res_tx
                            .input
                            .iter()
                            .any(|txin| txin.previous_output == outpoint);
                        if !has_spending {
                            continue;
                        }
                        tx_update.txs.push(Arc::clone(&res_tx));

                        self.apply_history_height(
                            tx_update,
                            pending_anchors,
                            res.txid(),
                            res.electrum_height(),
                            start_time,
                        );
                    }
                }
            }
        }

        Ok(())
    }

    async fn populate_with_txids(
        &self,
        start_time: u64,
        tx_update: &mut TxUpdate<ConfirmationBlockTime>,
        txids: impl IntoIterator<Item = Txid>,
        pending_anchors: &mut Vec<(Txid, u32)>,
    ) -> Result<(), Error> {
        let mut txs = Vec::<(Txid, Arc<Transaction>)>::new();
        let mut scripts = Vec::new();

        for txid in txids {
            let tx = self.fetch_tx(txid).await?;

            if let Some(first_output) = tx.output.first() {
                scripts.push(ElectrumScriptHash::new(&first_output.script_pubkey));
                txs.push((txid, tx));
            }
        }

        let spk_histories = self.inner.batch_script_get_history(scripts).await?;

        for ((txid, tx), spk_history) in txs.into_iter().zip(spk_histories) {
            if let Some(res) = spk_history.into_iter().find(|res| res.txid() == txid) {
                self.apply_history_height(
                    tx_update,
                    pending_anchors,
                    res.txid(),
                    res.electrum_height(),
                    start_time,
                );
            }

            tx_update.txs.push(tx);
        }

        Ok(())
    }

    fn apply_history_height(
        &self,
        tx_update: &mut TxUpdate<ConfirmationBlockTime>,
        pending_anchors: &mut Vec<(Txid, u32)>,
        txid: Txid,
        electrum_height: i64,
        start_time: u64,
    ) {
        match electrum_height.try_into() {
            // Returned heights 0 & -1 are reserved for unconfirmed txs.
            Ok(height) if height > 0 => {
                pending_anchors.push((txid, height));
            }
            _ => {
                tx_update.seen_ats.insert((txid, start_time));
            }
        }
    }

    /// Batch validate Merkle proofs, cache each confirmation anchor, and return them.
    async fn batch_fetch_anchors(
        &self,
        txs_with_heights: &[(Txid, u32)],
        batch_size: NonZeroUsize,
    ) -> Result<Vec<(Txid, ConfirmationBlockTime)>, Error> {
        let batch_size: usize = batch_size.get();

        let mut results = Vec::with_capacity(txs_with_heights.len());
        let mut to_fetch = Vec::new();

        // Figure out which block heights we need headers for.
        let mut needed_heights: Vec<u32> = txs_with_heights.iter().map(|&(_, h)| h).collect();
        needed_heights.sort_unstable();
        needed_heights.dedup();

        let mut height_to_hash = HashMap::with_capacity(needed_heights.len());

        // Collect headers of missing heights, and build `height_to_hash` map.
        {
            let mut cache = self.block_header_cache.lock().await;

            let mut missing_heights = Vec::new();
            for &height in &needed_heights {
                if let Some(header) = cache.get(&height) {
                    height_to_hash.insert(height, header.block_hash());
                } else {
                    missing_heights.push(height);
                }
            }

            if !missing_heights.is_empty() {
                for heights_chunk in missing_heights.chunks(batch_size) {
                    let headers: Vec<Header> = match self
                        .inner
                        .batch_block_header(heights_chunk.iter().copied())
                        .await
                    {
                        Ok(headers) => headers,
                        Err(error) => {
                            tracing::warn!(
                                error = %error,
                                batch_size = heights_chunk.len(),
                                "Batch header fetch failed; retrying with single requests."
                            );

                            let mut headers: Vec<Header> = Vec::with_capacity(heights_chunk.len());

                            for height in heights_chunk.iter().copied() {
                                match self.inner.block_header(height).await {
                                    Ok(header) => headers.push(header),
                                    Err(error) => {
                                        tracing::warn!(
                                            height,
                                            error = %error,
                                            "Skipping header fetch for anchor validation."
                                        );
                                    }
                                }
                            }

                            headers
                        }
                    };

                    for (height, header) in heights_chunk.iter().copied().zip(headers) {
                        height_to_hash.insert(height, header.block_hash());
                        cache.insert(height, header);
                    }
                }
            }
        }

        // Check our anchor cache and queue up any proofs we still need.
        {
            let anchor_cache = self.anchor_cache.lock().await;
            for &(txid, height) in txs_with_heights {
                let Some(hash) = height_to_hash.get(&height).copied() else {
                    tracing::warn!(
                        txid = %txid,
                        height,
                        "Skipping anchor validation because block header is missing."
                    );
                    continue;
                };

                if let Some(anchor) = anchor_cache.get(&(txid, hash)) {
                    results.push((txid, *anchor));
                } else {
                    to_fetch.push((txid, height));
                }
            }
        }

        // Fetch merkle proofs in conservative chunks and fallback to single-request mode if a
        // chunk fails. Some public servers disconnect on large/expensive proof batches.
        for tx_chunk in to_fetch.chunks(batch_size) {
            let chunk_proofs: Vec<(Txid, u32, Option<TransactionMerkel>)> = match self
                .inner
                .batch_transaction_get_merkle(tx_chunk.iter().copied())
                .await
            {
                Ok(proofs) => tx_chunk
                    .iter()
                    .copied()
                    .zip(proofs.into_iter().map(Some))
                    .map(|((txid, height), proof)| (txid, height, proof))
                    .collect(),
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        batch_size = tx_chunk.len(),
                        "Batch merkle fetch failed; retrying with single requests."
                    );

                    let mut proofs = Vec::with_capacity(tx_chunk.len());
                    for (txid, height) in tx_chunk.iter().copied() {
                        match self.inner.transaction_get_merkle(txid, height).await {
                            Ok(proof) => proofs.push((txid, height, Some(proof))),
                            Err(error) => {
                                tracing::warn!(
                                    txid = %txid,
                                    height,
                                    error = %error,
                                    "Skipping merkle proof fetch for anchor validation."
                                );
                                proofs.push((txid, height, None));
                            }
                        }
                    }

                    proofs
                }
            };

            // Validate each proof, retrying once for each stale header.
            for (txid, height, maybe_proof) in chunk_proofs {
                let Some(proof) = maybe_proof else {
                    continue;
                };

                let mut header: Header = {
                    let cache = self.block_header_cache.lock().await;
                    cache
                        .get(&(height))
                        .copied()
                        .expect("header already fetched above")
                };

                let mut valid: bool =
                    util::validate_merkle_proof(&txid, &header.merkle_root, &proof);

                if !valid {
                    match self.inner.block_header(height).await {
                        Ok(fresh_header) => {
                            header = fresh_header;

                            let mut cache = self.block_header_cache.lock().await;
                            cache.insert(height, header);

                            valid = util::validate_merkle_proof(&txid, &header.merkle_root, &proof);
                        }
                        Err(error) => {
                            tracing::debug!(
                                txid = %txid,
                                height,
                                error = %error,
                                "Skipping stale-header retry during anchor validation."
                            );
                            continue;
                        }
                    }
                }

                // Build and cache the anchor if merkle proof is valid.
                if valid {
                    let hash = header.block_hash();
                    let anchor = ConfirmationBlockTime {
                        confirmation_time: header.time as u64,
                        block_id: BlockId { height, hash },
                    };

                    let mut anchor_cache = self.anchor_cache.lock().await;
                    anchor_cache.insert((txid, hash), anchor);

                    results.push((txid, anchor));
                }
            }
        }

        Ok(results)
    }

    async fn fetch_prev_txout(
        &self,
        tx_update: &mut TxUpdate<ConfirmationBlockTime>,
    ) -> Result<(), Error> {
        let mut seen = HashSet::<Txid>::new();

        for tx in &tx_update.txs {
            let txid = tx.compute_txid();

            if tx.is_coinbase() || !seen.insert(txid) {
                continue;
            }

            for vin in &tx.input {
                let outpoint = vin.previous_output;
                let prev_tx = self.fetch_tx(outpoint.txid).await?;
                let vout = outpoint.vout as usize;

                match prev_tx.output.get(vout) {
                    Some(txout) => {
                        tx_update.txouts.insert(outpoint, txout.clone());
                    }
                    None => {
                        tracing::warn!(
                            txid = %outpoint.txid,
                            vout,
                            "Skipping prevout fetch: vout out of bounds"
                        );
                    }
                }
            }
        }

        Ok(())
    }
}

async fn fetch_tip_and_latest_blocks(
    client: &ElectrumClient,
    prev_tip: CheckPoint,
) -> Result<(CheckPoint, BTreeMap<u32, BlockHash>), Error> {
    let BlockHeader {
        height: new_tip_height,
        ..
    } = client.get_tip().await?;

    if new_tip_height < prev_tip.height() {
        return Ok((prev_tip, BTreeMap::new()));
    }

    let mut latest_blocks = {
        let start_height = new_tip_height.saturating_sub(CHAIN_SUFFIX_LENGTH - 1);
        let headers = client
            .block_headers(start_height, CHAIN_SUFFIX_LENGTH as usize)
            .await?
            .headers;
        let hashes = headers.into_iter().map(|header| header.block_hash());
        (start_height..)
            .zip(hashes)
            .collect::<BTreeMap<u32, BlockHash>>()
    };

    let mut agreement_cp = None::<CheckPoint>;
    for cp in prev_tip.iter() {
        let block_id = cp.block_id();
        let hash = match latest_blocks.get(&block_id.height) {
            Some(hash) => *hash,
            None => {
                let header = client.block_header(block_id.height).await?;
                let hash = header.block_hash();
                latest_blocks.insert(block_id.height, hash);
                hash
            }
        };

        if hash == block_id.hash {
            agreement_cp = Some(cp);
            break;
        }
    }

    let agreement_height = agreement_cp.as_ref().map(CheckPoint::height);
    let new_tip = latest_blocks
        .iter()
        .filter(|(height, _)| Some(**height) > agreement_height)
        .map(|(height, hash)| BlockId {
            height: *height,
            hash: *hash,
        })
        .fold(agreement_cp, |prev, block| {
            Some(match prev {
                Some(cp) => cp.insert(block),
                None => CheckPoint::new(block),
            })
        })
        .unwrap_or(prev_tip);

    Ok((new_tip, latest_blocks))
}

fn chain_update(
    mut tip: CheckPoint,
    latest_blocks: &BTreeMap<u32, BlockHash>,
    anchors: impl Iterator<Item = (ConfirmationBlockTime, Txid)>,
) -> CheckPoint {
    for (anchor, _) in anchors {
        let height = anchor.block_id.height;

        if tip.get(height).is_none() && height <= tip.height() {
            let hash = latest_blocks
                .get(&height)
                .copied()
                .unwrap_or(anchor.block_id.hash);
            tip = tip.insert(BlockId { hash, height });
        }
    }

    tip
}

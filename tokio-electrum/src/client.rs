//! Electrum client

use std::collections::{BTreeMap, HashSet};
use std::io;
#[cfg(feature = "socks")]
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use bitcoin::block::Header;
use bitcoin::constants::ChainHash;
use bitcoin::{FeeRate, Network, Transaction, Txid};
use electrum_streaming_client::notification::Notification;
use electrum_streaming_client::request::{
    BroadcastTx, EstimateFee, Features as GetServerFeatures, GetHistory, GetTx, GetTxMerkle,
    Header as GetBlockHeader, Headers, HeadersSubscribe, Ping, ScriptHashSubscribe,
    ScriptHashUnsubscribe,
};
use electrum_streaming_client::response::{ServerFeatures, Tx};
use electrum_streaming_client::{
    AsyncBatchRequest, AsyncClient, AsyncEventReceiver, AsyncPendingRequest,
    AsyncPendingRequestTuple, AsyncRequestError, AsyncRequestSendError, BatchRequestError, Event,
    Request, ResponseError, SatisfiedRequest,
};
use futures::{StreamExt, future};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{Mutex, MutexGuard, Notify, RwLock, broadcast, mpsc};
use tokio::time;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::crypto::ring;
use tokio_rustls::rustls::pki_types::{InvalidDnsNameError, ServerName};
use tokio_rustls::rustls::{ClientConfig, RootCertStore};

use crate::address::{ElectrumServerAddress, HostAndPort, Scheme};
use crate::builder::{ElectrumClientBuilder, ElectrumConnectionMode};
use crate::constant::PING_INTERVAL;
use crate::notification::ElectrumNotification;
#[cfg(feature = "socks")]
use crate::socks::TcpSocks5Stream;
use crate::status::{AtomicElectrumConnectionStatus, ElectrumConnectionStatus};
use crate::types::{BlockHeader, BlockHeaders, ElectrumScriptHash, TransactionMerkel};

type BoxReadStream = Box<dyn AsyncRead + Send + Unpin>;
type BoxWriteStream = Box<dyn AsyncWrite + Send + Unpin>;

/// Electrum client error
#[derive(Debug, Error)]
pub enum Error {
    /// I/O error
    #[error(transparent)]
    Io(#[from] io::Error),
    /// Invalid DNS name
    #[error(transparent)]
    InvalidDnsName(#[from] InvalidDnsNameError),
    /// Electrum async send request error
    #[error(transparent)]
    AsyncRequestSend(#[from] AsyncRequestSendError),
    /// Batch request error
    #[error(transparent)]
    BatchRequest(#[from] BatchRequestError),
    /// Electrum async request error
    #[error(transparent)]
    AsyncRequest(#[from] AsyncRequestError),
    /// Response error
    #[error(transparent)]
    Response(#[from] ResponseError),
    /// Socks error
    #[error(transparent)]
    #[cfg(feature = "socks")]
    Socks(#[from] tokio_socks::Error),
    /// MPSC try send error
    #[error("{0}")]
    MpscTrySend(String),
    /// Network mismatch
    #[error("the server network does not match the expected network")]
    NetworkMismatch,
    /// Timeout
    #[error("timeout")]
    Timeout,
    /// Disconnected
    #[error("disconnected")]
    Disconnected,
    /// Termination request
    #[error("termination request")]
    TerminationRequest,
}

#[derive(Debug, Clone, Copy)]
struct Config {
    connection_mode: ElectrumConnectionMode,
    connection_timeout: Duration,
    request_timeout: Duration,
    expected_network: Option<Network>,
}

#[derive(Debug)]
struct Channels {
    commands: (
        Sender<AsyncBatchRequest>,
        Mutex<Receiver<AsyncBatchRequest>>,
    ),
    ping: Notify,
    disconnected: Notify,
    terminate: Notify,
}

impl Channels {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(4096);
        let rx = Mutex::new(rx);

        Self {
            commands: (tx, rx),
            ping: Notify::new(),
            disconnected: Notify::new(),
            terminate: Notify::new(),
        }
    }

    #[inline]
    pub async fn rx_batch_requests(&self) -> MutexGuard<'_, Receiver<AsyncBatchRequest>> {
        self.commands.1.lock().await
    }

    #[inline]
    pub fn ping(&self) {
        self.ping.notify_one()
    }

    #[inline]
    pub fn disconnected(&self) {
        self.disconnected.notify_waiters()
    }

    #[inline]
    pub fn terminate(&self) {
        self.terminate.notify_one()
    }
}

#[derive(Debug, Default)]
struct ServicesTracker {
    network_mismatch: AtomicBool,
    headers_subscribed: AtomicBool,
    script_hashes: RwLock<HashSet<ElectrumScriptHash>>,
}

impl ServicesTracker {
    #[inline]
    fn network_mismatch(&self) -> bool {
        self.network_mismatch.load(Ordering::SeqCst)
    }

    #[inline]
    fn set_network_mismatch(&self, value: bool) {
        self.network_mismatch.store(value, Ordering::SeqCst);
    }

    #[inline]
    fn is_headers_subscribed(&self) -> bool {
        self.headers_subscribed.load(Ordering::SeqCst)
    }

    #[inline]
    fn set_headers_subscribed(&self, value: bool) {
        self.headers_subscribed.store(value, Ordering::SeqCst);
    }

    async fn reset(&self) {
        // Reset headers subscription
        self.set_headers_subscribed(false);

        // Reset script hashes
        let mut script_hashes = self.script_hashes.write().await;
        script_hashes.clear();
    }
}

#[derive(Debug)]
struct InnerClient {
    addr: ElectrumServerAddress,
    status: AtomicElectrumConnectionStatus,
    running: AtomicBool,
    channels: Channels,
    tracker: ServicesTracker,
    notification_sender: broadcast::Sender<ElectrumNotification>,
    config: Config,
}

impl InnerClient {
    #[inline]
    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    #[inline]
    fn send_notification(&self, notification: ElectrumNotification) {
        let _ = self.notification_sender.send(notification);
    }

    #[inline]
    fn status(&self) -> ElectrumConnectionStatus {
        self.status.load()
    }

    fn set_status(&self, status: ElectrumConnectionStatus, log: bool) {
        // Change status
        self.status.set(status);

        // Log
        if log {
            match status {
                ElectrumConnectionStatus::Initialized => {
                    tracing::trace!(addr = %self.addr, "Electrum client initialized.")
                }
                ElectrumConnectionStatus::Pending => {
                    tracing::trace!(addr = %self.addr, "Electrum client is pending.")
                }
                ElectrumConnectionStatus::Connecting => {
                    tracing::debug!("Connecting to '{}'", self.addr)
                }
                ElectrumConnectionStatus::Connected => {
                    tracing::info!("Connected to '{}'", self.addr)
                }
                ElectrumConnectionStatus::Disconnected => {
                    tracing::info!("Disconnected from '{}'", self.addr)
                }
                ElectrumConnectionStatus::Terminated => {
                    tracing::info!("Completely disconnected from '{}'", self.addr)
                }
            }
        }

        // Send notification
        self.send_notification(ElectrumNotification::ConnectionStatusChanged(status));
    }

    async fn validate_network(&self, client: &AsyncClient) -> Result<(), Error> {
        // Validate network
        if let Some(expected_network) = self.config.expected_network {
            let features: ServerFeatures =
                send_request_with_timeout(client, Duration::from_secs(10), GetServerFeatures)
                    .await?;

            let server_chain_hash: ChainHash =
                ChainHash::from_genesis_block_hash(features.genesis_hash);

            if server_chain_hash != expected_network.chain_hash() {
                // Set network mismatch
                self.tracker.set_network_mismatch(true);

                // Mark as terminated
                self.set_status(ElectrumConnectionStatus::Terminated, true);

                // Return error
                return Err(Error::NetworkMismatch);
            }
        }

        Ok(())
    }

    #[inline]
    async fn handle_terminate(&self) {
        // Wait to be notified
        self.channels.terminate.notified().await;
    }

    /// This **MUST** be called only by the [`Self::spawn_connection_task`] method!
    async fn connection_task(self: Arc<Self>) {
        // Set the connection task as running and get the previous value.
        let is_running: bool = self.running.swap(true, Ordering::SeqCst);

        // Re-check if the connection task is already running.
        // This is required because may happen that two tasks are spawned at the exact same moment.
        // Not use the "assert" macro since will cause the task to panic.
        if is_running {
            tracing::warn!(addr = %self.addr, "Electrum connection task is already running.");
            return;
        }

        // Reset service tracker
        self.tracker.reset().await;

        // Lock receiver
        let mut rx_batch_requests = self.channels.rx_batch_requests().await;

        // Auto-connect loop
        loop {
            // Connect and run message handler
            // The termination requests are handled inside this method!
            self.connect_and_run(&mut rx_batch_requests).await;

            // Get status
            let status: ElectrumConnectionStatus = self.status();

            // If the connection is terminated, break the loop.
            if status.is_terminated() {
                break;
            }

            // Check if the client is marked as disconnected. If not, update status.
            // Check if disconnected to avoid a possible double log
            if !status.is_disconnected() {
                self.set_status(ElectrumConnectionStatus::Disconnected, true);
            }

            // Sleep before retry to connect
            let interval: Duration = Duration::from_secs(10); // TODO: move this to a constant
            tracing::debug!(
                "Reconnecting to '{}' in {} secs",
                self.addr,
                interval.as_secs()
            );

            // Sleep before retry to connect
            // Handle termination to allow exiting immediately if request is received during the sleep.
            tokio::select! {
                // Sleep
                _ = time::sleep(interval) => {},
                // Handle termination notification
                _ = self.handle_terminate() => break,
            }
        }

        // Mark the connection task as stopped.
        self.running.store(false, Ordering::SeqCst);

        tracing::debug!(addr = %self.addr, "Auto connect loop terminated.");
    }

    async fn _try_connect(&self) -> Result<(BoxReadStream, BoxWriteStream), Error> {
        // Update status
        self.set_status(ElectrumConnectionStatus::Connecting, true);

        // Try to connect
        // If during connection the termination request is received, abort the connection and return error.
        // At this stem is NOT required to close the WebSocket connection.
        tokio::select! {
            // Connect
            res = connect(&self.addr, self.config.connection_mode, self.config.connection_timeout) => match res {
                Ok((reader, writer)) => {
                    // Update status
                    self.set_status(ElectrumConnectionStatus::Connected, true);

                    Ok((reader, writer))
                }
                Err(e) => {
                    // Update status
                    self.set_status(ElectrumConnectionStatus::Disconnected, false);

                    // Return error
                    Err(e)
                }
            },
            // Handle termination notification
            _ = self.handle_terminate() => Err(Error::TerminationRequest),
        }
    }

    /// Connect and run message handler
    async fn connect_and_run(
        &self,
        rx_batch_requests: &mut MutexGuard<'_, Receiver<AsyncBatchRequest>>,
    ) {
        match self._try_connect().await {
            // Connection success, go to post-connection stage
            Ok((reader, writer)) => {
                self.post_connection(reader, writer, rx_batch_requests)
                    .await
            }
            // Error during connection
            Err(e) => {
                tracing::error!(addr = %self.addr, error= %e, "Connection failed.");
            }
        }
    }

    /// To run after connection.
    /// Run message handlers, pinger and other services
    async fn post_connection(
        &self,
        reader: BoxReadStream,
        writer: BoxWriteStream,
        rx_batch_requests: &mut MutexGuard<'_, Receiver<AsyncBatchRequest>>,
    ) {
        // Construct new electrum async client
        let (client, receiver, worker) = AsyncClient::new_tokio(reader, writer);

        // If the header subscription was enabled, resubscribe.
        if self.tracker.is_headers_subscribed() {
            tracing::debug!(addr = %self.addr, "Resubscribing to headers.");

            let mut batch = AsyncBatchRequest::new();
            batch.event_request(HeadersSubscribe);

            match client.send_batch(batch) {
                Ok(true) => {
                    tracing::debug!(addr = %self.addr, "Successfully resubscribed to headers.")
                }
                Ok(false) => tracing::warn!(addr = %self.addr, "Headers subscription failed."),
                Err(e) => {
                    tracing::error!(addr = %self.addr, error = %e, "Error during headers resubscribe.")
                }
            }
        }

        {
            // Acquire read lock
            let script_hashes = self.tracker.script_hashes.read().await;

            // If are cached any script hashes, resubscribe.
            if !script_hashes.is_empty() {
                tracing::debug!(addr = %self.addr, "Resubscribing to {} script hashes.", script_hashes.len());

                let mut batch = AsyncBatchRequest::new();

                for script_hash in script_hashes.iter().copied() {
                    batch.event_request(ScriptHashSubscribe { script_hash });
                }

                drop(script_hashes);

                match client.send_batch(batch) {
                    Ok(true) => {
                        tracing::debug!(addr = %self.addr, "Successfully resubscribed to script hashes.")
                    }
                    Ok(false) => {
                        tracing::warn!(addr = %self.addr, "Script hashes subscription failed.")
                    }
                    Err(e) => {
                        tracing::error!(addr = %self.addr, error = %e, "Error during script hashes resubscribe.")
                    }
                }
            }
        }

        // Wait that one of the futures terminates/completes
        // Add also termination here, to allow closing the connection in case of termination request.
        tokio::select! {
            res = worker => match res {
                Ok(()) => tracing::trace!(addr = %self.addr, "Electrum worker exited."),
                Err(e) => tracing::error!(addr = %self.addr, error = %e, "Electrum worker exited with error.")
            },
            // Message sender handler
            res = self.sender_message_handler(&client, rx_batch_requests) => match res {
                Ok(()) => tracing::trace!(addr = %self.addr, "Electrum sender exited."),
                Err(e) => tracing::error!(addr = %self.addr, error = %e, "Electrum sender exited with error.")
            },
            // Message receiver handler
            res = self.receiver_message_handler(receiver) => match res {
                Ok(()) => tracing::trace!(addr = %self.addr, "Electrum receiver exited."),
                Err(e) => tracing::error!(addr = %self.addr, error = %e, "Electrum receiver exited with error.")
            },
            // Termination handler
            _ = self.handle_terminate() => {},
            // Pinger
            _ = self.pinger() => {}
        }

        // Notify disconnection
        self.channels.disconnected();

        // Close
        client.close();
    }

    async fn sender_message_handler(
        &self,
        client: &AsyncClient,
        rx_batch_request: &mut MutexGuard<'_, Receiver<AsyncBatchRequest>>,
    ) -> Result<(), Error> {
        // Validate the network before start processing the requests
        self.validate_network(client).await?;

        // Start the receiver loop
        loop {
            tokio::select! {
                // Batch request receiver
                Some(batch_request) = rx_batch_request.recv() => {
                    tracing::trace!("Sending batch request: {:?}", batch_request);

                    client.send_batch(batch_request)?;
                }
                // Ping channel receiver
                _ = self.channels.ping.notified() => {
                    tracing::trace!(addr = %self.addr, "Sending ping.");
                    send_request_with_timeout(client, Duration::from_secs(5), Ping).await?;
                    tracing::trace!(addr = %self.addr, "Ping sent.");
                }
                else => break
            }
        }

        Ok(())
    }

    async fn receiver_message_handler(
        &self,
        mut receiver: AsyncEventReceiver,
    ) -> Result<(), Error> {
        while let Some(event) = receiver.next().await {
            let notification: Option<ElectrumNotification> = match event {
                Event::Response(res) => match res {
                    SatisfiedRequest::HeadersSubscribe { resp, .. } => {
                        tracing::debug!(addr = %self.addr, "Subscribed to headers.");

                        // Mark as subscribed
                        self.tracker.set_headers_subscribed(true);

                        Some(ElectrumNotification::BlockHeader {
                            height: resp.height,
                            header: resp.header,
                        })
                    }
                    SatisfiedRequest::ScriptHashSubscribe { req, resp } => {
                        // Mark as subscribed
                        let mut script_hashes_set = self.tracker.script_hashes.write().await;
                        script_hashes_set.insert(req.script_hash);

                        Some(ElectrumNotification::ScriptHash {
                            hash: req.script_hash,
                            status: resp,
                        })
                    }
                    SatisfiedRequest::ScriptHashUnsubscribe { req, .. } => {
                        // Mark as unsubscribed
                        let mut script_hashes_set = self.tracker.script_hashes.write().await;
                        script_hashes_set.remove(&req.script_hash);

                        None
                    }
                    _ => None,
                },
                Event::ResponseError(err) => {
                    tracing::error!(addr = %self.addr, error = %err, "Error during response.");
                    None
                }
                Event::Notification(notification) => match notification {
                    Notification::Header(header) => Some(ElectrumNotification::BlockHeader {
                        height: header.height(),
                        header: *header.header(),
                    }),
                    Notification::ScriptHash(script_hash) => {
                        Some(ElectrumNotification::ScriptHash {
                            hash: script_hash.script_hash(),
                            status: script_hash.script_status(),
                        })
                    }
                    Notification::Unknown(unknown) => {
                        tracing::warn!(notification = ?unknown, "Received an unknown notification.");
                        None
                    }
                },
            };

            // Send notification, if any.
            if let Some(notification) = notification {
                self.send_notification(notification);
            }
        }

        Ok(())
    }

    async fn pinger(&self) {
        loop {
            // Ping!
            self.channels.ping();

            // Sleep
            time::sleep(PING_INTERVAL).await;
        }
    }

    #[inline]
    fn send_batch(&self, batch: AsyncBatchRequest) -> Result<(), Error> {
        if self.tracker.network_mismatch() {
            return Err(Error::NetworkMismatch);
        }

        self.channels
            .commands
            .0
            .try_send(batch)
            .map_err(|e| Error::MpscTrySend(e.to_string()))
    }
}

/// Electrum client
#[derive(Debug)]
pub struct ElectrumClient {
    inner: Arc<InnerClient>,
    atomic_counter: Arc<AtomicUsize>,
}

impl Clone for ElectrumClient {
    fn clone(&self) -> Self {
        self.atomic_counter.fetch_add(1, Ordering::SeqCst);

        Self {
            inner: self.inner.clone(),
            atomic_counter: self.atomic_counter.clone(),
        }
    }
}

impl Drop for ElectrumClient {
    fn drop(&mut self) {
        // Shutdown exactly once when the last client handle is dropped.
        if self.atomic_counter.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.disconnect();
        }
    }
}

impl ElectrumClient {
    /// Construct a new electrum client
    #[inline]
    pub fn new(addr: ElectrumServerAddress) -> Self {
        Self::builder(addr).build()
    }

    /// Construct a new electrum client builder
    #[inline]
    pub fn builder(addr: ElectrumServerAddress) -> ElectrumClientBuilder {
        ElectrumClientBuilder::new(addr)
    }

    pub(crate) fn from_builder(builder: ElectrumClientBuilder) -> Self {
        let (notification_sender, ..) = broadcast::channel(builder.notification_channel_size);

        Self {
            inner: Arc::new(InnerClient {
                addr: builder.addr,
                status: AtomicElectrumConnectionStatus::default(),
                running: AtomicBool::new(false),
                channels: Channels::new(),
                tracker: ServicesTracker::default(),
                notification_sender,
                config: Config {
                    connection_mode: builder.connection_mode,
                    connection_timeout: builder.connection_timeout,
                    request_timeout: builder.request_timeout,
                    expected_network: builder.expected_network,
                },
            }),
            atomic_counter: Arc::new(AtomicUsize::new(1)),
        }
    }

    /// Check if the connection task is running
    #[inline]
    fn is_running(&self) -> bool {
        self.inner.is_running()
    }

    /// Subscribe to notifications
    ///
    /// When you call this method, you subscribe to the notifications channel from that precise moment.
    /// Anything received by client before that moment is not included in the channel!
    #[inline]
    pub fn notifications(&self) -> broadcast::Receiver<ElectrumNotification> {
        self.inner.notification_sender.subscribe()
    }

    /// Get the current connection status
    #[inline]
    pub fn status(&self) -> ElectrumConnectionStatus {
        self.inner.status()
    }

    /// Connect to the electrum server and keep the connection alive.
    ///
    /// This automatically reconnects in case of disconnection.
    pub fn connect(&self) {
        // Immediately return if can't connect
        if !self.status().can_connect() {
            return;
        }

        // Update status
        // Change it to pending to avoid issues with the health check (initialized check)
        self.inner
            .set_status(ElectrumConnectionStatus::Pending, false);

        // Spawn connection task
        self.spawn_connection_task();
    }

    fn spawn_connection_task(&self) {
        // Check if the connection task is already running
        // This is checked also later, but it's checked also here to avoid a full-clone if we know that is already running.
        if self.is_running() {
            tracing::warn!(addr = %self.inner.addr, "Electrum connection task is already running.");
            return;
        }

        // Full-clone of the inner client
        let client: Arc<InnerClient> = self.inner.clone();

        // Spawn task
        tokio::spawn(client.connection_task());
    }

    /// Terminate connection with the electrum server
    pub fn disconnect(&self) {
        let status = self.status();

        // Check if it's already terminated or banned
        if status.is_terminated() {
            return;
        }

        // Notify termination
        self.inner.channels.terminate();

        // Update status
        self.inner
            .set_status(ElectrumConnectionStatus::Terminated, true);

        // Shutdown all notification loops
        self.inner.send_notification(ElectrumNotification::Shutdown);
    }

    async fn wait_batch_response<F>(&self, fut: F) -> Result<F::Output, Error>
    where
        F: IntoFuture,
    {
        tokio::select! {
            resp = time::timeout(self.inner.config.request_timeout, fut) => {
                match resp {
                    Ok(resp) => Ok(resp),
                    Err(_) => Err(Error::Timeout)
                }
            }
            _ = self.inner.channels.disconnected.notified() => Err(Error::Disconnected),
        }
    }

    /// Get server features
    pub async fn server_features(&self) -> Result<ServerFeatures, Error> {
        let mut batch = AsyncBatchRequest::new();
        let fut = batch.request(GetServerFeatures);

        self.inner.send_batch(batch)?;

        let resp = self.wait_batch_response(fut).await??;

        Ok(resp)
    }

    /// Get block header
    pub async fn block_header(&self, height: u32) -> Result<Header, Error> {
        let mut batch = AsyncBatchRequest::new();
        let fut = batch.request(GetBlockHeader { height });

        self.inner.send_batch(batch)?;

        let resp = self.wait_batch_response(fut).await??;

        Ok(resp.header)
    }

    /// Tries to fetch `count` block headers starting from `start_height`.
    pub async fn block_headers(
        &self,
        start_height: u32,
        count: usize,
    ) -> Result<BlockHeaders, Error> {
        let mut batch = AsyncBatchRequest::new();
        let fut = batch.request(Headers {
            start_height,
            count,
        });

        self.inner.send_batch(batch)?;

        let resp = self.wait_batch_response(fut).await??;

        Ok(BlockHeaders::from(resp))
    }

    /// Subscribe to block headers and return the current blockchain tip.
    ///
    /// The updates can be monitored with [`ElectrumClient::notifications`].
    pub async fn get_tip(&self) -> Result<BlockHeader, Error> {
        // Explicitly mark as subscribed, also if we mark it again in receiver_message_handler
        self.inner.tracker.set_headers_subscribed(true);

        let mut batch = AsyncBatchRequest::new();
        let fut = batch.request(HeadersSubscribe);

        self.inner.send_batch(batch)?;

        let resp = self.wait_batch_response(fut).await??;

        Ok(BlockHeader::from(resp))
    }

    /// Subscribe to block headers
    ///
    /// The updates can be monitored with [`ElectrumClient::notifications`].
    pub fn block_headers_subscribe(&self) -> Result<(), Error> {
        // Explicitly mark as subscribed, also if we mark it again in receiver_message_handler
        self.inner.tracker.set_headers_subscribed(true);

        let mut batch = AsyncBatchRequest::new();
        batch.event_request(HeadersSubscribe);
        self.inner.send_batch(batch)?;
        Ok(())
    }

    /// Subscribe to script hash
    ///
    /// The updates can be monitored with [`ElectrumClient::notifications`].
    #[inline]
    pub fn script_hash_subscribe<T>(&self, script_hash: T) -> Result<(), Error>
    where
        T: Into<ElectrumScriptHash>,
    {
        self.batch_script_hash_subscribe(vec![script_hash])
    }

    /// Subscribe to script hashes
    ///
    /// The updates can be monitored with [`ElectrumClient::notifications`].
    pub fn batch_script_hash_subscribe<I, T>(&self, script_hashes: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<ElectrumScriptHash>,
    {
        let mut batch = AsyncBatchRequest::new();

        for script_hash in script_hashes {
            batch.event_request(ScriptHashSubscribe {
                script_hash: script_hash.into(),
            });
        }

        self.inner.send_batch(batch)?;

        Ok(())
    }

    /// Unsubscribe from a script hash
    pub fn script_hash_unsubscribe<T>(&self, script_hash: T) -> Result<(), Error>
    where
        T: Into<ElectrumScriptHash>,
    {
        let mut batch = AsyncBatchRequest::new();
        batch.event_request(ScriptHashUnsubscribe {
            script_hash: script_hash.into(),
        });
        self.inner.send_batch(batch)?;
        Ok(())
    }

    /// Get history for scripts
    pub async fn script_get_history<T>(&self, script_hash: T) -> Result<Vec<Tx>, Error>
    where
        T: Into<ElectrumScriptHash>,
    {
        let mut batch = AsyncBatchRequest::new();
        let fut = batch.request(GetHistory {
            script_hash: script_hash.into(),
        });

        self.inner.send_batch(batch)?;

        let resp = self.wait_batch_response(fut).await??;

        Ok(resp)
    }

    /// Batch get history for scripts
    pub async fn batch_script_get_history<I, T>(
        &self,
        script_hashes: I,
    ) -> Result<Vec<Vec<Tx>>, Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<ElectrumScriptHash>,
    {
        let mut batch = AsyncBatchRequest::new();

        let mut futures = Vec::new();

        for script_hash in script_hashes {
            let fut = batch.request(GetHistory {
                script_hash: script_hash.into(),
            });
            futures.push(fut);
        }

        self.inner.send_batch(batch)?;

        let resp = self.wait_batch_response(future::join_all(futures)).await?;

        let mut output = Vec::new();

        for txs in resp.into_iter().flatten() {
            output.push(txs);
        }

        Ok(output)
    }

    /// Get transaction
    pub async fn transaction_get(&self, txid: Txid) -> Result<Transaction, Error> {
        let mut batch = AsyncBatchRequest::new();
        let fut = batch.request(GetTx { txid });

        self.inner.send_batch(batch)?;

        let resp = self.wait_batch_response(fut).await??;

        Ok(resp.tx)
    }

    /// Get transaction merkle
    pub async fn transaction_get_merkle(
        &self,
        txid: Txid,
        height: u32,
    ) -> Result<TransactionMerkel, Error> {
        let mut batch = AsyncBatchRequest::new();
        let fut = batch.request(GetTxMerkle { txid, height });

        self.inner.send_batch(batch)?;

        let resp = self.wait_batch_response(fut).await??;

        Ok(TransactionMerkel::from(resp))
    }

    /// Return the estimated transaction fee for a transaction to be confirmed within a certain number of blocks.
    ///
    /// Returns `None` if the server could not estimate.
    pub async fn estimate_fee(&self, number: usize) -> Result<Option<FeeRate>, Error> {
        let mut batch = AsyncBatchRequest::new();
        let fut = batch.request(EstimateFee { number });

        self.inner.send_batch(batch)?;

        let resp = self.wait_batch_response(fut).await??;

        Ok(resp.fee_rate)
    }

    /// Return the estimated transaction fees for a transaction to be confirmed within a certain number of blocks.
    pub async fn batch_estimate_fee<I>(&self, blocks: I) -> Result<BTreeMap<usize, FeeRate>, Error>
    where
        I: IntoIterator<Item = usize>,
    {
        let mut batch = AsyncBatchRequest::new();

        let mut indexes = Vec::new();
        let mut futures = Vec::new();

        for number in blocks.into_iter() {
            let fut = batch.request(EstimateFee { number });
            indexes.push(number);
            futures.push(fut);
        }

        self.inner.send_batch(batch)?;

        let resp = self.wait_batch_response(future::join_all(futures)).await?;

        let mut output = BTreeMap::new();

        for (res, num) in resp.into_iter().zip(indexes) {
            if let Ok(Some(fee)) = res.map(|res| res.fee_rate) {
                output.insert(num, fee);
            }
        }

        Ok(output)
    }

    /// Broadcast a transaction
    pub async fn broadcast_tx(&self, tx: Transaction) -> Result<Txid, Error> {
        let mut batch = AsyncBatchRequest::new();
        let fut = batch.request(BroadcastTx(tx));

        self.inner.send_batch(batch)?;

        let resp = self.wait_batch_response(fut).await??;

        Ok(resp)
    }
}

fn split_stream<T>(stream: T) -> (BoxReadStream, BoxWriteStream)
where
    T: AsyncRead + AsyncWrite + Send + 'static,
{
    // Split stream
    let (reader, writer) = tokio::io::split(stream);

    // Box split stream
    (Box::new(reader), Box::new(writer))
}

async fn connect(
    addr: &ElectrumServerAddress,
    mode: ElectrumConnectionMode,
    timeout: Duration,
) -> Result<(BoxReadStream, BoxWriteStream), Error> {
    match addr.scheme() {
        Scheme::Tcp => time::timeout(timeout, connect_tcp(addr.addr(), mode))
            .await
            .map_err(|_| Error::Timeout)?,
        Scheme::Ssl => time::timeout(timeout, connect_ssl(addr.addr(), mode))
            .await
            .map_err(|_| Error::Timeout)?,
    }
}

async fn connect_tcp(
    addr: &HostAndPort,
    mode: ElectrumConnectionMode,
) -> Result<(BoxReadStream, BoxWriteStream), Error> {
    match mode {
        ElectrumConnectionMode::Direct => connect_direct_tcp(addr).await,
        #[cfg(feature = "socks")]
        ElectrumConnectionMode::Proxy(proxy) => connect_proxy_tcp(addr, proxy).await,
    }
}

async fn connect_direct_tcp(addr: &HostAndPort) -> Result<(BoxReadStream, BoxWriteStream), Error> {
    // Connect
    let stream: TcpStream = TcpStream::connect(addr.to_string()).await?;

    // Split stream
    Ok(split_stream(stream))
}

#[cfg(feature = "socks")]
async fn connect_proxy_tcp(
    addr: &HostAndPort,
    proxy: SocketAddr,
) -> Result<(BoxReadStream, BoxWriteStream), Error> {
    let stream: TcpStream = TcpSocks5Stream::connect(proxy, addr.to_string()).await?;

    // Split stream
    Ok(split_stream(stream))
}

async fn connect_ssl(
    addr: &HostAndPort,
    mode: ElectrumConnectionMode,
) -> Result<(BoxReadStream, BoxWriteStream), Error> {
    match mode {
        ElectrumConnectionMode::Direct => connect_direct_ssl(addr).await,
        #[cfg(feature = "socks")]
        ElectrumConnectionMode::Proxy(proxy) => connect_proxy_ssl(addr, proxy).await,
    }
}

async fn connect_direct_ssl(addr: &HostAndPort) -> Result<(BoxReadStream, BoxWriteStream), Error> {
    // Connect to the server
    let tcp_stream: TcpStream = TcpStream::connect(addr.to_string()).await?;

    // Create TLS configuration
    ssl_connector(addr, tcp_stream).await
}

#[cfg(feature = "socks")]
async fn connect_proxy_ssl(
    addr: &HostAndPort,
    proxy: SocketAddr,
) -> Result<(BoxReadStream, BoxWriteStream), Error> {
    // Connect to the server
    let tcp_stream: TcpStream = TcpSocks5Stream::connect(proxy, addr.to_string()).await?;

    // Create TLS configuration
    ssl_connector(addr, tcp_stream).await
}

async fn ssl_connector<T>(
    addr: &HostAndPort,
    tcp_stream: T,
) -> Result<(BoxReadStream, BoxWriteStream), Error>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // Install ring provider
    let _ = ring::default_provider().install_default();

    // Create TLS configuration
    let mut root_cert_store: RootCertStore = RootCertStore::empty();
    root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let config: ClientConfig = ClientConfig::builder()
        .with_root_certificates(root_cert_store)
        .with_no_client_auth();

    let connector: TlsConnector = TlsConnector::from(Arc::new(config));

    let hostname: String = addr.host.to_string();

    // Connect to the server
    let domain: ServerName = ServerName::try_from(hostname)?;
    let tls_stream: TlsStream<_> = connector.connect(domain, tcp_stream).await?;

    // Split stream
    Ok(split_stream(tls_stream))
}

async fn send_request_with_timeout<Req>(
    client: &AsyncClient,
    timeout: Duration,
    req: Req,
) -> Result<Req::Response, Error>
where
    Req: Request,
    AsyncPendingRequestTuple<Req, Req::Response>: Into<AsyncPendingRequest>,
{
    Ok(time::timeout(timeout, client.send_request(req))
        .await
        .map_err(|_| Error::Timeout)??)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_client() -> ElectrumClient {
        let addr = ElectrumServerAddress::parse("tcp://127.0.0.1:50001").unwrap();
        ElectrumClient::new(addr)
    }

    #[tokio::test]
    async fn test_shutdown_on_drop() {
        let inner: Arc<InnerClient> = {
            let client: ElectrumClient = test_client();

            client.connect();

            tokio::time::sleep(Duration::from_secs(1)).await;

            assert!(client.is_running());

            // Clone the inner client
            let inner: Arc<InnerClient> = client.inner.clone();

            {
                let c2: ElectrumClient = client.clone();
                tokio::spawn(async move {
                    assert_eq!(c2.atomic_counter.load(Ordering::SeqCst), 2);

                    time::sleep(Duration::from_secs(1)).await;

                    // c2 dropped here
                });
            }

            time::sleep(Duration::from_secs(3)).await;

            assert!(client.is_running());
            assert_eq!(client.atomic_counter.load(Ordering::SeqCst), 1);

            inner
        }; // client dropped here

        time::sleep(Duration::from_secs(1)).await;

        assert_eq!(inner.status(), ElectrumConnectionStatus::Terminated);
        assert!(!inner.is_running());
    }
}

//! Electrum client

use std::collections::BTreeMap;
#[cfg(feature = "socks")]
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use std::{fmt, io};

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
use futures_util::{StreamExt, future};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{Mutex, MutexGuard, Notify, broadcast, mpsc};
use tokio::time;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::crypto::ring;
use tokio_rustls::rustls::pki_types::{InvalidDnsNameError, ServerName};
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
#[cfg(feature = "socks")]
use tokio_socks::tcp::Socks5Stream;

use crate::address::{ElectrumServerAddress, HostAndPort, Scheme};
use crate::builder::{ElectrumClientBuilder, ElectrumConnectionMode};
use crate::constant::PING_INTERVAL;
use crate::notification::ElectrumNotification;
use crate::status::{
    AtomicElectrumConnectionStatus, ElectrumConnectionStatus, InternalElectrumConnectionStatus,
};
use crate::types::{BlockHeader, BlockHeaders, ElectrumScriptHash, TransactionMerkel};

type BoxReadStream = Box<dyn AsyncRead + Send + Unpin>;
type BoxWriteStream = Box<dyn AsyncWrite + Send + Unpin>;

/// Electrum client error
#[derive(Debug)]
pub enum Error {
    /// I/O error
    Io(io::Error),
    /// Invalid DNS name
    InvalidDnsName(InvalidDnsNameError),
    /// Electrum async send request error
    AsyncRequestSend(AsyncRequestSendError),
    /// Batch request error
    BatchRequest(BatchRequestError),
    /// Electrum async request error
    AsyncRequest(AsyncRequestError),
    /// Response error
    Response(ResponseError),
    /// Socks error
    #[cfg(feature = "socks")]
    Socks(tokio_socks::Error),
    /// MPSC try send error
    MpscTrySend(String),
    /// Network mismatch
    NetworkMismatch,
    /// Local command queue is saturated
    CommandQueueSaturated,
    /// Timeout
    Timeout,
    /// Disconnected
    Disconnected,
    /// Termination request
    TerminationRequest,
}

impl core::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => e.fmt(f),
            Self::InvalidDnsName(e) => e.fmt(f),
            Self::AsyncRequestSend(e) => e.fmt(f),
            Self::BatchRequest(e) => e.fmt(f),
            Self::AsyncRequest(e) => e.fmt(f),
            Self::Response(e) => e.fmt(f),
            #[cfg(feature = "socks")]
            Self::Socks(e) => e.fmt(f),
            Self::MpscTrySend(e) => e.fmt(f),
            Self::NetworkMismatch => {
                f.write_str("the server network does not match the expected network")
            }
            Self::CommandQueueSaturated => {
                f.write_str("the local electrum command queue is saturated")
            }
            Self::Timeout => f.write_str("timeout"),
            Self::Disconnected => f.write_str("disconnected"),
            Self::TerminationRequest => f.write_str("termination request"),
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<InvalidDnsNameError> for Error {
    fn from(e: InvalidDnsNameError) -> Self {
        Self::InvalidDnsName(e)
    }
}

impl From<AsyncRequestSendError> for Error {
    fn from(e: AsyncRequestSendError) -> Self {
        Self::AsyncRequestSend(e)
    }
}

impl From<BatchRequestError> for Error {
    fn from(e: BatchRequestError) -> Self {
        Self::BatchRequest(e)
    }
}

impl From<AsyncRequestError> for Error {
    fn from(e: AsyncRequestError) -> Self {
        Self::AsyncRequest(e)
    }
}

impl From<ResponseError> for Error {
    fn from(e: ResponseError) -> Self {
        Self::Response(e)
    }
}

#[cfg(feature = "socks")]
impl From<tokio_socks::Error> for Error {
    fn from(e: tokio_socks::Error) -> Self {
        Self::Socks(e)
    }
}

impl Error {
    /// Returns true when the error indicates the client is no longer connected.
    #[inline]
    pub fn is_disconnected_like(&self) -> bool {
        matches!(
            self,
            Self::Disconnected | Self::BatchRequest(BatchRequestError::Canceled)
        )
    }
}

#[derive(Debug, Clone, Copy)]
struct Config {
    connection_mode: ElectrumConnectionMode,
    connection_timeout: Duration,
    request_timeout: Duration,
    ping_timeout: Duration,
    reconnect_delay_initial: Duration,
    reconnect_delay_max: Duration,
    max_consecutive_ping_timeouts: u8,
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
    pub fn new(command_channel_size: NonZeroUsize) -> Self {
        let (tx, rx) = mpsc::channel(command_channel_size.get());
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
    fn status(&self) -> InternalElectrumConnectionStatus {
        self.status.load()
    }

    fn set_status(&self, status: InternalElectrumConnectionStatus, log: bool) {
        // Change status
        self.status.set(status);

        // Log
        if log {
            match status {
                InternalElectrumConnectionStatus::Initialized => {
                    tracing::trace!(addr = %self.addr, "Electrum client initialized.")
                }
                InternalElectrumConnectionStatus::Pending => {
                    tracing::trace!(addr = %self.addr, "Electrum client is pending.")
                }
                InternalElectrumConnectionStatus::Connecting => {
                    tracing::debug!("Connecting to '{}'", self.addr)
                }
                InternalElectrumConnectionStatus::Connected => {
                    tracing::info!("Connected to '{}'", self.addr)
                }
                InternalElectrumConnectionStatus::Disconnected => {
                    tracing::info!("Disconnected from '{}'", self.addr)
                }
                InternalElectrumConnectionStatus::Terminated => {
                    tracing::info!("Completely disconnected from '{}'", self.addr)
                }
            }
        }

        // Send notification
        self.send_notification(ElectrumNotification::ConnectionStatusChanged(status.into()));
    }

    async fn validate_network(&self, client: &AsyncClient) -> Result<(), Error> {
        // Validate network
        let Some(expected_network) = self.config.expected_network else {
            return Ok(());
        };

        let features: ServerFeatures =
            send_request_with_timeout(client, self.config.request_timeout, GetServerFeatures)
                .await?;

        let server_chain_hash: ChainHash =
            ChainHash::from_genesis_block_hash(features.genesis_hash);

        if server_chain_hash != expected_network.chain_hash() {
            // Set network mismatch
            self.tracker.set_network_mismatch(true);

            // Mark as terminated
            self.set_status(InternalElectrumConnectionStatus::Terminated, true);

            // Return error
            return Err(Error::NetworkMismatch);
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

        // Lock receiver
        let mut rx_batch_requests = self.channels.rx_batch_requests().await;
        let mut reconnect_attempt: u32 = 0;

        // Auto-connect loop
        loop {
            // Connect and run message handler
            // The termination requests are handled inside this method!
            self.connect_and_run(&mut rx_batch_requests).await;

            // Get status
            let status: InternalElectrumConnectionStatus = self.status();

            // If the connection is terminated, break the loop.
            if status.is_terminated() {
                break;
            }

            // Check if the client is marked as disconnected. If not, update status.
            // Check if disconnected to avoid a possible double log
            if !status.is_disconnected_or_terminated() {
                self.set_status(InternalElectrumConnectionStatus::Disconnected, true);
            }

            // Sleep before retry to connect
            let interval = next_reconnect_interval(
                reconnect_attempt,
                self.config.reconnect_delay_initial,
                self.config.reconnect_delay_max,
            );
            reconnect_attempt = reconnect_attempt.saturating_add(1);

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
        self.set_status(InternalElectrumConnectionStatus::Connecting, true);

        // Try to connect
        // If during connection the termination request is received, abort the connection and return error.
        // At this stem is NOT required to close the WebSocket connection.
        tokio::select! {
            // Connect
            res = connect(&self.addr, self.config.connection_mode, self.config.connection_timeout) => match res {
                Ok((reader, writer)) => {
                    // Update status
                    self.set_status(InternalElectrumConnectionStatus::Connected, true);

                    Ok((reader, writer))
                }
                Err(e) => {
                    // Update status
                    self.set_status(InternalElectrumConnectionStatus::Disconnected, false);

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

        // Wait that one of the futures terminates/completes
        // Add also termination here, to allow closing the connection in case of termination request.
        tokio::select! {
            res = worker => match res {
                Ok(()) => tracing::warn!(addr = %self.addr, "Electrum worker exited."),
                Err(e) => tracing::error!(addr = %self.addr, error = %e, "Electrum worker exited with error.")
            },
            // Message sender handler
            res = self.sender_message_handler(&client, rx_batch_requests) => match res {
                Ok(()) => tracing::warn!(addr = %self.addr, "Electrum sender exited."),
                Err(e) => tracing::error!(addr = %self.addr, error = %e, "Electrum sender exited with error.")
            },
            // Message receiver handler
            res = self.receiver_message_handler(receiver) => match res {
                Ok(()) => tracing::warn!(addr = %self.addr, "Electrum receiver exited."),
                Err(e) => tracing::error!(addr = %self.addr, error = %e, "Electrum receiver exited with error.")
            },
            // Termination handler
            _ = self.handle_terminate() => {
                tracing::warn!(addr = %self.addr, "Received termination request.");
            },
            // Pinger
            _ = self.pinger() => {
                tracing::warn!(addr = %self.addr, "Electrum pinger exited.");
            }
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

        let mut consecutive_ping_timeouts: u8 = 0;
        let max_ping_timeouts = self.config.max_consecutive_ping_timeouts;

        // Start the receiver loop
        loop {
            tokio::select! {
                // "biased" in tokio::select! makes branch selection priority-based, top to bottom, instead of fair/randomized.
                biased;
                // Ping channel receiver
                _ = self.channels.ping.notified() => {
                    tracing::trace!(addr = %self.addr, "Sending ping.");

                    match send_request_with_timeout(client, self.config.ping_timeout, Ping).await {
                        Ok(()) => {
                            consecutive_ping_timeouts = 0;
                        }
                        Err(Error::Timeout) => {
                            consecutive_ping_timeouts = consecutive_ping_timeouts.saturating_add(1);

                            tracing::warn!(
                                addr = %self.addr,
                                consecutive_ping_timeouts,
                                "Electrum ping timed out."
                            );

                            if consecutive_ping_timeouts < max_ping_timeouts {
                                continue;
                            }

                            return Err(Error::Timeout);
                        }
                        Err(e) => return Err(e),
                    }
                    tracing::trace!(addr = %self.addr, "Ping sent.");
                }
                // Batch request receiver
                batch_request = rx_batch_request.recv() => match batch_request {
                    Some(batch_request) => {
                        tracing::trace!("Sending batch request: {:?}", batch_request);
                        client.send_batch(batch_request)?;
                    }
                    None => {
                        tracing::warn!(
                            addr = %self.addr,
                            "Electrum sender loop is exiting because all batch request senders were dropped."
                        );
                        return Ok(());
                    }
                }
            }
        }
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

                        Some(ElectrumNotification::BlockHeader {
                            height: resp.height,
                            header: resp.header,
                        })
                    }
                    SatisfiedRequest::ScriptHashSubscribe { req, resp } => {
                        Some(ElectrumNotification::ScriptHash {
                            hash: req.script_hash,
                            status: resp,
                        })
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

        tracing::warn!(
            addr = %self.addr,
            "Electrum event stream ended (peer may have closed the connection)."
        );

        Ok(())
    }

    async fn pinger(&self) {
        loop {
            // Sleep first to avoid forcing a ping immediately after connection bootstrap.
            time::sleep(PING_INTERVAL).await;

            // Ping!
            self.channels.ping();
        }
    }

    async fn send_batch(&self, batch: AsyncBatchRequest) -> Result<(), Error> {
        if self.tracker.network_mismatch() {
            return Err(Error::NetworkMismatch);
        }

        let send_result = time::timeout(
            self.config.request_timeout,
            self.channels.commands.0.send(batch),
        )
        .await
        .map_err(|_| Error::CommandQueueSaturated)?;

        send_result.map_err(|e| Error::MpscTrySend(e.to_string()))
    }
}

// The client MUST not impl Clone.
// We rely on Drop for disconnection.
// For who needs cloning, can wrap the client in an Arc.
/// Electrum client
#[derive(Debug)]
pub struct ElectrumClient {
    inner: Arc<InnerClient>,
}

impl Drop for ElectrumClient {
    #[inline]
    fn drop(&mut self) {
        tracing::debug!(addr = %self.inner.addr, "Dropping electrum client..");

        self.disconnect();
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
        let (notification_sender, ..) = broadcast::channel(builder.notification_channel_size.get());

        Self {
            inner: Arc::new(InnerClient {
                addr: builder.addr,
                status: AtomicElectrumConnectionStatus::default(),
                running: AtomicBool::new(false),
                channels: Channels::new(builder.command_channel_size),
                tracker: ServicesTracker::default(),
                notification_sender,
                config: Config {
                    connection_mode: builder.connection_mode,
                    connection_timeout: builder.connection_timeout,
                    request_timeout: builder.request_timeout,
                    ping_timeout: builder.ping_timeout,
                    reconnect_delay_initial: builder.reconnect_delay_initial,
                    reconnect_delay_max: builder.reconnect_delay_max,
                    max_consecutive_ping_timeouts: builder.max_consecutive_ping_timeouts.max(1),
                    expected_network: builder.expected_network,
                },
            }),
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

    #[inline]
    fn internal_status(&self) -> InternalElectrumConnectionStatus {
        self.inner.status()
    }

    /// Get the current connection status
    #[inline]
    pub fn status(&self) -> ElectrumConnectionStatus {
        self.internal_status().into()
    }

    /// Connect to the electrum server and keep the connection alive.
    ///
    /// This automatically reconnects in case of disconnection.
    pub fn connect(&self) {
        // Immediately return if can't connect
        if !self.internal_status().can_connect() {
            return;
        }

        // Update status
        // Change it to pending to avoid issues with the health check (initialized check)
        self.inner
            .set_status(InternalElectrumConnectionStatus::Pending, false);

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
        let status = self.internal_status();

        // Check if it's already terminated or banned
        if status.is_terminated() {
            return;
        }

        // Notify termination
        self.inner.channels.terminate();

        // Update status
        self.inner
            .set_status(InternalElectrumConnectionStatus::Terminated, true);

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

        self.inner.send_batch(batch).await?;

        let resp = self.wait_batch_response(fut).await??;

        Ok(resp)
    }

    /// Get block header
    pub async fn block_header(&self, height: u32) -> Result<Header, Error> {
        let mut batch = AsyncBatchRequest::new();
        let fut = batch.request(GetBlockHeader { height });

        self.inner.send_batch(batch).await?;

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

        self.inner.send_batch(batch).await?;

        let resp = self.wait_batch_response(fut).await??;

        Ok(BlockHeaders::from(resp))
    }

    /// Subscribe to block headers and return the current blockchain tip.
    ///
    /// The updates can be monitored with [`ElectrumClient::notifications`].
    pub async fn get_tip(&self) -> Result<BlockHeader, Error> {
        let mut batch = AsyncBatchRequest::new();
        let fut = batch.request(HeadersSubscribe);

        self.inner.send_batch(batch).await?;

        let resp = self.wait_batch_response(fut).await??;

        Ok(BlockHeader::from(resp))
    }

    /// Subscribe to block headers
    ///
    /// The updates can be monitored with [`ElectrumClient::notifications`].
    pub async fn block_headers_subscribe(&self) -> Result<(), Error> {
        let mut batch = AsyncBatchRequest::new();
        batch.event_request(HeadersSubscribe);
        self.inner.send_batch(batch).await?;
        Ok(())
    }

    /// Subscribe to script hash
    ///
    /// The updates can be monitored with [`ElectrumClient::notifications`].
    #[inline]
    pub async fn script_hash_subscribe<T>(&self, script_hash: T) -> Result<(), Error>
    where
        T: Into<ElectrumScriptHash>,
    {
        self.batch_script_hash_subscribe(vec![script_hash]).await
    }

    /// Subscribe to script hashes
    ///
    /// The updates can be monitored with [`ElectrumClient::notifications`].
    pub async fn batch_script_hash_subscribe<I, T>(&self, script_hashes: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<ElectrumScriptHash>,
    {
        let mut batch = AsyncBatchRequest::new();
        let mut has_requests = false;

        for script_hash in script_hashes {
            has_requests = true;
            batch.event_request(ScriptHashSubscribe {
                script_hash: script_hash.into(),
            });
        }

        if !has_requests {
            return Ok(());
        }

        self.inner.send_batch(batch).await?;

        Ok(())
    }

    /// Unsubscribe from a script hash
    #[inline]
    pub async fn script_hash_unsubscribe<T>(&self, script_hash: T) -> Result<(), Error>
    where
        T: Into<ElectrumScriptHash>,
    {
        self.batch_script_hash_unsubscribe(vec![script_hash]).await
    }

    /// Unsubscribe from script hashes
    pub async fn batch_script_hash_unsubscribe<I, T>(&self, script_hashes: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<ElectrumScriptHash>,
    {
        let mut batch = AsyncBatchRequest::new();
        let mut has_requests = false;

        for script_hash in script_hashes {
            has_requests = true;
            batch.event_request(ScriptHashUnsubscribe {
                script_hash: script_hash.into(),
            });
        }

        if !has_requests {
            return Ok(());
        }

        self.inner.send_batch(batch).await?;

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

        self.inner.send_batch(batch).await?;

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

        if futures.is_empty() {
            return Ok(Vec::new());
        }

        self.inner.send_batch(batch).await?;

        let resp = self.wait_batch_response(future::join_all(futures)).await?;

        let mut output: Vec<Vec<Tx>> = Vec::with_capacity(resp.len());

        // Propagate error
        // This avoids silently dropping the error, which may cause a partial sync.
        for txs in resp {
            output.push(txs?);
        }

        Ok(output)
    }

    /// Get transaction
    pub async fn transaction_get(&self, txid: Txid) -> Result<Transaction, Error> {
        let mut batch = AsyncBatchRequest::new();
        let fut = batch.request(GetTx { txid });

        self.inner.send_batch(batch).await?;

        let resp = self.wait_batch_response(fut).await??;

        Ok(resp.tx)
    }

    /// Batch get transactions.
    pub async fn batch_transaction_get<I>(&self, txids: I) -> Result<Vec<Transaction>, Error>
    where
        I: IntoIterator<Item = Txid>,
    {
        let mut batch = AsyncBatchRequest::new();
        let mut futures = Vec::new();

        for txid in txids {
            let fut = batch.request(GetTx { txid });
            futures.push(fut);
        }

        if futures.is_empty() {
            return Ok(Vec::new());
        }

        self.inner.send_batch(batch).await?;

        let resp = self.wait_batch_response(future::join_all(futures)).await?;
        let mut output = Vec::with_capacity(resp.len());

        // Propagate error to avoid partial sync results.
        for tx in resp {
            output.push(tx?.tx);
        }

        Ok(output)
    }

    /// Get transaction merkle
    pub async fn transaction_get_merkle(
        &self,
        txid: Txid,
        height: u32,
    ) -> Result<TransactionMerkel, Error> {
        let mut batch = AsyncBatchRequest::new();
        let fut = batch.request(GetTxMerkle { txid, height });

        self.inner.send_batch(batch).await?;

        let resp = self.wait_batch_response(fut).await??;

        Ok(TransactionMerkel::from(resp))
    }

    /// Batch get transaction merkle proofs.
    ///
    /// Per-request failures are returned inline so callers can decide whether to retry or
    /// continue partial processing.
    pub async fn batch_transaction_get_merkle<I>(
        &self,
        txids_and_heights: I,
    ) -> Result<Vec<Result<TransactionMerkel, Error>>, Error>
    where
        I: IntoIterator<Item = (Txid, u32)>,
    {
        let mut batch = AsyncBatchRequest::new();
        let mut futures = Vec::new();

        for (txid, height) in txids_and_heights {
            let fut = batch.request(GetTxMerkle { txid, height });
            futures.push(fut);
        }

        if futures.is_empty() {
            return Ok(Vec::new());
        }

        self.inner.send_batch(batch).await?;

        let resp = self.wait_batch_response(future::join_all(futures)).await?;
        Ok(resp
            .into_iter()
            .map(|res| res.map(TransactionMerkel::from).map_err(Error::from))
            .collect())
    }

    /// Return the estimated transaction fee for a transaction to be confirmed within a certain number of blocks.
    ///
    /// Returns `None` if the server could not estimate.
    pub async fn estimate_fee(&self, number: usize) -> Result<Option<FeeRate>, Error> {
        let mut batch = AsyncBatchRequest::new();
        let fut = batch.request(EstimateFee { number });

        self.inner.send_batch(batch).await?;

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

        if futures.is_empty() {
            return Ok(BTreeMap::new());
        }

        self.inner.send_batch(batch).await?;

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

        self.inner.send_batch(batch).await?;

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
    let stream: TcpStream = Socks5Stream::connect(proxy, addr.to_string())
        .await?
        .into_inner();

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
    let tcp_stream: TcpStream = Socks5Stream::connect(proxy, addr.to_string())
        .await?
        .into_inner();

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

fn next_reconnect_interval(attempt: u32, initial: Duration, max: Duration) -> Duration {
    // Guard against zero/invalid values configured by consumers.
    let initial = if initial.is_zero() {
        Duration::from_millis(250)
    } else {
        initial
    };
    let max = max.max(initial);

    // Exponential backoff with deterministic jitter capped at 250ms.
    let factor: u32 = 1_u32 << attempt.min(10);
    let base = initial.checked_mul(factor).unwrap_or(max).min(max);
    let jitter_ms: u64 = (u64::from(attempt) * 73) % 251;

    let interval: Duration = base.saturating_add(Duration::from_millis(jitter_ms));

    // Take the min interval
    interval.min(max)
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "socks")]
    use std::net::SocketAddr;

    use bitcoin::Amount;
    use testenv::TestEnv;
    use tokio::time::{sleep, timeout};

    use super::*;
    use crate::builder::ElectrumConnectionMode;

    fn test_client() -> ElectrumClient {
        let addr = ElectrumServerAddress::parse("tcp://127.0.0.1:50001").unwrap();
        ElectrumClient::new(addr)
    }

    fn test_client_with_request_timeout(request_timeout: Duration) -> ElectrumClient {
        let addr = ElectrumServerAddress::parse("tcp://127.0.0.1:50001").unwrap();
        ElectrumClient::builder(addr)
            .request_timeout(request_timeout)
            .build()
    }

    #[test]
    fn test_builder_wires_runtime_tuning() {
        let addr = ElectrumServerAddress::parse("tcp://127.0.0.1:50001").unwrap();
        let client = ElectrumClient::builder(addr)
            .request_timeout(Duration::from_secs(55))
            .ping_timeout(Duration::from_secs(9))
            .reconnect_delay_initial(Duration::from_secs(1))
            .reconnect_delay_max(Duration::from_secs(8))
            .max_consecutive_ping_timeouts(0)
            .command_channel_size(NonZeroUsize::new(2048).unwrap())
            .build();

        assert_eq!(client.inner.config.request_timeout, Duration::from_secs(55));
        assert_eq!(client.inner.config.ping_timeout, Duration::from_secs(9));
        assert_eq!(
            client.inner.config.reconnect_delay_initial,
            Duration::from_secs(1)
        );
        assert_eq!(
            client.inner.config.reconnect_delay_max,
            Duration::from_secs(8)
        );
        // Must clamp to at least 1.
        assert_eq!(client.inner.config.max_consecutive_ping_timeouts, 1);
    }

    #[test]
    fn test_next_reconnect_interval_bounds_and_growth() {
        assert_eq!(
            next_reconnect_interval(0, Duration::from_secs(2), Duration::from_secs(30)),
            Duration::from_secs(2)
        );

        assert_eq!(
            next_reconnect_interval(1, Duration::from_secs(2), Duration::from_secs(30)),
            Duration::from_secs(4) + Duration::from_millis(73)
        );

        // Capped at max.
        assert_eq!(
            next_reconnect_interval(20, Duration::from_secs(2), Duration::from_secs(30)),
            Duration::from_secs(30)
        );

        // Zero initial is sanitized.
        assert_eq!(
            next_reconnect_interval(0, Duration::ZERO, Duration::from_secs(1)),
            Duration::from_millis(250)
        );
    }

    #[tokio::test]
    async fn async_send_batch_returns_command_queue_saturated_when_queue_is_full() {
        let addr = ElectrumServerAddress::parse("tcp://127.0.0.1:50001").unwrap();
        let client = ElectrumClient::builder(addr)
            .request_timeout(Duration::from_millis(50))
            .command_channel_size(NonZeroUsize::new(1).unwrap())
            .build();

        // Fill command queue (no connection task is running, so nothing drains it).
        client.block_headers_subscribe().await.unwrap();

        let err = client
            .server_features()
            .await
            .expect_err("must fail with local backpressure");
        assert!(matches!(err, Error::CommandQueueSaturated));
    }

    #[tokio::test]
    async fn test_shutdown_on_drop() {
        let inner: Arc<InnerClient> = {
            let client: ElectrumClient = test_client();

            client.connect();

            tokio::time::sleep(Duration::from_secs(1)).await;

            assert!(client.is_running());

            // Clone the inner client
            client.inner.clone()
        }; // client dropped here

        time::sleep(Duration::from_secs(1)).await;

        assert_eq!(inner.status(), InternalElectrumConnectionStatus::Terminated);
        assert!(!inner.is_running());
    }

    #[tokio::test]
    async fn test_wait_batch_response_timeout() {
        let client = test_client_with_request_timeout(Duration::from_millis(10));
        let result = client
            .wait_batch_response(async {
                time::sleep(Duration::from_millis(100)).await;
                42_u8
            })
            .await;
        assert!(matches!(result, Err(Error::Timeout)));
    }

    #[tokio::test]
    async fn test_wait_batch_response_disconnected() {
        let client = test_client_with_request_timeout(Duration::from_secs(5));
        let inner = client.inner.clone();
        tokio::spawn(async move {
            time::sleep(Duration::from_millis(20)).await;
            inner.channels.disconnected();
        });

        let result = client.wait_batch_response(future::pending::<()>()).await;
        assert!(matches!(result, Err(Error::Disconnected)));
    }

    #[tokio::test]
    async fn test_connect_and_disconnect_guards() {
        let client = test_client();
        client.disconnect();
        assert!(client.internal_status().is_terminated());

        // No-op when already terminated.
        client.disconnect();
        assert!(client.internal_status().is_terminated());

        // No-op when status cannot connect.
        client
            .inner
            .set_status(InternalElectrumConnectionStatus::Pending, false);
        client.connect();
        assert_eq!(
            client.internal_status(),
            InternalElectrumConnectionStatus::Pending
        );
    }

    #[test]
    fn test_spawn_connection_task_returns_if_running() {
        let client = test_client();
        client.inner.running.store(true, Ordering::SeqCst);
        client.spawn_connection_task();
        assert!(client.inner.running.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_send_batch_returns_network_mismatch() {
        let client = test_client();
        client.inner.tracker.set_network_mismatch(true);

        let err = client
            .block_headers_subscribe()
            .await
            .expect_err("must fail");
        assert!(matches!(err, Error::NetworkMismatch));
    }

    #[tokio::test]
    async fn test_connect_helpers_error_paths() {
        let tcp_addr = ElectrumServerAddress::parse("tcp://127.0.0.1:1").unwrap();
        let ssl_addr = ElectrumServerAddress::parse("ssl://127.0.0.1:1").unwrap();

        assert!(
            connect_tcp(tcp_addr.addr(), ElectrumConnectionMode::Direct)
                .await
                .is_err()
        );
        assert!(
            connect_ssl(ssl_addr.addr(), ElectrumConnectionMode::Direct)
                .await
                .is_err()
        );
        assert!(
            connect(
                &tcp_addr,
                ElectrumConnectionMode::Direct,
                Duration::from_millis(10),
            )
            .await
            .is_err()
        );
        assert!(
            connect(
                &ssl_addr,
                ElectrumConnectionMode::Direct,
                Duration::from_millis(10),
            )
            .await
            .is_err()
        );

        #[cfg(feature = "socks")]
        {
            let proxy: SocketAddr = "127.0.0.1:1".parse().unwrap();

            assert!(
                connect_tcp(tcp_addr.addr(), ElectrumConnectionMode::Proxy(proxy))
                    .await
                    .is_err()
            );
            assert!(
                connect_ssl(ssl_addr.addr(), ElectrumConnectionMode::Proxy(proxy))
                    .await
                    .is_err()
            );
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

    async fn connected_regtest_client(env: &TestEnv) -> ElectrumClient {
        let addr =
            ElectrumServerAddress::parse(&format!("tcp://{}", env.electrsd.electrum_url)).unwrap();
        let client = ElectrumClient::new(addr);
        client.connect();
        wait_connected(&client).await;
        client
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
    async fn get_tip_and_history_from_regtest() {
        let env = TestEnv::new();

        let tracked_address = env.bitcoind.client.new_address().unwrap();
        env.bitcoind
            .client
            .generate_to_address(1, &tracked_address)
            .unwrap();

        let target_height = env.bitcoind.client.get_blockchain_info().unwrap().blocks as usize;
        env.electrsd.wait_height(target_height);

        let client = connected_regtest_client(&env).await;

        let tip = client.get_tip().await.unwrap();
        assert!(
            (tip.height as usize) >= target_height,
            "tip {} should be >= {}",
            tip.height,
            target_height
        );

        let script_hash = ElectrumScriptHash::new(&tracked_address.script_pubkey());
        let history = client.script_get_history(script_hash).await.unwrap();
        assert!(
            !history.is_empty(),
            "expected non-empty history for tracked spk"
        );
    }

    #[tokio::test]
    async fn receives_block_header_notifications() {
        let env = TestEnv::new();
        let client = connected_regtest_client(&env).await;

        let mut notifications = client.notifications();
        let tip = client.get_tip().await.unwrap();
        let expected_height = tip.height + 1;

        let mine_to = env.bitcoind.client.new_address().unwrap();
        env.bitcoind
            .client
            .generate_to_address(1, &mine_to)
            .unwrap();
        env.electrsd.wait_height(expected_height as usize);

        let saw_header = timeout(Duration::from_secs(15), async {
            loop {
                match notifications.recv().await {
                    Ok(ElectrumNotification::BlockHeader { height, .. })
                        if height >= expected_height =>
                    {
                        return true;
                    }
                    Ok(_) => {}
                    Err(_) => return false,
                }
            }
        })
        .await
        .unwrap_or(false);

        assert!(
            saw_header,
            "did not receive expected block header notification"
        );
    }

    #[tokio::test]
    async fn disconnect_emits_shutdown_notification() {
        let env = TestEnv::new();
        let client = connected_regtest_client(&env).await;
        let mut notifications = client.notifications();

        client.disconnect();

        let got_shutdown = timeout(Duration::from_secs(10), async {
            loop {
                match notifications.recv().await {
                    Ok(ElectrumNotification::Shutdown) => return true,
                    Ok(_) => {}
                    Err(_) => return false,
                }
            }
        })
        .await
        .unwrap_or(false);

        assert!(
            got_shutdown,
            "expected shutdown notification after disconnect"
        );
    }

    #[tokio::test]
    async fn script_subscribe_and_unsubscribe_flow() {
        let env = TestEnv::new();
        ensure_funded_wallet(&env);
        let client = connected_regtest_client(&env).await;
        let mut notifications = client.notifications();

        let tracked_address = env.bitcoind.client.new_address().unwrap();
        let tracked_hash = ElectrumScriptHash::new(&tracked_address.script_pubkey());

        client.script_hash_subscribe(tracked_hash).await.unwrap();

        let txid = env
            .bitcoind
            .client
            .send_to_address(&tracked_address, Amount::from_sat(25_000))
            .unwrap()
            .txid()
            .unwrap();
        env.electrsd.wait_tx(&txid);

        let saw_subscribed_update = timeout(Duration::from_secs(15), async {
            loop {
                match notifications.recv().await {
                    Ok(ElectrumNotification::ScriptHash { hash, .. }) if hash == tracked_hash => {
                        return true;
                    }
                    Ok(_) => {}
                    Err(_) => return false,
                }
            }
        })
        .await
        .unwrap_or(false);

        assert!(
            saw_subscribed_update,
            "expected script notification while subscribed"
        );

        client.script_hash_unsubscribe(tracked_hash).await.unwrap();
        client.script_hash_subscribe(tracked_hash).await.unwrap();

        // Drain queue to avoid matching stale events from the previous tx.
        while notifications.try_recv().is_ok() {}

        let txid = env
            .bitcoind
            .client
            .send_to_address(&tracked_address, Amount::from_sat(26_000))
            .unwrap()
            .txid()
            .unwrap();
        env.electrsd.wait_tx(&txid);

        let saw_resubscribed_update = timeout(Duration::from_secs(15), async {
            loop {
                match notifications.recv().await {
                    Ok(ElectrumNotification::ScriptHash { hash, .. }) if hash == tracked_hash => {
                        return true;
                    }
                    Ok(_) => {}
                    Err(_) => return false,
                }
            }
        })
        .await
        .unwrap_or(false);

        assert!(
            saw_resubscribed_update,
            "expected script notification after re-subscribing"
        );
    }

    #[tokio::test]
    async fn api_roundtrip_for_batch_history_tx_and_merkle() {
        let env = TestEnv::new();
        ensure_funded_wallet(&env);
        let client = connected_regtest_client(&env).await;

        let addr_a = env.bitcoind.client.new_address().unwrap();
        let addr_b = env.bitcoind.client.new_address().unwrap();

        let txid_a = env
            .bitcoind
            .client
            .send_to_address(&addr_a, Amount::from_sat(40_000))
            .unwrap()
            .txid()
            .unwrap();
        let _txid_b = env
            .bitcoind
            .client
            .send_to_address(&addr_b, Amount::from_sat(41_000))
            .unwrap()
            .txid()
            .unwrap();

        env.electrsd.wait_tx(&txid_a);
        env.bitcoind
            .client
            .generate_to_address(1, &env.bitcoind.client.new_address().unwrap())
            .unwrap();
        let tip_height = env.bitcoind.client.get_blockchain_info().unwrap().blocks as usize;
        env.electrsd.wait_height(tip_height);
        wait_connected(&client).await;

        let tip = client.get_tip().await.unwrap();
        let tip_header = client.block_header(tip.height).await.unwrap();
        assert_eq!(tip_header.block_hash(), tip.header.block_hash());

        let headers = client
            .block_headers(tip.height.saturating_sub(1), 2)
            .await
            .unwrap();
        assert!(!headers.headers.is_empty(), "expected headers response");

        client.block_headers_subscribe().await.unwrap();

        let hash_a = ElectrumScriptHash::new(&addr_a.script_pubkey());
        let hash_b = ElectrumScriptHash::new(&addr_b.script_pubkey());

        let single_history = client.script_get_history(hash_a).await.unwrap();
        assert!(
            !single_history.is_empty(),
            "expected history for first script"
        );

        let batch_history = client
            .batch_script_get_history([hash_a, hash_b])
            .await
            .unwrap();
        assert_eq!(batch_history.len(), 2);
        assert!(
            batch_history.iter().all(|h| !h.is_empty()),
            "expected both batch histories to be non-empty"
        );

        let tx = client.transaction_get(txid_a).await.unwrap();
        assert_eq!(tx.compute_txid(), txid_a);
        let batched = client.batch_transaction_get([txid_a]).await.unwrap();
        assert_eq!(batched.len(), 1);
        assert_eq!(batched[0].compute_txid(), txid_a);

        let confirmed = single_history
            .into_iter()
            .find(|entry| entry.electrum_height() > 0)
            .expect("expected a confirmed entry");
        let confirmation_height: u32 = confirmed.electrum_height().try_into().unwrap();

        let merkle = client
            .transaction_get_merkle(confirmed.txid(), confirmation_height)
            .await
            .unwrap();
        assert_eq!(merkle.block_height.to_consensus_u32(), confirmation_height);
        let merkle_batch = client
            .batch_transaction_get_merkle([(confirmed.txid(), confirmation_height)])
            .await
            .unwrap();
        assert_eq!(merkle_batch.len(), 1);
        let batch_entry = merkle_batch.into_iter().next().unwrap().unwrap();
        assert_eq!(
            batch_entry.block_height.to_consensus_u32(),
            confirmation_height
        );

        let _ = client.estimate_fee(2).await.unwrap();
        let fee_estimates = client.batch_estimate_fee([1_usize, 2, 6]).await.unwrap();
        assert!(fee_estimates.keys().all(|k| [1_usize, 2, 6].contains(k)));
    }

    #[tokio::test]
    async fn request_timeout_without_connection() {
        let addr = ElectrumServerAddress::parse("tcp://127.0.0.1:50001").unwrap();
        let client = ElectrumClient::builder(addr)
            .request_timeout(Duration::from_millis(50))
            .build();

        let err = client.server_features().await.expect_err("must timeout");
        assert!(matches!(err, Error::Timeout));
    }
}

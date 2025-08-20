use std::collections::HashSet;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bitcoin::block::Header;
use bitcoin::{Script, Transaction, Txid};
use electrum_streaming_client::notification::Notification;
use electrum_streaming_client::request::{
    GetHistory, GetTx, Header as GetBlockHeader, HeadersSubscribe, Ping, ScriptHashSubscribe,
    ScriptHashUnsubscribe,
};
use electrum_streaming_client::response::Tx;
use electrum_streaming_client::{
    AsyncClient, AsyncEventReceiver, AsyncPendingRequest, AsyncPendingRequestTuple,
    AsyncRequestError, AsyncRequestSendError, ErroredRequest, Event, Request, ResponseError,
    SatisfiedRequest,
};
use futures::StreamExt;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{broadcast, mpsc, Mutex, MutexGuard, Notify};
use tokio::time;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::pki_types::{InvalidDnsNameError, ServerName};
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;

use crate::address::{ElectrumServerAddress, HostAndPort, Scheme};
use crate::config::ElectrumConfig;
use crate::constant::PING_INTERVAL;
use crate::hash::ElectrumScriptHash;
use crate::notification::{ElectrumNotification, InternalNotification};
use crate::status::{AtomicElectrumConnectionStatus, ElectrumConnectionStatus};

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
    /// Electrum async request error
    #[error(transparent)]
    AsyncRequest(#[from] AsyncRequestError),
    /// Response error
    #[error(transparent)]
    Response(#[from] ResponseError),
    #[error("{0}")]
    MpscCommandTrySend(String),
    /// Timeout
    #[error("timeout")]
    Timeout,
    /// Disconnected
    #[error("disconnected")]
    Disconnected,
    /// Termination request
    #[error("termination request")]
    TerminationRequest,
    /// Premature exit
    #[error("premature exit")]
    PrematureExit,
}

enum Command {
    BlockHeader { height: u32 },
    HeadersSubscribe,
    ScriptHashSubscribe(ElectrumScriptHash),
    ScriptHashUnsubscribe(ElectrumScriptHash),
    GetHistory(ElectrumScriptHash),
    GetTransaction { txid: Txid },
}

#[derive(Debug)]
struct Channels {
    commands: (Sender<Vec<Command>>, Mutex<Receiver<Vec<Command>>>),
    ping: Notify,
    terminate: Notify,
}

impl Channels {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(4096);
        let rx = Mutex::new(rx);

        Self {
            commands: (tx, rx),
            ping: Notify::new(),
            terminate: Notify::new(),
        }
    }

    #[inline]
    pub async fn rx_commands(&self) -> MutexGuard<'_, Receiver<Vec<Command>>> {
        self.commands.1.lock().await
    }

    #[inline]
    pub fn ping(&self) {
        self.ping.notify_one()
    }

    #[inline]
    pub fn terminate(&self) {
        self.terminate.notify_one()
    }
}

#[derive(Debug, Default)]
struct ServicesTracker {
    headers_subscribed: bool,
    script_hashes: HashSet<ElectrumScriptHash>,
}

impl ServicesTracker {
    #[inline]
    fn is_headers_subscribed(&self) -> bool {
        self.headers_subscribed
    }

    #[inline]
    fn set_headers_subscribed(&mut self, value: bool) {
        self.headers_subscribed = value
    }
}

#[derive(Debug, Clone)]
pub struct ElectrumClient {
    addr: ElectrumServerAddress,
    status: Arc<AtomicElectrumConnectionStatus>,
    running: Arc<AtomicBool>,
    channels: Arc<Channels>,
    /// Internal notifications
    internal_notifications: broadcast::Sender<InternalNotification>,
    /// External notification sender
    notification_sender: broadcast::Sender<ElectrumNotification>,
    config: ElectrumConfig,
}

impl ElectrumClient {
    /// Construct a new electrum client
    #[inline]
    pub fn new(addr: ElectrumServerAddress) -> Self {
        Self::with_config(addr, ElectrumConfig::default())
    }

    /// Construct a new electrum client
    pub fn with_config(addr: ElectrumServerAddress, config: ElectrumConfig) -> Self {
        let (notification_sender, ..) = broadcast::channel(config.notification_channel_size);
        let (internal_notifications, ..) = broadcast::channel(config.notification_channel_size);

        Self {
            addr,
            status: Arc::new(AtomicElectrumConnectionStatus::default()),
            running: Arc::new(AtomicBool::new(false)),
            channels: Arc::new(Channels::new()),
            internal_notifications,
            notification_sender,
            config,
        }
    }

    /// Check if the connection task is running
    #[inline]
    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Subscribe to notifications
    ///
    /// When you call this method, you subscribe to the notifications channel from that precise moment.
    /// Anything received by client before that moment is not included in the channel!
    #[inline]
    pub fn notifications(&self) -> broadcast::Receiver<ElectrumNotification> {
        self.notification_sender.subscribe()
    }

    #[inline]
    fn send_notification(&self, notification: InternalNotification, external: bool) {
        if external {
            let _ = self.internal_notifications.send(notification.clone());

            // Send external notification
            if let InternalNotification::Notification(notification) = notification {
                let _ = self.notification_sender.send(notification);
            }
        } else {
            let _ = self.internal_notifications.send(notification);
        }
    }

    /// Get the current connection status
    #[inline]
    pub fn status(&self) -> ElectrumConnectionStatus {
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
        self.send_notification(
            InternalNotification::Notification(ElectrumNotification::ConnectionStatusChanged(
                status,
            )),
            true,
        );
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
        self.set_status(ElectrumConnectionStatus::Pending, false);

        // Spawn connection task
        self.spawn_connection_task();
    }

    fn spawn_connection_task(&self) {
        // Check if the connection task is already running
        // This is checked also later, but it's checked also here to avoid a full-clone if we know that is already running.
        if self.is_running() {
            tracing::warn!(addr = %self.addr, "Electrum connection task is already running.");
            return;
        }

        // Full-clone
        let client = self.clone();

        // Spawn task
        tokio::spawn(client.connection_task());
    }

    /// This **MUST** be called only by the [`Self::spawn_connection_task`] method!
    async fn connection_task(self) {
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
        let mut rx_commands = self.channels.rx_commands().await;

        // Build a new default service tracker
        // This store the data until the client is terminated
        let mut service_tracker = ServicesTracker::default();

        // Auto-connect loop
        loop {
            // Connect and run message handler
            // The termination requests are handled inside this method!
            self.connect_and_run(&mut rx_commands, &mut service_tracker)
                .await;

            // Get status
            let status: ElectrumConnectionStatus = self.status();

            // If the connection is terminated, break the loop.
            if status.is_terminated() {
                break;
            }

            // Check if the relay is marked as disconnected. If not, update status.
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

    #[inline]
    async fn handle_terminate(&self) {
        // Wait to be notified
        self.channels.terminate.notified().await;
    }

    async fn _try_connect(
        &self,
        timeout: Duration,
    ) -> Result<(BoxReadStream, BoxWriteStream), Error> {
        // Update status
        self.set_status(ElectrumConnectionStatus::Connecting, true);

        // Try to connect
        // If during connection the termination request is received, abort the connection and return error.
        // At this stem is NOT required to close the WebSocket connection.
        tokio::select! {
            // Connect
            res = connect(&self.addr, timeout) => match res {
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
        rx_commands: &mut MutexGuard<'_, Receiver<Vec<Command>>>,
        services_tracker: &mut ServicesTracker,
    ) {
        match self._try_connect(self.config.connection_timeout).await {
            // Connection success, go to post-connection stage
            Ok((reader, writer)) => {
                self.post_connection(reader, writer, rx_commands, services_tracker)
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
        rx_commands: &mut MutexGuard<'_, Receiver<Vec<Command>>>,
        services_tracker: &mut ServicesTracker,
    ) {
        // Construct new electrum async client
        let (client, receiver, worker) = AsyncClient::new_tokio(reader, writer);

        // If the header subscription was enabled, resubscribe.
        if services_tracker.is_headers_subscribed() {
            tracing::debug!(addr = %self.addr, "Resubscribing to headers.");
            if let Err(e) = self.send_command(Command::HeadersSubscribe) {
                tracing::error!(addr = %self.addr, error = %e, "Error during headers resubscribe.");
            }
        }

        // If are cached any script hashes, resubscribe.
        if !services_tracker.script_hashes.is_empty() {
            tracing::debug!(addr = %self.addr, "Resubscribing to script hashes.");

            let commands: Vec<Command> = services_tracker
                .script_hashes
                .iter()
                .copied()
                .map(Command::ScriptHashSubscribe)
                .collect();

            if let Err(e) = self.batch_commands(commands) {
                tracing::error!(addr = %self.addr, error = %e, "Error during headers resubscribe.");
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
            res = self.sender_message_handler(&client, rx_commands, services_tracker) => match res {
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

        // Close
        client.close();
    }

    async fn sender_message_handler(
        &self,
        client: &AsyncClient,
        rx_command: &mut MutexGuard<'_, Receiver<Vec<Command>>>,
        services_tracker: &mut ServicesTracker,
    ) -> Result<(), Error> {
        loop {
            tokio::select! {
                // Commands receiver
                Some(commands) = rx_command.recv() => {
                    for command in commands.into_iter() {
                        match command {
                            Command::BlockHeader {height} => {
                                client.send_event_request(GetBlockHeader { height })?;
                            }
                            Command::HeadersSubscribe => {
                                // Mark as subscribed
                                // This allows to automatically re-subscribe in case of disconnection.
                                services_tracker.set_headers_subscribed(true);

                                // Send request
                                client.send_event_request(HeadersSubscribe)?;
                            }
                            Command::ScriptHashSubscribe(script_hash) => {
                                // Cache it
                                // Return true if is successfully cached, meaning wasn't already inserted
                                if services_tracker.script_hashes.insert(script_hash) {
                                    // Send to electrum
                                    client.send_event_request(ScriptHashSubscribe {
                                        script_hash,
                                    })?;
                                }
                            }
                            Command::ScriptHashUnsubscribe(script_hash) => {
                                // Remove it
                                // Return true if is successfully removed
                                // If returns false, it was never subscribed, so there is no reason to send unsubscribe
                                if services_tracker.script_hashes.remove(&script_hash) {
                                    // Send to electrum
                                    client.send_event_request(ScriptHashUnsubscribe {
                                        script_hash,
                                    })?;
                                }
                            }
                            Command::GetHistory(script_hash) => {
                                // Send to electrum
                                client.send_event_request(GetHistory {
                                    script_hash,
                                })?;
                            }
                            Command::GetTransaction { txid } => {
                                client.send_event_request(GetTx { txid })?;
                            }
                        }
                    }
                }
                // Ping channel receiver
                _ = self.channels.ping.notified() => {
                    tracing::trace!(addr = %self.addr, "Sending ping.");
                    send_request_with_timeout(client, Duration::from_secs(10), Ping).await?;
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
            // Send event notification
            self.send_notification(InternalNotification::Event(event.clone()), false);

            let notification: Option<ElectrumNotification> = match event {
                Event::Response(res) => match res {
                    SatisfiedRequest::HeadersSubscribe { resp, .. } => {
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
                self.send_notification(InternalNotification::Notification(notification), true);
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

    pub fn disconnect(&self) {
        let status = self.status();

        // Check if it's already terminated or banned
        if status.is_terminated() {
            return;
        }

        // Notify termination
        self.channels.terminate();

        // Update status
        self.set_status(ElectrumConnectionStatus::Terminated, true);

        // Shutdown all notification loops
        self.send_notification(
            InternalNotification::Notification(ElectrumNotification::Shutdown),
            true,
        );
    }

    #[inline]
    fn send_command(&self, command: Command) -> Result<(), Error> {
        self.batch_commands(vec![command])
    }

    #[inline]
    fn batch_commands(&self, commands: Vec<Command>) -> Result<(), Error> {
        self.channels
            .commands
            .0
            .try_send(commands)
            .map_err(|e| Error::MpscCommandTrySend(e.to_string()))
    }

    pub async fn block_header(&self, height: u32) -> Result<Header, Error> {
        // Subscribe to notifications
        let mut notifications = self.internal_notifications.subscribe();

        // Send command
        self.send_command(Command::BlockHeader { height })?;

        // Wait for response
        handle_notification_events(&mut notifications, |event| {
            match event {
                Event::Response(SatisfiedRequest::Header { req, resp }) => {
                    if req.height == height {
                        return Some(Ok(resp.header));
                    }
                }
                Event::ResponseError(ErroredRequest::Header { req, error }) => {
                    if req.height == height {
                        return Some(Err(Error::Response(error)));
                    }
                }
                _ => {}
            }
            None
        })
        .await
    }

    /// Subscribe to headers
    ///
    /// The updates can be monitored with [`ElectrumClient::notifications`].
    pub fn subscribe_headers(&self) -> Result<(), Error> {
        self.send_command(Command::HeadersSubscribe)?;
        Ok(())
    }

    /// Subscribe to script hash
    ///
    /// The updates can be monitored with [`ElectrumClient::notifications`].
    pub fn subscribe_script_hash(&self, script_hash: ElectrumScriptHash) -> Result<(), Error> {
        self.send_command(Command::ScriptHashSubscribe(script_hash))?;
        Ok(())
    }

    /// Subscribe to script hashes
    ///
    /// The updates can be monitored with [`ElectrumClient::notifications`].
    pub fn batch_subscribe_script_hashes<I>(&self, script_hashes: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = ElectrumScriptHash>,
    {
        let commands: Vec<Command> = script_hashes
            .into_iter()
            .map(Command::ScriptHashSubscribe)
            .collect();
        self.batch_commands(commands)?;
        Ok(())
    }

    /// Unsubscribe from a script hash
    pub fn unsubscribe_script_hash(&self, script_hash: ElectrumScriptHash) -> Result<(), Error> {
        self.send_command(Command::ScriptHashUnsubscribe(script_hash))?;
        Ok(())
    }

    /// Request history for scripts
    pub async fn script_get_history(&self, script: &Script) -> Result<Vec<Tx>, Error> {
        let script_hash = ElectrumScriptHash::new(script);

        // Subscribe to notifications
        let mut notifications = self.internal_notifications.subscribe();

        // Send command
        self.send_command(Command::GetHistory(script_hash))?;

        // Wait for response
        handle_notification_events(&mut notifications, |event| {
            match event {
                Event::Response(SatisfiedRequest::GetHistory { req, resp }) => {
                    if req.script_hash == script_hash {
                        return Some(Ok(resp));
                    }
                }
                Event::ResponseError(ErroredRequest::GetHistory { req, error }) => {
                    if req.script_hash == script_hash {
                        return Some(Err(Error::Response(error)));
                    }
                }
                _ => {}
            }
            None
        })
        .await
    }

    /// Request history for scripts
    pub async fn get_transaction(&self, txid: Txid) -> Result<Transaction, Error> {
        // Subscribe to notifications
        let mut notifications = self.internal_notifications.subscribe();

        // Send command
        self.send_command(Command::GetTransaction { txid })?;

        // Wait for response
        handle_notification_events(&mut notifications, |event| {
            match event {
                Event::Response(SatisfiedRequest::GetTx { req, resp }) => {
                    if req.txid == txid {
                        return Some(Ok(resp.tx));
                    }
                }
                Event::ResponseError(ErroredRequest::GetTx { req, error }) => {
                    if req.txid == txid {
                        return Some(Err(Error::Response(error)));
                    }
                }
                _ => {}
            }
            None
        })
        .await
    }
}

async fn connect(
    addr: &ElectrumServerAddress,
    timeout: Duration,
) -> Result<(BoxReadStream, BoxWriteStream), Error> {
    match addr.scheme() {
        Scheme::Tcp => time::timeout(timeout, connect_tcp(addr.addr()))
            .await
            .map_err(|_| Error::Timeout)?,
        Scheme::Ssl => time::timeout(timeout, connect_ssl(addr.addr()))
            .await
            .map_err(|_| Error::Timeout)?,
    }
}

async fn connect_tcp(addr: &HostAndPort) -> Result<(BoxReadStream, BoxWriteStream), Error> {
    // Connect
    let stream: TcpStream = TcpStream::connect(addr.to_string()).await?;

    // Split stream
    let (reader, writer) = tokio::io::split(stream);

    // Box split stream
    Ok((Box::new(reader), Box::new(writer)))
}

async fn connect_ssl(addr: &HostAndPort) -> Result<(BoxReadStream, BoxWriteStream), Error> {
    // Create TLS configuration
    let mut root_cert_store: RootCertStore = RootCertStore::empty();
    root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let config: ClientConfig = ClientConfig::builder()
        .with_root_certificates(root_cert_store)
        .with_no_client_auth();

    let connector: TlsConnector = TlsConnector::from(Arc::new(config));

    let hostname: String = addr.host.to_string();

    // Connect to the server
    let tcp_stream: TcpStream = TcpStream::connect(addr.to_string()).await?;
    let domain: ServerName = ServerName::try_from(hostname)?;
    let tls_stream: TlsStream<TcpStream> = connector.connect(domain, tcp_stream).await?;

    // Split stream
    let (reader, writer) = tokio::io::split(tls_stream);

    // Box split stream
    Ok((Box::new(reader), Box::new(writer)))
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

async fn handle_notification_events<F, T>(
    notifications: &mut broadcast::Receiver<InternalNotification>,
    func: F,
) -> Result<T, Error>
where
    F: Fn(Event) -> Option<Result<T, Error>>,
{
    while let Ok(notification) = notifications.recv().await {
        match notification {
            InternalNotification::Event(event) => {
                if let Some(output) = func(event) {
                    return output;
                }
            }
            InternalNotification::Notification(notification) => match notification {
                ElectrumNotification::ConnectionStatusChanged(status) => {
                    if status.is_disconnected() {
                        return Err(Error::Disconnected);
                    }
                }
                ElectrumNotification::Shutdown => break,
                _ => {}
            },
        }
    }

    Err(Error::PrematureExit)
}

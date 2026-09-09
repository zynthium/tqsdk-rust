#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use bytes::BytesMut;
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex as AsyncMutex, Notify, mpsc, oneshot};
use tokio::time::timeout;
use tokio_util::codec::Decoder as TokioDecoder;
use yawc::{
    Role,
    codec::Decoder as WebSocketDecoder,
    frame::{Frame, OpCode},
};

use crate::engine::{DownstreamFrame, RelayEngine};
use crate::error::{RelayError, RelayResult};
use crate::interest::ClientId;
use crate::protocol::DownstreamCommand;
use crate::upstream::{
    UpstreamMarketEvent, UpstreamSourceProgress, UpstreamSourceUpdate, UpstreamTickSource,
};

const WS_ACCEPT_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const DEFAULT_OUTBOUND_CHANNEL_CAPACITY: usize = 1024;
const DEFAULT_OUTBOUND_BYTE_CAPACITY: usize = 8 * 1024 * 1024;
const DEFAULT_MAX_HEADER_BYTES: usize = 16 * 1024;
const DEFAULT_MAX_FRAME_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_MESSAGE_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_MAX_CONNECTIONS: usize = 1024;
const UPSTREAM_SUBSCRIPTION_SIGNAL_CAPACITY: usize = 1;

type SharedWebSocketFrame = Arc<[u8]>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MailboxPush {
    Queued,
    DroppedMarket,
    Full,
    Closed,
}

#[derive(Debug)]
enum MailboxReceive {
    Frame(SharedWebSocketFrame),
    Empty,
    Closed,
}

#[derive(Debug, Default)]
struct OutboundMailboxState {
    quotes: HashMap<Arc<str>, SharedWebSocketFrame>,
    quote_order: VecDeque<Arc<str>>,
    quote_bytes: usize,
    reliable: VecDeque<SharedWebSocketFrame>,
    reliable_bytes: usize,
    closed: bool,
}

impl OutboundMailboxState {
    fn frame_count(&self) -> usize {
        self.quotes.len().saturating_add(self.reliable.len())
    }

    fn retained_bytes(&self) -> usize {
        self.quote_bytes.saturating_add(self.reliable_bytes)
    }
}

/// Per-client bounded mailbox.
///
/// Quote state is coalesced by symbol. Other relay frames are reliable: a full
/// mailbox makes the client stale instead of silently dropping a chart update.
#[derive(Debug)]
struct OutboundMailbox {
    state: Mutex<OutboundMailboxState>,
    ready: Notify,
    frame_capacity: usize,
    byte_capacity: usize,
}

impl OutboundMailbox {
    fn new(frame_capacity: usize, byte_capacity: usize) -> Self {
        Self {
            state: Mutex::new(OutboundMailboxState::default()),
            ready: Notify::new(),
            frame_capacity: frame_capacity.max(1),
            byte_capacity: byte_capacity.max(1),
        }
    }

    fn try_enqueue(&self, frame: SharedWebSocketFrame, quote_symbol: Option<&str>) -> MailboxPush {
        let result = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if state.closed {
                MailboxPush::Closed
            } else if let Some(symbol) = quote_symbol {
                Self::enqueue_quote(
                    &mut state,
                    self.frame_capacity,
                    self.byte_capacity,
                    Arc::from(symbol),
                    frame,
                )
            } else {
                Self::enqueue_reliable(&mut state, self.frame_capacity, self.byte_capacity, frame)
            }
        };
        if matches!(result, MailboxPush::Queued) {
            self.ready.notify_one();
        }
        result
    }

    fn enqueue_quote(
        state: &mut OutboundMailboxState,
        frame_capacity: usize,
        byte_capacity: usize,
        symbol: Arc<str>,
        frame: SharedWebSocketFrame,
    ) -> MailboxPush {
        let frame_bytes = frame.len();
        if frame_bytes > byte_capacity {
            return MailboxPush::DroppedMarket;
        }

        let mut replacing = state.quotes.contains_key(symbol.as_ref());
        let mut replaced_bytes = state
            .quotes
            .get(symbol.as_ref())
            .map_or(0, |frame| frame.len());
        loop {
            let frame_count = state.frame_count().saturating_add(usize::from(!replacing));
            let retained_bytes = state
                .retained_bytes()
                .saturating_sub(replaced_bytes)
                .saturating_add(frame_bytes);
            if frame_count <= frame_capacity && retained_bytes <= byte_capacity {
                break;
            }

            let Some(evicted_symbol) = Self::evict_oldest_quote(state) else {
                return MailboxPush::DroppedMarket;
            };
            if evicted_symbol.as_ref() == symbol.as_ref() {
                replacing = false;
                replaced_bytes = 0;
            }
        }

        if replacing {
            let previous = state
                .quotes
                .insert(symbol, frame)
                .expect("existing quote frame must remain queued");
            state.quote_bytes = state
                .quote_bytes
                .saturating_sub(previous.len())
                .saturating_add(frame_bytes);
        } else {
            state.quote_bytes = state.quote_bytes.saturating_add(frame_bytes);
            state.quote_order.push_back(Arc::clone(&symbol));
            state.quotes.insert(symbol, frame);
        }
        MailboxPush::Queued
    }

    fn enqueue_reliable(
        state: &mut OutboundMailboxState,
        frame_capacity: usize,
        byte_capacity: usize,
        frame: SharedWebSocketFrame,
    ) -> MailboxPush {
        let frame_bytes = frame.len();
        if frame_bytes > byte_capacity {
            return MailboxPush::Full;
        }

        while state.frame_count() >= frame_capacity
            || state.retained_bytes().saturating_add(frame_bytes) > byte_capacity
        {
            if Self::evict_oldest_quote(state).is_none() {
                return MailboxPush::Full;
            }
        }

        state.reliable_bytes = state.reliable_bytes.saturating_add(frame_bytes);
        state.reliable.push_back(frame);
        MailboxPush::Queued
    }

    fn evict_oldest_quote(state: &mut OutboundMailboxState) -> Option<Arc<str>> {
        while let Some(symbol) = state.quote_order.pop_front() {
            if let Some(frame) = state.quotes.remove(symbol.as_ref()) {
                state.quote_bytes = state.quote_bytes.saturating_sub(frame.len());
                return Some(symbol);
            }
        }
        None
    }

    async fn recv(&self) -> Option<SharedWebSocketFrame> {
        loop {
            let notified = self.ready.notified();
            match self.try_recv() {
                MailboxReceive::Frame(frame) => return Some(frame),
                MailboxReceive::Closed => return None,
                MailboxReceive::Empty => notified.await,
            }
        }
    }

    fn try_recv(&self) -> MailboxReceive {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(frame) = state.reliable.pop_front() {
            state.reliable_bytes = state.reliable_bytes.saturating_sub(frame.len());
            return MailboxReceive::Frame(frame);
        }
        while let Some(symbol) = state.quote_order.pop_front() {
            if let Some(frame) = state.quotes.remove(symbol.as_ref()) {
                state.quote_bytes = state.quote_bytes.saturating_sub(frame.len());
                return MailboxReceive::Frame(frame);
            }
        }
        if state.closed {
            MailboxReceive::Closed
        } else {
            MailboxReceive::Empty
        }
    }

    fn close(&self) {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .closed = true;
        self.ready.notify_waiters();
    }
}

/// Explicit resource and timeout bounds for downstream WebSocket clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelayServerLimits {
    pub handshake_timeout: Duration,
    pub write_timeout: Duration,
    pub max_header_bytes: usize,
    pub max_frame_bytes: usize,
    pub max_message_bytes: usize,
    pub max_connections: usize,
}

impl Default for RelayServerLimits {
    fn default() -> Self {
        Self {
            handshake_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(10),
            max_header_bytes: DEFAULT_MAX_HEADER_BYTES,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            max_connections: DEFAULT_MAX_CONNECTIONS,
        }
    }
}

impl RelayServerLimits {
    #[must_use]
    pub fn sanitized(self) -> Self {
        Self {
            handshake_timeout: self.handshake_timeout.max(Duration::from_millis(1)),
            write_timeout: self.write_timeout.max(Duration::from_millis(1)),
            max_header_bytes: self.max_header_bytes.max(1),
            max_frame_bytes: self.max_frame_bytes.max(1),
            max_message_bytes: self.max_message_bytes.max(1),
            max_connections: self.max_connections.max(1),
        }
    }
}

enum ClientWebSocketFrame {
    Text(String),
    Ping(Vec<u8>),
    Pong,
    Close,
}

#[derive(Clone)]
pub struct RelayServer {
    engine: Arc<Mutex<RelayEngine>>,
    outbound: Arc<Mutex<HashMap<ClientId, Arc<OutboundMailbox>>>>,
    limits: RelayServerLimits,
    active_connections: Arc<AtomicUsize>,
    upstream_subscription_tx: mpsc::Sender<()>,
    upstream_subscription_rx: Arc<AsyncMutex<mpsc::Receiver<()>>>,
    pending_upstream_subscription_symbols: Arc<Mutex<BTreeSet<String>>>,
    outbound_channel_capacity: usize,
    outbound_byte_capacity: usize,
}

impl RelayServer {
    #[must_use]
    pub fn new(engine: Arc<Mutex<RelayEngine>>) -> Self {
        Self::with_limits_and_outbound_capacity(
            engine,
            RelayServerLimits::default(),
            DEFAULT_OUTBOUND_CHANNEL_CAPACITY,
        )
    }

    #[must_use]
    pub fn with_outbound_capacity(
        engine: Arc<Mutex<RelayEngine>>,
        outbound_channel_capacity: usize,
    ) -> Self {
        Self::with_limits_and_outbound_budget(
            engine,
            RelayServerLimits::default(),
            outbound_channel_capacity,
            DEFAULT_OUTBOUND_BYTE_CAPACITY,
        )
    }

    /// Sets explicit per-client frame and encoded-byte limits for outbound data.
    #[must_use]
    pub fn with_outbound_budget(
        engine: Arc<Mutex<RelayEngine>>,
        outbound_channel_capacity: usize,
        outbound_byte_capacity: usize,
    ) -> Self {
        Self::with_limits_and_outbound_budget(
            engine,
            RelayServerLimits::default(),
            outbound_channel_capacity,
            outbound_byte_capacity,
        )
    }

    #[must_use]
    pub fn with_limits(engine: Arc<Mutex<RelayEngine>>, limits: RelayServerLimits) -> Self {
        Self::with_limits_and_outbound_capacity(engine, limits, DEFAULT_OUTBOUND_CHANNEL_CAPACITY)
    }

    #[must_use]
    pub fn with_limits_and_outbound_capacity(
        engine: Arc<Mutex<RelayEngine>>,
        limits: RelayServerLimits,
        outbound_channel_capacity: usize,
    ) -> Self {
        Self::with_limits_and_outbound_budget(
            engine,
            limits,
            outbound_channel_capacity,
            DEFAULT_OUTBOUND_BYTE_CAPACITY,
        )
    }

    /// Sets explicit downstream transport and per-client mailbox resource bounds.
    #[must_use]
    pub fn with_limits_and_outbound_budget(
        engine: Arc<Mutex<RelayEngine>>,
        limits: RelayServerLimits,
        outbound_channel_capacity: usize,
        outbound_byte_capacity: usize,
    ) -> Self {
        let outbound_channel_capacity = outbound_channel_capacity.max(1);
        let outbound_byte_capacity = outbound_byte_capacity.max(1);
        let limits = limits.sanitized();
        let (upstream_subscription_tx, upstream_subscription_rx) =
            mpsc::channel(UPSTREAM_SUBSCRIPTION_SIGNAL_CAPACITY);
        Self {
            engine,
            outbound: Arc::new(Mutex::new(HashMap::new())),
            limits,
            active_connections: Arc::new(AtomicUsize::new(0)),
            upstream_subscription_tx,
            upstream_subscription_rx: Arc::new(AsyncMutex::new(upstream_subscription_rx)),
            pending_upstream_subscription_symbols: Arc::new(Mutex::new(BTreeSet::new())),
            outbound_channel_capacity,
            outbound_byte_capacity,
        }
    }

    #[must_use]
    pub fn limits(&self) -> RelayServerLimits {
        self.limits
    }

    #[must_use]
    pub fn engine(&self) -> Arc<Mutex<RelayEngine>> {
        self.engine.clone()
    }

    pub fn dispatch_frames<I>(&self, frames: I) -> RelayResult<usize>
    where
        I: IntoIterator<Item = DownstreamFrame>,
    {
        let mut sent = 0_usize;
        let mut stale_clients = BTreeSet::new();
        let mut encoded_by_payload = HashMap::<usize, SharedWebSocketFrame>::new();
        let deliveries = {
            let outbound = self
                .outbound
                .lock()
                .map_err(|_| RelayError::Internal("relay outbound lock poisoned".to_string()))?;
            frames
                .into_iter()
                .filter_map(|frame| {
                    outbound
                        .get(&frame.client_id)
                        .map(|mailbox| (frame, Arc::clone(mailbox)))
                })
                .collect::<Vec<_>>()
        };
        for (frame, mailbox) in deliveries {
            let payload_key = Arc::as_ptr(&frame.payload) as usize;
            let encoded = if let Some(encoded) = encoded_by_payload.get(&payload_key) {
                Arc::clone(encoded)
            } else {
                let encoded = encode_server_text_frame(frame.payload.as_ref(), self.limits)?;
                encoded_by_payload.insert(payload_key, Arc::clone(&encoded));
                encoded
            };
            match mailbox.try_enqueue(encoded, quote_symbol_from_payload(frame.payload.as_ref())) {
                MailboxPush::Queued => sent = sent.saturating_add(1),
                MailboxPush::DroppedMarket => {}
                MailboxPush::Full | MailboxPush::Closed => {
                    stale_clients.insert(frame.client_id);
                }
            }
        }
        if !stale_clients.is_empty() {
            let stale_mailboxes = {
                let mut outbound = self.outbound.lock().map_err(|_| {
                    RelayError::Internal("relay outbound lock poisoned".to_string())
                })?;
                stale_clients
                    .iter()
                    .filter_map(|client_id| outbound.remove(client_id))
                    .collect::<Vec<_>>()
            };
            for mailbox in stale_mailboxes {
                mailbox.close();
            }
            let mut engine = self
                .engine
                .lock()
                .map_err(|_| RelayError::Internal("relay engine lock poisoned".to_string()))?;
            for client_id in stale_clients {
                engine.remove_client(client_id);
            }
            drop(engine);
            self.enqueue_upstream_subscription_symbols(Vec::new())?;
        }
        Ok(sent)
    }

    pub fn request_pending_upstream_subscriptions(&self) -> RelayResult<()> {
        let symbols = {
            let mut engine = self
                .engine
                .lock()
                .map_err(|_| RelayError::Internal("relay engine lock poisoned".to_string()))?;
            engine.queue_missing_upstream_symbols_for_current_interests();
            engine.drain_pending_upstream_subscription_symbols()
        };
        self.enqueue_upstream_subscription_symbols(symbols)
    }

    pub async fn next_upstream_subscription_symbols(&self) -> Option<Vec<String>> {
        self.upstream_subscription_rx.lock().await.recv().await?;
        let mut pending = self
            .pending_upstream_subscription_symbols
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Some(std::mem::take(&mut *pending).into_iter().collect())
    }

    pub async fn pump_upstream_once<S>(&self, source: &mut S) -> RelayResult<usize>
    where
        S: UpstreamTickSource + Send,
    {
        let update = source.next_update().await;
        let progress = source.take_progress();
        let invalid_rows = source.take_invalid_tick_rows();
        let invalid_rows_by_symbol = source.take_invalid_tick_rows_by_symbol();
        let last_error = source.take_last_invalid_tick_row_error();
        self.process_upstream_update(
            update,
            progress,
            invalid_rows,
            invalid_rows_by_symbol,
            last_error,
        )
        .map(Option::unwrap_or_default)
    }

    pub async fn pump_upstream_until<S>(
        &self,
        source: &mut S,
        mut shutdown: oneshot::Receiver<()>,
    ) -> RelayResult<usize>
    where
        S: UpstreamTickSource + Send,
    {
        let mut sent = 0_usize;
        loop {
            let update = tokio::select! {
                biased;
                _ = &mut shutdown => return Ok(sent),
                update = source.next_update() => update,
            };
            let progress = source.take_progress();
            let invalid_rows = source.take_invalid_tick_rows();
            let invalid_rows_by_symbol = source.take_invalid_tick_rows_by_symbol();
            let last_error = source.take_last_invalid_tick_row_error();
            let Some(dispatched) = self.process_upstream_update(
                update,
                progress,
                invalid_rows,
                invalid_rows_by_symbol,
                last_error,
            )?
            else {
                return Ok(sent);
            };
            sent = sent.saturating_add(dispatched);
        }
    }

    pub fn process_upstream_update(
        &self,
        update: Option<UpstreamSourceUpdate>,
        progress: UpstreamSourceProgress,
        invalid_rows: u64,
        invalid_rows_by_symbol: std::collections::BTreeMap<String, u64>,
        last_error: Option<String>,
    ) -> RelayResult<Option<usize>> {
        let frames = {
            let mut engine = self
                .engine
                .lock()
                .map_err(|_| RelayError::Internal("relay engine lock poisoned".to_string()))?;
            engine.record_upstream_progress(progress);
            engine.record_upstream_invalid_tick_rows_by_symbol(
                invalid_rows,
                invalid_rows_by_symbol,
                last_error,
            );
            let Some(update) = update else {
                return Ok(None);
            };
            ingest_upstream_update(&mut engine, update)?
        };
        self.dispatch_frames(frames).map(Some)
    }

    pub async fn handle_text(
        &self,
        raw_client_id: u64,
        text: String,
    ) -> RelayResult<Vec<DownstreamFrame>> {
        let command = parse_downstream_command(&text)?;
        self.handle_command(raw_client_id, command).await
    }

    async fn handle_command(
        &self,
        raw_client_id: u64,
        command: DownstreamCommand,
    ) -> RelayResult<Vec<DownstreamFrame>> {
        let should_reconcile_upstream = !matches!(&command, DownstreamCommand::PeekMessage);
        let (frames, upstream_symbols) = {
            let mut engine = self
                .engine
                .lock()
                .map_err(|_| RelayError::Internal("relay engine lock poisoned".to_string()))?;
            let frames = engine.handle_command(ClientId::new(raw_client_id), command)?;
            let upstream_symbols = engine.drain_pending_upstream_subscription_symbols();
            (frames, upstream_symbols)
        };
        if should_reconcile_upstream || !upstream_symbols.is_empty() {
            self.enqueue_upstream_subscription_symbols(upstream_symbols)?;
        }
        Ok(frames)
    }

    pub async fn serve_once(&self, listener: TcpListener) -> RelayResult<()> {
        let (stream, _) = listener
            .accept()
            .await
            .map_err(|err| RelayError::Transport(format!("websocket accept failed: {err}")))?;
        if !self.try_acquire_connection() {
            return Err(RelayError::Transport(
                "relay downstream connection limit reached".to_string(),
            ));
        }
        let client_id = ClientId::new(1);
        let result = self.serve_stream(client_id, stream).await;
        let cleanup = self.remove_client(client_id);
        self.release_connection();
        result?;
        cleanup
    }

    pub async fn serve_until(
        &self,
        listener: TcpListener,
        mut shutdown: oneshot::Receiver<()>,
    ) -> RelayResult<()> {
        let mut next_client_id = 1_u64;
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => return Ok(()),
                accepted = listener.accept() => {
                    let (stream, _) = accepted.map_err(|err| {
                        RelayError::Transport(format!("websocket accept failed: {err}"))
                    })?;
                    if !self.try_acquire_connection() {
                        drop(stream);
                        continue;
                    }
                    let client_id = ClientId::new(next_client_id);
                    next_client_id = next_client_id.saturating_add(1);
                    let server = self.clone();
                    tokio::spawn(async move {
                        if let Err(err) = server.serve_stream(client_id, stream).await {
                            eprintln!("{err}");
                        }
                        if let Err(err) = server.remove_client(client_id) {
                            eprintln!("{err}");
                        }
                        server.release_connection();
                    });
                }
            }
        }
    }

    async fn serve_stream(&self, client_id: ClientId, mut stream: TcpStream) -> RelayResult<()> {
        let initial_read_buffer = accept_handshake(&mut stream, self.limits).await?;
        let outbound = self.register_client(client_id)?;
        let (read_half, mut write_half) = stream.into_split();
        let (incoming_tx, mut incoming_rx) = mpsc::channel(32);
        let limits = self.limits;
        let reader_task = tokio::spawn(async move {
            run_client_reader(read_half, initial_read_buffer, limits, incoming_tx).await;
        });

        let result = async {
            let mut peek_credit = false;
            loop {
                tokio::select! {
                    incoming = incoming_rx.recv() => {
                        let Some(read) = incoming else {
                            return Ok(());
                        };
                        match read {
                            Ok(ClientWebSocketFrame::Text(text)) => {
                                let command = parse_downstream_command(&text)?;
                                if matches!(command, DownstreamCommand::PeekMessage) {
                                    peek_credit = true;
                                }
                                let frames = self.handle_command(client_id.value(), command).await?;
                                self.dispatch_frames(frames)?;
                            }
                            Ok(ClientWebSocketFrame::Ping(payload)) => {
                                write_server_control_frame(&mut write_half, 0x0a, &payload, limits).await?;
                            }
                            Ok(ClientWebSocketFrame::Pong) => {}
                Ok(ClientWebSocketFrame::Close) => {
                    write_server_control_frame(&mut write_half, 0x08, &[], limits).await?;
                    return Ok(());
                }
                            Err(RelayError::Transport(message)) if message.contains("early eof") => return Ok(()),
                            Err(err) => return Err(err),
                        }
                    }
                    payload = outbound.recv(), if peek_credit => {
                        let Some(payload) = payload else {
                            return Ok(());
                        };
                    write_server_encoded_frame(&mut write_half, payload.as_ref(), limits).await?;
                        peek_credit = false;
                    }
                }
            }
        }
        .await;
        reader_task.abort();
        let _ = reader_task.await;
        result
    }

    fn try_acquire_connection(&self) -> bool {
        self.active_connections
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active < self.limits.max_connections).then_some(active.saturating_add(1))
            })
            .is_ok()
    }

    fn release_connection(&self) {
        self.active_connections.fetch_sub(1, Ordering::AcqRel);
    }

    fn remove_client(&self, client_id: ClientId) -> RelayResult<()> {
        let mut engine = self
            .engine
            .lock()
            .map_err(|_| RelayError::Internal("relay engine lock poisoned".to_string()))?;
        engine.remove_client(client_id);
        drop(engine);
        let mailbox = self
            .outbound
            .lock()
            .map_err(|_| RelayError::Internal("relay outbound lock poisoned".to_string()))?
            .remove(&client_id);
        if let Some(mailbox) = mailbox {
            mailbox.close();
        }
        self.enqueue_upstream_subscription_symbols(Vec::new())?;
        Ok(())
    }

    fn register_client(&self, client_id: ClientId) -> RelayResult<Arc<OutboundMailbox>> {
        let mailbox = Arc::new(OutboundMailbox::new(
            self.outbound_channel_capacity,
            self.outbound_byte_capacity,
        ));
        self.outbound
            .lock()
            .map_err(|_| RelayError::Internal("relay outbound lock poisoned".to_string()))?
            .insert(client_id, Arc::clone(&mailbox));
        Ok(mailbox)
    }

    fn enqueue_upstream_subscription_symbols(&self, symbols: Vec<String>) -> RelayResult<()> {
        self.pending_upstream_subscription_symbols
            .lock()
            .map_err(|_| {
                RelayError::Internal("relay upstream subscription pending set poisoned".to_string())
            })?
            .extend(symbols);
        match self.upstream_subscription_tx.try_send(()) {
            Ok(()) | Err(mpsc::error::TrySendError::Full(())) => Ok(()),
            Err(mpsc::error::TrySendError::Closed(())) => Err(RelayError::Internal(
                "relay upstream subscription queue closed".to_string(),
            )),
        }
    }
}

fn ingest_upstream_update(
    engine: &mut RelayEngine,
    update: UpstreamSourceUpdate,
) -> RelayResult<Vec<DownstreamFrame>> {
    match update {
        UpstreamSourceUpdate::Event(event) => ingest_upstream_event(engine, event),
        UpstreamSourceUpdate::Progress => Ok(Vec::new()),
    }
}

fn ingest_upstream_event(
    engine: &mut RelayEngine,
    event: UpstreamMarketEvent,
) -> RelayResult<Vec<DownstreamFrame>> {
    match event {
        UpstreamMarketEvent::Tick(tick) => engine.ingest_tick(tick.symbol, tick.row),
        UpstreamMarketEvent::Quote(quote) => engine.ingest_quote(quote.symbol, quote.quote),
        UpstreamMarketEvent::TradingStatus(status) => {
            let status = *status;
            engine.ingest_trading_status(status.symbol, status.trading_status)
        }
    }
}

fn parse_downstream_command(text: &str) -> RelayResult<DownstreamCommand> {
    let value: Value = serde_json::from_str(text)
        .map_err(|err| RelayError::invalid_protocol(format!("invalid JSON frame: {err}")))?;
    DownstreamCommand::from_value(value)
}

fn quote_symbol_from_payload(payload: &Value) -> Option<&str> {
    let data = payload.get("data")?.as_array()?;
    let [data] = data.as_slice() else {
        return None;
    };
    let quotes = data.get("quotes")?.as_object()?;
    if quotes.len() != 1 {
        return None;
    }
    quotes.keys().next().map(String::as_str)
}

async fn accept_handshake(
    stream: &mut TcpStream,
    limits: RelayServerLimits,
) -> RelayResult<BytesMut> {
    timeout(
        limits.handshake_timeout,
        accept_handshake_inner(stream, limits),
    )
    .await
    .map_err(|_| RelayError::Transport("websocket handshake timed out".to_string()))?
}

async fn accept_handshake_inner(
    stream: &mut TcpStream,
    limits: RelayServerLimits,
) -> RelayResult<BytesMut> {
    let mut buffer = BytesMut::with_capacity(1024);
    let mut chunk = [0_u8; 1024];
    let header_end = loop {
        if let Some(index) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
        if buffer.len() >= limits.max_header_bytes {
            return Err(RelayError::invalid_protocol(
                "websocket handshake header exceeds limit",
            ));
        }
        let remaining = limits.max_header_bytes.saturating_sub(buffer.len());
        let read_len = remaining.min(chunk.len());
        let read = stream.read(&mut chunk[..read_len]).await.map_err(|err| {
            RelayError::Transport(format!("websocket handshake read failed: {err}"))
        })?;
        if read == 0 {
            return Err(RelayError::invalid_protocol(
                "websocket handshake ended early",
            ));
        }
        buffer.extend_from_slice(&chunk[..read]);
    };

    let request = std::str::from_utf8(&buffer[..header_end]).map_err(|err| {
        RelayError::invalid_protocol(format!("invalid websocket handshake: {err}"))
    })?;
    let key = validate_handshake(request)?;
    let accept = websocket_accept_key(key);
    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\n\
Upgrade: websocket\r\n\
Connection: Upgrade\r\n\
Sec-WebSocket-Accept: {accept}\r\n\
\r\n"
    );
    write_all_with_timeout(
        stream,
        response.as_bytes(),
        limits.write_timeout,
        "websocket handshake write",
    )
    .await?;
    Ok(buffer.split_off(header_end))
}

fn validate_handshake(request: &str) -> RelayResult<&str> {
    let mut lines = request.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| RelayError::invalid_protocol("missing websocket request line"))?;
    if !request_line.starts_with("GET ") {
        return Err(RelayError::invalid_protocol(
            "websocket upgrade must use GET",
        ));
    }

    let mut key = None;
    let mut upgrade = false;
    let mut connection = false;
    let mut version = false;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim();
        if name.eq_ignore_ascii_case("sec-websocket-key") {
            key = Some(value);
        } else if name.eq_ignore_ascii_case("upgrade") {
            upgrade = value.eq_ignore_ascii_case("websocket");
        } else if name.eq_ignore_ascii_case("connection") {
            connection = value
                .split(',')
                .any(|token| token.trim().eq_ignore_ascii_case("upgrade"));
        } else if name.eq_ignore_ascii_case("sec-websocket-version") {
            version = value == "13";
        }
    }
    if !upgrade || !connection || !version {
        return Err(RelayError::invalid_protocol(
            "invalid websocket upgrade headers",
        ));
    }
    key.filter(|key| !key.is_empty())
        .ok_or_else(|| RelayError::invalid_protocol("missing sec-websocket-key"))
}

struct ClientWebSocketReader {
    decoder: WebSocketDecoder,
    buffer: BytesMut,
    fragmented_text: Option<Vec<u8>>,
    max_frame_bytes: usize,
    max_message_bytes: usize,
}

impl ClientWebSocketReader {
    fn new(initial_buffer: BytesMut, limits: RelayServerLimits) -> Self {
        Self {
            decoder: WebSocketDecoder::new(Role::Server, limits.max_frame_bytes),
            buffer: initial_buffer,
            fragmented_text: None,
            max_frame_bytes: limits.max_frame_bytes,
            max_message_bytes: limits.max_message_bytes,
        }
    }

    async fn next_frame<R>(&mut self, stream: &mut R) -> RelayResult<ClientWebSocketFrame>
    where
        R: AsyncRead + Unpin,
    {
        loop {
            if let Some(frame) =
                TokioDecoder::decode(&mut self.decoder, &mut self.buffer).map_err(|err| {
                    RelayError::invalid_protocol(format!("invalid websocket frame: {err}"))
                })?
            {
                if let Some(frame) = self.accept_frame(frame)? {
                    return Ok(frame);
                }
                continue;
            }
            if self.buffer.len() > self.max_frame_bytes.saturating_add(14) {
                return Err(RelayError::invalid_protocol(
                    "websocket frame buffer exceeds configured limit",
                ));
            }
            let mut chunk = [0_u8; 8192];
            let read = stream.read(&mut chunk).await.map_err(|err| {
                RelayError::Transport(format!("websocket frame read failed: {err}"))
            })?;
            if read == 0 {
                return Err(RelayError::Transport(
                    "websocket frame early eof".to_string(),
                ));
            }
            self.buffer.extend_from_slice(&chunk[..read]);
        }
    }

    fn accept_frame(&mut self, frame: Frame) -> RelayResult<Option<ClientWebSocketFrame>> {
        let (opcode, fin, payload) = frame.into_parts();
        match opcode {
            OpCode::Text => {
                if self.fragmented_text.is_some() {
                    return Err(RelayError::invalid_protocol(
                        "nested websocket text fragment",
                    ));
                }
                if fin {
                    return checked_message_buffer(payload.as_ref(), self.max_message_bytes)
                        .and_then(|message| text_frame(&message))
                        .map(Some);
                }
                self.fragmented_text = Some(checked_message_buffer(
                    payload.as_ref(),
                    self.max_message_bytes,
                )?);
                Ok(None)
            }
            OpCode::Continuation => {
                let Some(message) = self.fragmented_text.as_mut() else {
                    return Err(RelayError::invalid_protocol(
                        "websocket continuation without a text fragment",
                    ));
                };
                extend_message_buffer(message, payload.as_ref(), self.max_message_bytes)?;
                if !fin {
                    return Ok(None);
                }
                let message = self
                    .fragmented_text
                    .take()
                    .expect("fragment state checked above");
                text_frame(&message).map(Some)
            }
            OpCode::Binary => Err(RelayError::invalid_protocol(
                "relay only accepts websocket text messages",
            )),
            OpCode::Close => Ok(Some(ClientWebSocketFrame::Close)),
            OpCode::Ping => Ok(Some(ClientWebSocketFrame::Ping(payload.to_vec()))),
            OpCode::Pong => Ok(Some(ClientWebSocketFrame::Pong)),
        }
    }
}

async fn run_client_reader<R>(
    mut stream: R,
    initial_buffer: BytesMut,
    limits: RelayServerLimits,
    sender: mpsc::Sender<RelayResult<ClientWebSocketFrame>>,
) where
    R: AsyncRead + Unpin,
{
    let mut reader = ClientWebSocketReader::new(initial_buffer, limits);
    loop {
        let frame = reader.next_frame(&mut stream).await;
        let terminal = matches!(&frame, Ok(ClientWebSocketFrame::Close) | Err(_));
        if sender.send(frame).await.is_err() || terminal {
            return;
        }
    }
}

fn checked_message_buffer(payload: &[u8], max_message_bytes: usize) -> RelayResult<Vec<u8>> {
    if payload.len() > max_message_bytes {
        return Err(RelayError::invalid_protocol(
            "websocket message exceeds configured limit",
        ));
    }
    Ok(payload.to_vec())
}

fn extend_message_buffer(
    message: &mut Vec<u8>,
    payload: &[u8],
    max_message_bytes: usize,
) -> RelayResult<()> {
    if message.len().saturating_add(payload.len()) > max_message_bytes {
        return Err(RelayError::invalid_protocol(
            "websocket message exceeds configured limit",
        ));
    }
    message.extend_from_slice(payload);
    Ok(())
}

fn text_frame(payload: &[u8]) -> RelayResult<ClientWebSocketFrame> {
    String::from_utf8(payload.to_vec())
        .map(ClientWebSocketFrame::Text)
        .map_err(|err| {
            RelayError::invalid_protocol(format!("invalid websocket text payload: {err}"))
        })
}

fn encode_server_text_frame(
    value: &Value,
    limits: RelayServerLimits,
) -> RelayResult<SharedWebSocketFrame> {
    let bytes = serde_json::to_vec(value).map_err(|err| {
        RelayError::Internal(format!("relay websocket JSON encode failed: {err}"))
    })?;
    if bytes.len() > limits.max_message_bytes {
        return Err(RelayError::invalid_protocol(
            "outbound websocket message exceeds configured limit",
        ));
    }
    let mut frame = Vec::with_capacity(bytes.len() + 10);
    frame.push(0x81);
    match bytes.len() {
        len @ 0..=125 => frame.push(len as u8),
        len @ 126..=65535 => {
            frame.push(126);
            frame.extend_from_slice(&(len as u16).to_be_bytes());
        }
        len => {
            frame.push(127);
            frame.extend_from_slice(&(len as u64).to_be_bytes());
        }
    }
    frame.extend_from_slice(&bytes);
    Ok(Arc::from(frame))
}

async fn write_server_encoded_frame<W>(
    stream: &mut W,
    frame: &[u8],
    limits: RelayServerLimits,
) -> RelayResult<()>
where
    W: AsyncWrite + Unpin,
{
    write_all_with_timeout(stream, frame, limits.write_timeout, "websocket frame write").await
}

async fn write_server_control_frame<W>(
    stream: &mut W,
    opcode: u8,
    payload: &[u8],
    limits: RelayServerLimits,
) -> RelayResult<()>
where
    W: AsyncWrite + Unpin,
{
    if payload.len() > 125 {
        return Err(RelayError::invalid_protocol(
            "websocket control frame payload exceeds 125 bytes",
        ));
    }
    let mut frame = Vec::with_capacity(payload.len() + 2);
    frame.push(0x80 | opcode);
    frame.push(payload.len() as u8);
    frame.extend_from_slice(payload);
    write_all_with_timeout(
        stream,
        &frame,
        limits.write_timeout,
        "websocket control write",
    )
    .await
}

async fn write_all_with_timeout<W>(
    stream: &mut W,
    bytes: &[u8],
    write_timeout: Duration,
    operation: &str,
) -> RelayResult<()>
where
    W: AsyncWrite + Unpin,
{
    timeout(write_timeout, stream.write_all(bytes))
        .await
        .map_err(|_| RelayError::Transport(format!("{operation} timed out")))?
        .map_err(|err| RelayError::Transport(format!("{operation} failed: {err}")))
}

fn websocket_accept_key(client_key: &str) -> String {
    use sha1::{Digest, Sha1};

    let mut hasher = Sha1::new();
    hasher.update(client_key.as_bytes());
    hasher.update(WS_ACCEPT_GUID.as_bytes());
    base64_standard(&hasher.finalize())
}

fn base64_standard(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        encoded.push(ALPHABET[(b0 >> 2) as usize] as char);
        encoded.push(ALPHABET[(((b0 & 0b0000_0011) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            encoded.push(ALPHABET[(((b1 & 0b0000_1111) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            encoded.push('=');
        }
        if chunk.len() > 2 {
            encoded.push(ALPHABET[(b2 & 0b0011_1111) as usize] as char);
        } else {
            encoded.push('=');
        }
    }
    encoded
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use serde_json::json;
    use tokio::io::AsyncWriteExt;

    use super::*;

    fn masked_client_frame(fin: bool, opcode: u8, payload: &[u8]) -> Vec<u8> {
        assert!(
            payload.len() <= 125,
            "test helper only encodes short frames"
        );
        let mask = [0x11, 0x22, 0x33, 0x44];
        let mut frame = Vec::with_capacity(payload.len() + 6);
        frame.push(u8::from(fin) << 7 | opcode);
        frame.push(0x80 | payload.len() as u8);
        frame.extend_from_slice(&mask);
        frame.extend(
            payload
                .iter()
                .enumerate()
                .map(|(index, byte)| byte ^ mask[index % mask.len()]),
        );
        frame
    }

    #[test]
    fn full_reliable_outbound_queue_evicts_slow_client() {
        let engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(16, 16)));
        let server = RelayServer::with_outbound_capacity(engine, 1);
        let client_id = ClientId::new(1);
        let _receiver = server.register_client(client_id).unwrap();

        let sent = server
            .dispatch_frames([
                DownstreamFrame::new(client_id, json!({"seq": 1})),
                DownstreamFrame::new(client_id, json!({"seq": 2})),
            ])
            .unwrap();

        assert_eq!(sent, 1);
        assert_eq!(server.outbound.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn quote_outbound_queue_keeps_latest_market_state() {
        let engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(16, 16)));
        let server = RelayServer::with_outbound_capacity(engine, 1);
        let client_id = ClientId::new(1);
        let receiver = server.register_client(client_id).unwrap();

        let sent = server
            .dispatch_frames([
                DownstreamFrame::new(
                    client_id,
                    json!({
                        "aid": "rtn_data",
                        "data": [{"quotes": {"SHFE.au2602": {"last_price": 1}}}],
                    }),
                ),
                DownstreamFrame::new(
                    client_id,
                    json!({
                        "aid": "rtn_data",
                        "data": [{"quotes": {"SHFE.au2602": {"last_price": 2}}}],
                    }),
                ),
            ])
            .unwrap();

        assert_eq!(sent, 2);
        assert_eq!(server.outbound.lock().unwrap().len(), 1);
        let frame = receiver.recv().await.unwrap();
        assert!(String::from_utf8_lossy(&frame).contains("\"last_price\":2"));
    }

    #[test]
    fn outbound_byte_budget_evicts_only_oversized_reliable_client() {
        let engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(16, 16)));
        let server = RelayServer::with_outbound_budget(engine, 4, 1);
        let client_id = ClientId::new(1);
        let _receiver = server.register_client(client_id).unwrap();

        let sent = server
            .dispatch_frames([DownstreamFrame::new(client_id, json!({"seq": 1}))])
            .unwrap();

        assert_eq!(sent, 0);
        assert!(server.outbound.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn dispatch_shares_one_encoded_frame_for_shared_payload() {
        let engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(16, 16)));
        let server = RelayServer::with_outbound_capacity(engine, 2);
        let first_client = ClientId::new(1);
        let second_client = ClientId::new(2);
        let first = server.register_client(first_client).unwrap();
        let second = server.register_client(second_client).unwrap();
        let payload = Arc::new(json!({"aid": "rtn_data", "data": [{"quotes": {}}]}));

        assert_eq!(
            server
                .dispatch_frames([
                    DownstreamFrame::shared(first_client, Arc::clone(&payload)),
                    DownstreamFrame::shared(second_client, payload),
                ])
                .unwrap(),
            2
        );

        let first = first.recv().await.unwrap();
        let second = second.recv().await.unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(first.as_ref(), second.as_ref());
    }

    #[tokio::test]
    async fn upstream_subscription_updates_are_bounded_and_coalesced() {
        let engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(16, 16)));
        let server = RelayServer::new(engine);

        server
            .enqueue_upstream_subscription_symbols(vec![
                "SHFE.au2602".to_string(),
                "DCE.m2609".to_string(),
            ])
            .unwrap();
        server
            .enqueue_upstream_subscription_symbols(vec![
                "DCE.m2609".to_string(),
                "CZCE.SR601".to_string(),
            ])
            .unwrap();

        assert_eq!(
            server.next_upstream_subscription_symbols().await,
            Some(vec![
                "CZCE.SR601".to_string(),
                "DCE.m2609".to_string(),
                "SHFE.au2602".to_string(),
            ])
        );
        assert!(matches!(
            server.upstream_subscription_rx.lock().await.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn connection_limit_rejects_overflow_and_recovers_after_release() {
        let engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(16, 16)));
        let server = RelayServer::with_limits(
            engine,
            RelayServerLimits {
                max_connections: 1,
                ..RelayServerLimits::default()
            },
        );

        assert!(server.try_acquire_connection());
        assert!(!server.try_acquire_connection());
        server.release_connection();
        assert!(server.try_acquire_connection());
        server.release_connection();
    }

    #[tokio::test]
    async fn reader_keeps_partial_frame_state_when_outbound_is_ready() {
        let (mut client, server) = tokio::io::duplex(1024);
        let (sender, mut receiver) = mpsc::channel(4);
        let reader = tokio::spawn(run_client_reader(
            server,
            BytesMut::new(),
            RelayServerLimits::default(),
            sender,
        ));
        let text = r#"{\"aid\":\"peek_message\"}"#;
        let frame = masked_client_frame(true, 0x01, text.as_bytes());

        client.write_all(&frame[..3]).await.unwrap();
        let (outbound_ready, mut outbound) = mpsc::channel::<()>(1);
        outbound_ready.send(()).await.unwrap();
        tokio::select! {
            _ = outbound.recv() => {}
            _ = receiver.recv() => panic!("partial websocket frame completed early"),
        }

        client.write_all(&frame[3..]).await.unwrap();
        match receiver.recv().await.unwrap().unwrap() {
            ClientWebSocketFrame::Text(actual) => assert_eq!(actual, text),
            _ => panic!("expected text frame"),
        }
        drop(client);
        reader.await.unwrap();
    }

    #[tokio::test]
    async fn reader_preserves_fragmented_text_across_control_frames() {
        let (mut client, server) = tokio::io::duplex(1024);
        let (sender, mut receiver) = mpsc::channel(4);
        let reader = tokio::spawn(run_client_reader(
            server,
            BytesMut::new(),
            RelayServerLimits::default(),
            sender,
        ));

        client
            .write_all(&masked_client_frame(false, 0x01, b"hel"))
            .await
            .unwrap();
        client
            .write_all(&masked_client_frame(true, 0x09, b"p"))
            .await
            .unwrap();
        client
            .write_all(&masked_client_frame(true, 0x00, b"lo"))
            .await
            .unwrap();

        match receiver.recv().await.unwrap().unwrap() {
            ClientWebSocketFrame::Ping(payload) => assert_eq!(payload, b"p"),
            _ => panic!("expected ping frame"),
        }
        match receiver.recv().await.unwrap().unwrap() {
            ClientWebSocketFrame::Text(text) => assert_eq!(text, "hello"),
            _ => panic!("expected completed text frame"),
        }
        drop(client);
        reader.await.unwrap();
    }
}

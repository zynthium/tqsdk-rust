#![cfg_attr(not(test), forbid(unsafe_code))]
//! Optional embedded monitoring primitives for `tqsdk-rust`.

use std::collections::VecDeque;
use std::fmt::{Display, Formatter};
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const SCHEMA_VERSION: u16 = 1;
const INCIDENT_LIMIT: usize = 128;
const HTTP_READ_LIMIT: usize = 4096;
const HISTORY_SYMBOL_LIMIT: usize = 32;
const DEFAULT_CACHE_INVENTORY_REFRESH_INTERVAL: Duration = Duration::from_secs(30);
const MIN_CACHE_INVENTORY_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

/// Runtime monitoring mode requested by the caller.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MonitoringMode {
    /// Monitoring is disabled. No HTTP task should be started.
    #[default]
    Off,
    /// Lightweight runtime counters and bounded recent events.
    Light,
    /// Full monitoring. Heavy work must still run in background workers.
    Full,
}

/// Configuration for the embedded monitor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonitoringConfig {
    mode: MonitoringMode,
    bind_addr: SocketAddr,
    admin_enabled: bool,
    cache_inventory_refresh_interval: Duration,
    cache_inventory: Option<CacheInventoryConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheInventoryConfig {
    cache_dir: PathBuf,
    refresh_interval: Duration,
}

impl MonitoringConfig {
    /// Create a localhost light-mode dashboard configuration.
    #[must_use]
    pub fn localhost(port: u16) -> Self {
        Self {
            mode: MonitoringMode::Light,
            bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
            admin_enabled: false,
            cache_inventory_refresh_interval: DEFAULT_CACHE_INVENTORY_REFRESH_INTERVAL,
            cache_inventory: None,
        }
    }

    /// Create a disabled monitoring configuration.
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            mode: MonitoringMode::Off,
            bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            admin_enabled: false,
            cache_inventory_refresh_interval: DEFAULT_CACHE_INVENTORY_REFRESH_INTERVAL,
            cache_inventory: None,
        }
    }

    /// Switch the dashboard to full mode.
    #[must_use]
    pub fn full(mut self) -> Self {
        self.mode = MonitoringMode::Full;
        self
    }

    /// Enable admin-only management endpoints for future cache operations.
    ///
    /// The current implementation remains read-only; this flag is surfaced in
    /// snapshots so callers can verify the process is not accidentally writable.
    #[must_use]
    pub fn with_admin_enabled(mut self, enabled: bool) -> Self {
        self.admin_enabled = enabled;
        self
    }

    /// Enable low-frequency persistent tick cache inventory scanning.
    ///
    /// The scan runs in a background blocking task and never from the hot
    /// market/backtest update path.
    #[must_use]
    pub fn with_cache_inventory(mut self, cache_dir: impl Into<PathBuf>) -> Self {
        self.cache_inventory = Some(CacheInventoryConfig {
            cache_dir: cache_dir.into(),
            refresh_interval: self.cache_inventory_refresh_interval,
        });
        self
    }

    /// Set the cache inventory refresh interval.
    #[must_use]
    pub fn with_cache_inventory_refresh_interval(mut self, interval: Duration) -> Self {
        let interval = interval.max(MIN_CACHE_INVENTORY_REFRESH_INTERVAL);
        self.cache_inventory_refresh_interval = interval;
        if let Some(cache_inventory) = &mut self.cache_inventory {
            cache_inventory.refresh_interval = interval;
        }
        self
    }

    #[must_use]
    pub fn mode(&self) -> MonitoringMode {
        self.mode
    }

    #[must_use]
    pub fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }

    #[must_use]
    pub fn admin_enabled(&self) -> bool {
        self.admin_enabled
    }

    #[must_use]
    pub fn cache_inventory_config(&self) -> Option<&CacheInventoryConfig> {
        self.cache_inventory.as_ref()
    }

    #[must_use]
    pub fn is_enabled(&self) -> bool {
        !matches!(self.mode, MonitoringMode::Off)
    }
}

impl CacheInventoryConfig {
    #[must_use]
    pub fn cache_dir(&self) -> &Path {
        &self.cache_dir
    }

    #[must_use]
    pub fn refresh_interval(&self) -> Duration {
        self.refresh_interval
    }
}

/// Current runtime observed by the monitor.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MonitorRuntimeMode {
    #[default]
    Idle,
    Live,
    Backtest,
    Replay,
    CacheOnly,
}

/// Minimal sink used by hot paths.
///
/// Implementations must not block, await, allocate unbounded memory, or perform
/// JSON/file-system work from these methods.
pub trait MonitorSink: Send + Sync + 'static {
    fn observe_wait_step(&self, _elapsed_ns: u64) {}
    fn observe_tick_batch(&self, _stats: TickBatchStats) {}
    fn observe_cache_write(&self, _stats: CacheWriteStats) {}
    fn observe_backtest_step(&self, _stats: BacktestStepStats) {}
    fn observe_order_event(&self, _event: OrderMonitorEvent) {}
    fn observe_incident(&self, _incident: MonitorIncident) {}
}

/// No-op monitor sink used when monitoring is disabled.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopMonitorSink;

impl MonitorSink for NoopMonitorSink {}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct TickBatchStats {
    pub symbol_count: usize,
    pub tick_count: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct CacheWriteStats {
    pub rows: usize,
    pub elapsed_ns: u64,
    pub gap_detected: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct BacktestStepStats {
    pub rows: usize,
    pub elapsed_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OrderMonitorEvent {
    pub account_id: String,
    pub order_id: String,
    pub symbol: String,
    pub state: String,
    pub elapsed_ns: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MonitorSeverity {
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MonitorIncident {
    pub at_unix_millis: u64,
    pub severity: MonitorSeverity,
    pub message: String,
}

impl MonitorIncident {
    #[must_use]
    pub fn new(severity: MonitorSeverity, message: impl Into<String>) -> Self {
        Self {
            at_unix_millis: now_unix_millis(),
            severity,
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MonitorSnapshot {
    pub schema_version: u16,
    pub received_at_unix_millis: u64,
    pub process: ProcessPanel,
    pub latency: LatencyPanel,
    pub market: MarketPanel,
    pub cache: CachePanel,
    pub orders: OrderPanel,
    pub history: HistoryPanel,
    pub incidents: Vec<MonitorIncident>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProcessPanel {
    pub pid: u32,
    pub mode: MonitorRuntimeMode,
    pub started_at_unix_millis: u64,
    pub snapshot_seq: u64,
    pub monitoring_mode: String,
    pub admin_enabled: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct LatencyPanel {
    pub wait_steps: u64,
    pub wait_step_avg_ns: u64,
    pub wait_step_max_ns: u64,
    pub cache_writes: u64,
    pub cache_write_avg_ns: u64,
    pub cache_write_max_ns: u64,
    pub backtest_steps: u64,
    pub backtest_step_avg_ns: u64,
    pub backtest_step_max_ns: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct MarketPanel {
    pub tick_batches: u64,
    pub symbols_observed: u64,
    pub ticks_observed: u64,
    pub last_tick_batch: Option<TickBatchStats>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct CachePanel {
    pub writes: u64,
    pub rows_written: u64,
    pub gaps_detected: u64,
    pub last_write: Option<CacheWriteStats>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct OrderPanel {
    pub events: u64,
    pub last_event: Option<OrderMonitorEvent>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct HistoryPanel {
    pub cache_dir: Option<String>,
    pub inventory_symbols: u64,
    pub inventory_days: u64,
    pub inventory_files: u64,
    pub inventory_rows: u64,
    pub inventory_bytes: u64,
    pub problem_files: u64,
    pub missing_ranges: u64,
    pub last_refresh_unix_millis: Option<u64>,
    pub last_error: Option<String>,
    pub top_symbols: Vec<HistorySymbolPanel>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct HistorySymbolPanel {
    pub symbol: String,
    pub files: u64,
    pub rows: u64,
    pub bytes: u64,
    pub days: u64,
    pub problem_files: u64,
}

/// In-memory monitor registry optimized for cheap writes and snapshot reads.
#[derive(Debug)]
pub struct MonitorRegistry {
    started_at_unix_millis: u64,
    config: MonitoringConfig,
    mode: Mutex<MonitorRuntimeMode>,
    snapshot_seq: AtomicU64,
    wait_step_latency: LatencyCounter,
    cache_write_latency: LatencyCounter,
    backtest_step_latency: LatencyCounter,
    tick_batches: AtomicU64,
    symbols_observed: AtomicU64,
    ticks_observed: AtomicU64,
    cache_rows_written: AtomicU64,
    cache_gaps_detected: AtomicU64,
    order_events: AtomicU64,
    last_tick_batch: Mutex<Option<TickBatchStats>>,
    last_cache_write: Mutex<Option<CacheWriteStats>>,
    last_order_event: Mutex<Option<OrderMonitorEvent>>,
    history: Mutex<HistoryPanel>,
    incidents: Mutex<VecDeque<MonitorIncident>>,
}

impl MonitorRegistry {
    #[must_use]
    pub fn new(mode: MonitorRuntimeMode) -> Self {
        Self::with_config(mode, MonitoringConfig::disabled())
    }

    #[must_use]
    pub fn with_config(mode: MonitorRuntimeMode, config: MonitoringConfig) -> Self {
        Self {
            started_at_unix_millis: now_unix_millis(),
            config,
            mode: Mutex::new(mode),
            snapshot_seq: AtomicU64::new(0),
            wait_step_latency: LatencyCounter::default(),
            cache_write_latency: LatencyCounter::default(),
            backtest_step_latency: LatencyCounter::default(),
            tick_batches: AtomicU64::new(0),
            symbols_observed: AtomicU64::new(0),
            ticks_observed: AtomicU64::new(0),
            cache_rows_written: AtomicU64::new(0),
            cache_gaps_detected: AtomicU64::new(0),
            order_events: AtomicU64::new(0),
            last_tick_batch: Mutex::new(None),
            last_cache_write: Mutex::new(None),
            last_order_event: Mutex::new(None),
            history: Mutex::new(HistoryPanel::default()),
            incidents: Mutex::new(VecDeque::with_capacity(INCIDENT_LIMIT)),
        }
    }

    pub fn set_mode(&self, mode: MonitorRuntimeMode) {
        if let Ok(mut current) = self.mode.lock() {
            *current = mode;
        }
    }

    pub fn set_history_panel(&self, history: HistoryPanel) {
        if let Ok(mut current) = self.history.lock() {
            *current = history;
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> MonitorSnapshot {
        let wait = self.wait_step_latency.snapshot();
        let cache_write = self.cache_write_latency.snapshot();
        let backtest = self.backtest_step_latency.snapshot();
        let received_at_unix_millis = now_unix_millis();
        let snapshot_seq = self
            .snapshot_seq
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        MonitorSnapshot {
            schema_version: SCHEMA_VERSION,
            received_at_unix_millis,
            process: ProcessPanel {
                pid: std::process::id(),
                mode: self
                    .mode
                    .lock()
                    .map_or(MonitorRuntimeMode::Idle, |mode| *mode),
                started_at_unix_millis: self.started_at_unix_millis,
                snapshot_seq,
                monitoring_mode: format!("{:?}", self.config.mode()).to_ascii_lowercase(),
                admin_enabled: self.config.admin_enabled(),
            },
            latency: LatencyPanel {
                wait_steps: wait.count,
                wait_step_avg_ns: wait.average_ns(),
                wait_step_max_ns: wait.max_ns,
                cache_writes: cache_write.count,
                cache_write_avg_ns: cache_write.average_ns(),
                cache_write_max_ns: cache_write.max_ns,
                backtest_steps: backtest.count,
                backtest_step_avg_ns: backtest.average_ns(),
                backtest_step_max_ns: backtest.max_ns,
            },
            market: MarketPanel {
                tick_batches: self.tick_batches.load(Ordering::Relaxed),
                symbols_observed: self.symbols_observed.load(Ordering::Relaxed),
                ticks_observed: self.ticks_observed.load(Ordering::Relaxed),
                last_tick_batch: self.last_tick_batch.lock().map_or(None, |last| *last),
            },
            cache: CachePanel {
                writes: cache_write.count,
                rows_written: self.cache_rows_written.load(Ordering::Relaxed),
                gaps_detected: self.cache_gaps_detected.load(Ordering::Relaxed),
                last_write: self.last_cache_write.lock().map_or(None, |last| *last),
            },
            orders: OrderPanel {
                events: self.order_events.load(Ordering::Relaxed),
                last_event: self
                    .last_order_event
                    .lock()
                    .map_or(None, |last| last.clone()),
            },
            history: self
                .history
                .lock()
                .map_or_else(|_| HistoryPanel::default(), |history| history.clone()),
            incidents: self.incidents.lock().map_or_else(
                |_| Vec::new(),
                |incidents| incidents.iter().cloned().collect(),
            ),
        }
    }
}

impl MonitorSink for MonitorRegistry {
    fn observe_wait_step(&self, elapsed_ns: u64) {
        self.wait_step_latency.record(elapsed_ns);
    }

    fn observe_tick_batch(&self, stats: TickBatchStats) {
        self.tick_batches.fetch_add(1, Ordering::Relaxed);
        self.symbols_observed.fetch_add(
            saturating_usize_to_u64(stats.symbol_count),
            Ordering::Relaxed,
        );
        self.ticks_observed
            .fetch_add(saturating_usize_to_u64(stats.tick_count), Ordering::Relaxed);
        if let Ok(mut last) = self.last_tick_batch.lock() {
            *last = Some(stats);
        }
    }

    fn observe_cache_write(&self, stats: CacheWriteStats) {
        self.cache_write_latency.record(stats.elapsed_ns);
        self.cache_rows_written
            .fetch_add(saturating_usize_to_u64(stats.rows), Ordering::Relaxed);
        if stats.gap_detected {
            self.cache_gaps_detected.fetch_add(1, Ordering::Relaxed);
        }
        if let Ok(mut last) = self.last_cache_write.lock() {
            *last = Some(stats);
        }
    }

    fn observe_backtest_step(&self, stats: BacktestStepStats) {
        self.backtest_step_latency.record(stats.elapsed_ns);
    }

    fn observe_order_event(&self, event: OrderMonitorEvent) {
        self.order_events.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut last) = self.last_order_event.lock() {
            *last = Some(event);
        }
    }

    fn observe_incident(&self, incident: MonitorIncident) {
        if let Ok(mut incidents) = self.incidents.lock() {
            if incidents.len() == INCIDENT_LIMIT {
                incidents.pop_front();
            }
            incidents.push_back(incident);
        }
    }
}

/// Cheap clonable handle for instrumented code paths.
#[derive(Debug, Clone)]
pub struct MonitorHandle {
    registry: Arc<MonitorRegistry>,
}

impl MonitorHandle {
    #[must_use]
    pub fn new(registry: Arc<MonitorRegistry>) -> Self {
        Self { registry }
    }

    #[must_use]
    pub fn registry(&self) -> &Arc<MonitorRegistry> {
        &self.registry
    }

    #[must_use]
    pub fn snapshot(&self) -> MonitorSnapshot {
        self.registry.snapshot()
    }
}

impl MonitorSink for MonitorHandle {
    fn observe_wait_step(&self, elapsed_ns: u64) {
        self.registry.observe_wait_step(elapsed_ns);
    }

    fn observe_tick_batch(&self, stats: TickBatchStats) {
        self.registry.observe_tick_batch(stats);
    }

    fn observe_cache_write(&self, stats: CacheWriteStats) {
        self.registry.observe_cache_write(stats);
    }

    fn observe_backtest_step(&self, stats: BacktestStepStats) {
        self.registry.observe_backtest_step(stats);
    }

    fn observe_order_event(&self, event: OrderMonitorEvent) {
        self.registry.observe_order_event(event);
    }

    fn observe_incident(&self, incident: MonitorIncident) {
        self.registry.observe_incident(incident);
    }
}

/// Running embedded HTTP dashboard.
#[derive(Debug)]
pub struct EmbeddedMonitor {
    bound_addr: SocketAddr,
    registry: Arc<MonitorRegistry>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl EmbeddedMonitor {
    pub async fn start(
        config: MonitoringConfig,
        registry: Arc<MonitorRegistry>,
    ) -> Result<Self, MonitorError> {
        if !config.is_enabled() {
            return Err(MonitorError::Disabled);
        }
        let cache_inventory = config.cache_inventory_config().cloned();
        let listener = TcpListener::bind(config.bind_addr()).await?;
        let bound_addr = listener.local_addr()?;
        let task_registry = registry.clone();
        let mut tasks = Vec::new();
        tasks.push(tokio::spawn(async move {
            serve(listener, task_registry).await;
        }));
        if let Some(cache_inventory) = cache_inventory {
            let inventory_registry = registry.clone();
            tasks.push(tokio::spawn(async move {
                run_cache_inventory_worker(inventory_registry, cache_inventory).await;
            }));
        }
        Ok(Self {
            bound_addr,
            registry,
            tasks,
        })
    }

    #[must_use]
    pub fn bound_addr(&self) -> SocketAddr {
        self.bound_addr
    }

    #[must_use]
    pub fn registry(&self) -> &Arc<MonitorRegistry> {
        &self.registry
    }
}

impl Drop for EmbeddedMonitor {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

#[derive(Debug)]
pub enum MonitorError {
    Disabled,
    Io(io::Error),
    Json(serde_json::Error),
}

impl Display for MonitorError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Disabled => write!(formatter, "monitoring is disabled"),
            Self::Io(error) => write!(formatter, "{error}"),
            Self::Json(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for MonitorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Disabled => None,
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
        }
    }
}

impl From<io::Error> for MonitorError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for MonitorError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

#[derive(Debug, Default)]
struct LatencyCounter {
    count: AtomicU64,
    total_ns: AtomicU64,
    max_ns: AtomicU64,
}

impl LatencyCounter {
    fn record(&self, elapsed_ns: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.total_ns.fetch_add(elapsed_ns, Ordering::Relaxed);
        atomic_max(&self.max_ns, elapsed_ns);
    }

    fn snapshot(&self) -> LatencySnapshot {
        LatencySnapshot {
            count: self.count.load(Ordering::Relaxed),
            total_ns: self.total_ns.load(Ordering::Relaxed),
            max_ns: self.max_ns.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct LatencySnapshot {
    count: u64,
    total_ns: u64,
    max_ns: u64,
}

impl LatencySnapshot {
    fn average_ns(self) -> u64 {
        self.total_ns.checked_div(self.count).unwrap_or(0)
    }
}

fn atomic_max(slot: &AtomicU64, value: u64) {
    let mut current = slot.load(Ordering::Relaxed);
    while value > current {
        match slot.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(next) => current = next,
        }
    }
}

async fn serve(listener: TcpListener, registry: Arc<MonitorRegistry>) {
    loop {
        let Ok((stream, _peer)) = listener.accept().await else {
            break;
        };
        let stream_registry = registry.clone();
        tokio::spawn(async move {
            let _ = handle_connection(stream, stream_registry).await;
        });
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    registry: Arc<MonitorRegistry>,
) -> Result<(), MonitorError> {
    let mut buf = vec![0; HTTP_READ_LIMIT];
    let read = stream.read(&mut buf).await?;
    let request = String::from_utf8_lossy(&buf[..read]);
    let path = request_path(&request);
    match path {
        "/monitor" | "/monitor/" => write_html(&mut stream).await?,
        "/monitor/api/snapshot" | "/api/snapshot" => {
            let snapshot = registry.snapshot();
            write_json(&mut stream, &snapshot).await?;
        }
        "/healthz" => write_plain(&mut stream, 200, "ok").await?,
        _ => write_plain(&mut stream, 404, "not found").await?,
    }
    Ok(())
}

fn request_path(request: &str) -> &str {
    request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/")
}

async fn write_json<T: Serialize>(stream: &mut TcpStream, value: &T) -> Result<(), MonitorError> {
    let body = serde_json::to_vec(value)?;
    write_response(stream, 200, "application/json; charset=utf-8", &body).await
}

async fn write_html(stream: &mut TcpStream) -> Result<(), MonitorError> {
    write_response(
        stream,
        200,
        "text/html; charset=utf-8",
        MONITOR_HTML.as_bytes(),
    )
    .await
}

async fn write_plain(stream: &mut TcpStream, status: u16, body: &str) -> Result<(), MonitorError> {
    write_response(stream, status, "text/plain; charset=utf-8", body.as_bytes()).await
}

async fn write_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) -> Result<(), MonitorError> {
    let status_text = match status {
        200 => "OK",
        404 => "Not Found",
        _ => "OK",
    };
    let header = format!(
        "HTTP/1.1 {status} {status_text}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\ncache-control: no-store\r\nconnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.shutdown().await?;
    Ok(())
}

fn now_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

fn saturating_usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

async fn run_cache_inventory_worker(
    registry: Arc<MonitorRegistry>,
    cache_inventory: CacheInventoryConfig,
) {
    loop {
        refresh_cache_inventory_once(registry.clone(), cache_inventory.clone()).await;
        tokio::time::sleep(cache_inventory.refresh_interval()).await;
    }
}

async fn refresh_cache_inventory_once(
    registry: Arc<MonitorRegistry>,
    cache_inventory: CacheInventoryConfig,
) {
    let cache_dir = cache_inventory.cache_dir().to_path_buf();
    let panel = match tokio::task::spawn_blocking(move || {
        let cache = tqsdk_data::BacktestTickCache::open(&cache_dir)?;
        cache.inventory()
    })
    .await
    {
        Ok(Ok(inventory)) => history_panel_from_inventory(inventory),
        Ok(Err(error)) => history_panel_from_error(cache_inventory.cache_dir(), error.to_string()),
        Err(error) => history_panel_from_error(cache_inventory.cache_dir(), error.to_string()),
    };
    registry.set_history_panel(panel);
}

fn history_panel_from_inventory(inventory: tqsdk_data::BacktestTickCacheInventory) -> HistoryPanel {
    let tqsdk_data::BacktestTickCacheInventory {
        cache_dir,
        symbols,
        total_files,
        total_rows,
        total_bytes,
        total_days,
        problem_files,
        ..
    } = inventory;
    let inventory_symbols = symbols.len();
    let mut top_symbols = symbols
        .into_iter()
        .map(|symbol| HistorySymbolPanel {
            symbol: symbol.symbol,
            files: saturating_usize_to_u64(symbol.files),
            rows: saturating_usize_to_u64(symbol.rows),
            bytes: symbol.bytes,
            days: saturating_usize_to_u64(symbol.days),
            problem_files: saturating_usize_to_u64(symbol.problem_files),
        })
        .collect::<Vec<_>>();
    top_symbols.sort_by(|left, right| {
        right
            .rows
            .cmp(&left.rows)
            .then_with(|| left.symbol.cmp(&right.symbol))
    });
    top_symbols.truncate(HISTORY_SYMBOL_LIMIT);

    HistoryPanel {
        cache_dir: Some(cache_dir.display().to_string()),
        inventory_symbols: saturating_usize_to_u64(inventory_symbols),
        inventory_days: saturating_usize_to_u64(total_days),
        inventory_files: saturating_usize_to_u64(total_files),
        inventory_rows: saturating_usize_to_u64(total_rows),
        inventory_bytes: total_bytes,
        problem_files: saturating_usize_to_u64(problem_files),
        missing_ranges: 0,
        last_refresh_unix_millis: Some(now_unix_millis()),
        last_error: None,
        top_symbols,
    }
}

fn history_panel_from_error(cache_dir: &Path, error: String) -> HistoryPanel {
    HistoryPanel {
        cache_dir: Some(cache_dir.display().to_string()),
        last_refresh_unix_millis: Some(now_unix_millis()),
        last_error: Some(error),
        ..HistoryPanel::default()
    }
}

const MONITOR_HTML: &str = r##"<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>tqsdk 监控面板</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #101113;
      --panel: #171b1f;
      --panel-strong: #1d2429;
      --panel-soft: #121619;
      --line: #2b3d42;
      --line-strong: #3db7c4;
      --text: #edf5f6;
      --muted: #8fa2a7;
      --live: #40d889;
      --info: #3db7c4;
      --warn: #f0b84a;
      --bad: #f06470;
      --accent: #c98bff;
      --shadow: 0 18px 44px rgb(0 0 0 / 36%), inset 0 1px 0 rgb(255 255 255 / 4%);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-width: 320px;
      font: 13px/1.45 ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background:
        linear-gradient(180deg, rgb(255 255 255 / 2%), transparent 220px),
        linear-gradient(90deg, rgb(61 183 196 / 7%) 1px, transparent 1px),
        linear-gradient(0deg, rgb(61 183 196 / 5%) 1px, transparent 1px),
        var(--bg);
      background-size: auto, 48px 48px, 48px 48px, auto;
      color: var(--text);
    }
    main {
      display: grid;
      gap: 10px;
      width: min(1500px, 100%);
      min-height: 100vh;
      margin: 0 auto;
      padding: 12px;
    }
    button {
      min-width: 58px;
      min-height: 30px;
      border: 1px solid rgb(61 183 196 / 48%);
      border-radius: 7px;
      background: rgb(24 35 38 / 94%);
      color: #c9fbff;
      font: inherit;
      font-weight: 750;
      cursor: pointer;
    }
    button:hover { border-color: var(--info); }
    button:disabled { cursor: not-allowed; opacity: .5; }
    .monitor-header {
      position: relative;
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto minmax(0, 1fr);
      align-items: center;
      gap: 12px;
      min-height: 44px;
    }
    .monitor-header::after {
      content: "";
      position: absolute;
      right: 18%;
      bottom: -1px;
      left: 18%;
      height: 1px;
      background: linear-gradient(90deg, transparent, var(--info), transparent);
      box-shadow: 0 0 12px rgb(61 183 196 / 70%);
    }
    .brand, .controls {
      display: flex;
      align-items: center;
      gap: 10px;
      min-width: 0;
      color: var(--muted);
      white-space: nowrap;
    }
    .controls { justify-content: flex-end; }
    .brand-chip {
      border: 1px solid rgb(64 216 137 / 54%);
      border-radius: 7px;
      padding: 5px 9px;
      background: rgb(64 216 137 / 8%);
      color: #9cf6c4;
      font-weight: 900;
      letter-spacing: 0;
    }
    h1 {
      margin: 0;
      font-size: clamp(20px, 1.7vw, 27px);
      line-height: 1.1;
      text-align: center;
      letter-spacing: 0;
      white-space: nowrap;
    }
    h2 {
      margin: 0;
      font-size: 13px;
      line-height: 1;
      letter-spacing: 0;
    }
    .chip {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      min-height: 30px;
      border: 1px solid rgb(61 183 196 / 36%);
      border-radius: 999px;
      padding: 6px 11px;
      background: rgb(61 183 196 / 8%);
      color: #c9fbff;
      font-weight: 750;
    }
    .chip.live { border-color: rgb(64 216 137 / 45%); background: rgb(64 216 137 / 10%); color: #c8ffdf; }
    .chip.warn { border-color: rgb(240 184 74 / 52%); background: rgb(240 184 74 / 11%); color: #ffe0a0; }
    .chip.bad { border-color: rgb(240 100 112 / 56%); background: rgb(240 100 112 / 12%); color: #ffd1d5; }
    .dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: currentColor;
      box-shadow: 0 0 12px currentColor;
    }
    .panel, .metric-card {
      position: relative;
      overflow: hidden;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: linear-gradient(180deg, var(--panel), var(--panel-soft));
      box-shadow: var(--shadow);
    }
    .panel::after, .metric-card::after {
      content: "";
      position: absolute;
      top: 0;
      right: 12px;
      left: 12px;
      height: 1px;
      background: linear-gradient(90deg, transparent, rgb(61 183 196 / 70%), transparent);
      opacity: .8;
      pointer-events: none;
    }
    .panel-header {
      position: relative;
      z-index: 1;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      min-height: 36px;
      padding: 10px 12px 0;
    }
    .panel-title {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      color: #d8fbff;
      font-weight: 850;
    }
    .panel-title::before {
      content: "";
      width: 3px;
      height: 13px;
      border-radius: 2px;
      background: var(--info);
      box-shadow: 0 0 10px var(--info);
    }
    .hero {
      display: grid;
      grid-template-columns: minmax(260px, .9fr) minmax(0, 2.1fr);
      gap: 12px;
      align-items: stretch;
      padding: 16px;
    }
    .hero-main {
      display: grid;
      align-content: center;
      gap: 6px;
      min-width: 0;
    }
    .eyebrow, .label, .muted {
      color: var(--muted);
      font-size: 11px;
      font-weight: 750;
      text-transform: uppercase;
    }
    .hero-value {
      overflow-wrap: anywhere;
      font-size: clamp(30px, 4vw, 54px);
      line-height: .95;
      font-weight: 900;
      letter-spacing: 0;
    }
    .hero-meta {
      overflow-wrap: anywhere;
      color: var(--muted);
    }
    .hero-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 1px;
      overflow: hidden;
      border: 1px solid rgb(255 255 255 / 5%);
      border-radius: 8px;
      background: rgb(255 255 255 / 5%);
    }
    .hero-cell {
      min-width: 0;
      padding: 13px;
      background: rgb(17 22 25 / 84%);
    }
    .value {
      display: block;
      overflow-wrap: anywhere;
      margin-top: 5px;
      font-size: clamp(20px, 2.2vw, 34px);
      line-height: 1;
      font-weight: 900;
    }
    .metric-grid {
      display: grid;
      grid-template-columns: repeat(6, minmax(130px, 1fr));
      gap: 10px;
    }
    .metric-card {
      display: grid;
      gap: 6px;
      min-height: 86px;
      padding: 12px;
    }
    .metric-card.info { border-color: rgb(61 183 196 / 40%); }
    .metric-card.live { border-color: rgb(64 216 137 / 40%); }
    .metric-card.warn { border-color: rgb(240 184 74 / 40%); }
    .metric-card.bad { border-color: rgb(240 100 112 / 46%); }
    .metric-value {
      overflow-wrap: anywhere;
      font-size: 25px;
      line-height: 1;
      font-weight: 900;
    }
    .metric-foot {
      overflow-wrap: anywhere;
      color: var(--muted);
      font-size: 12px;
    }
    .content-grid {
      display: grid;
      grid-template-columns: minmax(0, 1.65fr) minmax(330px, .85fr);
      gap: 10px;
      align-items: start;
    }
    .side-stack {
      display: grid;
      gap: 10px;
    }
    .panel-body {
      position: relative;
      z-index: 1;
      padding: 10px 12px 12px;
    }
    .kv-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 1px;
      overflow: hidden;
      border: 1px solid rgb(255 255 255 / 5%);
      border-radius: 8px;
      background: rgb(255 255 255 / 5%);
    }
    .kv {
      min-width: 0;
      padding: 10px;
      background: rgb(18 24 27 / 78%);
    }
    .kv strong {
      display: block;
      overflow-wrap: anywhere;
      margin-top: 4px;
      font-size: 20px;
      line-height: 1.1;
    }
    .table-wrap {
      overflow: auto;
      max-height: 360px;
      margin-top: 10px;
      border: 1px solid rgb(255 255 255 / 6%);
      border-radius: 8px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 620px;
    }
    th, td {
      padding: 9px 10px;
      border-bottom: 1px solid rgb(255 255 255 / 6%);
      text-align: right;
      white-space: nowrap;
    }
    th:first-child, td:first-child { text-align: left; }
    th {
      position: sticky;
      top: 0;
      z-index: 1;
      background: var(--panel-strong);
      color: var(--muted);
      font-size: 11px;
      text-transform: uppercase;
    }
    tr:last-child td { border-bottom: 0; }
    .list {
      display: grid;
      gap: 7px;
      max-height: 272px;
      overflow: auto;
      padding-right: 2px;
    }
    .row {
      display: grid;
      gap: 4px;
      min-width: 0;
      border: 1px solid rgb(255 255 255 / 6%);
      border-radius: 8px;
      padding: 9px 10px;
      background: rgb(18 24 27 / 72%);
    }
    .row-head {
      display: flex;
      justify-content: space-between;
      gap: 10px;
      min-width: 0;
    }
    .row-head strong, .row p { overflow-wrap: anywhere; }
    .badge {
      flex: none;
      min-width: 48px;
      border: 1px solid var(--line);
      border-radius: 5px;
      padding: 2px 6px;
      text-align: center;
      font-size: 11px;
      font-weight: 850;
      text-transform: uppercase;
    }
    .badge.info { color: #c9fbff; border-color: rgb(61 183 196 / 48%); }
    .badge.warn { color: #ffe0a0; border-color: rgb(240 184 74 / 56%); }
    .badge.error { color: #ffd1d5; border-color: rgb(240 100 112 / 56%); }
    .empty {
      display: grid;
      min-height: 96px;
      place-items: center;
      border: 1px dashed rgb(255 255 255 / 10%);
      border-radius: 8px;
      color: var(--muted);
      text-align: center;
    }
    .error-panel {
      border-color: rgb(240 100 112 / 58%);
      background: rgb(64 12 19 / 82%);
      color: #ffd1d5;
      padding: 10px 12px;
    }
    details.panel { padding: 0; }
    details > summary {
      position: relative;
      z-index: 1;
      min-height: 42px;
      padding: 13px 14px;
      cursor: pointer;
      color: #d8fbff;
      font-weight: 850;
    }
    pre {
      position: relative;
      z-index: 1;
      overflow: auto;
      max-height: 420px;
      margin: 0;
      padding: 0 14px 14px;
      color: #d7e9ec;
      font: 12px/1.5 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      white-space: pre-wrap;
      word-break: break-word;
    }
    [hidden] { display: none !important; }
    @media (max-width: 1180px) {
      .metric-grid { grid-template-columns: repeat(3, minmax(150px, 1fr)); }
      .content-grid { grid-template-columns: 1fr; }
      .hero { grid-template-columns: 1fr; }
    }
    @media (max-width: 760px) {
      main { padding: 10px; }
      .monitor-header {
        grid-template-columns: 1fr;
        justify-items: start;
      }
      h1 { text-align: left; white-space: normal; }
      .brand, .controls { flex-wrap: wrap; white-space: normal; justify-content: flex-start; }
      .hero-grid, .kv-grid, .metric-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .hero-cell, .kv { padding: 10px; }
    }
    @media (max-width: 480px) {
      .hero-grid, .kv-grid, .metric-grid { grid-template-columns: 1fr; }
      button { flex: 1; }
    }
  </style>
</head>
<body>
  <main>
    <header class="monitor-header">
      <div class="brand">
        <span class="brand-chip">TQSDK</span>
        <span>Asia/Shanghai</span>
        <span id="clock">--:--:--</span>
      </div>
      <h1>策略进程监控面板</h1>
      <div class="controls">
        <span id="status-chip" class="chip warn"><span class="dot"></span><span id="status-label">读取中</span></span>
        <button id="pause-button" type="button">暂停</button>
        <button id="fullscreen-button" type="button">全屏</button>
      </div>
    </header>

    <section id="error-panel" class="panel error-panel" hidden></section>

    <section class="panel hero">
      <div class="hero-main">
        <div class="eyebrow">runtime</div>
        <div id="mode" class="hero-value">--</div>
        <div id="runtime-meta" class="hero-meta">等待 snapshot</div>
      </div>
      <div class="hero-grid" aria-label="核心监控指标">
        <div class="hero-cell">
          <div class="label">tick rows</div>
          <span id="hero-ticks" class="value">--</span>
        </div>
        <div class="hero-cell">
          <div class="label">cache rows</div>
          <span id="hero-cache-rows" class="value">--</span>
        </div>
        <div class="hero-cell">
          <div class="label">inventory</div>
          <span id="hero-inventory-rows" class="value">--</span>
        </div>
        <div class="hero-cell">
          <div class="label">p95 proxy</div>
          <span id="hero-latency" class="value">--</span>
        </div>
      </div>
    </section>

    <section class="metric-grid" aria-label="运行指标">
      <article class="metric-card live">
        <div class="label">行情批次</div>
        <div id="metric-tick-batches" class="metric-value">--</div>
        <div id="metric-tick-foot" class="metric-foot">--</div>
      </article>
      <article class="metric-card info">
        <div class="label">wait_update</div>
        <div id="metric-wait-steps" class="metric-value">--</div>
        <div id="metric-wait-foot" class="metric-foot">--</div>
      </article>
      <article class="metric-card info">
        <div class="label">回测推进</div>
        <div id="metric-backtest-steps" class="metric-value">--</div>
        <div id="metric-backtest-foot" class="metric-foot">--</div>
      </article>
      <article class="metric-card live">
        <div class="label">缓存写入</div>
        <div id="metric-cache-writes" class="metric-value">--</div>
        <div id="metric-cache-foot" class="metric-foot">--</div>
      </article>
      <article class="metric-card warn">
        <div class="label">缺口</div>
        <div id="metric-gaps" class="metric-value">--</div>
        <div id="metric-gaps-foot" class="metric-foot">--</div>
      </article>
      <article class="metric-card info">
        <div class="label">订单事件</div>
        <div id="metric-orders" class="metric-value">--</div>
        <div id="metric-orders-foot" class="metric-foot">--</div>
      </article>
    </section>

    <section class="content-grid">
      <section class="panel">
        <div class="panel-header">
          <h2 class="panel-title">历史缓存资产</h2>
          <span id="history-refresh" class="muted">未扫描</span>
        </div>
        <div class="panel-body">
          <div class="kv-grid">
            <div class="kv"><div class="label">symbols</div><strong id="history-symbols">--</strong></div>
            <div class="kv"><div class="label">days</div><strong id="history-days">--</strong></div>
            <div class="kv"><div class="label">files</div><strong id="history-files">--</strong></div>
            <div class="kv"><div class="label">bytes</div><strong id="history-bytes">--</strong></div>
          </div>
          <div id="history-error" class="error-panel" hidden></div>
          <div id="history-table" class="table-wrap"></div>
          <div id="history-empty" class="empty" hidden>暂无缓存资产数据</div>
        </div>
      </section>

      <aside class="side-stack">
        <section class="panel">
          <div class="panel-header">
            <h2 class="panel-title">订单与交易监控</h2>
            <span id="order-count" class="muted">0 events</span>
          </div>
          <div class="panel-body">
            <div id="order-last" class="empty">暂无订单事件</div>
          </div>
        </section>

        <section class="panel">
          <div class="panel-header">
            <h2 class="panel-title">状态变化事件</h2>
            <span id="incident-count" class="muted">0 incidents</span>
          </div>
          <div class="panel-body">
            <div id="incident-list" class="list"></div>
            <div id="incident-empty" class="empty">暂无事件</div>
          </div>
        </section>
      </aside>
    </section>

    <details class="panel">
      <summary>原始 snapshot</summary>
      <pre id="snapshot">loading</pre>
    </details>
  </main>
  <script>
    const POLL_INTERVAL_MS = 2000;
    const state = { paused: false, timer: null, sequence: 0, latest: null };
    const byId = (id) => document.getElementById(id);
    const text = (id, value) => { byId(id).textContent = value == null || value === '' ? '--' : String(value); };
    const show = (id, visible) => { byId(id).hidden = !visible; };
    const escapeHtml = (value) => String(value ?? '').replace(/[&<>"']/g, (char) => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
    }[char]));

    function fmtNumber(value) {
      if (value == null) return '--';
      return Number(value).toLocaleString('en-US');
    }
    function fmtBytes(value) {
      if (value == null) return '--';
      const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
      let size = Number(value);
      let unit = 0;
      while (size >= 1024 && unit < units.length - 1) {
        size /= 1024;
        unit += 1;
      }
      return `${size >= 10 || unit === 0 ? size.toFixed(0) : size.toFixed(1)} ${units[unit]}`;
    }
    function fmtNs(value) {
      if (!value) return '0 ns';
      if (value < 1000) return `${fmtNumber(value)} ns`;
      if (value < 1_000_000) return `${(value / 1000).toFixed(value < 10_000 ? 1 : 0)} µs`;
      if (value < 1_000_000_000) return `${(value / 1_000_000).toFixed(value < 10_000_000 ? 1 : 0)} ms`;
      return `${(value / 1_000_000_000).toFixed(2)} s`;
    }
    function fmtTime(value) {
      if (!value) return '--';
      return new Date(Number(value)).toLocaleTimeString('zh-CN', { hour12: false });
    }
    function fmtDateTime(value) {
      if (!value) return '--';
      return new Date(Number(value)).toLocaleString('zh-CN', { hour12: false });
    }
    function modeLabel(mode) {
      return {
        idle: 'Idle',
        live: 'Live',
        backtest: 'Backtest',
        replay: 'Replay',
        cache_only: 'Cache Only'
      }[mode] ?? String(mode ?? '--');
    }
    function statusModel(snapshot) {
      if (!snapshot) return { label: '读取中', tone: 'warn' };
      if (snapshot.history?.last_error) return { label: '缓存扫描异常', tone: 'bad' };
      if ((snapshot.incidents ?? []).some((item) => item.severity === 'error')) {
        return { label: '存在错误事件', tone: 'bad' };
      }
      if ((snapshot.cache?.gaps_detected ?? 0) > 0) return { label: '发现缺口', tone: 'warn' };
      if (snapshot.process?.mode === 'live') return { label: '实时监控中', tone: 'live' };
      if (snapshot.process?.mode === 'backtest') return { label: '回测监控中', tone: 'live' };
      return { label: '监控中', tone: 'live' };
    }
    function setStatus(snapshot) {
      const model = statusModel(snapshot);
      byId('status-chip').className = `chip ${model.tone}`;
      text('status-label', state.paused ? '已暂停' : model.label);
    }
    function renderHistory(history) {
      text('history-symbols', fmtNumber(history.inventory_symbols ?? 0));
      text('history-days', fmtNumber(history.inventory_days ?? 0));
      text('history-files', fmtNumber(history.inventory_files ?? 0));
      text('history-bytes', fmtBytes(history.inventory_bytes ?? 0));
      text('history-refresh', history.last_refresh_unix_millis ? `扫描 ${fmtTime(history.last_refresh_unix_millis)}` : '未扫描');
      const hasError = Boolean(history.last_error);
      show('history-error', hasError);
      if (hasError) byId('history-error').textContent = history.last_error;
      const rows = history.top_symbols ?? [];
      show('history-empty', rows.length === 0);
      byId('history-table').hidden = rows.length === 0;
      byId('history-table').innerHTML = rows.length === 0 ? '' : `
        <table>
          <thead><tr><th>symbol</th><th>rows</th><th>days</th><th>files</th><th>bytes</th><th>problems</th></tr></thead>
          <tbody>
            ${rows.map((row) => `
              <tr>
                <td>${escapeHtml(row.symbol)}</td>
                <td>${fmtNumber(row.rows)}</td>
                <td>${fmtNumber(row.days)}</td>
                <td>${fmtNumber(row.files)}</td>
                <td>${fmtBytes(row.bytes)}</td>
                <td>${fmtNumber(row.problem_files)}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>`;
    }
    function renderOrder(order) {
      text('order-count', `${fmtNumber(order.events ?? 0)} events`);
      const event = order.last_event;
      byId('order-last').className = event ? 'row' : 'empty';
      byId('order-last').innerHTML = event ? `
        <div class="row-head"><strong>${escapeHtml(event.symbol)}</strong><span class="badge info">${escapeHtml(event.state)}</span></div>
        <p class="muted">account ${escapeHtml(event.account_id)} · order ${escapeHtml(event.order_id)}</p>
        <p class="muted">elapsed ${fmtNs(event.elapsed_ns)}</p>
      ` : '暂无订单事件';
    }
    function renderIncidents(incidents) {
      const rows = [...(incidents ?? [])].reverse().slice(0, 12);
      text('incident-count', `${fmtNumber(incidents?.length ?? 0)} incidents`);
      show('incident-empty', rows.length === 0);
      byId('incident-list').hidden = rows.length === 0;
      byId('incident-list').innerHTML = rows.map((item) => `
        <div class="row">
          <div class="row-head">
            <strong>${fmtDateTime(item.at_unix_millis)}</strong>
            <span class="badge ${escapeHtml(item.severity)}">${escapeHtml(item.severity)}</span>
          </div>
          <p class="muted">${escapeHtml(item.message)}</p>
        </div>
      `).join('');
    }
    function render(snapshot) {
      state.latest = snapshot;
      setStatus(snapshot);
      text('mode', modeLabel(snapshot.process?.mode));
      text('runtime-meta', `pid ${snapshot.process?.pid ?? '--'} · seq ${fmtNumber(snapshot.process?.snapshot_seq)} · ${snapshot.process?.monitoring_mode ?? 'unknown'} · admin ${snapshot.process?.admin_enabled ? 'on' : 'off'} · started ${fmtDateTime(snapshot.process?.started_at_unix_millis)}`);
      text('hero-ticks', fmtNumber(snapshot.market?.ticks_observed ?? 0));
      text('hero-cache-rows', fmtNumber(snapshot.cache?.rows_written ?? 0));
      text('hero-inventory-rows', fmtNumber(snapshot.history?.inventory_rows ?? 0));
      text('hero-latency', fmtNs(Math.max(snapshot.latency?.wait_step_max_ns ?? 0, snapshot.latency?.cache_write_max_ns ?? 0, snapshot.latency?.backtest_step_max_ns ?? 0)));

      text('metric-tick-batches', fmtNumber(snapshot.market?.tick_batches ?? 0));
      text('metric-tick-foot', `${fmtNumber(snapshot.market?.symbols_observed ?? 0)} symbols · last ${fmtNumber(snapshot.market?.last_tick_batch?.tick_count ?? 0)} ticks`);
      text('metric-wait-steps', fmtNumber(snapshot.latency?.wait_steps ?? 0));
      text('metric-wait-foot', `avg ${fmtNs(snapshot.latency?.wait_step_avg_ns)} · max ${fmtNs(snapshot.latency?.wait_step_max_ns)}`);
      text('metric-backtest-steps', fmtNumber(snapshot.latency?.backtest_steps ?? 0));
      text('metric-backtest-foot', `avg ${fmtNs(snapshot.latency?.backtest_step_avg_ns)} · max ${fmtNs(snapshot.latency?.backtest_step_max_ns)}`);
      text('metric-cache-writes', fmtNumber(snapshot.cache?.writes ?? 0));
      text('metric-cache-foot', `${fmtNumber(snapshot.cache?.rows_written ?? 0)} rows · avg ${fmtNs(snapshot.latency?.cache_write_avg_ns)}`);
      text('metric-gaps', fmtNumber(snapshot.cache?.gaps_detected ?? 0));
      text('metric-gaps-foot', `${fmtNumber(snapshot.history?.problem_files ?? 0)} problem files · missing ${fmtNumber(snapshot.history?.missing_ranges ?? 0)}`);
      text('metric-orders', fmtNumber(snapshot.orders?.events ?? 0));
      text('metric-orders-foot', snapshot.orders?.last_event ? `${snapshot.orders.last_event.symbol} · ${snapshot.orders.last_event.state}` : 'no order events');

      renderHistory(snapshot.history ?? {});
      renderOrder(snapshot.orders ?? {});
      renderIncidents(snapshot.incidents ?? []);
      text('snapshot', JSON.stringify(snapshot, null, 2));
    }
    async function load() {
      const sequence = ++state.sequence;
      try {
        const response = await fetch('/monitor/api/snapshot', { cache: 'no-store' });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const snapshot = await response.json();
        if (sequence !== state.sequence) return;
        show('error-panel', false);
        render(snapshot);
      } catch (error) {
        byId('error-panel').textContent = `读取 snapshot 失败：${error instanceof Error ? error.message : String(error)}`;
        show('error-panel', true);
        byId('status-chip').className = 'chip bad';
        text('status-label', '读取异常');
      }
    }
    function schedule() {
      if (state.timer) window.clearTimeout(state.timer);
      if (!state.paused) {
        state.timer = window.setTimeout(async function tick() {
          await load();
          schedule();
        }, POLL_INTERVAL_MS);
      }
    }
    byId('pause-button').addEventListener('click', () => {
      state.paused = !state.paused;
      text('pause-button', state.paused ? '继续' : '暂停');
      setStatus(state.latest);
      if (!state.paused) void load();
      schedule();
    });
    byId('fullscreen-button').addEventListener('click', async () => {
      if (!document.fullscreenEnabled) return;
      if (document.fullscreenElement) {
        await document.exitFullscreen();
      } else {
        await document.documentElement.requestFullscreen();
      }
    });
    document.addEventListener('fullscreenchange', () => {
      text('fullscreen-button', document.fullscreenElement ? '退出' : '全屏');
    });
    if (!document.fullscreenEnabled) byId('fullscreen-button').disabled = true;
    setInterval(() => text('clock', new Date().toLocaleTimeString('zh-CN', { hour12: false })), 1000);
    text('clock', new Date().toLocaleTimeString('zh-CN', { hour12: false }));
    async function start() {
      await load();
      schedule();
    }
    void start();
  </script>
</body>
</html>
"##;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_snapshot_accumulates_runtime_metrics() {
        let registry = MonitorRegistry::with_config(
            MonitorRuntimeMode::Live,
            MonitoringConfig::localhost(0).with_admin_enabled(true),
        );

        registry.observe_wait_step(10);
        registry.observe_wait_step(30);
        registry.observe_tick_batch(TickBatchStats {
            symbol_count: 2,
            tick_count: 5,
        });
        registry.observe_cache_write(CacheWriteStats {
            rows: 4,
            elapsed_ns: 20,
            gap_detected: true,
        });
        registry.observe_order_event(OrderMonitorEvent {
            account_id: "SIM".to_string(),
            order_id: "order-1".to_string(),
            symbol: "SHFE.au2608".to_string(),
            state: "live".to_string(),
            elapsed_ns: Some(100),
        });

        let snapshot = registry.snapshot();

        assert_eq!(snapshot.process.mode, MonitorRuntimeMode::Live);
        assert!(snapshot.process.admin_enabled);
        assert_eq!(snapshot.latency.wait_steps, 2);
        assert_eq!(snapshot.latency.wait_step_avg_ns, 20);
        assert_eq!(snapshot.latency.wait_step_max_ns, 30);
        assert_eq!(snapshot.market.tick_batches, 1);
        assert_eq!(snapshot.market.ticks_observed, 5);
        assert_eq!(snapshot.cache.rows_written, 4);
        assert_eq!(snapshot.cache.gaps_detected, 1);
        assert_eq!(snapshot.orders.events, 1);
    }

    #[test]
    fn cache_inventory_refresh_interval_is_order_independent() {
        let config = MonitoringConfig::localhost(0)
            .with_cache_inventory_refresh_interval(Duration::from_secs(7))
            .with_cache_inventory("/tmp/tqsdk-monitor-cache");

        assert_eq!(
            config
                .cache_inventory_config()
                .map(CacheInventoryConfig::refresh_interval),
            Some(Duration::from_secs(7))
        );
    }

    #[test]
    fn monitor_html_exposes_dashboard_shell() {
        assert!(MONITOR_HTML.contains("策略进程监控面板"));
        assert!(MONITOR_HTML.contains("历史缓存资产"));
        assert!(MONITOR_HTML.contains("状态变化事件"));
        assert!(MONITOR_HTML.contains("/monitor/api/snapshot"));
    }

    #[tokio::test]
    async fn embedded_monitor_serves_dashboard_html() {
        let config = MonitoringConfig::localhost(0);
        let registry = Arc::new(MonitorRegistry::with_config(
            MonitorRuntimeMode::Backtest,
            config.clone(),
        ));
        let monitor = EmbeddedMonitor::start(config, registry)
            .await
            .expect("monitor starts");

        let mut stream = TcpStream::connect(monitor.bound_addr())
            .await
            .expect("connect monitor");
        stream
            .write_all(b"GET /monitor HTTP/1.1\r\nhost: localhost\r\n\r\n")
            .await
            .expect("write request");
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .expect("read response");
        let response = String::from_utf8(response).expect("utf8 response");

        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("content-type: text/html; charset=utf-8"));
        assert!(response.contains("策略进程监控面板"));
        assert!(response.contains("/monitor/api/snapshot"));
    }

    #[tokio::test]
    async fn embedded_monitor_serves_snapshot_json() {
        let config = MonitoringConfig::localhost(0);
        let registry = Arc::new(MonitorRegistry::with_config(
            MonitorRuntimeMode::Backtest,
            config.clone(),
        ));
        registry.observe_wait_step(42);
        let monitor = EmbeddedMonitor::start(config, registry)
            .await
            .expect("monitor starts");

        let mut stream = TcpStream::connect(monitor.bound_addr())
            .await
            .expect("connect monitor");
        stream
            .write_all(b"GET /monitor/api/snapshot HTTP/1.1\r\nhost: localhost\r\n\r\n")
            .await
            .expect("write request");
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .expect("read response");
        let response = String::from_utf8(response).expect("utf8 response");

        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("\"mode\":\"backtest\""));
        assert!(response.contains("\"wait_steps\":1"));
    }

    #[tokio::test]
    async fn embedded_monitor_refreshes_cache_inventory() {
        let cache_dir = temp_dir("cache-inventory");
        let config = MonitoringConfig::localhost(0)
            .with_cache_inventory(cache_dir.clone())
            .with_cache_inventory_refresh_interval(Duration::from_millis(1));
        let registry = Arc::new(MonitorRegistry::with_config(
            MonitorRuntimeMode::Backtest,
            config.clone(),
        ));
        let _monitor = EmbeddedMonitor::start(config, registry.clone())
            .await
            .expect("monitor starts");

        for _ in 0..50 {
            let snapshot = registry.snapshot();
            if snapshot.history.last_refresh_unix_millis.is_some() {
                assert_eq!(
                    snapshot.history.cache_dir.as_deref(),
                    Some(cache_dir.display().to_string().as_str())
                );
                assert_eq!(snapshot.history.inventory_symbols, 0);
                assert_eq!(snapshot.history.problem_files, 0);
                assert!(snapshot.history.last_error.is_none());
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        panic!("cache inventory did not refresh");
    }

    fn temp_dir(name: &str) -> PathBuf {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "tqsdk-monitor-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
}

#![cfg_attr(not(test), forbid(unsafe_code))]
//! Optional embedded monitoring primitives for `tqsdk-rust`.

use std::collections::VecDeque;
use std::fmt::{Display, Formatter};
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const SCHEMA_VERSION: u16 = 1;
const INCIDENT_LIMIT: usize = 128;
const HTTP_READ_LIMIT: usize = 4096;

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
}

impl MonitoringConfig {
    /// Create a localhost light-mode dashboard configuration.
    #[must_use]
    pub fn localhost(port: u16) -> Self {
        Self {
            mode: MonitoringMode::Light,
            bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
            admin_enabled: false,
        }
    }

    /// Create a disabled monitoring configuration.
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            mode: MonitoringMode::Off,
            bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            admin_enabled: false,
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
    pub fn is_enabled(&self) -> bool {
        !matches!(self.mode, MonitoringMode::Off)
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
    pub inventory_symbols: u64,
    pub inventory_days: u64,
    pub missing_ranges: u64,
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
            incidents: Mutex::new(VecDeque::with_capacity(INCIDENT_LIMIT)),
        }
    }

    pub fn set_mode(&self, mode: MonitorRuntimeMode) {
        if let Ok(mut current) = self.mode.lock() {
            *current = mode;
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
            history: HistoryPanel::default(),
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
    task: tokio::task::JoinHandle<()>,
}

impl EmbeddedMonitor {
    pub async fn start(
        config: MonitoringConfig,
        registry: Arc<MonitorRegistry>,
    ) -> Result<Self, MonitorError> {
        if !config.is_enabled() {
            return Err(MonitorError::Disabled);
        }
        let listener = TcpListener::bind(config.bind_addr()).await?;
        let bound_addr = listener.local_addr()?;
        let task_registry = registry.clone();
        let task = tokio::spawn(async move {
            serve(listener, task_registry).await;
        });
        Ok(Self {
            bound_addr,
            registry,
            task,
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
        self.task.abort();
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

const MONITOR_HTML: &str = r#"<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>tqsdk monitor</title>
  <style>
    body { margin: 0; font: 14px/1.5 system-ui, sans-serif; background: #0f172a; color: #e5edf5; }
    main { max-width: 1080px; margin: 0 auto; padding: 24px; }
    pre { overflow: auto; border: 1px solid #334155; border-radius: 8px; padding: 16px; background: #020617; }
  </style>
</head>
<body>
  <main>
    <h1>tqsdk monitor</h1>
    <pre id="snapshot">loading</pre>
  </main>
  <script>
    async function load() {
      const response = await fetch('/monitor/api/snapshot', { cache: 'no-store' });
      document.getElementById('snapshot').textContent =
        JSON.stringify(await response.json(), null, 2);
    }
    load();
    setInterval(load, 2000);
  </script>
</body>
</html>
"#;

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
}

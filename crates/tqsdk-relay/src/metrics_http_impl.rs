#![cfg_attr(not(test), forbid(unsafe_code))]

use std::sync::{Arc, Mutex, RwLock, TryLockError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::time::MissedTickBehavior;

use std::collections::VecDeque;

use tqsdk_relay::dashboard::dashboard_asset;
use tqsdk_relay::{
    DashboardSnapshotInputs, DashboardTimelineHistory, DashboardTimelineHistorySample, RelayEngine,
    RelayError, RelayResult, SymbolMetricsQuery,
};

const DASHBOARD_TIMELINE_HISTORY_WINDOW_MILLIS: u64 = 300_000;
const DASHBOARD_TIMELINE_HISTORY_MIN_SAMPLE_INTERVAL_MILLIS: u64 = 2_000;
const DASHBOARD_TIMELINE_HISTORY_SAMPLE_LIMIT: usize = 180;

#[derive(Debug, Clone)]
struct DashboardTimelineHistoryCache {
    samples: VecDeque<DashboardTimelineHistorySample>,
}

impl Default for DashboardTimelineHistoryCache {
    fn default() -> Self {
        Self {
            samples: VecDeque::with_capacity(DASHBOARD_TIMELINE_HISTORY_SAMPLE_LIMIT),
        }
    }
}

impl DashboardTimelineHistoryCache {
    fn push(&mut self, sample: DashboardTimelineHistorySample) {
        let sampled_at = sample.sampled_at_unix_millis;
        self.prune(sampled_at);
        if let Some(last) = self.samples.back_mut()
            && sampled_at
                < last
                    .sampled_at_unix_millis
                    .saturating_add(DASHBOARD_TIMELINE_HISTORY_MIN_SAMPLE_INTERVAL_MILLIS)
        {
            *last = sample;
            self.prune(sampled_at);
            return;
        }
        self.samples.push_back(sample);
        self.prune(sampled_at);
    }

    fn snapshot(&self) -> DashboardTimelineHistory {
        DashboardTimelineHistory {
            samples: self.samples.iter().cloned().collect(),
        }
    }

    fn prune(&mut self, now_unix_millis: u64) {
        let cutoff = now_unix_millis.saturating_sub(DASHBOARD_TIMELINE_HISTORY_WINDOW_MILLIS);
        while self
            .samples
            .front()
            .is_some_and(|sample| sample.sampled_at_unix_millis < cutoff)
        {
            self.samples.pop_front();
        }
        while self.samples.len() > DASHBOARD_TIMELINE_HISTORY_SAMPLE_LIMIT {
            self.samples.pop_front();
        }
    }
}

const DASHBOARD_SNAPSHOT_REFRESH_INTERVAL: Duration = Duration::from_secs(2);

pub(crate) trait HistoryMetrics: Send + Sync + std::fmt::Debug {
    fn health(&self) -> Value;
    fn metrics(&self) -> Value;
}

#[derive(Debug)]
pub(crate) struct NoHistoryMetrics;
impl HistoryMetrics for NoHistoryMetrics {
    fn health(&self) -> Value {
        json!({"configured": false, "listener": false, "ready": false})
    }
    fn metrics(&self) -> Value {
        self.health()
    }
}

pub(crate) async fn serve_metrics_until_with_history(
    listener: TcpListener,
    engine: Arc<Mutex<RelayEngine>>,
    mut shutdown: oneshot::Receiver<()>,
    history: Arc<dyn HistoryMetrics>,
) -> RelayResult<()> {
    let timeline_history = Arc::new(Mutex::new(DashboardTimelineHistoryCache::default()));
    let dashboard_cache = DashboardSnapshotCache::from_engine(&engine)?;
    let mut dashboard_refresh = tokio::time::interval(DASHBOARD_SNAPSHOT_REFRESH_INTERVAL);
    dashboard_refresh.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => return Ok(()),
            _ = dashboard_refresh.tick() => {
                if dashboard_cache.refresh_from_engine(&engine)? {
                    let inputs = dashboard_cache.load()?;
                    push_dashboard_timeline_history_sample(&timeline_history, inputs.as_ref())?;
                }
            }
            accepted = listener.accept() => {
                let (mut stream, _) = accepted.map_err(|err| {
                    RelayError::Transport(format!("metrics accept failed: {err}"))
                })?;
                let engine = engine.clone();
                let timeline_history = timeline_history.clone();
            let dashboard_cache = dashboard_cache.clone();
            let history = history.clone();
                tokio::spawn(async move {
                    if let Err(err) =
                    serve_metrics_stream(&mut stream, engine, dashboard_cache, timeline_history, history).await
                    {
                        eprintln!("{err}");
                    }
                });
            }
        }
    }
}

async fn serve_metrics_stream(
    stream: &mut TcpStream,
    engine: Arc<Mutex<RelayEngine>>,
    dashboard_cache: DashboardSnapshotCache,
    timeline_history: Arc<Mutex<DashboardTimelineHistoryCache>>,
    history: Arc<dyn HistoryMetrics>,
) -> RelayResult<()> {
    let request = read_http_request(stream).await?;
    let target = request_target(&request)?;
    let accept_gzip = request_accepts_gzip(&request);
    let response = match target.path {
        "/health" => {
            let health = engine
                .lock()
                .map_err(|_| RelayError::Internal("relay engine lock poisoned".to_string()))?
                .health_snapshot();
            with_history(
                serde_json::to_value(health).map_err(|err| {
                    RelayError::Internal(format!("health JSON encode failed: {err}"))
                })?,
                history.health(),
            )
        }
        "/metrics" => {
            let metrics = engine
                .lock()
                .map_err(|_| RelayError::Internal("relay engine lock poisoned".to_string()))?
                .metrics_snapshot();
            with_history(
                serde_json::to_value(metrics).map_err(|err| {
                    RelayError::Internal(format!("metrics JSON encode failed: {err}"))
                })?,
                history.metrics(),
            )
        }
        "/symbol-metrics" => {
            let query = match SymbolMetricsQuery::from_query_string(target.query) {
                Ok(query) => query,
                Err(error) => {
                    write_response(stream, 400, json!({ "error": error }), accept_gzip).await?;
                    return Ok(());
                }
            };
            let inputs = dashboard_cache.load()?;
            let symbol_metrics = inputs.symbol_metrics_snapshot(&query);
            serde_json::to_value(symbol_metrics).map_err(|err| {
                RelayError::Internal(format!("symbol metrics JSON encode failed: {err}"))
            })?
        }
        "/dashboard-snapshot" => {
            let query = match DashboardSnapshotQuery::from_query_string(target.query) {
                Ok(query) => query,
                Err(error) => {
                    write_response(stream, 400, json!({ "error": error }), accept_gzip).await?;
                    return Ok(());
                }
            };
            let inputs = dashboard_cache.load()?;
            let (mut dashboard, timeline_sample) =
                inputs.dashboard_snapshot_and_timeline_sample(&query.symbol_metrics);
            {
                let mut timeline_history = timeline_history.lock().map_err(|_| {
                    RelayError::Internal("dashboard timeline history lock poisoned".to_string())
                })?;
                timeline_history.push(timeline_sample);
                if query.include_timeline_history {
                    dashboard.timeline_history = Some(timeline_history.snapshot());
                }
            }
            serde_json::to_value(dashboard).map_err(|err| {
                RelayError::Internal(format!("dashboard snapshot JSON encode failed: {err}"))
            })?
        }
        path if path == "/dashboard"
            || path == "/dashboard/"
            || path.starts_with("/dashboard/") =>
        {
            let Some(asset) = dashboard_asset(path) else {
                write_response(stream, 404, json!({"error": "not found"}), accept_gzip).await?;
                return Ok(());
            };
            write_bytes_response(stream, 200, asset.content_type, asset.bytes, accept_gzip).await?;
            return Ok(());
        }
        _ => {
            write_response(stream, 404, json!({"error": "not found"}), accept_gzip).await?;
            return Ok(());
        }
    };
    write_response(stream, 200, response, accept_gzip).await
}

fn push_dashboard_timeline_history_sample(
    timeline_history: &Arc<Mutex<DashboardTimelineHistoryCache>>,
    inputs: &DashboardSnapshotInputs,
) -> RelayResult<()> {
    let mut timeline_history = timeline_history.lock().map_err(|_| {
        RelayError::Internal("dashboard timeline history lock poisoned".to_string())
    })?;
    let (_, sample) = inputs.dashboard_snapshot_and_timeline_sample(&SymbolMetricsQuery::default());
    timeline_history.push(sample);
    Ok(())
}

fn with_history(mut market: Value, history: Value) -> Value {
    if let Some(object) = market.as_object_mut() {
        object.insert("history".to_string(), history);
    }
    market
}

#[derive(Debug, Clone)]
struct DashboardSnapshotCache {
    latest: Arc<RwLock<Arc<DashboardSnapshotInputs>>>,
}

impl DashboardSnapshotCache {
    fn from_engine(engine: &Arc<Mutex<RelayEngine>>) -> RelayResult<Self> {
        let inputs = engine
            .lock()
            .map_err(|_| RelayError::Internal("relay engine lock poisoned".to_string()))?
            .dashboard_snapshot_inputs_at(current_unix_millis());
        Ok(Self::new(inputs))
    }

    fn new(inputs: DashboardSnapshotInputs) -> Self {
        Self {
            latest: Arc::new(RwLock::new(Arc::new(inputs))),
        }
    }

    fn load(&self) -> RelayResult<Arc<DashboardSnapshotInputs>> {
        self.latest
            .read()
            .map_err(|_| RelayError::Internal("dashboard snapshot cache poisoned".to_string()))
            .map(|inputs| inputs.clone())
    }

    fn store(&self, inputs: DashboardSnapshotInputs) -> RelayResult<()> {
        let mut latest = self
            .latest
            .write()
            .map_err(|_| RelayError::Internal("dashboard snapshot cache poisoned".to_string()))?;
        *latest = Arc::new(inputs);
        Ok(())
    }

    fn refresh_from_engine(&self, engine: &Arc<Mutex<RelayEngine>>) -> RelayResult<bool> {
        let inputs = match engine.try_lock() {
            Ok(engine) => engine.dashboard_snapshot_inputs_at(current_unix_millis()),
            Err(TryLockError::WouldBlock) => return Ok(false),
            Err(TryLockError::Poisoned(_)) => {
                return Err(RelayError::Internal(
                    "relay engine lock poisoned".to_string(),
                ));
            }
        };
        self.store(inputs)?;
        Ok(true)
    }
}

async fn read_http_request(stream: &mut TcpStream) -> RelayResult<String> {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 1024];
    while !buffer.windows(4).any(|window| window == b"\r\n\r\n") {
        let read = stream
            .read(&mut chunk)
            .await
            .map_err(|err| RelayError::Transport(format!("metrics read failed: {err}")))?;
        if read == 0 {
            return Err(RelayError::invalid_protocol(
                "metrics HTTP request ended early",
            ));
        }
        buffer.extend_from_slice(&chunk[..read]);
        if buffer.len() > 8192 {
            return Err(RelayError::invalid_protocol(
                "metrics HTTP request header too large",
            ));
        }
    }
    String::from_utf8(buffer)
        .map_err(|err| RelayError::invalid_protocol(format!("invalid metrics HTTP request: {err}")))
}

struct RequestTarget<'a> {
    path: &'a str,
    query: &'a str,
}

struct DashboardSnapshotQuery {
    symbol_metrics: SymbolMetricsQuery,
    include_timeline_history: bool,
}

impl DashboardSnapshotQuery {
    fn from_query_string(query: &str) -> Result<Self, &'static str> {
        if query.is_empty() {
            return Ok(Self {
                symbol_metrics: SymbolMetricsQuery::default(),
                include_timeline_history: false,
            });
        }

        let mut include_timeline_history = false;
        let mut symbol_metric_pairs = Vec::new();
        for pair in query.split('&') {
            if pair.is_empty() {
                continue;
            }
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            if key == "timeline_history" {
                include_timeline_history = parse_query_bool(value)?;
            } else {
                symbol_metric_pairs.push(pair);
            }
        }
        Ok(Self {
            symbol_metrics: SymbolMetricsQuery::from_query_string(&symbol_metric_pairs.join("&"))?,
            include_timeline_history,
        })
    }
}

fn parse_query_bool(value: &str) -> Result<bool, &'static str> {
    match value {
        "" | "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err("invalid timeline_history"),
    }
}

fn request_target(request: &str) -> RelayResult<RequestTarget<'_>> {
    let first = request
        .lines()
        .next()
        .ok_or_else(|| RelayError::invalid_protocol("metrics HTTP request missing request line"))?;
    let mut parts = first.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| RelayError::invalid_protocol("metrics HTTP request missing method"))?;
    let target = parts
        .next()
        .ok_or_else(|| RelayError::invalid_protocol("metrics HTTP request missing path"))?;
    if method != "GET" {
        return Err(RelayError::invalid_protocol(
            "metrics HTTP server only accepts GET",
        ));
    }
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    Ok(RequestTarget { path, query })
}

fn request_accepts_gzip(request: &str) -> bool {
    for line in request.lines() {
        if line.eq_ignore_ascii_case("") {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            if name.trim().eq_ignore_ascii_case("Accept-Encoding") {
                return value
                    .split(',')
                    .any(|s| s.trim().eq_ignore_ascii_case("gzip"));
            }
        }
    }
    false
}

async fn write_response(
    stream: &mut TcpStream,
    status: u16,
    body: Value,
    gzip: bool,
) -> RelayResult<()> {
    let body = body.to_string();
    write_bytes_response_with_cache_control(
        stream,
        status,
        "application/json",
        body.as_bytes(),
        "no-store",
        gzip,
    )
    .await
}

async fn write_bytes_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
    gzip: bool,
) -> RelayResult<()> {
    write_bytes_response_with_cache_control(
        stream,
        status,
        content_type,
        body,
        "public, max-age=60",
        gzip,
    )
    .await
}

async fn write_bytes_response_with_cache_control(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
    cache_control: &str,
    gzip: bool,
) -> RelayResult<()> {
    let (body, encoding_header) = if gzip {
        use std::io::Write;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(body).unwrap();
        (encoder.finish().unwrap(), "Content-Encoding: gzip\r\n")
    } else {
        (body.to_vec(), "")
    };

    let reason = status_reason(status);
    let header = format!(
        "HTTP/1.1 {status} {reason}\r\n\
Content-Type: {content_type}\r\n\
Content-Length: {}\r\n\
Cache-Control: {cache_control}\r\n\
X-Content-Type-Options: nosniff\r\n\
{encoding_header}\
Connection: close\r\n\
\r\n",
        body.len(),
    );
    stream
        .write_all(header.as_bytes())
        .await
        .map_err(|err| RelayError::Transport(format!("metrics write failed: {err}")))?;
    stream
        .write_all(&body)
        .await
        .map_err(|err| RelayError::Transport(format!("metrics write failed: {err}")))
}

fn status_reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "OK",
    }
}

fn current_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dashboard_cache_load_does_not_take_engine_lock() {
        let engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(16, 16)));
        {
            let mut engine = engine.lock().unwrap();
            engine.record_universe_refresh_success_for_symbols(
                ["SHFE.au2602"],
                11,
                None,
                None,
                1_700_000_000,
            );
        }
        let cache = DashboardSnapshotCache::from_engine(&engine).unwrap();

        let _engine_guard = engine.lock().unwrap();
        let inputs = cache.load().unwrap();
        let dashboard = inputs.dashboard_snapshot(&SymbolMetricsQuery::default());

        assert_eq!(dashboard.global.total, 1);
        assert!(dashboard.timeline.exchanges.contains_key("SHFE"));
    }

    #[test]
    fn dashboard_cache_refresh_skips_busy_engine() {
        let engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(16, 16)));
        let cache = DashboardSnapshotCache::from_engine(&engine).unwrap();

        let engine_guard = engine.lock().unwrap();
        assert!(!cache.refresh_from_engine(&engine).unwrap());
        drop(engine_guard);

        {
            let mut engine = engine.lock().unwrap();
            engine.record_universe_refresh_success_for_symbols(
                ["DCE.m2609"],
                10,
                None,
                None,
                1_700_000_001,
            );
        }

        assert!(cache.refresh_from_engine(&engine).unwrap());
        let inputs = cache.load().unwrap();
        let dashboard = inputs.dashboard_snapshot(&SymbolMetricsQuery::default());
        assert_eq!(dashboard.global.total, 1);
        assert!(dashboard.timeline.exchanges.contains_key("DCE"));
    }

    #[derive(Debug)]
    struct HistoryOverlay;

    impl HistoryMetrics for HistoryOverlay {
        fn health(&self) -> Value {
            json!({"configured": true, "ready": true})
        }
        fn metrics(&self) -> Value {
            json!({"configured": true, "active": 0})
        }
    }

    #[tokio::test]
    async fn health_and_metrics_add_history_without_waiting_for_market_lock() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(8, 8)));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(serve_metrics_until_with_history(
            listener,
            engine,
            shutdown_rx,
            Arc::new(HistoryOverlay),
        ));
        for path in ["/health", "/metrics"] {
            let mut stream = TcpStream::connect(address).await.unwrap();
            stream
                .write_all(format!("GET {path} HTTP/1.1\r\nHost: test\r\n\r\n").as_bytes())
                .await
                .unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).await.unwrap();
            assert!(response.contains("\"history\""));
            assert!(response.contains("\"configured\":true"));
        }
        let _ = shutdown_tx.send(());
        task.await.unwrap().unwrap();
    }

    #[test]
    fn dashboard_refresh_can_feed_timeline_history_without_request() {
        let engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(16, 16)));
        let cache = DashboardSnapshotCache::from_engine(&engine).unwrap();
        let timeline_history = Arc::new(Mutex::new(DashboardTimelineHistoryCache::default()));

        {
            let mut engine = engine.lock().unwrap();
            engine.record_universe_refresh_success_for_symbols(
                ["SHFE.au2602"],
                11,
                None,
                None,
                1_700_000_000,
            );
        }
        assert!(cache.refresh_from_engine(&engine).unwrap());
        let inputs = cache.load().unwrap();

        push_dashboard_timeline_history_sample(&timeline_history, inputs.as_ref()).unwrap();

        let history = timeline_history.lock().unwrap().snapshot();
        assert_eq!(history.samples.len(), 1);
        assert_eq!(history.samples[0].sample.global.total, 1);
        assert!(history.samples[0].sample.exchanges.contains_key("SHFE"));
    }
}

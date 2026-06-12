#![cfg_attr(not(test), forbid(unsafe_code))]

use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

use crate::dashboard::dashboard_asset;
use crate::engine::{DashboardTimelineHistoryCache, RelayEngine};
use crate::error::{RelayError, RelayResult};
use crate::symbol_metrics::SymbolMetricsQuery;

pub async fn serve_metrics_until(
    listener: TcpListener,
    engine: Arc<Mutex<RelayEngine>>,
    mut shutdown: oneshot::Receiver<()>,
) -> RelayResult<()> {
    let timeline_history = Arc::new(Mutex::new(DashboardTimelineHistoryCache::default()));
    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => return Ok(()),
            accepted = listener.accept() => {
                let (mut stream, _) = accepted.map_err(|err| {
                    RelayError::Transport(format!("metrics accept failed: {err}"))
                })?;
                let engine = engine.clone();
                let timeline_history = timeline_history.clone();
                tokio::spawn(async move {
                    if let Err(err) = serve_metrics_stream(&mut stream, engine, timeline_history).await {
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
    timeline_history: Arc<Mutex<DashboardTimelineHistoryCache>>,
) -> RelayResult<()> {
    let request = read_http_request(stream).await?;
    let target = request_target(&request)?;
    let response = match target.path {
        "/health" => {
            let health = engine
                .lock()
                .map_err(|_| RelayError::Internal("relay engine lock poisoned".to_string()))?
                .health_snapshot();
            serde_json::to_value(health)
                .map_err(|err| RelayError::Internal(format!("health JSON encode failed: {err}")))?
        }
        "/metrics" => {
            let metrics = engine
                .lock()
                .map_err(|_| RelayError::Internal("relay engine lock poisoned".to_string()))?
                .metrics_snapshot();
            serde_json::to_value(metrics)
                .map_err(|err| RelayError::Internal(format!("metrics JSON encode failed: {err}")))?
        }
        "/symbol-metrics" => {
            let query = match SymbolMetricsQuery::from_query_string(target.query) {
                Ok(query) => query,
                Err(error) => {
                    write_response(stream, 400, json!({ "error": error })).await?;
                    return Ok(());
                }
            };
            let inputs = engine
                .lock()
                .map_err(|_| RelayError::Internal("relay engine lock poisoned".to_string()))?
                .dashboard_snapshot_inputs_at(current_unix_millis());
            let symbol_metrics = inputs.symbol_metrics_snapshot(&query);
            serde_json::to_value(symbol_metrics).map_err(|err| {
                RelayError::Internal(format!("symbol metrics JSON encode failed: {err}"))
            })?
        }
        "/dashboard-snapshot" => {
            let query = match DashboardSnapshotQuery::from_query_string(target.query) {
                Ok(query) => query,
                Err(error) => {
                    write_response(stream, 400, json!({ "error": error })).await?;
                    return Ok(());
                }
            };
            let inputs = engine
                .lock()
                .map_err(|_| RelayError::Internal("relay engine lock poisoned".to_string()))?
                .dashboard_snapshot_inputs_at(current_unix_millis());
            let (mut dashboard, timeline_sample) =
                inputs.into_dashboard_snapshot_and_timeline_sample(&query.symbol_metrics);
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
                write_response(stream, 404, json!({"error": "not found"})).await?;
                return Ok(());
            };
            write_bytes_response(stream, 200, asset.content_type, asset.bytes).await?;
            return Ok(());
        }
        _ => {
            write_response(stream, 404, json!({"error": "not found"})).await?;
            return Ok(());
        }
    };
    write_response(stream, 200, response).await
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

async fn write_response(stream: &mut TcpStream, status: u16, body: Value) -> RelayResult<()> {
    let reason = status_reason(status);
    let body = body.to_string();
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\n\
Content-Type: application/json\r\n\
Content-Length: {}\r\n\
Cache-Control: no-store\r\n\
X-Content-Type-Options: nosniff\r\n\
Connection: close\r\n\
\r\n\
{body}",
        body.len(),
    );
    stream
        .write_all(response.as_bytes())
        .await
        .map_err(|err| RelayError::Transport(format!("metrics write failed: {err}")))
}

async fn write_bytes_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) -> RelayResult<()> {
    let header = format!(
        "HTTP/1.1 {status} {}\r\n\
Content-Type: {content_type}\r\n\
Content-Length: {}\r\n\
Cache-Control: public, max-age=60\r\n\
X-Content-Type-Options: nosniff\r\n\
Connection: close\r\n\
\r\n",
        status_reason(status),
        body.len(),
    );
    stream
        .write_all(header.as_bytes())
        .await
        .map_err(|err| RelayError::Transport(format!("metrics write failed: {err}")))?;
    stream
        .write_all(body)
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

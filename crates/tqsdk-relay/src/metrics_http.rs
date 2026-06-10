#![cfg_attr(not(test), forbid(unsafe_code))]

use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

use crate::dashboard::{DASHBOARD_HTML, DASHBOARD_JS};
use crate::engine::RelayEngine;
use crate::error::{RelayError, RelayResult};
use crate::symbol_metrics::SymbolMetricsQuery;

pub async fn serve_metrics_until(
    listener: TcpListener,
    engine: Arc<Mutex<RelayEngine>>,
    mut shutdown: oneshot::Receiver<()>,
) -> RelayResult<()> {
    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => return Ok(()),
            accepted = listener.accept() => {
                let (mut stream, _) = accepted.map_err(|err| {
                    RelayError::Transport(format!("metrics accept failed: {err}"))
                })?;
                let engine = engine.clone();
                tokio::spawn(async move {
                    if let Err(err) = serve_metrics_stream(&mut stream, engine).await {
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
            let symbol_metrics = engine
                .lock()
                .map_err(|_| RelayError::Internal("relay engine lock poisoned".to_string()))?
                .symbol_metrics_snapshot(&query);
            serde_json::to_value(symbol_metrics).map_err(|err| {
                RelayError::Internal(format!("symbol metrics JSON encode failed: {err}"))
            })?
        }
        "/dashboard" => {
            write_text_response(stream, 200, "text/html; charset=utf-8", DASHBOARD_HTML).await?;
            return Ok(());
        }
        "/dashboard/app.js" => {
            write_text_response(
                stream,
                200,
                "application/javascript; charset=utf-8",
                DASHBOARD_JS,
            )
            .await?;
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

async fn write_text_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &str,
) -> RelayResult<()> {
    let response = format!(
        "HTTP/1.1 {status} {}\r\n\
Content-Type: {content_type}\r\n\
Content-Length: {}\r\n\
Cache-Control: no-store\r\n\
X-Content-Type-Options: nosniff\r\n\
Connection: close\r\n\
\r\n\
{body}",
        status_reason(status),
        body.len(),
    );
    stream
        .write_all(response.as_bytes())
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

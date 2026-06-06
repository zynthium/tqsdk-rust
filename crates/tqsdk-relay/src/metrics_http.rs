#![cfg_attr(not(test), forbid(unsafe_code))]

use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

use crate::engine::RelayEngine;
use crate::error::{RelayError, RelayResult};
use crate::observability::RelaySourceStatus;

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
    let path = request_path(&request)?;
    let response = match path {
        "/health" => {
            let health = engine
                .lock()
                .map_err(|_| RelayError::Internal("relay engine lock poisoned".to_string()))?
                .health_snapshot();
            json!({
                "ready": health.ready,
                "upstream_status": source_status(health.upstream_status),
                "downstream_clients": health.downstream_clients,
            })
        }
        "/metrics" => {
            let metrics = engine
                .lock()
                .map_err(|_| RelayError::Internal("relay engine lock poisoned".to_string()))?
                .metrics_snapshot();
            serde_json::to_value(metrics)
                .map_err(|err| RelayError::Internal(format!("metrics JSON encode failed: {err}")))?
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

fn request_path(request: &str) -> RelayResult<&str> {
    let first = request
        .lines()
        .next()
        .ok_or_else(|| RelayError::invalid_protocol("metrics HTTP request missing request line"))?;
    let mut parts = first.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| RelayError::invalid_protocol("metrics HTTP request missing method"))?;
    let path = parts
        .next()
        .ok_or_else(|| RelayError::invalid_protocol("metrics HTTP request missing path"))?;
    if method != "GET" {
        return Err(RelayError::invalid_protocol(
            "metrics HTTP server only accepts GET",
        ));
    }
    Ok(path)
}

async fn write_response(stream: &mut TcpStream, status: u16, body: Value) -> RelayResult<()> {
    let reason = if status == 200 { "OK" } else { "Not Found" };
    let body = body.to_string();
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\n\
Content-Type: application/json\r\n\
Content-Length: {}\r\n\
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

fn source_status(status: RelaySourceStatus) -> &'static str {
    match status {
        RelaySourceStatus::Connecting => "connecting",
        RelaySourceStatus::Up => "up",
        RelaySourceStatus::Degraded => "degraded",
        RelaySourceStatus::Down => "down",
    }
}

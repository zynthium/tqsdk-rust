#![cfg_attr(not(test), forbid(unsafe_code))]

use std::sync::{Arc, Mutex};

use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::engine::RelayEngine;
use crate::error::RelayResult;
use crate::metrics_http_impl::{NoHistoryMetrics, serve_metrics_until_with_history};

/// Serves the relay's stable market-only metrics endpoint.
///
/// The relay binary uses the same implementation with its private history
/// overlay; this compatibility wrapper deliberately retains the public API.
pub async fn serve_metrics_until(
    listener: TcpListener,
    engine: Arc<Mutex<RelayEngine>>,
    shutdown: oneshot::Receiver<()>,
) -> RelayResult<()> {
    serve_metrics_until_with_history(listener, engine, shutdown, Arc::new(NoHistoryMetrics)).await
}

#![cfg_attr(not(test), forbid(unsafe_code))]

use std::sync::{Arc, Mutex};

use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tqsdk_relay::{RelayConfig, RelayEngine, RelayError, RelayServer};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), RelayError> {
    let config = RelayConfig::default();
    config.validate()?;

    let engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(
        config.tick_ring_capacity,
        config.kline_ring_capacity,
    )));
    let server = RelayServer::new(engine);
    let listener = TcpListener::bind(&config.downstream_listen)
        .await
        .map_err(|err| RelayError::Transport(format!("downstream bind failed: {err}")))?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let _ = shutdown_tx.send(());
        }
    });
    eprintln!(
        "tqsdk-relay listening: downstream={} metrics={}",
        config.downstream_listen, config.metrics_listen
    );
    server.serve_until(listener, shutdown_rx).await
}

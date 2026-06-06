#![cfg_attr(not(test), forbid(unsafe_code))]

use std::sync::{Arc, Mutex};

use tokio::net::TcpListener;
use tokio::sync::oneshot;
#[cfg(feature = "server")]
use tqsdk_relay::spawn_configured_upstream_pump;
use tqsdk_relay::{RelayConfig, RelayEngine, RelayError, RelayServer};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), RelayError> {
    let config = RelayConfig::from_env()?;

    let engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(
        config.tick_ring_capacity,
        config.kline_ring_capacity,
    )));
    let server = RelayServer::new(engine);
    let listener = TcpListener::bind(&config.downstream_listen)
        .await
        .map_err(|err| RelayError::Transport(format!("downstream bind failed: {err}")))?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    #[cfg(feature = "server")]
    let upstream_shutdown = spawn_configured_upstream_pump(&config, server.clone()).await?;
    #[cfg(not(feature = "server"))]
    let upstream_shutdown = {
        if config.upstream_tick_chart()?.is_some() {
            return Err(RelayError::invalid_config(
                "tqsdk-relay server feature is required for upstream websocket",
            ));
        }
        None::<oneshot::Sender<()>>
    };

    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            if let Some(upstream_shutdown) = upstream_shutdown {
                let _ = upstream_shutdown.send(());
            }
            let _ = shutdown_tx.send(());
        }
    });
    eprintln!(
        "tqsdk-relay listening: downstream={} metrics={}",
        config.downstream_listen, config.metrics_listen
    );
    server.serve_until(listener, shutdown_rx).await
}

#![cfg_attr(not(test), forbid(unsafe_code))]

use std::sync::{Arc, Mutex};

use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tqsdk_relay::{
    RelayConfig, RelayEngine, RelayError, RelayServer, RelayStartupReport, serve_metrics_until,
};
#[cfg(feature = "server")]
use tqsdk_relay::{resolve_configured_upstream_tick_charts, spawn_configured_upstream_pump};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), RelayError> {
    let config = RelayConfig::from_env()?;

    if config.dry_run {
        #[cfg(feature = "server")]
        let charts = resolve_configured_upstream_tick_charts(&config).await?;
        #[cfg(not(feature = "server"))]
        let charts = config.upstream_tick_charts()?;
        println!(
            "{}",
            RelayStartupReport::from_config_and_charts(&config, &charts).log_line()
        );
        return Ok(());
    }

    let engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(
        config.tick_ring_capacity,
        config.kline_ring_capacity,
    )));
    let startup_charts = if !config.futures_symbols.is_empty() {
        config.upstream_tick_charts()?
    } else {
        Vec::new()
    };
    eprintln!(
        "{}",
        RelayStartupReport::from_config_and_charts(&config, &startup_charts).log_line()
    );
    let server = RelayServer::new(engine.clone());
    let listener = TcpListener::bind(&config.downstream_listen)
        .await
        .map_err(|err| RelayError::Transport(format!("downstream bind failed: {err}")))?;
    let metrics_listener = TcpListener::bind(&config.metrics_listen)
        .await
        .map_err(|err| RelayError::Transport(format!("metrics bind failed: {err}")))?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (metrics_shutdown_tx, metrics_shutdown_rx) = oneshot::channel();
    #[cfg(feature = "server")]
    let upstream_shutdown = spawn_configured_upstream_pump(&config, server.clone()).await?;
    #[cfg(not(feature = "server"))]
    let upstream_shutdown = {
        if !config.upstream_tick_charts()?.is_empty() {
            return Err(RelayError::invalid_config(
                "tqsdk-relay server feature is required for upstream websocket",
            ));
        }
        None::<oneshot::Sender<()>>
    };

    tokio::spawn(async move {
        if let Err(err) = serve_metrics_until(metrics_listener, engine, metrics_shutdown_rx).await {
            eprintln!("{err}");
        }
    });
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            if let Some(upstream_shutdown) = upstream_shutdown {
                let _ = upstream_shutdown.send(());
            }
            let _ = metrics_shutdown_tx.send(());
            let _ = shutdown_tx.send(());
        }
    });
    eprintln!(
        "tqsdk-relay listening: downstream={} metrics={}",
        config.downstream_listen, config.metrics_listen
    );
    server.serve_until(listener, shutdown_rx).await
}

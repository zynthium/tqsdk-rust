#![cfg_attr(not(test), forbid(unsafe_code))]

use std::sync::{Arc, Mutex};

use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tqsdk_relay::{
    RelayConfig, RelayEngine, RelayError, RelayServer, RelayStartupReport, serve_metrics_until,
};
#[cfg(feature = "server")]
use tqsdk_relay::{resolve_configured_upstream_tick_charts, spawn_configured_upstream_pump};

#[cfg(feature = "history")]
mod history;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), RelayError> {
    let config = RelayConfig::from_env()?;
    #[cfg(feature = "history")]
    let history_config = history::HistoryConfig::from_env()?;

    if config.dry_run {
        #[cfg(feature = "server")]
        let charts = resolve_configured_upstream_tick_charts(&config).await?;
        #[cfg(not(feature = "server"))]
        let charts = {
            if let Some(expression) = config.futures_universe_expression.as_ref() {
                let symbols =
                    tqsdk_relay::universe::resolve_static_symbols_with_expression(expression)?;
                config.upstream_tick_charts_for_symbols(symbols.iter().map(String::as_str))?
            } else {
                Vec::new()
            }
        };
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
    let startup_charts = Vec::new();
    eprintln!(
        "{}",
        RelayStartupReport::from_config_and_charts(&config, &startup_charts).log_line()
    );
    let server =
        RelayServer::with_outbound_capacity(engine.clone(), config.outbound_channel_capacity);
    let listener = TcpListener::bind(&config.downstream_listen)
        .await
        .map_err(|err| RelayError::Transport(format!("downstream bind failed: {err}")))?;
    let metrics_listener = TcpListener::bind(&config.metrics_listen)
        .await
        .map_err(|err| RelayError::Transport(format!("metrics bind failed: {err}")))?;
    #[cfg(feature = "history")]
    let history_service = history_config.map(history::spawn).transpose()?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (metrics_shutdown_tx, metrics_shutdown_rx) = oneshot::channel();
    #[cfg(feature = "server")]
    let upstream_shutdown = spawn_configured_upstream_pump(&config, server.clone()).await?;
    #[cfg(not(feature = "server"))]
    let upstream_shutdown = {
        if config.has_upstream_futures_source() {
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
    let result = server.serve_until(listener, shutdown_rx).await;
    #[cfg(feature = "history")]
    if let Some(history_service) = history_service {
        history_service.shutdown()?;
    }
    result
}

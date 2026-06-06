#![cfg_attr(not(test), forbid(unsafe_code))]

use std::time::Duration;

use crate::config::RelayConfig;
use crate::error::RelayError;
use crate::error::RelayResult;
use crate::server::RelayServer;
use crate::upstream::WebSocketUpstreamTickSource;
use tokio::sync::oneshot;

const DEFAULT_UPSTREAM_RETRY_INTERVAL: Duration = Duration::from_secs(5);

pub async fn connect_configured_upstream(
    config: &RelayConfig,
) -> RelayResult<Option<WebSocketUpstreamTickSource>> {
    let Some(chart) = config.upstream_tick_chart()? else {
        return Ok(None);
    };
    WebSocketUpstreamTickSource::connect_with_tick_chart(config.upstream_market_url.clone(), chart)
        .await
        .map(Some)
}

pub async fn spawn_configured_upstream_pump(
    config: &RelayConfig,
    server: RelayServer,
) -> RelayResult<Option<oneshot::Sender<()>>> {
    spawn_configured_upstream_pump_with_retry_interval(
        config,
        server,
        DEFAULT_UPSTREAM_RETRY_INTERVAL,
    )
    .await
}

pub async fn spawn_configured_upstream_pump_with_retry_interval(
    config: &RelayConfig,
    server: RelayServer,
    retry_interval: Duration,
) -> RelayResult<Option<oneshot::Sender<()>>> {
    if config.upstream_tick_chart()?.is_none() {
        return Ok(None);
    }
    let config = config.clone();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        run_upstream_retry_loop(config, server, retry_interval, shutdown_rx).await;
    });
    Ok(Some(shutdown_tx))
}

async fn run_upstream_retry_loop(
    config: RelayConfig,
    server: RelayServer,
    retry_interval: Duration,
    mut shutdown: oneshot::Receiver<()>,
) {
    loop {
        match connect_configured_upstream(&config).await {
            Ok(Some(mut source)) => {
                let (pump_shutdown_tx, pump_shutdown_rx) = oneshot::channel();
                tokio::select! {
                    biased;
                    _ = &mut shutdown => {
                        let _ = pump_shutdown_tx.send(());
                        return;
                    }
                    result = server.pump_upstream_until(&mut source, pump_shutdown_rx) => {
                        if let Err(err) = result {
                            mark_upstream_degraded(&server);
                            eprintln!("{err}");
                        }
                    }
                }
            }
            Ok(None) => return,
            Err(RelayError::Transport(message)) => {
                mark_upstream_degraded(&server);
                eprintln!("relay upstream unavailable: {message}");
            }
            Err(err) => {
                mark_upstream_degraded(&server);
                eprintln!("{err}");
                return;
            }
        }
        tokio::select! {
            biased;
            _ = &mut shutdown => return,
            () = tokio::time::sleep(retry_interval) => {}
        }
    }
}

fn mark_upstream_degraded(server: &RelayServer) {
    let engine = server.engine();
    match engine.lock() {
        Ok(mut engine) => engine.mark_upstream_degraded(),
        Err(_) => eprintln!("relay internal error: relay engine lock poisoned"),
    }
}

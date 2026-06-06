#![cfg_attr(not(test), forbid(unsafe_code))]

use crate::config::RelayConfig;
use crate::error::RelayResult;
use crate::server::RelayServer;
use crate::upstream::WebSocketUpstreamTickSource;
use tokio::sync::oneshot;

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
    let Some(mut source) = connect_configured_upstream(config).await? else {
        return Ok(None);
    };
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        if let Err(err) = server.pump_upstream_until(&mut source, shutdown_rx).await {
            eprintln!("{err}");
        }
    });
    Ok(Some(shutdown_tx))
}

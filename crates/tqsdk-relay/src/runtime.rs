#![cfg_attr(not(test), forbid(unsafe_code))]

use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::RelayConfig;
use crate::error::RelayError;
use crate::error::RelayResult;
use crate::server::RelayServer;
#[cfg(feature = "metadata")]
use crate::universe::SessionFuturesUniverseResolver;
use crate::universe::{FuturesContract, resolve_futures_contracts_with_expression};
use crate::upstream::{UpstreamTickChart, UpstreamTickSource, WebSocketUpstreamTickSource};
use chrono::Timelike;
use tokio::sync::oneshot;

const DEFAULT_UPSTREAM_RETRY_INTERVAL: Duration = Duration::from_secs(5);

pub async fn connect_configured_upstream(
    config: &RelayConfig,
) -> RelayResult<Option<WebSocketUpstreamTickSource>> {
    let charts = configured_upstream_tick_charts(config).await?;
    if charts.is_empty() {
        return Ok(None);
    }
    WebSocketUpstreamTickSource::connect_with_tick_charts(
        config.upstream_market_url.clone(),
        charts,
    )
    .await
    .map(Some)
}

pub async fn resolve_configured_upstream_tick_chart(
    config: &RelayConfig,
) -> RelayResult<Option<crate::upstream::UpstreamTickChart>> {
    let mut charts = configured_upstream_tick_charts(config).await?;
    match charts.len() {
        0 => Ok(None),
        1 => Ok(charts.pop()),
        _ => Err(RelayError::invalid_config(
            "resolve_configured_upstream_tick_chart is only available for a single symbol; use resolve_configured_upstream_tick_charts",
        )),
    }
}

pub async fn resolve_configured_upstream_tick_charts(
    config: &RelayConfig,
) -> RelayResult<Vec<crate::upstream::UpstreamTickChart>> {
    configured_upstream_tick_charts(config).await
}

struct ConfiguredUpstream {
    source: WebSocketUpstreamTickSource,
}

struct ConfiguredTickCharts {
    charts: Vec<UpstreamTickChart>,
    contracts: Vec<FuturesContract>,
    calendar: Option<Vec<tqsdk_core::TradingCalendarDay>>,
}

async fn connect_configured_upstream_for_pump(
    config: &RelayConfig,
    server: &RelayServer,
) -> RelayResult<Option<ConfiguredUpstream>> {
    let configured = match configured_upstream_tick_charts_with_contracts(config).await {
        Ok(configured) if configured.charts.is_empty() => return Ok(None),
        Ok(configured) => configured,
        Err(err) => {
            record_universe_refresh_error(server, err.to_string());
            return Err(err);
        }
    };
    record_universe_refresh_success(
        server,
        config,
        &configured.charts,
        &configured.contracts,
        configured.calendar.as_deref(),
    );
    let mut source = WebSocketUpstreamTickSource::connect_with_tick_charts(
        config.upstream_market_url.clone(),
        configured.charts,
    )
    .await?;
    record_upstream_progress(server, source.take_progress());
    Ok(Some(ConfiguredUpstream { source }))
}

async fn configured_upstream_tick_charts(
    config: &RelayConfig,
) -> RelayResult<Vec<crate::upstream::UpstreamTickChart>> {
    Ok(configured_upstream_tick_charts_with_contracts(config)
        .await?
        .charts)
}

async fn configured_upstream_tick_charts_with_contracts(
    config: &RelayConfig,
) -> RelayResult<ConfiguredTickCharts> {
    if let Some(expression) = config.futures_universe_expression.as_ref() {
        if expression.is_static_symbol_only() {
            let symbols = crate::universe::resolve_static_symbols_with_expression(expression)?;
            let charts =
                config.upstream_tick_charts_for_symbols(symbols.iter().map(String::as_str))?;
            let contracts = crate::universe::static_contracts_with_expression(expression)?;
            return Ok(ConfiguredTickCharts {
                charts,
                contracts,
                calendar: None,
            });
        }
        #[cfg(feature = "metadata")]
        {
            let mut resolver = SessionFuturesUniverseResolver::from_config(config)?;
            let contracts =
                resolve_futures_contracts_with_expression(expression, &mut resolver).await?;
            let charts = config.upstream_tick_charts_for_symbols(
                contracts.iter().map(|contract| contract.symbol.as_str()),
            )?;
            let calendar =
                crate::universe::FuturesUniverseResolver::trading_calendar(&mut resolver)
                    .await
                    .ok();
            return Ok(ConfiguredTickCharts {
                charts,
                contracts,
                calendar,
            });
        }
        #[cfg(not(feature = "metadata"))]
        {
            return Err(RelayError::invalid_config(
                "tqsdk-relay metadata feature is required for dynamic futures universe expression",
            ));
        }
    }
    Ok(ConfiguredTickCharts {
        charts: Vec::new(),
        contracts: Vec::new(),
        calendar: None,
    })
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
    if !config.has_upstream_futures_source() {
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
        match connect_configured_upstream_for_pump(&config, &server).await {
            Ok(Some(mut upstream)) => {
                let (pump_shutdown_tx, pump_shutdown_rx) = oneshot::channel();
                let refresh = tokio::time::sleep(next_universe_refresh_delay(&config));
                tokio::pin!(refresh);
                let reconnect_delay = tokio::select! {
                    biased;
                    _ = &mut shutdown => {
                        let _ = pump_shutdown_tx.send(());
                        return;
                    }
                    () = &mut refresh, if config.refreshes_futures_universe() => {
                        let _ = pump_shutdown_tx.send(());
                        Duration::ZERO
                    }
                    result = server.pump_upstream_until(&mut upstream.source, pump_shutdown_rx) => {
                        if let Err(err) = result {
                            mark_upstream_degraded(&server);
                            eprintln!("{err}");
                        }
                        retry_interval
                    }
                };
                if reconnect_delay == Duration::ZERO {
                    continue;
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

fn record_upstream_progress(
    server: &RelayServer,
    progress: crate::upstream::UpstreamSourceProgress,
) {
    let engine = server.engine();
    match engine.lock() {
        Ok(mut engine) => engine.record_upstream_progress(progress),
        Err(_) => eprintln!("relay internal error: relay engine lock poisoned"),
    }
}

fn mark_upstream_degraded(server: &RelayServer) {
    let engine = server.engine();
    match engine.lock() {
        Ok(mut engine) => engine.mark_upstream_degraded(),
        Err(_) => eprintln!("relay internal error: relay engine lock poisoned"),
    }
}

fn record_universe_refresh_success(
    server: &RelayServer,
    config: &RelayConfig,
    charts: &[UpstreamTickChart],
    contracts: &[FuturesContract],
    calendar: Option<&[tqsdk_core::TradingCalendarDay]>,
) {
    let engine = server.engine();
    let upstream_ins_list_chars = max_ins_list_chars(charts);
    let unix_secs = current_unix_secs();
    match engine.lock() {
        Ok(mut engine) if contracts.is_empty() => {
            engine.record_universe_refresh_success_for_symbols(
                charts.iter().map(UpstreamTickChart::symbol),
                upstream_ins_list_chars,
                config.upstream_ins_list_limits.warn_chars,
                config.upstream_ins_list_limits.max_chars,
                unix_secs,
            );
            if let Some(calendar) = calendar {
                engine.record_trading_calendar(calendar);
            }
        }
        Ok(mut engine) => {
            engine.record_universe_refresh_success_for_contracts(
                contracts,
                upstream_ins_list_chars,
                config.upstream_ins_list_limits.warn_chars,
                config.upstream_ins_list_limits.max_chars,
                unix_secs,
            );
            if let Some(calendar) = calendar {
                engine.record_trading_calendar(calendar);
            }
        }
        Err(_) => eprintln!("relay internal error: relay engine lock poisoned"),
    }
}

fn max_ins_list_chars(charts: &[UpstreamTickChart]) -> usize {
    charts
        .iter()
        .map(UpstreamTickChart::ins_list_chars)
        .max()
        .unwrap_or(0)
}

fn record_universe_refresh_error(server: &RelayServer, message: String) {
    let engine = server.engine();
    match engine.lock() {
        Ok(mut engine) => engine.record_universe_refresh_error(message, current_unix_secs()),
        Err(_) => eprintln!("relay internal error: relay engine lock poisoned"),
    }
}

fn next_universe_refresh_delay(config: &RelayConfig) -> Duration {
    let seconds_after_midnight = chrono::Local::now().num_seconds_from_midnight();
    config
        .futures_universe_refresh
        .delay_from_seconds_after_midnight(seconds_after_midnight)
}

fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

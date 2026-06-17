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
use crate::upstream::{
    UpstreamTickChart, UpstreamTickSource, WebSocketUpstreamTickSource,
    upstream_subscription_ins_list_chars,
};
use chrono::Timelike;
use tokio::sync::oneshot;
use tokio::time::Instant;

const DEFAULT_UPSTREAM_RETRY_INTERVAL: Duration = Duration::from_secs(5);

pub async fn connect_configured_upstream(
    config: &RelayConfig,
) -> RelayResult<Option<WebSocketUpstreamTickSource>> {
    let charts = configured_upstream_tick_charts(config).await?;
    if charts.is_empty() {
        return Ok(None);
    }
    WebSocketUpstreamTickSource::connect_with_quote_symbols(
        config.upstream_market_url.clone(),
        charts.iter().map(UpstreamTickChart::symbol),
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

enum UpstreamPumpExit {
    Shutdown,
    SourceClosed,
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
    let mut source = WebSocketUpstreamTickSource::connect_with_quote_symbols(
        config.upstream_market_url.clone(),
        configured.charts.iter().map(UpstreamTickChart::symbol),
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
                match pump_configured_upstream_until(
                    &config,
                    &server,
                    &mut upstream.source,
                    retry_interval,
                    &mut shutdown,
                )
                .await
                {
                    Ok(UpstreamPumpExit::Shutdown) => return,
                    Ok(UpstreamPumpExit::SourceClosed) => {}
                    Err(err) => {
                        mark_upstream_degraded(&server);
                        eprintln!("{err}");
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

async fn pump_configured_upstream_until(
    config: &RelayConfig,
    server: &RelayServer,
    source: &mut WebSocketUpstreamTickSource,
    retry_interval: Duration,
    shutdown: &mut oneshot::Receiver<()>,
) -> RelayResult<UpstreamPumpExit> {
    let refresh_enabled = config.refreshes_futures_universe();
    let refresh = tokio::time::sleep(next_universe_refresh_delay(config));
    tokio::pin!(refresh);
    server.request_pending_upstream_subscriptions()?;
    loop {
        tokio::select! {
            biased;
            _ = &mut *shutdown => return Ok(UpstreamPumpExit::Shutdown),
            () = &mut refresh, if refresh_enabled => {
                let refreshed = refresh_configured_upstream(config, server, source).await;
                let next_delay = if refreshed {
                    next_universe_refresh_delay(config)
                } else {
                    retry_interval
                };
                refresh.as_mut().reset(Instant::now() + next_delay);
            }
            symbols = server.next_upstream_subscription_symbols() => {
                let Some(symbols) = symbols else {
                    continue;
                };
                subscribe_dynamic_upstream_symbols(config, server, source, symbols).await?;
            }
            update = source.next_update() => {
                let progress = source.take_progress();
                let invalid_rows = source.take_invalid_tick_rows();
                let invalid_rows_by_symbol = source.take_invalid_tick_rows_by_symbol();
                let last_error = source.take_last_invalid_tick_row_error();
                let Some(_dispatched) = server.process_upstream_update(
                    update,
                    progress,
                    invalid_rows,
                    invalid_rows_by_symbol,
                    last_error,
                )? else {
                    return Ok(UpstreamPumpExit::SourceClosed);
                };
            }
        }
    }
}

async fn refresh_configured_upstream(
    config: &RelayConfig,
    server: &RelayServer,
    source: &mut WebSocketUpstreamTickSource,
) -> bool {
    match connect_configured_upstream_for_pump(config, server).await {
        Ok(Some(refreshed)) => {
            *source = refreshed.source;
            true
        }
        Ok(None) => {
            eprintln!("relay upstream refresh returned no charts; keeping existing upstream");
            false
        }
        Err(RelayError::Transport(message)) => {
            eprintln!("relay upstream refresh failed; keeping existing upstream: {message}");
            false
        }
        Err(err) => {
            eprintln!("relay upstream refresh failed; keeping existing upstream: {err}");
            false
        }
    }
}

async fn subscribe_dynamic_upstream_symbols(
    config: &RelayConfig,
    server: &RelayServer,
    source: &mut WebSocketUpstreamTickSource,
    symbols: Vec<String>,
) -> RelayResult<()> {
    let missing_symbols = retain_missing_upstream_symbols(server, symbols)?;
    if missing_symbols.is_empty() {
        return Ok(());
    }
    let charts =
        config.upstream_tick_charts_for_symbols(missing_symbols.iter().map(String::as_str))?;
    if charts.is_empty() {
        return Ok(());
    }
    source.subscribe_tick_charts(&charts).await?;
    record_upstream_progress(server, source.take_progress());
    record_dynamic_upstream_subscription_success(server, &charts);
    Ok(())
}

fn retain_missing_upstream_symbols(
    server: &RelayServer,
    symbols: Vec<String>,
) -> RelayResult<Vec<String>> {
    let engine = server.engine();
    let engine = engine
        .lock()
        .map_err(|_| RelayError::Internal("relay engine lock poisoned".to_string()))?;
    Ok(engine.retain_missing_upstream_subscription_symbols(symbols))
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
    let upstream_ins_list_chars = upstream_subscription_ins_list_chars(charts);
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

fn record_dynamic_upstream_subscription_success(
    server: &RelayServer,
    charts: &[UpstreamTickChart],
) {
    let engine = server.engine();
    let upstream_ins_list_chars = upstream_subscription_ins_list_chars(charts);
    let unix_secs = current_unix_secs();
    match engine.lock() {
        Ok(mut engine) => engine.record_dynamic_upstream_subscription_sent(
            charts.iter().map(UpstreamTickChart::symbol),
            upstream_ins_list_chars,
            unix_secs,
        ),
        Err(_) => eprintln!("relay internal error: relay engine lock poisoned"),
    }
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

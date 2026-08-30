#![cfg_attr(not(test), forbid(unsafe_code))]

use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::{RelayConfig, RelayRuntimeConfig};
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
    connect_configured_upstream_with_runtime_config(&RelayRuntimeConfig::new(config.clone())).await
}

pub async fn connect_configured_upstream_with_runtime_config(
    config: &RelayRuntimeConfig,
) -> RelayResult<Option<WebSocketUpstreamTickSource>> {
    let charts = configured_upstream_tick_charts(config).await?;
    if charts.is_empty() {
        return Ok(None);
    }
    let relay = config.relay_config();
    WebSocketUpstreamTickSource::connect_with_quote_symbols(
        relay.upstream_market_url.clone(),
        charts.iter().map(UpstreamTickChart::symbol),
    )
    .await
    .map(Some)
}

pub async fn resolve_configured_upstream_tick_chart(
    config: &RelayConfig,
) -> RelayResult<Option<crate::upstream::UpstreamTickChart>> {
    resolve_configured_upstream_tick_chart_with_runtime_config(&RelayRuntimeConfig::new(
        config.clone(),
    ))
    .await
}

pub async fn resolve_configured_upstream_tick_chart_with_runtime_config(
    config: &RelayRuntimeConfig,
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
    resolve_configured_upstream_tick_charts_with_runtime_config(&RelayRuntimeConfig::new(
        config.clone(),
    ))
    .await
}

pub async fn resolve_configured_upstream_tick_charts_with_runtime_config(
    config: &RelayRuntimeConfig,
) -> RelayResult<Vec<crate::upstream::UpstreamTickChart>> {
    configured_upstream_tick_charts(config).await
}

struct ConfiguredUpstream {
    source: WebSocketUpstreamTickSource,
    charts: Vec<UpstreamTickChart>,
    contracts: Vec<FuturesContract>,
    calendar: Option<Vec<tqsdk_core::TradingCalendarDay>>,
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
    config: &RelayRuntimeConfig,
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
    let relay = config.relay_config();
    let mut source = match WebSocketUpstreamTickSource::connect_with_quote_symbols(
        relay.upstream_market_url.clone(),
        configured.charts.iter().map(UpstreamTickChart::symbol),
    )
    .await
    {
        Ok(source) => source,
        Err(err) => {
            record_universe_refresh_error(server, err.to_string());
            return Err(err);
        }
    };
    record_upstream_progress(server, source.take_progress());
    Ok(Some(ConfiguredUpstream {
        source,
        charts: configured.charts,
        contracts: configured.contracts,
        calendar: configured.calendar,
    }))
}

fn commit_configured_upstream(
    config: &RelayRuntimeConfig,
    server: &RelayServer,
    upstream: &ConfiguredUpstream,
) {
    record_universe_refresh_success(
        server,
        config.relay_config(),
        &upstream.charts,
        &upstream.contracts,
        upstream.calendar.as_deref(),
    );
}

async fn configured_upstream_tick_charts(
    config: &RelayRuntimeConfig,
) -> RelayResult<Vec<crate::upstream::UpstreamTickChart>> {
    Ok(configured_upstream_tick_charts_with_contracts(config)
        .await?
        .charts)
}

async fn configured_upstream_tick_charts_with_contracts(
    config: &RelayRuntimeConfig,
) -> RelayResult<ConfiguredTickCharts> {
    if config
        .futures_universe_spec()
        .is_some_and(|spec| spec.mode() != tqsdk_data::UniverseMode::Snapshot)
    {
        return Err(RelayError::invalid_config(
            "futures universe spec is a snapshot-only entry point",
        ));
    }
    let expanded_v2 = if config.futures_universe_spec().is_some()
        || !config.futures_universe_symbol_files().is_empty()
    {
        Some(
            tqsdk_data::UniverseInput::new(config.futures_universe_spec().cloned())
                .universe_symbol_files(config.futures_universe_symbol_files().iter().cloned())
                .expand()
                .map_err(|error| RelayError::invalid_config(error.to_string()))?,
        )
    } else {
        None
    };
    let mut contracts_by_symbol = std::collections::BTreeMap::<String, FuturesContract>::new();
    let mut calendar = None;
    let relay = config.relay_config();

    if let Some(expression) = relay.futures_universe_expression.as_ref() {
        if expression.is_static_symbol_only() {
            for contract in crate::universe::static_contracts_with_expression(expression)? {
                contracts_by_symbol.insert(contract.symbol.clone(), contract);
            }
        } else {
            #[cfg(feature = "metadata")]
            {
                let mut resolver = SessionFuturesUniverseResolver::from_config(relay)?;
                for contract in
                    resolve_futures_contracts_with_expression(expression, &mut resolver).await?
                {
                    contracts_by_symbol.insert(contract.symbol.clone(), contract);
                }
                calendar =
                    crate::universe::FuturesUniverseResolver::trading_calendar(&mut resolver)
                        .await
                        .ok();
            }
            #[cfg(not(feature = "metadata"))]
            {
                return Err(RelayError::invalid_config(
                    "tqsdk-relay metadata feature is required for dynamic futures universe expression",
                ));
            }
        }
    }

    if let Some(input) = expanded_v2.as_ref() {
        let requires_provider = input.spec().is_some_and(|spec| {
            spec.includes()
                .iter()
                .any(|selector| selector.view() != tqsdk_data::UniverseView::Symbol)
        });
        if requires_provider {
            #[cfg(feature = "metadata")]
            {
                let mut resolver = SessionFuturesUniverseResolver::from_config(relay)?;
                let (_, contracts) =
                    crate::universe::resolve_futures_universe_v2(input, &mut resolver).await?;
                for contract in contracts {
                    contracts_by_symbol.insert(contract.symbol.clone(), contract);
                }
                if calendar.is_none() {
                    calendar =
                        crate::universe::FuturesUniverseResolver::trading_calendar(&mut resolver)
                            .await
                            .ok();
                }
            }
            #[cfg(not(feature = "metadata"))]
            {
                return Err(RelayError::invalid_config(
                    "tqsdk-relay metadata feature is required for dynamic Universe V2 views",
                ));
            }
        } else {
            let compiled = tqsdk_data::compile_static_futures_universe_v2(input)?;
            for candidate in compiled.candidates() {
                let contract =
                    crate::universe::contract_from_configured_symbol(candidate.symbol())?;
                contracts_by_symbol.insert(contract.symbol.clone(), contract);
            }
        }
    }

    let contracts = contracts_by_symbol.into_values().collect::<Vec<_>>();
    let charts = relay.upstream_tick_charts_for_symbols(
        contracts.iter().map(|contract| contract.symbol.as_str()),
    )?;
    Ok(ConfiguredTickCharts {
        charts,
        contracts,
        calendar,
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
    spawn_configured_upstream_pump_with_runtime_config_and_retry_interval(
        &RelayRuntimeConfig::new(config.clone()),
        server,
        retry_interval,
    )
    .await
}

pub async fn spawn_configured_upstream_pump_with_runtime_config(
    config: &RelayRuntimeConfig,
    server: RelayServer,
) -> RelayResult<Option<oneshot::Sender<()>>> {
    spawn_configured_upstream_pump_with_runtime_config_and_retry_interval(
        config,
        server,
        DEFAULT_UPSTREAM_RETRY_INTERVAL,
    )
    .await
}

pub async fn spawn_configured_upstream_pump_with_runtime_config_and_retry_interval(
    config: &RelayRuntimeConfig,
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
    config: RelayRuntimeConfig,
    server: RelayServer,
    retry_interval: Duration,
    mut shutdown: oneshot::Receiver<()>,
) {
    loop {
        match connect_configured_upstream_for_pump(&config, &server).await {
            Ok(Some(mut upstream)) => {
                commit_configured_upstream(&config, &server, &upstream);
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
    config: &RelayRuntimeConfig,
    server: &RelayServer,
    source: &mut WebSocketUpstreamTickSource,
    retry_interval: Duration,
    shutdown: &mut oneshot::Receiver<()>,
) -> RelayResult<UpstreamPumpExit> {
    let refresh_enabled = config.refreshes_futures_universe();
    let refresh = tokio::time::sleep(next_universe_refresh_delay(config.relay_config()));
    tokio::pin!(refresh);
    server.request_pending_upstream_subscriptions()?;
    loop {
        tokio::select! {
            biased;
            _ = &mut *shutdown => return Ok(UpstreamPumpExit::Shutdown),
            () = &mut refresh, if refresh_enabled => {
                let refreshed = refresh_configured_upstream(config, server, source).await;
                let next_delay = if refreshed {
                    next_universe_refresh_delay(config.relay_config())
                } else {
                    retry_interval
                };
                refresh.as_mut().reset(Instant::now() + next_delay);
            }
            symbols = server.next_upstream_subscription_symbols() => {
                let Some(symbols) = symbols else {
                    continue;
                };
                subscribe_dynamic_upstream_symbols(
                    config.relay_config(),
                    server,
                    source,
                    symbols,
                )
                .await?;
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
    config: &RelayRuntimeConfig,
    server: &RelayServer,
    source: &mut WebSocketUpstreamTickSource,
) -> bool {
    match connect_configured_upstream_for_pump(config, server).await {
        Ok(Some(refreshed)) => {
            let ConfiguredUpstream {
                source: replacement,
                charts,
                contracts,
                calendar,
            } = refreshed;
            *source = replacement;
            record_universe_refresh_success(
                server,
                config.relay_config(),
                &charts,
                &contracts,
                calendar.as_deref(),
            );
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::engine::RelayEngine;

    #[tokio::test]
    async fn failed_replacement_connect_keeps_the_last_committed_universe() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let unavailable_addr = listener.local_addr().unwrap();
        drop(listener);
        let unique_suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let universe_file = std::env::temp_dir().join(format!(
            "tqsdk-relay-valid-replacement-{}-{unique_suffix}.txt",
            std::process::id()
        ));
        fs::write(&universe_file, "SHFE.au2606\nDCE.m2609\n").unwrap();
        let config = RelayRuntimeConfig::new(RelayConfig {
            upstream_market_url: format!("ws://{unavailable_addr}/market"),
            ..RelayConfig::default()
        })
        .universe_symbol_file(universe_file.clone());
        let mut initial_engine = RelayEngine::new_memory_only(16, 16);
        initial_engine.record_universe_refresh_success_for_symbols(
            ["SHFE.au2602"],
            "SHFE.au2602".len(),
            None,
            None,
            1,
        );
        let engine = Arc::new(Mutex::new(initial_engine));
        let server = RelayServer::new(engine.clone());

        let error = match connect_configured_upstream_for_pump(&config, &server).await {
            Ok(_) => panic!("replacement connection unexpectedly succeeded"),
            Err(error) => error,
        };
        assert!(matches!(error, RelayError::Transport(_)));
        let metrics = engine.lock().unwrap().metrics_snapshot();
        assert_eq!(metrics.upstream_symbols, 1);
        assert!(metrics.last_universe_refresh_error.is_some());

        fs::remove_file(universe_file).unwrap();
    }
}

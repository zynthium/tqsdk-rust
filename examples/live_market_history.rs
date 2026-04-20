use std::error::Error;
use std::time::{Duration, Instant};

use serde_json::Value;
use tokio::time::timeout;
use tqsdk_runtime_contract::{
    AdapterRegistry, Chart, CommitScope, DefaultRouteConnector, EndpointConfig, InputPayload,
    IoEvent, Kline, MarketChartCommand, MarketCommand, MarketSessionTarget, OutboundFrame,
    PasswordCredentials, ProtocolDomain, Quote, Runtime, RuntimeCommand, RuntimeHandle,
    SessionBootstrap, SessionConfig, SessionRuntime, TqAuthProvider,
};

const MARKET_ROUTE: &str = "market";
const CHART_ID: &str = "live-history-1m";
const DURATION_NS: i64 = 60_000_000_000;
const VIEW_WIDTH: usize = 64;
const INITIAL_TIMEOUT: Duration = Duration::from_secs(30);
const REALTIME_TIMEOUT: Duration = Duration::from_secs(30);
const RECV_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let username = read_env("TQ_AUTH_USER")?;
    let password = read_env("TQ_AUTH_PASS")?;
    let symbol = std::env::var("TQ_TEST_SYMBOL").unwrap_or_else(|_| "SHFE.ao2609".to_string());

    let handle = RuntimeHandle::with_adapters(default_adapters());
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let provider = TqAuthProvider::new(PasswordCredentials::new(username, password));
    let connector = DefaultRouteConnector::default();
    let adapters = default_adapters();
    let config = SessionConfig::new(EndpointConfig::from_env())
        .with_market_target(MarketSessionTarget::new(false, false))
        .enable_domain(ProtocolDomain::Market);

    let mut run = runtime
        .establish(&provider, &provider, &connector, &config, &adapters)
        .await?;

    println!("symbol={symbol}");
    println!(
        "routes={:?}",
        run.connected
            .routes
            .iter()
            .map(|route| route.route.label.as_str())
            .collect::<Vec<_>>()
    );
    run.connected
        .send_route_frame(
            MARKET_ROUTE,
            OutboundFrame::Text(r#"{"aid":"peek_message"}"#.to_string()),
        )
        .await?;

    let quote_command = handle
        .submit(RuntimeCommand::Market(MarketCommand::SubscribeQuotes {
            symbols: vec![tqsdk_runtime_contract::Symbol::new(symbol.clone())],
        }))
        .await?;

    let receipts = runtime.flush_outbound(&mut run).await?;
    println!("dispatch_receipts={:?}", receipts);

    let quote_ready = wait_for_quote_state(&handle, &mut run, &symbol, &[quote_command]).await?;
    print_snapshot("quote-ready", &quote_ready);

    let chart_command = handle
        .submit(RuntimeCommand::Market(MarketCommand::SetChart(
            MarketChartCommand {
                chart_id: CHART_ID.to_string(),
                symbols: vec![tqsdk_runtime_contract::Symbol::new(symbol.clone())],
                duration_ns: DURATION_NS,
                view_width: VIEW_WIDTH,
                left_kline_id: None,
                focus_datetime_ns: None,
                focus_position: None,
            },
        )))
        .await?;

    let receipts = runtime.flush_outbound(&mut run).await?;
    println!("dispatch_receipts={:?}", receipts);

    let initial =
        wait_for_history_state(&handle, &mut run, &symbol, &[quote_command, chart_command]).await?;
    print_snapshot("history-ready", &initial);

    let updated = wait_for_realtime_quote_update(
        &handle,
        &mut run,
        &symbol,
        &[quote_command, chart_command],
        &initial,
    )
    .await?;
    print_snapshot("realtime-update", &updated);

    Ok(())
}

fn default_adapters() -> AdapterRegistry {
    let mut registry = AdapterRegistry::new();
    registry.register_default_adapters();
    registry
}

async fn wait_for_quote_state(
    handle: &RuntimeHandle,
    run: &mut tqsdk_runtime_contract::SessionRun,
    symbol: &str,
    caused_by: &[tqsdk_runtime_contract::CommandId],
) -> Result<SnapshotSummary, Box<dyn Error>> {
    let deadline = Instant::now() + INITIAL_TIMEOUT;
    let mut diagnostics = Vec::new();

    loop {
        let summary = snapshot_summary(handle, symbol)?;
        if summary.quote.is_some() {
            return Ok(summary);
        }

        if Instant::now() >= deadline {
            let state_debug = state_debug(handle, symbol);
            return Err(format!(
                "initial history/quote snapshot not ready within {:?}; diagnostics={diagnostics:?}; state={state_debug}",
                INITIAL_TIMEOUT,
            )
            .into());
        }

        if let Some(diag) = pump_market_once(handle, run, caused_by).await? {
            push_diagnostic(&mut diagnostics, diag);
        }
    }
}

async fn wait_for_history_state(
    handle: &RuntimeHandle,
    run: &mut tqsdk_runtime_contract::SessionRun,
    symbol: &str,
    caused_by: &[tqsdk_runtime_contract::CommandId],
) -> Result<SnapshotSummary, Box<dyn Error>> {
    let deadline = Instant::now() + INITIAL_TIMEOUT;
    let mut diagnostics = Vec::new();

    loop {
        let summary = snapshot_summary(handle, symbol)?;
        if summary.has_history() && summary.quote.is_some() {
            return Ok(summary);
        }

        if Instant::now() >= deadline {
            let state_debug = state_debug(handle, symbol);
            return Err(format!(
                "initial history/quote snapshot not ready within {:?}; diagnostics={diagnostics:?}; state={state_debug}",
                INITIAL_TIMEOUT,
            )
            .into());
        }

        if let Some(diag) = pump_market_once(handle, run, caused_by).await? {
            push_diagnostic(&mut diagnostics, diag);
        }
    }
}

async fn wait_for_realtime_quote_update(
    handle: &RuntimeHandle,
    run: &mut tqsdk_runtime_contract::SessionRun,
    symbol: &str,
    caused_by: &[tqsdk_runtime_contract::CommandId],
    baseline: &SnapshotSummary,
) -> Result<SnapshotSummary, Box<dyn Error>> {
    let deadline = Instant::now() + REALTIME_TIMEOUT;
    let mut diagnostics = Vec::new();

    loop {
        let summary = snapshot_summary(handle, symbol)?;
        if summary.quote_changed_from(baseline) {
            return Ok(summary);
        }

        if Instant::now() >= deadline {
            return Err(format!(
                "no realtime quote change observed within {:?}; baseline={baseline:?}; diagnostics={diagnostics:?}",
                REALTIME_TIMEOUT
            )
            .into());
        }

        if let Some(diag) = pump_market_once(handle, run, caused_by).await? {
            push_diagnostic(&mut diagnostics, diag);
        }
    }
}

async fn pump_market_once(
    handle: &RuntimeHandle,
    run: &mut tqsdk_runtime_contract::SessionRun,
    caused_by: &[tqsdk_runtime_contract::CommandId],
) -> Result<Option<String>, Box<dyn Error>> {
    let recv = timeout(RECV_TIMEOUT, run.connected.recv_route_input(MARKET_ROUTE)).await;
    match recv {
        Ok(Ok(Some(input))) => {
            let diagnostic = describe_runtime_input(&input);
            let _ = handle.ingest(input, caused_by.to_vec(), CommitScope::RealtimeUpdate)?;
            run.connected
                .send_route_frame(
                    MARKET_ROUTE,
                    OutboundFrame::Text(r#"{"aid":"peek_message"}"#.to_string()),
                )
                .await?;
            Ok(Some(diagnostic))
        }
        Ok(Ok(None)) => Ok(None),
        Ok(Err(err)) => Err(Box::new(err)),
        Err(_) => {
            run.connected
                .send_route_frame(
                    MARKET_ROUTE,
                    OutboundFrame::Text(r#"{"aid":"peek_message"}"#.to_string()),
                )
                .await?;
            Ok(Some(format!("timeout:{:?}", RECV_TIMEOUT)))
        }
    }
}

#[derive(Debug, Clone)]
struct SnapshotSummary {
    revision: u64,
    quote: Option<Quote>,
    chart: Option<Chart>,
    kline_tail: Vec<KlinePoint>,
}

impl SnapshotSummary {
    fn has_history(&self) -> bool {
        self.chart
            .as_ref()
            .map(|chart| chart.right_id >= 0 && !self.kline_tail.is_empty())
            .unwrap_or(false)
    }

    fn quote_changed_from(&self, baseline: &Self) -> bool {
        match (&self.quote, &baseline.quote) {
            (Some(current), Some(previous)) => {
                current.datetime != previous.datetime
                    || current.volume != previous.volume
                    || current.open_interest != previous.open_interest
                    || !same_f64(current.last_price, previous.last_price)
                    || !same_f64(current.ask_price1, previous.ask_price1)
                    || !same_f64(current.bid_price1, previous.bid_price1)
            }
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
struct KlinePoint {
    bar_id: i64,
    datetime: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: i64,
    open_oi: i64,
    close_oi: i64,
}

fn snapshot_summary(
    handle: &RuntimeHandle,
    symbol: &str,
) -> Result<SnapshotSummary, Box<dyn Error>> {
    let reader = handle.reader();
    let guard = reader.read();
    let duration_segment = DURATION_NS.to_string();
    let quote = guard.decode_path::<Quote>(&["quotes", symbol])?;
    let chart = guard.decode_path::<Chart>(&["charts", CHART_ID])?;

    let mut kline_ids = guard
        .get_path(&["klines", symbol, duration_segment.as_str(), "data"])
        .and_then(Value::as_object)
        .map(|bars| {
            bars.keys()
                .filter_map(|bar_id| bar_id.parse::<i64>().ok())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    kline_ids.sort_unstable();

    let mut kline_tail = Vec::new();
    for bar_id in kline_ids
        .into_iter()
        .rev()
        .take(5)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
    {
        let bar_segment = bar_id.to_string();
        if let Some(kline) = guard.decode_path::<Kline>(&[
            "klines",
            symbol,
            duration_segment.as_str(),
            "data",
            bar_segment.as_str(),
        ])? {
            kline_tail.push(KlinePoint {
                bar_id,
                datetime: kline.datetime,
                open: kline.open,
                high: kline.high,
                low: kline.low,
                close: kline.close,
                volume: kline.volume,
                open_oi: kline.open_oi,
                close_oi: kline.close_oi,
            });
        }
    }

    Ok(SnapshotSummary {
        revision: guard.revision().get(),
        quote,
        chart,
        kline_tail,
    })
}

fn print_snapshot(label: &str, summary: &SnapshotSummary) {
    println!("== {label} ==");
    println!("revision={}", summary.revision);

    match &summary.quote {
        Some(quote) => {
            println!(
                "quote datetime={} last_price={} ask1={} bid1={} volume={} open_interest={}",
                quote.datetime,
                quote.last_price,
                quote.ask_price1,
                quote.bid_price1,
                quote.volume,
                quote.open_interest
            );
        }
        None => println!("quote <missing>"),
    }

    match &summary.chart {
        Some(chart) => {
            println!(
                "chart left_id={} right_id={} more_data={} ready={} tail_bars={}",
                chart.left_id,
                chart.right_id,
                chart.more_data,
                chart.ready,
                summary.kline_tail.len()
            );
        }
        None => println!("chart <missing>"),
    }

    for bar in &summary.kline_tail {
        println!(
            "kline id={} datetime={} o={} h={} l={} c={} volume={} open_oi={} close_oi={}",
            bar.bar_id,
            bar.datetime,
            bar.open,
            bar.high,
            bar.low,
            bar.close,
            bar.volume,
            bar.open_oi,
            bar.close_oi
        );
    }
}

fn describe_runtime_input(input: &tqsdk_runtime_contract::RuntimeInput) -> String {
    match input {
        tqsdk_runtime_contract::RuntimeInput::Io(IoEvent {
            route,
            payload: InputPayload::Json(value),
            ..
        }) => {
            let aid = value
                .get("aid")
                .and_then(Value::as_str)
                .unwrap_or("<none>")
                .to_string();
            let keys = value
                .as_object()
                .map(|object| object.keys().cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            let data_roots = value
                .get("data")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(Value::as_object)
                        .map(|item| item.keys().cloned().collect::<Vec<_>>())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            format!("io:{route}:aid={aid}:keys={keys:?}:data_roots={data_roots:?}")
        }
        tqsdk_runtime_contract::RuntimeInput::Io(IoEvent {
            route,
            payload: InputPayload::Text(text),
            ..
        }) => format!("io:{route}:text={text}"),
        tqsdk_runtime_contract::RuntimeInput::Io(IoEvent {
            route,
            payload: InputPayload::Binary(bytes),
            ..
        }) => format!("io:{route}:binary={}bytes", bytes.len()),
        tqsdk_runtime_contract::RuntimeInput::Internal(event) => {
            format!("internal:{}", event.label)
        }
        tqsdk_runtime_contract::RuntimeInput::Auth(event) => format!("auth:{}", event.label),
        tqsdk_runtime_contract::RuntimeInput::Replay(event) => format!("replay:{}", event.label),
        tqsdk_runtime_contract::RuntimeInput::Timer(event) => format!("timer:{}", event.label),
    }
}

fn same_f64(left: f64, right: f64) -> bool {
    left.to_bits() == right.to_bits()
}

fn state_debug(handle: &RuntimeHandle, symbol: &str) -> String {
    let reader = handle.reader();
    let guard = reader.read();
    let chart_keys = guard
        .get_path(&["charts"])
        .and_then(Value::as_object)
        .map(|charts| charts.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    let kline_symbols = guard
        .get_path(&["klines"])
        .and_then(Value::as_object)
        .map(|symbols| symbols.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    let symbol_durations = guard
        .get_path(&["klines", symbol])
        .and_then(Value::as_object)
        .map(|durations| durations.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    let duration_segment = DURATION_NS.to_string();
    let bar_ids = guard
        .get_path(&["klines", symbol, duration_segment.as_str(), "data"])
        .and_then(Value::as_object)
        .map(|bars| bars.keys().take(8).cloned().collect::<Vec<_>>())
        .unwrap_or_default();

    format!(
        "chart_keys={chart_keys:?}, kline_symbols={kline_symbols:?}, symbol_durations={symbol_durations:?}, sample_bar_ids={bar_ids:?}"
    )
}

fn push_diagnostic(diagnostics: &mut Vec<String>, diagnostic: String) {
    const MAX_DIAGNOSTICS: usize = 12;

    if diagnostics.len() == MAX_DIAGNOSTICS {
        diagnostics.remove(0);
    }
    diagnostics.push(diagnostic);
}

fn read_env(name: &str) -> Result<String, Box<dyn Error>> {
    let value = std::env::var(name)?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(format!("{name} is empty").into());
    }
    Ok(trimmed.to_string())
}

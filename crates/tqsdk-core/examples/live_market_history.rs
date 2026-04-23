use std::error::Error;
use std::time::{Duration, Instant};

use serde_json::Value;
use tokio::time::timeout;
use tqsdk_core::{
    AdapterRegistry, Chart, CommandId, CommitResult, CommitScope, DefaultRouteConnector,
    EndpointConfig, InputPayload, IoEvent, Kline, MarketChartCommand, MarketCommand,
    MarketSessionTarget, OutboundFrame, PasswordCredentials, ProtocolDomain, Quote, Runtime,
    RuntimeCommand, RuntimeHandle, RuntimeReader, SessionBootstrap, SessionConfig, SessionRun,
    SessionRuntime, TqAuthProvider, UpdateCursor,
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
        .with_market_target(MarketSessionTarget::futures_live())
        .enable_domain(ProtocolDomain::Market);

    let run = runtime
        .establish(&provider, &provider, &connector, &config, &adapters)
        .await?;
    let mut api = ExampleWaitApi::new(handle, runtime, run);

    println!("symbol={symbol}");
    println!(
        "routes={:?}",
        api.run
            .connected
            .routes
            .iter()
            .map(|route| route.route.label.as_str())
            .collect::<Vec<_>>()
    );
    api.prime_market().await?;

    let quote_command = api
        .submit(RuntimeCommand::Market(MarketCommand::SubscribeQuotes {
            symbols: vec![tqsdk_core::Symbol::new(symbol.clone())],
        }))
        .await?;
    println!("quote_command={}", quote_command.get());

    let quote_ready = wait_for_quote_state(&mut api, &symbol).await?;
    print_observation("quote-ready", &quote_ready);

    let chart_command = api
        .submit(RuntimeCommand::Market(MarketCommand::SetChart(
            MarketChartCommand {
                chart_id: CHART_ID.to_string(),
                symbols: vec![tqsdk_core::Symbol::new(symbol.clone())],
                duration_ns: DURATION_NS,
                view_width: VIEW_WIDTH,
                left_kline_id: None,
                focus_datetime_ns: None,
                focus_position: None,
            },
        )))
        .await?;
    println!("chart_command={}", chart_command.get());

    let history_ready = wait_for_history_state(&mut api, &symbol).await?;
    print_observation("history-ready", &history_ready);

    let realtime_update =
        wait_for_realtime_quote_update(&mut api, &symbol, &history_ready.summary).await?;
    print_observation("realtime-update", &realtime_update);

    Ok(())
}

fn default_adapters() -> AdapterRegistry {
    let mut registry = AdapterRegistry::new();
    registry.register_default_adapters();
    registry
}

struct ExampleWaitApi {
    handle: RuntimeHandle,
    reader: RuntimeReader,
    cursor: UpdateCursor,
    runtime: SessionRuntime,
    run: SessionRun,
    active_commands: Vec<CommandId>,
    last_commit: Option<CommitResult>,
    last_diagnostic: Option<String>,
}

impl ExampleWaitApi {
    fn new(handle: RuntimeHandle, runtime: SessionRuntime, run: SessionRun) -> Self {
        let reader = handle.reader();
        let cursor = reader.cursor();
        Self {
            handle,
            reader,
            cursor,
            runtime,
            run,
            active_commands: Vec::new(),
            last_commit: None,
            last_diagnostic: None,
        }
    }

    async fn prime_market(&mut self) -> Result<(), Box<dyn Error>> {
        self.run
            .connected
            .send_route_frame(
                MARKET_ROUTE,
                OutboundFrame::Text(r#"{"aid":"peek_message"}"#.to_string()),
            )
            .await?;
        Ok(())
    }

    async fn submit(&mut self, command: RuntimeCommand) -> Result<CommandId, Box<dyn Error>> {
        let command_id = self.handle.submit(command).await?;
        push_command_id(&mut self.active_commands, command_id);
        Ok(command_id)
    }

    async fn wait_update(&mut self, timeout_window: Duration) -> Result<bool, Box<dyn Error>> {
        self.last_commit = None;
        self.last_diagnostic = None;

        let deadline = Instant::now() + timeout_window;
        loop {
            if self.capture_next_commit() {
                return Ok(true);
            }

            let receipts = self.runtime.flush_outbound(&mut self.run).await?;
            for receipt in receipts {
                push_command_id(&mut self.active_commands, receipt.command_id);
            }

            if self.capture_next_commit() {
                return Ok(true);
            }

            let now = Instant::now();
            if now >= deadline {
                return Ok(false);
            }

            let recv_budget = deadline.saturating_duration_since(now).min(RECV_TIMEOUT);
            match timeout(
                recv_budget,
                self.run.connected.recv_route_input(MARKET_ROUTE),
            )
            .await
            {
                Ok(Ok(Some(input))) => {
                    self.last_diagnostic = Some(describe_runtime_input(&input));
                    let _ = self.handle.ingest(
                        input,
                        self.active_commands.clone(),
                        CommitScope::RealtimeUpdate,
                    )?;
                    self.run
                        .connected
                        .send_route_frame(
                            MARKET_ROUTE,
                            OutboundFrame::Text(r#"{"aid":"peek_message"}"#.to_string()),
                        )
                        .await?;

                    if self.capture_next_commit() {
                        return Ok(true);
                    }
                }
                Ok(Ok(None)) => {}
                Ok(Err(err)) => return Err(Box::new(err)),
                Err(_) => {
                    self.last_diagnostic = Some(format!("timeout:{recv_budget:?}"));
                    self.run
                        .connected
                        .send_route_frame(
                            MARKET_ROUTE,
                            OutboundFrame::Text(r#"{"aid":"peek_message"}"#.to_string()),
                        )
                        .await?;
                }
            }
        }
    }

    fn capture_next_commit(&mut self) -> bool {
        let Some(commit) = self.reader.next(&mut self.cursor) else {
            return false;
        };
        self.last_commit = Some(commit);
        true
    }

    fn reader(&self) -> &RuntimeReader {
        &self.reader
    }

    fn last_commit(&self) -> Option<&CommitResult> {
        self.last_commit.as_ref()
    }

    fn last_diagnostic(&self) -> Option<&str> {
        self.last_diagnostic.as_deref()
    }
}

#[derive(Debug, Clone)]
struct WaitUpdateObservation {
    commit: CommitResult,
    diagnostic: Option<String>,
    summary: SnapshotSummary,
}

async fn wait_for_quote_state(
    api: &mut ExampleWaitApi,
    symbol: &str,
) -> Result<WaitUpdateObservation, Box<dyn Error>> {
    wait_for_summary(
        api,
        symbol,
        INITIAL_TIMEOUT,
        |summary| summary.quote.is_some(),
        "quote snapshot not ready",
    )
    .await
}

async fn wait_for_history_state(
    api: &mut ExampleWaitApi,
    symbol: &str,
) -> Result<WaitUpdateObservation, Box<dyn Error>> {
    wait_for_summary(
        api,
        symbol,
        INITIAL_TIMEOUT,
        |summary| summary.has_history() && summary.quote.is_some(),
        "history snapshot not ready",
    )
    .await
}

async fn wait_for_realtime_quote_update(
    api: &mut ExampleWaitApi,
    symbol: &str,
    baseline: &SnapshotSummary,
) -> Result<WaitUpdateObservation, Box<dyn Error>> {
    wait_for_summary(
        api,
        symbol,
        REALTIME_TIMEOUT,
        |summary| summary.quote_changed_from(baseline),
        "realtime quote did not change",
    )
    .await
}

async fn wait_for_summary<F>(
    api: &mut ExampleWaitApi,
    symbol: &str,
    timeout_window: Duration,
    mut predicate: F,
    timeout_reason: &str,
) -> Result<WaitUpdateObservation, Box<dyn Error>>
where
    F: FnMut(&SnapshotSummary) -> bool,
{
    let deadline = Instant::now() + timeout_window;
    let mut diagnostics = Vec::new();

    loop {
        let now = Instant::now();
        if now >= deadline {
            let state_debug = state_debug(api.reader(), symbol);
            return Err(format!(
                "{timeout_reason} within {:?}; diagnostics={diagnostics:?}; state={state_debug}",
                timeout_window
            )
            .into());
        }

        let remaining = deadline.saturating_duration_since(now);
        if !api.wait_update(remaining).await? {
            let state_debug = state_debug(api.reader(), symbol);
            return Err(format!(
                "{timeout_reason} within {:?}; diagnostics={diagnostics:?}; state={state_debug}",
                timeout_window
            )
            .into());
        }

        let observation = current_observation(api, symbol)?;
        push_diagnostic(&mut diagnostics, observation_diagnostic(&observation));
        if predicate(&observation.summary) {
            return Ok(observation);
        }
    }
}

fn current_observation(
    api: &ExampleWaitApi,
    symbol: &str,
) -> Result<WaitUpdateObservation, Box<dyn Error>> {
    let commit = api
        .last_commit()
        .ok_or_else(|| "wait_update completed without a commit".to_string())?
        .clone();
    let diagnostic = api.last_diagnostic().map(str::to_owned);
    let summary = snapshot_summary(api.reader(), symbol)?;

    Ok(WaitUpdateObservation {
        commit,
        diagnostic,
        summary,
    })
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
    reader: &RuntimeReader,
    symbol: &str,
) -> Result<SnapshotSummary, Box<dyn Error>> {
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

fn print_observation(label: &str, observation: &WaitUpdateObservation) {
    println!("== {label} ==");
    println!(
        "wait_update revision={} scope={:?} caused_by={:?}",
        observation.commit.revision.get(),
        observation.commit.scope,
        observation
            .commit
            .caused_by
            .iter()
            .map(|id| id.get())
            .collect::<Vec<_>>()
    );
    println!(
        "changed_paths={:?}",
        observation
            .commit
            .changes
            .path_hits
            .iter()
            .take(8)
            .map(path_display)
            .collect::<Vec<_>>()
    );
    if let Some(diagnostic) = &observation.diagnostic {
        println!("source={diagnostic}");
    }
    print_snapshot(&observation.summary);
}

fn print_snapshot(summary: &SnapshotSummary) {
    println!("state_revision={}", summary.revision);

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

fn observation_diagnostic(observation: &WaitUpdateObservation) -> String {
    let mut description = format!(
        "revision={} scope={:?} paths={:?}",
        observation.commit.revision.get(),
        observation.commit.scope,
        observation
            .commit
            .changes
            .path_hits
            .iter()
            .take(6)
            .map(path_display)
            .collect::<Vec<_>>()
    );
    if let Some(diagnostic) = &observation.diagnostic {
        description.push_str(" source=");
        description.push_str(diagnostic);
    }
    description
}

fn path_display(path: &tqsdk_core::StatePath) -> String {
    path.segments().join("/")
}

fn describe_runtime_input(input: &tqsdk_core::RuntimeInput) -> String {
    match input {
        tqsdk_core::RuntimeInput::Io(IoEvent {
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
        tqsdk_core::RuntimeInput::Io(IoEvent {
            route,
            payload: InputPayload::Text(text),
            ..
        }) => format!("io:{route}:text={text}"),
        tqsdk_core::RuntimeInput::Io(IoEvent {
            route,
            payload: InputPayload::Binary(bytes),
            ..
        }) => format!("io:{route}:binary={}bytes", bytes.len()),
        tqsdk_core::RuntimeInput::Internal(event) => {
            format!("internal:{}", event.label)
        }
        tqsdk_core::RuntimeInput::Auth(event) => format!("auth:{}", event.label),
        tqsdk_core::RuntimeInput::Replay(event) => format!("replay:{}", event.label),
        tqsdk_core::RuntimeInput::Timer(event) => format!("timer:{}", event.label),
    }
}

fn same_f64(left: f64, right: f64) -> bool {
    left.to_bits() == right.to_bits()
}

fn state_debug(reader: &RuntimeReader, symbol: &str) -> String {
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

fn push_command_id(command_ids: &mut Vec<CommandId>, command_id: CommandId) {
    if !command_ids.contains(&command_id) {
        command_ids.push(command_id);
    }
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

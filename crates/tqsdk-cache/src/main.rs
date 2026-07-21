use std::collections::BTreeSet;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{Days, NaiveDate};
use clap::{Args, Parser, Subcommand, ValueEnum};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tqsdk::{BacktestRemoteFillCancellation, BacktestRemoteFillConfig, RemoteFillPlan, Tq};
use tqsdk_cache::{
    FillReport, FillReportCalendar, FillReportSymbolDayStats, FillSelectorReport,
    REPORT_SCHEMA_VERSION, TradingCalendarSnapshot, TradingDayWindow, default_fill_report_path,
    open_cache, open_read_only_cache, read_fill_report, read_trading_calendar_snapshot,
    write_fill_report, write_trading_calendar_snapshot,
};
use tqsdk_data::{
    DataClient, DataError, HistorySeriesCacheFileStatus, TradingCalendarRow,
    backtest_tick_trading_day_for_timestamp_ns,
};

mod progress;

use progress::{FillProgress, FillProgressSession, ProgressCalendar, ProgressMode};

#[derive(Debug, Parser)]
#[command(
    name = "tqsdk-cache",
    version,
    about = "Manage canonical daily TQBN tick caches"
)]
struct Cli {
    /// Canonical cache root. Defaults to TQSDK_HISTORY_CACHE_DIR or ~/.tqsdk/data_series_1.
    #[arg(long, global = true, value_name = "DIR")]
    cache_dir: Option<PathBuf>,
    /// Pretty-print the versioned JSON result written to stdout.
    #[arg(long, global = true)]
    pretty: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Fast filesystem-only inventory; safe to run while a fill is active.
    Inventory,
    /// Inspect coverage for explicit physical cache symbols.
    Inspect(InspectArgs),
    /// Fill missing closed trading days through the server-side backtest stream.
    Fill(FillArgs),
    /// Verify coverage and optionally replay cached ticks without remote fill.
    Verify(VerifyArgs),
    /// Deep read-only TQBN health diagnostics; requires a stable cache view.
    Doctor,
}

#[derive(Debug, Args)]
struct DaysArgs {
    /// First trading day, inclusive, in YYYY-MM-DD form.
    #[arg(long, value_name = "YYYY-MM-DD")]
    start_day: NaiveDate,
    /// Last trading day, inclusive, in YYYY-MM-DD form.
    #[arg(long, value_name = "YYYY-MM-DD")]
    end_day: NaiveDate,
}

#[derive(Debug, Args)]
struct OptionalDaysArgs {
    #[arg(long, value_name = "YYYY-MM-DD")]
    start_day: Option<NaiveDate>,
    #[arg(long, value_name = "YYYY-MM-DD")]
    end_day: Option<NaiveDate>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum CalendarMode {
    Auto,
    Required,
    Off,
}

impl CalendarMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Required => "required",
            Self::Off => "off",
        }
    }
}

#[derive(Debug, Args)]
struct FillDaysArgs {
    /// First closed trading day, inclusive, in YYYY-MM-DD form.
    #[arg(long, value_name = "YYYY-MM-DD", conflicts_with = "last_trading_days")]
    start_day: Option<NaiveDate>,
    /// Last closed trading day, inclusive, or the anchor for --last-trading-days.
    #[arg(long, value_name = "YYYY-MM-DD")]
    end_day: Option<NaiveDate>,
    /// Resolve the most recent N closed trading days from the generic calendar.
    #[arg(long, value_name = "COUNT", conflicts_with = "start_day")]
    last_trading_days: Option<usize>,
    /// Calendar planning policy. Cache coverage remains the completeness authority.
    #[arg(long, value_enum, default_value_t = CalendarMode::Auto)]
    calendar: CalendarMode,
}

#[derive(Debug, Args)]
struct SymbolsArgs {
    /// Physical cache symbol. Repeat for more symbols.
    #[arg(long = "symbol", value_name = "SYMBOL")]
    symbols: Vec<String>,
}

#[derive(Debug, Args)]
struct InspectArgs {
    #[command(flatten)]
    symbols: SymbolsArgs,
    #[command(flatten)]
    days: DaysArgs,
}

#[derive(Debug, Args)]
struct FillArgs {
    #[command(flatten)]
    symbols: SymbolsArgs,
    /// Futures universe expression resolved by the SDK; may be combined with --symbol.
    #[arg(long, value_name = "EXPRESSION")]
    universe: Option<String>,
    #[command(flatten)]
    days: FillDaysArgs,
    /// Resolve and inspect coverage without acquiring a fill lock or requesting remote ticks.
    #[arg(long)]
    dry_run: bool,
    /// Reserved for a future provisional-current-day implementation; exits with usage status.
    #[arg(long)]
    include_open_day: bool,
    /// Wait this many seconds for an existing fill owner instead of failing immediately.
    #[arg(long, value_name = "SECONDS")]
    lock_wait_secs: Option<u64>,
    /// Override TQSDK_REMOTE_FILL_SYMBOL_BATCH_SIZE for this invocation.
    #[arg(long, value_name = "COUNT")]
    symbol_batch_size: Option<usize>,
    /// Override TQSDK_REMOTE_FILL_SYMBOL_CONCURRENCY for this invocation.
    #[arg(long, value_name = "COUNT")]
    symbol_concurrency: Option<usize>,
    /// Override TQSDK_REMOTE_FILL_IDLE_TIMEOUT_SECS for this invocation.
    #[arg(long, value_name = "SECONDS")]
    idle_timeout_secs: Option<u64>,
    /// Override TQSDK_REMOTE_FILL_BATCH_TIMEOUT_SECS; zero disables the timeout.
    #[arg(long, value_name = "SECONDS")]
    batch_timeout_secs: Option<u64>,
    /// Explicit diagnostic mode: split remote requests at daily partition boundaries.
    #[arg(long)]
    daily_slices: bool,
    /// Report destination. Normal fills otherwise create a report under <cache-dir>/reports/.
    #[arg(long, value_name = "PATH")]
    report: Option<PathBuf>,
    /// Progress rendering mode for this fill only.
    #[arg(long, value_enum, default_value_t = ProgressMode::Auto)]
    progress: ProgressMode,
    /// Maximum active physical-symbol bars in TTY mode; zero keeps only the global bar.
    #[arg(long, value_name = "COUNT", default_value_t = 8)]
    progress_max_bars: usize,
}

#[derive(Debug, Args)]
struct VerifyArgs {
    #[command(flatten)]
    symbols: SymbolsArgs,
    #[command(flatten)]
    days: OptionalDaysArgs,
    /// Reuse a fill report's canonical cache root, range, and physical symbols.
    #[arg(long, value_name = "PATH")]
    report: Option<PathBuf>,
    /// Consume a local backtest replay after coverage verification.
    #[arg(long)]
    replay: bool,
    /// Require at least this many replayed ticks. Requires --replay.
    #[arg(long, value_name = "ROWS")]
    min_rows: Option<u64>,
}

#[derive(Debug)]
enum CliError {
    Usage(String),
    Data(DataError),
    Sdk(tqsdk::Error),
    Io(io::Error),
    Json(serde_json::Error),
}

impl CliError {
    fn exit_code(&self) -> i32 {
        match self {
            Self::Usage(_) => 2,
            Self::Data(DataError::CacheBusy { .. }) => 75,
            Self::Sdk(tqsdk::Error::Data(data))
                if matches!(&**data, DataError::CacheBusy { .. }) =>
            {
                75
            }
            _ => 1,
        }
    }
}

impl std::fmt::Display for CliError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Usage(message) => write!(formatter, "{message}"),
            Self::Data(error) => write!(formatter, "{error}"),
            Self::Sdk(error) => write!(formatter, "{error}"),
            Self::Io(error) => write!(formatter, "{error}"),
            Self::Json(error) => write!(formatter, "{error}"),
        }
    }
}

impl From<DataError> for CliError {
    fn from(value: DataError) -> Self {
        Self::Data(value)
    }
}

impl From<tqsdk::Error> for CliError {
    fn from(value: tqsdk::Error) -> Self {
        Self::Sdk(value)
    }
}

impl From<io::Error> for CliError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for CliError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

struct CommandOutcome {
    value: Value,
    exit_code: i32,
}

#[derive(Clone)]
struct CalendarResolution {
    mode: CalendarMode,
    snapshot: Option<TradingCalendarSnapshot>,
    source: String,
    persist_after_plan: bool,
}

struct ResolvedFillWindow {
    window: TradingDayWindow,
    calendar: CalendarResolution,
}

impl CalendarResolution {
    fn report_calendar(&self) -> FillReportCalendar {
        FillReportCalendar {
            mode: self.mode.as_str().to_string(),
            source: self.source.clone(),
            snapshot: self
                .snapshot
                .as_ref()
                .map(TradingCalendarSnapshot::metadata),
        }
    }

    fn progress_calendar(&self, window: &TradingDayWindow) -> Result<ProgressCalendar, CliError> {
        let start_day = parse_window_day(&window.start_day)?;
        let end_day = parse_window_day(&window.end_day)?;
        let days = match &self.snapshot {
            Some(snapshot) => snapshot.trading_days_between(start_day, end_day)?,
            None => partition_days_between(start_day, end_day)?,
        };
        Ok(ProgressCalendar {
            source: self.source.clone(),
            days,
        })
    }
}

async fn resolve_fill_window(
    cache_dir: &Path,
    args: &FillDaysArgs,
    dry_run: bool,
) -> Result<ResolvedFillWindow, CliError> {
    if args.last_trading_days.is_some() && matches!(args.calendar, CalendarMode::Off) {
        return Err(CliError::Usage(
            "--last-trading-days requires --calendar auto or required".to_string(),
        ));
    }
    if args.start_day.is_some() && args.last_trading_days.is_some() {
        return Err(CliError::Usage(
            "--start-day cannot be combined with --last-trading-days".to_string(),
        ));
    }

    let local_snapshot = if matches!(args.calendar, CalendarMode::Off) {
        None
    } else {
        match read_trading_calendar_snapshot(cache_dir) {
            Ok(snapshot) => snapshot,
            Err(_) if matches!(args.calendar, CalendarMode::Auto) => None,
            Err(error) => return Err(error.into()),
        }
    };
    if let Some(last_trading_days) = args.last_trading_days {
        if last_trading_days == 0 {
            return Err(CliError::Usage(
                "--last-trading-days must be greater than zero".to_string(),
            ));
        }
        let open_day = current_open_trading_day()?;
        let anchor = args.end_day.unwrap_or(open_day);
        let mut snapshot = local_snapshot;
        let mut source = "local".to_string();
        let mut persist_after_plan = false;
        if snapshot.as_ref().is_none_or(|snapshot| {
            eligible_calendar_days(snapshot, anchor, open_day)
                .map(|days| days.len() < last_trading_days)
                .unwrap_or(true)
        }) {
            let lookback_days = last_trading_days
                .checked_mul(4)
                .and_then(|days| days.checked_add(31))
                .ok_or_else(|| CliError::Usage("--last-trading-days is too large".to_string()))?;
            let start = anchor
                .checked_sub_days(Days::new(lookback_days as u64))
                .ok_or_else(|| {
                    CliError::Usage("failed to compute calendar lookback".to_string())
                })?;
            snapshot = Some(fetch_calendar_snapshot(start, anchor).await?);
            source = "remote".to_string();
            persist_after_plan = !dry_run;
        }
        let snapshot = snapshot.ok_or_else(|| {
            CliError::Usage("--last-trading-days requires a trading calendar".to_string())
        })?;
        let eligible = eligible_calendar_days(&snapshot, anchor, open_day)?;
        if eligible.len() < last_trading_days {
            return Err(CliError::Usage(format!(
                "trading calendar contains only {} closed trading days before {}",
                eligible.len(),
                anchor.format("%Y-%m-%d")
            )));
        }
        let selected = &eligible[eligible.len() - last_trading_days..];
        let window = TradingDayWindow::closed_from_days(selected[0], *selected.last().unwrap())?;
        return Ok(ResolvedFillWindow {
            window,
            calendar: CalendarResolution {
                mode: args.calendar,
                snapshot: Some(snapshot),
                source,
                persist_after_plan,
            },
        });
    }

    let start_day = args.start_day.ok_or_else(|| {
        CliError::Usage("fill requires --start-day or --last-trading-days".to_string())
    })?;
    let end_day = args
        .end_day
        .ok_or_else(|| CliError::Usage("fill requires --end-day with --start-day".to_string()))?;
    let window = TradingDayWindow::closed_from_days(start_day, end_day)?;
    let normalized_start = parse_window_day(&window.start_day)?;
    let normalized_end = parse_window_day(&window.end_day)?;
    let mut snapshot = local_snapshot;
    let mut source = if snapshot.is_some() {
        "local".to_string()
    } else if matches!(args.calendar, CalendarMode::Off) {
        "off".to_string()
    } else {
        "partition_fallback".to_string()
    };
    let mut persist_after_plan = false;
    let needs_calendar = snapshot.as_ref().is_some_and(|snapshot| {
        !snapshot
            .covers(normalized_start, normalized_end)
            .unwrap_or(false)
    });
    if matches!(args.calendar, CalendarMode::Required) && (snapshot.is_none() || needs_calendar) {
        snapshot = Some(fetch_calendar_snapshot(normalized_start, normalized_end).await?);
        source = "remote".to_string();
        persist_after_plan = !dry_run;
    } else if needs_calendar {
        snapshot = None;
        source = "partition_fallback".to_string();
    }
    Ok(ResolvedFillWindow {
        window,
        calendar: CalendarResolution {
            mode: args.calendar,
            snapshot,
            source,
            persist_after_plan,
        },
    })
}

async fn finish_calendar_after_plan(
    mut plan_rx: mpsc::Receiver<RemoteFillPlan>,
    cache_dir: PathBuf,
    window: TradingDayWindow,
    mut calendar: CalendarResolution,
    dry_run: bool,
    reporter: FillProgress,
) -> Result<CalendarResolution, CliError> {
    let Some(plan) = plan_rx.recv().await else {
        return Ok(calendar);
    };
    let start_day = parse_window_day(&window.start_day)?;
    let end_day = parse_window_day(&window.end_day)?;
    if calendar.snapshot.is_none()
        && matches!(calendar.mode, CalendarMode::Auto | CalendarMode::Required)
        && plan.requires_remote_fill()
    {
        match fetch_calendar_snapshot(start_day, end_day).await {
            Ok(snapshot) => {
                calendar.snapshot = Some(snapshot);
                calendar.source = "remote".to_string();
                calendar.persist_after_plan = !dry_run;
            }
            Err(error) if matches!(calendar.mode, CalendarMode::Auto) => {
                calendar.source = "partition_fallback".to_string();
                reporter.calendar_unavailable(error.to_string());
            }
            Err(error) => return Err(error),
        }
    }
    if calendar.persist_after_plan && !dry_run {
        if let Some(snapshot) = &calendar.snapshot {
            write_trading_calendar_snapshot(&cache_dir, snapshot)?;
            calendar.persist_after_plan = false;
        }
    }
    reporter.calendar_ready(calendar.progress_calendar(&window)?);
    Ok(calendar)
}

async fn fetch_calendar_snapshot(
    start_day: NaiveDate,
    end_day: NaiveDate,
) -> Result<TradingCalendarSnapshot, CliError> {
    let rows: Vec<TradingCalendarRow> = DataClient::new()
        .query_trading_calendar(start_day, end_day)
        .await?;
    Ok(TradingCalendarSnapshot::from_rows(rows)?)
}

fn eligible_calendar_days(
    snapshot: &TradingCalendarSnapshot,
    anchor: NaiveDate,
    open_day: NaiveDate,
) -> Result<Vec<NaiveDate>, CliError> {
    let mut days = Vec::new();
    for day in &snapshot.days {
        let date = parse_window_day(&day.date)?;
        if day.trading && date <= anchor && date < open_day {
            days.push(date);
        }
    }
    Ok(days)
}

fn parse_window_day(value: &str) -> Result<NaiveDate, CliError> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|error| CliError::Usage(format!("invalid trading day {value:?}: {error}")))
}

fn partition_days_between(
    start_day: NaiveDate,
    end_day: NaiveDate,
) -> Result<Vec<NaiveDate>, CliError> {
    if start_day > end_day {
        return Err(CliError::Usage(
            "start trading day must not be after end trading day".to_string(),
        ));
    }
    let mut days = Vec::new();
    let mut day = start_day;
    while day <= end_day {
        days.push(day);
        day = day
            .succ_opt()
            .ok_or_else(|| CliError::Usage("trading day date overflow".to_string()))?;
    }
    Ok(days)
}

fn current_open_trading_day() -> Result<NaiveDate, CliError> {
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_nanos()).ok())
        .ok_or_else(|| CliError::Usage("system clock is outside TQBN range".to_string()))?;
    Ok(backtest_tick_trading_day_for_timestamp_ns(now_ns)?)
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cli = Cli::parse();
    let pretty = cli.pretty;
    match run(cli).await {
        Ok(outcome) => {
            if let Err(error) = write_output(&outcome.value, pretty) {
                eprintln!("tqsdk-cache: {error}");
                std::process::exit(error.exit_code());
            }
            if outcome.exit_code != 0 {
                std::process::exit(outcome.exit_code);
            }
        }
        Err(error) => {
            eprintln!("tqsdk-cache: {error}");
            std::process::exit(error.exit_code());
        }
    }
}

async fn run(cli: Cli) -> Result<CommandOutcome, CliError> {
    match cli.command {
        Command::Inventory => inventory(cli.cache_dir.as_deref()),
        Command::Inspect(args) => inspect(cli.cache_dir.as_deref(), args),
        Command::Fill(args) => fill(cli.cache_dir.as_deref(), args).await,
        Command::Verify(args) => verify(cli.cache_dir.as_deref(), args).await,
        Command::Doctor => doctor(cli.cache_dir.as_deref()),
    }
}

fn inventory(cache_dir: Option<&Path>) -> Result<CommandOutcome, CliError> {
    let (cache, cache_dir) = open_read_only_cache(cache_dir)?;
    let inventory = cache.fast_inventory()?;
    Ok(CommandOutcome {
        value: json!({
            "schema_version": REPORT_SCHEMA_VERSION,
            "command": "inventory",
            "cache_dir": cache_dir,
            "backend_format": inventory.backend_format,
            "total_files": inventory.total_files,
            "total_bytes": inventory.total_bytes,
            "total_days": inventory.total_days,
            "problem_files": inventory.problem_files,
            "symbols": inventory.symbols.into_iter().map(|symbol| json!({
                "symbol": symbol.symbol,
                "files": symbol.files,
                "bytes": symbol.bytes,
                "days": symbol.days,
                "problem_files": symbol.problem_files,
            })).collect::<Vec<_>>(),
        }),
        exit_code: 0,
    })
}

fn inspect(cache_dir: Option<&Path>, args: InspectArgs) -> Result<CommandOutcome, CliError> {
    let symbols = normalized_symbols(args.symbols.symbols)?;
    let window = TradingDayWindow::from_days(args.days.start_day, args.days.end_day)?;
    let (cache, cache_dir) = open_read_only_cache(cache_dir)?;
    let statuses = symbols
        .iter()
        .map(|symbol| cache.inspect(symbol, window.start_ns, window.end_ns))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(CommandOutcome {
        value: json!({
            "schema_version": REPORT_SCHEMA_VERSION,
            "command": "inspect",
            "cache_dir": cache_dir,
            "requested_days": window,
            "statuses": statuses.iter().map(cache_status_json).collect::<Vec<_>>(),
        }),
        exit_code: 0,
    })
}

async fn fill(cache_dir: Option<&Path>, args: FillArgs) -> Result<CommandOutcome, CliError> {
    let config = fill_config(&args);
    let symbols = normalized_symbols_allow_empty(args.symbols.symbols)?;
    let universe = normalized_universe(args.universe)?;
    if symbols.is_empty() && universe.is_none() {
        return Err(CliError::Usage(
            "fill requires at least one --symbol or --universe expression".to_string(),
        ));
    }
    if args.include_open_day {
        return Err(CliError::Usage(
            "--include-open-day is reserved for a future provisional coverage mode and is not implemented"
                .to_string(),
        ));
    }
    let (cache, canonical_cache_dir) = if args.dry_run {
        open_read_only_cache(cache_dir)?
    } else {
        open_cache(cache_dir)?
    };
    let resolved = resolve_fill_window(&canonical_cache_dir, &args.days, args.dry_run).await?;
    let window = resolved.window;
    let selector = FillSelectorReport {
        symbols: symbols.clone(),
        universe: universe.clone(),
    };
    if args.dry_run {
        if args.report.is_some() {
            return Err(CliError::Usage(
                "--report cannot be used with --dry-run because dry-run writes no files"
                    .to_string(),
            ));
        }
        let builder = apply_fill_targets(
            builder_with_environment_auth(false)?
                .backtest(window.start_ns, window.end_ns)
                .cache_store(cache)
                .cache_only()
                .remote_fill_config(config),
            &symbols,
            universe.as_deref(),
        )?;
        let warmup = builder.warmup().await?;
        let report = fill_report_with_metadata(
            &warmup,
            &canonical_cache_dir,
            window,
            config,
            true,
            selector,
            &resolved.calendar,
        )?;
        let complete = report.complete;
        return Ok(CommandOutcome {
            value: json!({
                "schema_version": REPORT_SCHEMA_VERSION,
                "command": "fill",
                "cache_dir": canonical_cache_dir,
                "dry_run": true,
                "report_path": Value::Null,
                "report": report,
            }),
            exit_code: if complete { 0 } else { 1 },
        });
    }

    let progress_session = FillProgressSession::new(args.progress, args.progress_max_bars);
    let reporter = progress_session.observer();
    reporter.planning("resolving universe and inspecting strict cache coverage");
    if resolved.calendar.snapshot.is_some() {
        reporter.calendar_ready(resolved.calendar.progress_calendar(&window)?);
    }
    let cancellation = BacktestRemoteFillCancellation::new();
    let signal_cancellation = cancellation.clone();
    let signal_task = tokio::spawn(async move {
        wait_for_shutdown_signal(signal_cancellation).await;
    });
    let (plan_tx, plan_rx) = mpsc::channel(1);
    let calendar_task = tokio::spawn(finish_calendar_after_plan(
        plan_rx,
        canonical_cache_dir.clone(),
        window.clone(),
        resolved.calendar.clone(),
        false,
        reporter.clone(),
    ));
    let progress_callback = reporter.clone();
    let telemetry_callback = reporter.clone();
    let mut builder = builder_with_environment_auth(false)?
        .backtest(window.start_ns, window.end_ns)
        .cache_dir(&canonical_cache_dir)?
        .remote_on_miss()
        .remote_fill_config(config)
        .remote_fill_cancellation(cancellation.clone())
        .on_remote_fill_progress(move |event| progress_callback.observe_progress(event))
        .on_remote_fill_telemetry(move |event| {
            telemetry_callback.observe_telemetry(event);
            if let Some(plan) = event.plan() {
                let _ = plan_tx.try_send(plan.clone());
            }
        });
    if let Some(wait_secs) = args.lock_wait_secs {
        builder = builder.remote_fill_lock_wait(Duration::from_secs(wait_secs));
    }
    let builder = apply_fill_targets(builder, &symbols, universe.as_deref())?;
    let warmup = builder.warmup().await;
    signal_task.abort();
    let _ = signal_task.await;
    let calendar = match calendar_task.await {
        Ok(Ok(calendar)) => calendar,
        Ok(Err(error)) => {
            progress_session.finish("fill failed; calendar preparation did not complete");
            return Err(error);
        }
        Err(error) => {
            progress_session.finish("fill failed; calendar planning task did not complete");
            return Err(CliError::Usage(format!(
                "calendar planning task failed: {error}"
            )));
        }
    };

    if cancellation.is_cancelled() {
        let summary = "interrupted; partial accepted rows were flushed without coverage commit";
        progress_session.finish(summary);
        let inventory = cache.fast_inventory()?;
        return Ok(CommandOutcome {
            value: json!({
                "schema_version": REPORT_SCHEMA_VERSION,
                "command": "fill",
                "cache_dir": canonical_cache_dir,
                "status": "interrupted",
                "partial_inventory": fast_inventory_json(&inventory),
            }),
            exit_code: 130,
        });
    }

    let warmup = match warmup {
        Ok(warmup) => warmup,
        Err(error) => {
            progress_session.finish("fill failed; strict local coverage was not committed");
            return Err(error.into());
        }
    };
    let report = fill_report_with_metadata(
        &warmup,
        &canonical_cache_dir,
        window,
        config,
        false,
        selector,
        &calendar,
    )?;
    reporter.final_report(&report);
    progress_session.finish("fill complete; strict local coverage verified");
    let report_path = args
        .report
        .unwrap_or_else(|| default_fill_report_path(&canonical_cache_dir));
    write_fill_report(&report_path, &report)?;
    let complete = report.complete;
    Ok(CommandOutcome {
        value: json!({
            "schema_version": REPORT_SCHEMA_VERSION,
            "command": "fill",
            "cache_dir": canonical_cache_dir,
            "dry_run": false,
            "report_path": report_path,
            "report": report,
        }),
        exit_code: if complete { 0 } else { 1 },
    })
}

fn fill_report_with_metadata(
    warmup: &tqsdk::BacktestCacheWarmupReport,
    cache_dir: &Path,
    window: TradingDayWindow,
    config: BacktestRemoteFillConfig,
    dry_run: bool,
    selector: FillSelectorReport,
    calendar: &CalendarResolution,
) -> Result<FillReport, CliError> {
    let progress_calendar = calendar.progress_calendar(&window)?;
    let day_stats = warmup
        .symbols
        .iter()
        .map(|symbol| fill_symbol_day_stats(symbol, &progress_calendar.days))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(
        FillReport::from_warmup(warmup, cache_dir, window, config, dry_run).with_v2_metadata(
            selector,
            Some(calendar.report_calendar()),
            day_stats,
        ),
    )
}

fn fill_symbol_day_stats(
    symbol: &tqsdk::BacktestCacheWarmupSymbolReport,
    calendar_days: &[NaiveDate],
) -> Result<FillReportSymbolDayStats, CliError> {
    let mut planned_days = 0;
    let mut before_covered_days = 0;
    let mut after_covered_days = 0;
    for day in calendar_days {
        let range = tqsdk_data::backtest_tick_trading_day_range(*day)?;
        if range.start_ns < symbol.after.range_start_ns || range.end_ns > symbol.after.range_end_ns
        {
            continue;
        }
        planned_days += 1;
        if ranges_cover(&symbol.before.cached_ranges, range.start_ns, range.end_ns) {
            before_covered_days += 1;
        }
        if ranges_cover(&symbol.after.cached_ranges, range.start_ns, range.end_ns) {
            after_covered_days += 1;
        }
    }
    Ok(FillReportSymbolDayStats {
        symbol: symbol.symbol.clone(),
        planned_days,
        covered_days: after_covered_days,
        missing_days: planned_days.saturating_sub(after_covered_days),
        received_days: after_covered_days.saturating_sub(before_covered_days),
    })
}

fn ranges_cover(ranges: &[(i64, i64)], start_ns: i64, end_ns: i64) -> bool {
    ranges
        .iter()
        .any(|(cached_start, cached_end)| *cached_start <= start_ns && end_ns <= *cached_end)
}

async fn verify(cache_dir: Option<&Path>, args: VerifyArgs) -> Result<CommandOutcome, CliError> {
    if args.min_rows.is_some() && !args.replay {
        return Err(CliError::Usage("--min-rows requires --replay".to_string()));
    }

    let (cache, canonical_cache_dir, window, symbols, source_report) = match args.report {
        Some(report_path) => {
            if !args.symbols.symbols.is_empty()
                || args.days.start_day.is_some()
                || args.days.end_day.is_some()
            {
                return Err(CliError::Usage(
                    "--report cannot be combined with --symbol, --start-day, or --end-day"
                        .to_string(),
                ));
            }
            let report = read_fill_report(&report_path)?;
            let report_root = PathBuf::from(&report.cache_dir);
            let (cache, canonical_cache_dir) = open_cache(Some(&report_root))?;
            if let Some(requested_cache_dir) = cache_dir {
                let (_, requested_canonical_dir) = open_cache(Some(requested_cache_dir))?;
                if requested_canonical_dir != canonical_cache_dir {
                    return Err(CliError::Usage(format!(
                        "--cache-dir {} does not match report cache root {}",
                        requested_canonical_dir.display(),
                        canonical_cache_dir.display()
                    )));
                }
            }
            let symbols = report.physical_symbols()?;
            (
                cache,
                canonical_cache_dir,
                report.requested_days.clone(),
                symbols,
                Some(report),
            )
        }
        None => {
            let symbols = normalized_symbols(args.symbols.symbols)?;
            let start_day = args.days.start_day.ok_or_else(|| {
                CliError::Usage("verify requires --start-day without --report".to_string())
            })?;
            let end_day = args.days.end_day.ok_or_else(|| {
                CliError::Usage("verify requires --end-day without --report".to_string())
            })?;
            let window = TradingDayWindow::closed_from_days(start_day, end_day)?;
            let (cache, canonical_cache_dir) = open_cache(cache_dir)?;
            (cache, canonical_cache_dir, window, symbols, None)
        }
    };
    let _lock = cache.try_acquire_consistency_read_lock()?;
    let warmup = cache_only_warmup(&canonical_cache_dir, &window, &symbols).await?;
    let coverage_complete = warmup.symbols_missing == 0
        && warmup
            .symbols
            .iter()
            .all(|symbol| symbol.after.is_complete());

    let replay_rows = if args.replay {
        Some(cache_only_replay(&canonical_cache_dir, &window, &symbols).await?)
    } else {
        None
    };
    let replay_ok = args
        .min_rows
        .is_none_or(|minimum| replay_rows.is_some_and(|rows| rows >= minimum));
    Ok(CommandOutcome {
        value: json!({
            "schema_version": REPORT_SCHEMA_VERSION,
            "command": "verify",
            "cache_dir": canonical_cache_dir,
            "requested_days": window,
            "source_report": source_report.as_ref().map(|_| "bound"),
            "symbols": symbols,
            "coverage_complete": coverage_complete,
            "replay_rows": replay_rows,
            "min_rows": args.min_rows,
            "statuses": warmup.symbols.iter().map(|symbol| cache_status_json(&symbol.after)).collect::<Vec<_>>(),
        }),
        exit_code: if coverage_complete && replay_ok { 0 } else { 1 },
    })
}

fn doctor(cache_dir: Option<&Path>) -> Result<CommandOutcome, CliError> {
    let (cache, cache_dir) = open_cache(cache_dir)?;
    let _lock = cache.try_acquire_consistency_read_lock()?;
    let report = cache.diagnose()?;
    let problem_files = report.problem_files;
    Ok(CommandOutcome {
        value: json!({
            "schema_version": REPORT_SCHEMA_VERSION,
            "command": "doctor",
            "cache_dir": cache_dir,
            "backend_format": report.backend_format,
            "problem_files": problem_files,
            "files": report.files.into_iter().map(|file| json!({
                "path": file.path,
                "file_name": file.file_name,
                "trading_day": file.trading_day,
                "symbol": file.symbol,
                "status": file_status_name(file.status),
                "id_range": file.id_range,
                "rows": file.rows,
                "size_bytes": file.size_bytes,
                "schema_version": file.schema_version,
                "error": file.error,
            })).collect::<Vec<_>>(),
        }),
        exit_code: if problem_files == 0 { 0 } else { 1 },
    })
}

fn fill_config(args: &FillArgs) -> BacktestRemoteFillConfig {
    let mut config = BacktestRemoteFillConfig::from_environment();
    if let Some(value) = args.symbol_batch_size {
        config = config.with_symbol_batch_size(value);
    }
    if let Some(value) = args.symbol_concurrency {
        config = config.with_symbol_concurrency(value);
    }
    if let Some(value) = args.idle_timeout_secs {
        config = config.with_idle_timeout(Duration::from_secs(value));
    }
    if let Some(value) = args.batch_timeout_secs {
        config = config.with_batch_timeout((value != 0).then(|| Duration::from_secs(value)));
    }
    if args.daily_slices {
        config = config.with_slice(Some(Duration::from_secs(24 * 60 * 60)));
    }
    config
}

fn apply_fill_targets(
    mut builder: tqsdk::BacktestBuilder,
    symbols: &[String],
    universe: Option<&str>,
) -> Result<tqsdk::BacktestBuilder, CliError> {
    for symbol in symbols {
        builder = builder.symbol(symbol);
    }
    if let Some(universe) = universe {
        builder = builder.universe(universe)?;
    }
    Ok(builder)
}

fn builder_with_environment_auth(require_auth: bool) -> Result<tqsdk::TqBuilder, CliError> {
    let user = std::env::var("TQ_AUTH_USER").ok();
    let pass = std::env::var("TQ_AUTH_PASS").ok();
    match (user, pass) {
        (Some(user), Some(pass)) => Ok(Tq::futures().auth(user, pass)),
        (None, None) if !require_auth => Ok(Tq::futures()),
        (None, None) => Err(CliError::Usage(
            "fill requires TQ_AUTH_USER and TQ_AUTH_PASS".to_string(),
        )),
        _ => Err(CliError::Usage(
            "set both TQ_AUTH_USER and TQ_AUTH_PASS or neither".to_string(),
        )),
    }
}

async fn cache_only_warmup(
    cache_dir: &Path,
    window: &TradingDayWindow,
    symbols: &[String],
) -> Result<tqsdk::BacktestCacheWarmupReport, CliError> {
    let mut builder = Tq::futures()
        .backtest(window.start_ns, window.end_ns)
        .cache_dir(cache_dir)?
        .cache_only();
    for symbol in symbols {
        builder = builder.symbol(symbol);
    }
    Ok(builder.warmup().await?)
}

async fn cache_only_replay(
    cache_dir: &Path,
    window: &TradingDayWindow,
    symbols: &[String],
) -> Result<u64, CliError> {
    let mut builder = Tq::futures()
        .backtest(window.start_ns, window.end_ns)
        .cache_dir(cache_dir)?
        .cache_only();
    for symbol in symbols {
        builder = builder.symbol(symbol).tick(symbol, 1_024);
    }
    let mut tq = builder.connect().await?;
    while tq.next().await? {}
    Ok(tq
        .backtest_summary()
        .map(|summary| summary.tick_count() as u64)
        .unwrap_or_default())
}

fn normalized_symbols(values: Vec<String>) -> Result<Vec<String>, CliError> {
    let symbols = normalized_symbols_allow_empty(values)?;
    if symbols.is_empty() {
        return Err(CliError::Usage(
            "at least one --symbol is required".to_string(),
        ));
    }
    Ok(symbols)
}

fn normalized_symbols_allow_empty(values: Vec<String>) -> Result<Vec<String>, CliError> {
    let mut symbols = BTreeSet::new();
    for value in values {
        let value = value.trim();
        if value.is_empty() {
            return Err(CliError::Usage("--symbol must not be empty".to_string()));
        }
        symbols.insert(value.to_string());
    }
    Ok(symbols.into_iter().collect())
}

fn normalized_universe(value: Option<String>) -> Result<Option<String>, CliError> {
    value
        .map(|value| {
            let value = value.trim();
            if value.is_empty() {
                Err(CliError::Usage("--universe must not be empty".to_string()))
            } else {
                Ok(value.to_string())
            }
        })
        .transpose()
}

fn cache_status_json(status: &tqsdk::BacktestTickCacheStatus) -> Value {
    json!({
        "symbol": status.symbol,
        "backend_format": status.backend_format,
        "series_path": status.series_path,
        "series_path_exists": status.series_path_exists,
        "range_start_ns": status.range_start_ns,
        "range_end_ns": status.range_end_ns,
        "cached_ranges": status.cached_ranges,
        "missing_ranges": status.missing_ranges,
        "complete": status.is_complete(),
    })
}

fn fast_inventory_json(inventory: &tqsdk_data::BacktestTickCacheFastInventory) -> Value {
    json!({
        "total_files": inventory.total_files,
        "total_bytes": inventory.total_bytes,
        "total_days": inventory.total_days,
        "problem_files": inventory.problem_files,
        "symbols": inventory.symbols.iter().map(|symbol| json!({
            "symbol": symbol.symbol,
            "files": symbol.files,
            "bytes": symbol.bytes,
            "days": symbol.days,
            "problem_files": symbol.problem_files,
        })).collect::<Vec<_>>(),
    })
}

fn file_status_name(status: HistorySeriesCacheFileStatus) -> &'static str {
    match status {
        HistorySeriesCacheFileStatus::Readable => "readable",
        HistorySeriesCacheFileStatus::EmptySegment => "empty_segment",
        HistorySeriesCacheFileStatus::InvalidRowWidth => "invalid_row_width",
        HistorySeriesCacheFileStatus::IncompleteWrite => "incomplete_write",
        HistorySeriesCacheFileStatus::Ignored => "ignored",
    }
}

async fn wait_for_shutdown_signal(cancellation: BacktestRemoteFillCancellation) {
    #[cfg(unix)]
    {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {},
                    _ = terminate.recv() => {},
                }
            }
            Err(error) => {
                eprintln!("tqsdk-cache: SIGTERM handler unavailable ({error}); waiting for Ctrl-C");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
    cancellation.cancel();
    eprintln!("tqsdk-cache: cancellation requested; flushing accepted partial tick rows");
}

fn write_output(value: &Value, pretty: bool) -> Result<(), CliError> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    if pretty {
        serde_json::to_writer_pretty(&mut stdout, value)?;
    } else {
        serde_json::to_writer(&mut stdout, value)?;
    }
    stdout.write_all(b"\n")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{CalendarMode, FillDaysArgs, resolve_fill_window};
    use chrono::NaiveDate;
    use tqsdk_cache::{TradingCalendarSnapshot, write_trading_calendar_snapshot};
    use tqsdk_data::TradingCalendarRow;

    #[tokio::test(flavor = "current_thread")]
    async fn explicit_auto_range_uses_partition_fallback_without_a_snapshot() {
        let root = std::env::temp_dir().join(format!(
            "tqsdk-cache-calendar-auto-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let args = FillDaysArgs {
            start_day: Some(NaiveDate::from_ymd_opt(2020, 1, 2).unwrap()),
            end_day: Some(NaiveDate::from_ymd_opt(2020, 1, 3).unwrap()),
            last_trading_days: None,
            calendar: CalendarMode::Auto,
        };

        let resolved = resolve_fill_window(&root, &args, true).await.unwrap();

        assert_eq!(resolved.window.start_day, "2020-01-02");
        assert_eq!(resolved.window.end_day, "2020-01-03");
        assert!(resolved.calendar.snapshot.is_none());
        assert_eq!(resolved.calendar.source, "partition_fallback");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn explicit_auto_range_ignores_an_invalid_local_snapshot() {
        let root = std::env::temp_dir().join(format!(
            "tqsdk-cache-calendar-invalid-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(root.join("meta")).unwrap();
        std::fs::write(root.join("meta/trading-calendar-v1.json"), "invalid").unwrap();
        let args = FillDaysArgs {
            start_day: Some(NaiveDate::from_ymd_opt(2020, 1, 2).unwrap()),
            end_day: Some(NaiveDate::from_ymd_opt(2020, 1, 3).unwrap()),
            last_trading_days: None,
            calendar: CalendarMode::Auto,
        };

        let resolved = resolve_fill_window(&root, &args, true).await.unwrap();

        assert!(resolved.calendar.snapshot.is_none());
        assert_eq!(resolved.calendar.source, "partition_fallback");

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn last_trading_days_is_rejected_when_calendar_is_off() {
        let args = FillDaysArgs {
            start_day: None,
            end_day: None,
            last_trading_days: Some(5),
            calendar: CalendarMode::Off,
        };
        let error =
            match resolve_fill_window(std::path::Path::new("/tmp/unused"), &args, true).await {
                Ok(_) => panic!("calendar-off last-trading-days request should fail"),
                Err(error) => error,
            };

        assert!(error.to_string().contains("--last-trading-days"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn last_trading_days_uses_the_local_calendar_snapshot_without_network() {
        let root = std::env::temp_dir().join(format!(
            "tqsdk-cache-calendar-last-days-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let snapshot = TradingCalendarSnapshot::from_rows(vec![
            TradingCalendarRow {
                date: "2020-01-06".to_string(),
                trading: true,
            },
            TradingCalendarRow {
                date: "2020-01-07".to_string(),
                trading: true,
            },
            TradingCalendarRow {
                date: "2020-01-08".to_string(),
                trading: true,
            },
            TradingCalendarRow {
                date: "2020-01-09".to_string(),
                trading: true,
            },
            TradingCalendarRow {
                date: "2020-01-10".to_string(),
                trading: true,
            },
        ])
        .unwrap();
        write_trading_calendar_snapshot(&root, &snapshot).unwrap();
        let args = FillDaysArgs {
            start_day: None,
            end_day: Some(NaiveDate::from_ymd_opt(2020, 1, 10).unwrap()),
            last_trading_days: Some(2),
            calendar: CalendarMode::Auto,
        };

        let resolved = resolve_fill_window(&root, &args, false).await.unwrap();

        assert_eq!(resolved.window.start_day, "2020-01-09");
        assert_eq!(resolved.window.end_day, "2020-01-10");
        assert_eq!(resolved.calendar.source, "local");
        assert!(!resolved.calendar.persist_after_plan);

        let _ = std::fs::remove_dir_all(root);
    }
}

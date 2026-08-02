use std::collections::BTreeSet;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chrono::{Days, NaiveDate, SecondsFormat, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum, error::ErrorKind};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tqsdk::{BacktestRemoteFillCancellation, BacktestRemoteFillConfig, RemoteFillPlan, Tq};
use tqsdk_cache::{
    FillReport, FillReportCalendar, FillReportSymbolDayStats, FillSelectorReport,
    MINUTE_FILL_REPORT_SCHEMA_VERSION, MinuteCacheCoverageSnapshot, MinuteFillReport,
    MinuteFillReportSymbol, PersistedFillReport, REPORT_SCHEMA_VERSION, TradingCalendarSnapshot,
    TradingDayWindow, default_fill_report_path, default_minute_fill_report_path, open_cache,
    open_read_only_cache, read_fill_report, read_persisted_fill_report,
    read_trading_calendar_snapshot, write_fill_report, write_minute_fill_report,
    write_trading_calendar_snapshot,
};
use tqsdk_data::{
    BacktestTickCache, DataClient, DataError, HistorySeriesCacheFileStatus, MinuteKlineCache,
    MinuteKlineCacheSnapshot, TradingCalendarRow, backtest_tick_trading_day_for_timestamp_ns,
};

mod progress;
mod terminal;

use progress::{
    FillProgress, FillProgressSession, ProgressCalendar, ProgressMode, ProgressTerminalStatus,
};
use terminal::{write_error as write_terminal_error, write_result as write_terminal_result};

#[derive(Debug, Parser)]
#[command(
    name = "tqsdk-cache",
    version,
    about = "Manage canonical tick and 60-second minute backtest caches"
)]
struct Cli {
    /// Canonical cache root. Defaults to TQSDK_HISTORY_CACHE_DIR or ~/.tqsdk/data_series_1.
    #[arg(long, global = true, value_name = "DIR")]
    cache_dir: Option<PathBuf>,
    /// Pretty-print the JSON result. Requires --output-format json.
    #[arg(long, global = true)]
    pretty: bool,
    /// Output rendering. Text is the default for terminal-oriented use.
    #[arg(long, global = true, value_enum, default_value_t = OutputFormat::Text)]
    output_format: OutputFormat,
    /// JSON result contract. Requires --output-format json; V2 preserves the legacy shape.
    #[arg(long, global = true, value_enum)]
    output_schema: Option<OutputSchema>,
    /// Cache family to manage. `minute` stores only final canonical 60-second Klines.
    #[arg(long, global = true, value_enum, default_value_t = CacheKind::Tick)]
    kind: CacheKind,
    /// Backtest market used for remote minute fills. Futures is the default.
    #[arg(long, global = true, value_enum, default_value_t = MarketKind::Futures)]
    market: MarketKind,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum OutputSchema {
    V2,
    V3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum OutputFormat {
    Json,
    Text,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum CacheKind {
    Tick,
    Minute,
    All,
}

impl CacheKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Tick => "tick",
            Self::Minute => "minute",
            Self::All => "all",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum MarketKind {
    Futures,
    Stock,
}

impl MarketKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Futures => "futures",
            Self::Stock => "stock",
        }
    }
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Fast filesystem-only inventory; safe to run while a fill is active.
    Inventory,
    /// Inspect coverage for explicit cache symbols.
    Inspect(InspectArgs),
    /// Fill missing closed trading days through the server-side backtest stream.
    Fill(FillArgs),
    /// Verify coverage and optionally replay cached data without remote fill.
    Verify(VerifyArgs),
    /// Deep read-only cache health diagnostics; requires a stable cache view.
    Doctor,
    /// Explicitly remove canonical-minute month partitions.
    Purge(PurgeArgs),
}

impl Command {
    fn name(&self) -> &'static str {
        match self {
            Self::Inventory => "inventory",
            Self::Inspect(_) => "inspect",
            Self::Fill(_) => "fill",
            Self::Verify(_) => "verify",
            Self::Doctor => "doctor",
            Self::Purge(_) => "purge",
        }
    }
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
    /// First trading day, inclusive, in YYYY-MM-DD form.
    #[arg(long, value_name = "YYYY-MM-DD", conflicts_with = "last_trading_days")]
    start_day: Option<NaiveDate>,
    /// Last trading day, inclusive, or the anchor for --last-trading-days.
    ///
    /// The current trading day is always rejected for minute fills; tick fills
    /// use provisional coverage unless --require-final is set.
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
    /// Cache symbol. Repeat for more symbols.
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
    /// Resolve and inspect coverage without acquiring a fill lock or requesting remote data.
    #[arg(long)]
    dry_run: bool,
    /// Allow a current-day tick fill. Unsupported for --kind minute.
    #[arg(
        long,
        conflicts_with_all = ["last_trading_days", "require_final"]
    )]
    include_open_day: bool,
    /// For tick fills, reject the currently open trading day and require final coverage.
    /// Minute fills are always final-only.
    #[arg(long)]
    require_final: bool,
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
    /// Progress rendering mode for this fill; defaults to dynamic tty bars.
    #[arg(long, value_enum, default_value_t = ProgressMode::Tty)]
    progress: ProgressMode,
    /// Maximum active symbol bars in TTY mode; zero keeps only the global bar.
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

#[derive(Debug, Args)]
struct PurgeArgs {
    #[command(flatten)]
    symbols: SymbolsArgs,
    #[command(flatten)]
    days: DaysArgs,
    /// List the exact monthly files that would be removed without writing.
    #[arg(long)]
    dry_run: bool,
    /// Confirm destructive deletion. Required unless --dry-run is used.
    #[arg(long)]
    yes: bool,
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

    fn code(&self) -> &'static str {
        if self.is_cache_busy() {
            "cache_busy"
        } else {
            match self {
                Self::Usage(_) => "usage",
                Self::Data(_) => "data_error",
                Self::Sdk(_) => "sdk_error",
                Self::Io(_) => "io_error",
                Self::Json(_) => "json_error",
            }
        }
    }

    fn retryable(&self) -> bool {
        self.is_cache_busy()
            || matches!(
                self,
                Self::Io(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::Interrupted | io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                    )
            )
    }

    fn is_cache_busy(&self) -> bool {
        match self {
            Self::Data(DataError::CacheBusy { .. }) => true,
            Self::Sdk(tqsdk::Error::Data(data)) => {
                matches!(&**data, DataError::CacheBusy { .. })
            }
            _ => false,
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

impl CommandOutcome {
    fn command(&self) -> &str {
        self.value
            .get("command")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
    }

    fn status(&self) -> &'static str {
        match self.exit_code {
            0 => "success",
            1 => "incomplete",
            130 => "interrupted",
            _ => "error",
        }
    }
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
    provisional: Option<ProvisionalOpenDayWindow>,
}

struct FillReportMetadata<'a> {
    window: TradingDayWindow,
    config: BacktestRemoteFillConfig,
    dry_run: bool,
    selector: FillSelectorReport,
    calendar: &'a CalendarResolution,
    provisional: Option<ProvisionalOpenDayWindow>,
}

#[derive(Debug, Clone, Copy)]
struct ProvisionalOpenDayWindow {
    day_start_ns: i64,
    as_of_ns: i64,
}

const OPEN_DAY_HORIZON_LAG_NS: i64 = 5 * 1_000_000_000;

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
    allow_open_day: bool,
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
        return resolved_fill_window(
            window,
            CalendarResolution {
                mode: args.calendar,
                snapshot: Some(snapshot),
                source,
                persist_after_plan,
            },
            allow_open_day,
        );
    }

    let start_day = args.start_day.ok_or_else(|| {
        CliError::Usage("fill requires --start-day or --last-trading-days".to_string())
    })?;
    let end_day = args
        .end_day
        .ok_or_else(|| CliError::Usage("fill requires --end-day with --start-day".to_string()))?;
    let window = TradingDayWindow::through_open_day_from_days(start_day, end_day)?;
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
    resolved_fill_window(
        window,
        CalendarResolution {
            mode: args.calendar,
            snapshot,
            source,
            persist_after_plan,
        },
        allow_open_day,
    )
}

fn resolved_fill_window(
    window: TradingDayWindow,
    calendar: CalendarResolution,
    allow_open_day: bool,
) -> Result<ResolvedFillWindow, CliError> {
    let now_ns = current_time_ns()?;
    let open_day = backtest_tick_trading_day_for_timestamp_ns(now_ns)?;
    let end_day = parse_window_day(&window.end_day)?;
    let provisional = if end_day == open_day {
        if !allow_open_day {
            return Err(CliError::Usage(format!(
                "--require-final requires a closed --end-day; {} is the current open TQBN trading day",
                window.end_day
            )));
        }
        let range = tqsdk_data::backtest_tick_trading_day_range(open_day)?;
        let as_of_ns = now_ns
            .saturating_sub(OPEN_DAY_HORIZON_LAG_NS)
            .min(range.end_ns);
        if as_of_ns <= range.start_ns {
            return Err(CliError::Usage(
                "current trading day has not advanced far enough for a provisional snapshot"
                    .to_string(),
            ));
        }
        Some(ProvisionalOpenDayWindow {
            day_start_ns: range.start_ns,
            as_of_ns,
        })
    } else {
        None
    };
    Ok(ResolvedFillWindow {
        window,
        calendar,
        provisional,
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
    Ok(backtest_tick_trading_day_for_timestamp_ns(
        current_time_ns()?,
    )?)
}

fn current_time_ns() -> Result<i64, CliError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_nanos()).ok())
        .ok_or_else(|| CliError::Usage("system clock is outside TQBN range".to_string()))
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let started_at = Instant::now();
    let requested_output = output_preferences_from_process_args();
    let parse_output_format = requested_output.output_format.unwrap_or(OutputFormat::Text);
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
            ) =>
        {
            let _ = error.print();
            std::process::exit(error.exit_code());
        }
        Err(error) => {
            let exit_code = error.exit_code();
            match parse_output_format {
                OutputFormat::Text => {
                    let _ = error.print();
                }
                OutputFormat::Json if requested_output.output_schema == Some(OutputSchema::V2) => {
                    let _ = error.print();
                }
                OutputFormat::Json => {
                    let cli_error = CliError::Usage(error.to_string());
                    if let Err(write_error) = write_output(
                        &error_envelope(None, &cli_error, exit_code, started_at),
                        false,
                    ) {
                        eprintln!("tqsdk-cache: {write_error}");
                    }
                }
            }
            std::process::exit(exit_code);
        }
    };
    let pretty = cli.pretty;
    let output_schema_is_explicit = cli.output_schema.is_some();
    let output_schema = cli.output_schema.unwrap_or(OutputSchema::V3);
    let command = cli.command.name();
    if matches!(cli.output_format, OutputFormat::Text) && (pretty || output_schema_is_explicit) {
        let error = CliError::Usage(
            "--pretty and --output-schema require --output-format json".to_string(),
        );
        if let Err(write_error) = write_terminal_error_output(command, &error) {
            eprintln!("tqsdk-cache: {write_error}");
        }
        std::process::exit(error.exit_code());
    }
    let output_format = cli.output_format;
    match run(cli).await {
        Ok(outcome) => {
            let exit_code = outcome.exit_code;
            let write_result = match output_format {
                OutputFormat::Text => write_terminal_output(&outcome, started_at),
                OutputFormat::Json => match output_schema {
                    OutputSchema::V2 => write_output(&outcome.value, pretty),
                    OutputSchema::V3 => {
                        let output = result_envelope(&outcome, started_at);
                        write_output(&output, pretty)
                    }
                },
            };
            if let Err(error) = write_result {
                eprintln!("tqsdk-cache: {error}");
                std::process::exit(error.exit_code());
            }
            if exit_code != 0 {
                std::process::exit(exit_code);
            }
        }
        Err(error) => {
            let write_error = match output_format {
                OutputFormat::Text => write_terminal_error_output(command, &error),
                OutputFormat::Json if matches!(output_schema, OutputSchema::V2) => {
                    eprintln!("tqsdk-cache: {error}");
                    Ok(())
                }
                OutputFormat::Json => write_output(
                    &error_envelope(Some(command), &error, error.exit_code(), started_at),
                    pretty,
                ),
            };
            if let Err(write_error) = write_error {
                eprintln!("tqsdk-cache: {write_error}");
            }
            std::process::exit(error.exit_code());
        }
    }
}

#[derive(Default)]
struct OutputPreferences {
    output_format: Option<OutputFormat>,
    output_schema: Option<OutputSchema>,
}

fn output_preferences_from_process_args() -> OutputPreferences {
    let mut preferences = OutputPreferences::default();
    let mut args = std::env::args().skip(1);
    while let Some(argument) = args.next() {
        if argument == "--" {
            break;
        }
        if argument == "--pretty" {
            continue;
        }
        if let Some(value) = argument.strip_prefix("--output-format=") {
            preferences.output_format = parse_output_format(value);
            continue;
        }
        if argument == "--output-format" {
            preferences.output_format = args.next().as_deref().and_then(parse_output_format);
            continue;
        }
        if let Some(value) = argument.strip_prefix("--output-schema=") {
            preferences.output_schema = parse_output_schema(value);
            continue;
        }
        if argument == "--output-schema" {
            preferences.output_schema = args.next().as_deref().and_then(parse_output_schema);
        }
    }
    preferences
}

fn parse_output_format(value: &str) -> Option<OutputFormat> {
    match value {
        "json" => Some(OutputFormat::Json),
        "text" => Some(OutputFormat::Text),
        _ => None,
    }
}

fn parse_output_schema(value: &str) -> Option<OutputSchema> {
    match value {
        "v2" => Some(OutputSchema::V2),
        "v3" => Some(OutputSchema::V3),
        _ => None,
    }
}

fn write_terminal_output(outcome: &CommandOutcome, started_at: Instant) -> Result<(), CliError> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    write_terminal_result(
        &mut stdout,
        &outcome.value,
        outcome.status(),
        outcome.exit_code,
        elapsed_millis(started_at),
    )?;
    Ok(())
}

fn write_terminal_error_output(command: &str, error: &CliError) -> Result<(), CliError> {
    let stderr = io::stderr();
    let mut stderr = stderr.lock();
    write_terminal_error(
        &mut stderr,
        command,
        &error.to_string(),
        error.exit_code(),
        error.retryable(),
    )?;
    Ok(())
}

const RESULT_SCHEMA_VERSION: u8 = 3;
const RESULT_KIND: &str = "tqsdk-cache.result";

fn result_envelope(outcome: &CommandOutcome, started_at: Instant) -> Value {
    json!({
        "schema_version": RESULT_SCHEMA_VERSION,
        "kind": RESULT_KIND,
        "command": outcome.command(),
        "status": outcome.status(),
        "exit_code": outcome.exit_code,
        "generated_at": result_generated_at(),
        "duration_ms": elapsed_millis(started_at),
        "tool": {
            "name": "tqsdk-cache",
            "version": env!("CARGO_PKG_VERSION"),
        },
        "warnings": [],
        "result": &outcome.value,
        "error": Value::Null,
    })
}

fn error_envelope(
    command: Option<&str>,
    error: &CliError,
    exit_code: i32,
    started_at: Instant,
) -> Value {
    json!({
        "schema_version": RESULT_SCHEMA_VERSION,
        "kind": RESULT_KIND,
        "command": command.unwrap_or("unknown"),
        "status": "error",
        "exit_code": exit_code,
        "generated_at": result_generated_at(),
        "duration_ms": elapsed_millis(started_at),
        "tool": {
            "name": "tqsdk-cache",
            "version": env!("CARGO_PKG_VERSION"),
        },
        "warnings": [],
        "result": {},
        "error": {
            "code": error.code(),
            "message": error.to_string(),
            "retryable": error.retryable(),
        },
    })
}

fn result_generated_at() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn elapsed_millis(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX)
}

async fn run(cli: Cli) -> Result<CommandOutcome, CliError> {
    validate_command_kind(&cli.command, cli.kind)?;
    match cli.command {
        Command::Inventory => inventory(cli.cache_dir.as_deref(), cli.kind),
        Command::Inspect(args) => inspect(cli.cache_dir.as_deref(), cli.kind, args),
        Command::Fill(args) => fill(cli.cache_dir.as_deref(), cli.kind, cli.market, args).await,
        Command::Verify(args) => verify(cli.cache_dir.as_deref(), cli.kind, args).await,
        Command::Doctor => doctor(cli.cache_dir.as_deref(), cli.kind),
        Command::Purge(args) => purge(cli.cache_dir.as_deref(), cli.kind, args),
    }
}

fn validate_command_kind(command: &Command, kind: CacheKind) -> Result<(), CliError> {
    if matches!(kind, CacheKind::All) && !matches!(command, Command::Inventory | Command::Doctor) {
        return Err(CliError::Usage(
            "--kind all is only supported by inventory and doctor".to_string(),
        ));
    }
    if matches!(command, Command::Purge(_)) && !matches!(kind, CacheKind::Minute) {
        return Err(CliError::Usage(
            "purge currently supports only --kind minute".to_string(),
        ));
    }
    Ok(())
}

fn inventory(cache_dir: Option<&Path>, kind: CacheKind) -> Result<CommandOutcome, CliError> {
    let (cache, cache_dir) = open_read_only_cache(cache_dir)?;
    let tick_inventory = cache.fast_inventory()?;
    let minute_inventory = MinuteKlineCache::open_read_only(&cache_dir).fast_inventory()?;
    let tick_json = || {
        json!({
            "backend_format": tick_inventory.backend_format,
            "total_files": tick_inventory.total_files,
            "total_bytes": tick_inventory.total_bytes,
            "total_days": tick_inventory.total_days,
            "problem_files": tick_inventory.problem_files,
            "symbols": tick_inventory.symbols.iter().map(|symbol| json!({
                "symbol": symbol.symbol,
                "files": symbol.files,
                "bytes": symbol.bytes,
                "days": symbol.days,
                "problem_files": symbol.problem_files,
            })).collect::<Vec<_>>(),
        })
    };
    let minute_json = || {
        json!({
            "backend_format": minute_inventory.format_id,
            "total_files": minute_inventory.total_files,
            "total_bytes": minute_inventory.total_bytes,
            "total_days": Value::Null,
            "problem_files": 0,
            "symbols": minute_inventory.symbols.iter().map(|symbol| json!({
                "symbol": symbol.symbol,
                "files": symbol.files,
                "bytes": symbol.bytes,
                "months": symbol.months,
            })).collect::<Vec<_>>(),
        })
    };
    let result = match kind {
        CacheKind::Tick => tick_json(),
        CacheKind::Minute => minute_json(),
        CacheKind::All => json!({
            "cache_kind": kind.as_str(),
            "tick": tick_json(),
            "minute": minute_json(),
        }),
    };
    let mut result = result;
    if !matches!(kind, CacheKind::All) {
        result["cache_kind"] = Value::String(kind.as_str().to_string());
    }
    result["schema_version"] = json!(REPORT_SCHEMA_VERSION);
    result["command"] = json!("inventory");
    result["cache_dir"] = json!(cache_dir);
    Ok(CommandOutcome {
        value: result,
        exit_code: 0,
    })
}

fn inspect(
    cache_dir: Option<&Path>,
    kind: CacheKind,
    args: InspectArgs,
) -> Result<CommandOutcome, CliError> {
    let symbols = normalized_symbols(args.symbols.symbols)?;
    let window = TradingDayWindow::from_days(args.days.start_day, args.days.end_day)?;
    let (cache, cache_dir) = open_read_only_cache(cache_dir)?;
    let statuses = match kind {
        CacheKind::Tick => symbols
            .iter()
            .map(|symbol| {
                cache
                    .inspect(symbol, window.start_ns, window.end_ns)
                    .map(|status| cache_status_json(&status))
            })
            .collect::<Result<Vec<_>, _>>()?,
        CacheKind::Minute => {
            let minute_cache = MinuteKlineCache::open_read_only(&cache_dir);
            symbols
                .iter()
                .map(|symbol| {
                    let snapshot = minute_cache_snapshot_for_symbol(
                        &cache_dir,
                        symbol.as_str(),
                        window.start_ns,
                        window.end_ns,
                    )?;
                    minute_cache
                        .inspect(symbol, window.start_ns, window.end_ns, &snapshot)
                        .map(|status| minute_cache_status_json(&status))
                })
                .collect::<Result<Vec<_>, _>>()?
        }
        CacheKind::All => unreachable!("kind validation rejects all for inspect"),
    };
    Ok(CommandOutcome {
        value: json!({
            "schema_version": REPORT_SCHEMA_VERSION,
            "command": "inspect",
            "cache_dir": cache_dir,
            "cache_kind": kind.as_str(),
            "requested_days": window,
            "statuses": statuses,
        }),
        exit_code: 0,
    })
}

async fn fill(
    cache_dir: Option<&Path>,
    kind: CacheKind,
    market: MarketKind,
    args: FillArgs,
) -> Result<CommandOutcome, CliError> {
    match kind {
        CacheKind::Tick if matches!(market, MarketKind::Stock) => Err(CliError::Usage(
            "--market stock is supported only for --kind minute fill".to_string(),
        )),
        CacheKind::Tick => fill_tick(cache_dir, args).await,
        CacheKind::Minute => fill_minute(cache_dir, market, args).await,
        CacheKind::All => unreachable!("kind validation rejects all for fill"),
    }
}

async fn fill_minute(
    cache_dir: Option<&Path>,
    market: MarketKind,
    args: FillArgs,
) -> Result<CommandOutcome, CliError> {
    let config = fill_config(&args);
    if args.include_open_day {
        return Err(CliError::Usage(
            "--include-open-day is not supported for --kind minute; minute coverage is final-only"
                .to_string(),
        ));
    }
    let explicit_symbols = normalized_symbols_allow_empty(args.symbols.symbols)?;
    let universe = normalized_universe(args.universe)?;
    if explicit_symbols.is_empty() && universe.is_none() {
        return Err(CliError::Usage(
            "fill requires at least one --symbol or --universe expression".to_string(),
        ));
    }
    if matches!(market, MarketKind::Stock) && universe.is_some() {
        return Err(CliError::Usage(
            "--market stock requires explicit --symbol values; futures --universe selectors are unsupported"
                .to_string(),
        ));
    }
    let (_, canonical_cache_dir) = open_read_only_cache(cache_dir)?;
    let resolved =
        resolve_fill_window(&canonical_cache_dir, &args.days, args.dry_run, false).await?;
    debug_assert!(resolved.provisional.is_none());
    let window = resolved.window;
    let selector_symbols = explicit_symbols.clone();
    let symbols = resolve_minute_fill_symbols(
        canonical_cache_dir.as_path(),
        &window,
        market,
        explicit_symbols,
        universe.as_deref(),
    )
    .await?;
    let selector = FillSelectorReport {
        symbols: selector_symbols,
        universe,
    };
    if args.dry_run {
        if args.report.is_some() {
            return Err(CliError::Usage(
                "--report cannot be used with --dry-run because dry-run writes no files"
                    .to_string(),
            ));
        }
        let cache = MinuteKlineCache::open_read_only(&canonical_cache_dir);
        let statuses = symbols
            .iter()
            .map(|symbol| {
                let snapshot = minute_cache_snapshot_for_symbol(
                    &canonical_cache_dir,
                    symbol.as_str(),
                    window.start_ns,
                    window.end_ns,
                )?;
                cache.inspect(symbol, window.start_ns, window.end_ns, &snapshot)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let report = minute_cache_only_report(
            canonical_cache_dir.as_path(),
            window.clone(),
            market,
            selector,
            statuses,
        );
        return Ok(CommandOutcome {
            value: json!({
                "schema_version": REPORT_SCHEMA_VERSION,
                "command": "fill",
                "cache_kind": "minute",
                "market": market.as_str(),
                "cache_dir": canonical_cache_dir,
                "dry_run": true,
                "report_path": Value::Null,
                "report": report,
            }),
            exit_code: if report.complete { 0 } else { 1 },
        });
    }

    let progress_session =
        FillProgressSession::new(args.progress, args.progress_max_bars, "minute");
    let reporter = progress_session.observer();
    reporter.planning("inspecting final canonical-minute coverage");
    if resolved.calendar.snapshot.is_some() {
        reporter.calendar_ready(resolved.calendar.progress_calendar(&window)?);
    }
    let cancellation = BacktestRemoteFillCancellation::new();
    let signal_cancellation = cancellation.clone();
    let signal_task = tokio::spawn(async move {
        wait_for_shutdown_signal(signal_cancellation).await;
    });
    let progress_callback = reporter.clone();
    let telemetry_callback = reporter.clone();
    let mut builder = builder_with_market_environment_auth(market, false)?
        .backtest(window.start_ns, window.end_ns)
        .cache_dir(&canonical_cache_dir)?
        .remote_on_miss()
        .remote_fill_config(config)
        .remote_fill_cancellation(cancellation.clone())
        .on_remote_fill_progress(move |event| progress_callback.observe_progress(event))
        .on_remote_fill_telemetry(move |event| telemetry_callback.observe_telemetry(event));
    if let Some(wait_secs) = args.lock_wait_secs {
        builder = builder.remote_fill_lock_wait(Duration::from_secs(wait_secs));
    }
    for symbol in &symbols {
        builder = builder.kline(symbol, Duration::from_secs(60), 1)?;
    }
    let warmup = builder.warmup().await;
    signal_task.abort();
    let _ = signal_task.await;
    if cancellation.is_cancelled() {
        progress_session.finish(
            ProgressTerminalStatus::Interrupted,
            "interrupted; no incomplete minute range was marked final",
        );
        let inventory = MinuteKlineCache::open_read_only(&canonical_cache_dir).fast_inventory()?;
        return Ok(CommandOutcome {
            value: json!({
                "schema_version": REPORT_SCHEMA_VERSION,
                "command": "fill",
                "cache_kind": "minute",
                "market": market.as_str(),
                "cache_dir": canonical_cache_dir,
                "status": "interrupted",
                "partial_inventory": minute_fast_inventory_json(&inventory),
            }),
            exit_code: 130,
        });
    }
    let warmup = match warmup {
        Ok(warmup) => warmup,
        Err(error) => {
            progress_session.finish(
                ProgressTerminalStatus::Failed,
                "minute fill failed; final coverage was not committed for failed ranges",
            );
            return Err(error.into());
        }
    };
    let report = MinuteFillReport::from_warmup(
        &warmup,
        canonical_cache_dir.as_path(),
        window,
        market.as_str(),
        false,
    )
    .with_selector(selector);
    let completion_summary = if report.complete {
        "minute fill complete; final canonical-minute coverage verified"
    } else {
        "minute fill completed with missing canonical-minute coverage"
    };
    progress_session.finish(
        if report.complete {
            ProgressTerminalStatus::Complete
        } else {
            ProgressTerminalStatus::Failed
        },
        completion_summary,
    );
    let report_path = args
        .report
        .unwrap_or_else(|| default_minute_fill_report_path(&canonical_cache_dir));
    write_minute_fill_report(&report_path, &report)?;
    Ok(CommandOutcome {
        value: json!({
            "schema_version": REPORT_SCHEMA_VERSION,
            "command": "fill",
            "cache_kind": "minute",
            "market": market.as_str(),
            "cache_dir": canonical_cache_dir,
            "dry_run": false,
            "report_path": report_path,
            "report": report,
        }),
        exit_code: if report.complete { 0 } else { 1 },
    })
}

fn minute_cache_snapshot_for_symbol(
    cache_dir: &Path,
    symbol: &str,
    start_ns: i64,
    end_ns: i64,
) -> Result<MinuteKlineCacheSnapshot, DataError> {
    tqsdk_data::resolve_minute_cache_metadata_snapshot(cache_dir, symbol, start_ns, end_ns)?
        .as_ref()
        .map(|metadata| {
            MinuteKlineCacheSnapshot::new(
                metadata.schema_version,
                metadata.snapshot_hash.clone(),
                metadata.session.snapshot_hash(),
            )
        })
        .transpose()?
        .map_or_else(|| Ok(MinuteKlineCacheSnapshot::cst_v1()), Ok)
}

async fn resolve_minute_fill_symbols(
    cache_dir: &Path,
    window: &TradingDayWindow,
    market: MarketKind,
    explicit_symbols: Vec<String>,
    universe: Option<&str>,
) -> Result<Vec<String>, CliError> {
    let mut symbols = explicit_symbols.into_iter().collect::<BTreeSet<_>>();
    let Some(universe) = universe else {
        return Ok(symbols.into_iter().collect());
    };
    debug_assert!(matches!(market, MarketKind::Futures));
    let builder = builder_with_market_environment_auth(MarketKind::Futures, false)?
        .backtest(window.start_ns, window.end_ns)
        .cache_store(BacktestTickCache::open_read_only(cache_dir))
        .cache_only()
        .universe(universe)?;
    let resolved = builder.warmup().await?;
    symbols.extend(resolved.logical_symbols);
    if symbols.is_empty() {
        return Err(CliError::Usage(
            "--universe resolved no canonical-minute symbols".to_string(),
        ));
    }
    Ok(symbols.into_iter().collect())
}

fn minute_cache_only_report(
    cache_dir: &Path,
    requested_days: TradingDayWindow,
    market: MarketKind,
    selector: FillSelectorReport,
    statuses: Vec<tqsdk_data::MinuteKlineCacheStatus>,
) -> MinuteFillReport {
    let symbols = statuses
        .into_iter()
        .map(|status| {
            let complete = status.is_complete();
            MinuteFillReportSymbol {
                symbol: status.symbol.clone(),
                action: if complete {
                    "skipped_complete".to_string()
                } else {
                    "missing_cache_only".to_string()
                },
                before: MinuteCacheCoverageSnapshot::from(&status),
                after: MinuteCacheCoverageSnapshot::from(&status),
                rows_written: 0,
            }
        })
        .collect::<Vec<_>>();
    let complete = !symbols.is_empty() && symbols.iter().all(|symbol| symbol.after.complete);
    MinuteFillReport {
        schema_version: MINUTE_FILL_REPORT_SCHEMA_VERSION,
        cache_kind: "minute".to_string(),
        generated_at: Utc::now().to_rfc3339(),
        cache_dir: cache_dir.display().to_string(),
        requested_range: (requested_days.start_ns, requested_days.end_ns),
        requested_days,
        selector,
        market: market.as_str().to_string(),
        logical_symbols: symbols.iter().map(|symbol| symbol.symbol.clone()).collect(),
        symbols,
        remote_used: false,
        rows_written: 0,
        complete,
        dry_run: true,
    }
}

async fn fill_tick(cache_dir: Option<&Path>, args: FillArgs) -> Result<CommandOutcome, CliError> {
    let allow_open_day = args.include_open_day || !args.require_final;
    let config = fill_config(&args);
    let symbols = normalized_symbols_allow_empty(args.symbols.symbols)?;
    let universe = normalized_universe(args.universe)?;
    if symbols.is_empty() && universe.is_none() {
        return Err(CliError::Usage(
            "fill requires at least one --symbol or --universe expression".to_string(),
        ));
    }
    let (cache, canonical_cache_dir) = if args.dry_run {
        open_read_only_cache(cache_dir)?
    } else {
        open_cache(cache_dir)?
    };
    let resolved = resolve_fill_window(
        &canonical_cache_dir,
        &args.days,
        args.dry_run,
        allow_open_day,
    )
    .await?;
    let window = resolved.window.clone();
    let operation_end_ns = resolved
        .provisional
        .map_or(window.end_ns, |provisional| provisional.as_of_ns);
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
        let mut builder = builder_with_environment_auth(false)?
            .backtest(window.start_ns, operation_end_ns)
            .cache_store(cache)
            .cache_only()
            .remote_fill_config(config);
        if let Some(provisional) = resolved.provisional {
            builder = builder
                .provisional_open_day_fill(provisional.day_start_ns, provisional.as_of_ns)?;
        }
        let builder = apply_fill_targets(builder, &symbols, universe.as_deref())?;
        let warmup = builder.warmup().await?;
        let report = fill_report_with_metadata(
            &warmup,
            &canonical_cache_dir,
            FillReportMetadata {
                window,
                config,
                dry_run: true,
                selector,
                calendar: &resolved.calendar,
                provisional: resolved.provisional,
            },
        )?;
        let complete = fill_operation_complete(&report, resolved.provisional);
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

    let progress_session = FillProgressSession::new(args.progress, args.progress_max_bars, "tick");
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
        .backtest(window.start_ns, operation_end_ns)
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
    if let Some(provisional) = resolved.provisional {
        builder =
            builder.provisional_open_day_fill(provisional.day_start_ns, provisional.as_of_ns)?;
    }
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
            progress_session.finish(
                ProgressTerminalStatus::Failed,
                "fill failed; calendar preparation did not complete",
            );
            return Err(error);
        }
        Err(error) => {
            progress_session.finish(
                ProgressTerminalStatus::Failed,
                "fill failed; calendar planning task did not complete",
            );
            return Err(CliError::Usage(format!(
                "calendar planning task failed: {error}"
            )));
        }
    };

    if cancellation.is_cancelled() {
        let summary =
            "interrupted; partial accepted rows were flushed without advancing the checkpoint";
        progress_session.finish(ProgressTerminalStatus::Interrupted, summary);
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
            progress_session.finish(
                ProgressTerminalStatus::Failed,
                "fill failed; strict local coverage was not committed",
            );
            return Err(error.into());
        }
    };
    let report = fill_report_with_metadata(
        &warmup,
        &canonical_cache_dir,
        FillReportMetadata {
            window,
            config,
            dry_run: false,
            selector,
            calendar: &calendar,
            provisional: resolved.provisional,
        },
    )?;
    reporter.final_report(&report);
    let completion_summary = if resolved.provisional.is_some() {
        "fill complete; provisional open-day checkpoint verified"
    } else {
        "fill complete; strict local coverage verified"
    };
    progress_session.finish(ProgressTerminalStatus::Complete, completion_summary);
    let report_path = args
        .report
        .unwrap_or_else(|| default_fill_report_path(&canonical_cache_dir));
    write_fill_report(&report_path, &report)?;
    let complete = fill_operation_complete(&report, resolved.provisional);
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
    metadata: FillReportMetadata<'_>,
) -> Result<FillReport, CliError> {
    let FillReportMetadata {
        window,
        config,
        dry_run,
        selector,
        calendar,
        provisional,
    } = metadata;
    let progress_calendar = calendar.progress_calendar(&window)?;
    let day_stats = warmup
        .symbols
        .iter()
        .map(|symbol| fill_symbol_day_stats(symbol, &progress_calendar.days))
        .collect::<Result<Vec<_>, _>>()?;
    let report = FillReport::from_warmup(warmup, cache_dir, window, config, dry_run)
        .with_v2_metadata(selector, Some(calendar.report_calendar()), day_stats);
    if let Some(provisional) = provisional {
        let complete_through_ns = provisional_complete_through(warmup, cache_dir, provisional)?;
        Ok(report.with_provisional_state(complete_through_ns))
    } else {
        Ok(report)
    }
}

fn provisional_complete_through(
    warmup: &tqsdk::BacktestCacheWarmupReport,
    cache_dir: &Path,
    provisional: ProvisionalOpenDayWindow,
) -> Result<Option<i64>, CliError> {
    let cache = BacktestTickCache::open_read_only(cache_dir);
    let mut shared_complete_through = None;
    for symbol in &warmup.symbols {
        let closed_start_ns = symbol.after.range_start_ns;
        let closed_end_ns = symbol.after.range_end_ns.min(provisional.day_start_ns);
        if closed_start_ns < closed_end_ns
            && !ranges_cover(&symbol.after.cached_ranges, closed_start_ns, closed_end_ns)
        {
            return Ok(None);
        }

        let open_start_ns = symbol.after.range_start_ns.max(provisional.day_start_ns);
        let open_end_ns = symbol.after.range_end_ns.min(provisional.as_of_ns);
        if open_start_ns >= open_end_ns {
            continue;
        }
        let symbol_complete_through =
            if ranges_cover(&symbol.after.cached_ranges, open_start_ns, open_end_ns) {
                open_end_ns
            } else {
                let Some(checkpoint) = cache.provisional_coverage(
                    symbol.symbol.as_str(),
                    open_start_ns,
                    open_end_ns,
                )?
                else {
                    return Ok(None);
                };
                checkpoint.complete_through_ns.min(open_end_ns)
            };
        shared_complete_through = Some(
            shared_complete_through.map_or(symbol_complete_through, |current: i64| {
                current.min(symbol_complete_through)
            }),
        );
    }
    Ok(shared_complete_through)
}

fn fill_operation_complete(
    report: &FillReport,
    provisional: Option<ProvisionalOpenDayWindow>,
) -> bool {
    provisional.map_or(report.complete, |provisional| {
        report
            .complete_through_ns
            .is_some_and(|through| through >= provisional.as_of_ns)
    })
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

async fn verify(
    cache_dir: Option<&Path>,
    kind: CacheKind,
    args: VerifyArgs,
) -> Result<CommandOutcome, CliError> {
    match kind {
        CacheKind::Tick => verify_tick(cache_dir, args).await,
        CacheKind::Minute => verify_minute(cache_dir, args).await,
        CacheKind::All => unreachable!("kind validation rejects all for verify"),
    }
}

async fn verify_tick(
    cache_dir: Option<&Path>,
    args: VerifyArgs,
) -> Result<CommandOutcome, CliError> {
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

async fn verify_minute(
    cache_dir: Option<&Path>,
    args: VerifyArgs,
) -> Result<CommandOutcome, CliError> {
    if args.min_rows.is_some() && !args.replay {
        return Err(CliError::Usage("--min-rows requires --replay".to_string()));
    }
    let (canonical_cache_dir, window, symbols, source_report, report_market) = match args.report {
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
            let PersistedFillReport::Minute(report) = read_persisted_fill_report(&report_path)?
            else {
                return Err(CliError::Usage(
                    "--kind minute verify requires a canonical-minute fill report".to_string(),
                ));
            };
            let report_root = PathBuf::from(&report.cache_dir);
            let (_, canonical_cache_dir) = open_read_only_cache(Some(&report_root))?;
            if let Some(requested_cache_dir) = cache_dir {
                let (_, requested_canonical_dir) = open_read_only_cache(Some(requested_cache_dir))?;
                if requested_canonical_dir != canonical_cache_dir {
                    return Err(CliError::Usage(format!(
                        "--cache-dir {} does not match report cache root {}",
                        requested_canonical_dir.display(),
                        canonical_cache_dir.display()
                    )));
                }
            }
            let symbols = report.symbols()?;
            (
                canonical_cache_dir,
                report.requested_days,
                symbols,
                Some("bound"),
                report.market,
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
            let (_, canonical_cache_dir) = open_read_only_cache(cache_dir)?;
            (
                canonical_cache_dir,
                window,
                symbols,
                None,
                "futures".to_string(),
            )
        }
    };
    let cache = MinuteKlineCache::open_read_only(&canonical_cache_dir);
    let snapshots = symbols
        .iter()
        .map(|symbol| {
            minute_cache_snapshot_for_symbol(
                &canonical_cache_dir,
                symbol,
                window.start_ns,
                window.end_ns,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let statuses = symbols
        .iter()
        .zip(&snapshots)
        .map(|(symbol, snapshot)| cache.inspect(symbol, window.start_ns, window.end_ns, snapshot))
        .collect::<Result<Vec<_>, _>>()?;
    let coverage_complete = statuses
        .iter()
        .all(tqsdk_data::MinuteKlineCacheStatus::is_complete);
    let replay_rows = if args.replay && coverage_complete {
        let mut total = 0_u64;
        for (symbol, snapshot) in symbols.iter().zip(&snapshots) {
            let mut reader = cache.open_reader(symbol, window.start_ns, window.end_ns, snapshot)?;
            while reader.next_kline()?.is_some() {
                total = total.saturating_add(1);
            }
        }
        Some(total)
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
            "cache_kind": "minute",
            "market": report_market,
            "cache_dir": canonical_cache_dir,
            "requested_days": window,
            "source_report": source_report,
            "symbols": symbols,
            "coverage_complete": coverage_complete,
            "replay_rows": replay_rows,
            "min_rows": args.min_rows,
            "statuses": statuses.iter().map(minute_cache_status_json).collect::<Vec<_>>(),
        }),
        exit_code: if coverage_complete && replay_ok { 0 } else { 1 },
    })
}

fn purge(
    cache_dir: Option<&Path>,
    kind: CacheKind,
    args: PurgeArgs,
) -> Result<CommandOutcome, CliError> {
    debug_assert!(matches!(kind, CacheKind::Minute));
    let symbols = normalized_symbols(args.symbols.symbols)?;
    if symbols.len() != 1 {
        return Err(CliError::Usage(
            "--kind minute purge requires exactly one --symbol".to_string(),
        ));
    }
    if !args.dry_run && !args.yes {
        return Err(CliError::Usage(
            "--kind minute purge is destructive; pass --yes or use --dry-run".to_string(),
        ));
    }
    let symbol = symbols.into_iter().next().expect("one symbol was required");
    let window = TradingDayWindow::from_days(args.days.start_day, args.days.end_day)?;
    let (_, canonical_cache_dir) = open_read_only_cache(cache_dir)?;
    if args.dry_run {
        let cache = MinuteKlineCache::open_read_only(&canonical_cache_dir);
        let snapshot = minute_cache_snapshot_for_symbol(
            &canonical_cache_dir,
            &symbol,
            window.start_ns,
            window.end_ns,
        )?;
        let status = cache.inspect(&symbol, window.start_ns, window.end_ns, &snapshot)?;
        let would_remove_files = status
            .months
            .iter()
            .filter(|month| month.present)
            .map(|month| {
                let size_bytes = std::fs::metadata(&month.path)
                    .map(|metadata| metadata.len())
                    .unwrap_or_default();
                json!({
                    "trading_month": month.trading_month,
                    "path": month.path,
                    "size_bytes": size_bytes,
                })
            })
            .collect::<Vec<_>>();
        return Ok(CommandOutcome {
            value: json!({
                "schema_version": REPORT_SCHEMA_VERSION,
                "command": "purge",
                "cache_kind": "minute",
                "cache_dir": canonical_cache_dir,
                "symbol": symbol,
                "requested_days": window,
                "dry_run": true,
                "would_remove_files": would_remove_files,
            }),
            exit_code: 0,
        });
    }

    let cache = MinuteKlineCache::open(&canonical_cache_dir)?;
    let report = cache.purge_range(&symbol, window.start_ns, window.end_ns)?;
    Ok(CommandOutcome {
        value: json!({
            "schema_version": REPORT_SCHEMA_VERSION,
            "command": "purge",
            "cache_kind": "minute",
            "cache_dir": canonical_cache_dir,
            "symbol": symbol,
            "requested_days": window,
            "dry_run": false,
            "removed_files": report.removed_files,
            "removed_bytes": report.removed_bytes,
            "removed_months": report.removed_months,
        }),
        exit_code: 0,
    })
}

fn doctor(cache_dir: Option<&Path>, kind: CacheKind) -> Result<CommandOutcome, CliError> {
    let (read_only_tick_cache, canonical_cache_dir) = open_read_only_cache(cache_dir)?;
    let minute_value = || -> Result<Value, CliError> {
        let report = MinuteKlineCache::open_read_only(&canonical_cache_dir).diagnose()?;
        Ok(json!({
            "backend_format": report.format_id,
            "problem_files": report.problem_files,
            "files": report.files.into_iter().map(|file| json!({
                "path": file.path,
                "trading_month": file.trading_month,
                "symbol": file.symbol,
                "status": minute_diagnostic_status_name(file.status),
                "rows": file.rows,
                "cached_ranges": file.cached_ranges,
                "size_bytes": file.size_bytes,
                "schema_version": file.schema_version,
                "error": file.error,
            })).collect::<Vec<_>>(),
        }))
    };
    let tick_value = || -> Result<Value, CliError> {
        let _lock = read_only_tick_cache.try_acquire_consistency_read_lock()?;
        let report = read_only_tick_cache.diagnose()?;
        Ok(json!({
            "backend_format": report.backend_format,
            "problem_files": report.problem_files,
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
        }))
    };
    let mut value = match kind {
        CacheKind::Tick => tick_value()?,
        CacheKind::Minute => minute_value()?,
        CacheKind::All => {
            let tick = tick_value()?;
            let minute = minute_value()?;
            json!({
                "cache_kind": kind.as_str(),
                "tick": tick,
                "minute": minute,
            })
        }
    };
    if !matches!(kind, CacheKind::All) {
        value["cache_kind"] = json!(kind.as_str());
    }
    let problem_files = match kind {
        CacheKind::All => {
            value["tick"]["problem_files"].as_u64().unwrap_or_default()
                + value["minute"]["problem_files"]
                    .as_u64()
                    .unwrap_or_default()
        }
        _ => value["problem_files"].as_u64().unwrap_or_default(),
    };
    value["schema_version"] = json!(REPORT_SCHEMA_VERSION);
    value["command"] = json!("doctor");
    value["cache_dir"] = json!(canonical_cache_dir);
    Ok(CommandOutcome {
        value,
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
    builder_with_market_environment_auth(MarketKind::Futures, require_auth)
}

fn builder_with_market_environment_auth(
    market: MarketKind,
    require_auth: bool,
) -> Result<tqsdk::TqBuilder, CliError> {
    let user = std::env::var("TQ_AUTH_USER").ok();
    let pass = std::env::var("TQ_AUTH_PASS").ok();
    let builder = match market {
        MarketKind::Futures => Tq::futures(),
        MarketKind::Stock => Tq::stock(),
    };
    match (user, pass) {
        (Some(user), Some(pass)) => Ok(builder.auth(user, pass)),
        (None, None) if !require_auth => Ok(builder),
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

fn minute_cache_status_json(status: &tqsdk_data::MinuteKlineCacheStatus) -> Value {
    json!({
        "symbol": status.symbol,
        "backend_format": status.format_id,
        "namespace_dir": status.namespace_dir,
        "range_start_ns": status.range_start_ns,
        "range_end_ns": status.range_end_ns,
        "cached_ranges": status.cached_ranges,
        "missing_ranges": status.missing_ranges,
        "complete": status.is_complete(),
        "months": status.months.iter().map(|month| json!({
            "trading_month": month.trading_month,
            "path": month.path,
            "present": month.present,
            "rows": month.rows,
            "cached_ranges": month.cached_ranges,
        })).collect::<Vec<_>>(),
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

fn minute_fast_inventory_json(inventory: &tqsdk_data::MinuteKlineCacheInventory) -> Value {
    json!({
        "backend_format": inventory.format_id,
        "total_files": inventory.total_files,
        "total_bytes": inventory.total_bytes,
        "symbols": inventory.symbols.iter().map(|symbol| json!({
            "symbol": symbol.symbol,
            "files": symbol.files,
            "bytes": symbol.bytes,
            "months": symbol.months,
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

fn minute_diagnostic_status_name(
    status: tqsdk_data::MinuteKlineCacheDiagnosticStatus,
) -> &'static str {
    match status {
        tqsdk_data::MinuteKlineCacheDiagnosticStatus::Readable => "readable",
        tqsdk_data::MinuteKlineCacheDiagnosticStatus::LegacyUnsupported => "legacy_unsupported",
        tqsdk_data::MinuteKlineCacheDiagnosticStatus::UnsupportedVersion => "unsupported_version",
        tqsdk_data::MinuteKlineCacheDiagnosticStatus::Corrupt => "corrupt",
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
    use super::{CalendarMode, Cli, Command, FillDaysArgs, ProgressMode, resolve_fill_window};
    use chrono::NaiveDate;
    use clap::Parser;
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

        let resolved = resolve_fill_window(&root, &args, true, false)
            .await
            .unwrap();

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

        let resolved = resolve_fill_window(&root, &args, true, false)
            .await
            .unwrap();

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
        let error = match resolve_fill_window(
            std::path::Path::new("/tmp/unused"),
            &args,
            true,
            false,
        )
        .await
        {
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

        let resolved = resolve_fill_window(&root, &args, false, true)
            .await
            .unwrap();

        assert_eq!(resolved.window.start_day, "2020-01-09");
        assert_eq!(resolved.window.end_day, "2020-01-10");
        assert!(resolved.provisional.is_none());
        assert_eq!(resolved.calendar.source, "local");
        assert!(!resolved.calendar.persist_after_plan);

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn fill_progress_defaults_to_tty() {
        let cli = Cli::try_parse_from([
            "tqsdk-cache",
            "fill",
            "--symbol",
            "SHFE.au2608",
            "--start-day",
            "2026-07-20",
            "--end-day",
            "2026-07-21",
        ])
        .unwrap();

        let Command::Fill(args) = cli.command else {
            panic!("expected fill command");
        };
        assert_eq!(args.progress, ProgressMode::Tty);
    }

    #[test]
    fn open_day_fill_requires_an_explicit_day_range() {
        let error = Cli::try_parse_from([
            "tqsdk-cache",
            "fill",
            "--symbol",
            "SHFE.au2608",
            "--last-trading-days",
            "1",
            "--include-open-day",
        ])
        .unwrap_err();

        assert_eq!(error.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn open_day_compat_flag_conflicts_with_require_final() {
        let error = Cli::try_parse_from([
            "tqsdk-cache",
            "fill",
            "--symbol",
            "SHFE.au2608",
            "--start-day",
            "2026-07-24",
            "--end-day",
            "2026-07-24",
            "--include-open-day",
            "--require-final",
        ])
        .unwrap_err();

        assert_eq!(error.kind(), clap::error::ErrorKind::ArgumentConflict);
    }
}

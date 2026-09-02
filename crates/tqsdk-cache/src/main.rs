use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, NaiveDate, SecondsFormat, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum, error::ErrorKind};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tqsdk::{BacktestRemoteFillCancellation, BacktestRemoteFillConfig, RemoteFillPlan, Tq};
use tqsdk_cache::{
    FillReport, FillReportCalendar, FillReportSymbolDayStats, FillSelectorReport,
    MINUTE_FILL_REPORT_SCHEMA_VERSION, MinuteCacheCoverageSnapshot, MinuteFillReport,
    MinuteFillReportSymbol, PersistedFillReport, REPORT_SCHEMA_VERSION,
    TradingCalendarHolidaysSnapshot, TradingDayWindow, UnifiedFillReport, UnifiedFillReportStatus,
    default_daily_fill_report_path, default_fill_report_path, default_minute_fill_report_path,
    open_cache, open_read_only_cache, read_persisted_fill_report,
    read_trading_calendar_holidays_snapshot, write_trading_calendar_holidays_snapshot,
    write_unified_fill_report,
};
use tqsdk_data::{
    BacktestHistoryClient, BacktestHistoryFillCancellation, BacktestHistoryFillConfig,
    BacktestHistoryFillSymbolStatus, BacktestHistoryFillTerminalStatus,
    BacktestHistoryMaintenanceClient, BacktestHistoryPolicy, BacktestHistoryRequest,
    BacktestTickCache, BacktestTickCacheLockRepairMode, BacktestTickCacheLockRepairStatus,
    DailyKlineCache, DataClient, DataError, HISTORY_SERIES_CACHE_FORMAT_ID,
    HISTORY_SERIES_CACHE_SCHEMA_VERSION, HistorySeriesCacheFileStatus,
    MINUTE_KLINE_CACHE_FORMAT_ID, MINUTE_KLINE_CACHE_SCHEMA_VERSION, MinuteKlineCache,
    MinuteKlineCacheSnapshot, backtest_tick_trading_day_for_timestamp_ns,
    backtest_tick_trading_day_range,
};
use tqsdk_session::SessionClientBuilder;

mod progress;
mod query;
mod snapshot;
mod terminal;

use progress::{
    FillProgress, FillProgressSession, ProgressCalendar, ProgressMode, ProgressTerminalStatus,
};
use query::{QueryArgs, QueryExecution, QueryRawOutput};
use terminal::{write_error as write_terminal_error, write_result as write_terminal_result};

#[derive(Debug, Parser)]
#[command(
    name = "tqsdk-cache",
    version,
    about = "Manage canonical tick, 60-second minute, and native daily backtest caches"
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
    /// Cache family to manage. `minute` and `daily` store only final Klines.
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
    Jsonl,
    LlmCsv,
    Text,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum CacheKind {
    Tick,
    Minute,
    Daily,
    All,
}

impl CacheKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Tick => "tick",
            Self::Minute => "minute",
            Self::Daily => "daily",
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
    /// Retry a bounded set of provider-unavailable history membership probes.
    RefreshProviderMembership(ProviderMembershipRefreshArgs),
    /// Verify coverage and optionally replay cached data without remote fill.
    Verify(VerifyArgs),
    /// Deep read-only cache health diagnostics; requires a stable cache view.
    Doctor,
    /// Inspect or repair missing Tick TQBN companion locks.
    RepairLocks(RepairLocksArgs),
    /// Re-encode legacy Tick or canonical-minute cache partitions into their current schema.
    Migrate(MigrateArgs),
    /// Verify and migrate one immutable historical-universe V4 plan to V5.
    MigrateUniverse(MigrateUniverseArgs),
    /// Explicitly refresh one logical symbol's metadata sidecar from the official source.
    MetadataRefresh(MetadataRefreshArgs),
    /// Explicitly remove canonical-minute month partitions.
    Purge(PurgeArgs),
    /// Query cache-backed history and emit raw JSONL or token-aware LLM CSV context.
    Query(QueryArgs),
    /// Manage immutable history generations under an explicit history root.
    Snapshot(snapshot::SnapshotArgs),
}

impl Command {
    fn name(&self) -> &'static str {
        match self {
            Self::Inventory => "inventory",
            Self::Inspect(_) => "inspect",
            Self::Fill(_) => "fill",
            Self::RefreshProviderMembership(_) => "refresh-provider-membership",
            Self::Verify(_) => "verify",
            Self::Doctor => "doctor",
            Self::RepairLocks(_) => "repair-locks",
            Self::Migrate(_) => "migrate",
            Self::MigrateUniverse(_) => "migrate-universe",
            Self::MetadataRefresh(_) => "metadata-refresh",
            Self::Purge(_) => "purge",
            Self::Query(_) => "query",
            Self::Snapshot(_) => "snapshot",
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

#[derive(Debug, Args)]
struct RepairLocksArgs {
    /// Create each missing legacy partition or per-file companion lock. Without this flag, only report the repair plan.
    #[arg(long)]
    apply: bool,
}

#[derive(Debug, Args)]
struct MigrateArgs {
    /// Rewrite legacy Tick TQBN or canonical-minute partitions. Without this flag, only report the migration plan.
    #[arg(long)]
    apply: bool,
    /// New empty rollback directory. Required with --apply; each original cache file is hard-linked here before rewrite.
    #[arg(long, value_name = "DIR", required_if_eq("apply", "true"))]
    backup_dir: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct MigrateUniverseArgs {
    /// Content-addressed V4 plan hash, including the sha256: prefix.
    #[arg(long, value_name = "SHA256")]
    plan_sha256: String,
    /// Publish the verified V5 artifact. Without this flag, only verify and report the mapping.
    #[arg(long)]
    apply: bool,
}

#[derive(Debug, Args)]
struct MetadataRefreshArgs {
    /// Logical cache symbol whose metadata sidecar should be refreshed.
    #[arg(long, value_name = "SYMBOL")]
    symbol: String,
    /// Inclusive RFC 3339 start timestamp; metadata windows are [start, end).
    #[arg(long, value_name = "RFC3339")]
    start: String,
    /// Exclusive RFC 3339 end timestamp; metadata windows are [start, end).
    #[arg(long, value_name = "RFC3339")]
    end: String,
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
    /// Last trading day, inclusive, or the anchor for --last-trading-days. Defaults to latest
    /// closed trading day when only --start-day is set.
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
    /// Refetch the raw holiday set and advance its active local snapshot.
    ///
    /// With --dry-run the fetched candidate is reported but never persisted.
    #[arg(long)]
    refresh_calendar: bool,
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

    /// Read exact symbols from a UTF-8 file. Repeat to combine files with a
    /// Universe V2 expression; files are expanded before any provider access.
    #[arg(long = "universe-file", value_name = "PATH")]
    universe_files: Vec<PathBuf>,

    /// Hidden compatibility override. V2 timelines now publish V5 by default.
    #[arg(
        long,
        default_value_t = tqsdk_data::HistoricalPlanWritePolicy::V4WithV3Rollback,
        value_name = "POLICY",
        hide = true
    )]
    historical_plan_write_policy: tqsdk_data::HistoricalPlanWritePolicy,
    /// Legacy compatibility input. New fills prepare plans internally from
    /// `--universe`.
    #[arg(
        long = "universe-plan",
        value_name = "PATH",
        conflicts_with = "universe",
        hide = true
    )]
    universe_timeline: Option<PathBuf>,
    /// Permit an unproven legacy v2 plan. V3 is required by default.
    #[arg(
        long,
        requires = "universe_timeline",
        hide = true,
        help = "Permit a legacy v2 universe plan without pinned kind-specific targets"
    )]
    allow_legacy_universe_plan: bool,
    #[command(flatten)]
    days: FillDaysArgs,
    /// Resolve and inspect coverage without acquiring a fill lock or requesting remote data.
    #[arg(long)]
    dry_run: bool,
    /// Explicitly purge only stale canonical-minute month partitions before remote fill retries.
    #[arg(long)]
    repair_stale: bool,
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
struct ProviderMembershipRefreshArgs {
    /// Pin refresh to this historical provider acquisition cutoff.
    #[arg(long, value_name = "SHA256")]
    acquisition_sha256: String,
    /// Retry at most this many unavailable contracts in one operation.
    #[arg(long, value_name = "COUNT", default_value_t = 4)]
    max_symbols: usize,
    /// Ignore persisted retry due times while retaining the bounded budget.
    #[arg(long)]
    force: bool,
    /// Select candidates and emit report without provider access or writes.
    #[arg(long)]
    dry_run: bool,
    /// Override default logical symbol concurrency for bounded native-daily probes.
    #[arg(long, value_name = "COUNT")]
    symbol_concurrency: Option<usize>,
    /// Override idle timeout for each bounded native-daily probe.
    #[arg(long, value_name = "SECONDS")]
    idle_timeout_secs: Option<u64>,
    /// Override native-daily batch timeout; zero disables timeout.
    #[arg(long, value_name = "SECONDS")]
    batch_timeout_secs: Option<u64>,
    /// Progress rendering mode; defaults to dynamic TTY bars.
    #[arg(long, value_enum, default_value_t = ProgressMode::Tty)]
    progress: ProgressMode,
    /// Maximum active symbol bars in TTY mode; zero keeps only global bar.
    #[arg(long, value_name = "COUNT", default_value_t = 4)]
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
    days: OptionalDaysArgs,
    /// List the exact cache files that would be removed without writing.
    #[arg(long)]
    dry_run: bool,
    /// Confirm destructive deletion. Required unless --dry-run is used.
    #[arg(long)]
    yes: bool,
}

#[derive(Debug)]
enum CliError {
    Usage(String),
    Migration(String),
    Data(DataError),
    Sdk(tqsdk::Error),
    Io(io::Error),
    Json(serde_json::Error),
}

impl CliError {
    fn exit_code(&self) -> i32 {
        match self {
            Self::Usage(_) => 2,
            Self::Migration(_) => 1,
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
                Self::Migration(_) => "migration_failed",
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
            Self::Usage(message) | Self::Migration(message) => write!(formatter, "{message}"),
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
    snapshot: Option<TradingCalendarHolidaysSnapshot>,
    source: String,
    persist_after_plan: bool,
    persisted: bool,
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
            persisted: self.persisted,
            snapshot: self
                .snapshot
                .as_ref()
                .map(TradingCalendarHolidaysSnapshot::metadata),
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
    if args.refresh_calendar && matches!(args.calendar, CalendarMode::Off) {
        return Err(CliError::Usage(
            "--refresh-calendar requires --calendar auto or required".to_string(),
        ));
    }
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
        match read_trading_calendar_holidays_snapshot(cache_dir) {
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
        if let Some(anchor) = args.end_day {
            if anchor >= open_day {
                return Err(CliError::Usage(format!(
                    "--end-day with --last-trading-days must be before the current open TQBN trading day {}",
                    open_day.format("%Y-%m-%d")
                )));
            }
        }
        let anchor = args.end_day.unwrap_or_else(|| {
            open_day
                .pred_opt()
                .expect("current TQBN trading day must have a previous date")
        });
        let mut snapshot = local_snapshot;
        let mut source = if snapshot.is_some() {
            "local".to_string()
        } else {
            "remote".to_string()
        };
        let mut persist_after_plan = false;
        let mut persisted = snapshot.is_some();
        let mut fetched_remote = false;
        if args.refresh_calendar || snapshot.is_none() {
            snapshot = Some(fetch_calendar_snapshot().await?);
            source = "remote".to_string();
            persist_after_plan = !dry_run;
            persisted = false;
            fetched_remote = true;
        }
        let (snapshot, eligible) = loop {
            let active_snapshot = snapshot.as_ref().ok_or_else(|| {
                CliError::Usage("--last-trading-days requires a trading calendar".to_string())
            })?;
            match eligible_calendar_days(active_snapshot, anchor) {
                Ok(eligible) if eligible.len() >= last_trading_days => {
                    break (active_snapshot.clone(), eligible);
                }
                Ok(_) if !fetched_remote => {
                    snapshot = Some(fetch_calendar_snapshot().await?);
                    source = "remote".to_string();
                    persist_after_plan = !dry_run;
                    persisted = false;
                    fetched_remote = true;
                    continue;
                }
                Ok(eligible) => {
                    return Err(CliError::Usage(format!(
                        "trading calendar contains only {} closed trading days through {} (supported years {} to {})",
                        eligible.len(),
                        anchor.format("%Y-%m-%d"),
                        active_snapshot.supported_year_start,
                        active_snapshot.supported_year_end,
                    )));
                }
                Err(_) if !fetched_remote => {
                    snapshot = Some(fetch_calendar_snapshot().await?);
                    source = "remote".to_string();
                    persist_after_plan = !dry_run;
                    persisted = false;
                    fetched_remote = true;
                }
                Err(error) => return Err(error),
            }
        };
        let selected = &eligible[eligible.len() - last_trading_days..];
        let window = TradingDayWindow::closed_from_days(selected[0], *selected.last().unwrap())?;
        return resolved_fill_window(
            window,
            CalendarResolution {
                mode: args.calendar,
                snapshot: Some(snapshot),
                source,
                persist_after_plan,
                persisted,
            },
            allow_open_day,
        );
    }

    let start_day = args.start_day.ok_or_else(|| {
        CliError::Usage("fill requires --start-day or --last-trading-days".to_string())
    })?;
    let window = match args.end_day {
        Some(end_day) => TradingDayWindow::through_open_day_from_days(start_day, end_day)?,
        None => {
            let current_open_day = current_open_trading_day()?;
            let current_open_range = backtest_tick_trading_day_range(current_open_day)?;
            let latest_closed_day = backtest_tick_trading_day_for_timestamp_ns(
                current_open_range.start_ns.saturating_sub(1),
            )?;
            TradingDayWindow::closed_from_days(start_day, latest_closed_day)?
        }
    };
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
    let mut persisted = snapshot.is_some();
    if !matches!(args.calendar, CalendarMode::Off) {
        let needs_calendar = snapshot.as_ref().is_none_or(|snapshot| {
            !snapshot
                .covers(normalized_start, normalized_end)
                .unwrap_or(false)
        });
        if args.refresh_calendar
            || (matches!(args.calendar, CalendarMode::Required) && needs_calendar)
        {
            snapshot = Some(fetch_calendar_snapshot().await?);
            source = "remote".to_string();
            persist_after_plan = !dry_run;
            persisted = false;
        } else if needs_calendar {
            snapshot = None;
            source = "partition_fallback".to_string();
            persisted = false;
        }
    }
    resolved_fill_window(
        window,
        CalendarResolution {
            mode: args.calendar,
            snapshot,
            source,
            persist_after_plan,
            persisted,
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
    if calendar.snapshot.is_none()
        && matches!(calendar.mode, CalendarMode::Auto | CalendarMode::Required)
        && plan.requires_remote_fill()
    {
        match fetch_calendar_snapshot().await {
            Ok(snapshot) => {
                calendar.snapshot = Some(snapshot);
                calendar.source = "remote".to_string();
                calendar.persist_after_plan = !dry_run;
                calendar.persisted = false;
            }
            Err(error) if matches!(calendar.mode, CalendarMode::Auto) => {
                calendar.source = "partition_fallback".to_string();
                reporter.calendar_unavailable(error.to_string());
            }
            Err(error) => return Err(error),
        }
    }
    persist_calendar_if_needed(&cache_dir, &mut calendar, dry_run)?;
    reporter.calendar_ready(calendar.progress_calendar(&window)?);
    Ok(calendar)
}

fn persist_calendar_if_needed(
    cache_dir: &Path,
    calendar: &mut CalendarResolution,
    dry_run: bool,
) -> Result<(), CliError> {
    if calendar.persist_after_plan && !dry_run {
        let snapshot = calendar.snapshot.as_ref().ok_or_else(|| {
            CliError::Usage("calendar persistence was requested without a snapshot".to_string())
        })?;
        write_trading_calendar_holidays_snapshot(cache_dir, snapshot)?;
        calendar.persist_after_plan = false;
        calendar.persisted = true;
    }
    Ok(())
}

async fn fetch_calendar_snapshot() -> Result<TradingCalendarHolidaysSnapshot, CliError> {
    let holidays = DataClient::new().query_trading_calendar_holidays().await?;
    Ok(TradingCalendarHolidaysSnapshot::from_holidays(holidays)?)
}

fn eligible_calendar_days(
    snapshot: &TradingCalendarHolidaysSnapshot,
    anchor: NaiveDate,
) -> Result<Vec<NaiveDate>, CliError> {
    let start = NaiveDate::from_ymd_opt(snapshot.supported_year_start, 1, 1).ok_or_else(|| {
        CliError::Usage("failed to build trading calendar lower bound".to_string())
    })?;
    Ok(snapshot.trading_days_between(start, anchor)?)
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
                OutputFormat::Jsonl | OutputFormat::LlmCsv => {
                    let _ = error.print();
                }
            }
            std::process::exit(exit_code);
        }
    };
    let pretty = cli.pretty;
    let output_schema_is_explicit = cli.output_schema.is_some();
    let output_schema = cli.output_schema.unwrap_or(OutputSchema::V3);
    let command = cli.command.name();
    if !matches!(cli.output_format, OutputFormat::Json) && (pretty || output_schema_is_explicit) {
        let error = CliError::Usage(
            "--pretty and --output-schema require --output-format json".to_string(),
        );
        let write_error = if matches!(
            cli.output_format,
            OutputFormat::Jsonl | OutputFormat::LlmCsv
        ) {
            eprintln!("tqsdk-cache {command}: {error}");
            Ok(())
        } else {
            write_terminal_error_output(command, &error)
        };
        if let Err(write_error) = write_error {
            eprintln!("tqsdk-cache: {write_error}");
        }
        std::process::exit(error.exit_code());
    }
    let output_format = cli.output_format;
    if let Command::Query(args) = &cli.command {
        let query = query::execute(
            cli.cache_dir.as_deref(),
            cli.kind,
            cli.market,
            output_format,
            args.clone(),
        )
        .await;
        match query {
            Ok(QueryExecution::Raw(raw)) => {
                let exit_code = raw.exit_code;
                if let Err(error) = write_query_raw_output(raw) {
                    eprintln!("tqsdk-cache query: {error}");
                    std::process::exit(error.exit_code());
                }
                if exit_code != 0 {
                    std::process::exit(exit_code);
                }
            }
            Ok(QueryExecution::Summary(outcome)) => {
                let exit_code = outcome.exit_code;
                let write_result = match output_format {
                    OutputFormat::Text => write_terminal_output(&outcome, started_at),
                    OutputFormat::Json => match output_schema {
                        OutputSchema::V2 => write_output(&outcome.value, pretty),
                        OutputSchema::V3 => {
                            write_output(&result_envelope(&outcome, started_at), pretty)
                        }
                    },
                    OutputFormat::Jsonl | OutputFormat::LlmCsv => Err(CliError::Usage(
                        "query raw output was not produced for the requested format".to_string(),
                    )),
                };
                if let Err(error) = write_result {
                    eprintln!("tqsdk-cache query: {error}");
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
                    OutputFormat::Jsonl | OutputFormat::LlmCsv => {
                        eprintln!("tqsdk-cache query: {error}");
                        Ok(())
                    }
                };
                if let Err(write_error) = write_error {
                    eprintln!("tqsdk-cache: {write_error}");
                }
                std::process::exit(error.exit_code());
            }
        }
        return;
    }
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
                OutputFormat::Jsonl | OutputFormat::LlmCsv => Err(CliError::Usage(
                    "--output-format jsonl and llm-csv are supported only by query".to_string(),
                )),
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
                OutputFormat::Jsonl | OutputFormat::LlmCsv => {
                    eprintln!("tqsdk-cache {command}: {error}");
                    Ok(())
                }
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
        "jsonl" => Some(OutputFormat::Jsonl),
        "llm-csv" => Some(OutputFormat::LlmCsv),
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

fn write_query_raw_output(raw: QueryRawOutput) -> Result<(), CliError> {
    if let Some(path) = raw.output_path {
        write_atomically(path.as_path(), raw.payload.as_slice())?;
        eprintln!("tqsdk-cache query: wrote {}", path.display());
        return Ok(());
    }
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    stdout.write_all(raw.payload.as_slice())?;
    stdout.flush()?;
    Ok(())
}

fn write_atomically(path: &Path, bytes: &[u8]) -> Result<(), CliError> {
    if path.as_os_str().is_empty() || path.file_name().is_none() {
        return Err(CliError::Usage(
            "--output must name a file, not a directory".to_string(),
        ));
    }
    if fs::metadata(path).is_ok_and(|metadata| metadata.is_dir()) {
        return Err(CliError::Usage(
            "--output must name a file, not a directory".to_string(),
        ));
    }
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| CliError::Usage("--output filename must be valid UTF-8".to_string()))?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| CliError::Usage("system clock is before UNIX epoch".to_string()))?
        .as_nanos();
    let temporary = parent.join(format!(".{file_name}.{}.{}", std::process::id(), nonce));
    let write_result = (|| -> Result<(), io::Error> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary, path)?;
        Ok(())
    })();
    if let Err(error) = write_result {
        let _ = fs::remove_file(&temporary);
        return Err(error.into());
    }
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
    if matches!(cli.command, Command::Snapshot(_)) && cli.cache_dir.is_some() {
        return Err(CliError::Usage(
            "snapshot commands use --history-root; do not pass --cache-dir".to_string(),
        ));
    }
    match cli.command {
        Command::Inventory => inventory(cli.cache_dir.as_deref(), cli.kind),
        Command::Inspect(args) => inspect(cli.cache_dir.as_deref(), cli.kind, args),
        Command::Fill(args) => fill(cli.cache_dir.as_deref(), cli.kind, cli.market, args).await,
        Command::RefreshProviderMembership(args) => {
            refresh_provider_membership(cli.cache_dir.as_deref(), cli.kind, cli.market, args).await
        }
        Command::Verify(args) => verify(cli.cache_dir.as_deref(), cli.kind, args).await,
        Command::Doctor => doctor(cli.cache_dir.as_deref(), cli.kind),
        Command::RepairLocks(args) => repair_locks(cli.cache_dir.as_deref(), cli.kind, args),
        Command::Migrate(args) => migrate(cli.cache_dir.as_deref(), cli.kind, args),
        Command::MigrateUniverse(args) => migrate_universe(cli.cache_dir.as_deref(), args),
        Command::MetadataRefresh(args) => {
            metadata_refresh(cli.cache_dir.as_deref(), cli.kind, cli.market, args).await
        }
        Command::Purge(args) => purge(cli.cache_dir.as_deref(), cli.kind, args),
        Command::Query(_) => unreachable!("main dispatches query output separately"),
        Command::Snapshot(args) => snapshot::execute(args).await,
    }
}

fn validate_command_kind(command: &Command, kind: CacheKind) -> Result<(), CliError> {
    if matches!(command, Command::Snapshot(_)) {
        return Ok(());
    }
    if matches!(command, Command::MigrateUniverse(_)) {
        if matches!(kind, CacheKind::Tick) {
            return Ok(());
        }
        return Err(CliError::Usage(
            "migrate-universe does not use --kind; leave the default tick kind".to_string(),
        ));
    }
    if matches!(command, Command::RefreshProviderMembership(_)) {
        if matches!(kind, CacheKind::Tick) {
            return Ok(());
        }
        return Err(CliError::Usage(
            "refresh-provider-membership always probes native daily history; leave default tick --kind"
                .to_string(),
        ));
    }
    if matches!(kind, CacheKind::All) && !matches!(command, Command::Inventory | Command::Doctor) {
        return Err(CliError::Usage(
            "--kind all is only supported by inventory and doctor".to_string(),
        ));
    }
    if matches!(kind, CacheKind::Daily)
        && !matches!(
            command,
            Command::Inventory
                | Command::Inspect(_)
                | Command::Fill(_)
                | Command::Verify(_)
                | Command::Doctor
                | Command::Purge(_)
        )
    {
        return Err(CliError::Usage(
            "--kind daily supports inventory, inspect, fill, verify, doctor, and purge".to_string(),
        ));
    }
    if matches!(command, Command::Purge(_))
        && !matches!(kind, CacheKind::Tick | CacheKind::Minute | CacheKind::Daily)
    {
        return Err(CliError::Usage(
            "purge supports only --kind tick, --kind minute, or --kind daily".to_string(),
        ));
    }
    if matches!(command, Command::RepairLocks(_)) && !matches!(kind, CacheKind::Tick) {
        return Err(CliError::Usage(
            "repair-locks currently supports only --kind tick".to_string(),
        ));
    }
    if matches!(command, Command::Migrate(_))
        && !matches!(kind, CacheKind::Tick | CacheKind::Minute)
    {
        return Err(CliError::Usage(
            "migrate supports only --kind tick or --kind minute".to_string(),
        ));
    }
    if matches!(command, Command::MetadataRefresh(_)) && !matches!(kind, CacheKind::Tick) {
        return Err(CliError::Usage(
            "metadata-refresh does not use --kind; leave the default tick kind".to_string(),
        ));
    }
    Ok(())
}

async fn metadata_refresh(
    cache_dir: Option<&Path>,
    kind: CacheKind,
    market: MarketKind,
    args: MetadataRefreshArgs,
) -> Result<CommandOutcome, CliError> {
    debug_assert!(matches!(kind, CacheKind::Tick));
    if !matches!(market, MarketKind::Futures) {
        return Err(CliError::Usage(
            "metadata-refresh supports only --market futures".to_string(),
        ));
    }
    let start_ns = parse_metadata_refresh_timestamp(args.start.as_str(), "--start")?;
    let end_ns = parse_metadata_refresh_timestamp(args.end.as_str(), "--end")?;
    if start_ns >= end_ns {
        return Err(CliError::Usage(
            "metadata-refresh range must satisfy start < end".to_string(),
        ));
    }

    let (cache, canonical_cache_dir) = open_cache(cache_dir)?;
    let _lock = cache.try_acquire_remote_fill_lock()?;
    let snapshot = BacktestHistoryMaintenanceClient::builder(&canonical_cache_dir)
        .auth_env()
        .build()?
        .refresh_metadata(args.symbol.as_str(), start_ns, end_ns)
        .await?;

    Ok(CommandOutcome {
        value: json!({
            "schema_version": REPORT_SCHEMA_VERSION,
            "command": "metadata-refresh",
            "cache_dir": canonical_cache_dir,
            "symbol": args.symbol,
            "requested_range": {
                "start": args.start,
                "end": args.end,
                "start_ns": start_ns,
                "end_ns": end_ns,
            },
            "snapshot": {
                "schema_version": snapshot.schema_version,
                "snapshot_hash": snapshot.snapshot_hash,
                "captured_at_ns": snapshot.captured_at_ns,
                "trading_days": snapshot.trading_days.len(),
                "physical_segments": snapshot.physical_segments.len(),
                "session_hash": snapshot.session.snapshot_hash(),
            },
        }),
        exit_code: 0,
    })
}

fn parse_metadata_refresh_timestamp(value: &str, flag: &str) -> Result<i64, CliError> {
    DateTime::parse_from_rfc3339(value)
        .map_err(|error| CliError::Usage(format!("{flag} must be RFC 3339: {error}")))?
        .timestamp_nanos_opt()
        .ok_or_else(|| CliError::Usage(format!("{flag} is outside the i64 nanosecond range")))
}

fn inventory(cache_dir: Option<&Path>, kind: CacheKind) -> Result<CommandOutcome, CliError> {
    let (cache, cache_dir) = open_read_only_cache(cache_dir)?;
    let tick_inventory = cache.fast_inventory()?;
    let minute_inventory = MinuteKlineCache::open_read_only(&cache_dir).fast_inventory()?;
    let daily_inventory = DailyKlineCache::open_read_only(&cache_dir).fast_inventory()?;
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
    let daily_json = || {
        json!({
            "backend_format": daily_inventory.format_id,
            "total_files": daily_inventory.total_files,
            "total_bytes": daily_inventory.total_bytes,
            "total_days": Value::Null,
            "problem_files": daily_inventory.problem_files,
            "symbols": daily_inventory.symbols.iter().map(|symbol| json!({
                "symbol": symbol.symbol,
                "files": symbol.files,
                "bytes": symbol.bytes,
                "problem_files": symbol.problem_files,
            })).collect::<Vec<_>>(),
        })
    };
    let result = match kind {
        CacheKind::Tick => tick_json(),
        CacheKind::Minute => minute_json(),
        CacheKind::Daily => daily_json(),
        CacheKind::All => json!({
            "cache_kind": kind.as_str(),
            "tick": tick_json(),
            "minute": minute_json(),
            "daily": daily_json(),
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
        CacheKind::Daily => {
            let daily_cache = DailyKlineCache::open_read_only(&cache_dir);
            symbols
                .iter()
                .map(|symbol| {
                    let snapshot = daily_cache_snapshot_for_symbol(&cache_dir, symbol.as_str())?;
                    daily_cache
                        .inspect(symbol, window.start_ns, window.end_ns, &snapshot)
                        .map(|status| daily_cache_status_json(&status))
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

#[derive(Debug)]
enum ProviderHistoricalUniverseInput {
    Legacy(tqsdk_data::HistoricalFillUniverseSpec),
    V2(tqsdk_data::ExpandedUniverseInput),
}

enum CompiledProviderHistoricalPlan {
    Legacy(Box<tqsdk_data::HistoricalUniversePlan>),
    V5(Box<tqsdk_data::HistoricalUniversePlanV5>),
}

impl ProviderHistoricalUniverseInput {
    fn canonical(&self) -> String {
        match self {
            Self::Legacy(spec) => spec.to_string(),
            Self::V2(input) => input
                .spec()
                .expect("historical V2 input always has a specification")
                .canonical_text()
                .to_string(),
        }
    }

    const fn language(&self) -> &'static str {
        match self {
            Self::Legacy(_) => "legacy-v1",
            Self::V2(_) => "v2",
        }
    }

    fn normalized_ast_sha256(&self) -> Option<&str> {
        match self {
            Self::Legacy(_) => None,
            Self::V2(input) => input
                .spec()
                .map(tqsdk_data::UniverseSpec::canonical_ast_hash),
        }
    }

    fn input_sources_sha256(&self) -> Option<&str> {
        match self {
            Self::Legacy(_) => None,
            Self::V2(input) => input.input_sources_sha256(),
        }
    }

    fn scope_provider_current_bootstrap(
        &self,
        acquisition: &tqsdk_data::HistoricalCatalogAcquisition,
    ) -> tqsdk_data::Result<tqsdk_data::HistoricalCatalogAcquisition> {
        match self {
            Self::Legacy(_) => Ok(acquisition.clone()),
            Self::V2(input) => {
                tqsdk_data::scope_provider_current_timeline_bootstrap(acquisition, input)
            }
        }
    }
}

fn prepare_provider_historical_universe(
    args: &FillArgs,
    value: &str,
) -> Result<ProviderHistoricalUniverseInput, CliError> {
    let dispatch = tqsdk_data::parse_historical_universe_compatible(value)
        .map_err(|error| CliError::Usage(error.to_string()))?;
    match dispatch {
        tqsdk_data::HistoricalUniverseDispatch::Legacy { spec, .. } => {
            if !args.universe_files.is_empty() {
                return Err(CliError::Usage(
                    "--universe-file with historical fill requires a Universe V2 timeline expression"
                        .to_string(),
                ));
            }
            Ok(ProviderHistoricalUniverseInput::Legacy(spec))
        }
        tqsdk_data::HistoricalUniverseDispatch::V2 { spec, .. } => {
            if spec.mode() != tqsdk_data::UniverseMode::Timeline {
                return Err(CliError::Usage(
                    "historical provider fill requires timeline(...) Universe V2 mode".to_string(),
                ));
            }
            args.historical_plan_write_policy
                .ensure_v2_timeline_enabled()
                .map_err(|error| CliError::Usage(error.to_string()))?;
            if let Some(selector) = spec.includes().iter().find(|selector| {
                matches!(
                    selector.view(),
                    tqsdk_data::UniverseView::Main | tqsdk_data::UniverseView::Top(_)
                )
            }) {
                return Err(CliError::Usage(
                    tqsdk_data::HistoricalUniverseV4Error::UnsupportedTimelineRanking {
                        view: selector.view(),
                    }
                    .to_string(),
                ));
            }
            let input = tqsdk_data::UniverseInput::from_spec(spec)
                .universe_symbol_files(args.universe_files.iter().cloned())
                .expand()
                .map_err(|error| CliError::Usage(error.to_string()))?;
            Ok(ProviderHistoricalUniverseInput::V2(input))
        }
        _ => unreachable!("historical universe dispatch is non-exhaustive"),
    }
}

async fn prepare_current_fill_universe(
    args: &mut FillArgs,
    market: MarketKind,
) -> Result<(), CliError> {
    let dispatch = args
        .universe
        .as_deref()
        .map(tqsdk_data::parse_snapshot_universe_compatible)
        .transpose()
        .map_err(|error| CliError::Usage(error.to_string()))?;

    match dispatch {
        Some(tqsdk_data::SnapshotUniverseDispatch::Legacy { .. }) => {
            if !args.universe_files.is_empty() {
                let expanded = tqsdk_data::UniverseInput::new(None)
                    .universe_symbol_files(args.universe_files.iter().cloned())
                    .expand()
                    .map_err(|error| CliError::Usage(error.to_string()))?;
                args.symbols
                    .symbols
                    .extend(expanded.expanded_symbols().iter().cloned());
            }
        }
        Some(tqsdk_data::SnapshotUniverseDispatch::V2 { spec, .. }) => {
            let input = tqsdk_data::UniverseInput::from_spec(spec)
                .universe_symbol_files(args.universe_files.iter().cloned())
                .expand()
                .map_err(|error| CliError::Usage(error.to_string()))?;
            let requires_provider = input.spec().is_some_and(|spec| {
                spec.includes()
                    .iter()
                    .any(|selector| selector.view() != tqsdk_data::UniverseView::Symbol)
            });
            let compiled = if requires_provider {
                if !matches!(market, MarketKind::Futures) {
                    return Err(CliError::Usage(
                        "Universe V2 futures views require --market futures".to_string(),
                    ));
                }
                let user = std::env::var("TQ_AUTH_USER").map_err(|_| {
                    CliError::Usage(
                        "dynamic Universe V2 fill requires TQ_AUTH_USER and TQ_AUTH_PASS"
                            .to_string(),
                    )
                })?;
                let pass = std::env::var("TQ_AUTH_PASS").map_err(|_| {
                    CliError::Usage(
                        "dynamic Universe V2 fill requires TQ_AUTH_USER and TQ_AUTH_PASS"
                            .to_string(),
                    )
                })?;
                let client = tqsdk_data::session_client_builder_for_futures_discovery(&user, &pass)
                    .build()
                    .map_err(tqsdk::Error::from)?;
                let mut resolver = tqsdk_data::SessionFuturesUniverseResolver::new(client);
                if input.spec().is_some_and(|spec| {
                    spec.includes().iter().any(|selector| {
                        matches!(selector.view(), tqsdk_data::UniverseView::Top(limit) if limit > 1)
                    })
                }) {
                    let activity_client = tqsdk_session::SessionClientBuilder::new(user, pass)
                        .futures_market()
                        .build()
                        .map_err(tqsdk::Error::from)?;
                    resolver = resolver.with_activity_client(activity_client);
                }
                tqsdk_data::resolve_futures_universe_v2(&input, &mut resolver).await?
            } else {
                tqsdk_data::compile_static_futures_universe_v2(&input)?
            };
            args.symbols.symbols.extend(
                compiled
                    .candidates()
                    .iter()
                    .map(|candidate| candidate.symbol().to_string()),
            );
            args.universe = None;
        }
        None => {
            if !args.universe_files.is_empty() {
                let input = tqsdk_data::UniverseInput::new(None)
                    .universe_symbol_files(args.universe_files.iter().cloned())
                    .expand()
                    .map_err(|error| CliError::Usage(error.to_string()))?;
                let compiled = tqsdk_data::compile_static_futures_universe_v2(&input)?;
                args.symbols.symbols.extend(
                    compiled
                        .candidates()
                        .iter()
                        .map(|candidate| candidate.symbol().to_string()),
                );
            }
        }
        Some(_) => unreachable!("snapshot universe dispatch is non-exhaustive"),
    }
    args.universe_files.clear();
    Ok(())
}

async fn fill(
    cache_dir: Option<&Path>,
    kind: CacheKind,
    market: MarketKind,
    args: FillArgs,
) -> Result<CommandOutcome, CliError> {
    if args.universe_timeline.is_some()
        && !matches!(kind, CacheKind::Tick | CacheKind::Minute | CacheKind::Daily)
    {
        return Err(CliError::Usage(
            "legacy --universe-plan supports only --kind tick, minute, or daily fill".to_string(),
        ));
    }
    if let Some(universe) = args.universe.as_deref()
        && (universe.trim() == "physical:all" || universe.trim().starts_with("timeline("))
    {
        let historical = prepare_provider_historical_universe(&args, universe.trim())?;
        return fill_provider_history_universe(cache_dir, kind, market, args, historical).await;
    }
    if let Some(plan_path) = args.universe_timeline.clone() {
        if !args.universe_files.is_empty() {
            return Err(CliError::Usage(
                "legacy --universe-plan cannot be combined with --universe-file".to_string(),
            ));
        }
        return fill_historical_universe_plan(cache_dir, kind, market, args, plan_path, None).await;
    }
    let mut args = args;
    prepare_current_fill_universe(&mut args, market).await?;
    match kind {
        CacheKind::Tick if args.repair_stale => Err(CliError::Usage(
            "--repair-stale is supported only for --kind minute fill".to_string(),
        )),
        CacheKind::Tick if matches!(market, MarketKind::Stock) => Err(CliError::Usage(
            "--market stock is supported only for --kind minute fill".to_string(),
        )),
        CacheKind::Tick => fill_tick(cache_dir, args).await,
        CacheKind::Minute => fill_minute(cache_dir, market, args).await,
        CacheKind::Daily => fill_daily(cache_dir, market, args).await,
        CacheKind::All => unreachable!("kind validation rejects all for fill"),
    }
}

async fn fill_provider_history_universe(
    cache_dir: Option<&Path>,
    kind: CacheKind,
    market: MarketKind,
    args: FillArgs,
    historical: ProviderHistoricalUniverseInput,
) -> Result<CommandOutcome, CliError> {
    if !matches!(market, MarketKind::Futures) {
        return Err(CliError::Usage(
            "historical --universe supports only --market futures".to_string(),
        ));
    }
    if !args.symbols.symbols.is_empty()
        || args.repair_stale
        || args.include_open_day
        || args.require_final
        || args.daily_slices
    {
        return Err(CliError::Usage(
            "historical --universe cannot be combined with explicit symbols, repair, open-day, require-final, or slicing flags"
                .to_string(),
        ));
    }
    if args.dry_run && args.report.is_some() {
        return Err(CliError::Usage(
            "--report cannot be combined with --dry-run because dry-run performs no writes"
                .to_string(),
        ));
    }
    let (_, canonical_cache_dir) = open_read_only_cache(cache_dir)?;
    let resolved =
        resolve_fill_window(&canonical_cache_dir, &args.days, args.dry_run, false).await?;
    let user = std::env::var("TQ_AUTH_USER").map_err(|_| {
        CliError::Usage("historical --universe requires TQ_AUTH_USER and TQ_AUTH_PASS".to_string())
    })?;
    let pass = std::env::var("TQ_AUTH_PASS").map_err(|_| {
        CliError::Usage("historical --universe requires TQ_AUTH_USER and TQ_AUTH_PASS".to_string())
    })?;
    let discovery = tqsdk_data::session_client_builder_for_futures_discovery(&user, &pass)
        .build()
        .map_err(tqsdk::Error::from)?;
    let observed_at_ns = chrono::Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| CliError::Usage("current timestamp exceeds nanosecond range".to_string()))?;
    let discovered_acquisition =
        tqsdk_data::ProviderCurrentHistoricalCatalogAcquirer::new(discovery)
            .acquire(resolved.window.end_ns, observed_at_ns)
            .await?;
    let acquisition = historical.scope_provider_current_bootstrap(&discovered_acquisition)?;
    let store = tqsdk_data::HistoricalUniverseArtifactStore::new(&canonical_cache_dir);
    if !args.dry_run {
        if acquisition.acquisition_sha256 != discovered_acquisition.acquisition_sha256 {
            store.publish_acquisition(&discovered_acquisition)?;
        }
        store.publish_acquisition(&acquisition)?;
        return bootstrap_provider_history_and_fill(ProviderHistoryFillContext {
            canonical_cache_dir,
            kind,
            market,
            args,
            historical,
            resolved,
            acquisition,
            auth_user: user,
            auth_pass: pass,
        })
        .await;
    }
    let artifact_path = store.acquisition_path(&acquisition.acquisition_sha256)?;
    let persisted_path = if args.dry_run {
        None
    } else {
        Some(store.publish_acquisition(&acquisition)?)
    };
    let proven_boundaries = acquisition
        .contracts
        .iter()
        .filter(|contract| {
            contract
                .first_available_data_ns
                .contains_key(&historical_data_kind(kind))
        })
        .count();
    let value = json!({
        "schema_version": REPORT_SCHEMA_VERSION,
        "command": "fill",
        "cache_kind": kind.as_str(),
        "market": market.as_str(),
        "cache_dir": canonical_cache_dir,
        "dry_run": args.dry_run,
        "status": "preparation_required",
        "complete": false,
        "remote_used": true,
        "rows_written": 0,
        "requested_days": resolved.window,
        "historical_universe": {
            "canonical": historical.canonical(),
            "language": historical.language(),
            "normalized_ast_sha256": historical.normalized_ast_sha256(),
            "input_sources_sha256": historical.input_sources_sha256(),
            "write_policy": args.historical_plan_write_policy.to_string(),
                "proof": "provider_current_observed",
                "source_identity": acquisition.source_identity,
                "scope_exchanges": tqsdk_data::PROVIDER_CURRENT_PHYSICAL_FUTURES_EXCHANGES,
                "complete_roster": acquisition.complete,
                "discovery_contracts": discovered_acquisition.contracts.len(),
                "bootstrap_contracts": acquisition.contracts.len(),
                "discovery_acquisition_sha256": discovered_acquisition.acquisition_sha256,
                "bootstrap_acquisition_sha256": acquisition.acquisition_sha256,
                "contracts": acquisition.contracts.len(),
            "expired_contracts": acquisition.contracts.iter().filter(|contract| contract.expired).count(),
            "active_contracts": acquisition.contracts.iter().filter(|contract| !contract.expired).count(),
            "kind_boundaries_proven": proven_boundaries,
            "acquisition_sha256": acquisition.acquisition_sha256,
            "artifact_path": artifact_path,
            "persisted_path": persisted_path,
            "executable": false,
            "blocked_reason": "dry-run does not mutate the native-daily cache, so provider data-membership preparation cannot complete",
        },
    });
    if let Some(path) = &args.report {
        write_atomically(path, &serde_json::to_vec_pretty(&value)?)?;
    }
    Ok(CommandOutcome {
        value,
        exit_code: 1,
    })
}

const PROVIDER_MEMBERSHIP_REFRESH_MAX_SYMBOLS: usize = 32;
const PROVIDER_MEMBERSHIP_CANARY_WINDOW_NS: i64 = 24 * 60 * 60 * 1_000_000_000;
const PROVIDER_MEMBERSHIP_DEFAULT_RETRY_TIMEOUT_SECS: u64 = 15;
const PROVIDER_MEMBERSHIP_DEFAULT_CANARY_TIMEOUT_SECS: u64 = 30;

#[derive(Debug)]
struct ProviderMembershipCanaryReport {
    symbol: String,
    healthy: bool,
    remote_used: bool,
    error: Option<String>,
}

struct ProviderMembershipCanaryCache {
    path: PathBuf,
}

impl ProviderMembershipCanaryCache {
    fn new(namespace_dir: &Path) -> Result<Self, CliError> {
        fs::create_dir_all(namespace_dir)?;
        let nonce = current_timestamp_ns()?;
        let path = namespace_dir.join(format!(
            ".provider-daily-canary-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&path)?;
        Ok(Self { path })
    }
}

impl Drop for ProviderMembershipCanaryCache {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

async fn refresh_provider_membership(
    cache_dir: Option<&Path>,
    _kind: CacheKind,
    market: MarketKind,
    args: ProviderMembershipRefreshArgs,
) -> Result<CommandOutcome, CliError> {
    if !matches!(market, MarketKind::Futures) {
        return Err(CliError::Usage(
            "refresh-provider-membership supports only --market futures".to_string(),
        ));
    }
    if args.max_symbols == 0 || args.max_symbols > PROVIDER_MEMBERSHIP_REFRESH_MAX_SYMBOLS {
        return Err(CliError::Usage(format!(
            "--max-symbols must be within 1..={PROVIDER_MEMBERSHIP_REFRESH_MAX_SYMBOLS}"
        )));
    }

    let (_, canonical_cache_dir) = open_read_only_cache(cache_dir)?;
    let store = tqsdk_data::HistoricalUniverseArtifactStore::new(&canonical_cache_dir);
    let _operation_lock = (!args.dry_run)
        .then(|| store.try_acquire_provider_daily_retry_operation_lock())
        .transpose()?;
    let acquisition = store.load_acquisition(&args.acquisition_sha256)?;
    let retry_state = store
        .find_provider_daily_retry_state(&acquisition)?
        .map_or_else(
            || tqsdk_data::ProviderDailyUnavailableRetryState::from_acquisition(&acquisition),
            Ok,
        )?;
    let now_ns = current_timestamp_ns()?;
    let candidates = retry_state.candidates(&acquisition)?;
    let due_count = candidates
        .iter()
        .filter(|candidate| candidate.retry.next_retry_at_ns <= now_ns)
        .count();
    let selected = candidates
        .iter()
        .filter(|candidate| args.force || candidate.retry.next_retry_at_ns <= now_ns)
        .take(args.max_symbols)
        .cloned()
        .collect::<Vec<_>>();
    let selected_json = selected
        .iter()
        .map(provider_membership_retry_candidate_json)
        .collect::<Vec<_>>();

    if args.dry_run {
        return Ok(CommandOutcome {
            value: json!({
                "schema_version": REPORT_SCHEMA_VERSION,
                "command": "refresh-provider-membership",
                "cache_kind": "daily",
                "market": market.as_str(),
                "cache_dir": canonical_cache_dir,
                "dry_run": true,
                "status": "planned",
                "complete": true,
                "acquisition_sha256": acquisition.acquisition_sha256,
                "candidate_count": candidates.len(),
                "due_count": due_count,
                "selected_count": selected.len(),
                "force": args.force,
                "selected": selected_json,
            }),
            exit_code: 0,
        });
    }
    if selected.is_empty() {
        return Ok(CommandOutcome {
            value: json!({
                "schema_version": REPORT_SCHEMA_VERSION,
                "command": "refresh-provider-membership",
                "cache_kind": "daily",
                "market": market.as_str(),
                "cache_dir": canonical_cache_dir,
                "dry_run": false,
                "status": "idle",
                "complete": true,
                "acquisition_sha256": acquisition.acquisition_sha256,
                "candidate_count": candidates.len(),
                "due_count": due_count,
                "selected_count": 0,
                "force": args.force,
                "selected": selected_json,
            }),
            exit_code: 0,
        });
    }

    let user = std::env::var("TQ_AUTH_USER").map_err(|_| {
        CliError::Usage(
            "refresh-provider-membership requires TQ_AUTH_USER and TQ_AUTH_PASS".to_string(),
        )
    })?;
    let pass = std::env::var("TQ_AUTH_PASS").map_err(|_| {
        CliError::Usage(
            "refresh-provider-membership requires TQ_AUTH_USER and TQ_AUTH_PASS".to_string(),
        )
    })?;
    let discovered_before =
        acquire_provider_membership_current(&user, &pass, acquisition.requested_as_of_ns).await?;
    let current_before = acquisition.project_provider_current_refresh(&discovered_before)?;
    acquisition.validate_provider_daily_refresh_current(&current_before)?;

    let cancellation = BacktestHistoryFillCancellation::new();
    let signal_task = spawn_shutdown_signal_handler(cancellation.clone(), CacheKind::Daily)?;
    let progress_session = FillProgressSession::new(
        args.progress,
        args.progress_max_bars,
        "daily-membership-refresh",
    );
    let reporter = progress_session.observer();
    let operation = async {
        reporter.planning("probing provider-health canary before bounded membership refresh");
        let canary =
            probe_provider_membership_canary(&store, &acquisition, &args, cancellation.clone())
                .await?;
        if cancellation.is_cancelled() {
            progress_session.finish(
                ProgressTerminalStatus::Interrupted,
                "provider membership refresh was cancelled before retries",
            );
            return Ok(provider_membership_refresh_cancelled_outcome(
                &canonical_cache_dir,
                market,
                &acquisition,
                candidates.len(),
                due_count,
                selected_json,
                canary,
            ));
        }
        if !canary.healthy {
            progress_session.finish(
                ProgressTerminalStatus::Failed,
                "provider-health canary did not complete remotely; retry schedule unchanged",
            );
            return Ok(CommandOutcome {
                value: json!({
                    "schema_version": REPORT_SCHEMA_VERSION,
                    "command": "refresh-provider-membership",
                    "cache_kind": "daily",
                    "market": market.as_str(),
                    "cache_dir": canonical_cache_dir,
                    "dry_run": false,
                    "status": "provider_unhealthy",
                    "complete": false,
                    "acquisition_sha256": acquisition.acquisition_sha256,
                    "candidate_count": candidates.len(),
                    "due_count": due_count,
                    "selected_count": selected.len(),
                    "selected": selected_json,
                    "canary": provider_membership_canary_json(&canary),
                    "retry_state_advanced": false,
                }),
                exit_code: 1,
            });
        }

        reporter.planning("retrying bounded provider-unavailable native daily probes");
        let config = provider_membership_refresh_fill_config(&args)?;
        let requests = selected
            .iter()
            .enumerate()
            .map(|(index, candidate)| {
                let observation = acquisition
                    .provider_daily_observations
                    .get(&candidate.symbol)
                    .expect("retry candidate validated against provider-history acquisition");
                BacktestHistoryRequest::kline(
                    u64::try_from(index).expect("bounded retry request id fits u64"),
                    &candidate.symbol,
                    Duration::from_secs(24 * 60 * 60),
                    observation.range_start_ns,
                    observation.range_end_ns,
                )
            })
            .collect::<Vec<_>>();
        let client = BacktestHistoryClient::builder(canonical_cache_dir.clone())
            .policy(BacktestHistoryPolicy::RemoteOnMiss)
            .auth_env()
            .build()?;
        let progress_callback = reporter.clone();
        let report = client
            .orchestrate_fill(requests, config, cancellation.clone(), move |event| {
                progress_callback.observe_history_progress(&event)
            })
            .await;
        drop(client);
        let report = report?;
        if report.status() == BacktestHistoryFillTerminalStatus::Interrupted
            || cancellation.is_cancelled()
        {
            progress_session.finish(
                ProgressTerminalStatus::Interrupted,
                "provider membership refresh was cancelled",
            );
            return Ok(provider_membership_refresh_cancelled_outcome(
                &canonical_cache_dir,
                market,
                &acquisition,
                candidates.len(),
                due_count,
                selected_json,
                canary,
            ));
        }

        let mut updates = BTreeMap::new();
        let mut symbol_reports = Vec::new();
        let daily_cache = DailyKlineCache::open_read_only(&canonical_cache_dir);
        for item in report.symbols() {
            let prior = acquisition
                .provider_daily_observations
                .get(&item.symbol)
                .expect("retry request derived from provider-history acquisition");
            let observation = match item.status {
                BacktestHistoryFillSymbolStatus::Complete => {
                    let snapshot =
                        daily_cache_snapshot_for_symbol(&canonical_cache_dir, &item.symbol)?;
                    let first_row_ns = daily_cache
                        .read_range(
                            &item.symbol,
                            prior.range_start_ns,
                            prior.range_end_ns,
                            &snapshot,
                        )?
                        .first()
                        .map(|row| row.datetime);
                    tqsdk_data::HistoricalDailyObservation::new(
                        prior.range_start_ns,
                        prior.range_end_ns,
                        first_row_ns,
                    )?
                }
                BacktestHistoryFillSymbolStatus::Failed => {
                    let Some(unavailable_after_ns) = isolated_provider_history_unavailable_after_ns(
                        item.error.as_deref(),
                        config,
                    ) else {
                        let sample = item.error.as_deref().unwrap_or("unknown provider failure");
                        progress_session.finish(
                            ProgressTerminalStatus::Failed,
                            "provider membership refresh encountered non-timeout failure",
                        );
                        return Err(DataError::InvalidResponse(format!(
                            "provider membership refresh has blocking failure for {}: {sample}",
                            item.symbol
                        ))
                        .into());
                    };
                    tqsdk_data::HistoricalDailyObservation::provider_unavailable(
                        prior.range_start_ns,
                        prior.range_end_ns,
                        unavailable_after_ns,
                    )?
                }
                BacktestHistoryFillSymbolStatus::Interrupted => {
                    progress_session.finish(
                        ProgressTerminalStatus::Interrupted,
                        "provider membership refresh was interrupted",
                    );
                    return Ok(provider_membership_refresh_cancelled_outcome(
                        &canonical_cache_dir,
                        market,
                        &acquisition,
                        candidates.len(),
                        due_count,
                        selected_json,
                        canary,
                    ));
                }
            };
            let status = match observation.status {
                tqsdk_data::HistoricalDailyObservationStatus::Complete => "complete",
                tqsdk_data::HistoricalDailyObservationStatus::ProviderUnavailable => {
                    "provider_unavailable"
                }
                _ => {
                    return Err(DataError::InvalidState(
                        "provider membership retry emitted unsupported observation status",
                    )
                    .into());
                }
            };
            symbol_reports.push(json!({
                "symbol": item.symbol,
                "status": status,
                "first_row_ns": observation.first_row_ns,
                "unavailable_after_ns": observation.provider_unavailable_after_ns,
                "remote_used": item.remote_used,
                "error": item.error,
            }));
            updates.insert(item.symbol.clone(), observation);
        }
        let attempted_symbols = selected
            .iter()
            .map(|candidate| candidate.symbol.clone())
            .collect::<BTreeSet<_>>();
        if updates.len() != attempted_symbols.len() {
            return Err(DataError::InvalidState(
                "provider membership retry report omitted an attempted symbol",
            )
            .into());
        }

        reporter.planning("revalidating stable provider roster before publishing retry result");
        let discovered_after =
            acquire_provider_membership_current(&user, &pass, acquisition.requested_as_of_ns)
                .await?;
        let current_after = acquisition.project_provider_current_refresh(&discovered_after)?;
        acquisition.validate_provider_daily_refresh_current(&current_after)?;
        if cancellation.is_cancelled() {
            progress_session.finish(
                ProgressTerminalStatus::Interrupted,
                "provider membership refresh was cancelled before publication",
            );
            return Ok(provider_membership_refresh_cancelled_outcome(
                &canonical_cache_dir,
                market,
                &acquisition,
                candidates.len(),
                due_count,
                selected_json,
                canary,
            ));
        }

        let upgraded = updates.values().any(|observation| {
            observation.status == tqsdk_data::HistoricalDailyObservationStatus::Complete
        });
        let receipt_at_ns = current_timestamp_ns()?;
        let (next_acquisition, catalog_path, acquisition_path) = if upgraded {
            let next = acquisition.refresh_provider_daily_observations(current_after, updates)?;
            let semantic = tqsdk_data::HistoricalSemanticCatalog::from_provider_history_observed(
                &next,
                tqsdk_data::PROVIDER_DAILY_MEMBERSHIP_CALENDAR_IDENTITY,
            )?;
            let receipt =
                retry_state.refreshed(&acquisition, &next, &attempted_symbols, receipt_at_ns)?;
            if cancellation.is_cancelled() {
                return Ok(provider_membership_refresh_cancelled_outcome(
                    &canonical_cache_dir,
                    market,
                    &acquisition,
                    candidates.len(),
                    due_count,
                    selected_json,
                    canary,
                ));
            }
            let acquisition_path = store.publish_acquisition(&next)?;
            if cancellation.is_cancelled() {
                return Ok(provider_membership_refresh_cancelled_outcome(
                    &canonical_cache_dir,
                    market,
                    &acquisition,
                    candidates.len(),
                    due_count,
                    selected_json,
                    canary,
                ));
            }
            let catalog_path = store.publish_semantic_catalog(&semantic)?;
            if !receipt.is_empty() {
                store.publish_provider_daily_retry_state(&receipt)?;
            }
            (next, Some(catalog_path), Some(acquisition_path))
        } else {
            let receipt = retry_state.refreshed(
                &acquisition,
                &acquisition,
                &attempted_symbols,
                receipt_at_ns,
            )?;
            store.publish_provider_daily_retry_state(&receipt)?;
            (acquisition.clone(), None, None)
        };
        progress_session.finish(
            ProgressTerminalStatus::Complete,
            "bounded provider membership retry completed",
        );
        Ok(CommandOutcome {
            value: json!({
                "schema_version": REPORT_SCHEMA_VERSION,
                "command": "refresh-provider-membership",
                "cache_kind": "daily",
                "market": market.as_str(),
                "cache_dir": canonical_cache_dir,
                "dry_run": false,
                "status": "complete",
                "complete": true,
                "source_acquisition_sha256": acquisition.acquisition_sha256,
                "acquisition_sha256": next_acquisition.acquisition_sha256,
                "acquisition_path": acquisition_path,
                "semantic_catalog_path": catalog_path,
                "candidate_count": candidates.len(),
                "due_count": due_count,
                "selected_count": selected.len(),
                "selected": selected_json,
                "canary": provider_membership_canary_json(&canary),
                "upgraded": upgraded,
                "symbol_reports": symbol_reports,
            }),
            exit_code: 0,
        })
    }
    .await;
    signal_task.abort();
    let _ = signal_task.await;
    operation
}

async fn acquire_provider_membership_current(
    user: &str,
    pass: &str,
    requested_as_of_ns: i64,
) -> Result<tqsdk_data::HistoricalCatalogAcquisition, CliError> {
    let discovery = tqsdk_data::session_client_builder_for_futures_discovery(user, pass)
        .build()
        .map_err(tqsdk::Error::from)?;
    tqsdk_data::ProviderCurrentHistoricalCatalogAcquirer::new(discovery)
        .acquire(requested_as_of_ns, current_timestamp_ns()?)
        .await
        .map_err(Into::into)
}

fn provider_membership_refresh_fill_config(
    args: &ProviderMembershipRefreshArgs,
) -> Result<BacktestHistoryFillConfig, DataError> {
    let mut config = BacktestHistoryFillConfig::default().with_symbol_batch_size(1)?;
    if let Some(value) = args.symbol_concurrency {
        config = config.with_symbol_concurrency(value)?;
    }
    if let Some(value) = args.idle_timeout_secs {
        config = config.with_idle_timeout(Duration::from_secs(value))?;
    }
    config = match args.batch_timeout_secs {
        Some(0) => config.without_batch_timeout(),
        Some(value) => config.with_batch_timeout(Some(Duration::from_secs(value)))?,
        None => config.with_batch_timeout(Some(Duration::from_secs(
            PROVIDER_MEMBERSHIP_DEFAULT_RETRY_TIMEOUT_SECS,
        )))?,
    };
    Ok(config)
}

fn provider_membership_canary_fill_config(
    args: &ProviderMembershipRefreshArgs,
) -> Result<BacktestHistoryFillConfig, DataError> {
    let mut config = provider_membership_refresh_fill_config(args)?;
    if args.batch_timeout_secs.is_none() {
        config = config.with_batch_timeout(Some(Duration::from_secs(
            PROVIDER_MEMBERSHIP_DEFAULT_CANARY_TIMEOUT_SECS,
        )))?;
    }
    Ok(config)
}

async fn probe_provider_membership_canary(
    store: &tqsdk_data::HistoricalUniverseArtifactStore,
    acquisition: &tqsdk_data::HistoricalCatalogAcquisition,
    args: &ProviderMembershipRefreshArgs,
    cancellation: BacktestHistoryFillCancellation,
) -> Result<ProviderMembershipCanaryReport, CliError> {
    let (symbol, start_ns, end_ns) = provider_membership_canary_target(acquisition)?;
    let temporary_cache = ProviderMembershipCanaryCache::new(&store.namespace_dir())?;
    let client = BacktestHistoryClient::builder(temporary_cache.path.clone())
        .policy(BacktestHistoryPolicy::RemoteOnMiss)
        .auth_env()
        .build()?;
    let result = client
        .orchestrate_fill(
            [BacktestHistoryRequest::kline(
                0,
                &symbol,
                Duration::from_secs(24 * 60 * 60),
                start_ns,
                end_ns,
            )],
            provider_membership_canary_fill_config(args)?,
            cancellation,
            |_| {},
        )
        .await;
    drop(client);
    match result {
        Ok(report) => {
            let item = report.symbols().first();
            let remote_used = item.is_some_and(|item| item.remote_used);
            let complete = report.status() == BacktestHistoryFillTerminalStatus::Complete
                && item
                    .is_some_and(|item| item.status == BacktestHistoryFillSymbolStatus::Complete);
            let error = item.and_then(|item| item.error.clone()).or_else(|| {
                (!remote_used).then(|| "canary did not use remote provider".to_string())
            });
            Ok(ProviderMembershipCanaryReport {
                symbol,
                healthy: complete && remote_used,
                remote_used,
                error,
            })
        }
        Err(error) => Ok(ProviderMembershipCanaryReport {
            symbol,
            healthy: false,
            remote_used: false,
            error: Some(error.to_string()),
        }),
    }
}

fn provider_membership_canary_target(
    acquisition: &tqsdk_data::HistoricalCatalogAcquisition,
) -> Result<(String, i64, i64), CliError> {
    let mut candidates = acquisition
        .contracts
        .iter()
        .filter_map(|contract| {
            acquisition
                .provider_daily_observations
                .get(&contract.symbol)
                .filter(|observation| {
                    observation.status == tqsdk_data::HistoricalDailyObservationStatus::Complete
                })
                .and_then(|observation| observation.first_row_ns)
                .filter(|first_row_ns| *first_row_ns < acquisition.requested_as_of_ns)
                .map(|first_row_ns| (contract.expired, contract.symbol.clone(), first_row_ns))
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then(left.2.cmp(&right.2))
            .then(left.1.cmp(&right.1))
    });
    candidates
        .into_iter()
        .next()
        .map(|(_, symbol, first_row_ns)| {
            let end_ns = first_row_ns
                .saturating_add(PROVIDER_MEMBERSHIP_CANARY_WINDOW_NS)
                .min(acquisition.requested_as_of_ns);
            (symbol, first_row_ns, end_ns)
        })
        .ok_or_else(|| {
            CliError::Usage(
                "provider membership refresh requires at least one complete native-daily observation for canary"
                    .to_string(),
            )
        })
}

fn provider_membership_retry_candidate_json(
    candidate: &tqsdk_data::ProviderDailyUnavailableRetryCandidate,
) -> Value {
    json!({
        "symbol": candidate.symbol,
        "expired": candidate.expired,
        "unavailable_after_ns": candidate.unavailable_after_ns,
        "attempts": candidate.retry.attempts,
        "next_retry_at_ns": candidate.retry.next_retry_at_ns,
    })
}

fn provider_membership_canary_json(canary: &ProviderMembershipCanaryReport) -> Value {
    json!({
        "symbol": canary.symbol,
        "healthy": canary.healthy,
        "remote_used": canary.remote_used,
        "error": canary.error,
    })
}

fn provider_membership_refresh_cancelled_outcome(
    cache_dir: &Path,
    market: MarketKind,
    acquisition: &tqsdk_data::HistoricalCatalogAcquisition,
    candidate_count: usize,
    due_count: usize,
    selected: Vec<Value>,
    canary: ProviderMembershipCanaryReport,
) -> CommandOutcome {
    CommandOutcome {
        value: json!({
            "schema_version": REPORT_SCHEMA_VERSION,
            "command": "refresh-provider-membership",
            "cache_kind": "daily",
            "market": market.as_str(),
            "cache_dir": cache_dir,
            "dry_run": false,
            "status": "cancelled",
            "complete": false,
            "acquisition_sha256": acquisition.acquisition_sha256,
            "candidate_count": candidate_count,
            "due_count": due_count,
            "selected_count": selected.len(),
            "selected": selected,
            "canary": provider_membership_canary_json(&canary),
            "retry_state_advanced": false,
        }),
        exit_code: 130,
    }
}

fn current_timestamp_ns() -> Result<i64, CliError> {
    chrono::Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| CliError::Usage("current timestamp exceeds nanosecond range".to_string()))
}

struct ProviderHistoryFillContext {
    canonical_cache_dir: PathBuf,
    kind: CacheKind,
    market: MarketKind,
    args: FillArgs,
    historical: ProviderHistoricalUniverseInput,
    resolved: ResolvedFillWindow,
    acquisition: tqsdk_data::HistoricalCatalogAcquisition,
    auth_user: String,
    auth_pass: String,
}

async fn bootstrap_provider_history_and_fill(
    context: ProviderHistoryFillContext,
) -> Result<CommandOutcome, CliError> {
    let ProviderHistoryFillContext {
        canonical_cache_dir,
        kind,
        market,
        mut args,
        historical,
        resolved,
        acquisition,
        auth_user,
        auth_pass,
    } = context;
    if !acquisition.complete {
        return Err(DataError::Validation(
            "provider roster changed during acquisition; retry historical universe fill"
                .to_string(),
        )
        .into());
    }

    let store = tqsdk_data::HistoricalUniverseArtifactStore::new(&canonical_cache_dir);
    let (acquisition, provider_unavailable, signal_context) = 'prepared: {
        if let Some(observed) =
            store.find_matching_provider_history_observed_acquisition(&acquisition)?
        {
            let provider_unavailable = provider_unavailable_from_observed_acquisition(&observed)?;
            break 'prepared (observed, provider_unavailable, None);
        }

        let bootstrap_start_ns = tqsdk_data::PROVIDER_DAILY_HISTORY_BOOTSTRAP_START_NS;
        let bootstrap_end_ns = resolved.window.end_ns;
        let client = BacktestHistoryClient::builder(canonical_cache_dir.clone())
            .policy(BacktestHistoryPolicy::RemoteOnMiss)
            .auth_env()
            .build()?;
        let requests = acquisition
            .contracts
            .iter()
            .enumerate()
            .map(|(index, contract)| {
                BacktestHistoryRequest::kline(
                    u64::try_from(index).expect("provider roster count fits request id"),
                    &contract.symbol,
                    Duration::from_secs(24 * 60 * 60),
                    bootstrap_start_ns,
                    bootstrap_end_ns,
                )
            })
            .collect::<Vec<_>>();
        let progress_session =
            FillProgressSession::new(args.progress, args.progress_max_bars, "daily-bootstrap");
        let reporter = progress_session.observer();
        reporter.planning("bootstrapping native daily history for provider roster");
        let cancellation = BacktestHistoryFillCancellation::new();
        let signal_task = spawn_shutdown_signal_handler(cancellation.clone(), CacheKind::Daily)?;
        let progress_callback = reporter.clone();
        // Keep every terminal outcome attributable to one symbol. Exact timeouts
        // remain bounded provider-unavailable audit facts for this acquisition.
        let mut bootstrap_config = history_fill_config(&args)?.with_symbol_batch_size(1)?;
        if args.batch_timeout_secs.is_none() {
            bootstrap_config =
                bootstrap_config.with_batch_timeout(Some(Duration::from_secs(15)))?;
        }
        let bootstrap = client
            .orchestrate_fill(
                requests,
                bootstrap_config,
                cancellation.clone(),
                move |event| progress_callback.observe_history_progress(&event),
            )
            .await;
        // A timed-out provider chart can keep the owning session unhealthy after the
        // scheduler returns. Release bootstrap sessions before roster refresh and target fill.
        drop(client);
        let bootstrap = bootstrap?;
        if bootstrap.status() == BacktestHistoryFillTerminalStatus::Interrupted {
            progress_session.finish(
                ProgressTerminalStatus::Interrupted,
                "native daily data-membership bootstrap was cancelled",
            );
            return Err(DataError::InvalidState("provider history preparation cancelled").into());
        }
        let blocking_failures = bootstrap
            .symbols()
            .iter()
            .filter(|item| {
                if item.status != BacktestHistoryFillSymbolStatus::Failed {
                    return false;
                }
                isolated_provider_history_unavailable_after_ns(
                    item.error.as_deref(),
                    bootstrap_config,
                )
                .is_none()
            })
            .collect::<Vec<_>>();
        if !blocking_failures.is_empty() {
            let sample = blocking_failures
                .iter()
                .take(8)
                .map(|item| item.symbol.as_str())
                .collect::<Vec<_>>()
                .join(",");
            progress_session.finish(
                ProgressTerminalStatus::Failed,
                "native daily bootstrap encountered non-timeout failures",
            );
            return Err(DataError::InvalidResponse(format!(
            "provider native-daily bootstrap has {} blocking failures (sample: {}); data-membership artifact was not published",
            blocking_failures.len(),
            sample
        ))
        .into());
        }
        // Exact scheduler timeouts remain acquisition audit facts. They are not
        // retried as absence proofs: a provider-unavailable chart says nothing
        // about listing or whether data may become observable in a later run.
        let provider_unavailable = bootstrap
            .symbols()
            .iter()
            .filter_map(|item| {
                (item.status == BacktestHistoryFillSymbolStatus::Failed)
                    .then(|| {
                        isolated_provider_history_unavailable_after_ns(
                            item.error.as_deref(),
                            bootstrap_config,
                        )
                        .map(|timeout_ns| (item.symbol.clone(), timeout_ns))
                    })
                    .flatten()
            })
            .collect::<BTreeMap<_, _>>();
        let unavailable_circuit_breaker =
            provider_history_unavailable_limit(bootstrap.symbols().len());
        let confirmed_complete = bootstrap.completed_symbols();
        if !provider_history_bootstrap_is_publishable(
            confirmed_complete,
            provider_unavailable.len(),
            bootstrap.symbols().len(),
        ) {
            progress_session.finish(
                ProgressTerminalStatus::Failed,
                "native daily bootstrap provider-unavailable circuit breaker opened",
            );
            return Err(DataError::InvalidResponse(format!(
            "provider native-daily bootstrap observed {} unavailable contracts out of {}; limit {}; data-membership artifact was not published",
            provider_unavailable.len(),
            bootstrap.symbols().len(),
            unavailable_circuit_breaker
        ))
        .into());
        }
        progress_session.finish(
        ProgressTerminalStatus::Complete,
        format!(
            "native daily bootstrap complete; deriving data membership ({} bounded provider-unavailable candidates)",
            provider_unavailable.len()
        ),
    );

        if cancellation.is_cancelled() {
            return Err(DataError::InvalidState("provider history preparation cancelled").into());
        }
        let completed_at_ns = chrono::Utc::now().timestamp_nanos_opt().ok_or_else(|| {
            CliError::Usage("current timestamp exceeds nanosecond range".to_string())
        })?;
        let refreshed_discovery =
            tqsdk_data::session_client_builder_for_futures_discovery(&auth_user, &auth_pass)
                .build()
                .map_err(tqsdk::Error::from)?;
        let refreshed_discovery =
            tqsdk_data::ProviderCurrentHistoricalCatalogAcquirer::new(refreshed_discovery)
                .acquire(bootstrap_end_ns, completed_at_ns)
                .await?;
        let refreshed = historical.scope_provider_current_bootstrap(&refreshed_discovery)?;
        if refreshed.acquisition_sha256 != refreshed_discovery.acquisition_sha256 {
            store.publish_acquisition(&refreshed_discovery)?;
        }
        store.publish_acquisition(&refreshed)?;
        if !refreshed.complete
            || acquisition.roster_after != refreshed.roster_after
            || acquisition.contracts != refreshed.contracts
        {
            return Err(DataError::Validation(
            "provider roster or metadata changed during daily bootstrap; retry historical universe fill"
                .to_string(),
        )
        .into());
        }
        let acquisition = refreshed;

        let daily_cache = DailyKlineCache::open_read_only(&canonical_cache_dir);
        let mut observations = BTreeMap::new();
        for contract in &acquisition.contracts {
            if let Some(unavailable_after_ns) = provider_unavailable.get(&contract.symbol) {
                observations.insert(
                    contract.symbol.clone(),
                    tqsdk_data::HistoricalDailyObservation::provider_unavailable(
                        bootstrap_start_ns,
                        bootstrap_end_ns,
                        *unavailable_after_ns,
                    )?,
                );
                continue;
            }
            let snapshot = daily_cache_snapshot_for_symbol(&canonical_cache_dir, &contract.symbol)?;
            let origin = daily_cache
                .read_range(
                    &contract.symbol,
                    bootstrap_start_ns,
                    bootstrap_end_ns,
                    &snapshot,
                )?
                .first()
                .map(|row| row.datetime);
            observations.insert(
                contract.symbol.clone(),
                tqsdk_data::HistoricalDailyObservation::new(
                    bootstrap_start_ns,
                    bootstrap_end_ns,
                    origin,
                )?,
            );
        }

        let acquisition =
            tqsdk_data::promote_provider_daily_history_observations(acquisition, observations)?;
        break 'prepared (
            acquisition,
            provider_unavailable,
            Some((cancellation, signal_task)),
        );
    };

    let semantic = tqsdk_data::HistoricalSemanticCatalog::from_provider_history_observed(
        &acquisition,
        tqsdk_data::PROVIDER_DAILY_MEMBERSHIP_CALENDAR_IDENTITY,
    )?;
    let bootstrap_retry_state = signal_context
        .as_ref()
        .map(|_| tqsdk_data::ProviderDailyUnavailableRetryState::from_acquisition(&acquisition))
        .transpose()?;
    let contract_count = semantic.catalog.contracts.len().max(1);
    let budget = tqsdk_data::UniverseBudget::new(
        contract_count.saturating_mul(4),
        contract_count.saturating_mul(8),
    )?;
    let compiled_plan = match historical {
        ProviderHistoricalUniverseInput::Legacy(historical) => {
            let spec = match historical {
                tqsdk_data::HistoricalFillUniverseSpec::ObservedPhysicalAll => {
                    tqsdk_data::HistoricalFillUniverseSpec::parse("timeline(active:all)")?
                }
                timeline @ tqsdk_data::HistoricalFillUniverseSpec::Timeline(_) => timeline,
            };
            let resolution = tqsdk_data::compile_historical_universe_resolution(
                &acquisition,
                &semantic,
                &spec,
                resolved.window.start_ns,
                resolved.window.end_ns,
                budget,
            )?;
            CompiledProviderHistoricalPlan::Legacy(Box::new(resolution.plan))
        }
        ProviderHistoricalUniverseInput::V2(input) => {
            let spec = input
                .spec()
                .expect("historical V2 input always has a specification");
            let resolution = tqsdk_data::compile_historical_universe_resolution_v4(
                &(&acquisition, &semantic),
                spec,
                input.expanded_symbols(),
                resolved.window.start_ns,
                resolved.window.end_ns,
                budget,
                None,
            )
            .map_err(|error| DataError::Validation(error.to_string()))?
            .with_input_sources_sha256(input.input_sources_sha256().map(str::to_owned));
            let plan = resolution
                .prepare_plan()
                .map_err(|error| DataError::Validation(error.to_string()))?;
            CompiledProviderHistoricalPlan::V5(Box::new(plan))
        }
    };
    if signal_context
        .as_ref()
        .is_some_and(|(cancellation, _)| cancellation.is_cancelled())
    {
        return Err(DataError::InvalidState("provider history preparation cancelled").into());
    }
    store.publish_acquisition(&acquisition)?;
    if let Some(state) = &bootstrap_retry_state {
        if !state.is_empty() {
            store.publish_provider_daily_retry_state(state)?;
        }
    }
    if signal_context
        .as_ref()
        .is_some_and(|(cancellation, _)| cancellation.is_cancelled())
    {
        return Err(DataError::InvalidState("provider history preparation cancelled").into());
    }
    store.publish_semantic_catalog(&semantic)?;
    if signal_context
        .as_ref()
        .is_some_and(|(cancellation, _)| cancellation.is_cancelled())
    {
        return Err(DataError::InvalidState("provider history preparation cancelled").into());
    }
    let (plan_path, artifact_report) = match compiled_plan {
        CompiledProviderHistoricalPlan::Legacy(plan) => {
            let plan_path = store.publish_plan(&plan)?;
            store.verify_plan_artifact_chain(&plan)?;
            (plan_path, None)
        }
        CompiledProviderHistoricalPlan::V5(plan) => {
            let plan_path = store.publish_current_plan(&plan)?;
            store.verify_current_plan_artifact_chain(&plan)?;
            let report = json!({
                "plan_version": plan.plan_version(),
                "plan_sha256": plan.plan_sha256(),
                "plan_path": plan_path,
                "normalized_ast_sha256": plan.identity().normalized_ast_sha256(),
                "input_sources_sha256": plan.identity().input_sources_sha256(),
            });
            (plan_path, Some(report))
        }
    };

    args.days.start_day = None;
    args.days.end_day = None;
    args.days.last_trading_days = None;
    args.days.calendar = CalendarMode::Auto;
    args.days.refresh_calendar = false;
    let report_path = args.report.clone();
    let mut outcome = fill_historical_universe_plan(
        Some(&canonical_cache_dir),
        kind,
        market,
        args,
        plan_path,
        signal_context,
    )
    .await?;
    if let Some(object) = outcome.value.as_object_mut() {
        if let Some(artifact_report) = artifact_report {
            object.insert("historical_universe_artifacts".to_string(), artifact_report);
        }
        object.insert(
            "provider_daily_membership".to_string(),
            json!({
                "observed_candidates": acquisition.contracts.len(),
                "data_members": semantic.catalog.contracts.len(),
                "provider_unavailable": provider_unavailable.len(),
                "provider_unavailable_sample": provider_unavailable.keys().take(16).collect::<Vec<_>>(),
                "membership_start": "first_native_daily_row",
            }),
        );
    }
    if let Some(path) = report_path {
        write_atomically(&path, &serde_json::to_vec_pretty(&outcome.value)?)?;
    }
    Ok(outcome)
}

fn historical_data_kind(kind: CacheKind) -> tqsdk_data::HistoricalDataKind {
    match kind {
        CacheKind::Tick => tqsdk_data::HistoricalDataKind::Tick,
        CacheKind::Minute => tqsdk_data::HistoricalDataKind::Minute,
        CacheKind::Daily => tqsdk_data::HistoricalDataKind::Daily,
        CacheKind::All => unreachable!("historical universe kind gate rejects all"),
    }
}

fn isolated_provider_history_unavailable_after_ns(
    error: Option<&str>,
    config: BacktestHistoryFillConfig,
) -> Option<u64> {
    let message = error?;
    let timeout = if message.starts_with("history fill batch made no progress for ") {
        config.idle_timeout()
    } else if message.starts_with("history fill batch exceeded ") {
        config.batch_timeout()?
    } else {
        return None;
    };
    u64::try_from(timeout.as_nanos())
        .ok()
        .filter(|value| *value > 0)
}

fn provider_history_unavailable_limit(roster_len: usize) -> usize {
    roster_len.div_ceil(20).max(8)
}

fn provider_history_bootstrap_is_publishable(
    completed: usize,
    unavailable: usize,
    roster_len: usize,
) -> bool {
    completed > 0 && unavailable <= provider_history_unavailable_limit(roster_len)
}

fn provider_unavailable_from_observed_acquisition(
    acquisition: &tqsdk_data::HistoricalCatalogAcquisition,
) -> Result<BTreeMap<String, u64>, CliError> {
    if acquisition.proof != tqsdk_data::HistoricalCatalogProof::ProviderHistoryObserved {
        return Err(DataError::Validation(
            "provider-history observation reuse requires provider_history_observed proof"
                .to_string(),
        )
        .into());
    }

    let mut provider_unavailable = BTreeMap::new();
    for (symbol, observation) in &acquisition.provider_daily_observations {
        if observation.status != tqsdk_data::HistoricalDailyObservationStatus::ProviderUnavailable {
            continue;
        }
        let unavailable_after_ns = observation.provider_unavailable_after_ns.ok_or_else(|| {
            DataError::Validation(format!(
                "provider-history observation for {symbol} omits unavailable timeout"
            ))
        })?;
        provider_unavailable.insert(symbol.clone(), unavailable_after_ns);
    }
    Ok(provider_unavailable)
}

#[derive(Debug)]
struct HistoricalUniverseFillPlan {
    plan_version: u32,
    plan_sha256: String,
    catalog_id: String,
    start_ns: i64,
    end_ns: i64,
    targets: Vec<tqsdk_data::HistoricalUniverseFillTarget>,
    legacy_unproven: bool,
}

fn load_historical_universe_fill_plan(
    store: &tqsdk_data::HistoricalUniverseArtifactStore,
    plan_path: &Path,
    kind: tqsdk_data::HistoricalDataKind,
    allow_legacy: bool,
) -> Result<HistoricalUniverseFillPlan, CliError> {
    let bytes = fs::read(plan_path)?;
    let plan_version = serde_json::from_slice::<Value>(&bytes)?
        .get("plan_version")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            CliError::Usage("historical universe plan lacks plan_version".to_string())
        })?;
    match plan_version {
        5 => {
            let plan: tqsdk_data::HistoricalUniversePlanV5 = serde_json::from_slice(&bytes)?;
            if plan.canonical_json_bytes()? != bytes {
                return Err(CliError::Usage(
                    "historical universe plan does not use canonical V5 JSON".to_string(),
                ));
            }
            store.verify_current_plan_artifact_chain(&plan)?;
            let targets = plan
                .execution()
                .targets()
                .get(&kind)
                .ok_or_else(|| {
                    CliError::Usage(format!(
                        "historical universe plan lacks pinned {kind:?} targets"
                    ))
                })?
                .iter()
                .map(|target| tqsdk_data::HistoricalUniverseFillTarget {
                    symbol: target.source_symbol.clone(),
                    start_ns: target.start_ns,
                    end_ns: target.end_ns,
                })
                .collect();
            Ok(HistoricalUniverseFillPlan {
                plan_version: plan.plan_version(),
                plan_sha256: plan.plan_sha256().to_string(),
                catalog_id: plan.timeline().catalog_id.clone(),
                start_ns: plan.timeline().start_ns,
                end_ns: plan.timeline().end_ns,
                targets,
                legacy_unproven: false,
            })
        }
        1..=3 => {
            let plan: tqsdk_data::HistoricalUniversePlan = serde_json::from_slice(&bytes)?;
            store.verify_plan_artifact_chain(&plan)?;
            let (targets, legacy_unproven) =
                historical_universe_fill_targets(&plan, kind, allow_legacy)?;
            Ok(HistoricalUniverseFillPlan {
                plan_version: plan.plan_version,
                plan_sha256: plan.plan_sha256,
                catalog_id: plan.timeline.catalog_id,
                start_ns: plan.timeline.start_ns,
                end_ns: plan.timeline.end_ns,
                targets,
                legacy_unproven,
            })
        }
        4 => Err(CliError::Usage(
            "historical universe V4 plan must be migrated to V5 before filling".to_string(),
        )),
        version => Err(CliError::Usage(format!(
            "unsupported historical universe plan version {version}"
        ))),
    }
}

async fn fill_historical_universe_plan(
    cache_dir: Option<&Path>,
    kind: CacheKind,
    market: MarketKind,
    args: FillArgs,
    plan_path: PathBuf,
    signal_context: Option<(BacktestHistoryFillCancellation, tokio::task::JoinHandle<()>)>,
) -> Result<CommandOutcome, CliError> {
    let (_, preflight_cache_dir) = open_read_only_cache(cache_dir)?;
    let preflight_store = tqsdk_data::HistoricalUniverseArtifactStore::new(preflight_cache_dir);
    let plan = load_historical_universe_fill_plan(
        &preflight_store,
        &plan_path,
        historical_data_kind(kind),
        args.allow_legacy_universe_plan,
    )?;

    if !matches!(market, MarketKind::Futures) {
        return Err(CliError::Usage(
            "legacy --universe-plan supports only --market futures".to_string(),
        ));
    }
    if !args.symbols.symbols.is_empty()
        || args.days.start_day.is_some()
        || args.days.end_day.is_some()
        || args.days.last_trading_days.is_some()
        || !matches!(args.days.calendar, CalendarMode::Auto)
        || args.days.refresh_calendar
        || args.include_open_day
        || args.require_final
        || args.repair_stale
        || args.daily_slices
    {
        return Err(CliError::Usage(
            "legacy --universe-plan supplies exact source ranges; omit symbol, trading-day, calendar, open-day, repair, and slicing flags"
                .to_string(),
        ));
    }
    if args.dry_run && args.report.is_some() {
        return Err(CliError::Usage(
            "--report cannot be combined with --dry-run because dry-run performs no writes"
                .to_string(),
        ));
    }

    let targets = plan.targets.clone();
    let legacy_unproven = plan.legacy_unproven;
    if targets.is_empty() {
        return Err(CliError::Usage(
            "historical universe plan resolves no physical fill targets".to_string(),
        ));
    }
    let (_, canonical_cache_dir) = open_read_only_cache(cache_dir)?;
    let mut builder =
        BacktestHistoryClient::builder(canonical_cache_dir.clone()).policy(if args.dry_run {
            BacktestHistoryPolicy::CacheOnly
        } else {
            BacktestHistoryPolicy::RemoteOnMiss
        });
    if !args.dry_run {
        builder = builder.auth_env();
    }
    let client = builder.build()?;
    let requests = targets
        .iter()
        .enumerate()
        .map(|(index, target)| {
            let request_id = u64::try_from(index).expect("symbol count fits request identifier");
            match kind {
                CacheKind::Tick => BacktestHistoryRequest::tick(
                    request_id,
                    &target.symbol,
                    target.start_ns,
                    target.end_ns,
                ),
                CacheKind::Minute => BacktestHistoryRequest::kline(
                    request_id,
                    &target.symbol,
                    Duration::from_secs(60),
                    target.start_ns,
                    target.end_ns,
                ),
                CacheKind::Daily => BacktestHistoryRequest::kline(
                    request_id,
                    &target.symbol,
                    Duration::from_secs(24 * 60 * 60),
                    target.start_ns,
                    target.end_ns,
                ),
                CacheKind::All => unreachable!("historical plan kind gate rejects all"),
            }
        })
        .collect::<Vec<_>>();

    let progress_session =
        FillProgressSession::new(args.progress, args.progress_max_bars, kind.as_str());
    let reporter = progress_session.observer();
    reporter.planning("validating pinned historical plan and materializing exact source ranges");
    let (cancellation, signal_task) = match signal_context {
        Some(context) => context,
        None => {
            let cancellation = BacktestHistoryFillCancellation::new();
            let signal_task = spawn_shutdown_signal_handler(cancellation.clone(), kind)?;
            (cancellation, signal_task)
        }
    };
    let progress_callback = reporter.clone();
    let fill_result = client
        .orchestrate_fill(
            requests.clone(),
            history_fill_config(&args)?,
            cancellation,
            move |event| progress_callback.observe_history_progress(&event),
        )
        .await;
    signal_task.abort();
    let _ = signal_task.await;

    let report = match fill_result {
        Ok(report) => report,
        Err(error) => {
            progress_session.finish(
                ProgressTerminalStatus::Failed,
                "historical universe fill failed; incomplete ranges were not committed",
            );
            if let Some(path) = &args.report {
                let value = json!({
                    "schema_version": REPORT_SCHEMA_VERSION,
                    "command": "fill",
                    "cache_kind": kind.as_str(),
                    "market": market.as_str(),
                    "cache_dir": canonical_cache_dir,
                    "status": "failed",
                    "complete": false,
                    "legacy_unproven": legacy_unproven,
                    "error": error.to_string(),
                    "universe_plan": {
                        "path": plan_path,
                    "plan_sha256": plan.plan_sha256,
                    "catalog_id": plan.catalog_id,
                    },
                });
                write_atomically(path, &serde_json::to_vec_pretty(&value)?)?;
            }
            return Err(error.into());
        }
    };
    let (status, terminal, exit_code) = match report.status() {
        BacktestHistoryFillTerminalStatus::Complete => {
            ("complete", ProgressTerminalStatus::Complete, 0)
        }
        BacktestHistoryFillTerminalStatus::Interrupted => {
            ("interrupted", ProgressTerminalStatus::Interrupted, 130)
        }
        BacktestHistoryFillTerminalStatus::Failed => ("failed", ProgressTerminalStatus::Failed, 1),
    };
    progress_session.finish(
        terminal,
        if exit_code == 0 {
            "historical universe fill complete; terminal coverage verified"
        } else if exit_code == 130 {
            "historical universe fill interrupted; accepted rows were flushed"
        } else {
            "historical universe fill finished with failed source ranges"
        },
    );
    let symbol_reports = report
        .symbols()
        .iter()
        .map(|item| {
            let item_status = match item.status {
                BacktestHistoryFillSymbolStatus::Complete => "complete",
                BacktestHistoryFillSymbolStatus::Interrupted => "interrupted",
                BacktestHistoryFillSymbolStatus::Failed => "failed",
            };
            json!({
                "symbol": item.symbol,
                "requested_range": item.requested_range,
                "status": item_status,
                "rows_written": item.rows_written,
                "remote_used": item.remote_used,
                "remote_filled_ranges": item.remote_filled_ranges,
                "error": item.error,
            })
        })
        .collect::<Vec<_>>();
    let universe_timeline = json!({
        "path": plan_path,
        "plan_version": plan.plan_version,
        "plan_sha256": plan.plan_sha256,
        "catalog_id": plan.catalog_id,
        "start_ns": plan.start_ns,
        "end_ns": plan.end_ns,
        "physical_symbols": requests.len(),
        "target_count": requests.len(),
    });
    let value = json!({
        "schema_version": REPORT_SCHEMA_VERSION,
        "command": "fill",
        "cache_kind": kind.as_str(),
        "market": market.as_str(),
        "cache_dir": canonical_cache_dir,
        "dry_run": args.dry_run,
        "status": status,
        "complete": exit_code == 0,
        "legacy_unproven": legacy_unproven,
        "remote_used": report.symbols().iter().any(|item| item.remote_used),
        "rows_written": report.rows_written(),
        "plan_sha256": plan.plan_sha256,
        "symbols_warmed": requests.len(),
        "universe_timeline": universe_timeline,
        "symbols": symbol_reports,
    });
    if let Some(path) = &args.report {
        write_atomically(path, &serde_json::to_vec_pretty(&value)?)?;
    }
    Ok(CommandOutcome { value, exit_code })
}

fn historical_universe_fill_targets(
    plan: &tqsdk_data::HistoricalUniversePlan,
    kind: tqsdk_data::HistoricalDataKind,
    allow_legacy: bool,
) -> Result<(Vec<tqsdk_data::HistoricalUniverseFillTarget>, bool), DataError> {
    plan.verify()?;
    match plan.plan_version {
        3 => {
            let execution = plan.v3_execution.as_ref().ok_or_else(|| {
                DataError::Validation(
                    "historical universe plan v3 lacks pinned execution targets".to_string(),
                )
            })?;
            let targets = execution.targets.get(&kind).ok_or_else(|| {
                DataError::Validation(format!(
                    "historical universe plan v3 lacks pinned {kind:?} targets"
                ))
            })?;
            Ok((
                targets
                    .iter()
                    .map(|target| tqsdk_data::HistoricalUniverseFillTarget {
                        symbol: target.source_symbol.clone(),
                        start_ns: target.start_ns,
                        end_ns: target.end_ns,
                    })
                    .collect(),
                false,
            ))
        }
        2 if allow_legacy => Ok((plan.physical_fill_targets()?, true)),
        2 => Err(DataError::Validation(
            "legacy historical universe plan v2 is unproven; pass --allow-legacy-universe-plan to opt in"
                .to_string(),
        )),
        version => Err(DataError::Validation(format!(
            "historical universe fill requires plan v3; version {version} is unsupported"
        ))),
    }
}

async fn fill_daily(
    cache_dir: Option<&Path>,
    market: MarketKind,
    args: FillArgs,
) -> Result<CommandOutcome, CliError> {
    let fill_config = history_fill_config(&args)?;
    if !matches!(market, MarketKind::Futures) {
        return Err(CliError::Usage(
            "--kind daily fill supports only --market futures".to_string(),
        ));
    }
    if args.repair_stale {
        return Err(CliError::Usage(
            "--repair-stale is supported only for --kind minute fill".to_string(),
        ));
    }
    if args.include_open_day {
        return Err(CliError::Usage(
            "--include-open-day is not supported for --kind daily; daily coverage is final-only"
                .to_string(),
        ));
    }
    if args.daily_slices {
        return Err(CliError::Usage(
            "--daily-slices is not supported for --kind daily; native daily fill requests the exact missing range"
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
    let (_, canonical_cache_dir) = open_read_only_cache(cache_dir)?;
    let mut resolved =
        resolve_fill_window(&canonical_cache_dir, &args.days, args.dry_run, false).await?;
    persist_calendar_if_needed(
        canonical_cache_dir.as_path(),
        &mut resolved.calendar,
        args.dry_run,
    )?;
    debug_assert!(resolved.provisional.is_none());
    let window = resolved.window;
    let symbols = resolve_minute_fill_symbols(explicit_symbols, universe.as_deref()).await?;

    if args.dry_run {
        let before = daily_cache_statuses(&canonical_cache_dir, &symbols, &window)?;
        let complete = before
            .iter()
            .all(tqsdk_data::DailyKlineCacheStatus::is_complete);
        return Ok(CommandOutcome {
            value: json!({
                "schema_version": REPORT_SCHEMA_VERSION,
                "command": "fill",
                "cache_kind": "daily",
                "market": market.as_str(),
                "cache_dir": canonical_cache_dir,
                "dry_run": true,
                "report_path": Value::Null,
                "requested_days": window,
                "symbols": before.iter().map(daily_cache_status_json).collect::<Vec<_>>(),
                "complete": complete,
                "remote_used": false,
                "rows_written": 0,
            }),
            exit_code: if complete { 0 } else { 1 },
        });
    }

    let builder = BacktestHistoryClient::builder(canonical_cache_dir.clone())
        .policy(BacktestHistoryPolicy::RemoteOnMiss)
        .auth_env();
    let client = builder.build()?;
    let progress_session = FillProgressSession::new(args.progress, args.progress_max_bars, "daily");
    let reporter = progress_session.observer();
    reporter.planning("materializing final native daily coverage");
    let report_path = args
        .report
        .clone()
        .unwrap_or_else(|| default_daily_fill_report_path(&canonical_cache_dir));
    let requests = symbols
        .iter()
        .enumerate()
        .map(|(index, symbol)| {
            BacktestHistoryRequest::kline(
                u64::try_from(index).expect("symbol count fits request identifier"),
                symbol,
                Duration::from_secs(24 * 60 * 60),
                window.start_ns,
                window.end_ns,
            )
        })
        .collect::<Vec<_>>();
    let cancellation = BacktestHistoryFillCancellation::new();
    let signal_cancellation = cancellation.clone();
    let signal_task = spawn_shutdown_signal_handler(signal_cancellation, CacheKind::Daily)?;
    let progress_callback = reporter.clone();
    let report = match client
        .orchestrate_fill(requests, fill_config, cancellation.clone(), move |event| {
            progress_callback.observe_history_progress(&event);
        })
        .await
    {
        Ok(report) => report,
        Err(error) => {
            signal_task.abort();
            let _ = signal_task.await;
            let failed_report = UnifiedFillReport::from_planned_terminal(
                "daily",
                &canonical_cache_dir,
                window.clone(),
                market.as_str(),
                &symbols,
                UnifiedFillReportStatus::Failed,
                Some(error.to_string()),
            );
            write_unified_fill_report(&report_path, &failed_report)?;
            progress_session.finish(
                ProgressTerminalStatus::Failed,
                "daily fill failed; final coverage was not committed for failed ranges",
            );
            return Err(error.into());
        }
    };
    signal_task.abort();
    let _ = signal_task.await;
    let after = daily_cache_statuses(&canonical_cache_dir, &symbols, &window)?;
    let coverage_complete = after
        .iter()
        .all(tqsdk_data::DailyKlineCacheStatus::is_complete);
    let rows_written = report.rows_written();
    let remote_used = report.symbols().iter().any(|item| item.remote_used);
    let interrupted = matches!(
        report.status(),
        BacktestHistoryFillTerminalStatus::Interrupted
    );
    let complete =
        coverage_complete && matches!(report.status(), BacktestHistoryFillTerminalStatus::Complete);
    progress_session.finish(
        match report.status() {
            BacktestHistoryFillTerminalStatus::Complete if complete => {
                ProgressTerminalStatus::Complete
            }
            BacktestHistoryFillTerminalStatus::Interrupted => ProgressTerminalStatus::Interrupted,
            BacktestHistoryFillTerminalStatus::Complete
            | BacktestHistoryFillTerminalStatus::Failed => ProgressTerminalStatus::Failed,
        },
        if interrupted {
            "daily fill interrupted; accepted rows were flushed without committing failed ranges"
        } else if complete {
            "daily fill complete; final native daily coverage verified"
        } else {
            "daily fill completed with missing native daily coverage"
        },
    );
    let daily_report = UnifiedFillReport::from_history_fill(
        "daily",
        &canonical_cache_dir,
        window.clone(),
        market.as_str(),
        &report,
    );
    write_unified_fill_report(&report_path, &daily_report)?;

    Ok(CommandOutcome {
        value: json!({
            "schema_version": REPORT_SCHEMA_VERSION,
            "command": "fill",
            "cache_kind": "daily",
            "market": market.as_str(),
            "cache_dir": canonical_cache_dir,
            "dry_run": false,
            "report_path": report_path,
            "requested_days": window,
            "symbols": after.iter().map(daily_cache_status_json).collect::<Vec<_>>(),
            "complete": complete,
            "remote_used": remote_used,
            "rows_written": rows_written,
            "report": daily_report,
        }),
        exit_code: if interrupted {
            130
        } else if complete {
            0
        } else {
            1
        },
    })
}

fn daily_cache_statuses(
    cache_dir: &Path,
    symbols: &[String],
    window: &TradingDayWindow,
) -> Result<Vec<tqsdk_data::DailyKlineCacheStatus>, CliError> {
    let cache = DailyKlineCache::open_read_only(cache_dir);
    symbols
        .iter()
        .map(|symbol| {
            let snapshot = daily_cache_snapshot_for_symbol(cache_dir, symbol)?;
            cache.inspect(symbol, window.start_ns, window.end_ns, &snapshot)
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
}

async fn fill_minute(
    cache_dir: Option<&Path>,
    market: MarketKind,
    args: FillArgs,
) -> Result<CommandOutcome, CliError> {
    let config = fill_config(&args);
    if args.repair_stale && args.dry_run {
        return Err(CliError::Usage(
            "--repair-stale cannot be used with --dry-run because dry-run never removes cache partitions"
                .to_string(),
        ));
    }
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
    let mut resolved =
        resolve_fill_window(&canonical_cache_dir, &args.days, args.dry_run, false).await?;
    persist_calendar_if_needed(
        canonical_cache_dir.as_path(),
        &mut resolved.calendar,
        args.dry_run,
    )?;
    debug_assert!(resolved.provisional.is_none());
    let calendar_report = resolved.calendar.report_calendar();
    let window = resolved.window;
    let selector_symbols = explicit_symbols.clone();
    let symbols = resolve_minute_fill_symbols(explicit_symbols, universe.as_deref()).await?;
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
            calendar_report,
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
    if args.repair_stale {
        reporter.planning(
            "checking explicitly requested stale canonical-minute partitions under remote-fill lock",
        );
    }
    let report_path = args
        .report
        .clone()
        .unwrap_or_else(|| default_minute_fill_report_path(&canonical_cache_dir));
    let cancellation = BacktestRemoteFillCancellation::new();
    let signal_cancellation = cancellation.clone();
    let signal_task = spawn_shutdown_signal_handler(signal_cancellation, CacheKind::Minute)?;
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
    if args.repair_stale {
        builder = builder.repair_stale_minute_partitions();
    }
    if let Some(wait_secs) = args.lock_wait_secs {
        builder = builder.remote_fill_lock_wait(Duration::from_secs(wait_secs));
    }
    for symbol in &symbols {
        builder = builder.kline(symbol, Duration::from_secs(60), 1)?;
    }
    let warmup = builder.warmup().await;
    signal_task.abort();
    let _ = signal_task.await;
    if fill_was_interrupted(cancellation.is_cancelled(), warmup.is_ok()) {
        let interrupted_report = UnifiedFillReport::from_planned_terminal(
            "minute",
            &canonical_cache_dir,
            window.clone(),
            market.as_str(),
            &symbols,
            UnifiedFillReportStatus::Interrupted,
            Some("cancelled".to_string()),
        );
        write_unified_fill_report(&report_path, &interrupted_report)?;
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
            let failed_report = UnifiedFillReport::from_planned_terminal(
                "minute",
                &canonical_cache_dir,
                window.clone(),
                market.as_str(),
                &symbols,
                UnifiedFillReportStatus::Failed,
                Some(error.to_string()),
            );
            write_unified_fill_report(&report_path, &failed_report)?;
            progress_session.finish(
                ProgressTerminalStatus::Failed,
                "minute fill failed; final coverage was not committed for failed ranges",
            );
            return Err(error.into());
        }
    };
    let repaired_stale_partitions = warmup.stale_minute_partitions_repaired;
    let report = MinuteFillReport::from_warmup(
        &warmup,
        canonical_cache_dir.as_path(),
        window,
        market.as_str(),
        false,
    )
    .with_selector(selector)
    .with_calendar(calendar_report);
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
    let persisted_report = UnifiedFillReport::from_minute_fill(&report);
    write_unified_fill_report(&report_path, &persisted_report)?;
    Ok(CommandOutcome {
        value: json!({
            "schema_version": REPORT_SCHEMA_VERSION,
            "command": "fill",
            "cache_kind": "minute",
            "market": market.as_str(),
            "cache_dir": canonical_cache_dir,
            "dry_run": false,
            "repaired_stale_partitions": repaired_stale_partitions,
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

fn daily_cache_snapshot_for_symbol(
    cache_dir: &Path,
    symbol: &str,
) -> Result<MinuteKlineCacheSnapshot, DataError> {
    DailyKlineCache::open_read_only(cache_dir)
        .stored_snapshot(symbol)?
        .map_or_else(|| Ok(MinuteKlineCacheSnapshot::cst_v1()), Ok)
}

async fn resolve_minute_fill_symbols(
    explicit_symbols: Vec<String>,
    universe: Option<&str>,
) -> Result<Vec<String>, CliError> {
    let mut symbols = explicit_symbols.into_iter().collect::<BTreeSet<_>>();
    let Some(universe) = universe else {
        return Ok(symbols.into_iter().collect());
    };
    let expression = tqsdk_data::UniverseExpression::parse(universe)?;
    if expression.is_static_symbol_only() {
        symbols.extend(tqsdk_data::resolve_static_symbols_with_expression(
            &expression,
        )?);
    } else {
        let user = std::env::var("TQ_AUTH_USER").map_err(|_| {
            CliError::Usage(
                "dynamic futures universe requires TQ_AUTH_USER and TQ_AUTH_PASS".to_string(),
            )
        })?;
        let pass = std::env::var("TQ_AUTH_PASS").map_err(|_| {
            CliError::Usage(
                "dynamic futures universe requires TQ_AUTH_USER and TQ_AUTH_PASS".to_string(),
            )
        })?;
        let discovery = tqsdk_data::session_client_builder_for_futures_discovery(&user, &pass)
            .build()
            .map_err(tqsdk::Error::from)?;
        let mut resolver = tqsdk_data::SessionFuturesUniverseResolver::new(discovery);
        if tqsdk_data::expression_requires_activity_quotes(&expression) {
            let activity = SessionClientBuilder::new(user, pass)
                .futures_market()
                .build()
                .map_err(tqsdk::Error::from)?;
            resolver = resolver.with_activity_client(activity);
        }
        symbols.extend(
            tqsdk_data::resolve_futures_universe_symbols(&expression, &mut resolver).await?,
        );
    }
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
    calendar: FillReportCalendar,
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
        calendar: Some(calendar),
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
    let mut resolved = resolve_fill_window(
        &canonical_cache_dir,
        &args.days,
        args.dry_run,
        allow_open_day,
    )
    .await?;
    persist_calendar_if_needed(
        canonical_cache_dir.as_path(),
        &mut resolved.calendar,
        args.dry_run,
    )?;
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
    let report_path = args
        .report
        .clone()
        .unwrap_or_else(|| default_fill_report_path(&canonical_cache_dir));
    let cancellation = BacktestRemoteFillCancellation::new();
    let signal_cancellation = cancellation.clone();
    let signal_task = spawn_shutdown_signal_handler(signal_cancellation, CacheKind::Tick)?;
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
    if fill_was_interrupted(cancellation.is_cancelled(), warmup.is_ok()) {
        signal_task.abort();
        let _ = signal_task.await;
        calendar_task.abort();
        let _ = calendar_task.await;
        let summary = "interrupted; partial accepted rows were flushed without committing final or provisional coverage";
        let interrupted_report = UnifiedFillReport::from_planned_terminal(
            "tick",
            &canonical_cache_dir,
            window.clone(),
            "futures",
            &symbols,
            UnifiedFillReportStatus::Interrupted,
            Some("cancelled".to_string()),
        );
        write_unified_fill_report(&report_path, &interrupted_report)?;
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
    // Keep the second-signal hard-exit path alive while calendar preparation
    // settles after a successful warmup.
    let calendar_result = calendar_task.await;
    signal_task.abort();
    let _ = signal_task.await;
    let calendar = match calendar_result {
        Ok(Ok(calendar)) => calendar,
        Ok(Err(error)) => {
            let failed_report = UnifiedFillReport::from_planned_terminal(
                "tick",
                &canonical_cache_dir,
                window.clone(),
                "futures",
                &symbols,
                UnifiedFillReportStatus::Failed,
                Some(error.to_string()),
            );
            write_unified_fill_report(&report_path, &failed_report)?;
            progress_session.finish(
                ProgressTerminalStatus::Failed,
                "fill failed; calendar preparation did not complete",
            );
            return Err(error);
        }
        Err(error) => {
            let failed_report = UnifiedFillReport::from_planned_terminal(
                "tick",
                &canonical_cache_dir,
                window.clone(),
                "futures",
                &symbols,
                UnifiedFillReportStatus::Failed,
                Some(error.to_string()),
            );
            write_unified_fill_report(&report_path, &failed_report)?;
            progress_session.finish(
                ProgressTerminalStatus::Failed,
                "fill failed; calendar planning task did not complete",
            );
            return Err(CliError::Usage(format!(
                "calendar planning task failed: {error}"
            )));
        }
    };

    let warmup = match warmup {
        Ok(warmup) => warmup,
        Err(error) => {
            let failed_report = UnifiedFillReport::from_planned_terminal(
                "tick",
                &canonical_cache_dir,
                window.clone(),
                "futures",
                &symbols,
                UnifiedFillReportStatus::Failed,
                Some(error.to_string()),
            );
            write_unified_fill_report(&report_path, &failed_report)?;
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
    let persisted_report = UnifiedFillReport::from_tick_fill(&report);
    write_unified_fill_report(&report_path, &persisted_report)?;
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
        CacheKind::Daily => verify_daily(cache_dir, args).await,
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
            let (report_root, window, symbols) = match read_persisted_fill_report(&report_path)? {
                PersistedFillReport::Tick(report) => (
                    PathBuf::from(&report.cache_dir),
                    report.requested_days.clone(),
                    report.physical_symbols()?,
                ),
                PersistedFillReport::Unified(report) if report.cache_kind == "tick" => (
                    PathBuf::from(&report.cache_dir),
                    report.requested_days.clone(),
                    report.symbols()?,
                ),
                _ => {
                    return Err(CliError::Usage(
                        "--kind tick verify requires a tick fill report".to_string(),
                    ));
                }
            };
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
            (cache, canonical_cache_dir, window, symbols, Some("bound"))
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
            "cache_kind": "tick",
            "cache_dir": canonical_cache_dir,
            "requested_days": window,
            "source_report": source_report,
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
            let (report_root, symbols, requested_days, report_market) =
                match read_persisted_fill_report(&report_path)? {
                    PersistedFillReport::Minute(report) => (
                        PathBuf::from(&report.cache_dir),
                        report.symbols()?,
                        report.requested_days,
                        report.market,
                    ),
                    PersistedFillReport::Unified(report) if report.cache_kind == "minute" => (
                        PathBuf::from(&report.cache_dir),
                        report.symbols()?,
                        report.requested_days,
                        report.market,
                    ),
                    _ => {
                        return Err(CliError::Usage(
                            "--kind minute verify requires a canonical-minute fill report"
                                .to_string(),
                        ));
                    }
                };
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
            (
                canonical_cache_dir,
                requested_days,
                symbols,
                Some("bound"),
                report_market,
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
    let root_gate = BacktestTickCache::open(&canonical_cache_dir)?;
    let _lock = root_gate.try_acquire_consistency_read_lock()?;
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

async fn verify_daily(
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
            let (report_root, symbols, requested_days, report_market) =
                match read_persisted_fill_report(&report_path)? {
                    PersistedFillReport::Daily(report) => (
                        PathBuf::from(&report.cache_dir),
                        report.symbols()?,
                        report.requested_days,
                        report.market,
                    ),
                    PersistedFillReport::Unified(report) if report.cache_kind == "daily" => (
                        PathBuf::from(&report.cache_dir),
                        report.symbols()?,
                        report.requested_days,
                        report.market,
                    ),
                    _ => {
                        return Err(CliError::Usage(
                            "--kind daily verify requires a native-daily fill report".to_string(),
                        ));
                    }
                };
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
            (
                canonical_cache_dir,
                requested_days,
                symbols,
                Some("bound"),
                report_market,
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

    let root_gate = BacktestTickCache::open(&canonical_cache_dir)?;
    let _lock = root_gate.try_acquire_consistency_read_lock()?;
    let cache = DailyKlineCache::open_read_only(&canonical_cache_dir);
    let snapshots = symbols
        .iter()
        .map(|symbol| daily_cache_snapshot_for_symbol(&canonical_cache_dir, symbol))
        .collect::<Result<Vec<_>, _>>()?;
    let statuses = symbols
        .iter()
        .zip(&snapshots)
        .map(|(symbol, snapshot)| cache.inspect(symbol, window.start_ns, window.end_ns, snapshot))
        .collect::<Result<Vec<_>, _>>()?;
    let coverage_complete = statuses
        .iter()
        .all(tqsdk_data::DailyKlineCacheStatus::is_complete);
    let replay_rows = if args.replay && coverage_complete {
        let mut total = 0_u64;
        for (symbol, snapshot) in symbols.iter().zip(&snapshots) {
            let rows = cache.read_range(symbol, window.start_ns, window.end_ns, snapshot)?;
            total = total.saturating_add(u64::try_from(rows.len()).unwrap_or(u64::MAX));
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
            "cache_kind": "daily",
            "market": report_market,
            "cache_dir": canonical_cache_dir,
            "requested_days": window,
            "source_report": source_report,
            "symbols": symbols,
            "coverage_complete": coverage_complete,
            "replay_rows": replay_rows,
            "min_rows": args.min_rows,
            "statuses": statuses.iter().map(daily_cache_status_json).collect::<Vec<_>>(),
        }),
        exit_code: if coverage_complete && replay_ok { 0 } else { 1 },
    })
}

fn purge(
    cache_dir: Option<&Path>,
    kind: CacheKind,
    args: PurgeArgs,
) -> Result<CommandOutcome, CliError> {
    match kind {
        CacheKind::Tick => purge_tick(cache_dir, args),
        CacheKind::Minute => purge_minute(cache_dir, args),
        CacheKind::Daily => purge_daily(cache_dir, args),
        CacheKind::All => unreachable!("kind validation rejects this purge kind"),
    }
}

fn purge_tick(cache_dir: Option<&Path>, args: PurgeArgs) -> Result<CommandOutcome, CliError> {
    let symbols = normalized_symbols(args.symbols.symbols)?;
    if symbols.len() != 1 {
        return Err(CliError::Usage(
            "--kind tick purge requires exactly one --symbol".to_string(),
        ));
    }
    if !args.dry_run && !args.yes {
        return Err(CliError::Usage(
            "--kind tick purge is destructive; pass --yes or use --dry-run".to_string(),
        ));
    }
    let symbol = symbols.into_iter().next().expect("one symbol was required");
    let start_day = args
        .days
        .start_day
        .ok_or_else(|| CliError::Usage("--kind tick purge requires --start-day".to_string()))?;
    let end_day = args
        .days
        .end_day
        .ok_or_else(|| CliError::Usage("--kind tick purge requires --end-day".to_string()))?;
    let window = TradingDayWindow::from_days(start_day, end_day)?;
    let (_, canonical_cache_dir) = open_read_only_cache(cache_dir)?;

    if args.dry_run {
        let start_day = parse_window_day(&window.start_day)?;
        let end_day = parse_window_day(&window.end_day)?;
        let cache = BacktestTickCache::open_read_only(&canonical_cache_dir);
        let would_remove_files = cache
            .diagnose()?
            .files
            .into_iter()
            .filter(|file| file.symbol == symbol)
            .filter_map(|file| {
                let trading_day = file
                    .trading_day
                    .as_deref()
                    .and_then(|value| NaiveDate::parse_from_str(value, "%Y-%m-%d").ok())?;
                (start_day <= trading_day && trading_day <= end_day).then(|| {
                    json!({
                        "trading_day": trading_day,
                        "path": file.path,
                        "size_bytes": file.size_bytes,
                    })
                })
            })
            .collect::<Vec<_>>();
        return Ok(CommandOutcome {
            value: json!({
                "schema_version": REPORT_SCHEMA_VERSION,
                "command": "purge",
                "cache_kind": "tick",
                "cache_dir": canonical_cache_dir,
                "symbol": symbol,
                "requested_days": window,
                "dry_run": true,
                "would_remove_files": would_remove_files,
            }),
            exit_code: 0,
        });
    }

    let cache = BacktestTickCache::open(&canonical_cache_dir)?;
    let _lock = cache.try_acquire_consistency_read_lock()?;
    let report = cache.purge_symbol_ticks_in_range(&symbol, window.start_ns, window.end_ns)?;
    Ok(CommandOutcome {
        value: json!({
            "schema_version": REPORT_SCHEMA_VERSION,
            "command": "purge",
            "cache_kind": "tick",
            "cache_dir": canonical_cache_dir,
            "symbol": symbol,
            "requested_days": window,
            "dry_run": false,
            "removed": report.removed,
            "removed_files": report.removed_files,
            "removed_bytes": report.removed_bytes,
        }),
        exit_code: 0,
    })
}

fn purge_minute(cache_dir: Option<&Path>, args: PurgeArgs) -> Result<CommandOutcome, CliError> {
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
    let start_day = args
        .days
        .start_day
        .ok_or_else(|| CliError::Usage("--kind minute purge requires --start-day".to_string()))?;
    let end_day = args
        .days
        .end_day
        .ok_or_else(|| CliError::Usage("--kind minute purge requires --end-day".to_string()))?;
    let window = TradingDayWindow::from_days(start_day, end_day)?;
    let (_, canonical_cache_dir) = open_read_only_cache(cache_dir)?;
    if args.dry_run {
        let cache = MinuteKlineCache::open_read_only(&canonical_cache_dir);
        let start_day = parse_window_day(&window.start_day)?;
        let end_day = parse_window_day(&window.end_day)?;
        let trading_months = partition_days_between(start_day, end_day)?
            .into_iter()
            .map(|day| day.format("%Y%m").to_string())
            .collect::<BTreeSet<_>>();
        let mut would_remove_files = Vec::new();
        for trading_month in trading_months {
            let path = cache.month_file_path(&symbol, &trading_month);
            match fs::metadata(&path) {
                Ok(metadata) if metadata.is_file() => would_remove_files.push(json!({
                    "trading_month": trading_month,
                    "path": path,
                    "size_bytes": metadata.len(),
                })),
                Ok(_) => {
                    return Err(CliError::Data(DataError::InvalidResponse(format!(
                        "minute kline cache purge target {} is not a regular file",
                        path.display()
                    ))));
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
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

    let root_gate = BacktestTickCache::open(&canonical_cache_dir)?;
    let _lock = root_gate.try_acquire_consistency_read_lock()?;
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

fn purge_daily(cache_dir: Option<&Path>, args: PurgeArgs) -> Result<CommandOutcome, CliError> {
    if args.days.start_day.is_some() || args.days.end_day.is_some() {
        return Err(CliError::Usage(
            "--kind daily purge removes one complete symbol file; do not pass --start-day or --end-day"
                .to_string(),
        ));
    }
    let symbols = normalized_symbols(args.symbols.symbols)?;
    if symbols.len() != 1 {
        return Err(CliError::Usage(
            "--kind daily purge requires exactly one --symbol".to_string(),
        ));
    }
    if !args.dry_run && !args.yes {
        return Err(CliError::Usage(
            "--kind daily purge is destructive; pass --yes or use --dry-run".to_string(),
        ));
    }
    let symbol = symbols.into_iter().next().expect("one symbol was required");
    let (_, canonical_cache_dir) = open_read_only_cache(cache_dir)?;

    if args.dry_run {
        let cache = DailyKlineCache::open_read_only(&canonical_cache_dir);
        let path = cache.symbol_file_path(&symbol);
        let would_remove_files = match fs::metadata(&path) {
            Ok(metadata) if metadata.is_file() => vec![json!({
                "path": path,
                "size_bytes": metadata.len(),
            })],
            Ok(_) => {
                return Err(CliError::Data(DataError::InvalidResponse(format!(
                    "daily kline cache purge target {} is not a regular file",
                    path.display()
                ))));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        return Ok(CommandOutcome {
            value: json!({
                "schema_version": REPORT_SCHEMA_VERSION,
                "command": "purge",
                "cache_kind": "daily",
                "cache_dir": canonical_cache_dir,
                "symbol": symbol,
                "requested_days": Value::Null,
                "dry_run": true,
                "would_remove_files": would_remove_files,
            }),
            exit_code: 0,
        });
    }

    let root_gate = BacktestTickCache::open(&canonical_cache_dir)?;
    let _lock = root_gate.try_acquire_consistency_read_lock()?;
    let cache = DailyKlineCache::open(&canonical_cache_dir)?;
    let report = cache.purge_symbol(&symbol)?;
    Ok(CommandOutcome {
        value: json!({
            "schema_version": REPORT_SCHEMA_VERSION,
            "command": "purge",
            "cache_kind": "daily",
            "cache_dir": canonical_cache_dir,
            "symbol": symbol,
            "requested_days": Value::Null,
            "dry_run": false,
            "removed": report.removed,
            "removed_files": usize::from(report.removed),
            "removed_bytes": report.removed_bytes,
        }),
        exit_code: 0,
    })
}

fn doctor(cache_dir: Option<&Path>, kind: CacheKind) -> Result<CommandOutcome, CliError> {
    let (read_only_tick_cache, canonical_cache_dir) = open_read_only_cache(cache_dir)?;
    let _lock = read_only_tick_cache.try_acquire_consistency_read_lock()?;
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
    let daily_value = || -> Result<Value, CliError> {
        let report = DailyKlineCache::open_read_only(&canonical_cache_dir).diagnose_all()?;
        Ok(json!({
            "backend_format": report.format_id,
            "problem_files": report.problem_files,
            "files": report.files.into_iter().map(|file| json!({
                "path": file.path,
                "symbol": file.symbol,
                "status": daily_diagnostic_status_name(file.status),
                "rows": file.rows,
                "cached_ranges": file.cached_ranges,
                "size_bytes": file.size_bytes,
                "schema_version": file.schema_version,
                "error": file.error,
            })).collect::<Vec<_>>(),
        }))
    };
    let mut value = match kind {
        CacheKind::Tick => tick_value()?,
        CacheKind::Minute => minute_value()?,
        CacheKind::Daily => daily_value()?,
        CacheKind::All => {
            let tick = tick_value()?;
            let minute = minute_value()?;
            let daily = daily_value()?;
            json!({
                "cache_kind": kind.as_str(),
                "tick": tick,
                "minute": minute,
                "daily": daily,
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
                + value["daily"]["problem_files"].as_u64().unwrap_or_default()
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

fn repair_locks(
    cache_dir: Option<&Path>,
    kind: CacheKind,
    args: RepairLocksArgs,
) -> Result<CommandOutcome, CliError> {
    debug_assert!(matches!(kind, CacheKind::Tick));
    let (cache, canonical_cache_dir) = if args.apply {
        let (_, canonical_cache_dir) = open_read_only_cache(cache_dir)?;
        (
            BacktestTickCache::open(&canonical_cache_dir)?,
            canonical_cache_dir,
        )
    } else {
        open_read_only_cache(cache_dir)?
    };
    let _lock = cache.try_acquire_consistency_read_lock()?;
    let mode = if args.apply {
        BacktestTickCacheLockRepairMode::Apply
    } else {
        BacktestTickCacheLockRepairMode::DryRun
    };
    let report = cache.repair_tick_locks(mode)?;
    Ok(CommandOutcome {
        value: json!({
            "schema_version": REPORT_SCHEMA_VERSION,
            "command": "repair-locks",
            "cache_kind": "tick",
            "cache_dir": canonical_cache_dir,
            "dry_run": !args.apply,
            "legacy_partition_locks_scanned": report.legacy_partition_locks.len(),
            "legacy_partition_locks_missing": report.legacy_partition_locks_missing,
            "legacy_partition_locks_created": report.legacy_partition_locks_created,
            "legacy_partition_locks_already_present": report.legacy_partition_locks_already_present,
            "legacy_partition_locks_failed": report.legacy_partition_locks_failed,
            "legacy_partition_locks": report.legacy_partition_locks.into_iter().map(|lock| json!({
                "partition_dir": lock.partition_dir,
                "lock_path": lock.lock_path,
                "status": tick_lock_repair_status_name(lock.status),
                "error": lock.error,
            })).collect::<Vec<_>>(),
            "scanned_files": report.files.len(),
            "missing_files": report.missing_files,
            "created_files": report.created_files,
            "already_present_files": report.already_present_files,
            "failed_files": report.failed_files,
            "files": report.files.into_iter().map(|file| json!({
                "path": file.path,
                "lock_path": file.lock_path,
                "status": tick_lock_repair_status_name(file.status),
                "error": file.error,
            })).collect::<Vec<_>>(),
        }),
        exit_code: if report.legacy_partition_locks_failed == 0 && report.failed_files == 0 {
            0
        } else {
            1
        },
    })
}

#[derive(Debug)]
struct MinuteMigrationFile {
    path: PathBuf,
    size_bytes: u64,
}

#[derive(Debug)]
struct MinuteMigrationPlan {
    problem_files: usize,
    legacy_files: usize,
    source_bytes: u64,
    symbols: Vec<String>,
    files: Vec<MinuteMigrationFile>,
}

#[derive(Debug)]
struct TickMigrationFile {
    path: PathBuf,
    size_bytes: u64,
}

#[derive(Debug)]
struct TickMigrationPlan {
    problem_files: usize,
    legacy_files: usize,
    source_bytes: u64,
    symbols: Vec<String>,
    files: Vec<TickMigrationFile>,
}

#[derive(Debug, Default)]
struct MigrationOutcomeDetails {
    backup_dir: Option<PathBuf>,
    backup_data_files: usize,
    backup_lock_files: usize,
    rewritten_bytes: Option<u64>,
    completed: bool,
}

fn migrate_minute(cache_dir: Option<&Path>, args: MigrateArgs) -> Result<CommandOutcome, CliError> {
    if !args.apply && args.backup_dir.is_some() {
        return Err(CliError::Usage("--backup-dir requires --apply".to_string()));
    }

    let (_, canonical_cache_dir) = open_read_only_cache(cache_dir)?;
    let read_only_cache = MinuteKlineCache::open_read_only(&canonical_cache_dir);
    let plan = minute_migration_plan(&read_only_cache)?;
    if !args.apply {
        return Ok(minute_migration_outcome(
            canonical_cache_dir,
            true,
            &plan,
            MigrationOutcomeDetails::default(),
        ));
    }
    if plan.problem_files != 0 || plan.legacy_files == 0 {
        return Ok(minute_migration_outcome(
            canonical_cache_dir,
            false,
            &plan,
            MigrationOutcomeDetails::default(),
        ));
    }

    let backup_dir = args
        .backup_dir
        .ok_or_else(|| CliError::Usage("--backup-dir is required with --apply".to_string()))?;
    let root_gate = BacktestTickCache::open(&canonical_cache_dir)?;
    let _lock = root_gate.try_acquire_consistency_read_lock()?;
    let cache = MinuteKlineCache::open(&canonical_cache_dir)?;
    // The dry-run plan is intentionally obtained without the exclusive gate.
    // Rebuild it under the gate so every rewritten v4 file is backed up first.
    let plan = minute_migration_plan(&MinuteKlineCache::open_read_only(&canonical_cache_dir))?;
    if plan.problem_files != 0 || plan.legacy_files == 0 {
        return Ok(minute_migration_outcome(
            canonical_cache_dir,
            false,
            &plan,
            MigrationOutcomeDetails::default(),
        ));
    }
    let backup_dir = prepare_migration_backup_dir(&canonical_cache_dir, &backup_dir)?;
    let (backup_data_files, backup_lock_files) =
        match backup_minute_migration_inputs(&canonical_cache_dir, &backup_dir, &plan) {
            Ok(report) => report,
            Err(error) => {
                return Err(CliError::Migration(format!(
                    "migration did not start; partial backup retained at {}: {error}",
                    backup_dir.display()
                )));
            }
        };
    let report = cache.migrate_legacy_v4().map_err(|error| {
        CliError::Migration(format!(
            "migration rewrite failed; backup retained at {}: {error}",
            backup_dir.display()
        ))
    })?;
    let after = cache.diagnose().map_err(|error| {
        CliError::Migration(format!(
            "migration rewrite completed but validation failed; backup retained at {}: {error}",
            backup_dir.display()
        ))
    })?;
    let remaining_legacy = after
        .files
        .iter()
        .filter(|file| file.schema_version != Some(MINUTE_KLINE_CACHE_SCHEMA_VERSION))
        .count();
    if after.problem_files != 0 || remaining_legacy != 0 {
        return Err(CliError::Migration(format!(
            "migration validation found {} problem files and {} non-v{} files; backup retained at {}",
            after.problem_files,
            remaining_legacy,
            MINUTE_KLINE_CACHE_SCHEMA_VERSION,
            backup_dir.display()
        )));
    }

    Ok(minute_migration_outcome(
        canonical_cache_dir,
        false,
        &plan,
        MigrationOutcomeDetails {
            backup_dir: Some(backup_dir),
            backup_data_files,
            backup_lock_files,
            rewritten_bytes: Some(report.rewritten_bytes),
            completed: report.rewritten_files == plan.legacy_files,
        },
    ))
}

fn minute_migration_plan(cache: &MinuteKlineCache) -> Result<MinuteMigrationPlan, CliError> {
    let report = cache.diagnose()?;
    let mut files = Vec::new();
    let mut symbols = BTreeSet::new();
    let mut problem_files = 0usize;
    for file in report.files {
        if file.schema_version == Some(4)
            && file.status == tqsdk_data::MinuteKlineCacheDiagnosticStatus::LegacyUnsupported
        {
            files.push(MinuteMigrationFile {
                path: file.path,
                size_bytes: file.size_bytes,
            });
            symbols.insert(file.symbol);
        } else if file.status != tqsdk_data::MinuteKlineCacheDiagnosticStatus::Readable {
            problem_files = problem_files.saturating_add(1);
        }
    }
    let source_bytes = files.iter().map(|file| file.size_bytes).sum();
    Ok(MinuteMigrationPlan {
        problem_files,
        legacy_files: files.len(),
        source_bytes,
        symbols: symbols.into_iter().collect(),
        files,
    })
}

fn minute_migration_outcome(
    cache_dir: PathBuf,
    dry_run: bool,
    plan: &MinuteMigrationPlan,
    details: MigrationOutcomeDetails,
) -> CommandOutcome {
    CommandOutcome {
        value: json!({
            "schema_version": REPORT_SCHEMA_VERSION,
            "command": "migrate",
            "cache_kind": "minute",
            "cache_dir": cache_dir,
            "dry_run": dry_run,
            "completed": details.completed,
            "format_id": MINUTE_KLINE_CACHE_FORMAT_ID,
            "target_schema_version": MINUTE_KLINE_CACHE_SCHEMA_VERSION,
            "symbols": plan.symbols,
            "legacy_files": plan.legacy_files,
            "problem_files": plan.problem_files,
            "source_bytes": plan.source_bytes,
            "backup_dir": details.backup_dir,
            "backup_data_files": details.backup_data_files,
            "backup_lock_files": details.backup_lock_files,
            "rewritten_bytes": details.rewritten_bytes,
        }),
        exit_code: if plan.problem_files == 0 { 0 } else { 1 },
    }
}

fn migrate_universe(
    cache_dir: Option<&Path>,
    args: MigrateUniverseArgs,
) -> Result<CommandOutcome, CliError> {
    let cache_dir = cache_dir
        .map(PathBuf::from)
        .unwrap_or_else(tqsdk_data::default_history_cache_dir);
    let store = tqsdk_data::HistoricalUniverseArtifactStore::new(&cache_dir);
    let migration = if args.apply {
        store.migrate_v4_plan(&args.plan_sha256)?
    } else {
        store.preview_v4_migration(&args.plan_sha256)?
    };
    Ok(CommandOutcome {
        value: json!({
            "command": "migrate-universe",
            "cache_dir": cache_dir,
            "dry_run": !args.apply,
            "migration": migration,
        }),
        exit_code: 0,
    })
}

fn migrate(
    cache_dir: Option<&Path>,
    kind: CacheKind,
    args: MigrateArgs,
) -> Result<CommandOutcome, CliError> {
    match kind {
        CacheKind::Tick => migrate_tick(cache_dir, kind, args),
        CacheKind::Minute => migrate_minute(cache_dir, args),
        CacheKind::Daily | CacheKind::All => unreachable!("validated before migration dispatch"),
    }
}

fn migrate_tick(
    cache_dir: Option<&Path>,
    kind: CacheKind,
    args: MigrateArgs,
) -> Result<CommandOutcome, CliError> {
    debug_assert!(matches!(kind, CacheKind::Tick));
    if !args.apply && args.backup_dir.is_some() {
        return Err(CliError::Usage("--backup-dir requires --apply".to_string()));
    }

    let (read_only_cache, canonical_cache_dir) = open_read_only_cache(cache_dir)?;
    let plan = tick_migration_plan(&read_only_cache)?;
    if !args.apply {
        return Ok(migration_outcome(
            canonical_cache_dir,
            true,
            &plan,
            MigrationOutcomeDetails::default(),
        ));
    }
    if plan.problem_files != 0 || plan.legacy_files == 0 {
        return Ok(migration_outcome(
            canonical_cache_dir,
            false,
            &plan,
            MigrationOutcomeDetails::default(),
        ));
    }

    let backup_dir = args
        .backup_dir
        .as_deref()
        .expect("clap requires --backup-dir with --apply");
    let (cache, canonical_cache_dir) = open_cache(Some(canonical_cache_dir.as_path()))?;
    let _lock = cache.try_acquire_consistency_read_lock()?;
    let plan = tick_migration_plan(&cache)?;
    if plan.problem_files != 0 || plan.legacy_files == 0 {
        return Ok(migration_outcome(
            canonical_cache_dir,
            false,
            &plan,
            MigrationOutcomeDetails::default(),
        ));
    }

    let backup_dir = prepare_migration_backup_dir(&canonical_cache_dir, backup_dir)?;
    let (backup_data_files, backup_lock_files) =
        match backup_migration_inputs(&canonical_cache_dir, &backup_dir, &plan) {
            Ok(report) => report,
            Err(error) => {
                return Err(CliError::Migration(format!(
                    "migration did not start; partial backup retained at {}: {error}",
                    backup_dir.display()
                )));
            }
        };

    for symbol in &plan.symbols {
        if let Err(error) = cache.compact_symbol_ticks(symbol) {
            return Err(CliError::Migration(format!(
                "migration stopped while rewriting {symbol}; backup retained at {}: {error}",
                backup_dir.display()
            )));
        }
    }

    let after = cache.diagnose().map_err(|error| {
        CliError::Migration(format!(
            "migration rewrite completed but validation failed; backup retained at {}: {error}",
            backup_dir.display()
        ))
    })?;
    let remaining_legacy = after
        .files
        .iter()
        .filter(|file| file.schema_version != Some(HISTORY_SERIES_CACHE_SCHEMA_VERSION))
        .count();
    if after.problem_files != 0 || remaining_legacy != 0 {
        return Err(CliError::Migration(format!(
            "migration validation found {} problem files and {} non-v{} files; backup retained at {}",
            after.problem_files,
            remaining_legacy,
            HISTORY_SERIES_CACHE_SCHEMA_VERSION,
            backup_dir.display()
        )));
    }
    let target_symbols = plan.symbols.iter().collect::<BTreeSet<_>>();
    let after_bytes = after
        .files
        .iter()
        .filter(|file| target_symbols.contains(&file.symbol))
        .map(|file| file.size_bytes)
        .sum();

    Ok(migration_outcome(
        canonical_cache_dir,
        false,
        &plan,
        MigrationOutcomeDetails {
            backup_dir: Some(backup_dir),
            backup_data_files,
            backup_lock_files,
            rewritten_bytes: Some(after_bytes),
            completed: true,
        },
    ))
}

fn tick_migration_plan(cache: &BacktestTickCache) -> Result<TickMigrationPlan, CliError> {
    let report = cache.diagnose()?;
    let legacy_files = report
        .files
        .iter()
        .filter(|file| file.schema_version != Some(HISTORY_SERIES_CACHE_SCHEMA_VERSION))
        .count();
    let legacy_symbols = report
        .files
        .iter()
        .filter(|file| file.schema_version != Some(HISTORY_SERIES_CACHE_SCHEMA_VERSION))
        .map(|file| file.symbol.clone())
        .collect::<BTreeSet<_>>();
    let files = report
        .files
        .iter()
        .filter(|file| legacy_symbols.contains(&file.symbol))
        .map(|file| TickMigrationFile {
            path: file.path.clone(),
            size_bytes: file.size_bytes,
        })
        .collect::<Vec<_>>();
    let source_bytes = files.iter().map(|file| file.size_bytes).sum();

    Ok(TickMigrationPlan {
        problem_files: report.problem_files,
        legacy_files,
        source_bytes,
        symbols: legacy_symbols.into_iter().collect(),
        files,
    })
}

fn migration_outcome(
    cache_dir: PathBuf,
    dry_run: bool,
    plan: &TickMigrationPlan,
    details: MigrationOutcomeDetails,
) -> CommandOutcome {
    CommandOutcome {
        value: json!({
            "schema_version": REPORT_SCHEMA_VERSION,
            "command": "migrate",
            "cache_kind": "tick",
            "cache_dir": cache_dir,
            "dry_run": dry_run,
            "preflight_ok": plan.problem_files == 0,
            "completed": details.completed,
            "target_format": HISTORY_SERIES_CACHE_FORMAT_ID,
            "target_schema_version": HISTORY_SERIES_CACHE_SCHEMA_VERSION,
            "legacy_symbols": plan.symbols,
            "legacy_files": plan.legacy_files,
            "problem_files": plan.problem_files,
            "source_bytes": plan.source_bytes,
            "backup_dir": details.backup_dir,
            "backup_data_files": details.backup_data_files,
            "backup_lock_files": details.backup_lock_files,
            "rewritten_bytes": details.rewritten_bytes,
        }),
        exit_code: if plan.problem_files == 0 { 0 } else { 1 },
    }
}

fn prepare_migration_backup_dir(cache_dir: &Path, requested: &Path) -> Result<PathBuf, CliError> {
    let name = requested
        .file_name()
        .filter(|name| !name.is_empty())
        .ok_or_else(|| CliError::Usage("--backup-dir must name a new directory".to_string()))?;
    let parent = requested
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent = fs::canonicalize(parent)?;
    if !fs::metadata(&parent)?.is_dir() {
        return Err(CliError::Usage(
            "--backup-dir parent must be a directory".to_string(),
        ));
    }
    let backup_dir = parent.join(name);
    if backup_dir.exists() {
        return Err(CliError::Usage(
            "--backup-dir must not already exist".to_string(),
        ));
    }
    if backup_dir.starts_with(cache_dir) {
        return Err(CliError::Usage(
            "--backup-dir must be outside cache root".to_string(),
        ));
    }
    fs::create_dir(&backup_dir)?;
    Ok(backup_dir)
}

fn backup_migration_inputs(
    cache_dir: &Path,
    backup_dir: &Path,
    plan: &TickMigrationPlan,
) -> Result<(usize, usize), CliError> {
    let mut backup_data_files = 0;
    let mut lock_paths = BTreeSet::new();
    for file in &plan.files {
        let relative = file.path.strip_prefix(cache_dir).map_err(|_| {
            CliError::Migration(format!(
                "migration input {} is outside cache root {}",
                file.path.display(),
                cache_dir.display()
            ))
        })?;
        let target = backup_dir.join(relative);
        hard_link_migration_file(&file.path, &target)?;
        backup_data_files += 1;
        lock_paths.insert(file.path.with_extension("tqbn.lock"));
        let partition_lock = file.path.parent().ok_or_else(|| {
            CliError::Migration(format!(
                "migration input {} has no parent directory",
                file.path.display()
            ))
        })?;
        lock_paths.insert(partition_lock.join(".tqbn.lock"));
    }

    let mut backup_lock_files = 0;
    for lock_path in lock_paths {
        if !lock_path.exists() {
            continue;
        }
        let relative = lock_path.strip_prefix(cache_dir).map_err(|_| {
            CliError::Migration(format!(
                "lock {} is outside cache root {}",
                lock_path.display(),
                cache_dir.display()
            ))
        })?;
        copy_migration_lock(&lock_path, &backup_dir.join(relative))?;
        backup_lock_files += 1;
    }
    Ok((backup_data_files, backup_lock_files))
}

fn backup_minute_migration_inputs(
    cache_dir: &Path,
    backup_dir: &Path,
    plan: &MinuteMigrationPlan,
) -> Result<(usize, usize), CliError> {
    let mut backup_data_files = 0usize;
    let mut lock_paths = BTreeSet::new();
    for file in &plan.files {
        let relative = file.path.strip_prefix(cache_dir).map_err(|_| {
            CliError::Migration(format!(
                "migration input {} is outside cache root {}",
                file.path.display(),
                cache_dir.display()
            ))
        })?;
        hard_link_migration_file(&file.path, &backup_dir.join(relative))?;
        backup_data_files = backup_data_files.saturating_add(1);
        lock_paths.insert(file.path.with_extension("tqmk.lock"));
    }

    let mut backup_lock_files = 0usize;
    for lock_path in lock_paths {
        if !lock_path.exists() {
            continue;
        }
        let relative = lock_path.strip_prefix(cache_dir).map_err(|_| {
            CliError::Migration(format!(
                "migration lock {} is outside cache root {}",
                lock_path.display(),
                cache_dir.display()
            ))
        })?;
        copy_migration_lock(&lock_path, &backup_dir.join(relative))?;
        backup_lock_files = backup_lock_files.saturating_add(1);
    }
    Ok((backup_data_files, backup_lock_files))
}

fn hard_link_migration_file(source: &Path, target: &Path) -> Result<(), CliError> {
    let metadata = fs::symlink_metadata(source)?;
    if !metadata.file_type().is_file() {
        return Err(CliError::Migration(format!(
            "migration source {} is not a regular file",
            source.display()
        )));
    }
    let parent = target.parent().ok_or_else(|| {
        CliError::Migration(format!("backup target {} has no parent", target.display()))
    })?;
    fs::create_dir_all(parent)?;
    fs::hard_link(source, target).map_err(|error| {
        CliError::Migration(format!(
            "cannot hard-link {} to {}; --backup-dir must share cache filesystem: {error}",
            source.display(),
            target.display()
        ))
    })
}

fn copy_migration_lock(source: &Path, target: &Path) -> Result<(), CliError> {
    let metadata = fs::symlink_metadata(source)?;
    if !metadata.file_type().is_file() {
        return Err(CliError::Migration(format!(
            "migration lock {} is not a regular file",
            source.display()
        )));
    }
    let parent = target.parent().ok_or_else(|| {
        CliError::Migration(format!("backup target {} has no parent", target.display()))
    })?;
    fs::create_dir_all(parent)?;
    fs::copy(source, target)?;
    fs::set_permissions(target, metadata.permissions())?;
    Ok(())
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

fn history_fill_config(args: &FillArgs) -> Result<BacktestHistoryFillConfig, DataError> {
    let mut config = BacktestHistoryFillConfig::default();
    if let Some(value) = args.symbol_batch_size {
        config = config.with_symbol_batch_size(value)?;
    }
    if let Some(value) = args.symbol_concurrency {
        config = config.with_symbol_concurrency(value)?;
    }
    if let Some(value) = args.idle_timeout_secs {
        config = config.with_idle_timeout(Duration::from_secs(value))?;
    }
    if let Some(value) = args.batch_timeout_secs {
        config = if value == 0 {
            config.without_batch_timeout()
        } else {
            config.with_batch_timeout(Some(Duration::from_secs(value)))?
        };
    }
    if let Some(value) = args.lock_wait_secs {
        config = config.with_lock_wait(Some(Duration::from_secs(value)))?;
    }
    Ok(config)
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
        builder = builder.symbol(symbol);
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

fn daily_cache_status_json(status: &tqsdk_data::DailyKlineCacheStatus) -> Value {
    json!({
        "symbol": status.symbol,
        "backend_format": status.format_id,
        "namespace_dir": status.namespace_dir,
        "path": status.path,
        "range_start_ns": status.range_start_ns,
        "range_end_ns": status.range_end_ns,
        "rows": status.rows,
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

fn tick_lock_repair_status_name(status: BacktestTickCacheLockRepairStatus) -> &'static str {
    match status {
        BacktestTickCacheLockRepairStatus::Missing => "missing",
        BacktestTickCacheLockRepairStatus::AlreadyPresent => "already_present",
        BacktestTickCacheLockRepairStatus::Created => "created",
        BacktestTickCacheLockRepairStatus::Failed => "failed",
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

fn daily_diagnostic_status_name(
    status: tqsdk_data::DailyKlineCacheDiagnosticStatus,
) -> &'static str {
    match status {
        tqsdk_data::DailyKlineCacheDiagnosticStatus::Missing => "missing",
        tqsdk_data::DailyKlineCacheDiagnosticStatus::Readable => "readable",
        tqsdk_data::DailyKlineCacheDiagnosticStatus::UnsupportedVersion => "unsupported_version",
        tqsdk_data::DailyKlineCacheDiagnosticStatus::Corrupt => "corrupt",
    }
}

trait ShutdownCancellation {
    fn cancel(&self);
}

impl ShutdownCancellation for BacktestRemoteFillCancellation {
    fn cancel(&self) {
        BacktestRemoteFillCancellation::cancel(self);
    }
}

impl ShutdownCancellation for BacktestHistoryFillCancellation {
    fn cancel(&self) {
        BacktestHistoryFillCancellation::cancel(self);
    }
}

#[cfg(unix)]
fn spawn_shutdown_signal_handler(
    cancellation: impl ShutdownCancellation + Send + Sync + 'static,
    kind: CacheKind,
) -> Result<tokio::task::JoinHandle<()>, CliError> {
    let interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let terminate = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
    {
        Ok(terminate) => Some(terminate),
        Err(error) => {
            eprintln!("tqsdk-cache: SIGTERM handler unavailable ({error}); waiting for SIGINT");
            None
        }
    };
    Ok(tokio::spawn(wait_for_shutdown_signal(
        cancellation,
        kind,
        interrupt,
        terminate,
    )))
}

#[cfg(not(unix))]
fn spawn_shutdown_signal_handler(
    cancellation: impl ShutdownCancellation + Send + Sync + 'static,
    kind: CacheKind,
) -> Result<tokio::task::JoinHandle<()>, CliError> {
    Ok(tokio::spawn(wait_for_shutdown_signal(cancellation, kind)))
}

#[cfg(unix)]
async fn wait_for_shutdown_signal(
    cancellation: impl ShutdownCancellation + Send + Sync + 'static,
    kind: CacheKind,
    mut interrupt: tokio::signal::unix::Signal,
    mut terminate: Option<tokio::signal::unix::Signal>,
) {
    wait_for_one_shutdown_signal(&mut interrupt, terminate.as_mut()).await;
    cancellation.cancel();
    eprintln!("{}", shutdown_cancellation_message(kind));
    wait_for_one_shutdown_signal(&mut interrupt, terminate.as_mut()).await;
    eprintln!("tqsdk-cache: second shutdown signal received; exiting immediately");
    std::process::exit(130);
}

#[cfg(unix)]
async fn wait_for_one_shutdown_signal(
    interrupt: &mut tokio::signal::unix::Signal,
    terminate: Option<&mut tokio::signal::unix::Signal>,
) {
    match terminate {
        Some(terminate) => {
            tokio::select! {
                _ = interrupt.recv() => {},
                _ = terminate.recv() => {},
            }
        }
        None => {
            let _ = interrupt.recv().await;
        }
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal(
    cancellation: impl ShutdownCancellation + Send + Sync + 'static,
    kind: CacheKind,
) {
    let _ = tokio::signal::ctrl_c().await;
    cancellation.cancel();
    eprintln!("{}", shutdown_cancellation_message(kind));
    let _ = tokio::signal::ctrl_c().await;
    eprintln!("tqsdk-cache: second shutdown signal received; exiting immediately");
    std::process::exit(130);
}

fn shutdown_cancellation_message(kind: CacheKind) -> &'static str {
    match kind {
        CacheKind::Tick => {
            "tqsdk-cache: cancellation requested; flushing accepted partial tick rows"
        }
        CacheKind::Minute => {
            "tqsdk-cache: cancellation requested; incomplete minute ranges will remain uncommitted"
        }
        CacheKind::Daily => {
            "tqsdk-cache: cancellation requested; incomplete daily ranges will remain uncommitted"
        }
        CacheKind::All => "tqsdk-cache: cancellation requested",
    }
}

fn fill_was_interrupted(cancellation_requested: bool, warmup_succeeded: bool) -> bool {
    cancellation_requested && !warmup_succeeded
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
    use std::path::PathBuf;
    use std::time::Duration;

    use super::{
        CacheKind, CalendarMode, Cli, Command, FillDaysArgs, MarketKind, MigrateArgs, ProgressMode,
        ProviderMembershipRefreshArgs, current_open_trading_day, fill_historical_universe_plan,
        fill_was_interrupted, historical_universe_fill_targets,
        isolated_provider_history_unavailable_after_ns, migrate, persist_calendar_if_needed,
        provider_history_bootstrap_is_publishable, provider_history_unavailable_limit,
        provider_membership_canary_fill_config, provider_membership_refresh_fill_config,
        resolve_fill_window,
    };
    use chrono::NaiveDate;
    use clap::Parser;
    use tqsdk::advanced::core::Tick;
    use tqsdk_cache::{
        TradingCalendarHolidaysSnapshot, TradingCalendarSnapshot,
        read_trading_calendar_holidays_snapshot, write_trading_calendar_holidays_snapshot,
        write_trading_calendar_snapshot,
    };
    use tqsdk_data::{
        BacktestHistoryFillConfig, BacktestTickCache, TradingCalendarHolidays, TradingCalendarRow,
    };

    #[test]
    fn successful_warmup_wins_a_late_shutdown_signal_race() {
        assert!(!fill_was_interrupted(true, true));
        assert!(fill_was_interrupted(true, false));
        assert!(!fill_was_interrupted(false, false));
    }

    #[test]
    fn provider_membership_canary_gets_grace_but_candidates_stay_bounded() {
        let defaults = ProviderMembershipRefreshArgs {
            acquisition_sha256: "sha256:fixture".to_string(),
            max_symbols: 1,
            force: false,
            dry_run: true,
            symbol_concurrency: None,
            idle_timeout_secs: None,
            batch_timeout_secs: None,
            progress: ProgressMode::Off,
            progress_max_bars: 4,
        };
        assert_eq!(
            provider_membership_refresh_fill_config(&defaults)
                .unwrap()
                .batch_timeout(),
            Some(Duration::from_secs(15))
        );
        assert_eq!(
            provider_membership_canary_fill_config(&defaults)
                .unwrap()
                .batch_timeout(),
            Some(Duration::from_secs(30))
        );

        let overridden = ProviderMembershipRefreshArgs {
            batch_timeout_secs: Some(21),
            ..defaults
        };
        assert_eq!(
            provider_membership_refresh_fill_config(&overridden)
                .unwrap()
                .batch_timeout(),
            Some(Duration::from_secs(21))
        );
        assert_eq!(
            provider_membership_canary_fill_config(&overridden)
                .unwrap()
                .batch_timeout(),
            Some(Duration::from_secs(21))
        );
    }

    #[test]
    fn provider_membership_refresh_cli_pins_acquisition_and_keeps_default_kind() {
        let cli = Cli::try_parse_from([
            "tqsdk-cache",
            "refresh-provider-membership",
            "--acquisition-sha256",
            "sha256:fixture",
            "--dry-run",
        ])
        .unwrap();
        assert_eq!(cli.kind, CacheKind::Tick);
        let Command::RefreshProviderMembership(args) = cli.command else {
            panic!("refresh-provider-membership must parse to its dedicated command");
        };
        assert_eq!(args.acquisition_sha256, "sha256:fixture");
        assert_eq!(args.max_symbols, 4);
        assert!(args.dry_run);
    }

    #[test]
    fn only_isolated_provider_timeouts_are_data_unavailable_outcomes() {
        let config = BacktestHistoryFillConfig::default()
            .with_idle_timeout(Duration::from_secs(9))
            .unwrap()
            .with_batch_timeout(Some(Duration::from_secs(15)))
            .unwrap();
        assert_eq!(
            isolated_provider_history_unavailable_after_ns(
                Some("history fill batch made no progress for 9s"),
                config,
            ),
            Some(9_000_000_000)
        );
        assert_eq!(
            isolated_provider_history_unavailable_after_ns(
                Some("history fill batch exceeded 15s"),
                config,
            ),
            Some(15_000_000_000)
        );
        assert_eq!(
            isolated_provider_history_unavailable_after_ns(Some("authentication failed"), config),
            None
        );
        assert_eq!(
            isolated_provider_history_unavailable_after_ns(None, config),
            None
        );
    }

    #[test]
    fn provider_history_unavailable_circuit_breaker_boundaries() {
        let roster_len = 100;
        let limit = provider_history_unavailable_limit(roster_len);
        assert_eq!(limit, 8);
        assert!(!provider_history_bootstrap_is_publishable(0, 0, roster_len));
        assert!(provider_history_bootstrap_is_publishable(
            roster_len - limit,
            limit,
            roster_len,
        ));
        assert!(!provider_history_bootstrap_is_publishable(
            roster_len - limit - 1,
            limit + 1,
            roster_len,
        ));
    }

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
            refresh_calendar: false,
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
            refresh_calendar: false,
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
            refresh_calendar: false,
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
    async fn start_day_defaults_to_latest_closed_trading_day() {
        let args = FillDaysArgs {
            start_day: Some(NaiveDate::from_ymd_opt(2020, 1, 2).unwrap()),
            end_day: None,
            last_trading_days: None,
            calendar: CalendarMode::Off,
            refresh_calendar: false,
        };
        let current_open_range =
            super::backtest_tick_trading_day_range(current_open_trading_day().unwrap()).unwrap();

        let resolved = resolve_fill_window(std::path::Path::new("/tmp/unused"), &args, true, false)
            .await
            .unwrap();

        assert_eq!(resolved.window.start_day, "2020-01-02");
        assert_eq!(resolved.window.end_ns, current_open_range.start_ns);
    }

    #[test]
    fn fill_accepts_start_day_without_end_day() {
        let cli = Cli::try_parse_from([
            "tqsdk-cache",
            "fill",
            "--symbol",
            "SHFE.au2608",
            "--start-day",
            "2026-07-20",
        ])
        .unwrap();
        let Command::Fill(args) = cli.command else {
            panic!("expected fill command");
        };

        assert_eq!(
            args.days.start_day,
            Some(NaiveDate::from_ymd_opt(2026, 7, 20).unwrap())
        );
        assert!(args.days.end_day.is_none());
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
        let snapshot = raw_snapshot(2020, 2020);
        write_trading_calendar_holidays_snapshot(&root, &snapshot).unwrap();
        let legacy = TradingCalendarSnapshot::from_rows(vec![
            TradingCalendarRow {
                date: "2020-01-06".to_string(),
                trading: true,
            },
            TradingCalendarRow {
                date: "2020-01-07".to_string(),
                trading: true,
            },
        ])
        .unwrap();
        let legacy_path = write_trading_calendar_snapshot(&root, &legacy).unwrap();
        let legacy_before = std::fs::read(&legacy_path).unwrap();
        let args = FillDaysArgs {
            start_day: None,
            end_day: Some(NaiveDate::from_ymd_opt(2020, 1, 10).unwrap()),
            last_trading_days: Some(2),
            calendar: CalendarMode::Auto,
            refresh_calendar: false,
        };

        let resolved = resolve_fill_window(&root, &args, false, true)
            .await
            .unwrap();

        assert_eq!(resolved.window.start_day, "2020-01-09");
        assert_eq!(resolved.window.end_day, "2020-01-10");
        assert!(resolved.provisional.is_none());
        assert_eq!(resolved.calendar.source, "local");
        assert!(!resolved.calendar.persist_after_plan);
        assert!(resolved.calendar.persisted);
        assert_eq!(std::fs::read(legacy_path).unwrap(), legacy_before);

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn last_trading_days_resolves_weekend_anchors_backward() {
        let root = std::env::temp_dir().join(format!(
            "tqsdk-cache-calendar-weekend-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        write_trading_calendar_holidays_snapshot(&root, &raw_snapshot(2020, 2020)).unwrap();
        let args = FillDaysArgs {
            start_day: None,
            end_day: Some(NaiveDate::from_ymd_opt(2020, 1, 11).unwrap()),
            last_trading_days: Some(2),
            calendar: CalendarMode::Auto,
            refresh_calendar: false,
        };

        let resolved = resolve_fill_window(&root, &args, false, false)
            .await
            .unwrap();

        assert_eq!(resolved.window.start_day, "2020-01-09");
        assert_eq!(resolved.window.end_day, "2020-01-10");
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn last_trading_days_rejects_current_open_day_anchor_before_fetching() {
        let open_day = current_open_trading_day().unwrap();
        let args = FillDaysArgs {
            start_day: None,
            end_day: Some(open_day),
            last_trading_days: Some(1),
            calendar: CalendarMode::Auto,
            refresh_calendar: false,
        };

        let error = match resolve_fill_window(
            std::path::Path::new("/tmp/unused"),
            &args,
            true,
            false,
        )
        .await
        {
            Ok(_) => panic!("current open-day anchor should be rejected"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("current open TQBN trading day"));
    }

    #[test]
    fn raw_calendar_fails_closed_outside_its_supported_years() {
        let snapshot = raw_snapshot(2020, 2020);
        let error =
            super::eligible_calendar_days(&snapshot, NaiveDate::from_ymd_opt(2021, 1, 4).unwrap())
                .unwrap_err();

        assert!(error.to_string().contains("support years 2020 to 2020"));
    }

    #[test]
    fn dry_run_calendar_persistence_does_not_create_an_active_pointer() {
        let root = std::env::temp_dir().join(format!(
            "tqsdk-cache-calendar-dry-run-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut calendar = super::CalendarResolution {
            mode: CalendarMode::Auto,
            snapshot: Some(raw_snapshot(2020, 2020)),
            source: "remote".to_string(),
            persist_after_plan: true,
            persisted: false,
        };

        persist_calendar_if_needed(&root, &mut calendar, true).unwrap();

        assert!(
            read_trading_calendar_holidays_snapshot(&root)
                .unwrap()
                .is_none()
        );
        assert!(calendar.persist_after_plan);
        assert!(!calendar.persisted);
    }

    fn raw_snapshot(start_year: i32, end_year: i32) -> TradingCalendarHolidaysSnapshot {
        let holidays = (start_year..=end_year)
            .map(|year| NaiveDate::from_ymd_opt(year, 1, 1).unwrap())
            .collect::<Vec<_>>();
        TradingCalendarHolidaysSnapshot::from_holidays(
            TradingCalendarHolidays::new("https://example.invalid/holidays.json", holidays)
                .unwrap(),
        )
        .unwrap()
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
    fn fill_accepts_refresh_calendar() {
        let cli = Cli::try_parse_from([
            "tqsdk-cache",
            "fill",
            "--symbol",
            "SHFE.au2608",
            "--start-day",
            "2026-07-20",
            "--end-day",
            "2026-07-21",
            "--refresh-calendar",
        ])
        .unwrap();

        let Command::Fill(args) = cli.command else {
            panic!("expected fill command");
        };
        assert!(args.days.refresh_calendar);
    }

    #[test]
    fn fill_accepts_historical_universe_timeline_path() {
        let alias_error = Cli::try_parse_from([
            "tqsdk-cache",
            "fill",
            "--universe-timeline",
            "fixture-plan.json",
        ])
        .unwrap_err();
        assert_eq!(alias_error.kind(), clap::error::ErrorKind::UnknownArgument);

        let cli = Cli::try_parse_from([
            "tqsdk-cache",
            "fill",
            "--universe-plan",
            "fixture-plan.json",
        ])
        .unwrap();
        let Command::Fill(args) = cli.command else {
            panic!("expected fill command");
        };
        assert_eq!(
            args.universe_timeline,
            Some(PathBuf::from("fixture-plan.json"))
        );
    }

    #[test]
    fn legacy_universe_plan_flag_requires_a_plan_path() {
        let error = Cli::try_parse_from(["tqsdk-cache", "fill", "--allow-legacy-universe-plan"])
            .unwrap_err();
        assert_eq!(
            error.kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        );

        let cli = Cli::try_parse_from([
            "tqsdk-cache",
            "fill",
            "--universe-plan",
            "fixture-plan.json",
            "--allow-legacy-universe-plan",
        ])
        .unwrap();
        let Command::Fill(args) = cli.command else {
            panic!("expected fill command");
        };
        assert!(args.allow_legacy_universe_plan);
    }

    #[test]
    fn legacy_v2_targets_require_explicit_opt_in() {
        let scope = tqsdk_data::DynamicUniverseScope::all();
        let plan = tqsdk_data::CatalogSnapshot::new(
            "fixture-v2",
            "calendar:fixture-v2",
            true,
            scope.clone(),
            vec![
                tqsdk_data::CatalogContract::new(
                    "SHFE.au2406",
                    "SHFE",
                    "au",
                    vec![tqsdk_data::ActiveInterval::new(10, 20).unwrap()],
                )
                .unwrap(),
            ],
        )
        .unwrap()
        .compile_timeline(1, 30, scope, [])
        .unwrap()
        .prepare(tqsdk_data::UniverseBudget::new(8, 16).unwrap())
        .unwrap();

        let error =
            historical_universe_fill_targets(&plan, tqsdk_data::HistoricalDataKind::Minute, false)
                .unwrap_err();
        assert!(error.to_string().contains("--allow-legacy-universe-plan"));

        let (targets, legacy_unproven) =
            historical_universe_fill_targets(&plan, tqsdk_data::HistoricalDataKind::Minute, true)
                .unwrap();
        assert!(legacy_unproven);
        assert_eq!(targets.len(), 1);
    }

    #[tokio::test]
    async fn v3_plan_dry_run_uses_verified_pinned_minute_targets() {
        use std::collections::BTreeMap;

        let root = std::env::temp_dir().join(format!(
            "tqsdk-cache-v3-plan-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let lifecycle = vec![tqsdk_data::ActiveInterval::new(100, 400).unwrap()];
        let acquisition = tqsdk_data::HistoricalCatalogAcquisition::new(
            tqsdk_data::HistoricalCatalogProof::AuthoritativeLifecycle,
            "fixture-authoritative:v1",
            "physical:all",
            500,
            600,
            true,
            vec!["SHFE.au2404".to_string()],
            vec!["SHFE.au2404".to_string()],
            vec![tqsdk_data::HistoricalAcquisitionContract {
                symbol: "SHFE.au2404".to_string(),
                exchange_id: "SHFE".to_string(),
                product_id: "au".to_string(),
                expired: true,
                expire_datetime_ns: Some(400),
                authoritative_lifecycle: lifecycle.clone(),
                first_available_data_ns: BTreeMap::from([
                    (tqsdk_data::HistoricalDataKind::Tick, 101),
                    (tqsdk_data::HistoricalDataKind::Minute, 102),
                    (tqsdk_data::HistoricalDataKind::Daily, 103),
                ]),
            }],
        )
        .unwrap();
        let catalog = tqsdk_data::CatalogSnapshot::new(
            "fixture-v3",
            "calendar:fixture-v3",
            true,
            tqsdk_data::DynamicUniverseScope::all(),
            vec![tqsdk_data::CatalogContract::new("SHFE.au2404", "SHFE", "au", lifecycle).unwrap()],
        )
        .unwrap();
        let semantic = tqsdk_data::HistoricalSemanticCatalog::new(
            &acquisition,
            "timeline(active:all)",
            catalog,
        )
        .unwrap();
        let plan = tqsdk_data::compile_historical_universe_resolution(
            &acquisition,
            &semantic,
            &tqsdk_data::HistoricalFillUniverseSpec::parse("timeline(active:all)").unwrap(),
            100,
            400,
            tqsdk_data::UniverseBudget::new(8, 16).unwrap(),
        )
        .unwrap()
        .plan;
        let store = tqsdk_data::HistoricalUniverseArtifactStore::new(&root);
        store.publish_acquisition(&acquisition).unwrap();
        store.publish_semantic_catalog(&semantic).unwrap();
        let plan_path = store.publish_plan(&plan).unwrap();

        let cli = Cli::try_parse_from([
            "tqsdk-cache",
            "fill",
            "--universe-plan",
            plan_path.to_str().unwrap(),
            "--dry-run",
        ])
        .unwrap();
        let Command::Fill(args) = cli.command else {
            panic!("expected fill command");
        };
        let outcome = fill_historical_universe_plan(
            Some(&root),
            CacheKind::Minute,
            MarketKind::Futures,
            args,
            plan_path,
            None,
        )
        .await
        .unwrap();
        assert_eq!(outcome.value["plan_sha256"], plan.plan_sha256);
        assert_eq!(outcome.value["legacy_unproven"], false);
        assert_eq!(outcome.value["universe_timeline"]["target_count"], 1);

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn migrate_apply_requires_an_explicit_backup_directory() {
        let error = Cli::try_parse_from(["tqsdk-cache", "migrate", "--apply"]).unwrap_err();

        assert_eq!(
            error.kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        );
    }

    #[test]
    fn migrate_apply_hard_links_legacy_input_and_rewrites_to_v3() {
        let parent = std::env::temp_dir().join(format!(
            "tqsdk-cache-migrate-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let cache_dir = parent.join("cache");
        let backup_dir = parent.join("backup");
        let cache = BacktestTickCache::open(&cache_dir).unwrap();
        cache
            .store_ticks(
                "SHFE.op2701",
                1_000,
                2_000,
                [Tick {
                    id: 1,
                    datetime: 1_000,
                    ..Tick::default()
                }],
            )
            .unwrap();
        let source_path = cache.diagnose().unwrap().files[0].path.clone();
        let mut bytes = std::fs::read(&source_path).unwrap();
        bytes[5..9].copy_from_slice(&2_u32.to_le_bytes());
        std::fs::write(&source_path, bytes).unwrap();

        let outcome = migrate(
            Some(&cache_dir),
            CacheKind::Tick,
            MigrateArgs {
                apply: true,
                backup_dir: Some(backup_dir.clone()),
            },
        )
        .unwrap();

        assert_eq!(outcome.value["completed"], true);
        let backup_path = backup_dir.join(source_path.strip_prefix(&cache_dir).unwrap());
        let backup_bytes = std::fs::read(backup_path).unwrap();
        assert_eq!(&backup_bytes[5..9], &2_u32.to_le_bytes());
        let files = BacktestTickCache::open_read_only(&cache_dir)
            .diagnose()
            .unwrap()
            .files;
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].schema_version, Some(3));
        assert!(
            backup_dir
                .join("series/19700101/tick/SHFE.op2701.tqbn.lock")
                .is_file()
        );
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

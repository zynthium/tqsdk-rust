use std::collections::BTreeSet;
use std::io::{self, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::NaiveDate;
use clap::{Args, Parser, Subcommand};
use indicatif::{ProgressBar, ProgressDrawTarget};
use serde_json::{Value, json};
use tqsdk::{
    BacktestRemoteFillCancellation, BacktestRemoteFillConfig, BacktestRemoteFillProgress, Tq,
};
use tqsdk_cache::{
    FillReport, REPORT_SCHEMA_VERSION, TradingDayWindow, default_fill_report_path, open_cache,
    open_read_only_cache, read_fill_report, write_fill_report,
};
use tqsdk_data::{DataError, HistorySeriesCacheFileStatus};

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
    days: DaysArgs,
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
struct FillProgress {
    bar: ProgressBar,
    interactive: bool,
}

impl FillProgress {
    fn new() -> Self {
        let interactive = io::stderr().is_terminal();
        let bar = if interactive {
            let bar = ProgressBar::new_spinner();
            bar.set_draw_target(ProgressDrawTarget::stderr_with_hz(4));
            bar.enable_steady_tick(Duration::from_millis(120));
            bar
        } else {
            ProgressBar::hidden()
        };
        Self { bar, interactive }
    }

    fn observe(&self, event: &BacktestRemoteFillProgress) {
        let message = match event {
            BacktestRemoteFillProgress::FillStarted {
                requested_symbols,
                total_batches,
                symbol_batch_size,
                symbol_concurrency,
                ..
            } => format!(
                "fill started: symbols={requested_symbols} batches={total_batches} batch_size={symbol_batch_size} concurrency={symbol_concurrency}"
            ),
            BacktestRemoteFillProgress::BatchStarted {
                batch_number,
                total_batches,
                requested_range,
                symbols,
                ..
            } => format!(
                "batch {batch_number}/{total_batches}: symbols={} range=[{}, {})",
                symbols.join(","),
                requested_range.0,
                requested_range.1
            ),
            BacktestRemoteFillProgress::TickObserved {
                symbol,
                trading_day,
                accepted_rows,
            } => format!(
                "downloading {symbol}: trading_day={trading_day} accepted_rows={accepted_rows}"
            ),
            BacktestRemoteFillProgress::BatchFinished {
                batch_number,
                total_batches,
                completed_batches,
                rows,
                ..
            } => format!(
                "batch {batch_number}/{total_batches} complete: completed={completed_batches} rows={rows}"
            ),
            BacktestRemoteFillProgress::BatchFailed {
                batch_number,
                total_batches,
                symbols,
                error,
                ..
            } => format!(
                "batch {batch_number}/{total_batches} failed: symbols={} error={error}",
                symbols.join(",")
            ),
        };
        if self.interactive {
            self.bar.set_message(message);
            self.bar.tick();
        } else {
            eprintln!("tqsdk-cache: {message}");
        }
    }

    fn finish(&self, message: &str) {
        if self.interactive {
            self.bar.finish_with_message(message.to_string());
        } else {
            eprintln!("tqsdk-cache: {message}");
        }
    }
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
    let window = TradingDayWindow::closed_from_days(args.days.start_day, args.days.end_day)?;
    if args.dry_run {
        if args.report.is_some() {
            return Err(CliError::Usage(
                "--report cannot be used with --dry-run because dry-run writes no files"
                    .to_string(),
            ));
        }
        let (cache, canonical_cache_dir) = open_read_only_cache(cache_dir)?;
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
        let report = FillReport::from_warmup(&warmup, &canonical_cache_dir, window, config, true);
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

    let (cache, canonical_cache_dir) = open_cache(cache_dir)?;
    let reporter = FillProgress::new();
    let cancellation = BacktestRemoteFillCancellation::new();
    let signal_cancellation = cancellation.clone();
    let signal_task = tokio::spawn(async move {
        wait_for_shutdown_signal(signal_cancellation).await;
    });
    let callback = reporter.clone();
    let mut builder = builder_with_environment_auth(false)?
        .backtest(window.start_ns, window.end_ns)
        .cache_dir(&canonical_cache_dir)?
        .remote_on_miss()
        .remote_fill_config(config)
        .remote_fill_cancellation(cancellation.clone())
        .on_remote_fill_progress(move |event| callback.observe(event));
    if let Some(wait_secs) = args.lock_wait_secs {
        builder = builder.remote_fill_lock_wait(Duration::from_secs(wait_secs));
    }
    let builder = apply_fill_targets(builder, &symbols, universe.as_deref())?;
    let warmup = builder.warmup().await;
    signal_task.abort();
    let _ = signal_task.await;

    if cancellation.is_cancelled() {
        reporter.finish("interrupted; partial accepted rows were flushed without coverage commit");
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

    let warmup = warmup?;
    reporter.finish("fill complete; verifying final local coverage");
    let report = FillReport::from_warmup(&warmup, &canonical_cache_dir, window, config, false);
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

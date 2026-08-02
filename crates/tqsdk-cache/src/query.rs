use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, FixedOffset, SecondsFormat, Utc};
use clap::{Args, Subcommand, ValueEnum};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use sha1::{Digest, Sha1};
use tqsdk::advanced::core::{Kline, Tick};
use tqsdk_data::{
    BacktestHistoryClient, BacktestHistoryFinality, BacktestHistoryMetadataCache,
    BacktestHistoryMetadataSnapshot, BacktestHistoryPolicy, BacktestHistoryRequest,
    BacktestHistoryRequestFailure, BacktestHistoryRequestReport, BacktestHistoryRows, DataError,
};

use crate::{CacheKind, CliError, CommandOutcome, MarketKind, OutputFormat};

const QUERY_SCHEMA_VERSION: u8 = 1;
const JSONL_PROTOCOL: &str = "tqsdk-history-jsonl/1";
const LLM_CSV_PROTOCOL: &str = "tqllm-csv/3";
const DEFAULT_MAX_MEMORY_BYTES: usize = 128 * 1024 * 1024;

#[derive(Debug, Clone, Args)]
pub(crate) struct QueryArgs {
    #[command(subcommand)]
    command: Option<QuerySubcommand>,
    #[command(flatten)]
    request: QueryRequestArgs,
}

#[derive(Debug, Clone, Subcommand)]
enum QuerySubcommand {
    /// Describe the stable row schema and accepted field aliases.
    Schema(QuerySchemaArgs),
}

#[derive(Debug, Clone, Args)]
struct QuerySchemaArgs {
    /// Row family whose field schema should be printed.
    #[arg(long, value_enum)]
    series: QuerySeries,
}

#[derive(Debug, Clone, Args)]
struct QueryRequestArgs {
    /// Logical symbol. Repeat for a homogeneous batch.
    #[arg(long = "symbol", value_name = "SYMBOL")]
    symbols: Vec<String>,
    /// Row family for simple homogeneous requests.
    #[arg(long, value_enum)]
    series: Option<QuerySeries>,
    /// Inclusive RFC 3339 start timestamp; query windows are [start, end).
    #[arg(long, value_name = "RFC3339")]
    start: Option<String>,
    /// Exclusive RFC 3339 end timestamp; query windows are [start, end).
    #[arg(long, value_name = "RFC3339")]
    end: Option<String>,
    /// Kline duration such as 15s, 1m, 5m, or 1h. Required for --series kline.
    #[arg(long, value_name = "DURATION")]
    period: Option<String>,
    /// Read policy. Remote-on-miss checks the cache before lazily reading auth.
    #[arg(long, value_enum, default_value_t = QueryPolicy::RemoteOnMiss)]
    policy: QueryPolicy,
    /// TOML file for a heterogeneous query batch. It cannot be combined with simple request flags.
    #[arg(long, value_name = "PATH")]
    request_file: Option<PathBuf>,
    /// Strict row projection. Comma-separated aliases are accepted; output always uses canonical names.
    #[arg(long, value_delimiter = ',', value_name = "FIELD")]
    fields: Vec<String>,
    /// Timestamp codec for row data.
    #[arg(long, value_enum, default_value_t = TimestampMode::Full)]
    timestamp: TimestampMode,
    /// Time representation for LLM CSV. Defaults to compact ISO, or to offset when --timestamp offset is set.
    #[arg(long, value_enum)]
    llm_time: Option<LlmTimeMode>,
    /// Time zone for LLM CSV timestamps. Defaults to Asia/Shanghai; use utc for UTC output.
    #[arg(long, value_enum)]
    llm_timezone: Option<LlmTimezone>,
    /// Number codec for row data. scaled-int requires --price-tick.
    #[arg(long, value_enum, default_value_t = NumberFormat::Decimal)]
    number_format: NumberFormat,
    /// Price tick used by --number-format scaled-int. TOML requests may override it per block.
    #[arg(long, value_name = "PRICE")]
    price_tick: Option<f64>,
    /// Maximum heap bytes retained while terminally collecting the query.
    #[arg(long, value_name = "BYTES", default_value_t = DEFAULT_MAX_MEMORY_BYTES)]
    max_memory_bytes: usize,
    /// Permit completed blocks to be emitted when sibling requests or LLM metadata checks fail.
    #[arg(long)]
    allow_partial: bool,
    /// Maximum locally estimated GPT-5.6 data tokens for --output-format llm-csv.
    #[arg(long, value_name = "TOKENS")]
    data_token_budget: Option<usize>,
    /// Compression policy used only when an LLM token budget is exceeded.
    #[arg(long, value_enum, default_value_t = CompressionMode::Auto)]
    compression: CompressionMode,
    /// Deterministic row-preservation strategy for lossy LLM compression.
    #[arg(long, value_enum, default_value_t = FocusMode::Balanced)]
    focus: FocusMode,
    /// Atomically write raw jsonl or llm-csv output to this path instead of stdout.
    #[arg(long, value_name = "PATH")]
    output: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum QuerySeries {
    Tick,
    Kline,
}

impl QuerySeries {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Tick => "tick",
            Self::Kline => "kline",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum QueryPolicy {
    CacheOnly,
    RemoteOnMiss,
}

impl QueryPolicy {
    const fn as_str(self) -> &'static str {
        match self {
            Self::CacheOnly => "cache-only",
            Self::RemoteOnMiss => "remote-on-miss",
        }
    }

    const fn into_history_policy(self) -> BacktestHistoryPolicy {
        match self {
            Self::CacheOnly => BacktestHistoryPolicy::CacheOnly,
            Self::RemoteOnMiss => BacktestHistoryPolicy::RemoteOnMiss,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum TimestampMode {
    /// Full ISO 8601 timestamps, which are easiest to inspect directly.
    Full,
    /// Signed nanosecond offsets from the block start; the reference is emitted in metadata.
    Offset,
}

impl TimestampMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Full => "iso8601",
            Self::Offset => "offset-ns",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum LlmTimeMode {
    /// Human-readable UTC timestamps at the exact precision represented by the block.
    Iso,
    /// Integer offsets from the block reference, with the unit declared in block metadata.
    Offset,
    /// Both compact ISO timestamps and integer offsets for side-by-side comparison.
    Both,
}

impl LlmTimeMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Iso => "iso",
            Self::Offset => "offset",
            Self::Both => "both",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum LlmTimezone {
    /// China Standard Time, rendered as the unambiguous Asia/Shanghai +08:00 offset.
    Shanghai,
    /// Coordinated Universal Time.
    Utc,
}

impl LlmTimezone {
    const fn label(self) -> &'static str {
        match self {
            Self::Shanghai => "Asia/Shanghai",
            Self::Utc => "UTC",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum NumberFormat {
    Decimal,
    ScaledInt,
}

impl NumberFormat {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Decimal => "decimal",
            Self::ScaledInt => "scaled-int",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum CompressionMode {
    Auto,
    Off,
}

impl CompressionMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Off => "off",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum FocusMode {
    Balanced,
    Price,
    VolumeOi,
    Microstructure,
}

impl FocusMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Balanced => "balanced",
            Self::Price => "price",
            Self::VolumeOi => "volume-oi",
            Self::Microstructure => "microstructure",
        }
    }
}

pub(crate) enum QueryExecution {
    Summary(CommandOutcome),
    Raw(QueryRawOutput),
}

pub(crate) struct QueryRawOutput {
    pub(crate) payload: Vec<u8>,
    pub(crate) output_path: Option<PathBuf>,
    pub(crate) exit_code: i32,
}

#[derive(Debug, Clone)]
struct QuerySettings {
    policy: QueryPolicy,
    timestamp: TimestampMode,
    llm_time: Option<LlmTimeMode>,
    llm_timezone: Option<LlmTimezone>,
    number_format: NumberFormat,
    max_memory_bytes: usize,
    allow_partial: bool,
    data_token_budget: Option<usize>,
    compression: CompressionMode,
    focus: FocusMode,
    output_path: Option<PathBuf>,
}

impl QuerySettings {
    const fn llm_time_mode(&self) -> LlmTimeMode {
        match self.llm_time {
            Some(mode) => mode,
            None => match self.timestamp {
                TimestampMode::Full => LlmTimeMode::Iso,
                TimestampMode::Offset => LlmTimeMode::Offset,
            },
        }
    }

    const fn llm_timezone(&self) -> LlmTimezone {
        match self.llm_timezone {
            Some(timezone) => timezone,
            None => LlmTimezone::Shanghai,
        }
    }
}

#[derive(Debug, Clone)]
struct QuerySpec {
    request_id: u64,
    symbol: String,
    series: QuerySeries,
    duration: Option<Duration>,
    duration_ns: Option<i64>,
    start_ns: i64,
    end_ns: i64,
    fields: Vec<Field>,
    weight: u32,
    price_tick: Option<f64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryRequestFile {
    version: u8,
    #[serde(rename = "request", default)]
    requests: Vec<QueryRequestFileItem>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryRequestFileItem {
    symbol: String,
    series: QuerySeries,
    start: String,
    end: String,
    period: Option<String>,
    fields: Option<Vec<String>>,
    weight: Option<u32>,
    price_tick: Option<f64>,
}

#[derive(Debug, Clone)]
struct QueryArtifact {
    cache_dir: PathBuf,
    query_id: String,
    query_hash: String,
    settings: QuerySettings,
    blocks: Vec<QueryBlock>,
    failures: Vec<QueryFailure>,
}

#[derive(Debug, Clone)]
struct QueryBlock {
    block_id: String,
    spec: QuerySpec,
    request: BacktestHistoryRequestReport,
    rows: BacktestHistoryRows,
    metadata: MetadataStatus,
    data_hash: String,
}

#[derive(Debug, Clone)]
enum MetadataStatus {
    Verified(BacktestHistoryMetadataSnapshot),
    Missing { reason: String },
}

#[derive(Debug, Clone)]
struct QueryFailure {
    request_id: u64,
    symbol: String,
    code: &'static str,
    message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Field {
    Time,
    Id,
    Open,
    High,
    Low,
    Close,
    Volume,
    OpenOi,
    CloseOi,
    LastPrice,
    Average,
    Highest,
    Lowest,
    AskPrice1,
    AskVolume1,
    BidPrice1,
    BidVolume1,
    AskPrice2,
    AskVolume2,
    BidPrice2,
    BidVolume2,
    AskPrice3,
    AskVolume3,
    BidPrice3,
    BidVolume3,
    AskPrice4,
    AskVolume4,
    BidPrice4,
    BidVolume4,
    AskPrice5,
    AskVolume5,
    BidPrice5,
    BidVolume5,
    Amount,
    OpenInterest,
}

impl Field {
    const fn code(self) -> &'static str {
        match self {
            Self::Time => "t",
            Self::Id => "id",
            Self::Open => "o",
            Self::High => "h",
            Self::Low => "l",
            Self::Close => "c",
            Self::Volume => "v",
            Self::OpenOi => "oi0",
            Self::CloseOi | Self::OpenInterest => "oi",
            Self::LastPrice => "lp",
            Self::Average => "avg",
            Self::Highest => "hi",
            Self::Lowest => "lo",
            Self::AskPrice1 => "ap1",
            Self::AskVolume1 => "av1",
            Self::BidPrice1 => "bp1",
            Self::BidVolume1 => "bv1",
            Self::AskPrice2 => "ap2",
            Self::AskVolume2 => "av2",
            Self::BidPrice2 => "bp2",
            Self::BidVolume2 => "bv2",
            Self::AskPrice3 => "ap3",
            Self::AskVolume3 => "av3",
            Self::BidPrice3 => "bp3",
            Self::BidVolume3 => "bv3",
            Self::AskPrice4 => "ap4",
            Self::AskVolume4 => "av4",
            Self::BidPrice4 => "bp4",
            Self::BidVolume4 => "bv4",
            Self::AskPrice5 => "ap5",
            Self::AskVolume5 => "av5",
            Self::BidPrice5 => "bp5",
            Self::BidVolume5 => "bv5",
            Self::Amount => "amt",
        }
    }

    const fn aliases(self) -> &'static [&'static str] {
        match self {
            Self::Time => &["t", "time", "timestamp", "datetime"],
            Self::Id => &["id"],
            Self::Open => &["o", "open"],
            Self::High => &["h", "high"],
            Self::Low => &["l", "low"],
            Self::Close => &["c", "close"],
            Self::Volume => &["v", "volume"],
            Self::OpenOi => &["oi0", "open_oi"],
            Self::CloseOi => &["oi", "close_oi"],
            Self::LastPrice => &["lp", "last_price"],
            Self::Average => &["avg", "average"],
            Self::Highest => &["hi", "highest"],
            Self::Lowest => &["lo", "lowest"],
            Self::AskPrice1 => &["ap1", "ask_price1"],
            Self::AskVolume1 => &["av1", "ask_volume1"],
            Self::BidPrice1 => &["bp1", "bid_price1"],
            Self::BidVolume1 => &["bv1", "bid_volume1"],
            Self::AskPrice2 => &["ap2", "ask_price2"],
            Self::AskVolume2 => &["av2", "ask_volume2"],
            Self::BidPrice2 => &["bp2", "bid_price2"],
            Self::BidVolume2 => &["bv2", "bid_volume2"],
            Self::AskPrice3 => &["ap3", "ask_price3"],
            Self::AskVolume3 => &["av3", "ask_volume3"],
            Self::BidPrice3 => &["bp3", "bid_price3"],
            Self::BidVolume3 => &["bv3", "bid_volume3"],
            Self::AskPrice4 => &["ap4", "ask_price4"],
            Self::AskVolume4 => &["av4", "ask_volume4"],
            Self::BidPrice4 => &["bp4", "bid_price4"],
            Self::BidVolume4 => &["bv4", "bid_volume4"],
            Self::AskPrice5 => &["ap5", "ask_price5"],
            Self::AskVolume5 => &["av5", "ask_volume5"],
            Self::BidPrice5 => &["bp5", "bid_price5"],
            Self::BidVolume5 => &["bv5", "bid_volume5"],
            Self::Amount => &["amt", "amount"],
            Self::OpenInterest => &["oi", "open_interest"],
        }
    }

    const fn value_kind(self) -> &'static str {
        match self {
            Self::Time => "timestamp",
            Self::Id
            | Self::Volume
            | Self::OpenOi
            | Self::CloseOi
            | Self::AskVolume1
            | Self::BidVolume1
            | Self::AskVolume2
            | Self::BidVolume2
            | Self::AskVolume3
            | Self::BidVolume3
            | Self::AskVolume4
            | Self::BidVolume4
            | Self::AskVolume5
            | Self::BidVolume5
            | Self::OpenInterest => "integer",
            Self::Amount => "decimal",
            _ => "price",
        }
    }
}

const KLINE_FIELDS: [Field; 9] = [
    Field::Time,
    Field::Id,
    Field::Open,
    Field::High,
    Field::Low,
    Field::Close,
    Field::Volume,
    Field::OpenOi,
    Field::CloseOi,
];

const TICK_FIELDS: [Field; 29] = [
    Field::Time,
    Field::Id,
    Field::LastPrice,
    Field::Average,
    Field::Highest,
    Field::Lowest,
    Field::AskPrice1,
    Field::AskVolume1,
    Field::BidPrice1,
    Field::BidVolume1,
    Field::AskPrice2,
    Field::AskVolume2,
    Field::BidPrice2,
    Field::BidVolume2,
    Field::AskPrice3,
    Field::AskVolume3,
    Field::BidPrice3,
    Field::BidVolume3,
    Field::AskPrice4,
    Field::AskVolume4,
    Field::BidPrice4,
    Field::BidVolume4,
    Field::AskPrice5,
    Field::AskVolume5,
    Field::BidPrice5,
    Field::BidVolume5,
    Field::Volume,
    Field::Amount,
    Field::OpenInterest,
];

#[derive(Debug, Clone, Copy)]
enum CellValue {
    Timestamp(i64),
    Integer(i64),
    Float { value: f64, price: bool },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LlmTimestampPrecision {
    Minute,
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl LlmTimestampPrecision {
    fn from_timestamps(values: &[i64]) -> Self {
        const MINUTE_NS: i64 = 60 * 1_000_000_000;
        const SECOND_NS: i64 = 1_000_000_000;
        const MILLISECOND_NS: i64 = 1_000_000;
        const MICROSECOND_NS: i64 = 1_000;

        if values.iter().all(|value| value.rem_euclid(MINUTE_NS) == 0) {
            Self::Minute
        } else if values.iter().all(|value| value.rem_euclid(SECOND_NS) == 0) {
            Self::Second
        } else if values
            .iter()
            .all(|value| value.rem_euclid(MILLISECOND_NS) == 0)
        {
            Self::Millisecond
        } else if values
            .iter()
            .all(|value| value.rem_euclid(MICROSECOND_NS) == 0)
        {
            Self::Microsecond
        } else {
            Self::Nanosecond
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Minute => "m",
            Self::Second => "s",
            Self::Millisecond => "ms",
            Self::Microsecond => "us",
            Self::Nanosecond => "ns",
        }
    }

    fn format(self, value: i64, timezone: LlmTimezone) -> Result<String, CliError> {
        let seconds = value.div_euclid(1_000_000_000);
        let nanos = u32::try_from(value.rem_euclid(1_000_000_000)).map_err(|_| {
            CliError::Usage("timestamp nanosecond remainder is invalid".to_string())
        })?;
        let utc = DateTime::<Utc>::from_timestamp(seconds, nanos).ok_or_else(|| {
            CliError::Usage(format!("timestamp {value} is outside RFC 3339 range"))
        })?;
        Ok(match timezone {
            LlmTimezone::Utc => self.format_utc(utc),
            LlmTimezone::Shanghai => {
                let shanghai = FixedOffset::east_opt(8 * 60 * 60)
                    .expect("Asia/Shanghai UTC+08:00 offset must be valid");
                self.format_shanghai(utc.with_timezone(&shanghai))
            }
        })
    }

    fn format_utc(self, datetime: DateTime<Utc>) -> String {
        match self {
            Self::Minute => datetime.format("%Y-%m-%dT%H:%MZ").to_string(),
            Self::Second => datetime.to_rfc3339_opts(SecondsFormat::Secs, true),
            Self::Millisecond => datetime.to_rfc3339_opts(SecondsFormat::Millis, true),
            Self::Microsecond => datetime.to_rfc3339_opts(SecondsFormat::Micros, true),
            Self::Nanosecond => datetime.to_rfc3339_opts(SecondsFormat::Nanos, true),
        }
    }

    fn format_shanghai(self, datetime: DateTime<FixedOffset>) -> String {
        match self {
            Self::Minute => datetime.format("%Y-%m-%dT%H:%M%:z").to_string(),
            Self::Second => datetime.to_rfc3339_opts(SecondsFormat::Secs, true),
            Self::Millisecond => datetime.to_rfc3339_opts(SecondsFormat::Millis, true),
            Self::Microsecond => datetime.to_rfc3339_opts(SecondsFormat::Micros, true),
            Self::Nanosecond => datetime.to_rfc3339_opts(SecondsFormat::Nanos, true),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct LlmTimeCodec {
    mode: LlmTimeMode,
    timezone: LlmTimezone,
    precision: LlmTimestampPrecision,
    reference_ns: i64,
    offset_unit_ns: i64,
}

impl LlmTimeCodec {
    fn for_block(block: &QueryBlock, settings: &QuerySettings) -> Self {
        let timestamps = llm_timestamps(block);
        Self {
            mode: settings.llm_time_mode(),
            timezone: settings.llm_timezone(),
            precision: LlmTimestampPrecision::from_timestamps(timestamps.as_slice()),
            reference_ns: block.spec.start_ns,
            offset_unit_ns: llm_offset_unit_ns(block, timestamps.as_slice()),
        }
    }

    fn format_timestamp(self, value: i64) -> Result<String, CliError> {
        self.precision.format(value, self.timezone)
    }

    fn offset(self, value: i64) -> String {
        let offset = i128::from(value) - i128::from(self.reference_ns);
        (offset / i128::from(self.offset_unit_ns)).to_string()
    }

    fn offset_unit_label(self) -> String {
        format_duration_label_from_ns(self.offset_unit_ns)
    }
}

#[derive(Debug, Clone)]
struct LlmBlock {
    weight: u32,
    row_lines: Vec<String>,
    important_indices: Vec<usize>,
    prefix_lines: Vec<String>,
    header: String,
}

impl LlmBlock {
    fn render(&self, selected: &[usize], compression: &str) -> String {
        let mut lines = self.prefix_lines.clone();
        let mut data_line = vec![
            "data".to_string(),
            "compression".to_string(),
            compression.to_string(),
        ];
        if selected.len() == self.row_lines.len() {
            data_line.extend(["rows".to_string(), selected.len().to_string()]);
        } else {
            data_line.extend([
                "rows_original".to_string(),
                self.row_lines.len().to_string(),
                "rows_emitted".to_string(),
                selected.len().to_string(),
            ]);
        }
        lines.push(csv_line(data_line));
        lines.push(self.header.clone());
        lines.extend(selected.iter().map(|index| self.row_lines[*index].clone()));
        lines.push(csv_line(vec![
            "block_end".to_string(),
            "rows".to_string(),
            selected.len().to_string(),
        ]));
        lines.join("\n")
    }
}

pub(crate) async fn execute(
    cache_dir: Option<&Path>,
    cache_kind: CacheKind,
    market: MarketKind,
    output_format: OutputFormat,
    args: QueryArgs,
) -> Result<QueryExecution, CliError> {
    if !matches!(cache_kind, CacheKind::Tick) {
        return Err(CliError::Usage(
            "query does not use --kind; select rows with --series tick|kline".to_string(),
        ));
    }
    if !matches!(market, MarketKind::Futures) {
        return Err(CliError::Usage(
            "cache-backed query currently supports only --market futures".to_string(),
        ));
    }

    if let Some(QuerySubcommand::Schema(schema)) = args.command {
        return schema_execution(output_format, schema);
    }

    let (settings, specs) = parse_request(args.request)?;
    validate_output_settings(output_format, &settings)?;
    let (_, canonical_cache_dir) = tqsdk_cache::open_read_only_cache(cache_dir)?;
    let requests = specs
        .iter()
        .map(QuerySpec::to_history_request)
        .collect::<Vec<_>>();
    let builder = BacktestHistoryClient::builder(canonical_cache_dir.clone())
        .policy(settings.policy.into_history_policy())
        .collect_limit_bytes(settings.max_memory_bytes);
    let client = match settings.policy {
        QueryPolicy::CacheOnly => builder,
        QueryPolicy::RemoteOnMiss => builder.auth_env(),
    }
    .build()?;
    let collected = client
        .query_batch(requests)
        .await?
        .collect_all(settings.max_memory_bytes)
        .await?;
    let mut artifact = collect_artifact(canonical_cache_dir, settings, specs, collected)?;

    if !artifact.failures.is_empty() && !artifact.settings.allow_partial {
        return Err(first_failure_error(artifact.failures.as_slice()));
    }
    if matches!(output_format, OutputFormat::LlmCsv) {
        retain_llm_verified_blocks(&mut artifact)?;
    }
    if artifact.blocks.is_empty() && artifact.failures.is_empty() {
        return Err(CliError::Data(DataError::InvalidResponse(
            "backtest history query completed without a terminal block".to_string(),
        )));
    }

    let exit_code = if artifact.failures.is_empty() { 0 } else { 1 };
    match output_format {
        OutputFormat::Jsonl => Ok(QueryExecution::Raw(QueryRawOutput {
            payload: render_jsonl(&artifact)?.into_bytes(),
            output_path: artifact.settings.output_path.clone(),
            exit_code,
        })),
        OutputFormat::LlmCsv => Ok(QueryExecution::Raw(QueryRawOutput {
            payload: render_llm_csv(&artifact)?.into_bytes(),
            output_path: artifact.settings.output_path.clone(),
            exit_code,
        })),
        OutputFormat::Json | OutputFormat::Text => Ok(QueryExecution::Summary(CommandOutcome {
            value: summary_value(&artifact)?,
            exit_code,
        })),
    }
}

fn schema_execution(
    output_format: OutputFormat,
    args: QuerySchemaArgs,
) -> Result<QueryExecution, CliError> {
    if !matches!(output_format, OutputFormat::Json | OutputFormat::Text) {
        return Err(CliError::Usage(
            "query schema supports only --output-format text or json".to_string(),
        ));
    }
    let fields = schema_fields(args.series)
        .iter()
        .map(|field| {
            json!({
                "name": field.code(),
                "aliases": field.aliases(),
                "value_kind": field.value_kind(),
            })
        })
        .collect::<Vec<_>>();
    let defaults = default_fields(args.series)
        .into_iter()
        .map(|field| field.code())
        .collect::<Vec<_>>();
    Ok(QueryExecution::Summary(CommandOutcome {
        value: json!({
            "schema_version": QUERY_SCHEMA_VERSION,
            "command": "query",
            "subcommand": "schema",
            "series": args.series.as_str(),
            "default_fields": defaults,
            "fields": fields,
        }),
        exit_code: 0,
    }))
}

fn parse_request(args: QueryRequestArgs) -> Result<(QuerySettings, Vec<QuerySpec>), CliError> {
    if args.max_memory_bytes == 0 {
        return Err(CliError::Usage(
            "--max-memory-bytes must be greater than zero".to_string(),
        ));
    }
    if args.data_token_budget == Some(0) {
        return Err(CliError::Usage(
            "--data-token-budget must be greater than zero".to_string(),
        ));
    }
    let settings = QuerySettings {
        policy: args.policy,
        timestamp: args.timestamp,
        llm_time: args.llm_time,
        llm_timezone: args.llm_timezone,
        number_format: args.number_format,
        max_memory_bytes: args.max_memory_bytes,
        allow_partial: args.allow_partial,
        data_token_budget: args.data_token_budget,
        compression: args.compression,
        focus: args.focus,
        output_path: args.output.clone(),
    };
    let specs = match &args.request_file {
        Some(path) => parse_request_file(path, &args)?,
        None => parse_simple_requests(&args)?,
    };
    validate_scaled_int(&settings, specs.as_slice())?;
    Ok((settings, specs))
}

fn parse_simple_requests(args: &QueryRequestArgs) -> Result<Vec<QuerySpec>, CliError> {
    let symbols = normalized_symbols(args.symbols.as_slice())?;
    let series = args.series.ok_or_else(|| {
        CliError::Usage("query requires --series tick|kline or --request-file".to_string())
    })?;
    let start = args.start.as_deref().ok_or_else(|| {
        CliError::Usage("query requires --start RFC3339 or --request-file".to_string())
    })?;
    let end = args.end.as_deref().ok_or_else(|| {
        CliError::Usage("query requires --end RFC3339 or --request-file".to_string())
    })?;
    let (duration, duration_ns) = parse_series_duration(series, args.period.as_deref())?;
    let start_ns = parse_rfc3339_ns(start, "--start")?;
    let end_ns = parse_rfc3339_ns(end, "--end")?;
    validate_range(start_ns, end_ns)?;
    let fields = resolve_fields(series, args.fields.as_slice())?;
    symbols
        .into_iter()
        .enumerate()
        .map(|(index, symbol)| {
            Ok(QuerySpec {
                request_id: u64::try_from(index + 1).map_err(|_| {
                    CliError::Usage("too many query symbols for request ids".to_string())
                })?,
                symbol,
                series,
                duration,
                duration_ns,
                start_ns,
                end_ns,
                fields: fields.clone(),
                weight: 1,
                price_tick: args.price_tick,
            })
        })
        .collect()
}

fn parse_request_file(path: &Path, args: &QueryRequestArgs) -> Result<Vec<QuerySpec>, CliError> {
    if !args.symbols.is_empty()
        || args.series.is_some()
        || args.start.is_some()
        || args.end.is_some()
        || args.period.is_some()
        || !args.fields.is_empty()
    {
        return Err(CliError::Usage(
            "--request-file cannot be combined with --symbol, --series, --start, --end, --period, or --fields"
                .to_string(),
        ));
    }
    let contents = fs::read_to_string(path)?;
    let file: QueryRequestFile = toml::from_str(&contents).map_err(|error| {
        CliError::Usage(format!(
            "invalid query request TOML {}: {error}",
            path.display()
        ))
    })?;
    if file.version != QUERY_SCHEMA_VERSION {
        return Err(CliError::Usage(format!(
            "unsupported query request TOML version {}; expected {QUERY_SCHEMA_VERSION}",
            file.version
        )));
    }
    if file.requests.is_empty() {
        return Err(CliError::Usage(
            "query request TOML must contain at least one [[request]] table".to_string(),
        ));
    }
    file.requests
        .into_iter()
        .enumerate()
        .map(|(index, request)| {
            let symbol = normalized_one_symbol(request.symbol.as_str())?;
            let (duration, duration_ns) =
                parse_series_duration(request.series, request.period.as_deref())?;
            let start_ns = parse_rfc3339_ns(request.start.as_str(), "request.start")?;
            let end_ns = parse_rfc3339_ns(request.end.as_str(), "request.end")?;
            validate_range(start_ns, end_ns)?;
            let fields = resolve_fields(
                request.series,
                request.fields.unwrap_or_default().as_slice(),
            )?;
            let weight = request.weight.unwrap_or(1);
            if weight == 0 {
                return Err(CliError::Usage(format!(
                    "request {} weight must be greater than zero",
                    index + 1
                )));
            }
            Ok(QuerySpec {
                request_id: u64::try_from(index + 1).map_err(|_| {
                    CliError::Usage("too many TOML requests for request ids".to_string())
                })?,
                symbol,
                series: request.series,
                duration,
                duration_ns,
                start_ns,
                end_ns,
                fields,
                weight,
                price_tick: request.price_tick.or(args.price_tick),
            })
        })
        .collect()
}

fn validate_scaled_int(settings: &QuerySettings, specs: &[QuerySpec]) -> Result<(), CliError> {
    if !matches!(settings.number_format, NumberFormat::ScaledInt) {
        return Ok(());
    }
    for spec in specs {
        let price_tick = spec.price_tick.ok_or_else(|| {
            CliError::Usage(format!(
                "--number-format scaled-int requires --price-tick for {} (or price_tick in its TOML request)",
                spec.symbol
            ))
        })?;
        if !price_tick.is_finite() || price_tick <= 0.0 {
            return Err(CliError::Usage(format!(
                "price tick for {} must be a finite number greater than zero",
                spec.symbol
            )));
        }
    }
    Ok(())
}

fn validate_output_settings(
    output_format: OutputFormat,
    settings: &QuerySettings,
) -> Result<(), CliError> {
    let raw = matches!(output_format, OutputFormat::Jsonl | OutputFormat::LlmCsv);
    if settings.output_path.is_some() && !raw {
        return Err(CliError::Usage(
            "--output is supported only by --output-format jsonl or llm-csv".to_string(),
        ));
    }
    if settings.data_token_budget.is_some() && !matches!(output_format, OutputFormat::LlmCsv) {
        return Err(CliError::Usage(
            "--data-token-budget requires --output-format llm-csv".to_string(),
        ));
    }
    if settings.llm_time.is_some() && !matches!(output_format, OutputFormat::LlmCsv) {
        return Err(CliError::Usage(
            "--llm-time requires --output-format llm-csv".to_string(),
        ));
    }
    if settings.llm_timezone.is_some() && !matches!(output_format, OutputFormat::LlmCsv) {
        return Err(CliError::Usage(
            "--llm-timezone requires --output-format llm-csv".to_string(),
        ));
    }
    if matches!(settings.compression, CompressionMode::Off)
        && !matches!(output_format, OutputFormat::LlmCsv)
    {
        return Err(CliError::Usage(
            "--compression requires --output-format llm-csv".to_string(),
        ));
    }
    if !matches!(settings.focus, FocusMode::Balanced)
        && !matches!(output_format, OutputFormat::LlmCsv)
    {
        return Err(CliError::Usage(
            "--focus requires --output-format llm-csv".to_string(),
        ));
    }
    Ok(())
}

impl QuerySpec {
    fn to_history_request(&self) -> BacktestHistoryRequest {
        match self.series {
            QuerySeries::Tick => BacktestHistoryRequest::tick(
                self.request_id,
                self.symbol.clone(),
                self.start_ns,
                self.end_ns,
            ),
            QuerySeries::Kline => BacktestHistoryRequest::kline(
                self.request_id,
                self.symbol.clone(),
                self.duration.expect("validated Kline query has a duration"),
                self.start_ns,
                self.end_ns,
            ),
        }
    }
}

fn collect_artifact(
    cache_dir: PathBuf,
    settings: QuerySettings,
    specs: Vec<QuerySpec>,
    collected: tqsdk_data::BacktestHistoryCollectedBatch,
) -> Result<QueryArtifact, CliError> {
    let query_hash = query_hash(specs.as_slice(), &settings);
    let query_id = format!("q-{}", &query_hash[..16]);
    let mut by_request_id = collected
        .completed
        .into_iter()
        .map(|result| (result.request.request_id, result))
        .collect::<BTreeMap<_, _>>();
    let metadata_cache = BacktestHistoryMetadataCache::open_read_only(cache_dir.as_path());
    let mut blocks = Vec::new();
    let mut failures = collected
        .failed
        .into_iter()
        .map(QueryFailure::from_history_failure)
        .collect::<Vec<_>>();

    for spec in specs {
        let Some(collected) = by_request_id.remove(&spec.request_id) else {
            if !failures
                .iter()
                .any(|failure| failure.request_id == spec.request_id)
            {
                failures.push(QueryFailure {
                    request_id: spec.request_id,
                    symbol: spec.symbol.clone(),
                    code: "missing_terminal",
                    message: "request produced no terminal result".to_string(),
                });
            }
            continue;
        };
        validate_terminal_coverage(&collected.request)?;
        let metadata = load_metadata(&metadata_cache, &collected.request)?;
        let data_hash = data_hash(&spec, &collected.rows, &settings)?;
        blocks.push(QueryBlock {
            block_id: format!("b{}", spec.request_id),
            spec,
            request: collected.request,
            rows: collected.rows,
            metadata,
            data_hash,
        });
    }
    failures.sort_by_key(|failure| failure.request_id);
    Ok(QueryArtifact {
        cache_dir,
        query_id,
        query_hash,
        settings,
        blocks,
        failures,
    })
}

impl QueryFailure {
    fn from_history_failure(value: BacktestHistoryRequestFailure) -> Self {
        Self {
            request_id: value.request_id,
            symbol: value.symbol,
            code: "request_failed",
            message: value.error,
        }
    }
}

fn validate_terminal_coverage(report: &BacktestHistoryRequestReport) -> Result<(), CliError> {
    if !matches!(report.coverage.finality, BacktestHistoryFinality::Final) {
        return Err(CliError::Data(DataError::InvalidResponse(format!(
            "terminal request {} is not final",
            report.request_id
        ))));
    }
    let mut ranges = report.coverage.cached_ranges.clone();
    ranges.extend(report.coverage.remote_filled_ranges.iter().copied());
    if !ranges_cover(report.coverage.requested_range, ranges.as_slice()) {
        return Err(CliError::Data(DataError::InvalidResponse(format!(
            "terminal request {} does not prove full requested coverage",
            report.request_id
        ))));
    }
    Ok(())
}

fn load_metadata(
    cache: &BacktestHistoryMetadataCache,
    report: &BacktestHistoryRequestReport,
) -> Result<MetadataStatus, CliError> {
    let Some(snapshot) = cache.load_active(report.symbol.as_str())? else {
        return Ok(MetadataStatus::Missing {
            reason: "active metadata sidecar is absent".to_string(),
        });
    };
    if report.snapshot_hash.is_empty() {
        return Ok(MetadataStatus::Missing {
            reason: "terminal request did not bind a metadata snapshot".to_string(),
        });
    }
    if snapshot.snapshot_hash != report.snapshot_hash {
        return Ok(MetadataStatus::Missing {
            reason: format!(
                "active metadata snapshot {} does not match terminal snapshot {}",
                snapshot.snapshot_hash, report.snapshot_hash
            ),
        });
    }
    Ok(MetadataStatus::Verified(snapshot))
}

fn retain_llm_verified_blocks(artifact: &mut QueryArtifact) -> Result<(), CliError> {
    let mut retained = Vec::new();
    let mut metadata_failures = Vec::new();
    for block in artifact.blocks.drain(..) {
        match &block.metadata {
            MetadataStatus::Verified(_) => retained.push(block),
            MetadataStatus::Missing { reason } => metadata_failures.push(QueryFailure {
                request_id: block.spec.request_id,
                symbol: block.spec.symbol.clone(),
                code: "metadata_unavailable",
                message: reason.clone(),
            }),
        }
    }
    if !metadata_failures.is_empty() && !artifact.settings.allow_partial {
        return Err(CliError::Data(DataError::InvalidResponse(format!(
            "LLM output requires verified metadata sidecars: {}",
            metadata_failures
                .iter()
                .map(|failure| format!("{} ({})", failure.symbol, failure.message))
                .collect::<Vec<_>>()
                .join(", ")
        ))));
    }
    artifact.blocks = retained;
    artifact.failures.extend(metadata_failures);
    artifact.failures.sort_by_key(|failure| failure.request_id);
    Ok(())
}

fn first_failure_error(failures: &[QueryFailure]) -> CliError {
    let failure = &failures[0];
    CliError::Data(DataError::RequestFailed {
        request_id: failure.request_id,
        message: failure.message.clone(),
        emitted_rows: 0,
    })
}

fn summary_value(artifact: &QueryArtifact) -> Result<Value, CliError> {
    let blocks = artifact
        .blocks
        .iter()
        .map(|block| block_summary_value(artifact, block))
        .collect::<Result<Vec<_>, _>>()?;
    let failures = artifact
        .failures
        .iter()
        .map(|failure| {
            json!({
                "request_id": failure.request_id,
                "symbol": failure.symbol,
                "code": failure.code,
                "message": failure.message,
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "schema_version": QUERY_SCHEMA_VERSION,
        "command": "query",
        "cache_dir": artifact.cache_dir,
        "query_id": artifact.query_id,
        "query_hash": artifact.query_hash,
        "policy": artifact.settings.policy.as_str(),
        "partial": !artifact.failures.is_empty(),
        "blocks": blocks,
        "failures": failures,
    }))
}

fn block_summary_value(artifact: &QueryArtifact, block: &QueryBlock) -> Result<Value, CliError> {
    let metadata = metadata_value(&block.metadata)?;
    Ok(json!({
        "block_id": block.block_id,
        "request_id": block.spec.request_id,
        "symbol": block.spec.symbol,
        "series": block.spec.series.as_str(),
        "period_ns": block.spec.duration_ns,
        "requested": range_value(block.spec.start_ns, block.spec.end_ns),
        "rows": block.rows.len(),
        "fields": block.spec.fields.iter().map(|field| field.code()).collect::<Vec<_>>(),
        "source": if block.request.remote_used { "remote-on-miss" } else { "cache" },
        "finality": finality_value(block.request.coverage.finality),
        "coverage": coverage_value(&block.request),
        "physical_segments": segments_value(block.request.physical_segments.as_slice()),
        "snapshot_hash": block.request.snapshot_hash,
        "metadata": metadata,
        "data_hash": block.data_hash,
        "drill_down_id": drill_down_id(artifact, block),
    }))
}

fn render_jsonl(artifact: &QueryArtifact) -> Result<String, CliError> {
    let mut lines = Vec::new();
    lines.push(serde_json::to_string(&json!({
        "record": "manifest",
        "protocol": JSONL_PROTOCOL,
        "query_id": artifact.query_id,
        "query_hash": artifact.query_hash,
        "policy": artifact.settings.policy.as_str(),
        "timestamp": artifact.settings.timestamp.as_str(),
        "number_format": artifact.settings.number_format.as_str(),
        "partial": !artifact.failures.is_empty(),
        "blocks": artifact.blocks.len(),
    }))?);
    for block in &artifact.blocks {
        lines.push(serde_json::to_string(&json!({
            "record": "block",
            "block_id": block.block_id,
            "request_id": block.spec.request_id,
            "symbol": block.spec.symbol,
            "series": block.spec.series.as_str(),
            "period_ns": block.spec.duration_ns,
            "requested": range_value(block.spec.start_ns, block.spec.end_ns),
            "fields": block.spec.fields.iter().map(|field| field.code()).collect::<Vec<_>>(),
            "source": if block.request.remote_used { "remote-on-miss" } else { "cache" },
            "finality": finality_value(block.request.coverage.finality),
            "coverage": coverage_value(&block.request),
            "physical_segments": segments_value(block.request.physical_segments.as_slice()),
            "metadata": metadata_value(&block.metadata)?,
            "data_hash": block.data_hash,
            "drill_down_id": drill_down_id(artifact, block),
        }))?);
        for row in rows_as_json(block, &artifact.settings)? {
            lines.push(serde_json::to_string(&json!({
                "record": "row",
                "block_id": block.block_id,
                "data": row,
            }))?);
        }
        lines.push(serde_json::to_string(&json!({
            "record": "complete",
            "block_id": block.block_id,
            "rows": block.rows.len(),
            "data_hash": block.data_hash,
        }))?);
    }
    for failure in &artifact.failures {
        lines.push(serde_json::to_string(&json!({
            "record": "gap",
            "request_id": failure.request_id,
            "symbol": failure.symbol,
            "code": failure.code,
            "message": failure.message,
        }))?);
    }
    lines.push(serde_json::to_string(&json!({
        "record": "end",
        "protocol": JSONL_PROTOCOL,
        "query_id": artifact.query_id,
        "query_hash": artifact.query_hash,
        "status": if artifact.failures.is_empty() { "success" } else { "partial" },
    }))?);
    Ok(format!("{}\n", lines.join("\n")))
}

fn render_llm_csv(artifact: &QueryArtifact) -> Result<String, CliError> {
    let blocks = artifact
        .blocks
        .iter()
        .map(|block| LlmBlock::from_query_block(block, artifact))
        .collect::<Result<Vec<_>, _>>()?;
    let mut selections = blocks
        .iter()
        .map(|block| (0..block.row_lines.len()).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let (full, full_tokens) = render_llm_document(
        artifact,
        blocks.as_slice(),
        selections.as_slice(),
        "lossless",
    );
    let Some(budget) = artifact.settings.data_token_budget else {
        return Ok(full);
    };
    if full_tokens <= budget {
        return Ok(full);
    }
    if matches!(artifact.settings.compression, CompressionMode::Off) {
        return Err(CliError::Usage(format!(
            "LLM payload estimates {full_tokens} tokens, exceeding --data-token-budget {budget}; enable compression or raise the budget"
        )));
    }

    selections = allocate_compressed_rows(artifact, blocks.as_slice(), budget)?;
    let (payload, estimated_tokens) =
        render_llm_document(artifact, blocks.as_slice(), selections.as_slice(), "lossy");
    if estimated_tokens > budget {
        return Err(CliError::Usage(format!(
            "--data-token-budget {budget} is too small for required tqllm-csv metadata ({estimated_tokens} estimated tokens)"
        )));
    }
    Ok(payload)
}

fn allocate_compressed_rows(
    artifact: &QueryArtifact,
    blocks: &[LlmBlock],
    budget: usize,
) -> Result<Vec<Vec<usize>>, CliError> {
    let empty = vec![Vec::new(); blocks.len()];
    let (_, base_tokens) = render_llm_document(artifact, blocks, empty.as_slice(), "lossy");
    if base_tokens > budget {
        return Err(CliError::Usage(format!(
            "--data-token-budget {budget} is too small for required tqllm-csv metadata ({base_tokens} estimated tokens)"
        )));
    }
    let residual = budget - base_tokens;
    let total_weight = blocks
        .iter()
        .map(|block| usize::try_from(block.weight).unwrap_or(usize::MAX))
        .try_fold(0_usize, usize::checked_add)
        .ok_or_else(|| CliError::Usage("query block weights overflow".to_string()))?;
    let mut counts = blocks
        .iter()
        .map(|block| {
            if block.row_lines.is_empty() {
                return Ok(0_usize);
            }
            let allowance = residual
                .saturating_mul(usize::try_from(block.weight).unwrap_or(usize::MAX))
                / total_weight.max(1);
            let raw_tokens = estimate_tokens(block.row_lines.join("\n").as_str()).max(1);
            let estimated = block.row_lines.len().saturating_mul(allowance) / raw_tokens;
            Ok(estimated.min(block.row_lines.len()))
        })
        .collect::<Result<Vec<_>, CliError>>()?;
    let mut selections = selected_rows(blocks, counts.as_slice());
    let (_, mut estimated_tokens) =
        render_llm_document(artifact, blocks, selections.as_slice(), "lossy");
    while estimated_tokens > budget {
        let Some((index, _)) = counts
            .iter()
            .enumerate()
            .filter(|(_, count)| **count > 0)
            .max_by_key(|(index, count)| (**count, blocks[*index].weight))
        else {
            break;
        };
        counts[index] -= 1;
        selections = selected_rows(blocks, counts.as_slice());
        (_, estimated_tokens) =
            render_llm_document(artifact, blocks, selections.as_slice(), "lossy");
    }
    Ok(selections)
}

fn selected_rows(blocks: &[LlmBlock], counts: &[usize]) -> Vec<Vec<usize>> {
    blocks
        .iter()
        .zip(counts)
        .map(|(block, count)| select_row_indices(block, *count))
        .collect()
}

fn select_row_indices(block: &LlmBlock, count: usize) -> Vec<usize> {
    if count == 0 || block.row_lines.is_empty() {
        return Vec::new();
    }
    if count >= block.row_lines.len() {
        return (0..block.row_lines.len()).collect();
    }
    let mut selected = BTreeSet::new();
    selected.insert(0);
    if count > 1 {
        selected.insert(block.row_lines.len() - 1);
    }
    for index in &block.important_indices {
        if selected.len() >= count {
            break;
        }
        selected.insert(*index);
    }
    let denominator = count.saturating_sub(1).max(1);
    for offset in 0..count {
        if selected.len() >= count {
            break;
        }
        let index = offset.saturating_mul(block.row_lines.len() - 1) / denominator;
        selected.insert(index);
    }
    if selected.len() < count {
        for index in 0..block.row_lines.len() {
            if selected.len() >= count {
                break;
            }
            selected.insert(index);
        }
    }
    selected.into_iter().take(count).collect()
}

fn render_llm_document(
    artifact: &QueryArtifact,
    blocks: &[LlmBlock],
    selections: &[Vec<usize>],
    compression: &str,
) -> (String, usize) {
    let mut lines = vec![csv_line(vec![
        "protocol".to_string(),
        LLM_CSV_PROTOCOL.to_string(),
    ])];
    let mut metadata = vec![
        "meta".to_string(),
        "model".to_string(),
        "gpt-5.6".to_string(),
        "numbers".to_string(),
        artifact.settings.number_format.as_str().to_string(),
        "compression".to_string(),
        compression.to_string(),
        "partial".to_string(),
        (!artifact.failures.is_empty()).to_string(),
    ];
    if compression == "lossy" {
        metadata.extend([
            "focus".to_string(),
            artifact.settings.focus.as_str().to_string(),
        ]);
    }
    lines.push(csv_line(metadata));
    for (block, selected) in blocks.iter().zip(selections) {
        lines.push(block.render(selected.as_slice(), compression));
    }
    for failure in &artifact.failures {
        lines.push(csv_line(vec![
            "gap".to_string(),
            failure.request_id.to_string(),
            failure.symbol.clone(),
            failure.code.to_string(),
            protocol_text(failure.message.as_str()),
        ]));
    }
    lines.push(csv_line(vec![
        "document_end".to_string(),
        "status".to_string(),
        if artifact.failures.is_empty() {
            "success".to_string()
        } else {
            "partial".to_string()
        },
    ]));
    let payload = format!("{}\n", lines.join("\n"));
    let estimated_tokens = estimate_tokens(payload.as_str());
    (payload, estimated_tokens)
}

impl LlmBlock {
    fn from_query_block(block: &QueryBlock, artifact: &QueryArtifact) -> Result<Self, CliError> {
        match &block.metadata {
            MetadataStatus::Verified(_) => {}
            MetadataStatus::Missing { reason } => {
                return Err(CliError::Data(DataError::InvalidResponse(format!(
                    "LLM block {} is missing metadata: {reason}",
                    block.block_id
                ))));
            }
        }
        let time_codec = LlmTimeCodec::for_block(block, &artifact.settings);
        let rows = rows_as_llm_csv(block, &artifact.settings, time_codec)?;
        let mut block_cells = vec![
            "block".to_string(),
            block.block_id.clone(),
            "symbol".to_string(),
            block.spec.symbol.clone(),
            "series".to_string(),
            block.spec.series.as_str().to_string(),
        ];
        if let Some(duration) = block.spec.duration {
            block_cells.extend(["period".to_string(), format_duration_label(duration)]);
        }
        block_cells.extend([
            "rows".to_string(),
            block.rows.len().to_string(),
            "source".to_string(),
            llm_source(&block.request).to_string(),
            "final".to_string(),
            "true".to_string(),
        ]);
        if matches!(artifact.settings.number_format, NumberFormat::ScaledInt) {
            let price_tick = block.spec.price_tick.ok_or_else(|| {
                CliError::Usage(format!(
                    "--number-format scaled-int requires --price-tick for {}",
                    block.spec.symbol
                ))
            })?;
            block_cells.extend(["price_tick".to_string(), format_decimal(price_tick)]);
        }
        let underlying = llm_underlying_symbol(block);
        if let Some(underlying) = underlying {
            block_cells.extend(["underlying".to_string(), underlying.to_string()]);
        }

        let mut prefix_lines = vec![
            csv_line(block_cells),
            llm_time_line(block, time_codec)?,
            llm_columns_line(block, time_codec),
        ];
        if underlying.is_none() {
            for segment in &block.request.physical_segments {
                prefix_lines.push(csv_line(vec![
                    "segment".to_string(),
                    "underlying".to_string(),
                    segment.physical_symbol.clone(),
                    "start".to_string(),
                    time_codec.format_timestamp(segment.start_ns)?,
                    "end".to_string(),
                    time_codec.format_timestamp(segment.end_ns)?,
                ]));
            }
        }
        if let Some(summary) = summary_line(block, &artifact.settings)? {
            prefix_lines.push(summary);
        }
        Ok(Self {
            weight: block.spec.weight,
            important_indices: important_indices(block, artifact.settings.focus),
            row_lines: rows,
            prefix_lines,
            header: llm_header(block, time_codec),
        })
    }
}

fn llm_source(report: &BacktestHistoryRequestReport) -> &'static str {
    if !report.remote_used {
        "cache"
    } else if report.coverage.cached_ranges.is_empty() {
        "remote"
    } else {
        "cache+remote"
    }
}

fn llm_underlying_symbol(block: &QueryBlock) -> Option<&str> {
    let first = block.request.physical_segments.first()?;
    if first.physical_symbol == block.spec.symbol
        || !block
            .request
            .physical_segments
            .iter()
            .all(|segment| segment.physical_symbol == first.physical_symbol)
    {
        return None;
    }
    Some(first.physical_symbol.as_str())
}

fn llm_time_line(block: &QueryBlock, codec: LlmTimeCodec) -> Result<String, CliError> {
    let mut cells = vec![
        "time".to_string(),
        "mode".to_string(),
        codec.mode.as_str().to_string(),
        "timezone".to_string(),
        codec.timezone.label().to_string(),
    ];
    if matches!(codec.mode, LlmTimeMode::Iso | LlmTimeMode::Both) {
        cells.extend(["precision".to_string(), codec.precision.label().to_string()]);
    }
    if matches!(codec.mode, LlmTimeMode::Offset | LlmTimeMode::Both) {
        cells.extend(["unit".to_string(), codec.offset_unit_label()]);
    }
    cells.extend([
        "ref".to_string(),
        codec.format_timestamp(block.spec.start_ns)?,
        "end".to_string(),
    ]);
    match codec.mode {
        LlmTimeMode::Iso => cells.push(codec.format_timestamp(block.spec.end_ns)?),
        LlmTimeMode::Offset | LlmTimeMode::Both => cells.push(codec.offset(block.spec.end_ns)),
    }
    cells.extend([
        "end_exclusive".to_string(),
        "true".to_string(),
        match block.spec.series {
            QuerySeries::Tick => "row_time".to_string(),
            QuerySeries::Kline => "bar_time".to_string(),
        },
        match block.spec.series {
            QuerySeries::Tick => "event".to_string(),
            QuerySeries::Kline => "start".to_string(),
        },
    ]);
    Ok(csv_line(cells))
}

fn llm_columns_line(block: &QueryBlock, codec: LlmTimeCodec) -> String {
    let mut cells = vec!["columns".to_string()];
    for field in &block.spec.fields {
        cells.push(format!(
            "{}={}",
            field.code(),
            llm_field_name(*field, block.spec.series)
        ));
        if matches!(field, Field::Time) && matches!(codec.mode, LlmTimeMode::Both) {
            cells.push(format!("dt=offset_{}", codec.offset_unit_label()));
        }
    }
    csv_line(cells)
}

fn llm_field_name(field: Field, series: QuerySeries) -> &'static str {
    match field {
        Field::Time => "time",
        Field::Id => "id",
        Field::Open => "open",
        Field::High => "high",
        Field::Low => "low",
        Field::Close => "close",
        Field::Volume => match series {
            QuerySeries::Tick => "cumulative_volume",
            QuerySeries::Kline => "bar_volume",
        },
        Field::OpenOi => "open_oi",
        Field::CloseOi => "close_oi",
        Field::LastPrice => "last_price",
        Field::Average => "average",
        Field::Highest => "highest",
        Field::Lowest => "lowest",
        Field::AskPrice1 => "ask_price1",
        Field::AskVolume1 => "ask_volume1",
        Field::BidPrice1 => "bid_price1",
        Field::BidVolume1 => "bid_volume1",
        Field::AskPrice2 => "ask_price2",
        Field::AskVolume2 => "ask_volume2",
        Field::BidPrice2 => "bid_price2",
        Field::BidVolume2 => "bid_volume2",
        Field::AskPrice3 => "ask_price3",
        Field::AskVolume3 => "ask_volume3",
        Field::BidPrice3 => "bid_price3",
        Field::BidVolume3 => "bid_volume3",
        Field::AskPrice4 => "ask_price4",
        Field::AskVolume4 => "ask_volume4",
        Field::BidPrice4 => "bid_price4",
        Field::BidVolume4 => "bid_volume4",
        Field::AskPrice5 => "ask_price5",
        Field::AskVolume5 => "ask_volume5",
        Field::BidPrice5 => "bid_price5",
        Field::BidVolume5 => "bid_volume5",
        Field::Amount => "amount",
        Field::OpenInterest => "open_interest",
    }
}

fn llm_header(block: &QueryBlock, codec: LlmTimeCodec) -> String {
    let mut cells = Vec::with_capacity(block.spec.fields.len() + 1);
    for field in &block.spec.fields {
        cells.push(field.code().to_string());
        if matches!(field, Field::Time) && matches!(codec.mode, LlmTimeMode::Both) {
            cells.push("dt".to_string());
        }
    }
    csv_line(cells)
}

fn rows_as_llm_csv(
    block: &QueryBlock,
    settings: &QuerySettings,
    codec: LlmTimeCodec,
) -> Result<Vec<String>, CliError> {
    match &block.rows {
        BacktestHistoryRows::Ticks(rows) => rows
            .iter()
            .map(|row| llm_csv_row(block, settings, codec, |field| tick_cell(row, field)))
            .collect(),
        BacktestHistoryRows::Klines { rows, .. } => rows
            .iter()
            .map(|row| llm_csv_row(block, settings, codec, |field| kline_cell(row, field)))
            .collect(),
    }
}

fn llm_csv_row(
    block: &QueryBlock,
    settings: &QuerySettings,
    codec: LlmTimeCodec,
    cell: impl Fn(Field) -> CellValue,
) -> Result<String, CliError> {
    let mut cells = Vec::with_capacity(block.spec.fields.len() + 1);
    for field in &block.spec.fields {
        match cell(*field) {
            CellValue::Timestamp(value) => match codec.mode {
                LlmTimeMode::Iso => cells.push(codec.format_timestamp(value)?),
                LlmTimeMode::Offset => cells.push(codec.offset(value)),
                LlmTimeMode::Both => {
                    cells.push(codec.format_timestamp(value)?);
                    cells.push(codec.offset(value));
                }
            },
            value => cells.push(format_cell(value, &block.spec, settings)?),
        }
    }
    Ok(csv_line(cells))
}

fn llm_timestamps(block: &QueryBlock) -> Vec<i64> {
    let mut timestamps = vec![block.spec.start_ns, block.spec.end_ns];
    timestamps.extend(
        block
            .request
            .physical_segments
            .iter()
            .flat_map(|segment| [segment.start_ns, segment.end_ns]),
    );
    match &block.rows {
        BacktestHistoryRows::Ticks(rows) => {
            timestamps.extend(rows.iter().map(|row| row.datetime));
        }
        BacktestHistoryRows::Klines { rows, .. } => {
            timestamps.extend(rows.iter().map(|row| row.datetime));
        }
    }
    timestamps
}

fn llm_offset_unit_ns(block: &QueryBlock, timestamps: &[i64]) -> i64 {
    if let Some(period_ns) = block
        .spec
        .duration_ns
        .filter(|period_ns| timestamps_align_to_unit(block.spec.start_ns, timestamps, *period_ns))
    {
        return period_ns;
    }
    const CANDIDATES: [i64; 7] = [
        24 * 60 * 60 * 1_000_000_000,
        60 * 60 * 1_000_000_000,
        60 * 1_000_000_000,
        1_000_000_000,
        1_000_000,
        1_000,
        1,
    ];
    CANDIDATES
        .into_iter()
        .find(|unit_ns| timestamps_align_to_unit(block.spec.start_ns, timestamps, *unit_ns))
        .unwrap_or(1)
}

fn timestamps_align_to_unit(reference_ns: i64, timestamps: &[i64], unit_ns: i64) -> bool {
    unit_ns > 0
        && timestamps.iter().all(|timestamp| {
            (i128::from(*timestamp) - i128::from(reference_ns)) % i128::from(unit_ns) == 0
        })
}

fn format_duration_label(duration: Duration) -> String {
    format_duration_label_nanos(duration.as_nanos())
}

fn format_duration_label_from_ns(value_ns: i64) -> String {
    if value_ns <= 0 {
        return format!("{value_ns}ns");
    }
    format_duration_label_nanos(value_ns as u128)
}

fn format_duration_label_nanos(value_ns: u128) -> String {
    const UNITS: [(&str, u128); 7] = [
        ("d", 24 * 60 * 60 * 1_000_000_000),
        ("h", 60 * 60 * 1_000_000_000),
        ("m", 60 * 1_000_000_000),
        ("s", 1_000_000_000),
        ("ms", 1_000_000),
        ("us", 1_000),
        ("ns", 1),
    ];
    for (suffix, unit_ns) in UNITS {
        if value_ns % unit_ns == 0 {
            return format!("{}{suffix}", value_ns / unit_ns);
        }
    }
    format!("{value_ns}ns")
}

fn rows_as_csv(block: &QueryBlock, settings: &QuerySettings) -> Result<Vec<String>, CliError> {
    match &block.rows {
        BacktestHistoryRows::Ticks(rows) => rows
            .iter()
            .map(|row| {
                block
                    .spec
                    .fields
                    .iter()
                    .map(|field| format_cell(tick_cell(row, *field), &block.spec, settings))
                    .collect::<Result<Vec<_>, _>>()
                    .map(csv_line)
            })
            .collect(),
        BacktestHistoryRows::Klines { rows, .. } => rows
            .iter()
            .map(|row| {
                block
                    .spec
                    .fields
                    .iter()
                    .map(|field| format_cell(kline_cell(row, *field), &block.spec, settings))
                    .collect::<Result<Vec<_>, _>>()
                    .map(csv_line)
            })
            .collect(),
    }
}

fn rows_as_json(block: &QueryBlock, settings: &QuerySettings) -> Result<Vec<Value>, CliError> {
    match &block.rows {
        BacktestHistoryRows::Ticks(rows) => rows
            .iter()
            .map(|row| row_json(block, settings, |field| tick_cell(row, field)))
            .collect(),
        BacktestHistoryRows::Klines { rows, .. } => rows
            .iter()
            .map(|row| row_json(block, settings, |field| kline_cell(row, field)))
            .collect(),
    }
}

fn row_json(
    block: &QueryBlock,
    settings: &QuerySettings,
    cell: impl Fn(Field) -> CellValue,
) -> Result<Value, CliError> {
    let mut values = Map::new();
    for field in &block.spec.fields {
        values.insert(
            field.code().to_string(),
            json_cell(cell(*field), &block.spec, settings)?,
        );
    }
    Ok(Value::Object(values))
}

fn tick_cell(row: &Tick, field: Field) -> CellValue {
    match field {
        Field::Time => CellValue::Timestamp(row.datetime),
        Field::Id => CellValue::Integer(row.id),
        Field::LastPrice => float_cell(row.last_price, true),
        Field::Average => float_cell(row.average, true),
        Field::Highest => float_cell(row.highest, true),
        Field::Lowest => float_cell(row.lowest, true),
        Field::AskPrice1 => float_cell(row.ask_price1, true),
        Field::AskVolume1 => CellValue::Integer(row.ask_volume1),
        Field::BidPrice1 => float_cell(row.bid_price1, true),
        Field::BidVolume1 => CellValue::Integer(row.bid_volume1),
        Field::AskPrice2 => float_cell(row.ask_price2, true),
        Field::AskVolume2 => CellValue::Integer(row.ask_volume2),
        Field::BidPrice2 => float_cell(row.bid_price2, true),
        Field::BidVolume2 => CellValue::Integer(row.bid_volume2),
        Field::AskPrice3 => float_cell(row.ask_price3, true),
        Field::AskVolume3 => CellValue::Integer(row.ask_volume3),
        Field::BidPrice3 => float_cell(row.bid_price3, true),
        Field::BidVolume3 => CellValue::Integer(row.bid_volume3),
        Field::AskPrice4 => float_cell(row.ask_price4, true),
        Field::AskVolume4 => CellValue::Integer(row.ask_volume4),
        Field::BidPrice4 => float_cell(row.bid_price4, true),
        Field::BidVolume4 => CellValue::Integer(row.bid_volume4),
        Field::AskPrice5 => float_cell(row.ask_price5, true),
        Field::AskVolume5 => CellValue::Integer(row.ask_volume5),
        Field::BidPrice5 => float_cell(row.bid_price5, true),
        Field::BidVolume5 => CellValue::Integer(row.bid_volume5),
        Field::Volume => CellValue::Integer(row.volume),
        Field::Amount => float_cell(row.amount, false),
        Field::OpenInterest => CellValue::Integer(row.open_interest),
        _ => unreachable!("Tick field selection is validated against the Tick schema"),
    }
}

fn kline_cell(row: &Kline, field: Field) -> CellValue {
    match field {
        Field::Time => CellValue::Timestamp(row.datetime),
        Field::Id => CellValue::Integer(row.id),
        Field::Open => float_cell(row.open, true),
        Field::High => float_cell(row.high, true),
        Field::Low => float_cell(row.low, true),
        Field::Close => float_cell(row.close, true),
        Field::Volume => CellValue::Integer(row.volume),
        Field::OpenOi => CellValue::Integer(row.open_oi),
        Field::CloseOi => CellValue::Integer(row.close_oi),
        _ => unreachable!("Kline field selection is validated against the Kline schema"),
    }
}

const fn float_cell(value: f64, price: bool) -> CellValue {
    CellValue::Float { value, price }
}

fn format_cell(
    cell: CellValue,
    spec: &QuerySpec,
    settings: &QuerySettings,
) -> Result<String, CliError> {
    match cell {
        CellValue::Timestamp(value) => match settings.timestamp {
            TimestampMode::Full => format_timestamp(value),
            TimestampMode::Offset => Ok(value.saturating_sub(spec.start_ns).to_string()),
        },
        CellValue::Integer(value) => Ok(value.to_string()),
        CellValue::Float { value, price } => {
            if !value.is_finite() {
                return Ok(String::new());
            }
            if price && matches!(settings.number_format, NumberFormat::ScaledInt) {
                return scaled_price(value, spec.price_tick, spec.symbol.as_str())
                    .map(|value| value.to_string());
            }
            Ok(format_decimal(value))
        }
    }
}

fn json_cell(
    cell: CellValue,
    spec: &QuerySpec,
    settings: &QuerySettings,
) -> Result<Value, CliError> {
    match cell {
        CellValue::Timestamp(value) => match settings.timestamp {
            TimestampMode::Full => Ok(Value::String(format_timestamp(value)?)),
            TimestampMode::Offset => Ok(Value::from(value.saturating_sub(spec.start_ns))),
        },
        CellValue::Integer(value) => Ok(Value::from(value)),
        CellValue::Float { value, price: _ } if !value.is_finite() => Ok(Value::Null),
        CellValue::Float { value, price: true }
            if matches!(settings.number_format, NumberFormat::ScaledInt) =>
        {
            Ok(Value::from(scaled_price(
                value,
                spec.price_tick,
                spec.symbol.as_str(),
            )?))
        }
        CellValue::Float { value, .. } => serde_json::Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| {
                CliError::Data(DataError::InvalidResponse(
                    "non-finite JSON value".to_string(),
                ))
            }),
    }
}

fn scaled_price(value: f64, price_tick: Option<f64>, symbol: &str) -> Result<i64, CliError> {
    let price_tick = price_tick.ok_or_else(|| {
        CliError::Usage(format!(
            "--number-format scaled-int requires --price-tick for {symbol}"
        ))
    })?;
    let scaled = value / price_tick;
    let rounded = scaled.round();
    if !rounded.is_finite() || rounded < i64::MIN as f64 || rounded > i64::MAX as f64 {
        return Err(CliError::Usage(format!(
            "price {value} for {symbol} cannot be represented as a scaled integer"
        )));
    }
    let tolerance = 1e-8_f64.max(scaled.abs() * 1e-12);
    if (scaled - rounded).abs() > tolerance {
        return Err(CliError::Usage(format!(
            "price {value} for {symbol} is not an integer multiple of price tick {price_tick}"
        )));
    }
    Ok(rounded as i64)
}

fn summary_line(block: &QueryBlock, settings: &QuerySettings) -> Result<Option<String>, CliError> {
    if block.rows.is_empty() {
        return Ok(None);
    }
    let mut cells = vec!["summary".to_string()];
    match &block.rows {
        BacktestHistoryRows::Ticks(rows) => {
            append_tick_summary(&mut cells, rows.as_slice(), &block.spec, settings)?;
        }
        BacktestHistoryRows::Klines { rows, .. } => {
            append_kline_summary(&mut cells, rows.as_slice(), &block.spec, settings)?;
        }
    }
    Ok(Some(csv_line(cells)))
}

fn append_tick_summary(
    cells: &mut Vec<String>,
    rows: &[Tick],
    spec: &QuerySpec,
    settings: &QuerySettings,
) -> Result<(), CliError> {
    let Some(first) = rows.first() else {
        return Ok(());
    };
    let last = rows.last().expect("first row proves a last row");
    push_summary(
        cells,
        "first_lp",
        format_price(first.last_price, spec, settings)?,
    );
    push_summary(
        cells,
        "last_lp",
        format_price(last.last_price, spec, settings)?,
    );
    push_summary(
        cells,
        "high_lp",
        optional_price(
            finite_max(rows.iter().map(|row| row.last_price)),
            spec,
            settings,
        )?,
    );
    push_summary(
        cells,
        "low_lp",
        optional_price(
            finite_min(rows.iter().map(|row| row.last_price)),
            spec,
            settings,
        )?,
    );
    push_summary(
        cells,
        "volume_delta",
        last.volume.saturating_sub(first.volume).to_string(),
    );
    push_summary(cells, "oi_last", last.open_interest.to_string());
    Ok(())
}

fn append_kline_summary(
    cells: &mut Vec<String>,
    rows: &[Kline],
    spec: &QuerySpec,
    settings: &QuerySettings,
) -> Result<(), CliError> {
    let Some(first) = rows.first() else {
        return Ok(());
    };
    let last = rows.last().expect("first row proves a last row");
    push_summary(cells, "open", format_price(first.open, spec, settings)?);
    push_summary(cells, "close", format_price(last.close, spec, settings)?);
    push_summary(
        cells,
        "high",
        optional_price(finite_max(rows.iter().map(|row| row.high)), spec, settings)?,
    );
    push_summary(
        cells,
        "low",
        optional_price(finite_min(rows.iter().map(|row| row.low)), spec, settings)?,
    );
    push_summary(
        cells,
        "volume_sum",
        rows.iter()
            .fold(0_i64, |sum, row| sum.saturating_add(row.volume))
            .to_string(),
    );
    push_summary(cells, "oi_last", last.close_oi.to_string());
    Ok(())
}

fn push_summary(cells: &mut Vec<String>, key: &str, value: String) {
    cells.push(key.to_string());
    cells.push(value);
}

fn format_price(
    value: f64,
    spec: &QuerySpec,
    settings: &QuerySettings,
) -> Result<String, CliError> {
    format_cell(float_cell(value, true), spec, settings)
}

fn optional_price(
    value: Option<f64>,
    spec: &QuerySpec,
    settings: &QuerySettings,
) -> Result<String, CliError> {
    value.map_or_else(
        || Ok(String::new()),
        |value| format_price(value, spec, settings),
    )
}

fn finite_max(values: impl Iterator<Item = f64>) -> Option<f64> {
    values.filter(|value| value.is_finite()).reduce(f64::max)
}

fn finite_min(values: impl Iterator<Item = f64>) -> Option<f64> {
    values.filter(|value| value.is_finite()).reduce(f64::min)
}

fn important_indices(block: &QueryBlock, focus: FocusMode) -> Vec<usize> {
    match (&block.rows, focus) {
        (_, FocusMode::Balanced) => Vec::new(),
        (BacktestHistoryRows::Klines { rows, .. }, FocusMode::Price) => vec![
            index_of_max(rows, |row| row.high),
            index_of_min(rows, |row| row.low),
        ],
        (BacktestHistoryRows::Klines { rows, .. }, FocusMode::VolumeOi) => vec![
            index_of_max(rows, |row| row.volume as f64),
            index_of_max(rows, |row| {
                row.close_oi.saturating_sub(row.open_oi).unsigned_abs() as f64
            }),
        ],
        (BacktestHistoryRows::Klines { rows, .. }, FocusMode::Microstructure) => vec![
            index_of_max(rows, |row| (row.high - row.low).abs()),
            index_of_max(rows, |row| row.volume as f64),
        ],
        (BacktestHistoryRows::Ticks(rows), FocusMode::Price) => vec![
            index_of_max(rows, |row| row.last_price),
            index_of_min(rows, |row| row.last_price),
        ],
        (BacktestHistoryRows::Ticks(rows), FocusMode::VolumeOi) => vec![
            index_of_max(rows, |row| row.volume as f64),
            index_of_max(rows, |row| row.open_interest as f64),
        ],
        (BacktestHistoryRows::Ticks(rows), FocusMode::Microstructure) => vec![
            index_of_max(rows, |row| (row.ask_price1 - row.bid_price1).abs()),
            index_of_max(rows, |row| {
                row.ask_volume1.saturating_add(row.bid_volume1) as f64
            }),
        ],
    }
    .into_iter()
    .flatten()
    .collect()
}

fn index_of_max<T>(rows: &[T], value: impl Fn(&T) -> f64) -> Option<usize> {
    rows.iter()
        .enumerate()
        .filter_map(|(index, row)| {
            let value = value(row);
            value.is_finite().then_some((index, value))
        })
        .max_by(|left, right| left.1.total_cmp(&right.1))
        .map(|(index, _)| index)
}

fn index_of_min<T>(rows: &[T], value: impl Fn(&T) -> f64) -> Option<usize> {
    rows.iter()
        .enumerate()
        .filter_map(|(index, row)| {
            let value = value(row);
            value.is_finite().then_some((index, value))
        })
        .min_by(|left, right| left.1.total_cmp(&right.1))
        .map(|(index, _)| index)
}

fn data_hash(
    spec: &QuerySpec,
    rows: &BacktestHistoryRows,
    settings: &QuerySettings,
) -> Result<String, CliError> {
    let block = QueryBlock {
        block_id: String::new(),
        spec: spec.clone(),
        request: placeholder_report(spec),
        rows: rows.clone(),
        metadata: MetadataStatus::Missing {
            reason: String::new(),
        },
        data_hash: String::new(),
    };
    let mut content = spec
        .fields
        .iter()
        .map(|field| field.code())
        .collect::<Vec<_>>()
        .join(",");
    for row in rows_as_csv(&block, settings)? {
        content.push('\n');
        content.push_str(&row);
    }
    Ok(hash_hex(content.as_bytes()))
}

fn placeholder_report(spec: &QuerySpec) -> BacktestHistoryRequestReport {
    BacktestHistoryRequestReport {
        request_id: spec.request_id,
        symbol: spec.symbol.clone(),
        kind: if matches!(spec.series, QuerySeries::Tick) {
            tqsdk_data::BacktestHistoryKind::Tick
        } else {
            tqsdk_data::BacktestHistoryKind::Kline {
                duration: spec.duration.expect("validated Kline query has a duration"),
            }
        },
        rows: 0,
        physical_segments: Vec::new(),
        snapshot_hash: String::new(),
        coverage: tqsdk_data::BacktestHistoryCoverageReport {
            requested_range: (spec.start_ns, spec.end_ns),
            expanded_source_range: (spec.start_ns, spec.end_ns),
            cached_ranges: Vec::new(),
            remote_filled_ranges: Vec::new(),
            finality: BacktestHistoryFinality::Final,
        },
        remote_used: false,
    }
}

fn query_hash(specs: &[QuerySpec], settings: &QuerySettings) -> String {
    let mut content = format!(
        "{QUERY_SCHEMA_VERSION}|{}|{}|{}|{}|{}|{}",
        settings.policy.as_str(),
        settings.timestamp.as_str(),
        settings.number_format.as_str(),
        settings.focus.as_str(),
        settings.compression.as_str(),
        settings.data_token_budget.unwrap_or_default()
    );
    for spec in specs {
        content.push_str(&format!(
            "|{}|{}|{}|{}|{}|{}|{}|{}|{}",
            spec.request_id,
            spec.symbol,
            spec.series.as_str(),
            spec.duration_ns.unwrap_or_default(),
            spec.start_ns,
            spec.end_ns,
            spec.fields
                .iter()
                .map(|field| field.code())
                .collect::<Vec<_>>()
                .join(","),
            spec.weight,
            spec.price_tick.map_or_else(String::new, format_decimal),
        ));
    }
    hash_hex(content.as_bytes())
}

fn drill_down_id(artifact: &QueryArtifact, block: &QueryBlock) -> String {
    format!(
        "{}:{}:{}",
        artifact.query_id,
        block.block_id,
        &block.data_hash[..12]
    )
}

fn metadata_value(metadata: &MetadataStatus) -> Result<Value, CliError> {
    match metadata {
        MetadataStatus::Verified(snapshot) => Ok(json!({
            "status": "verified",
            "snapshot_hash": snapshot.snapshot_hash,
            "captured_at": timestamp_value(snapshot.captured_at_ns)?,
            "session": serde_json::to_value(&snapshot.session)?,
        })),
        MetadataStatus::Missing { reason } => Ok(json!({
            "status": "missing",
            "reason": reason,
        })),
    }
}

fn coverage_value(report: &BacktestHistoryRequestReport) -> Value {
    json!({
        "requested": range_value(report.coverage.requested_range.0, report.coverage.requested_range.1),
        "expanded_source": range_value(report.coverage.expanded_source_range.0, report.coverage.expanded_source_range.1),
        "cached_ranges": ranges_value(report.coverage.cached_ranges.as_slice()),
        "remote_filled_ranges": ranges_value(report.coverage.remote_filled_ranges.as_slice()),
    })
}

fn ranges_value(ranges: &[(i64, i64)]) -> Vec<Value> {
    ranges
        .iter()
        .map(|(start_ns, end_ns)| range_value(*start_ns, *end_ns))
        .collect()
}

fn range_value(start_ns: i64, end_ns: i64) -> Value {
    json!({
        "start_ns": start_ns,
        "end_ns": end_ns,
        "start": timestamp_value(start_ns).unwrap_or(Value::Null),
        "end": timestamp_value(end_ns).unwrap_or(Value::Null),
    })
}

fn segments_value(segments: &[tqsdk_data::BacktestHistoryPhysicalSegment]) -> Vec<Value> {
    segments
        .iter()
        .map(|segment| {
            json!({
                "physical_symbol": segment.physical_symbol,
                "start_ns": segment.start_ns,
                "end_ns": segment.end_ns,
                "start": timestamp_value(segment.start_ns).unwrap_or(Value::Null),
                "end": timestamp_value(segment.end_ns).unwrap_or(Value::Null),
            })
        })
        .collect()
}

fn finality_value(finality: BacktestHistoryFinality) -> Value {
    match finality {
        BacktestHistoryFinality::Final => json!({ "kind": "final" }),
        BacktestHistoryFinality::Provisional { as_of_ns } => json!({
            "kind": "provisional",
            "as_of_ns": as_of_ns,
            "as_of": timestamp_value(as_of_ns).unwrap_or(Value::Null),
        }),
    }
}

fn parse_series_duration(
    series: QuerySeries,
    period: Option<&str>,
) -> Result<(Option<Duration>, Option<i64>), CliError> {
    match series {
        QuerySeries::Tick if period.is_some() => Err(CliError::Usage(
            "--period is valid only for --series kline".to_string(),
        )),
        QuerySeries::Tick => Ok((None, None)),
        QuerySeries::Kline => {
            let period = period.ok_or_else(|| {
                CliError::Usage("--series kline requires --period (for example 1m)".to_string())
            })?;
            let duration = parse_duration(period)?;
            let duration_ns = i64::try_from(duration.as_nanos()).map_err(|_| {
                CliError::Usage(format!("Kline period {period:?} exceeds i64 nanoseconds"))
            })?;
            Ok((Some(duration), Some(duration_ns)))
        }
    }
}

fn parse_duration(value: &str) -> Result<Duration, CliError> {
    let value = value.trim();
    let units = [
        ("ns", 1_u64),
        ("us", 1_000_u64),
        ("ms", 1_000_000_u64),
        ("s", 1_000_000_000_u64),
        ("m", 60 * 1_000_000_000_u64),
        ("h", 60 * 60 * 1_000_000_000_u64),
        ("d", 24 * 60 * 60 * 1_000_000_000_u64),
    ];
    let Some((number, multiplier)) = units.iter().find_map(|(suffix, multiplier)| {
        value
            .strip_suffix(suffix)
            .filter(|number| !number.is_empty())
            .map(|number| (number, *multiplier))
    }) else {
        return Err(CliError::Usage(format!(
            "invalid Kline period {value:?}; use an integer with ns, us, ms, s, m, h, or d"
        )));
    };
    let number = number.parse::<u64>().map_err(|_| {
        CliError::Usage(format!(
            "invalid Kline period {value:?}; duration magnitude must be a positive integer"
        ))
    })?;
    let nanos = number
        .checked_mul(multiplier)
        .ok_or_else(|| CliError::Usage(format!("Kline period {value:?} is too large")))?;
    if nanos == 0 || nanos > i64::MAX as u64 {
        return Err(CliError::Usage(format!(
            "Kline period {value:?} must be between 1ns and i64::MAX nanoseconds"
        )));
    }
    Ok(Duration::from_nanos(nanos))
}

fn parse_rfc3339_ns(value: &str, flag: &str) -> Result<i64, CliError> {
    DateTime::parse_from_rfc3339(value)
        .map_err(|error| CliError::Usage(format!("{flag} must be RFC 3339: {error}")))?
        .timestamp_nanos_opt()
        .ok_or_else(|| CliError::Usage(format!("{flag} is outside the i64 nanosecond range")))
}

fn validate_range(start_ns: i64, end_ns: i64) -> Result<(), CliError> {
    if start_ns >= end_ns {
        return Err(CliError::Usage(
            "query range must satisfy start < end".to_string(),
        ));
    }
    Ok(())
}

fn normalized_symbols(values: &[String]) -> Result<Vec<String>, CliError> {
    if values.is_empty() {
        return Err(CliError::Usage(
            "query requires at least one --symbol or --request-file".to_string(),
        ));
    }
    let mut seen = BTreeSet::new();
    values
        .iter()
        .map(|value| normalized_one_symbol(value))
        .filter(|symbol| match symbol {
            Ok(symbol) => seen.insert(symbol.clone()),
            Err(_) => true,
        })
        .collect()
}

fn normalized_one_symbol(value: &str) -> Result<String, CliError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(CliError::Usage(
            "query symbol must not be empty".to_string(),
        ));
    }
    if value.chars().any(|character| character.is_control()) {
        return Err(CliError::Usage(
            "query symbol must not contain control characters".to_string(),
        ));
    }
    Ok(value.to_string())
}

fn resolve_fields(series: QuerySeries, requested: &[String]) -> Result<Vec<Field>, CliError> {
    if requested.is_empty() {
        return Ok(default_fields(series));
    }
    let mut selected = BTreeSet::new();
    for value in requested {
        for alias in value.split(',') {
            let alias = alias.trim().to_ascii_lowercase();
            let field = schema_fields(series)
                .iter()
                .copied()
                .find(|field| field.aliases().contains(&alias.as_str()))
                .ok_or_else(|| {
                    CliError::Usage(format!(
                        "field {alias:?} is not valid for {} rows; use `query schema --series {}`",
                        series.as_str(),
                        series.as_str()
                    ))
                })?;
            if !selected.insert(field) {
                return Err(CliError::Usage(format!(
                    "field {alias:?} selects duplicate canonical field {}",
                    field.code()
                )));
            }
        }
    }
    Ok(schema_fields(series)
        .iter()
        .copied()
        .filter(|field| selected.contains(field))
        .collect())
}

fn default_fields(series: QuerySeries) -> Vec<Field> {
    match series {
        QuerySeries::Tick => vec![
            Field::Time,
            Field::LastPrice,
            Field::AskPrice1,
            Field::AskVolume1,
            Field::BidPrice1,
            Field::BidVolume1,
            Field::Volume,
            Field::OpenInterest,
        ],
        QuerySeries::Kline => vec![
            Field::Time,
            Field::Open,
            Field::High,
            Field::Low,
            Field::Close,
            Field::Volume,
            Field::CloseOi,
        ],
    }
}

fn schema_fields(series: QuerySeries) -> &'static [Field] {
    match series {
        QuerySeries::Tick => &TICK_FIELDS,
        QuerySeries::Kline => &KLINE_FIELDS,
    }
}

fn ranges_cover(requested: (i64, i64), ranges: &[(i64, i64)]) -> bool {
    let mut ranges = ranges
        .iter()
        .copied()
        .filter(|(start, end)| start < end && *end > requested.0 && *start < requested.1)
        .collect::<Vec<_>>();
    ranges.sort_unstable();
    let mut covered_through = requested.0;
    for (start, end) in ranges {
        if start > covered_through {
            return false;
        }
        covered_through = covered_through.max(end);
        if covered_through >= requested.1 {
            return true;
        }
    }
    false
}

fn timestamp_value(value: i64) -> Result<Value, CliError> {
    Ok(Value::String(format_timestamp(value)?))
}

fn format_timestamp(value: i64) -> Result<String, CliError> {
    let seconds = value.div_euclid(1_000_000_000);
    let nanos = u32::try_from(value.rem_euclid(1_000_000_000))
        .map_err(|_| CliError::Usage("timestamp nanosecond remainder is invalid".to_string()))?;
    DateTime::<Utc>::from_timestamp(seconds, nanos)
        .map(|value| value.to_rfc3339_opts(SecondsFormat::Nanos, true))
        .ok_or_else(|| CliError::Usage(format!("timestamp {value} is outside RFC 3339 range")))
}

fn format_decimal(value: f64) -> String {
    if !value.is_finite() {
        return String::new();
    }
    if value == 0.0 {
        return "0".to_string();
    }
    let rendered = value.to_string();
    expand_scientific_decimal(rendered.as_str()).unwrap_or(rendered)
}

fn expand_scientific_decimal(value: &str) -> Option<String> {
    let exponent_index = value.find(['e', 'E'])?;
    let mantissa = &value[..exponent_index];
    let exponent = value[exponent_index + 1..].parse::<i32>().ok()?;
    let (sign, mantissa) = match mantissa.strip_prefix('-') {
        Some(value) => ("-", value),
        None => ("", mantissa.strip_prefix('+').unwrap_or(mantissa)),
    };
    let decimal_index = mantissa.find('.').unwrap_or(mantissa.len());
    let digits = mantissa.replace('.', "");
    if digits.is_empty() || !digits.chars().all(|character| character.is_ascii_digit()) {
        return None;
    }
    let decimal_position = i32::try_from(decimal_index).ok()?.checked_add(exponent)?;
    let rendered = if decimal_position <= 0 {
        format!(
            "{sign}0.{}{}",
            "0".repeat(usize::try_from(-decimal_position).ok()?),
            digits
        )
    } else if usize::try_from(decimal_position).ok()? >= digits.len() {
        format!(
            "{sign}{digits}{}",
            "0".repeat(
                usize::try_from(decimal_position)
                    .ok()?
                    .saturating_sub(digits.len())
            )
        )
    } else {
        let position = usize::try_from(decimal_position).ok()?;
        format!("{sign}{}.{}", &digits[..position], &digits[position..])
    };
    (rendered.len() <= 32).then_some(rendered)
}

fn estimate_tokens(value: &str) -> usize {
    value
        .len()
        .div_ceil(3)
        .saturating_add(value.lines().count())
}

fn hash_hex(value: &[u8]) -> String {
    let mut hasher = Sha1::new();
    hasher.update(value);
    format!("{:x}", hasher.finalize())
}

fn csv_line<I, S>(cells: I) -> String
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    cells
        .into_iter()
        .map(|cell| csv_escape(cell.as_ref()))
        .collect::<Vec<_>>()
        .join(",")
}

fn csv_escape(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn protocol_text(value: &str) -> String {
    value
        .chars()
        .map(|character| match character {
            '\r' | '\n' | '\0' => ' ',
            character if character.is_control() => ' ',
            character => character,
        })
        .collect::<String>()
}

#[cfg(test)]
mod tests {
    use super::{
        CompressionMode, FocusMode, QuerySeries, TimestampMode, csv_line, estimate_tokens,
        parse_duration, resolve_fields,
    };

    #[test]
    fn duration_parser_supports_human_periods() {
        assert_eq!(parse_duration("15s").unwrap().as_secs(), 15);
        assert_eq!(parse_duration("5m").unwrap().as_secs(), 300);
        assert!(parse_duration("15").is_err());
        assert!(parse_duration("0s").is_err());
    }

    #[test]
    fn fields_are_canonicalized_and_duplicates_are_rejected() {
        let fields = resolve_fields(QuerySeries::Kline, &["close,time,open".to_string()]).unwrap();
        assert_eq!(
            fields.iter().map(|field| field.code()).collect::<Vec<_>>(),
            vec!["t", "o", "c"]
        );
        assert!(resolve_fields(QuerySeries::Kline, &["time,timestamp".to_string()]).is_err());
    }

    #[test]
    fn csv_escapes_protocol_values_without_changing_field_rows() {
        assert_eq!(
            csv_line(["segment", "SHFE.au,2601"]),
            "segment,\"SHFE.au,2601\""
        );
    }

    #[test]
    fn token_estimate_is_conservative_for_short_ascii_context() {
        assert!(estimate_tokens("t,o,c\n1,2,3\n") >= 4);
        assert_eq!(TimestampMode::Full.as_str(), "iso8601");
        assert_eq!(CompressionMode::Auto.as_str(), "auto");
        assert_eq!(FocusMode::Balanced.as_str(), "balanced");
    }
}

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use chrono::{DateTime, FixedOffset, SecondsFormat, Utc};
use serde_json::{Map, Value, json};
use sha1::{Digest, Sha1};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use tokio::time::{Instant, timeout, timeout_at};
use tqsdk_data::{
    BacktestHistoryFailureReason, BacktestHistoryFinality, BacktestHistoryRequest,
    BacktestHistorySchemaSeries, BacktestHistorySnapshotError, BacktestHistorySnapshotEvent,
    BacktestHistorySnapshotQueryResources, BacktestHistorySnapshotResourceBudget,
    BacktestHistorySnapshotResourceReservation, BacktestHistoryValueKind,
    backtest_history_default_fields, backtest_history_resolve_fields,
    backtest_history_schema_fields,
};
use tqsdk_relay::{RelayError, RelayResult};

use super::codec::{HistoryColumn, HistoryRowCodec};
use super::snapshot::{PinnedSnapshot, SnapshotSlot};

const MAX_REQUEST_BYTES: usize = 16 * 1024;
const MAX_IDENTITY_BYTES: usize = 512;
const MAX_KLINE_ROWS: usize = 10_000;
const MAX_TICK_ROWS: usize = 50_000;
const MAX_RESPONSE_BYTES: usize = 32 * 1024 * 1024;
const JSON_CELL_ALLOCATION_BYTES: usize = 128;
const GLOBAL_BUFFER_BYTES: usize = 512 * 1024 * 1024;
const MAX_ACTIVE_REQUESTS: usize = 8;
const READ_TIMEOUT: Duration = Duration::from_secs(2);
const QUEUE_TIMEOUT: Duration = Duration::from_millis(100);
const TOTAL_TIMEOUT: Duration = Duration::from_secs(10);
const WRITE_GRACE: Duration = Duration::from_secs(1);
const SCHEMA_PATH: &str = "/v1/history/schema";
const QUERY_PATH: &str = "/v1/history/query";
const COVERAGE_PATH: &str = "/v1/history/coverage";
const SECOND_NS: u64 = 1_000_000_000;
const MINUTE_NS: u64 = 60 * SECOND_NS;
const DAY_NS: u64 = 24 * 60 * MINUTE_NS;
static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

struct HistoryState {
    snapshots: Arc<SnapshotSlot>,
    active: Arc<Semaphore>,
    buffers: Arc<ByteBudget>,
    scan_budget: Arc<SnapshotScanBudget>,
}

impl HistoryState {
    fn new(snapshots: Arc<SnapshotSlot>) -> Self {
        let buffers = Arc::new(ByteBudget::new(GLOBAL_BUFFER_BYTES));
        Self {
            snapshots,
            active: Arc::new(Semaphore::new(MAX_ACTIVE_REQUESTS)),
            scan_budget: Arc::new(SnapshotScanBudget {
                buffers: buffers.clone(),
            }),
            buffers,
        }
    }
}

pub(super) async fn serve_until(
    listener: TcpListener,
    identity_header: String,
    snapshots: Arc<SnapshotSlot>,
    mut shutdown: oneshot::Receiver<()>,
) -> RelayResult<()> {
    let state = Arc::new(HistoryState::new(snapshots.clone()));
    let (reload_shutdown, reload_receiver) = oneshot::channel();
    let reload_task = tokio::spawn(snapshots.reload_loop(reload_receiver));
    let result = loop {
        tokio::select! {
            _ = &mut shutdown => break Ok(()),
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(value) => value,
                    Err(error) => break Err(RelayError::Transport(format!("history accept failed: {error}"))),
                };
                let state = state.clone();
                let identity_header = identity_header.clone();
                tokio::spawn(async move {
                    if let Err(error) = serve_stream(stream, &identity_header, state).await {
                        eprintln!("history HTTP connection failed: {error}");
                    }
                });
            }
        }
    };
    let _ = reload_shutdown.send(());
    let _ = reload_task.await;
    result
}

async fn serve_stream(
    mut stream: TcpStream,
    identity_header: &str,
    state: Arc<HistoryState>,
) -> RelayResult<()> {
    let request = match timeout(READ_TIMEOUT, read_request(&mut stream)).await {
        Ok(Ok(value)) => value,
        Ok(Err(error)) => {
            return write_response(
                &mut stream,
                error_response(
                    400,
                    "invalid_request",
                    &error.to_string(),
                    next_id().1,
                    json!({}),
                ),
                None,
                &state,
            )
            .await;
        }
        Err(_) => {
            return write_response(
                &mut stream,
                error_response(
                    400,
                    "invalid_request",
                    "request read timed out",
                    next_id().1,
                    json!({}),
                ),
                None,
                &state,
            )
            .await;
        }
    };
    let if_none_match = parse_request(&request).ok().and_then(|parsed| {
        header_values(&parsed.headers, "if-none-match")
            .first()
            .map(|value| (*value).to_owned())
    });
    let (data_id, request_id) = next_id();
    let deadline = Instant::now() + TOTAL_TIMEOUT;
    let route_deadline = deadline - WRITE_GRACE;
    let routed = timeout_at(
        route_deadline,
        route_request(
            &request,
            identity_header,
            state.clone(),
            data_id,
            request_id.clone(),
        ),
    );
    tokio::pin!(routed);
    let response = tokio::select! {
        result = &mut routed => match result {
            Ok(response) => response,
            Err(_) => error_response(504, "history_timeout", "history request exceeded total timeout", request_id, json!({})),
        },
        _ = wait_for_disconnect(&mut stream) => return Ok(()),
    };
    match timeout_at(
        deadline,
        write_response(&mut stream, response, if_none_match.as_deref(), &state),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Ok(()),
    }
}

async fn wait_for_disconnect(stream: &mut TcpStream) {
    let mut byte = [0_u8; 1];
    loop {
        match stream.read(&mut byte).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
    }
}

async fn route_request(
    request: &str,
    identity_header: &str,
    state: Arc<HistoryState>,
    data_id: u64,
    request_id: String,
) -> Response {
    let Ok(parsed) = parse_request(request) else {
        return error_response(
            400,
            "invalid_request",
            "malformed HTTP request",
            request_id,
            json!({}),
        );
    };
    if !has_exactly_one_identity(&parsed.headers, identity_header) {
        return error_response(
            400,
            "missing_identity",
            "trusted identity header required exactly once",
            request_id,
            json!({}),
        );
    }
    if !is_known_path(parsed.path) {
        return error_response(
            404,
            "route_not_found",
            "unknown history endpoint",
            request_id,
            json!({}),
        );
    }
    if parsed.method != "GET" || parsed.path == SCHEMA_PATH && parsed.query.is_some() {
        return error_response(
            400,
            "invalid_request",
            "history endpoint only accepts GET without unsupported parameters",
            request_id,
            json!({}),
        );
    }
    if parsed.path == SCHEMA_PATH {
        return Response::success(schema_response(), request_id);
    }
    let data = match parse_data_request(parsed.path, parsed.query, data_id) {
        Ok(value) => value,
        Err(message) => {
            return error_response(400, "invalid_request", message, request_id, json!({}));
        }
    };
    let admission = match timeout(QUEUE_TIMEOUT, state.active.clone().acquire_owned()).await {
        Ok(Ok(permit)) => Arc::new(ActiveRequestPin { _permit: permit }),
        _ => {
            return error_response(
                429,
                "history_overloaded",
                "history request admission queue is full",
                request_id,
                json!({"retry_after_ms": 100}),
            );
        }
    };
    let Some(snapshot) = state.snapshots.current() else {
        return error_with_permit(
            503,
            "history_unavailable",
            "no valid history snapshot is loaded",
            request_id,
            json!({}),
            admission,
        );
    };
    if snapshot.is_unhealthy() {
        return error_with_permit(
            503,
            "snapshot_unhealthy",
            "loaded history snapshot is unhealthy",
            request_id,
            json!({"snapshot_id": snapshot.snapshot_id()}),
            admission,
        );
    }
    if parsed.path == COVERAGE_PATH {
        let series_name = data.series_name();
        return match snapshot.inspect(data.request).await {
            Ok(inspection) => {
                let report = inspection.report();
                let mut body = Map::new();
                body.insert("snapshot_id".into(), json!(snapshot.snapshot_id()));
                body.insert("symbol".into(), json!(data.symbol));
                body.insert("series".into(), json!(series_name));
                if let Some(period) = data.period {
                    body.insert("period".into(), json!(period));
                }
                body.insert("start".into(), json!(data.start));
                body.insert("end".into(), json!(data.end));
                body.insert("complete".into(), json!(true));
                body.insert(
                    "final".into(),
                    json!(matches!(
                        report.coverage.finality,
                        BacktestHistoryFinality::Final
                    )),
                );
                body.insert(
                    "metadata_snapshot_hash".into(),
                    json!(snapshot.metadata_snapshot_hash()),
                );
                Response::success_with_permit(Value::Object(body), request_id, admission)
            }
            Err(error) => snapshot_error(error, &snapshot, request_id, admission),
        };
    }
    query_response(snapshot, data, request_id, admission, &state).await
}

async fn query_response(
    snapshot: PinnedSnapshot,
    data: ParsedDataRequest,
    request_id: String,
    admission: Arc<ActiveRequestPin>,
    state: &Arc<HistoryState>,
) -> Response {
    let codec = match HistoryRowCodec::new(data.series, data.columns) {
        Ok(value) => value,
        Err(error) => {
            return error_with_permit(
                400,
                "invalid_request",
                &error.to_string(),
                request_id,
                json!({}),
                admission,
            );
        }
    };
    let resources =
        BacktestHistorySnapshotQueryResources::new(state.scan_budget.clone(), admission.clone());
    let mut run = match snapshot.query_with_resources(data.request, resources).await {
        Ok(value) => value,
        Err(error) => {
            return snapshot_error(error, &snapshot, request_id, admission);
        }
    };
    let row_limit = match data.series {
        BacktestHistorySchemaSeries::Tick => MAX_TICK_ROWS,
        BacktestHistorySchemaSeries::Kline => MAX_KLINE_ROWS,
    };
    let mut rows = Vec::new();
    let mut bytes = 0_usize;
    let mut held = Vec::new();
    loop {
        match run.next().await {
            Some(BacktestHistorySnapshotEvent::Chunk(chunk)) => {
                let Some(json_upper_bound) = chunk
                    .rows
                    .len()
                    .checked_mul(codec.column_names().len())
                    .and_then(|cells| cells.checked_mul(JSON_CELL_ALLOCATION_BYTES))
                    .and_then(|bytes| {
                        chunk
                            .rows
                            .len()
                            .checked_mul(64)
                            .and_then(|rows| bytes.checked_add(rows))
                    })
                else {
                    return error_with_permit(
                        413,
                        "response_too_large",
                        "history JSON allocation upper bound overflowed",
                        request_id,
                        json!({"limit_bytes": MAX_RESPONSE_BYTES}),
                        admission,
                    );
                };
                let Some(json_permit) = state.buffers.try_reserve(json_upper_bound) else {
                    return error_with_permit(
                        429,
                        "history_overloaded",
                        "history daemon global buffer budget is exhausted",
                        request_id,
                        json!({"limit_bytes": GLOBAL_BUFFER_BYTES}),
                        admission,
                    );
                };
                let encoded = match codec.encode_chunk(&chunk.rows) {
                    Ok(value) => value,
                    Err(error) => {
                        return error_with_permit(
                            500,
                            "history_internal",
                            &error.to_string(),
                            request_id,
                            json!({}),
                            admission,
                        );
                    }
                };
                if encoded.estimated_json_bytes > json_upper_bound {
                    return error_with_permit(
                        500,
                        "history_internal",
                        "history JSON allocation exceeded its reserved upper bound",
                        request_id,
                        json!({}),
                        admission,
                    );
                }
                if rows.len().saturating_add(encoded.row_count) > row_limit {
                    return error_with_permit(
                        413,
                        "row_limit_exceeded",
                        "history response row limit exceeded",
                        request_id,
                        json!({"limit_rows": row_limit}),
                        admission,
                    );
                }
                let Some(next_bytes) = bytes.checked_add(encoded.estimated_json_bytes) else {
                    return error_with_permit(
                        413,
                        "response_too_large",
                        "history response byte count overflowed",
                        request_id,
                        json!({"limit_bytes": MAX_RESPONSE_BYTES}),
                        admission,
                    );
                };
                if next_bytes > MAX_RESPONSE_BYTES {
                    return error_with_permit(
                        413,
                        "response_too_large",
                        "history response exceeds uncompressed byte limit",
                        request_id,
                        json!({"limit_bytes": MAX_RESPONSE_BYTES}),
                        admission,
                    );
                }
                bytes = next_bytes;
                rows.extend(encoded.rows);
                held.push(json_permit);
            }
            Some(BacktestHistorySnapshotEvent::RequestCompleted(report)) => {
                if report.rows != rows.len()
                    || !matches!(report.coverage.finality, BacktestHistoryFinality::Final)
                {
                    return error_with_permit(
                        500,
                        "history_internal",
                        "history terminal report disagrees with buffered response",
                        request_id,
                        json!({}),
                        admission,
                    );
                }
                let mut body = Map::new();
                body.insert("snapshot_id".into(), json!(snapshot.snapshot_id()));
                body.insert("columns".into(), json!(codec.column_names()));
                body.insert("rows".into(), Value::Array(rows));
                if data.include_provenance
                    && data.symbol.starts_with("KQ.m@")
                    && !report.physical_segments.is_empty()
                {
                    let segments = report
                        .physical_segments
                        .iter()
                        .map(|segment| {
                            json!({
                                "symbol": segment.physical_symbol,
                                "start": format_ns(segment.start_ns),
                                "end": format_ns(segment.end_ns),
                            })
                        })
                        .collect::<Vec<_>>();
                    body.insert(
                        "provenance".into(),
                        json!({"logical_symbol": data.symbol, "segments": segments}),
                    );
                }
                return Response::success_with_resources(
                    Value::Object(body),
                    request_id,
                    admission,
                    held,
                );
            }
            Some(BacktestHistorySnapshotEvent::RequestFailed { error, .. }) => {
                return snapshot_error(error, &snapshot, request_id, admission);
            }
            None => {
                return error_with_permit(
                    500,
                    "history_internal",
                    "history query ended without terminal result",
                    request_id,
                    json!({}),
                    admission,
                );
            }
        }
    }
}

struct ParsedDataRequest {
    request: BacktestHistoryRequest,
    symbol: String,
    series: BacktestHistorySchemaSeries,
    period: Option<String>,
    start: String,
    end: String,
    columns: Vec<HistoryColumn>,
    include_provenance: bool,
}

impl ParsedDataRequest {
    const fn series_name(&self) -> &'static str {
        match self.series {
            BacktestHistorySchemaSeries::Tick => "tick",
            BacktestHistorySchemaSeries::Kline => "kline",
        }
    }
}

fn parse_data_request(
    path: &str,
    query: Option<&str>,
    request_id: u64,
) -> Result<ParsedDataRequest, &'static str> {
    let query = query
        .filter(|value| !value.is_empty())
        .ok_or("missing query parameters")?;
    let parameters = parse_query_parameters(query)?;
    let allowed = if path == QUERY_PATH {
        [
            "symbol", "series", "period", "start", "end", "fields", "include",
        ]
        .as_slice()
    } else {
        ["symbol", "series", "period", "start", "end"].as_slice()
    };
    if parameters
        .keys()
        .any(|key| !allowed.contains(&key.as_str()))
    {
        return Err("unknown history query parameter");
    }
    let required = |name| {
        parameters
            .get(name)
            .filter(|value| !value.is_empty())
            .cloned()
            .ok_or("missing required history query parameter")
    };
    let symbol = required("symbol")?;
    if !is_history_symbol(&symbol) {
        return Err("invalid history symbol");
    }
    let series = match required("series")?.as_str() {
        "tick" => BacktestHistorySchemaSeries::Tick,
        "kline" => BacktestHistorySchemaSeries::Kline,
        _ => return Err("series must be tick or kline"),
    };
    let start_ns = parse_rfc3339_ns(&required("start")?)?;
    let end_ns = parse_rfc3339_ns(&required("end")?)?;
    if end_ns <= start_ns {
        return Err("history end must be after start");
    }
    let period = parameters.get("period").cloned();
    let request = match series {
        BacktestHistorySchemaSeries::Tick => {
            if period.is_some() {
                return Err("tick requests must not include period");
            }
            BacktestHistoryRequest::tick(request_id, symbol.clone(), start_ns, end_ns)
        }
        BacktestHistorySchemaSeries::Kline => {
            let value = period.as_deref().ok_or("kline requests require period")?;
            BacktestHistoryRequest::kline(
                request_id,
                symbol.clone(),
                Duration::from_nanos(parse_legal_period_ns(value)?),
                start_ns,
                end_ns,
            )
        }
    };
    let include_provenance = match parameters.get("include").map(String::as_str) {
        None => false,
        Some("provenance") => true,
        Some(_) => return Err("include only accepts provenance"),
    };
    let columns = if path == QUERY_PATH {
        parse_projection(series, parameters.get("fields").map(String::as_str))?
    } else {
        Vec::new()
    };
    Ok(ParsedDataRequest {
        request,
        symbol,
        series,
        period,
        start: format_ns(start_ns),
        end: format_ns(end_ns),
        columns,
        include_provenance,
    })
}

fn parse_projection(
    series: BacktestHistorySchemaSeries,
    fields: Option<&str>,
) -> Result<Vec<HistoryColumn>, &'static str> {
    let Some(fields) = fields else {
        return Ok(backtest_history_default_fields(series)
            .iter()
            .copied()
            .map(HistoryColumn::Field)
            .collect());
    };
    if fields.is_empty() {
        return Err("fields must not be empty");
    }
    let mut seen = BTreeSet::new();
    let mut columns = Vec::new();
    for alias in fields.split(',').map(str::trim) {
        if alias.is_empty() {
            return Err("fields contains an empty value");
        }
        if alias.eq_ignore_ascii_case("tns") {
            if !seen.insert("tns") {
                return Err("fields contains a duplicate value");
            }
            columns.push(HistoryColumn::RawNanoseconds);
            continue;
        }
        let field = backtest_history_resolve_fields(series, [alias])
            .map_err(|_| "fields contains an unsupported value")?
            .into_iter()
            .next()
            .ok_or("fields contains an unsupported value")?;
        if !seen.insert(field.canonical_name()) {
            return Err("fields contains a duplicate value");
        }
        columns.push(HistoryColumn::Field(field));
    }
    Ok(columns)
}

fn schema_response() -> Value {
    json!({
        "wire_version": "tqsdk-history-http/1",
        "series": [
            schema_series("tick", BacktestHistorySchemaSeries::Tick),
            schema_series("kline", BacktestHistorySchemaSeries::Kline),
        ],
        "period_classes": [
            {"name": "sub_minute", "description": "supported existing sub-minute periods below 60 seconds"},
            {"name": "intraday_minutes", "description": "integer minute periods from 1m"},
            {"name": "trading_days", "description": "integer day periods 1d through 28d"},
        ],
        "derived_fields": [
            {"canonical_name": "tns", "value_kind": "integer", "description": "raw nanosecond timestamp"}
        ],
    })
}

fn schema_series(name: &'static str, series: BacktestHistorySchemaSeries) -> Value {
    let fields = backtest_history_schema_fields(series)
        .iter()
        .map(|field| {
            json!({
                "canonical_name": field.canonical_name(),
                "aliases": field.aliases(),
                "value_kind": value_kind_name(field.value_kind()),
            })
        })
        .collect::<Vec<_>>();
    let defaults = backtest_history_default_fields(series)
        .iter()
        .map(|field| field.canonical_name())
        .collect::<Vec<_>>();
    json!({"name": name, "fields": fields, "default_fields": defaults})
}

const fn value_kind_name(kind: BacktestHistoryValueKind) -> &'static str {
    match kind {
        BacktestHistoryValueKind::Timestamp => "timestamp",
        BacktestHistoryValueKind::Integer => "integer",
        BacktestHistoryValueKind::Price => "price",
        BacktestHistoryValueKind::Decimal => "decimal",
    }
}

async fn read_request(stream: &mut TcpStream) -> RelayResult<String> {
    let mut buffer = Vec::with_capacity(1024);
    let mut chunk = [0_u8; 1024];
    loop {
        let read = stream.read(&mut chunk).await.map_err(|error| {
            RelayError::Transport(format!("history request read failed: {error}"))
        })?;
        if read == 0 {
            return Err(RelayError::invalid_protocol(
                "history request closed before headers",
            ));
        }
        buffer.extend_from_slice(&chunk[..read]);
        if buffer.len() > MAX_REQUEST_BYTES {
            return Err(RelayError::invalid_protocol(
                "history request headers exceed limit",
            ));
        }
        if buffer.windows(4).any(|window| window == b"\r\n\r\n") {
            return String::from_utf8(buffer)
                .map_err(|_| RelayError::invalid_protocol("history request must be UTF-8"));
        }
    }
}

struct ParsedRequest<'a> {
    method: &'a str,
    path: &'a str,
    query: Option<&'a str>,
    headers: Vec<(&'a str, &'a str)>,
}

fn parse_request(request: &str) -> Result<ParsedRequest<'_>, ()> {
    let mut lines = request.split("\r\n");
    let mut parts = lines.next().ok_or(())?.split_ascii_whitespace();
    let method = parts.next().ok_or(())?;
    let target = parts.next().ok_or(())?;
    if parts.next() != Some("HTTP/1.1") || parts.next().is_some() {
        return Err(());
    }
    let (path, query) = target
        .split_once('?')
        .map_or((target, None), |(path, query)| (path, Some(query)));
    if path.is_empty() || !path.starts_with('/') {
        return Err(());
    }
    let headers = lines
        .take_while(|line| !line.is_empty())
        .map(|line| {
            line.split_once(':')
                .map(|(name, value)| (name.trim(), value.trim()))
                .ok_or(())
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ParsedRequest {
        method,
        path,
        query,
        headers,
    })
}

fn header_values<'a>(headers: &'a [(&str, &'a str)], expected: &str) -> Vec<&'a str> {
    headers
        .iter()
        .filter_map(|(name, value)| name.eq_ignore_ascii_case(expected).then_some(*value))
        .collect()
}

fn has_exactly_one_identity(headers: &[(&str, &str)], expected: &str) -> bool {
    let values = header_values(headers, expected);
    values.len() == 1
        && !values[0].is_empty()
        && values[0].len() <= MAX_IDENTITY_BYTES
        && !values[0]
            .chars()
            .any(|value| value.is_control() && value != '\t')
}

fn is_history_symbol(symbol: &str) -> bool {
    symbol
        .strip_prefix("KQ.i@")
        .or_else(|| symbol.strip_prefix("KQ.m@"))
        .map_or_else(|| is_qualified_symbol(symbol), is_qualified_symbol)
}

fn is_qualified_symbol(symbol: &str) -> bool {
    let Some((exchange, instrument)) = symbol.split_once('.') else {
        return false;
    };
    !exchange.is_empty()
        && !instrument.is_empty()
        && !instrument.contains('.')
        && exchange
            .bytes()
            .all(|value| value.is_ascii_uppercase() || value.is_ascii_digit())
        && instrument
            .bytes()
            .all(|value| value.is_ascii_alphanumeric() || matches!(value, b'_' | b'-'))
}

fn parse_legal_period_ns(period: &str) -> Result<u64, &'static str> {
    let split = period
        .find(|value: char| !value.is_ascii_digit())
        .ok_or("period must include a unit")?;
    let magnitude = period[..split]
        .parse::<u64>()
        .map_err(|_| "period magnitude is invalid")?;
    if magnitude == 0 {
        return Err("period must be positive");
    }
    let multiplier = match &period[split..] {
        "ns" => 1,
        "us" => 1_000,
        "ms" => 1_000_000,
        "s" => SECOND_NS,
        "m" => MINUTE_NS,
        "h" => 60 * MINUTE_NS,
        "d" => DAY_NS,
        _ => return Err("period unit is invalid"),
    };
    let value = magnitude
        .checked_mul(multiplier)
        .ok_or("period overflows nanoseconds")?;
    let legal = value < MINUTE_NS
        || value < DAY_NS && value % MINUTE_NS == 0
        || value <= 28 * DAY_NS && value % DAY_NS == 0;
    legal.then_some(value).ok_or("period is not supported")
}

fn parse_query_parameters(query: &str) -> Result<BTreeMap<String, String>, &'static str> {
    let mut parameters = BTreeMap::new();
    for pair in query.split('&') {
        if pair.is_empty() {
            return Err("empty history query parameter");
        }
        let (name, value) = pair
            .split_once('=')
            .ok_or("query parameter is missing equals")?;
        let name = decode_query_component(name)?;
        let value = decode_query_component(value)?;
        if name.is_empty() || parameters.insert(name, value).is_some() {
            return Err("duplicate or empty history query parameter");
        }
    }
    Ok(parameters)
}

fn decode_query_component(value: &str) -> Result<String, &'static str> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' => {
                if index + 2 >= bytes.len() {
                    return Err("invalid percent escape");
                }
                let high = decode_hex(bytes[index + 1]).ok_or("invalid percent escape")?;
                let low = decode_hex(bytes[index + 2]).ok_or("invalid percent escape")?;
                decoded.push(high * 16 + low);
                index += 3;
            }
            b'+' => {
                decoded.push(b'+');
                index += 1;
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8(decoded).map_err(|_| "query parameter is not UTF-8")
}

const fn decode_hex(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn parse_rfc3339_ns(value: &str) -> Result<i64, &'static str> {
    DateTime::parse_from_rfc3339(value)
        .map_err(|_| "history timestamp must be RFC3339")?
        .timestamp_nanos_opt()
        .ok_or("history timestamp is out of range")
}

fn format_ns(value: i64) -> String {
    DateTime::<Utc>::from_timestamp(
        value.div_euclid(1_000_000_000),
        value.rem_euclid(1_000_000_000) as u32,
    )
    .expect("validated history timestamp")
    .with_timezone(&FixedOffset::east_opt(8 * 60 * 60).expect("fixed offset"))
    .to_rfc3339_opts(SecondsFormat::AutoSi, false)
}

fn is_known_path(path: &str) -> bool {
    matches!(path, SCHEMA_PATH | QUERY_PATH | COVERAGE_PATH)
}

struct Response {
    status: u16,
    body: Value,
    request_id: String,
    admission: Option<Arc<ActiveRequestPin>>,
    _buffers: Vec<BytePermit>,
}

impl Response {
    fn success(body: Value, request_id: String) -> Self {
        Self {
            status: 200,
            body,
            request_id,
            admission: None,
            _buffers: Vec::new(),
        }
    }

    fn success_with_permit(
        body: Value,
        request_id: String,
        admission: Arc<ActiveRequestPin>,
    ) -> Self {
        Self {
            status: 200,
            body,
            request_id,
            admission: Some(admission),
            _buffers: Vec::new(),
        }
    }

    fn success_with_resources(
        body: Value,
        request_id: String,
        admission: Arc<ActiveRequestPin>,
        buffers: Vec<BytePermit>,
    ) -> Self {
        Self {
            status: 200,
            body,
            request_id,
            admission: Some(admission),
            _buffers: buffers,
        }
    }
}

fn error_response(
    status: u16,
    code: &'static str,
    message: &str,
    request_id: String,
    details: Value,
) -> Response {
    let body_request_id = request_id.clone();
    Response {
        status,
        body: json!({"error": {
            "code": code,
            "message": message,
            "request_id": body_request_id,
            "details": details,
        }}),
        request_id,
        admission: None,
        _buffers: Vec::new(),
    }
}

fn error_with_permit(
    status: u16,
    code: &'static str,
    message: &str,
    request_id: String,
    details: Value,
    admission: Arc<ActiveRequestPin>,
) -> Response {
    let mut response = error_response(status, code, message, request_id, details);
    response.admission = Some(admission);
    response
}

fn snapshot_error(
    error: BacktestHistorySnapshotError,
    snapshot: &PinnedSnapshot,
    request_id: String,
    admission: Arc<ActiveRequestPin>,
) -> Response {
    let snapshot_id = snapshot.snapshot_id();
    let (status, code, details) = match error.reason() {
        BacktestHistoryFailureReason::InvalidRequest => (400, "invalid_request", json!({})),
        BacktestHistoryFailureReason::SymbolNotFound => (404, "symbol_not_found", json!({})),
        BacktestHistoryFailureReason::CoverageIncomplete { missing_ranges } => (
            409,
            "coverage_incomplete",
            json!({"missing_ranges": missing_ranges.iter().map(|(start, end)| {
                json!({"start": format_ns(*start), "end": format_ns(*end)})
            }).collect::<Vec<_>>() }),
        ),
        BacktestHistoryFailureReason::ProvisionalData { as_of_ns } => (
            409,
            "provisional_data",
            json!({"as_of": format_ns(*as_of_ns)}),
        ),
        BacktestHistoryFailureReason::MetadataIncomplete => (409, "metadata_incomplete", json!({})),
        BacktestHistoryFailureReason::ResponseTooLarge {
            limit_bytes: 0,
            attempted_bytes,
        } => (
            429,
            "history_overloaded",
            json!({"limit_bytes": GLOBAL_BUFFER_BYTES, "attempted_bytes": attempted_bytes}),
        ),
        BacktestHistoryFailureReason::ResponseTooLarge {
            limit_bytes,
            attempted_bytes,
        } => (
            413,
            "response_too_large",
            json!({"limit_bytes": limit_bytes, "attempted_bytes": attempted_bytes}),
        ),
        BacktestHistoryFailureReason::SnapshotUnavailable
        | BacktestHistoryFailureReason::SnapshotIncompatible => (
            503,
            "history_unavailable",
            json!({"snapshot_id": snapshot_id}),
        ),
        BacktestHistoryFailureReason::HistoryTimeout => (504, "history_timeout", json!({})),
        BacktestHistoryFailureReason::SnapshotCorrupt => {
            if snapshot.mark_corrupt() {
                (500, "snapshot_corrupt", json!({"snapshot_id": snapshot_id}))
            } else {
                (
                    503,
                    "snapshot_unhealthy",
                    json!({"snapshot_id": snapshot_id}),
                )
            }
        }
        BacktestHistoryFailureReason::Cancelled | BacktestHistoryFailureReason::Internal => {
            (500, "history_internal", json!({}))
        }
        _ => (500, "history_internal", json!({})),
    };
    error_with_permit(
        status,
        code,
        &error.to_string(),
        request_id,
        details,
        admission,
    )
}

fn next_id() -> (u64, String) {
    let id = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    (id, format!("r-{id}"))
}

async fn write_response(
    stream: &mut TcpStream,
    mut response: Response,
    if_none_match: Option<&str>,
    state: &HistoryState,
) -> RelayResult<()> {
    let mut body_len = encoded_json_len(&response.body)?;
    if response.status == 200 && body_len > MAX_RESPONSE_BYTES {
        let request_id = response.request_id.clone();
        response = error_response(
            413,
            "response_too_large",
            "history response exceeds uncompressed byte limit",
            request_id,
            json!({"limit_bytes": MAX_RESPONSE_BYTES, "attempted_bytes": body_len}),
        );
        body_len = encoded_json_len(&response.body)?;
    }
    let Some(body_permit) = state.buffers.try_reserve(body_len) else {
        let fallback = error_response(
            429,
            "history_overloaded",
            "history daemon global buffer budget is exhausted",
            response.request_id.clone(),
            json!({"limit_bytes": GLOBAL_BUFFER_BYTES}),
        );
        let body = serde_json::to_vec(&fallback.body).map_err(|error| {
            RelayError::Internal(format!("history JSON encode failed: {error}"))
        })?;
        return write_bytes(stream, 429, &body, None).await;
    };
    let mut body = Vec::with_capacity(body_len);
    serde_json::to_writer(&mut body, &response.body)
        .map_err(|error| RelayError::Internal(format!("history JSON encode failed: {error}")))?;
    debug_assert_eq!(body.len(), body_len);
    let mut etag = None;
    if response.status == 200 {
        let mut digest = Sha1::new();
        digest.update(&body);
        let selected = format!("\"{:x}\"", digest.finalize());
        if if_none_match.is_some_and(|header| etag_matches(header, &selected)) {
            response.status = 304;
            body.clear();
        }
        etag = Some(selected);
    }
    let result = write_bytes(stream, response.status, &body, etag.as_deref()).await;
    drop(body_permit);
    result
}

fn encoded_json_len(value: &Value) -> RelayResult<usize> {
    let mut counter = JsonSizeCounter::default();
    serde_json::to_writer(&mut counter, value)
        .map_err(|error| RelayError::Internal(format!("history JSON size failed: {error}")))?;
    Ok(counter.bytes)
}

#[derive(Default)]
struct JsonSizeCounter {
    bytes: usize,
}

impl Write for JsonSizeCounter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(buffer.len())
            .ok_or_else(|| std::io::Error::other("history JSON size overflow"))?;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn etag_matches(header: &str, selected: &str) -> bool {
    header
        .split(',')
        .map(str::trim)
        .any(|candidate| candidate == "*" || candidate == selected)
}

async fn write_bytes(
    stream: &mut TcpStream,
    status: u16,
    body: &[u8],
    etag: Option<&str>,
) -> RelayResult<()> {
    let reason = match status {
        200 => "OK",
        304 => "Not Modified",
        400 => "Bad Request",
        404 => "Not Found",
        409 => "Conflict",
        413 => "Payload Too Large",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "Internal Server Error",
    };
    let mut headers = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    if let Some(etag) = etag {
        headers.push_str(&format!("ETag: {etag}\r\n"));
    }
    headers.push_str("\r\n");
    stream
        .write_all(headers.as_bytes())
        .await
        .map_err(|error| {
            RelayError::Transport(format!("history response headers failed: {error}"))
        })?;
    stream
        .write_all(body)
        .await
        .map_err(|error| RelayError::Transport(format!("history response body failed: {error}")))?;
    stream.shutdown().await.map_err(|error| {
        RelayError::Transport(format!("history response shutdown failed: {error}"))
    })
}

struct ActiveRequestPin {
    _permit: OwnedSemaphorePermit,
}

struct SnapshotScanBudget {
    buffers: Arc<ByteBudget>,
}

impl BacktestHistorySnapshotResourceBudget for SnapshotScanBudget {
    fn try_reserve(&self, bytes: usize) -> Option<BacktestHistorySnapshotResourceReservation> {
        self.buffers
            .try_reserve(bytes)
            .map(BacktestHistorySnapshotResourceReservation::new)
    }
}

struct ByteBudget {
    limit: usize,
    used: AtomicUsize,
}

impl ByteBudget {
    const fn new(limit: usize) -> Self {
        Self {
            limit,
            used: AtomicUsize::new(0),
        }
    }

    fn try_reserve(self: &Arc<Self>, bytes: usize) -> Option<BytePermit> {
        let mut used = self.used.load(Ordering::Acquire);
        loop {
            let next = used.checked_add(bytes)?;
            if next > self.limit {
                return None;
            }
            match self
                .used
                .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    return Some(BytePermit {
                        budget: self.clone(),
                        bytes,
                    });
                }
                Err(current) => used = current,
            }
        }
    }
}

struct BytePermit {
    budget: Arc<ByteBudget>,
    bytes: usize,
}

impl Drop for BytePermit {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use tqsdk_data::BacktestHistoryField;

    use super::*;

    #[test]
    fn schema_uses_typed_fields_and_declares_derived_tns() {
        let schema = schema_response();
        assert_eq!(schema["wire_version"], "tqsdk-history-http/1");
        assert_eq!(schema["series"][0]["fields"][0]["canonical_name"], "t");
        assert_eq!(schema["series"][1]["default_fields"][0], "t");
        assert_eq!(schema["derived_fields"][0]["canonical_name"], "tns");
    }

    #[test]
    fn query_parser_preserves_tns_projection_order() {
        let data = parse_data_request(
            QUERY_PATH,
            Some("symbol=SHFE.au2612&series=tick&start=2026-08-01T09%3A00%3A00%2B08%3A00&end=2026-08-01T10%3A00%3A00%2B08%3A00&fields=t%2Ctns%2Clp"),
            1,
        ).unwrap();
        assert_eq!(
            data.columns,
            vec![
                HistoryColumn::Field(BacktestHistoryField::Time),
                HistoryColumn::RawNanoseconds,
                HistoryColumn::Field(BacktestHistoryField::LastPrice),
            ]
        );
        assert_eq!(data.start, "2026-08-01T09:00:00+08:00");
        let raw_plus = parse_data_request(
            QUERY_PATH,
            Some("symbol=SHFE.au2612&series=tick&start=2026-08-01T09:00:00+08:00&end=2026-08-01T10:00:00+08:00"),
            2,
        )
        .unwrap();
        assert_eq!(raw_plus.start, "2026-08-01T09:00:00+08:00");
        assert!(parse_data_request(COVERAGE_PATH, Some(
            "symbol=SHFE.au2612&series=tick&start=2026-08-01T09%3A00%3A00%2B08%3A00&end=2026-08-01T10%3A00%3A00%2B08%3A00&fields=t"
        ), 2).is_err());
    }

    #[test]
    fn period_etag_and_budget_boundaries_are_stable() {
        assert!(parse_legal_period_ns("500ms").is_ok());
        assert!(parse_legal_period_ns("1m").is_ok());
        assert!(parse_legal_period_ns("28d").is_ok());
        assert!(parse_legal_period_ns("0s").is_err());
        assert!(parse_legal_period_ns("29d").is_err());
        assert!(etag_matches("\"a\", \"b\"", "\"b\""));
        assert!(etag_matches("*", "\"b\""));
        assert!(!etag_matches("W/\"b\"", "\"b\""));
        let budget = Arc::new(ByteBudget::new(10));
        let scan_budget = SnapshotScanBudget {
            buffers: budget.clone(),
        };
        let scan = scan_budget.try_reserve(6).unwrap();
        assert!(budget.try_reserve(5).is_none());
        drop(scan);
        assert!(budget.try_reserve(10).is_some());
    }
}

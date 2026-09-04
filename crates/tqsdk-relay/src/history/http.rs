use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::thread::{self, JoinHandle};
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

use super::affinity::HistoryAffinity;
use super::codec::{HistoryColumn, HistoryRowCodec};
use super::observability::{Gauge, HistoryObservability, RequestAudit};
use super::snapshot::{PinnedSnapshot, SnapshotSlot};

const MAX_REQUEST_BYTES: usize = 16 * 1024;
const MAX_IDENTITY_BYTES: usize = 512;
const MAX_KLINE_ROWS: usize = 10_000;
const MAX_TICK_ROWS: usize = 50_000;
const MAX_RESPONSE_BYTES: usize = 32 * 1024 * 1024;
const JSON_CELL_ALLOCATION_BYTES: usize = 128;
const GLOBAL_BUFFER_BYTES: usize = 512 * 1024 * 1024;
const MAX_ACTIVE_REQUESTS: usize = 8;
const QUEUE_TIMEOUT: Duration = Duration::from_millis(100);
const READ_TIMEOUT: Duration = Duration::from_secs(2);
const TOTAL_TIMEOUT: Duration = Duration::from_secs(10);
const WRITE_GRACE: Duration = Duration::from_secs(1);
const GZIP_MIN_BYTES: usize = 64 * 1024;
const GZIP_WORKERS: usize = 2;
const GZIP_QUEUE_PER_WORKER: usize = 2;
const GZIP_STARTUP_TIMEOUT: Duration = Duration::from_secs(5);
const GZIP_OUTPUT_OVERHEAD: usize = 64 * 1024;
const SCHEMA_PATH: &str = "/v1/history/schema";
const QUERY_PATH: &str = "/v1/history/query";
const COVERAGE_PATH: &str = "/v1/history/coverage";
const FUTURE_START_TOLERANCE_NS: i64 = 5_000_000_000;
const SECOND_NS: u64 = 1_000_000_000;
const MINUTE_NS: u64 = 60 * SECOND_NS;
const DAY_NS: u64 = 24 * 60 * MINUTE_NS;
static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

struct HistoryState {
    snapshots: Arc<SnapshotSlot>,
    active: Arc<Semaphore>,
    buffers: Arc<ByteBudget>,
    scan_budget: Arc<SnapshotScanBudget>,
    compression: Option<Arc<CompressionPool>>,
    observability: Arc<HistoryObservability>,
}

impl HistoryState {
    fn new(
        snapshots: Arc<SnapshotSlot>,
        compression: Option<Arc<CompressionPool>>,
        observability: Arc<HistoryObservability>,
    ) -> Self {
        let buffers = Arc::new(ByteBudget::monitored(
            GLOBAL_BUFFER_BYTES,
            observability.clone(),
        ));
        Self {
            snapshots,
            active: Arc::new(Semaphore::new(MAX_ACTIVE_REQUESTS)),
            scan_budget: Arc::new(SnapshotScanBudget {
                buffers: buffers.clone(),
            }),
            buffers,
            compression,
            observability,
        }
    }
}

pub(super) async fn serve_until(
    listener: TcpListener,
    identity_header: String,
    snapshots: Arc<SnapshotSlot>,
    compression: Option<Arc<CompressionPool>>,
    observability: Arc<HistoryObservability>,
    mut shutdown: oneshot::Receiver<()>,
) -> RelayResult<()> {
    let state = Arc::new(HistoryState::new(
        snapshots.clone(),
        compression,
        observability,
    ));
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
    let deadline = Instant::now() + TOTAL_TIMEOUT;
    let preparation_deadline = deadline - WRITE_GRACE;
    let read_deadline = (Instant::now() + READ_TIMEOUT).min(preparation_deadline);
    let (data_id, request_id) = next_id();
    let mut audit = state.observability.begin_request(request_id.clone());
    let request = match timeout_at(read_deadline, read_request(&mut stream)).await {
        Ok(Ok(value)) => value,
        Ok(Err(error)) => {
            audit.endpoint("invalid");
            return prepare_and_write_audited(
                &mut stream,
                deadline,
                error_response(
                    400,
                    "invalid_request",
                    &error.to_string(),
                    request_id,
                    json!({}),
                ),
                None,
                GzipNegotiation::default(),
                &state,
                audit,
            )
            .await;
        }
        Err(_) => {
            audit.endpoint("invalid");
            return prepare_and_write_audited(
                &mut stream,
                deadline,
                error_response(
                    400,
                    "invalid_request",
                    "history request exceeded total timeout",
                    request_id,
                    json!({}),
                ),
                None,
                GzipNegotiation::default(),
                &state,
                audit,
            )
            .await;
        }
    };
    let parsed_headers = parse_request(&request).ok().map(|parsed| parsed.headers);
    let if_none_match = parsed_headers.as_ref().and_then(|headers| {
        header_values(headers, "if-none-match")
            .first()
            .map(|value| (*value).to_owned())
    });
    let gzip = parsed_headers
        .as_ref()
        .map_or(GzipNegotiation::default(), |headers| GzipNegotiation {
            accepts: accepts_gzip(headers),
            vary: state.compression.is_some(),
        });
    audit.endpoint(
        match parse_request(&request).ok().map(|parsed| parsed.path) {
            Some(SCHEMA_PATH) => "schema",
            Some(QUERY_PATH) => "query",
            Some(COVERAGE_PATH) => "coverage",
            _ => "unknown",
        },
    );
    let response = {
        let routed = timeout_at(
            preparation_deadline,
            route_request(
                &request,
                identity_header,
                state.clone(),
                data_id,
                request_id.clone(),
                &mut audit,
            ),
        );
        tokio::pin!(routed);
        tokio::select! {
                result = &mut routed => match result {
                    Ok(response) => response,
                    Err(_) => error_response(504, "history_timeout", "history request exceeded total timeout", request_id.clone(), json!({})),
                },
                _ = wait_for_disconnect(&mut stream) => return Ok(()),
        }
    };
    let prepared = match timeout_at(
        preparation_deadline,
        prepare_response(response, if_none_match.as_deref(), gzip, &state),
    )
    .await
    {
        Ok(Ok(prepared)) => prepared,
        Ok(Err(error)) => {
            audit.finish(500, Some("history_internal"), 0);
            return Err(error);
        }
        Err(_) => match timeout_at(
            deadline,
            prepare_response(
                error_response(
                    504,
                    "history_timeout",
                    "history request exceeded total timeout",
                    request_id,
                    json!({}),
                ),
                None,
                gzip,
                &state,
            ),
        )
        .await
        {
            Ok(Ok(prepared)) => prepared,
            Ok(Err(error)) => {
                audit.finish(500, Some("history_internal"), 0);
                return Err(error);
            }
            Err(_) => {
                audit.finish(504, Some("history_timeout"), 0);
                return Ok(());
            }
        },
    };
    let status = prepared.status;
    let bytes = prepared.body.len();
    let error_code = prepared.error_code;
    match timeout_at(deadline, write_prepared_response(&mut stream, prepared)).await {
        Ok(Ok(())) => {
            audit.finish(status, error_code, bytes);
            Ok(())
        }
        Ok(Err(error)) => {
            audit.finish(500, Some("write_failed"), 0);
            Err(error)
        }
        Err(_) => {
            audit.finish(504, Some("history_timeout"), 0);
            Ok(())
        }
    }
}

async fn prepare_and_write_audited(
    stream: &mut TcpStream,
    deadline: Instant,
    response: Response,
    if_none_match: Option<&str>,
    gzip: GzipNegotiation,
    state: &HistoryState,
    audit: RequestAudit,
) -> RelayResult<()> {
    let audit = audit;
    let prepared = match timeout_at(
        deadline,
        prepare_response(response, if_none_match, gzip, state),
    )
    .await
    {
        Ok(Ok(prepared)) => prepared,
        Ok(Err(error)) => {
            audit.finish(500, Some("history_internal"), 0);
            return Err(error);
        }
        Err(_) => {
            audit.finish(504, Some("history_timeout"), 0);
            return Ok(());
        }
    };
    let status = prepared.status;
    let error_code = prepared.error_code;
    let bytes = prepared.body.len();
    match timeout_at(deadline, write_prepared_response(stream, prepared)).await {
        Ok(Ok(())) => {
            audit.finish(status, error_code, bytes);
            Ok(())
        }
        Ok(Err(error)) => {
            audit.finish(500, Some("write_failed"), 0);
            Err(error)
        }
        Err(_) => {
            audit.finish(504, Some("history_timeout"), 0);
            Ok(())
        }
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
    audit: &mut RequestAudit,
) -> Response {
    let Ok(parsed) = parse_request(request) else {
        audit.endpoint("invalid");
        return error_response(
            400,
            "invalid_request",
            "malformed HTTP request",
            request_id,
            json!({}),
        );
    };
    if parsed.method == "OPTIONS" {
        if !is_known_path(parsed.path) {
            return error_response(
                404,
                "route_not_found",
                "unknown history endpoint",
                request_id,
                json!({}),
            );
        }
        return Response::success(json!({}), request_id);
    }
    if !has_exactly_one_identity(&parsed.headers, identity_header) {
        return error_response(
            400,
            "missing_identity",
            "trusted identity header required exactly once",
            request_id,
            json!({}),
        );
    }
    if let Some(identity) = header_values(&parsed.headers, identity_header).first() {
        audit.identity(identity);
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
    audit.query(
        None,
        &data.symbol,
        data.series_name(),
        data.period.as_deref(),
        (&data.start, &data.end),
        data.columns
            .iter()
            .copied()
            .map(HistoryColumn::canonical_name)
            .collect(),
    );
    if let Some(server_time_ns) = Utc::now().timestamp_nanos_opt() {
        if request_starts_in_future(data.start_ns, server_time_ns) {
            return error_response(
                409,
                "coverage_incomplete",
                "history request range starts after server time",
                request_id,
                json!({
                    "reason": "range_starts_in_future",
                    "requested_start": data.start,
                    "server_time": format_ns(server_time_ns),
                    "retryable": true,
                    "clock_skew_tolerance_seconds": 5,
                }),
            );
        }
    }
    let queue_gauge = state.observability.request_queued();
    let admission = match timeout(QUEUE_TIMEOUT, state.active.clone().acquire_owned()).await {
        Ok(Ok(permit)) => {
            drop(queue_gauge);
            Arc::new(ActiveRequestPin {
                _permit: permit,
                _gauge: Some(state.observability.request_active()),
            })
        }
        _ => {
            drop(queue_gauge);
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
    audit.query(
        Some(snapshot.snapshot_id()),
        &data.symbol,
        data.series_name(),
        data.period.as_deref(),
        (&data.start, &data.end),
        data.columns
            .iter()
            .copied()
            .map(HistoryColumn::canonical_name)
            .collect(),
    );
    if parsed.path == COVERAGE_PATH {
        let series_name = data.series_name();
        return match snapshot.inspect(data.request).await {
            Ok(inspection) => {
                let report = inspection.report();
                audit.rows(0);
                let mut body = Map::new();
                body.insert("snapshot_id".into(), json!(snapshot.snapshot_id()));
                body.insert("source_mode".into(), json!(snapshot.source_mode()));
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
                    json!(snapshot.metadata_snapshot_hash(report)),
                );
                Response::success_with_permit(Value::Object(body), request_id, admission)
            }
            Err(error) => snapshot_error(error, &snapshot, request_id, admission),
        };
    }
    query_response(snapshot, data, request_id, admission, &state, audit).await
}

async fn query_response(
    snapshot: PinnedSnapshot,
    data: ParsedDataRequest,
    request_id: String,
    admission: Arc<ActiveRequestPin>,
    state: &Arc<HistoryState>,
    audit: &mut RequestAudit,
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
                audit.rows(rows.len());
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
                body.insert("source_mode".into(), json!(snapshot.source_mode()));
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
    start_ns: i64,
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
        start_ns,
        start: format_ns(start_ns),
        end: format_ns(end_ns),
        columns,
        include_provenance,
    })
}

fn request_starts_in_future(start_ns: i64, server_time_ns: i64) -> bool {
    start_ns > server_time_ns.saturating_add(FUTURE_START_TOLERANCE_NS)
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
    error_code: Option<&'static str>,
    body: Value,
    request_id: String,
    admission: Option<Arc<ActiveRequestPin>>,
    _buffers: Vec<BytePermit>,
}

impl Response {
    fn success(body: Value, request_id: String) -> Self {
        Self {
            status: 200,
            error_code: None,
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
            error_code: None,
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
            error_code: None,
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
        error_code: Some(code),
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

async fn prepare_response(
    mut response: Response,
    if_none_match: Option<&str>,
    gzip: GzipNegotiation,
    state: &HistoryState,
) -> RelayResult<PreparedResponse> {
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
    let Some(mut body_permit) = state.buffers.try_reserve(body_len) else {
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
        return Ok(PreparedResponse::unbudgeted(
            429,
            fallback.error_code,
            body,
            None,
            false,
            gzip.vary,
            response.admission,
        ));
    };
    let mut body = Vec::with_capacity(body_len);
    serde_json::to_writer(&mut body, &response.body)
        .map_err(|error| RelayError::Internal(format!("history JSON encode failed: {error}")))?;
    debug_assert_eq!(body.len(), body_len);
    let mut content_encoding_gzip = false;
    if response.status == 200 && body.len() >= GZIP_MIN_BYTES && gzip.accepts {
        if let Some(compression) = &state.compression {
            match compression.try_compress(
                body,
                body_permit,
                response.admission.clone(),
                &state.buffers,
            ) {
                Ok(receiver) => match receiver.await {
                    Ok(CompressionResult::Gzip {
                        body: compressed,
                        permit,
                    }) => {
                        body = compressed;
                        body_permit = permit;
                        content_encoding_gzip = true;
                    }
                    Ok(CompressionResult::Identity {
                        body: identity,
                        permit,
                    }) => {
                        body = identity;
                        body_permit = permit;
                    }
                    Err(_) => {
                        let fallback = error_response(
                            500,
                            "history_internal",
                            "history gzip worker stopped before completing response",
                            response.request_id.clone(),
                            json!({}),
                        );
                        let fallback_body =
                            serde_json::to_vec(&fallback.body).map_err(|error| {
                                RelayError::Internal(format!("history JSON encode failed: {error}"))
                            })?;
                        return Ok(PreparedResponse::unbudgeted(
                            500,
                            fallback.error_code,
                            fallback_body,
                            None,
                            false,
                            gzip.vary,
                            response.admission,
                        ));
                    }
                },
                Err((identity, permit)) => {
                    state.observability.compression_fallback();
                    body = identity;
                    body_permit = permit;
                }
            }
        }
    }
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
    Ok(PreparedResponse {
        status: response.status,
        error_code: response.error_code,
        body,
        etag,
        gzip: content_encoding_gzip,
        vary_accept_encoding: gzip.vary,
        _body_permit: Some(body_permit),
        _admission: response.admission,
    })
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

#[derive(Clone, Copy, Default)]
struct GzipNegotiation {
    accepts: bool,
    vary: bool,
}

fn accepts_gzip(headers: &[(&str, &str)]) -> bool {
    let mut explicit_gzip = false;
    for value in header_values(headers, "accept-encoding") {
        for coding in value.split(',') {
            let mut pieces = coding.split(';');
            let is_gzip = pieces
                .next()
                .is_some_and(|name| name.trim().eq_ignore_ascii_case("gzip"));
            if !is_gzip {
                continue;
            }
            explicit_gzip = true;
            let mut quality = None;
            for parameter in pieces {
                if let Some((name, value)) = parameter.trim().split_once('=')
                    && name.trim().eq_ignore_ascii_case("q")
                    && quality.replace(value.trim()).is_some()
                {
                    return false;
                }
            }
            if !quality.is_none_or(positive_http_qvalue) {
                return false;
            }
        }
    }
    explicit_gzip
}

fn positive_http_qvalue(value: &str) -> bool {
    let Some((whole, fraction)) = value.split_once('.') else {
        return value == "1";
    };
    if fraction.len() > 3 || !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
        return false;
    }
    match whole {
        "0" => fraction.bytes().any(|byte| byte != b'0'),
        "1" => fraction.bytes().all(|byte| byte == b'0'),
        _ => false,
    }
}

struct PreparedResponse {
    status: u16,
    error_code: Option<&'static str>,
    body: Vec<u8>,
    etag: Option<String>,
    gzip: bool,
    vary_accept_encoding: bool,
    _body_permit: Option<BytePermit>,
    _admission: Option<Arc<ActiveRequestPin>>,
}

impl PreparedResponse {
    fn unbudgeted(
        status: u16,
        error_code: Option<&'static str>,
        body: Vec<u8>,
        etag: Option<String>,
        gzip: bool,
        vary_accept_encoding: bool,
        admission: Option<Arc<ActiveRequestPin>>,
    ) -> Self {
        Self {
            status,
            error_code,
            body,
            etag,
            gzip,
            vary_accept_encoding,
            // The global budget was exhausted in this fallback path, so the
            // small deterministic error is intentionally unbudgeted.
            _body_permit: None,
            _admission: admission,
        }
    }
}

async fn write_prepared_response(
    stream: &mut TcpStream,
    prepared: PreparedResponse,
) -> RelayResult<()> {
    let PreparedResponse {
        status,
        error_code: _,
        body,
        etag,
        gzip,
        vary_accept_encoding,
        _body_permit,
        _admission,
    } = prepared;
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
    headers.push_str("Access-Control-Allow-Origin: *\r\n");
    headers.push_str("Access-Control-Allow-Methods: GET, OPTIONS\r\n");
    headers.push_str("Access-Control-Allow-Headers: If-None-Match\r\n");
    headers.push_str("Access-Control-Expose-Headers: ETag\r\n");
    headers.push_str("Access-Control-Max-Age: 600\r\n");
    if let Some(etag) = etag {
        headers.push_str(&format!("ETag: {etag}\r\n"));
    }
    if gzip {
        headers.push_str("Content-Encoding: gzip\r\n");
    }
    if vary_accept_encoding {
        headers.push_str("Vary: Accept-Encoding\r\n");
    }
    headers.push_str("\r\n");
    stream
        .write_all(headers.as_bytes())
        .await
        .map_err(|error| {
            RelayError::Transport(format!("history response headers failed: {error}"))
        })?;
    stream
        .write_all(&body)
        .await
        .map_err(|error| RelayError::Transport(format!("history response body failed: {error}")))?;
    stream.shutdown().await.map_err(|error| {
        RelayError::Transport(format!("history response shutdown failed: {error}"))
    })
}

struct ActiveRequestPin {
    _permit: OwnedSemaphorePermit,
    _gauge: Option<Gauge>,
}

/// A deliberately small, dedicated compression pool.  It is not Tokio's
/// blocking pool: each worker binds before accepting work and owns every byte
/// permit for the complete lifetime of a compression job.
pub(super) struct CompressionPool {
    queues: Mutex<Option<Vec<SyncSender<CompressionJob>>>>,
    next: AtomicUsize,
    stopping: std::sync::atomic::AtomicBool,
    threads: Mutex<Vec<JoinHandle<()>>>,
    observability: Mutex<Option<Arc<HistoryObservability>>>,
    #[cfg(test)]
    _parked_receivers: Mutex<Vec<mpsc::Receiver<CompressionJob>>>,
}

impl std::fmt::Debug for CompressionPool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CompressionPool")
            .finish_non_exhaustive()
    }
}

impl CompressionPool {
    pub(super) fn attach_observability(&self, observability: Arc<HistoryObservability>) {
        *self
            .observability
            .lock()
            .expect("history compression observability lock poisoned") = Some(observability);
    }

    pub(super) fn spawn(affinity: HistoryAffinity) -> RelayResult<Arc<Self>> {
        Self::spawn_inner(Some(affinity))
    }

    #[cfg(test)]
    fn spawn_for_test() -> RelayResult<Arc<Self>> {
        Self::spawn_inner(None)
    }

    #[cfg(test)]
    fn saturated_for_test() -> Arc<Self> {
        let mut queues = Vec::with_capacity(GZIP_WORKERS);
        let mut parked_receivers = Vec::with_capacity(GZIP_WORKERS);
        for _ in 0..GZIP_WORKERS {
            let (sender, receiver) = mpsc::sync_channel(GZIP_QUEUE_PER_WORKER);
            queues.push(sender);
            parked_receivers.push(receiver);
        }
        Arc::new(Self {
            queues: Mutex::new(Some(queues)),
            next: AtomicUsize::new(0),
            stopping: std::sync::atomic::AtomicBool::new(false),
            threads: Mutex::new(Vec::new()),
            observability: Mutex::new(None),
            _parked_receivers: Mutex::new(parked_receivers),
        })
    }

    fn spawn_inner(affinity: Option<HistoryAffinity>) -> RelayResult<Arc<Self>> {
        let mut queues = Vec::with_capacity(GZIP_WORKERS);
        let mut receivers = Vec::with_capacity(GZIP_WORKERS);
        for _ in 0..GZIP_WORKERS {
            let (sender, receiver) = mpsc::sync_channel(GZIP_QUEUE_PER_WORKER);
            queues.push(sender);
            receivers.push(receiver);
        }
        let pool = Arc::new(Self {
            queues: Mutex::new(Some(queues)),
            next: AtomicUsize::new(0),
            stopping: std::sync::atomic::AtomicBool::new(false),
            threads: Mutex::new(Vec::with_capacity(GZIP_WORKERS)),
            observability: Mutex::new(None),
            #[cfg(test)]
            _parked_receivers: Mutex::new(Vec::new()),
        });
        let (ready_tx, ready_rx) = mpsc::sync_channel(GZIP_WORKERS);
        for (worker, receiver) in receivers.into_iter().enumerate() {
            let worker_pool = pool.clone();
            let worker_affinity = affinity.clone();
            let ready = ready_tx.clone();
            let thread = match thread::Builder::new()
                .name(format!("tqsdk-history-gzip-{worker}"))
                .spawn(move || compression_worker(receiver, worker_pool, worker_affinity, ready))
            {
                Ok(thread) => thread,
                Err(error) => {
                    let _ = pool.shutdown();
                    return Err(RelayError::Internal(format!(
                        "history gzip worker spawn failed: {error}"
                    )));
                }
            };
            pool.threads
                .lock()
                .map_err(|_| RelayError::Internal("history gzip lock poisoned".to_string()))?
                .push(thread);
        }
        drop(ready_tx);
        for _ in 0..GZIP_WORKERS {
            match ready_rx.recv_timeout(GZIP_STARTUP_TIMEOUT) {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    let _ = pool.shutdown();
                    return Err(RelayError::invalid_config(error));
                }
                Err(error) => {
                    let _ = pool.shutdown();
                    return Err(RelayError::Internal(format!(
                        "history gzip workers did not become ready: {error}"
                    )));
                }
            }
        }
        Ok(pool)
    }

    fn try_compress(
        &self,
        body: Vec<u8>,
        identity_permit: BytePermit,
        active_pin: Option<Arc<ActiveRequestPin>>,
        buffers: &Arc<ByteBudget>,
    ) -> Result<oneshot::Receiver<CompressionResult>, (Vec<u8>, BytePermit)> {
        if self.stopping.load(Ordering::Acquire) {
            return Err((body, identity_permit));
        }
        let Some(limit) = gzip_output_upper_bound(body.len()) else {
            return Err((body, identity_permit));
        };
        let Some(output_permit) = buffers.try_reserve(limit) else {
            return Err((body, identity_permit));
        };
        let observability = self
            .observability
            .lock()
            .expect("history compression observability lock poisoned")
            .clone();
        let (complete, receiver) = oneshot::channel();
        let mut job = CompressionJob {
            body,
            identity_permit,
            _active_pin: active_pin,
            output_permit,
            observability: observability.clone(),
            queued_gauge: observability
                .as_ref()
                .map(|observability| observability.compression_queued()),
            complete,
            #[cfg(test)]
            before_compress: None,
        };
        let Ok(queues) = self.queues.lock() else {
            return Err((job.body, job.identity_permit));
        };
        let Some(queues) = queues.as_ref() else {
            return Err((job.body, job.identity_permit));
        };
        if self.stopping.load(Ordering::Acquire) {
            return Err((job.body, job.identity_permit));
        }
        let start = self.next.fetch_add(1, Ordering::Relaxed) % queues.len();
        for offset in 0..queues.len() {
            let index = (start + offset) % queues.len();
            match queues[index].try_send(job) {
                Ok(()) => return Ok(receiver),
                Err(TrySendError::Full(returned)) | Err(TrySendError::Disconnected(returned)) => {
                    job = returned;
                }
            }
        }
        Err((job.body, job.identity_permit))
    }

    pub(super) fn shutdown(&self) -> RelayResult<()> {
        let mut queues = self
            .queues
            .lock()
            .map_err(|_| RelayError::Internal("history gzip queue lock poisoned".to_string()))?;
        self.stopping.store(true, Ordering::Release);
        drop(queues.take());
        drop(queues);
        #[cfg(test)]
        self._parked_receivers
            .lock()
            .map_err(|_| RelayError::Internal("history gzip test queue lock poisoned".to_string()))?
            .clear();
        let mut threads = self
            .threads
            .lock()
            .map_err(|_| RelayError::Internal("history gzip lock poisoned".to_string()))?;
        for thread in threads.drain(..) {
            thread.join().map_err(|_| {
                RelayError::Internal("history gzip worker panicked during shutdown".to_string())
            })?;
        }
        Ok(())
    }
}

struct CompressionJob {
    body: Vec<u8>,
    identity_permit: BytePermit,
    _active_pin: Option<Arc<ActiveRequestPin>>,
    output_permit: BytePermit,
    observability: Option<Arc<HistoryObservability>>,
    queued_gauge: Option<Gauge>,
    complete: oneshot::Sender<CompressionResult>,
    #[cfg(test)]
    before_compress: Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>,
}

enum CompressionResult {
    Gzip { body: Vec<u8>, permit: BytePermit },
    Identity { body: Vec<u8>, permit: BytePermit },
}

fn compression_worker(
    receiver: mpsc::Receiver<CompressionJob>,
    pool: Arc<CompressionPool>,
    affinity: Option<HistoryAffinity>,
    ready: mpsc::SyncSender<Result<(), String>>,
) {
    if let Err(error) = affinity.map_or(Ok(()), |affinity| affinity.bind_current()) {
        let _ = ready.send(Err(error.to_string()));
        return;
    }
    if ready.send(Ok(())).is_err() {
        return;
    }
    loop {
        match receiver.recv() {
            Ok(job) if pool.stopping.load(Ordering::Acquire) => drop(job),
            Ok(job) => compress_job(job),
            Err(_) => return,
        }
    }
}

fn compress_job(job: CompressionJob) {
    let CompressionJob {
        body,
        identity_permit,
        _active_pin,
        output_permit,
        observability,
        queued_gauge,
        complete,
        #[cfg(test)]
        before_compress,
    } = job;
    drop(queued_gauge);
    let _active_gauge = observability
        .as_ref()
        .map(|observability| observability.compression_active());
    #[cfg(test)]
    if let Some((started, release)) = before_compress {
        let _ = started.send(());
        let _ = release.recv();
    }
    let maximum = output_permit.bytes;
    let mut encoder =
        flate2::write::GzEncoder::new(BoundedBytes::new(maximum), flate2::Compression::fast());
    let result = encoder
        .write_all(&body)
        .and_then(|()| encoder.finish())
        .map(|output| CompressionResult::Gzip {
            body: output.bytes,
            permit: output_permit,
        })
        .unwrap_or(CompressionResult::Identity {
            body,
            permit: identity_permit,
        });
    if let Some(observability) = &observability {
        match result {
            CompressionResult::Gzip { .. } => observability.compression_success(),
            CompressionResult::Identity { .. } => observability.compression_failure(),
        }
    }
    let _ = complete.send(result);
}

struct BoundedBytes {
    bytes: Vec<u8>,
    maximum: usize,
}

impl BoundedBytes {
    fn new(maximum: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(maximum),
            maximum,
        }
    }
}

impl Write for BoundedBytes {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let next = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .filter(|next| *next <= self.maximum)
            .ok_or_else(|| std::io::Error::other("history gzip output exceeds reserved bound"))?;
        debug_assert!(next <= self.maximum);
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn gzip_output_upper_bound(input: usize) -> Option<usize> {
    input.checked_add(GZIP_OUTPUT_OVERHEAD)
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
    high_water: AtomicUsize,
    observability: Option<Arc<HistoryObservability>>,
}

impl ByteBudget {
    #[cfg(test)]
    const fn new(limit: usize) -> Self {
        Self {
            limit,
            used: AtomicUsize::new(0),
            high_water: AtomicUsize::new(0),
            observability: None,
        }
    }

    fn monitored(limit: usize, observability: Arc<HistoryObservability>) -> Self {
        Self {
            limit,
            used: AtomicUsize::new(0),
            high_water: AtomicUsize::new(0),
            observability: Some(observability),
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
                    self.high_water.fetch_max(next, Ordering::AcqRel);
                    self.publish();
                    return Some(BytePermit {
                        budget: self.clone(),
                        bytes,
                    });
                }
                Err(current) => used = current,
            }
        }
    }

    fn publish(&self) {
        if let Some(observability) = &self.observability {
            observability.note_buffers(
                self.used.load(Ordering::Acquire),
                self.limit,
                self.high_water.load(Ordering::Acquire),
            );
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
        self.budget.publish();
    }
}

#[cfg(test)]
mod tests {
    use tqsdk_data::BacktestHistoryField;

    use super::super::observability::{HistoryObservability, MemoryAuditSink};
    use super::*;

    #[test]
    fn future_start_check_allows_clock_skew_but_rejects_later_ranges() {
        let server_time_ns = 1_000_000_000_000;

        assert!(!request_starts_in_future(
            server_time_ns + FUTURE_START_TOLERANCE_NS,
            server_time_ns,
        ));
        assert!(request_starts_in_future(
            server_time_ns + FUTURE_START_TOLERANCE_NS + 1,
            server_time_ns,
        ));
        assert!(!request_starts_in_future(
            server_time_ns - 1,
            server_time_ns,
        ));
    }

    #[tokio::test]
    async fn malformed_request_is_audited_once() {
        let sink = Arc::new(MemoryAuditSink::default());
        let observability = Arc::new(HistoryObservability::with_audit(false, sink.clone()));
        let state = Arc::new(HistoryState::new(
            Arc::new(SnapshotSlot::new(std::env::temp_dir())),
            None,
            observability,
        ));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            serve_stream(stream, "x-history-identity", state)
                .await
                .unwrap();
        });
        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(b"not HTTP\r\n\r\n").await.unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        server.await.unwrap();
        assert!(
            std::str::from_utf8(&response)
                .unwrap()
                .starts_with("HTTP/1.1 400")
        );
        assert_eq!(sink.len(), 1);
        let records = sink.records();
        assert_eq!(records[0].endpoint, "invalid");
        assert_eq!(records[0].status, 400);
        assert_eq!(records[0].error_code, Some("invalid_request"));
        assert!(records[0].selected_representation_bytes > 0);
    }

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

    #[test]
    fn gzip_requires_explicit_positive_accept_encoding() {
        assert!(accepts_gzip(&[("Accept-Encoding", "br, gzip")]));
        assert!(accepts_gzip(&[("Accept-Encoding", "gzip; q=0.1")]));
        assert!(accepts_gzip(&[("Accept-Encoding", "gzip; q=1.000")]));
        assert!(accepts_gzip(&[("Accept-Encoding", "gzip; q=0.001")]));
        assert!(!accepts_gzip(&[("Accept-Encoding", "gzip; q=0")]));
        assert!(!accepts_gzip(&[("Accept-Encoding", "gzip; q=0.0")]));
        assert!(!accepts_gzip(&[("Accept-Encoding", "gzip; q=2")]));
        assert!(!accepts_gzip(&[("Accept-Encoding", "gzip; q=1.001")]));
        assert!(!accepts_gzip(&[("Accept-Encoding", "gzip; q=1e0")]));
        assert!(!accepts_gzip(&[("Accept-Encoding", "gzip; q=0.0001")]));
        assert!(!accepts_gzip(&[
            ("Accept-Encoding", "gzip; q=1"),
            ("Accept-Encoding", "gzip; q=0"),
        ]));
        assert!(!accepts_gzip(&[("Accept-Encoding", "br, *")]));
        assert!(!accepts_gzip(&[]));
    }

    #[test]
    fn saturated_compression_pool_immediately_returns_identity_and_keeps_its_permit() {
        let pool = CompressionPool::saturated_for_test();
        let budget = Arc::new(ByteBudget::new(2 * 1024 * 1024));
        let mut receivers = Vec::new();
        for _ in 0..GZIP_WORKERS * GZIP_QUEUE_PER_WORKER {
            let body = vec![7; 32];
            let permit = budget.try_reserve(body.len()).unwrap();
            match pool.try_compress(body, permit, None, &budget) {
                Ok(receiver) => receivers.push(receiver),
                Err(_) => panic!("empty compression queue must accept the job"),
            }
        }
        let body = vec![9; 32];
        let permit = budget.try_reserve(body.len()).unwrap();
        let (identity, identity_permit) = match pool.try_compress(body, permit, None, &budget) {
            Ok(_) => panic!("full compression queues must not wait or enqueue"),
            Err(identity) => identity,
        };
        assert_eq!(identity, vec![9; 32]);
        assert!(budget.used.load(Ordering::Acquire) >= identity_permit.bytes);
        drop(identity_permit);
        drop(receivers);
        drop(pool);
        assert_eq!(budget.used.load(Ordering::Acquire), 0);
    }

    #[test]
    fn compression_shutdown_releases_queued_guards_and_rejects_new_jobs() {
        let pool = CompressionPool::saturated_for_test();
        let budget = Arc::new(ByteBudget::new(2 * 1024 * 1024));
        let active = Arc::new(Semaphore::new(1));
        let active_pin = Arc::new(ActiveRequestPin {
            _permit: active.clone().try_acquire_owned().unwrap(),
            _gauge: None,
        });
        let body = vec![5; GZIP_MIN_BYTES];
        let permit = budget.try_reserve(body.len()).unwrap();
        let mut receiver = match pool.try_compress(body, permit, Some(active_pin), &budget) {
            Ok(receiver) => receiver,
            Err(_) => panic!("empty compression queue must accept the job"),
        };

        assert!(active.clone().try_acquire_owned().is_err());
        assert!(budget.used.load(Ordering::Acquire) > 0);
        pool.shutdown().unwrap();

        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Closed)
        ));
        assert!(active.clone().try_acquire_owned().is_ok());
        assert_eq!(budget.used.load(Ordering::Acquire), 0);

        let body = vec![7; GZIP_MIN_BYTES];
        let permit = budget.try_reserve(body.len()).unwrap();
        let (identity, identity_permit) = match pool.try_compress(body, permit, None, &budget) {
            Err(identity) => identity,
            Ok(_) => panic!("stopped compression pool must return identity immediately"),
        };
        assert_eq!(identity, vec![7; GZIP_MIN_BYTES]);
        assert_eq!(budget.used.load(Ordering::Acquire), identity_permit.bytes);
        drop(identity_permit);
        assert_eq!(budget.used.load(Ordering::Acquire), 0);
    }

    #[test]
    fn dropped_compression_receiver_keeps_guards_until_worker_finishes() {
        let budget = Arc::new(ByteBudget::new(2 * 1024 * 1024));
        let body = vec![3; GZIP_MIN_BYTES];
        let identity_permit = budget.try_reserve(body.len()).unwrap();
        let output_permit = budget
            .try_reserve(gzip_output_upper_bound(body.len()).unwrap())
            .unwrap();
        let active = Arc::new(Semaphore::new(1));
        let active_pin = Arc::new(ActiveRequestPin {
            _permit: active.clone().try_acquire_owned().unwrap(),
            _gauge: None,
        });
        let (complete, receiver) = oneshot::channel();
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let job = CompressionJob {
            body,
            identity_permit,
            _active_pin: Some(active_pin),
            output_permit,
            observability: None,
            queued_gauge: None,
            complete,
            before_compress: Some((started_tx, release_rx)),
        };
        let worker = thread::spawn(move || compress_job(job));
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        drop(receiver);
        assert!(active.clone().try_acquire_owned().is_err());
        assert!(budget.used.load(Ordering::Acquire) > 0);
        release_tx.send(()).unwrap();
        worker.join().unwrap();
        assert!(active.clone().try_acquire_owned().is_ok());
        assert_eq!(budget.used.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn http_writer_selects_distinct_gzip_representation_etag() {
        let pool = CompressionPool::spawn_for_test().unwrap();
        let state = HistoryState::new(
            Arc::new(SnapshotSlot::new(std::env::temp_dir())),
            Some(pool.clone()),
            Arc::new(HistoryObservability::enabled(true)),
        );
        let plain = write_test_response(
            &state,
            GzipNegotiation {
                accepts: false,
                vary: true,
            },
            None,
            "x".repeat(GZIP_MIN_BYTES),
        )
        .await;
        let plain_etag = test_header(&plain, "etag").unwrap();
        assert_eq!(
            test_header(&plain, "vary"),
            Some("Accept-Encoding".to_string())
        );
        let gzip = write_test_response(
            &state,
            GzipNegotiation {
                accepts: true,
                vary: true,
            },
            None,
            "x".repeat(GZIP_MIN_BYTES),
        )
        .await;
        assert_eq!(
            test_header(&gzip, "content-encoding"),
            Some("gzip".to_string())
        );
        assert_eq!(
            test_header(&gzip, "vary"),
            Some("Accept-Encoding".to_string())
        );
        let gzip_etag = test_header(&gzip, "etag").unwrap();
        assert_ne!(plain_etag, gzip_etag);
        let compressed = test_body(&gzip);
        let mut decoder = flate2::read::GzDecoder::new(compressed);
        let mut decoded = String::new();
        std::io::Read::read_to_string(&mut decoder, &mut decoded).unwrap();
        assert_eq!(
            decoded,
            serde_json::to_string(&Value::String("x".repeat(GZIP_MIN_BYTES))).unwrap()
        );
        let not_modified = write_test_response(
            &state,
            GzipNegotiation {
                accepts: true,
                vary: true,
            },
            Some(&gzip_etag),
            "x".repeat(GZIP_MIN_BYTES),
        )
        .await;
        assert!(test_head(&not_modified).starts_with("HTTP/1.1 304 Not Modified\r\n"));
        assert_eq!(
            test_header(&not_modified, "content-encoding"),
            Some("gzip".to_string())
        );
        assert_eq!(
            test_header(&not_modified, "access-control-allow-origin"),
            Some("*".to_string())
        );
        assert_eq!(
            test_header(&not_modified, "access-control-allow-credentials"),
            None
        );
        assert!(test_body(&not_modified).is_empty());
        pool.shutdown().unwrap();
    }

    async fn write_test_response(
        state: &HistoryState,
        gzip: GzipNegotiation,
        if_none_match: Option<&str>,
        text: String,
    ) -> Vec<u8> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let reader = tokio::spawn(async move {
            let mut stream = TcpStream::connect(address).await.unwrap();
            let mut output = Vec::new();
            stream.read_to_end(&mut output).await.unwrap();
            output
        });
        let (mut stream, _) = listener.accept().await.unwrap();
        let prepared = prepare_response(
            Response::success(Value::String(text), "test".to_string()),
            if_none_match,
            gzip,
            state,
        )
        .await
        .unwrap();
        write_prepared_response(&mut stream, prepared)
            .await
            .unwrap();
        reader.await.unwrap()
    }

    fn test_header(response: &[u8], expected: &str) -> Option<String> {
        test_head(response).split("\r\n").skip(1).find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case(expected)
                .then(|| value.trim().to_string())
        })
    }

    fn test_head(response: &[u8]) -> &str {
        let marker = response
            .windows(4)
            .position(|bytes| bytes == b"\r\n\r\n")
            .unwrap();
        std::str::from_utf8(&response[..marker + 4]).unwrap()
    }

    fn test_body(response: &[u8]) -> &[u8] {
        let marker = response
            .windows(4)
            .position(|bytes| bytes == b"\r\n\r\n")
            .unwrap();
        &response[marker + 4..]
    }
}

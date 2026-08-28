use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use chrono::DateTime;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::time::timeout;
use tqsdk_data::{
    BacktestHistorySchemaSeries, BacktestHistoryValueKind, backtest_history_default_fields,
    backtest_history_resolve_fields, backtest_history_schema_fields,
};
use tqsdk_relay::{RelayError, RelayResult};

const MAX_REQUEST_BYTES: usize = 16 * 1024;
const MAX_IDENTITY_BYTES: usize = 512;
const READ_TIMEOUT: Duration = Duration::from_secs(2);
const SCHEMA_PATH: &str = "/v1/history/schema";
const QUERY_PATH: &str = "/v1/history/query";
const COVERAGE_PATH: &str = "/v1/history/coverage";
const SECOND_NS: u64 = 1_000_000_000;
const MINUTE_NS: u64 = 60 * SECOND_NS;
const DAY_NS: u64 = 24 * 60 * MINUTE_NS;
static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

pub(super) async fn serve_until(
    listener: TcpListener,
    identity_header: String,
    mut shutdown: oneshot::Receiver<()>,
) -> RelayResult<()> {
    loop {
        tokio::select! {
            _ = &mut shutdown => return Ok(()),
            accepted = listener.accept() => {
                let (stream, _) = accepted.map_err(|error| {
                    RelayError::Transport(format!("history accept failed: {error}"))
                })?;
                let identity_header = identity_header.clone();
                tokio::spawn(async move {
                    if let Err(error) = serve_stream(stream, &identity_header).await {
                        eprintln!("history HTTP connection failed: {error}");
                    }
                });
            }
        }
    }
}

async fn serve_stream(mut stream: TcpStream, identity_header: &str) -> RelayResult<()> {
    let request = match timeout(READ_TIMEOUT, read_request(&mut stream)).await {
        Ok(Ok(request)) => request,
        Ok(Err(error)) => {
            write_error(&mut stream, 400, "invalid_request", &error.to_string()).await?;
            return Ok(());
        }
        Err(_) => {
            write_error(
                &mut stream,
                400,
                "invalid_request",
                "request read timed out",
            )
            .await?;
            return Ok(());
        }
    };

    let response = route_request(&request, identity_header);
    write_json(&mut stream, response.status, &response.body).await
}

fn route_request(request: &str, identity_header: &str) -> Response {
    let Ok(parsed) = parse_request(request) else {
        return error_response(400, "invalid_request", "malformed HTTP request");
    };
    if !has_exactly_one_identity(&parsed.headers, identity_header) {
        return error_response(
            400,
            "missing_identity",
            "trusted identity header is required exactly once",
        );
    }
    if !is_known_path(parsed.path) {
        return error_response(404, "route_not_found", "unknown history endpoint");
    }
    if parsed.method != "GET" || parsed.path == SCHEMA_PATH && parsed.query.is_some() {
        return error_response(
            400,
            "invalid_request",
            "history endpoint only accepts GET without unsupported parameters",
        );
    }
    if parsed.path != SCHEMA_PATH {
        if validate_data_request(parsed.path, parsed.query).is_err() {
            return error_response(400, "invalid_request", "invalid history query parameters");
        }
        return error_response(
            503,
            "history_unavailable",
            "history query service is not ready",
        );
    }
    Response::new(200, schema_response())
}

fn schema_response() -> Value {
    json!({
        "wire_version": "tqsdk-history-http/1",
        "series": [
            schema_series("tick", BacktestHistorySchemaSeries::Tick),
            schema_series("kline", BacktestHistorySchemaSeries::Kline),
        ],
        "period_classes": [
            { "name": "sub_minute", "description": "supported existing sub-minute periods below 60 seconds" },
            { "name": "intraday_minutes", "description": "integer minute periods from 1m" },
            { "name": "trading_days", "description": "integer day periods from 1d through 28d" },
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
    json!({ "name": name, "fields": fields, "default_fields": defaults })
}

const fn value_kind_name(value_kind: BacktestHistoryValueKind) -> &'static str {
    match value_kind {
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
    let request_line = lines.next().ok_or(())?;
    let mut parts = request_line.split_ascii_whitespace();
    let method = parts.next().ok_or(())?;
    let target = parts.next().ok_or(())?;
    if parts.next().is_none() || parts.next().is_some() {
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

fn has_exactly_one_identity(headers: &[(&str, &str)], expected_name: &str) -> bool {
    let values = headers
        .iter()
        .filter_map(|(name, value)| name.eq_ignore_ascii_case(expected_name).then_some(*value))
        .collect::<Vec<_>>();
    values.len() == 1
        && !values[0].is_empty()
        && values[0].len() <= MAX_IDENTITY_BYTES
        && !values[0]
            .chars()
            .any(|value| value.is_control() && value != '\t')
}

fn validate_data_request(path: &str, query: Option<&str>) -> Result<(), ()> {
    let query = query.filter(|value| !value.is_empty()).ok_or(())?;
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
        return Err(());
    }
    for required in ["symbol", "series", "start", "end"] {
        if !parameters.contains_key(required) {
            return Err(());
        }
    }
    if !is_history_symbol(parameters["symbol"].as_str()) {
        return Err(());
    }

    let series = match parameters.get("series").map(String::as_str) {
        Some("tick") => BacktestHistorySchemaSeries::Tick,
        Some("kline") => BacktestHistorySchemaSeries::Kline,
        _ => return Err(()),
    };
    match (series, parameters.contains_key("period")) {
        (BacktestHistorySchemaSeries::Tick, false) | (BacktestHistorySchemaSeries::Kline, true) => {
        }
        _ => return Err(()),
    }
    if let Some(period) = parameters.get("period") {
        parse_legal_period_ns(period.as_str())?;
    }

    let start = DateTime::parse_from_rfc3339(parameters["start"].as_str()).map_err(|_| ())?;
    let end = DateTime::parse_from_rfc3339(parameters["end"].as_str()).map_err(|_| ())?;
    if end <= start {
        return Err(());
    }
    if let Some(fields) = parameters.get("fields") {
        backtest_history_resolve_fields(series, fields.split(',')).map_err(|_| ())?;
    }
    if parameters
        .get("include")
        .is_some_and(|value| value != "provenance")
    {
        return Err(());
    }
    Ok(())
}

fn is_history_symbol(symbol: &str) -> bool {
    if let Some(underlying) = symbol
        .strip_prefix("KQ.i@")
        .or_else(|| symbol.strip_prefix("KQ.m@"))
    {
        return is_qualified_symbol(underlying);
    }
    !symbol.contains('@') && is_qualified_symbol(symbol)
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

fn parse_legal_period_ns(period: &str) -> Result<u64, ()> {
    let units = [
        ("ns", 1_u64),
        ("us", 1_000_u64),
        ("ms", 1_000_000_u64),
        ("s", SECOND_NS),
        ("m", MINUTE_NS),
        ("h", 60 * MINUTE_NS),
        ("d", DAY_NS),
    ];
    let duration_ns = units
        .iter()
        .find_map(|(suffix, multiplier)| {
            period
                .strip_suffix(suffix)
                .filter(|number| !number.is_empty())
                .and_then(|number| number.parse::<u64>().ok())
                .and_then(|number| number.checked_mul(*multiplier))
        })
        .filter(|value| *value > 0)
        .ok_or(())?;
    let legal = duration_ns < MINUTE_NS
        || duration_ns < DAY_NS && duration_ns % MINUTE_NS == 0
        || (DAY_NS..=28 * DAY_NS).contains(&duration_ns) && duration_ns % DAY_NS == 0;
    legal.then_some(duration_ns).ok_or(())
}

fn parse_query_parameters(query: &str) -> Result<BTreeMap<String, String>, ()> {
    let mut parameters = BTreeMap::new();
    for pair in query.split('&') {
        let (key, value) = pair.split_once('=').ok_or(())?;
        let key = decode_query_component(key)?;
        let value = decode_query_component(value)?;
        if key.is_empty() || value.is_empty() || parameters.insert(key, value).is_some() {
            return Err(());
        }
    }
    Ok(parameters)
}

fn decode_query_component(value: &str) -> Result<String, ()> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let high = *bytes.get(index + 1).ok_or(())?;
            let low = *bytes.get(index + 2).ok_or(())?;
            decoded.push(hex_value(high)?.checked_mul(16).ok_or(())? + hex_value(low)?);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).map_err(|_| ())
}

const fn hex_value(value: u8) -> Result<u8, ()> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(()),
    }
}

fn is_known_path(path: &str) -> bool {
    path == SCHEMA_PATH || path == QUERY_PATH || path == COVERAGE_PATH
}

struct Response {
    status: u16,
    body: Value,
}

impl Response {
    const fn new(status: u16, body: Value) -> Self {
        Self { status, body }
    }
}

fn error_response(status: u16, code: &'static str, message: &'static str) -> Response {
    Response::new(
        status,
        json!({
            "error": {
                "code": code,
                "message": message,
                "request_id": next_request_id(),
                "details": {},
            }
        }),
    )
}

fn next_request_id() -> String {
    format!("r-{}", NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed))
}

async fn write_error(
    stream: &mut TcpStream,
    status: u16,
    code: &'static str,
    message: &str,
) -> RelayResult<()> {
    write_json(
        stream,
        status,
        &json!({
            "error": {
                "code": code,
                "message": message,
                "request_id": next_request_id(),
                "details": {},
            }
        }),
    )
    .await
}

async fn write_json(stream: &mut TcpStream, status: u16, body: &Value) -> RelayResult<()> {
    let body = serde_json::to_vec(body)
        .map_err(|error| RelayError::Internal(format!("history JSON encode failed: {error}")))?;
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        503 => "Service Unavailable",
        _ => "Internal Server Error",
    };
    let headers = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(headers.as_bytes())
        .await
        .map_err(|error| {
            RelayError::Transport(format!("history response header write failed: {error}"))
        })?;
    stream.write_all(&body).await.map_err(|error| {
        RelayError::Transport(format!("history response body write failed: {error}"))
    })?;
    stream.shutdown().await.map_err(|error| {
        RelayError::Transport(format!("history response shutdown failed: {error}"))
    })
}

#[cfg(test)]
mod tests {
    use super::{route_request, schema_response};

    const IDENTITY: &str = "X-Trusted-Identity";

    fn request(method: &str, target: &str, headers: &str) -> String {
        format!("{method} {target} HTTP/1.1\r\nHost: relay\r\n{headers}\r\n")
    }

    #[test]
    fn schema_is_typed_and_in_canonical_order() {
        let schema = schema_response();
        assert_eq!(schema["wire_version"], "tqsdk-history-http/1");
        assert_eq!(schema["series"][0]["name"], "tick");
        assert_eq!(schema["series"][0]["fields"][0]["canonical_name"], "t");
        assert_eq!(schema["series"][1]["name"], "kline");
        assert_eq!(schema["series"][1]["fields"][0]["canonical_name"], "t");
        assert_eq!(schema["series"][1]["default_fields"][0], "t");
    }

    #[test]
    fn schema_requires_one_identity_and_rejects_query_parameters() {
        let missing = route_request(&request("GET", "/v1/history/schema", ""), IDENTITY);
        assert_eq!(missing.status, 400);
        assert_eq!(missing.body["error"]["code"], "missing_identity");
        assert!(
            missing.body["error"]["request_id"]
                .as_str()
                .unwrap()
                .starts_with("r-")
        );
        assert_eq!(missing.body["error"]["details"], serde_json::json!({}));
        let duplicate = route_request(
            &request(
                "GET",
                "/v1/history/schema",
                "X-Trusted-Identity: a\r\nX-Trusted-Identity: b\r\n",
            ),
            IDENTITY,
        );
        assert_eq!(duplicate.status, 400);
        let query = route_request(
            &request("GET", "/v1/history/schema?x=1", "X-Trusted-Identity: a\r\n"),
            IDENTITY,
        );
        assert_eq!(query.status, 400);

        let unicode = route_request(
            &request(
                "GET",
                "/v1/history/schema",
                "X-Trusted-Identity: 研究-client\r\n",
            ),
            IDENTITY,
        );
        assert_eq!(unicode.status, 200);
        let control = route_request(
            &request(
                "GET",
                "/v1/history/schema",
                "X-Trusted-Identity: bad\0client\r\n",
            ),
            IDENTITY,
        );
        assert_eq!(control.status, 400);
    }

    #[test]
    fn methods_and_unknown_paths_follow_the_frozen_minimum_contract() {
        let headers = "X-Trusted-Identity: a\r\n";
        assert_eq!(
            route_request(&request("OPTIONS", "/v1/history/schema", headers), IDENTITY).status,
            400
        );
        assert_eq!(
            route_request(&request("POST", "/v1/history/query", headers), IDENTITY).status,
            400
        );
        assert_eq!(
            route_request(&request("GET", "/v1/history/missing", headers), IDENTITY).body["error"]
                ["code"],
            "route_not_found"
        );
        for target in [
            "/v1/history/query",
            "/v1/history/query?symbol=SHFE.au2612&series=tick&start=&end=x",
            "/v1/history/query?symbol=SHFE.au2612&symbol=DCE.m2609&series=tick&start=x&end=y",
            "/v1/history/coverage?symbol=SHFE.au2612&series=tick&start=x&end=y&unknown=1",
            "/v1/history/query?symbol=garbage&series=tick&start=2026-08-01T09%3A00%3A00%2B08%3A00&end=2026-08-01T10%3A00%3A00%2B08%3A00",
            "/v1/history/query?symbol=KQ.m%40SHFE.au&series=kline&period=garbage&start=2026-08-01T09%3A00%3A00%2B08%3A00&end=2026-08-01T10%3A00%3A00%2B08%3A00",
        ] {
            assert_eq!(
                route_request(&request("GET", target, headers), IDENTITY).status,
                400,
                "{target}"
            );
        }
        let unavailable = route_request(
            &request(
                "GET",
                "/v1/history/query?symbol=SHFE.au2612&series=tick&start=2026-08-01T09%3A00%3A00%2B08%3A00&end=2026-08-01T10%3A00%3A00%2B08%3A00",
                headers,
            ),
            IDENTITY,
        );
        assert_eq!(unavailable.status, 503);
        assert_eq!(unavailable.body["error"]["code"], "history_unavailable");
        for period in ["500ms", "1m", "28d"] {
            let target = format!(
                "/v1/history/coverage?symbol=KQ.i%40DCE.m&series=kline&period={period}&start=2026-08-01T09%3A00%3A00%2B08%3A00&end=2026-08-01T10%3A00%3A00%2B08%3A00"
            );
            assert_eq!(
                route_request(&request("GET", target.as_str(), headers), IDENTITY).status,
                503,
                "{period}"
            );
        }
    }
}

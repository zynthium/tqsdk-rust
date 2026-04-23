#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use chrono::{Datelike, Days, FixedOffset, NaiveDate, Utc};
use serde_json::Value;
use tqsdk_core::{Chart, Kline, MarketChartCommand, MarketCommand, RuntimeCommand, Symbol};

use crate::error::{DataError, Result};

const DEFAULT_HOLIDAY_URL: &str = "https://files.shinnytech.com/shinny_chinese_holiday.json";
const DEFAULT_CONTINUOUS_TABLE_URL: &str = "https://files.shinnytech.com/continuous_table.json";
const DEFAULT_KLINE_DATA_TIMEOUT: Duration = Duration::from_secs(30);

static NEXT_KLINE_SERIES_CHART_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
struct DataServiceEndpoints {
    holiday_url: String,
    continuous_table_url: String,
}

impl Default for DataServiceEndpoints {
    fn default() -> Self {
        Self {
            holiday_url: DEFAULT_HOLIDAY_URL.to_string(),
            continuous_table_url: DEFAULT_CONTINUOUS_TABLE_URL.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ContUnderlyingUpdate {
    date: NaiveDate,
    underlying: String,
}

/// A single historical continuous-contract row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoricalContQuotesRow {
    pub date: String,
    pub underlyings: BTreeMap<String, String>,
}

/// Request for a one-shot owned kline history window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KlineDataSeriesRequest {
    symbol: String,
    duration: Duration,
    data_length: usize,
    left_kline_id: Option<i64>,
    timeout: Duration,
}

impl KlineDataSeriesRequest {
    #[must_use]
    pub fn new(symbol: impl Into<String>, duration: Duration, data_length: usize) -> Self {
        Self {
            symbol: symbol.into(),
            duration,
            data_length,
            left_kline_id: None,
            timeout: DEFAULT_KLINE_DATA_TIMEOUT,
        }
    }

    #[must_use]
    pub fn with_left_kline_id(mut self, left_kline_id: i64) -> Self {
        self.left_kline_id = Some(left_kline_id);
        self
    }

    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    #[must_use]
    pub fn duration(&self) -> Duration {
        self.duration
    }

    #[must_use]
    pub fn data_length(&self) -> usize {
        self.data_length
    }

    #[must_use]
    pub fn left_kline_id(&self) -> Option<i64> {
        self.left_kline_id
    }

    #[must_use]
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    fn validate(&self) -> Result<i64> {
        if self.symbol.is_empty() {
            return Err(DataError::Validation(
                "symbol must not be empty".to_string(),
            ));
        }
        if self.data_length == 0 {
            return Err(DataError::Validation(
                "data_length must be greater than zero".to_string(),
            ));
        }
        let duration_ns = i64::try_from(self.duration.as_nanos()).map_err(|_| {
            DataError::Validation("duration is too large to encode as i64 nanoseconds".to_string())
        })?;
        if duration_ns <= 0 {
            return Err(DataError::Validation(
                "duration must be greater than zero".to_string(),
            ));
        }
        Ok(duration_ns)
    }
}

/// Owned result of a one-shot kline history request.
#[derive(Debug, Clone, Default)]
pub struct KlineDataSeries {
    symbol: String,
    duration_ns: i64,
    requested_length: usize,
    left_kline_id: Option<i64>,
    rows: Vec<Kline>,
}

impl KlineDataSeries {
    fn new(
        symbol: String,
        duration_ns: i64,
        requested_length: usize,
        left_kline_id: Option<i64>,
        rows: Vec<Kline>,
    ) -> Self {
        Self {
            symbol,
            duration_ns,
            requested_length,
            left_kline_id,
            rows,
        }
    }

    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    #[must_use]
    pub fn duration_ns(&self) -> i64 {
        self.duration_ns
    }

    #[must_use]
    pub fn requested_length(&self) -> usize {
        self.requested_length
    }

    #[must_use]
    pub fn left_kline_id(&self) -> Option<i64> {
        self.left_kline_id
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    #[must_use]
    pub fn last(&self) -> Option<&Kline> {
        self.rows.last()
    }

    #[must_use]
    pub fn get(&self, index: usize) -> Option<&Kline> {
        self.rows.get(index)
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = &Kline> + DoubleEndedIterator {
        self.rows.iter()
    }

    #[must_use]
    pub fn rows(&self) -> &[Kline] {
        &self.rows
    }

    #[must_use]
    pub fn into_rows(self) -> Vec<Kline> {
        self.rows
    }
}

/// Thin research/offline data wrapper over [`tqsdk_session::SessionClient`].
pub struct DataClient {
    session: Option<tqsdk_session::SessionClient>,
    http: reqwest::Client,
    endpoints: DataServiceEndpoints,
}

impl Default for DataClient {
    fn default() -> Self {
        Self::new()
    }
}

impl DataClient {
    #[must_use]
    pub fn new() -> Self {
        Self {
            session: None,
            http: reqwest::Client::new(),
            endpoints: DataServiceEndpoints::default(),
        }
    }

    #[must_use]
    pub fn from_session(session: tqsdk_session::SessionClient) -> Self {
        Self::new().with_session(session)
    }

    #[must_use]
    pub fn with_session(mut self, session: tqsdk_session::SessionClient) -> Self {
        self.session = Some(session);
        self
    }

    #[doc(hidden)]
    #[must_use]
    pub fn new_for_test_with_urls(
        holiday_url: impl Into<String>,
        continuous_table_url: impl Into<String>,
    ) -> Self {
        Self {
            session: None,
            http: reqwest::Client::new(),
            endpoints: DataServiceEndpoints {
                holiday_url: holiday_url.into(),
                continuous_table_url: continuous_table_url.into(),
            },
        }
    }

    #[must_use]
    pub fn session(&self) -> Option<&tqsdk_session::SessionClient> {
        self.session.as_ref()
    }

    #[must_use]
    pub fn into_session(self) -> Option<tqsdk_session::SessionClient> {
        self.session
    }

    pub async fn query_his_cont_quotes(
        &self,
        symbols: &[&str],
        days: usize,
        end_date: Option<NaiveDate>,
    ) -> Result<Vec<HistoricalContQuotesRow>> {
        validate_symbols(symbols)?;
        if days == 0 {
            return Err(DataError::Validation(
                "days must be greater than zero".to_string(),
            ));
        }

        let end_date = end_date.unwrap_or_else(current_cst_date);
        let lookback_days = days
            .checked_mul(2)
            .and_then(|value| value.checked_add(30))
            .ok_or_else(|| {
                DataError::Validation("days overflow when computing lookback".to_string())
            })?;
        let start_date = end_date
            .checked_sub_days(Days::new(lookback_days as u64))
            .ok_or_else(|| {
                DataError::Validation("failed to compute history start date".to_string())
            })?;

        let trading_days = self.trading_days(start_date, end_date).await?;
        let updates = self.fetch_continuous_updates(symbols).await?;

        let mut indices: BTreeMap<String, usize> = symbols
            .iter()
            .map(|symbol| ((*symbol).to_string(), 0_usize))
            .collect();
        let mut current: BTreeMap<String, String> = symbols
            .iter()
            .map(|symbol| ((*symbol).to_string(), String::new()))
            .collect();
        let mut rows = Vec::with_capacity(trading_days.len());

        for trading_day in trading_days {
            let mut underlyings = BTreeMap::new();
            for symbol in symbols {
                let updates_for_symbol = updates.get(*symbol).ok_or_else(|| {
                    DataError::InvalidResponse(format!("missing continuous updates for {symbol}"))
                })?;
                let index = indices.get_mut(*symbol).expect("symbol index missing");
                while *index < updates_for_symbol.len()
                    && updates_for_symbol[*index].date <= trading_day
                {
                    current.insert(
                        (*symbol).to_string(),
                        updates_for_symbol[*index].underlying.clone(),
                    );
                    *index += 1;
                }
                underlyings.insert(
                    (*symbol).to_string(),
                    current.get(*symbol).cloned().unwrap_or_default(),
                );
            }

            rows.push(HistoricalContQuotesRow {
                date: trading_day.format("%Y-%m-%d").to_string(),
                underlyings,
            });
        }

        if rows.len() > days {
            rows.drain(0..rows.len() - days);
        }

        Ok(rows)
    }

    pub async fn get_kline_data_series(
        &self,
        request: KlineDataSeriesRequest,
    ) -> Result<KlineDataSeries> {
        let duration_ns = request.validate()?;
        let Some(session) = self.session.as_ref() else {
            return Err(DataError::InvalidState(
                "get_kline_data_series requires a session-backed data client",
            ));
        };

        let chart_id = next_kline_series_chart_id(request.symbol(), duration_ns);
        let result = self
            .await_kline_data_series(session, &request, duration_ns, chart_id.as_str())
            .await;

        let _ = session
            .submit(RuntimeCommand::Market(MarketCommand::CancelChart {
                chart_id,
            }))
            .await;

        result
    }

    async fn await_kline_data_series(
        &self,
        session: &tqsdk_session::SessionClient,
        request: &KlineDataSeriesRequest,
        duration_ns: i64,
        chart_id: &str,
    ) -> Result<KlineDataSeries> {
        let command_id = session
            .submit(RuntimeCommand::Market(MarketCommand::SetChart(
                MarketChartCommand {
                    chart_id: chart_id.to_string(),
                    symbols: vec![Symbol::new(request.symbol())],
                    duration_ns,
                    view_width: request.data_length(),
                    left_kline_id: request.left_kline_id(),
                    focus_datetime_ns: None,
                    focus_position: None,
                },
            )))
            .await?;
        let reader = session.reader_clone();
        let deadline = tokio::time::Instant::now() + request.timeout();

        loop {
            if let Some(series) =
                try_read_ready_kline_data_series(&reader, request, duration_ns, chart_id)?
            {
                return Ok(series);
            }

            if let Some(status) = session.command_status(command_id)?
                && matches!(status.as_str(), "rejected" | "failed" | "cancelled")
            {
                return Err(DataError::InvalidResponse(format!(
                    "set chart command reached terminal status {status}"
                )));
            }

            if tokio::time::Instant::now() >= deadline {
                return Err(DataError::Timeout(request.timeout()));
            }

            let mut progress = false;
            progress |= session.flush_outbound().await?;
            progress |= session.drive_pending_once().await?;
            progress |= session.drive_route_once(Some(deadline)).await?;

            if progress {
                continue;
            }

            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(DataError::Timeout(request.timeout()));
            }

            tokio::time::sleep(remaining.min(Duration::from_millis(1))).await;
        }
    }

    async fn trading_days(
        &self,
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Result<Vec<NaiveDate>> {
        if start_date > end_date {
            return Err(DataError::Validation(
                "start_date must be less than or equal to end_date".to_string(),
            ));
        }

        let payload = self.fetch_json(&self.endpoints.holiday_url).await?;
        let holidays = payload.as_array().ok_or_else(|| {
            DataError::InvalidResponse("holiday payload must be an array".to_string())
        })?;

        let mut holiday_set = HashSet::new();
        let mut years = Vec::with_capacity(holidays.len());
        for holiday in holidays {
            let Some(value) = holiday.as_str() else {
                return Err(DataError::InvalidResponse(
                    "holiday entry must be a string".to_string(),
                ));
            };
            let date = parse_iso_date(value)?;
            holiday_set.insert(date);
            years.push(date.year());
        }

        let (Some(first_year), Some(last_year)) = (years.iter().min(), years.iter().max()) else {
            return Err(DataError::InvalidResponse(
                "holiday payload must not be empty".to_string(),
            ));
        };
        let first_day = NaiveDate::from_ymd_opt(*first_year, 1, 1).ok_or_else(|| {
            DataError::InvalidResponse("failed to build holiday lower bound".to_string())
        })?;
        let last_day = NaiveDate::from_ymd_opt(*last_year, 12, 31).ok_or_else(|| {
            DataError::InvalidResponse("failed to build holiday upper bound".to_string())
        })?;
        if start_date < first_day || end_date > last_day {
            return Err(DataError::Validation(format!(
                "trading calendar supports {} to {}",
                first_day.format("%Y-%m-%d"),
                last_day.format("%Y-%m-%d")
            )));
        }

        let mut days = Vec::new();
        let mut current = start_date;
        while current <= end_date {
            let trading =
                current.weekday().number_from_monday() <= 5 && !holiday_set.contains(&current);
            if trading {
                days.push(current);
            }
            current = current.checked_add_days(Days::new(1)).ok_or_else(|| {
                DataError::Validation("failed to advance trading day".to_string())
            })?;
        }

        Ok(days)
    }

    async fn fetch_continuous_updates(
        &self,
        symbols: &[&str],
    ) -> Result<BTreeMap<String, Vec<ContUnderlyingUpdate>>> {
        let payload = self
            .fetch_json(&self.endpoints.continuous_table_url)
            .await?;
        let object = payload.as_object().ok_or_else(|| {
            DataError::InvalidResponse("continuous table payload must be an object".to_string())
        })?;

        let mut updates = BTreeMap::new();
        for symbol in symbols {
            let normalized = symbol.strip_prefix("KQ.m@").ok_or_else(|| {
                DataError::Validation(format!("symbol {symbol} is not a continuous-contract code"))
            })?;
            let Some(entries) = object.get(normalized).and_then(Value::as_array) else {
                return Err(DataError::Validation(format!(
                    "continuous table does not contain {symbol}"
                )));
            };

            let mut parsed = Vec::with_capacity(entries.len());
            for entry in entries {
                let Some(entry) = entry.as_array() else {
                    return Err(DataError::InvalidResponse(
                        "continuous table entry must be an array".to_string(),
                    ));
                };
                if entry.len() != 2 {
                    return Err(DataError::InvalidResponse(
                        "continuous table entry must contain exactly 2 items".to_string(),
                    ));
                }
                let date = parse_compact_date_value(&entry[0])?;
                let underlying = entry[1].as_str().ok_or_else(|| {
                    DataError::InvalidResponse(
                        "continuous table underlying must be a string".to_string(),
                    )
                })?;
                parsed.push(ContUnderlyingUpdate {
                    date,
                    underlying: underlying.to_string(),
                });
            }
            parsed.sort_by_key(|entry| entry.date);
            updates.insert((*symbol).to_string(), parsed);
        }

        Ok(updates)
    }

    async fn fetch_json(&self, url: &str) -> Result<Value> {
        let response = self.http.get(url).send().await?.error_for_status()?;
        Ok(response.json::<Value>().await?)
    }
}

fn try_read_ready_kline_data_series(
    reader: &tqsdk_core::RuntimeReader,
    request: &KlineDataSeriesRequest,
    duration_ns: i64,
    chart_id: &str,
) -> Result<Option<KlineDataSeries>> {
    let snapshot = reader.read();
    let Some(chart) = snapshot
        .decode_path::<Chart>(&["charts", chart_id])
        .map_err(contract_error_into_data)?
    else {
        return Ok(None);
    };
    if !chart.ready || chart.more_data {
        return Ok(None);
    }

    let duration_key = duration_ns.to_string();
    let data_path = ["klines", request.symbol(), duration_key.as_str(), "data"];
    let mut ids = snapshot
        .get_path(&data_path)
        .and_then(|value| value.as_object())
        .map(|data| {
            data.keys()
                .filter_map(|key| key.parse::<i64>().ok())
                .filter(|id| chart.left_id <= *id && *id <= chart.right_id)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    ids.sort_unstable();
    if ids.len() > request.data_length() {
        ids.drain(0..ids.len() - request.data_length());
    }

    let mut rows = Vec::with_capacity(ids.len());
    for id in ids {
        let id_key = id.to_string();
        if let Some(row) = snapshot
            .decode_path::<Kline>(&[
                "klines",
                request.symbol(),
                duration_key.as_str(),
                "data",
                id_key.as_str(),
            ])
            .map_err(contract_error_into_data)?
        {
            rows.push(row);
        }
    }

    Ok(Some(KlineDataSeries::new(
        request.symbol().to_string(),
        duration_ns,
        request.data_length(),
        request.left_kline_id(),
        rows,
    )))
}

fn next_kline_series_chart_id(symbol: &str, duration_ns: i64) -> String {
    let sequence = NEXT_KLINE_SERIES_CHART_ID.fetch_add(1, Ordering::Relaxed);
    format!(
        "data-kline-{}-{duration_ns}-{sequence}",
        sanitize_chart_token(symbol)
    )
}

fn sanitize_chart_token(raw: &str) -> String {
    raw.chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

fn contract_error_into_data(error: tqsdk_core::ContractError) -> DataError {
    DataError::Session(tqsdk_session::SessionFacadeError::from(error))
}

fn current_cst_date() -> NaiveDate {
    let offset = FixedOffset::east_opt(8 * 60 * 60).expect("CST offset must be valid");
    Utc::now().with_timezone(&offset).date_naive()
}

fn validate_symbols(symbols: &[&str]) -> Result<()> {
    if symbols.is_empty() {
        return Err(DataError::Validation(
            "symbols must not be empty".to_string(),
        ));
    }
    let mut unique = HashSet::new();
    for symbol in symbols {
        if symbol.is_empty() {
            return Err(DataError::Validation(
                "symbols must not contain empty entries".to_string(),
            ));
        }
        if !unique.insert(*symbol) {
            return Err(DataError::Validation(format!(
                "duplicate symbol {symbol} is not supported"
            )));
        }
    }
    Ok(())
}

fn parse_iso_date(value: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|error| {
        DataError::InvalidResponse(format!("failed to parse ISO date {value}: {error}"))
    })
}

fn parse_compact_date_value(value: &Value) -> Result<NaiveDate> {
    match value {
        Value::String(value) => parse_compact_date_str(value),
        Value::Number(value) => parse_compact_date_str(&value.to_string()),
        other => Err(DataError::InvalidResponse(format!(
            "continuous table date must be string or number, got {other}"
        ))),
    }
}

fn parse_compact_date_str(value: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(value, "%Y%m%d").map_err(|error| {
        DataError::InvalidResponse(format!("failed to parse compact date {value}: {error}"))
    })
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    use serde_json::json;
    use tqsdk_core::{
        AdapterRegistry, CommitScope, InputPayload, IoEvent, ProtocolDomain, RuntimeHandle,
        RuntimeInput,
    };
    use tqsdk_session::{SessionClient, SessionFacadeConfig};

    use super::*;

    #[test]
    fn query_his_cont_quotes_returns_last_n_trading_days_with_fill_forward() {
        run_on_tokio(async {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let server = std::thread::spawn(move || {
                let (mut holiday_stream, _) = listener.accept().unwrap();
                let holiday_request = read_http_request(&mut holiday_stream);
                assert!(
                    holiday_request.starts_with("GET /holiday.json HTTP/1.1"),
                    "{holiday_request}"
                );
                write_http_ok(
                    &mut holiday_stream,
                    r#"["2026-05-01","2026-05-02","2026-05-03"]"#,
                );

                let (mut cont_stream, _) = listener.accept().unwrap();
                let cont_request = read_http_request(&mut cont_stream);
                assert!(
                    cont_request.starts_with("GET /continuous_table.json HTTP/1.1"),
                    "{cont_request}"
                );
                write_http_ok(
                    &mut cont_stream,
                    r#"{
                        "DCE.a": [[20260429, "DCE.a2605"], [20260502, "DCE.a2609"]],
                        "DCE.eg": [[20260428, "DCE.eg2605"], [20260503, "DCE.eg2609"]]
                    }"#,
                );
            });

            let client = DataClient::new_for_test_with_urls(
                format!("http://{addr}/holiday.json"),
                format!("http://{addr}/continuous_table.json"),
            );

            let rows = client
                .query_his_cont_quotes(
                    &["KQ.m@DCE.a", "KQ.m@DCE.eg"],
                    3,
                    Some(NaiveDate::from_ymd_opt(2026, 5, 4).unwrap()),
                )
                .await
                .unwrap();

            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0].date, "2026-04-29");
            assert_eq!(rows[0].underlyings["KQ.m@DCE.a"], "DCE.a2605");
            assert_eq!(rows[0].underlyings["KQ.m@DCE.eg"], "DCE.eg2605");
            assert_eq!(rows[1].date, "2026-04-30");
            assert_eq!(rows[1].underlyings["KQ.m@DCE.a"], "DCE.a2605");
            assert_eq!(rows[1].underlyings["KQ.m@DCE.eg"], "DCE.eg2605");
            assert_eq!(rows[2].date, "2026-05-04");
            assert_eq!(rows[2].underlyings["KQ.m@DCE.a"], "DCE.a2609");
            assert_eq!(rows[2].underlyings["KQ.m@DCE.eg"], "DCE.eg2609");

            server.join().unwrap();
        });
    }

    #[test]
    fn await_kline_data_series_returns_ready_rows_within_chart_bounds() {
        run_on_tokio(async {
            let (session, handle) = test_session_and_handle();
            let client = DataClient::from_session(session.clone());
            let request = KlineDataSeriesRequest::new("SHFE.ao2609", Duration::from_secs(60), 2)
                .with_left_kline_id(100)
                .with_timeout(Duration::from_millis(100));
            let duration_ns = request.validate().unwrap();

            let seed_thread = std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(5));
                seed_ready_kline_chart(
                    &handle,
                    "data-kline-test",
                    "SHFE.ao2609",
                    duration_ns,
                    100,
                    101,
                );
            });

            let series = client
                .await_kline_data_series(&session, &request, duration_ns, "data-kline-test")
                .await
                .unwrap();

            assert_eq!(series.symbol(), "SHFE.ao2609");
            assert_eq!(series.duration_ns(), duration_ns);
            assert_eq!(series.requested_length(), 2);
            assert_eq!(series.left_kline_id(), Some(100));
            assert_eq!(series.len(), 2);
            assert_eq!(series.rows()[0].id, 100);
            assert_eq!(series.rows()[1].id, 101);
            assert_eq!(series.last().map(|row| row.close), Some(620.0));

            seed_thread.join().unwrap();
        });
    }

    #[test]
    fn get_kline_data_series_requires_session_backed_client() {
        run_on_tokio(async {
            let err = DataClient::new()
                .get_kline_data_series(KlineDataSeriesRequest::new(
                    "SHFE.ao2609",
                    Duration::from_secs(60),
                    2,
                ))
                .await
                .unwrap_err();

            assert!(matches!(
                err,
                DataError::InvalidState(message)
                    if message
                        == "get_kline_data_series requires a session-backed data client"
            ));
        });
    }

    #[test]
    fn get_kline_data_series_times_out_without_ready_chart() {
        run_on_tokio(async {
            let (session, _handle) = test_session_and_handle();
            let client = DataClient::from_session(session);

            let err = client
                .get_kline_data_series(
                    KlineDataSeriesRequest::new("SHFE.ao2609", Duration::from_secs(60), 2)
                        .with_timeout(Duration::from_millis(10)),
                )
                .await
                .unwrap_err();

            assert!(
                matches!(err, DataError::Timeout(timeout) if timeout == Duration::from_millis(10))
            );
        });
    }

    #[test]
    fn get_kline_data_series_rejects_invalid_requests() {
        run_on_tokio(async {
            let client = DataClient::new();

            let err = client
                .get_kline_data_series(KlineDataSeriesRequest::new("", Duration::from_secs(60), 2))
                .await
                .unwrap_err();
            assert!(
                matches!(err, DataError::Validation(message) if message == "symbol must not be empty")
            );

            let err = client
                .get_kline_data_series(KlineDataSeriesRequest::new(
                    "SHFE.ao2609",
                    Duration::from_secs(60),
                    0,
                ))
                .await
                .unwrap_err();
            assert!(
                matches!(err, DataError::Validation(message) if message == "data_length must be greater than zero")
            );

            let err = client
                .get_kline_data_series(KlineDataSeriesRequest::new(
                    "SHFE.ao2609",
                    Duration::ZERO,
                    2,
                ))
                .await
                .unwrap_err();
            assert!(
                matches!(err, DataError::Validation(message) if message == "duration must be greater than zero")
            );
        });
    }

    #[test]
    fn query_his_cont_quotes_rejects_invalid_inputs() {
        run_on_tokio(async {
            let client = DataClient::new();

            let err = client
                .query_his_cont_quotes(&[], 1, Some(NaiveDate::from_ymd_opt(2026, 5, 4).unwrap()))
                .await
                .unwrap_err();
            assert!(
                matches!(err, DataError::Validation(message) if message == "symbols must not be empty")
            );

            let err = client
                .query_his_cont_quotes(
                    &["KQ.m@DCE.a", "KQ.m@DCE.a"],
                    1,
                    Some(NaiveDate::from_ymd_opt(2026, 5, 4).unwrap()),
                )
                .await
                .unwrap_err();
            assert!(
                matches!(err, DataError::Validation(message) if message.contains("duplicate symbol"))
            );

            let err = client
                .query_his_cont_quotes(
                    &["KQ.m@DCE.a"],
                    0,
                    Some(NaiveDate::from_ymd_opt(2026, 5, 4).unwrap()),
                )
                .await
                .unwrap_err();
            assert!(
                matches!(err, DataError::Validation(message) if message == "days must be greater than zero")
            );
        });
    }

    fn run_on_tokio<F, T>(future: F) -> T
    where
        F: Future<Output = T>,
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(future)
    }

    fn test_session_and_handle() -> (SessionClient, RuntimeHandle) {
        let mut adapters = AdapterRegistry::new();
        adapters.register_default_adapters();

        let handle = RuntimeHandle::with_adapters(adapters);
        let session =
            SessionClient::new_for_test_with_handle(handle.clone(), SessionFacadeConfig::default());

        (session, handle)
    }

    fn seed_ready_kline_chart(
        handle: &RuntimeHandle,
        chart_id: &str,
        symbol: &str,
        duration_ns: i64,
        left_id: i64,
        right_id: i64,
    ) {
        handle
            .ingest(
                RuntimeInput::Io(IoEvent {
                    route: "market".to_string(),
                    domains: vec![ProtocolDomain::Market],
                    payload: InputPayload::Json(json!({
                        "aid": "rtn_data",
                        "data": [{
                            "charts": {
                                chart_id: {
                                    "state": {
                                        "ins_list": symbol,
                                        "duration": duration_ns,
                                    },
                                    "left_id": left_id,
                                    "right_id": right_id,
                                    "more_data": false,
                                    "ready": true,
                                }
                            },
                            "klines": {
                                symbol: {
                                    duration_ns.to_string(): {
                                        "data": {
                                            "99": {
                                                "id": 99,
                                                "datetime": 1_713_659_940_000_000_000_i64,
                                                "open": 617.0,
                                                "high": 618.0,
                                                "low": 616.0,
                                                "close": 617.5,
                                                "volume": 11,
                                                "open_oi": 99,
                                                "close_oi": 100
                                            },
                                            "100": {
                                                "id": 100,
                                                "datetime": 1_713_660_000_000_000_000_i64,
                                                "open": 618.0,
                                                "high": 620.0,
                                                "low": 617.0,
                                                "close": 619.0,
                                                "volume": 12,
                                                "open_oi": 100,
                                                "close_oi": 101
                                            },
                                            "101": {
                                                "id": 101,
                                                "datetime": 1_713_660_060_000_000_000_i64,
                                                "open": 619.0,
                                                "high": 621.0,
                                                "low": 618.0,
                                                "close": 620.0,
                                                "volume": 15,
                                                "open_oi": 101,
                                                "close_oi": 103
                                            },
                                            "102": {
                                                "id": 102,
                                                "datetime": 1_713_660_120_000_000_000_i64,
                                                "open": 620.0,
                                                "high": 622.0,
                                                "low": 619.0,
                                                "close": 621.0,
                                                "volume": 16,
                                                "open_oi": 103,
                                                "close_oi": 104
                                            }
                                        }
                                    }
                                }
                            }
                        }]
                    })),
                }),
                vec![],
                CommitScope::RealtimeUpdate,
            )
            .unwrap()
            .expect("seed ready kline chart should produce a commit");
    }

    fn read_http_request(stream: &mut std::net::TcpStream) -> String {
        let mut buffer = [0_u8; 4096];
        let size = stream.read(&mut buffer).unwrap();
        String::from_utf8_lossy(&buffer[..size]).into_owned()
    }

    fn write_http_ok(stream: &mut std::net::TcpStream, body: &str) {
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(response.as_bytes()).unwrap();
        stream.flush().unwrap();
    }
}

#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::HashSet;
#[cfg(feature = "services")]
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tqsdk_core::{MarketChartCommand, MarketCommand, RuntimeCommand, Symbol};

use crate::error::{DataError, Result};
use crate::greeks::{
    OptionGreeksRequest, OptionGreeksResult, build_option_greeks_row, validate_option_metadata,
};
use crate::history_series_cache::{
    HistorySeriesCache, HistorySeriesCacheMaintenanceReport, default_history_cache_dir,
};
use crate::live_quote::await_quote_snapshots;

mod chart_ids;
mod chart_reader;
mod cont_quotes;
mod history_series;
mod page;
mod permissions;

pub use cont_quotes::{
    HistoricalContQuotesRow, HistoricalContUnderlyingRow, HistoricalContUnderlyingSegment,
    TradingCalendarRow, historical_cont_underlying_segments,
};
pub use page::{
    KlineDataPage, KlineDataPageRequest, KlineDataSeries, KlineDataSeriesRequest, TickDataPage,
    TickDataPageRequest, TickDataSeries, TickDataSeriesRequest,
};

use page::{KlineDataPageSpec, TickDataPageSpec};

const DEFAULT_HOLIDAY_URL: &str = "https://files.shinnytech.com/shinny_chinese_holiday.json";
const DEFAULT_CONTINUOUS_TABLE_URL: &str = "https://files.shinnytech.com/continuous_table.json";
const DEFAULT_HISTORY_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_HISTORY_PAGE_VIEW_WIDTH: usize = 2_000;
#[cfg(feature = "services")]
const DATA_SERVICE_HTTP_SEND_ATTEMPTS: usize = 3;
#[cfg(feature = "services")]
const DATA_SERVICE_HTTP_RETRY_BASE_DELAY: Duration = Duration::from_millis(250);
pub(crate) const MAX_HISTORY_VIEW_WIDTH: usize = 10_000;
#[cfg(feature = "services")]
const DIRECT_HTTPS_HOSTS: &[(&str, &str)] = &[
    (
        "auth.shinnytech.com",
        "TQSDK_DIRECT_RESOLVE_AUTH_SHINNYTECH_COM",
    ),
    (
        "api.shinnytech.com",
        "TQSDK_DIRECT_RESOLVE_API_SHINNYTECH_COM",
    ),
    (
        "files.shinnytech.com",
        "TQSDK_DIRECT_RESOLVE_FILES_SHINNYTECH_COM",
    ),
];

#[cfg(feature = "services")]
fn direct_reqwest_client() -> reqwest::Client {
    let mut builder = reqwest::Client::builder().no_proxy().http1_only();
    for (host, env_name) in DIRECT_HTTPS_HOSTS {
        if let Some(addrs) = resolve_https_host(host, env_name) {
            builder = builder.resolve_to_addrs(host, &addrs);
        }
    }
    builder.build().expect("direct reqwest client should build")
}

#[cfg(feature = "services")]
fn resolve_https_host(host: &str, env_name: &str) -> Option<Vec<SocketAddr>> {
    if let Some(addrs) = resolve_https_host_from_env(env_name) {
        return Some(addrs);
    }
    let addrs = (host, 443).to_socket_addrs().ok()?.collect::<Vec<_>>();
    (!addrs.is_empty()).then_some(addrs)
}

#[cfg(feature = "services")]
fn resolve_https_host_from_env(env_name: &str) -> Option<Vec<SocketAddr>> {
    let addrs = std::env::var(env_name)
        .ok()?
        .split(',')
        .filter_map(|value| value.trim().parse::<IpAddr>().ok())
        .map(|ip| SocketAddr::new(ip, 443))
        .collect::<Vec<_>>();
    (!addrs.is_empty()).then_some(addrs)
}
const MARKET_POLL_BUDGET: Duration = Duration::from_millis(250);

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

#[derive(Debug, Clone, Copy, Default)]
struct HistorySeriesCacheMaintenanceConfig {
    max_bytes: Option<u64>,
    retention_days: Option<u64>,
}

impl HistorySeriesCacheMaintenanceConfig {
    fn enabled(self) -> bool {
        self.max_bytes.is_some() || self.retention_days.is_some()
    }
}

/// Thin research/offline data wrapper over [`tqsdk_session::SessionClient`].
#[derive(Clone)]
pub struct DataClient {
    session: Option<tqsdk_session::SessionClient>,
    history_cache: Option<Arc<HistorySeriesCache>>,
    history_cache_maintenance: HistorySeriesCacheMaintenanceConfig,
    #[cfg(feature = "services")]
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
            history_cache: None,
            history_cache_maintenance: HistorySeriesCacheMaintenanceConfig::default(),
            #[cfg(feature = "services")]
            http: direct_reqwest_client(),
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

    #[must_use]
    pub fn with_history_cache(mut self, history_cache: HistorySeriesCache) -> Self {
        self.history_cache = Some(Arc::new(history_cache));
        self
    }

    #[cfg(all(test, feature = "services"))]
    #[must_use]
    fn new_for_test_with_urls(
        holiday_url: impl Into<String>,
        continuous_table_url: impl Into<String>,
    ) -> Self {
        Self {
            session: None,
            history_cache: None,
            history_cache_maintenance: HistorySeriesCacheMaintenanceConfig::default(),
            #[cfg(feature = "services")]
            http: direct_reqwest_client(),
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
    pub fn history_cache(&self) -> Option<&HistorySeriesCache> {
        self.history_cache.as_deref()
    }

    /// Explicitly run the maintenance limits configured on this client.
    ///
    /// History reads and writes never invoke this automatically: tick and
    /// Kline cache retention is operator-owned. `None` means either no cache
    /// or no maintenance limit was configured.
    pub fn run_configured_history_cache_maintenance(
        &self,
    ) -> Result<Option<HistorySeriesCacheMaintenanceReport>> {
        let Some(cache) = self.history_cache() else {
            return Ok(None);
        };
        if !self.history_cache_maintenance.enabled() {
            return Ok(None);
        }
        cache
            .enforce_limits(
                self.history_cache_maintenance.max_bytes,
                self.history_cache_maintenance.retention_days,
            )
            .map(Some)
    }

    #[must_use]
    pub fn into_session(self) -> Option<tqsdk_session::SessionClient> {
        self.session
    }

    fn require_session(&self, message: &'static str) -> Result<&tqsdk_session::SessionClient> {
        self.session
            .as_ref()
            .ok_or(DataError::InvalidState(message))
    }

    pub(crate) fn is_session_backed(&self) -> bool {
        self.session.is_some()
    }

    pub async fn get_kline_data_page(
        &self,
        request: KlineDataPageRequest,
    ) -> Result<KlineDataPage> {
        let spec = request.validate()?;
        let session =
            self.require_session("get_kline_data_page requires a session-backed data client")?;
        self.require_history_download_permission_async(session)
            .await?;
        let chart_id = chart_reader::next_kline_page_chart_id(request.symbol(), spec.duration_ns);
        let result = self
            .await_kline_data_page(session, &request, spec, chart_id.as_str())
            .await;
        chart_reader::cancel_chart_best_effort(session, chart_id).await;
        result
    }

    pub async fn get_tick_data_page(&self, request: TickDataPageRequest) -> Result<TickDataPage> {
        let spec = request.validate()?;
        let session =
            self.require_session("get_tick_data_page requires a session-backed data client")?;
        self.require_history_download_permission_async(session)
            .await?;
        let chart_id = chart_reader::next_tick_page_chart_id(request.symbol());
        let result = self
            .await_tick_data_page(session, &request, spec, chart_id.as_str())
            .await;
        chart_reader::cancel_chart_best_effort(session, chart_id).await;
        result
    }

    pub async fn query_option_greeks(
        &self,
        request: OptionGreeksRequest,
    ) -> Result<OptionGreeksResult> {
        let spec = request.validate()?;
        let session =
            self.require_session("query_option_greeks requires a session-backed data client")?;

        let symbol_refs = spec.symbols.iter().map(String::as_str).collect::<Vec<_>>();
        let metadata_infos = session.query_symbol_info(&symbol_refs).await?;
        let mut live_symbols = dedup_symbols_preserve_order(spec.symbols.iter().cloned());

        for (symbol, metadata) in spec.symbols.iter().zip(metadata_infos.iter()) {
            validate_option_metadata(symbol, metadata)?;
            let underlying_symbol = metadata.underlying_symbol.as_ref().ok_or_else(|| {
                DataError::InvalidResponse(format!(
                    "option metadata for {symbol} missing underlying_symbol"
                ))
            })?;
            live_symbols.push(underlying_symbol.as_str().to_string());
        }
        let live_symbols = dedup_symbols_preserve_order(live_symbols);
        let live_symbol_refs = live_symbols.iter().map(String::as_str).collect::<Vec<_>>();
        session
            .check_md_grants(&live_symbol_refs)
            .await
            .map_err(session_error_into_data)?;
        let live_quotes = await_quote_snapshots(session, &live_symbols, spec.timeout).await?;

        let mut rows = Vec::with_capacity(spec.symbols.len());
        for (index, (symbol, metadata)) in
            spec.symbols.iter().zip(metadata_infos.iter()).enumerate()
        {
            let option_quote = live_quotes.get(symbol).ok_or_else(|| {
                DataError::InvalidResponse(format!("missing live quote snapshot for {symbol}"))
            })?;
            let underlying_quote = live_quotes
                .get(
                    metadata
                        .underlying_symbol
                        .as_ref()
                        .map(|symbol| symbol.as_str())
                        .unwrap_or_default(),
                )
                .ok_or_else(|| {
                    let underlying = metadata
                        .underlying_symbol
                        .as_ref()
                        .map(|symbol| symbol.as_str())
                        .unwrap_or("<missing>");
                    DataError::InvalidResponse(format!(
                        "missing live quote snapshot for underlying {underlying} of {symbol}"
                    ))
                })?;
            let explicit_volatility = spec
                .volatilities
                .as_ref()
                .and_then(|volatilities| volatilities.get(index))
                .copied();
            rows.push(build_option_greeks_row(
                symbol,
                metadata,
                option_quote,
                underlying_quote,
                explicit_volatility,
                spec.risk_free_rate,
            )?);
        }

        Ok(OptionGreeksResult::new(rows))
    }

    async fn await_kline_data_page(
        &self,
        session: &tqsdk_session::SessionClient,
        request: &KlineDataPageRequest,
        spec: KlineDataPageSpec,
        chart_id: &str,
    ) -> Result<KlineDataPage> {
        let command = MarketChartCommand {
            chart_id: chart_id.to_string(),
            symbols: vec![Symbol::new(request.symbol())],
            duration_ns: spec.duration_ns,
            view_width: spec.view_width,
            left_kline_id: request.left_kline_id(),
            focus_datetime_ns: request.focus_datetime_ns(),
            focus_position: request.focus_position(),
        };
        let expected_state = chart_reader::ExpectedChartState::from_command(&command);
        let command_id = session
            .submit(RuntimeCommand::Market(MarketCommand::SetChart(command)))
            .await?;
        let reader = session.reader_clone();
        chart_reader::wait_for_ready_chart(
            session,
            &reader,
            chart_id,
            &expected_state,
            command_id,
            request.timeout(),
        )
        .await?;
        chart_reader::read_ready_kline_data_page(
            &reader,
            request.symbol(),
            spec.duration_ns,
            spec.view_width,
            chart_id,
        )?
        .ok_or_else(|| DataError::InvalidResponse("ready kline chart snapshot missing".to_string()))
    }

    async fn await_tick_data_page(
        &self,
        session: &tqsdk_session::SessionClient,
        request: &TickDataPageRequest,
        spec: TickDataPageSpec,
        chart_id: &str,
    ) -> Result<TickDataPage> {
        let command = MarketChartCommand {
            chart_id: chart_id.to_string(),
            symbols: vec![Symbol::new(request.symbol())],
            duration_ns: 0,
            view_width: spec.view_width,
            left_kline_id: request.left_id(),
            focus_datetime_ns: request.focus_datetime_ns(),
            focus_position: request.focus_position(),
        };
        let expected_state = chart_reader::ExpectedChartState::from_command(&command);
        let command_id = session
            .submit(RuntimeCommand::Market(MarketCommand::SetChart(command)))
            .await?;
        let reader = session.reader_clone();
        chart_reader::wait_for_ready_chart(
            session,
            &reader,
            chart_id,
            &expected_state,
            command_id,
            request.timeout(),
        )
        .await?;
        chart_reader::read_ready_tick_data_page(
            &reader,
            request.symbol(),
            spec.view_width,
            chart_id,
        )?
        .ok_or_else(|| DataError::InvalidResponse("ready tick chart snapshot missing".to_string()))
    }

    #[cfg(feature = "services")]
    async fn fetch_json(&self, url: &str) -> Result<Value> {
        let response = send_data_service_request_with_retry(|| self.http.get(url))
            .await?
            .error_for_status()?;
        Ok(response.json::<Value>().await?)
    }

    #[cfg(not(feature = "services"))]
    async fn fetch_json(&self, _url: &str) -> Result<Value> {
        Err(DataError::InvalidState(
            "tqsdk-data services feature is disabled",
        ))
    }
}

#[cfg(feature = "services")]
async fn send_data_service_request_with_retry(
    mut request: impl FnMut() -> reqwest::RequestBuilder,
) -> std::result::Result<reqwest::Response, reqwest::Error> {
    let mut attempt = 1_usize;
    loop {
        match request().send().await {
            Ok(response) => return Ok(response),
            Err(error)
                if attempt < DATA_SERVICE_HTTP_SEND_ATTEMPTS
                    && is_retryable_data_service_send_error(&error) =>
            {
                tokio::time::sleep(
                    DATA_SERVICE_HTTP_RETRY_BASE_DELAY.saturating_mul(attempt as u32),
                )
                .await;
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(feature = "services")]
fn is_retryable_data_service_send_error(error: &reqwest::Error) -> bool {
    error.is_timeout() || error.is_connect() || error.is_request()
}

#[derive(Clone, Default)]
pub struct DataClientBuilder {
    session: Option<tqsdk_session::SessionClient>,
    history_cache_enabled: bool,
    history_cache_dir: Option<PathBuf>,
    history_cache_maintenance: HistorySeriesCacheMaintenanceConfig,
}

impl DataClientBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_session(mut self, session: tqsdk_session::SessionClient) -> Self {
        self.session = Some(session);
        self
    }

    #[must_use]
    pub fn history_cache_enabled(mut self, enabled: bool) -> Self {
        self.history_cache_enabled = enabled;
        self
    }

    #[must_use]
    pub fn history_cache_dir(mut self, dir: impl AsRef<Path>) -> Self {
        self.history_cache_dir = Some(dir.as_ref().to_path_buf());
        self
    }

    #[must_use]
    /// Configure a size limit for an explicit
    /// [`DataClient::run_configured_history_cache_maintenance`] call.
    ///
    /// Downloading or reading history never applies this limit automatically.
    pub fn history_cache_max_bytes(mut self, max_bytes: u64) -> Self {
        self.history_cache_maintenance.max_bytes = Some(max_bytes);
        self
    }

    #[must_use]
    /// Configure a retention limit for an explicit
    /// [`DataClient::run_configured_history_cache_maintenance`] call.
    ///
    /// Downloading or reading history never applies this limit automatically.
    pub fn history_cache_retention_days(mut self, retention_days: u64) -> Self {
        self.history_cache_maintenance.retention_days = Some(retention_days);
        self
    }

    pub fn build(self) -> Result<DataClient> {
        let mut client = DataClient::new();
        if let Some(session) = self.session {
            client = client.with_session(session);
        }
        client.history_cache_maintenance = self.history_cache_maintenance;
        if self.history_cache_enabled {
            let cache = if let Some(dir) = self.history_cache_dir {
                HistorySeriesCache::open(dir)
            } else {
                HistorySeriesCache::open(default_history_cache_dir())
            }?;
            client = client.with_history_cache(cache);
        }
        Ok(client)
    }
}

pub(crate) fn normalize_history_view_width(view_width: usize) -> Result<usize> {
    if view_width == 0 {
        return Err(DataError::Validation(
            "view_width must be greater than zero".to_string(),
        ));
    }
    Ok(view_width.min(MAX_HISTORY_VIEW_WIDTH))
}

fn dedup_symbols_preserve_order(symbols: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut deduped = Vec::new();
    let mut seen = HashSet::new();
    for symbol in symbols {
        if seen.insert(symbol.clone()) {
            deduped.push(symbol);
        }
    }
    deduped
}

fn contract_error_into_data(error: tqsdk_core::ContractError) -> DataError {
    DataError::Session(tqsdk_session::SessionFacadeError::from(error))
}

fn session_error_into_data(error: tqsdk_session::SessionFacadeError) -> DataError {
    match error {
        tqsdk_session::SessionFacadeError::Core(tqsdk_core::ContractError::Auth(message)) => {
            DataError::PermissionDenied(message)
        }
        other => DataError::Session(other),
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    #[cfg(feature = "services")]
    use std::io::{Read, Write};
    #[cfg(feature = "services")]
    use std::net::TcpListener;

    use chrono::NaiveDate;
    use serde_json::json;
    use tqsdk_core::Tick;
    use tqsdk_core::{
        AdapterRegistry, CommitScope, InputPayload, IoEvent, Kline, ProtocolDomain, RuntimeHandle,
        RuntimeInput,
    };
    use tqsdk_session::SessionClient;

    use super::*;

    #[cfg(feature = "services")]
    #[test]
    fn fetch_json_retries_a_transient_send_failure() {
        run_on_tokio(async {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let server = std::thread::spawn(move || {
                for attempt in 0..2 {
                    let (mut stream, _) = listener.accept().unwrap();
                    let _ = read_http_request(&mut stream);
                    if attempt == 0 {
                        continue;
                    }
                    write_http_ok(&mut stream, r#"{"status":"ok"}"#);
                }
            });
            let client = DataClient::new();

            let payload = client
                .fetch_json(&format!("http://{addr}/flaky.json"))
                .await
                .expect("transient send failure should be retried");

            assert_eq!(payload, json!({"status": "ok"}));
            server.join().unwrap();
        });
    }

    #[cfg(feature = "services")]
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

    #[cfg(feature = "services")]
    #[test]
    fn query_his_cont_underlyings_returns_single_symbol_mapping() {
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
                        "DCE.a": [[20260429, "DCE.a2605"], [20260502, "DCE.a2609"]]
                    }"#,
                );
            });

            let client = DataClient::new_for_test_with_urls(
                format!("http://{addr}/holiday.json"),
                format!("http://{addr}/continuous_table.json"),
            );

            let rows = client
                .query_his_cont_underlyings(
                    "KQ.m@DCE.a",
                    3,
                    Some(NaiveDate::from_ymd_opt(2026, 5, 4).unwrap()),
                )
                .await
                .unwrap();

            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0].date, "2026-04-29");
            assert_eq!(rows[0].symbol, "KQ.m@DCE.a");
            assert_eq!(rows[0].underlying, "DCE.a2605");
            assert_eq!(rows[1].date, "2026-04-30");
            assert_eq!(rows[1].underlying, "DCE.a2605");
            assert_eq!(rows[2].date, "2026-05-04");
            assert_eq!(rows[2].underlying, "DCE.a2609");

            server.join().unwrap();
        });
    }

    #[cfg(feature = "services")]
    #[test]
    fn query_trading_calendar_and_days_use_holiday_payload() {
        run_on_tokio(async {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let server = std::thread::spawn(move || {
                for _ in 0..2 {
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
                }
            });

            let client = DataClient::new_for_test_with_urls(
                format!("http://{addr}/holiday.json"),
                format!("http://{addr}/continuous_table.json"),
            );
            let start = NaiveDate::from_ymd_opt(2026, 4, 29).unwrap();
            let end = NaiveDate::from_ymd_opt(2026, 5, 4).unwrap();

            let calendar = client.query_trading_calendar(start, end).await.unwrap();
            assert_eq!(
                calendar,
                vec![
                    TradingCalendarRow {
                        date: "2026-04-29".to_string(),
                        trading: true,
                    },
                    TradingCalendarRow {
                        date: "2026-04-30".to_string(),
                        trading: true,
                    },
                    TradingCalendarRow {
                        date: "2026-05-01".to_string(),
                        trading: false,
                    },
                    TradingCalendarRow {
                        date: "2026-05-02".to_string(),
                        trading: false,
                    },
                    TradingCalendarRow {
                        date: "2026-05-03".to_string(),
                        trading: false,
                    },
                    TradingCalendarRow {
                        date: "2026-05-04".to_string(),
                        trading: true,
                    },
                ]
            );

            let trading_days = client.query_trading_days(start, end).await.unwrap();
            assert_eq!(
                trading_days,
                vec![
                    NaiveDate::from_ymd_opt(2026, 4, 29).unwrap(),
                    NaiveDate::from_ymd_opt(2026, 4, 30).unwrap(),
                    NaiveDate::from_ymd_opt(2026, 5, 4).unwrap(),
                ]
            );

            server.join().unwrap();
        });
    }

    #[test]
    fn get_kline_data_page_returns_ready_rows_within_chart_bounds() {
        run_on_tokio(async {
            let (session, handle) = test_session_and_handle();
            let client = DataClient::from_session(session.clone());
            let request = KlineDataPageRequest::new("SHFE.ao2609", Duration::from_secs(60), 2)
                .with_left_kline_id(100)
                .with_timeout(Duration::from_millis(100));
            let duration_ns = request.validate().unwrap().duration_ns;

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

            let page = client
                .await_kline_data_page(
                    &session,
                    &request,
                    request.validate().unwrap(),
                    "data-kline-test",
                )
                .await
                .unwrap();

            assert_eq!(page.symbol(), "SHFE.ao2609");
            assert_eq!(page.duration_ns(), duration_ns);
            assert_eq!(page.view_width(), 2);
            assert_eq!(page.chart_left_id(), 100);
            assert_eq!(page.chart_right_id(), 101);
            assert_eq!(page.next_left_kline_id(), Some(102));
            assert_eq!(page.len(), 2);
            assert_eq!(page.rows()[0].id, 100);
            assert_eq!(page.rows()[1].id, 101);
            assert_eq!(page.last().map(|row| row.close), Some(620.0));

            seed_thread.join().unwrap();
        });
    }

    #[test]
    fn get_tick_data_page_returns_ready_rows_within_chart_bounds() {
        run_on_tokio(async {
            let (session, handle) = test_session_and_handle();
            let client = DataClient::from_session(session.clone());
            let request =
                TickDataPageRequest::new("SHFE.ao2609", 2).with_timeout(Duration::from_millis(100));

            let seed_thread = std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(5));
                seed_ready_tick_chart(&handle, "data-tick-test", "SHFE.ao2609", 200, 201);
            });

            let page = client
                .await_tick_data_page(
                    &session,
                    &request,
                    request.validate().unwrap(),
                    "data-tick-test",
                )
                .await
                .unwrap();

            assert_eq!(page.symbol(), "SHFE.ao2609");
            assert_eq!(page.view_width(), 2);
            assert_eq!(page.chart_left_id(), 200);
            assert_eq!(page.chart_right_id(), 201);
            assert_eq!(page.next_left_id(), Some(202));
            assert_eq!(page.len(), 2);
            assert_eq!(page.rows()[0].id, 200);
            assert_eq!(page.rows()[1].id, 201);
            assert_eq!(page.last().map(|row| row.last_price), Some(618.5));

            seed_thread.join().unwrap();
        });
    }

    #[test]
    fn get_kline_data_page_allows_ready_chart_with_more_data_true() {
        run_on_tokio(async {
            let (session, handle) = test_session_and_handle();
            let client = DataClient::from_session(session.clone());
            let request = KlineDataPageRequest::new("SHFE.ao2609", Duration::from_secs(60), 2)
                .with_left_kline_id(100)
                .with_timeout(Duration::from_millis(100));
            let duration_ns = request.validate().unwrap().duration_ns;

            let seed_thread = std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(5));
                seed_kline_chart(
                    &handle,
                    "data-kline-more-data",
                    "SHFE.ao2609",
                    duration_ns,
                    100,
                    101,
                    true,
                );
            });

            let page = client
                .await_kline_data_page(
                    &session,
                    &request,
                    request.validate().unwrap(),
                    "data-kline-more-data",
                )
                .await
                .unwrap();

            assert_eq!(page.chart_left_id(), 100);
            assert_eq!(page.chart_right_id(), 101);
            assert!(page.more_data());
            assert_eq!(page.len(), 2);

            seed_thread.join().unwrap();
        });
    }

    #[test]
    fn get_tick_data_page_allows_ready_chart_with_more_data_true() {
        run_on_tokio(async {
            let (session, handle) = test_session_and_handle();
            let client = DataClient::from_session(session.clone());
            let request =
                TickDataPageRequest::new("SHFE.ao2609", 2).with_timeout(Duration::from_millis(100));

            let seed_thread = std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(5));
                seed_tick_chart(
                    &handle,
                    "data-tick-more-data",
                    "SHFE.ao2609",
                    200,
                    201,
                    true,
                );
            });

            let page = client
                .await_tick_data_page(
                    &session,
                    &request,
                    request.validate().unwrap(),
                    "data-tick-more-data",
                )
                .await
                .unwrap();

            assert_eq!(page.chart_left_id(), 200);
            assert_eq!(page.chart_right_id(), 201);
            assert!(page.more_data());
            assert_eq!(page.len(), 2);

            seed_thread.join().unwrap();
        });
    }

    #[test]
    fn get_kline_data_page_requires_session_backed_client() {
        run_on_tokio(async {
            let err = DataClient::new()
                .get_kline_data_page(KlineDataPageRequest::new(
                    "SHFE.ao2609",
                    Duration::from_secs(60),
                    2,
                ))
                .await
                .unwrap_err();

            assert!(matches!(
                err,
                DataError::InvalidState(message)
                    if message == "get_kline_data_page requires a session-backed data client"
            ));
        });
    }

    #[test]
    fn get_tick_data_page_requires_session_backed_client() {
        run_on_tokio(async {
            let err = DataClient::new()
                .get_tick_data_page(TickDataPageRequest::new("SHFE.ao2609", 2))
                .await
                .unwrap_err();

            assert!(matches!(
                err,
                DataError::InvalidState(message)
                    if message == "get_tick_data_page requires a session-backed data client"
            ));
        });
    }

    #[test]
    fn get_kline_data_page_requires_tq_dl_when_auth_context_is_known() {
        run_on_tokio(async {
            let (session, handle) = test_session_and_handle();
            seed_auth_features(&handle, &["query"]);
            let client = DataClient::from_session(session);

            let err = client
                .get_kline_data_page(KlineDataPageRequest::new(
                    "SHFE.ao2609",
                    Duration::from_secs(60),
                    2,
                ))
                .await
                .unwrap_err();

            assert!(matches!(
                err,
                DataError::PermissionDenied(message)
                    if message.contains("tq_dl permission")
            ));
        });
    }

    #[test]
    fn get_kline_data_page_times_out_without_ready_chart() {
        run_on_tokio(async {
            let (session, _handle) = test_session_and_handle();
            let client = DataClient::from_session(session);

            let err = client
                .get_kline_data_page(
                    KlineDataPageRequest::new("SHFE.ao2609", Duration::from_secs(60), 2)
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
    fn get_tick_data_page_times_out_without_ready_chart() {
        run_on_tokio(async {
            let (session, _handle) = test_session_and_handle();
            let client = DataClient::from_session(session);

            let err = client
                .get_tick_data_page(
                    TickDataPageRequest::new("SHFE.ao2609", 2)
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
    fn get_kline_data_page_rejects_invalid_requests() {
        run_on_tokio(async {
            let client = DataClient::new();

            let err = client
                .get_kline_data_page(KlineDataPageRequest::new("", Duration::from_secs(60), 2))
                .await
                .unwrap_err();
            assert!(
                matches!(err, DataError::Validation(message) if message == "symbol must not be empty")
            );

            let err = client
                .get_kline_data_page(KlineDataPageRequest::new(
                    "SHFE.ao2609",
                    Duration::from_secs(60),
                    0,
                ))
                .await
                .unwrap_err();
            assert!(
                matches!(err, DataError::Validation(message) if message == "view_width must be greater than zero")
            );

            let err = client
                .get_kline_data_page(KlineDataPageRequest::new("SHFE.ao2609", Duration::ZERO, 2))
                .await
                .unwrap_err();
            assert!(
                matches!(err, DataError::Validation(message) if message == "duration must be greater than zero")
            );

            let err = client
                .get_kline_data_page(
                    KlineDataPageRequest::new("SHFE.ao2609", Duration::from_secs(60), 2)
                        .with_left_kline_id(1)
                        .with_focus_datetime_ns(1),
                )
                .await
                .unwrap_err();
            assert!(
                matches!(err, DataError::Validation(message) if message == "left_kline_id and focus_datetime_ns cannot both be set")
            );
        });
    }

    #[test]
    fn get_tick_data_page_rejects_invalid_requests() {
        run_on_tokio(async {
            let client = DataClient::new();

            let err = client
                .get_tick_data_page(TickDataPageRequest::new("", 2))
                .await
                .unwrap_err();
            assert!(
                matches!(err, DataError::Validation(message) if message == "symbol must not be empty")
            );

            let err = client
                .get_tick_data_page(TickDataPageRequest::new("SHFE.ao2609", 0))
                .await
                .unwrap_err();
            assert!(
                matches!(err, DataError::Validation(message) if message == "view_width must be greater than zero")
            );

            let err = client
                .get_tick_data_page(
                    TickDataPageRequest::new("SHFE.ao2609", 2)
                        .with_left_id(1)
                        .with_focus_datetime_ns(1),
                )
                .await
                .unwrap_err();
            assert!(
                matches!(err, DataError::Validation(message) if message == "left_id and focus_datetime_ns cannot both be set")
            );
        });
    }

    #[test]
    fn extend_kline_rows_in_window_applies_bounds_and_next_id() {
        let mut rows = Vec::new();
        let next_left_kline_id = history_series::extend_rows_in_window(
            &mut rows,
            vec![
                Kline {
                    id: 100,
                    datetime: 10,
                    close: 1.0,
                    ..Kline::default()
                },
                Kline {
                    id: 101,
                    datetime: 20,
                    close: 2.0,
                    ..Kline::default()
                },
                Kline {
                    id: 102,
                    datetime: 30,
                    close: 3.0,
                    ..Kline::default()
                },
            ],
            15,
            30,
        );

        assert_eq!(next_left_kline_id, Some(102));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, 101);
    }

    #[test]
    fn extend_tick_rows_in_window_applies_bounds_and_next_id() {
        let mut rows = Vec::new();
        let next_left_id = history_series::extend_rows_in_window(
            &mut rows,
            vec![
                Tick {
                    id: 200,
                    datetime: 10,
                    last_price: 1.0,
                    ..Tick::default()
                },
                Tick {
                    id: 201,
                    datetime: 20,
                    last_price: 2.0,
                    ..Tick::default()
                },
                Tick {
                    id: 202,
                    datetime: 30,
                    last_price: 3.0,
                    ..Tick::default()
                },
            ],
            15,
            30,
        );

        assert_eq!(next_left_id, Some(202));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, 201);
    }

    #[test]
    fn dedup_sort_kline_rows_by_id_keeps_latest_row_per_id() {
        let rows = history_series::dedup_sort_rows_by_id(vec![
            Kline {
                id: 2,
                close: 2.0,
                ..Kline::default()
            },
            Kline {
                id: 1,
                close: 1.0,
                ..Kline::default()
            },
            Kline {
                id: 2,
                close: 20.0,
                ..Kline::default()
            },
        ]);

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].id, 1);
        assert_eq!(rows[1].id, 2);
        assert_eq!(rows[1].close, 20.0);
    }

    #[test]
    fn dedup_sort_tick_rows_by_id_keeps_latest_row_per_id() {
        let rows = history_series::dedup_sort_rows_by_id(vec![
            Tick {
                id: 2,
                last_price: 2.0,
                ..Tick::default()
            },
            Tick {
                id: 1,
                last_price: 1.0,
                ..Tick::default()
            },
            Tick {
                id: 2,
                last_price: 20.0,
                ..Tick::default()
            },
        ]);

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].id, 1);
        assert_eq!(rows[1].id, 2);
        assert_eq!(rows[1].last_price, 20.0);
    }

    #[test]
    fn get_kline_data_series_requires_session_backed_client() {
        run_on_tokio(async {
            let err = DataClient::new()
                .get_kline_data_series(KlineDataSeriesRequest::new(
                    "SHFE.ao2609",
                    Duration::from_secs(60),
                    0,
                    10,
                ))
                .await
                .unwrap_err();

            assert!(matches!(
                err,
                DataError::InvalidState(message)
                    if message == "get_kline_data_series requires a session-backed data client"
            ));
        });
    }

    #[test]
    fn get_tick_data_series_requires_session_backed_client() {
        run_on_tokio(async {
            let err = DataClient::new()
                .get_tick_data_series(TickDataSeriesRequest::new("SHFE.ao2609", 0, 10))
                .await
                .unwrap_err();

            assert!(matches!(
                err,
                DataError::InvalidState(message)
                    if message == "get_tick_data_series requires a session-backed data client"
            ));
        });
    }

    #[test]
    fn query_option_greeks_requires_session_backed_client() {
        run_on_tokio(async {
            let err = DataClient::new()
                .query_option_greeks(OptionGreeksRequest::new(["SHFE.au2606C720"]))
                .await
                .unwrap_err();

            assert!(matches!(
                err,
                DataError::InvalidState(message)
                    if message == "query_option_greeks requires a session-backed data client"
            ));
        });
    }

    #[test]
    fn get_kline_data_series_requires_tq_dl_when_auth_context_is_known() {
        run_on_tokio(async {
            let (session, handle) = test_session_and_handle();
            seed_auth_features(&handle, &["query"]);
            let client = DataClient::from_session(session);

            let err = client
                .get_kline_data_series(KlineDataSeriesRequest::new(
                    "SHFE.ao2609",
                    Duration::from_secs(60),
                    0,
                    10,
                ))
                .await
                .unwrap_err();

            assert!(matches!(
                err,
                DataError::PermissionDenied(message)
                    if message.contains("tq_dl permission")
            ));
        });
    }

    #[test]
    fn kline_data_download_requires_tq_dl_when_auth_context_is_known() {
        let (session, handle) = test_session_and_handle();
        seed_auth_features(&handle, &["query"]);
        let client = DataClient::from_session(session);

        let err = client
            .kline_data_download(KlineDataSeriesRequest::new(
                "SHFE.ao2609",
                Duration::from_secs(60),
                0,
                10,
            ))
            .unwrap_err();

        assert!(matches!(
            err,
            DataError::PermissionDenied(message)
                if message.contains("tq_dl permission")
        ));
    }

    #[test]
    fn get_kline_data_series_rejects_invalid_requests() {
        run_on_tokio(async {
            let client = DataClient::new();

            let err = client
                .get_kline_data_series(KlineDataSeriesRequest::new(
                    "",
                    Duration::from_secs(60),
                    0,
                    10,
                ))
                .await
                .unwrap_err();
            assert!(
                matches!(err, DataError::Validation(message) if message == "symbol must not be empty")
            );

            let err = client
                .get_kline_data_series(KlineDataSeriesRequest::new(
                    "SHFE.ao2609",
                    Duration::ZERO,
                    0,
                    10,
                ))
                .await
                .unwrap_err();
            assert!(
                matches!(err, DataError::Validation(message) if message == "duration must be greater than zero")
            );

            let err = client
                .get_kline_data_series(KlineDataSeriesRequest::new(
                    "SHFE.ao2609",
                    Duration::from_secs(60),
                    10,
                    10,
                ))
                .await
                .unwrap_err();
            assert!(
                matches!(err, DataError::Validation(message) if message == "end_datetime_ns must be greater than start_datetime_ns")
            );

            let err = client
                .get_kline_data_series(
                    KlineDataSeriesRequest::new("SHFE.ao2609", Duration::from_secs(60), 0, 10)
                        .with_page_view_width(0),
                )
                .await
                .unwrap_err();
            assert!(
                matches!(err, DataError::Validation(message) if message == "view_width must be greater than zero")
            );
        });
    }

    #[test]
    fn get_tick_data_series_rejects_invalid_requests() {
        run_on_tokio(async {
            let client = DataClient::new();

            let err = client
                .get_tick_data_series(TickDataSeriesRequest::new("", 0, 10))
                .await
                .unwrap_err();
            assert!(
                matches!(err, DataError::Validation(message) if message == "symbol must not be empty")
            );

            let err = client
                .get_tick_data_series(TickDataSeriesRequest::new("SHFE.ao2609", 10, 10))
                .await
                .unwrap_err();
            assert!(
                matches!(err, DataError::Validation(message) if message == "end_datetime_ns must be greater than start_datetime_ns")
            );

            let err = client
                .get_tick_data_series(
                    TickDataSeriesRequest::new("SHFE.ao2609", 0, 10).with_page_view_width(0),
                )
                .await
                .unwrap_err();
            assert!(
                matches!(err, DataError::Validation(message) if message == "view_width must be greater than zero")
            );
        });
    }

    #[test]
    fn query_option_greeks_rejects_invalid_requests() {
        run_on_tokio(async {
            let client = DataClient::new();

            let err = client
                .query_option_greeks(OptionGreeksRequest::new(Vec::<String>::new()))
                .await
                .unwrap_err();
            assert!(
                matches!(err, DataError::Validation(message) if message == "symbols must not be empty")
            );

            let err = client
                .query_option_greeks(
                    OptionGreeksRequest::new(["SHFE.au2606C720"]).with_volatilities(vec![0.2, 0.3]),
                )
                .await
                .unwrap_err();
            assert!(
                matches!(err, DataError::Validation(message) if message == "volatilities length must match symbols length")
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

    #[test]
    fn historical_cont_underlying_segments_compacts_adjacent_rows() {
        let rows = vec![
            HistoricalContUnderlyingRow {
                date: "2026-04-28".to_string(),
                symbol: "KQ.m@DCE.a".to_string(),
                underlying: String::new(),
            },
            HistoricalContUnderlyingRow {
                date: "2026-04-29".to_string(),
                symbol: "KQ.m@DCE.a".to_string(),
                underlying: "DCE.a2605".to_string(),
            },
            HistoricalContUnderlyingRow {
                date: "2026-04-30".to_string(),
                symbol: "KQ.m@DCE.a".to_string(),
                underlying: "DCE.a2605".to_string(),
            },
            HistoricalContUnderlyingRow {
                date: "2026-05-04".to_string(),
                symbol: "KQ.m@DCE.a".to_string(),
                underlying: "DCE.a2609".to_string(),
            },
            HistoricalContUnderlyingRow {
                date: "2026-05-05".to_string(),
                symbol: "KQ.m@DCE.a".to_string(),
                underlying: "DCE.a2609".to_string(),
            },
            HistoricalContUnderlyingRow {
                date: "2026-05-06".to_string(),
                symbol: "KQ.m@DCE.a".to_string(),
                underlying: "DCE.a2605".to_string(),
            },
        ];

        let segments = historical_cont_underlying_segments(&rows);

        assert_eq!(
            segments,
            vec![
                HistoricalContUnderlyingSegment {
                    symbol: "KQ.m@DCE.a".to_string(),
                    underlying: "DCE.a2605".to_string(),
                    start_date: "2026-04-29".to_string(),
                    end_date: "2026-04-30".to_string(),
                    trading_days: 2,
                },
                HistoricalContUnderlyingSegment {
                    symbol: "KQ.m@DCE.a".to_string(),
                    underlying: "DCE.a2609".to_string(),
                    start_date: "2026-05-04".to_string(),
                    end_date: "2026-05-05".to_string(),
                    trading_days: 2,
                },
                HistoricalContUnderlyingSegment {
                    symbol: "KQ.m@DCE.a".to_string(),
                    underlying: "DCE.a2605".to_string(),
                    start_date: "2026-05-06".to_string(),
                    end_date: "2026-05-06".to_string(),
                    trading_days: 1,
                },
            ]
        );
    }

    #[test]
    fn dedup_symbols_preserve_order_keeps_first_occurrence() {
        let symbols = dedup_symbols_preserve_order(vec![
            "SHFE.au2606C720".to_string(),
            "SHFE.au2606".to_string(),
            "SHFE.au2606C720".to_string(),
            "SHFE.au2606".to_string(),
            "SHFE.au2608".to_string(),
        ]);

        assert_eq!(
            symbols,
            vec![
                "SHFE.au2606C720".to_string(),
                "SHFE.au2606".to_string(),
                "SHFE.au2608".to_string(),
            ]
        );
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
            tqsdk_session::testing::ManualSession::from_runtime(handle.clone()).into_client();

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
        seed_kline_chart(
            handle,
            chart_id,
            symbol,
            duration_ns,
            left_id,
            right_id,
            false,
        );
    }

    fn seed_kline_chart(
        handle: &RuntimeHandle,
        chart_id: &str,
        symbol: &str,
        duration_ns: i64,
        left_id: i64,
        right_id: i64,
        more_data: bool,
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
                                        "view_width": 2,
                                        "left_kline_id": left_id,
                                    },
                                    "left_id": left_id,
                                    "right_id": right_id,
                                    "more_data": more_data,
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

    fn seed_ready_tick_chart(
        handle: &RuntimeHandle,
        chart_id: &str,
        symbol: &str,
        left_id: i64,
        right_id: i64,
    ) {
        seed_tick_chart(handle, chart_id, symbol, left_id, right_id, false);
    }

    fn seed_tick_chart(
        handle: &RuntimeHandle,
        chart_id: &str,
        symbol: &str,
        left_id: i64,
        right_id: i64,
        more_data: bool,
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
                                        "duration": 0,
                                        "view_width": 2,
                                    },
                                    "left_id": left_id,
                                    "right_id": right_id,
                                    "more_data": more_data,
                                    "ready": true,
                                }
                            },
                            "ticks": {
                                symbol: {
                                    "data": {
                                        "199": {
                                            "id": 199,
                                            "datetime": 1_713_659_999_500_000_000_i64,
                                            "last_price": 617.8,
                                            "average": 617.9,
                                            "highest": 618.0,
                                            "lowest": 617.5,
                                            "ask_price1": 617.9,
                                            "ask_volume1": 2,
                                            "bid_price1": 617.8,
                                            "bid_volume1": 3,
                                            "volume": 10,
                                            "amount": 6178.0,
                                            "open_interest": 100
                                        },
                                        "200": {
                                            "id": 200,
                                            "datetime": 1_713_660_000_000_000_000_i64,
                                            "last_price": 618.0,
                                            "average": 618.2,
                                            "highest": 619.0,
                                            "lowest": 617.5,
                                            "ask_price1": 618.2,
                                            "ask_volume1": 4,
                                            "bid_price1": 618.0,
                                            "bid_volume1": 5,
                                            "volume": 12,
                                            "amount": 7416.0,
                                            "open_interest": 101
                                        },
                                        "201": {
                                            "id": 201,
                                            "datetime": 1_713_660_000_500_000_000_i64,
                                            "last_price": 618.5,
                                            "average": 618.3,
                                            "highest": 619.2,
                                            "lowest": 617.5,
                                            "ask_price1": 618.6,
                                            "ask_volume1": 3,
                                            "bid_price1": 618.4,
                                            "bid_volume1": 6,
                                            "volume": 15,
                                            "amount": 9277.5,
                                            "open_interest": 102
                                        },
                                        "202": {
                                            "id": 202,
                                            "datetime": 1_713_660_001_000_000_000_i64,
                                            "last_price": 619.0,
                                            "average": 618.5,
                                            "highest": 619.5,
                                            "lowest": 617.5,
                                            "ask_price1": 619.1,
                                            "ask_volume1": 5,
                                            "bid_price1": 618.9,
                                            "bid_volume1": 4,
                                            "volume": 18,
                                            "amount": 11142.0,
                                            "open_interest": 103
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
            .expect("seed ready tick chart should produce a commit");
    }

    fn seed_auth_features(handle: &RuntimeHandle, features: &[&str]) {
        handle
            .ingest(
                RuntimeInput::Io(IoEvent {
                    route: "system".to_string(),
                    domains: vec![ProtocolDomain::System],
                    payload: InputPayload::Json(json!({
                        "aid": "rtn_data",
                        "data": [{
                            "auth": {
                                "context": {
                                    "features": features,
                                }
                            }
                        }]
                    })),
                }),
                vec![],
                CommitScope::RealtimeUpdate,
            )
            .unwrap()
            .expect("seed auth features should produce a commit");
    }

    #[cfg(feature = "services")]
    fn read_http_request(stream: &mut std::net::TcpStream) -> String {
        let mut buffer = [0_u8; 4096];
        let size = stream.read(&mut buffer).unwrap();
        String::from_utf8_lossy(&buffer[..size]).into_owned()
    }

    #[cfg(feature = "services")]
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

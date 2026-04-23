#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use chrono::{Datelike, Days, FixedOffset, NaiveDate, Utc};
use serde_json::Value;
use tqsdk_core::{Chart, Kline, MarketChartCommand, MarketCommand, RuntimeCommand, Symbol, Tick};

use crate::error::{DataError, Result};

const DEFAULT_HOLIDAY_URL: &str = "https://files.shinnytech.com/shinny_chinese_holiday.json";
const DEFAULT_CONTINUOUS_TABLE_URL: &str = "https://files.shinnytech.com/continuous_table.json";
const DEFAULT_HISTORY_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_HISTORY_PAGE_VIEW_WIDTH: usize = 2_000;
const MAX_HISTORY_VIEW_WIDTH: usize = 10_000;
const HISTORY_DOWNLOAD_PERMISSION_MESSAGE: &str = "history data download requires tq_dl permission; upgrade: https://www.shinnytech.com/tqsdk-buy/";

static NEXT_HISTORY_CHART_ID: AtomicU64 = AtomicU64::new(1);

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

/// Request for a one-shot owned kline history page backed by the market chart contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KlineDataPageRequest {
    symbol: String,
    duration: Duration,
    view_width: usize,
    left_kline_id: Option<i64>,
    focus_datetime_ns: Option<i64>,
    focus_position: Option<usize>,
    timeout: Duration,
}

impl KlineDataPageRequest {
    #[must_use]
    pub fn new(symbol: impl Into<String>, duration: Duration, view_width: usize) -> Self {
        Self {
            symbol: symbol.into(),
            duration,
            view_width,
            left_kline_id: None,
            focus_datetime_ns: None,
            focus_position: None,
            timeout: DEFAULT_HISTORY_REQUEST_TIMEOUT,
        }
    }

    #[must_use]
    pub fn with_left_kline_id(mut self, left_kline_id: i64) -> Self {
        self.left_kline_id = Some(left_kline_id);
        self
    }

    #[must_use]
    pub fn with_focus_datetime_ns(mut self, focus_datetime_ns: i64) -> Self {
        self.focus_datetime_ns = Some(focus_datetime_ns);
        self
    }

    #[must_use]
    pub fn with_focus_position(mut self, focus_position: usize) -> Self {
        self.focus_position = Some(focus_position);
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
    pub fn view_width(&self) -> usize {
        self.view_width
    }

    #[must_use]
    pub fn left_kline_id(&self) -> Option<i64> {
        self.left_kline_id
    }

    #[must_use]
    pub fn focus_datetime_ns(&self) -> Option<i64> {
        self.focus_datetime_ns
    }

    #[must_use]
    pub fn focus_position(&self) -> Option<usize> {
        self.focus_position
    }

    #[must_use]
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    fn validate(&self) -> Result<KlineDataPageSpec> {
        if self.symbol.is_empty() {
            return Err(DataError::Validation(
                "symbol must not be empty".to_string(),
            ));
        }
        if self.left_kline_id.is_some() && self.focus_datetime_ns.is_some() {
            return Err(DataError::Validation(
                "left_kline_id and focus_datetime_ns cannot both be set".to_string(),
            ));
        }
        if self.focus_position.is_some() && self.focus_datetime_ns.is_none() {
            return Err(DataError::Validation(
                "focus_position requires focus_datetime_ns".to_string(),
            ));
        }
        if let Some(left_kline_id) = self.left_kline_id
            && left_kline_id < 0
        {
            return Err(DataError::Validation(
                "left_kline_id must be greater than or equal to zero".to_string(),
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
        Ok(KlineDataPageSpec {
            duration_ns,
            view_width: normalize_history_view_width(self.view_width)?,
        })
    }
}

/// Owned result of a one-shot kline history page.
#[derive(Debug, Clone, Default)]
pub struct KlineDataPage {
    symbol: String,
    duration_ns: i64,
    view_width: usize,
    chart_left_id: i64,
    chart_right_id: i64,
    rows: Vec<Kline>,
}

impl KlineDataPage {
    pub(crate) fn new(
        symbol: String,
        duration_ns: i64,
        view_width: usize,
        chart_left_id: i64,
        chart_right_id: i64,
        rows: Vec<Kline>,
    ) -> Self {
        Self {
            symbol,
            duration_ns,
            view_width,
            chart_left_id,
            chart_right_id,
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
    pub fn view_width(&self) -> usize {
        self.view_width
    }

    #[must_use]
    pub fn chart_left_id(&self) -> i64 {
        self.chart_left_id
    }

    #[must_use]
    pub fn chart_right_id(&self) -> i64 {
        self.chart_right_id
    }

    #[must_use]
    pub fn next_left_kline_id(&self) -> Option<i64> {
        if self.chart_right_id < 0 {
            None
        } else {
            self.chart_right_id.checked_add(1)
        }
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

/// Request for a one-shot owned tick history page backed by the market chart contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TickDataPageRequest {
    symbol: String,
    view_width: usize,
    left_id: Option<i64>,
    focus_datetime_ns: Option<i64>,
    focus_position: Option<usize>,
    timeout: Duration,
}

impl TickDataPageRequest {
    #[must_use]
    pub fn new(symbol: impl Into<String>, view_width: usize) -> Self {
        Self {
            symbol: symbol.into(),
            view_width,
            left_id: None,
            focus_datetime_ns: None,
            focus_position: None,
            timeout: DEFAULT_HISTORY_REQUEST_TIMEOUT,
        }
    }

    #[must_use]
    pub fn with_left_id(mut self, left_id: i64) -> Self {
        self.left_id = Some(left_id);
        self
    }

    #[must_use]
    pub fn with_focus_datetime_ns(mut self, focus_datetime_ns: i64) -> Self {
        self.focus_datetime_ns = Some(focus_datetime_ns);
        self
    }

    #[must_use]
    pub fn with_focus_position(mut self, focus_position: usize) -> Self {
        self.focus_position = Some(focus_position);
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
    pub fn view_width(&self) -> usize {
        self.view_width
    }

    #[must_use]
    pub fn left_id(&self) -> Option<i64> {
        self.left_id
    }

    #[must_use]
    pub fn focus_datetime_ns(&self) -> Option<i64> {
        self.focus_datetime_ns
    }

    #[must_use]
    pub fn focus_position(&self) -> Option<usize> {
        self.focus_position
    }

    #[must_use]
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    fn validate(&self) -> Result<TickDataPageSpec> {
        if self.symbol.is_empty() {
            return Err(DataError::Validation(
                "symbol must not be empty".to_string(),
            ));
        }
        if self.left_id.is_some() && self.focus_datetime_ns.is_some() {
            return Err(DataError::Validation(
                "left_id and focus_datetime_ns cannot both be set".to_string(),
            ));
        }
        if self.focus_position.is_some() && self.focus_datetime_ns.is_none() {
            return Err(DataError::Validation(
                "focus_position requires focus_datetime_ns".to_string(),
            ));
        }
        if let Some(left_id) = self.left_id
            && left_id < 0
        {
            return Err(DataError::Validation(
                "left_id must be greater than or equal to zero".to_string(),
            ));
        }
        Ok(TickDataPageSpec {
            view_width: normalize_history_view_width(self.view_width)?,
        })
    }
}

/// Owned result of a one-shot tick history page.
#[derive(Debug, Clone, Default)]
pub struct TickDataPage {
    symbol: String,
    view_width: usize,
    chart_left_id: i64,
    chart_right_id: i64,
    rows: Vec<Tick>,
}

impl TickDataPage {
    pub(crate) fn new(
        symbol: String,
        view_width: usize,
        chart_left_id: i64,
        chart_right_id: i64,
        rows: Vec<Tick>,
    ) -> Self {
        Self {
            symbol,
            view_width,
            chart_left_id,
            chart_right_id,
            rows,
        }
    }

    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    #[must_use]
    pub fn view_width(&self) -> usize {
        self.view_width
    }

    #[must_use]
    pub fn chart_left_id(&self) -> i64 {
        self.chart_left_id
    }

    #[must_use]
    pub fn chart_right_id(&self) -> i64 {
        self.chart_right_id
    }

    #[must_use]
    pub fn next_left_id(&self) -> Option<i64> {
        if self.chart_right_id < 0 {
            None
        } else {
            self.chart_right_id.checked_add(1)
        }
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
    pub fn last(&self) -> Option<&Tick> {
        self.rows.last()
    }

    #[must_use]
    pub fn get(&self, index: usize) -> Option<&Tick> {
        self.rows.get(index)
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = &Tick> + DoubleEndedIterator {
        self.rows.iter()
    }

    #[must_use]
    pub fn rows(&self) -> &[Tick] {
        &self.rows
    }

    #[must_use]
    pub fn into_rows(self) -> Vec<Tick> {
        self.rows
    }
}

/// Request for a one-shot owned kline history series in `[start_datetime_ns, end_datetime_ns)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KlineDataSeriesRequest {
    symbol: String,
    duration: Duration,
    start_datetime_ns: i64,
    end_datetime_ns: i64,
    page_view_width: usize,
    timeout: Duration,
}

impl KlineDataSeriesRequest {
    #[must_use]
    pub fn new(
        symbol: impl Into<String>,
        duration: Duration,
        start_datetime_ns: i64,
        end_datetime_ns: i64,
    ) -> Self {
        Self {
            symbol: symbol.into(),
            duration,
            start_datetime_ns,
            end_datetime_ns,
            page_view_width: DEFAULT_HISTORY_PAGE_VIEW_WIDTH,
            timeout: DEFAULT_HISTORY_REQUEST_TIMEOUT,
        }
    }

    #[must_use]
    pub fn with_page_view_width(mut self, page_view_width: usize) -> Self {
        self.page_view_width = page_view_width;
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
    pub fn start_datetime_ns(&self) -> i64 {
        self.start_datetime_ns
    }

    #[must_use]
    pub fn end_datetime_ns(&self) -> i64 {
        self.end_datetime_ns
    }

    #[must_use]
    pub fn page_view_width(&self) -> usize {
        self.page_view_width
    }

    #[must_use]
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    fn validate(&self) -> Result<KlineDataSeriesSpec> {
        if self.symbol.is_empty() {
            return Err(DataError::Validation(
                "symbol must not be empty".to_string(),
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
        if self.end_datetime_ns <= self.start_datetime_ns {
            return Err(DataError::Validation(
                "end_datetime_ns must be greater than start_datetime_ns".to_string(),
            ));
        }
        Ok(KlineDataSeriesSpec {
            duration_ns,
            start_datetime_ns: self.start_datetime_ns,
            end_datetime_ns: self.end_datetime_ns,
            page_view_width: normalize_history_view_width(self.page_view_width)?,
        })
    }
}

/// Owned result of a one-shot kline history series.
#[derive(Debug, Clone, Default)]
pub struct KlineDataSeries {
    symbol: String,
    duration_ns: i64,
    start_datetime_ns: i64,
    end_datetime_ns: i64,
    rows: Vec<Kline>,
}

impl KlineDataSeries {
    fn new(
        symbol: String,
        duration_ns: i64,
        start_datetime_ns: i64,
        end_datetime_ns: i64,
        rows: Vec<Kline>,
    ) -> Self {
        Self {
            symbol,
            duration_ns,
            start_datetime_ns,
            end_datetime_ns,
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
    pub fn start_datetime_ns(&self) -> i64 {
        self.start_datetime_ns
    }

    #[must_use]
    pub fn end_datetime_ns(&self) -> i64 {
        self.end_datetime_ns
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

/// Request for a one-shot owned tick history series in `[start_datetime_ns, end_datetime_ns)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TickDataSeriesRequest {
    symbol: String,
    start_datetime_ns: i64,
    end_datetime_ns: i64,
    page_view_width: usize,
    timeout: Duration,
}

impl TickDataSeriesRequest {
    #[must_use]
    pub fn new(symbol: impl Into<String>, start_datetime_ns: i64, end_datetime_ns: i64) -> Self {
        Self {
            symbol: symbol.into(),
            start_datetime_ns,
            end_datetime_ns,
            page_view_width: DEFAULT_HISTORY_PAGE_VIEW_WIDTH,
            timeout: DEFAULT_HISTORY_REQUEST_TIMEOUT,
        }
    }

    #[must_use]
    pub fn with_page_view_width(mut self, page_view_width: usize) -> Self {
        self.page_view_width = page_view_width;
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
    pub fn start_datetime_ns(&self) -> i64 {
        self.start_datetime_ns
    }

    #[must_use]
    pub fn end_datetime_ns(&self) -> i64 {
        self.end_datetime_ns
    }

    #[must_use]
    pub fn page_view_width(&self) -> usize {
        self.page_view_width
    }

    #[must_use]
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    fn validate(&self) -> Result<TickDataSeriesSpec> {
        if self.symbol.is_empty() {
            return Err(DataError::Validation(
                "symbol must not be empty".to_string(),
            ));
        }
        if self.end_datetime_ns <= self.start_datetime_ns {
            return Err(DataError::Validation(
                "end_datetime_ns must be greater than start_datetime_ns".to_string(),
            ));
        }
        Ok(TickDataSeriesSpec {
            start_datetime_ns: self.start_datetime_ns,
            end_datetime_ns: self.end_datetime_ns,
            page_view_width: normalize_history_view_width(self.page_view_width)?,
        })
    }
}

/// Owned result of a one-shot tick history series.
#[derive(Debug, Clone, Default)]
pub struct TickDataSeries {
    symbol: String,
    start_datetime_ns: i64,
    end_datetime_ns: i64,
    rows: Vec<Tick>,
}

impl TickDataSeries {
    fn new(symbol: String, start_datetime_ns: i64, end_datetime_ns: i64, rows: Vec<Tick>) -> Self {
        Self {
            symbol,
            start_datetime_ns,
            end_datetime_ns,
            rows,
        }
    }

    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    #[must_use]
    pub fn start_datetime_ns(&self) -> i64 {
        self.start_datetime_ns
    }

    #[must_use]
    pub fn end_datetime_ns(&self) -> i64 {
        self.end_datetime_ns
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
    pub fn last(&self) -> Option<&Tick> {
        self.rows.last()
    }

    #[must_use]
    pub fn get(&self, index: usize) -> Option<&Tick> {
        self.rows.get(index)
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = &Tick> + DoubleEndedIterator {
        self.rows.iter()
    }

    #[must_use]
    pub fn rows(&self) -> &[Tick] {
        &self.rows
    }

    #[must_use]
    pub fn into_rows(self) -> Vec<Tick> {
        self.rows
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct KlineDataPageSpec {
    duration_ns: i64,
    view_width: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TickDataPageSpec {
    view_width: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct KlineDataSeriesSpec {
    duration_ns: i64,
    start_datetime_ns: i64,
    end_datetime_ns: i64,
    page_view_width: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TickDataSeriesSpec {
    start_datetime_ns: i64,
    end_datetime_ns: i64,
    page_view_width: usize,
}

/// Thin research/offline data wrapper over [`tqsdk_session::SessionClient`].
#[derive(Clone)]
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

    fn require_session(&self, message: &'static str) -> Result<&tqsdk_session::SessionClient> {
        self.session
            .as_ref()
            .ok_or(DataError::InvalidState(message))
    }

    pub(crate) fn is_session_backed(&self) -> bool {
        self.session.is_some()
    }

    pub(crate) fn require_history_download_permission(&self) -> Result<()> {
        let Some(session) = self.session.as_ref() else {
            return Ok(());
        };
        let Some(auth_context) = session.auth_context()? else {
            return Ok(());
        };
        let Some(features) = auth_context.get("features").and_then(Value::as_array) else {
            return Ok(());
        };
        if features
            .iter()
            .filter_map(Value::as_str)
            .any(|feature| feature == "tq_dl")
        {
            Ok(())
        } else {
            Err(DataError::PermissionDenied(
                HISTORY_DOWNLOAD_PERMISSION_MESSAGE.to_string(),
            ))
        }
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

    pub async fn get_kline_data_page(
        &self,
        request: KlineDataPageRequest,
    ) -> Result<KlineDataPage> {
        let spec = request.validate()?;
        let session =
            self.require_session("get_kline_data_page requires a session-backed data client")?;
        self.require_history_download_permission()?;
        let chart_id = next_kline_page_chart_id(request.symbol(), spec.duration_ns);
        let result = self
            .await_kline_data_page(session, &request, spec, chart_id.as_str())
            .await;
        cancel_chart_best_effort(session, chart_id).await;
        result
    }

    pub async fn get_tick_data_page(&self, request: TickDataPageRequest) -> Result<TickDataPage> {
        let spec = request.validate()?;
        let session =
            self.require_session("get_tick_data_page requires a session-backed data client")?;
        self.require_history_download_permission()?;
        let chart_id = next_tick_page_chart_id(request.symbol());
        let result = self
            .await_tick_data_page(session, &request, spec, chart_id.as_str())
            .await;
        cancel_chart_best_effort(session, chart_id).await;
        result
    }

    pub async fn get_kline_data_series(
        &self,
        request: KlineDataSeriesRequest,
    ) -> Result<KlineDataSeries> {
        let spec = request.validate()?;
        self.require_session("get_kline_data_series requires a session-backed data client")?;
        self.require_history_download_permission()?;

        let mut rows = Vec::new();
        let mut last_next_left_kline_id = None;
        let mut next_left_kline_id = None;
        let mut use_focus = true;

        loop {
            let mut page_request = KlineDataPageRequest::new(
                request.symbol(),
                request.duration(),
                spec.page_view_width,
            )
            .with_timeout(request.timeout());
            if use_focus {
                page_request = page_request
                    .with_focus_datetime_ns(spec.start_datetime_ns)
                    .with_focus_position(0);
            } else if let Some(left_kline_id) = next_left_kline_id {
                page_request = page_request.with_left_kline_id(left_kline_id);
            }

            let page = self.get_kline_data_page(page_request).await?;
            let page_len = page.len();
            let Some(new_next_left_kline_id) = extend_kline_rows_in_window(
                &mut rows,
                page.into_rows(),
                spec.start_datetime_ns,
                spec.end_datetime_ns,
            ) else {
                break;
            };

            if last_next_left_kline_id == Some(new_next_left_kline_id)
                || page_len < spec.page_view_width
            {
                break;
            }

            last_next_left_kline_id = Some(new_next_left_kline_id);
            next_left_kline_id = Some(new_next_left_kline_id);
            use_focus = false;
        }

        Ok(KlineDataSeries::new(
            request.symbol().to_string(),
            spec.duration_ns,
            spec.start_datetime_ns,
            spec.end_datetime_ns,
            dedup_sort_klines_by_id(rows),
        ))
    }

    pub async fn get_tick_data_series(
        &self,
        request: TickDataSeriesRequest,
    ) -> Result<TickDataSeries> {
        let spec = request.validate()?;
        self.require_session("get_tick_data_series requires a session-backed data client")?;
        self.require_history_download_permission()?;

        let mut rows = Vec::new();
        let mut last_next_left_id = None;
        let mut next_left_id = None;
        let mut use_focus = true;

        loop {
            let mut page_request = TickDataPageRequest::new(request.symbol(), spec.page_view_width)
                .with_timeout(request.timeout());
            if use_focus {
                page_request = page_request
                    .with_focus_datetime_ns(spec.start_datetime_ns)
                    .with_focus_position(0);
            } else if let Some(left_id) = next_left_id {
                page_request = page_request.with_left_id(left_id);
            }

            let page = self.get_tick_data_page(page_request).await?;
            let page_len = page.len();
            let Some(new_next_left_id) = extend_tick_rows_in_window(
                &mut rows,
                page.into_rows(),
                spec.start_datetime_ns,
                spec.end_datetime_ns,
            ) else {
                break;
            };

            if last_next_left_id == Some(new_next_left_id) || page_len < spec.page_view_width {
                break;
            }

            last_next_left_id = Some(new_next_left_id);
            next_left_id = Some(new_next_left_id);
            use_focus = false;
        }

        Ok(TickDataSeries::new(
            request.symbol().to_string(),
            spec.start_datetime_ns,
            spec.end_datetime_ns,
            dedup_sort_ticks_by_id(rows),
        ))
    }

    async fn await_kline_data_page(
        &self,
        session: &tqsdk_session::SessionClient,
        request: &KlineDataPageRequest,
        spec: KlineDataPageSpec,
        chart_id: &str,
    ) -> Result<KlineDataPage> {
        let command_id = session
            .submit(RuntimeCommand::Market(MarketCommand::SetChart(
                MarketChartCommand {
                    chart_id: chart_id.to_string(),
                    symbols: vec![Symbol::new(request.symbol())],
                    duration_ns: spec.duration_ns,
                    view_width: spec.view_width,
                    left_kline_id: request.left_kline_id(),
                    focus_datetime_ns: request.focus_datetime_ns(),
                    focus_position: request.focus_position(),
                },
            )))
            .await?;
        let reader = session.reader_clone();
        wait_for_ready_chart(session, &reader, chart_id, command_id, request.timeout()).await?;
        read_ready_kline_data_page(
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
        let command_id = session
            .submit(RuntimeCommand::Market(MarketCommand::SetChart(
                MarketChartCommand {
                    chart_id: chart_id.to_string(),
                    symbols: vec![Symbol::new(request.symbol())],
                    duration_ns: 0,
                    view_width: spec.view_width,
                    left_kline_id: request.left_id(),
                    focus_datetime_ns: request.focus_datetime_ns(),
                    focus_position: request.focus_position(),
                },
            )))
            .await?;
        let reader = session.reader_clone();
        wait_for_ready_chart(session, &reader, chart_id, command_id, request.timeout()).await?;
        read_ready_tick_data_page(&reader, request.symbol(), spec.view_width, chart_id)?.ok_or_else(
            || DataError::InvalidResponse("ready tick chart snapshot missing".to_string()),
        )
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

async fn wait_for_ready_chart(
    session: &tqsdk_session::SessionClient,
    reader: &tqsdk_core::RuntimeReader,
    chart_id: &str,
    command_id: tqsdk_core::CommandId,
    timeout: Duration,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        if chart_is_ready(reader, chart_id)? {
            return Ok(());
        }

        if let Some(status) = session.command_status(command_id)?
            && matches!(status.as_str(), "rejected" | "failed" | "cancelled")
        {
            return Err(DataError::InvalidResponse(format!(
                "set chart command reached terminal status {status}"
            )));
        }

        if tokio::time::Instant::now() >= deadline {
            return Err(DataError::Timeout(timeout));
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
            return Err(DataError::Timeout(timeout));
        }

        tokio::time::sleep(remaining.min(Duration::from_millis(1))).await;
    }
}

fn chart_is_ready(reader: &tqsdk_core::RuntimeReader, chart_id: &str) -> Result<bool> {
    let snapshot = reader.read();
    let Some(chart) = snapshot
        .decode_path::<Chart>(&["charts", chart_id])
        .map_err(contract_error_into_data)?
    else {
        return Ok(false);
    };
    Ok(chart.ready && !chart.more_data)
}

fn read_ready_kline_data_page(
    reader: &tqsdk_core::RuntimeReader,
    symbol: &str,
    duration_ns: i64,
    view_width: usize,
    chart_id: &str,
) -> Result<Option<KlineDataPage>> {
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
    let data_path = ["klines", symbol, duration_key.as_str(), "data"];
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
    if ids.len() > view_width {
        ids.drain(0..ids.len() - view_width);
    }

    let mut rows = Vec::with_capacity(ids.len());
    for id in ids {
        let id_key = id.to_string();
        if let Some(row) = snapshot
            .decode_path::<Kline>(&[
                "klines",
                symbol,
                duration_key.as_str(),
                "data",
                id_key.as_str(),
            ])
            .map_err(contract_error_into_data)?
        {
            rows.push(row);
        }
    }

    Ok(Some(KlineDataPage::new(
        symbol.to_string(),
        duration_ns,
        view_width,
        chart.left_id,
        chart.right_id,
        rows,
    )))
}

fn read_ready_tick_data_page(
    reader: &tqsdk_core::RuntimeReader,
    symbol: &str,
    view_width: usize,
    chart_id: &str,
) -> Result<Option<TickDataPage>> {
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

    let mut ids = snapshot
        .get_path(&["ticks", symbol, "data"])
        .and_then(|value| value.as_object())
        .map(|data| {
            data.keys()
                .filter_map(|key| key.parse::<i64>().ok())
                .filter(|id| chart.left_id <= *id && *id <= chart.right_id)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    ids.sort_unstable();
    if ids.len() > view_width {
        ids.drain(0..ids.len() - view_width);
    }

    let mut rows = Vec::with_capacity(ids.len());
    for id in ids {
        let id_key = id.to_string();
        if let Some(row) = snapshot
            .decode_path::<Tick>(&["ticks", symbol, "data", id_key.as_str()])
            .map_err(contract_error_into_data)?
        {
            rows.push(row);
        }
    }

    Ok(Some(TickDataPage::new(
        symbol.to_string(),
        view_width,
        chart.left_id,
        chart.right_id,
        rows,
    )))
}

fn next_kline_page_chart_id(symbol: &str, duration_ns: i64) -> String {
    let sequence = NEXT_HISTORY_CHART_ID.fetch_add(1, Ordering::Relaxed);
    format!(
        "data-kline-page-{}-{duration_ns}-{sequence}",
        sanitize_chart_token(symbol)
    )
}

fn next_tick_page_chart_id(symbol: &str) -> String {
    let sequence = NEXT_HISTORY_CHART_ID.fetch_add(1, Ordering::Relaxed);
    format!("data-tick-page-{}-{sequence}", sanitize_chart_token(symbol))
}

fn normalize_history_view_width(view_width: usize) -> Result<usize> {
    if view_width == 0 {
        return Err(DataError::Validation(
            "view_width must be greater than zero".to_string(),
        ));
    }
    Ok(view_width.min(MAX_HISTORY_VIEW_WIDTH))
}

async fn cancel_chart_best_effort(session: &tqsdk_session::SessionClient, chart_id: String) {
    let _ = session
        .submit(RuntimeCommand::Market(MarketCommand::CancelChart {
            chart_id,
        }))
        .await;
}

fn extend_kline_rows_in_window(
    target: &mut Vec<Kline>,
    page: Vec<Kline>,
    start_datetime_ns: i64,
    end_datetime_ns: i64,
) -> Option<i64> {
    let mut next_left_kline_id = None;
    for row in page {
        if row.datetime == 0 || row.datetime >= end_datetime_ns {
            break;
        }
        next_left_kline_id = row.id.checked_add(1);
        if row.datetime >= start_datetime_ns {
            target.push(row);
        }
    }
    next_left_kline_id
}

fn extend_tick_rows_in_window(
    target: &mut Vec<Tick>,
    page: Vec<Tick>,
    start_datetime_ns: i64,
    end_datetime_ns: i64,
) -> Option<i64> {
    let mut next_left_id = None;
    for row in page {
        if row.datetime == 0 || row.datetime >= end_datetime_ns {
            break;
        }
        next_left_id = row.id.checked_add(1);
        if row.datetime >= start_datetime_ns {
            target.push(row);
        }
    }
    next_left_id
}

fn dedup_sort_klines_by_id(rows: Vec<Kline>) -> Vec<Kline> {
    let mut by_id = BTreeMap::new();
    for row in rows {
        by_id.insert(row.id, row);
    }
    by_id.into_values().collect()
}

fn dedup_sort_ticks_by_id(rows: Vec<Tick>) -> Vec<Tick> {
    let mut by_id = BTreeMap::new();
    for row in rows {
        by_id.insert(row.id, row);
    }
    by_id.into_values().collect()
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
        let next_left_kline_id = extend_kline_rows_in_window(
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
        let next_left_id = extend_tick_rows_in_window(
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
        let rows = dedup_sort_klines_by_id(vec![
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
        let rows = dedup_sort_ticks_by_id(vec![
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

    fn seed_ready_tick_chart(
        handle: &RuntimeHandle,
        chart_id: &str,
        symbol: &str,
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
                                        "duration": 0,
                                    },
                                    "left_id": left_id,
                                    "right_id": right_id,
                                    "more_data": false,
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

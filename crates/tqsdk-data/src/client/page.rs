use std::time::Duration;

use tqsdk_core::{Kline, Tick};

use crate::error::{DataError, Result};
use crate::market_cache::{MarketCacheEvent, MarketCacheReplay};

use super::{
    DEFAULT_HISTORY_PAGE_VIEW_WIDTH, DEFAULT_HISTORY_REQUEST_TIMEOUT,
    normalize_history_view_width,
};

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

    pub(super) fn validate(&self) -> Result<KlineDataPageSpec> {
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
    more_data: bool,
    rows: Vec<Kline>,
}

impl KlineDataPage {
    pub(crate) fn new(
        symbol: String,
        duration_ns: i64,
        view_width: usize,
        chart_left_id: i64,
        chart_right_id: i64,
        more_data: bool,
        rows: Vec<Kline>,
    ) -> Self {
        Self {
            symbol,
            duration_ns,
            view_width,
            chart_left_id,
            chart_right_id,
            more_data,
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
    pub fn more_data(&self) -> bool {
        self.more_data
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

    pub(super) fn validate(&self) -> Result<TickDataPageSpec> {
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
    more_data: bool,
    rows: Vec<Tick>,
}

impl TickDataPage {
    pub(crate) fn new(
        symbol: String,
        view_width: usize,
        chart_left_id: i64,
        chart_right_id: i64,
        more_data: bool,
        rows: Vec<Tick>,
    ) -> Self {
        Self {
            symbol,
            view_width,
            chart_left_id,
            chart_right_id,
            more_data,
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
    pub fn more_data(&self) -> bool {
        self.more_data
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

    pub(super) fn validate(&self) -> Result<KlineDataSeriesSpec> {
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
    pub(super) fn new(
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

    pub fn into_market_cache_events(
        self,
        source: impl AsRef<str>,
    ) -> Result<Vec<MarketCacheEvent>> {
        let source = source.as_ref();
        let symbol = self.symbol;
        let duration_ns = self.duration_ns;
        self.rows
            .into_iter()
            .map(|row| {
                MarketCacheEvent::kline(
                    source,
                    symbol.as_str(),
                    row.datetime,
                    Some(row.datetime),
                    duration_ns,
                    row,
                )
            })
            .collect()
    }

    pub fn into_market_cache_replay(self, source: impl AsRef<str>) -> Result<MarketCacheReplay> {
        Ok(MarketCacheReplay::new(
            self.into_market_cache_events(source)?,
        ))
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

    pub(super) fn validate(&self) -> Result<TickDataSeriesSpec> {
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
    pub(super) fn new(
        symbol: String,
        start_datetime_ns: i64,
        end_datetime_ns: i64,
        rows: Vec<Tick>,
    ) -> Self {
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

    pub fn into_market_cache_events(
        self,
        source: impl AsRef<str>,
    ) -> Result<Vec<MarketCacheEvent>> {
        let source = source.as_ref();
        let symbol = self.symbol;
        self.rows
            .into_iter()
            .map(|row| {
                MarketCacheEvent::tick(
                    source,
                    symbol.as_str(),
                    row.datetime,
                    Some(row.datetime),
                    row,
                )
            })
            .collect()
    }

    pub fn into_market_cache_replay(self, source: impl AsRef<str>) -> Result<MarketCacheReplay> {
        Ok(MarketCacheReplay::new(
            self.into_market_cache_events(source)?,
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct KlineDataPageSpec {
    pub(super) duration_ns: i64,
    pub(super) view_width: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TickDataPageSpec {
    pub(super) view_width: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct KlineDataSeriesSpec {
    pub(super) duration_ns: i64,
    pub(super) start_datetime_ns: i64,
    pub(super) end_datetime_ns: i64,
    pub(super) page_view_width: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TickDataSeriesSpec {
    pub(super) start_datetime_ns: i64,
    pub(super) end_datetime_ns: i64,
    pub(super) page_view_width: usize,
}

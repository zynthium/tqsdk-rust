use std::collections::HashMap;
use std::time::Duration;

use chrono::{FixedOffset, NaiveDate, TimeZone, Utc};
use tqsdk_task::backtest::StrategyBacktest;
use tqsdk_task::replay::{ReplayMarketSource, StrategyReplaySourceBuilder};

use super::{Result, Tq, data_validation};

const NANOS_PER_SECOND: i64 = 1_000_000_000;
const NANOS_PER_DAY: i64 = 86_400 * NANOS_PER_SECOND;
const TRADING_DAY_START_OFFSET_NS: i64 = 6 * 60 * 60 * NANOS_PER_SECOND;
const TRADING_DAY_END_OFFSET_NS: i64 = 18 * 60 * 60 * NANOS_PER_SECOND;
const CST_OFFSET_SECONDS: i32 = 8 * 60 * 60;
const CST_1990_01_01_NS: i64 = 631_123_200_000_000_000;

#[derive(Debug, Clone, Default)]
pub(super) struct LocalBacktestRecipe {
    quote_symbols: Vec<String>,
    price_ticks: HashMap<String, f64>,
    instrument_specs: Vec<tqsdk_session::InstrumentSpec>,
    default_price_tick: Option<f64>,
}

impl LocalBacktestRecipe {
    #[must_use]
    pub(super) fn quote_symbol(mut self, symbol: impl Into<String>) -> Self {
        self.quote_symbols.push(symbol.into());
        self
    }

    #[must_use]
    pub(super) fn price_tick(mut self, symbol: impl Into<String>, tick: f64) -> Self {
        self.price_ticks.insert(symbol.into(), tick);
        self
    }

    #[must_use]
    pub(super) fn instrument_spec(mut self, spec: tqsdk_session::InstrumentSpec) -> Self {
        self.instrument_specs.push(spec);
        self
    }

    #[must_use]
    pub(super) fn instrument_specs(
        mut self,
        specs: impl IntoIterator<Item = tqsdk_session::InstrumentSpec>,
    ) -> Self {
        self.instrument_specs.extend(specs);
        self
    }

    #[must_use]
    pub(super) fn default_price_tick(mut self, tick: f64) -> Self {
        self.default_price_tick = Some(tick);
        self
    }

    pub(super) fn declared_quote_minute_history_requests(
        &self,
        start_datetime_ns: i64,
        end_datetime_ns: i64,
    ) -> Result<Vec<tqsdk_data::KlineDataSeriesRequest>> {
        declared_quote_minute_history_requests(
            &self.quote_symbols,
            start_datetime_ns,
            end_datetime_ns,
        )
    }

    pub(super) async fn connect(self, replay: ReplayMarketSource) -> Result<Tq> {
        let mut builder = StrategyBacktest::builder(replay);
        if let Some(default_price_tick) = self.default_price_tick {
            builder = builder.default_price_tick(default_price_tick);
        }
        builder = builder.instrument_specs(self.instrument_specs);
        for symbol in &self.quote_symbols {
            builder = builder.quote(symbol);
        }
        for (symbol, tick) in &self.price_ticks {
            builder = builder.price_tick(symbol, *tick);
        }
        let backtest = builder.build().await?;
        Ok(Tq::from_local_backtest(backtest))
    }
}

pub(super) fn replay_from_klines(
    series: impl IntoIterator<Item = tqsdk_data::KlineDataSeries>,
) -> Result<ReplayMarketSource> {
    let mut builder = StrategyReplaySourceBuilder::new();
    for series in series {
        builder = builder.kline_series(series, "history-kline")?;
    }
    Ok(builder.build())
}

pub(super) fn replay_from_klines_as(
    replay_symbol: impl AsRef<str>,
    series: impl IntoIterator<Item = tqsdk_data::KlineDataSeries>,
) -> Result<ReplayMarketSource> {
    let replay_symbol = replay_symbol.as_ref().to_owned();
    let mut builder = StrategyReplaySourceBuilder::new();
    for series in series {
        builder = builder.kline_series_as(series, replay_symbol.as_str(), "history-kline")?;
    }
    Ok(builder.build())
}

pub(super) async fn fetch_kline_series(
    data: &tqsdk_data::DataClient,
    requests: impl IntoIterator<Item = tqsdk_data::KlineDataSeriesRequest>,
) -> Result<Vec<tqsdk_data::KlineDataSeries>> {
    let mut series = Vec::new();
    for request in requests {
        series.push(data.get_kline_data_series(request).await?);
    }
    Ok(series)
}

pub(super) fn minute_history_request(
    symbol: impl Into<String>,
    start_datetime_ns: i64,
    end_datetime_ns: i64,
) -> tqsdk_data::KlineDataSeriesRequest {
    tqsdk_data::KlineDataSeriesRequest::new(
        symbol,
        Duration::from_secs(60),
        start_datetime_ns,
        end_datetime_ns,
    )
}

pub(super) fn declared_quote_minute_history_requests(
    symbols: &[String],
    start_datetime_ns: i64,
    end_datetime_ns: i64,
) -> Result<Vec<tqsdk_data::KlineDataSeriesRequest>> {
    if symbols.is_empty() {
        return Err(data_validation(
            "local_backtest_quote_minute_history requires at least one quote_symbol",
        ));
    }
    if end_datetime_ns <= start_datetime_ns {
        return Err(data_validation(
            "end_datetime_ns must be greater than start_datetime_ns",
        ));
    }

    let mut requests: Vec<tqsdk_data::KlineDataSeriesRequest> = Vec::new();
    for symbol in symbols {
        if symbol.is_empty() {
            return Err(data_validation("quote_symbol must not be empty"));
        }
        if requests.iter().any(|request| request.symbol() == symbol) {
            continue;
        }
        requests.push(minute_history_request(
            symbol.clone(),
            start_datetime_ns,
            end_datetime_ns,
        ));
    }
    Ok(requests)
}

pub(super) async fn continuous_minute_history_requests(
    data: &tqsdk_data::DataClient,
    symbol: &str,
    start_datetime_ns: i64,
    end_datetime_ns: i64,
) -> Result<Vec<tqsdk_data::KlineDataSeriesRequest>> {
    if symbol.is_empty() {
        return Err(data_validation("continuous symbol must not be empty"));
    }
    if end_datetime_ns <= start_datetime_ns {
        return Err(data_validation(
            "end_datetime_ns must be greater than start_datetime_ns",
        ));
    }
    let trading_start = trading_day_from_timestamp_ns(start_datetime_ns)?;
    let end_inclusive_ns = end_datetime_ns.checked_sub(1).ok_or_else(|| {
        data_validation("end_datetime_ns is too small to compute an inclusive end")
    })?;
    let trading_end = trading_day_from_timestamp_ns(end_inclusive_ns)?;
    let trading_days = data.query_trading_days(trading_start, trading_end).await?;
    if trading_days.is_empty() {
        return Ok(Vec::new());
    }

    let segments = data
        .query_his_cont_underlying_segments(symbol, trading_days.len(), Some(trading_end))
        .await?;
    continuous_minute_history_requests_for_segments(
        symbol,
        start_datetime_ns,
        end_datetime_ns,
        &segments,
    )
}

fn continuous_minute_history_requests_for_segments(
    symbol: &str,
    start_datetime_ns: i64,
    end_datetime_ns: i64,
    segments: &[tqsdk_data::HistoricalContUnderlyingSegment],
) -> Result<Vec<tqsdk_data::KlineDataSeriesRequest>> {
    if symbol.is_empty() {
        return Err(data_validation("continuous symbol must not be empty"));
    }
    if end_datetime_ns <= start_datetime_ns {
        return Err(data_validation(
            "end_datetime_ns must be greater than start_datetime_ns",
        ));
    }

    let mut requests: Vec<tqsdk_data::KlineDataSeriesRequest> = Vec::new();
    for segment in segments {
        if segment.symbol != symbol {
            return Err(data_validation(format!(
                "continuous segment symbol {} does not match requested {symbol}",
                segment.symbol
            )));
        }
        if segment.underlying.is_empty() {
            continue;
        }

        let segment_start_date = parse_segment_date(&segment.start_date)?;
        let segment_end_date = parse_segment_date(&segment.end_date)?;
        let segment_start_ns = trading_day_start_time_ns(segment_start_date)?;
        let segment_end_ns = trading_day_end_time_ns(segment_end_date)?;
        let request_start = start_datetime_ns.max(segment_start_ns);
        let request_end = end_datetime_ns.min(segment_end_ns);
        if request_start < request_end {
            requests.push(minute_history_request(
                segment.underlying.clone(),
                request_start,
                request_end,
            ));
        }
    }

    Ok(requests)
}

pub(super) fn replay_from_ticks(
    series: impl IntoIterator<Item = tqsdk_data::TickDataSeries>,
) -> Result<ReplayMarketSource> {
    let mut builder = StrategyReplaySourceBuilder::new();
    for series in series {
        builder = builder.tick_series(series, "history-tick")?;
    }
    Ok(builder.build())
}

pub(super) fn replay_from_ticks_as(
    replay_symbol: impl AsRef<str>,
    series: impl IntoIterator<Item = tqsdk_data::TickDataSeries>,
) -> Result<ReplayMarketSource> {
    let replay_symbol = replay_symbol.as_ref().to_owned();
    let mut builder = StrategyReplaySourceBuilder::new();
    for series in series {
        builder = builder.tick_series_as(series, replay_symbol.as_str(), "history-tick")?;
    }
    Ok(builder.build())
}

pub(super) async fn fetch_tick_series(
    data: &tqsdk_data::DataClient,
    requests: impl IntoIterator<Item = tqsdk_data::TickDataSeriesRequest>,
) -> Result<Vec<tqsdk_data::TickDataSeries>> {
    let mut series = Vec::new();
    for request in requests {
        series.push(data.get_tick_data_series(request).await?);
    }
    Ok(series)
}

fn trading_day_from_timestamp_ns(timestamp_ns: i64) -> Result<NaiveDate> {
    let elapsed = timestamp_ns
        .checked_sub(CST_1990_01_01_NS)
        .ok_or_else(|| data_validation("timestamp is before supported trading-day base"))?;
    let mut days = elapsed.div_euclid(NANOS_PER_DAY);
    if elapsed.rem_euclid(NANOS_PER_DAY) >= TRADING_DAY_END_OFFSET_NS {
        days += 1;
    }
    let week_day = days.rem_euclid(7);
    if week_day >= 5 {
        days += 7 - week_day;
    }
    let trading_day_ns = CST_1990_01_01_NS
        .checked_add(days.checked_mul(NANOS_PER_DAY).ok_or_else(|| {
            data_validation("trading-day timestamp overflowed while scaling days")
        })?)
        .ok_or_else(|| data_validation("trading-day timestamp overflowed"))?;
    timestamp_ns_to_cst_date(trading_day_ns)
}

fn trading_day_start_time_ns(trading_day: NaiveDate) -> Result<i64> {
    let mut start_time = cst_midnight_ns(trading_day)?
        .checked_sub(TRADING_DAY_START_OFFSET_NS)
        .ok_or_else(|| data_validation("trading-day start timestamp underflowed"))?;
    let elapsed = start_time
        .checked_sub(CST_1990_01_01_NS)
        .ok_or_else(|| data_validation("trading-day start is before supported base"))?;
    let week_day = elapsed.div_euclid(NANOS_PER_DAY).rem_euclid(7);
    if week_day >= 5 {
        start_time = start_time
            .checked_sub((week_day - 4) * NANOS_PER_DAY)
            .ok_or_else(|| data_validation("weekend-adjusted trading-day start underflowed"))?;
    }
    Ok(start_time)
}

fn trading_day_end_time_ns(trading_day: NaiveDate) -> Result<i64> {
    cst_midnight_ns(trading_day)?
        .checked_add(TRADING_DAY_END_OFFSET_NS)
        .ok_or_else(|| data_validation("trading-day end timestamp overflowed"))
}

fn parse_segment_date(value: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|error| data_validation(format!("invalid segment date {value}: {error}")))
}

fn cst_midnight_ns(date: NaiveDate) -> Result<i64> {
    let midnight = date
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| data_validation("failed to build CST midnight"))?;
    let cst = cst_offset();
    let local = cst
        .from_local_datetime(&midnight)
        .single()
        .ok_or_else(|| data_validation("failed to resolve CST midnight"))?;
    local
        .timestamp()
        .checked_mul(NANOS_PER_SECOND)
        .ok_or_else(|| data_validation("CST midnight timestamp overflowed"))
}

fn timestamp_ns_to_cst_date(timestamp_ns: i64) -> Result<NaiveDate> {
    let seconds = timestamp_ns.div_euclid(NANOS_PER_SECOND);
    let nanos = timestamp_ns.rem_euclid(NANOS_PER_SECOND) as u32;
    let utc = Utc
        .timestamp_opt(seconds, nanos)
        .single()
        .ok_or_else(|| data_validation("failed to resolve timestamp"))?;
    Ok(utc.with_timezone(&cst_offset()).date_naive())
}

fn cst_offset() -> FixedOffset {
    FixedOffset::east_opt(CST_OFFSET_SECONDS).expect("CST offset must be valid")
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use tqsdk_core::{Kline, Symbol};
    use tqsdk_task::ReplayMarketEvent;

    use super::{
        LocalBacktestRecipe, continuous_minute_history_requests_for_segments,
        declared_quote_minute_history_requests, trading_day_from_timestamp_ns,
        trading_day_start_time_ns,
    };
    use crate::Error;

    #[test]
    fn declared_quote_minute_history_requests_dedupes_declared_symbols() {
        let start = cst_datetime_ns(2026, 5, 18, 9, 0, 0);
        let end = cst_datetime_ns(2026, 5, 18, 10, 0, 0);
        let symbols = vec![
            "SHFE.rb2601".to_string(),
            "SHFE.rb2601".to_string(),
            "DCE.i2601".to_string(),
        ];

        let requests = declared_quote_minute_history_requests(&symbols, start, end).unwrap();

        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].symbol(), "SHFE.rb2601");
        assert_eq!(requests[0].duration(), std::time::Duration::from_secs(60));
        assert_eq!(requests[0].start_datetime_ns(), start);
        assert_eq!(requests[0].end_datetime_ns(), end);
        assert_eq!(requests[1].symbol(), "DCE.i2601");
        assert_eq!(requests[1].duration(), std::time::Duration::from_secs(60));
    }

    #[test]
    fn continuous_minute_history_requests_clip_underlying_segments_to_backtest_window() {
        let start = cst_datetime_ns(2026, 5, 15, 21, 0, 0);
        let end = cst_datetime_ns(2026, 5, 20, 10, 0, 0);
        let segments = [
            tqsdk_data::HistoricalContUnderlyingSegment {
                symbol: "KQ.m@SHFE.rb".to_string(),
                underlying: "SHFE.rb2601".to_string(),
                start_date: "2026-05-15".to_string(),
                end_date: "2026-05-18".to_string(),
                trading_days: 2,
            },
            tqsdk_data::HistoricalContUnderlyingSegment {
                symbol: "KQ.m@SHFE.rb".to_string(),
                underlying: "SHFE.rb2605".to_string(),
                start_date: "2026-05-19".to_string(),
                end_date: "2026-05-20".to_string(),
                trading_days: 2,
            },
        ];

        let requests =
            continuous_minute_history_requests_for_segments("KQ.m@SHFE.rb", start, end, &segments)
                .unwrap();

        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].symbol(), "SHFE.rb2601");
        assert_eq!(requests[0].duration(), std::time::Duration::from_secs(60));
        assert_eq!(requests[0].start_datetime_ns(), start);
        assert_eq!(
            requests[0].end_datetime_ns(),
            cst_datetime_ns(2026, 5, 18, 18, 0, 0)
        );
        assert_eq!(requests[1].symbol(), "SHFE.rb2605");
        assert_eq!(
            requests[1].start_datetime_ns(),
            cst_datetime_ns(2026, 5, 18, 18, 0, 0)
        );
        assert_eq!(requests[1].end_datetime_ns(), end);
    }

    #[test]
    fn continuous_minute_history_uses_official_style_trading_day_boundaries() {
        let friday_night = cst_datetime_ns(2026, 5, 15, 21, 0, 0);
        let trading_day = trading_day_from_timestamp_ns(friday_night).unwrap();
        assert_eq!(trading_day, NaiveDate::from_ymd_opt(2026, 5, 18).unwrap());
        assert_eq!(
            trading_day_start_time_ns(trading_day).unwrap(),
            cst_datetime_ns(2026, 5, 15, 18, 0, 0)
        );
    }

    #[test]
    fn continuous_minute_history_rejects_mismatched_segments() {
        let start = cst_datetime_ns(2026, 5, 18, 9, 0, 0);
        let end = cst_datetime_ns(2026, 5, 18, 10, 0, 0);
        let segments = [tqsdk_data::HistoricalContUnderlyingSegment {
            symbol: "KQ.m@SHFE.au".to_string(),
            underlying: "SHFE.au2601".to_string(),
            start_date: "2026-05-18".to_string(),
            end_date: "2026-05-18".to_string(),
            trading_days: 1,
        }];

        let result =
            continuous_minute_history_requests_for_segments("KQ.m@SHFE.rb", start, end, &segments);

        assert!(matches!(result, Err(Error::Data(_))));
    }

    #[tokio::test]
    async fn local_backtest_recipe_connect_applies_instrument_specs() {
        let replay = tqsdk_task::ReplayMarketSource::new(vec![
            ReplayMarketEvent::kline(
                "fixture",
                "SHFE.rb2501",
                1_000,
                Some(1_000),
                60_000_000_000,
                Kline {
                    id: 1,
                    datetime: 1_000,
                    open: 100.0,
                    high: 105.0,
                    low: 99.0,
                    close: 102.0,
                    volume: 10,
                    ..Kline::default()
                },
            )
            .unwrap(),
        ]);

        let mut tq = LocalBacktestRecipe::default()
            .instrument_spec(instrument_spec("SHFE.rb2501", 0.5, 10))
            .connect(replay)
            .await
            .unwrap();
        let quote = tq.quote("SHFE.rb2501").await.unwrap();

        assert!(tq.next().await.unwrap());
        let quote = quote.load().unwrap();
        assert_eq!(quote.last_price, 102.0);
        assert_eq!(quote.ask_price1, 102.5);
        assert_eq!(quote.bid_price1, 101.5);
    }

    fn instrument_spec(
        symbol: &str,
        price_tick: f64,
        volume_multiple: i64,
    ) -> tqsdk_session::InstrumentSpec {
        tqsdk_session::InstrumentSpec {
            symbol: Symbol::new(symbol),
            exchange_id: "SHFE".to_string(),
            product_id: "rb".to_string(),
            class: tqsdk_session::InstrumentClass::Future,
            price_tick,
            volume_multiple,
            expire_datetime_secs: None,
            underlying_symbol: None,
        }
    }

    fn cst_datetime_ns(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
    ) -> i64 {
        use chrono::TimeZone;

        chrono::FixedOffset::east_opt(8 * 60 * 60)
            .unwrap()
            .with_ymd_and_hms(year, month, day, hour, minute, second)
            .single()
            .unwrap()
            .timestamp()
            * 1_000_000_000
    }
}

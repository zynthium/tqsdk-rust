use std::collections::{BTreeMap, HashSet};

use chrono::{Datelike, Days, FixedOffset, NaiveDate, Utc};
use serde_json::Value;
use tqsdk_core::TradingCalendarDay;

use crate::error::{DataError, Result};

use super::DataClient;

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

/// A single historical continuous-contract underlying mapping row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoricalContUnderlyingRow {
    pub date: String,
    pub symbol: String,
    pub underlying: String,
}

/// A contiguous historical continuous-contract underlying segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoricalContUnderlyingSegment {
    pub symbol: String,
    pub underlying: String,
    pub start_date: String,
    pub end_date: String,
    pub trading_days: usize,
}

/// A single exchange trading-calendar row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradingCalendarRow {
    pub date: String,
    pub trading: bool,
}

impl DataClient {
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

    pub async fn query_his_cont_underlyings(
        &self,
        symbol: &str,
        days: usize,
        end_date: Option<NaiveDate>,
    ) -> Result<Vec<HistoricalContUnderlyingRow>> {
        let rows = self
            .query_his_cont_quotes(&[symbol], days, end_date)
            .await?;
        Ok(rows
            .into_iter()
            .map(|mut row| HistoricalContUnderlyingRow {
                date: row.date,
                symbol: symbol.to_string(),
                underlying: row.underlyings.remove(symbol).unwrap_or_default(),
            })
            .collect())
    }

    pub async fn query_his_cont_underlying_segments(
        &self,
        symbol: &str,
        days: usize,
        end_date: Option<NaiveDate>,
    ) -> Result<Vec<HistoricalContUnderlyingSegment>> {
        let rows = self
            .query_his_cont_underlyings(symbol, days, end_date)
            .await?;
        Ok(historical_cont_underlying_segments(&rows))
    }

    pub async fn query_trading_calendar(
        &self,
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Result<Vec<TradingCalendarRow>> {
        Ok(self
            .trading_calendar(start_date, end_date)
            .await?
            .into_iter()
            .map(|day| TradingCalendarRow {
                date: day.date.format("%Y-%m-%d").to_string(),
                trading: day.trading,
            })
            .collect())
    }

    pub async fn query_trading_days(
        &self,
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Result<Vec<NaiveDate>> {
        Ok(self
            .trading_calendar(start_date, end_date)
            .await?
            .into_iter()
            .filter_map(|day| day.trading.then_some(day.date))
            .collect())
    }

    async fn trading_days(
        &self,
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Result<Vec<NaiveDate>> {
        self.query_trading_days(start_date, end_date).await
    }

    async fn trading_calendar(
        &self,
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Result<Vec<TradingCalendarDay>> {
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
            days.push(TradingCalendarDay {
                date: current,
                trading,
            });
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
}

#[must_use]
pub fn historical_cont_underlying_segments(
    rows: &[HistoricalContUnderlyingRow],
) -> Vec<HistoricalContUnderlyingSegment> {
    let mut segments: Vec<HistoricalContUnderlyingSegment> = Vec::new();
    for row in rows {
        if row.underlying.is_empty() {
            continue;
        }

        if let Some(last) = segments.last_mut() {
            if last.symbol == row.symbol && last.underlying == row.underlying {
                last.end_date.clone_from(&row.date);
                last.trading_days += 1;
                continue;
            }
        }

        segments.push(HistoricalContUnderlyingSegment {
            symbol: row.symbol.clone(),
            underlying: row.underlying.clone(),
            start_date: row.date.clone(),
            end_date: row.date.clone(),
            trading_days: 1,
        });
    }
    segments
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
    let mut seen = HashSet::new();
    for symbol in symbols {
        if symbol.is_empty() {
            return Err(DataError::Validation(
                "symbols must not contain empty entries".to_string(),
            ));
        }
        if !seen.insert(*symbol) {
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

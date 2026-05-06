#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{HashMap, HashSet};

use chrono::{Datelike, NaiveDate};
use serde_json::{Value, json};
use tqsdk_core::{EdbIndexData, SymbolRanking, SymbolSettlement, TradingCalendarDay};

use crate::client::SessionClient;
use crate::direct_query::{EdbDataAlign, EdbDataFill, SymbolRankingType};
use crate::error::{Result, SessionFacadeError};

#[path = "services_helpers.rs"]
mod helpers;

use self::helpers::{
    fetch_json_get, fetch_json_post, json_value_to_f64, next_day, parse_iso_date,
    parse_service_url, ranking_value, split_symbol,
};

const DEFAULT_SETTLEMENT_URL: &str = "https://md-settlement-system-fc-api.shinnytech.com/mss";
const DEFAULT_RANKING_URL: &str = "https://symbol-ranking-system-fc-api.shinnytech.com/srs";
const DEFAULT_EDB_URL: &str = "https://edb.shinnytech.com/data/index_data";
const DEFAULT_HOLIDAY_URL: &str = "https://files.shinnytech.com/shinny_chinese_holiday.json";

type EdbRowMap = HashMap<String, HashMap<i32, f64>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionServiceEndpoints {
    pub(crate) settlement_url: String,
    pub(crate) ranking_url: String,
    pub(crate) edb_url: String,
    pub(crate) holiday_url: String,
}

impl Default for SessionServiceEndpoints {
    fn default() -> Self {
        Self {
            settlement_url: DEFAULT_SETTLEMENT_URL.to_string(),
            ranking_url: DEFAULT_RANKING_URL.to_string(),
            edb_url: DEFAULT_EDB_URL.to_string(),
            holiday_url: DEFAULT_HOLIDAY_URL.to_string(),
        }
    }
}

impl SessionClient {
    pub async fn get_trading_calendar(
        &self,
        start_dt: NaiveDate,
        end_dt: NaiveDate,
    ) -> Result<Vec<TradingCalendarDay>> {
        if start_dt > end_dt {
            return Err(SessionFacadeError::from(
                tqsdk_core::ContractError::validation(
                    "start_dt must be less than or equal to end_dt",
                ),
            ));
        }

        let holidays = fetch_json_get(self, &self.service_endpoints().holiday_url).await?;
        let items = holidays.as_array().ok_or_else(|| {
            SessionFacadeError::from(tqsdk_core::ContractError::validation(
                "trading calendar holiday payload must be an array",
            ))
        })?;

        let mut holiday_set = HashSet::new();
        let mut years = Vec::new();
        for item in items {
            let value = item.as_str().ok_or_else(|| {
                SessionFacadeError::from(tqsdk_core::ContractError::validation(
                    "trading calendar holiday entry must be a string",
                ))
            })?;
            let date = parse_iso_date(value)?;
            holiday_set.insert(date);
            years.push(date.year());
        }

        let (Some(first_year), Some(last_year)) = (years.iter().min(), years.iter().max()) else {
            return Err(SessionFacadeError::from(
                tqsdk_core::ContractError::validation("trading calendar holiday payload is empty"),
            ));
        };
        let first_day = NaiveDate::from_ymd_opt(*first_year, 1, 1).ok_or_else(|| {
            SessionFacadeError::from(tqsdk_core::ContractError::validation(
                "failed to build trading calendar lower bound",
            ))
        })?;
        let last_day = NaiveDate::from_ymd_opt(*last_year, 12, 31).ok_or_else(|| {
            SessionFacadeError::from(tqsdk_core::ContractError::validation(
                "failed to build trading calendar upper bound",
            ))
        })?;
        if start_dt < first_day || end_dt > last_day {
            return Err(SessionFacadeError::from(
                tqsdk_core::ContractError::validation(format!(
                    "trading calendar supports {} to {}",
                    first_day.format("%Y-%m-%d"),
                    last_day.format("%Y-%m-%d")
                )),
            ));
        }

        let mut rows = Vec::new();
        let mut current = start_dt;
        while current <= end_dt {
            let trading =
                current.weekday().number_from_monday() <= 5 && !holiday_set.contains(&current);
            rows.push(TradingCalendarDay {
                date: current.format("%Y-%m-%d").to_string(),
                trading,
            });
            current = next_day(current)?;
        }

        Ok(rows)
    }

    pub async fn query_symbol_settlement(
        &self,
        symbols: &[&str],
        days: usize,
        start_dt: Option<NaiveDate>,
    ) -> Result<Vec<SymbolSettlement>> {
        if symbols.is_empty() {
            return Err(SessionFacadeError::from(
                tqsdk_core::ContractError::validation("symbols must not be empty"),
            ));
        }
        if symbols.iter().any(|symbol| symbol.is_empty()) {
            return Err(SessionFacadeError::from(
                tqsdk_core::ContractError::validation("symbols must not contain empty entries"),
            ));
        }
        if days == 0 {
            return Err(SessionFacadeError::from(
                tqsdk_core::ContractError::validation("days must be greater than zero"),
            ));
        }

        let mut url = parse_service_url(&self.service_endpoints().settlement_url, "settlement")?;
        let query_days = if start_dt.is_some() {
            days
        } else {
            days.checked_add(1).ok_or_else(|| {
                SessionFacadeError::from(tqsdk_core::ContractError::validation(
                    "days overflow when requesting the latest settlement window",
                ))
            })?
        };

        {
            let mut pairs = url.query_pairs_mut();
            pairs.append_pair("symbols", &symbols.join(","));
            pairs.append_pair("days", &query_days.to_string());
            if let Some(start_dt) = start_dt {
                pairs.append_pair("start_date", &start_dt.format("%Y%m%d").to_string());
            }
        }

        let payload = fetch_json_get(self, url.as_str()).await?;
        let object = payload.as_object().ok_or_else(|| {
            SessionFacadeError::from(tqsdk_core::ContractError::validation(
                "symbol settlement payload must be an object",
            ))
        })?;

        let mut dates: Vec<&String> = object.keys().collect();
        dates.sort_unstable_by(|left, right| right.cmp(left));

        let mut rows = Vec::new();
        for date in dates.into_iter().take(days) {
            if let Some(symbol_map) = object.get(date).and_then(Value::as_object) {
                for (symbol, settlement) in symbol_map {
                    if settlement.is_null() {
                        continue;
                    }
                    rows.push(SymbolSettlement {
                        datetime: date.clone(),
                        symbol: symbol.clone(),
                        settlement: json_value_to_f64(settlement),
                    });
                }
            }
        }

        rows.sort_by(|left, right| {
            left.datetime
                .cmp(&right.datetime)
                .then_with(|| left.symbol.cmp(&right.symbol))
        });
        Ok(rows)
    }

    pub async fn query_symbol_ranking(
        &self,
        symbol: &str,
        ranking_type: SymbolRankingType,
        days: usize,
        start_dt: Option<NaiveDate>,
        broker: Option<&str>,
    ) -> Result<Vec<SymbolRanking>> {
        validate_symbol_ranking_args(symbol, days, broker)?;
        let url = build_symbol_ranking_url(
            &self.service_endpoints().ranking_url,
            symbol,
            days,
            start_dt,
            broker,
        )?;
        let payload = fetch_json_get(self, url.as_str()).await?;
        parse_symbol_ranking_payload(payload, ranking_type)
    }

    pub async fn query_edb_data(
        &self,
        ids: &[i32],
        start_dt: NaiveDate,
        end_dt: NaiveDate,
        align: Option<EdbDataAlign>,
        fill: Option<EdbDataFill>,
    ) -> Result<Vec<EdbIndexData>> {
        let deduped_ids = validate_and_dedup_edb_ids(ids, start_dt, end_dt)?;
        let payload = fetch_json_post(
            self,
            &self.service_endpoints().edb_url,
            &build_edb_data_request_body(&deduped_ids, start_dt, end_dt),
        )
        .await?;
        let (mut dates, mut rows) = parse_edb_data_payload(payload, &deduped_ids)?;
        align_and_fill_edb_rows(
            &mut dates,
            &mut rows,
            &deduped_ids,
            start_dt,
            end_dt,
            align,
            fill,
        )?;
        Ok(collect_edb_data_rows(dates, rows, &deduped_ids))
    }
}

fn validate_symbol_ranking_args(symbol: &str, days: usize, broker: Option<&str>) -> Result<()> {
    if symbol.is_empty() {
        return Err(SessionFacadeError::from(
            tqsdk_core::ContractError::validation("symbol must not be empty"),
        ));
    }
    if days == 0 {
        return Err(SessionFacadeError::from(
            tqsdk_core::ContractError::validation("days must be greater than zero"),
        ));
    }
    if matches!(broker, Some("")) {
        return Err(SessionFacadeError::from(
            tqsdk_core::ContractError::validation("broker must not be empty"),
        ));
    }
    Ok(())
}

fn build_symbol_ranking_url(
    ranking_url: &str,
    symbol: &str,
    days: usize,
    start_dt: Option<NaiveDate>,
    broker: Option<&str>,
) -> Result<String> {
    let mut url = parse_service_url(ranking_url, "ranking")?;
    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("symbol", symbol);
        pairs.append_pair("days", &days.to_string());
        if let Some(start_dt) = start_dt {
            pairs.append_pair("start_date", &start_dt.format("%Y%m%d").to_string());
        }
        if let Some(broker) = broker {
            pairs.append_pair("broker", broker);
        }
    }
    Ok(url.to_string())
}

fn parse_symbol_ranking_payload(
    payload: Value,
    ranking_type: SymbolRankingType,
) -> Result<Vec<SymbolRanking>> {
    let object = payload.as_object().ok_or_else(|| {
        SessionFacadeError::from(tqsdk_core::ContractError::validation(
            "symbol ranking payload must be an object",
        ))
    })?;

    let mut rows = HashMap::new();
    for (datetime, symbols) in object {
        let Some(symbols) = symbols.as_object() else {
            continue;
        };
        for (resolved_symbol, ranking_sets) in symbols {
            let Some(ranking_sets) = ranking_sets.as_object() else {
                continue;
            };
            for (ranking_name, brokers) in ranking_sets {
                let Some(brokers) = brokers.as_object() else {
                    continue;
                };
                for (broker_name, ranking_payload) in brokers {
                    let Some(ranking_payload) = ranking_payload.as_object() else {
                        continue;
                    };
                    let row = rows
                        .entry(format!("{datetime}|{resolved_symbol}|{broker_name}"))
                        .or_insert_with(|| {
                            let (exchange_id, instrument_id) = split_symbol(resolved_symbol);
                            SymbolRanking {
                                datetime: datetime.clone(),
                                symbol: resolved_symbol.clone(),
                                exchange_id: exchange_id.to_string(),
                                instrument_id: instrument_id.to_string(),
                                broker: broker_name.clone(),
                                volume: f64::NAN,
                                volume_change: f64::NAN,
                                volume_ranking: f64::NAN,
                                long_oi: f64::NAN,
                                long_change: f64::NAN,
                                long_ranking: f64::NAN,
                                short_oi: f64::NAN,
                                short_change: f64::NAN,
                                short_ranking: f64::NAN,
                            }
                        });
                    let volume = ranking_payload
                        .get("volume")
                        .map(json_value_to_f64)
                        .unwrap_or(f64::NAN);
                    let change = ranking_payload
                        .get("varvolume")
                        .map(json_value_to_f64)
                        .unwrap_or(f64::NAN);
                    let ranking = ranking_payload
                        .get("ranking")
                        .map(json_value_to_f64)
                        .unwrap_or(f64::NAN);
                    match ranking_name.as_str() {
                        "volume_ranking" => {
                            row.volume = volume;
                            row.volume_change = change;
                            row.volume_ranking = ranking;
                        }
                        "long_ranking" => {
                            row.long_oi = volume;
                            row.long_change = change;
                            row.long_ranking = ranking;
                        }
                        "short_ranking" => {
                            row.short_oi = volume;
                            row.short_change = change;
                            row.short_ranking = ranking;
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    let rank_field = ranking_type.rank_field();
    let mut rows: Vec<_> = rows
        .into_values()
        .filter(|row| !ranking_value(row, rank_field).is_nan())
        .collect();
    rows.sort_by(|left, right| {
        left.datetime.cmp(&right.datetime).then_with(|| {
            ranking_value(left, rank_field)
                .partial_cmp(&ranking_value(right, rank_field))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
    });
    Ok(rows)
}

fn validate_and_dedup_edb_ids(
    ids: &[i32],
    start_dt: NaiveDate,
    end_dt: NaiveDate,
) -> Result<Vec<i32>> {
    if ids.is_empty() {
        return Err(SessionFacadeError::from(
            tqsdk_core::ContractError::validation("ids must not be empty"),
        ));
    }
    if start_dt > end_dt {
        return Err(SessionFacadeError::from(
            tqsdk_core::ContractError::validation("start_dt must be less than or equal to end_dt"),
        ));
    }

    let mut deduped_ids = Vec::with_capacity(ids.len());
    let mut seen = HashSet::new();
    for id in ids {
        if seen.insert(*id) {
            deduped_ids.push(*id);
        }
    }
    if deduped_ids.len() > 100 {
        return Err(SessionFacadeError::from(
            tqsdk_core::ContractError::validation("ids must contain at most 100 unique entries"),
        ));
    }
    Ok(deduped_ids)
}

fn build_edb_data_request_body(
    deduped_ids: &[i32],
    start_dt: NaiveDate,
    end_dt: NaiveDate,
) -> Value {
    json!({
        "ids": deduped_ids,
        "start": start_dt.format("%Y-%m-%d").to_string(),
        "end": end_dt.format("%Y-%m-%d").to_string(),
    })
}

fn parse_edb_data_payload(payload: Value, deduped_ids: &[i32]) -> Result<(Vec<String>, EdbRowMap)> {
    if payload
        .get("error_code")
        .and_then(Value::as_i64)
        .unwrap_or_default()
        != 0
    {
        let message = payload
            .get("error_msg")
            .and_then(Value::as_str)
            .unwrap_or("unknown edb query failure");
        return Err(SessionFacadeError::from(tqsdk_core::ContractError::http(
            format!("edb query failed: {message}"),
        )));
    }

    let data = payload.get("data").cloned().unwrap_or(Value::Null);
    let ids_from_server = data
        .get("ids")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_i64)
                .map(|value| value as i32)
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| deduped_ids.to_vec());

    let mut rows = HashMap::<String, HashMap<i32, f64>>::new();
    if let Some(values) = data.get("values").and_then(Value::as_object) {
        for (date, items) in values {
            let Some(items) = items.as_array() else {
                continue;
            };
            let mut row = HashMap::new();
            for (index, id) in ids_from_server.iter().enumerate() {
                row.insert(
                    *id,
                    items.get(index).map(json_value_to_f64).unwrap_or(f64::NAN),
                );
            }
            rows.insert(date.clone(), row);
        }
    }

    let mut dates: Vec<String> = rows.keys().cloned().collect();
    dates.sort_unstable();
    Ok((dates, rows))
}

fn align_and_fill_edb_rows(
    dates: &mut Vec<String>,
    rows: &mut EdbRowMap,
    deduped_ids: &[i32],
    start_dt: NaiveDate,
    end_dt: NaiveDate,
    align: Option<EdbDataAlign>,
    fill: Option<EdbDataFill>,
) -> Result<()> {
    if matches!(align, Some(EdbDataAlign::Day)) {
        let mut current = start_dt;
        let mut aligned_dates = Vec::new();
        let mut aligned_rows = HashMap::new();
        while current <= end_dt {
            let date = current.format("%Y-%m-%d").to_string();
            aligned_rows.insert(date.clone(), rows.get(&date).cloned().unwrap_or_default());
            aligned_dates.push(date);
            current = next_day(current)?;
        }
        *rows = aligned_rows;
        *dates = aligned_dates;

        match fill {
            Some(EdbDataFill::Forward) => {
                let mut last_values = HashMap::<i32, f64>::new();
                for date in dates {
                    let row = rows.entry(date.clone()).or_default();
                    for id in deduped_ids {
                        let value = row.get(id).copied().unwrap_or(f64::NAN);
                        if value.is_nan() {
                            if let Some(last) = last_values.get(id) {
                                row.insert(*id, *last);
                            }
                        } else {
                            last_values.insert(*id, value);
                        }
                    }
                }
            }
            Some(EdbDataFill::Backward) => {
                let mut next_values = HashMap::<i32, f64>::new();
                for date in dates.iter().rev() {
                    let row = rows.entry(date.clone()).or_default();
                    for id in deduped_ids {
                        let value = row.get(id).copied().unwrap_or(f64::NAN);
                        if value.is_nan() {
                            if let Some(next) = next_values.get(id) {
                                row.insert(*id, *next);
                            }
                        } else {
                            next_values.insert(*id, value);
                        }
                    }
                }
            }
            None => {}
        }
    }
    Ok(())
}

fn collect_edb_data_rows(
    dates: Vec<String>,
    rows: EdbRowMap,
    deduped_ids: &[i32],
) -> Vec<EdbIndexData> {
    dates
        .into_iter()
        .map(|date| EdbIndexData {
            values: deduped_ids
                .iter()
                .map(|id| {
                    let value = rows
                        .get(&date)
                        .and_then(|row| row.get(id).copied())
                        .unwrap_or(f64::NAN);
                    (*id, value)
                })
                .collect(),
            date,
        })
        .collect()
}

#[cfg(test)]
#[path = "services_tests.rs"]
mod tests;

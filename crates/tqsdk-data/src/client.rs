#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{BTreeMap, HashSet};

use chrono::{Datelike, Days, FixedOffset, NaiveDate, Utc};
use serde_json::Value;

use crate::error::{DataError, Result};

const DEFAULT_HOLIDAY_URL: &str = "https://files.shinnytech.com/shinny_chinese_holiday.json";
const DEFAULT_CONTINUOUS_TABLE_URL: &str = "https://files.shinnytech.com/continuous_table.json";

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

/// Thin research/offline data wrapper over [`tqsdk_session::SessionClient`].
pub struct DataClient {
    session: tqsdk_session::SessionClient,
    http: reqwest::Client,
    endpoints: DataServiceEndpoints,
}

impl DataClient {
    #[must_use]
    pub fn new(session: tqsdk_session::SessionClient) -> Self {
        Self {
            session,
            http: reqwest::Client::new(),
            endpoints: DataServiceEndpoints::default(),
        }
    }

    #[doc(hidden)]
    #[must_use]
    pub fn new_for_test_with_urls(
        session: tqsdk_session::SessionClient,
        holiday_url: impl Into<String>,
        continuous_table_url: impl Into<String>,
    ) -> Self {
        Self {
            session,
            http: reqwest::Client::new(),
            endpoints: DataServiceEndpoints {
                holiday_url: holiday_url.into(),
                continuous_table_url: continuous_table_url.into(),
            },
        }
    }

    #[must_use]
    pub fn session(&self) -> &tqsdk_session::SessionClient {
        &self.session
    }

    #[must_use]
    pub fn into_session(self) -> tqsdk_session::SessionClient {
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
    use std::io::{Read, Write};
    use std::net::TcpListener;

    use tqsdk_core::{AdapterRegistry, RuntimeHandle};
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
                test_session(),
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
    fn query_his_cont_quotes_rejects_invalid_inputs() {
        run_on_tokio(async {
            let client = DataClient::new(test_session());

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

    fn test_session() -> SessionClient {
        let mut adapters = AdapterRegistry::new();
        adapters.register_default_adapters();
        let handle = RuntimeHandle::with_adapters(adapters);
        SessionClient::new_for_test_with_handle(handle, SessionFacadeConfig::default())
    }

    fn run_on_tokio<F>(future: F)
    where
        F: std::future::Future<Output = ()>,
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(future);
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

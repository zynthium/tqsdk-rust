use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use crate::{Auth, Result, data_validation, session_builder};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BacktestKlineFillRequest {
    pub(crate) symbol: String,
    pub(crate) duration_ns: i64,
    pub(crate) start_ns: i64,
    pub(crate) end_ns: i64,
}

impl BacktestKlineFillRequest {
    pub(crate) fn new(
        symbol: impl Into<String>,
        duration_ns: i64,
        start_ns: i64,
        end_ns: i64,
    ) -> Self {
        Self {
            symbol: symbol.into(),
            duration_ns,
            start_ns,
            end_ns,
        }
    }
}

pub(crate) struct BacktestKlineFillReport {
    pub(crate) rows_by_series: BTreeMap<(String, i64), usize>,
}

pub(crate) async fn fill_backtest_kline_cache(
    auth: &Auth,
    cache_dir: &Path,
    requests: Vec<BacktestKlineFillRequest>,
) -> Result<BacktestKlineFillReport> {
    let session = session_builder(Some(auth.clone()), false, Vec::new(), None, None)?.build()?;
    let client = tqsdk_data::DataClientBuilder::new()
        .with_session(session)
        .history_cache_enabled(true)
        .history_cache_dir(cache_dir)
        .build()?;
    let mut rows_by_series = BTreeMap::new();
    for request in requests {
        validate_request(&request)?;
        let series = client
            .get_kline_data_series(tqsdk_data::KlineDataSeriesRequest::new(
                &request.symbol,
                Duration::from_nanos(request.duration_ns as u64),
                request.start_ns,
                request.end_ns,
            ))
            .await?;
        rows_by_series.insert((request.symbol, request.duration_ns), series.len());
    }
    Ok(BacktestKlineFillReport { rows_by_series })
}

fn validate_request(request: &BacktestKlineFillRequest) -> Result<()> {
    if request.symbol.is_empty() {
        return Err(data_validation(
            "remote backtest kline cache fill symbol is empty",
        ));
    }
    if request.duration_ns <= 0 {
        return Err(data_validation(
            "remote backtest kline cache fill duration must be positive",
        ));
    }
    if request.start_ns >= request.end_ns {
        return Err(data_validation(format!(
            "remote backtest kline cache fill range is invalid for {} duration {}: [{}, {})",
            request.symbol, request.duration_ns, request.start_ns, request.end_ns
        )));
    }
    Ok(())
}

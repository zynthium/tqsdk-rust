use std::collections::BTreeMap;
use std::time::Duration;

use tqsdk_core::Kline;
use tqsdk_data::{MINUTE_KLINE_DURATION_NS, MinuteKlineCache, MinuteKlineCacheSnapshot};
use tqsdk_wait::BacktestMarketKind;

use crate::{Auth, Result, data_validation};

const REMOTE_MINUTE_KLINE_DATA_LENGTH: usize = 10_000;
const REMOTE_MINUTE_KLINE_READY_TIMEOUT: Duration = Duration::from_secs(30);
const REMOTE_MINUTE_KLINE_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// One final canonical-minute range that must be materialized from an
/// official server-side backtest stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BacktestMinuteKlineFillRequest {
    pub(crate) symbol: String,
    pub(crate) start_ns: i64,
    pub(crate) end_ns: i64,
}

impl BacktestMinuteKlineFillRequest {
    pub(crate) fn new(symbol: impl Into<String>, start_ns: i64, end_ns: i64) -> Self {
        Self {
            symbol: symbol.into(),
            start_ns,
            end_ns,
        }
    }
}

pub(crate) struct BacktestMinuteKlineFillReport {
    pub(crate) rows_by_symbol: BTreeMap<String, usize>,
}

/// Fill the independent v3 canonical-minute cache from the server-side
/// backtest Kline stream.
///
/// The backtest terminal signal is the only completion authority.  We never
/// infer coverage from timestamp gaps: empty sessions and suspended symbols
/// are still valid final ranges.  Rows are deduplicated by datetime with the
/// latest server update winning, then final coverage is committed only after
/// the server reaches its terminal state.
pub(crate) async fn fill_backtest_minute_kline_cache(
    auth: &Auth,
    cache: &MinuteKlineCache,
    snapshot: &MinuteKlineCacheSnapshot,
    market_kind: BacktestMarketKind,
    requests: Vec<BacktestMinuteKlineFillRequest>,
) -> Result<BacktestMinuteKlineFillReport> {
    let mut rows_by_symbol = BTreeMap::new();
    for request in requests {
        validate_request(&request)?;
        let rows = collect_server_minutes(auth, market_kind, &request).await?;
        cache.store_final_range(
            &request.symbol,
            request.start_ns,
            request.end_ns,
            snapshot,
            rows.as_slice(),
        )?;
        rows_by_symbol.insert(request.symbol, rows.len());
    }
    Ok(BacktestMinuteKlineFillReport { rows_by_symbol })
}

async fn collect_server_minutes(
    auth: &Auth,
    market_kind: BacktestMarketKind,
    request: &BacktestMinuteKlineFillRequest,
) -> Result<Vec<Kline>> {
    let builder = tqsdk_wait::TqApiBuilder::new(auth.user.clone(), auth.pass.clone());
    let builder = match market_kind {
        BacktestMarketKind::Futures => {
            builder.futures_backtest(request.start_ns, request.end_ns)?
        }
        BacktestMarketKind::Stock => builder.stock_backtest(request.start_ns, request.end_ns)?,
    };
    let mut api = builder.backtest_cache_fill_mode().build().await?;
    let ready_deadline = tokio::time::Instant::now() + REMOTE_MINUTE_KLINE_READY_TIMEOUT;
    let handle = api
        .kline_ready(
            &request.symbol,
            Duration::from_nanos(MINUTE_KLINE_DURATION_NS as u64),
            REMOTE_MINUTE_KLINE_DATA_LENGTH,
            Some(ready_deadline),
        )
        .await?;

    let mut rows = BTreeMap::<i64, Kline>::new();
    accept_rows(&mut rows, handle.rows()?, request);
    loop {
        let step = tokio::time::timeout(REMOTE_MINUTE_KLINE_IDLE_TIMEOUT, api.step_until(None))
            .await
            .map_err(|_| {
                data_validation(format!(
                    "remote 60-second kline cache fill stalled for {}",
                    request.symbol
                ))
            })??;
        let Some(step) = step else {
            break;
        };
        accept_rows(&mut rows, handle.changed_rows(&step)?, request);
    }

    Ok(rows.into_values().collect())
}

fn accept_rows(
    target: &mut BTreeMap<i64, Kline>,
    rows: Vec<Kline>,
    request: &BacktestMinuteKlineFillRequest,
) {
    for row in rows {
        if row.datetime >= request.start_ns && row.datetime < request.end_ns {
            target.insert(row.datetime, row);
        }
    }
}

fn validate_request(request: &BacktestMinuteKlineFillRequest) -> Result<()> {
    if request.symbol.is_empty() {
        return Err(data_validation(
            "remote canonical minute cache fill symbol is empty",
        ));
    }
    if request.start_ns >= request.end_ns {
        return Err(data_validation(format!(
            "remote canonical minute cache fill range is invalid for {}: [{}, {})",
            request.symbol, request.start_ns, request.end_ns
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{BacktestMinuteKlineFillRequest, validate_request};

    #[test]
    fn minute_fill_request_rejects_empty_symbol_and_invalid_range() {
        assert!(validate_request(&BacktestMinuteKlineFillRequest::new("", 1, 2)).is_err());
        assert!(
            validate_request(&BacktestMinuteKlineFillRequest::new("SHFE.rb2601", 2, 2)).is_err()
        );
        assert!(
            validate_request(&BacktestMinuteKlineFillRequest::new("SHFE.rb2601", 1, 2)).is_ok()
        );
    }
}

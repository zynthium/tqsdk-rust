use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use tqsdk_data::{BacktestTickCache, BacktestTickFill};
use tqsdk_task::{BacktestMarketStream, ReplayMarketEvent};

use crate::{Result, data_validation};

const REMOTE_TICK_DATA_LENGTH: usize = 10_000;
const REMOTE_FILL_END_TOLERANCE_NS: i64 = 1_000_000_000;
const REMOTE_STEP_POLL_TIMEOUT: Duration = Duration::from_secs(5);
const REMOTE_FILL_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

pub(crate) struct RemoteBacktestCachingStream {
    api: tqsdk_wait::TqApi,
    handles: BTreeMap<String, tqsdk_wait::TickHandle>,
    cache: BacktestTickCache,
    fills: BTreeMap<String, BacktestTickFill>,
    pending: VecDeque<ReplayMarketEvent>,
    last_progress: tokio::time::Instant,
    finalized: bool,
}

pub(crate) struct RemoteBacktestCacheFillReport {
    pub(crate) rows_by_symbol: BTreeMap<String, usize>,
}

pub(crate) async fn fill_backtest_tick_cache(
    user: String,
    pass: String,
    start_ns: i64,
    end_ns: i64,
    symbols: Vec<String>,
    cache: BacktestTickCache,
) -> Result<RemoteBacktestCacheFillReport> {
    let mut stream =
        RemoteBacktestCachingStream::connect(user, pass, start_ns, end_ns, symbols, cache).await?;
    let mut rows_by_symbol = BTreeMap::new();
    while let Some(event) = stream.next_remote_event().await? {
        *rows_by_symbol
            .entry(event.symbol().to_string())
            .or_insert(0) += 1;
    }
    Ok(RemoteBacktestCacheFillReport { rows_by_symbol })
}

impl RemoteBacktestCachingStream {
    pub(crate) async fn connect(
        user: String,
        pass: String,
        start_ns: i64,
        end_ns: i64,
        symbols: Vec<String>,
        cache: BacktestTickCache,
    ) -> Result<Self> {
        let mut api = tqsdk_wait::TqApiBuilder::new(user, pass)
            .futures_backtest(start_ns, end_ns)?
            .build()
            .await?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let mut handles = BTreeMap::new();
        let mut fills = BTreeMap::new();
        for symbol in symbols {
            let handle = api
                .tick_ready(&symbol, REMOTE_TICK_DATA_LENGTH, Some(deadline))
                .await?;
            fills.insert(
                symbol.clone(),
                BacktestTickFill::new(symbol.clone(), start_ns, end_ns),
            );
            handles.insert(symbol, handle);
        }
        Ok(Self {
            api,
            handles,
            cache,
            fills,
            pending: VecDeque::new(),
            last_progress: tokio::time::Instant::now(),
            finalized: false,
        })
    }

    async fn next_remote_event(&mut self) -> Result<Option<ReplayMarketEvent>> {
        loop {
            if let Some(event) = self.pending.pop_front() {
                return Ok(Some(event));
            }
            if self.fills_complete()? {
                self.finalize_cache()?;
                return Ok(None);
            }
            if self.last_progress.elapsed() >= REMOTE_FILL_IDLE_TIMEOUT {
                self.finalize_cache()?;
                return Ok(None);
            }

            let deadline = tokio::time::Instant::now() + REMOTE_STEP_POLL_TIMEOUT;
            let Some(step) = self.api.step_until(Some(deadline)).await? else {
                continue;
            };

            let mut made_progress = false;
            for (symbol, handle) in &self.handles {
                if !step.is_changing(handle) {
                    continue;
                }

                let mut accepted_rows = Vec::new();
                let mut accepted_events = Vec::new();
                for row in handle.changed_rows(&step)? {
                    let Some(fill) = self.fills.get_mut(symbol) else {
                        continue;
                    };
                    if !fill.push(row.clone())? {
                        continue;
                    }

                    accepted_events.push(ReplayMarketEvent::tick(
                        "server-backtest",
                        symbol,
                        row.datetime,
                        Some(row.datetime),
                        row.clone(),
                    )?);
                    accepted_rows.push(row);
                }

                if !accepted_rows.is_empty() {
                    self.cache.append_partial_ticks(symbol, accepted_rows)?;
                    made_progress = true;
                    self.pending.extend(accepted_events);
                }
            }

            if made_progress {
                self.last_progress = tokio::time::Instant::now();
            }
        }
    }

    fn fills_complete(&self) -> Result<bool> {
        for fill in self.fills.values() {
            if !fill.finish(REMOTE_FILL_END_TOLERANCE_NS)?.complete {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn finalize_cache(&mut self) -> Result<()> {
        if self.finalized {
            return Ok(());
        }
        self.finalized = true;
        for (symbol, fill) in &self.fills {
            let report = fill.finish(REMOTE_FILL_END_TOLERANCE_NS)?;
            if !report.complete {
                return Err(data_validation(format!(
                    "incomplete remote backtest cache fill for {symbol}: {:?}",
                    report.gap_summary
                )));
            }
            self.cache.mark_complete(
                symbol,
                report.requested_range.0,
                report.requested_range.1,
                report.unique_rows,
                report.id_range,
            )?;
            self.cache.compact_symbol_ticks(symbol)?;
        }
        Ok(())
    }
}

impl BacktestMarketStream for RemoteBacktestCachingStream {
    fn next_event<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = tqsdk_task::Result<Option<ReplayMarketEvent>>> + 'a>> {
        Box::pin(async move {
            self.next_remote_event()
                .await
                .map_err(|error| tqsdk_task::TaskError::External(error.to_string()))
        })
    }
}

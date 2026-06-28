use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use tqsdk_data::{BacktestTickCache, BacktestTickFill};
use tqsdk_task::{BacktestMarketStream, ReplayMarketEvent};

use crate::{Result, data_validation};

const REMOTE_TICK_DATA_LENGTH: usize = 10_000;
const REMOTE_FILL_END_TOLERANCE_NS: i64 = 1_000_000_000;

pub(crate) struct RemoteBacktestCachingStream {
    api: tqsdk_wait::TqApi,
    handles: BTreeMap<String, tqsdk_wait::TickHandle>,
    cache: BacktestTickCache,
    fills: BTreeMap<String, BacktestTickFill>,
    pending: VecDeque<ReplayMarketEvent>,
    finalized: bool,
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
            finalized: false,
        })
    }

    async fn next_remote_event(&mut self) -> Result<Option<ReplayMarketEvent>> {
        if let Some(event) = self.pending.pop_front() {
            return Ok(Some(event));
        }

        while let Some(step) = self.api.step_until(None).await? {
            for (symbol, handle) in &self.handles {
                if !step.is_changing(handle) {
                    continue;
                }

                for row in handle.changed_rows(&step)? {
                    let Some(fill) = self.fills.get_mut(symbol) else {
                        continue;
                    };
                    if !fill.push(row.clone())? {
                        continue;
                    }

                    self.cache.append_partial_ticks(symbol, [row.clone()])?;
                    self.pending.push_back(ReplayMarketEvent::tick(
                        "server-backtest",
                        symbol,
                        row.datetime,
                        Some(row.datetime),
                        row,
                    )?);
                }
            }

            if let Some(event) = self.pending.pop_front() {
                return Ok(Some(event));
            }
        }

        self.finalize_cache()?;
        Ok(None)
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

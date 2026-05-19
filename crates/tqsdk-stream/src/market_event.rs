#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::Stream;
use tqsdk_core::{MarketChartCommand, Quote, RuntimeReader, SharedCommitResult, StatePath, Symbol};

use crate::api::{TqStream, duration_to_ns};
use crate::filter::PathCommitStream;
use crate::typed::ValueUpdate;
use crate::window::{
    CommitTouchSet, KlineProjection, KlineRowBatch, KlineRowSpec, RowProjectionCursor,
    TickProjection, TickRowBatch, TickRowSpec, kline_chart_id, project_kline_rows_from_market,
    project_tick_rows_from_market, tick_chart_id,
};
use crate::{Result, StreamFacadeError};

#[derive(Debug, Clone)]
struct TickEventSpec {
    projection: TickProjection,
    chart_path: StatePath,
    data_path: StatePath,
}

#[derive(Debug, Clone)]
struct KlineEventSpec {
    projection: KlineProjection,
    chart_path: StatePath,
    data_path: StatePath,
}

/// Typed market data update emitted by [`MarketEventStream`].
#[derive(Debug, Clone)]
#[expect(
    clippy::large_enum_variant,
    reason = "MarketEvent preserves allocation-free public pattern matching on the market hot path."
)]
pub enum MarketEvent {
    Quote(ValueUpdate<Quote>),
    TickRows(ValueUpdate<TickRowBatch>),
    KlineRows(ValueUpdate<KlineRowBatch>),
}

/// Builder for a unified mixed market data stream.
pub struct MarketEventBuilder<'a> {
    stream: &'a TqStream,
    quotes: Vec<String>,
    ticks: Vec<(String, usize)>,
    klines: Vec<(String, Duration, usize)>,
}

impl<'a> MarketEventBuilder<'a> {
    pub(crate) fn new(stream: &'a TqStream) -> Self {
        Self {
            stream,
            quotes: Vec::new(),
            ticks: Vec::new(),
            klines: Vec::new(),
        }
    }

    #[must_use]
    pub fn quote(mut self, symbol: impl Into<String>) -> Self {
        self.quotes.push(symbol.into());
        self
    }

    #[must_use]
    pub fn tick(mut self, symbol: impl Into<String>, data_length: usize) -> Self {
        self.ticks.push((symbol.into(), data_length));
        self
    }

    #[must_use]
    pub fn kline(
        mut self,
        symbol: impl Into<String>,
        duration: Duration,
        data_length: usize,
    ) -> Self {
        self.klines.push((symbol.into(), duration, data_length));
        self
    }

    pub async fn build(self) -> Result<MarketEventStream> {
        if self.quotes.is_empty() && self.ticks.is_empty() && self.klines.is_empty() {
            return Err(StreamFacadeError::InvalidState(
                "market event stream requires at least one subscription",
            ));
        }

        let quote_symbols = self
            .quotes
            .into_iter()
            .map(Symbol::new)
            .collect::<BTreeSet<_>>();

        let tick_specs = self
            .ticks
            .into_iter()
            .map(|(symbol, view_width)| {
                let chart_id = tick_chart_id(symbol.as_str(), view_width);
                TickEventSpec {
                    chart_path: StatePath::new(["charts", chart_id.as_str()]),
                    data_path: StatePath::new(["ticks", symbol.as_str(), "data"]),
                    projection: TickProjection {
                        spec: TickRowSpec {
                            symbol,
                            view_width,
                            chart_id,
                        },
                        cursor: RowProjectionCursor::default(),
                    },
                }
            })
            .collect::<Vec<_>>();

        let kline_specs = self
            .klines
            .into_iter()
            .map(|(symbol, duration, view_width)| {
                let duration_ns = duration_to_ns(duration)?;
                let duration_key = duration_ns.to_string();
                let chart_id = kline_chart_id(symbol.as_str(), duration_ns, view_width);
                Ok(KlineEventSpec {
                    chart_path: StatePath::new(["charts", chart_id.as_str()]),
                    data_path: StatePath::new([
                        "klines",
                        symbol.as_str(),
                        duration_key.as_str(),
                        "data",
                    ]),
                    projection: KlineProjection {
                        spec: KlineRowSpec {
                            symbol,
                            duration_ns,
                            view_width,
                            chart_id,
                        },
                        cursor: RowProjectionCursor::default(),
                    },
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let tick_specs_by_chart = tick_specs
            .iter()
            .enumerate()
            .map(|(index, spec)| (spec.projection.spec.chart_id.clone(), index))
            .fold(
                BTreeMap::<String, Vec<usize>>::new(),
                |mut map, (key, index)| {
                    map.entry(key).or_default().push(index);
                    map
                },
            );
        let tick_specs_by_symbol = tick_specs
            .iter()
            .enumerate()
            .map(|(index, spec)| (spec.projection.spec.symbol.clone(), index))
            .fold(
                BTreeMap::<String, Vec<usize>>::new(),
                |mut map, (key, index)| {
                    map.entry(key).or_default().push(index);
                    map
                },
            );
        let kline_specs_by_chart = kline_specs
            .iter()
            .enumerate()
            .map(|(index, spec)| (spec.projection.spec.chart_id.clone(), index))
            .fold(
                BTreeMap::<String, Vec<usize>>::new(),
                |mut map, (key, index)| {
                    map.entry(key).or_default().push(index);
                    map
                },
            );
        let kline_specs_by_series = kline_specs.iter().enumerate().fold(
            BTreeMap::<String, BTreeMap<i64, Vec<usize>>>::new(),
            |mut map, (index, spec)| {
                map.entry(spec.projection.spec.symbol.clone())
                    .or_default()
                    .entry(spec.projection.spec.duration_ns)
                    .or_default()
                    .push(index);
                map
            },
        );

        let paths = quote_symbols
            .iter()
            .map(|symbol| StatePath::new(["quotes", symbol.as_str()]))
            .chain(
                tick_specs
                    .iter()
                    .flat_map(|spec| [spec.chart_path.clone(), spec.data_path.clone()].into_iter()),
            )
            .chain(
                kline_specs
                    .iter()
                    .flat_map(|spec| [spec.chart_path.clone(), spec.data_path.clone()].into_iter()),
            )
            .collect::<Vec<_>>();

        let inner = self.stream.path_commit_stream(paths)?;

        let quote_lease = if quote_symbols.is_empty() {
            None
        } else {
            Some(
                self.stream
                    .session()
                    .ensure_quotes(quote_symbols.iter().map(Symbol::as_str))
                    .await?,
            )
        };

        let mut chart_leases = Vec::new();
        for spec in &tick_specs {
            chart_leases.push(
                self.stream
                    .session()
                    .ensure_chart(MarketChartCommand {
                        chart_id: spec.projection.spec.chart_id.clone(),
                        symbols: vec![Symbol::new(spec.projection.spec.symbol.clone())],
                        duration_ns: 0,
                        view_width: spec.projection.spec.view_width,
                        left_kline_id: None,
                        focus_datetime_ns: None,
                        focus_position: None,
                    })
                    .await?,
            );
        }

        for spec in &kline_specs {
            chart_leases.push(
                self.stream
                    .session()
                    .ensure_chart(MarketChartCommand {
                        chart_id: spec.projection.spec.chart_id.clone(),
                        symbols: vec![Symbol::new(spec.projection.spec.symbol.clone())],
                        duration_ns: spec.projection.spec.duration_ns,
                        view_width: spec.projection.spec.view_width,
                        left_kline_id: None,
                        focus_datetime_ns: None,
                        focus_position: None,
                    })
                    .await?,
            );
        }

        Ok(MarketEventStream {
            inner,
            reader: self.stream.reader().clone(),
            quote_lease,
            chart_leases,
            quote_symbols,
            tick_specs,
            kline_specs,
            tick_specs_by_chart,
            tick_specs_by_symbol,
            kline_specs_by_chart,
            kline_specs_by_series,
            pending: VecDeque::new(),
        })
    }
}

/// Unified stream of typed quote, tick-row, and kline-row updates.
pub struct MarketEventStream {
    inner: PathCommitStream,
    reader: RuntimeReader,
    quote_lease: Option<tqsdk_session::MarketQuoteLease>,
    chart_leases: Vec<tqsdk_session::MarketChartLease>,
    quote_symbols: BTreeSet<Symbol>,
    tick_specs: Vec<TickEventSpec>,
    kline_specs: Vec<KlineEventSpec>,
    tick_specs_by_chart: BTreeMap<String, Vec<usize>>,
    tick_specs_by_symbol: BTreeMap<String, Vec<usize>>,
    kline_specs_by_chart: BTreeMap<String, Vec<usize>>,
    kline_specs_by_series: BTreeMap<String, BTreeMap<i64, Vec<usize>>>,
    pending: VecDeque<Result<MarketEvent>>,
}

impl MarketEventStream {
    pub async fn close(self) -> Result<()> {
        if let Some(lease) = self.quote_lease {
            lease.close().await?;
        }

        for lease in self.chart_leases {
            lease.close().await?;
        }

        Ok(())
    }

    fn collect_events(&mut self, commit: SharedCommitResult) -> Result<()> {
        let touches = CommitTouchSet::from_commit(&commit);
        let quote_hits = touches
            .quote_symbols()
            .filter(|symbol| self.quote_symbols.contains(symbol))
            .cloned()
            .collect::<Vec<_>>();

        let mut tick_hits = Vec::new();
        for chart_id in touches.chart_ids() {
            if let Some(indices) = self.tick_specs_by_chart.get(chart_id) {
                tick_hits.extend(indices.iter().copied());
            }
        }
        for symbol in touches.tick_symbols() {
            if let Some(indices) = self.tick_specs_by_symbol.get(symbol) {
                tick_hits.extend(indices.iter().copied());
            }
        }
        tick_hits.sort_unstable();
        tick_hits.dedup();

        let mut kline_hits = Vec::new();
        for chart_id in touches.chart_ids() {
            if let Some(indices) = self.kline_specs_by_chart.get(chart_id) {
                kline_hits.extend(indices.iter().copied());
            }
        }
        for (symbol, duration_ns) in touches.kline_series() {
            if let Some(durations) = self.kline_specs_by_series.get(symbol)
                && let Some(indices) = durations.get(&duration_ns)
            {
                kline_hits.extend(indices.iter().copied());
            }
        }
        kline_hits.sort_unstable();
        kline_hits.dedup();

        if !quote_hits.is_empty() || !tick_hits.is_empty() || !kline_hits.is_empty() {
            let market = self.reader.read_market_state();
            for symbol in quote_hits {
                if let Some(value) = market.quote(&symbol)? {
                    self.pending.push_back(Ok(MarketEvent::Quote(ValueUpdate {
                        commit: commit.clone(),
                        value,
                    })));
                }
            }

            for index in tick_hits {
                let spec = &mut self.tick_specs[index].projection;
                if let Some(value) =
                    project_tick_rows_from_market(&market, &spec.spec, &mut spec.cursor, &touches)?
                {
                    self.pending
                        .push_back(Ok(MarketEvent::TickRows(ValueUpdate {
                            commit: commit.clone(),
                            value,
                        })));
                }
            }
            for index in kline_hits {
                let spec = &mut self.kline_specs[index].projection;
                if let Some(value) =
                    project_kline_rows_from_market(&market, &spec.spec, &mut spec.cursor, &touches)?
                {
                    self.pending
                        .push_back(Ok(MarketEvent::KlineRows(ValueUpdate {
                            commit: commit.clone(),
                            value,
                        })));
                }
            }
        }

        Ok(())
    }
}

impl Stream for MarketEventStream {
    type Item = Result<MarketEvent>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if let Some(event) = this.pending.pop_front() {
            return Poll::Ready(Some(event));
        }

        loop {
            match Pin::new(&mut this.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(commit))) => match this.collect_events(commit) {
                    Ok(()) => {
                        if let Some(event) = this.pending.pop_front() {
                            return Poll::Ready(Some(event));
                        }
                    }
                    Err(error) => return Poll::Ready(Some(Err(error))),
                },
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

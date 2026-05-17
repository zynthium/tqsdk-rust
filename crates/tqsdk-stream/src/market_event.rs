#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{BTreeSet, VecDeque};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::Stream;
use tqsdk_core::{
    CommitResult, MarketChartCommand, Quote, RuntimeReader, SharedCommitResult, StatePath, Symbol,
};

use crate::api::{TqStream, duration_to_ns};
use crate::filter::PathCommitStream;
use crate::quote_subscription::changed_quote_symbols;
use crate::typed::ValueUpdate;
use crate::window::{
    KlineWindow, KlineWindowSpec, TickWindow, TickWindowSpec, kline_chart_id,
    project_kline_window_from_market, project_tick_window_from_market, tick_chart_id,
};
use crate::{Result, StreamFacadeError};

#[derive(Debug, Clone)]
struct TickEventSpec {
    window: TickWindowSpec,
    chart_path: StatePath,
    data_path: StatePath,
}

#[derive(Debug, Clone)]
struct KlineEventSpec {
    window: KlineWindowSpec,
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
    TickWindow(ValueUpdate<TickWindow>),
    KlineWindow(ValueUpdate<KlineWindow>),
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
                    window: TickWindowSpec {
                        symbol,
                        view_width,
                        chart_id,
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
                    window: KlineWindowSpec {
                        symbol,
                        duration_ns,
                        view_width,
                        chart_id,
                    },
                })
            })
            .collect::<Result<Vec<_>>>()?;

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

        let inner = self.stream.commit_stream()?.filter_paths(paths);

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
                        chart_id: spec.window.chart_id.clone(),
                        symbols: vec![Symbol::new(spec.window.symbol.clone())],
                        duration_ns: 0,
                        view_width: spec.window.view_width,
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
                        chart_id: spec.window.chart_id.clone(),
                        symbols: vec![Symbol::new(spec.window.symbol.clone())],
                        duration_ns: spec.window.duration_ns,
                        view_width: spec.window.view_width,
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
            pending: VecDeque::new(),
        })
    }
}

/// Unified stream of typed quote, tick-window, and kline-window updates.
pub struct MarketEventStream {
    inner: PathCommitStream,
    reader: RuntimeReader,
    quote_lease: Option<tqsdk_session::MarketQuoteLease>,
    chart_leases: Vec<tqsdk_session::MarketChartLease>,
    quote_symbols: BTreeSet<Symbol>,
    tick_specs: Vec<TickEventSpec>,
    kline_specs: Vec<KlineEventSpec>,
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
        let quote_hits = changed_quote_symbols(&commit)
            .into_iter()
            .filter(|symbol| self.quote_symbols.contains(symbol))
            .collect::<Vec<_>>();
        if !quote_hits.is_empty() {
            let market = self.reader.read_market_state();
            for symbol in quote_hits {
                if let Some(value) = market.quote(&symbol)? {
                    self.pending.push_back(Ok(MarketEvent::Quote(ValueUpdate {
                        commit: commit.clone(),
                        value,
                    })));
                }
            }
        }

        let tick_hits = self
            .tick_specs
            .iter()
            .filter(|spec| {
                commit_touches_path(&commit, &spec.chart_path)
                    || commit_touches_path(&commit, &spec.data_path)
            })
            .collect::<Vec<_>>();
        let kline_hits = self
            .kline_specs
            .iter()
            .filter(|spec| {
                commit_touches_path(&commit, &spec.chart_path)
                    || commit_touches_path(&commit, &spec.data_path)
            })
            .collect::<Vec<_>>();

        if !tick_hits.is_empty() || !kline_hits.is_empty() {
            let market = self.reader.read_market_state();
            for spec in tick_hits {
                if let Some(value) = project_tick_window_from_market(&market, &spec.window)? {
                    self.pending
                        .push_back(Ok(MarketEvent::TickWindow(ValueUpdate {
                            commit: commit.clone(),
                            value,
                        })));
                }
            }
            for spec in kline_hits {
                if let Some(value) = project_kline_window_from_market(&market, &spec.window)? {
                    self.pending
                        .push_back(Ok(MarketEvent::KlineWindow(ValueUpdate {
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

fn commit_touches_path(commit: &CommitResult, target: &StatePath) -> bool {
    commit
        .changes
        .path_hits
        .iter()
        .any(|changed| path_matches(target, changed))
}

fn path_matches(target: &StatePath, changed: &StatePath) -> bool {
    let target_segments = target.segments();
    let changed_segments = changed.segments();

    target_segments.len() <= changed_segments.len()
        && target_segments
            .iter()
            .zip(changed_segments.iter())
            .all(|(left, right)| left == right)
}

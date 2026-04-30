#![cfg_attr(not(test), forbid(unsafe_code))]

use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use tqsdk_core::{Kline, MarketCommand, MarketStateReadGuard, RuntimeCommand, Tick};

use crate::{PathCommitStream, Result, ValueUpdate};

/// Owned snapshot of the current kline serial window.
#[derive(Debug, Clone, Default)]
pub struct KlineWindow {
    symbol: String,
    duration_ns: i64,
    view_width: usize,
    chart_id: String,
    rows: Vec<Kline>,
}

impl KlineWindow {
    #[must_use]
    pub fn new(
        symbol: String,
        duration_ns: i64,
        view_width: usize,
        chart_id: String,
        rows: Vec<Kline>,
    ) -> Self {
        Self {
            symbol,
            duration_ns,
            view_width,
            chart_id,
            rows,
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    #[must_use]
    pub fn last(&self) -> Option<&Kline> {
        self.rows.last()
    }

    #[must_use]
    pub fn get(&self, index: usize) -> Option<&Kline> {
        self.rows.get(index)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Kline> {
        self.rows.iter()
    }

    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    #[must_use]
    pub fn duration_ns(&self) -> i64 {
        self.duration_ns
    }

    #[must_use]
    pub fn view_width(&self) -> usize {
        self.view_width
    }

    #[must_use]
    pub fn chart_id(&self) -> &str {
        &self.chart_id
    }
}

/// Owned snapshot of the current tick serial window.
#[derive(Debug, Clone, Default)]
pub struct TickWindow {
    symbol: String,
    view_width: usize,
    chart_id: String,
    rows: Vec<Tick>,
}

impl TickWindow {
    #[must_use]
    pub fn new(symbol: String, view_width: usize, chart_id: String, rows: Vec<Tick>) -> Self {
        Self {
            symbol,
            view_width,
            chart_id,
            rows,
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    #[must_use]
    pub fn last(&self) -> Option<&Tick> {
        self.rows.last()
    }

    #[must_use]
    pub fn get(&self, index: usize) -> Option<&Tick> {
        self.rows.get(index)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Tick> {
        self.rows.iter()
    }

    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    #[must_use]
    pub fn view_width(&self) -> usize {
        self.view_width
    }

    #[must_use]
    pub fn chart_id(&self) -> &str {
        &self.chart_id
    }
}

#[derive(Debug, Clone)]
pub(crate) struct KlineWindowSpec {
    pub(crate) symbol: String,
    pub(crate) duration_ns: i64,
    pub(crate) view_width: usize,
    pub(crate) chart_id: String,
}

#[derive(Debug, Clone)]
pub(crate) struct TickWindowSpec {
    pub(crate) symbol: String,
    pub(crate) view_width: usize,
    pub(crate) chart_id: String,
}

struct ProjectedValueStream<T, C> {
    inner: PathCommitStream,
    reader: tqsdk_core::RuntimeReader,
    context: C,
    projector: for<'a> fn(MarketStateReadGuard<'a>, &C) -> Result<Option<T>>,
    marker: PhantomData<fn() -> T>,
}

impl<T, C> ProjectedValueStream<T, C> {
    fn new(
        inner: PathCommitStream,
        reader: tqsdk_core::RuntimeReader,
        context: C,
        projector: for<'a> fn(MarketStateReadGuard<'a>, &C) -> Result<Option<T>>,
    ) -> Self {
        Self {
            inner,
            reader,
            context,
            projector,
            marker: PhantomData,
        }
    }
}

impl<T, C> Stream for ProjectedValueStream<T, C>
where
    C: Unpin,
{
    type Item = Result<ValueUpdate<T>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            match Pin::new(&mut this.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(commit))) => {
                    let market = this.reader.read_market_state();
                    match (this.projector)(market, &this.context) {
                        Ok(Some(value)) => {
                            return Poll::Ready(Some(Ok(ValueUpdate { commit, value })));
                        }
                        Ok(None) => continue,
                        Err(error) => return Poll::Ready(Some(Err(error))),
                    }
                }
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// Commit-driven stream of ready kline windows.
pub struct KlineWindowStream {
    inner: ProjectedValueStream<KlineWindow, KlineWindowSpec>,
    session: tqsdk_session::SessionClient,
    chart_id: String,
}

impl KlineWindowStream {
    pub(crate) fn new(
        inner: PathCommitStream,
        session: tqsdk_session::SessionClient,
        reader: tqsdk_core::RuntimeReader,
        symbol: String,
        duration_ns: i64,
        view_width: usize,
        chart_id: String,
    ) -> Self {
        Self {
            inner: ProjectedValueStream::new(
                inner,
                reader,
                KlineWindowSpec {
                    symbol,
                    duration_ns,
                    view_width,
                    chart_id: chart_id.clone(),
                },
                project_kline_window,
            ),
            session,
            chart_id,
        }
    }

    #[must_use]
    pub fn chart_id(&self) -> &str {
        &self.chart_id
    }

    pub async fn close(self) -> Result<()> {
        self.session
            .submit(RuntimeCommand::Market(MarketCommand::CancelChart {
                chart_id: self.chart_id,
            }))
            .await?;
        Ok(())
    }
}

impl Stream for KlineWindowStream {
    type Item = Result<ValueUpdate<KlineWindow>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll_next(cx)
    }
}

/// Commit-driven stream of ready tick windows.
pub struct TickWindowStream {
    inner: ProjectedValueStream<TickWindow, TickWindowSpec>,
    session: tqsdk_session::SessionClient,
    chart_id: String,
}

impl TickWindowStream {
    pub(crate) fn new(
        inner: PathCommitStream,
        session: tqsdk_session::SessionClient,
        reader: tqsdk_core::RuntimeReader,
        symbol: String,
        view_width: usize,
        chart_id: String,
    ) -> Self {
        Self {
            inner: ProjectedValueStream::new(
                inner,
                reader,
                TickWindowSpec {
                    symbol,
                    view_width,
                    chart_id: chart_id.clone(),
                },
                project_tick_window,
            ),
            session,
            chart_id,
        }
    }

    #[must_use]
    pub fn chart_id(&self) -> &str {
        &self.chart_id
    }

    pub async fn close(self) -> Result<()> {
        self.session
            .submit(RuntimeCommand::Market(MarketCommand::CancelChart {
                chart_id: self.chart_id,
            }))
            .await?;
        Ok(())
    }
}

impl Stream for TickWindowStream {
    type Item = Result<ValueUpdate<TickWindow>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll_next(cx)
    }
}

pub(crate) fn kline_chart_id(symbol: &str, duration_ns: i64, view_width: usize) -> String {
    format!(
        "stream-kline-{}-{duration_ns}-{view_width}",
        sanitize_chart_token(symbol)
    )
}

pub(crate) fn tick_chart_id(symbol: &str, view_width: usize) -> String {
    format!("stream-tick-{}-{view_width}", sanitize_chart_token(symbol))
}

fn sanitize_chart_token(raw: &str) -> String {
    raw.chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

pub(crate) fn project_kline_window(
    market: MarketStateReadGuard<'_>,
    spec: &KlineWindowSpec,
) -> Result<Option<KlineWindow>> {
    project_kline_window_from_market(&market, spec)
}

pub(crate) fn project_kline_window_from_market(
    market: &MarketStateReadGuard<'_>,
    spec: &KlineWindowSpec,
) -> Result<Option<KlineWindow>> {
    if !chart_is_ready(market, spec.chart_id.as_str()) {
        return Ok(None);
    }

    let window = read_kline_window(market, spec)?;
    if window.is_empty() {
        return Ok(None);
    }

    Ok(Some(window))
}

pub(crate) fn project_tick_window(
    market: MarketStateReadGuard<'_>,
    spec: &TickWindowSpec,
) -> Result<Option<TickWindow>> {
    project_tick_window_from_market(&market, spec)
}

pub(crate) fn project_tick_window_from_market(
    market: &MarketStateReadGuard<'_>,
    spec: &TickWindowSpec,
) -> Result<Option<TickWindow>> {
    if !chart_is_ready(market, spec.chart_id.as_str()) {
        return Ok(None);
    }

    let window = read_tick_window(market, spec)?;
    if window.is_empty() {
        return Ok(None);
    }

    Ok(Some(window))
}

fn chart_is_ready(market: &MarketStateReadGuard<'_>, chart_id: &str) -> bool {
    let ready = market
        .get_path(&["charts", chart_id, "ready"])
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    let more_data = market
        .get_path(&["charts", chart_id, "more_data"])
        .and_then(|value| value.as_bool())
        .unwrap_or(true);

    ready && !more_data
}

fn read_kline_window(
    market: &MarketStateReadGuard<'_>,
    spec: &KlineWindowSpec,
) -> Result<KlineWindow> {
    let duration_key = spec.duration_ns.to_string();
    let data_path = [
        "klines",
        spec.symbol.as_str(),
        duration_key.as_str(),
        "data",
    ];
    let mut rows = Vec::new();

    if let Some(data) = market
        .get_path(&data_path)
        .and_then(|value| value.as_object())
    {
        let mut ids = data
            .keys()
            .filter_map(|key| key.parse::<i64>().ok())
            .collect::<Vec<_>>();
        ids.sort_unstable();

        for id in ids.into_iter().rev().take(spec.view_width).rev() {
            let id_key = id.to_string();
            if let Some(row) = market.decode_path::<Kline>(&[
                "klines",
                spec.symbol.as_str(),
                duration_key.as_str(),
                "data",
                id_key.as_str(),
            ])? {
                rows.push(row);
            }
        }
    }

    Ok(KlineWindow::new(
        spec.symbol.clone(),
        spec.duration_ns,
        spec.view_width,
        spec.chart_id.clone(),
        rows,
    ))
}

fn read_tick_window(
    market: &MarketStateReadGuard<'_>,
    spec: &TickWindowSpec,
) -> Result<TickWindow> {
    let mut rows = Vec::new();

    if let Some(data) = market
        .get_path(&["ticks", spec.symbol.as_str(), "data"])
        .and_then(|value| value.as_object())
    {
        let mut ids = data
            .keys()
            .filter_map(|key| key.parse::<i64>().ok())
            .collect::<Vec<_>>();
        ids.sort_unstable();

        for id in ids.into_iter().rev().take(spec.view_width).rev() {
            let id_key = id.to_string();
            if let Some(row) = market.decode_path::<Tick>(&[
                "ticks",
                spec.symbol.as_str(),
                "data",
                id_key.as_str(),
            ])? {
                rows.push(row);
            }
        }
    }

    Ok(TickWindow::new(
        spec.symbol.clone(),
        spec.view_width,
        spec.chart_id.clone(),
        rows,
    ))
}

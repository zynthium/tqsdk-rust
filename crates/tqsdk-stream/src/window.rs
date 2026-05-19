#![cfg_attr(not(test), forbid(unsafe_code))]

use std::cmp::Ordering;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use tqsdk_core::{
    CommitResult, CommitScope, Kline, MarketStateReadGuard, ObjectKey, StatePath, Symbol, Tick,
};

use crate::{PathCommitStream, Result, ValueUpdate};

/// Kind of row batch emitted by kline/tick streams.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RowBatchKind {
    InitialSnapshot,
    #[default]
    Delta,
    ResyncSnapshot,
}

/// Owned kline rows made visible by one stream commit.
#[derive(Debug, Clone, Default)]
pub struct KlineRowBatch {
    symbol: String,
    duration_ns: i64,
    view_width: usize,
    chart_id: String,
    kind: RowBatchKind,
    rows: Vec<Kline>,
}

impl KlineRowBatch {
    #[must_use]
    pub fn new(
        symbol: String,
        duration_ns: i64,
        view_width: usize,
        chart_id: String,
        kind: RowBatchKind,
        rows: Vec<Kline>,
    ) -> Self {
        Self {
            symbol,
            duration_ns,
            view_width,
            chart_id,
            kind,
            rows,
        }
    }

    #[must_use]
    pub fn rows(&self) -> &[Kline] {
        &self.rows
    }

    #[must_use]
    pub fn into_rows(self) -> Vec<Kline> {
        self.rows
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

    #[must_use]
    pub fn kind(&self) -> RowBatchKind {
        self.kind
    }
}

/// Owned tick rows made visible by one stream commit.
#[derive(Debug, Clone, Default)]
pub struct TickRowBatch {
    symbol: String,
    view_width: usize,
    chart_id: String,
    kind: RowBatchKind,
    rows: Vec<Tick>,
}

impl TickRowBatch {
    #[must_use]
    pub fn new(
        symbol: String,
        view_width: usize,
        chart_id: String,
        kind: RowBatchKind,
        rows: Vec<Tick>,
    ) -> Self {
        Self {
            symbol,
            view_width,
            chart_id,
            kind,
            rows,
        }
    }

    #[must_use]
    pub fn rows(&self) -> &[Tick] {
        &self.rows
    }

    #[must_use]
    pub fn into_rows(self) -> Vec<Tick> {
        self.rows
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

    #[must_use]
    pub fn kind(&self) -> RowBatchKind {
        self.kind
    }
}

#[derive(Debug, Clone)]
pub(crate) struct KlineRowSpec {
    pub(crate) symbol: String,
    pub(crate) duration_ns: i64,
    pub(crate) view_width: usize,
    pub(crate) chart_id: String,
}

#[derive(Debug, Clone)]
pub(crate) struct TickRowSpec {
    pub(crate) symbol: String,
    pub(crate) view_width: usize,
    pub(crate) chart_id: String,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct RowProjectionCursor {
    emitted_snapshot: bool,
    bounds: Option<(i64, i64)>,
}

#[derive(Debug, Clone)]
pub(crate) struct KlineProjection {
    pub(crate) spec: KlineRowSpec,
    pub(crate) cursor: RowProjectionCursor,
}

#[derive(Debug, Clone)]
pub(crate) struct TickProjection {
    pub(crate) spec: TickRowSpec,
    pub(crate) cursor: RowProjectionCursor,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct KlineSeriesTouch {
    symbol: String,
    duration_ns: i64,
}

#[derive(Debug, Clone)]
struct TickRowTouch {
    symbol: String,
    row_ids: Vec<i64>,
}

#[derive(Debug, Clone)]
struct KlineRowTouch {
    series: KlineSeriesTouch,
    row_ids: Vec<i64>,
}

/// Parsed market-object touches for one commit.
#[derive(Debug, Clone)]
pub(crate) struct CommitTouchSet {
    scope: CommitScope,
    quote_symbols: Vec<Symbol>,
    chart_ids: Vec<String>,
    tick_rows: Vec<TickRowTouch>,
    kline_rows: Vec<KlineRowTouch>,
}

impl Default for CommitTouchSet {
    fn default() -> Self {
        Self::with_capacity(CommitScope::RealtimeUpdate, 0)
    }
}

impl CommitTouchSet {
    #[must_use]
    pub(crate) fn from_commit(commit: &CommitResult) -> Self {
        let hit_count = commit.changes.object_hits.len()
            + commit.changes.path_hits.len()
            + commit.changes.field_hits.len();
        let mut touches = Self::with_capacity(commit.scope, hit_count);

        for object in &commit.changes.object_hits {
            touches.record_object(object);
        }
        for path in &commit.changes.path_hits {
            touches.record_path(path);
        }
        for hit in &commit.changes.field_hits {
            touches.record_object(&hit.object);
            touches.record_path(&hit.path);
        }

        touches
    }

    fn with_capacity(scope: CommitScope, hit_count: usize) -> Self {
        Self {
            scope,
            quote_symbols: touch_vec(hit_count),
            chart_ids: touch_vec(hit_count),
            tick_rows: touch_vec(hit_count),
            kline_rows: touch_vec(hit_count),
        }
    }

    pub(crate) fn quote_symbols(&self) -> impl Iterator<Item = &Symbol> {
        self.quote_symbols.iter()
    }

    pub(crate) fn chart_ids(&self) -> impl Iterator<Item = &str> {
        self.chart_ids.iter().map(String::as_str)
    }

    pub(crate) fn tick_symbols(&self) -> impl Iterator<Item = &str> {
        self.tick_rows.iter().map(|touch| touch.symbol.as_str())
    }

    pub(crate) fn kline_series(&self) -> impl Iterator<Item = (&str, i64)> {
        self.kline_rows
            .iter()
            .map(|touch| (touch.series.symbol.as_str(), touch.series.duration_ns))
    }

    fn tick_row_ids(&self, spec: &TickRowSpec) -> Option<&[i64]> {
        self.tick_rows
            .binary_search_by(|touch| touch.symbol.as_str().cmp(spec.symbol.as_str()))
            .ok()
            .map(|index| self.tick_rows[index].row_ids.as_slice())
    }

    fn kline_row_ids(&self, spec: &KlineRowSpec) -> Option<&[i64]> {
        self.kline_rows
            .binary_search_by(|touch| touch.series.cmp_key(spec.symbol.as_str(), spec.duration_ns))
            .ok()
            .map(|index| self.kline_rows[index].row_ids.as_slice())
    }

    fn record_object(&mut self, object: &ObjectKey) {
        match object {
            ObjectKey::Quote { symbol } => {
                insert_sorted_unique(&mut self.quote_symbols, symbol.clone());
            }
            ObjectKey::Chart { chart_id } => {
                insert_sorted_unique(&mut self.chart_ids, chart_id.as_str().to_string());
            }
            ObjectKey::Tick { symbol, tick_id } => {
                self.record_tick_row(symbol.as_str(), *tick_id);
            }
            ObjectKey::Kline { series, bar_id } => {
                self.record_kline_row(series.primary.as_str(), series.duration_ns, *bar_id);
            }
            _ => {}
        }
    }

    fn record_path(&mut self, path: &StatePath) {
        let segments = path.segments();
        match segments {
            [root, symbol, ..] if root == "quotes" => {
                insert_sorted_unique(&mut self.quote_symbols, Symbol::new(symbol.clone()));
            }
            [root, chart_id, ..] if root == "charts" => {
                insert_sorted_unique(&mut self.chart_ids, chart_id.clone());
            }
            [root, symbol, branch, row_id, ..] if root == "ticks" && branch == "data" => {
                if let Ok(row_id) = row_id.parse::<i64>() {
                    self.record_tick_row(symbol, row_id);
                }
            }
            [root, symbol, row_id, ..] if root == "ticks" => {
                if let Ok(row_id) = row_id.parse::<i64>() {
                    self.record_tick_row(symbol, row_id);
                }
            }
            [root, symbol, duration, branch, row_id, ..]
                if root == "klines" && branch == "data" =>
            {
                if let (Ok(duration_ns), Ok(row_id)) =
                    (duration.parse::<i64>(), row_id.parse::<i64>())
                {
                    self.record_kline_row(symbol, duration_ns, row_id);
                }
            }
            [root, symbol, duration, row_id, ..] if root == "klines" => {
                if let (Ok(duration_ns), Ok(row_id)) =
                    (duration.parse::<i64>(), row_id.parse::<i64>())
                {
                    self.record_kline_row(symbol, duration_ns, row_id);
                }
            }
            _ => {}
        }
    }

    fn record_tick_row(&mut self, symbol: &str, row_id: i64) {
        match self
            .tick_rows
            .binary_search_by(|touch| touch.symbol.as_str().cmp(symbol))
        {
            Ok(index) => insert_sorted_unique(&mut self.tick_rows[index].row_ids, row_id),
            Err(index) => self
                .tick_rows
                .insert(index, TickRowTouch::new(symbol, row_id)),
        }
    }

    fn record_kline_row(&mut self, symbol: &str, duration_ns: i64, row_id: i64) {
        match self
            .kline_rows
            .binary_search_by(|touch| touch.series.cmp_key(symbol, duration_ns))
        {
            Ok(index) => insert_sorted_unique(&mut self.kline_rows[index].row_ids, row_id),
            Err(index) => {
                self.kline_rows
                    .insert(index, KlineRowTouch::new(symbol, duration_ns, row_id));
            }
        }
    }
}

impl TickRowTouch {
    fn new(symbol: &str, row_id: i64) -> Self {
        Self {
            symbol: symbol.to_string(),
            row_ids: vec![row_id],
        }
    }
}

impl KlineRowTouch {
    fn new(symbol: &str, duration_ns: i64, row_id: i64) -> Self {
        Self {
            series: KlineSeriesTouch::new(symbol, duration_ns),
            row_ids: vec![row_id],
        }
    }
}

impl KlineSeriesTouch {
    fn new(symbol: impl AsRef<str>, duration_ns: i64) -> Self {
        Self {
            symbol: symbol.as_ref().to_string(),
            duration_ns,
        }
    }

    fn cmp_key(&self, symbol: &str, duration_ns: i64) -> Ordering {
        self.symbol
            .as_str()
            .cmp(symbol)
            .then_with(|| self.duration_ns.cmp(&duration_ns))
    }
}

fn insert_sorted_unique<T: Ord>(items: &mut Vec<T>, item: T) {
    if let Err(index) = items.binary_search(&item) {
        items.insert(index, item);
    }
}

fn touch_vec<T>(hit_count: usize) -> Vec<T> {
    if hit_count > 4 {
        Vec::with_capacity(hit_count.min(16))
    } else {
        Vec::new()
    }
}

struct ProjectedValueStream<T, C> {
    inner: PathCommitStream,
    reader: tqsdk_core::RuntimeReader,
    context: C,
    projector: for<'a> fn(&MarketStateReadGuard<'a>, &CommitTouchSet, &mut C) -> Result<Option<T>>,
    marker: PhantomData<fn() -> T>,
}

impl<T, C> ProjectedValueStream<T, C> {
    fn new(
        inner: PathCommitStream,
        reader: tqsdk_core::RuntimeReader,
        context: C,
        projector: for<'a> fn(
            &MarketStateReadGuard<'a>,
            &CommitTouchSet,
            &mut C,
        ) -> Result<Option<T>>,
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
                    let touches = CommitTouchSet::from_commit(&commit);
                    let market = this.reader.read_market_state();
                    match (this.projector)(&market, &touches, &mut this.context) {
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

/// Commit-driven stream of ready kline row batches.
pub struct KlineRowStream {
    inner: ProjectedValueStream<KlineRowBatch, KlineProjection>,
    lease: tqsdk_session::MarketChartLease,
    chart_id: String,
}

impl KlineRowStream {
    pub(crate) fn new(
        inner: PathCommitStream,
        lease: tqsdk_session::MarketChartLease,
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
                KlineProjection {
                    spec: KlineRowSpec {
                        symbol,
                        duration_ns,
                        view_width,
                        chart_id: chart_id.clone(),
                    },
                    cursor: RowProjectionCursor::default(),
                },
                project_kline_rows,
            ),
            lease,
            chart_id,
        }
    }

    #[must_use]
    pub fn chart_id(&self) -> &str {
        &self.chart_id
    }

    pub async fn close(self) -> Result<()> {
        self.lease.close().await.map_err(Into::into)
    }
}

impl Stream for KlineRowStream {
    type Item = Result<ValueUpdate<KlineRowBatch>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll_next(cx)
    }
}

/// Commit-driven stream of ready tick row batches.
pub struct TickRowStream {
    inner: ProjectedValueStream<TickRowBatch, TickProjection>,
    lease: tqsdk_session::MarketChartLease,
    chart_id: String,
}

impl TickRowStream {
    pub(crate) fn new(
        inner: PathCommitStream,
        lease: tqsdk_session::MarketChartLease,
        reader: tqsdk_core::RuntimeReader,
        symbol: String,
        view_width: usize,
        chart_id: String,
    ) -> Self {
        Self {
            inner: ProjectedValueStream::new(
                inner,
                reader,
                TickProjection {
                    spec: TickRowSpec {
                        symbol,
                        view_width,
                        chart_id: chart_id.clone(),
                    },
                    cursor: RowProjectionCursor::default(),
                },
                project_tick_rows,
            ),
            lease,
            chart_id,
        }
    }

    #[must_use]
    pub fn chart_id(&self) -> &str {
        &self.chart_id
    }

    pub async fn close(self) -> Result<()> {
        self.lease.close().await.map_err(Into::into)
    }
}

impl Stream for TickRowStream {
    type Item = Result<ValueUpdate<TickRowBatch>>;

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

fn project_kline_rows(
    market: &MarketStateReadGuard<'_>,
    touches: &CommitTouchSet,
    projection: &mut KlineProjection,
) -> Result<Option<KlineRowBatch>> {
    project_kline_rows_from_market(market, &projection.spec, &mut projection.cursor, touches)
}

fn project_tick_rows(
    market: &MarketStateReadGuard<'_>,
    touches: &CommitTouchSet,
    projection: &mut TickProjection,
) -> Result<Option<TickRowBatch>> {
    project_tick_rows_from_market(market, &projection.spec, &mut projection.cursor, touches)
}

pub(crate) fn project_kline_rows_from_market(
    market: &MarketStateReadGuard<'_>,
    spec: &KlineRowSpec,
    cursor: &mut RowProjectionCursor,
    touches: &CommitTouchSet,
) -> Result<Option<KlineRowBatch>> {
    if !chart_is_ready(market, spec.chart_id.as_str()) {
        return Ok(None);
    }

    let Some(bounds) = chart_bounds(market, spec.chart_id.as_str()) else {
        return Ok(None);
    };

    let kind = batch_kind(cursor, bounds, touches.scope);
    let rows = match kind {
        RowBatchKind::InitialSnapshot | RowBatchKind::ResyncSnapshot => {
            read_kline_rows_in_range(market, spec, bounds)?
        }
        RowBatchKind::Delta => {
            let rows = touches
                .kline_row_ids(spec)
                .into_iter()
                .flat_map(|ids| ids.iter().copied())
                .filter(|id| bounds.0 <= *id && *id <= bounds.1);
            read_kline_rows_by_id(market, spec, rows)?
        }
    };

    cursor.emitted_snapshot = true;
    cursor.bounds = Some(bounds);

    if rows.is_empty() && kind == RowBatchKind::Delta {
        return Ok(None);
    }

    Ok(Some(KlineRowBatch::new(
        spec.symbol.clone(),
        spec.duration_ns,
        spec.view_width,
        spec.chart_id.clone(),
        kind,
        rows,
    )))
}

pub(crate) fn project_tick_rows_from_market(
    market: &MarketStateReadGuard<'_>,
    spec: &TickRowSpec,
    cursor: &mut RowProjectionCursor,
    touches: &CommitTouchSet,
) -> Result<Option<TickRowBatch>> {
    if !chart_is_ready(market, spec.chart_id.as_str()) {
        return Ok(None);
    }

    let Some(bounds) = chart_bounds(market, spec.chart_id.as_str()) else {
        return Ok(None);
    };

    let kind = batch_kind(cursor, bounds, touches.scope);
    let rows = match kind {
        RowBatchKind::InitialSnapshot | RowBatchKind::ResyncSnapshot => {
            read_tick_rows_in_range(market, spec, bounds)?
        }
        RowBatchKind::Delta => {
            let rows = touches
                .tick_row_ids(spec)
                .into_iter()
                .flat_map(|ids| ids.iter().copied())
                .filter(|id| bounds.0 <= *id && *id <= bounds.1);
            read_tick_rows_by_id(market, spec, rows)?
        }
    };

    cursor.emitted_snapshot = true;
    cursor.bounds = Some(bounds);

    if rows.is_empty() && kind == RowBatchKind::Delta {
        return Ok(None);
    }

    Ok(Some(TickRowBatch::new(
        spec.symbol.clone(),
        spec.view_width,
        spec.chart_id.clone(),
        kind,
        rows,
    )))
}

fn batch_kind(
    cursor: &RowProjectionCursor,
    bounds: (i64, i64),
    scope: CommitScope,
) -> RowBatchKind {
    if !cursor.emitted_snapshot {
        return RowBatchKind::InitialSnapshot;
    }

    if scope == CommitScope::ResyncRecovery
        || cursor
            .bounds
            .is_some_and(|previous| bounds.0 < previous.0 || bounds.1 < previous.1)
    {
        RowBatchKind::ResyncSnapshot
    } else {
        RowBatchKind::Delta
    }
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

fn chart_bounds(market: &MarketStateReadGuard<'_>, chart_id: &str) -> Option<(i64, i64)> {
    let left_id = market
        .get_path(&["charts", chart_id, "left_id"])
        .and_then(|value| value.as_i64())?;
    let right_id = market
        .get_path(&["charts", chart_id, "right_id"])
        .and_then(|value| value.as_i64())?;

    (left_id <= right_id).then_some((left_id, right_id))
}

fn read_kline_rows_in_range(
    market: &MarketStateReadGuard<'_>,
    spec: &KlineRowSpec,
    bounds: (i64, i64),
) -> Result<Vec<Kline>> {
    read_kline_rows_by_id(market, spec, bounds.0..=bounds.1)
}

fn read_kline_rows_by_id(
    market: &MarketStateReadGuard<'_>,
    spec: &KlineRowSpec,
    ids: impl IntoIterator<Item = i64>,
) -> Result<Vec<Kline>> {
    let duration_key = spec.duration_ns.to_string();
    let mut rows = Vec::new();

    for id in ids {
        if let Some(row) = with_i64_path_segment(id, |id_key| {
            market.decode_path::<Kline>(&[
                "klines",
                spec.symbol.as_str(),
                duration_key.as_str(),
                "data",
                id_key,
            ])
        })? {
            rows.push(row);
        }
    }

    Ok(rows)
}

fn read_tick_rows_in_range(
    market: &MarketStateReadGuard<'_>,
    spec: &TickRowSpec,
    bounds: (i64, i64),
) -> Result<Vec<Tick>> {
    read_tick_rows_by_id(market, spec, bounds.0..=bounds.1)
}

fn read_tick_rows_by_id(
    market: &MarketStateReadGuard<'_>,
    spec: &TickRowSpec,
    ids: impl IntoIterator<Item = i64>,
) -> Result<Vec<Tick>> {
    let mut rows = Vec::new();

    for id in ids {
        if let Some(row) = with_i64_path_segment(id, |id_key| {
            market.decode_path::<Tick>(&["ticks", spec.symbol.as_str(), "data", id_key])
        })? {
            rows.push(row);
        }
    }

    Ok(rows)
}

fn with_i64_path_segment<T>(value: i64, f: impl FnOnce(&str) -> T) -> T {
    let mut buffer = [0_u8; 20];
    let mut cursor = buffer.len();
    let mut remaining = value.unsigned_abs();

    if remaining == 0 {
        cursor -= 1;
        buffer[cursor] = b'0';
    } else {
        while remaining > 0 {
            cursor -= 1;
            buffer[cursor] = b'0' + (remaining % 10) as u8;
            remaining /= 10;
        }
    }

    if value < 0 {
        cursor -= 1;
        buffer[cursor] = b'-';
    }

    let segment = std::str::from_utf8(&buffer[cursor..])
        .expect("integer path segment should always be valid UTF-8");
    f(segment)
}

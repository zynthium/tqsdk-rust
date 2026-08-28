use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tqsdk_core::{Kline, Tick};
use tqsdk_data::{
    HistorySeriesCache, KlineDataSeriesRequest, KlineSessionTemplate, MinuteKlineCache,
    MinuteKlineCacheSnapshot, MinuteKlineReader, TickDataSeriesReader, TickDataSeriesRequest,
    TickKlineAggregator,
};

use crate::{
    BacktestMarketStream, CANONICAL_MINUTE_KLINE_NS, MinuteKlineAggregator,
    MinuteKlineSessionTemplate, ReplayMarketEvent, Result, TaskError,
};

#[derive(Debug, Clone)]
pub struct HistoryBacktestKlineRequest {
    pub symbol: String,
    pub duration_ns: i64,
}

pub struct HistoryBacktestReplayRequest {
    pub cache: HistorySeriesCache,
    pub start_ns: i64,
    pub end_ns: i64,
    pub tick_symbols: Vec<String>,
    pub native_klines: Vec<HistoryBacktestKlineRequest>,
    pub synthetic_klines: Vec<HistoryBacktestKlineRequest>,
}

/// One cache-backed tick range replayed under a possibly different symbol.
///
/// This lets a continuous contract reuse the physical contract's history while
/// keeping its stable strategy-facing symbol during replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryBacktestTickSource {
    pub replay_symbol: String,
    pub cache_symbol: String,
    pub start_ns: i64,
    pub end_ns: i64,
}

/// One synthetic kline stream sourced from a projected tick range.
///
/// Its Tick source may begin before the replay request's start solely to
/// establish the cumulative-volume baseline.  Such priming rows update the
/// aggregator but are never emitted as replay events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryBacktestSyntheticKlineSource {
    pub tick_source: HistoryBacktestTickSource,
    pub duration_ns: i64,
}

/// One interval where a logical minute-Kline replay symbol maps to a concrete
/// trade/quote underlying.
///
/// The cache key stays logical (for example `KQ.m@...`), while this mapping is
/// applied to each replay event so local simulation can transact the dated
/// concrete contract without duplicating minute-cache files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryBacktestMinuteKlineUnderlyingSegment {
    pub start_ns: i64,
    pub end_ns: i64,
    pub underlying_symbol: String,
}

/// One canonical-minute cache stream projected into a replay Kline serial.
///
/// `duration_ns == 60s` replays the durable canonical series.  Larger periods
/// must be integer multiples of 60s and are aggregated locally from closed
/// canonical minutes; no higher-period cache or remote history series is read.
#[derive(Debug, Clone)]
pub struct HistoryBacktestMinuteKlineSource {
    pub cache: MinuteKlineCache,
    pub snapshot: MinuteKlineCacheSnapshot,
    pub replay_symbol: String,
    pub cache_symbol: String,
    pub start_ns: i64,
    pub end_ns: i64,
    pub duration_ns: i64,
    pub session: MinuteKlineSessionTemplate,
    pub underlying_segments: Vec<HistoryBacktestMinuteKlineUnderlyingSegment>,
}

/// One already materialized native-Kline stream owned by the caller.
///
/// The data layer remains responsible for reading and aggregating durable
/// history. The task layer only merges these final rows into replay order.
#[derive(Debug, Clone)]
pub struct HistoryBacktestNativeKlineSource {
    pub replay_symbol: String,
    pub duration_ns: i64,
    pub rows: Vec<Kline>,
    pub underlying_segments: Vec<HistoryBacktestMinuteKlineUnderlyingSegment>,
}

/// Replay request with explicit logical-to-physical tick projections.
pub struct HistoryBacktestProjectedReplayRequest {
    pub cache: HistorySeriesCache,
    pub start_ns: i64,
    pub end_ns: i64,
    pub tick_sources: Vec<HistoryBacktestTickSource>,
    pub native_klines: Vec<HistoryBacktestKlineRequest>,
    pub synthetic_kline_sources: Vec<HistoryBacktestSyntheticKlineSource>,
    pub minute_kline_sources: Vec<HistoryBacktestMinuteKlineSource>,
}

pub struct HistoryBacktestReplayStream {
    cursors: Vec<HistoryCursor>,
    heap: BinaryHeap<HeapItem>,
    refill_cursor: Option<usize>,
}

struct HistoryCursor {
    symbol: String,
    underlying_projection: UnderlyingProjection,
    symbol_rank: usize,
    producer: CursorProducer,
    next: Option<QueuedEvent>,
}

enum UnderlyingProjection {
    None,
    Static(String),
    Segments(Vec<HistoryBacktestMinuteKlineUnderlyingSegment>),
}

enum CursorProducer {
    Tick {
        reader: TickDataSeriesReader,
    },
    ProjectedTick(Box<ProjectedTickProducer>),
    SyntheticKline {
        reader: TickDataSeriesReader,
        synth: Box<TickKlineAggregator>,
        emit_range: (i64, i64),
    },
    ProjectedSyntheticKline(Box<ProjectedSyntheticKlineProducer>),
    NativeKline {
        events: VecDeque<QueuedEvent>,
    },
    MinuteKline(Box<MinuteKlineProducer>),
}

struct MinuteKlineProducer {
    reader: MinuteKlineReader,
    duration_ns: i64,
    aggregator: Option<MinuteKlineAggregator>,
    pending: VecDeque<QueuedEvent>,
}

struct ProjectedTickProducer {
    cache: HistorySeriesCache,
    sources: VecDeque<HistoryBacktestTickSource>,
    current: Option<ProjectedTickReader>,
}

struct ProjectedTickReader {
    reader: TickDataSeriesReader,
    underlying_symbol: Option<Arc<str>>,
}

struct ProjectedTick {
    tick: Tick,
    underlying_symbol: Option<Arc<str>>,
}

struct ProjectedSyntheticKlineProducer {
    ticks: ProjectedTickProducer,
    synth: Box<TickKlineAggregator>,
    emit_range: (i64, i64),
}

struct QueuedEvent {
    event: ReplayMarketEvent,
    source_rank: u8,
    row_id: i64,
    source_datetime_ns: i64,
}

#[derive(Debug, Clone, Copy)]
struct HeapItem {
    cursor_index: usize,
    event_time_ns: i64,
    source_rank: u8,
    symbol_rank: usize,
    row_id: i64,
}

impl HistoryBacktestReplayStream {
    pub fn new(request: HistoryBacktestReplayRequest) -> Result<Self> {
        validate_request_range(request.start_ns, request.end_ns)?;
        let mut cursors = Vec::new();

        for symbol in request.tick_symbols {
            validate_symbol(&symbol)?;
            let reader = request
                .cache
                .open_tick_data_series_reader(TickDataSeriesRequest::new(
                    &symbol,
                    request.start_ns,
                    request.end_ns,
                ))
                .map_err(data_error_to_task)?;
            cursors.push(HistoryCursor {
                symbol,
                underlying_projection: UnderlyingProjection::None,
                symbol_rank: 0,
                producer: CursorProducer::Tick { reader },
                next: None,
            });
        }

        for spec in request.synthetic_klines {
            validate_kline_request(&spec)?;
            let reader = request
                .cache
                .open_tick_data_series_reader(TickDataSeriesRequest::new(
                    &spec.symbol,
                    request.start_ns,
                    request.end_ns,
                ))
                .map_err(data_error_to_task)?;
            let synth = TickKlineAggregator::new(
                spec.symbol.clone(),
                spec.duration_ns,
                KlineSessionTemplate::cst_trading_day(),
            )
            .map_err(data_error_to_task)?;
            cursors.push(HistoryCursor {
                symbol: spec.symbol,
                underlying_projection: UnderlyingProjection::None,
                symbol_rank: 0,
                producer: CursorProducer::SyntheticKline {
                    reader,
                    synth: Box::new(synth),
                    emit_range: (request.start_ns, request.end_ns),
                },
                next: None,
            });
        }

        for spec in request.native_klines {
            validate_kline_request(&spec)?;
            let duration = Duration::from_nanos(spec.duration_ns as u64);
            let series = request
                .cache
                .read_kline_data_series(KlineDataSeriesRequest::new(
                    &spec.symbol,
                    duration,
                    request.start_ns,
                    request.end_ns,
                ))
                .map_err(data_error_to_task)?;
            let events = native_kline_events(
                &spec.symbol,
                spec.duration_ns,
                request.start_ns,
                request.end_ns,
                series.into_rows(),
            )?;
            cursors.push(HistoryCursor {
                symbol: spec.symbol,
                underlying_projection: UnderlyingProjection::None,
                symbol_rank: 0,
                producer: CursorProducer::NativeKline { events },
                next: None,
            });
        }

        let symbol_ranks = cursors
            .iter()
            .map(|cursor| cursor.symbol.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .enumerate()
            .map(|(rank, symbol)| (symbol, rank))
            .collect::<BTreeMap<_, _>>();
        let mut heap = BinaryHeap::new();
        for (cursor_index, cursor) in cursors.iter_mut().enumerate() {
            cursor.symbol_rank = symbol_ranks
                .get(cursor.symbol.as_str())
                .copied()
                .unwrap_or(cursor_index);
            push_next_event(cursor, cursor_index, &mut heap)?;
        }

        Ok(Self {
            cursors,
            heap,
            refill_cursor: None,
        })
    }

    pub fn new_projected(request: HistoryBacktestProjectedReplayRequest) -> Result<Self> {
        Self::new_projected_with_sessions(request, BTreeMap::new())
    }

    /// Builds a projected replay using persisted data-layer session templates
    /// for Tick-derived Klines.
    ///
    /// Callers that do not have persisted metadata can continue using
    /// [`Self::new_projected`], which retains the historical CST-day fallback.
    /// The facade uses this constructor after the data layer has resolved the
    /// authoritative calendar/session snapshot.
    pub fn new_projected_with_sessions(
        request: HistoryBacktestProjectedReplayRequest,
        sessions_by_symbol: BTreeMap<String, KlineSessionTemplate>,
    ) -> Result<Self> {
        Self::new_projected_with_sessions_and_native_klines(request, sessions_by_symbol, Vec::new())
    }

    /// Builds projected replay plus caller-owned final Kline rows.
    pub fn new_projected_with_sessions_and_native_klines(
        request: HistoryBacktestProjectedReplayRequest,
        sessions_by_symbol: BTreeMap<String, KlineSessionTemplate>,
        native_kline_sources: Vec<HistoryBacktestNativeKlineSource>,
    ) -> Result<Self> {
        Self::new_projected_inner(request, &sessions_by_symbol, native_kline_sources)
    }

    fn new_projected_inner(
        request: HistoryBacktestProjectedReplayRequest,
        sessions_by_symbol: &BTreeMap<String, KlineSessionTemplate>,
        native_kline_sources: Vec<HistoryBacktestNativeKlineSource>,
    ) -> Result<Self> {
        validate_request_range(request.start_ns, request.end_ns)?;
        let mut cursors = Vec::new();
        let cache_dir = request.cache.root_dir().to_path_buf();

        let mut tick_sources = Vec::with_capacity(request.tick_sources.len());
        for source in request.tick_sources {
            validate_projected_tick_source(&source, request.start_ns, request.end_ns)?;
            tick_sources.push(source);
        }
        for sources in consecutive_projected_tick_source_chains(tick_sources) {
            let symbol = sources
                .first()
                .expect("a projected tick source chain is non-empty")
                .replay_symbol
                .clone();
            cursors.push(HistoryCursor {
                symbol,
                underlying_projection: UnderlyingProjection::None,
                symbol_rank: 0,
                producer: CursorProducer::ProjectedTick(Box::new(ProjectedTickProducer::new(
                    &cache_dir, sources,
                ))),
                next: None,
            });
        }

        let mut synthetic_sources = Vec::with_capacity(request.synthetic_kline_sources.len());
        for source in request.synthetic_kline_sources {
            validate_projected_synthetic_tick_source(
                &source.tick_source,
                request.start_ns,
                request.end_ns,
            )?;
            if source.duration_ns <= 0 {
                return Err(TaskError::InvalidState(
                    "projected synthetic kline duration_ns must be positive",
                ));
            }
            synthetic_sources.push(source);
        }
        for sources in consecutive_projected_synthetic_source_chains(synthetic_sources) {
            let first = sources
                .first()
                .expect("a projected synthetic source chain is non-empty");
            let symbol = first.tick_source.replay_symbol.clone();
            let duration_ns = first.duration_ns;
            let synth = TickKlineAggregator::new(
                symbol.clone(),
                duration_ns,
                sessions_by_symbol
                    .get(symbol.as_str())
                    .cloned()
                    .unwrap_or_else(KlineSessionTemplate::cst_trading_day),
            )
            .map_err(data_error_to_task)?;
            let tick_sources = sources
                .into_iter()
                .map(|source| source.tick_source)
                .collect();
            cursors.push(HistoryCursor {
                symbol,
                underlying_projection: UnderlyingProjection::None,
                symbol_rank: 0,
                producer: CursorProducer::ProjectedSyntheticKline(Box::new(
                    ProjectedSyntheticKlineProducer {
                        ticks: ProjectedTickProducer::new(&cache_dir, tick_sources),
                        synth: Box::new(synth),
                        emit_range: (request.start_ns, request.end_ns),
                    },
                )),
                next: None,
            });
        }

        for spec in request.native_klines {
            validate_kline_request(&spec)?;
            let duration = Duration::from_nanos(spec.duration_ns as u64);
            let series = request
                .cache
                .read_kline_data_series(KlineDataSeriesRequest::new(
                    &spec.symbol,
                    duration,
                    request.start_ns,
                    request.end_ns,
                ))
                .map_err(data_error_to_task)?;
            let events = native_kline_events(
                &spec.symbol,
                spec.duration_ns,
                request.start_ns,
                request.end_ns,
                series.into_rows(),
            )?;
            cursors.push(HistoryCursor {
                symbol: spec.symbol,
                underlying_projection: UnderlyingProjection::None,
                symbol_rank: 0,
                producer: CursorProducer::NativeKline { events },
                next: None,
            });
        }

        for source in native_kline_sources {
            validate_kline_request(&HistoryBacktestKlineRequest {
                symbol: source.replay_symbol.clone(),
                duration_ns: source.duration_ns,
            })?;
            let events = native_kline_events(
                &source.replay_symbol,
                source.duration_ns,
                request.start_ns,
                request.end_ns,
                source.rows,
            )?;
            let underlying_projection = if source.underlying_segments.is_empty() {
                UnderlyingProjection::None
            } else {
                UnderlyingProjection::Segments(source.underlying_segments)
            };
            cursors.push(HistoryCursor {
                symbol: source.replay_symbol,
                underlying_projection,
                symbol_rank: 0,
                producer: CursorProducer::NativeKline { events },
                next: None,
            });
        }

        for source in request.minute_kline_sources {
            validate_projected_minute_kline_source(&source, request.start_ns, request.end_ns)?;
            let reader = source
                .cache
                .open_reader(
                    &source.cache_symbol,
                    source.start_ns,
                    source.end_ns,
                    &source.snapshot,
                )
                .map_err(data_error_to_task)?;
            let aggregator = (source.duration_ns > CANONICAL_MINUTE_KLINE_NS)
                .then(|| MinuteKlineAggregator::new(source.duration_ns, source.session.clone()))
                .transpose()
                .map_err(data_error_to_task)?;
            let underlying_projection = if source.underlying_segments.is_empty() {
                (source.replay_symbol != source.cache_symbol)
                    .then_some(source.cache_symbol)
                    .map_or(UnderlyingProjection::None, UnderlyingProjection::Static)
            } else {
                UnderlyingProjection::Segments(source.underlying_segments)
            };
            cursors.push(HistoryCursor {
                symbol: source.replay_symbol,
                underlying_projection,
                symbol_rank: 0,
                producer: CursorProducer::MinuteKline(Box::new(MinuteKlineProducer {
                    reader,
                    duration_ns: source.duration_ns,
                    aggregator,
                    pending: VecDeque::new(),
                })),
                next: None,
            });
        }

        let symbol_ranks = cursors
            .iter()
            .map(|cursor| cursor.symbol.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .enumerate()
            .map(|(rank, symbol)| (symbol, rank))
            .collect::<BTreeMap<_, _>>();
        let mut heap = BinaryHeap::new();
        for (cursor_index, cursor) in cursors.iter_mut().enumerate() {
            cursor.symbol_rank = symbol_ranks
                .get(cursor.symbol.as_str())
                .copied()
                .unwrap_or(cursor_index);
            push_next_event(cursor, cursor_index, &mut heap)?;
        }

        Ok(Self {
            cursors,
            heap,
            refill_cursor: None,
        })
    }

    fn next_event_sync(&mut self) -> Result<Option<ReplayMarketEvent>> {
        if let Some(cursor_index) = self.refill_cursor.take() {
            let cursor = &mut self.cursors[cursor_index];
            push_next_event(cursor, cursor_index, &mut self.heap)?;
        }
        let Some(item) = self.heap.pop() else {
            return Ok(None);
        };
        let cursor = &mut self.cursors[item.cursor_index];
        let queued = cursor
            .next
            .take()
            .expect("heap item must reference a non-empty cursor");
        debug_assert_eq!(queued.event.event_time_ns(), item.event_time_ns);
        debug_assert_eq!(queued.source_rank, item.source_rank);
        debug_assert_eq!(queued.row_id, item.row_id);
        let underlying_symbol = cursor
            .underlying_projection
            .resolve(queued.source_datetime_ns)?;
        let event = match underlying_symbol {
            Some(underlying_symbol) => queued.event.with_underlying_symbol(underlying_symbol)?,
            None => queued.event,
        };
        self.refill_cursor = Some(item.cursor_index);
        Ok(Some(event))
    }
}

impl BacktestMarketStream for HistoryBacktestReplayStream {
    fn next_event_ready(&mut self) -> Option<Result<Option<ReplayMarketEvent>>> {
        Some(self.next_event_sync())
    }

    fn next_event<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<ReplayMarketEvent>>> + 'a>> {
        Box::pin(async move { self.next_event_sync() })
    }
}

impl UnderlyingProjection {
    fn resolve(&self, source_datetime_ns: i64) -> Result<Option<String>> {
        match self {
            Self::None => Ok(None),
            Self::Static(symbol) => Ok(Some(symbol.clone())),
            Self::Segments(segments) => segments
                .iter()
                .find(|segment| {
                    source_datetime_ns >= segment.start_ns && source_datetime_ns < segment.end_ns
                })
                .map(|segment| Some(segment.underlying_symbol.clone()))
                .ok_or(TaskError::InvalidState(
                    "minute kline event is outside its continuous-underlying mapping",
                )),
        }
    }
}

impl CursorProducer {
    fn next_event(&mut self, symbol: &str) -> Result<Option<QueuedEvent>> {
        match self {
            Self::Tick { reader } => {
                let Some(tick) = reader.next_tick().map_err(data_error_to_task)? else {
                    return Ok(None);
                };
                tick_event(symbol, tick).map(Some)
            }
            Self::ProjectedTick(producer) => producer
                .next_tick()?
                .map(|projected| {
                    tick_event(symbol, projected.tick).and_then(|queued| {
                        project_queued_event(queued, projected.underlying_symbol.as_deref())
                    })
                })
                .transpose(),
            Self::SyntheticKline {
                reader,
                synth,
                emit_range,
            } => loop {
                let Some(tick) = reader.next_tick().map_err(data_error_to_task)? else {
                    return Ok(None);
                };
                let Some(update) = synth.update(&tick).map_err(data_error_to_task)? else {
                    continue;
                };
                if update.event_time_ns < emit_range.0 || update.event_time_ns >= emit_range.1 {
                    continue;
                }
                return synthetic_kline_event(
                    synth.symbol(),
                    synth.duration_ns(),
                    update.event_time_ns,
                    update.updated,
                )
                .map(Some);
            },
            Self::ProjectedSyntheticKline(producer) => loop {
                let Some(projected) = producer.ticks.next_tick()? else {
                    return Ok(None);
                };
                let Some(update) = producer
                    .synth
                    .update(&projected.tick)
                    .map_err(data_error_to_task)?
                else {
                    continue;
                };
                if update.event_time_ns < producer.emit_range.0
                    || update.event_time_ns >= producer.emit_range.1
                {
                    continue;
                }
                let queued = synthetic_kline_event(
                    producer.synth.symbol(),
                    producer.synth.duration_ns(),
                    update.event_time_ns,
                    update.updated,
                )?;
                return project_queued_event(queued, projected.underlying_symbol.as_deref())
                    .map(Some);
            },
            Self::NativeKline { events } => Ok(events.pop_front()),
            Self::MinuteKline(producer) => loop {
                if let Some(event) = producer.pending.pop_front() {
                    return Ok(Some(event));
                }
                let Some(row) = producer.reader.next_kline().map_err(data_error_to_task)? else {
                    return Ok(None);
                };
                if producer.duration_ns == CANONICAL_MINUTE_KLINE_NS {
                    minute_kline_events(
                        symbol,
                        producer.duration_ns,
                        producer.reader.range_start_ns(),
                        producer.reader.range_end_ns(),
                        row,
                        &mut producer.pending,
                    )?;
                } else {
                    let update = producer
                        .aggregator
                        .as_mut()
                        .expect("aggregated minute source initializes an aggregator")
                        .update(&row)
                        .map_err(data_error_to_task)?;
                    let Some(update) = update else {
                        continue;
                    };
                    if let Some(opened) = update.opened
                        && opened.datetime >= producer.reader.range_start_ns()
                        && opened.datetime < producer.reader.range_end_ns()
                    {
                        producer
                            .pending
                            .push_back(aggregated_minute_kline_open_event(
                                symbol,
                                producer.duration_ns,
                                &opened,
                            )?);
                    }
                    if update.event_time_ns >= producer.reader.range_start_ns()
                        && update.event_time_ns < producer.reader.range_end_ns()
                    {
                        producer
                            .pending
                            .push_back(aggregated_minute_kline_update_event(
                                symbol,
                                producer.duration_ns,
                                update.event_time_ns,
                                update.updated,
                            )?);
                    }
                }
            },
        }
    }
}

impl ProjectedTickProducer {
    fn new(cache_dir: &std::path::Path, sources: Vec<HistoryBacktestTickSource>) -> Self {
        Self {
            cache: HistorySeriesCache::open_read_only(cache_dir),
            sources: sources.into(),
            current: None,
        }
    }

    fn next_tick(&mut self) -> Result<Option<ProjectedTick>> {
        loop {
            if let Some(current) = self.current.as_mut() {
                if let Some(tick) = current.reader.next_tick().map_err(data_error_to_task)? {
                    return Ok(Some(ProjectedTick {
                        tick,
                        underlying_symbol: current.underlying_symbol.clone(),
                    }));
                }
                self.current = None;
            }

            let Some(source) = self.sources.pop_front() else {
                return Ok(None);
            };
            let reader = self
                .cache
                .open_tick_data_series_reader(TickDataSeriesRequest::new(
                    &source.cache_symbol,
                    source.start_ns,
                    source.end_ns,
                ))
                .map_err(data_error_to_task)?;
            let underlying_symbol = (source.replay_symbol != source.cache_symbol)
                .then(|| Arc::<str>::from(source.cache_symbol));
            self.current = Some(ProjectedTickReader {
                reader,
                underlying_symbol,
            });
        }
    }
}

fn consecutive_projected_tick_source_chains(
    sources: Vec<HistoryBacktestTickSource>,
) -> Vec<Vec<HistoryBacktestTickSource>> {
    let mut chains: Vec<Vec<HistoryBacktestTickSource>> = Vec::new();
    for source in sources {
        let extends_current =
            chains
                .last()
                .and_then(|chain| chain.last())
                .is_some_and(|previous| {
                    previous.replay_symbol == source.replay_symbol
                        && previous.end_ns <= source.start_ns
                });
        if extends_current {
            chains
                .last_mut()
                .expect("the current projected tick chain exists")
                .push(source);
        } else {
            chains.push(vec![source]);
        }
    }
    chains
}

fn consecutive_projected_synthetic_source_chains(
    sources: Vec<HistoryBacktestSyntheticKlineSource>,
) -> Vec<Vec<HistoryBacktestSyntheticKlineSource>> {
    let mut chains: Vec<Vec<HistoryBacktestSyntheticKlineSource>> = Vec::new();
    for source in sources {
        let extends_current =
            chains
                .last()
                .and_then(|chain| chain.last())
                .is_some_and(|previous| {
                    previous.duration_ns == source.duration_ns
                        && previous.tick_source.replay_symbol == source.tick_source.replay_symbol
                        && previous.tick_source.end_ns <= source.tick_source.start_ns
                });
        if extends_current {
            chains
                .last_mut()
                .expect("the current projected synthetic chain exists")
                .push(source);
        } else {
            chains.push(vec![source]);
        }
    }
    chains
}

fn project_queued_event(
    mut queued: QueuedEvent,
    underlying_symbol: Option<&str>,
) -> Result<QueuedEvent> {
    if let Some(underlying_symbol) = underlying_symbol {
        queued.event = queued.event.with_underlying_symbol(underlying_symbol)?;
    }
    Ok(queued)
}

fn push_next_event(
    cursor: &mut HistoryCursor,
    cursor_index: usize,
    heap: &mut BinaryHeap<HeapItem>,
) -> Result<()> {
    if cursor.next.is_none() {
        cursor.next = cursor.producer.next_event(&cursor.symbol)?;
    }
    if let Some(queued) = cursor.next.as_ref() {
        heap.push(HeapItem {
            cursor_index,
            event_time_ns: queued.event.event_time_ns(),
            source_rank: queued.source_rank,
            symbol_rank: cursor.symbol_rank,
            row_id: queued.row_id,
        });
    }
    Ok(())
}

fn validate_projected_tick_source(
    source: &HistoryBacktestTickSource,
    request_start_ns: i64,
    request_end_ns: i64,
) -> Result<()> {
    validate_symbol(&source.replay_symbol)?;
    validate_symbol(&source.cache_symbol)?;
    if source.start_ns >= source.end_ns {
        return Err(TaskError::InvalidState(
            "projected tick source start_ns must be less than end_ns",
        ));
    }
    if source.start_ns < request_start_ns || source.end_ns > request_end_ns {
        return Err(TaskError::InvalidState(
            "projected tick source must be inside replay request range",
        ));
    }
    Ok(())
}

fn validate_projected_synthetic_tick_source(
    source: &HistoryBacktestTickSource,
    request_start_ns: i64,
    request_end_ns: i64,
) -> Result<()> {
    validate_symbol(&source.replay_symbol)?;
    validate_symbol(&source.cache_symbol)?;
    if source.start_ns >= source.end_ns {
        return Err(TaskError::InvalidState(
            "projected synthetic tick source start_ns must be less than end_ns",
        ));
    }
    if source.end_ns <= request_start_ns
        || source.start_ns >= request_end_ns
        || source.end_ns > request_end_ns
    {
        return Err(TaskError::InvalidState(
            "projected synthetic tick source must overlap the replay request range",
        ));
    }
    Ok(())
}

fn validate_projected_minute_kline_source(
    source: &HistoryBacktestMinuteKlineSource,
    request_start_ns: i64,
    request_end_ns: i64,
) -> Result<()> {
    validate_symbol(&source.replay_symbol)?;
    validate_symbol(&source.cache_symbol)?;
    if source.start_ns >= source.end_ns {
        return Err(TaskError::InvalidState(
            "projected minute kline source start_ns must be less than end_ns",
        ));
    }
    if source.start_ns < request_start_ns || source.end_ns > request_end_ns {
        return Err(TaskError::InvalidState(
            "projected minute kline source must be inside replay request range",
        ));
    }
    if source.duration_ns != CANONICAL_MINUTE_KLINE_NS
        && (source.duration_ns <= CANONICAL_MINUTE_KLINE_NS
            || source.duration_ns % CANONICAL_MINUTE_KLINE_NS != 0)
    {
        return Err(TaskError::InvalidState(
            "minute kline source duration must be 60 seconds or an integer multiple above it",
        ));
    }
    if source.snapshot.session_hash != source.session.snapshot_hash() {
        return Err(TaskError::InvalidState(
            "minute kline source session hash does not match its cache snapshot",
        ));
    }
    validate_minute_underlying_segments(source)?;
    Ok(())
}

fn validate_minute_underlying_segments(source: &HistoryBacktestMinuteKlineSource) -> Result<()> {
    if source.underlying_segments.is_empty() {
        if source.replay_symbol.starts_with("KQ.m@") {
            return Err(TaskError::InvalidState(
                "continuous minute kline source requires complete underlying segments",
            ));
        }
        return Ok(());
    }

    let mut expected_start_ns = source.start_ns;
    for segment in &source.underlying_segments {
        validate_symbol(&segment.underlying_symbol)?;
        if segment.start_ns != expected_start_ns || segment.end_ns <= segment.start_ns {
            return Err(TaskError::InvalidState(
                "minute kline underlying segments must be contiguous and non-empty",
            ));
        }
        if segment.end_ns > source.end_ns {
            return Err(TaskError::InvalidState(
                "minute kline underlying segment exceeds its source range",
            ));
        }
        expected_start_ns = segment.end_ns;
    }
    if expected_start_ns != source.end_ns {
        return Err(TaskError::InvalidState(
            "minute kline underlying segments must cover the complete source range",
        ));
    }
    Ok(())
}

fn tick_event(symbol: &str, tick: Tick) -> Result<QueuedEvent> {
    let row_id = tick.id;
    let source_datetime_ns = tick.datetime;
    let event = ReplayMarketEvent::tick(
        "history-cache",
        symbol,
        source_datetime_ns,
        Some(source_datetime_ns),
        tick,
    )?;
    Ok(QueuedEvent {
        event,
        source_rank: 0,
        row_id,
        source_datetime_ns,
    })
}

fn synthetic_kline_event(
    symbol: &str,
    duration_ns: i64,
    event_time_ns: i64,
    row: Kline,
) -> Result<QueuedEvent> {
    let row_id = row.id;
    let source_datetime_ns = row.datetime;
    let event = ReplayMarketEvent::kline(
        "history-cache-synth-kline",
        symbol,
        event_time_ns,
        Some(event_time_ns),
        duration_ns,
        row,
    )?;
    Ok(QueuedEvent {
        event,
        source_rank: 1,
        row_id,
        source_datetime_ns,
    })
}

fn minute_kline_events(
    symbol: &str,
    duration_ns: i64,
    start_ns: i64,
    end_ns: i64,
    row: Kline,
    pending: &mut VecDeque<QueuedEvent>,
) -> Result<()> {
    if row.datetime >= start_ns && row.datetime < end_ns {
        pending.push_back(minute_kline_open_event(symbol, duration_ns, &row)?);
    }
    let close_time =
        row.datetime
            .checked_add(CANONICAL_MINUTE_KLINE_NS)
            .ok_or(TaskError::InvalidState(
                "canonical minute kline close timestamp overflow",
            ))?;
    if close_time >= start_ns && close_time < end_ns {
        pending.push_back(minute_kline_close_event(
            symbol,
            duration_ns,
            close_time,
            row,
        )?);
    }
    Ok(())
}

fn minute_kline_open_event(symbol: &str, duration_ns: i64, row: &Kline) -> Result<QueuedEvent> {
    kline_open_event(
        "history-cache-minute-kline-open",
        symbol,
        duration_ns,
        row,
        2,
    )
}

fn minute_kline_close_event(
    symbol: &str,
    duration_ns: i64,
    close_time: i64,
    row: Kline,
) -> Result<QueuedEvent> {
    kline_close_event(
        "history-cache-minute-kline-close",
        symbol,
        duration_ns,
        close_time,
        row,
        3,
    )
}

fn aggregated_minute_kline_open_event(
    symbol: &str,
    duration_ns: i64,
    row: &Kline,
) -> Result<QueuedEvent> {
    kline_open_event(
        "history-cache-minute-kline-aggregate-open",
        symbol,
        duration_ns,
        row,
        2,
    )
}

fn aggregated_minute_kline_update_event(
    symbol: &str,
    duration_ns: i64,
    event_time_ns: i64,
    row: Kline,
) -> Result<QueuedEvent> {
    kline_close_event(
        "history-cache-minute-kline-aggregate-update",
        symbol,
        duration_ns,
        event_time_ns,
        row,
        3,
    )
}

fn native_kline_events(
    symbol: &str,
    duration_ns: i64,
    start_ns: i64,
    end_ns: i64,
    rows: Vec<Kline>,
) -> Result<VecDeque<QueuedEvent>> {
    let mut events = Vec::new();
    for row in rows {
        let open_time = row.datetime;
        if open_time >= start_ns && open_time < end_ns {
            events.push(native_kline_open_event(symbol, duration_ns, &row)?);
        }
        if let Some(close_time) = row.datetime.checked_add(duration_ns)
            && close_time >= start_ns
            && close_time < end_ns
        {
            events.push(native_kline_close_event(
                symbol,
                duration_ns,
                close_time,
                row,
            )?);
        }
    }
    events.sort_by_key(|event| {
        (
            event.event.event_time_ns(),
            event.source_rank,
            event.row_id,
            event.event.received_at_ns(),
        )
    });
    Ok(events.into())
}

fn native_kline_open_event(symbol: &str, duration_ns: i64, row: &Kline) -> Result<QueuedEvent> {
    kline_open_event(
        "history-cache-native-kline-open",
        symbol,
        duration_ns,
        row,
        2,
    )
}

fn kline_open_event(
    source: &str,
    symbol: &str,
    duration_ns: i64,
    row: &Kline,
    source_rank: u8,
) -> Result<QueuedEvent> {
    let row_id = row.id;
    let open_row = Kline {
        id: row.id,
        datetime: row.datetime,
        open: row.open,
        high: row.open,
        low: row.open,
        close: row.open,
        volume: 0,
        open_oi: row.open_oi,
        close_oi: row.open_oi,
        epoch: row.epoch,
    };
    let event = ReplayMarketEvent::kline(
        source,
        symbol,
        row.datetime,
        Some(row.datetime),
        duration_ns,
        open_row,
    )?;
    Ok(QueuedEvent {
        event,
        source_rank,
        row_id,
        source_datetime_ns: row.datetime,
    })
}

fn native_kline_close_event(
    symbol: &str,
    duration_ns: i64,
    close_time: i64,
    row: Kline,
) -> Result<QueuedEvent> {
    kline_close_event(
        "history-cache-native-kline-close",
        symbol,
        duration_ns,
        close_time,
        row,
        3,
    )
}

fn kline_close_event(
    source: &str,
    symbol: &str,
    duration_ns: i64,
    close_time: i64,
    row: Kline,
    source_rank: u8,
) -> Result<QueuedEvent> {
    let row_id = row.id;
    let source_datetime_ns = row.datetime;
    let event = ReplayMarketEvent::kline(
        source,
        symbol,
        close_time,
        Some(close_time),
        duration_ns,
        row,
    )?;
    Ok(QueuedEvent {
        event,
        source_rank,
        row_id,
        source_datetime_ns,
    })
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .event_time_ns
            .cmp(&self.event_time_ns)
            .then_with(|| other.source_rank.cmp(&self.source_rank))
            .then_with(|| other.symbol_rank.cmp(&self.symbol_rank))
            .then_with(|| other.row_id.cmp(&self.row_id))
            .then_with(|| other.cursor_index.cmp(&self.cursor_index))
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.cursor_index == other.cursor_index
            && self.event_time_ns == other.event_time_ns
            && self.source_rank == other.source_rank
            && self.symbol_rank == other.symbol_rank
            && self.row_id == other.row_id
    }
}

impl Eq for HeapItem {}

fn validate_request_range(start_ns: i64, end_ns: i64) -> Result<()> {
    if end_ns <= start_ns {
        return Err(TaskError::InvalidState(
            "history backtest replay end_ns must be greater than start_ns",
        ));
    }
    Ok(())
}

fn validate_kline_request(request: &HistoryBacktestKlineRequest) -> Result<()> {
    validate_symbol(&request.symbol)?;
    if request.duration_ns <= 0 {
        return Err(TaskError::InvalidState(
            "history backtest kline duration must be positive",
        ));
    }
    Ok(())
}

fn validate_symbol(symbol: &str) -> Result<()> {
    if symbol.is_empty() {
        return Err(TaskError::InvalidState(
            "history backtest replay symbol must not be empty",
        ));
    }
    Ok(())
}

fn data_error_to_task(error: tqsdk_data::DataError) -> TaskError {
    TaskError::External(error.to_string())
}

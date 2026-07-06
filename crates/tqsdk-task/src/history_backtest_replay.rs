use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use tqsdk_core::{Kline, Tick};
use tqsdk_data::{
    HistorySeriesCache, KlineDataSeriesRequest, TickDataSeriesReader, TickDataSeriesRequest,
};

use crate::kline_synth::KlineSynthesizer;
use crate::{BacktestMarketStream, ReplayMarketEvent, Result, TaskError};

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

pub struct HistoryBacktestReplayStream {
    cursors: Vec<HistoryCursor>,
    heap: BinaryHeap<HeapItem>,
}

struct HistoryCursor {
    symbol: String,
    symbol_rank: usize,
    producer: CursorProducer,
    next: Option<QueuedEvent>,
}

enum CursorProducer {
    Tick {
        reader: TickDataSeriesReader,
    },
    SyntheticKline {
        reader: TickDataSeriesReader,
        synth: KlineSynthesizer,
    },
    NativeKline {
        events: VecDeque<QueuedEvent>,
    },
}

struct QueuedEvent {
    event: ReplayMarketEvent,
    source_rank: u8,
    row_id: i64,
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
            let synth = KlineSynthesizer::new(spec.symbol.clone(), spec.duration_ns)?;
            cursors.push(HistoryCursor {
                symbol: spec.symbol,
                symbol_rank: 0,
                producer: CursorProducer::SyntheticKline { reader, synth },
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

        Ok(Self { cursors, heap })
    }

    fn next_event_sync(&mut self) -> Result<Option<ReplayMarketEvent>> {
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
        push_next_event(cursor, item.cursor_index, &mut self.heap)?;
        Ok(Some(queued.event))
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

impl CursorProducer {
    fn next_event(&mut self, symbol: &str) -> Result<Option<QueuedEvent>> {
        match self {
            Self::Tick { reader } => {
                let Some(tick) = reader.next_tick().map_err(data_error_to_task)? else {
                    return Ok(None);
                };
                tick_event(symbol, tick).map(Some)
            }
            Self::SyntheticKline { reader, synth } => loop {
                let Some(tick) = reader.next_tick().map_err(data_error_to_task)? else {
                    return Ok(None);
                };
                let Some(row) = synth.update(&tick) else {
                    continue;
                };
                return synthetic_kline_event(
                    synth.symbol(),
                    synth.duration_ns(),
                    tick.datetime,
                    row,
                )
                .map(Some);
            },
            Self::NativeKline { events } => Ok(events.pop_front()),
        }
    }
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

fn tick_event(symbol: &str, tick: Tick) -> Result<QueuedEvent> {
    let row_id = tick.id;
    let event = ReplayMarketEvent::tick(
        "history-cache",
        symbol,
        tick.datetime,
        Some(tick.datetime),
        tick,
    )?;
    Ok(QueuedEvent {
        event,
        source_rank: 0,
        row_id,
    })
}

fn synthetic_kline_event(
    symbol: &str,
    duration_ns: i64,
    event_time_ns: i64,
    row: Kline,
) -> Result<QueuedEvent> {
    let row_id = row.id;
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
    })
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
        "history-cache-native-kline-open",
        symbol,
        row.datetime,
        Some(row.datetime),
        duration_ns,
        open_row,
    )?;
    Ok(QueuedEvent {
        event,
        source_rank: 2,
        row_id,
    })
}

fn native_kline_close_event(
    symbol: &str,
    duration_ns: i64,
    close_time: i64,
    row: Kline,
) -> Result<QueuedEvent> {
    let row_id = row.id;
    let event = ReplayMarketEvent::kline(
        "history-cache-native-kline-close",
        symbol,
        close_time,
        Some(close_time),
        duration_ns,
        row,
    )?;
    Ok(QueuedEvent {
        event,
        source_rank: 3,
        row_id,
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

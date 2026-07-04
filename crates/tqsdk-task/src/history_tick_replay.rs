use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::future::Future;
use std::pin::Pin;

use tqsdk_data::{HistorySeriesCache, TickDataSeriesReader, TickDataSeriesRequest};

use crate::{BacktestMarketStream, ReplayMarketEvent, Result, TaskError};

pub struct HistoryTickReplayStream {
    cursors: Vec<TickSeriesCursor>,
    heap: BinaryHeap<HeapItem>,
}

struct TickSeriesCursor {
    symbol: String,
    symbol_rank: usize,
    reader: TickDataSeriesReader,
    next_tick: Option<tqsdk_core::Tick>,
}

#[derive(Debug, Clone, Copy)]
struct HeapItem {
    cursor_index: usize,
    datetime: i64,
    tick_id: i64,
    symbol_rank: usize,
}

impl HistoryTickReplayStream {
    pub fn new(
        cache: HistorySeriesCache,
        requests: impl IntoIterator<Item = TickDataSeriesRequest>,
    ) -> tqsdk_data::Result<Self> {
        let mut cursors = Vec::new();
        let mut heap = BinaryHeap::new();
        for request in requests {
            let symbol = request.symbol().to_string();
            let reader = cache.open_tick_data_series_reader(request)?;
            let cursor = TickSeriesCursor {
                symbol,
                symbol_rank: 0,
                reader,
                next_tick: None,
            };
            cursors.push(cursor);
        }

        let symbol_ranks = cursors
            .iter()
            .map(|cursor| cursor.symbol.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .enumerate()
            .map(|(rank, symbol)| (symbol, rank))
            .collect::<BTreeMap<_, _>>();
        for (cursor_index, cursor) in cursors.iter_mut().enumerate() {
            cursor.symbol_rank = symbol_ranks
                .get(cursor.symbol.as_str())
                .copied()
                .unwrap_or(cursor_index);
            push_next_tick(cursor, cursor_index, &mut heap)?;
        }
        Ok(Self { cursors, heap })
    }
}

impl BacktestMarketStream for HistoryTickReplayStream {
    fn next_event_ready(&mut self) -> Option<Result<Option<ReplayMarketEvent>>> {
        Some(self.next_event_sync())
    }

    fn next_event<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<ReplayMarketEvent>>> + 'a>> {
        Box::pin(async move { self.next_event_sync() })
    }
}

impl HistoryTickReplayStream {
    fn next_event_sync(&mut self) -> Result<Option<ReplayMarketEvent>> {
        let Some(item) = self.heap.pop() else {
            return Ok(None);
        };
        let cursor = &mut self.cursors[item.cursor_index];
        let symbol = cursor.symbol.clone();
        let tick = cursor
            .next_tick
            .take()
            .expect("heap item must reference a non-empty tick cursor");
        debug_assert_eq!(tick.datetime, item.datetime);
        debug_assert_eq!(tick.id, item.tick_id);
        push_next_tick(cursor, item.cursor_index, &mut self.heap).map_err(data_error_to_task)?;
        ReplayMarketEvent::tick(
            "history-cache",
            symbol,
            tick.datetime,
            Some(tick.datetime),
            tick,
        )
        .map(Some)
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .datetime
            .cmp(&self.datetime)
            .then_with(|| other.tick_id.cmp(&self.tick_id))
            .then_with(|| other.symbol_rank.cmp(&self.symbol_rank))
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
            && self.datetime == other.datetime
            && self.tick_id == other.tick_id
            && self.symbol_rank == other.symbol_rank
    }
}

impl Eq for HeapItem {}

fn push_next_tick(
    cursor: &mut TickSeriesCursor,
    cursor_index: usize,
    heap: &mut BinaryHeap<HeapItem>,
) -> tqsdk_data::Result<()> {
    if cursor.next_tick.is_none() {
        cursor.next_tick = cursor.reader.next_tick()?;
    }
    if let Some(tick) = cursor.next_tick.as_ref() {
        heap.push(HeapItem {
            cursor_index,
            datetime: tick.datetime,
            tick_id: tick.id,
            symbol_rank: cursor.symbol_rank,
        });
    }
    Ok(())
}

fn data_error_to_task(error: tqsdk_data::DataError) -> TaskError {
    TaskError::External(error.to_string())
}

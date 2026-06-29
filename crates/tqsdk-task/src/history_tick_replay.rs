use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::future::Future;
use std::pin::Pin;

use tqsdk_data::{HistorySeriesCache, TickDataSeriesRequest};

use crate::{BacktestMarketStream, ReplayMarketEvent, Result};

pub struct HistoryTickReplayStream {
    cursors: Vec<TickSeriesCursor>,
    heap: BinaryHeap<HeapItem>,
}

#[derive(Debug, Clone)]
struct TickSeriesCursor {
    symbol: String,
    rows: Vec<tqsdk_core::Tick>,
    next_index: usize,
}

#[derive(Debug, Clone)]
struct HeapItem {
    cursor_index: usize,
    symbol: String,
    tick: tqsdk_core::Tick,
}

impl HistoryTickReplayStream {
    pub fn new(
        cache: HistorySeriesCache,
        requests: impl IntoIterator<Item = TickDataSeriesRequest>,
    ) -> tqsdk_data::Result<Self> {
        let mut cursors = Vec::new();
        let mut heap = BinaryHeap::new();
        for request in requests {
            let series = cache.read_tick_data_series(request)?;
            let cursor_index = cursors.len();
            let mut cursor = TickSeriesCursor {
                symbol: series.symbol().to_string(),
                rows: series.into_rows(),
                next_index: 0,
            };
            push_next_tick(&mut cursor, cursor_index, &mut heap);
            cursors.push(cursor);
        }
        Ok(Self { cursors, heap })
    }
}

impl BacktestMarketStream for HistoryTickReplayStream {
    fn next_event<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<ReplayMarketEvent>>> + 'a>> {
        Box::pin(async move {
            let Some(item) = self.heap.pop() else {
                return Ok(None);
            };
            push_next_tick(
                &mut self.cursors[item.cursor_index],
                item.cursor_index,
                &mut self.heap,
            );
            ReplayMarketEvent::tick(
                "history-cache",
                item.symbol,
                item.tick.datetime,
                Some(item.tick.datetime),
                item.tick,
            )
            .map(Some)
        })
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .tick
            .datetime
            .cmp(&self.tick.datetime)
            .then_with(|| other.tick.id.cmp(&self.tick.id))
            .then_with(|| other.symbol.cmp(&self.symbol))
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
            && self.symbol == other.symbol
            && self.tick.datetime == other.tick.datetime
            && self.tick.id == other.tick.id
    }
}

impl Eq for HeapItem {}

fn push_next_tick(
    cursor: &mut TickSeriesCursor,
    cursor_index: usize,
    heap: &mut BinaryHeap<HeapItem>,
) {
    if let Some(tick) = cursor.rows.get(cursor.next_index).cloned() {
        cursor.next_index += 1;
        heap.push(HeapItem {
            cursor_index,
            symbol: cursor.symbol.clone(),
            tick,
        });
    }
}

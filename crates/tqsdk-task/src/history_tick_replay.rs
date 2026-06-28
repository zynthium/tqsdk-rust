use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::future::Future;
use std::pin::Pin;

use tqsdk_data::{
    HistorySeriesCache, HistorySeriesKind, HistorySeriesReadRequest, HistorySeriesReader,
    HistorySeriesRow, TickDataSeriesRequest,
};

use crate::{BacktestMarketStream, ReplayMarketEvent, Result, TaskError};

pub struct HistoryTickReplayStream {
    readers: Vec<Box<dyn HistorySeriesReader>>,
    heap: BinaryHeap<HeapItem>,
}

#[derive(Debug, Clone)]
struct HeapItem {
    reader_index: usize,
    symbol: String,
    tick: tqsdk_core::Tick,
}

impl HistoryTickReplayStream {
    pub fn new(
        cache: HistorySeriesCache,
        requests: impl IntoIterator<Item = TickDataSeriesRequest>,
    ) -> tqsdk_data::Result<Self> {
        let mut readers = Vec::new();
        let mut heap = BinaryHeap::new();
        for request in requests {
            let symbol = request.symbol().to_string();
            let mut reader = cache.open_reader(HistorySeriesReadRequest {
                symbol: symbol.clone(),
                kind: HistorySeriesKind::Tick,
                range_start_ns: request.start_datetime_ns(),
                range_end_ns: request.end_datetime_ns(),
            })?;
            let reader_index = readers.len();
            push_next_tick(&mut *reader, reader_index, &symbol, &mut heap)?;
            readers.push(reader);
        }
        Ok(Self { readers, heap })
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
                &mut *self.readers[item.reader_index],
                item.reader_index,
                &item.symbol,
                &mut self.heap,
            )
            .map_err(|error| TaskError::External(error.to_string()))?;
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
            .then_with(|| other.reader_index.cmp(&self.reader_index))
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.reader_index == other.reader_index
            && self.symbol == other.symbol
            && self.tick.datetime == other.tick.datetime
            && self.tick.id == other.tick.id
    }
}

impl Eq for HeapItem {}

fn push_next_tick(
    reader: &mut dyn HistorySeriesReader,
    reader_index: usize,
    symbol: &str,
    heap: &mut BinaryHeap<HeapItem>,
) -> tqsdk_data::Result<()> {
    while let Some(row) = reader.next_row()? {
        if let HistorySeriesRow::Tick(tick) = row {
            heap.push(HeapItem {
                reader_index,
                symbol: symbol.to_string(),
                tick,
            });
            break;
        }
    }
    Ok(())
}

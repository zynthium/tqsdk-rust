#![allow(dead_code)]

use super::super::store::HISTORY_SERIES_CACHE_FORMAT_ID;

pub(in crate::history_series_cache) const TQBN_FORMAT_ID: &str = HISTORY_SERIES_CACHE_FORMAT_ID;
pub(in crate::history_series_cache) const TQBN_SCHEMA_VERSION: u32 = 3;
pub(super) const TQBN_LEGACY_SCHEMA_VERSION: u32 = 2;
pub(super) const TQBN_MAGIC: [u8; 4] = *b"TQBN";
pub(super) const TQBN_BLOCK_MAGIC: [u8; 4] = *b"TQBB";
pub(super) const TQBN_VERSION: u8 = 1;
pub(super) const FIXED_PRICE_SCALE: i64 = 1_000_000_000;
pub(super) const FIXED_AMOUNT_SCALE: i64 = 1_000_000;
pub(super) const UNDEF_PRICE: i64 = i64::MAX;
pub(super) const UNDEF_AMOUNT: i64 = i64::MAX;
pub(super) const NONE_EPOCH: i64 = i64::MIN;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::history_series_cache) enum TqbnRType {
    Kline = 1,
    Tick1 = 2,
    Tick5 = 3,
    Coverage = 16,
    Index = 17,
    MetadataUpdate = 18,
    ProvisionalCoverage = 19,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct TqbnRecordHeader {
    pub length_words: u8,
    pub rtype: u8,
    pub flags: u16,
    pub instrument_id: u32,
    pub ts_event: u64,
}

impl TqbnRecordHeader {
    pub(super) const LENGTH_MULTIPLIER: usize = 4;

    pub(super) fn new<R>(rtype: TqbnRType, instrument_id: u32, ts_event: u64) -> Self {
        let size = std::mem::size_of::<R>();
        assert_eq!(
            size % Self::LENGTH_MULTIPLIER,
            0,
            "TQBN record layout size must be a multiple of 4 bytes"
        );
        let length_words = u8::try_from(size / Self::LENGTH_MULTIPLIER)
            .expect("TQBN record layout length must fit in u8 words");

        Self {
            length_words,
            rtype: rtype as u8,
            flags: 0,
            instrument_id,
            ts_event,
        }
    }

    pub(super) fn record_size(self) -> usize {
        self.length_words as usize * Self::LENGTH_MULTIPLIER
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct TqbnKlineRecordV1 {
    pub hd: TqbnRecordHeader,
    pub row_id: i64,
    pub open: i64,
    pub high: i64,
    pub low: i64,
    pub close: i64,
    pub volume: i64,
    pub open_oi: i64,
    pub close_oi: i64,
    pub epoch: i64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct TqbnTick1RecordV1 {
    pub hd: TqbnRecordHeader,
    pub row_id: i64,
    pub last_price: i64,
    pub average: i64,
    pub highest: i64,
    pub lowest: i64,
    pub ask_price1: i64,
    pub ask_volume1: i64,
    pub bid_price1: i64,
    pub bid_volume1: i64,
    pub volume: i64,
    pub amount: i64,
    pub open_interest: i64,
    pub epoch: i64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct TqbnTick5RecordV1 {
    pub hd: TqbnRecordHeader,
    pub row_id: i64,
    pub last_price: i64,
    pub average: i64,
    pub highest: i64,
    pub lowest: i64,
    pub ask_price1: i64,
    pub ask_volume1: i64,
    pub bid_price1: i64,
    pub bid_volume1: i64,
    pub ask_price2: i64,
    pub ask_volume2: i64,
    pub bid_price2: i64,
    pub bid_volume2: i64,
    pub ask_price3: i64,
    pub ask_volume3: i64,
    pub bid_price3: i64,
    pub bid_volume3: i64,
    pub ask_price4: i64,
    pub ask_volume4: i64,
    pub bid_price4: i64,
    pub bid_volume4: i64,
    pub ask_price5: i64,
    pub ask_volume5: i64,
    pub bid_price5: i64,
    pub bid_volume5: i64,
    pub volume: i64,
    pub amount: i64,
    pub open_interest: i64,
    pub epoch: i64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct TqbnCoverageRecordV1 {
    pub hd: TqbnRecordHeader,
    pub range_start_ns: i64,
    pub range_end_ns: i64,
    pub rows: u64,
    pub id_start: i64,
    pub id_end: i64,
    pub has_id_range: u8,
    pub reserved: [u8; 7],
}

/// Durable high-water mark for an open trading-day snapshot.
///
/// This is deliberately a distinct record type from [`TqbnCoverageRecordV1`]:
/// readers that do not understand it skip the record by `length_words`, while
/// readers that do understand it must never promote it to final coverage.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct TqbnProvisionalCoverageRecordV1 {
    pub hd: TqbnRecordHeader,
    pub range_start_ns: i64,
    pub complete_through_ns: i64,
    pub as_of_ns: i64,
    pub rows: u64,
    pub id_start: i64,
    pub id_end: i64,
    pub has_id_range: u8,
    pub reserved: [u8; 7],
}

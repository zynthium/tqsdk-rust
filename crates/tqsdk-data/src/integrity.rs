#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{BTreeSet, HashMap, hash_map::Entry};

use tqsdk_core::{Kline, Tick};

use crate::HistorySeriesCacheReport;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryDataKind {
    Kline { duration_ns: i64 },
    Tick,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HistoryPermissionStatus {
    #[default]
    Unknown,
    Checked,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum HistoryCacheStatus {
    #[default]
    NotUsed,
    Hit {
        hit_rows: usize,
    },
    MissDownloaded {
        hit_rows: usize,
        downloaded_rows: usize,
        downloaded_ranges: Vec<(i64, i64)>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryDuplicateField {
    Id,
    Datetime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DuplicatedHistoryRow {
    pub field: HistoryDuplicateField,
    pub first_index: usize,
    pub duplicate_index: usize,
    pub value: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NonMonotonicHistoryTimestamp {
    pub previous_index: usize,
    pub previous_datetime_ns: i64,
    pub index: usize,
    pub datetime_ns: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutOfRangeHistoryRow {
    pub index: usize,
    pub id: i64,
    pub datetime_ns: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryIntegrityReport {
    pub symbol: String,
    pub data_kind: HistoryDataKind,
    pub requested_range: (i64, i64),
    pub returned_range: Option<(i64, i64)>,
    pub row_count: usize,
    pub missing_intervals: Vec<(i64, i64)>,
    pub duplicated_rows: Vec<DuplicatedHistoryRow>,
    pub non_monotonic_timestamps: Vec<NonMonotonicHistoryTimestamp>,
    pub out_of_range_rows: Vec<OutOfRangeHistoryRow>,
    pub mutable_tail_refreshed: bool,
    pub permission_status: HistoryPermissionStatus,
    pub cache_status: HistoryCacheStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryIntegrityCheck {
    symbol: String,
    data_kind: HistoryDataKind,
    requested_range: (i64, i64),
    permission_status: HistoryPermissionStatus,
    cache_usage: Option<HistoryCacheUsage>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HistoryCacheUsage {
    hit_rows: usize,
    downloaded_ranges: Vec<(i64, i64)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HistoryRowSample {
    index: usize,
    id: i64,
    datetime_ns: i64,
}

impl HistoryIntegrityCheck {
    #[must_use]
    pub fn kline(
        symbol: impl Into<String>,
        duration_ns: i64,
        start_datetime_ns: i64,
        end_datetime_ns: i64,
    ) -> Self {
        Self::new(
            symbol,
            HistoryDataKind::Kline { duration_ns },
            start_datetime_ns,
            end_datetime_ns,
        )
    }

    #[must_use]
    pub fn tick(symbol: impl Into<String>, start_datetime_ns: i64, end_datetime_ns: i64) -> Self {
        Self::new(
            symbol,
            HistoryDataKind::Tick,
            start_datetime_ns,
            end_datetime_ns,
        )
    }

    fn new(
        symbol: impl Into<String>,
        data_kind: HistoryDataKind,
        start_datetime_ns: i64,
        end_datetime_ns: i64,
    ) -> Self {
        Self {
            symbol: symbol.into(),
            data_kind,
            requested_range: (start_datetime_ns, end_datetime_ns),
            permission_status: HistoryPermissionStatus::Unknown,
            cache_usage: None,
        }
    }

    #[must_use]
    pub fn with_permission_status(mut self, permission_status: HistoryPermissionStatus) -> Self {
        self.permission_status = permission_status;
        self
    }

    #[must_use]
    pub fn with_cache_usage(mut self, hit_rows: usize, downloaded_ranges: Vec<(i64, i64)>) -> Self {
        self.cache_usage = Some(HistoryCacheUsage {
            hit_rows,
            downloaded_ranges,
        });
        self
    }

    #[must_use]
    pub fn with_cache_report(self, cache_report: &HistorySeriesCacheReport) -> Self {
        self.with_cache_usage(
            cache_report.hit_rows,
            cache_report.downloaded_ranges.clone(),
        )
    }

    #[must_use]
    pub fn inspect_klines(&self, rows: &[Kline]) -> HistoryIntegrityReport {
        let samples = rows
            .iter()
            .enumerate()
            .map(|(index, row)| HistoryRowSample {
                index,
                id: row.id,
                datetime_ns: row.datetime,
            })
            .collect::<Vec<_>>();
        self.inspect_samples(&samples)
    }

    #[must_use]
    pub fn inspect_ticks(&self, rows: &[Tick]) -> HistoryIntegrityReport {
        let samples = rows
            .iter()
            .enumerate()
            .map(|(index, row)| HistoryRowSample {
                index,
                id: row.id,
                datetime_ns: row.datetime,
            })
            .collect::<Vec<_>>();
        self.inspect_samples(&samples)
    }

    fn inspect_samples(&self, samples: &[HistoryRowSample]) -> HistoryIntegrityReport {
        let returned_range = returned_range(samples);
        let row_count = samples.len();
        let cache_status = self.cache_status(row_count);

        HistoryIntegrityReport {
            symbol: self.symbol.clone(),
            data_kind: self.data_kind,
            requested_range: self.requested_range,
            returned_range,
            row_count,
            missing_intervals: self.missing_intervals(samples),
            duplicated_rows: duplicated_rows(samples),
            non_monotonic_timestamps: non_monotonic_timestamps(samples),
            out_of_range_rows: out_of_range_rows(samples, self.requested_range),
            mutable_tail_refreshed: self.mutable_tail_refreshed(),
            permission_status: self.permission_status,
            cache_status,
        }
    }

    fn missing_intervals(&self, samples: &[HistoryRowSample]) -> Vec<(i64, i64)> {
        let HistoryDataKind::Kline { duration_ns } = self.data_kind else {
            return Vec::new();
        };
        missing_cadence_intervals(samples, self.requested_range, duration_ns)
    }

    fn cache_status(&self, row_count: usize) -> HistoryCacheStatus {
        let Some(cache_usage) = &self.cache_usage else {
            return HistoryCacheStatus::NotUsed;
        };

        if cache_usage.downloaded_ranges.is_empty() {
            return HistoryCacheStatus::Hit {
                hit_rows: cache_usage.hit_rows,
            };
        }

        HistoryCacheStatus::MissDownloaded {
            hit_rows: cache_usage.hit_rows,
            downloaded_rows: row_count.saturating_sub(cache_usage.hit_rows),
            downloaded_ranges: cache_usage.downloaded_ranges.clone(),
        }
    }

    fn mutable_tail_refreshed(&self) -> bool {
        let Some(cache_usage) = &self.cache_usage else {
            return false;
        };
        let requested_end = self.requested_range.1;
        cache_usage
            .downloaded_ranges
            .iter()
            .any(|range| range.1 >= requested_end)
    }
}

fn returned_range(samples: &[HistoryRowSample]) -> Option<(i64, i64)> {
    let mut datetimes = samples.iter().map(|sample| sample.datetime_ns);
    let first = datetimes.next()?;
    let (min, max) = datetimes.fold((first, first), |(min, max), datetime| {
        (min.min(datetime), max.max(datetime))
    });
    Some((min, max))
}

fn duplicated_rows(samples: &[HistoryRowSample]) -> Vec<DuplicatedHistoryRow> {
    let mut duplicates = Vec::new();
    let mut seen_ids = HashMap::new();
    let mut seen_datetimes = HashMap::new();

    for sample in samples {
        match seen_ids.entry(sample.id) {
            Entry::Occupied(first) => {
                duplicates.push(DuplicatedHistoryRow {
                    field: HistoryDuplicateField::Id,
                    first_index: *first.get(),
                    duplicate_index: sample.index,
                    value: sample.id,
                });
            }
            Entry::Vacant(first) => {
                first.insert(sample.index);
            }
        }
        match seen_datetimes.entry(sample.datetime_ns) {
            Entry::Occupied(first) => {
                duplicates.push(DuplicatedHistoryRow {
                    field: HistoryDuplicateField::Datetime,
                    first_index: *first.get(),
                    duplicate_index: sample.index,
                    value: sample.datetime_ns,
                });
            }
            Entry::Vacant(first) => {
                first.insert(sample.index);
            }
        }
    }

    duplicates
}

fn non_monotonic_timestamps(samples: &[HistoryRowSample]) -> Vec<NonMonotonicHistoryTimestamp> {
    samples
        .windows(2)
        .filter_map(|window| {
            let previous = window[0];
            let current = window[1];
            (current.datetime_ns < previous.datetime_ns).then_some(NonMonotonicHistoryTimestamp {
                previous_index: previous.index,
                previous_datetime_ns: previous.datetime_ns,
                index: current.index,
                datetime_ns: current.datetime_ns,
            })
        })
        .collect()
}

fn out_of_range_rows(
    samples: &[HistoryRowSample],
    requested_range: (i64, i64),
) -> Vec<OutOfRangeHistoryRow> {
    samples
        .iter()
        .filter(|sample| {
            sample.datetime_ns < requested_range.0 || sample.datetime_ns >= requested_range.1
        })
        .map(|sample| OutOfRangeHistoryRow {
            index: sample.index,
            id: sample.id,
            datetime_ns: sample.datetime_ns,
        })
        .collect()
}

fn missing_cadence_intervals(
    samples: &[HistoryRowSample],
    requested_range: (i64, i64),
    duration_ns: i64,
) -> Vec<(i64, i64)> {
    if duration_ns <= 0 || requested_range.1 <= requested_range.0 {
        return Vec::new();
    }

    let datetimes = samples
        .iter()
        .filter_map(|sample| {
            (sample.datetime_ns >= requested_range.0 && sample.datetime_ns < requested_range.1)
                .then_some(sample.datetime_ns)
        })
        .collect::<BTreeSet<_>>();

    let mut missing = Vec::new();
    let mut cursor = requested_range.0;

    for datetime in datetimes {
        if datetime > cursor {
            missing.push((cursor, datetime.min(requested_range.1)));
        }
        if let Some(next_cursor) = datetime.checked_add(duration_ns) {
            cursor = cursor.max(next_cursor);
        } else {
            cursor = requested_range.1;
        }
        if cursor >= requested_range.1 {
            break;
        }
    }

    if cursor < requested_range.1 {
        missing.push((cursor, requested_range.1));
    }

    missing
}

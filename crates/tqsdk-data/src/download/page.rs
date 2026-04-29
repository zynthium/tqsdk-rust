use tqsdk_core::{Kline, Tick};

/// Progressive state for a range download request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DataDownloadProgress {
    pub(super) start_datetime_ns: i64,
    pub(super) end_datetime_ns: i64,
    pub(super) cursor_datetime_ns: Option<i64>,
    pub(super) emitted_rows: usize,
    pub(super) emitted_pages: usize,
    pub(super) complete: bool,
}

impl DataDownloadProgress {
    pub(super) fn new(start_datetime_ns: i64, end_datetime_ns: i64) -> Self {
        Self {
            start_datetime_ns,
            end_datetime_ns,
            cursor_datetime_ns: None,
            emitted_rows: 0,
            emitted_pages: 0,
            complete: false,
        }
    }

    #[must_use]
    pub fn start_datetime_ns(&self) -> i64 {
        self.start_datetime_ns
    }

    #[must_use]
    pub fn end_datetime_ns(&self) -> i64 {
        self.end_datetime_ns
    }

    #[must_use]
    pub fn cursor_datetime_ns(&self) -> Option<i64> {
        self.cursor_datetime_ns
    }

    #[must_use]
    pub fn emitted_rows(&self) -> usize {
        self.emitted_rows
    }

    #[must_use]
    pub fn emitted_pages(&self) -> usize {
        self.emitted_pages
    }

    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.complete
    }

    #[must_use]
    pub fn completion_ratio(&self) -> f64 {
        if self.complete {
            return 1.0;
        }
        let Some(cursor_datetime_ns) = self.cursor_datetime_ns else {
            return 0.0;
        };
        if self.end_datetime_ns <= self.start_datetime_ns {
            return 1.0;
        }
        let progressed = (cursor_datetime_ns - self.start_datetime_ns).max(0) as f64;
        let span = (self.end_datetime_ns - self.start_datetime_ns) as f64;
        (progressed / span).clamp(0.0, 1.0)
    }

    #[must_use]
    pub fn completion_percent(&self) -> f64 {
        self.completion_ratio() * 100.0
    }
}

/// A filtered kline page emitted by [`super::KlineDataDownload`].
#[derive(Debug, Clone, Default)]
pub struct KlineDataDownloadPage {
    rows: Vec<Kline>,
    progress: DataDownloadProgress,
}

impl KlineDataDownloadPage {
    pub(super) fn new(rows: Vec<Kline>, progress: DataDownloadProgress) -> Self {
        Self { rows, progress }
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

    pub fn iter(&self) -> impl ExactSizeIterator<Item = &Kline> + DoubleEndedIterator {
        self.rows.iter()
    }

    #[must_use]
    pub fn rows(&self) -> &[Kline] {
        &self.rows
    }

    #[must_use]
    pub fn progress(&self) -> DataDownloadProgress {
        self.progress
    }

    #[must_use]
    pub fn into_rows(self) -> Vec<Kline> {
        self.rows
    }
}

/// A filtered tick page emitted by [`super::TickDataDownload`].
#[derive(Debug, Clone, Default)]
pub struct TickDataDownloadPage {
    rows: Vec<Tick>,
    progress: DataDownloadProgress,
}

impl TickDataDownloadPage {
    pub(super) fn new(rows: Vec<Tick>, progress: DataDownloadProgress) -> Self {
        Self { rows, progress }
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

    pub fn iter(&self) -> impl ExactSizeIterator<Item = &Tick> + DoubleEndedIterator {
        self.rows.iter()
    }

    #[must_use]
    pub fn rows(&self) -> &[Tick] {
        &self.rows
    }

    #[must_use]
    pub fn progress(&self) -> DataDownloadProgress {
        self.progress
    }

    #[must_use]
    pub fn into_rows(self) -> Vec<Tick> {
        self.rows
    }
}

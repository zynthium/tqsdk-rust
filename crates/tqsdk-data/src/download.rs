#![cfg_attr(not(test), forbid(unsafe_code))]

use std::time::Duration;

use tqsdk_core::{Kline, Tick};

use crate::client::{
    DataClient, KlineDataPage, KlineDataPageRequest, KlineDataSeriesRequest, TickDataPage,
    TickDataPageRequest, TickDataSeriesRequest,
};
use crate::error::{DataError, Result};

const MAX_HISTORY_VIEW_WIDTH: usize = 10_000;

/// Progressive state for a range download request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DataDownloadProgress {
    start_datetime_ns: i64,
    end_datetime_ns: i64,
    cursor_datetime_ns: Option<i64>,
    emitted_rows: usize,
    emitted_pages: usize,
    complete: bool,
}

impl DataDownloadProgress {
    fn new(start_datetime_ns: i64, end_datetime_ns: i64) -> Self {
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

/// A filtered kline page emitted by [`KlineDataDownload`].
#[derive(Debug, Clone, Default)]
pub struct KlineDataDownloadPage {
    rows: Vec<Kline>,
    progress: DataDownloadProgress,
}

impl KlineDataDownloadPage {
    fn new(rows: Vec<Kline>, progress: DataDownloadProgress) -> Self {
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

/// A filtered tick page emitted by [`TickDataDownload`].
#[derive(Debug, Clone, Default)]
pub struct TickDataDownloadPage {
    rows: Vec<Tick>,
    progress: DataDownloadProgress,
}

impl TickDataDownloadPage {
    fn new(rows: Vec<Tick>, progress: DataDownloadProgress) -> Self {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct KlineDownloadSpec {
    duration_ns: i64,
    start_datetime_ns: i64,
    end_datetime_ns: i64,
    page_view_width: usize,
    timeout: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TickDownloadSpec {
    start_datetime_ns: i64,
    end_datetime_ns: i64,
    page_view_width: usize,
    timeout: Duration,
}

#[derive(Clone)]
struct DataClientKlinePageSource {
    client: DataClient,
}

#[derive(Clone)]
struct DataClientTickPageSource {
    client: DataClient,
}

trait KlinePageSource: Clone {
    async fn load_page(&self, request: KlineDataPageRequest) -> Result<KlineDataPage>;
}

trait TickPageSource: Clone {
    async fn load_page(&self, request: TickDataPageRequest) -> Result<TickDataPage>;
}

impl KlinePageSource for DataClientKlinePageSource {
    async fn load_page(&self, request: KlineDataPageRequest) -> Result<KlineDataPage> {
        self.client.get_kline_data_page(request).await
    }
}

impl TickPageSource for DataClientTickPageSource {
    async fn load_page(&self, request: TickDataPageRequest) -> Result<TickDataPage> {
        self.client.get_tick_data_page(request).await
    }
}

#[derive(Clone)]
struct KlineDataDownloadInner<S> {
    source: S,
    symbol: String,
    duration: Duration,
    spec: KlineDownloadSpec,
    progress: DataDownloadProgress,
    last_next_left_kline_id: Option<i64>,
    next_left_kline_id: Option<i64>,
    last_emitted_kline_id: Option<i64>,
    use_focus: bool,
    finished: bool,
}

impl<S> KlineDataDownloadInner<S>
where
    S: KlinePageSource,
{
    fn new(source: S, request: KlineDataSeriesRequest) -> Result<Self> {
        let spec = validate_kline_download_request(&request)?;
        Ok(Self {
            source,
            symbol: request.symbol().to_string(),
            duration: request.duration(),
            spec,
            progress: DataDownloadProgress::new(spec.start_datetime_ns, spec.end_datetime_ns),
            last_next_left_kline_id: None,
            next_left_kline_id: None,
            last_emitted_kline_id: None,
            use_focus: true,
            finished: false,
        })
    }

    async fn next_page(&mut self) -> Result<Option<KlineDataDownloadPage>> {
        if self.finished {
            return Ok(None);
        }

        loop {
            let mut request = KlineDataPageRequest::new(
                self.symbol.clone(),
                self.duration,
                self.spec.page_view_width,
            )
            .with_timeout(self.spec.timeout);
            if self.use_focus {
                request = request
                    .with_focus_datetime_ns(self.spec.start_datetime_ns)
                    .with_focus_position(0);
            } else if let Some(left_kline_id) = self.next_left_kline_id {
                request = request.with_left_kline_id(left_kline_id);
            } else {
                self.progress.complete = true;
                self.finished = true;
                return Ok(None);
            }

            let page = self.source.load_page(request).await?;
            let raw_page_len = page.len();
            let next_left_kline_id = page.next_left_kline_id();
            let last_raw_datetime_ns = page.last().map(|row| row.datetime);

            if let Some(last_raw_datetime_ns) = last_raw_datetime_ns {
                self.progress.cursor_datetime_ns = Some(
                    last_raw_datetime_ns
                        .clamp(self.spec.start_datetime_ns, self.spec.end_datetime_ns),
                );
            }

            let rows = page
                .into_rows()
                .into_iter()
                .filter(|row| {
                    row.datetime >= self.spec.start_datetime_ns
                        && row.datetime < self.spec.end_datetime_ns
                        && self
                            .last_emitted_kline_id
                            .is_none_or(|last_emitted_kline_id| row.id > last_emitted_kline_id)
                })
                .collect::<Vec<_>>();

            let terminal = next_left_kline_id.is_none()
                || raw_page_len < self.spec.page_view_width
                || self.last_next_left_kline_id == next_left_kline_id
                || last_raw_datetime_ns.is_some_and(|last_raw_datetime_ns| {
                    last_raw_datetime_ns >= self.spec.end_datetime_ns
                });

            if let Some(last_row) = rows.last() {
                self.last_emitted_kline_id = Some(last_row.id);
                self.progress.emitted_pages += 1;
                self.progress.emitted_rows += rows.len();
                self.progress.complete = terminal;
                let progress = self.progress;
                if terminal {
                    self.finished = true;
                } else {
                    self.last_next_left_kline_id = next_left_kline_id;
                    self.next_left_kline_id = next_left_kline_id;
                    self.use_focus = false;
                }
                return Ok(Some(KlineDataDownloadPage::new(rows, progress)));
            }

            if terminal {
                self.progress.complete = true;
                self.finished = true;
                return Ok(None);
            }

            self.last_next_left_kline_id = next_left_kline_id;
            self.next_left_kline_id = next_left_kline_id;
            self.use_focus = false;
        }
    }
}

#[derive(Clone)]
struct TickDataDownloadInner<S> {
    source: S,
    symbol: String,
    spec: TickDownloadSpec,
    progress: DataDownloadProgress,
    last_next_left_id: Option<i64>,
    next_left_id: Option<i64>,
    last_emitted_id: Option<i64>,
    use_focus: bool,
    finished: bool,
}

impl<S> TickDataDownloadInner<S>
where
    S: TickPageSource,
{
    fn new(source: S, request: TickDataSeriesRequest) -> Result<Self> {
        let spec = validate_tick_download_request(&request)?;
        Ok(Self {
            source,
            symbol: request.symbol().to_string(),
            spec,
            progress: DataDownloadProgress::new(spec.start_datetime_ns, spec.end_datetime_ns),
            last_next_left_id: None,
            next_left_id: None,
            last_emitted_id: None,
            use_focus: true,
            finished: false,
        })
    }

    async fn next_page(&mut self) -> Result<Option<TickDataDownloadPage>> {
        if self.finished {
            return Ok(None);
        }

        loop {
            let mut request =
                TickDataPageRequest::new(self.symbol.clone(), self.spec.page_view_width)
                    .with_timeout(self.spec.timeout);
            if self.use_focus {
                request = request
                    .with_focus_datetime_ns(self.spec.start_datetime_ns)
                    .with_focus_position(0);
            } else if let Some(left_id) = self.next_left_id {
                request = request.with_left_id(left_id);
            } else {
                self.progress.complete = true;
                self.finished = true;
                return Ok(None);
            }

            let page = self.source.load_page(request).await?;
            let raw_page_len = page.len();
            let next_left_id = page.next_left_id();
            let last_raw_datetime_ns = page.last().map(|row| row.datetime);

            if let Some(last_raw_datetime_ns) = last_raw_datetime_ns {
                self.progress.cursor_datetime_ns = Some(
                    last_raw_datetime_ns
                        .clamp(self.spec.start_datetime_ns, self.spec.end_datetime_ns),
                );
            }

            let rows = page
                .into_rows()
                .into_iter()
                .filter(|row| {
                    row.datetime >= self.spec.start_datetime_ns
                        && row.datetime < self.spec.end_datetime_ns
                        && self
                            .last_emitted_id
                            .is_none_or(|last_emitted_id| row.id > last_emitted_id)
                })
                .collect::<Vec<_>>();

            let terminal = next_left_id.is_none()
                || raw_page_len < self.spec.page_view_width
                || self.last_next_left_id == next_left_id
                || last_raw_datetime_ns.is_some_and(|last_raw_datetime_ns| {
                    last_raw_datetime_ns >= self.spec.end_datetime_ns
                });

            if let Some(last_row) = rows.last() {
                self.last_emitted_id = Some(last_row.id);
                self.progress.emitted_pages += 1;
                self.progress.emitted_rows += rows.len();
                self.progress.complete = terminal;
                let progress = self.progress;
                if terminal {
                    self.finished = true;
                } else {
                    self.last_next_left_id = next_left_id;
                    self.next_left_id = next_left_id;
                    self.use_focus = false;
                }
                return Ok(Some(TickDataDownloadPage::new(rows, progress)));
            }

            if terminal {
                self.progress.complete = true;
                self.finished = true;
                return Ok(None);
            }

            self.last_next_left_id = next_left_id;
            self.next_left_id = next_left_id;
            self.use_focus = false;
        }
    }
}

/// Pure-async pull-based kline download substrate over repeated history pages.
pub struct KlineDataDownload {
    inner: KlineDataDownloadInner<DataClientKlinePageSource>,
}

impl KlineDataDownload {
    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.inner.symbol
    }

    #[must_use]
    pub fn duration_ns(&self) -> i64 {
        self.inner.spec.duration_ns
    }

    #[must_use]
    pub fn start_datetime_ns(&self) -> i64 {
        self.inner.spec.start_datetime_ns
    }

    #[must_use]
    pub fn end_datetime_ns(&self) -> i64 {
        self.inner.spec.end_datetime_ns
    }

    #[must_use]
    pub fn page_view_width(&self) -> usize {
        self.inner.spec.page_view_width
    }

    #[must_use]
    pub fn progress(&self) -> DataDownloadProgress {
        self.inner.progress
    }

    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.inner.finished
    }

    pub async fn next_page(&mut self) -> Result<Option<KlineDataDownloadPage>> {
        self.inner.next_page().await
    }
}

impl std::fmt::Debug for KlineDataDownload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KlineDataDownload")
            .field("symbol", &self.symbol())
            .field("duration_ns", &self.duration_ns())
            .field("start_datetime_ns", &self.start_datetime_ns())
            .field("end_datetime_ns", &self.end_datetime_ns())
            .field("page_view_width", &self.page_view_width())
            .field("progress", &self.progress())
            .field("is_finished", &self.is_finished())
            .finish()
    }
}

/// Pure-async pull-based tick download substrate over repeated history pages.
pub struct TickDataDownload {
    inner: TickDataDownloadInner<DataClientTickPageSource>,
}

impl TickDataDownload {
    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.inner.symbol
    }

    #[must_use]
    pub fn start_datetime_ns(&self) -> i64 {
        self.inner.spec.start_datetime_ns
    }

    #[must_use]
    pub fn end_datetime_ns(&self) -> i64 {
        self.inner.spec.end_datetime_ns
    }

    #[must_use]
    pub fn page_view_width(&self) -> usize {
        self.inner.spec.page_view_width
    }

    #[must_use]
    pub fn progress(&self) -> DataDownloadProgress {
        self.inner.progress
    }

    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.inner.finished
    }

    pub async fn next_page(&mut self) -> Result<Option<TickDataDownloadPage>> {
        self.inner.next_page().await
    }
}

impl std::fmt::Debug for TickDataDownload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TickDataDownload")
            .field("symbol", &self.symbol())
            .field("start_datetime_ns", &self.start_datetime_ns())
            .field("end_datetime_ns", &self.end_datetime_ns())
            .field("page_view_width", &self.page_view_width())
            .field("progress", &self.progress())
            .field("is_finished", &self.is_finished())
            .finish()
    }
}

impl DataClient {
    /// Opens a pull-based kline range download over repeated history pages.
    pub fn kline_data_download(
        &self,
        request: KlineDataSeriesRequest,
    ) -> Result<KlineDataDownload> {
        let inner = KlineDataDownloadInner::new(
            DataClientKlinePageSource {
                client: self.clone(),
            },
            request,
        )?;
        if !self.is_session_backed() {
            return Err(DataError::InvalidState(
                "kline_data_download requires a session-backed data client",
            ));
        }
        self.require_history_download_permission()?;
        Ok(KlineDataDownload { inner })
    }

    /// Opens a pull-based tick range download over repeated history pages.
    pub fn tick_data_download(&self, request: TickDataSeriesRequest) -> Result<TickDataDownload> {
        let inner = TickDataDownloadInner::new(
            DataClientTickPageSource {
                client: self.clone(),
            },
            request,
        )?;
        if !self.is_session_backed() {
            return Err(DataError::InvalidState(
                "tick_data_download requires a session-backed data client",
            ));
        }
        self.require_history_download_permission()?;
        Ok(TickDataDownload { inner })
    }
}

fn validate_kline_download_request(request: &KlineDataSeriesRequest) -> Result<KlineDownloadSpec> {
    if request.symbol().is_empty() {
        return Err(DataError::Validation(
            "symbol must not be empty".to_string(),
        ));
    }
    let duration_ns = i64::try_from(request.duration().as_nanos()).map_err(|_| {
        DataError::Validation("duration is too large to encode as i64 nanoseconds".to_string())
    })?;
    if duration_ns <= 0 {
        return Err(DataError::Validation(
            "duration must be greater than zero".to_string(),
        ));
    }
    if request.end_datetime_ns() <= request.start_datetime_ns() {
        return Err(DataError::Validation(
            "end_datetime_ns must be greater than start_datetime_ns".to_string(),
        ));
    }
    Ok(KlineDownloadSpec {
        duration_ns,
        start_datetime_ns: request.start_datetime_ns(),
        end_datetime_ns: request.end_datetime_ns(),
        page_view_width: normalize_history_view_width(request.page_view_width())?,
        timeout: request.timeout(),
    })
}

fn validate_tick_download_request(request: &TickDataSeriesRequest) -> Result<TickDownloadSpec> {
    if request.symbol().is_empty() {
        return Err(DataError::Validation(
            "symbol must not be empty".to_string(),
        ));
    }
    if request.end_datetime_ns() <= request.start_datetime_ns() {
        return Err(DataError::Validation(
            "end_datetime_ns must be greater than start_datetime_ns".to_string(),
        ));
    }
    Ok(TickDownloadSpec {
        start_datetime_ns: request.start_datetime_ns(),
        end_datetime_ns: request.end_datetime_ns(),
        page_view_width: normalize_history_view_width(request.page_view_width())?,
        timeout: request.timeout(),
    })
}

fn normalize_history_view_width(view_width: usize) -> Result<usize> {
    if view_width == 0 {
        return Err(DataError::Validation(
            "view_width must be greater than zero".to_string(),
        ));
    }
    Ok(view_width.min(MAX_HISTORY_VIEW_WIDTH))
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use tqsdk_core::{Kline, Tick};

    use super::*;

    #[derive(Debug, Clone)]
    struct TestKlineSource {
        pages: Arc<Mutex<VecDeque<KlineDataPage>>>,
    }

    impl KlinePageSource for TestKlineSource {
        async fn load_page(&self, _request: KlineDataPageRequest) -> Result<KlineDataPage> {
            self.pages
                .lock()
                .expect("pages mutex should not be poisoned")
                .pop_front()
                .ok_or_else(|| DataError::InvalidResponse("missing kline test page".to_string()))
        }
    }

    #[derive(Debug, Clone)]
    struct TestTickSource {
        pages: Arc<Mutex<VecDeque<TickDataPage>>>,
    }

    impl TickPageSource for TestTickSource {
        async fn load_page(&self, _request: TickDataPageRequest) -> Result<TickDataPage> {
            self.pages
                .lock()
                .expect("pages mutex should not be poisoned")
                .pop_front()
                .ok_or_else(|| DataError::InvalidResponse("missing tick test page".to_string()))
        }
    }

    #[tokio::test]
    async fn kline_data_download_skips_empty_leading_pages_and_reports_progress() {
        let source = TestKlineSource {
            pages: Arc::new(Mutex::new(VecDeque::from([
                KlineDataPage::new(
                    "SHFE.ao2609".to_string(),
                    60_000_000_000,
                    2,
                    90,
                    91,
                    vec![
                        Kline {
                            id: 90,
                            datetime: 10,
                            ..Kline::default()
                        },
                        Kline {
                            id: 91,
                            datetime: 20,
                            ..Kline::default()
                        },
                    ],
                ),
                KlineDataPage::new(
                    "SHFE.ao2609".to_string(),
                    60_000_000_000,
                    2,
                    92,
                    93,
                    vec![
                        Kline {
                            id: 92,
                            datetime: 30,
                            close: 1.0,
                            ..Kline::default()
                        },
                        Kline {
                            id: 93,
                            datetime: 40,
                            close: 2.0,
                            ..Kline::default()
                        },
                    ],
                ),
                KlineDataPage::new(
                    "SHFE.ao2609".to_string(),
                    60_000_000_000,
                    2,
                    94,
                    95,
                    vec![
                        Kline {
                            id: 94,
                            datetime: 50,
                            ..Kline::default()
                        },
                        Kline {
                            id: 95,
                            datetime: 60,
                            ..Kline::default()
                        },
                    ],
                ),
            ]))),
        };

        let mut download = KlineDataDownloadInner::new(
            source,
            KlineDataSeriesRequest::new("SHFE.ao2609", Duration::from_secs(60), 25, 45)
                .with_page_view_width(2),
        )
        .unwrap();

        let page = download.next_page().await.unwrap().unwrap();
        assert_eq!(page.len(), 2);
        assert_eq!(page.rows()[0].id, 92);
        assert_eq!(page.rows()[1].id, 93);
        assert_eq!(page.progress().emitted_rows(), 2);
        assert_eq!(page.progress().emitted_pages(), 1);
        assert!(!page.progress().is_complete());
        assert_eq!(page.progress().cursor_datetime_ns(), Some(40));
        assert!((page.progress().completion_percent() - 75.0).abs() < f64::EPSILON);

        assert!(download.next_page().await.unwrap().is_none());
        assert!(download.progress.is_complete());
        assert!(download.finished);
        assert_eq!(download.progress.emitted_rows(), 2);
    }

    #[tokio::test]
    async fn tick_data_download_marks_last_emitted_page_complete() {
        let source = TestTickSource {
            pages: Arc::new(Mutex::new(VecDeque::from([TickDataPage::new(
                "SHFE.ao2609".to_string(),
                4,
                200,
                201,
                vec![
                    Tick {
                        id: 200,
                        datetime: 100,
                        last_price: 1.0,
                        ..Tick::default()
                    },
                    Tick {
                        id: 201,
                        datetime: 110,
                        last_price: 2.0,
                        ..Tick::default()
                    },
                ],
            )]))),
        };

        let mut download =
            TickDataDownloadInner::new(source, TickDataSeriesRequest::new("SHFE.ao2609", 90, 120))
                .unwrap();

        let page = download.next_page().await.unwrap().unwrap();
        assert_eq!(page.len(), 2);
        assert!(page.progress().is_complete());
        assert_eq!(page.progress().emitted_rows(), 2);
        assert_eq!(page.progress().emitted_pages(), 1);
        assert_eq!(page.progress().completion_percent(), 100.0);

        assert!(download.next_page().await.unwrap().is_none());
    }

    #[test]
    fn kline_data_download_progress_reports_zero_before_any_page() {
        let progress = DataDownloadProgress::new(10, 20);
        assert_eq!(progress.completion_ratio(), 0.0);
        assert_eq!(progress.completion_percent(), 0.0);
        assert!(!progress.is_complete());
    }

    #[test]
    fn kline_data_download_requires_session_backed_client() {
        let err = DataClient::new()
            .kline_data_download(KlineDataSeriesRequest::new(
                "SHFE.ao2609",
                Duration::from_secs(60),
                0,
                10,
            ))
            .unwrap_err();

        assert!(matches!(
            err,
            DataError::InvalidState(message)
                if message == "kline_data_download requires a session-backed data client"
        ));
    }

    #[test]
    fn tick_data_download_rejects_invalid_requests() {
        let err = DataClient::new()
            .tick_data_download(TickDataSeriesRequest::new("", 0, 10))
            .unwrap_err();
        assert!(
            matches!(err, DataError::Validation(message) if message == "symbol must not be empty")
        );

        let err = DataClient::new()
            .tick_data_download(TickDataSeriesRequest::new("SHFE.ao2609", 10, 10))
            .unwrap_err();
        assert!(matches!(
            err,
            DataError::Validation(message)
                if message == "end_datetime_ns must be greater than start_datetime_ns"
        ));
    }
}

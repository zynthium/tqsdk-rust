#![cfg_attr(not(test), forbid(unsafe_code))]

use std::time::Duration;

use tqsdk_core::{Kline, Tick};

use crate::client::{
    DataClient, KlineDataSeriesRequest, TickDataSeriesRequest, normalize_history_view_width,
};
use crate::error::{DataError, Result};

mod inner;
mod page;

use inner::{
    DataClientKlinePageSource, DataClientTickPageSource, KlineDataDownloadInner,
    TickDataDownloadInner,
};
pub use page::{DataDownloadProgress, KlineDataDownloadPage, TickDataDownloadPage};

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

    /// Collects all remaining pages into owned kline rows.
    ///
    /// If some pages have already been consumed with [`Self::next_page`], this
    /// only collects the remaining rows and preserves the download progress.
    pub async fn collect_remaining(&mut self) -> Result<Vec<Kline>> {
        self.inner.collect_remaining().await
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

    /// Collects all remaining pages into owned tick rows.
    ///
    /// If some pages have already been consumed with [`Self::next_page`], this
    /// only collects the remaining rows and preserves the download progress.
    pub async fn collect_remaining(&mut self) -> Result<Vec<Tick>> {
        self.inner.collect_remaining().await
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

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use tqsdk_core::{Kline, Tick};

    use super::inner::{KlinePageSource, TickPageSource};
    use super::*;
    use crate::client::{KlineDataPage, KlineDataPageRequest, TickDataPage, TickDataPageRequest};

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
                    true,
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
                    true,
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
                    false,
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
                false,
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

    #[tokio::test]
    async fn kline_data_download_continues_when_short_page_has_more_data() {
        let source = TestKlineSource {
            pages: Arc::new(Mutex::new(VecDeque::from([
                KlineDataPage::new(
                    "SHFE.ao2609".to_string(),
                    60_000_000_000,
                    4,
                    100,
                    101,
                    true,
                    vec![
                        Kline {
                            id: 100,
                            datetime: 10,
                            close: 1.0,
                            ..Kline::default()
                        },
                        Kline {
                            id: 101,
                            datetime: 20,
                            close: 2.0,
                            ..Kline::default()
                        },
                    ],
                ),
                KlineDataPage::new(
                    "SHFE.ao2609".to_string(),
                    60_000_000_000,
                    4,
                    102,
                    103,
                    false,
                    vec![
                        Kline {
                            id: 102,
                            datetime: 30,
                            close: 3.0,
                            ..Kline::default()
                        },
                        Kline {
                            id: 103,
                            datetime: 40,
                            close: 4.0,
                            ..Kline::default()
                        },
                    ],
                ),
            ]))),
        };

        let mut download = KlineDataDownloadInner::new(
            source,
            KlineDataSeriesRequest::new("SHFE.ao2609", Duration::from_secs(60), 0, 50)
                .with_page_view_width(4),
        )
        .unwrap();

        let first_page = download.next_page().await.unwrap().unwrap();
        assert!(!first_page.progress().is_complete());

        let second_page = download.next_page().await.unwrap().unwrap();
        assert_eq!(second_page.rows()[0].id, 102);
        assert!(second_page.progress().is_complete());
    }

    #[tokio::test]
    async fn tick_data_download_continues_when_short_page_has_more_data() {
        let source = TestTickSource {
            pages: Arc::new(Mutex::new(VecDeque::from([
                TickDataPage::new(
                    "SHFE.ao2609".to_string(),
                    4,
                    200,
                    201,
                    true,
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
                ),
                TickDataPage::new(
                    "SHFE.ao2609".to_string(),
                    4,
                    202,
                    203,
                    false,
                    vec![
                        Tick {
                            id: 202,
                            datetime: 120,
                            last_price: 3.0,
                            ..Tick::default()
                        },
                        Tick {
                            id: 203,
                            datetime: 130,
                            last_price: 4.0,
                            ..Tick::default()
                        },
                    ],
                ),
            ]))),
        };

        let mut download =
            TickDataDownloadInner::new(source, TickDataSeriesRequest::new("SHFE.ao2609", 90, 140))
                .unwrap();

        let first_page = download.next_page().await.unwrap().unwrap();
        assert!(!first_page.progress().is_complete());

        let second_page = download.next_page().await.unwrap().unwrap();
        assert_eq!(second_page.rows()[0].id, 202);
        assert!(second_page.progress().is_complete());
    }

    #[tokio::test]
    async fn kline_data_download_collect_remaining_materializes_remaining_pages() {
        let source = TestKlineSource {
            pages: Arc::new(Mutex::new(VecDeque::from([
                KlineDataPage::new(
                    "SHFE.ao2609".to_string(),
                    60_000_000_000,
                    2,
                    100,
                    101,
                    true,
                    vec![
                        Kline {
                            id: 100,
                            datetime: 10,
                            ..Kline::default()
                        },
                        Kline {
                            id: 101,
                            datetime: 20,
                            ..Kline::default()
                        },
                    ],
                ),
                KlineDataPage::new(
                    "SHFE.ao2609".to_string(),
                    60_000_000_000,
                    2,
                    102,
                    103,
                    false,
                    vec![
                        Kline {
                            id: 102,
                            datetime: 30,
                            ..Kline::default()
                        },
                        Kline {
                            id: 103,
                            datetime: 40,
                            ..Kline::default()
                        },
                    ],
                ),
            ]))),
        };

        let mut download = KlineDataDownloadInner::new(
            source,
            KlineDataSeriesRequest::new("SHFE.ao2609", Duration::from_secs(60), 0, 50)
                .with_page_view_width(2),
        )
        .unwrap();

        let rows = download.collect_remaining().await.unwrap();

        assert_eq!(
            rows.iter().map(|row| row.id).collect::<Vec<_>>(),
            vec![100, 101, 102, 103]
        );
        assert!(download.progress.is_complete());
        assert!(download.finished);
        assert!(download.next_page().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn tick_data_download_collect_remaining_materializes_after_partial_page_consumption() {
        let source = TestTickSource {
            pages: Arc::new(Mutex::new(VecDeque::from([
                TickDataPage::new(
                    "SHFE.ao2609".to_string(),
                    2,
                    200,
                    201,
                    true,
                    vec![
                        Tick {
                            id: 200,
                            datetime: 100,
                            ..Tick::default()
                        },
                        Tick {
                            id: 201,
                            datetime: 110,
                            ..Tick::default()
                        },
                    ],
                ),
                TickDataPage::new(
                    "SHFE.ao2609".to_string(),
                    2,
                    202,
                    203,
                    false,
                    vec![
                        Tick {
                            id: 202,
                            datetime: 120,
                            ..Tick::default()
                        },
                        Tick {
                            id: 203,
                            datetime: 130,
                            ..Tick::default()
                        },
                    ],
                ),
            ]))),
        };

        let mut download =
            TickDataDownloadInner::new(source, TickDataSeriesRequest::new("SHFE.ao2609", 90, 140))
                .unwrap();
        let first_page = download.next_page().await.unwrap().unwrap();
        assert_eq!(first_page.len(), 2);

        let rows = download.collect_remaining().await.unwrap();

        assert_eq!(
            rows.iter().map(|row| row.id).collect::<Vec<_>>(),
            vec![202, 203]
        );
        assert!(download.progress.is_complete());
        assert!(download.finished);
        assert_eq!(download.progress.emitted_rows(), 4);
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

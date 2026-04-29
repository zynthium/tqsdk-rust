use std::time::Duration;

use tqsdk_core::{Kline, Tick};

use crate::client::{
    DataClient, KlineDataPage, KlineDataPageRequest, KlineDataSeriesRequest, TickDataPage,
    TickDataPageRequest, TickDataSeriesRequest,
};
use crate::error::Result;

use super::{
    DataDownloadProgress, KlineDataDownloadPage, KlineDownloadSpec, TickDataDownloadPage,
    TickDownloadSpec, validate_kline_download_request, validate_tick_download_request,
};

#[derive(Clone)]
pub(super) struct DataClientKlinePageSource {
    pub(super) client: DataClient,
}

#[derive(Clone)]
pub(super) struct DataClientTickPageSource {
    pub(super) client: DataClient,
}

pub(super) trait KlinePageSource: Clone {
    async fn load_page(&self, request: KlineDataPageRequest) -> Result<KlineDataPage>;
}

pub(super) trait TickPageSource: Clone {
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
pub(super) struct KlineDataDownloadInner<S> {
    pub(super) source: S,
    pub(super) symbol: String,
    pub(super) duration: Duration,
    pub(super) spec: KlineDownloadSpec,
    pub(super) progress: DataDownloadProgress,
    pub(super) last_next_left_kline_id: Option<i64>,
    pub(super) next_left_kline_id: Option<i64>,
    pub(super) last_emitted_kline_id: Option<i64>,
    pub(super) use_focus: bool,
    pub(super) finished: bool,
}

impl<S> KlineDataDownloadInner<S>
where
    S: KlinePageSource,
{
    pub(super) fn new(source: S, request: KlineDataSeriesRequest) -> Result<Self> {
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

    pub(super) async fn next_page(&mut self) -> Result<Option<KlineDataDownloadPage>> {
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
            let next_left_kline_id = page.next_left_kline_id();
            let last_raw_datetime_ns = page.last().map(|row| row.datetime);
            let terminal = !page.more_data()
                || next_left_kline_id.is_none()
                || self.last_next_left_kline_id == next_left_kline_id
                || last_raw_datetime_ns.is_some_and(|last_raw_datetime_ns| {
                    last_raw_datetime_ns >= self.spec.end_datetime_ns
                });

            update_progress_cursor(
                &mut self.progress,
                self.spec.start_datetime_ns,
                self.spec.end_datetime_ns,
                last_raw_datetime_ns,
            );

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

            if let Some(last_row) = rows.last() {
                self.last_emitted_kline_id = Some(last_row.id);
                let progress = record_emitted_page(&mut self.progress, rows.len(), terminal);
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

    pub(super) async fn collect_remaining(&mut self) -> Result<Vec<Kline>> {
        let mut rows = Vec::new();
        while let Some(page) = self.next_page().await? {
            rows.extend(page.into_rows());
        }
        Ok(rows)
    }
}

#[derive(Clone)]
pub(super) struct TickDataDownloadInner<S> {
    pub(super) source: S,
    pub(super) symbol: String,
    pub(super) spec: TickDownloadSpec,
    pub(super) progress: DataDownloadProgress,
    pub(super) last_next_left_id: Option<i64>,
    pub(super) next_left_id: Option<i64>,
    pub(super) last_emitted_id: Option<i64>,
    pub(super) use_focus: bool,
    pub(super) finished: bool,
}

impl<S> TickDataDownloadInner<S>
where
    S: TickPageSource,
{
    pub(super) fn new(source: S, request: TickDataSeriesRequest) -> Result<Self> {
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

    pub(super) async fn next_page(&mut self) -> Result<Option<TickDataDownloadPage>> {
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
            let next_left_id = page.next_left_id();
            let last_raw_datetime_ns = page.last().map(|row| row.datetime);
            let terminal = !page.more_data()
                || next_left_id.is_none()
                || self.last_next_left_id == next_left_id
                || last_raw_datetime_ns.is_some_and(|last_raw_datetime_ns| {
                    last_raw_datetime_ns >= self.spec.end_datetime_ns
                });

            update_progress_cursor(
                &mut self.progress,
                self.spec.start_datetime_ns,
                self.spec.end_datetime_ns,
                last_raw_datetime_ns,
            );

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

            if let Some(last_row) = rows.last() {
                self.last_emitted_id = Some(last_row.id);
                let progress = record_emitted_page(&mut self.progress, rows.len(), terminal);
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

    pub(super) async fn collect_remaining(&mut self) -> Result<Vec<Tick>> {
        let mut rows = Vec::new();
        while let Some(page) = self.next_page().await? {
            rows.extend(page.into_rows());
        }
        Ok(rows)
    }
}

fn update_progress_cursor(
    progress: &mut DataDownloadProgress,
    start_datetime_ns: i64,
    end_datetime_ns: i64,
    last_raw_datetime_ns: Option<i64>,
) {
    if let Some(last_raw_datetime_ns) = last_raw_datetime_ns {
        progress.cursor_datetime_ns =
            Some(last_raw_datetime_ns.clamp(start_datetime_ns, end_datetime_ns));
    }
}

fn record_emitted_page(
    progress: &mut DataDownloadProgress,
    row_count: usize,
    terminal: bool,
) -> DataDownloadProgress {
    progress.emitted_pages += 1;
    progress.emitted_rows += row_count;
    progress.complete = terminal;
    *progress
}

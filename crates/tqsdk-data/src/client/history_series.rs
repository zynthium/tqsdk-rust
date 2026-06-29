#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::BTreeMap;
use std::sync::Arc;

use tqsdk_core::{Kline, Tick};

use crate::error::Result;
use crate::history_series_cache::{
    HistorySeriesCache, HistorySeriesCacheReport, HistorySeriesKind, HistorySeriesWriteRows,
    HistorySeriesWriteSegment, rangeset_intersection,
};
use crate::integrity::HistoryPermissionStatus;

use super::chart_ids::{next_history_chart_sequence, sanitize_chart_token};
use super::{
    DataClient, KlineDataPageRequest, KlineDataSeries, KlineDataSeriesRequest, TickDataPageRequest,
    TickDataSeries, TickDataSeriesRequest, chart_reader, page,
};

pub(super) trait HistoryRow {
    fn id(&self) -> i64;
    fn datetime(&self) -> i64;
}

impl HistoryRow for Kline {
    fn id(&self) -> i64 {
        self.id
    }

    fn datetime(&self) -> i64 {
        self.datetime
    }
}

impl HistoryRow for Tick {
    fn id(&self) -> i64 {
        self.id
    }

    fn datetime(&self) -> i64 {
        self.datetime
    }
}

impl DataClient {
    pub async fn get_kline_data_series(
        &self,
        request: KlineDataSeriesRequest,
    ) -> Result<KlineDataSeries> {
        let spec = request.validate()?;
        let session =
            self.require_session("get_kline_data_series requires a session-backed data client")?;
        self.require_history_download_permission_async(session)
            .await?;

        if let Some(cache) = self.history_cache.clone() {
            return self
                .get_cached_kline_data_series(cache, request, spec)
                .await;
        }

        let mut rows = Vec::new();
        let mut last_next_left_kline_id = None;
        let mut next_left_kline_id = None;
        let mut use_focus = true;

        loop {
            let mut page_request = KlineDataPageRequest::new(
                request.symbol(),
                request.duration(),
                spec.page_view_width,
            )
            .with_timeout(request.timeout());
            if use_focus {
                page_request = page_request
                    .with_focus_datetime_ns(spec.start_datetime_ns)
                    .with_focus_position(0);
            } else if let Some(left_kline_id) = next_left_kline_id {
                page_request = page_request.with_left_kline_id(left_kline_id);
            } else {
                break;
            }

            let page = self.get_kline_data_page(page_request).await?;
            let next_left = page.next_left_kline_id();
            let terminal = !page.more_data()
                || next_left.is_none()
                || last_next_left_kline_id == next_left
                || page
                    .last()
                    .is_some_and(|row| row.datetime >= spec.end_datetime_ns);

            extend_rows_in_window(
                &mut rows,
                page.into_rows(),
                spec.start_datetime_ns,
                spec.end_datetime_ns,
            );

            if terminal {
                break;
            }

            last_next_left_kline_id = next_left;
            next_left_kline_id = next_left;
            use_focus = false;
        }

        let series = KlineDataSeries::new(
            request.symbol().to_string(),
            spec.duration_ns,
            spec.start_datetime_ns,
            spec.end_datetime_ns,
            dedup_sort_rows_by_id(rows),
        )
        .with_permission_status(HistoryPermissionStatus::Checked);
        Ok(series)
    }

    pub async fn get_tick_data_series(
        &self,
        request: TickDataSeriesRequest,
    ) -> Result<TickDataSeries> {
        let spec = request.validate()?;
        let session =
            self.require_session("get_tick_data_series requires a session-backed data client")?;
        self.require_history_download_permission_async(session)
            .await?;

        if let Some(cache) = self.history_cache.clone() {
            return self.get_cached_tick_data_series(cache, request, spec).await;
        }

        let mut rows = Vec::new();
        let mut last_next_left_id = None;
        let mut next_left_id = None;
        let mut use_focus = true;

        loop {
            let mut page_request = TickDataPageRequest::new(request.symbol(), spec.page_view_width)
                .with_timeout(request.timeout());
            if use_focus {
                page_request = page_request
                    .with_focus_datetime_ns(spec.start_datetime_ns)
                    .with_focus_position(0);
            } else if let Some(left_id) = next_left_id {
                page_request = page_request.with_left_id(left_id);
            } else {
                break;
            }

            let page = self.get_tick_data_page(page_request).await?;
            let next_left = page.next_left_id();
            let terminal = !page.more_data()
                || next_left.is_none()
                || last_next_left_id == next_left
                || page
                    .last()
                    .is_some_and(|row| row.datetime >= spec.end_datetime_ns);

            extend_rows_in_window(
                &mut rows,
                page.into_rows(),
                spec.start_datetime_ns,
                spec.end_datetime_ns,
            );

            if terminal {
                break;
            }

            last_next_left_id = next_left;
            next_left_id = next_left;
            use_focus = false;
        }

        let series = TickDataSeries::new(
            request.symbol().to_string(),
            spec.start_datetime_ns,
            spec.end_datetime_ns,
            dedup_sort_rows_by_id(rows),
        )
        .with_permission_status(HistoryPermissionStatus::Checked);
        Ok(series)
    }

    async fn get_cached_kline_data_series(
        &self,
        cache: Arc<HistorySeriesCache>,
        request: KlineDataSeriesRequest,
        spec: page::KlineDataSeriesSpec,
    ) -> Result<KlineDataSeries> {
        let mut downloaded_id_ranges = Vec::new();
        let mut downloaded_datetime_ranges = Vec::new();
        let requested_range = (spec.start_datetime_ns, spec.end_datetime_ns);
        let kind = HistorySeriesKind::Kline {
            duration_ns: spec.duration_ns,
        };
        let missing_ranges = cache
            .kline_coverage(
                request.symbol(),
                spec.duration_ns,
                requested_range.0,
                requested_range.1,
            )?
            .missing_ranges;
        for missing in missing_ranges.into_iter().filter(|range| range.0 < range.1) {
            let rows = self
                .download_official_kline_range(&request, spec.duration_ns, missing.0, missing.1)
                .await?;
            if rows.is_empty() {
                continue;
            }
            let still_missing = cache
                .kline_coverage(
                    request.symbol(),
                    spec.duration_ns,
                    requested_range.0,
                    requested_range.1,
                )?
                .missing_ranges;
            let write_ranges = rangeset_intersection(&[missing], &still_missing);
            for write_range in write_ranges {
                let rows_to_write = filter_klines_by_datetime_ranges(rows.clone(), &[write_range]);
                if rows_to_write.is_empty() {
                    continue;
                }
                let report = cache.write_segment(HistorySeriesWriteSegment {
                    symbol: request.symbol(),
                    kind,
                    declared_range_ns: Some(write_range),
                    rows: HistorySeriesWriteRows::Klines(&rows_to_write),
                })?;
                if let Some(id_range) = report.id_range {
                    downloaded_id_ranges.push(id_range);
                    downloaded_datetime_ranges.push(write_range);
                }
            }
        }

        let rows = read_cached_klines(
            cache.as_ref(),
            request.symbol(),
            spec.duration_ns,
            spec.start_datetime_ns,
            spec.end_datetime_ns,
        )?;
        let hit_rows = cache_hit_rows(&rows, &downloaded_id_ranges);
        let series = KlineDataSeries::new(
            request.symbol().to_string(),
            spec.duration_ns,
            spec.start_datetime_ns,
            spec.end_datetime_ns,
            rows,
        )
        .with_cache_report(HistorySeriesCacheReport::new(
            cache.root_dir().to_path_buf(),
            hit_rows,
            downloaded_datetime_ranges,
        ))
        .with_permission_status(HistoryPermissionStatus::Checked);
        self.enforce_history_cache_limits(cache.as_ref())?;
        Ok(series)
    }

    async fn get_cached_tick_data_series(
        &self,
        cache: Arc<HistorySeriesCache>,
        request: TickDataSeriesRequest,
        spec: page::TickDataSeriesSpec,
    ) -> Result<TickDataSeries> {
        let mut downloaded_id_ranges = Vec::new();
        let mut downloaded_datetime_ranges = Vec::new();
        let requested_range = (spec.start_datetime_ns, spec.end_datetime_ns);
        let kind = HistorySeriesKind::Tick;
        let missing_ranges = cache
            .tick_coverage(request.symbol(), requested_range.0, requested_range.1)?
            .missing_ranges;
        for missing in missing_ranges.into_iter().filter(|range| range.0 < range.1) {
            let rows = self
                .download_official_tick_range(&request, missing.0, missing.1)
                .await?;
            if rows.is_empty() {
                continue;
            }
            let still_missing = cache
                .tick_coverage(request.symbol(), requested_range.0, requested_range.1)?
                .missing_ranges;
            let write_ranges = rangeset_intersection(&[missing], &still_missing);
            for write_range in write_ranges {
                let rows_to_write = filter_ticks_by_datetime_ranges(rows.clone(), &[write_range]);
                if rows_to_write.is_empty() {
                    continue;
                }
                let report = cache.write_segment(HistorySeriesWriteSegment {
                    symbol: request.symbol(),
                    kind,
                    declared_range_ns: Some(write_range),
                    rows: HistorySeriesWriteRows::Ticks(&rows_to_write),
                })?;
                if let Some(id_range) = report.id_range {
                    downloaded_id_ranges.push(id_range);
                    downloaded_datetime_ranges.push(write_range);
                }
            }
        }

        let rows = read_cached_ticks(
            cache.as_ref(),
            request.symbol(),
            spec.start_datetime_ns,
            spec.end_datetime_ns,
        )?;
        let hit_rows = cache_hit_rows(&rows, &downloaded_id_ranges);
        let series = TickDataSeries::new(
            request.symbol().to_string(),
            spec.start_datetime_ns,
            spec.end_datetime_ns,
            rows,
        )
        .with_cache_report(HistorySeriesCacheReport::new(
            cache.root_dir().to_path_buf(),
            hit_rows,
            downloaded_datetime_ranges,
        ))
        .with_permission_status(HistoryPermissionStatus::Checked);
        self.enforce_history_cache_limits(cache.as_ref())?;
        Ok(series)
    }

    fn enforce_history_cache_limits(&self, cache: &HistorySeriesCache) -> Result<()> {
        if self.history_cache_maintenance.enabled() {
            cache.enforce_limits(
                self.history_cache_maintenance.max_bytes,
                self.history_cache_maintenance.retention_days,
            )?;
        }
        Ok(())
    }

    async fn download_official_kline_range(
        &self,
        request: &KlineDataSeriesRequest,
        duration_ns: i64,
        start_datetime_ns: i64,
        end_datetime_ns: i64,
    ) -> Result<Vec<Kline>> {
        let session =
            self.require_session("get_kline_data_series requires a session-backed data client")?;
        let chart_id = next_data_series_chart_id("kline", request.symbol(), duration_ns);
        let result = async {
            let mut rows = Vec::new();
            let mut current_id = None;
            loop {
                let mut page_request =
                    KlineDataPageRequest::new(request.symbol(), request.duration(), 2_000)
                        .with_timeout(request.timeout());
                if let Some(left_id) = current_id {
                    page_request = page_request.with_left_kline_id(left_id);
                } else {
                    page_request = page_request
                        .with_focus_datetime_ns(start_datetime_ns)
                        .with_focus_position(0);
                }
                let page = self
                    .await_kline_data_page(
                        session,
                        &page_request,
                        page_request.validate()?,
                        chart_id.as_str(),
                    )
                    .await?;
                let more_data = page.more_data();
                let next_id = extend_rows_in_window(
                    &mut rows,
                    page.into_rows(),
                    start_datetime_ns,
                    end_datetime_ns,
                );
                if !more_data || next_id.is_none() || next_id == current_id {
                    break;
                }
                current_id = next_id;
            }
            Ok(dedup_sort_rows_by_id(rows))
        }
        .await;
        chart_reader::cancel_chart_best_effort(session, chart_id).await;
        result
    }

    async fn download_official_tick_range(
        &self,
        request: &TickDataSeriesRequest,
        start_datetime_ns: i64,
        end_datetime_ns: i64,
    ) -> Result<Vec<Tick>> {
        let session =
            self.require_session("get_tick_data_series requires a session-backed data client")?;
        let chart_id = next_data_series_chart_id("tick", request.symbol(), 0);
        let result = async {
            let mut rows = Vec::new();
            let mut current_id = None;
            loop {
                let mut page_request = TickDataPageRequest::new(request.symbol(), 2_000)
                    .with_timeout(request.timeout());
                if let Some(left_id) = current_id {
                    page_request = page_request.with_left_id(left_id);
                } else {
                    page_request = page_request
                        .with_focus_datetime_ns(start_datetime_ns)
                        .with_focus_position(0);
                }
                let page = self
                    .await_tick_data_page(
                        session,
                        &page_request,
                        page_request.validate()?,
                        chart_id.as_str(),
                    )
                    .await?;
                let more_data = page.more_data();
                let next_id = extend_rows_in_window(
                    &mut rows,
                    page.into_rows(),
                    start_datetime_ns,
                    end_datetime_ns,
                );
                if !more_data || next_id.is_none() || next_id == current_id {
                    break;
                }
                current_id = next_id;
            }
            Ok(dedup_sort_rows_by_id(rows))
        }
        .await;
        chart_reader::cancel_chart_best_effort(session, chart_id).await;
        result
    }
}

pub(super) fn extend_rows_in_window<R: HistoryRow>(
    target: &mut Vec<R>,
    page: Vec<R>,
    start_datetime_ns: i64,
    end_datetime_ns: i64,
) -> Option<i64> {
    let mut next_left_id = None;
    for row in page {
        if row.datetime() == 0 || row.datetime() >= end_datetime_ns {
            break;
        }
        next_left_id = row.id().checked_add(1);
        if row.datetime() >= start_datetime_ns {
            target.push(row);
        }
    }
    next_left_id
}

pub(super) fn dedup_sort_rows_by_id<R: HistoryRow>(rows: Vec<R>) -> Vec<R> {
    let mut by_id = BTreeMap::new();
    for row in rows {
        by_id.insert(row.id(), row);
    }
    by_id.into_values().collect()
}

fn cache_hit_rows<R: HistoryRow>(rows: &[R], downloaded_id_ranges: &[(i64, i64)]) -> usize {
    rows.iter()
        .filter(|row| {
            !downloaded_id_ranges
                .iter()
                .any(|range| row.id() >= range.0 && row.id() < range.1)
        })
        .count()
}

fn read_cached_klines(
    cache: &HistorySeriesCache,
    symbol: &str,
    duration_ns: i64,
    start_datetime_ns: i64,
    end_datetime_ns: i64,
) -> Result<Vec<Kline>> {
    cache.read_kline_window(symbol, duration_ns, start_datetime_ns, end_datetime_ns)
}

fn read_cached_ticks(
    cache: &HistorySeriesCache,
    symbol: &str,
    start_datetime_ns: i64,
    end_datetime_ns: i64,
) -> Result<Vec<Tick>> {
    cache.read_tick_window(symbol, start_datetime_ns, end_datetime_ns)
}

fn next_data_series_chart_id(kind: &str, symbol: &str, duration_ns: i64) -> String {
    let sequence = next_history_chart_sequence();
    format!(
        "data-series-{kind}-{}-{duration_ns}-{sequence}",
        sanitize_chart_token(symbol)
    )
}

fn filter_klines_by_datetime_ranges(rows: Vec<Kline>, ranges: &[(i64, i64)]) -> Vec<Kline> {
    rows.into_iter()
        .filter(|row| {
            ranges
                .iter()
                .any(|range| row.datetime >= range.0 && row.datetime < range.1)
        })
        .collect()
}

fn filter_ticks_by_datetime_ranges(rows: Vec<Tick>, ranges: &[(i64, i64)]) -> Vec<Tick> {
    rows.into_iter()
        .filter(|row| {
            ranges
                .iter()
                .any(|range| row.datetime >= range.0 && row.datetime < range.1)
        })
        .collect()
}

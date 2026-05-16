use std::collections::BTreeSet;

use tqsdk_core::{ChartId, Kline, MarketStateReadGuard, ObjectKey, StatePath};

use crate::{
    change::ChangeTrackedRef,
    step::{WaitReadHandle, WaitStep},
    views::KlineWindow,
};

/// Handle to a subscribed kline chart plus its current materialized window.
#[derive(Clone)]
pub struct KlineHandle {
    reader: WaitReadHandle,
    pub(crate) symbol: String,
    pub(crate) duration_ns: i64,
    pub(crate) view_width: usize,
    pub(crate) chart_id: String,
}

impl std::fmt::Debug for KlineHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KlineHandle")
            .field("symbol", &self.symbol)
            .field("duration_ns", &self.duration_ns)
            .field("view_width", &self.view_width)
            .field("chart_id", &self.chart_id)
            .finish_non_exhaustive()
    }
}

impl KlineHandle {
    pub(crate) fn new(
        reader: WaitReadHandle,
        symbol: String,
        duration_ns: i64,
        view_width: usize,
        chart_id: String,
    ) -> Self {
        Self {
            reader,
            symbol,
            duration_ns,
            view_width,
            chart_id,
        }
    }

    pub fn is_ready(&self) -> crate::error::Result<bool> {
        let guard = self.reader.reader().read_market_state();
        Ok(chart_is_ready(&guard, self.chart_id.as_str()))
    }

    pub fn has_rows(&self) -> crate::error::Result<bool> {
        let guard = self.reader.reader().read_market_state();
        let Some((left_id, right_id)) = chart_bounds(&guard, self.chart_id.as_str()) else {
            return Ok(false);
        };

        for id in left_id..=right_id {
            if self.decode_row_from_guard(&guard, id)?.is_some() {
                return Ok(true);
            }
        }

        Ok(false)
    }

    pub fn window(&self) -> crate::error::Result<KlineWindow> {
        let guard = self.reader.reader().read_market_state();
        let mut rows = Vec::new();
        let duration_key = self.duration_ns.to_string();

        if let Some((left_id, right_id)) = chart_bounds(&guard, self.chart_id.as_str()) {
            for id in left_id..=right_id {
                let id_key = id.to_string();
                if let Some(row) = guard.decode_path::<Kline>(&[
                    "klines",
                    self.symbol.as_str(),
                    duration_key.as_str(),
                    "data",
                    id_key.as_str(),
                ])? {
                    rows.push(row);
                }
            }
        }

        Ok(KlineWindow::new(
            self.symbol.clone(),
            self.duration_ns,
            self.view_width,
            self.chart_id.clone(),
            rows,
        ))
    }

    pub fn row(&self, id: i64) -> crate::error::Result<Option<Kline>> {
        let guard = self.reader.reader().read_market_state();
        let Some((left_id, right_id)) = chart_bounds(&guard, self.chart_id.as_str()) else {
            return Ok(None);
        };
        if id < left_id || id > right_id {
            return Ok(None);
        }

        self.decode_row_from_guard(&guard, id)
    }

    pub fn rows(&self) -> crate::error::Result<Vec<Kline>> {
        Ok(self.window()?.into_rows())
    }

    pub fn completed_rows(&self) -> crate::error::Result<Vec<Kline>> {
        Ok(self.window()?.completed_rows().to_vec())
    }

    pub fn last(&self) -> crate::error::Result<Option<Kline>> {
        let guard = self.reader.reader().read_market_state();
        let Some((_, right_id)) = chart_bounds(&guard, self.chart_id.as_str()) else {
            return Ok(None);
        };

        self.decode_row_from_guard(&guard, right_id)
    }

    pub fn last_completed(&self) -> crate::error::Result<Option<Kline>> {
        let guard = self.reader.reader().read_market_state();
        let Some((left_id, right_id)) = chart_bounds(&guard, self.chart_id.as_str()) else {
            return Ok(None);
        };
        let completed_id = right_id - 1;
        if completed_id < left_id {
            return Ok(None);
        }

        self.decode_row_from_guard(&guard, completed_id)
    }

    pub fn rows_since(&self, last_seen_id: i64) -> crate::error::Result<Vec<Kline>> {
        let guard = self.reader.reader().read_market_state();
        let Some((left_id, right_id)) = chart_bounds(&guard, self.chart_id.as_str()) else {
            return Ok(Vec::new());
        };
        let start_id = left_id.max(last_seen_id.saturating_add(1));
        let mut rows = Vec::new();
        for id in start_id..=right_id {
            if let Some(row) = self.decode_row_from_guard(&guard, id)? {
                rows.push(row);
            }
        }
        Ok(rows)
    }

    pub fn changed_rows(&self, step: &WaitStep) -> crate::error::Result<Vec<Kline>> {
        if !step.is_changing(self) {
            return Ok(Vec::new());
        }

        let changed_ids = self.changed_row_ids(step);
        if changed_ids.is_empty() {
            return self.rows();
        }

        let guard = self.reader.reader().read_market_state();
        let mut rows = Vec::new();
        for id in changed_ids {
            if !id_in_chart_bounds(&guard, self.chart_id.as_str(), id) {
                continue;
            }
            if let Some(row) = self.decode_row_from_guard(&guard, id)? {
                rows.push(row);
            }
        }
        Ok(rows)
    }

    fn changed_row_ids(&self, step: &WaitStep) -> BTreeSet<i64> {
        let mut ids = BTreeSet::new();

        for key in &step.changes().object_hits {
            if let ObjectKey::Kline { series, bar_id } = key
                && series.primary.as_str() == self.symbol
                && series.duration_ns == self.duration_ns
            {
                ids.insert(*bar_id);
            }
        }

        for hit in &step.changes().field_hits {
            if let ObjectKey::Kline { series, bar_id } = &hit.object
                && series.primary.as_str() == self.symbol
                && series.duration_ns == self.duration_ns
            {
                ids.insert(*bar_id);
            }
        }

        for path in &step.changes().path_hits {
            if let Some(row_id) = kline_row_id_from_path(path, &self.symbol, self.duration_ns) {
                ids.insert(row_id);
            }
        }

        ids
    }

    fn decode_row_from_guard(
        &self,
        guard: &MarketStateReadGuard<'_>,
        id: i64,
    ) -> crate::error::Result<Option<Kline>> {
        let duration_key = self.duration_ns.to_string();
        let id_key = id.to_string();
        guard
            .decode_path::<Kline>(&[
                "klines",
                self.symbol.as_str(),
                duration_key.as_str(),
                "data",
                id_key.as_str(),
            ])
            .map_err(Into::into)
    }
}

fn kline_row_id_from_path(path: &StatePath, symbol: &str, duration_ns: i64) -> Option<i64> {
    match path.segments() {
        [root, path_symbol, duration, row_id]
            if root == "klines"
                && path_symbol == symbol
                && duration.parse::<i64>().ok()? == duration_ns =>
        {
            row_id.parse().ok()
        }
        [root, path_symbol, duration, data, row_id]
            if root == "klines"
                && path_symbol == symbol
                && duration.parse::<i64>().ok()? == duration_ns
                && data == "data" =>
        {
            row_id.parse().ok()
        }
        _ => None,
    }
}

fn chart_bounds(guard: &MarketStateReadGuard<'_>, chart_id: &str) -> Option<(i64, i64)> {
    let left_id = guard
        .get_path(&["charts", chart_id, "left_id"])
        .and_then(|value| value.as_i64())?;
    let right_id = guard
        .get_path(&["charts", chart_id, "right_id"])
        .and_then(|value| value.as_i64())?;

    (left_id <= right_id).then_some((left_id, right_id))
}

fn chart_is_ready(guard: &MarketStateReadGuard<'_>, chart_id: &str) -> bool {
    let ready = guard
        .get_path(&["charts", chart_id, "ready"])
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    let more_data = guard
        .get_path(&["charts", chart_id, "more_data"])
        .and_then(|value| value.as_bool())
        .unwrap_or(true);

    ready && !more_data
}

fn id_in_chart_bounds(guard: &MarketStateReadGuard<'_>, chart_id: &str, id: i64) -> bool {
    chart_bounds(guard, chart_id).is_some_and(|(left_id, right_id)| id >= left_id && id <= right_id)
}

impl ChangeTrackedRef for KlineHandle {
    fn object_key(&self) -> Option<ObjectKey> {
        Some(ObjectKey::Chart {
            chart_id: ChartId::new(self.chart_id.clone()),
        })
    }

    fn state_path(&self) -> StatePath {
        StatePath::new(["charts", self.chart_id.as_str()])
    }

    fn visit_extra_state_paths(&self, visit: &mut dyn FnMut(StatePath)) {
        let duration_key = self.duration_ns.to_string();
        visit(StatePath::new([
            "klines",
            self.symbol.as_str(),
            duration_key.as_str(),
            "data",
        ]));
    }

    fn visit_field_state_paths(&self, visit: &mut dyn FnMut(StatePath)) {
        self.visit_extra_state_paths(visit);
    }
}

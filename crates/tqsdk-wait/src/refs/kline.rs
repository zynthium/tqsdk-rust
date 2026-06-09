use std::collections::{BTreeMap, BTreeSet};

use tqsdk_core::{ChartId, Kline, MarketStateReadGuard, ObjectKey, StatePath};

use crate::{
    change::ChangeTrackedRef,
    step::{WaitReadHandle, WaitStep},
    views::{KlineWindow, MultiKlineRow, MultiKlineWindow},
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

/// Handle to a multi-contract kline chart aligned by the primary symbol.
#[derive(Clone)]
pub struct MultiKlineHandle {
    reader: WaitReadHandle,
    symbols: Vec<String>,
    duration_ns: i64,
    view_width: usize,
    chart_id: String,
}

impl std::fmt::Debug for MultiKlineHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MultiKlineHandle")
            .field("symbols", &self.symbols)
            .field("duration_ns", &self.duration_ns)
            .field("view_width", &self.view_width)
            .field("chart_id", &self.chart_id)
            .finish_non_exhaustive()
    }
}

impl MultiKlineHandle {
    pub(crate) fn new(
        reader: WaitReadHandle,
        symbols: Vec<String>,
        duration_ns: i64,
        view_width: usize,
        chart_id: String,
    ) -> Self {
        Self {
            reader,
            symbols,
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
        Ok(!self.window()?.is_empty())
    }

    pub fn window(&self) -> crate::error::Result<MultiKlineWindow> {
        let guard = self.reader.reader().read_market_state();
        let mut rows = Vec::new();

        if let Some((left_id, right_id)) = chart_bounds(&guard, self.chart_id.as_str()) {
            for id in left_id..=right_id {
                if let Some(row) = self.decode_aligned_row_from_guard(&guard, id)? {
                    rows.push(row);
                }
            }
        }

        let excess = rows.len().saturating_sub(self.view_width);
        if excess > 0 {
            rows.drain(0..excess);
        }

        Ok(MultiKlineWindow::new(
            self.symbols.clone(),
            self.duration_ns,
            self.view_width,
            self.chart_id.clone(),
            rows,
        ))
    }

    #[must_use]
    pub fn symbols(&self) -> Vec<&str> {
        self.symbols.iter().map(String::as_str).collect()
    }

    #[must_use]
    pub fn primary_symbol(&self) -> &str {
        self.symbols.first().map_or("", String::as_str)
    }

    #[must_use]
    pub fn duration_ns(&self) -> i64 {
        self.duration_ns
    }

    #[must_use]
    pub fn view_width(&self) -> usize {
        self.view_width
    }

    #[must_use]
    pub fn chart_id(&self) -> &str {
        &self.chart_id
    }

    fn decode_aligned_row_from_guard(
        &self,
        guard: &MarketStateReadGuard<'_>,
        primary_id: i64,
    ) -> crate::error::Result<Option<MultiKlineRow>> {
        let Some(primary_symbol) = self.symbols.first() else {
            return Ok(None);
        };
        let Some(primary_row) =
            decode_kline_row(guard, primary_symbol.as_str(), self.duration_ns, primary_id)?
        else {
            return Ok(None);
        };

        let mut rows = BTreeMap::new();
        rows.insert(primary_symbol.clone(), primary_row);

        for symbol in self.symbols.iter().skip(1) {
            let Some(bound_id) = binding_row_id(
                guard,
                primary_symbol.as_str(),
                self.duration_ns,
                symbol.as_str(),
                primary_id,
            ) else {
                return Ok(None);
            };
            let Some(row) = decode_kline_row(guard, symbol.as_str(), self.duration_ns, bound_id)?
            else {
                return Ok(None);
            };
            rows.insert(symbol.clone(), row);
        }

        Ok(Some(MultiKlineRow::new(primary_id, rows)))
    }
}

impl ChangeTrackedRef for MultiKlineHandle {
    fn object_key(&self) -> Option<ObjectKey> {
        Some(ObjectKey::Chart {
            chart_id: ChartId::new(self.chart_id.clone()),
        })
    }

    fn state_path(&self) -> StatePath {
        StatePath::new(["charts", self.chart_id.as_str()])
    }

    fn visit_extra_state_paths(&self, visit: &mut dyn FnMut(StatePath)) {
        let Some(primary_symbol) = self.symbols.first() else {
            return;
        };
        let duration_key = self.duration_ns.to_string();
        for symbol in &self.symbols {
            visit(StatePath::new([
                "klines",
                symbol.as_str(),
                duration_key.as_str(),
                "data",
            ]));
        }
        for symbol in self.symbols.iter().skip(1) {
            visit(StatePath::new([
                "klines",
                primary_symbol.as_str(),
                duration_key.as_str(),
                "binding",
                symbol.as_str(),
            ]));
        }
    }

    fn visit_field_state_paths(&self, visit: &mut dyn FnMut(StatePath)) {
        self.visit_extra_state_paths(visit);
    }
}

fn decode_kline_row(
    guard: &MarketStateReadGuard<'_>,
    symbol: &str,
    duration_ns: i64,
    id: i64,
) -> crate::error::Result<Option<Kline>> {
    let duration_key = duration_ns.to_string();
    let id_key = id.to_string();
    guard
        .decode_path::<Kline>(&[
            "klines",
            symbol,
            duration_key.as_str(),
            "data",
            id_key.as_str(),
        ])
        .map_err(Into::into)
}

fn binding_row_id(
    guard: &MarketStateReadGuard<'_>,
    primary_symbol: &str,
    duration_ns: i64,
    secondary_symbol: &str,
    primary_id: i64,
) -> Option<i64> {
    let duration_key = duration_ns.to_string();
    let id_key = primary_id.to_string();
    let value = guard.get_path(&[
        "klines",
        primary_symbol,
        duration_key.as_str(),
        "binding",
        secondary_symbol,
        id_key.as_str(),
    ])?;
    let bound_id = value
        .as_i64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))?;
    (bound_id >= 0).then_some(bound_id)
}

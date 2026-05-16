use std::collections::BTreeSet;

use tqsdk_core::{ChartId, MarketStateReadGuard, ObjectKey, StatePath, Tick};

use crate::{
    change::ChangeTrackedRef,
    step::{WaitReadHandle, WaitStep},
    views::TickWindow,
};

/// Handle to a subscribed tick chart plus its current materialized window.
#[derive(Clone)]
pub struct TickHandle {
    reader: WaitReadHandle,
    pub(crate) symbol: String,
    pub(crate) view_width: usize,
    pub(crate) chart_id: String,
}

impl std::fmt::Debug for TickHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TickHandle")
            .field("symbol", &self.symbol)
            .field("view_width", &self.view_width)
            .field("chart_id", &self.chart_id)
            .finish_non_exhaustive()
    }
}

impl TickHandle {
    pub(crate) fn new(
        reader: WaitReadHandle,
        symbol: String,
        view_width: usize,
        chart_id: String,
    ) -> Self {
        Self {
            reader,
            symbol,
            view_width,
            chart_id,
        }
    }

    pub fn is_ready(&self) -> crate::error::Result<bool> {
        let guard = self.reader.reader().read_market_state();
        let ready = guard
            .get_path(&["charts", self.chart_id.as_str(), "ready"])
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
        let more_data = guard
            .get_path(&["charts", self.chart_id.as_str(), "more_data"])
            .and_then(|value| value.as_bool())
            .unwrap_or(true);

        Ok(ready && !more_data && !self.window()?.is_empty())
    }

    pub fn window(&self) -> crate::error::Result<TickWindow> {
        let guard = self.reader.reader().read_market_state();
        let mut rows = Vec::new();

        if let Some((left_id, right_id)) = chart_bounds(&guard, self.chart_id.as_str()) {
            for id in left_id..=right_id {
                let id_key = id.to_string();
                if let Some(row) = guard.decode_path::<Tick>(&[
                    "ticks",
                    self.symbol.as_str(),
                    "data",
                    id_key.as_str(),
                ])? {
                    rows.push(row);
                }
            }
        }

        Ok(TickWindow::new(
            self.symbol.clone(),
            self.view_width,
            self.chart_id.clone(),
            rows,
        ))
    }

    pub fn rows(&self) -> crate::error::Result<Vec<Tick>> {
        Ok(self.window()?.into_rows())
    }

    pub fn last(&self) -> crate::error::Result<Option<Tick>> {
        Ok(self.window()?.last().cloned())
    }

    pub fn rows_since(&self, last_seen_id: i64) -> crate::error::Result<Vec<Tick>> {
        Ok(self
            .window()?
            .into_rows()
            .into_iter()
            .filter(|row| row.id > last_seen_id)
            .collect())
    }

    pub fn changed_rows(&self, step: &WaitStep) -> crate::error::Result<Vec<Tick>> {
        if !step.is_changing(self) {
            return Ok(Vec::new());
        }

        let window = self.window()?;
        let changed_ids = self.changed_row_ids(step);
        if changed_ids.is_empty() {
            return Ok(window.into_rows());
        }

        Ok(window
            .into_rows()
            .into_iter()
            .filter(|row| changed_ids.contains(&row.id))
            .collect())
    }

    fn changed_row_ids(&self, step: &WaitStep) -> BTreeSet<i64> {
        let mut ids = BTreeSet::new();

        for key in &step.changes().object_hits {
            if let ObjectKey::Tick { symbol, tick_id } = key
                && symbol.as_str() == self.symbol
            {
                ids.insert(*tick_id);
            }
        }

        for hit in &step.changes().field_hits {
            if let ObjectKey::Tick { symbol, tick_id } = &hit.object
                && symbol.as_str() == self.symbol
            {
                ids.insert(*tick_id);
            }
        }

        for path in &step.changes().path_hits {
            if let Some(row_id) = tick_row_id_from_path(path, &self.symbol) {
                ids.insert(row_id);
            }
        }

        ids
    }
}

fn tick_row_id_from_path(path: &StatePath, symbol: &str) -> Option<i64> {
    match path.segments() {
        [root, path_symbol, row_id] if root == "ticks" && path_symbol == symbol => {
            row_id.parse().ok()
        }
        [root, path_symbol, data, row_id]
            if root == "ticks" && path_symbol == symbol && data == "data" =>
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

impl ChangeTrackedRef for TickHandle {
    fn object_key(&self) -> Option<ObjectKey> {
        Some(ObjectKey::Chart {
            chart_id: ChartId::new(self.chart_id.clone()),
        })
    }

    fn state_path(&self) -> StatePath {
        StatePath::new(["charts", self.chart_id.as_str()])
    }

    fn visit_extra_state_paths(&self, visit: &mut dyn FnMut(StatePath)) {
        visit(StatePath::new(["ticks", self.symbol.as_str(), "data"]));
    }

    fn visit_field_state_paths(&self, visit: &mut dyn FnMut(StatePath)) {
        self.visit_extra_state_paths(visit);
    }
}

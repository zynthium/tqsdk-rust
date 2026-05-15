use tqsdk_core::{ChartId, Kline, MarketStateReadGuard, ObjectKey, StatePath};

use crate::{change::ChangeTrackedRef, step::WaitReadHandle, views::KlineWindow};

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

    pub fn rows(&self) -> crate::error::Result<Vec<Kline>> {
        Ok(self.window()?.into_rows())
    }

    pub fn completed_rows(&self) -> crate::error::Result<Vec<Kline>> {
        Ok(self.window()?.completed_rows().to_vec())
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

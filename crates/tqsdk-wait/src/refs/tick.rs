use tqsdk_core::{ChartId, MarketStateReadGuard, ObjectKey, StatePath, Tick};

use crate::{api::TqApi, change::ChangeTrackedRef, views::TickWindow};

/// Handle to a subscribed tick chart plus its current materialized window.
#[derive(Debug, Clone)]
pub struct TickSerialRef {
    pub(crate) symbol: String,
    pub(crate) view_width: usize,
    pub(crate) chart_id: String,
}

impl TickSerialRef {
    pub fn is_ready(&self, api: &TqApi) -> crate::error::Result<bool> {
        let guard = api.driver.reader.read_market_state();
        let ready = guard
            .get_path(&["charts", self.chart_id.as_str(), "ready"])
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
        let more_data = guard
            .get_path(&["charts", self.chart_id.as_str(), "more_data"])
            .and_then(|value| value.as_bool())
            .unwrap_or(true);

        Ok(ready && !more_data && !self.load(api)?.is_empty())
    }

    pub fn load(&self, api: &TqApi) -> crate::error::Result<TickWindow> {
        let guard = api.driver.reader.read_market_state();
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

impl ChangeTrackedRef for TickSerialRef {
    fn object_key(&self) -> Option<ObjectKey> {
        Some(ObjectKey::Chart {
            chart_id: ChartId::new(self.chart_id.clone()),
        })
    }

    fn state_path(&self) -> StatePath {
        StatePath::new(["charts", self.chart_id.as_str()])
    }
}

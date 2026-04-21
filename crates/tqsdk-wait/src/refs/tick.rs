use tqsdk_core::Tick;

use crate::{api::TqApi, views::TickWindow};

#[derive(Debug, Clone)]
pub struct TickSerialRef {
    pub(crate) symbol: String,
    pub(crate) view_width: usize,
    pub(crate) chart_id: String,
}

impl TickSerialRef {
    pub fn is_ready(&self, api: &TqApi) -> crate::error::Result<bool> {
        let guard = api.driver.reader.read();
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
        let guard = api.driver.reader.read();
        let mut rows = Vec::new();

        if let Some(data) = guard
            .get_path(&["ticks", self.symbol.as_str(), "data"])
            .and_then(|value| value.as_object())
        {
            let mut ids = data
                .keys()
                .filter_map(|key| key.parse::<i64>().ok())
                .collect::<Vec<_>>();
            ids.sort_unstable();

            for id in ids.into_iter().rev().take(self.view_width).rev() {
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

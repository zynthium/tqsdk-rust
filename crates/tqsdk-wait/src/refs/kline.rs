use tqsdk_core::{ChartId, Kline, ObjectKey, StatePath};

use crate::{api::TqApi, change::ChangeTrackedRef, views::KlineWindow};

#[derive(Debug, Clone)]
pub struct KlineSerialRef {
    pub(crate) symbol: String,
    pub(crate) duration_ns: i64,
    pub(crate) view_width: usize,
    pub(crate) chart_id: String,
}

impl KlineSerialRef {
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

    pub fn load(&self, api: &TqApi) -> crate::error::Result<KlineWindow> {
        let guard = api.driver.reader.read();
        let mut rows = Vec::new();
        let duration_key = self.duration_ns.to_string();
        let data_path = [
            "klines",
            self.symbol.as_str(),
            duration_key.as_str(),
            "data",
        ];

        if let Some(data) = guard
            .get_path(&data_path)
            .and_then(|value| value.as_object())
        {
            let mut ids = data
                .keys()
                .filter_map(|key| key.parse::<i64>().ok())
                .collect::<Vec<_>>();
            ids.sort_unstable();

            for id in ids.into_iter().rev().take(self.view_width).rev() {
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
}

impl ChangeTrackedRef for KlineSerialRef {
    fn object_key(&self) -> Option<ObjectKey> {
        Some(ObjectKey::Chart {
            chart_id: ChartId::new(self.chart_id.clone()),
        })
    }

    fn state_path(&self) -> StatePath {
        StatePath::new(["charts", self.chart_id.as_str()])
    }
}

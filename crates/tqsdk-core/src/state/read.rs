use std::any::type_name;

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::{ContractError, Result, ids::Revision};

use super::{MarketStateView, TradeStateView};

/// Borrowed, revision-bound view into the runtime state tree.
#[derive(Clone, Copy)]
pub struct StateReadView<'a> {
    revision: Revision,
    data: &'a Value,
}

impl<'a> StateReadView<'a> {
    pub(crate) fn new(revision: Revision, data: &'a Value) -> Self {
        Self { revision, data }
    }

    /// Returns the snapshot revision this view is bound to.
    pub fn revision(&self) -> Revision {
        self.revision
    }

    /// Returns a typed market-domain view over this revision-bound snapshot.
    pub fn market_state(&self) -> MarketStateView<'a> {
        MarketStateView::new(*self)
    }

    /// Returns a typed trade-domain view over this revision-bound snapshot.
    pub fn trade_state(&self) -> TradeStateView<'a> {
        TradeStateView::new(*self)
    }

    /// Looks up a value at the provided path.
    pub fn get<I, S>(&self, path: I) -> Option<&'a Value>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        get_at_path(self.data, path)
    }

    /// Looks up a value using a borrowed path slice.
    ///
    /// This avoids per-segment ownership when the caller already has
    /// `&str` segments and is the preferred hot-path lookup surface.
    pub fn get_path(&self, path: &[&str]) -> Option<&'a Value> {
        get_at_path(self.data, path.iter().copied())
    }

    /// Decodes a value at the provided path into `T`.
    pub fn decode<T, I, S>(&self, path: I) -> Result<Option<T>>
    where
        T: DeserializeOwned,
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let segments: Vec<String> = path
            .into_iter()
            .map(|segment| segment.as_ref().to_owned())
            .collect();
        let Some(value) = self.get(segments.iter().map(String::as_str)) else {
            return Ok(None);
        };

        decode_value_at_path(value, &segments).map(Some)
    }

    /// Decodes a value using a borrowed path slice.
    ///
    /// Unlike [`Self::decode`], the success path performs no per-segment
    /// allocations. Prefer this method in latency-sensitive readers when the
    /// path is known as `&str` segments.
    pub fn decode_path<T>(&self, path: &[&str]) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        let Some(value) = self.get_path(path) else {
            return Ok(None);
        };

        decode_value_at_path(value, path).map(Some)
    }
}

pub(crate) fn get_at_path<I, S>(data: &Value, path: I) -> Option<&Value>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut cursor = data;
    for segment in path {
        let map = cursor.as_object()?;
        cursor = map.get(segment.as_ref())?;
    }
    Some(cursor)
}

pub(crate) fn decode_value_at_path<T, S>(value: &Value, path: &[S]) -> Result<T>
where
    T: DeserializeOwned,
    S: AsRef<str>,
{
    T::deserialize(value).map_err(|err| {
        ContractError::validation(format!(
            "failed to decode state path {} as {}: {err}",
            format_state_path(path),
            type_name::<T>()
        ))
    })
}

fn format_state_path<S>(path: &[S]) -> String
where
    S: AsRef<str>,
{
    if path.is_empty() {
        "<root>".to_string()
    } else {
        let mut formatted = String::new();
        for (index, segment) in path.iter().enumerate() {
            if index > 0 {
                formatted.push('.');
            }
            formatted.push_str(segment.as_ref());
        }
        formatted
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde_json::json;

    use super::*;

    #[test]
    fn state_read_get_at_path_returns_nested_values() {
        let data = json!({
            "quotes": {
                "SHFE.au2606": {
                    "last_price": 610.5,
                    "volume": 12
                }
            }
        });
        let read = StateReadView::new(Revision::new(9), &data);

        assert_eq!(read.revision(), Revision::new(9));
        assert_eq!(
            read.get_path(&["quotes", "SHFE.au2606", "last_price"]),
            Some(&json!(610.5))
        );
        let decoded = read
            .decode_path::<VolumeFixture>(&["quotes", "SHFE.au2606"])
            .expect("volume fixture decode should succeed")
            .expect("volume fixture should exist");
        assert_eq!(decoded.volume, 12);
        assert!(read.get_path(&["quotes", "DCE.m2605"]).is_none());
    }

    #[test]
    fn state_read_decode_value_reports_path_on_type_error() {
        let data = json!({"quotes": {"SHFE.au2606": {"volume": "not-a-number"}}});
        let read = StateReadView::new(Revision::new(1), &data);

        let error = read
            .decode_path::<VolumeFixture>(&["quotes", "SHFE.au2606"])
            .expect_err("invalid nested type should report validation error");

        let message = error.to_string();
        assert!(message.contains("quotes.SHFE.au2606"));
        assert!(message.contains("VolumeFixture"));
    }

    #[test]
    fn decode_value_at_path_formats_root_path_for_root_errors() {
        let error = decode_value_at_path::<VolumeFixture, &str>(&json!("invalid"), &[])
            .expect_err("root decode should fail");

        assert!(error.to_string().contains("<root>"));
    }

    #[derive(Debug, Deserialize)]
    struct VolumeFixture {
        volume: i64,
    }
}

use std::any::type_name;

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::{ContractError, Result, ids::Revision};

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

fn get_at_path<I, S>(data: &Value, path: I) -> Option<&Value>
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

fn decode_value_at_path<T, S>(value: &Value, path: &[S]) -> Result<T>
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

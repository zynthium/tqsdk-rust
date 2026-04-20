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

    pub fn revision(&self) -> Revision {
        self.revision
    }

    pub fn get<I, S>(&self, path: I) -> Option<&'a Value>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut cursor = self.data;
        for segment in path {
            let map = cursor.as_object()?;
            cursor = map.get(segment.as_ref())?;
        }
        Some(cursor)
    }

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
}

fn decode_value_at_path<T>(value: &Value, path: &[String]) -> Result<T>
where
    T: DeserializeOwned,
{
    T::deserialize(value).map_err(|err| {
        ContractError::validation(format!(
            "failed to decode state path {} as {}: {err}",
            format_state_path(path),
            type_name::<T>()
        ))
    })
}

fn format_state_path(path: &[String]) -> String {
    if path.is_empty() {
        "<root>".to_string()
    } else {
        path.join(".")
    }
}

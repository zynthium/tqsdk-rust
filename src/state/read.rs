use serde_json::Value;

use crate::ids::Revision;

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
}

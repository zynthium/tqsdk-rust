use serde_json::{Map, Value};

use crate::{events::NormalizedMutation, ids::Revision};

use super::{PathSegment, StateReadView};

#[derive(Debug, Clone, PartialEq)]
pub struct StateSnapshot {
    revision: Revision,
    data: Value,
}

pub(crate) type StateStore = StateSnapshot;

impl StateSnapshot {
    pub fn new(revision: Revision) -> Self {
        Self {
            revision,
            data: Value::Object(Map::new()),
        }
    }

    pub fn revision(&self) -> Revision {
        self.revision
    }

    pub fn get<I, S>(&self, path: I) -> Option<&Value>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.read().get(path)
    }

    pub fn read(&self) -> StateReadView<'_> {
        StateReadView::new(self.revision, &self.data)
    }

    pub(crate) fn apply(
        &mut self,
        revision: Revision,
        mutations: &[NormalizedMutation],
    ) -> Vec<NormalizedMutation> {
        let mut applied = Vec::new();
        for mutation in mutations {
            if let Some(changed) = apply_mutation(&mut self.data, mutation) {
                applied.push(changed);
            }
        }
        if !applied.is_empty() {
            self.revision = revision;
        }
        applied
    }
}

fn apply_mutation(root: &mut Value, mutation: &NormalizedMutation) -> Option<NormalizedMutation> {
    let target = ensure_object_path(root, mutation.path.segments());
    let map = target
        .as_object_mut()
        .expect("state snapshot path targets must always resolve to objects");

    let mut changed_fields = Vec::new();
    for field in &mutation.fields {
        let has_changed = if field.value.is_null() {
            map.contains_key(&field.field)
        } else {
            map.get(&field.field) != Some(&field.value)
        };

        if !has_changed {
            continue;
        }

        if field.value.is_null() {
            map.remove(&field.field);
        } else {
            map.insert(field.field.clone(), field.value.clone());
        }

        changed_fields.push(field.clone());
    }

    if changed_fields.is_empty() {
        None
    } else {
        Some(NormalizedMutation {
            path: mutation.path.clone(),
            object: mutation.object.clone(),
            fields: changed_fields,
            source: mutation.source,
        })
    }
}

fn ensure_object_path<'a>(root: &'a mut Value, path: &[PathSegment]) -> &'a mut Value {
    if !root.is_object() {
        *root = Value::Object(Map::new());
    }

    let mut cursor = root;
    for segment in path {
        let map = cursor
            .as_object_mut()
            .expect("state snapshot intermediate nodes must always be objects");
        cursor = map
            .entry(segment.clone())
            .or_insert_with(|| Value::Object(Map::new()));
        if !cursor.is_object() {
            *cursor = Value::Object(Map::new());
        }
    }

    cursor
}

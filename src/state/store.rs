use serde_json::{Map, Value};

use crate::{Result, events::NormalizedMutation, ids::Revision};

use super::{PathSegment, StateReadView};

/// Owned snapshot clone of the runtime state tree.
///
/// Prefer `StateReadView` and `SnapshotReadGuard` on hot paths. Keep
/// `StateSnapshot` when detached ownership is required.
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

    pub fn decode<T, I, S>(&self, path: I) -> Result<Option<T>>
    where
        T: serde::de::DeserializeOwned,
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.read().decode(path)
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
    let mut changed_fields = Vec::new();
    apply_mutation_at_path(
        root,
        mutation.path.segments(),
        &mutation.fields,
        &mut changed_fields,
    );

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

fn apply_mutation_at_path(
    cursor: &mut Value,
    path: &[PathSegment],
    fields: &[crate::events::FieldMutation],
    changed_fields: &mut Vec<crate::events::FieldMutation>,
) {
    if path.is_empty() {
        apply_fields(cursor, fields, changed_fields);
        return;
    }

    let segment = &path[0];
    let child = ensure_child_object(cursor, segment);
    apply_mutation_at_path(child, &path[1..], fields, changed_fields);
    prune_empty_child(cursor, segment);
}

fn apply_fields(
    cursor: &mut Value,
    fields: &[crate::events::FieldMutation],
    changed_fields: &mut Vec<crate::events::FieldMutation>,
) {
    if !cursor.is_object() {
        *cursor = Value::Object(Map::new());
    }

    let map = cursor
        .as_object_mut()
        .expect("state snapshot path targets must always resolve to objects");

    for field in fields {
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
}

fn ensure_child_object<'a>(root: &'a mut Value, segment: &PathSegment) -> &'a mut Value {
    if !root.is_object() {
        *root = Value::Object(Map::new());
    }

    let map = root
        .as_object_mut()
        .expect("state snapshot intermediate nodes must always be objects");
    let child = map
        .entry(segment.clone())
        .or_insert_with(|| Value::Object(Map::new()));
    if !child.is_object() {
        *child = Value::Object(Map::new());
    }

    child
}

fn prune_empty_child(root: &mut Value, segment: &PathSegment) {
    let Some(map) = root.as_object_mut() else {
        return;
    };

    let should_remove = map
        .get(segment)
        .and_then(Value::as_object)
        .is_some_and(Map::is_empty);
    if should_remove {
        map.remove(segment);
    }
}

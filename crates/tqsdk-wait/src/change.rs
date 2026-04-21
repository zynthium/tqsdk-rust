#![cfg_attr(not(test), forbid(unsafe_code))]

use tqsdk_core::{ChangeSet, ObjectKey, StatePath};

pub trait ChangeTrackedRef {
    fn object_key(&self) -> Option<ObjectKey>;
    fn state_path(&self) -> StatePath;
}

pub fn matches_any(changes: &ChangeSet, target: &impl ChangeTrackedRef) -> bool {
    if let Some(key) = target.object_key()
        && changes.object_hits.contains(&key)
    {
        return true;
    }

    changes
        .path_hits
        .iter()
        .any(|path| path == &target.state_path())
}

pub fn matches_fields(
    changes: &ChangeSet,
    target: &impl ChangeTrackedRef,
    fields: &[&str],
) -> bool {
    let Some(key) = target.object_key() else {
        return false;
    };

    changes
        .field_hits
        .iter()
        .any(|hit| hit.object == key && fields.iter().any(|field| *field == hit.field))
}

#![cfg_attr(not(test), forbid(unsafe_code))]

use tqsdk_core::{ChangeSet, ObjectKey, StatePath};

/// Lightweight handle that can be matched against the latest diff commit.
pub trait ChangeTrackedRef {
    fn object_key(&self) -> Option<ObjectKey>;
    fn state_path(&self) -> StatePath;
}

pub(crate) fn matches_any(changes: &ChangeSet, target: &impl ChangeTrackedRef) -> bool {
    if let Some(key) = target.object_key()
        && changes.object_hits.contains(&key)
    {
        return true;
    }

    changes
        .path_hits
        .iter()
        .any(|path| path_matches(&target.state_path(), path))
}

pub(crate) fn matches_fields(
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

fn path_matches(target: &StatePath, changed: &StatePath) -> bool {
    let target_segments = target.segments();
    let changed_segments = changed.segments();

    target_segments.len() <= changed_segments.len()
        && target_segments
            .iter()
            .zip(changed_segments.iter())
            .all(|(left, right)| left == right)
}

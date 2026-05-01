use std::collections::HashSet;
use std::{fmt, sync::Arc};

use crate::events::NormalizedMutation;
use crate::ids::{CommandId, CursorId, ProtocolDomain, Revision};

use super::{ObjectKey, StatePath};

pub(crate) trait CursorTracker: Send + Sync {
    fn update(&self, next_revision: Revision);
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChangeHit {
    pub path: StatePath,
    pub object: ObjectKey,
    pub field: String,
}

impl ChangeHit {
    pub fn field(path: StatePath, object: ObjectKey, field: impl Into<String>) -> Self {
        Self {
            path,
            object,
            field: field.into(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChangeSet {
    pub path_hits: Vec<StatePath>,
    pub object_hits: Vec<ObjectKey>,
    pub field_hits: Vec<ChangeHit>,
}

impl ChangeSet {
    pub fn from_mutations(mutations: &[NormalizedMutation]) -> Self {
        let mut path_seen = HashSet::with_capacity(mutations.len());
        let mut object_seen = HashSet::with_capacity(mutations.len());
        let mut field_seen = HashSet::with_capacity(mutations.len());

        let mut changes = Self::default();

        for mutation in mutations {
            if path_seen.insert(mutation.path.clone()) {
                changes.path_hits.push(mutation.path.clone());
            }

            if let Some(object) = &mutation.object {
                if object_seen.insert(object.clone()) {
                    changes.object_hits.push(object.clone());
                }

                for field in &mutation.fields {
                    let hit = ChangeHit::field(
                        mutation.path.clone(),
                        object.clone(),
                        field.field.clone(),
                    );
                    if field_seen.insert(hit.clone()) {
                        changes.field_hits.push(hit);
                    }
                }
            }
        }

        changes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitScope {
    InitialReady,
    RealtimeUpdate,
    ResyncRecovery,
    ReplayStep,
    QueryRefresh,
    SessionTransition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitResult {
    pub revision: Revision,
    pub domains: Vec<ProtocolDomain>,
    pub changes: ChangeSet,
    pub caused_by: Vec<CommandId>,
    pub scope: CommitScope,
}

impl CommitResult {
    pub fn new(
        revision: Revision,
        domains: Vec<ProtocolDomain>,
        changes: ChangeSet,
        caused_by: Vec<CommandId>,
        scope: CommitScope,
    ) -> Self {
        Self {
            revision,
            domains,
            changes,
            caused_by,
            scope,
        }
    }
}

pub type SharedCommitResult = Arc<CommitResult>;

pub struct UpdateCursor {
    id: CursorId,
    next_revision: Revision,
    tracker: Option<Arc<dyn CursorTracker>>,
}

impl UpdateCursor {
    pub fn new(id: CursorId, next_revision: Revision) -> Self {
        Self {
            id,
            next_revision,
            tracker: None,
        }
    }

    pub(crate) fn with_tracker(
        id: CursorId,
        next_revision: Revision,
        tracker: Arc<dyn CursorTracker>,
    ) -> Self {
        Self {
            id,
            next_revision,
            tracker: Some(tracker),
        }
    }

    pub fn id(&self) -> CursorId {
        self.id
    }

    pub fn next_revision(&self) -> Revision {
        self.next_revision
    }

    pub(crate) fn set_next_revision(&mut self, next_revision: Revision) {
        self.next_revision = next_revision;
        if let Some(tracker) = &self.tracker {
            tracker.update(next_revision);
        }
    }
}

impl Clone for UpdateCursor {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            next_revision: self.next_revision,
            tracker: self.tracker.clone(),
        }
    }
}

impl fmt::Debug for UpdateCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UpdateCursor")
            .field("id", &self.id)
            .field("next_revision", &self.next_revision)
            .finish()
    }
}

impl PartialEq for UpdateCursor {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.next_revision == other.next_revision
    }
}

impl Eq for UpdateCursor {}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use serde_json::json;

    use crate::{
        FieldMutation, MutationSource, NormalizedMutation,
        ids::{CursorId, Revision, Symbol},
    };

    use super::*;

    #[test]
    fn change_set_deduplicates_path_object_and_field_hits() {
        let path = StatePath::new(["quotes", "SHFE.au2606"]);
        let object = ObjectKey::Quote {
            symbol: Symbol::new("SHFE.au2606"),
        };
        let mutations = vec![
            mutation(path.clone(), object.clone(), "last_price", json!(610.0)),
            mutation(path.clone(), object.clone(), "last_price", json!(611.0)),
            mutation(path.clone(), object.clone(), "ask_price1", json!(611.2)),
        ];

        let changes = ChangeSet::from_mutations(&mutations);

        assert_eq!(changes.path_hits, vec![path.clone()]);
        assert_eq!(changes.object_hits, vec![object.clone()]);
        assert_eq!(
            changes.field_hits,
            vec![
                ChangeHit::field(path.clone(), object.clone(), "last_price"),
                ChangeHit::field(path, object, "ask_price1"),
            ]
        );
    }

    #[test]
    fn update_cursor_notifies_tracker_when_revision_advances() {
        let tracker = Arc::new(RecordingTracker::default());
        let mut cursor =
            UpdateCursor::with_tracker(CursorId::new(7), Revision::new(10), tracker.clone());

        cursor.set_next_revision(Revision::new(11));

        assert_eq!(cursor.id(), CursorId::new(7));
        assert_eq!(cursor.next_revision(), Revision::new(11));
        assert_eq!(tracker.revisions(), vec![Revision::new(11)]);
    }

    #[test]
    fn update_cursor_clone_keeps_identity_and_next_revision() {
        let cursor = UpdateCursor::new(CursorId::new(3), Revision::new(42));
        let cloned = cursor.clone();

        assert_eq!(cursor, cloned);
        assert_eq!(
            format!("{cloned:?}"),
            "UpdateCursor { id: CursorId(3), next_revision: Revision(42) }"
        );
    }

    fn mutation(
        path: StatePath,
        object: ObjectKey,
        field: &str,
        value: serde_json::Value,
    ) -> NormalizedMutation {
        NormalizedMutation {
            path,
            object: Some(object),
            fields: vec![FieldMutation {
                field: field.to_string(),
                value,
            }],
            source: MutationSource::MarketDiff,
        }
    }

    #[derive(Default)]
    struct RecordingTracker {
        revisions: Mutex<Vec<Revision>>,
    }

    impl RecordingTracker {
        fn revisions(&self) -> Vec<Revision> {
            self.revisions
                .lock()
                .expect("tracker lock poisoned")
                .clone()
        }
    }

    impl CursorTracker for RecordingTracker {
        fn update(&self, next_revision: Revision) {
            self.revisions
                .lock()
                .expect("tracker lock poisoned")
                .push(next_revision);
        }
    }
}

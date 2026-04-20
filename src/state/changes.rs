use crate::events::NormalizedMutation;
use crate::ids::{CommandId, CursorId, Revision};

use super::{ObjectKey, StatePath};

#[derive(Debug, Clone, PartialEq, Eq)]
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
        let mut changes = Self::default();

        for mutation in mutations {
            if !changes.path_hits.contains(&mutation.path) {
                changes.path_hits.push(mutation.path.clone());
            }

            if let Some(object) = &mutation.object {
                if !changes.object_hits.contains(object) {
                    changes.object_hits.push(object.clone());
                }

                for field in &mutation.fields {
                    let hit = ChangeHit::field(
                        mutation.path.clone(),
                        object.clone(),
                        field.field.clone(),
                    );
                    if !changes.field_hits.contains(&hit) {
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
    pub changes: ChangeSet,
    pub caused_by: Vec<CommandId>,
    pub scope: CommitScope,
}

impl CommitResult {
    pub fn new(
        revision: Revision,
        changes: ChangeSet,
        caused_by: Vec<CommandId>,
        scope: CommitScope,
    ) -> Self {
        Self {
            revision,
            changes,
            caused_by,
            scope,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateCursor {
    id: CursorId,
    next_revision: Revision,
}

impl UpdateCursor {
    pub fn new(id: CursorId, next_revision: Revision) -> Self {
        Self { id, next_revision }
    }

    pub fn id(&self) -> CursorId {
        self.id
    }

    pub fn next_revision(&self) -> Revision {
        self.next_revision
    }

    pub(crate) fn set_next_revision(&mut self, next_revision: Revision) {
        self.next_revision = next_revision;
    }
}

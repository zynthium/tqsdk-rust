use crate::ids::{
    AccountId, ChartId, CommandId, CursorId, NotificationId, OrderId, QueryId, ReplaySessionId, Revision, SchemaId,
    Symbol, TradeId,
};
use crate::events::NormalizedMutation;
use serde_json::{Map, Value};

pub type PathSegment = String;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StatePath(Vec<PathSegment>);

impl StatePath {
    pub fn new<I, S>(segments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self(segments.into_iter().map(Into::into).collect())
    }

    pub fn segments(&self) -> &[PathSegment] {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SeriesKey {
    pub primary: Symbol,
    pub secondary: Vec<Symbol>,
    pub duration_ns: i64,
    pub view_width: usize,
    pub right_id: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ObjectKey {
    Quote { symbol: Symbol },
    Kline { series: SeriesKey, bar_id: i64 },
    Tick { symbol: Symbol, tick_id: i64 },
    TradingStatus { symbol: Symbol },
    Chart { chart_id: ChartId },
    Account { account_id: AccountId },
    Position { account_id: AccountId, symbol: Symbol },
    Order { account_id: AccountId, order_id: OrderId },
    Trade { account_id: AccountId, trade_id: TradeId },
    Settlement { account_id: AccountId, trading_day: String },
    QueryResult { query_id: QueryId },
    SchemaNode { schema_id: SchemaId },
    ReplayCursor { session_id: ReplaySessionId },
    Notification { notification_id: NotificationId },
}

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
                    let hit = ChangeHit::field(mutation.path.clone(), object.clone(), field.field.clone());
                    if !changes.field_hits.contains(&hit) {
                        changes.field_hits.push(hit);
                    }
                }
            }
        }

        changes
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct StateSnapshot {
    revision: Revision,
    data: Value,
}

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
        let mut cursor = &self.data;
        for segment in path {
            let map = cursor.as_object()?;
            cursor = map.get(segment.as_ref())?;
        }
        Some(cursor)
    }

    pub(crate) fn apply(&mut self, revision: Revision, mutations: &[NormalizedMutation]) -> Vec<NormalizedMutation> {
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
    pub fn new(revision: Revision, changes: ChangeSet, caused_by: Vec<CommandId>, scope: CommitScope) -> Self {
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

use crate::ids::{AccountId, CommandId, CursorId, OrderId, QueryId, ReplaySessionId, Revision, SchemaId, Symbol, TradeId};

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
    Account { account_id: AccountId },
    Position { account_id: AccountId, symbol: Symbol },
    Order { account_id: AccountId, order_id: OrderId },
    Trade { account_id: AccountId, trade_id: TradeId },
    QueryResult { query_id: QueryId },
    SchemaNode { schema_id: SchemaId },
    ReplayCursor { session_id: ReplaySessionId },
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateSnapshot {
    revision: Revision,
}

impl StateSnapshot {
    pub fn new(revision: Revision) -> Self {
        Self { revision }
    }

    pub fn revision(&self) -> Revision {
        self.revision
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
}

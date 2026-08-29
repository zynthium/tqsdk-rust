use serde::Serialize;
use serde_json::{Value, json};

use crate::{
    ids::{ProtocolDomain, ReplaySessionId},
    state::{ObjectKey, StatePath},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeInput {
    Io(IoEvent),
    Timer(TimerEvent),
    Auth(AuthEvent),
    Replay(ReplayEvent),
    Internal(InternalEvent),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IoEvent {
    pub route: String,
    pub domains: Vec<ProtocolDomain>,
    pub payload: InputPayload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputPayload {
    Json(Value),
    Text(String),
    Binary(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerEvent {
    pub label: &'static str,
    pub payload: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthEvent {
    pub label: &'static str,
    pub payload: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayEvent {
    pub label: &'static str,
    pub session_id: Option<ReplaySessionId>,
    pub payload: Option<Value>,
}

/// A replay-owned historical-universe transition.
///
/// This is deliberately a neutral runtime payload: catalogue discovery, cache
/// planning and strategy policy remain in higher-level crates.  Converting it
/// to [`ReplayEvent`] ensures membership state uses the same replay commit and
/// revision path as any accompanying market input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayUniverseBatch {
    pub session_id: ReplaySessionId,
    pub effective_ns: i64,
    pub changes: Vec<ReplayUniverseChange>,
}

impl ReplayUniverseBatch {
    #[must_use]
    pub fn into_runtime_input(self) -> RuntimeInput {
        RuntimeInput::Replay(ReplayEvent {
            label: "universe",
            session_id: Some(self.session_id),
            payload: Some(json!({
                "universe": {
                    "effective_ns": self.effective_ns,
                    "changes": self.changes,
                }
            })),
        })
    }

    /// Build the normalized replay-state mutation used by local replay batches.
    #[must_use]
    pub fn into_normalized_mutation(self) -> NormalizedMutation {
        NormalizedMutation {
            path: StatePath::new(["replay", self.session_id.as_str(), "universe"]),
            object: None,
            fields: vec![
                FieldMutation {
                    field: "changes".to_string(),
                    value: json!(self.changes),
                },
                FieldMutation {
                    field: "effective_ns".to_string(),
                    value: json!(self.effective_ns),
                },
            ],
            source: MutationSource::ReplayStep,
        }
    }
}

/// One immutable change in a [`ReplayUniverseBatch`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReplayUniverseChange {
    pub instrument: String,
    pub active: bool,
    pub readiness: Option<String>,
    pub provenance: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalEvent {
    pub label: &'static str,
    pub payload: Option<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FieldMutation {
    pub field: String,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedMutation {
    pub path: StatePath,
    pub object: Option<ObjectKey>,
    pub fields: Vec<FieldMutation>,
    pub source: MutationSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationSource {
    MarketDiff,
    TradeReply,
    QueryResult,
    SchemaBootstrap,
    ReplayStep,
    SessionControl,
}

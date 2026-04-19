use serde_json::Value;

use crate::state::{ObjectKey, StatePath};

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
    pub label: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerEvent {
    pub label: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthEvent {
    pub label: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayEvent {
    pub label: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalEvent {
    pub label: &'static str,
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

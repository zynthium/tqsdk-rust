use serde_json::Value;

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

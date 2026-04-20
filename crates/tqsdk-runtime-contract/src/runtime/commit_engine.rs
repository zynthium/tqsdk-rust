use serde_json::{Value, json};

use crate::{
    events::{FieldMutation, MutationSource, NormalizedMutation},
    ids::{CommandId, ProtocolDomain, Revision},
    state::{ChangeSet, CommitResult, CommitScope, ObjectKey, StatePath, StateStore},
    transport::{BootstrapResult, SessionPhase, SessionRoute, SessionRouteEndpoint, SessionTarget},
};

pub(crate) struct CommitEngine;

impl CommitEngine {
    pub(crate) fn apply(
        snapshot: &mut StateStore,
        mutations: Vec<NormalizedMutation>,
        caused_by: Vec<CommandId>,
        scope: CommitScope,
    ) -> Option<CommitResult> {
        if mutations.is_empty() {
            return None;
        }

        let next_revision = Revision::new(snapshot.revision().get() + 1);
        let applied = snapshot.apply(next_revision, &mutations);
        if applied.is_empty() {
            return None;
        }

        let changes = ChangeSet::from_mutations(&applied);
        Some(CommitResult::new(next_revision, changes, caused_by, scope))
    }
}

pub(crate) fn session_snapshot_mutations(result: &BootstrapResult) -> Vec<NormalizedMutation> {
    vec![
        session_auth_mutation(result),
        session_lifecycle_mutation(result.phase, None),
        session_topology_mutation(result),
    ]
}

pub(crate) fn session_auth_mutation(result: &BootstrapResult) -> NormalizedMutation {
    let mut fields = vec![
        FieldMutation {
            field: "access_token_present".to_string(),
            value: json!(!result.auth.access_token().is_empty()),
        },
        FieldMutation {
            field: "auth_id".to_string(),
            value: result
                .auth
                .auth_id()
                .map(|auth_id| json!(auth_id.as_str()))
                .unwrap_or(Value::Null),
        },
        FieldMutation {
            field: "features".to_string(),
            value: json!(result.auth.features()),
        },
    ];
    sort_field_mutations(&mut fields);

    NormalizedMutation {
        path: StatePath::new(["system", "auth", "context"]),
        object: Some(ObjectKey::SessionAuth),
        fields,
        source: MutationSource::SessionControl,
    }
}

pub(crate) fn session_lifecycle_mutation(
    phase: SessionPhase,
    detail: Option<Value>,
) -> NormalizedMutation {
    let mut fields = vec![
        FieldMutation {
            field: "detail".to_string(),
            value: detail.unwrap_or(Value::Null),
        },
        FieldMutation {
            field: "phase".to_string(),
            value: json!(phase.as_str()),
        },
    ];
    sort_field_mutations(&mut fields);

    NormalizedMutation {
        path: StatePath::new(["system", "session", "lifecycle"]),
        object: Some(ObjectKey::SessionLifecycle),
        fields,
        source: MutationSource::SessionControl,
    }
}

pub(crate) fn session_topology_mutation(result: &BootstrapResult) -> NormalizedMutation {
    let mut fields = vec![
        FieldMutation {
            field: "enabled_domains".to_string(),
            value: json!(
                result
                    .enabled_domains
                    .iter()
                    .copied()
                    .map(ProtocolDomain::as_str)
                    .collect::<Vec<_>>()
            ),
        },
        FieldMutation {
            field: "routes".to_string(),
            value: Value::Array(
                result
                    .topology
                    .routes
                    .iter()
                    .map(normalize_session_route)
                    .collect(),
            ),
        },
    ];
    sort_field_mutations(&mut fields);

    NormalizedMutation {
        path: StatePath::new(["system", "session", "topology"]),
        object: Some(ObjectKey::SessionTopology),
        fields,
        source: MutationSource::SessionControl,
    }
}

pub(crate) fn normalize_session_route(route: &SessionRoute) -> Value {
    json!({
        "label": route.label,
        "target": normalize_session_target(&route.target),
        "domains": route.domains.iter().copied().map(ProtocolDomain::as_str).collect::<Vec<_>>(),
        "endpoint": normalize_session_endpoint(&route.endpoint),
    })
}

pub(crate) fn normalize_session_target(target: &SessionTarget) -> Value {
    match target {
        SessionTarget::Shared => json!({ "kind": "shared" }),
        SessionTarget::Account(account_id) => json!({
            "kind": "account",
            "account_id": account_id.as_str(),
        }),
        SessionTarget::Replay(session_id) => json!({
            "kind": "replay",
            "session_id": session_id.as_str(),
        }),
    }
}

pub(crate) fn normalize_session_endpoint(endpoint: &SessionRouteEndpoint) -> Value {
    match endpoint {
        SessionRouteEndpoint::WebSocket { url, .. } => json!({
            "kind": "websocket",
            "url": url,
        }),
        SessionRouteEndpoint::Http { url } => json!({
            "kind": "http",
            "url": url,
        }),
        SessionRouteEndpoint::Replay { label } => json!({
            "kind": "replay",
            "label": label,
        }),
        SessionRouteEndpoint::Internal { label } => json!({
            "kind": "internal",
            "label": label,
        }),
    }
}

pub(crate) fn sort_field_mutations(fields: &mut [FieldMutation]) {
    fields.sort_by(|left, right| left.field.cmp(&right.field));
}

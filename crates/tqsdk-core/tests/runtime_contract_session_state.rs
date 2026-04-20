use serde_json::json;
use tqsdk_core::{
    AccountId, AdapterRegistry, AuthContext, AuthId, BootstrapResult, CommitScope, ObjectKey,
    ProtocolDomain, Runtime, RuntimeHandle, SessionPhase, SessionRoute, SessionRouteEndpoint,
    SessionTarget, SessionTopology, StatePath,
};

#[test]
fn session_phase_transitions_commit_into_runtime_snapshot() {
    let handle = runtime_with_default_adapters();

    let commit = handle
        .record_session_phase(
            SessionPhase::Connecting,
            Some(json!({"route": "market"})),
            vec![],
        )
        .unwrap()
        .unwrap();

    assert_eq!(commit.revision.get(), 1);
    assert_eq!(commit.scope, CommitScope::SessionTransition);
    assert_eq!(
        commit.changes.path_hits,
        vec![StatePath::new(["system", "session", "lifecycle"])]
    );
    assert_eq!(
        commit.changes.object_hits,
        vec![ObjectKey::SessionLifecycle]
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "lifecycle", "phase"]),
        Some(&json!("connecting"))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "lifecycle", "detail"]),
        Some(&json!({"route": "market"}))
    );

    let repeated = handle
        .record_session_phase(
            SessionPhase::Connecting,
            Some(json!({"route": "market"})),
            vec![],
        )
        .unwrap();
    assert_eq!(repeated, None);

    let running = handle
        .record_session_phase(SessionPhase::Running, None, vec![])
        .unwrap()
        .unwrap();
    assert_eq!(running.revision.get(), 2);
    assert_eq!(running.scope, CommitScope::SessionTransition);
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "lifecycle", "phase"]),
        Some(&json!("running"))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "lifecycle", "detail"]),
        None
    );
}

#[test]
fn bootstrap_and_resync_results_share_session_commit_contract() {
    let handle = runtime_with_default_adapters();

    let bootstrap = BootstrapResult::new(
        AuthContext::new("access-token")
            .with_auth_id(AuthId::new("auth-1"))
            .with_feature("trade"),
        vec![ProtocolDomain::System, ProtocolDomain::Trade],
    )
    .with_topology(
        SessionTopology::default()
            .with_route(SessionRoute {
                label: "market".to_string(),
                target: SessionTarget::Shared,
                domains: vec![ProtocolDomain::System, ProtocolDomain::Market],
                endpoint: SessionRouteEndpoint::WebSocket {
                    url: "wss://market.example".to_string(),
                    connect: Default::default(),
                },
            })
            .with_route(SessionRoute {
                label: "trade:simnow".to_string(),
                target: SessionTarget::Account(AccountId::new("simnow")),
                domains: vec![ProtocolDomain::Trade],
                endpoint: SessionRouteEndpoint::Internal {
                    label: "trade-router".to_string(),
                },
            }),
    );

    let initial_ready = handle
        .record_session_bootstrap(&bootstrap, vec![])
        .unwrap()
        .unwrap();
    assert_eq!(initial_ready.revision.get(), 1);
    assert_eq!(initial_ready.scope, CommitScope::InitialReady);
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "auth", "context", "auth_id"]),
        Some(&json!("auth-1"))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "auth", "context", "features"]),
        Some(&json!(["trade"]))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "auth", "context", "access_token_present"]),
        Some(&json!(true))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "lifecycle", "phase"]),
        Some(&json!("running"))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "topology", "enabled_domains"]),
        Some(&json!(["system", "trade"]))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "topology", "routes"]),
        Some(&json!([
            {
                "label": "market",
                "target": {"kind": "shared"},
                "domains": ["system", "market"],
                "endpoint": {"kind": "websocket", "url": "wss://market.example"},
            },
            {
                "label": "trade:simnow",
                "target": {"kind": "account", "account_id": "simnow"},
                "domains": ["trade"],
                "endpoint": {"kind": "internal", "label": "trade-router"},
            }
        ]))
    );

    let repeated = handle.record_session_bootstrap(&bootstrap, vec![]).unwrap();
    assert_eq!(repeated, None);

    let resync = BootstrapResult {
        phase: SessionPhase::Running,
        auth: AuthContext::new("access-token")
            .with_auth_id(AuthId::new("auth-1"))
            .with_feature("trade")
            .with_feature("query"),
        enabled_domains: vec![
            ProtocolDomain::System,
            ProtocolDomain::Trade,
            ProtocolDomain::Query,
        ],
        topology: SessionTopology::default().with_route(SessionRoute {
            label: "trade:simnow".to_string(),
            target: SessionTarget::Account(AccountId::new("simnow")),
            domains: vec![ProtocolDomain::Trade, ProtocolDomain::Query],
            endpoint: SessionRouteEndpoint::Http {
                url: "https://query.example/graphql".to_string(),
            },
        }),
    };

    let recovery = handle
        .record_session_resync(&resync, vec![])
        .unwrap()
        .unwrap();
    assert_eq!(recovery.revision.get(), 2);
    assert_eq!(recovery.scope, CommitScope::ResyncRecovery);
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "auth", "context", "features"]),
        Some(&json!(["trade", "query"]))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "topology", "enabled_domains"]),
        Some(&json!(["system", "trade", "query"]))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "topology", "routes"]),
        Some(&json!([{
            "label": "trade:simnow",
            "target": {"kind": "account", "account_id": "simnow"},
            "domains": ["trade", "query"],
            "endpoint": {"kind": "http", "url": "https://query.example/graphql"},
        }]))
    );
}

fn runtime_with_default_adapters() -> RuntimeHandle {
    let mut registry = AdapterRegistry::new();
    registry.register_default_adapters();
    RuntimeHandle::with_adapters(registry)
}

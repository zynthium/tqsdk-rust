use tqsdk_session::{SessionClientBuilder, SessionFacadeConfig, SessionFacadeError};

#[test]
fn builder_keeps_explicit_facade_config() {
    let config = SessionFacadeConfig::default().with_default_view_width(256);

    let builder = SessionClientBuilder::new("user", "pass").facade_config(config);

    assert_eq!(builder.facade_config_ref().default_view_width, 256);
}

#[test]
fn builder_accepts_explicit_query_schema_and_replay_urls() {
    let builder = SessionClientBuilder::new("user", "pass")
        .query_url("https://query.example.com/graphql")
        .schema_url("https://schema.example.com/latest.json")
        .replay_url("wss://replay.example.com/feed");

    let endpoints = builder.endpoints();

    assert_eq!(
        endpoints.query_url.as_deref(),
        Some("https://query.example.com/graphql")
    );
    assert_eq!(
        endpoints.schema_url.as_deref(),
        Some("https://schema.example.com/latest.json")
    );
    assert_eq!(
        endpoints.replay_url.as_deref(),
        Some("wss://replay.example.com/feed")
    );
}

#[test]
fn facade_config_clamps_zero_view_width_to_one() {
    let config = SessionFacadeConfig::default().with_default_view_width(0);

    assert_eq!(config.default_view_width, 1);
}

#[test]
fn facade_error_converts_core_errors_and_formats_messages() {
    let error =
        SessionFacadeError::from(tqsdk_core::ContractError::validation("bad session state"));

    assert_eq!(error.to_string(), "validation error: bad session state");
    assert!(std::error::Error::source(&error).is_some());

    let invalid_state = SessionFacadeError::InvalidState("missing session config");
    assert_eq!(
        invalid_state.to_string(),
        "invalid session facade state: missing session config"
    );
    assert!(std::error::Error::source(&invalid_state).is_none());
}

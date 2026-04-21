use tqsdk_session::{SessionClientBuilder, SessionFacadeConfig};

#[test]
fn builder_keeps_explicit_facade_config() {
    let config = SessionFacadeConfig::default().with_default_view_width(256);

    let builder = SessionClientBuilder::new("user", "pass").facade_config(config.clone());

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

use tqsdk_core::{AuthContext, AuthId};

#[test]
fn auth_context_constructor_and_accessors_are_the_public_contract() {
    let auth = AuthContext::new("access-token")
        .with_auth_id(AuthId::new("auth-1"))
        .with_feature("trade")
        .with_feature("query");

    assert_eq!(auth.access_token(), "access-token");
    assert_eq!(auth.auth_id().map(AuthId::as_str), Some("auth-1"));
    assert_eq!(auth.features(), &["trade".to_string(), "query".to_string()]);

    let debug = format!("{auth:?}");
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains("access-token"));
}

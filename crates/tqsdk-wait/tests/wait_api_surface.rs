use std::time::Duration;

use tqsdk_session::SessionFacadeError;
use tqsdk_wait::WaitFacadeError;

mod support;

#[tokio::test]
async fn wait_update_returns_deferred_commit_before_polling() {
    let mut api = support::seeded_api();
    support::seed_quote_commit(&mut api, "SHFE.au2602", 618.5);

    assert!(api.wait_update(None).await.unwrap());
    assert!(api.last_commit().is_some());
}

#[test]
fn concurrent_wait_update_is_rejected() {
    let api = support::seeded_api();

    let _guard = api.begin_wait_for_test().unwrap();

    assert_eq!(
        api.begin_wait_for_test(),
        Err(WaitFacadeError::ConcurrentWaitUpdate)
    );
}

#[test]
fn wait_driver_keeps_single_runtime_reader_without_snapshot_cache() {
    let driver = include_str!("../src/driver.rs");

    assert!(driver.contains("pub(crate) reader: tqsdk_core::RuntimeReader"));
    assert!(driver.contains("pub(crate) cursor: tqsdk_core::UpdateCursor"));
    assert!(!driver.contains("StateSnapshot"));
    assert!(!driver.contains("StateStore"));
}

#[tokio::test]
async fn wait_update_timeout_returns_false() {
    let mut api = support::seeded_api();

    let ready = api
        .wait_update(Some(
            tokio::time::Instant::now() + Duration::from_millis(10),
        ))
        .await
        .unwrap();

    assert!(!ready);
}

#[tokio::test]
async fn wait_api_exposes_underlying_session_for_direct_queries() {
    let api = support::seeded_api();

    let err = api
        .session()
        .query_graphql_value("query { __typename }", None)
        .await
        .unwrap_err();

    assert_eq!(
        err,
        SessionFacadeError::InvalidState("query value helper requires an enabled query route")
    );
}

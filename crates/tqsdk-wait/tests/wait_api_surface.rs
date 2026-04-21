use std::time::Duration;

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

use std::time::Duration;

use futures::StreamExt;
use tqsdk_session::SessionFacadeError;
use tqsdk_stream::StreamFacadeError;

mod support;

#[tokio::test(flavor = "current_thread")]
async fn stream_exposes_underlying_session_for_direct_queries() {
    let stream = support::core_seed::seeded_stream();

    let err = stream
        .session()
        .query_graphql_value("query { __typename }", None)
        .await
        .unwrap_err();

    assert_eq!(
        err,
        SessionFacadeError::InvalidState("query value helper requires an enabled query route")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn commit_stream_stays_idle_until_new_commit_arrives() {
    let stream = support::core_seed::seeded_stream();
    let mut commits = stream.commit_stream().unwrap();

    let next = tokio::time::timeout(
        Duration::from_millis(10),
        futures::StreamExt::next(&mut commits),
    )
    .await;

    assert!(next.is_err());
}

#[test]
fn commit_stream_can_retry_after_missing_runtime_error() {
    let stream = support::core_seed::seeded_stream();

    let err = match stream.commit_stream() {
        Ok(_) => panic!("commit_stream should fail without an active Tokio runtime"),
        Err(err) => err,
    };
    assert_eq!(
        err,
        StreamFacadeError::InvalidState("commit_stream requires an active Tokio runtime")
    );

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut commits = stream.commit_stream().unwrap();

        support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 621.0);

        let commit = commits
            .next()
            .await
            .expect("commit stream should yield a new item after retry")
            .expect("retry inside runtime should start the driver");
        assert!(commit.revision.get() > 0);
    });
}

#[test]
fn stream_facade_error_display_covers_lagged_and_closed_cases() {
    assert_eq!(
        StreamFacadeError::Lagged { skipped: 3 }.to_string(),
        "stream receiver lagged and skipped 3 commit(s)"
    );
    assert_eq!(
        StreamFacadeError::Closed.to_string(),
        "stream driver closed"
    );
}

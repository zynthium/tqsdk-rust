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

#[tokio::test(flavor = "current_thread")]
async fn commit_stream_reports_closed_when_stream_facade_drops() {
    let stream = support::core_seed::seeded_stream();
    let mut commits = stream.commit_stream().unwrap();

    drop(stream);

    let update = tokio::time::timeout(Duration::from_millis(50), commits.next())
        .await
        .expect("commit stream should observe a close after stream facade drop")
        .expect("commit stream should yield a close item");

    assert!(matches!(update, Err(StreamFacadeError::Closed)));
}

#[tokio::test(flavor = "current_thread")]
async fn into_session_closes_existing_commit_receivers() {
    let stream = support::core_seed::seeded_stream();
    let mut commits = stream.commit_stream().unwrap();
    let session = stream.into_session();

    let err = session
        .query_graphql_value("query { __typename }", None)
        .await
        .unwrap_err();
    assert_eq!(
        err,
        SessionFacadeError::InvalidState("query value helper requires an enabled query route")
    );

    let update = tokio::time::timeout(Duration::from_millis(50), commits.next())
        .await
        .expect("commit stream should observe a close after into_session")
        .expect("commit stream should yield a close item");

    assert!(matches!(update, Err(StreamFacadeError::Closed)));
}

#[tokio::test(flavor = "current_thread")]
async fn commit_stream_returns_closed_after_driver_was_explicitly_closed() {
    let stream = support::core_seed::seeded_stream();
    let mut commits = stream.commit_stream().unwrap();

    stream.close_driver_for_test();

    let update = tokio::time::timeout(Duration::from_millis(50), commits.next())
        .await
        .expect("commit stream should observe a close after driver shutdown")
        .expect("commit stream should yield a close item");
    assert!(matches!(update, Err(StreamFacadeError::Closed)));

    let err = match stream.commit_stream() {
        Ok(_) => panic!("commit_stream should stay closed after driver shutdown"),
        Err(err) => err,
    };
    assert_eq!(err, StreamFacadeError::Closed);
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

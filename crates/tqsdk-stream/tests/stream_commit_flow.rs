use futures::StreamExt;
use tqsdk_core::Quote;
use tqsdk_stream::StreamFacadeError;

mod support;

#[tokio::test(flavor = "current_thread")]
async fn commit_stream_emits_commit_and_reader_sees_matching_state() {
    let stream = support::core_seed::seeded_stream();
    let mut commits = stream.commit_stream().unwrap();

    support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 618.5);

    let commit = commits
        .next()
        .await
        .expect("commit stream should yield an item")
        .expect("first item should be a commit");

    assert!(commit.changes.path_hits.iter().any(|path| {
        matches!(
            path.segments(),
            [first, second] if first == "quotes" && second == "SHFE.au2602"
        )
    }));

    let snapshot = stream.reader().read();
    let quote = snapshot
        .decode_path::<Quote>(&["quotes", "SHFE.au2602"])
        .unwrap()
        .expect("quote snapshot should be readable");
    assert_eq!(quote.last_price, 618.5);
    assert_eq!(snapshot.revision(), commit.revision);
}

#[tokio::test(flavor = "current_thread")]
async fn multiple_commit_receivers_observe_same_revision() {
    let stream = support::core_seed::seeded_stream();
    let mut first = stream.commit_stream().unwrap();
    let mut second = stream.commit_stream().unwrap();

    support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 619.0);

    let first_commit = first
        .next()
        .await
        .expect("first receiver should yield an item")
        .expect("first receiver should observe a commit");
    let second_commit = second
        .next()
        .await
        .expect("second receiver should yield an item")
        .expect("second receiver should observe a commit");

    assert_eq!(first_commit.revision, second_commit.revision);
}

#[tokio::test(flavor = "current_thread")]
async fn lagged_receiver_reports_backpressure_explicitly() {
    let stream = support::core_seed::seeded_stream_with_capacity(1);
    let mut commits = stream.commit_stream().unwrap();

    support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 618.0);
    support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 619.0);
    support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 620.0);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let update = commits
        .next()
        .await
        .expect("commit stream should yield lag information");

    assert!(matches!(
        update,
        Err(StreamFacadeError::Lagged { skipped }) if skipped >= 1
    ));
}

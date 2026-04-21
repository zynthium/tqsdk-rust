use futures::StreamExt;
use tqsdk_core::Quote;

mod support;

#[tokio::test(flavor = "current_thread")]
async fn quote_stream_decodes_matching_quote_and_skips_other_symbols() {
    let stream = support::core_seed::seeded_stream();
    let mut quotes = stream.quote_stream("SHFE.au2602").unwrap();

    support::core_seed::seed_quote_commit(&stream, "SHFE.ag2606", 5103.0);
    support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 625.0);

    let update = quotes
        .next()
        .await
        .expect("quote stream should yield a matching update")
        .expect("quote stream should decode the matching quote");

    assert_eq!(update.value.instrument_id, "SHFE.au2602");
    assert_eq!(update.value.last_price, 625.0);
    assert_eq!(update.commit.revision, stream.reader().read().revision());
}

#[tokio::test(flavor = "current_thread")]
async fn path_stream_decodes_typed_value_for_selected_path() {
    let stream = support::core_seed::seeded_stream();
    let mut quotes = stream
        .path_stream::<Quote, _, _>(["quotes", "SHFE.au2602"])
        .unwrap();

    support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 626.0);

    let update = quotes
        .next()
        .await
        .expect("path stream should yield a matching update")
        .expect("path stream should decode the requested value");

    assert_eq!(update.value.instrument_id, "SHFE.au2602");
    assert_eq!(update.value.last_price, 626.0);
    assert_eq!(update.commit.revision, stream.reader().read().revision());
}

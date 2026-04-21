use futures::StreamExt;
use serde_json::json;
use tqsdk_core::{CommitScope, ObjectKey, ProtocolDomain, Quote, Symbol};

mod support;

#[tokio::test(flavor = "current_thread")]
async fn path_commit_stream_skips_unmatched_commits_and_matches_prefix() {
    let stream = support::core_seed::seeded_stream();
    let mut commits = stream
        .commit_stream()
        .unwrap()
        .filter_path(["quotes", "SHFE.au2602"]);

    support::core_seed::seed_quote_commit(&stream, "SHFE.ag2606", 5101.0);
    support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 620.5);

    let commit = commits
        .next()
        .await
        .expect("path-filtered stream should yield a matching commit")
        .expect("matching commit should arrive without filter errors");

    let snapshot = stream.reader().read();
    let quote = snapshot
        .decode_path::<Quote>(&["quotes", "SHFE.au2602"])
        .unwrap()
        .expect("filtered symbol quote should be readable");

    assert_eq!(commit.revision, snapshot.revision());
    assert_eq!(quote.last_price, 620.5);
}

#[tokio::test(flavor = "current_thread")]
async fn scope_commit_stream_skips_other_scopes() {
    let stream = support::core_seed::seeded_stream();
    let mut commits = stream
        .commit_stream()
        .unwrap()
        .filter_scope(CommitScope::QueryRefresh);

    support::core_seed::seed_quote_commit_with_scope(
        &stream,
        "SHFE.au2602",
        621.0,
        CommitScope::RealtimeUpdate,
    );
    support::core_seed::seed_quote_commit_with_scope(
        &stream,
        "SHFE.au2602",
        622.0,
        CommitScope::QueryRefresh,
    );

    let commit = commits
        .next()
        .await
        .expect("scope-filtered stream should yield a matching commit")
        .expect("matching scope should arrive without filter errors");

    let snapshot = stream.reader().read();
    let quote = snapshot
        .decode_path::<Quote>(&["quotes", "SHFE.au2602"])
        .unwrap()
        .expect("quote snapshot should be readable after scope match");

    assert_eq!(commit.scope, CommitScope::QueryRefresh);
    assert_eq!(commit.revision, snapshot.revision());
    assert_eq!(quote.last_price, 622.0);
}

#[tokio::test(flavor = "current_thread")]
async fn object_commit_stream_matches_object_hits() {
    let stream = support::core_seed::seeded_stream();
    let mut commits = stream
        .commit_stream()
        .unwrap()
        .filter_object(ObjectKey::Quote {
            symbol: Symbol::new("SHFE.au2602"),
        });

    support::core_seed::seed_quote_commit(&stream, "SHFE.ag2606", 5102.0);
    support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 623.0);

    let commit = commits
        .next()
        .await
        .expect("object-filtered stream should yield a matching commit")
        .expect("matching object should arrive without filter errors");

    let snapshot = stream.reader().read();
    let quote = snapshot
        .decode_path::<Quote>(&["quotes", "SHFE.au2602"])
        .unwrap()
        .expect("object-filtered symbol quote should be readable");

    assert_eq!(commit.revision, snapshot.revision());
    assert_eq!(quote.last_price, 623.0);
}

#[tokio::test(flavor = "current_thread")]
async fn domain_commit_stream_matches_commit_domains() {
    let stream = support::core_seed::seeded_stream();
    let mut commits = stream
        .commit_stream()
        .unwrap()
        .filter_domain(ProtocolDomain::System);

    support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 623.5);
    support::core_seed::seed_quote_fields_commit_on_domains_with_scope(
        &stream,
        "SHFE.au2602",
        json!({
            "instrument_id": "SHFE.au2602",
            "last_price": 624.0
        }),
        vec![ProtocolDomain::Market, ProtocolDomain::System],
        CommitScope::RealtimeUpdate,
    );

    let commit = commits
        .next()
        .await
        .expect("domain-filtered stream should yield a matching commit")
        .expect("matching domain should arrive without filter errors");

    let snapshot = stream.reader().read();
    let quote = snapshot
        .decode_path::<Quote>(&["quotes", "SHFE.au2602"])
        .unwrap()
        .expect("domain-filtered quote should be readable");

    assert_eq!(commit.revision, snapshot.revision());
    assert_eq!(
        commit.domains,
        vec![ProtocolDomain::Market, ProtocolDomain::System]
    );
    assert_eq!(quote.last_price, 624.0);
}

#[tokio::test(flavor = "current_thread")]
async fn field_commit_stream_skips_unmatched_fields() {
    let stream = support::core_seed::seeded_stream();
    let target = ObjectKey::Quote {
        symbol: Symbol::new("SHFE.au2602"),
    };
    let mut commits = stream
        .commit_stream()
        .unwrap()
        .filter_fields(target, ["last_price"]);

    support::core_seed::seed_quote_fields_commit_with_scope(
        &stream,
        "SHFE.au2602",
        json!({
            "instrument_id": "SHFE.au2602"
        }),
        CommitScope::RealtimeUpdate,
    );
    support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 624.0);

    let commit = commits
        .next()
        .await
        .expect("field-filtered stream should yield a matching commit")
        .expect("matching field should arrive without filter errors");

    let snapshot = stream.reader().read();
    let quote = snapshot
        .decode_path::<Quote>(&["quotes", "SHFE.au2602"])
        .unwrap()
        .expect("field-filtered quote should be readable");

    assert_eq!(commit.revision, snapshot.revision());
    assert_eq!(quote.last_price, 624.0);
}

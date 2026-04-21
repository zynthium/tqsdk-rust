mod support;

#[tokio::test(flavor = "current_thread")]
async fn quote_change_is_visible_after_wait_update() {
    let mut api = support::seeded_api();
    let quote = api.quote_ref("SHFE.au2602");
    support::seed_quote_commit(&mut api, "SHFE.au2602", 619.0);

    assert!(api.wait_update(None).await.unwrap());
    assert!(api.is_changing(&quote).unwrap());
    assert!(api.is_changing_fields(&quote, &["last_price"]).unwrap());
    assert!(!api.is_changing_fields(&quote, &["ask_price1"]).unwrap());
}

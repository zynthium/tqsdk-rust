use std::collections::BTreeMap;

use tqsdk_data::{
    HistoricalAcquisitionContract, HistoricalCatalogAcquisition, HistoricalCatalogProof,
    HistoricalSemanticCatalog, PROVIDER_DAILY_HISTORY_BOOTSTRAP_START_NS,
    promote_provider_daily_history,
};

fn current() -> HistoricalCatalogAcquisition {
    let origin = PROVIDER_DAILY_HISTORY_BOOTSTRAP_START_NS;
    let contract = HistoricalAcquisitionContract {
        symbol: "CZCE.CF001".to_string(),
        exchange_id: "CZCE".to_string(),
        product_id: "CF".to_string(),
        expired: true,
        expire_datetime_ns: None,
        authoritative_lifecycle: Vec::new(),
        first_available_data_ns: BTreeMap::new(),
    };
    HistoricalCatalogAcquisition::new(
        HistoricalCatalogProof::ProviderCurrentObserved,
        "provider-current:v1",
        "physical:all",
        origin + 600,
        origin + 700,
        true,
        vec![contract.symbol.clone()],
        vec![contract.symbol.clone()],
        vec![contract],
    )
    .unwrap()
}

#[test]
fn empty_expired_contract_does_not_require_expiry_but_nonempty_one_does() {
    let origin = PROVIDER_DAILY_HISTORY_BOOTSTRAP_START_NS;
    let empty = promote_provider_daily_history(
        current(),
        &BTreeMap::from([("CZCE.CF001".to_string(), None)]),
    )
    .unwrap();
    let semantic =
        HistoricalSemanticCatalog::from_provider_history_observed(&empty, "calendar:test-v1")
            .unwrap();
    assert!(semantic.catalog.contracts.is_empty());

    let nonempty = promote_provider_daily_history(
        current(),
        &BTreeMap::from([("CZCE.CF001".to_string(), Some(origin + 100))]),
    )
    .unwrap();
    assert!(
        HistoricalSemanticCatalog::from_provider_history_observed(&nonempty, "calendar:test-v1")
            .unwrap_err()
            .to_string()
            .contains("lacks expiry metadata")
    );
}

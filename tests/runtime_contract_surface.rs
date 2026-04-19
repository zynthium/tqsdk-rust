use tqsdk_runtime_contract::{ContractError, ProtocolDomain, Revision, Symbol};

#[test]
fn ids_and_domain_surface_are_stable() {
    let revision = Revision::new(7);
    let symbol = Symbol::new("SHFE.au2602");

    assert_eq!(revision.get(), 7);
    assert_eq!(symbol.as_str(), "SHFE.au2602");
    assert_eq!(ProtocolDomain::Trade.as_str(), "trade");
    assert_eq!(
        ContractError::validation("bad command").to_string(),
        "validation error: bad command"
    );
}

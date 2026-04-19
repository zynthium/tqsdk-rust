use tqsdk_runtime_contract::{
    AccountId, CausationMeta, CommandEnvelope, CommandId, CommandStatus, ContractError, MarketCommand,
    OutboundRequest, ProtocolDomain, QueryCommand, QueryId, ReplayCommand, Revision, RuntimeCommand,
    SchemaCommand, SchemaId, Symbol, SystemCommand, TradeCommand,
};

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

#[test]
fn runtime_commands_route_to_expected_domains() {
    let market = RuntimeCommand::Market(MarketCommand::SubscribeQuotes {
        symbols: vec![Symbol::new("SHFE.au2602")],
    });
    let trade = RuntimeCommand::Trade(TradeCommand::InsertOrder {
        account_id: AccountId::new("sim"),
        symbol: Symbol::new("SHFE.au2602"),
        volume: 2,
    });
    let query = RuntimeCommand::Query(QueryCommand::Fetch {
        query_id: QueryId::new("quotes-page-1"),
        path: "/graphql/quotes".to_string(),
    });
    let schema = RuntimeCommand::Schema(SchemaCommand::Refresh {
        schema_id: SchemaId::new("instrument-schema"),
    });
    let replay = RuntimeCommand::Replay(ReplayCommand::Step);
    let system = RuntimeCommand::System(SystemCommand::Shutdown);

    assert_eq!(market.domain(), ProtocolDomain::Market);
    assert_eq!(trade.domain(), ProtocolDomain::Trade);
    assert_eq!(query.domain(), ProtocolDomain::Query);
    assert_eq!(schema.domain(), ProtocolDomain::Schema);
    assert_eq!(replay.domain(), ProtocolDomain::Replay);
    assert_eq!(system.domain(), ProtocolDomain::System);

    let envelope = CommandEnvelope {
        id: CommandId::new(9),
        command: market,
        causation: CausationMeta::default(),
    };

    assert_eq!(envelope.id.get(), 9);
    assert_eq!(CommandStatus::Queued.as_str(), "queued");
    assert!(matches!(
        OutboundRequest::internal_label("flush-peek"),
        OutboundRequest::Internal(_)
    ));
}

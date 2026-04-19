use serde_json::json;
use tqsdk_runtime_contract::{
    AccountId, CausationMeta, ChangeHit, ChangeSet, CommandEnvelope, CommandId, CommandStatus, CommitResult,
    CommitScope, ContractError, CursorId, FieldMutation, MarketCommand, MutationSource, NormalizedMutation,
    ObjectKey, OutboundRequest, ProtocolDomain, QueryCommand, QueryId, ReplayCommand, Revision, RuntimeCommand,
    SchemaCommand, SchemaId, SeriesKey, StatePath, StateSnapshot, Symbol, SystemCommand, TradeCommand,
    UpdateCursor,
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

#[test]
fn snapshot_cursor_and_mutation_types_are_revision_bound() {
    let path = StatePath::new(["market", "quotes", "SHFE.au2602"]);
    let quote_key = ObjectKey::Quote {
        symbol: Symbol::new("SHFE.au2602"),
    };
    let mutation = NormalizedMutation {
        path: path.clone(),
        object: Some(quote_key.clone()),
        fields: vec![FieldMutation {
            field: "last_price".to_string(),
            value: json!(618.5),
        }],
        source: MutationSource::MarketDiff,
    };

    let snapshot = StateSnapshot::new(Revision::new(3));
    let cursor = UpdateCursor::new(CursorId::new(1), Revision::new(4));
    let changes = ChangeSet {
        path_hits: vec![path.clone()],
        object_hits: vec![quote_key.clone()],
        field_hits: vec![ChangeHit::field(path.clone(), quote_key.clone(), "last_price")],
    };
    let commit = CommitResult::new(Revision::new(4), changes.clone(), vec![], CommitScope::RealtimeUpdate);

    assert_eq!(snapshot.revision().get(), 3);
    assert_eq!(cursor.next_revision().get(), 4);
    assert_eq!(mutation.fields.len(), 1);
    assert_eq!(changes.object_hits.len(), 1);
    assert_eq!(commit.revision.get(), 4);

    let series = SeriesKey {
        primary: Symbol::new("SHFE.au2602"),
        secondary: vec![Symbol::new("SHFE.au2604")],
        duration_ns: 60_000_000_000,
        view_width: 128,
        right_id: Some(42),
    };

    assert_eq!(series.view_width, 128);
}

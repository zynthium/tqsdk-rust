use serde_json::json;
use tqsdk_core::{
    AccountId, AdapterRegistry, CausationMeta, ChangeHit, ChangeSet, CommandEnvelope, CommandId,
    CommandStatus, CommitLog, CommitReadGuard, CommitResult, CommitScope, ContractError, CursorId,
    CursorLagged, FieldMutation, MarketChartCommand, MarketCommand, MutationSource,
    NormalizedMutation, ObjectKey, OrderId, OutboundRequest, ProtocolAdapter, ProtocolDomain,
    QueryCommand, QueryId, Quote, ReplayCommand, Result, Revision, Runtime, RuntimeCommand,
    RuntimeHandle, RuntimeInput, RuntimeReader, SchemaCommand, SchemaId, SeriesKey,
    SnapshotReadGuard, StatePath, StateReadView, StateSnapshot, Symbol, SystemCommand,
    TradeCommand, TradeDirection, TradeInsertOrderCommand, TradeOffset, TradePreInsertOrderCommand,
    TradePriceType, TradeTimeCondition, TradeVolumeCondition, UpdateCursor,
};

struct TestAdapter;

impl ProtocolAdapter for TestAdapter {
    fn domain(&self) -> ProtocolDomain {
        ProtocolDomain::System
    }

    fn accepts_command(&self, cmd: &RuntimeCommand) -> bool {
        matches!(cmd, RuntimeCommand::System(_))
    }

    fn encode(&mut self, _cmd: &RuntimeCommand) -> Result<Vec<tqsdk_core::OutboundRequest>> {
        Err(ContractError::UnsupportedCommand("system skeleton"))
    }

    fn accepts_input(&self, input: &RuntimeInput) -> bool {
        matches!(input, RuntimeInput::Internal(_))
    }

    fn decode(&mut self, _input: &RuntimeInput) -> Result<Vec<tqsdk_core::NormalizedMutation>> {
        Ok(vec![])
    }
}

#[test]
fn ids_and_domain_surface_are_stable() {
    let revision = Revision::new(7);
    let symbol = Symbol::new("SHFE.au2602");
    let account_id = AccountId::new("simnow");
    let order_id = OrderId::new("order-1");

    assert_eq!(revision.get(), 7);
    assert_eq!(symbol.as_str(), "SHFE.au2602");
    assert_eq!(symbol.to_string(), "SHFE.au2602");
    assert_eq!(account_id.to_string(), "simnow");
    assert_eq!(order_id.to_string(), "order-1");
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
    let trade = RuntimeCommand::Trade(TradeCommand::InsertOrder(TradeInsertOrderCommand {
        account_id: AccountId::new("sim"),
        order_id: OrderId::new("order-1"),
        symbol: Symbol::new("SHFE.au2602"),
        direction: TradeDirection::Buy,
        offset: Some(TradeOffset::Open),
        volume: 2,
        price_type: TradePriceType::Limit,
        limit_price: Some(json!(618.5)),
        time_condition: TradeTimeCondition::Gfd,
        volume_condition: TradeVolumeCondition::Any,
    }));
    let query = RuntimeCommand::Query(QueryCommand::Fetch {
        query_id: QueryId::new("quotes-page-1"),
        query: "query Quotes { symbols { instrument_id } }".to_string(),
        variables: None,
    });
    let trade_pre_insert =
        RuntimeCommand::Trade(TradeCommand::PreInsertOrder(TradePreInsertOrderCommand {
            account_id: AccountId::new("sim"),
            order_id: OrderId::new("pre-1"),
            symbol: Symbol::new("SHFE.au2602"),
            direction: TradeDirection::Buy,
            offset: Some(TradeOffset::Open),
            volume: 1,
            price_type: TradePriceType::Limit,
            limit_price: Some(json!(0.0)),
            time_condition: TradeTimeCondition::Gfd,
            volume_condition: TradeVolumeCondition::Any,
            hedge_flag: "SPECULATION".to_string(),
            contingent_condition: "IMMEDIATELY".to_string(),
        }));
    let schema = RuntimeCommand::Schema(SchemaCommand::Refresh {
        schema_id: SchemaId::new("instrument-schema"),
        path: "/t/symbols/latest.json".to_string(),
    });
    let replay = RuntimeCommand::Replay(ReplayCommand::Step);
    let system = RuntimeCommand::System(SystemCommand::Shutdown);

    assert_eq!(market.domain(), ProtocolDomain::Market);
    assert_eq!(trade.domain(), ProtocolDomain::Trade);
    assert_eq!(trade_pre_insert.domain(), ProtocolDomain::Trade);
    assert_eq!(query.domain(), ProtocolDomain::Query);
    assert_eq!(schema.domain(), ProtocolDomain::Schema);
    assert_eq!(replay.domain(), ProtocolDomain::Replay);
    assert_eq!(system.domain(), ProtocolDomain::System);
    assert!(matches!(
        RuntimeCommand::Market(MarketCommand::SetChart(MarketChartCommand {
            chart_id: "chart-1".to_string(),
            symbols: vec![Symbol::new("SHFE.au2602")],
            duration_ns: 60_000_000_000,
            view_width: 128,
            left_kline_id: Some(0),
            focus_datetime_ns: None,
            focus_position: None,
        }))
        .domain(),
        ProtocolDomain::Market
    ));

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
        field_hits: vec![ChangeHit::field(
            path.clone(),
            quote_key.clone(),
            "last_price",
        )],
    };
    let commit = CommitResult::new(
        Revision::new(4),
        vec![ProtocolDomain::Market],
        changes.clone(),
        vec![],
        CommitScope::RealtimeUpdate,
    );

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

#[test]
fn reader_surface_is_primary_and_compatibility_surface_remains_available() {
    let mut registry = AdapterRegistry::new();
    registry.register_domain(ProtocolDomain::System);

    let handle = RuntimeHandle::new();
    let default_handle = RuntimeHandle::default();
    let reader: RuntimeReader = handle.reader();
    let default_reader: RuntimeReader = default_handle.reader();
    let view_cursor: UpdateCursor = reader.cursor();
    let compatibility_cursor: UpdateCursor = handle.cursor();
    let compatibility_snapshot: StateSnapshot = handle.latest_snapshot();
    let log = CommitLog::new();
    let read_guard: SnapshotReadGuard<'_> = reader.read();
    let read_view: StateReadView<'_> = read_guard.view();

    assert_eq!(registry.domains(), &[ProtocolDomain::System]);
    assert_eq!(view_cursor.next_revision().get(), 1);
    assert_eq!(compatibility_cursor.next_revision().get(), 1);
    assert_eq!(compatibility_snapshot.revision().get(), 0);
    assert_eq!(log.head_revision(), None);
    assert_eq!(reader.head_revision(), None);
    assert_eq!(read_guard.revision().get(), 0);
    assert_eq!(read_view.revision().get(), 0);
    assert!(
        read_guard
            .decode::<Quote, _, _>(["quotes", "SHFE.au2602"])
            .unwrap()
            .is_none()
    );
    assert!(
        read_guard
            .decode_path::<Quote>(&["quotes", "SHFE.au2602"])
            .unwrap()
            .is_none()
    );
    assert!(
        compatibility_snapshot
            .decode::<Quote, _, _>(["quotes", "SHFE.au2602"])
            .unwrap()
            .is_none()
    );
    assert!(
        compatibility_snapshot
            .decode_path::<Quote>(&["quotes", "SHFE.au2602"])
            .unwrap()
            .is_none()
    );
    assert_eq!(default_handle.latest_snapshot().revision().get(), 0);
    assert_eq!(default_handle.cursor().next_revision().get(), 1);
    assert_eq!(default_reader.cursor().next_revision().get(), 1);

    fn assert_runtime<T: Runtime>(_value: &T) {}
    assert_runtime(&handle);
    assert_runtime(&default_handle);

    let adapter = TestAdapter;
    assert_eq!(adapter.domain(), ProtocolDomain::System);
}

#[test]
fn public_surface_exports_are_usable_together() {
    let _revision = tqsdk_core::Revision::new(11);
    let _command = tqsdk_core::RuntimeCommand::System(tqsdk_core::SystemCommand::RefreshAuth);
    let _input = tqsdk_core::RuntimeInput::Internal(tqsdk_core::InternalEvent {
        label: "checkpoint",
        payload: None,
    });
    let _scope = tqsdk_core::CommitScope::SessionTransition;
    let _domain = tqsdk_core::ProtocolDomain::Schema;
    let handle = tqsdk_core::RuntimeHandle::default();
    let reader = handle.reader();
    let cursor = reader.cursor();
    let guard = reader.read();
    let _view = guard.view();
    let _lagged: Option<CursorLagged> = None;
    let _next_view = RuntimeReader::next_view;
    let _typed_guard: Option<CommitReadGuard<'_>> = None;

    assert_eq!(cursor.next_revision().get(), 1);
}

#[test]
fn transport_namespace_is_stable_and_session_runtime_bridge_stays_internal() {
    let lib = include_str!("../src/lib.rs");

    assert!(
        lib.contains("pub mod transport;"),
        "transport contract namespace should be part of the public surface"
    );
    assert!(
        !lib.contains("pub mod session_runtime;"),
        "session runtime implementation module should not be part of the stable public surface"
    );
    assert!(
        !lib.contains("pub use auth::{AuthContext, AuthProvider, DynAuthProvider}"),
        "dyn auth bridge should stay behind tqsdk_core::internal instead of the root public surface"
    );
    assert!(
        lib.contains("#[doc(hidden)]") && lib.contains("pub mod internal"),
        "cross-crate implementation bridge should be explicit and doc-hidden"
    );
    assert!(
        lib.contains("not part of the stable public contract"),
        "internal bridge should document its unstable sibling-crate-only status"
    );
}

use serde_json::{json, Value};
use tqsdk_runtime_contract::{
    AccountId, AdapterRegistry, FieldMutation, InternalEvent, MarketAdapter, MarketChartCommand, MarketCommand,
    MutationSource, NormalizedMutation, ObjectKey, OrderId, OutboundFrame, OutboundRequest, ProtocolAdapter,
    ProtocolDomain, QueryAdapter, QueryCommand, QueryId, ReplayAdapter, ReplayCommand, RuntimeCommand, RuntimeInput,
    SchemaAdapter, SchemaCommand, StatePath, Symbol, SystemAdapter, SystemCommand, TradeAdapter, TradeCommand,
    TradeDirection, TradeInsertOrderCommand, TradeLoginCommand, TradeOffset, TradePriceType, TradeTimeCondition,
    TradeVolumeCondition,
};

#[derive(Clone)]
struct StubAdapter {
    domain: ProtocolDomain,
    accepts_command_domain: ProtocolDomain,
    accepted_input_label: &'static str,
    encoded: Vec<OutboundRequest>,
    decoded: Vec<NormalizedMutation>,
}

impl ProtocolAdapter for StubAdapter {
    fn domain(&self) -> ProtocolDomain {
        self.domain
    }

    fn accepts_command(&self, cmd: &RuntimeCommand) -> bool {
        cmd.domain() == self.accepts_command_domain
    }

    fn encode(&mut self, _cmd: &RuntimeCommand) -> tqsdk_runtime_contract::Result<Vec<OutboundRequest>> {
        Ok(self.encoded.clone())
    }

    fn accepts_input(&self, input: &RuntimeInput) -> bool {
        matches!(
            input,
            RuntimeInput::Internal(InternalEvent { label }) if *label == self.accepted_input_label
        )
    }

    fn decode(&mut self, _input: &RuntimeInput) -> tqsdk_runtime_contract::Result<Vec<NormalizedMutation>> {
        Ok(self.decoded.clone())
    }
}

#[test]
fn adapter_registry_routes_commands_and_fans_out_inputs() {
    let mut registry = AdapterRegistry::new();
    registry.register_adapter(StubAdapter {
        domain: ProtocolDomain::System,
        accepts_command_domain: ProtocolDomain::System,
        accepted_input_label: "shared",
        encoded: vec![OutboundRequest::internal_label("shutdown-runtime")],
        decoded: vec![mutation("system", "event", "shared", MutationSource::SessionControl)],
    });
    registry.register_adapter(StubAdapter {
        domain: ProtocolDomain::Replay,
        accepts_command_domain: ProtocolDomain::Replay,
        accepted_input_label: "shared",
        encoded: vec![OutboundRequest::Replay(tqsdk_runtime_contract::ReplayRequest { action: "step" })],
        decoded: vec![mutation("replay", "event", "shared", MutationSource::ReplayStep)],
    });

    let command = RuntimeCommand::System(SystemCommand::Shutdown);
    assert_eq!(registry.domains(), &[ProtocolDomain::System, ProtocolDomain::Replay]);
    assert_eq!(registry.owning_domain(&command), Some(ProtocolDomain::System));
    assert_eq!(
        registry.encode_command(&command).unwrap(),
        vec![OutboundRequest::internal_label("shutdown-runtime")]
    );

    let input = RuntimeInput::Internal(InternalEvent { label: "shared" });
    assert_eq!(
        registry.decode_input(&input).unwrap(),
        vec![
            mutation("system", "event", "shared", MutationSource::SessionControl),
            mutation("replay", "event", "shared", MutationSource::ReplayStep),
        ]
    );
}

#[test]
fn default_protocol_adapters_cover_domain_registration_and_encode_shapes() {
    let mut registry = AdapterRegistry::new();
    registry.register_default_adapters();

    assert_eq!(
        registry.domains(),
        &[
            ProtocolDomain::System,
            ProtocolDomain::Market,
            ProtocolDomain::Trade,
            ProtocolDomain::Replay,
            ProtocolDomain::Query,
            ProtocolDomain::Schema,
        ]
    );

    let market_subscribe = registry
        .encode_command(&RuntimeCommand::Market(MarketCommand::SubscribeQuotes {
            symbols: vec![Symbol::new("SHFE.au2602"), Symbol::new("DCE.m2609"), Symbol::new("SHFE.au2602")],
        }))
        .unwrap();
    assert_json_frame(
        &market_subscribe[0],
        json!({"aid": "subscribe_quote", "ins_list": "DCE.m2609,SHFE.au2602"}),
    );
    assert_json_frame(&market_subscribe[1], json!({"aid": "peek_message"}));

    let market_unsubscribe = registry
        .encode_command(&RuntimeCommand::Market(MarketCommand::UnsubscribeQuotes {
            symbols: vec![Symbol::new("DCE.m2609")],
        }))
        .unwrap();
    assert_json_frame(
        &market_unsubscribe[0],
        json!({"aid": "subscribe_quote", "ins_list": "SHFE.au2602"}),
    );
    assert_json_frame(&market_unsubscribe[1], json!({"aid": "peek_message"}));

    let chart_requests = registry
        .encode_command(&RuntimeCommand::Market(MarketCommand::SetChart(MarketChartCommand {
            chart_id: "chart-1".to_string(),
            symbols: vec![Symbol::new("SHFE.au2602"), Symbol::new("SHFE.ag2606")],
            duration_ns: 60_000_000_000,
            view_width: 128,
            left_kline_id: Some(42),
            focus_datetime_ns: None,
            focus_position: None,
        })))
        .unwrap();
    assert_json_frame(
        &chart_requests[0],
        json!({
            "aid": "set_chart",
            "chart_id": "chart-1",
            "ins_list": "SHFE.au2602,SHFE.ag2606",
            "duration": 60_000_000_000_i64,
            "view_width": 128,
            "left_kline_id": 42,
        }),
    );
    assert_json_frame(&chart_requests[1], json!({"aid": "peek_message"}));

    let trading_status_requests = registry
        .encode_command(&RuntimeCommand::Market(MarketCommand::SubscribeTradingStatus {
            symbols: vec![Symbol::new("SHFE.au2602"), Symbol::new("CZCE.SR609")],
        }))
        .unwrap();
    assert_json_frame(
        &trading_status_requests[0],
        json!({"aid": "subscribe_trading_status", "ins_list": "CZCE.SR609,SHFE.au2602"}),
    );
    assert_json_frame(&trading_status_requests[1], json!({"aid": "peek_message"}));

    let trade_login = registry
        .encode_command(&RuntimeCommand::Trade(TradeCommand::Login(TradeLoginCommand {
            account_id: AccountId::new("simnow"),
            broker_id: "9999".to_string(),
            password: "secret".to_string(),
            account_type: tqsdk_runtime_contract::TradeAccountType::Future,
            front_broker: Some("9999".to_string()),
            front_url: Some("tcp://127.0.0.1:12345".to_string()),
            client_app_id: Some("SHINNY_TQ_1.0".to_string()),
            client_system_info: Some("SYSINFO".to_string()),
        })))
        .unwrap();
    assert_json_frame(
        &trade_login[0],
        json!({
            "aid": "req_login",
            "bid": "9999",
            "user_name": "simnow",
            "password": "secret",
            "client_app_id": "SHINNY_TQ_1.0",
            "client_system_info": "SYSINFO",
            "broker_id": "9999",
            "front": "tcp://127.0.0.1:12345",
        }),
    );

    let trade_insert = registry
        .encode_command(&RuntimeCommand::Trade(TradeCommand::InsertOrder(TradeInsertOrderCommand {
            account_id: AccountId::new("simnow"),
            order_id: OrderId::new("order-1"),
            symbol: Symbol::new("SHFE.au2602"),
            direction: TradeDirection::Buy,
            offset: Some(TradeOffset::Open),
            volume: 2,
            price_type: TradePriceType::Limit,
            limit_price: Some(json!(618.5)),
            time_condition: TradeTimeCondition::Gfd,
            volume_condition: TradeVolumeCondition::Any,
        })))
        .unwrap();
    assert_json_frame(
        &trade_insert[0],
        json!({
            "aid": "insert_order",
            "user_id": "simnow",
            "order_id": "order-1",
            "exchange_id": "SHFE",
            "instrument_id": "au2602",
            "direction": "BUY",
            "offset": "OPEN",
            "volume": 2,
            "price_type": "LIMIT",
            "limit_price": 618.5,
            "time_condition": "GFD",
            "volume_condition": "ANY",
        }),
    );

    let query_fetch = registry
        .encode_command(&RuntimeCommand::Query(QueryCommand::Fetch {
            query_id: QueryId::new("quotes-page-1"),
            query: "query Quotes($instrument_id: String!) { symbols(instrument_id: $instrument_id) { instrument_id } }"
                .to_string(),
            variables: Some(json!({"instrument_id": "au2602"})),
        }))
        .unwrap();
    assert_json_frame(
        &query_fetch[0],
        json!({
            "aid": "ins_query",
            "query_id": "quotes-page-1",
            "query": "query Quotes($instrument_id: String!) { symbols(instrument_id: $instrument_id) { instrument_id } }",
            "variables": {"instrument_id": "au2602"},
        }),
    );
    assert_json_frame(&query_fetch[1], json!({"aid": "peek_message"}));

    assert_eq!(
        registry
            .encode_command(&RuntimeCommand::Schema(SchemaCommand::Refresh {
                schema_id: tqsdk_runtime_contract::SchemaId::new("instrument-schema"),
                path: "/t/symbols/latest.json".to_string(),
            }))
            .unwrap(),
        vec![OutboundRequest::Http(tqsdk_runtime_contract::HttpRequest {
            path: "/t/symbols/latest.json".to_string(),
        })]
    );

    assert_eq!(
        registry
            .encode_command(&RuntimeCommand::Replay(ReplayCommand::Step))
            .unwrap(),
        vec![OutboundRequest::Replay(tqsdk_runtime_contract::ReplayRequest { action: "step" })]
    );

    assert_eq!(
        registry
            .encode_command(&RuntimeCommand::System(SystemCommand::RefreshAuth))
            .unwrap(),
        vec![OutboundRequest::internal_label("refresh-auth")]
    );
}

#[test]
fn concrete_adapters_are_public_and_instantiable() {
    let _ = SystemAdapter::default();
    let _ = MarketAdapter::default();
    let _ = TradeAdapter::default();
    let _ = QueryAdapter::default();
    let _ = SchemaAdapter::default();
    let _ = ReplayAdapter::default();
}

fn mutation(prefix: &str, field: &str, value: &str, source: MutationSource) -> NormalizedMutation {
    NormalizedMutation {
        path: StatePath::new([prefix]),
        object: Option::<ObjectKey>::None,
        fields: vec![FieldMutation {
            field: field.to_string(),
            value: json!(value),
        }],
        source,
    }
}

fn assert_json_frame(request: &OutboundRequest, expected: Value) {
    match request {
        OutboundRequest::Transport(OutboundFrame::Text(text)) => {
            let actual: Value = serde_json::from_str(text).unwrap();
            assert_eq!(actual, expected);
        }
        other => panic!("expected text transport frame, got {other:?}"),
    }
}

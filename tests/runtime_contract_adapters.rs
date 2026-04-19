use serde_json::{Value, json};
use tqsdk_runtime_contract::{
    AccountId, AdapterRegistry, ChartId, FieldMutation, InputPayload, InternalEvent, MarketAdapter,
    MarketChartCommand, MarketCommand, MutationSource, NormalizedMutation, NotificationId,
    ObjectKey, OrderId, OutboundFrame, OutboundRequest, ProtocolAdapter, ProtocolDomain,
    QueryAdapter, QueryCommand, QueryId, ReplayAdapter, ReplayCommand, ReplayEvent,
    ReplaySessionId, RuntimeCommand, RuntimeInput, SchemaAdapter, SchemaCommand, StatePath, Symbol,
    SystemAdapter, SystemCommand, TradeAdapter, TradeCommand, TradeDirection,
    TradeInsertOrderCommand, TradeLoginCommand, TradeOffset, TradePriceType, TradeTimeCondition,
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

    fn encode(
        &mut self,
        _cmd: &RuntimeCommand,
    ) -> tqsdk_runtime_contract::Result<Vec<OutboundRequest>> {
        Ok(self.encoded.clone())
    }

    fn accepts_input(&self, input: &RuntimeInput) -> bool {
        matches!(
            input,
            RuntimeInput::Internal(InternalEvent { label, .. }) if *label == self.accepted_input_label
        )
    }

    fn decode(
        &mut self,
        _input: &RuntimeInput,
    ) -> tqsdk_runtime_contract::Result<Vec<NormalizedMutation>> {
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
        decoded: vec![mutation(
            "system",
            "event",
            "shared",
            MutationSource::SessionControl,
        )],
    });
    registry.register_adapter(StubAdapter {
        domain: ProtocolDomain::Replay,
        accepts_command_domain: ProtocolDomain::Replay,
        accepted_input_label: "shared",
        encoded: vec![OutboundRequest::Replay(
            tqsdk_runtime_contract::ReplayRequest { action: "step" },
        )],
        decoded: vec![mutation(
            "replay",
            "event",
            "shared",
            MutationSource::ReplayStep,
        )],
    });

    let command = RuntimeCommand::System(SystemCommand::Shutdown);
    assert_eq!(
        registry.domains(),
        &[ProtocolDomain::System, ProtocolDomain::Replay]
    );
    assert_eq!(
        registry.owning_domain(&command),
        Some(ProtocolDomain::System)
    );
    assert_eq!(
        registry.encode_command(&command).unwrap(),
        vec![OutboundRequest::internal_label("shutdown-runtime")]
    );

    let input = RuntimeInput::Internal(InternalEvent {
        label: "shared",
        payload: None,
    });
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
            symbols: vec![
                Symbol::new("SHFE.au2602"),
                Symbol::new("DCE.m2609"),
                Symbol::new("SHFE.au2602"),
            ],
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
        .encode_command(&RuntimeCommand::Market(MarketCommand::SetChart(
            MarketChartCommand {
                chart_id: "chart-1".to_string(),
                symbols: vec![Symbol::new("SHFE.au2602"), Symbol::new("SHFE.ag2606")],
                duration_ns: 60_000_000_000,
                view_width: 128,
                left_kline_id: Some(42),
                focus_datetime_ns: None,
                focus_position: None,
            },
        )))
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
        .encode_command(&RuntimeCommand::Market(
            MarketCommand::SubscribeTradingStatus {
                symbols: vec![Symbol::new("SHFE.au2602"), Symbol::new("CZCE.SR609")],
            },
        ))
        .unwrap();
    assert_json_frame(
        &trading_status_requests[0],
        json!({"aid": "subscribe_trading_status", "ins_list": "CZCE.SR609,SHFE.au2602"}),
    );
    assert_json_frame(&trading_status_requests[1], json!({"aid": "peek_message"}));

    let trade_login = registry
        .encode_command(&RuntimeCommand::Trade(TradeCommand::Login(
            TradeLoginCommand {
                account_id: AccountId::new("simnow"),
                broker_id: "9999".to_string(),
                password: "secret".to_string(),
                account_type: tqsdk_runtime_contract::TradeAccountType::Future,
                front_broker: Some("9999".to_string()),
                front_url: Some("tcp://127.0.0.1:12345".to_string()),
                client_app_id: Some("SHINNY_TQ_1.0".to_string()),
                client_system_info: Some("SYSINFO".to_string()),
            },
        )))
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

    let trade_query_account_info = registry
        .encode_command(&RuntimeCommand::Trade(TradeCommand::QueryAccountInfo {
            account_id: AccountId::new("simnow"),
        }))
        .unwrap();
    assert_json_frame(
        &trade_query_account_info[0],
        json!({
            "aid": "qry_account_info",
            "user_id": "simnow",
        }),
    );

    let trade_query_account_register = registry
        .encode_command(&RuntimeCommand::Trade(TradeCommand::QueryAccountRegister {
            account_id: AccountId::new("simnow"),
        }))
        .unwrap();
    assert_json_frame(
        &trade_query_account_register[0],
        json!({
            "aid": "qry_account_register",
            "user_id": "simnow",
        }),
    );

    let trade_set_risk_management_rule = registry
        .encode_command(&RuntimeCommand::Trade(
            TradeCommand::SetRiskManagementRule {
                account_id: AccountId::new("simnow"),
                rule: json!({
                    "exchange_id": "SSE",
                    "enable": true,
                    "self_trade": {
                        "count_limit": 3,
                    },
                }),
            },
        ))
        .unwrap();
    assert_json_frame(
        &trade_set_risk_management_rule[0],
        json!({
            "aid": "set_risk_management_rule",
            "user_id": "simnow",
            "exchange_id": "SSE",
            "enable": true,
            "self_trade": {
                "count_limit": 3,
            },
        }),
    );

    let trade_insert = registry
        .encode_command(&RuntimeCommand::Trade(TradeCommand::InsertOrder(
            TradeInsertOrderCommand {
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
            },
        )))
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
        vec![OutboundRequest::Replay(
            tqsdk_runtime_contract::ReplayRequest { action: "step" }
        )]
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

#[test]
fn default_protocol_adapters_decode_structured_inputs_into_mutations() {
    let mut registry = AdapterRegistry::new();
    registry.register_default_adapters();

    let market = registry
        .decode_input(&RuntimeInput::Io(tqsdk_runtime_contract::IoEvent {
            route: "market.shared".to_string(),
            domains: vec![ProtocolDomain::Market],
            payload: InputPayload::Json(json!({
                "aid": "rtn_data",
                "data": [
                    {
                        "quotes": {
                            "SHFE.au2602": {
                                "last_price": 618.5,
                                "ask_price1": 619.0
                            }
                        }
                    },
                    {
                        "klines": {
                            "SHFE.au2602": {
                                "60000000000": {
                                    "42": {
                                        "open": 610.0,
                                        "close": 618.5
                                    }
                                }
                            }
                        }
                    },
                    {
                        "ticks": {
                            "SHFE.au2602": {
                                "17": {
                                    "last_price": 618.5,
                                    "volume": 200
                                }
                            }
                        }
                    },
                    {
                        "charts": {
                            "chart-1": {
                                "left_id": 40,
                                "right_id": 42,
                                "more_data": false
                            }
                        }
                    },
                    {
                        "trading_status": {
                            "SHFE.au2602": {
                                "symbol": "SHFE.au2602",
                                "trade_status": "CONTINOUS"
                            }
                        }
                    }
                ]
            })),
        }))
        .unwrap();
    assert_eq!(
        market,
        vec![
            NormalizedMutation {
                path: StatePath::new(["quotes", "SHFE.au2602"]),
                object: Some(ObjectKey::Quote {
                    symbol: Symbol::new("SHFE.au2602"),
                }),
                fields: vec![
                    FieldMutation {
                        field: "ask_price1".to_string(),
                        value: json!(619.0),
                    },
                    FieldMutation {
                        field: "last_price".to_string(),
                        value: json!(618.5),
                    },
                ],
                source: MutationSource::MarketDiff,
            },
            NormalizedMutation {
                path: StatePath::new(["klines", "SHFE.au2602", "60000000000", "42"]),
                object: Some(ObjectKey::Kline {
                    series: tqsdk_runtime_contract::SeriesKey {
                        primary: Symbol::new("SHFE.au2602"),
                        secondary: vec![],
                        duration_ns: 60_000_000_000,
                        view_width: 0,
                        right_id: None,
                    },
                    bar_id: 42,
                }),
                fields: vec![
                    FieldMutation {
                        field: "close".to_string(),
                        value: json!(618.5),
                    },
                    FieldMutation {
                        field: "open".to_string(),
                        value: json!(610.0),
                    },
                ],
                source: MutationSource::MarketDiff,
            },
            NormalizedMutation {
                path: StatePath::new(["ticks", "SHFE.au2602", "17"]),
                object: Some(ObjectKey::Tick {
                    symbol: Symbol::new("SHFE.au2602"),
                    tick_id: 17,
                }),
                fields: vec![
                    FieldMutation {
                        field: "last_price".to_string(),
                        value: json!(618.5),
                    },
                    FieldMutation {
                        field: "volume".to_string(),
                        value: json!(200),
                    },
                ],
                source: MutationSource::MarketDiff,
            },
            NormalizedMutation {
                path: StatePath::new(["charts", "chart-1"]),
                object: Some(ObjectKey::Chart {
                    chart_id: ChartId::new("chart-1"),
                }),
                fields: vec![
                    FieldMutation {
                        field: "left_id".to_string(),
                        value: json!(40),
                    },
                    FieldMutation {
                        field: "more_data".to_string(),
                        value: json!(false),
                    },
                    FieldMutation {
                        field: "right_id".to_string(),
                        value: json!(42),
                    },
                ],
                source: MutationSource::MarketDiff,
            },
            NormalizedMutation {
                path: StatePath::new(["trading_status", "SHFE.au2602"]),
                object: Some(ObjectKey::TradingStatus {
                    symbol: Symbol::new("SHFE.au2602"),
                }),
                fields: vec![
                    FieldMutation {
                        field: "symbol".to_string(),
                        value: json!("SHFE.au2602"),
                    },
                    FieldMutation {
                        field: "trade_status".to_string(),
                        value: json!("CONTINOUS"),
                    },
                ],
                source: MutationSource::MarketDiff,
            }
        ]
    );

    let trade = registry
        .decode_input(&RuntimeInput::Io(tqsdk_runtime_contract::IoEvent {
            route: "trade.simnow".to_string(),
            domains: vec![ProtocolDomain::Trade],
            payload: InputPayload::Json(json!({
                "aid": "rtn_data",
                "data": [
                    {
                        "trade": {
                            "simnow": {
                                "accounts": {
                                    "CNY": {
                                        "balance": 100000.0,
                                        "available": 80000.0
                                    }
                                },
                                "orders": {
                                    "order-1": {
                                        "status": "ALIVE",
                                        "volume_left": 2
                                    }
                                },
                                "trades": {
                                    "trade-1": {
                                        "order_id": "order-1",
                                        "trade_price": 618.5
                                    }
                                },
                                "his_settlements": {
                                    "20260419": {
                                        "content": ["line-1", "line-2"]
                                    }
                                },
                                "risk_management_rule": {
                                    "SSE": {
                                        "exchange_id": "SSE",
                                        "enable": true,
                                        "self_trade": {
                                            "count_limit": 3
                                        }
                                    }
                                },
                                "risk_management_data": {
                                    "SHFE.au2602": {
                                        "exchange_id": "SHFE",
                                        "instrument_id": "au2602",
                                        "user_id": "simnow"
                                    }
                                },
                                "trade_more_data": false
                            }
                        }
                    },
                    {
                        "trade": {
                            "simnow": {
                                "positions": {
                                    "SHFE.au2602": {
                                        "pos": 2,
                                        "volume_long_today": 2
                                    }
                                }
                            }
                        }
                    }
                ]
            })),
        }))
        .unwrap();
    assert_eq!(
        trade,
        vec![
            NormalizedMutation {
                path: StatePath::new(["trade", "simnow", "accounts", "CNY"]),
                object: Some(ObjectKey::Account {
                    account_id: AccountId::new("simnow"),
                }),
                fields: vec![
                    FieldMutation {
                        field: "available".to_string(),
                        value: json!(80000.0),
                    },
                    FieldMutation {
                        field: "balance".to_string(),
                        value: json!(100000.0),
                    },
                ],
                source: MutationSource::TradeReply,
            },
            NormalizedMutation {
                path: StatePath::new(["trade", "simnow", "his_settlements", "20260419"]),
                object: Some(ObjectKey::Settlement {
                    account_id: AccountId::new("simnow"),
                    trading_day: "20260419".to_string(),
                }),
                fields: vec![FieldMutation {
                    field: "content".to_string(),
                    value: json!(["line-1", "line-2"]),
                }],
                source: MutationSource::TradeReply,
            },
            NormalizedMutation {
                path: StatePath::new(["trade", "simnow", "orders", "order-1"]),
                object: Some(ObjectKey::Order {
                    account_id: AccountId::new("simnow"),
                    order_id: OrderId::new("order-1"),
                }),
                fields: vec![
                    FieldMutation {
                        field: "status".to_string(),
                        value: json!("ALIVE"),
                    },
                    FieldMutation {
                        field: "volume_left".to_string(),
                        value: json!(2),
                    },
                ],
                source: MutationSource::TradeReply,
            },
            NormalizedMutation {
                path: StatePath::new(["trade", "simnow", "risk_management_data", "SHFE.au2602"]),
                object: Some(ObjectKey::RiskManagementData {
                    account_id: AccountId::new("simnow"),
                    symbol: Symbol::new("SHFE.au2602"),
                }),
                fields: vec![
                    FieldMutation {
                        field: "exchange_id".to_string(),
                        value: json!("SHFE"),
                    },
                    FieldMutation {
                        field: "instrument_id".to_string(),
                        value: json!("au2602"),
                    },
                    FieldMutation {
                        field: "user_id".to_string(),
                        value: json!("simnow"),
                    },
                ],
                source: MutationSource::TradeReply,
            },
            NormalizedMutation {
                path: StatePath::new(["trade", "simnow", "risk_management_rule", "SSE"]),
                object: Some(ObjectKey::RiskManagementRule {
                    account_id: AccountId::new("simnow"),
                    exchange_id: "SSE".to_string(),
                }),
                fields: vec![
                    FieldMutation {
                        field: "enable".to_string(),
                        value: json!(true),
                    },
                    FieldMutation {
                        field: "exchange_id".to_string(),
                        value: json!("SSE"),
                    },
                ],
                source: MutationSource::TradeReply,
            },
            NormalizedMutation {
                path: StatePath::new([
                    "trade",
                    "simnow",
                    "risk_management_rule",
                    "SSE",
                    "self_trade"
                ]),
                object: None,
                fields: vec![FieldMutation {
                    field: "count_limit".to_string(),
                    value: json!(3),
                }],
                source: MutationSource::TradeReply,
            },
            NormalizedMutation {
                path: StatePath::new(["trade", "simnow", "trade_more_data"]),
                object: None,
                fields: vec![FieldMutation {
                    field: "value".to_string(),
                    value: json!(false),
                }],
                source: MutationSource::TradeReply,
            },
            NormalizedMutation {
                path: StatePath::new(["trade", "simnow", "trades", "trade-1"]),
                object: Some(ObjectKey::Trade {
                    account_id: AccountId::new("simnow"),
                    trade_id: tqsdk_runtime_contract::TradeId::new("trade-1"),
                }),
                fields: vec![
                    FieldMutation {
                        field: "order_id".to_string(),
                        value: json!("order-1"),
                    },
                    FieldMutation {
                        field: "trade_price".to_string(),
                        value: json!(618.5),
                    },
                ],
                source: MutationSource::TradeReply,
            },
            NormalizedMutation {
                path: StatePath::new(["trade", "simnow", "positions", "SHFE.au2602"]),
                object: Some(ObjectKey::Position {
                    account_id: AccountId::new("simnow"),
                    symbol: Symbol::new("SHFE.au2602"),
                }),
                fields: vec![
                    FieldMutation {
                        field: "pos".to_string(),
                        value: json!(2),
                    },
                    FieldMutation {
                        field: "volume_long_today".to_string(),
                        value: json!(2),
                    },
                ],
                source: MutationSource::TradeReply,
            }
        ]
    );

    let query = registry
        .decode_input(&RuntimeInput::Io(tqsdk_runtime_contract::IoEvent {
            route: "ins.query".to_string(),
            domains: vec![ProtocolDomain::Query],
            payload: InputPayload::Json(json!({
                "aid": "rtn_data",
                "data": [{
                    "symbols": {
                        "quotes-page-1": {
                            "items": [{"instrument_id": "au2602"}],
                            "has_more": false
                        }
                    }
                }]
            })),
        }))
        .unwrap();
    assert_eq!(
        query,
        vec![NormalizedMutation {
            path: StatePath::new(["query", "quotes-page-1"]),
            object: Some(ObjectKey::QueryResult {
                query_id: QueryId::new("quotes-page-1"),
            }),
            fields: vec![
                FieldMutation {
                    field: "has_more".to_string(),
                    value: json!(false),
                },
                FieldMutation {
                    field: "items".to_string(),
                    value: json!([{ "instrument_id": "au2602" }]),
                },
            ],
            source: MutationSource::QueryResult,
        }]
    );

    let schema = registry
        .decode_input(&RuntimeInput::Io(tqsdk_runtime_contract::IoEvent {
            route: "instrument-schema".to_string(),
            domains: vec![ProtocolDomain::Schema],
            payload: InputPayload::Json(json!({
                "nodes": {
                    "quote": {
                        "fields": ["last_price", "ask_price1"]
                    }
                }
            })),
        }))
        .unwrap();
    assert_eq!(
        schema,
        vec![NormalizedMutation {
            path: StatePath::new(["schema", "instrument-schema", "nodes", "quote"]),
            object: Some(ObjectKey::SchemaNode {
                schema_id: tqsdk_runtime_contract::SchemaId::new("instrument-schema"),
            }),
            fields: vec![FieldMutation {
                field: "fields".to_string(),
                value: json!(["last_price", "ask_price1"]),
            }],
            source: MutationSource::SchemaBootstrap,
        }]
    );

    let replay = registry
        .decode_input(&RuntimeInput::Replay(ReplayEvent {
            label: "step",
            session_id: Some(ReplaySessionId::new("rb-replay")),
            payload: Some(json!({
                "cursor": {
                    "dt": 1713500000000_i64,
                    "state": "running"
                }
            })),
        }))
        .unwrap();
    assert_eq!(
        replay,
        vec![NormalizedMutation {
            path: StatePath::new(["replay", "rb-replay", "cursor"]),
            object: Some(ObjectKey::ReplayCursor {
                session_id: ReplaySessionId::new("rb-replay"),
            }),
            fields: vec![
                FieldMutation {
                    field: "dt".to_string(),
                    value: json!(1713500000000_i64),
                },
                FieldMutation {
                    field: "state".to_string(),
                    value: json!("running"),
                },
            ],
            source: MutationSource::ReplayStep,
        }]
    );

    let system = registry
        .decode_input(&RuntimeInput::Io(tqsdk_runtime_contract::IoEvent {
            route: "market.shared".to_string(),
            domains: vec![ProtocolDomain::System],
            payload: InputPayload::Json(json!({
                "aid": "rtn_data",
                "data": [{
                    "notify": {
                        "notify-1": {
                            "code": 2019112901,
                            "level": "INFO",
                            "content": "connected"
                        }
                    }
                }]
            })),
        }))
        .unwrap();
    assert_eq!(
        system,
        vec![NormalizedMutation {
            path: StatePath::new(["system", "notify", "notify-1"]),
            object: Some(ObjectKey::Notification {
                notification_id: NotificationId::new("notify-1"),
            }),
            fields: vec![
                FieldMutation {
                    field: "code".to_string(),
                    value: json!(2019112901),
                },
                FieldMutation {
                    field: "content".to_string(),
                    value: json!("connected"),
                },
                FieldMutation {
                    field: "level".to_string(),
                    value: json!("INFO"),
                },
            ],
            source: MutationSource::SessionControl,
        }]
    );
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

#[test]
fn trade_adapter_decodes_settlement_query_reply_into_trade_snapshot() {
    let mut registry = AdapterRegistry::new();
    registry.register_default_adapters();

    let trade = registry
        .decode_input(&RuntimeInput::Io(tqsdk_runtime_contract::IoEvent {
            route: "trade.simnow".to_string(),
            domains: vec![ProtocolDomain::Trade],
            payload: InputPayload::Json(json!({
                "aid": "qry_settlement_info",
                "user_name": "simnow",
                "trading_day": "20260420",
                "settlement_info": "line-1\nline-2",
            })),
        }))
        .unwrap();

    assert_eq!(
        trade,
        vec![NormalizedMutation {
            path: StatePath::new(["trade", "simnow", "his_settlements", "20260420"]),
            object: Some(ObjectKey::Settlement {
                account_id: AccountId::new("simnow"),
                trading_day: "20260420".to_string(),
            }),
            fields: vec![
                FieldMutation {
                    field: "content".to_string(),
                    value: json!("line-1\nline-2"),
                },
                FieldMutation {
                    field: "parsed".to_string(),
                    value: json!(false),
                },
            ],
            source: MutationSource::TradeReply,
        }]
    );
}

#[test]
fn trade_adapter_decodes_trade_session_branch_into_session_object() {
    let mut registry = AdapterRegistry::new();
    registry.register_default_adapters();

    let trade = registry
        .decode_input(&RuntimeInput::Io(tqsdk_runtime_contract::IoEvent {
            route: "trade.simnow".to_string(),
            domains: vec![ProtocolDomain::Trade],
            payload: InputPayload::Json(json!({
                "aid": "rtn_data",
                "data": [{
                    "trade": {
                        "simnow": {
                            "session": {
                                "trading_day": "20260420",
                                "user_id": "simnow",
                            }
                        }
                    }
                }]
            })),
        }))
        .unwrap();

    assert_eq!(
        trade,
        vec![NormalizedMutation {
            path: StatePath::new(["trade", "simnow", "session"]),
            object: Some(ObjectKey::TradeSession {
                account_id: AccountId::new("simnow"),
            }),
            fields: vec![
                FieldMutation {
                    field: "trading_day".to_string(),
                    value: json!("20260420"),
                },
                FieldMutation {
                    field: "user_id".to_string(),
                    value: json!("simnow"),
                },
            ],
            source: MutationSource::TradeReply,
        }]
    );
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

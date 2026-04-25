use serde_json::{Value, json};
use tqsdk_core::{
    AccountId, AdapterRegistry, CommitScope, FieldMutation, InputPayload, IoEvent, MutationSource,
    NormalizedMutation, ObjectKey, OrderId, OutboundRequest, ProtocolAdapter, ProtocolDomain,
    Runtime, RuntimeCommand, RuntimeHandle, RuntimeInput, StatePath, Symbol,
};

#[derive(Clone)]
struct DomainStateAdapter {
    decoded: Vec<NormalizedMutation>,
}

impl ProtocolAdapter for DomainStateAdapter {
    fn domain(&self) -> ProtocolDomain {
        ProtocolDomain::Market
    }

    fn accepts_command(&self, _cmd: &RuntimeCommand) -> bool {
        false
    }

    fn encode(&mut self, _cmd: &RuntimeCommand) -> tqsdk_core::Result<Vec<OutboundRequest>> {
        Ok(Vec::new())
    }

    fn accepts_input(&self, input: &RuntimeInput) -> bool {
        matches!(
            input,
            RuntimeInput::Io(IoEvent { route, .. }) if route == "domain-state"
        )
    }

    fn decode(&mut self, _input: &RuntimeInput) -> tqsdk_core::Result<Vec<NormalizedMutation>> {
        Ok(self.decoded.clone())
    }
}

#[test]
fn state_snapshot_exposes_typed_market_and_trade_root_views() {
    let symbol = Symbol::new("SHFE.au2602");
    let account_id = AccountId::new("simnow");
    let order_id = OrderId::new("order-1");

    let mut registry = AdapterRegistry::new();
    registry.register_adapter(DomainStateAdapter {
        decoded: vec![
            quote_mutation(symbol.clone(), 618.5),
            trading_status_mutation(symbol.clone(), "CONTINOUS"),
            account_mutation(account_id.clone(), 1000.0),
            position_mutation(account_id.clone(), symbol.clone(), 2),
            order_mutation(
                account_id.clone(),
                order_id.clone(),
                symbol.clone(),
                "ALIVE",
            ),
        ],
    });

    let handle = RuntimeHandle::with_adapters(registry);
    handle
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "domain-state".to_string(),
                domains: vec![ProtocolDomain::Market, ProtocolDomain::Trade],
                payload: InputPayload::Json(json!({})),
            }),
            Vec::new(),
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("domain state mutations should publish a commit");

    let snapshot = handle.latest_snapshot();
    let market = snapshot.market_state();
    let trade = snapshot.trade_state();

    assert_eq!(market.revision(), snapshot.revision());
    assert_eq!(trade.revision(), snapshot.revision());
    assert_eq!(market.quote(&symbol).unwrap().unwrap().last_price, 618.5);
    assert_eq!(
        market
            .trading_status(&symbol)
            .unwrap()
            .unwrap()
            .trade_status,
        "CONTINOUS"
    );
    assert_eq!(trade.account(&account_id).unwrap().unwrap().balance, 1000.0);
    assert_eq!(
        trade
            .position(&account_id, &symbol)
            .unwrap()
            .unwrap()
            .pos_long,
        2
    );
    assert_eq!(
        trade.order(&account_id, &order_id).unwrap().unwrap().status,
        "ALIVE"
    );
    assert!(
        trade
            .order(&account_id, &OrderId::new("missing"))
            .unwrap()
            .is_none()
    );
}

#[test]
fn runtime_reader_exposes_partition_scoped_domain_state_reads() {
    let symbol = Symbol::new("SHFE.au2602");
    let account_id = AccountId::new("simnow");
    let order_id = OrderId::new("order-1");

    let mut registry = AdapterRegistry::new();
    registry.register_adapter(DomainStateAdapter {
        decoded: vec![
            quote_mutation(symbol.clone(), 619.5),
            trading_status_mutation(symbol.clone(), "CONTINOUS"),
            account_mutation(account_id.clone(), 2000.0),
            position_mutation(account_id.clone(), symbol.clone(), 3),
            order_mutation(
                account_id.clone(),
                order_id.clone(),
                symbol.clone(),
                "ALIVE",
            ),
        ],
    });

    let handle = RuntimeHandle::with_adapters(registry);
    handle
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "domain-state".to_string(),
                domains: vec![ProtocolDomain::Market, ProtocolDomain::Trade],
                payload: InputPayload::Json(json!({})),
            }),
            Vec::new(),
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("domain state mutations should publish a commit");

    let reader = handle.reader();
    let market = reader.read_market_state();
    let trade = reader.read_trade_state();

    assert_eq!(market.revision(), handle.latest_snapshot().revision());
    assert_eq!(trade.revision(), handle.latest_snapshot().revision());
    assert_eq!(market.quote(&symbol).unwrap().unwrap().last_price, 619.5);
    assert_eq!(
        market
            .trading_status(&symbol)
            .unwrap()
            .unwrap()
            .trade_status,
        "CONTINOUS"
    );
    assert_eq!(trade.account(&account_id).unwrap().unwrap().balance, 2000.0);
    assert_eq!(
        trade
            .position(&account_id, &symbol)
            .unwrap()
            .unwrap()
            .pos_long,
        3
    );
    assert_eq!(
        trade.order(&account_id, &order_id).unwrap().unwrap().status,
        "ALIVE"
    );
}

#[test]
fn runtime_reader_domain_state_reads_do_not_materialize_full_snapshot() {
    let source = include_str!("../src/runtime/reader.rs");

    let market_block = source
        .split("pub fn read_market_state")
        .nth(1)
        .and_then(|tail| tail.split("pub fn read_trade_state").next())
        .expect("RuntimeReader::read_market_state should exist");
    assert!(
        !market_block.contains("snapshot()"),
        "market domain reads should borrow state partitions directly"
    );

    let trade_block = source
        .split("pub fn read_trade_state")
        .nth(1)
        .and_then(|tail| tail.split("pub fn next(").next())
        .expect("RuntimeReader::read_trade_state should exist");
    assert!(
        !trade_block.contains("snapshot()"),
        "trade domain reads should borrow state partitions directly"
    );
}

fn quote_mutation(symbol: Symbol, last_price: f64) -> NormalizedMutation {
    NormalizedMutation {
        path: StatePath::new(["quotes", symbol.as_str()]),
        object: Some(ObjectKey::Quote {
            symbol: symbol.clone(),
        }),
        fields: vec![
            field("datetime", json!("2026-04-25 10:00:00.000")),
            field("last_price", json!(last_price)),
        ],
        source: MutationSource::MarketDiff,
    }
}

fn trading_status_mutation(symbol: Symbol, trade_status: &str) -> NormalizedMutation {
    NormalizedMutation {
        path: StatePath::new(["trading_status", symbol.as_str()]),
        object: Some(ObjectKey::TradingStatus {
            symbol: symbol.clone(),
        }),
        fields: vec![
            field("symbol", json!(symbol.as_str())),
            field("trade_status", json!(trade_status)),
        ],
        source: MutationSource::MarketDiff,
    }
}

fn account_mutation(account_id: AccountId, balance: f64) -> NormalizedMutation {
    NormalizedMutation {
        path: StatePath::new(["trade", account_id.as_str(), "accounts", "CNY"]),
        object: Some(ObjectKey::Account {
            account_id: account_id.clone(),
        }),
        fields: vec![
            field("user_id", json!(account_id.as_str())),
            field("balance", json!(balance)),
        ],
        source: MutationSource::TradeReply,
    }
}

fn position_mutation(account_id: AccountId, symbol: Symbol, long_today: i64) -> NormalizedMutation {
    NormalizedMutation {
        path: StatePath::new(["trade", account_id.as_str(), "positions", symbol.as_str()]),
        object: Some(ObjectKey::Position {
            account_id: account_id.clone(),
            symbol: symbol.clone(),
        }),
        fields: vec![
            field("user_id", json!(account_id.as_str())),
            field("instrument_id", json!("au2602")),
            field("exchange_id", json!("SHFE")),
            field("pos_long", json!(long_today)),
            field("pos_long_today", json!(long_today)),
        ],
        source: MutationSource::TradeReply,
    }
}

fn order_mutation(
    account_id: AccountId,
    order_id: OrderId,
    symbol: Symbol,
    status: &str,
) -> NormalizedMutation {
    NormalizedMutation {
        path: StatePath::new(["trade", account_id.as_str(), "orders", order_id.as_str()]),
        object: Some(ObjectKey::Order {
            account_id: account_id.clone(),
            order_id: order_id.clone(),
        }),
        fields: vec![
            field("exchange_id", json!("SHFE")),
            field("instrument_id", json!("au2602")),
            field("order_id", json!(order_id.as_str())),
            field("status", json!(status)),
            field("user_id", json!(account_id.as_str())),
            field("volume_left", json!(1)),
            field("volume_orign", json!(1)),
            field("symbol", json!(symbol.as_str())),
        ],
        source: MutationSource::TradeReply,
    }
}

fn field(field: &str, value: Value) -> FieldMutation {
    FieldMutation {
        field: field.to_string(),
        value,
    }
}

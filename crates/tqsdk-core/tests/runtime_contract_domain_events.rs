use serde_json::{Value, json};
use tqsdk_core::{
    AccountId, AdapterRegistry, CommitScope, DomainEvent, FieldMutation, InputPayload, IoEvent,
    MarketEvent, MutationSource, NormalizedMutation, ObjectKey, OrderId, OutboundRequest,
    ProtocolAdapter, ProtocolDomain, Runtime, RuntimeCommand, RuntimeHandle, RuntimeInput,
    StatePath, Symbol, TradeEvent, collect_domain_events,
};

#[derive(Clone)]
struct DomainEventAdapter {
    decoded: Vec<NormalizedMutation>,
}

impl ProtocolAdapter for DomainEventAdapter {
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
            RuntimeInput::Io(IoEvent { route, .. }) if route == "domain-events"
        )
    }

    fn decode(&mut self, _input: &RuntimeInput) -> tqsdk_core::Result<Vec<NormalizedMutation>> {
        Ok(self.decoded.clone())
    }
}

#[test]
fn runtime_commit_can_be_projected_into_typed_domain_events() {
    let symbol = Symbol::new("SHFE.au2602");
    let account_id = AccountId::new("simnow");
    let order_id = OrderId::new("order-1");

    let mut registry = AdapterRegistry::new();
    registry.register_adapter(DomainEventAdapter {
        decoded: vec![
            quote_mutation(symbol.clone(), 618.5),
            order_mutation(
                account_id.clone(),
                order_id.clone(),
                symbol.clone(),
                "ALIVE",
            ),
        ],
    });

    let handle = RuntimeHandle::with_adapters(registry);
    let commit = handle
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "domain-events".to_string(),
                domains: vec![ProtocolDomain::Market, ProtocolDomain::Trade],
                payload: InputPayload::Json(json!({})),
            }),
            Vec::new(),
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("domain mutations should publish a commit");

    let snapshot = handle.latest_snapshot();
    let events = collect_domain_events(&commit, snapshot.read()).unwrap();

    let quote = events
        .iter()
        .find_map(|event| match event {
            DomainEvent::Market(MarketEvent::QuoteUpdate { symbol, quote }) => {
                Some((symbol, quote))
            }
            _ => None,
        })
        .expect("quote update event should be emitted");
    assert_eq!(quote.0.as_str(), "SHFE.au2602");
    assert_eq!(quote.1.last_price, 618.5);

    let order = events
        .iter()
        .find_map(|event| match event {
            DomainEvent::Trade(TradeEvent::OrderUpdate {
                account_id,
                order_id,
                order,
            }) => Some((account_id, order_id, order)),
            _ => None,
        })
        .expect("order update event should be emitted");
    assert_eq!(order.0.as_str(), "simnow");
    assert_eq!(order.1.as_str(), "order-1");
    assert_eq!(order.2.status, "ALIVE");
    assert_eq!(order.2.instrument_id, "au2602");
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

use serde_json::{Value, json};
use tqsdk_core::adapter::{MarketAdapter, TradeAdapter};
use tqsdk_core::{
    AdapterRegistry, CommitScope, ContractError, InputPayload, IoEvent, NormalizedMutation,
    OutboundDispatch, OutboundFrame, OutboundRequest, ProtocolAdapter, ProtocolDomain,
    RuntimeCommand, RuntimeHandle, RuntimeInput, TradeCommand,
};
use tqsdk_session::testing::ManualSession;
use tqsdk_task::TaskHost;
use tqsdk_wait::TqApi;

pub fn seeded_host() -> TaskHost {
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();
    let handle = RuntimeHandle::with_adapters(adapters);
    let session = ManualSession::from_runtime(handle).into_client();
    TaskHost::new(TqApi::new(session))
}

pub fn market_only_host() -> TaskHost {
    let mut adapters = AdapterRegistry::new();
    adapters.register_adapter(MarketAdapter::default());
    let handle = RuntimeHandle::with_adapters(adapters);
    let session = ManualSession::from_runtime(handle).into_client();
    TaskHost::new(TqApi::new(session))
}

pub fn host_with_trade_adapter<A>(trade_adapter: A) -> TaskHost
where
    A: ProtocolAdapter + 'static,
{
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();
    adapters.register_adapter(trade_adapter);
    let handle = RuntimeHandle::with_adapters(adapters);
    let session = ManualSession::from_runtime(handle).into_client();
    TaskHost::new(TqApi::new(session))
}

#[derive(Debug, Default)]
pub struct FailNthTradeInsertAdapter {
    inner: TradeAdapter,
    fail_on_insert: usize,
    seen_insert_orders: usize,
}

impl FailNthTradeInsertAdapter {
    pub fn new(fail_on_insert: usize) -> Self {
        Self {
            inner: TradeAdapter,
            fail_on_insert,
            seen_insert_orders: 0,
        }
    }
}

impl ProtocolAdapter for FailNthTradeInsertAdapter {
    fn domain(&self) -> ProtocolDomain {
        ProtocolDomain::Trade
    }

    fn accepts_command(&self, cmd: &RuntimeCommand) -> bool {
        self.inner.accepts_command(cmd)
    }

    fn encode(&mut self, cmd: &RuntimeCommand) -> tqsdk_core::Result<Vec<OutboundRequest>> {
        if matches!(cmd, RuntimeCommand::Trade(TradeCommand::InsertOrder(_))) {
            self.seen_insert_orders += 1;
            if self.seen_insert_orders == self.fail_on_insert {
                return Err(ContractError::validation(format!(
                    "injected trade insert failure at batch order {}",
                    self.seen_insert_orders
                )));
            }
        }

        self.inner.encode(cmd)
    }

    fn accepts_input(&self, input: &RuntimeInput) -> bool {
        self.inner.accepts_input(input)
    }

    fn decode(&mut self, input: &RuntimeInput) -> tqsdk_core::Result<Vec<NormalizedMutation>> {
        self.inner.decode(input)
    }
}

pub fn drain_dispatches(host: &TaskHost) -> Vec<OutboundDispatch> {
    host.api()
        .session()
        .handle()
        .drain_dispatches()
        .expect("manual test host should drain dispatches")
}

pub fn drain_order_dispatches(host: &TaskHost) -> Vec<OutboundDispatch> {
    drain_dispatches(host)
        .into_iter()
        .filter(is_order_dispatch)
        .collect()
}

fn is_order_dispatch(dispatch: &OutboundDispatch) -> bool {
    if dispatch.domain != ProtocolDomain::Trade {
        return false;
    }
    try_transport_payload(&dispatch.request).is_some_and(|payload| {
        matches!(
            payload.get("aid").and_then(Value::as_str),
            Some("insert_order" | "cancel_order")
        )
    })
}

fn try_transport_payload(request: &OutboundRequest) -> Option<Value> {
    match request {
        OutboundRequest::Transport(OutboundFrame::Text(text)) => serde_json::from_str(text).ok(),
        OutboundRequest::Transport(OutboundFrame::Binary(bytes)) => {
            serde_json::from_slice(bytes).ok()
        }
        _ => None,
    }
}

pub fn transport_payload(request: &OutboundRequest) -> serde_json::Value {
    try_transport_payload(request)
        .unwrap_or_else(|| panic!("expected transport request, got {request:?}"))
}

pub fn seed_quote_commit(host: &TaskHost, symbol: &str, last_price: f64) {
    seed_quote_book_commit(host, symbol, last_price, last_price, last_price);
}

pub fn seed_quote_book_commit(
    host: &TaskHost,
    symbol: &str,
    ask_price1: f64,
    bid_price1: f64,
    last_price: f64,
) {
    host.api()
        .session()
        .handle()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "quotes": {
                            symbol: {
                                "instrument_id": symbol,
                                "ask_price1": ask_price1,
                                "bid_price1": bid_price1,
                                "last_price": last_price,
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("seed quote commit should produce a commit");
}

pub fn seed_position_commit(host: &TaskHost, account_id: &str, symbol: &str, pos: i64) {
    let (pos_long, pos_short) = if pos >= 0 { (pos, 0) } else { (0, -pos) };
    host.api()
        .session()
        .handle()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "trade".to_string(),
                domains: vec![ProtocolDomain::Trade],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "trade": {
                            account_id: {
                                "positions": {
                                    symbol: {
                                        "user_id": account_id,
                                        "exchange_id": symbol.split_once('.').expect("symbol should contain exchange").0,
                                        "instrument_id": symbol.split_once('.').expect("symbol should contain exchange").1,
                                        "pos": pos,
                                        "pos_long": pos_long,
                                        "pos_short": pos_short,
                                    }
                                }
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("seed position commit should produce a commit");
}

pub fn seed_position_detail_commit(
    host: &TaskHost,
    account_id: &str,
    symbol: &str,
    pos_long_today: i64,
    pos_long_his: i64,
    pos_short_today: i64,
    pos_short_his: i64,
) {
    let pos_long = pos_long_today + pos_long_his;
    let pos_short = pos_short_today + pos_short_his;
    let pos = pos_long - pos_short;
    let (exchange_id, instrument_id) = symbol
        .split_once('.')
        .expect("symbol should contain exchange");
    host.api()
        .session()
        .handle()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "trade".to_string(),
                domains: vec![ProtocolDomain::Trade],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "trade": {
                            account_id: {
                                "positions": {
                                    symbol: {
                                        "user_id": account_id,
                                        "exchange_id": exchange_id,
                                        "instrument_id": instrument_id,
                                        "pos": pos,
                                        "pos_long": pos_long,
                                        "pos_short": pos_short,
                                        "pos_long_today": pos_long_today,
                                        "pos_long_his": pos_long_his,
                                        "pos_short_today": pos_short_today,
                                        "pos_short_his": pos_short_his,
                                    }
                                }
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("seed detailed position commit should produce a commit");
}

#[derive(Clone, Copy)]
pub struct OrderStatusSeed<'a> {
    pub direction: &'a str,
    pub offset: &'a str,
    pub limit_price: f64,
    pub status: &'a str,
    pub volume_orign: i64,
    pub volume_left: i64,
}

pub fn seed_order_status_commit(
    host: &TaskHost,
    account_id: &str,
    symbol: &str,
    order_id: &str,
    status: &str,
    volume_orign: i64,
    volume_left: i64,
) {
    seed_order_status_commit_with_seed(
        host,
        account_id,
        symbol,
        order_id,
        OrderStatusSeed {
            direction: "BUY",
            offset: "OPEN",
            limit_price: 3678.0,
            status,
            volume_orign,
            volume_left,
        },
    );
}

pub fn seed_order_status_commit_with_seed(
    host: &TaskHost,
    account_id: &str,
    symbol: &str,
    order_id: &str,
    seed: OrderStatusSeed<'_>,
) {
    let (exchange_id, instrument_id) = symbol
        .split_once('.')
        .expect("symbol should contain exchange");
    host.api()
        .session()
        .handle()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "trade".to_string(),
                domains: vec![ProtocolDomain::Trade],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "trade": {
                            account_id: {
                                "orders": {
                                    order_id: {
                                        "seqno": 1,
                                        "user_id": account_id,
                                        "order_id": order_id,
                                        "exchange_order_id": "exchange-order-1",
                                        "exchange_id": exchange_id,
                                        "instrument_id": instrument_id,
                                        "direction": seed.direction,
                                        "offset": seed.offset,
                                        "volume_orign": seed.volume_orign,
                                        "volume_left": seed.volume_left,
                                        "limit_price": seed.limit_price,
                                        "price_type": "LIMIT",
                                        "volume_condition": "ANY",
                                        "time_condition": "GFD",
                                        "insert_date_time": 1_713_660_000_000_000_000_i64,
                                        "status": seed.status,
                                    }
                                }
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("seed order status commit should produce a commit");
}

pub fn seed_trade_commit(
    host: &TaskHost,
    account_id: &str,
    symbol: &str,
    order_id: &str,
    trade_id: &str,
    volume: i64,
    price: f64,
) {
    let (exchange_id, instrument_id) = symbol
        .split_once('.')
        .expect("symbol should contain exchange");
    host.api()
        .session()
        .handle()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "trade".to_string(),
                domains: vec![ProtocolDomain::Trade],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "trade": {
                            account_id: {
                                "trades": {
                                    trade_id: {
                                        "seqno": 1,
                                        "user_id": account_id,
                                        "order_id": order_id,
                                        "trade_id": trade_id,
                                        "exchange_trade_id": "exchange-trade-1",
                                        "exchange_id": exchange_id,
                                        "instrument_id": instrument_id,
                                        "direction": "BUY",
                                        "offset": "OPEN",
                                        "price": price,
                                        "volume": volume,
                                        "trade_date_time": 1_713_660_000_000_000_000_i64,
                                    }
                                }
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("seed trade commit should produce a commit");
}

pub fn seed_wait_order_finished_commit(
    host: &TaskHost,
    account_id: &str,
    symbol: &str,
    order_seq: u64,
    volume_orign: i64,
) {
    let order_id = format!("wait-order-{order_seq}");
    seed_order_status_commit(
        host,
        account_id,
        symbol,
        &order_id,
        "FINISHED",
        volume_orign,
        0,
    );
}

use std::collections::HashMap;

use serde_json::{Value, json};
use tqsdk_core::{CommitScope, InputPayload, IoEvent, Position, ProtocolDomain, RuntimeInput};

use crate::{Result, TaskHost};

/// Deterministic fake market seed data for strategy tests.
#[derive(Debug, Clone, Default)]
pub struct FakeMarket {
    quotes: Vec<FakeQuote>,
    accounts: Vec<FakeAccount>,
    positions: Vec<FakePosition>,
}

#[derive(Debug, Clone)]
struct FakeQuote {
    symbol: String,
    last_price: f64,
}

#[derive(Debug, Clone)]
struct FakeAccount {
    account_id: String,
    available: f64,
}

#[derive(Debug, Clone)]
struct FakePosition {
    account_id: String,
    symbol: String,
    net_position: i64,
}

impl FakeMarket {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn quote(mut self, symbol: impl AsRef<str>, last_price: f64) -> Self {
        self.quotes.push(FakeQuote {
            symbol: symbol.as_ref().to_owned(),
            last_price,
        });
        self
    }

    #[must_use]
    pub fn account(mut self, account_id: impl AsRef<str>, available: f64) -> Self {
        self.accounts.push(FakeAccount {
            account_id: account_id.as_ref().to_owned(),
            available,
        });
        self
    }

    #[must_use]
    pub fn position(
        mut self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
        net_position: i64,
    ) -> Self {
        self.positions.push(FakePosition {
            account_id: account_id.as_ref().to_owned(),
            symbol: symbol.as_ref().to_owned(),
            net_position,
        });
        self
    }
}

pub(super) fn seed_market(
    host: &TaskHost,
    market: &FakeMarket,
) -> Result<HashMap<(String, String), Position>> {
    if !market.quotes.is_empty() {
        host.api().session().handle().ingest(
            RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "quotes": market.quotes.iter().map(|quote| {
                            (
                                quote.symbol.clone(),
                                json!({
                                    "instrument_id": quote.symbol,
                                    "datetime": "2026-04-27 09:30:00.000000",
                                    "last_price": quote.last_price
                                })
                            )
                        }).collect::<serde_json::Map<_, _>>()
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )?;
    }

    let mut positions = HashMap::new();
    if !market.accounts.is_empty() || !market.positions.is_empty() {
        let mut trade_accounts = serde_json::Map::new();
        for account in &market.accounts {
            trade_accounts
                .entry(account.account_id.clone())
                .or_insert_with(|| json!({ "accounts": {}, "positions": {} }));
            if let Some(account_node) = trade_accounts
                .get_mut(&account.account_id)
                .and_then(Value::as_object_mut)
            {
                account_node.insert(
                    "accounts".to_string(),
                    json!({
                        "CNY": {
                            "user_id": account.account_id,
                            "currency": "CNY",
                            "balance": account.available,
                            "available": account.available
                        }
                    }),
                );
            }
        }

        for position in &market.positions {
            let decoded = position_from_net(
                &position.account_id,
                &position.symbol,
                position.net_position,
            );
            positions.insert(
                (position.account_id.clone(), position.symbol.clone()),
                decoded.clone(),
            );
            trade_accounts
                .entry(position.account_id.clone())
                .or_insert_with(|| json!({ "accounts": {}, "positions": {} }));
            if let Some(account_node) = trade_accounts
                .get_mut(&position.account_id)
                .and_then(Value::as_object_mut)
            {
                let positions_node = account_node
                    .entry("positions".to_string())
                    .or_insert_with(|| json!({}));
                if let Some(positions_map) = positions_node.as_object_mut() {
                    positions_map.insert(position.symbol.clone(), position_to_value(&decoded));
                }
            }
        }

        host.api().session().handle().ingest(
            RuntimeInput::Io(IoEvent {
                route: "trade".to_string(),
                domains: vec![ProtocolDomain::Trade],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "trade": trade_accounts
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )?;
    }

    Ok(positions)
}

pub(super) fn position_from_net(account_id: &str, symbol: &str, net: i64) -> Position {
    let (exchange_id, instrument_id) = split_symbol(symbol);
    serde_json::from_value(position_json(
        account_id,
        symbol,
        exchange_id,
        instrument_id,
        net,
    ))
    .expect("static fake position payload should decode")
}

pub(super) fn position_to_value(position: &Position) -> Value {
    let symbol = format!("{}.{}", position.exchange_id, position.instrument_id);
    position_json(
        &position.user_id,
        &symbol,
        &position.exchange_id,
        &position.instrument_id,
        position.pos,
    )
}

fn position_json(
    account_id: &str,
    _symbol: &str,
    exchange_id: &str,
    instrument_id: &str,
    net: i64,
) -> Value {
    json!({
        "user_id": account_id,
        "exchange_id": exchange_id,
        "instrument_id": instrument_id,
        "volume_long": net.max(0),
        "volume_short": (-net).max(0),
        "pos_long": net.max(0),
        "pos_short": (-net).max(0),
        "pos": net
    })
}

fn split_symbol(symbol: &str) -> (&str, &str) {
    symbol.split_once('.').unwrap_or(("", symbol))
}

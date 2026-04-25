use serde::de::DeserializeOwned;

use crate::{
    Result,
    ids::{AccountId, ChartId, OrderId, Symbol, TradeId},
    state::{CommitResult, ObjectKey, SeriesKey, StatePath, StateReadView},
    types::{
        Account, Chart, Kline, Order, Position, PreInsertOrder, Quote, RiskManagementData,
        RiskManagementRule, SecurityAccount, SecurityOrder, SecurityPosition, SecurityTrade,
        SettlementInfo, Tick, Trade, TradingStatus,
    },
};

#[derive(Debug, Clone)]
#[non_exhaustive]
#[expect(
    clippy::large_enum_variant,
    reason = "domain events carry typed state snapshots inline to avoid extra heap indirection on commit fanout"
)]
pub enum MarketEvent {
    QuoteUpdate {
        symbol: Symbol,
        quote: Quote,
    },
    TradingStatusUpdate {
        symbol: Symbol,
        status: TradingStatus,
    },
    ChartUpdate {
        chart_id: ChartId,
        chart: Chart,
    },
    KlineUpdate {
        series: SeriesKey,
        bar_id: i64,
        kline: Kline,
    },
    TickUpdate {
        symbol: Symbol,
        tick_id: i64,
        tick: Tick,
    },
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum TradeEvent {
    AccountUpdate {
        account_id: AccountId,
        account: Account,
    },
    SecurityAccountUpdate {
        account_id: AccountId,
        account: SecurityAccount,
    },
    PositionUpdate {
        account_id: AccountId,
        symbol: Symbol,
        position: Position,
    },
    SecurityPositionUpdate {
        account_id: AccountId,
        symbol: Symbol,
        position: SecurityPosition,
    },
    PreInsertOrderUpdate {
        account_id: AccountId,
        order_id: OrderId,
        pre_insert_order: PreInsertOrder,
    },
    OrderUpdate {
        account_id: AccountId,
        order_id: OrderId,
        order: Order,
    },
    SecurityOrderUpdate {
        account_id: AccountId,
        order_id: OrderId,
        order: SecurityOrder,
    },
    TradeUpdate {
        account_id: AccountId,
        trade_id: TradeId,
        trade: Trade,
    },
    SecurityTradeUpdate {
        account_id: AccountId,
        trade_id: TradeId,
        trade: SecurityTrade,
    },
    RiskManagementRuleUpdate {
        account_id: AccountId,
        exchange_id: String,
        rule: RiskManagementRule,
    },
    RiskManagementDataUpdate {
        account_id: AccountId,
        symbol: Symbol,
        data: RiskManagementData,
    },
    SettlementInfoUpdate {
        account_id: AccountId,
        trading_day: String,
        settlement: SettlementInfo,
    },
}

#[derive(Debug, Clone)]
#[non_exhaustive]
#[expect(
    clippy::large_enum_variant,
    reason = "domain events preserve direct enum matching for public consumers; callers can box at their queue boundary"
)]
pub enum DomainEvent {
    Market(MarketEvent),
    Trade(TradeEvent),
}

pub fn collect_domain_events(
    commit: &CommitResult,
    snapshot: StateReadView<'_>,
) -> Result<Vec<DomainEvent>> {
    let mut events = Vec::new();

    for object in &commit.changes.object_hits {
        let Some(path) = path_for_object(commit, object) else {
            continue;
        };

        if let Some(event) = decode_market_event(object, path, snapshot)? {
            events.push(DomainEvent::Market(event));
            continue;
        }

        if let Some(event) = decode_trade_event(object, path, snapshot)? {
            events.push(DomainEvent::Trade(event));
        }
    }

    Ok(events)
}

fn decode_market_event(
    object: &ObjectKey,
    path: &StatePath,
    snapshot: StateReadView<'_>,
) -> Result<Option<MarketEvent>> {
    match object {
        ObjectKey::Quote { symbol } => decode_at_path::<Quote>(snapshot, path).map(|value| {
            value.map(|quote| MarketEvent::QuoteUpdate {
                symbol: symbol.clone(),
                quote,
            })
        }),
        ObjectKey::TradingStatus { symbol } => {
            decode_at_path::<TradingStatus>(snapshot, path).map(|value| {
                value.map(|status| MarketEvent::TradingStatusUpdate {
                    symbol: symbol.clone(),
                    status,
                })
            })
        }
        ObjectKey::Chart { chart_id } => decode_at_path::<Chart>(snapshot, path).map(|value| {
            value.map(|chart| MarketEvent::ChartUpdate {
                chart_id: chart_id.clone(),
                chart,
            })
        }),
        ObjectKey::Kline { series, bar_id } => {
            decode_at_path::<Kline>(snapshot, path).map(|value| {
                value.map(|kline| MarketEvent::KlineUpdate {
                    series: series.clone(),
                    bar_id: *bar_id,
                    kline,
                })
            })
        }
        ObjectKey::Tick { symbol, tick_id } => {
            decode_at_path::<Tick>(snapshot, path).map(|value| {
                value.map(|tick| MarketEvent::TickUpdate {
                    symbol: symbol.clone(),
                    tick_id: *tick_id,
                    tick,
                })
            })
        }
        _ => Ok(None),
    }
}

fn decode_trade_event(
    object: &ObjectKey,
    path: &StatePath,
    snapshot: StateReadView<'_>,
) -> Result<Option<TradeEvent>> {
    match object {
        ObjectKey::Account { account_id } => {
            if path_object_has_field(snapshot, path, "asset") {
                decode_at_path::<SecurityAccount>(snapshot, path).map(|value| {
                    value.map(|account| TradeEvent::SecurityAccountUpdate {
                        account_id: account_id.clone(),
                        account,
                    })
                })
            } else {
                decode_at_path::<Account>(snapshot, path).map(|value| {
                    value.map(|account| TradeEvent::AccountUpdate {
                        account_id: account_id.clone(),
                        account,
                    })
                })
            }
        }
        ObjectKey::Position { account_id, symbol } => {
            if path_object_has_field(snapshot, path, "create_date") {
                decode_at_path::<SecurityPosition>(snapshot, path).map(|value| {
                    value.map(|position| TradeEvent::SecurityPositionUpdate {
                        account_id: account_id.clone(),
                        symbol: symbol.clone(),
                        position,
                    })
                })
            } else {
                decode_at_path::<Position>(snapshot, path).map(|value| {
                    value.map(|position| TradeEvent::PositionUpdate {
                        account_id: account_id.clone(),
                        symbol: symbol.clone(),
                        position,
                    })
                })
            }
        }
        ObjectKey::PreInsertOrder {
            account_id,
            order_id,
        } => decode_at_path::<PreInsertOrder>(snapshot, path).map(|value| {
            value.map(|pre_insert_order| TradeEvent::PreInsertOrderUpdate {
                account_id: account_id.clone(),
                order_id: order_id.clone(),
                pre_insert_order,
            })
        }),
        ObjectKey::Order {
            account_id,
            order_id,
        } => {
            if path_object_has_field(snapshot, path, "frozen_fee") {
                decode_at_path::<SecurityOrder>(snapshot, path).map(|value| {
                    value.map(|order| TradeEvent::SecurityOrderUpdate {
                        account_id: account_id.clone(),
                        order_id: order_id.clone(),
                        order,
                    })
                })
            } else {
                decode_at_path::<Order>(snapshot, path).map(|value| {
                    value.map(|order| TradeEvent::OrderUpdate {
                        account_id: account_id.clone(),
                        order_id: order_id.clone(),
                        order,
                    })
                })
            }
        }
        ObjectKey::Trade {
            account_id,
            trade_id,
        } => {
            if path_object_has_field(snapshot, path, "fee") {
                decode_at_path::<SecurityTrade>(snapshot, path).map(|value| {
                    value.map(|trade| TradeEvent::SecurityTradeUpdate {
                        account_id: account_id.clone(),
                        trade_id: trade_id.clone(),
                        trade,
                    })
                })
            } else {
                decode_at_path::<Trade>(snapshot, path).map(|value| {
                    value.map(|trade| TradeEvent::TradeUpdate {
                        account_id: account_id.clone(),
                        trade_id: trade_id.clone(),
                        trade,
                    })
                })
            }
        }
        ObjectKey::RiskManagementRule {
            account_id,
            exchange_id,
        } => decode_at_path::<RiskManagementRule>(snapshot, path).map(|value| {
            value.map(|rule| TradeEvent::RiskManagementRuleUpdate {
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                rule,
            })
        }),
        ObjectKey::RiskManagementData { account_id, symbol } => {
            decode_at_path::<RiskManagementData>(snapshot, path).map(|value| {
                value.map(|data| TradeEvent::RiskManagementDataUpdate {
                    account_id: account_id.clone(),
                    symbol: symbol.clone(),
                    data,
                })
            })
        }
        ObjectKey::Settlement {
            account_id,
            trading_day,
        } => decode_at_path::<SettlementInfo>(snapshot, path).map(|value| {
            value.map(|settlement| TradeEvent::SettlementInfoUpdate {
                account_id: account_id.clone(),
                trading_day: trading_day.clone(),
                settlement,
            })
        }),
        _ => Ok(None),
    }
}

fn path_for_object<'a>(commit: &'a CommitResult, object: &ObjectKey) -> Option<&'a StatePath> {
    commit
        .changes
        .field_hits
        .iter()
        .find(|hit| &hit.object == object)
        .map(|hit| &hit.path)
}

fn decode_at_path<T>(snapshot: StateReadView<'_>, path: &StatePath) -> Result<Option<T>>
where
    T: DeserializeOwned,
{
    let segments = path
        .segments()
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    snapshot.decode_path(&segments)
}

fn path_object_has_field(snapshot: StateReadView<'_>, path: &StatePath, field: &str) -> bool {
    let segments = path
        .segments()
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    snapshot
        .get_path(&segments)
        .and_then(|value| value.as_object())
        .is_some_and(|object| object.contains_key(field))
}

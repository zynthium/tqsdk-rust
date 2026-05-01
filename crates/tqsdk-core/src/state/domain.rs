use std::sync::RwLockReadGuard;

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::{
    Account, AccountId, Order, OrderId, Position, Quote, Result, Revision, Symbol, TradingStatus,
};

use super::{
    StateReadView,
    read::{decode_value_at_path, get_at_path},
};

#[derive(Clone, Copy)]
pub struct MarketStateView<'a> {
    source: MarketStateSource<'a>,
}

#[derive(Clone, Copy)]
enum MarketStateSource<'a> {
    Snapshot(StateReadView<'a>),
    Partitions {
        revision: Revision,
        quotes: &'a Value,
        trading_status: &'a Value,
        charts: &'a Value,
        klines: &'a Value,
        ticks: &'a Value,
    },
}

impl<'a> MarketStateView<'a> {
    pub(crate) fn new(read: StateReadView<'a>) -> Self {
        Self {
            source: MarketStateSource::Snapshot(read),
        }
    }

    pub(crate) fn from_partitions(
        revision: Revision,
        quotes: &'a Value,
        trading_status: &'a Value,
        charts: &'a Value,
        klines: &'a Value,
        ticks: &'a Value,
    ) -> Self {
        Self {
            source: MarketStateSource::Partitions {
                revision,
                quotes,
                trading_status,
                charts,
                klines,
                ticks,
            },
        }
    }

    pub fn revision(&self) -> Revision {
        match self.source {
            MarketStateSource::Snapshot(read) => read.revision(),
            MarketStateSource::Partitions { revision, .. } => revision,
        }
    }

    /// Returns a raw value from a market-rooted path such as
    /// `quotes/{symbol}`, `charts/{chart_id}`, `klines/...`, or `ticks/...`.
    pub fn get_path(&self, path: &[&str]) -> Option<&'a Value> {
        match self.source {
            MarketStateSource::Snapshot(read) => read.get_path(path),
            MarketStateSource::Partitions {
                quotes,
                trading_status,
                charts,
                klines,
                ticks,
                ..
            } => {
                let (root, rest) = path.split_first()?;
                let partition = match *root {
                    "quotes" => quotes,
                    "trading_status" => trading_status,
                    "charts" => charts,
                    "klines" => klines,
                    "ticks" => ticks,
                    _ => return None,
                };
                get_at_path(partition, rest.iter().copied())
            }
        }
    }

    /// Decodes a value from a market-rooted path.
    pub fn decode_path<T>(&self, path: &[&str]) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        let Some(value) = self.get_path(path) else {
            return Ok(None);
        };

        decode_value_at_path(value, path).map(Some)
    }

    pub fn quote(&self, symbol: &Symbol) -> Result<Option<Quote>> {
        match self.source {
            MarketStateSource::Snapshot(read) => read.decode_path(&["quotes", symbol.as_str()]),
            MarketStateSource::Partitions { quotes, .. } => {
                decode_partition_path(quotes, &[symbol.as_str()], &["quotes", symbol.as_str()])
            }
        }
    }

    pub fn trading_status(&self, symbol: &Symbol) -> Result<Option<TradingStatus>> {
        match self.source {
            MarketStateSource::Snapshot(read) => {
                read.decode_path(&["trading_status", symbol.as_str()])
            }
            MarketStateSource::Partitions { trading_status, .. } => decode_partition_path(
                trading_status,
                &[symbol.as_str()],
                &["trading_status", symbol.as_str()],
            ),
        }
    }
}

pub struct MarketStateReadGuard<'a> {
    revision: Revision,
    quotes: RwLockReadGuard<'a, Value>,
    trading_status: RwLockReadGuard<'a, Value>,
    charts: RwLockReadGuard<'a, Value>,
    klines: RwLockReadGuard<'a, Value>,
    ticks: RwLockReadGuard<'a, Value>,
}

impl<'a> MarketStateReadGuard<'a> {
    pub(crate) fn new(
        revision: Revision,
        quotes: RwLockReadGuard<'a, Value>,
        trading_status: RwLockReadGuard<'a, Value>,
        charts: RwLockReadGuard<'a, Value>,
        klines: RwLockReadGuard<'a, Value>,
        ticks: RwLockReadGuard<'a, Value>,
    ) -> Self {
        Self {
            revision,
            quotes,
            trading_status,
            charts,
            klines,
            ticks,
        }
    }

    pub fn view(&self) -> MarketStateView<'_> {
        MarketStateView::from_partitions(
            self.revision,
            &self.quotes,
            &self.trading_status,
            &self.charts,
            &self.klines,
            &self.ticks,
        )
    }

    pub fn revision(&self) -> Revision {
        self.revision
    }

    pub fn quote(&self, symbol: &Symbol) -> Result<Option<Quote>> {
        self.view().quote(symbol)
    }

    pub fn trading_status(&self, symbol: &Symbol) -> Result<Option<TradingStatus>> {
        self.view().trading_status(symbol)
    }

    /// Returns a raw value from a market-rooted path.
    pub fn get_path(&self, path: &[&str]) -> Option<&Value> {
        self.view().get_path(path)
    }

    /// Decodes a value from a market-rooted path.
    pub fn decode_path<T>(&self, path: &[&str]) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        self.view().decode_path(path)
    }
}

#[derive(Clone, Copy)]
pub struct TradeStateView<'a> {
    source: TradeStateSource<'a>,
}

#[derive(Clone, Copy)]
enum TradeStateSource<'a> {
    Snapshot(StateReadView<'a>),
    Partition {
        revision: Revision,
        trade: &'a Value,
    },
}

impl<'a> TradeStateView<'a> {
    pub(crate) fn new(read: StateReadView<'a>) -> Self {
        Self {
            source: TradeStateSource::Snapshot(read),
        }
    }

    pub(crate) fn from_partition(revision: Revision, trade: &'a Value) -> Self {
        Self {
            source: TradeStateSource::Partition { revision, trade },
        }
    }

    pub fn revision(&self) -> Revision {
        match self.source {
            TradeStateSource::Snapshot(read) => read.revision(),
            TradeStateSource::Partition { revision, .. } => revision,
        }
    }

    pub fn account(&self, account_id: &AccountId) -> Result<Option<Account>> {
        match self.source {
            TradeStateSource::Snapshot(read) => {
                read.decode_path(&["trade", account_id.as_str(), "accounts", "CNY"])
            }
            TradeStateSource::Partition { trade, .. } => decode_partition_path(
                trade,
                &[account_id.as_str(), "accounts", "CNY"],
                &["trade", account_id.as_str(), "accounts", "CNY"],
            ),
        }
    }

    pub fn position(&self, account_id: &AccountId, symbol: &Symbol) -> Result<Option<Position>> {
        match self.source {
            TradeStateSource::Snapshot(read) => {
                read.decode_path(&["trade", account_id.as_str(), "positions", symbol.as_str()])
            }
            TradeStateSource::Partition { trade, .. } => decode_partition_path(
                trade,
                &[account_id.as_str(), "positions", symbol.as_str()],
                &["trade", account_id.as_str(), "positions", symbol.as_str()],
            ),
        }
    }

    pub fn order(&self, account_id: &AccountId, order_id: &OrderId) -> Result<Option<Order>> {
        match self.source {
            TradeStateSource::Snapshot(read) => {
                read.decode_path(&["trade", account_id.as_str(), "orders", order_id.as_str()])
            }
            TradeStateSource::Partition { trade, .. } => decode_partition_path(
                trade,
                &[account_id.as_str(), "orders", order_id.as_str()],
                &["trade", account_id.as_str(), "orders", order_id.as_str()],
            ),
        }
    }

    /// Decodes a value from a `trade`-partition relative path.
    pub fn decode_path<T>(&self, path: &[&str]) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        match self.source {
            TradeStateSource::Snapshot(read) => {
                let mut full_path = Vec::with_capacity(path.len() + 1);
                full_path.push("trade");
                full_path.extend_from_slice(path);
                read.decode_path(&full_path)
            }
            TradeStateSource::Partition { trade, .. } => {
                let mut display_path = Vec::with_capacity(path.len() + 1);
                display_path.push("trade");
                display_path.extend_from_slice(path);
                decode_partition_path(trade, path, &display_path)
            }
        }
    }
}

pub struct TradeStateReadGuard<'a> {
    revision: Revision,
    trade: RwLockReadGuard<'a, Value>,
}

impl<'a> TradeStateReadGuard<'a> {
    pub(crate) fn new(revision: Revision, trade: RwLockReadGuard<'a, Value>) -> Self {
        Self { revision, trade }
    }

    pub fn view(&self) -> TradeStateView<'_> {
        TradeStateView::from_partition(self.revision, &self.trade)
    }

    pub fn revision(&self) -> Revision {
        self.revision
    }

    pub fn account(&self, account_id: &AccountId) -> Result<Option<Account>> {
        self.view().account(account_id)
    }

    pub fn position(&self, account_id: &AccountId, symbol: &Symbol) -> Result<Option<Position>> {
        self.view().position(account_id, symbol)
    }

    pub fn order(&self, account_id: &AccountId, order_id: &OrderId) -> Result<Option<Order>> {
        self.view().order(account_id, order_id)
    }

    /// Decodes a value from a `trade`-partition relative path.
    pub fn decode_path<T>(&self, path: &[&str]) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        self.view().decode_path(path)
    }

    /// Returns a raw `Value` reference at a partition-relative path.
    pub(crate) fn get_path(&self, path: &[&str]) -> Option<&Value> {
        get_at_path(&self.trade, path.iter().copied())
    }
}

fn decode_partition_path<T>(
    partition: &Value,
    lookup_path: &[&str],
    display_path: &[&str],
) -> Result<Option<T>>
where
    T: DeserializeOwned,
{
    let Some(value) = get_at_path(partition, lookup_path.iter().copied()) else {
        return Ok(None);
    };

    decode_value_at_path(value, display_path).map(Some)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{AccountId, OrderId, Symbol};

    use super::*;

    #[test]
    fn market_state_view_reads_quote_and_returns_none_for_missing_symbol() {
        let data = json!({
            "quotes": {
                "SHFE.au2606": {
                    "instrument_id": "au2606",
                    "exchange_id": "SHFE",
                    "last_price": 610.5
                }
            }
        });
        let read = StateReadView::new(Revision::new(12), &data);
        let market = read.market_state();

        let quote = market
            .quote(&Symbol::new("SHFE.au2606"))
            .expect("quote decode should succeed")
            .expect("quote should exist");

        assert_eq!(market.revision(), Revision::new(12));
        assert_eq!(quote.instrument_id, "au2606");
        assert_eq!(quote.exchange_id, "SHFE");
        assert_eq!(quote.last_price, 610.5);
        assert!(
            market
                .quote(&Symbol::new("DCE.m2605"))
                .expect("missing quote lookup should not fail")
                .is_none()
        );
    }

    #[test]
    fn market_state_view_reads_from_partitions_without_full_snapshot() {
        let quotes = json!({
            "SHFE.au2606": {
                "instrument_id": "au2606",
                "exchange_id": "SHFE",
                "last_price": 611.0
            }
        });
        let empty = json!({});
        let market = MarketStateView::from_partitions(
            Revision::new(13),
            &quotes,
            &empty,
            &empty,
            &empty,
            &empty,
        );

        let quote = market
            .quote(&Symbol::new("SHFE.au2606"))
            .expect("partition quote decode should succeed")
            .expect("partition quote should exist");

        assert_eq!(market.revision(), Revision::new(13));
        assert_eq!(quote.last_price, 611.0);
        assert_eq!(
            market.get_path(&["quotes", "SHFE.au2606", "instrument_id"]),
            Some(&json!("au2606"))
        );
        assert!(market.get_path(&["trade", "sim"]).is_none());
    }

    #[test]
    fn trade_state_view_reads_account_position_order_and_trade() {
        let data = json!({
            "trade": {
                "sim": {
                    "accounts": {
                        "CNY": {
                            "user_id": "sim",
                            "balance": 100000.0
                        }
                    },
                    "positions": {
                        "SHFE.au2606": {
                            "user_id": "sim",
                            "exchange_id": "SHFE",
                            "instrument_id": "au2606",
                            "pos": 3
                        }
                    },
                    "orders": {
                        "ORDER-1": {
                            "user_id": "sim",
                            "order_id": "ORDER-1",
                            "exchange_id": "SHFE",
                            "instrument_id": "au2606",
                            "volume_left": 2
                        }
                    },
                    "trades": {
                        "TRADE-1": {
                            "user_id": "sim",
                            "order_id": "ORDER-1",
                            "trade_id": "TRADE-1",
                            "exchange_id": "SHFE",
                            "instrument_id": "au2606",
                            "volume": 1
                        }
                    }
                }
            }
        });
        let trade = StateReadView::new(Revision::new(14), &data).trade_state();
        let account_id = AccountId::new("sim");
        let symbol = Symbol::new("SHFE.au2606");

        let account = trade
            .account(&account_id)
            .expect("account decode should succeed")
            .expect("account should exist");
        let position = trade
            .position(&account_id, &symbol)
            .expect("position decode should succeed")
            .expect("position should exist");
        let order = trade
            .order(&account_id, &OrderId::new("ORDER-1"))
            .expect("order decode should succeed")
            .expect("order should exist");
        let trade_value: crate::Trade = trade
            .decode_path(&["sim", "trades", "TRADE-1"])
            .expect("trade decode should succeed")
            .expect("trade should exist");

        assert_eq!(trade.revision(), Revision::new(14));
        assert_eq!(account.user_id, "sim");
        assert_eq!(position.pos, 3);
        assert_eq!(order.volume_left, 2);
        assert_eq!(trade_value.volume, 1);
    }

    #[test]
    fn trade_state_view_reads_from_partition_without_full_snapshot() {
        let trade_partition = json!({
            "sim": {
                "accounts": {
                    "CNY": {
                        "user_id": "sim",
                        "available": 999.0
                    }
                }
            }
        });
        let trade = TradeStateView::from_partition(Revision::new(15), &trade_partition);

        let account = trade
            .account(&AccountId::new("sim"))
            .expect("partition account decode should succeed")
            .expect("partition account should exist");

        assert_eq!(trade.revision(), Revision::new(15));
        assert_eq!(account.available, 999.0);
        assert!(
            trade
                .account(&AccountId::new("missing"))
                .expect("missing partition account should not fail")
                .is_none()
        );
    }
}

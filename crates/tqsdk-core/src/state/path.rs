use crate::ids::{
    AccountId, ChartId, CommandId, NotificationId, OrderId, QueryId, ReplaySessionId, SchemaId,
    Symbol, TradeId,
};

pub type PathSegment = String;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StatePath(Vec<PathSegment>);

impl StatePath {
    pub fn new<I, S>(segments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self(segments.into_iter().map(Into::into).collect())
    }

    pub(crate) fn quote(symbol: &Symbol) -> Self {
        Self(vec!["quotes".to_string(), symbol.as_str().to_string()])
    }

    pub fn segments(&self) -> &[PathSegment] {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SeriesKey {
    pub primary: Symbol,
    pub secondary: Vec<Symbol>,
    pub duration_ns: i64,
    pub view_width: usize,
    pub right_id: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ObjectKey {
    SessionAuth,
    SessionLifecycle,
    SessionTopology,
    SessionReconnect,
    Quote {
        symbol: Symbol,
    },
    Kline {
        series: SeriesKey,
        bar_id: i64,
    },
    Tick {
        symbol: Symbol,
        tick_id: i64,
    },
    TradingStatus {
        symbol: Symbol,
    },
    Chart {
        chart_id: ChartId,
    },
    Command {
        command_id: CommandId,
    },
    Account {
        account_id: AccountId,
    },
    TradeSession {
        account_id: AccountId,
    },
    RiskManagementRule {
        account_id: AccountId,
        exchange_id: String,
    },
    RiskManagementData {
        account_id: AccountId,
        symbol: Symbol,
    },
    Position {
        account_id: AccountId,
        symbol: Symbol,
    },
    PreInsertOrder {
        account_id: AccountId,
        order_id: OrderId,
    },
    Order {
        account_id: AccountId,
        order_id: OrderId,
    },
    Trade {
        account_id: AccountId,
        trade_id: TradeId,
    },
    Settlement {
        account_id: AccountId,
        trading_day: String,
    },
    QueryResult {
        query_id: QueryId,
    },
    SchemaNode {
        schema_id: SchemaId,
    },
    ReplayCursor {
        session_id: ReplaySessionId,
    },
    Notification {
        notification_id: NotificationId,
    },
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn state_path_preserves_segment_order() {
        let path = StatePath::new(["trade", "sim", "orders", "ORDER-1"]);

        assert_eq!(
            path.segments(),
            &[
                "trade".to_string(),
                "sim".to_string(),
                "orders".to_string(),
                "ORDER-1".to_string(),
            ]
        );
    }

    #[test]
    fn quote_path_uses_quotes_root_and_symbol() {
        let path = StatePath::quote(&Symbol::new("SHFE.au2606"));

        assert_eq!(
            path.segments(),
            &["quotes".to_string(), "SHFE.au2606".to_string()]
        );
    }

    #[test]
    fn object_key_equality_distinguishes_domain_identity() {
        let quote = ObjectKey::Quote {
            symbol: Symbol::new("SHFE.au2606"),
        };
        let position = ObjectKey::Position {
            account_id: AccountId::new("sim"),
            symbol: Symbol::new("SHFE.au2606"),
        };
        let same_quote = ObjectKey::Quote {
            symbol: Symbol::new("SHFE.au2606"),
        };

        let mut keys = HashSet::new();
        keys.insert(quote.clone());
        keys.insert(position.clone());
        keys.insert(same_quote);

        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&quote));
        assert!(keys.contains(&position));
    }
}

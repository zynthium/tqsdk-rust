#![cfg_attr(not(test), forbid(unsafe_code))]

use serde_json::Value;
use tqsdk_core::{CommandId, EdbIndexData, SymbolRanking, SymbolSettlement, TradingCalendarDay};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Ranking dimension accepted by the symbol ranking service.
pub enum SymbolRankingType {
    Volume,
    Long,
    Short,
}

impl SymbolRankingType {
    #[cfg(feature = "services")]
    pub(crate) fn rank_field(self) -> &'static str {
        match self {
            Self::Volume => "volume_ranking",
            Self::Long => "long_ranking",
            Self::Short => "short_ranking",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Alignment strategy for EDB series queries.
pub enum EdbDataAlign {
    Day,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Missing-value fill strategy for EDB series queries.
pub enum EdbDataFill {
    Forward,
    Backward,
}

#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq)]
/// Optional filters forwarded to the official option metadata query.
pub struct OptionQueryFilter {
    pub option_class: Option<String>,
    pub exercise_year: Option<i32>,
    pub exercise_month: Option<i32>,
    pub strike_price: Option<f64>,
    pub expired: Option<bool>,
    pub has_a: Option<bool>,
}

impl OptionQueryFilter {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
/// Parameters for the ATM option query helper.
pub struct AtmOptionQuery {
    pub underlying_price: f64,
    pub price_levels: Vec<i32>,
    pub option_class: String,
    pub exercise_year: Option<i32>,
    pub exercise_month: Option<i32>,
    pub has_a: Option<bool>,
}

impl AtmOptionQuery {
    #[must_use]
    pub fn new(
        underlying_price: f64,
        price_levels: impl Into<Vec<i32>>,
        option_class: impl Into<String>,
    ) -> Self {
        Self {
            underlying_price,
            price_levels: price_levels.into(),
            option_class: option_class.into(),
            exercise_year: None,
            exercise_month: None,
            has_a: None,
        }
    }
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
/// Parameters for fetching all option levels around a target underlying price.
pub struct AllLevelOptionQuery {
    pub underlying_price: f64,
    pub option_class: String,
    pub exercise_year: Option<i32>,
    pub exercise_month: Option<i32>,
    pub has_a: Option<bool>,
}

impl AllLevelOptionQuery {
    #[must_use]
    pub fn new(underlying_price: f64, option_class: impl Into<String>) -> Self {
        Self {
            underlying_price,
            option_class: option_class.into(),
            exercise_year: None,
            exercise_month: None,
            has_a: None,
        }
    }
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
/// Parameters for the finance-option all-level query helper.
pub struct FinanceOptionLevelQuery {
    pub underlying_price: f64,
    pub option_class: String,
    pub nearbys: Vec<i32>,
    pub has_a: Option<bool>,
}

impl FinanceOptionLevelQuery {
    #[must_use]
    pub fn new(
        underlying_price: f64,
        option_class: impl Into<String>,
        nearbys: impl Into<Vec<i32>>,
    ) -> Self {
        Self {
            underlying_price,
            option_class: option_class.into(),
            nearbys: nearbys.into(),
            has_a: None,
        }
    }
}

#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
/// Grouped option symbols ordered by moneyness.
pub struct OptionLevelQuotes {
    pub in_money: Vec<String>,
    pub at_money: Vec<String>,
    pub out_of_money: Vec<String>,
}

/// Raw one-shot query/schema command surface.
///
/// These methods expose the low-level command contract. The `*_value` helpers
/// drive the session until the corresponding command completes and then return
/// the decoded payload.
#[expect(
    async_fn_in_trait,
    reason = "session query traits are intended for static dispatch without async-trait boxing"
)]
pub trait SessionRawQuery {
    async fn query_graphql(
        &self,
        query: &str,
        variables: Option<Value>,
    ) -> crate::error::Result<CommandId>;

    async fn refresh_schema(&self, schema_id: &str, path: &str) -> crate::error::Result<CommandId>;

    async fn query_graphql_value(
        &self,
        query: &str,
        variables: Option<Value>,
    ) -> crate::error::Result<Value>;

    async fn refresh_schema_value(
        &self,
        schema_id: &str,
        path: &str,
    ) -> crate::error::Result<Value>;
}

/// Thin typed wrappers over official GraphQL metadata queries.
#[expect(
    async_fn_in_trait,
    reason = "session query traits are intended for static dispatch without async-trait boxing"
)]
pub trait SessionMetadataQuery {
    async fn query_symbol_info(
        &self,
        symbols: &[&str],
    ) -> crate::error::Result<Vec<crate::SymbolInfo>>;

    async fn query_instrument_specs(
        &self,
        symbols: &[&str],
    ) -> crate::error::Result<Vec<crate::InstrumentSpec>>;

    async fn query_quotes(
        &self,
        ins_class: Option<&str>,
        exchange_id: Option<&str>,
        product_id: Option<&str>,
        expired: Option<bool>,
        has_night: Option<bool>,
    ) -> crate::error::Result<Vec<String>>;

    async fn query_cont_quotes(
        &self,
        exchange_id: Option<&str>,
        product_id: Option<&str>,
        has_night: Option<bool>,
    ) -> crate::error::Result<Vec<String>>;

    async fn query_options(
        &self,
        underlying_symbol: &str,
        filter: &OptionQueryFilter,
    ) -> crate::error::Result<Vec<String>>;

    async fn query_atm_options(
        &self,
        underlying_symbol: &str,
        query: &AtmOptionQuery,
    ) -> crate::error::Result<Vec<Option<String>>>;

    async fn query_all_level_options(
        &self,
        underlying_symbol: &str,
        query: &AllLevelOptionQuery,
    ) -> crate::error::Result<OptionLevelQuotes>;

    async fn query_all_level_finance_options(
        &self,
        underlying_symbol: &str,
        query: &FinanceOptionLevelQuery,
    ) -> crate::error::Result<OptionLevelQuotes>;
}

/// Thin typed wrappers over official HTTP direct-query services.
#[expect(
    async_fn_in_trait,
    reason = "session query traits are intended for static dispatch without async-trait boxing"
)]
pub trait SessionServiceQuery {
    async fn get_trading_calendar(
        &self,
        start_dt: chrono::NaiveDate,
        end_dt: chrono::NaiveDate,
    ) -> crate::error::Result<Vec<TradingCalendarDay>>;

    async fn query_symbol_settlement(
        &self,
        symbols: &[&str],
        days: usize,
        start_dt: Option<chrono::NaiveDate>,
    ) -> crate::error::Result<Vec<SymbolSettlement>>;

    async fn query_symbol_ranking(
        &self,
        symbol: &str,
        ranking_type: SymbolRankingType,
        days: usize,
        start_dt: Option<chrono::NaiveDate>,
        broker: Option<&str>,
    ) -> crate::error::Result<Vec<SymbolRanking>>;

    async fn query_edb_data(
        &self,
        ids: &[i32],
        start_dt: chrono::NaiveDate,
        end_dt: chrono::NaiveDate,
        align: Option<EdbDataAlign>,
        fill: Option<EdbDataFill>,
    ) -> crate::error::Result<Vec<EdbIndexData>>;
}

/// Full direct-query surface for callers that want one trait bound.
///
/// This trait is intentionally not object-safe: the crate keeps these async
/// calls statically dispatched to avoid boxing futures on the performance
/// critical substrate.
pub trait SessionDirectQuery: SessionRawQuery + SessionMetadataQuery + SessionServiceQuery {}

impl<T> SessionDirectQuery for T where
    T: SessionRawQuery + SessionMetadataQuery + SessionServiceQuery
{
}

#[cfg(test)]
mod tests {
    use super::{AllLevelOptionQuery, AtmOptionQuery, FinanceOptionLevelQuery, OptionQueryFilter};

    #[test]
    fn option_filter_new_returns_empty_filter() {
        assert_eq!(OptionQueryFilter::new(), OptionQueryFilter::default());
    }

    #[test]
    fn option_request_constructors_preserve_required_fields() {
        let atm = AtmOptionQuery::new(3188.0, [-2, 0, 2], "CALL");
        assert_eq!(atm.underlying_price, 3188.0);
        assert_eq!(atm.price_levels, vec![-2, 0, 2]);
        assert_eq!(atm.option_class, "CALL");
        assert_eq!(atm.exercise_year, None);

        let all_level = AllLevelOptionQuery::new(3188.0, "PUT");
        assert_eq!(all_level.underlying_price, 3188.0);
        assert_eq!(all_level.option_class, "PUT");
        assert_eq!(all_level.exercise_month, None);

        let finance = FinanceOptionLevelQuery::new(3188.0, "CALL", [0, 3]);
        assert_eq!(finance.underlying_price, 3188.0);
        assert_eq!(finance.option_class, "CALL");
        assert_eq!(finance.nearbys, vec![0, 3]);
        assert_eq!(finance.has_a, None);
    }
}

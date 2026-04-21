#![cfg_attr(not(test), forbid(unsafe_code))]

use serde_json::Value;
use tqsdk_core::{
    CommandId, EdbIndexData, Quote, SymbolRanking, SymbolSettlement, TradingCalendarDay,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SymbolRankingType {
    Volume,
    Long,
    Short,
}

impl SymbolRankingType {
    pub(crate) fn rank_field(self) -> &'static str {
        match self {
            Self::Volume => "volume_ranking",
            Self::Long => "long_ranking",
            Self::Short => "short_ranking",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdbDataAlign {
    Day,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdbDataFill {
    Forward,
    Backward,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct OptionQueryFilter {
    pub option_class: Option<String>,
    pub exercise_year: Option<i32>,
    pub exercise_month: Option<i32>,
    pub strike_price: Option<f64>,
    pub expired: Option<bool>,
    pub has_a: Option<bool>,
}

#[allow(async_fn_in_trait)]
pub trait SessionDirectQuery {
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

    async fn query_symbol_info(&self, symbols: &[&str]) -> crate::error::Result<Vec<Quote>>;

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

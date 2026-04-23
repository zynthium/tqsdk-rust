#![cfg_attr(not(test), forbid(unsafe_code))]

use chrono::Utc;
use serde_json::{Map, Value, json};
use tqsdk_core::Quote;

use crate::client::SessionClient;
use crate::direct_query::{
    AllLevelOptionQuery, AtmOptionQuery, FinanceOptionLevelQuery, OptionLevelQuotes,
    OptionQueryFilter,
};
use crate::error::Result;

#[path = "metadata_helpers.rs"]
mod helpers;

use self::helpers::{
    filter_option_nodes, non_empty_str, parse_option_nodes, parse_query_cont_quotes_result,
    parse_query_options_result, parse_query_quotes_result, parse_query_symbol_info_quotes,
    sort_options_and_get_atm_index, validate_finance_nearbys, validate_finance_underlying,
    validate_option_class, validate_price_levels, validation,
};

const FUTURE_EXCHANGES: &[&str] = &["CFFEX", "SHFE", "DCE", "CZCE", "INE", "GFEX"];

const QUERY_QUOTES: &str = r#"query($class_:[Class], $exchange_id:[String], $product_id:[String], $expired:Boolean, $has_night:Boolean){
  multi_symbol_info(class: $class_, exchange_id: $exchange_id, product_id: $product_id, expired: $expired, has_night: $has_night) {
    ... on basic { instrument_id }
  }
}"#;

const QUERY_CONT_QUOTES: &str = r#"query($class_:[Class], $has_night:Boolean){
  multi_symbol_info(class: $class_, has_night: $has_night) {
    ... on derivative {
      underlying {
        edges {
          node {
            ... on basic { instrument_id exchange_id }
            ... on future { product_id }
          }
        }
      }
    }
  }
}"#;

const QUERY_OPTIONS: &str = r#"query($instrument_id:[String], $derivative_class:[Class]){
  multi_symbol_info(instrument_id: $instrument_id) {
    ... on basic {
      instrument_id
      derivatives(class: $derivative_class) {
        edges {
          node {
            ... on basic {
              class
              instrument_id
              exchange_id
              english_name
            }
            ... on option {
              expired
              expire_datetime
              last_exercise_datetime
              strike_price
              call_or_put
            }
          }
        }
      }
    }
  }
}"#;

const QUERY_SYMBOL_INFO: &str = r#"query($instrument_id:[String]){
  multi_symbol_info(instrument_id: $instrument_id){
    ... on basic {
      instrument_id
      exchange_id
      instrument_name
      english_name
      class
      price_tick
      price_decs
      trading_day
      trading_time {
        day
        night
      }
    }
    ... on tradeable {
      pre_close
      volume_multiple
      quote_multiple
      upper_limit
      lower_limit
    }
    ... on index {
      index_multiple
    }
    ... on future {
      pre_open_interest
      expired
      product_id
      product_short_name
      delivery_year
      delivery_month
      expire_datetime
      settlement_price
      max_market_order_volume
      max_limit_order_volume
      min_market_order_volume
      min_limit_order_volume
      open_max_market_order_volume
      open_max_limit_order_volume
      open_min_market_order_volume
      open_min_limit_order_volume
    }
    ... on option {
      pre_open_interest
      expired
      product_short_name
      expire_datetime
      last_exercise_datetime
      settlement_price
      max_market_order_volume
      max_limit_order_volume
      min_market_order_volume
      min_limit_order_volume
      open_max_market_order_volume
      open_max_limit_order_volume
      open_min_market_order_volume
      open_min_limit_order_volume
      strike_price
      call_or_put
      exercise_type
    }
    ... on combine {
      expired
      product_id
      expire_datetime
      max_market_order_volume
      max_limit_order_volume
      min_market_order_volume
      min_limit_order_volume
      open_max_market_order_volume
      open_max_limit_order_volume
      open_min_market_order_volume
      open_min_limit_order_volume
      leg1 { ... on basic { instrument_id } }
      leg2 { ... on basic { instrument_id } }
    }
    ... on derivative {
      underlying {
        edges {
          node {
            ... on basic {
              instrument_id
              exchange_id
              instrument_name
              english_name
              class
              price_tick
              price_decs
              trading_day
              trading_time { day night }
            }
            ... on tradeable {
              pre_close
              volume_multiple
              quote_multiple
              upper_limit
              lower_limit
            }
            ... on index {
              index_multiple
            }
            ... on future {
              pre_open_interest
              expired
              product_id
              product_short_name
              delivery_year
              delivery_month
              expire_datetime
              settlement_price
              max_market_order_volume
              max_limit_order_volume
              min_market_order_volume
              min_limit_order_volume
              open_max_market_order_volume
              open_max_limit_order_volume
              open_min_market_order_volume
              open_min_limit_order_volume
            }
          }
        }
      }
    }
  }
}"#;

impl SessionClient {
    pub async fn query_symbol_info(&self, symbols: &[&str]) -> Result<Vec<Quote>> {
        if symbols.is_empty() {
            return Err(validation("symbols must not be empty"));
        }
        if symbols.iter().any(|symbol| symbol.is_empty()) {
            return Err(validation("symbols must not contain empty entries"));
        }

        let symbol_list: Vec<String> = symbols.iter().map(|symbol| (*symbol).to_string()).collect();
        let payload = self
            .query_graphql_value(
                QUERY_SYMBOL_INFO,
                Some(json!({
                    "instrument_id": symbol_list,
                })),
            )
            .await?;

        parse_query_symbol_info_quotes(&payload, &symbol_list, Utc::now().timestamp())
    }

    pub async fn query_quotes(
        &self,
        ins_class: Option<&str>,
        exchange_id: Option<&str>,
        product_id: Option<&str>,
        expired: Option<bool>,
        has_night: Option<bool>,
    ) -> Result<Vec<String>> {
        let ins_class = non_empty_str(ins_class, "ins_class")?;
        let exchange_id = non_empty_str(exchange_id, "exchange_id")?;
        let product_id = non_empty_str(product_id, "product_id")?;

        let mut variables = Map::from_iter([
            ("class_".to_string(), Value::Null),
            ("exchange_id".to_string(), Value::Null),
            ("product_id".to_string(), Value::Null),
            ("expired".to_string(), Value::Null),
            ("has_night".to_string(), Value::Null),
        ]);

        if let Some(ins_class) = ins_class {
            variables.insert("class_".to_string(), json!([ins_class]));
        }
        if let Some(exchange_id) = exchange_id {
            let is_future_exchange = FUTURE_EXCHANGES.contains(&exchange_id);
            let need_pass_exchange = match ins_class {
                Some(class) => !matches!(class, "INDEX" | "CONT") || !is_future_exchange,
                None => true,
            };
            if need_pass_exchange {
                variables.insert("exchange_id".to_string(), json!([exchange_id]));
            }
        }
        if let Some(product_id) = product_id {
            variables.insert("product_id".to_string(), json!([product_id]));
        }
        if let Some(expired) = expired {
            variables.insert("expired".to_string(), json!(expired));
        }
        if let Some(has_night) = has_night {
            variables.insert("has_night".to_string(), json!(has_night));
        }

        let payload = self
            .query_graphql_value(QUERY_QUOTES, Some(Value::Object(variables)))
            .await?;

        let target_exchange = if matches!(ins_class, Some("INDEX") | Some("CONT"))
            && matches!(exchange_id, Some(exchange) if FUTURE_EXCHANGES.contains(&exchange))
        {
            exchange_id
        } else {
            None
        };
        Ok(parse_query_quotes_result(&payload, target_exchange))
    }

    pub async fn query_cont_quotes(
        &self,
        exchange_id: Option<&str>,
        product_id: Option<&str>,
        has_night: Option<bool>,
    ) -> Result<Vec<String>> {
        let exchange_id = non_empty_str(exchange_id, "exchange_id")?;
        let product_id = non_empty_str(product_id, "product_id")?;
        let payload = self
            .query_graphql_value(
                QUERY_CONT_QUOTES,
                Some(json!({
                    "class_": ["CONT"],
                    "has_night": has_night,
                })),
            )
            .await?;
        Ok(parse_query_cont_quotes_result(
            &payload,
            exchange_id,
            product_id,
        ))
    }

    pub async fn query_options(
        &self,
        underlying_symbol: &str,
        filter: &OptionQueryFilter,
    ) -> Result<Vec<String>> {
        if underlying_symbol.is_empty() {
            return Err(validation("underlying_symbol must not be empty"));
        }

        let payload = self
            .query_graphql_value(
                QUERY_OPTIONS,
                Some(json!({
                    "instrument_id": [underlying_symbol],
                    "derivative_class": ["OPTION"],
                })),
            )
            .await?;
        Ok(parse_query_options_result(
            &payload,
            filter.option_class.as_deref(),
            filter.exercise_year,
            filter.exercise_month,
            filter.strike_price,
            filter.expired,
            filter.has_a,
        ))
    }

    pub async fn query_atm_options(
        &self,
        underlying_symbol: &str,
        query: &AtmOptionQuery,
    ) -> Result<Vec<Option<String>>> {
        if underlying_symbol.is_empty() {
            return Err(validation("underlying_symbol must not be empty"));
        }
        validate_option_class(query.option_class.as_str())?;
        validate_price_levels(&query.price_levels)?;

        let payload = self
            .query_graphql_value(
                QUERY_OPTIONS,
                Some(json!({
                    "instrument_id": [underlying_symbol],
                    "derivative_class": ["OPTION"],
                })),
            )
            .await?;
        let mut nodes = filter_option_nodes(
            parse_option_nodes(&payload),
            Some(query.option_class.as_str()),
            query.exercise_year,
            query.exercise_month,
            query.has_a,
            None,
        );
        if nodes.is_empty() {
            return Ok(query.price_levels.iter().map(|_| None).collect());
        }

        let atm_index = sort_options_and_get_atm_index(
            &mut nodes,
            query.underlying_price,
            query.option_class.as_str(),
        )?;
        let mut result = Vec::with_capacity(query.price_levels.len());
        for price_level in &query.price_levels {
            let index = atm_index as i64 - *price_level as i64;
            if index >= 0 && (index as usize) < nodes.len() {
                result.push(Some(nodes[index as usize].instrument_id.clone()));
            } else {
                result.push(None);
            }
        }
        Ok(result)
    }

    pub async fn query_all_level_options(
        &self,
        underlying_symbol: &str,
        query: &AllLevelOptionQuery,
    ) -> Result<OptionLevelQuotes> {
        if underlying_symbol.is_empty() {
            return Err(validation("underlying_symbol must not be empty"));
        }
        validate_option_class(query.option_class.as_str())?;

        let payload = self
            .query_graphql_value(
                QUERY_OPTIONS,
                Some(json!({
                    "instrument_id": [underlying_symbol],
                    "derivative_class": ["OPTION"],
                })),
            )
            .await?;
        let mut nodes = filter_option_nodes(
            parse_option_nodes(&payload),
            Some(query.option_class.as_str()),
            query.exercise_year,
            query.exercise_month,
            query.has_a,
            None,
        );
        if nodes.is_empty() {
            return Ok(OptionLevelQuotes::default());
        }
        let atm_index = sort_options_and_get_atm_index(
            &mut nodes,
            query.underlying_price,
            query.option_class.as_str(),
        )?;
        Ok(OptionLevelQuotes {
            in_money: nodes[..atm_index]
                .iter()
                .map(|node| node.instrument_id.clone())
                .collect(),
            at_money: vec![nodes[atm_index].instrument_id.clone()],
            out_of_money: nodes[atm_index + 1..]
                .iter()
                .map(|node| node.instrument_id.clone())
                .collect(),
        })
    }

    pub async fn query_all_level_finance_options(
        &self,
        underlying_symbol: &str,
        query: &FinanceOptionLevelQuery,
    ) -> Result<OptionLevelQuotes> {
        if underlying_symbol.is_empty() {
            return Err(validation("underlying_symbol must not be empty"));
        }
        validate_finance_underlying(underlying_symbol)?;
        validate_option_class(query.option_class.as_str())?;
        validate_finance_nearbys(underlying_symbol, &query.nearbys)?;

        let payload = self
            .query_graphql_value(
                QUERY_OPTIONS,
                Some(json!({
                    "instrument_id": [underlying_symbol],
                    "derivative_class": ["OPTION"],
                })),
            )
            .await?;
        let mut nodes = filter_option_nodes(
            parse_option_nodes(&payload),
            Some(query.option_class.as_str()),
            None,
            None,
            query.has_a,
            Some(&query.nearbys),
        );
        if nodes.is_empty() {
            return Ok(OptionLevelQuotes::default());
        }
        let atm_index = sort_options_and_get_atm_index(
            &mut nodes,
            query.underlying_price,
            query.option_class.as_str(),
        )?;
        Ok(OptionLevelQuotes {
            in_money: nodes[..atm_index]
                .iter()
                .map(|node| node.instrument_id.clone())
                .collect(),
            at_money: vec![nodes[atm_index].instrument_id.clone()],
            out_of_money: nodes[atm_index + 1..]
                .iter()
                .map(|node| node.instrument_id.clone())
                .collect(),
        })
    }
}

#[cfg(test)]
#[path = "metadata_tests.rs"]
mod tests;

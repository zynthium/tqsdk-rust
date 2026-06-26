#![cfg_attr(not(test), forbid(unsafe_code))]

use crate::client::SessionClient;
use crate::direct_query::{
    AllLevelOptionQuery, AtmOptionQuery, FinanceOptionLevelQuery, OptionLevelQuotes,
    OptionQueryFilter,
};
use crate::error::Result;
use chrono::Utc;
use serde_json::{Map, Value, json};

mod decoder;
#[path = "metadata_helpers.rs"]
mod helpers;

use self::decoder::MetadataSymbolDecoder;
use self::helpers::{
    non_empty_str, parse_query_cont_quotes_result, parse_query_quotes_result,
    validate_finance_nearbys, validate_finance_underlying, validate_option_class,
    validate_price_levels, validation,
};

const FUTURE_EXCHANGES: &[&str] = &["CFFEX", "SHFE", "DCE", "CZCE", "INE", "GFEX"];

const QUERY_QUOTES_SELECTION: &str = r#"    ... on basic { instrument_id }"#;

const QUERY_CONT_QUOTES_SELECTION: &str = r#"    ... on derivative {
      underlying {
        edges {
          node {
            ... on basic { instrument_id exchange_id }
            ... on future { product_id }
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
      open_limit
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

#[derive(Debug, Clone, PartialEq)]
struct MetadataQueryRequest {
    query: String,
    variables: Value,
    target_exchange: Option<String>,
}

fn build_multi_symbol_info_query(
    variable_defs: &[&str],
    arguments: &[&str],
    selection: &str,
) -> String {
    let variables = if variable_defs.is_empty() {
        String::new()
    } else {
        format!("({})", variable_defs.join(", "))
    };
    let arguments = if arguments.is_empty() {
        String::new()
    } else {
        format!("({})", arguments.join(", "))
    };
    format!("query{variables}{{\n  multi_symbol_info{arguments} {{\n{selection}\n  }}\n}}")
}

fn build_query_quotes_request(
    ins_class: Option<&str>,
    exchange_id: Option<&str>,
    product_id: Option<&str>,
    expired: Option<bool>,
    has_night: Option<bool>,
) -> Result<MetadataQueryRequest> {
    let ins_class = non_empty_str(ins_class, "ins_class")?;
    let exchange_id = non_empty_str(exchange_id, "exchange_id")?;
    let product_id = non_empty_str(product_id, "product_id")?;

    let mut variable_defs = Vec::new();
    let mut arguments = Vec::new();
    let mut variables = Map::new();

    if let Some(ins_class) = ins_class {
        variable_defs.push("$class_:[Class]");
        arguments.push("class: $class_");
        variables.insert("class_".to_string(), json!([ins_class]));
    }
    if let Some(exchange_id) = exchange_id {
        let is_future_exchange = FUTURE_EXCHANGES.contains(&exchange_id);
        let need_pass_exchange = match ins_class {
            Some(class) => !matches!(class, "INDEX" | "CONT") || !is_future_exchange,
            None => true,
        };
        if need_pass_exchange {
            variable_defs.push("$exchange_id:[String]");
            arguments.push("exchange_id: $exchange_id");
            variables.insert("exchange_id".to_string(), json!([exchange_id]));
        }
    }
    if let Some(product_id) = product_id {
        variable_defs.push("$product_id:[String]");
        arguments.push("product_id: $product_id");
        variables.insert("product_id".to_string(), json!([product_id]));
    }
    if let Some(expired) = expired {
        variable_defs.push("$expired:Boolean");
        arguments.push("expired: $expired");
        variables.insert("expired".to_string(), json!(expired));
    }
    if let Some(has_night) = has_night {
        variable_defs.push("$has_night:Boolean");
        arguments.push("has_night: $has_night");
        variables.insert("has_night".to_string(), json!(has_night));
    }

    let target_exchange = if matches!(ins_class, Some("INDEX") | Some("CONT"))
        && matches!(exchange_id, Some(exchange) if FUTURE_EXCHANGES.contains(&exchange))
    {
        exchange_id.map(str::to_string)
    } else {
        None
    };

    Ok(MetadataQueryRequest {
        query: build_multi_symbol_info_query(&variable_defs, &arguments, QUERY_QUOTES_SELECTION),
        variables: Value::Object(variables),
        target_exchange,
    })
}

fn build_query_cont_quotes_request(has_night: Option<bool>) -> Result<MetadataQueryRequest> {
    let mut variable_defs = vec!["$class_:[Class]"];
    let mut arguments = vec!["class: $class_"];
    let mut variables = Map::from_iter([("class_".to_string(), json!(["CONT"]))]);

    if let Some(has_night) = has_night {
        variable_defs.push("$has_night:Boolean");
        arguments.push("has_night: $has_night");
        variables.insert("has_night".to_string(), json!(has_night));
    }

    Ok(MetadataQueryRequest {
        query: build_multi_symbol_info_query(
            &variable_defs,
            &arguments,
            QUERY_CONT_QUOTES_SELECTION,
        ),
        variables: Value::Object(variables),
        target_exchange: None,
    })
}

impl SessionClient {
    pub async fn query_symbol_info(&self, symbols: &[&str]) -> Result<Vec<crate::SymbolInfo>> {
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

        MetadataSymbolDecoder::new(Utc::now().timestamp())
            .decode_symbol_infos(&payload, &symbol_list)
    }

    pub async fn query_instrument_specs(
        &self,
        symbols: &[&str],
    ) -> Result<Vec<crate::InstrumentSpec>> {
        self.query_symbol_info(symbols)
            .await?
            .into_iter()
            .map(crate::InstrumentSpec::try_from)
            .collect()
    }

    pub async fn query_quotes(
        &self,
        ins_class: Option<&str>,
        exchange_id: Option<&str>,
        product_id: Option<&str>,
        expired: Option<bool>,
        has_night: Option<bool>,
    ) -> Result<Vec<String>> {
        let request =
            build_query_quotes_request(ins_class, exchange_id, product_id, expired, has_night)?;
        let payload = self
            .query_graphql_value(&request.query, Some(request.variables))
            .await?;

        Ok(parse_query_quotes_result(
            &payload,
            request.target_exchange.as_deref(),
        ))
    }

    pub async fn query_cont_quotes(
        &self,
        exchange_id: Option<&str>,
        product_id: Option<&str>,
        has_night: Option<bool>,
    ) -> Result<Vec<String>> {
        let exchange_id = non_empty_str(exchange_id, "exchange_id")?;
        let product_id = non_empty_str(product_id, "product_id")?;
        let request = build_query_cont_quotes_request(has_night)?;
        let payload = self
            .query_graphql_value(&request.query, Some(request.variables))
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
        Ok(MetadataSymbolDecoder::new(Utc::now().timestamp())
            .decode_option_symbols(&payload, filter))
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
        MetadataSymbolDecoder::new(Utc::now().timestamp()).decode_atm_options(&payload, query)
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
        MetadataSymbolDecoder::new(Utc::now().timestamp()).decode_option_levels(&payload, query)
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
        MetadataSymbolDecoder::new(Utc::now().timestamp())
            .decode_finance_option_levels(&payload, query)
    }
}

#[cfg(test)]
mod tests;

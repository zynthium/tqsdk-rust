#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::HashMap;

use chrono::{Datelike, FixedOffset, TimeZone, Utc};
use serde_json::{Map, Value, json};
use tqsdk_core::Quote;

use crate::client::SessionClient;
use crate::direct_query::OptionQueryFilter;
use crate::error::{Result, SessionFacadeError};

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
              class_
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
        let mut variables = Map::new();
        if let Some(ins_class) = non_empty_str(ins_class, "ins_class")? {
            variables.insert("class_".to_string(), json!([ins_class]));
        }
        if let Some(exchange_id) = non_empty_str(exchange_id, "exchange_id")? {
            let is_future_exchange = FUTURE_EXCHANGES.contains(&exchange_id);
            let need_pass_exchange = match ins_class {
                Some(class) => !matches!(class, "INDEX" | "CONT") || !is_future_exchange,
                None => true,
            };
            if need_pass_exchange {
                variables.insert("exchange_id".to_string(), json!([exchange_id]));
            }
        }
        if let Some(product_id) = non_empty_str(product_id, "product_id")? {
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
}

fn validation(message: impl Into<String>) -> SessionFacadeError {
    SessionFacadeError::from(tqsdk_core::ContractError::validation(message))
}

fn non_empty_str<'a>(value: Option<&'a str>, name: &str) -> Result<Option<&'a str>> {
    match value {
        Some("") => Err(validation(format!("{name} must not be empty"))),
        other => Ok(other),
    }
}

fn parse_query_quotes_result(payload: &Value, target_exchange: Option<&str>) -> Vec<String> {
    let mut items = Vec::new();
    if let Some(result) = payload.get("result")
        && let Some(symbols) = result.get("multi_symbol_info").and_then(Value::as_array)
    {
        for symbol in symbols {
            if let Some(instrument_id) = symbol.get("instrument_id").and_then(Value::as_str) {
                if let Some(target_exchange) = target_exchange {
                    if instrument_id.contains(target_exchange) {
                        items.push(instrument_id.to_string());
                    }
                } else {
                    items.push(instrument_id.to_string());
                }
            }
        }
    }
    items
}

fn parse_query_cont_quotes_result(
    payload: &Value,
    exchange_id: Option<&str>,
    product_id: Option<&str>,
) -> Vec<String> {
    let mut items = Vec::new();
    if let Some(result) = payload.get("result")
        && let Some(symbols) = result.get("multi_symbol_info").and_then(Value::as_array)
    {
        for symbol in symbols {
            if let Some(underlying) = symbol.get("underlying")
                && let Some(edges) = underlying.get("edges").and_then(Value::as_array)
            {
                for edge in edges {
                    let Some(node) = edge.get("node") else {
                        continue;
                    };
                    let exchange_matches = exchange_id.is_none_or(|expected| {
                        node.get("exchange_id")
                            .and_then(Value::as_str)
                            .is_some_and(|value| value == expected)
                    });
                    let product_matches = product_id.is_none_or(|expected| {
                        node.get("product_id")
                            .and_then(Value::as_str)
                            .is_some_and(|value| value == expected)
                    });
                    if exchange_matches
                        && product_matches
                        && let Some(instrument_id) =
                            node.get("instrument_id").and_then(Value::as_str)
                    {
                        items.push(instrument_id.to_string());
                    }
                }
            }
        }
    }
    items
}

fn parse_query_options_result(
    payload: &Value,
    option_class: Option<&str>,
    exercise_year: Option<i32>,
    exercise_month: Option<i32>,
    strike_price: Option<f64>,
    expired: Option<bool>,
    has_a: Option<bool>,
) -> Vec<String> {
    let mut options = Vec::new();
    if let Some(result) = payload.get("result")
        && let Some(symbols) = result.get("multi_symbol_info").and_then(Value::as_array)
    {
        for symbol in symbols {
            if let Some(derivatives) = symbol.get("derivatives")
                && let Some(edges) = derivatives.get("edges").and_then(Value::as_array)
            {
                for edge in edges {
                    let Some(node) = edge.get("node") else {
                        continue;
                    };
                    let mut matches = true;
                    if let Some(option_class) = option_class {
                        matches = matches
                            && node
                                .get("call_or_put")
                                .and_then(Value::as_str)
                                .is_some_and(|value| value == option_class);
                    }
                    if let Some(exercise_year) = exercise_year {
                        matches = matches
                            && timestamp_nano_to_datetime(
                                node.get("last_exercise_datetime").and_then(Value::as_i64),
                            )
                            .is_some_and(|datetime| datetime.year() == exercise_year);
                    }
                    if let Some(exercise_month) = exercise_month {
                        matches = matches
                            && timestamp_nano_to_datetime(
                                node.get("last_exercise_datetime").and_then(Value::as_i64),
                            )
                            .is_some_and(|datetime| datetime.month() as i32 == exercise_month);
                    }
                    if let Some(strike_price) = strike_price {
                        matches = matches
                            && node
                                .get("strike_price")
                                .and_then(Value::as_f64)
                                .is_some_and(|value| (value - strike_price).abs() < f64::EPSILON);
                    }
                    if let Some(expired) = expired {
                        matches = matches
                            && node
                                .get("expired")
                                .and_then(Value::as_bool)
                                .is_some_and(|value| value == expired);
                    }
                    if let Some(has_a) = has_a {
                        let a_count = node
                            .get("english_name")
                            .and_then(Value::as_str)
                            .map(|text| text.matches('A').count())
                            .unwrap_or_default();
                        matches = matches && ((has_a && a_count > 0) || (!has_a && a_count == 0));
                    }
                    if matches
                        && let Some(instrument_id) =
                            node.get("instrument_id").and_then(Value::as_str)
                    {
                        options.push(instrument_id.to_string());
                    }
                }
            }
        }
    }
    options
}

fn parse_query_symbol_info_quotes(
    payload: &Value,
    symbol_list: &[String],
    current_ts: i64,
) -> Result<Vec<Quote>> {
    let mut quotes: HashMap<String, Map<String, Value>> = HashMap::new();
    let mut combine_leg1: HashMap<String, String> = HashMap::new();

    if let Some(result) = payload.get("result")
        && let Some(symbols) = result.get("multi_symbol_info").and_then(Value::as_array)
    {
        for symbol in symbols {
            let Some(symbol) = symbol.as_object() else {
                continue;
            };
            let Some(instrument_id) = symbol.get("instrument_id").and_then(Value::as_str) else {
                continue;
            };

            let mut underlying_nodes: Vec<(String, Map<String, Value>)> = Vec::new();
            let mut entry = quotes.remove(instrument_id).unwrap_or_default();
            update_symbol_info_map(&mut entry, symbol);

            if let Some(leg1) = symbol
                .get("leg1")
                .and_then(|value| value.get("instrument_id"))
                .and_then(Value::as_str)
            {
                combine_leg1.insert(instrument_id.to_string(), leg1.to_string());
            }

            if let Some(underlying) = symbol.get("underlying")
                && let Some(edges) = underlying.get("edges").and_then(Value::as_array)
            {
                for edge in edges {
                    if let Some(node) = edge.get("node").and_then(Value::as_object)
                        && let Some(underlying_id) =
                            node.get("instrument_id").and_then(Value::as_str)
                    {
                        entry.insert(
                            "underlying_symbol".to_string(),
                            Value::String(underlying_id.to_string()),
                        );
                        underlying_nodes.push((underlying_id.to_string(), node.clone()));
                    }
                }
            }

            quotes.insert(instrument_id.to_string(), entry);

            for (underlying_id, underlying_node) in underlying_nodes {
                let mut underlying_entry = quotes.remove(&underlying_id).unwrap_or_default();
                update_symbol_info_map(&mut underlying_entry, &underlying_node);
                quotes.insert(underlying_id, underlying_entry);
            }
        }
    }

    let mut leg1_volume_map = HashMap::new();
    for (symbol, quote) in &quotes {
        if let Some(volume_multiple) = quote.get("volume_multiple") {
            leg1_volume_map.insert(symbol.clone(), volume_multiple.clone());
        }
    }

    for (symbol, leg1_symbol) in combine_leg1 {
        let leg1_volume = leg1_volume_map.get(&leg1_symbol).cloned();
        if let Some(combine) = quotes.get_mut(&symbol) {
            let volume_missing = combine
                .get("volume_multiple")
                .and_then(Value::as_i64)
                .unwrap_or_default()
                == 0;
            if volume_missing && let Some(leg1_volume) = leg1_volume {
                combine.insert("volume_multiple".to_string(), leg1_volume);
            }
        }
    }

    let mut underlying_delivery = HashMap::new();
    for (symbol, quote) in &quotes {
        if let (Some(delivery_year), Some(delivery_month)) =
            (quote.get("delivery_year"), quote.get("delivery_month"))
        {
            underlying_delivery.insert(
                symbol.clone(),
                (delivery_year.clone(), delivery_month.clone()),
            );
        }
    }

    let offset =
        FixedOffset::east_opt(8 * 3600).ok_or_else(|| validation("failed to build CST offset"))?;
    for quote in quotes.values_mut() {
        let ins_class = quote
            .get("ins_class")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        let exchange_id = quote
            .get("exchange_id")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        let underlying_symbol = quote
            .get("underlying_symbol")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        let last_exercise_datetime = quote.get("last_exercise_datetime").and_then(Value::as_i64);
        let expire_datetime = quote.get("expire_datetime").and_then(Value::as_i64);

        if let Some(ins_class) = ins_class
            && ins_class == "OPTION"
            && let Some(exchange_id) = exchange_id
        {
            if matches!(exchange_id.as_str(), "DCE" | "CZCE" | "SHFE" | "GFEX")
                && let Some(underlying_symbol) = &underlying_symbol
                && let Some((delivery_year, delivery_month)) =
                    underlying_delivery.get(underlying_symbol)
            {
                quote.insert("delivery_year".to_string(), delivery_year.clone());
                quote.insert("delivery_month".to_string(), delivery_month.clone());
            }
            if exchange_id == "CFFEX"
                && let Some(last_exercise_datetime) = last_exercise_datetime
                && let Some(datetime) = offset.timestamp_opt(last_exercise_datetime, 0).single()
            {
                quote.insert("delivery_year".to_string(), json!(datetime.year()));
                quote.insert("delivery_month".to_string(), json!(datetime.month() as i64));
            }
        }

        if let Some(expire_datetime) = expire_datetime
            && let (Some(expire_datetime), Some(current_datetime)) = (
                offset.timestamp_opt(expire_datetime, 0).single(),
                offset.timestamp_opt(current_ts, 0).single(),
            )
        {
            let days = (expire_datetime.date_naive() - current_datetime.date_naive()).num_days();
            quote.insert("expire_rest_days".to_string(), json!(days));
        }
    }

    let mut result = Vec::with_capacity(symbol_list.len());
    for symbol in symbol_list {
        let mut quote = Value::Object(quotes.remove(symbol).unwrap_or_else(|| {
            let mut missing = Map::new();
            missing.insert("instrument_id".to_string(), Value::String(symbol.clone()));
            missing
        }));
        strip_null_object_fields(&mut quote);
        let parsed = serde_json::from_value::<Quote>(quote).map_err(|error| {
            validation(format!(
                "failed to parse symbol info for `{symbol}`: {error}"
            ))
        })?;
        result.push(parsed);
    }
    Ok(result)
}

fn update_symbol_info_map(target: &mut Map<String, Value>, symbol: &Map<String, Value>) {
    copy_string(target, symbol, "class", "ins_class");
    copy_string(target, symbol, "instrument_id", "instrument_id");
    copy_string(target, symbol, "instrument_name", "instrument_name");
    copy_string(target, symbol, "exchange_id", "exchange_id");
    copy_string(target, symbol, "product_id", "product_id");
    copy_string(target, symbol, "product_short_name", "product_short_name");
    copy_string(target, symbol, "exercise_type", "exercise_type");
    copy_value(target, symbol, "price_tick", "price_tick");
    copy_value(target, symbol, "volume_multiple", "volume_multiple");
    copy_value(target, symbol, "index_multiple", "volume_multiple");
    copy_value(
        target,
        symbol,
        "max_limit_order_volume",
        "max_limit_order_volume",
    );
    copy_value(
        target,
        symbol,
        "max_market_order_volume",
        "max_market_order_volume",
    );
    copy_value(
        target,
        symbol,
        "min_limit_order_volume",
        "min_limit_order_volume",
    );
    copy_value(
        target,
        symbol,
        "min_market_order_volume",
        "min_market_order_volume",
    );
    copy_value(
        target,
        symbol,
        "open_max_limit_order_volume",
        "open_max_limit_order_volume",
    );
    copy_value(
        target,
        symbol,
        "open_max_market_order_volume",
        "open_max_market_order_volume",
    );
    copy_value(
        target,
        symbol,
        "open_min_limit_order_volume",
        "open_min_limit_order_volume",
    );
    copy_value(
        target,
        symbol,
        "open_min_market_order_volume",
        "open_min_market_order_volume",
    );
    copy_value(target, symbol, "strike_price", "strike_price");
    copy_value(target, symbol, "expired", "expired");
    copy_value(target, symbol, "upper_limit", "upper_limit");
    copy_value(target, symbol, "lower_limit", "lower_limit");
    copy_value(target, symbol, "pre_open_interest", "pre_open_interest");
    copy_value(target, symbol, "pre_close", "pre_close");
    copy_value(target, symbol, "price_decs", "price_decs");
    copy_value(target, symbol, "delivery_year", "delivery_year");
    copy_value(target, symbol, "delivery_month", "delivery_month");
    copy_value(target, symbol, "settlement_price", "pre_settlement");
    if let Some(expire_datetime) = symbol
        .get("expire_datetime")
        .and_then(timestamp_nano_to_seconds)
    {
        target.insert("expire_datetime".to_string(), json!(expire_datetime));
    }
    if let Some(last_exercise_datetime) =
        symbol.get("last_exercise_datetime").and_then(Value::as_i64)
    {
        let seconds = last_exercise_datetime / 1_000_000_000;
        target.insert("last_exercise_datetime".to_string(), json!(seconds));
        if let Some(datetime) = timestamp_nano_to_datetime(Some(last_exercise_datetime)) {
            target.insert("exercise_year".to_string(), json!(datetime.year()));
            target.insert("exercise_month".to_string(), json!(datetime.month() as i64));
        }
    }
    copy_string(target, symbol, "call_or_put", "option_class");
    if let Some(trading_time) = symbol.get("trading_time") {
        target.insert("trading_time".to_string(), trading_time.clone());
    }
}

fn copy_string(target: &mut Map<String, Value>, source: &Map<String, Value>, from: &str, to: &str) {
    if let Some(value) = source.get(from).and_then(Value::as_str) {
        target.insert(to.to_string(), Value::String(value.to_string()));
    }
}

fn copy_value(target: &mut Map<String, Value>, source: &Map<String, Value>, from: &str, to: &str) {
    if let Some(value) = source.get(from) {
        target.insert(to.to_string(), value.clone());
    }
}

fn timestamp_nano_to_seconds(value: &Value) -> Option<i64> {
    if let Some(value) = value.as_i64() {
        return Some(value / 1_000_000_000);
    }
    value
        .as_f64()
        .map(|value| (value / 1_000_000_000.0).round() as i64)
}

fn timestamp_nano_to_datetime(value: Option<i64>) -> Option<chrono::DateTime<Utc>> {
    let value = value?;
    chrono::DateTime::<Utc>::from_timestamp(value / 1_000_000_000, (value % 1_000_000_000) as u32)
}

fn strip_null_object_fields(value: &mut Value) {
    if let Some(object) = value.as_object_mut() {
        object.retain(|_, value| !value.is_null());
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use serde_json::json;

    use super::{
        parse_query_cont_quotes_result, parse_query_options_result, parse_query_quotes_result,
        parse_query_symbol_info_quotes,
    };

    #[test]
    fn parse_quotes_filters_exchange_for_future_index_queries() {
        let payload = json!({
            "result": {
                "multi_symbol_info": [
                    { "instrument_id": "SHFE.cu2605" },
                    { "instrument_id": "DCE.m2609" }
                ]
            }
        });

        assert_eq!(
            parse_query_quotes_result(&payload, Some("SHFE")),
            vec!["SHFE.cu2605"]
        );
        assert_eq!(
            parse_query_quotes_result(&payload, None),
            vec!["SHFE.cu2605", "DCE.m2609"]
        );
    }

    #[test]
    fn parse_cont_quotes_filters_exchange_and_product() {
        let payload = json!({
            "result": {
                "multi_symbol_info": [
                    {
                        "underlying": {
                            "edges": [
                                { "node": { "instrument_id": "SHFE.cu2605", "exchange_id": "SHFE", "product_id": "cu" } },
                                { "node": { "instrument_id": "DCE.m2609", "exchange_id": "DCE", "product_id": "m" } }
                            ]
                        }
                    }
                ]
            }
        });

        assert_eq!(
            parse_query_cont_quotes_result(&payload, Some("SHFE"), None),
            vec!["SHFE.cu2605"]
        );
        assert_eq!(
            parse_query_cont_quotes_result(&payload, None, Some("m")),
            vec!["DCE.m2609"]
        );
    }

    #[test]
    fn parse_options_filters_requested_dimensions() {
        let ts = Utc
            .with_ymd_and_hms(2026, 12, 1, 0, 0, 0)
            .unwrap()
            .timestamp()
            * 1_000_000_000;
        let payload = json!({
            "result": {
                "multi_symbol_info": [
                    {
                        "derivatives": {
                            "edges": [
                                { "node": { "instrument_id": "SHFE.cu2605C3000", "english_name": "AAA", "call_or_put": "CALL", "strike_price": 3000.0, "expired": false, "last_exercise_datetime": ts } },
                                { "node": { "instrument_id": "SHFE.cu2605P3100", "english_name": "BBB", "call_or_put": "PUT", "strike_price": 3100.0, "expired": true, "last_exercise_datetime": ts } }
                            ]
                        }
                    }
                ]
            }
        });

        assert_eq!(
            parse_query_options_result(&payload, Some("CALL"), None, None, None, None, None),
            vec!["SHFE.cu2605C3000"]
        );
        assert_eq!(
            parse_query_options_result(&payload, None, Some(2026), Some(12), None, None, None)
                .len(),
            2
        );
        assert_eq!(
            parse_query_options_result(&payload, None, None, None, Some(3100.0), None, None),
            vec!["SHFE.cu2605P3100"]
        );
        assert_eq!(
            parse_query_options_result(&payload, None, None, None, None, None, Some(true)),
            vec!["SHFE.cu2605C3000"]
        );
    }

    #[test]
    fn parse_symbol_info_maps_graphql_payload_to_quote_schema() {
        let expire_ts = 1_831_801_600_i64 * 1_000_000_000;
        let exercise_ts = 1_814_492_800_i64 * 1_000_000_000;
        let payload = json!({
            "result": {
                "multi_symbol_info": [
                    {
                        "instrument_id": "SHFE.cu2605",
                        "class": "FUTURE",
                        "exchange_id": "SHFE",
                        "product_id": "cu",
                        "instrument_name": "沪铜",
                        "price_tick": 10.0,
                        "volume_multiple": 5,
                        "delivery_year": 2026,
                        "delivery_month": 5,
                        "expire_datetime": expire_ts,
                        "max_limit_order_volume": 100,
                        "max_market_order_volume": 50,
                        "min_limit_order_volume": 2,
                        "min_market_order_volume": 1,
                        "trading_time": {
                            "day": [["09:00:00", "10:15:00"]],
                            "night": [["21:00:00", "23:00:00"]]
                        }
                    },
                    {
                        "instrument_id": "SHFE.cu2605C3000",
                        "class": "OPTION",
                        "exchange_id": "SHFE",
                        "call_or_put": "CALL",
                        "strike_price": 3000.0,
                        "open_min_market_order_volume": 3,
                        "open_min_limit_order_volume": 5,
                        "open_max_market_order_volume": 9,
                        "open_max_limit_order_volume": 11,
                        "expired": false,
                        "last_exercise_datetime": exercise_ts,
                        "expire_datetime": expire_ts,
                        "underlying": {
                            "edges": [
                                {
                                    "node": {
                                        "instrument_id": "SHFE.cu2605",
                                        "class": "FUTURE",
                                        "exchange_id": "SHFE",
                                        "product_id": "cu",
                                        "delivery_year": 2026,
                                        "delivery_month": 5,
                                        "volume_multiple": 5
                                    }
                                }
                            ]
                        }
                    }
                ]
            }
        });

        let quotes = parse_query_symbol_info_quotes(
            &payload,
            &["SHFE.cu2605".to_string(), "SHFE.cu2605C3000".to_string()],
            1_700_000_000,
        )
        .unwrap();

        assert_eq!(quotes.len(), 2);
        assert_eq!(quotes[0].instrument_id, "SHFE.cu2605");
        assert_eq!(quotes[0].ins_class, "FUTURE");
        assert_eq!(quotes[0].trading_time.day[0][0], "09:00:00");
        assert_eq!(quotes[1].instrument_id, "SHFE.cu2605C3000");
        assert_eq!(quotes[1].option_class, "CALL");
        assert_eq!(quotes[1].underlying_symbol, "SHFE.cu2605");
        assert_eq!(quotes[1].delivery_year, 2026);
        assert_eq!(quotes[1].delivery_month, 5);
        assert_eq!(quotes[1].exercise_year, 2027);
        assert_eq!(quotes[1].exercise_month, 7);
        assert!(quotes[1].expire_rest_days.is_some());
    }
}

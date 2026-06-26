use std::collections::{HashMap, HashSet};

use chrono::{Datelike, FixedOffset, TimeZone, Utc};
use serde_json::{Map, Value, json};

use crate::SymbolInfo;
use crate::direct_query::{
    AllLevelOptionQuery, AtmOptionQuery, FinanceOptionLevelQuery, OptionLevelQuotes,
    OptionQueryFilter,
};
use crate::error::Result;

use super::helpers::validation;

pub(super) struct MetadataSymbolDecoder {
    current_ts: i64,
}

#[derive(Debug, Clone)]
struct OptionNode {
    instrument_id: String,
    english_name: String,
    call_or_put: String,
    strike_price: f64,
    expired: bool,
    last_exercise_datetime: i64,
    exercise_year: i32,
    exercise_month: i32,
}

#[derive(Clone, Copy, Debug)]
enum BisectPriority {
    Left,
    Right,
}

impl MetadataSymbolDecoder {
    pub(super) fn new(current_ts: i64) -> Self {
        Self { current_ts }
    }

    pub(super) fn decode_symbol_infos(
        &self,
        payload: &Value,
        symbol_list: &[String],
    ) -> Result<Vec<SymbolInfo>> {
        let mut quotes: HashMap<String, Map<String, Value>> = HashMap::new();
        let mut combine_leg1: HashMap<String, String> = HashMap::new();

        if let Some(result) = payload.get("result")
            && let Some(symbols) = result.get("multi_symbol_info").and_then(Value::as_array)
        {
            for symbol in symbols {
                let Some(symbol) = symbol.as_object() else {
                    continue;
                };
                let Some(instrument_id) = symbol.get("instrument_id").and_then(Value::as_str)
                else {
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

        fill_combine_leg_volume(&mut quotes, combine_leg1);
        self.repair_symbol_info_maps(&mut quotes)?;

        let mut result = Vec::with_capacity(symbol_list.len());
        for symbol in symbol_list {
            let mut quote = Value::Object(quotes.remove(symbol).unwrap_or_else(|| {
                let mut missing = Map::new();
                missing.insert("instrument_id".to_string(), Value::String(symbol.clone()));
                missing
            }));
            strip_null_object_fields(&mut quote);
            let Some(map) = quote.as_object() else {
                return Err(validation(format!(
                    "failed to parse symbol info for `{symbol}`: expected object"
                )));
            };
            result.push(SymbolInfo::from_metadata_map(symbol, map)?);
        }
        Ok(result)
    }

    pub(super) fn decode_option_symbols(
        &self,
        payload: &Value,
        filter: &OptionQueryFilter,
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
                        if option_symbol_matches_filter(node, filter)
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

    pub(super) fn decode_atm_options(
        &self,
        payload: &Value,
        query: &AtmOptionQuery,
    ) -> Result<Vec<Option<String>>> {
        let mut nodes = self.filtered_option_nodes(
            payload,
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
            &query.option_class,
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

    pub(super) fn decode_option_levels(
        &self,
        payload: &Value,
        query: &AllLevelOptionQuery,
    ) -> Result<OptionLevelQuotes> {
        let nodes = self.filtered_option_nodes(
            payload,
            Some(query.option_class.as_str()),
            query.exercise_year,
            query.exercise_month,
            query.has_a,
            None,
        );
        option_levels_from_nodes(nodes, query.underlying_price, &query.option_class)
    }

    pub(super) fn decode_finance_option_levels(
        &self,
        payload: &Value,
        query: &FinanceOptionLevelQuery,
    ) -> Result<OptionLevelQuotes> {
        let nodes = self.filtered_option_nodes(
            payload,
            Some(query.option_class.as_str()),
            None,
            None,
            query.has_a,
            Some(&query.nearbys),
        );
        option_levels_from_nodes(nodes, query.underlying_price, &query.option_class)
    }

    fn filtered_option_nodes(
        &self,
        payload: &Value,
        option_class: Option<&str>,
        exercise_year: Option<i32>,
        exercise_month: Option<i32>,
        has_a: Option<bool>,
        nearbys: Option<&[i32]>,
    ) -> Vec<OptionNode> {
        filter_option_nodes(
            parse_option_nodes(payload),
            option_class,
            exercise_year,
            exercise_month,
            has_a,
            nearbys,
        )
    }

    fn repair_symbol_info_maps(
        &self,
        quotes: &mut HashMap<String, Map<String, Value>>,
    ) -> Result<()> {
        let mut underlying_delivery = HashMap::new();
        for (symbol, quote) in quotes.iter() {
            if let (Some(delivery_year), Some(delivery_month)) =
                (quote.get("delivery_year"), quote.get("delivery_month"))
            {
                underlying_delivery.insert(
                    symbol.clone(),
                    (delivery_year.clone(), delivery_month.clone()),
                );
            }
        }

        let offset = FixedOffset::east_opt(8 * 3600)
            .ok_or_else(|| validation("failed to build CST offset"))?;
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
            let last_exercise_datetime =
                quote.get("last_exercise_datetime").and_then(Value::as_i64);
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
                    offset.timestamp_opt(self.current_ts, 0).single(),
                )
            {
                let days =
                    (expire_datetime.date_naive() - current_datetime.date_naive()).num_days();
                quote.insert("expire_rest_days".to_string(), json!(days));
            }
        }
        Ok(())
    }
}

fn option_symbol_matches_filter(node: &Value, filter: &OptionQueryFilter) -> bool {
    let mut matches = true;
    if let Some(option_class) = filter.option_class.as_deref() {
        matches = matches
            && node
                .get("call_or_put")
                .and_then(Value::as_str)
                .is_some_and(|value| value == option_class);
    }
    if let Some(exercise_year) = filter.exercise_year {
        matches = matches
            && timestamp_nano_to_datetime(
                node.get("last_exercise_datetime").and_then(Value::as_i64),
            )
            .is_some_and(|datetime| datetime.year() == exercise_year);
    }
    if let Some(exercise_month) = filter.exercise_month {
        matches = matches
            && timestamp_nano_to_datetime(
                node.get("last_exercise_datetime").and_then(Value::as_i64),
            )
            .is_some_and(|datetime| datetime.month() as i32 == exercise_month);
    }
    if let Some(strike_price) = filter.strike_price {
        matches = matches
            && node
                .get("strike_price")
                .and_then(Value::as_f64)
                .is_some_and(|value| (value - strike_price).abs() < f64::EPSILON);
    }
    if let Some(expired) = filter.expired {
        matches = matches
            && node
                .get("expired")
                .and_then(Value::as_bool)
                .is_some_and(|value| value == expired);
    }
    if let Some(has_a) = filter.has_a {
        let a_count = node
            .get("english_name")
            .and_then(Value::as_str)
            .map(|text| text.matches('A').count())
            .unwrap_or_default();
        matches = matches && ((has_a && a_count > 0) || (!has_a && a_count == 0));
    }
    matches
}

fn parse_option_nodes(payload: &Value) -> Vec<OptionNode> {
    let mut nodes = Vec::new();
    if let Some(result) = payload.get("result")
        && let Some(symbols) = result.get("multi_symbol_info").and_then(Value::as_array)
    {
        for symbol in symbols {
            if let Some(derivatives) = symbol.get("derivatives")
                && let Some(edges) = derivatives.get("edges").and_then(Value::as_array)
            {
                for edge in edges {
                    let Some(node) = edge.get("node").and_then(Value::as_object) else {
                        continue;
                    };
                    let Some(instrument_id) = node.get("instrument_id").and_then(Value::as_str)
                    else {
                        continue;
                    };
                    let Some(call_or_put) = node.get("call_or_put").and_then(Value::as_str) else {
                        continue;
                    };
                    let Some(strike_price) = node.get("strike_price").and_then(Value::as_f64)
                    else {
                        continue;
                    };
                    let Some(last_exercise_datetime) =
                        node.get("last_exercise_datetime").and_then(Value::as_i64)
                    else {
                        continue;
                    };
                    let Some(datetime) = timestamp_nano_to_datetime(Some(last_exercise_datetime))
                    else {
                        continue;
                    };
                    nodes.push(OptionNode {
                        instrument_id: instrument_id.to_string(),
                        english_name: node
                            .get("english_name")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string(),
                        call_or_put: call_or_put.to_string(),
                        strike_price,
                        expired: node
                            .get("expired")
                            .and_then(Value::as_bool)
                            .unwrap_or(false),
                        last_exercise_datetime,
                        exercise_year: datetime.year(),
                        exercise_month: datetime.month() as i32,
                    });
                }
            }
        }
    }
    nodes
}

fn filter_option_nodes(
    options: Vec<OptionNode>,
    option_class: Option<&str>,
    exercise_year: Option<i32>,
    exercise_month: Option<i32>,
    has_a: Option<bool>,
    nearbys: Option<&[i32]>,
) -> Vec<OptionNode> {
    let mut filtered: Vec<OptionNode> = options
        .into_iter()
        .filter(|option| {
            let mut matches = true;
            if let Some(option_class) = option_class {
                matches = matches && option.call_or_put == option_class;
            }
            if let Some(has_a) = has_a {
                let count = option.english_name.matches('A').count();
                matches = matches && ((has_a && count > 0) || (!has_a && count == 0));
            }
            if let Some(exercise_year) = exercise_year {
                matches = matches && option.exercise_year == exercise_year;
            }
            if let Some(exercise_month) = exercise_month {
                matches = matches && option.exercise_month == exercise_month;
            }
            matches
        })
        .collect();

    if let Some(nearbys) = nearbys {
        filtered.retain(|option| !option.expired);
        let mut expiries: Vec<i64> = filtered
            .iter()
            .map(|option| option.last_exercise_datetime)
            .collect();
        expiries.sort_unstable();
        expiries.dedup();
        let selected: HashSet<i64> = nearbys
            .iter()
            .filter_map(|index| expiries.get(*index as usize).copied())
            .collect();
        filtered.retain(|option| selected.contains(&option.last_exercise_datetime));
    }

    filtered
}

fn option_levels_from_nodes(
    mut nodes: Vec<OptionNode>,
    underlying_price: f64,
    option_class: &str,
) -> Result<OptionLevelQuotes> {
    if nodes.is_empty() {
        return Ok(OptionLevelQuotes::default());
    }
    let atm_index = sort_options_and_get_atm_index(&mut nodes, underlying_price, option_class)?;
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

fn sort_options_and_get_atm_index(
    options: &mut [OptionNode],
    underlying_price: f64,
    option_class: &str,
) -> Result<usize> {
    if options.is_empty() {
        return Err(validation("options must not be empty"));
    }

    options.sort_by(|left, right| {
        left.last_exercise_datetime
            .cmp(&right.last_exercise_datetime)
            .then_with(|| {
                left.strike_price
                    .partial_cmp(&right.strike_price)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    });

    let strikes: Vec<f64> = options.iter().map(|option| option.strike_price).collect();
    let priority = if option_class == "CALL" {
        BisectPriority::Right
    } else {
        BisectPriority::Left
    };
    let atm_index = bisect_value_index(&strikes, underlying_price, priority);
    let atm_instrument_id = options
        .iter()
        .find(|option| option.strike_price == strikes[atm_index])
        .map(|option| option.instrument_id.clone())
        .ok_or_else(|| validation("failed to locate ATM option instrument"))?;

    if option_class == "PUT" {
        options.sort_by(|left, right| {
            right
                .strike_price
                .partial_cmp(&left.strike_price)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    options
        .iter()
        .position(|option| option.instrument_id == atm_instrument_id)
        .ok_or_else(|| validation("failed to locate ATM option index"))
}

fn bisect_value_index(values: &[f64], target: f64, priority: BisectPriority) -> usize {
    let insert_index = values.partition_point(|value| *value <= target);
    if 0 < insert_index && insert_index < values.len() {
        let left_distance = target - values[insert_index - 1];
        let right_distance = values[insert_index] - target;
        if left_distance == right_distance {
            match priority {
                BisectPriority::Left => insert_index - 1,
                BisectPriority::Right => insert_index,
            }
        } else if left_distance < right_distance {
            insert_index - 1
        } else {
            insert_index
        }
    } else if insert_index == 0 {
        0
    } else {
        values.len().saturating_sub(1)
    }
}

fn fill_combine_leg_volume(
    quotes: &mut HashMap<String, Map<String, Value>>,
    combine_leg1: HashMap<String, String>,
) {
    let mut leg1_volume_map = HashMap::new();
    for (symbol, quote) in quotes.iter() {
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
}

fn update_symbol_info_map(target: &mut Map<String, Value>, symbol: &Map<String, Value>) {
    copy_string(target, symbol, "class", "ins_class");
    copy_string(target, symbol, "instrument_id", "instrument_id");
    copy_string(target, symbol, "instrument_name", "instrument_name");
    copy_string(target, symbol, "exchange_id", "exchange_id");
    copy_string(target, symbol, "product_id", "product_id");
    copy_string(target, symbol, "product_short_name", "product_short_name");
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
    copy_value(target, symbol, "settlement_price", "pre_settlement");
    copy_value(target, symbol, "pre_open_interest", "pre_open_interest");
    copy_value(target, symbol, "pre_close", "pre_close");
    copy_value(target, symbol, "open_limit", "open_limit");
    copy_value(target, symbol, "delivery_year", "delivery_year");
    copy_value(target, symbol, "delivery_month", "delivery_month");
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

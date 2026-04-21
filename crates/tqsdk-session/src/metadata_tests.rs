use chrono::{Datelike, TimeZone, Utc};
use serde_json::json;

use super::helpers::{
    BisectPriority, OptionNode, bisect_value_index, filter_option_nodes,
    parse_query_cont_quotes_result, parse_query_options_result, parse_query_quotes_result,
    parse_query_symbol_info_quotes, sort_options_and_get_atm_index, timestamp_nano_to_datetime,
    validate_finance_nearbys, validate_finance_underlying, validate_price_levels,
};

fn make_option_for_test(
    instrument_id: &str,
    strike_price: f64,
    call_or_put: &str,
    last_exercise_datetime: i64,
    english_name: &str,
    expired: bool,
) -> OptionNode {
    let datetime = timestamp_nano_to_datetime(Some(last_exercise_datetime)).unwrap();
    OptionNode {
        instrument_id: instrument_id.to_string(),
        english_name: english_name.to_string(),
        call_or_put: call_or_put.to_string(),
        strike_price,
        expired,
        last_exercise_datetime,
        exercise_year: datetime.year(),
        exercise_month: datetime.month() as i32,
    }
}

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
        parse_query_options_result(&payload, None, Some(2026), Some(12), None, None, None).len(),
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

#[test]
fn bisect_value_index_prefers_virtual_side_on_equal_distance() {
    let values = vec![100.0, 110.0];
    assert_eq!(
        values[bisect_value_index(&values, 105.0, BisectPriority::Right)],
        110.0
    );
    assert_eq!(
        values[bisect_value_index(&values, 105.0, BisectPriority::Left)],
        100.0
    );
}

#[test]
fn sort_options_uses_virtual_contract_as_atm_when_distance_ties() {
    let ts = Utc
        .with_ymd_and_hms(2026, 12, 1, 0, 0, 0)
        .unwrap()
        .timestamp()
        * 1_000_000_000;

    let mut calls = vec![
        make_option_for_test("C90", 90.0, "CALL", ts, "", false),
        make_option_for_test("C110", 110.0, "CALL", ts, "", false),
    ];
    let call_index = sort_options_and_get_atm_index(&mut calls, 100.0, "CALL").unwrap();
    assert_eq!(calls[call_index].instrument_id, "C110");

    let mut puts = vec![
        make_option_for_test("P90", 90.0, "PUT", ts, "", false),
        make_option_for_test("P110", 110.0, "PUT", ts, "", false),
    ];
    let put_index = sort_options_and_get_atm_index(&mut puts, 100.0, "PUT").unwrap();
    assert_eq!(puts[put_index].instrument_id, "P90");
}

#[test]
fn filter_option_nodes_keeps_requested_nearbys() {
    let ts1 = Utc
        .with_ymd_and_hms(2026, 11, 1, 0, 0, 0)
        .unwrap()
        .timestamp()
        * 1_000_000_000;
    let ts2 = Utc
        .with_ymd_and_hms(2026, 12, 1, 0, 0, 0)
        .unwrap()
        .timestamp()
        * 1_000_000_000;
    let filtered = filter_option_nodes(
        vec![
            make_option_for_test("A1", 100.0, "CALL", ts1, "", false),
            make_option_for_test("A2", 110.0, "CALL", ts1, "", false),
            make_option_for_test("B1", 100.0, "CALL", ts2, "", false),
            make_option_for_test("B2", 110.0, "CALL", ts2, "", false),
        ],
        Some("CALL"),
        None,
        None,
        None,
        Some(&[1]),
    );
    let ids: std::collections::HashSet<String> = filtered
        .into_iter()
        .map(|option| option.instrument_id)
        .collect();
    assert!(ids.contains("B1"));
    assert!(ids.contains("B2"));
    assert!(!ids.contains("A1"));
    assert!(!ids.contains("A2"));
}

#[test]
fn finance_option_validations_match_expected_ranges() {
    validate_price_levels(&[-101]).unwrap_err();
    validate_finance_underlying("SHFE.au2605").unwrap_err();
    validate_finance_nearbys("SSE.000300", &[0, 5]).unwrap();
    validate_finance_nearbys("SSE.000300", &[6]).unwrap_err();
    validate_finance_nearbys("SSE.510300", &[0, 3]).unwrap();
    validate_finance_nearbys("SSE.510300", &[4]).unwrap_err();
}

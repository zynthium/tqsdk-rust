use chrono::{TimeZone, Utc};
use serde_json::{Value, json};

use crate::direct_query::{
    AllLevelOptionQuery, AtmOptionQuery, FinanceOptionLevelQuery, OptionQueryFilter,
};

use super::{
    build_query_cont_quotes_request, build_query_quotes_request,
    decoder::MetadataSymbolDecoder,
    helpers::{
        parse_query_cont_quotes_result, parse_query_quotes_result, validate_finance_nearbys,
        validate_finance_underlying, validate_option_class, validate_price_levels,
    },
};

#[test]
fn query_quotes_request_omits_unset_optional_filters() {
    let request =
        build_query_quotes_request(Some("FUTURE"), Some("SHFE"), Some("au"), Some(false), None)
            .unwrap();

    assert!(request.query.contains("$class_:[Class]"));
    assert!(request.query.contains("$exchange_id:[String]"));
    assert!(request.query.contains("$product_id:[String]"));
    assert!(request.query.contains("$expired:Boolean"));
    assert!(!request.query.contains("has_night"));
    assert_eq!(
        request.variables,
        json!({
            "class_": ["FUTURE"],
            "exchange_id": ["SHFE"],
            "product_id": ["au"],
            "expired": false,
        })
    );
    assert_eq!(request.target_exchange, None);
}

#[test]
fn query_cont_quotes_request_omits_has_night_when_unset() {
    let request = build_query_cont_quotes_request(None).unwrap();

    assert!(request.query.contains("$class_:[Class]"));
    assert!(!request.query.contains("has_night"));
    assert_eq!(request.variables, json!({ "class_": ["CONT"] }));
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
fn decoder_filters_option_symbols_by_requested_dimensions() {
    let payload = option_payload();
    let decoder = MetadataSymbolDecoder::new(1_700_000_000);

    let call_filter = OptionQueryFilter {
        option_class: Some("CALL".to_string()),
        exercise_year: Some(2026),
        exercise_month: Some(5),
        ..OptionQueryFilter::default()
    };
    assert_eq!(
        decoder.decode_option_symbols(&payload, &call_filter),
        vec!["C1".to_string(), "C2".to_string(), "C3".to_string()]
    );

    let put_filter = OptionQueryFilter {
        option_class: Some("PUT".to_string()),
        strike_price: Some(590.0),
        expired: Some(false),
        ..OptionQueryFilter::default()
    };
    assert_eq!(
        decoder.decode_option_symbols(&payload, &put_filter),
        vec!["P1".to_string()]
    );

    let has_a_filter = OptionQueryFilter {
        has_a: Some(true),
        ..OptionQueryFilter::default()
    };
    assert_eq!(
        decoder.decode_option_symbols(&payload, &has_a_filter),
        vec!["C2".to_string()]
    );
}

#[test]
fn decoder_maps_graphql_payload_to_symbol_info_schema() {
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
                    "settlement_price": 72_000.0,
                    "open_limit": 2_000,
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

    let infos = MetadataSymbolDecoder::new(1_700_000_000)
        .decode_symbol_infos(
            &payload,
            &["SHFE.cu2605".to_string(), "SHFE.cu2605C3000".to_string()],
        )
        .unwrap();

    assert_eq!(infos.len(), 2);
    assert_eq!(infos[0].instrument_id.as_str(), "SHFE.cu2605");
    assert_eq!(infos[0].class, crate::InstrumentClass::Future);
    assert_eq!(infos[0].price_tick, Some(10.0));
    assert_eq!(infos[0].volume_multiple, Some(5));
    assert_eq!(infos[0].open_limit, Some(2_000));
    assert_eq!(infos[0].pre_settlement, Some(72_000.0));
    assert_eq!(infos[0].expire_datetime_secs, Some(1_831_801_600));
    assert_eq!(infos[0].trading_time.day[0][0], "09:00:00");
    assert_eq!(infos[0].trading_time.night[0][0], "21:00:00");
    assert_eq!(infos[1].instrument_id.as_str(), "SHFE.cu2605C3000");
    assert_eq!(infos[1].option_class.as_deref(), Some("CALL"));
    assert_eq!(
        infos[1]
            .underlying_symbol
            .as_ref()
            .map(|symbol| symbol.as_str()),
        Some("SHFE.cu2605")
    );
    assert_eq!(infos[1].delivery_year, Some(2026));
    assert_eq!(infos[1].delivery_month, Some(5));
    assert_eq!(infos[1].last_exercise_datetime_secs, Some(1_814_492_800));
    assert_eq!(infos[1].exercise_year, Some(2027));
    assert_eq!(infos[1].exercise_month, Some(7));
    assert!(infos[1].expire_rest_days.is_some());
}

#[test]
fn decoder_uses_virtual_contract_side_as_atm_when_distance_ties() {
    let payload = json!({
        "result": {
            "multi_symbol_info": [{
                "derivatives": {
                    "edges": [
                        { "node": option_node("C90", 90.0, "CALL", expiry_nanos(2026, 12), "", false) },
                        { "node": option_node("C110", 110.0, "CALL", expiry_nanos(2026, 12), "", false) },
                        { "node": option_node("P90", 90.0, "PUT", expiry_nanos(2026, 12), "", false) },
                        { "node": option_node("P110", 110.0, "PUT", expiry_nanos(2026, 12), "", false) }
                    ]
                }
            }]
        }
    });
    let decoder = MetadataSymbolDecoder::new(1_700_000_000);

    let calls = decoder
        .decode_atm_options(&payload, &AtmOptionQuery::new(100.0, vec![0], "CALL"))
        .unwrap();
    assert_eq!(calls, vec![Some("C110".to_string())]);

    let puts = decoder
        .decode_atm_options(&payload, &AtmOptionQuery::new(100.0, vec![0], "PUT"))
        .unwrap();
    assert_eq!(puts, vec![Some("P90".to_string())]);
}

#[test]
fn decoder_groups_all_level_options_by_moneyness() {
    let payload = option_payload();
    let mut query = AllLevelOptionQuery::new(595.0, "CALL");
    query.exercise_year = Some(2026);
    query.exercise_month = Some(5);

    let quotes = MetadataSymbolDecoder::new(1_700_000_000)
        .decode_option_levels(&payload, &query)
        .unwrap();

    assert_eq!(quotes.in_money, vec!["C1"]);
    assert_eq!(quotes.at_money, vec!["C2"]);
    assert_eq!(quotes.out_of_money, vec!["C3"]);
}

#[test]
fn decoder_keeps_requested_finance_option_nearbys() {
    let payload = json!({
        "result": {
            "multi_symbol_info": [{
                "derivatives": {
                    "edges": [
                        { "node": option_node("A1", 100.0, "CALL", expiry_nanos(2026, 11), "", false) },
                        { "node": option_node("A2", 110.0, "CALL", expiry_nanos(2026, 11), "", false) },
                        { "node": option_node("B1", 100.0, "CALL", expiry_nanos(2026, 12), "", false) },
                        { "node": option_node("B2", 110.0, "CALL", expiry_nanos(2026, 12), "", false) }
                    ]
                }
            }]
        }
    });

    let quotes = MetadataSymbolDecoder::new(1_700_000_000)
        .decode_finance_option_levels(
            &payload,
            &FinanceOptionLevelQuery::new(105.0, "CALL", vec![1]),
        )
        .unwrap();

    assert_eq!(quotes.in_money, vec!["B1"]);
    assert_eq!(quotes.at_money, vec!["B2"]);
    assert!(quotes.out_of_money.is_empty());
}

#[test]
fn finance_option_validations_match_expected_ranges() {
    validate_option_class("STRADDLE").unwrap_err();
    validate_price_levels(&[-101]).unwrap_err();
    validate_finance_underlying("SHFE.au2605").unwrap_err();
    validate_finance_nearbys("SSE.000300", &[0, 5]).unwrap();
    validate_finance_nearbys("SSE.000300", &[6]).unwrap_err();
    validate_finance_nearbys("SSE.510300", &[0, 3]).unwrap();
    validate_finance_nearbys("SSE.510300", &[4]).unwrap_err();
}

fn option_payload() -> Value {
    let may_2026 = expiry_nanos(2026, 5);
    json!({
        "result": {
            "multi_symbol_info": [{
                "derivatives": {
                    "edges": [
                        { "node": option_node("C1", 590.0, "CALL", may_2026, "Plain Call", true) },
                        { "node": option_node("C2", 600.0, "CALL", may_2026, "Alpha Call A", false) },
                        { "node": option_node("C3", 610.0, "CALL", may_2026, "Plain Call", false) },
                        { "node": option_node("P1", 590.0, "PUT", may_2026, "Plain Put", false) },
                        { "node": {
                            "instrument_id": "BROKEN",
                            "call_or_put": "CALL"
                        }}
                    ]
                }
            }]
        }
    })
}

fn option_node(
    instrument_id: &str,
    strike_price: f64,
    call_or_put: &str,
    last_exercise_datetime: i64,
    english_name: &str,
    expired: bool,
) -> Value {
    json!({
        "instrument_id": instrument_id,
        "english_name": english_name,
        "call_or_put": call_or_put,
        "strike_price": strike_price,
        "expired": expired,
        "last_exercise_datetime": last_exercise_datetime,
    })
}

fn expiry_nanos(year: i32, month: u32) -> i64 {
    Utc.with_ymd_and_hms(year, month, 1, 0, 0, 0)
        .unwrap()
        .timestamp()
        * 1_000_000_000
}

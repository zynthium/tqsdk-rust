use serde_json::Value;

use crate::error::{Result, SessionFacadeError};

pub(super) fn validation(message: impl Into<String>) -> SessionFacadeError {
    SessionFacadeError::from(tqsdk_core::ContractError::validation(message))
}

pub(super) fn non_empty_str<'a>(value: Option<&'a str>, name: &str) -> Result<Option<&'a str>> {
    match value {
        Some("") => Err(validation(format!("{name} must not be empty"))),
        other => Ok(other),
    }
}

pub(super) fn parse_query_quotes_result(
    payload: &Value,
    target_exchange: Option<&str>,
) -> Vec<String> {
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

pub(super) fn parse_query_cont_quotes_result(
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

pub(super) fn validate_option_class(option_class: &str) -> Result<()> {
    if matches!(option_class, "CALL" | "PUT") {
        Ok(())
    } else {
        Err(validation("option_class must be either `CALL` or `PUT`"))
    }
}

pub(super) fn validate_price_levels(price_levels: &[i32]) -> Result<()> {
    if price_levels
        .iter()
        .all(|price_level| (-100..=100).contains(price_level))
    {
        Ok(())
    } else {
        Err(validation(
            "price_levels must only contain integers in [-100, 100]",
        ))
    }
}

pub(super) fn validate_finance_underlying(underlying_symbol: &str) -> Result<()> {
    const ALLOWED: &[&str] = &[
        "SSE.000300",
        "SSE.510050",
        "SSE.510300",
        "SZSE.159919",
        "SZSE.159915",
        "SZSE.159922",
        "SSE.510500",
        "SSE.000016",
        "SSE.000852",
    ];
    if ALLOWED.contains(&underlying_symbol) {
        Ok(())
    } else {
        Err(validation("unsupported finance option underlying"))
    }
}

pub(super) fn validate_finance_nearbys(underlying_symbol: &str, nearbys: &[i32]) -> Result<()> {
    let is_index = matches!(
        underlying_symbol,
        "SSE.000300" | "SSE.000852" | "SSE.000016"
    );
    if is_index {
        if nearbys.iter().all(|value| matches!(value, 0..=5)) {
            Ok(())
        } else {
            Err(validation(format!(
                "index option nearbys for `{underlying_symbol}` must be in [0, 5]"
            )))
        }
    } else if nearbys.iter().all(|value| matches!(value, 0..=3)) {
        Ok(())
    } else {
        Err(validation(format!(
            "ETF option nearbys for `{underlying_symbol}` must be in [0, 3]"
        )))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn non_empty_str_rejects_empty_input() {
        assert_eq!(
            non_empty_str(Some("SHFE"), "exchange").unwrap(),
            Some("SHFE")
        );
        assert_eq!(non_empty_str(None, "exchange").unwrap(), None);

        let error = non_empty_str(Some(""), "exchange").expect_err("empty input should fail");
        assert!(error.to_string().contains("exchange must not be empty"));
    }

    #[test]
    fn parse_query_quotes_result_extracts_symbols() {
        let payload = json!({
            "result": {
                "multi_symbol_info": [
                    {"instrument_id": "SHFE.au2606"},
                    {"instrument_id": "DCE.m2605"},
                    {"instrument_id": 42}
                ]
            }
        });

        assert_eq!(
            parse_query_quotes_result(&payload, None),
            vec!["SHFE.au2606".to_string(), "DCE.m2605".to_string()]
        );
        assert_eq!(
            parse_query_quotes_result(&payload, Some("SHFE")),
            vec!["SHFE.au2606".to_string()]
        );
    }

    #[test]
    fn parse_query_cont_quotes_result_filters_nested_nodes() {
        let payload = json!({
            "result": {
                "multi_symbol_info": [{
                    "underlying": {
                        "edges": [
                            {"node": {"instrument_id": "KQ.m@DCE.m", "exchange_id": "DCE", "product_id": "m"}},
                            {"node": {"instrument_id": "KQ.rb@SHFE.rb", "exchange_id": "SHFE", "product_id": "rb"}}
                        ]
                    }
                }]
            }
        });

        assert_eq!(
            parse_query_cont_quotes_result(&payload, Some("DCE"), Some("m")),
            vec!["KQ.m@DCE.m".to_string()]
        );
        assert!(parse_query_cont_quotes_result(&json!({"result": {}}), None, None).is_empty());
    }

    #[test]
    fn validate_finance_nearbys_rejects_negative_or_out_of_range_nearby() {
        assert!(validate_finance_nearbys("SSE.000300", &[0, 5]).is_ok());
        assert!(validate_finance_nearbys("SSE.510050", &[0, 3]).is_ok());
        assert!(validate_finance_nearbys("SSE.000300", &[-1]).is_err());
        assert!(validate_finance_nearbys("SSE.510050", &[4]).is_err());
    }

    #[test]
    fn option_validators_reject_unsupported_inputs() {
        assert!(validate_option_class("CALL").is_ok());
        assert!(validate_option_class("STRADDLE").is_err());
        assert!(validate_price_levels(&[-100, 0, 100]).is_ok());
        assert!(validate_price_levels(&[-101]).is_err());
        assert!(validate_finance_underlying("SSE.000300").is_ok());
        assert!(validate_finance_underlying("SHFE.au2606").is_err());
    }
}

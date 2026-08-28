use tqsdk_data::{
    BacktestHistoryField, BacktestHistorySchemaSeries, BacktestHistoryValueKind,
    backtest_history_default_fields, backtest_history_resolve_fields,
    backtest_history_schema_fields,
};

#[test]
fn stable_history_schema_preserves_cache_query_contract() {
    assert_schema(
        BacktestHistorySchemaSeries::Kline,
        &[
            (
                "t",
                &["t", "time", "timestamp", "datetime"],
                BacktestHistoryValueKind::Timestamp,
            ),
            ("id", &["id"], BacktestHistoryValueKind::Integer),
            ("o", &["o", "open"], BacktestHistoryValueKind::Price),
            ("h", &["h", "high"], BacktestHistoryValueKind::Price),
            ("l", &["l", "low"], BacktestHistoryValueKind::Price),
            ("c", &["c", "close"], BacktestHistoryValueKind::Price),
            ("v", &["v", "volume"], BacktestHistoryValueKind::Integer),
            (
                "oi0",
                &["oi0", "open_oi"],
                BacktestHistoryValueKind::Integer,
            ),
            ("oi", &["oi", "close_oi"], BacktestHistoryValueKind::Integer),
        ],
        &["t", "o", "h", "l", "c", "v", "oi"],
    );
    assert_schema(
        BacktestHistorySchemaSeries::Tick,
        &[
            (
                "t",
                &["t", "time", "timestamp", "datetime"],
                BacktestHistoryValueKind::Timestamp,
            ),
            ("id", &["id"], BacktestHistoryValueKind::Integer),
            ("lp", &["lp", "last_price"], BacktestHistoryValueKind::Price),
            ("avg", &["avg", "average"], BacktestHistoryValueKind::Price),
            ("hi", &["hi", "highest"], BacktestHistoryValueKind::Price),
            ("lo", &["lo", "lowest"], BacktestHistoryValueKind::Price),
            (
                "ap1",
                &["ap1", "ask_price1"],
                BacktestHistoryValueKind::Price,
            ),
            (
                "av1",
                &["av1", "ask_volume1"],
                BacktestHistoryValueKind::Integer,
            ),
            (
                "bp1",
                &["bp1", "bid_price1"],
                BacktestHistoryValueKind::Price,
            ),
            (
                "bv1",
                &["bv1", "bid_volume1"],
                BacktestHistoryValueKind::Integer,
            ),
            (
                "ap2",
                &["ap2", "ask_price2"],
                BacktestHistoryValueKind::Price,
            ),
            (
                "av2",
                &["av2", "ask_volume2"],
                BacktestHistoryValueKind::Integer,
            ),
            (
                "bp2",
                &["bp2", "bid_price2"],
                BacktestHistoryValueKind::Price,
            ),
            (
                "bv2",
                &["bv2", "bid_volume2"],
                BacktestHistoryValueKind::Integer,
            ),
            (
                "ap3",
                &["ap3", "ask_price3"],
                BacktestHistoryValueKind::Price,
            ),
            (
                "av3",
                &["av3", "ask_volume3"],
                BacktestHistoryValueKind::Integer,
            ),
            (
                "bp3",
                &["bp3", "bid_price3"],
                BacktestHistoryValueKind::Price,
            ),
            (
                "bv3",
                &["bv3", "bid_volume3"],
                BacktestHistoryValueKind::Integer,
            ),
            (
                "ap4",
                &["ap4", "ask_price4"],
                BacktestHistoryValueKind::Price,
            ),
            (
                "av4",
                &["av4", "ask_volume4"],
                BacktestHistoryValueKind::Integer,
            ),
            (
                "bp4",
                &["bp4", "bid_price4"],
                BacktestHistoryValueKind::Price,
            ),
            (
                "bv4",
                &["bv4", "bid_volume4"],
                BacktestHistoryValueKind::Integer,
            ),
            (
                "ap5",
                &["ap5", "ask_price5"],
                BacktestHistoryValueKind::Price,
            ),
            (
                "av5",
                &["av5", "ask_volume5"],
                BacktestHistoryValueKind::Integer,
            ),
            (
                "bp5",
                &["bp5", "bid_price5"],
                BacktestHistoryValueKind::Price,
            ),
            (
                "bv5",
                &["bv5", "bid_volume5"],
                BacktestHistoryValueKind::Integer,
            ),
            ("v", &["v", "volume"], BacktestHistoryValueKind::Integer),
            ("amt", &["amt", "amount"], BacktestHistoryValueKind::Decimal),
            (
                "oi",
                &["oi", "open_interest"],
                BacktestHistoryValueKind::Integer,
            ),
        ],
        &["t", "lp", "ap1", "av1", "bp1", "bv1", "v", "oi"],
    );
}

#[test]
fn aliases_canonicalize_and_duplicates_are_rejected() {
    let fields = backtest_history_resolve_fields(
        BacktestHistorySchemaSeries::Kline,
        ["close", "time", "open_oi"],
    )
    .unwrap();
    assert_eq!(
        fields,
        vec![
            BacktestHistoryField::Close,
            BacktestHistoryField::Time,
            BacktestHistoryField::OpenOi,
        ]
    );
    assert!(
        backtest_history_resolve_fields(BacktestHistorySchemaSeries::Tick, ["time", "timestamp"],)
            .is_err()
    );
}

fn assert_schema(
    series: BacktestHistorySchemaSeries,
    expected_fields: &[(&str, &[&str], BacktestHistoryValueKind)],
    expected_defaults: &[&str],
) {
    let fields = backtest_history_schema_fields(series);
    assert_eq!(fields.len(), expected_fields.len());
    assert_eq!(
        fields
            .iter()
            .map(|field| field.canonical_name())
            .collect::<Vec<_>>(),
        expected_fields
            .iter()
            .map(|(name, _, _)| *name)
            .collect::<Vec<_>>(),
    );
    for (field, (name, aliases, value_kind)) in fields.iter().zip(expected_fields) {
        assert_eq!(field.canonical_name(), *name);
        assert_eq!(field.aliases(), *aliases);
        assert_eq!(field.value_kind(), *value_kind);
    }
    assert_eq!(
        backtest_history_default_fields(series)
            .iter()
            .map(|field| field.canonical_name())
            .collect::<Vec<_>>(),
        expected_defaults,
    );
}

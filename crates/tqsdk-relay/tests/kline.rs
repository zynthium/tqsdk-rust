use tqsdk_relay::{KlineSynthesis, RelayTickRow};

fn tick(id: i64, datetime: i64, price: f64, volume: i64, oi: i64) -> RelayTickRow {
    RelayTickRow {
        id,
        datetime,
        last_price: price,
        volume,
        open_interest: oi,
    }
}

#[test]
fn fixed_window_is_start_inclusive_end_exclusive() {
    let mut synth = KlineSynthesis::new("SHFE.au2602", 60_000_000_000);

    synth.push_tick(tick(1, 0, 610.0, 10, 100)).unwrap();
    synth
        .push_tick(tick(2, 59_999_999_999, 612.0, 15, 110))
        .unwrap();
    let completed = synth
        .push_tick(tick(3, 60_000_000_000, 620.0, 20, 120))
        .unwrap();

    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].datetime, 0);
    assert_eq!(completed[0].open, 610.0);
    assert_eq!(completed[0].high, 612.0);
    assert_eq!(completed[0].low, 610.0);
    assert_eq!(completed[0].close, 612.0);

    let current = synth.current_bar().unwrap();
    assert_eq!(current.datetime, 60_000_000_000);
    assert_eq!(current.open, 620.0);
    assert_eq!(current.close, 620.0);
}

#[test]
fn completed_bar_volume_uses_tick_volume_delta_inside_window() {
    let mut synth = KlineSynthesis::new("SHFE.au2602", 60_000_000_000);

    synth.push_tick(tick(1, 0, 610.0, 100, 1000)).unwrap();
    synth
        .push_tick(tick(2, 30_000_000_000, 612.0, 140, 1005))
        .unwrap();
    let completed = synth
        .push_tick(tick(3, 60_000_000_000, 611.0, 155, 1010))
        .unwrap();

    assert_eq!(completed[0].volume, 40);
    assert_eq!(completed[0].open_oi, 1000);
    assert_eq!(completed[0].close_oi, 1005);
}

#[test]
fn gaps_do_not_emit_empty_bars_without_ticks() {
    let mut synth = KlineSynthesis::new("SHFE.au2602", 60_000_000_000);

    synth.push_tick(tick(1, 0, 610.0, 100, 1000)).unwrap();
    let completed = synth
        .push_tick(tick(2, 180_000_000_000, 612.0, 140, 1005))
        .unwrap();

    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].datetime, 0);
    assert_eq!(synth.current_bar().unwrap().datetime, 180_000_000_000);
}

#[test]
fn old_tick_rows_do_not_reopen_completed_windows() {
    let mut synth = KlineSynthesis::new("SHFE.au2602", 60_000_000_000);

    synth.push_tick(tick(1, 0, 610.0, 100, 1000)).unwrap();
    let completed = synth
        .push_tick(tick(2, 60_000_000_000, 620.0, 130, 1005))
        .unwrap();
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].datetime, 0);

    let completed = synth.push_tick(tick(1, 0, 611.0, 110, 1001)).unwrap();

    assert!(completed.is_empty());
    let current = synth.current_bar().unwrap();
    assert_eq!(current.datetime, 60_000_000_000);
    assert_eq!(current.open, 620.0);
    assert_eq!(current.close, 620.0);
}

#[test]
fn exposes_symbol_and_duration() {
    let synth = KlineSynthesis::new("SHFE.au2602", 60_000_000_000);

    assert_eq!(synth.symbol(), "SHFE.au2602");
    assert_eq!(synth.duration_ns(), 60_000_000_000);
}

#[test]
fn rejects_non_positive_duration() {
    let err = KlineSynthesis::try_new("SHFE.au2602", 0).unwrap_err();

    assert_eq!(
        err.to_string(),
        "invalid relay config: kline duration_ns must be greater than zero"
    );
}

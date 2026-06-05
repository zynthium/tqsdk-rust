use tqsdk_relay::{MarketCache, RelayTickRow};

fn tick(id: i64, last_price: f64) -> RelayTickRow {
    RelayTickRow {
        id,
        datetime: 1_713_660_000_000_000_000 + id,
        last_price,
        volume: id * 10,
        open_interest: id * 100,
    }
}

#[test]
fn tick_ring_retains_latest_rows_per_symbol() {
    let mut cache = MarketCache::new(2, 16);

    cache.push_tick("SHFE.au2602", tick(1, 610.0));
    cache.push_tick("SHFE.au2602", tick(2, 611.0));
    cache.push_tick("SHFE.au2602", tick(3, 612.0));

    let rows = cache.ticks("SHFE.au2602");

    assert_eq!(rows, vec![tick(2, 611.0), tick(3, 612.0)]);
}

#[test]
fn quote_snapshot_is_derived_from_latest_tick() {
    let mut cache = MarketCache::new(4, 16);

    cache.push_tick("SHFE.au2602", tick(1, 610.0));
    cache.push_tick("SHFE.au2602", tick(2, 611.5));

    let quote = cache.quote("SHFE.au2602").unwrap();

    assert_eq!(quote.instrument_id, "SHFE.au2602");
    assert_eq!(quote.last_price, 611.5);
    assert_eq!(quote.volume, 20);
    assert_eq!(quote.open_interest, 200);
    assert_eq!(quote.datetime, "1713660000000000002");
}

#[test]
fn unknown_symbol_returns_no_quote_or_ticks() {
    let cache = MarketCache::new(4, 16);

    assert!(cache.ticks("SHFE.au2602").is_empty());
    assert!(cache.quote("SHFE.au2602").is_none());
}

#[test]
fn tick_rings_are_independent_per_symbol() {
    let mut cache = MarketCache::new(2, 16);

    cache.push_tick("SHFE.au2602", tick(1, 610.0));
    cache.push_tick("SHFE.au2602", tick(2, 611.0));
    cache.push_tick("DCE.m2609", tick(10, 3300.0));
    cache.push_tick("SHFE.au2602", tick(3, 612.0));

    assert_eq!(
        cache.ticks("SHFE.au2602"),
        vec![tick(2, 611.0), tick(3, 612.0)]
    );
    assert_eq!(cache.ticks("DCE.m2609"), vec![tick(10, 3300.0)]);
}

#[test]
fn ticks_returns_cloned_rows() {
    let mut cache = MarketCache::new(2, 16);
    cache.push_tick("SHFE.au2602", tick(1, 610.0));

    let mut rows = cache.ticks("SHFE.au2602");
    rows[0].last_price = 999.0;

    assert_eq!(cache.ticks("SHFE.au2602"), vec![tick(1, 610.0)]);
}

#[test]
fn quote_returns_cloned_snapshot() {
    let mut cache = MarketCache::new(2, 16);
    cache.push_tick("SHFE.au2602", tick(1, 610.0));

    let mut quote = cache.quote("SHFE.au2602").unwrap();
    quote.last_price = 999.0;

    assert_eq!(cache.quote("SHFE.au2602").unwrap().last_price, 610.0);
}

#[test]
fn kline_capacity_returns_configured_capacity() {
    let cache = MarketCache::new(4, 32);

    assert_eq!(cache.kline_capacity(), 32);
}

#[test]
#[should_panic(expected = "tick_capacity must be greater than zero")]
fn new_rejects_zero_tick_capacity() {
    let _ = MarketCache::new(0, 16);
}

#[test]
#[should_panic(expected = "kline_capacity must be greater than zero")]
fn new_rejects_zero_kline_capacity() {
    let _ = MarketCache::new(4, 0);
}

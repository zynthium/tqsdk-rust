#[test]
fn backtest_warmup_does_not_serially_chunk_remote_symbols() {
    let source = include_str!("../src/lib.rs");
    let warmup_start = source
        .find("pub async fn warmup")
        .expect("BacktestBuilder::warmup should exist");
    let next_method_start = source[warmup_start..]
        .find("/// Require all backtest ticks")
        .expect("BacktestBuilder::cache_only docs should follow warmup");
    let warmup_source = &source[warmup_start..warmup_start + next_method_start];

    assert!(
        !warmup_source.contains("remote_symbols.chunks("),
        "warmup must pass all missing symbols to the bounded remote scheduler; \
         facade-level serial chunking defeats remote fill concurrency"
    );
    assert!(
        warmup_source.contains("backtest_remote::fill_backtest_tick_cache("),
        "warmup should use the shared remote cache-fill scheduler"
    );
}

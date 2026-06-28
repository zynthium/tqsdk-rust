use tqsdk::advanced::task::{ReplayMarketEvent, ReplayMarketSource};
use tqsdk::prelude::*;
use tqsdk_core::{Kline, Quote, Symbol, Tick};

#[test]
fn prelude_exposes_default_strategy_surface() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<TqBuilder>();

    let _builder = Tq::futures().auth("demo-user", "demo-pass");
    let _: Option<Tq> = None;
    let _: Option<QuoteRef> = None;
    let _: Option<QuoteSet> = None;
    let _: Option<TargetPos> = None;
}

#[tokio::test]
async fn facade_replay_backtest_accepts_empty_replay() {
    let replay = ReplayMarketSource::new(vec![]);
    let mut tq = Tq::futures()
        .replay_backtest(replay)
        .connect()
        .await
        .unwrap();
    assert!(!tq.next().await.unwrap());
}

#[tokio::test]
async fn facade_backtest_cache_mode_requires_declared_symbols() {
    let cache_dir = temp_cache_dir();
    let result = Tq::futures()
        .backtest(1_000, 61_000_000_000)
        .cache_dir(&cache_dir)
        .unwrap()
        .cache_only()
        .prepare()
        .await;
    let err = match result {
        Ok(_) => panic!("cache-backed backtest unexpectedly prepared without symbols"),
        Err(error) => error,
    };

    assert!(
        err.to_string().contains("at least one symbol"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn facade_backtest_cache_mode_replays_cached_ticks() {
    let symbol = "SHFE.rb2501";
    let cache_dir = temp_cache_dir();
    let cache = BacktestTickCache::open(&cache_dir).unwrap();
    cache
        .store_ticks(
            symbol,
            1_000,
            3_000,
            [tick(1, 1_000, 100.0), tick(2, 2_000, 101.0)],
        )
        .unwrap();

    let mut tq = Tq::futures()
        .backtest(1_000, 3_000)
        .cache_dir(&cache_dir)
        .unwrap()
        .symbol(symbol)
        .connect()
        .await
        .unwrap();
    let quote = tq.quote(symbol).await.unwrap();

    assert!(tq.next().await.unwrap());
    assert_eq!(quote.load().unwrap().last_price, 100.0);
    assert!(tq.next().await.unwrap());
    assert_eq!(quote.load().unwrap().last_price, 101.0);
    assert!(!tq.next().await.unwrap());
}

#[tokio::test]
async fn facade_backtest_remote_on_miss_requires_auth_only_when_cache_missing() {
    let symbol = "SHFE.rb2501";
    let cache_dir = temp_cache_dir();
    let cache = BacktestTickCache::open(&cache_dir).unwrap();
    cache
        .store_ticks(
            symbol,
            1_000,
            3_000,
            [tick(1, 1_000, 100.0), tick(2, 2_000, 101.0)],
        )
        .unwrap();

    let prepared = Tq::futures()
        .backtest(1_000, 3_000)
        .cache_dir(&cache_dir)
        .unwrap()
        .symbol(symbol)
        .cache(BacktestCachePolicy::RemoteOnMiss)
        .prepare()
        .await
        .unwrap();
    assert!(!prepared.data_report().remote_used);

    let missing = match Tq::futures()
        .backtest(1_000, 4_000)
        .cache_dir(&cache_dir)
        .unwrap()
        .symbol(symbol)
        .cache(BacktestCachePolicy::RemoteOnMiss)
        .prepare()
        .await
    {
        Ok(_) => panic!("remote-on-miss unexpectedly prepared with missing cache and no auth"),
        Err(error) => error,
    };
    assert!(
        missing
            .to_string()
            .contains("remote backtest cache fill requires auth")
    );
}

#[tokio::test]
async fn facade_backtest_remote_on_miss_prepare_marks_remote_used_when_cache_missing() {
    let cache_dir = temp_cache_dir();
    let prepared = Tq::futures()
        .auth("demo-user", "demo-pass")
        .backtest(1_000, 4_000)
        .cache_dir(&cache_dir)
        .unwrap()
        .symbol("SHFE.rb2601")
        .cache(BacktestCachePolicy::RemoteOnMiss)
        .prepare()
        .await
        .unwrap();

    assert!(prepared.data_report().remote_used);
    assert_eq!(prepared.data_report().resolved_symbols, 1);
}

#[tokio::test]
#[ignore = "requires TQ_AUTH_USER/TQ_AUTH_PASS and remote backtest service"]
async fn facade_backtest_remote_on_miss_live_smoke() {
    let user = std::env::var("TQ_AUTH_USER").unwrap();
    let pass = std::env::var("TQ_AUTH_PASS").unwrap();
    let cache_dir = temp_cache_dir();
    let symbol = "SHFE.au2608";
    let start_ns = 1_781_172_000_000_000_000;
    let end_ns = 1_781_258_401_000_000_000;

    let mut tq = Tq::futures()
        .auth(user, pass)
        .backtest(start_ns, end_ns)
        .cache_dir(&cache_dir)
        .unwrap()
        .symbol(symbol)
        .cache(BacktestCachePolicy::RemoteOnMiss)
        .connect()
        .await
        .unwrap();
    let quote = tq.quote(symbol).await.unwrap();

    assert!(tq.next().await.unwrap());
    assert!(quote.load().unwrap().last_price.is_finite());
}

#[tokio::test]
async fn facade_local_backtest_accepts_instrument_specs_for_klines() {
    let replay = ReplayMarketSource::new(vec![
        ReplayMarketEvent::kline(
            "fixture",
            "SHFE.rb2501",
            1_000,
            Some(1_000),
            60_000_000_000,
            Kline {
                id: 1,
                datetime: 1_000,
                open: 100.0,
                high: 105.0,
                low: 99.0,
                close: 102.0,
                volume: 10,
                ..Kline::default()
            },
        )
        .unwrap(),
    ]);

    let mut tq = Tq::futures()
        .replay_backtest(replay)
        .instrument_spec(instrument_spec("SHFE.rb2501", 0.5, 10))
        .connect()
        .await
        .unwrap();
    let quote = tq.quote("SHFE.rb2501").await.unwrap();

    assert!(tq.next().await.unwrap());
    let quote = quote.load().unwrap();
    assert_eq!(quote.last_price, 102.0);
    assert_eq!(quote.ask_price1, 102.5);
    assert_eq!(quote.bid_price1, 101.5);
}

#[tokio::test]
async fn facade_local_backtest_target_pos_uses_underlying_for_continuous_replay_symbol() {
    let alias = "KQ.m@SHFE.rb";
    let underlying = "SHFE.rb2501";
    let replay = ReplayMarketSource::new(vec![
        quote_event(alias, 1_000, 100.0, 10, 99.0, 8)
            .with_underlying_symbol(underlying)
            .unwrap(),
        quote_event(alias, 2_000, 101.0, 10, 100.0, 8)
            .with_underlying_symbol(underlying)
            .unwrap(),
    ]);

    let mut tq = Tq::futures()
        .replay_backtest(replay)
        .connect()
        .await
        .unwrap();
    let quote = tq.quote(alias).await.unwrap();
    let target = tq.target_pos(LOCAL_BACKTEST_ACCOUNT_ID, alias).unwrap();
    target.set(1).unwrap();

    assert!(tq.next().await.unwrap());
    assert_eq!(quote.load().unwrap().underlying_symbol, underlying);
    assert_eq!(
        tq.position(LOCAL_BACKTEST_ACCOUNT_ID, alias)
            .load()
            .unwrap()
            .pos_long,
        1
    );
    assert_eq!(
        tq.position(LOCAL_BACKTEST_ACCOUNT_ID, underlying)
            .load()
            .unwrap()
            .pos_long,
        1
    );

    assert!(tq.next().await.unwrap());
    assert!(!tq.next().await.unwrap());

    let summary = tq.backtest_summary().unwrap();
    assert_eq!(summary.trades().len(), 1);
    assert_eq!(summary.trades()[0].exchange_id, "SHFE");
    assert_eq!(summary.trades()[0].instrument_id, "rb2501");
    let metrics = tq.backtest_performance_metrics().unwrap();
    assert_eq!(metrics.open_trade_count(), 1);
    assert_eq!(metrics.close_trade_count(), 0);
    assert_eq!(metrics.start_balance(), metrics.end_balance());
    let report = tq.backtest_performance_report(2).unwrap();
    assert_eq!(
        report.metrics().open_trade_count(),
        metrics.open_trade_count()
    );
    assert_eq!(
        report.metrics().balance_return_rate(),
        metrics.balance_return_rate()
    );
    assert_eq!(report.daily_balance_returns().len(), 1);
    assert_eq!(report.rolling_balance_sharpe_ratios().len(), 1);
    assert_eq!(target.execution_report().trades.len(), 1);

    let (event_cursor, events) = target.execution_events_since(0);
    assert!(!events.is_empty());
    assert!(target.execution_events_since(event_cursor).1.is_empty());
    let (trade_cursor, trades) = target.execution_trades_since(0);
    assert_eq!(trades.len(), 1);
    assert!(target.execution_trades_since(trade_cursor).1.is_empty());
}

#[test]
fn advanced_namespaces_keep_curated_underlying_access() {
    let _session = tqsdk::advanced::session::SessionClientBuilder::new("demo-user", "demo-pass")
        .futures_market();
    let _ = std::any::type_name::<tqsdk::advanced::session::InstrumentSpec>();
    let _ = std::any::type_name::<tqsdk::advanced::session::SymbolInfo>();
    let _data = tqsdk::advanced::data::DataClient::new();
    let _split = tqsdk::advanced::task::VolumeSplitPolicy::new(1, 2).unwrap();

    let _ = std::any::type_name::<tqsdk::advanced::runtime::RuntimeReader>();
}

#[test]
fn facade_root_exports_are_curated() {
    let source = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/lib.rs"))
        .expect("read facade source");
    let top_level_pub_uses = top_level_pub_use_statements(&source);

    for (crate_name, symbol) in [
        ("tqsdk_data", "DataClient"),
        ("tqsdk_session", "SessionClient"),
        ("tqsdk_session", "SessionClientBuilder"),
        ("tqsdk_task", "TargetPosTask"),
        ("tqsdk_task", "TaskHost"),
        ("tqsdk_wait", "TqApi"),
        ("tqsdk_wait", "TqApiBuilder"),
    ] {
        assert!(
            !top_level_pub_uses
                .iter()
                .any(|statement| pub_use_exports_symbol(statement, crate_name, symbol)),
            "facade root exports lower-level symbol directly: {crate_name}::{symbol}"
        );
    }

    for wildcard_export in [
        "pub use tqsdk_core::*",
        "pub use tqsdk_data::*",
        "pub use tqsdk_session::*",
        "pub use tqsdk_task::*",
        "pub use tqsdk_wait::*",
    ] {
        assert!(
            !source.contains(wildcard_export),
            "advanced namespace must be curated, found wildcard: {wildcard_export}"
        );
    }
}

fn top_level_pub_use_statements(source: &str) -> Vec<&str> {
    let mut statements = Vec::new();
    let mut depth = 0usize;
    let mut statement_start = None;

    for (index, ch) in source.char_indices() {
        if let Some(start) = statement_start {
            if ch == ';' {
                statements.push(&source[start..=index]);
                statement_start = None;
            }
            continue;
        }

        match ch {
            '{' => depth += 1,
            '}' => depth = depth.saturating_sub(1),
            _ => {
                if depth == 0
                    && source[index..].starts_with("pub use")
                    && line_prefix_is_whitespace(source, index)
                {
                    statement_start = Some(index);
                }
            }
        }
    }

    statements
}

fn pub_use_exports_symbol(statement: &str, crate_name: &str, symbol: &str) -> bool {
    let normalized = statement
        .chars()
        .filter(|ch| !ch.is_whitespace() && *ch != ';')
        .collect::<String>();
    let Some(exports) = normalized.strip_prefix(&format!("pubuse{crate_name}::")) else {
        return false;
    };

    if let Some(grouped_exports) = exports
        .strip_prefix('{')
        .and_then(|exports| exports.strip_suffix('}'))
    {
        return grouped_exports
            .split(',')
            .map(strip_alias)
            .any(|exported| exported == symbol);
    }

    strip_alias(exports) == symbol
}

fn line_prefix_is_whitespace(source: &str, index: usize) -> bool {
    source[..index]
        .rsplit_once('\n')
        .map_or(&source[..index], |(_previous, prefix)| prefix)
        .trim()
        .is_empty()
}

fn strip_alias(export: &str) -> &str {
    export
        .split_once("as")
        .map_or(export, |(symbol, _alias)| symbol)
}

fn instrument_spec(
    symbol: &str,
    price_tick: f64,
    volume_multiple: i64,
) -> tqsdk::advanced::session::InstrumentSpec {
    let (exchange_id, product_id) =
        symbol
            .split_once('.')
            .map_or(("", symbol), |(exchange, instrument)| {
                let product_len = instrument
                    .chars()
                    .take_while(|ch| ch.is_ascii_alphabetic())
                    .count();
                (exchange, &instrument[..product_len])
            });
    tqsdk::advanced::session::InstrumentSpec {
        symbol: Symbol::new(symbol),
        exchange_id: exchange_id.to_string(),
        product_id: product_id.to_string(),
        class: tqsdk::advanced::session::InstrumentClass::Future,
        price_tick,
        volume_multiple,
        expire_datetime_secs: None,
        underlying_symbol: None,
    }
}

fn quote_event(
    symbol: &str,
    datetime: i64,
    ask_price1: f64,
    ask_volume1: i64,
    bid_price1: f64,
    bid_volume1: i64,
) -> ReplayMarketEvent {
    ReplayMarketEvent::quote(
        "fixture",
        symbol,
        datetime,
        Some(datetime),
        Quote {
            datetime: "2026-05-15 09:30:00.000000".to_string(),
            last_price: ask_price1,
            ask_price1,
            ask_volume1,
            bid_price1,
            bid_volume1,
            ..Quote::default()
        },
    )
    .unwrap()
}

#[test]
fn facade_does_not_expose_premature_stock_surface() {
    let source = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/lib.rs"))
        .expect("read facade source");

    for removed_surface in ["pub fn stock(", "MarketKind::Stock"] {
        assert!(
            !source.contains(removed_surface),
            "premature stock facade surface remains: {removed_surface}"
        );
    }
}

#[test]
fn facade_result_accepts_session_errors() {
    let error = tqsdk_session::SessionFacadeError::InvalidState("facade contract");
    let _: tqsdk::Error = error.into();
}

#[test]
fn facade_exposes_tqkq_target_helpers_instead_of_literal_account_ids() {
    let source = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/lib.rs"))
        .expect("read facade source");

    for required_surface in [
        "pub async fn tqkq_account_id(&self) -> Result<String>",
        "pub async fn target_pos_tqkq(&mut self, symbol: &str) -> Result<TargetPos>",
        "pub async fn target_pos_tqkq_numbered(",
        "tqkq_login_command()",
        "tqkq_login_command_numbered(number)",
    ] {
        assert!(
            source.contains(required_surface),
            "missing resolved TQKQ facade helper: {required_surface}"
        );
    }
}

#[test]
fn facade_exposes_default_account_login_helpers() {
    let source = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/lib.rs"))
        .expect("read facade source");

    for required_surface in [
        "pub fn default_account_id(&self) -> Result<&str>",
        "pub fn account_default(&self) -> Result<tqsdk_wait::AccountRef>",
        "pub fn position_default(&self, symbol: &str) -> Result<tqsdk_wait::PositionRef>",
        "pub fn target_pos_default(&mut self, symbol: &str) -> Result<TargetPos>",
        "pub async fn login_trade_account(",
        "pub async fn login_tqkq_account(&mut self) -> Result<tqsdk_wait::AccountRef>",
        "pub fn tqkq_sim(mut self) -> Self",
        "pub fn trade_account_env(self) -> Result<Self>",
    ] {
        assert!(
            source.contains(required_surface),
            "missing default account facade helper: {required_surface}"
        );
    }
}

#[test]
fn facade_exposes_market_relay_builder_method() {
    let builder = tqsdk::Tq::futures()
        .auth("demo-user", "demo-pass")
        .market_relay("ws://127.0.0.1:7788/market")
        .trade_target_tqkq();

    let debug = format!("{builder:?}");
    assert!(debug.contains("market_url"));
}

#[test]
fn target_pos_wrapper_uses_sync_intent_api_and_no_direct_wait() {
    let source = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/lib.rs"))
        .expect("read facade source");

    for expected_surface in [
        "pub fn set(&self, volume: i64) -> Result<()>",
        "pub fn close(&self) -> Result<()>",
        "pub fn is_finished(&self) -> bool",
        "pub fn last_error(&self) -> Option<tqsdk_task::TaskError>",
        "pub fn execution_report(&self) -> tqsdk_task::TargetPosTaskExecutionReport",
    ] {
        assert!(
            source.contains(expected_surface),
            "missing explicit target-position wrapper surface: {expected_surface}"
        );
    }

    for removed_surface in [
        "pub async fn set(&mut self",
        "pub async fn close(&mut self",
        "pub async fn wait_target_reached",
    ] {
        assert!(
            !source.contains(removed_surface),
            "misleading async target-position surface remains: {removed_surface}"
        );
    }
}

#[test]
fn default_facade_contract_example_exists() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/examples/api_contract_s33_default_facade.rs"
    );
    let source = std::fs::read_to_string(path).expect("read default facade example");

    for required in [
        "use tqsdk::prelude::*;",
        "Tq::futures()",
        ".auth_env()?",
        ".trade_target_tqkq()",
        "target_pos_tqkq(\"SHFE.au2602\").await?",
        "while tq.next().await?",
        "target.set(1)?",
    ] {
        assert!(
            source.contains(required),
            "default facade example missing required flow fragment: {required}"
        );
    }
}

#[test]
fn replay_backtest_contract_example_exposes_custom_replay_flow() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/examples/api_contract_s38_facade_local_backtest.rs"
    );
    let source = std::fs::read_to_string(path).expect("read local backtest facade example");

    for required in [
        "ReplayMarketSource::new(vec![])",
        ".replay_backtest(replay)",
        ".quote_symbol(\"SHFE.au2510\")",
        ".price_tick(\"SHFE.au2510\", 0.02)",
    ] {
        assert!(
            source.contains(required),
            "local replay backtest example missing required flow fragment: {required}"
        );
    }
}

#[test]
fn backtest_contract_example_exposes_cache_backed_flow() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/examples/api_contract_s43_facade_backtest_history_cache.rs"
    );
    let source = std::fs::read_to_string(path).expect("read cache-backed backtest example");

    for required in [
        "BacktestTickCache::open",
        ".backtest(1_000, 3_000)",
        ".cache_dir(&cache_dir)?",
        ".cache_only()",
        ".symbol(SYMBOL)",
        "while tq.next().await?",
    ] {
        assert!(
            source.contains(required),
            "cache-backed backtest example missing required flow fragment: {required}"
        );
    }
}

fn temp_cache_dir() -> std::path::PathBuf {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "tqsdk-facade-contract-cache-{}-{unique}",
        std::process::id()
    ))
}

fn tick(id: i64, datetime: i64, last_price: f64) -> Tick {
    Tick {
        id,
        datetime,
        last_price,
        ask_price1: last_price + 0.5,
        ask_volume1: 1,
        bid_price1: last_price - 0.5,
        bid_volume1: 1,
        ..Tick::default()
    }
}

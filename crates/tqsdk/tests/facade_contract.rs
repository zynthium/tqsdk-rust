use tqsdk::prelude::*;

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
async fn facade_local_backtest_alias_history_helpers_build_empty_replay() {
    let kline_series = Vec::<tqsdk::advanced::data::KlineDataSeries>::new();
    let mut tq = Tq::futures()
        .local_backtest_klines_as("KQ.m@SHFE.rb", kline_series)
        .unwrap()
        .connect()
        .await
        .unwrap();
    assert!(!tq.next().await.unwrap());

    let tick_series = Vec::<tqsdk::advanced::data::TickDataSeries>::new();
    let mut tq = Tq::futures()
        .local_backtest_ticks_as("KQ.m@SHFE.rb", tick_series)
        .unwrap()
        .connect()
        .await
        .unwrap();
    assert!(!tq.next().await.unwrap());
}

#[test]
fn advanced_namespaces_keep_curated_underlying_access() {
    let _session = tqsdk::advanced::session::SessionClientBuilder::new("demo-user", "demo-pass")
        .futures_market();
    let _stream =
        tqsdk::advanced::stream::TqStreamBuilder::new("demo-user", "demo-pass").futures_market();
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
        ("tqsdk_stream", "TqStream"),
        ("tqsdk_stream", "TqStreamBuilder"),
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
        "pub use tqsdk_stream::*",
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

#[test]
fn facade_does_not_expose_premature_stock_or_hardcoded_trade_login() {
    let source = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/lib.rs"))
        .expect("read facade source");

    for removed_surface in [
        "pub fn stock(",
        "pub async fn login_trade_account(",
        "MarketKind::Stock",
        "TradeAccountType::Future,",
    ] {
        assert!(
            !source.contains(removed_surface),
            "premature or futures-only facade surface remains: {removed_surface}"
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

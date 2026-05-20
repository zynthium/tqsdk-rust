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
